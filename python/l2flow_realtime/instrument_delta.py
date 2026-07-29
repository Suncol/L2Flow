"""Stateful Wire V2 tick-delta sessions and explicit-EOF cursors."""

from __future__ import annotations

import mmap
import socket
import struct
import threading
from dataclasses import dataclass
from enum import IntEnum
from typing import Iterator, Optional, Union

from ._generation import (
    GenerationEndpoint,
    parse_generation_endpoint,
    validate_same_session,
)
from ._history_columns import (
    LazyWireColumns,
    tick_columns,
    validate_tick_payload_canonical,
)
from ._stream_control import (
    OK,
    TERMINAL_FLAG,
    StreamCheckpointMismatchError,
    StreamNotFoundError,
    StreamResourceExhaustedError,
    UINT64_MAX,
    positive_uint32,
    raise_status,
    recv_packet,
    request_id,
    send_packet,
    validate_page_fd,
    validate_page_records,
    validate_response_prefix,
    validate_socket_path,
    validate_timeout,
)
from .checkpoint import (
    CHECKPOINT_BYTES,
    PAYLOAD_PROJECTION_CORE_V2,
    TICK_RECORD_COVERAGE_COMPLETE,
    InstrumentTickDeltaCheckpoint,
)
from .models import (
    ClientClosedError,
    L2FlowRealtimeError,
    ProtocolError,
    SessionIdentity,
    StaleSessionError,
    UnavailableError,
    WireFormatError,
)
from .wire import (
    CONTROL_MAGIC,
    TICK_BYTES,
    WIRE_MAJOR,
    WIRE_MINOR,
)


DELTA_PAGE_MAGIC = b"L2FIDT2\x00"
DELTA_PAGE_HEADER_BYTES = 4096
DELTA_PAGE_ENDIAN_MARKER = 0x01020304
DELTA_OPEN_SESSION_OPCODE = 4
DELTA_OPEN_INSTRUMENT_OPCODE = 5
DELTA_READ_OPCODE = 6
DELTA_CHECKPOINT_MISMATCH = 7
DELTA_SELECTED_SOURCE_MASK = (1 << 1) | (1 << 3)

_OPEN_SESSION_REQUEST = struct.Struct("<8sHHHHIIQQ")
_OPEN_SESSION_RESPONSE_BYTES = 296
_OPEN_INSTRUMENT_PREFIX = struct.Struct("<8sHHHHIIQIIIIQ")
_OPEN_INSTRUMENT_RESPONSE_BYTES = 776
_READ_REQUEST = struct.Struct("<8sHHHHIIQQQ")
_READ_RESPONSE = struct.Struct("<8sHHHHIIQQQQQ")
_METADATA_PREFIX = struct.Struct("<II")
_METADATA_TAIL = struct.Struct("<4Q5QII8s")
_PAGE_PREFIX = struct.Struct("<8sHHIIIQQIIQQQQQ")
_PAYLOAD_COMMON = struct.Struct("<IIIIQQQ")
_PAYLOAD_SOURCE = struct.Struct("<IIII6B")

assert _OPEN_SESSION_REQUEST.size == 40
assert _OPEN_INSTRUMENT_PREFIX.size == 56
assert _READ_REQUEST.size == 48
assert _READ_RESPONSE.size == 64
assert _METADATA_TAIL.size == 88
assert _PAGE_PREFIX.size == 88


class InstrumentTickDeltaBaseKind(IntEnum):
    ORIGIN = 1
    CHECKPOINT = 2


class InstrumentTickDeltaSessionClosedError(ClientClosedError):
    """The stateful delta session is closed."""


class InstrumentTickDeltaCursorClosedError(ClientClosedError):
    """The stateful instrument cursor is closed."""


class DeltaCheckpointUnverifiedError(UnavailableError):
    """A target checkpoint was requested before explicit EOF."""


@dataclass(frozen=True, slots=True)
class InstrumentTickDeltaGeneration:
    """The immutable generation pinned by one delta session."""

    endpoint: GenerationEndpoint

    @property
    def session_identity(self) -> SessionIdentity:
        return self.endpoint.session_identity

    @property
    def run_id(self) -> bytes:
        return self.endpoint.run_id

    @property
    def session_epoch(self) -> int:
        return self.endpoint.session_epoch

    @property
    def generation(self) -> int:
        return self.endpoint.generation

    @property
    def data_state_generation(self) -> int:
        return self.endpoint.data_state_generation

    @property
    def ingress_sequence_exclusive(self) -> int:
        return self.endpoint.ingress_sequence_exclusive

    @property
    def tick_stream_sequence_exclusive(self) -> int:
        return self.endpoint.tick_stream_sequence_exclusive

    @property
    def recv_monotonic_cut_ns(self) -> int:
        return self.endpoint.recv_monotonic_cut_ns

    @property
    def history_published_monotonic_ns(self) -> int:
        return self.endpoint.history_published_monotonic_ns

    @property
    def accepted_sequence(self) -> int:
        return self.endpoint.accepted_sequence

    @property
    def durable_sequence(self) -> int:
        return self.endpoint.durable_sequence

    @property
    def applied_sequence(self) -> int:
        return self.endpoint.applied_sequence

    @property
    def processing_lag_records(self) -> int:
        return self.accepted_sequence - self.applied_sequence

    @property
    def durability_lag_records(self) -> int:
        return self.accepted_sequence - self.durable_sequence

    @property
    def catalog_generation(self) -> int:
        return self.endpoint.catalog_generation

    @property
    def catalog_digest(self) -> bytes:
        return self.endpoint.catalog_digest

    @property
    def input_identity_sha256(self) -> bytes:
        return self.endpoint.input_identity_sha256

    @property
    def source_stream_ids(self) -> tuple[int, int, int, int]:
        return self.endpoint.source_stream_ids

    @property
    def source_sequence_exclusive(
        self,
    ) -> tuple[int, int, int, int]:
        return self.endpoint.source_sequence_exclusive

    @property
    def trade_date(self) -> int:
        return self.endpoint.trade_date

    @property
    def capacity(self) -> int:
        return self.endpoint.capacity

    @property
    def bound_count(self) -> int:
        return self.endpoint.bound_count

    @property
    def available_count(self) -> int:
        return self.endpoint.available_count

    @property
    def snapshot_available_count(self) -> int:
        return self.endpoint.snapshot_available_count

    @property
    def tick_available_count(self) -> int:
        return self.endpoint.tick_available_count

    @property
    def factor_eligible_count(self) -> int:
        return self.endpoint.factor_eligible_count

    @property
    def catalog_scope(self):
        return self.endpoint.catalog_scope

    @property
    def coverage_complete(self) -> bool:
        return self.endpoint.coverage_complete

    @property
    def coverage_from_open(self) -> bool:
        return self.endpoint.coverage_from_open

    @property
    def record_coverage_complete(self) -> bool:
        return self.endpoint.record_coverage_complete

    @property
    def flags(self) -> int:
        return self.endpoint.flags


@dataclass(frozen=True, slots=True)
class InstrumentTickDeltaMetadata:
    """Exact finite delta bounds and EOF count authority."""

    base_kind: InstrumentTickDeltaBaseKind
    selected_source_mask: int
    base_checkpoint: Optional[InstrumentTickDeltaCheckpoint]
    target_checkpoint: InstrumentTickDeltaCheckpoint
    delta_tick_source_record_counts: tuple[int, int, int, int]
    delta_tick_record_count: int
    ingress_sequence_begin_inclusive: int
    ingress_sequence_end_exclusive: int
    tick_stream_sequence_begin_inclusive: int
    tick_stream_sequence_end_exclusive: int
    tick_record_coverage_complete: bool
    payload_projection: int

    @property
    def instrument_id(self) -> int:
        return self.target_checkpoint.instrument_id

    @property
    def origin(self) -> Optional[InstrumentTickDeltaCheckpoint]:
        return self.base_checkpoint


@dataclass(frozen=True, slots=True)
class InstrumentTickDeltaPage:
    """One client-owned dense tick page, or the explicit terminal page."""

    generation: InstrumentTickDeltaGeneration
    metadata: InstrumentTickDeltaMetadata
    page_index: int
    tick_columns: LazyWireColumns
    first_ingress_sequence: int
    last_ingress_sequence: int
    first_tick_stream_sequence: int
    last_tick_stream_sequence: int
    mapping_bytes: int
    eof: bool
    cumulative_record_count: int
    cumulative_source_record_counts: tuple[int, int, int, int]

    @property
    def record_count(self) -> int:
        return self.tick_columns.row_count

    @property
    def instrument_id(self) -> int:
        return self.metadata.instrument_id

    def __len__(self) -> int:
        return self.record_count

    def materialize_all(self) -> dict[str, tuple[object, ...]]:
        return self.tick_columns.materialize_all()


def _parse_checkpoint(
    data: bytes | memoryview, offset: int
) -> InstrumentTickDeltaCheckpoint:
    try:
        return InstrumentTickDeltaCheckpoint.from_wire(
            data
            if isinstance(data, (bytes, memoryview))
            else memoryview(data),
            offset,
        )
    except (TypeError, ValueError) as error:
        raise WireFormatError(
            f"invalid tick delta checkpoint: {error}"
        ) from error


def _parse_metadata(
    data: bytes | memoryview, offset: int = 0
) -> InstrumentTickDeltaMetadata:
    if len(data) < offset + 736:
        raise WireFormatError("tick delta metadata is truncated")
    base_value, selected_mask = _METADATA_PREFIX.unpack_from(data, offset)
    try:
        base_kind = InstrumentTickDeltaBaseKind(base_value)
    except ValueError as error:
        raise WireFormatError(
            "tick delta base kind is unsupported"
        ) from error
    base_wire = bytes(data[offset + 8 : offset + 328])
    if base_kind is InstrumentTickDeltaBaseKind.ORIGIN:
        if any(base_wire):
            raise WireFormatError(
                "origin delta contains a nonzero base checkpoint"
            )
        base_checkpoint = None
    else:
        base_checkpoint = _parse_checkpoint(data, offset + 8)
    target = _parse_checkpoint(data, offset + 328)
    tail = _METADATA_TAIL.unpack_from(data, offset + 648)
    source_counts = tuple(tail[:4])
    (
        record_count,
        ingress_begin,
        ingress_end,
        tick_begin,
        tick_end,
        flags,
        projection,
        reserved,
    ) = tail[4:]
    if selected_mask != DELTA_SELECTED_SOURCE_MASK:
        raise WireFormatError(
            "tick delta selected source mask is not slots 1 and 3"
        )
    if (
        source_counts[0]
        or source_counts[2]
        or sum(source_counts) != record_count
    ):
        raise WireFormatError(
            "tick delta source counts do not reconcile"
        )
    if flags != TICK_RECORD_COVERAGE_COMPLETE or any(reserved):
        raise WireFormatError(
            "tick delta flags/reserved bytes are invalid"
        )
    if projection != PAYLOAD_PROJECTION_CORE_V2:
        raise WireFormatError("unsupported tick delta projection")
    origin_counts = (
        (0, 0, 0, 0)
        if base_checkpoint is None
        else base_checkpoint.instrument_tick_source_record_counts
    )
    expected_counts = tuple(
        target_value - origin_value
        for target_value, origin_value in zip(
            target.instrument_tick_source_record_counts,
            origin_counts,
        )
    )
    if source_counts != expected_counts:
        raise WireFormatError(
            "tick delta counts disagree with its checkpoints"
        )
    if base_checkpoint is None:
        expected_ingress_begin = 1
        expected_tick_begin = 1
    else:
        try:
            target.ensure_successor_of(base_checkpoint)
        except StaleSessionError as error:
            raise WireFormatError(
                "tick delta checkpoints do not form a successor pair"
            ) from error
        expected_ingress_begin = (
            base_checkpoint.ingress_sequence_exclusive
        )
        expected_tick_begin = (
            base_checkpoint.tick_stream_sequence_exclusive
        )
    if (
        ingress_begin != expected_ingress_begin
        or ingress_end != target.ingress_sequence_exclusive
        or tick_begin != expected_tick_begin
        or tick_end != target.tick_stream_sequence_exclusive
        or ingress_begin > ingress_end
        or tick_begin > tick_end
    ):
        raise WireFormatError(
            "tick delta half-open bounds disagree with checkpoints"
        )
    return InstrumentTickDeltaMetadata(
        base_kind=base_kind,
        selected_source_mask=selected_mask,
        base_checkpoint=base_checkpoint,
        target_checkpoint=target,
        delta_tick_source_record_counts=source_counts,  # type: ignore[arg-type]
        delta_tick_record_count=record_count,
        ingress_sequence_begin_inclusive=ingress_begin,
        ingress_sequence_end_exclusive=ingress_end,
        tick_stream_sequence_begin_inclusive=tick_begin,
        tick_stream_sequence_end_exclusive=tick_end,
        tick_record_coverage_complete=True,
        payload_projection=projection,
    )


class InstrumentTickDeltaSession:
    """One pinned target generation with one active cursor at a time."""

    __slots__ = (
        "_channel",
        "_generation",
        "_session_token",
        "_closed",
        "_active_cursor",
        "_lock",
    )

    def __init__(
        self,
        channel: socket.socket,
        endpoint: GenerationEndpoint,
        session_token: int,
    ) -> None:
        self._channel = channel
        self._generation = InstrumentTickDeltaGeneration(endpoint)
        if (
            not isinstance(session_token, int)
            or isinstance(session_token, bool)
            or session_token <= 0
            or session_token > UINT64_MAX
        ):
            raise ProtocolError("delta session token is invalid")
        self._session_token = session_token
        self._closed = False
        self._active_cursor: Optional[InstrumentTickDeltaCursor] = None
        self._lock = threading.RLock()

    @property
    def generation(self) -> InstrumentTickDeltaGeneration:
        return self._generation

    @property
    def session_identity(self) -> SessionIdentity:
        return self._generation.session_identity

    @property
    def closed(self) -> bool:
        return self._closed

    def _require_open(self) -> None:
        if self._closed:
            raise InstrumentTickDeltaSessionClosedError(
                "tick delta session is closed"
            )

    def _fail_closed(self) -> None:
        self._closed = True
        active = self._active_cursor
        self._active_cursor = None
        if active is not None:
            active._closed = True
        self._channel.close()

    def open_instrument(
        self,
        instrument_id: int,
        *,
        base_checkpoint: Optional[
            InstrumentTickDeltaCheckpoint
        ] = None,
        requested_page_records: int = 4096,
    ) -> "InstrumentTickDeltaCursor":
        instrument_id = positive_uint32(
            instrument_id, "instrument_id"
        )
        requested_page_records = validate_page_records(
            requested_page_records
        )
        if base_checkpoint is None:
            base_kind = InstrumentTickDeltaBaseKind.ORIGIN
            base_wire = b"\x00" * CHECKPOINT_BYTES
        else:
            if not isinstance(
                base_checkpoint, InstrumentTickDeltaCheckpoint
            ):
                raise TypeError(
                    "base_checkpoint must be a verified tick checkpoint"
                )
            base_checkpoint.ensure_session(
                run_id=self._generation.endpoint.run_id,
                session_epoch=(
                    self._generation.endpoint.session_epoch
                ),
                trade_date=self._generation.endpoint.trade_date,
                capacity=self._generation.endpoint.capacity,
            )
            if base_checkpoint.instrument_id != instrument_id:
                raise ValueError(
                    "base checkpoint belongs to another instrument"
                )
            base_kind = InstrumentTickDeltaBaseKind.CHECKPOINT
            base_wire = base_checkpoint.to_wire()
        open_id = request_id()
        request = (
            _OPEN_INSTRUMENT_PREFIX.pack(
                CONTROL_MAGIC,
                WIRE_MAJOR,
                WIRE_MINOR,
                DELTA_OPEN_INSTRUMENT_OPCODE,
                0,
                384,
                0,
                open_id,
                instrument_id,
                requested_page_records,
                int(base_kind),
                0,
                self._session_token,
            )
            + base_wire
            + b"\x00" * 8
        )
        with self._lock:
            self._require_open()
            if self._active_cursor is not None:
                raise L2FlowRealtimeError(
                    "delta session already has an active "
                    "instrument cursor"
                )
            try:
                send_packet(
                    self._channel,
                    request,
                    "tick delta OPEN_INSTRUMENT",
                )
                packet = recv_packet(
                    self._channel,
                    _OPEN_INSTRUMENT_RESPONSE_BYTES,
                    "tick delta OPEN_INSTRUMENT",
                )
            except BaseException:
                self._fail_closed()
                raise
            try:
                status, flags, _reserved = (
                    validate_response_prefix(
                        packet.data,
                        expected_bytes=(
                            _OPEN_INSTRUMENT_RESPONSE_BYTES
                        ),
                        expected_request_id=open_id,
                        allowed_flags=0,
                        operation=(
                            "tick delta OPEN_INSTRUMENT"
                        ),
                    )
                )
                if flags or packet.fds:
                    raise ProtocolError(
                        "delta instrument OPEN returned "
                        "flags/descriptors"
                    )
                if status != OK:
                    if any(packet.data[32:]):
                        raise ProtocolError(
                            "delta instrument OPEN error carries "
                            "success metadata"
                        )
                    raise_status(
                        "tick delta OPEN_INSTRUMENT", status
                    )
                initial_token = struct.unpack_from(
                    "<Q", packet.data, 32
                )[0]
                if initial_token == 0:
                    raise ProtocolError(
                        "delta instrument OPEN returned zero token"
                    )
                metadata = _parse_metadata(packet.data, 40)
                if metadata.target_checkpoint.endpoint != (
                    self._generation.endpoint
                ):
                    raise ProtocolError(
                        "delta instrument OPEN changed target "
                        "generation"
                    )
                if metadata.instrument_id != instrument_id:
                    raise ProtocolError(
                        "delta instrument OPEN returned another "
                        "instrument"
                    )
                if (
                    metadata.base_kind is not base_kind
                    or metadata.base_checkpoint != base_checkpoint
                ):
                    raise ProtocolError(
                        "delta instrument OPEN changed its base "
                        "checkpoint"
                    )
                cursor = InstrumentTickDeltaCursor(
                    self,
                    metadata,
                    initial_token,
                    requested_page_records,
                )
                self._active_cursor = cursor
                return cursor
            except BaseException:
                self._fail_closed()
                raise
            finally:
                packet.close()

    def _cursor_finished(
        self,
        cursor: "InstrumentTickDeltaCursor",
        *,
        exhausted: bool,
    ) -> None:
        with self._lock:
            if self._active_cursor is cursor:
                self._active_cursor = None
        if not exhausted:
            self.close()

    def close(self) -> None:
        try:
            self._channel.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        with self._lock:
            if not self._closed:
                self._fail_closed()

    def __enter__(self) -> "InstrumentTickDeltaSession":
        with self._lock:
            self._require_open()
        return self

    def __exit__(self, _type, _value, _traceback) -> None:
        self.close()


class InstrumentTickDeltaCursor:
    """Forward-only tick cursor whose checkpoint is published only at EOF."""

    __slots__ = (
        "_session",
        "_metadata",
        "_read_token",
        "_requested_page_records",
        "_next_page_index",
        "_closed",
        "_eof",
        "_verified_checkpoint",
        "_records_read",
        "_source_counts",
        "_last_ingress",
        "_last_source",
        "_last_tick",
    )

    def __init__(
        self,
        session: InstrumentTickDeltaSession,
        metadata: InstrumentTickDeltaMetadata,
        read_token: int,
        requested_page_records: int,
    ) -> None:
        self._session = session
        self._metadata = metadata
        if (
            not isinstance(read_token, int)
            or isinstance(read_token, bool)
            or read_token <= 0
            or read_token > UINT64_MAX
        ):
            raise ProtocolError("delta initial read token is invalid")
        self._read_token = read_token
        self._requested_page_records = validate_page_records(
            requested_page_records
        )
        self._next_page_index = 0
        self._closed = False
        self._eof = False
        self._verified_checkpoint: Optional[
            InstrumentTickDeltaCheckpoint
        ] = None
        self._records_read = 0
        self._source_counts = [0, 0, 0, 0]
        self._last_ingress = 0
        self._last_source = [0, 0, 0, 0]
        self._last_tick = 0

    @property
    def metadata(self) -> InstrumentTickDeltaMetadata:
        return self._metadata

    @property
    def generation(self) -> InstrumentTickDeltaGeneration:
        return self._session.generation

    @property
    def instrument_id(self) -> int:
        return self._metadata.instrument_id

    @property
    def session_identity(self) -> SessionIdentity:
        return self._session.session_identity

    @property
    def base_kind(self) -> InstrumentTickDeltaBaseKind:
        return self._metadata.base_kind

    @property
    def base_checkpoint(
        self,
    ) -> Optional[InstrumentTickDeltaCheckpoint]:
        return self._metadata.base_checkpoint

    @property
    def expected_record_count(self) -> int:
        return self._metadata.delta_tick_record_count

    @property
    def requested_page_records(self) -> int:
        return self._requested_page_records

    @property
    def next_page_index(self) -> int:
        return self._next_page_index

    @property
    def cumulative_record_count(self) -> int:
        return self._records_read

    @property
    def cumulative_source_record_counts(
        self,
    ) -> tuple[int, int, int, int]:
        return tuple(self._source_counts)  # type: ignore[return-value]

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def eof(self) -> bool:
        return self._eof

    @property
    def done(self) -> bool:
        return self._eof

    @property
    def verified_checkpoint(self) -> InstrumentTickDeltaCheckpoint:
        if self._verified_checkpoint is None:
            raise DeltaCheckpointUnverifiedError(
                "tick checkpoint is available only after explicit EOF"
            )
        return self._verified_checkpoint

    def _require_open(self) -> None:
        if self._closed:
            raise InstrumentTickDeltaCursorClosedError(
                "tick delta cursor is closed"
            )
        self._session._require_open()

    def read_page(self) -> Optional[InstrumentTickDeltaPage]:
        with self._session._lock:
            self._require_open()
            if self._eof:
                return None
            if self._next_page_index > UINT64_MAX:
                self._session._fail_closed()
                raise UnavailableError(
                    "tick delta page index space is exhausted"
                )
            read_id = request_id()
            request = _READ_REQUEST.pack(
                CONTROL_MAGIC,
                WIRE_MAJOR,
                WIRE_MINOR,
                DELTA_READ_OPCODE,
                0,
                _READ_REQUEST.size,
                0,
                read_id,
                self._next_page_index,
                self._read_token,
            )
            try:
                send_packet(
                    self._session._channel,
                    request,
                    "tick delta READ",
                )
                packet = recv_packet(
                    self._session._channel,
                    _READ_RESPONSE.size,
                    "tick delta READ",
                )
            except BaseException:
                self._session._fail_closed()
                raise
            try:
                status, flags, record_count = (
                    validate_response_prefix(
                        packet.data,
                        expected_bytes=_READ_RESPONSE.size,
                        expected_request_id=read_id,
                        allowed_flags=TERMINAL_FLAG,
                        operation="tick delta READ",
                        record_count_field=True,
                    )
                )
                (
                    _magic,
                    _major,
                    _minor,
                    _status,
                    _flags,
                    _message_bytes,
                    _record_count,
                    _response_id,
                    mapping_bytes,
                    page_index,
                    generation,
                    next_token,
                ) = _READ_RESPONSE.unpack(packet.data)
                if status != OK:
                    if packet.fds:
                        raise ProtocolError(
                            "tick delta READ error carries a "
                            "descriptor"
                        )
                    if (
                        flags != 0
                        or record_count != 0
                        or mapping_bytes != 0
                        or page_index != 0
                        or generation != 0
                        or next_token != 0
                    ):
                        raise ProtocolError(
                            "tick delta READ error carries "
                            "success metadata"
                        )
                    raise_status("tick delta READ", status)
                if generation != self.generation.generation:
                    raise ProtocolError(
                        "tick delta READ changed target generation"
                    )
                if page_index != self._next_page_index:
                    raise ProtocolError(
                        "tick delta READ page index mismatch"
                    )
                terminal = bool(flags & TERMINAL_FLAG)
                if terminal:
                    if (
                        record_count != 0
                        or mapping_bytes != 0
                        or next_token != 0
                        or packet.fds
                    ):
                        raise ProtocolError(
                            "tick delta EOF is not canonical"
                        )
                    self._verify_eof()
                    self._verified_checkpoint = (
                        self._metadata.target_checkpoint
                    )
                    self._eof = True
                    self._read_token = 0
                    self._next_page_index += 1
                    self._session._cursor_finished(
                        self, exhausted=True
                    )
                    return InstrumentTickDeltaPage(
                        generation=self.generation,
                        metadata=self._metadata,
                        page_index=page_index,
                        tick_columns=tick_columns(b""),
                        first_ingress_sequence=0,
                        last_ingress_sequence=0,
                        first_tick_stream_sequence=0,
                        last_tick_stream_sequence=0,
                        mapping_bytes=0,
                        eof=True,
                        cumulative_record_count=(
                            self._records_read
                        ),
                        cumulative_source_record_counts=tuple(
                            self._source_counts
                        ),  # type: ignore[arg-type]
                    )
                expected_mapping_bytes = (
                    DELTA_PAGE_HEADER_BYTES
                    + record_count * TICK_BYTES
                )
                if (
                    record_count == 0
                    or record_count
                    > self._requested_page_records
                    or mapping_bytes != expected_mapping_bytes
                    or next_token == 0
                    or next_token == self._read_token
                    or len(packet.fds) != 1
                ):
                    raise ProtocolError(
                        "nonterminal tick delta response is "
                        "noncanonical"
                    )
                validate_page_fd(packet.only_fd, mapping_bytes)
                page = self._parse_page(
                    packet.only_fd,
                    mapping_bytes,
                    record_count,
                    page_index,
                )
                self._read_token = next_token
                self._next_page_index += 1
                return page
            except BaseException:
                self._session._fail_closed()
                raise
            finally:
                packet.close()

    def _parse_page(
        self,
        fd: int,
        mapping_bytes: int,
        response_record_count: int,
        response_page_index: int,
    ) -> InstrumentTickDeltaPage:
        mapped = mmap.mmap(fd, mapping_bytes, access=mmap.ACCESS_READ)
        try:
            (
                magic,
                major,
                minor,
                header_bytes,
                endian_marker,
                flags,
                total_bytes,
                page_index,
                record_count,
                tick_payload_bytes,
                ticks_offset,
                first_ingress,
                last_ingress,
                first_tick,
                last_tick,
            ) = _PAGE_PREFIX.unpack_from(mapped)
            if (
                magic != DELTA_PAGE_MAGIC
                or (major, minor) != (WIRE_MAJOR, WIRE_MINOR)
                or header_bytes != DELTA_PAGE_HEADER_BYTES
                or endian_marker != DELTA_PAGE_ENDIAN_MARKER
            ):
                raise WireFormatError(
                    "tick delta page is not canonical Wire V2"
                )
            if flags != 0 or any(mapped[824:DELTA_PAGE_HEADER_BYTES]):
                raise WireFormatError(
                    "tick delta page flags/reserved bytes are nonzero"
                )
            if (
                total_bytes != mapping_bytes
                or page_index != response_page_index
                or record_count != response_record_count
                or tick_payload_bytes != TICK_BYTES
                or ticks_offset != DELTA_PAGE_HEADER_BYTES
                or total_bytes
                != DELTA_PAGE_HEADER_BYTES + record_count * TICK_BYTES
            ):
                raise WireFormatError(
                    "tick delta page dense layout is invalid"
                )
            metadata = _parse_metadata(mapped, 88)
            if metadata != self._metadata:
                raise WireFormatError(
                    "tick delta page metadata changed"
                )
            payload_block = bytes(
                mapped[
                    ticks_offset:
                    ticks_offset + record_count * TICK_BYTES
                ]
            )
        finally:
            mapped.close()
        ingress_values, tick_values = self._validate_payloads(
            payload_block
        )
        if (
            first_ingress != ingress_values[0]
            or last_ingress != ingress_values[-1]
            or first_tick != tick_values[0]
            or last_tick != tick_values[-1]
        ):
            raise WireFormatError(
                "tick delta page bounds disagree with payloads"
            )
        new_total = self._records_read + record_count
        if (
            new_total > self._metadata.delta_tick_record_count
            or any(
                current > expected
                for current, expected in zip(
                    self._source_counts,
                    self._metadata.delta_tick_source_record_counts,
                )
            )
        ):
            raise WireFormatError(
                "tick delta pages exceed declared counts"
            )
        self._records_read = new_total
        return InstrumentTickDeltaPage(
            generation=self.generation,
            metadata=self._metadata,
            page_index=response_page_index,
            tick_columns=tick_columns(payload_block),
            first_ingress_sequence=first_ingress,
            last_ingress_sequence=last_ingress,
            first_tick_stream_sequence=first_tick,
            last_tick_stream_sequence=last_tick,
            mapping_bytes=mapping_bytes,
            eof=False,
            cumulative_record_count=new_total,
            cumulative_source_record_counts=tuple(
                self._source_counts
            ),  # type: ignore[arg-type]
        )

    def _validate_payloads(
        self, payloads: bytes
    ) -> tuple[tuple[int, ...], tuple[int, ...]]:
        ingress_values: list[int] = []
        tick_values: list[int] = []
        target = self._metadata.target_checkpoint
        for offset in range(0, len(payloads), TICK_BYTES):
            (
                schema,
                record_bytes,
                instrument_id,
                ordinal,
                source_sequence,
                ingress,
                tick_sequence,
            ) = _PAYLOAD_COMMON.unpack_from(payloads, offset)
            (
                source_stream_id,
                trade_date,
                _vendor_time,
                reserved,
                source_slot,
                event_kind,
                market,
                quantity_unit,
                security_type,
                asset_scope,
            ) = _PAYLOAD_SOURCE.unpack_from(payloads, offset + 96)
            expected_slot = {2: 1, 4: 3, 5: 3}.get(event_kind)
            expected_market = 1 if event_kind == 2 else 2
            if (
                schema != 2
                or record_bytes != TICK_BYTES
                or instrument_id != target.instrument_id
                or ordinal != target.ordinal
                or expected_slot is None
                or source_slot != expected_slot
                or source_stream_id
                != target.source_stream_ids[source_slot]
                or trade_date != target.trade_date
                or reserved != 0
                or market != expected_market
                or quantity_unit > 5
                or security_type > 7
                or asset_scope > 2
                or ingress
                < self._metadata.ingress_sequence_begin_inclusive
                or ingress
                >= self._metadata.ingress_sequence_end_exclusive
                or tick_sequence
                < self._metadata.tick_stream_sequence_begin_inclusive
                or tick_sequence
                >= self._metadata.tick_stream_sequence_end_exclusive
                or tick_sequence > ingress
                or source_sequence
                < (
                    1
                    if self._metadata.base_checkpoint is None
                    else self._metadata.base_checkpoint
                    .source_sequence_exclusive[source_slot]
                )
                or source_sequence
                >= target.source_sequence_exclusive[source_slot]
                or any(payloads[offset + 118 : offset + 128])
            ):
                raise WireFormatError(
                    "tick delta payload identity/bounds are invalid"
                )
            validate_tick_payload_canonical(
                payloads,
                offset,
                event_kind=event_kind,
                trade_date=target.trade_date,
            )
            if (
                ingress <= self._last_ingress
                or source_sequence
                <= self._last_source[source_slot]
                or tick_sequence <= self._last_tick
            ):
                raise WireFormatError(
                    "tick delta sequences are not strictly increasing"
                )
            self._last_ingress = ingress
            self._last_source[source_slot] = source_sequence
            self._last_tick = tick_sequence
            self._source_counts[source_slot] += 1
            if (
                self._source_counts[source_slot]
                > self._metadata.delta_tick_source_record_counts[
                    source_slot
                ]
            ):
                raise WireFormatError(
                    "tick delta source pages exceed declared count"
                )
            ingress_values.append(ingress)
            tick_values.append(tick_sequence)
        return tuple(ingress_values), tuple(tick_values)

    def _verify_eof(self) -> None:
        if (
            self._records_read
            != self._metadata.delta_tick_record_count
            or tuple(self._source_counts)
            != self._metadata.delta_tick_source_record_counts
        ):
            raise WireFormatError(
                "explicit tick delta EOF does not reconcile counts"
            )

    def pages(
        self, *, include_eof: bool = False
    ) -> Iterator[InstrumentTickDeltaPage]:
        if not isinstance(include_eof, bool):
            raise TypeError("include_eof must be bool")
        while not self._eof:
            page = self.read_page()
            if page is None:
                break
            if page.eof:
                if include_eof:
                    yield page
                break
            yield page

    def close(self) -> None:
        # A premature cursor close invalidates the stateful session and must
        # be able to wake a READ blocked while holding the session lock.
        if not self._eof:
            self._closed = True
            self._session.close()
            return
        with self._session._lock:
            if self._closed:
                return
            self._closed = True
        self._session._cursor_finished(
            self, exhausted=True
        )

    def __enter__(self) -> "InstrumentTickDeltaCursor":
        with self._session._lock:
            self._require_open()
        return self

    def __exit__(self, _type, _value, _traceback) -> None:
        self.close()


def _open_instrument_tick_delta_session(
    control_socket_path: Union[str, bytes],
    *,
    expected_generation: int,
    expected_run_id: bytes,
    expected_session_epoch: int,
    expected_trade_date: int,
    expected_capacity: int,
    timeout: Optional[float],
) -> InstrumentTickDeltaSession:
    path = validate_socket_path(control_socket_path)
    if (
        not isinstance(expected_generation, int)
        or isinstance(expected_generation, bool)
        or expected_generation < 0
        or expected_generation > UINT64_MAX
    ):
        raise ValueError("expected_generation must be a uint64")
    timeout = validate_timeout(timeout)
    channel = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    channel.settimeout(timeout)
    try:
        channel.connect(path)
        open_id = request_id()
        request = _OPEN_SESSION_REQUEST.pack(
            CONTROL_MAGIC,
            WIRE_MAJOR,
            WIRE_MINOR,
            DELTA_OPEN_SESSION_OPCODE,
            0,
            _OPEN_SESSION_REQUEST.size,
            0,
            open_id,
            expected_generation,
        )
        send_packet(channel, request, "tick delta OPEN_SESSION")
        with recv_packet(
            channel,
            _OPEN_SESSION_RESPONSE_BYTES,
            "tick delta OPEN_SESSION",
        ) as packet:
            status, flags, _reserved = validate_response_prefix(
                packet.data,
                expected_bytes=_OPEN_SESSION_RESPONSE_BYTES,
                expected_request_id=open_id,
                allowed_flags=0,
                operation="tick delta OPEN_SESSION",
            )
            if flags or packet.fds:
                raise ProtocolError(
                    "delta session OPEN returned flags/descriptors"
                )
            if status != OK:
                if any(packet.data[32:]):
                    raise ProtocolError(
                        "delta session OPEN error carries "
                        "success metadata"
                    )
                if status == DELTA_CHECKPOINT_MISMATCH:
                    raise StreamCheckpointMismatchError(
                        "requested delta generation changed"
                    )
                raise_status("tick delta OPEN_SESSION", status)
            endpoint = parse_generation_endpoint(packet.data, 32)
            token = struct.unpack_from("<Q", packet.data, 288)[0]
            if token == 0:
                raise ProtocolError(
                    "delta session OPEN returned zero token"
                )
            if (
                expected_generation
                and endpoint.generation != expected_generation
            ):
                raise ProtocolError(
                    "delta session ignored expected_generation"
                )
            validate_same_session(
                endpoint,
                run_id=expected_run_id,
                session_epoch=expected_session_epoch,
                trade_date=expected_trade_date,
                capacity=expected_capacity,
            )
        return InstrumentTickDeltaSession(channel, endpoint, token)
    except BaseException:
        channel.close()
        raise


__all__ = [
    "DeltaCheckpointUnverifiedError",
    "InstrumentTickDeltaBaseKind",
    "InstrumentTickDeltaCursor",
    "InstrumentTickDeltaCursorClosedError",
    "InstrumentTickDeltaGeneration",
    "InstrumentTickDeltaMetadata",
    "InstrumentTickDeltaPage",
    "InstrumentTickDeltaSession",
    "InstrumentTickDeltaSessionClosedError",
    "StreamCheckpointMismatchError",
    "StreamNotFoundError",
    "StreamResourceExhaustedError",
]
