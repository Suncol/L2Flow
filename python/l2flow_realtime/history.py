"""Wire V2 complete-history cursor for one observed instrument.

The cursor owns a stateful ``SOCK_SEQPACKET`` connection.  Each nonterminal
READ copies one independently sealed memfd into client-owned column blocks.
Only a separate, zero-record terminal response is accepted as EOF.
"""

from __future__ import annotations

import mmap
import socket
import struct
import threading
from dataclasses import dataclass
from types import MappingProxyType
from typing import Iterator, Mapping, Optional, Union

from ._generation import (
    GenerationEndpoint,
    parse_generation_endpoint,
    validate_same_session,
)
from ._history_columns import (
    LazyWireColumns,
    snapshot_columns,
    tick_columns,
    validate_snapshot_payload_canonical,
    validate_tick_payload_canonical,
)
from ._stream_control import (
    OK,
    TERMINAL_FLAG,
    StreamNotFoundError,
    StreamResourceExhaustedError,
    UINT64_MAX,
    positive_uint32,
    positive_uint64,
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
from .models import (
    ClientClosedError,
    ProtocolError,
    SessionIdentity,
    StaleSessionError,
    WireFormatError,
)
from .wire import (
    CONTROL_MAGIC,
    SNAPSHOT_BYTES,
    TICK_BYTES,
    WIRE_MAJOR,
    WIRE_MINOR,
)


HISTORY_PAGE_MAGIC = b"L2FHST2\x00"
HISTORY_PAGE_HEADER_BYTES = 4096
HISTORY_OPEN_OPCODE = 2
HISTORY_READ_OPCODE = 3
HISTORY_GENERATION_CHANGED = 7
HISTORY_PAYLOAD_PROJECTION_CORE_V2 = 1
HISTORY_DESCRIPTOR_BYTES = 40
HISTORY_PAGE_ENDIAN_MARKER = 0x01020304
HISTORY_PROJECTION_FLAGS_MASK = 0x3

_OPEN_REQUEST = struct.Struct("<8sHHHHIIQIIQ2Q")
_OPEN_RESPONSE_BYTES = 376
_READ_REQUEST = struct.Struct("<8sHHHHIIQQQ")
_READ_RESPONSE = struct.Struct("<8sHHHHIIQQQQQ")
_HISTORY_LOCAL = struct.Struct("<II4Q3QII8s")
_PAGE_PREFIX = struct.Struct("<8sHHIIIQQIIQQIIQIIQQ")
_DESCRIPTOR = struct.Struct("<QQQII4BI")
_PAYLOAD_COMMON = struct.Struct("<IIIIQQQ")
_PAYLOAD_SOURCE = struct.Struct("<IIII6B")

assert _OPEN_REQUEST.size == 64
assert _READ_REQUEST.size == 48
assert _READ_RESPONSE.size == 64
assert _HISTORY_LOCAL.size == 80
assert _PAGE_PREFIX.size == 104
assert _DESCRIPTOR.size == HISTORY_DESCRIPTOR_BYTES


class HistoryGenerationChangedError(StaleSessionError):
    """The requested exact generation is no longer the published cut."""


class HistoryCursorClosedError(ClientClosedError):
    """A history read was attempted after its cursor was closed."""


@dataclass(frozen=True, slots=True)
class HistoryGeneration:
    """Count authority for one instrument in one pinned generation."""

    endpoint: GenerationEndpoint
    instrument_id: int
    ordinal: int
    instrument_source_record_counts: tuple[int, int, int, int]
    instrument_record_count: int
    snapshot_record_count: int
    tick_record_count: int
    payload_projection: int

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
    def history_published_monotonic_ns(self) -> int:
        return self.endpoint.history_published_monotonic_ns

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
    def catalog_scope(self):
        return self.endpoint.catalog_scope

    @property
    def coverage_complete(self) -> bool:
        return self.endpoint.coverage_complete

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
    def coverage_from_open(self) -> bool:
        return self.endpoint.coverage_from_open

    @property
    def record_coverage_complete(self) -> bool:
        return self.endpoint.record_coverage_complete

    @property
    def flags(self) -> int:
        return self.endpoint.flags


@dataclass(frozen=True, slots=True)
class HistoryPage:
    """One client-owned, immutable page in generation order."""

    generation: HistoryGeneration
    page_index: int
    descriptor_columns: Mapping[str, tuple[object, ...]]
    snapshot_columns: LazyWireColumns
    tick_columns: LazyWireColumns
    first_ingress_sequence: int
    last_ingress_sequence: int
    mapping_bytes: int
    eof: bool
    cumulative_record_count: int
    cumulative_source_record_counts: tuple[int, int, int, int]

    @property
    def record_count(self) -> int:
        return len(self.descriptor_columns["ingress_sequence"])

    @property
    def snapshot_count(self) -> int:
        return self.snapshot_columns.row_count

    @property
    def tick_count(self) -> int:
        return self.tick_columns.row_count

    def __len__(self) -> int:
        return self.record_count

    def materialize_all(self) -> dict[str, object]:
        """Force descriptor, snapshot, and tick columns as separate blocks."""

        return {
            "descriptor_columns": dict(self.descriptor_columns),
            "snapshot_columns": self.snapshot_columns.materialize_all(),
            "tick_columns": self.tick_columns.materialize_all(),
        }


def _parse_generation(
    data: bytes | memoryview, offset: int = 0
) -> HistoryGeneration:
    endpoint = parse_generation_endpoint(data, offset)
    fields = _HISTORY_LOCAL.unpack_from(data, offset + 256)
    (
        instrument_id,
        ordinal,
        *tail,
    ) = fields
    source_counts = tuple(tail[:4])
    record_count, snapshot_count, tick_count = tail[4:7]
    payload_projection, reserved0, reserved = tail[7:10]
    if instrument_id == 0 or ordinal != instrument_id - 1:
        raise WireFormatError("history generation instrument ID is invalid")
    if instrument_id > endpoint.bound_count:
        raise WireFormatError(
            "history instrument is outside the observed bound prefix"
        )
    if reserved0 or any(reserved):
        raise WireFormatError("history generation reserved bytes are nonzero")
    if payload_projection != HISTORY_PAYLOAD_PROJECTION_CORE_V2:
        raise WireFormatError("unsupported history payload projection")
    if source_counts[0] + source_counts[2] != snapshot_count:
        raise WireFormatError(
            "history snapshot count does not match source counts"
        )
    if source_counts[1] + source_counts[3] != tick_count:
        raise WireFormatError(
            "history tick count does not match source counts"
        )
    if sum(source_counts) != record_count:
        raise WireFormatError(
            "history record count does not match source counts"
        )
    if snapshot_count + tick_count != record_count:
        raise WireFormatError("history payload counts do not reconcile")
    for count, endpoint_exclusive in zip(
        source_counts, endpoint.source_sequence_exclusive
    ):
        if count > endpoint_exclusive - 1:
            raise WireFormatError(
                "instrument history exceeds its source endpoint"
            )
    return HistoryGeneration(
        endpoint=endpoint,
        instrument_id=instrument_id,
        ordinal=ordinal,
        instrument_source_record_counts=source_counts,  # type: ignore[arg-type]
        instrument_record_count=record_count,
        snapshot_record_count=snapshot_count,
        tick_record_count=tick_count,
        payload_projection=payload_projection,
    )


def _same_generation(
    first: HistoryGeneration, second: HistoryGeneration
) -> bool:
    return first == second


class HistoryCursor:
    """Forward-only cursor that requires an explicit terminal READ for EOF."""

    __slots__ = (
        "_channel",
        "_generation",
        "_read_token",
        "_requested_page_records",
        "_next_page_index",
        "_closed",
        "_eof",
        "_records_read",
        "_snapshots_read",
        "_ticks_read",
        "_source_counts",
        "_last_ingress",
        "_last_source",
        "_last_tick",
        "_lock",
    )

    def __init__(
        self,
        channel: socket.socket,
        generation: HistoryGeneration,
        read_token: int,
        requested_page_records: int,
    ) -> None:
        self._channel = channel
        self._generation = generation
        self._read_token = positive_uint64(
            read_token, "initial_read_token"
        )
        self._requested_page_records = validate_page_records(
            requested_page_records
        )
        self._next_page_index = 0
        self._closed = False
        self._eof = False
        self._records_read = 0
        self._snapshots_read = 0
        self._ticks_read = 0
        self._source_counts = [0, 0, 0, 0]
        self._last_ingress = 0
        self._last_source = [0, 0, 0, 0]
        self._last_tick = 0
        self._lock = threading.Lock()

    @property
    def generation(self) -> HistoryGeneration:
        return self._generation

    @property
    def session_identity(self) -> SessionIdentity:
        return self._generation.session_identity

    @property
    def instrument_id(self) -> int:
        return self._generation.instrument_id

    @property
    def ordinal(self) -> int:
        return self._generation.ordinal

    @property
    def requested_page_records(self) -> int:
        return self._requested_page_records

    @property
    def next_page_index(self) -> int:
        return self._next_page_index

    @property
    def expected_record_count(self) -> int:
        return self._generation.instrument_record_count

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

    def _require_readable(self) -> None:
        if self._closed:
            raise HistoryCursorClosedError("history cursor is closed")

    def _fail_closed(self) -> None:
        self._closed = True
        self._channel.close()

    def read_page(self) -> Optional[HistoryPage]:
        with self._lock:
            self._require_readable()
            if self._eof:
                return None
            if self._next_page_index > UINT64_MAX:
                self._fail_closed()
                raise ProtocolError(
                    "history page index space is exhausted"
                )
            read_id = request_id()
            request = _READ_REQUEST.pack(
                CONTROL_MAGIC,
                WIRE_MAJOR,
                WIRE_MINOR,
                HISTORY_READ_OPCODE,
                0,
                _READ_REQUEST.size,
                0,
                read_id,
                self._next_page_index,
                self._read_token,
            )
            try:
                send_packet(self._channel, request, "history READ")
                packet = recv_packet(
                    self._channel,
                    _READ_RESPONSE.size,
                    "history READ",
                )
            except BaseException:
                self._fail_closed()
                raise
            try:
                status, flags, record_count = (
                    validate_response_prefix(
                        packet.data,
                        expected_bytes=_READ_RESPONSE.size,
                        expected_request_id=read_id,
                        allowed_flags=TERMINAL_FLAG,
                        operation="history READ",
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
                            "history READ error carries a descriptor"
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
                            "history READ error carries success "
                            "metadata"
                        )
                    if status == HISTORY_GENERATION_CHANGED:
                        raise HistoryGenerationChangedError(
                            "history generation changed during "
                            "pinned read"
                        )
                    raise_status("history READ", status)
                if generation != self._generation.generation:
                    raise ProtocolError(
                        "history READ response changed generation"
                    )
                if page_index != self._next_page_index:
                    raise ProtocolError(
                        "history READ page index mismatch"
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
                            "history EOF is not canonical"
                        )
                    self._verify_eof()
                    self._eof = True
                    self._read_token = 0
                    self._next_page_index += 1
                    empty_descriptors = MappingProxyType(
                        {
                            name: ()
                            for name in (
                                "ingress_sequence",
                                "source_sequence",
                                "tick_stream_sequence",
                                "payload_index",
                                "projection_flags",
                                "payload_kind",
                                "event_kind",
                                "source_slot",
                            )
                        }
                    )
                    self._channel.close()
                    return HistoryPage(
                        generation=self._generation,
                        page_index=page_index,
                        descriptor_columns=empty_descriptors,
                        snapshot_columns=snapshot_columns(b""),
                        tick_columns=tick_columns(b""),
                        first_ingress_sequence=0,
                        last_ingress_sequence=0,
                        mapping_bytes=0,
                        eof=True,
                        cumulative_record_count=(
                            self._records_read
                        ),
                        cumulative_source_record_counts=tuple(
                            self._source_counts
                        ),  # type: ignore[arg-type]
                    )
                maximum_mapping_bytes = (
                    HISTORY_PAGE_HEADER_BYTES
                    + record_count
                    * (HISTORY_DESCRIPTOR_BYTES + SNAPSHOT_BYTES)
                )
                if (
                    record_count == 0
                    or record_count
                    > self._requested_page_records
                    or mapping_bytes < HISTORY_PAGE_HEADER_BYTES
                    or mapping_bytes > maximum_mapping_bytes
                    or next_token == 0
                    or next_token == self._read_token
                    or len(packet.fds) != 1
                ):
                    raise ProtocolError(
                        "nonterminal history READ response is "
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
                self._fail_closed()
                raise
            finally:
                packet.close()

    def _parse_page(
        self,
        fd: int,
        mapping_bytes: int,
        response_record_count: int,
        response_page_index: int,
    ) -> HistoryPage:
        mapped = mmap.mmap(fd, mapping_bytes, access=mmap.ACCESS_READ)
        try:
            prefix = _PAGE_PREFIX.unpack_from(mapped)
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
                descriptor_bytes,
                descriptors_offset,
                snapshots_offset,
                snapshot_count,
                snapshot_payload_bytes,
                ticks_offset,
                tick_count,
                tick_payload_bytes,
                first_ingress,
                last_ingress,
            ) = prefix
            if (
                magic != HISTORY_PAGE_MAGIC
                or (major, minor) != (WIRE_MAJOR, WIRE_MINOR)
                or header_bytes != HISTORY_PAGE_HEADER_BYTES
                or endian_marker != HISTORY_PAGE_ENDIAN_MARKER
            ):
                raise WireFormatError(
                    "history page header is not canonical Wire V2"
                )
            if flags != 0 or any(mapped[440:HISTORY_PAGE_HEADER_BYTES]):
                raise WireFormatError(
                    "history page flags/reserved bytes are nonzero"
                )
            if (
                total_bytes != mapping_bytes
                or page_index != response_page_index
                or record_count != response_record_count
            ):
                raise WireFormatError(
                    "history page disagrees with READ response"
                )
            if (
                descriptor_bytes != HISTORY_DESCRIPTOR_BYTES
                or snapshot_payload_bytes != SNAPSHOT_BYTES
                or tick_payload_bytes != TICK_BYTES
                or snapshot_count + tick_count != record_count
            ):
                raise WireFormatError(
                    "history page payload counts/sizes are invalid"
                )
            expected_descriptors = HISTORY_PAGE_HEADER_BYTES
            expected_snapshots = (
                expected_descriptors
                + record_count * HISTORY_DESCRIPTOR_BYTES
            )
            expected_ticks = (
                expected_snapshots + snapshot_count * SNAPSHOT_BYTES
            )
            expected_total = expected_ticks + tick_count * TICK_BYTES
            if (
                descriptors_offset != expected_descriptors
                or snapshots_offset != expected_snapshots
                or ticks_offset != expected_ticks
                or total_bytes != expected_total
            ):
                raise WireFormatError(
                    "history page dense layout is noncanonical"
                )
            generation = _parse_generation(mapped, 104)
            if not _same_generation(generation, self._generation):
                raise WireFormatError(
                    "history page generation metadata changed"
                )
            descriptor_block = bytes(
                mapped[
                    descriptors_offset:
                    descriptors_offset
                    + record_count * HISTORY_DESCRIPTOR_BYTES
                ]
            )
            snapshot_block = bytes(
                mapped[
                    snapshots_offset:
                    snapshots_offset + snapshot_count * SNAPSHOT_BYTES
                ]
            )
            tick_block = bytes(
                mapped[
                    ticks_offset:
                    ticks_offset + tick_count * TICK_BYTES
                ]
            )
        finally:
            mapped.close()

        columns = self._validate_descriptors_and_payloads(
            descriptor_block, snapshot_block, tick_block
        )
        if (
            first_ingress != columns["ingress_sequence"][0]
            or last_ingress != columns["ingress_sequence"][-1]
        ):
            raise WireFormatError(
                "history page ingress bounds disagree with descriptors"
            )
        new_records = self._records_read + record_count
        new_snapshots = self._snapshots_read + snapshot_count
        new_ticks = self._ticks_read + tick_count
        expected = self._generation
        if (
            new_records > expected.instrument_record_count
            or new_snapshots > expected.snapshot_record_count
            or new_ticks > expected.tick_record_count
            or any(
                current > declared
                for current, declared in zip(
                    self._source_counts,
                    expected.instrument_source_record_counts,
                )
            )
        ):
            raise WireFormatError(
                "history pages exceed generation counts"
            )
        self._records_read = new_records
        self._snapshots_read = new_snapshots
        self._ticks_read = new_ticks
        return HistoryPage(
            generation=self._generation,
            page_index=response_page_index,
            descriptor_columns=MappingProxyType(columns),
            snapshot_columns=snapshot_columns(snapshot_block),
            tick_columns=tick_columns(tick_block),
            first_ingress_sequence=first_ingress,
            last_ingress_sequence=last_ingress,
            mapping_bytes=mapping_bytes,
            eof=False,
            cumulative_record_count=new_records,
            cumulative_source_record_counts=tuple(
                self._source_counts
            ),  # type: ignore[arg-type]
        )

    def _validate_descriptors_and_payloads(
        self,
        descriptors: bytes,
        snapshots: bytes,
        ticks: bytes,
    ) -> dict[str, tuple[object, ...]]:
        names = (
            "ingress_sequence",
            "source_sequence",
            "tick_stream_sequence",
            "payload_index",
            "projection_flags",
            "payload_kind",
            "event_kind",
            "source_slot",
        )
        values: dict[str, list[object]] = {name: [] for name in names}
        next_snapshot = 0
        next_tick = 0
        for offset in range(0, len(descriptors), _DESCRIPTOR.size):
            (
                ingress,
                source,
                tick_sequence,
                payload_index,
                projection_flags,
                payload_kind,
                event_kind,
                source_slot,
                reserved0,
                reserved1,
            ) = _DESCRIPTOR.unpack_from(descriptors, offset)
            if reserved0 or reserved1:
                raise WireFormatError(
                    "history descriptor reserved fields are nonzero"
                )
            expected_slot = {1: 0, 2: 1, 3: 2, 4: 3, 5: 3}.get(
                event_kind
            )
            if expected_slot is None or source_slot != expected_slot:
                raise WireFormatError(
                    "history descriptor event/source slot disagree"
                )
            if projection_flags & ~HISTORY_PROJECTION_FLAGS_MASK:
                raise WireFormatError(
                    "history descriptor has unknown projection flags"
                )
            if payload_kind == 1:
                if (
                    event_kind not in (1, 3)
                    or payload_index != next_snapshot
                    or projection_flags != 0
                ):
                    raise WireFormatError(
                        "history snapshot descriptor is invalid"
                    )
                payloads = snapshots
                payload_base = payload_index * SNAPSHOT_BYTES
                expected_bytes = SNAPSHOT_BYTES
                next_snapshot += 1
            elif payload_kind == 2:
                if (
                    event_kind not in (2, 4, 5)
                    or payload_index != next_tick
                ):
                    raise WireFormatError(
                        "history tick descriptor is invalid"
                    )
                payloads = ticks
                payload_base = payload_index * TICK_BYTES
                expected_bytes = TICK_BYTES
                next_tick += 1
            else:
                raise WireFormatError(
                    "history descriptor payload kind is unsupported"
                )
            self._validate_payload_common(
                payloads,
                base=payload_base,
                expected_bytes=expected_bytes,
                ingress=ingress,
                source=source,
                tick_sequence=tick_sequence,
                source_slot=source_slot,
                event_kind=event_kind,
            )
            if payload_kind == 2 and struct.unpack_from(
                "<I", payloads, payload_base + 132
            )[0] != projection_flags:
                raise WireFormatError(
                    "history tick projection flags disagree"
                )
            endpoint = self._generation.endpoint
            if (
                ingress <= self._last_ingress
                or ingress >= endpoint.ingress_sequence_exclusive
            ):
                raise WireFormatError(
                    "history ingress sequence is outside its "
                    "strict generation order"
                )
            if (
                source <= self._last_source[source_slot]
                or source
                >= endpoint.source_sequence_exclusive[source_slot]
            ):
                raise WireFormatError(
                    "history source sequence is outside its "
                    "strict generation order"
                )
            if payload_kind == 2:
                if (
                    tick_sequence <= self._last_tick
                    or tick_sequence
                    >= endpoint.tick_stream_sequence_exclusive
                    or tick_sequence > ingress
                ):
                    raise WireFormatError(
                        "history tick sequence is outside its "
                        "strict generation order"
                    )
                self._last_tick = tick_sequence
            elif tick_sequence != 0:
                raise WireFormatError(
                    "snapshot history descriptor has a tick sequence"
                )
            self._last_ingress = ingress
            self._last_source[source_slot] = source
            self._source_counts[source_slot] += 1
            if (
                self._source_counts[source_slot]
                > self._generation.instrument_source_record_counts[
                    source_slot
                ]
            ):
                raise WireFormatError(
                    "history source pages exceed generation count"
                )
            for name, item in zip(
                names,
                (
                    ingress,
                    source,
                    tick_sequence,
                    payload_index,
                    projection_flags,
                    payload_kind,
                    event_kind,
                    source_slot,
                ),
            ):
                values[name].append(item)
        if (
            next_snapshot * SNAPSHOT_BYTES != len(snapshots)
            or next_tick * TICK_BYTES != len(ticks)
        ):
            raise WireFormatError(
                "history descriptor payload indices are not dense"
            )
        return {name: tuple(items) for name, items in values.items()}

    def _validate_payload_common(
        self,
        payloads: bytes,
        *,
        base: int,
        expected_bytes: int,
        ingress: int,
        source: int,
        tick_sequence: int,
        source_slot: int,
        event_kind: int,
    ) -> None:
        if base < 0 or base + expected_bytes > len(payloads):
            raise WireFormatError("history payload is truncated")
        (
            schema,
            record_bytes,
            instrument_id,
            ordinal,
            payload_source,
            payload_ingress,
            payload_tick,
        ) = _PAYLOAD_COMMON.unpack_from(payloads, base)
        (
            source_stream_id,
            trade_date,
            _vendor_time,
            reserved,
            payload_slot,
            payload_kind,
            market,
            quantity_unit,
            security_type,
            asset_scope,
        ) = _PAYLOAD_SOURCE.unpack_from(payloads, base + 96)
        expected_market = 1 if event_kind in (1, 2) else 2
        if (
            schema != 2
            or record_bytes != expected_bytes
            or instrument_id != self._generation.instrument_id
            or ordinal != self._generation.ordinal
            or payload_source != source
            or payload_ingress != ingress
            or payload_tick != tick_sequence
            or source_stream_id
            != self._generation.endpoint.source_stream_ids[source_slot]
            or trade_date != self._generation.trade_date
            or reserved != 0
            or payload_slot != source_slot
            or payload_kind != event_kind
            or market != expected_market
            or quantity_unit > 5
            or security_type > 7
            or asset_scope > 2
            or any(payloads[base + 118 : base + 128])
        ):
            raise WireFormatError(
                "history descriptor and payload identity disagree"
            )
        if expected_bytes == SNAPSHOT_BYTES:
            validate_snapshot_payload_canonical(
                payloads,
                base,
                event_kind=event_kind,
                trade_date=self._generation.trade_date,
            )
        else:
            validate_tick_payload_canonical(
                payloads,
                base,
                event_kind=event_kind,
                trade_date=self._generation.trade_date,
            )

    def _verify_eof(self) -> None:
        expected = self._generation
        if (
            self._records_read != expected.instrument_record_count
            or self._snapshots_read != expected.snapshot_record_count
            or self._ticks_read != expected.tick_record_count
            or tuple(self._source_counts)
            != expected.instrument_source_record_counts
        ):
            raise WireFormatError(
                "explicit history EOF does not reconcile generation counts"
            )

    def pages(
        self, *, include_eof: bool = False
    ) -> Iterator[HistoryPage]:
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

    @property
    def done(self) -> bool:
        return self._eof

    def close(self) -> None:
        try:
            self._channel.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        with self._lock:
            if not self._closed:
                self._fail_closed()

    def __enter__(self) -> "HistoryCursor":
        with self._lock:
            self._require_readable()
        return self

    def __exit__(self, _type, _value, _traceback) -> None:
        self.close()


def _open_instrument_history(
    control_socket_path: Union[str, bytes],
    *,
    instrument_id: int,
    requested_page_records: int,
    expected_generation: int,
    expected_run_id: bytes,
    expected_session_epoch: int,
    expected_trade_date: int,
    expected_capacity: int,
    timeout: Optional[float],
) -> HistoryCursor:
    path = validate_socket_path(control_socket_path)
    instrument_id = positive_uint32(instrument_id, "instrument_id")
    requested_page_records = validate_page_records(
        requested_page_records
    )
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
        request = _OPEN_REQUEST.pack(
            CONTROL_MAGIC,
            WIRE_MAJOR,
            WIRE_MINOR,
            HISTORY_OPEN_OPCODE,
            0,
            _OPEN_REQUEST.size,
            0,
            open_id,
            instrument_id,
            requested_page_records,
            expected_generation,
            0,
            0,
        )
        send_packet(channel, request, "history OPEN")
        with recv_packet(
            channel, _OPEN_RESPONSE_BYTES, "history OPEN"
        ) as packet:
            status, flags, _reserved = validate_response_prefix(
                packet.data,
                expected_bytes=_OPEN_RESPONSE_BYTES,
                expected_request_id=open_id,
                allowed_flags=0,
                operation="history OPEN",
            )
            if flags or packet.fds:
                raise ProtocolError(
                    "history OPEN returned flags or descriptors"
                )
            if status != OK:
                if any(packet.data[32:]):
                    raise ProtocolError(
                        "history OPEN error carries success metadata"
                    )
                if status == HISTORY_GENERATION_CHANGED:
                    raise HistoryGenerationChangedError(
                        "requested history generation changed"
                    )
                raise_status("history OPEN", status)
            read_token = struct.unpack_from("<Q", packet.data, 32)[0]
            if read_token == 0:
                raise ProtocolError(
                    "history OPEN returned zero read token"
                )
            generation = _parse_generation(packet.data, 40)
            if generation.instrument_id != instrument_id:
                raise ProtocolError(
                    "history OPEN returned another instrument"
                )
            if (
                expected_generation
                and generation.generation != expected_generation
            ):
                raise ProtocolError(
                    "history OPEN ignored expected_generation"
                )
            validate_same_session(
                generation.endpoint,
                run_id=expected_run_id,
                session_epoch=expected_session_epoch,
                trade_date=expected_trade_date,
                capacity=expected_capacity,
            )
        return HistoryCursor(
            channel,
            generation,
            read_token,
            requested_page_records,
        )
    except BaseException:
        channel.close()
        raise


__all__ = [
    "HistoryCursor",
    "HistoryCursorClosedError",
    "HistoryGeneration",
    "HistoryGenerationChangedError",
    "HistoryPage",
    "StreamNotFoundError",
    "StreamResourceExhaustedError",
]
