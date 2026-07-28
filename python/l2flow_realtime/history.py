"""Immutable Instrument Store history over the L2Flow control socket.

This module implements the Python side of the independent history protocol.
Opening a cursor pins one immutable Store generation in the server.  Every
non-terminal read returns one completed, read-only, sealed memfd page; a
successful zero-row response with the EOF flag is the only end-of-stream
marker.

The history payload is the ``core_v1`` projection.  It guarantees complete
record coverage for the selected instrument and generation, but it is not a
field-complete copy of the C++ Store payload.
"""

from __future__ import annotations

import array
import fcntl
import mmap
import os
import secrets
import socket
import stat
import struct
import sys
import threading
from dataclasses import dataclass, field, replace
from typing import Optional, Tuple, Union

from .models import (
    ClientClosedError,
    L2FlowRealtimeError,
    MarketEventKind,
    ProtocolError,
    Snapshot,
    StaleSessionError,
    Tick,
    UnavailableError,
    WireFormatError,
)
from .wire import (
    CONTROL_MAGIC,
    SNAPSHOT_BYTES,
    TICK_BYTES,
    TICK_PROJECTION_RAW_TICK_FLAG_OMITTED,
    TICK_PROJECTION_RAW_TYPE_OMITTED,
    WIRE_MAJOR,
    WIRE_MINOR,
    parse_snapshot,
    parse_tick,
)


HISTORY_MAGIC = b"L2FHST1\x00"
HISTORY_ENDIAN_MARKER = 0x01020304

OPEN_HISTORY_OPCODE = 2
READ_HISTORY_OPCODE = 3

CONTROL_OK = 0
CONTROL_INVALID_REQUEST = 1
CONTROL_UNSUPPORTED_VERSION = 2
CONTROL_UNAVAILABLE = 3
CONTROL_NOT_FOUND = 4
CONTROL_RESOURCE_EXHAUSTED = 5
CONTROL_INTERNAL_FAILURE = 6

HISTORY_RESPONSE_EOF = 1 << 0

HISTORY_GENERATION_COVERAGE_FROM_OPEN = 1 << 0
HISTORY_GENERATION_RECORD_COVERAGE_COMPLETE = 1 << 1
HISTORY_GENERATION_FIELD_COMPLETE = 1 << 2
_HISTORY_GENERATION_KNOWN_FLAGS = (
    HISTORY_GENERATION_COVERAGE_FROM_OPEN
    | HISTORY_GENERATION_RECORD_COVERAGE_COMPLETE
    | HISTORY_GENERATION_FIELD_COMPLETE
)

HISTORY_CORE_V1_PROJECTION = 1

HISTORY_PAYLOAD_SNAPSHOT = 1
HISTORY_PAYLOAD_TICK = 2

HISTORY_PROJECTION_RAW_TYPE_OMITTED = (
    TICK_PROJECTION_RAW_TYPE_OMITTED
)
HISTORY_PROJECTION_RAW_TICK_FLAG_OMITTED = (
    TICK_PROJECTION_RAW_TICK_FLAG_OMITTED
)
_HISTORY_DESCRIPTOR_KNOWN_PROJECTION_FLAGS = (
    HISTORY_PROJECTION_RAW_TYPE_OMITTED
    | HISTORY_PROJECTION_RAW_TICK_FLAG_OMITTED
)

HISTORY_PAGE_HEADER_BYTES = 4096
HISTORY_DESCRIPTOR_BYTES = 40
HISTORY_GENERATION_INFO_BYTES = 256
OPEN_HISTORY_REQUEST_BYTES = 64
OPEN_HISTORY_RESPONSE_BYTES = 296
READ_HISTORY_REQUEST_BYTES = 48
READ_HISTORY_RESPONSE_BYTES = 64

_UINT32_MAX = 0xFFFFFFFF
_UINT64_MAX = 0xFFFFFFFFFFFFFFFF
_DEFAULT_PAGE_RECORDS = 4096
_MAXIMUM_PAGE_RECORDS = 1024 * 1024

# All structures are little-endian and use the natural C offsets documented by
# the protocol.  Every asserted size is part of the cross-process ABI.
_CONTROL_RESPONSE_PREFIX = struct.Struct("<8sHHHHIIQ")
_OPEN_REQUEST = struct.Struct("<8sHHHHIIQQIIQQ")
_OPEN_RESPONSE_PREFIX = _CONTROL_RESPONSE_PREFIX
_READ_REQUEST = struct.Struct("<8sHHHHIIQQQ")
_READ_RESPONSE = struct.Struct("<8sHHHHIIQQQQQ")
_GENERATION_INFO = struct.Struct(
    "<16sQQIIIIQQQ32s32s4I4Q4QQII3Q"
)
_PAGE_PREFIX = struct.Struct("<8sHHIIIQQIIQQIIQIIQQ")
_DESCRIPTOR = struct.Struct("<QQQII4BI")

assert _CONTROL_RESPONSE_PREFIX.size == 32
assert _OPEN_REQUEST.size == OPEN_HISTORY_REQUEST_BYTES
assert _READ_REQUEST.size == READ_HISTORY_REQUEST_BYTES
assert _READ_RESPONSE.size == READ_HISTORY_RESPONSE_BYTES
assert _GENERATION_INFO.size == HISTORY_GENERATION_INFO_BYTES
assert _PAGE_PREFIX.size == 104
assert _DESCRIPTOR.size == HISTORY_DESCRIPTOR_BYTES


class HistoryNotFoundError(L2FlowRealtimeError):
    """The requested instrument is unknown to the generation registry."""


class HistoryResourceExhaustedError(L2FlowRealtimeError):
    """The server could not allocate another history cursor or page."""


class HistoryInternalFailureError(UnavailableError):
    """The server failed an internal history-projection invariant."""


@dataclass(frozen=True, slots=True)
class HistoryGeneration:
    """Identity and completeness boundary of one pinned Store generation."""

    run_id: bytes
    session_epoch: int
    generation: int
    trade_date: int
    instrument_id: int
    registry_ordinal: int
    instrument_count: int
    ingress_sequence_exclusive: int
    recv_monotonic_cut_ns: int
    registry_version: int
    registry_sha256: bytes
    input_identity_sha256: bytes
    source_stream_ids: Tuple[int, int, int, int]
    source_sequence_exclusive: Tuple[int, int, int, int]
    source_record_counts: Tuple[int, int, int, int]
    total_record_count: int
    flags: int
    payload_projection: int

    @property
    def coverage_from_open(self) -> bool:
        return (
            self.flags & HISTORY_GENERATION_COVERAGE_FROM_OPEN
        ) != 0

    @property
    def record_coverage_complete(self) -> bool:
        return (
            self.flags & HISTORY_GENERATION_RECORD_COVERAGE_COMPLETE
        ) != 0

    @property
    def field_complete(self) -> bool:
        return (
            self.flags & HISTORY_GENERATION_FIELD_COMPLETE
        ) != 0


@dataclass(frozen=True, slots=True)
class HistoryRecord:
    """One record restored to the Store cursor's mixed ingress order."""

    ingress_sequence: int
    source_sequence: int
    tick_stream_sequence: int
    payload_index: int
    projection_flags: int
    payload_kind: int
    event_kind: MarketEventKind
    source_slot: int
    value: Union[Snapshot, Tick]


@dataclass(frozen=True, slots=True)
class HistoryPage:
    """One immutable history page or the explicit zero-row EOF page."""

    generation: HistoryGeneration
    page_index: int
    records: Tuple[HistoryRecord, ...]
    eof: bool
    cumulative_record_count: int
    cumulative_source_record_counts: Tuple[int, int, int, int]

    def __post_init__(self) -> None:
        if self.eof and self.records:
            raise ValueError("an EOF history page must contain zero records")
        if self.cumulative_record_count < len(self.records):
            raise ValueError("history page cumulative count is invalid")

    def to_columns_by_kind(self):
        """Return homogeneous snapshot/tick columns for this mixed page."""

        from .batch import _history_columns_by_kind

        built = _history_columns_by_kind(
            self.records, self.generation, self.page_index
        )
        return {
            kind: columns
            for kind, (columns, _types, _records) in built.items()
        }

    def to_arrow_by_kind(self):
        """Return snapshot/tick Arrow RecordBatches keyed by kind."""

        from .batch import _history_columns_by_kind, _to_arrow

        built = _history_columns_by_kind(
            self.records, self.generation, self.page_index
        )
        return {
            kind: _to_arrow(columns, types)
            for kind, (columns, types, _records) in built.items()
        }

    def to_polars_by_kind(self):
        """Return snapshot/tick Polars frames keyed by kind.

        The two frames remain homogeneous; ``ingress_sequence`` is the stable
        key for reconstructing the mixed Store order when needed.
        """

        from .batch import _history_columns_by_kind, _to_polars

        built = _history_columns_by_kind(
            self.records, self.generation, self.page_index
        )
        return {
            kind: _to_polars(columns, types)
            for kind, (columns, types, _records) in built.items()
        }


@dataclass(frozen=True, slots=True)
class _HistoryPageLayout:
    """Validated offsets and counts for one canonical mapped history page."""

    total_bytes: int
    page_index: int
    record_count: int
    descriptor_offset: int
    snapshot_offset: int
    snapshot_count: int
    tick_offset: int
    tick_count: int
    first_ingress: int
    last_ingress: int


def _positive_uint64(value, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{field} must be an integer")
    if value <= 0 or value > _UINT64_MAX:
        raise ValueError(f"{field} must be a positive uint64")
    return value


def _positive_uint32(value, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{field} must be an integer")
    if value <= 0 or value > _UINT32_MAX:
        raise ValueError(f"{field} must be a positive uint32")
    return value


def _request_id(value: Optional[int] = None) -> int:
    return (
        _positive_uint64(value, "request_id")
        if value is not None
        else (secrets.randbits(64) or 1)
    )


def _validate_socket_path(
    control_socket_path: Union[str, os.PathLike],
) -> Union[str, bytes]:
    path = os.fspath(control_socket_path)
    if not isinstance(path, (str, bytes)):
        raise TypeError("control_socket_path must be path-like")
    nul = b"\x00" if isinstance(path, bytes) else "\x00"
    if not path or nul in path:
        raise ValueError("control_socket_path must be non-empty")
    if not os.path.isabs(path):
        raise ValueError("control_socket_path must be absolute")
    return path


def _validate_timeout(timeout: Optional[float]) -> Optional[float]:
    if timeout is not None:
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
            raise TypeError("timeout must be a number or None")
        if timeout <= 0:
            raise ValueError("timeout must be positive or None")
        return float(timeout)
    return None


def _close_fds(descriptors) -> None:
    for descriptor in descriptors:
        try:
            os.close(descriptor)
        except OSError:
            pass


def _recv_packet(
    channel: socket.socket,
    expected_bytes: int,
) -> Tuple[bytes, Tuple[int, ...]]:
    descriptor_array = array.array("i")
    ancillary_capacity = socket.CMSG_SPACE(descriptor_array.itemsize)
    recv_flags = getattr(socket, "MSG_CMSG_CLOEXEC", 0)
    data, ancillary, message_flags, _address = channel.recvmsg(
        expected_bytes, ancillary_capacity, recv_flags
    )
    received_fds = []
    try:
        unexpected_ancillary = False
        for level, kind, payload in ancillary:
            if level != socket.SOL_SOCKET or kind != socket.SCM_RIGHTS:
                unexpected_ancillary = True
                continue
            complete_bytes = (
                len(payload)
                - len(payload) % descriptor_array.itemsize
            )
            if complete_bytes != len(payload):
                unexpected_ancillary = True
            if complete_bytes:
                values = array.array("i")
                values.frombytes(payload[:complete_bytes])
                received_fds.extend(values.tolist())
        truncation_flags = getattr(socket, "MSG_TRUNC", 0) | getattr(
            socket, "MSG_CTRUNC", 0
        )
        if message_flags & truncation_flags:
            raise ProtocolError("truncated history control response")
        if unexpected_ancillary:
            raise ProtocolError(
                "unexpected or malformed history ancillary message"
            )
        if len(data) != expected_bytes:
            raise ProtocolError(
                "history response has "
                f"{len(data)} bytes; expected {expected_bytes}"
            )
        for descriptor in received_fds:
            os.set_inheritable(descriptor, False)
        result = tuple(received_fds)
        received_fds.clear()
        return data, result
    except Exception:
        _close_fds(received_fds)
        raise


def _send_packet(channel: socket.socket, payload: bytes) -> None:
    sent = channel.send(payload)
    if sent != len(payload):
        raise ProtocolError(
            f"short history request send: {sent} of {len(payload)}"
        )


def _raise_status(operation: str, status: int) -> None:
    if status == CONTROL_OK:
        return
    if status == CONTROL_INVALID_REQUEST:
        raise ProtocolError(f"{operation}: server rejected an invalid request")
    if status == CONTROL_UNSUPPORTED_VERSION:
        raise ProtocolError(
            f"{operation}: server does not support history protocol V1"
        )
    if status == CONTROL_UNAVAILABLE:
        raise UnavailableError(
            f"{operation}: no healthy Store generation is available"
        )
    if status == CONTROL_NOT_FOUND:
        raise HistoryNotFoundError(
            f"{operation}: instrument is unknown to the Store generation "
            "registry"
        )
    if status == CONTROL_RESOURCE_EXHAUSTED:
        raise HistoryResourceExhaustedError(
            f"{operation}: history service resources are exhausted"
        )
    if status == CONTROL_INTERNAL_FAILURE:
        raise HistoryInternalFailureError(
            f"{operation}: history service failed an internal invariant"
        )
    raise ProtocolError(f"{operation}: unknown control status {status}")


def _validate_response_prefix(
    data: bytes,
    *,
    expected_bytes: int,
    expected_request_id: int,
    allowed_flags: int,
    operation: str,
    require_reserved0_zero: bool = True,
) -> Tuple[int, int]:
    (
        magic,
        major,
        minor,
        status,
        flags,
        message_bytes,
        reserved0,
        request_id,
    ) = _CONTROL_RESPONSE_PREFIX.unpack_from(data)
    if magic != CONTROL_MAGIC:
        raise ProtocolError(f"{operation}: control magic mismatch")
    if major != WIRE_MAJOR or minor != WIRE_MINOR:
        raise ProtocolError(f"{operation}: protocol version mismatch")
    if message_bytes != expected_bytes:
        raise ProtocolError(f"{operation}: message_bytes mismatch")
    if require_reserved0_zero and reserved0 != 0:
        raise ProtocolError(f"{operation}: reserved field is nonzero")
    if request_id != expected_request_id:
        raise ProtocolError(f"{operation}: request_id mismatch")
    if flags & ~allowed_flags:
        raise ProtocolError(f"{operation}: unknown response flags")
    return status, flags


def _parse_generation_info(
    data: Union[bytes, mmap.mmap],
    offset: int,
) -> HistoryGeneration:
    fields = _GENERATION_INFO.unpack_from(data, offset)
    generation = HistoryGeneration(
        run_id=bytes(fields[0]),
        session_epoch=fields[1],
        generation=fields[2],
        trade_date=fields[3],
        instrument_id=fields[4],
        registry_ordinal=fields[5],
        instrument_count=fields[6],
        ingress_sequence_exclusive=fields[7],
        recv_monotonic_cut_ns=fields[8],
        registry_version=fields[9],
        registry_sha256=bytes(fields[10]),
        input_identity_sha256=bytes(fields[11]),
        source_stream_ids=tuple(fields[12:16]),
        source_sequence_exclusive=tuple(fields[16:20]),
        source_record_counts=tuple(fields[20:24]),
        total_record_count=fields[24],
        flags=fields[25],
        payload_projection=fields[26],
    )
    reserved = fields[27:30]
    if any(reserved):
        raise WireFormatError("history generation reserved fields are nonzero")
    if (
        not any(generation.run_id)
        or generation.session_epoch == 0
        or generation.generation == 0
        or generation.trade_date == 0
        or generation.instrument_id == 0
        or generation.instrument_count == 0
        or generation.registry_ordinal >= generation.instrument_count
        or generation.ingress_sequence_exclusive == 0
        or generation.registry_version == 0
        or not any(generation.registry_sha256)
        or not any(generation.input_identity_sha256)
    ):
        raise WireFormatError("history generation identity is invalid")
    if (
        len(set(generation.source_stream_ids)) != 4
        or any(value == 0 for value in generation.source_stream_ids)
        or any(
            value == 0
            for value in generation.source_sequence_exclusive
        )
    ):
        raise WireFormatError("history generation source watermark is invalid")
    if generation.flags & ~_HISTORY_GENERATION_KNOWN_FLAGS:
        raise WireFormatError("history generation has unknown flags")
    if not generation.record_coverage_complete:
        raise WireFormatError(
            "history generation does not guarantee complete record coverage"
        )
    if generation.field_complete:
        raise WireFormatError(
            "core_v1 history must not claim field-complete Store payloads"
        )
    if generation.payload_projection != HISTORY_CORE_V1_PROJECTION:
        raise WireFormatError("unsupported history payload projection")
    if sum(generation.source_record_counts) != generation.total_record_count:
        raise WireFormatError(
            "history instrument source counts do not match total count"
        )
    if (
        sum(
            sequence_exclusive - 1
            for sequence_exclusive in (
                generation.source_sequence_exclusive
            )
        )
        != generation.ingress_sequence_exclusive - 1
    ):
        raise WireFormatError(
            "history source watermarks do not reconcile with ingress cut"
        )
    if generation.total_record_count > (
        generation.ingress_sequence_exclusive - 1
    ):
        raise WireFormatError(
            "history instrument count exceeds the generation ingress prefix"
        )
    for count, sequence_exclusive in zip(
        generation.source_record_counts,
        generation.source_sequence_exclusive,
    ):
        if count > sequence_exclusive - 1:
            raise WireFormatError(
                "history instrument source count exceeds source watermark"
            )
    return generation


def _same_generation(
    left: HistoryGeneration,
    right: HistoryGeneration,
) -> bool:
    return left == right


def _validate_expected_generation(
    generation: HistoryGeneration,
    *,
    instrument_id: int,
    expected_run_id: Optional[bytes],
    expected_session_epoch: Optional[int],
    expected_trade_date: Optional[int],
    expected_instrument_count: Optional[int],
    expected_registry_version: Optional[int],
    expected_registry_sha256: Optional[bytes],
) -> None:
    if generation.instrument_id != instrument_id:
        raise WireFormatError(
            "history generation instrument_id does not match request"
        )
    if expected_run_id is not None and generation.run_id != expected_run_id:
        raise StaleSessionError("history service run_id changed")
    if (
        expected_session_epoch is not None
        and generation.session_epoch != expected_session_epoch
    ):
        raise StaleSessionError("history service session_epoch changed")
    if (
        expected_trade_date is not None
        and generation.trade_date != expected_trade_date
    ):
        raise StaleSessionError("history service trade_date changed")
    if (
        expected_instrument_count is not None
        and generation.instrument_count != expected_instrument_count
    ):
        raise StaleSessionError("history service instrument_count changed")
    if (
        expected_registry_version is not None
        and generation.registry_version != expected_registry_version
    ):
        raise StaleSessionError("history service registry_version changed")
    if (
        expected_registry_sha256 is not None
        and generation.registry_sha256 != expected_registry_sha256
    ):
        raise StaleSessionError("history service registry_sha256 changed")


def _history_tick_parser(payload: bytes) -> Tick:
    """History tick parse hook.

    The Store contract permits zero for ``tick_stream_sequence`` even though
    the live-ring parser requires a nonzero value.  For that one field, parse a
    private copy with a temporary nonzero sentinel and restore the exact Store
    value in the immutable model.  Every other byte still passes the existing
    strict core tick parser.
    """

    tick_stream_sequence = struct.unpack_from("<Q", payload, 32)[0]
    if tick_stream_sequence != 0:
        return parse_tick(payload)
    parseable = bytearray(payload)
    struct.pack_into("<Q", parseable, 32, 1)
    parsed = parse_tick(bytes(parseable))
    return replace(
        parsed,
        common=replace(parsed.common, tick_stream_sequence=0),
    )


def _validate_page_fd(fd: int, expected_bytes: int) -> None:
    if expected_bytes < HISTORY_PAGE_HEADER_BYTES:
        raise WireFormatError("history page is smaller than its header")
    if expected_bytes > sys.maxsize:
        raise WireFormatError("history page exceeds the local mmap size limit")
    descriptor_flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    if descriptor_flags & os.O_ACCMODE != os.O_RDONLY:
        raise WireFormatError("history page fd is not read-only")
    get_seals = getattr(fcntl, "F_GET_SEALS", 1034)
    seals = fcntl.fcntl(fd, get_seals)
    required_seals = (
        getattr(fcntl, "F_SEAL_WRITE", 0x0008)
        | getattr(fcntl, "F_SEAL_GROW", 0x0004)
        | getattr(fcntl, "F_SEAL_SHRINK", 0x0002)
        | getattr(fcntl, "F_SEAL_SEAL", 0x0001)
    )
    if seals & required_seals != required_seals:
        raise WireFormatError("history page fd lacks required immutable seals")
    descriptor_stat = os.fstat(fd)
    if (
        not stat.S_ISREG(descriptor_stat.st_mode)
        or descriptor_stat.st_size != expected_bytes
    ):
        raise WireFormatError("history page fd size or type is invalid")


def _validate_page_header(
    data: Union[bytes, bytearray, mmap.mmap],
    *,
    expected_bytes: int,
    expected_records: int,
    expected_page_index: int,
    generation: HistoryGeneration,
    prior_ingress_sequence: int,
) -> _HistoryPageLayout:
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
        descriptor_offset,
        snapshot_offset,
        snapshot_count,
        snapshot_payload_bytes,
        tick_offset,
        tick_count,
        tick_payload_bytes,
        first_ingress,
        last_ingress,
    ) = _PAGE_PREFIX.unpack_from(data)
    if magic != HISTORY_MAGIC:
        raise WireFormatError("history page magic mismatch")
    if major != WIRE_MAJOR or minor != WIRE_MINOR:
        raise WireFormatError("history page protocol version mismatch")
    if header_bytes != HISTORY_PAGE_HEADER_BYTES:
        raise WireFormatError("history page header_bytes mismatch")
    if endian_marker != HISTORY_ENDIAN_MARKER:
        raise WireFormatError("history page endian marker mismatch")
    if flags != 0:
        raise WireFormatError("history page has unknown flags")
    if total_bytes != expected_bytes:
        raise WireFormatError("history page total_bytes mismatch")
    if page_index != expected_page_index:
        raise WireFormatError("history page index mismatch")
    if record_count != expected_records or record_count == 0:
        raise WireFormatError("history data page record_count mismatch")
    if descriptor_bytes != HISTORY_DESCRIPTOR_BYTES:
        raise WireFormatError("history descriptor stride mismatch")
    if snapshot_payload_bytes != SNAPSHOT_BYTES:
        raise WireFormatError("history snapshot payload stride mismatch")
    if tick_payload_bytes != TICK_BYTES:
        raise WireFormatError("history tick payload stride mismatch")
    if snapshot_count + tick_count != record_count:
        raise WireFormatError("history page payload counts mismatch")

    expected_descriptor_offset = HISTORY_PAGE_HEADER_BYTES
    expected_snapshot_offset = (
        expected_descriptor_offset + record_count * HISTORY_DESCRIPTOR_BYTES
    )
    expected_tick_offset = (
        expected_snapshot_offset + snapshot_count * SNAPSHOT_BYTES
    )
    expected_total_bytes = expected_tick_offset + tick_count * TICK_BYTES
    if (
        descriptor_offset != expected_descriptor_offset
        or snapshot_offset != expected_snapshot_offset
        or tick_offset != expected_tick_offset
        or total_bytes != expected_total_bytes
    ):
        raise WireFormatError("history page regions are not canonical")
    if first_ingress == 0 or last_ingress < first_ingress:
        raise WireFormatError("history page ingress bounds are invalid")
    if first_ingress <= prior_ingress_sequence:
        raise WireFormatError(
            "history ingress order did not advance across pages"
        )

    page_generation = _parse_generation_info(data, 104)
    if not _same_generation(page_generation, generation):
        raise WireFormatError(
            "history page belongs to another Store generation"
        )
    if any(data[360:HISTORY_PAGE_HEADER_BYTES]):
        raise WireFormatError("history page reserved bytes are nonzero")
    return _HistoryPageLayout(
        total_bytes=total_bytes,
        page_index=page_index,
        record_count=record_count,
        descriptor_offset=descriptor_offset,
        snapshot_offset=snapshot_offset,
        snapshot_count=snapshot_count,
        tick_offset=tick_offset,
        tick_count=tick_count,
        first_ingress=first_ingress,
        last_ingress=last_ingress,
    )


def _decode_page_objects(
    data: Union[bytes, bytearray, mmap.mmap],
    *,
    layout: _HistoryPageLayout,
    generation: HistoryGeneration,
    prior_ingress_sequence: int,
    prior_source_sequences: Tuple[int, int, int, int],
) -> Tuple[
    Tuple[HistoryRecord, ...],
    Tuple[int, int, int, int],
    int,
    Tuple[int, int, int, int],
]:
    records = []
    source_counts = [0, 0, 0, 0]
    next_snapshot_index = 0
    next_tick_index = 0
    last_seen = prior_ingress_sequence
    last_source_sequences = list(prior_source_sequences)
    for index in range(layout.record_count):
        descriptor_position = (
            layout.descriptor_offset + index * HISTORY_DESCRIPTOR_BYTES
        )
        descriptor = _DESCRIPTOR.unpack_from(data, descriptor_position)
        (
            ingress_sequence,
            source_sequence,
            tick_stream_sequence,
            payload_index,
            projection_flags,
            payload_kind,
            event_kind_raw,
            source_slot,
            reserved_byte,
            reserved_word,
        ) = descriptor
        if reserved_byte != 0 or reserved_word != 0:
            raise WireFormatError(
                "history descriptor reserved fields are nonzero"
            )
        if (
            ingress_sequence == 0
            or source_sequence == 0
            or ingress_sequence <= last_seen
            or ingress_sequence >= generation.ingress_sequence_exclusive
        ):
            raise WireFormatError(
                "history descriptor sequence is outside the generation"
            )
        if source_slot >= 4:
            raise WireFormatError(
                "history descriptor source_slot exceeds four"
            )
        if (
            source_sequence
            >= generation.source_sequence_exclusive[source_slot]
            or source_sequence <= last_source_sequences[source_slot]
        ):
            raise WireFormatError(
                "history source sequence is not strictly ordered "
                "within its watermark"
            )
        if (
            projection_flags
            & ~_HISTORY_DESCRIPTOR_KNOWN_PROJECTION_FLAGS
        ):
            raise WireFormatError(
                "history descriptor has unknown projection flags"
            )
        try:
            event_kind = MarketEventKind(event_kind_raw)
        except ValueError as error:
            raise WireFormatError(
                "history descriptor has unsupported event_kind"
            ) from error
        expected_source_slot = {
            MarketEventKind.SHANGHAI_SNAPSHOT: 0,
            MarketEventKind.SHANGHAI_TICK: 1,
            MarketEventKind.SHENZHEN_SNAPSHOT: 2,
            MarketEventKind.SHENZHEN_ORDER: 3,
            MarketEventKind.SHENZHEN_TRANSACTION: 3,
        }[event_kind]
        if source_slot != expected_source_slot:
            raise WireFormatError(
                "history descriptor kind and source_slot disagree"
            )
        if (
            projection_flags != 0
            and event_kind is not MarketEventKind.SHANGHAI_TICK
        ):
            raise WireFormatError(
                "history raw-string omission flags require Shanghai tick"
            )

        if payload_kind == HISTORY_PAYLOAD_SNAPSHOT:
            if (
                event_kind
                not in (
                    MarketEventKind.SHANGHAI_SNAPSHOT,
                    MarketEventKind.SHENZHEN_SNAPSHOT,
                )
                or projection_flags != 0
                or tick_stream_sequence != 0
                or payload_index != next_snapshot_index
            ):
                raise WireFormatError(
                    "history snapshot descriptor is inconsistent"
                )
            payload_position = (
                layout.snapshot_offset + payload_index * SNAPSHOT_BYTES
            )
            payload = bytes(
                data[payload_position : payload_position + SNAPSHOT_BYTES]
            )
            value: Union[Snapshot, Tick] = parse_snapshot(payload)
            next_snapshot_index += 1
        elif payload_kind == HISTORY_PAYLOAD_TICK:
            if (
                event_kind
                not in (
                    MarketEventKind.SHANGHAI_TICK,
                    MarketEventKind.SHENZHEN_ORDER,
                    MarketEventKind.SHENZHEN_TRANSACTION,
                )
                or payload_index != next_tick_index
                or tick_stream_sequence == _UINT64_MAX
                or tick_stream_sequence > ingress_sequence
            ):
                raise WireFormatError(
                    "history tick descriptor is inconsistent"
                )
            payload_position = (
                layout.tick_offset + payload_index * TICK_BYTES
            )
            payload = bytes(
                data[payload_position : payload_position + TICK_BYTES]
            )
            if (
                projection_flags & HISTORY_PROJECTION_RAW_TYPE_OMITTED
            ) and (payload[165] != 0 or any(payload[272:304])):
                raise WireFormatError(
                    "history raw_type omission flag conflicts with payload"
                )
            if (
                projection_flags
                & HISTORY_PROJECTION_RAW_TICK_FLAG_OMITTED
            ) and (payload[166] != 0 or any(payload[304:336])):
                raise WireFormatError(
                    "history raw_tick_flag omission flag conflicts "
                    "with payload"
                )
            value = _history_tick_parser(payload)
            if value.projection_flags != projection_flags:
                raise WireFormatError(
                    "history descriptor and tick projection flags disagree"
                )
            next_tick_index += 1
        else:
            raise WireFormatError(
                "history descriptor has unsupported payload_kind"
            )

        common = value.common
        if (
            common.instrument_id != generation.instrument_id
            or common.registry_ordinal != generation.registry_ordinal
            or common.trade_date != generation.trade_date
            or common.event_kind is not event_kind
            or common.source_slot != source_slot
            or common.source_stream_id
            != generation.source_stream_ids[source_slot]
            or common.ingress_sequence != ingress_sequence
            or common.source_sequence != source_sequence
            or common.tick_stream_sequence != tick_stream_sequence
        ):
            raise WireFormatError(
                "history descriptor and payload identity disagree"
            )
        records.append(
            HistoryRecord(
                ingress_sequence=ingress_sequence,
                source_sequence=source_sequence,
                tick_stream_sequence=tick_stream_sequence,
                payload_index=payload_index,
                projection_flags=projection_flags,
                payload_kind=payload_kind,
                event_kind=event_kind,
                source_slot=source_slot,
                value=value,
            )
        )
        source_counts[source_slot] += 1
        last_source_sequences[source_slot] = source_sequence
        last_seen = ingress_sequence

    if (
        next_snapshot_index != layout.snapshot_count
        or next_tick_index != layout.tick_count
    ):
        raise WireFormatError(
            "history descriptor payload indices are not dense"
        )
    if records[0].ingress_sequence != layout.first_ingress:
        raise WireFormatError(
            "history page first ingress does not match descriptors"
        )
    if records[-1].ingress_sequence != layout.last_ingress:
        raise WireFormatError(
            "history page last ingress does not match descriptors"
        )
    return (
        tuple(records),
        tuple(source_counts),
        last_seen,
        tuple(last_source_sequences),
    )


def _parse_page(
    fd: int,
    *,
    expected_bytes: int,
    expected_records: int,
    expected_page_index: int,
    generation: HistoryGeneration,
    prior_ingress_sequence: int,
    prior_source_sequences: Tuple[int, int, int, int],
) -> Tuple[
    Tuple[HistoryRecord, ...],
    Tuple[int, int, int, int],
    int,
    Tuple[int, int, int, int],
]:
    _validate_page_fd(fd, expected_bytes)
    mapping = mmap.mmap(fd, expected_bytes, access=mmap.ACCESS_READ)
    try:
        layout = _validate_page_header(
            mapping,
            expected_bytes=expected_bytes,
            expected_records=expected_records,
            expected_page_index=expected_page_index,
            generation=generation,
            prior_ingress_sequence=prior_ingress_sequence,
        )
        return _decode_page_objects(
            mapping,
            layout=layout,
            generation=generation,
            prior_ingress_sequence=prior_ingress_sequence,
            prior_source_sequences=prior_source_sequences,
        )
    finally:
        mapping.close()


@dataclass(slots=True)
class HistoryCursor:
    """Stateful client cursor pinned to one immutable Store generation."""

    _channel: socket.socket
    generation: HistoryGeneration
    requested_page_records: int
    _next_page_index: int = 0
    _cumulative_record_count: int = 0
    _cumulative_source_counts: Tuple[int, int, int, int] = (0, 0, 0, 0)
    _last_ingress_sequence: int = 0
    _last_source_sequences: Tuple[int, int, int, int] = (0, 0, 0, 0)
    _read_token: int = 0
    _closed: bool = False
    _eof: bool = False
    _lock: threading.Lock = field(
        default_factory=threading.Lock, repr=False
    )

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def done(self) -> bool:
        return self._eof

    @property
    def next_page_index(self) -> int:
        return self._next_page_index

    @property
    def cumulative_record_count(self) -> int:
        return self._cumulative_record_count

    def _require_open(self) -> None:
        if self._closed:
            raise ClientClosedError("history cursor is closed")
        if self._eof:
            raise ClientClosedError("history cursor is already at EOF")

    def close(self) -> None:
        # shutdown is deliberately outside the cursor lock: read() holds that
        # lock across blocking socket I/O, and timeout=None must still be
        # cancellable from another thread. Python keeps a closed socket
        # object's descriptor at -1, so a concurrent idempotent shutdown
        # cannot target an unrelated descriptor that the OS later reuses.
        try:
            self._channel.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        with self._lock:
            if not self._closed:
                self._closed = True
                self._channel.close()

    def __enter__(self) -> "HistoryCursor":
        self._require_open()
        return self

    def __exit__(self, _type, _value, _traceback) -> None:
        self.close()

    def read(self) -> HistoryPage:
        """Read one data page or the explicit zero-row EOF page."""

        with self._lock:
            self._require_open()
            if self._next_page_index > _UINT64_MAX:
                self._closed = True
                self._channel.close()
                raise UnavailableError(
                    "history page index space is exhausted"
                )
            request_id = _request_id()
            request = _READ_REQUEST.pack(
                CONTROL_MAGIC,
                WIRE_MAJOR,
                WIRE_MINOR,
                READ_HISTORY_OPCODE,
                0,
                READ_HISTORY_REQUEST_BYTES,
                0,
                request_id,
                self._next_page_index,
                self._read_token,
            )
            try:
                _send_packet(self._channel, request)
                response, fds = _recv_packet(
                    self._channel, READ_HISTORY_RESPONSE_BYTES
                )
            except Exception:
                self._closed = True
                self._channel.close()
                raise

            try:
                status, response_flags = _validate_response_prefix(
                    response,
                    expected_bytes=READ_HISTORY_RESPONSE_BYTES,
                    expected_request_id=request_id,
                    allowed_flags=HISTORY_RESPONSE_EOF,
                    operation="read_history",
                    require_reserved0_zero=False,
                )
                (
                    _magic,
                    _major,
                    _minor,
                    _status,
                    _flags,
                    _message_bytes,
                    row_count,
                    _request_id_value,
                    page_bytes,
                    page_index,
                    generation_number,
                    next_read_token,
                ) = _READ_RESPONSE.unpack(response)
                if status != CONTROL_OK:
                    if fds:
                        raise ProtocolError(
                            "failed history response unexpectedly carried fd"
                        )
                    if (
                        response_flags != 0
                        or row_count != 0
                        or page_bytes != 0
                        or page_index != 0
                        or generation_number != 0
                        or next_read_token != 0
                    ):
                        raise ProtocolError(
                            "failed history response carried success metadata"
                        )
                    _raise_status("read_history", status)
                if page_index != self._next_page_index:
                    raise ProtocolError(
                        "history read response page_index mismatch"
                    )
                if generation_number != self.generation.generation:
                    raise ProtocolError(
                        "history read response generation mismatch"
                    )
                eof = (response_flags & HISTORY_RESPONSE_EOF) != 0
                if eof:
                    if (
                        row_count != 0
                        or page_bytes != 0
                        or fds
                        or next_read_token != 0
                    ):
                        raise ProtocolError(
                            "history EOF must be zero-row and carry no fd"
                        )
                    if (
                        self._cumulative_record_count
                        != self.generation.total_record_count
                        or self._cumulative_source_counts
                        != self.generation.source_record_counts
                    ):
                        raise WireFormatError(
                            "history EOF arrived before all generation "
                            "records were reconciled"
                        )
                    self._eof = True
                    self._next_page_index += 1
                    self._closed = True
                    self._channel.close()
                    return HistoryPage(
                        generation=self.generation,
                        page_index=page_index,
                        records=(),
                        eof=True,
                        cumulative_record_count=(
                            self._cumulative_record_count
                        ),
                        cumulative_source_record_counts=(
                            self._cumulative_source_counts
                        ),
                    )

                if (
                    row_count == 0
                    or row_count > self.requested_page_records
                    or page_bytes < HISTORY_PAGE_HEADER_BYTES
                    or page_bytes
                    > HISTORY_PAGE_HEADER_BYTES
                    + row_count
                    * (HISTORY_DESCRIPTOR_BYTES + SNAPSHOT_BYTES)
                    or len(fds) != 1
                    or next_read_token == 0
                    or next_read_token == self._read_token
                ):
                    raise ProtocolError(
                        "history data response row/fd metadata is invalid"
                    )
                (
                    records,
                    source_counts,
                    last_ingress,
                    last_source_sequences,
                ) = _parse_page(
                    fds[0],
                    expected_bytes=page_bytes,
                    expected_records=row_count,
                    expected_page_index=self._next_page_index,
                    generation=self.generation,
                    prior_ingress_sequence=self._last_ingress_sequence,
                    prior_source_sequences=self._last_source_sequences,
                )
                new_total = self._cumulative_record_count + len(records)
                new_source_counts = tuple(
                    current + added
                    for current, added in zip(
                        self._cumulative_source_counts, source_counts
                    )
                )
                if new_total > self.generation.total_record_count:
                    raise WireFormatError(
                        "history pages exceed generation record count"
                    )
                if any(
                    current > expected
                    for current, expected in zip(
                        new_source_counts,
                        self.generation.source_record_counts,
                    )
                ):
                    raise WireFormatError(
                        "history pages exceed generation source counts"
                    )
                self._cumulative_record_count = new_total
                self._cumulative_source_counts = new_source_counts
                self._last_ingress_sequence = last_ingress
                self._last_source_sequences = last_source_sequences
                self._read_token = next_read_token
                self._next_page_index += 1
                return HistoryPage(
                    generation=self.generation,
                    page_index=page_index,
                    records=records,
                    eof=False,
                    cumulative_record_count=new_total,
                    cumulative_source_record_counts=new_source_counts,
                )
            except Exception:
                self._closed = True
                self._channel.close()
                raise
            finally:
                _close_fds(fds)

    def pages(self):
        """Yield data pages followed by the explicit zero-row EOF page."""

        while not self.done:
            yield self.read()

    def records(self):
        """Yield every record without materializing the complete history."""

        for page in self.pages():
            yield from page.records


def open_history_cursor(
    control_socket_path: Union[str, os.PathLike],
    instrument_id: int,
    *,
    requested_page_records: int = _DEFAULT_PAGE_RECORDS,
    timeout: Optional[float] = 1.0,
    request_id: Optional[int] = None,
    expected_run_id: Optional[bytes] = None,
    expected_session_epoch: Optional[int] = None,
    expected_trade_date: Optional[int] = None,
    expected_instrument_count: Optional[int] = None,
    expected_registry_version: Optional[int] = None,
    expected_registry_sha256: Optional[bytes] = None,
) -> HistoryCursor:
    """Connect and pin the latest healthy Store generation for one instrument."""

    path = _validate_socket_path(control_socket_path)
    instrument_id = _positive_uint32(instrument_id, "instrument_id")
    requested_page_records = _positive_uint32(
        requested_page_records, "requested_page_records"
    )
    if requested_page_records > _MAXIMUM_PAGE_RECORDS:
        raise ValueError(
            "requested_page_records exceeds the Store absolute batch limit"
        )
    timeout = _validate_timeout(timeout)
    request_id = _request_id(request_id)

    if expected_run_id is not None:
        expected_run_id = bytes(expected_run_id)
        if len(expected_run_id) != 16 or not any(expected_run_id):
            raise ValueError("expected_run_id must be a nonzero 16-byte value")
    if expected_session_epoch is not None:
        expected_session_epoch = _positive_uint64(
            expected_session_epoch, "expected_session_epoch"
        )
    if expected_trade_date is not None:
        expected_trade_date = _positive_uint32(
            expected_trade_date, "expected_trade_date"
        )
    if expected_instrument_count is not None:
        expected_instrument_count = _positive_uint32(
            expected_instrument_count, "expected_instrument_count"
        )
    if expected_registry_version is not None:
        expected_registry_version = _positive_uint64(
            expected_registry_version, "expected_registry_version"
        )
    if expected_registry_sha256 is not None:
        expected_registry_sha256 = bytes(expected_registry_sha256)
        if (
            len(expected_registry_sha256) != 32
            or not any(expected_registry_sha256)
        ):
            raise ValueError(
                "expected_registry_sha256 must be a nonzero 32-byte value"
            )
    request = _OPEN_REQUEST.pack(
        CONTROL_MAGIC,
        WIRE_MAJOR,
        WIRE_MINOR,
        OPEN_HISTORY_OPCODE,
        0,
        OPEN_HISTORY_REQUEST_BYTES,
        0,
        request_id,
        0,
        instrument_id,
        requested_page_records,
        0,
        0,
    )
    channel = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    try:
        channel.settimeout(timeout)
        channel.connect(path)
        _send_packet(channel, request)
        response, fds = _recv_packet(
            channel, OPEN_HISTORY_RESPONSE_BYTES
        )
        try:
            status, flags = _validate_response_prefix(
                response,
                expected_bytes=OPEN_HISTORY_RESPONSE_BYTES,
                expected_request_id=request_id,
                allowed_flags=0,
                operation="open_history",
            )
            if flags != 0:
                raise ProtocolError(
                    "history open response flags must be zero"
                )
            if fds:
                raise ProtocolError(
                    "history open response must not carry an fd"
                )
            if status != CONTROL_OK and any(response[32:]):
                raise ProtocolError(
                    "failed history open response carried generation metadata"
                )
            _raise_status("open_history", status)
            initial_read_token = struct.unpack_from("<Q", response, 32)[0]
            if initial_read_token == 0:
                raise ProtocolError(
                    "successful history open response has zero read token"
                )
            generation = _parse_generation_info(response, 40)
            _validate_expected_generation(
                generation,
                instrument_id=instrument_id,
                expected_run_id=expected_run_id,
                expected_session_epoch=expected_session_epoch,
                expected_trade_date=expected_trade_date,
                expected_instrument_count=expected_instrument_count,
                expected_registry_version=expected_registry_version,
                expected_registry_sha256=expected_registry_sha256,
            )
        finally:
            _close_fds(fds)
        return HistoryCursor(
            _channel=channel,
            generation=generation,
            requested_page_records=requested_page_records,
            _read_token=initial_read_token,
        )
    except Exception:
        channel.close()
        raise


def connect_history(
    control_socket_path: Union[str, os.PathLike],
    instrument_id: int,
    **kwargs,
) -> HistoryCursor:
    """Alias for :func:`open_history_cursor`."""

    return open_history_cursor(
        control_socket_path, instrument_id, **kwargs
    )


__all__ = [
    "HistoryCursor",
    "HistoryGeneration",
    "HistoryInternalFailureError",
    "HistoryNotFoundError",
    "HistoryPage",
    "HistoryRecord",
    "HistoryResourceExhaustedError",
    "connect_history",
    "open_history_cursor",
]
