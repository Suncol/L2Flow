"""Fixed V2 protocol for the isolated Python history worker.

The worker control socket transfers commands and slot ownership only.  The
shared memfd contains a fixed, columnar numeric result schema.  Raw Wire V2
history pages and their file descriptors are never valid payloads for this
protocol.
"""

from __future__ import annotations

import array
import hashlib
import mmap
import os
import socket
import struct
import sys
import time
from dataclasses import dataclass
from enum import IntEnum
from typing import Mapping, Sequence

from ._generation import DailyCatalogSessionIdentity
from ._stream_control import UINT64_MAX
from .checkpoint import CHECKPOINT_BYTES
from .models import CatalogScope, ProtocolError, WireFormatError


CONTROL_MAGIC = b"L2FHWK2\x00"
RING_MAGIC = b"L2FHRR2\x00"
SLOT_MAGIC = b"L2FHRS2\x00"
PROTOCOL_MAJOR = 2
PROTOCOL_MINOR = 1
CONTROL_PACKET_BYTES = 512
CONTROL_PAYLOAD_OFFSET = 80
CONTROL_PAYLOAD_BYTES = CONTROL_PACKET_BYTES - CONTROL_PAYLOAD_OFFSET
RING_HEADER_BYTES = 4096
SLOT_HEADER_BYTES = 4096
SLOT_SCHEMA_DIGEST_OFFSET = 192
SLOT_CHECKPOINT_OFFSET = 256
MAX_CONTROL_PATH_BYTES = 107
MAX_RING_SLOTS = 64
MAX_RESULT_BATCH_RECORDS = 65_536
MAX_RING_BYTES = 1 << 30
NO_SLOT = 0xFFFFFFFF
NO_TIMEOUT_NS = UINT64_MAX

SLOT_FLAG_DATA = 1 << 0
SLOT_FLAG_EOF = 1 << 1


class WorkerOpcode(IntEnum):
    INIT = 1
    READY = 2
    OPEN = 3
    OPENED = 4
    RESULT_READY = 5
    COMPLETE = 6
    RELEASE = 7
    CANCEL = 8
    CANCELED = 9
    STOP = 10
    STOPPED = 11
    ERROR = 12


class WorkerStatus(IntEnum):
    OK = 0
    INVALID_REQUEST = 1
    STALE_SESSION = 2
    UNAVAILABLE = 3
    WIRE_FAILURE = 4
    INTERNAL_FAILURE = 5


@dataclass(frozen=True, slots=True)
class ResultColumnSpec:
    name: str
    format: str
    width: int


# This is the only result-row schema.  A command chooses a projection mask,
# but every column retains the same bit, scalar type, and slot offset.  Raw
# vendor strings and the original 336-byte Wire V2 tick row are deliberately
# absent.
_RESULT_COLUMN_LAYOUT = (
    ("record_schema_version", "I"),
    ("record_bytes", "I"),
    ("instrument_id", "I"),
    ("ordinal", "I"),
    ("source_sequence", "Q"),
    ("ingress_sequence", "Q"),
    ("tick_stream_sequence", "Q"),
    ("vendor_sequence_id", "Q"),
    ("event_time_unix_ns", "q"),
    ("recv_realtime_ns", "q"),
    ("recv_monotonic_ns", "q"),
    ("exchange_time_ns_since_midnight", "Q"),
    ("quality_flags", "Q"),
    ("market_notices", "Q"),
    ("source_stream_id", "I"),
    ("trade_date", "I"),
    ("vendor_local_time_raw", "I"),
    ("source_slot", "B"),
    ("event_kind", "B"),
    ("market", "B"),
    ("quantity_unit", "B"),
    ("security_type", "B"),
    ("asset_scope", "B"),
    ("validity_bitmap", "I"),
    ("projection_flags", "I"),
    ("channel", "q"),
    ("native_event_sequence", "q"),
    ("source_raw_code_1", "i"),
    ("source_raw_code_2", "i"),
    ("action", "B"),
    ("side", "B"),
    ("order_type", "B"),
    ("aggressor", "B"),
    ("phase", "B"),
    ("primary_order_id", "q"),
    ("buy_order_id", "q"),
    ("sell_order_id", "q"),
    ("price_raw", "q"),
    ("price_p6", "q"),
    ("price_scale", "B"),
    ("price_valid", "B"),
    ("price_is_null", "B"),
    ("quantity_raw", "q"),
    ("quantity_scale", "B"),
    ("quantity_valid", "B"),
    ("quantity_is_null", "B"),
    ("trade_amount_raw", "q"),
    ("trade_amount_p6", "q"),
    ("trade_amount_scale", "B"),
    ("trade_amount_valid", "B"),
    ("trade_amount_is_null", "B"),
    ("matched_quantity_raw", "q"),
    ("matched_quantity_scale", "B"),
    ("matched_quantity_valid", "B"),
    ("matched_quantity_is_null", "B"),
)


def _scalar_width(format_: str) -> int:
    width = struct.calcsize("<" + format_)
    native = array.array(format_)
    if native.itemsize != width:
        raise RuntimeError(
            f"native array width for {format_!r} is not {width}"
        )
    return width


RESULT_COLUMN_SPECS = tuple(
    ResultColumnSpec(name, format_, _scalar_width(format_))
    for name, format_ in _RESULT_COLUMN_LAYOUT
)
ALL_RESULT_COLUMNS = tuple(
    spec.name for spec in RESULT_COLUMN_SPECS
)
RESULT_COLUMN_BY_NAME = {
    spec.name: spec for spec in RESULT_COLUMN_SPECS
}
RESULT_COLUMN_INDEX = {
    spec.name: index for index, spec in enumerate(RESULT_COLUMN_SPECS)
}
if len(RESULT_COLUMN_SPECS) > 64:
    raise RuntimeError("history worker result schema exceeds uint64 mask")

RESULT_SCHEMA_DIGEST = hashlib.sha256(
    (
        "l2flow-history-worker-result-v2\0"
        + "\0".join(
            f"{index}:{spec.name}:{spec.format}:{spec.width}"
            for index, spec in enumerate(RESULT_COLUMN_SPECS)
        )
    ).encode("ascii")
).digest()

DEFAULT_RESULT_COLUMNS = (
    "ingress_sequence",
    "tick_stream_sequence",
    "price_p6",
    "price_valid",
    "price_is_null",
)

_CONTROL_PREFIX = struct.Struct("<8sHHHHIQQII4Q")
_RING_PREFIX = struct.Struct("<8sHHIQIIIQQ32s")
_SLOT_PREFIX = struct.Struct("<8sHHIIII18Q")
_INIT_PREFIX = struct.Struct("<16s32sQQQIIIIIIQQHH")
_OPEN_PREFIX = struct.Struct("<IIQII8x")

assert _CONTROL_PREFIX.size == 76
assert _RING_PREFIX.size == 84
assert _SLOT_PREFIX.size == 172
assert _INIT_PREFIX.size == 116
assert _OPEN_PREFIX.size == 32
assert SLOT_CHECKPOINT_OFFSET + CHECKPOINT_BYTES <= SLOT_HEADER_BYTES


def _align_up(value: int, alignment: int) -> int:
    return (value + alignment - 1) // alignment * alignment


def _positive_int(
    value: object, field: str, maximum: int
) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{field} must be an integer")
    if value <= 0 or value > maximum:
        raise ValueError(
            f"{field} must be in the inclusive range [1, {maximum}]"
        )
    return value


def column_mask(columns: Sequence[str]) -> int:
    if isinstance(columns, (str, bytes, bytearray)):
        raise TypeError("result columns must be a sequence of names")
    names = tuple(columns)
    if not names:
        raise ValueError("at least one result column is required")
    if len(set(names)) != len(names):
        raise ValueError("result column names must be unique")
    mask = 0
    for name in names:
        if not isinstance(name, str):
            raise TypeError("result column names must be strings")
        try:
            index = RESULT_COLUMN_INDEX[name]
        except KeyError:
            raise KeyError(name) from None
        mask |= 1 << index
    return mask


def column_names(mask: int) -> tuple[str, ...]:
    if (
        not isinstance(mask, int)
        or isinstance(mask, bool)
        or mask <= 0
        or mask >> len(RESULT_COLUMN_SPECS)
    ):
        raise ValueError("result column mask is invalid")
    return tuple(
        spec.name
        for index, spec in enumerate(RESULT_COLUMN_SPECS)
        if mask & (1 << index)
    )


@dataclass(frozen=True, slots=True)
class RingLayout:
    slot_count: int
    batch_capacity: int
    slot_stride: int
    total_bytes: int
    column_offsets: Mapping[str, int]

    def slot_base(self, slot_index: int) -> int:
        if (
            not isinstance(slot_index, int)
            or isinstance(slot_index, bool)
            or slot_index < 0
            or slot_index >= self.slot_count
        ):
            raise ValueError("result slot index is out of range")
        return RING_HEADER_BYTES + slot_index * self.slot_stride


def make_ring_layout(
    slot_count: int, batch_capacity: int
) -> RingLayout:
    slot_count = _positive_int(
        slot_count, "ring_slots", MAX_RING_SLOTS
    )
    batch_capacity = _positive_int(
        batch_capacity,
        "result_batch_records",
        MAX_RESULT_BATCH_RECORDS,
    )
    offset = SLOT_HEADER_BYTES
    offsets: dict[str, int] = {}
    for spec in RESULT_COLUMN_SPECS:
        offset = _align_up(offset, 64)
        offsets[spec.name] = offset
        offset += spec.width * batch_capacity
    slot_stride = _align_up(offset, RING_HEADER_BYTES)
    total_bytes = RING_HEADER_BYTES + slot_count * slot_stride
    if total_bytes > MAX_RING_BYTES:
        raise ValueError("history worker result ring exceeds 1 GiB")
    return RingLayout(
        slot_count=slot_count,
        batch_capacity=batch_capacity,
        slot_stride=slot_stride,
        total_bytes=total_bytes,
        column_offsets=offsets,
    )


@dataclass(frozen=True, slots=True)
class ControlPacket:
    opcode: WorkerOpcode
    status: WorkerStatus
    request_id: int
    transfer_sequence: int
    slot_index: int
    record_count: int
    args: tuple[int, int, int, int]
    payload: bytes


def pack_control(
    opcode: WorkerOpcode,
    *,
    status: WorkerStatus = WorkerStatus.OK,
    request_id: int = 0,
    transfer_sequence: int = 0,
    slot_index: int = NO_SLOT,
    record_count: int = 0,
    args: tuple[int, int, int, int] = (0, 0, 0, 0),
    payload: bytes = b"",
) -> bytes:
    try:
        opcode = WorkerOpcode(opcode)
        status = WorkerStatus(status)
    except ValueError as error:
        raise ValueError("unknown history worker opcode/status") from error
    if len(args) != 4:
        raise ValueError("control args must contain four uint64 values")
    integers = (
        request_id,
        transfer_sequence,
        *args,
    )
    if any(
        not isinstance(value, int)
        or isinstance(value, bool)
        or value < 0
        or value > UINT64_MAX
        for value in integers
    ):
        raise ValueError("control uint64 field is out of range")
    if (
        not isinstance(slot_index, int)
        or isinstance(slot_index, bool)
        or slot_index < 0
        or slot_index > 0xFFFFFFFF
        or not isinstance(record_count, int)
        or isinstance(record_count, bool)
        or record_count < 0
        or record_count > 0xFFFFFFFF
    ):
        raise ValueError("control uint32 field is out of range")
    if not isinstance(payload, bytes):
        raise TypeError("control payload must be bytes")
    if len(payload) > CONTROL_PAYLOAD_BYTES:
        raise ValueError("control payload exceeds the fixed packet")
    packet = bytearray(CONTROL_PACKET_BYTES)
    _CONTROL_PREFIX.pack_into(
        packet,
        0,
        CONTROL_MAGIC,
        PROTOCOL_MAJOR,
        PROTOCOL_MINOR,
        int(opcode),
        int(status),
        CONTROL_PACKET_BYTES,
        request_id,
        transfer_sequence,
        slot_index,
        record_count,
        *args,
    )
    packet[
        CONTROL_PAYLOAD_OFFSET :
        CONTROL_PAYLOAD_OFFSET + len(payload)
    ] = payload
    return bytes(packet)


def parse_control(value: bytes) -> ControlPacket:
    if not isinstance(value, bytes) or len(value) != CONTROL_PACKET_BYTES:
        raise ProtocolError(
            "history worker control packet has the wrong size"
        )
    fields = _CONTROL_PREFIX.unpack_from(value)
    (
        magic,
        major,
        minor,
        opcode_value,
        status_value,
        message_bytes,
        request_id,
        transfer_sequence,
        slot_index,
        record_count,
        *args,
    ) = fields
    if (
        magic != CONTROL_MAGIC
        or (major, minor) != (PROTOCOL_MAJOR, PROTOCOL_MINOR)
        or message_bytes != CONTROL_PACKET_BYTES
        or any(value[76:CONTROL_PAYLOAD_OFFSET])
    ):
        raise ProtocolError(
            "history worker control prefix is noncanonical"
        )
    try:
        opcode = WorkerOpcode(opcode_value)
        status = WorkerStatus(status_value)
    except ValueError as error:
        raise ProtocolError(
            "history worker control opcode/status is unknown"
        ) from error
    return ControlPacket(
        opcode=opcode,
        status=status,
        request_id=request_id,
        transfer_sequence=transfer_sequence,
        slot_index=slot_index,
        record_count=record_count,
        args=tuple(args),  # type: ignore[arg-type]
        payload=value[CONTROL_PAYLOAD_OFFSET:],
    )


def send_control(channel: socket.socket, packet: bytes) -> None:
    if not isinstance(channel, socket.socket):
        raise TypeError("history worker control channel must be a socket")
    if not isinstance(packet, bytes) or len(packet) != CONTROL_PACKET_BYTES:
        raise ValueError("history worker control packet has the wrong size")
    sent = channel.send(packet)
    if sent != CONTROL_PACKET_BYTES:
        raise ProtocolError(
            "history worker control packet was sent partially"
        )


def recv_control(channel: socket.socket) -> ControlPacket:
    if not isinstance(channel, socket.socket):
        raise TypeError("history worker control channel must be a socket")
    data, ancillary, flags, _address = channel.recvmsg(
        CONTROL_PACKET_BYTES,
        socket.CMSG_SPACE(struct.calcsize("i")),
    )
    if not data:
        raise EOFError("history worker control channel closed")
    for level, kind, encoded in ancillary:
        if level == socket.SOL_SOCKET and kind == socket.SCM_RIGHTS:
            descriptors = array.array("i")
            usable = len(encoded) - len(encoded) % descriptors.itemsize
            descriptors.frombytes(encoded[:usable])
            for descriptor in descriptors:
                try:
                    os.close(descriptor)
                except OSError:
                    pass
    if (
        flags
        & (
            getattr(socket, "MSG_TRUNC", 0x20)
            | getattr(socket, "MSG_CTRUNC", 0x08)
        )
        or ancillary
    ):
        raise ProtocolError(
            "history worker control packet carried truncation or fds"
        )
    return parse_control(data)


def pack_init_payload(
    *,
    expected_session: DailyCatalogSessionIdentity,
    timeout_ns: int,
    result_column_mask: int,
    control_socket_path: str | bytes,
) -> bytes:
    path = os.fsencode(control_socket_path)
    if not isinstance(
        expected_session, DailyCatalogSessionIdentity
    ):
        raise TypeError(
            "expected_session must be DailyCatalogSessionIdentity"
        )
    if (
        not path
        or b"\x00" in path
        or len(path) > MAX_CONTROL_PATH_BYTES
    ):
        raise ValueError(
            "worker control path must fit Linux sockaddr_un"
        )
    column_names(result_column_mask)
    payload = bytearray(CONTROL_PAYLOAD_BYTES)
    _INIT_PREFIX.pack_into(
        payload,
        0,
        expected_session.run_id,
        expected_session.catalog_digest,
        expected_session.session_epoch,
        expected_session.catalog_generation,
        expected_session.catalog_version,
        expected_session.trade_date,
        expected_session.catalog_trade_date,
        expected_session.capacity,
        expected_session.bound_count,
        int(expected_session.catalog_scope),
        int(expected_session.coverage_complete),
        timeout_ns,
        result_column_mask,
        len(path),
        0,
    )
    payload[_INIT_PREFIX.size : _INIT_PREFIX.size + len(path)] = path
    return bytes(payload)


def parse_init_payload(
    payload: bytes,
) -> tuple[DailyCatalogSessionIdentity, int, int, bytes]:
    if len(payload) != CONTROL_PAYLOAD_BYTES:
        raise ProtocolError("worker INIT payload has the wrong size")
    (
        run_id,
        catalog_digest,
        session_epoch,
        catalog_generation,
        catalog_version,
        trade_date,
        catalog_trade_date,
        capacity,
        bound_count,
        catalog_scope,
        coverage_complete,
        timeout_ns,
        result_column_mask,
        path_length,
        reserved,
    ) = _INIT_PREFIX.unpack_from(payload)
    end = _INIT_PREFIX.size + path_length
    if (
        timeout_ns == 0
        or reserved
        or path_length == 0
        or path_length > MAX_CONTROL_PATH_BYTES
        or end > len(payload)
        or any(payload[end:])
    ):
        raise ProtocolError("worker INIT payload is noncanonical")
    try:
        column_names(result_column_mask)
    except ValueError as error:
        raise ProtocolError("worker INIT column mask is invalid") from error
    path = payload[_INIT_PREFIX.size:end]
    if b"\x00" in path or not os.path.isabs(path):
        raise ProtocolError("worker INIT path is invalid")
    try:
        expected_session = DailyCatalogSessionIdentity(
            run_id=run_id,
            session_epoch=session_epoch,
            trade_date=trade_date,
            capacity=capacity,
            catalog_digest=catalog_digest,
            catalog_generation=catalog_generation,
            bound_count=bound_count,
            catalog_scope=CatalogScope(catalog_scope),
            coverage_complete=bool(coverage_complete)
            if coverage_complete in (0, 1)
            else coverage_complete,
            catalog_trade_date=catalog_trade_date,
            catalog_version=catalog_version,
        )
    except (TypeError, ValueError, WireFormatError) as error:
        raise ProtocolError(
            "worker INIT daily catalog identity is invalid"
        ) from error
    return (
        expected_session,
        timeout_ns,
        result_column_mask,
        path,
    )


def pack_open_payload(
    *,
    instrument_id: int,
    requested_page_records: int,
    expected_generation: int,
    base_checkpoint: bytes | None,
) -> bytes:
    if base_checkpoint is None:
        base_kind = 0
        checkpoint = b"\x00" * CHECKPOINT_BYTES
    else:
        if (
            not isinstance(base_checkpoint, bytes)
            or len(base_checkpoint) != CHECKPOINT_BYTES
        ):
            raise ValueError(
                "base checkpoint must be one fixed wire checkpoint"
            )
        base_kind = 1
        checkpoint = base_checkpoint
    payload = bytearray(CONTROL_PAYLOAD_BYTES)
    _OPEN_PREFIX.pack_into(
        payload,
        0,
        instrument_id,
        requested_page_records,
        expected_generation,
        base_kind,
        0,
    )
    payload[
        _OPEN_PREFIX.size :
        _OPEN_PREFIX.size + CHECKPOINT_BYTES
    ] = checkpoint
    return bytes(payload)


def parse_open_payload(
    payload: bytes,
) -> tuple[int, int, int, bytes | None]:
    if len(payload) != CONTROL_PAYLOAD_BYTES:
        raise ProtocolError("worker OPEN payload has the wrong size")
    (
        instrument_id,
        requested_page_records,
        expected_generation,
        base_kind,
        reserved,
    ) = _OPEN_PREFIX.unpack_from(payload)
    checkpoint_end = _OPEN_PREFIX.size + CHECKPOINT_BYTES
    checkpoint = payload[_OPEN_PREFIX.size:checkpoint_end]
    if (
        instrument_id == 0
        or requested_page_records == 0
        or reserved
        or base_kind not in (0, 1)
        or any(payload[checkpoint_end:])
        or (base_kind == 0 and any(checkpoint))
    ):
        raise ProtocolError("worker OPEN payload is noncanonical")
    return (
        instrument_id,
        requested_page_records,
        expected_generation,
        None if base_kind == 0 else checkpoint,
    )


def pack_error_payload(error: BaseException) -> bytes:
    text = f"{type(error).__name__}: {error}".encode(
        "utf-8", errors="replace"
    )
    text = text[: CONTROL_PAYLOAD_BYTES - 1]
    return text + b"\x00" * (CONTROL_PAYLOAD_BYTES - len(text))


def parse_error_payload(payload: bytes) -> str:
    if len(payload) != CONTROL_PAYLOAD_BYTES:
        raise ProtocolError("worker ERROR payload has the wrong size")
    first_nul = payload.find(b"\x00")
    if first_nul < 0 or any(payload[first_nul:]):
        raise ProtocolError("worker ERROR payload is noncanonical")
    return payload[:first_nul].decode("utf-8", errors="replace")


def pack_ring_header(
    layout: RingLayout, result_column_mask: int
) -> bytes:
    column_names(result_column_mask)
    header = bytearray(RING_HEADER_BYTES)
    _RING_PREFIX.pack_into(
        header,
        0,
        RING_MAGIC,
        PROTOCOL_MAJOR,
        PROTOCOL_MINOR,
        RING_HEADER_BYTES,
        layout.total_bytes,
        layout.slot_count,
        layout.batch_capacity,
        SLOT_HEADER_BYTES,
        layout.slot_stride,
        result_column_mask,
        RESULT_SCHEMA_DIGEST,
    )
    return bytes(header)


def validate_ring_header(
    mapping: mmap.mmap,
) -> tuple[RingLayout, int]:
    if len(mapping) < RING_HEADER_BYTES:
        raise WireFormatError("history worker ring is truncated")
    (
        magic,
        major,
        minor,
        header_bytes,
        total_bytes,
        slot_count,
        batch_capacity,
        slot_header_bytes,
        slot_stride,
        result_column_mask,
        digest,
    ) = _RING_PREFIX.unpack_from(mapping)
    if (
        magic != RING_MAGIC
        or (major, minor) != (PROTOCOL_MAJOR, PROTOCOL_MINOR)
        or header_bytes != RING_HEADER_BYTES
        or slot_header_bytes != SLOT_HEADER_BYTES
        or digest != RESULT_SCHEMA_DIGEST
        or any(mapping[_RING_PREFIX.size:RING_HEADER_BYTES])
    ):
        raise WireFormatError(
            "history worker ring header is noncanonical"
        )
    expected = make_ring_layout(slot_count, batch_capacity)
    if (
        total_bytes != len(mapping)
        or total_bytes != expected.total_bytes
        or slot_stride != expected.slot_stride
    ):
        raise WireFormatError(
            "history worker ring layout does not reconcile"
        )
    try:
        column_names(result_column_mask)
    except ValueError as error:
        raise WireFormatError(
            "history worker ring column mask is invalid"
        ) from error
    return expected, result_column_mask


@dataclass(frozen=True, slots=True)
class SlotHeader:
    slot_index: int
    flags: int
    request_id: int
    transfer_sequence: int
    page_index: int
    generation: int
    record_count: int
    cumulative_record_count: int
    first_ingress_sequence: int
    last_ingress_sequence: int
    first_tick_stream_sequence: int
    last_tick_stream_sequence: int
    cumulative_source_record_counts: tuple[int, int, int, int]
    worker_read_start_ns: int
    worker_read_return_ns: int
    worker_publish_begin_ns: int
    result_column_mask: int
    checkpoint_wire: bytes | None

    @property
    def eof(self) -> bool:
        return bool(self.flags & SLOT_FLAG_EOF)


def publish_slot(
    mapping: mmap.mmap,
    layout: RingLayout,
    *,
    slot_index: int,
    flags: int,
    request_id: int,
    transfer_sequence: int,
    page_index: int,
    generation: int,
    record_count: int,
    cumulative_record_count: int,
    first_ingress_sequence: int,
    last_ingress_sequence: int,
    first_tick_stream_sequence: int,
    last_tick_stream_sequence: int,
    cumulative_source_record_counts: tuple[int, int, int, int],
    worker_read_start_ns: int,
    worker_read_return_ns: int,
    worker_publish_begin_ns: int,
    result_column_mask: int,
    columns: Mapping[str, Sequence[object]] | None = None,
    checkpoint_wire: bytes | None = None,
) -> int:
    base = layout.slot_base(slot_index)
    if flags not in (SLOT_FLAG_DATA, SLOT_FLAG_EOF):
        raise ValueError("result slot flags are invalid")
    if record_count < 0 or record_count > layout.batch_capacity:
        raise ValueError("result slot record_count exceeds capacity")
    if len(cumulative_source_record_counts) != 4:
        raise ValueError("result slot source counts must contain four values")
    if flags == SLOT_FLAG_DATA:
        if record_count == 0 or columns is None or checkpoint_wire is not None:
            raise ValueError("data slot payload is incomplete")
        selected_names = column_names(result_column_mask)
        if set(columns) != set(selected_names):
            raise ValueError("data slot columns differ from its projection")
        for name in selected_names:
            values = columns[name]
            if len(values) != record_count:
                raise ValueError(
                    f"result column {name!r} has the wrong row count"
                )
            spec = RESULT_COLUMN_BY_NAME[name]
            encoded = array.array(spec.format, values)
            expected_bytes = spec.width * record_count
            if len(encoded) * encoded.itemsize != expected_bytes:
                raise RuntimeError("native result column width changed")
            start = base + layout.column_offsets[name]
            mapping[start : start + expected_bytes] = encoded
        checkpoint = b"\x00" * CHECKPOINT_BYTES
    else:
        if (
            record_count
            or columns is not None
            or result_column_mask
            or not isinstance(checkpoint_wire, bytes)
            or len(checkpoint_wire) != CHECKPOINT_BYTES
        ):
            raise ValueError("terminal slot payload is invalid")
        checkpoint = checkpoint_wire

    if worker_publish_begin_ns == 0:
        # Column bytes are complete at this boundary.  The fixed slot header
        # and RESULT_READY/COMPLETE notification are deliberately later, so
        # this is a publication-begin marker rather than a completion time.
        worker_publish_begin_ns = time.monotonic_ns()
    if worker_publish_begin_ns < worker_read_return_ns:
        raise ValueError(
            "result slot publication precedes its completed read"
        )
    header = bytearray(SLOT_HEADER_BYTES)
    _SLOT_PREFIX.pack_into(
        header,
        0,
        SLOT_MAGIC,
        PROTOCOL_MAJOR,
        PROTOCOL_MINOR,
        SLOT_HEADER_BYTES,
        slot_index,
        flags,
        0,
        request_id,
        transfer_sequence,
        page_index,
        generation,
        record_count,
        cumulative_record_count,
        first_ingress_sequence,
        last_ingress_sequence,
        first_tick_stream_sequence,
        last_tick_stream_sequence,
        *cumulative_source_record_counts,
        worker_read_start_ns,
        worker_read_return_ns,
        worker_publish_begin_ns,
        result_column_mask,
    )
    header[
        SLOT_SCHEMA_DIGEST_OFFSET :
        SLOT_SCHEMA_DIGEST_OFFSET + len(RESULT_SCHEMA_DIGEST)
    ] = RESULT_SCHEMA_DIGEST
    header[
        SLOT_CHECKPOINT_OFFSET :
        SLOT_CHECKPOINT_OFFSET + CHECKPOINT_BYTES
    ] = checkpoint
    # The header is written last.  Ownership transfers only after the worker
    # subsequently sends RESULT_READY/COMPLETE on the control socket.
    mapping[base : base + SLOT_HEADER_BYTES] = header
    return worker_publish_begin_ns


def parse_slot_header(
    mapping: mmap.mmap,
    layout: RingLayout,
    *,
    expected_slot_index: int,
    expected_request_id: int,
    expected_transfer_sequence: int,
    expected_record_count: int,
    configured_column_mask: int,
) -> SlotHeader:
    base = layout.slot_base(expected_slot_index)
    fields = _SLOT_PREFIX.unpack_from(mapping, base)
    (
        magic,
        major,
        minor,
        header_bytes,
        slot_index,
        flags,
        reserved,
        request_id,
        transfer_sequence,
        page_index,
        generation,
        record_count,
        cumulative_record_count,
        first_ingress,
        last_ingress,
        first_tick,
        last_tick,
        *tail,
    ) = fields
    source_counts = tuple(tail[:4])
    (
        read_start,
        read_return,
        publish_ns,
        result_column_mask,
    ) = tail[4:]
    digest = bytes(
        mapping[
            base + SLOT_SCHEMA_DIGEST_OFFSET :
            base + SLOT_SCHEMA_DIGEST_OFFSET
            + len(RESULT_SCHEMA_DIGEST)
        ]
    )
    checkpoint = bytes(
        mapping[
            base + SLOT_CHECKPOINT_OFFSET :
            base + SLOT_CHECKPOINT_OFFSET + CHECKPOINT_BYTES
        ]
    )
    reserved_regions = (
        mapping[base + _SLOT_PREFIX.size : base + SLOT_SCHEMA_DIGEST_OFFSET],
        mapping[
            base + SLOT_SCHEMA_DIGEST_OFFSET
            + len(RESULT_SCHEMA_DIGEST) :
            base + SLOT_CHECKPOINT_OFFSET
        ],
        mapping[
            base + SLOT_CHECKPOINT_OFFSET + CHECKPOINT_BYTES :
            base + SLOT_HEADER_BYTES
        ],
    )
    if (
        magic != SLOT_MAGIC
        or (major, minor) != (PROTOCOL_MAJOR, PROTOCOL_MINOR)
        or header_bytes != SLOT_HEADER_BYTES
        or reserved
        or digest != RESULT_SCHEMA_DIGEST
        or any(any(region) for region in reserved_regions)
        or slot_index != expected_slot_index
        or request_id != expected_request_id
        or transfer_sequence != expected_transfer_sequence
        or record_count != expected_record_count
        or record_count > layout.batch_capacity
        or flags not in (SLOT_FLAG_DATA, SLOT_FLAG_EOF)
        or read_start == 0
        or read_return < read_start
        or publish_ns < read_return
    ):
        raise WireFormatError(
            "history worker result slot header is inconsistent"
        )
    if flags == SLOT_FLAG_DATA:
        if (
            record_count == 0
            or result_column_mask != configured_column_mask
            or any(checkpoint)
            or cumulative_record_count < record_count
            or sum(source_counts) != cumulative_record_count
            or first_ingress == 0
            or last_ingress < first_ingress
            or first_tick == 0
            or last_tick < first_tick
        ):
            raise WireFormatError(
                "history worker data slot metadata is invalid"
            )
        checkpoint_wire = None
    else:
        if (
            record_count
            or result_column_mask
            or not any(checkpoint)
            or sum(source_counts) != cumulative_record_count
            or any(
                (
                    first_ingress,
                    last_ingress,
                    first_tick,
                    last_tick,
                )
            )
        ):
            raise WireFormatError(
                "history worker terminal slot metadata is invalid"
            )
        checkpoint_wire = checkpoint
    return SlotHeader(
        slot_index=slot_index,
        flags=flags,
        request_id=request_id,
        transfer_sequence=transfer_sequence,
        page_index=page_index,
        generation=generation,
        record_count=record_count,
        cumulative_record_count=cumulative_record_count,
        first_ingress_sequence=first_ingress,
        last_ingress_sequence=last_ingress,
        first_tick_stream_sequence=first_tick,
        last_tick_stream_sequence=last_tick,
        cumulative_source_record_counts=(
            source_counts  # type: ignore[arg-type]
        ),
        worker_read_start_ns=read_start,
        worker_read_return_ns=read_return,
        worker_publish_begin_ns=publish_ns,
        result_column_mask=result_column_mask,
        checkpoint_wire=checkpoint_wire,
    )


def column_region(
    layout: RingLayout,
    slot_index: int,
    name: str,
    record_count: int,
) -> tuple[int, int, ResultColumnSpec]:
    try:
        spec = RESULT_COLUMN_BY_NAME[name]
        relative = layout.column_offsets[name]
    except KeyError:
        raise KeyError(name) from None
    if record_count < 0 or record_count > layout.batch_capacity:
        raise ValueError("result record_count is out of range")
    begin = layout.slot_base(slot_index) + relative
    return begin, begin + spec.width * record_count, spec


if sys.byteorder != "little":
    raise RuntimeError(
        "L2Flow history worker V2 requires a little-endian Python host"
    )


__all__ = [
    "ALL_RESULT_COLUMNS",
    "CONTROL_PACKET_BYTES",
    "DEFAULT_RESULT_COLUMNS",
    "MAX_RESULT_BATCH_RECORDS",
    "MAX_RING_SLOTS",
    "NO_SLOT",
    "NO_TIMEOUT_NS",
    "PROTOCOL_MAJOR",
    "PROTOCOL_MINOR",
    "RESULT_COLUMN_BY_NAME",
    "RESULT_COLUMN_INDEX",
    "RESULT_COLUMN_SPECS",
    "RESULT_SCHEMA_DIGEST",
    "RING_HEADER_BYTES",
    "SLOT_FLAG_DATA",
    "SLOT_FLAG_EOF",
    "SLOT_HEADER_BYTES",
    "ControlPacket",
    "ResultColumnSpec",
    "RingLayout",
    "SlotHeader",
    "WorkerOpcode",
    "WorkerStatus",
    "column_mask",
    "column_names",
    "column_region",
    "make_ring_layout",
    "pack_control",
    "pack_error_payload",
    "pack_init_payload",
    "pack_open_payload",
    "pack_ring_header",
    "parse_control",
    "parse_error_payload",
    "parse_init_payload",
    "parse_open_payload",
    "parse_slot_header",
    "publish_slot",
    "recv_control",
    "send_control",
    "validate_ring_header",
]
