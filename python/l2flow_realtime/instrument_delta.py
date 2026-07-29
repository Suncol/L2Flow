"""V2 instrument-local tick deltas over one pinned Store session.

The protocol is intentionally finite and transactional.  One seqpacket
connection pins a target generation, then streams one instrument at a time.
Only an explicit successful zero-row terminal response verifies the declared
target checkpoint.
"""

from __future__ import annotations

import os
import secrets
import socket
import struct
import threading
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Optional, Tuple, Union

from ._fd_owner import _ReceivedPacket, _recv_fds
from .batch import tick_wire_numpy_records, tick_wire_record_count
from .checkpoint import InstrumentTickDeltaCheckpoint
from .models import (
    ClientClosedError,
    L2FlowRealtimeError,
    ProtocolError,
    SessionIdentity,
    StaleSessionError,
    UnavailableError,
    WireFormatError,
)
from .native import NativeInstrumentTickDeltaPageValidator
from .wire import (
    CONTROL_MAGIC,
    TICK_BYTES,
    WIRE_MAJOR,
    WIRE_MINOR,
)


INSTRUMENT_TICK_DELTA_PAGE_MAGIC_V2 = b"L2FIDT2\x00"
INSTRUMENT_TICK_DELTA_ENDIAN_MARKER_V2 = 0x01020304
INSTRUMENT_TICK_DELTA_PAGE_HEADER_BYTES_V2 = 4096
INSTRUMENT_TICK_DELTA_SOURCE_MASK_V2 = (1 << 1) | (1 << 3)

OPEN_INSTRUMENT_TICK_DELTA_SESSION_OPCODE_V2 = 4
OPEN_INSTRUMENT_TICK_DELTA_OPCODE_V2 = 5
READ_INSTRUMENT_TICK_DELTA_OPCODE_V2 = 6

INSTRUMENT_TICK_DELTA_OK = 0
INSTRUMENT_TICK_DELTA_INVALID_REQUEST = 1
INSTRUMENT_TICK_DELTA_UNSUPPORTED_VERSION = 2
INSTRUMENT_TICK_DELTA_UNAVAILABLE = 3
INSTRUMENT_TICK_DELTA_NOT_FOUND = 4
INSTRUMENT_TICK_DELTA_RESOURCE_EXHAUSTED = 5
INSTRUMENT_TICK_DELTA_INTERNAL_FAILURE = 6
INSTRUMENT_TICK_DELTA_CHECKPOINT_MISMATCH = 7

INSTRUMENT_TICK_DELTA_RESPONSE_TERMINAL_V2 = 1 << 0
INSTRUMENT_TICK_DELTA_COVERAGE_FROM_OPEN_V2 = 1 << 0
INSTRUMENT_TICK_DELTA_RECORD_COVERAGE_COMPLETE_V2 = 1 << 1
INSTRUMENT_TICK_DELTA_FIELD_COMPLETE_V2 = 1 << 2
_KNOWN_ENDPOINT_FLAGS = (
    INSTRUMENT_TICK_DELTA_COVERAGE_FROM_OPEN_V2
    | INSTRUMENT_TICK_DELTA_RECORD_COVERAGE_COMPLETE_V2
    | INSTRUMENT_TICK_DELTA_FIELD_COMPLETE_V2
)
INSTRUMENT_TICK_DELTA_CORE_V1_PROJECTION = 1

INSTRUMENT_TICK_DELTA_ENDPOINT_BYTES_V2 = 256
INSTRUMENT_TICK_DELTA_CHECKPOINT_BYTES_V2 = 320
INSTRUMENT_TICK_DELTA_METADATA_BYTES_V2 = 736
OPEN_INSTRUMENT_TICK_DELTA_SESSION_REQUEST_BYTES_V2 = 40
OPEN_INSTRUMENT_TICK_DELTA_SESSION_RESPONSE_BYTES_V2 = 296
OPEN_INSTRUMENT_TICK_DELTA_REQUEST_BYTES_V2 = 384
OPEN_INSTRUMENT_TICK_DELTA_RESPONSE_BYTES_V2 = 776
READ_INSTRUMENT_TICK_DELTA_REQUEST_BYTES_V2 = 48
READ_INSTRUMENT_TICK_DELTA_RESPONSE_BYTES_V2 = 64

DEFAULT_INSTRUMENT_TICK_DELTA_PAGE_RECORDS = 16_384
_MAXIMUM_PAGE_RECORDS = 1024 * 1024
_UINT32_MAX = 0xFFFFFFFF
_UINT64_MAX = 0xFFFFFFFFFFFFFFFF

_CONTROL_RESPONSE_PREFIX = struct.Struct("<8sHHHHIIQ")
_OPEN_SESSION_REQUEST = struct.Struct("<8sHHHHIIQQ")
_OPEN_INSTRUMENT_PREFIX = struct.Struct("<8sHHHHIIQIIIIQ")
_READ_REQUEST = struct.Struct("<8sHHHHIIQQQ")
_READ_RESPONSE = struct.Struct("<8sHHHHIIQQQQQ")
_ENDPOINT = struct.Struct(
    "<16sQQIIQQQQ32s32s4I4QII64s"
)
_CHECKPOINT_TAIL = struct.Struct("<II4QQ16s")
_METADATA_HEAD = struct.Struct("<II")
_METADATA_TAIL = struct.Struct("<4Q5QII8s")
_PAGE_PREFIX = struct.Struct("<8sHHIIIQQIIQQQQQ")

assert _CONTROL_RESPONSE_PREFIX.size == 32
assert (
    _OPEN_SESSION_REQUEST.size
    == OPEN_INSTRUMENT_TICK_DELTA_SESSION_REQUEST_BYTES_V2
)
assert _OPEN_INSTRUMENT_PREFIX.size == 56
assert (
    _READ_REQUEST.size
    == READ_INSTRUMENT_TICK_DELTA_REQUEST_BYTES_V2
)
assert (
    _READ_RESPONSE.size
    == READ_INSTRUMENT_TICK_DELTA_RESPONSE_BYTES_V2
)
assert _ENDPOINT.size == INSTRUMENT_TICK_DELTA_ENDPOINT_BYTES_V2
assert _CHECKPOINT_TAIL.size == 64
assert _METADATA_HEAD.size == 8
assert _METADATA_TAIL.size == 88
assert _PAGE_PREFIX.size == 88


class InstrumentTickDeltaBaseKind(IntEnum):
    ORIGIN = 1
    CHECKPOINT = 2


class InstrumentTickDeltaNotFoundError(L2FlowRealtimeError):
    """The target registry does not contain the requested instrument."""


class InstrumentTickDeltaResourceExhaustedError(
    L2FlowRealtimeError
):
    """The service cannot allocate another delta cursor or page."""


class InstrumentTickDeltaInternalFailureError(UnavailableError):
    """The service failed an internal delta-projection invariant."""


class InstrumentTickDeltaCheckpointMismatchError(StaleSessionError):
    """The supplied checkpoint is not a valid base for the pinned target."""


class InstrumentTickDeltaCheckpointUnavailableError(
    UnavailableError
):
    """A target checkpoint was requested before explicit verified EOF."""


@dataclass(frozen=True, slots=True)
class InstrumentTickDeltaGeneration:
    """Complete global endpoint of the session's pinned Store generation."""

    run_id: bytes
    session_epoch: int
    generation: int
    trade_date: int
    instrument_count: int
    ingress_sequence_exclusive: int
    tick_stream_sequence_exclusive: int
    recv_monotonic_cut_ns: int
    registry_version: int
    registry_sha256: bytes
    input_identity_sha256: bytes
    source_stream_ids: Tuple[int, int, int, int]
    source_sequence_exclusive: Tuple[int, int, int, int]
    flags: int
    payload_projection: int

    @property
    def session_identity(self) -> SessionIdentity:
        return SessionIdentity(self.run_id, self.session_epoch)

    @property
    def coverage_from_open(self) -> bool:
        return (
            self.flags
            & INSTRUMENT_TICK_DELTA_COVERAGE_FROM_OPEN_V2
        ) != 0

    @property
    def record_coverage_complete(self) -> bool:
        return (
            self.flags
            & INSTRUMENT_TICK_DELTA_RECORD_COVERAGE_COMPLETE_V2
        ) != 0

    @property
    def field_complete(self) -> bool:
        return (
            self.flags
            & INSTRUMENT_TICK_DELTA_FIELD_COMPLETE_V2
        ) != 0


@dataclass(frozen=True, slots=True)
class InstrumentTickDeltaMetadata:
    """Immutable authority for one half-open instrument delta."""

    base_kind: InstrumentTickDeltaBaseKind
    base_checkpoint: Optional[InstrumentTickDeltaCheckpoint]
    target_checkpoint: InstrumentTickDeltaCheckpoint
    delta_tick_source_record_counts: Tuple[int, int, int, int]
    delta_tick_record_count: int
    ingress_sequence_begin_inclusive: int
    ingress_sequence_end_exclusive: int
    tick_stream_sequence_begin_inclusive: int
    tick_stream_sequence_end_exclusive: int
    flags: int
    payload_projection: int

    @property
    def instrument_id(self) -> int:
        return self.target_checkpoint.instrument_id

    @property
    def origin(self) -> bool:
        return self.base_kind is InstrumentTickDeltaBaseKind.ORIGIN


@dataclass(frozen=True, slots=True)
class InstrumentTickDeltaPage:
    """One validated columnar tick page or the explicit terminal page."""

    instrument_id: int
    page_index: int
    wire_records: bytes
    eof: bool
    cumulative_record_count: int
    cumulative_source_record_counts: Tuple[int, int, int, int]

    def __post_init__(self) -> None:
        if (
            not isinstance(self.instrument_id, int)
            or isinstance(self.instrument_id, bool)
            or self.instrument_id <= 0
            or self.instrument_id > _UINT32_MAX
        ):
            raise ValueError("instrument_id must be a positive uint32")
        if (
            not isinstance(self.page_index, int)
            or isinstance(self.page_index, bool)
            or self.page_index < 0
            or self.page_index > _UINT64_MAX
        ):
            raise ValueError("page_index must fit uint64")
        record_count = tick_wire_record_count(self.wire_records)
        if not isinstance(self.eof, bool):
            raise TypeError("eof must be bool")
        if self.eof and record_count:
            raise ValueError("instrument delta EOF page must be empty")
        if not self.eof and not record_count:
            raise ValueError(
                "instrument delta data page must be nonempty"
            )
        if (
            not isinstance(self.cumulative_record_count, int)
            or isinstance(self.cumulative_record_count, bool)
            or self.cumulative_record_count < record_count
            or self.cumulative_record_count > _UINT64_MAX
        ):
            raise ValueError(
                "instrument delta cumulative count is invalid"
            )
        counts = tuple(self.cumulative_source_record_counts)
        if (
            len(counts) != 4
            or any(
                not isinstance(count, int)
                or isinstance(count, bool)
                or count < 0
                or count > _UINT64_MAX
                for count in counts
            )
            or counts[0] != 0
            or counts[2] != 0
            or sum(counts) != self.cumulative_record_count
        ):
            raise ValueError(
                "instrument delta cumulative source counts are invalid"
            )
        object.__setattr__(
            self, "cumulative_source_record_counts", counts
        )

    def __len__(self) -> int:
        return tick_wire_record_count(self.wire_records)

    def numpy_records(self):
        """Return a zero-copy, read-only structured view of the tick rows."""

        return tick_wire_numpy_records(self.wire_records)


def _positive_uint32(value: object, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{field} must be an integer")
    if value <= 0 or value > _UINT32_MAX:
        raise ValueError(f"{field} must be a positive uint32")
    return value


def _positive_uint64(value: object, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{field} must be an integer")
    if value <= 0 or value > _UINT64_MAX:
        raise ValueError(f"{field} must be a positive uint64")
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
    if timeout is None:
        return None
    if isinstance(timeout, bool) or not isinstance(
        timeout, (int, float)
    ):
        raise TypeError("timeout must be a number or None")
    if timeout <= 0:
        raise ValueError("timeout must be positive or None")
    return float(timeout)


def _recv_packet(
    channel: socket.socket,
    expected_bytes: int,
) -> _ReceivedPacket:
    return _recv_fds(
        channel,
        expected_bytes,
        response_name="instrument delta response",
    )


def _send_packet(channel: socket.socket, payload: bytes) -> None:
    sent = channel.send(payload)
    if sent != len(payload):
        raise ProtocolError(
            "short instrument delta request send: "
            f"{sent} of {len(payload)}"
        )


def _raise_status(operation: str, status: int) -> None:
    if status == INSTRUMENT_TICK_DELTA_OK:
        return
    if status == INSTRUMENT_TICK_DELTA_INVALID_REQUEST:
        raise ProtocolError(f"{operation}: invalid request rejected")
    if status == INSTRUMENT_TICK_DELTA_UNSUPPORTED_VERSION:
        raise ProtocolError(
            f"{operation}: instrument delta V2 is unsupported"
        )
    if status == INSTRUMENT_TICK_DELTA_UNAVAILABLE:
        raise UnavailableError(
            f"{operation}: no healthy Store generation is available"
        )
    if status == INSTRUMENT_TICK_DELTA_NOT_FOUND:
        raise InstrumentTickDeltaNotFoundError(
            f"{operation}: instrument is absent from the target registry"
        )
    if status == INSTRUMENT_TICK_DELTA_RESOURCE_EXHAUSTED:
        raise InstrumentTickDeltaResourceExhaustedError(
            f"{operation}: instrument delta resources are exhausted"
        )
    if status == INSTRUMENT_TICK_DELTA_INTERNAL_FAILURE:
        raise InstrumentTickDeltaInternalFailureError(
            f"{operation}: server failed an internal invariant"
        )
    if status == INSTRUMENT_TICK_DELTA_CHECKPOINT_MISMATCH:
        raise InstrumentTickDeltaCheckpointMismatchError(
            f"{operation}: checkpoint does not match the pinned target"
        )
    raise ProtocolError(f"{operation}: unknown control status {status}")


def _validate_response_prefix(
    data: bytes,
    *,
    expected_bytes: int,
    expected_request_id: int,
    allowed_flags: int,
    operation: str,
    reserved_is_record_count: bool = False,
) -> Tuple[int, int, int]:
    (
        magic,
        major,
        minor,
        status,
        flags,
        message_bytes,
        reserved_or_count,
        request_id,
    ) = _CONTROL_RESPONSE_PREFIX.unpack_from(data)
    if magic != CONTROL_MAGIC:
        raise ProtocolError(f"{operation}: control magic mismatch")
    if major != WIRE_MAJOR or minor != WIRE_MINOR:
        raise ProtocolError(f"{operation}: protocol version mismatch")
    if message_bytes != expected_bytes:
        raise ProtocolError(f"{operation}: message_bytes mismatch")
    if not reserved_is_record_count and reserved_or_count != 0:
        raise ProtocolError(f"{operation}: reserved field is nonzero")
    if request_id != expected_request_id:
        raise ProtocolError(f"{operation}: request_id mismatch")
    if flags & ~allowed_flags:
        raise ProtocolError(f"{operation}: unknown response flags")
    return status, flags, reserved_or_count


def _validate_endpoint(
    endpoint: InstrumentTickDeltaGeneration,
) -> None:
    if (
        len(endpoint.run_id) != 16
        or not any(endpoint.run_id)
        or endpoint.session_epoch == 0
        or endpoint.generation == 0
        or endpoint.trade_date == 0
        or endpoint.instrument_count == 0
        or endpoint.ingress_sequence_exclusive == 0
        or endpoint.tick_stream_sequence_exclusive == 0
        or endpoint.registry_version == 0
        or len(endpoint.registry_sha256) != 32
        or not any(endpoint.registry_sha256)
        or len(endpoint.input_identity_sha256) != 32
        or not any(endpoint.input_identity_sha256)
    ):
        raise WireFormatError(
            "instrument delta generation identity is invalid"
        )
    if (
        len(endpoint.source_stream_ids) != 4
        or len(set(endpoint.source_stream_ids)) != 4
        or any(value == 0 for value in endpoint.source_stream_ids)
        or len(endpoint.source_sequence_exclusive) != 4
        or any(
            value == 0
            for value in endpoint.source_sequence_exclusive
        )
    ):
        raise WireFormatError(
            "instrument delta source endpoint is invalid"
        )
    ingress_prefix = sum(
        value - 1 for value in endpoint.source_sequence_exclusive
    )
    if (
        ingress_prefix > _UINT64_MAX
        or endpoint.ingress_sequence_exclusive - 1
        != ingress_prefix
    ):
        raise WireFormatError(
            "instrument delta source endpoints do not reconcile "
            "with ingress"
        )
    tick_endpoint = (
        1
        + endpoint.source_sequence_exclusive[1]
        - 1
        + endpoint.source_sequence_exclusive[3]
        - 1
    )
    if (
        tick_endpoint > _UINT64_MAX
        or endpoint.tick_stream_sequence_exclusive != tick_endpoint
    ):
        raise WireFormatError(
            "instrument delta tick sources do not reconcile with "
            "tick-stream endpoint"
        )
    if endpoint.flags & ~_KNOWN_ENDPOINT_FLAGS:
        raise WireFormatError(
            "instrument delta generation has unknown flags"
        )
    if not endpoint.record_coverage_complete:
        raise WireFormatError(
            "instrument delta lacks complete tick record coverage"
        )
    if endpoint.field_complete:
        raise WireFormatError(
            "CoreV1 instrument delta must not claim field completeness"
        )
    if (
        endpoint.payload_projection
        != INSTRUMENT_TICK_DELTA_CORE_V1_PROJECTION
    ):
        raise WireFormatError(
            "unsupported instrument delta payload projection"
        )


def _parse_generation_endpoint(
    data: Union[bytes, bytearray],
    offset: int = 0,
) -> InstrumentTickDeltaGeneration:
    fields = _ENDPOINT.unpack_from(data, offset)
    if any(fields[21]):
        raise WireFormatError(
            "instrument delta generation reserved bytes are nonzero"
        )
    endpoint = InstrumentTickDeltaGeneration(
        run_id=bytes(fields[0]),
        session_epoch=fields[1],
        generation=fields[2],
        trade_date=fields[3],
        instrument_count=fields[4],
        ingress_sequence_exclusive=fields[5],
        tick_stream_sequence_exclusive=fields[6],
        recv_monotonic_cut_ns=fields[7],
        registry_version=fields[8],
        registry_sha256=bytes(fields[9]),
        input_identity_sha256=bytes(fields[10]),
        source_stream_ids=tuple(fields[11:15]),
        source_sequence_exclusive=tuple(fields[15:19]),
        flags=fields[19],
        payload_projection=fields[20],
    )
    _validate_endpoint(endpoint)
    return endpoint


def _checkpoint_endpoint(
    checkpoint: InstrumentTickDeltaCheckpoint,
) -> InstrumentTickDeltaGeneration:
    return InstrumentTickDeltaGeneration(
        run_id=checkpoint.run_id,
        session_epoch=checkpoint.session_epoch,
        generation=checkpoint.generation,
        trade_date=checkpoint.trade_date,
        instrument_count=checkpoint.instrument_count,
        ingress_sequence_exclusive=(
            checkpoint.ingress_sequence_exclusive
        ),
        tick_stream_sequence_exclusive=(
            checkpoint.tick_stream_sequence_exclusive
        ),
        recv_monotonic_cut_ns=checkpoint.recv_monotonic_cut_ns,
        registry_version=checkpoint.registry_version,
        registry_sha256=checkpoint.registry_sha256,
        input_identity_sha256=checkpoint.input_identity_sha256,
        source_stream_ids=checkpoint.source_stream_ids,
        source_sequence_exclusive=(
            checkpoint.source_sequence_exclusive
        ),
        flags=checkpoint.flags,
        payload_projection=checkpoint.payload_projection,
    )


def _parse_checkpoint(
    data: Union[bytes, bytearray],
    offset: int = 0,
) -> InstrumentTickDeltaCheckpoint:
    endpoint = _parse_generation_endpoint(data, offset)
    (
        instrument_id,
        registry_ordinal,
        count0,
        count1,
        count2,
        count3,
        total_count,
        reserved,
    ) = _CHECKPOINT_TAIL.unpack_from(
        data, offset + INSTRUMENT_TICK_DELTA_ENDPOINT_BYTES_V2
    )
    counts = (count0, count1, count2, count3)
    if instrument_id == 0 or registry_ordinal >= endpoint.instrument_count:
        raise WireFormatError(
            "instrument delta checkpoint instrument identity is invalid"
        )
    if any(reserved):
        raise WireFormatError(
            "instrument delta checkpoint reserved bytes are nonzero"
        )
    if counts[0] != 0 or counts[2] != 0:
        raise WireFormatError(
            "instrument delta checkpoint snapshot counts are nonzero"
        )
    if sum(counts) > _UINT64_MAX or total_count != sum(counts):
        raise WireFormatError(
            "instrument delta checkpoint total count mismatch"
        )
    try:
        return InstrumentTickDeltaCheckpoint(
            run_id=endpoint.run_id,
            session_epoch=endpoint.session_epoch,
            trade_date=endpoint.trade_date,
            instrument_count=endpoint.instrument_count,
            registry_version=endpoint.registry_version,
            registry_sha256=endpoint.registry_sha256,
            instrument_id=instrument_id,
            registry_ordinal=registry_ordinal,
            generation=endpoint.generation,
            input_identity_sha256=endpoint.input_identity_sha256,
            ingress_sequence_exclusive=(
                endpoint.ingress_sequence_exclusive
            ),
            tick_stream_sequence_exclusive=(
                endpoint.tick_stream_sequence_exclusive
            ),
            recv_monotonic_cut_ns=endpoint.recv_monotonic_cut_ns,
            source_stream_ids=endpoint.source_stream_ids,
            source_sequence_exclusive=(
                endpoint.source_sequence_exclusive
            ),
            instrument_tick_counts=counts,
            coverage_from_open=endpoint.coverage_from_open,
            record_coverage_complete=(
                endpoint.record_coverage_complete
            ),
            field_complete=endpoint.field_complete,
            payload_projection=endpoint.payload_projection,
        )
    except (TypeError, ValueError) as error:
        raise WireFormatError(
            f"invalid instrument delta checkpoint: {error}"
        ) from error


def _pack_generation_endpoint(
    endpoint: InstrumentTickDeltaGeneration,
) -> bytes:
    _validate_endpoint(endpoint)
    return _ENDPOINT.pack(
        endpoint.run_id,
        endpoint.session_epoch,
        endpoint.generation,
        endpoint.trade_date,
        endpoint.instrument_count,
        endpoint.ingress_sequence_exclusive,
        endpoint.tick_stream_sequence_exclusive,
        endpoint.recv_monotonic_cut_ns,
        endpoint.registry_version,
        endpoint.registry_sha256,
        endpoint.input_identity_sha256,
        *endpoint.source_stream_ids,
        *endpoint.source_sequence_exclusive,
        endpoint.flags,
        endpoint.payload_projection,
        bytes(64),
    )


def _pack_checkpoint(
    checkpoint: InstrumentTickDeltaCheckpoint,
) -> bytes:
    if not isinstance(checkpoint, InstrumentTickDeltaCheckpoint):
        raise TypeError(
            "checkpoint must be InstrumentTickDeltaCheckpoint"
        )
    return _pack_generation_endpoint(
        _checkpoint_endpoint(checkpoint)
    ) + _CHECKPOINT_TAIL.pack(
        checkpoint.instrument_id,
        checkpoint.registry_ordinal,
        *checkpoint.instrument_tick_counts,
        checkpoint.total_instrument_tick_count,
        bytes(16),
    )


def _pack_metadata(metadata: InstrumentTickDeltaMetadata) -> bytes:
    base = (
        bytes(INSTRUMENT_TICK_DELTA_CHECKPOINT_BYTES_V2)
        if metadata.base_checkpoint is None
        else _pack_checkpoint(metadata.base_checkpoint)
    )
    packed = (
        _METADATA_HEAD.pack(
            int(metadata.base_kind),
            INSTRUMENT_TICK_DELTA_SOURCE_MASK_V2,
        )
        + base
        + _pack_checkpoint(metadata.target_checkpoint)
        + _METADATA_TAIL.pack(
            *metadata.delta_tick_source_record_counts,
            metadata.delta_tick_record_count,
            metadata.ingress_sequence_begin_inclusive,
            metadata.ingress_sequence_end_exclusive,
            metadata.tick_stream_sequence_begin_inclusive,
            metadata.tick_stream_sequence_end_exclusive,
            metadata.flags,
            metadata.payload_projection,
            bytes(8),
        )
    )
    if len(packed) != INSTRUMENT_TICK_DELTA_METADATA_BYTES_V2:
        raise RuntimeError("instrument delta metadata packing failed")
    return packed


def _same_static_generation(
    left: InstrumentTickDeltaGeneration,
    right: InstrumentTickDeltaGeneration,
) -> bool:
    return (
        left.run_id == right.run_id
        and left.session_epoch == right.session_epoch
        and left.trade_date == right.trade_date
        and left.instrument_count == right.instrument_count
        and left.registry_version == right.registry_version
        and left.registry_sha256 == right.registry_sha256
        and left.source_stream_ids == right.source_stream_ids
        and left.flags == right.flags
        and left.payload_projection == right.payload_projection
    )


def _parse_metadata(
    data: Union[bytes, bytearray],
    offset: int = 0,
) -> InstrumentTickDeltaMetadata:
    base_kind_raw, selected_source_mask = _METADATA_HEAD.unpack_from(
        data, offset
    )
    try:
        base_kind = InstrumentTickDeltaBaseKind(base_kind_raw)
    except ValueError as error:
        raise WireFormatError(
            "instrument delta metadata has invalid base_kind"
        ) from error
    if selected_source_mask != INSTRUMENT_TICK_DELTA_SOURCE_MASK_V2:
        raise WireFormatError(
            "instrument delta metadata source mask mismatch"
        )

    base_offset = offset + 8
    target_offset = (
        base_offset + INSTRUMENT_TICK_DELTA_CHECKPOINT_BYTES_V2
    )
    if base_kind is InstrumentTickDeltaBaseKind.ORIGIN:
        if any(
            data[
                base_offset:
                base_offset + INSTRUMENT_TICK_DELTA_CHECKPOINT_BYTES_V2
            ]
        ):
            raise WireFormatError(
                "origin instrument delta must carry a zero base checkpoint"
            )
        base_checkpoint = None
    else:
        base_checkpoint = _parse_checkpoint(data, base_offset)
    target_checkpoint = _parse_checkpoint(data, target_offset)
    tail = _METADATA_TAIL.unpack_from(data, offset + 648)
    delta_counts = tuple(tail[0:4])
    delta_total = tail[4]
    ingress_begin = tail[5]
    ingress_end = tail[6]
    tick_begin = tail[7]
    tick_end = tail[8]
    flags = tail[9]
    payload_projection = tail[10]
    reserved = tail[11]
    if any(reserved):
        raise WireFormatError(
            "instrument delta metadata reserved bytes are nonzero"
        )
    if delta_counts[0] != 0 or delta_counts[2] != 0:
        raise WireFormatError(
            "instrument delta snapshot source counts are nonzero"
        )
    if sum(delta_counts) > _UINT64_MAX or delta_total != sum(
        delta_counts
    ):
        raise WireFormatError(
            "instrument delta metadata total count mismatch"
        )
    target_endpoint = _checkpoint_endpoint(target_checkpoint)
    if (
        flags != target_endpoint.flags
        or payload_projection
        != target_endpoint.payload_projection
    ):
        raise WireFormatError(
            "instrument delta metadata coverage/projection mismatch"
        )
    if (
        ingress_end
        != target_checkpoint.ingress_sequence_exclusive
        or tick_end
        != target_checkpoint.tick_stream_sequence_exclusive
    ):
        raise WireFormatError(
            "instrument delta metadata target bounds mismatch"
        )

    if base_checkpoint is None:
        base_counts = (0, 0, 0, 0)
        base_source_endpoints = (1, 1, 1, 1)
        if ingress_begin != 1 or tick_begin != 1:
            raise WireFormatError(
                "origin instrument delta must begin at sequence one"
            )
    else:
        base_endpoint = _checkpoint_endpoint(base_checkpoint)
        if not _same_static_generation(
            base_endpoint, target_endpoint
        ):
            raise WireFormatError(
                "instrument delta checkpoint endpoints disagree"
            )
        if (
            base_checkpoint.instrument_id
            != target_checkpoint.instrument_id
            or base_checkpoint.registry_ordinal
            != target_checkpoint.registry_ordinal
        ):
            raise WireFormatError(
                "instrument delta checkpoint instrument changed"
            )
        try:
            target_checkpoint.ensure_successor_of(base_checkpoint)
        except StaleSessionError as error:
            raise WireFormatError(str(error)) from error
        if (
            ingress_begin
            != base_checkpoint.ingress_sequence_exclusive
            or tick_begin
            != base_checkpoint.tick_stream_sequence_exclusive
        ):
            raise WireFormatError(
                "instrument delta metadata base bounds mismatch"
            )
        base_counts = base_checkpoint.instrument_tick_counts
        base_source_endpoints = (
            base_checkpoint.source_sequence_exclusive
        )

    expected_delta_counts = tuple(
        target - base
        for target, base in zip(
            target_checkpoint.instrument_tick_counts, base_counts
        )
    )
    if delta_counts != expected_delta_counts:
        raise WireFormatError(
            "instrument delta counts do not equal target minus base"
        )
    source_endpoint_deltas = tuple(
        target - base
        for target, base in zip(
            target_checkpoint.source_sequence_exclusive,
            base_source_endpoints,
        )
    )
    if any(
        delta_counts[slot] > source_endpoint_deltas[slot]
        for slot in (1, 3)
    ):
        raise WireFormatError(
            "instrument-local delta count exceeds its source delta"
        )
    if ingress_begin > ingress_end or tick_begin > tick_end:
        raise WireFormatError(
            "instrument delta half-open bounds move backwards"
        )
    ingress_delta = ingress_end - ingress_begin
    tick_delta = tick_end - tick_begin
    if delta_total > tick_delta or tick_delta > ingress_delta:
        raise WireFormatError(
            "instrument/tick/ingress delta counts do not nest"
        )
    return InstrumentTickDeltaMetadata(
        base_kind=base_kind,
        base_checkpoint=base_checkpoint,
        target_checkpoint=target_checkpoint,
        delta_tick_source_record_counts=delta_counts,
        delta_tick_record_count=delta_total,
        ingress_sequence_begin_inclusive=ingress_begin,
        ingress_sequence_end_exclusive=ingress_end,
        tick_stream_sequence_begin_inclusive=tick_begin,
        tick_stream_sequence_end_exclusive=tick_end,
        flags=flags,
        payload_projection=payload_projection,
    )


def _validate_expected_generation(
    generation: InstrumentTickDeltaGeneration,
    *,
    expected_run_id: Optional[bytes],
    expected_session_epoch: Optional[int],
    expected_trade_date: Optional[int],
    expected_instrument_count: Optional[int],
    expected_registry_version: Optional[int],
    expected_registry_sha256: Optional[bytes],
) -> None:
    if (
        expected_run_id is not None
        and generation.run_id != expected_run_id
    ):
        raise StaleSessionError(
            "instrument delta service run_id changed"
        )
    if (
        expected_session_epoch is not None
        and generation.session_epoch != expected_session_epoch
    ):
        raise StaleSessionError(
            "instrument delta service session_epoch changed"
        )
    if (
        expected_trade_date is not None
        and generation.trade_date != expected_trade_date
    ):
        raise StaleSessionError(
            "instrument delta service trade_date changed"
        )
    if (
        expected_instrument_count is not None
        and generation.instrument_count != expected_instrument_count
    ):
        raise StaleSessionError(
            "instrument delta service instrument_count changed"
        )
    if (
        expected_registry_version is not None
        and generation.registry_version != expected_registry_version
    ):
        raise StaleSessionError(
            "instrument delta service registry_version changed"
        )
    if (
        expected_registry_sha256 is not None
        and generation.registry_sha256 != expected_registry_sha256
    ):
        raise StaleSessionError(
            "instrument delta service registry_sha256 changed"
        )


def _parse_page(
    validator: NativeInstrumentTickDeltaPageValidator,
    fd: int,
    *,
    expected_bytes: int,
    expected_records: int,
    expected_page_index: int,
    metadata_wire: bytes,
    prior_ingress_sequence: int,
    prior_tick_stream_sequence: int,
    prior_source_sequences: Tuple[int, int, int, int],
):
    return validator.validate(
        fd,
        expected_mapping_bytes=expected_bytes,
        expected_record_count=expected_records,
        expected_page_index=expected_page_index,
        expected_metadata=metadata_wire,
        prior_ingress_sequence=prior_ingress_sequence,
        prior_tick_stream_sequence=prior_tick_stream_sequence,
        prior_source_sequences=prior_source_sequences,
    )


@dataclass(slots=True)
class InstrumentTickDeltaCursor:
    """One finite instrument cursor within a pinned delta session."""

    _session: "InstrumentTickDeltaSession"
    _metadata: InstrumentTickDeltaMetadata
    requested_page_records: int
    _read_token: int
    _next_page_index: int = 0
    _cumulative_record_count: int = 0
    _cumulative_source_counts: Tuple[int, int, int, int] = (
        0,
        0,
        0,
        0,
    )
    _last_ingress_sequence: int = 0
    _last_tick_stream_sequence: int = 0
    _last_source_sequences: Tuple[int, int, int, int] = (
        0,
        0,
        0,
        0,
    )
    _closed: bool = False
    _eof: bool = False
    _metadata_wire: bytes = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._metadata_wire = _pack_metadata(self._metadata)

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

    @property
    def verified_checkpoint(self) -> InstrumentTickDeltaCheckpoint:
        if not self._eof:
            raise InstrumentTickDeltaCheckpointUnavailableError(
                "target checkpoint is unavailable before explicit EOF"
            )
        return self._metadata.target_checkpoint

    def _require_open(self) -> None:
        if self._closed:
            raise ClientClosedError(
                "instrument delta cursor is closed"
            )
        if self._eof:
            raise ClientClosedError(
                "instrument delta cursor is already at EOF"
            )

    def close(self) -> None:
        if self._eof:
            self._closed = True
            return
        self._closed = True
        self._session.close()

    def __enter__(self) -> "InstrumentTickDeltaCursor":
        self._require_open()
        return self

    def __exit__(self, _type, _value, _traceback) -> None:
        self.close()

    def read(self) -> InstrumentTickDeltaPage:
        """Read one dense data page or the explicit terminal page."""

        with self._session._lock:
            self._require_open()
            self._session._require_open()
            if self._next_page_index > _UINT64_MAX:
                self._session._fail_closed()
                raise UnavailableError(
                    "instrument delta page index space is exhausted"
                )
            request_id = _request_id()
            request = _READ_REQUEST.pack(
                CONTROL_MAGIC,
                WIRE_MAJOR,
                WIRE_MINOR,
                READ_INSTRUMENT_TICK_DELTA_OPCODE_V2,
                0,
                READ_INSTRUMENT_TICK_DELTA_REQUEST_BYTES_V2,
                0,
                request_id,
                self._next_page_index,
                self._read_token,
            )
            try:
                _send_packet(self._session._channel, request)
                packet = _recv_packet(
                    self._session._channel,
                    READ_INSTRUMENT_TICK_DELTA_RESPONSE_BYTES_V2,
                )
            except BaseException:
                self._session._fail_closed()
                raise
            try:
                response = packet.data
                fds = packet.fds
                status, response_flags, row_count = (
                    _validate_response_prefix(
                        response,
                        expected_bytes=(
                            READ_INSTRUMENT_TICK_DELTA_RESPONSE_BYTES_V2
                        ),
                        expected_request_id=request_id,
                        allowed_flags=(
                            INSTRUMENT_TICK_DELTA_RESPONSE_TERMINAL_V2
                        ),
                        operation="read_instrument_tick_delta",
                        reserved_is_record_count=True,
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
                    _response_request_id,
                    page_bytes,
                    page_index,
                    target_generation,
                    next_read_token,
                ) = _READ_RESPONSE.unpack(response)
                if status != INSTRUMENT_TICK_DELTA_OK:
                    if fds:
                        raise ProtocolError(
                            "failed instrument delta read carried an fd"
                        )
                    if (
                        response_flags != 0
                        or row_count != 0
                        or page_bytes != 0
                        or page_index != 0
                        or target_generation != 0
                        or next_read_token != 0
                    ):
                        raise ProtocolError(
                            "failed instrument delta read carried "
                            "success metadata"
                        )
                    _raise_status(
                        "read_instrument_tick_delta", status
                    )
                if page_index != self._next_page_index:
                    raise ProtocolError(
                        "instrument delta response page index mismatch"
                    )
                if (
                    target_generation
                    != self._metadata.target_checkpoint.generation
                ):
                    raise ProtocolError(
                        "instrument delta response target changed"
                    )
                terminal = (
                    response_flags
                    & INSTRUMENT_TICK_DELTA_RESPONSE_TERMINAL_V2
                ) != 0
                if terminal:
                    if (
                        row_count != 0
                        or page_bytes != 0
                        or fds
                        or next_read_token != 0
                    ):
                        raise ProtocolError(
                            "instrument delta terminal response must "
                            "be zero-row and carry no fd"
                        )
                    if (
                        self._cumulative_record_count
                        != self._metadata.delta_tick_record_count
                        or self._cumulative_source_counts
                        != self._metadata
                        .delta_tick_source_record_counts
                    ):
                        raise WireFormatError(
                            "instrument delta EOF counts do not "
                            "reconcile"
                        )
                    self._eof = True
                    self._closed = True
                    self._next_page_index += 1
                    self._session._finish_cursor(self)
                    return InstrumentTickDeltaPage(
                        instrument_id=self.instrument_id,
                        page_index=page_index,
                        wire_records=b"",
                        eof=True,
                        cumulative_record_count=(
                            self._cumulative_record_count
                        ),
                        cumulative_source_record_counts=(
                            self._cumulative_source_counts
                        ),
                    )

                maximum_page_bytes = (
                    INSTRUMENT_TICK_DELTA_PAGE_HEADER_BYTES_V2
                    + row_count * TICK_BYTES
                )
                if (
                    row_count == 0
                    or row_count > self.requested_page_records
                    or page_bytes != maximum_page_bytes
                    or len(fds) != 1
                    or next_read_token == 0
                    or next_read_token == self._read_token
                ):
                    raise ProtocolError(
                        "instrument delta data response metadata "
                        "is invalid"
                    )
                native_page = self._session._validate_page(
                    fds[0],
                    expected_bytes=page_bytes,
                    expected_records=row_count,
                    expected_page_index=self._next_page_index,
                    metadata_wire=self._metadata_wire,
                    prior_ingress_sequence=(
                        self._last_ingress_sequence
                    ),
                    prior_tick_stream_sequence=(
                        self._last_tick_stream_sequence
                    ),
                    prior_source_sequences=(
                        self._last_source_sequences
                    ),
                )
                new_total = (
                    self._cumulative_record_count
                    + tick_wire_record_count(native_page.wire_records)
                )
                new_source_counts = tuple(
                    current + added
                    for current, added in zip(
                        self._cumulative_source_counts,
                        native_page.source_counts,
                    )
                )
                if (
                    new_total
                    > self._metadata.delta_tick_record_count
                    or any(
                        current > expected
                        for current, expected in zip(
                            new_source_counts,
                            self._metadata
                            .delta_tick_source_record_counts,
                        )
                    )
                ):
                    raise WireFormatError(
                        "instrument delta pages exceed declared counts"
                    )
                self._cumulative_record_count = new_total
                self._cumulative_source_counts = new_source_counts
                self._last_ingress_sequence = (
                    native_page.last_ingress_sequence
                )
                self._last_tick_stream_sequence = (
                    native_page.last_tick_stream_sequence
                )
                self._last_source_sequences = (
                    native_page.last_source_sequences
                )
                self._read_token = next_read_token
                self._next_page_index += 1
                return InstrumentTickDeltaPage(
                    instrument_id=self.instrument_id,
                    page_index=page_index,
                    wire_records=native_page.wire_records,
                    eof=False,
                    cumulative_record_count=new_total,
                    cumulative_source_record_counts=(
                        new_source_counts
                    ),
                )
            except BaseException:
                self._session._fail_closed()
                raise
            finally:
                packet.close()

    def pages(self):
        """Yield data pages followed by the explicit terminal page."""

        while not self.done:
            yield self.read()


class InstrumentTickDeltaSession:
    """One socket connection with a fixed immutable target generation."""

    def __init__(
        self,
        channel: socket.socket,
        target_generation: InstrumentTickDeltaGeneration,
        delta_session_token: int,
        requested_page_records: int,
    ) -> None:
        self._channel = channel
        self.target_generation = target_generation
        self._delta_session_token = delta_session_token
        self._requested_page_records = requested_page_records
        self._active_cursor: Optional[
            InstrumentTickDeltaCursor
        ] = None
        self._page_validator: Optional[
            NativeInstrumentTickDeltaPageValidator
        ] = None
        self._closed = False
        self._lock = threading.RLock()

    @property
    def session_identity(self) -> SessionIdentity:
        return self.target_generation.session_identity

    @property
    def closed(self) -> bool:
        return self._closed

    def _require_open(self) -> None:
        if self._closed:
            raise ClientClosedError(
                "instrument delta session is closed"
            )

    def _fail_closed(self) -> None:
        self._closed = True
        if self._active_cursor is not None:
            self._active_cursor._closed = True
        self._active_cursor = None
        self._channel.close()

    def _finish_cursor(
        self, cursor: InstrumentTickDeltaCursor
    ) -> None:
        if self._active_cursor is cursor:
            self._active_cursor = None

    def _validate_page(
        self,
        fd: int,
        *,
        expected_bytes: int,
        expected_records: int,
        expected_page_index: int,
        metadata_wire: bytes,
        prior_ingress_sequence: int,
        prior_tick_stream_sequence: int,
        prior_source_sequences: Tuple[int, int, int, int],
    ):
        if self._page_validator is None:
            self._page_validator = (
                NativeInstrumentTickDeltaPageValidator()
            )
        return _parse_page(
            self._page_validator,
            fd,
            expected_bytes=expected_bytes,
            expected_records=expected_records,
            expected_page_index=expected_page_index,
            metadata_wire=metadata_wire,
            prior_ingress_sequence=prior_ingress_sequence,
            prior_tick_stream_sequence=prior_tick_stream_sequence,
            prior_source_sequences=prior_source_sequences,
        )

    def close(self) -> None:
        try:
            self._channel.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        with self._lock:
            if not self._closed:
                self._fail_closed()

    def __enter__(self) -> "InstrumentTickDeltaSession":
        self._require_open()
        return self

    def __exit__(self, _type, _value, _traceback) -> None:
        self.close()

    def open_instrument(
        self,
        instrument_id: int,
        *,
        after: Optional[InstrumentTickDeltaCheckpoint] = None,
        requested_page_records: Optional[int] = None,
        request_id: Optional[int] = None,
    ) -> InstrumentTickDeltaCursor:
        """Open one origin or verified-checkpoint delta sequentially."""

        instrument_id = _positive_uint32(
            instrument_id, "instrument_id"
        )
        if requested_page_records is None:
            requested_page_records = self._requested_page_records
        requested_page_records = _positive_uint32(
            requested_page_records, "requested_page_records"
        )
        if requested_page_records > _MAXIMUM_PAGE_RECORDS:
            raise ValueError(
                "requested_page_records exceeds the absolute limit"
            )
        if after is not None:
            if not isinstance(after, InstrumentTickDeltaCheckpoint):
                raise TypeError(
                    "after must be a verified checkpoint or None"
                )
            if after.instrument_id != instrument_id:
                raise StaleSessionError(
                    "checkpoint belongs to another instrument"
                )
            target = self.target_generation
            after.ensure_session(
                run_id=target.run_id,
                session_epoch=target.session_epoch,
                trade_date=target.trade_date,
                instrument_count=target.instrument_count,
                registry_version=target.registry_version,
                registry_sha256=target.registry_sha256,
            )
            after_endpoint = _checkpoint_endpoint(after)
            if (
                after.generation > target.generation
                or after.ingress_sequence_exclusive
                > target.ingress_sequence_exclusive
                or after.tick_stream_sequence_exclusive
                > target.tick_stream_sequence_exclusive
                or after.recv_monotonic_cut_ns
                > target.recv_monotonic_cut_ns
                or any(
                    base > end
                    for base, end in zip(
                        after.source_sequence_exclusive,
                        target.source_sequence_exclusive,
                    )
                )
                or not _same_static_generation(
                    after_endpoint, target
                )
            ):
                raise InstrumentTickDeltaCheckpointMismatchError(
                    "checkpoint is not a predecessor of the "
                    "pinned target"
                )
            if (
                after.generation == target.generation
                and after_endpoint != target
            ):
                raise InstrumentTickDeltaCheckpointMismatchError(
                    "checkpoint conflicts with the same target generation"
                )
            base_kind = InstrumentTickDeltaBaseKind.CHECKPOINT
            packed_checkpoint = _pack_checkpoint(after)
        else:
            base_kind = InstrumentTickDeltaBaseKind.ORIGIN
            packed_checkpoint = bytes(
                INSTRUMENT_TICK_DELTA_CHECKPOINT_BYTES_V2
            )
        request_id = _request_id(request_id)
        request = bytearray(
            OPEN_INSTRUMENT_TICK_DELTA_REQUEST_BYTES_V2
        )
        _OPEN_INSTRUMENT_PREFIX.pack_into(
            request,
            0,
            CONTROL_MAGIC,
            WIRE_MAJOR,
            WIRE_MINOR,
            OPEN_INSTRUMENT_TICK_DELTA_OPCODE_V2,
            0,
            OPEN_INSTRUMENT_TICK_DELTA_REQUEST_BYTES_V2,
            0,
            request_id,
            instrument_id,
            requested_page_records,
            int(base_kind),
            0,
            self._delta_session_token,
        )
        request[56:376] = packed_checkpoint

        with self._lock:
            self._require_open()
            if self._active_cursor is not None:
                raise RuntimeError(
                    "the prior instrument cursor must reach explicit "
                    "EOF first"
                )
            try:
                _send_packet(self._channel, bytes(request))
                packet = _recv_packet(
                    self._channel,
                    OPEN_INSTRUMENT_TICK_DELTA_RESPONSE_BYTES_V2,
                )
            except BaseException:
                self._fail_closed()
                raise
            try:
                response = packet.data
                fds = packet.fds
                status, flags, _reserved = _validate_response_prefix(
                    response,
                    expected_bytes=(
                        OPEN_INSTRUMENT_TICK_DELTA_RESPONSE_BYTES_V2
                    ),
                    expected_request_id=request_id,
                    allowed_flags=0,
                    operation="open_instrument_tick_delta",
                )
                if flags != 0 or fds:
                    raise ProtocolError(
                        "instrument delta open response flags/fd "
                        "are invalid"
                    )
                if status != INSTRUMENT_TICK_DELTA_OK:
                    if any(response[32:]):
                        raise ProtocolError(
                            "failed instrument delta open carried "
                            "success metadata"
                        )
                    _raise_status(
                        "open_instrument_tick_delta", status
                    )
                initial_read_token = struct.unpack_from(
                    "<Q", response, 32
                )[0]
                if initial_read_token == 0:
                    raise ProtocolError(
                        "instrument delta open returned zero read token"
                    )
                metadata = _parse_metadata(response, 40)
                if metadata.base_kind is not base_kind:
                    raise ProtocolError(
                        "instrument delta response base mode changed"
                    )
                if metadata.instrument_id != instrument_id:
                    raise WireFormatError(
                        "instrument delta response instrument changed"
                    )
                if (
                    base_kind
                    is InstrumentTickDeltaBaseKind.CHECKPOINT
                    and metadata.base_checkpoint != after
                ):
                    raise InstrumentTickDeltaCheckpointMismatchError(
                        "server did not echo the requested checkpoint"
                    )
                if (
                    _checkpoint_endpoint(
                        metadata.target_checkpoint
                    )
                    != self.target_generation
                ):
                    raise StaleSessionError(
                        "instrument delta target generation changed"
                    )
                cursor = InstrumentTickDeltaCursor(
                    _session=self,
                    _metadata=metadata,
                    requested_page_records=requested_page_records,
                    _read_token=initial_read_token,
                )
                self._active_cursor = cursor
                return cursor
            except (
                InstrumentTickDeltaNotFoundError,
                InstrumentTickDeltaResourceExhaustedError,
                InstrumentTickDeltaCheckpointMismatchError,
            ):
                raise
            except BaseException:
                self._fail_closed()
                raise
            finally:
                packet.close()


def open_instrument_tick_delta_session(
    control_socket_path: Union[str, os.PathLike],
    *,
    requested_page_records: int = (
        DEFAULT_INSTRUMENT_TICK_DELTA_PAGE_RECORDS
    ),
    timeout: Optional[float] = 1.0,
    request_id: Optional[int] = None,
    expected_run_id: Optional[bytes] = None,
    expected_session_epoch: Optional[int] = None,
    expected_trade_date: Optional[int] = None,
    expected_instrument_count: Optional[int] = None,
    expected_registry_version: Optional[int] = None,
    expected_registry_sha256: Optional[bytes] = None,
) -> InstrumentTickDeltaSession:
    """Connect once and pin one immutable Store target generation."""

    path = _validate_socket_path(control_socket_path)
    requested_page_records = _positive_uint32(
        requested_page_records, "requested_page_records"
    )
    if requested_page_records > _MAXIMUM_PAGE_RECORDS:
        raise ValueError(
            "requested_page_records exceeds the absolute limit"
        )
    timeout = _validate_timeout(timeout)
    request_id = _request_id(request_id)
    if expected_run_id is not None:
        expected_run_id = bytes(expected_run_id)
        if len(expected_run_id) != 16 or not any(expected_run_id):
            raise ValueError(
                "expected_run_id must be a nonzero 16-byte value"
            )
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
                "expected_registry_sha256 must be a nonzero "
                "32-byte value"
            )
    request = _OPEN_SESSION_REQUEST.pack(
        CONTROL_MAGIC,
        WIRE_MAJOR,
        WIRE_MINOR,
        OPEN_INSTRUMENT_TICK_DELTA_SESSION_OPCODE_V2,
        0,
        OPEN_INSTRUMENT_TICK_DELTA_SESSION_REQUEST_BYTES_V2,
        0,
        request_id,
        0,
    )
    channel = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    try:
        channel.settimeout(timeout)
        channel.connect(path)
        _send_packet(channel, request)
        packet = _recv_packet(
            channel,
            OPEN_INSTRUMENT_TICK_DELTA_SESSION_RESPONSE_BYTES_V2,
        )
        try:
            response = packet.data
            fds = packet.fds
            status, flags, _reserved = _validate_response_prefix(
                response,
                expected_bytes=(
                    OPEN_INSTRUMENT_TICK_DELTA_SESSION_RESPONSE_BYTES_V2
                ),
                expected_request_id=request_id,
                allowed_flags=0,
                operation="open_instrument_tick_delta_session",
            )
            if flags != 0 or fds:
                raise ProtocolError(
                    "instrument delta session response flags/fd "
                    "are invalid"
                )
            if status != INSTRUMENT_TICK_DELTA_OK:
                if any(response[32:]):
                    raise ProtocolError(
                        "failed instrument delta session open carried "
                        "success metadata"
                    )
                _raise_status(
                    "open_instrument_tick_delta_session", status
                )
            target = _parse_generation_endpoint(response, 32)
            session_token = struct.unpack_from("<Q", response, 288)[0]
            if session_token == 0:
                raise ProtocolError(
                    "instrument delta session returned zero token"
                )
            _validate_expected_generation(
                target,
                expected_run_id=expected_run_id,
                expected_session_epoch=expected_session_epoch,
                expected_trade_date=expected_trade_date,
                expected_instrument_count=expected_instrument_count,
                expected_registry_version=expected_registry_version,
                expected_registry_sha256=expected_registry_sha256,
            )
        finally:
            packet.close()
        return InstrumentTickDeltaSession(
            channel,
            target,
            session_token,
            requested_page_records,
        )
    except BaseException:
        channel.close()
        raise


__all__ = [
    "DEFAULT_INSTRUMENT_TICK_DELTA_PAGE_RECORDS",
    "InstrumentTickDeltaBaseKind",
    "InstrumentTickDeltaCheckpointMismatchError",
    "InstrumentTickDeltaCheckpointUnavailableError",
    "InstrumentTickDeltaCursor",
    "InstrumentTickDeltaGeneration",
    "InstrumentTickDeltaInternalFailureError",
    "InstrumentTickDeltaMetadata",
    "InstrumentTickDeltaNotFoundError",
    "InstrumentTickDeltaPage",
    "InstrumentTickDeltaResourceExhaustedError",
    "InstrumentTickDeltaSession",
    "open_instrument_tick_delta_session",
]
