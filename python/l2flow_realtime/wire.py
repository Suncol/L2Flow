"""Fixed-width little-endian cursor/status codecs for L2Flow Wire V3."""

from __future__ import annotations

import struct
from typing import Union

from .models import (
    Dataset,
    EventChangeCursor,
    FastTickCursor,
    InstrumentStableStatus,
    KLineChangeCursor,
    RepairState,
)


WIRE_MAGIC = b"L2F3"
WIRE_MAJOR = 3
WIRE_MINOR = 0

_CURSOR = struct.Struct("<16sIIQ")
_STATUS = struct.Struct("<4sHHBBHIQQ")
assert _CURSOR.size == 32
assert _STATUS.size == 32

Cursor = Union[FastTickCursor, EventChangeCursor, KLineChangeCursor]


def encode_cursor(cursor: Cursor) -> bytes:
    """Encode any V3 instrument cursor into its common 32-byte ABI."""

    if isinstance(cursor, FastTickCursor):
        offset = cursor.next_arrival_row
    elif isinstance(cursor, (EventChangeCursor, KLineChangeCursor)):
        offset = cursor.next_change_sequence
    else:
        raise TypeError("cursor must be a V3 instrument cursor")
    return _CURSOR.pack(cursor.session_id, cursor.instrument_id, 0, offset)


def _decode_cursor_fields(data: bytes) -> tuple[bytes, int, int]:
    if not isinstance(data, bytes) or len(data) != _CURSOR.size:
        raise ValueError("a V3 cursor must contain exactly 32 bytes")
    session_id, instrument_id, reserved, offset = _CURSOR.unpack(data)
    if reserved != 0:
        raise ValueError("V3 cursor reserved bits are nonzero")
    return session_id, instrument_id, offset


def decode_fast_tick_cursor(data: bytes) -> FastTickCursor:
    session_id, instrument_id, offset = _decode_cursor_fields(data)
    return FastTickCursor(session_id, instrument_id, offset)


def decode_event_change_cursor(data: bytes) -> EventChangeCursor:
    session_id, instrument_id, offset = _decode_cursor_fields(data)
    return EventChangeCursor(session_id, instrument_id, offset)


def decode_kline_change_cursor(data: bytes) -> KLineChangeCursor:
    session_id, instrument_id, offset = _decode_cursor_fields(data)
    return KLineChangeCursor(session_id, instrument_id, offset)


def encode_stable_status(status: InstrumentStableStatus) -> bytes:
    return _STATUS.pack(
        WIRE_MAGIC,
        WIRE_MAJOR,
        WIRE_MINOR,
        int(status.dataset),
        int(status.repair_state),
        0,
        status.instrument_id,
        status.stable_tail,
        status.repair_through_arrival_id,
    )


def decode_stable_status(data: bytes) -> InstrumentStableStatus:
    if not isinstance(data, bytes) or len(data) != _STATUS.size:
        raise ValueError("a V3 stable status must contain exactly 32 bytes")
    (
        magic,
        major,
        minor,
        dataset,
        repair_state,
        reserved,
        instrument_id,
        stable_tail,
        repair_through,
    ) = _STATUS.unpack(data)
    if magic != WIRE_MAGIC or major != WIRE_MAJOR:
        raise ValueError("incompatible L2Flow Wire major version")
    if minor > WIRE_MINOR or reserved != 0:
        raise ValueError("unsupported V3 status encoding")
    return InstrumentStableStatus(
        Dataset(dataset),
        RepairState(repair_state),
        instrument_id,
        stable_tail,
        repair_through,
    )
