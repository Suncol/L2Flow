"""Parsers for client-owned copies of the fixed little-endian V1 wire ABI."""

from __future__ import annotations

import struct
from typing import Tuple

from .models import (
    Aggressor,
    BestQueue,
    BookLevel,
    CommonRecord,
    DecimalValue,
    Instrument,
    KLine,
    Market,
    MarketEventKind,
    OrderType,
    QuantityValue,
    Side,
    Snapshot,
    Tick,
    TickAction,
    TradingPhase,
    WireFormatError,
)


WIRE_MAJOR = 1
WIRE_MINOR = 0
CONTROL_MAGIC = b"L2FCTL1\x00"
SNAPSHOT_BYTES = 3104
TICK_BYTES = 336
KLINE_BYTES = 192
INSTRUMENT_BYTES = 64

_INSTRUMENT = struct.Struct("<IBBBBQIIQIIQQQ")
_DECIMAL = struct.Struct("<qqBBB5x")
_QUANTITY = struct.Struct("<qBBB5x")
_COMMON = struct.Struct("<IIIIQQQQqqqQQQIIII6B10x")
_SNAPSHOT_META = struct.Struct("<qiI")
_BOOK_TAIL = struct.Struct("<IB3x")
_QUEUE_HEADER = struct.Struct("<IIII")
_TICK_HEAD = struct.Struct("<IIqqii8B")
_KLINE = struct.Struct(
    "<QIIIIQQQ" + "q" * 6 + "Q" * 11 + "BBB5x"
)

assert _INSTRUMENT.size == INSTRUMENT_BYTES
assert _DECIMAL.size == 24
assert _QUANTITY.size == 16
assert _COMMON.size == 128
assert _SNAPSHOT_META.size == 16
assert _BOOK_TAIL.size == 8
assert _QUEUE_HEADER.size == 16
assert _TICK_HEAD.size == 40
assert _KLINE.size == KLINE_BYTES


def _require_size(data: bytes, expected: int, name: str) -> None:
    if len(data) != expected:
        raise WireFormatError(
            f"{name} payload has {len(data)} bytes; expected {expected}"
        )


def _wire_bool(value: int, field: str) -> bool:
    if value not in (0, 1):
        raise WireFormatError(f"{field} is not a canonical wire boolean")
    return value == 1


def _enum(enum_type, value: int, field: str):
    try:
        return enum_type(value)
    except ValueError as error:
        raise WireFormatError(
            f"{field} contains unsupported value {value}"
        ) from error


def _decimal(data: bytes, offset: int) -> DecimalValue:
    raw, normalized_p6, scale, valid, is_null = _DECIMAL.unpack_from(
        data, offset
    )
    if any(data[offset + 19 : offset + 24]):
        raise WireFormatError("decimal reserved bytes are nonzero")
    return DecimalValue(
        raw=raw,
        normalized_p6=normalized_p6,
        scale=scale,
        valid=_wire_bool(valid, "decimal.valid"),
        is_null=_wire_bool(is_null, "decimal.is_null"),
    )


def _quantity(data: bytes, offset: int) -> QuantityValue:
    raw, scale, valid, is_null = _QUANTITY.unpack_from(data, offset)
    if any(data[offset + 11 : offset + 16]):
        raise WireFormatError("quantity reserved bytes are nonzero")
    return QuantityValue(
        raw=raw,
        scale=scale,
        valid=_wire_bool(valid, "quantity.valid"),
        is_null=_wire_bool(is_null, "quantity.is_null"),
    )


def _common(data: bytes, expected_bytes: int) -> CommonRecord:
    fields = _COMMON.unpack_from(data, 0)
    if fields[0] != 1:
        raise WireFormatError(
            f"unsupported record schema version {fields[0]}"
        )
    if fields[1] != expected_bytes:
        raise WireFormatError(
            f"record_bytes is {fields[1]}; expected {expected_bytes}"
        )
    if fields[2] == 0 or fields[5] == 0:
        raise WireFormatError(
            "available records require nonzero instrument and ingress IDs"
        )
    if fields[17] != 0 or any(data[118:128]):
        raise WireFormatError("common record reserved fields are nonzero")
    result = CommonRecord(
        record_schema_version=fields[0],
        record_bytes=fields[1],
        instrument_id=fields[2],
        registry_ordinal=fields[3],
        source_sequence=fields[4],
        ingress_sequence=fields[5],
        tick_stream_sequence=fields[6],
        vendor_sequence_id=fields[7],
        event_time_unix_ns=fields[8],
        recv_realtime_ns=fields[9],
        recv_monotonic_ns=fields[10],
        exchange_time_ns_since_midnight=fields[11],
        quality_flags=fields[12],
        market_notices=fields[13],
        source_stream_id=fields[14],
        trade_date=fields[15],
        vendor_local_time_raw=fields[16],
        source_slot=fields[18],
        event_kind=_enum(MarketEventKind, fields[19], "event_kind"),
        market=_enum(Market, fields[20], "market"),
        quantity_unit=fields[21],
        security_type=fields[22],
        asset_scope=fields[23],
    )
    if result.source_slot >= 4:
        raise WireFormatError("source_slot exceeds the four-source ABI")
    expected_source_slot = {
        MarketEventKind.SHANGHAI_SNAPSHOT: 0,
        MarketEventKind.SHANGHAI_TICK: 1,
        MarketEventKind.SHENZHEN_SNAPSHOT: 2,
        MarketEventKind.SHENZHEN_ORDER: 3,
        MarketEventKind.SHENZHEN_TRANSACTION: 3,
    }[result.event_kind]
    if result.source_slot != expected_source_slot:
        raise WireFormatError("event_kind and source_slot disagree")
    return result


def parse_instrument(
    row: bytes, security_id_source: bytes, security_id: bytes
) -> Instrument:
    _require_size(row, INSTRUMENT_BYTES, "instrument")
    fields = _INSTRUMENT.unpack(row)
    if fields[0] == 0:
        raise WireFormatError("instrument_id cannot be zero")
    if fields[6] != len(security_id_source):
        raise WireFormatError("security_id_source length does not match row")
    if fields[9] != len(security_id):
        raise WireFormatError("security_id length does not match row")
    if any(fields[index] != 0 for index in (7, 10, 11, 12, 13)):
        raise WireFormatError("instrument reserved fields are nonzero")
    return Instrument(
        instrument_id=fields[0],
        market=fields[1],
        quantity_unit=fields[2],
        security_type=fields[3],
        asset_scope=fields[4],
        security_id_source=bytes(security_id_source),
        security_id=bytes(security_id),
    )


def _book_level(data: bytes, offset: int) -> BookLevel:
    order_count, order_count_valid = _BOOK_TAIL.unpack_from(
        data, offset + 40
    )
    if any(data[offset + 45 : offset + 48]):
        raise WireFormatError("book level reserved bytes are nonzero")
    return BookLevel(
        price=_decimal(data, offset),
        quantity=_quantity(data, offset + 24),
        order_count=order_count,
        order_count_valid=_wire_bool(
            order_count_valid, "book.order_count_valid"
        ),
    )


def _best_queue(
    data: bytes, header_offset: int, quantities_offset: int
) -> BestQueue:
    total, actual, retained, _reserved = _QUEUE_HEADER.unpack_from(
        data, header_offset
    )
    if _reserved != 0:
        raise WireFormatError("best queue reserved field is nonzero")
    if retained > 50:
        raise WireFormatError("best queue retained_count exceeds 50")
    quantities = tuple(
        _quantity(data, quantities_offset + index * _QUANTITY.size)
        for index in range(50)
    )
    return BestQueue(total, actual, retained, quantities)


def parse_snapshot(data: bytes) -> Snapshot:
    _require_size(data, SNAPSHOT_BYTES, "snapshot")
    common = _common(data, SNAPSHOT_BYTES)
    if common.event_kind not in (
        MarketEventKind.SHANGHAI_SNAPSHOT,
        MarketEventKind.SHENZHEN_SNAPSHOT,
    ):
        raise WireFormatError("snapshot payload contains a non-snapshot kind")
    if common.tick_stream_sequence != 0:
        raise WireFormatError("snapshot tick_stream_sequence must be zero")
    expected_market = (
        Market.SHANGHAI
        if common.event_kind is MarketEventKind.SHANGHAI_SNAPSHOT
        else Market.SHENZHEN
    )
    if common.market is not expected_market:
        raise WireFormatError("snapshot event kind and market disagree")

    trade_count, image_status, channel = _SNAPSHOT_META.unpack_from(data, 128)
    cursor = 144
    pre_close_price = _decimal(data, cursor)
    cursor += 24
    open_price = _decimal(data, cursor)
    cursor += 24
    high_price = _decimal(data, cursor)
    cursor += 24
    low_price = _decimal(data, cursor)
    cursor += 24
    last_price = _decimal(data, cursor)
    cursor += 24
    close_price = _decimal(data, cursor)
    cursor += 24
    trade_volume = _quantity(data, cursor)
    cursor += 16
    turnover = _decimal(data, cursor)
    cursor += 24
    total_bid_quantity = _quantity(data, cursor)
    cursor += 16
    weighted_average_bid_price = _decimal(data, cursor)
    cursor += 24
    total_ask_quantity = _quantity(data, cursor)
    cursor += 16
    weighted_average_ask_price = _decimal(data, cursor)
    cursor += 24
    high_limit_price = _decimal(data, cursor)
    cursor += 24
    low_limit_price = _decimal(data, cursor)
    cursor += 24
    iopv = _decimal(data, cursor)
    cursor += 24
    open_interest = _quantity(data, cursor)
    cursor += 16
    (
        actual_bid_depth,
        actual_ask_depth,
        retained_bid_depth,
        retained_ask_depth,
    ) = struct.unpack_from("<IIII", data, cursor)
    cursor += 16
    if retained_bid_depth > 10 or retained_ask_depth > 10:
        raise WireFormatError("snapshot retained depth exceeds ten")
    bids = tuple(
        _book_level(data, cursor + index * 48) for index in range(10)
    )
    cursor += 10 * 48
    asks = tuple(
        _book_level(data, cursor + index * 48) for index in range(10)
    )
    cursor += 10 * 48
    bid_header_offset = cursor
    ask_header_offset = cursor + 16
    cursor += 32
    bid_quantities_offset = cursor
    ask_quantities_offset = cursor + 50 * 16
    cursor += 100 * 16
    if cursor != SNAPSHOT_BYTES:
        raise AssertionError("internal snapshot parser offset mismatch")
    return Snapshot(
        common=common,
        trade_count=trade_count,
        image_status=image_status,
        channel=channel,
        pre_close_price=pre_close_price,
        open_price=open_price,
        high_price=high_price,
        low_price=low_price,
        last_price=last_price,
        close_price=close_price,
        trade_volume=trade_volume,
        turnover=turnover,
        total_bid_quantity=total_bid_quantity,
        weighted_average_bid_price=weighted_average_bid_price,
        total_ask_quantity=total_ask_quantity,
        weighted_average_ask_price=weighted_average_ask_price,
        high_limit_price=high_limit_price,
        low_limit_price=low_limit_price,
        iopv=iopv,
        open_interest=open_interest,
        actual_bid_depth=actual_bid_depth,
        actual_ask_depth=actual_ask_depth,
        retained_bid_depth=retained_bid_depth,
        retained_ask_depth=retained_ask_depth,
        bids=bids,
        asks=asks,
        bid1_queue=_best_queue(
            data, bid_header_offset, bid_quantities_offset
        ),
        ask1_queue=_best_queue(
            data, ask_header_offset, ask_quantities_offset
        ),
    )


def parse_tick(data: bytes) -> Tick:
    _require_size(data, TICK_BYTES, "tick")
    common = _common(data, TICK_BYTES)
    if common.event_kind not in (
        MarketEventKind.SHANGHAI_TICK,
        MarketEventKind.SHENZHEN_ORDER,
        MarketEventKind.SHENZHEN_TRANSACTION,
    ):
        raise WireFormatError("tick payload contains a snapshot kind")
    if common.tick_stream_sequence == 0:
        raise WireFormatError("tick_stream_sequence must be nonzero")
    expected_market = (
        Market.SHANGHAI
        if common.event_kind is MarketEventKind.SHANGHAI_TICK
        else Market.SHENZHEN
    )
    if common.market is not expected_market:
        raise WireFormatError("tick event kind and market disagree")
    fields = _TICK_HEAD.unpack_from(data, 128)
    raw_type_length = fields[11]
    raw_tick_flag_length = fields[12]
    if raw_type_length > 32 or raw_tick_flag_length > 32:
        raise WireFormatError("tick raw byte length exceeds wire capacity")
    if fields[1] != 0 or fields[13] != 0:
        raise WireFormatError("tick reserved fields are nonzero")
    if any(data[272 + raw_type_length : 304]) or any(
        data[304 + raw_tick_flag_length : 336]
    ):
        raise WireFormatError("tick raw byte padding is nonzero")
    return Tick(
        common=common,
        validity_bitmap=fields[0],
        channel=fields[2],
        native_event_sequence=fields[3],
        source_raw_code_1=fields[4],
        source_raw_code_2=fields[5],
        action=_enum(TickAction, fields[6], "tick.action"),
        side=_enum(Side, fields[7], "tick.side"),
        order_type=_enum(OrderType, fields[8], "tick.order_type"),
        aggressor=_enum(Aggressor, fields[9], "tick.aggressor"),
        phase=_enum(TradingPhase, fields[10], "tick.phase"),
        price=_decimal(data, 168),
        quantity=_quantity(data, 192),
        trade_amount=_decimal(data, 208),
        matched_quantity=_quantity(data, 232),
        primary_order_id=struct.unpack_from("<q", data, 248)[0],
        buy_order_id=struct.unpack_from("<q", data, 256)[0],
        sell_order_id=struct.unpack_from("<q", data, 264)[0],
        raw_type=bytes(data[272 : 272 + raw_type_length]),
        raw_tick_flag=bytes(data[304 : 304 + raw_tick_flag_length]),
    )


def parse_kline(data: bytes) -> KLine:
    _require_size(data, KLINE_BYTES, "KLine")
    fields = _KLINE.unpack(data)
    if fields[0] == 0 or fields[2] == 0 or fields[3] == 0:
        raise WireFormatError(
            "available KLine requires generation, instrument and window"
        )
    if fields[27] != 1:
        raise WireFormatError("available KLine must have present=1")
    if fields[4] != 0 or any(data[187:192]):
        raise WireFormatError("KLine reserved fields are nonzero")
    return KLine(
        generation=fields[0],
        trade_date=fields[1],
        instrument_id=fields[2],
        window_id=fields[3],
        window_duration_ns=fields[5],
        window_start_ns_since_midnight=fields[6],
        window_end_ns_since_midnight=fields[7],
        window_start_unix_ns=fields[8],
        window_end_unix_ns=fields[9],
        open_price_p6=fields[10],
        high_price_p6=fields[11],
        low_price_p6=fields[12],
        close_price_p6=fields[13],
        volume_raw=fields[14],
        trade_count=fields[15],
        revision=fields[16],
        first_event_time_ns_since_midnight=fields[17],
        first_event_sequence=fields[18],
        first_source_sequence=fields[19],
        first_ingress_sequence=fields[20],
        last_event_time_ns_since_midnight=fields[21],
        last_event_sequence=fields[22],
        last_source_sequence=fields[23],
        last_ingress_sequence=fields[24],
        volume_scale=fields[25],
        quantity_unit=fields[26],
    )


def validate_snapshot_identity(
    snapshot: Snapshot, instrument_id: int
) -> None:
    if snapshot.common.instrument_id != instrument_id:
        raise WireFormatError("snapshot instrument_id does not match request")


def validate_tick_identity(tick: Tick, instrument_id: int) -> None:
    if tick.common.instrument_id != instrument_id:
        raise WireFormatError("tick instrument_id does not match request")


def validate_kline_identity(
    kline: KLine, instrument_id: int, window_id: int
) -> None:
    if (
        kline.instrument_id != instrument_id
        or kline.window_id != window_id
    ):
        raise WireFormatError("KLine identity does not match request")
