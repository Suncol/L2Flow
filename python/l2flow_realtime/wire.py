"""Parsers for client-owned copies of the fixed little-endian Wire V2 ABI."""

from __future__ import annotations

import struct

from .models import (
    Aggressor,
    AvailabilityFlag,
    BindingState,
    CommonRecord,
    DecimalValue,
    Instrument,
    InstrumentStatus,
    KLineCoverageFlag,
    Market,
    MarketEventKind,
    OrderType,
    QuantityValue,
    Side,
    TickAction,
    TickProjectionFlag,
    TradingPhase,
    WireFormatError,
)


WIRE_MAJOR = 2
WIRE_MINOR = 4
CONTROL_MAGIC = b"L2FCTL2\x00"
INSTRUMENT_BYTES = 128
SNAPSHOT_BYTES = 3104
TICK_BYTES = 336
KLINE_BYTES = 192

_INSTRUMENT = struct.Struct("<QIIII4BIQQIIQQ7Q")
_DECIMAL = struct.Struct("<qqBBB5x")
_QUANTITY = struct.Struct("<qBBB5x")
_COMMON = struct.Struct("<IIIIQQQQqqqQQQIIII6B10x")
_TICK_HEAD = struct.Struct("<IIqqii8B")
_KLINE = struct.Struct(
    "<QIIIIQQQ" + "q" * 6 + "Q" * 11 + "BBB5x"
)

assert _INSTRUMENT.size == INSTRUMENT_BYTES
assert _DECIMAL.size == 24
assert _QUANTITY.size == 16
assert _COMMON.size == 128
assert _TICK_HEAD.size == 40
assert _KLINE.size == KLINE_BYTES


def _require_size(data: bytes, expected: int, name: str) -> None:
    if not isinstance(data, bytes) or len(data) != expected:
        length = len(data) if isinstance(data, bytes) else "non-bytes"
        raise WireFormatError(
            f"{name} payload has {length} bytes; expected {expected}"
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


def parse_common(data: bytes, expected_bytes: int) -> CommonRecord:
    fields = _COMMON.unpack_from(data, 0)
    if fields[0] != 2:
        raise WireFormatError(
            f"unsupported record schema version {fields[0]}"
        )
    if fields[1] != expected_bytes:
        raise WireFormatError(
            f"record_bytes is {fields[1]}; expected {expected_bytes}"
        )
    if fields[2] == 0 or fields[5] == 0:
        raise WireFormatError(
            "available records require instrument and ingress IDs"
        )
    if fields[3] != fields[2] - 1:
        raise WireFormatError("record instrument_id and ordinal disagree")
    if fields[17] != 0 or any(data[118:128]):
        raise WireFormatError("common record reserved fields are nonzero")
    result = CommonRecord(
        record_schema_version=fields[0],
        record_bytes=fields[1],
        instrument_id=fields[2],
        ordinal=fields[3],
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
    expected_source_slot = {
        MarketEventKind.SHANGHAI_SNAPSHOT: 0,
        MarketEventKind.SHANGHAI_TICK: 1,
        MarketEventKind.SHENZHEN_SNAPSHOT: 2,
        MarketEventKind.SHENZHEN_ORDER: 3,
        MarketEventKind.SHENZHEN_TRANSACTION: 3,
    }[result.event_kind]
    if result.source_slot != expected_source_slot:
        raise WireFormatError("event_kind and source_slot disagree")
    expected_market = (
        Market.SHANGHAI
        if result.event_kind
        in (
            MarketEventKind.SHANGHAI_SNAPSHOT,
            MarketEventKind.SHANGHAI_TICK,
        )
        else Market.SHENZHEN
    )
    if result.market is not expected_market:
        raise WireFormatError("event_kind and market disagree")
    return result


def parse_instrument(
    row: bytes,
    security_id_source: bytes,
    security_id: bytes,
    *,
    session_epoch: int,
    requested_instrument_id: int,
    status: InstrumentStatus,
) -> Instrument:
    _require_size(row, INSTRUMENT_BYTES, "instrument")
    fields = _INSTRUMENT.unpack(row)
    (
        publish_tag,
        instrument_id,
        ordinal,
        binding_state_value,
        availability_value,
        market,
        quantity_unit,
        security_type,
        asset_scope,
        reserved0,
        _source_offset,
        _id_offset,
        source_length,
        id_length,
        first_ingress_sequence,
        last_ingress_sequence,
        *reserved,
    ) = fields
    if publish_tag & 1:
        raise WireFormatError("instrument publish tag is unstable")
    if instrument_id != requested_instrument_id:
        raise WireFormatError("instrument row does not match request")
    if instrument_id == 0 or ordinal != instrument_id - 1:
        raise WireFormatError("instrument identity is invalid")
    binding_state = _enum(
        BindingState, binding_state_value, "instrument.binding_state"
    )
    availability = AvailabilityFlag(availability_value)
    if availability_value & ~0xF:
        raise WireFormatError("instrument has unknown availability flags")
    if (
        availability & AvailabilityFlag.FACTOR_ELIGIBLE
        and not availability & AvailabilityFlag.HAS_SNAPSHOT
    ):
        raise WireFormatError(
            "instrument factor eligibility lacks snapshot availability"
        )
    if market == 0:
        raise WireFormatError("bound instrument market is zero")
    if source_length != len(security_id_source):
        raise WireFormatError("security_id_source length does not match row")
    if id_length != len(security_id) or id_length == 0:
        raise WireFormatError("security_id length does not match row")
    if reserved0 != 0 or any(reserved):
        raise WireFormatError("instrument reserved fields are nonzero")
    expected_binding = (
        BindingState.AVAILABLE
        if status is InstrumentStatus.AVAILABLE
        else BindingState.BOUND_NO_DATA
    )
    if binding_state is not expected_binding:
        raise WireFormatError("instrument C status and row state disagree")
    return Instrument(
        session_epoch=session_epoch,
        instrument_id=instrument_id,
        status=status,
        ordinal=ordinal,
        binding_state=binding_state,
        availability_flags=availability,
        market=market,
        quantity_unit=quantity_unit,
        security_type=security_type,
        asset_scope=asset_scope,
        security_id_source=bytes(security_id_source),
        security_id=bytes(security_id),
        first_ingress_sequence=first_ingress_sequence,
        last_ingress_sequence=last_ingress_sequence,
    )


def parse_snapshot_payload(
    data: bytes, expected_instrument_id: int
) -> tuple[CommonRecord, DecimalValue]:
    _require_size(data, SNAPSHOT_BYTES, "snapshot")
    common = parse_common(data, SNAPSHOT_BYTES)
    if common.instrument_id != expected_instrument_id:
        raise WireFormatError("snapshot instrument_id does not match request")
    if common.event_kind not in (
        MarketEventKind.SHANGHAI_SNAPSHOT,
        MarketEventKind.SHENZHEN_SNAPSHOT,
    ):
        raise WireFormatError("snapshot payload contains a non-snapshot kind")
    if common.tick_stream_sequence != 0:
        raise WireFormatError("snapshot tick_stream_sequence is nonzero")
    return common, _decimal(data, 240)


def parse_tick_payload(
    data: bytes, expected_instrument_id: int
) -> tuple[
    CommonRecord,
    DecimalValue,
    QuantityValue,
    TickAction,
    Side,
    TickProjectionFlag,
]:
    _require_size(data, TICK_BYTES, "tick")
    common = parse_common(data, TICK_BYTES)
    if common.instrument_id != expected_instrument_id:
        raise WireFormatError("tick instrument_id does not match request")
    if common.event_kind not in (
        MarketEventKind.SHANGHAI_TICK,
        MarketEventKind.SHENZHEN_ORDER,
        MarketEventKind.SHENZHEN_TRANSACTION,
    ):
        raise WireFormatError("tick payload contains a snapshot kind")
    if common.tick_stream_sequence == 0:
        raise WireFormatError("tick_stream_sequence is zero")
    fields = _TICK_HEAD.unpack_from(data, 128)
    projection_flags = fields[1]
    raw_type_length = fields[11]
    raw_tick_flag_length = fields[12]
    if projection_flags & ~0x3:
        raise WireFormatError("tick has unknown projection flags")
    if fields[13] != 0:
        raise WireFormatError("tick reserved byte is nonzero")
    if raw_type_length > 32 or raw_tick_flag_length > 32:
        raise WireFormatError("tick raw byte length exceeds capacity")
    if any(data[272 + raw_type_length : 304]) or any(
        data[304 + raw_tick_flag_length : 336]
    ):
        raise WireFormatError("tick raw byte padding is nonzero")
    if (
        projection_flags
        & int(TickProjectionFlag.RAW_TYPE_OMITTED)
        and (raw_type_length or any(data[272:304]))
    ):
        raise WireFormatError("raw_type omission flag conflicts with payload")
    if (
        projection_flags
        & int(TickProjectionFlag.RAW_TICK_FLAG_OMITTED)
        and (raw_tick_flag_length or any(data[304:336]))
    ):
        raise WireFormatError(
            "raw_tick_flag omission flag conflicts with payload"
        )
    if common.event_kind is not MarketEventKind.SHANGHAI_TICK and (
        projection_flags
        or raw_type_length
        or raw_tick_flag_length
        or any(data[272:336])
    ):
        raise WireFormatError(
            "Shenzhen tick contains Shanghai-only raw projections"
        )
    return (
        common,
        _decimal(data, 168),
        _quantity(data, 192),
        _enum(TickAction, fields[6], "tick.action"),
        _enum(Side, fields[7], "tick.side"),
        TickProjectionFlag(projection_flags),
    )


def parse_kline_payload(
    data: bytes, expected_instrument_id: int, expected_window_id: int
) -> dict[str, int]:
    _require_size(data, KLINE_BYTES, "KLine")
    fields = _KLINE.unpack(data)
    if fields[0] == 0 or fields[2] == 0 or fields[3] == 0:
        raise WireFormatError(
            "available KLine requires generation, instrument and window"
        )
    if (
        fields[2] != expected_instrument_id
        or fields[3] != expected_window_id
    ):
        raise WireFormatError("KLine identity does not match request")
    if fields[27] != 1:
        raise WireFormatError("available KLine must have present=1")
    known_coverage_flags = int(
        KLineCoverageFlag.PROCESS_START_PARTIAL
        | KLineCoverageFlag.NATURAL_WINDOW_LEFT_TRUNCATED
    )
    if fields[4] & ~known_coverage_flags:
        raise WireFormatError("KLine coverage_flags contain unknown bits")
    if (
        fields[4]
        & int(KLineCoverageFlag.NATURAL_WINDOW_LEFT_TRUNCATED)
        and not fields[4]
        & int(KLineCoverageFlag.PROCESS_START_PARTIAL)
    ):
        raise WireFormatError(
            "left-truncated KLine requires process-start coverage"
        )
    if any(data[187:192]):
        raise WireFormatError("KLine reserved fields are nonzero")
    names = (
        "generation",
        "trade_date",
        "instrument_id",
        "window_id",
        "coverage_flags",
        "window_duration_ns",
        "window_start_ns_since_midnight",
        "window_end_ns_since_midnight",
        "window_start_unix_ns",
        "window_end_unix_ns",
        "open_price_p6",
        "high_price_p6",
        "low_price_p6",
        "close_price_p6",
        "volume_raw",
        "trade_count",
        "revision",
        "first_event_time_ns_since_midnight",
        "first_event_sequence",
        "first_source_sequence",
        "first_ingress_sequence",
        "last_event_time_ns_since_midnight",
        "last_event_sequence",
        "last_source_sequence",
        "last_ingress_sequence",
        "volume_scale",
        "quantity_unit",
        "_present",
    )
    parsed = dict(zip(names, fields))
    del parsed["instrument_id"]
    del parsed["window_id"]
    del parsed["_present"]
    parsed["coverage_flags"] = KLineCoverageFlag(
        parsed["coverage_flags"]
    )
    return parsed
