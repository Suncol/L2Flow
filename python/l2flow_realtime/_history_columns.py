"""Lazy, client-owned columns for immutable Wire V2 history pages.

The history protocols carry dense arrays of the existing V2 snapshot and tick
payloads.  This module extracts only a requested column and deliberately does
not construct the public latest-value dataclasses.  ``materialize_all()`` is
the explicit, separate path that forces every projected wire column.
"""

from __future__ import annotations

import struct
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from functools import lru_cache
from typing import Callable

from .models import WireFormatError
from .wire import SNAPSHOT_BYTES, TICK_BYTES


_WireBuffer = bytes | memoryview
_Extractor = Callable[[_WireBuffer, int], object]
_DECIMAL = struct.Struct("<qqBBB5x")
_QUANTITY = struct.Struct("<qBBB5x")
_TICK_HEAD = struct.Struct("<IIqqii8B")


@dataclass(frozen=True, slots=True)
class _ColumnSpec:
    extract: _Extractor
    scalar_format: str | None = None
    scalar_offset: int = 0


def _scalar(format_: str, offset: int) -> _ColumnSpec:
    unpacker = struct.Struct("<" + format_)
    return _ColumnSpec(
        lambda payload, base: unpacker.unpack_from(
            payload, base + offset
        )[0],
        scalar_format=format_,
        scalar_offset=offset,
    )


def _fixed_bytes(
    offset: int, length: int, *, length_offset: int | None = None
) -> _ColumnSpec:
    def extract(payload: bytes, base: int) -> bytes:
        actual = (
            length
            if length_offset is None
            else payload[base + length_offset]
        )
        if actual > length:
            raise WireFormatError(
                "wire byte-column length exceeds capacity"
            )
        return bytes(payload[base + offset : base + offset + actual])

    return _ColumnSpec(extract)


def _common_specs() -> dict[str, _ColumnSpec]:
    return {
        "record_schema_version": _scalar("I", 0),
        "record_bytes": _scalar("I", 4),
        "instrument_id": _scalar("I", 8),
        "ordinal": _scalar("I", 12),
        "source_sequence": _scalar("Q", 16),
        "ingress_sequence": _scalar("Q", 24),
        "tick_stream_sequence": _scalar("Q", 32),
        "vendor_sequence_id": _scalar("Q", 40),
        "event_time_unix_ns": _scalar("q", 48),
        "recv_realtime_ns": _scalar("q", 56),
        "recv_monotonic_ns": _scalar("q", 64),
        "exchange_time_ns_since_midnight": _scalar("Q", 72),
        "quality_flags": _scalar("Q", 80),
        "market_notices": _scalar("Q", 88),
        "source_stream_id": _scalar("I", 96),
        "trade_date": _scalar("I", 100),
        "vendor_local_time_raw": _scalar("I", 104),
        "source_slot": _scalar("B", 112),
        "event_kind": _scalar("B", 113),
        "market": _scalar("B", 114),
        "quantity_unit": _scalar("B", 115),
        "security_type": _scalar("B", 116),
        "asset_scope": _scalar("B", 117),
    }


def _add_decimal(
    specs: dict[str, _ColumnSpec], name: str, offset: int
) -> None:
    specs[f"{name}_raw"] = _scalar("q", offset)
    specs[f"{name}_p6"] = _scalar("q", offset + 8)
    specs[f"{name}_scale"] = _scalar("B", offset + 16)
    specs[f"{name}_valid"] = _scalar("B", offset + 17)
    specs[f"{name}_is_null"] = _scalar("B", offset + 18)


def _add_quantity(
    specs: dict[str, _ColumnSpec], name: str, offset: int
) -> None:
    specs[f"{name}_raw"] = _scalar("q", offset)
    specs[f"{name}_scale"] = _scalar("B", offset + 8)
    specs[f"{name}_valid"] = _scalar("B", offset + 9)
    specs[f"{name}_is_null"] = _scalar("B", offset + 10)


def _tick_specs() -> dict[str, _ColumnSpec]:
    specs = _common_specs()
    specs.update(
        {
            "validity_bitmap": _scalar("I", 128),
            "projection_flags": _scalar("I", 132),
            "channel": _scalar("q", 136),
            "native_event_sequence": _scalar("q", 144),
            "source_raw_code_1": _scalar("i", 152),
            "source_raw_code_2": _scalar("i", 156),
            "action": _scalar("B", 160),
            "side": _scalar("B", 161),
            "order_type": _scalar("B", 162),
            "aggressor": _scalar("B", 163),
            "phase": _scalar("B", 164),
            "raw_type": _fixed_bytes(
                272, 32, length_offset=165
            ),
            "raw_tick_flag": _fixed_bytes(
                304, 32, length_offset=166
            ),
            "primary_order_id": _scalar("q", 248),
            "buy_order_id": _scalar("q", 256),
            "sell_order_id": _scalar("q", 264),
        }
    )
    _add_decimal(specs, "price", 168)
    _add_quantity(specs, "quantity", 192)
    _add_decimal(specs, "trade_amount", 208)
    _add_quantity(specs, "matched_quantity", 232)
    return specs


def _snapshot_specs() -> dict[str, _ColumnSpec]:
    specs = _common_specs()
    specs.update(
        {
            "trade_count": _scalar("q", 128),
            "image_status": _scalar("i", 136),
            "channel": _scalar("I", 140),
            "actual_bid_depth": _scalar("I", 496),
            "actual_ask_depth": _scalar("I", 500),
            "retained_bid_depth": _scalar("I", 504),
            "retained_ask_depth": _scalar("I", 508),
            "bid1_queue_total_order_count": _scalar("I", 1472),
            "bid1_queue_actual_revealed_count": _scalar("I", 1476),
            "bid1_queue_retained_count": _scalar("I", 1480),
            "ask1_queue_total_order_count": _scalar("I", 1488),
            "ask1_queue_actual_revealed_count": _scalar("I", 1492),
            "ask1_queue_retained_count": _scalar("I", 1496),
        }
    )
    for name, offset in (
        ("pre_close_price", 144),
        ("open_price", 168),
        ("high_price", 192),
        ("low_price", 216),
        ("last_price", 240),
        ("close_price", 264),
        ("turnover", 304),
        ("weighted_average_bid_price", 344),
        ("weighted_average_ask_price", 384),
        ("high_limit_price", 408),
        ("low_limit_price", 432),
        ("iopv", 456),
    ):
        _add_decimal(specs, name, offset)
    for name, offset in (
        ("trade_volume", 288),
        ("total_bid_quantity", 328),
        ("total_ask_quantity", 368),
        ("open_interest", 480),
    ):
        _add_quantity(specs, name, offset)
    for side, region in (("bid", 512), ("ask", 992)):
        for index in range(10):
            level = region + index * 48
            suffix = index + 1
            _add_decimal(specs, f"{side}_price_{suffix}", level)
            _add_quantity(
                specs, f"{side}_quantity_{suffix}", level + 24
            )
            specs[f"{side}_order_count_{suffix}"] = _scalar(
                "I", level + 40
            )
            specs[f"{side}_order_count_valid_{suffix}"] = _scalar(
                "B", level + 44
            )
    for side, region in (("bid1_queue", 1504), ("ask1_queue", 2304)):
        for index in range(50):
            _add_quantity(
                specs,
                f"{side}_quantity_{index + 1}",
                region + index * 16,
            )
    return specs


TICK_COLUMN_SPECS = _tick_specs()
SNAPSHOT_COLUMN_SPECS = _snapshot_specs()


def _canonical_bool(value: int, field: str) -> None:
    if value not in (0, 1):
        raise WireFormatError(
            f"{field} is not a canonical wire boolean"
        )


def _validate_decimal(
    payloads: _WireBuffer, base: int, field: str
) -> None:
    _raw, _normalized, _scale, valid, is_null = (
        _DECIMAL.unpack_from(payloads, base)
    )
    _canonical_bool(valid, f"{field}.valid")
    _canonical_bool(is_null, f"{field}.is_null")
    if any(payloads[base + 19 : base + 24]):
        raise WireFormatError(f"{field} reserved bytes are nonzero")


def _validate_quantity(
    payloads: _WireBuffer, base: int, field: str
) -> None:
    _raw, _scale, valid, is_null = _QUANTITY.unpack_from(
        payloads, base
    )
    _canonical_bool(valid, f"{field}.valid")
    _canonical_bool(is_null, f"{field}.is_null")
    if any(payloads[base + 11 : base + 16]):
        raise WireFormatError(f"{field} reserved bytes are nonzero")


def _validate_common_projection(
    payloads: _WireBuffer,
    base: int,
    *,
    event_kind: int,
    trade_date: int,
) -> None:
    expected_market = 1 if event_kind in (1, 2) else 2
    if (
        struct.unpack_from("<I", payloads, base + 100)[0]
        != trade_date
        or struct.unpack_from("<I", payloads, base + 108)[0] != 0
        or payloads[base + 114] != expected_market
        or payloads[base + 115] > 5
        or payloads[base + 116] > 7
        or payloads[base + 117] > 2
        or any(payloads[base + 118 : base + 128])
    ):
        raise WireFormatError(
            "projected payload common metadata is noncanonical"
        )


def validate_snapshot_payload_canonical(
    payloads: _WireBuffer,
    base: int,
    *,
    event_kind: int,
    trade_date: int,
) -> None:
    """Eagerly validate every projected snapshot field, not just read columns."""

    if (
        base < 0
        or base + SNAPSHOT_BYTES > len(payloads)
        or event_kind not in (1, 3)
    ):
        raise WireFormatError("snapshot payload bounds/kind are invalid")
    _validate_common_projection(
        payloads,
        base,
        event_kind=event_kind,
        trade_date=trade_date,
    )
    for name, offset in (
        ("pre_close_price", 144),
        ("open_price", 168),
        ("high_price", 192),
        ("low_price", 216),
        ("last_price", 240),
        ("close_price", 264),
        ("turnover", 304),
        ("weighted_average_bid_price", 344),
        ("weighted_average_ask_price", 384),
        ("high_limit_price", 408),
        ("low_limit_price", 432),
        ("iopv", 456),
    ):
        _validate_decimal(payloads, base + offset, name)
    for name, offset in (
        ("trade_volume", 288),
        ("total_bid_quantity", 328),
        ("total_ask_quantity", 368),
        ("open_interest", 480),
    ):
        _validate_quantity(payloads, base + offset, name)

    (
        _actual_bid,
        _actual_ask,
        retained_bid,
        retained_ask,
    ) = struct.unpack_from("<IIII", payloads, base + 496)
    if retained_bid > 10 or retained_ask > 10:
        raise WireFormatError("snapshot retained depth exceeds ten")
    for side, region in (("bid", 512), ("ask", 992)):
        for index in range(10):
            level = base + region + index * 48
            _validate_decimal(
                payloads, level, f"{side}[{index}].price"
            )
            _validate_quantity(
                payloads,
                level + 24,
                f"{side}[{index}].quantity",
            )
            _canonical_bool(
                payloads[level + 44],
                f"{side}[{index}].order_count_valid",
            )
            if any(payloads[level + 45 : level + 48]):
                raise WireFormatError(
                    f"{side}[{index}] reserved bytes are nonzero"
                )

    for side, header, quantities in (
        ("bid1_queue", 1472, 1504),
        ("ask1_queue", 1488, 2304),
    ):
        _total, _actual, retained, reserved = struct.unpack_from(
            "<IIII", payloads, base + header
        )
        if reserved != 0 or retained > 50:
            raise WireFormatError(
                f"{side} header is noncanonical"
            )
        for index in range(50):
            _validate_quantity(
                payloads,
                base + quantities + index * 16,
                f"{side}[{index}]",
            )


def validate_tick_payload_canonical(
    payloads: _WireBuffer,
    base: int,
    *,
    event_kind: int,
    trade_date: int,
) -> int:
    """Eagerly validate every projected tick field and return its flags."""

    if (
        base < 0
        or base + TICK_BYTES > len(payloads)
        or event_kind not in (2, 4, 5)
    ):
        raise WireFormatError("tick payload bounds/kind are invalid")
    _validate_common_projection(
        payloads,
        base,
        event_kind=event_kind,
        trade_date=trade_date,
    )
    fields = _TICK_HEAD.unpack_from(payloads, base + 128)
    projection_flags = fields[1]
    raw_type_length = fields[11]
    raw_tick_flag_length = fields[12]
    if projection_flags & ~0x3:
        raise WireFormatError("tick has unknown projection flags")
    if (
        fields[6] > 4
        or fields[7] > 4
        or fields[8] > 3
        or fields[9] > 3
        or fields[10] > 7
        or fields[13] != 0
    ):
        raise WireFormatError("tick enum/reserved fields are invalid")
    if raw_type_length > 32 or raw_tick_flag_length > 32:
        raise WireFormatError("tick raw byte length exceeds capacity")
    raw_type = base + 272
    raw_tick_flag = base + 304
    if any(
        payloads[
            raw_type + raw_type_length : raw_type + 32
        ]
    ) or any(
        payloads[
            raw_tick_flag + raw_tick_flag_length :
            raw_tick_flag + 32
        ]
    ):
        raise WireFormatError("tick raw byte padding is nonzero")
    if projection_flags & 0x1 and (
        raw_type_length or any(payloads[raw_type : raw_type + 32])
    ):
        raise WireFormatError(
            "raw_type omission flag conflicts with payload"
        )
    if projection_flags & 0x2 and (
        raw_tick_flag_length
        or any(payloads[raw_tick_flag : raw_tick_flag + 32])
    ):
        raise WireFormatError(
            "raw_tick_flag omission flag conflicts with payload"
        )
    if event_kind != 2 and (
        projection_flags
        or raw_type_length
        or raw_tick_flag_length
        or any(payloads[raw_type : raw_tick_flag + 32])
    ):
        raise WireFormatError(
            "Shenzhen tick contains Shanghai-only raw projections"
        )
    _validate_decimal(payloads, base + 168, "price")
    _validate_quantity(payloads, base + 192, "quantity")
    _validate_decimal(payloads, base + 208, "trade_amount")
    _validate_quantity(
        payloads, base + 232, "matched_quantity"
    )
    return projection_flags


class LazyWireColumns(Mapping[str, tuple[object, ...]]):
    """A read-only mapping that builds and caches one requested column."""

    __slots__ = ("_payloads", "_record_bytes", "_specs", "_cache")

    def __init__(
        self,
        payloads: _WireBuffer,
        record_bytes: int,
        specs: Mapping[str, _ColumnSpec],
    ) -> None:
        if isinstance(payloads, memoryview):
            if (
                not payloads.readonly
                or not payloads.c_contiguous
                or payloads.itemsize != 1
            ):
                raise TypeError(
                    "wire payload view must be read-only contiguous bytes"
                )
            payloads = payloads.cast("B")
        elif not isinstance(payloads, bytes):
            raise TypeError(
                "wire payload block must be bytes or a read-only view"
            )
        if record_bytes <= 0 or len(payloads) % record_bytes:
            raise ValueError("wire payload block is not record-aligned")
        self._payloads = payloads
        self._record_bytes = record_bytes
        self._specs = specs
        self._cache: dict[str, tuple[object, ...]] = {}

    def __len__(self) -> int:
        return len(self._specs)

    @property
    def row_count(self) -> int:
        return len(self._payloads) // self._record_bytes

    @property
    def materialized_column_count(self) -> int:
        return len(self._cache)

    @property
    def wire_records(self) -> bytes:
        """Return the immutable dense wire rows owned by this page."""

        if isinstance(self._payloads, bytes):
            return self._payloads
        return self._payloads.tobytes()

    @property
    def wire_view(self) -> memoryview:
        """Return a read-only dense view without copying page-owned bytes."""

        return memoryview(self._payloads)

    def __iter__(self) -> Iterator[str]:
        return iter(self._specs)

    def __getitem__(self, name: str) -> tuple[object, ...]:
        return self.read_columns(name)[name]

    def read_columns(
        self, *names: str
    ) -> dict[str, tuple[object, ...]]:
        """Materialize selected columns, fusing fixed scalars into one pass."""

        if not names:
            raise ValueError("at least one column name is required")
        if any(not isinstance(name, str) for name in names):
            raise TypeError("column names must be strings")
        if len(set(names)) != len(names):
            raise ValueError("column names must be unique")

        missing: list[tuple[str, _ColumnSpec]] = []
        for name in names:
            if name in self._cache:
                continue
            try:
                missing.append((name, self._specs[name]))
            except KeyError:
                raise KeyError(name) from None

        scalar = [
            item
            for item in missing
            if item[1].scalar_format is not None
        ]
        if scalar:
            ordered = sorted(
                scalar, key=lambda item: item[1].scalar_offset
            )
            layout = tuple(
                (
                    spec.scalar_format,
                    spec.scalar_offset,
                )
                for _name, spec in ordered
            )
            unpacker = _scalar_row_unpacker(
                self._record_bytes, layout
            )
            rows = unpacker.iter_unpack(self._payloads)
            if len(ordered) == 1:
                materialized = (tuple(row[0] for row in rows),)
            else:
                materialized = tuple(zip(*rows))
            if not materialized:
                materialized = tuple(() for _item in ordered)
            for (name, _spec), values in zip(
                ordered, materialized
            ):
                self._cache[name] = values

        for name, spec in missing:
            if name in self._cache:
                continue
            self._cache[name] = tuple(
                spec.extract(self._payloads, base)
                for base in range(
                    0, len(self._payloads), self._record_bytes
                )
            )

        return {name: self._cache[name] for name in names}

    def materialize_all(self) -> dict[str, tuple[object, ...]]:
        """Force every V2 projected column and return a client-owned mapping."""

        return self.read_columns(*self._specs)


@lru_cache(maxsize=256)
def _scalar_row_unpacker(
    record_bytes: int,
    layout: tuple[tuple[str | None, int], ...],
) -> struct.Struct:
    format_parts = ["<"]
    cursor = 0
    for format_, offset in layout:
        assert format_ is not None
        scalar = struct.Struct("<" + format_)
        if offset < cursor:
            raise ValueError("scalar columns overlap or are unordered")
        if offset > cursor:
            format_parts.append(f"{offset - cursor}x")
        format_parts.append(format_)
        cursor = offset + scalar.size
    if cursor > record_bytes:
        raise ValueError("scalar column exceeds its wire record")
    if cursor < record_bytes:
        format_parts.append(f"{record_bytes - cursor}x")
    unpacker = struct.Struct("".join(format_parts))
    if unpacker.size != record_bytes:
        raise AssertionError("scalar row unpacker has the wrong stride")
    return unpacker


def tick_columns(payloads: _WireBuffer) -> LazyWireColumns:
    return LazyWireColumns(payloads, TICK_BYTES, TICK_COLUMN_SPECS)


def snapshot_columns(payloads: _WireBuffer) -> LazyWireColumns:
    return LazyWireColumns(
        payloads, SNAPSHOT_BYTES, SNAPSHOT_COLUMN_SPECS
    )


__all__ = [
    "LazyWireColumns",
    "SNAPSHOT_COLUMN_SPECS",
    "TICK_COLUMN_SPECS",
    "snapshot_columns",
    "tick_columns",
    "validate_snapshot_payload_canonical",
    "validate_tick_payload_canonical",
]
