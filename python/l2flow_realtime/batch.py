"""Column-oriented batch views and optional Arrow/Polars adapters."""

from __future__ import annotations

import importlib
from dataclasses import dataclass
from typing import Dict, Generic, Iterator, Mapping, Sequence, Tuple, TypeVar

from .models import (
    KLine,
    LatestResult,
    LatestStatus,
    OptionalDependencyError,
    SessionIdentity,
    Snapshot,
    Tick,
)
from .wire import TICK_BYTES


RecordT = TypeVar("RecordT")
ColumnDict = Dict[str, list]
_TICK_COLUMN_NUMPY_DTYPE = None

_COMMON_TYPES = {
    "run_id": "binary",
    "session_epoch": "u64",
    "status": "u8",
    "instrument_id": "u32",
    "event_kind": "u8",
    "market": "u8",
    "source_slot": "u8",
    "source_stream_id": "u32",
    "source_sequence": "u64",
    "ingress_sequence": "u64",
    "tick_stream_sequence": "u64",
    "vendor_sequence_id": "u64",
    "event_time_unix_ns": "i64",
    "recv_realtime_ns": "i64",
    "recv_monotonic_ns": "i64",
    "exchange_time_ns_since_midnight": "u64",
    "quality_flags": "u64",
    "market_notices": "u64",
    "trade_date": "u32",
    "quantity_unit": "u8",
    "security_type": "u8",
    "asset_scope": "u8",
}

_SNAPSHOT_TYPES = {
    **_COMMON_TYPES,
    "trade_count": "i64",
    "image_status": "i32",
    "channel": "u32",
    "pre_close_price_p6": "i64",
    "open_price_p6": "i64",
    "high_price_p6": "i64",
    "low_price_p6": "i64",
    "last_price_p6": "i64",
    "close_price_p6": "i64",
    "trade_volume_raw": "i64",
    "trade_volume_scale": "u8",
    "turnover_p6": "i64",
    "total_bid_quantity_raw": "i64",
    "total_bid_quantity_scale": "u8",
    "total_ask_quantity_raw": "i64",
    "total_ask_quantity_scale": "u8",
    "weighted_average_bid_price_p6": "i64",
    "weighted_average_ask_price_p6": "i64",
    "high_limit_price_p6": "i64",
    "low_limit_price_p6": "i64",
    "iopv_p6": "i64",
    "open_interest_raw": "i64",
    "open_interest_scale": "u8",
    "actual_bid_depth": "u32",
    "actual_ask_depth": "u32",
    "retained_bid_depth": "u32",
    "retained_ask_depth": "u32",
}
for _side in ("bid", "ask"):
    for _level in range(1, 11):
        _SNAPSHOT_TYPES[f"{_side}_price_p6_{_level}"] = "i64"
        _SNAPSHOT_TYPES[f"{_side}_quantity_raw_{_level}"] = "i64"
        _SNAPSHOT_TYPES[f"{_side}_quantity_scale_{_level}"] = "u8"
        _SNAPSHOT_TYPES[f"{_side}_order_count_{_level}"] = "u32"

_TICK_TYPES = {
    **_COMMON_TYPES,
    "validity_bitmap": "u32",
    "projection_flags": "u32",
    "channel": "i64",
    "native_event_sequence": "i64",
    "source_raw_code_1": "i32",
    "source_raw_code_2": "i32",
    "action": "u8",
    "side": "u8",
    "order_type": "u8",
    "aggressor": "u8",
    "phase": "u8",
    "price_p6": "i64",
    "quantity_raw": "i64",
    "quantity_scale": "u8",
    "trade_amount_p6": "i64",
    "matched_quantity_raw": "i64",
    "matched_quantity_scale": "u8",
    "primary_order_id": "i64",
    "buy_order_id": "i64",
    "sell_order_id": "i64",
    "raw_type": "binary",
    "raw_tick_flag": "binary",
}

_KLINE_TYPES = {
    "run_id": "binary",
    "session_epoch": "u64",
    "status": "u8",
    "instrument_id": "u32",
    "window_id": "u32",
    "generation": "u64",
    "trade_date": "u32",
    "window_duration_ns": "u64",
    "window_start_ns_since_midnight": "u64",
    "window_end_ns_since_midnight": "u64",
    "window_start_unix_ns": "i64",
    "window_end_unix_ns": "i64",
    "open_price_p6": "i64",
    "high_price_p6": "i64",
    "low_price_p6": "i64",
    "close_price_p6": "i64",
    "volume_raw": "u64",
    "volume_scale": "u8",
    "quantity_unit": "u8",
    "trade_count": "u64",
    "revision": "u64",
    "first_event_time_ns_since_midnight": "u64",
    "first_event_sequence": "u64",
    "first_source_sequence": "u64",
    "first_ingress_sequence": "u64",
    "last_event_time_ns_since_midnight": "u64",
    "last_event_sequence": "u64",
    "last_source_sequence": "u64",
    "last_ingress_sequence": "u64",
}

_HISTORY_TYPES = {
    "store_generation": "u64",
    "registry_version": "u64",
    "registry_sha256": "binary",
    "input_identity_sha256": "binary",
    "payload_projection": "u32",
    "history_page_index": "u64",
    "projection_flags": "u32",
    "record_coverage_complete": "u8",
    "field_complete": "u8",
    "coverage_from_open": "u8",
}


def _new_columns(types: Mapping[str, str]) -> ColumnDict:
    return {name: [] for name in types}


def _append_common(
    columns: ColumnDict,
    identity: SessionIdentity,
    status: int,
    requested_instrument_id: int,
    record,
) -> None:
    columns["run_id"].append(identity.run_id)
    columns["session_epoch"].append(identity.session_epoch)
    columns["status"].append(status)
    columns["instrument_id"].append(requested_instrument_id)
    common = record.common if record is not None else None
    values = {
        "event_kind": int(common.event_kind) if common else None,
        "market": int(common.market) if common else None,
        "source_slot": common.source_slot if common else None,
        "source_stream_id": common.source_stream_id if common else None,
        "source_sequence": common.source_sequence if common else None,
        "ingress_sequence": common.ingress_sequence if common else None,
        "tick_stream_sequence": (
            common.tick_stream_sequence if common else None
        ),
        "vendor_sequence_id": common.vendor_sequence_id if common else None,
        "event_time_unix_ns": common.event_time_unix_ns if common else None,
        "recv_realtime_ns": common.recv_realtime_ns if common else None,
        "recv_monotonic_ns": common.recv_monotonic_ns if common else None,
        "exchange_time_ns_since_midnight": (
            common.exchange_time_ns_since_midnight if common else None
        ),
        "quality_flags": common.quality_flags if common else None,
        "market_notices": common.market_notices if common else None,
        "trade_date": common.trade_date if common else None,
        "quantity_unit": common.quantity_unit if common else None,
        "security_type": common.security_type if common else None,
        "asset_scope": common.asset_scope if common else None,
    }
    for name, value in values.items():
        columns[name].append(value)


def _snapshot_columns(
    results: Sequence[LatestResult], identity: SessionIdentity
) -> ColumnDict:
    columns = _new_columns(_SNAPSHOT_TYPES)
    scalar_fields = (
        "trade_count",
        "image_status",
        "channel",
        "actual_bid_depth",
        "actual_ask_depth",
        "retained_bid_depth",
        "retained_ask_depth",
    )
    for result in results:
        value = result.value
        _append_common(
            columns,
            identity,
            int(result.status),
            result.requested_instrument_id,
            value,
        )
        for name in scalar_fields:
            columns[name].append(
                getattr(value, name) if value is not None else None
            )
        decimals = {
            "pre_close_price_p6": "pre_close_price",
            "open_price_p6": "open_price",
            "high_price_p6": "high_price",
            "low_price_p6": "low_price",
            "last_price_p6": "last_price",
            "close_price_p6": "close_price",
            "turnover_p6": "turnover",
            "weighted_average_bid_price_p6": (
                "weighted_average_bid_price"
            ),
            "weighted_average_ask_price_p6": (
                "weighted_average_ask_price"
            ),
            "high_limit_price_p6": "high_limit_price",
            "low_limit_price_p6": "low_limit_price",
            "iopv_p6": "iopv",
        }
        for column, attribute in decimals.items():
            columns[column].append(
                getattr(value, attribute).p6 if value is not None else None
            )
        quantities = {
            "trade_volume_raw": "trade_volume",
            "total_bid_quantity_raw": "total_bid_quantity",
            "total_ask_quantity_raw": "total_ask_quantity",
            "open_interest_raw": "open_interest",
        }
        for column, attribute in quantities.items():
            columns[column].append(
                getattr(value, attribute).value
                if value is not None
                else None
            )
        columns["trade_volume_scale"].append(
            value.trade_volume.scale
            if value is not None and value.trade_volume.value is not None
            else None
        )
        for column, attribute in (
            ("total_bid_quantity_scale", "total_bid_quantity"),
            ("total_ask_quantity_scale", "total_ask_quantity"),
            ("open_interest_scale", "open_interest"),
        ):
            quantity = getattr(value, attribute) if value is not None else None
            columns[column].append(
                quantity.scale
                if quantity is not None and quantity.value is not None
                else None
            )
        for side_name in ("bid", "ask"):
            levels = (
                value.bids
                if value is not None and side_name == "bid"
                else value.asks if value is not None else ()
            )
            for index in range(10):
                level = levels[index] if value is not None else None
                number = index + 1
                columns[f"{side_name}_price_p6_{number}"].append(
                    level.price.p6 if level is not None else None
                )
                columns[f"{side_name}_quantity_raw_{number}"].append(
                    level.quantity.value if level is not None else None
                )
                columns[f"{side_name}_quantity_scale_{number}"].append(
                    level.quantity.scale
                    if level is not None
                    and level.quantity.value is not None
                    else None
                )
                columns[f"{side_name}_order_count_{number}"].append(
                    level.order_count
                    if level is not None and level.order_count_valid
                    else None
                )
    return columns


def _tick_columns_from_values(
    values: Sequence[Tick],
    identity: SessionIdentity,
    statuses: Sequence[int],
    requested_ids: Sequence[int],
) -> ColumnDict:
    columns = _new_columns(_TICK_TYPES)
    for value, status, requested_id in zip(
        values, statuses, requested_ids
    ):
        _append_common(columns, identity, status, requested_id, value)
        attributes = {
            "validity_bitmap": "validity_bitmap",
            "projection_flags": "projection_flags",
            "channel": "channel",
            "native_event_sequence": "native_event_sequence",
            "source_raw_code_1": "source_raw_code_1",
            "source_raw_code_2": "source_raw_code_2",
            "primary_order_id": "primary_order_id",
            "buy_order_id": "buy_order_id",
            "sell_order_id": "sell_order_id",
            "raw_type": "raw_type",
            "raw_tick_flag": "raw_tick_flag",
        }
        for column, attribute in attributes.items():
            columns[column].append(
                getattr(value, attribute) if value is not None else None
            )
        for name in ("action", "side", "order_type", "aggressor", "phase"):
            columns[name].append(
                int(getattr(value, name)) if value is not None else None
            )
        columns["price_p6"].append(value.price.p6 if value else None)
        columns["quantity_raw"].append(
            value.quantity.value if value else None
        )
        columns["quantity_scale"].append(
            value.quantity.scale
            if value is not None and value.quantity.value is not None
            else None
        )
        columns["trade_amount_p6"].append(
            value.trade_amount.p6 if value else None
        )
        columns["matched_quantity_raw"].append(
            value.matched_quantity.value if value else None
        )
        columns["matched_quantity_scale"].append(
            value.matched_quantity.scale
            if value is not None
            and value.matched_quantity.value is not None
            else None
        )
    return columns


def _tick_columns(
    results: Sequence[LatestResult], identity: SessionIdentity
) -> ColumnDict:
    return _tick_columns_from_values(
        [result.value for result in results],
        identity,
        [int(result.status) for result in results],
        [result.requested_instrument_id for result in results],
    )


def _kline_columns(
    results: Sequence[LatestResult], identity: SessionIdentity
) -> ColumnDict:
    columns = _new_columns(_KLINE_TYPES)
    fields = tuple(
        name
        for name in _KLINE_TYPES
        if name
        not in {
            "run_id",
            "session_epoch",
            "status",
            "instrument_id",
            "window_id",
        }
    )
    for result in results:
        value = result.value
        columns["run_id"].append(identity.run_id)
        columns["session_epoch"].append(identity.session_epoch)
        columns["status"].append(int(result.status))
        columns["instrument_id"].append(result.requested_instrument_id)
        columns["window_id"].append(result.requested_window_id)
        for name in fields:
            columns[name].append(
                getattr(value, name) if value is not None else None
            )
    return columns


def _history_columns_by_kind(records, generation, page_index: int):
    """Build two homogeneous tables while preserving ingress for re-merge."""

    identity = SessionIdentity(
        generation.run_id, generation.session_epoch
    )
    snapshots = tuple(
        record
        for record in records
        if isinstance(record.value, Snapshot)
    )
    ticks = tuple(
        record for record in records if isinstance(record.value, Tick)
    )
    snapshot_results = tuple(
        LatestResult(
            generation.instrument_id,
            LatestStatus.AVAILABLE,
            record.value,
        )
        for record in snapshots
    )
    columns_by_kind = {
        "snapshots": (
            _snapshot_columns(snapshot_results, identity),
            {**_SNAPSHOT_TYPES, **_HISTORY_TYPES},
            snapshots,
        ),
        "ticks": (
            _tick_columns_from_values(
                [record.value for record in ticks],
                identity,
                [int(LatestStatus.AVAILABLE)] * len(ticks),
                [generation.instrument_id] * len(ticks),
            ),
            {**_TICK_TYPES, **_HISTORY_TYPES},
            ticks,
        ),
    }
    for columns, _types, selected in columns_by_kind.values():
        count = len(selected)
        columns["store_generation"] = [generation.generation] * count
        columns["registry_version"] = [generation.registry_version] * count
        columns["registry_sha256"] = [generation.registry_sha256] * count
        columns["input_identity_sha256"] = [
            generation.input_identity_sha256
        ] * count
        columns["payload_projection"] = [
            generation.payload_projection
        ] * count
        columns["history_page_index"] = [page_index] * count
        columns["projection_flags"] = [
            record.projection_flags for record in selected
        ]
        columns["record_coverage_complete"] = [
            int(generation.record_coverage_complete)
        ] * count
        columns["field_complete"] = [
            int(generation.field_complete)
        ] * count
        columns["coverage_from_open"] = [
            int(generation.coverage_from_open)
        ] * count
    return columns_by_kind


def _arrow_type(module, tag: str):
    return {
        "u8": module.uint8,
        "u32": module.uint32,
        "u64": module.uint64,
        "i32": module.int32,
        "i64": module.int64,
        "binary": module.binary,
    }[tag]()


def _to_arrow(columns: ColumnDict, types: Mapping[str, str]):
    try:
        arrow = importlib.import_module("pyarrow")
    except ImportError as error:
        raise OptionalDependencyError(
            "to_arrow() requires the optional 'pyarrow' package"
        ) from error
    arrays = [
        arrow.array(columns[name], type=_arrow_type(arrow, tag))
        for name, tag in types.items()
    ]
    return arrow.RecordBatch.from_arrays(arrays, list(types))


def _to_polars(columns: ColumnDict, types: Mapping[str, str]):
    try:
        polars = importlib.import_module("polars")
    except ImportError as error:
        raise OptionalDependencyError(
            "to_polars() requires the optional 'polars' package"
        ) from error
    dtype = {
        "u8": polars.UInt8,
        "u32": polars.UInt32,
        "u64": polars.UInt64,
        "i32": polars.Int32,
        "i64": polars.Int64,
        "binary": polars.Binary,
    }
    series = [
        polars.Series(name, columns[name], dtype=dtype[tag])
        for name, tag in types.items()
    ]
    return polars.DataFrame(series)


@dataclass(frozen=True, slots=True)
class LatestBatch(Generic[RecordT]):
    kind: str
    session_identity: SessionIdentity
    results: Tuple[LatestResult[RecordT], ...]

    def __len__(self) -> int:
        return len(self.results)

    def __iter__(self) -> Iterator[LatestResult[RecordT]]:
        return iter(self.results)

    def __getitem__(self, index):
        return self.results[index]

    def _columns_and_types(self):
        if self.kind == "snapshot":
            return (
                _snapshot_columns(self.results, self.session_identity),
                _SNAPSHOT_TYPES,
            )
        if self.kind == "tick":
            return (
                _tick_columns(self.results, self.session_identity),
                _TICK_TYPES,
            )
        if self.kind == "kline":
            return (
                _kline_columns(self.results, self.session_identity),
                _KLINE_TYPES,
            )
        raise ValueError(f"unsupported batch kind {self.kind!r}")

    def to_columns(self) -> ColumnDict:
        return self._columns_and_types()[0]

    def to_dict(self) -> ColumnDict:
        return self.to_columns()

    def to_arrow(self):
        columns, types = self._columns_and_types()
        return _to_arrow(columns, types)

    def to_polars(self):
        columns, types = self._columns_and_types()
        return _to_polars(columns, types)


@dataclass(frozen=True, slots=True)
class TickBatch:
    session_identity: SessionIdentity
    first_sequence: int
    next_sequence: int
    ticks: Tuple[Tick, ...]

    def __post_init__(self) -> None:
        if self.first_sequence <= 0:
            raise ValueError("first_sequence must be positive")
        if self.next_sequence != self.first_sequence + len(self.ticks):
            raise ValueError("tick batch cursor is not contiguous")
        for index, tick in enumerate(self.ticks):
            expected = self.first_sequence + index
            if tick.common.tick_stream_sequence != expected:
                raise ValueError("tick batch payload sequence is not contiguous")

    def __len__(self) -> int:
        return len(self.ticks)

    def __iter__(self) -> Iterator[Tick]:
        return iter(self.ticks)

    def __getitem__(self, index):
        return self.ticks[index]

    def to_columns(self) -> ColumnDict:
        return _tick_columns_from_values(
            self.ticks,
            self.session_identity,
            [0] * len(self.ticks),
            [tick.common.instrument_id for tick in self.ticks],
        )

    def to_dict(self) -> ColumnDict:
        return self.to_columns()

    def to_arrow(self):
        return _to_arrow(self.to_columns(), _TICK_TYPES)

    def to_polars(self):
        return _to_polars(self.to_columns(), _TICK_TYPES)


@dataclass(frozen=True, slots=True)
class TickColumnBatch:
    """Client-owned contiguous tick block for high-rate columnar analysis.

    ``numpy_records()`` returns a read-only structured view over the copied
    V1 wire block. It avoids constructing one Python dataclass graph per tick;
    the ordinary :class:`TickBatch` remains the fully decoded object API.
    """

    session_identity: SessionIdentity
    first_sequence: int
    next_sequence: int
    wire_records: bytes

    def __post_init__(self) -> None:
        if self.first_sequence <= 0:
            raise ValueError("first_sequence must be positive")
        if len(self.wire_records) % TICK_BYTES != 0:
            raise ValueError("tick column block is not record-aligned")
        if self.next_sequence != self.first_sequence + len(self):
            raise ValueError("tick column batch cursor is not contiguous")

    def __len__(self) -> int:
        return len(self.wire_records) // TICK_BYTES

    def numpy_records(self):
        """Return a zero-copy, read-only NumPy structured wire view."""

        global _TICK_COLUMN_NUMPY_DTYPE
        numpy = importlib.import_module("numpy")
        if _TICK_COLUMN_NUMPY_DTYPE is None:
            _TICK_COLUMN_NUMPY_DTYPE = numpy.dtype(
                {
                    "names": [
                        "record_schema_version",
                        "record_bytes",
                        "instrument_id",
                        "registry_ordinal",
                        "source_sequence",
                        "ingress_sequence",
                        "tick_stream_sequence",
                        "vendor_sequence_id",
                        "event_time_unix_ns",
                        "recv_realtime_ns",
                        "recv_monotonic_ns",
                        "exchange_time_ns_since_midnight",
                        "quality_flags",
                        "market_notices",
                        "source_stream_id",
                        "trade_date",
                        "vendor_local_time_raw",
                        "common_reserved",
                        "source_slot",
                        "event_kind",
                        "market",
                        "quantity_unit",
                        "security_type",
                        "asset_scope",
                        "validity_bitmap",
                        "projection_flags",
                        "channel",
                        "native_event_sequence",
                        "source_raw_code_1",
                        "source_raw_code_2",
                        "action",
                        "side",
                        "order_type",
                        "aggressor",
                        "phase",
                        "raw_type_length",
                        "raw_tick_flag_length",
                        "tick_reserved_u8",
                        "price_raw",
                        "price_p6",
                        "price_scale",
                        "price_valid",
                        "price_is_null",
                        "quantity_raw",
                        "quantity_scale",
                        "quantity_valid",
                        "quantity_is_null",
                        "trade_amount_raw",
                        "trade_amount_p6",
                        "trade_amount_scale",
                        "trade_amount_valid",
                        "trade_amount_is_null",
                        "matched_quantity_raw",
                        "matched_quantity_scale",
                        "matched_quantity_valid",
                        "matched_quantity_is_null",
                        "primary_order_id",
                        "buy_order_id",
                        "sell_order_id",
                    ],
                    "formats": [
                        "<u4", "<u4", "<u4", "<u4",
                        "<u8", "<u8", "<u8", "<u8",
                        "<i8", "<i8", "<i8", "<u8",
                        "<u8", "<u8", "<u4", "<u4",
                        "<u4", "<u4",
                        "u1", "u1", "u1", "u1", "u1", "u1",
                        "<u4", "<u4", "<i8", "<i8", "<i4", "<i4",
                        "u1", "u1", "u1", "u1", "u1", "u1", "u1",
                        "u1", "<i8", "<i8", "u1", "u1", "u1",
                        "<i8", "u1", "u1", "u1",
                        "<i8", "<i8", "u1", "u1", "u1",
                        "<i8", "u1", "u1", "u1",
                        "<i8", "<i8", "<i8",
                    ],
                    "offsets": [
                        0, 4, 8, 12, 16, 24, 32, 40,
                        48, 56, 64, 72, 80, 88, 96, 100, 104, 108,
                        112, 113, 114, 115, 116, 117,
                        128, 132, 136, 144, 152, 156,
                        160, 161, 162, 163, 164, 165, 166, 167,
                        168, 176, 184, 185, 186,
                        192, 200, 201, 202,
                        208, 216, 224, 225, 226,
                        232, 240, 241, 242,
                        248, 256, 264,
                    ],
                    "itemsize": TICK_BYTES,
                }
            )
        return numpy.frombuffer(
            self.wire_records,
            dtype=_TICK_COLUMN_NUMPY_DTYPE,
            count=len(self),
        )
