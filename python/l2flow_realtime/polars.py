"""Optional Polars adapters and an EOF-atomic raw-event history cache.

Importing :mod:`l2flow_realtime` does not import Polars.  This module also
defers the optional import until an adapter is constructed or a schema/frame
helper is called.  Install the ``polars`` project extra to use this surface.

The background history handle publishes only complete, explicit-EOF-verified
raw instrument-event generations.  Its tables contain source-slot 1/3 tick,
order, and transaction rows; they are not snapshot history and are not the
separate CERTIFIED derived-event stream.
"""

from __future__ import annotations

import array
import importlib
import importlib.util
import os
import tempfile
import threading
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping, Optional, Sequence

from ._history_worker_protocol import (
    DEFAULT_RESULT_COLUMNS,
    RESULT_COLUMN_BY_NAME,
)
from .certified_order_events import (
    CertifiedOrderEventState,
    _validate_history_coverage as _validate_certified_event_coverage,
)
from .models import (
    HistoryCoverageInfo,
    LatestStatus,
    UnavailableError,
)


POLARS_SCHEMA_VERSION = 1
POLARS_RAW_EVENT_DEFAULT_COLUMNS = DEFAULT_RESULT_COLUMNS

_POLARS: Any = None
_POLARS_LOCK = threading.Lock()
_UINT64_MAX = (1 << 64) - 1
_USE_CLIENT_TIMEOUT = object()


class PolarsUnavailableError(ImportError):
    """The optional Polars dependency is not installed."""


class PolarsSchemaError(ValueError):
    """Input rows do not satisfy the fixed L2Flow Polars schema."""


class PolarsHistoryError(RuntimeError):
    """Base error for the background Polars history handle."""


class PolarsHistoryNotReadyError(PolarsHistoryError):
    """No explicit-EOF-verified generation has been committed yet."""


class PolarsHistoryRefreshError(PolarsHistoryError):
    """The background updater failed or could not meet freshness."""


class PolarsHistoryClosedError(PolarsHistoryError):
    """The background history handle has been closed."""


class PolarsHistoryCoverageError(PolarsHistoryError):
    """A generation does not meet the requested temporal coverage."""


class PolarsHistoryState(Enum):
    STARTING = "starting"
    READY = "ready"
    REFRESHING = "refreshing"
    FAILED = "failed"
    CLOSED = "closed"


class PolarsHistoryCoverageOrigin(Enum):
    FROM_OPEN_CAPTURE = "from_open_capture"
    FROM_OPEN_RECOVERED = "from_open_recovered"
    PROCESS_START_PARTIAL = "process_start_partial"


class PolarsHistoryProductKind(Enum):
    """Stable semantic product domains eligible for atomic replacement."""

    FAST_RAW_INSTRUMENT_TICKS = "fast_raw_instrument_ticks_v2"
    FAST_DERIVED_INSTRUMENT_EVENTS = (
        "fast_derived_instrument_events_v1"
    )
    CERTIFIED_ORDER_EVENTS = "certified_order_events_v1"
    CERTIFIED_TICKS = "certified_ticks_v1"


@dataclass(frozen=True, slots=True)
class PolarsHistoryDatasetIdentity:
    """Session-independent identity of one logical Polars history dataset.

    Run/session and temporal origin deliberately do not participate because
    online promotion replaces a partial session with a distinct recovered
    session.  Numeric instrument IDs are scoped by ``catalog_digest``.  The
    ordered projection and both schema/projection versions prevent an equal-
    shaped but semantically different table from being substituted.
    """

    product_kind: PolarsHistoryProductKind
    instrument_id: Optional[int]
    projection_columns: tuple[str, ...]
    payload_projection: int
    polars_schema_version: int = POLARS_SCHEMA_VERSION
    catalog_digest: Optional[bytes] = None
    catalog_scope: Optional[int] = None
    catalog_version: Optional[int] = None
    market: Optional[int] = None

    def __post_init__(self) -> None:
        try:
            kind = PolarsHistoryProductKind(self.product_kind)
        except (TypeError, ValueError) as error:
            raise ValueError("product_kind is invalid") from error
        columns = self.projection_columns
        if (
            not isinstance(columns, tuple)
            or not columns
            or any(not isinstance(name, str) or not name for name in columns)
            or len(set(columns)) != len(columns)
        ):
            raise ValueError(
                "projection_columns must be a nonempty unique tuple"
            )
        for value, name in (
            (self.payload_projection, "payload_projection"),
            (self.polars_schema_version, "polars_schema_version"),
        ):
            if (
                not isinstance(value, int)
                or isinstance(value, bool)
                or value <= 0
                or value > 0xFFFFFFFF
            ):
                raise ValueError(f"{name} must be a positive uint32")
        instrument_product = kind in (
            PolarsHistoryProductKind.FAST_RAW_INSTRUMENT_TICKS,
            PolarsHistoryProductKind.FAST_DERIVED_INSTRUMENT_EVENTS,
        )
        digest = self.catalog_digest
        if digest is not None and (
            not isinstance(digest, bytes)
            or len(digest) != 32
            or not any(digest)
        ):
            raise ValueError(
                "catalog_digest must be nonzero exact 32-byte data"
            )
        qualified_catalog = (
            self.catalog_scope is not None
            or self.catalog_version is not None
        )
        if qualified_catalog:
            if digest is None:
                raise ValueError(
                    "catalog source/version require catalog_digest"
                )
            if (
                not isinstance(self.catalog_scope, int)
                or isinstance(self.catalog_scope, bool)
                or self.catalog_scope <= 0
                or self.catalog_scope > 0xFFFFFFFF
            ):
                raise ValueError(
                    "catalog_scope must be a positive uint32"
                )
            if (
                not isinstance(self.catalog_version, int)
                or isinstance(self.catalog_version, bool)
                or self.catalog_version <= 0
                or self.catalog_version > _UINT64_MAX
            ):
                raise ValueError(
                    "catalog_version must be a positive uint64"
                )
        if instrument_product:
            if (
                not isinstance(self.instrument_id, int)
                or isinstance(self.instrument_id, bool)
                or self.instrument_id <= 0
                or self.instrument_id > 0xFFFFFFFF
            ):
                raise ValueError(
                    "instrument products require a positive uint32 ID"
                )
            if digest is None:
                raise ValueError(
                    "instrument products require a nonzero catalog digest"
                )
        elif self.instrument_id is not None:
            raise ValueError(
                "global CERTIFIED products cannot carry an instrument"
            )
        derived = (
            kind
            is PolarsHistoryProductKind.FAST_DERIVED_INSTRUMENT_EVENTS
        )
        if derived:
            if (
                not isinstance(self.market, int)
                or isinstance(self.market, bool)
                or self.market <= 0
                or self.market > 0xFF
            ):
                raise ValueError(
                    "FAST-derived identity requires a nonzero uint8 market"
                )
        elif self.market is not None:
            raise ValueError(
                "market is only valid for FAST-derived instrument events"
            )
        object.__setattr__(self, "product_kind", kind)


def polars_available() -> bool:
    """Return whether the optional dependency can be imported, without it."""

    return importlib.util.find_spec("polars") is not None


def _require_polars():
    global _POLARS
    if _POLARS is not None:
        return _POLARS
    with _POLARS_LOCK:
        if _POLARS is not None:
            return _POLARS
        try:
            module = importlib.import_module("polars")
        except ModuleNotFoundError as error:
            if error.name != "polars":
                raise
            raise PolarsUnavailableError(
                "Polars support is optional; install "
                "l2flow-realtime[polars]"
            ) from error
        _POLARS = module
        return module


def _validate_column_names(
    columns: Sequence[str],
    *,
    available: Mapping[str, object],
) -> tuple[str, ...]:
    if isinstance(columns, (str, bytes, bytearray)):
        raise TypeError("columns must be a sequence of names")
    result = tuple(columns)
    if not result:
        raise ValueError("at least one column is required")
    if any(not isinstance(name, str) for name in result):
        raise TypeError("column names must be strings")
    if len(set(result)) != len(result):
        raise ValueError("column names must be unique")
    for name in result:
        if name not in available:
            raise KeyError(name)
    return result


def _raw_dtype(format_: str):
    pl = _require_polars()
    try:
        return {
            "B": pl.UInt8,
            "I": pl.UInt32,
            "Q": pl.UInt64,
            "i": pl.Int32,
            "q": pl.Int64,
        }[format_]
    except KeyError:
        raise PolarsSchemaError(
            f"unsupported raw result scalar format {format_!r}"
        ) from None


def raw_event_schema(
    columns: Sequence[str] = POLARS_RAW_EVENT_DEFAULT_COLUMNS,
) -> dict[str, Any]:
    """Return the fixed schema for selected raw instrument-event columns."""

    names = _validate_column_names(
        columns, available=RESULT_COLUMN_BY_NAME
    )
    return {
        name: _raw_dtype(RESULT_COLUMN_BY_NAME[name].format)
        for name in names
    }


def _owned_raw_series(name: str, values: Sequence[object]):
    """Copy one worker-ring column into an independently owned C buffer."""

    pl = _require_polars()
    spec = RESULT_COLUMN_BY_NAME[name]
    try:
        owned = (
            values
            if isinstance(values, array.array)
            and values.typecode == spec.format
            else array.array(spec.format, values)
        )
        return pl.Series(
            name,
            owned,
            dtype=_raw_dtype(spec.format),
            strict=True,
        )
    except (OverflowError, TypeError, ValueError) as error:
        raise PolarsSchemaError(
            f"raw event column {name!r} violates its fixed schema"
        ) from error


def empty_raw_event_frame(
    columns: Sequence[str] = POLARS_RAW_EVENT_DEFAULT_COLUMNS,
):
    """Build a zero-row DataFrame with the exact selected raw schema."""

    pl = _require_polars()
    schema = raw_event_schema(columns)
    return pl.DataFrame(
        [pl.Series(name, [], dtype=dtype) for name, dtype in schema.items()]
    )


def raw_event_batch_frame(
    batch,
    *,
    columns: Optional[Sequence[str]] = None,
):
    """Copy one leased raw-event batch into an owned Polars DataFrame."""

    if columns is None:
        try:
            columns = batch.columns.column_names
        except AttributeError as error:
            raise TypeError(
                "batch does not expose raw event column names"
            ) from error
    names = _validate_column_names(
        columns, available=RESULT_COLUMN_BY_NAME
    )
    copy_arrays = getattr(batch, "copy_column_arrays", None)
    values = (
        copy_arrays(*names)
        if callable(copy_arrays)
        else batch.read_columns(*names)
    )
    try:
        record_count = int(batch.record_count)
    except (AttributeError, TypeError, ValueError) as error:
        raise TypeError("batch does not expose a valid record_count") from error
    if record_count < 0:
        raise PolarsSchemaError("batch record_count is negative")
    if any(len(values[name]) != record_count for name in names):
        raise PolarsSchemaError(
            "raw event column lengths disagree with record_count"
        )
    pl = _require_polars()
    return pl.DataFrame(
        [_owned_raw_series(name, values[name]) for name in names]
    )


def raw_event_batch_lazyframe(
    batch,
    *,
    columns: Optional[Sequence[str]] = None,
):
    """Capture one batch eagerly, then return a LazyFrame over that cut."""

    return raw_event_batch_frame(batch, columns=columns).lazy()


def history_coverage_frame(coverage: HistoryCoverageInfo):
    """Return one row of session-scoped history coverage metadata."""

    if not isinstance(coverage, HistoryCoverageInfo):
        raise TypeError("coverage must be HistoryCoverageInfo")
    pl = _require_polars()
    return pl.DataFrame(
        {
            "run_id": [coverage.run_id],
            "session_epoch": [coverage.session_epoch],
            "trade_date": [coverage.trade_date],
            "coverage_kind": [int(coverage.coverage_kind)],
            "coverage_start_unix_ns": [
                coverage.coverage_start_unix_ns
            ],
        },
        schema={
            "run_id": pl.Binary,
            "session_epoch": pl.UInt64,
            "trade_date": pl.UInt32,
            "coverage_kind": pl.UInt8,
            "coverage_start_unix_ns": pl.UInt64,
        },
        strict=True,
    )


_COMMON_SCHEMA_NAMES = (
    "record_schema_version",
    "record_bytes",
    "ordinal",
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
    "source_slot",
    "event_kind",
    "market",
    "quantity_unit",
    "security_type",
    "asset_scope",
)


def _common_schema() -> dict[str, Any]:
    pl = _require_polars()
    return {
        "record_schema_version": pl.UInt32,
        "record_bytes": pl.UInt32,
        "ordinal": pl.UInt32,
        "source_sequence": pl.UInt64,
        "ingress_sequence": pl.UInt64,
        "tick_stream_sequence": pl.UInt64,
        "vendor_sequence_id": pl.UInt64,
        "event_time_unix_ns": pl.Int64,
        "recv_realtime_ns": pl.Int64,
        "recv_monotonic_ns": pl.Int64,
        "exchange_time_ns_since_midnight": pl.UInt64,
        "quality_flags": pl.UInt64,
        "market_notices": pl.UInt64,
        "source_stream_id": pl.UInt32,
        "trade_date": pl.UInt32,
        "vendor_local_time_raw": pl.UInt32,
        "source_slot": pl.UInt8,
        "event_kind": pl.UInt8,
        "market": pl.UInt8,
        "quantity_unit": pl.UInt8,
        "security_type": pl.UInt8,
        "asset_scope": pl.UInt8,
    }


def _latest_prefix_schema() -> dict[str, Any]:
    pl = _require_polars()
    return {
        "request_index": pl.UInt32,
        "session_epoch": pl.UInt64,
        "instrument_id": pl.UInt32,
        "status": pl.UInt8,
    }


def latest_snapshot_schema(*, include_wire_payload: bool = False):
    pl = _require_polars()
    schema = _latest_prefix_schema()
    schema.update(_common_schema())
    schema.update(
        {
            "last_price_raw": pl.Int64,
            "last_price_p6": pl.Int64,
            "last_price_scale": pl.UInt8,
            "last_price_valid": pl.Boolean,
            "last_price_is_null": pl.Boolean,
        }
    )
    if include_wire_payload:
        schema["wire_payload"] = pl.Binary
    return schema


def latest_tick_schema(*, include_wire_payload: bool = False):
    pl = _require_polars()
    schema = _latest_prefix_schema()
    schema.update(_common_schema())
    schema.update(
        {
            "price_raw": pl.Int64,
            "price_p6": pl.Int64,
            "price_scale": pl.UInt8,
            "price_valid": pl.Boolean,
            "price_is_null": pl.Boolean,
            "quantity_raw": pl.Int64,
            "quantity_scale": pl.UInt8,
            "quantity_valid": pl.Boolean,
            "quantity_is_null": pl.Boolean,
            "action": pl.UInt8,
            "side": pl.UInt8,
            "projection_flags": pl.UInt32,
        }
    )
    if include_wire_payload:
        schema["wire_payload"] = pl.Binary
    return schema


def certified_tick_schema(*, include_wire_payload: bool = False):
    """Return the fixed schema for dense CERTIFIED Tick journal rows."""

    pl = _require_polars()
    schema = {
        "canonical_apply_sequence": pl.UInt64,
        "correction_epoch": pl.UInt64,
        "feed_epoch": pl.UInt64,
        "certified_monotonic_ns": pl.UInt64,
        "instrument_id": pl.UInt32,
    }
    schema.update(_common_schema())
    schema.update(
        {
            "price_raw": pl.Int64,
            "price_p6": pl.Int64,
            "price_scale": pl.UInt8,
            "price_valid": pl.Boolean,
            "price_is_null": pl.Boolean,
            "quantity_raw": pl.Int64,
            "quantity_scale": pl.UInt8,
            "quantity_valid": pl.Boolean,
            "quantity_is_null": pl.Boolean,
            "action": pl.UInt8,
            "side": pl.UInt8,
            "projection_flags": pl.UInt32,
        }
    )
    if include_wire_payload:
        schema["wire_payload"] = pl.Binary
    return schema


def latest_kline_schema(
    *,
    include_wire_payload: bool = False,
    include_session_coverage: bool = True,
):
    pl = _require_polars()
    schema = _latest_prefix_schema()
    schema["window_id"] = pl.UInt32
    schema.update(
        {
            "generation": pl.UInt64,
            "trade_date": pl.UInt32,
            "window_duration_ns": pl.UInt64,
            "window_start_ns_since_midnight": pl.UInt64,
            "window_end_ns_since_midnight": pl.UInt64,
            "window_start_unix_ns": pl.Int64,
            "window_end_unix_ns": pl.Int64,
            "open_price_p6": pl.Int64,
            "high_price_p6": pl.Int64,
            "low_price_p6": pl.Int64,
            "close_price_p6": pl.Int64,
            "volume_raw": pl.Int64,
            "trade_count": pl.UInt64,
            "revision": pl.UInt64,
            "first_event_time_ns_since_midnight": pl.UInt64,
            "first_event_sequence": pl.UInt64,
            "first_source_sequence": pl.UInt64,
            "first_ingress_sequence": pl.UInt64,
            "last_event_time_ns_since_midnight": pl.UInt64,
            "last_event_sequence": pl.UInt64,
            "last_source_sequence": pl.UInt64,
            "last_ingress_sequence": pl.UInt64,
            "volume_scale": pl.UInt8,
            "quantity_unit": pl.UInt8,
            "coverage_flags": pl.UInt32,
            "temporal_coverage": pl.UInt8,
            "process_start_partial": pl.Boolean,
            "natural_window_left_truncated": pl.Boolean,
        }
    )
    if include_session_coverage:
        schema.update(
            {
                "session_coverage_kind": pl.UInt8,
                "coverage_start_unix_ns": pl.UInt64,
            }
        )
    if include_wire_payload:
        schema["wire_payload"] = pl.Binary
    return schema


def _frame_from_rows(rows: Sequence[Mapping[str, object]], schema):
    pl = _require_polars()
    return pl.DataFrame(rows, schema=schema, strict=True)


def _common_values(common) -> dict[str, object]:
    if common is None:
        return {name: None for name in _COMMON_SCHEMA_NAMES}
    values = {}
    for name in _COMMON_SCHEMA_NAMES:
        value = getattr(common, name)
        if name in ("event_kind", "market"):
            value = int(value)
        values[name] = value
    return values


def _decimal_values(prefix: str, value) -> dict[str, object]:
    if value is None:
        return {
            f"{prefix}_raw": None,
            f"{prefix}_p6": None,
            f"{prefix}_scale": None,
            f"{prefix}_valid": None,
            f"{prefix}_is_null": None,
        }
    return {
        f"{prefix}_raw": value.raw,
        f"{prefix}_p6": value.normalized_p6,
        f"{prefix}_scale": value.scale,
        f"{prefix}_valid": value.valid,
        f"{prefix}_is_null": value.is_null,
    }


def _quantity_values(prefix: str, value) -> dict[str, object]:
    if value is None:
        return {
            f"{prefix}_raw": None,
            f"{prefix}_scale": None,
            f"{prefix}_valid": None,
            f"{prefix}_is_null": None,
        }
    return {
        f"{prefix}_raw": value.raw,
        f"{prefix}_scale": value.scale,
        f"{prefix}_valid": value.valid,
        f"{prefix}_is_null": value.is_null,
    }


def latest_snapshots_frame(
    values: Sequence[object],
    *,
    include_wire_payload: bool = False,
):
    rows = []
    for request_index, value in enumerate(values):
        row = {
            "request_index": request_index,
            "session_epoch": value.session_epoch,
            "instrument_id": value.instrument_id,
            "status": int(value.status),
        }
        row.update(_common_values(value.common))
        row.update(_decimal_values("last_price", value.last_price))
        if include_wire_payload:
            row["wire_payload"] = value.wire_payload
        rows.append(row)
    return _frame_from_rows(
        rows,
        latest_snapshot_schema(
            include_wire_payload=include_wire_payload
        ),
    )


def latest_ticks_frame(
    values: Sequence[object],
    *,
    include_wire_payload: bool = False,
):
    rows = []
    for request_index, value in enumerate(values):
        row = {
            "request_index": request_index,
            "session_epoch": value.session_epoch,
            "instrument_id": value.instrument_id,
            "status": int(value.status),
        }
        row.update(_common_values(value.common))
        row.update(_decimal_values("price", value.price))
        row.update(_quantity_values("quantity", value.quantity))
        row.update(
            {
                "action": (
                    None if value.action is None else int(value.action)
                ),
                "side": None if value.side is None else int(value.side),
                "projection_flags": (
                    None
                    if value.projection_flags is None
                    else int(value.projection_flags)
                ),
            }
        )
        if include_wire_payload:
            row["wire_payload"] = value.wire_payload
        rows.append(row)
    return _frame_from_rows(
        rows,
        latest_tick_schema(include_wire_payload=include_wire_payload),
    )


def certified_tick_batch_frame(
    batch,
    *,
    include_wire_payload: bool = False,
):
    """Copy one owned CERTIFIED Tick batch into a typed DataFrame.

    The native reader has already validated the stable slot copies and dense
    canonical cursor.  Conversion occurs off the FAST path and the resulting
    Polars columns own their data independently of the reader and batch.
    """

    rows = []
    for value in batch:
        row = {
            "canonical_apply_sequence": (
                value.canonical_apply_sequence
            ),
            "correction_epoch": value.correction_epoch,
            "feed_epoch": value.feed_epoch,
            "certified_monotonic_ns": value.certified_monotonic_ns,
            "instrument_id": value.common.instrument_id,
        }
        row.update(_common_values(value.common))
        row.update(_decimal_values("price", value.price))
        row.update(_quantity_values("quantity", value.quantity))
        row.update(
            {
                "action": int(value.action),
                "side": int(value.side),
                "projection_flags": int(value.projection_flags),
            }
        )
        if include_wire_payload:
            row["wire_payload"] = value.wire_payload
        rows.append(row)
    return _frame_from_rows(
        rows,
        certified_tick_schema(
            include_wire_payload=include_wire_payload
        ),
    )


def certified_tick_batch_lazyframe(
    batch,
    *,
    include_wire_payload: bool = False,
):
    """Return a LazyFrame pinned to one owned CERTIFIED Tick batch."""

    return certified_tick_batch_frame(
        batch,
        include_wire_payload=include_wire_payload,
    ).lazy()


_KLINE_PAYLOAD_NAMES = (
    "generation",
    "trade_date",
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
)


def latest_klines_frame(
    values: Sequence[object],
    *,
    session_coverage=None,
    include_wire_payload: bool = False,
    include_session_coverage: bool = True,
):
    if include_session_coverage and session_coverage is None:
        raise ValueError(
            "session_coverage is required when it is included"
        )
    rows = []
    for request_index, value in enumerate(values):
        available = value.status is LatestStatus.AVAILABLE
        row = {
            "request_index": request_index,
            "session_epoch": value.session_epoch,
            "instrument_id": value.instrument_id,
            "status": int(value.status),
            "window_id": value.window_id,
        }
        row.update(
            {
                name: getattr(value, name) if available else None
                for name in _KLINE_PAYLOAD_NAMES
            }
        )
        row.update(
            {
                "coverage_flags": (
                    int(value.coverage_flags) if available else None
                ),
                "temporal_coverage": (
                    int(value.temporal_coverage) if available else None
                ),
                "process_start_partial": (
                    value.process_start_partial if available else None
                ),
                "natural_window_left_truncated": (
                    value.natural_window_left_truncated
                    if available
                    else None
                ),
            }
        )
        if include_session_coverage:
            row.update(
                {
                    "session_coverage_kind": int(
                        session_coverage.coverage_kind
                    ),
                    "coverage_start_unix_ns": (
                        session_coverage.coverage_start_unix_ns
                    ),
                }
            )
        if include_wire_payload:
            row["wire_payload"] = value.wire_payload
        rows.append(row)
    return _frame_from_rows(
        rows,
        latest_kline_schema(
            include_wire_payload=include_wire_payload,
            include_session_coverage=include_session_coverage,
        ),
    )


_DERIVED_UINT64_COLUMNS = (
    "derived_event_sequence",
    "revision",
    "trade_count",
    "cancel_count",
    "quality_flags",
    "source_quality_flags",
    "source_market_notices",
    "source_sequence",
    "ingress_sequence",
    "tick_stream_sequence",
    "vendor_sequence_id",
    "event_time_ns_since_midnight",
    "vendor_local_time_ns_since_midnight",
)
_DERIVED_UINT32_COLUMNS = (
    "trade_date",
    "instrument_id",
    "vendor_local_time_raw",
)
_DERIVED_UINT8_COLUMNS = (
    "market",
    "event_kind",
    "operation",
    "finality",
    "side",
    "side_source",
    "aggressor",
    "phase",
    "phase_at_first",
    "phase_at_add",
    "phase_at_last",
    "order_type",
    "order_source",
    "price_source",
    "original_quantity_status",
)
_DERIVED_INT64_COLUMNS = (
    "channel",
    "order_id",
    "buy_order_id",
    "sell_order_id",
    "price_p6",
    "execution_boundary_price_p6",
    "quantity",
    "trade_amount_p6",
    "published_quantity",
    "original_quantity",
    "remaining_quantity",
    "source_matched_quantity",
    "observed_pre_add_trade_quantity",
    "post_add_trade_quantity",
    "total_trade_quantity",
    "total_cancel_quantity",
    "native_event_sequence",
    "event_time_unix_ns",
    "recv_realtime_ns",
    "recv_monotonic_ns",
)
_DERIVED_BOOLEAN_COLUMNS = (
    "price_valid",
    "execution_boundary_price_valid",
    "trade_amount_valid",
    "published_quantity_valid",
    "original_quantity_valid",
    "remaining_quantity_valid",
    "source_matched_quantity_valid",
    "add_seen",
    "apply_to_book",
    "referenced_order_found",
    "side_from_order",
    "event_time_valid",
    "event_time_unix_ns_valid",
    "vendor_local_time_valid",
)
_DERIVED_COLUMN_ORDER = (
    "derived_event_sequence",
    "trade_date",
    "instrument_id",
    "market",
    "event_kind",
    "channel",
    "order_id",
    "buy_order_id",
    "sell_order_id",
    "operation",
    "finality",
    "revision",
    "side",
    "side_source",
    "aggressor",
    "phase",
    "phase_at_first",
    "phase_at_add",
    "phase_at_last",
    "order_type",
    "order_source",
    "price_source",
    "original_quantity_status",
    "price_p6",
    "price_valid",
    "execution_boundary_price_p6",
    "execution_boundary_price_valid",
    "quantity",
    "trade_amount_p6",
    "trade_amount_valid",
    "published_quantity",
    "published_quantity_valid",
    "original_quantity",
    "original_quantity_valid",
    "remaining_quantity",
    "remaining_quantity_valid",
    "source_matched_quantity",
    "source_matched_quantity_valid",
    "observed_pre_add_trade_quantity",
    "post_add_trade_quantity",
    "total_trade_quantity",
    "total_cancel_quantity",
    "trade_count",
    "cancel_count",
    "quality_flags",
    "source_quality_flags",
    "source_market_notices",
    "add_seen",
    "apply_to_book",
    "referenced_order_found",
    "side_from_order",
    "native_event_sequence",
    "source_sequence",
    "ingress_sequence",
    "tick_stream_sequence",
    "vendor_sequence_id",
    "event_time_ns_since_midnight",
    "event_time_unix_ns",
    "recv_realtime_ns",
    "recv_monotonic_ns",
    "vendor_local_time_raw",
    "vendor_local_time_ns_since_midnight",
    "event_time_valid",
    "event_time_unix_ns_valid",
    "vendor_local_time_valid",
    "source_tick_event_ordinal",
    "event_uid",
)


def derived_event_schema(*, certified: bool = False) -> dict[str, Any]:
    """Return the fixed typed event schema, including nullable Event UID."""

    if not isinstance(certified, bool):
        raise TypeError("certified must be bool")
    pl = _require_polars()
    schema: dict[str, Any] = {}
    if certified:
        schema["canonical_apply_sequence"] = pl.UInt64
    data_types = {
        **{name: pl.UInt64 for name in _DERIVED_UINT64_COLUMNS},
        **{name: pl.UInt32 for name in _DERIVED_UINT32_COLUMNS},
        **{name: pl.UInt8 for name in _DERIVED_UINT8_COLUMNS},
        **{name: pl.Int64 for name in _DERIVED_INT64_COLUMNS},
        **{name: pl.Boolean for name in _DERIVED_BOOLEAN_COLUMNS},
    }
    # Schema 2 supplies both fields for source-backed rows.  Source-free daily
    # finalization rows deliberately keep them null.  Product-local dense
    # event sequences are never substituted for the source-tick ordinal.
    data_types["source_tick_event_ordinal"] = pl.UInt32
    data_types["event_uid"] = pl.Binary
    schema.update(
        {name: data_types[name] for name in _DERIVED_COLUMN_ORDER}
    )
    return schema


def _raw_history_dataset_identity(
    checkpoint, columns: Sequence[str]
) -> PolarsHistoryDatasetIdentity:
    return PolarsHistoryDatasetIdentity(
        product_kind=(
            PolarsHistoryProductKind.FAST_RAW_INSTRUMENT_TICKS
        ),
        instrument_id=checkpoint.instrument_id,
        projection_columns=tuple(columns),
        payload_projection=checkpoint.payload_projection,
        catalog_digest=checkpoint.catalog_digest,
    )


def _derived_history_dataset_identity(
    checkpoint,
) -> PolarsHistoryDatasetIdentity:
    raw = checkpoint.raw_checkpoint
    return PolarsHistoryDatasetIdentity(
        product_kind=(
            PolarsHistoryProductKind.FAST_DERIVED_INSTRUMENT_EVENTS
        ),
        instrument_id=checkpoint.instrument_id,
        projection_columns=tuple(derived_event_schema()),
        payload_projection=raw.payload_projection,
        catalog_digest=raw.catalog_digest,
        market=int(checkpoint.market),
    )


def _certified_order_event_dataset_identity(
    session=None,
) -> PolarsHistoryDatasetIdentity:
    catalog = (
        {}
        if session is None
        else {
            "catalog_digest": session.catalog_digest,
            "catalog_scope": int(session.catalog_scope),
            "catalog_version": session.catalog_version,
        }
    )
    return PolarsHistoryDatasetIdentity(
        product_kind=PolarsHistoryProductKind.CERTIFIED_ORDER_EVENTS,
        instrument_id=None,
        projection_columns=tuple(derived_event_schema(certified=True)),
        # CERTIFIED Event result/wire projection V1.
        payload_projection=1,
        **catalog,
    )


def _certified_tick_dataset_identity(
    *,
    include_wire_payload: bool,
    session=None,
) -> PolarsHistoryDatasetIdentity:
    schema = certified_tick_schema(
        include_wire_payload=include_wire_payload
    )
    catalog = (
        {}
        if session is None
        else {
            "catalog_digest": session.catalog_digest,
            "catalog_scope": int(session.catalog_scope),
            "catalog_version": session.catalog_version,
        }
    )
    return PolarsHistoryDatasetIdentity(
        product_kind=PolarsHistoryProductKind.CERTIFIED_TICKS,
        instrument_id=None,
        projection_columns=tuple(schema),
        # The append-only CERTIFIED Tick journal/export contract is V1.
        payload_projection=1,
        **catalog,
    )


def _derived_event_row(event) -> dict[str, object]:
    result: dict[str, object] = {}
    for name in _DERIVED_UINT64_COLUMNS:
        result[name] = getattr(event, name)
    for name in _DERIVED_UINT32_COLUMNS:
        result[name] = getattr(event, name)
    for name in _DERIVED_UINT8_COLUMNS:
        value = getattr(event, name)
        result[name] = int(value)
    for name in _DERIVED_INT64_COLUMNS:
        result[name] = getattr(event, name)
    for name in _DERIVED_BOOLEAN_COLUMNS:
        result[name] = bool(getattr(event, name))
    ordinal = getattr(event, "source_tick_event_ordinal", None)
    uid = getattr(event, "event_uid", None)
    if uid is not None and ordinal is None:
        raise PolarsSchemaError(
            "event_uid is present without source_tick_event_ordinal"
        )
    encoded_uid = None if uid is None else bytes(uid)
    if encoded_uid is not None and len(encoded_uid) != 48:
        raise PolarsSchemaError("event_uid must encode to exactly 48 bytes")
    result["source_tick_event_ordinal"] = ordinal
    result["event_uid"] = encoded_uid
    return result


def derived_events_frame(events: Sequence[object]):
    """Build an owned event DataFrame without inventing Event UIDs."""

    return _frame_from_rows(
        [_derived_event_row(event) for event in events],
        derived_event_schema(),
    )


def derived_event_batch_frame(batch):
    """Materialize one derived-history batch into an owned DataFrame."""

    return derived_events_frame(tuple(batch))


def live_order_event_batch_frame(batch):
    """Materialize a live Event batch with nullable structural Event UIDs."""

    try:
        events = tuple(batch.event(index) for index in range(len(batch)))
    except AttributeError as error:
        raise TypeError("batch does not expose typed live events") from error
    return derived_events_frame(events)


def certified_order_event_batch_frame(batch):
    """Materialize one CERTIFIED batch, retaining canonical apply sequence."""

    rows = []
    for envelope in batch:
        row = {
            "canonical_apply_sequence": envelope.canonical_apply_sequence
        }
        row.update(_derived_event_row(envelope.event))
        rows.append(row)
    return _frame_from_rows(rows, derived_event_schema(certified=True))


@dataclass(frozen=True, slots=True)
class PolarsInstrumentTickHistorySnapshot:
    """One immutable, explicit-EOF-verified table generation."""

    _frame: Any
    checkpoint: Any
    history_coverage: HistoryCoverageInfo
    cache_version: int
    coverage_origin: PolarsHistoryCoverageOrigin
    cache_committed_monotonic_ns: int
    manifest_chunk_count: int
    dataset_identity: PolarsHistoryDatasetIdentity

    @property
    def dataframe(self):
        """Return a shallow frame clone sharing immutable column buffers."""

        return self._frame.clone()

    @property
    def lazyframe(self):
        """Return a LazyFrame pinned to this already-captured generation."""

        return self._frame.clone().lazy()

    @property
    def run_id(self) -> bytes:
        return self.checkpoint.run_id

    @property
    def session_epoch(self) -> int:
        return self.checkpoint.session_epoch

    @property
    def trade_date(self) -> int:
        return self.checkpoint.trade_date

    @property
    def instrument_id(self) -> int:
        return self.checkpoint.instrument_id

    @property
    def generation(self) -> int:
        return self.checkpoint.generation

    @property
    def row_count(self) -> int:
        return self._frame.height

    @property
    def coverage_from_open(self) -> bool:
        return self.history_coverage.coverage_from_open

    @property
    def catalog_coverage_complete(self) -> bool:
        return self.checkpoint.coverage_complete

    @property
    def record_coverage_complete(self) -> bool:
        return self.checkpoint.record_coverage_complete

    @property
    def tick_record_coverage_complete(self) -> bool:
        return self.checkpoint.tick_record_coverage_complete

    @property
    def coverage_start_unix_ns(self) -> Optional[int]:
        return self.history_coverage.coverage_start_unix_ns

    @property
    def history_published_monotonic_ns(self) -> int:
        return self.checkpoint.history_published_monotonic_ns


@dataclass(frozen=True, slots=True)
class _CommittedHistory:
    frame: Any
    chunks: tuple[Any, ...]
    checkpoint: Any
    history_coverage: HistoryCoverageInfo
    version: int
    committed_ns: int


def _positive_float_or_none(value, field: str) -> Optional[float]:
    if value is None:
        return None
    if (
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or value <= 0
    ):
        raise ValueError(f"{field} must be positive or None")
    return float(value)


def _positive_int_or_none(value, field: str) -> Optional[int]:
    if value is None:
        return None
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or value <= 0
        or value > _UINT64_MAX
    ):
        raise ValueError(f"{field} must be a positive uint64 or None")
    return value


def _compact_owned_history_chunks(
    chunks: Sequence[Any], compact_after_chunks: Optional[int]
) -> tuple[Any, ...]:
    """Bound private authoritative chunk retention with owned buffers.

    Live reconciliation handles retain their durable History prefix because a
    later generation is rebuilt from that prefix plus a verified delta.
    Compacting only the public manifest would leave those private per-delta
    DataFrames alive indefinitely.  This mirrors the manifest's strict
    ``len(chunks) > threshold`` rule and returns one rechunked owned frame.
    Callers replace their private reference only after public commit, so old
    pinned snapshots continue to own their prior buffers independently.
    """

    normalized = tuple(chunk for chunk in chunks if chunk.height)
    if (
        compact_after_chunks is None
        or len(normalized) <= compact_after_chunks
    ):
        return normalized
    pl = _require_polars()
    compacted = pl.concat(
        normalized, how="vertical", rechunk=False
    ).rechunk()
    return (compacted,)


@dataclass(frozen=True, slots=True)
class LivePolarsHistorySnapshot:
    """One immutable generation published by :class:`LivePolarsHistory`.

    ``dataframe`` is a shallow clone.  It shares immutable Arrow buffers with
    the committed manifest while protecting the store's DataFrame object from
    caller mutation.  ``lazyframe`` is therefore pinned to this generation;
    collecting it after a later publication cannot observe mixed generations.
    """

    _frame: Any
    _chunks: tuple[Any, ...]
    history_coverage: HistoryCoverageInfo
    generation: int
    continuity_token: Any
    metadata: Any
    source: str
    version: int
    committed_monotonic_ns: int
    dataset_identity: Optional[PolarsHistoryDatasetIdentity]

    @property
    def dataframe(self):
        return self._frame.clone()

    @property
    def lazyframe(self):
        return self._frame.clone().lazy()

    @property
    def row_count(self) -> int:
        return self._frame.height

    @property
    def manifest_chunk_count(self) -> int:
        return len(self._chunks)


@dataclass(frozen=True, slots=True)
class _LivePolarsCommit:
    frame: Any
    chunks: tuple[Any, ...]
    history_coverage: HistoryCoverageInfo
    generation: int
    continuity_token: Any
    metadata: Any
    source: str
    version: int
    committed_monotonic_ns: int
    dataset_identity: Optional[PolarsHistoryDatasetIdentity]


class LivePolarsHistory:
    """Thread-safe immutable Polars manifest with atomic pointer publication.

    Producers build owned DataFrame chunks outside the lock and then call
    ``publish_full`` or ``publish_delta``.  A delta is accepted only against
    the exact continuity token observed by the producer.  The lock protects a
    single manifest-pointer swap; concatenation and optional rechunking never
    run on readers or while the publication lock is held.

    This class does not silently evict rows.  Limits fail closed and retain the
    previous committed generation.  ``spill`` writes an atomic Parquet copy
    but deliberately does not replace the in-memory frame, so enabling a spill
    cannot add disk I/O to DataFrame/LazyFrame reads.
    """

    def __init__(
        self,
        schema: Mapping[str, Any],
        history_coverage: HistoryCoverageInfo,
        *,
        maximum_rows: Optional[int] = None,
        maximum_chunks: Optional[int] = None,
        compact_after_chunks: Optional[int] = 256,
    ) -> None:
        _require_polars()
        if not isinstance(schema, Mapping) or not schema:
            raise ValueError("schema must be a nonempty mapping")
        if not isinstance(history_coverage, HistoryCoverageInfo):
            raise TypeError("history_coverage must be HistoryCoverageInfo")
        if not history_coverage.available:
            raise PolarsHistoryCoverageError(
                "history coverage is unavailable for this session"
            )
        self._schema = dict(schema)
        self._history_coverage = history_coverage
        self._maximum_rows = _positive_int_or_none(
            maximum_rows, "maximum_rows"
        )
        self._maximum_chunks = _positive_int_or_none(
            maximum_chunks, "maximum_chunks"
        )
        self._compact_after_chunks = _positive_int_or_none(
            compact_after_chunks, "compact_after_chunks"
        )
        self._condition = threading.Condition(threading.RLock())
        self._commit: Optional[_LivePolarsCommit] = None
        self._state = PolarsHistoryState.STARTING
        self._last_error: Optional[BaseException] = None

    @property
    def schema(self) -> dict[str, Any]:
        return dict(self._schema)

    @property
    def history_coverage(self) -> HistoryCoverageInfo:
        return self._history_coverage

    @property
    def state(self) -> PolarsHistoryState:
        with self._condition:
            return self._state

    @property
    def last_error(self) -> Optional[BaseException]:
        with self._condition:
            return self._last_error

    @property
    def continuity_token(self):
        with self._condition:
            return (
                None
                if self._commit is None
                else self._commit.continuity_token
            )

    @staticmethod
    def _same_token(left, right) -> bool:
        try:
            result = left == right
        except BaseException as error:
            raise PolarsSchemaError(
                "continuity tokens are not equality comparable"
            ) from error
        if not isinstance(result, bool):
            raise PolarsSchemaError(
                "continuity-token equality must return bool"
            )
        return result

    def _validated_chunks(self, chunks: Sequence[Any]) -> tuple[Any, ...]:
        if isinstance(chunks, (str, bytes, bytearray)):
            raise TypeError("chunks must be a sequence of DataFrames")
        pl = _require_polars()
        result = []
        for chunk in chunks:
            if not isinstance(chunk, pl.DataFrame):
                raise TypeError("every manifest chunk must be a DataFrame")
            if tuple(chunk.schema.items()) != tuple(self._schema.items()):
                raise PolarsSchemaError(
                    "manifest chunk does not match the fixed schema"
                )
            if chunk.height:
                # clone() is metadata-only for immutable Polars buffers and
                # prevents a caller from retaining our DataFrame object.
                result.append(chunk.clone())
        return tuple(result)

    def _build_commit(
        self,
        *,
        base: Optional[_LivePolarsCommit],
        chunks: Sequence[Any],
        generation: int,
        continuity_token: Any,
        metadata: Any,
        source: str,
        expected_total_rows: int,
        history_coverage: Optional[HistoryCoverageInfo] = None,
        dataset_identity: Optional[PolarsHistoryDatasetIdentity] = None,
    ) -> _LivePolarsCommit:
        if (
            not isinstance(generation, int)
            or isinstance(generation, bool)
            or generation < 0
            or generation > _UINT64_MAX
        ):
            raise ValueError("generation must fit uint64")
        if continuity_token is None:
            raise ValueError("continuity_token must not be None")
        if base is not None and generation < base.generation:
            raise PolarsHistoryRefreshError(
                "manifest generation moved backwards"
            )
        if (
            not isinstance(expected_total_rows, int)
            or isinstance(expected_total_rows, bool)
            or expected_total_rows < 0
            or expected_total_rows > _UINT64_MAX
        ):
            raise ValueError("expected_total_rows must fit uint64")
        if not isinstance(source, str) or not source:
            raise ValueError("source must be a nonempty string")
        coverage = (
            self._history_coverage
            if history_coverage is None
            else history_coverage
        )
        if not isinstance(coverage, HistoryCoverageInfo):
            raise TypeError("history_coverage must be HistoryCoverageInfo")
        identity = (
            None
            if base is None
            else base.dataset_identity
        )
        if dataset_identity is not None:
            if not isinstance(
                dataset_identity, PolarsHistoryDatasetIdentity
            ):
                raise TypeError(
                    "dataset_identity must be PolarsHistoryDatasetIdentity"
                )
            if identity is not None and dataset_identity != identity:
                raise PolarsHistoryCoverageError(
                    "an append cannot change dataset identity"
                )
            identity = dataset_identity
        owned = self._validated_chunks(chunks)
        manifest = (() if base is None else base.chunks) + owned
        pl = _require_polars()
        if manifest:
            frame = pl.concat(manifest, how="vertical", rechunk=False)
        else:
            frame = pl.DataFrame(
                [
                    pl.Series(name, [], dtype=dtype)
                    for name, dtype in self._schema.items()
                ]
            )
        if frame.height != expected_total_rows:
            raise PolarsSchemaError(
                "manifest row count does not match the verified boundary"
            )
        if (
            self._maximum_rows is not None
            and frame.height > self._maximum_rows
        ):
            raise PolarsHistoryRefreshError(
                "complete history exceeds maximum_rows; no rows were evicted"
            )
        if (
            self._compact_after_chunks is not None
            and len(manifest) > self._compact_after_chunks
        ):
            compacted = frame.rechunk()
            manifest = (compacted,)
            frame = compacted
        if (
            self._maximum_chunks is not None
            and len(manifest) > self._maximum_chunks
        ):
            raise PolarsHistoryRefreshError(
                "complete history exceeds maximum_chunks; no rows were evicted"
            )
        return _LivePolarsCommit(
            frame=frame,
            chunks=manifest,
            history_coverage=coverage,
            generation=generation,
            continuity_token=continuity_token,
            metadata=metadata,
            source=source,
            version=1 if base is None else base.version + 1,
            committed_monotonic_ns=time.monotonic_ns(),
            dataset_identity=identity,
        )

    def _install(
        self,
        commit: _LivePolarsCommit,
        *,
        expected: Optional[_LivePolarsCommit],
    ) -> LivePolarsHistorySnapshot:
        with self._condition:
            if self._state is PolarsHistoryState.CLOSED:
                raise PolarsHistoryClosedError(
                    "history manifest is closed"
                )
            if self._state is PolarsHistoryState.FAILED:
                raise PolarsHistoryRefreshError(
                    "history manifest has failed"
                ) from self._last_error
            if self._commit is not expected:
                raise PolarsHistoryRefreshError(
                    "manifest changed while a shadow generation was built"
                )
            self._commit = commit
            self._history_coverage = commit.history_coverage
            self._last_error = None
            self._state = PolarsHistoryState.READY
            self._condition.notify_all()
            return self._snapshot_of(commit)

    def publish_full(
        self,
        chunks: Sequence[Any],
        *,
        generation: int,
        continuity_token: Any,
        expected_total_rows: int,
        metadata: Any = None,
        source: str = "history",
        dataset_identity: Optional[PolarsHistoryDatasetIdentity] = None,
    ) -> LivePolarsHistorySnapshot:
        """Publish the first complete generation after external verification."""

        with self._condition:
            if self._commit is not None:
                raise PolarsHistoryRefreshError(
                    "publish_full requires an empty manifest"
                )
            if self._state is PolarsHistoryState.CLOSED:
                raise PolarsHistoryClosedError(
                    "history manifest is closed"
                )
            if self._state is PolarsHistoryState.FAILED:
                raise PolarsHistoryRefreshError(
                    "history manifest has failed"
                ) from self._last_error
        commit = self._build_commit(
            base=None,
            chunks=chunks,
            generation=generation,
            continuity_token=continuity_token,
            metadata=metadata,
            source=source,
            expected_total_rows=expected_total_rows,
            dataset_identity=dataset_identity,
        )
        return self._install(commit, expected=None)

    def publish_delta(
        self,
        chunks: Sequence[Any],
        *,
        expected_base_token: Any,
        generation: int,
        continuity_token: Any,
        expected_total_rows: int,
        metadata: Any = None,
        source: Optional[str] = None,
    ) -> LivePolarsHistorySnapshot:
        """Append one externally verified delta to the exact observed base."""

        with self._condition:
            if self._state is PolarsHistoryState.CLOSED:
                raise PolarsHistoryClosedError(
                    "history manifest is closed"
                )
            if self._state is PolarsHistoryState.FAILED:
                raise PolarsHistoryRefreshError(
                    "history manifest has failed"
                ) from self._last_error
            base = self._commit
            if base is None:
                raise PolarsHistoryNotReadyError(
                    "full history has not been published"
                )
            if not self._same_token(
                base.continuity_token, expected_base_token
            ):
                raise PolarsHistoryRefreshError(
                    "delta base token does not match the committed manifest"
                )
            if self._same_token(
                base.continuity_token, continuity_token
            ):
                pl = _require_polars()
                for chunk in chunks:
                    if not isinstance(chunk, pl.DataFrame):
                        raise TypeError(
                            "every manifest chunk must be a DataFrame"
                        )
                    if chunk.height:
                        raise PolarsHistoryRefreshError(
                            "unchanged continuity token cannot append rows"
                        )
        commit = self._build_commit(
            base=base,
            chunks=chunks,
            generation=generation,
            continuity_token=continuity_token,
            metadata=metadata,
            source=base.source if source is None else source,
            expected_total_rows=expected_total_rows,
        )
        return self._install(commit, expected=base)

    def promote(
        self,
        candidate: LivePolarsHistorySnapshot,
        *,
        expected_base_token: Any,
        continuity_check,
        source: str,
        allow_session_replacement: bool = False,
    ) -> LivePolarsHistorySnapshot:
        """Atomically replace the manifest after an explicit continuity proof.

        Promotion is intentionally impossible without ``continuity_check``.
        The callback receives stable current and candidate snapshots and must
        return the literal ``True``.  A false result or exception fail-closes
        this store while retaining the last-good generation for an explicit
        stale read.
        """

        if not isinstance(candidate, LivePolarsHistorySnapshot):
            raise TypeError("candidate must be LivePolarsHistorySnapshot")
        if not callable(continuity_check):
            raise TypeError("continuity_check must be callable")
        current = self.snapshot()
        if not self._same_token(
            current.continuity_token, expected_base_token
        ):
            raise PolarsHistoryRefreshError(
                "promotion base token does not match the committed manifest"
            )
        original = current.history_coverage
        target = candidate.history_coverage
        coverage_allowed = original == target or (
            allow_session_replacement
            and original.trade_date == target.trade_date
            and original.process_start_partial
            and target.coverage_from_open
        )
        if not coverage_allowed:
            raise PolarsHistoryCoverageError(
                "promotion coverage/identity change was not authorized"
            )
        if (
            allow_session_replacement
            and (
                current.dataset_identity is None
                or candidate.dataset_identity is None
            )
        ):
            raise PolarsHistoryCoverageError(
                "session replacement requires stable dataset identities"
            )
        if (
            allow_session_replacement
            and original.identity != target.identity
            and (
                current.dataset_identity.catalog_digest is None
                or candidate.dataset_identity.catalog_digest is None
            )
        ):
            raise PolarsHistoryCoverageError(
                "cross-session replacement requires catalog identity"
            )
        if current.dataset_identity != candidate.dataset_identity:
            raise PolarsHistoryCoverageError(
                "promotion candidate has a different dataset identity"
            )
        try:
            verified = continuity_check(current, candidate)
            if verified is not True:
                raise PolarsHistoryCoverageError(
                    "promotion continuity check did not return True"
                )
            if tuple(candidate._frame.schema.items()) != tuple(
                self._schema.items()
            ):
                raise PolarsSchemaError(
                    "promotion candidate has a different schema"
                )
            with self._condition:
                base = self._commit
                if (
                    base is None
                    or not self._same_token(
                        base.continuity_token, expected_base_token
                    )
                ):
                    raise PolarsHistoryRefreshError(
                        "manifest changed during promotion verification"
                    )
            commit = self._build_commit(
                base=None,
                chunks=(candidate._frame,),
                generation=candidate.generation,
                continuity_token=candidate.continuity_token,
                metadata=candidate.metadata,
                source=source,
                expected_total_rows=candidate.row_count,
                history_coverage=target,
                dataset_identity=candidate.dataset_identity,
            )
            commit = _LivePolarsCommit(
                frame=commit.frame,
                chunks=commit.chunks,
                history_coverage=commit.history_coverage,
                generation=commit.generation,
                continuity_token=commit.continuity_token,
                metadata=commit.metadata,
                source=commit.source,
                version=base.version + 1,
                committed_monotonic_ns=commit.committed_monotonic_ns,
                dataset_identity=commit.dataset_identity,
            )
            return self._install(commit, expected=base)
        except BaseException as error:
            self.fail(error)
            raise

    @staticmethod
    def _snapshot_of(commit: _LivePolarsCommit) -> LivePolarsHistorySnapshot:
        return LivePolarsHistorySnapshot(
            _frame=commit.frame,
            _chunks=commit.chunks,
            history_coverage=commit.history_coverage,
            generation=commit.generation,
            continuity_token=commit.continuity_token,
            metadata=commit.metadata,
            source=commit.source,
            version=commit.version,
            committed_monotonic_ns=commit.committed_monotonic_ns,
            dataset_identity=commit.dataset_identity,
        )

    def snapshot(
        self, *, allow_stale: bool = False
    ) -> LivePolarsHistorySnapshot:
        if not isinstance(allow_stale, bool):
            raise TypeError("allow_stale must be bool")
        with self._condition:
            if self._state is PolarsHistoryState.CLOSED:
                raise PolarsHistoryClosedError(
                    "history manifest is closed"
                )
            commit = self._commit
            if commit is None:
                raise PolarsHistoryNotReadyError(
                    "history manifest has no committed generation"
                )
            if (
                self._state is PolarsHistoryState.FAILED
                and not allow_stale
            ):
                raise PolarsHistoryRefreshError(
                    "history manifest failed; last-good data is stale"
                ) from self._last_error
            return self._snapshot_of(commit)

    def wait_ready(self, timeout: Optional[float] = None) -> None:
        deadline = (
            None
            if timeout is None
            else time.monotonic()
            + _positive_float_or_none(timeout, "timeout")
        )
        with self._condition:
            while self._commit is None:
                if self._state is PolarsHistoryState.CLOSED:
                    raise PolarsHistoryClosedError(
                        "history manifest is closed"
                    )
                if self._state is PolarsHistoryState.FAILED:
                    raise PolarsHistoryRefreshError(
                        "initial manifest publication failed"
                    ) from self._last_error
                remaining = (
                    None
                    if deadline is None
                    else max(0.0, deadline - time.monotonic())
                )
                if remaining == 0.0:
                    raise TimeoutError(
                        "timed out waiting for history manifest"
                    )
                self._condition.wait(timeout=remaining)

    def fail(self, error: BaseException) -> None:
        if not isinstance(error, BaseException):
            raise TypeError("error must be an exception")
        with self._condition:
            if self._state is not PolarsHistoryState.CLOSED:
                self._last_error = error
                self._state = PolarsHistoryState.FAILED
                self._condition.notify_all()

    def mark_refreshing(self) -> None:
        """Expose refresh activity without changing the committed pointer."""

        with self._condition:
            if self._state is PolarsHistoryState.CLOSED:
                raise PolarsHistoryClosedError(
                    "history manifest is closed"
                )
            if self._commit is None:
                raise PolarsHistoryNotReadyError(
                    "history manifest has no committed generation"
                )
            if self._state is PolarsHistoryState.FAILED:
                raise PolarsHistoryRefreshError(
                    "history manifest has failed"
                ) from self._last_error
            self._state = PolarsHistoryState.REFRESHING
            self._condition.notify_all()

    def finish_refresh(self) -> None:
        """Finish a verified no-op refresh without creating a new version."""

        with self._condition:
            if self._state is PolarsHistoryState.CLOSED:
                raise PolarsHistoryClosedError(
                    "history manifest is closed"
                )
            if self._commit is None:
                raise PolarsHistoryNotReadyError(
                    "history manifest has no committed generation"
                )
            if self._state is PolarsHistoryState.FAILED:
                raise PolarsHistoryRefreshError(
                    "history manifest has failed"
                ) from self._last_error
            self._state = PolarsHistoryState.READY
            self._condition.notify_all()

    def close(self) -> None:
        with self._condition:
            self._state = PolarsHistoryState.CLOSED
            self._condition.notify_all()

    def spill(
        self,
        path,
        *,
        expected_token: Any = None,
        compression: str = "zstd",
    ) -> str:
        """Atomically replace a Parquet path from one pinned snapshot.

        This flushes through Polars and uses ``os.replace`` but does not call
        ``fsync`` on the file or parent directory, so it is an atomic spill,
        not a crash-durability guarantee.
        """

        snapshot = self.snapshot()
        if expected_token is not None and not self._same_token(
            snapshot.continuity_token, expected_token
        ):
            raise PolarsHistoryRefreshError(
                "spill snapshot token does not match expected_token"
            )
        target = os.path.abspath(os.fspath(path))
        directory = os.path.dirname(target)
        if not os.path.isdir(directory):
            raise FileNotFoundError(
                f"spill directory does not exist: {directory}"
            )
        descriptor, temporary = tempfile.mkstemp(
            prefix=".l2flow-polars-", suffix=".parquet", dir=directory
        )
        os.close(descriptor)
        try:
            snapshot._frame.write_parquet(
                temporary, compression=compression
            )
            os.replace(temporary, target)
        except BaseException:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass
            raise
        return target


class PolarsInstrumentTickHistory:
    """Background full+delta cache for one instrument's raw tick history.

    A refresh builds shadow Polars chunks and publishes them only after the
    reader exposes its independently verified terminal checkpoint.  Calls to
    :meth:`latest_dataframe` therefore observe either the complete previous
    generation or the complete next generation, never an in-progress mix.
    """

    def __init__(
        self,
        reader,
        instrument,
        *,
        columns: Sequence[str] = POLARS_RAW_EVENT_DEFAULT_COLUMNS,
        coverage_requirement: str = "from_open",
        history_coverage: HistoryCoverageInfo,
        startup_prefix_recovered: bool = False,
        refresh_interval: Optional[float] = 1.0,
        batch_records: Optional[int] = None,
        maximum_rows: Optional[int] = None,
        maximum_chunks: Optional[int] = None,
        compact_after_chunks: Optional[int] = 256,
        autostart: bool = True,
    ) -> None:
        _require_polars()
        self._columns = _validate_column_names(
            columns, available=RESULT_COLUMN_BY_NAME
        )
        if coverage_requirement not in (
            "from_open",
            "allow_process_start_partial",
        ):
            raise ValueError("unsupported coverage_requirement")
        if not isinstance(startup_prefix_recovered, bool):
            raise TypeError("startup_prefix_recovered must be bool")
        if not isinstance(history_coverage, HistoryCoverageInfo):
            raise TypeError("history_coverage must be HistoryCoverageInfo")
        if not history_coverage.available:
            raise PolarsHistoryCoverageError(
                "history coverage is unavailable for this session"
            )
        if batch_records is not None:
            batch_records = _positive_int_or_none(
                batch_records, "batch_records"
            )
        self._reader = reader
        self._instrument = instrument
        self._coverage_requirement = coverage_requirement
        self._history_coverage = history_coverage
        self._startup_prefix_recovered = startup_prefix_recovered
        self._refresh_interval = _positive_float_or_none(
            refresh_interval, "refresh_interval"
        )
        self._batch_records = batch_records
        self._maximum_rows = _positive_int_or_none(
            maximum_rows, "maximum_rows"
        )
        self._maximum_chunks = _positive_int_or_none(
            maximum_chunks, "maximum_chunks"
        )
        self._compact_after_chunks = _positive_int_or_none(
            compact_after_chunks, "compact_after_chunks"
        )
        self._condition = threading.Condition(threading.RLock())
        self._stop = False
        self._state = PolarsHistoryState.STARTING
        self._committed: Optional[_CommittedHistory] = None
        self._last_error: Optional[BaseException] = None
        self._refresh_requested = 0
        self._refresh_completed = 0
        self._thread: Optional[threading.Thread] = None
        if autostart:
            self.start()

    @property
    def state(self) -> PolarsHistoryState:
        with self._condition:
            return self._state

    @property
    def last_error(self) -> Optional[BaseException]:
        with self._condition:
            return self._last_error

    @property
    def columns(self) -> tuple[str, ...]:
        return self._columns

    @property
    def committed_generation(self) -> Optional[int]:
        with self._condition:
            committed = self._committed
            return (
                None
                if committed is None
                else committed.checkpoint.generation
            )

    @property
    def dataset_identity(self) -> PolarsHistoryDatasetIdentity:
        with self._condition:
            committed = self._committed
            if committed is None:
                raise PolarsHistoryNotReadyError(
                    "history dataset identity is not ready"
                )
            return _raw_history_dataset_identity(
                committed.checkpoint, self._columns
            )

    def start(self) -> None:
        with self._condition:
            if self._state is PolarsHistoryState.CLOSED:
                raise PolarsHistoryClosedError(
                    "history handle is closed"
                )
            if self._thread is not None:
                return
            thread = threading.Thread(
                target=self._run,
                name="l2flow-polars-history",
                daemon=True,
            )
            self._thread = thread
            thread.start()

    def _read_cursor(self, cursor, base: Optional[_CommittedHistory]):
        chunks: list[Any] = []
        emitted = 0
        with cursor:
            for batch in cursor.batches():
                frame = raw_event_batch_frame(
                    batch, columns=self._columns
                )
                chunks.append(frame)
                emitted += frame.height
            checkpoint = cursor.verified_checkpoint

        if base is None:
            expected = checkpoint.instrument_tick_record_count
            base_chunks: tuple[Any, ...] = ()
            next_version = 1
        else:
            checkpoint.ensure_successor_of(base.checkpoint)
            expected = (
                checkpoint.instrument_tick_record_count
                - base.checkpoint.instrument_tick_record_count
            )
            base_chunks = base.chunks
            next_version = base.version + (
                checkpoint != base.checkpoint
            )
        if emitted != expected:
            raise PolarsSchemaError(
                "Polars rows do not reconcile with the verified checkpoint"
            )
        if (
            not checkpoint.record_coverage_complete
            or not checkpoint.tick_record_coverage_complete
        ):
            raise PolarsHistoryCoverageError(
                "verified checkpoint lacks complete tick record coverage"
            )
        coverage = self._history_coverage
        if (
            coverage.run_id != checkpoint.run_id
            or coverage.session_epoch != checkpoint.session_epoch
            or coverage.trade_date != checkpoint.trade_date
            or coverage.coverage_from_open
            != checkpoint.coverage_from_open
        ):
            raise PolarsHistoryCoverageError(
                "history coverage and checkpoint identity disagree"
            )
        if (
            not coverage.coverage_from_open
            and not coverage.process_start_partial
        ):
            raise PolarsHistoryCoverageError(
                "history coverage has no readable temporal origin"
            )
        if (
            self._coverage_requirement == "from_open"
            and not coverage.coverage_from_open
        ):
            raise PolarsHistoryCoverageError(
                "history is process-start partial, not from-open"
            )

        if base is not None and checkpoint == base.checkpoint:
            if chunks:
                raise PolarsSchemaError(
                    "unchanged checkpoint returned nonempty delta"
                )
            return base

        manifest = base_chunks + tuple(chunks)
        pl = _require_polars()
        if manifest:
            frame = pl.concat(manifest, how="vertical", rechunk=False)
        else:
            frame = empty_raw_event_frame(self._columns)
        if frame.height != checkpoint.instrument_tick_record_count:
            raise PolarsSchemaError(
                "full Polars row count disagrees with checkpoint"
            )
        if (
            self._maximum_rows is not None
            and frame.height > self._maximum_rows
        ):
            raise PolarsHistoryRefreshError(
                "complete history exceeds maximum_rows; no rows were evicted"
            )
        if (
            self._compact_after_chunks is not None
            and len(manifest) > self._compact_after_chunks
        ):
            compacted = frame.rechunk()
            manifest = (compacted,)
            frame = compacted
        if (
            self._maximum_chunks is not None
            and len(manifest) > self._maximum_chunks
        ):
            raise PolarsHistoryRefreshError(
                "complete history exceeds maximum_chunks"
            )
        return _CommittedHistory(
            frame=frame,
            chunks=manifest,
            checkpoint=checkpoint,
            history_coverage=coverage,
            version=next_version,
            committed_ns=time.monotonic_ns(),
        )

    def _open_full(self):
        kwargs = {}
        if self._batch_records is not None:
            kwargs["batch_records"] = self._batch_records
        return self._reader.read_all(self._instrument, **kwargs)

    def _open_update(self, checkpoint):
        kwargs = {}
        if self._batch_records is not None:
            kwargs["batch_records"] = self._batch_records
        return self._reader.read_updates(
            self._instrument, checkpoint, **kwargs
        )

    def _publish(self, committed: _CommittedHistory) -> None:
        with self._condition:
            if self._stop:
                return
            self._committed = committed
            self._last_error = None
            self._state = PolarsHistoryState.READY
            self._condition.notify_all()

    def _fail(self, error: BaseException) -> None:
        with self._condition:
            self._last_error = error
            self._state = PolarsHistoryState.FAILED
            self._condition.notify_all()

    def _run(self) -> None:
        while True:
            with self._condition:
                if self._stop:
                    return
            try:
                initial = self._read_cursor(self._open_full(), None)
            except UnavailableError as error:
                with self._condition:
                    if self._stop:
                        return
                    self._last_error = error
                    self._state = PolarsHistoryState.STARTING
                    delay = self._refresh_interval or 1.0
                    self._condition.wait(timeout=delay)
                continue
            except BaseException as error:
                self._fail(error)
                return
            self._publish(initial)
            with self._condition:
                self._refresh_completed = self._refresh_requested
                self._condition.notify_all()
            # Do not pin the first commit in this long-lived thread frame.
            # The atomic store now owns it; after a later rechunk publication
            # superseded buffers must remain only when a caller pinned them.
            initial = None
            break

        while True:
            with self._condition:
                if self._stop:
                    return
                if self._refresh_requested == self._refresh_completed:
                    self._condition.wait(timeout=self._refresh_interval)
                if self._stop:
                    return
                request_target = self._refresh_requested
                base = self._committed
                if base is None:
                    self._fail(
                        PolarsHistoryRefreshError(
                            "history lost its committed base"
                        )
                    )
                    return
                self._state = PolarsHistoryState.REFRESHING
            try:
                updated = self._read_cursor(
                    self._open_update(base.checkpoint), base
                )
            except BaseException as error:
                self._fail(error)
                return
            self._publish(updated)
            with self._condition:
                self._refresh_completed = max(
                    self._refresh_completed, request_target
                )
                self._condition.notify_all()

    def _deadline(self, timeout: Optional[float]) -> Optional[float]:
        timeout = _positive_float_or_none(timeout, "timeout")
        return None if timeout is None else time.monotonic() + timeout

    @staticmethod
    def _remaining(deadline: Optional[float]) -> Optional[float]:
        if deadline is None:
            return None
        return max(0.0, deadline - time.monotonic())

    def wait_ready(self, timeout: Optional[float] = None) -> None:
        deadline = self._deadline(timeout)
        with self._condition:
            while self._committed is None:
                if self._state is PolarsHistoryState.CLOSED:
                    raise PolarsHistoryClosedError(
                        "history handle is closed"
                    )
                if self._state is PolarsHistoryState.FAILED:
                    raise PolarsHistoryRefreshError(
                        "initial history read failed"
                    ) from self._last_error
                remaining = self._remaining(deadline)
                if remaining == 0.0:
                    raise TimeoutError(
                        "timed out waiting for initial history"
                    )
                self._condition.wait(timeout=remaining)

    def refresh(self, timeout: Optional[float] = None) -> None:
        deadline = self._deadline(timeout)
        with self._condition:
            if self._state is PolarsHistoryState.CLOSED:
                raise PolarsHistoryClosedError(
                    "history handle is closed"
                )
            if self._state is PolarsHistoryState.FAILED:
                raise PolarsHistoryRefreshError(
                    "history updater failed"
                ) from self._last_error
            self._refresh_requested += 1
            request = self._refresh_requested
            self._condition.notify_all()
            while self._refresh_completed < request:
                if self._state is PolarsHistoryState.FAILED:
                    raise PolarsHistoryRefreshError(
                        "history refresh failed"
                    ) from self._last_error
                if self._state is PolarsHistoryState.CLOSED:
                    raise PolarsHistoryClosedError(
                        "history handle is closed"
                    )
                remaining = self._remaining(deadline)
                if remaining == 0.0:
                    raise TimeoutError("timed out refreshing history")
                self._condition.wait(timeout=remaining)

    def latest_snapshot(
        self,
        *,
        consistency: str = "cached",
        timeout: Optional[float] = None,
        allow_stale: bool = False,
    ) -> PolarsInstrumentTickHistorySnapshot:
        if consistency not in ("cached", "latest_published"):
            raise ValueError("unsupported consistency")
        if not isinstance(allow_stale, bool):
            raise TypeError("allow_stale must be bool")
        if consistency == "latest_published":
            self.refresh(timeout)
        with self._condition:
            if self._state is PolarsHistoryState.CLOSED:
                raise PolarsHistoryClosedError(
                    "history handle is closed"
                )
            committed = self._committed
            if committed is None:
                raise PolarsHistoryNotReadyError(
                    "initial history generation is not ready"
                )
            if (
                self._state is PolarsHistoryState.FAILED
                and not allow_stale
            ):
                raise PolarsHistoryRefreshError(
                    "history updater failed; last-good data is stale"
                ) from self._last_error
            checkpoint = committed.checkpoint
            coverage = committed.history_coverage
            if coverage.coverage_from_open:
                origin = (
                    PolarsHistoryCoverageOrigin.FROM_OPEN_RECOVERED
                    if self._startup_prefix_recovered
                    else PolarsHistoryCoverageOrigin.FROM_OPEN_CAPTURE
                )
            else:
                origin = (
                    PolarsHistoryCoverageOrigin.PROCESS_START_PARTIAL
                )
            return PolarsInstrumentTickHistorySnapshot(
                _frame=committed.frame,
                checkpoint=checkpoint,
                history_coverage=coverage,
                cache_version=committed.version,
                coverage_origin=origin,
                cache_committed_monotonic_ns=committed.committed_ns,
                manifest_chunk_count=len(committed.chunks),
                dataset_identity=_raw_history_dataset_identity(
                    checkpoint, self._columns
                ),
            )

    def latest_dataframe(self, **kwargs):
        """Return an owned shallow clone of the latest committed frame."""

        return self.latest_snapshot(**kwargs).dataframe

    def latest_lazyframe(self, **kwargs):
        """Return a LazyFrame pinned to the latest committed frame."""

        return self.latest_snapshot(**kwargs).lazyframe

    def spill(
        self,
        path,
        *,
        expected_checkpoint=None,
        compression: str = "zstd",
    ) -> str:
        """Atomically replace a Parquet copy of one pinned checkpoint.

        ``os.replace`` makes publication atomic to path readers.  As with the
        common manifest spill, this intentionally does not claim file or
        directory ``fsync`` crash durability.
        """

        snapshot = self.latest_snapshot()
        if (
            expected_checkpoint is not None
            and snapshot.checkpoint != expected_checkpoint
        ):
            raise PolarsHistoryRefreshError(
                "spill checkpoint does not match expected_checkpoint"
            )
        target = os.path.abspath(os.fspath(path))
        directory = os.path.dirname(target)
        if not os.path.isdir(directory):
            raise FileNotFoundError(
                f"spill directory does not exist: {directory}"
            )
        descriptor, temporary = tempfile.mkstemp(
            prefix=".l2flow-polars-", suffix=".parquet", dir=directory
        )
        os.close(descriptor)
        try:
            snapshot._frame.write_parquet(
                temporary, compression=compression
            )
            os.replace(temporary, target)
        except BaseException:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass
            raise
        return target

    def close(self) -> None:
        with self._condition:
            if self._state is PolarsHistoryState.CLOSED:
                return
            self._stop = True
            thread = self._thread
            self._condition.notify_all()
        if thread is not None and thread is not threading.current_thread():
            thread.join()
        try:
            self._reader.close()
        finally:
            with self._condition:
                self._state = PolarsHistoryState.CLOSED
                self._condition.notify_all()

    def __enter__(self) -> "PolarsInstrumentTickHistory":
        self.start()
        return self

    def __exit__(self, _type, _value, _traceback) -> None:
        self.close()


def _coverage_requirement(
    coverage: HistoryCoverageInfo, requirement: str
) -> None:
    if requirement not in (
        "from_open",
        "allow_process_start_partial",
    ):
        raise ValueError("unsupported coverage_requirement")
    if not isinstance(coverage, HistoryCoverageInfo):
        raise TypeError("history coverage must be HistoryCoverageInfo")
    if not coverage.available:
        raise PolarsHistoryCoverageError(
            "history coverage is unavailable for this session"
        )
    if requirement == "from_open" and not coverage.coverage_from_open:
        raise PolarsHistoryCoverageError(
            "history is process-start partial, not from-open"
        )


def _validate_raw_checkpoint_coverage(
    coverage: HistoryCoverageInfo,
    checkpoint,
    requirement: str,
) -> None:
    if (
        coverage.run_id != checkpoint.run_id
        or coverage.session_epoch != checkpoint.session_epoch
        or coverage.trade_date != checkpoint.trade_date
        or coverage.coverage_from_open != checkpoint.coverage_from_open
    ):
        raise PolarsHistoryCoverageError(
            "history coverage and checkpoint identity disagree"
        )
    if (
        not checkpoint.record_coverage_complete
        or not checkpoint.tick_record_coverage_complete
    ):
        raise PolarsHistoryCoverageError(
            "verified checkpoint lacks complete tick-record coverage"
        )
    _coverage_requirement(coverage, requirement)


@dataclass(frozen=True, slots=True)
class PolarsInstrumentDerivedEventHistorySnapshot:
    """One EOF-verified FAST-derived event generation."""

    _manifest: LivePolarsHistorySnapshot
    checkpoint: Any

    @property
    def dataframe(self):
        return self._manifest.dataframe

    @property
    def lazyframe(self):
        return self._manifest.lazyframe

    @property
    def history_coverage(self) -> HistoryCoverageInfo:
        return self._manifest.history_coverage

    @property
    def generation(self) -> int:
        return self._manifest.generation

    @property
    def row_count(self) -> int:
        return self._manifest.row_count

    @property
    def cache_version(self) -> int:
        return self._manifest.version

    @property
    def manifest_chunk_count(self) -> int:
        return self._manifest.manifest_chunk_count

    @property
    def cache_committed_monotonic_ns(self) -> int:
        return self._manifest.committed_monotonic_ns

    @property
    def dataset_identity(self) -> PolarsHistoryDatasetIdentity:
        identity = self._manifest.dataset_identity
        if identity is None:
            raise PolarsHistoryCoverageError(
                "FAST-derived history lacks a dataset identity"
            )
        return identity


class PolarsInstrumentDerivedEventHistory:
    """Background FAST-derived full+updates cache for one instrument.

    Every native cursor is consumed through explicit EOF before its chunks are
    installed.  The native reader retains its order-state machine across
    updates; this wrapper never reconstructs order state from Polars rows.
    """

    def __init__(
        self,
        reader,
        *,
        coverage_requirement: str = "from_open",
        refresh_interval: Optional[float] = 1.0,
        maximum_rows: Optional[int] = None,
        maximum_chunks: Optional[int] = None,
        compact_after_chunks: Optional[int] = 256,
        autostart: bool = True,
    ) -> None:
        _require_polars()
        coverage = getattr(reader, "history_coverage", None)
        _coverage_requirement(coverage, coverage_requirement)
        self._reader = reader
        self._coverage_requirement = coverage_requirement
        self._refresh_interval = _positive_float_or_none(
            refresh_interval, "refresh_interval"
        )
        self._manifest = LivePolarsHistory(
            derived_event_schema(),
            coverage,
            maximum_rows=maximum_rows,
            maximum_chunks=maximum_chunks,
            compact_after_chunks=compact_after_chunks,
        )
        self._condition = threading.Condition(threading.RLock())
        self._stop = False
        self._refresh_requested = 0
        self._refresh_completed = 0
        self._thread: Optional[threading.Thread] = None
        if autostart:
            self.start()

    @property
    def state(self) -> PolarsHistoryState:
        return self._manifest.state

    @property
    def last_error(self) -> Optional[BaseException]:
        return self._manifest.last_error

    @property
    def history_coverage(self) -> HistoryCoverageInfo:
        return self._manifest.history_coverage

    @property
    def dataset_identity(self) -> PolarsHistoryDatasetIdentity:
        identity = self._manifest.snapshot().dataset_identity
        if identity is None:
            raise PolarsHistoryCoverageError(
                "FAST-derived history lacks a dataset identity"
            )
        return identity

    def start(self) -> None:
        with self._condition:
            if self._manifest.state is PolarsHistoryState.CLOSED:
                raise PolarsHistoryClosedError(
                    "derived history is closed"
                )
            if self._thread is not None:
                return
            thread = threading.Thread(
                target=self._run,
                name="l2flow-polars-derived-history",
                daemon=True,
            )
            self._thread = thread
            thread.start()

    @staticmethod
    def _validate_event_sequence(frame, begin: int) -> int:
        count = frame.height
        if not count:
            return begin
        sequence = frame["derived_event_sequence"]
        expected_end = begin + count
        if (
            sequence.item(0) != begin
            or sequence.item(-1) != expected_end - 1
        ):
            raise PolarsSchemaError(
                "derived event batch is not at the expected dense boundary"
            )
        if count > 1:
            pl = _require_polars()
            dense = frame.select(
                pl.col("derived_event_sequence")
                .diff()
                .drop_nulls()
                .eq(1)
                .all()
            ).item()
            if dense is not True:
                raise PolarsSchemaError(
                    "derived event sequence is not dense and increasing"
                )
        return expected_end

    def _read_cursor(self, cursor, base_checkpoint=None):
        chunks = []
        emitted = 0
        sequence = (
            1
            if base_checkpoint is None
            else base_checkpoint.derived_event_sequence_exclusive
        )
        with cursor:
            for batch in cursor.batches():
                frame = derived_event_batch_frame(batch)
                sequence = self._validate_event_sequence(frame, sequence)
                emitted += frame.height
                chunks.append(frame)
            checkpoint = cursor.verified_checkpoint
        raw = checkpoint.raw_checkpoint
        _validate_raw_checkpoint_coverage(
            self._manifest.history_coverage,
            raw,
            self._coverage_requirement,
        )
        if base_checkpoint is None:
            expected = checkpoint.derived_event_sequence_exclusive - 1
        else:
            raw.ensure_successor_of(base_checkpoint.raw_checkpoint)
            if (
                checkpoint.instrument_id != base_checkpoint.instrument_id
                or checkpoint.market != base_checkpoint.market
                or checkpoint.trade_date != base_checkpoint.trade_date
                or checkpoint.derived_event_sequence_exclusive
                < base_checkpoint.derived_event_sequence_exclusive
                or (base_checkpoint.finalized and not checkpoint.finalized)
            ):
                raise PolarsHistoryRefreshError(
                    "derived checkpoint is not a monotone successor"
                )
            expected = (
                checkpoint.derived_event_sequence_exclusive
                - base_checkpoint.derived_event_sequence_exclusive
            )
        if (
            emitted != expected
            or sequence != checkpoint.derived_event_sequence_exclusive
        ):
            raise PolarsSchemaError(
                "derived rows do not reconcile with the EOF checkpoint"
            )
        return tuple(chunks), checkpoint

    def _publish_initial(self, chunks, checkpoint) -> None:
        self._manifest.publish_full(
            chunks,
            generation=checkpoint.raw_checkpoint.generation,
            continuity_token=checkpoint,
            expected_total_rows=(
                checkpoint.derived_event_sequence_exclusive - 1
            ),
            metadata=checkpoint,
            source="fast_derived_history",
            dataset_identity=_derived_history_dataset_identity(
                checkpoint
            ),
        )

    def _publish_update(self, chunks, base, checkpoint) -> None:
        if checkpoint == base:
            if chunks:
                raise PolarsSchemaError(
                    "unchanged derived checkpoint returned nonempty rows"
                )
            self._manifest.finish_refresh()
            return
        self._manifest.publish_delta(
            chunks,
            expected_base_token=base,
            generation=checkpoint.raw_checkpoint.generation,
            continuity_token=checkpoint,
            expected_total_rows=(
                checkpoint.derived_event_sequence_exclusive - 1
            ),
            metadata=checkpoint,
        )

    def _run(self) -> None:
        while True:
            with self._condition:
                if self._stop:
                    return
            try:
                chunks, checkpoint = self._read_cursor(
                    self._reader.read_all()
                )
                self._publish_initial(chunks, checkpoint)
                # publish_full cloned or rechunked every buffer it needs.
                # Retaining this full-read tuple in the background thread
                # would defeat later manifest compaction until shutdown.
                chunks = ()
            except UnavailableError as error:
                with self._condition:
                    if self._stop:
                        return
                    delay = self._refresh_interval or 1.0
                    self._condition.wait(timeout=delay)
                continue
            except BaseException as error:
                self._manifest.fail(error)
                return
            break

        while True:
            with self._condition:
                while (
                    not self._stop
                    and self._refresh_interval is None
                    and self._refresh_requested
                    == self._refresh_completed
                ):
                    self._condition.wait()
                if self._stop:
                    return
                if (
                    self._refresh_requested == self._refresh_completed
                    and self._refresh_interval is not None
                ):
                    self._condition.wait(timeout=self._refresh_interval)
                    if self._stop:
                        return
                request_target = self._refresh_requested
            try:
                base = self._manifest.snapshot().continuity_token
                self._manifest.mark_refreshing()
                chunks, checkpoint = self._read_cursor(
                    self._reader.read_updates(base), base
                )
                self._publish_update(chunks, base, checkpoint)
            except BaseException as error:
                self._manifest.fail(error)
                with self._condition:
                    self._condition.notify_all()
                return
            with self._condition:
                self._refresh_completed = max(
                    self._refresh_completed, request_target
                )
                self._condition.notify_all()

    @staticmethod
    def _deadline(timeout: Optional[float]) -> Optional[float]:
        timeout = _positive_float_or_none(timeout, "timeout")
        return None if timeout is None else time.monotonic() + timeout

    @staticmethod
    def _remaining(deadline: Optional[float]) -> Optional[float]:
        if deadline is None:
            return None
        return max(0.0, deadline - time.monotonic())

    def wait_ready(self, timeout: Optional[float] = None) -> None:
        self._manifest.wait_ready(timeout)

    def refresh(self, timeout: Optional[float] = None) -> None:
        deadline = self._deadline(timeout)
        self._manifest.wait_ready(self._remaining(deadline))
        with self._condition:
            if self._stop:
                raise PolarsHistoryClosedError(
                    "derived history is closed"
                )
            if self._manifest.state is PolarsHistoryState.FAILED:
                raise PolarsHistoryRefreshError(
                    "derived history updater failed"
                ) from self._manifest.last_error
            self._refresh_requested += 1
            request = self._refresh_requested
            self._condition.notify_all()
            while self._refresh_completed < request:
                if self._manifest.state is PolarsHistoryState.FAILED:
                    raise PolarsHistoryRefreshError(
                        "derived history refresh failed"
                    ) from self._manifest.last_error
                if self._stop:
                    raise PolarsHistoryClosedError(
                        "derived history is closed"
                    )
                remaining = self._remaining(deadline)
                if remaining == 0.0:
                    raise TimeoutError(
                        "timed out refreshing derived history"
                    )
                self._condition.wait(timeout=remaining)

    def latest_snapshot(
        self,
        *,
        consistency: str = "cached",
        timeout: Optional[float] = None,
        allow_stale: bool = False,
    ) -> PolarsInstrumentDerivedEventHistorySnapshot:
        if consistency not in ("cached", "latest_published"):
            raise ValueError("unsupported consistency")
        if consistency == "latest_published":
            self.refresh(timeout)
        manifest = self._manifest.snapshot(allow_stale=allow_stale)
        return PolarsInstrumentDerivedEventHistorySnapshot(
            _manifest=manifest,
            checkpoint=manifest.metadata,
        )

    def latest_dataframe(self, **kwargs):
        return self.latest_snapshot(**kwargs).dataframe

    def latest_lazyframe(self, **kwargs):
        return self.latest_snapshot(**kwargs).lazyframe

    def spill(self, path, **kwargs) -> str:
        return self._manifest.spill(path, **kwargs)

    def close(self) -> None:
        with self._condition:
            if self._stop:
                return
            self._stop = True
            thread = self._thread
            self._condition.notify_all()
        if thread is not None and thread is not threading.current_thread():
            thread.join()
        try:
            self._reader.close()
        finally:
            self._manifest.close()
            with self._condition:
                self._condition.notify_all()

    def __enter__(self) -> "PolarsInstrumentDerivedEventHistory":
        self.start()
        return self

    def __exit__(self, _type, _value, _traceback) -> None:
        self.close()


@dataclass(frozen=True, slots=True)
class _CertifiedPolarsBoundary:
    next_event_sequence: int
    published_event_sequence: int
    event_generation: int
    correction_epoch: int
    coherent_canonical_apply_frontier: int
    state: int


@dataclass(frozen=True, slots=True)
class _CertifiedPolarsMetadata:
    status: Any
    next_event_sequence: int
    caught_up: bool


@dataclass(frozen=True, slots=True)
class PolarsCertifiedOrderEventHistorySnapshot:
    """One coherent prefix of the CERTIFIED Event history-to-tail stream."""

    _manifest: LivePolarsHistorySnapshot

    @property
    def dataframe(self):
        return self._manifest.dataframe

    @property
    def lazyframe(self):
        return self._manifest.lazyframe

    @property
    def history_coverage(self) -> HistoryCoverageInfo:
        return self._manifest.history_coverage

    @property
    def generation(self) -> int:
        return self._manifest.generation

    @property
    def row_count(self) -> int:
        return self._manifest.row_count

    @property
    def cache_version(self) -> int:
        return self._manifest.version

    @property
    def status(self):
        return self._manifest.metadata.status

    @property
    def next_event_sequence(self) -> int:
        return self._manifest.metadata.next_event_sequence

    @property
    def caught_up(self) -> bool:
        return self._manifest.metadata.caught_up

    @property
    def manifest_chunk_count(self) -> int:
        return self._manifest.manifest_chunk_count

    @property
    def dataset_identity(self) -> PolarsHistoryDatasetIdentity:
        identity = self._manifest.dataset_identity
        if identity is None:
            raise PolarsHistoryCoverageError(
                "CERTIFIED Event history lacks a dataset identity"
            )
        return identity


class PolarsCertifiedOrderEventHistory:
    """Background CERTIFIED Event history-to-tail Polars cache.

    Initial rows remain private until the reader has drained one coherent
    published prefix.  Tail batches are then appended atomically.  A cached
    snapshot may be a valid older prefix; ``latest_published`` performs at
    least one read and waits until that read catches the advertised frontier.
    """

    def __init__(
        self,
        reader,
        *,
        coverage_requirement: str = "from_open",
        poll_interval: Optional[float] = 0.001,
        maximum_rows: Optional[int] = None,
        maximum_chunks: Optional[int] = None,
        compact_after_chunks: Optional[int] = 256,
        autostart: bool = True,
        dataset_identity: Optional[
            PolarsHistoryDatasetIdentity
        ] = None,
    ) -> None:
        _require_polars()
        coverage = getattr(reader, "history_coverage", None)
        try:
            _validate_certified_event_coverage(
                coverage, coverage_requirement
            )
        except (TypeError, ValueError, UnavailableError) as error:
            raise PolarsHistoryCoverageError(str(error)) from error
        if getattr(reader, "next_event_sequence", None) != 1:
            raise PolarsHistoryCoverageError(
                "complete CERTIFIED Polars history must start at sequence 1"
            )
        self._reader = reader
        expected_identity = _certified_order_event_dataset_identity()
        if dataset_identity is None:
            dataset_identity = expected_identity
        elif (
            not isinstance(
                dataset_identity, PolarsHistoryDatasetIdentity
            )
            or dataset_identity.product_kind
            is not expected_identity.product_kind
            or dataset_identity.instrument_id is not None
            or dataset_identity.projection_columns
            != expected_identity.projection_columns
            or dataset_identity.payload_projection
            != expected_identity.payload_projection
            or dataset_identity.polars_schema_version
            != expected_identity.polars_schema_version
        ):
            raise PolarsHistoryCoverageError(
                "CERTIFIED Event dataset identity has the wrong projection"
            )
        self._dataset_identity = dataset_identity
        self._poll_interval = _positive_float_or_none(
            poll_interval, "poll_interval"
        )
        self._manifest = LivePolarsHistory(
            derived_event_schema(certified=True),
            coverage,
            maximum_rows=maximum_rows,
            maximum_chunks=maximum_chunks,
            compact_after_chunks=compact_after_chunks,
        )
        self._condition = threading.Condition(threading.RLock())
        self._stop = False
        self._refresh_requested = 0
        self._refresh_completed = 0
        self._thread: Optional[threading.Thread] = None
        if autostart:
            self.start()

    @property
    def state(self) -> PolarsHistoryState:
        return self._manifest.state

    @property
    def last_error(self) -> Optional[BaseException]:
        return self._manifest.last_error

    @property
    def history_coverage(self) -> HistoryCoverageInfo:
        return self._manifest.history_coverage

    @property
    def dataset_identity(self) -> PolarsHistoryDatasetIdentity:
        return self._dataset_identity

    def start(self) -> None:
        with self._condition:
            if self._manifest.state is PolarsHistoryState.CLOSED:
                raise PolarsHistoryClosedError(
                    "CERTIFIED history is closed"
                )
            if self._thread is not None:
                return
            thread = threading.Thread(
                target=self._run,
                name="l2flow-polars-certified-history",
                daemon=True,
            )
            self._thread = thread
            thread.start()

    @staticmethod
    def _boundary(batch) -> _CertifiedPolarsBoundary:
        status = batch.status
        return _CertifiedPolarsBoundary(
            next_event_sequence=batch.next_event_sequence,
            published_event_sequence=status.event_published_sequence,
            event_generation=status.event_generation,
            correction_epoch=status.correction_epoch,
            coherent_canonical_apply_frontier=(
                status.coherent_canonical_apply_frontier
            ),
            state=int(status.state),
        )

    @staticmethod
    def _caught_up(batch) -> bool:
        return (
            batch.next_event_sequence
            == batch.status.event_published_sequence + 1
        )

    @classmethod
    def _require_publishable_lifecycle(cls, batch) -> None:
        try:
            state = CertifiedOrderEventState(batch.status.state)
        except (TypeError, ValueError) as error:
            raise PolarsSchemaError(
                "CERTIFIED Event batch has an invalid producer state"
            ) from error
        if state in (
            CertifiedOrderEventState.DISABLED,
            CertifiedOrderEventState.FROZEN_CONFLICT,
            CertifiedOrderEventState.FROZEN_RESOURCE,
        ):
            raise PolarsHistoryRefreshError(
                "CERTIFIED Event producer cannot publish History in "
                f"{state.name}"
            )
        # STOPPED is a valid clean end only after every advertised Event row
        # is visible through the coherent Tick/Event frontier.  A nonempty
        # batch may merely be bounded by batch_records, so keep draining it;
        # an empty, non-caught-up STOPPED read can never make future progress.
        if state is CertifiedOrderEventState.STOPPED:
            status = batch.status
            unresolved = any(
                int(getattr(status, name, 0)) != 0
                for name in (
                    "gap_open_channel_count",
                    "catching_up_channel_count",
                    "frozen_channel_count",
                    "resource_exhaustion_count",
                    "conflicting_duplicate_count",
                )
            )
            if unresolved:
                raise PolarsHistoryRefreshError(
                    "CERTIFIED Event producer stopped with an unresolved "
                    "gap, conflict, or resource failure"
                )
            if not cls._caught_up(batch) and len(batch) == 0:
                raise PolarsHistoryRefreshError(
                    "CERTIFIED Event producer stopped before its advertised "
                    "prefix became coherently readable"
                )

    def _read_batch(self):
        begin = self._reader.next_event_sequence
        batch = self._reader.read_batch()
        if batch.history_coverage != self._manifest.history_coverage:
            raise PolarsHistoryCoverageError(
                "CERTIFIED batch coverage changed after attachment"
            )
        if batch.next_event_sequence != begin + len(batch):
            raise PolarsSchemaError(
                "CERTIFIED batch cursor does not reconcile with row count"
            )
        self._require_publishable_lifecycle(batch)
        frame = certified_order_event_batch_frame(batch)
        if frame.height:
            sequence = frame["derived_event_sequence"]
            if (
                sequence.item(0) != begin
                or sequence.item(-1) != begin + frame.height - 1
            ):
                raise PolarsSchemaError(
                    "CERTIFIED Event sequence does not match its dense cursor"
                )
            if frame.height > 1:
                pl = _require_polars()
                dense = frame.select(
                    pl.col("derived_event_sequence")
                    .diff()
                    .drop_nulls()
                    .eq(1)
                    .all()
                ).item()
                if dense is not True:
                    raise PolarsSchemaError(
                        "CERTIFIED Event sequence is not dense"
                    )
        return frame, batch

    def _run(self) -> None:
        initial_chunks = []
        try:
            while True:
                with self._condition:
                    if self._stop:
                        return
                frame, batch = self._read_batch()
                if frame.height:
                    initial_chunks.append(frame)
                if self._caught_up(batch):
                    boundary = self._boundary(batch)
                    self._manifest.publish_full(
                        initial_chunks,
                        generation=boundary.event_generation,
                        continuity_token=boundary,
                        expected_total_rows=(
                            boundary.next_event_sequence - 1
                        ),
                        metadata=_CertifiedPolarsMetadata(
                            status=batch.status,
                            next_event_sequence=(
                                batch.next_event_sequence
                            ),
                            caught_up=True,
                        ),
                        source="certified_event_history_to_tail",
                        dataset_identity=self._dataset_identity,
                    )
                    # The manifest owns the initial prefix (or its rechunked
                    # replacement); release each temporary input batch before
                    # entering the lifetime-long tail loop.
                    initial_chunks.clear()
                    break
        except BaseException as error:
            self._manifest.fail(error)
            return

        drain_immediately = False
        while True:
            with self._condition:
                while (
                    not self._stop
                    and self._poll_interval is None
                    and self._refresh_requested
                    == self._refresh_completed
                ):
                    self._condition.wait()
                if self._stop:
                    return
                if (
                    self._refresh_requested == self._refresh_completed
                    and self._poll_interval is not None
                    and not drain_immediately
                ):
                    self._condition.wait(timeout=self._poll_interval)
                    if self._stop:
                        return
                request_target = self._refresh_requested
            try:
                base = self._manifest.snapshot()
                frame, batch = self._read_batch()
                boundary = self._boundary(batch)
                caught_up = self._caught_up(batch)
                if frame.height or boundary != base.continuity_token:
                    self._manifest.publish_delta(
                        (frame,) if frame.height else (),
                        expected_base_token=base.continuity_token,
                        generation=boundary.event_generation,
                        continuity_token=boundary,
                        expected_total_rows=(
                            boundary.next_event_sequence - 1
                        ),
                        metadata=_CertifiedPolarsMetadata(
                            status=batch.status,
                            next_event_sequence=(
                                batch.next_event_sequence
                            ),
                            caught_up=caught_up,
                        ),
                    )
                if caught_up:
                    with self._condition:
                        self._refresh_completed = max(
                            self._refresh_completed, request_target
                        )
                        self._condition.notify_all()
                drain_immediately = not caught_up
            except BaseException as error:
                self._manifest.fail(error)
                with self._condition:
                    self._condition.notify_all()
                return

    @staticmethod
    def _deadline(timeout: Optional[float]) -> Optional[float]:
        timeout = _positive_float_or_none(timeout, "timeout")
        return None if timeout is None else time.monotonic() + timeout

    @staticmethod
    def _remaining(deadline: Optional[float]) -> Optional[float]:
        if deadline is None:
            return None
        return max(0.0, deadline - time.monotonic())

    def wait_ready(self, timeout: Optional[float] = None) -> None:
        self._manifest.wait_ready(timeout)

    def refresh(self, timeout: Optional[float] = None) -> None:
        deadline = self._deadline(timeout)
        self._manifest.wait_ready(self._remaining(deadline))
        with self._condition:
            if self._stop:
                raise PolarsHistoryClosedError(
                    "CERTIFIED history is closed"
                )
            if self._manifest.state is PolarsHistoryState.FAILED:
                raise PolarsHistoryRefreshError(
                    "CERTIFIED history-to-tail updater failed"
                ) from self._manifest.last_error
            self._refresh_requested += 1
            request = self._refresh_requested
            self._condition.notify_all()
            while self._refresh_completed < request:
                if self._manifest.state is PolarsHistoryState.FAILED:
                    raise PolarsHistoryRefreshError(
                        "CERTIFIED history refresh failed"
                    ) from self._manifest.last_error
                if self._stop:
                    raise PolarsHistoryClosedError(
                        "CERTIFIED history is closed"
                    )
                remaining = self._remaining(deadline)
                if remaining == 0.0:
                    raise TimeoutError(
                        "timed out draining CERTIFIED published prefix"
                    )
                self._condition.wait(timeout=remaining)

    def latest_snapshot(
        self,
        *,
        consistency: str = "cached",
        timeout: Optional[float] = None,
        allow_stale: bool = False,
    ) -> PolarsCertifiedOrderEventHistorySnapshot:
        if consistency not in ("cached", "latest_published"):
            raise ValueError("unsupported consistency")
        if consistency == "latest_published":
            self.refresh(timeout)
        manifest = self._manifest.snapshot(allow_stale=allow_stale)
        return PolarsCertifiedOrderEventHistorySnapshot(manifest)

    def latest_dataframe(self, **kwargs):
        return self.latest_snapshot(**kwargs).dataframe

    def latest_lazyframe(self, **kwargs):
        return self.latest_snapshot(**kwargs).lazyframe

    def spill(self, path, **kwargs) -> str:
        return self._manifest.spill(path, **kwargs)

    def close(self) -> None:
        with self._condition:
            if self._stop:
                return
            self._stop = True
            thread = self._thread
            self._condition.notify_all()
        if thread is not None and thread is not threading.current_thread():
            thread.join()
        try:
            self._reader.close()
        finally:
            self._manifest.close()
            with self._condition:
                self._condition.notify_all()

    def __enter__(self) -> "PolarsCertifiedOrderEventHistory":
        self.start()
        return self

    def __exit__(self, _type, _value, _traceback) -> None:
        self.close()

class PolarsClient:
    """Optional Polars facade over one existing :class:`L2FlowClient`."""

    def __init__(self, client) -> None:
        _require_polars()
        self._client = client
        self._coverage_lock = threading.Lock()
        self._kline_coverage = None

    @property
    def client(self):
        return self._client

    def warmup(self) -> None:
        """Import Polars and build one empty fixed-schema frame."""

        empty_raw_event_frame()

    def latest_snapshots(
        self,
        instrument_ids: Sequence[int],
        *,
        include_wire_payload: bool = False,
    ):
        values = self._client.latest_snapshots(instrument_ids)
        return latest_snapshots_frame(
            values, include_wire_payload=include_wire_payload
        )

    def latest_snapshot(self, instrument_id: int, **kwargs):
        return self.latest_snapshots((instrument_id,), **kwargs)

    def latest_snapshot_lazy(self, instrument_id: int, **kwargs):
        return self.latest_snapshot(instrument_id, **kwargs).lazy()

    def latest_snapshots_lazy(self, *args, **kwargs):
        return self.latest_snapshots(*args, **kwargs).lazy()

    def latest_ticks(
        self,
        instrument_ids: Sequence[int],
        *,
        include_wire_payload: bool = False,
    ):
        values = self._client.latest_ticks(instrument_ids)
        return latest_ticks_frame(
            values, include_wire_payload=include_wire_payload
        )

    def latest_tick(self, instrument_id: int, **kwargs):
        return self.latest_ticks((instrument_id,), **kwargs)

    def latest_tick_lazy(self, instrument_id: int, **kwargs):
        return self.latest_tick(instrument_id, **kwargs).lazy()

    def latest_ticks_lazy(self, *args, **kwargs):
        return self.latest_ticks(*args, **kwargs).lazy()

    def _session_kline_coverage(self):
        with self._coverage_lock:
            if self._kline_coverage is None:
                self._kline_coverage = self._client.kline_coverage()
            return self._kline_coverage

    def kline_coverage(self):
        pl = _require_polars()
        value = self._session_kline_coverage()
        return pl.DataFrame(
            {
                "session_epoch": [value.session_epoch],
                "coverage_start_unix_ns": [
                    value.coverage_start_unix_ns
                ],
                "coverage_kind": [int(value.coverage_kind)],
            },
            schema={
                "session_epoch": pl.UInt64,
                "coverage_start_unix_ns": pl.UInt64,
                "coverage_kind": pl.UInt8,
            },
            strict=True,
        )

    def kline_coverage_lazy(self):
        return self.kline_coverage().lazy()

    def latest_klines(
        self,
        instrument_ids: Sequence[int],
        window_ids: Sequence[int],
        *,
        include_wire_payload: bool = False,
        include_session_coverage: bool = True,
    ):
        values = self._client.latest_klines(
            instrument_ids, window_ids
        )
        coverage = (
            self._session_kline_coverage()
            if include_session_coverage
            else None
        )
        return latest_klines_frame(
            values,
            session_coverage=coverage,
            include_wire_payload=include_wire_payload,
            include_session_coverage=include_session_coverage,
        )

    def latest_kline(
        self, instrument_id: int, window_id: int, **kwargs
    ):
        return self.latest_klines(
            (instrument_id,), (window_id,), **kwargs
        )

    def latest_kline_lazy(
        self, instrument_id: int, window_id: int, **kwargs
    ):
        return self.latest_kline(
            instrument_id, window_id, **kwargs
        ).lazy()

    def latest_klines_lazy(self, *args, **kwargs):
        return self.latest_klines(*args, **kwargs).lazy()

    def raw_event_batch(self, batch, *, columns=None):
        return raw_event_batch_frame(batch, columns=columns)

    def raw_event_batch_lazy(self, batch, *, columns=None):
        return raw_event_batch_lazyframe(batch, columns=columns)

    def derived_event_batch(self, batch):
        return derived_event_batch_frame(batch)

    def derived_event_batch_lazy(self, batch):
        return derived_event_batch_frame(batch).lazy()

    def live_order_event_batch(self, batch):
        return live_order_event_batch_frame(batch)

    def live_order_event_batch_lazy(self, batch):
        return live_order_event_batch_frame(batch).lazy()

    def certified_order_event_batch(self, batch):
        return certified_order_event_batch_frame(batch)

    def certified_order_event_batch_lazy(self, batch):
        return certified_order_event_batch_frame(batch).lazy()

    def certified_tick_batch(
        self,
        batch,
        *,
        include_wire_payload: bool = False,
    ):
        return certified_tick_batch_frame(
            batch,
            include_wire_payload=include_wire_payload,
        )

    def certified_tick_batch_lazy(
        self,
        batch,
        *,
        include_wire_payload: bool = False,
    ):
        return certified_tick_batch_lazyframe(
            batch,
            include_wire_payload=include_wire_payload,
        )

    def coverage_frame(self, coverage: HistoryCoverageInfo):
        return history_coverage_frame(coverage)

    def history_coverage(self):
        return history_coverage_frame(self._client.history_coverage())

    def history_coverage_lazy(self):
        return self.history_coverage().lazy()

    def open_instrument_tick_history(
        self,
        instrument,
        *,
        columns: Sequence[str] = POLARS_RAW_EVENT_DEFAULT_COLUMNS,
        coverage_requirement: str = "from_open",
        refresh_interval: Optional[float] = 1.0,
        live_tail: bool = False,
        tail_poll_interval: float = 0.001,
        batch_records: int = 4096,
        ring_slots: int = 4,
        maximum_rows: Optional[int] = None,
        maximum_chunks: Optional[int] = None,
        compact_after_chunks: Optional[int] = 256,
    ):
        """Open immutable FAST raw history, optionally with reconciled tail.

        ``live_tail=False`` preserves the generation-polled full+delta path.
        ``live_tail=True`` additionally consumes the bounded FAST ring and
        repairs any overrun from immutable History before publishing another
        atomic snapshot.
        """

        if not isinstance(live_tail, bool):
            raise TypeError("live_tail must be bool")
        coverage = self._client.history_coverage()
        _coverage_requirement(coverage, coverage_requirement)
        reader = self._client.open_instrument_raw_event_history(
            raw_event_columns=columns,
            ring_slots=ring_slots,
            batch_capacity=batch_records,
        )
        try:
            coverage = reader.history_coverage
            _coverage_requirement(coverage, coverage_requirement)
            session = self._client.session_info()
            if (
                session.identity != coverage.identity
                or session.trade_date != coverage.trade_date
            ):
                raise PolarsHistoryCoverageError(
                    "raw reader and FAST session/day identities disagree"
                )
            if live_tail:
                from .polars_fast_tick_history import (
                    PolarsFastTickHistory,
                )
                # The polling ring reader receives a separate mapping and
                # lock domain.  It therefore cannot serialize the caller's
                # latest FAST/CERTIFIED point reads on this client merely by
                # taking Python's client/native-reader locks.
                tail_client = (
                    self._client._open_independent_read_client()
                )
                try:
                    return PolarsFastTickHistory(
                        tail_client,
                        reader,
                        instrument,
                        history_coverage=coverage,
                        columns=columns,
                        coverage_requirement=coverage_requirement,
                        batch_records=batch_records,
                        tail_poll_interval=tail_poll_interval,
                        durable_refresh_interval=refresh_interval,
                        maximum_rows=maximum_rows,
                        maximum_chunks=maximum_chunks,
                        compact_after_chunks=compact_after_chunks,
                        owns_tail_client=True,
                    )
                except BaseException:
                    tail_client.close()
                    raise
            return PolarsInstrumentTickHistory(
                reader,
                instrument,
                columns=columns,
                coverage_requirement=coverage_requirement,
                history_coverage=coverage,
                startup_prefix_recovered=(
                    session.startup_prefix_recovered
                ),
                refresh_interval=refresh_interval,
                batch_records=batch_records,
                maximum_rows=maximum_rows,
                maximum_chunks=maximum_chunks,
                compact_after_chunks=compact_after_chunks,
            )
        except BaseException:
            reader.close()
            raise

    def open_instrument_derived_event_history(
        self,
        instrument,
        *,
        coverage_requirement: str = "from_open",
        refresh_interval: Optional[float] = 1.0,
        live_tail: bool = False,
        event_control_socket_path=None,
        native_library=None,
        native_library_path=None,
        timeout=_USE_CLIENT_TIMEOUT,
        tail_poll_interval: Optional[float] = 0.001,
        tail_batch_records: int = 4096,
        maximum_order_states: int = 1_000_000,
        page_records: int = 4096,
        maximum_rows: Optional[int] = None,
        maximum_chunks: Optional[int] = None,
        compact_after_chunks: Optional[int] = 256,
    ):
        """Open FAST-derived History, optionally with a live Event tail.

        ``live_tail=False`` preserves the EOF-verified generation-polled
        History path and does not touch the Event control plane.  Explicit
        ``live_tail=True`` starts the reconciler thread and requires
        ``event_control_socket_path``.  Cross-path overlap uses stable
        ``EventUid`` values and raw-tick checkpoint frontiers, never either
        product's dense event sequence.
        """

        if not isinstance(live_tail, bool):
            raise TypeError("live_tail must be bool")
        if live_tail and event_control_socket_path is None:
            raise ValueError(
                "event_control_socket_path is required for live_tail"
            )
        if live_tail and (
            native_library is not None
            and native_library_path is not None
        ):
            raise ValueError(
                "native_library and native_library_path are mutually "
                "exclusive"
            )
        if live_tail and (
            not isinstance(tail_batch_records, int)
            or isinstance(tail_batch_records, bool)
            or tail_batch_records <= 0
            or tail_batch_records > 65_536
        ):
            raise ValueError(
                "tail_batch_records must be in [1, 65536]"
            )
        coverage = self._client.history_coverage()
        _coverage_requirement(coverage, coverage_requirement)
        reader = self._client.open_instrument_derived_event_history(
            instrument,
            maximum_order_states=maximum_order_states,
            page_records=page_records,
        )
        try:
            if live_tail:
                from .polars_fast_derived_event_history import (
                    PolarsFastDerivedEventHistory,
                )

                live_arguments = {
                    "native_library": native_library,
                    "native_library_path": native_library_path,
                    "batch_records": tail_batch_records,
                }
                if timeout is not _USE_CLIENT_TIMEOUT:
                    live_arguments["timeout"] = timeout

                return PolarsFastDerivedEventHistory(
                    self._client,
                    reader,
                    event_control_socket_path,
                    coverage_requirement=coverage_requirement,
                    live_open_kwargs=live_arguments,
                    tail_poll_interval=tail_poll_interval,
                    durable_refresh_interval=refresh_interval,
                    maximum_rows=maximum_rows,
                    maximum_chunks=maximum_chunks,
                    compact_after_chunks=compact_after_chunks,
                    autostart=True,
                )
            return PolarsInstrumentDerivedEventHistory(
                reader,
                coverage_requirement=coverage_requirement,
                refresh_interval=refresh_interval,
                maximum_rows=maximum_rows,
                maximum_chunks=maximum_chunks,
                compact_after_chunks=compact_after_chunks,
            )
        except BaseException:
            reader.close()
            raise

    def open_certified_order_event_history(
        self,
        certified_control_socket_path,
        *,
        coverage_requirement: str = "from_open",
        native_library=None,
        native_library_path=None,
        timeout=_USE_CLIENT_TIMEOUT,
        batch_records: int = 4096,
        poll_interval: Optional[float] = 0.001,
        maximum_rows: Optional[int] = None,
        maximum_chunks: Optional[int] = None,
        compact_after_chunks: Optional[int] = 256,
    ) -> PolarsCertifiedOrderEventHistory:
        """Open a canonical CERTIFIED Event history-to-tail Polars cache."""

        fast_session = self._client.session_info()
        dataset_identity = _certified_order_event_dataset_identity(
            fast_session
        )
        arguments = {
            "native_library": native_library,
            "native_library_path": native_library_path,
            "coverage_requirement": coverage_requirement,
            "start_event_sequence": 1,
            "batch_records": batch_records,
        }
        # ``None`` is meaningful to the core client (no control timeout), but
        # omitting this key preserves the client's configured default.
        if timeout is not _USE_CLIENT_TIMEOUT:
            arguments["timeout"] = timeout
        reader = self._client.open_certified_order_events(
            certified_control_socket_path,
            **arguments,
        )
        try:
            current_session = self._client.session_info()
            reader_coverage = getattr(
                reader, "history_coverage", None
            )
            if (
                current_session.identity != fast_session.identity
                or current_session.trade_date != fast_session.trade_date
                or current_session.catalog_digest
                != fast_session.catalog_digest
                or current_session.catalog_scope
                != fast_session.catalog_scope
                or current_session.catalog_version
                != fast_session.catalog_version
                or not isinstance(
                    reader_coverage, HistoryCoverageInfo
                )
                or reader_coverage.identity
                != fast_session.identity
                or reader_coverage.trade_date
                != fast_session.trade_date
            ):
                raise PolarsHistoryCoverageError(
                    "CERTIFIED Event and FAST catalog sessions disagree"
                )
            return PolarsCertifiedOrderEventHistory(
                reader,
                coverage_requirement=coverage_requirement,
                poll_interval=poll_interval,
                maximum_rows=maximum_rows,
                maximum_chunks=maximum_chunks,
                compact_after_chunks=compact_after_chunks,
                dataset_identity=dataset_identity,
            )
        except BaseException:
            reader.close()
            raise

    def open_certified_tick_history(
        self,
        certified_control_socket_path,
        *,
        native_library=None,
        native_library_path=None,
        timeout=_USE_CLIENT_TIMEOUT,
        batch_records: int = 4096,
        include_wire_payload: bool = False,
        poll_interval: Optional[float] = 0.001,
        maximum_rows: Optional[int] = None,
        maximum_chunks: Optional[int] = None,
        compact_after_chunks: Optional[int] = 256,
    ):
        """Open the dense from-open CERTIFIED Tick Polars history-to-tail.

        This is an opt-in consumer of the independent append-only CERTIFIED
        journal.  It neither reads the bounded FAST ring nor adds work to the
        normal latest FAST/CERTIFIED paths.
        """

        from .polars_certified_tick_history import (
            PolarsCertifiedTickHistory,
        )

        if not isinstance(include_wire_payload, bool):
            raise TypeError("include_wire_payload must be bool")
        fast_session = self._client.session_info()
        dataset_identity = _certified_tick_dataset_identity(
            include_wire_payload=include_wire_payload,
            session=fast_session,
        )
        arguments = {
            "native_library": native_library,
            "native_library_path": native_library_path,
            "start_canonical_apply_sequence": 1,
            "batch_records": batch_records,
        }
        if timeout is not _USE_CLIENT_TIMEOUT:
            arguments["timeout"] = timeout
        reader = self._client.open_certified_tick_history(
            certified_control_socket_path,
            **arguments,
        )
        try:
            current_session = self._client.session_info()
            reader_coverage = getattr(
                reader, "history_coverage", None
            )
            if (
                current_session.identity != fast_session.identity
                or current_session.trade_date != fast_session.trade_date
                or current_session.catalog_digest
                != fast_session.catalog_digest
                or current_session.catalog_scope
                != fast_session.catalog_scope
                or current_session.catalog_version
                != fast_session.catalog_version
                or not isinstance(
                    reader_coverage, HistoryCoverageInfo
                )
                or not reader_coverage.coverage_from_open
                or reader_coverage.identity != fast_session.identity
                or reader_coverage.trade_date != fast_session.trade_date
            ):
                raise PolarsHistoryCoverageError(
                    "CERTIFIED Tick and FAST catalog sessions disagree"
                )
            return PolarsCertifiedTickHistory(
                reader,
                include_wire_payload=include_wire_payload,
                poll_interval=poll_interval,
                maximum_rows=maximum_rows,
                maximum_chunks=maximum_chunks,
                compact_after_chunks=compact_after_chunks,
                dataset_identity=dataset_identity,
            )
        except BaseException:
            reader.close()
            raise

    def auto_promoting_history(
        self,
        active_history,
        promotion_factory,
        *,
        poll_interval: float = 0.05,
        expected_dataset_identity: Optional[
            PolarsHistoryDatasetIdentity
        ] = None,
    ):
        """Wrap a history with an atomic online-recovery replacement.

        The factory is invoked only by the opt-in monitor thread.  It returns
        a fully configured recovered history handle once the promoted control
        plane is available, or ``None`` while it remains hidden.
        """

        from .polars_promotion import AutoPromotingPolarsHistory

        history = AutoPromotingPolarsHistory(
            active_history,
            promotion_factory,
            poll_interval=poll_interval,
            expected_dataset_identity=expected_dataset_identity,
        )
        try:
            history.start()
            return history
        except BaseException:
            history.close()
            raise


def as_polars(client) -> PolarsClient:
    """Create the optional Polars facade without changing the core client."""

    return PolarsClient(client)


def __getattr__(name: str):
    """Lazily expose optional history helpers without import cycles."""

    if name in ("FastTickHistoryToken", "PolarsFastTickHistory"):
        from .polars_fast_tick_history import (
            FastTickHistoryToken,
            PolarsFastTickHistory,
        )

        return {
            "FastTickHistoryToken": FastTickHistoryToken,
            "PolarsFastTickHistory": PolarsFastTickHistory,
        }[name]
    if name in (
        "FastDerivedEventHistoryToken",
        "PolarsFastDerivedEventHistory",
        "PolarsFastDerivedEventHistorySnapshot",
    ):
        from .polars_fast_derived_event_history import (
            FastDerivedEventHistoryToken,
            PolarsFastDerivedEventHistory,
            PolarsFastDerivedEventHistorySnapshot,
        )

        return {
            "FastDerivedEventHistoryToken": (
                FastDerivedEventHistoryToken
            ),
            "PolarsFastDerivedEventHistory": (
                PolarsFastDerivedEventHistory
            ),
            "PolarsFastDerivedEventHistorySnapshot": (
                PolarsFastDerivedEventHistorySnapshot
            ),
        }[name]
    if name == "AutoPromotingPolarsHistory":
        from .polars_promotion import AutoPromotingPolarsHistory

        return AutoPromotingPolarsHistory
    if name in (
        "CertifiedTickHistoryToken",
        "PolarsCertifiedTickHistory",
        "PolarsCertifiedTickHistorySnapshot",
    ):
        from .polars_certified_tick_history import (
            CertifiedTickHistoryToken,
            PolarsCertifiedTickHistory,
            PolarsCertifiedTickHistorySnapshot,
        )

        return {
            "CertifiedTickHistoryToken": CertifiedTickHistoryToken,
            "PolarsCertifiedTickHistory": PolarsCertifiedTickHistory,
            "PolarsCertifiedTickHistorySnapshot": (
                PolarsCertifiedTickHistorySnapshot
            ),
        }[name]
    raise AttributeError(name)


__all__ = [
    "POLARS_RAW_EVENT_DEFAULT_COLUMNS",
    "POLARS_SCHEMA_VERSION",
    "AutoPromotingPolarsHistory",
    "CertifiedTickHistoryToken",
    "FastDerivedEventHistoryToken",
    "FastTickHistoryToken",
    "LivePolarsHistory",
    "LivePolarsHistorySnapshot",
    "PolarsClient",
    "PolarsCertifiedOrderEventHistory",
    "PolarsCertifiedOrderEventHistorySnapshot",
    "PolarsCertifiedTickHistory",
    "PolarsCertifiedTickHistorySnapshot",
    "PolarsHistoryClosedError",
    "PolarsHistoryCoverageError",
    "PolarsHistoryCoverageOrigin",
    "PolarsHistoryDatasetIdentity",
    "PolarsHistoryError",
    "PolarsHistoryNotReadyError",
    "PolarsHistoryRefreshError",
    "PolarsHistoryState",
    "PolarsHistoryProductKind",
    "PolarsInstrumentTickHistory",
    "PolarsInstrumentTickHistorySnapshot",
    "PolarsFastTickHistory",
    "PolarsFastDerivedEventHistory",
    "PolarsFastDerivedEventHistorySnapshot",
    "PolarsInstrumentDerivedEventHistory",
    "PolarsInstrumentDerivedEventHistorySnapshot",
    "PolarsSchemaError",
    "PolarsUnavailableError",
    "as_polars",
    "certified_order_event_batch_frame",
    "certified_tick_batch_frame",
    "certified_tick_batch_lazyframe",
    "certified_tick_schema",
    "derived_event_batch_frame",
    "derived_event_schema",
    "derived_events_frame",
    "empty_raw_event_frame",
    "history_coverage_frame",
    "latest_kline_schema",
    "latest_klines_frame",
    "latest_snapshot_schema",
    "latest_snapshots_frame",
    "latest_tick_schema",
    "latest_ticks_frame",
    "live_order_event_batch_frame",
    "polars_available",
    "raw_event_batch_frame",
    "raw_event_batch_lazyframe",
    "raw_event_schema",
]
