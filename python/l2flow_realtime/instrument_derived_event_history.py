"""Full and rolling derived order/trade/cancel history for one instrument.

Unlike :mod:`instrument_raw_event_history`, this module does not expose Wire
rows.  It calls the native Shanghai/Shenzhen state machines through a fixed C
ABI and returns their source events and order revisions.  One reader is bound
to one instrument and keeps the native order state alive across ``read_all``
and every later ``read_updates`` call.
"""

from __future__ import annotations

import ctypes
import os
from dataclasses import dataclass
from enum import IntEnum
from typing import Iterator, Optional, TYPE_CHECKING, Union

from ._history_worker_protocol import MAX_RESULT_BATCH_RECORDS
from .checkpoint import CHECKPOINT_BYTES, InstrumentTickDeltaCheckpoint
from .models import (
    InstrumentKey,
    InstrumentLookupStatus,
    InstrumentStatus,
    L2FlowRealtimeError,
    Market,
    SessionInfo,
    UnavailableError,
    WireFormatError,
)
from .native import _SessionInfoC


if TYPE_CHECKING:
    from .client import L2FlowClient


_OK = 0
_INVALID_STATE = 3
_BUFFER_TOO_SMALL = 10
_DERIVED_CHECKPOINT_BYTES = CHECKPOINT_BYTES + 32
_UINT64_MAX = (1 << 64) - 1
_SIZE_T_MAX = ctypes.c_size_t(-1).value


class InstrumentDerivedEventKind(IntEnum):
    ORDER_REVISION = 1
    TRADE = 2
    CANCEL = 3
    STATUS = 4


class InstrumentOrderRevisionOperation(IntEnum):
    INSERT = 0
    UPDATE = 1
    FINALIZE = 2


class InstrumentOrderFinality(IntEnum):
    PROVISIONAL = 0
    FINAL = 1
    CONFLICT = 2


class _DerivedEventRowC(ctypes.Structure):
    _fields_ = [
        ("record_schema_version", ctypes.c_uint32),
        ("record_bytes", ctypes.c_uint32),
        ("derived_event_sequence", ctypes.c_uint64),
        ("trade_date", ctypes.c_uint32),
        ("instrument_id", ctypes.c_uint32),
        ("channel", ctypes.c_int64),
        ("order_id", ctypes.c_int64),
        ("buy_order_id", ctypes.c_int64),
        ("sell_order_id", ctypes.c_int64),
        ("price_p6", ctypes.c_int64),
        ("execution_boundary_price_p6", ctypes.c_int64),
        ("quantity", ctypes.c_int64),
        ("trade_amount_p6", ctypes.c_int64),
        ("published_quantity", ctypes.c_int64),
        ("original_quantity", ctypes.c_int64),
        ("remaining_quantity", ctypes.c_int64),
        ("source_matched_quantity", ctypes.c_int64),
        ("observed_pre_add_trade_quantity", ctypes.c_int64),
        ("post_add_trade_quantity", ctypes.c_int64),
        ("total_trade_quantity", ctypes.c_int64),
        ("total_cancel_quantity", ctypes.c_int64),
        ("revision", ctypes.c_uint64),
        ("trade_count", ctypes.c_uint64),
        ("cancel_count", ctypes.c_uint64),
        ("quality_flags", ctypes.c_uint64),
        ("source_quality_flags", ctypes.c_uint64),
        ("source_market_notices", ctypes.c_uint64),
        ("native_event_sequence", ctypes.c_int64),
        ("source_sequence", ctypes.c_uint64),
        ("ingress_sequence", ctypes.c_uint64),
        ("tick_stream_sequence", ctypes.c_uint64),
        ("vendor_sequence_id", ctypes.c_uint64),
        ("event_time_ns_since_midnight", ctypes.c_uint64),
        ("event_time_unix_ns", ctypes.c_int64),
        ("recv_realtime_ns", ctypes.c_int64),
        ("recv_monotonic_ns", ctypes.c_int64),
        ("vendor_local_time_raw", ctypes.c_uint32),
        ("reserved0", ctypes.c_uint32),
        ("vendor_local_time_ns_since_midnight", ctypes.c_uint64),
        ("market", ctypes.c_uint8),
        ("event_kind", ctypes.c_uint8),
        ("operation", ctypes.c_uint8),
        ("finality", ctypes.c_uint8),
        ("side", ctypes.c_uint8),
        ("side_source", ctypes.c_uint8),
        ("aggressor", ctypes.c_uint8),
        ("phase", ctypes.c_uint8),
        ("phase_at_first", ctypes.c_uint8),
        ("phase_at_add", ctypes.c_uint8),
        ("phase_at_last", ctypes.c_uint8),
        ("order_type", ctypes.c_uint8),
        ("order_source", ctypes.c_uint8),
        ("price_source", ctypes.c_uint8),
        ("original_quantity_status", ctypes.c_uint8),
        ("price_valid", ctypes.c_uint8),
        ("execution_boundary_price_valid", ctypes.c_uint8),
        ("trade_amount_valid", ctypes.c_uint8),
        ("published_quantity_valid", ctypes.c_uint8),
        ("original_quantity_valid", ctypes.c_uint8),
        ("remaining_quantity_valid", ctypes.c_uint8),
        ("source_matched_quantity_valid", ctypes.c_uint8),
        ("add_seen", ctypes.c_uint8),
        ("apply_to_book", ctypes.c_uint8),
        ("referenced_order_found", ctypes.c_uint8),
        ("side_from_order", ctypes.c_uint8),
        ("event_time_valid", ctypes.c_uint8),
        ("event_time_unix_ns_valid", ctypes.c_uint8),
        ("vendor_local_time_valid", ctypes.c_uint8),
        ("reserved1", ctypes.c_uint8 * 3),
    ]


class _DerivedCheckpointC(ctypes.Structure):
    _fields_ = [
        ("raw_checkpoint", ctypes.c_uint8 * CHECKPOINT_BYTES),
        ("derived_event_sequence_exclusive", ctypes.c_uint64),
        ("order_state_count", ctypes.c_uint64),
        ("instrument_id", ctypes.c_uint32),
        ("trade_date", ctypes.c_uint32),
        ("market", ctypes.c_uint8),
        ("finalized", ctypes.c_uint8),
        ("reserved", ctypes.c_uint8 * 6),
    ]


assert ctypes.sizeof(_DerivedCheckpointC) == _DERIVED_CHECKPOINT_BYTES
assert ctypes.sizeof(_DerivedEventRowC) == 320


@dataclass(frozen=True, slots=True)
class InstrumentDerivedEventCheckpoint:
    """EOF-verified boundary plus live native state identity.

    ``raw_checkpoint`` is portable within the retained realtime session.
    This outer checkpoint is deliberately accepted only by the reader which
    returned it, because no order-state serialization or crash recovery is
    implemented.
    """

    raw_checkpoint: InstrumentTickDeltaCheckpoint
    derived_event_sequence_exclusive: int
    order_state_count: int
    instrument_id: int
    trade_date: int
    market: Market
    finalized: bool
    _native_value: bytes

    def __post_init__(self) -> None:
        if len(self._native_value) != _DERIVED_CHECKPOINT_BYTES:
            raise ValueError("derived checkpoint native value has wrong size")
        if (
            self.instrument_id != self.raw_checkpoint.instrument_id
            or self.trade_date != self.raw_checkpoint.trade_date
        ):
            raise ValueError(
                "derived and raw checkpoint identities disagree"
            )
        if (
            self.derived_event_sequence_exclusive <= 0
            or self.order_state_count < 0
        ):
            raise ValueError("derived checkpoint counters are invalid")


@dataclass(frozen=True, slots=True)
class InstrumentDerivedEvent:
    derived_event_sequence: int
    trade_date: int
    instrument_id: int
    market: Market
    event_kind: InstrumentDerivedEventKind
    channel: int
    order_id: int
    buy_order_id: int
    sell_order_id: int
    operation: int
    finality: int
    revision: int
    side: int
    side_source: int
    aggressor: int
    phase: int
    phase_at_first: int
    phase_at_add: int
    phase_at_last: int
    order_type: int
    order_source: int
    price_source: int
    original_quantity_status: int
    price_p6: int
    price_valid: bool
    execution_boundary_price_p6: int
    execution_boundary_price_valid: bool
    quantity: int
    trade_amount_p6: int
    trade_amount_valid: bool
    published_quantity: int
    published_quantity_valid: bool
    original_quantity: int
    original_quantity_valid: bool
    remaining_quantity: int
    remaining_quantity_valid: bool
    source_matched_quantity: int
    source_matched_quantity_valid: bool
    observed_pre_add_trade_quantity: int
    post_add_trade_quantity: int
    total_trade_quantity: int
    total_cancel_quantity: int
    trade_count: int
    cancel_count: int
    quality_flags: int
    source_quality_flags: int
    source_market_notices: int
    add_seen: bool
    apply_to_book: bool
    referenced_order_found: bool
    side_from_order: bool
    native_event_sequence: int
    source_sequence: int
    ingress_sequence: int
    tick_stream_sequence: int
    vendor_sequence_id: int
    event_time_ns_since_midnight: int
    event_time_unix_ns: int
    recv_realtime_ns: int
    recv_monotonic_ns: int
    vendor_local_time_raw: int
    vendor_local_time_ns_since_midnight: int
    event_time_valid: bool
    event_time_unix_ns_valid: bool
    vendor_local_time_valid: bool


def _event_from_c(row: _DerivedEventRowC) -> InstrumentDerivedEvent:
    if (
        row.record_schema_version != 1
        or row.record_bytes != ctypes.sizeof(_DerivedEventRowC)
        or row.reserved0
        or any(row.reserved1)
    ):
        raise WireFormatError("invalid derived event row header/reserved data")
    boolean_fields = (
        row.price_valid,
        row.execution_boundary_price_valid,
        row.trade_amount_valid,
        row.published_quantity_valid,
        row.original_quantity_valid,
        row.remaining_quantity_valid,
        row.source_matched_quantity_valid,
        row.add_seen,
        row.apply_to_book,
        row.referenced_order_found,
        row.side_from_order,
        row.event_time_valid,
        row.event_time_unix_ns_valid,
        row.vendor_local_time_valid,
    )
    if any(value not in (0, 1) for value in boolean_fields):
        raise WireFormatError(
            "invalid derived event boolean encoding"
        )
    try:
        market = Market(row.market)
        kind = InstrumentDerivedEventKind(row.event_kind)
    except ValueError as error:
        raise WireFormatError("invalid derived event enum") from error
    return InstrumentDerivedEvent(
        derived_event_sequence=row.derived_event_sequence,
        trade_date=row.trade_date,
        instrument_id=row.instrument_id,
        market=market,
        event_kind=kind,
        channel=row.channel,
        order_id=row.order_id,
        buy_order_id=row.buy_order_id,
        sell_order_id=row.sell_order_id,
        operation=row.operation,
        finality=row.finality,
        revision=row.revision,
        side=row.side,
        side_source=row.side_source,
        aggressor=row.aggressor,
        phase=row.phase,
        phase_at_first=row.phase_at_first,
        phase_at_add=row.phase_at_add,
        phase_at_last=row.phase_at_last,
        order_type=row.order_type,
        order_source=row.order_source,
        price_source=row.price_source,
        original_quantity_status=row.original_quantity_status,
        price_p6=row.price_p6,
        price_valid=bool(row.price_valid),
        execution_boundary_price_p6=(
            row.execution_boundary_price_p6
        ),
        execution_boundary_price_valid=bool(
            row.execution_boundary_price_valid
        ),
        quantity=row.quantity,
        trade_amount_p6=row.trade_amount_p6,
        trade_amount_valid=bool(row.trade_amount_valid),
        published_quantity=row.published_quantity,
        published_quantity_valid=bool(row.published_quantity_valid),
        original_quantity=row.original_quantity,
        original_quantity_valid=bool(row.original_quantity_valid),
        remaining_quantity=row.remaining_quantity,
        remaining_quantity_valid=bool(row.remaining_quantity_valid),
        source_matched_quantity=row.source_matched_quantity,
        source_matched_quantity_valid=bool(
            row.source_matched_quantity_valid
        ),
        observed_pre_add_trade_quantity=(
            row.observed_pre_add_trade_quantity
        ),
        post_add_trade_quantity=row.post_add_trade_quantity,
        total_trade_quantity=row.total_trade_quantity,
        total_cancel_quantity=row.total_cancel_quantity,
        trade_count=row.trade_count,
        cancel_count=row.cancel_count,
        quality_flags=row.quality_flags,
        source_quality_flags=row.source_quality_flags,
        source_market_notices=row.source_market_notices,
        add_seen=bool(row.add_seen),
        apply_to_book=bool(row.apply_to_book),
        referenced_order_found=bool(row.referenced_order_found),
        side_from_order=bool(row.side_from_order),
        native_event_sequence=row.native_event_sequence,
        source_sequence=row.source_sequence,
        ingress_sequence=row.ingress_sequence,
        tick_stream_sequence=row.tick_stream_sequence,
        vendor_sequence_id=row.vendor_sequence_id,
        event_time_ns_since_midnight=(
            row.event_time_ns_since_midnight
        ),
        event_time_unix_ns=row.event_time_unix_ns,
        recv_realtime_ns=row.recv_realtime_ns,
        recv_monotonic_ns=row.recv_monotonic_ns,
        vendor_local_time_raw=row.vendor_local_time_raw,
        vendor_local_time_ns_since_midnight=(
            row.vendor_local_time_ns_since_midnight
        ),
        event_time_valid=bool(row.event_time_valid),
        event_time_unix_ns_valid=bool(
            row.event_time_unix_ns_valid
        ),
        vendor_local_time_valid=bool(
            row.vendor_local_time_valid
        ),
    )


class InstrumentDerivedEventBatch:
    """One owned native result page, materialized lazily into Python rows."""

    __slots__ = ("_rows", "_record_count")

    def __init__(self, rows, record_count: int) -> None:
        self._rows = rows
        self._record_count = record_count

    def __len__(self) -> int:
        return self._record_count

    def __iter__(self) -> Iterator[InstrumentDerivedEvent]:
        for index in range(self._record_count):
            yield _event_from_c(self._rows[index])

    def row(self, index: int) -> InstrumentDerivedEvent:
        if not isinstance(index, int) or isinstance(index, bool):
            raise TypeError("index must be an integer")
        if index < 0:
            index += self._record_count
        if index < 0 or index >= self._record_count:
            raise IndexError(index)
        return _event_from_c(self._rows[index])

    def materialize(self) -> tuple[InstrumentDerivedEvent, ...]:
        return tuple(self)


class InstrumentDerivedEventHistoryError(L2FlowRealtimeError):
    def __init__(
        self, operation: str, code: int, raw_error: Optional[int] = None
    ) -> None:
        self.operation = operation
        self.code = code
        self.raw_error = raw_error
        detail = f"{operation} failed with derived-history code {code}"
        if raw_error is not None:
            detail += f" (raw-history code {raw_error})"
        super().__init__(detail)


def _bind_library(library) -> None:
    if getattr(library, "_l2flow_derived_history_bound_v1", False):
        return
    handle = ctypes.c_void_p
    try:
        open_ = (
            library.l2flow_instrument_derived_event_history_session_open_v1
        )
    except AttributeError as error:
        raise UnavailableError(
            "native library lacks instrument derived-event history V1"
        ) from error
    open_.argtypes = [
        ctypes.c_char_p,
        ctypes.POINTER(_SessionInfoC),
        ctypes.c_uint32,
        ctypes.c_uint8,
        ctypes.c_size_t,
        ctypes.c_uint32,
        ctypes.POINTER(handle),
    ]
    open_.restype = ctypes.c_int
    library.l2flow_instrument_derived_event_history_session_close_v1.argtypes = [
        handle
    ]
    library.l2flow_instrument_derived_event_history_session_close_v1.restype = (
        None
    )
    library.l2flow_instrument_derived_event_history_begin_full_v1.argtypes = [
        handle,
        ctypes.c_uint64,
        ctypes.c_uint32,
    ]
    library.l2flow_instrument_derived_event_history_begin_full_v1.restype = (
        ctypes.c_int
    )
    library.l2flow_instrument_derived_event_history_begin_update_v1.argtypes = [
        handle,
        ctypes.POINTER(_DerivedCheckpointC),
        ctypes.c_uint64,
        ctypes.c_uint32,
    ]
    library.l2flow_instrument_derived_event_history_begin_update_v1.restype = (
        ctypes.c_int
    )
    library.l2flow_instrument_derived_event_history_read_v1.argtypes = [
        handle,
        ctypes.POINTER(_DerivedEventRowC),
        ctypes.c_size_t,
        ctypes.POINTER(ctypes.c_size_t),
        ctypes.POINTER(ctypes.c_uint32),
    ]
    library.l2flow_instrument_derived_event_history_read_v1.restype = (
        ctypes.c_int
    )
    checkpoint = (
        library.l2flow_instrument_derived_event_history_verified_checkpoint_v1
    )
    checkpoint.argtypes = [handle, ctypes.POINTER(_DerivedCheckpointC)]
    checkpoint.restype = ctypes.c_int
    library.l2flow_instrument_derived_event_history_finalize_v1.argtypes = [
        handle,
        ctypes.POINTER(_DerivedEventRowC),
        ctypes.c_size_t,
        ctypes.POINTER(ctypes.c_size_t),
    ]
    library.l2flow_instrument_derived_event_history_finalize_v1.restype = (
        ctypes.c_int
    )
    library.l2flow_instrument_derived_event_history_last_raw_error_v1.argtypes = [
        handle
    ]
    library.l2flow_instrument_derived_event_history_last_raw_error_v1.restype = (
        ctypes.c_int
    )
    library._l2flow_derived_history_bound_v1 = True


def _expected_generation(value: Optional[int]) -> int:
    if value is None:
        return 0
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or value <= 0
        or value > _UINT64_MAX
    ):
        raise ValueError(
            "expected_generation must be a positive uint64 or None"
        )
    return value


def _session_to_c(session: SessionInfo) -> _SessionInfoC:
    output = _SessionInfoC()
    output.run_id[:] = session.run_id
    output.layout_digest[:] = session.layout_digest
    output.catalog_digest[:] = session.catalog_digest
    for name in (
        "session_epoch",
        "catalog_generation",
        "data_state_generation",
        "accepted_sequence",
        "applied_sequence",
        "processing_lag_records",
        "tick_ring_capacity",
        "tick_highest_published_sequence",
        "tick_contiguous_published_sequence",
        "kline_generation",
        "heartbeat_monotonic_ns",
        "published_records",
        "trade_date",
        "flags",
        "capacity",
        "window_count",
        "bound_count",
        "available_count",
        "snapshot_available_count",
        "tick_available_count",
        "factor_eligible_count",
    ):
        setattr(output, name, int(getattr(session, name)))
    output.server_state = int(session.server_state)
    output.catalog_scope = int(session.catalog_scope)
    output.coverage_complete = int(session.coverage_complete)
    return output


def _checkpoint_from_c(
    value: _DerivedCheckpointC,
) -> InstrumentDerivedEventCheckpoint:
    if any(value.reserved) or value.finalized not in (0, 1):
        raise WireFormatError(
            "derived checkpoint reserved bytes are nonzero"
        )
    raw_wire = bytes(value.raw_checkpoint)
    raw = InstrumentTickDeltaCheckpoint.from_wire(raw_wire)
    try:
        market = Market(value.market)
    except ValueError as error:
        raise WireFormatError(
            "derived checkpoint market is invalid"
        ) from error
    return InstrumentDerivedEventCheckpoint(
        raw_checkpoint=raw,
        derived_event_sequence_exclusive=(
            value.derived_event_sequence_exclusive
        ),
        order_state_count=value.order_state_count,
        instrument_id=value.instrument_id,
        trade_date=value.trade_date,
        market=market,
        finalized=bool(value.finalized),
        _native_value=ctypes.string_at(
            ctypes.byref(value), ctypes.sizeof(value)
        ),
    )


def _checkpoint_to_c(
    checkpoint: InstrumentDerivedEventCheckpoint,
) -> _DerivedCheckpointC:
    if not isinstance(checkpoint, InstrumentDerivedEventCheckpoint):
        raise TypeError(
            "checkpoint must be InstrumentDerivedEventCheckpoint"
        )
    return _DerivedCheckpointC.from_buffer_copy(
        checkpoint._native_value
    )


class InstrumentDerivedEventReadCursor:
    """One finite derived full/update read with explicit EOF verification."""

    __slots__ = ("_reader", "_done", "_closed", "_checkpoint")

    def __init__(
        self, reader: "InstrumentDerivedEventHistoryReader"
    ) -> None:
        self._reader = reader
        self._done = False
        self._closed = False
        self._checkpoint: Optional[
            InstrumentDerivedEventCheckpoint
        ] = None

    @property
    def done(self) -> bool:
        return self._done

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def verified_checkpoint(
        self,
    ) -> InstrumentDerivedEventCheckpoint:
        if not self._done or self._checkpoint is None:
            raise UnavailableError(
                "derived checkpoint is unavailable before explicit EOF"
            )
        return self._checkpoint

    def read_batch(self) -> Optional[InstrumentDerivedEventBatch]:
        if self._closed:
            raise UnavailableError("derived history cursor is closed")
        if self._done:
            return None
        batch, eof = self._reader._read_native_page()
        if eof:
            self._checkpoint = self._reader._verified_checkpoint()
            self._done = True
            self._reader._complete_cursor(self)
            return None
        return batch

    def batches(self) -> Iterator[InstrumentDerivedEventBatch]:
        while not self._done:
            batch = self.read_batch()
            if batch is None:
                break
            yield batch

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if not self._done:
            # Native state has no rollback copy. Fail-close the owning reader
            # rather than allowing an update from a partially reduced page.
            self._reader.close()
        else:
            self._reader._complete_cursor(self)

    def __enter__(self) -> "InstrumentDerivedEventReadCursor":
        if self._closed:
            raise UnavailableError("derived history cursor is closed")
        return self

    def __exit__(self, _type, _value, _traceback) -> None:
        self.close()


class InstrumentDerivedEventHistoryReader:
    """One native derived-history state machine bound to one instrument.

    The reader and its cursor are serial-only. A reader cannot switch
    instruments: this is required because each core validates monotonically
    increasing observed sequence numbers, while serial full reads of different
    instruments would move the global tick sequence backwards.
    """

    __slots__ = (
        "_client",
        "_library",
        "_handle",
        "_instrument_id",
        "_market",
        "_page_records",
        "_event_capacity",
        "_active_cursor",
        "_closed",
    )

    def __init__(
        self,
        client: "L2FlowClient",
        library,
        handle,
        *,
        instrument_id: int,
        market: Market,
        page_records: int,
    ) -> None:
        self._client = client
        self._library = library
        self._handle = handle
        self._instrument_id = instrument_id
        self._market = market
        self._page_records = page_records
        self._event_capacity = max(3, page_records * 3)
        self._active_cursor: Optional[
            InstrumentDerivedEventReadCursor
        ] = None
        self._closed = False

    @property
    def instrument_id(self) -> int:
        return self._instrument_id

    @property
    def market(self) -> Market:
        return self._market

    @property
    def closed(self) -> bool:
        return self._closed

    def _require_available(self) -> None:
        if self._closed:
            raise UnavailableError("derived history reader is closed")
        if self._active_cursor is not None:
            raise UnavailableError(
                "one derived history cursor is already active"
            )

    def _raise(self, operation: str, code: int) -> None:
        if code == _OK:
            return
        raw_error = (
            self._library
            .l2flow_instrument_derived_event_history_last_raw_error_v1(
                self._handle
            )
            if code == 5
            else None
        )
        raise InstrumentDerivedEventHistoryError(
            operation, code, raw_error
        )

    def read_all(
        self, *, expected_generation: Optional[int] = None
    ) -> InstrumentDerivedEventReadCursor:
        self._require_available()
        generation = _expected_generation(expected_generation)
        code = (
            self._library
            .l2flow_instrument_derived_event_history_begin_full_v1(
                self._handle, generation, self._page_records
            )
        )
        self._raise("begin_full", code)
        cursor = InstrumentDerivedEventReadCursor(self)
        self._active_cursor = cursor
        return cursor

    def read_updates(
        self,
        checkpoint: InstrumentDerivedEventCheckpoint,
        *,
        expected_generation: Optional[int] = None,
    ) -> InstrumentDerivedEventReadCursor:
        self._require_available()
        if (
            checkpoint.instrument_id != self._instrument_id
            or checkpoint.market is not self._market
        ):
            raise ValueError(
                "derived checkpoint belongs to another instrument/market"
            )
        native_checkpoint = _checkpoint_to_c(checkpoint)
        generation = _expected_generation(expected_generation)
        code = (
            self._library
            .l2flow_instrument_derived_event_history_begin_update_v1(
                self._handle,
                ctypes.byref(native_checkpoint),
                generation,
                self._page_records,
            )
        )
        self._raise("begin_update", code)
        cursor = InstrumentDerivedEventReadCursor(self)
        self._active_cursor = cursor
        return cursor

    def _read_native_page(
        self,
    ) -> tuple[InstrumentDerivedEventBatch, bool]:
        capacity = self._event_capacity
        while True:
            rows = (_DerivedEventRowC * capacity)()
            count = ctypes.c_size_t()
            eof = ctypes.c_uint32()
            code = (
                self._library
                .l2flow_instrument_derived_event_history_read_v1(
                    self._handle,
                    rows,
                    capacity,
                    ctypes.byref(count),
                    ctypes.byref(eof),
                )
            )
            if code == _BUFFER_TOO_SMALL:
                if count.value <= capacity:
                    raise WireFormatError(
                        "derived BUFFER_TOO_SMALL did not increase capacity"
                    )
                capacity = count.value
                self._event_capacity = max(
                    self._event_capacity, capacity
                )
                continue
            self._raise("read", code)
            if count.value > capacity:
                raise WireFormatError(
                    "derived read count exceeds result capacity"
                )
            if eof.value not in (0, 1):
                raise WireFormatError("derived EOF flag is invalid")
            if eof.value and count.value:
                raise WireFormatError(
                    "derived EOF returned nonempty records"
                )
            return (
                InstrumentDerivedEventBatch(rows, count.value),
                bool(eof.value),
            )

    def _verified_checkpoint(
        self,
    ) -> InstrumentDerivedEventCheckpoint:
        value = _DerivedCheckpointC()
        code = (
            self._library
            .l2flow_instrument_derived_event_history_verified_checkpoint_v1(
                self._handle, ctypes.byref(value)
            )
        )
        self._raise("verified_checkpoint", code)
        return _checkpoint_from_c(value)

    def _complete_cursor(
        self, cursor: InstrumentDerivedEventReadCursor
    ) -> None:
        if self._active_cursor is cursor:
            self._active_cursor = None

    def finalize_trading_day(
        self,
    ) -> tuple[InstrumentDerivedEvent, ...]:
        self._require_available()
        capacity = self._event_capacity
        while True:
            rows = (_DerivedEventRowC * capacity)()
            count = ctypes.c_size_t()
            code = (
                self._library
                .l2flow_instrument_derived_event_history_finalize_v1(
                    self._handle,
                    rows,
                    capacity,
                    ctypes.byref(count),
                )
            )
            if code == _BUFFER_TOO_SMALL:
                if count.value <= capacity:
                    raise WireFormatError(
                        "derived finalize capacity did not increase"
                    )
                capacity = count.value
                continue
            self._raise("finalize", code)
            if count.value > capacity:
                raise WireFormatError(
                    "derived finalize count exceeds result capacity"
                )
            return InstrumentDerivedEventBatch(
                rows, count.value
            ).materialize()

    def close(self) -> None:
        if not self._closed:
            self._library.l2flow_instrument_derived_event_history_session_close_v1(
                self._handle
            )
            self._handle = ctypes.c_void_p()
            self._active_cursor = None
            self._closed = True

    def __enter__(self) -> "InstrumentDerivedEventHistoryReader":
        if self._closed:
            raise UnavailableError("derived history reader is closed")
        return self

    def __exit__(self, _type, _value, _traceback) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except BaseException:
            pass


def _open_instrument_derived_event_history(
    client: "L2FlowClient",
    instrument: Union[int, InstrumentKey],
    *,
    maximum_order_states: int,
    page_records: int,
) -> InstrumentDerivedEventHistoryReader:
    if (
        not isinstance(maximum_order_states, int)
        or isinstance(maximum_order_states, bool)
        or maximum_order_states <= 0
        or maximum_order_states > _SIZE_T_MAX
    ):
        raise ValueError("maximum_order_states must be positive")
    if (
        not isinstance(page_records, int)
        or isinstance(page_records, bool)
        or page_records <= 0
        or page_records > MAX_RESULT_BATCH_RECORDS
    ):
        raise ValueError(
            "page_records exceeds the supported derived-history limit"
        )

    if isinstance(instrument, InstrumentKey):
        lookup = client.resolve_key(instrument)
        if lookup.status is not InstrumentLookupStatus.FOUND:
            raise UnavailableError(
                "derived history instrument key was not observed"
            )
        instrument_id = lookup.instrument_id
    elif isinstance(instrument, int) and not isinstance(instrument, bool):
        instrument_id = instrument
    else:
        raise TypeError("instrument must be an ID or InstrumentKey")
    observed = client.instrument(instrument_id)
    if observed.status not in (
        InstrumentStatus.AVAILABLE,
        InstrumentStatus.BOUND_NO_DATA,
    ):
        raise UnavailableError(
            "derived history instrument is not bound"
        )
    try:
        market = Market(observed.market)
    except ValueError as error:
        raise UnavailableError(
            "derived history instrument has unknown market"
        ) from error
    if market not in (Market.SHANGHAI, Market.SHENZHEN):
        raise UnavailableError(
            "derived history supports Shanghai/Shenzhen only"
        )

    with client._lock:
        session = client._checked_session()
        path = client._control_socket_path
        if path is None:
            raise UnavailableError(
                "derived history requires a control-socket client"
            )
        library = client._native._library
        timeout = client._control_timeout
    _bind_library(library)
    session_c = _session_to_c(session)
    handle = ctypes.c_void_p()
    timeout_ms = (
        0
        if timeout is None
        else min(0xFFFFFFFF, max(1, int(timeout * 1000)))
    )
    code = (
        library.l2flow_instrument_derived_event_history_session_open_v1(
            os.fsencode(path),
            ctypes.byref(session_c),
            instrument_id,
            int(market),
            maximum_order_states,
            timeout_ms,
            ctypes.byref(handle),
        )
    )
    if code != _OK:
        raise InstrumentDerivedEventHistoryError(
            "session_open", code
        )
    try:
        return InstrumentDerivedEventHistoryReader(
            client,
            library,
            handle,
            instrument_id=instrument_id,
            market=market,
            page_records=page_records,
        )
    except BaseException:
        library.l2flow_instrument_derived_event_history_session_close_v1(
            handle
        )
        raise


__all__ = [
    "InstrumentDerivedEvent",
    "InstrumentDerivedEventBatch",
    "InstrumentDerivedEventCheckpoint",
    "InstrumentDerivedEventHistoryError",
    "InstrumentDerivedEventHistoryReader",
    "InstrumentDerivedEventKind",
    "InstrumentDerivedEventReadCursor",
    "InstrumentOrderFinality",
    "InstrumentOrderRevisionOperation",
]
