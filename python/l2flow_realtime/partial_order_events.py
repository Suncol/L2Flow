"""Process-start partial order events repaired by bounded native reordering.

This client attaches to the stable partial Event broker and pins one
publication/correction identity and mapping descriptor. Within that mapping it
reads live committed cuts of the append-only Event prefix, affected-channel
health, and latest materialized order revisions. The product is explicitly
``PROCESS_START`` with ``BOUNDED_REORDERED_PARTIAL`` quality by default. It is
not CERTIFIED/from-open history and its checkpoint is neither a feeder
watermark nor a durable replay manifest.
"""

from __future__ import annotations

import ctypes
import os
import threading
from dataclasses import dataclass
from enum import IntEnum, IntFlag
from typing import Iterator, Optional, Union

from .instrument_derived_event_history import (
    InstrumentDerivedEvent,
    _DerivedEventRowC,
    _event_from_c,
)
from .models import (
    EventUid,
    HistoryCoverageInfo,
    L2FlowRealtimeError,
    Market,
    SessionIdentity,
    StaleSessionError,
    TemporalCoverageKind,
    UnavailableError,
    WireFormatError,
)
from .native import load_native_library


_OPEN_OK = 0
_OPEN_BROKER_UNAVAILABLE = 3
_OPEN_FULL_REPLACEMENT_REQUIRED = 17
_READ_OK = 0
_READ_INVALID_ARGUMENT = 1
_READ_INCONSISTENT = 2
_READ_NOT_YET_PUBLISHED = 3
_READ_OUTPUT_TOO_SMALL = 4
_READ_NOT_FOUND = 5
_READ_CORRUPT = 6
_READ_FULL_REPLACEMENT_REQUIRED = 7
_STATUS_SCHEMA_VERSION = 1
_RESULT_SCHEMA_VERSION = 1
_DEFAULT_BATCH_RECORDS = 4096
_MAX_BATCH_RECORDS = 65_536
_UINT32_MAX = (1 << 32) - 1
_UINT64_MAX = (1 << 64) - 1


class PartialOrderEventTemporalCoverage(IntEnum):
    """The local capture boundary represented by this distinct V2 ABI."""

    PROCESS_START = 1


class PartialOrderEventOrderingQuality(IntEnum):
    """The only quality supported by the current process-start V2 ABI."""

    BOUNDED_REORDERED_PARTIAL = 1


class PartialOrderEventServiceState(IntEnum):
    INITIALIZING = 1
    CONTIGUOUS = 2
    REORDERING = 3
    CATCHING_UP = 4
    FROZEN_CONFLICT = 5
    FROZEN_RESOURCE = 6
    RESTARTING = 7
    CORRECTION_PENDING = 8
    STOPPED_CLEAN = 9


class PartialOrderEventLastError(IntEnum):
    NONE = 0
    OUT_OF_ORDER_INPUT = 1
    CONFLICTING_DUPLICATE = 2
    RESOURCE_EXHAUSTED = 3
    WORKER_EXITED = 4
    PERMANENT_GAP = 5
    PROJECTION_FAILURE = 6
    PUBLICATION_FAILURE = 7
    PUBLICATION_INVARIANT = 8


class PartialOrderEventBrokerState(IntEnum):
    UNAVAILABLE = 1
    READY = 2
    STALE = 3
    RESTARTING = 4
    STOPPED_CLEAN = 5


class PartialOrderEventChannelFlag(IntFlag):
    NONE = 0
    ORIGIN_ESTABLISHED = 1 << 0
    AFFECTED = 1 << 1
    STALE = 1 << 2


_KNOWN_CHANNEL_FLAGS = int(
    PartialOrderEventChannelFlag.ORIGIN_ESTABLISHED
    | PartialOrderEventChannelFlag.AFFECTED
    | PartialOrderEventChannelFlag.STALE
)


class _ExpectedSessionC(ctypes.Structure):
    _fields_ = [
        ("run_id", ctypes.c_uint8 * 16),
        ("session_epoch", ctypes.c_uint64),
        ("trade_date", ctypes.c_uint32),
        ("reserved", ctypes.c_uint32),
    ]


class _CheckpointC(ctypes.Structure):
    _fields_ = [
        ("run_id", ctypes.c_uint8 * 16),
        ("session_epoch", ctypes.c_uint64),
        ("publication_generation", ctypes.c_uint64),
        ("correction_epoch", ctypes.c_uint64),
        ("next_event_sequence", ctypes.c_uint64),
        ("next_order_state_physical_slot", ctypes.c_uint64),
        ("trade_date", ctypes.c_uint32),
        ("reserved", ctypes.c_uint32),
    ]


class _SessionC(ctypes.Structure):
    _fields_ = [
        ("run_id", ctypes.c_uint8 * 16),
        ("session_epoch", ctypes.c_uint64),
        ("publication_generation", ctypes.c_uint64),
        ("correction_epoch", ctypes.c_uint64),
        ("coverage_start_unix_ns", ctypes.c_uint64),
        ("event_capacity", ctypes.c_uint64),
        ("order_state_capacity", ctypes.c_uint64),
        ("total_mapping_bytes", ctypes.c_uint64),
        ("trade_date", ctypes.c_uint32),
        ("temporal_coverage", ctypes.c_uint32),
        ("ordering_quality", ctypes.c_uint32),
        ("affected_channel_capacity", ctypes.c_uint32),
        ("broker_state", ctypes.c_uint32),
        ("broker_stale", ctypes.c_uint32),
        ("reserved", ctypes.c_uint8 * 8),
    ]


class _StatusC(ctypes.Structure):
    _fields_ = [
        ("status_schema_version", ctypes.c_uint32),
        ("status_bytes", ctypes.c_uint32),
        ("run_id", ctypes.c_uint8 * 16),
        ("session_epoch", ctypes.c_uint64),
        ("publication_generation", ctypes.c_uint64),
        ("correction_epoch", ctypes.c_uint64),
        ("coverage_start_unix_ns", ctypes.c_uint64),
        ("event_capacity", ctypes.c_uint64),
        ("order_state_capacity", ctypes.c_uint64),
        ("commit_sequence", ctypes.c_uint64),
        ("heartbeat_monotonic_ns", ctypes.c_uint64),
        ("captured_source_frontier", ctypes.c_uint64),
        ("canonical_apply_frontier", ctypes.c_uint64),
        ("event_published_frontier", ctypes.c_uint64),
        ("history_generation", ctypes.c_uint64),
        ("order_state_generation", ctypes.c_uint64),
        ("order_state_canonical_frontier", ctypes.c_uint64),
        ("committed_event_region_bytes", ctypes.c_uint64),
        ("shanghai_order_state_count", ctypes.c_uint64),
        ("shenzhen_order_state_count", ctypes.c_uint64),
        ("pending_count", ctypes.c_uint64),
        ("reorder_high_water", ctypes.c_uint64),
        ("oldest_gap_age_ns", ctypes.c_uint64),
        ("trade_date", ctypes.c_uint32),
        ("temporal_coverage", ctypes.c_uint32),
        ("ordering_quality", ctypes.c_uint32),
        ("service_state", ctypes.c_uint32),
        ("stale", ctypes.c_uint32),
        ("last_error", ctypes.c_uint32),
        ("affected_channel_count", ctypes.c_uint32),
        ("channel_health_count", ctypes.c_uint32),
        ("affected_channel_capacity", ctypes.c_uint32),
        ("broker_state", ctypes.c_uint32),
        ("broker_stale", ctypes.c_uint32),
        ("reserved", ctypes.c_uint8 * 24),
    ]


class _EnvelopeC(ctypes.Structure):
    _fields_ = [
        ("canonical_apply_sequence", ctypes.c_uint64),
        ("event", _DerivedEventRowC),
    ]


class _OrderStateC(ctypes.Structure):
    _fields_ = [
        ("canonical_apply_sequence", ctypes.c_uint64),
        ("order_revision", _DerivedEventRowC),
    ]


class _OrderKeyC(ctypes.Structure):
    _fields_ = [
        ("market", ctypes.c_uint32),
        ("instrument_id", ctypes.c_uint32),
        ("channel", ctypes.c_int64),
        ("order_id", ctypes.c_int64),
    ]


class _ChannelHealthC(ctypes.Structure):
    _fields_ = [
        ("commit_sequence", ctypes.c_uint64),
        ("channel", ctypes.c_int64),
        ("expected_native_sequence", ctypes.c_int64),
        ("contiguous_native_sequence", ctypes.c_int64),
        ("highest_observed_native_sequence", ctypes.c_int64),
        ("oldest_missing_native_sequence", ctypes.c_int64),
        ("pending_count", ctypes.c_uint64),
        ("oldest_gap_age_ns", ctypes.c_uint64),
        ("market", ctypes.c_uint32),
        ("flags", ctypes.c_uint32),
        ("service_state", ctypes.c_uint32),
        ("last_error", ctypes.c_uint32),
        ("reserved", ctypes.c_uint8 * 48),
    ]


class _ReadBatchResultC(ctypes.Structure):
    _fields_ = [
        ("result_schema_version", ctypes.c_uint32),
        ("result_bytes", ctypes.c_uint32),
        ("records_written", ctypes.c_uint64),
        ("checkpoint", _CheckpointC),
        ("status", _StatusC),
        ("reserved", ctypes.c_uint8 * 32),
    ]


class _ChannelBatchResultC(ctypes.Structure):
    _fields_ = [
        ("result_schema_version", ctypes.c_uint32),
        ("result_bytes", ctypes.c_uint32),
        ("records_written", ctypes.c_uint64),
        ("required_capacity", ctypes.c_uint64),
        ("status", _StatusC),
        ("reserved", ctypes.c_uint8 * 32),
    ]


class _OrderStateBatchResultC(ctypes.Structure):
    _fields_ = [
        ("result_schema_version", ctypes.c_uint32),
        ("result_bytes", ctypes.c_uint32),
        ("records_written", ctypes.c_uint64),
        ("checkpoint", _CheckpointC),
        ("status", _StatusC),
        ("reserved", ctypes.c_uint8 * 32),
    ]


assert ctypes.sizeof(_ExpectedSessionC) == 32
assert ctypes.sizeof(_CheckpointC) == 64
assert ctypes.sizeof(_SessionC) == 104
assert ctypes.sizeof(_StatusC) == 256
assert ctypes.sizeof(_EnvelopeC) == 328
assert ctypes.sizeof(_OrderStateC) == 328
assert ctypes.sizeof(_OrderKeyC) == 24
assert ctypes.sizeof(_ChannelHealthC) == 128
assert ctypes.sizeof(_ReadBatchResultC) == 368
assert ctypes.sizeof(_ChannelBatchResultC) == 312
assert ctypes.sizeof(_OrderStateBatchResultC) == 368


def _uint(value: int, name: str, maximum: int, *, nonzero: bool = False) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or value < int(nonzero)
        or value > maximum
    ):
        qualifier = "positive " if nonzero else ""
        raise ValueError(f"{name} must be a {qualifier}unsigned ABI integer")
    return value


def _run_id(value: bytes) -> bytes:
    if not isinstance(value, bytes) or len(value) != 16 or not any(value):
        raise ValueError("run_id must be exactly 16 nonzero bytes")
    return value


@dataclass(frozen=True, slots=True)
class PartialOrderEventCheckpoint:
    """Generation-pinned cursor, not a durable or from-open checkpoint."""

    run_id: bytes
    session_epoch: int
    trade_date: int
    publication_generation: int
    correction_epoch: int
    next_event_sequence: int
    next_order_state_physical_slot: int = 0

    def __post_init__(self) -> None:
        _run_id(self.run_id)
        _uint(self.session_epoch, "session_epoch", _UINT64_MAX, nonzero=True)
        _uint(self.trade_date, "trade_date", _UINT32_MAX, nonzero=True)
        _uint(
            self.publication_generation,
            "publication_generation",
            _UINT64_MAX,
            nonzero=True,
        )
        _uint(
            self.correction_epoch,
            "correction_epoch",
            _UINT64_MAX,
            nonzero=True,
        )
        _uint(
            self.next_event_sequence,
            "next_event_sequence",
            _UINT64_MAX,
            nonzero=True,
        )
        _uint(
            self.next_order_state_physical_slot,
            "next_order_state_physical_slot",
            _UINT64_MAX,
        )

    def _to_c(self) -> _CheckpointC:
        result = _CheckpointC()
        result.run_id[:] = self.run_id
        result.session_epoch = self.session_epoch
        result.publication_generation = self.publication_generation
        result.correction_epoch = self.correction_epoch
        result.next_event_sequence = self.next_event_sequence
        result.next_order_state_physical_slot = (
            self.next_order_state_physical_slot
        )
        result.trade_date = self.trade_date
        return result


@dataclass(frozen=True, slots=True)
class PartialOrderEventSession:
    run_id: bytes
    session_epoch: int
    trade_date: int
    publication_generation: int
    correction_epoch: int
    coverage_start_unix_ns: int
    event_capacity: int
    order_state_capacity: int
    total_mapping_bytes: int
    affected_channel_capacity: int
    temporal_coverage: PartialOrderEventTemporalCoverage
    ordering_quality: PartialOrderEventOrderingQuality
    broker_state: PartialOrderEventBrokerState
    broker_stale: bool

    def __post_init__(self) -> None:
        _run_id(self.run_id)
        for name, value, maximum in (
            ("session_epoch", self.session_epoch, _UINT64_MAX),
            ("trade_date", self.trade_date, _UINT32_MAX),
            (
                "publication_generation",
                self.publication_generation,
                _UINT64_MAX,
            ),
            ("correction_epoch", self.correction_epoch, _UINT64_MAX),
            (
                "coverage_start_unix_ns",
                self.coverage_start_unix_ns,
                _UINT64_MAX,
            ),
            ("event_capacity", self.event_capacity, _UINT64_MAX),
            (
                "order_state_capacity",
                self.order_state_capacity,
                _UINT64_MAX,
            ),
            (
                "total_mapping_bytes",
                self.total_mapping_bytes,
                _UINT64_MAX,
            ),
            (
                "affected_channel_capacity",
                self.affected_channel_capacity,
                _UINT32_MAX,
            ),
        ):
            _uint(value, name, maximum, nonzero=True)
        try:
            temporal = PartialOrderEventTemporalCoverage(
                self.temporal_coverage
            )
            ordering = PartialOrderEventOrderingQuality(
                self.ordering_quality
            )
            broker = PartialOrderEventBrokerState(self.broker_state)
        except (TypeError, ValueError) as error:
            raise ValueError("partial Event session enum is invalid") from error
        if temporal is not PartialOrderEventTemporalCoverage.PROCESS_START:
            raise ValueError("partial Event session must be PROCESS_START")
        if not isinstance(self.broker_stale, bool):
            raise TypeError("broker_stale must be bool")
        object.__setattr__(self, "temporal_coverage", temporal)
        object.__setattr__(self, "ordering_quality", ordering)
        object.__setattr__(self, "broker_state", broker)

    @property
    def identity(self) -> SessionIdentity:
        return SessionIdentity(self.run_id, self.session_epoch)

    @property
    def history_coverage(self) -> HistoryCoverageInfo:
        return HistoryCoverageInfo(
            run_id=self.run_id,
            session_epoch=self.session_epoch,
            trade_date=self.trade_date,
            coverage_kind=TemporalCoverageKind.PROCESS_START_PARTIAL,
            coverage_start_unix_ns=self.coverage_start_unix_ns,
        )

    @property
    def native_completeness_proven(self) -> bool:
        """Always false: the current feeder exposes no completeness proof."""

        return False

    def checkpoint(
        self,
        next_event_sequence: int = 1,
        next_order_state_physical_slot: int = 0,
    ) -> PartialOrderEventCheckpoint:
        _uint(
            next_event_sequence,
            "next_event_sequence",
            _UINT64_MAX,
            nonzero=True,
        )
        _uint(
            next_order_state_physical_slot,
            "next_order_state_physical_slot",
            _UINT64_MAX,
        )
        if next_event_sequence > self.event_capacity + 1:
            raise ValueError("next_event_sequence exceeds event capacity")
        if next_order_state_physical_slot > self.order_state_capacity:
            raise ValueError(
                "next_order_state_physical_slot exceeds table capacity"
            )
        return PartialOrderEventCheckpoint(
            run_id=self.run_id,
            session_epoch=self.session_epoch,
            trade_date=self.trade_date,
            publication_generation=self.publication_generation,
            correction_epoch=self.correction_epoch,
            next_event_sequence=next_event_sequence,
            next_order_state_physical_slot=(
                next_order_state_physical_slot
            ),
        )


@dataclass(frozen=True, slots=True)
class PartialOrderEventStatus:
    run_id: bytes
    session_epoch: int
    trade_date: int
    publication_generation: int
    correction_epoch: int
    coverage_start_unix_ns: int
    event_capacity: int
    order_state_capacity: int
    commit_sequence: int
    heartbeat_monotonic_ns: int
    captured_source_frontier: int
    canonical_apply_frontier: int
    event_published_frontier: int
    history_generation: int
    order_state_generation: int
    order_state_canonical_frontier: int
    committed_event_region_bytes: int
    shanghai_order_state_count: int
    shenzhen_order_state_count: int
    pending_count: int
    reorder_high_water: int
    oldest_gap_age_ns: int
    temporal_coverage: PartialOrderEventTemporalCoverage
    ordering_quality: PartialOrderEventOrderingQuality
    state: PartialOrderEventServiceState
    stale: bool
    last_error: PartialOrderEventLastError
    affected_channel_count: int
    channel_health_count: int
    affected_channel_capacity: int
    broker_state: PartialOrderEventBrokerState
    broker_stale: bool

    @property
    def history_coverage(self) -> HistoryCoverageInfo:
        return HistoryCoverageInfo(
            run_id=self.run_id,
            session_epoch=self.session_epoch,
            trade_date=self.trade_date,
            coverage_kind=TemporalCoverageKind.PROCESS_START_PARTIAL,
            coverage_start_unix_ns=self.coverage_start_unix_ns,
        )

    @property
    def order_state_available(self) -> bool:
        return (
            self.order_state_generation != 0
            and self.order_state_canonical_frontier != 0
        )


@dataclass(frozen=True, slots=True)
class PartialOrderEventChannelHealth:
    commit_sequence: int
    market: Market
    channel: int
    expected_native_sequence: int
    contiguous_native_sequence: int
    highest_observed_native_sequence: int
    oldest_missing_native_sequence: int
    pending_count: int
    oldest_gap_age_ns: int
    flags: PartialOrderEventChannelFlag
    state: PartialOrderEventServiceState
    last_error: PartialOrderEventLastError

    @property
    def affected(self) -> bool:
        return bool(self.flags & PartialOrderEventChannelFlag.AFFECTED)


@dataclass(frozen=True, slots=True)
class PartialOrderEventOrderKey:
    market: Market
    instrument_id: int
    channel: int
    order_id: int

    def __post_init__(self) -> None:
        try:
            market = Market(self.market)
        except (TypeError, ValueError) as error:
            raise ValueError("market must be SHANGHAI or SHENZHEN") from error
        if market not in (Market.SHANGHAI, Market.SHENZHEN):
            raise ValueError("market must be SHANGHAI or SHENZHEN")
        _uint(self.instrument_id, "instrument_id", _UINT32_MAX, nonzero=True)
        if (
            not isinstance(self.channel, int)
            or isinstance(self.channel, bool)
            or self.channel < 0
            or (market is Market.SHANGHAI and self.channel == 0)
            or self.channel > (1 << 63) - 1
        ):
            raise ValueError("channel is outside the market key domain")
        if (
            not isinstance(self.order_id, int)
            or isinstance(self.order_id, bool)
            or self.order_id <= 0
            or self.order_id > (1 << 63) - 1
        ):
            raise ValueError("order_id must be a positive int64")
        object.__setattr__(self, "market", market)

    def _to_c(self) -> _OrderKeyC:
        result = _OrderKeyC()
        result.market = int(self.market)
        result.instrument_id = self.instrument_id
        result.channel = self.channel
        result.order_id = self.order_id
        return result


@dataclass(frozen=True, slots=True)
class PartialOrderEvent:
    canonical_apply_sequence: int
    event: InstrumentDerivedEvent

    @property
    def event_uid(self) -> Optional[EventUid]:
        return self.event.event_uid


@dataclass(frozen=True, slots=True)
class PartialOrderState:
    canonical_apply_sequence: int
    order_revision: InstrumentDerivedEvent


@dataclass(frozen=True, slots=True)
class PartialOrderEventChannelBatch:
    rows: tuple[PartialOrderEventChannelHealth, ...]
    status: PartialOrderEventStatus

    def __len__(self) -> int:
        return len(self.rows)

    def __iter__(self) -> Iterator[PartialOrderEventChannelHealth]:
        return iter(self.rows)


class PartialOrderEventBatch:
    __slots__ = (
        "_rows",
        "_count",
        "_bytes",
        "_session_identity",
        "checkpoint",
        "status",
        "history_coverage",
    )

    def __init__(
        self,
        rows,
        count: int,
        checkpoint: PartialOrderEventCheckpoint,
        status: PartialOrderEventStatus,
        session: PartialOrderEventSession,
    ) -> None:
        self._rows = rows
        self._count = count
        byte_type = ctypes.c_uint8 * (len(rows) * ctypes.sizeof(_EnvelopeC))
        self._bytes = byte_type.from_buffer(rows)
        self._session_identity = session.identity
        self.checkpoint = checkpoint
        self.status = status
        self.history_coverage = session.history_coverage

    def __len__(self) -> int:
        return self._count

    @property
    def buffer(self) -> memoryview:
        return memoryview(self._bytes)[
            : self._count * ctypes.sizeof(_EnvelopeC)
        ].toreadonly()

    @property
    def tail_idle(self) -> bool:
        return (
            not self
            and not self.status.broker_stale
            and self.status.broker_state is PartialOrderEventBrokerState.READY
            and not self.status.stale
            and self.status.state is PartialOrderEventServiceState.CONTIGUOUS
        )

    @property
    def end_of_stream(self) -> bool:
        return (
            not self
            and self.status.state
            is PartialOrderEventServiceState.STOPPED_CLEAN
        )

    def row(self, index: int) -> PartialOrderEvent:
        if not isinstance(index, int) or isinstance(index, bool):
            raise TypeError("index must be an integer")
        if index < 0:
            index += self._count
        if index < 0 or index >= self._count:
            raise IndexError(index)
        value = self._rows[index]
        return PartialOrderEvent(
            canonical_apply_sequence=value.canonical_apply_sequence,
            event=_event_from_c(
                value.event, session_identity=self._session_identity
            ),
        )

    def __iter__(self) -> Iterator[PartialOrderEvent]:
        for index in range(self._count):
            yield self.row(index)


class PartialOrderStateBatch:
    __slots__ = (
        "_rows",
        "_count",
        "_session_identity",
        "checkpoint",
        "status",
    )

    def __init__(
        self,
        rows,
        count: int,
        checkpoint: PartialOrderEventCheckpoint,
        status: PartialOrderEventStatus,
        session_identity: SessionIdentity,
    ) -> None:
        self._rows = rows
        self._count = count
        self._session_identity = session_identity
        self.checkpoint = checkpoint
        self.status = status

    @property
    def next_physical_slot(self) -> int:
        return self.checkpoint.next_order_state_physical_slot

    def __len__(self) -> int:
        return self._count

    def row(self, index: int) -> PartialOrderState:
        if not isinstance(index, int) or isinstance(index, bool):
            raise TypeError("index must be an integer")
        if index < 0:
            index += self._count
        if index < 0 or index >= self._count:
            raise IndexError(index)
        value = self._rows[index]
        return PartialOrderState(
            canonical_apply_sequence=value.canonical_apply_sequence,
            order_revision=_event_from_c(
                value.order_revision,
                session_identity=self._session_identity,
            ),
        )

    def __iter__(self) -> Iterator[PartialOrderState]:
        for index in range(self._count):
            yield self.row(index)


class PartialOrderEventError(L2FlowRealtimeError):
    def __init__(
        self,
        operation: str,
        code: int,
        *,
        system_error_number: int = 0,
    ) -> None:
        self.operation = operation
        self.code = code
        self.system_error_number = system_error_number
        detail = f"{operation} failed with partial Event V2 code {code}"
        if system_error_number:
            detail += (
                f": [errno {system_error_number}] "
                f"{os.strerror(system_error_number)}"
            )
        super().__init__(detail)


class PartialOrderEventFullReplacementRequired(StaleSessionError):
    """The pinned mapping is no longer the broker's current identity."""

    def __init__(
        self, checkpoint: Optional[PartialOrderEventCheckpoint] = None
    ) -> None:
        self.checkpoint = checkpoint
        super().__init__(
            "partial Event cursors cannot cross publication_generation or "
            "correction_epoch; attach the current process-start generation "
            "and replace derived state from its prefix"
        )


def _bind_library(library) -> None:
    if getattr(library, "_l2flow_partial_order_event_bound_v2", False):
        return
    handle = ctypes.c_void_p
    try:
        open_ = library.l2flow_partial_order_event_reader_open_v2
    except AttributeError as error:
        raise UnavailableError(
            "native library lacks partial order-event reader V2"
        ) from error
    open_.argtypes = [
        ctypes.c_char_p,
        ctypes.POINTER(_ExpectedSessionC),
        ctypes.POINTER(_CheckpointC),
        ctypes.c_uint32,
        ctypes.POINTER(handle),
        ctypes.POINTER(ctypes.c_int),
    ]
    open_.restype = ctypes.c_int
    library.l2flow_partial_order_event_reader_close_v2.argtypes = [handle]
    library.l2flow_partial_order_event_reader_close_v2.restype = None
    library.l2flow_partial_order_event_reader_session_v2.argtypes = [
        handle,
        ctypes.POINTER(_SessionC),
    ]
    library.l2flow_partial_order_event_reader_session_v2.restype = ctypes.c_int
    library.l2flow_partial_order_event_reader_status_v2.argtypes = [
        handle,
        ctypes.POINTER(_StatusC),
    ]
    library.l2flow_partial_order_event_reader_status_v2.restype = ctypes.c_int
    library.l2flow_partial_order_event_reader_read_v2.argtypes = [
        handle,
        ctypes.POINTER(_CheckpointC),
        ctypes.POINTER(_EnvelopeC),
        ctypes.c_size_t,
        ctypes.POINTER(_ReadBatchResultC),
    ]
    library.l2flow_partial_order_event_reader_read_v2.restype = ctypes.c_int
    library.l2flow_partial_order_event_reader_affected_channels_v2.argtypes = [
        handle,
        ctypes.POINTER(_ChannelHealthC),
        ctypes.c_size_t,
        ctypes.POINTER(_ChannelBatchResultC),
    ]
    library.l2flow_partial_order_event_reader_affected_channels_v2.restype = (
        ctypes.c_int
    )
    library.l2flow_partial_order_event_reader_find_order_state_v2.argtypes = [
        handle,
        ctypes.POINTER(_OrderKeyC),
        ctypes.POINTER(_OrderStateC),
        ctypes.POINTER(_StatusC),
    ]
    library.l2flow_partial_order_event_reader_find_order_state_v2.restype = (
        ctypes.c_int
    )
    library.l2flow_partial_order_event_reader_order_states_v2.argtypes = [
        handle,
        ctypes.POINTER(_CheckpointC),
        ctypes.POINTER(_OrderStateC),
        ctypes.c_size_t,
        ctypes.POINTER(_OrderStateBatchResultC),
    ]
    library.l2flow_partial_order_event_reader_order_states_v2.restype = (
        ctypes.c_int
    )
    library._l2flow_partial_order_event_bound_v2 = True


def _expected_session(run_id: bytes, session_epoch: int, trade_date: int):
    result = _ExpectedSessionC()
    result.run_id[:] = _run_id(run_id)
    result.session_epoch = _uint(
        session_epoch, "session_epoch", _UINT64_MAX, nonzero=True
    )
    result.trade_date = _uint(
        trade_date, "trade_date", _UINT32_MAX, nonzero=True
    )
    return result


def _checkpoint_from_c(value: _CheckpointC) -> PartialOrderEventCheckpoint:
    if value.reserved != 0:
        raise WireFormatError("partial Event checkpoint reserved field is set")
    return PartialOrderEventCheckpoint(
        run_id=bytes(value.run_id),
        session_epoch=value.session_epoch,
        trade_date=value.trade_date,
        publication_generation=value.publication_generation,
        correction_epoch=value.correction_epoch,
        next_event_sequence=value.next_event_sequence,
        next_order_state_physical_slot=(
            value.next_order_state_physical_slot
        ),
    )


def _session_from_c(value: _SessionC) -> PartialOrderEventSession:
    if any(value.reserved) or value.broker_stale not in (0, 1):
        raise WireFormatError("invalid partial Event session ABI")
    try:
        temporal = PartialOrderEventTemporalCoverage(value.temporal_coverage)
        ordering = PartialOrderEventOrderingQuality(value.ordering_quality)
        broker = PartialOrderEventBrokerState(value.broker_state)
    except ValueError as error:
        raise WireFormatError("invalid partial Event session enum") from error
    if temporal is not PartialOrderEventTemporalCoverage.PROCESS_START:
        raise WireFormatError("partial Event mapping is not PROCESS_START")
    session = PartialOrderEventSession(
        run_id=_run_id(bytes(value.run_id)),
        session_epoch=_uint(
            value.session_epoch, "session_epoch", _UINT64_MAX, nonzero=True
        ),
        trade_date=_uint(
            value.trade_date, "trade_date", _UINT32_MAX, nonzero=True
        ),
        publication_generation=_uint(
            value.publication_generation,
            "publication_generation",
            _UINT64_MAX,
            nonzero=True,
        ),
        correction_epoch=_uint(
            value.correction_epoch,
            "correction_epoch",
            _UINT64_MAX,
            nonzero=True,
        ),
        coverage_start_unix_ns=_uint(
            value.coverage_start_unix_ns,
            "coverage_start_unix_ns",
            _UINT64_MAX,
            nonzero=True,
        ),
        event_capacity=_uint(
            value.event_capacity, "event_capacity", _UINT64_MAX, nonzero=True
        ),
        order_state_capacity=_uint(
            value.order_state_capacity,
            "order_state_capacity",
            _UINT64_MAX,
            nonzero=True,
        ),
        total_mapping_bytes=_uint(
            value.total_mapping_bytes,
            "total_mapping_bytes",
            _UINT64_MAX,
            nonzero=True,
        ),
        affected_channel_capacity=_uint(
            value.affected_channel_capacity,
            "affected_channel_capacity",
            _UINT32_MAX,
            nonzero=True,
        ),
        temporal_coverage=temporal,
        ordering_quality=ordering,
        broker_state=broker,
        broker_stale=bool(value.broker_stale),
    )
    return session


def _status_from_c(value: _StatusC) -> PartialOrderEventStatus:
    if (
        value.status_schema_version != _STATUS_SCHEMA_VERSION
        or value.status_bytes != ctypes.sizeof(_StatusC)
        or any(value.reserved)
        or value.stale not in (0, 1)
        or value.broker_stale not in (0, 1)
    ):
        raise WireFormatError("invalid partial Event status ABI")
    try:
        temporal = PartialOrderEventTemporalCoverage(value.temporal_coverage)
        ordering = PartialOrderEventOrderingQuality(value.ordering_quality)
        state = PartialOrderEventServiceState(value.service_state)
        last_error = PartialOrderEventLastError(value.last_error)
        broker_state = PartialOrderEventBrokerState(value.broker_state)
    except ValueError as error:
        raise WireFormatError("invalid partial Event status enum") from error
    if temporal is not PartialOrderEventTemporalCoverage.PROCESS_START:
        raise WireFormatError("partial Event status is not PROCESS_START")
    return PartialOrderEventStatus(
        run_id=_run_id(bytes(value.run_id)),
        session_epoch=value.session_epoch,
        trade_date=value.trade_date,
        publication_generation=value.publication_generation,
        correction_epoch=value.correction_epoch,
        coverage_start_unix_ns=value.coverage_start_unix_ns,
        event_capacity=value.event_capacity,
        order_state_capacity=value.order_state_capacity,
        commit_sequence=value.commit_sequence,
        heartbeat_monotonic_ns=value.heartbeat_monotonic_ns,
        captured_source_frontier=value.captured_source_frontier,
        canonical_apply_frontier=value.canonical_apply_frontier,
        event_published_frontier=value.event_published_frontier,
        history_generation=value.history_generation,
        order_state_generation=value.order_state_generation,
        order_state_canonical_frontier=value.order_state_canonical_frontier,
        committed_event_region_bytes=value.committed_event_region_bytes,
        shanghai_order_state_count=value.shanghai_order_state_count,
        shenzhen_order_state_count=value.shenzhen_order_state_count,
        pending_count=value.pending_count,
        reorder_high_water=value.reorder_high_water,
        oldest_gap_age_ns=value.oldest_gap_age_ns,
        temporal_coverage=temporal,
        ordering_quality=ordering,
        state=state,
        stale=bool(value.stale),
        last_error=last_error,
        affected_channel_count=value.affected_channel_count,
        channel_health_count=value.channel_health_count,
        affected_channel_capacity=value.affected_channel_capacity,
        broker_state=broker_state,
        broker_stale=bool(value.broker_stale),
    )


def _channel_from_c(value: _ChannelHealthC) -> PartialOrderEventChannelHealth:
    if any(value.reserved) or value.flags & ~_KNOWN_CHANNEL_FLAGS:
        raise WireFormatError("invalid partial Event channel-health ABI")
    try:
        market = Market(value.market)
        state = PartialOrderEventServiceState(value.service_state)
        last_error = PartialOrderEventLastError(value.last_error)
    except ValueError as error:
        raise WireFormatError("invalid partial Event channel enum") from error
    if market not in (Market.SHANGHAI, Market.SHENZHEN):
        raise WireFormatError("invalid partial Event channel market")
    return PartialOrderEventChannelHealth(
        commit_sequence=value.commit_sequence,
        market=market,
        channel=value.channel,
        expected_native_sequence=value.expected_native_sequence,
        contiguous_native_sequence=value.contiguous_native_sequence,
        highest_observed_native_sequence=value.highest_observed_native_sequence,
        oldest_missing_native_sequence=value.oldest_missing_native_sequence,
        pending_count=value.pending_count,
        oldest_gap_age_ns=value.oldest_gap_age_ns,
        flags=PartialOrderEventChannelFlag(value.flags),
        state=state,
        last_error=last_error,
    )


def _validate_result(value, expected_type) -> None:
    if (
        value.result_schema_version != _RESULT_SCHEMA_VERSION
        or value.result_bytes != ctypes.sizeof(expected_type)
        or any(value.reserved)
    ):
        raise WireFormatError("invalid partial Event result ABI")


def _status_matches_session(
    status: PartialOrderEventStatus,
    session: PartialOrderEventSession,
) -> None:
    if (
        status.run_id != session.run_id
        or status.session_epoch != session.session_epoch
        or status.trade_date != session.trade_date
        or status.publication_generation != session.publication_generation
        or status.correction_epoch != session.correction_epoch
        or status.coverage_start_unix_ns != session.coverage_start_unix_ns
        or status.temporal_coverage is not session.temporal_coverage
        or status.ordering_quality is not session.ordering_quality
    ):
        raise WireFormatError("partial Event status changed generation identity")


class PartialOrderEventReader:
    """Thread-safe, generation-pinned reader for one partial Event mapping."""

    __slots__ = (
        "_library",
        "_handle",
        "_session",
        "_checkpoint",
        "_batch_records",
        "_lock",
        "_closed",
    )

    def __init__(
        self,
        library,
        handle,
        session: PartialOrderEventSession,
        checkpoint: PartialOrderEventCheckpoint,
        batch_records: int,
    ) -> None:
        self._library = library
        self._handle = handle
        self._session = session
        self._checkpoint = checkpoint
        self._batch_records = batch_records
        self._lock = threading.Lock()
        self._closed = False

    @classmethod
    def connect(
        cls,
        control_socket_path: Union[str, os.PathLike],
        *,
        run_id: bytes,
        session_epoch: int,
        trade_date: int,
        checkpoint: Optional[PartialOrderEventCheckpoint] = None,
        start_event_sequence: int = 1,
        timeout: float = 1.0,
        batch_records: int = _DEFAULT_BATCH_RECORDS,
        native_library=None,
        native_library_path=None,
    ) -> "PartialOrderEventReader":
        if native_library is not None and native_library_path is not None:
            raise ValueError(
                "native_library and native_library_path are mutually exclusive"
            )
        path = os.fspath(control_socket_path)
        if not path or "\x00" in path or not os.path.isabs(path):
            raise ValueError("control_socket_path must be absolute")
        if (
            not isinstance(timeout, (int, float))
            or isinstance(timeout, bool)
            or timeout <= 0
            or timeout > 60
        ):
            raise ValueError("timeout must be in (0, 60] seconds")
        _uint(
            start_event_sequence,
            "start_event_sequence",
            _UINT64_MAX,
            nonzero=True,
        )
        _uint(
            batch_records,
            "batch_records",
            _MAX_BATCH_RECORDS,
            nonzero=True,
        )
        expected = _expected_session(run_id, session_epoch, trade_date)
        if checkpoint is not None:
            if not isinstance(checkpoint, PartialOrderEventCheckpoint):
                raise TypeError(
                    "checkpoint must be PartialOrderEventCheckpoint or None"
                )
            if start_event_sequence != 1:
                raise ValueError(
                    "start_event_sequence cannot accompany checkpoint"
                )
            if (
                checkpoint.run_id != run_id
                or checkpoint.session_epoch != session_epoch
                or checkpoint.trade_date != trade_date
            ):
                raise ValueError("checkpoint and expected session disagree")
            native_checkpoint = checkpoint._to_c()
            checkpoint_pointer = ctypes.byref(native_checkpoint)
        else:
            native_checkpoint = None
            checkpoint_pointer = None
        library = (
            native_library
            if native_library is not None
            else load_native_library(native_library_path)
        )
        _bind_library(library)
        handle = ctypes.c_void_p()
        system_error = ctypes.c_int()
        code = library.l2flow_partial_order_event_reader_open_v2(
            os.fsencode(path),
            ctypes.byref(expected),
            checkpoint_pointer,
            max(1, int(timeout * 1000)),
            ctypes.byref(handle),
            ctypes.byref(system_error),
        )
        if code == _OPEN_FULL_REPLACEMENT_REQUIRED:
            raise PartialOrderEventFullReplacementRequired(checkpoint)
        if code == _OPEN_BROKER_UNAVAILABLE:
            raise UnavailableError(
                "partial Event broker has no last-good generation available"
            )
        if code != _OPEN_OK or not handle.value:
            raise PartialOrderEventError(
                "open", code, system_error_number=system_error.value
            )
        try:
            native_session = _SessionC()
            read = library.l2flow_partial_order_event_reader_session_v2(
                handle, ctypes.byref(native_session)
            )
            if read == _READ_FULL_REPLACEMENT_REQUIRED:
                raise PartialOrderEventFullReplacementRequired(checkpoint)
            if read != _READ_OK:
                raise PartialOrderEventError("session", read)
            session = _session_from_c(native_session)
            if (
                session.run_id != run_id
                or session.session_epoch != session_epoch
                or session.trade_date != trade_date
            ):
                raise WireFormatError("partial Event mapping session mismatch")
            if checkpoint is None:
                if start_event_sequence > session.event_capacity + 1:
                    raise ValueError(
                        "start_event_sequence exceeds the natural journal tail"
                    )
                checkpoint = session.checkpoint(start_event_sequence)
            elif (
                checkpoint.publication_generation
                != session.publication_generation
                or checkpoint.correction_epoch != session.correction_epoch
            ):
                raise PartialOrderEventFullReplacementRequired(checkpoint)
            elif (
                checkpoint.next_event_sequence > session.event_capacity + 1
                or checkpoint.next_order_state_physical_slot
                > session.order_state_capacity
            ):
                raise WireFormatError(
                    "partial Event checkpoint cursor exceeds mapping capacity"
                )
            return cls(
                library, handle, session, checkpoint, batch_records
            )
        except BaseException:
            library.l2flow_partial_order_event_reader_close_v2(handle)
            raise

    @property
    def session(self) -> PartialOrderEventSession:
        return self._session

    @property
    def history_coverage(self) -> HistoryCoverageInfo:
        return self._session.history_coverage

    @property
    def checkpoint(self) -> PartialOrderEventCheckpoint:
        return self._checkpoint

    @property
    def next_event_sequence(self) -> int:
        return self._checkpoint.next_event_sequence

    @property
    def closed(self) -> bool:
        return self._closed

    def _require_open(self) -> None:
        if self._closed:
            raise PartialOrderEventError(
                "reader_closed", _READ_INVALID_ARGUMENT
            )

    def status(self) -> PartialOrderEventStatus:
        with self._lock:
            self._require_open()
            value = _StatusC()
            code = self._library.l2flow_partial_order_event_reader_status_v2(
                self._handle, ctypes.byref(value)
            )
            if code == _READ_FULL_REPLACEMENT_REQUIRED:
                raise PartialOrderEventFullReplacementRequired(self._checkpoint)
            if code != _READ_OK:
                raise PartialOrderEventError("status", code)
            status = _status_from_c(value)
            _status_matches_session(status, self._session)
            return status

    def read_batch(self) -> PartialOrderEventBatch:
        with self._lock:
            self._require_open()
            rows = (_EnvelopeC * self._batch_records)()
            result = _ReadBatchResultC()
            native_checkpoint = self._checkpoint._to_c()
            code = self._library.l2flow_partial_order_event_reader_read_v2(
                self._handle,
                ctypes.byref(native_checkpoint),
                rows,
                self._batch_records,
                ctypes.byref(result),
            )
            if code == _READ_FULL_REPLACEMENT_REQUIRED:
                raise PartialOrderEventFullReplacementRequired(self._checkpoint)
            if code in (_READ_INCONSISTENT, _READ_CORRUPT):
                raise PartialOrderEventError("read", code)
            if code not in (_READ_OK, _READ_NOT_YET_PUBLISHED):
                raise PartialOrderEventError("read", code)
            _validate_result(result, _ReadBatchResultC)
            if result.records_written > self._batch_records:
                raise WireFormatError("partial Event batch exceeds capacity")
            if code != _READ_OK and result.records_written != 0:
                raise WireFormatError("idle partial Event read returned rows")
            checkpoint = _checkpoint_from_c(result.checkpoint)
            expected_next = (
                self._checkpoint.next_event_sequence + result.records_written
            )
            if (
                checkpoint.run_id != self._checkpoint.run_id
                or checkpoint.session_epoch != self._checkpoint.session_epoch
                or checkpoint.trade_date != self._checkpoint.trade_date
                or checkpoint.publication_generation
                != self._checkpoint.publication_generation
                or checkpoint.correction_epoch
                != self._checkpoint.correction_epoch
                or checkpoint.next_event_sequence != expected_next
                or checkpoint.next_order_state_physical_slot
                != self._checkpoint.next_order_state_physical_slot
            ):
                raise WireFormatError("partial Event checkpoint advanced invalidly")
            status = _status_from_c(result.status)
            _status_matches_session(status, self._session)
            self._checkpoint = checkpoint
            return PartialOrderEventBatch(
                rows,
                result.records_written,
                checkpoint,
                status,
                self._session,
            )

    def affected_channels(self) -> PartialOrderEventChannelBatch:
        with self._lock:
            self._require_open()
            capacity = 0
            for _attempt in range(4):
                rows = (_ChannelHealthC * capacity)() if capacity else None
                result = _ChannelBatchResultC()
                code = self._library.l2flow_partial_order_event_reader_affected_channels_v2(
                    self._handle,
                    rows,
                    capacity,
                    ctypes.byref(result),
                )
                if code == _READ_FULL_REPLACEMENT_REQUIRED:
                    raise PartialOrderEventFullReplacementRequired(
                        self._checkpoint
                    )
                _validate_result(result, _ChannelBatchResultC)
                if code == _READ_OUTPUT_TOO_SMALL:
                    if (
                        result.records_written != 0
                        or result.required_capacity <= capacity
                        or result.required_capacity
                        > self._session.affected_channel_capacity
                    ):
                        raise WireFormatError(
                            "invalid affected-channel capacity negotiation"
                        )
                    capacity = result.required_capacity
                    continue
                if code != _READ_OK:
                    raise PartialOrderEventError("affected_channels", code)
                if (
                    result.records_written > capacity
                    or result.required_capacity != result.records_written
                ):
                    raise WireFormatError("invalid affected-channel result")
                status = _status_from_c(result.status)
                _status_matches_session(status, self._session)
                converted = tuple(
                    _channel_from_c(rows[index])
                    for index in range(result.records_written)
                )
                if any(
                    row.commit_sequence != status.commit_sequence
                    for row in converted
                ):
                    raise WireFormatError(
                        "channel rows do not belong to the returned cut"
                    )
                return PartialOrderEventChannelBatch(converted, status)
            raise PartialOrderEventError(
                "affected_channels", _READ_INCONSISTENT
            )

    def find_order_state(
        self, key: PartialOrderEventOrderKey
    ) -> Optional[PartialOrderState]:
        if not isinstance(key, PartialOrderEventOrderKey):
            raise TypeError("key must be PartialOrderEventOrderKey")
        with self._lock:
            self._require_open()
            native_key = key._to_c()
            row = _OrderStateC()
            native_status = _StatusC()
            code = self._library.l2flow_partial_order_event_reader_find_order_state_v2(
                self._handle,
                ctypes.byref(native_key),
                ctypes.byref(row),
                ctypes.byref(native_status),
            )
            if code == _READ_FULL_REPLACEMENT_REQUIRED:
                raise PartialOrderEventFullReplacementRequired(self._checkpoint)
            if code == _READ_NOT_FOUND:
                return None
            if code != _READ_OK:
                raise PartialOrderEventError("find_order_state", code)
            status = _status_from_c(native_status)
            _status_matches_session(status, self._session)
            revision = _event_from_c(
                row.order_revision,
                session_identity=self._session.identity,
            )
            if (
                revision.market is not key.market
                or revision.instrument_id != key.instrument_id
                or revision.channel != key.channel
                or revision.order_id != key.order_id
            ):
                raise WireFormatError("order-state lookup returned another key")
            return PartialOrderState(row.canonical_apply_sequence, revision)

    def read_order_states(
        self,
        *,
        capacity: int = _DEFAULT_BATCH_RECORDS,
    ) -> PartialOrderStateBatch:
        """Read one physical-slot page coherent with its returned status cut.

        The cursor does not pin that cut across calls, so pages read while the
        producer is publishing are not a single cross-page snapshot. A full
        scan is stable once writes to this mapping are quiescent.
        """
        _uint(capacity, "capacity", _MAX_BATCH_RECORDS, nonzero=True)
        with self._lock:
            self._require_open()
            rows = (_OrderStateC * capacity)()
            result = _OrderStateBatchResultC()
            native_checkpoint = self._checkpoint._to_c()
            code = self._library.l2flow_partial_order_event_reader_order_states_v2(
                self._handle,
                ctypes.byref(native_checkpoint),
                rows,
                capacity,
                ctypes.byref(result),
            )
            if code == _READ_FULL_REPLACEMENT_REQUIRED:
                raise PartialOrderEventFullReplacementRequired(self._checkpoint)
            if code != _READ_OK:
                raise PartialOrderEventError("read_order_states", code)
            _validate_result(result, _OrderStateBatchResultC)
            checkpoint = _checkpoint_from_c(result.checkpoint)
            if (
                result.records_written > capacity
                or checkpoint.next_order_state_physical_slot
                < self._checkpoint.next_order_state_physical_slot
                or checkpoint.next_order_state_physical_slot
                > self._session.order_state_capacity
                or checkpoint.next_event_sequence
                != self._checkpoint.next_event_sequence
                or checkpoint.run_id != self._checkpoint.run_id
                or checkpoint.session_epoch
                != self._checkpoint.session_epoch
                or checkpoint.trade_date != self._checkpoint.trade_date
                or checkpoint.publication_generation
                != self._checkpoint.publication_generation
                or checkpoint.correction_epoch
                != self._checkpoint.correction_epoch
            ):
                raise WireFormatError("invalid order-state enumeration cursor")
            status = _status_from_c(result.status)
            _status_matches_session(status, self._session)
            self._checkpoint = checkpoint
            return PartialOrderStateBatch(
                rows,
                result.records_written,
                checkpoint,
                status,
                self._session.identity,
            )

    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self._library.l2flow_partial_order_event_reader_close_v2(
                    self._handle
                )
                self._handle = ctypes.c_void_p()
                self._closed = True

    def __enter__(self) -> "PartialOrderEventReader":
        self._require_open()
        return self

    def __exit__(self, _type, _value, _traceback) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except BaseException:
            pass


def open_partial_order_events(
    control_socket_path: Union[str, os.PathLike],
    *,
    run_id: bytes,
    session_epoch: int,
    trade_date: int,
    **kwargs,
) -> PartialOrderEventReader:
    return PartialOrderEventReader.connect(
        control_socket_path,
        run_id=run_id,
        session_epoch=session_epoch,
        trade_date=trade_date,
        **kwargs,
    )


__all__ = [
    "PartialOrderEvent",
    "PartialOrderEventBatch",
    "PartialOrderEventBrokerState",
    "PartialOrderEventChannelBatch",
    "PartialOrderEventChannelFlag",
    "PartialOrderEventChannelHealth",
    "PartialOrderEventCheckpoint",
    "PartialOrderEventError",
    "PartialOrderEventFullReplacementRequired",
    "PartialOrderEventLastError",
    "PartialOrderEventOrderKey",
    "PartialOrderEventOrderingQuality",
    "PartialOrderEventReader",
    "PartialOrderEventServiceState",
    "PartialOrderEventSession",
    "PartialOrderEventStatus",
    "PartialOrderEventTemporalCoverage",
    "PartialOrderState",
    "PartialOrderStateBatch",
    "open_partial_order_events",
]
