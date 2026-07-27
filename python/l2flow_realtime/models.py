"""Immutable public models for the L2Flow realtime shared-memory client."""

from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum
from typing import Generic, Optional, Tuple, TypeVar


class L2FlowRealtimeError(RuntimeError):
    """Base class for realtime client failures."""


class ProtocolError(L2FlowRealtimeError):
    """The control-plane peer returned a malformed or incompatible message."""


class WireFormatError(L2FlowRealtimeError):
    """A native reader copy did not conform to the published wire ABI."""


class NativeReaderError(L2FlowRealtimeError):
    """The native shared-memory reader rejected an operation."""

    def __init__(self, operation: str, code: int, detail: str = "") -> None:
        message = f"{operation} failed with native reader error {code}"
        if detail:
            message += f": {detail}"
        super().__init__(message)
        self.operation = operation
        self.code = code
        self.detail = detail


class UnavailableError(L2FlowRealtimeError):
    """The service cannot currently provide trustworthy read coverage."""


class StaleSessionError(UnavailableError):
    """The mapped session changed or its live heartbeat became stale."""


class ClientClosedError(L2FlowRealtimeError):
    """An operation was attempted after the client was closed."""


class InconsistentReadError(UnavailableError):
    """A bounded native consistency read could not obtain a stable copy."""


class TickOverrunError(L2FlowRealtimeError):
    """The requested tick sequence has already been overwritten."""

    def __init__(self, expected_sequence: int, observed_sequence: int) -> None:
        super().__init__(
            "tick cursor overrun: expected sequence "
            f"{expected_sequence}, observed {observed_sequence}"
        )
        self.expected_sequence = expected_sequence
        self.observed_sequence = observed_sequence


class OptionalDependencyError(ImportError):
    """An explicitly requested Arrow or Polars adapter is not installed."""


class LatestStatus(IntEnum):
    AVAILABLE = 0
    NOT_YET_OBSERVED = 1
    UNKNOWN_INSTRUMENT = 2
    INVALID_INSTRUMENT_ID = 3
    UNKNOWN_WINDOW = 4
    INVALID_WINDOW_ID = 5


class ServerState(IntEnum):
    INITIALIZING = 1
    ACTIVE = 2
    DRAINING = 3
    STOPPED_CLEAN = 4
    FAILED = 5


class Market(IntEnum):
    UNKNOWN = 0
    SHANGHAI = 1
    SHENZHEN = 2


class MarketEventKind(IntEnum):
    SHANGHAI_SNAPSHOT = 1
    SHANGHAI_TICK = 2
    SHENZHEN_SNAPSHOT = 3
    SHENZHEN_ORDER = 4
    SHENZHEN_TRANSACTION = 5


class TickAction(IntEnum):
    UNKNOWN = 0
    ADD = 1
    CANCEL = 2
    TRADE = 3
    STATUS = 4


class Side(IntEnum):
    UNKNOWN = 0
    BUY = 1
    SELL = 2
    BORROW = 3
    LEND = 4


class OrderType(IntEnum):
    UNKNOWN = 0
    MARKET = 1
    LIMIT = 2
    SAME_SIDE_BEST = 3


class Aggressor(IntEnum):
    UNKNOWN = 0
    BUY = 1
    SELL = 2
    NEUTRAL = 3


class TradingPhase(IntEnum):
    UNKNOWN = 0
    START = 1
    OPENING_CALL = 2
    CONTINUOUS = 3
    SUSPENDED = 4
    CLOSING_CALL = 5
    CLOSED = 6
    END = 7


@dataclass(frozen=True, slots=True)
class SessionIdentity:
    run_id: bytes
    session_epoch: int

    def __post_init__(self) -> None:
        if len(self.run_id) != 16:
            raise ValueError("run_id must contain exactly 16 bytes")
        if self.session_epoch <= 0:
            raise ValueError("session_epoch must be positive")


@dataclass(frozen=True, slots=True)
class SessionInfo:
    run_id: bytes
    session_epoch: int
    registry_version: int
    registry_sha256: bytes
    tick_ring_capacity: int
    tick_highest_published_sequence: int
    tick_contiguous_published_sequence: int
    kline_generation: int
    heartbeat_monotonic_ns: int
    trade_date: int
    server_state: ServerState
    flags: int
    instrument_count: int
    window_count: int

    def __post_init__(self) -> None:
        if len(self.run_id) != 16 or not any(self.run_id):
            raise ValueError("run_id must be a nonzero 16-byte identity")
        if len(self.registry_sha256) != 32:
            raise ValueError("registry_sha256 must contain 32 bytes")
        if self.session_epoch <= 0:
            raise ValueError("session_epoch must be positive")
        if self.tick_ring_capacity <= 0:
            raise ValueError("tick_ring_capacity must be positive")
        if (
            self.tick_contiguous_published_sequence
            > self.tick_highest_published_sequence
        ):
            raise ValueError(
                "contiguous tick sequence cannot exceed highest published"
            )

    @property
    def identity(self) -> SessionIdentity:
        return SessionIdentity(self.run_id, self.session_epoch)

    @property
    def coverage_lost(self) -> bool:
        return (self.flags & 1) != 0

    @property
    def kline_enabled(self) -> bool:
        return (self.flags & 2) != 0

    @property
    def oldest_tick_sequence(self) -> int:
        if (
            self.tick_contiguous_published_sequence
            >= self.tick_ring_capacity
        ):
            return (
                self.tick_contiguous_published_sequence
                - self.tick_ring_capacity
                + 1
            )
        return 1


@dataclass(frozen=True, slots=True)
class Instrument:
    instrument_id: int
    market: int
    quantity_unit: int
    security_type: int
    asset_scope: int
    security_id_source: bytes
    security_id: bytes


@dataclass(frozen=True, slots=True)
class DecimalValue:
    raw: int
    normalized_p6: int
    scale: int
    valid: bool
    is_null: bool

    @property
    def p6(self) -> Optional[int]:
        return self.normalized_p6 if self.valid and not self.is_null else None


@dataclass(frozen=True, slots=True)
class QuantityValue:
    raw: int
    scale: int
    valid: bool
    is_null: bool

    @property
    def value(self) -> Optional[int]:
        return self.raw if self.valid and not self.is_null else None


@dataclass(frozen=True, slots=True)
class CommonRecord:
    record_schema_version: int
    record_bytes: int
    instrument_id: int
    registry_ordinal: int
    source_sequence: int
    ingress_sequence: int
    tick_stream_sequence: int
    vendor_sequence_id: int
    event_time_unix_ns: int
    recv_realtime_ns: int
    recv_monotonic_ns: int
    exchange_time_ns_since_midnight: int
    quality_flags: int
    market_notices: int
    source_stream_id: int
    trade_date: int
    vendor_local_time_raw: int
    source_slot: int
    event_kind: MarketEventKind
    market: Market
    quantity_unit: int
    security_type: int
    asset_scope: int


@dataclass(frozen=True, slots=True)
class BookLevel:
    price: DecimalValue
    quantity: QuantityValue
    order_count: int
    order_count_valid: bool


@dataclass(frozen=True, slots=True)
class BestQueue:
    total_order_count: int
    actual_revealed_count: int
    retained_count: int
    quantities: Tuple[QuantityValue, ...]


@dataclass(frozen=True, slots=True)
class Snapshot:
    common: CommonRecord
    trade_count: int
    image_status: int
    channel: int
    pre_close_price: DecimalValue
    open_price: DecimalValue
    high_price: DecimalValue
    low_price: DecimalValue
    last_price: DecimalValue
    close_price: DecimalValue
    trade_volume: QuantityValue
    turnover: DecimalValue
    total_bid_quantity: QuantityValue
    weighted_average_bid_price: DecimalValue
    total_ask_quantity: QuantityValue
    weighted_average_ask_price: DecimalValue
    high_limit_price: DecimalValue
    low_limit_price: DecimalValue
    iopv: DecimalValue
    open_interest: QuantityValue
    actual_bid_depth: int
    actual_ask_depth: int
    retained_bid_depth: int
    retained_ask_depth: int
    bids: Tuple[BookLevel, ...]
    asks: Tuple[BookLevel, ...]
    bid1_queue: BestQueue
    ask1_queue: BestQueue


@dataclass(frozen=True, slots=True)
class Tick:
    common: CommonRecord
    validity_bitmap: int
    channel: int
    native_event_sequence: int
    source_raw_code_1: int
    source_raw_code_2: int
    action: TickAction
    side: Side
    order_type: OrderType
    aggressor: Aggressor
    phase: TradingPhase
    price: DecimalValue
    quantity: QuantityValue
    trade_amount: DecimalValue
    matched_quantity: QuantityValue
    primary_order_id: int
    buy_order_id: int
    sell_order_id: int
    raw_type: bytes
    raw_tick_flag: bytes


@dataclass(frozen=True, slots=True)
class KLine:
    generation: int
    trade_date: int
    instrument_id: int
    window_id: int
    window_duration_ns: int
    window_start_ns_since_midnight: int
    window_end_ns_since_midnight: int
    window_start_unix_ns: int
    window_end_unix_ns: int
    open_price_p6: int
    high_price_p6: int
    low_price_p6: int
    close_price_p6: int
    volume_raw: int
    trade_count: int
    revision: int
    first_event_time_ns_since_midnight: int
    first_event_sequence: int
    first_source_sequence: int
    first_ingress_sequence: int
    last_event_time_ns_since_midnight: int
    last_event_sequence: int
    last_source_sequence: int
    last_ingress_sequence: int
    volume_scale: int
    quantity_unit: int


RecordT = TypeVar("RecordT")


@dataclass(frozen=True, slots=True)
class LatestResult(Generic[RecordT]):
    requested_instrument_id: int
    status: LatestStatus
    value: Optional[RecordT]
    requested_window_id: Optional[int] = None

    def __post_init__(self) -> None:
        if self.status is LatestStatus.AVAILABLE:
            if self.value is None:
                raise ValueError("an available result must contain a value")
        elif self.value is not None:
            raise ValueError("a non-available result cannot contain a value")

    @property
    def available(self) -> bool:
        return self.status is LatestStatus.AVAILABLE
