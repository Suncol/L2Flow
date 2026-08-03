"""Public, immutable models for the declared daily-catalog Wire V2 client."""

from __future__ import annotations

import struct
from dataclasses import dataclass
from enum import IntEnum, IntFlag
from typing import Optional


_UINT32_MAX = (1 << 32) - 1
_UINT64_MAX = (1 << 64) - 1
_EVENT_UID_MAGIC = b"L2EU"
_EVENT_UID_VERSION = 1
_EVENT_UID_WIRE = struct.Struct(">4sBBH16sQIQI")


class L2FlowRealtimeError(RuntimeError):
    """Base class for realtime client failures."""


class ProtocolError(L2FlowRealtimeError):
    """The control peer returned a malformed or incompatible message."""


class WireFormatError(L2FlowRealtimeError):
    """A native reader copy did not conform to Wire V2."""


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
    """The service cannot currently provide a trustworthy read."""


class StaleSessionError(UnavailableError):
    """The mapped session changed or its heartbeat became stale."""


class ClientClosedError(L2FlowRealtimeError):
    """An operation was attempted after the client was closed."""


class InconsistentReadError(UnavailableError):
    """A bounded native read could not obtain a coherent copy."""


class TickOverrunError(L2FlowRealtimeError):
    """The requested tick stream sequence has already been overwritten."""

    def __init__(self, expected_sequence: int, observed_sequence: int) -> None:
        super().__init__(
            "tick stream overrun: expected sequence "
            f"{expected_sequence}, observed {observed_sequence}"
        )
        self.expected_sequence = expected_sequence
        self.observed_sequence = observed_sequence


class CatalogScope(IntEnum):
    DECLARED_DAILY_A_SHARE = 2


class SelectionScope(IntEnum):
    CATALOG_ALL = 1
    AVAILABLE_ANY = 2
    SNAPSHOT_AVAILABLE = 3
    TICK_AVAILABLE = 4
    FACTOR_ELIGIBLE = 5
    BOUND = CATALOG_ALL
    OBSERVED_ANY = AVAILABLE_ANY


class InstrumentStatus(IntEnum):
    AVAILABLE = 0
    BOUND_NO_DATA = 1
    UNBOUND = 2
    INVALID_ID = 3


class LatestStatus(IntEnum):
    AVAILABLE = 0
    BOUND_NO_DATA = 1
    TYPE_UNAVAILABLE = 2
    UNBOUND = 3
    INVALID_INSTRUMENT_ID = 4
    UNKNOWN_WINDOW = 5
    INVALID_WINDOW_ID = 6


class InstrumentLookupStatus(IntEnum):
    FOUND = 0
    UNKNOWN = 1
    INVALID_MARKET = 2
    EMPTY_SECURITY_ID = 3


class BindingState(IntEnum):
    UNBOUND = 0
    BINDING = 1
    BOUND_NO_DATA = 2
    AVAILABLE = 3


class AvailabilityFlag(IntFlag):
    NONE = 0
    HAS_SNAPSHOT = 1 << 0
    HAS_TICK = 1 << 1
    HAS_KLINE = 1 << 2
    FACTOR_ELIGIBLE = 1 << 3


class KLineTemporalCoverage(IntEnum):
    """Temporal origin of the configured KLine input prefix."""

    DISABLED = 0
    FROM_OPEN = 1
    PROCESS_START_PARTIAL = 2


class TemporalCoverageKind(IntEnum):
    """Temporal origin of a history/event product.

    The aliases retain the terminology used by the live event-delta API while
    the canonical names match the shared history coverage ABI.
    """

    UNAVAILABLE = 0
    FROM_OPEN = 1
    PROCESS_START_PARTIAL = 2

    DISABLED = UNAVAILABLE
    FROM_MARKET_OPEN = FROM_OPEN
    FROM_PROCESS_START = PROCESS_START_PARTIAL


class EventUidScope(IntEnum):
    """Domain encoded into :class:`EventUid`."""

    SESSION_SOURCE_TICK = 1


class KLineCoverageFlag(IntFlag):
    """Per-bar Wire V2.5 KLine coverage qualifiers."""

    NONE = 0
    PROCESS_START_PARTIAL = 1 << 0
    NATURAL_WINDOW_LEFT_TRUNCATED = 1 << 1


class ServerState(IntEnum):
    INITIALIZING = 1
    ACTIVE = 2
    DRAINING = 3
    STOPPED_CLEAN = 4
    FAILED = 5
    LIVE_PARTIAL = 6


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


class TickProjectionFlag(IntFlag):
    NONE = 0
    RAW_TYPE_OMITTED = 1 << 0
    RAW_TICK_FLAG_OMITTED = 1 << 1


def _uint64(value: int, field: str, *, nonzero: bool = False) -> None:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or value < (1 if nonzero else 0)
        or value > _UINT64_MAX
    ):
        qualifier = "nonzero " if nonzero else ""
        raise ValueError(f"{field} must be a {qualifier}uint64")


def _uint32(value: int, field: str, *, nonzero: bool = False) -> None:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or value < (1 if nonzero else 0)
        or value > _UINT32_MAX
    ):
        qualifier = "nonzero " if nonzero else ""
        raise ValueError(f"{field} must be a {qualifier}uint32")


def _digest(value: bytes, length: int, field: str) -> None:
    if not isinstance(value, bytes) or len(value) != length:
        raise ValueError(f"{field} must contain exactly {length} bytes")


def _validate_counts(
    capacity: int,
    bound_count: int,
    available_count: int,
    snapshot_available_count: int,
    tick_available_count: int,
    factor_eligible_count: int,
) -> None:
    for value, name in (
        (capacity, "capacity"),
        (bound_count, "bound_count"),
        (available_count, "available_count"),
        (snapshot_available_count, "snapshot_available_count"),
        (tick_available_count, "tick_available_count"),
        (factor_eligible_count, "factor_eligible_count"),
    ):
        _uint32(value, name)
    if not (
        factor_eligible_count
        <= snapshot_available_count
        <= available_count
        <= bound_count
        <= capacity
    ):
        raise ValueError("daily-catalog counts are inconsistent")
    if tick_available_count > available_count:
        raise ValueError("tick_available_count exceeds available_count")


@dataclass(frozen=True, slots=True)
class SessionIdentity:
    """The scope required to interpret every numeric instrument ID."""

    run_id: bytes
    session_epoch: int

    def __post_init__(self) -> None:
        _digest(self.run_id, 16, "run_id")
        if not any(self.run_id):
            raise ValueError("run_id must be nonzero")
        _uint64(self.session_epoch, "session_epoch", nonzero=True)


@dataclass(frozen=True, slots=True)
class HistoryCoverageInfo:
    """Session-scoped temporal coverage for a history/event product.

    ``coverage_start_unix_ns`` is deliberately independent of KLine window
    coverage. A process-start product may leave it as ``None`` when its exact
    first-event boundary is not present in that product's own ABI.
    """

    run_id: bytes
    session_epoch: int
    trade_date: int
    coverage_kind: TemporalCoverageKind
    coverage_start_unix_ns: Optional[int] = None

    def __post_init__(self) -> None:
        _digest(self.run_id, 16, "run_id")
        if not any(self.run_id):
            raise ValueError("run_id must be nonzero")
        _uint64(self.session_epoch, "session_epoch", nonzero=True)
        _uint32(self.trade_date, "trade_date", nonzero=True)
        try:
            coverage_kind = TemporalCoverageKind(self.coverage_kind)
        except (TypeError, ValueError) as error:
            raise ValueError("coverage_kind is invalid") from error
        if self.coverage_start_unix_ns is not None:
            _uint64(
                self.coverage_start_unix_ns,
                "coverage_start_unix_ns",
                nonzero=True,
            )
            if (
                coverage_kind
                is not TemporalCoverageKind.PROCESS_START_PARTIAL
            ):
                raise ValueError(
                    "only PROCESS_START_PARTIAL may carry a coverage start"
                )
        object.__setattr__(self, "coverage_kind", coverage_kind)

    @property
    def identity(self) -> SessionIdentity:
        return SessionIdentity(self.run_id, self.session_epoch)

    @property
    def available(self) -> bool:
        return self.coverage_kind is not TemporalCoverageKind.UNAVAILABLE

    @property
    def coverage_from_open(self) -> bool:
        return self.coverage_kind is TemporalCoverageKind.FROM_OPEN

    @property
    def process_start_partial(self) -> bool:
        return (
            self.coverage_kind
            is TemporalCoverageKind.PROCESS_START_PARTIAL
        )


@dataclass(frozen=True, slots=True)
class EventUid:
    """Collision-free identity of one event emitted by one source tick.

    Dense product-local event sequences are intentionally absent. The final
    coordinate is a zero-based ordinal in the deterministic emission order of
    one source tick. Consequently an event UID must not be constructed until
    the producer supplies that ordinal explicitly.

    Source-free trading-day finalization rows have an all-zero anchor and are
    outside this scope; they remain UID-unavailable unless a separately
    versioned boundary-event identity domain is introduced.

    ``bytes(uid)`` is a fixed 48-byte, network-order structural encoding, not
    a hash. Equality therefore has no hash-collision semantics.
    """

    scope: EventUidScope
    session_identity: SessionIdentity
    instrument_id: int
    tick_stream_sequence: int
    source_tick_event_ordinal: int

    def __post_init__(self) -> None:
        try:
            scope = EventUidScope(self.scope)
        except (TypeError, ValueError) as error:
            raise ValueError("event UID scope is invalid") from error
        if not isinstance(self.session_identity, SessionIdentity):
            raise TypeError("session_identity must be SessionIdentity")
        _uint32(self.instrument_id, "instrument_id", nonzero=True)
        _uint64(
            self.tick_stream_sequence,
            "tick_stream_sequence",
            nonzero=True,
        )
        _uint32(
            self.source_tick_event_ordinal,
            "source_tick_event_ordinal",
        )
        object.__setattr__(self, "scope", scope)

    @property
    def wire(self) -> bytes:
        return _EVENT_UID_WIRE.pack(
            _EVENT_UID_MAGIC,
            _EVENT_UID_VERSION,
            int(self.scope),
            0,
            self.session_identity.run_id,
            self.session_identity.session_epoch,
            self.instrument_id,
            self.tick_stream_sequence,
            self.source_tick_event_ordinal,
        )

    def __bytes__(self) -> bytes:
        return self.wire

    @classmethod
    def from_wire(cls, value: bytes) -> "EventUid":
        if not isinstance(value, bytes):
            raise TypeError("event UID wire value must be exact bytes")
        if len(value) != _EVENT_UID_WIRE.size:
            raise ValueError(
                f"event UID wire value must contain {_EVENT_UID_WIRE.size} bytes"
            )
        (
            magic,
            version,
            scope,
            reserved,
            run_id,
            session_epoch,
            instrument_id,
            tick_stream_sequence,
            source_tick_event_ordinal,
        ) = _EVENT_UID_WIRE.unpack(value)
        if (
            magic != _EVENT_UID_MAGIC
            or version != _EVENT_UID_VERSION
            or reserved != 0
        ):
            raise ValueError("event UID wire header is invalid")
        return cls(
            scope=EventUidScope(scope),
            session_identity=SessionIdentity(run_id, session_epoch),
            instrument_id=instrument_id,
            tick_stream_sequence=tick_stream_sequence,
            source_tick_event_ordinal=source_tick_event_ordinal,
        )


@dataclass(frozen=True, slots=True)
class KLineCoverageInfo:
    """Immutable temporal-coverage metadata for one mapped session."""

    session_epoch: int
    coverage_start_unix_ns: int
    coverage_kind: KLineTemporalCoverage

    def __post_init__(self) -> None:
        _uint64(self.session_epoch, "session_epoch", nonzero=True)
        _uint64(
            self.coverage_start_unix_ns,
            "coverage_start_unix_ns",
        )
        coverage_kind = KLineTemporalCoverage(self.coverage_kind)
        if (
            coverage_kind
            is KLineTemporalCoverage.PROCESS_START_PARTIAL
        ) != (self.coverage_start_unix_ns != 0):
            raise ValueError(
                "only PROCESS_START_PARTIAL requires a nonzero "
                "coverage_start_unix_ns"
            )
        object.__setattr__(self, "coverage_kind", coverage_kind)


@dataclass(frozen=True, slots=True)
class SessionInfo:
    run_id: bytes
    layout_digest: bytes
    catalog_digest: bytes
    session_epoch: int
    catalog_generation: int
    data_state_generation: int
    accepted_sequence: int
    applied_sequence: int
    processing_lag_records: int
    tick_ring_capacity: int
    tick_highest_published_sequence: int
    tick_contiguous_published_sequence: int
    kline_generation: int
    heartbeat_monotonic_ns: int
    published_records: int
    trade_date: int
    server_state: ServerState
    flags: int
    capacity: int
    window_count: int
    catalog_scope: CatalogScope
    coverage_complete: bool
    bound_count: int
    available_count: int
    snapshot_available_count: int
    tick_available_count: int
    factor_eligible_count: int
    catalog_trade_date: int
    catalog_version: int

    def __post_init__(self) -> None:
        _digest(self.run_id, 16, "run_id")
        _digest(self.layout_digest, 32, "layout_digest")
        _digest(self.catalog_digest, 32, "catalog_digest")
        if not any(self.run_id):
            raise ValueError("run_id must be nonzero")
        if not any(self.catalog_digest):
            raise ValueError("catalog_digest must be nonzero")
        _uint64(self.session_epoch, "session_epoch", nonzero=True)
        for value, name in (
            (self.catalog_generation, "catalog_generation"),
            (self.data_state_generation, "data_state_generation"),
            (self.accepted_sequence, "accepted_sequence"),
            (self.applied_sequence, "applied_sequence"),
            (self.processing_lag_records, "processing_lag_records"),
            (self.tick_ring_capacity, "tick_ring_capacity"),
            (
                self.tick_highest_published_sequence,
                "tick_highest_published_sequence",
            ),
            (
                self.tick_contiguous_published_sequence,
                "tick_contiguous_published_sequence",
            ),
            (self.kline_generation, "kline_generation"),
            (self.heartbeat_monotonic_ns, "heartbeat_monotonic_ns"),
            (self.published_records, "published_records"),
            (self.catalog_version, "catalog_version"),
        ):
            _uint64(value, name)
        if self.accepted_sequence == _UINT64_MAX:
            raise ValueError("accepted_sequence uses the reserved sentinel")
        for value, name in (
            (self.trade_date, "trade_date"),
            (self.catalog_trade_date, "catalog_trade_date"),
            (self.flags, "flags"),
            (self.window_count, "window_count"),
        ):
            _uint32(value, name)
        if self.capacity == 0:
            raise ValueError("capacity must be nonzero")
        if self.tick_ring_capacity == 0:
            raise ValueError("tick_ring_capacity must be nonzero")
        if self.flags & ~0x7F:
            raise ValueError("flags contain an unknown Wire V2 bit")
        if self.flags & 0x78 and not self.flags & 0x4:
            raise ValueError(
                "strong recovered-prefix flags require coverage_from_open"
            )
        if self.flags & 0x10 and not self.flags & 0x2:
            raise ValueError(
                "full_day_kline_valid requires KLine to be enabled"
            )
        server_state = ServerState(self.server_state)
        if (
            server_state is ServerState.ACTIVE
            and not self.flags & (1 << 2)
        ):
            raise ValueError("ACTIVE requires coverage_from_open")
        if (
            server_state is ServerState.LIVE_PARTIAL
            and self.flags & 0x7C
        ):
            raise ValueError(
                "LIVE_PARTIAL cannot carry from-open or strong prefix flags"
            )
        if (
            CatalogScope(self.catalog_scope)
            is not CatalogScope.DECLARED_DAILY_A_SHARE
        ):
            raise ValueError(
                "Wire V2 catalog scope must be DECLARED_DAILY_A_SHARE"
            )
        if self.coverage_complete is not True:
            raise ValueError("daily A-share catalog coverage must be complete")
        if self.catalog_trade_date != self.trade_date:
            raise ValueError("catalog_trade_date does not match trade_date")
        if self.catalog_version == 0:
            raise ValueError("catalog_version must be nonzero")
        if self.catalog_generation != 1:
            raise ValueError("frozen catalog_generation must equal one")
        if self.bound_count != self.capacity:
            raise ValueError("daily catalog must bind every physical row")
        if self.applied_sequence > self.accepted_sequence:
            raise ValueError("applied_sequence exceeds accepted_sequence")
        if (
            self.processing_lag_records
            != self.accepted_sequence - self.applied_sequence
        ):
            raise ValueError(
                "processing_lag_records does not match sequence watermarks"
            )
        if (
            self.tick_contiguous_published_sequence
            > self.tick_highest_published_sequence
        ):
            raise ValueError(
                "contiguous tick sequence exceeds highest published"
            )
        _validate_counts(
            self.capacity,
            self.bound_count,
            self.available_count,
            self.snapshot_available_count,
            self.tick_available_count,
            self.factor_eligible_count,
        )
        object.__setattr__(self, "server_state", server_state)
        object.__setattr__(
            self, "catalog_scope", CatalogScope(self.catalog_scope)
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
    def kline_temporal_coverage(self) -> KLineTemporalCoverage:
        if not self.kline_enabled:
            return KLineTemporalCoverage.DISABLED
        if self.coverage_from_open:
            return KLineTemporalCoverage.FROM_OPEN
        return KLineTemporalCoverage.PROCESS_START_PARTIAL

    @property
    def coverage_from_open(self) -> bool:
        return (self.flags & (1 << 2)) != 0

    @property
    def startup_prefix_recovered(self) -> bool:
        return (self.flags & (1 << 3)) != 0

    @property
    def full_day_kline_valid(self) -> bool:
        return (self.flags & (1 << 4)) != 0

    @property
    def full_day_factor_valid(self) -> bool:
        return (self.flags & (1 << 5)) != 0

    @property
    def certified_prefix_valid(self) -> bool:
        return (self.flags & (1 << 6)) != 0


@dataclass(frozen=True, slots=True)
class InstrumentKey:
    """An exact opaque-byte dynamic-directory lookup key."""

    market: int
    security_id_source: bytes
    security_id: bytes

    def __post_init__(self) -> None:
        if (
            not isinstance(self.market, int)
            or isinstance(self.market, bool)
            or not 0 <= self.market <= 0xFF
        ):
            raise ValueError("market must fit uint8")
        if not isinstance(self.security_id_source, bytes):
            raise TypeError("security_id_source must be exact bytes")
        if not isinstance(self.security_id, bytes):
            raise TypeError("security_id must be exact bytes")


@dataclass(frozen=True, slots=True)
class InstrumentLookupResult:
    session_epoch: int
    key: InstrumentKey
    status: InstrumentLookupStatus
    instrument_id: int

    def __post_init__(self) -> None:
        _uint64(self.session_epoch, "session_epoch", nonzero=True)
        object.__setattr__(
            self, "status", InstrumentLookupStatus(self.status)
        )
        _uint32(
            self.instrument_id,
            "instrument_id",
            nonzero=self.status is InstrumentLookupStatus.FOUND,
        )
        if (
            self.status is not InstrumentLookupStatus.FOUND
            and self.instrument_id != 0
        ):
            raise ValueError("unresolved keys must return instrument_id zero")


@dataclass(frozen=True, slots=True)
class Instrument:
    """One session-scoped point lookup, including semantic miss states."""

    session_epoch: int
    instrument_id: int
    status: InstrumentStatus
    ordinal: Optional[int] = None
    binding_state: Optional[BindingState] = None
    availability_flags: Optional[AvailabilityFlag] = None
    market: Optional[int] = None
    quantity_unit: Optional[int] = None
    security_type: Optional[int] = None
    asset_scope: Optional[int] = None
    security_id_source: Optional[bytes] = None
    security_id: Optional[bytes] = None
    first_ingress_sequence: Optional[int] = None
    last_ingress_sequence: Optional[int] = None

    def __post_init__(self) -> None:
        _uint64(self.session_epoch, "session_epoch", nonzero=True)
        _uint32(self.instrument_id, "instrument_id")
        object.__setattr__(self, "status", InstrumentStatus(self.status))
        row_fields = (
            self.ordinal,
            self.binding_state,
            self.availability_flags,
            self.market,
            self.quantity_unit,
            self.security_type,
            self.asset_scope,
            self.security_id_source,
            self.security_id,
            self.first_ingress_sequence,
            self.last_ingress_sequence,
        )
        if self.status in (
            InstrumentStatus.UNBOUND,
            InstrumentStatus.INVALID_ID,
        ):
            if any(value is not None for value in row_fields):
                raise ValueError("unbound/invalid instrument has no row")
            return
        if any(value is None for value in row_fields):
            raise ValueError("bound instrument is missing row fields")
        if self.instrument_id == 0 or self.ordinal != self.instrument_id - 1:
            raise ValueError("instrument_id and ordinal disagree")
        binding_state = BindingState(self.binding_state)
        availability = AvailabilityFlag(self.availability_flags)
        if int(availability) & ~0xF:
            raise ValueError("instrument has unknown availability flags")
        if (
            availability & AvailabilityFlag.FACTOR_ELIGIBLE
            and not availability & AvailabilityFlag.HAS_SNAPSHOT
        ):
            raise ValueError("factor eligibility requires snapshot data")
        expected_binding = (
            BindingState.AVAILABLE
            if self.status is InstrumentStatus.AVAILABLE
            else BindingState.BOUND_NO_DATA
        )
        if binding_state is not expected_binding:
            raise ValueError("instrument status and binding state disagree")
        if expected_binding is BindingState.BOUND_NO_DATA:
            if (
                availability
                or self.first_ingress_sequence
                or self.last_ingress_sequence
            ):
                raise ValueError("BOUND_NO_DATA row contains availability")
        elif (
            not availability
            or not self.first_ingress_sequence
            or self.last_ingress_sequence < self.first_ingress_sequence
        ):
            raise ValueError("AVAILABLE row has invalid ingress bounds")
        if not isinstance(self.security_id_source, bytes):
            raise TypeError("security_id_source must be exact bytes")
        if not isinstance(self.security_id, bytes) or not self.security_id:
            raise ValueError("security_id must be nonempty exact bytes")
        object.__setattr__(self, "binding_state", binding_state)
        object.__setattr__(self, "availability_flags", availability)


@dataclass(frozen=True, slots=True)
class DecimalValue:
    raw: int
    normalized_p6: int
    scale: int
    valid: bool
    is_null: bool


@dataclass(frozen=True, slots=True)
class QuantityValue:
    raw: int
    scale: int
    valid: bool
    is_null: bool


@dataclass(frozen=True, slots=True)
class CommonRecord:
    record_schema_version: int
    record_bytes: int
    instrument_id: int
    ordinal: int
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
class LatestSnapshot:
    session_epoch: int
    instrument_id: int
    status: LatestStatus
    common: Optional[CommonRecord] = None
    last_price: Optional[DecimalValue] = None
    wire_payload: Optional[bytes] = None

    def __post_init__(self) -> None:
        _validate_latest(
            self.session_epoch,
            self.instrument_id,
            self.status,
            self.common,
            self.wire_payload,
            3104,
        )
        if (
            LatestStatus(self.status) is LatestStatus.AVAILABLE
        ) != (self.last_price is not None):
            raise ValueError("snapshot price presence disagrees with status")
        object.__setattr__(self, "status", LatestStatus(self.status))


@dataclass(frozen=True, slots=True)
class LatestTick:
    session_epoch: int
    instrument_id: int
    status: LatestStatus
    common: Optional[CommonRecord] = None
    price: Optional[DecimalValue] = None
    quantity: Optional[QuantityValue] = None
    action: Optional[TickAction] = None
    side: Optional[Side] = None
    projection_flags: Optional[TickProjectionFlag] = None
    wire_payload: Optional[bytes] = None

    def __post_init__(self) -> None:
        _validate_latest(
            self.session_epoch,
            self.instrument_id,
            self.status,
            self.common,
            self.wire_payload,
            336,
        )
        present = (
            self.price,
            self.quantity,
            self.action,
            self.side,
            self.projection_flags,
        )
        available = LatestStatus(self.status) is LatestStatus.AVAILABLE
        if available != all(value is not None for value in present):
            raise ValueError("tick fields presence disagrees with status")
        if not available and any(value is not None for value in present):
            raise ValueError("unavailable tick contains payload fields")
        if available:
            object.__setattr__(self, "action", TickAction(self.action))
            object.__setattr__(self, "side", Side(self.side))
            object.__setattr__(
                self,
                "projection_flags",
                TickProjectionFlag(self.projection_flags),
            )
        object.__setattr__(self, "status", LatestStatus(self.status))


@dataclass(frozen=True, slots=True)
class LatestKLine:
    session_epoch: int
    instrument_id: int
    window_id: int
    status: LatestStatus
    generation: Optional[int] = None
    trade_date: Optional[int] = None
    window_duration_ns: Optional[int] = None
    window_start_ns_since_midnight: Optional[int] = None
    window_end_ns_since_midnight: Optional[int] = None
    window_start_unix_ns: Optional[int] = None
    window_end_unix_ns: Optional[int] = None
    open_price_p6: Optional[int] = None
    high_price_p6: Optional[int] = None
    low_price_p6: Optional[int] = None
    close_price_p6: Optional[int] = None
    volume_raw: Optional[int] = None
    trade_count: Optional[int] = None
    revision: Optional[int] = None
    first_event_time_ns_since_midnight: Optional[int] = None
    first_event_sequence: Optional[int] = None
    first_source_sequence: Optional[int] = None
    first_ingress_sequence: Optional[int] = None
    last_event_time_ns_since_midnight: Optional[int] = None
    last_event_sequence: Optional[int] = None
    last_source_sequence: Optional[int] = None
    last_ingress_sequence: Optional[int] = None
    volume_scale: Optional[int] = None
    quantity_unit: Optional[int] = None
    wire_payload: Optional[bytes] = None
    # Additive V2.4 metadata is intentionally last so existing positional
    # construction of the older payload fields keeps its meaning.
    coverage_flags: Optional[KLineCoverageFlag] = None

    def __post_init__(self) -> None:
        _uint64(self.session_epoch, "session_epoch", nonzero=True)
        _uint32(self.instrument_id, "instrument_id")
        _uint32(self.window_id, "window_id")
        status = LatestStatus(self.status)
        values = tuple(
            getattr(self, name)
            for name in self.__dataclass_fields__
            if name
            not in (
                "session_epoch",
                "instrument_id",
                "window_id",
                "status",
                "wire_payload",
            )
        )
        available = status is LatestStatus.AVAILABLE
        if available != all(value is not None for value in values):
            raise ValueError("KLine fields presence disagrees with status")
        if not available and any(value is not None for value in values):
            raise ValueError("unavailable KLine contains payload fields")
        if available:
            if (
                not isinstance(self.wire_payload, bytes)
                or len(self.wire_payload) != 192
            ):
                raise ValueError("available KLine requires 192 wire bytes")
            if (
                not isinstance(self.coverage_flags, int)
                or isinstance(self.coverage_flags, bool)
                or self.coverage_flags < 0
                or self.coverage_flags > _UINT32_MAX
            ):
                raise ValueError("KLine coverage_flags must fit uint32")
            coverage_flags = int(self.coverage_flags)
            known_coverage_flags = int(
                KLineCoverageFlag.PROCESS_START_PARTIAL
                | KLineCoverageFlag.NATURAL_WINDOW_LEFT_TRUNCATED
            )
            if coverage_flags & ~known_coverage_flags:
                raise ValueError("KLine coverage_flags contain unknown bits")
            if (
                coverage_flags
                & int(KLineCoverageFlag.NATURAL_WINDOW_LEFT_TRUNCATED)
                and not coverage_flags
                & int(KLineCoverageFlag.PROCESS_START_PARTIAL)
            ):
                raise ValueError(
                    "left-truncated KLine requires process-start coverage"
                )
            object.__setattr__(
                self,
                "coverage_flags",
                KLineCoverageFlag(coverage_flags),
            )
        elif self.wire_payload is not None:
            raise ValueError("unavailable KLine has a wire payload")
        object.__setattr__(self, "status", status)

    @property
    def temporal_coverage(self) -> Optional[KLineTemporalCoverage]:
        """Coverage origin for an available bar, otherwise ``None``."""

        if self.status is not LatestStatus.AVAILABLE:
            return None
        if self.coverage_flags & KLineCoverageFlag.PROCESS_START_PARTIAL:
            return KLineTemporalCoverage.PROCESS_START_PARTIAL
        return KLineTemporalCoverage.FROM_OPEN

    @property
    def process_start_partial(self) -> bool:
        return bool(
            self.coverage_flags is not None
            and self.coverage_flags
            & KLineCoverageFlag.PROCESS_START_PARTIAL
        )

    @property
    def natural_window_left_truncated(self) -> bool:
        return bool(
            self.coverage_flags is not None
            and self.coverage_flags
            & KLineCoverageFlag.NATURAL_WINDOW_LEFT_TRUNCATED
        )


def _validate_latest(
    session_epoch: int,
    instrument_id: int,
    status: LatestStatus,
    common: Optional[CommonRecord],
    wire_payload: Optional[bytes],
    expected_bytes: int,
) -> None:
    _uint64(session_epoch, "session_epoch", nonzero=True)
    _uint32(instrument_id, "instrument_id")
    status = LatestStatus(status)
    available = status is LatestStatus.AVAILABLE
    if available != (common is not None):
        raise ValueError("record presence disagrees with latest status")
    if available:
        if common.instrument_id != instrument_id:
            raise ValueError("record instrument_id does not match request")
        if (
            not isinstance(wire_payload, bytes)
            or len(wire_payload) != expected_bytes
        ):
            raise ValueError(
                f"available record requires {expected_bytes} wire bytes"
            )
    elif wire_payload is not None:
        raise ValueError("unavailable record has a wire payload")


@dataclass(frozen=True, slots=True)
class SelectionEnvelope:
    """A coherent daily-catalog selection and its session-scoped IDs."""

    run_id: bytes
    catalog_digest: bytes
    session_epoch: int
    catalog_generation: int
    data_state_generation: int
    accepted_sequence: int
    applied_sequence: int
    processing_lag_records: int
    capacity: int
    catalog_scope: CatalogScope
    coverage_complete: bool
    bound_count: int
    available_count: int
    snapshot_available_count: int
    tick_available_count: int
    factor_eligible_count: int
    selection_scope: SelectionScope
    returned_row_count: int
    instrument_ids: tuple[int, ...]

    def __post_init__(self) -> None:
        _digest(self.run_id, 16, "run_id")
        _digest(self.catalog_digest, 32, "catalog_digest")
        if not any(self.run_id):
            raise ValueError("run_id must be nonzero")
        _uint64(self.session_epoch, "session_epoch", nonzero=True)
        for value, name in (
            (self.catalog_generation, "catalog_generation"),
            (self.data_state_generation, "data_state_generation"),
            (self.accepted_sequence, "accepted_sequence"),
            (self.applied_sequence, "applied_sequence"),
            (self.processing_lag_records, "processing_lag_records"),
        ):
            _uint64(value, name)
        if self.accepted_sequence == _UINT64_MAX:
            raise ValueError("accepted_sequence uses the reserved sentinel")
        scope = CatalogScope(self.catalog_scope)
        selection = SelectionScope(self.selection_scope)
        if scope is not CatalogScope.DECLARED_DAILY_A_SHARE:
            raise ValueError(
                "selection must use DECLARED_DAILY_A_SHARE catalog scope"
            )
        if self.coverage_complete is not True:
            raise ValueError(
                "daily A-share selection must declare complete coverage"
            )
        if self.catalog_generation != 1:
            raise ValueError("frozen catalog_generation must equal one")
        if self.bound_count != self.capacity:
            raise ValueError("daily catalog must bind every physical row")
        if self.applied_sequence > self.accepted_sequence:
            raise ValueError("applied_sequence exceeds accepted_sequence")
        if (
            self.processing_lag_records
            != self.accepted_sequence - self.applied_sequence
        ):
            raise ValueError(
                "processing_lag_records does not match sequence watermarks"
            )
        _validate_counts(
            self.capacity,
            self.bound_count,
            self.available_count,
            self.snapshot_available_count,
            self.tick_available_count,
            self.factor_eligible_count,
        )
        if self.capacity == 0:
            raise ValueError("capacity must be nonzero")
        expected = {
            SelectionScope.CATALOG_ALL: self.bound_count,
            SelectionScope.AVAILABLE_ANY: self.available_count,
            SelectionScope.SNAPSHOT_AVAILABLE:
                self.snapshot_available_count,
            SelectionScope.TICK_AVAILABLE: self.tick_available_count,
            SelectionScope.FACTOR_ELIGIBLE: self.factor_eligible_count,
        }[selection]
        if self.returned_row_count != expected:
            raise ValueError("selection row count disagrees with its scope")
        if len(self.instrument_ids) != self.returned_row_count:
            raise ValueError("instrument ID count disagrees with envelope")
        if tuple(sorted(set(self.instrument_ids))) != self.instrument_ids:
            raise ValueError("selected instrument IDs are not ordered unique")
        if any(value <= 0 or value > self.capacity for value in self.instrument_ids):
            raise ValueError("selected instrument ID is outside capacity")
        object.__setattr__(self, "catalog_scope", scope)
        object.__setattr__(self, "selection_scope", selection)
