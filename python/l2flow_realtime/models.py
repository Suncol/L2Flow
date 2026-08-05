"""Immutable public models for the instrument-local L2Flow Wire V3 API."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import IntEnum
from types import MappingProxyType
from typing import Any, Mapping, Optional


_UINT32_MAX = (1 << 32) - 1
_UINT64_MAX = (1 << 64) - 1
_INT32_MIN = -(1 << 31)
_INT32_MAX = (1 << 31) - 1
_INT64_MIN = -(1 << 63)
_INT64_MAX = (1 << 63) - 1


class L2FlowRealtimeError(RuntimeError):
    """Base class for V3 client and CDC failures."""


class CursorMismatchError(L2FlowRealtimeError):
    """A cursor belongs to another session or instrument."""


class CdcProtocolError(L2FlowRealtimeError):
    """A CDC stream is non-contiguous, malformed, or contradictory."""


class RepairState(IntEnum):
    LIVE = 0
    REPAIR_REQUIRED = 1
    REBUILDING = 2
    CATCHING_UP = 3
    SOURCE_CONFLICT = 4
    UNRECOVERABLE = 5


class Dataset(IntEnum):
    FAST_TICK = 1
    DERIVED_EVENT = 2
    KLINE = 3


class EventMutationKind(IntEnum):
    INSERT = 0
    UPDATE = 1
    DELETE = 2
    RANGE_REPLACE_BEGIN = 3
    RANGE_REPLACE_CHUNK = 4
    RANGE_REPLACE_COMMIT = 5


class KLineMutationKind(IntEnum):
    UPSERT = 0
    DELETE = 1
    RANGE_REPLACE_BEGIN = 2
    RANGE_REPLACE_CHUNK = 3
    RANGE_REPLACE_COMMIT = 4


def _session_id(value: bytes) -> None:
    if not isinstance(value, bytes) or len(value) != 16 or not any(value):
        raise ValueError("session_id must be exactly 16 nonzero bytes")


def _uint32(value: int, name: str, *, nonzero: bool = False) -> None:
    minimum = 1 if nonzero else 0
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or value < minimum
        or value > _UINT32_MAX
    ):
        raise ValueError(f"{name} must be a valid uint32")


def _uint64(value: int, name: str, *, nonzero: bool = False) -> None:
    minimum = 1 if nonzero else 0
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or value < minimum
        or value > _UINT64_MAX
    ):
        raise ValueError(f"{name} must be a valid uint64")


def _signed_integer(
    value: int,
    name: str,
    minimum: int,
    maximum: int,
    *,
    positive: bool = False,
) -> None:
    lower = 1 if positive else minimum
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or value < lower
        or value > maximum
    ):
        suffix = "positive " if positive else ""
        bits = 32 if minimum == _INT32_MIN else 64
        raise ValueError(f"{name} must be a valid {suffix}int{bits}")


def _reject_reserved_values(
    values: Mapping[str, Any], reserved: frozenset[str], owner: str
) -> MappingProxyType:
    copied = dict(values)
    overlap = reserved.intersection(copied)
    if overlap:
        names = ", ".join(sorted(overlap))
        raise ValueError(f"{owner} values override reserved fields: {names}")
    return MappingProxyType(copied)


@dataclass(frozen=True, slots=True)
class FastTickCursor:
    session_id: bytes
    instrument_id: int
    next_arrival_row: int = 1

    def __post_init__(self) -> None:
        _session_id(self.session_id)
        _uint32(self.instrument_id, "instrument_id", nonzero=True)
        _uint64(self.next_arrival_row, "next_arrival_row", nonzero=True)


@dataclass(frozen=True, slots=True)
class EventChangeCursor:
    session_id: bytes
    instrument_id: int
    next_change_sequence: int = 1

    def __post_init__(self) -> None:
        _session_id(self.session_id)
        _uint32(self.instrument_id, "instrument_id", nonzero=True)
        _uint64(
            self.next_change_sequence,
            "next_change_sequence",
            nonzero=True,
        )


@dataclass(frozen=True, slots=True)
class KLineChangeCursor:
    session_id: bytes
    instrument_id: int
    next_change_sequence: int = 1

    def __post_init__(self) -> None:
        _session_id(self.session_id)
        _uint32(self.instrument_id, "instrument_id", nonzero=True)
        _uint64(
            self.next_change_sequence,
            "next_change_sequence",
            nonzero=True,
        )


@dataclass(frozen=True, order=True, slots=True)
class EventOrderKey:
    channel: int
    business_sequence: int
    source_event_ordinal: int = 0
    derived_event_ordinal: int = 0
    affected_order_id: int = 0

    def __post_init__(self) -> None:
        _signed_integer(
            self.channel,
            "channel",
            _INT32_MIN,
            _INT32_MAX,
            positive=True,
        )
        _signed_integer(
            self.business_sequence,
            "business_sequence",
            _INT64_MIN,
            _INT64_MAX,
            positive=True,
        )
        _uint32(self.source_event_ordinal, "source_event_ordinal")
        _uint32(self.derived_event_ordinal, "derived_event_ordinal")
        _signed_integer(
            self.affected_order_id,
            "affected_order_id",
            _INT64_MIN,
            _INT64_MAX,
        )


@dataclass(frozen=True, slots=True)
class EventUid:
    instrument_id: int
    channel: int
    business_sequence: int
    kind: int
    affected_order_id: int = 0
    occurrence: int = 0

    def __post_init__(self) -> None:
        _uint32(self.instrument_id, "instrument_id", nonzero=True)
        _signed_integer(
            self.channel,
            "channel",
            _INT32_MIN,
            _INT32_MAX,
            positive=True,
        )
        _signed_integer(
            self.business_sequence,
            "business_sequence",
            _INT64_MIN,
            _INT64_MAX,
            positive=True,
        )
        _uint32(self.kind, "kind", nonzero=True)
        _signed_integer(
            self.affected_order_id,
            "affected_order_id",
            _INT64_MIN,
            _INT64_MAX,
        )
        _uint32(self.occurrence, "occurrence")


@dataclass(frozen=True, slots=True)
class DerivedEvent:
    uid: EventUid
    order_key: EventOrderKey
    source_arrival_id: int
    values: Mapping[str, Any] = field(default_factory=dict)

    _RESERVED_FIELDS = frozenset(
        {
            "instrument_id",
            "channel",
            "business_sequence",
            "source_event_ordinal",
            "derived_event_ordinal",
            "affected_order_id",
            "event_kind",
            "occurrence",
            "source_arrival_id",
        }
    )

    def __post_init__(self) -> None:
        _uint64(self.source_arrival_id, "source_arrival_id", nonzero=True)
        if (
            self.uid.channel != self.order_key.channel
            or self.uid.business_sequence
            != self.order_key.business_sequence
            or self.uid.affected_order_id
            != self.order_key.affected_order_id
        ):
            raise ValueError("EventUid and EventOrderKey disagree")
        object.__setattr__(
            self,
            "values",
            _reject_reserved_values(
                self.values, self._RESERVED_FIELDS, "DerivedEvent"
            ),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "instrument_id": self.uid.instrument_id,
            "channel": self.order_key.channel,
            "business_sequence": self.order_key.business_sequence,
            "source_event_ordinal": self.order_key.source_event_ordinal,
            "derived_event_ordinal": self.order_key.derived_event_ordinal,
            "affected_order_id": self.order_key.affected_order_id,
            "event_kind": self.uid.kind,
            "occurrence": self.uid.occurrence,
            "source_arrival_id": self.source_arrival_id,
            **dict(self.values),
        }


@dataclass(frozen=True, order=True, slots=True)
class KLineBarKey:
    instrument_id: int
    window_id: int
    window_start_ns_since_midnight: int

    def __post_init__(self) -> None:
        _uint32(self.instrument_id, "instrument_id", nonzero=True)
        _uint32(self.window_id, "window_id", nonzero=True)
        _uint64(
            self.window_start_ns_since_midnight,
            "window_start_ns_since_midnight",
        )


@dataclass(frozen=True, slots=True)
class KLineBar:
    key: KLineBarKey
    revision: int
    values: Mapping[str, Any] = field(default_factory=dict)

    _RESERVED_FIELDS = frozenset(
        {
            "instrument_id",
            "window_id",
            "window_start_ns_since_midnight",
            "revision",
        }
    )

    def __post_init__(self) -> None:
        _uint64(self.revision, "revision", nonzero=True)
        object.__setattr__(
            self,
            "values",
            _reject_reserved_values(
                self.values, self._RESERVED_FIELDS, "KLineBar"
            ),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "instrument_id": self.key.instrument_id,
            "window_id": self.key.window_id,
            "window_start_ns_since_midnight": (
                self.key.window_start_ns_since_midnight
            ),
            "revision": self.revision,
            **dict(self.values),
        }


@dataclass(frozen=True, slots=True)
class FastTickRow:
    instrument_tick_sequence: int
    values: Mapping[str, Any]

    _RESERVED_FIELDS = frozenset({"instrument_tick_sequence"})

    def __post_init__(self) -> None:
        _uint64(
            self.instrument_tick_sequence,
            "instrument_tick_sequence",
            nonzero=True,
        )
        object.__setattr__(
            self,
            "values",
            _reject_reserved_values(
                self.values, self._RESERVED_FIELDS, "FastTickRow"
            ),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "instrument_tick_sequence": self.instrument_tick_sequence,
            **dict(self.values),
        }


@dataclass(frozen=True, slots=True)
class EventMutation:
    change_sequence: int
    kind: EventMutationKind
    transaction_id: int = 0
    uid: Optional[EventUid] = None
    row: Optional[DerivedEvent] = None
    replace_entire_instrument: bool = False
    range_begin: Optional[EventOrderKey] = None
    range_end_exclusive: Optional[EventOrderKey] = None
    replacement_rows: tuple[DerivedEvent, ...] = ()

    def __post_init__(self) -> None:
        _uint64(self.change_sequence, "change_sequence", nonzero=True)
        _uint64(self.transaction_id, "transaction_id")
        if not isinstance(self.replace_entire_instrument, bool):
            raise ValueError("replace_entire_instrument must be bool")
        object.__setattr__(self, "kind", EventMutationKind(self.kind))
        object.__setattr__(
            self, "replacement_rows", tuple(self.replacement_rows)
        )


@dataclass(frozen=True, slots=True)
class KLineMutation:
    change_sequence: int
    kind: KLineMutationKind
    key: Optional[KLineBarKey] = None
    bar: Optional[KLineBar] = None
    transaction_id: int = 0
    replace_entire_instrument: bool = False
    replacement_bars: tuple[KLineBar, ...] = ()

    def __post_init__(self) -> None:
        _uint64(self.change_sequence, "change_sequence", nonzero=True)
        _uint64(self.transaction_id, "transaction_id")
        if not isinstance(self.replace_entire_instrument, bool):
            raise ValueError("replace_entire_instrument must be bool")
        object.__setattr__(self, "kind", KLineMutationKind(self.kind))
        object.__setattr__(
            self, "replacement_bars", tuple(self.replacement_bars)
        )


@dataclass(frozen=True, slots=True)
class InstrumentStableStatus:
    dataset: Dataset
    repair_state: RepairState
    instrument_id: int
    stable_tail: int
    repair_through_arrival_id: int = 0

    def __post_init__(self) -> None:
        object.__setattr__(self, "dataset", Dataset(self.dataset))
        object.__setattr__(self, "repair_state", RepairState(self.repair_state))
        _uint32(self.instrument_id, "instrument_id", nonzero=True)
        _uint64(self.stable_tail, "stable_tail")
        _uint64(
            self.repair_through_arrival_id,
            "repair_through_arrival_id",
        )
