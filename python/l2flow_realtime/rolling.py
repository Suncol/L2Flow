"""Atomic, bounded rolling transactions over Wire V2 instrument deltas."""

from __future__ import annotations

import copy
import threading
from dataclasses import dataclass
from itertools import chain
from typing import (
    Generic,
    Optional,
    Protocol,
    TypeVar,
    runtime_checkable,
)

from ._history_columns import TICK_COLUMN_SPECS, tick_columns
from .checkpoint import InstrumentTickDeltaCheckpoint
from .instrument_delta import (
    InstrumentTickDeltaCursor,
    InstrumentTickDeltaPage,
)
from .models import (
    ClientClosedError,
    StaleSessionError,
    UnavailableError,
    WireFormatError,
)
from .wire import TICK_BYTES


FactorStateT = TypeVar("FactorStateT")
FactorValueT = TypeVar("FactorValueT")
_UINT32_MAX = 0xFFFFFFFF
_UINT64_MAX = 0xFFFFFFFFFFFFFFFF
_NO_FACTOR_VALUE = object()


def _positive_uint(value: object, field: str, maximum: int) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{field} must be an integer")
    if value <= 0 or value > maximum:
        bits = 32 if maximum == _UINT32_MAX else 64
        raise ValueError(f"{field} must be a positive uint{bits}")
    return value


def _uint64(value: object, field: str) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or value < 0
        or value > _UINT64_MAX
    ):
        raise ValueError(f"{field} must fit uint64")
    return value


@dataclass(frozen=True, slots=True)
class _TickSegment:
    payload: bytes
    first_record: int
    record_count: int

    def __post_init__(self) -> None:
        if not isinstance(self.payload, bytes):
            raise TypeError("tick segment payload must be bytes")
        if (
            self.first_record < 0
            or self.record_count <= 0
            or (
                self.first_record + self.record_count
            ) * TICK_BYTES
            > len(self.payload)
            or len(self.payload) % TICK_BYTES
        ):
            raise ValueError("tick segment is not record-aligned")

    def view(self) -> memoryview:
        begin = self.first_record * TICK_BYTES
        end = begin + self.record_count * TICK_BYTES
        return memoryview(self.payload)[begin:end]


def _split_segments(
    segments: tuple[_TickSegment, ...],
    prefix_records: int,
) -> tuple[tuple[_TickSegment, ...], tuple[_TickSegment, ...]]:
    """Split immutable segments without copying their payload bytes."""

    if prefix_records < 0:
        raise ValueError("prefix_records must be nonnegative")
    prefix: list[_TickSegment] = []
    suffix: list[_TickSegment] = []
    remaining = prefix_records
    for segment in segments:
        if remaining == 0:
            suffix.append(segment)
            continue
        if remaining >= segment.record_count:
            prefix.append(segment)
            remaining -= segment.record_count
            continue
        prefix.append(
            _TickSegment(
                segment.payload,
                segment.first_record,
                remaining,
            )
        )
        suffix.append(
            _TickSegment(
                segment.payload,
                segment.first_record + remaining,
                segment.record_count - remaining,
            )
        )
        remaining = 0
    if remaining:
        raise ValueError("prefix exceeds the segmented tick rows")
    return tuple(prefix), tuple(suffix)


class InstrumentTickColumns:
    """Immutable, segmented Wire V2 tick rows with lazy selected columns."""

    __slots__ = (
        "_segments",
        "_row_count",
        "_column_cache",
        "_dense_cache",
    )

    def __init__(self, wire_records: bytes = b"") -> None:
        if not isinstance(wire_records, bytes):
            raise TypeError("wire_records must be bytes")
        if len(wire_records) % TICK_BYTES:
            raise ValueError("wire_records are not tick-record aligned")
        count = len(wire_records) // TICK_BYTES
        self._segments = (
            (_TickSegment(wire_records, 0, count),)
            if count
            else ()
        )
        self._row_count = count
        self._column_cache: dict[str, tuple[object, ...]] = {}
        self._dense_cache: Optional[bytes] = wire_records

    @classmethod
    def _from_segments(
        cls, segments: tuple[_TickSegment, ...]
    ) -> "InstrumentTickColumns":
        instance = cls.__new__(cls)
        instance._segments = segments
        instance._row_count = sum(
            segment.record_count for segment in segments
        )
        instance._column_cache = {}
        instance._dense_cache = b"" if not segments else None
        return instance

    def __len__(self) -> int:
        return self._row_count

    def __getitem__(self, name: str) -> tuple[object, ...]:
        return self.read_columns(name)[name]

    @property
    def row_count(self) -> int:
        return self._row_count

    @property
    def segment_count(self) -> int:
        return len(self._segments)

    @property
    def materialized_column_count(self) -> int:
        return len(self._column_cache)

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(TICK_COLUMN_SPECS)

    def keys(self) -> tuple[str, ...]:
        return self.column_names

    @property
    def wire_records(self) -> bytes:
        """Return dense rows, copying only when the window spans segments."""

        cached = self._dense_cache
        if cached is not None:
            return cached
        if len(self._segments) == 1:
            dense = self._segments[0].view().tobytes()
        else:
            dense = b"".join(
                segment.view() for segment in self._segments
            )
        self._dense_cache = dense
        return dense

    def read_columns(
        self, *names: str
    ) -> dict[str, tuple[object, ...]]:
        """Materialize selected columns with one fused scan per segment."""

        if not names:
            raise ValueError("at least one column name is required")
        if any(not isinstance(name, str) for name in names):
            raise TypeError("column names must be strings")
        if len(set(names)) != len(names):
            raise ValueError("column names must be unique")
        for name in names:
            if name not in TICK_COLUMN_SPECS:
                raise KeyError(name)

        missing = [
            name for name in names if name not in self._column_cache
        ]
        if missing:
            pieces: dict[str, list[tuple[object, ...]]] = {
                name: [] for name in missing
            }
            for segment in self._segments:
                materialized = tick_columns(
                    segment.view()
                ).read_columns(*missing)
                for name in missing:
                    pieces[name].append(materialized[name])
            for name in missing:
                column_pieces = pieces[name]
                if not column_pieces:
                    values: tuple[object, ...] = ()
                elif len(column_pieces) == 1:
                    values = column_pieces[0]
                else:
                    values = tuple(
                        chain.from_iterable(column_pieces)
                    )
                self._column_cache[name] = values
        return {name: self._column_cache[name] for name in names}

    def materialize_all(self) -> dict[str, tuple[object, ...]]:
        return self.read_columns(*TICK_COLUMN_SPECS)

    def _append_bounded(
        self,
        appended: "InstrumentTickColumns",
        window_size: int,
    ) -> tuple["InstrumentTickColumns", "InstrumentTickColumns"]:
        combined = self._segments + appended._segments
        evicted_count = max(
            0, len(self) + len(appended) - window_size
        )
        evicted, retained = _split_segments(
            combined, evicted_count
        )
        return (
            InstrumentTickColumns._from_segments(retained),
            InstrumentTickColumns._from_segments(evicted),
        )


@dataclass(frozen=True, slots=True)
class InstrumentTickRollingWindow:
    """Immutable factor-facing view of the current bounded tick tail."""

    instrument_id: int
    window_size: int
    columns: InstrumentTickColumns
    seen_count: int

    def __post_init__(self) -> None:
        _positive_uint(
            self.instrument_id, "instrument_id", _UINT32_MAX
        )
        _positive_uint(
            self.window_size, "window_size", _UINT64_MAX
        )
        if not isinstance(self.columns, InstrumentTickColumns):
            raise TypeError("columns must be InstrumentTickColumns")
        _uint64(self.seen_count, "seen_count")
        if len(self.columns) != min(
            self.seen_count, self.window_size
        ):
            raise ValueError(
                "rolling window must retain the exact count tail"
            )

    def __len__(self) -> int:
        return len(self.columns)

    @property
    def wire_records(self) -> bytes:
        return self.columns.wire_records

    def read_columns(
        self, *names: str
    ) -> dict[str, tuple[object, ...]]:
        return self.columns.read_columns(*names)


@dataclass(frozen=True, slots=True)
class InstrumentTickRollingState(Generic[FactorStateT]):
    """One immutable committed window/checkpoint/factor-state bundle."""

    instrument_id: int
    window_size: int
    columns: InstrumentTickColumns
    seen_count: int
    checkpoint: Optional[InstrumentTickDeltaCheckpoint]
    factor_state: FactorStateT
    version: int

    def __post_init__(self) -> None:
        InstrumentTickRollingWindow(
            instrument_id=self.instrument_id,
            window_size=self.window_size,
            columns=self.columns,
            seen_count=self.seen_count,
        )
        _uint64(self.version, "version")
        if self.checkpoint is None:
            if self.seen_count != 0:
                raise ValueError(
                    "uncheckpointed rolling state must be empty"
                )
        else:
            if not isinstance(
                self.checkpoint, InstrumentTickDeltaCheckpoint
            ):
                raise TypeError(
                    "checkpoint must be InstrumentTickDeltaCheckpoint"
                )
            if self.checkpoint.instrument_id != self.instrument_id:
                raise StaleSessionError(
                    "rolling checkpoint belongs to another instrument"
                )
            if (
                self.seen_count
                != self.checkpoint.instrument_tick_record_count
            ):
                raise ValueError(
                    "seen_count does not match checkpoint tick count"
                )

    @property
    def window(self) -> InstrumentTickRollingWindow:
        return InstrumentTickRollingWindow(
            instrument_id=self.instrument_id,
            window_size=self.window_size,
            columns=self.columns,
            seen_count=self.seen_count,
        )

    @property
    def wire_records(self) -> bytes:
        return self.columns.wire_records

    def read_columns(
        self, *names: str
    ) -> dict[str, tuple[object, ...]]:
        return self.columns.read_columns(*names)


@dataclass(frozen=True, slots=True)
class InstrumentTickRollingUpdate:
    """One page transition supplied to the rolling factor."""

    page_index: int
    before: InstrumentTickRollingWindow
    after: InstrumentTickRollingWindow
    appended: InstrumentTickColumns
    evicted: InstrumentTickColumns

    def __post_init__(self) -> None:
        _uint64(self.page_index, "page_index")
        if not isinstance(self.appended, InstrumentTickColumns):
            raise TypeError("appended must be InstrumentTickColumns")
        if not isinstance(self.evicted, InstrumentTickColumns):
            raise TypeError("evicted must be InstrumentTickColumns")
        if self.before.instrument_id != self.after.instrument_id:
            raise StaleSessionError(
                "rolling update instrument identity changed"
            )
        if self.before.window_size != self.after.window_size:
            raise StaleSessionError(
                "rolling update window size changed"
            )
        if (
            self.after.seen_count
            != self.before.seen_count + len(self.appended)
        ):
            raise ValueError("rolling update seen_count mismatch")
        expected_evicted = max(
            0,
            len(self.before)
            + len(self.appended)
            - self.before.window_size,
        )
        if len(self.evicted) != expected_evicted:
            raise ValueError("rolling update eviction count mismatch")


@runtime_checkable
class InstrumentTickRollingFactor(
    Protocol[FactorStateT, FactorValueT]
):
    """A shadow-state transition returning ``(new_state, value)``.

    Only the returned factor state participates in the atomic commit. Factor
    implementations must not rely on external side effects being rolled back.
    """

    def update(
        self,
        state: FactorStateT,
        change: InstrumentTickRollingUpdate,
    ) -> tuple[FactorStateT, FactorValueT]:
        ...


@dataclass(frozen=True, slots=True)
class InstrumentTickRollingCommit(
    Generic[FactorStateT, FactorValueT]
):
    """The state published by one EOF-verified atomic commit."""

    state: InstrumentTickRollingState[FactorStateT]
    factor_value: Optional[FactorValueT]
    advanced: bool

    def __post_init__(self) -> None:
        if self.state.checkpoint is None:
            raise ValueError(
                "a rolling commit requires a verified checkpoint"
            )
        if not isinstance(self.advanced, bool):
            raise TypeError("advanced must be bool")

    @property
    def checkpoint(self) -> InstrumentTickDeltaCheckpoint:
        checkpoint = self.state.checkpoint
        assert checkpoint is not None
        return checkpoint


def _factor_transition(
    result: object,
) -> tuple[object, object]:
    if not isinstance(result, tuple) or len(result) != 2:
        raise TypeError(
            "factor.update must return (new_factor_state, value)"
        )
    return result


class InstrumentTickRollingStore(
    Generic[FactorStateT, FactorValueT]
):
    """Morning-start rolling state for one instrument and one fixed factor."""

    __slots__ = (
        "_instrument_id",
        "_window_size",
        "_factor",
        "_committed",
        "_active_token",
        "_lock",
    )

    def __init__(
        self,
        instrument_id: int,
        window_size: int,
        *,
        factor: InstrumentTickRollingFactor[
            FactorStateT, FactorValueT
        ],
        initial_factor_state: FactorStateT = None,  # type: ignore[assignment]
    ) -> None:
        self._instrument_id = _positive_uint(
            instrument_id, "instrument_id", _UINT32_MAX
        )
        self._window_size = _positive_uint(
            window_size, "window_size", _UINT64_MAX
        )
        if not isinstance(factor, InstrumentTickRollingFactor):
            raise TypeError(
                "factor must implement InstrumentTickRollingFactor"
            )
        self._factor = factor
        self._committed = InstrumentTickRollingState(
            instrument_id=self._instrument_id,
            window_size=self._window_size,
            columns=InstrumentTickColumns(),
            seen_count=0,
            checkpoint=None,
            factor_state=copy.deepcopy(initial_factor_state),
            version=0,
        )
        self._active_token: Optional[object] = None
        self._lock = threading.RLock()

    @property
    def instrument_id(self) -> int:
        return self._instrument_id

    @property
    def window_size(self) -> int:
        return self._window_size

    @property
    def checkpoint(
        self,
    ) -> Optional[InstrumentTickDeltaCheckpoint]:
        with self._lock:
            return self._committed.checkpoint

    @property
    def state(self) -> InstrumentTickRollingState[FactorStateT]:
        return self.snapshot()

    def _public_state(
        self,
        state: InstrumentTickRollingState[FactorStateT],
    ) -> InstrumentTickRollingState[FactorStateT]:
        return InstrumentTickRollingState(
            instrument_id=state.instrument_id,
            window_size=state.window_size,
            columns=state.columns,
            seen_count=state.seen_count,
            checkpoint=state.checkpoint,
            factor_state=copy.deepcopy(state.factor_state),
            version=state.version,
        )

    def snapshot(self) -> InstrumentTickRollingState[FactorStateT]:
        """Return a safe immutable view of the currently committed bundle."""

        with self._lock:
            committed = self._committed
        return self._public_state(committed)

    def begin(
        self, cursor: InstrumentTickDeltaCursor
    ) -> "InstrumentTickRollingTransaction[FactorStateT, FactorValueT]":
        """Take ownership of a fresh V2 cursor for one transaction."""

        if not isinstance(cursor, InstrumentTickDeltaCursor):
            raise TypeError(
                "cursor must be InstrumentTickDeltaCursor"
            )
        with self._lock:
            if self._active_token is not None:
                raise RuntimeError(
                    "an instrument rolling transaction is already active"
                )
            base = self._committed
            if cursor.instrument_id != self._instrument_id:
                raise StaleSessionError(
                    "delta cursor belongs to another instrument"
                )
            if cursor.base_checkpoint != base.checkpoint:
                raise StaleSessionError(
                    "delta cursor base does not match committed checkpoint"
                )
            if cursor.done or cursor.closed:
                raise ClientClosedError(
                    "delta cursor is already finished"
                )
            if (
                cursor.next_page_index != 0
                or cursor.cumulative_record_count != 0
                or cursor.cumulative_source_record_counts
                != (0, 0, 0, 0)
            ):
                raise ValueError(
                    "rolling transaction requires an unread delta cursor"
                )
            token = object()
            transaction = InstrumentTickRollingTransaction(
                store=self,
                cursor=cursor,
                token=token,
                base=base,
                shadow_factor_state=copy.deepcopy(
                    base.factor_state
                ),
            )
            self._active_token = token
            return transaction

    def update(
        self, cursor: InstrumentTickDeltaCursor
    ) -> InstrumentTickRollingCommit[
        FactorStateT, FactorValueT
    ]:
        """Consume through explicit EOF and atomically publish the result."""

        with self.begin(cursor) as transaction:
            transaction.consume()
            return transaction.commit()

    def _release(self, token: object) -> None:
        with self._lock:
            if self._active_token is token:
                self._active_token = None


class InstrumentTickRollingTransaction(
    Generic[FactorStateT, FactorValueT]
):
    """Isolated shadow window committed only after verified explicit EOF."""

    __slots__ = (
        "_store",
        "_cursor",
        "_token",
        "_base",
        "_base_version",
        "_shadow_columns",
        "_shadow_seen_count",
        "_shadow_factor_state",
        "_next_page_index",
        "_delta_record_count",
        "_delta_source_counts",
        "_target_checkpoint",
        "_factor_value",
        "_eof",
        "_active",
    )

    def __init__(
        self,
        *,
        store: InstrumentTickRollingStore[
            FactorStateT, FactorValueT
        ],
        cursor: InstrumentTickDeltaCursor,
        token: object,
        base: InstrumentTickRollingState[FactorStateT],
        shadow_factor_state: FactorStateT,
    ) -> None:
        self._store = store
        self._cursor = cursor
        self._token = token
        self._base = base
        self._base_version = base.version
        self._shadow_columns = base.columns
        self._shadow_seen_count = base.seen_count
        self._shadow_factor_state = shadow_factor_state
        self._next_page_index = 0
        self._delta_record_count = 0
        self._delta_source_counts = (0, 0, 0, 0)
        self._target_checkpoint: Optional[
            InstrumentTickDeltaCheckpoint
        ] = None
        self._factor_value: object = _NO_FACTOR_VALUE
        self._eof = False
        self._active = True

    @property
    def active(self) -> bool:
        return self._active

    @property
    def done(self) -> bool:
        return self._eof

    @property
    def delta_record_count(self) -> int:
        return self._delta_record_count

    @property
    def state(self) -> InstrumentTickRollingState[FactorStateT]:
        self._require_active()
        if not self._eof or self._target_checkpoint is None:
            raise RuntimeError(
                "shadow state is unavailable before explicit EOF"
            )
        return self._make_public_state(
            self._target_checkpoint,
            self._next_version(),
        )

    def __enter__(
        self,
    ) -> "InstrumentTickRollingTransaction[FactorStateT, FactorValueT]":
        self._require_active()
        return self

    def __exit__(self, _type, _value, _traceback) -> None:
        self.abort()

    def _require_active(self) -> None:
        if not self._active:
            raise ClientClosedError(
                "instrument rolling transaction is no longer active"
            )

    def _window(
        self,
        columns: InstrumentTickColumns,
        seen_count: int,
    ) -> InstrumentTickRollingWindow:
        return InstrumentTickRollingWindow(
            instrument_id=self._store.instrument_id,
            window_size=self._store.window_size,
            columns=columns,
            seen_count=seen_count,
        )

    def _validate_page_identity(
        self, page: InstrumentTickDeltaPage
    ) -> None:
        if not isinstance(page, InstrumentTickDeltaPage):
            raise TypeError(
                "delta cursor returned a non-V2 page"
            )
        if page.page_index != self._next_page_index:
            raise WireFormatError(
                "instrument delta pages are not contiguous"
            )
        if page.instrument_id != self._store.instrument_id:
            raise StaleSessionError(
                "instrument delta page belongs to another instrument"
            )
        if (
            page.metadata != self._cursor.metadata
            or page.generation != self._cursor.generation
        ):
            raise StaleSessionError(
                "instrument delta page changed its pinned generation"
            )

    def _apply_data_page(
        self, page: InstrumentTickDeltaPage
    ) -> InstrumentTickRollingUpdate:
        row_count = len(page)
        if row_count == 0:
            raise WireFormatError(
                "non-EOF instrument delta page must be nonempty"
            )
        if self._shadow_seen_count > _UINT64_MAX - row_count:
            raise WireFormatError(
                "rolling seen_count would overflow uint64"
            )
        expected_cumulative = self._delta_record_count + row_count
        source_counts = page.cumulative_source_record_counts
        if (
            page.cumulative_record_count != expected_cumulative
            or sum(source_counts) != expected_cumulative
            or source_counts[0]
            or source_counts[2]
            or any(
                current < prior
                for current, prior in zip(
                    source_counts, self._delta_source_counts
                )
            )
            or any(
                current > expected
                for current, expected in zip(
                    source_counts,
                    page.metadata.delta_tick_source_record_counts,
                )
            )
        ):
            raise WireFormatError(
                "instrument delta cumulative counts are inconsistent"
            )

        before = self._window(
            self._shadow_columns, self._shadow_seen_count
        )
        appended = InstrumentTickColumns(page.wire_records)
        after_columns, evicted = self._shadow_columns._append_bounded(
            appended, self._store.window_size
        )
        after_seen_count = self._shadow_seen_count + row_count
        after = self._window(after_columns, after_seen_count)
        update = InstrumentTickRollingUpdate(
            page_index=page.page_index,
            before=before,
            after=after,
            appended=appended,
            evicted=evicted,
        )
        new_factor_state, factor_value = _factor_transition(
            self._store._factor.update(
                self._shadow_factor_state, update
            )
        )

        self._shadow_columns = after_columns
        self._shadow_seen_count = after_seen_count
        self._shadow_factor_state = (  # type: ignore[assignment]
            new_factor_state
        )
        self._factor_value = factor_value
        self._delta_record_count = expected_cumulative
        self._delta_source_counts = source_counts
        self._next_page_index += 1
        return update

    def _apply_eof(self, page: InstrumentTickDeltaPage) -> None:
        if len(page):
            raise WireFormatError(
                "instrument delta EOF page must be empty"
            )
        if (
            not self._cursor.done
            or page.cumulative_record_count
            != self._delta_record_count
            or page.cumulative_source_record_counts
            != self._delta_source_counts
        ):
            raise WireFormatError(
                "instrument delta EOF did not reconcile its pages"
            )
        checkpoint = self._cursor.verified_checkpoint
        if (
            not isinstance(
                checkpoint, InstrumentTickDeltaCheckpoint
            )
            or checkpoint
            != self._cursor.metadata.target_checkpoint
            or checkpoint.instrument_id
            != self._store.instrument_id
        ):
            raise WireFormatError(
                "delta cursor returned an invalid target checkpoint"
            )
        base = self._base.checkpoint
        if base is None:
            base_counts = (0, 0, 0, 0)
        else:
            checkpoint.ensure_successor_of(base)
            base_counts = (
                base.instrument_tick_source_record_counts
            )
        expected_source_counts = tuple(
            target - origin
            for target, origin in zip(
                checkpoint.instrument_tick_source_record_counts,
                base_counts,
            )
        )
        if (
            self._delta_record_count
            != self._cursor.expected_record_count
            or self._delta_source_counts != expected_source_counts
            or self._shadow_seen_count
            != checkpoint.instrument_tick_record_count
        ):
            raise WireFormatError(
                "rolling tick counts do not reconcile at EOF"
            )
        self._target_checkpoint = checkpoint
        self._eof = True
        self._next_page_index += 1

    def step(
        self,
    ) -> Optional[InstrumentTickRollingUpdate]:
        """Read and apply exactly one V2 data page or terminal EOF."""

        self._require_active()
        if self._eof:
            raise ClientClosedError(
                "instrument rolling transaction is already at EOF"
            )
        try:
            page = self._cursor.read_page()
            if page is None:
                raise WireFormatError(
                    "delta cursor ended without an explicit EOF page"
                )
            self._validate_page_identity(page)
            if page.eof:
                self._apply_eof(page)
                return None
            return self._apply_data_page(page)
        except BaseException:
            self.abort()
            raise

    def consume(
        self,
    ) -> "InstrumentTickRollingTransaction[FactorStateT, FactorValueT]":
        """Consume every page, including the independently verified EOF."""

        self._require_active()
        while not self._eof:
            self.step()
        return self

    def _next_version(self) -> int:
        assert self._target_checkpoint is not None
        if self._target_checkpoint == self._base.checkpoint:
            return self._base_version
        if self._base_version == _UINT64_MAX:
            raise UnavailableError(
                "rolling state version space is exhausted"
            )
        return self._base_version + 1

    def _make_public_state(
        self,
        checkpoint: InstrumentTickDeltaCheckpoint,
        version: int,
    ) -> InstrumentTickRollingState[FactorStateT]:
        return InstrumentTickRollingState(
            instrument_id=self._store.instrument_id,
            window_size=self._store.window_size,
            columns=self._shadow_columns,
            seen_count=self._shadow_seen_count,
            checkpoint=checkpoint,
            factor_state=copy.deepcopy(
                self._shadow_factor_state
            ),
            version=version,
        )

    def commit(
        self,
    ) -> InstrumentTickRollingCommit[
        FactorStateT, FactorValueT
    ]:
        """Publish window, checkpoint, and factor state in one pointer swap."""

        self._require_active()
        try:
            if (
                not self._eof
                or self._target_checkpoint is None
                or not self._cursor.done
            ):
                raise RuntimeError(
                    "explicit instrument delta EOF is required before "
                    "commit"
                )
            advanced = (
                self._target_checkpoint != self._base.checkpoint
            )
            version = self._next_version()
            if advanced:
                committed_factor_state = copy.deepcopy(
                    self._shadow_factor_state
                )
                committed = InstrumentTickRollingState(
                    instrument_id=self._store.instrument_id,
                    window_size=self._store.window_size,
                    columns=self._shadow_columns,
                    seen_count=self._shadow_seen_count,
                    checkpoint=self._target_checkpoint,
                    factor_state=committed_factor_state,
                    version=version,
                )
                public_state = InstrumentTickRollingState(
                    instrument_id=committed.instrument_id,
                    window_size=committed.window_size,
                    columns=committed.columns,
                    seen_count=committed.seen_count,
                    checkpoint=committed.checkpoint,
                    factor_state=copy.deepcopy(
                        committed.factor_state
                    ),
                    version=committed.version,
                )
            else:
                committed = self._base
                public_state = self._store._public_state(
                    self._base
                )
            value = (
                None
                if self._factor_value is _NO_FACTOR_VALUE
                else self._factor_value
            )
            result = InstrumentTickRollingCommit(
                state=public_state,
                factor_value=value,  # type: ignore[arg-type]
                advanced=advanced,
            )
            with self._store._lock:
                if (
                    self._store._active_token is not self._token
                    or self._store._committed is not self._base
                    or self._store._committed.version
                    != self._base_version
                ):
                    raise StaleSessionError(
                        "rolling store changed during transaction"
                    )
                if advanced:
                    self._store._committed = committed
                self._store._active_token = None
                self._active = False
            self._clear_references()
            return result
        except BaseException:
            self.abort()
            raise

    def abort(self) -> None:
        """Discard all shadow mutations and close an unfinished cursor."""

        if not self._active:
            self._clear_references()
            return
        store = self._store
        cursor = self._cursor
        store._release(self._token)
        self._active = False
        try:
            if not cursor.done:
                try:
                    cursor.close()
                except BaseException:
                    # The transaction is already detached from the store.
                    # A close failure must not mask the read/factor failure
                    # that caused this abort.
                    pass
        finally:
            self._clear_references()

    def _clear_references(self) -> None:
        self._store = None  # type: ignore[assignment]
        self._cursor = None  # type: ignore[assignment]
        self._token = None  # type: ignore[assignment]
        self._base = None  # type: ignore[assignment]
        self._shadow_columns = None  # type: ignore[assignment]
        self._shadow_factor_state = None  # type: ignore[assignment]
        self._target_checkpoint = None
        self._factor_value = _NO_FACTOR_VALUE


__all__ = [
    "InstrumentTickColumns",
    "InstrumentTickRollingCommit",
    "InstrumentTickRollingFactor",
    "InstrumentTickRollingState",
    "InstrumentTickRollingStore",
    "InstrumentTickRollingTransaction",
    "InstrumentTickRollingUpdate",
    "InstrumentTickRollingWindow",
]
