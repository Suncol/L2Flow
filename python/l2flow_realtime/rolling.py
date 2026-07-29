"""Transactional instrument-local column windows for tick deltas."""

from __future__ import annotations

import copy
import threading
from dataclasses import dataclass
from typing import (
    Any,
    Generic,
    Optional,
    Protocol,
    Tuple,
    TypeVar,
    runtime_checkable,
)

from .batch import tick_wire_numpy_records, tick_wire_record_count
from .checkpoint import InstrumentTickDeltaCheckpoint
from .instrument_delta import InstrumentTickDeltaPage
from .models import (
    ClientClosedError,
    MarketEventKind,
    StaleSessionError,
    WireFormatError,
)
from .wire import TICK_BYTES, _COMMON


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


def _schema(value: object, field: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field} must be a string")
    if not value:
        raise ValueError(f"{field} must be non-empty")
    return value


@dataclass(frozen=True, slots=True)
class InstrumentTickColumns:
    """Immutable, record-aligned tick wire columns."""

    wire_records: bytes

    def __post_init__(self) -> None:
        tick_wire_record_count(self.wire_records)

    def __len__(self) -> int:
        return len(self.wire_records) // TICK_BYTES

    def numpy_records(self):
        """Return a zero-copy, read-only structured NumPy view."""

        return tick_wire_numpy_records(self.wire_records)


def _validate_checkpoint_tail(
    columns: InstrumentTickColumns,
    *,
    instrument_id: int,
    checkpoint: InstrumentTickDeltaCheckpoint,
    complete: bool,
) -> None:
    """Validate persisted tail identity without constructing Tick objects."""

    source_counts = [0, 0, 0, 0]
    prior_ingress = 0
    prior_tick = 0
    prior_sources = [0, 0, 0, 0]
    wire = columns.wire_records
    for offset in range(0, len(wire), TICK_BYTES):
        fields = _COMMON.unpack_from(wire, offset)
        (
            schema_version,
            record_bytes,
            row_instrument_id,
            registry_ordinal,
            source_sequence,
            ingress_sequence,
            tick_sequence,
        ) = fields[:7]
        source_stream_id = fields[14]
        trade_date = fields[15]
        common_reserved = fields[17]
        source_slot = fields[18]
        event_kind = fields[19]
        lane_valid = (
            source_slot == 1
            and event_kind == MarketEventKind.SHANGHAI_TICK
        ) or (
            source_slot == 3
            and event_kind
            in (
                MarketEventKind.SHENZHEN_ORDER,
                MarketEventKind.SHENZHEN_TRANSACTION,
            )
        )
        if (
            schema_version != 1
            or record_bytes != TICK_BYTES
            or common_reserved != 0
            or any(wire[offset + 118 : offset + 128])
        ):
            raise WireFormatError(
                "initial rolling columns contain a noncanonical "
                "common record"
            )
        if row_instrument_id != instrument_id:
            raise StaleSessionError(
                "initial rolling columns belong to another instrument"
            )
        if not lane_valid or source_sequence == 0:
            raise WireFormatError(
                "initial rolling columns contain an invalid tick lane"
            )
        if (
            registry_ordinal != checkpoint.registry_ordinal
            or trade_date != checkpoint.trade_date
            or source_stream_id
            != checkpoint.source_stream_ids[source_slot]
            or ingress_sequence <= prior_ingress
            or ingress_sequence
            >= checkpoint.ingress_sequence_exclusive
            or tick_sequence <= prior_tick
            or tick_sequence
            >= checkpoint.tick_stream_sequence_exclusive
            or tick_sequence > ingress_sequence
            or source_sequence <= prior_sources[source_slot]
            or source_sequence
            >= checkpoint.source_sequence_exclusive[source_slot]
        ):
            raise StaleSessionError(
                "initial rolling columns do not belong to their "
                "checkpoint"
            )
        source_counts[source_slot] += 1
        prior_ingress = ingress_sequence
        prior_tick = tick_sequence
        prior_sources[source_slot] = source_sequence

    if any(
        source_counts[slot]
        > checkpoint.instrument_tick_counts[slot]
        for slot in (1, 3)
    ):
        raise ValueError(
            "initial rolling tail exceeds checkpoint source counts"
        )
    if complete and tuple(source_counts) != checkpoint.instrument_tick_counts:
        raise ValueError(
            "complete initial rolling tail counts do not match checkpoint"
        )


def _roll_window_columns(
    retained: bytes,
    appended: bytes,
    window_size: int,
) -> tuple[bytes, bytes]:
    """Return ``(new_tail, evicted_prefix)`` with one copy per output byte."""

    retained_count = len(retained) // TICK_BYTES
    appended_count = len(appended) // TICK_BYTES
    evicted_count = max(
        0, retained_count + appended_count - window_size
    )
    if evicted_count == 0:
        return retained + appended, b""

    if evicted_count <= retained_count:
        split = evicted_count * TICK_BYTES
        new_tail = b"".join(
            (memoryview(retained)[split:], appended)
        )
        return new_tail, retained[:split]

    appended_split = (
        evicted_count - retained_count
    ) * TICK_BYTES
    evicted = b"".join(
        (retained, memoryview(appended)[:appended_split])
    )
    return appended[appended_split:], evicted


@dataclass(frozen=True, slots=True)
class InstrumentTickRollingWindow:
    """Immutable factor-facing view of a page-local shadow window."""

    instrument_id: int
    window_size: int
    columns: InstrumentTickColumns
    seen_count: int
    state_schema: str

    def __post_init__(self) -> None:
        _positive_uint(self.instrument_id, "instrument_id", _UINT32_MAX)
        _positive_uint(self.window_size, "window_size", _UINT64_MAX)
        if not isinstance(self.columns, InstrumentTickColumns):
            raise TypeError("columns must be InstrumentTickColumns")
        _uint64(self.seen_count, "seen_count")
        _schema(self.state_schema, "state_schema")
        if len(self.columns) != min(self.seen_count, self.window_size):
            raise ValueError(
                "rolling window must retain the exact count tail"
            )

    def __len__(self) -> int:
        return len(self.columns)

    @property
    def wire_records(self) -> bytes:
        return self.columns.wire_records

    def numpy_records(self):
        return self.columns.numpy_records()


@dataclass(frozen=True, slots=True)
class InstrumentTickRollingState(Generic[FactorStateT]):
    """Immutable committed state at one verified checkpoint."""

    instrument_id: int
    window_size: int
    columns: InstrumentTickColumns
    seen_count: int
    checkpoint: Optional[InstrumentTickDeltaCheckpoint]
    state_schema: str
    factor_schema: str
    factor_state: FactorStateT

    def __post_init__(self) -> None:
        InstrumentTickRollingWindow(
            instrument_id=self.instrument_id,
            window_size=self.window_size,
            columns=self.columns,
            seen_count=self.seen_count,
            state_schema=self.state_schema,
        )
        _schema(self.factor_schema, "factor_schema")
        if self.checkpoint is None:
            if self.seen_count != 0 or len(self.columns):
                raise ValueError(
                    "uncheckpointed rolling state must be empty"
                )
        else:
            if self.checkpoint.instrument_id != self.instrument_id:
                raise StaleSessionError(
                    "rolling checkpoint belongs to another instrument"
                )
            if (
                self.seen_count
                != self.checkpoint.total_instrument_tick_count
            ):
                raise ValueError(
                    "seen_count does not match checkpoint tick counts"
                )

    @property
    def window(self) -> InstrumentTickRollingWindow:
        return InstrumentTickRollingWindow(
            instrument_id=self.instrument_id,
            window_size=self.window_size,
            columns=self.columns,
            seen_count=self.seen_count,
            state_schema=self.state_schema,
        )

    @property
    def wire_records(self) -> bytes:
        return self.columns.wire_records

    def numpy_records(self):
        return self.columns.numpy_records()


@dataclass(frozen=True, slots=True)
class InstrumentTickRollingUpdate:
    """One bounded page transition passed to a column factor."""

    page_index: int
    before: InstrumentTickRollingWindow
    after: InstrumentTickRollingWindow
    appended: InstrumentTickColumns
    evicted: InstrumentTickColumns

    def __post_init__(self) -> None:
        _uint64(self.page_index, "page_index")
        if self.before.instrument_id != self.after.instrument_id:
            raise StaleSessionError(
                "rolling update instrument identity mismatch"
            )
        if (
            self.before.window_size != self.after.window_size
            or self.before.state_schema != self.after.state_schema
        ):
            raise StaleSessionError(
                "rolling update schema/window changed"
            )
        if (
            self.after.seen_count
            != self.before.seen_count + len(self.appended)
        ):
            raise ValueError("rolling update seen_count mismatch")
        expected_evicted = max(
            0,
            len(self.before) + len(self.appended)
            - self.before.window_size,
        )
        if len(self.evicted) != expected_evicted:
            raise ValueError("rolling update eviction count mismatch")


@dataclass(frozen=True, slots=True)
class InstrumentTickRollingGeneration(Generic[FactorStateT]):
    """Optional EOF hook input, including the now-verified target."""

    before: InstrumentTickRollingState[FactorStateT]
    after: InstrumentTickRollingState[FactorStateT]

    @property
    def checkpoint(self) -> InstrumentTickDeltaCheckpoint:
        checkpoint = self.after.checkpoint
        assert checkpoint is not None
        return checkpoint


@runtime_checkable
class InstrumentTickRollingFactor(
    Protocol[FactorStateT, FactorValueT]
):
    """Pure column transition returning ``(new_state, value)``."""

    def update(
        self,
        state: FactorStateT,
        change: InstrumentTickRollingUpdate,
    ) -> Tuple[FactorStateT, FactorValueT]:
        ...


@dataclass(frozen=True, slots=True)
class InstrumentTickRollingCommit(
    Generic[FactorStateT, FactorValueT]
):
    """Result published by one successful atomic transaction."""

    state: InstrumentTickRollingState[FactorStateT]
    factor_value: Optional[FactorValueT]

    @property
    def checkpoint(self) -> InstrumentTickDeltaCheckpoint:
        checkpoint = self.state.checkpoint
        assert checkpoint is not None
        return checkpoint


def _factor_transition(
    result: object,
    operation: str,
) -> tuple[object, object]:
    if not isinstance(result, tuple) or len(result) != 2:
        raise TypeError(
            f"{operation} must return (new_factor_state, value)"
        )
    return result


class InstrumentTickRollingStore(Generic[FactorStateT]):
    """Committed count-window and factor state for one instrument."""

    def __init__(
        self,
        instrument_id: int,
        window_size: int,
        *,
        state_schema: str,
        factor_schema: str,
        factor_state: FactorStateT = None,  # type: ignore[assignment]
        checkpoint: Optional[InstrumentTickDeltaCheckpoint] = None,
        wire_records: bytes = b"",
        seen_count: Optional[int] = None,
    ) -> None:
        _positive_uint(instrument_id, "instrument_id", _UINT32_MAX)
        _positive_uint(window_size, "window_size", _UINT64_MAX)
        state_schema = _schema(state_schema, "state_schema")
        factor_schema = _schema(factor_schema, "factor_schema")
        columns = InstrumentTickColumns(wire_records)
        if len(columns) > window_size:
            raise ValueError("initial columns exceed window_size")
        if checkpoint is not None:
            if checkpoint.instrument_id != instrument_id:
                raise StaleSessionError(
                    "initial checkpoint belongs to another instrument"
                )
            expected_seen = checkpoint.total_instrument_tick_count
        else:
            expected_seen = 0
        if seen_count is None:
            seen_count = expected_seen
        _uint64(seen_count, "seen_count")
        if seen_count != expected_seen:
            raise ValueError(
                "seen_count must equal the checkpoint instrument count"
            )
        if checkpoint is None and len(columns):
            raise ValueError(
                "initial columns require a verified checkpoint"
            )
        if len(columns) != min(seen_count, window_size):
            raise ValueError(
                "initial columns must retain the exact count tail"
            )
        if checkpoint is not None:
            _validate_checkpoint_tail(
                columns,
                instrument_id=instrument_id,
                checkpoint=checkpoint,
                complete=seen_count <= window_size,
            )

        self._instrument_id = instrument_id
        self._window_size = window_size
        self._state_schema = state_schema
        self._factor_schema = factor_schema
        self._wire_records = columns.wire_records
        self._seen_count = seen_count
        self._checkpoint = checkpoint
        self._factor_state = copy.deepcopy(factor_state)
        self._version = 0
        self._active_token: Optional[object] = None
        self._lock = threading.RLock()

    @property
    def instrument_id(self) -> int:
        return self._instrument_id

    @property
    def window_size(self) -> int:
        return self._window_size

    @property
    def checkpoint(self) -> Optional[InstrumentTickDeltaCheckpoint]:
        with self._lock:
            return self._checkpoint

    @property
    def state(self) -> InstrumentTickRollingState[FactorStateT]:
        return self.snapshot()

    def snapshot(self) -> InstrumentTickRollingState[FactorStateT]:
        """Return an immutable copy of the committed state."""

        with self._lock:
            return InstrumentTickRollingState(
                instrument_id=self._instrument_id,
                window_size=self._window_size,
                columns=InstrumentTickColumns(self._wire_records),
                seen_count=self._seen_count,
                checkpoint=self._checkpoint,
                state_schema=self._state_schema,
                factor_schema=self._factor_schema,
                factor_state=copy.deepcopy(self._factor_state),
            )

    def begin(
        self,
        cursor: Any,
        factor: InstrumentTickRollingFactor[
            FactorStateT, FactorValueT
        ],
    ) -> "InstrumentTickRollingTransaction":
        """Start one isolated, page-streaming transaction."""

        if not isinstance(factor, InstrumentTickRollingFactor):
            raise TypeError(
                "factor must implement InstrumentTickRollingFactor"
            )
        factor_schema = getattr(factor, "factor_schema", None)
        if (
            factor_schema is not None
            and factor_schema != self._factor_schema
        ):
            raise StaleSessionError(
                "factor schema does not match rolling state"
            )
        if getattr(cursor, "instrument_id", None) != self._instrument_id:
            raise StaleSessionError(
                "delta cursor belongs to another instrument"
            )
        if getattr(cursor, "base_checkpoint", None) != self._checkpoint:
            raise StaleSessionError(
                "delta cursor base does not match committed checkpoint"
            )
        if getattr(cursor, "done", False):
            raise ClientClosedError("delta cursor is already at EOF")
        with self._lock:
            if self._active_token is not None:
                raise RuntimeError(
                    "an instrument rolling transaction is already active"
                )
            token = object()
            transaction = InstrumentTickRollingTransaction(
                _store=self,
                _cursor=cursor,
                _factor=factor,
                _token=token,
                _base_version=self._version,
                _base=self.snapshot(),
                _shadow_wire_records=self._wire_records,
                _shadow_seen_count=self._seen_count,
                _shadow_factor_state=copy.deepcopy(self._factor_state),
            )
            self._active_token = token
            return transaction

    def update(
        self,
        cursor: Any,
        factor: InstrumentTickRollingFactor[
            FactorStateT, FactorValueT
        ],
    ) -> InstrumentTickRollingCommit[FactorStateT, FactorValueT]:
        """Consume a complete cursor and atomically publish its result."""

        with self.begin(cursor, factor) as transaction:
            for page in cursor.pages():
                transaction.apply_page(page)
            return transaction.commit()

    def _release(self, token: object) -> None:
        with self._lock:
            if self._active_token is token:
                self._active_token = None


@dataclass(slots=True)
class InstrumentTickRollingTransaction(
    Generic[FactorStateT, FactorValueT]
):
    """Page-bounded shadow state for one complete instrument delta."""

    _store: InstrumentTickRollingStore[FactorStateT]
    _cursor: Any
    _factor: InstrumentTickRollingFactor[
        FactorStateT, FactorValueT
    ]
    _token: object
    _base_version: int
    _base: InstrumentTickRollingState[FactorStateT]
    _shadow_wire_records: bytes
    _shadow_seen_count: int
    _shadow_factor_state: FactorStateT
    _next_page_index: int = 0
    _delta_record_count: int = 0
    _delta_source_counts: Tuple[int, int, int, int] = (
        0,
        0,
        0,
        0,
    )
    _eof: bool = False
    _active: bool = True
    _factor_value: object = _NO_FACTOR_VALUE
    _target_checkpoint: Optional[
        InstrumentTickDeltaCheckpoint
    ] = None

    @property
    def active(self) -> bool:
        return self._active

    @property
    def done(self) -> bool:
        return self._eof

    def __enter__(
        self,
    ) -> "InstrumentTickRollingTransaction[FactorStateT, FactorValueT]":
        self._require_active()
        return self

    def __exit__(self, _type, _value, _traceback) -> None:
        self.abort()

    def __del__(self) -> None:
        try:
            self.abort()
        except BaseException:
            pass

    @property
    def state(self) -> InstrumentTickRollingState[FactorStateT]:
        if not self._eof or self._target_checkpoint is None:
            raise RuntimeError(
                "shadow state is not verifiable before explicit EOF"
            )
        return self._shadow_state(self._target_checkpoint)

    def _require_active(self) -> None:
        if not self._active:
            raise ClientClosedError(
                "instrument rolling transaction is no longer active"
            )

    def _shadow_window(self) -> InstrumentTickRollingWindow:
        return InstrumentTickRollingWindow(
            instrument_id=self._store.instrument_id,
            window_size=self._store.window_size,
            columns=InstrumentTickColumns(
                self._shadow_wire_records
            ),
            seen_count=self._shadow_seen_count,
            state_schema=self._base.state_schema,
        )

    def _shadow_state(
        self,
        checkpoint: InstrumentTickDeltaCheckpoint,
    ) -> InstrumentTickRollingState[FactorStateT]:
        return InstrumentTickRollingState(
            instrument_id=self._store.instrument_id,
            window_size=self._store.window_size,
            columns=InstrumentTickColumns(
                self._shadow_wire_records
            ),
            seen_count=self._shadow_seen_count,
            checkpoint=checkpoint,
            state_schema=self._base.state_schema,
            factor_schema=self._base.factor_schema,
            factor_state=copy.deepcopy(self._shadow_factor_state),
        )

    def _verified_checkpoint(
        self,
    ) -> InstrumentTickDeltaCheckpoint:
        checkpoint = self._cursor.verified_checkpoint
        if not isinstance(
            checkpoint, InstrumentTickDeltaCheckpoint
        ):
            raise WireFormatError(
                "delta cursor returned an invalid verified checkpoint"
            )
        return checkpoint

    def _apply_factor_page(
        self,
        *,
        page_index: int,
        before: InstrumentTickRollingWindow,
        appended: InstrumentTickColumns,
        evicted: InstrumentTickColumns,
    ) -> None:
        change = InstrumentTickRollingUpdate(
            page_index=page_index,
            before=before,
            after=self._shadow_window(),
            appended=appended,
            evicted=evicted,
        )
        new_state, factor_value = _factor_transition(
            self._factor.update(self._shadow_factor_state, change),
            "factor.update",
        )
        self._shadow_factor_state = new_state  # type: ignore[assignment]
        self._factor_value = factor_value

    def _apply_generation_hook(
        self,
        checkpoint: InstrumentTickDeltaCheckpoint,
    ) -> None:
        hook = getattr(self._factor, "on_generation", None)
        if hook is None:
            return
        if not callable(hook):
            raise TypeError("factor.on_generation must be callable")
        new_state, factor_value = _factor_transition(
            hook(
                self._shadow_factor_state,
                InstrumentTickRollingGeneration(
                    before=self._base,
                    after=self._shadow_state(checkpoint),
                ),
            ),
            "factor.on_generation",
        )
        self._shadow_factor_state = new_state  # type: ignore[assignment]
        self._factor_value = factor_value

    def apply_page(self, page: InstrumentTickDeltaPage) -> None:
        """Apply one data or explicit EOF page entirely to shadow state."""

        self._require_active()
        try:
            if self._eof:
                raise ClientClosedError(
                    "instrument rolling transaction is already at EOF"
                )
            if not isinstance(page, InstrumentTickDeltaPage):
                raise TypeError(
                    "rolling input must be InstrumentTickDeltaPage"
                )
            if page.page_index != self._next_page_index:
                raise WireFormatError(
                    "instrument delta pages are not contiguous"
                )
            if page.instrument_id != self._store.instrument_id:
                raise StaleSessionError(
                    "instrument delta page belongs to another instrument"
                )
            row_count = len(page)
            expected_cumulative = self._delta_record_count + row_count
            if (
                expected_cumulative > _UINT64_MAX
                or page.cumulative_record_count
                != expected_cumulative
                or any(
                    current < prior
                    for current, prior in zip(
                        page.cumulative_source_record_counts,
                        self._delta_source_counts,
                    )
                )
            ):
                raise WireFormatError(
                    "instrument delta cumulative counts are inconsistent"
                )

            if page.eof:
                if row_count:
                    raise WireFormatError(
                        "instrument delta EOF page must be empty"
                    )
                if not getattr(self._cursor, "done", False):
                    raise WireFormatError(
                        "instrument delta cursor did not enter EOF"
                    )
                if (
                    page.cumulative_source_record_counts
                    != self._delta_source_counts
                ):
                    raise WireFormatError(
                        "instrument delta EOF source counts changed"
                    )
                checkpoint = self._verified_checkpoint()
                if checkpoint.instrument_id != self._store.instrument_id:
                    raise StaleSessionError(
                        "verified checkpoint belongs to another "
                        "instrument"
                    )
                if self._base.checkpoint is not None:
                    checkpoint.ensure_successor_of(
                        self._base.checkpoint
                    )
                    base_counts = (
                        self._base.checkpoint.instrument_tick_counts
                    )
                else:
                    base_counts = (0, 0, 0, 0)
                expected_source_counts = tuple(
                    target - base
                    for target, base in zip(
                        checkpoint.instrument_tick_counts,
                        base_counts,
                    )
                )
                if (
                    self._shadow_seen_count
                    != checkpoint.total_instrument_tick_count
                    or self._delta_source_counts
                    != expected_source_counts
                ):
                    raise WireFormatError(
                        "rolling tick counts do not reconcile at EOF"
                    )
                self._target_checkpoint = checkpoint
                self._apply_generation_hook(checkpoint)
                self._eof = True
                self._next_page_index += 1
                return

            if row_count == 0:
                raise WireFormatError(
                    "non-EOF instrument delta page must be nonempty"
                )
            if self._shadow_seen_count > _UINT64_MAX - row_count:
                raise WireFormatError(
                    "rolling seen_count would overflow uint64"
                )
            before = self._shadow_window()
            appended = InstrumentTickColumns(page.wire_records)
            (
                self._shadow_wire_records,
                evicted_wire_records,
            ) = _roll_window_columns(
                self._shadow_wire_records,
                page.wire_records,
                self._store.window_size,
            )
            evicted = InstrumentTickColumns(evicted_wire_records)
            self._shadow_seen_count += row_count
            self._delta_record_count = expected_cumulative
            self._delta_source_counts = (
                page.cumulative_source_record_counts
            )
            self._apply_factor_page(
                page_index=page.page_index,
                before=before,
                appended=appended,
                evicted=evicted,
            )
            self._next_page_index += 1
        except BaseException:
            self.abort()
            raise

    def commit(
        self,
    ) -> InstrumentTickRollingCommit[FactorStateT, FactorValueT]:
        """Publish all shadow fields together after verified EOF."""

        self._require_active()
        try:
            if (
                not self._eof
                or self._target_checkpoint is None
                or not getattr(self._cursor, "done", False)
            ):
                raise RuntimeError(
                    "explicit instrument delta EOF is required before "
                    "commit"
                )
            committed_factor_state = copy.deepcopy(
                self._shadow_factor_state
            )
            state = InstrumentTickRollingState(
                instrument_id=self._store.instrument_id,
                window_size=self._store.window_size,
                columns=InstrumentTickColumns(
                    self._shadow_wire_records
                ),
                seen_count=self._shadow_seen_count,
                checkpoint=self._target_checkpoint,
                state_schema=self._base.state_schema,
                factor_schema=self._base.factor_schema,
                factor_state=copy.deepcopy(committed_factor_state),
            )
            value = (
                None
                if self._factor_value is _NO_FACTOR_VALUE
                else self._factor_value
            )
            commit_result = InstrumentTickRollingCommit(
                state=state,
                factor_value=value,  # type: ignore[arg-type]
            )
            with self._store._lock:
                if (
                    self._store._active_token is not self._token
                    or self._store._version != self._base_version
                ):
                    raise StaleSessionError(
                        "rolling store changed during transaction"
                    )
                self._store._wire_records = (
                    self._shadow_wire_records
                )
                self._store._seen_count = self._shadow_seen_count
                self._store._checkpoint = self._target_checkpoint
                self._store._factor_state = committed_factor_state
                self._store._version = self._base_version + 1
                self._store._active_token = None
                self._active = False
            self._clear_references()
            return commit_result
        except BaseException:
            self.abort()
            raise

    def _clear_references(self) -> None:
        self._store = None  # type: ignore[assignment]
        self._cursor = None
        self._factor = None  # type: ignore[assignment]
        self._token = None  # type: ignore[assignment]
        self._base = None  # type: ignore[assignment]
        self._shadow_wire_records = b""
        self._shadow_factor_state = None  # type: ignore[assignment]
        self._factor_value = _NO_FACTOR_VALUE
        self._target_checkpoint = None

    def abort(self) -> None:
        """Discard every shadow mutation; idempotent."""

        if not self._active:
            self._clear_references()
            return

        store = self._store
        token = self._token
        cursor = self._cursor
        try:
            store._release(token)
        except BaseException:
            # Keep the transaction active so an explicit retry or finalizer
            # can finish releasing the store token.
            return
        try:
            self._active = False
            try:
                if not getattr(cursor, "done", False):
                    cursor.close()
            except BaseException:
                pass
        finally:
            self._clear_references()


__all__ = [
    "InstrumentTickColumns",
    "InstrumentTickRollingCommit",
    "InstrumentTickRollingFactor",
    "InstrumentTickRollingGeneration",
    "InstrumentTickRollingState",
    "InstrumentTickRollingStore",
    "InstrumentTickRollingTransaction",
    "InstrumentTickRollingUpdate",
    "InstrumentTickRollingWindow",
]
