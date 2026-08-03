"""Reconciled FAST-derived instrument Event History with a live tail.

The immutable per-instrument derived History endpoint is authoritative.  The
global live Event ring is only an opt-in latency accelerator.  The two
products have independent dense event-sequence domains, so this module never
compares those event sequences.  Cross-product reconciliation uses only the
raw History checkpoint's source-tick frontier and schema-2 ``EventUid``.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Any, Optional, Sequence

from .instrument_derived_event_history import (
    InstrumentDerivedEventCheckpoint,
)
from .models import (
    EventUid,
    HistoryCoverageInfo,
    SessionIdentity,
    UnavailableError,
)
from .order_event_delta_live import (
    LiveOrderEventDeltaOverrunError,
    LiveOrderEventDeltaUnavailableError,
)
from .polars import (
    LivePolarsHistory,
    LivePolarsHistorySnapshot,
    PolarsHistoryClosedError,
    PolarsHistoryCoverageError,
    PolarsHistoryDatasetIdentity,
    PolarsHistoryNotReadyError,
    PolarsHistoryRefreshError,
    PolarsHistoryState,
    PolarsSchemaError,
    _compact_owned_history_chunks,
    _coverage_requirement,
    _derived_history_dataset_identity,
    _positive_float_or_none,
    _require_polars,
    _validate_raw_checkpoint_coverage,
    derived_event_batch_frame,
    derived_event_schema,
    derived_events_frame,
)


_UINT64_MAX = (1 << 64) - 1


@dataclass(frozen=True, slots=True)
class FastDerivedEventHistoryToken:
    """Durable per-instrument cut plus a global live-ring cursor.

    The two fields deliberately are not ordered or compared with each other:
    ``derived_event_sequence_exclusive`` belongs to the instrument History
    product while ``live_next_event_sequence`` belongs to the global live
    Event product.
    """

    checkpoint: InstrumentDerivedEventCheckpoint
    live_next_event_sequence: int

    def __post_init__(self) -> None:
        if not isinstance(
            self.checkpoint, InstrumentDerivedEventCheckpoint
        ):
            raise TypeError(
                "checkpoint must be InstrumentDerivedEventCheckpoint"
            )
        if (
            not isinstance(self.live_next_event_sequence, int)
            or isinstance(self.live_next_event_sequence, bool)
            or self.live_next_event_sequence <= 0
            or self.live_next_event_sequence > _UINT64_MAX
        ):
            raise ValueError(
                "live_next_event_sequence must be a positive uint64"
            )


@dataclass(frozen=True, slots=True)
class PolarsFastDerivedEventHistorySnapshot:
    """One immutable, UID-reconciled FAST-derived History+tail cut."""

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
    def continuity_token(self) -> FastDerivedEventHistoryToken:
        token = self._manifest.continuity_token
        if not isinstance(token, FastDerivedEventHistoryToken):
            raise PolarsSchemaError(
                "FAST-derived live snapshot has an invalid token"
            )
        return token

    @property
    def checkpoint(self) -> InstrumentDerivedEventCheckpoint:
        return self.continuity_token.checkpoint

    @property
    def live_next_event_sequence(self) -> int:
        return self.continuity_token.live_next_event_sequence

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
                "FAST-derived live history lacks a dataset identity"
            )
        return identity


@dataclass(frozen=True, slots=True)
class _EventTailChunk:
    frame: Any
    event_uids: tuple[bytes, ...]
    tick_sequences: tuple[int, ...]

    def __post_init__(self) -> None:
        if (
            self.frame.height != len(self.event_uids)
            or len(self.event_uids) != len(self.tick_sequences)
        ):
            raise PolarsSchemaError(
                "tail frame and UID/source-tick coordinates disagree"
            )

    @property
    def row_count(self) -> int:
        return len(self.event_uids)

    def retained_from(self, boundary: int) -> Optional["_EventTailChunk"]:
        keep = tuple(tick >= boundary for tick in self.tick_sequences)
        if not any(keep):
            return None
        if all(keep):
            return self
        pl = _require_polars()
        frame = self.frame.filter(
            pl.Series("_keep", keep, dtype=pl.Boolean)
        )
        return _EventTailChunk(
            frame,
            tuple(
                uid for uid, retained in zip(self.event_uids, keep)
                if retained
            ),
            tuple(
                tick for tick, retained in zip(self.tick_sequences, keep)
                if retained
            ),
        )


def _uid_coordinates(
    encoded: bytes,
    *,
    coverage: HistoryCoverageInfo,
    instrument_id: int,
    tick_stream_sequence: int,
    source_tick_event_ordinal: int,
) -> None:
    if not isinstance(encoded, bytes) or len(encoded) != 48:
        raise PolarsSchemaError(
            "source-backed Event history requires a 48-byte EventUid"
        )
    try:
        uid = EventUid.from_wire(encoded)
    except (TypeError, ValueError) as error:
        raise PolarsSchemaError("EventUid wire value is invalid") from error
    if (
        uid.session_identity != coverage.identity
        or uid.instrument_id != instrument_id
        or uid.tick_stream_sequence != tick_stream_sequence
        or uid.source_tick_event_ordinal
        != source_tick_event_ordinal
    ):
        raise PolarsHistoryCoverageError(
            "EventUid coordinates disagree with source session/event row"
        )


def _history_chunk_uids(
    chunks: Sequence[Any],
    *,
    coverage: HistoryCoverageInfo,
    instrument_id: int,
    trade_date: int,
) -> set[bytes]:
    """Validate structural identities and return all source-backed UIDs."""

    result: set[bytes] = set()
    for frame in chunks:
        instruments = frame["instrument_id"].to_list()
        dates = frame["trade_date"].to_list()
        ticks = frame["tick_stream_sequence"].to_list()
        ordinals = frame["source_tick_event_ordinal"].to_list()
        uids = frame["event_uid"].to_list()
        for current_instrument, date, tick, ordinal, encoded in zip(
            instruments, dates, ticks, ordinals, uids
        ):
            if current_instrument != instrument_id or date != trade_date:
                raise PolarsHistoryCoverageError(
                    "derived History row belongs to another instrument/day"
                )
            if tick == 0:
                if encoded is not None or ordinal is not None:
                    raise PolarsSchemaError(
                        "source-free derived row invented a UID coordinate"
                    )
                continue
            if ordinal is None:
                raise PolarsSchemaError(
                    "source-backed derived row lacks its UID ordinal"
                )
            _uid_coordinates(
                encoded,
                coverage=coverage,
                instrument_id=instrument_id,
                tick_stream_sequence=tick,
                source_tick_event_ordinal=ordinal,
            )
            if encoded in result:
                raise PolarsSchemaError(
                    "derived History contains a duplicate EventUid"
                )
            result.add(encoded)
    return result


class PolarsFastDerivedEventHistory:
    """Per-instrument derived History with an optional global live tail.

    A complete History generation is reduced through native order state and
    verified at EOF before publication.  Live Event batches are copied into
    immutable Polars chunks and filtered by instrument.  As History advances,
    live rows below its raw source-tick frontier are replaced atomically.
    Every replaced live UID must be present in History; a missing UID fails
    closed and retains only the explicitly requested stale snapshot.

    On ring overrun, the first retained event can be in the middle of a source
    tick.  The reader therefore waits until the raw History frontier is
    *strictly greater* than that event's source tick before reopening the tail.
    """

    def __init__(
        self,
        client,
        history_reader,
        event_control_socket_path,
        *,
        coverage_requirement: str = "from_open",
        live_open_kwargs: Optional[dict[str, Any]] = None,
        tail_poll_interval: Optional[float] = 0.001,
        durable_refresh_interval: Optional[float] = 1.0,
        maximum_rows: Optional[int] = None,
        maximum_chunks: Optional[int] = None,
        compact_after_chunks: Optional[int] = 256,
        autostart: bool = True,
    ) -> None:
        _require_polars()
        coverage = getattr(history_reader, "history_coverage", None)
        _coverage_requirement(coverage, coverage_requirement)
        if live_open_kwargs is None:
            live_arguments: dict[str, Any] = {}
        elif not isinstance(live_open_kwargs, dict):
            raise TypeError("live_open_kwargs must be a dict or None")
        else:
            live_arguments = dict(live_open_kwargs)
        if "start_event_sequence" in live_arguments:
            raise ValueError(
                "live_open_kwargs cannot override start_event_sequence"
            )
        self._client = client
        self._reader = history_reader
        self._event_control_socket_path = event_control_socket_path
        self._live_open_kwargs = live_arguments
        self._coverage = coverage
        self._coverage_requirement = coverage_requirement
        self._tail_poll_interval = _positive_float_or_none(
            tail_poll_interval, "tail_poll_interval"
        )
        self._durable_refresh_interval = _positive_float_or_none(
            durable_refresh_interval, "durable_refresh_interval"
        )
        # Keep the shadow-generation builder under the same bounds as the
        # published manifest.  The destination validates these limits again
        # during promotion, but enforcing them while building the candidate
        # avoids a transient unbounded replacement allocation.
        self._maximum_rows = maximum_rows
        self._maximum_chunks = maximum_chunks
        self._compact_after_chunks = compact_after_chunks
        self._manifest = LivePolarsHistory(
            derived_event_schema(),
            coverage,
            maximum_rows=maximum_rows,
            maximum_chunks=maximum_chunks,
            compact_after_chunks=compact_after_chunks,
        )
        self._condition = threading.Condition(threading.RLock())
        self._thread: Optional[threading.Thread] = None
        self._stop = False
        self._last_error: Optional[BaseException] = None
        self._live_stale_error: Optional[BaseException] = None
        self._refresh_requested = 0
        self._refresh_completed = 0
        self._stream = None
        self._event_session_identity: Optional[SessionIdentity] = None
        self._instrument_id = getattr(
            history_reader, "instrument_id", None
        )
        self._durable_checkpoint: Optional[
            InstrumentDerivedEventCheckpoint
        ] = None
        self._durable_chunks: tuple[Any, ...] = ()
        self._durable_uids: set[bytes] = set()
        self._tail_chunks: tuple[_EventTailChunk, ...] = ()
        self._tail_uids: set[bytes] = set()
        self._tail_scanned_event_sequence: Optional[int] = None
        self._last_live_tick_sequence: Optional[int] = None
        self._published_token: Optional[
            FastDerivedEventHistoryToken
        ] = None
        if autostart:
            self.start()

    @property
    def state(self) -> PolarsHistoryState:
        return self._manifest.state

    @property
    def last_error(self) -> Optional[BaseException]:
        with self._condition:
            return self._last_error

    @property
    def live_stale_error(self) -> Optional[BaseException]:
        """Current transient live-tail outage, if the cache is stale."""

        with self._condition:
            return self._live_stale_error

    @property
    def history_coverage(self) -> HistoryCoverageInfo:
        return self._coverage

    @property
    def dataset_identity(self) -> PolarsHistoryDatasetIdentity:
        return self.latest_snapshot().dataset_identity

    def start(self) -> None:
        with self._condition:
            if self._stop:
                raise PolarsHistoryClosedError(
                    "FAST-derived live history is closed"
                )
            if self._thread is not None:
                return
            self._thread = threading.Thread(
                target=self._run,
                name="l2flow-polars-fast-event-tail",
                daemon=True,
            )
            self._thread.start()

    @staticmethod
    def _validate_history_sequence(frame, begin: int) -> int:
        if not frame.height:
            return begin
        values = frame["derived_event_sequence"]
        end = begin + frame.height
        if values.item(0) != begin or values.item(-1) != end - 1:
            raise PolarsSchemaError(
                "derived History sequence is not at its expected boundary"
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
                    "derived History sequence is not dense/increasing"
                )
        return end

    def _read_history_cursor(
        self,
        cursor,
        base: Optional[InstrumentDerivedEventCheckpoint],
    ) -> tuple[
        tuple[Any, ...], InstrumentDerivedEventCheckpoint, set[bytes]
    ]:
        chunks = []
        emitted = 0
        sequence = (
            1 if base is None else base.derived_event_sequence_exclusive
        )
        with cursor:
            for batch in cursor.batches():
                frame = derived_event_batch_frame(batch)
                sequence = self._validate_history_sequence(
                    frame, sequence
                )
                chunks.append(frame)
                emitted += frame.height
            checkpoint = cursor.verified_checkpoint
        if not isinstance(checkpoint, InstrumentDerivedEventCheckpoint):
            raise PolarsHistoryRefreshError(
                "derived cursor returned the wrong checkpoint type"
            )
        raw = checkpoint.raw_checkpoint
        _validate_raw_checkpoint_coverage(
            self._coverage, raw, self._coverage_requirement
        )
        if base is None:
            expected = checkpoint.derived_event_sequence_exclusive - 1
        else:
            raw.ensure_successor_of(base.raw_checkpoint)
            if (
                checkpoint.instrument_id != base.instrument_id
                or checkpoint.market is not base.market
                or checkpoint.trade_date != base.trade_date
                or checkpoint.derived_event_sequence_exclusive
                < base.derived_event_sequence_exclusive
                or (base.finalized and not checkpoint.finalized)
            ):
                raise PolarsHistoryRefreshError(
                    "derived checkpoint is not a monotone successor"
                )
            expected = (
                checkpoint.derived_event_sequence_exclusive
                - base.derived_event_sequence_exclusive
            )
        if emitted != expected or sequence != (
            checkpoint.derived_event_sequence_exclusive
        ):
            raise PolarsSchemaError(
                "derived rows do not reconcile with the EOF checkpoint"
            )
        if (
            self._instrument_id is not None
            and checkpoint.instrument_id != self._instrument_id
        ):
            raise PolarsHistoryCoverageError(
                "derived reader changed its bound instrument"
            )
        uids = _history_chunk_uids(
            chunks,
            coverage=self._coverage,
            instrument_id=checkpoint.instrument_id,
            trade_date=checkpoint.trade_date,
        )
        return tuple(chunks), checkpoint, uids

    def _read_full(self):
        return self._read_history_cursor(self._reader.read_all(), None)

    def _read_update(self, checkpoint):
        return self._read_history_cursor(
            self._reader.read_updates(checkpoint), checkpoint
        )

    def _validate_stream(self, stream, requested: int) -> None:
        if (
            getattr(stream, "next_sequence", None) != requested
            or getattr(stream, "source_session_identity", None)
            != self._coverage.identity
            or getattr(stream, "history_coverage", None) != self._coverage
        ):
            raise PolarsHistoryCoverageError(
                "derived History and live Event source sessions disagree"
            )
        snapshot = getattr(stream, "control_snapshot", None)
        event_session = getattr(snapshot, "event_session", None)
        event_identity = getattr(event_session, "identity", None)
        if (
            snapshot is None
            or event_session is None
            or event_session.trade_date != self._coverage.trade_date
            or not isinstance(event_identity, SessionIdentity)
        ):
            raise PolarsHistoryCoverageError(
                "live Event control snapshot has an invalid session"
            )
        if self._event_session_identity is None:
            self._event_session_identity = event_identity
        elif event_identity != self._event_session_identity:
            raise PolarsHistoryCoverageError(
                "live Event ring session changed during reconciliation"
            )

    def _open_stream(self, requested: int):
        stream = self._client.open_live_order_events(
            self._event_control_socket_path,
            start_event_sequence=requested,
            **self._live_open_kwargs,
        )
        try:
            self._validate_stream(stream, requested)
            return stream
        except BaseException:
            stream.close()
            raise

    @staticmethod
    def _overrun_observed(error) -> int:
        metadata = getattr(error, "metadata", None)
        observed = getattr(metadata, "observed_sequence", 0)
        if (
            not isinstance(observed, int)
            or isinstance(observed, bool)
            or observed <= 0
            or observed > _UINT64_MAX
        ):
            raise PolarsHistoryRefreshError(
                "live Event overrun lacks a usable retained sequence"
            ) from error
        return observed

    def _open_retained_batch(self, requested: int = 1):
        """Open at the current retained prefix and copy its first batch."""

        while True:
            stream = self._open_stream(requested)
            snapshot = stream.control_snapshot
            published = snapshot.event_published_sequence
            capacity = snapshot.event_session.ring_capacity
            oldest = max(1, published - capacity + 1)
            if requested < oldest:
                stream.close()
                requested = oldest
                continue
            try:
                batch = stream.read_batch()
            except LiveOrderEventDeltaOverrunError as error:
                stream.close()
                observed = self._overrun_observed(error)
                if observed <= requested:
                    raise PolarsHistoryRefreshError(
                        "live Event retained sequence did not move forward"
                    ) from error
                requested = observed
                continue
            return stream, requested, batch

    def _validated_live_events(
        self,
        batch,
        *,
        expected_sequence: int,
        previous_tick: Optional[int],
    ) -> tuple[tuple[Any, ...], Optional[int]]:
        metadata = getattr(batch, "metadata", None)
        next_sequence = getattr(metadata, "next_sequence", None)
        if (
            not isinstance(next_sequence, int)
            or isinstance(next_sequence, bool)
            or next_sequence != expected_sequence + len(batch)
        ):
            raise PolarsSchemaError(
                "live Event batch does not match its global cursor"
            )
        events = tuple(batch.event(index) for index in range(len(batch)))
        last = previous_tick
        batch_uids: set[bytes] = set()
        for event in events:
            uid = getattr(event, "event_uid", None)
            tick = getattr(event, "tick_stream_sequence", 0)
            if uid is None:
                if (
                    tick != 0
                    or getattr(
                        event, "source_tick_event_ordinal", None
                    ) is not None
                ):
                    raise PolarsSchemaError(
                        "live source-backed Event lacks stable EventUid"
                    )
                if event.trade_date != self._coverage.trade_date:
                    raise PolarsHistoryCoverageError(
                        "live source-free Event belongs to another day"
                    )
                # Source-free FINALIZE has no collision-free cross-product
                # coordinate.  Keep it out of the tail; _tail_chunk requires
                # a finalized History checkpoint before it may be dropped.
                continue
            encoded = bytes(uid)
            _uid_coordinates(
                encoded,
                coverage=self._coverage,
                instrument_id=event.instrument_id,
                tick_stream_sequence=tick,
                source_tick_event_ordinal=(
                    event.source_tick_event_ordinal
                ),
            )
            if event.trade_date != self._coverage.trade_date:
                raise PolarsHistoryCoverageError(
                    "live Event row belongs to another trading day"
                )
            if last is not None and tick < last:
                raise PolarsSchemaError(
                    "live Event source ticks moved backwards"
                )
            if encoded in batch_uids:
                raise PolarsSchemaError(
                    "live Event batch contains a duplicate EventUid"
                )
            batch_uids.add(encoded)
            last = tick
        return events, last

    def _tail_chunk(
        self,
        events: Sequence[Any],
        checkpoint: InstrumentDerivedEventCheckpoint,
        durable_uids: set[bytes],
    ) -> Optional[_EventTailChunk]:
        selected = []
        uids = []
        ticks = []
        boundary = (
            checkpoint.raw_checkpoint.tick_stream_sequence_exclusive
        )
        for event in events:
            if event.instrument_id != checkpoint.instrument_id:
                continue
            if event.event_uid is None:
                if not checkpoint.finalized:
                    raise PolarsHistoryCoverageError(
                        "live source-free FINALIZE lacks finalized History proof"
                    )
                # The finalized History generation is authoritative.  Never
                # publish this UID-less live copy or substitute a dense event
                # sequence as identity.
                continue
            encoded = bytes(event.event_uid)
            tick = event.tick_stream_sequence
            if tick < boundary:
                if encoded not in durable_uids:
                    raise PolarsHistoryRefreshError(
                        "derived History is missing an overlapping live UID"
                    )
                continue
            if encoded in durable_uids or encoded in self._tail_uids:
                raise PolarsSchemaError(
                    "FAST-derived History+tail would duplicate EventUid"
                )
            selected.append(event)
            uids.append(encoded)
            ticks.append(tick)
        if not selected:
            return None
        return _EventTailChunk(
            derived_events_frame(tuple(selected)),
            tuple(uids),
            tuple(ticks),
        )

    def _install_initial(
        self,
        chunks: tuple[Any, ...],
        checkpoint: InstrumentDerivedEventCheckpoint,
        durable_uids: set[bytes],
        stream,
        requested: int,
        batch,
    ) -> None:
        chunks = _compact_owned_history_chunks(
            chunks, self._compact_after_chunks
        )
        events, last_tick = self._validated_live_events(
            batch, expected_sequence=requested, previous_tick=None
        )
        tail = self._tail_chunk(events, checkpoint, durable_uids)
        tail_chunks = () if tail is None else (tail,)
        next_sequence = batch.metadata.next_sequence
        token = FastDerivedEventHistoryToken(
            checkpoint, next_sequence
        )
        identity = _derived_history_dataset_identity(checkpoint)
        self._manifest.publish_full(
            chunks + tuple(item.frame for item in tail_chunks),
            generation=checkpoint.raw_checkpoint.generation,
            continuity_token=token,
            expected_total_rows=(
                checkpoint.derived_event_sequence_exclusive
                - 1
                + sum(item.row_count for item in tail_chunks)
            ),
            metadata=checkpoint,
            source=(
                "fast_derived_history_plus_live_tail"
                if tail_chunks
                else "fast_derived_history"
            ),
            dataset_identity=identity,
        )
        self._instrument_id = checkpoint.instrument_id
        self._durable_checkpoint = checkpoint
        self._durable_chunks = chunks
        self._durable_uids = set(durable_uids)
        self._tail_chunks = tail_chunks
        self._tail_uids = {
            uid for item in tail_chunks for uid in item.event_uids
        }
        self._tail_scanned_event_sequence = next_sequence
        self._last_live_tick_sequence = last_tick
        self._published_token = token
        self._stream = stream

    def _advance_unpublished_history(
        self,
        chunks: tuple[Any, ...],
        checkpoint: InstrumentDerivedEventCheckpoint,
        durable_uids: set[bytes],
    ) -> tuple[
        tuple[Any, ...], InstrumentDerivedEventCheckpoint, set[bytes]
    ]:
        delta, successor, delta_uids = self._read_update(checkpoint)
        if successor == checkpoint:
            if delta:
                raise PolarsHistoryRefreshError(
                    "unchanged derived checkpoint emitted rows"
                )
            return chunks, checkpoint, durable_uids
        if durable_uids.intersection(delta_uids):
            raise PolarsSchemaError(
                "derived History update repeats an EventUid"
            )
        identity = _derived_history_dataset_identity(successor)
        if identity != _derived_history_dataset_identity(checkpoint):
            raise PolarsHistoryCoverageError(
                "derived History dataset identity changed"
            )
        return (
            _compact_owned_history_chunks(
                chunks + delta, self._compact_after_chunks
            ),
            successor,
            durable_uids.union(delta_uids),
        )

    def _publish_initial(self) -> None:
        chunks, checkpoint, durable_uids = self._read_full()
        while True:
            with self._condition:
                if self._stop:
                    return
            try:
                stream, requested, batch = self._open_retained_batch()
            except (
                UnavailableError,
                LiveOrderEventDeltaUnavailableError,
            ):
                # Keep the already EOF-verified native History state.  A
                # transient Event control-plane outage must not force a
                # second begin_full on this stateful derived reader.
                with self._condition:
                    self._condition.wait(
                        timeout=self._durable_refresh_interval or 0.05
                    )
                continue
            try:
                events, _ = self._validated_live_events(
                    batch,
                    expected_sequence=requested,
                    previous_tick=None,
                )
                source_ticks = tuple(
                    event.tick_stream_sequence
                    for event in events
                    if event.event_uid is not None
                )
                oldest_tick = (
                    None if not source_ticks else source_ticks[0]
                )
                boundary = (
                    checkpoint.raw_checkpoint
                    .tick_stream_sequence_exclusive
                )
                if oldest_tick is None or boundary > oldest_tick:
                    self._install_initial(
                        chunks,
                        checkpoint,
                        durable_uids,
                        stream,
                        requested,
                        batch,
                    )
                    return
            except BaseException:
                stream.close()
                raise
            stream.close()
            before = checkpoint
            try:
                chunks, checkpoint, durable_uids = (
                    self._advance_unpublished_history(
                        chunks, checkpoint, durable_uids
                    )
                )
            except UnavailableError:
                # The immutable endpoint is not published far enough yet.
                # Preserve both the verified checkpoint and native order
                # state, then retry its update rather than replaying full.
                if getattr(self._reader, "closed", False):
                    raise
                with self._condition:
                    self._condition.wait(
                        timeout=self._durable_refresh_interval or 0.05
                    )
                continue
            if checkpoint == before:
                with self._condition:
                    self._condition.wait(
                        timeout=self._durable_refresh_interval or 0.01
                    )

    def _publish_live_batch(self, batch) -> None:
        checkpoint = self._durable_checkpoint
        scanned = self._tail_scanned_event_sequence
        published = self._published_token
        if checkpoint is None or scanned is None or published is None:
            raise PolarsHistoryNotReadyError(
                "FAST-derived History is not initialized"
            )
        events, last_tick = self._validated_live_events(
            batch,
            expected_sequence=scanned,
            previous_tick=self._last_live_tick_sequence,
        )
        tail = self._tail_chunk(
            events, checkpoint, self._durable_uids
        )
        next_sequence = batch.metadata.next_sequence
        self._tail_scanned_event_sequence = next_sequence
        self._last_live_tick_sequence = last_tick
        if tail is None:
            return
        token = FastDerivedEventHistoryToken(
            checkpoint, next_sequence
        )
        current = self._manifest.snapshot()
        snapshot = self._manifest.publish_delta(
            (tail.frame,),
            expected_base_token=current.continuity_token,
            generation=checkpoint.raw_checkpoint.generation,
            continuity_token=token,
            expected_total_rows=current.row_count + tail.row_count,
            metadata=checkpoint,
            source="fast_derived_history_plus_live_tail",
        )
        self._tail_chunks += (tail,)
        self._tail_uids.update(tail.event_uids)
        self._published_token = snapshot.continuity_token

    def _partition_tail(
        self, boundary: int
    ) -> tuple[
        tuple[_EventTailChunk, ...], set[bytes], set[bytes]
    ]:
        retained = []
        covered: set[bytes] = set()
        retained_uids: set[bytes] = set()
        for chunk in self._tail_chunks:
            for uid, tick in zip(
                chunk.event_uids, chunk.tick_sequences
            ):
                if tick < boundary:
                    covered.add(uid)
                else:
                    retained_uids.add(uid)
            suffix = chunk.retained_from(boundary)
            if suffix is not None:
                retained.append(suffix)
        return tuple(retained), covered, retained_uids

    def _refresh_durable(self) -> bool:
        base = self._durable_checkpoint
        published = self._published_token
        scanned = self._tail_scanned_event_sequence
        if base is None or published is None or scanned is None:
            raise PolarsHistoryNotReadyError(
                "FAST-derived History is not initialized"
            )
        delta, checkpoint, delta_uids = self._read_update(base)
        if checkpoint == base:
            if delta:
                raise PolarsHistoryRefreshError(
                    "unchanged derived checkpoint emitted rows"
                )
            return False
        if self._durable_uids.intersection(delta_uids):
            raise PolarsSchemaError(
                "derived History update repeats an EventUid"
            )
        expected_identity = _derived_history_dataset_identity(base)
        identity = _derived_history_dataset_identity(checkpoint)
        if identity != expected_identity:
            raise PolarsHistoryCoverageError(
                "derived History dataset identity changed"
            )
        durable_chunks = _compact_owned_history_chunks(
            self._durable_chunks + delta,
            self._compact_after_chunks,
        )
        durable_uids = self._durable_uids.union(delta_uids)
        boundary = (
            checkpoint.raw_checkpoint.tick_stream_sequence_exclusive
        )
        tail_chunks, covered_uids, retained_uids = (
            self._partition_tail(boundary)
        )
        missing = covered_uids.difference(durable_uids)
        if missing:
            raise PolarsHistoryRefreshError(
                "derived History is missing an overlapping live UID"
            )
        if durable_uids.intersection(retained_uids):
            raise PolarsSchemaError(
                "History frontier and retained live UIDs overlap"
            )
        token = FastDerivedEventHistoryToken(checkpoint, scanned)
        candidate = LivePolarsHistory(
            derived_event_schema(),
            self._coverage,
            maximum_rows=self._maximum_rows,
            maximum_chunks=self._maximum_chunks,
            compact_after_chunks=self._compact_after_chunks,
        )
        try:
            candidate_snapshot = candidate.publish_full(
                durable_chunks
                + tuple(item.frame for item in tail_chunks),
                generation=checkpoint.raw_checkpoint.generation,
                continuity_token=token,
                expected_total_rows=(
                    checkpoint.derived_event_sequence_exclusive
                    - 1
                    + sum(item.row_count for item in tail_chunks)
                ),
                metadata=checkpoint,
                source="fast_derived_history_reconciled_tail",
                dataset_identity=identity,
            )
            current = self._manifest.snapshot()
            promoted = self._manifest.promote(
                candidate_snapshot,
                expected_base_token=current.continuity_token,
                continuity_check=lambda observed, replacement: (
                    observed.continuity_token == published
                    and replacement.continuity_token == token
                    and replacement.dataset_identity == expected_identity
                    and not missing
                ),
                source="fast_derived_history_reconciled_tail",
            )
        finally:
            candidate.close()
        self._durable_checkpoint = checkpoint
        self._durable_chunks = durable_chunks
        self._durable_uids = durable_uids
        self._tail_chunks = tail_chunks
        self._tail_uids = retained_uids
        self._published_token = promoted.continuity_token
        return True

    def _repair_overrun(self, error) -> None:
        requested = self._overrun_observed(error)
        stream = self._stream
        if stream is not None:
            stream.close()
        self._stream = None
        while True:
            with self._condition:
                if self._stop:
                    return
            candidate, start, batch = self._open_retained_batch(requested)
            try:
                events, _ = self._validated_live_events(
                    batch,
                    expected_sequence=start,
                    previous_tick=None,
                )
                if not events:
                    raise PolarsHistoryRefreshError(
                        "overrun repair could not inspect retained Event"
                    )
                source_ticks = tuple(
                    event.tick_stream_sequence
                    for event in events
                    if event.event_uid is not None
                )
                checkpoint = self._durable_checkpoint
                if checkpoint is None:
                    raise PolarsHistoryNotReadyError(
                        "overrun occurred before History initialization"
                    )
                boundary = (
                    checkpoint.raw_checkpoint
                    .tick_stream_sequence_exclusive
                )
                if (
                    (not source_ticks and checkpoint.finalized)
                    or (
                        source_ticks
                        and boundary > source_ticks[0]
                    )
                ):
                    self._stream = candidate
                    self._tail_scanned_event_sequence = start
                    self._last_live_tick_sequence = None
                    self._publish_live_batch(batch)
                    return
            except BaseException:
                candidate.close()
                raise
            candidate.close()
            try:
                changed = self._refresh_durable()
            except UnavailableError:
                if getattr(self._reader, "closed", False):
                    raise
                changed = False
            requested = start
            if not changed:
                with self._condition:
                    self._condition.wait(
                        timeout=self._durable_refresh_interval or 0.01
                    )

    def _reconnect_live_stream(self, initial_error: BaseException) -> bool:
        """Reconnect at the exact last validated live cursor.

        The committed manifest remains readable in REFRESHING state.  A new
        stream is accepted only through _validate_stream, which pins both the
        source identity and the Event-ring identity.  No cursor is advanced
        while the control plane is unavailable.
        """

        requested = self._tail_scanned_event_sequence
        if requested is None:
            raise PolarsHistoryNotReadyError(
                "live Event outage occurred before cursor initialization"
            )
        stream = self._stream
        if stream is not None:
            stream.close()
        self._stream = None
        with self._condition:
            self._live_stale_error = initial_error
            self._condition.notify_all()
        self._manifest.mark_refreshing()

        while True:
            with self._condition:
                if self._stop:
                    return False
            candidate = None
            try:
                candidate = self._open_stream(requested)
                with self._condition:
                    if self._stop:
                        candidate.close()
                        return False
                batch = candidate.read_batch()
                # Validation and publication happen before this reader becomes
                # the active stream. Any identity/cursor mismatch fails closed.
                self._publish_live_batch(batch)
                self._stream = candidate
                candidate = None
                self._manifest.finish_refresh()
                with self._condition:
                    self._live_stale_error = None
                    self._condition.notify_all()
                return True
            except (
                UnavailableError,
                LiveOrderEventDeltaUnavailableError,
            ) as error:
                if candidate is not None:
                    candidate.close()
                with self._condition:
                    self._live_stale_error = error
                    if self._stop:
                        return False
                    self._condition.wait(
                        timeout=self._durable_refresh_interval or 0.05
                    )
            except BaseException:
                if candidate is not None:
                    candidate.close()
                raise

    def _fail(self, error: BaseException) -> None:
        with self._condition:
            self._last_error = error
            self._condition.notify_all()
        self._manifest.fail(error)

    def _run(self) -> None:
        try:
            while True:
                with self._condition:
                    if self._stop:
                        return
                try:
                    self._publish_initial()
                    break
                except (UnavailableError, LiveOrderEventDeltaUnavailableError):
                    if getattr(self._reader, "closed", False):
                        raise
                    with self._condition:
                        self._condition.wait(
                            timeout=self._durable_refresh_interval or 0.05
                        )
            with self._condition:
                self._refresh_completed = self._refresh_requested
                self._condition.notify_all()
            last_refresh = time.monotonic()
            while True:
                with self._condition:
                    if self._stop:
                        return
                    requested = self._refresh_requested
                due = (
                    self._durable_refresh_interval is not None
                    and time.monotonic() - last_refresh
                    >= self._durable_refresh_interval
                )
                if requested > self._refresh_completed or due:
                    self._refresh_durable()
                    last_refresh = time.monotonic()
                    with self._condition:
                        self._refresh_completed = max(
                            self._refresh_completed, requested
                        )
                        self._condition.notify_all()
                stream = self._stream
                if stream is None:
                    raise PolarsHistoryRefreshError(
                        "FAST live Event reader disappeared"
                    )
                try:
                    batch = stream.read_batch()
                except LiveOrderEventDeltaOverrunError as error:
                    self._repair_overrun(error)
                    last_refresh = time.monotonic()
                    continue
                except LiveOrderEventDeltaUnavailableError as error:
                    if not self._reconnect_live_stream(error):
                        return
                    last_refresh = time.monotonic()
                    continue
                if len(batch):
                    self._publish_live_batch(batch)
                    continue
                # Even an empty successful read has a validated cursor.  It
                # normally does not advance, but consume it defensively.
                self._publish_live_batch(batch)
                with self._condition:
                    self._condition.wait(timeout=self._tail_poll_interval)
        except BaseException as error:
            with self._condition:
                stopping = self._stop
            if not stopping:
                self._fail(error)

    def wait_ready(self, timeout: Optional[float] = None) -> None:
        self._manifest.wait_ready(timeout)

    def refresh(self, timeout: Optional[float] = None) -> None:
        deadline = (
            None
            if timeout is None
            else time.monotonic()
            + _positive_float_or_none(timeout, "timeout")
        )
        with self._condition:
            if self._stop:
                raise PolarsHistoryClosedError(
                    "FAST-derived live history is closed"
                )
            if self._thread is None:
                raise PolarsHistoryNotReadyError(
                    "FAST-derived live history has not been started"
                )
            if self._last_error is not None:
                raise PolarsHistoryRefreshError(
                    "FAST-derived live updater failed"
                ) from self._last_error
            self._refresh_requested += 1
            request = self._refresh_requested
            self._condition.notify_all()
            while self._refresh_completed < request:
                if self._last_error is not None:
                    raise PolarsHistoryRefreshError(
                        "FAST-derived live refresh failed"
                    ) from self._last_error
                remaining = (
                    None
                    if deadline is None
                    else max(0.0, deadline - time.monotonic())
                )
                if remaining == 0.0:
                    raise TimeoutError(
                        "timed out refreshing FAST-derived live history"
                    )
                self._condition.wait(timeout=remaining)

    def latest_snapshot(
        self, *, allow_stale: bool = False
    ) -> PolarsFastDerivedEventHistorySnapshot:
        return PolarsFastDerivedEventHistorySnapshot(
            self._manifest.snapshot(allow_stale=allow_stale)
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
        stream = self._stream
        if stream is not None:
            stream.close()
        try:
            self._reader.close()
        finally:
            self._manifest.close()

    def __enter__(self) -> "PolarsFastDerivedEventHistory":
        self.start()
        return self

    def __exit__(self, _type, _value, _traceback) -> None:
        self.close()


__all__ = [
    "FastDerivedEventHistoryToken",
    "PolarsFastDerivedEventHistory",
    "PolarsFastDerivedEventHistorySnapshot",
]
