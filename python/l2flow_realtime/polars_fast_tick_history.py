"""Reconciled immutable FAST instrument history with an optional live tail."""

from __future__ import annotations

import struct
import threading
import time
from dataclasses import dataclass
from typing import Any, Optional, Sequence

from ._history_columns import tick_columns
from ._history_worker_protocol import (
    DEFAULT_RESULT_COLUMNS,
    RESULT_COLUMN_BY_NAME,
)
from .checkpoint import InstrumentTickDeltaCheckpoint
from .fast_tick_live import FastTickBatch, FastTickStreamReader
from .models import (
    HistoryCoverageInfo,
    TickOverrunError,
    UnavailableError,
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
    _compact_owned_history_chunks,
    _owned_raw_series,
    _positive_float_or_none,
    _raw_history_dataset_identity,
    raw_event_batch_frame,
    raw_event_schema,
)
from .wire import TICK_BYTES


_TICK_SEQUENCE = struct.Struct("<Q")


@dataclass(frozen=True, slots=True)
class FastTickHistoryToken:
    """Exact durable checkpoint plus consumed global FAST ring frontier."""

    checkpoint: InstrumentTickDeltaCheckpoint
    live_next_sequence: int

    def __post_init__(self) -> None:
        if not isinstance(
            self.checkpoint, InstrumentTickDeltaCheckpoint
        ):
            raise TypeError(
                "checkpoint must be InstrumentTickDeltaCheckpoint"
            )
        if (
            not isinstance(self.live_next_sequence, int)
            or isinstance(self.live_next_sequence, bool)
            or self.live_next_sequence
            < self.checkpoint.tick_stream_sequence_exclusive
            or self.live_next_sequence > (1 << 64) - 1
        ):
            raise ValueError(
                "live_next_sequence must be at or after the durable cut"
            )


@dataclass(frozen=True, slots=True)
class _TailChunk:
    frame: Any
    wire_records: bytes

    @property
    def row_count(self) -> int:
        return len(self.wire_records) // TICK_BYTES


def _wire_frame(wire_records: bytes, columns: tuple[str, ...]):
    """Copy selected Wire V2 tick columns into an owned Polars frame."""

    if len(wire_records) % TICK_BYTES:
        raise ValueError("FAST tail bytes are not tick-record aligned")
    values = tick_columns(wire_records).read_columns(*columns)
    # _owned_raw_series first creates a C-width owned array.  Polars then owns
    # its immutable series buffer, so no view can outlive or alias the ring.
    from .polars import _require_polars

    pl = _require_polars()
    return pl.DataFrame(
        [_owned_raw_series(name, values[name]) for name in columns]
    )


def _wire_suffix(wire_records: bytes, sequence: int) -> bytes:
    """Select instrument rows at or after one global sequence boundary."""

    pieces = []
    for offset in range(0, len(wire_records), TICK_BYTES):
        current = _TICK_SEQUENCE.unpack_from(
            wire_records, offset + 32
        )[0]
        if current >= sequence:
            pieces.append(wire_records[offset : offset + TICK_BYTES])
    return b"".join(pieces)


class PolarsFastTickHistory:
    """Full+delta FAST raw history with reconciled low-latency ring tail.

    The immutable generation endpoint is the authority for completeness.  The
    ring is only an acceleration layer.  Normal reads append owned tail chunks;
    an overrun waits for a generation whose global tick frontier reaches the
    observed ring lower bound, rebuilds the overlapping suffix in shadow from
    History, and atomically replaces the manifest.  Therefore no missing or
    duplicate instrument row is exposed by the repaired generation.

    This object is entirely opt-in.  Construction starts one Python thread;
    pass ``autostart=False`` when lifecycle control must be explicit.  It
    never changes producer-side callback work.
    """

    def __init__(
        self,
        client,
        history_reader,
        instrument,
        *,
        history_coverage: HistoryCoverageInfo,
        columns: Sequence[str] = DEFAULT_RESULT_COLUMNS,
        coverage_requirement: str = "from_open",
        batch_records: int = 4096,
        tail_poll_interval: float = 0.001,
        durable_refresh_interval: Optional[float] = 1.0,
        maximum_rows: Optional[int] = None,
        maximum_chunks: Optional[int] = None,
        compact_after_chunks: Optional[int] = 256,
        autostart: bool = True,
        owns_tail_client: bool = False,
    ) -> None:
        if not isinstance(history_coverage, HistoryCoverageInfo):
            raise TypeError("history_coverage must be HistoryCoverageInfo")
        if not history_coverage.available:
            raise PolarsHistoryCoverageError(
                "FAST History coverage is unavailable"
            )
        if coverage_requirement not in (
            "from_open",
            "allow_process_start_partial",
        ):
            raise ValueError(
                "unsupported coverage_requirement"
            )
        if (
            coverage_requirement == "from_open"
            and not history_coverage.coverage_from_open
        ):
            raise PolarsHistoryCoverageError(
                "FAST session is process-start partial, not from-open"
            )
        names = tuple(columns)
        if not names or len(set(names)) != len(names):
            raise ValueError("columns must be nonempty and unique")
        if any(name not in RESULT_COLUMN_BY_NAME for name in names):
            raise KeyError("unknown FAST raw-history column")
        if (
            not isinstance(batch_records, int)
            or isinstance(batch_records, bool)
            or batch_records <= 0
            or batch_records > 65_536
        ):
            raise ValueError("batch_records is outside the record limit")
        if not isinstance(owns_tail_client, bool):
            raise TypeError("owns_tail_client must be bool")
        tail_poll = _positive_float_or_none(
            tail_poll_interval, "tail_poll_interval"
        )
        durable_refresh = _positive_float_or_none(
            durable_refresh_interval, "durable_refresh_interval"
        )
        self._client = client
        self._owns_tail_client = owns_tail_client
        self._reader = history_reader
        self._instrument = instrument
        self._coverage = history_coverage
        self._columns = names
        self._batch_records = batch_records
        self._tail_poll_interval = tail_poll
        self._durable_refresh_interval = durable_refresh
        self._maximum_rows = maximum_rows
        self._maximum_chunks = maximum_chunks
        self._compact_after_chunks = compact_after_chunks
        self._manifest = LivePolarsHistory(
            raw_event_schema(names),
            history_coverage,
            maximum_rows=maximum_rows,
            maximum_chunks=maximum_chunks,
            compact_after_chunks=compact_after_chunks,
        )
        self._condition = threading.Condition(threading.RLock())
        self._thread: Optional[threading.Thread] = None
        self._stop = False
        self._last_error: Optional[BaseException] = None
        self._refresh_requested = 0
        self._refresh_completed = 0
        self._stream: Optional[FastTickStreamReader] = None
        self._durable_checkpoint: Optional[
            InstrumentTickDeltaCheckpoint
        ] = None
        self._durable_chunks: tuple[Any, ...] = ()
        self._tail_chunks: tuple[_TailChunk, ...] = ()
        self._tail_scanned_sequence: Optional[int] = None
        self._published_token: Optional[FastTickHistoryToken] = None
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
    def columns(self) -> tuple[str, ...]:
        return self._columns

    @property
    def history_coverage(self) -> HistoryCoverageInfo:
        return self._coverage

    @property
    def dataset_identity(self) -> PolarsHistoryDatasetIdentity:
        snapshot = self._manifest.snapshot()
        identity = snapshot.dataset_identity
        if identity is None:
            raise PolarsHistoryCoverageError(
                "FAST history lacks a stable dataset identity"
            )
        return identity

    def start(self) -> None:
        with self._condition:
            if self._stop:
                raise PolarsHistoryClosedError(
                    "FAST Polars history is closed"
                )
            if self._thread is not None:
                return
            self._thread = threading.Thread(
                target=self._run,
                name="l2flow-polars-fast-tail",
                daemon=True,
            )
            self._thread.start()

    def _open_full(self):
        return self._reader.read_all(
            self._instrument, batch_records=self._batch_records
        )

    def _open_update(self, checkpoint):
        return self._reader.read_updates(
            self._instrument,
            checkpoint,
            batch_records=self._batch_records,
        )

    def _open_stream(self, sequence: int):
        """Open one exact live seam without crossing session or frontier."""

        stream = self._client.open_fast_tick_stream(
            sequence,
            batch_records=self._batch_records,
        )
        try:
            if (
                stream.session_identity != self._coverage.identity
                or stream.trade_date != self._coverage.trade_date
                or stream.next_sequence != sequence
            ):
                raise PolarsHistoryCoverageError(
                    "FAST History and live-tail sessions/frontiers disagree"
                )
            return stream
        except BaseException:
            stream.close()
            raise

    def _read_history_cursor(self, cursor, base):
        chunks = []
        emitted = 0
        with cursor:
            for batch in cursor.batches():
                frame = raw_event_batch_frame(
                    batch, columns=self._columns
                )
                chunks.append(frame)
                emitted += frame.height
            checkpoint = cursor.verified_checkpoint
        if not isinstance(checkpoint, InstrumentTickDeltaCheckpoint):
            raise PolarsHistoryRefreshError(
                "History cursor returned the wrong checkpoint type"
            )
        if base is None:
            expected = checkpoint.instrument_tick_record_count
        else:
            checkpoint.ensure_successor_of(base)
            expected = (
                checkpoint.instrument_tick_record_count
                - base.instrument_tick_record_count
            )
        if emitted != expected:
            raise PolarsHistoryRefreshError(
                "History rows disagree with the verified checkpoint"
            )
        if (
            checkpoint.run_id != self._coverage.run_id
            or checkpoint.session_epoch != self._coverage.session_epoch
            or checkpoint.trade_date != self._coverage.trade_date
            or checkpoint.coverage_from_open
            != self._coverage.coverage_from_open
            or not checkpoint.record_coverage_complete
            or not checkpoint.tick_record_coverage_complete
        ):
            raise PolarsHistoryCoverageError(
                "History checkpoint and temporal coverage disagree"
            )
        return tuple(chunks), checkpoint

    def _publish_initial(self) -> None:
        chunks, checkpoint = self._read_history_cursor(
            self._open_full(), None
        )
        durable_chunks = _compact_owned_history_chunks(
            chunks, self._compact_after_chunks
        )
        token = FastTickHistoryToken(
            checkpoint, checkpoint.tick_stream_sequence_exclusive
        )
        stream = self._open_stream(token.live_next_sequence)
        try:
            self._manifest.publish_full(
                durable_chunks,
                generation=checkpoint.generation,
                continuity_token=token,
                expected_total_rows=(
                    checkpoint.instrument_tick_record_count
                ),
                metadata=checkpoint,
                source="fast_history",
                dataset_identity=_raw_history_dataset_identity(
                    checkpoint, self._columns
                ),
            )
        except BaseException:
            stream.close()
            raise
        self._durable_checkpoint = checkpoint
        # Resolve an InstrumentKey only for the initial full scan.  Every
        # later update is numeric and therefore cannot contend on the caller's
        # point-read client merely to repeat a stable catalog lookup.
        self._instrument = checkpoint.instrument_id
        self._durable_chunks = durable_chunks
        self._tail_scanned_sequence = token.live_next_sequence
        self._published_token = token
        self._stream = stream

    def _publish_tail(self, batch: FastTickBatch) -> None:
        checkpoint = self._durable_checkpoint
        token = self._published_token
        scanned = self._tail_scanned_sequence
        if checkpoint is None or token is None or scanned is None:
            raise PolarsHistoryNotReadyError(
                "FAST durable history is not initialized"
            )
        if (
            batch.session_identity != self._coverage.identity
            or batch.trade_date != self._coverage.trade_date
            or batch.first_sequence != scanned
        ):
            raise PolarsHistoryRefreshError(
                "FAST tail batch is discontinuous or changed session"
            )
        selected = batch.instrument_wire_records(
            checkpoint.instrument_id
        )
        if not selected:
            # The public token may remain at the last instrument row, but the
            # private global frontier records that this entire interval was
            # scanned and contained no row for the selected instrument.
            self._tail_scanned_sequence = batch.next_sequence
            return
        frame = _wire_frame(selected, self._columns)
        next_token = FastTickHistoryToken(
            checkpoint, batch.next_sequence
        )
        current = self._manifest.snapshot()
        snapshot = self._manifest.publish_delta(
            (frame,),
            expected_base_token=current.continuity_token,
            generation=checkpoint.generation,
            continuity_token=next_token,
            expected_total_rows=current.row_count + frame.height,
            metadata=checkpoint,
            source="fast_history_plus_live_tail",
        )
        self._tail_chunks += (_TailChunk(frame, selected),)
        self._tail_scanned_sequence = batch.next_sequence
        self._published_token = snapshot.continuity_token

    def _tail_suffix(
        self, boundary: int
    ) -> tuple[tuple[_TailChunk, ...], int]:
        chunks = []
        count = 0
        for chunk in self._tail_chunks:
            suffix = _wire_suffix(chunk.wire_records, boundary)
            if not suffix:
                continue
            frame = _wire_frame(suffix, self._columns)
            current = _TailChunk(frame, suffix)
            chunks.append(current)
            count += current.row_count
        return tuple(chunks), count

    def _refresh_durable(self) -> bool:
        base = self._durable_checkpoint
        published = self._published_token
        stream = self._stream
        scanned = self._tail_scanned_sequence
        if (
            base is None
            or published is None
            or stream is None
            or scanned is None
        ):
            raise PolarsHistoryNotReadyError(
                "FAST durable history is not initialized"
            )
        delta_chunks, checkpoint = self._read_history_cursor(
            self._open_update(base), base
        )
        if checkpoint == base:
            if delta_chunks:
                raise PolarsHistoryRefreshError(
                    "unchanged History checkpoint emitted rows"
                )
            return False

        durable_chunks = _compact_owned_history_chunks(
            self._durable_chunks + delta_chunks,
            self._compact_after_chunks,
        )
        boundary = checkpoint.tick_stream_sequence_exclusive
        suffix, suffix_count = self._tail_suffix(boundary)
        next_sequence = max(scanned, boundary)
        next_token = FastTickHistoryToken(checkpoint, next_sequence)
        candidate = LivePolarsHistory(
            raw_event_schema(self._columns),
            self._coverage,
            maximum_rows=self._maximum_rows,
            maximum_chunks=self._maximum_chunks,
            compact_after_chunks=self._compact_after_chunks,
        )
        replacement_stream = None
        try:
            if boundary > scanned:
                replacement_stream = self._open_stream(boundary)
            candidate_snapshot = candidate.publish_full(
                durable_chunks + tuple(item.frame for item in suffix),
                generation=checkpoint.generation,
                continuity_token=next_token,
                expected_total_rows=(
                    checkpoint.instrument_tick_record_count + suffix_count
                ),
                metadata=checkpoint,
                source="fast_history_reconciled_tail",
                dataset_identity=_raw_history_dataset_identity(
                    checkpoint, self._columns
                ),
            )
            current = self._manifest.snapshot()
            promoted = self._manifest.promote(
                candidate_snapshot,
                expected_base_token=current.continuity_token,
                continuity_check=lambda observed, replacement: (
                    observed.continuity_token == published
                    and replacement.continuity_token == next_token
                    and replacement.row_count
                    == checkpoint.instrument_tick_record_count + suffix_count
                ),
                source="fast_history_reconciled_tail",
            )
        except BaseException:
            if replacement_stream is not None:
                replacement_stream.close()
            raise
        finally:
            candidate.close()
        self._durable_checkpoint = checkpoint
        self._durable_chunks = durable_chunks
        self._tail_chunks = suffix
        self._tail_scanned_sequence = next_sequence
        self._published_token = promoted.continuity_token
        if replacement_stream is not None:
            stream.close()
            self._stream = replacement_stream
        return True

    def _repair_overrun(self, observed_sequence: int) -> None:
        while True:
            with self._condition:
                if self._stop:
                    return
            self._refresh_durable()
            checkpoint = self._durable_checkpoint
            if (
                checkpoint is not None
                and checkpoint.tick_stream_sequence_exclusive
                >= observed_sequence
            ):
                stream = self._stream
                if (
                    stream is None
                    or self._tail_scanned_sequence
                    != checkpoint.tick_stream_sequence_exclusive
                    or stream.next_sequence
                    != checkpoint.tick_stream_sequence_exclusive
                ):
                    raise PolarsHistoryRefreshError(
                        "FAST overrun repair did not establish the durable "
                        "history-to-tail seam"
                    )
                return
            with self._condition:
                self._condition.wait(
                    timeout=self._durable_refresh_interval or 0.01
                )

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
                except UnavailableError:
                    with self._condition:
                        self._condition.wait(
                            timeout=(
                                self._durable_refresh_interval or 0.05
                            )
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
                now = time.monotonic()
                due = (
                    self._durable_refresh_interval is not None
                    and now - last_refresh
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
                        "FAST tail reader disappeared"
                    )
                try:
                    batch = stream.read()
                except TickOverrunError as error:
                    self._repair_overrun(error.observed_sequence)
                    last_refresh = time.monotonic()
                    continue
                if len(batch):
                    self._publish_tail(batch)
                    continue
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
                    "FAST Polars history is closed"
                )
            if self._thread is None:
                raise PolarsHistoryNotReadyError(
                    "FAST Polars history has not been started"
                )
            if self._last_error is not None:
                raise PolarsHistoryRefreshError(
                    "FAST Polars history updater failed"
                ) from self._last_error
            self._refresh_requested += 1
            requested = self._refresh_requested
            self._condition.notify_all()
            while self._refresh_completed < requested:
                if self._last_error is not None:
                    raise PolarsHistoryRefreshError(
                        "FAST Polars history refresh failed"
                    ) from self._last_error
                remaining = (
                    None
                    if deadline is None
                    else max(0.0, deadline - time.monotonic())
                )
                if remaining == 0.0:
                    raise TimeoutError(
                        "timed out refreshing FAST Polars history"
                    )
                self._condition.wait(timeout=remaining)

    def latest_snapshot(
        self, *, allow_stale: bool = False
    ) -> LivePolarsHistorySnapshot:
        return self._manifest.snapshot(allow_stale=allow_stale)

    def latest_dataframe(self, **kwargs):
        return self.latest_snapshot(**kwargs).dataframe

    def latest_lazyframe(self, **kwargs):
        return self.latest_snapshot(**kwargs).lazyframe

    def spill(self, path, **kwargs) -> str:
        """Atomically spill one pinned FAST History+tail snapshot."""

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
            try:
                self._manifest.close()
            finally:
                if self._owns_tail_client:
                    self._client.close()

    def __enter__(self) -> "PolarsFastTickHistory":
        self.start()
        return self

    def __exit__(self, _type, _value, _traceback) -> None:
        self.close()


__all__ = ["FastTickHistoryToken", "PolarsFastTickHistory"]
