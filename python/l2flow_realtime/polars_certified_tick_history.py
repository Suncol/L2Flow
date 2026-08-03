"""Atomic Polars cache for the append-only CERTIFIED Tick journal.

The native reader remains the cursor and continuity authority.  This module
converts only owned, fully validated batches and publishes complete prefixes
by an atomic manifest swap.  It never reads the bounded FAST ring.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Any, Optional

from .certified_tick_history import (
    CertifiedTickHistoryProducerFailedError,
    CertifiedTickHistoryState,
)
from .models import HistoryCoverageInfo
from .polars import (
    LivePolarsHistory,
    LivePolarsHistorySnapshot,
    PolarsHistoryClosedError,
    PolarsHistoryCoverageError,
    PolarsHistoryDatasetIdentity,
    PolarsHistoryRefreshError,
    PolarsHistoryState,
    PolarsSchemaError,
    _certified_tick_dataset_identity,
    _coverage_requirement,
    _positive_float_or_none,
    _require_polars,
    certified_tick_batch_frame,
    certified_tick_schema,
)


@dataclass(frozen=True, slots=True)
class CertifiedTickHistoryToken:
    """Stable boundary of one published dense canonical prefix."""

    next_canonical_apply_sequence: int
    canonical_apply_frontier: int
    generation: int
    state: int
    failure: int


@dataclass(frozen=True, slots=True)
class _CertifiedTickPolarsMetadata:
    status: Any
    next_canonical_apply_sequence: int
    caught_up: bool


@dataclass(frozen=True, slots=True)
class PolarsCertifiedTickHistorySnapshot:
    """One pinned, immutable prefix of the CERTIFIED Tick journal."""

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
    def next_canonical_apply_sequence(self) -> int:
        return self._manifest.metadata.next_canonical_apply_sequence

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
                "CERTIFIED Tick history lacks a dataset identity"
            )
        return identity


class PolarsCertifiedTickHistory:
    """Maintain a complete from-open CERTIFIED Tick DataFrame/LazyFrame.

    Construction requires a reader positioned at canonical sequence 1.
    Initial chunks stay private until the reader reaches one coherent
    frontier.  Dense tail batches are then appended to a shadow manifest and
    atomically published.  Existing snapshots remain pinned to old immutable
    buffers, and configured memory limits fail closed without row eviction.
    """

    def __init__(
        self,
        reader,
        *,
        include_wire_payload: bool = False,
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
        if not isinstance(include_wire_payload, bool):
            raise TypeError("include_wire_payload must be bool")
        coverage = getattr(reader, "history_coverage", None)
        _coverage_requirement(coverage, "from_open")
        if getattr(
            reader, "next_canonical_apply_sequence", None
        ) != 1:
            raise PolarsHistoryCoverageError(
                "complete CERTIFIED Tick Polars history must start at "
                "canonical apply sequence 1"
            )
        expected_identity = _certified_tick_dataset_identity(
            include_wire_payload=include_wire_payload
        )
        if dataset_identity is None:
            dataset_identity = expected_identity
        elif not isinstance(
            dataset_identity, PolarsHistoryDatasetIdentity
        ):
            raise TypeError(
                "dataset_identity must be PolarsHistoryDatasetIdentity"
            )
        if (
            dataset_identity.product_kind
            != expected_identity.product_kind
            or dataset_identity.instrument_id is not None
            or dataset_identity.projection_columns
            != expected_identity.projection_columns
            or dataset_identity.payload_projection
            != expected_identity.payload_projection
            or dataset_identity.polars_schema_version
            != expected_identity.polars_schema_version
            or dataset_identity.market is not None
        ):
            raise PolarsHistoryCoverageError(
                "CERTIFIED Tick dataset identity does not match its schema"
            )
        self._reader = reader
        self._include_wire_payload = include_wire_payload
        self._dataset_identity = dataset_identity
        self._poll_interval = _positive_float_or_none(
            poll_interval, "poll_interval"
        )
        self._manifest = LivePolarsHistory(
            certified_tick_schema(
                include_wire_payload=include_wire_payload
            ),
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
                    "CERTIFIED Tick history is closed"
                )
            if self._thread is not None:
                return
            thread = threading.Thread(
                target=self._run,
                name="l2flow-polars-certified-tick-history",
                daemon=True,
            )
            self._thread = thread
            thread.start()

    @staticmethod
    def _boundary(batch) -> CertifiedTickHistoryToken:
        status = batch.status
        return CertifiedTickHistoryToken(
            next_canonical_apply_sequence=(
                batch.next_canonical_apply_sequence
            ),
            canonical_apply_frontier=(
                status.canonical_apply_frontier
            ),
            generation=status.generation,
            state=int(status.state),
            failure=int(status.failure),
        )

    @staticmethod
    def _caught_up(batch) -> bool:
        return (
            batch.next_canonical_apply_sequence
            == batch.status.canonical_apply_frontier + 1
        )

    @staticmethod
    def _require_publishable_lifecycle(batch) -> None:
        """Fail closed before publishing a terminal producer failure.

        A native read can validly return the last readable prefix together
        with its coherent terminal status.  Rows accompanied by FAILED are
        useful only to the low-level diagnostic reader: they must never make
        a Polars manifest READY, nor extend its last-good prefix.  COMPLETE
        and ACTIVE are the only lifecycle states from which an atomic prefix
        may be published.
        """

        state = batch.status.state
        if state is CertifiedTickHistoryState.FAILED:
            raise CertifiedTickHistoryProducerFailedError(batch.status)
        if state is CertifiedTickHistoryState.COMPLETE:
            return
        if state is CertifiedTickHistoryState.ACTIVE:
            return
        raise PolarsSchemaError(
            "CERTIFIED Tick batch has an unknown lifecycle state"
        )

    def _read_batch(self):
        begin = self._reader.next_canonical_apply_sequence
        batch = self._reader.read_batch()
        if batch.history_coverage != self._manifest.history_coverage:
            raise PolarsHistoryCoverageError(
                "CERTIFIED Tick batch coverage changed after attachment"
            )
        end = batch.next_canonical_apply_sequence
        if end != begin + len(batch):
            raise PolarsSchemaError(
                "CERTIFIED Tick cursor does not reconcile with row count"
            )
        if end == 0 or end - 1 > batch.status.canonical_apply_frontier:
            raise PolarsSchemaError(
                "CERTIFIED Tick batch exceeds its coherent frontier"
            )
        frame = certified_tick_batch_frame(
            batch,
            include_wire_payload=self._include_wire_payload,
        )
        if frame.height != len(batch):
            raise PolarsSchemaError(
                "CERTIFIED Tick DataFrame row count changed"
            )
        if frame.height:
            sequence = frame["canonical_apply_sequence"]
            if sequence.item(0) != begin or sequence.item(-1) != end - 1:
                raise PolarsSchemaError(
                    "CERTIFIED Tick sequence does not match its cursor"
                )
            if frame.height > 1:
                pl = _require_polars()
                dense = frame.select(
                    pl.col("canonical_apply_sequence")
                    .diff()
                    .drop_nulls()
                    .eq(1)
                    .all()
                ).item()
                if dense is not True:
                    raise PolarsSchemaError(
                        "CERTIFIED Tick canonical sequence is not dense"
                    )
        return frame, batch

    @staticmethod
    def _metadata(batch, caught_up: bool):
        return _CertifiedTickPolarsMetadata(
            status=batch.status,
            next_canonical_apply_sequence=(
                batch.next_canonical_apply_sequence
            ),
            caught_up=caught_up,
        )

    def _run(self) -> None:
        initial_chunks = []
        try:
            while True:
                with self._condition:
                    if self._stop:
                        return
                frame, batch = self._read_batch()
                self._require_publishable_lifecycle(batch)
                if frame.height:
                    initial_chunks.append(frame)
                if self._caught_up(batch):
                    boundary = self._boundary(batch)
                    self._manifest.publish_full(
                        initial_chunks,
                        generation=boundary.generation,
                        continuity_token=boundary,
                        expected_total_rows=(
                            boundary.next_canonical_apply_sequence - 1
                        ),
                        metadata=self._metadata(batch, True),
                        source="certified_tick_history_to_tail",
                        dataset_identity=self._dataset_identity,
                    )
                    # The committed manifest owns this prefix.  Keeping the
                    # accumulation list in the lifetime-long tail loop would
                    # pin every pre-compaction input batch until close().
                    initial_chunks.clear()
                    break
        except BaseException as error:
            self._manifest.fail(error)
            with self._condition:
                self._condition.notify_all()
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
                self._require_publishable_lifecycle(batch)
                boundary = self._boundary(batch)
                caught_up = self._caught_up(batch)
                if frame.height or boundary != base.continuity_token:
                    self._manifest.publish_delta(
                        (frame,) if frame.height else (),
                        expected_base_token=base.continuity_token,
                        generation=boundary.generation,
                        continuity_token=boundary,
                        expected_total_rows=(
                            boundary.next_canonical_apply_sequence - 1
                        ),
                        metadata=self._metadata(batch, caught_up),
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
                    "CERTIFIED Tick history is closed"
                )
            if self._manifest.state is PolarsHistoryState.FAILED:
                raise PolarsHistoryRefreshError(
                    "CERTIFIED Tick updater failed"
                ) from self._manifest.last_error
            self._refresh_requested += 1
            request = self._refresh_requested
            self._condition.notify_all()
            while self._refresh_completed < request:
                if self._manifest.state is PolarsHistoryState.FAILED:
                    raise PolarsHistoryRefreshError(
                        "CERTIFIED Tick refresh failed"
                    ) from self._manifest.last_error
                if self._stop:
                    raise PolarsHistoryClosedError(
                        "CERTIFIED Tick history is closed"
                    )
                remaining = self._remaining(deadline)
                if remaining == 0.0:
                    raise TimeoutError(
                        "timed out draining CERTIFIED Tick prefix"
                    )
                self._condition.wait(timeout=remaining)

    def latest_snapshot(
        self,
        *,
        consistency: str = "cached",
        timeout: Optional[float] = None,
        allow_stale: bool = False,
    ) -> PolarsCertifiedTickHistorySnapshot:
        if consistency not in ("cached", "latest_published"):
            raise ValueError("unsupported consistency")
        if consistency == "latest_published":
            self.refresh(timeout)
        return PolarsCertifiedTickHistorySnapshot(
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
        try:
            self._reader.close()
        finally:
            self._manifest.close()
            with self._condition:
                self._condition.notify_all()

    def __enter__(self) -> "PolarsCertifiedTickHistory":
        self.start()
        return self

    def __exit__(self, _type, _value, _traceback) -> None:
        self.close()


__all__ = [
    "CertifiedTickHistoryToken",
    "PolarsCertifiedTickHistory",
    "PolarsCertifiedTickHistorySnapshot",
]
