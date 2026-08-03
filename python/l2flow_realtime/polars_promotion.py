"""Atomic Polars history replacement when online recovery is promoted."""

from __future__ import annotations

import threading
import time
from typing import Optional

from .models import UnavailableError
from .polars import (
    PolarsHistoryClosedError,
    PolarsHistoryCoverageError,
    PolarsHistoryDatasetIdentity,
    PolarsHistoryError,
    PolarsHistoryNotReadyError,
    PolarsHistoryRefreshError,
    PolarsHistoryState,
    _positive_float_or_none,
)


class AutoPromotingPolarsHistory:
    """Atomically switch a partial/absent dataset to recovered from-open data.

    ``promotion_factory`` is polled on a background control thread and should
    return ``None`` (or raise :class:`UnavailableError`) until the recovered
    control plane is exposed.  Once it returns a history handle, that handle is
    started and allowed to build a complete EOF-verified shadow dataset.  The
    public pointer changes only after the candidate reports from-open coverage,
    the same trade date, fixed Polars schema, and exact stable dataset identity
    (product, catalog-scoped instrument, and projection).

    The preview rows are *replaced*, never concatenated with recovered rows:
    online recovery uses a distinct run/session identity and overlap cannot be
    proven by product-local row numbers.  Snapshots obtained before the swap
    remain pinned to their old immutable buffers.
    """

    def __init__(
        self,
        active_history,
        promotion_factory,
        *,
        poll_interval: float = 0.05,
        expected_dataset_identity: Optional[
            PolarsHistoryDatasetIdentity
        ] = None,
    ) -> None:
        if active_history is None and promotion_factory is None:
            raise ValueError(
                "at least one active history or promotion factory is required"
            )
        if not callable(promotion_factory):
            raise TypeError("promotion_factory must be callable")
        if (
            expected_dataset_identity is not None
            and not isinstance(
                expected_dataset_identity,
                PolarsHistoryDatasetIdentity,
            )
        ):
            raise TypeError(
                "expected_dataset_identity must be "
                "PolarsHistoryDatasetIdentity or None"
            )
        if active_history is None and expected_dataset_identity is None:
            raise ValueError(
                "expected_dataset_identity is required when no preview "
                "history establishes the dataset"
            )
        self._current = active_history
        self._promotion_factory = promotion_factory
        self._expected_dataset_identity = expected_dataset_identity
        self._poll_interval = _positive_float_or_none(
            poll_interval, "poll_interval"
        )
        self._condition = threading.Condition(threading.RLock())
        self._thread: Optional[threading.Thread] = None
        self._stop = False
        self._promoted = False
        self._last_error: Optional[BaseException] = None
        self._retired_histories = []
        self._spill_users = {}

    @property
    def promoted(self) -> bool:
        with self._condition:
            return self._promoted

    @property
    def state(self) -> PolarsHistoryState:
        with self._condition:
            if self._stop:
                return PolarsHistoryState.CLOSED
            if self._last_error is not None:
                return PolarsHistoryState.FAILED
            current = self._current
            # Keep the wrapper lock through the sample.  The promotion thread
            # cannot replace and then close ``current`` between selecting the
            # handle and observing its state.
            return (
                PolarsHistoryState.STARTING
                if current is None
                else current.state
            )

    @property
    def last_error(self) -> Optional[BaseException]:
        with self._condition:
            return self._last_error

    @staticmethod
    def _dataset_identity(history, snapshot):
        identity = getattr(history, "dataset_identity", None)
        if not isinstance(identity, PolarsHistoryDatasetIdentity):
            raise PolarsHistoryCoverageError(
                "history does not expose a stable dataset identity"
            )
        snapshot_identity = getattr(
            snapshot, "dataset_identity", identity
        )
        if snapshot_identity != identity:
            raise PolarsHistoryCoverageError(
                "history snapshot changed dataset identity"
            )
        return identity

    def _validated_dataset_identity(self, history, snapshot):
        identity = self._dataset_identity(history, snapshot)
        expected = self._expected_dataset_identity
        if expected is not None and identity != expected:
            raise PolarsHistoryCoverageError(
                "history targets another requested dataset"
            )
        return identity

    @property
    def dataset_identity(self) -> PolarsHistoryDatasetIdentity:
        with self._condition:
            if self._stop:
                raise PolarsHistoryClosedError(
                    "auto-promoting history is closed"
                )
            current = self._current
            if current is None:
                raise PolarsHistoryNotReadyError(
                    "recovered history has not been promoted"
                )
            snapshot = current.latest_snapshot()
            return self._validated_dataset_identity(current, snapshot)

    def start(self) -> None:
        with self._condition:
            if self._stop:
                raise PolarsHistoryClosedError(
                    "auto-promoting history is closed"
                )
            if self._thread is not None:
                return
            current = self._current
            if current is not None:
                current.start()
            self._thread = threading.Thread(
                target=self._run,
                name="l2flow-polars-promotion",
                daemon=True,
            )
            self._thread.start()

    def _stopping(self) -> bool:
        with self._condition:
            return self._stop

    def _run(self) -> None:
        candidate = None
        candidate_owned = False
        try:
            while not self._stopping():
                try:
                    candidate = self._promotion_factory()
                except UnavailableError:
                    candidate = None
                if candidate is not None:
                    break
                with self._condition:
                    self._condition.wait(timeout=self._poll_interval)
            if candidate is None or self._stopping():
                return
            with self._condition:
                if candidate is self._current:
                    raise PolarsHistoryCoverageError(
                        "promotion factory returned the active history"
                    )
            candidate_owned = True
            candidate.start()
            while not self._stopping():
                try:
                    candidate.wait_ready(timeout=self._poll_interval)
                    break
                except TimeoutError:
                    continue
            if self._stopping():
                candidate.close()
                return
            replacement = candidate.latest_snapshot()
            candidate_identity = self._validated_dataset_identity(
                candidate, replacement
            )
            if not replacement.history_coverage.coverage_from_open:
                raise PolarsHistoryCoverageError(
                    "promotion candidate is not from-open complete"
                )
            with self._condition:
                previous = self._current
            if previous is not None:
                while True:
                    try:
                        previous_snapshot = previous.latest_snapshot()
                        break
                    except PolarsHistoryNotReadyError:
                        if self._stopping():
                            candidate.close()
                            return
                        try:
                            previous.wait_ready(
                                timeout=self._poll_interval
                            )
                        except TimeoutError:
                            continue
                previous_identity = self._validated_dataset_identity(
                    previous, previous_snapshot
                )
                if previous_identity != candidate_identity:
                    raise PolarsHistoryCoverageError(
                        "promotion candidate targets another dataset"
                    )
                cross_session = (
                    previous_snapshot.history_coverage.identity
                    != replacement.history_coverage.identity
                )
                if cross_session and (
                    previous_identity.catalog_digest is None
                    or candidate_identity.catalog_digest is None
                ):
                    raise PolarsHistoryCoverageError(
                        "cross-session promotion requires catalog identity"
                    )
                if (
                    previous_snapshot.history_coverage.trade_date
                    != replacement.history_coverage.trade_date
                ):
                    raise PolarsHistoryCoverageError(
                        "promotion candidate belongs to another trade date"
                    )
                if (
                    tuple(previous_snapshot.dataframe.schema.items())
                    != tuple(replacement.dataframe.schema.items())
                ):
                    raise PolarsHistoryCoverageError(
                        "promotion candidate has a different fixed schema"
                    )
            with self._condition:
                if self._stop:
                    candidate.close()
                    return
                # This single reference replacement is the dataset commit.
                # Candidate construction and all validation happened outside
                # the lock, so readers cannot observe a mixed generation.
                self._current = candidate
                candidate_owned = False
                self._promoted = True
                close_previous = (
                    previous is not None
                    and previous is not candidate
                    and self._spill_users.get(id(previous), 0) == 0
                )
                if (
                    previous is not None
                    and previous is not candidate
                    and not close_previous
                ):
                    self._retired_histories.append(previous)
                self._condition.notify_all()
            if close_previous:
                try:
                    previous.close()
                except BaseException:
                    # The candidate is already the committed public pointer.
                    # A preview cleanup failure must not close or roll back the
                    # valid replacement; retain it for a best-effort retry at
                    # wrapper shutdown.
                    with self._condition:
                        self._retired_histories.append(previous)
        except BaseException as error:
            if candidate is not None and candidate_owned:
                try:
                    candidate.close()
                except BaseException:
                    pass
            with self._condition:
                if not self._stop:
                    self._last_error = error
                    self._condition.notify_all()

    def wait_ready(self, timeout: Optional[float] = None) -> None:
        deadline = (
            None
            if timeout is None
            else time.monotonic()
            + _positive_float_or_none(timeout, "timeout")
        )
        while True:
            with self._condition:
                if self._stop:
                    raise PolarsHistoryClosedError(
                        "auto-promoting history is closed"
                    )
                if self._last_error is not None:
                    raise PolarsHistoryRefreshError(
                        "automatic promotion failed"
                    ) from self._last_error
                current = self._current
                remaining = (
                    None
                    if deadline is None
                    else max(0.0, deadline - time.monotonic())
                )
                if remaining == 0.0:
                    raise TimeoutError(
                        "timed out waiting for Polars history"
                    )
                if current is None:
                    self._condition.wait(timeout=remaining)
                    continue
            try:
                current.wait_ready(timeout=remaining)
                with self._condition:
                    if current is not self._current:
                        continue
                    snapshot = current.latest_snapshot()
                    self._validated_dataset_identity(current, snapshot)
                    return
            except PolarsHistoryClosedError:
                # Promotion closes the preview only after the candidate has
                # been committed.  A waiter that selected that preview just
                # before the swap should retry against the replacement.
                with self._condition:
                    if current is self._current:
                        raise

    def wait_promoted(self, timeout: Optional[float] = None) -> None:
        deadline = (
            None
            if timeout is None
            else time.monotonic()
            + _positive_float_or_none(timeout, "timeout")
        )
        with self._condition:
            while not self._promoted:
                if self._stop:
                    raise PolarsHistoryClosedError(
                        "auto-promoting history is closed"
                    )
                if self._last_error is not None:
                    raise PolarsHistoryRefreshError(
                        "automatic promotion failed"
                    ) from self._last_error
                remaining = (
                    None
                    if deadline is None
                    else max(0.0, deadline - time.monotonic())
                )
                if remaining == 0.0:
                    raise TimeoutError(
                        "timed out waiting for online promotion"
                    )
                self._condition.wait(timeout=remaining)

    def latest_snapshot(self, **kwargs):
        with self._condition:
            if self._stop:
                raise PolarsHistoryClosedError(
                    "auto-promoting history is closed"
                )
            if self._last_error is not None:
                raise PolarsHistoryRefreshError(
                    "automatic promotion failed"
                ) from self._last_error
            current = self._current
            if current is None:
                raise PolarsHistoryNotReadyError(
                    "recovered history has not been promoted"
                )
            # Promotion cannot close the selected handle until this cheap
            # immutable snapshot capture has completed.
            snapshot = current.latest_snapshot(**kwargs)
            self._validated_dataset_identity(current, snapshot)
            return snapshot

    def latest_dataframe(self, **kwargs):
        return self.latest_snapshot(**kwargs).dataframe

    def latest_lazyframe(self, **kwargs):
        return self.latest_snapshot(**kwargs).lazyframe

    def spill(self, path, **kwargs) -> str:
        """Spill through one leased active history handle.

        Selection and lease acquisition are atomic with promotion.  Disk I/O
        runs outside the wrapper lock; a swapped-out preview is retired only
        after every spill lease releases, so candidate startup and reads are
        not blocked by Parquet latency.  The selected handle validates its
        dataset identity before the spill call; the handle's own atomic
        snapshot capture chooses the exact cut written to Parquet.
        """

        with self._condition:
            if self._stop:
                raise PolarsHistoryClosedError(
                    "auto-promoting history is closed"
                )
            if self._last_error is not None:
                raise PolarsHistoryRefreshError(
                    "automatic promotion failed"
                ) from self._last_error
            current = self._current
            if current is None:
                raise PolarsHistoryNotReadyError(
                    "recovered history has not been promoted"
                )
            spill = getattr(current, "spill", None)
            if not callable(spill):
                raise PolarsHistoryError(
                    "current history handle does not support spill"
                )
            snapshot = current.latest_snapshot()
            self._validated_dataset_identity(current, snapshot)
            key = id(current)
            self._spill_users[key] = self._spill_users.get(key, 0) + 1
        try:
            return spill(path, **kwargs)
        finally:
            cleanup_retired = False
            with self._condition:
                users = self._spill_users.get(key, 0)
                if users > 1:
                    self._spill_users[key] = users - 1
                elif users == 1 and any(
                    history is current
                    for history in self._retired_histories
                ):
                    # Keep the final lease visible while close runs outside
                    # the wrapper lock.  Wrapper shutdown consequently waits
                    # and cannot concurrently close the same retired handle.
                    cleanup_retired = True
                else:
                    self._spill_users.pop(key, None)
                self._condition.notify_all()
            cleanup_succeeded = False
            if cleanup_retired:
                try:
                    current.close()
                    cleanup_succeeded = True
                except BaseException:
                    pass
                with self._condition:
                    self._spill_users.pop(key, None)
                    if cleanup_succeeded:
                        self._retired_histories = [
                            history
                            for history in self._retired_histories
                            if history is not current
                        ]
                    self._condition.notify_all()

    def refresh(self, timeout: Optional[float] = None) -> None:
        deadline = (
            None
            if timeout is None
            else time.monotonic()
            + _positive_float_or_none(timeout, "timeout")
        )
        while True:
            with self._condition:
                if self._stop:
                    raise PolarsHistoryClosedError(
                        "auto-promoting history is closed"
                    )
                if self._last_error is not None:
                    raise PolarsHistoryRefreshError(
                        "automatic promotion failed"
                    ) from self._last_error
                current = self._current
                if current is None:
                    raise PolarsHistoryNotReadyError(
                        "recovered history has not been promoted"
                    )
                remaining = (
                    None
                    if deadline is None
                    else max(0.0, deadline - time.monotonic())
                )
                if remaining == 0.0:
                    raise TimeoutError(
                        "timed out refreshing Polars history"
                    )
            try:
                # Do not hold the wrapper lock across a potentially blocking
                # history refresh: close and promotion must remain responsive.
                current.refresh(remaining)
                with self._condition:
                    if current is not self._current:
                        continue
                    snapshot = current.latest_snapshot()
                    self._validated_dataset_identity(current, snapshot)
                    return
            except PolarsHistoryClosedError:
                with self._condition:
                    if current is self._current:
                        raise

    def close(self) -> None:
        with self._condition:
            if self._stop:
                return
            self._stop = True
            thread = self._thread
            current = self._current
            self._condition.notify_all()
        if thread is not None and thread is not threading.current_thread():
            thread.join()
        with self._condition:
            while self._spill_users:
                self._condition.wait()
            final_current = self._current
            retired = tuple(self._retired_histories)
            self._retired_histories.clear()
        try:
            if final_current is not None:
                final_current.close()
            if current is not None and current is not final_current:
                current.close()
        finally:
            for history in retired:
                try:
                    history.close()
                except BaseException:
                    pass

    def __enter__(self) -> "AutoPromotingPolarsHistory":
        self.start()
        return self

    def __exit__(self, _type, _value, _traceback) -> None:
        self.close()


__all__ = ["AutoPromotingPolarsHistory"]
