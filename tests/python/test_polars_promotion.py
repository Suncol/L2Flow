from __future__ import annotations

import dataclasses
import os
import sys
import threading
import unittest
from types import SimpleNamespace


REPOSITORY = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)
PYTHON_ROOT = os.path.join(REPOSITORY, "python")
if PYTHON_ROOT not in sys.path:
    sys.path.insert(0, PYTHON_ROOT)


from l2flow_realtime.models import (  # noqa: E402
    HistoryCoverageInfo,
    TemporalCoverageKind,
    UnavailableError,
)
from l2flow_realtime.polars import (  # noqa: E402
    PolarsHistoryClosedError,
    PolarsHistoryCoverageError,
    PolarsHistoryDatasetIdentity,
    PolarsHistoryNotReadyError,
    PolarsHistoryProductKind,
    PolarsHistoryRefreshError,
    PolarsHistoryState,
    as_polars,
    polars_available,
)


DATASET_IDENTITY = PolarsHistoryDatasetIdentity(
    PolarsHistoryProductKind.FAST_RAW_INSTRUMENT_TICKS,
    1,
    ("tick_stream_sequence",),
    1,
    catalog_digest=b"C" * 32,
)
from l2flow_realtime.polars_promotion import (  # noqa: E402
    AutoPromotingPolarsHistory,
)


class FakeHistory:
    def __init__(
        self,
        frame,
        coverage,
        dataset_identity=DATASET_IDENTITY,
    ) -> None:
        self.frame = frame
        self.coverage = coverage
        self.dataset_identity = dataset_identity
        self.state = PolarsHistoryState.STARTING
        self.closed = False
        self.started = threading.Event()
        self.spill_calls = []

    def start(self):
        self.state = PolarsHistoryState.READY
        self.started.set()

    def wait_ready(self, timeout=None):
        if not self.started.wait(timeout):
            raise TimeoutError

    def latest_snapshot(self, **_kwargs):
        if self.closed:
            raise RuntimeError("closed")
        return SimpleNamespace(
            dataframe=self.frame.clone(),
            lazyframe=self.frame.clone().lazy(),
            history_coverage=self.coverage,
            dataset_identity=self.dataset_identity,
        )

    def refresh(self, _timeout=None):
        pass

    def spill(self, path, **kwargs):
        if self.closed:
            raise PolarsHistoryClosedError("closed")
        self.spill_calls.append((path, kwargs))
        return str(path)

    def close(self):
        self.closed = True
        self.state = PolarsHistoryState.CLOSED


class BlockingPreview(FakeHistory):
    def __init__(self, frame, coverage) -> None:
        super().__init__(frame, coverage)
        self.wait_entered = threading.Event()
        self.closed_event = threading.Event()

    def start(self):
        self.state = PolarsHistoryState.STARTING

    def wait_ready(self, timeout=None):
        self.wait_entered.set()
        if not self.closed_event.wait(timeout):
            raise TimeoutError
        raise PolarsHistoryClosedError("preview replaced")

    def close(self):
        super().close()
        self.closed_event.set()


class BlockingRefreshPreview(FakeHistory):
    def __init__(self, frame, coverage) -> None:
        super().__init__(frame, coverage)
        self.refresh_entered = threading.Event()
        self.closed_event = threading.Event()

    def refresh(self, _timeout=None):
        self.refresh_entered.set()
        self.closed_event.wait()
        raise PolarsHistoryClosedError("preview replaced")

    def close(self):
        super().close()
        self.closed_event.set()


class BlockingSpillPreview(FakeHistory):
    def __init__(self, frame, coverage) -> None:
        super().__init__(frame, coverage)
        self.spill_entered = threading.Event()
        self.spill_release = threading.Event()

    def spill(self, path, **kwargs):
        self.spill_entered.set()
        if not self.spill_release.wait(1.0):
            raise TimeoutError("spill was not released")
        return super().spill(path, **kwargs)


class MultiBlockingSpillPreview(FakeHistory):
    def __init__(self, frame, coverage, expected_spills) -> None:
        super().__init__(frame, coverage)
        self.expected_spills = expected_spills
        self.spill_release = threading.Event()
        self.all_spills_entered = threading.Event()
        self._spill_lock = threading.Lock()
        self._spill_count = 0

    def spill(self, path, **kwargs):
        with self._spill_lock:
            self._spill_count += 1
            if self._spill_count == self.expected_spills:
                self.all_spills_entered.set()
        if not self.spill_release.wait(1.0):
            raise TimeoutError("spills were not released")
        return super().spill(path, **kwargs)


class FailingClosePreview(FakeHistory):
    def __init__(self, frame, coverage) -> None:
        super().__init__(frame, coverage)
        self.close_calls = 0

    def close(self):
        self.close_calls += 1
        if self.close_calls == 1:
            raise RuntimeError("preview cleanup failed")
        super().close()


class DelayedReadyHistory(FakeHistory):
    def __init__(self, frame, coverage) -> None:
        super().__init__(frame, coverage)
        self.ready_release = threading.Event()

    def start(self):
        self.state = PolarsHistoryState.STARTING

    def wait_ready(self, timeout=None):
        if not self.ready_release.wait(timeout):
            raise TimeoutError

    def latest_snapshot(self, **kwargs):
        if not self.ready_release.is_set():
            raise PolarsHistoryNotReadyError("preview is still building")
        return super().latest_snapshot(**kwargs)

    def make_ready(self):
        self.state = PolarsHistoryState.READY
        self.ready_release.set()


@unittest.skipUnless(polars_available(), "Polars is not installed")
class PolarsPromotionTests(unittest.TestCase):
    def _assert_identity_rejected(
        self,
        candidate_identity,
        *,
        active_identity=DATASET_IDENTITY,
    ) -> None:
        import polars as pl

        partial_coverage = HistoryCoverageInfo(
            b"P" * 16,
            1,
            20260803,
            TemporalCoverageKind.PROCESS_START_PARTIAL,
            1_775_000_000_000_000_000,
        )
        recovered_coverage = HistoryCoverageInfo(
            b"R" * 16,
            2,
            20260803,
            TemporalCoverageKind.FROM_OPEN,
        )
        active = FakeHistory(
            pl.DataFrame(
                {"tick_stream_sequence": [100]},
                schema={"tick_stream_sequence": pl.UInt64},
            ),
            partial_coverage,
            active_identity,
        )
        candidate = FakeHistory(
            pl.DataFrame(
                {"tick_stream_sequence": [1, 2]},
                schema={"tick_stream_sequence": pl.UInt64},
            ),
            recovered_coverage,
            candidate_identity,
        )
        pinned = active.latest_snapshot()
        history = AutoPromotingPolarsHistory(
            active, lambda: candidate, poll_interval=0.001
        )
        history.start()
        with self.assertRaises(PolarsHistoryRefreshError):
            history.wait_promoted(1.0)
        self.assertFalse(history.promoted)
        self.assertFalse(active.closed)
        self.assertTrue(candidate.closed)
        self.assertEqual(
            pinned.dataframe["tick_stream_sequence"].to_list(), [100]
        )
        history.close()

    def test_partial_is_replaced_not_concatenated(self):
        import polars as pl

        partial_coverage = HistoryCoverageInfo(
            b"P" * 16,
            1,
            20260803,
            TemporalCoverageKind.PROCESS_START_PARTIAL,
            1_775_000_000_000_000_000,
        )
        recovered_coverage = HistoryCoverageInfo(
            b"R" * 16,
            2,
            20260803,
            TemporalCoverageKind.FROM_OPEN,
        )
        partial = FakeHistory(
            pl.DataFrame(
                {"tick_stream_sequence": [100]},
                schema={"tick_stream_sequence": pl.UInt64},
            ),
            partial_coverage,
        )
        recovered = FakeHistory(
            pl.DataFrame(
                {"tick_stream_sequence": [1, 2, 3]},
                schema={"tick_stream_sequence": pl.UInt64},
            ),
            recovered_coverage,
        )
        calls = 0

        def factory():
            nonlocal calls
            calls += 1
            if calls == 1:
                raise UnavailableError("not exposed")
            return recovered

        history = AutoPromotingPolarsHistory(
            partial, factory, poll_interval=0.001
        )
        history.start()
        pinned = history.latest_snapshot()
        history.wait_promoted(1.0)
        self.assertEqual(
            history.latest_dataframe()["tick_stream_sequence"].to_list(),
            [1, 2, 3],
        )
        self.assertEqual(
            pinned.dataframe["tick_stream_sequence"].to_list(), [100]
        )
        self.assertTrue(partial.closed)
        history.close()
        self.assertTrue(recovered.closed)

    def test_no_preview_history_waits_for_recovered_candidate(self):
        import polars as pl

        coverage = HistoryCoverageInfo(
            b"R" * 16,
            2,
            20260803,
            TemporalCoverageKind.FROM_OPEN,
        )
        recovered = FakeHistory(
            pl.DataFrame(
                {"tick_stream_sequence": [1]},
                schema={"tick_stream_sequence": pl.UInt64},
            ),
            coverage,
        )
        history = AutoPromotingPolarsHistory(
            None,
            lambda: recovered,
            poll_interval=0.001,
            expected_dataset_identity=DATASET_IDENTITY,
        )
        history.start()
        history.wait_ready(1.0)
        history.wait_promoted(1.0)
        self.assertEqual(history.latest_dataframe().height, 1)
        history.close()

    def test_no_preview_requires_explicit_dataset_identity(self):
        with self.assertRaises(ValueError):
            AutoPromotingPolarsHistory(
                None, lambda: None, poll_interval=0.001
            )

    def test_candidate_waits_for_initializing_preview_compatibility(self):
        import polars as pl

        preview = DelayedReadyHistory(
            pl.DataFrame(
                {"tick_stream_sequence": [100]},
                schema={"tick_stream_sequence": pl.UInt64},
            ),
            HistoryCoverageInfo(
                b"P" * 16,
                1,
                20260803,
                TemporalCoverageKind.PROCESS_START_PARTIAL,
                1_775_000_000_000_000_000,
            ),
        )
        recovered = FakeHistory(
            pl.DataFrame(
                {"tick_stream_sequence": [1]},
                schema={"tick_stream_sequence": pl.UInt64},
            ),
            HistoryCoverageInfo(
                b"R" * 16,
                2,
                20260803,
                TemporalCoverageKind.FROM_OPEN,
            ),
        )
        history = AutoPromotingPolarsHistory(
            preview, lambda: recovered, poll_interval=0.001
        )
        history.start()
        self.assertTrue(recovered.started.wait(1.0))
        self.assertFalse(history.promoted)
        self.assertIsNone(history.last_error)
        preview.make_ready()
        history.wait_promoted(1.0)
        self.assertEqual(history.latest_dataframe().height, 1)
        history.close()

    def test_factory_returning_active_handle_does_not_close_preview(self):
        import polars as pl

        preview = FakeHistory(
            pl.DataFrame(
                {"tick_stream_sequence": [100]},
                schema={"tick_stream_sequence": pl.UInt64},
            ),
            HistoryCoverageInfo(
                b"P" * 16,
                1,
                20260803,
                TemporalCoverageKind.PROCESS_START_PARTIAL,
                1_775_000_000_000_000_000,
            ),
        )
        history = AutoPromotingPolarsHistory(
            preview, lambda: preview, poll_interval=0.001
        )
        history.start()
        with self.assertRaises(PolarsHistoryRefreshError):
            history.wait_promoted(1.0)
        self.assertFalse(preview.closed)
        history.close()
        self.assertTrue(preview.closed)

    def test_expected_identity_is_enforced_on_preview_reads(self):
        import polars as pl

        wrong_identity = dataclasses.replace(
            DATASET_IDENTITY, instrument_id=2
        )
        preview = FakeHistory(
            pl.DataFrame(
                {"tick_stream_sequence": [100]},
                schema={"tick_stream_sequence": pl.UInt64},
            ),
            HistoryCoverageInfo(
                b"P" * 16,
                1,
                20260803,
                TemporalCoverageKind.PROCESS_START_PARTIAL,
                1_775_000_000_000_000_000,
            ),
            wrong_identity,
        )
        history = AutoPromotingPolarsHistory(
            preview,
            lambda: None,
            poll_interval=0.001,
            expected_dataset_identity=DATASET_IDENTITY,
        )
        history.start()
        with self.assertRaises(PolarsHistoryCoverageError):
            history.latest_snapshot()
        with self.assertRaises(PolarsHistoryCoverageError):
            history.wait_ready(1.0)
        history.close()

    def test_promotion_fixed_schema_includes_column_order(self):
        import polars as pl

        identity = dataclasses.replace(
            DATASET_IDENTITY, projection_columns=("left", "right")
        )
        preview = FakeHistory(
            pl.DataFrame(
                {"left": [100], "right": [101]},
                schema={"left": pl.UInt64, "right": pl.Int64},
            ),
            HistoryCoverageInfo(
                b"P" * 16,
                1,
                20260803,
                TemporalCoverageKind.PROCESS_START_PARTIAL,
                1_775_000_000_000_000_000,
            ),
            identity,
        )
        recovered = FakeHistory(
            pl.DataFrame(
                {"right": [2], "left": [1]},
                schema={"right": pl.Int64, "left": pl.UInt64},
            ),
            HistoryCoverageInfo(
                b"R" * 16,
                2,
                20260803,
                TemporalCoverageKind.FROM_OPEN,
            ),
            identity,
        )
        history = AutoPromotingPolarsHistory(
            preview, lambda: recovered, poll_interval=0.001
        )
        history.start()
        with self.assertRaises(PolarsHistoryRefreshError):
            history.wait_promoted(1.0)
        self.assertFalse(preview.closed)
        self.assertTrue(recovered.closed)
        history.close()

    def test_wait_ready_retries_candidate_when_preview_closes_on_swap(self):
        import polars as pl

        partial_coverage = HistoryCoverageInfo(
            b"P" * 16,
            1,
            20260803,
            TemporalCoverageKind.PROCESS_START_PARTIAL,
            1_775_000_000_000_000_000,
        )
        recovered_coverage = HistoryCoverageInfo(
            b"R" * 16,
            2,
            20260803,
            TemporalCoverageKind.FROM_OPEN,
        )
        preview = BlockingPreview(
            pl.DataFrame(
                {"tick_stream_sequence": [100]},
                schema={"tick_stream_sequence": pl.UInt64},
            ),
            partial_coverage,
        )
        recovered = FakeHistory(
            pl.DataFrame(
                {"tick_stream_sequence": [1, 2]},
                schema={"tick_stream_sequence": pl.UInt64},
            ),
            recovered_coverage,
        )

        def factory():
            if not preview.wait_entered.wait(1.0):
                raise TimeoutError("waiter did not select preview")
            return recovered

        history = AutoPromotingPolarsHistory(
            preview, factory, poll_interval=0.001
        )
        history.start()
        # This initially blocks on the preview.  Promotion swaps in the ready
        # candidate, closes the preview, and the wrapper retries internally.
        history.wait_ready(1.0)
        self.assertTrue(history.promoted)
        self.assertEqual(history.latest_dataframe().height, 2)
        history.close()

    def test_refresh_does_not_block_promotion_and_retries_replacement(self):
        import polars as pl

        partial_coverage = HistoryCoverageInfo(
            b"P" * 16,
            1,
            20260803,
            TemporalCoverageKind.PROCESS_START_PARTIAL,
            1_775_000_000_000_000_000,
        )
        recovered_coverage = HistoryCoverageInfo(
            b"R" * 16,
            2,
            20260803,
            TemporalCoverageKind.FROM_OPEN,
        )
        preview = BlockingRefreshPreview(
            pl.DataFrame(
                {"tick_stream_sequence": [100]},
                schema={"tick_stream_sequence": pl.UInt64},
            ),
            partial_coverage,
        )
        recovered = FakeHistory(
            pl.DataFrame(
                {"tick_stream_sequence": [1, 2]},
                schema={"tick_stream_sequence": pl.UInt64},
            ),
            recovered_coverage,
        )

        def factory():
            if not preview.refresh_entered.wait(1.0):
                raise TimeoutError("refresh did not select preview")
            return recovered

        history = AutoPromotingPolarsHistory(
            preview, factory, poll_interval=0.001
        )
        history.start()
        history.refresh(1.0)
        self.assertTrue(history.promoted)
        self.assertTrue(preview.closed)
        history.close()

    def test_spill_protects_selected_preview_until_capture_returns(self):
        import polars as pl

        partial_coverage = HistoryCoverageInfo(
            b"P" * 16,
            1,
            20260803,
            TemporalCoverageKind.PROCESS_START_PARTIAL,
            1_775_000_000_000_000_000,
        )
        recovered_coverage = HistoryCoverageInfo(
            b"R" * 16,
            2,
            20260803,
            TemporalCoverageKind.FROM_OPEN,
        )
        preview = BlockingSpillPreview(
            pl.DataFrame(
                {"tick_stream_sequence": [100]},
                schema={"tick_stream_sequence": pl.UInt64},
            ),
            partial_coverage,
        )
        recovered = FakeHistory(
            pl.DataFrame(
                {"tick_stream_sequence": [1]},
                schema={"tick_stream_sequence": pl.UInt64},
            ),
            recovered_coverage,
        )

        def factory():
            if not preview.spill_entered.wait(1.0):
                raise TimeoutError("spill did not select preview")
            return recovered

        history = AutoPromotingPolarsHistory(
            preview, factory, poll_interval=0.001
        )
        history.start()
        result = []

        def spill():
            result.append(history.spill("preview.parquet", marker=7))

        thread = threading.Thread(target=spill)
        thread.start()
        self.assertTrue(preview.spill_entered.wait(1.0))
        self.assertTrue(recovered.started.wait(1.0))
        # Disk I/O does not retain the wrapper lock: promotion may commit the
        # replacement, but the leased preview cannot be closed until its spill
        # call returns.
        history.wait_promoted(1.0)
        self.assertFalse(preview.closed)
        preview.spill_release.set()
        thread.join(timeout=1.0)
        self.assertFalse(thread.is_alive())
        self.assertEqual(result, ["preview.parquet"])
        self.assertTrue(preview.closed)
        self.assertEqual(
            preview.spill_calls,
            [("preview.parquet", {"marker": 7})],
        )
        history.close()

    def test_preview_close_failure_does_not_rollback_valid_candidate(self):
        import polars as pl

        preview = FailingClosePreview(
            pl.DataFrame(
                {"tick_stream_sequence": [100]},
                schema={"tick_stream_sequence": pl.UInt64},
            ),
            HistoryCoverageInfo(
                b"P" * 16,
                1,
                20260803,
                TemporalCoverageKind.PROCESS_START_PARTIAL,
                1_775_000_000_000_000_000,
            ),
        )
        recovered = FakeHistory(
            pl.DataFrame(
                {"tick_stream_sequence": [1]},
                schema={"tick_stream_sequence": pl.UInt64},
            ),
            HistoryCoverageInfo(
                b"R" * 16,
                2,
                20260803,
                TemporalCoverageKind.FROM_OPEN,
            ),
        )
        history = AutoPromotingPolarsHistory(
            preview, lambda: recovered, poll_interval=0.001
        )
        history.start()
        history.wait_promoted(1.0)
        self.assertIsNone(history.last_error)
        self.assertFalse(recovered.closed)
        self.assertEqual(history.latest_dataframe().height, 1)
        self.assertEqual(preview.close_calls, 1)
        history.close()
        self.assertTrue(recovered.closed)
        self.assertTrue(preview.closed)
        self.assertEqual(preview.close_calls, 2)

    def test_last_concurrent_spill_reclaims_retired_preview(self):
        import polars as pl

        partial_coverage = HistoryCoverageInfo(
            b"P" * 16,
            1,
            20260803,
            TemporalCoverageKind.PROCESS_START_PARTIAL,
            1_775_000_000_000_000_000,
        )
        recovered_coverage = HistoryCoverageInfo(
            b"R" * 16,
            2,
            20260803,
            TemporalCoverageKind.FROM_OPEN,
        )
        preview = MultiBlockingSpillPreview(
            pl.DataFrame(
                {"tick_stream_sequence": [100]},
                schema={"tick_stream_sequence": pl.UInt64},
            ),
            partial_coverage,
            expected_spills=2,
        )
        recovered = FakeHistory(
            pl.DataFrame(
                {"tick_stream_sequence": [1]},
                schema={"tick_stream_sequence": pl.UInt64},
            ),
            recovered_coverage,
        )

        def factory():
            if not preview.all_spills_entered.wait(1.0):
                raise TimeoutError("both spills did not select preview")
            return recovered

        history = AutoPromotingPolarsHistory(
            preview, factory, poll_interval=0.001
        )
        history.start()
        results = []
        errors = []

        def spill(index):
            try:
                results.append(history.spill(f"preview-{index}.parquet"))
            except BaseException as error:
                errors.append(error)

        threads = [
            threading.Thread(target=spill, args=(index,))
            for index in range(2)
        ]
        for thread in threads:
            thread.start()
        self.assertTrue(preview.all_spills_entered.wait(1.0))
        history.wait_promoted(1.0)
        self.assertFalse(preview.closed)
        preview.spill_release.set()
        for thread in threads:
            thread.join(timeout=1.0)
            self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertCountEqual(
            results,
            ["preview-0.parquet", "preview-1.parquet"],
        )
        self.assertTrue(preview.closed)
        self.assertEqual(history._retired_histories, [])
        history.close()

    def test_wrong_instrument_candidate_is_rejected(self):
        self._assert_identity_rejected(
            PolarsHistoryDatasetIdentity(
                PolarsHistoryProductKind.FAST_RAW_INSTRUMENT_TICKS,
                2,
                ("tick_stream_sequence",),
                1,
                catalog_digest=b"C" * 32,
            )
        )

    def test_same_numeric_instrument_in_wrong_catalog_is_rejected(self):
        self._assert_identity_rejected(
            PolarsHistoryDatasetIdentity(
                PolarsHistoryProductKind.FAST_RAW_INSTRUMENT_TICKS,
                1,
                ("tick_stream_sequence",),
                1,
                catalog_digest=b"D" * 32,
            )
        )

    def test_wrong_product_candidate_is_rejected(self):
        self._assert_identity_rejected(
            PolarsHistoryDatasetIdentity(
                PolarsHistoryProductKind.CERTIFIED_TICKS,
                None,
                ("tick_stream_sequence",),
                1,
            )
        )

    def test_wrong_projection_candidate_is_rejected(self):
        self._assert_identity_rejected(
            PolarsHistoryDatasetIdentity(
                PolarsHistoryProductKind.FAST_RAW_INSTRUMENT_TICKS,
                1,
                ("price_p6",),
                1,
                catalog_digest=b"C" * 32,
            )
        )

    def test_wrong_payload_projection_version_is_rejected(self):
        self._assert_identity_rejected(
            PolarsHistoryDatasetIdentity(
                PolarsHistoryProductKind.FAST_RAW_INSTRUMENT_TICKS,
                1,
                ("tick_stream_sequence",),
                2,
                catalog_digest=b"C" * 32,
            )
        )

    def test_global_certified_cross_session_requires_catalog_identity(self):
        unscoped = PolarsHistoryDatasetIdentity(
            PolarsHistoryProductKind.CERTIFIED_ORDER_EVENTS,
            None,
            ("canonical_apply_sequence",),
            1,
        )
        self._assert_identity_rejected(
            unscoped, active_identity=unscoped
        )

    def test_global_certified_catalog_source_version_digest_are_exact(self):
        active = PolarsHistoryDatasetIdentity(
            PolarsHistoryProductKind.CERTIFIED_ORDER_EVENTS,
            None,
            ("canonical_apply_sequence",),
            1,
            catalog_digest=b"C" * 32,
            catalog_scope=2,
            catalog_version=7,
        )
        candidates = (
            PolarsHistoryDatasetIdentity(
                PolarsHistoryProductKind.CERTIFIED_ORDER_EVENTS,
                None,
                ("canonical_apply_sequence",),
                1,
                catalog_digest=b"D" * 32,
                catalog_scope=2,
                catalog_version=7,
            ),
            PolarsHistoryDatasetIdentity(
                PolarsHistoryProductKind.CERTIFIED_ORDER_EVENTS,
                None,
                ("canonical_apply_sequence",),
                1,
                catalog_digest=b"C" * 32,
                catalog_scope=3,
                catalog_version=7,
            ),
            PolarsHistoryDatasetIdentity(
                PolarsHistoryProductKind.CERTIFIED_ORDER_EVENTS,
                None,
                ("canonical_apply_sequence",),
                1,
                catalog_digest=b"C" * 32,
                catalog_scope=2,
                catalog_version=8,
            ),
        )
        for candidate in candidates:
            with self.subTest(candidate=candidate):
                self._assert_identity_rejected(
                    candidate, active_identity=active
                )

    def test_public_facade_starts_promotion_monitor_before_return(self):
        import polars as pl

        partial = FakeHistory(
            pl.DataFrame(
                {"tick_stream_sequence": [100]},
                schema={"tick_stream_sequence": pl.UInt64},
            ),
            HistoryCoverageInfo(
                b"P" * 16,
                1,
                20260803,
                TemporalCoverageKind.PROCESS_START_PARTIAL,
                1_775_000_000_000_000_000,
            ),
        )
        recovered = FakeHistory(
            pl.DataFrame(
                {"tick_stream_sequence": [1]},
                schema={"tick_stream_sequence": pl.UInt64},
            ),
            HistoryCoverageInfo(
                b"R" * 16,
                2,
                20260803,
                TemporalCoverageKind.FROM_OPEN,
            ),
        )
        history = as_polars(SimpleNamespace()).auto_promoting_history(
            partial, lambda: recovered, poll_interval=0.001
        )
        history.wait_promoted(1.0)
        self.assertTrue(history.promoted)
        history.close()


if __name__ == "__main__":
    unittest.main()
