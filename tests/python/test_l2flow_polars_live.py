import os
import subprocess
import sys
import tempfile
import threading
import unittest
from dataclasses import dataclass
from types import SimpleNamespace


REPOSITORY = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)
PYTHON_ROOT = os.path.join(REPOSITORY, "python")
if PYTHON_ROOT not in sys.path:
    sys.path.insert(0, PYTHON_ROOT)


from l2flow_realtime.models import (  # noqa: E402
    KLineCoverageFlag,
    KLineCoverageInfo,
    KLineTemporalCoverage,
    HistoryCoverageInfo,
    EventUid,
    EventUidScope,
    LatestKLine,
    LatestSnapshot,
    LatestStatus,
    SessionIdentity,
    TemporalCoverageKind,
)
from l2flow_realtime.certified_order_events import (  # noqa: E402
    CertifiedOrderEventState,
)
from l2flow_realtime.polars import (  # noqa: E402
    LivePolarsHistory,
    PolarsCertifiedOrderEventHistory,
    PolarsHistoryCoverageOrigin,
    PolarsHistoryProductKind,
    PolarsHistoryNotReadyError,
    PolarsHistoryRefreshError,
    PolarsSchemaError,
    PolarsHistoryState,
    PolarsInstrumentTickHistory,
    PolarsInstrumentDerivedEventHistory,
    as_polars,
    empty_raw_event_frame,
    certified_order_event_batch_frame,
    derived_event_schema,
    derived_events_frame,
    history_coverage_frame,
    latest_klines_frame,
    latest_snapshots_frame,
    polars_available,
    raw_event_batch_frame,
    raw_event_schema,
)


@dataclass(frozen=True)
class _FakeCheckpoint:
    generation: int
    instrument_tick_record_count: int
    coverage_from_open: bool = True
    run_id: bytes = b"P" * 16
    session_epoch: int = 7
    trade_date: int = 20260803
    instrument_id: int = 1
    coverage_complete: bool = True
    record_coverage_complete: bool = True
    tick_record_coverage_complete: bool = True
    history_published_monotonic_ns: int = 100
    payload_projection: int = 1
    catalog_digest: bytes = b"C" * 32

    def ensure_successor_of(self, base) -> None:
        if (
            self.run_id != base.run_id
            or self.session_epoch != base.session_epoch
            or self.trade_date != base.trade_date
            or self.instrument_id != base.instrument_id
            or self.coverage_from_open != base.coverage_from_open
            or self.generation < base.generation
            or self.instrument_tick_record_count
            < base.instrument_tick_record_count
        ):
            raise ValueError("checkpoint is not a successor")


class _FakeColumns:
    column_names = ("ingress_sequence", "price_p6")


class _FakeBatch:
    columns = _FakeColumns()

    def __init__(self, rows) -> None:
        self._rows = tuple(rows)
        self.record_count = len(self._rows)
        self.closed = False

    def read_columns(self, *names):
        if self.closed:
            raise RuntimeError("batch is closed")
        return {
            name: tuple(row[name] for row in self._rows)
            for name in names
        }

    def close(self) -> None:
        self.closed = True
        self._rows = ()


class _FakeArrayBatch(_FakeBatch):
    def __init__(self, rows) -> None:
        super().__init__(rows)
        self.bulk_copy_used = False

    def copy_column_arrays(self, *names):
        import array

        self.bulk_copy_used = True
        formats = {"ingress_sequence": "Q", "price_p6": "q"}
        return {
            name: array.array(
                formats[name], (row[name] for row in self._rows)
            )
            for name in names
        }


class _FakeCursor:
    def __init__(
        self,
        rows,
        checkpoint,
        *,
        before_eof=None,
        entered=None,
    ) -> None:
        self._rows = tuple(rows)
        self._checkpoint = checkpoint
        self._before_eof = before_eof
        self._entered = entered
        self._eof = False

    def __enter__(self):
        return self

    def __exit__(self, _type, _value, _traceback) -> None:
        pass

    def batches(self):
        if self._rows:
            batch = _FakeBatch(self._rows)
            try:
                yield batch
            finally:
                batch.close()
        if self._entered is not None:
            self._entered.set()
        if self._before_eof is not None:
            self._before_eof.wait(timeout=5.0)
        self._eof = True

    @property
    def verified_checkpoint(self):
        if not self._eof:
            raise RuntimeError("checkpoint requested before EOF")
        return self._checkpoint


class _FakeReader:
    def __init__(self, initial, updates=()) -> None:
        self._initial = initial
        self._updates = list(updates)
        self.closed = False
        self.update_calls = 0

    def read_all(self, _instrument, **_kwargs):
        return self._initial

    def read_updates(self, _instrument, _checkpoint, **_kwargs):
        self.update_calls += 1
        if not self._updates:
            raise RuntimeError("unexpected update")
        return self._updates.pop(0)

    def close(self) -> None:
        self.closed = True


def _derived_event(sequence=1, *, ordinal=None, event_uid=None):
    import polars as pl

    values = {}
    for name, dtype in derived_event_schema().items():
        if name == "derived_event_sequence":
            values[name] = sequence
        elif name == "source_tick_event_ordinal":
            values[name] = ordinal
        elif name == "event_uid":
            values[name] = event_uid
        elif dtype == pl.Boolean:
            values[name] = False
        else:
            values[name] = 1
    return SimpleNamespace(**values)


@unittest.skipUnless(polars_available(), "Polars is not installed")
class PolarsFrameTests(unittest.TestCase):
    @staticmethod
    def _event(*, ordinal=None, event_uid=None):
        return _derived_event(1, ordinal=ordinal, event_uid=event_uid)

    def test_raw_schema_is_fixed_and_empty_frame_matches(self):
        import polars as pl

        schema = raw_event_schema(
            ("ingress_sequence", "price_p6", "price_valid")
        )
        self.assertEqual(schema["ingress_sequence"], pl.UInt64)
        self.assertEqual(schema["price_p6"], pl.Int64)
        self.assertEqual(schema["price_valid"], pl.UInt8)
        frame = empty_raw_event_frame(tuple(schema))
        self.assertEqual(frame.height, 0)
        self.assertEqual(dict(frame.schema), schema)

    def test_raw_frame_owns_data_after_batch_is_closed(self):
        import polars as pl

        batch = _FakeBatch(
            (
                {"ingress_sequence": 1, "price_p6": -10},
                {
                    "ingress_sequence": (1 << 63) + 5,
                    "price_p6": 20,
                },
            )
        )
        frame = raw_event_batch_frame(batch)
        batch.close()
        self.assertEqual(frame["ingress_sequence"].dtype, pl.UInt64)
        self.assertEqual(
            frame["ingress_sequence"].to_list(),
            [1, (1 << 63) + 5],
        )
        self.assertEqual(frame["price_p6"].to_list(), [-10, 20])

    def test_raw_frame_prefers_bulk_owned_array_copy(self):
        batch = _FakeArrayBatch(
            ({"ingress_sequence": 3, "price_p6": -7},)
        )
        frame = raw_event_batch_frame(batch)
        batch.close()
        self.assertTrue(batch.bulk_copy_used)
        self.assertEqual(frame["ingress_sequence"].item(), 3)
        self.assertEqual(frame["price_p6"].item(), -7)

    def test_latest_unavailable_row_is_preserved_with_null_payload(self):
        import polars as pl

        value = LatestSnapshot(
            session_epoch=9,
            instrument_id=12,
            status=LatestStatus.BOUND_NO_DATA,
        )
        frame = latest_snapshots_frame((value,))
        self.assertEqual(frame.height, 1)
        self.assertEqual(frame["status"].dtype, pl.UInt8)
        self.assertEqual(
            frame["status"].item(), int(LatestStatus.BOUND_NO_DATA)
        )
        self.assertIsNone(frame["last_price_p6"].item())

    def test_facade_returns_direct_dataframe_and_lazyframe(self):
        value = LatestSnapshot(
            session_epoch=9,
            instrument_id=12,
            status=LatestStatus.BOUND_NO_DATA,
        )
        client = SimpleNamespace(
            latest_snapshots=lambda instrument_ids: tuple(
                value for _ in instrument_ids
            )
        )
        facade = as_polars(client)
        frame = facade.latest_snapshot(12)
        lazy = facade.latest_snapshot_lazy(12)
        self.assertEqual(frame.height, 1)
        self.assertEqual(lazy.collect().height, 1)

    def test_partial_kline_coverage_is_not_lost(self):
        bar = LatestKLine(
            session_epoch=11,
            instrument_id=2,
            window_id=60_000,
            status=LatestStatus.AVAILABLE,
            generation=4,
            trade_date=20260803,
            window_duration_ns=60_000_000_000,
            window_start_ns_since_midnight=1,
            window_end_ns_since_midnight=2,
            window_start_unix_ns=3,
            window_end_unix_ns=4,
            open_price_p6=10,
            high_price_p6=12,
            low_price_p6=9,
            close_price_p6=11,
            volume_raw=100,
            trade_count=2,
            revision=1,
            first_event_time_ns_since_midnight=1,
            first_event_sequence=1,
            first_source_sequence=1,
            first_ingress_sequence=1,
            last_event_time_ns_since_midnight=2,
            last_event_sequence=2,
            last_source_sequence=2,
            last_ingress_sequence=2,
            volume_scale=0,
            quantity_unit=1,
            wire_payload=b"K" * 192,
            coverage_flags=(
                KLineCoverageFlag.PROCESS_START_PARTIAL
                | KLineCoverageFlag.NATURAL_WINDOW_LEFT_TRUNCATED
            ),
        )
        coverage = KLineCoverageInfo(
            session_epoch=11,
            coverage_start_unix_ns=123,
            coverage_kind=(
                KLineTemporalCoverage.PROCESS_START_PARTIAL
            ),
        )
        frame = latest_klines_frame(
            (bar,), session_coverage=coverage
        )
        self.assertEqual(frame["coverage_flags"].item(), 3)
        self.assertTrue(frame["process_start_partial"].item())
        self.assertTrue(
            frame["natural_window_left_truncated"].item()
        )
        self.assertEqual(frame["coverage_start_unix_ns"].item(), 123)

    def test_history_coverage_has_an_exact_fixed_schema(self):
        import polars as pl

        coverage = HistoryCoverageInfo(
            run_id=b"C" * 16,
            session_epoch=5,
            trade_date=20260803,
            coverage_kind=TemporalCoverageKind.PROCESS_START_PARTIAL,
            coverage_start_unix_ns=456,
        )
        frame = history_coverage_frame(coverage)
        self.assertEqual(frame["run_id"].dtype, pl.Binary)
        self.assertEqual(frame["coverage_kind"].dtype, pl.UInt8)
        self.assertEqual(frame["coverage_start_unix_ns"].item(), 456)

    def test_event_uid_is_nullable_and_exact_bytes_when_supplied(self):
        import polars as pl

        without_uid = derived_events_frame((self._event(),))
        self.assertEqual(without_uid["event_uid"].dtype, pl.Binary)
        self.assertIsNone(
            without_uid["source_tick_event_ordinal"].item()
        )
        self.assertIsNone(without_uid["event_uid"].item())

        uid = EventUid(
            scope=EventUidScope.SESSION_SOURCE_TICK,
            session_identity=SessionIdentity(b"U" * 16, 9),
            instrument_id=1,
            tick_stream_sequence=1,
            source_tick_event_ordinal=3,
        )
        event = self._event(ordinal=3, event_uid=uid)
        frame = derived_events_frame((event,))
        self.assertEqual(frame["source_tick_event_ordinal"].item(), 3)
        self.assertEqual(frame["event_uid"].item(), bytes(uid))
        self.assertEqual(len(frame["event_uid"].item()), 48)

        certified = certified_order_event_batch_frame(
            (
                SimpleNamespace(
                    canonical_apply_sequence=17,
                    event=event,
                ),
            )
        )
        self.assertEqual(
            certified["canonical_apply_sequence"].dtype, pl.UInt64
        )
        self.assertEqual(certified["canonical_apply_sequence"].item(), 17)


@unittest.skipUnless(polars_available(), "Polars is not installed")
class PolarsBackgroundHistoryTests(unittest.TestCase):
    columns = ("ingress_sequence", "price_p6")

    def _handle(self, reader, **kwargs):
        history_coverage = kwargs.pop(
            "history_coverage",
            HistoryCoverageInfo(
                run_id=b"P" * 16,
                session_epoch=7,
                trade_date=20260803,
                coverage_kind=TemporalCoverageKind.FROM_OPEN,
            ),
        )
        return PolarsInstrumentTickHistory(
            reader,
            1,
            columns=self.columns,
            history_coverage=history_coverage,
            refresh_interval=None,
            **kwargs,
        )

    def test_initial_rows_are_invisible_until_explicit_eof(self):
        release = threading.Event()
        entered = threading.Event()
        cursor = _FakeCursor(
            ({"ingress_sequence": 1, "price_p6": 100},),
            _FakeCheckpoint(1, 1),
            before_eof=release,
            entered=entered,
        )
        handle = self._handle(_FakeReader(cursor))
        self.addCleanup(handle.close)
        self.assertTrue(entered.wait(timeout=2.0))
        with self.assertRaises(PolarsHistoryNotReadyError):
            handle.latest_dataframe()
        release.set()
        handle.wait_ready(timeout=2.0)
        self.assertEqual(handle.latest_dataframe().height, 1)
        identity = handle.dataset_identity
        self.assertEqual(
            identity.product_kind,
            PolarsHistoryProductKind.FAST_RAW_INSTRUMENT_TICKS,
        )
        self.assertEqual(identity.instrument_id, 1)
        self.assertEqual(identity.projection_columns, self.columns)

    def test_delta_is_published_atomically_after_eof(self):
        initial = _FakeCursor(
            ({"ingress_sequence": 1, "price_p6": 100},),
            _FakeCheckpoint(1, 1),
        )
        release = threading.Event()
        entered = threading.Event()
        update = _FakeCursor(
            ({"ingress_sequence": 2, "price_p6": 101},),
            _FakeCheckpoint(3, 2),
            before_eof=release,
            entered=entered,
        )
        handle = self._handle(_FakeReader(initial, (update,)))
        self.addCleanup(handle.close)
        handle.wait_ready(timeout=2.0)
        old = handle.latest_snapshot()

        errors = []

        def refresh():
            try:
                handle.refresh(timeout=3.0)
            except BaseException as error:
                errors.append(error)

        thread = threading.Thread(target=refresh)
        thread.start()
        self.assertTrue(entered.wait(timeout=2.0))
        self.assertEqual(handle.state, PolarsHistoryState.REFRESHING)
        self.assertEqual(handle.latest_dataframe().height, 1)
        release.set()
        thread.join(timeout=3.0)
        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])

        current = handle.latest_snapshot()
        self.assertEqual(current.generation, 3)
        self.assertEqual(current.row_count, 2)
        self.assertEqual(current.cache_version, 2)
        self.assertEqual(
            current.dataframe["ingress_sequence"].to_list(), [1, 2]
        )
        self.assertEqual(old.generation, 1)
        self.assertEqual(old.dataframe.height, 1)

    def test_empty_update_advances_generation_without_rows(self):
        initial = _FakeCursor(
            ({"ingress_sequence": 1, "price_p6": 100},),
            _FakeCheckpoint(1, 1),
        )
        update = _FakeCursor((), _FakeCheckpoint(2, 1))
        handle = self._handle(_FakeReader(initial, (update,)))
        self.addCleanup(handle.close)
        handle.wait_ready(timeout=2.0)
        handle.refresh(timeout=2.0)
        snapshot = handle.latest_snapshot()
        self.assertEqual(snapshot.generation, 2)
        self.assertEqual(snapshot.row_count, 1)
        self.assertEqual(snapshot.cache_version, 2)

    def test_lazyframe_remains_pinned_after_later_commit(self):
        initial = _FakeCursor(
            ({"ingress_sequence": 1, "price_p6": 100},),
            _FakeCheckpoint(1, 1),
        )
        update = _FakeCursor(
            ({"ingress_sequence": 2, "price_p6": 101},),
            _FakeCheckpoint(2, 2),
        )
        handle = self._handle(_FakeReader(initial, (update,)))
        self.addCleanup(handle.close)
        handle.wait_ready(timeout=2.0)
        lazy = handle.latest_lazyframe()
        handle.refresh(timeout=2.0)
        self.assertEqual(lazy.collect().height, 1)
        self.assertEqual(handle.latest_lazyframe().collect().height, 2)

    def test_partial_requires_explicit_opt_in(self):
        rejected = self._handle(
            _FakeReader(
                _FakeCursor((), _FakeCheckpoint(1, 0, False))
            ),
            history_coverage=HistoryCoverageInfo(
                run_id=b"P" * 16,
                session_epoch=7,
                trade_date=20260803,
                coverage_kind=(
                    TemporalCoverageKind.PROCESS_START_PARTIAL
                ),
            ),
        )
        self.addCleanup(rejected.close)
        with self.assertRaises(PolarsHistoryRefreshError):
            rejected.wait_ready(timeout=2.0)

        allowed = self._handle(
            _FakeReader(
                _FakeCursor((), _FakeCheckpoint(1, 0, False))
            ),
            coverage_requirement="allow_process_start_partial",
            history_coverage=HistoryCoverageInfo(
                run_id=b"P" * 16,
                session_epoch=7,
                trade_date=20260803,
                coverage_kind=(
                    TemporalCoverageKind.PROCESS_START_PARTIAL
                ),
                coverage_start_unix_ns=123,
            ),
        )
        self.addCleanup(allowed.close)
        allowed.wait_ready(timeout=2.0)
        snapshot = allowed.latest_snapshot()
        self.assertFalse(snapshot.coverage_from_open)
        self.assertEqual(snapshot.coverage_start_unix_ns, 123)
        self.assertEqual(
            snapshot.coverage_origin,
            PolarsHistoryCoverageOrigin.PROCESS_START_PARTIAL,
        )

    def test_failed_delta_does_not_replace_last_good_snapshot(self):
        initial = _FakeCursor(
            ({"ingress_sequence": 1, "price_p6": 100},),
            _FakeCheckpoint(1, 1),
        )
        invalid = _FakeCursor(
            ({"ingress_sequence": 2, "price_p6": 101},),
            _FakeCheckpoint(2, 3),
        )
        handle = self._handle(_FakeReader(initial, (invalid,)))
        self.addCleanup(handle.close)
        handle.wait_ready(timeout=2.0)
        with self.assertRaises(PolarsHistoryRefreshError):
            handle.refresh(timeout=2.0)
        self.assertEqual(handle.state, PolarsHistoryState.FAILED)
        with self.assertRaises(PolarsHistoryRefreshError):
            handle.latest_dataframe()
        stale = handle.latest_dataframe(allow_stale=True)
        self.assertEqual(stale.height, 1)

    def test_generation_polled_raw_spill_is_checkpoint_pinned(self):
        import polars as pl

        checkpoint = _FakeCheckpoint(1, 1)
        handle = self._handle(
            _FakeReader(
                _FakeCursor(
                    ({"ingress_sequence": 1, "price_p6": 100},),
                    checkpoint,
                )
            )
        )
        self.addCleanup(handle.close)
        handle.wait_ready(timeout=2.0)
        with tempfile.TemporaryDirectory() as directory:
            target = os.path.join(directory, "raw.parquet")
            self.assertEqual(
                handle.spill(
                    target, expected_checkpoint=checkpoint
                ),
                target,
            )
            self.assertEqual(
                pl.read_parquet(target)["ingress_sequence"].to_list(),
                [1],
            )
            with self.assertRaises(PolarsHistoryRefreshError):
                handle.spill(
                    target,
                    expected_checkpoint=_FakeCheckpoint(2, 1),
                )
            # Failed optimistic validation happens before any temp write and
            # leaves the previously published path untouched.
            self.assertEqual(
                pl.read_parquet(target)["ingress_sequence"].to_list(),
                [1],
            )


@dataclass(frozen=True)
class _FakeDerivedCheckpoint:
    raw_checkpoint: _FakeCheckpoint
    derived_event_sequence_exclusive: int
    instrument_id: int = 1
    trade_date: int = 20260803
    market: int = 1
    finalized: bool = False


class _FakeDerivedReader:
    def __init__(self, initial, updates, coverage) -> None:
        self._initial = initial
        self._updates = list(updates)
        self.history_coverage = coverage
        self.closed = False

    def read_all(self):
        return self._initial

    def read_updates(self, _checkpoint):
        if not self._updates:
            raise RuntimeError("unexpected derived update")
        return self._updates.pop(0)

    def close(self) -> None:
        self.closed = True


class _FakeDerivedCursor:
    def __init__(
        self,
        events,
        checkpoint,
        *,
        before_eof=None,
        entered=None,
    ) -> None:
        self._events = tuple(events)
        self._checkpoint = checkpoint
        self._before_eof = before_eof
        self._entered = entered
        self._eof = False

    def __enter__(self):
        return self

    def __exit__(self, _type, _value, _traceback) -> None:
        pass

    def batches(self):
        if self._events:
            yield self._events
        if self._entered is not None:
            self._entered.set()
        if self._before_eof is not None:
            self._before_eof.wait(timeout=5.0)
        self._eof = True

    @property
    def verified_checkpoint(self):
        if not self._eof:
            raise RuntimeError("derived checkpoint requested before EOF")
        return self._checkpoint


@dataclass(frozen=True)
class _FakeCertifiedStatus:
    event_published_sequence: int
    event_generation: int
    correction_epoch: int = 0
    coherent_canonical_apply_frontier: int = 1
    state: int = 3
    resource_exhaustion_count: int = 0
    conflicting_duplicate_count: int = 0
    gap_open_channel_count: int = 0
    catching_up_channel_count: int = 0
    frozen_channel_count: int = 0


class _FakeCertifiedBatch:
    def __init__(self, events, next_sequence, status, coverage) -> None:
        self._events = tuple(
            SimpleNamespace(
                canonical_apply_sequence=index + 10,
                event=event,
            )
            for index, event in enumerate(events)
        )
        self.next_event_sequence = next_sequence
        self.status = status
        self.history_coverage = coverage

    def __len__(self):
        return len(self._events)

    def __iter__(self):
        return iter(self._events)


class _FakeCertifiedReader:
    def __init__(self, batches, coverage) -> None:
        self._batches = list(batches)
        self.history_coverage = coverage
        self.next_event_sequence = 1
        self.closed = False

    def read_batch(self):
        if not self._batches:
            raise RuntimeError("unexpected CERTIFIED read")
        batch = self._batches.pop(0)
        if (
            batch.next_event_sequence
            != self.next_event_sequence + len(batch)
        ):
            raise RuntimeError("fake CERTIFIED cursor mismatch")
        self.next_event_sequence = batch.next_event_sequence
        return batch

    def close(self) -> None:
        self.closed = True


class _BlockingCertifiedReader(_FakeCertifiedReader):
    def __init__(self, batches, coverage, block_call) -> None:
        super().__init__(batches, coverage)
        self._block_call = block_call
        self._calls = 0
        self.entered = threading.Event()
        self.release = threading.Event()

    def read_batch(self):
        self._calls += 1
        if self._calls == self._block_call:
            self.entered.set()
            self.release.wait(timeout=5.0)
        return super().read_batch()


@unittest.skipUnless(polars_available(), "Polars is not installed")
class LivePolarsManifestTests(unittest.TestCase):
    def setUp(self):
        self.coverage = HistoryCoverageInfo(
            run_id=b"M" * 16,
            session_epoch=17,
            trade_date=20260803,
            coverage_kind=TemporalCoverageKind.FROM_OPEN,
        )

    def test_manifest_delta_is_atomic_and_lazyframe_is_pinned(self):
        import polars as pl

        history = LivePolarsHistory(
            {"value": pl.UInt64}, self.coverage
        )
        self.addCleanup(history.close)
        first = pl.DataFrame(
            {"value": [1]}, schema={"value": pl.UInt64}
        )
        history.publish_full(
            (first,),
            generation=1,
            continuity_token="one",
            expected_total_rows=1,
        )
        pinned = history.snapshot().lazyframe
        second = pl.DataFrame(
            {"value": [2]}, schema={"value": pl.UInt64}
        )
        history.publish_delta(
            (second,),
            expected_base_token="one",
            generation=2,
            continuity_token="two",
            expected_total_rows=2,
        )
        self.assertEqual(pinned.collect()["value"].to_list(), [1])
        self.assertEqual(
            history.snapshot().dataframe["value"].to_list(), [1, 2]
        )

    def test_failed_empty_manifest_cannot_be_republished(self):
        import polars as pl

        history = LivePolarsHistory(
            {"value": pl.UInt64}, self.coverage
        )
        self.addCleanup(history.close)
        failure = RuntimeError("terminal producer failure")
        history.fail(failure)

        with self.assertRaises(PolarsHistoryRefreshError) as context:
            history.publish_full(
                (
                    pl.DataFrame(
                        {"value": [1]}, schema={"value": pl.UInt64}
                    ),
                ),
                generation=1,
                continuity_token="one",
                expected_total_rows=1,
            )
        self.assertIs(context.exception.__cause__, failure)
        self.assertIs(history.state, PolarsHistoryState.FAILED)
        with self.assertRaises(PolarsHistoryNotReadyError):
            history.snapshot(allow_stale=True)

    def test_failed_committed_manifest_rejects_delta_and_keeps_stale_cut(self):
        import polars as pl

        history = LivePolarsHistory(
            {"value": pl.UInt64}, self.coverage
        )
        self.addCleanup(history.close)
        first = pl.DataFrame(
            {"value": [1]}, schema={"value": pl.UInt64}
        )
        history.publish_full(
            (first,),
            generation=1,
            continuity_token="one",
            expected_total_rows=1,
        )
        failure = RuntimeError("terminal tail failure")
        history.fail(failure)

        with self.assertRaises(PolarsHistoryRefreshError) as context:
            history.publish_delta(
                (
                    pl.DataFrame(
                        {"value": [2]}, schema={"value": pl.UInt64}
                    ),
                ),
                expected_base_token="one",
                generation=2,
                continuity_token="two",
                expected_total_rows=2,
            )
        self.assertIs(context.exception.__cause__, failure)
        self.assertIs(history.state, PolarsHistoryState.FAILED)
        stale = history.snapshot(allow_stale=True)
        self.assertEqual(stale.continuity_token, "one")
        self.assertEqual(stale.dataframe["value"].to_list(), [1])

    def test_manifest_fixed_schema_includes_column_order(self):
        import polars as pl

        history = LivePolarsHistory(
            {"left": pl.UInt64, "right": pl.Int64}, self.coverage
        )
        self.addCleanup(history.close)
        swapped = pl.DataFrame(
            {"right": [2], "left": [1]},
            schema={"right": pl.Int64, "left": pl.UInt64},
        )
        with self.assertRaises(PolarsSchemaError):
            history.publish_full(
                (swapped,),
                generation=1,
                continuity_token="swapped",
                expected_total_rows=1,
            )
        with self.assertRaises(PolarsHistoryNotReadyError):
            history.snapshot()

    def test_fast_derived_full_and_update_are_eof_atomic(self):
        coverage = HistoryCoverageInfo(
            run_id=b"P" * 16,
            session_epoch=7,
            trade_date=20260803,
            coverage_kind=TemporalCoverageKind.FROM_OPEN,
        )
        initial_checkpoint = _FakeDerivedCheckpoint(
            _FakeCheckpoint(1, 2), 3
        )
        update_checkpoint = _FakeDerivedCheckpoint(
            _FakeCheckpoint(2, 3), 4
        )
        initial = _FakeDerivedCursor(
            (_derived_event(1), _derived_event(2)),
            initial_checkpoint,
        )
        update = _FakeDerivedCursor(
            (_derived_event(3),), update_checkpoint
        )
        reader = _FakeDerivedReader(initial, (update,), coverage)
        history = PolarsInstrumentDerivedEventHistory(
            reader, refresh_interval=None
        )
        self.addCleanup(history.close)
        history.wait_ready(timeout=2.0)
        old = history.latest_snapshot()
        self.assertEqual(old.row_count, 2)
        self.assertEqual(
            old.dataset_identity.product_kind,
            PolarsHistoryProductKind.FAST_DERIVED_INSTRUMENT_EVENTS,
        )
        self.assertEqual(old.dataset_identity.instrument_id, 1)
        self.assertEqual(old.dataset_identity.market, 1)
        history.refresh(timeout=2.0)
        current = history.latest_snapshot()
        self.assertEqual(current.generation, 2)
        self.assertEqual(current.row_count, 3)
        self.assertEqual(old.lazyframe.collect().height, 2)

    def test_fast_derived_rows_remain_hidden_before_eof(self):
        coverage = HistoryCoverageInfo(
            run_id=b"P" * 16,
            session_epoch=7,
            trade_date=20260803,
            coverage_kind=TemporalCoverageKind.FROM_OPEN,
        )
        release = threading.Event()
        entered = threading.Event()
        cursor = _FakeDerivedCursor(
            (_derived_event(1),),
            _FakeDerivedCheckpoint(_FakeCheckpoint(1, 1), 2),
            before_eof=release,
            entered=entered,
        )
        reader = _FakeDerivedReader(cursor, (), coverage)
        history = PolarsInstrumentDerivedEventHistory(
            reader, refresh_interval=None
        )
        self.addCleanup(history.close)
        self.assertTrue(entered.wait(timeout=2.0))
        with self.assertRaises(PolarsHistoryNotReadyError):
            history.latest_dataframe()
        release.set()
        history.wait_ready(timeout=2.0)
        self.assertEqual(history.latest_dataframe().height, 1)

    def test_certified_history_drains_prefix_then_tails(self):
        coverage = HistoryCoverageInfo(
            run_id=b"C" * 16,
            session_epoch=19,
            trade_date=20260803,
            coverage_kind=TemporalCoverageKind.FROM_OPEN,
        )
        initial = _FakeCertifiedBatch(
            (_derived_event(1), _derived_event(2)),
            3,
            _FakeCertifiedStatus(2, 5),
            coverage,
        )
        tail = _FakeCertifiedBatch(
            (_derived_event(3),),
            4,
            _FakeCertifiedStatus(3, 6),
            coverage,
        )
        reader = _FakeCertifiedReader((initial, tail), coverage)
        history = PolarsCertifiedOrderEventHistory(
            reader, poll_interval=None
        )
        self.addCleanup(history.close)
        history.wait_ready(timeout=2.0)
        old = history.latest_snapshot()
        self.assertTrue(old.caught_up)
        self.assertEqual(old.row_count, 2)
        self.assertEqual(
            history.dataset_identity.product_kind,
            PolarsHistoryProductKind.CERTIFIED_ORDER_EVENTS,
        )
        self.assertIsNone(history.dataset_identity.instrument_id)
        history.refresh(timeout=2.0)
        current = history.latest_snapshot()
        self.assertEqual(current.generation, 6)
        self.assertEqual(current.next_event_sequence, 4)
        self.assertEqual(current.row_count, 3)
        self.assertEqual(old.lazyframe.collect().height, 2)

    def test_certified_initial_prefix_is_hidden_until_caught_up(self):
        coverage = HistoryCoverageInfo(
            run_id=b"C" * 16,
            session_epoch=19,
            trade_date=20260803,
            coverage_kind=TemporalCoverageKind.FROM_OPEN,
        )
        first = _FakeCertifiedBatch(
            (_derived_event(1),),
            2,
            _FakeCertifiedStatus(2, 5),
            coverage,
        )
        second = _FakeCertifiedBatch(
            (_derived_event(2),),
            3,
            _FakeCertifiedStatus(2, 5),
            coverage,
        )
        reader = _BlockingCertifiedReader(
            (first, second), coverage, block_call=2
        )
        history = PolarsCertifiedOrderEventHistory(
            reader, poll_interval=None
        )
        self.addCleanup(history.close)
        self.assertTrue(reader.entered.wait(timeout=2.0))
        with self.assertRaises(PolarsHistoryNotReadyError):
            history.latest_dataframe()
        reader.release.set()
        history.wait_ready(timeout=2.0)
        self.assertEqual(history.latest_dataframe().height, 2)

    def test_certified_frozen_initial_cut_never_becomes_ready(self):
        coverage = HistoryCoverageInfo(
            run_id=b"C" * 16,
            session_epoch=19,
            trade_date=20260803,
            coverage_kind=TemporalCoverageKind.FROM_OPEN,
        )
        terminal = _FakeCertifiedBatch(
            (_derived_event(1), _derived_event(2)),
            3,
            _FakeCertifiedStatus(
                2,
                5,
                state=CertifiedOrderEventState.FROZEN_CONFLICT,
            ),
            coverage,
        )
        history = PolarsCertifiedOrderEventHistory(
            _FakeCertifiedReader((terminal,), coverage),
            poll_interval=None,
        )
        self.addCleanup(history.close)
        with self.assertRaises(PolarsHistoryRefreshError):
            history.wait_ready(timeout=2.0)
        self.assertIs(history.state, PolarsHistoryState.FAILED)
        self.assertIn("FROZEN_CONFLICT", str(history.last_error))
        with self.assertRaises(PolarsHistoryNotReadyError):
            history.latest_snapshot(allow_stale=True)

    def test_certified_disabled_initial_cut_never_becomes_ready(self):
        coverage = HistoryCoverageInfo(
            run_id=b"C" * 16,
            session_epoch=19,
            trade_date=20260803,
            coverage_kind=TemporalCoverageKind.FROM_OPEN,
        )
        disabled = _FakeCertifiedBatch(
            (),
            1,
            _FakeCertifiedStatus(
                0,
                0,
                state=CertifiedOrderEventState.DISABLED,
            ),
            coverage,
        )
        history = PolarsCertifiedOrderEventHistory(
            _FakeCertifiedReader((disabled,), coverage),
            poll_interval=None,
        )
        self.addCleanup(history.close)
        with self.assertRaises(PolarsHistoryRefreshError):
            history.wait_ready(timeout=2.0)
        self.assertIs(history.state, PolarsHistoryState.FAILED)
        self.assertIn("DISABLED", str(history.last_error))

    def test_certified_frozen_tail_fails_on_the_observing_poll(self):
        coverage = HistoryCoverageInfo(
            run_id=b"C" * 16,
            session_epoch=19,
            trade_date=20260803,
            coverage_kind=TemporalCoverageKind.FROM_OPEN,
        )
        initial = _FakeCertifiedBatch(
            (_derived_event(1), _derived_event(2)),
            3,
            _FakeCertifiedStatus(2, 5),
            coverage,
        )
        terminal = _FakeCertifiedBatch(
            (_derived_event(3),),
            4,
            _FakeCertifiedStatus(
                3,
                6,
                state=CertifiedOrderEventState.FROZEN_RESOURCE,
            ),
            coverage,
        )
        history = PolarsCertifiedOrderEventHistory(
            _FakeCertifiedReader((initial, terminal), coverage),
            poll_interval=None,
        )
        self.addCleanup(history.close)
        history.wait_ready(timeout=2.0)
        last_good = history.latest_snapshot()
        with self.assertRaises(PolarsHistoryRefreshError):
            history.refresh(timeout=2.0)
        self.assertIs(history.state, PolarsHistoryState.FAILED)
        self.assertIn("FROZEN_RESOURCE", str(history.last_error))
        with self.assertRaises(PolarsHistoryRefreshError):
            history.latest_snapshot()
        stale = history.latest_snapshot(allow_stale=True)
        self.assertEqual(stale.cache_version, last_good.cache_version)
        self.assertEqual(stale.row_count, 2)

    def test_certified_stopped_requires_a_coherently_closed_prefix(self):
        coverage = HistoryCoverageInfo(
            run_id=b"C" * 16,
            session_epoch=19,
            trade_date=20260803,
            coverage_kind=TemporalCoverageKind.FROM_OPEN,
        )
        incomplete = _FakeCertifiedBatch(
            (),
            1,
            _FakeCertifiedStatus(
                1,
                5,
                state=CertifiedOrderEventState.STOPPED,
            ),
            coverage,
        )
        history = PolarsCertifiedOrderEventHistory(
            _FakeCertifiedReader((incomplete,), coverage),
            poll_interval=None,
        )
        self.addCleanup(history.close)
        with self.assertRaises(PolarsHistoryRefreshError):
            history.wait_ready(timeout=2.0)
        self.assertIs(history.state, PolarsHistoryState.FAILED)
        self.assertIn("coherently readable", str(history.last_error))

    def test_certified_stopped_rejects_unresolved_status_counters(self):
        coverage = HistoryCoverageInfo(
            run_id=b"C" * 16,
            session_epoch=19,
            trade_date=20260803,
            coverage_kind=TemporalCoverageKind.FROM_OPEN,
        )
        incomplete = _FakeCertifiedBatch(
            (),
            1,
            _FakeCertifiedStatus(
                0,
                5,
                state=CertifiedOrderEventState.STOPPED,
                gap_open_channel_count=1,
            ),
            coverage,
        )
        history = PolarsCertifiedOrderEventHistory(
            _FakeCertifiedReader((incomplete,), coverage),
            poll_interval=None,
        )
        self.addCleanup(history.close)
        with self.assertRaises(PolarsHistoryRefreshError):
            history.wait_ready(timeout=2.0)
        self.assertIs(history.state, PolarsHistoryState.FAILED)
        self.assertIn("unresolved", str(history.last_error))

    def test_certified_stopped_drains_batch_bounded_prefix_before_ready(self):
        coverage = HistoryCoverageInfo(
            run_id=b"C" * 16,
            session_epoch=19,
            trade_date=20260803,
            coverage_kind=TemporalCoverageKind.FROM_OPEN,
        )
        first = _FakeCertifiedBatch(
            (_derived_event(1),),
            2,
            _FakeCertifiedStatus(
                2,
                5,
                state=CertifiedOrderEventState.STOPPED,
            ),
            coverage,
        )
        complete = _FakeCertifiedBatch(
            (_derived_event(2),),
            3,
            _FakeCertifiedStatus(
                2,
                5,
                state=CertifiedOrderEventState.STOPPED,
            ),
            coverage,
        )
        history = PolarsCertifiedOrderEventHistory(
            _FakeCertifiedReader((first, complete), coverage),
            poll_interval=None,
        )
        self.addCleanup(history.close)
        history.wait_ready(timeout=2.0)
        snapshot = history.latest_snapshot()
        self.assertEqual(snapshot.row_count, 2)
        self.assertEqual(
            snapshot.status.state,
            CertifiedOrderEventState.STOPPED,
        )


class PolarsOptionalImportTests(unittest.TestCase):
    def test_core_package_does_not_import_polars(self):
        environment = dict(os.environ)
        environment["PYTHONPATH"] = PYTHON_ROOT
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                "import sys; import l2flow_realtime; "
                "print(int('polars' in sys.modules))",
            ],
            cwd=REPOSITORY,
            env=environment,
            check=True,
            text=True,
            capture_output=True,
        )
        self.assertEqual(result.stdout.strip(), "0")


if __name__ == "__main__":
    unittest.main()
