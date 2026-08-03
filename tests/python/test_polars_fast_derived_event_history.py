from __future__ import annotations

import unittest
from types import SimpleNamespace

from l2flow_realtime._generation import (
    ENDPOINT_FLAG_COVERAGE_FROM_OPEN,
    ENDPOINT_FLAG_RECORD_COVERAGE_COMPLETE,
    GenerationEndpoint,
)
from l2flow_realtime.checkpoint import (
    CHECKPOINT_BYTES,
    InstrumentTickDeltaCheckpoint,
)
from l2flow_realtime.instrument_derived_event_history import (
    InstrumentDerivedEventCheckpoint,
    InstrumentDerivedEventKind,
    InstrumentOrderRevisionOperation,
)
from l2flow_realtime.models import (
    CatalogScope,
    EventUid,
    EventUidScope,
    HistoryCoverageInfo,
    Market,
    SessionIdentity,
    TemporalCoverageKind,
)
from l2flow_realtime.order_event_delta_live import (
    LiveOrderEventDeltaOverrunError,
)
from l2flow_realtime.polars import (
    PolarsClient,
    PolarsHistoryCoverageError,
    PolarsHistoryNotReadyError,
    PolarsHistoryProductKind,
    PolarsHistoryRefreshError,
    PolarsHistoryState,
    PolarsInstrumentDerivedEventHistory,
    derived_event_schema,
    polars_available,
)
from l2flow_realtime.polars_fast_derived_event_history import (
    FastDerivedEventHistoryToken,
    PolarsFastDerivedEventHistory,
)


RUN_ID = b"D" * 16
SESSION_EPOCH = 7
TRADE_DATE = 20260803
CATALOG_DIGEST = b"C" * 32
INPUT_DIGEST = b"I" * 32
SOURCE_IDS = (100, 101, 102, 103)
EVENT_RUN_ID = b"E" * 16
EVENT_SESSION_EPOCH = 17


def coverage(*, from_open: bool = True) -> HistoryCoverageInfo:
    return HistoryCoverageInfo(
        run_id=RUN_ID,
        session_epoch=SESSION_EPOCH,
        trade_date=TRADE_DATE,
        coverage_kind=(
            TemporalCoverageKind.FROM_OPEN
            if from_open
            else TemporalCoverageKind.PROCESS_START_PARTIAL
        ),
        coverage_start_unix_ns=None if from_open else 123,
    )


def raw_checkpoint(
    generation: int,
    tick_exclusive: int,
    *,
    from_open: bool = True,
) -> InstrumentTickDeltaCheckpoint:
    count = tick_exclusive - 1
    endpoint = GenerationEndpoint(
        run_id=RUN_ID,
        session_epoch=SESSION_EPOCH,
        generation=generation,
        catalog_generation=1,
        data_state_generation=generation,
        ingress_sequence_exclusive=tick_exclusive,
        tick_stream_sequence_exclusive=tick_exclusive,
        recv_monotonic_cut_ns=10_000 + generation,
        history_published_monotonic_ns=20_000 + generation,
        accepted_sequence=count,
        applied_sequence=count,
        catalog_digest=CATALOG_DIGEST,
        input_identity_sha256=INPUT_DIGEST,
        source_stream_ids=SOURCE_IDS,
        source_sequence_exclusive=(1, tick_exclusive, 1, 1),
        trade_date=TRADE_DATE,
        capacity=4,
        bound_count=4,
        available_count=1,
        snapshot_available_count=0,
        tick_available_count=1,
        factor_eligible_count=0,
        catalog_scope=CatalogScope.DECLARED_DAILY_A_SHARE,
        coverage_complete=True,
        flags=(
            ENDPOINT_FLAG_RECORD_COVERAGE_COMPLETE
            | (
                ENDPOINT_FLAG_COVERAGE_FROM_OPEN
                if from_open
                else 0
            )
        ),
    )
    return InstrumentTickDeltaCheckpoint.from_endpoint(
        endpoint,
        instrument_id=1,
        ordinal=0,
        instrument_tick_source_record_counts=(0, count, 0, 0),
        payload_projection=1,
        tick_record_coverage_complete=True,
    )


def derived_checkpoint(
    generation: int,
    tick_exclusive: int,
    event_exclusive: int,
    *,
    from_open: bool = True,
    finalized: bool = False,
) -> InstrumentDerivedEventCheckpoint:
    raw = raw_checkpoint(
        generation, tick_exclusive, from_open=from_open
    )
    return InstrumentDerivedEventCheckpoint(
        raw_checkpoint=raw,
        derived_event_sequence_exclusive=event_exclusive,
        order_state_count=0,
        instrument_id=1,
        trade_date=TRADE_DATE,
        market=Market.SHANGHAI,
        finalized=finalized,
        _native_value=bytes(CHECKPOINT_BYTES + 32),
    )


def event_uid(instrument_id: int, tick: int, ordinal: int = 0):
    return EventUid(
        scope=EventUidScope.SESSION_SOURCE_TICK,
        session_identity=SessionIdentity(RUN_ID, SESSION_EPOCH),
        instrument_id=instrument_id,
        tick_stream_sequence=tick,
        source_tick_event_ordinal=ordinal,
    )


def event(
    product_sequence: int,
    tick: int,
    *,
    instrument_id: int = 1,
    ordinal: int = 0,
    uid=None,
    event_kind: int = int(InstrumentDerivedEventKind.ORDER_REVISION),
    operation: int = int(InstrumentOrderRevisionOperation.UPDATE),
):
    import polars as pl

    values = {}
    for name, dtype in derived_event_schema().items():
        if name == "derived_event_sequence":
            values[name] = product_sequence
        elif name == "instrument_id":
            values[name] = instrument_id
        elif name == "trade_date":
            values[name] = TRADE_DATE
        elif name == "market":
            values[name] = int(Market.SHANGHAI)
        elif name == "tick_stream_sequence":
            values[name] = tick
        elif name == "source_tick_event_ordinal":
            values[name] = ordinal
        elif name == "event_uid":
            values[name] = uid
        elif name == "event_kind":
            values[name] = event_kind
        elif name == "operation":
            values[name] = operation
        elif dtype == pl.Boolean:
            values[name] = False
        else:
            values[name] = 1
    return SimpleNamespace(**values)


class HistoryCursor:
    def __init__(self, events, checkpoint) -> None:
        self._events = tuple(events)
        self._checkpoint = checkpoint
        self._eof = False

    def __enter__(self):
        return self

    def __exit__(self, _type, _value, _traceback):
        pass

    def batches(self):
        if self._events:
            yield self._events
        self._eof = True

    @property
    def verified_checkpoint(self):
        if not self._eof:
            raise RuntimeError("checkpoint requested before EOF")
        return self._checkpoint


class ChunkedHistoryCursor:
    def __init__(self, event_batches, checkpoint) -> None:
        self._event_batches = tuple(
            tuple(events) for events in event_batches
        )
        self._checkpoint = checkpoint
        self._eof = False

    def __enter__(self):
        return self

    def __exit__(self, _type, _value, _traceback):
        pass

    def batches(self):
        yield from self._event_batches
        self._eof = True

    @property
    def verified_checkpoint(self):
        if not self._eof:
            raise RuntimeError("checkpoint requested before EOF")
        return self._checkpoint


class HistoryReader:
    instrument_id = 1

    def __init__(self, initial, updates, session_coverage) -> None:
        self._initial = initial
        self._updates = list(updates)
        self.history_coverage = session_coverage
        self.update_calls = 0
        self.closed = False

    def read_all(self):
        return self._initial

    def read_updates(self, _checkpoint):
        self.update_calls += 1
        if not self._updates:
            raise RuntimeError("unexpected derived update")
        return self._updates.pop(0)

    def close(self):
        self.closed = True


class LiveBatch:
    def __init__(self, events, begin: int) -> None:
        self._events = tuple(events)
        self.metadata = SimpleNamespace(
            next_sequence=begin + len(self._events),
            observed_sequence=begin,
            published_event_sequence=max(
                0, begin + len(self._events) - 1
            ),
            consumed_source_tick_sequence=max(
                (
                    current.tick_stream_sequence
                    for current in self._events
                ),
                default=0,
            ),
        )

    def __len__(self):
        return len(self._events)

    def event(self, index):
        return self._events[index]


class LiveReader:
    def __init__(
        self,
        start: int,
        scripts,
        session_coverage,
        *,
        published: int,
        capacity: int,
        event_identity: SessionIdentity = SessionIdentity(
            EVENT_RUN_ID, EVENT_SESSION_EPOCH
        ),
    ) -> None:
        self.next_sequence = start
        self.source_session_identity = session_coverage.identity
        self.history_coverage = session_coverage
        self.control_snapshot = SimpleNamespace(
            event_published_sequence=published,
            event_session=SimpleNamespace(
                identity=event_identity,
                trade_date=TRADE_DATE,
                ring_capacity=capacity,
            ),
        )
        self._scripts = list(scripts)
        self.closed = False

    def read_batch(self):
        if not self._scripts:
            return LiveBatch((), self.next_sequence)
        result = self._scripts.pop(0)
        if isinstance(result, BaseException):
            raise result
        if result.metadata.next_sequence != self.next_sequence + len(result):
            raise RuntimeError("fake live cursor is discontinuous")
        self.next_sequence = result.metadata.next_sequence
        return result

    def close(self):
        self.closed = True


class Client:
    def __init__(self, readers, session_coverage=None, history_reader=None):
        self._readers = {
            key: list(value) for key, value in readers.items()
        }
        self._coverage = session_coverage
        self._history_reader = history_reader
        self.open_calls = []

    def history_coverage(self):
        return self._coverage

    def open_instrument_derived_event_history(self, *_args, **_kwargs):
        return self._history_reader

    def open_live_order_events(
        self,
        path,
        *,
        start_event_sequence,
        **kwargs,
    ):
        self.open_calls.append(
            (path, start_event_sequence, dict(kwargs))
        )
        try:
            return self._readers[start_event_sequence].pop(0)
        except (KeyError, IndexError) as error:
            raise RuntimeError(
                f"unexpected live open at {start_event_sequence}"
            ) from error


@unittest.skipUnless(polars_available(), "Polars is not installed")
class PolarsFastDerivedEventHistoryTests(unittest.TestCase):
    def test_uid_reconciliation_replaces_overlap_without_using_event_sequence(self):
        import polars as pl

        session_coverage = coverage()
        uid10 = event_uid(1, 10)
        uid11 = event_uid(1, 11)
        uid12 = event_uid(1, 12)
        first = derived_checkpoint(1, 12, 3)
        second = derived_checkpoint(2, 13, 4)
        reader = HistoryReader(
            HistoryCursor(
                (
                    event(1, 10, uid=uid10),
                    event(2, 11, uid=uid11),
                ),
                first,
            ),
            (
                HistoryCursor(
                    (event(3, 12, uid=uid12),), second
                ),
            ),
            session_coverage,
        )
        # These product-local live event sequences intentionally have no
        # relationship to the per-instrument History sequence.
        initial_live = LiveBatch(
            (
                event(800, 11, uid=uid11),
                event(801, 12, uid=uid12),
            ),
            1,
        )
        live = LiveReader(
            1,
            (initial_live,),
            session_coverage,
            published=2,
            capacity=8,
        )
        cache = PolarsFastDerivedEventHistory(
            Client({1: (live,)}),
            reader,
            "/events.sock",
            durable_refresh_interval=None,
            autostart=False,
        )
        self.addCleanup(cache.close)
        cache._publish_initial()
        pinned = cache.latest_snapshot()
        self.assertEqual(pinned.row_count, 3)
        self.assertEqual(
            pinned.dataframe.filter(
                pl.col("event_uid") == pl.lit(bytes(uid12))
            )["derived_event_sequence"].item(),
            801,
        )

        self.assertTrue(cache._refresh_durable())
        current = cache.latest_snapshot()
        self.assertEqual(current.row_count, 3)
        self.assertEqual(
            current.dataframe["event_uid"].n_unique(), 3
        )
        self.assertEqual(
            current.dataframe.filter(
                pl.col("event_uid") == pl.lit(bytes(uid12))
            )["derived_event_sequence"].item(),
            3,
        )
        self.assertEqual(
            pinned.dataframe.filter(
                pl.col("event_uid") == pl.lit(bytes(uid12))
            )["derived_event_sequence"].item(),
            801,
        )
        self.assertIsInstance(
            current.continuity_token, FastDerivedEventHistoryToken
        )
        self.assertEqual(
            current.dataset_identity.product_kind,
            PolarsHistoryProductKind.FAST_DERIVED_INSTRUMENT_EVENTS,
        )
        self.assertEqual(current.dataset_identity.instrument_id, 1)
        self.assertEqual(current.dataset_identity.market, 1)

    def test_private_durable_chunks_follow_compaction_threshold(self):
        session_coverage = coverage()
        uid10 = event_uid(1, 10)
        uid11 = event_uid(1, 11)
        uid12 = event_uid(1, 12)
        first = derived_checkpoint(1, 12, 3)
        second = derived_checkpoint(2, 13, 4)
        history = HistoryReader(
            ChunkedHistoryCursor(
                (
                    (event(1, 10, uid=uid10),),
                    (event(2, 11, uid=uid11),),
                ),
                first,
            ),
            (
                ChunkedHistoryCursor(
                    ((event(3, 12, uid=uid12),),), second
                ),
            ),
            session_coverage,
        )
        stream = LiveReader(
            1,
            (LiveBatch((), 1),),
            session_coverage,
            published=0,
            capacity=8,
        )
        cache = PolarsFastDerivedEventHistory(
            Client({1: (stream,)}),
            history,
            "/events.sock",
            durable_refresh_interval=None,
            compact_after_chunks=1,
            autostart=False,
        )
        self.addCleanup(cache.close)

        cache._publish_initial()
        pinned = cache.latest_snapshot()
        self.assertEqual(len(cache._durable_chunks), 1)
        self.assertEqual(cache._durable_chunks[0].height, 2)

        self.assertTrue(cache._refresh_durable())
        self.assertEqual(len(cache._durable_chunks), 1)
        self.assertEqual(cache._durable_chunks[0].height, 3)
        self.assertEqual(cache.latest_snapshot().row_count, 3)
        self.assertEqual(pinned.row_count, 2)

    def test_overrun_waits_until_raw_frontier_is_strictly_after_oldest_tick(self):
        session_coverage = coverage()
        uid18 = event_uid(1, 18)
        uid26 = event_uid(1, 26)
        initial = derived_checkpoint(1, 20, 2)
        equal_frontier = derived_checkpoint(2, 25, 2)
        covering_frontier = derived_checkpoint(3, 26, 2)
        history = HistoryReader(
            HistoryCursor((event(1, 18, uid=uid18),), initial),
            (
                HistoryCursor((), equal_frontier),
                HistoryCursor((), covering_frontier),
            ),
            session_coverage,
        )
        initial_stream = LiveReader(
            1,
            (LiveBatch((), 1),),
            session_coverage,
            published=0,
            capacity=8,
        )
        retained = LiveBatch(
            (
                event(
                    500,
                    25,
                    instrument_id=2,
                    uid=event_uid(2, 25),
                ),
                event(501, 26, uid=uid26),
            ),
            5,
        )
        repaired_streams = tuple(
            LiveReader(
                5,
                (retained,),
                session_coverage,
                published=6,
                capacity=2,
            )
            for _ in range(3)
        )
        client = Client({1: (initial_stream,), 5: repaired_streams})
        cache = PolarsFastDerivedEventHistory(
            client,
            history,
            "/events.sock",
            durable_refresh_interval=None,
            autostart=False,
        )
        self.addCleanup(cache.close)
        cache._publish_initial()
        overrun = LiveOrderEventDeltaOverrunError(
            "read",
            10,
            metadata=SimpleNamespace(observed_sequence=5),
        )
        cache._repair_overrun(overrun)

        snapshot = cache.latest_snapshot()
        # Exclusive 25 covers ticks <25, not tick 25.  Only exclusive 26 can
        # bridge an oldest retained row whose source tick is 25.
        self.assertEqual(history.update_calls, 2)
        self.assertEqual(
            snapshot.checkpoint.raw_checkpoint
            .tick_stream_sequence_exclusive,
            26,
        )
        self.assertEqual(
            set(snapshot.dataframe["event_uid"].to_list()),
            {bytes(uid18), bytes(uid26)},
        )
        self.assertEqual(
            [value[1] for value in client.open_calls],
            [1, 5, 5, 5],
        )

    def test_missing_history_uid_in_covered_overlap_fails_before_publication(self):
        session_coverage = coverage()
        uid10 = event_uid(1, 10)
        uid11 = event_uid(1, 11)
        checkpoint = derived_checkpoint(1, 12, 2)
        history = HistoryReader(
            HistoryCursor((event(1, 10, uid=uid10),), checkpoint),
            (),
            session_coverage,
        )
        stream = LiveReader(
            1,
            (LiveBatch((event(999, 11, uid=uid11),), 1),),
            session_coverage,
            published=1,
            capacity=8,
        )
        cache = PolarsFastDerivedEventHistory(
            Client({1: (stream,)}),
            history,
            "/events.sock",
            durable_refresh_interval=None,
            autostart=False,
        )
        self.addCleanup(cache.close)
        with self.assertRaisesRegex(
            PolarsHistoryRefreshError, "missing an overlapping live UID"
        ):
            cache._publish_initial()
        with self.assertRaises(PolarsHistoryNotReadyError):
            cache.latest_snapshot()
        self.assertTrue(stream.closed)

    def test_source_free_finalize_requires_finalized_history_proof(self):
        session_coverage = coverage()
        checkpoint = derived_checkpoint(1, 2, 1)
        history = HistoryReader(
            HistoryCursor((), checkpoint), (), session_coverage
        )
        finalize = event(
            900,
            0,
            ordinal=None,
            uid=None,
            operation=int(InstrumentOrderRevisionOperation.FINALIZE),
        )
        stream = LiveReader(
            1,
            (LiveBatch((finalize,), 1),),
            session_coverage,
            published=1,
            capacity=8,
        )
        cache = PolarsFastDerivedEventHistory(
            Client({1: (stream,)}),
            history,
            "/events.sock",
            durable_refresh_interval=None,
            autostart=False,
        )
        self.addCleanup(cache.close)
        with self.assertRaisesRegex(
            PolarsHistoryCoverageError, "finalized History proof"
        ):
            cache._publish_initial()
        with self.assertRaises(PolarsHistoryNotReadyError):
            cache.latest_snapshot()

    def test_source_free_finalize_uses_only_authoritative_history_row(self):
        session_coverage = coverage()
        checkpoint = derived_checkpoint(
            1, 2, 2, finalized=True
        )
        durable_finalize = event(
            1,
            0,
            ordinal=None,
            uid=None,
            operation=int(InstrumentOrderRevisionOperation.FINALIZE),
        )
        live_finalize = event(
            900,
            0,
            ordinal=None,
            uid=None,
            operation=int(InstrumentOrderRevisionOperation.FINALIZE),
        )
        history = HistoryReader(
            HistoryCursor((durable_finalize,), checkpoint),
            (),
            session_coverage,
        )
        stream = LiveReader(
            1,
            (LiveBatch((live_finalize,), 1),),
            session_coverage,
            published=1,
            capacity=8,
        )
        cache = PolarsFastDerivedEventHistory(
            Client({1: (stream,)}),
            history,
            "/events.sock",
            durable_refresh_interval=None,
            autostart=False,
        )
        self.addCleanup(cache.close)
        cache._publish_initial()
        snapshot = cache.latest_snapshot()
        self.assertEqual(snapshot.row_count, 1)
        self.assertEqual(
            snapshot.dataframe["derived_event_sequence"].to_list(), [1]
        )
        self.assertEqual(snapshot.dataframe["event_uid"].to_list(), [None])

    def test_event_uid_ordinal_mismatch_fails_closed(self):
        session_coverage = coverage()
        checkpoint = derived_checkpoint(1, 10, 1)
        history = HistoryReader(
            HistoryCursor((), checkpoint), (), session_coverage
        )
        stream = LiveReader(
            1,
            (
                LiveBatch(
                    (event(20, 10, ordinal=1, uid=event_uid(1, 10, 0)),),
                    1,
                ),
            ),
            session_coverage,
            published=1,
            capacity=8,
        )
        cache = PolarsFastDerivedEventHistory(
            Client({1: (stream,)}),
            history,
            "/events.sock",
            durable_refresh_interval=None,
            autostart=False,
        )
        self.addCleanup(cache.close)
        with self.assertRaisesRegex(
            PolarsHistoryCoverageError, "coordinates disagree"
        ):
            cache._publish_initial()

    def test_reconnect_rejects_changed_event_ring_session(self):
        session_coverage = coverage()
        checkpoint = derived_checkpoint(1, 2, 1)
        history = HistoryReader(
            HistoryCursor((), checkpoint), (), session_coverage
        )
        first = LiveReader(
            1,
            (LiveBatch((), 1),),
            session_coverage,
            published=0,
            capacity=8,
        )
        replacement = LiveReader(
            2,
            (LiveBatch((), 2),),
            session_coverage,
            published=1,
            capacity=8,
            event_identity=SessionIdentity(b"F" * 16, 18),
        )
        cache = PolarsFastDerivedEventHistory(
            Client({1: (first,), 2: (replacement,)}),
            history,
            "/events.sock",
            durable_refresh_interval=None,
            autostart=False,
        )
        self.addCleanup(cache.close)
        cache._publish_initial()
        with self.assertRaisesRegex(
            PolarsHistoryCoverageError, "ring session changed"
        ):
            cache._open_stream(2)
        self.assertTrue(replacement.closed)

    def test_close_closes_live_stream_and_history_reader(self):
        session_coverage = coverage()
        checkpoint = derived_checkpoint(1, 2, 1)
        history = HistoryReader(
            HistoryCursor((), checkpoint), (), session_coverage
        )
        stream = LiveReader(
            1,
            (LiveBatch((), 1),),
            session_coverage,
            published=0,
            capacity=8,
        )
        cache = PolarsFastDerivedEventHistory(
            Client({1: (stream,)}),
            history,
            "/events.sock",
            durable_refresh_interval=None,
            autostart=False,
        )
        cache._publish_initial()
        cache.close()
        self.assertTrue(stream.closed)
        self.assertTrue(history.closed)
        self.assertEqual(cache.state, PolarsHistoryState.CLOSED)

    def test_partial_coverage_requires_explicit_opt_in(self):
        partial = coverage(from_open=False)
        checkpoint = derived_checkpoint(
            1, 2, 1, from_open=False
        )
        allowed_reader = HistoryReader(
            HistoryCursor((), checkpoint), (), partial
        )
        partial_stream = LiveReader(
            1,
            (LiveBatch((), 1),),
            partial,
            published=0,
            capacity=8,
        )
        allowed = PolarsFastDerivedEventHistory(
            Client({1: (partial_stream,)}),
            allowed_reader,
            "/events.sock",
            coverage_requirement="allow_process_start_partial",
            autostart=False,
        )
        self.assertEqual(allowed.history_coverage, partial)
        allowed._publish_initial()
        self.assertEqual(
            allowed.latest_snapshot().history_coverage, partial
        )
        allowed.close()

        with self.assertRaises(PolarsHistoryCoverageError):
            PolarsFastDerivedEventHistory(
                Client({}),
                HistoryReader(
                    HistoryCursor((), checkpoint), (), partial
                ),
                "/events.sock",
                coverage_requirement="from_open",
                autostart=False,
            )

    def test_public_facade_is_opt_in_and_live_tail_autostarts(self):
        session_coverage = coverage()
        checkpoint = derived_checkpoint(1, 2, 1)
        plain_reader = HistoryReader(
            HistoryCursor((), checkpoint), (), session_coverage
        )
        plain_client = Client(
            {}, session_coverage, plain_reader
        )
        plain = PolarsClient(
            plain_client
        ).open_instrument_derived_event_history(
            1, refresh_interval=None
        )
        self.addCleanup(plain.close)
        self.assertIsInstance(
            plain, PolarsInstrumentDerivedEventHistory
        )
        plain.wait_ready(timeout=2.0)
        self.assertEqual(plain_client.open_calls, [])

        live_reader = HistoryReader(
            HistoryCursor((), checkpoint), (), session_coverage
        )
        stream = LiveReader(
            1,
            (LiveBatch((), 1),),
            session_coverage,
            published=0,
            capacity=8,
        )
        live_client = Client(
            {1: (stream,)}, session_coverage, live_reader
        )
        live = PolarsClient(
            live_client
        ).open_instrument_derived_event_history(
            1,
            live_tail=True,
            event_control_socket_path="/events.sock",
            refresh_interval=None,
        )
        self.addCleanup(live.close)
        self.assertIsInstance(live, PolarsFastDerivedEventHistory)
        live.wait_ready(timeout=2.0)
        self.assertEqual(len(live_client.open_calls), 1)
        self.assertEqual(live_client.open_calls[0][0], "/events.sock")


if __name__ == "__main__":
    unittest.main()
