import ctypes
import gc
import os
import struct
import tempfile
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from l2flow_realtime.client import L2FlowClient
from l2flow_realtime.certified_tick_history import (
    CertifiedTickHistoryCapacityError,
    CertifiedTickHistoryFailure,
    CertifiedTickHistoryProducerFailedError,
    CertifiedTickHistoryReader,
    CertifiedTickHistoryState,
    _ReadResultC,
    _SessionC,
    _SlotC,
    _StatusC,
)
from l2flow_realtime.models import (
    CatalogScope,
    HistoryCoverageInfo,
    SessionIdentity,
    ServerState,
    TemporalCoverageKind,
    TickAction,
    UnavailableError,
    WireFormatError,
)
from l2flow_realtime.polars import (
    PolarsCertifiedTickHistory,
    PolarsHistoryCoverageError,
    PolarsHistoryProductKind,
    PolarsHistoryRefreshError,
    PolarsHistoryState,
    as_polars,
    certified_tick_batch_frame,
    certified_tick_batch_lazyframe,
    certified_tick_schema,
    polars_available,
)


_COMMON = struct.Struct("<IIIIQQQQqqqQQQIIII6B10x")
_TICK_HEAD = struct.Struct("<IIqqii8B")
_DECIMAL = struct.Struct("<qqBBB5x")
_QUANTITY = struct.Struct("<qBBB5x")
_ENVELOPE = struct.Struct("<QQQQ")
_RUN_ID = bytes(range(1, 17))
_TRADE_DATE = 20260730


class _Function:
    def __init__(self, callback):
        self.callback = callback
        self.argtypes = None
        self.restype = None

    def __call__(self, *args):
        return self.callback(*args)


def _tick_payload(sequence: int, instrument_id: int = 1) -> bytes:
    payload = bytearray(336)
    _COMMON.pack_into(
        payload,
        0,
        2,
        len(payload),
        instrument_id,
        instrument_id - 1,
        sequence,
        sequence,
        sequence,
        0,
        1_000 + sequence,
        2_000 + sequence,
        3_000 + sequence,
        4_000 + sequence,
        0,
        0,
        5,
        _TRADE_DATE,
        93_000_000,
        0,
        1,
        2,
        1,
        2,
        1,
        1,
    )
    _TICK_HEAD.pack_into(
        payload,
        128,
        0,
        0,
        5,
        6,
        0,
        0,
        3,
        1,
        2,
        1,
        3,
        0,
        0,
        0,
    )
    _DECIMAL.pack_into(payload, 168, 12_345, 123_450_000, 2, 1, 0)
    _QUANTITY.pack_into(payload, 192, 800 + sequence, 0, 1, 0)
    return bytes(payload)


class _FakeLibrary:
    def __init__(self):
        self.run_id = _RUN_ID
        self.published = 2
        self.state = CertifiedTickHistoryState.ACTIVE
        self.failure = CertifiedTickHistoryFailure.NONE
        self.closed = False
        self.corrupt_sequence = None
        self.timeout_ms = None
        self.read_calls = 0
        self.status_calls = 0
        self.l2flow_certified_tick_history_reader_open_v1 = _Function(
            self._open
        )
        self.l2flow_certified_tick_history_reader_close_v1 = _Function(
            self._close
        )
        self.l2flow_certified_tick_history_reader_session_v1 = _Function(
            self._session
        )
        self.l2flow_certified_tick_history_reader_status_v1 = _Function(
            self._status
        )
        self.l2flow_certified_tick_history_reader_read_v1 = _Function(
            self._read
        )

    def _open(self, path, expected, timeout_ms, output, system_error):
        value = expected._obj
        self.timeout_ms = int(timeout_ms)
        if (
            path != b"/tmp/certified-ticks.sock"
            or bytes(value.run_id) != self.run_id
            or value.session_epoch != 7
            or value.trade_date != _TRADE_DATE
        ):
            system_error._obj.value = 22
            return 2
        output._obj.value = 0x1234
        system_error._obj.value = 0
        return 0

    def _close(self, _handle):
        self.closed = True

    def _session(self, _handle, output):
        value = output._obj
        value.run_id[:] = self.run_id
        value.session_epoch = 7
        value.total_mapping_bytes = 8192
        value.tick_capacity = 8
        value.trade_date = _TRADE_DATE
        value.slot_bytes = ctypes.sizeof(_SlotC)
        value.coverage_kind = TemporalCoverageKind.FROM_OPEN
        value.reserved_coverage = 0
        value.coverage_start_unix_ns = 0
        return 0

    def _fill_status(self, value):
        value.status_schema_version = 1
        value.status_bytes = ctypes.sizeof(_StatusC)
        value.publish_tag = 2
        value.heartbeat_monotonic_ns = 100
        value.canonical_apply_frontier = self.published
        value.generation = self.published
        value.published_tick_count = self.published
        value.committed_mapping_bytes = 8192
        value.state = int(self.state)
        value.failure = int(self.failure)

    def _status(self, _handle, output):
        self.status_calls += 1
        self._fill_status(output._obj)
        return 0

    def _fill_row(self, row: _SlotC, sequence: int):
        row.publish_tag = 2
        canonical = (
            sequence + 1
            if sequence == self.corrupt_sequence
            else sequence
        )
        envelope = _ENVELOPE.pack(
            canonical,
            1,
            1,
            10_000 + sequence,
        ) + _tick_payload(sequence)
        ctypes.memmove(
            ctypes.addressof(row) + _SlotC.payload_words.offset,
            envelope,
            len(envelope),
        )

    def _read(self, _handle, first, rows, capacity, output):
        self.read_calls += 1
        first = int(first)
        result = output._obj
        result.result_schema_version = 1
        result.result_bytes = ctypes.sizeof(_ReadResultC)
        result.next_canonical_apply_sequence = first
        self._fill_status(result.status)
        if first > 8:
            if first != 9 or self.published != 8:
                return 5
            if self.state is CertifiedTickHistoryState.ACTIVE:
                return 2
            if self.state is CertifiedTickHistoryState.COMPLETE:
                return 3
            return 4
        if first <= self.published:
            count = min(int(capacity), self.published - first + 1)
            for index in range(count):
                self._fill_row(rows[index], first + index)
            result.records_written = count
            result.next_canonical_apply_sequence = first + count
            return 0
        if self.state is CertifiedTickHistoryState.ACTIVE:
            return 2
        if self.state is CertifiedTickHistoryState.COMPLETE:
            return 3
        return 4


def _coverage(kind=TemporalCoverageKind.FROM_OPEN):
    return HistoryCoverageInfo(
        run_id=_RUN_ID,
        session_epoch=7,
        trade_date=_TRADE_DATE,
        coverage_kind=kind,
        coverage_start_unix_ns=(
            1_000
            if kind is TemporalCoverageKind.PROCESS_START_PARTIAL
            else None
        ),
    )


class CertifiedTickHistoryReaderTests(unittest.TestCase):
    def _connect(self, library, **kwargs):
        return CertifiedTickHistoryReader.connect(
            "/tmp/certified-ticks.sock",
            run_id=_RUN_ID,
            session_epoch=7,
            trade_date=_TRADE_DATE,
            history_coverage=_coverage(),
            batch_records=4,
            native_library=library,
            **kwargs,
        )

    def test_owned_dense_history_becomes_tail_and_fails_closed(self):
        library = _FakeLibrary()
        reader = self._connect(library, timeout=None)
        self.assertEqual(library.timeout_ms, 0)
        self.assertTrue(reader.session.coverage_from_open)
        status = reader.status()
        self.assertEqual(status.state, CertifiedTickHistoryState.ACTIVE)
        self.assertEqual(status.canonical_apply_frontier, 2)

        history = reader.read_batch()
        self.assertEqual(len(history), 2)
        self.assertEqual(len(history.buffer), 2 * 512)
        self.assertTrue(history.buffer.readonly)
        self.assertEqual(history.first_canonical_apply_sequence, 1)
        self.assertEqual(history.next_canonical_apply_sequence, 3)
        self.assertEqual(
            [row.canonical_apply_sequence for row in history],
            [1, 2],
        )
        self.assertEqual(history.row(0).action, TickAction.TRADE)
        self.assertEqual(history.row(0).price.normalized_p6, 123_450_000)
        saved = history.envelope_bytes(0)

        idle = reader.read_batch()
        self.assertEqual(len(idle), 0)
        self.assertTrue(idle.tail_idle)
        self.assertEqual(reader.next_canonical_apply_sequence, 3)
        # A caught-up poll reads only the coherent status cut.  It must not
        # allocate/pass another default-size owned array through the C ABI.
        self.assertEqual(library.read_calls, 1)

        library.published = 3
        tail = reader.read_batch()
        self.assertEqual(len(tail), 1)
        self.assertEqual(tail.row(0).canonical_apply_sequence, 3)
        self.assertEqual(reader.next_canonical_apply_sequence, 4)
        self.assertEqual(history.envelope_bytes(0), saved)

        library.state = CertifiedTickHistoryState.FAILED
        library.failure = CertifiedTickHistoryFailure.UPSTREAM
        with self.assertRaises(CertifiedTickHistoryProducerFailedError) as cm:
            reader.read_batch()
        self.assertEqual(
            cm.exception.status.failure,
            CertifiedTickHistoryFailure.UPSTREAM,
        )
        self.assertEqual(reader.next_canonical_apply_sequence, 4)

        reader.close()
        reader.close()
        self.assertTrue(library.closed)


@unittest.skipUnless(polars_available(), "Polars is not installed")
class PolarsCertifiedTickHistoryTests(unittest.TestCase):
    def _reader(self, library, *, batch_records=4):
        return CertifiedTickHistoryReader.connect(
            "/tmp/certified-ticks.sock",
            run_id=_RUN_ID,
            session_epoch=7,
            trade_date=_TRADE_DATE,
            history_coverage=_coverage(),
            batch_records=batch_records,
            native_library=library,
        )

    def test_silent_tail_is_atomic_compacted_and_old_lazyframe_is_pinned(self):
        import polars as pl

        library = _FakeLibrary()
        history = PolarsCertifiedTickHistory(
            self._reader(library, batch_records=1),
            poll_interval=0.001,
            compact_after_chunks=1,
        )
        self.addCleanup(history.close)
        history.wait_ready(timeout=2.0)
        old = history.latest_snapshot()
        pinned = old.lazyframe
        self.assertEqual(old.row_count, 2)
        self.assertTrue(old.caught_up)
        self.assertEqual(old.manifest_chunk_count, 1)
        self.assertEqual(
            old.dataset_identity.product_kind,
            PolarsHistoryProductKind.CERTIFIED_TICKS,
        )
        # This versions the CERTIFIED Tick journal/export contract (V1),
        # independently of the embedded TickPayload schema version (V2).
        self.assertEqual(old.dataset_identity.payload_projection, 1)
        self.assertIsNone(old.dataset_identity.instrument_id)

        # No explicit refresh is issued: the opt-in updater silently observes
        # and publishes the new dense canonical suffix.
        library.published = 3
        deadline = time.monotonic() + 2.0
        while True:
            current = history.latest_snapshot()
            if current.row_count == 3:
                break
            if time.monotonic() >= deadline:
                self.fail("silent CERTIFIED Tick tail did not publish")
            time.sleep(0.001)
        self.assertEqual(
            current.dataframe["canonical_apply_sequence"].to_list(),
            [1, 2, 3],
        )
        self.assertEqual(current.manifest_chunk_count, 1)
        self.assertEqual(
            pinned.collect()["canonical_apply_sequence"].to_list(),
            [1, 2],
        )

        # Empty caught-up polls still carry lifecycle state.  COMPLETE must be
        # atomically visible even though it appends no tick row.
        library.state = CertifiedTickHistoryState.COMPLETE
        deadline = time.monotonic() + 2.0
        while history.latest_snapshot().status.state is not (
            CertifiedTickHistoryState.COMPLETE
        ):
            if time.monotonic() >= deadline:
                self.fail("terminal CERTIFIED Tick status stayed hidden")
            time.sleep(0.001)

        with tempfile.TemporaryDirectory() as directory:
            target = os.path.join(directory, "certified-ticks.parquet")
            self.assertEqual(history.spill(target), target)
            self.assertEqual(
                pl.read_parquet(target)[
                    "canonical_apply_sequence"
                ].to_list(),
                [1, 2, 3],
            )

    def test_producer_failure_retains_only_explicit_stale_prefix(self):
        library = _FakeLibrary()
        history = PolarsCertifiedTickHistory(
            self._reader(library), poll_interval=None
        )
        self.addCleanup(history.close)
        history.wait_ready(timeout=2.0)
        library.state = CertifiedTickHistoryState.FAILED
        library.failure = CertifiedTickHistoryFailure.UPSTREAM
        with self.assertRaises(PolarsHistoryRefreshError):
            history.refresh(timeout=2.0)
        with self.assertRaises(PolarsHistoryRefreshError):
            history.latest_snapshot()
        stale = history.latest_snapshot(allow_stale=True)
        self.assertEqual(stale.row_count, 2)
        self.assertEqual(stale.next_canonical_apply_sequence, 3)

    def test_initial_failed_prefix_never_publishes_ready(self):
        library = _FakeLibrary()
        library.state = CertifiedTickHistoryState.FAILED
        library.failure = CertifiedTickHistoryFailure.UPSTREAM
        history = PolarsCertifiedTickHistory(
            self._reader(library), poll_interval=None
        )
        self.addCleanup(history.close)

        with self.assertRaises(PolarsHistoryRefreshError):
            history.wait_ready(timeout=2.0)
        self.assertIs(history.state, PolarsHistoryState.FAILED)
        self.assertIsInstance(
            history.last_error,
            CertifiedTickHistoryProducerFailedError,
        )
        # The first owned prefix already carries FAILED.  The adapter must
        # fail on that coherent cut rather than publish READY and require a
        # second poll to discover the terminal lifecycle.
        self.assertEqual(library.read_calls, 1)

    def test_failed_tail_batch_does_not_extend_last_good_manifest(self):
        library = _FakeLibrary()
        history = PolarsCertifiedTickHistory(
            self._reader(library), poll_interval=None
        )
        self.addCleanup(history.close)
        history.wait_ready(timeout=2.0)
        good = history.latest_snapshot()
        self.assertEqual(good.row_count, 2)

        # Sequence 3 is readable, but the same coherent journal cut is
        # terminal FAILED.  That row is not a last-good Polars generation.
        library.published = 3
        library.state = CertifiedTickHistoryState.FAILED
        library.failure = CertifiedTickHistoryFailure.UPSTREAM
        with self.assertRaises(PolarsHistoryRefreshError):
            history.refresh(timeout=2.0)

        self.assertIs(history.state, PolarsHistoryState.FAILED)
        self.assertIsInstance(
            history.last_error,
            CertifiedTickHistoryProducerFailedError,
        )
        stale = history.latest_snapshot(allow_stale=True)
        self.assertEqual(stale.row_count, 2)
        self.assertEqual(stale.next_canonical_apply_sequence, 3)
        self.assertEqual(
            stale.dataframe["canonical_apply_sequence"].to_list(),
            [1, 2],
        )
        # One initial prefix read plus the failed tail read: no extra poll is
        # needed to expose the failure.
        self.assertEqual(library.read_calls, 2)

    def test_from_open_and_memory_limits_fail_closed(self):
        partial = type(
            "PartialReader",
            (),
            {
                "history_coverage": _coverage(
                    TemporalCoverageKind.PROCESS_START_PARTIAL
                ),
                "next_canonical_apply_sequence": 1,
            },
        )()
        with self.assertRaises(PolarsHistoryCoverageError):
            PolarsCertifiedTickHistory(partial, autostart=False)

        library = _FakeLibrary()
        history = PolarsCertifiedTickHistory(
            self._reader(library),
            poll_interval=None,
            maximum_rows=1,
        )
        self.addCleanup(history.close)
        with self.assertRaises(PolarsHistoryRefreshError):
            history.wait_ready(timeout=2.0)
        self.assertIsNotNone(history.last_error)

    def test_polars_client_opens_sequence_one_and_preserves_timeout_default(self):
        library = _FakeLibrary()
        reader = self._reader(library)

        class CoreClient:
            def __init__(self):
                self.arguments = None
                self.session = type(
                    "FastSession",
                    (),
                    {
                        "identity": SessionIdentity(_RUN_ID, 7),
                        "trade_date": _TRADE_DATE,
                        "catalog_digest": b"D" * 32,
                        "catalog_scope": (
                            CatalogScope.DECLARED_DAILY_A_SHARE
                        ),
                        "catalog_version": 1,
                    },
                )()

            def session_info(self):
                return self.session

            def open_certified_tick_history(self, path, **kwargs):
                self.arguments = (path, kwargs)
                return reader

        core = CoreClient()
        history = as_polars(core).open_certified_tick_history(
            "/tmp/certified-ticks.sock",
            poll_interval=None,
            batch_records=32,
        )
        self.addCleanup(history.close)
        history.wait_ready(timeout=2.0)
        path, arguments = core.arguments
        self.assertEqual(path, "/tmp/certified-ticks.sock")
        self.assertEqual(arguments["start_canonical_apply_sequence"], 1)
        self.assertEqual(arguments["batch_records"], 32)
        self.assertNotIn("timeout", arguments)
        identity = history.dataset_identity
        self.assertEqual(identity.catalog_digest, b"D" * 32)
        self.assertEqual(
            identity.catalog_scope,
            int(CatalogScope.DECLARED_DAILY_A_SHARE),
        )
        self.assertEqual(identity.catalog_version, 1)


class CertifiedTickHistoryReaderAdditionalTests(unittest.TestCase):
    def _connect(self, library, **kwargs):
        return CertifiedTickHistoryReader.connect(
            "/tmp/certified-ticks.sock",
            run_id=_RUN_ID,
            session_epoch=7,
            trade_date=_TRADE_DATE,
            history_coverage=_coverage(),
            batch_records=4,
            native_library=library,
            **kwargs,
        )

    def test_whole_batch_validation_preserves_cursor_on_dense_gap(self):
        library = _FakeLibrary()
        library.corrupt_sequence = 2
        reader = self._connect(library)
        with self.assertRaises(WireFormatError):
            reader.read_batch()
        self.assertEqual(reader.next_canonical_apply_sequence, 1)
        reader.close()

    def test_capacity_plus_one_uses_native_full_tail_lifecycle(self):
        for state, failure in (
            (
                CertifiedTickHistoryState.ACTIVE,
                CertifiedTickHistoryFailure.NONE,
            ),
            (
                CertifiedTickHistoryState.COMPLETE,
                CertifiedTickHistoryFailure.NONE,
            ),
            (
                CertifiedTickHistoryState.FAILED,
                CertifiedTickHistoryFailure.TICK_CAPACITY,
            ),
        ):
            with self.subTest(state=state):
                library = _FakeLibrary()
                library.published = 8
                library.state = state
                library.failure = failure
                reader = self._connect(
                    library, start_canonical_apply_sequence=9
                )
                if state is CertifiedTickHistoryState.FAILED:
                    with self.assertRaises(
                        CertifiedTickHistoryProducerFailedError
                    ):
                        reader.read_batch()
                else:
                    batch = reader.read_batch()
                    self.assertEqual(len(batch), 0)
                    self.assertIs(batch.status.state, state)
                    self.assertEqual(
                        batch.next_canonical_apply_sequence, 9
                    )
                    self.assertEqual(
                        batch.tail_idle,
                        state is CertifiedTickHistoryState.ACTIVE,
                    )
                    self.assertEqual(
                        batch.end_of_stream,
                        state is CertifiedTickHistoryState.COMPLETE,
                    )
                self.assertEqual(library.read_calls, 1)
                self.assertEqual(
                    reader.next_canonical_apply_sequence, 9
                )
                reader.close()

    def test_capacity_plus_one_before_full_frontier_is_native_out_of_range(self):
        library = _FakeLibrary()
        reader = self._connect(
            library, start_canonical_apply_sequence=9
        )
        with self.assertRaises(CertifiedTickHistoryCapacityError):
            reader.read_batch()
        self.assertEqual(library.read_calls, 1)
        self.assertEqual(reader.next_canonical_apply_sequence, 9)
        reader.close()

    def test_attach_rejects_cursor_beyond_capacity_plus_one(self):
        library = _FakeLibrary()
        with self.assertRaisesRegex(WireFormatError, "session or cursor"):
            self._connect(
                library, start_canonical_apply_sequence=10
            )
        self.assertTrue(library.closed)


class CertifiedTickHistoryClientTests(unittest.TestCase):
    def _connect(self, library, **kwargs):
        return CertifiedTickHistoryReader.connect(
            "/tmp/certified-ticks.sock",
            run_id=_RUN_ID,
            session_epoch=7,
            trade_date=_TRADE_DATE,
            history_coverage=_coverage(),
            batch_records=4,
            native_library=library,
            **kwargs,
        )

    @staticmethod
    def _client():
        session = SimpleNamespace(
            identity=SessionIdentity(_RUN_ID, 7),
            run_id=_RUN_ID,
            session_epoch=7,
            trade_date=_TRADE_DATE,
            coverage_from_open=True,
            certified_prefix_valid=True,
            coverage_lost=False,
            server_state=ServerState.ACTIVE,
            heartbeat_monotonic_ns=1,
        )

        class Native:
            def __init__(self):
                self._library = object()
                self.current_session = session
                self.closed = False

            def session(self):
                return self.current_session

            @staticmethod
            def history_coverage():
                return SimpleNamespace(
                    session_epoch=7,
                    coverage_kind=TemporalCoverageKind.FROM_OPEN,
                    coverage_start_unix_ns=None,
                )

            def close(self):
                self.closed = True

        native = Native()
        return L2FlowClient(native, stale_after_ns=None), native

    def test_public_client_forwards_verified_session_coverage_and_cursor(self):
        client, native = self._client()
        returned = SimpleNamespace(close=lambda: None)
        calls = []

        def open_reader(path, **kwargs):
            calls.append((path, kwargs))
            return returned

        with patch(
            "l2flow_realtime.certified_tick_history."
            "open_certified_tick_history",
            side_effect=open_reader,
        ):
            actual = client.open_certified_tick_history(
                "/tmp/certified-ticks.sock",
                timeout=None,
                start_canonical_apply_sequence=5,
                batch_records=17,
            )
        self.assertIs(actual, returned)
        self.assertEqual(len(calls), 1)
        path, arguments = calls[0]
        self.assertEqual(path, "/tmp/certified-ticks.sock")
        self.assertEqual(
            arguments["expected_session"].identity,
            SessionIdentity(_RUN_ID, 7),
        )
        self.assertTrue(arguments["history_coverage"].coverage_from_open)
        self.assertIs(arguments["native_library"], native._library)
        self.assertIsNone(arguments["timeout"])
        self.assertEqual(arguments["start_canonical_apply_sequence"], 5)
        self.assertEqual(arguments["batch_records"], 17)
        client.close()

    def test_partial_coverage_is_rejected_before_native_open(self):
        library = _FakeLibrary()
        with self.assertRaisesRegex(UnavailableError, "from-open"):
            CertifiedTickHistoryReader.connect(
                "/tmp/certified-ticks.sock",
                run_id=_RUN_ID,
                session_epoch=7,
                trade_date=_TRADE_DATE,
                history_coverage=_coverage(
                    TemporalCoverageKind.PROCESS_START_PARTIAL
                ),
                native_library=library,
            )
        self.assertIsNone(library.timeout_ms)

    def test_forgotten_close_releases_native_reader(self):
        library = _FakeLibrary()
        reader = self._connect(library)
        self.assertFalse(library.closed)
        del reader
        gc.collect()
        self.assertTrue(library.closed)

    @unittest.skipUnless(polars_available(), "Polars is not installed")
    def test_polars_batch_has_fixed_owned_schema_and_lazy_cut(self):
        library = _FakeLibrary()
        reader = self._connect(library)
        batch = reader.read_batch()
        frame = certified_tick_batch_frame(
            batch, include_wire_payload=True
        )
        self.assertEqual(frame.height, 2)
        self.assertEqual(
            dict(frame.schema),
            certified_tick_schema(include_wire_payload=True),
        )
        self.assertEqual(
            frame["canonical_apply_sequence"].to_list(), [1, 2]
        )
        self.assertEqual(frame["instrument_id"].to_list(), [1, 1])
        self.assertEqual(frame["action"].to_list(), [3, 3])
        self.assertEqual(len(frame["wire_payload"].item(0)), 336)
        lazy = certified_tick_batch_lazyframe(batch)
        self.assertEqual(
            lazy.collect()["canonical_apply_sequence"].to_list(),
            [1, 2],
        )
        reader.close()


if __name__ == "__main__":
    unittest.main()
