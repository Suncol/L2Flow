import ctypes
import gc
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from l2flow_realtime.client import L2FlowClient
from l2flow_realtime.certified_order_events import (
    CertifiedOrderEventCapacityError,
    CertifiedOrderEventError,
    CertifiedOrderEventProducerFailedError,
    CertifiedOrderEventReader,
    CertifiedOrderEventState,
    _EnvelopeC,
    _ReadResultC,
    _SessionC,
    _StatusC,
)
from l2flow_realtime.instrument_derived_event_history import (
    InstrumentDerivedEventKind,
)
from l2flow_realtime.models import (
    Market,
    ServerState,
    SessionIdentity,
    UnavailableError,
    WireFormatError,
)


class _Function:
    def __init__(self, callback):
        self.callback = callback
        self.argtypes = None
        self.restype = None

    def __call__(self, *args):
        return self.callback(*args)


class _FakeLibrary:
    def __init__(self):
        self.run_id = bytes(range(1, 17))
        self.event_capacity = 8
        self.published = 2
        self.state = CertifiedOrderEventState.CONTIGUOUS
        self.coverage_flags = 3
        self.coverage_start_unix_ns = 0
        self.open_requirement = None
        self.closed = False
        self.l2flow_certified_order_event_reader_open_v1 = _Function(
            self._open
        )
        self.l2flow_certified_order_event_reader_close_v1 = _Function(
            self._close
        )
        self.l2flow_certified_order_event_reader_session_v1 = _Function(
            self._session
        )
        self.l2flow_certified_order_event_reader_status_v1 = _Function(
            self._status
        )
        self.l2flow_certified_order_event_reader_read_v1 = _Function(
            self._read
        )

    def _open(
        self,
        path,
        expected,
        coverage_requirement,
        _timeout_ms,
        output,
        system_error,
    ):
        value = expected._obj
        self.open_requirement = int(coverage_requirement)
        if (
            path != b"/tmp/certified-events.sock"
            or bytes(value.run_id) != self.run_id
            or value.session_epoch != 7
            or value.trade_date != 20260730
        ):
            system_error._obj.value = 22
            return 2
        temporal = self.coverage_flags & 5
        if (
            self.open_requirement == 0 and temporal != 1
        ) or (
            self.open_requirement == 1 and temporal != 4
        ):
            return 15
        output._obj.value = 0x1234
        system_error._obj.value = 0
        return 0

    def _close(self, _handle):
        self.closed = True

    def _session(self, _handle, output):
        value = output._obj
        value.run_id[:] = self.run_id
        value.session_epoch = 7
        value.event_capacity = self.event_capacity
        value.trade_date = 20260730
        value.coverage_flags = self.coverage_flags
        value.coverage_start_unix_ns = self.coverage_start_unix_ns
        return 0

    def _fill_status(self, value, published):
        value.status_schema_version = 1
        value.status_bytes = ctypes.sizeof(_StatusC)
        value.coverage_flags = self.coverage_flags
        value.certified_state = int(self.state)
        value.tick_publish_tag = 2
        value.tick_heartbeat_monotonic_ns = 100
        value.tick_canonical_apply_frontier = published
        value.correction_epoch = 1
        value.observed_native_message_count = published
        value.certified_tick_count = published
        value.gap_opened_count = 1
        value.gap_recovered_count = 1
        value.event_publish_tag = 2
        value.event_heartbeat_monotonic_ns = 101
        value.event_canonical_apply_frontier = published
        value.event_published_sequence = published
        value.event_generation = published
        value.committed_mapping_bytes = 8192
        value.coherent_canonical_apply_frontier = published
        value.channel_state_count = 1

    def _status(self, _handle, output):
        self._fill_status(output._obj, self.published)
        return 0

    @staticmethod
    def _fill_row(row: _EnvelopeC, sequence: int):
        row.canonical_apply_sequence = sequence
        event = row.event
        event.record_schema_version = 2
        event.record_bytes = ctypes.sizeof(event)
        event.derived_event_sequence = sequence
        event.trade_date = 20260730
        event.instrument_id = 11
        event.channel = 1
        event.market = 1
        event.event_kind = 2
        event.quantity = 100 + sequence
        event.native_event_sequence = sequence
        event.source_sequence = sequence
        event.ingress_sequence = sequence
        event.tick_stream_sequence = sequence
        event.reserved0 = 0
        event.reserved1[0] = 1

    def _read(
        self,
        _handle,
        expected_event_sequence,
        rows,
        capacity,
        output,
    ):
        expected = int(expected_event_sequence)
        count = min(
            int(capacity),
            max(self.published - expected + 1, 0),
        )
        for index in range(count):
            self._fill_row(rows[index], expected + index)
        result = output._obj
        result.result_schema_version = 1
        result.result_bytes = ctypes.sizeof(_ReadResultC)
        result.records_written = count
        result.next_event_sequence = expected + count
        self._fill_status(result.status, self.published)
        if count:
            return 0
        if expected > self.event_capacity:
            if (
                expected == self.event_capacity + 1
                and self.published == self.event_capacity
            ):
                if self.state is CertifiedOrderEventState.STOPPED:
                    return 6
                if self.state in (
                    CertifiedOrderEventState.FROZEN_CONFLICT,
                    CertifiedOrderEventState.FROZEN_RESOURCE,
                ):
                    return 7
                return 2
            return 3
        if self.state in (
            CertifiedOrderEventState.FROZEN_CONFLICT,
            CertifiedOrderEventState.FROZEN_RESOURCE,
        ):
            return 7
        if self.state is CertifiedOrderEventState.STOPPED:
            return 6
        return 1 if self.published == 0 else 2


class CertifiedOrderEventReaderTests(unittest.TestCase):
    @staticmethod
    def _client(*, partial: bool):
        run_id = bytes(range(1, 17))
        session = SimpleNamespace(
            identity=SessionIdentity(run_id, 7),
            run_id=run_id,
            session_epoch=7,
            trade_date=20260730,
            coverage_from_open=not partial,
            certified_prefix_valid=not partial,
            coverage_lost=False,
            server_state=(
                ServerState.LIVE_PARTIAL
                if partial
                else ServerState.ACTIVE
            ),
            heartbeat_monotonic_ns=1,
            catalog_digest=b"C" * 32,
            catalog_scope=1,
            catalog_version=9,
        )

        class Native:
            def __init__(self):
                self._library = object()
                self.current_session = session

            def session(self):
                return self.current_session

            def close(self):
                pass

        native = Native()
        return L2FlowClient(native, stale_after_ns=None), native

    def test_recovered_history_cursor_becomes_live_tail(self):
        library = _FakeLibrary()
        reader = CertifiedOrderEventReader.connect(
            "/tmp/certified-events.sock",
            run_id=library.run_id,
            session_epoch=7,
            trade_date=20260730,
            batch_records=4,
            native_library=library,
        )

        self.assertTrue(reader.session.coverage_from_open)
        self.assertTrue(reader.session.startup_prefix_recovered)
        self.assertEqual(reader.session.coverage_start_unix_ns, 0)
        self.assertEqual(library.open_requirement, 0)
        self.assertTrue(
            reader.session.history_coverage.coverage_from_open
        )
        coverage = reader.history_coverage
        status = reader.status()
        self.assertEqual(status.state, CertifiedOrderEventState.CONTIGUOUS)
        self.assertEqual(status.tick_canonical_apply_frontier, 2)
        self.assertEqual(status.event_canonical_apply_frontier, 2)
        self.assertEqual(status.coherent_canonical_apply_frontier, 2)
        self.assertEqual(status.gap_opened_count, 1)
        self.assertEqual(status.gap_recovered_count, 1)

        history = reader.read_batch()
        self.assertEqual(len(history), 2)
        self.assertEqual(len(history.buffer), 2 * 328)
        self.assertTrue(history.buffer.readonly)
        self.assertEqual(history.next_event_sequence, 3)
        self.assertEqual(
            [row.canonical_apply_sequence for row in history],
            [1, 2],
        )
        self.assertEqual(history.row(0).event.market, Market.SHANGHAI)
        self.assertEqual(history.row(0).source_tick_event_ordinal, 0)
        self.assertEqual(
            history.row(0).event_uid.session_identity,
            SessionIdentity(library.run_id, 7),
        )
        self.assertEqual(
            history.row(0).event_uid.instrument_id, 11
        )
        self.assertEqual(
            history.row(0).event_uid.tick_stream_sequence, 1
        )
        self.assertTrue(history.history_coverage.coverage_from_open)
        self.assertIs(history.history_coverage, coverage)
        self.assertEqual(
            history.row(0).event.event_kind,
            InstrumentDerivedEventKind.TRADE,
        )

        idle = reader.read_batch()
        self.assertEqual(len(idle), 0)
        self.assertEqual(idle.next_event_sequence, 3)
        self.assertEqual(reader.next_event_sequence, 3)

        library.published = 3
        tail = reader.read_batch()
        self.assertEqual(len(tail), 1)
        self.assertEqual(tail.row(0).canonical_apply_sequence, 3)
        self.assertEqual(tail.row(0).event.native_event_sequence, 3)
        self.assertEqual(tail.status.coherent_canonical_apply_frontier, 3)
        self.assertEqual(reader.next_event_sequence, 4)

        reader.close()
        reader.close()
        self.assertTrue(library.closed)

    def test_process_start_coverage_requires_explicit_opt_in(self):
        library = _FakeLibrary()
        library.coverage_flags = 4
        library.coverage_start_unix_ns = 1_722_750_365_000_000_000

        with self.assertRaises(CertifiedOrderEventError):
            CertifiedOrderEventReader.connect(
                "/tmp/certified-events.sock",
                run_id=library.run_id,
                session_epoch=7,
                trade_date=20260730,
                native_library=library,
            )

        library.closed = False
        reader = CertifiedOrderEventReader.connect(
            "/tmp/certified-events.sock",
            run_id=library.run_id,
            session_epoch=7,
            trade_date=20260730,
            coverage_requirement="process_start_partial",
            native_library=library,
        )
        self.addCleanup(reader.close)
        self.assertEqual(library.open_requirement, 1)
        self.assertFalse(reader.session.coverage_from_open)
        self.assertTrue(reader.session.process_start_partial)
        self.assertFalse(reader.session.startup_prefix_recovered)
        self.assertEqual(
            reader.history_coverage.coverage_start_unix_ns,
            library.coverage_start_unix_ns,
        )

    def test_any_explicit_accepts_both_temporal_coverages(self):
        library = _FakeLibrary()
        reader = CertifiedOrderEventReader.connect(
            "/tmp/certified-events.sock",
            run_id=library.run_id,
            session_epoch=7,
            trade_date=20260730,
            coverage_requirement="any_explicit",
            native_library=library,
        )
        self.addCleanup(reader.close)
        self.assertEqual(library.open_requirement, 2)

    def test_unknown_coverage_requirement_is_rejected_before_open(self):
        library = _FakeLibrary()
        with self.assertRaises(ValueError):
            CertifiedOrderEventReader.connect(
                "/tmp/certified-events.sock",
                run_id=library.run_id,
                session_epoch=7,
                trade_date=20260730,
                coverage_requirement="partial",
                native_library=library,
            )
        self.assertIsNone(library.open_requirement)

    def test_public_client_forwards_partial_requirement_and_catalog_cut(self):
        client, native = self._client(partial=True)
        returned = SimpleNamespace(close=lambda: None)
        calls = []

        def open_reader(path, **kwargs):
            calls.append((path, kwargs))
            return returned

        with patch(
            "l2flow_realtime.certified_order_events."
            "open_certified_order_events",
            side_effect=open_reader,
        ):
            actual = client.open_certified_order_events(
                "/tmp/certified-events.sock",
                coverage_requirement="process_start_partial",
                timeout=None,
                start_event_sequence=3,
                batch_records=17,
            )
        self.assertIs(actual, returned)
        self.assertEqual(len(calls), 1)
        path, arguments = calls[0]
        self.assertEqual(path, "/tmp/certified-events.sock")
        self.assertEqual(
            arguments["coverage_requirement"],
            "process_start_partial",
        )
        self.assertIs(arguments["native_library"], native._library)
        self.assertIsNone(arguments["timeout"])
        self.assertEqual(arguments["start_event_sequence"], 3)
        self.assertEqual(arguments["batch_records"], 17)
        client.close()

    def test_public_client_default_rejects_partial_fast_session(self):
        client, _native = self._client(partial=True)
        self.addCleanup(client.close)
        with self.assertRaisesRegex(UnavailableError, "from-open"):
            client.open_certified_order_events(
                "/tmp/certified-events.sock"
            )

    def test_forgotten_close_releases_native_reader(self):
        library = _FakeLibrary()
        reader = CertifiedOrderEventReader.connect(
            "/tmp/certified-events.sock",
            run_id=library.run_id,
            session_epoch=7,
            trade_date=20260730,
            native_library=library,
        )
        self.assertFalse(library.closed)
        del reader
        gc.collect()
        self.assertTrue(library.closed)

    def test_exactly_full_natural_tail_preserves_lifecycle(self):
        cases = (
            (CertifiedOrderEventState.CONTIGUOUS, "idle"),
            (CertifiedOrderEventState.STOPPED, "complete"),
            (CertifiedOrderEventState.FROZEN_RESOURCE, "failed"),
        )
        for state, expected in cases:
            with self.subTest(state=state):
                library = _FakeLibrary()
                library.event_capacity = 2
                library.published = 2
                library.state = state
                reader = CertifiedOrderEventReader.connect(
                    "/tmp/certified-events.sock",
                    run_id=library.run_id,
                    session_epoch=7,
                    trade_date=20260730,
                    start_event_sequence=3,
                    native_library=library,
                )
                self.addCleanup(reader.close)
                self.assertEqual(reader.next_event_sequence, 3)
                if expected == "failed":
                    with self.assertRaises(
                        CertifiedOrderEventProducerFailedError
                    ) as cm:
                        reader.read_batch()
                    self.assertIs(cm.exception.status.state, state)
                    self.assertEqual(reader.next_event_sequence, 3)
                    continue
                batch = reader.read_batch()
                self.assertEqual(len(batch), 0)
                self.assertEqual(batch.next_event_sequence, 3)
                self.assertEqual(reader.next_event_sequence, 3)
                self.assertIs(batch.status.state, state)
                self.assertEqual(batch.tail_idle, expected == "idle")
                self.assertEqual(
                    batch.end_of_stream, expected == "complete"
                )

    def test_attach_rejects_only_beyond_exact_natural_tail(self):
        library = _FakeLibrary()
        library.event_capacity = 2
        library.published = 2
        with self.assertRaises(WireFormatError):
            CertifiedOrderEventReader.connect(
                "/tmp/certified-events.sock",
                run_id=library.run_id,
                session_epoch=7,
                trade_date=20260730,
                start_event_sequence=4,
                native_library=library,
            )
        self.assertTrue(library.closed)

        # The native layer, not attach validation, decides whether capacity+1
        # is a genuine full-journal tail or an invalid future seek.
        sparse = _FakeLibrary()
        sparse.event_capacity = 2
        sparse.published = 1
        reader = CertifiedOrderEventReader.connect(
            "/tmp/certified-events.sock",
            run_id=sparse.run_id,
            session_epoch=7,
            trade_date=20260730,
            start_event_sequence=3,
            native_library=sparse,
        )
        self.addCleanup(reader.close)
        with self.assertRaises(CertifiedOrderEventCapacityError):
            reader.read_batch()


if __name__ == "__main__":
    unittest.main()
