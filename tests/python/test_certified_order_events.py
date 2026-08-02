import ctypes
import gc
import unittest

from l2flow_realtime.certified_order_events import (
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
from l2flow_realtime.models import Market


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
        self.published = 2
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

    def _open(self, path, expected, _timeout_ms, output, system_error):
        value = expected._obj
        if (
            path != b"/tmp/certified-events.sock"
            or bytes(value.run_id) != self.run_id
            or value.session_epoch != 7
            or value.trade_date != 20260730
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
        value.event_capacity = 8
        value.trade_date = 20260730
        value.coverage_flags = 3
        return 0

    @staticmethod
    def _fill_status(value, published):
        value.status_schema_version = 1
        value.status_bytes = ctypes.sizeof(_StatusC)
        value.coverage_flags = 3
        value.certified_state = 3
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
        event.record_schema_version = 1
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
        return 0


class CertifiedOrderEventReaderTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
