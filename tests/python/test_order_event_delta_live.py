import ctypes
import os
import unittest

from l2flow_realtime.models import (
    HistoryCoverageInfo,
    SessionIdentity,
    TemporalCoverageKind,
)
from l2flow_realtime.order_event_delta_live import (
    LiveOrderEventDeltaOverrunError,
    LiveOrderEventDeltaProducerState,
    LiveOrderEventDeltaReader,
    LiveOrderEventDeltaSession,
    LiveOrderEventDeltaTemporalCoverage,
    LiveOrderEventDeltaUnavailableError,
    LiveOrderEventDeltaWireError,
    _DerivedEventRowC,
    _LiveReadResultC,
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
        self.cursor = 1
        self.opened_start_event_sequence = None
        self.closed = False
        self.read_calls = 0
        self.mode = "normal"
        self.l2flow_order_event_delta_reader_open_v1 = _Function(
            self._open
        )
        self.l2flow_order_event_delta_reader_open_at_v1 = _Function(
            self._open_at
        )
        self.l2flow_order_event_delta_reader_close_v1 = _Function(
            self._close
        )
        self.l2flow_order_event_delta_reader_read_v1 = _Function(
            self._read
        )
        self.l2flow_order_event_delta_reader_session_v1 = _Function(
            self._session
        )
        self.l2flow_order_event_delta_reader_state_v1 = _Function(
            self._state
        )
        self.l2flow_order_event_delta_error_name_v1 = _Function(
            self._error_name
        )

    def _open(self, _fd, session, output, system_error):
        self.opened_start_event_sequence = 1
        native_session = session._obj
        if native_session.session_epoch != 7:
            return 7
        output._obj.value = 0x1234
        system_error._obj.value = 0
        return 0

    def _open_at(
        self,
        fd,
        session,
        start_event_sequence,
        output,
        system_error,
    ):
        self.cursor = int(start_event_sequence)
        result = self._open(fd, session, output, system_error)
        self.opened_start_event_sequence = int(start_event_sequence)
        return result

    def _close(self, _handle):
        self.closed = True

    @staticmethod
    def _fill_result(
        output,
        *,
        count,
        next_sequence,
        published=2,
        source=2,
        heartbeat=555,
        state=2,
        flags=0,
        observed=0,
    ):
        result = output._obj
        result.result_schema_version = 1
        result.result_bytes = ctypes.sizeof(_LiveReadResultC)
        result.records_written = count
        result.next_sequence = next_sequence
        result.observed_sequence = observed
        result.published_event_sequence = published
        result.consumed_source_tick_sequence = source
        result.heartbeat_monotonic_ns = heartbeat
        result.producer_state = state
        result.header_flags = flags

    @staticmethod
    def _fill_row(row, sequence):
        row.record_schema_version = 2
        row.record_bytes = ctypes.sizeof(_DerivedEventRowC)
        row.derived_event_sequence = sequence
        row.trade_date = 20260730
        row.instrument_id = 1
        row.channel = 9
        row.market = 1
        row.event_kind = 1
        row.order_id = 10_000 + sequence
        row.revision = 1
        row.tick_stream_sequence = 2
        row.reserved0 = sequence - 1
        row.reserved1[0] = 1

    def _read(self, _handle, rows, capacity, output):
        self.read_calls += 1
        if self.mode == "overrun":
            self._fill_result(
                output,
                count=0,
                next_sequence=1,
                published=3,
                source=3,
                observed=2,
            )
            return 10
        if self.mode == "failed":
            self._fill_result(
                output,
                count=0,
                next_sequence=self.cursor,
                state=5,
                flags=1,
            )
            return 8
        if self.mode == "bad_next":
            self._fill_result(
                output,
                count=1,
                next_sequence=self.cursor + 2,
            )
            if capacity:
                self._fill_row(rows[0], self.cursor)
            return 0

        available = 3 - self.cursor
        count = min(int(capacity), max(available, 0))
        for index in range(count):
            self._fill_row(rows[index], self.cursor + index)
        self.cursor += count
        self._fill_result(
            output,
            count=count,
            next_sequence=self.cursor,
        )
        return 0

    @staticmethod
    def _session(_handle, _output):
        return 0

    def _state(self, _handle, output):
        output._obj.value = 5 if self.mode == "failed" else 2
        return 0

    @staticmethod
    def _error_name(code):
        return {
            0: b"none",
            8: b"unavailable",
            9: b"inconsistent_read",
            10: b"overrun",
        }.get(int(code), b"error")


def _session():
    return LiveOrderEventDeltaSession(
        run_id=bytes(range(1, 17)),
        session_epoch=7,
        trade_date=20260730,
        ring_capacity=8,
        total_mapping_bytes=8192,
    )


def _source_coverage():
    return HistoryCoverageInfo(
        run_id=b"S" * 16,
        session_epoch=70,
        trade_date=20260730,
        coverage_kind=TemporalCoverageKind.FROM_OPEN,
    )


class LiveOrderEventDeltaTests(unittest.TestCase):
    def test_poll_batches_buffer_and_retained_fd(self):
        library = _FakeLibrary()
        read_fd, write_fd = os.pipe()
        try:
            reader = LiveOrderEventDeltaReader.open(
                library,
                read_fd,
                _session(),
                batch_records=1,
                history_coverage=_source_coverage(),
            )
            # Native open duplicates in production; default Python ownership
            # explicitly leaves the supplied descriptor with the caller.
            os.fstat(read_fd)
            self.assertTrue(
                reader.session.history_coverage.coverage_from_open
            )
            coverage = reader.history_coverage

            metadata = reader.poll()
            self.assertEqual(metadata.records_written, 0)
            self.assertEqual(metadata.next_sequence, 1)
            self.assertEqual(metadata.published_event_sequence, 2)
            self.assertEqual(
                metadata.producer_state,
                LiveOrderEventDeltaProducerState.ACTIVE,
            )

            first = reader.read_batch()
            self.assertEqual(len(first), 1)
            self.assertEqual(len(first.buffer), 320)
            self.assertTrue(first.buffer.readonly)
            self.assertFalse(first.drains_published_prefix)
            self.assertEqual(first.row(0).derived_event_sequence, 1)
            self.assertEqual(first.row(0).order_id, 10_001)
            self.assertEqual(
                first.event(0).source_tick_event_ordinal, 0
            )
            self.assertEqual(first.source_tick_event_ordinal(0), 0)
            self.assertEqual(
                first.event_uid(0).session_identity,
                SessionIdentity(b"S" * 16, 70),
            )
            self.assertNotEqual(
                first.event_uid(0).session_identity,
                reader.session.identity,
            )
            self.assertTrue(first.history_coverage.coverage_from_open)
            self.assertIs(first.history_coverage, coverage)
            self.assertEqual(reader.next_sequence, 2)

            batches = list(
                reader.read_available(maximum_batches=2)
            )
            self.assertEqual([len(batch) for batch in batches], [1])
            self.assertTrue(batches[0].drains_published_prefix)
            self.assertEqual(batches[0].row(0).derived_event_sequence, 2)
            self.assertEqual(
                batches[0].source_tick_event_ordinal(0), 1
            )
            self.assertNotEqual(
                first.event_uid(0), batches[0].event_uid(0)
            )
            self.assertEqual(reader.next_sequence, 3)
            self.assertEqual(
                reader.producer_state(),
                LiveOrderEventDeltaProducerState.ACTIVE,
            )
            reader.close()
            reader.close()
            self.assertTrue(library.closed)
        finally:
            os.close(read_fd)
            os.close(write_fd)

    def test_take_fd_ownership_closes_caller_descriptor(self):
        library = _FakeLibrary()
        read_fd, write_fd = os.pipe()
        reader = LiveOrderEventDeltaReader.open(
            library,
            read_fd,
            _session(),
            take_fd_ownership=True,
        )
        with self.assertRaises(OSError):
            os.fstat(read_fd)
        reader.close()
        os.close(write_fd)

    def test_unscoped_direct_fd_exposes_ordinal_without_inventing_uid(self):
        library = _FakeLibrary()
        read_fd, write_fd = os.pipe()
        try:
            reader = LiveOrderEventDeltaReader.open(
                library, read_fd, _session(), batch_records=1
            )
            batch = reader.read_batch()
            self.assertEqual(batch.source_tick_event_ordinal(0), 0)
            self.assertIsNone(batch.event_uid(0))
            self.assertIsNone(reader.source_session_identity)
            reader.close()
        finally:
            os.close(read_fd)
            os.close(write_fd)

    def test_explicit_positive_start_is_fixed_at_native_open(self):
        library = _FakeLibrary()
        read_fd, write_fd = os.pipe()
        try:
            reader = LiveOrderEventDeltaReader.open(
                library,
                read_fd,
                _session(),
                start_event_sequence=2,
            )
            self.assertEqual(library.opened_start_event_sequence, 2)
            self.assertEqual(reader.next_sequence, 2)
            polled = reader.poll()
            self.assertEqual(polled.next_sequence, 2)
            batch = reader.read_batch()
            self.assertEqual(len(batch), 1)
            self.assertEqual(
                batch.row(0).derived_event_sequence, 2
            )
            self.assertEqual(reader.next_sequence, 3)
            reader.close()
        finally:
            os.close(read_fd)
            os.close(write_fd)

    def test_overrun_latches_python_reader(self):
        library = _FakeLibrary()
        library.mode = "overrun"
        read_fd, write_fd = os.pipe()
        try:
            reader = LiveOrderEventDeltaReader.open(
                library, read_fd, _session()
            )
            with self.assertRaises(
                LiveOrderEventDeltaOverrunError
            ) as caught:
                reader.read_batch()
            self.assertEqual(
                caught.exception.metadata.observed_sequence, 2
            )
            self.assertTrue(reader.failed)
            calls = library.read_calls
            with self.assertRaises(
                LiveOrderEventDeltaUnavailableError
            ):
                reader.poll()
            self.assertEqual(library.read_calls, calls)
            reader.close()
        finally:
            os.close(read_fd)
            os.close(write_fd)

    def test_failed_producer_and_bad_success_fail_closed(self):
        for mode, error_type in (
            ("failed", LiveOrderEventDeltaUnavailableError),
            ("bad_next", LiveOrderEventDeltaWireError),
        ):
            with self.subTest(mode=mode):
                library = _FakeLibrary()
                library.mode = mode
                read_fd, write_fd = os.pipe()
                try:
                    reader = LiveOrderEventDeltaReader.open(
                        library, read_fd, _session()
                    )
                    with self.assertRaises(error_type):
                        reader.read_batch()
                    self.assertTrue(reader.failed)
                    reader.close()
                finally:
                    os.close(read_fd)
                    os.close(write_fd)

    def test_configuration_validation(self):
        partial = LiveOrderEventDeltaSession(
            run_id=bytes(range(1, 17)),
            session_epoch=1,
            trade_date=20260730,
            ring_capacity=8,
            total_mapping_bytes=8192,
            temporal_coverage=(
                LiveOrderEventDeltaTemporalCoverage.FROM_PROCESS_START
            ),
        )
        self.assertTrue(partial.history_coverage.process_start_partial)
        self.assertIsNone(
            partial.history_coverage.coverage_start_unix_ns
        )
        with self.assertRaises(ValueError):
            LiveOrderEventDeltaSession(
                run_id=b"\0" * 16,
                session_epoch=1,
                trade_date=20260730,
                ring_capacity=1,
                total_mapping_bytes=4096,
            )
        with self.assertRaises(ValueError):
            LiveOrderEventDeltaSession(
                run_id=bytes(range(1, 17)),
                session_epoch=1,
                trade_date=20260730,
                ring_capacity=8,
                total_mapping_bytes=4096,
            )
        library = _FakeLibrary()
        with self.assertRaises(ValueError):
            LiveOrderEventDeltaReader.open(
                library, -1, _session()
            )
        for invalid_start in (0, -1, True, 1 << 64):
            with self.subTest(start_event_sequence=invalid_start):
                with self.assertRaises(ValueError):
                    LiveOrderEventDeltaReader.open(
                        library,
                        0,
                        _session(),
                        start_event_sequence=invalid_start,
                    )


if __name__ == "__main__":
    unittest.main()
