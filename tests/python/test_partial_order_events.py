import ctypes
import gc
import unittest

from l2flow_realtime.models import (
    Market,
    TemporalCoverageKind,
    WireFormatError,
)
from l2flow_realtime.partial_order_events import (
    PartialOrderEventBrokerState,
    PartialOrderEventChannelFlag,
    PartialOrderEventCheckpoint,
    PartialOrderEventFullReplacementRequired,
    PartialOrderEventOrderKey,
    PartialOrderEventOrderingQuality,
    PartialOrderEventReader,
    PartialOrderEventServiceState,
    PartialOrderEventTemporalCoverage,
    _ChannelBatchResultC,
    _ChannelHealthC,
    _OrderStateBatchResultC,
    _ReadBatchResultC,
    _SessionC,
    _StatusC,
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
        self.publication_generation = 5
        self.correction_epoch = 2
        self.ordering_quality = 1
        self.published = 2
        self.broker_state = PartialOrderEventBrokerState.READY
        self.broker_stale = False
        self.service_state = PartialOrderEventServiceState.REORDERING
        self.service_stale = True
        self.open_full_replacement_required = False
        self.runtime_full_replacement_required = False
        self.closed = False
        for name, callback in (
            ("open", self._open),
            ("close", self._close),
            ("session", self._session),
            ("status", self._status),
            ("read", self._read),
            ("affected_channels", self._affected_channels),
            ("find_order_state", self._find_order_state),
            ("order_states", self._order_states),
        ):
            setattr(
                self,
                f"l2flow_partial_order_event_reader_{name}_v2",
                _Function(callback),
            )

    def _open(
        self,
        path,
        expected,
        checkpoint,
        _timeout_ms,
        output,
        system_error,
    ):
        value = expected._obj
        if (
            path != b"/tmp/partial-events.sock"
            or bytes(value.run_id) != self.run_id
            or value.session_epoch != 7
            or value.trade_date != 20260803
        ):
            system_error._obj.value = 22
            return 2
        if self.open_full_replacement_required:
            return 17
        if checkpoint is not None:
            saved = checkpoint._obj
            if (
                saved.publication_generation
                != self.publication_generation
                or saved.correction_epoch != self.correction_epoch
            ):
                return 17
        output._obj.value = 0x1234
        system_error._obj.value = 0
        return 0

    def _close(self, _handle):
        self.closed = True

    def _session(self, _handle, output):
        if self.runtime_full_replacement_required:
            return 7
        value = output._obj
        value.run_id[:] = self.run_id
        value.session_epoch = 7
        value.publication_generation = self.publication_generation
        value.correction_epoch = self.correction_epoch
        value.coverage_start_unix_ns = 1_785_700_800_000_000_000
        value.event_capacity = 8
        value.order_state_capacity = 16
        value.total_mapping_bytes = 65536
        value.trade_date = 20260803
        value.temporal_coverage = 1
        value.ordering_quality = self.ordering_quality
        value.affected_channel_capacity = 4
        value.broker_state = int(self.broker_state)
        value.broker_stale = int(self.broker_stale)
        return 0

    def _fill_status(self, value):
        value.status_schema_version = 1
        value.status_bytes = ctypes.sizeof(_StatusC)
        value.run_id[:] = self.run_id
        value.session_epoch = 7
        value.publication_generation = self.publication_generation
        value.correction_epoch = self.correction_epoch
        value.coverage_start_unix_ns = 1_785_700_800_000_000_000
        value.event_capacity = 8
        value.order_state_capacity = 16
        value.commit_sequence = 3
        value.heartbeat_monotonic_ns = 100
        value.captured_source_frontier = 4
        value.canonical_apply_frontier = self.published
        value.event_published_frontier = self.published
        value.history_generation = 1
        value.order_state_generation = self.publication_generation
        value.order_state_canonical_frontier = self.published
        value.committed_event_region_bytes = self.published * 384
        value.shanghai_order_state_count = 1
        value.pending_count = 1
        value.reorder_high_water = 2
        value.oldest_gap_age_ns = 50
        value.trade_date = 20260803
        value.temporal_coverage = 1
        value.ordering_quality = self.ordering_quality
        value.service_state = int(self.service_state)
        value.stale = int(self.service_stale)
        value.affected_channel_count = 1
        value.channel_health_count = 1
        value.affected_channel_capacity = 4
        value.broker_state = int(self.broker_state)
        value.broker_stale = int(self.broker_stale)

    def _status(self, _handle, output):
        if self.runtime_full_replacement_required:
            return 7
        self._fill_status(output._obj)
        return 0

    @staticmethod
    def _fill_event(row, sequence, *, order_revision=False):
        row.canonical_apply_sequence = sequence
        event = (
            row.order_revision if order_revision else row.event
        )
        event.record_schema_version = 2
        event.record_bytes = ctypes.sizeof(event)
        event.derived_event_sequence = sequence
        event.trade_date = 20260803
        event.instrument_id = 11
        event.channel = 7
        event.order_id = 99 if order_revision else 0
        event.market = int(Market.SHANGHAI)
        event.event_kind = 1 if order_revision else 2
        event.operation = 0
        event.quantity = 100 + sequence
        event.remaining_quantity = 80
        event.remaining_quantity_valid = int(order_revision)
        event.native_event_sequence = sequence
        event.source_sequence = sequence
        event.ingress_sequence = sequence
        event.tick_stream_sequence = sequence
        event.reserved0 = 0
        event.reserved1[0] = 1

    def _copy_checkpoint(
        self, target, source, next_sequence, next_state_slot=None
    ):
        target.run_id[:] = self.run_id
        target.session_epoch = 7
        target.publication_generation = self.publication_generation
        target.correction_epoch = self.correction_epoch
        target.next_event_sequence = next_sequence
        target.next_order_state_physical_slot = (
            source.next_order_state_physical_slot
            if next_state_slot is None
            else next_state_slot
        )
        target.trade_date = 20260803
        self.assert_checkpoint_source(source)

    def assert_checkpoint_source(self, source):
        assert bytes(source.run_id) == self.run_id
        assert source.publication_generation == self.publication_generation
        assert source.correction_epoch == self.correction_epoch

    def _read(self, _handle, checkpoint, rows, capacity, output):
        if self.runtime_full_replacement_required:
            return 7
        saved = checkpoint._obj
        self.assert_checkpoint_source(saved)
        count = min(
            int(capacity),
            max(self.published - saved.next_event_sequence + 1, 0),
        )
        for index in range(count):
            self._fill_event(rows[index], saved.next_event_sequence + index)
        result = output._obj
        result.result_schema_version = 1
        result.result_bytes = ctypes.sizeof(_ReadBatchResultC)
        result.records_written = count
        self._copy_checkpoint(
            result.checkpoint,
            saved,
            saved.next_event_sequence + count,
        )
        self._fill_status(result.status)
        return 0 if count else 3

    def _affected_channels(self, _handle, rows, capacity, output):
        if self.runtime_full_replacement_required:
            return 7
        result = output._obj
        result.result_schema_version = 1
        result.result_bytes = ctypes.sizeof(_ChannelBatchResultC)
        result.required_capacity = 1
        self._fill_status(result.status)
        if int(capacity) < 1:
            return 4
        row: _ChannelHealthC = rows[0]
        row.commit_sequence = 3
        row.market = int(Market.SHENZHEN)
        row.channel = 0
        row.expected_native_sequence = 101
        row.contiguous_native_sequence = 100
        row.highest_observed_native_sequence = 102
        row.oldest_missing_native_sequence = 101
        row.pending_count = 1
        row.oldest_gap_age_ns = 50
        row.flags = int(
            PartialOrderEventChannelFlag.ORIGIN_ESTABLISHED
            | PartialOrderEventChannelFlag.AFFECTED
            | PartialOrderEventChannelFlag.STALE
        )
        row.service_state = int(PartialOrderEventServiceState.REORDERING)
        result.records_written = 1
        return 0

    def _find_order_state(self, _handle, key, output, status):
        if self.runtime_full_replacement_required:
            return 7
        value = key._obj
        if (
            value.market != int(Market.SHANGHAI)
            or value.instrument_id != 11
            or value.channel != 7
            or value.order_id != 99
        ):
            return 5
        self._fill_event(output._obj, 2, order_revision=True)
        self._fill_status(status._obj)
        return 0

    def _order_states(self, _handle, checkpoint, rows, _capacity, output):
        if self.runtime_full_replacement_required:
            return 7
        saved = checkpoint._obj
        self.assert_checkpoint_source(saved)
        first = saved.next_order_state_physical_slot
        result = output._obj
        result.result_schema_version = 1
        result.result_bytes = ctypes.sizeof(_OrderStateBatchResultC)
        self._fill_status(result.status)
        if first == 16:
            self._copy_checkpoint(
                result.checkpoint, saved, saved.next_event_sequence, 16
            )
            return 0
        self._fill_event(rows[0], 2, order_revision=True)
        result.records_written = 1
        self._copy_checkpoint(
            result.checkpoint, saved, saved.next_event_sequence, 16
        )
        return 0


class PartialOrderEventReaderTests(unittest.TestCase):
    def _connect(self, library, **kwargs):
        return PartialOrderEventReader.connect(
            "/tmp/partial-events.sock",
            run_id=library.run_id,
            session_epoch=7,
            trade_date=20260803,
            native_library=library,
            batch_records=4,
            **kwargs,
        )

    def test_process_start_history_status_and_cursor(self):
        library = _FakeLibrary()
        reader = self._connect(library)
        self.addCleanup(reader.close)

        self.assertIs(
            reader.session.temporal_coverage,
            PartialOrderEventTemporalCoverage.PROCESS_START,
        )
        self.assertIs(
            reader.session.ordering_quality,
            PartialOrderEventOrderingQuality.BOUNDED_REORDERED_PARTIAL,
        )
        self.assertFalse(reader.session.native_completeness_proven)
        self.assertFalse(
            hasattr(
                PartialOrderEventOrderingQuality,
                "NATIVE_ORDER_PROVEN",
            )
        )
        self.assertTrue(reader.history_coverage.process_start_partial)
        self.assertFalse(reader.history_coverage.coverage_from_open)
        self.assertIs(
            reader.history_coverage.coverage_kind,
            TemporalCoverageKind.PROCESS_START_PARTIAL,
        )

        status = reader.status()
        self.assertIs(status.state, PartialOrderEventServiceState.REORDERING)
        self.assertEqual(status.pending_count, 1)
        batch = reader.read_batch()
        self.assertEqual(len(batch), 2)
        self.assertEqual(len(batch.buffer), 2 * 328)
        self.assertTrue(batch.buffer.readonly)
        self.assertEqual(
            [row.canonical_apply_sequence for row in batch], [1, 2]
        )
        self.assertEqual(batch.row(0).event.market, Market.SHANGHAI)
        self.assertEqual(batch.checkpoint.next_event_sequence, 3)
        self.assertEqual(batch.checkpoint.publication_generation, 5)
        self.assertEqual(batch.checkpoint.correction_epoch, 2)
        self.assertEqual(reader.checkpoint, batch.checkpoint)

        idle = reader.read_batch()
        self.assertEqual(len(idle), 0)
        self.assertEqual(idle.checkpoint.next_event_sequence, 3)

    def test_channel_health_and_order_state_views(self):
        library = _FakeLibrary()
        reader = self._connect(library)
        self.addCleanup(reader.close)

        channels = reader.affected_channels()
        self.assertEqual(len(channels), 1)
        self.assertEqual(channels.rows[0].market, Market.SHENZHEN)
        self.assertEqual(channels.rows[0].channel, 0)
        self.assertTrue(channels.rows[0].affected)
        self.assertEqual(channels.rows[0].oldest_missing_native_sequence, 101)

        key = PartialOrderEventOrderKey(Market.SHANGHAI, 11, 7, 99)
        state = reader.find_order_state(key)
        self.assertIsNotNone(state)
        self.assertEqual(state.canonical_apply_sequence, 2)
        self.assertEqual(state.order_revision.remaining_quantity, 80)
        self.assertIsNone(
            reader.find_order_state(
                PartialOrderEventOrderKey(Market.SHANGHAI, 12, 7, 99)
            )
        )

        page = reader.read_order_states(capacity=4)
        self.assertEqual(len(page), 1)
        self.assertEqual(page.next_physical_slot, 16)
        self.assertEqual(page.row(0).order_revision.order_id, 99)
        eof = reader.read_order_states(capacity=4)
        self.assertEqual(len(eof), 0)
        self.assertEqual(eof.next_physical_slot, 16)

    def test_generation_or_correction_change_requires_full_replacement(self):
        library = _FakeLibrary()
        reader = self._connect(library)
        state_page = reader.read_order_states(capacity=4)
        self.assertEqual(
            state_page.checkpoint.next_order_state_physical_slot, 16
        )
        checkpoint = reader.checkpoint
        reader.close()

        library.publication_generation += 1
        with self.assertRaises(PartialOrderEventFullReplacementRequired):
            self._connect(library, checkpoint=checkpoint)

        corrected_checkpoint = PartialOrderEventCheckpoint(
            run_id=library.run_id,
            session_epoch=7,
            trade_date=20260803,
            publication_generation=library.publication_generation,
            correction_epoch=library.correction_epoch,
            next_event_sequence=1,
        )
        library.correction_epoch += 1
        with self.assertRaises(PartialOrderEventFullReplacementRequired):
            self._connect(library, checkpoint=corrected_checkpoint)

    def test_open_race_without_checkpoint_requires_full_replacement(self):
        library = _FakeLibrary()
        library.open_full_replacement_required = True
        with self.assertRaises(PartialOrderEventFullReplacementRequired) as raised:
            self._connect(library)
        self.assertIsNone(raised.exception.checkpoint)

    def test_runtime_identity_change_requires_replacement_on_every_view(self):
        library = _FakeLibrary()
        reader = self._connect(library)
        self.addCleanup(reader.close)
        library.runtime_full_replacement_required = True
        key = PartialOrderEventOrderKey(Market.SHANGHAI, 11, 7, 99)

        operations = (
            reader.status,
            reader.read_batch,
            reader.affected_channels,
            lambda: reader.find_order_state(key),
            lambda: reader.read_order_states(capacity=4),
        )
        for operation in operations:
            with self.subTest(operation=operation):
                with self.assertRaises(
                    PartialOrderEventFullReplacementRequired
                ) as raised:
                    operation()
                self.assertEqual(raised.exception.checkpoint, reader.checkpoint)

    def test_stale_broker_generation_remains_explicitly_readable(self):
        library = _FakeLibrary()
        library.broker_state = PartialOrderEventBrokerState.STALE
        library.broker_stale = True
        reader = self._connect(library)
        self.addCleanup(reader.close)
        self.assertTrue(reader.session.broker_stale)
        self.assertIs(
            reader.status().broker_state,
            PartialOrderEventBrokerState.STALE,
        )
        self.assertEqual(len(reader.read_batch()), 2)

    def test_tail_idle_requires_a_live_ready_broker(self):
        library = _FakeLibrary()
        library.service_state = PartialOrderEventServiceState.CONTIGUOUS
        library.service_stale = False
        reader = self._connect(library)
        self.addCleanup(reader.close)
        self.assertEqual(len(reader.read_batch()), 2)
        self.assertTrue(reader.read_batch().tail_idle)

        for broker_state, broker_stale in (
            (PartialOrderEventBrokerState.READY, True),
            (PartialOrderEventBrokerState.STALE, True),
            (PartialOrderEventBrokerState.RESTARTING, True),
            (PartialOrderEventBrokerState.UNAVAILABLE, True),
            (PartialOrderEventBrokerState.STOPPED_CLEAN, False),
        ):
            with self.subTest(
                broker_state=broker_state,
                broker_stale=broker_stale,
            ):
                stale_library = _FakeLibrary()
                stale_library.broker_state = broker_state
                stale_library.broker_stale = broker_stale
                stale_library.service_state = (
                    PartialOrderEventServiceState.CONTIGUOUS
                )
                stale_library.service_stale = False
                stale_reader = self._connect(stale_library)
                self.addCleanup(stale_reader.close)
                self.assertEqual(len(stale_reader.read_batch()), 2)
                self.assertFalse(stale_reader.read_batch().tail_idle)

        for service_state in (
            PartialOrderEventServiceState.INITIALIZING,
            PartialOrderEventServiceState.REORDERING,
            PartialOrderEventServiceState.CATCHING_UP,
            PartialOrderEventServiceState.RESTARTING,
            PartialOrderEventServiceState.FROZEN_CONFLICT,
            PartialOrderEventServiceState.FROZEN_RESOURCE,
            PartialOrderEventServiceState.CORRECTION_PENDING,
            PartialOrderEventServiceState.STOPPED_CLEAN,
        ):
            with self.subTest(service_state=service_state):
                noncontiguous_library = _FakeLibrary()
                noncontiguous_library.service_state = service_state
                noncontiguous_library.service_stale = False
                noncontiguous_reader = self._connect(noncontiguous_library)
                self.addCleanup(noncontiguous_reader.close)
                self.assertEqual(len(noncontiguous_reader.read_batch()), 2)
                self.assertFalse(noncontiguous_reader.read_batch().tail_idle)

        mapping_stale_library = _FakeLibrary()
        mapping_stale_library.service_state = (
            PartialOrderEventServiceState.CONTIGUOUS
        )
        mapping_stale_library.service_stale = True
        mapping_stale_reader = self._connect(mapping_stale_library)
        self.addCleanup(mapping_stale_reader.close)
        self.assertEqual(len(mapping_stale_reader.read_batch()), 2)
        self.assertFalse(mapping_stale_reader.read_batch().tail_idle)

    def test_unsupported_native_proof_value_fails_closed(self):
        library = _FakeLibrary()
        library.ordering_quality = 2
        with self.assertRaisesRegex(
            WireFormatError, "partial Event session enum"
        ):
            self._connect(library)

    def test_forgotten_close_releases_native_handle(self):
        library = _FakeLibrary()
        reader = self._connect(library)
        del reader
        gc.collect()
        self.assertTrue(library.closed)


if __name__ == "__main__":
    unittest.main()
