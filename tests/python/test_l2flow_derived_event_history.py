import ctypes
import unittest

from l2flow_realtime import (
    HistoryCoverageInfo,
    Market,
    SessionIdentity,
    TemporalCoverageKind,
    UnavailableError,
)
from l2flow_realtime.instrument_derived_event_history import (
    InstrumentDerivedEventHistoryReader,
    InstrumentDerivedEventKind,
    _BUFFER_TOO_SMALL,
    _DerivedCheckpointC,
    _DerivedEventRowC,
    _session_to_c,
)
from test_l2flow_history_v2 import _target_checkpoint
from test_l2flow_realtime import session_info


def _set_size(pointer, value):
    ctypes.cast(
        pointer, ctypes.POINTER(ctypes.c_size_t)
    ).contents.value = value


def _set_u32(pointer, value):
    ctypes.cast(
        pointer, ctypes.POINTER(ctypes.c_uint32)
    ).contents.value = value


class _FakeDerivedLibrary:
    def __init__(self):
        self.mode = None
        self.step = 0
        self.update_count = 0
        self.read_calls = 0
        self.closed = False
        self.next_sequence = 1
        self.order_state_count = 0

    def l2flow_instrument_derived_event_history_begin_full_v1(
        self, _handle, _generation, _page_records
    ):
        self.mode = "full"
        self.step = 0
        return 0

    def l2flow_instrument_derived_event_history_begin_update_v1(
        self, _handle, _checkpoint, _generation, _page_records
    ):
        self.update_count += 1
        self.mode = "update" if self.update_count == 1 else "empty"
        self.step = 0
        return 0

    def _fill(self, row, sequence, kind):
        row.record_schema_version = 2
        row.record_bytes = ctypes.sizeof(_DerivedEventRowC)
        row.derived_event_sequence = sequence
        row.trade_date = _target_checkpoint().trade_date
        row.instrument_id = 1
        row.market = int(Market.SHANGHAI)
        row.event_kind = int(kind)
        row.channel = 9
        row.native_event_sequence = 500 + sequence
        row.source_sequence = sequence
        row.ingress_sequence = sequence
        row.tick_stream_sequence = sequence
        row.reserved0 = 0
        row.reserved1[0] = 1

    def l2flow_instrument_derived_event_history_read_v1(
        self, _handle, rows, capacity, count, eof
    ):
        self.read_calls += 1
        if self.step:
            _set_size(count, 0)
            _set_u32(eof, 1)
            return 0
        if self.mode == "empty":
            self.step = 1
            _set_size(count, 0)
            _set_u32(eof, 1)
            return 0
        required = 4 if self.mode == "full" else 1
        _set_size(count, required)
        _set_u32(eof, 0)
        if capacity < required:
            return _BUFFER_TOO_SMALL
        if self.mode == "full":
            for index in range(required):
                kind = (
                    InstrumentDerivedEventKind.ORDER_REVISION
                    if index == 1
                    else InstrumentDerivedEventKind.TRADE
                )
                self._fill(
                    rows[index], self.next_sequence + index, kind
                )
            order = rows[1]
            order.order_id = 11_001
            order.revision = 1
            order.original_quantity = 101
            order.original_quantity_valid = 1
            order.original_quantity_status = 2
            order.price_p6 = 1_235_000
            order.price_valid = 1
            order.price_source = 2
            self.next_sequence += required
            self.order_state_count = 1
        else:
            self._fill(
                rows[0],
                self.next_sequence,
                InstrumentDerivedEventKind.ORDER_REVISION,
            )
            order = rows[0]
            order.order_id = 11_001
            order.operation = 1
            order.revision = 2
            order.original_quantity = 151
            order.original_quantity_valid = 1
            order.original_quantity_status = 1
            order.published_quantity = 50
            order.published_quantity_valid = 1
            order.remaining_quantity = 50
            order.remaining_quantity_valid = 1
            order.source_matched_quantity = 101
            order.source_matched_quantity_valid = 1
            order.observed_pre_add_trade_quantity = 101
            order.price_p6 = 1_236_000
            order.price_valid = 1
            order.price_source = 1
            order.add_seen = 1
            order.apply_to_book = 1
            self.next_sequence += 1
        self.step = 1
        return 0

    def l2flow_instrument_derived_event_history_verified_checkpoint_v1(
        self, _handle, output
    ):
        value = ctypes.cast(
            output, ctypes.POINTER(_DerivedCheckpointC)
        ).contents
        raw = _target_checkpoint().to_wire()
        ctypes.memmove(value.raw_checkpoint, raw, len(raw))
        value.derived_event_sequence_exclusive = self.next_sequence
        value.order_state_count = self.order_state_count
        value.instrument_id = 1
        value.trade_date = _target_checkpoint().trade_date
        value.market = int(Market.SHANGHAI)
        value.finalized = 0
        return 0

    def l2flow_instrument_derived_event_history_last_raw_error_v1(
        self, _handle
    ):
        return 0

    def l2flow_instrument_derived_event_history_session_close_v1(
        self, _handle
    ):
        self.closed = True


def _reader(library):
    session = session_info()
    return InstrumentDerivedEventHistoryReader(
        object(),
        library,
        ctypes.c_void_p(1),
        instrument_id=1,
        market=Market.SHANGHAI,
        page_records=1,
        history_coverage=HistoryCoverageInfo(
            run_id=session.run_id,
            session_epoch=session.session_epoch,
            trade_date=session.trade_date,
            coverage_kind=TemporalCoverageKind.FROM_OPEN,
        ),
    )


class DerivedEventHistoryTests(unittest.TestCase):
    def test_session_bridge_copies_complete_daily_catalog_identity(self):
        session = session_info()
        value = _session_to_c(session)
        self.assertEqual(bytes(value.run_id), session.run_id)
        self.assertEqual(
            bytes(value.catalog_digest), session.catalog_digest
        )
        self.assertEqual(
            value.catalog_generation, session.catalog_generation
        )
        self.assertEqual(value.bound_count, session.bound_count)
        self.assertEqual(
            value.catalog_scope, int(session.catalog_scope)
        )
        self.assertEqual(value.coverage_complete, 1)
        self.assertEqual(
            value.catalog_trade_date, session.catalog_trade_date
        )
        self.assertEqual(
            value.catalog_version, session.catalog_version
        )
        self.assertEqual(value.reserved_catalog, 0)

    def test_full_update_empty_and_buffer_retry(self):
        library = _FakeDerivedLibrary()
        reader = _reader(library)
        with reader.read_all(expected_generation=9) as full:
            batches = list(full.batches())
            self.assertEqual(len(batches), 1)
            rows = batches[0].materialize()
            self.assertEqual(len(rows), 4)
            self.assertEqual(
                rows[1].event_kind,
                InstrumentDerivedEventKind.ORDER_REVISION,
            )
            self.assertEqual(rows[1].original_quantity, 101)
            self.assertEqual(rows[0].source_tick_event_ordinal, 0)
            self.assertEqual(
                rows[0].event_uid.session_identity,
                SessionIdentity(
                    session_info().run_id,
                    session_info().session_epoch,
                ),
            )
            first = full.verified_checkpoint
        # First data call reports BUFFER_TOO_SMALL, then the same native page
        # is retried, then a separate EOF is consumed.
        self.assertEqual(library.read_calls, 3)

        with reader.read_updates(
            first, expected_generation=10
        ) as update:
            batches = list(update.batches())
            self.assertEqual(len(batches), 1)
            order = batches[0].row(0)
            self.assertEqual(order.revision, 2)
            self.assertEqual(order.original_quantity, 151)
            self.assertEqual(order.source_tick_event_ordinal, 0)
            self.assertIsNotNone(order.event_uid)
            self.assertEqual(
                order.observed_pre_add_trade_quantity, 101
            )
            second = update.verified_checkpoint
        self.assertEqual(
            second.derived_event_sequence_exclusive,
            first.derived_event_sequence_exclusive + 1,
        )

        with reader.read_updates(
            second, expected_generation=10
        ) as empty:
            self.assertEqual(list(empty.batches()), [])
            third = empty.verified_checkpoint
        self.assertEqual(
            third.derived_event_sequence_exclusive,
            second.derived_event_sequence_exclusive,
        )
        reader.close()

    def test_early_close_fail_closes_reader(self):
        library = _FakeDerivedLibrary()
        reader = _reader(library)
        cursor = reader.read_all()
        with self.assertRaises(UnavailableError):
            _ = cursor.verified_checkpoint
        cursor.close()
        self.assertTrue(reader.closed)
        self.assertTrue(library.closed)

    def test_expected_generation_is_strict(self):
        reader = _reader(_FakeDerivedLibrary())
        for invalid in (True, 0, -1, 1 << 64):
            with self.subTest(invalid=invalid):
                with self.assertRaises(ValueError):
                    reader.read_all(expected_generation=invalid)
        reader.close()


if __name__ == "__main__":
    unittest.main()
