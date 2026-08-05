from __future__ import annotations

import importlib.util
import unittest

import l2flow_realtime as rt


SESSION = bytes.fromhex("31000000000000000000000000000013")


def event(sequence: int, *, price: int | None = None) -> rt.DerivedEvent:
    key = rt.EventOrderKey(3, sequence)
    uid = rt.EventUid(1, 3, sequence, 1)
    return rt.DerivedEvent(
        uid,
        key,
        sequence,
        {"price_p6": sequence if price is None else price},
    )


class FakeTransport:
    session_id = SESSION

    def __init__(self) -> None:
        self.event_view = rt.EventStableView(
            (),
            rt.EventChangeCursor(SESSION, 1, 1),
            rt.InstrumentStableStatus(
                rt.Dataset.DERIVED_EVENT, rt.RepairState.LIVE, 1, 0
            ),
        )
        self.kline_view = rt.KLineStableView(
            (),
            rt.KLineChangeCursor(SESSION, 1, 1),
            rt.InstrumentStableStatus(
                rt.Dataset.KLINE, rt.RepairState.LIVE, 1, 0
            ),
        )
        self.event_batch = ((), rt.EventChangeCursor(SESSION, 1, 1))
        self.kline_batch = ((), rt.KLineChangeCursor(SESSION, 1, 1))

    def acquire_event_stable(self, instrument_id: int):
        return self.event_view

    def acquire_kline_stable(self, instrument_id: int):
        return self.kline_view

    def read_event_changes(self, cursor, maximum_changes):
        return self.event_batch

    def read_kline_changes(self, cursor, maximum_changes):
        return self.kline_batch


class WireV3Test(unittest.TestCase):
    def test_cursor_and_status_are_exact_32_byte_little_endian_values(self):
        cursor = rt.FastTickCursor(SESSION, 7, 19)
        encoded = rt.encode_cursor(cursor)
        self.assertEqual(len(encoded), 32)
        self.assertEqual(rt.decode_fast_tick_cursor(encoded), cursor)
        self.assertEqual(
            encoded,
            SESSION
            + b"\x07\x00\x00\x00"
            + b"\x00\x00\x00\x00"
            + b"\x13\x00\x00\x00\x00\x00\x00\x00",
        )

        status = rt.InstrumentStableStatus(
            rt.Dataset.DERIVED_EVENT,
            rt.RepairState.REBUILDING,
            7,
            11,
            23,
        )
        status_wire = rt.encode_stable_status(status)
        self.assertEqual(
            status_wire,
            b"L2F3"
            + b"\x03\x00\x00\x00"
            + b"\x02\x02\x00\x00"
            + b"\x07\x00\x00\x00"
            + b"\x0b\x00\x00\x00\x00\x00\x00\x00"
            + b"\x17\x00\x00\x00\x00\x00\x00\x00",
        )
        self.assertEqual(rt.decode_stable_status(status_wire), status)

        with self.assertRaises(ValueError):
            rt.decode_stable_status(status_wire[:8] + b"\xff" + status_wire[9:])

    def test_cursor_domains_have_no_global_market_position(self):
        fast = rt.FastTickCursor(SESSION, 1, 10)
        event_cursor = rt.EventChangeCursor(SESSION, 1, 20)
        kline_cursor = rt.KLineChangeCursor(SESSION, 2, 30)
        self.assertFalse(hasattr(fast, "global_sequence"))
        self.assertFalse(hasattr(event_cursor, "generation"))
        self.assertEqual(
            rt.decode_event_change_cursor(rt.encode_cursor(event_cursor)),
            event_cursor,
        )
        self.assertEqual(
            rt.decode_kline_change_cursor(rt.encode_cursor(kline_cursor)),
            kline_cursor,
        )


class ClientValidationV3Test(unittest.TestCase):
    def test_event_stable_view_requires_order_uid_uniqueness_and_cardinality(self):
        transport = FakeTransport()
        client = rt.L2FlowClient(transport)
        cursor = rt.EventChangeCursor(SESSION, 1, 1)
        status = rt.InstrumentStableStatus(
            rt.Dataset.DERIVED_EVENT, rt.RepairState.LIVE, 1, 2
        )
        transport.event_view = rt.EventStableView(
            (event(2), event(1)), cursor, status
        )
        with self.assertRaises(rt.CursorMismatchError):
            client.acquire_event_stable(1)

        transport.event_view = rt.EventStableView(
            (event(1),),
            cursor,
            rt.InstrumentStableStatus(
                rt.Dataset.DERIVED_EVENT, rt.RepairState.LIVE, 1, 2
            ),
        )
        with self.assertRaises(rt.CursorMismatchError):
            client.acquire_event_stable(1)

    def test_kline_stable_view_requires_key_order_and_cardinality(self):
        transport = FakeTransport()
        client = rt.L2FlowClient(transport)
        first = rt.KLineBar(rt.KLineBarKey(1, 1, 100), 1)
        second = rt.KLineBar(rt.KLineBarKey(1, 1, 200), 1)
        transport.kline_view = rt.KLineStableView(
            (second, first),
            rt.KLineChangeCursor(SESSION, 1, 1),
            rt.InstrumentStableStatus(
                rt.Dataset.KLINE, rt.RepairState.LIVE, 1, 2
            ),
        )
        with self.assertRaises(rt.CursorMismatchError):
            client.acquire_kline_stable(1)

    def test_client_rejects_noncontiguous_or_foreign_cdc_rows(self):
        transport = FakeTransport()
        client = rt.L2FlowClient(transport)
        cursor = rt.EventChangeCursor(SESSION, 1, 1)
        row = event(1)
        transport.event_batch = (
            (
                rt.EventMutation(
                    2,
                    rt.EventMutationKind.INSERT,
                    uid=row.uid,
                    row=row,
                ),
            ),
            rt.EventChangeCursor(SESSION, 1, 2),
        )
        with self.assertRaises(rt.CursorMismatchError):
            client.read_event_changes(cursor, 8)

        foreign_key = rt.KLineBarKey(2, 1, 100)
        foreign_bar = rt.KLineBar(foreign_key, 1)
        transport.kline_batch = (
            (
                rt.KLineMutation(
                    1,
                    rt.KLineMutationKind.UPSERT,
                    foreign_key,
                    foreign_bar,
                ),
            ),
            rt.KLineChangeCursor(SESSION, 1, 2),
        )
        with self.assertRaises(rt.CursorMismatchError):
            client.read_kline_changes(
                rt.KLineChangeCursor(SESSION, 1, 1), 8
            )


class CdcV3Test(unittest.TestCase):
    def test_event_batch_failure_does_not_publish_a_prefix(self):
        first = event(1)
        applier = rt.EventCdcApplier()
        with self.assertRaises(rt.CdcProtocolError):
            applier.apply(
                (
                    rt.EventMutation(
                        1,
                        rt.EventMutationKind.INSERT,
                        uid=first.uid,
                        row=first,
                    ),
                    rt.EventMutation(
                        3,
                        rt.EventMutationKind.DELETE,
                        uid=first.uid,
                    ),
                )
            )
        self.assertEqual(applier.rows, ())
        self.assertEqual(applier.next_change_sequence, 1)

    def test_public_row_values_are_top_level_immutable(self):
        row = event(1)
        with self.assertRaises(TypeError):
            row.values["price_p6"] = 2

    def test_public_row_values_cannot_override_stable_identity(self):
        with self.assertRaises(ValueError):
            rt.FastTickRow(1, {"instrument_tick_sequence": 99})
        with self.assertRaises(ValueError):
            rt.DerivedEvent(
                rt.EventUid(1, 3, 1, 1),
                rt.EventOrderKey(3, 1),
                1,
                {"business_sequence": 99},
            )
        with self.assertRaises(ValueError):
            rt.KLineBar(
                rt.KLineBarKey(1, 1, 0), 1, {"revision": 99}
            )
        with self.assertRaises(ValueError):
            rt.DerivedEvent(
                rt.EventUid(1, 3, 1, 1, affected_order_id=7),
                rt.EventOrderKey(3, 1, affected_order_id=8),
                1,
            )

    def test_cdc_appliers_reject_cross_instrument_rows_atomically(self):
        first = event(1)
        foreign = rt.DerivedEvent(
            rt.EventUid(2, 3, 2, 1),
            rt.EventOrderKey(3, 2),
            2,
        )
        events = rt.EventCdcApplier((first,))
        with self.assertRaises(rt.CdcProtocolError):
            events.apply(
                (
                    rt.EventMutation(
                        1,
                        rt.EventMutationKind.INSERT,
                        uid=foreign.uid,
                        row=foreign,
                    ),
                )
            )
        self.assertEqual(events.rows, (first,))
        self.assertEqual(events.next_change_sequence, 1)

        first_bar = rt.KLineBar(rt.KLineBarKey(1, 1, 0), 1)
        foreign_bar = rt.KLineBar(rt.KLineBarKey(2, 1, 0), 1)
        bars = rt.KLineCdcApplier((first_bar,))
        with self.assertRaises(rt.CdcProtocolError):
            bars.apply(
                (
                    rt.KLineMutation(
                        1,
                        rt.KLineMutationKind.UPSERT,
                        foreign_bar.key,
                        foreign_bar,
                    ),
                )
            )
        self.assertEqual(bars.bars, (first_bar,))
        self.assertEqual(bars.next_change_sequence, 1)

    def test_event_range_replace_is_invisible_until_commit(self):
        applier = rt.EventCdcApplier((event(100), event(102)))
        old = applier.rows
        applier.apply(
            (
                rt.EventMutation(
                    1,
                    rt.EventMutationKind.RANGE_REPLACE_BEGIN,
                    transaction_id=9,
                    replace_entire_instrument=True,
                ),
                rt.EventMutation(
                    2,
                    rt.EventMutationKind.RANGE_REPLACE_CHUNK,
                    transaction_id=9,
                    replacement_rows=(
                        event(100),
                        event(101),
                        event(102),
                    ),
                ),
            )
        )
        self.assertIs(applier.rows, old)
        self.assertTrue(applier.transaction_pending)

        applier.apply(
            (
                rt.EventMutation(
                    3,
                    rt.EventMutationKind.RANGE_REPLACE_COMMIT,
                    transaction_id=9,
                ),
            )
        )
        self.assertEqual(
            [row.order_key.business_sequence for row in applier.rows],
            [100, 101, 102],
        )
        self.assertFalse(applier.transaction_pending)

    def test_event_cdc_rejects_non_contiguous_changes(self):
        applier = rt.EventCdcApplier()
        with self.assertRaises(rt.CdcProtocolError):
            applier.apply(
                (
                    rt.EventMutation(
                        2,
                        rt.EventMutationKind.INSERT,
                        row=event(1),
                    ),
                )
            )

    def test_kline_revision_must_increase(self):
        key = rt.KLineBarKey(1, 60_000, 9_000_000_000)
        first = rt.KLineBar(key, 1, {"close_price_p6": 10})
        second = rt.KLineBar(key, 2, {"close_price_p6": 11})
        applier = rt.KLineCdcApplier()
        applier.apply((rt.KLineMutation(1, rt.KLineMutationKind.UPSERT, key, first),))
        applier.apply((rt.KLineMutation(2, rt.KLineMutationKind.UPSERT, key, second),))
        self.assertEqual(applier.bars[0].revision, 2)
        with self.assertRaises(rt.CdcProtocolError):
            applier.apply((rt.KLineMutation(3, rt.KLineMutationKind.UPSERT, key, second),))

    def test_kline_range_replace_is_invisible_until_commit(self):
        first_key = rt.KLineBarKey(1, 1_000, 9_000_000_000)
        second_key = rt.KLineBarKey(1, 1_000, 10_000_000_000)
        old = rt.KLineBar(first_key, 1, {"close_price_p6": 10})
        revised = rt.KLineBar(first_key, 2, {"close_price_p6": 11})
        inserted = rt.KLineBar(second_key, 1, {"close_price_p6": 12})
        applier = rt.KLineCdcApplier((old,))
        applier.apply(
            (
                rt.KLineMutation(
                    1,
                    rt.KLineMutationKind.RANGE_REPLACE_BEGIN,
                    transaction_id=7,
                    replace_entire_instrument=True,
                ),
                rt.KLineMutation(
                    2,
                    rt.KLineMutationKind.RANGE_REPLACE_CHUNK,
                    transaction_id=7,
                    replacement_bars=(revised, inserted),
                ),
            )
        )
        self.assertEqual(applier.bars, (old,))
        self.assertTrue(applier.transaction_pending)
        applier.apply(
            (
                rt.KLineMutation(
                    3,
                    rt.KLineMutationKind.RANGE_REPLACE_COMMIT,
                    transaction_id=7,
                ),
            )
        )
        self.assertEqual(applier.bars, (revised, inserted))
        self.assertFalse(applier.transaction_pending)


@unittest.skipUnless(
    importlib.util.find_spec("polars") is not None,
    "Polars optional dependency is not installed",
)
class PolarsV3Test(unittest.TestCase):
    def test_point_update_replaces_only_its_immutable_block(self):
        from l2flow_realtime.polars import ImmutablePolarsBlockTable

        table = ImmutablePolarsBlockTable(("key",), rows_per_block=2)
        table.replace_all(
            {"key": key, "value": key} for key in range(1, 7)
        )
        before = table.block_identities
        table.upsert({"key": 3, "value": 99})
        after = table.block_identities
        self.assertEqual(len(after), 3)
        self.assertEqual(before[0], after[0])
        self.assertNotEqual(before[1], after[1])
        self.assertEqual(before[2], after[2])
        self.assertEqual(table.frame()["key"].to_list(), list(range(1, 7)))

    def test_event_polars_keeps_old_blocks_until_range_commit(self):
        from l2flow_realtime.polars import EventPolarsHistory

        history = EventPolarsHistory(
            (event(100), event(102)), rows_per_block=1
        )
        old_ids = history.blocks.block_identities
        history.apply(
            (
                rt.EventMutation(
                    1,
                    rt.EventMutationKind.RANGE_REPLACE_BEGIN,
                    transaction_id=5,
                    replace_entire_instrument=True,
                ),
                rt.EventMutation(
                    2,
                    rt.EventMutationKind.RANGE_REPLACE_CHUNK,
                    transaction_id=5,
                    replacement_rows=(event(100), event(101), event(102)),
                ),
            )
        )
        self.assertEqual(history.blocks.block_identities, old_ids)
        history.apply(
            (
                rt.EventMutation(
                    3,
                    rt.EventMutationKind.RANGE_REPLACE_COMMIT,
                    transaction_id=5,
                ),
            )
        )
        self.assertEqual(
            history.blocks.frame()["business_sequence"].to_list(),
            [100, 101, 102],
        )

    def test_event_partial_range_reuses_unaffected_blocks(self):
        from l2flow_realtime.polars import EventPolarsHistory

        history = EventPolarsHistory(
            tuple(event(sequence) for sequence in range(1, 7)),
            rows_per_block=2,
        )
        before = history.blocks.block_identities
        history.apply(
            (
                rt.EventMutation(
                    1,
                    rt.EventMutationKind.RANGE_REPLACE_BEGIN,
                    transaction_id=17,
                    range_begin=rt.EventOrderKey(3, 3),
                    range_end_exclusive=rt.EventOrderKey(3, 5),
                ),
                rt.EventMutation(
                    2,
                    rt.EventMutationKind.RANGE_REPLACE_CHUNK,
                    transaction_id=17,
                    replacement_rows=(event(3, price=30), event(4, price=40)),
                ),
            )
        )
        self.assertEqual(history.blocks.block_identities, before)
        history.apply(
            (
                rt.EventMutation(
                    3,
                    rt.EventMutationKind.RANGE_REPLACE_COMMIT,
                    transaction_id=17,
                ),
            )
        )
        after = history.blocks.block_identities
        self.assertEqual(before[0], after[0])
        self.assertNotEqual(before[1], after[1])
        self.assertEqual(before[2], after[2])
        self.assertEqual(
            history.blocks.frame()["price_p6"].to_list(),
            [1, 2, 30, 40, 5, 6],
        )

    def test_event_update_removes_the_previous_order_key(self):
        from l2flow_realtime.polars import EventPolarsHistory

        old = event(1)
        moved = rt.DerivedEvent(
            old.uid,
            rt.EventOrderKey(3, 1, source_event_ordinal=1),
            old.source_arrival_id,
            {"price_p6": 2},
        )
        history = EventPolarsHistory((old, event(2)), rows_per_block=1)
        history.apply(
            (
                rt.EventMutation(
                    1,
                    rt.EventMutationKind.UPDATE,
                    uid=old.uid,
                    row=moved,
                ),
            )
        )
        self.assertEqual(history.blocks.row_count, 2)
        self.assertEqual(history.rows[0], moved)
        self.assertEqual(
            history.blocks.frame()["source_event_ordinal"].to_list(),
            [1, 0],
        )

    def test_kline_polars_keeps_old_blocks_until_range_commit(self):
        from l2flow_realtime.polars import KLinePolarsHistory

        key = rt.KLineBarKey(1, 1_000, 9_000_000_000)
        old = rt.KLineBar(key, 1, {"close_price_p6": 10})
        revised = rt.KLineBar(key, 2, {"close_price_p6": 11})
        history = KLinePolarsHistory((old,), rows_per_block=1)
        old_ids = history.blocks.block_identities
        history.apply(
            (
                rt.KLineMutation(
                    1,
                    rt.KLineMutationKind.RANGE_REPLACE_BEGIN,
                    transaction_id=8,
                    replace_entire_instrument=True,
                ),
                rt.KLineMutation(
                    2,
                    rt.KLineMutationKind.RANGE_REPLACE_CHUNK,
                    transaction_id=8,
                    replacement_bars=(revised,),
                ),
            )
        )
        self.assertEqual(history.blocks.block_identities, old_ids)
        self.assertEqual(history.blocks.frame()["revision"].to_list(), [1])
        history.apply(
            (
                rt.KLineMutation(
                    3,
                    rt.KLineMutationKind.RANGE_REPLACE_COMMIT,
                    transaction_id=8,
                ),
            )
        )
        self.assertEqual(history.blocks.frame()["revision"].to_list(), [2])


class RemovedSurfaceTest(unittest.TestCase):
    def test_snapshot_certified_generation_and_global_ring_are_not_exported(self):
        for name in (
            "LatestSnapshot",
            "CertifiedTickHistoryReader",
            "CertifiedOrderEventReader",
            "HistoryGeneration",
            "FastTickStreamReader",
        ):
            self.assertFalse(hasattr(rt, name), name)


if __name__ == "__main__":
    unittest.main()
