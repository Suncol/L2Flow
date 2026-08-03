from __future__ import annotations

import dataclasses
import os
import struct
import sys
import tempfile
import threading
import time
import unittest
from types import SimpleNamespace


REPOSITORY = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)
PYTHON_ROOT = os.path.join(REPOSITORY, "python")
if PYTHON_ROOT not in sys.path:
    sys.path.insert(0, PYTHON_ROOT)


from l2flow_realtime._generation import (  # noqa: E402
    ENDPOINT_FLAG_COVERAGE_FROM_OPEN,
    ENDPOINT_FLAG_RECORD_COVERAGE_COMPLETE,
    GenerationEndpoint,
)
from l2flow_realtime.checkpoint import (  # noqa: E402
    InstrumentTickDeltaCheckpoint,
)
from l2flow_realtime.fast_tick_live import FastTickBatch  # noqa: E402
from l2flow_realtime.models import (  # noqa: E402
    CatalogScope,
    HistoryCoverageInfo,
    SessionIdentity,
    TemporalCoverageKind,
    TickOverrunError,
)
from l2flow_realtime.polars import polars_available  # noqa: E402
from l2flow_realtime.polars import (  # noqa: E402
    PolarsClient,
    PolarsHistoryCoverageError,
    PolarsHistoryNotReadyError,
    PolarsHistoryProductKind,
    PolarsHistoryRefreshError,
    PolarsInstrumentTickHistory,
)
from l2flow_realtime.polars_fast_tick_history import (  # noqa: E402
    FastTickHistoryToken,
    PolarsFastTickHistory,
)
from l2flow_realtime.wire import TICK_BYTES  # noqa: E402


RUN_ID = bytes(range(1, 17))
CATALOG_DIGEST = b"C" * 32
INPUT_DIGEST = b"I" * 32
TRADE_DATE = 20260803
SOURCE_IDS = (100, 101, 102, 103)


def endpoint(
    generation: int,
    tick_exclusive: int,
) -> GenerationEndpoint:
    count = tick_exclusive - 1
    return GenerationEndpoint(
        run_id=RUN_ID,
        session_epoch=9,
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
            ENDPOINT_FLAG_COVERAGE_FROM_OPEN
            | ENDPOINT_FLAG_RECORD_COVERAGE_COMPLETE
        ),
    )


def checkpoint(
    generation: int,
    tick_exclusive: int,
    instrument_count: int,
) -> InstrumentTickDeltaCheckpoint:
    return InstrumentTickDeltaCheckpoint.from_endpoint(
        endpoint(generation, tick_exclusive),
        instrument_id=1,
        ordinal=0,
        instrument_tick_source_record_counts=(
            0,
            instrument_count,
            0,
            0,
        ),
        payload_projection=1,
        tick_record_coverage_complete=True,
    )


def tick(instrument_id: int, sequence: int, price: int) -> bytes:
    payload = bytearray(TICK_BYTES)
    struct.pack_into(
        "<IIIIQQQ",
        payload,
        0,
        2,
        TICK_BYTES,
        instrument_id,
        instrument_id - 1,
        sequence,
        sequence,
        sequence,
    )
    struct.pack_into("<I", payload, 100, TRADE_DATE)
    struct.pack_into("<q", payload, 176, price)
    payload[185] = 1
    return bytes(payload)


class FakeBatch:
    def __init__(self, rows) -> None:
        self.rows = tuple(rows)
        self.record_count = len(self.rows)

    def read_columns(self, *names):
        return {
            name: tuple(row[name] for row in self.rows)
            for name in names
        }

    def close(self):
        pass


class FakeCursor:
    def __init__(self, rows, verified_checkpoint) -> None:
        self.rows = tuple(rows)
        self.verified_checkpoint = verified_checkpoint

    def __enter__(self):
        return self

    def __exit__(self, _type, _value, _traceback):
        pass

    def batches(self):
        if self.rows:
            yield FakeBatch(self.rows)


class FakeChunkedCursor:
    def __init__(self, row_batches, verified_checkpoint) -> None:
        self.row_batches = tuple(tuple(rows) for rows in row_batches)
        self.verified_checkpoint = verified_checkpoint

    def __enter__(self):
        return self

    def __exit__(self, _type, _value, _traceback):
        pass

    def batches(self):
        for rows in self.row_batches:
            yield FakeBatch(rows)


class FakeHistoryReader:
    def __init__(self, full, updates) -> None:
        self.full = full
        self.updates = list(updates)
        self.closed = False

    def read_all(self, _instrument, **_kwargs):
        return self.full

    def read_updates(self, _instrument, _checkpoint, **_kwargs):
        return self.updates.pop(0)

    def close(self):
        self.closed = True


class FakeStream:
    def __init__(
        self,
        sequence,
        *,
        identity=SessionIdentity(RUN_ID, 9),
        trade_date=TRADE_DATE,
    ) -> None:
        self.next_sequence = sequence
        self.session_identity = identity
        self.trade_date = trade_date
        self.closed = False

    def read(self):
        return FastTickBatch(
            self.session_identity,
            self.trade_date,
            self.next_sequence,
            self.next_sequence,
            b"",
        )

    def close(self):
        self.closed = True


class FakeClient:
    def __init__(self) -> None:
        self.opened = []

    def open_fast_tick_stream(self, sequence, *, batch_records):
        stream = FakeStream(sequence)
        self.opened.append((sequence, batch_records, stream))
        return stream


@unittest.skipUnless(polars_available(), "Polars is not installed")
class PolarsFastTickHistoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.coverage = HistoryCoverageInfo(
            RUN_ID,
            9,
            TRADE_DATE,
            TemporalCoverageKind.FROM_OPEN,
        )

    def test_history_tail_then_generation_reconciliation_has_no_duplicate(self):
        first = checkpoint(1, 3, 2)
        second = checkpoint(2, 5, 3)
        reader = FakeHistoryReader(
            FakeCursor(
                (
                    {"tick_stream_sequence": 1, "price_p6": 101},
                    {"tick_stream_sequence": 2, "price_p6": 102},
                ),
                first,
            ),
            [
                FakeCursor(
                    (
                        {
                            "tick_stream_sequence": 3,
                            "price_p6": 103,
                        },
                    ),
                    second,
                )
            ],
        )
        client = FakeClient()
        history = PolarsFastTickHistory(
            client,
            reader,
            1,
            history_coverage=self.coverage,
            columns=("tick_stream_sequence", "price_p6"),
            autostart=False,
        )
        history._publish_initial()
        history._publish_tail(
            FastTickBatch(
                SessionIdentity(RUN_ID, 9),
                TRADE_DATE,
                3,
                5,
                tick(1, 3, 103) + tick(2, 4, 999),
            )
        )
        live = history.latest_dataframe()
        self.assertEqual(
            live["tick_stream_sequence"].to_list(), [1, 2, 3]
        )

        self.assertTrue(history._refresh_durable())
        reconciled = history.latest_snapshot()
        self.assertEqual(
            reconciled.dataframe["tick_stream_sequence"].to_list(),
            [1, 2, 3],
        )
        self.assertEqual(reconciled.row_count, 3)
        self.assertEqual(
            reconciled.continuity_token,
            FastTickHistoryToken(second, 5),
        )
        history.close()

    def test_snapshot_is_pinned_across_later_tail_publication(self):
        first = checkpoint(1, 3, 2)
        reader = FakeHistoryReader(
            FakeCursor(
                (
                    {"tick_stream_sequence": 1, "price_p6": 101},
                    {"tick_stream_sequence": 2, "price_p6": 102},
                ),
                first,
            ),
            [],
        )
        history = PolarsFastTickHistory(
            FakeClient(),
            reader,
            1,
            history_coverage=self.coverage,
            columns=("tick_stream_sequence", "price_p6"),
            autostart=False,
        )
        history._publish_initial()
        pinned = history.latest_snapshot()
        history._publish_tail(
            FastTickBatch(
                SessionIdentity(RUN_ID, 9),
                TRADE_DATE,
                3,
                4,
                tick(1, 3, 103),
            )
        )
        self.assertEqual(pinned.lazyframe.collect().height, 2)
        self.assertEqual(history.latest_dataframe().height, 3)
        history.close()

    def test_private_durable_chunks_follow_compaction_threshold(self):
        first = checkpoint(1, 4, 3)
        second = checkpoint(2, 6, 5)
        reader = FakeHistoryReader(
            FakeChunkedCursor(
                (
                    ({"tick_stream_sequence": 1, "price_p6": 101},),
                    ({"tick_stream_sequence": 2, "price_p6": 102},),
                    ({"tick_stream_sequence": 3, "price_p6": 103},),
                ),
                first,
            ),
            [
                FakeChunkedCursor(
                    (
                        ({"tick_stream_sequence": 4, "price_p6": 104},),
                        ({"tick_stream_sequence": 5, "price_p6": 105},),
                    ),
                    second,
                )
            ],
        )
        history = PolarsFastTickHistory(
            FakeClient(),
            reader,
            1,
            history_coverage=self.coverage,
            columns=("tick_stream_sequence", "price_p6"),
            compact_after_chunks=1,
            autostart=False,
        )
        self.addCleanup(history.close)

        history._publish_initial()
        pinned = history.latest_snapshot()
        self.assertEqual(len(history._durable_chunks), 1)
        self.assertEqual(history._durable_chunks[0].height, 3)

        self.assertTrue(history._refresh_durable())
        self.assertEqual(len(history._durable_chunks), 1)
        self.assertEqual(history._durable_chunks[0].height, 5)
        self.assertEqual(
            history.latest_dataframe()["tick_stream_sequence"].to_list(),
            [1, 2, 3, 4, 5],
        )
        # Replacing the private authority reference after promotion cannot
        # mutate a previously pinned public cut.
        self.assertEqual(
            pinned.dataframe["tick_stream_sequence"].to_list(),
            [1, 2, 3],
        )

    def test_empty_global_batch_proves_gap_before_next_instrument_row(self):
        first = checkpoint(1, 3, 2)
        reader = FakeHistoryReader(
            FakeCursor(
                (
                    {"tick_stream_sequence": 1, "price_p6": 101},
                    {"tick_stream_sequence": 2, "price_p6": 102},
                ),
                first,
            ),
            [],
        )
        history = PolarsFastTickHistory(
            FakeClient(),
            reader,
            1,
            history_coverage=self.coverage,
            columns=("tick_stream_sequence", "price_p6"),
            autostart=False,
        )
        history._publish_initial()
        history._publish_tail(
            FastTickBatch(
                SessionIdentity(RUN_ID, 9),
                TRADE_DATE,
                3,
                5,
                tick(2, 3, 900) + tick(2, 4, 901),
            )
        )
        # No public version is needed for a scanned interval containing only
        # other instruments, but it authorizes the exact next global cut.
        self.assertEqual(history.latest_dataframe().height, 2)
        history._publish_tail(
            FastTickBatch(
                SessionIdentity(RUN_ID, 9),
                TRADE_DATE,
                5,
                6,
                tick(1, 5, 105),
            )
        )
        self.assertEqual(
            history.latest_dataframe()["tick_stream_sequence"].to_list(),
            [1, 2, 5],
        )
        history.close()

    def test_discontinuous_tail_batch_is_rejected_without_publication(self):
        first = checkpoint(1, 3, 2)
        history = PolarsFastTickHistory(
            FakeClient(),
            FakeHistoryReader(
                FakeCursor(
                    (
                        {"tick_stream_sequence": 1, "price_p6": 101},
                        {"tick_stream_sequence": 2, "price_p6": 102},
                    ),
                    first,
                ),
                [],
            ),
            1,
            history_coverage=self.coverage,
            columns=("tick_stream_sequence", "price_p6"),
            autostart=False,
        )
        history._publish_initial()
        before = history.latest_snapshot()
        with self.assertRaisesRegex(
            PolarsHistoryRefreshError, "discontinuous"
        ):
            history._publish_tail(
                FastTickBatch(
                    SessionIdentity(RUN_ID, 9),
                    TRADE_DATE,
                    4,
                    5,
                    tick(1, 4, 104),
                )
            )
        after = history.latest_snapshot()
        self.assertEqual(after.version, before.version)
        self.assertEqual(after.row_count, before.row_count)
        history.close()

    def test_initial_stream_mismatch_never_publishes_history(self):
        first = checkpoint(1, 3, 2)
        client = FakeClient()

        def mismatched_stream(sequence, *, batch_records):
            stream = FakeStream(
                sequence,
                identity=SessionIdentity(b"X" * 16, 99),
            )
            client.opened.append((sequence, batch_records, stream))
            return stream

        client.open_fast_tick_stream = mismatched_stream
        history = PolarsFastTickHistory(
            client,
            FakeHistoryReader(
                FakeCursor(
                    (
                        {"tick_stream_sequence": 1, "price_p6": 101},
                        {"tick_stream_sequence": 2, "price_p6": 102},
                    ),
                    first,
                ),
                [],
            ),
            1,
            history_coverage=self.coverage,
            columns=("tick_stream_sequence", "price_p6"),
            autostart=False,
        )
        with self.assertRaises(PolarsHistoryCoverageError):
            history._publish_initial()
        self.assertTrue(client.opened[0][2].closed)
        with self.assertRaises(PolarsHistoryNotReadyError):
            history.latest_snapshot()
        history.close()

    def test_reopened_stream_mismatch_cannot_replace_valid_seam(self):
        first = checkpoint(1, 3, 2)
        second = checkpoint(2, 5, 3)
        client = FakeClient()
        history = PolarsFastTickHistory(
            client,
            FakeHistoryReader(
                FakeCursor(
                    (
                        {"tick_stream_sequence": 1, "price_p6": 101},
                        {"tick_stream_sequence": 2, "price_p6": 102},
                    ),
                    first,
                ),
                [
                    FakeCursor(
                        (
                            {
                                "tick_stream_sequence": 3,
                                "price_p6": 103,
                            },
                        ),
                        second,
                    )
                ],
            ),
            1,
            history_coverage=self.coverage,
            columns=("tick_stream_sequence", "price_p6"),
            autostart=False,
        )
        history._publish_initial()
        original_stream = history._stream
        before = history.latest_snapshot()
        invalid_stream = FakeStream(
            5,
            identity=SessionIdentity(b"X" * 16, 99),
        )

        def mismatched_stream(_sequence, *, batch_records):
            client.opened.append((5, batch_records, invalid_stream))
            return invalid_stream

        client.open_fast_tick_stream = mismatched_stream
        with self.assertRaises(PolarsHistoryCoverageError):
            history._refresh_durable()
        self.assertTrue(invalid_stream.closed)
        self.assertIs(history._stream, original_stream)
        self.assertFalse(original_stream.closed)
        after = history.latest_snapshot()
        self.assertEqual(after.version, before.version)
        self.assertEqual(after.continuity_token, before.continuity_token)
        history.close()

    def test_overrun_waits_for_durable_frontier_and_atomically_repairs_seam(
        self,
    ):
        first = checkpoint(1, 3, 2)
        recovered = checkpoint(2, 9, 6)
        pre_overrun_tail = FastTickBatch(
            SessionIdentity(RUN_ID, 9),
            TRADE_DATE,
            3,
            6,
            tick(1, 3, 103)
            + tick(2, 4, 904)
            + tick(1, 5, 105),
        )
        resumed_tail = FastTickBatch(
            SessionIdentity(RUN_ID, 9),
            TRADE_DATE,
            9,
            11,
            tick(1, 9, 109) + tick(2, 10, 910),
        )

        overrun_waiting = threading.Event()
        release_overrun = threading.Event()
        durable_waiting = threading.Event()
        release_durable = threading.Event()
        seam_read_waiting = threading.Event()
        release_seam_read = threading.Event()

        class GatedHistoryReader(FakeHistoryReader):
            def __init__(self):
                super().__init__(
                    FakeCursor(
                        (
                            {
                                "tick_stream_sequence": 1,
                                "price_p6": 101,
                            },
                            {
                                "tick_stream_sequence": 2,
                                "price_p6": 102,
                            },
                        ),
                        first,
                    ),
                    [
                        FakeCursor((), first),
                        FakeCursor(
                            (
                                {
                                    "tick_stream_sequence": 3,
                                    "price_p6": 103,
                                },
                                {
                                    "tick_stream_sequence": 5,
                                    "price_p6": 105,
                                },
                                {
                                    "tick_stream_sequence": 6,
                                    "price_p6": 106,
                                },
                                {
                                    "tick_stream_sequence": 7,
                                    "price_p6": 107,
                                },
                            ),
                            recovered,
                        ),
                    ],
                )
                self.update_calls = 0

            def read_updates(self, instrument, base, **kwargs):
                self.update_calls += 1
                if self.update_calls == 2:
                    durable_waiting.set()
                    if not release_durable.wait(timeout=2.0):
                        raise TimeoutError(
                            "test did not release the durable frontier"
                        )
                return super().read_updates(instrument, base, **kwargs)

        class OverrunningStream(FakeStream):
            def __init__(self):
                super().__init__(3)
                self.read_calls = 0

            def read(self):
                self.read_calls += 1
                if self.read_calls == 1:
                    self.next_sequence = pre_overrun_tail.next_sequence
                    return pre_overrun_tail
                if self.read_calls == 2:
                    overrun_waiting.set()
                    if not release_overrun.wait(timeout=2.0):
                        raise TimeoutError(
                            "test did not release the simulated overrun"
                        )
                    raise TickOverrunError(6, 8)
                return super().read()

        class ResumedStream(FakeStream):
            def __init__(self):
                super().__init__(9)
                self.read_calls = 0

            def read(self):
                self.read_calls += 1
                if self.read_calls == 1:
                    seam_read_waiting.set()
                    if not release_seam_read.wait(timeout=2.0):
                        raise TimeoutError(
                            "test did not release the repaired live seam"
                        )
                    self.next_sequence = resumed_tail.next_sequence
                    return resumed_tail
                return super().read()

        initial_stream = OverrunningStream()
        resumed_stream = ResumedStream()

        class RepairClient:
            def __init__(self):
                self.opened = []

            def open_fast_tick_stream(self, sequence, *, batch_records):
                if sequence == 3:
                    stream = initial_stream
                elif sequence == 9:
                    stream = resumed_stream
                else:
                    raise AssertionError(
                        f"unexpected repaired seam {sequence}"
                    )
                self.opened.append((sequence, batch_records, stream))
                return stream

        reader = GatedHistoryReader()
        client = RepairClient()
        history = PolarsFastTickHistory(
            client,
            reader,
            1,
            history_coverage=self.coverage,
            columns=("tick_stream_sequence", "price_p6"),
            tail_poll_interval=0.001,
            durable_refresh_interval=None,
        )
        self.addCleanup(history.close)
        # Release every possible blocking fake before close if an assertion
        # aborts the test early.
        self.addCleanup(release_overrun.set)
        self.addCleanup(release_durable.set)
        self.addCleanup(release_seam_read.set)

        history.wait_ready(timeout=2.0)
        self.assertTrue(overrun_waiting.wait(timeout=2.0))
        pinned = history.latest_snapshot()
        self.assertEqual(
            pinned.dataframe["tick_stream_sequence"].to_list(),
            [1, 2, 3, 5],
        )

        release_overrun.set()
        self.assertTrue(durable_waiting.wait(timeout=2.0))
        # The first refresh still advertises the old durable frontier.  No
        # speculative seam or partial replacement may become visible.
        while_waiting = history.latest_snapshot()
        self.assertEqual(while_waiting.version, pinned.version)
        self.assertEqual(
            while_waiting.dataframe[
                "tick_stream_sequence"
            ].to_list(),
            [1, 2, 3, 5],
        )
        self.assertEqual([item[0] for item in client.opened], [3])

        release_durable.set()
        self.assertTrue(seam_read_waiting.wait(timeout=2.0))
        repaired = history.latest_snapshot()
        repaired_sequences = repaired.dataframe[
            "tick_stream_sequence"
        ].to_list()
        self.assertEqual(repaired_sequences, [1, 2, 3, 5, 6, 7])
        self.assertEqual(len(repaired_sequences), len(set(repaired_sequences)))
        self.assertEqual(
            repaired.continuity_token,
            FastTickHistoryToken(recovered, 9),
        )
        self.assertEqual([item[0] for item in client.opened], [3, 9])
        self.assertTrue(initial_stream.closed)
        self.assertFalse(resumed_stream.closed)
        # The replacement is a new immutable cut; the pre-overrun snapshot
        # remains pinned to its original buffers and rows.
        self.assertEqual(
            pinned.lazyframe.collect()[
                "tick_stream_sequence"
            ].to_list(),
            [1, 2, 3, 5],
        )

        release_seam_read.set()
        deadline = time.monotonic() + 2.0
        expected = [1, 2, 3, 5, 6, 7, 9]
        while True:
            current = history.latest_snapshot()
            sequences = current.dataframe[
                "tick_stream_sequence"
            ].to_list()
            if sequences == expected:
                break
            if time.monotonic() >= deadline:
                self.fail(
                    "repaired FAST seam did not publish its resumed tail"
                )
            time.sleep(0.001)
        self.assertEqual(len(sequences), len(set(sequences)))
        self.assertEqual(
            current.continuity_token,
            FastTickHistoryToken(recovered, 11),
        )

    def test_process_start_partial_live_tail_requires_explicit_opt_in(self):
        partial_coverage = HistoryCoverageInfo(
            RUN_ID,
            9,
            TRADE_DATE,
            TemporalCoverageKind.PROCESS_START_PARTIAL,
            coverage_start_unix_ns=1_786_000_000_000_000_000,
        )
        partial_checkpoint = dataclasses.replace(
            checkpoint(1, 3, 2), coverage_from_open=False
        )
        reader = FakeHistoryReader(
            FakeCursor(
                (
                    {"tick_stream_sequence": 1, "price_p6": 101},
                    {"tick_stream_sequence": 2, "price_p6": 102},
                ),
                partial_checkpoint,
            ),
            [],
        )
        reader.history_coverage = partial_coverage

        class PartialStream(FakeStream):
            def __init__(self):
                super().__init__(3)
                self.read_calls = 0

            def read(self):
                self.read_calls += 1
                if self.read_calls == 1:
                    batch = FastTickBatch(
                        partial_coverage.identity,
                        partial_coverage.trade_date,
                        3,
                        4,
                        tick(1, 3, 103),
                    )
                    self.next_sequence = batch.next_sequence
                    return batch
                return super().read()

        partial_stream = PartialStream()

        class OwnedTailClient(FakeClient):
            def __init__(self):
                super().__init__()
                self.closed = False

            def open_fast_tick_stream(self, sequence, *, batch_records):
                if sequence != 3:
                    raise AssertionError(
                        f"unexpected partial live seam {sequence}"
                    )
                self.opened.append(
                    (sequence, batch_records, partial_stream)
                )
                return partial_stream

            def close(self):
                self.closed = True

        tail_client = OwnedTailClient()

        class FacadeClient:
            def __init__(self):
                self.independent_opens = 0

            def history_coverage(current):
                return partial_coverage

            def open_instrument_raw_event_history(
                current, **_kwargs
            ):
                return reader

            def session_info(current):
                return SimpleNamespace(
                    identity=partial_coverage.identity,
                    trade_date=partial_coverage.trade_date,
                    startup_prefix_recovered=False,
                )

            def _open_independent_read_client(current):
                current.independent_opens += 1
                return tail_client

        client = FacadeClient()
        frames = PolarsClient(client)
        with self.assertRaises(PolarsHistoryCoverageError):
            frames.open_instrument_tick_history(
                1,
                columns=("tick_stream_sequence", "price_p6"),
                live_tail=True,
                refresh_interval=None,
            )
        self.assertEqual(client.independent_opens, 0)

        history = frames.open_instrument_tick_history(
            1,
            columns=("tick_stream_sequence", "price_p6"),
            coverage_requirement="allow_process_start_partial",
            live_tail=True,
            refresh_interval=None,
            tail_poll_interval=0.001,
        )
        self.addCleanup(history.close)
        history.wait_ready(timeout=2.0)
        self.assertIsInstance(history, PolarsFastTickHistory)
        self.assertTrue(history.history_coverage.process_start_partial)
        self.assertEqual(
            history.history_coverage.coverage_start_unix_ns,
            partial_coverage.coverage_start_unix_ns,
        )
        self.assertEqual(client.independent_opens, 1)
        self.assertEqual([item[0] for item in tail_client.opened], [3])
        deadline = time.monotonic() + 2.0
        while True:
            sequences = history.latest_dataframe()[
                "tick_stream_sequence"
            ].to_list()
            if sequences == [1, 2, 3]:
                break
            if time.monotonic() >= deadline:
                self.fail("partial FAST live tail did not append")
            time.sleep(0.001)
        self.assertEqual(sequences, [1, 2, 3])
        self.assertGreaterEqual(partial_stream.read_calls, 1)
        self.assertEqual(
            history.latest_snapshot().continuity_token.live_next_sequence,
            4,
        )
        history.close()
        self.assertTrue(reader.closed)
        self.assertTrue(tail_client.closed)

    def test_default_construction_autostarts_and_spill_is_atomic_copy(self):
        first = checkpoint(1, 3, 2)
        history = PolarsFastTickHistory(
            FakeClient(),
            FakeHistoryReader(
                FakeCursor(
                    (
                        {"tick_stream_sequence": 1, "price_p6": 101},
                        {"tick_stream_sequence": 2, "price_p6": 102},
                    ),
                    first,
                ),
                [],
            ),
            1,
            history_coverage=self.coverage,
            columns=("tick_stream_sequence", "price_p6"),
            tail_poll_interval=0.001,
        )
        history.wait_ready(1.0)
        self.assertEqual(history.history_coverage, self.coverage)
        self.assertEqual(
            history.dataset_identity.product_kind,
            PolarsHistoryProductKind.FAST_RAW_INSTRUMENT_TICKS,
        )
        self.assertEqual(history.dataset_identity.instrument_id, 1)
        self.assertEqual(
            history.dataset_identity.projection_columns,
            ("tick_stream_sequence", "price_p6"),
        )
        with tempfile.TemporaryDirectory() as directory:
            target = os.path.join(directory, "fast.parquet")
            self.assertEqual(history.spill(target), target)
            import polars as pl

            self.assertEqual(
                pl.read_parquet(target)["tick_stream_sequence"].to_list(),
                [1, 2],
            )
        history.close()

    def test_public_default_does_not_open_live_tail_mapping(self):
        first = checkpoint(1, 3, 2)
        reader = FakeHistoryReader(
            FakeCursor(
                (
                    {"tick_stream_sequence": 1, "price_p6": 101},
                    {"tick_stream_sequence": 2, "price_p6": 102},
                ),
                first,
            ),
            [],
        )
        reader.history_coverage = self.coverage

        class FacadeClient:
            def __init__(self):
                self.independent_opens = 0

            def history_coverage(current):
                return self.coverage

            def open_instrument_raw_event_history(
                current, **_kwargs
            ):
                return reader

            def session_info(current):
                return SimpleNamespace(
                    identity=self.coverage.identity,
                    trade_date=self.coverage.trade_date,
                    startup_prefix_recovered=False,
                )

            def _open_independent_read_client(current):
                current.independent_opens += 1
                raise AssertionError(
                    "default raw History must not open a live-tail mapping"
                )

        client = FacadeClient()
        history = PolarsClient(client).open_instrument_tick_history(
            1,
            columns=("tick_stream_sequence", "price_p6"),
            refresh_interval=None,
        )
        self.addCleanup(history.close)
        history.wait_ready(timeout=1.0)
        self.assertIsInstance(history, PolarsInstrumentTickHistory)
        self.assertEqual(client.independent_opens, 0)


if __name__ == "__main__":
    unittest.main()
