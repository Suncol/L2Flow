from __future__ import annotations

import struct
import unittest
from types import SimpleNamespace

from l2flow_realtime.fast_tick_live import (
    FastTickBatch,
    FastTickStreamReader,
)
from l2flow_realtime.models import SessionIdentity, TickOverrunError
from l2flow_realtime.native import NativeTickRead
from l2flow_realtime.wire import TICK_BYTES


RUN_ID = bytes(range(1, 17))
IDENTITY = SessionIdentity(RUN_ID, 19)
TRADE_DATE = 20260803


def tick(instrument_id: int, sequence: int) -> bytes:
    payload = bytearray(TICK_BYTES)
    struct.pack_into(
        "<II", payload, 8, instrument_id, instrument_id - 1
    )
    struct.pack_into("<Q", payload, 32, sequence)
    struct.pack_into("<I", payload, 100, TRADE_DATE)
    return bytes(payload)


class FakeClient:
    def __init__(self, reads, *, contiguous: int = 10) -> None:
        self.reads = list(reads)
        self.calls = []
        self.session = SimpleNamespace(
            identity=IDENTITY,
            trade_date=TRADE_DATE,
            tick_contiguous_published_sequence=contiguous,
        )

    def session_info(self):
        return self.session

    def _read_fast_ticks(self, expected, maximum):
        self.calls.append((expected, maximum))
        value = self.reads.pop(0)
        if isinstance(value, BaseException):
            raise value
        return value


class ChangingSessionClient(FakeClient):
    def __init__(self) -> None:
        super().__init__([], contiguous=10)
        self.samples = 0

    def session_info(self):
        self.samples += 1
        if self.samples == 1:
            return self.session
        return SimpleNamespace(
            identity=SessionIdentity(b"N" * 16, 20),
            trade_date=20260804,
            tick_contiguous_published_sequence=1,
        )


class FastTickLiveTests(unittest.TestCase):
    def test_owned_dense_batch_and_instrument_filter(self):
        records = tick(1, 7) + tick(2, 8) + tick(1, 9)
        batch = FastTickBatch(
            IDENTITY, TRADE_DATE, 7, 10, records
        )
        self.assertEqual(len(batch), 3)
        self.assertEqual(
            batch.read_columns("instrument_id", "tick_stream_sequence"),
            {
                "instrument_id": (1, 2, 1),
                "tick_stream_sequence": (7, 8, 9),
            },
        )
        selected = batch.instrument_wire_records(1)
        self.assertEqual(len(selected), 2 * TICK_BYTES)
        self.assertEqual(
            struct.unpack_from("<Q", selected, 32)[0], 7
        )
        self.assertEqual(
            struct.unpack_from("<Q", selected, TICK_BYTES + 32)[0], 9
        )

    def test_cursor_advances_only_after_valid_owned_batch(self):
        client = FakeClient(
            [
                NativeTickRead((tick(1, 4), tick(2, 5)), 6, 0),
                NativeTickRead((), 6, 0),
            ]
        )
        reader = FastTickStreamReader(client, 4, batch_records=8)
        first = reader.read()
        self.assertEqual((first.first_sequence, first.next_sequence), (4, 6))
        self.assertEqual(reader.next_sequence, 6)
        second = reader.read(3)
        self.assertEqual(len(second), 0)
        self.assertEqual(reader.next_sequence, 6)
        self.assertEqual(client.calls, [(4, 8), (6, 3)])

    def test_overrun_leaves_cursor_for_history_reconciliation(self):
        error = TickOverrunError(4, 20)
        client = FakeClient([error])
        reader = FastTickStreamReader(client, 4)
        with self.assertRaises(TickOverrunError):
            reader.read()
        self.assertEqual(reader.next_sequence, 4)

    def test_invalid_native_sequence_does_not_advance(self):
        client = FakeClient(
            [NativeTickRead((tick(1, 9),), 5, 0)]
        )
        reader = FastTickStreamReader(client, 4)
        with self.assertRaisesRegex(
            RuntimeError, "identity/sequence is noncanonical"
        ):
            reader.read()
        self.assertEqual(reader.next_sequence, 4)

    def test_after_latest_starts_at_successor(self):
        client = FakeClient([], contiguous=42)
        reader = FastTickStreamReader.after_latest(
            client, batch_records=2
        )
        self.assertEqual(reader.next_sequence, 43)

    def test_after_latest_rejects_session_change_between_samples(self):
        with self.assertRaisesRegex(
            RuntimeError, "session changed while opening"
        ):
            FastTickStreamReader.after_latest(ChangingSessionClient())


if __name__ == "__main__":
    unittest.main()
