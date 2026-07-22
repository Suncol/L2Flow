from __future__ import annotations

import struct
import unittest

from helpers import make_batch
from l2flow_factor import (
    BatchClosedError,
    BookImbalancePassthroughPlaceholder,
    CancelRatePassthroughPlaceholder,
    CheckpointError,
    ClockSemantics,
    FactorSpec,
    FailClosedNativeMux,
    IncrementalEventWindow,
    IncrementalTimeWindow,
    InputFamily,
    InputMode,
    InputSpec,
    MicropricePassthroughPlaceholder,
    NativeProofUnavailable,
    PLACEHOLDER_FACTOR_IDS,
    PlaceholderStatus,
    TradeImbalancePassthroughPlaceholder,
    TradeIntensityPassthroughPlaceholder,
)


class WindowTests(unittest.TestCase):
    def test_event_window_checkpoint_preserves_accumulator_bits(self) -> None:
        window = IncrementalEventWindow(3)
        for value in (1.0e16, 1.0, -1.0e16, 3.0, 1.0e-9):
            window.append(value)
        before_total = struct.pack("<d", window.total)
        before_values = window.values()
        restored = IncrementalEventWindow.restore_bytes(window.checkpoint_bytes())
        self.assertEqual(restored.values(), before_values)
        self.assertEqual(struct.pack("<d", restored.total), before_total)
        next_value = 7.0
        window.append(next_value)
        restored.append(next_value)
        self.assertEqual(struct.pack("<d", restored.total), struct.pack("<d", window.total))

    def test_time_window_boundary_checkpoint_and_invalid_restore(self) -> None:
        window = IncrementalTimeWindow(10)
        window.append(10, 1.0)
        window.append(20, 2.0)  # timestamp 10 remains: boundary is inclusive.
        window.append(21, 3.0)  # timestamp 10 now expires.
        restored = IncrementalTimeWindow.restore_bytes(window.checkpoint_bytes())
        self.assertEqual(restored.entries(), ((20, 2.0), (21, 3.0)))
        self.assertEqual(struct.pack("<d", restored.total), struct.pack("<d", 5.0))
        damaged = bytearray(window.checkpoint_bytes())
        damaged[-1] ^= 1
        with self.assertRaises(CheckpointError):
            IncrementalTimeWindow.restore_bytes(bytes(damaged))


def placeholder_spec(factor_id: str, family: InputFamily) -> FactorSpec:
    return FactorSpec(
        factor_id=factor_id,
        factor_version="0.0.0-placeholder",
        state_schema_version=1,
        input_mode=(
            InputMode.SNAPSHOT_ONLY
            if family is InputFamily.SNAPSHOT
            else InputMode.TICK_ONLY
        ),
        inputs=(
            InputSpec(
                1001 if family is InputFamily.SNAPSHOT else 1002,
                family,
                shard_id=1,
            ),
        ),
        clock_semantics=ClockSemantics.RECEIVE_MONOTONIC,
    )


class PlaceholderTests(unittest.TestCase):
    def test_all_five_are_exact_passthrough_and_explicitly_invalid(self) -> None:
        cases = (
            (BookImbalancePassthroughPlaceholder, InputFamily.SNAPSHOT, 1001),
            (MicropricePassthroughPlaceholder, InputFamily.SNAPSHOT, 1001),
            (TradeImbalancePassthroughPlaceholder, InputFamily.TICK, 1002),
            (CancelRatePassthroughPlaceholder, InputFamily.TICK, 1002),
            (TradeIntensityPassthroughPlaceholder, InputFamily.TICK, 1002),
        )
        self.assertEqual(
            PLACEHOLDER_FACTOR_IDS,
            (
                "book_imbalance",
                "microprice",
                "trade_imbalance",
                "cancel_rate",
                "trade_intensity",
            ),
        )
        self.assertNotIn("order_flow_imbalance", PLACEHOLDER_FACTOR_IDS)
        for factor_type, family, source in cases:
            with self.subTest(factor=factor_type.FACTOR_ID):
                batch, _, _, raw_records = make_batch(
                    family, source_stream_id=source
                )
                factor = factor_type(placeholder_spec(factor_type.FACTOR_ID, family))
                with batch:
                    leased_input = batch.records
                    (result,) = factor.on_batch(batch)
                    self.assertIs(result.records, leased_input)
                    self.assertEqual(result.records.tobytes(), raw_records.tobytes())
                    self.assertIs(result.status, PlaceholderStatus.PASSTHROUGH_PLACEHOLDER)
                    self.assertIsNone(result.factor_value)
                    self.assertFalse(result.factor_value_valid)
                    self.assertEqual(len(result.content_digest), 32)
                with self.assertRaises(BatchClosedError):
                    _ = result.records[0]

    def test_native_mux_absence_fails_closed(self) -> None:
        native = FailClosedNativeMux()
        with self.assertRaises(NativeProofUnavailable):
            native.select_safe_candidate(object())
        with self.assertRaises(NativeProofUnavailable):
            native.prove_snapshot_asof_tick(object())


if __name__ == "__main__":
    unittest.main()
