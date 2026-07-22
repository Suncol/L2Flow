from __future__ import annotations

from dataclasses import replace
import unittest

import numpy as np

from helpers import make_batch, REGISTRY
from l2flow_factor import (
    AttachError,
    BatchClosedError,
    ClockSemantics,
    ConsumerAttachSpec,
    EpochPolicy,
    FactorSpec,
    GapPolicy,
    InputFamily,
    InputMode,
    InputSpec,
    OutputCadence,
    ValidationError,
    Warmup,
    WindowKind,
    WindowSpec,
)


def tick_spec(inputs: tuple[InputSpec, ...] | None = None) -> FactorSpec:
    return FactorSpec(
        factor_id="test_factor",
        factor_version="1.0.0",
        state_schema_version=1,
        input_mode=InputMode.TICK_ONLY,
        inputs=inputs or (InputSpec(1002, InputFamily.TICK, shard_id=1),),
        clock_semantics=ClockSemantics.RECEIVE_MONOTONIC,
        windows=(
            WindowSpec("z_window", WindowKind.EVENT_COUNT, 3),
            WindowSpec("a_window", WindowKind.EVENT_TIME, 10),
        ),
        required_validity_mask=3,
        forbidden_quality_mask=5,
        on_gap_policy=GapPolicy.INVALIDATE,
        on_clock_epoch_change=EpochPolicy.RESET_AND_WARMUP,
        output_cadence=OutputCadence.EACH_TICK,
        warmup=Warmup(min_ticks=1),
    )


class FactorSpecTests(unittest.TestCase):
    def test_canonical_hash_sorts_declarative_inputs_and_windows(self) -> None:
        left_inputs = (
            InputSpec(1003, InputFamily.TICK, shard_id=1),
            InputSpec(1002, InputFamily.TICK, shard_id=1),
        )
        right_inputs = tuple(reversed(left_inputs))
        left = tick_spec(left_inputs)
        right = tick_spec(right_inputs)
        self.assertEqual(left.canonical_json(), right.canonical_json())
        self.assertEqual(left.sha256(), right.sha256())
        self.assertNotEqual(
            left.sha256(), replace(left, forbidden_quality_mask=6).sha256()
        )
        self.assertNotEqual(
            left.sha256(), replace(left, required_validity_mask=6).sha256()
        )

    def test_live_latest_requires_zero_source_and_explicit_nondeterminism(self) -> None:
        inputs = (
            InputSpec(1002, InputFamily.TICK, shard_id=1),
            InputSpec(0, InputFamily.LATEST_STATE, shard_id=1),
        )
        with self.assertRaises(ValidationError):
            FactorSpec(
                "live", "1", 1, InputMode.LIVE_LATEST, inputs,
                ClockSemantics.RECEIVE_MONOTONIC,
            )
        accepted = FactorSpec(
            "live", "1", 1, InputMode.LIVE_LATEST, inputs,
            ClockSemantics.RECEIVE_MONOTONIC,
            nondeterministic_live_latest=True,
        )
        self.assertTrue(accepted.nondeterministic_live_latest)
        with self.assertRaises(ValidationError):
            InputSpec(123, InputFamily.LATEST_STATE)
        with self.assertRaises(ValidationError):
            InputSpec(1, InputFamily.QUALITY)


class BatchViewTests(unittest.TestCase):
    def test_readonly_and_context_lifetime(self) -> None:
        batch, _, _, _ = make_batch()
        with batch:
            leased = batch.records
            detached = leased.copy()
            self.assertFalse(leased.flags.writeable)
            with self.assertRaises(ValueError):
                leased[0] = leased[0]
            self.assertEqual(int(leased[0]["header"]["instrument_id"]), 1)
            np.add(leased["header"]["instrument_id"], 1)
            np.sum(leased["header"]["instrument_id"])
        with self.assertRaises(BatchClosedError):
            _ = batch.records
        with self.assertRaises(BatchClosedError):
            _ = leased[0]
        with self.assertRaises(BatchClosedError):
            np.add(leased, leased)
        with self.assertRaises(BatchClosedError):
            np.sum(leased)
        self.assertEqual(len(detached), 2)
        self.assertFalse(detached.flags.writeable)

    def test_attach_rejects_registry_or_minimum_header_mismatch(self) -> None:
        _, metadata, expected, records = make_batch()
        wrong = replace(expected, registry_sha256=bytes.fromhex("33" * 32))
        from l2flow_factor import MdlBatchView

        with self.assertRaises(AttachError):
            MdlBatchView.attach(records, metadata, wrong)
        damaged = records.copy()
        damaged["header"]["magic"][0] = 0
        damaged = np.frombuffer(damaged.tobytes(order="C"), dtype=records.dtype)
        with self.assertRaises(AttachError):
            MdlBatchView.attach(damaged, metadata, expected)
        wrong_day = records.copy()
        wrong_day["header"]["trade_date"][0] += 1
        wrong_day = np.frombuffer(wrong_day.tobytes(order="C"), dtype=records.dtype)
        with self.assertRaises(AttachError):
            MdlBatchView.attach(wrong_day, metadata, expected)

        writeable_owner = records.copy()
        disguised_alias = writeable_owner.view()
        disguised_alias.flags.writeable = False
        with self.assertRaises(AttachError):
            MdlBatchView.attach(disguised_alias, metadata, expected)

        with self.assertRaises(ValidationError):
            replace(
                metadata,
                end_canonical_cursor=metadata.begin_canonical_cursor,
            )


if __name__ == "__main__":
    unittest.main()
