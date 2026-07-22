from __future__ import annotations

import hashlib
import json
import unittest
from unittest import mock

from helpers import make_batch
from l2flow_factor import (
    CheckpointError,
    ClockSemantics,
    CursorError,
    FactorSpec,
    FactorTransactionRuntime,
    InputFamily,
    InputMode,
    InputSpec,
    InstrumentShardRouter,
    PluginExecutionError,
    RuntimeOutput,
    ShardOwnershipError,
    decode_runtime_checkpoint,
)


def make_spec() -> FactorSpec:
    return FactorSpec(
        factor_id="runtime_test",
        factor_version="1",
        state_schema_version=1,
        input_mode=InputMode.TICK_ONLY,
        inputs=(InputSpec(1002, InputFamily.TICK, shard_id=1),),
        clock_semantics=ClockSemantics.RECEIVE_MONOTONIC,
    )


def make_mux_spec() -> FactorSpec:
    return FactorSpec(
        factor_id="runtime_test",
        factor_version="1",
        state_schema_version=1,
        input_mode=InputMode.RECEIVE_TIME_MUX,
        inputs=(
            InputSpec(1003, InputFamily.TICK, shard_id=1),
            InputSpec(1002, InputFamily.TICK, shard_id=1),
        ),
        clock_semantics=ClockSemantics.RECEIVE_MONOTONIC,
    )


class CountingPlugin:
    def __init__(self, fail_once: bool = False, spec: FactorSpec | None = None) -> None:
        self.spec = spec or make_spec()
        self.count = 0
        self.fail_once = fail_once
        self.calls = 0

    def serialize_state(self) -> bytes:
        return json.dumps(
            {"count": self.count}, sort_keys=True, separators=(",", ":")
        ).encode("ascii")

    def restore_state(self, state: bytes) -> None:
        value = json.loads(state.decode("ascii"))
        if set(value) != {"count"} or type(value["count"]) is not int:
            raise ValueError("bad state")
        self.count = value["count"]

    def on_batch(self, batch):
        self.calls += 1
        self.count += len(batch.records)
        if self.fail_once:
            self.fail_once = False
            raise RuntimeError("injected")
        metadata = batch.metadata
        return (
            RuntimeOutput(
                (
                    "runtime_test",
                    str(metadata.source_stream_id),
                    str(metadata.end_canonical_cursor),
                ),
                str(self.count).encode("ascii"),
            ),
        )


class Revalidator:
    def __init__(self) -> None:
        self.calls = 0
        self.authorized = True

    def __call__(self, batch) -> bool:
        self.calls += 1
        _ = batch.metadata
        return self.authorized


class RuntimeTests(unittest.TestCase):
    def test_exception_rolls_back_plugin_state_and_cursor(self) -> None:
        plugin = CountingPlugin(fail_once=True)
        revalidator = Revalidator()
        runtime = FactorTransactionRuntime(plugin, 1, revalidator)
        batch, metadata, _, _ = make_batch()
        with batch:
            with self.assertRaises(PluginExecutionError):
                runtime.process_batch(batch)
            self.assertEqual(plugin.count, 0)
            self.assertEqual(runtime.cursors, {})
            outputs = runtime.process_batch(batch)
            self.assertEqual(plugin.count, 2)
            self.assertEqual(len(outputs), 1)
            self.assertEqual(runtime.cursors[metadata.input_key], 12)

    def test_success_commits_once_and_duplicate_cursor_is_rejected(self) -> None:
        plugin = CountingPlugin()
        revalidator = Revalidator()
        runtime = FactorTransactionRuntime(plugin, 1, revalidator)
        batch, _, _, _ = make_batch(batch_quality_flags=1)
        with batch:
            first = runtime.process_batch(batch)
            self.assertEqual(len(first), 1)
            with self.assertRaises(CursorError):
                runtime.process_batch(batch)
            self.assertEqual(plugin.calls, 1)
            self.assertEqual(revalidator.calls, 1)
        next_batch, _, _, _ = make_batch(
            begin_cursor=12,
            origin_wal_start=102,
            batch_quality_flags=2,
            watermark_set_id=10,
        )
        with next_batch:
            runtime.process_batch(next_batch)
        self.assertEqual(plugin.count, 4)
        self.assertEqual(plugin.calls, 2)
        self.assertEqual(revalidator.calls, 2)
        self.assertEqual(runtime.watermark_set.entries[0].input_quality_flags, 3)
        self.assertFalse(hasattr(runtime, "_transactions"))
        self.assertFalse(hasattr(runtime, "_outputs"))

        reused_id, _, _, _ = make_batch(
            begin_cursor=14, origin_wal_start=104, watermark_set_id=10
        )
        with reused_id:
            with self.assertRaises(CursorError):
                runtime.process_batch(reused_id)
        self.assertEqual(plugin.calls, 2)
        self.assertEqual(revalidator.calls, 2)

    def test_external_plugin_mutation_is_rejected_before_execution(self) -> None:
        plugin = CountingPlugin()
        revalidator = Revalidator()
        runtime = FactorTransactionRuntime(plugin, 1, revalidator)
        plugin.count = 7
        batch, _, _, _ = make_batch()
        with batch:
            with self.assertRaises(PluginExecutionError):
                runtime.process_batch(batch)
        self.assertEqual(plugin.calls, 0)
        self.assertEqual(revalidator.calls, 0)

    def test_shard_and_generation_are_single_owner(self) -> None:
        self.assertEqual(InstrumentShardRouter.owner(17), 1)
        plugin = CountingPlugin()
        runtime = FactorTransactionRuntime(plugin, 1, Revalidator())
        wrong_shard, _, _, _ = make_batch(shard_id=2)
        with wrong_shard:
            with self.assertRaises(CursorError):
                runtime.process_batch(wrong_shard)
        first, _, _, _ = make_batch()
        with first:
            runtime.process_batch(first)
        changed, _, _, _ = make_batch(begin_cursor=12, canonical_generation=8)
        with changed:
            with self.assertRaises(CursorError):
                runtime.process_batch(changed)

    def test_run_identity_is_global_but_input_generations_are_per_input(self) -> None:
        alternate_writer = bytes.fromhex("33" * 16)
        plugin = CountingPlugin(spec=make_mux_spec())
        revalidator = Revalidator()
        runtime = FactorTransactionRuntime(plugin, 1, revalidator)
        first, _, _, _ = make_batch(source_stream_id=1002)
        second, _, _, _ = make_batch(
            source_stream_id=1003,
            origin_source_writer_instance=alternate_writer,
            origin_source_generation=19,
            canonical_generation=23,
            watermark_set_id=10,
        )
        with first:
            runtime.process_batch(first)
        with second:
            runtime.process_batch(second)
        self.assertEqual(plugin.calls, 2)
        self.assertEqual(len(runtime.cursors), 2)

        mismatches = (
            {"trade_date": 20260723},
            {"clock_epoch_algorithm": 2},
            {"clock_epoch_digest": bytes.fromhex("44" * 32)},
            {"registry_version": 6},
            {"registry_sha256": bytes.fromhex("55" * 32)},
        )
        for changes in mismatches:
            with self.subTest(changes=tuple(changes)):
                candidate_plugin = CountingPlugin(spec=make_mux_spec())
                candidate_revalidator = Revalidator()
                candidate = FactorTransactionRuntime(
                    candidate_plugin, 1, candidate_revalidator
                )
                committed, _, _, _ = make_batch(source_stream_id=1002)
                rejected, _, _, _ = make_batch(source_stream_id=1003, **changes)
                with committed:
                    candidate.process_batch(committed)
                with rejected:
                    with self.assertRaises(CursorError):
                        candidate.process_batch(rejected)
                self.assertEqual(candidate_plugin.count, 2)
                self.assertEqual(candidate_revalidator.calls, 1)

    def test_runtime_checkpoint_roundtrip_and_type_confusion_rejected(self) -> None:
        plugin = CountingPlugin()
        runtime = FactorTransactionRuntime(plugin, 1, Revalidator())
        batch, metadata, _, _ = make_batch()
        with batch:
            runtime.process_batch(batch)
        encoded = runtime.checkpoint_bytes()
        restored_plugin = CountingPlugin()
        restored = FactorTransactionRuntime(restored_plugin, 1, Revalidator())
        restored.restore_checkpoint_bytes(encoded)
        self.assertEqual(restored_plugin.count, 2)
        self.assertEqual(restored.cursors[metadata.input_key], 12)
        self.assertEqual(restored.run_identity, runtime.run_identity)
        self.assertEqual(restored.watermark_set, runtime.watermark_set)
        continuation, _, _, _ = make_batch(
            begin_cursor=12, origin_wal_start=102, watermark_set_id=10
        )
        with continuation:
            restored.process_batch(continuation)
        self.assertEqual(restored_plugin.count, 4)

        body, _ = encoded[:-32], encoded[-32:]
        value = json.loads(body.decode("ascii"))
        value["shard_id"] = True
        damaged_body = json.dumps(
            value, sort_keys=True, separators=(",", ":")
        ).encode("ascii")
        import hashlib

        damaged = damaged_body + hashlib.sha256(
            b"l2flow.factor.runtime-state.v1\x00" + damaged_body
        ).digest()
        fresh = FactorTransactionRuntime(CountingPlugin(), 1, Revalidator())
        with self.assertRaises(CheckpointError):
            fresh.restore_checkpoint_bytes(damaged)

    def test_runtime_checkpoint_rejects_cursor_permutation_and_clock_split(self) -> None:
        plugin = CountingPlugin(spec=make_mux_spec())
        runtime = FactorTransactionRuntime(plugin, 1, Revalidator())
        first, _, _, _ = make_batch(source_stream_id=1002)
        second, _, _, _ = make_batch(source_stream_id=1003, watermark_set_id=10)
        with first:
            runtime.process_batch(first)
        with second:
            runtime.process_batch(second)
        encoded = runtime.checkpoint_bytes()
        decoded = decode_runtime_checkpoint(encoded)
        self.assertEqual(decoded.watermark_set, runtime.watermark_set)

        def resign(value):
            damaged_body = json.dumps(
                value, sort_keys=True, separators=(",", ":")
            ).encode("ascii")
            return damaged_body + hashlib.sha256(
                b"l2flow.factor.runtime-state.v1\x00" + damaged_body
            ).digest()

        body = json.loads(encoded[:-32].decode("ascii"))
        body["cursors"].reverse()
        with self.assertRaises(CheckpointError):
            decode_runtime_checkpoint(resign(body))

        body = json.loads(encoded[:-32].decode("ascii"))
        body["run_identity"]["clock_epoch_digest"] = "66" * 32
        with self.assertRaises(CheckpointError):
            decode_runtime_checkpoint(resign(body))

    def test_runtime_checkpoint_bounds_raw_plugin_state_before_base64(self) -> None:
        plugin = CountingPlugin()
        runtime = FactorTransactionRuntime(plugin, 1, Revalidator())
        batch, _, _, _ = make_batch()
        with batch:
            runtime.process_batch(batch)
        with mock.patch("l2flow_factor.runtime._MAX_PLUGIN_STATE_BYTES", 1):
            with self.assertRaises(CheckpointError):
                runtime.checkpoint_bytes()


if __name__ == "__main__":
    unittest.main()
