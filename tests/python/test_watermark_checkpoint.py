from __future__ import annotations

from dataclasses import replace
import hashlib
import json
from pathlib import Path
import struct
import tempfile
import unittest

from helpers import make_batch
from l2flow_factor import (
    AtomicCheckpointStore,
    CheckpointError,
    ClockSemantics,
    DurabilityBarrierError,
    FactorCheckpoint,
    FactorInputWatermark,
    FactorInputWatermarkSet,
    FactorSpec,
    FactorTransactionRuntime,
    InputFamily,
    InputMode,
    InputSpec,
    RUNTIME_STATE_CODEC_V1,
    RawNamespace,
    RuntimeOutput,
    ValidationError,
    check_durability_barrier,
    decode_checkpoint,
    encode_checkpoint,
)


DAY1 = bytes.fromhex("00112233445566778899aabbccddeeff")
DAY2 = bytes.fromhex("102132435465768798a9babcbddcedfe")
DIGEST1 = bytes.fromhex("22" * 32)
DIGEST2 = bytes.fromhex("11" * 32)


class RuntimeStatePlugin:
    def __init__(self) -> None:
        self.spec = FactorSpec(
            factor_id="checkpoint_test",
            factor_version="1",
            state_schema_version=1,
            input_mode=InputMode.TICK_ONLY,
            inputs=(InputSpec(1002, InputFamily.TICK, shard_id=1),),
            clock_semantics=ClockSemantics.RECEIVE_MONOTONIC,
        )
        self.count = 0

    def serialize_state(self) -> bytes:
        return json.dumps(
            {"count": self.count}, sort_keys=True, separators=(",", ":")
        ).encode("ascii")

    def restore_state(self, state: bytes) -> None:
        value = json.loads(state.decode("ascii"))
        if set(value) != {"count"} or type(value["count"]) is not int:
            raise ValueError("invalid runtime state")
        self.count = value["count"]

    def on_batch(self, batch):
        self.count += len(batch.records)
        return (
            RuntimeOutput(
                ("checkpoint_test", str(batch.metadata.end_canonical_cursor)),
                str(self.count).encode("ascii"),
            ),
        )


def entry1(**changes):
    values = dict(
        source_stream_id=1001,
        origin_capture_date=20260722,
        origin_stream_day_id=DAY1,
        family=InputFamily.SNAPSHOT,
        shard_id=7,
        canonical_cursor=128877,
        max_consumed_origin_wal_end_pos=4455667788,
        observed_raw_durable_wal_pos=4456000000,
        clock_epoch_algorithm=1,
        clock_epoch_digest=DIGEST1,
        clock_epoch_label=77,
        input_quality_flags=5,
    )
    values.update(changes)
    return FactorInputWatermark(**values)


def entry2(**changes):
    values = dict(
        source_stream_id=1002,
        origin_capture_date=20260722,
        origin_stream_day_id=DAY2,
        family=InputFamily.TICK,
        shard_id=7,
        canonical_cursor=9823411,
        max_consumed_origin_wal_end_pos=8877665544,
        observed_raw_durable_wal_pos=8878000000,
        clock_epoch_algorithm=1,
        clock_epoch_digest=DIGEST2,
        clock_epoch_label=88,
        input_quality_flags=0,
    )
    values.update(changes)
    return FactorInputWatermark(**values)


def watermark_set() -> FactorInputWatermarkSet:
    return FactorInputWatermarkSet(918273, 20260722, (entry2(), entry1()))


class WatermarkTests(unittest.TestCase):
    def test_cross_language_input_identity_golden(self) -> None:
        watermarks = watermark_set()
        self.assertEqual(
            watermarks.input_identity_hex(),
            "f37cca8804904e90882ac3713016e4ff07d42b1942047c841a855daa346a50de",
        )
        changed_observation = FactorInputWatermarkSet(
            999,
            20260722,
            (
                entry1(observed_raw_durable_wal_pos=9999999999, clock_epoch_label=999),
                entry2(observed_raw_durable_wal_pos=9999999999, clock_epoch_label=999),
            ),
        )
        self.assertEqual(
            watermarks.input_identity_hash(), changed_observation.input_identity_hash()
        )
        self.assertNotEqual(
            watermarks.input_identity_hash(),
            FactorInputWatermarkSet(
                918273, 20260722, (entry1(input_quality_flags=4), entry2())
            ).input_identity_hash(),
        )

    def test_entry_parity_and_capacity_validation(self) -> None:
        with self.assertRaises(ValidationError):
            entry1(input_quality_flags=1 << 40)
        with self.assertRaises(ValidationError):
            entry1(canonical_cursor=0)
        with self.assertRaises(ValidationError):
            entry1(max_consumed_origin_wal_end_pos=0)
        with self.assertRaises(ValidationError):
            FactorInputWatermarkSet(1, 20260722, (entry1(),) * 65_537)

    def test_barrier_checks_each_exact_namespace(self) -> None:
        watermarks = watermark_set()
        positions = {
            entry1().namespace: entry1().max_consumed_origin_wal_end_pos,
            entry2().namespace: entry2().max_consumed_origin_wal_end_pos,
        }
        self.assertTrue(check_durability_barrier(watermarks, positions).satisfied)
        lagging = dict(positions)
        lagging[entry2().namespace] -= 1
        result = check_durability_barrier(watermarks, lagging)
        self.assertFalse(result.satisfied)
        self.assertEqual(len(result.failures), 1)
        missing = {entry1().namespace: positions[entry1().namespace]}
        self.assertFalse(check_durability_barrier(watermarks, missing).satisfied)


class CheckpointTests(unittest.TestCase):
    def make_checkpoint(self) -> FactorCheckpoint:
        return FactorCheckpoint(
            factor_id="checkpoint_test",
            factor_version="1",
            factor_code_sha256=bytes.fromhex("33" * 32),
            factor_config_sha256=bytes.fromhex("44" * 32),
            state_schema_sha256=bytes.fromhex("55" * 32),
            registry_version=6,
            registry_sha256=bytes.fromhex("66" * 32),
            watermark_set=watermark_set(),
            state_codec="test-canonical-bytes-v1",
            state_bytes=b"strict-state-bytes",
        )

    def positions(self):
        return {
            entry1().namespace: entry1().max_consumed_origin_wal_end_pos,
            entry2().namespace: entry2().max_consumed_origin_wal_end_pos,
        }

    def test_atomic_publish_load_and_restore_barrier(self) -> None:
        checkpoint = self.make_checkpoint()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "factor.checkpoint"
            with self.assertRaises(CheckpointError):
                AtomicCheckpointStore.publish(path, checkpoint, self.positions())
            digest = AtomicCheckpointStore.publish(
                path,
                checkpoint,
                self.positions(),
                generic_state_watermark_binding_asserted=True,
            )
            self.assertEqual(digest, hashlib.sha256(path.read_bytes()).digest())
            restored = AtomicCheckpointStore.load(
                path,
                expected_factor_id=checkpoint.factor_id,
                expected_factor_version=checkpoint.factor_version,
                expected_factor_code_sha256=checkpoint.factor_code_sha256,
                expected_factor_config_sha256=checkpoint.factor_config_sha256,
                expected_state_schema_sha256=checkpoint.state_schema_sha256,
                expected_registry_version=checkpoint.registry_version,
                expected_registry_sha256=checkpoint.registry_sha256,
                current_durable_positions=self.positions(),
                generic_state_watermark_binding_asserted=True,
            )
            self.assertEqual(restored, checkpoint)
            with self.assertRaises(DurabilityBarrierError):
                AtomicCheckpointStore.load(
                    path,
                    expected_factor_id=checkpoint.factor_id,
                    expected_factor_version=checkpoint.factor_version,
                    expected_factor_code_sha256=checkpoint.factor_code_sha256,
                    expected_factor_config_sha256=checkpoint.factor_config_sha256,
                    expected_state_schema_sha256=checkpoint.state_schema_sha256,
                    expected_registry_version=checkpoint.registry_version,
                    expected_registry_sha256=checkpoint.registry_sha256,
                    current_durable_positions={},
                    generic_state_watermark_binding_asserted=True,
                )
            with self.assertRaises(CheckpointError):
                AtomicCheckpointStore.load(
                    path,
                    expected_factor_id=checkpoint.factor_id,
                    expected_factor_version=checkpoint.factor_version,
                    expected_factor_code_sha256=checkpoint.factor_code_sha256,
                    expected_factor_config_sha256=checkpoint.factor_config_sha256,
                    expected_state_schema_sha256=checkpoint.state_schema_sha256,
                    expected_registry_version=True,
                    expected_registry_sha256=checkpoint.registry_sha256,
                    current_durable_positions=self.positions(),
                    generic_state_watermark_binding_asserted=True,
                )
            for field, value in (
                ("expected_factor_id", "other_factor"),
                ("expected_factor_version", "2"),
            ):
                arguments = {
                    "expected_factor_id": checkpoint.factor_id,
                    "expected_factor_version": checkpoint.factor_version,
                    "expected_factor_code_sha256": checkpoint.factor_code_sha256,
                    "expected_factor_config_sha256": checkpoint.factor_config_sha256,
                    "expected_state_schema_sha256": checkpoint.state_schema_sha256,
                    "expected_registry_version": checkpoint.registry_version,
                    "expected_registry_sha256": checkpoint.registry_sha256,
                    "current_durable_positions": self.positions(),
                    "generic_state_watermark_binding_asserted": True,
                }
                arguments[field] = value
                with self.subTest(field=field), self.assertRaises(CheckpointError):
                    AtomicCheckpointStore.load(path, **arguments)

    def test_runtime_codec_binds_state_to_full_watermark_map(self) -> None:
        plugin = RuntimeStatePlugin()
        runtime = FactorTransactionRuntime(plugin, 1, lambda batch: True)
        batch, _, _, _ = make_batch()
        with batch:
            runtime.process_batch(batch)
        state = runtime.checkpoint_bytes()
        watermark = runtime.watermark_set
        checkpoint = FactorCheckpoint(
            factor_id=plugin.spec.factor_id,
            factor_version=plugin.spec.factor_version,
            factor_code_sha256=bytes.fromhex("33" * 32),
            factor_config_sha256=plugin.spec.sha256(),
            state_schema_sha256=bytes.fromhex("55" * 32),
            registry_version=5,
            registry_sha256=bytes.fromhex("22" * 32),
            watermark_set=watermark,
            state_codec=RUNTIME_STATE_CODEC_V1,
            state_bytes=state,
        )
        positions = {
            entry.namespace: entry.max_consumed_origin_wal_end_pos
            for entry in watermark.entries
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "runtime.checkpoint"
            AtomicCheckpointStore.publish(path, checkpoint, positions)
            restored = AtomicCheckpointStore.load(
                path,
                expected_factor_id=checkpoint.factor_id,
                expected_factor_version=checkpoint.factor_version,
                expected_factor_code_sha256=checkpoint.factor_code_sha256,
                expected_factor_config_sha256=checkpoint.factor_config_sha256,
                expected_state_schema_sha256=checkpoint.state_schema_sha256,
                expected_registry_version=checkpoint.registry_version,
                expected_registry_sha256=checkpoint.registry_sha256,
                current_durable_positions=positions,
            )
            self.assertEqual(restored, checkpoint)

            with self.assertRaises(CheckpointError):
                AtomicCheckpointStore.publish(
                    path,
                    replace(
                        checkpoint,
                        factor_config_sha256=bytes.fromhex("77" * 32),
                    ),
                    positions,
                )

            wrong_entries = (
                replace(
                    watermark.entries[0], max_consumed_origin_wal_end_pos=1
                ),
                # Observation metadata is excluded from input_identity, but
                # the known runtime codec still binds the full outer map.
                replace(
                    watermark.entries[0],
                    observed_raw_durable_wal_pos=(
                        watermark.entries[0].observed_raw_durable_wal_pos + 1
                    ),
                ),
            )
            for wrong_entry in wrong_entries:
                wrong_watermark = FactorInputWatermarkSet(
                    watermark.watermark_set_id,
                    watermark.trade_date,
                    (wrong_entry,),
                )
                mismatched = replace(checkpoint, watermark_set=wrong_watermark)
                with self.assertRaises(CheckpointError):
                    AtomicCheckpointStore.publish(path, mismatched, positions)

    def test_noncanonical_json_and_corruption_are_rejected(self) -> None:
        encoded = encode_checkpoint(self.make_checkpoint())
        payload_length = struct.unpack_from("<Q", encoded, 8)[0]
        payload = encoded[16 : 16 + payload_length] + b" "
        prefix = encoded[:8] + struct.pack("<Q", len(payload))
        noncanonical = prefix + payload + hashlib.sha256(prefix + payload).digest()
        with self.assertRaises(CheckpointError):
            decode_checkpoint(noncanonical)
        corrupted = bytearray(encoded)
        corrupted[-1] ^= 1
        with self.assertRaises(CheckpointError):
            decode_checkpoint(bytes(corrupted))


if __name__ == "__main__":
    unittest.main()
