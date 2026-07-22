from __future__ import annotations

import copy
from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
import pickle
import struct
import tempfile
import threading
import unittest
from unittest.mock import patch

from l2flow_factor import (
    FactorInputWatermark,
    FactorInputWatermarkSet,
    InputFamily,
)
from l2flow_history.manifest import (
    ArtifactEntry,
    ArtifactVisibility,
    AtomicManifestStore,
    CoverageKind,
    DatasetKind,
    HistoryManifest,
    InjectedManifestCrash,
    ManifestConflictError,
    ManifestError,
    ManifestStoreError,
    PartitionSpec,
    SidecarDescriptor,
    SourceNamespace,
    SourceRangeReceipt,
    decode_manifest,
    encode_manifest,
    manifest_sha256,
)
from l2flow_history.watermark_sidecar import (
    SidecarConflictError,
    SidecarError,
    SidecarNamespace,
    SidecarReference,
    SidecarReferenceError,
    WatermarkSidecar,
    decode_sidecar,
    encode_sidecar,
    sidecar_sha256,
    watermark_full_map_sha256,
)


RUN_ID = bytes.fromhex("00112233445566778899aabbccddeeff")
MANIFEST_RUN_ID = bytes.fromhex("102132435465768798a9bacbdcedfe0f")
STREAM_DAY_A = bytes.fromhex("11111111111111111111111111111111")
STREAM_DAY_B = bytes.fromhex("22222222222222222222222222222222")
WRITER_A = bytes.fromhex("33333333333333333333333333333333")
CERT_A = hashlib.sha256(b"common-cut-a").digest()
CERT_B = hashlib.sha256(b"common-cut-b").digest()


def _digest(label: str) -> bytes:
    return hashlib.sha256(label.encode("ascii")).digest()


def _entry(
    source_stream_id: int,
    family: InputFamily,
    *,
    stream_day_id: bytes = STREAM_DAY_A,
    shard_id: int = 70_000,
    cursor: int = 100,
    observed: int = 180,
    label: int = 7,
) -> FactorInputWatermark:
    return FactorInputWatermark(
        source_stream_id=source_stream_id,
        origin_capture_date=20260722,
        origin_stream_day_id=stream_day_id,
        family=family,
        shard_id=shard_id,
        canonical_cursor=cursor,
        max_consumed_origin_wal_end_pos=cursor + 20,
        observed_raw_durable_wal_pos=observed,
        clock_epoch_algorithm=1,
        clock_epoch_digest=_digest(f"clock-{source_stream_id}"),
        clock_epoch_label=label,
        input_quality_flags=0x21,
    )


def _watermark_set(
    watermark_set_id: int = 11,
    *,
    observed_delta: int = 0,
    label_delta: int = 0,
    reverse_entries: bool = False,
) -> FactorInputWatermarkSet:
    entries = (
        _entry(
            20,
            InputFamily.SNAPSHOT,
            stream_day_id=STREAM_DAY_B,
            cursor=200,
            observed=300 + observed_delta,
            label=8 + label_delta,
        ),
        _entry(
            10,
            InputFamily.TICK,
            observed=180 + observed_delta,
            label=7 + label_delta,
        ),
    )
    if reverse_entries:
        entries = tuple(reversed(entries))
    return FactorInputWatermarkSet(
        watermark_set_id=watermark_set_id,
        trade_date=20260722,
        entries=entries,
    )


def _sidecar(*, reverse_sets: bool = False) -> WatermarkSidecar:
    sets = (_watermark_set(11), _watermark_set(12, observed_delta=1))
    if reverse_sets:
        sets = tuple(reversed(sets))
    return WatermarkSidecar(
        namespace=SidecarNamespace(RUN_ID, 3),
        watermark_sets=sets,
    )


def _source_namespace(
    *,
    source_stream_id: int = 10,
    stream_day_id: bytes = STREAM_DAY_A,
    shard_id: int = 70_000,
) -> SourceNamespace:
    return SourceNamespace(
        trade_date=20260722,
        source_stream_id=source_stream_id,
        origin_capture_date=20260722,
        origin_stream_day_id=stream_day_id,
        family=InputFamily.TICK,
        shard_id=shard_id,
        origin_source_writer_instance=WRITER_A,
        origin_source_generation=4,
        canonical_generation=8,
        clock_epoch_algorithm=1,
        clock_epoch_digest=_digest("source-clock"),
        schema_sha256=_digest("schema"),
        dtype_sha256=_digest("dtype"),
        registry_version=9,
        registry_sha256=_digest("registry"),
        normalizer_build_sha256=_digest("normalizer-build"),
        normalizer_config_sha256=_digest("normalizer-config"),
    )


def _receipt(
    *,
    namespace: SourceNamespace | None = None,
    begin: int = 100,
    end: int = 200,
    coverage: CoverageKind = CoverageKind.SEALED_COMMON_CUT,
    certificate: bytes | None = CERT_A,
) -> SourceRangeReceipt:
    if coverage is CoverageKind.STAGING:
        certificate = None
    return SourceRangeReceipt(
        namespace=_source_namespace() if namespace is None else namespace,
        begin_canonical_cursor=begin,
        end_canonical_cursor=end,
        first_origin_ingress_sequence=begin + 1,
        last_origin_ingress_sequence=end + 1,
        min_origin_wal_end_pos=begin + 10,
        max_origin_wal_end_pos=end + 10,
        coverage_kind=coverage,
        coverage_certificate_sha256=certificate,
    )


def _canonical_artifact(
    data: bytes,
    *,
    name: str = "part-canonical.parquet",
    visibility: ArtifactVisibility = ArtifactVisibility.VISIBLE,
    receipts: tuple[SourceRangeReceipt, ...] | None = None,
    row_count: int = 4,
) -> ArtifactEntry:
    partition = PartitionSpec(
        dataset=DatasetKind.CANONICAL,
        trade_date=20260722,
        market="SH",
        event_type="tick",
        bucket=7,
    )
    if receipts is None:
        receipts = (_receipt(),)
    return ArtifactEntry(
        artifact_type=DatasetKind.CANONICAL,
        relative_path=partition.relative_prefix + name,
        file_sha256=hashlib.sha256(data).digest(),
        file_size_bytes=len(data),
        row_count=row_count,
        logical_content_sha256=_digest("canonical-logical"),
        min_sort_key_v1=b"canonical-min-sort-key",
        max_sort_key_v1=b"canonical-max-sort-key",
        partition=partition,
        source_receipts=receipts,
        visibility=visibility,
    )


def _factor_artifact(
    data: bytes,
    reference: SidecarReference,
    *,
    name: str = "part-factor.parquet",
    visibility: ArtifactVisibility = ArtifactVisibility.VISIBLE,
    receipts: tuple[SourceRangeReceipt, ...] | None = None,
    row_count: int = 3,
) -> ArtifactEntry:
    partition = PartitionSpec(
        dataset=DatasetKind.FACTOR,
        trade_date=20260722,
        factor_group="microstructure",
        factor_version="v1.0",
        bucket=7,
    )
    if receipts is None:
        receipts = (_receipt(),)
    return ArtifactEntry(
        artifact_type=DatasetKind.FACTOR,
        relative_path=partition.relative_prefix + name,
        file_sha256=hashlib.sha256(data).digest(),
        file_size_bytes=len(data),
        row_count=row_count,
        logical_content_sha256=_digest("factor-logical"),
        min_sort_key_v1=b"factor-min-sort-key",
        max_sort_key_v1=b"factor-max-sort-key",
        partition=partition,
        source_receipts=receipts,
        factor_group_membership_sha256=_digest("factor-group-membership"),
        factor_sidecar_refs=(reference,),
        visibility=visibility,
    )


def _envelope_payload(blob: bytes) -> bytes:
    length = struct.unpack_from("<Q", blob, 8)[0]
    return blob[16 : 16 + length]


def _resign_payload(blob: bytes, payload: bytes) -> bytes:
    prefix = blob[:8] + struct.pack("<Q", len(payload))
    return prefix + payload + hashlib.sha256(prefix + payload).digest()


def _resign_json(blob: bytes, value: object) -> bytes:
    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return _resign_payload(blob, payload)


def _write_file(root: Path, relative_path: str, data: bytes) -> None:
    target = root.joinpath(*relative_path.split("/"))
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(data)
    target.chmod(0o600)


class WatermarkSidecarTests(unittest.TestCase):
    def test_canonical_order_full_observation_and_golden_hash(self) -> None:
        first = _sidecar(reverse_sets=False)
        second = _sidecar(reverse_sets=True)
        self.assertEqual(encode_sidecar(first), encode_sidecar(second))
        self.assertEqual(decode_sidecar(encode_sidecar(first)), first)
        self.assertEqual(
            first.watermark_sets[0].entries[0].observed_raw_durable_wal_pos,
            180,
        )
        self.assertEqual(first.watermark_sets[0].entries[0].clock_epoch_label, 7)
        self.assertEqual(
            sidecar_sha256(first).hex(),
            "629575f9e462f838c83ecbcdde627c0b6f672d6fcb90dbdc9da15cb09be7184d",
        )

    def test_stable_identity_excludes_observations_but_full_map_does_not(self) -> None:
        first = _watermark_set(11)
        observed_change = _watermark_set(11, observed_delta=5, label_delta=9)
        self.assertEqual(first.input_identity_hash(), observed_change.input_identity_hash())
        self.assertNotEqual(
            watermark_full_map_sha256(first),
            watermark_full_map_sha256(observed_change),
        )

    def test_id_reuse_and_reference_mismatches_fail_closed(self) -> None:
        namespace = SidecarNamespace(RUN_ID, 3)
        first = _watermark_set(11)
        changed = _watermark_set(11, observed_delta=1)
        with self.assertRaises(SidecarConflictError):
            WatermarkSidecar(namespace, (first, changed))
        sidecar = WatermarkSidecar(namespace, (first,))
        reference = sidecar.reference_for(11)
        self.assertEqual(sidecar.resolve(reference), first)
        with self.assertRaises(SidecarReferenceError):
            sidecar.resolve(replace(reference, watermark_set_id=99))
        with self.assertRaises(SidecarReferenceError):
            sidecar.resolve(
                replace(
                    reference,
                    namespace=SidecarNamespace(bytes.fromhex("ff" * 16), 3),
                )
            )
        with self.assertRaises(SidecarReferenceError):
            sidecar.resolve(
                replace(reference, input_identity_sha256=_digest("wrong-input"))
            )
        with self.assertRaises(SidecarReferenceError):
            sidecar.resolve(
                replace(reference, full_map_sha256=_digest("wrong-full-map"))
            )
        with self.assertRaises(SidecarReferenceError):
            sidecar.resolve(reference, expected_full_map=changed)
        with self.assertRaises(SidecarConflictError):
            sidecar.with_appended((first,))
        with patch(
            "l2flow_history.watermark_sidecar._MAX_SIDECAR_SETS",
            2,
        ):
            with self.assertRaisesRegex(SidecarError, "V1 bound"):
                sidecar.with_appended(
                    item for item in (_watermark_set(12), _watermark_set(13))
                )

    def test_strict_envelope_duplicate_unknown_type_order_and_hash(self) -> None:
        encoded = encode_sidecar(_sidecar())
        tampered = bytearray(encoded)
        tampered[-1] ^= 1
        with self.assertRaises(SidecarError):
            decode_sidecar(bytes(tampered))

        root = json.loads(_envelope_payload(encoded))
        root["unknown"] = 1
        with self.assertRaises(SidecarError):
            decode_sidecar(_resign_json(encoded, root))

        root = json.loads(_envelope_payload(encoded))
        root["namespace"]["table_generation"] = True
        with self.assertRaises(SidecarError):
            decode_sidecar(_resign_json(encoded, root))

        root = json.loads(_envelope_payload(encoded))
        root["sets"] = list(reversed(root["sets"]))
        with self.assertRaises(SidecarError):
            decode_sidecar(_resign_json(encoded, root))

        payload = _envelope_payload(encoded)
        duplicate = payload.replace(
            b'{"encoding":',
            b'{"encoding":"l2flow-watermark-sidecar-v1","encoding":',
            1,
        )
        with self.assertRaises(SidecarError):
            decode_sidecar(_resign_payload(encoded, duplicate))


class ManifestModelTests(unittest.TestCase):
    def test_partition_market_rules_and_configured_shard_width(self) -> None:
        PartitionSpec(DatasetKind.CANONICAL, 20260722, 0, "SH", "tick")
        PartitionSpec(DatasetKind.CANONICAL, 20260722, 0, "SZ", "snapshot")
        PartitionSpec(DatasetKind.CANONICAL, 20260722, 0, "GLOBAL", "quality")
        PartitionSpec(DatasetKind.CANONICAL, 20260722, 0, "GLOBAL", "control")
        with self.assertRaises(ManifestError):
            PartitionSpec(DatasetKind.CANONICAL, 20260722, 0, "GLOBAL", "tick")
        with self.assertRaises(ManifestError):
            PartitionSpec(DatasetKind.CANONICAL, 20260722, 0, "SH", "control")
        self.assertEqual(_source_namespace(shard_id=70_000).shard_id, 70_000)

    def test_safe_relative_paths_reject_traversal_and_wrong_partition(self) -> None:
        data = b"opaque artifact bytes"
        artifact = _canonical_artifact(data)
        for unsafe in (
            "/absolute.parquet",
            "../escape.parquet",
            "canonical/../escape.parquet",
            "canonical\\escape.parquet",
            "canonical//escape.parquet",
        ):
            with self.subTest(unsafe=unsafe), self.assertRaises(ManifestError):
                replace(artifact, relative_path=unsafe)
        with self.assertRaises(ManifestError):
            replace(
                artifact,
                relative_path="canonical/trade_date=20260722/market=SZ/"
                "event_type=tick/bucket=07/part.parquet",
            )
        with self.assertRaises(ManifestError):
            replace(
                artifact,
                relative_path=artifact.partition.relative_prefix
                + "extra/part-canonical.parquet",
            )

    def test_visibility_requires_one_shared_sealed_common_cut(self) -> None:
        data = b"opaque artifact bytes"
        with self.assertRaises(ManifestError):
            _canonical_artifact(
                data,
                receipts=(_receipt(coverage=CoverageKind.STAGING),),
            )
        other_namespace = _source_namespace(
            source_stream_id=11,
            stream_day_id=STREAM_DAY_B,
        )
        with self.assertRaises(ManifestConflictError):
            _canonical_artifact(
                data,
                receipts=(
                    _receipt(certificate=CERT_A),
                    _receipt(namespace=other_namespace, certificate=CERT_B),
                ),
            )

    def test_exact_scopes_are_not_aggregated_and_same_scope_overlap_rejected(self) -> None:
        data = b"opaque artifact bytes"
        first = _receipt(begin=100, end=200)
        second_scope = _receipt(
            namespace=_source_namespace(
                source_stream_id=11,
                stream_day_id=STREAM_DAY_B,
            ),
            begin=100,
            end=200,
        )
        artifact = _canonical_artifact(data, receipts=(second_scope, first))
        self.assertEqual(len(artifact.source_receipts), 2)
        self.assertNotEqual(
            artifact.source_receipts[0].namespace,
            artifact.source_receipts[1].namespace,
        )
        with self.assertRaises(ManifestConflictError):
            _canonical_artifact(
                data,
                receipts=(first, _receipt(begin=150, end=250)),
            )
        with self.assertRaises(ManifestError):
            _receipt(begin=100, end=100)
        with self.assertRaises(ManifestError):
            replace(artifact, row_count=0)
        with self.assertRaises(ManifestError):
            replace(artifact, row_count=1_000_001)
        with self.assertRaises(ManifestError):
            replace(artifact, file_size_bytes=11)
        with self.assertRaises(ManifestError):
            replace(artifact, file_size_bytes=2 * 1024 * 1024 * 1024 + 1)

    def test_persistent_sidecar_descriptor_and_orphan_rejection(self) -> None:
        sidecar = _sidecar()
        descriptor = SidecarDescriptor.from_sidecar("watermarks/run.l2ws", sidecar)
        artifact = _factor_artifact(
            b"factor artifact bytes", sidecar.reference_for(11)
        )
        with self.assertRaises(ManifestConflictError):
            HistoryManifest(MANIFEST_RUN_ID, 1, None, (artifact,))
        manifest = HistoryManifest(
            MANIFEST_RUN_ID,
            1,
            None,
            (artifact,),
            (descriptor,),
        )
        manifest.validate_sidecars({sidecar.namespace: sidecar})
        extra = WatermarkSidecar(
            SidecarNamespace(RUN_ID, 4),
            sidecar.watermark_sets,
        )

        class LyingMapping(dict):
            def __len__(self) -> int:
                return 0

        with patch("l2flow_history.manifest._MAX_SIDECAR_DESCRIPTORS", 1):
            with self.assertRaisesRegex(ManifestError, "exceeds the V1 bound"):
                manifest.validate_sidecars(
                    LyingMapping(
                        {
                            sidecar.namespace: sidecar,
                            extra.namespace: extra,
                        }
                    )
                )
        with self.assertRaises(ManifestConflictError):
            manifest.validate_sidecars({})
        wrong = _sidecar(reverse_sets=True)
        wrong = WatermarkSidecar(
            wrong.namespace,
            (wrong.watermark_sets[0],),
        )
        with self.assertRaises(ManifestConflictError):
            manifest.validate_sidecars({sidecar.namespace: wrong})

    def test_manifest_canonical_order_roundtrip_and_golden_hash(self) -> None:
        sidecar = _sidecar()
        descriptor = SidecarDescriptor.from_sidecar("watermarks/run.l2ws", sidecar)
        canonical = _canonical_artifact(b"canonical bytes")
        factor = _factor_artifact(
            b"factor artifact bytes", sidecar.reference_for(11)
        )
        first = HistoryManifest(
            MANIFEST_RUN_ID,
            1,
            None,
            (factor, canonical),
            (descriptor,),
        )
        second = HistoryManifest(
            MANIFEST_RUN_ID,
            1,
            None,
            (canonical, factor),
            (descriptor,),
        )
        self.assertEqual(encode_manifest(first), encode_manifest(second))
        self.assertEqual(decode_manifest(encode_manifest(first)), first)
        self.assertEqual(
            manifest_sha256(first).hex(),
            "6a5aaf244b0b44b68b0e0c052e15975a8917c7169a857ddec1888318f86f9c24",
        )

    def test_manifest_strict_duplicate_unknown_type_order_and_hash(self) -> None:
        manifest = HistoryManifest(
            MANIFEST_RUN_ID,
            1,
            None,
            (
                _canonical_artifact(b"first artifact", name="part-z.parquet"),
                _canonical_artifact(b"second artifact", name="part-a.parquet"),
            ),
        )
        encoded = encode_manifest(manifest)
        tampered = bytearray(encoded)
        tampered[-1] ^= 1
        with self.assertRaises(ManifestError):
            decode_manifest(bytes(tampered))

        root = json.loads(_envelope_payload(encoded))
        root["unknown"] = 1
        with self.assertRaises(ManifestError):
            decode_manifest(_resign_json(encoded, root))

        root = json.loads(_envelope_payload(encoded))
        root["generation"] = True
        with self.assertRaises(ManifestError):
            decode_manifest(_resign_json(encoded, root))

        root = json.loads(_envelope_payload(encoded))
        root["artifacts"][0]["min_sort_key_v1"] = ""
        with self.assertRaises(ManifestError):
            decode_manifest(_resign_json(encoded, root))

        root = json.loads(_envelope_payload(encoded))
        root["artifacts"][0]["file_size_bytes"] = 11
        with self.assertRaises(ManifestError):
            decode_manifest(_resign_json(encoded, root))

        root = json.loads(_envelope_payload(encoded))
        root["artifacts"][0]["file_size_bytes"] = 2 * 1024 * 1024 * 1024 + 1
        with self.assertRaises(ManifestError):
            decode_manifest(_resign_json(encoded, root))

        root = json.loads(_envelope_payload(encoded))
        root["artifacts"][0]["row_count"] = 1_000_001
        with self.assertRaises(ManifestError):
            decode_manifest(_resign_json(encoded, root))

        root = json.loads(_envelope_payload(encoded))
        root["artifacts"] = list(reversed(root["artifacts"]))
        with self.assertRaises(ManifestError):
            decode_manifest(_resign_json(encoded, root))

        payload = _envelope_payload(encoded)
        duplicate = payload.replace(
            b'{"artifacts":',
            b'{"artifacts":[],"artifacts":',
            1,
        )
        with self.assertRaises(ManifestError):
            decode_manifest(_resign_payload(encoded, duplicate))

    def test_successor_is_append_only_with_one_way_staging_promotion(self) -> None:
        visible = _canonical_artifact(
            b"visible artifact", name="part-visible.parquet"
        )
        staging_receipt = _receipt(coverage=CoverageKind.STAGING)
        staging = _canonical_artifact(
            b"staging artifact",
            name="part-staging.parquet",
            visibility=ArtifactVisibility.STAGING,
            receipts=(staging_receipt,),
        )
        first = HistoryManifest(
            MANIFEST_RUN_ID,
            1,
            None,
            (visible, staging),
        )
        predecessor = manifest_sha256(first)
        promoted = replace(
            staging,
            visibility=ArtifactVisibility.VISIBLE,
            source_receipts=(_receipt(),),
        )
        second = HistoryManifest(
            MANIFEST_RUN_ID,
            2,
            predecessor,
            (visible, promoted),
        )
        second.validate_successor(first)

        with self.assertRaises(ManifestConflictError):
            HistoryManifest(
                MANIFEST_RUN_ID,
                2,
                predecessor,
                (visible,),
            ).validate_successor(first)
        with self.assertRaises(ManifestConflictError):
            HistoryManifest(
                MANIFEST_RUN_ID,
                2,
                predecessor,
                (replace(visible, row_count=5), staging),
            ).validate_successor(first)
        with self.assertRaises(ManifestConflictError):
            HistoryManifest(
                MANIFEST_RUN_ID,
                2,
                predecessor,
                (visible, replace(staging, row_count=5)),
            ).validate_successor(first)
        with self.assertRaises(ManifestConflictError):
            HistoryManifest(
                MANIFEST_RUN_ID,
                2,
                predecessor,
                (visible, replace(staging, min_sort_key_v1=b"changed-min")),
            ).validate_successor(first)
        with self.assertRaises(ManifestConflictError):
            HistoryManifest(
                MANIFEST_RUN_ID,
                3,
                manifest_sha256(second),
                (replace(visible, visibility=ArtifactVisibility.STAGING), promoted),
            ).validate_successor(second)


class AtomicManifestStoreTests(unittest.TestCase):
    def _store_fixture(
        self,
        root: Path,
    ) -> tuple[
        AtomicManifestStore,
        WatermarkSidecar,
        SidecarDescriptor,
        ArtifactEntry,
    ]:
        store = AtomicManifestStore(root)
        sidecar = _sidecar()
        descriptor = store.publish_sidecar(sidecar, relative_path="run.l2ws")
        artifact_data = b"opaque bytes; the manifest store does not call this Parquet"
        artifact = _factor_artifact(artifact_data, sidecar.reference_for(11))
        _write_file(root, artifact.relative_path, artifact_data)
        return store, sidecar, descriptor, artifact

    def test_store_pins_one_absolute_root_and_rejects_symlink_traversal(self) -> None:
        with self.assertRaisesRegex(ManifestStoreError, "normalized absolute"):
            AtomicManifestStore(".")

        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            real = parent / "real"
            target = real / "store"
            target.mkdir(parents=True)
            os.symlink(real, parent / "link")
            with self.assertRaisesRegex(ManifestStoreError, "securely traverse"):
                AtomicManifestStore(parent / "link" / "store")

        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            configured = parent / "root"
            configured.mkdir()
            store = AtomicManifestStore(configured)
            with self.assertRaisesRegex(ManifestStoreError, "cannot be copied"):
                copy.copy(store)
            with self.assertRaisesRegex(ManifestStoreError, "cannot be copied"):
                copy.deepcopy(store)
            with self.assertRaisesRegex(ManifestStoreError, "cannot be serialized"):
                pickle.dumps(store)
            retained = parent / "retained"
            os.rename(configured, retained)
            configured.mkdir()
            store.publish_sidecar(_sidecar(), relative_path="run.l2ws")
            self.assertTrue((retained / "run.l2ws").is_file())
            self.assertFalse((configured / "run.l2ws").exists())
            store.close()
            with self.assertRaisesRegex(ManifestStoreError, "closed"):
                store.load_current()

            removed = parent / "removed"
            removed.mkdir()
            removed_store = AtomicManifestStore(removed)
            os.rmdir(removed)
            with self.assertRaisesRegex(ManifestStoreError, "identity changed"):
                removed_store.load_current()
            removed_store.close()

            concurrent_root = parent / "concurrent"
            concurrent_root.mkdir()
            concurrent_store = AtomicManifestStore(concurrent_root)
            start = threading.Barrier(3)
            errors: list[BaseException] = []

            def close_concurrently() -> None:
                start.wait()
                try:
                    concurrent_store.close()
                except BaseException as error:
                    errors.append(error)

            threads = tuple(
                threading.Thread(target=close_concurrently) for _ in range(2)
            )
            for thread in threads:
                thread.start()
            start.wait()
            for thread in threads:
                thread.join()
            self.assertEqual(errors, [])
            with self.assertRaisesRegex(ManifestStoreError, "closed"):
                concurrent_store.load_current()

    def test_publish_sidecar_manifest_cas_and_strict_readback(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store, sidecar, descriptor, artifact = self._store_fixture(root)
            first = HistoryManifest(
                MANIFEST_RUN_ID,
                1,
                None,
                (artifact,),
                (descriptor,),
            )
            first_digest = store.publish(
                first,
                expected_current_generation=None,
                expected_current_sha256=None,
                sidecars={sidecar.namespace: sidecar},
            )
            self.assertEqual(store.load_current(), (first, first_digest))
            self.assertEqual(
                store.load_current_with_sidecars(),
                (first, first_digest, (sidecar,)),
            )

            staging = _canonical_artifact(
                b"not yet published",
                name="part-staging.parquet",
                visibility=ArtifactVisibility.STAGING,
                receipts=(_receipt(coverage=CoverageKind.STAGING),),
            )
            second = HistoryManifest(
                MANIFEST_RUN_ID,
                2,
                first_digest,
                (artifact, staging),
                (descriptor,),
            )
            second_digest = store.publish(
                second,
                expected_current_generation=1,
                expected_current_sha256=first_digest,
                sidecars={sidecar.namespace: sidecar},
            )
            self.assertEqual(store.load_current(), (second, second_digest))

            competing = HistoryManifest(
                MANIFEST_RUN_ID,
                3,
                second_digest,
                (artifact, staging),
                (descriptor,),
            )
            with self.assertRaises(ManifestConflictError):
                store.publish(
                    competing,
                    expected_current_generation=1,
                    expected_current_sha256=first_digest,
                    sidecars={sidecar.namespace: sidecar},
                )
            with self.assertRaises(ManifestConflictError):
                store.publish(
                    competing,
                    expected_current_generation=1,
                    expected_current_sha256=second_digest,
                    sidecars={sidecar.namespace: sidecar},
                )

            (root / descriptor.relative_path).write_bytes(b"tampered sidecar")
            with self.assertRaises(ManifestStoreError):
                store.load_current_with_sidecars()

    def test_publish_rejects_missing_tampered_hardlinked_and_symlinked_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = AtomicManifestStore(root)
            data = b"opaque artifact"
            artifact = _canonical_artifact(data)
            manifest = HistoryManifest(MANIFEST_RUN_ID, 1, None, (artifact,))
            with self.assertRaises(ManifestStoreError):
                store.publish(
                    manifest,
                    expected_current_generation=None,
                    expected_current_sha256=None,
                    sidecars={},
                )

            _write_file(root, artifact.relative_path, b"wrong bytes same-ish")
            with self.assertRaises(ManifestStoreError):
                store.publish(
                    manifest,
                    expected_current_generation=None,
                    expected_current_sha256=None,
                    sidecars={},
                )

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = AtomicManifestStore(root)
            data = b"opaque artifact"
            artifact = _canonical_artifact(data)
            _write_file(root, artifact.relative_path, data)
            target = root.joinpath(*artifact.relative_path.split("/"))
            os.link(target, target.with_name("second-link.parquet"))
            manifest = HistoryManifest(MANIFEST_RUN_ID, 1, None, (artifact,))
            with self.assertRaises(ManifestStoreError):
                store.publish(
                    manifest,
                    expected_current_generation=None,
                    expected_current_sha256=None,
                    sidecars={},
                )

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = AtomicManifestStore(root)
            data = b"opaque artifact"
            artifact = _canonical_artifact(data)
            _write_file(root, artifact.relative_path, data)
            root.joinpath(*artifact.relative_path.split("/")).chmod(0o644)
            manifest = HistoryManifest(MANIFEST_RUN_ID, 1, None, (artifact,))
            with self.assertRaisesRegex(
                ManifestStoreError,
                "single-link regular file",
            ):
                store.publish(
                    manifest,
                    expected_current_generation=None,
                    expected_current_sha256=None,
                    sidecars={},
                )

        with tempfile.TemporaryDirectory() as directory, tempfile.TemporaryDirectory() as outside:
            root = Path(directory)
            store = AtomicManifestStore(root)
            data = b"opaque artifact"
            artifact = _canonical_artifact(data)
            outside_target = Path(outside).joinpath(*artifact.relative_path.split("/")[1:])
            outside_target.parent.mkdir(parents=True)
            outside_target.write_bytes(data)
            os.symlink(outside, root / "canonical")
            manifest = HistoryManifest(MANIFEST_RUN_ID, 1, None, (artifact,))
            with self.assertRaises(ManifestStoreError):
                store.publish(
                    manifest,
                    expected_current_generation=None,
                    expected_current_sha256=None,
                    sidecars={},
                )

    def test_sidecar_publication_is_noreplace_and_crash_retry_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sidecar = _sidecar()

            def crash(stage: str) -> None:
                if stage == "after_sidecar_noreplace":
                    raise InjectedManifestCrash(stage)

            crashing = AtomicManifestStore(root, fault_injector=crash)
            with self.assertRaises(InjectedManifestCrash):
                crashing.publish_sidecar(sidecar, relative_path="run.l2ws")
            descriptor = AtomicManifestStore(root).publish_sidecar(
                sidecar,
                relative_path="run.l2ws",
            )
            self.assertEqual((root / "run.l2ws").read_bytes(), encode_sidecar(sidecar))
            self.assertEqual(descriptor.namespace, sidecar.namespace)
            conflicting = WatermarkSidecar(
                sidecar.namespace,
                (_watermark_set(99),),
            )
            with self.assertRaises(ManifestConflictError):
                AtomicManifestStore(root).publish_sidecar(
                    conflicting,
                    relative_path="run.l2ws",
                )

    def test_noreplace_publication_refuses_crash_unsafe_link_unlink_fallback(self) -> None:
        class LibcWithoutRenameAt2:
            pass

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "watermarks").mkdir(mode=0o700)
            store = AtomicManifestStore(root)
            with patch(
                "l2flow_history.manifest.ctypes.CDLL",
                return_value=LibcWithoutRenameAt2(),
            ):
                with self.assertRaisesRegex(
                    ManifestStoreError,
                    "sidecar publication failed",
                ):
                    store.publish_sidecar(
                        _sidecar(),
                        relative_path="watermarks/run.l2ws",
                    )
            self.assertFalse((root / "watermarks" / "run.l2ws").exists())
            self.assertEqual(tuple((root / "watermarks").iterdir()), ())

        with tempfile.TemporaryDirectory() as directory, tempfile.TemporaryDirectory() as outside:
            root = Path(directory)
            os.symlink(outside, root / "watermarks")
            with self.assertRaises(ManifestStoreError):
                AtomicManifestStore(root).publish_sidecar(
                    _sidecar(),
                    relative_path="watermarks/run.l2ws",
                )
            self.assertFalse(Path(outside, "run.l2ws").exists())

    def test_manifest_crash_windows_keep_or_advance_current_unambiguously(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = b"opaque artifact"
            artifact = _canonical_artifact(data)
            _write_file(root, artifact.relative_path, data)
            first = HistoryManifest(MANIFEST_RUN_ID, 1, None, (artifact,))
            first_digest = AtomicManifestStore(root).publish(
                first,
                expected_current_generation=None,
                expected_current_sha256=None,
                sidecars={},
            )
            staging_a = _canonical_artifact(
                b"stage-a artifact",
                name="part-stage-a.parquet",
                visibility=ArtifactVisibility.STAGING,
                receipts=(_receipt(coverage=CoverageKind.STAGING),),
            )
            second = HistoryManifest(
                MANIFEST_RUN_ID,
                2,
                first_digest,
                (artifact, staging_a),
            )

            def before_current(stage: str) -> None:
                if stage == "after_manifest_directory_fsync":
                    raise InjectedManifestCrash(stage)

            with self.assertRaises(InjectedManifestCrash):
                AtomicManifestStore(root, fault_injector=before_current).publish(
                    second,
                    expected_current_generation=1,
                    expected_current_sha256=first_digest,
                    sidecars={},
                )
            self.assertEqual(AtomicManifestStore(root).load_current(), (first, first_digest))
            second_digest = AtomicManifestStore(root).publish(
                second,
                expected_current_generation=1,
                expected_current_sha256=first_digest,
                sidecars={},
            )

            staging_b = _canonical_artifact(
                b"stage-b artifact",
                name="part-stage-b.parquet",
                visibility=ArtifactVisibility.STAGING,
                receipts=(_receipt(coverage=CoverageKind.STAGING),),
            )
            third = HistoryManifest(
                MANIFEST_RUN_ID,
                3,
                second_digest,
                (artifact, staging_a, staging_b),
            )

            def after_current(stage: str) -> None:
                if stage == "after_current_replace":
                    raise InjectedManifestCrash(stage)

            with self.assertRaises(InjectedManifestCrash):
                AtomicManifestStore(root, fault_injector=after_current).publish(
                    third,
                    expected_current_generation=2,
                    expected_current_sha256=second_digest,
                    sidecars={},
                )
            loaded = AtomicManifestStore(root).load_current()
            self.assertIsNotNone(loaded)
            assert loaded is not None
            self.assertEqual(loaded[0], third)
            third_digest = loaded[1]
            self.assertEqual(
                AtomicManifestStore(root).publish(
                    third,
                    expected_current_generation=2,
                    expected_current_sha256=second_digest,
                    sidecars={},
                ),
                third_digest,
            )


if __name__ == "__main__":
    unittest.main()
