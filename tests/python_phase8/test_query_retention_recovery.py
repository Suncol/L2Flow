from __future__ import annotations

from dataclasses import replace
import hashlib
import itertools
import os
from pathlib import Path
import struct
import sys
import tempfile
import unittest
from unittest import mock


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPOSITORY_ROOT / "python"))

from l2flow_factor.canonical import InputFamily  # noqa: E402
from l2flow_factor.watermark import (  # noqa: E402
    FactorInputWatermark,
    FactorInputWatermarkSet,
)
from l2flow_history import parquet_backend  # noqa: E402
from l2flow_history.manifest import (  # noqa: E402
    ArtifactEntry,
    ArtifactVisibility,
    CoverageKind,
    DatasetKind,
    HistoryManifest,
    PartitionSpec,
    SidecarDescriptor,
    SourceNamespace,
    SourceRangeReceipt,
)
from l2flow_history.model import (  # noqa: E402
    CanonicalArchiveRow,
    FactorHistoryRow,
    FactorImplementationStatus,
    canonical_logical_rows_sha256,
    canonical_sort_key_wire_v1,
    factor_logical_rows_sha256,
    factor_sort_key_wire_v1,
)
from l2flow_history.query import (  # noqa: E402
    CanonicalLineageCatalog,
    FactorLineageCatalog,
    FactorLiveSource,
    FixedQuerySnapshot,
    HotCanonicalSource,
    LatestHistoryMixError,
    LineageResolutionError,
    QueryBudget,
    QueryBudgetExceeded,
    QueryConflictError,
    QuerySnapshotChanged,
    QueryValidationError,
    StorageTier,
    merge_canonical_history,
    merge_factor_history,
    query_latest_current,
    read_manifest_visible_canonical_rows,
)
from l2flow_history import query as query_module  # noqa: E402
from l2flow_history.retention import (  # noqa: E402
    ActiveReference,
    ActiveReferenceKind,
    ActiveReferenceSnapshot,
    ArtifactKind,
    BackupEvidence,
    CanonicalCursorScope,
    ConsumerCheckpoint,
    ConsumerRegistrySnapshot,
    PublicationEvidence,
    RawWalScope,
    RegisteredConsumer,
    RetentionApproval,
    RetentionArtifact,
    RetentionGate,
    RetentionValidationError,
    SidecarOwnerKind,
    SidecarReference as RetentionSidecarReference,
    SidecarReferenceSnapshot,
    plan_retention,
)
from l2flow_history import retention as retention_module  # noqa: E402
from l2flow_history.watermark_sidecar import (  # noqa: E402
    SidecarNamespace,
    SidecarReference,
    WatermarkSidecar,
)


HEADER = struct.Struct("<IHHIIIIQQQQQQqqqIIHHHBB")
TRADE_DATE = 20260722
POLICY = bytes([0x91]) * 32
BACKUP_POLICY = bytes([0x92]) * 32


def sha(byte: int) -> bytes:
    return bytes([byte]) * 32


def canonical_row(
    *,
    ingress: int = 1,
    wal_end: int = 101,
    shard_event_id: int = 999,
    payload_byte: int = 0,
) -> CanonicalArchiveRow:
    header = HEADER.pack(
        0x3145434D,
        1,
        2,
        192,
        7,
        1,
        TRADE_DATE,
        0,
        shard_event_id,
        ingress,
        wal_end,
        ingress,
        ingress,
        1000 + ingress,
        2000 + ingress,
        3000 + ingress,
        600000,
        1,
        1,
        1,
        24,
        4,
        0,
    )
    return CanonicalArchiveRow(
        origin_capture_date=TRADE_DATE,
        origin_stream_day_id=b"D" * 16,
        record_bytes=header + bytes([payload_byte]) * (192 - len(header)),
    )


def source_namespace(
    *,
    family: InputFamily = InputFamily.TICK,
    registry_sha256: bytes = sha(6),
    canonical_generation: int = 3,
    shard_id: int = 0,
) -> SourceNamespace:
    return SourceNamespace(
        trade_date=TRADE_DATE,
        source_stream_id=7,
        origin_capture_date=TRADE_DATE,
        origin_stream_day_id=b"D" * 16,
        family=family,
        shard_id=shard_id,
        origin_source_writer_instance=b"W" * 16,
        origin_source_generation=2,
        canonical_generation=canonical_generation,
        clock_epoch_algorithm=1,
        clock_epoch_digest=sha(7),
        schema_sha256=sha(3),
        dtype_sha256=sha(4),
        registry_version=5,
        registry_sha256=registry_sha256,
        normalizer_build_sha256=sha(8),
        normalizer_config_sha256=sha(9),
    )


def receipt(
    *,
    namespace: SourceNamespace | None = None,
    first_ingress: int = 1,
    last_ingress: int = 10,
    min_wal: int = 101,
    max_wal: int = 110,
) -> SourceRangeReceipt:
    return SourceRangeReceipt(
        namespace=namespace or source_namespace(),
        begin_canonical_cursor=0,
        end_canonical_cursor=10,
        first_origin_ingress_sequence=first_ingress,
        last_origin_ingress_sequence=last_ingress,
        min_origin_wal_end_pos=min_wal,
        max_origin_wal_end_pos=max_wal,
        coverage_kind=CoverageKind.SEALED_COMMON_CUT,
        coverage_certificate_sha256=sha(0x33),
    )


def canonical_artifact(
    row: CanonicalArchiveRow,
    *,
    file_byte: int = 0x41,
    part: str = "a",
    source_receipt: SourceRangeReceipt | None = None,
) -> ArtifactEntry:
    sort_key = canonical_sort_key_wire_v1(row)
    return ArtifactEntry(
        artifact_type=DatasetKind.CANONICAL,
        relative_path=(
            f"canonical/trade_date={TRADE_DATE:08d}/market=SH/"
            f"event_type=tick/bucket={row.bucket:02d}/part-{part}.parquet"
        ),
        file_sha256=sha(file_byte),
        file_size_bytes=512,
        row_count=1,
        logical_content_sha256=canonical_logical_rows_sha256((row,)),
        min_sort_key_v1=sort_key,
        max_sort_key_v1=sort_key,
        partition=PartitionSpec(
            dataset=DatasetKind.CANONICAL,
            trade_date=TRADE_DATE,
            bucket=row.bucket,
            market="SH",
            event_type="tick",
        ),
        source_receipts=(source_receipt or receipt(),),
        visibility=ArtifactVisibility.VISIBLE,
    )


def manifest(
    artifacts: tuple[ArtifactEntry, ...],
    *,
    run_byte: int = 0x51,
    sidecars: tuple[SidecarDescriptor, ...] = (),
) -> HistoryManifest:
    return HistoryManifest(
        run_id=bytes([run_byte]) * 16,
        generation=1,
        previous_manifest_sha256=None,
        artifacts=artifacts,
        sidecars=sidecars,
    )


def watermark_set(
    *,
    set_id: int = 1,
    cursor: int = 5,
    wal_end: int = 105,
    quality: int = 1,
) -> FactorInputWatermarkSet:
    return FactorInputWatermarkSet(
        watermark_set_id=set_id,
        trade_date=TRADE_DATE,
        entries=(
            FactorInputWatermark(
                source_stream_id=7,
                origin_capture_date=TRADE_DATE,
                origin_stream_day_id=b"D" * 16,
                family=InputFamily.TICK,
                shard_id=0,
                canonical_cursor=cursor,
                max_consumed_origin_wal_end_pos=wal_end,
                observed_raw_durable_wal_pos=max(wal_end, 110),
                clock_epoch_algorithm=1,
                clock_epoch_digest=sha(7),
                clock_epoch_label=88,
                input_quality_flags=quality,
            ),
        ),
    )


def factor_row(
    watermark: FactorInputWatermarkSet,
    *,
    run_byte: int = 0x61,
    table_generation: int = 1,
    config_sha256: bytes = sha(0x11),
    clock_sha256: bytes = sha(7),
    quality: int = 1,
    registry_sha256: bytes = sha(6),
    clock_label: int = 88,
    latency: int = 3,
    asof_ns: int = 1000,
    instrument_id: int = 600000,
) -> FactorHistoryRow:
    return FactorHistoryRow.from_float(
        factor_id="book_imbalance",
        factor_version="v1",
        factor_config_sha256=config_sha256,
        factor_code_sha256=sha(0x12),
        state_schema_version=1,
        state_schema_sha256=sha(0x13),
        trade_date=TRADE_DATE,
        registry_version=5,
        registry_sha256=registry_sha256,
        instrument_id=instrument_id,
        asof_ns=asof_ns,
        value=0.25,
        run_id=bytes([run_byte]) * 16,
        watermark_table_generation=table_generation,
        watermark_set_id=watermark.watermark_set_id,
        input_identity_sha256=watermark.input_identity_hash(),
        clock_epoch_algorithm=1,
        clock_epoch_digest=clock_sha256,
        clock_epoch_label=clock_label,
        input_quality_flags=quality,
        implementation_status=FactorImplementationStatus.IMPLEMENTED,
        calculation_latency_ns=latency,
    )


def factor_source(
    row: FactorHistoryRow,
    watermark: FactorInputWatermarkSet,
    *,
    tier: StorageTier = StorageTier.HOT_CANONICAL,
    snapshot_sha256: bytes = sha(0x71),
) -> FactorLiveSource:
    namespace = SidecarNamespace(
        run_id=row.run_id,
        table_generation=row.watermark_table_generation,
    )
    sidecar = WatermarkSidecar(namespace=namespace, watermark_sets=(watermark,))
    return FactorLiveSource(
        tier=tier,
        source_snapshot_sha256=snapshot_sha256,
        rows=(row,),
        sidecars=(sidecar,),
        references=(sidecar.reference_for(watermark.watermark_set_id),),
        source_receipts=(receipt(),),
    )


def budget(
    *,
    sources: int = 16,
    rows: int = 100,
    results: int = 100,
    byte_count: int = 1 << 20,
    deadline: int = 1 << 60,
    lineage_entries: int | None = None,
    lineage_bytes: int | None = None,
    lineage_work: int | None = None,
) -> QueryBudget:
    return QueryBudget(
        sources,
        rows,
        results,
        byte_count,
        deadline,
        lineage_entries,
        lineage_bytes,
        lineage_work,
    )


def canonical_snapshot(
    catalog: CanonicalLineageCatalog,
    *,
    sequence: int = 1,
) -> FixedQuerySnapshot:
    return FixedQuerySnapshot(
        snapshot_sequence=sequence,
        cold_manifest_set_sha256=catalog.manifest_set_sha256,
        hot_frontier_sha256=catalog.hot_frontier_sha256,
        canonical_lineage_catalog_sha256=catalog.catalog_sha256,
    )


def factor_snapshot(catalog: FactorLineageCatalog) -> FixedQuerySnapshot:
    return FixedQuerySnapshot(
        snapshot_sequence=1,
        cold_manifest_set_sha256=catalog.manifest_set_sha256,
        hot_frontier_sha256=catalog.hot_frontier_sha256,
        factor_lineage_catalog_sha256=catalog.catalog_sha256,
        latest_generation_sha256=catalog.latest_generation_sha256,
    )


class QueryTests(unittest.TestCase):
    def test_manifest_visible_cold_and_frozen_hot_dedupe_with_locator_provenance(self) -> None:
        row = canonical_row(shard_event_id=999_999)
        artifact = canonical_artifact(row)
        current = manifest((artifact,))
        hot = HotCanonicalSource(
            hot_frontier_sha256=sha(0x70),
            rows=(row,),
            receipts=(receipt(),),
        )
        catalog = CanonicalLineageCatalog.from_manifests(
            (current,),
            hot_frontier_sha256=sha(0x70),
            hot_sources=(hot,),
        )
        snapshot = canonical_snapshot(catalog)
        readback = parquet_backend.ParquetReadResult(
            rows=(row,), descriptor=artifact.parquet_descriptor()
        )
        with tempfile.TemporaryDirectory() as root:
            parent = Path(root, *Path(artifact.relative_path).parent.parts)
            parent.mkdir(parents=True)
            with mock.patch(
                "l2flow_history.parquet_backend.read_canonical_parquet_part",
                return_value=readback,
            ) as reader:
                cold = read_manifest_visible_canonical_rows(
                    root,
                    artifact.file_sha256,
                    snapshot=snapshot,
                    lineage_catalog=catalog,
                    budget=budget(),
                    clock_ns=lambda: 1,
                )
            self.assertEqual(
                reader.call_args.kwargs["expected_descriptor"],
                artifact.parquet_descriptor(),
            )
            self.assertIn("maximum_decoded_bytes", reader.call_args.kwargs)
        result = merge_canonical_history(
            cold,
            (hot,),
            snapshot=snapshot,
            snapshot_observer=lambda: snapshot,
            lineage_catalog=catalog,
            budget=budget(),
            clock_ns=lambda: 1,
        )
        self.assertEqual(len(result.rows), 1)
        self.assertEqual(result.rows[0].duplicate_count, 1)
        self.assertEqual(
            result.rows[0].provenance,
            (StorageTier.COLD_PARQUET, StorageTier.HOT_CANONICAL),
        )
        self.assertEqual(len(result.rows[0].raw_locator_evidence), 2)
        # shard_event_id is a Canonical event cursor, never a Raw WAL cursor.
        self.assertEqual(
            result.rows[0].raw_locator_evidence[0].origin_wal_end_pos,
            row.header.origin_wal_end_pos,
        )

    def test_source_local_receipt_prevents_borrowing_from_another_artifact(self) -> None:
        row = canonical_row()
        wrong = canonical_artifact(
            row,
            file_byte=0x41,
            part="wrong",
            source_receipt=receipt(first_ingress=2, last_ingress=2),
        )
        right = canonical_artifact(row, file_byte=0x42, part="right")
        current = manifest((wrong, right))
        catalog = CanonicalLineageCatalog.from_manifests(
            (current,), hot_frontier_sha256=sha(0x70)
        )
        snapshot = canonical_snapshot(catalog)
        cold = (
            query_module.CanonicalQueryRow._from_cold_readback(
                row,
                source_snapshot_sha256=snapshot.cold_manifest_set_sha256,
                source_id_sha256=wrong.file_sha256,
            ),
        )
        with self.assertRaises(LineageResolutionError):
            merge_canonical_history(
                cold,
                (),
                snapshot=snapshot,
                snapshot_observer=lambda: snapshot,
                lineage_catalog=catalog,
                budget=budget(),
                clock_ns=lambda: 1,
            )

    def test_canonical_conflict_snapshot_observer_and_empty_query(self) -> None:
        first = HotCanonicalSource(sha(0x70), (canonical_row(payload_byte=1),), (receipt(),))
        second = HotCanonicalSource(sha(0x70), (canonical_row(payload_byte=2),), (receipt(),))
        catalog = CanonicalLineageCatalog.from_manifests(
            (), hot_frontier_sha256=sha(0x70), hot_sources=(first, second)
        )
        snapshot = canonical_snapshot(catalog)
        with self.assertRaises(QueryConflictError):
            merge_canonical_history(
                (),
                (first, second),
                snapshot=snapshot,
                snapshot_observer=lambda: snapshot,
                lineage_catalog=catalog,
                budget=budget(),
                clock_ns=lambda: 1,
            )
        changed = replace(snapshot, snapshot_sequence=2)
        observations = iter((snapshot, changed))
        with self.assertRaises(QuerySnapshotChanged):
            merge_canonical_history(
                (),
                (),
                snapshot=snapshot,
                snapshot_observer=lambda: next(observations),
                lineage_catalog=catalog,
                budget=budget(),
                clock_ns=lambda: 1,
            )
        empty_catalog = CanonicalLineageCatalog.from_manifests(
            (), hot_frontier_sha256=sha(0x72)
        )
        empty_snapshot = canonical_snapshot(empty_catalog)
        empty = merge_canonical_history(
            (),
            (),
            snapshot=empty_snapshot,
            snapshot_observer=lambda: empty_snapshot,
            lineage_catalog=empty_catalog,
            budget=budget(),
            clock_ns=lambda: 1,
        )
        self.assertEqual(empty.rows, ())

    def test_budget_wire_sizes_deadline_overrun_and_empty_sources(self) -> None:
        row = canonical_row()
        hot = HotCanonicalSource(sha(0x70), (row,), (receipt(),))
        catalog = CanonicalLineageCatalog.from_manifests(
            (), hot_frontier_sha256=sha(0x70), hot_sources=(hot,)
        )
        snapshot = canonical_snapshot(catalog)
        self.assertEqual(24 + len(row.record_bytes), 216)
        with self.assertRaises(QueryBudgetExceeded):
            merge_canonical_history(
                (),
                (hot,),
                snapshot=snapshot,
                snapshot_observer=lambda: snapshot,
                lineage_catalog=catalog,
                budget=budget(byte_count=215),
                clock_ns=lambda: 1,
            )

        cold_artifact = canonical_artifact(row)
        cold_manifest = manifest((cold_artifact,))
        cold_catalog = CanonicalLineageCatalog.from_manifests(
            (cold_manifest,), hot_frontier_sha256=sha(0x74)
        )
        cold_snapshot = canonical_snapshot(cold_catalog)
        cold_query_row = query_module.CanonicalQueryRow._from_cold_readback(
            row,
            source_snapshot_sha256=cold_snapshot.cold_manifest_set_sha256,
            source_id_sha256=cold_artifact.file_sha256,
        )

        def stop_after_byte_failure():
            yield cold_query_row
            raise AssertionError("scanner pulled another row after byte failure")

        with self.assertRaises(QueryBudgetExceeded):
            merge_canonical_history(
                stop_after_byte_failure(),
                (),
                snapshot=cold_snapshot,
                snapshot_observer=lambda: cold_snapshot,
                lineage_catalog=cold_catalog,
                budget=budget(byte_count=215),
                clock_ns=lambda: 1,
            )
        ticks = iter((0, 0, 0, 0, 10, 10, 10))
        with self.assertRaises(QueryBudgetExceeded):
            merge_canonical_history(
                (),
                (hot,),
                snapshot=snapshot,
                snapshot_observer=lambda: snapshot,
                lineage_catalog=catalog,
                budget=budget(deadline=10),
                clock_ns=lambda: next(ticks, 10),
            )
        empty_one = HotCanonicalSource(sha(0x70), (), ())
        empty_two = HotCanonicalSource(sha(0x70), (), (receipt(),))
        empty_catalog = CanonicalLineageCatalog.from_manifests(
            (),
            hot_frontier_sha256=sha(0x70),
            hot_sources=(empty_one, empty_two),
        )
        empty_snapshot = canonical_snapshot(empty_catalog)
        with self.assertRaises(QueryBudgetExceeded):
            merge_canonical_history(
                (),
                (empty_one, empty_two),
                snapshot=empty_snapshot,
                snapshot_observer=lambda: empty_snapshot,
                lineage_catalog=empty_catalog,
                budget=budget(sources=1),
                clock_ns=lambda: 1,
            )
        with self.assertRaises(QueryValidationError):
            query_module._bounded_take(itertools.repeat(object()), 2, "test")

    def test_manifest_chain_heads_and_global_paths_are_not_mixed(self) -> None:
        row = canonical_row()
        one = manifest((canonical_artifact(row, file_byte=0x41),), run_byte=0x51)
        divergent_same_run = manifest(
            (canonical_artifact(row, file_byte=0x42),), run_byte=0x51
        )
        with self.assertRaises(QueryValidationError):
            CanonicalLineageCatalog.from_manifests(
                (one, divergent_same_run), hot_frontier_sha256=sha(0x70)
            )
        rebound_path = replace(
            one.artifacts[0], file_sha256=sha(0x43)
        )
        other_run = manifest((rebound_path,), run_byte=0x52)
        with self.assertRaises(QueryValidationError):
            CanonicalLineageCatalog.from_manifests(
                (one, other_run), hot_frontier_sha256=sha(0x70)
            )
        with self.assertRaises(QueryValidationError):
            query_module._manifest_inventory((one,), maximum_members=0)

    def test_manifest_reader_physical_and_decoded_byte_gates(self) -> None:
        row = canonical_row()
        artifact = canonical_artifact(row)
        current = manifest((artifact,))
        catalog = CanonicalLineageCatalog.from_manifests(
            (current,), hot_frontier_sha256=sha(0x70)
        )
        snapshot = canonical_snapshot(catalog)
        with mock.patch(
            "l2flow_history.parquet_backend.read_canonical_parquet_part"
        ) as reader:
            with self.assertRaises(QueryBudgetExceeded):
                read_manifest_visible_canonical_rows(
                    ".",
                    artifact.file_sha256,
                    snapshot=snapshot,
                    lineage_catalog=catalog,
                    budget=budget(byte_count=artifact.file_size_bytes - 1),
                    clock_ns=lambda: 1,
                )
            reader.assert_not_called()

        with tempfile.TemporaryDirectory() as root:
            parent = Path(root, *Path(artifact.relative_path).parent.parts)
            parent.mkdir(parents=True)
            with mock.patch(
                "l2flow_history.parquet_backend.read_canonical_parquet_part",
                side_effect=parquet_backend.ParquetReadBudgetExceeded("decoded"),
            ):
                with self.assertRaises(QueryBudgetExceeded):
                    read_manifest_visible_canonical_rows(
                        root,
                        artifact.file_sha256,
                        snapshot=snapshot,
                        lineage_catalog=catalog,
                        budget=budget(byte_count=artifact.file_size_bytes),
                        clock_ns=lambda: 1,
                    )

    def test_secure_directory_walk_rejects_intermediate_symlink(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            Path(root, "real").mkdir()
            os.symlink("real", Path(root, "link"))
            with self.assertRaises(LineageResolutionError):
                query_module._open_store_subdirectory_no_symlinks(root, ("link",))

    def test_factor_replay_semantics_live_sidecars_and_zero_consumption(self) -> None:
        first_wm = watermark_set()
        first_row = factor_row(first_wm, run_byte=0x61, clock_label=1, latency=10)
        second_wm = replace(first_wm, watermark_set_id=2)
        second_row = factor_row(
            second_wm,
            run_byte=0x62,
            table_generation=2,
            clock_label=999,
            latency=9999,
        )
        first = factor_source(first_row, first_wm)
        second = factor_source(second_row, second_wm)
        catalog = FactorLineageCatalog.from_manifests(
            (),
            sidecars=(),
            hot_frontier_sha256=sha(0x71),
            live_sources=(first, second),
        )
        snapshot = factor_snapshot(catalog)
        result = merge_factor_history(
            (),
            (first, second),
            snapshot=snapshot,
            snapshot_observer=lambda: snapshot,
            lineage_catalog=catalog,
            budget=budget(),
            clock_ns=lambda: 1,
        )
        self.assertEqual(len(result.rows), 1)
        self.assertEqual(len(result.rows[0].lineage_references), 2)
        self.assertTrue(
            all(
                not item.sidecar_descriptor_paths
                and not item.sidecar_manifest_sha256s
                for item in result.rows[0].lineage_references
            )
        )
        exact_wire = query_module._factor_scan_bytes(first_row)
        from l2flow_history.model import _factor_row_wire_v1

        self.assertEqual(exact_wire, len(_factor_row_wire_v1(first_row)))

        zero_wm = watermark_set(cursor=0, wal_end=0, quality=0)
        zero_row = factor_row(zero_wm, run_byte=0x63, quality=0)
        zero_source = factor_source(zero_row, zero_wm)
        zero_catalog = FactorLineageCatalog.from_manifests(
            (),
            sidecars=(),
            hot_frontier_sha256=sha(0x71),
            live_sources=(zero_source,),
        )
        zero_snapshot = factor_snapshot(zero_catalog)
        zero_result = merge_factor_history(
            (),
            (zero_source,),
            snapshot=zero_snapshot,
            snapshot_observer=lambda: zero_snapshot,
            lineage_catalog=zero_catalog,
            budget=budget(),
            clock_ns=lambda: 1,
        )
        coverage = zero_result.rows[0].lineage_references[0].input_coverage[0]
        self.assertTrue(coverage.zero_consumption)
        self.assertIsNone(coverage.receipt)

    def test_factor_lineage_route_index_is_per_entry_not_entries_times_receipts(self) -> None:
        base = watermark_set(cursor=635, wal_end=1635)
        snapshot_entry = replace(
            base.entries[0],
            family=InputFamily.SNAPSHOT,
            shard_id=1,
        )
        watermark = FactorInputWatermarkSet(
            watermark_set_id=base.watermark_set_id,
            trade_date=base.trade_date,
            entries=(base.entries[0], snapshot_entry),
        )
        row = factor_row(watermark)
        initial = factor_source(row, watermark)
        receipts = []
        for family, shard_id in (
            (InputFamily.TICK, 0),
            (InputFamily.SNAPSHOT, 1),
        ):
            namespace = source_namespace(family=family, shard_id=shard_id)
            for index in range(64):
                receipts.append(
                    replace(
                        receipt(namespace=namespace),
                        begin_canonical_cursor=index * 10,
                        end_canonical_cursor=(index + 1) * 10,
                        first_origin_ingress_sequence=index * 10 + 1,
                        last_origin_ingress_sequence=(index + 1) * 10,
                        min_origin_wal_end_pos=1000 + index * 10,
                        max_origin_wal_end_pos=1009 + index * 10,
                    )
                )
        source = replace(initial, source_receipts=tuple(receipts))
        catalog = FactorLineageCatalog.from_manifests(
            (),
            sidecars=(),
            hot_frontier_sha256=sha(0x71),
            live_sources=(source,),
        )
        snapshot = factor_snapshot(catalog)
        original = query_module._NamespaceReceiptRanges.covering_cursor
        calls = 0

        def counted(ranges, cursor):
            nonlocal calls
            calls += 1
            return original(ranges, cursor)

        with mock.patch.object(
            query_module._NamespaceReceiptRanges,
            "covering_cursor",
            counted,
        ), mock.patch(
            "l2flow_history.query._entry_matches_namespace",
            side_effect=AssertionError("linear receipt scan was used"),
        ):
            result = merge_factor_history(
                (),
                (source,),
                snapshot=snapshot,
                snapshot_observer=lambda: snapshot,
                lineage_catalog=catalog,
                budget=budget(lineage_entries=16, lineage_work=32),
                clock_ns=lambda: 1,
            )
        # One sizing pass and one construction pass per watermark entry;
        # receipt count (128) does not multiply this lookup count.
        self.assertEqual(calls, 2 * len(watermark.entries))
        self.assertEqual(result.stats.lineage_entries, 4)
        self.assertEqual(result.stats.lineage_work, 13)

        repeated = replace(source, rows=(row,) * 5)
        repeated_catalog = FactorLineageCatalog.from_manifests(
            (),
            sidecars=(),
            hot_frontier_sha256=sha(0x71),
            live_sources=(repeated,),
        )
        repeated_snapshot = factor_snapshot(repeated_catalog)
        original_resolver = query_module.resolve_factor_lineage
        with mock.patch(
            "l2flow_history.query.resolve_factor_lineage",
            wraps=original_resolver,
        ) as resolver:
            repeated_result = merge_factor_history(
                (),
                (repeated,),
                snapshot=repeated_snapshot,
                snapshot_observer=lambda: repeated_snapshot,
                lineage_catalog=repeated_catalog,
                budget=budget(lineage_entries=32, lineage_work=64),
                clock_ns=lambda: 1,
            )
        self.assertEqual(resolver.call_count, 1)
        self.assertEqual(len(repeated_result.rows[0].observations), 5)
        self.assertEqual(len(repeated_result.rows[0].lineage_references), 1)

    def test_factor_lineage_caps_and_deadline_precede_nested_allocation(self) -> None:
        watermark = watermark_set()
        row = factor_row(watermark)
        source = factor_source(row, watermark)
        catalog = FactorLineageCatalog.from_manifests(
            (),
            sidecars=(),
            hot_frontier_sha256=sha(0x71),
            live_sources=(source,),
        )
        snapshot = factor_snapshot(catalog)
        query_row = query_module.FactorQueryRow._from_live_source(
            row,
            tier=StorageTier.HOT_CANONICAL,
            source_snapshot_sha256=source.source_snapshot_sha256,
            source_id_sha256=source.source_id_sha256,
        )
        observation_bytes = query_module._factor_observation_lineage_bytes(
            query_row
        )

        one_row_budget = budget(rows=1, results=1)
        one_row_result = merge_factor_history(
            (),
            (source,),
            snapshot=snapshot,
            snapshot_observer=lambda: snapshot,
            lineage_catalog=catalog,
            budget=one_row_budget,
            clock_ns=lambda: 1,
        )
        self.assertEqual(one_row_budget.max_lineage_entries, 3)
        self.assertEqual(one_row_budget.max_lineage_work, 7)
        self.assertEqual(one_row_result.stats.lineage_entries, 3)
        self.assertEqual(one_row_result.stats.lineage_work, 7)

        with mock.patch(
            "l2flow_history.query.FactorInputCoverageEvidence",
            side_effect=AssertionError("coverage allocated before lineage gate"),
        ), mock.patch.object(
            WatermarkSidecar,
            "resolve",
            side_effect=AssertionError("hashing ran before entry preflight"),
        ) as resolver:
            with self.assertRaises(QueryBudgetExceeded):
                merge_factor_history(
                    (),
                    (source,),
                    snapshot=snapshot,
                    snapshot_observer=lambda: snapshot,
                    lineage_catalog=catalog,
                    budget=budget(lineage_entries=2, lineage_work=100),
                    clock_ns=lambda: 1,
                )
            resolver.assert_not_called()

        with mock.patch(
            "l2flow_history.query.FactorInputCoverageEvidence",
            side_effect=AssertionError("coverage allocated before byte gate"),
        ):
            with self.assertRaisesRegex(QueryBudgetExceeded, "lineage-byte"):
                merge_factor_history(
                    (),
                    (source,),
                    snapshot=snapshot,
                    snapshot_observer=lambda: snapshot,
                    lineage_catalog=catalog,
                    budget=budget(
                        lineage_entries=10,
                        lineage_bytes=observation_bytes + 1,
                        lineage_work=100,
                    ),
                    clock_ns=lambda: 1,
                )

        with mock.patch.object(
            WatermarkSidecar,
            "resolve",
            side_effect=AssertionError("hashing ran before work preflight"),
        ), mock.patch(
            "l2flow_history.query.FactorInputCoverageEvidence",
            side_effect=AssertionError("coverage allocated before work gate"),
        ):
            with self.assertRaisesRegex(QueryBudgetExceeded, "lineage-work"):
                merge_factor_history(
                    (),
                    (source,),
                    snapshot=snapshot,
                    snapshot_observer=lambda: snapshot,
                    lineage_catalog=catalog,
                    budget=budget(lineage_entries=10, lineage_work=1),
                    clock_ns=lambda: 1,
                )

        calls = 0

        def lineage_clock():
            nonlocal calls
            calls += 1
            return 10 if calls >= 5 else 0

        tracker = query_module._BudgetTracker(
            budget(deadline=10, lineage_entries=10, lineage_work=100),
            lineage_clock,
        )
        with mock.patch(
            "l2flow_history.query.FactorInputCoverageEvidence",
            side_effect=AssertionError("deadline failed after coverage allocation"),
        ):
            with self.assertRaisesRegex(QueryBudgetExceeded, "deadline"):
                query_module.resolve_factor_lineage(
                    query_row,
                    catalog,
                    _tracker=tracker,
                )
        self.assertEqual(calls, 5)

    def test_canonical_pathological_receipt_search_consumes_work_before_evidence(self) -> None:
        row = canonical_row()
        source_receipts = []
        for generation in range(1, 11):
            minimum_wal = 101 if generation == 1 else 200 + generation
            source_receipts.append(
                receipt(
                    namespace=source_namespace(
                        canonical_generation=generation
                    ),
                    first_ingress=1,
                    last_ingress=10,
                    min_wal=minimum_wal,
                    max_wal=minimum_wal,
                )
            )
        source = HotCanonicalSource(
            sha(0x70),
            (row,),
            tuple(source_receipts),
        )
        catalog = CanonicalLineageCatalog.from_manifests(
            (),
            hot_frontier_sha256=sha(0x70),
            hot_sources=(source,),
        )
        snapshot = canonical_snapshot(catalog)
        with mock.patch(
            "l2flow_history.query.CanonicalRawLocatorEvidence",
            side_effect=AssertionError("evidence allocated before work gate"),
        ):
            with self.assertRaisesRegex(QueryBudgetExceeded, "lineage-work"):
                merge_canonical_history(
                    (),
                    (source,),
                    snapshot=snapshot,
                    snapshot_observer=lambda: snapshot,
                    lineage_catalog=catalog,
                    budget=budget(lineage_work=1),
                    clock_ns=lambda: 1,
                )

    def test_factor_live_source_is_permutation_stable_and_receipts_are_unambiguous(self) -> None:
        first_watermark = watermark_set()
        second_watermark = replace(first_watermark, watermark_set_id=2)
        first_row = factor_row(
            first_watermark,
            run_byte=0x61,
            table_generation=1,
            clock_label=1,
            latency=10,
        )
        second_row = factor_row(
            second_watermark,
            run_byte=0x62,
            table_generation=2,
            clock_label=999,
            latency=9999,
        )
        first = factor_source(first_row, first_watermark)
        second = factor_source(second_row, second_watermark)
        common = {
            "tier": StorageTier.HOT_CANONICAL,
            "source_snapshot_sha256": sha(0x71),
            "sidecars": first.sidecars + second.sidecars,
            "references": first.references + second.references,
            "source_receipts": (receipt(),),
        }
        forward = FactorLiveSource(rows=(first_row, second_row), **common)
        reverse = FactorLiveSource(rows=(second_row, first_row), **common)
        self.assertEqual(forward.rows, reverse.rows)
        self.assertEqual(forward.source_id_sha256, reverse.source_id_sha256)
        self.assertEqual(forward.canonical(), reverse.canonical())
        with self.assertRaisesRegex(QueryValidationError, "match exactly"):
            FactorLiveSource(rows=(first_row,), **common)

        conflicting_content = replace(
            first_row,
            factor_config_sha256=sha(0x55),
        )
        same_reference_common = {
            "tier": StorageTier.HOT_CANONICAL,
            "source_snapshot_sha256": sha(0x71),
            "sidecars": first.sidecars,
            "references": first.references,
            "source_receipts": (receipt(),),
        }
        content_forward = FactorLiveSource(
            rows=(first_row, conflicting_content),
            **same_reference_common,
        )
        content_reverse = FactorLiveSource(
            rows=(conflicting_content, first_row),
            **same_reference_common,
        )
        self.assertEqual(content_forward.rows, content_reverse.rows)
        self.assertEqual(
            content_forward.source_id_sha256,
            content_reverse.source_id_sha256,
        )

        forward_catalog = FactorLineageCatalog.from_manifests(
            (),
            sidecars=(),
            hot_frontier_sha256=sha(0x71),
            live_sources=(forward,),
        )
        reverse_catalog = FactorLineageCatalog.from_manifests(
            (),
            sidecars=(),
            hot_frontier_sha256=sha(0x71),
            live_sources=(reverse,),
        )
        self.assertEqual(forward_catalog.catalog_sha256, reverse_catalog.catalog_sha256)
        forward_snapshot = factor_snapshot(forward_catalog)
        reverse_snapshot = factor_snapshot(reverse_catalog)
        forward_result = merge_factor_history(
            (),
            (forward,),
            snapshot=forward_snapshot,
            snapshot_observer=lambda: forward_snapshot,
            lineage_catalog=forward_catalog,
            budget=budget(),
            clock_ns=lambda: 1,
        )
        reverse_result = merge_factor_history(
            (),
            (reverse,),
            snapshot=reverse_snapshot,
            snapshot_observer=lambda: reverse_snapshot,
            lineage_catalog=reverse_catalog,
            budget=budget(),
            clock_ns=lambda: 1,
        )
        self.assertEqual(forward_result, reverse_result)

        overlapping = replace(
            receipt(),
            last_origin_ingress_sequence=11,
            max_origin_wal_end_pos=111,
        )
        with self.assertRaisesRegex(QueryValidationError, "receipt range"):
            HotCanonicalSource(
                sha(0x70),
                (canonical_row(),),
                (receipt(), overlapping),
            )

    def test_factor_result_budget_precedes_merged_row_allocation(self) -> None:
        watermark = watermark_set()
        rows = (
            factor_row(watermark, asof_ns=1000, instrument_id=600000),
            factor_row(watermark, asof_ns=2000, instrument_id=600001),
        )
        for tier, snapshot_digest in (
            (StorageTier.HOT_CANONICAL, sha(0x71)),
            (StorageTier.LATEST_CURRENT, sha(0x79)),
        ):
            with self.subTest(tier=tier):
                one = factor_source(
                    rows[0],
                    watermark,
                    tier=tier,
                    snapshot_sha256=snapshot_digest,
                )
                source = replace(one, rows=rows)
                catalog = FactorLineageCatalog.from_manifests(
                    (),
                    sidecars=(),
                    hot_frontier_sha256=sha(0x71),
                    latest_generation_sha256=(
                        sha(0x79)
                        if tier is StorageTier.LATEST_CURRENT
                        else None
                    ),
                    live_sources=(source,),
                )
                snapshot = factor_snapshot(catalog)
                operation = (
                    merge_factor_history
                    if tier is StorageTier.HOT_CANONICAL
                    else query_latest_current
                )
                arguments = (
                    {
                        "cold_rows": (),
                        "hot_sources": (source,),
                    }
                    if tier is StorageTier.HOT_CANONICAL
                    else {"sources": (source,)}
                )
                with mock.patch(
                    "l2flow_history.query.MergedFactorRow",
                    side_effect=AssertionError("result allocated before budget gate"),
                ), mock.patch(
                    "l2flow_history.query.resolve_factor_lineage",
                    side_effect=AssertionError("lineage resolved before budget gate"),
                ):
                    with self.assertRaises(QueryBudgetExceeded):
                        operation(
                            **arguments,
                            snapshot=snapshot,
                            snapshot_observer=lambda: snapshot,
                            lineage_catalog=catalog,
                            budget=budget(results=1),
                            clock_ns=lambda: 1,
                        )

    def test_factor_stable_conflict_clock_quality_registry_and_latest_boundary(self) -> None:
        wm = watermark_set()
        good_row = factor_row(wm)
        good = factor_source(good_row, wm)
        conflict_row = factor_row(wm, run_byte=0x62, config_sha256=sha(0x21))
        conflict = factor_source(conflict_row, wm)
        catalog = FactorLineageCatalog.from_manifests(
            (), sidecars=(), hot_frontier_sha256=sha(0x71), live_sources=(good, conflict)
        )
        snapshot = factor_snapshot(catalog)
        with self.assertRaises(QueryConflictError):
            merge_factor_history(
                (),
                (good, conflict),
                snapshot=snapshot,
                snapshot_observer=lambda: snapshot,
                lineage_catalog=catalog,
                budget=budget(),
                clock_ns=lambda: 1,
            )
        for bad_row in (
            factor_row(wm, run_byte=0x63, clock_sha256=sha(0x77)),
            factor_row(wm, run_byte=0x64, quality=0),
            factor_row(wm, run_byte=0x65, registry_sha256=sha(0x66)),
        ):
            bad = factor_source(bad_row, wm)
            bad_catalog = FactorLineageCatalog.from_manifests(
                (), sidecars=(), hot_frontier_sha256=sha(0x71), live_sources=(bad,)
            )
            bad_snapshot = factor_snapshot(bad_catalog)
            with self.assertRaises(LineageResolutionError):
                merge_factor_history(
                    (),
                    (bad,),
                    snapshot=bad_snapshot,
                    snapshot_observer=lambda: bad_snapshot,
                    lineage_catalog=bad_catalog,
                    budget=budget(),
                    clock_ns=lambda: 1,
                )

        latest = factor_source(
            good_row,
            wm,
            tier=StorageTier.LATEST_CURRENT,
            snapshot_sha256=sha(0x79),
        )
        latest_catalog = FactorLineageCatalog.from_manifests(
            (),
            sidecars=(),
            hot_frontier_sha256=sha(0x71),
            latest_generation_sha256=sha(0x79),
            live_sources=(latest,),
        )
        latest_snapshot = factor_snapshot(latest_catalog)
        with self.assertRaises(QueryValidationError):
            merge_factor_history(
                (),
                (latest,),
                snapshot=latest_snapshot,
                snapshot_observer=lambda: latest_snapshot,
                lineage_catalog=latest_catalog,
                budget=budget(),
                clock_ns=lambda: 1,
            )
        current = query_latest_current(
            (latest,),
            snapshot=latest_snapshot,
            snapshot_observer=lambda: latest_snapshot,
            lineage_catalog=latest_catalog,
            budget=budget(),
            clock_ns=lambda: 1,
        )
        self.assertTrue(current.current_only)


class BackendBudgetTests(unittest.TestCase):
    def test_footer_decoded_budget_rejects_before_parquet_read(self) -> None:
        class Schema:
            metadata = {}

            def __len__(self) -> int:
                return 1

            def equals(self, other: object, check_metadata: bool = True) -> bool:
                return True

        class Column:
            compression = "ZSTD"
            total_compressed_size = 4

        class RowGroup:
            num_rows = 1
            num_columns = 1
            total_byte_size = 100

            def column(self, index: int) -> Column:
                return Column()

        class Metadata:
            num_rows = 1
            num_row_groups = 1

            def row_group(self, index: int) -> RowGroup:
                return RowGroup()

        class FakeParquetFile:
            read_called = False
            metadata = Metadata()
            schema_arrow = Schema()

            def __init__(self, source: object) -> None:
                pass

            def read(self) -> object:
                type(self).read_called = True
                raise AssertionError("decode must not run")

        class PQ:
            ParquetFile = FakeParquetFile

        with tempfile.NamedTemporaryFile() as file:
            file.write(b"PAR1xxxxPAR1")
            file.flush()
            os.chmod(file.name, 0o600)
            fd = os.open(file.name, os.O_RDONLY)
            try:
                with mock.patch.object(
                    parquet_backend,
                    "_parse_footer_metadata",
                    return_value=(1, 0, sha(1), b"a", b"a"),
                ), mock.patch.object(
                    parquet_backend, "_footer_metadata_v1", return_value={}
                ), mock.patch.object(
                    parquet_backend, "_canonical_schema", return_value=Schema()
                ):
                    with self.assertRaises(
                        parquet_backend.ParquetReadBudgetExceeded
                    ):
                        parquet_backend._read_arrow_table_from_fd(
                            fd,
                            pa=object(),
                            pq=PQ(),
                            expected_schema_name="l2flow.canonical.parquet.v1",
                            maximum_decoded_bytes=99,
                        )
            finally:
                os.close(fd)
        self.assertFalse(FakeParquetFile.read_called)

    def test_expected_descriptor_rejects_before_arrow_open(self) -> None:
        class FakeParquetFile:
            opened = False

            def __init__(self, source: object) -> None:
                type(self).opened = True
                raise AssertionError("Arrow must not open mismatched descriptor bytes")

        class PQ:
            ParquetFile = FakeParquetFile

        expected = canonical_artifact(canonical_row()).parquet_descriptor()
        with tempfile.NamedTemporaryFile() as file:
            file.write(b"PAR1xxxxPAR1")
            file.flush()
            os.chmod(file.name, 0o600)
            fd = os.open(file.name, os.O_RDONLY)
            try:
                with self.assertRaises(parquet_backend.ParquetValidationError):
                    parquet_backend._read_validated_fd(
                        fd,
                        basename=expected.basename,
                        schema_name=expected.schema_name,
                        pa=object(),
                        pq=PQ(),
                        expected_descriptor=expected,
                    )
            finally:
                os.close(fd)
        self.assertFalse(FakeParquetFile.opened)


def raw_scope(*, stream: int = 7) -> RawWalScope:
    return RawWalScope(TRADE_DATE, stream, b"D" * 16)


def canonical_scope() -> CanonicalCursorScope:
    return CanonicalCursorScope(
        TRADE_DATE,
        7,
        b"D" * 16,
        b"W" * 16,
        2,
        3,
        2,
        0,
    )


def retention_artifact(
    artifact_id: str = "raw-a",
    *,
    kind: ArtifactKind = ArtifactKind.RAW_SEGMENT,
    scope: RawWalScope | CanonicalCursorScope | None = None,
    begin: int | None = 100,
    end: int | None = 200,
    policy: bytes = POLICY,
    content_byte: int = 0xA1,
) -> RetentionArtifact:
    return RetentionArtifact(
        artifact_id=artifact_id,
        kind=kind,
        content_sha256=sha(content_byte),
        retention_policy_sha256=policy,
        byte_size=1024,
        retain_until_ns=1000,
        required_manifest_sha256s=(sha(0xB1),),
        scope=raw_scope() if scope is None and kind is ArtifactKind.RAW_SEGMENT else scope,
        begin_cursor=begin,
        end_cursor=end,
    )


def retention_inputs(
    artifacts: tuple[RetentionArtifact, ...],
    *,
    checkpoint_cursor: int = 200,
    registrations: tuple[RegisteredConsumer, ...] | None = None,
    checkpoints: tuple[ConsumerCheckpoint, ...] | None = None,
    active: tuple[ActiveReference, ...] = (),
    sidecar_refs: tuple[RetentionSidecarReference, ...] = (),
) -> dict[str, object]:
    scoped = next((item for item in artifacts if item.scope is not None), None)
    if registrations is None:
        registrations = (
            ()
            if scoped is None
            else (RegisteredConsumer("consumer-a", scoped.scope),)
        )
    if checkpoints is None:
        checkpoints = (
            ()
            if scoped is None
            else (
                ConsumerCheckpoint(
                    "consumer-a", scoped.scope, checkpoint_cursor, sha(0xC1)
                ),
            )
        )
    publications = tuple(
        PublicationEvidence(
            item.artifact_id,
            item.required_manifest_sha256s,
            True,
            scope=item.scope,
            begin_cursor=item.begin_cursor,
            end_cursor=item.end_cursor,
        )
        for item in artifacts
    )
    backups = tuple(
        BackupEvidence(
            item.artifact_id,
            item.content_sha256,
            BACKUP_POLICY,
            (sha(0xD1),),
            True,
        )
        for item in artifacts
    )
    return {
        "observed_at_ns": 1000,
        "policy_sha256": POLICY,
        "backup_policy_sha256": BACKUP_POLICY,
        "required_backup_copies": 1,
        "minimum_human_approvers": 2,
        "allow_automated_approval": False,
        "publication_evidence": publications,
        "backup_evidence": backups,
        "consumer_registry": ConsumerRegistrySnapshot(
            registry_generation=1,
            complete=True,
            registrations=registrations,
            checkpoints=checkpoints,
        ),
        "active_references": ActiveReferenceSnapshot(True, active),
        "sidecar_references": SidecarReferenceSnapshot(True, sidecar_refs),
    }


def approved_plan(
    artifacts: tuple[RetentionArtifact, ...],
    inputs: dict[str, object],
) -> tuple[object, object]:
    proposal = plan_retention(artifacts, **inputs)
    approval = RetentionApproval(
        proposal.proposal_sha256,
        POLICY,
        ("alice", "bob"),
        False,
        True,
    )
    final = plan_retention(artifacts, approval=approval, **inputs)
    return proposal, final


class RetentionTests(unittest.TestCase):
    def test_two_pass_dry_run_and_exclusive_cursor_boundary(self) -> None:
        artifact = retention_artifact()
        proposal, final = approved_plan((artifact,), retention_inputs((artifact,)))
        self.assertEqual(proposal.proposed_artifact_ids, (artifact.artifact_id,))
        self.assertEqual(final.authorized_artifact_ids, (artifact.artifact_id,))
        self.assertTrue(final.dry_run)
        self.assertFalse(final.mutation_performed)
        self.assertEqual(len(final.evaluations[0].gates), len(RetentionGate))
        behind = plan_retention(
            (artifact,), **retention_inputs((artifact,), checkpoint_cursor=199)
        )
        durable = next(
            gate
            for gate in behind.evaluations[0].gates
            if gate.gate is RetentionGate.DURABLE_CONSUMERS
        )
        self.assertFalse(durable.satisfied)

    def test_scope_no_consumer_policy_publication_and_retention_policy(self) -> None:
        artifact = retention_artifact()
        no_consumers = retention_inputs(
            (artifact,), registrations=(), checkpoints=()
        )
        blocked = plan_retention((artifact,), **no_consumers)
        self.assertEqual(blocked.proposed_artifact_ids, ())
        allowed = plan_retention(
            (artifact,), allow_no_applicable_consumers=True, **no_consumers
        )
        self.assertEqual(allowed.proposed_artifact_ids, (artifact.artifact_id,))
        self.assertNotEqual(blocked.proposal_sha256, allowed.proposal_sha256)

        wrong_publication = dict(retention_inputs((artifact,)))
        wrong_publication["publication_evidence"] = (
            PublicationEvidence(
                artifact.artifact_id,
                artifact.required_manifest_sha256s,
                True,
                scope=raw_scope(stream=8),
                begin_cursor=100,
                end_cursor=200,
            ),
        )
        wrong_plan = plan_retention((artifact,), **wrong_publication)
        publication_gate = next(
            gate
            for gate in wrong_plan.evaluations[0].gates
            if gate.gate is RetentionGate.PUBLISHED_MANIFESTS
        )
        self.assertFalse(publication_gate.satisfied)

        wrong_policy_artifact = retention_artifact(policy=sha(0x99))
        wrong_policy = plan_retention(
            (wrong_policy_artifact,),
            **retention_inputs((wrong_policy_artifact,)),
        )
        period_gate = next(
            gate
            for gate in wrong_policy.evaluations[0].gates
            if gate.gate is RetentionGate.RETENTION_PERIOD
        )
        self.assertFalse(period_gate.satisfied)

    def test_artifact_cursor_domains_are_kind_exact(self) -> None:
        with self.assertRaises(RetentionValidationError):
            retention_artifact(scope=canonical_scope())
        with self.assertRaises(RetentionValidationError):
            retention_artifact(
                kind=ArtifactKind.CANONICAL_SEGMENT,
                scope=raw_scope(),
            )
        with self.assertRaises(RetentionValidationError):
            retention_artifact(
                kind=ArtifactKind.PARQUET_PART,
                scope=raw_scope(),
            )

    def test_manifest_query_replay_audit_refs_and_unknown_fail_closed(self) -> None:
        raw = retention_artifact()
        manifest_artifact = retention_artifact(
            "manifest-a",
            kind=ArtifactKind.MANIFEST,
            scope=None,
            begin=None,
            end=None,
            content_byte=0xA2,
        )
        manifest_ref = ActiveReference(
            raw.artifact_id,
            ActiveReferenceKind.MANIFEST,
            manifest_artifact.artifact_id,
            sha(0xE1),
        )
        blocked = plan_retention(
            (raw, manifest_artifact),
            **retention_inputs((raw, manifest_artifact), active=(manifest_ref,)),
        )
        self.assertNotIn(raw.artifact_id, blocked.proposed_artifact_ids)
        root_ref = ActiveReference(
            manifest_artifact.artifact_id,
            ActiveReferenceKind.MANIFEST,
            "CURRENT_ROOT:history",
            sha(0xE2),
        )
        rooted = plan_retention(
            (raw, manifest_artifact),
            **retention_inputs((raw, manifest_artifact), active=(root_ref,)),
        )
        self.assertNotIn(manifest_artifact.artifact_id, rooted.proposed_artifact_ids)
        unknown = ActiveReference(
            "missing",
            ActiveReferenceKind.QUERY,
            "query-a",
            sha(0xE3),
        )
        with self.assertRaises(RetentionValidationError):
            plan_retention(
                (raw,), **retention_inputs((raw,), active=(unknown,))
            )

    def test_sidecar_dependency_closure_and_latest_reference(self) -> None:
        part = retention_artifact(
            "factor-part",
            kind=ArtifactKind.PARQUET_PART,
            scope=None,
            begin=None,
            end=None,
            content_byte=0xA3,
        )
        sidecar = retention_artifact(
            "watermarks",
            kind=ArtifactKind.WATERMARK_SIDECAR,
            scope=None,
            begin=None,
            end=None,
            content_byte=0xA4,
        )
        history_ref = RetentionSidecarReference(
            sidecar.artifact_id,
            SidecarOwnerKind.HISTORY_ARTIFACT,
            part.artifact_id,
            sha(0xF1),
        )
        proposal = plan_retention(
            (sidecar, part),
            **retention_inputs((sidecar, part), sidecar_refs=(history_ref,)),
        )
        self.assertEqual(
            set(proposal.proposed_artifact_ids),
            {part.artifact_id, sidecar.artifact_id},
        )
        latest_ref = RetentionSidecarReference(
            sidecar.artifact_id,
            SidecarOwnerKind.LATEST_CURRENT,
            "latest-factor",
            sha(0xF2),
        )
        blocked = plan_retention(
            (sidecar, part),
            **retention_inputs(
                (sidecar, part), sidecar_refs=(history_ref, latest_ref)
            ),
        )
        self.assertNotIn(sidecar.artifact_id, blocked.proposed_artifact_ids)

    def test_invalid_manifest_and_sidecar_reference_graphs_fail_closed(self) -> None:
        raw = retention_artifact()
        part = retention_artifact(
            "factor-part",
            kind=ArtifactKind.PARQUET_PART,
            scope=None,
            begin=None,
            end=None,
            content_byte=0xA3,
        )
        sidecar = retention_artifact(
            "watermarks",
            kind=ArtifactKind.WATERMARK_SIDECAR,
            scope=None,
            begin=None,
            end=None,
            content_byte=0xA4,
        )
        manifest_artifact = retention_artifact(
            "manifest-a",
            kind=ArtifactKind.MANIFEST,
            scope=None,
            begin=None,
            end=None,
            content_byte=0xA5,
        )

        invalid_sidecar_references = (
            (
                (sidecar, part),
                RetentionSidecarReference(
                    "missing",
                    SidecarOwnerKind.LATEST_CURRENT,
                    "latest-factor",
                    sha(0xF3),
                ),
            ),
            (
                (sidecar,),
                RetentionSidecarReference(
                    sidecar.artifact_id,
                    SidecarOwnerKind.HISTORY_ARTIFACT,
                    "missing-owner",
                    sha(0xF4),
                ),
            ),
            (
                (sidecar, raw),
                RetentionSidecarReference(
                    sidecar.artifact_id,
                    SidecarOwnerKind.HISTORY_ARTIFACT,
                    raw.artifact_id,
                    sha(0xF5),
                ),
            ),
            (
                (sidecar,),
                RetentionSidecarReference(
                    sidecar.artifact_id,
                    SidecarOwnerKind.HISTORY_ARTIFACT,
                    sidecar.artifact_id,
                    sha(0xF6),
                ),
            ),
        )
        for artifacts, reference in invalid_sidecar_references:
            with self.subTest(sidecar_reference=reference):
                with self.assertRaises(RetentionValidationError):
                    plan_retention(
                        artifacts,
                        **retention_inputs(artifacts, sidecar_refs=(reference,)),
                    )

        invalid_manifest_references = (
            (
                (manifest_artifact,),
                ActiveReference(
                    manifest_artifact.artifact_id,
                    ActiveReferenceKind.MANIFEST,
                    manifest_artifact.artifact_id,
                    sha(0xE4),
                ),
            ),
            (
                (raw, part),
                ActiveReference(
                    raw.artifact_id,
                    ActiveReferenceKind.MANIFEST,
                    part.artifact_id,
                    sha(0xE5),
                ),
            ),
            (
                (raw,),
                ActiveReference(
                    raw.artifact_id,
                    ActiveReferenceKind.MANIFEST,
                    "external-holder",
                    sha(0xE6),
                ),
            ),
        )
        for artifacts, reference in invalid_manifest_references:
            with self.subTest(manifest_reference=reference):
                with self.assertRaises(RetentionValidationError):
                    plan_retention(
                        artifacts,
                        **retention_inputs(artifacts, active=(reference,)),
                    )

    def test_retention_snapshots_are_indexed_and_encoded_once(self) -> None:
        shared_scope = raw_scope()
        raw_a = retention_artifact(
            "raw-a",
            scope=shared_scope,
            begin=100,
            end=200,
            content_byte=0xA1,
        )
        raw_b = retention_artifact(
            "raw-b",
            scope=shared_scope,
            begin=200,
            end=300,
            content_byte=0xA2,
        )
        part = retention_artifact(
            "factor-part",
            kind=ArtifactKind.PARQUET_PART,
            scope=None,
            begin=None,
            end=None,
            content_byte=0xA3,
        )
        sidecar = retention_artifact(
            "watermarks",
            kind=ArtifactKind.WATERMARK_SIDECAR,
            scope=None,
            begin=None,
            end=None,
            content_byte=0xA4,
        )
        artifacts = (raw_a, raw_b, part, sidecar)
        registrations = tuple(
            RegisteredConsumer(f"consumer-{index}", shared_scope)
            for index in range(8)
        )
        checkpoints = tuple(
            ConsumerCheckpoint(
                f"consumer-{index}",
                shared_scope,
                299 if index == 0 else 400,
                sha(0xC0 + index),
            )
            for index in range(8)
        )
        active = (
            ActiveReference(
                part.artifact_id,
                ActiveReferenceKind.QUERY,
                "query-a",
                sha(0xE7),
            ),
        )
        sidecar_refs = (
            RetentionSidecarReference(
                sidecar.artifact_id,
                SidecarOwnerKind.HISTORY_ARTIFACT,
                part.artifact_id,
                sha(0xF7),
            ),
        )
        inputs = retention_inputs(
            artifacts,
            registrations=registrations,
            checkpoints=checkpoints,
            active=active,
            sidecar_refs=sidecar_refs,
        )

        canonical_counts = {"consumer": 0, "active": 0, "sidecar": 0}
        snapshot_models: dict[str, object] = {}
        hash_inputs: list[object] = []
        original_consumer_canonical = ConsumerRegistrySnapshot.canonical
        original_active_canonical = ActiveReferenceSnapshot.canonical
        original_sidecar_canonical = SidecarReferenceSnapshot.canonical
        original_hash_json = retention_module._hash_json

        def consumer_canonical(value: ConsumerRegistrySnapshot) -> dict[str, object]:
            canonical_counts["consumer"] += 1
            model = original_consumer_canonical(value)
            snapshot_models["consumer"] = model
            return model

        def active_canonical(value: ActiveReferenceSnapshot) -> dict[str, object]:
            canonical_counts["active"] += 1
            model = original_active_canonical(value)
            snapshot_models["active"] = model
            return model

        def sidecar_canonical(value: SidecarReferenceSnapshot) -> dict[str, object]:
            canonical_counts["sidecar"] += 1
            model = original_sidecar_canonical(value)
            snapshot_models["sidecar"] = model
            return model

        def record_hash_json(domain: bytes, value: object) -> bytes:
            hash_inputs.append(value)
            return original_hash_json(domain, value)

        with (
            mock.patch.object(
                ConsumerRegistrySnapshot,
                "canonical",
                new=consumer_canonical,
            ),
            mock.patch.object(
                ActiveReferenceSnapshot,
                "canonical",
                new=active_canonical,
            ),
            mock.patch.object(
                SidecarReferenceSnapshot,
                "canonical",
                new=sidecar_canonical,
            ),
            mock.patch.object(
                retention_module,
                "_index_consumers",
                wraps=retention_module._index_consumers,
            ) as index_consumers,
            mock.patch.object(
                retention_module,
                "_hash_json",
                side_effect=record_hash_json,
            ),
        ):
            result = plan_retention(artifacts, **inputs)

        self.assertEqual(
            canonical_counts,
            {"consumer": 1, "active": 1, "sidecar": 1},
        )
        index_consumers.assert_called_once_with(inputs["consumer_registry"])

        def contains_identity(value: object, target: object) -> bool:
            if value is target:
                return True
            if isinstance(value, dict):
                return any(
                    contains_identity(item, target)
                    for item in value.values()
                )
            if isinstance(value, (list, tuple)):
                return any(contains_identity(item, target) for item in value)
            return False

        # Each complete snapshot model reaches JSON hashing only through the
        # single proposal model.  Per-artifact gates contain its fixed digest
        # and their indexed relevant slice, not the complete snapshot body.
        for model in snapshot_models.values():
            self.assertEqual(
                sum(contains_identity(value, model) for value in hash_inputs),
                1,
            )

        durable_by_id = {
            evaluation.artifact.artifact_id: next(
                gate
                for gate in evaluation.gates
                if gate.gate is RetentionGate.DURABLE_CONSUMERS
            )
            for evaluation in result.evaluations
        }
        self.assertTrue(durable_by_id[raw_a.artifact_id].satisfied)
        self.assertFalse(durable_by_id[raw_b.artifact_id].satisfied)
        self.assertEqual(
            durable_by_id[raw_b.artifact_id].detail,
            "checkpoint_before_exclusive_end",
        )
        repeated = plan_retention(artifacts, **inputs)
        self.assertEqual(repeated.proposal_sha256, result.proposal_sha256)
        self.assertEqual(repeated.plan_sha256, result.plan_sha256)

    def test_determinism_policy_change_stales_approval_and_snapshot_hashes(self) -> None:
        first = retention_artifact()
        inputs = retention_inputs((first,))
        proposal = plan_retention((first,), **inputs)
        approval = RetentionApproval(
            proposal.proposal_sha256,
            POLICY,
            ("alice", "bob"),
            False,
            True,
        )
        changed = dict(inputs)
        changed["minimum_human_approvers"] = 3
        stale = plan_retention((first,), approval=approval, **changed)
        self.assertEqual(stale.authorized_artifact_ids, ())
        self.assertNotEqual(stale.proposal_sha256, proposal.proposal_sha256)
        self.assertEqual(
            ActiveReferenceSnapshot(True, ()).snapshot_sha256,
            ActiveReferenceSnapshot(True, ()).snapshot_sha256,
        )
        self.assertNotEqual(
            ActiveReferenceSnapshot(True, ()).snapshot_sha256,
            ActiveReferenceSnapshot(False, ()).snapshot_sha256,
        )
        with self.assertRaises(RetentionValidationError):
            retention_module._bounded_take(itertools.repeat(object()), 2, "test")


if __name__ == "__main__":
    unittest.main()
