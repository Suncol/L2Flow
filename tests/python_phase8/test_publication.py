from __future__ import annotations

from dataclasses import replace
import hashlib
import importlib.util
from pathlib import Path
import struct
import tempfile
import time
import unittest
from unittest.mock import patch

import l2flow_history.publication as publication_module
from l2flow_factor import (
    FactorInputWatermark,
    FactorInputWatermarkSet,
    InputFamily,
)
from l2flow_history.manifest import (
    ArtifactVisibility,
    AtomicManifestStore,
    CoverageKind,
    DatasetKind,
    HistoryManifest,
    PartitionSpec,
    SidecarNamespace,
    SourceNamespace,
    SourceRangeReceipt,
)
from l2flow_history.model import (
    CANONICAL_PARQUET_SCHEMA_NAME_V1,
    FACTOR_PARQUET_SCHEMA_NAME_V1,
    CanonicalArchiveRow,
    FactorHistoryRow,
    FactorImplementationStatus,
    ParquetPartDescriptor,
    canonical_logical_rows_sha256,
    canonical_sort_key_wire_v1,
    factor_logical_rows_sha256,
    factor_sort_key_wire_v1,
)
from l2flow_history.parquet_backend import (
    ParquetReadResult,
    ParquetValidationError,
    write_canonical_parquet_part,
    write_factor_parquet_part,
)
from l2flow_history.publication import (
    PublicationValidationError,
    UpstreamValidatedFactorGroup,
    UpstreamValidatedReceipts,
    assemble_canonical_artifact,
    assemble_factor_artifact,
)
from l2flow_history.watermark_sidecar import WatermarkSidecar


_HEADER = struct.Struct("<IHHIIIIQQQQQQqqqIIHHHBB")
_RECORD_SIZES = {1: 2048, 2: 192, 3: 192, 4: 256}
_STREAM_DAY = bytes.fromhex("11111111111111111111111111111111")
_RUN_ID = bytes.fromhex("00112233445566778899aabbccddeeff")
_CERTIFICATE = hashlib.sha256(b"upstream common cut identity").digest()
_EVIDENCE = _CERTIFICATE
_OTHER_EVIDENCE = hashlib.sha256(b"different validation evidence").digest()
_HAS_PYARROW = importlib.util.find_spec("pyarrow") is not None


def _hash(label: str) -> bytes:
    return hashlib.sha256(label.encode("ascii")).digest()


def _canonical_row(
    *,
    event_type: int = 2,
    instrument_id: int = 600000,
    trade_date: int = 20260722,
    source_stream_id: int = 7,
    ingress: int = 7,
    wal_end: int = 700,
    shard_event_id: int = 9_000_000,
    market: int | None = None,
) -> CanonicalArchiveRow:
    record_size = _RECORD_SIZES[event_type]
    if event_type in (1, 2):
        selected_market = 1 if market is None else market
        instrument = instrument_id
        channel = 1
        vendor_sequence = ingress
        exchange_sequence = ingress
        exchange_time = 100
        sub_index = 0
        message_id = 4 if event_type == 1 else 24
        service_id = 4
    elif event_type == 3:
        selected_market = 1 if market is None else market
        instrument = instrument_id if selected_market else 0
        channel = 1 if selected_market else 0
        vendor_sequence = ingress
        exchange_sequence = ingress
        exchange_time = 100
        sub_index = 1
        message_id = 24
        service_id = 4
    elif event_type == 4:
        selected_market = 0
        instrument = 0
        channel = 0
        vendor_sequence = 0
        exchange_sequence = 0
        exchange_time = 0
        sub_index = 0
        message_id = 1
        service_id = 1
    else:
        raise AssertionError("test event_type")
    header = _HEADER.pack(
        0x3145434D,
        1,
        event_type,
        record_size,
        source_stream_id,
        1,
        trade_date,
        0,
        shard_event_id,
        ingress,
        wal_end,
        vendor_sequence,
        exchange_sequence,
        exchange_time,
        1_000,
        2_000,
        instrument,
        channel,
        selected_market,
        101,
        message_id,
        service_id,
        sub_index,
    )
    return CanonicalArchiveRow(
        origin_capture_date=trade_date,
        origin_stream_day_id=_STREAM_DAY,
        record_bytes=header + bytes([event_type]) * (record_size - _HEADER.size),
    )


def _canonical_result(
    rows: tuple[CanonicalArchiveRow, ...],
    *,
    basename: str = "part-canonical.parquet",
) -> ParquetReadResult:
    descriptor = ParquetPartDescriptor(
        basename=basename,
        schema_name=CANONICAL_PARQUET_SCHEMA_NAME_V1,
        row_count=len(rows),
        byte_size=4096,
        bucket=rows[0].bucket,
        logical_rows_sha256=canonical_logical_rows_sha256(rows),
        file_sha256=_hash("canonical parquet file"),
        min_sort_key_v1=canonical_sort_key_wire_v1(rows[0]),
        max_sort_key_v1=canonical_sort_key_wire_v1(rows[-1]),
    )
    return ParquetReadResult(rows=rows, descriptor=descriptor)


def _assemble_canonical_readback(
    result: object,
    *,
    expected_descriptor: ParquetPartDescriptor | None = None,
    **kwargs: object,
):
    descriptor = (
        result.descriptor
        if expected_descriptor is None and isinstance(result, ParquetReadResult)
        else expected_descriptor
    )
    if descriptor is None:
        descriptor = _canonical_result((_canonical_row(),)).descriptor
    with patch(
        "l2flow_history.publication.read_canonical_parquet_part",
        return_value=result,
    ):
        return assemble_canonical_artifact(
            "/validated/archive",
            descriptor.basename,
            expected_descriptor=descriptor,
            **kwargs,
        )


def _source_namespace(
    row: CanonicalArchiveRow,
    *,
    source_stream_id: int | None = None,
    shard_id: int = 4_000_000_000,
    canonical_generation: int = 987_654_321,
) -> SourceNamespace:
    return SourceNamespace(
        trade_date=row.header.trade_date,
        source_stream_id=(
            row.header.source_stream_id
            if source_stream_id is None
            else source_stream_id
        ),
        origin_capture_date=row.origin_capture_date,
        origin_stream_day_id=row.origin_stream_day_id,
        family=InputFamily(row.header.event_family),
        shard_id=shard_id,
        origin_source_writer_instance=bytes.fromhex("22" * 16),
        origin_source_generation=123_456,
        canonical_generation=canonical_generation,
        clock_epoch_algorithm=1,
        clock_epoch_digest=_hash("clock"),
        schema_sha256=_hash("schema"),
        dtype_sha256=_hash("dtype"),
        registry_version=5,
        registry_sha256=_hash("registry"),
        normalizer_build_sha256=_hash("normalizer build"),
        normalizer_config_sha256=_hash("normalizer config"),
    )


def _receipt(
    row: CanonicalArchiveRow,
    *,
    namespace: SourceNamespace | None = None,
    first_ingress: int | None = None,
    last_ingress: int | None = None,
    min_wal: int | None = None,
    max_wal: int | None = None,
    begin_cursor: int = 123,
    end_cursor: int = 124,
    coverage: CoverageKind = CoverageKind.SEALED_COMMON_CUT,
) -> SourceRangeReceipt:
    ingress = row.header.origin_ingress_sequence
    wal_end = row.header.origin_wal_end_pos
    return SourceRangeReceipt(
        namespace=_source_namespace(row) if namespace is None else namespace,
        begin_canonical_cursor=begin_cursor,
        end_canonical_cursor=end_cursor,
        first_origin_ingress_sequence=(
            ingress if first_ingress is None else first_ingress
        ),
        last_origin_ingress_sequence=(
            ingress if last_ingress is None else last_ingress
        ),
        min_origin_wal_end_pos=wal_end if min_wal is None else min_wal,
        max_origin_wal_end_pos=wal_end if max_wal is None else max_wal,
        coverage_kind=coverage,
        coverage_certificate_sha256=(
            _CERTIFICATE
            if coverage is CoverageKind.SEALED_COMMON_CUT
            else None
        ),
    )


def _validated_receipts(*receipts: SourceRangeReceipt) -> UpstreamValidatedReceipts:
    return UpstreamValidatedReceipts(tuple(receipts), _EVIDENCE)


def _canonical_partition(
    row: CanonicalArchiveRow,
    *,
    market: str | None = None,
    event_type: str | None = None,
    bucket: int | None = None,
    trade_date: int | None = None,
) -> PartitionSpec:
    markets = {0: "GLOBAL", 1: "SH", 2: "SZ"}
    return PartitionSpec(
        dataset=DatasetKind.CANONICAL,
        trade_date=row.header.trade_date if trade_date is None else trade_date,
        market=markets[row.header.market] if market is None else market,
        event_type=row.header.event_family if event_type is None else event_type,
        bucket=row.bucket if bucket is None else bucket,
    )


def _watermark_set(
    watermark_set_id: int,
    *,
    trade_date: int = 20260722,
    observed_raw_durable_wal_pos: int | None = None,
) -> FactorInputWatermarkSet:
    entry = FactorInputWatermark(
        source_stream_id=7,
        origin_capture_date=20260722,
        origin_stream_day_id=_STREAM_DAY,
        family=InputFamily.TICK,
        shard_id=99,
        canonical_cursor=watermark_set_id + 10,
        max_consumed_origin_wal_end_pos=watermark_set_id + 700,
        observed_raw_durable_wal_pos=(
            watermark_set_id + 800
            if observed_raw_durable_wal_pos is None
            else observed_raw_durable_wal_pos
        ),
        clock_epoch_algorithm=1,
        clock_epoch_digest=_hash("clock"),
        clock_epoch_label=11,
        input_quality_flags=0,
    )
    return FactorInputWatermarkSet(
        watermark_set_id=watermark_set_id,
        trade_date=trade_date,
        entries=(entry,),
    )


def _factor_row(
    watermark: FactorInputWatermarkSet,
    *,
    instrument_id: int = 600000,
    asof_ns: int = 100,
    input_identity_sha256: bytes | None = None,
) -> FactorHistoryRow:
    return FactorHistoryRow.from_float(
        value=None,
        factor_id="book_imbalance",
        factor_version="1.0.0",
        factor_config_sha256=_hash("factor config"),
        factor_code_sha256=_hash("factor code"),
        state_schema_version=1,
        state_schema_sha256=_hash("factor state schema"),
        trade_date=20260722,
        registry_version=5,
        registry_sha256=_hash("registry"),
        instrument_id=instrument_id,
        asof_ns=asof_ns,
        run_id=_RUN_ID,
        watermark_table_generation=3,
        watermark_set_id=watermark.watermark_set_id,
        input_identity_sha256=(
            watermark.input_identity_hash()
            if input_identity_sha256 is None
            else input_identity_sha256
        ),
        clock_epoch_algorithm=1,
        clock_epoch_digest=_hash("clock"),
        clock_epoch_label=11,
        input_quality_flags=0,
        implementation_status=FactorImplementationStatus.PASSTHROUGH_PLACEHOLDER,
    )


def _factor_result(rows: tuple[FactorHistoryRow, ...]) -> ParquetReadResult:
    descriptor = ParquetPartDescriptor(
        basename="part-factor.parquet",
        schema_name=FACTOR_PARQUET_SCHEMA_NAME_V1,
        row_count=len(rows),
        byte_size=8192,
        bucket=rows[0].bucket,
        logical_rows_sha256=factor_logical_rows_sha256(rows),
        file_sha256=_hash("factor parquet file"),
        min_sort_key_v1=factor_sort_key_wire_v1(rows[0]),
        max_sort_key_v1=factor_sort_key_wire_v1(rows[-1]),
    )
    return ParquetReadResult(rows=rows, descriptor=descriptor)


def _assemble_factor_readback(
    result: object,
    *,
    expected_descriptor: ParquetPartDescriptor | None = None,
    **kwargs: object,
):
    descriptor = (
        result.descriptor
        if expected_descriptor is None and isinstance(result, ParquetReadResult)
        else expected_descriptor
    )
    if descriptor is None:
        watermark = _watermark_set(1)
        descriptor = _factor_result((_factor_row(watermark),)).descriptor
    membership_row = (
        result.rows[0]
        if isinstance(result, ParquetReadResult)
        and result.rows
        and isinstance(result.rows[0], FactorHistoryRow)
        else _factor_row(_watermark_set(1))
    )
    partition = kwargs.get("partition")
    group = (
        partition.factor_group
        if isinstance(partition, PartitionSpec)
        and partition.factor_group is not None
        else "microstructure"
    )
    kwargs.setdefault(
        "factor_group_membership",
        _factor_group_membership(membership_row, factor_group=group),
    )
    with patch(
        "l2flow_history.publication.read_factor_parquet_part",
        return_value=result,
    ):
        return assemble_factor_artifact(
            "/validated/archive",
            descriptor.basename,
            expected_descriptor=descriptor,
            **kwargs,
        )


def _factor_partition(row: FactorHistoryRow) -> PartitionSpec:
    return PartitionSpec(
        dataset=DatasetKind.FACTOR,
        trade_date=row.trade_date,
        factor_group="microstructure",
        factor_version=row.factor_version,
        bucket=row.bucket,
    )


def _factor_group_membership(
    row: FactorHistoryRow,
    *,
    factor_group: str = "microstructure",
) -> UpstreamValidatedFactorGroup:
    return UpstreamValidatedFactorGroup(
        factor_group=factor_group,
        factor_id=row.factor_id,
        factor_version=row.factor_version,
        factor_config_sha256=row.factor_config_sha256,
        factor_code_sha256=row.factor_code_sha256,
        state_schema_version=row.state_schema_version,
        state_schema_sha256=row.state_schema_sha256,
        implementation_status=row.implementation_status,
        numeric_dtype=row.numeric_dtype,
        validation_evidence_sha256=_hash("factor group catalog"),
    )


def _factor_receipt(
    row: FactorHistoryRow,
    watermarks: tuple[FactorInputWatermarkSet, ...],
    *,
    source_stream_id: int | None = None,
    shard_id: int | None = None,
    clock_epoch_digest: bytes | None = None,
    registry_sha256: bytes | None = None,
    canonical_generation: int = 888,
    begin_cursor: int = 1,
    end_cursor: int | None = None,
    min_wal: int | None = None,
    max_wal: int | None = None,
) -> SourceRangeReceipt:
    entries = tuple(watermark.entries[0] for watermark in watermarks)
    first = entries[0]
    return SourceRangeReceipt(
        namespace=SourceNamespace(
            trade_date=row.trade_date,
            source_stream_id=(
                first.source_stream_id
                if source_stream_id is None
                else source_stream_id
            ),
            origin_capture_date=first.origin_capture_date,
            origin_stream_day_id=first.origin_stream_day_id,
            family=first.family,
            shard_id=first.shard_id if shard_id is None else shard_id,
            origin_source_writer_instance=bytes.fromhex("33" * 16),
            origin_source_generation=999,
            canonical_generation=canonical_generation,
            clock_epoch_algorithm=first.clock_epoch_algorithm,
            clock_epoch_digest=(
                first.clock_epoch_digest
                if clock_epoch_digest is None
                else clock_epoch_digest
            ),
            schema_sha256=_hash("schema"),
            dtype_sha256=_hash("dtype"),
            registry_version=row.registry_version,
            registry_sha256=(
                row.registry_sha256
                if registry_sha256 is None
                else registry_sha256
            ),
            normalizer_build_sha256=_hash("normalizer build"),
            normalizer_config_sha256=_hash("normalizer config"),
        ),
        begin_canonical_cursor=begin_cursor,
        end_canonical_cursor=(
            max(entry.canonical_cursor for entry in entries)
            if end_cursor is None
            else end_cursor
        ),
        first_origin_ingress_sequence=1,
        last_origin_ingress_sequence=1,
        min_origin_wal_end_pos=(
            min(entry.max_consumed_origin_wal_end_pos for entry in entries)
            if min_wal is None
            else min_wal
        ),
        max_origin_wal_end_pos=(
            max(entry.max_consumed_origin_wal_end_pos for entry in entries)
            if max_wal is None
            else max_wal
        ),
        coverage_kind=CoverageKind.SEALED_COMMON_CUT,
        coverage_certificate_sha256=_CERTIFICATE,
    )


class CanonicalPublicationTests(unittest.TestCase):
    def test_checked_assembly_preserves_descriptor_and_only_provable_receipt_fields(self) -> None:
        row = _canonical_row(shard_event_id=9_000_000, wal_end=700)
        result = _canonical_result((row,))
        # The deliberately unrelated cursor and upstream-only shard/generation
        # prove that publication does not confuse shard_event_id with WAL or
        # claim those non-row facts were derived from the row.
        receipt = _receipt(
            row,
            begin_cursor=123,
            end_cursor=124,
            namespace=_source_namespace(
                row,
                shard_id=4_000_000_000,
                canonical_generation=987_654_321,
            ),
        )
        with patch(
            "l2flow_history.publication.read_canonical_parquet_part",
            return_value=result,
        ) as reader:
            artifact = assemble_canonical_artifact(
                "/validated/archive",
                result.descriptor.basename,
                expected_descriptor=result.descriptor,
                partition=_canonical_partition(row),
                upstream_receipts=_validated_receipts(receipt),
                visibility=ArtifactVisibility.VISIBLE,
            )
        reader.assert_called_once_with(
            "/validated/archive",
            result.descriptor.basename,
            expected_descriptor=result.descriptor,
        )
        self.assertEqual(artifact.file_sha256, result.descriptor.file_sha256)
        self.assertEqual(artifact.file_size_bytes, result.descriptor.byte_size)
        self.assertEqual(artifact.row_count, 1)
        self.assertEqual(
            artifact.logical_content_sha256,
            result.descriptor.logical_rows_sha256,
        )
        self.assertEqual(
            artifact.relative_path,
            artifact.partition.relative_prefix + result.descriptor.basename,
        )
        self.assertEqual(artifact.source_receipts, (receipt,))
        self.assertEqual(artifact.parquet_descriptor(), result.descriptor)

    def test_global_control_and_quality_project_to_global_not_exchange_market(self) -> None:
        for event_type in (3, 4):
            with self.subTest(event_type=event_type):
                row = _canonical_row(event_type=event_type, market=0)
                artifact = _assemble_canonical_readback(
                    _canonical_result((row,)),
                    partition=_canonical_partition(row),
                    upstream_receipts=_validated_receipts(_receipt(row)),
                )
                self.assertEqual(artifact.partition.market, "GLOBAL")

    def test_requires_backend_result_and_rejects_descriptor_or_partition_mismatch(self) -> None:
        row = _canonical_row()
        result = _canonical_result((row,))
        kwargs = {
            "partition": _canonical_partition(row),
            "upstream_receipts": _validated_receipts(_receipt(row)),
        }
        with self.assertRaises(PublicationValidationError):
            _assemble_canonical_readback(b"PAR1...", **kwargs)
        with self.assertRaises(PublicationValidationError):
            _assemble_canonical_readback(
                replace(
                    result,
                    descriptor=replace(
                        result.descriptor,
                        schema_name=FACTOR_PARQUET_SCHEMA_NAME_V1,
                    ),
                ),
                **kwargs,
            )
        with self.assertRaises(PublicationValidationError):
            _assemble_canonical_readback(
                replace(
                    result,
                    descriptor=replace(result.descriptor, row_count=2),
                ),
                **kwargs,
            )
        with self.assertRaises(PublicationValidationError):
            _assemble_canonical_readback(
                replace(
                    result,
                    descriptor=replace(
                        result.descriptor,
                        logical_rows_sha256=_hash("wrong logical"),
                    ),
                ),
                **kwargs,
            )
        other_bucket = (row.bucket + 1) % 32
        with self.assertRaises(PublicationValidationError):
            _assemble_canonical_readback(
                result,
                partition=_canonical_partition(row, bucket=other_bucket),
                upstream_receipts=kwargs["upstream_receipts"],
            )
        with self.assertRaises(PublicationValidationError):
            _assemble_canonical_readback(
                result,
                partition=_canonical_partition(row, market="SZ"),
                upstream_receipts=kwargs["upstream_receipts"],
            )
        snapshot = _canonical_row(event_type=1)
        with self.assertRaises(PublicationValidationError):
            _assemble_canonical_readback(
                _canonical_result((snapshot,)),
                partition=_canonical_partition(snapshot, event_type="tick"),
                upstream_receipts=_validated_receipts(_receipt(snapshot)),
            )

    def test_rejects_unproved_namespace_ingress_wal_and_ambiguous_coverage(self) -> None:
        row = _canonical_row(ingress=7, wal_end=700)
        result = _canonical_result((row,))
        partition = _canonical_partition(row)
        bad_receipts = (
            _receipt(
                row,
                namespace=_source_namespace(row, source_stream_id=8),
            ),
            _receipt(row, first_ingress=8, last_ingress=9),
            _receipt(row, min_wal=701, max_wal=800),
        )
        for receipt in bad_receipts:
            with self.subTest(receipt=receipt), self.assertRaises(
                PublicationValidationError
            ):
                _assemble_canonical_readback(
                    result,
                    partition=partition,
                    upstream_receipts=_validated_receipts(receipt),
                )
        with self.assertRaises(PublicationValidationError):
            _assemble_canonical_readback(
                result,
                partition=partition,
                upstream_receipts=_validated_receipts(
                    _receipt(row, begin_cursor=10, end_cursor=11),
                    _receipt(row, begin_cursor=20, end_cursor=21),
                ),
            )

    def test_canonical_receipt_lookup_is_indexed_by_projected_route(self) -> None:
        count = 64
        rows_list: list[CanonicalArchiveRow] = []
        instrument_id = 600000
        target_bucket = _canonical_row(instrument_id=instrument_id).bucket
        while len(rows_list) < count:
            candidate = _canonical_row(
                instrument_id=instrument_id,
                source_stream_id=len(rows_list) + 1,
                ingress=len(rows_list) + 1,
                wal_end=700 + len(rows_list),
            )
            if candidate.bucket == target_bucket:
                rows_list.append(candidate)
            instrument_id += 1
        rows = tuple(rows_list)
        receipts = tuple(_receipt(row) for row in rows)
        with patch.object(
            publication_module,
            "_receipt_covers_canonical_row",
            wraps=publication_module._receipt_covers_canonical_row,
        ) as coverage_check:
            artifact = _assemble_canonical_readback(
                _canonical_result(rows),
                partition=_canonical_partition(rows[0]),
                upstream_receipts=_validated_receipts(*receipts),
            )
        self.assertEqual(artifact.row_count, count)
        self.assertEqual(coverage_check.call_count, count)

    def test_explicit_upstream_wrapper_and_visibility_contract_are_required(self) -> None:
        row = _canonical_row()
        result = _canonical_result((row,))
        with self.assertRaises(PublicationValidationError):
            _assemble_canonical_readback(
                result,
                partition=_canonical_partition(row),
                upstream_receipts=(_receipt(row),),
            )
        with self.assertRaises(PublicationValidationError):
            UpstreamValidatedReceipts((_receipt(row),), bytes(32))
        with patch("l2flow_history.publication._MAX_PUBLICATION_RECEIPTS_V1", 1):
            with self.assertRaisesRegex(
                PublicationValidationError,
                "nonempty bounded tuple",
            ):
                UpstreamValidatedReceipts(
                    (_receipt(row), _receipt(row, begin_cursor=1, end_cursor=2)),
                    _EVIDENCE,
                )
        with self.assertRaises(PublicationValidationError):
            _assemble_canonical_readback(
                result,
                partition=_canonical_partition(row),
                upstream_receipts=_validated_receipts(
                    _receipt(row, coverage=CoverageKind.STAGING)
                ),
                visibility=ArtifactVisibility.VISIBLE,
            )
        with self.assertRaises(PublicationValidationError):
            _assemble_canonical_readback(
                result,
                partition=_canonical_partition(row),
                upstream_receipts=UpstreamValidatedReceipts(
                    (_receipt(row),),
                    _OTHER_EVIDENCE,
                ),
                visibility=ArtifactVisibility.VISIBLE,
            )


class FactorPublicationTests(unittest.TestCase):
    def test_every_row_resolves_persistent_identity_and_full_sidecar_map(self) -> None:
        first_watermark = _watermark_set(1)
        second_watermark = _watermark_set(2)
        sidecar = WatermarkSidecar(
            SidecarNamespace(_RUN_ID, 3),
            (second_watermark, first_watermark),
        )
        first = _factor_row(first_watermark, instrument_id=600000, asof_ns=100)
        second = _factor_row(second_watermark, instrument_id=600022, asof_ns=200)
        self.assertEqual(first.bucket, second.bucket)
        rows = (first, second)
        artifact = _assemble_factor_readback(
            _factor_result(rows),
            partition=_factor_partition(first),
            upstream_receipts=_validated_receipts(
                _factor_receipt(first, (first_watermark, second_watermark))
            ),
            sidecars={sidecar.namespace: sidecar},
            visibility=ArtifactVisibility.VISIBLE,
        )
        self.assertEqual(len(artifact.factor_sidecar_refs), 2)
        self.assertEqual(artifact.parquet_descriptor(), _factor_result(rows).descriptor)
        for reference, watermark in zip(
            artifact.factor_sidecar_refs,
            (first_watermark, second_watermark),
        ):
            self.assertEqual(sidecar.resolve(reference), watermark)
            self.assertEqual(
                reference.input_identity_sha256,
                watermark.input_identity_hash(),
            )

    def test_factor_receipt_projection_cursor_boundary_clock_registry_and_quality(self) -> None:
        watermark = _watermark_set(1)
        sidecar = WatermarkSidecar(
            SidecarNamespace(_RUN_ID, 3),
            (watermark,),
        )
        row = _factor_row(watermark)
        result = _factor_result((row,))
        partition = _factor_partition(row)

        bad_receipts = (
            _factor_receipt(row, (watermark,), source_stream_id=8),
            _factor_receipt(row, (watermark,), shard_id=100),
            _factor_receipt(
                row,
                (watermark,),
                clock_epoch_digest=_hash("wrong receipt clock"),
            ),
            _factor_receipt(
                row,
                (watermark,),
                registry_sha256=_hash("wrong receipt registry"),
            ),
            # canonical_cursor is exclusive-next: equality with begin means
            # this receipt contributed no consumed record.
            _factor_receipt(
                row,
                (watermark,),
                begin_cursor=watermark.entries[0].canonical_cursor,
                end_cursor=watermark.entries[0].canonical_cursor + 1,
            ),
            _factor_receipt(
                row,
                (watermark,),
                begin_cursor=1,
                end_cursor=watermark.entries[0].canonical_cursor - 1,
            ),
            _factor_receipt(
                row,
                (watermark,),
                min_wal=watermark.entries[0].max_consumed_origin_wal_end_pos + 1,
                max_wal=watermark.entries[0].max_consumed_origin_wal_end_pos + 2,
            ),
        )
        for receipt in bad_receipts:
            with self.subTest(receipt=receipt), self.assertRaises(
                PublicationValidationError
            ):
                _assemble_factor_readback(
                    result,
                    partition=partition,
                    upstream_receipts=_validated_receipts(receipt),
                    sidecars={sidecar.namespace: sidecar},
                )

        valid = _factor_receipt(row, (watermark,))
        unrelated = _factor_receipt(
            row,
            (watermark,),
            source_stream_id=8,
        )
        with self.assertRaises(PublicationValidationError):
            _assemble_factor_readback(
                result,
                partition=partition,
                upstream_receipts=_validated_receipts(valid, unrelated),
                sidecars={sidecar.namespace: sidecar},
            )

        wrong_clock_row = replace(row, clock_epoch_digest=_hash("wrong row clock"))
        with self.assertRaises(PublicationValidationError):
            _assemble_factor_readback(
                _factor_result((wrong_clock_row,)),
                partition=_factor_partition(wrong_clock_row),
                upstream_receipts=_validated_receipts(valid),
                sidecars={sidecar.namespace: sidecar},
            )
        wrong_quality_row = replace(row, input_quality_flags=1)
        with self.assertRaises(PublicationValidationError):
            _assemble_factor_readback(
                _factor_result((wrong_quality_row,)),
                partition=_factor_partition(wrong_quality_row),
                upstream_receipts=_validated_receipts(valid),
                sidecars={sidecar.namespace: sidecar},
            )

    def test_zero_consumption_uses_one_exact_route_without_positive_coverage(self) -> None:
        base = _watermark_set(1)
        zero_entry = replace(
            base.entries[0],
            canonical_cursor=0,
            max_consumed_origin_wal_end_pos=0,
            observed_raw_durable_wal_pos=0,
        )
        zero = FactorInputWatermarkSet(1, 20260722, (zero_entry,))
        zero_sidecar = WatermarkSidecar(SidecarNamespace(_RUN_ID, 3), (zero,))
        zero_row = _factor_row(zero)
        route_receipt = _factor_receipt(zero_row, (base,))
        artifact = _assemble_factor_readback(
            _factor_result((zero_row,)),
            partition=_factor_partition(zero_row),
            upstream_receipts=_validated_receipts(route_receipt),
            sidecars={zero_sidecar.namespace: zero_sidecar},
        )
        self.assertEqual(artifact.source_receipts, (route_receipt,))

        other_generation = replace(
            route_receipt,
            namespace=replace(
                route_receipt.namespace,
                canonical_generation=(
                    route_receipt.namespace.canonical_generation + 1
                ),
            ),
            begin_canonical_cursor=route_receipt.end_canonical_cursor,
            end_canonical_cursor=route_receipt.end_canonical_cursor + 1,
        )
        with self.assertRaisesRegex(
            PublicationValidationError,
            "exactly one complete SourceNamespace",
        ):
            _assemble_factor_readback(
                _factor_result((zero_row,)),
                partition=_factor_partition(zero_row),
                upstream_receipts=_validated_receipts(
                    route_receipt,
                    other_generation,
                ),
                sidecars={zero_sidecar.namespace: zero_sidecar},
            )

        malformed_entry = replace(base.entries[0])
        object.__setattr__(malformed_entry, "canonical_cursor", 0)
        malformed = FactorInputWatermarkSet(2, 20260722, (malformed_entry,))
        malformed_sidecar = WatermarkSidecar(
            SidecarNamespace(_RUN_ID, 3),
            (malformed,),
        )
        malformed_row = _factor_row(malformed)
        with self.assertRaisesRegex(
            PublicationValidationError,
            "zero Canonical cursor",
        ):
            _assemble_factor_readback(
                _factor_result((malformed_row,)),
                partition=_factor_partition(malformed_row),
                upstream_receipts=_validated_receipts(route_receipt),
                sidecars={malformed_sidecar.namespace: malformed_sidecar},
            )

    def test_factor_reference_and_coverage_are_cached_for_repeated_rows(self) -> None:
        watermark = _watermark_set(1)
        sidecar = WatermarkSidecar(
            SidecarNamespace(_RUN_ID, 3),
            (watermark,),
        )
        count = 64
        rows_list: list[FactorHistoryRow] = []
        instrument_id = 600000
        target_bucket = _factor_row(
            watermark,
            instrument_id=instrument_id,
        ).bucket
        while len(rows_list) < count:
            candidate = _factor_row(
                watermark,
                instrument_id=instrument_id,
                asof_ns=100 + len(rows_list),
            )
            if candidate.bucket == target_bucket:
                rows_list.append(candidate)
            instrument_id += 1
        rows = tuple(rows_list)
        with patch.object(
            publication_module,
            "_resolve_factor_reference",
            wraps=publication_module._resolve_factor_reference,
        ) as resolve_reference, patch.object(
            publication_module,
            "_validate_factor_watermark_coverage",
            wraps=publication_module._validate_factor_watermark_coverage,
        ) as validate_coverage:
            artifact = _assemble_factor_readback(
                _factor_result(rows),
                partition=_factor_partition(rows[0]),
                upstream_receipts=_validated_receipts(
                    _factor_receipt(rows[0], (watermark,))
                ),
                sidecars={sidecar.namespace: sidecar},
            )
        self.assertEqual(artifact.row_count, count)
        self.assertEqual(resolve_reference.call_count, 1)
        self.assertEqual(validate_coverage.call_count, 1)

    def test_adjacent_factor_ranges_cannot_hide_two_complete_namespaces(self) -> None:
        first_watermark = _watermark_set(1)
        second_watermark = _watermark_set(2)
        sidecar = WatermarkSidecar(
            SidecarNamespace(_RUN_ID, 3),
            (first_watermark, second_watermark),
        )
        first = _factor_row(first_watermark, instrument_id=600000, asof_ns=100)
        second_instrument = first.instrument_id + 1
        while True:
            second = _factor_row(
                second_watermark,
                instrument_id=second_instrument,
                asof_ns=200,
            )
            if second.bucket == first.bucket:
                break
            second_instrument += 1
        first_cursor = first_watermark.entries[0].canonical_cursor
        second_cursor = second_watermark.entries[0].canonical_cursor
        first_receipt = _factor_receipt(
            first,
            (first_watermark,),
            begin_cursor=1,
            end_cursor=first_cursor,
            canonical_generation=888,
        )
        second_receipt = _factor_receipt(
            second,
            (second_watermark,),
            begin_cursor=first_cursor,
            end_cursor=second_cursor,
            canonical_generation=889,
        )
        with self.assertRaisesRegex(
            PublicationValidationError,
            "exactly one complete SourceNamespace",
        ):
            _assemble_factor_readback(
                _factor_result((first, second)),
                partition=_factor_partition(first),
                upstream_receipts=_validated_receipts(
                    first_receipt,
                    second_receipt,
                ),
                sidecars={sidecar.namespace: sidecar},
            )

    def test_stale_observed_durable_is_not_publication_authority(self) -> None:

        stale = _watermark_set(2, observed_raw_durable_wal_pos=1)
        stale_sidecar = WatermarkSidecar(
            SidecarNamespace(_RUN_ID, 3),
            (stale,),
        )
        stale_row = _factor_row(stale)
        artifact = _assemble_factor_readback(
            _factor_result((stale_row,)),
            partition=_factor_partition(stale_row),
            upstream_receipts=_validated_receipts(
                _factor_receipt(stale_row, (stale,))
            ),
            sidecars={stale_sidecar.namespace: stale_sidecar},
            visibility=ArtifactVisibility.VISIBLE,
        )
        self.assertEqual(artifact.visibility, ArtifactVisibility.VISIBLE)

    def test_rejects_orphan_identity_trade_version_bucket_and_descriptor_mismatch(self) -> None:
        watermark = _watermark_set(1)
        sidecar = WatermarkSidecar(
            SidecarNamespace(_RUN_ID, 3),
            (watermark,),
        )
        row = _factor_row(watermark)
        result = _factor_result((row,))
        receipt = _validated_receipts(_factor_receipt(row, (watermark,)))
        kwargs = {
            "partition": _factor_partition(row),
            "upstream_receipts": receipt,
            "sidecars": {sidecar.namespace: sidecar},
        }
        extra_sidecar = WatermarkSidecar(
            SidecarNamespace(_RUN_ID, 4),
            (watermark,),
        )

        class LyingMapping(dict):
            def __len__(self) -> int:
                return 0

        with patch("l2flow_history.publication._MAX_PUBLICATION_SIDECARS_V1", 1):
            with self.assertRaisesRegex(
                PublicationValidationError,
                "exceeds the V1 bound",
            ):
                _assemble_factor_readback(
                    result,
                    partition=kwargs["partition"],
                    upstream_receipts=receipt,
                    sidecars=LyingMapping(
                        {
                            sidecar.namespace: sidecar,
                            extra_sidecar.namespace: extra_sidecar,
                        }
                    ),
                )
        with self.assertRaises(PublicationValidationError):
            _assemble_factor_readback(result, **{**kwargs, "sidecars": {}})
        wrong_identity_row = _factor_row(
            watermark,
            input_identity_sha256=_hash("wrong input identity"),
        )
        with self.assertRaises(PublicationValidationError):
            _assemble_factor_readback(
                _factor_result((wrong_identity_row,)),
                partition=_factor_partition(wrong_identity_row),
                upstream_receipts=receipt,
                sidecars={sidecar.namespace: sidecar},
            )
        with self.assertRaises(PublicationValidationError):
            _assemble_factor_readback(
                result,
                partition=replace(kwargs["partition"], factor_version="2.0.0"),
                upstream_receipts=receipt,
                sidecars=kwargs["sidecars"],
            )
        with self.assertRaises(PublicationValidationError):
            _assemble_factor_readback(
                result,
                partition=replace(kwargs["partition"], factor_group="arbitrary"),
                upstream_receipts=receipt,
                sidecars=kwargs["sidecars"],
                factor_group_membership=_factor_group_membership(row),
            )
        with self.assertRaises(PublicationValidationError):
            _assemble_factor_readback(
                replace(
                    result,
                    descriptor=replace(
                        result.descriptor,
                        logical_rows_sha256=_hash("wrong factor logical"),
                    ),
                ),
                **kwargs,
            )
        other_bucket = (row.bucket + 1) % 32
        with self.assertRaises(PublicationValidationError):
            _assemble_factor_readback(
                result,
                partition=replace(kwargs["partition"], bucket=other_bucket),
                upstream_receipts=receipt,
                sidecars=kwargs["sidecars"],
            )

        other_day_watermark = _watermark_set(1, trade_date=20260721)
        other_day_sidecar = WatermarkSidecar(
            SidecarNamespace(_RUN_ID, 3),
            (other_day_watermark,),
        )
        other_day_row = _factor_row(other_day_watermark)
        with self.assertRaises(PublicationValidationError):
            _assemble_factor_readback(
                _factor_result((other_day_row,)),
                partition=_factor_partition(other_day_row),
                upstream_receipts=receipt,
                sidecars={other_day_sidecar.namespace: other_day_sidecar},
            )


@unittest.skipUnless(_HAS_PYARROW, "real publication integration requires PyArrow")
class RealPublicationIntegrationTests(unittest.TestCase):
    def test_real_canonical_and_factor_backend_readback_paths(self) -> None:
        canonical = _canonical_row()
        with tempfile.TemporaryDirectory() as directory:
            descriptor = write_canonical_parquet_part(
                (canonical,),
                directory,
                "part-real-canonical.parquet",
            )
            artifact = assemble_canonical_artifact(
                directory,
                descriptor.basename,
                expected_descriptor=descriptor,
                partition=_canonical_partition(canonical),
                upstream_receipts=_validated_receipts(_receipt(canonical)),
                visibility=ArtifactVisibility.VISIBLE,
            )
            self.assertEqual(artifact.parquet_descriptor(), descriptor)

    def test_real_store_manifest_sidecar_query_and_tamper_rejection(self) -> None:
        from l2flow_history.query import (
            CanonicalLineageCatalog,
            FactorLineageCatalog,
            FixedQuerySnapshot,
            QueryBudget,
            merge_canonical_history,
            merge_factor_history,
            read_manifest_visible_canonical_rows,
            read_manifest_visible_factor_rows,
        )

        canonical = _canonical_row()
        canonical_partition = _canonical_partition(canonical)
        watermark = _watermark_set(1)
        factor = _factor_row(watermark)
        factor_partition = _factor_partition(factor)
        sidecar = WatermarkSidecar(
            SidecarNamespace(_RUN_ID, 3),
            (watermark,),
        )

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            canonical_directory = root / canonical_partition.relative_prefix
            canonical_directory.mkdir(parents=True)
            canonical_descriptor = write_canonical_parquet_part(
                (canonical,),
                canonical_directory,
                "part-e2e-canonical.parquet",
            )
            canonical_artifact = assemble_canonical_artifact(
                canonical_directory,
                canonical_descriptor.basename,
                expected_descriptor=canonical_descriptor,
                partition=canonical_partition,
                upstream_receipts=_validated_receipts(_receipt(canonical)),
                visibility=ArtifactVisibility.VISIBLE,
            )

            factor_directory = root / factor_partition.relative_prefix
            factor_directory.mkdir(parents=True)
            factor_descriptor = write_factor_parquet_part(
                (factor,),
                factor_directory,
                "part-e2e-factor.parquet",
            )
            factor_artifact = assemble_factor_artifact(
                factor_directory,
                factor_descriptor.basename,
                expected_descriptor=factor_descriptor,
                partition=factor_partition,
                upstream_receipts=_validated_receipts(
                    _factor_receipt(factor, (watermark,))
                ),
                sidecars={sidecar.namespace: sidecar},
                factor_group_membership=_factor_group_membership(factor),
                visibility=ArtifactVisibility.VISIBLE,
            )

            store = AtomicManifestStore(root)
            sidecar_descriptor = store.publish_sidecar(
                sidecar,
                relative_path="watermarks.l2ws",
            )
            manifest = HistoryManifest(
                run_id=_RUN_ID,
                generation=1,
                previous_manifest_sha256=None,
                artifacts=(canonical_artifact, factor_artifact),
                sidecars=(sidecar_descriptor,),
            )
            manifest_digest = store.publish(
                manifest,
                expected_current_generation=None,
                expected_current_sha256=None,
                sidecars={sidecar.namespace: sidecar},
            )
            loaded = store.load_current_with_sidecars()
            self.assertIsNotNone(loaded)
            assert loaded is not None
            loaded_manifest, loaded_digest, loaded_sidecars = loaded
            self.assertEqual((loaded_manifest, loaded_digest), (manifest, manifest_digest))
            self.assertEqual(loaded_sidecars, (sidecar,))

            hot_frontier = _hash("phase8 e2e fixed hot frontier")
            budget = QueryBudget(
                max_sources=8,
                max_scanned_rows=16,
                max_result_rows=16,
                max_bytes=1 << 26,
                deadline_monotonic_ns=time.monotonic_ns() + 5_000_000_000,
            )

            canonical_catalog = CanonicalLineageCatalog.from_manifests(
                (loaded_manifest,),
                hot_frontier_sha256=hot_frontier,
            )
            canonical_snapshot = FixedQuerySnapshot(
                snapshot_sequence=1,
                cold_manifest_set_sha256=canonical_catalog.manifest_set_sha256,
                hot_frontier_sha256=hot_frontier,
                canonical_lineage_catalog_sha256=canonical_catalog.catalog_sha256,
            )
            cold_canonical = read_manifest_visible_canonical_rows(
                root,
                canonical_artifact.file_sha256,
                snapshot=canonical_snapshot,
                lineage_catalog=canonical_catalog,
                budget=budget,
            )
            canonical_result = merge_canonical_history(
                cold_canonical,
                (),
                snapshot=canonical_snapshot,
                snapshot_observer=lambda: canonical_snapshot,
                lineage_catalog=canonical_catalog,
                budget=budget,
            )
            self.assertEqual(tuple(row.archive_row for row in canonical_result.rows), (canonical,))
            self.assertEqual(len(canonical_result.rows[0].raw_locator_evidence), 1)

            factor_catalog = FactorLineageCatalog.from_manifests(
                (loaded_manifest,),
                sidecars=loaded_sidecars,
                hot_frontier_sha256=hot_frontier,
            )
            factor_snapshot = FixedQuerySnapshot(
                snapshot_sequence=2,
                cold_manifest_set_sha256=factor_catalog.manifest_set_sha256,
                hot_frontier_sha256=hot_frontier,
                factor_lineage_catalog_sha256=factor_catalog.catalog_sha256,
            )
            cold_factor = read_manifest_visible_factor_rows(
                root,
                factor_artifact.file_sha256,
                snapshot=factor_snapshot,
                lineage_catalog=factor_catalog,
                budget=budget,
            )
            factor_result = merge_factor_history(
                cold_factor,
                (),
                snapshot=factor_snapshot,
                snapshot_observer=lambda: factor_snapshot,
                lineage_catalog=factor_catalog,
                budget=budget,
            )
            self.assertEqual(tuple(row.archive_row for row in factor_result.rows), (factor,))
            self.assertEqual(len(factor_result.rows[0].lineage_references), 1)

            canonical_file = canonical_directory / canonical_descriptor.basename
            with canonical_file.open("r+b") as handle:
                first = handle.read(1)
                self.assertEqual(len(first), 1)
                handle.seek(0)
                handle.write(bytes([first[0] ^ 0x01]))
                handle.flush()
            with self.assertRaises(ParquetValidationError):
                read_manifest_visible_canonical_rows(
                    root,
                    canonical_artifact.file_sha256,
                    snapshot=canonical_snapshot,
                    lineage_catalog=canonical_catalog,
                    budget=budget,
                )

        watermark = _watermark_set(1)
        factor = _factor_row(watermark)
        sidecar = WatermarkSidecar(
            SidecarNamespace(_RUN_ID, 3),
            (watermark,),
        )
        with tempfile.TemporaryDirectory() as directory:
            descriptor = write_factor_parquet_part(
                (factor,),
                directory,
                "part-real-factor.parquet",
            )
            artifact = assemble_factor_artifact(
                directory,
                descriptor.basename,
                expected_descriptor=descriptor,
                partition=_factor_partition(factor),
                upstream_receipts=_validated_receipts(
                    _factor_receipt(factor, (watermark,))
                ),
                sidecars={sidecar.namespace: sidecar},
                factor_group_membership=_factor_group_membership(factor),
                visibility=ArtifactVisibility.VISIBLE,
            )
            self.assertEqual(artifact.parquet_descriptor(), descriptor)


if __name__ == "__main__":
    unittest.main()
