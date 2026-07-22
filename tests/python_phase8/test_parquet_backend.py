"""Repository-local Phase-8 Parquet tests.

The direct ``pyarrow.parquet`` reads below prove that the artifacts are real
Apache Parquet and bypass L2Flow's reader.  They are not a second independent
Parquet implementation.  Cross-implementation compatibility and sustained
production performance remain formal external acceptance boundaries.
"""

from __future__ import annotations

from dataclasses import replace
import hashlib
import os
from pathlib import Path
import stat
import struct
import sys
import tempfile
import unittest
from unittest.mock import patch


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPOSITORY_ROOT / "python"))

from l2flow_history.model import (  # noqa: E402
    MAX_PARQUET_PART_BYTES_V1,
    CanonicalArchiveRow,
    FactorHistoryRow,
    FactorImplementationStatus,
    HistoryConflictError,
    HistoryValidationError,
    canonical_logical_rows_sha256,
    canonical_sort_and_dedupe,
    canonical_sort_key_wire_v1,
    factor_logical_rows_sha256,
    factor_sort_and_dedupe,
    factor_sort_key_wire_v1,
    instrument_bucket_v1,
)
from l2flow_history import parquet_backend as backend  # noqa: E402


_HEADER = struct.Struct("<IHHIIIIQQQQQQqqqIIHHHBB")
_RECORD_SIZES = {1: 2048, 2: 192, 3: 192, 4: 256}


def canonical_row(
    *,
    event_type: int = 2,
    instrument_id: int = 600000,
    origin_ingress_sequence: int = 1,
    exchange_sequence: int = 1,
    exchange_time_ns: int = 100,
    source_stream_id: int = 7,
    origin_capture_date: int = 20260722,
    stream_day_byte: int = 0x41,
) -> CanonicalArchiveRow:
    """Build a synthetic record with a valid common Canonical V1 envelope."""
    record_size = _RECORD_SIZES[event_type]
    if event_type in (1, 2):
        market = 1
        instrument = instrument_id
        channel = 1
        vendor_sequence = origin_ingress_sequence
        sub_index = 0
        message_id = 4 if event_type == 1 else 24
        projected_exchange_sequence = 0 if event_type == 1 else exchange_sequence
        projected_exchange_time = exchange_time_ns
    elif event_type == 3:
        market = 1
        instrument = instrument_id
        channel = 1
        vendor_sequence = origin_ingress_sequence
        sub_index = 1
        message_id = 24
        projected_exchange_sequence = exchange_sequence
        projected_exchange_time = exchange_time_ns
    elif event_type == 4:
        market = 0
        instrument = 0
        channel = 0
        vendor_sequence = 0
        sub_index = 0
        message_id = 1
        projected_exchange_sequence = 0
        projected_exchange_time = 0
    else:
        raise AssertionError("test helper event_type")
    header = _HEADER.pack(
        0x3145434D,
        1,
        event_type,
        record_size,
        source_stream_id,
        1,
        20260722,
        0,
        origin_ingress_sequence,
        origin_ingress_sequence,
        origin_ingress_sequence * 100,
        vendor_sequence,
        projected_exchange_sequence,
        projected_exchange_time,
        1_000 + origin_ingress_sequence,
        2_000 + origin_ingress_sequence,
        instrument,
        channel,
        market,
        101,
        message_id,
        4 if event_type != 4 else 1,
        sub_index,
    )
    payload = bytes([event_type]) * (record_size - _HEADER.size)
    return CanonicalArchiveRow(
        origin_capture_date=origin_capture_date,
        origin_stream_day_id=bytes([stream_day_byte]) * 16,
        record_bytes=header + payload,
    )


def factor_row(
    *,
    instrument_id: int = 600000,
    asof_ns: int = 100,
    identity_byte: int = 0x31,
    run_byte: int = 0x41,
    table_generation: int = 1,
    watermark_set_id: int = 1,
    clock_label: int = 11,
    latency_ns: int = 12,
    quality: int = 0,
    status: FactorImplementationStatus = (
        FactorImplementationStatus.PASSTHROUGH_PLACEHOLDER
    ),
    value: float | None = None,
) -> FactorHistoryRow:
    kwargs = dict(
        factor_id="book_imbalance",
        factor_version="1.0.0",
        factor_config_sha256=b"c" * 32,
        factor_code_sha256=b"d" * 32,
        state_schema_version=1,
        state_schema_sha256=b"s" * 32,
        trade_date=20260722,
        registry_version=5,
        registry_sha256=b"r" * 32,
        instrument_id=instrument_id,
        asof_ns=asof_ns,
        run_id=bytes([run_byte]) * 16,
        watermark_table_generation=table_generation,
        watermark_set_id=watermark_set_id,
        input_identity_sha256=bytes([identity_byte]) * 32,
        clock_epoch_algorithm=1,
        clock_epoch_digest=b"e" * 32,
        clock_epoch_label=clock_label,
        input_quality_flags=quality,
        implementation_status=status,
        calculation_latency_ns=latency_ns,
    )
    return FactorHistoryRow.from_float(value=value, **kwargs)


class LogicalModelTests(unittest.TestCase):
    def test_bucket_v1_golden(self) -> None:
        self.assertEqual(
            {
                value: instrument_bucket_v1(value)
                for value in (0, 1, 2, 100, 600000, 300750, 0xFFFFFFFF)
            },
            {
                0: 4,
                1: 7,
                2: 24,
                100: 21,
                600000: 30,
                300750: 4,
                0xFFFFFFFF: 18,
            },
        )
        with self.assertRaises(HistoryValidationError):
            instrument_bucket_v1(True)
        with self.assertRaises(HistoryValidationError):
            instrument_bucket_v1(1 << 32)

    def test_logical_hash_and_sort_key_goldens(self) -> None:
        canonical = canonical_row()
        factor = factor_row()
        self.assertEqual(
            canonical_logical_rows_sha256(iter((canonical,))).hex(),
            "7a63d48d4ebb99be01fec003ef5fbe3a22631553a313a0b671c44f8ac988bb4e",
        )
        self.assertEqual(
            canonical_sort_key_wire_v1(canonical).hex(),
            "c027090064000000000000000100000000000000070000000100000000000000"
            "0072273501414141414141414141414141414141410100",
        )
        self.assertEqual(
            factor_logical_rows_sha256(iter((factor,))).hex(),
            "3741c4863ce9f464c07555dbe7b57aa95250fef3b0fb9cf077069b270489088b",
        )
        self.assertEqual(
            factor.semantic_content_sha256.hex(),
            "69a6b27b1861322816a47a71fa86a3dcb470dd6d0ed63f994c7994ba9c9035d7",
        )
        self.assertEqual(
            factor_sort_key_wire_v1(factor).hex(),
            "c027090064000000000000003131313131313131313131313131313131313131"
            "3131313131313131313131310e00626f6f6b5f696d62616c616e63650500312e"
            "302e30",
        )

    def test_canonical_sort_dedupe_complete_identity_and_conflict(self) -> None:
        later = canonical_row(
            instrument_id=600022,
            origin_ingress_sequence=2,
            exchange_sequence=2,
            exchange_time_ns=200,
        )
        earlier = canonical_row(
            instrument_id=600000,
            origin_ingress_sequence=1,
            exchange_sequence=1,
            exchange_time_ns=100,
        )
        self.assertEqual(earlier.bucket, later.bucket)
        self.assertEqual(
            canonical_sort_and_dedupe((later, earlier, earlier)),
            (earlier, later),
        )
        self.assertEqual(
            canonical_logical_rows_sha256((later, earlier, earlier)),
            canonical_logical_rows_sha256((earlier, later)),
        )

        # Same record-local cursor in another stream-day namespace is distinct.
        another_day = canonical_row(stream_day_byte=0x42)
        self.assertEqual(len(canonical_sort_and_dedupe((earlier, another_day))), 2)

        conflicting = canonical_row(exchange_time_ns=101)
        self.assertEqual(conflicting.identity_key, earlier.identity_key)
        with self.assertRaises(HistoryConflictError):
            canonical_sort_and_dedupe((earlier, conflicting))

    def test_row_iterables_stop_at_the_v1_bound_before_full_materialization(self) -> None:
        row = canonical_row()

        def three_rows():
            yield row
            yield row
            yield row

        with patch("l2flow_history.model.MAX_ARCHIVE_PART_ROWS_V1", 2):
            with self.assertRaisesRegex(HistoryValidationError, "part bound"):
                canonical_sort_and_dedupe(three_rows())

    def test_factor_placeholder_and_replay_semantic_equivalence(self) -> None:
        with self.assertRaises(HistoryValidationError):
            factor_row(value=1.0)
        with self.assertRaises(HistoryValidationError):
            replace(factor_row(), value_bits=1)

        physical_a = factor_row(
            run_byte=0x44,
            table_generation=9,
            watermark_set_id=8,
            clock_label=7,
            latency_ns=6,
        )
        physical_b = factor_row(
            run_byte=0x43,
            table_generation=2,
            watermark_set_id=3,
            clock_label=4,
            latency_ns=5,
        )
        self.assertEqual(physical_a.identity_key, physical_b.identity_key)
        self.assertEqual(
            physical_a.semantic_content_sha256,
            physical_b.semantic_content_sha256,
        )
        self.assertNotEqual(physical_a.content_sha256, physical_b.content_sha256)
        self.assertEqual(factor_sort_and_dedupe((physical_a, physical_b)), (physical_b,))
        self.assertEqual(
            factor_logical_rows_sha256((physical_a, physical_b)),
            factor_logical_rows_sha256((physical_b, physical_a)),
        )

        conflicting = replace(physical_a, input_quality_flags=1)
        with self.assertRaises(HistoryConflictError):
            factor_sort_and_dedupe((physical_a, conflicting))

    def test_factor_v1_freezes_float64_and_persistent_watermark_reference(self) -> None:
        implemented = factor_row(
            status=FactorImplementationStatus.IMPLEMENTED,
            value=1.25,
            table_generation=17,
        )
        self.assertEqual(implemented.numeric_dtype, "float64")
        self.assertEqual(implemented.value, 1.25)
        self.assertEqual(implemented.watermark_table_generation, 17)
        with self.assertRaises(HistoryValidationError):
            replace(implemented, numeric_dtype="float32")
        with self.assertRaises(HistoryValidationError):
            replace(implemented, state_schema_sha256=bytes(32))


class RealParquetTests(unittest.TestCase):
    def test_all_canonical_families_are_exact_real_zstd_parquet(self) -> None:
        import pyarrow.parquet as pq

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for event_type, family in (
                (1, "snapshot"),
                (2, "tick"),
                (3, "quality"),
                (4, "control"),
            ):
                row = canonical_row(event_type=event_type)
                basename = f"part-{family}.parquet"
                descriptor = backend.write_canonical_parquet_part(
                    (row,),
                    root,
                    basename,
                    row_group_size=1,
                )
                path = root / basename
                raw_file = path.read_bytes()
                self.assertEqual(raw_file[:4], b"PAR1")
                self.assertEqual(raw_file[-4:], b"PAR1")
                self.assertEqual(
                    hashlib.sha256(raw_file).digest(),
                    descriptor.file_sha256,
                )
                self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
                self.assertEqual(descriptor.row_count, 1)
                self.assertEqual(descriptor.min_sort_key_v1, canonical_sort_key_wire_v1(row))
                self.assertEqual(descriptor.max_sort_key_v1, canonical_sort_key_wire_v1(row))

                result = backend.read_canonical_parquet_part(
                    root,
                    basename,
                    expected_descriptor=descriptor,
                )
                self.assertEqual(result.rows, (row,))

                # Direct PyArrow read bypasses L2Flow's reader implementation.
                direct = pq.read_table(path)
                self.assertEqual(direct.column("record_bytes").to_pylist(), [row.record_bytes])
                self.assertEqual(direct.column("event_type").to_pylist(), [event_type])
                metadata = pq.ParquetFile(path).metadata
                for group_index in range(metadata.num_row_groups):
                    group = metadata.row_group(group_index)
                    for column_index in range(group.num_columns):
                        self.assertEqual(group.column(column_index).compression, "ZSTD")
                footer = metadata.metadata or {}
                self.assertFalse(any(b"file_sha256" in key for key in footer))

    def test_canonical_writer_sorts_dedupes_and_freezes_bounds(self) -> None:
        first = canonical_row(instrument_id=600000, origin_ingress_sequence=1)
        second = canonical_row(
            instrument_id=600022,
            origin_ingress_sequence=2,
            exchange_sequence=2,
            exchange_time_ns=200,
        )
        self.assertEqual(first.bucket, second.bucket)
        with tempfile.TemporaryDirectory() as directory:
            descriptor = backend.write_canonical_parquet_part(
                (second, first, first),
                directory,
                "part-sorted.parquet",
                row_group_size=1,
            )
            self.assertEqual(descriptor.row_count, 2)
            self.assertEqual(
                descriptor.logical_rows_sha256,
                canonical_logical_rows_sha256((first, second)),
            )
            self.assertEqual(descriptor.min_sort_key_v1, canonical_sort_key_wire_v1(first))
            self.assertEqual(descriptor.max_sort_key_v1, canonical_sort_key_wire_v1(second))
            self.assertEqual(
                backend.read_canonical_parquet_part(
                    directory,
                    "part-sorted.parquet",
                    expected_descriptor=descriptor,
                ).rows,
                (first, second),
            )

    def test_factor_placeholder_and_implemented_float64_roundtrip(self) -> None:
        import pyarrow.parquet as pq

        placeholder_first = factor_row(instrument_id=600000, asof_ns=100)
        placeholder_second = factor_row(
            instrument_id=600022,
            asof_ns=200,
            identity_byte=0x32,
            watermark_set_id=2,
        )
        self.assertEqual(placeholder_first.bucket, placeholder_second.bucket)
        with tempfile.TemporaryDirectory() as directory:
            descriptor = backend.write_factor_parquet_part(
                (placeholder_second, placeholder_first),
                directory,
                "part-placeholder.parquet",
                row_group_size=1,
            )
            result = backend.read_factor_parquet_part(
                directory,
                "part-placeholder.parquet",
                expected_descriptor=descriptor,
            )
            self.assertEqual(result.rows, (placeholder_first, placeholder_second))
            self.assertTrue(all(not row.value_valid for row in result.rows))
            self.assertTrue(all(row.value_bits == 0 for row in result.rows))
            self.assertEqual(
                descriptor.min_sort_key_v1,
                factor_sort_key_wire_v1(placeholder_first),
            )
            self.assertEqual(
                descriptor.max_sort_key_v1,
                factor_sort_key_wire_v1(placeholder_second),
            )

            direct = pq.read_table(Path(directory) / "part-placeholder.parquet")
            self.assertEqual(direct.column("numeric_dtype").to_pylist(), ["float64", "float64"])
            self.assertEqual(direct.column("value_valid").to_pylist(), [False, False])
            self.assertEqual(
                direct.column("watermark_table_generation").to_pylist(),
                [1, 1],
            )

        implemented = factor_row(
            status=FactorImplementationStatus.IMPLEMENTED,
            value=-0.0,
        )
        with tempfile.TemporaryDirectory() as directory:
            descriptor = backend.write_factor_parquet_part(
                (implemented,),
                directory,
                "part-implemented.parquet",
            )
            result = backend.read_factor_parquet_part(
                directory,
                "part-implemented.parquet",
                expected_descriptor=descriptor,
            )
            self.assertEqual(result.rows, (implemented,))
            self.assertEqual(result.rows[0].value_bits, 1 << 63)

    def test_noreplace_never_overwrites_existing_final(self) -> None:
        original = canonical_row()
        replacement = canonical_row(origin_ingress_sequence=2, exchange_sequence=2)
        with tempfile.TemporaryDirectory() as directory:
            first = backend.write_canonical_parquet_part(
                (original,), directory, "part-fixed.parquet"
            )
            path = Path(directory) / "part-fixed.parquet"
            before = path.read_bytes()
            with self.assertRaises(FileExistsError):
                backend.write_canonical_parquet_part(
                    (replacement,), directory, "part-fixed.parquet"
                )
            self.assertEqual(path.read_bytes(), before)
            self.assertEqual(
                backend.read_canonical_parquet_part(
                    directory,
                    "part-fixed.parquet",
                    expected_descriptor=first,
                ).rows,
                (original,),
            )

    def test_tamper_and_external_descriptor_mismatch_fail_closed(self) -> None:
        row = canonical_row()
        with tempfile.TemporaryDirectory() as directory:
            descriptor = backend.write_canonical_parquet_part(
                (row,), directory, "part-tamper.parquet"
            )
            bad_descriptor = replace(descriptor, file_sha256=bytes(32))
            with self.assertRaises(backend.ParquetValidationError):
                backend.read_canonical_parquet_part(
                    directory,
                    "part-tamper.parquet",
                    expected_descriptor=bad_descriptor,
                )

            with open(Path(directory) / "part-tamper.parquet", "ab") as artifact:
                artifact.write(b"tamper")
                artifact.flush()
                os.fsync(artifact.fileno())
            with self.assertRaises(backend.ParquetValidationError):
                backend.read_canonical_parquet_part(
                    directory,
                    "part-tamper.parquet",
                    expected_descriptor=descriptor,
                )

    def test_read_transaction_detects_controlled_same_inode_mutation(self) -> None:
        row = canonical_row()
        mutation_stages = (
            backend.FAULT_AFTER_READ_DECODE,
            backend.FAULT_AFTER_READ_FINAL_HASH,
        )
        for mutation_stage in mutation_stages:
            with self.subTest(stage=mutation_stage), tempfile.TemporaryDirectory() as directory:
                basename = "part-read-transaction.parquet"
                descriptor = backend.write_canonical_parquet_part(
                    (row,), directory, basename
                )
                path = Path(directory) / basename
                original_size = path.stat().st_size
                mutated = False

                def mutate(stage: str) -> None:
                    nonlocal mutated
                    if stage != mutation_stage or mutated:
                        return
                    mutated = True
                    with open(path, "r+b", buffering=0) as artifact:
                        artifact.seek(4)
                        original = artifact.read(1)
                        self.assertEqual(len(original), 1)
                        artifact.seek(4)
                        artifact.write(bytes((original[0] ^ 0x01,)))
                        os.fsync(artifact.fileno())

                with self.assertRaisesRegex(
                    backend.ParquetValidationError,
                    "changed during read transaction",
                ):
                    backend.read_canonical_parquet_part(
                        directory,
                        basename,
                        expected_descriptor=descriptor,
                        fault_injector=mutate,
                    )
                self.assertTrue(mutated)
                self.assertEqual(path.stat().st_size, original_size)

    def test_read_transaction_hook_order_and_stable_hash_binding(self) -> None:
        row = canonical_row()
        observed: list[str] = []
        with tempfile.TemporaryDirectory() as directory:
            basename = "part-stable-read.parquet"
            descriptor = backend.write_canonical_parquet_part(
                (row,), directory, basename
            )
            result = backend.read_canonical_parquet_part(
                directory,
                basename,
                expected_descriptor=descriptor,
                fault_injector=observed.append,
            )
            self.assertEqual(result.rows, (row,))
            self.assertEqual(result.descriptor, descriptor)
            self.assertEqual(
                observed,
                [
                    backend.FAULT_AFTER_READ_INITIAL_HASH,
                    backend.FAULT_AFTER_READ_DECODE,
                    backend.FAULT_AFTER_READ_FINAL_HASH,
                ],
            )
            self.assertEqual(
                result.descriptor.file_sha256,
                hashlib.sha256((Path(directory) / basename).read_bytes()).digest(),
            )

    def test_footer_logical_hash_and_exact_sort_bounds_are_recomputed(self) -> None:
        import pyarrow.parquet as pq

        row = canonical_row()
        mutations = (
            (b"l2flow.archive.min_sort_key_v1", b"00", "min/max sort key"),
            (b"l2flow.archive.logical_rows_sha256", b"00" * 32, "logical_rows"),
        )
        for key, value, expected_error in mutations:
            with self.subTest(key=key), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "part-footer.parquet"
                backend.write_canonical_parquet_part(
                    (row,), directory, path.name, row_group_size=1
                )
                table = pq.read_table(path)
                metadata = dict(table.schema.metadata or {})
                metadata[key] = value
                pq.write_table(
                    table.replace_schema_metadata(metadata),
                    path,
                    compression="zstd",
                    version="2.6",
                    data_page_version="2.0",
                    row_group_size=1,
                    store_schema=True,
                )
                os.chmod(path, 0o600)
                with self.assertRaisesRegex(
                    backend.ParquetValidationError,
                    expected_error,
                ):
                    backend.read_canonical_parquet_part(directory, path.name)

    def test_missing_pyarrow_seam_has_no_fallback_or_artifact(self) -> None:
        row = canonical_row()
        original_importer = backend._PYARROW_IMPORTER

        def missing(_: str) -> object:
            raise ModuleNotFoundError("test seam: pyarrow unavailable")

        with tempfile.TemporaryDirectory() as directory:
            backend._PYARROW_IMPORTER = missing
            try:
                with self.assertRaises(backend.ParquetDependencyError):
                    backend.write_canonical_parquet_part(
                        (row,), directory, "part-missing.parquet"
                    )
            finally:
                backend._PYARROW_IMPORTER = original_importer
            self.assertEqual(list(Path(directory).iterdir()), [])

    def test_basename_and_sparse_oversize_are_rejected_before_arrow_read(self) -> None:
        row = canonical_row()
        with tempfile.TemporaryDirectory() as directory:
            for invalid in (
                "../part-escape.parquet",
                "/tmp/part-absolute.parquet",
                "part-no-extension",
                ".parquet",
            ):
                with self.assertRaises(HistoryValidationError):
                    backend.write_canonical_parquet_part((row,), directory, invalid)

            path = Path(directory) / "part-oversize.parquet"
            fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600)
            try:
                os.fchmod(fd, 0o600)
                os.ftruncate(fd, MAX_PARQUET_PART_BYTES_V1 + 1)
            finally:
                os.close(fd)
            with self.assertRaisesRegex(
                backend.ParquetValidationError,
                "exceeds the V1 byte bound",
            ):
                backend.read_canonical_parquet_part(directory, path.name)

    @unittest.skipUnless(hasattr(os, "mkfifo"), "POSIX FIFO test")
    def test_fifo_substitution_fails_before_blocking_or_arrow_decode(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "part-fifo.parquet"
            os.mkfifo(path, 0o600)
            with self.assertRaisesRegex(
                backend.ParquetValidationError,
                "not a regular file",
            ):
                backend.read_canonical_parquet_part(directory, path.name)

    def test_fault_matrix_before_and_after_noreplace_publish(self) -> None:
        class InjectedCrash(RuntimeError):
            pass

        row = canonical_row()
        source_snapshot = row

        def injector_for(target: str):
            def inject(stage: str) -> None:
                if stage == target:
                    raise InjectedCrash(stage)

            return inject

        before_publish = (
            backend.FAULT_AFTER_TEMP_WRITE,
            backend.FAULT_AFTER_TEMP_FSYNC,
            backend.FAULT_AFTER_TEMP_VALIDATION,
        )
        for stage in before_publish:
            with self.subTest(stage=stage), tempfile.TemporaryDirectory() as directory:
                with self.assertRaises(InjectedCrash):
                    backend.write_canonical_parquet_part(
                        (row,),
                        directory,
                        "part-crash.parquet",
                        fault_injector=injector_for(stage),
                    )
                self.assertFalse((Path(directory) / "part-crash.parquet").exists())
                self.assertEqual(row, source_snapshot)

        after_publish = (
            backend.FAULT_AFTER_RENAME,
            backend.FAULT_AFTER_DIRECTORY_FSYNC,
        )
        for stage in after_publish:
            with self.subTest(stage=stage), tempfile.TemporaryDirectory() as directory:
                with self.assertRaises(InjectedCrash):
                    backend.write_canonical_parquet_part(
                        (row,),
                        directory,
                        "part-crash.parquet",
                        fault_injector=injector_for(stage),
                    )
                path = Path(directory) / "part-crash.parquet"
                self.assertTrue(path.exists())
                before_retry = path.read_bytes()
                self.assertEqual(
                    backend.read_canonical_parquet_part(
                        directory, "part-crash.parquet"
                    ).rows,
                    (row,),
                )
                with self.assertRaises(FileExistsError):
                    backend.write_canonical_parquet_part(
                        (row,), directory, "part-crash.parquet"
                    )
                self.assertEqual(path.read_bytes(), before_retry)
                self.assertEqual(row, source_snapshot)


if __name__ == "__main__":
    unittest.main()
