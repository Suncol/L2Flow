"""Real Apache Parquet backend for Phase-8 V1 archive rows.

PyArrow is imported only when a read or write operation is requested.  If it
or its ZSTD codec is unavailable, the operation fails closed; there is no
similarly-named private format and no silent fallback.

Publication is Linux/POSIX-oriented: a same-directory typed temporary file is
written and read back through a retained descriptor, fsynced, published with
``renameat2(RENAME_NOREPLACE)``, and followed by a directory fsync.  The
whole-file SHA-256 is returned only in :class:`ParquetPartDescriptor`; putting
that digest in the file being hashed would be self-referential.
"""

from __future__ import annotations

from dataclasses import dataclass
import ctypes
import errno
import hashlib
import importlib
import os
import re
import secrets
import stat
from typing import Any, Callable, Iterable

from .model import (
    ARCHIVE_BUCKET_COUNT_V1,
    ARCHIVE_BUCKET_HASH_ALGORITHM_V1,
    ARCHIVE_BUCKET_HASH_VERSION_V1,
    CANONICAL_HEADER_FIELD_NAMES_V1,
    CANONICAL_PARQUET_SCHEMA_NAME_V1,
    FACTOR_NUMERIC_DTYPE_V1,
    FACTOR_PARQUET_SCHEMA_NAME_V1,
    MAX_ARCHIVE_PART_ROWS_V1,
    MAX_PARQUET_PART_BYTES_V1,
    CanonicalArchiveRow,
    FactorHistoryRow,
    FactorImplementationStatus,
    HistoryValidationError,
    ParquetPartDescriptor,
    canonical_logical_rows_sha256,
    canonical_sort_and_dedupe,
    canonical_sort_key_wire_v1,
    factor_logical_rows_sha256,
    factor_sort_and_dedupe,
    factor_sort_key_wire_v1,
    validate_parquet_basename_v1,
)


class ParquetBackendError(RuntimeError):
    """Base error for physical archive operations."""


class ParquetDependencyError(ParquetBackendError):
    """A real PyArrow/Parquet/ZSTD capability is unavailable."""


class ParquetValidationError(ParquetBackendError):
    """A Parquet file failed schema, metadata, content, or hash validation."""


class ParquetReadBudgetExceeded(ParquetBackendError):
    """A valid read candidate exceeds the caller's pre-decode resource budget."""


class ParquetPublishError(ParquetBackendError):
    """The requested atomic/no-replace publication could not be completed."""


# Explicit test seam.  Keeping this as import_module (rather than importing
# pyarrow at module load) also lets applications use the logical model without
# installing a Parquet runtime.
_PYARROW_IMPORTER: Callable[[str], Any] = importlib.import_module

_RENAME_NOREPLACE = 1
_FOOTER_KEYS_V1 = frozenset(
    {
        b"l2flow.archive.bucket",
        b"l2flow.archive.bucket_count",
        b"l2flow.archive.bucket_hash_algorithm",
        b"l2flow.archive.bucket_hash_version",
        b"l2flow.archive.compression",
        b"l2flow.archive.logical_rows_sha256",
        b"l2flow.archive.max_sort_key_v1",
        b"l2flow.archive.min_sort_key_v1",
        b"l2flow.archive.numeric_dtype",
        b"l2flow.archive.physical_format",
        b"l2flow.archive.row_count",
        b"l2flow.archive.schema_name",
        b"l2flow.archive.schema_version",
    }
)
_UINT_ASCII_RE = re.compile(rb"(?:0|[1-9][0-9]*)\Z")
_LOWER_HEX_32_RE = re.compile(rb"[0-9a-f]{64}\Z")
MAX_PARQUET_ROW_GROUPS_V1 = 65_536
MAX_PARQUET_DECODED_BYTES_V1 = 4 * 1024 * 1024 * 1024

FAULT_AFTER_TEMP_WRITE = "after_temp_write"
FAULT_AFTER_TEMP_FSYNC = "after_temp_fsync"
FAULT_AFTER_TEMP_VALIDATION = "after_temp_validation"
FAULT_AFTER_RENAME = "after_rename"
FAULT_AFTER_DIRECTORY_FSYNC = "after_directory_fsync"
FAULT_AFTER_READ_INITIAL_HASH = "after_read_initial_hash"
FAULT_AFTER_READ_DECODE = "after_read_decode"
FAULT_AFTER_READ_FINAL_HASH = "after_read_final_hash"
_FAULT_STAGES = frozenset(
    {
        FAULT_AFTER_TEMP_WRITE,
        FAULT_AFTER_TEMP_FSYNC,
        FAULT_AFTER_TEMP_VALIDATION,
        FAULT_AFTER_RENAME,
        FAULT_AFTER_DIRECTORY_FSYNC,
        FAULT_AFTER_READ_INITIAL_HASH,
        FAULT_AFTER_READ_DECODE,
        FAULT_AFTER_READ_FINAL_HASH,
    }
)


@dataclass(frozen=True, slots=True)
class ParquetReadResult:
    rows: tuple[CanonicalArchiveRow | FactorHistoryRow, ...]
    descriptor: ParquetPartDescriptor


def _inject_fault(
    fault_injector: Callable[[str], None] | None,
    stage: str,
) -> None:
    if stage not in _FAULT_STAGES:
        raise AssertionError("internal fault injection stage is not frozen")
    if fault_injector is not None:
        fault_injector(stage)


def _load_pyarrow() -> tuple[Any, Any]:
    try:
        pa = _PYARROW_IMPORTER("pyarrow")
        pq = _PYARROW_IMPORTER("pyarrow.parquet")
    except (ImportError, ModuleNotFoundError) as error:
        raise ParquetDependencyError(
            "real Apache Parquet support requires PyArrow; no fallback is allowed"
        ) from error
    try:
        zstd_available = bool(pa.Codec.is_available("zstd"))
    except Exception as error:  # malformed/incompatible module also fails closed
        raise ParquetDependencyError("PyArrow does not expose a usable codec API") from error
    if not zstd_available:
        raise ParquetDependencyError("the installed PyArrow lacks the ZSTD codec")
    return pa, pq


def _footer_metadata_v1(
    *,
    schema_name: str,
    row_count: int,
    bucket: int,
    logical_rows_sha256: bytes,
    min_sort_key_v1: bytes,
    max_sort_key_v1: bytes,
) -> dict[bytes, bytes]:
    numeric_dtype = (
        FACTOR_NUMERIC_DTYPE_V1.encode("ascii")
        if schema_name == FACTOR_PARQUET_SCHEMA_NAME_V1
        else b"not_applicable"
    )
    return {
        b"l2flow.archive.bucket": str(bucket).encode("ascii"),
        b"l2flow.archive.bucket_count": str(ARCHIVE_BUCKET_COUNT_V1).encode("ascii"),
        b"l2flow.archive.bucket_hash_algorithm": (
            ARCHIVE_BUCKET_HASH_ALGORITHM_V1.encode("ascii")
        ),
        b"l2flow.archive.bucket_hash_version": str(
            ARCHIVE_BUCKET_HASH_VERSION_V1
        ).encode("ascii"),
        b"l2flow.archive.compression": b"ZSTD",
        b"l2flow.archive.logical_rows_sha256": logical_rows_sha256.hex().encode(
            "ascii"
        ),
        b"l2flow.archive.max_sort_key_v1": max_sort_key_v1.hex().encode("ascii"),
        b"l2flow.archive.min_sort_key_v1": min_sort_key_v1.hex().encode("ascii"),
        b"l2flow.archive.numeric_dtype": numeric_dtype,
        b"l2flow.archive.physical_format": b"APACHE_PARQUET",
        b"l2flow.archive.row_count": str(row_count).encode("ascii"),
        b"l2flow.archive.schema_name": schema_name.encode("ascii"),
        b"l2flow.archive.schema_version": b"1",
    }


def _canonical_schema(pa: Any, metadata: dict[bytes, bytes]) -> Any:
    fields = [
        pa.field("origin_capture_date", pa.uint32(), nullable=False),
        pa.field("origin_stream_day_id", pa.binary(16), nullable=False),
        pa.field("record_bytes", pa.binary(), nullable=False),
        pa.field("record_sha256", pa.binary(32), nullable=False),
        pa.field("magic", pa.uint32(), nullable=False),
        pa.field("schema_version", pa.uint16(), nullable=False),
        pa.field("event_type", pa.uint16(), nullable=False),
        pa.field("record_size", pa.uint32(), nullable=False),
        pa.field("source_stream_id", pa.uint32(), nullable=False),
        pa.field("connection_epoch", pa.uint32(), nullable=False),
        pa.field("trade_date", pa.uint32(), nullable=False),
        pa.field("quality_flags", pa.uint64(), nullable=False),
        pa.field("shard_event_id", pa.uint64(), nullable=False),
        pa.field("origin_ingress_sequence", pa.uint64(), nullable=False),
        pa.field("origin_wal_end_pos", pa.uint64(), nullable=False),
        pa.field("vendor_sequence_id", pa.uint64(), nullable=False),
        pa.field("exchange_sequence", pa.uint64(), nullable=False),
        pa.field("exchange_time_ns", pa.int64(), nullable=False),
        pa.field("recv_realtime_ns", pa.int64(), nullable=False),
        pa.field("recv_monotonic_ns", pa.int64(), nullable=False),
        pa.field("instrument_id", pa.uint32(), nullable=False),
        pa.field("channel", pa.uint32(), nullable=False),
        pa.field("market", pa.uint16(), nullable=False),
        pa.field("origin_service_version", pa.uint16(), nullable=False),
        pa.field("origin_message_id", pa.uint16(), nullable=False),
        pa.field("origin_service_id", pa.uint8(), nullable=False),
        pa.field("sub_index", pa.uint8(), nullable=False),
        pa.field("bucket", pa.uint8(), nullable=False),
    ]
    return pa.schema(fields, metadata=metadata)


def _factor_schema(pa: Any, metadata: dict[bytes, bytes]) -> Any:
    fields = [
        pa.field("factor_id", pa.string(), nullable=False),
        pa.field("factor_version", pa.string(), nullable=False),
        pa.field("factor_config_sha256", pa.binary(32), nullable=False),
        pa.field("factor_code_sha256", pa.binary(32), nullable=False),
        pa.field("state_schema_version", pa.uint32(), nullable=False),
        pa.field("state_schema_sha256", pa.binary(32), nullable=False),
        pa.field("trade_date", pa.uint32(), nullable=False),
        pa.field("registry_version", pa.uint64(), nullable=False),
        pa.field("registry_sha256", pa.binary(32), nullable=False),
        pa.field("instrument_id", pa.uint32(), nullable=False),
        pa.field("asof_ns", pa.int64(), nullable=False),
        pa.field("numeric_dtype", pa.string(), nullable=False),
        pa.field("value_bits", pa.uint64(), nullable=False),
        pa.field("value_valid", pa.bool_(), nullable=False),
        pa.field("run_id", pa.binary(16), nullable=False),
        pa.field("watermark_table_generation", pa.uint64(), nullable=False),
        pa.field("watermark_set_id", pa.uint64(), nullable=False),
        pa.field("input_identity_sha256", pa.binary(32), nullable=False),
        pa.field("clock_epoch_algorithm", pa.uint32(), nullable=False),
        pa.field("clock_epoch_digest", pa.binary(32), nullable=False),
        pa.field("clock_epoch_label", pa.uint64(), nullable=False),
        pa.field("input_quality_flags", pa.uint64(), nullable=False),
        pa.field("implementation_status", pa.string(), nullable=False),
        pa.field("calculation_latency_ns", pa.uint64(), nullable=False),
        pa.field("bucket", pa.uint8(), nullable=False),
    ]
    return pa.schema(fields, metadata=metadata)


def _canonical_table(pa: Any, rows: tuple[CanonicalArchiveRow, ...], schema: Any) -> Any:
    columns: dict[str, list[object]] = {
        "origin_capture_date": [],
        "origin_stream_day_id": [],
        "record_bytes": [],
        "record_sha256": [],
        **{name: [] for name in CANONICAL_HEADER_FIELD_NAMES_V1},
        "bucket": [],
    }
    for row in rows:
        columns["origin_capture_date"].append(row.origin_capture_date)
        columns["origin_stream_day_id"].append(row.origin_stream_day_id)
        columns["record_bytes"].append(row.record_bytes)
        columns["record_sha256"].append(row.record_sha256)
        for name, value in row.header.as_dict().items():
            columns[name].append(value)
        columns["bucket"].append(row.bucket)
    return pa.Table.from_pydict(columns, schema=schema)


def _factor_table(pa: Any, rows: tuple[FactorHistoryRow, ...], schema: Any) -> Any:
    columns = {
        "factor_id": [row.factor_id for row in rows],
        "factor_version": [row.factor_version for row in rows],
        "factor_config_sha256": [row.factor_config_sha256 for row in rows],
        "factor_code_sha256": [row.factor_code_sha256 for row in rows],
        "state_schema_version": [row.state_schema_version for row in rows],
        "state_schema_sha256": [row.state_schema_sha256 for row in rows],
        "trade_date": [row.trade_date for row in rows],
        "registry_version": [row.registry_version for row in rows],
        "registry_sha256": [row.registry_sha256 for row in rows],
        "instrument_id": [row.instrument_id for row in rows],
        "asof_ns": [row.asof_ns for row in rows],
        "numeric_dtype": [row.numeric_dtype for row in rows],
        "value_bits": [row.value_bits for row in rows],
        "value_valid": [row.value_valid for row in rows],
        "run_id": [row.run_id for row in rows],
        "watermark_table_generation": [
            row.watermark_table_generation for row in rows
        ],
        "watermark_set_id": [row.watermark_set_id for row in rows],
        "input_identity_sha256": [row.input_identity_sha256 for row in rows],
        "clock_epoch_algorithm": [row.clock_epoch_algorithm for row in rows],
        "clock_epoch_digest": [row.clock_epoch_digest for row in rows],
        "clock_epoch_label": [row.clock_epoch_label for row in rows],
        "input_quality_flags": [row.input_quality_flags for row in rows],
        "implementation_status": [row.implementation_status.value for row in rows],
        "calculation_latency_ns": [row.calculation_latency_ns for row in rows],
        "bucket": [row.bucket for row in rows],
    }
    return pa.Table.from_pydict(columns, schema=schema)


def _require_nonempty_homogeneous_canonical(
    rows: Iterable[CanonicalArchiveRow],
) -> tuple[CanonicalArchiveRow, ...]:
    ordered = canonical_sort_and_dedupe(rows)
    if not ordered:
        raise HistoryValidationError("a Canonical Parquet part must not be empty")
    first = ordered[0]
    partition = (
        first.header.trade_date,
        first.header.market,
        first.header.event_type,
        first.bucket,
    )
    if any(
        (
            row.header.trade_date,
            row.header.market,
            row.header.event_type,
            row.bucket,
        )
        != partition
        for row in ordered
    ):
        raise HistoryValidationError(
            "Canonical part rows must share trade_date/market/event_type/bucket"
        )
    return ordered


def _require_nonempty_homogeneous_factor(
    rows: Iterable[FactorHistoryRow],
) -> tuple[FactorHistoryRow, ...]:
    ordered = factor_sort_and_dedupe(rows)
    if not ordered:
        raise HistoryValidationError("a factor Parquet part must not be empty")
    first = ordered[0]
    partition = (
        first.trade_date,
        first.factor_id,
        first.factor_version,
        first.factor_config_sha256,
        first.factor_code_sha256,
        first.state_schema_version,
        first.state_schema_sha256,
        first.registry_version,
        first.registry_sha256,
        first.implementation_status,
        first.numeric_dtype,
        first.bucket,
    )
    if any(
        (
            row.trade_date,
            row.factor_id,
            row.factor_version,
            row.factor_config_sha256,
            row.factor_code_sha256,
            row.state_schema_version,
            row.state_schema_sha256,
            row.registry_version,
            row.registry_sha256,
            row.implementation_status,
            row.numeric_dtype,
            row.bucket,
        )
        != partition
        for row in ordered
    ):
        raise HistoryValidationError(
            "factor part rows must share partition/version/schema/registry/bucket"
        )
    return ordered


def _validate_row_group_size(value: object) -> int:
    if type(value) is not int or value < 1 or value > MAX_ARCHIVE_PART_ROWS_V1:
        raise HistoryValidationError(
            "row_group_size must be in [1, MAX_ARCHIVE_PART_ROWS_V1]"
        )
    return value


def _directory_fd(directory: str | os.PathLike[str]) -> int:
    path = os.fspath(directory)
    if not isinstance(path, str) or not path:
        raise HistoryValidationError("directory must be a non-empty text path")
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | os.O_CLOEXEC
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags)
    except OSError as error:
        raise ParquetPublishError("cannot securely open Parquet directory") from error
    if not stat.S_ISDIR(os.fstat(fd).st_mode):
        os.close(fd)
        raise ParquetPublishError("Parquet directory descriptor is not a directory")
    return fd


def _validate_regular_fd(fd: int) -> os.stat_result:
    info = os.fstat(fd)
    if not stat.S_ISREG(info.st_mode):
        raise ParquetValidationError("Parquet artifact is not a regular file")
    if info.st_nlink != 1:
        raise ParquetValidationError("Parquet artifact must have exactly one hard link")
    if info.st_mode & 0o077:
        raise ParquetValidationError("Parquet artifact must not grant group/world access")
    if info.st_size < 12:
        raise ParquetValidationError("Parquet artifact is too short")
    if info.st_size > MAX_PARQUET_PART_BYTES_V1:
        raise ParquetValidationError("Parquet artifact exceeds the V1 byte bound")
    return info


def _file_state_v1(info: os.stat_result) -> tuple[int, int, int, int, int, int, int]:
    """Return every inode fact frozen across one complete read transaction."""

    return (
        info.st_dev,
        info.st_ino,
        info.st_mode,
        info.st_nlink,
        info.st_size,
        info.st_mtime_ns,
        info.st_ctime_ns,
    )


def _require_same_file_state_v1(
    expected: os.stat_result,
    observed: os.stat_result,
) -> None:
    if _file_state_v1(observed) != _file_state_v1(expected):
        raise ParquetValidationError("Parquet artifact changed during read transaction")


def _sha256_fd(fd: int, size: int) -> bytes:
    hasher = hashlib.sha256()
    offset = 0
    while offset < size:
        chunk = os.pread(fd, min(1024 * 1024, size - offset), offset)
        if not chunk:
            raise ParquetValidationError("unexpected EOF while hashing Parquet file")
        hasher.update(chunk)
        offset += len(chunk)
    return hasher.digest()


def _check_parquet_magic(fd: int, size: int) -> None:
    if os.pread(fd, 4, 0) != b"PAR1" or os.pread(fd, 4, size - 4) != b"PAR1":
        raise ParquetValidationError("artifact does not have Apache Parquet PAR1 framing")


def _rename_noreplace(dir_fd: int, source: str, destination: str) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    try:
        renameat2 = libc.renameat2
    except AttributeError as error:
        raise ParquetPublishError(
            "renameat2(RENAME_NOREPLACE) is unavailable; refusing unsafe fallback"
        ) from error
    renameat2.argtypes = [
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    ]
    renameat2.restype = ctypes.c_int
    result = renameat2(
        dir_fd,
        os.fsencode(source),
        dir_fd,
        os.fsencode(destination),
        _RENAME_NOREPLACE,
    )
    if result != 0:
        code = ctypes.get_errno()
        if code == errno.EEXIST:
            raise FileExistsError(code, os.strerror(code), destination)
        raise ParquetPublishError(
            f"renameat2(RENAME_NOREPLACE) failed: {os.strerror(code)}"
        )


def _parse_footer_metadata(
    schema: Any,
    expected_schema_name: str,
) -> tuple[int, int, bytes, bytes, bytes]:
    metadata = schema.metadata
    if metadata is None or set(metadata) != _FOOTER_KEYS_V1:
        raise ParquetValidationError("Parquet footer metadata is not the exact V1 set")
    if metadata[b"l2flow.archive.schema_name"] != expected_schema_name.encode("ascii"):
        raise ParquetValidationError("Parquet footer schema_name mismatch")
    expected_numeric_dtype = (
        FACTOR_NUMERIC_DTYPE_V1.encode("ascii")
        if expected_schema_name == FACTOR_PARQUET_SCHEMA_NAME_V1
        else b"not_applicable"
    )
    exact_values = {
        b"l2flow.archive.bucket_count": b"32",
        b"l2flow.archive.bucket_hash_algorithm": (
            ARCHIVE_BUCKET_HASH_ALGORITHM_V1.encode("ascii")
        ),
        b"l2flow.archive.bucket_hash_version": b"1",
        b"l2flow.archive.compression": b"ZSTD",
        b"l2flow.archive.numeric_dtype": expected_numeric_dtype,
        b"l2flow.archive.physical_format": b"APACHE_PARQUET",
        b"l2flow.archive.schema_version": b"1",
    }
    for key, expected in exact_values.items():
        if metadata[key] != expected:
            raise ParquetValidationError(f"Parquet footer {key!r} mismatch")

    row_count_raw = metadata[b"l2flow.archive.row_count"]
    bucket_raw = metadata[b"l2flow.archive.bucket"]
    logical_raw = metadata[b"l2flow.archive.logical_rows_sha256"]
    min_sort_raw = metadata[b"l2flow.archive.min_sort_key_v1"]
    max_sort_raw = metadata[b"l2flow.archive.max_sort_key_v1"]
    if _UINT_ASCII_RE.fullmatch(row_count_raw) is None:
        raise ParquetValidationError("Parquet footer row_count is not canonical uint ASCII")
    if _UINT_ASCII_RE.fullmatch(bucket_raw) is None:
        raise ParquetValidationError("Parquet footer bucket is not canonical uint ASCII")
    if _LOWER_HEX_32_RE.fullmatch(logical_raw) is None:
        raise ParquetValidationError("Parquet footer logical hash is not lowercase SHA-256")
    for name, value in (
        ("min_sort_key_v1", min_sort_raw),
        ("max_sort_key_v1", max_sort_raw),
    ):
        if (
            len(value) < 2
            or len(value) > 2048
            or len(value) % 2 != 0
            or re.fullmatch(rb"[0-9a-f]+", value) is None
        ):
            raise ParquetValidationError(f"Parquet footer {name} is not canonical hex")
    row_count = int(row_count_raw)
    bucket = int(bucket_raw)
    if row_count < 1 or row_count > MAX_ARCHIVE_PART_ROWS_V1:
        raise ParquetValidationError("Parquet footer row_count is outside V1 bounds")
    if bucket < 0 or bucket >= ARCHIVE_BUCKET_COUNT_V1:
        raise ParquetValidationError("Parquet footer bucket is outside V1 bounds")
    return (
        row_count,
        bucket,
        bytes.fromhex(logical_raw.decode("ascii")),
        bytes.fromhex(min_sort_raw.decode("ascii")),
        bytes.fromhex(max_sort_raw.decode("ascii")),
    )


def _read_arrow_table_from_fd(
    fd: int,
    *,
    pa: Any,
    pq: Any,
    expected_schema_name: str,
    maximum_decoded_bytes: int,
) -> tuple[Any, int, int, bytes, bytes, bytes]:
    info = _validate_regular_fd(fd)
    _check_parquet_magic(fd, info.st_size)
    try:
        with os.fdopen(os.dup(fd), "rb") as source:
            parquet_file = pq.ParquetFile(source)
            parquet_metadata = parquet_file.metadata
            arrow_schema = parquet_file.schema_arrow
            (
                row_count,
                bucket,
                logical_hash,
                min_sort_key,
                max_sort_key,
            ) = _parse_footer_metadata(arrow_schema, expected_schema_name)
            expected_metadata = _footer_metadata_v1(
                schema_name=expected_schema_name,
                row_count=row_count,
                bucket=bucket,
                logical_rows_sha256=logical_hash,
                min_sort_key_v1=min_sort_key,
                max_sort_key_v1=max_sort_key,
            )
            expected_schema = (
                _canonical_schema(pa, expected_metadata)
                if expected_schema_name == CANONICAL_PARQUET_SCHEMA_NAME_V1
                else _factor_schema(pa, expected_metadata)
            )
            if not arrow_schema.equals(expected_schema, check_metadata=True):
                raise ParquetValidationError(
                    "Parquet Arrow schema is not the exact V1 schema"
                )
            if parquet_metadata.num_rows != row_count:
                raise ParquetValidationError("Parquet physical/footer row count mismatch")
            if (
                parquet_metadata.num_row_groups < 1
                or parquet_metadata.num_row_groups > MAX_PARQUET_ROW_GROUPS_V1
                or parquet_metadata.num_row_groups > row_count
            ):
                raise ParquetValidationError("Parquet row-group count is outside V1 bounds")
            physical_rows = 0
            decoded_bytes = 0
            compressed_bytes = 0
            for row_group_index in range(parquet_metadata.num_row_groups):
                row_group = parquet_metadata.row_group(row_group_index)
                if row_group.num_rows < 1 or row_group.num_columns != len(expected_schema):
                    raise ParquetValidationError("Parquet row-group shape is invalid")
                physical_rows += row_group.num_rows
                if row_group.total_byte_size < 0:
                    raise ParquetValidationError("Parquet row-group byte size is invalid")
                decoded_bytes += row_group.total_byte_size
                for column_index in range(row_group.num_columns):
                    column = row_group.column(column_index)
                    if column.compression != "ZSTD":
                        raise ParquetValidationError(
                            "every Parquet column chunk must use ZSTD"
                        )
                    if column.total_compressed_size < 0:
                        raise ParquetValidationError(
                            "Parquet compressed column size is invalid"
                        )
                    compressed_bytes += column.total_compressed_size
            if physical_rows != row_count:
                raise ParquetValidationError("Parquet row-group row count mismatch")
            if decoded_bytes > MAX_PARQUET_DECODED_BYTES_V1:
                raise ParquetValidationError("Parquet decoded bytes exceed the V1 bound")
            if decoded_bytes > maximum_decoded_bytes:
                raise ParquetReadBudgetExceeded(
                    "Parquet decoded bytes exceed the caller read budget"
                )
            if compressed_bytes > info.st_size:
                raise ParquetValidationError("Parquet compressed sizes exceed file size")
            # Only allocate/decompress after all cheap footer bounds and the
            # exact logical schema have passed.
            table = parquet_file.read()
    except (ParquetValidationError, ParquetReadBudgetExceeded):
        raise
    except Exception as error:
        raise ParquetValidationError("PyArrow could not read the Parquet artifact") from error
    if table.num_rows != row_count:
        raise ParquetValidationError("Parquet footer/table row count mismatch")
    if not table.schema.equals(expected_schema, check_metadata=True):
        raise ParquetValidationError("Parquet table schema changed during read")
    if any(table.column(index).null_count != 0 for index in range(table.num_columns)):
        raise ParquetValidationError("V1 Parquet columns must not contain nulls")
    return table, row_count, bucket, logical_hash, min_sort_key, max_sort_key


def _canonical_rows_from_table(
    table: Any,
    *,
    row_count: int,
    bucket: int,
    logical_hash: bytes,
    min_sort_key: bytes,
    max_sort_key: bytes,
) -> tuple[CanonicalArchiveRow, ...]:
    columns = table.to_pydict()
    rows: list[CanonicalArchiveRow] = []
    try:
        for index in range(row_count):
            row = CanonicalArchiveRow(
                origin_capture_date=columns["origin_capture_date"][index],
                origin_stream_day_id=columns["origin_stream_day_id"][index],
                record_bytes=columns["record_bytes"][index],
            )
            if columns["record_sha256"][index] != row.record_sha256:
                raise ParquetValidationError("Canonical record_sha256 projection mismatch")
            if columns["bucket"][index] != row.bucket or row.bucket != bucket:
                raise ParquetValidationError("Canonical row/footer bucket mismatch")
            for name, value in row.header.as_dict().items():
                if columns[name][index] != value:
                    raise ParquetValidationError(
                        f"Canonical header projection mismatch for {name}"
                    )
            rows.append(row)
    except (KeyError, IndexError, TypeError, HistoryValidationError) as error:
        raise ParquetValidationError("Canonical Parquet row decoding failed") from error
    result = tuple(rows)
    try:
        canonical = canonical_sort_and_dedupe(result)
    except HistoryValidationError as error:
        raise ParquetValidationError("Canonical Parquet rows conflict") from error
    if canonical != result:
        raise ParquetValidationError("Canonical Parquet rows are not in frozen total order")
    if canonical_logical_rows_sha256(result) != logical_hash:
        raise ParquetValidationError("Canonical logical_rows_sha256 mismatch")
    if (
        canonical_sort_key_wire_v1(result[0]) != min_sort_key
        or canonical_sort_key_wire_v1(result[-1]) != max_sort_key
    ):
        raise ParquetValidationError("Canonical exact min/max sort key mismatch")
    _require_nonempty_homogeneous_canonical(result)
    return result


def _factor_rows_from_table(
    table: Any,
    *,
    row_count: int,
    bucket: int,
    logical_hash: bytes,
    min_sort_key: bytes,
    max_sort_key: bytes,
) -> tuple[FactorHistoryRow, ...]:
    columns = table.to_pydict()
    rows: list[FactorHistoryRow] = []
    try:
        for index in range(row_count):
            row = FactorHistoryRow(
                factor_id=columns["factor_id"][index],
                factor_version=columns["factor_version"][index],
                factor_config_sha256=columns["factor_config_sha256"][index],
                factor_code_sha256=columns["factor_code_sha256"][index],
                state_schema_version=columns["state_schema_version"][index],
                state_schema_sha256=columns["state_schema_sha256"][index],
                trade_date=columns["trade_date"][index],
                registry_version=columns["registry_version"][index],
                registry_sha256=columns["registry_sha256"][index],
                instrument_id=columns["instrument_id"][index],
                asof_ns=columns["asof_ns"][index],
                numeric_dtype=columns["numeric_dtype"][index],
                value_bits=columns["value_bits"][index],
                value_valid=columns["value_valid"][index],
                run_id=columns["run_id"][index],
                watermark_table_generation=columns[
                    "watermark_table_generation"
                ][index],
                watermark_set_id=columns["watermark_set_id"][index],
                input_identity_sha256=columns["input_identity_sha256"][index],
                clock_epoch_algorithm=columns["clock_epoch_algorithm"][index],
                clock_epoch_digest=columns["clock_epoch_digest"][index],
                clock_epoch_label=columns["clock_epoch_label"][index],
                input_quality_flags=columns["input_quality_flags"][index],
                implementation_status=FactorImplementationStatus(
                    columns["implementation_status"][index]
                ),
                calculation_latency_ns=columns["calculation_latency_ns"][index],
            )
            if columns["bucket"][index] != row.bucket or row.bucket != bucket:
                raise ParquetValidationError("factor row/footer bucket mismatch")
            rows.append(row)
    except (KeyError, IndexError, TypeError, ValueError, HistoryValidationError) as error:
        raise ParquetValidationError("factor Parquet row decoding failed") from error
    result = tuple(rows)
    try:
        canonical = factor_sort_and_dedupe(result)
    except HistoryValidationError as error:
        raise ParquetValidationError("factor Parquet rows conflict") from error
    if canonical != result:
        raise ParquetValidationError("factor Parquet rows are not in frozen total order")
    if factor_logical_rows_sha256(result) != logical_hash:
        raise ParquetValidationError("factor logical_rows_sha256 mismatch")
    if (
        factor_sort_key_wire_v1(result[0]) != min_sort_key
        or factor_sort_key_wire_v1(result[-1]) != max_sort_key
    ):
        raise ParquetValidationError("factor exact min/max sort key mismatch")
    _require_nonempty_homogeneous_factor(result)
    return result


def _descriptor_from_stable_read(
    *,
    info: os.stat_result,
    file_sha256: bytes,
    basename: str,
    schema_name: str,
    row_count: int,
    bucket: int,
    logical_hash: bytes,
    min_sort_key: bytes,
    max_sort_key: bytes,
) -> ParquetPartDescriptor:
    return ParquetPartDescriptor(
        basename=basename,
        schema_name=schema_name,
        row_count=row_count,
        byte_size=info.st_size,
        bucket=bucket,
        logical_rows_sha256=logical_hash,
        file_sha256=file_sha256,
        min_sort_key_v1=min_sort_key,
        max_sort_key_v1=max_sort_key,
    )


def _read_validated_fd(
    fd: int,
    *,
    basename: str,
    schema_name: str,
    pa: Any,
    pq: Any,
    fault_injector: Callable[[str], None] | None = None,
    maximum_decoded_bytes: int = MAX_PARQUET_DECODED_BYTES_V1,
    expected_descriptor: ParquetPartDescriptor | None = None,
) -> ParquetReadResult:
    if fault_injector is not None and not callable(fault_injector):
        raise HistoryValidationError("fault_injector must be callable or None")

    # Bind decoded rows and the returned whole-file SHA to one stable inode
    # state.  Stat checks catch replacement, metadata and size changes, while
    # hashing both sides of decode also catches same-size byte mutation.
    initial_info = _validate_regular_fd(fd)
    initial_sha256 = _sha256_fd(fd, initial_info.st_size)
    _require_same_file_state_v1(initial_info, _validate_regular_fd(fd))
    if expected_descriptor is not None and (
        initial_info.st_size != expected_descriptor.byte_size
        or initial_sha256 != expected_descriptor.file_sha256
    ):
        raise ParquetValidationError(
            "Parquet bytes differ from the expected descriptor before decode"
        )
    _inject_fault(fault_injector, FAULT_AFTER_READ_INITIAL_HASH)
    (
        table,
        row_count,
        bucket,
        logical_hash,
        min_sort_key,
        max_sort_key,
    ) = _read_arrow_table_from_fd(
        fd,
        pa=pa,
        pq=pq,
        expected_schema_name=schema_name,
        maximum_decoded_bytes=maximum_decoded_bytes,
    )
    _inject_fault(fault_injector, FAULT_AFTER_READ_DECODE)
    _require_same_file_state_v1(initial_info, _validate_regular_fd(fd))
    rows: tuple[CanonicalArchiveRow | FactorHistoryRow, ...]
    if schema_name == CANONICAL_PARQUET_SCHEMA_NAME_V1:
        rows = _canonical_rows_from_table(
            table,
            row_count=row_count,
            bucket=bucket,
            logical_hash=logical_hash,
            min_sort_key=min_sort_key,
            max_sort_key=max_sort_key,
        )
    else:
        rows = _factor_rows_from_table(
            table,
            row_count=row_count,
            bucket=bucket,
            logical_hash=logical_hash,
            min_sort_key=min_sort_key,
            max_sort_key=max_sort_key,
        )
    final_sha256 = _sha256_fd(fd, initial_info.st_size)
    _require_same_file_state_v1(initial_info, _validate_regular_fd(fd))
    if final_sha256 != initial_sha256:
        raise ParquetValidationError("Parquet artifact bytes changed during read transaction")
    _inject_fault(fault_injector, FAULT_AFTER_READ_FINAL_HASH)
    _require_same_file_state_v1(initial_info, _validate_regular_fd(fd))
    descriptor = _descriptor_from_stable_read(
        info=initial_info,
        file_sha256=final_sha256,
        basename=basename,
        schema_name=schema_name,
        row_count=row_count,
        bucket=bucket,
        logical_hash=logical_hash,
        min_sort_key=min_sort_key,
        max_sort_key=max_sort_key,
    )
    return ParquetReadResult(rows=rows, descriptor=descriptor)


def _write_atomic_part(
    *,
    directory: str | os.PathLike[str],
    basename: str,
    table: Any,
    schema_name: str,
    row_group_size: int,
    pa: Any,
    pq: Any,
    fault_injector: Callable[[str], None] | None,
) -> ParquetReadResult:
    validate_parquet_basename_v1(basename)
    row_group_size = _validate_row_group_size(row_group_size)
    if fault_injector is not None and not callable(fault_injector):
        raise HistoryValidationError("fault_injector must be callable or None")
    dir_fd = _directory_fd(directory)
    temporary = f".{basename}.tmp-{secrets.token_hex(12)}"
    temp_fd = -1
    published = False
    try:
        flags = os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC
        flags |= getattr(os, "O_NOFOLLOW", 0)
        temp_fd = os.open(temporary, flags, 0o600, dir_fd=dir_fd)
        os.fchmod(temp_fd, 0o600)
        try:
            with os.fdopen(os.dup(temp_fd), "wb") as sink:
                pq.write_table(
                    table,
                    sink,
                    compression="zstd",
                    version="2.6",
                    data_page_version="2.0",
                    row_group_size=row_group_size,
                    write_statistics=True,
                    store_schema=True,
                )
                sink.flush()
        except Exception as error:
            raise ParquetBackendError("PyArrow failed to write the Parquet part") from error
        _inject_fault(fault_injector, FAULT_AFTER_TEMP_WRITE)
        os.fsync(temp_fd)
        _inject_fault(fault_injector, FAULT_AFTER_TEMP_FSYNC)

        # Validate the complete footer, schema, row bytes, sorting and logical
        # hash before the final name can become visible.
        _read_validated_fd(
            temp_fd,
            basename=basename,
            schema_name=schema_name,
            pa=pa,
            pq=pq,
        )
        _inject_fault(fault_injector, FAULT_AFTER_TEMP_VALIDATION)
        _rename_noreplace(dir_fd, temporary, basename)
        published = True
        _inject_fault(fault_injector, FAULT_AFTER_RENAME)
        os.fsync(dir_fd)
        _inject_fault(fault_injector, FAULT_AFTER_DIRECTORY_FSYNC)

        # O_NONBLOCK prevents a replaced/non-regular path such as a FIFO from
        # blocking before the mandatory fstat regular-file gate can run.
        final_flags = (
            os.O_RDONLY
            | os.O_CLOEXEC
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_NONBLOCK", 0)
        )
        final_fd = os.open(basename, final_flags, dir_fd=dir_fd)
        try:
            # A second path-based readback binds the returned external
            # descriptor to the actual published inode/name.
            return _read_validated_fd(
                final_fd,
                basename=basename,
                schema_name=schema_name,
                pa=pa,
                pq=pq,
            )
        finally:
            os.close(final_fd)
    finally:
        if temp_fd >= 0:
            os.close(temp_fd)
        if not published and temp_fd >= 0:
            try:
                os.unlink(temporary, dir_fd=dir_fd)
            except FileNotFoundError:
                pass
        os.close(dir_fd)


def write_canonical_parquet_part(
    rows: Iterable[CanonicalArchiveRow],
    directory: str | os.PathLike[str],
    basename: str,
    *,
    row_group_size: int = 65_536,
    fault_injector: Callable[[str], None] | None = None,
) -> ParquetPartDescriptor:
    pa, pq = _load_pyarrow()
    ordered = _require_nonempty_homogeneous_canonical(rows)
    logical_hash = canonical_logical_rows_sha256(ordered)
    metadata = _footer_metadata_v1(
        schema_name=CANONICAL_PARQUET_SCHEMA_NAME_V1,
        row_count=len(ordered),
        bucket=ordered[0].bucket,
        logical_rows_sha256=logical_hash,
        min_sort_key_v1=canonical_sort_key_wire_v1(ordered[0]),
        max_sort_key_v1=canonical_sort_key_wire_v1(ordered[-1]),
    )
    schema = _canonical_schema(pa, metadata)
    table = _canonical_table(pa, ordered, schema)
    return _write_atomic_part(
        directory=directory,
        basename=basename,
        table=table,
        schema_name=CANONICAL_PARQUET_SCHEMA_NAME_V1,
        row_group_size=row_group_size,
        pa=pa,
        pq=pq,
        fault_injector=fault_injector,
    ).descriptor


def write_factor_parquet_part(
    rows: Iterable[FactorHistoryRow],
    directory: str | os.PathLike[str],
    basename: str,
    *,
    row_group_size: int = 65_536,
    fault_injector: Callable[[str], None] | None = None,
) -> ParquetPartDescriptor:
    pa, pq = _load_pyarrow()
    ordered = _require_nonempty_homogeneous_factor(rows)
    logical_hash = factor_logical_rows_sha256(ordered)
    metadata = _footer_metadata_v1(
        schema_name=FACTOR_PARQUET_SCHEMA_NAME_V1,
        row_count=len(ordered),
        bucket=ordered[0].bucket,
        logical_rows_sha256=logical_hash,
        min_sort_key_v1=factor_sort_key_wire_v1(ordered[0]),
        max_sort_key_v1=factor_sort_key_wire_v1(ordered[-1]),
    )
    schema = _factor_schema(pa, metadata)
    table = _factor_table(pa, ordered, schema)
    return _write_atomic_part(
        directory=directory,
        basename=basename,
        table=table,
        schema_name=FACTOR_PARQUET_SCHEMA_NAME_V1,
        row_group_size=row_group_size,
        pa=pa,
        pq=pq,
        fault_injector=fault_injector,
    ).descriptor


def _read_named_part(
    directory: str | os.PathLike[str],
    basename: str,
    *,
    schema_name: str,
    expected_descriptor: ParquetPartDescriptor | None,
    fault_injector: Callable[[str], None] | None,
    maximum_decoded_bytes: int | None,
) -> ParquetReadResult:
    pa, pq = _load_pyarrow()
    validate_parquet_basename_v1(basename)
    if maximum_decoded_bytes is None:
        maximum_decoded_bytes = MAX_PARQUET_DECODED_BYTES_V1
    if (
        type(maximum_decoded_bytes) is not int
        or maximum_decoded_bytes < 1
        or maximum_decoded_bytes > MAX_PARQUET_DECODED_BYTES_V1
    ):
        raise HistoryValidationError(
            "maximum_decoded_bytes must be a positive bounded integer"
        )
    if expected_descriptor is not None:
        if not isinstance(expected_descriptor, ParquetPartDescriptor):
            raise HistoryValidationError(
                "expected_descriptor must be a ParquetPartDescriptor"
            )
        if expected_descriptor.basename != basename:
            raise ParquetValidationError("expected descriptor basename mismatch")
        if expected_descriptor.schema_name != schema_name:
            raise ParquetValidationError("expected descriptor schema_name mismatch")
    dir_fd = _directory_fd(directory)
    try:
        # The type check necessarily happens after open.  O_NONBLOCK keeps a
        # FIFO/device substitution from hanging before that fail-closed gate.
        flags = (
            os.O_RDONLY
            | os.O_CLOEXEC
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_NONBLOCK", 0)
        )
        try:
            fd = os.open(basename, flags, dir_fd=dir_fd)
        except OSError as error:
            raise ParquetValidationError("cannot securely open published Parquet part") from error
        try:
            result = _read_validated_fd(
                fd,
                basename=basename,
                schema_name=schema_name,
                pa=pa,
                pq=pq,
                fault_injector=fault_injector,
                maximum_decoded_bytes=maximum_decoded_bytes,
                expected_descriptor=expected_descriptor,
            )
        finally:
            os.close(fd)
    finally:
        os.close(dir_fd)
    if expected_descriptor is not None and result.descriptor != expected_descriptor:
        raise ParquetValidationError("published Parquet part descriptor mismatch")
    return result


def read_canonical_parquet_part(
    directory: str | os.PathLike[str],
    basename: str,
    *,
    expected_descriptor: ParquetPartDescriptor | None = None,
    fault_injector: Callable[[str], None] | None = None,
    maximum_decoded_bytes: int | None = None,
) -> ParquetReadResult:
    """Read one Canonical part with a caller budget checked before decode.

    ``maximum_decoded_bytes`` applies to this artifact's footer-declared Arrow
    decoded size.  Independent calls do not share a cumulative decode or
    process-memory budget.
    """

    return _read_named_part(
        directory,
        basename,
        schema_name=CANONICAL_PARQUET_SCHEMA_NAME_V1,
        expected_descriptor=expected_descriptor,
        fault_injector=fault_injector,
        maximum_decoded_bytes=maximum_decoded_bytes,
    )


def read_factor_parquet_part(
    directory: str | os.PathLike[str],
    basename: str,
    *,
    expected_descriptor: ParquetPartDescriptor | None = None,
    fault_injector: Callable[[str], None] | None = None,
    maximum_decoded_bytes: int | None = None,
) -> ParquetReadResult:
    """Read one Factor part with a caller budget checked before decode.

    The bound is per artifact and covers footer-declared decoded bytes; it is
    not a cross-call PyArrow allocation budget.
    """

    return _read_named_part(
        directory,
        basename,
        schema_name=FACTOR_PARQUET_SCHEMA_NAME_V1,
        expected_descriptor=expected_descriptor,
        fault_injector=fault_injector,
        maximum_decoded_bytes=maximum_decoded_bytes,
    )


# Concise V1 aliases for callers that version at the module boundary.
write_canonical_parquet_part_v1 = write_canonical_parquet_part
write_factor_parquet_part_v1 = write_factor_parquet_part
read_canonical_parquet_part_v1 = read_canonical_parquet_part
read_factor_parquet_part_v1 = read_factor_parquet_part


__all__ = [
    "FAULT_AFTER_DIRECTORY_FSYNC",
    "FAULT_AFTER_READ_DECODE",
    "FAULT_AFTER_READ_FINAL_HASH",
    "FAULT_AFTER_READ_INITIAL_HASH",
    "FAULT_AFTER_RENAME",
    "FAULT_AFTER_TEMP_FSYNC",
    "FAULT_AFTER_TEMP_VALIDATION",
    "FAULT_AFTER_TEMP_WRITE",
    "MAX_PARQUET_DECODED_BYTES_V1",
    "MAX_PARQUET_ROW_GROUPS_V1",
    "ParquetBackendError",
    "ParquetDependencyError",
    "ParquetPublishError",
    "ParquetReadBudgetExceeded",
    "ParquetReadResult",
    "ParquetValidationError",
    "read_canonical_parquet_part",
    "read_canonical_parquet_part_v1",
    "read_factor_parquet_part",
    "read_factor_parquet_part_v1",
    "write_canonical_parquet_part",
    "write_canonical_parquet_part_v1",
    "write_factor_parquet_part",
    "write_factor_parquet_part_v1",
]
