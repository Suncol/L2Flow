"""Frozen, dependency-free Phase-8 archive row models.

This module deliberately does not import PyArrow.  It defines the logical rows
and their deterministic hashes; :mod:`parquet_backend` is the physical Apache
Parquet implementation.

Canonical payload semantics continue to belong to CanonicalSchemaV1.  The
archive model validates the frozen common header envelope and preserves the
entire record byte-for-byte.  It does not claim that a header projection is a
replacement for the family-specific C++ payload validator.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
import hashlib
import math
import re
import struct
from typing import Iterable


class HistoryValidationError(ValueError):
    """A Phase-8 logical row or descriptor violates its frozen V1 contract."""


class HistoryConflictError(HistoryValidationError):
    """One idempotency key was reused for different logical content."""


CANONICAL_MAGIC_V1 = 0x3145434D
CANONICAL_SCHEMA_VERSION_V1 = 1
CANONICAL_HEADER_BYTES_V1 = 112
CANONICAL_QUALITY_FLAGS_MASK_V1 = 0x0000000FFFFFFFFF
CANONICAL_RECORD_SIZE_BY_EVENT_TYPE_V1 = {
    1: 2048,  # snapshot
    2: 192,   # tick
    3: 192,   # quality
    4: 256,   # control
}
CANONICAL_EVENT_FAMILY_BY_TYPE_V1 = {
    1: "snapshot",
    2: "tick",
    3: "quality",
    4: "control",
}

ARCHIVE_BUCKET_COUNT_V1 = 32
ARCHIVE_BUCKET_HASH_VERSION_V1 = 1
ARCHIVE_BUCKET_HASH_ALGORITHM_V1 = "SHA256_DOMAIN_U32LE_PREFIX64LE_MOD32_V1"
ARCHIVE_BUCKET_HASH_DOMAIN_V1 = b"l2flow.archive.instrument-bucket.v1\x00"

CANONICAL_LOGICAL_ROWS_DOMAIN_V1 = (
    b"l2flow.archive.canonical.logical-rows.v1\x00"
)
CANONICAL_LOGICAL_ROW_DOMAIN_V1 = b"l2flow.archive.canonical.logical-row.v1\x00"
FACTOR_LOGICAL_ROWS_DOMAIN_V1 = (
    b"l2flow.archive.factor-history.logical-rows.v1\x00"
)
FACTOR_LOGICAL_ROW_DOMAIN_V1 = (
    b"l2flow.archive.factor-history.logical-row.v1\x00"
)
FACTOR_SEMANTIC_ROW_DOMAIN_V1 = (
    b"l2flow.archive.factor-history.semantic-row.v1\x00"
)

CANONICAL_PARQUET_SCHEMA_NAME_V1 = "l2flow.canonical.parquet.v1"
FACTOR_PARQUET_SCHEMA_NAME_V1 = "l2flow.factor-history.parquet.v1"
FACTOR_NUMERIC_DTYPE_V1 = "float64"
PARQUET_PHYSICAL_FORMAT = "APACHE_PARQUET"
PARQUET_COMPRESSION_V1 = "ZSTD"

MAX_ARCHIVE_PART_ROWS_V1 = 1_000_000
MAX_PARQUET_PART_BYTES_V1 = 2 * 1024 * 1024 * 1024
MAX_FACTOR_TEXT_BYTES_V1 = 256

_CANONICAL_HEADER_STRUCT_V1 = struct.Struct("<IHHIIIIQQQQQQqqqIIHHHBB")
if _CANONICAL_HEADER_STRUCT_V1.size != CANONICAL_HEADER_BYTES_V1:
    raise RuntimeError("CanonicalHeaderV1 Python layout is not 112 bytes")

CANONICAL_HEADER_FIELD_NAMES_V1 = (
    "magic",
    "schema_version",
    "event_type",
    "record_size",
    "source_stream_id",
    "connection_epoch",
    "trade_date",
    "quality_flags",
    "shard_event_id",
    "origin_ingress_sequence",
    "origin_wal_end_pos",
    "vendor_sequence_id",
    "exchange_sequence",
    "exchange_time_ns",
    "recv_realtime_ns",
    "recv_monotonic_ns",
    "instrument_id",
    "channel",
    "market",
    "origin_service_version",
    "origin_message_id",
    "origin_service_id",
    "sub_index",
)

_FACTOR_ID_RE = re.compile(r"[a-z][a-z0-9_]{0,127}\Z")
_FACTOR_VERSION_RE = re.compile(r"[0-9A-Za-z][0-9A-Za-z._+-]{0,63}\Z")
_PARQUET_BASENAME_RE = re.compile(
    r"part-[0-9A-Za-z][0-9A-Za-z._-]{0,119}\.parquet\Z"
)


def _strict_uint(value: object, bits: int, name: str, *, positive: bool = False) -> int:
    if type(value) is not int:
        raise HistoryValidationError(f"{name} must be an integer")
    minimum = 1 if positive else 0
    maximum = (1 << bits) - 1
    if value < minimum or value > maximum:
        qualifier = "positive " if positive else ""
        raise HistoryValidationError(f"{name} must be a {qualifier}uint{bits}")
    return value


def _strict_int64(value: object, name: str, *, positive: bool = False) -> int:
    if type(value) is not int:
        raise HistoryValidationError(f"{name} must be an integer")
    minimum = 1 if positive else -(1 << 63)
    if value < minimum or value > (1 << 63) - 1:
        qualifier = "positive " if positive else ""
        raise HistoryValidationError(f"{name} must be a {qualifier}int64")
    return value


def _strict_bytes(
    value: object,
    width: int,
    name: str,
    *,
    nonzero: bool = False,
) -> bytes:
    if not isinstance(value, (bytes, bytearray, memoryview)):
        raise HistoryValidationError(f"{name} must be bytes-like")
    result = bytes(value)
    if len(result) != width:
        raise HistoryValidationError(f"{name} must contain exactly {width} bytes")
    if nonzero and not any(result):
        raise HistoryValidationError(f"{name} must not be all zero")
    return result


def _strict_text(value: object, name: str, pattern: re.Pattern[str]) -> str:
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise HistoryValidationError(f"{name} has an invalid V1 spelling")
    if len(value.encode("ascii")) > MAX_FACTOR_TEXT_BYTES_V1:
        raise HistoryValidationError(f"{name} is too long")
    return value


@dataclass(frozen=True, slots=True)
class CanonicalHeaderProjectionV1:
    magic: int
    schema_version: int
    event_type: int
    record_size: int
    source_stream_id: int
    connection_epoch: int
    trade_date: int
    quality_flags: int
    shard_event_id: int
    origin_ingress_sequence: int
    origin_wal_end_pos: int
    vendor_sequence_id: int
    exchange_sequence: int
    exchange_time_ns: int
    recv_realtime_ns: int
    recv_monotonic_ns: int
    instrument_id: int
    channel: int
    market: int
    origin_service_version: int
    origin_message_id: int
    origin_service_id: int
    sub_index: int

    @classmethod
    def from_record_bytes(cls, record_bytes: bytes) -> "CanonicalHeaderProjectionV1":
        if len(record_bytes) < CANONICAL_HEADER_BYTES_V1:
            raise HistoryValidationError("Canonical record is shorter than its V1 header")
        return cls(*_CANONICAL_HEADER_STRUCT_V1.unpack_from(record_bytes, 0))

    def __post_init__(self) -> None:
        if self.magic != CANONICAL_MAGIC_V1:
            raise HistoryValidationError("Canonical record has invalid V1 magic")
        if self.schema_version != CANONICAL_SCHEMA_VERSION_V1:
            raise HistoryValidationError("Canonical record has unsupported schema_version")
        expected_size = CANONICAL_RECORD_SIZE_BY_EVENT_TYPE_V1.get(self.event_type)
        if expected_size is None:
            raise HistoryValidationError("Canonical record has unknown event_type")
        if self.record_size != expected_size:
            raise HistoryValidationError("Canonical record_size does not match event_type")
        if self.source_stream_id == 0:
            raise HistoryValidationError("Canonical source_stream_id must be nonzero")
        if (
            self.origin_ingress_sequence == 0
            or self.origin_wal_end_pos == 0
            or self.origin_service_id == 0
            or self.origin_message_id == 0
        ):
            raise HistoryValidationError("Canonical origin cursor fields must be nonzero")
        if self.event_type != 4 and self.origin_service_version == 0:
            raise HistoryValidationError(
                "non-control Canonical origin_service_version must be nonzero"
            )
        if self.shard_event_id == 0:
            raise HistoryValidationError("Canonical shard_event_id must be nonzero")
        if self.recv_realtime_ns < 0 or self.recv_monotonic_ns < 0:
            raise HistoryValidationError("Canonical receive timestamps must be nonnegative")
        if self.quality_flags & ~CANONICAL_QUALITY_FLAGS_MASK_V1:
            raise HistoryValidationError("Canonical quality_flags contains unknown V1 bits")
        if self.market not in (0, 1, 2):
            raise HistoryValidationError("Canonical market is unknown to V1")
        if self.trade_date == 0:
            raise HistoryValidationError("Canonical trade_date must be nonzero")
        if self.event_type in (1, 2):
            if self.market == 0 or self.instrument_id == 0:
                raise HistoryValidationError(
                    "market Canonical records require market and instrument_id"
                )
            if self.vendor_sequence_id == 0 or self.sub_index != 0:
                raise HistoryValidationError(
                    "market Canonical records have an invalid origin cursor"
                )
        elif self.event_type == 3:
            if self.vendor_sequence_id == 0 or not 1 <= self.sub_index <= 3:
                raise HistoryValidationError(
                    "quality Canonical records require sub_index in [1, 3]"
                )
        elif self.event_type == 4:
            if (
                self.market != 0
                or self.instrument_id != 0
                or self.channel != 0
                or self.exchange_sequence != 0
                or self.exchange_time_ns != 0
                or self.vendor_sequence_id != 0
                or self.sub_index != 0
            ):
                raise HistoryValidationError(
                    "control Canonical record contains forbidden market fields"
                )

    @property
    def event_family(self) -> str:
        return CANONICAL_EVENT_FAMILY_BY_TYPE_V1[self.event_type]

    def as_dict(self) -> dict[str, int]:
        return {
            name: getattr(self, name)
            for name in CANONICAL_HEADER_FIELD_NAMES_V1
        }


@dataclass(frozen=True, slots=True)
class CanonicalArchiveRow:
    """One exact Canonical V1 record plus its Raw stream-day namespace."""

    origin_capture_date: int
    origin_stream_day_id: bytes
    record_bytes: bytes
    header: CanonicalHeaderProjectionV1 = field(init=False, repr=False)
    record_sha256: bytes = field(init=False, repr=False)

    def __post_init__(self) -> None:
        _strict_uint(
            self.origin_capture_date,
            32,
            "origin_capture_date",
            positive=True,
        )
        object.__setattr__(
            self,
            "origin_stream_day_id",
            _strict_bytes(
                self.origin_stream_day_id,
                16,
                "origin_stream_day_id",
                nonzero=True,
            ),
        )
        if not isinstance(self.record_bytes, (bytes, bytearray, memoryview)):
            raise HistoryValidationError("record_bytes must be bytes-like")
        exact = bytes(self.record_bytes)
        header = CanonicalHeaderProjectionV1.from_record_bytes(exact)
        if len(exact) != header.record_size:
            raise HistoryValidationError(
                "record_bytes length does not equal Canonical record_size"
            )
        object.__setattr__(self, "record_bytes", exact)
        object.__setattr__(self, "header", header)
        object.__setattr__(self, "record_sha256", hashlib.sha256(exact).digest())

    @property
    def bucket(self) -> int:
        return instrument_bucket_v1(self.header.instrument_id)

    @property
    def sort_key(self) -> tuple[int, int, int, int, int, int]:
        """The six-column order specified by design section 14.2."""
        return (
            self.header.instrument_id,
            self.header.exchange_time_ns,
            self.header.exchange_sequence,
            self.header.source_stream_id,
            self.header.origin_ingress_sequence,
            self.header.sub_index,
        )

    @property
    def identity_key(self) -> tuple[int, bytes, int, int, int, int]:
        """The complete cross-segment idempotency key from design 11.2."""
        return (
            self.origin_capture_date,
            self.origin_stream_day_id,
            self.header.source_stream_id,
            self.header.origin_ingress_sequence,
            self.header.sub_index,
            self.header.schema_version,
        )

    @property
    def total_sort_key(self) -> tuple[object, ...]:
        # Context fields are tie-breakers only.  They make the order total when
        # a part spans reconnects/stream days without changing the 14.2 prefix.
        return self.sort_key + (
            self.origin_capture_date,
            self.origin_stream_day_id,
            self.header.schema_version,
        )

    @property
    def content_sha256(self) -> bytes:
        return hashlib.sha256(
            CANONICAL_LOGICAL_ROW_DOMAIN_V1 + _canonical_row_wire_v1(self)
        ).digest()


class FactorImplementationStatus(str, Enum):
    IMPLEMENTED = "IMPLEMENTED"
    PASSTHROUGH_PLACEHOLDER = "PASSTHROUGH_PLACEHOLDER"


@dataclass(frozen=True, slots=True)
class FactorHistoryRow:
    factor_id: str
    factor_version: str
    factor_config_sha256: bytes
    factor_code_sha256: bytes
    state_schema_version: int
    state_schema_sha256: bytes
    trade_date: int
    registry_version: int
    registry_sha256: bytes
    instrument_id: int
    asof_ns: int
    value_bits: int
    value_valid: bool
    run_id: bytes
    watermark_table_generation: int
    watermark_set_id: int
    input_identity_sha256: bytes
    clock_epoch_algorithm: int
    clock_epoch_digest: bytes
    clock_epoch_label: int
    input_quality_flags: int
    implementation_status: FactorImplementationStatus
    calculation_latency_ns: int = 0
    numeric_dtype: str = FACTOR_NUMERIC_DTYPE_V1

    def __post_init__(self) -> None:
        _strict_text(self.factor_id, "factor_id", _FACTOR_ID_RE)
        _strict_text(self.factor_version, "factor_version", _FACTOR_VERSION_RE)
        object.__setattr__(
            self,
            "factor_config_sha256",
            _strict_bytes(
                self.factor_config_sha256,
                32,
                "factor_config_sha256",
                nonzero=True,
            ),
        )
        object.__setattr__(
            self,
            "factor_code_sha256",
            _strict_bytes(
                self.factor_code_sha256,
                32,
                "factor_code_sha256",
                nonzero=True,
            ),
        )
        _strict_uint(
            self.state_schema_version,
            32,
            "state_schema_version",
            positive=True,
        )
        object.__setattr__(
            self,
            "state_schema_sha256",
            _strict_bytes(
                self.state_schema_sha256,
                32,
                "state_schema_sha256",
                nonzero=True,
            ),
        )
        _strict_uint(self.trade_date, 32, "trade_date", positive=True)
        _strict_uint(self.registry_version, 64, "registry_version", positive=True)
        object.__setattr__(
            self,
            "registry_sha256",
            _strict_bytes(
                self.registry_sha256,
                32,
                "registry_sha256",
                nonzero=True,
            ),
        )
        _strict_uint(self.instrument_id, 32, "instrument_id", positive=True)
        _strict_int64(self.asof_ns, "asof_ns", positive=True)
        _strict_uint(self.value_bits, 64, "value_bits")
        if type(self.value_valid) is not bool:
            raise HistoryValidationError("value_valid must be bool")
        object.__setattr__(
            self,
            "run_id",
            _strict_bytes(self.run_id, 16, "run_id", nonzero=True),
        )
        _strict_uint(
            self.watermark_table_generation,
            64,
            "watermark_table_generation",
            positive=True,
        )
        _strict_uint(
            self.watermark_set_id,
            64,
            "watermark_set_id",
            positive=True,
        )
        object.__setattr__(
            self,
            "input_identity_sha256",
            _strict_bytes(
                self.input_identity_sha256,
                32,
                "input_identity_sha256",
                nonzero=True,
            ),
        )
        _strict_uint(
            self.clock_epoch_algorithm,
            32,
            "clock_epoch_algorithm",
            positive=True,
        )
        object.__setattr__(
            self,
            "clock_epoch_digest",
            _strict_bytes(
                self.clock_epoch_digest,
                32,
                "clock_epoch_digest",
                nonzero=True,
            ),
        )
        _strict_uint(self.clock_epoch_label, 64, "clock_epoch_label")
        _strict_uint(self.input_quality_flags, 64, "input_quality_flags")
        if self.input_quality_flags & ~CANONICAL_QUALITY_FLAGS_MASK_V1:
            raise HistoryValidationError(
                "input_quality_flags contains unknown Canonical V1 bits"
            )
        if not isinstance(self.implementation_status, FactorImplementationStatus):
            raise HistoryValidationError(
                "implementation_status must be a FactorImplementationStatus"
            )
        _strict_uint(self.calculation_latency_ns, 64, "calculation_latency_ns")
        if self.numeric_dtype != FACTOR_NUMERIC_DTYPE_V1:
            raise HistoryValidationError("FactorHistoryRow V1 numeric_dtype is float64")

        if not self.value_valid and self.value_bits != 0:
            raise HistoryValidationError("an invalid factor value must use zero value_bits")
        if self.value_valid:
            numeric = struct.unpack("<d", struct.pack("<Q", self.value_bits))[0]
            if not math.isfinite(numeric):
                raise HistoryValidationError("a valid factor value must be finite")
        if (
            self.implementation_status
            is FactorImplementationStatus.PASSTHROUGH_PLACEHOLDER
            and self.value_valid
        ):
            raise HistoryValidationError(
                "a passthrough placeholder cannot claim a valid mathematical value"
            )

    @classmethod
    def from_float(
        cls,
        *,
        value: float | None,
        **kwargs: object,
    ) -> "FactorHistoryRow":
        if value is None:
            return cls(value_bits=0, value_valid=False, **kwargs)
        if type(value) is not float or not math.isfinite(value):
            raise HistoryValidationError("value must be a finite float or None")
        bits = struct.unpack("<Q", struct.pack("<d", value))[0]
        return cls(value_bits=bits, value_valid=True, **kwargs)

    @property
    def value(self) -> float | None:
        if not self.value_valid:
            return None
        return struct.unpack("<d", struct.pack("<Q", self.value_bits))[0]

    @property
    def bucket(self) -> int:
        return instrument_bucket_v1(self.instrument_id)

    @property
    def sort_key(self) -> tuple[int, int, bytes]:
        return (self.instrument_id, self.asof_ns, self.input_identity_sha256)

    @property
    def identity_key(self) -> tuple[str, str, int, int, bytes]:
        return (
            self.factor_id,
            self.factor_version,
            self.instrument_id,
            self.asof_ns,
            self.input_identity_sha256,
        )

    @property
    def total_sort_key(self) -> tuple[object, ...]:
        return self.sort_key + (self.factor_id, self.factor_version)

    @property
    def reference_tie_break_key(self) -> tuple[object, ...]:
        """Deterministically selects among semantically identical replays.

        These fields locate/describe one physical execution.  They are not
        part of factor semantic equivalence.
        """
        return (
            self.watermark_table_generation,
            self.run_id,
            self.watermark_set_id,
            self.clock_epoch_label,
            self.calculation_latency_ns,
        )

    @property
    def content_sha256(self) -> bytes:
        return hashlib.sha256(
            FACTOR_LOGICAL_ROW_DOMAIN_V1 + _factor_row_wire_v1(self)
        ).digest()

    @property
    def semantic_content_sha256(self) -> bytes:
        return hashlib.sha256(
            FACTOR_SEMANTIC_ROW_DOMAIN_V1 + _factor_semantic_wire_v1(self)
        ).digest()


@dataclass(frozen=True, slots=True)
class ParquetPartDescriptor:
    """External descriptor; ``file_sha256`` is never embedded in its file."""

    basename: str
    schema_name: str
    row_count: int
    byte_size: int
    bucket: int
    logical_rows_sha256: bytes
    file_sha256: bytes
    min_sort_key_v1: bytes
    max_sort_key_v1: bytes
    physical_format: str = PARQUET_PHYSICAL_FORMAT
    compression: str = PARQUET_COMPRESSION_V1
    bucket_count: int = ARCHIVE_BUCKET_COUNT_V1
    bucket_hash_version: int = ARCHIVE_BUCKET_HASH_VERSION_V1

    def __post_init__(self) -> None:
        validate_parquet_basename_v1(self.basename)
        if self.schema_name not in (
            CANONICAL_PARQUET_SCHEMA_NAME_V1,
            FACTOR_PARQUET_SCHEMA_NAME_V1,
        ):
            raise HistoryValidationError("unknown Phase-8 Parquet schema_name")
        _strict_uint(self.row_count, 64, "row_count", positive=True)
        if self.row_count > MAX_ARCHIVE_PART_ROWS_V1:
            raise HistoryValidationError("row_count exceeds the V1 part bound")
        _strict_uint(self.byte_size, 64, "byte_size", positive=True)
        if self.byte_size < 12:
            raise HistoryValidationError("byte_size is too small for Apache Parquet")
        if self.byte_size > MAX_PARQUET_PART_BYTES_V1:
            raise HistoryValidationError("byte_size exceeds the V1 Parquet part bound")
        _strict_uint(self.bucket, 8, "bucket")
        if self.bucket >= ARCHIVE_BUCKET_COUNT_V1:
            raise HistoryValidationError("bucket is outside the frozen 32 buckets")
        object.__setattr__(
            self,
            "logical_rows_sha256",
            _strict_bytes(self.logical_rows_sha256, 32, "logical_rows_sha256"),
        )
        object.__setattr__(
            self,
            "file_sha256",
            _strict_bytes(self.file_sha256, 32, "file_sha256"),
        )
        for name in ("min_sort_key_v1", "max_sort_key_v1"):
            value = getattr(self, name)
            if not isinstance(value, (bytes, bytearray, memoryview)):
                raise HistoryValidationError(f"{name} must be bytes-like")
            exact = bytes(value)
            if not exact or len(exact) > 1024:
                raise HistoryValidationError(f"{name} is outside V1 bounds")
            object.__setattr__(self, name, exact)
        if self.physical_format != PARQUET_PHYSICAL_FORMAT:
            raise HistoryValidationError("physical_format must be APACHE_PARQUET")
        if self.compression != PARQUET_COMPRESSION_V1:
            raise HistoryValidationError("compression must be ZSTD")
        if self.bucket_count != ARCHIVE_BUCKET_COUNT_V1:
            raise HistoryValidationError("bucket_count must be the frozen value 32")
        if self.bucket_hash_version != ARCHIVE_BUCKET_HASH_VERSION_V1:
            raise HistoryValidationError("unsupported bucket_hash_version")


def validate_parquet_basename_v1(value: object) -> str:
    if not isinstance(value, str) or _PARQUET_BASENAME_RE.fullmatch(value) is None:
        raise HistoryValidationError(
            "Parquet basename must match part-<safe-token>.parquet"
        )
    if "/" in value or "\\" in value or "\x00" in value or value in (".", ".."):
        raise HistoryValidationError("Parquet path must be a basename")
    return value


def instrument_bucket_v1(instrument_id: int) -> int:
    """Frozen SHA-256 bucket: first digest u64-LE modulo exactly 32."""
    _strict_uint(instrument_id, 32, "instrument_id")
    digest = hashlib.sha256(
        ARCHIVE_BUCKET_HASH_DOMAIN_V1 + struct.pack("<I", instrument_id)
    ).digest()
    return int.from_bytes(digest[:8], "little") % ARCHIVE_BUCKET_COUNT_V1


canonical_bucket_v1 = instrument_bucket_v1


def _materialize_rows(rows: Iterable[object], expected_type: type) -> list[object]:
    try:
        iterator = iter(rows)
    except TypeError as error:
        raise HistoryValidationError("rows must be iterable") from error
    result: list[object] = []
    for row in iterator:
        if len(result) >= MAX_ARCHIVE_PART_ROWS_V1:
            raise HistoryValidationError("rows exceeds the V1 part bound")
        if type(row) is not expected_type:
            raise HistoryValidationError(
                f"rows must contain only {expected_type.__name__}"
            )
        result.append(row)
    return result


def canonical_sort_and_dedupe(
    rows: Iterable[CanonicalArchiveRow],
) -> tuple[CanonicalArchiveRow, ...]:
    materialized = _materialize_rows(rows, CanonicalArchiveRow)
    by_identity: dict[tuple[int, bytes, int, int, int, int], CanonicalArchiveRow] = {}
    for generic_row in materialized:
        row = generic_row
        assert isinstance(row, CanonicalArchiveRow)
        previous = by_identity.get(row.identity_key)
        if previous is None:
            by_identity[row.identity_key] = row
        elif previous != row:
            raise HistoryConflictError(
                "Canonical idempotency key has conflicting exact record content"
            )
    return tuple(sorted(by_identity.values(), key=lambda row: row.total_sort_key))


def factor_sort_and_dedupe(
    rows: Iterable[FactorHistoryRow],
) -> tuple[FactorHistoryRow, ...]:
    materialized = _materialize_rows(rows, FactorHistoryRow)
    by_identity: dict[tuple[str, str, int, int, bytes], FactorHistoryRow] = {}
    for generic_row in materialized:
        row = generic_row
        assert isinstance(row, FactorHistoryRow)
        previous = by_identity.get(row.identity_key)
        if previous is None:
            by_identity[row.identity_key] = row
        elif _factor_semantic_wire_v1(previous) != _factor_semantic_wire_v1(row):
            raise HistoryConflictError(
                "factor history idempotency key has conflicting content"
            )
        elif row.reference_tie_break_key < previous.reference_tie_break_key:
            # Run-local/table-local references, display-only clock label, and
            # calculation latency may legitimately differ on replay.  Select
            # one physical reference without making caller order observable.
            by_identity[row.identity_key] = row
    return tuple(sorted(by_identity.values(), key=lambda row: row.total_sort_key))


def _canonical_row_wire_v1(row: CanonicalArchiveRow) -> bytes:
    return b"".join(
        (
            struct.pack("<I", row.origin_capture_date),
            row.origin_stream_day_id,
            struct.pack("<I", len(row.record_bytes)),
            row.record_bytes,
        )
    )


def canonical_sort_key_wire_v1(row: CanonicalArchiveRow) -> bytes:
    if type(row) is not CanonicalArchiveRow:
        raise HistoryValidationError("row must be a CanonicalArchiveRow")
    return b"".join(
        (
            struct.pack("<I", row.header.instrument_id),
            struct.pack("<q", row.header.exchange_time_ns),
            struct.pack("<Q", row.header.exchange_sequence),
            struct.pack("<I", row.header.source_stream_id),
            struct.pack("<Q", row.header.origin_ingress_sequence),
            struct.pack("<B", row.header.sub_index),
            struct.pack("<I", row.origin_capture_date),
            row.origin_stream_day_id,
            struct.pack("<H", row.header.schema_version),
        )
    )


def _text_wire_v1(value: str) -> bytes:
    encoded = value.encode("ascii")
    if len(encoded) > 0xFFFF:
        raise HistoryValidationError("text cannot fit the V1 logical row wire")
    return struct.pack("<H", len(encoded)) + encoded


def _factor_semantic_wire_v1(row: FactorHistoryRow) -> bytes:
    return b"".join(
        (
            _text_wire_v1(row.factor_id),
            _text_wire_v1(row.factor_version),
            row.factor_config_sha256,
            row.factor_code_sha256,
            struct.pack("<I", row.state_schema_version),
            row.state_schema_sha256,
            struct.pack("<I", row.trade_date),
            struct.pack("<Q", row.registry_version),
            row.registry_sha256,
            struct.pack("<I", row.instrument_id),
            struct.pack("<q", row.asof_ns),
            struct.pack("<Q", row.value_bits),
            struct.pack("<B", 1 if row.value_valid else 0),
            row.input_identity_sha256,
            struct.pack("<I", row.clock_epoch_algorithm),
            row.clock_epoch_digest,
            struct.pack("<Q", row.input_quality_flags),
            _text_wire_v1(row.implementation_status.value),
            _text_wire_v1(row.numeric_dtype),
        )
    )


def _factor_row_wire_v1(row: FactorHistoryRow) -> bytes:
    semantic = _factor_semantic_wire_v1(row)
    return b"".join(
        (
            struct.pack("<I", len(semantic)),
            semantic,
            row.run_id,
            struct.pack("<Q", row.watermark_table_generation),
            struct.pack("<Q", row.watermark_set_id),
            struct.pack("<Q", row.clock_epoch_label),
            struct.pack("<Q", row.calculation_latency_ns),
        )
    )


def factor_sort_key_wire_v1(row: FactorHistoryRow) -> bytes:
    if type(row) is not FactorHistoryRow:
        raise HistoryValidationError("row must be a FactorHistoryRow")
    return b"".join(
        (
            struct.pack("<I", row.instrument_id),
            struct.pack("<q", row.asof_ns),
            row.input_identity_sha256,
            _text_wire_v1(row.factor_id),
            _text_wire_v1(row.factor_version),
        )
    )


def canonical_logical_rows_sha256(
    rows: Iterable[CanonicalArchiveRow],
) -> bytes:
    ordered = canonical_sort_and_dedupe(rows)
    hasher = hashlib.sha256()
    hasher.update(CANONICAL_LOGICAL_ROWS_DOMAIN_V1)
    hasher.update(struct.pack("<Q", len(ordered)))
    for row in ordered:
        wire = _canonical_row_wire_v1(row)
        hasher.update(struct.pack("<I", len(wire)))
        hasher.update(wire)
    return hasher.digest()


def factor_logical_rows_sha256(rows: Iterable[FactorHistoryRow]) -> bytes:
    ordered = factor_sort_and_dedupe(rows)
    hasher = hashlib.sha256()
    hasher.update(FACTOR_LOGICAL_ROWS_DOMAIN_V1)
    hasher.update(struct.pack("<Q", len(ordered)))
    for row in ordered:
        wire = _factor_row_wire_v1(row)
        hasher.update(struct.pack("<I", len(wire)))
        hasher.update(wire)
    return hasher.digest()


__all__ = [
    "ARCHIVE_BUCKET_COUNT_V1",
    "ARCHIVE_BUCKET_HASH_ALGORITHM_V1",
    "ARCHIVE_BUCKET_HASH_VERSION_V1",
    "CANONICAL_EVENT_FAMILY_BY_TYPE_V1",
    "CANONICAL_HEADER_FIELD_NAMES_V1",
    "CANONICAL_PARQUET_SCHEMA_NAME_V1",
    "CANONICAL_RECORD_SIZE_BY_EVENT_TYPE_V1",
    "FACTOR_PARQUET_SCHEMA_NAME_V1",
    "FACTOR_NUMERIC_DTYPE_V1",
    "MAX_ARCHIVE_PART_ROWS_V1",
    "MAX_PARQUET_PART_BYTES_V1",
    "CanonicalArchiveRow",
    "CanonicalHeaderProjectionV1",
    "FactorHistoryRow",
    "FactorImplementationStatus",
    "HistoryConflictError",
    "HistoryValidationError",
    "ParquetPartDescriptor",
    "canonical_bucket_v1",
    "canonical_logical_rows_sha256",
    "canonical_sort_and_dedupe",
    "canonical_sort_key_wire_v1",
    "factor_logical_rows_sha256",
    "factor_sort_and_dedupe",
    "factor_sort_key_wire_v1",
    "instrument_bucket_v1",
    "validate_parquet_basename_v1",
]
