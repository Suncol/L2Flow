from __future__ import annotations

import ctypes
import errno
import fcntl
import hashlib
import os
import re
import secrets
import stat
import threading
from dataclasses import dataclass
from enum import Enum
from pathlib import PurePosixPath
from typing import Any, Callable, Mapping

from l2flow_factor import InputFamily

from .model import (
    CANONICAL_PARQUET_SCHEMA_NAME_V1,
    FACTOR_PARQUET_SCHEMA_NAME_V1,
    HistoryValidationError,
    ParquetPartDescriptor,
    validate_parquet_basename_v1,
)
from .watermark_sidecar import (
    SidecarError,
    SidecarNamespace,
    SidecarReference,
    WatermarkSidecar,
    _MAX_SIDECAR_JSON_BYTES,
    _canonical_json,
    _decode_envelope,
    _decode_json,
    _encode_envelope,
    _fixed_bytes as _sidecar_fixed_bytes,
    _require_object,
    _uint as _sidecar_uint,
    decode_sidecar,
    encode_sidecar,
)


_MANIFEST_MAGIC = b"L2MNFJ1\x00"
_CURRENT_MAGIC = b"L2CURJ1\x00"
_MAX_MANIFEST_JSON_BYTES = 64 * 1024 * 1024
_MAX_CURRENT_JSON_BYTES = 16 * 1024
_MAX_ARTIFACTS = 65_536
_MAX_RECEIPTS_PER_ARTIFACT = 65_536
_MAX_SIDECAR_REFS_PER_ARTIFACT = 65_536
_MAX_SIDECAR_DESCRIPTORS = 65_536
_MAX_WATERMARK_SETS_PER_SIDECAR = 65_536
_CURRENT_NAME = "CURRENT"
_RENAME_NOREPLACE = 1


class ManifestError(SidecarError):
    """A history manifest failed its strict V1 contract."""


class ManifestConflictError(ManifestError):
    """A manifest generation, path, or CAS predecessor conflicted."""


class ManifestStoreError(ManifestError):
    """The Linux manifest store could not prove an atomic publication."""


class InjectedManifestCrash(BaseException):
    """Fault-injection signal that intentionally leaves crash artifacts behind."""


def _freeze_sidecar_mapping(
    value: object,
) -> dict[SidecarNamespace, WatermarkSidecar]:
    """Bound and snapshot a possibly custom Mapping before validation/I/O."""

    if not isinstance(value, Mapping):
        raise ManifestError("sidecars must be a mapping")
    try:
        iterator = iter(value.items())
    except (TypeError, RuntimeError) as error:
        raise ManifestError("sidecar mapping cannot be iterated") from error
    frozen: dict[SidecarNamespace, WatermarkSidecar] = {}
    try:
        for item in iterator:
            if len(frozen) >= _MAX_SIDECAR_DESCRIPTORS:
                raise ManifestError("sidecar mapping exceeds the V1 bound")
            if type(item) is not tuple or len(item) != 2:
                raise ManifestError("sidecar mapping yielded a malformed item")
            namespace, sidecar = item
            if not isinstance(namespace, SidecarNamespace) or not isinstance(
                sidecar,
                WatermarkSidecar,
            ):
                raise ManifestError("sidecar mapping contains an invalid value")
            if sidecar.namespace != namespace:
                raise ManifestConflictError(
                    "sidecar mapping key/namespace mismatch"
                )
            if namespace in frozen:
                raise ManifestConflictError("sidecar mapping repeats a namespace")
            frozen[namespace] = sidecar
    except (TypeError, RuntimeError) as error:
        raise ManifestError("sidecar mapping changed while being frozen") from error
    return frozen


def _uint(value: Any, bits: int, name: str, *, positive: bool = False) -> int:
    try:
        return _sidecar_uint(value, bits, name, positive=positive)
    except SidecarError as error:
        raise ManifestError(str(error)) from error


def _fixed_bytes(
    value: Any,
    width: int,
    name: str,
    *,
    nonzero: bool = True,
) -> bytes:
    try:
        return _sidecar_fixed_bytes(
            value,
            width,
            name,
            nonzero=nonzero,
        )
    except SidecarError as error:
        raise ManifestError(str(error)) from error


class DatasetKind(Enum):
    CANONICAL = "canonical"
    FACTOR = "factor"


class ArtifactVisibility(Enum):
    STAGING = "staging"
    VISIBLE = "visible"


class CoverageKind(Enum):
    STAGING = "staging"
    SEALED_COMMON_CUT = "sealed_common_cut"


def _safe_token(value: Any, name: str, *, maximum: int = 128) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > maximum
        or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._+-]*", value) is None
    ):
        raise ManifestError(f"{name} is not a bounded safe token")
    return value


def _safe_relative_path(value: Any) -> str:
    if not isinstance(value, str) or not value or len(value) > 4096:
        raise ManifestError("artifact path must be a non-empty bounded string")
    if "\\" in value or "\x00" in value or value.startswith("/") or "//" in value:
        raise ManifestError("artifact path is not a safe relative POSIX path")
    path = PurePosixPath(value)
    parts = path.parts
    if not parts or any(
        part in ("", ".", "..")
        or len(part.encode("utf-8")) > 255
        or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._=+-]*", part) is None
        for part in parts
    ):
        raise ManifestError("artifact path contains an unsafe component")
    normalized = str(path)
    if normalized != value:
        raise ManifestError("artifact path is not in canonical relative form")
    return normalized


def _bounded_bytes(value: Any, name: str, *, maximum: int = 1024) -> bytes:
    if isinstance(value, str):
        if (
            not value
            or len(value) % 2 != 0
            or value.lower() != value
            or re.fullmatch(r"[0-9a-f]+", value) is None
        ):
            raise ManifestError(f"{name} must be canonical lowercase hex")
        try:
            result = bytes.fromhex(value)
        except ValueError as error:
            raise ManifestError(f"{name} must be canonical lowercase hex") from error
    elif isinstance(value, (bytes, bytearray, memoryview)):
        result = bytes(value)
    else:
        raise ManifestError(f"{name} must be bytes or canonical lowercase hex")
    if not result or len(result) > maximum:
        raise ManifestError(f"{name} is outside its byte bound")
    return result


@dataclass(frozen=True, slots=True)
class PartitionSpec:
    dataset: DatasetKind
    trade_date: int
    bucket: int
    market: str | None = None
    event_type: str | None = None
    factor_group: str | None = None
    factor_version: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.dataset, DatasetKind):
            raise ManifestError("partition dataset must be a DatasetKind")
        _uint(self.trade_date, 32, "trade_date", positive=True)
        if type(self.bucket) is not int or not 0 <= self.bucket < 32:
            raise ManifestError("Parquet bucket must be an exact integer in [0, 31]")
        if self.dataset is DatasetKind.CANONICAL:
            if self.event_type not in ("tick", "snapshot", "quality", "control"):
                raise ManifestError("Canonical partition event_type is invalid")
            if self.event_type in ("tick", "snapshot") and self.market not in (
                "SH",
                "SZ",
            ):
                raise ManifestError(
                    "tick/snapshot partitions require market SH or SZ"
                )
            if self.event_type == "control" and self.market != "GLOBAL":
                raise ManifestError("control partitions require market GLOBAL")
            if self.event_type == "quality" and self.market not in (
                "SH",
                "SZ",
                "GLOBAL",
            ):
                raise ManifestError(
                    "quality partitions require market SH, SZ, or GLOBAL"
                )
            if self.factor_group is not None or self.factor_version is not None:
                raise ManifestError("Canonical partition cannot carry factor fields")
        else:
            if self.market is not None or self.event_type is not None:
                raise ManifestError("Factor partition cannot carry market/event_type")
            _safe_token(self.factor_group, "factor_group")
            _safe_token(self.factor_version, "factor_version", maximum=64)

    @property
    def relative_prefix(self) -> str:
        if self.dataset is DatasetKind.CANONICAL:
            return (
                f"canonical/trade_date={self.trade_date:08d}/"
                f"market={self.market}/event_type={self.event_type}/"
                f"bucket={self.bucket:02d}/"
            )
        return (
            f"factors/trade_date={self.trade_date:08d}/"
            f"factor_group={self.factor_group}/"
            f"factor_version={self.factor_version}/"
            f"bucket={self.bucket:02d}/"
        )

    def canonical_object(self) -> dict[str, Any]:
        return {
            "bucket": self.bucket,
            "dataset": self.dataset.value,
            "event_type": self.event_type,
            "factor_group": self.factor_group,
            "factor_version": self.factor_version,
            "market": self.market,
            "trade_date": self.trade_date,
        }


@dataclass(frozen=True, slots=True)
class SourceNamespace:
    trade_date: int
    source_stream_id: int
    origin_capture_date: int
    origin_stream_day_id: bytes
    family: InputFamily
    shard_id: int
    origin_source_writer_instance: bytes
    origin_source_generation: int
    canonical_generation: int
    clock_epoch_algorithm: int
    clock_epoch_digest: bytes
    schema_sha256: bytes
    dtype_sha256: bytes
    registry_version: int
    registry_sha256: bytes
    normalizer_build_sha256: bytes
    normalizer_config_sha256: bytes

    def __post_init__(self) -> None:
        _uint(self.trade_date, 32, "trade_date", positive=True)
        _uint(self.source_stream_id, 32, "source_stream_id", positive=True)
        _uint(self.origin_capture_date, 32, "origin_capture_date", positive=True)
        object.__setattr__(
            self,
            "origin_stream_day_id",
            _fixed_bytes(self.origin_stream_day_id, 16, "origin_stream_day_id"),
        )
        if not isinstance(self.family, InputFamily):
            raise ManifestError("family must be an InputFamily")
        self.family.canonical_event_type
        _uint(self.shard_id, 32, "shard_id")
        object.__setattr__(
            self,
            "origin_source_writer_instance",
            _fixed_bytes(
                self.origin_source_writer_instance,
                16,
                "origin_source_writer_instance",
            ),
        )
        for name, value in (
            ("origin_source_generation", self.origin_source_generation),
            ("canonical_generation", self.canonical_generation),
            ("registry_version", self.registry_version),
        ):
            _uint(value, 64, name, positive=True)
        _uint(
            self.clock_epoch_algorithm,
            32,
            "clock_epoch_algorithm",
            positive=True,
        )
        for name in (
            "clock_epoch_digest",
            "schema_sha256",
            "dtype_sha256",
            "registry_sha256",
            "normalizer_build_sha256",
            "normalizer_config_sha256",
        ):
            object.__setattr__(
                self,
                name,
                _fixed_bytes(getattr(self, name), 32, name),
            )

    @property
    def sort_key(self) -> tuple[Any, ...]:
        return (
            self.source_stream_id,
            self.origin_capture_date,
            self.origin_stream_day_id,
            self.family.canonical_event_type,
            self.shard_id,
            self.origin_source_writer_instance,
            self.origin_source_generation,
            self.canonical_generation,
            self.clock_epoch_algorithm,
            self.clock_epoch_digest,
            self.schema_sha256,
            self.dtype_sha256,
            self.registry_version,
            self.registry_sha256,
            self.normalizer_build_sha256,
            self.normalizer_config_sha256,
            self.trade_date,
        )

    def canonical_object(self) -> dict[str, Any]:
        return {
            "canonical_generation": self.canonical_generation,
            "clock_epoch_algorithm": self.clock_epoch_algorithm,
            "clock_epoch_digest": self.clock_epoch_digest.hex(),
            "dtype_sha256": self.dtype_sha256.hex(),
            "family": self.family.value,
            "normalizer_build_sha256": self.normalizer_build_sha256.hex(),
            "normalizer_config_sha256": self.normalizer_config_sha256.hex(),
            "origin_capture_date": self.origin_capture_date,
            "origin_source_generation": self.origin_source_generation,
            "origin_source_writer_instance": (
                self.origin_source_writer_instance.hex()
            ),
            "origin_stream_day_id": self.origin_stream_day_id.hex(),
            "registry_sha256": self.registry_sha256.hex(),
            "registry_version": self.registry_version,
            "schema_sha256": self.schema_sha256.hex(),
            "shard_id": self.shard_id,
            "source_stream_id": self.source_stream_id,
            "trade_date": self.trade_date,
        }


@dataclass(frozen=True, slots=True)
class SourceRangeReceipt:
    namespace: SourceNamespace
    begin_canonical_cursor: int
    end_canonical_cursor: int
    first_origin_ingress_sequence: int
    last_origin_ingress_sequence: int
    min_origin_wal_end_pos: int
    max_origin_wal_end_pos: int
    coverage_kind: CoverageKind
    coverage_certificate_sha256: bytes | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.namespace, SourceNamespace):
            raise ManifestError("receipt namespace must be a SourceNamespace")
        begin = _uint(
            self.begin_canonical_cursor, 64, "begin_canonical_cursor"
        )
        end = _uint(self.end_canonical_cursor, 64, "end_canonical_cursor")
        if end <= begin:
            raise ManifestError("Canonical cursor range must be non-empty")
        first_ingress = _uint(
            self.first_origin_ingress_sequence,
            64,
            "first_origin_ingress_sequence",
            positive=True,
        )
        last_ingress = _uint(
            self.last_origin_ingress_sequence,
            64,
            "last_origin_ingress_sequence",
            positive=True,
        )
        if last_ingress < first_ingress:
            raise ManifestError("origin ingress range is reversed")
        minimum_wal = _uint(
            self.min_origin_wal_end_pos,
            64,
            "min_origin_wal_end_pos",
            positive=True,
        )
        maximum_wal = _uint(
            self.max_origin_wal_end_pos,
            64,
            "max_origin_wal_end_pos",
            positive=True,
        )
        if maximum_wal < minimum_wal:
            raise ManifestError("origin WAL range is reversed")
        if not isinstance(self.coverage_kind, CoverageKind):
            raise ManifestError("coverage_kind must be a CoverageKind")
        if self.coverage_kind is CoverageKind.SEALED_COMMON_CUT:
            object.__setattr__(
                self,
                "coverage_certificate_sha256",
                _fixed_bytes(
                    self.coverage_certificate_sha256,
                    32,
                    "coverage_certificate_sha256",
                ),
            )
        elif self.coverage_certificate_sha256 is not None:
            raise ManifestError(
                "staging receipt cannot claim a coverage certificate"
            )

    @property
    def sort_key(self) -> tuple[Any, ...]:
        return self.namespace.sort_key + (
            self.begin_canonical_cursor,
            self.end_canonical_cursor,
            self.first_origin_ingress_sequence,
            self.min_origin_wal_end_pos,
        )

    def canonical_object(self) -> dict[str, Any]:
        return {
            "begin_canonical_cursor": self.begin_canonical_cursor,
            "coverage_certificate_sha256": (
                None
                if self.coverage_certificate_sha256 is None
                else self.coverage_certificate_sha256.hex()
            ),
            "coverage_kind": self.coverage_kind.value,
            "end_canonical_cursor": self.end_canonical_cursor,
            "first_origin_ingress_sequence": self.first_origin_ingress_sequence,
            "last_origin_ingress_sequence": self.last_origin_ingress_sequence,
            "max_origin_wal_end_pos": self.max_origin_wal_end_pos,
            "min_origin_wal_end_pos": self.min_origin_wal_end_pos,
            "namespace": self.namespace.canonical_object(),
        }


@dataclass(frozen=True, slots=True)
class SidecarDescriptor:
    """Persistent locator and whole-file identity for one watermark sidecar."""

    namespace: SidecarNamespace
    relative_path: str
    file_sha256: bytes
    file_size_bytes: int
    watermark_set_count: int

    def __post_init__(self) -> None:
        if not isinstance(self.namespace, SidecarNamespace):
            raise ManifestError("sidecar descriptor namespace is invalid")
        path = _safe_relative_path(self.relative_path)
        if not path.endswith(".l2ws"):
            raise ManifestError("watermark sidecar path must end in .l2ws")
        object.__setattr__(self, "relative_path", path)
        object.__setattr__(
            self,
            "file_sha256",
            _fixed_bytes(self.file_sha256, 32, "sidecar file_sha256"),
        )
        _uint(
            self.file_size_bytes,
            64,
            "sidecar file_size_bytes",
            positive=True,
        )
        count = _uint(
            self.watermark_set_count,
            32,
            "watermark_set_count",
        )
        if count > _MAX_WATERMARK_SETS_PER_SIDECAR:
            raise ManifestError("watermark_set_count exceeds the V1 bound")

    @property
    def sort_key(self) -> tuple[bytes, int]:
        return (
            self.namespace.run_id,
            self.namespace.table_generation,
        )

    @classmethod
    def from_sidecar(
        cls,
        relative_path: str,
        sidecar: WatermarkSidecar,
    ) -> "SidecarDescriptor":
        if not isinstance(sidecar, WatermarkSidecar):
            raise ManifestError("sidecar must be a WatermarkSidecar")
        encoded = encode_sidecar(sidecar)
        return cls(
            namespace=sidecar.namespace,
            relative_path=relative_path,
            file_sha256=hashlib.sha256(encoded).digest(),
            file_size_bytes=len(encoded),
            watermark_set_count=len(sidecar.watermark_sets),
        )

    def canonical_object(self) -> dict[str, Any]:
        return {
            "file_sha256": self.file_sha256.hex(),
            "file_size_bytes": self.file_size_bytes,
            "namespace": self.namespace.canonical_object(),
            "relative_path": self.relative_path,
            "watermark_set_count": self.watermark_set_count,
        }


@dataclass(frozen=True, slots=True)
class ArtifactEntry:
    artifact_type: DatasetKind
    relative_path: str
    file_sha256: bytes
    file_size_bytes: int
    row_count: int
    logical_content_sha256: bytes
    min_sort_key_v1: bytes
    max_sort_key_v1: bytes
    partition: PartitionSpec
    source_receipts: tuple[SourceRangeReceipt, ...]
    factor_group_membership_sha256: bytes | None = None
    factor_sidecar_refs: tuple[SidecarReference, ...] = ()
    visibility: ArtifactVisibility = ArtifactVisibility.STAGING

    def __post_init__(self) -> None:
        if not isinstance(self.artifact_type, DatasetKind):
            raise ManifestError("artifact_type must be a DatasetKind")
        path = _safe_relative_path(self.relative_path)
        object.__setattr__(self, "relative_path", path)
        if not path.endswith(".parquet"):
            raise ManifestError("typed history artifact path must end in .parquet")
        object.__setattr__(
            self,
            "file_sha256",
            _fixed_bytes(self.file_sha256, 32, "file_sha256"),
        )
        _uint(self.file_size_bytes, 64, "file_size_bytes", positive=True)
        _uint(self.row_count, 64, "row_count", positive=True)
        object.__setattr__(
            self,
            "logical_content_sha256",
            _fixed_bytes(
                self.logical_content_sha256, 32, "logical_content_sha256"
            ),
        )
        object.__setattr__(
            self,
            "min_sort_key_v1",
            _bounded_bytes(self.min_sort_key_v1, "min_sort_key_v1"),
        )
        object.__setattr__(
            self,
            "max_sort_key_v1",
            _bounded_bytes(self.max_sort_key_v1, "max_sort_key_v1"),
        )
        if not isinstance(self.partition, PartitionSpec):
            raise ManifestError("partition must be a PartitionSpec")
        if self.partition.dataset is not self.artifact_type:
            raise ManifestError("artifact type and partition dataset disagree")
        if path != self.partition.relative_prefix + PurePosixPath(path).name:
            raise ManifestError("artifact path does not match its typed partition")
        try:
            validate_parquet_basename_v1(PurePosixPath(path).name)
            # Close the manifest schema over every backend descriptor bound;
            # invalid row/file limits must fail at construction/decode time,
            # not later when a query happens to request the adapter.
            self.parquet_descriptor()
        except HistoryValidationError as error:
            raise ManifestError(
                "artifact cannot reconstruct a valid ParquetPartDescriptor"
            ) from error
        if type(self.source_receipts) is not tuple or len(
            self.source_receipts
        ) > _MAX_RECEIPTS_PER_ARTIFACT:
            raise ManifestError("source_receipts must be a bounded tuple")
        if any(
            not isinstance(receipt, SourceRangeReceipt)
            for receipt in self.source_receipts
        ):
            raise ManifestError("source_receipts contain an invalid value")
        ordered_receipts = tuple(
            sorted(self.source_receipts, key=lambda receipt: receipt.sort_key)
        )
        previous: SourceRangeReceipt | None = None
        for receipt in ordered_receipts:
            if previous is not None:
                if receipt.sort_key == previous.sort_key:
                    raise ManifestConflictError("duplicate exact source receipt")
                if (
                    receipt.namespace == previous.namespace
                    and receipt.begin_canonical_cursor
                    < previous.end_canonical_cursor
                ):
                    raise ManifestConflictError(
                        "source receipts overlap inside one exact namespace"
                    )
            previous = receipt
        object.__setattr__(self, "source_receipts", ordered_receipts)
        if self.artifact_type is DatasetKind.CANONICAL:
            if self.factor_group_membership_sha256 is not None:
                raise ManifestError(
                    "Canonical artifact cannot carry factor-group membership"
                )
        else:
            object.__setattr__(
                self,
                "factor_group_membership_sha256",
                _fixed_bytes(
                    self.factor_group_membership_sha256,
                    32,
                    "factor_group_membership_sha256",
                ),
            )
        if type(self.factor_sidecar_refs) is not tuple or len(
            self.factor_sidecar_refs
        ) > _MAX_SIDECAR_REFS_PER_ARTIFACT:
            raise ManifestError("factor_sidecar_refs must be a bounded tuple")
        if any(
            not isinstance(reference, SidecarReference)
            for reference in self.factor_sidecar_refs
        ):
            raise ManifestError("factor_sidecar_refs contain an invalid value")
        ordered_refs = tuple(
            sorted(self.factor_sidecar_refs, key=lambda reference: reference.sort_key)
        )
        for left, right in zip(ordered_refs, ordered_refs[1:]):
            if left.sort_key == right.sort_key:
                raise ManifestConflictError("duplicate factor sidecar reference")
        object.__setattr__(self, "factor_sidecar_refs", ordered_refs)
        if not isinstance(self.visibility, ArtifactVisibility):
            raise ManifestError("visibility must be an ArtifactVisibility")
        if self.artifact_type is DatasetKind.CANONICAL and ordered_refs:
            raise ManifestError("Canonical artifact cannot carry factor sidecar refs")
        if self.artifact_type is DatasetKind.FACTOR and not ordered_refs:
            raise ManifestError("nonempty Factor artifact requires sidecar references")
        if self.visibility is ArtifactVisibility.VISIBLE:
            if not ordered_receipts:
                raise ManifestError("visible artifact requires source coverage receipts")
            if any(
                receipt.coverage_kind is not CoverageKind.SEALED_COMMON_CUT
                for receipt in ordered_receipts
            ):
                raise ManifestError(
                    "only sealed/common-cut coverage may become manifest-visible"
                )
            certificates = {
                receipt.coverage_certificate_sha256
                for receipt in ordered_receipts
            }
            if len(certificates) != 1:
                raise ManifestConflictError(
                    "visible artifact receipts must share one common-cut certificate"
                )

    @property
    def sort_key(self) -> tuple[str, str]:
        return (self.artifact_type.value, self.relative_path)

    def canonical_object(self) -> dict[str, Any]:
        return {
            "artifact_type": self.artifact_type.value,
            "factor_group_membership_sha256": (
                None
                if self.factor_group_membership_sha256 is None
                else self.factor_group_membership_sha256.hex()
            ),
            "factor_sidecar_refs": [
                reference.canonical_object()
                for reference in self.factor_sidecar_refs
            ],
            "file_sha256": self.file_sha256.hex(),
            "file_size_bytes": self.file_size_bytes,
            "logical_content_sha256": self.logical_content_sha256.hex(),
            "max_sort_key_v1": self.max_sort_key_v1.hex(),
            "min_sort_key_v1": self.min_sort_key_v1.hex(),
            "partition": self.partition.canonical_object(),
            "relative_path": self.relative_path,
            "row_count": self.row_count,
            "source_receipts": [
                receipt.canonical_object() for receipt in self.source_receipts
            ],
            "visibility": self.visibility.value,
        }

    def parquet_descriptor(self) -> ParquetPartDescriptor:
        """Rebuild the exact backend descriptor persisted by this entry."""
        schema_name = (
            CANONICAL_PARQUET_SCHEMA_NAME_V1
            if self.artifact_type is DatasetKind.CANONICAL
            else FACTOR_PARQUET_SCHEMA_NAME_V1
        )
        return ParquetPartDescriptor(
            basename=PurePosixPath(self.relative_path).name,
            schema_name=schema_name,
            row_count=self.row_count,
            byte_size=self.file_size_bytes,
            bucket=self.partition.bucket,
            logical_rows_sha256=self.logical_content_sha256,
            file_sha256=self.file_sha256,
            min_sort_key_v1=self.min_sort_key_v1,
            max_sort_key_v1=self.max_sort_key_v1,
        )


def _receipt_range_identity(receipt: SourceRangeReceipt) -> tuple[Any, ...]:
    return (
        receipt.namespace,
        receipt.begin_canonical_cursor,
        receipt.end_canonical_cursor,
        receipt.first_origin_ingress_sequence,
        receipt.last_origin_ingress_sequence,
        receipt.min_origin_wal_end_pos,
        receipt.max_origin_wal_end_pos,
    )


def _is_staging_promotion(old: ArtifactEntry, new: ArtifactEntry) -> bool:
    if (
        old.visibility is not ArtifactVisibility.STAGING
        or new.visibility is not ArtifactVisibility.VISIBLE
        or old.artifact_type is not new.artifact_type
        or old.relative_path != new.relative_path
        or old.file_sha256 != new.file_sha256
        or old.file_size_bytes != new.file_size_bytes
        or old.row_count != new.row_count
        or old.logical_content_sha256 != new.logical_content_sha256
        or old.min_sort_key_v1 != new.min_sort_key_v1
        or old.max_sort_key_v1 != new.max_sort_key_v1
        or old.partition != new.partition
        or old.factor_group_membership_sha256
        != new.factor_group_membership_sha256
        or old.factor_sidecar_refs != new.factor_sidecar_refs
        or len(old.source_receipts) != len(new.source_receipts)
    ):
        return False
    for old_receipt, new_receipt in zip(
        old.source_receipts,
        new.source_receipts,
    ):
        if _receipt_range_identity(old_receipt) != _receipt_range_identity(
            new_receipt
        ):
            return False
        if new_receipt.coverage_kind is not CoverageKind.SEALED_COMMON_CUT:
            return False
        if (
            old_receipt.coverage_kind is CoverageKind.SEALED_COMMON_CUT
            and old_receipt.coverage_certificate_sha256
            != new_receipt.coverage_certificate_sha256
        ):
            return False
    return True


@dataclass(frozen=True, slots=True)
class HistoryManifest:
    run_id: bytes
    generation: int
    previous_manifest_sha256: bytes | None
    artifacts: tuple[ArtifactEntry, ...]
    sidecars: tuple[SidecarDescriptor, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "run_id", _fixed_bytes(self.run_id, 16, "run_id"))
        _uint(self.generation, 64, "generation", positive=True)
        if self.generation == 1:
            if self.previous_manifest_sha256 is not None:
                raise ManifestError("generation 1 must not name a predecessor")
        else:
            object.__setattr__(
                self,
                "previous_manifest_sha256",
                _fixed_bytes(
                    self.previous_manifest_sha256,
                    32,
                    "previous_manifest_sha256",
                ),
            )
        if type(self.artifacts) is not tuple or len(self.artifacts) > _MAX_ARTIFACTS:
            raise ManifestError("artifacts must be a bounded tuple")
        if any(not isinstance(item, ArtifactEntry) for item in self.artifacts):
            raise ManifestError("artifacts contain an invalid value")
        ordered = tuple(sorted(self.artifacts, key=lambda item: item.sort_key))
        paths: set[str] = set()
        previous_key: tuple[str, str] | None = None
        for item in ordered:
            if item.relative_path in paths or item.sort_key == previous_key:
                raise ManifestConflictError("manifest artifact path is duplicated")
            paths.add(item.relative_path)
            previous_key = item.sort_key
        object.__setattr__(self, "artifacts", ordered)
        if type(self.sidecars) is not tuple or len(
            self.sidecars
        ) > _MAX_SIDECAR_DESCRIPTORS:
            raise ManifestError("sidecars must be a bounded tuple")
        if any(
            not isinstance(descriptor, SidecarDescriptor)
            for descriptor in self.sidecars
        ):
            raise ManifestError("sidecars contain an invalid descriptor")
        ordered_sidecars = tuple(
            sorted(self.sidecars, key=lambda descriptor: descriptor.sort_key)
        )
        namespaces: set[SidecarNamespace] = set()
        sidecar_paths: set[str] = set()
        for descriptor in ordered_sidecars:
            if descriptor.namespace in namespaces:
                raise ManifestConflictError(
                    "manifest has duplicate sidecar namespaces"
                )
            if descriptor.relative_path in sidecar_paths:
                raise ManifestConflictError("manifest has duplicate sidecar paths")
            namespaces.add(descriptor.namespace)
            sidecar_paths.add(descriptor.relative_path)
        for artifact in ordered:
            for reference in artifact.factor_sidecar_refs:
                if reference.namespace not in namespaces:
                    raise ManifestConflictError(
                        "manifest has an orphan sidecar reference"
                    )
        object.__setattr__(self, "sidecars", ordered_sidecars)

    def canonical_object(self) -> dict[str, Any]:
        return {
            "artifacts": [item.canonical_object() for item in self.artifacts],
            "encoding": "l2flow-history-manifest-v1",
            "generation": self.generation,
            "previous_manifest_sha256": (
                None
                if self.previous_manifest_sha256 is None
                else self.previous_manifest_sha256.hex()
            ),
            "run_id": self.run_id.hex(),
            "sidecars": [
                descriptor.canonical_object() for descriptor in self.sidecars
            ],
        }

    @property
    def visible_artifacts(self) -> tuple[ArtifactEntry, ...]:
        return tuple(
            item
            for item in self.artifacts
            if item.visibility is ArtifactVisibility.VISIBLE
        )

    def validate_sidecars(
        self,
        sidecars: Mapping[SidecarNamespace, WatermarkSidecar],
    ) -> None:
        sidecars = _freeze_sidecar_mapping(sidecars)
        descriptors = {
            descriptor.namespace: descriptor for descriptor in self.sidecars
        }
        if set(sidecars) != set(descriptors):
            raise ManifestConflictError(
                "sidecar mapping does not exactly match manifest descriptors"
            )
        for namespace, descriptor in descriptors.items():
            sidecar = sidecars[namespace]
            encoded = encode_sidecar(sidecar)
            if len(encoded) != descriptor.file_size_bytes:
                raise ManifestConflictError("sidecar descriptor size mismatch")
            if hashlib.sha256(encoded).digest() != descriptor.file_sha256:
                raise ManifestConflictError("sidecar descriptor SHA-256 mismatch")
            if len(sidecar.watermark_sets) != descriptor.watermark_set_count:
                raise ManifestConflictError("sidecar descriptor set-count mismatch")
        for artifact in self.artifacts:
            for reference in artifact.factor_sidecar_refs:
                sidecar = sidecars.get(reference.namespace)
                if sidecar is None:
                    raise ManifestConflictError("manifest has an orphan sidecar reference")
                sidecar.resolve(reference)

    def validate_successor(self, previous: "HistoryManifest") -> None:
        if not isinstance(previous, HistoryManifest):
            raise ManifestError("previous must be a HistoryManifest")
        if self.run_id != previous.run_id:
            raise ManifestConflictError("manifest run_id changed inside one chain")
        if self.generation != previous.generation + 1:
            raise ManifestConflictError("manifest generation is not predecessor + 1")
        if self.previous_manifest_sha256 != manifest_sha256(previous):
            raise ManifestConflictError("manifest predecessor SHA-256 mismatch")
        previous_by_path = {item.relative_path: item for item in previous.artifacts}
        current_by_path = {item.relative_path: item for item in self.artifacts}
        for path, old in previous_by_path.items():
            item = current_by_path.get(path)
            if item is None:
                raise ManifestConflictError(
                    "successor removed an immutable artifact entry"
                )
            if old.visibility is ArtifactVisibility.VISIBLE:
                if item != old:
                    raise ManifestConflictError(
                        "successor changed a visible artifact entry"
                    )
                continue
            if item == old:
                continue
            if not _is_staging_promotion(old, item):
                raise ManifestConflictError(
                    "successor made an invalid staging artifact transition"
                )
        for item in self.artifacts:
            old = previous_by_path.get(item.relative_path)
            if old is None:
                continue
            if (
                old.artifact_type is not item.artifact_type
                or old.file_sha256 != item.file_sha256
                or old.file_size_bytes != item.file_size_bytes
                or old.logical_content_sha256 != item.logical_content_sha256
                or old.min_sort_key_v1 != item.min_sort_key_v1
                or old.max_sort_key_v1 != item.max_sort_key_v1
                or old.partition != item.partition
                or old.factor_group_membership_sha256
                != item.factor_group_membership_sha256
            ):
                raise ManifestConflictError(
                    "immutable artifact path was rebound to different file content"
                )
        previous_sidecars = {
            descriptor.namespace: descriptor for descriptor in previous.sidecars
        }
        current_sidecars = {
            descriptor.namespace: descriptor for descriptor in self.sidecars
        }
        for namespace, descriptor in previous_sidecars.items():
            if current_sidecars.get(namespace) != descriptor:
                raise ManifestConflictError(
                    "successor removed or changed an immutable sidecar descriptor"
                )


def encode_manifest(manifest: HistoryManifest) -> bytes:
    if not isinstance(manifest, HistoryManifest):
        raise ManifestError("manifest must be a HistoryManifest")
    payload = _canonical_json(manifest.canonical_object())
    return _encode_envelope(_MANIFEST_MAGIC, payload, _MAX_MANIFEST_JSON_BYTES)


def manifest_sha256(manifest: HistoryManifest) -> bytes:
    return hashlib.sha256(encode_manifest(manifest)).digest()


def _decode_partition(value: Any) -> PartitionSpec:
    item = _require_object(
        value,
        {
            "bucket",
            "dataset",
            "event_type",
            "factor_group",
            "factor_version",
            "market",
            "trade_date",
        },
        "partition",
    )
    try:
        return PartitionSpec(
            dataset=DatasetKind(item["dataset"]),
            trade_date=_uint(item["trade_date"], 32, "trade_date", positive=True),
            bucket=_uint(item["bucket"], 8, "bucket"),
            market=item["market"],
            event_type=item["event_type"],
            factor_group=item["factor_group"],
            factor_version=item["factor_version"],
        )
    except (ValueError, TypeError) as error:
        if isinstance(error, SidecarError):
            raise
        raise ManifestError("partition failed strict validation") from error


_SOURCE_NAMESPACE_KEYS = {
    "canonical_generation",
    "clock_epoch_algorithm",
    "clock_epoch_digest",
    "dtype_sha256",
    "family",
    "normalizer_build_sha256",
    "normalizer_config_sha256",
    "origin_capture_date",
    "origin_source_generation",
    "origin_source_writer_instance",
    "origin_stream_day_id",
    "registry_sha256",
    "registry_version",
    "schema_sha256",
    "shard_id",
    "source_stream_id",
    "trade_date",
}


def _decode_source_namespace(value: Any) -> SourceNamespace:
    item = _require_object(value, _SOURCE_NAMESPACE_KEYS, "source namespace")
    try:
        return SourceNamespace(
            trade_date=_uint(item["trade_date"], 32, "trade_date", positive=True),
            source_stream_id=_uint(
                item["source_stream_id"], 32, "source_stream_id", positive=True
            ),
            origin_capture_date=_uint(
                item["origin_capture_date"],
                32,
                "origin_capture_date",
                positive=True,
            ),
            origin_stream_day_id=_fixed_bytes(
                item["origin_stream_day_id"], 16, "origin_stream_day_id"
            ),
            family=InputFamily(item["family"]),
            shard_id=_uint(item["shard_id"], 32, "shard_id"),
            origin_source_writer_instance=_fixed_bytes(
                item["origin_source_writer_instance"],
                16,
                "origin_source_writer_instance",
            ),
            origin_source_generation=_uint(
                item["origin_source_generation"],
                64,
                "origin_source_generation",
                positive=True,
            ),
            canonical_generation=_uint(
                item["canonical_generation"],
                64,
                "canonical_generation",
                positive=True,
            ),
            clock_epoch_algorithm=_uint(
                item["clock_epoch_algorithm"],
                32,
                "clock_epoch_algorithm",
                positive=True,
            ),
            clock_epoch_digest=_fixed_bytes(
                item["clock_epoch_digest"], 32, "clock_epoch_digest"
            ),
            schema_sha256=_fixed_bytes(
                item["schema_sha256"], 32, "schema_sha256"
            ),
            dtype_sha256=_fixed_bytes(
                item["dtype_sha256"], 32, "dtype_sha256"
            ),
            registry_version=_uint(
                item["registry_version"], 64, "registry_version", positive=True
            ),
            registry_sha256=_fixed_bytes(
                item["registry_sha256"], 32, "registry_sha256"
            ),
            normalizer_build_sha256=_fixed_bytes(
                item["normalizer_build_sha256"],
                32,
                "normalizer_build_sha256",
            ),
            normalizer_config_sha256=_fixed_bytes(
                item["normalizer_config_sha256"],
                32,
                "normalizer_config_sha256",
            ),
        )
    except (ValueError, TypeError) as error:
        if isinstance(error, SidecarError):
            raise
        raise ManifestError("source namespace failed strict validation") from error


def _decode_receipt(value: Any) -> SourceRangeReceipt:
    item = _require_object(
        value,
        {
            "begin_canonical_cursor",
            "coverage_certificate_sha256",
            "coverage_kind",
            "end_canonical_cursor",
            "first_origin_ingress_sequence",
            "last_origin_ingress_sequence",
            "max_origin_wal_end_pos",
            "min_origin_wal_end_pos",
            "namespace",
        },
        "source receipt",
    )
    try:
        return SourceRangeReceipt(
            namespace=_decode_source_namespace(item["namespace"]),
            begin_canonical_cursor=_uint(
                item["begin_canonical_cursor"], 64, "begin_canonical_cursor"
            ),
            end_canonical_cursor=_uint(
                item["end_canonical_cursor"], 64, "end_canonical_cursor"
            ),
            first_origin_ingress_sequence=_uint(
                item["first_origin_ingress_sequence"],
                64,
                "first_origin_ingress_sequence",
                positive=True,
            ),
            last_origin_ingress_sequence=_uint(
                item["last_origin_ingress_sequence"],
                64,
                "last_origin_ingress_sequence",
                positive=True,
            ),
            min_origin_wal_end_pos=_uint(
                item["min_origin_wal_end_pos"],
                64,
                "min_origin_wal_end_pos",
                positive=True,
            ),
            max_origin_wal_end_pos=_uint(
                item["max_origin_wal_end_pos"],
                64,
                "max_origin_wal_end_pos",
                positive=True,
            ),
            coverage_kind=CoverageKind(item["coverage_kind"]),
            coverage_certificate_sha256=(
                None
                if item["coverage_certificate_sha256"] is None
                else _fixed_bytes(
                    item["coverage_certificate_sha256"],
                    32,
                    "coverage_certificate_sha256",
                )
            ),
        )
    except (ValueError, TypeError) as error:
        if isinstance(error, SidecarError):
            raise
        raise ManifestError("source receipt failed strict validation") from error


def _decode_sidecar_reference(value: Any) -> SidecarReference:
    item = _require_object(
        value,
        {
            "full_map_sha256",
            "input_identity_sha256",
            "namespace",
            "watermark_set_id",
        },
        "sidecar reference",
    )
    namespace_object = _require_object(
        item["namespace"], {"run_id", "table_generation"}, "sidecar namespace"
    )
    return SidecarReference(
        namespace=SidecarNamespace(
            run_id=_fixed_bytes(namespace_object["run_id"], 16, "run_id"),
            table_generation=_uint(
                namespace_object["table_generation"],
                64,
                "table_generation",
                positive=True,
            ),
        ),
        watermark_set_id=_uint(
            item["watermark_set_id"], 64, "watermark_set_id", positive=True
        ),
        input_identity_sha256=_fixed_bytes(
            item["input_identity_sha256"], 32, "input_identity_sha256"
        ),
        full_map_sha256=_fixed_bytes(
            item["full_map_sha256"], 32, "full_map_sha256"
        ),
    )


def _decode_sidecar_descriptor(value: Any) -> SidecarDescriptor:
    item = _require_object(
        value,
        {
            "file_sha256",
            "file_size_bytes",
            "namespace",
            "relative_path",
            "watermark_set_count",
        },
        "sidecar descriptor",
    )
    namespace_object = _require_object(
        item["namespace"],
        {"run_id", "table_generation"},
        "sidecar descriptor namespace",
    )
    return SidecarDescriptor(
        namespace=SidecarNamespace(
            run_id=_fixed_bytes(namespace_object["run_id"], 16, "run_id"),
            table_generation=_uint(
                namespace_object["table_generation"],
                64,
                "table_generation",
                positive=True,
            ),
        ),
        relative_path=_safe_relative_path(item["relative_path"]),
        file_sha256=_fixed_bytes(
            item["file_sha256"], 32, "sidecar file_sha256"
        ),
        file_size_bytes=_uint(
            item["file_size_bytes"],
            64,
            "sidecar file_size_bytes",
            positive=True,
        ),
        watermark_set_count=_uint(
            item["watermark_set_count"],
            32,
            "watermark_set_count",
        ),
    )


def _decode_artifact(value: Any) -> ArtifactEntry:
    item = _require_object(
        value,
        {
            "artifact_type",
            "factor_group_membership_sha256",
            "factor_sidecar_refs",
            "file_sha256",
            "file_size_bytes",
            "logical_content_sha256",
            "max_sort_key_v1",
            "min_sort_key_v1",
            "partition",
            "relative_path",
            "row_count",
            "source_receipts",
            "visibility",
        },
        "artifact",
    )
    receipts_value = item["source_receipts"]
    refs_value = item["factor_sidecar_refs"]
    if not isinstance(receipts_value, list) or len(
        receipts_value
    ) > _MAX_RECEIPTS_PER_ARTIFACT:
        raise ManifestError("artifact source receipt array is invalid")
    if not isinstance(refs_value, list) or len(
        refs_value
    ) > _MAX_SIDECAR_REFS_PER_ARTIFACT:
        raise ManifestError("artifact sidecar reference array is invalid")
    try:
        return ArtifactEntry(
            artifact_type=DatasetKind(item["artifact_type"]),
            relative_path=_safe_relative_path(item["relative_path"]),
            file_sha256=_fixed_bytes(item["file_sha256"], 32, "file_sha256"),
            file_size_bytes=_uint(
                item["file_size_bytes"], 64, "file_size_bytes", positive=True
            ),
            row_count=_uint(item["row_count"], 64, "row_count"),
            logical_content_sha256=_fixed_bytes(
                item["logical_content_sha256"],
                32,
                "logical_content_sha256",
            ),
            min_sort_key_v1=_bounded_bytes(
                item["min_sort_key_v1"], "min_sort_key_v1"
            ),
            max_sort_key_v1=_bounded_bytes(
                item["max_sort_key_v1"], "max_sort_key_v1"
            ),
            partition=_decode_partition(item["partition"]),
            source_receipts=tuple(_decode_receipt(value) for value in receipts_value),
            factor_group_membership_sha256=(
                None
                if item["factor_group_membership_sha256"] is None
                else _fixed_bytes(
                    item["factor_group_membership_sha256"],
                    32,
                    "factor_group_membership_sha256",
                )
            ),
            factor_sidecar_refs=tuple(
                _decode_sidecar_reference(value) for value in refs_value
            ),
            visibility=ArtifactVisibility(item["visibility"]),
        )
    except (ValueError, TypeError) as error:
        if isinstance(error, SidecarError):
            raise
        raise ManifestError("artifact failed strict validation") from error


def decode_manifest(blob: bytes) -> HistoryManifest:
    try:
        payload = _decode_envelope(
            blob,
            _MANIFEST_MAGIC,
            _MAX_MANIFEST_JSON_BYTES,
        )
        root = _decode_json(payload, _MAX_MANIFEST_JSON_BYTES)
        root = _require_object(
            root,
            {
                "artifacts",
                "encoding",
                "generation",
                "previous_manifest_sha256",
                "run_id",
                "sidecars",
            },
            "manifest",
        )
        if root["encoding"] != "l2flow-history-manifest-v1":
            raise ManifestError("history manifest encoding mismatch")
        artifacts_value = root["artifacts"]
        if not isinstance(artifacts_value, list) or len(
            artifacts_value
        ) > _MAX_ARTIFACTS:
            raise ManifestError("manifest artifact array is invalid")
        sidecars_value = root["sidecars"]
        if not isinstance(sidecars_value, list) or len(
            sidecars_value
        ) > _MAX_SIDECAR_DESCRIPTORS:
            raise ManifestError("manifest sidecar descriptor array is invalid")
        manifest = HistoryManifest(
            run_id=_fixed_bytes(root["run_id"], 16, "run_id"),
            generation=_uint(root["generation"], 64, "generation", positive=True),
            previous_manifest_sha256=(
                None
                if root["previous_manifest_sha256"] is None
                else _fixed_bytes(
                    root["previous_manifest_sha256"],
                    32,
                    "previous_manifest_sha256",
                )
            ),
            artifacts=tuple(_decode_artifact(value) for value in artifacts_value),
            sidecars=tuple(
                _decode_sidecar_descriptor(value) for value in sidecars_value
            ),
        )
        if _canonical_json(manifest.canonical_object()) != payload:
            raise ManifestError("history manifest JSON is not canonical")
        return manifest
    except SidecarError as error:
        if isinstance(error, ManifestError):
            raise
        raise ManifestError(str(error)) from error


@dataclass(frozen=True, slots=True)
class _CurrentPointer:
    generation: int
    manifest_filename: str
    manifest_sha256: bytes

    def __post_init__(self) -> None:
        _uint(self.generation, 64, "generation", positive=True)
        _safe_token(self.manifest_filename, "manifest_filename", maximum=255)
        object.__setattr__(
            self,
            "manifest_sha256",
            _fixed_bytes(self.manifest_sha256, 32, "manifest_sha256"),
        )
        if self.manifest_filename != _manifest_filename(
            self.generation, self.manifest_sha256
        ):
            raise ManifestError("CURRENT manifest filename/identity mismatch")

    def canonical_object(self) -> dict[str, Any]:
        return {
            "encoding": "l2flow-history-current-v1",
            "generation": self.generation,
            "manifest_filename": self.manifest_filename,
            "manifest_sha256": self.manifest_sha256.hex(),
        }


def _manifest_filename(generation: int, digest: bytes) -> str:
    _uint(generation, 64, "generation", positive=True)
    digest = _fixed_bytes(digest, 32, "manifest_sha256")
    return f"manifest-{generation:020d}-{digest.hex()}.l2hm"


def _encode_current(pointer: _CurrentPointer) -> bytes:
    return _encode_envelope(
        _CURRENT_MAGIC,
        _canonical_json(pointer.canonical_object()),
        _MAX_CURRENT_JSON_BYTES,
    )


def _decode_current(blob: bytes) -> _CurrentPointer:
    try:
        payload = _decode_envelope(blob, _CURRENT_MAGIC, _MAX_CURRENT_JSON_BYTES)
        root = _decode_json(payload, _MAX_CURRENT_JSON_BYTES)
        root = _require_object(
            root,
            {
                "encoding",
                "generation",
                "manifest_filename",
                "manifest_sha256",
            },
            "CURRENT",
        )
        if root["encoding"] != "l2flow-history-current-v1":
            raise ManifestError("CURRENT encoding mismatch")
        pointer = _CurrentPointer(
            generation=_uint(
                root["generation"], 64, "generation", positive=True
            ),
            manifest_filename=_safe_token(
                root["manifest_filename"], "manifest_filename", maximum=255
            ),
            manifest_sha256=_fixed_bytes(
                root["manifest_sha256"], 32, "manifest_sha256"
            ),
        )
        if _canonical_json(pointer.canonical_object()) != payload:
            raise ManifestError("CURRENT JSON is not canonical")
        return pointer
    except SidecarError as error:
        if isinstance(error, ManifestError):
            raise
        raise ManifestError(str(error)) from error


FaultInjector = Callable[[str], None]


class AtomicManifestStore:
    """Linux-local immutable manifest chain with a flock-serialized CURRENT CAS.

    Artifact entries are lineage assertions over opaque files.  This class does
    not parse Parquet and never deletes or mutates source data/artifact files.
    Cooperative writers must use this store and its directory flock; hostile
    same-UID mutation requires a separate service UID/mount boundary.
    """

    def __init__(
        self,
        directory: str | os.PathLike[str],
        *,
        fault_injector: FaultInjector | None = None,
        maximum_verified_artifact_bytes: int = 2 * 1024 * 1024 * 1024,
    ) -> None:
        configured_directory = os.fspath(directory)
        if (
            not isinstance(configured_directory, str)
            or not configured_directory
            or "\x00" in configured_directory
            or not configured_directory.startswith("/")
            or configured_directory.startswith("//")
            or configured_directory == "/"
            or os.path.normpath(configured_directory) != configured_directory
        ):
            raise ManifestStoreError(
                "manifest directory must be a normalized absolute non-root path"
            )
        components = configured_directory.split("/")[1:]
        if not components or any(item in ("", ".", "..") for item in components):
            raise ManifestStoreError("manifest directory path is invalid")
        if fault_injector is not None and not callable(fault_injector):
            raise ManifestStoreError("fault_injector must be callable")
        self._fault_injector = fault_injector
        self._maximum_verified_artifact_bytes = _uint(
            maximum_verified_artifact_bytes,
            64,
            "maximum_verified_artifact_bytes",
            positive=True,
        )
        self._directory = configured_directory
        self._lifecycle_lock = threading.Lock()
        self._root_fd = self._open_pinned_root(components)
        root_status = os.fstat(self._root_fd)
        self._root_identity = (root_status.st_dev, root_status.st_ino)

    @staticmethod
    def _open_pinned_root(components: list[str]) -> int:
        flags = (
            os.O_RDONLY
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        descriptor = -1
        try:
            descriptor = os.open("/", flags)
            for component in components:
                child = os.open(component, flags, dir_fd=descriptor)
                child_status = os.fstat(child)
                if not stat.S_ISDIR(child_status.st_mode):
                    os.close(child)
                    raise ManifestStoreError(
                        "manifest root component is not a directory"
                    )
                os.close(descriptor)
                descriptor = child
            return descriptor
        except OSError as error:
            if descriptor >= 0:
                os.close(descriptor)
            raise ManifestStoreError(
                "cannot securely traverse manifest directory"
            ) from error
        except ManifestStoreError:
            if descriptor >= 0:
                os.close(descriptor)
            raise

    def close(self) -> None:
        lock = getattr(self, "_lifecycle_lock", None)
        if lock is None:
            return
        with lock:
            descriptor = getattr(self, "_root_fd", -1)
            if descriptor >= 0:
                self._root_fd = -1
                os.close(descriptor)

    def __enter__(self) -> "AtomicManifestStore":
        lock = getattr(self, "_lifecycle_lock", None)
        if lock is None:
            raise ManifestStoreError("manifest store is closed")
        with lock:
            if getattr(self, "_root_fd", -1) < 0:
                raise ManifestStoreError("manifest store is closed")
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    def __copy__(self) -> "AtomicManifestStore":
        raise ManifestStoreError("a pinned manifest store cannot be copied")

    def __deepcopy__(self, memo: object) -> "AtomicManifestStore":
        raise ManifestStoreError("a pinned manifest store cannot be copied")

    def __reduce_ex__(self, protocol: int) -> object:
        raise ManifestStoreError("a pinned manifest store cannot be serialized")

    def _fault(self, step: str) -> None:
        if self._fault_injector is not None:
            self._fault_injector(step)

    def _open_directory(self) -> int:
        flags = (
            os.O_RDONLY
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        descriptor = -1
        try:
            lock = getattr(self, "_lifecycle_lock", None)
            if lock is None:
                raise ManifestStoreError("manifest store is closed")
            with lock:
                root_fd = getattr(self, "_root_fd", -1)
                if root_fd < 0:
                    raise ManifestStoreError("manifest store is closed")
                retained_status = os.fstat(root_fd)
                if (
                    not stat.S_ISDIR(retained_status.st_mode)
                    or retained_status.st_nlink < 1
                    or (retained_status.st_dev, retained_status.st_ino)
                    != self._root_identity
                ):
                    raise ManifestStoreError("retained manifest root identity changed")
                descriptor = os.open(".", flags, dir_fd=root_fd)
                status = os.fstat(descriptor)
                if (
                    not stat.S_ISDIR(status.st_mode)
                    or status.st_nlink < 1
                    or (status.st_dev, status.st_ino) != self._root_identity
                ):
                    os.close(descriptor)
                    descriptor = -1
                    raise ManifestStoreError("manifest root identity changed")
            return descriptor
        except OSError as error:
            if descriptor >= 0:
                os.close(descriptor)
            raise ManifestStoreError("cannot securely open manifest directory") from error

    @staticmethod
    def _open_relative_parent(
        directory_fd: int,
        relative_path: str,
    ) -> tuple[int, str]:
        if (
            not isinstance(relative_path, str)
            or not relative_path
            or relative_path.startswith("/")
            or "\x00" in relative_path
            or "\\" in relative_path
        ):
            raise ManifestStoreError("relative store path is invalid")
        parts = relative_path.split("/")
        if any(part in ("", ".", "..") for part in parts):
            raise ManifestStoreError("relative store path is not canonical")
        flags = (
            os.O_RDONLY
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        parent = -1
        try:
            parent = os.open(".", flags, dir_fd=directory_fd)
            for component in parts[:-1]:
                child = os.open(component, flags, dir_fd=parent)
                status = os.fstat(child)
                if not stat.S_ISDIR(status.st_mode):
                    os.close(child)
                    raise ManifestStoreError(
                        "relative store path parent is not a directory"
                    )
                os.close(parent)
                parent = child
            return parent, parts[-1]
        except OSError as error:
            if parent >= 0:
                os.close(parent)
            raise ManifestStoreError(
                "cannot securely traverse relative store path"
            ) from error
        except ManifestStoreError:
            if parent >= 0:
                os.close(parent)
            raise

    @classmethod
    def _open_relative_regular(
        cls,
        directory_fd: int,
        relative_path: str,
    ) -> int:
        parent = -1
        descriptor = -1
        try:
            parent, basename = cls._open_relative_parent(
                directory_fd,
                relative_path,
            )
            descriptor = os.open(
                basename,
                os.O_RDONLY
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0)
                | getattr(os, "O_NONBLOCK", 0),
                dir_fd=parent,
            )
            return descriptor
        except OSError as error:
            if descriptor >= 0:
                os.close(descriptor)
            raise ManifestStoreError("manifest object open failed") from error
        finally:
            if parent >= 0:
                os.close(parent)

    @classmethod
    def _read_regular_at(
        cls,
        directory_fd: int,
        name: str,
        maximum_bytes: int,
    ) -> bytes:
        descriptor = -1
        try:
            descriptor = cls._open_relative_regular(directory_fd, name)
            status = os.fstat(descriptor)
            if (
                not stat.S_ISREG(status.st_mode)
                or status.st_nlink != 1
                or status.st_mode & 0o077
                or status.st_size < 0
                or status.st_size > maximum_bytes
            ):
                raise ManifestStoreError("manifest object is not a bounded regular file")
            remaining = status.st_size
            chunks: list[bytes] = []
            while remaining:
                chunk = os.read(descriptor, min(remaining, 1024 * 1024))
                if not chunk:
                    raise ManifestStoreError("manifest object became truncated")
                chunks.append(chunk)
                remaining -= len(chunk)
            if os.read(descriptor, 1):
                raise ManifestStoreError("manifest object grew during read")
            after = os.fstat(descriptor)
            if (
                after.st_dev != status.st_dev
                or after.st_ino != status.st_ino
                or after.st_size != status.st_size
                or after.st_mtime_ns != status.st_mtime_ns
                or after.st_ctime_ns != status.st_ctime_ns
            ):
                raise ManifestStoreError("manifest object changed during read")
            return b"".join(chunks)
        except OSError as error:
            raise ManifestStoreError("manifest object read failed") from error
        finally:
            if descriptor >= 0:
                os.close(descriptor)

    @classmethod
    def _verify_regular_digest_at(
        cls,
        directory_fd: int,
        relative_path: str,
        *,
        expected_size: int,
        expected_sha256: bytes,
        maximum_bytes: int,
    ) -> None:
        if expected_size > maximum_bytes:
            raise ManifestStoreError("published object exceeds verification budget")
        descriptor = -1
        try:
            descriptor = cls._open_relative_regular(directory_fd, relative_path)
            status = os.fstat(descriptor)
            if (
                not stat.S_ISREG(status.st_mode)
                or status.st_nlink != 1
                or status.st_mode & 0o077
                or status.st_size != expected_size
            ):
                raise ManifestStoreError(
                    "published object is not the expected single-link regular file"
                )
            digest = hashlib.sha256()
            remaining = expected_size
            while remaining:
                chunk = os.read(descriptor, min(remaining, 1024 * 1024))
                if not chunk:
                    raise ManifestStoreError("published object became truncated")
                digest.update(chunk)
                remaining -= len(chunk)
            if os.read(descriptor, 1):
                raise ManifestStoreError("published object grew during verification")
            after = os.fstat(descriptor)
            if (
                after.st_dev != status.st_dev
                or after.st_ino != status.st_ino
                or after.st_size != status.st_size
                or after.st_mtime_ns != status.st_mtime_ns
                or after.st_ctime_ns != status.st_ctime_ns
            ):
                raise ManifestStoreError("published object changed during verification")
            if digest.digest() != expected_sha256:
                raise ManifestStoreError("published object SHA-256 mismatch")
        except OSError as error:
            raise ManifestStoreError("published object verification failed") from error
        finally:
            if descriptor >= 0:
                os.close(descriptor)

    @staticmethod
    def _write_all(descriptor: int, value: bytes) -> None:
        offset = 0
        while offset < len(value):
            try:
                written = os.write(descriptor, value[offset:])
            except InterruptedError:
                continue
            if written <= 0:
                raise ManifestStoreError("manifest write made no progress")
            offset += written

    @staticmethod
    def _new_temporary_name(prefix: str) -> str:
        return f".{prefix}.tmp-{os.getpid()}-{secrets.token_hex(12)}"

    @staticmethod
    def _rename_noreplace(directory_fd: int, source: str, target: str) -> None:
        libc = ctypes.CDLL(None, use_errno=True)
        renameat2 = getattr(libc, "renameat2", None)
        if renameat2 is None:
            raise OSError(
                errno.ENOSYS,
                "renameat2(RENAME_NOREPLACE) is unavailable; refusing unsafe fallback",
                target,
            )
        renameat2.argtypes = [
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        ]
        renameat2.restype = ctypes.c_int
        result = renameat2(
            directory_fd,
            os.fsencode(source),
            directory_fd,
            os.fsencode(target),
            _RENAME_NOREPLACE,
        )
        if result == 0:
            return
        error_number = ctypes.get_errno()
        if error_number == errno.EEXIST:
            raise FileExistsError(error_number, os.strerror(error_number), target)
        # A link+unlink fallback has a crash window that can leave the final
        # object with two links and make every idempotent retry fail.  Treat an
        # unavailable syscall/filesystem flag as an unsupported capability.
        raise OSError(error_number, os.strerror(error_number), target)

    def _load_current_locked(
        self,
        directory_fd: int,
    ) -> tuple[_CurrentPointer, HistoryManifest, bytes] | None:
        try:
            current_bytes = self._read_regular_at(
                directory_fd,
                _CURRENT_NAME,
                _MAX_CURRENT_JSON_BYTES + 48,
            )
        except ManifestStoreError as error:
            cause = error.__cause__
            if isinstance(cause, OSError) and cause.errno == errno.ENOENT:
                return None
            raise
        pointer = _decode_current(current_bytes)
        manifest_bytes = self._read_regular_at(
            directory_fd,
            pointer.manifest_filename,
            _MAX_MANIFEST_JSON_BYTES + 48,
        )
        if hashlib.sha256(manifest_bytes).digest() != pointer.manifest_sha256:
            raise ManifestStoreError("CURRENT manifest file SHA-256 mismatch")
        manifest = decode_manifest(manifest_bytes)
        if manifest.generation != pointer.generation:
            raise ManifestStoreError("CURRENT generation differs from manifest")
        return pointer, manifest, manifest_bytes

    @staticmethod
    def _unlink_owned_temp(directory_fd: int, name: str | None) -> None:
        if name is None:
            return
        try:
            os.unlink(name, dir_fd=directory_fd)
        except FileNotFoundError:
            return

    def _publish_immutable_manifest(
        self,
        directory_fd: int,
        filename: str,
        encoded: bytes,
    ) -> None:
        temporary = self._new_temporary_name("manifest")
        descriptor = -1
        preserve_temporary = False
        try:
            descriptor = os.open(
                temporary,
                os.O_WRONLY
                | os.O_CREAT
                | os.O_EXCL
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0),
                0o600,
                dir_fd=directory_fd,
            )
            self._write_all(descriptor, encoded)
            os.fsync(descriptor)
            self._fault("after_manifest_file_fsync")
            os.close(descriptor)
            descriptor = -1
            try:
                self._rename_noreplace(directory_fd, temporary, filename)
                temporary = ""
            except FileExistsError:
                existing = self._read_regular_at(
                    directory_fd,
                    filename,
                    _MAX_MANIFEST_JSON_BYTES + 48,
                )
                if existing != encoded:
                    raise ManifestConflictError(
                        "immutable manifest filename has conflicting bytes"
                    )
                self._unlink_owned_temp(directory_fd, temporary)
                temporary = ""
            self._fault("after_manifest_noreplace")
            os.fsync(directory_fd)
            self._fault("after_manifest_directory_fsync")
            readback = self._read_regular_at(
                directory_fd,
                filename,
                _MAX_MANIFEST_JSON_BYTES + 48,
            )
            if readback != encoded or encode_manifest(decode_manifest(readback)) != encoded:
                raise ManifestStoreError("immutable manifest strict readback failed")
        except InjectedManifestCrash:
            preserve_temporary = True
            raise
        except OSError as error:
            raise ManifestStoreError("immutable manifest publication failed") from error
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            if temporary and not preserve_temporary:
                self._unlink_owned_temp(directory_fd, temporary)

    def _publish_sidecar_blob_locked(
        self,
        directory_fd: int,
        descriptor: SidecarDescriptor,
        encoded: bytes,
    ) -> None:
        parent_fd = -1
        temporary = self._new_temporary_name("sidecar")
        file_descriptor = -1
        preserve_temporary = False
        try:
            parent_fd, basename = self._open_relative_parent(
                directory_fd,
                descriptor.relative_path,
            )
            file_descriptor = os.open(
                temporary,
                os.O_WRONLY
                | os.O_CREAT
                | os.O_EXCL
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0),
                0o600,
                dir_fd=parent_fd,
            )
            self._write_all(file_descriptor, encoded)
            os.fsync(file_descriptor)
            self._fault("after_sidecar_file_fsync")
            os.close(file_descriptor)
            file_descriptor = -1
            try:
                self._rename_noreplace(parent_fd, temporary, basename)
                temporary = ""
            except FileExistsError:
                existing = self._read_regular_at(
                    directory_fd,
                    descriptor.relative_path,
                    _MAX_SIDECAR_JSON_BYTES + 48,
                )
                if existing != encoded:
                    raise ManifestConflictError(
                        "immutable sidecar path has conflicting bytes"
                    )
                self._unlink_owned_temp(parent_fd, temporary)
                temporary = ""
            self._fault("after_sidecar_noreplace")
            os.fsync(parent_fd)
            self._fault("after_sidecar_directory_fsync")
            readback = self._read_regular_at(
                directory_fd,
                descriptor.relative_path,
                _MAX_SIDECAR_JSON_BYTES + 48,
            )
            if (
                readback != encoded
                or encode_sidecar(decode_sidecar(readback)) != encoded
            ):
                raise ManifestStoreError("immutable sidecar strict readback failed")
        except InjectedManifestCrash:
            preserve_temporary = True
            raise
        except OSError as error:
            raise ManifestStoreError("immutable sidecar publication failed") from error
        finally:
            if file_descriptor >= 0:
                os.close(file_descriptor)
            if parent_fd >= 0:
                if temporary and not preserve_temporary:
                    self._unlink_owned_temp(parent_fd, temporary)
                os.close(parent_fd)

    def publish_sidecar(
        self,
        sidecar: WatermarkSidecar,
        *,
        relative_path: str,
    ) -> SidecarDescriptor:
        """Publish one immutable, strictly readable sidecar without replacement."""
        descriptor = SidecarDescriptor.from_sidecar(relative_path, sidecar)
        encoded = encode_sidecar(sidecar)
        directory_fd = self._open_directory()
        try:
            fcntl.flock(directory_fd, fcntl.LOCK_EX)
            self._publish_sidecar_blob_locked(
                directory_fd,
                descriptor,
                encoded,
            )
            return descriptor
        finally:
            try:
                fcntl.flock(directory_fd, fcntl.LOCK_UN)
            finally:
                os.close(directory_fd)

    def _validate_published_inputs_locked(
        self,
        directory_fd: int,
        manifest: HistoryManifest,
        sidecars: Mapping[SidecarNamespace, WatermarkSidecar],
    ) -> None:
        manifest.validate_sidecars(sidecars)
        for descriptor in manifest.sidecars:
            readback = self._read_regular_at(
                directory_fd,
                descriptor.relative_path,
                _MAX_SIDECAR_JSON_BYTES + 48,
            )
            if len(readback) != descriptor.file_size_bytes:
                raise ManifestStoreError("published sidecar size mismatch")
            if hashlib.sha256(readback).digest() != descriptor.file_sha256:
                raise ManifestStoreError("published sidecar SHA-256 mismatch")
            decoded = decode_sidecar(readback)
            expected = sidecars[descriptor.namespace]
            if (
                decoded != expected
                or decoded.namespace != descriptor.namespace
                or len(decoded.watermark_sets)
                != descriptor.watermark_set_count
                or encode_sidecar(decoded) != readback
            ):
                raise ManifestStoreError(
                    "published sidecar identity/readback mismatch"
                )
        for artifact in manifest.visible_artifacts:
            self._verify_regular_digest_at(
                directory_fd,
                artifact.relative_path,
                expected_size=artifact.file_size_bytes,
                expected_sha256=artifact.file_sha256,
                maximum_bytes=self._maximum_verified_artifact_bytes,
            )

    def _publish_current_locked(
        self,
        directory_fd: int,
        pointer: _CurrentPointer,
        expected_before: _CurrentPointer | None,
    ) -> None:
        encoded = _encode_current(pointer)
        temporary = self._new_temporary_name("CURRENT")
        descriptor = -1
        preserve_temporary = False
        try:
            descriptor = os.open(
                temporary,
                os.O_WRONLY
                | os.O_CREAT
                | os.O_EXCL
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0),
                0o600,
                dir_fd=directory_fd,
            )
            self._write_all(descriptor, encoded)
            os.fsync(descriptor)
            self._fault("after_current_file_fsync")
            os.close(descriptor)
            descriptor = -1
            current = self._load_current_locked(directory_fd)
            current_pointer = None if current is None else current[0]
            if current_pointer != expected_before:
                raise ManifestConflictError("CURRENT changed before CAS replacement")
            os.replace(
                temporary,
                _CURRENT_NAME,
                src_dir_fd=directory_fd,
                dst_dir_fd=directory_fd,
            )
            temporary = ""
            self._fault("after_current_replace")
            os.fsync(directory_fd)
            self._fault("after_current_directory_fsync")
            loaded = self._load_current_locked(directory_fd)
            if loaded is None or loaded[0] != pointer:
                raise ManifestStoreError("CURRENT strict readback failed")
        except InjectedManifestCrash:
            preserve_temporary = True
            raise
        except OSError as error:
            raise ManifestStoreError("CURRENT atomic publication failed") from error
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            if temporary and not preserve_temporary:
                self._unlink_owned_temp(directory_fd, temporary)

    def publish(
        self,
        manifest: HistoryManifest,
        *,
        expected_current_generation: int | None,
        expected_current_sha256: bytes | None,
        sidecars: Mapping[SidecarNamespace, WatermarkSidecar],
    ) -> bytes:
        if not isinstance(manifest, HistoryManifest):
            raise ManifestStoreError("manifest must be a HistoryManifest")
        try:
            frozen_sidecars = _freeze_sidecar_mapping(sidecars)
        except ManifestError as error:
            raise ManifestStoreError("sidecar mapping failed bounded freezing") from error
        expected = (
            None
            if expected_current_sha256 is None
            else _fixed_bytes(
                expected_current_sha256, 32, "expected_current_sha256"
            )
        )
        expected_generation = (
            None
            if expected_current_generation is None
            else _uint(
                expected_current_generation,
                64,
                "expected_current_generation",
                positive=True,
            )
        )
        if (expected_generation is None) != (expected is None):
            raise ManifestConflictError(
                "CURRENT CAS generation and SHA-256 must both be absent or present"
            )
        if manifest.generation == 1:
            if expected_generation is not None or expected is not None:
                raise ManifestConflictError(
                    "generation 1 requires an empty CURRENT expectation"
                )
        elif (
            expected_generation != manifest.generation - 1
            or expected != manifest.previous_manifest_sha256
        ):
            raise ManifestConflictError(
                "CURRENT expectation must name the candidate predecessor"
            )
        encoded = encode_manifest(manifest)
        digest = hashlib.sha256(encoded).digest()
        filename = _manifest_filename(manifest.generation, digest)
        pointer = _CurrentPointer(manifest.generation, filename, digest)
        directory_fd = self._open_directory()
        try:
            fcntl.flock(directory_fd, fcntl.LOCK_EX)
            self._validate_published_inputs_locked(
                directory_fd,
                manifest,
                frozen_sidecars,
            )
            current = self._load_current_locked(directory_fd)
            if current is not None and current[0] == pointer:
                if current[2] != encoded:
                    raise ManifestConflictError(
                        "CURRENT identity names different manifest bytes"
                    )
                # Complete a possible crash after CURRENT rename but before its
                # directory barrier, then prove strict readback again.
                os.fsync(directory_fd)
                verified = self._load_current_locked(directory_fd)
                if verified is None or verified[0] != pointer:
                    raise ManifestStoreError("idempotent CURRENT readback failed")
                return digest
            current_pointer = None if current is None else current[0]
            current_manifest = None if current is None else current[1]
            current_digest = None if current_pointer is None else current_pointer.manifest_sha256
            current_generation = (
                None if current_pointer is None else current_pointer.generation
            )
            if (
                current_generation != expected_generation
                or current_digest != expected
            ):
                raise ManifestConflictError("CURRENT compare-and-swap mismatch")
            if current_manifest is None:
                if manifest.generation != 1 or manifest.previous_manifest_sha256 is not None:
                    raise ManifestConflictError(
                        "an empty manifest store accepts only generation 1"
                    )
            else:
                manifest.validate_successor(current_manifest)
            self._publish_immutable_manifest(
                directory_fd,
                filename,
                encoded,
            )
            self._publish_current_locked(
                directory_fd,
                pointer,
                current_pointer,
            )
            return digest
        finally:
            try:
                fcntl.flock(directory_fd, fcntl.LOCK_UN)
            finally:
                os.close(directory_fd)

    def load_current(self) -> tuple[HistoryManifest, bytes] | None:
        directory_fd = self._open_directory()
        try:
            fcntl.flock(directory_fd, fcntl.LOCK_SH)
            current = self._load_current_locked(directory_fd)
            if current is None:
                return None
            return current[1], current[0].manifest_sha256
        finally:
            try:
                fcntl.flock(directory_fd, fcntl.LOCK_UN)
            finally:
                os.close(directory_fd)

    def load_current_with_sidecars(
        self,
    ) -> tuple[HistoryManifest, bytes, tuple[WatermarkSidecar, ...]] | None:
        """Load one CURRENT snapshot and every exact sidecar it describes.

        Relative paths are traversed from the already-open store dirfd without
        following symlinks.  Each returned sidecar has therefore passed the
        descriptor size/hash/count checks and a strict canonical-codec
        round-trip while the shared store lease was held.  Parquet artifacts
        are intentionally left to the descriptor-aware query readers, which
        validate their schemas and decoded rows in addition to whole-file
        identity.
        """

        directory_fd = self._open_directory()
        try:
            fcntl.flock(directory_fd, fcntl.LOCK_SH)
            current = self._load_current_locked(directory_fd)
            if current is None:
                return None
            pointer, manifest, _ = current
            sidecars: dict[SidecarNamespace, WatermarkSidecar] = {}
            for descriptor in manifest.sidecars:
                readback = self._read_regular_at(
                    directory_fd,
                    descriptor.relative_path,
                    _MAX_SIDECAR_JSON_BYTES + 48,
                )
                if len(readback) != descriptor.file_size_bytes:
                    raise ManifestStoreError(
                        "CURRENT sidecar size differs from its descriptor"
                    )
                if hashlib.sha256(readback).digest() != descriptor.file_sha256:
                    raise ManifestStoreError(
                        "CURRENT sidecar SHA-256 differs from its descriptor"
                    )
                decoded = decode_sidecar(readback)
                if (
                    decoded.namespace != descriptor.namespace
                    or len(decoded.watermark_sets)
                    != descriptor.watermark_set_count
                    or encode_sidecar(decoded) != readback
                ):
                    raise ManifestStoreError(
                        "CURRENT sidecar failed strict descriptor readback"
                    )
                sidecars[descriptor.namespace] = decoded
            manifest.validate_sidecars(sidecars)
            ordered = tuple(
                sidecars[descriptor.namespace]
                for descriptor in manifest.sidecars
            )
            return manifest, pointer.manifest_sha256, ordered
        finally:
            try:
                fcntl.flock(directory_fd, fcntl.LOCK_UN)
            finally:
                os.close(directory_fd)
