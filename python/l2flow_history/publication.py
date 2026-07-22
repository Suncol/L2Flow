"""Checked assembly from a Parquet backend readback to a manifest artifact.

The boundary is deliberately narrow: the public API accepts a store path,
basename, and expected descriptor, then obtains ``ParquetReadResult`` itself
through the backend's final-file reader.  Callers cannot inject arbitrary file
bytes or an unchecked public ``ParquetReadResult``.  This module recomputes
every fact derivable from the decoded rows and carries the backend's whole-file
identity into :class:`ArtifactEntry`.

Some receipt facts are not present in archive rows.  In particular, rows do
not prove the configured shard, Raw/Canonical generations, writer instance,
normalizer identity, or that a common-cut certificate is healthy and
route-complete.  Those facts must arrive in ``UpstreamValidatedReceipts``.
The evidence hash in that wrapper is a caller-provided locator/identity; this
module does not interpret it as a certificate signature or validate health.
"""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from dataclasses import dataclass, field
import hashlib
import json
import os
import re
from types import MappingProxyType
from typing import Mapping

from l2flow_factor import FactorInputWatermark, FactorInputWatermarkSet, InputFamily

from .manifest import (
    ArtifactEntry,
    ArtifactVisibility,
    DatasetKind,
    ManifestError,
    PartitionSpec,
    SidecarNamespace,
    SidecarReference,
    SourceRangeReceipt,
)
from .model import (
    CANONICAL_PARQUET_SCHEMA_NAME_V1,
    FACTOR_PARQUET_SCHEMA_NAME_V1,
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
)
from .parquet_backend import (
    ParquetReadResult,
    read_canonical_parquet_part,
    read_factor_parquet_part,
)
from .watermark_sidecar import (
    SidecarError,
    WatermarkSidecar,
)


class PublicationError(ValueError):
    """A backend readback cannot truthfully become a manifest artifact."""


class PublicationValidationError(PublicationError):
    """Decoded rows, descriptor, partition, receipts, or lineage disagree."""


_MAX_PUBLICATION_SIDECARS_V1 = 65_536
_MAX_PUBLICATION_RECEIPTS_V1 = 65_536


def _fixed_hash(value: object, name: str) -> bytes:
    if not isinstance(value, (bytes, bytearray, memoryview)):
        raise PublicationValidationError(f"{name} must be bytes-like")
    result = bytes(value)
    if len(result) != 32 or not any(result):
        raise PublicationValidationError(f"{name} must be exactly 32 nonzero bytes")
    return result


@dataclass(frozen=True, slots=True)
class UpstreamValidatedReceipts:
    """Receipts whose non-row facts were validated by an upstream authority.

    Construction is an explicit trust-boundary marker, not proof validation.
    For a VISIBLE artifact, ``validation_evidence_sha256`` is the identity of
    the caller-validated common-cut certificate and must equal the certificate
    identity persisted by every receipt.  This module does not treat that hash
    as a signature or locally prove route completeness/generation health.  For
    STAGING, the evidence identity is an assembly-time audit input and is not
    claimed to be persisted when the receipts carry no certificate.
    """

    receipts: tuple[SourceRangeReceipt, ...]
    validation_evidence_sha256: bytes

    def __post_init__(self) -> None:
        if (
            type(self.receipts) is not tuple
            or not self.receipts
            or len(self.receipts) > _MAX_PUBLICATION_RECEIPTS_V1
        ):
            raise PublicationValidationError(
                "upstream-validated receipts must be a nonempty bounded tuple"
            )
        if any(
            not isinstance(receipt, SourceRangeReceipt)
            for receipt in self.receipts
        ):
            raise PublicationValidationError(
                "upstream-validated receipts contain an invalid value"
            )
        object.__setattr__(
            self,
            "validation_evidence_sha256",
            _fixed_hash(
                self.validation_evidence_sha256,
                "validation_evidence_sha256",
            ),
        )


@dataclass(frozen=True, slots=True)
class UpstreamValidatedFactorGroup:
    """Explicit trust boundary for a factor part's group assignment.

    ``FactorHistoryRow`` does not carry ``factor_group``.  The persisted digest
    binds the partition label and row factor identity to a caller-validated
    catalog evidence identity; that hash is not treated as a signature.
    """

    factor_group: str
    factor_id: str
    factor_version: str
    factor_config_sha256: bytes
    factor_code_sha256: bytes
    state_schema_version: int
    state_schema_sha256: bytes
    implementation_status: FactorImplementationStatus
    numeric_dtype: str
    validation_evidence_sha256: bytes
    membership_sha256: bytes = field(init=False)

    def __post_init__(self) -> None:
        if (
            type(self.factor_group) is not str
            or len(self.factor_group) > 128
            or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._+-]*", self.factor_group)
            is None
        ):
            raise PublicationValidationError("factor_group is not a bounded safe token")
        if (
            type(self.factor_id) is not str
            or re.fullmatch(r"[a-z][a-z0-9_]{0,127}", self.factor_id) is None
        ):
            raise PublicationValidationError("factor_id is invalid")
        if (
            type(self.factor_version) is not str
            or re.fullmatch(
                r"[0-9A-Za-z][0-9A-Za-z._+-]{0,63}",
                self.factor_version,
            )
            is None
        ):
            raise PublicationValidationError("factor_version is invalid")
        config_sha256 = _fixed_hash(
            self.factor_config_sha256,
            "factor_config_sha256",
        )
        code_sha256 = _fixed_hash(
            self.factor_code_sha256,
            "factor_code_sha256",
        )
        state_sha256 = _fixed_hash(
            self.state_schema_sha256,
            "state_schema_sha256",
        )
        if (
            type(self.state_schema_version) is not int
            or self.state_schema_version < 1
            or self.state_schema_version > (1 << 32) - 1
        ):
            raise PublicationValidationError(
                "state_schema_version must be a positive uint32"
            )
        if type(self.implementation_status) is not FactorImplementationStatus:
            raise PublicationValidationError(
                "implementation_status must be exact FactorImplementationStatus"
            )
        if self.numeric_dtype != "float64":
            raise PublicationValidationError("factor catalog numeric_dtype must be float64")
        object.__setattr__(self, "factor_config_sha256", config_sha256)
        object.__setattr__(self, "factor_code_sha256", code_sha256)
        object.__setattr__(self, "state_schema_sha256", state_sha256)
        evidence = _fixed_hash(
            self.validation_evidence_sha256,
            "factor group validation_evidence_sha256",
        )
        object.__setattr__(self, "validation_evidence_sha256", evidence)
        payload = json.dumps(
            {
                "factor_group": self.factor_group,
                "factor_id": self.factor_id,
                "factor_version": self.factor_version,
                "factor_config_sha256": config_sha256.hex(),
                "factor_code_sha256": code_sha256.hex(),
                "state_schema_version": self.state_schema_version,
                "state_schema_sha256": state_sha256.hex(),
                "implementation_status": self.implementation_status.value,
                "numeric_dtype": self.numeric_dtype,
                "validation_evidence_sha256": evidence.hex(),
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("ascii")
        object.__setattr__(
            self,
            "membership_sha256",
            hashlib.sha256(
                b"l2flow.factor-group-membership.v1\x00" + payload
            ).digest(),
        )


def _validated_readback(
    readback: object,
    *,
    expected_descriptor: ParquetPartDescriptor,
    partition: PartitionSpec,
    dataset: DatasetKind,
    schema_name: str,
    row_type: type[CanonicalArchiveRow] | type[FactorHistoryRow],
) -> tuple[
    tuple[CanonicalArchiveRow, ...] | tuple[FactorHistoryRow, ...],
    ParquetPartDescriptor,
]:
    if type(readback) is not ParquetReadResult:
        raise PublicationValidationError(
            "publication requires an exact ParquetReadResult backend readback"
        )
    if not isinstance(partition, PartitionSpec) or partition.dataset is not dataset:
        raise PublicationValidationError("partition dataset disagrees with publication type")
    descriptor = readback.descriptor
    if type(descriptor) is not ParquetPartDescriptor:
        raise PublicationValidationError(
            "readback descriptor must be an exact ParquetPartDescriptor"
        )
    if descriptor.schema_name != schema_name:
        raise PublicationValidationError("Parquet descriptor schema_name mismatch")
    if descriptor != expected_descriptor:
        raise PublicationValidationError(
            "backend readback descriptor differs from the expected descriptor"
        )
    rows = readback.rows
    if type(rows) is not tuple or not rows:
        raise PublicationValidationError("Parquet readback rows must be a nonempty tuple")
    if any(type(row) is not row_type for row in rows):
        raise PublicationValidationError("Parquet readback contains the wrong row type")
    if descriptor.row_count != len(rows):
        raise PublicationValidationError("Parquet descriptor row_count mismatch")
    if descriptor.bucket != partition.bucket:
        raise PublicationValidationError("Parquet descriptor/partition bucket mismatch")
    return rows, descriptor


def _validated_receipts(value: object) -> UpstreamValidatedReceipts:
    if type(value) is not UpstreamValidatedReceipts:
        raise PublicationValidationError(
            "caller must supply explicit UpstreamValidatedReceipts"
        )
    return value


_CANONICAL_MARKET = {
    0: "GLOBAL",
    1: "SH",
    2: "SZ",
}


def _receipt_covers_canonical_row(
    receipt: SourceRangeReceipt,
    row: CanonicalArchiveRow,
) -> bool:
    namespace = receipt.namespace
    header = row.header
    return (
        namespace.trade_date == header.trade_date
        and namespace.source_stream_id == header.source_stream_id
        and namespace.origin_capture_date == row.origin_capture_date
        and namespace.origin_stream_day_id == row.origin_stream_day_id
        and namespace.family.value == header.event_family
        and receipt.first_origin_ingress_sequence
        <= header.origin_ingress_sequence
        <= receipt.last_origin_ingress_sequence
        and receipt.min_origin_wal_end_pos
        <= header.origin_wal_end_pos
        <= receipt.max_origin_wal_end_pos
    )


def _canonical_receipt_route(namespace: SourceNamespace) -> tuple[object, ...]:
    """The exact source-route fields carried by a Canonical archive row."""

    return (
        namespace.trade_date,
        namespace.origin_capture_date,
        namespace.source_stream_id,
        namespace.origin_stream_day_id,
        namespace.family,
    )


def _canonical_row_route(row: CanonicalArchiveRow) -> tuple[object, ...]:
    return (
        row.header.trade_date,
        row.origin_capture_date,
        row.header.source_stream_id,
        row.origin_stream_day_id,
        InputFamily(row.header.event_family),
    )


@dataclass(frozen=True, slots=True)
class _IndexedReceipt:
    index: int
    receipt: SourceRangeReceipt


@dataclass(frozen=True, slots=True)
class _CanonicalIngressRanges:
    """One projected route, indexed by inclusive ingress ranges.

    Prefix maxima let a lookup stop as soon as no earlier interval can contain
    the requested ingress sequence.  Only those candidates are checked against
    the WAL range, rather than rescanning every receipt in the publication.
    """

    receipts: tuple[_IndexedReceipt, ...]
    first_ingress: tuple[int, ...]
    prefix_max_last_ingress: tuple[int, ...]

    @classmethod
    def build(cls, values: list[_IndexedReceipt]) -> "_CanonicalIngressRanges":
        ordered = tuple(
            sorted(
                values,
                key=lambda item: (
                    item.receipt.first_origin_ingress_sequence,
                    item.receipt.last_origin_ingress_sequence,
                    item.receipt.min_origin_wal_end_pos,
                    item.receipt.max_origin_wal_end_pos,
                    item.receipt.namespace.sort_key,
                    item.receipt.begin_canonical_cursor,
                    item.index,
                ),
            )
        )
        starts: list[int] = []
        prefix: list[int] = []
        maximum = 0
        for item in ordered:
            starts.append(item.receipt.first_origin_ingress_sequence)
            maximum = max(maximum, item.receipt.last_origin_ingress_sequence)
            prefix.append(maximum)
        return cls(ordered, tuple(starts), tuple(prefix))

    def matching_indices(self, row: CanonicalArchiveRow) -> tuple[int, ...]:
        ingress = row.header.origin_ingress_sequence
        index = bisect_right(self.first_ingress, ingress) - 1
        matches: list[int] = []
        while index >= 0:
            if self.prefix_max_last_ingress[index] < ingress:
                break
            item = self.receipts[index]
            if _receipt_covers_canonical_row(item.receipt, row):
                matches.append(item.index)
                if len(matches) == 2:
                    break
            index -= 1
        return tuple(matches)


@dataclass(frozen=True, slots=True)
class _CanonicalReceiptIndex:
    routes: Mapping[tuple[object, ...], _CanonicalIngressRanges]

    @classmethod
    def build(cls, receipts: tuple[SourceRangeReceipt, ...]) -> "_CanonicalReceiptIndex":
        grouped: dict[tuple[object, ...], list[_IndexedReceipt]] = {}
        for index, receipt in enumerate(receipts):
            grouped.setdefault(
                _canonical_receipt_route(receipt.namespace), []
            ).append(_IndexedReceipt(index, receipt))
        return cls(
            MappingProxyType(
                {
                    route: _CanonicalIngressRanges.build(values)
                    for route, values in grouped.items()
                }
            )
        )

    def matching_indices(self, row: CanonicalArchiveRow) -> tuple[int, ...]:
        ranges = self.routes.get(_canonical_row_route(row))
        return () if ranges is None else ranges.matching_indices(row)


def _assemble(
    *,
    descriptor: ParquetPartDescriptor,
    partition: PartitionSpec,
    receipts: UpstreamValidatedReceipts,
    visibility: ArtifactVisibility,
    artifact_type: DatasetKind,
    factor_group_membership_sha256: bytes | None = None,
    sidecar_references: tuple[SidecarReference, ...] = (),
) -> ArtifactEntry:
    if not isinstance(visibility, ArtifactVisibility):
        raise PublicationValidationError(
            "visibility must be an ArtifactVisibility"
        )
    if visibility is ArtifactVisibility.VISIBLE:
        certificates = {
            receipt.coverage_certificate_sha256
            for receipt in receipts.receipts
        }
        if certificates != {receipts.validation_evidence_sha256}:
            raise PublicationValidationError(
                "visible receipts must persist the caller-validated common-cut identity"
            )
    try:
        return ArtifactEntry(
            artifact_type=artifact_type,
            relative_path=partition.relative_prefix + descriptor.basename,
            file_sha256=descriptor.file_sha256,
            file_size_bytes=descriptor.byte_size,
            row_count=descriptor.row_count,
            logical_content_sha256=descriptor.logical_rows_sha256,
            min_sort_key_v1=descriptor.min_sort_key_v1,
            max_sort_key_v1=descriptor.max_sort_key_v1,
            partition=partition,
            source_receipts=receipts.receipts,
            factor_group_membership_sha256=factor_group_membership_sha256,
            factor_sidecar_refs=sidecar_references,
            visibility=visibility,
        )
    except (ManifestError, SidecarError) as error:
        raise PublicationValidationError(
            "validated readback cannot satisfy the manifest artifact contract"
        ) from error


def assemble_canonical_artifact(
    directory: str | os.PathLike[str],
    basename: str,
    *,
    expected_descriptor: ParquetPartDescriptor,
    partition: PartitionSpec,
    upstream_receipts: UpstreamValidatedReceipts,
    visibility: ArtifactVisibility = ArtifactVisibility.STAGING,
) -> ArtifactEntry:
    """Cross-check a Canonical final-file readback and build its artifact entry.

    Each row must have exactly one receipt matching the fields the row proves:
    trade/source/capture/stream-day/family plus inclusive origin ingress and
    origin WAL-end coverage.  ``shard_event_id`` is deliberately not used as a
    WAL cursor (or as the receipt's array-position Canonical cursor).
    """

    if type(expected_descriptor) is not ParquetPartDescriptor:
        raise PublicationValidationError(
            "expected_descriptor must be an exact ParquetPartDescriptor"
        )
    if expected_descriptor.basename != basename:
        raise PublicationValidationError("basename/expected descriptor mismatch")
    readback = read_canonical_parquet_part(
        directory,
        basename,
        expected_descriptor=expected_descriptor,
    )
    generic_rows, descriptor = _validated_readback(
        readback,
        expected_descriptor=expected_descriptor,
        partition=partition,
        dataset=DatasetKind.CANONICAL,
        schema_name=CANONICAL_PARQUET_SCHEMA_NAME_V1,
        row_type=CanonicalArchiveRow,
    )
    receipts = _validated_receipts(upstream_receipts)
    rows = tuple(generic_rows)
    assert all(type(row) is CanonicalArchiveRow for row in rows)
    canonical_rows = tuple(row for row in rows if type(row) is CanonicalArchiveRow)
    try:
        if canonical_sort_and_dedupe(canonical_rows) != canonical_rows:
            raise PublicationValidationError(
                "Canonical readback is not in unique frozen total order"
            )
        logical_hash = canonical_logical_rows_sha256(canonical_rows)
    except HistoryValidationError as error:
        raise PublicationValidationError("Canonical row set is invalid") from error
    if descriptor.logical_rows_sha256 != logical_hash:
        raise PublicationValidationError("Canonical logical row hash mismatch")
    if (
        descriptor.min_sort_key_v1
        != canonical_sort_key_wire_v1(canonical_rows[0])
        or descriptor.max_sort_key_v1
        != canonical_sort_key_wire_v1(canonical_rows[-1])
    ):
        raise PublicationValidationError("Canonical descriptor sort bounds mismatch")

    receipt_index = _CanonicalReceiptIndex.build(receipts.receipts)
    matched_receipts: set[int] = set()
    for row in canonical_rows:
        header = row.header
        if header.trade_date != partition.trade_date:
            raise PublicationValidationError("Canonical row trade_date mismatch")
        if header.event_family != partition.event_type:
            raise PublicationValidationError("Canonical row event family mismatch")
        if _CANONICAL_MARKET[header.market] != partition.market:
            raise PublicationValidationError("Canonical row market mismatch")
        if row.bucket != partition.bucket or row.bucket != descriptor.bucket:
            raise PublicationValidationError("Canonical row bucket mismatch")
        matches = receipt_index.matching_indices(row)
        if len(matches) != 1:
            raise PublicationValidationError(
                "Canonical row lacks one unique provable receipt coverage"
            )
        matched_receipts.add(matches[0])
    if matched_receipts != set(range(len(receipts.receipts))):
        raise PublicationValidationError(
            "Canonical publication contains an unrelated source receipt"
        )

    return _assemble(
        descriptor=descriptor,
        partition=partition,
        receipts=receipts,
        visibility=visibility,
        artifact_type=DatasetKind.CANONICAL,
    )


def _validated_sidecar_mapping(
    value: object,
) -> dict[SidecarNamespace, WatermarkSidecar]:
    if not isinstance(value, Mapping):
        raise PublicationValidationError("sidecars must be a mapping")
    try:
        iterator = iter(value.items())
    except (TypeError, RuntimeError) as error:
        raise PublicationValidationError(
            "sidecar mapping could not be frozen"
        ) from error
    frozen: dict[SidecarNamespace, WatermarkSidecar] = {}
    try:
        for item in iterator:
            if len(frozen) >= _MAX_PUBLICATION_SIDECARS_V1:
                raise PublicationValidationError(
                    "sidecar mapping exceeds the V1 bound"
                )
            if type(item) is not tuple or len(item) != 2:
                raise PublicationValidationError(
                    "sidecar mapping yielded a malformed item"
                )
            namespace, sidecar = item
            if not isinstance(namespace, SidecarNamespace) or not isinstance(
                sidecar,
                WatermarkSidecar,
            ):
                raise PublicationValidationError("sidecar mapping is not typed")
            if sidecar.namespace != namespace:
                raise PublicationValidationError(
                    "sidecar mapping namespace mismatch"
                )
            if namespace in frozen:
                raise PublicationValidationError(
                    "sidecar mapping repeated one namespace while being frozen"
                )
            frozen[namespace] = sidecar
    except (TypeError, RuntimeError) as error:
        raise PublicationValidationError(
            "sidecar mapping changed while being frozen"
        ) from error
    return frozen


def _receipt_covers_watermark_entry(
    receipt: SourceRangeReceipt,
    row: FactorHistoryRow,
    watermark: FactorInputWatermarkSet,
    entry: FactorInputWatermark,
) -> bool:
    namespace = receipt.namespace
    # canonical_cursor is an exclusive-next boundary: begin < cursor <= end.
    # Equality with begin consumed none of this receipt; equality with end
    # consumed its complete range.  The cursor is not a WAL byte position.
    return (
        namespace.trade_date == watermark.trade_date
        and namespace.source_stream_id == entry.source_stream_id
        and namespace.origin_capture_date == entry.origin_capture_date
        and namespace.origin_stream_day_id == entry.origin_stream_day_id
        and namespace.family is entry.family
        and namespace.shard_id == entry.shard_id
        and namespace.clock_epoch_algorithm == entry.clock_epoch_algorithm
        and namespace.clock_epoch_digest == entry.clock_epoch_digest
        and namespace.registry_version == row.registry_version
        and namespace.registry_sha256 == row.registry_sha256
        and receipt.begin_canonical_cursor
        < entry.canonical_cursor
        <= receipt.end_canonical_cursor
        and receipt.min_origin_wal_end_pos
        <= entry.max_consumed_origin_wal_end_pos
        <= receipt.max_origin_wal_end_pos
    )


def _factor_receipt_route(namespace: SourceNamespace) -> tuple[object, ...]:
    """Projected route persisted by one factor watermark entry.

    Registry identity is deliberately not part of this key.  It is checked
    against the selected complete namespace after the route has proved unique,
    matching the fixed-snapshot query resolver's ambiguity rule.
    """

    return (
        namespace.trade_date,
        namespace.source_stream_id,
        namespace.origin_capture_date,
        namespace.origin_stream_day_id,
        namespace.family,
        namespace.shard_id,
        namespace.clock_epoch_algorithm,
        namespace.clock_epoch_digest,
    )


def _factor_entry_route(
    watermark: FactorInputWatermarkSet,
    entry: FactorInputWatermark,
) -> tuple[object, ...]:
    return (
        watermark.trade_date,
        entry.source_stream_id,
        entry.origin_capture_date,
        entry.origin_stream_day_id,
        entry.family,
        entry.shard_id,
        entry.clock_epoch_algorithm,
        entry.clock_epoch_digest,
    )


@dataclass(frozen=True, slots=True)
class _FactorCursorRanges:
    namespace: SourceNamespace
    receipts: tuple[_IndexedReceipt, ...]
    begin_cursors: tuple[int, ...]
    route_identity_receipt_indices: tuple[int, ...]

    @classmethod
    def build(
        cls,
        namespace: SourceNamespace,
        values: list[_IndexedReceipt],
    ) -> "_FactorCursorRanges":
        ordered = tuple(
            sorted(
                values,
                key=lambda item: (
                    item.receipt.begin_canonical_cursor,
                    item.receipt.end_canonical_cursor,
                    item.receipt.min_origin_wal_end_pos,
                    item.receipt.max_origin_wal_end_pos,
                    item.index,
                ),
            )
        )
        previous: SourceRangeReceipt | None = None
        for item in ordered:
            receipt = item.receipt
            if receipt.namespace != namespace:
                raise PublicationValidationError(
                    "factor receipt index mixed complete SourceNamespace values"
                )
            if (
                previous is not None
                and receipt.begin_canonical_cursor
                < previous.end_canonical_cursor
            ):
                raise PublicationValidationError(
                    "factor receipts overlap inside one complete SourceNamespace"
                )
            previous = receipt
        return cls(
            namespace=namespace,
            receipts=ordered,
            begin_cursors=tuple(
                item.receipt.begin_canonical_cursor for item in ordered
            ),
            route_identity_receipt_indices=tuple(
                sorted(item.index for item in ordered)
            ),
        )

    def matching_indices(
        self,
        row: FactorHistoryRow,
        watermark: FactorInputWatermarkSet,
        entry: FactorInputWatermark,
    ) -> tuple[int, ...]:
        namespace = self.namespace
        if (
            namespace.registry_version != row.registry_version
            or namespace.registry_sha256 != row.registry_sha256
        ):
            return ()
        zero_cursor = entry.canonical_cursor == 0
        zero_wal = entry.max_consumed_origin_wal_end_pos == 0
        if zero_cursor != zero_wal:
            raise PublicationValidationError(
                "zero Canonical cursor and zero consumed WAL must agree"
            )
        if zero_cursor:
            # No positive range is claimed.  Every receipt under the one exact
            # complete namespace is used only as route-identity evidence.
            return self.route_identity_receipt_indices

        index = bisect_left(self.begin_cursors, entry.canonical_cursor) - 1
        if index < 0:
            return ()
        item = self.receipts[index]
        if not _receipt_covers_watermark_entry(
            item.receipt,
            row,
            watermark,
            entry,
        ):
            return ()
        return (item.index,)


@dataclass(frozen=True, slots=True)
class _FactorReceiptIndex:
    routes: Mapping[tuple[object, ...], _FactorCursorRanges]

    @classmethod
    def build(cls, receipts: tuple[SourceRangeReceipt, ...]) -> "_FactorReceiptIndex":
        grouped: dict[
            tuple[object, ...],
            dict[SourceNamespace, list[_IndexedReceipt]],
        ] = {}
        for index, receipt in enumerate(receipts):
            by_namespace = grouped.setdefault(
                _factor_receipt_route(receipt.namespace), {}
            )
            by_namespace.setdefault(receipt.namespace, []).append(
                _IndexedReceipt(index, receipt)
            )

        routes: dict[tuple[object, ...], _FactorCursorRanges] = {}
        for route, by_namespace in grouped.items():
            if len(by_namespace) != 1:
                raise PublicationValidationError(
                    "factor projected route must name exactly one complete "
                    "SourceNamespace"
                )
            namespace, values = next(iter(by_namespace.items()))
            routes[route] = _FactorCursorRanges.build(namespace, values)
        return cls(MappingProxyType(routes))

    def matching_indices(
        self,
        row: FactorHistoryRow,
        watermark: FactorInputWatermarkSet,
        entry: FactorInputWatermark,
    ) -> tuple[int, ...]:
        ranges = self.routes.get(_factor_entry_route(watermark, entry))
        return () if ranges is None else ranges.matching_indices(
            row,
            watermark,
            entry,
        )


def _resolve_factor_reference(
    sidecar: WatermarkSidecar,
    watermark_set_id: int,
) -> tuple[SidecarReference, FactorInputWatermarkSet]:
    """Resolve and hash-check one persistent sidecar reference exactly once."""

    reference = sidecar.reference_for(watermark_set_id)
    return reference, sidecar.resolve(reference)


def _validate_factor_watermark_coverage(
    watermark: FactorInputWatermarkSet,
    row: FactorHistoryRow,
    receipt_index: _FactorReceiptIndex,
) -> tuple[int, ...]:
    """Validate one full map and return every receipt used by its entries."""

    quality_flags = 0
    matched_receipts: set[int] = set()
    for entry in watermark.entries:
        quality_flags |= entry.input_quality_flags
        if (
            entry.clock_epoch_algorithm != row.clock_epoch_algorithm
            or entry.clock_epoch_digest != row.clock_epoch_digest
        ):
            raise PublicationValidationError(
                "Factor row clock identity differs from its watermark map"
            )
        matches = receipt_index.matching_indices(row, watermark, entry)
        zero_consumption = (
            entry.canonical_cursor == 0
            and entry.max_consumed_origin_wal_end_pos == 0
        )
        if (not matches) or (not zero_consumption and len(matches) != 1):
            raise PublicationValidationError(
                "watermark entry lacks one unique provable source route/receipt"
            )
        matched_receipts.update(matches)
    if quality_flags != row.input_quality_flags:
        raise PublicationValidationError(
            "Factor row input quality is not the watermark-entry bitwise OR"
        )
    return tuple(sorted(matched_receipts))


def assemble_factor_artifact(
    directory: str | os.PathLike[str],
    basename: str,
    *,
    expected_descriptor: ParquetPartDescriptor,
    partition: PartitionSpec,
    upstream_receipts: UpstreamValidatedReceipts,
    sidecars: Mapping[SidecarNamespace, WatermarkSidecar],
    factor_group_membership: UpstreamValidatedFactorGroup,
    visibility: ArtifactVisibility = ArtifactVisibility.STAGING,
) -> ArtifactEntry:
    """Cross-check a Factor readback and resolve every persistent sidecar ref."""

    if type(expected_descriptor) is not ParquetPartDescriptor:
        raise PublicationValidationError(
            "expected_descriptor must be an exact ParquetPartDescriptor"
        )
    if expected_descriptor.basename != basename:
        raise PublicationValidationError("basename/expected descriptor mismatch")
    readback = read_factor_parquet_part(
        directory,
        basename,
        expected_descriptor=expected_descriptor,
    )
    generic_rows, descriptor = _validated_readback(
        readback,
        expected_descriptor=expected_descriptor,
        partition=partition,
        dataset=DatasetKind.FACTOR,
        schema_name=FACTOR_PARQUET_SCHEMA_NAME_V1,
        row_type=FactorHistoryRow,
    )
    receipts = _validated_receipts(upstream_receipts)
    if type(factor_group_membership) is not UpstreamValidatedFactorGroup:
        raise PublicationValidationError(
            "factor publication requires explicit group-membership evidence"
        )
    sidecar_mapping = _validated_sidecar_mapping(sidecars)
    rows = tuple(generic_rows)
    assert all(type(row) is FactorHistoryRow for row in rows)
    factor_rows = tuple(row for row in rows if type(row) is FactorHistoryRow)
    try:
        if factor_sort_and_dedupe(factor_rows) != factor_rows:
            raise PublicationValidationError(
                "Factor readback is not in unique frozen total order"
            )
        logical_hash = factor_logical_rows_sha256(factor_rows)
    except HistoryValidationError as error:
        raise PublicationValidationError("Factor row set is invalid") from error
    if descriptor.logical_rows_sha256 != logical_hash:
        raise PublicationValidationError("Factor logical row hash mismatch")
    if (
        descriptor.min_sort_key_v1 != factor_sort_key_wire_v1(factor_rows[0])
        or descriptor.max_sort_key_v1 != factor_sort_key_wire_v1(factor_rows[-1])
    ):
        raise PublicationValidationError("Factor descriptor sort bounds mismatch")

    receipt_index = _FactorReceiptIndex.build(receipts.receipts)
    references: dict[tuple[bytes, int, int], SidecarReference] = {}
    resolved_watermarks: dict[
        tuple[SidecarNamespace, int],
        tuple[SidecarReference, FactorInputWatermarkSet],
    ] = {}
    coverage_cache: dict[tuple[object, ...], tuple[int, ...]] = {}
    matched_receipts: set[int] = set()
    for row in factor_rows:
        if row.trade_date != partition.trade_date:
            raise PublicationValidationError("Factor row trade_date mismatch")
        if row.factor_version != partition.factor_version:
            raise PublicationValidationError("Factor row factor_version mismatch")
        if (
            partition.factor_group != factor_group_membership.factor_group
            or row.factor_id != factor_group_membership.factor_id
            or row.factor_version != factor_group_membership.factor_version
            or row.factor_config_sha256
            != factor_group_membership.factor_config_sha256
            or row.factor_code_sha256
            != factor_group_membership.factor_code_sha256
            or row.state_schema_version
            != factor_group_membership.state_schema_version
            or row.state_schema_sha256
            != factor_group_membership.state_schema_sha256
            or row.implementation_status
            is not factor_group_membership.implementation_status
            or row.numeric_dtype != factor_group_membership.numeric_dtype
        ):
            raise PublicationValidationError(
                "Factor row/partition differs from validated group membership"
            )
        if row.bucket != partition.bucket or row.bucket != descriptor.bucket:
            raise PublicationValidationError("Factor row bucket mismatch")
        namespace = SidecarNamespace(
            run_id=row.run_id,
            table_generation=row.watermark_table_generation,
        )
        sidecar = sidecar_mapping.get(namespace)
        if sidecar is None:
            raise PublicationValidationError("Factor row has an orphan sidecar namespace")
        resolved_key = (namespace, row.watermark_set_id)
        resolved = resolved_watermarks.get(resolved_key)
        if resolved is None:
            try:
                resolved = _resolve_factor_reference(
                    sidecar,
                    row.watermark_set_id,
                )
            except SidecarError as error:
                raise PublicationValidationError(
                    "Factor row sidecar reference cannot be fully resolved"
                ) from error
            resolved_watermarks[resolved_key] = resolved
        reference, watermark = resolved
        if reference.input_identity_sha256 != row.input_identity_sha256:
            raise PublicationValidationError("Factor row input identity mismatch")
        if watermark.trade_date != row.trade_date:
            raise PublicationValidationError("Factor row watermark trade_date mismatch")
        coverage_key = (
            reference,
            row.trade_date,
            row.registry_version,
            row.registry_sha256,
            row.clock_epoch_algorithm,
            row.clock_epoch_digest,
            row.input_quality_flags,
        )
        coverage = coverage_cache.get(coverage_key)
        if coverage is None:
            coverage = _validate_factor_watermark_coverage(
                watermark,
                row,
                receipt_index,
            )
            coverage_cache[coverage_key] = coverage
        matched_receipts.update(coverage)
        previous = references.get(reference.sort_key)
        if previous is not None and previous != reference:
            raise PublicationValidationError(
                "one persistent sidecar key resolved to conflicting maps"
            )
        references[reference.sort_key] = reference

    if matched_receipts != set(range(len(receipts.receipts))):
        raise PublicationValidationError(
            "Factor publication contains an unrelated source receipt"
        )

    return _assemble(
        descriptor=descriptor,
        partition=partition,
        receipts=receipts,
        visibility=visibility,
        artifact_type=DatasetKind.FACTOR,
        factor_group_membership_sha256=(
            factor_group_membership.membership_sha256
        ),
        sidecar_references=tuple(references.values()),
    )


__all__ = [
    "PublicationError",
    "PublicationValidationError",
    "UpstreamValidatedFactorGroup",
    "UpstreamValidatedReceipts",
    "assemble_canonical_artifact",
    "assemble_factor_artifact",
]
