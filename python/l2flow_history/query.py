"""Bounded fixed-snapshot Phase-8 hot/cold queries.

Cold source authority comes only from actual ``HistoryManifest`` objects and
their visible ``ArtifactEntry`` values.  Watermark authority comes only from
the manifest module's descriptors and the canonical watermark-sidecar codec.
Unpublished hot/current sources must be supplied as typed source snapshots
whose content is bound to the fixed hot/current digest.

The supported public cold-read functions perform the real descriptor-checked
backend read.  As with any Python underscore API, private constructors and
module tokens are an in-process trust boundary, not an unforgeable security
boundary; callers that bypass the public reader own that validation.

Canonical ``SourceRangeReceipt`` minima/maxima are locator and coverage
evidence.  They do *not* prove that this module read or validated a Raw record;
an external Raw reader must do byte-level confirmation when that is required.
"""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from collections.abc import Mapping
from dataclasses import InitVar, dataclass, field
from enum import Enum
import hashlib
import json
import os
import re
import stat
import time
from types import MappingProxyType
from typing import Callable, Iterable, Iterator, Union

from l2flow_factor.canonical import InputFamily
from l2flow_factor.watermark import FactorInputWatermark, FactorInputWatermarkSet

from .manifest import (
    ArtifactEntry,
    DatasetKind,
    HistoryManifest,
    SourceNamespace,
    SourceRangeReceipt,
    manifest_sha256,
)
from .model import (
    CANONICAL_RECORD_SIZE_BY_EVENT_TYPE_V1,
    MAX_FACTOR_TEXT_BYTES_V1,
    CanonicalArchiveRow,
    FactorHistoryRow,
)
from .watermark_sidecar import (
    SidecarNamespace,
    SidecarReference,
    WatermarkSidecar,
    encode_sidecar,
)


MAX_QUERY_ROWS = 1_000_000
MAX_QUERY_RESULT_ROWS = 1_000_000
MAX_QUERY_SOURCES = 65_536
MAX_QUERY_BYTES = 1 << 40
MAX_QUERY_LINEAGE_ENTRIES = 3_000_000
MAX_QUERY_LINEAGE_BYTES = 1 << 40
MAX_QUERY_LINEAGE_WORK = 10_000_000
MAX_ROW_BYTES = 16 << 20
MAX_CATALOG_ITEMS = 1_000_000
_ROW_CONSTRUCTION_TOKEN = object()
_COLD_ROW_CONSTRUCTION_TOKEN = object()
_CATALOG_CONSTRUCTION_TOKEN = object()


class HistoryQueryError(Exception):
    """Base class for query failures."""


class QueryValidationError(HistoryQueryError, ValueError):
    """A typed query value is malformed, ambiguous, or unbounded."""


class QueryConflictError(HistoryQueryError):
    """One complete identity names different stable content."""


class QueryBudgetExceeded(HistoryQueryError):
    """A source, row, byte, result, or deadline budget was exhausted."""


class QuerySnapshotChanged(HistoryQueryError):
    """Rows/evidence are not bound to one fixed query snapshot."""


class LatestHistoryMixError(HistoryQueryError):
    """Current-only Latest data was offered to historical merge."""


class LineageResolutionError(HistoryQueryError):
    """A row cannot be resolved through its exact source evidence."""


def _uint(value: object, bits: int, name: str, *, positive: bool = False) -> int:
    if type(value) is not int:
        raise QueryValidationError(f"{name} must be an integer")
    minimum = 1 if positive else 0
    if value < minimum or value > (1 << bits) - 1:
        raise QueryValidationError(f"{name} is outside uint{bits}")
    return value


def _fixed_bytes(
    value: object,
    width: int,
    name: str,
    *,
    nonzero: bool = True,
) -> bytes:
    if type(value) is not bytes or len(value) != width:
        raise QueryValidationError(f"{name} must be exact immutable {width}-byte data")
    if nonzero and not any(value):
        raise QueryValidationError(f"{name} must not be all zero")
    return value


def _hash_json(domain: bytes, value: object) -> bytes:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")
    return hashlib.sha256(domain + encoded).digest()


def _typed_tuple(
    values: object,
    expected_type: type,
    name: str,
    *,
    maximum: int = MAX_CATALOG_ITEMS,
) -> tuple[object, ...]:
    if type(values) is not tuple or len(values) > maximum:
        raise QueryValidationError(f"{name} must be a bounded tuple")
    if any(type(item) is not expected_type for item in values):
        raise QueryValidationError(f"{name} contains an invalid value")
    return values


def _bounded_take(
    values: object,
    maximum: int,
    name: str,
) -> tuple[object, ...]:
    """Materialize at most ``maximum`` values, rejecting value ``maximum+1``."""

    if isinstance(values, (str, bytes, bytearray, memoryview)):
        raise QueryValidationError(f"{name} must be a typed iterable")
    try:
        iterator = iter(values)  # type: ignore[arg-type]
    except TypeError as error:
        raise QueryValidationError(f"{name} must be a typed iterable") from error
    output: list[object] = []
    for item in iterator:
        if len(output) >= maximum:
            raise QueryValidationError(f"{name} exceeds the hard bound")
        output.append(item)
    return tuple(output)


def _iter_values(values: object, name: str) -> Iterator[object]:
    """Return an iterator without materializing caller-controlled input."""

    if isinstance(values, (str, bytes, bytearray, memoryview)):
        raise QueryValidationError(f"{name} must be a typed iterable")
    try:
        return iter(values)  # type: ignore[arg-type]
    except TypeError as error:
        raise QueryValidationError(f"{name} must be a typed iterable") from error


def _require_cumulative_member_bounds(
    values: tuple[object, ...],
    attributes: tuple[str, ...],
    name: str,
) -> None:
    """Bound each repeated payload category across a source catalog."""

    totals = {attribute: 0 for attribute in attributes}
    for value in values:
        for attribute in attributes:
            count = len(getattr(value, attribute))
            if count > MAX_CATALOG_ITEMS - totals[attribute]:
                raise QueryValidationError(
                    f"{name} cumulative {attribute} exceeds the hard bound"
                )
            totals[attribute] += count


class StorageTier(str, Enum):
    COLD_PARQUET = "cold_parquet"
    HOT_CANONICAL = "hot_canonical"
    LATEST_CURRENT = "latest_current"


_TIER_ORDER = {
    StorageTier.COLD_PARQUET: 0,
    StorageTier.HOT_CANONICAL: 1,
    StorageTier.LATEST_CURRENT: 2,
}


class HistoryKind(str, Enum):
    CANONICAL = "canonical"
    FACTOR = "factor"


def fixed_manifest_set_sha256(manifests: Iterable[HistoryManifest]) -> bytes:
    if isinstance(manifests, (str, bytes, bytearray, memoryview)):
        raise QueryValidationError("manifests must be a typed iterable")
    values = _bounded_take(manifests, MAX_CATALOG_ITEMS, "manifests")
    if len(values) > MAX_CATALOG_ITEMS or any(
        type(item) is not HistoryManifest for item in values
    ):
        raise QueryValidationError("manifests must be a bounded typed iterable")
    hashes = tuple(sorted(manifest_sha256(item) for item in values))
    if len(hashes) != len(set(hashes)):
        raise QueryValidationError("the fixed manifest set contains duplicates")
    run_ids = [item.run_id for item in values]
    if len(run_ids) != len(set(run_ids)):
        raise QueryValidationError(
            "a fixed manifest set must contain one CURRENT head per run_id"
        )
    return _hash_json(
        b"l2flow.query.fixed-manifest-set.v1\x00",
        [item.hex() for item in hashes],
    )


@dataclass(frozen=True, slots=True)
class FixedQuerySnapshot:
    snapshot_sequence: int
    cold_manifest_set_sha256: bytes
    hot_frontier_sha256: bytes
    canonical_lineage_catalog_sha256: bytes | None = None
    factor_lineage_catalog_sha256: bytes | None = None
    latest_generation_sha256: bytes | None = None

    def __post_init__(self) -> None:
        _uint(self.snapshot_sequence, 64, "snapshot_sequence", positive=True)
        _fixed_bytes(self.cold_manifest_set_sha256, 32, "cold_manifest_set_sha256")
        _fixed_bytes(self.hot_frontier_sha256, 32, "hot_frontier_sha256")
        for name in (
            "canonical_lineage_catalog_sha256",
            "factor_lineage_catalog_sha256",
            "latest_generation_sha256",
        ):
            value = getattr(self, name)
            if value is not None:
                _fixed_bytes(value, 32, name)

    def expected_tier_digest(self, tier: StorageTier) -> bytes:
        if not isinstance(tier, StorageTier):
            raise QueryValidationError("tier must be StorageTier")
        if tier is StorageTier.COLD_PARQUET:
            return self.cold_manifest_set_sha256
        if tier is StorageTier.HOT_CANONICAL:
            return self.hot_frontier_sha256
        if self.latest_generation_sha256 is None:
            raise QueryValidationError("Latest query requires a fixed generation")
        return self.latest_generation_sha256


@dataclass(frozen=True, slots=True)
class QueryBudget:
    max_sources: int
    max_scanned_rows: int
    max_result_rows: int
    max_bytes: int
    deadline_monotonic_ns: int
    max_lineage_entries: int | None = None
    max_lineage_bytes: int | None = None
    max_lineage_work: int | None = None

    def __post_init__(self) -> None:
        _uint(self.max_sources, 32, "max_sources", positive=True)
        _uint(self.max_scanned_rows, 64, "max_scanned_rows", positive=True)
        _uint(self.max_result_rows, 64, "max_result_rows", positive=True)
        _uint(self.max_bytes, 64, "max_bytes", positive=True)
        _uint(self.deadline_monotonic_ns, 64, "deadline_monotonic_ns", positive=True)
        lineage_entries = self.max_lineage_entries
        if lineage_entries is None:
            # One normal one-input Factor row emits one resolution, one input
            # coverage value, and one replay observation.  Wider input maps
            # must opt into a correspondingly wider explicit lineage budget.
            lineage_entries = min(
                MAX_QUERY_LINEAGE_ENTRIES,
                3 * self.max_scanned_rows,
            )
            object.__setattr__(self, "max_lineage_entries", lineage_entries)
        lineage_bytes = self.max_lineage_bytes
        if lineage_bytes is None:
            # Preserve the old five-argument API while ensuring that a caller's
            # decoded-byte ceiling is also the default lineage-byte ceiling.
            lineage_bytes = self.max_bytes
            object.__setattr__(self, "max_lineage_bytes", lineage_bytes)
        _uint(lineage_entries, 64, "max_lineage_entries", positive=True)
        _uint(lineage_bytes, 64, "max_lineage_bytes", positive=True)
        lineage_work = self.max_lineage_work
        if lineage_work is None:
            # A one-input Factor resolution performs a bounded handful of
            # entry-level hashing/validation/coverage passes.  Wider maps or
            # pathological Canonical interval candidates require opt-in work.
            lineage_work = min(
                MAX_QUERY_LINEAGE_WORK,
                7 * self.max_scanned_rows,
            )
            object.__setattr__(self, "max_lineage_work", lineage_work)
        _uint(lineage_work, 64, "max_lineage_work", positive=True)
        if self.max_sources > MAX_QUERY_SOURCES:
            raise QueryValidationError("max_sources exceeds the hard limit")
        if self.max_scanned_rows > MAX_QUERY_ROWS:
            raise QueryValidationError("max_scanned_rows exceeds the hard limit")
        if self.max_result_rows > MAX_QUERY_RESULT_ROWS:
            raise QueryValidationError("max_result_rows exceeds the hard limit")
        if self.max_bytes > MAX_QUERY_BYTES:
            raise QueryValidationError("max_bytes exceeds the hard limit")
        if lineage_entries > MAX_QUERY_LINEAGE_ENTRIES:
            raise QueryValidationError("max_lineage_entries exceeds the hard limit")
        if lineage_bytes > MAX_QUERY_LINEAGE_BYTES:
            raise QueryValidationError("max_lineage_bytes exceeds the hard limit")
        if lineage_work > MAX_QUERY_LINEAGE_WORK:
            raise QueryValidationError("max_lineage_work exceeds the hard limit")


def _receipt_wire(receipt: SourceRangeReceipt) -> dict[str, object]:
    return receipt.canonical_object()


def _receipt_total_sort_key(receipt: SourceRangeReceipt) -> tuple[object, ...]:
    """Order every persisted receipt field, including coverage observations."""

    return receipt.sort_key + (
        receipt.last_origin_ingress_sequence,
        receipt.max_origin_wal_end_pos,
        receipt.coverage_kind.value,
        b""
        if receipt.coverage_certificate_sha256 is None
        else receipt.coverage_certificate_sha256,
    )


def _ordered_unambiguous_receipts(
    values: object,
    name: str,
) -> tuple[SourceRangeReceipt, ...]:
    receipts = _typed_tuple(values, SourceRangeReceipt, name)
    ordered = tuple(sorted(receipts, key=_receipt_total_sort_key))
    previous: SourceRangeReceipt | None = None
    for receipt in ordered:
        if previous is not None:
            if receipt.sort_key == previous.sort_key:
                raise QueryValidationError(f"{name} repeats one receipt range")
            if (
                receipt.namespace == previous.namespace
                and receipt.begin_canonical_cursor
                < previous.end_canonical_cursor
            ):
                raise QueryValidationError(
                    f"{name} overlaps inside one exact namespace"
                )
        previous = receipt
    return ordered


def _canonical_receipt_route(namespace: SourceNamespace) -> tuple[object, ...]:
    """Fields that a Canonical row carries for locating its exact Raw route."""

    return (
        namespace.trade_date,
        namespace.origin_capture_date,
        namespace.source_stream_id,
        namespace.origin_stream_day_id,
        namespace.family,
    )


def _factor_receipt_route(namespace: SourceNamespace) -> tuple[object, ...]:
    """Fields persisted by one Factor input watermark for route resolution."""

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


@dataclass(frozen=True, slots=True)
class _IngressReceiptRanges:
    receipts: tuple[SourceRangeReceipt, ...]
    first_ingress: tuple[int, ...]
    prefix_max_last_ingress: tuple[int, ...]

    @classmethod
    def from_receipts(
        cls,
        receipts: Iterable[SourceRangeReceipt],
    ) -> "_IngressReceiptRanges":
        ordered = tuple(
            sorted(
                receipts,
                key=lambda item: (
                    item.first_origin_ingress_sequence,
                    item.last_origin_ingress_sequence,
                    item.namespace.sort_key,
                    item.begin_canonical_cursor,
                ),
            )
        )
        starts: list[int] = []
        prefix: list[int] = []
        maximum = 0
        for receipt in ordered:
            starts.append(receipt.first_origin_ingress_sequence)
            maximum = max(maximum, receipt.last_origin_ingress_sequence)
            prefix.append(maximum)
        return cls(ordered, tuple(starts), tuple(prefix))

    def unique_match(
        self,
        ingress_sequence: int,
        wal_end_pos: int,
        tracker: "_BudgetTracker | None",
    ) -> SourceRangeReceipt | None:
        """Return the unique interval match in O(log R + candidate intervals)."""

        index = bisect_right(self.first_ingress, ingress_sequence) - 1
        match: SourceRangeReceipt | None = None
        while index >= 0:
            if tracker is not None:
                # Arbitrary ingress/WAL rectangles need not be disjoint even
                # though their exact-namespace Canonical cursor ranges are.
                # Bound this output-sensitive fallback explicitly.
                tracker.consume_lineage_work(1)
            if self.prefix_max_last_ingress[index] < ingress_sequence:
                break
            receipt = self.receipts[index]
            if (
                ingress_sequence <= receipt.last_origin_ingress_sequence
                and receipt.min_origin_wal_end_pos
                <= wal_end_pos
                <= receipt.max_origin_wal_end_pos
            ):
                if match is not None:
                    return None
                match = receipt
            index -= 1
        return match


@dataclass(frozen=True, slots=True)
class _NamespaceReceiptRanges:
    receipts: tuple[SourceRangeReceipt, ...]
    begin_cursors: tuple[int, ...]

    @classmethod
    def from_receipts(
        cls,
        receipts: Iterable[SourceRangeReceipt],
    ) -> "_NamespaceReceiptRanges":
        ordered = tuple(
            sorted(receipts, key=lambda item: item.begin_canonical_cursor)
        )
        previous: SourceRangeReceipt | None = None
        for receipt in ordered:
            if previous is not None and (
                receipt.begin_canonical_cursor < previous.end_canonical_cursor
            ):
                raise QueryValidationError(
                    "receipt index overlaps inside one exact namespace"
                )
            previous = receipt
        return cls(
            ordered,
            tuple(item.begin_canonical_cursor for item in ordered),
        )

    def covering_cursor(self, cursor: int) -> SourceRangeReceipt | None:
        """Resolve the non-overlapping persisted range in O(log R)."""

        index = bisect_left(self.begin_cursors, cursor) - 1
        if index < 0:
            return None
        candidate = self.receipts[index]
        if cursor <= candidate.end_canonical_cursor:
            return candidate
        return None


@dataclass(frozen=True, slots=True)
class _ReceiptIndex:
    by_namespace: Mapping[SourceNamespace, _NamespaceReceiptRanges]
    canonical_routes: Mapping[tuple[object, ...], _IngressReceiptRanges]
    factor_route_namespaces: Mapping[
        tuple[object, ...], tuple[SourceNamespace, ...]
    ]

    @classmethod
    def build(cls, receipts: tuple[SourceRangeReceipt, ...]) -> "_ReceiptIndex":
        namespace_values: dict[SourceNamespace, list[SourceRangeReceipt]] = {}
        canonical_values: dict[tuple[object, ...], list[SourceRangeReceipt]] = {}
        factor_namespaces: dict[tuple[object, ...], set[SourceNamespace]] = {}
        for receipt in receipts:
            namespace_values.setdefault(receipt.namespace, []).append(receipt)
            canonical_values.setdefault(
                _canonical_receipt_route(receipt.namespace), []
            ).append(receipt)
            factor_namespaces.setdefault(
                _factor_receipt_route(receipt.namespace), set()
            ).add(receipt.namespace)
        by_namespace = {
            namespace: _NamespaceReceiptRanges.from_receipts(values)
            for namespace, values in namespace_values.items()
        }
        canonical_routes = {
            route: _IngressReceiptRanges.from_receipts(values)
            for route, values in canonical_values.items()
        }
        route_namespaces = {
            route: tuple(sorted(values, key=lambda item: item.sort_key))
            for route, values in factor_namespaces.items()
        }
        return cls(
            MappingProxyType(by_namespace),
            MappingProxyType(canonical_routes),
            MappingProxyType(route_namespaces),
        )


@dataclass(frozen=True, slots=True)
class HotCanonicalSource:
    """One unpublished Canonical source bound to a retained hot frontier."""

    hot_frontier_sha256: bytes
    rows: tuple[CanonicalArchiveRow, ...]
    receipts: tuple[SourceRangeReceipt, ...]
    source_id_sha256: bytes = field(init=False)

    def __post_init__(self) -> None:
        _fixed_bytes(self.hot_frontier_sha256, 32, "hot_frontier_sha256")
        rows = _typed_tuple(self.rows, CanonicalArchiveRow, "hot Canonical rows")
        ordered_rows = tuple(sorted(rows, key=lambda item: item.total_sort_key))
        ordered = _ordered_unambiguous_receipts(self.receipts, "hot receipts")
        keys = [item.identity_key for item in ordered_rows]
        if len(keys) != len(set(keys)):
            raise QueryValidationError("hot Canonical row identity repeats")
        object.__setattr__(self, "rows", ordered_rows)
        object.__setattr__(self, "receipts", ordered)
        object.__setattr__(
            self,
            "source_id_sha256",
            _hash_json(
                b"l2flow.query.hot-canonical-source.v1\x00",
                {
                    "hot_frontier_sha256": self.hot_frontier_sha256.hex(),
                    "receipts": [_receipt_wire(item) for item in ordered],
                    "rows": [
                        {
                            "identity": [
                                str(item.origin_capture_date),
                                item.origin_stream_day_id.hex(),
                                str(item.header.source_stream_id),
                                str(item.header.origin_ingress_sequence),
                                str(item.header.sub_index),
                                str(item.header.schema_version),
                            ],
                            "record_sha256": item.record_sha256.hex(),
                        }
                        for item in ordered_rows
                    ],
                },
            ),
        )

    def canonical(self) -> dict[str, object]:
        return {
            "hot_frontier_sha256": self.hot_frontier_sha256.hex(),
            "receipts": [_receipt_wire(item) for item in self.receipts],
            "row_sha256s": [item.record_sha256.hex() for item in self.rows],
            "source_id_sha256": self.source_id_sha256.hex(),
        }


@dataclass(frozen=True, slots=True)
class FactorLiveSource:
    """Unpublished hot or current-only factor source and its exact inputs."""

    tier: StorageTier
    source_snapshot_sha256: bytes
    rows: tuple[FactorHistoryRow, ...]
    sidecars: tuple[WatermarkSidecar, ...]
    references: tuple[SidecarReference, ...]
    source_receipts: tuple[SourceRangeReceipt, ...]
    source_id_sha256: bytes = field(init=False)

    def __post_init__(self) -> None:
        if self.tier not in (StorageTier.HOT_CANONICAL, StorageTier.LATEST_CURRENT):
            raise QueryValidationError("live factor source tier must be Hot or Latest")
        _fixed_bytes(self.source_snapshot_sha256, 32, "source_snapshot_sha256")
        rows = _typed_tuple(self.rows, FactorHistoryRow, "factor live rows")
        sidecars = _typed_tuple(self.sidecars, WatermarkSidecar, "live sidecars")
        refs = _typed_tuple(self.references, SidecarReference, "factor references")
        ordered_receipts = _ordered_unambiguous_receipts(
            self.source_receipts,
            "factor source receipts",
        )
        ordered_rows = tuple(
            sorted(
                rows,
                key=lambda item: (
                    item.total_sort_key,
                    item.reference_tie_break_key,
                    item.content_sha256,
                ),
            )
        )
        ordered_sidecars = tuple(sorted(sidecars, key=lambda item: item.namespace))
        ordered_refs = tuple(sorted(refs, key=lambda item: item.sort_key))
        if len({item.namespace for item in ordered_sidecars}) != len(ordered_sidecars):
            raise QueryValidationError("factor live sidecar namespace repeats")
        if len({item.sort_key for item in ordered_refs}) != len(ordered_refs):
            raise QueryValidationError("factor source references repeat")
        sidecar_by_namespace = {item.namespace: item for item in ordered_sidecars}
        if set(sidecar_by_namespace) != {item.namespace for item in ordered_refs}:
            raise QueryValidationError(
                "factor live sidecars and referenced namespaces must match exactly"
            )
        for reference in ordered_refs:
            try:
                sidecar_by_namespace[reference.namespace].resolve(reference)
            except Exception as error:
                raise QueryValidationError(
                    "factor live source has an unresolved sidecar reference"
                ) from error
        row_reference_keys = {
            (
                item.run_id,
                item.watermark_table_generation,
                item.watermark_set_id,
                item.input_identity_sha256,
            )
            for item in ordered_rows
        }
        allowed_reference_keys = {
            (
                item.namespace.run_id,
                item.namespace.table_generation,
                item.watermark_set_id,
                item.input_identity_sha256,
            )
            for item in ordered_refs
        }
        if row_reference_keys != allowed_reference_keys:
            raise QueryValidationError(
                "factor live rows and source references must match exactly"
            )
        object.__setattr__(self, "rows", ordered_rows)
        object.__setattr__(self, "sidecars", ordered_sidecars)
        object.__setattr__(self, "references", ordered_refs)
        object.__setattr__(self, "source_receipts", ordered_receipts)
        object.__setattr__(
            self,
            "source_id_sha256",
            _hash_json(
                b"l2flow.query.factor-live-source.v1\x00",
                {
                    "receipts": [_receipt_wire(item) for item in ordered_receipts],
                    "references": [item.canonical_object() for item in ordered_refs],
                    "rows": [
                        {
                            "content_sha256": item.content_sha256.hex(),
                            "identity": [
                                item.factor_id,
                                item.factor_version,
                                str(item.instrument_id),
                                str(item.asof_ns),
                                item.input_identity_sha256.hex(),
                            ],
                        }
                        for item in ordered_rows
                    ],
                    "sidecars": [
                        {
                            "file_sha256": hashlib.sha256(encode_sidecar(item)).hexdigest(),
                            "namespace": item.namespace.canonical_object(),
                        }
                        for item in ordered_sidecars
                    ],
                    "source_snapshot_sha256": self.source_snapshot_sha256.hex(),
                    "tier": self.tier.value,
                },
            ),
        )

    def canonical(self) -> dict[str, object]:
        return {
            "receipts": [_receipt_wire(item) for item in self.source_receipts],
            "references": [item.canonical_object() for item in self.references],
            "row_sha256s": [item.content_sha256.hex() for item in self.rows],
            "sidecars": [
                {
                    "file_sha256": hashlib.sha256(encode_sidecar(item)).hexdigest(),
                    "namespace": item.namespace.canonical_object(),
                }
                for item in self.sidecars
            ],
            "source_id_sha256": self.source_id_sha256.hex(),
            "source_snapshot_sha256": self.source_snapshot_sha256.hex(),
            "tier": self.tier.value,
        }


@dataclass(frozen=True, slots=True)
class _VisibleArtifactBinding:
    artifact: ArtifactEntry
    manifest_sha256s: tuple[bytes, ...]
    receipt_index: _ReceiptIndex
    factor_reference_index: Mapping[
        tuple[SidecarNamespace, int], SidecarReference
    ]


def _factor_reference_index(
    references: tuple[SidecarReference, ...],
) -> Mapping[tuple[SidecarNamespace, int], SidecarReference]:
    values: dict[tuple[SidecarNamespace, int], SidecarReference] = {}
    for reference in references:
        key = (reference.namespace, reference.watermark_set_id)
        if key in values:
            raise QueryValidationError("factor reference index repeats one key")
        values[key] = reference
    return MappingProxyType(values)


def _manifest_inventory(
    manifests: tuple[HistoryManifest, ...],
    *,
    maximum_members: int = MAX_CATALOG_ITEMS,
) -> tuple[tuple[HistoryManifest, ...], tuple[bytes, ...]]:
    if type(maximum_members) is not int or maximum_members < 0:
        raise QueryValidationError("maximum_members must be a nonnegative integer")
    values = _typed_tuple(
        manifests,
        HistoryManifest,
        "manifests",
        maximum=MAX_CATALOG_ITEMS,
    )
    pairs = tuple(sorted(((manifest_sha256(item), item) for item in values), key=lambda x: x[0]))
    hashes = tuple(item[0] for item in pairs)
    if len(hashes) != len(set(hashes)):
        raise QueryValidationError("the fixed manifest set contains duplicates")
    run_ids = [item[1].run_id for item in pairs]
    if len(run_ids) != len(set(run_ids)):
        raise QueryValidationError(
            "a fixed manifest set must contain one CURRENT head per run_id"
        )
    artifacts_by_path: dict[str, ArtifactEntry] = {}
    sidecars_by_path: dict[str, object] = {}
    member_count = 0
    for _, manifest in pairs:
        manifest_members = len(manifest.artifacts) + len(manifest.sidecars)
        if manifest_members > maximum_members - member_count:
            raise QueryValidationError(
                "fixed manifest artifact/sidecar inventory exceeds the hard bound"
            )
        member_count += manifest_members
        for artifact in manifest.artifacts:
            existing = artifacts_by_path.get(artifact.relative_path)
            if existing is not None and existing != artifact:
                raise QueryValidationError(
                    "fixed manifests rebind one artifact path to different metadata"
                )
            artifacts_by_path[artifact.relative_path] = artifact
        for descriptor in manifest.sidecars:
            existing_descriptor = sidecars_by_path.get(descriptor.relative_path)
            if existing_descriptor is not None and existing_descriptor != descriptor:
                raise QueryValidationError(
                    "fixed manifests rebind one sidecar path to different metadata"
                )
            sidecars_by_path[descriptor.relative_path] = descriptor
    return tuple(item[1] for item in pairs), hashes


def _visible_artifacts(
    manifests: tuple[HistoryManifest, ...],
    hashes: tuple[bytes, ...],
    dataset: DatasetKind,
) -> tuple[_VisibleArtifactBinding, ...]:
    by_file: dict[bytes, tuple[ArtifactEntry, set[bytes]]] = {}
    for manifest, digest in zip(manifests, hashes):
        for artifact in manifest.visible_artifacts:
            if artifact.artifact_type is not dataset:
                continue
            existing = by_file.get(artifact.file_sha256)
            if existing is None:
                by_file[artifact.file_sha256] = (artifact, {digest})
            elif existing[0] != artifact:
                raise QueryValidationError(
                    "one visible file SHA-256 is bound to different artifact metadata"
                )
            else:
                existing[1].add(digest)
    return tuple(
        _VisibleArtifactBinding(
            artifact=value[0],
            manifest_sha256s=tuple(sorted(value[1])),
            receipt_index=_ReceiptIndex.build(value[0].source_receipts),
            factor_reference_index=_factor_reference_index(
                value[0].factor_sidecar_refs
            ),
        )
        for _, value in sorted(by_file.items())
    )


@dataclass(frozen=True, slots=True)
class CanonicalLineageCatalog:
    manifests: tuple[HistoryManifest, ...]
    hot_frontier_sha256: bytes
    hot_sources: tuple[HotCanonicalSource, ...]
    _construction_token: InitVar[object] = None
    manifest_set_sha256: bytes = field(init=False)
    catalog_sha256: bytes = field(init=False)
    visible_artifacts: tuple[_VisibleArtifactBinding, ...] = field(init=False, repr=False)
    _artifact_index: Mapping[bytes, _VisibleArtifactBinding] = field(
        init=False, repr=False, compare=False
    )
    _hot_source_index: Mapping[bytes, HotCanonicalSource] = field(
        init=False, repr=False, compare=False
    )
    _hot_row_indexes: Mapping[
        bytes, Mapping[tuple[int, bytes, int, int, int, int], CanonicalArchiveRow]
    ] = field(init=False, repr=False, compare=False)
    _hot_receipt_indexes: Mapping[bytes, _ReceiptIndex] = field(
        init=False, repr=False, compare=False
    )

    def __post_init__(self, _construction_token: object) -> None:
        if _construction_token is not _CATALOG_CONSTRUCTION_TOKEN:
            raise QueryValidationError("use CanonicalLineageCatalog.from_manifests")
        manifests, hashes = _manifest_inventory(self.manifests)
        _fixed_bytes(self.hot_frontier_sha256, 32, "hot_frontier_sha256")
        sources = _typed_tuple(
            self.hot_sources,
            HotCanonicalSource,
            "hot Canonical sources",
            maximum=MAX_QUERY_SOURCES,
        )
        ordered_sources = tuple(sorted(sources, key=lambda item: item.source_id_sha256))
        _require_cumulative_member_bounds(
            ordered_sources,
            ("rows", "receipts"),
            "hot Canonical sources",
        )
        if len({item.source_id_sha256 for item in ordered_sources}) != len(ordered_sources):
            raise QueryValidationError("hot Canonical source identity repeats")
        if any(
            item.hot_frontier_sha256 != self.hot_frontier_sha256
            for item in ordered_sources
        ):
            raise QueryValidationError("hot Canonical source is bound to another frontier")
        artifacts = _visible_artifacts(manifests, hashes, DatasetKind.CANONICAL)
        manifest_set = _hash_json(
            b"l2flow.query.fixed-manifest-set.v1\x00",
            [item.hex() for item in hashes],
        )
        object.__setattr__(self, "manifests", manifests)
        object.__setattr__(self, "hot_sources", ordered_sources)
        object.__setattr__(self, "visible_artifacts", artifacts)
        object.__setattr__(
            self,
            "_artifact_index",
            MappingProxyType(
                {item.artifact.file_sha256: item for item in artifacts}
            ),
        )
        object.__setattr__(
            self,
            "_hot_source_index",
            MappingProxyType(
                {item.source_id_sha256: item for item in ordered_sources}
            ),
        )
        object.__setattr__(
            self,
            "_hot_row_indexes",
            MappingProxyType(
                {
                    source.source_id_sha256: MappingProxyType(
                        {row.identity_key: row for row in source.rows}
                    )
                    for source in ordered_sources
                }
            ),
        )
        object.__setattr__(
            self,
            "_hot_receipt_indexes",
            MappingProxyType(
                {
                    source.source_id_sha256: _ReceiptIndex.build(source.receipts)
                    for source in ordered_sources
                }
            ),
        )
        object.__setattr__(self, "manifest_set_sha256", manifest_set)
        object.__setattr__(
            self,
            "catalog_sha256",
            _hash_json(
                b"l2flow.query.canonical-lineage-catalog.v2\x00",
                {
                    "hot_frontier_sha256": self.hot_frontier_sha256.hex(),
                    "hot_sources": [item.canonical() for item in ordered_sources],
                    "manifest_sha256s": [item.hex() for item in hashes],
                },
            ),
        )

    @classmethod
    def from_manifests(
        cls,
        manifests: Iterable[HistoryManifest],
        *,
        hot_frontier_sha256: bytes,
        hot_sources: Iterable[HotCanonicalSource] = (),
    ) -> "CanonicalLineageCatalog":
        return cls(
            manifests=_bounded_take(manifests, MAX_CATALOG_ITEMS, "manifests"),
            hot_frontier_sha256=hot_frontier_sha256,
            hot_sources=_bounded_take(
                hot_sources,
                MAX_QUERY_SOURCES,
                "hot Canonical sources",
            ),
            _construction_token=_CATALOG_CONSTRUCTION_TOKEN,
        )


@dataclass(frozen=True, slots=True)
class _SidecarBinding:
    sidecar: WatermarkSidecar
    file_sha256: bytes
    descriptor_paths: tuple[str, ...]
    manifest_sha256s: tuple[bytes, ...]


@dataclass(frozen=True, slots=True)
class _LiveFactorBinding:
    source: FactorLiveSource
    receipt_index: _ReceiptIndex
    rows: frozenset[FactorHistoryRow]
    sidecars: Mapping[
        SidecarNamespace, tuple[WatermarkSidecar, bytes]
    ]
    references: Mapping[tuple[SidecarNamespace, int], SidecarReference]


@dataclass(frozen=True, slots=True)
class FactorLineageCatalog:
    manifests: tuple[HistoryManifest, ...]
    sidecars: tuple[WatermarkSidecar, ...]
    hot_frontier_sha256: bytes
    latest_generation_sha256: bytes | None
    live_sources: tuple[FactorLiveSource, ...]
    _construction_token: InitVar[object] = None
    manifest_set_sha256: bytes = field(init=False)
    catalog_sha256: bytes = field(init=False)
    visible_artifacts: tuple[_VisibleArtifactBinding, ...] = field(init=False, repr=False)
    sidecar_bindings: tuple[_SidecarBinding, ...] = field(init=False, repr=False)
    _artifact_index: Mapping[bytes, _VisibleArtifactBinding] = field(
        init=False, repr=False, compare=False
    )
    _sidecar_index: Mapping[SidecarNamespace, _SidecarBinding] = field(
        init=False, repr=False, compare=False
    )
    _live_source_index: Mapping[
        tuple[StorageTier, bytes], _LiveFactorBinding
    ] = field(init=False, repr=False, compare=False)
    _watermark_index: Mapping[
        SidecarReference, FactorInputWatermarkSet
    ] = field(init=False, repr=False, compare=False)

    def __post_init__(self, _construction_token: object) -> None:
        if _construction_token is not _CATALOG_CONSTRUCTION_TOKEN:
            raise QueryValidationError("use FactorLineageCatalog.from_manifests")
        manifests, hashes = _manifest_inventory(self.manifests)
        _fixed_bytes(self.hot_frontier_sha256, 32, "hot_frontier_sha256")
        if self.latest_generation_sha256 is not None:
            _fixed_bytes(
                self.latest_generation_sha256,
                32,
                "latest_generation_sha256",
            )
        sidecars = _typed_tuple(self.sidecars, WatermarkSidecar, "sidecars")
        ordered_sidecars = tuple(sorted(sidecars, key=lambda item: item.namespace))
        namespaces = [item.namespace for item in ordered_sidecars]
        if len(namespaces) != len(set(namespaces)):
            raise QueryValidationError("sidecar namespace repeats")
        sidecar_by_namespace = {item.namespace: item for item in ordered_sidecars}

        descriptor_info: dict[SidecarNamespace, tuple[bytes, set[str], set[bytes]]] = {}
        for manifest, digest in zip(manifests, hashes):
            supplied: dict[SidecarNamespace, WatermarkSidecar] = {}
            for descriptor in manifest.sidecars:
                sidecar = sidecar_by_namespace.get(descriptor.namespace)
                if sidecar is None:
                    raise QueryValidationError("manifest sidecar descriptor is unresolved")
                supplied[descriptor.namespace] = sidecar
                existing = descriptor_info.get(descriptor.namespace)
                if existing is None:
                    descriptor_info[descriptor.namespace] = (
                        descriptor.file_sha256,
                        {descriptor.relative_path},
                        {digest},
                    )
                elif existing[0] != descriptor.file_sha256:
                    raise QueryValidationError(
                        "one sidecar namespace has conflicting descriptor bytes"
                    )
                else:
                    existing[1].add(descriptor.relative_path)
                    existing[2].add(digest)
            try:
                manifest.validate_sidecars(supplied)
            except Exception as error:
                raise QueryValidationError(
                    "manifest sidecar descriptors/content failed exact validation"
                ) from error
        if set(sidecar_by_namespace) != set(descriptor_info):
            raise QueryValidationError("retained sidecars differ from manifest descriptors")

        binding_by_namespace: dict[SidecarNamespace, _SidecarBinding] = {}
        for namespace in sorted(sidecar_by_namespace):
            sidecar = sidecar_by_namespace[namespace]
            file_sha, paths, manifest_hashes = descriptor_info[namespace]
            if hashlib.sha256(encode_sidecar(sidecar)).digest() != file_sha:
                raise QueryValidationError("sidecar bytes disagree with descriptor SHA-256")
            binding_by_namespace[namespace] = _SidecarBinding(
                sidecar=sidecar,
                file_sha256=file_sha,
                descriptor_paths=tuple(sorted(paths)),
                manifest_sha256s=tuple(sorted(manifest_hashes)),
            )

        sources = _typed_tuple(
            self.live_sources,
            FactorLiveSource,
            "factor live sources",
            maximum=MAX_QUERY_SOURCES,
        )
        ordered_sources = tuple(
            sorted(sources, key=lambda item: (item.tier.value, item.source_id_sha256))
        )
        _require_cumulative_member_bounds(
            ordered_sources,
            ("rows", "sidecars", "references", "source_receipts"),
            "factor live sources",
        )
        source_keys = [(item.tier, item.source_id_sha256) for item in ordered_sources]
        if len(source_keys) != len(set(source_keys)):
            raise QueryValidationError("factor live source identity repeats")
        for source in ordered_sources:
            expected = (
                self.hot_frontier_sha256
                if source.tier is StorageTier.HOT_CANONICAL
                else self.latest_generation_sha256
            )
            if expected is None or source.source_snapshot_sha256 != expected:
                raise QueryValidationError("factor live source is snapshot-mismatched")
            for sidecar in source.sidecars:
                encoded_sha = hashlib.sha256(encode_sidecar(sidecar)).digest()
                existing = binding_by_namespace.get(sidecar.namespace)
                if existing is not None:
                    if (
                        existing.file_sha256 != encoded_sha
                        or existing.sidecar != sidecar
                    ):
                        raise QueryValidationError(
                            "one cold/live sidecar namespace has conflicting content"
                        )
                else:
                    binding_by_namespace[sidecar.namespace] = _SidecarBinding(
                        sidecar=sidecar,
                        file_sha256=encoded_sha,
                        descriptor_paths=(),
                        manifest_sha256s=(),
                    )
            for reference in source.references:
                binding = binding_by_namespace.get(reference.namespace)
                if binding is None:
                    raise QueryValidationError("factor live source has an orphan reference")
                try:
                    binding.sidecar.resolve(reference)
                except Exception as error:
                    raise QueryValidationError(
                        "factor live source reference failed exact resolution"
                    ) from error

        bindings = tuple(
            binding_by_namespace[key] for key in sorted(binding_by_namespace)
        )

        live_binding_values: dict[
            tuple[StorageTier, bytes], _LiveFactorBinding
        ] = {}
        for source in ordered_sources:
            live_sidecars: dict[
                SidecarNamespace, tuple[WatermarkSidecar, bytes]
            ] = {}
            for sidecar in source.sidecars:
                live_sidecars[sidecar.namespace] = (
                    sidecar,
                    hashlib.sha256(encode_sidecar(sidecar)).digest(),
                )
            live_binding_values[(source.tier, source.source_id_sha256)] = (
                _LiveFactorBinding(
                    source=source,
                    receipt_index=_ReceiptIndex.build(source.source_receipts),
                    rows=frozenset(source.rows),
                    sidecars=MappingProxyType(live_sidecars),
                    references=_factor_reference_index(source.references),
                )
            )

        artifacts = _visible_artifacts(manifests, hashes, DatasetKind.FACTOR)
        resolved_watermarks: dict[
            SidecarReference, FactorInputWatermarkSet
        ] = {}
        for artifact_binding in artifacts:
            for reference in artifact_binding.artifact.factor_sidecar_refs:
                sidecar_binding = binding_by_namespace.get(reference.namespace)
                if (
                    sidecar_binding is None
                    or not sidecar_binding.manifest_sha256s
                ):
                    raise QueryValidationError(
                        "cold factor reference has no manifest sidecar"
                    )
                try:
                    resolved_watermarks[reference] = (
                        sidecar_binding.sidecar.resolve(reference)
                    )
                except Exception as error:
                    raise QueryValidationError(
                        "cold factor reference failed exact resolution"
                    ) from error
        for source in ordered_sources:
            for reference in source.references:
                sidecar_binding = binding_by_namespace[reference.namespace]
                try:
                    resolved_watermarks[reference] = (
                        sidecar_binding.sidecar.resolve(reference)
                    )
                except Exception as error:
                    raise QueryValidationError(
                        "live factor reference failed exact resolution"
                    ) from error
        manifest_set = _hash_json(
            b"l2flow.query.fixed-manifest-set.v1\x00",
            [item.hex() for item in hashes],
        )
        object.__setattr__(self, "manifests", manifests)
        object.__setattr__(self, "sidecars", ordered_sidecars)
        object.__setattr__(self, "live_sources", ordered_sources)
        object.__setattr__(self, "visible_artifacts", artifacts)
        object.__setattr__(self, "sidecar_bindings", bindings)
        object.__setattr__(
            self,
            "_artifact_index",
            MappingProxyType(
                {item.artifact.file_sha256: item for item in artifacts}
            ),
        )
        object.__setattr__(
            self,
            "_sidecar_index",
            MappingProxyType(
                {item.sidecar.namespace: item for item in bindings}
            ),
        )
        object.__setattr__(
            self,
            "_live_source_index",
            MappingProxyType(live_binding_values),
        )
        object.__setattr__(
            self,
            "_watermark_index",
            MappingProxyType(resolved_watermarks),
        )
        object.__setattr__(self, "manifest_set_sha256", manifest_set)
        object.__setattr__(
            self,
            "catalog_sha256",
            _hash_json(
                b"l2flow.query.factor-lineage-catalog.v2\x00",
                {
                    "hot_frontier_sha256": self.hot_frontier_sha256.hex(),
                    "latest_generation_sha256": (
                        None
                        if self.latest_generation_sha256 is None
                        else self.latest_generation_sha256.hex()
                    ),
                    "live_sources": [item.canonical() for item in ordered_sources],
                    "manifest_sha256s": [item.hex() for item in hashes],
                    "sidecars": [
                        {
                            "file_sha256": item.file_sha256.hex(),
                            "manifest_sha256s": [value.hex() for value in item.manifest_sha256s],
                            "namespace": item.sidecar.namespace.canonical_object(),
                            "paths": list(item.descriptor_paths),
                        }
                        for item in bindings
                    ],
                },
            ),
        )

    @classmethod
    def from_manifests(
        cls,
        manifests: Iterable[HistoryManifest],
        *,
        sidecars: Iterable[WatermarkSidecar],
        hot_frontier_sha256: bytes,
        latest_generation_sha256: bytes | None = None,
        live_sources: Iterable[FactorLiveSource] = (),
    ) -> "FactorLineageCatalog":
        return cls(
            manifests=_bounded_take(manifests, MAX_CATALOG_ITEMS, "manifests"),
            sidecars=_bounded_take(sidecars, MAX_CATALOG_ITEMS, "sidecars"),
            hot_frontier_sha256=hot_frontier_sha256,
            latest_generation_sha256=latest_generation_sha256,
            live_sources=_bounded_take(
                live_sources,
                MAX_QUERY_SOURCES,
                "factor live sources",
            ),
            _construction_token=_CATALOG_CONSTRUCTION_TOKEN,
        )


@dataclass(frozen=True, slots=True)
class CanonicalQueryRow:
    archive_row: CanonicalArchiveRow
    tier: StorageTier
    source_snapshot_sha256: bytes
    source_id_sha256: bytes
    _construction_token: InitVar[object] = None

    def __post_init__(self, _construction_token: object) -> None:
        if type(self.archive_row) is not CanonicalArchiveRow:
            raise QueryValidationError("archive_row must be exact CanonicalArchiveRow")
        if not isinstance(self.tier, StorageTier):
            raise QueryValidationError("tier must be StorageTier")
        if self.tier is StorageTier.LATEST_CURRENT:
            raise LatestHistoryMixError("Canonical history cannot contain Latest")
        expected_token = (
            _COLD_ROW_CONSTRUCTION_TOKEN
            if self.tier is StorageTier.COLD_PARQUET
            else _ROW_CONSTRUCTION_TOKEN
        )
        if _construction_token is not expected_token:
            raise QueryValidationError(
                "query rows are created only by frozen source/readback adapters"
            )
        _fixed_bytes(self.source_snapshot_sha256, 32, "source_snapshot_sha256")
        _fixed_bytes(self.source_id_sha256, 32, "source_id_sha256")

    @classmethod
    def _from_live_source(
        cls,
        archive_row: CanonicalArchiveRow,
        *,
        source_snapshot_sha256: bytes,
        source_id_sha256: bytes,
    ) -> "CanonicalQueryRow":
        return cls(
            archive_row=archive_row,
            tier=StorageTier.HOT_CANONICAL,
            source_snapshot_sha256=source_snapshot_sha256,
            source_id_sha256=source_id_sha256,
            _construction_token=_ROW_CONSTRUCTION_TOKEN,
        )

    @classmethod
    def _from_cold_readback(
        cls,
        archive_row: CanonicalArchiveRow,
        *,
        source_snapshot_sha256: bytes,
        source_id_sha256: bytes,
    ) -> "CanonicalQueryRow":
        return cls(
            archive_row=archive_row,
            tier=StorageTier.COLD_PARQUET,
            source_snapshot_sha256=source_snapshot_sha256,
            source_id_sha256=source_id_sha256,
            _construction_token=_COLD_ROW_CONSTRUCTION_TOKEN,
        )

    @property
    def key(self) -> tuple[int, bytes, int, int, int, int]:
        return self.archive_row.identity_key

    @property
    def scan_bytes(self) -> int:
        # Exact frozen Canonical logical-row wire: capture-date u32,
        # stream-day-id[16], record-length u32, then the record bytes.
        return 24 + len(self.archive_row.record_bytes)


def _factor_scan_bytes(row: FactorHistoryRow) -> int:
    # Exact frozen Factor logical-row wire.  The 301-byte fixed portion includes
    # four uint16 text lengths plus the outer semantic length and replay fields.
    return 301 + sum(
        len(item.encode("ascii"))
        for item in (
            row.factor_id,
            row.factor_version,
            row.implementation_status.value,
            row.numeric_dtype,
        )
    )


@dataclass(frozen=True, slots=True)
class FactorQueryRow:
    archive_row: FactorHistoryRow
    tier: StorageTier
    source_snapshot_sha256: bytes
    source_id_sha256: bytes
    _construction_token: InitVar[object] = None
    encoded_bytes: int = field(init=False)

    def __post_init__(self, _construction_token: object) -> None:
        if type(self.archive_row) is not FactorHistoryRow:
            raise QueryValidationError("archive_row must be exact FactorHistoryRow")
        if not isinstance(self.tier, StorageTier):
            raise QueryValidationError("tier must be StorageTier")
        expected_token = (
            _COLD_ROW_CONSTRUCTION_TOKEN
            if self.tier is StorageTier.COLD_PARQUET
            else _ROW_CONSTRUCTION_TOKEN
        )
        if _construction_token is not expected_token:
            raise QueryValidationError(
                "query rows are created only by frozen source/readback adapters"
            )
        _fixed_bytes(self.source_snapshot_sha256, 32, "source_snapshot_sha256")
        _fixed_bytes(self.source_id_sha256, 32, "source_id_sha256")
        encoded = _factor_scan_bytes(self.archive_row)
        if encoded > MAX_ROW_BYTES:
            raise QueryValidationError("decoded row exceeds the row-size hard limit")
        object.__setattr__(self, "encoded_bytes", encoded)

    @classmethod
    def _from_live_source(
        cls,
        archive_row: FactorHistoryRow,
        *,
        tier: StorageTier,
        source_snapshot_sha256: bytes,
        source_id_sha256: bytes,
    ) -> "FactorQueryRow":
        if tier not in (StorageTier.HOT_CANONICAL, StorageTier.LATEST_CURRENT):
            raise QueryValidationError("live factor row tier must be Hot or Latest")
        return cls(
            archive_row=archive_row,
            tier=tier,
            source_snapshot_sha256=source_snapshot_sha256,
            source_id_sha256=source_id_sha256,
            _construction_token=_ROW_CONSTRUCTION_TOKEN,
        )

    @classmethod
    def _from_cold_readback(
        cls,
        archive_row: FactorHistoryRow,
        *,
        source_snapshot_sha256: bytes,
        source_id_sha256: bytes,
    ) -> "FactorQueryRow":
        return cls(
            archive_row=archive_row,
            tier=StorageTier.COLD_PARQUET,
            source_snapshot_sha256=source_snapshot_sha256,
            source_id_sha256=source_id_sha256,
            _construction_token=_COLD_ROW_CONSTRUCTION_TOKEN,
        )

    @property
    def key(self) -> tuple[str, str, int, int, bytes]:
        return self.archive_row.identity_key

    @property
    def current_key(self) -> tuple[str, str, int]:
        row = self.archive_row
        return (row.factor_id, row.factor_version, row.instrument_id)

    @property
    def scan_bytes(self) -> int:
        return self.encoded_bytes


def _hot_canonical_by_id(
    catalog: CanonicalLineageCatalog,
    source_id: bytes,
) -> HotCanonicalSource | None:
    return catalog._hot_source_index.get(source_id)


def _live_factor_by_id(
    catalog: FactorLineageCatalog,
    tier: StorageTier,
    source_id: bytes,
) -> FactorLiveSource | None:
    binding = catalog._live_source_index.get((tier, source_id))
    return None if binding is None else binding.source


def _live_factor_binding(
    catalog: FactorLineageCatalog,
    tier: StorageTier,
    source_id: bytes,
) -> _LiveFactorBinding | None:
    return catalog._live_source_index.get((tier, source_id))


def _validate_factor_row_catalog_membership(
    row: FactorQueryRow,
    catalog: FactorLineageCatalog,
) -> None:
    """Perform every row-specific source check before resolution-cache reuse."""

    if row.tier is StorageTier.COLD_PARQUET:
        binding = catalog._artifact_index.get(row.source_id_sha256)
        if binding is None:
            raise LineageResolutionError(
                "cold factor row has no exact visible artifact"
            )
        model = row.archive_row
        partition = binding.artifact.partition
        if (
            partition.trade_date != model.trade_date
            or partition.bucket != model.bucket
            or partition.factor_version != model.factor_version
        ):
            raise LineageResolutionError(
                "factor row does not belong to its source partition"
            )
        return
    binding = _live_factor_binding(catalog, row.tier, row.source_id_sha256)
    if binding is None:
        raise LineageResolutionError(
            "factor row has no exact live source snapshot"
        )
    if row.archive_row not in binding.rows:
        raise LineageResolutionError(
            "factor row is not in its frozen live source"
        )


def _canonical_market(row: CanonicalArchiveRow) -> str:
    return {0: "GLOBAL", 1: "SH", 2: "SZ"}[row.header.market]


def _lineage_json_size(value: object) -> int:
    return len(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("ascii")
    )


def _lineage_text_size(value: str | None) -> int:
    return 8 if value is None else 8 + len(value.encode("utf-8"))


def _lineage_hash_sequence_size(values: tuple[bytes, ...]) -> int:
    return 8 + 32 * len(values)


def _canonical_locator_lineage_bytes(
    *,
    tier: StorageTier,
    source_id_sha256: bytes,
    source_snapshot_sha256: bytes,
    receipt: SourceRangeReceipt,
    artifact_relative_path: str | None,
    artifact_file_sha256: bytes | None,
    manifest_sha256s: tuple[bytes, ...],
) -> int:
    """Exact size of the deterministic V1 lineage-accounting wire."""

    return sum(
        (
            len(b"l2flow.query.canonical-lineage-accounting.v1\x00"),
            1,
            len(tier.value.encode("ascii")),
            len(source_id_sha256),
            len(source_snapshot_sha256),
            16,  # origin ingress sequence and Raw WAL end position
            _lineage_json_size(receipt.namespace.canonical_object()),
            _lineage_json_size(receipt.canonical_object()),
            _lineage_text_size(artifact_relative_path),
            1 + (0 if artifact_file_sha256 is None else len(artifact_file_sha256)),
            _lineage_hash_sequence_size(manifest_sha256s),
        )
    )


def _watermark_entry_wire(entry: FactorInputWatermark) -> dict[str, object]:
    return {
        "canonical_cursor": entry.canonical_cursor,
        "clock_epoch_algorithm": entry.clock_epoch_algorithm,
        "clock_epoch_digest": entry.clock_epoch_digest.hex(),
        "clock_epoch_label": entry.clock_epoch_label,
        "family": entry.family.value,
        "input_quality_flags": entry.input_quality_flags,
        "max_consumed_origin_wal_end_pos": entry.max_consumed_origin_wal_end_pos,
        "observed_raw_durable_wal_pos": entry.observed_raw_durable_wal_pos,
        "origin_capture_date": entry.origin_capture_date,
        "origin_stream_day_id": entry.origin_stream_day_id.hex(),
        "shard_id": entry.shard_id,
        "source_stream_id": entry.source_stream_id,
    }


def _factor_coverage_lineage_bytes(
    entry: FactorInputWatermark,
    namespace: SourceNamespace,
    receipt: SourceRangeReceipt | None,
) -> int:
    return sum(
        (
            len(b"l2flow.query.factor-coverage-accounting.v1\x00"),
            _lineage_json_size(_watermark_entry_wire(entry)),
            _lineage_json_size(namespace.canonical_object()),
            1,
            1 + (
                0 if receipt is None else _lineage_json_size(receipt.canonical_object())
            ),
        )
    )


def _factor_resolution_base_lineage_bytes(
    *,
    tier: StorageTier,
    source_id_sha256: bytes,
    source_snapshot_sha256: bytes,
    reference: SidecarReference,
    sidecar_descriptor_paths: tuple[str, ...],
    sidecar_manifest_sha256s: tuple[bytes, ...],
    artifact_relative_path: str | None,
    artifact_file_sha256: bytes | None,
    artifact_manifest_sha256s: tuple[bytes, ...],
) -> int:
    return sum(
        (
            len(b"l2flow.query.factor-resolution-accounting.v1\x00"),
            1,
            len(tier.value.encode("ascii")),
            len(source_id_sha256),
            len(source_snapshot_sha256),
            _lineage_json_size(reference.canonical_object()),
            32,  # sidecar file SHA-256
            8
            + sum(_lineage_text_size(item) for item in sidecar_descriptor_paths),
            _lineage_hash_sequence_size(sidecar_manifest_sha256s),
            _lineage_text_size(artifact_relative_path),
            1 + (0 if artifact_file_sha256 is None else len(artifact_file_sha256)),
            _lineage_hash_sequence_size(artifact_manifest_sha256s),
        )
    )


def _factor_observation_lineage_bytes(row: FactorQueryRow) -> int:
    model = row.archive_row
    return sum(
        (
            len(b"l2flow.query.factor-observation-accounting.v1\x00"),
            len(row.tier.value.encode("ascii")),
            len(row.source_id_sha256),
            len(model.run_id),
            8 * 5,
        )
    )


@dataclass(frozen=True, slots=True)
class CanonicalRawLocatorEvidence:
    """Exact source-local Raw locator/coverage evidence, not Raw byte proof."""

    tier: StorageTier
    source_id_sha256: bytes
    source_snapshot_sha256: bytes
    namespace: SourceNamespace
    origin_ingress_sequence: int
    origin_wal_end_pos: int
    receipt: SourceRangeReceipt = field(compare=False, hash=False, repr=False)
    artifact_relative_path: str | None = None
    artifact_file_sha256: bytes | None = None
    manifest_sha256s: tuple[bytes, ...] = ()

    @property
    def raw_record_locator_key(self) -> tuple[object, ...]:
        return (
            self.namespace.origin_capture_date,
            self.namespace.source_stream_id,
            self.namespace.origin_stream_day_id,
            self.origin_ingress_sequence,
            self.origin_wal_end_pos,
        )

    @property
    def evidence_key(self) -> tuple[object, ...]:
        return (
            _TIER_ORDER[self.tier],
            self.source_id_sha256,
            self.namespace.sort_key,
            self.receipt.begin_canonical_cursor,
            self.receipt.end_canonical_cursor,
            self.artifact_relative_path or "",
            self.manifest_sha256s,
        )


def resolve_canonical_raw_locator(
    row: CanonicalQueryRow,
    catalog: CanonicalLineageCatalog,
    *,
    _tracker: "_BudgetTracker | None" = None,
) -> CanonicalRawLocatorEvidence:
    if type(row) is not CanonicalQueryRow:
        raise QueryValidationError("row must be exact CanonicalQueryRow")
    if not isinstance(catalog, CanonicalLineageCatalog):
        raise QueryValidationError("catalog must be CanonicalLineageCatalog")
    artifact: ArtifactEntry | None = None
    manifests: tuple[bytes, ...] = ()
    artifact_path: str | None = None
    artifact_sha: bytes | None = None
    receipt_index: _ReceiptIndex
    if _tracker is not None:
        _tracker.check_deadline()
    if row.tier is StorageTier.COLD_PARQUET:
        binding = catalog._artifact_index.get(row.source_id_sha256)
        if binding is None:
            raise LineageResolutionError("cold Canonical row has no exact visible artifact")
        artifact = binding.artifact
        model = row.archive_row
        partition = artifact.partition
        if (
            partition.trade_date != model.header.trade_date
            or partition.bucket != model.bucket
            or partition.event_type != model.header.event_family
            or partition.market != _canonical_market(model)
        ):
            raise LineageResolutionError("Canonical row does not belong to its source partition")
        receipt_index = binding.receipt_index
        manifests = binding.manifest_sha256s
        artifact_path = artifact.relative_path
        artifact_sha = artifact.file_sha256
    elif row.tier is StorageTier.HOT_CANONICAL:
        source = _hot_canonical_by_id(catalog, row.source_id_sha256)
        if source is None:
            raise LineageResolutionError("hot Canonical row has no exact source snapshot")
        retained_row = catalog._hot_row_indexes[row.source_id_sha256].get(
            row.archive_row.identity_key
        )
        if retained_row != row.archive_row:
            raise LineageResolutionError("hot Canonical row is not in its frozen source")
        receipt_index = catalog._hot_receipt_indexes[row.source_id_sha256]
    else:
        raise LatestHistoryMixError("Latest-current cannot enter Canonical history")

    model = row.archive_row
    route = (
        model.header.trade_date,
        model.origin_capture_date,
        model.header.source_stream_id,
        model.origin_stream_day_id,
        InputFamily(model.header.event_family),
    )
    ranges = receipt_index.canonical_routes.get(route)
    receipt = None if ranges is None else ranges.unique_match(
        model.header.origin_ingress_sequence,
        model.header.origin_wal_end_pos,
        _tracker,
    )
    if receipt is None:
        raise LineageResolutionError(
            "Canonical row has no unique locator inside its own source receipts"
        )
    if _tracker is not None:
        _tracker.reserve_lineage(
            1,
            _canonical_locator_lineage_bytes(
                tier=row.tier,
                source_id_sha256=row.source_id_sha256,
                source_snapshot_sha256=row.source_snapshot_sha256,
                receipt=receipt,
                artifact_relative_path=artifact_path,
                artifact_file_sha256=artifact_sha,
                manifest_sha256s=manifests,
            ),
        )
    return CanonicalRawLocatorEvidence(
        tier=row.tier,
        source_id_sha256=row.source_id_sha256,
        source_snapshot_sha256=row.source_snapshot_sha256,
        namespace=receipt.namespace,
        origin_ingress_sequence=model.header.origin_ingress_sequence,
        origin_wal_end_pos=model.header.origin_wal_end_pos,
        receipt=receipt,
        artifact_relative_path=artifact_path,
        artifact_file_sha256=artifact_sha,
        manifest_sha256s=manifests,
    )


# Compatibility spelling keeps the important "locator" semantics explicit in
# the returned type and its documentation.
resolve_canonical_raw_lineage = resolve_canonical_raw_locator


def _entry_matches_namespace(
    entry: FactorInputWatermark,
    namespace: SourceNamespace,
) -> bool:
    return (
        entry.source_stream_id == namespace.source_stream_id
        and entry.origin_capture_date == namespace.origin_capture_date
        and entry.origin_stream_day_id == namespace.origin_stream_day_id
        and entry.family is namespace.family
        and entry.shard_id == namespace.shard_id
        and entry.clock_epoch_algorithm == namespace.clock_epoch_algorithm
        and entry.clock_epoch_digest == namespace.clock_epoch_digest
    )


@dataclass(frozen=True, slots=True)
class FactorInputCoverageEvidence:
    """Route identity plus optional positive-consumption coverage receipt."""

    entry: FactorInputWatermark
    namespace: SourceNamespace
    zero_consumption: bool
    receipt: SourceRangeReceipt | None = field(compare=False, hash=False, repr=False)


def _watermark_coverage_parts(
    entry: FactorInputWatermark,
    receipt_index: _ReceiptIndex,
    trade_date: int,
    tracker: "_BudgetTracker | None",
) -> tuple[SourceNamespace, bool, SourceRangeReceipt | None]:
    if tracker is not None:
        tracker.check_deadline()
    route = (
        trade_date,
        entry.source_stream_id,
        entry.origin_capture_date,
        entry.origin_stream_day_id,
        entry.family,
        entry.shard_id,
        entry.clock_epoch_algorithm,
        entry.clock_epoch_digest,
    )
    namespaces = receipt_index.factor_route_namespaces.get(route, ())
    if len(namespaces) != 1:
        raise LineageResolutionError(
            "factor watermark input has no unique complete SourceNamespace"
        )
    namespace = namespaces[0]
    if entry.canonical_cursor == 0 and entry.max_consumed_origin_wal_end_pos == 0:
        return namespace, True, None
    ranges = receipt_index.by_namespace[namespace]
    receipt = ranges.covering_cursor(entry.canonical_cursor)
    if receipt is None or not (
        receipt.min_origin_wal_end_pos
        <= entry.max_consumed_origin_wal_end_pos
        <= receipt.max_origin_wal_end_pos
    ):
        raise LineageResolutionError(
            "factor watermark input has no unique source-local coverage receipt"
        )
    return namespace, False, receipt


@dataclass(frozen=True, slots=True)
class FactorLineageResolution:
    tier: StorageTier
    source_id_sha256: bytes
    source_snapshot_sha256: bytes
    reference: SidecarReference
    sidecar_file_sha256: bytes
    sidecar_descriptor_paths: tuple[str, ...]
    sidecar_manifest_sha256s: tuple[bytes, ...]
    input_coverage: tuple[FactorInputCoverageEvidence, ...]
    watermark_set: FactorInputWatermarkSet = field(compare=False, hash=False, repr=False)
    artifact_relative_path: str | None = None
    artifact_file_sha256: bytes | None = None
    artifact_manifest_sha256s: tuple[bytes, ...] = ()

    @property
    def reference_key(self) -> tuple[bytes, int, int]:
        return self.reference.sort_key

    @property
    def evidence_key(self) -> tuple[object, ...]:
        return (
            _TIER_ORDER[self.tier],
            self.source_id_sha256,
            self.reference.sort_key,
            self.reference.full_map_sha256,
            self.artifact_relative_path or "",
            self.artifact_manifest_sha256s,
        )


def _factor_resolution_lineage_totals(
    resolution: FactorLineageResolution,
    tracker: "_BudgetTracker | None" = None,
) -> tuple[int, int]:
    byte_count = _factor_resolution_base_lineage_bytes(
        tier=resolution.tier,
        source_id_sha256=resolution.source_id_sha256,
        source_snapshot_sha256=resolution.source_snapshot_sha256,
        reference=resolution.reference,
        sidecar_descriptor_paths=resolution.sidecar_descriptor_paths,
        sidecar_manifest_sha256s=resolution.sidecar_manifest_sha256s,
        artifact_relative_path=resolution.artifact_relative_path,
        artifact_file_sha256=resolution.artifact_file_sha256,
        artifact_manifest_sha256s=resolution.artifact_manifest_sha256s,
    )
    for item in resolution.input_coverage:
        if tracker is not None:
            tracker.check_deadline()
        byte_count += _factor_coverage_lineage_bytes(
            item.entry,
            item.namespace,
            item.receipt,
        )
    return 1 + len(resolution.input_coverage), byte_count


def _factor_resolution_cache_key(row: FactorQueryRow) -> tuple[object, ...]:
    model = row.archive_row
    return (
        row.tier,
        row.source_id_sha256,
        model.run_id,
        model.watermark_table_generation,
        model.watermark_set_id,
        model.input_identity_sha256,
        model.trade_date,
        model.clock_epoch_algorithm,
        model.clock_epoch_digest,
        model.input_quality_flags,
        model.registry_version,
        model.registry_sha256,
    )


def resolve_factor_lineage(
    row: FactorQueryRow,
    catalog: FactorLineageCatalog,
    *,
    _tracker: "_BudgetTracker | None" = None,
) -> FactorLineageResolution:
    if type(row) is not FactorQueryRow:
        raise QueryValidationError("row must be exact FactorQueryRow")
    if not isinstance(catalog, FactorLineageCatalog):
        raise QueryValidationError("catalog must be FactorLineageCatalog")

    artifact_path: str | None = None
    artifact_sha: bytes | None = None
    artifact_manifests: tuple[bytes, ...] = ()
    live_binding: _LiveFactorBinding | None = None
    if _tracker is not None:
        _tracker.check_deadline()
    if row.tier is StorageTier.COLD_PARQUET:
        binding = catalog._artifact_index.get(row.source_id_sha256)
        if binding is None:
            raise LineageResolutionError("cold factor row has no exact visible artifact")
        artifact = binding.artifact
        model = row.archive_row
        if (
            artifact.partition.trade_date != model.trade_date
            or artifact.partition.bucket != model.bucket
            or artifact.partition.factor_version != model.factor_version
        ):
            raise LineageResolutionError("factor row does not belong to its source partition")
        reference_index = binding.factor_reference_index
        receipt_index = binding.receipt_index
        artifact_path = artifact.relative_path
        artifact_sha = artifact.file_sha256
        artifact_manifests = binding.manifest_sha256s
    else:
        live_binding = _live_factor_binding(
            catalog, row.tier, row.source_id_sha256
        )
        if live_binding is None:
            raise LineageResolutionError("factor row has no exact live source snapshot")
        if row.archive_row not in live_binding.rows:
            raise LineageResolutionError("factor row is not in its frozen live source")
        reference_index = live_binding.references
        receipt_index = live_binding.receipt_index

    namespace = SidecarNamespace(
        run_id=row.archive_row.run_id,
        table_generation=row.archive_row.watermark_table_generation,
    )
    reference = reference_index.get(
        (namespace, row.archive_row.watermark_set_id)
    )
    if reference is None:
        raise LineageResolutionError(
            "factor row attempted to borrow another source artifact's reference"
        )
    if row.tier is StorageTier.COLD_PARQUET:
        sidecar_binding = catalog._sidecar_index.get(namespace)
        if sidecar_binding is None or not sidecar_binding.manifest_sha256s:
            raise LineageResolutionError(
                "cold factor row references no manifest-described sidecar"
            )
        selected_sidecar = sidecar_binding.sidecar
        sidecar_file_sha256 = sidecar_binding.file_sha256
        descriptor_paths = sidecar_binding.descriptor_paths
        sidecar_manifests = sidecar_binding.manifest_sha256s
    else:
        assert live_binding is not None
        selected = live_binding.sidecars.get(namespace)
        if selected is None:
            raise LineageResolutionError("live factor source has no exact sidecar")
        selected_sidecar, sidecar_file_sha256 = selected
        descriptor_paths = ()
        sidecar_manifests = ()
    # The catalog constructor has already validated this immutable reference
    # against the descriptor-bound sidecar and cached its exact full map.
    # Query execution therefore performs no uninterruptible O(E) re-hash.
    watermark = catalog._watermark_index.get(reference)
    if watermark is None:
        raise LineageResolutionError("factor watermark reference is unresolved")
    if _tracker is not None:
        entry_count = len(watermark.entries)
        _tracker.ensure_lineage_capacity(1 + entry_count)
        # Conservative abstract work units cover validation, deterministic
        # sizing, both route/coverage passes, and cached-total construction.
        _tracker.consume_lineage_work(1 + 6 * entry_count)
    if (
        watermark.trade_date != row.archive_row.trade_date
        or reference.input_identity_sha256 != row.archive_row.input_identity_sha256
    ):
        raise LineageResolutionError("factor row and complete watermark map disagree")
    aggregate_quality = 0
    for entry in watermark.entries:
        if _tracker is not None:
            _tracker.check_deadline()
        if (
            entry.clock_epoch_algorithm != row.archive_row.clock_epoch_algorithm
            or entry.clock_epoch_digest != row.archive_row.clock_epoch_digest
        ):
            raise LineageResolutionError(
                "watermark entry clock identity differs from factor row"
            )
        aggregate_quality |= entry.input_quality_flags
    if aggregate_quality != row.archive_row.input_quality_flags:
        raise LineageResolutionError("watermark quality OR differs from factor row")
    lineage_bytes = _factor_resolution_base_lineage_bytes(
        tier=row.tier,
        source_id_sha256=row.source_id_sha256,
        source_snapshot_sha256=row.source_snapshot_sha256,
        reference=reference,
        sidecar_descriptor_paths=descriptor_paths,
        sidecar_manifest_sha256s=sidecar_manifests,
        artifact_relative_path=artifact_path,
        artifact_file_sha256=artifact_sha,
        artifact_manifest_sha256s=artifact_manifests,
    )
    # First coverage pass validates and sizes all nested evidence without
    # retaining an O(E) intermediate collection.
    for entry in watermark.entries:
        namespace_value, _, receipt_value = _watermark_coverage_parts(
            entry,
            receipt_index,
            watermark.trade_date,
            _tracker,
        )
        if (
            namespace_value.registry_version != row.archive_row.registry_version
            or namespace_value.registry_sha256 != row.archive_row.registry_sha256
        ):
            raise LineageResolutionError(
                "source receipt registry differs from factor row"
            )
        lineage_bytes += _factor_coverage_lineage_bytes(
            entry,
            namespace_value,
            receipt_value,
        )
    if _tracker is not None:
        _tracker.reserve_lineage(1 + len(watermark.entries), lineage_bytes)

    # Only after the exact entry/byte reservation succeeds may nested coverage
    # objects be allocated.
    coverage_values: list[FactorInputCoverageEvidence] = []
    for entry in watermark.entries:
        namespace_value, zero_consumption, receipt_value = (
            _watermark_coverage_parts(
                entry,
                receipt_index,
                watermark.trade_date,
                _tracker,
            )
        )
        coverage_values.append(
            FactorInputCoverageEvidence(
                entry=entry,
                namespace=namespace_value,
                zero_consumption=zero_consumption,
                receipt=receipt_value,
            )
        )
    coverage = tuple(coverage_values)
    return FactorLineageResolution(
        tier=row.tier,
        source_id_sha256=row.source_id_sha256,
        source_snapshot_sha256=row.source_snapshot_sha256,
        reference=reference,
        sidecar_file_sha256=sidecar_file_sha256,
        sidecar_descriptor_paths=descriptor_paths,
        sidecar_manifest_sha256s=sidecar_manifests,
        input_coverage=coverage,
        watermark_set=watermark,
        artifact_relative_path=artifact_path,
        artifact_file_sha256=artifact_sha,
        artifact_manifest_sha256s=artifact_manifests,
    )


@dataclass(frozen=True, slots=True)
class FactorReplayObservation:
    tier: StorageTier
    source_id_sha256: bytes
    run_id: bytes
    table_generation: int
    watermark_set_id: int
    clock_epoch_label: int
    calculation_latency_ns: int
    lineage: FactorLineageResolution = field(compare=False, hash=False)

    @property
    def sort_key(self) -> tuple[object, ...]:
        return (
            _TIER_ORDER[self.tier],
            self.source_id_sha256,
            self.table_generation,
            self.run_id,
            self.watermark_set_id,
            self.clock_epoch_label,
            self.calculation_latency_ns,
            self.lineage.sidecar_file_sha256,
        )


@dataclass(frozen=True, slots=True)
class MergedCanonicalRow:
    archive_row: CanonicalArchiveRow
    provenance: tuple[StorageTier, ...]
    source_ids: tuple[bytes, ...]
    raw_locator_evidence: tuple[CanonicalRawLocatorEvidence, ...]
    duplicate_count: int


@dataclass(frozen=True, slots=True)
class MergedFactorRow:
    archive_row: FactorHistoryRow
    semantic_content_sha256: bytes
    provenance: tuple[StorageTier, ...]
    source_ids: tuple[bytes, ...]
    lineage_references: tuple[FactorLineageResolution, ...]
    observations: tuple[FactorReplayObservation, ...]
    duplicate_count: int


@dataclass(frozen=True, slots=True)
class QueryStats:
    scanned_sources: int
    scanned_rows: int
    scanned_bytes: int
    emitted_rows: int
    deduplicated_rows: int
    lineage_entries: int = 0
    lineage_bytes: int = 0
    lineage_work: int = 0


@dataclass(frozen=True, slots=True)
class CanonicalQueryResult:
    snapshot: FixedQuerySnapshot
    rows: tuple[MergedCanonicalRow, ...]
    stats: QueryStats
    current_only: bool = field(init=False, default=False)


@dataclass(frozen=True, slots=True)
class FactorQueryResult:
    snapshot: FixedQuerySnapshot
    rows: tuple[MergedFactorRow, ...]
    stats: QueryStats
    current_only: bool = field(init=False, default=False)


@dataclass(frozen=True, slots=True)
class LatestCurrentQueryResult:
    snapshot: FixedQuerySnapshot
    rows: tuple[MergedFactorRow, ...]
    stats: QueryStats
    current_only: bool = field(init=False, default=True)


class _BudgetTracker:
    def __init__(self, budget: QueryBudget, clock_ns: Callable[[], int]) -> None:
        if not isinstance(budget, QueryBudget):
            raise QueryValidationError("budget must be QueryBudget")
        if not callable(clock_ns):
            raise QueryValidationError("clock_ns must be callable")
        self.budget = budget
        self.clock_ns = clock_ns
        self.sources: set[bytes] = set()
        self.rows = 0
        self.bytes = 0
        self.lineage_entries = 0
        self.lineage_bytes = 0
        self.lineage_work = 0
        self.check_deadline()

    def check_deadline(self) -> None:
        now = self.clock_ns()
        _uint(now, 64, "clock_ns result")
        if now >= self.budget.deadline_monotonic_ns:
            raise QueryBudgetExceeded("query deadline exhausted")

    def consume(self, byte_count: int, source_id: bytes) -> None:
        _uint(byte_count, 64, "scan byte_count", positive=True)
        _fixed_bytes(source_id, 32, "source_id_sha256")
        self.reserve_source(source_id)
        self.check_deadline()
        if self.rows >= self.budget.max_scanned_rows:
            raise QueryBudgetExceeded("query scanned-row budget exhausted")
        if self.bytes > self.budget.max_bytes - byte_count:
            raise QueryBudgetExceeded("query decoded-byte budget exhausted")
        self.rows += 1
        self.bytes += byte_count

    def reserve_source(self, source_id: bytes) -> None:
        _fixed_bytes(source_id, 32, "source_id_sha256")
        self.check_deadline()
        if source_id not in self.sources and len(self.sources) >= self.budget.max_sources:
            raise QueryBudgetExceeded("query source budget exhausted")
        self.sources.add(source_id)

    def reserve_lineage(self, entry_count: int, byte_count: int) -> None:
        _uint(entry_count, 64, "lineage entry_count", positive=True)
        _uint(byte_count, 64, "lineage byte_count", positive=True)
        self.check_deadline()
        entry_limit = self.budget.max_lineage_entries
        byte_limit = self.budget.max_lineage_bytes
        assert entry_limit is not None and byte_limit is not None
        if self.lineage_entries > entry_limit - entry_count:
            raise QueryBudgetExceeded("query lineage-entry budget exhausted")
        if self.lineage_bytes > byte_limit - byte_count:
            raise QueryBudgetExceeded("query lineage-byte budget exhausted")
        self.lineage_entries += entry_count
        self.lineage_bytes += byte_count

    def ensure_lineage_capacity(self, entry_count: int) -> None:
        """Fail before any nested lineage objects or expensive entry hashes."""

        _uint(entry_count, 64, "lineage preflight entry_count", positive=True)
        self.check_deadline()
        entry_limit = self.budget.max_lineage_entries
        assert entry_limit is not None
        if self.lineage_entries > entry_limit - entry_count:
            raise QueryBudgetExceeded("query lineage-entry budget exhausted")

    def consume_lineage_work(self, count: int) -> None:
        _uint(count, 64, "lineage work count", positive=True)
        self.check_deadline()
        limit = self.budget.max_lineage_work
        assert limit is not None
        if self.lineage_work > limit - count:
            raise QueryBudgetExceeded("query lineage-work budget exhausted")
        self.lineage_work += count

    def check_results(self, count: int) -> None:
        self.check_deadline()
        if count > self.budget.max_result_rows:
            raise QueryBudgetExceeded("query result-row budget exhausted")


def _observe_snapshot(
    snapshot: FixedQuerySnapshot,
    snapshot_observer: Callable[[], FixedQuerySnapshot],
) -> None:
    if not isinstance(snapshot, FixedQuerySnapshot):
        raise QueryValidationError("snapshot must be FixedQuerySnapshot")
    if not callable(snapshot_observer):
        raise QueryValidationError("snapshot_observer must be callable")
    observed = snapshot_observer()
    if not isinstance(observed, FixedQuerySnapshot):
        raise QueryValidationError("snapshot observer returned the wrong type")
    if snapshot != observed:
        raise QuerySnapshotChanged("fixed query snapshot changed")


def _validate_canonical_catalog(
    snapshot: FixedQuerySnapshot,
    catalog: CanonicalLineageCatalog,
) -> None:
    if not isinstance(catalog, CanonicalLineageCatalog):
        raise QueryValidationError("lineage catalog must be CanonicalLineageCatalog")
    if (
        snapshot.cold_manifest_set_sha256 != catalog.manifest_set_sha256
        or snapshot.hot_frontier_sha256 != catalog.hot_frontier_sha256
        or snapshot.canonical_lineage_catalog_sha256 != catalog.catalog_sha256
    ):
        raise QuerySnapshotChanged("Canonical evidence is not fixed-snapshot bound")


def _validate_factor_catalog(
    snapshot: FixedQuerySnapshot,
    catalog: FactorLineageCatalog,
    *,
    latest: bool,
) -> None:
    if not isinstance(catalog, FactorLineageCatalog):
        raise QueryValidationError("lineage catalog must be FactorLineageCatalog")
    if (
        snapshot.cold_manifest_set_sha256 != catalog.manifest_set_sha256
        or snapshot.hot_frontier_sha256 != catalog.hot_frontier_sha256
        or snapshot.factor_lineage_catalog_sha256 != catalog.catalog_sha256
    ):
        raise QuerySnapshotChanged("factor evidence is not fixed-snapshot bound")
    if latest and (
        snapshot.latest_generation_sha256 is None
        or snapshot.latest_generation_sha256 != catalog.latest_generation_sha256
    ):
        raise QuerySnapshotChanged("Latest generation is not fixed-snapshot bound")


def _scan_rows(
    values: Iterable[Union[CanonicalQueryRow, FactorQueryRow]],
    expected_type: type,
    allowed_tiers: tuple[StorageTier, ...],
    snapshot: FixedQuerySnapshot,
    tracker: _BudgetTracker,
) -> list[Union[CanonicalQueryRow, FactorQueryRow]]:
    output: list[Union[CanonicalQueryRow, FactorQueryRow]] = []
    for row in _iter_values(values, "rows"):
        if type(row) is not expected_type:
            raise QueryValidationError(f"row must be exact {expected_type.__name__}")
        if row.tier not in allowed_tiers:
            if row.tier is StorageTier.LATEST_CURRENT:
                raise LatestHistoryMixError("Latest-current rows cannot enter history")
            raise QueryValidationError("row is in the wrong storage tier")
        if row.source_snapshot_sha256 != snapshot.expected_tier_digest(row.tier):
            raise QuerySnapshotChanged("row is not bound to the fixed tier snapshot")
        tracker.consume(row.scan_bytes, row.source_id_sha256)
        output.append(row)
    return output


def merge_canonical_history(
    cold_rows: Iterable[CanonicalQueryRow],
    hot_sources: Iterable[HotCanonicalSource],
    *,
    snapshot: FixedQuerySnapshot,
    snapshot_observer: Callable[[], FixedQuerySnapshot],
    lineage_catalog: CanonicalLineageCatalog,
    budget: QueryBudget,
    clock_ns: Callable[[], int] = time.monotonic_ns,
) -> CanonicalQueryResult:
    _observe_snapshot(snapshot, snapshot_observer)
    _validate_canonical_catalog(snapshot, lineage_catalog)
    tracker = _BudgetTracker(budget, clock_ns)
    scanned = _scan_rows(
        cold_rows,
        CanonicalQueryRow,
        (StorageTier.COLD_PARQUET,),
        snapshot,
        tracker,
    )
    seen_hot_source_ids: set[bytes] = set()
    for candidate in _iter_values(hot_sources, "hot_sources"):
        tracker.check_deadline()
        if type(candidate) is not HotCanonicalSource:
            raise QueryValidationError("hot_sources must contain exact HotCanonicalSource")
        source = candidate
        if source.source_id_sha256 in seen_hot_source_ids:
            raise QueryValidationError("hot source repeats in one query")
        seen_hot_source_ids.add(source.source_id_sha256)
        retained = _hot_canonical_by_id(lineage_catalog, source.source_id_sha256)
        if retained != source:
            raise QuerySnapshotChanged("hot source is not in the fixed catalog")
        tracker.reserve_source(source.source_id_sha256)
        scanned.extend(
            _scan_rows(
                (
                    CanonicalQueryRow._from_live_source(
                        row,
                        source_snapshot_sha256=source.hot_frontier_sha256,
                        source_id_sha256=source.source_id_sha256,
                    )
                    for row in source.rows
                ),
                CanonicalQueryRow,
                (StorageTier.HOT_CANONICAL,),
                snapshot,
                tracker,
            )
        )
    _observe_snapshot(snapshot, snapshot_observer)
    grouped: dict[tuple[int, bytes, int, int, int, int], list[CanonicalQueryRow]] = {}
    for generic in scanned:
        tracker.check_deadline()
        row = generic
        assert isinstance(row, CanonicalQueryRow)
        grouped.setdefault(row.key, []).append(row)
    tracker.check_results(len(grouped))

    merged: list[MergedCanonicalRow] = []
    locator_cache: dict[
        tuple[object, ...], CanonicalRawLocatorEvidence
    ] = {}
    for duplicates in grouped.values():
        tracker.check_deadline()
        first = duplicates[0]
        first_model = first.archive_row
        evidence: list[CanonicalRawLocatorEvidence] = []
        for candidate in duplicates:
            tracker.check_deadline()
            if (
                candidate.archive_row.record_sha256 != first_model.record_sha256
                or candidate.archive_row.record_bytes != first_model.record_bytes
            ):
                raise QueryConflictError(
                    "one complete Canonical key has different exact record bytes"
                )
            locator_key = (
                candidate.tier,
                candidate.source_id_sha256,
                candidate.key,
                candidate.archive_row.record_sha256,
            )
            locator = locator_cache.get(locator_key)
            if locator is None:
                locator = resolve_canonical_raw_locator(
                    candidate,
                    lineage_catalog,
                    _tracker=tracker,
                )
                locator_cache[locator_key] = locator
            evidence.append(locator)
        if len({item.raw_record_locator_key for item in evidence}) != 1:
            raise QueryConflictError("Canonical duplicates resolve to different Raw locators")
        unique_evidence = {
            item.evidence_key: item for item in evidence
        }
        merged.append(
            MergedCanonicalRow(
                archive_row=first_model,
                provenance=tuple(
                    sorted({item.tier for item in duplicates}, key=_TIER_ORDER.__getitem__)
                ),
                source_ids=tuple(sorted({item.source_id_sha256 for item in duplicates})),
                raw_locator_evidence=tuple(
                    unique_evidence[key] for key in sorted(unique_evidence)
                ),
                duplicate_count=len(duplicates) - 1,
            )
        )
    merged.sort(key=lambda item: item.archive_row.total_sort_key)
    tracker.check_deadline()
    _observe_snapshot(snapshot, snapshot_observer)
    return CanonicalQueryResult(
        snapshot=snapshot,
        rows=tuple(merged),
        stats=QueryStats(
            scanned_sources=len(tracker.sources),
            scanned_rows=tracker.rows,
            scanned_bytes=tracker.bytes,
            emitted_rows=len(merged),
            deduplicated_rows=tracker.rows - len(merged),
            lineage_entries=tracker.lineage_entries,
            lineage_bytes=tracker.lineage_bytes,
            lineage_work=tracker.lineage_work,
        ),
    )


def _factor_semantic_tuple(row: FactorHistoryRow) -> tuple[object, ...]:
    return (
        row.factor_id,
        row.factor_version,
        row.factor_config_sha256,
        row.factor_code_sha256,
        row.state_schema_version,
        row.state_schema_sha256,
        row.trade_date,
        row.registry_version,
        row.registry_sha256,
        row.instrument_id,
        row.asof_ns,
        row.value_bits,
        row.value_valid,
        row.input_identity_sha256,
        row.clock_epoch_algorithm,
        row.clock_epoch_digest,
        row.input_quality_flags,
        row.implementation_status,
        row.numeric_dtype,
    )


def _merge_factor_scanned(
    scanned: list[Union[CanonicalQueryRow, FactorQueryRow]],
    catalog: FactorLineageCatalog,
    tracker: _BudgetTracker,
    *,
    use_current_key: bool,
) -> list[MergedFactorRow]:
    grouped: dict[object, list[FactorQueryRow]] = {}
    for generic in scanned:
        tracker.check_deadline()
        row = generic
        assert isinstance(row, FactorQueryRow)
        key: object = row.current_key if use_current_key else row.key
        grouped.setdefault(key, []).append(row)

    # Enforce the output cardinality contract before allocating merged rows,
    # replay observations, or lineage tuples for those rows.
    tracker.check_results(len(grouped))
    merged: list[MergedFactorRow] = []
    reference_bindings: dict[tuple[bytes, int, int], tuple[bytes, bytes]] = {}
    resolution_cache: dict[
        tuple[object, ...], tuple[FactorLineageResolution, tuple[int, int]]
    ] = {}
    for duplicates in grouped.values():
        tracker.check_deadline()
        first = duplicates[0]
        semantic = _factor_semantic_tuple(first.archive_row)
        for candidate in duplicates[1:]:
            tracker.check_deadline()
            if use_current_key and candidate.key != first.key:
                raise QueryConflictError(
                    "one fixed Latest slot identity has multiple historical keys"
                )
            if (
                candidate.archive_row.semantic_content_sha256
                != first.archive_row.semantic_content_sha256
                or _factor_semantic_tuple(candidate.archive_row) != semantic
            ):
                raise QueryConflictError(
                    "one factor key has different stable semantic content"
                )
        representative = min(
            (item.archive_row for item in duplicates),
            key=lambda item: item.reference_tie_break_key,
        )
        observations: list[FactorReplayObservation] = []
        lineages: dict[tuple[object, ...], FactorLineageResolution] = {}
        for item in duplicates:
            tracker.check_deadline()
            model = item.archive_row
            # Reserve the per-row replay observation before resolving or
            # constructing any nested lineage for that row.
            tracker.reserve_lineage(
                1,
                _factor_observation_lineage_bytes(item),
            )
            _validate_factor_row_catalog_membership(item, catalog)
            cache_key = _factor_resolution_cache_key(item)
            cached_resolution = resolution_cache.get(cache_key)
            resolution_was_built = cached_resolution is None
            if cached_resolution is None:
                resolution = resolve_factor_lineage(
                    item,
                    catalog,
                    _tracker=tracker,
                )
                tracker.check_deadline()
                resolution_totals = _factor_resolution_lineage_totals(
                    resolution,
                    tracker,
                )
                tracker.check_deadline()
                resolution_cache[cache_key] = (
                    resolution,
                    resolution_totals,
                )
            else:
                resolution, resolution_totals = cached_resolution
            binding = (
                resolution.reference.input_identity_sha256,
                resolution.reference.full_map_sha256,
            )
            previous = reference_bindings.get(resolution.reference_key)
            if previous is not None and previous != binding:
                raise QueryConflictError(
                    "one run-local watermark reference maps differently"
                )
            reference_bindings[resolution.reference_key] = binding
            if resolution.evidence_key not in lineages:
                # A cached immutable resolution may be shared physically, but
                # each emitted row carries its own logical lineage occurrence.
                if not resolution_was_built:
                    tracker.reserve_lineage(*resolution_totals)
                lineages[resolution.evidence_key] = resolution
            observations.append(
                FactorReplayObservation(
                    tier=item.tier,
                    source_id_sha256=item.source_id_sha256,
                    run_id=model.run_id,
                    table_generation=model.watermark_table_generation,
                    watermark_set_id=model.watermark_set_id,
                    clock_epoch_label=model.clock_epoch_label,
                    calculation_latency_ns=model.calculation_latency_ns,
                    lineage=resolution,
                )
            )
        observations.sort(key=lambda item: item.sort_key)
        merged.append(
            MergedFactorRow(
                archive_row=representative,
                semantic_content_sha256=representative.semantic_content_sha256,
                provenance=tuple(
                    sorted({item.tier for item in duplicates}, key=_TIER_ORDER.__getitem__)
                ),
                source_ids=tuple(sorted({item.source_id_sha256 for item in duplicates})),
                lineage_references=tuple(lineages[key] for key in sorted(lineages)),
                observations=tuple(observations),
                duplicate_count=len(duplicates) - 1,
            )
        )
    merged.sort(key=lambda item: item.archive_row.total_sort_key)
    return merged


def merge_factor_history(
    cold_rows: Iterable[FactorQueryRow],
    hot_sources: Iterable[FactorLiveSource],
    *,
    snapshot: FixedQuerySnapshot,
    snapshot_observer: Callable[[], FixedQuerySnapshot],
    lineage_catalog: FactorLineageCatalog,
    budget: QueryBudget,
    clock_ns: Callable[[], int] = time.monotonic_ns,
) -> FactorQueryResult:
    _observe_snapshot(snapshot, snapshot_observer)
    _validate_factor_catalog(snapshot, lineage_catalog, latest=False)
    tracker = _BudgetTracker(budget, clock_ns)
    scanned = _scan_rows(
        cold_rows,
        FactorQueryRow,
        (StorageTier.COLD_PARQUET,),
        snapshot,
        tracker,
    )
    seen_hot_source_ids: set[bytes] = set()
    for candidate in _iter_values(hot_sources, "factor hot_sources"):
        tracker.check_deadline()
        if (
            type(candidate) is not FactorLiveSource
            or candidate.tier is not StorageTier.HOT_CANONICAL
        ):
            raise QueryValidationError(
                "factor hot_sources must contain exact Hot FactorLiveSource"
            )
        source = candidate
        if source.source_id_sha256 in seen_hot_source_ids:
            raise QueryValidationError("factor hot source repeats in one query")
        seen_hot_source_ids.add(source.source_id_sha256)
        retained = _live_factor_by_id(
            lineage_catalog,
            StorageTier.HOT_CANONICAL,
            source.source_id_sha256,
        )
        if retained != source:
            raise QuerySnapshotChanged("factor hot source is not in the fixed catalog")
        tracker.reserve_source(source.source_id_sha256)
        scanned.extend(
            _scan_rows(
                (
                    FactorQueryRow._from_live_source(
                        row,
                        tier=StorageTier.HOT_CANONICAL,
                        source_snapshot_sha256=source.source_snapshot_sha256,
                        source_id_sha256=source.source_id_sha256,
                    )
                    for row in source.rows
                ),
                FactorQueryRow,
                (StorageTier.HOT_CANONICAL,),
                snapshot,
                tracker,
            )
        )
    _observe_snapshot(snapshot, snapshot_observer)
    merged = _merge_factor_scanned(
        scanned,
        lineage_catalog,
        tracker,
        use_current_key=False,
    )
    tracker.check_results(len(merged))
    _observe_snapshot(snapshot, snapshot_observer)
    return FactorQueryResult(
        snapshot=snapshot,
        rows=tuple(merged),
        stats=QueryStats(
            scanned_sources=len(tracker.sources),
            scanned_rows=tracker.rows,
            scanned_bytes=tracker.bytes,
            emitted_rows=len(merged),
            deduplicated_rows=tracker.rows - len(merged),
            lineage_entries=tracker.lineage_entries,
            lineage_bytes=tracker.lineage_bytes,
            lineage_work=tracker.lineage_work,
        ),
    )


def query_latest_current(
    sources: Iterable[FactorLiveSource],
    *,
    snapshot: FixedQuerySnapshot,
    snapshot_observer: Callable[[], FixedQuerySnapshot],
    lineage_catalog: FactorLineageCatalog,
    budget: QueryBudget,
    clock_ns: Callable[[], int] = time.monotonic_ns,
) -> LatestCurrentQueryResult:
    _observe_snapshot(snapshot, snapshot_observer)
    snapshot.expected_tier_digest(StorageTier.LATEST_CURRENT)
    _validate_factor_catalog(snapshot, lineage_catalog, latest=True)
    tracker = _BudgetTracker(budget, clock_ns)
    seen_source_ids: set[bytes] = set()
    scanned: list[Union[CanonicalQueryRow, FactorQueryRow]] = []
    for candidate in _iter_values(sources, "Latest sources"):
        tracker.check_deadline()
        if (
            type(candidate) is not FactorLiveSource
            or candidate.tier is not StorageTier.LATEST_CURRENT
        ):
            raise QueryValidationError(
                "Latest sources must contain exact current FactorLiveSource"
            )
        source = candidate
        if source.source_id_sha256 in seen_source_ids:
            raise QueryValidationError("Latest source repeats in one query")
        seen_source_ids.add(source.source_id_sha256)
        retained = _live_factor_by_id(
            lineage_catalog,
            StorageTier.LATEST_CURRENT,
            source.source_id_sha256,
        )
        if retained != source:
            raise QuerySnapshotChanged("Latest source is not in the fixed catalog")
        tracker.reserve_source(source.source_id_sha256)
        scanned.extend(
            _scan_rows(
                (
                    FactorQueryRow._from_live_source(
                        row,
                        tier=StorageTier.LATEST_CURRENT,
                        source_snapshot_sha256=source.source_snapshot_sha256,
                        source_id_sha256=source.source_id_sha256,
                    )
                    for row in source.rows
                ),
                FactorQueryRow,
                (StorageTier.LATEST_CURRENT,),
                snapshot,
                tracker,
            )
        )
    _observe_snapshot(snapshot, snapshot_observer)
    merged = _merge_factor_scanned(
        scanned,
        lineage_catalog,
        tracker,
        use_current_key=True,
    )
    tracker.check_results(len(merged))
    _observe_snapshot(snapshot, snapshot_observer)
    return LatestCurrentQueryResult(
        snapshot=snapshot,
        rows=tuple(merged),
        stats=QueryStats(
            scanned_sources=len(tracker.sources),
            scanned_rows=tracker.rows,
            scanned_bytes=tracker.bytes,
            emitted_rows=len(merged),
            deduplicated_rows=tracker.rows - len(merged),
            lineage_entries=tracker.lineage_entries,
            lineage_bytes=tracker.lineage_bytes,
            lineage_work=tracker.lineage_work,
        ),
    )


def merge_history(
    kind: HistoryKind,
    cold_rows: Iterable[Union[CanonicalQueryRow, FactorQueryRow]],
    hot_sources: Iterable[Union[HotCanonicalSource, FactorLiveSource]],
    *,
    snapshot: FixedQuerySnapshot,
    snapshot_observer: Callable[[], FixedQuerySnapshot],
    budget: QueryBudget,
    canonical_lineage_catalog: CanonicalLineageCatalog | None = None,
    factor_lineage_catalog: FactorLineageCatalog | None = None,
    clock_ns: Callable[[], int] = time.monotonic_ns,
) -> Union[CanonicalQueryResult, FactorQueryResult]:
    if kind is HistoryKind.CANONICAL:
        if canonical_lineage_catalog is None:
            raise QueryValidationError("Canonical merge requires its locator catalog")
        return merge_canonical_history(
            cold_rows,  # type: ignore[arg-type]
            hot_sources,  # type: ignore[arg-type]
            snapshot=snapshot,
            snapshot_observer=snapshot_observer,
            lineage_catalog=canonical_lineage_catalog,
            budget=budget,
            clock_ns=clock_ns,
        )
    if kind is HistoryKind.FACTOR:
        if factor_lineage_catalog is None:
            raise QueryValidationError("factor merge requires its watermark catalog")
        return merge_factor_history(
            cold_rows,  # type: ignore[arg-type]
            hot_sources,  # type: ignore[arg-type]
            snapshot=snapshot,
            snapshot_observer=snapshot_observer,
            lineage_catalog=factor_lineage_catalog,
            budget=budget,
            clock_ns=clock_ns,
        )
    raise QueryValidationError("kind must be HistoryKind.CANONICAL or FACTOR")


def _canonical_rows_from_validated_readback(
    readback: object,
    artifact: ArtifactEntry,
    *,
    snapshot: FixedQuerySnapshot,
    lineage_catalog: CanonicalLineageCatalog,
) -> tuple[CanonicalQueryRow, ...]:
    """Adapt backend-validated rows after exact manifest descriptor matching."""

    from .parquet_backend import ParquetReadResult

    _validate_canonical_catalog(snapshot, lineage_catalog)
    if type(readback) is not ParquetReadResult:
        raise QueryValidationError("readback must be exact ParquetReadResult")
    if type(artifact) is not ArtifactEntry or artifact.artifact_type is not DatasetKind.CANONICAL:
        raise QueryValidationError("artifact must be an exact Canonical ArtifactEntry")
    binding = lineage_catalog._artifact_index.get(artifact.file_sha256)
    if binding is None or binding.artifact != artifact:
        raise LineageResolutionError("artifact is not visible in the fixed manifest set")
    if readback.descriptor != artifact.parquet_descriptor():
        raise LineageResolutionError("Parquet readback descriptor differs from manifest")
    if len(readback.rows) != artifact.row_count or any(
        type(item) is not CanonicalArchiveRow for item in readback.rows
    ):
        raise LineageResolutionError("Canonical readback rows disagree with manifest type/count")
    return tuple(
        CanonicalQueryRow._from_cold_readback(
            item,
            source_snapshot_sha256=snapshot.cold_manifest_set_sha256,
            source_id_sha256=artifact.file_sha256,
        )
        for item in readback.rows
    )


def _factor_rows_from_validated_readback(
    readback: object,
    artifact: ArtifactEntry,
    *,
    snapshot: FixedQuerySnapshot,
    lineage_catalog: FactorLineageCatalog,
) -> tuple[FactorQueryRow, ...]:
    """Adapt backend-validated factor rows after exact descriptor matching."""

    from .parquet_backend import ParquetReadResult

    _validate_factor_catalog(snapshot, lineage_catalog, latest=False)
    if type(readback) is not ParquetReadResult:
        raise QueryValidationError("readback must be exact ParquetReadResult")
    if type(artifact) is not ArtifactEntry or artifact.artifact_type is not DatasetKind.FACTOR:
        raise QueryValidationError("artifact must be an exact factor ArtifactEntry")
    binding = lineage_catalog._artifact_index.get(artifact.file_sha256)
    if binding is None or binding.artifact != artifact:
        raise LineageResolutionError("artifact is not visible in the fixed manifest set")
    if readback.descriptor != artifact.parquet_descriptor():
        raise LineageResolutionError("Parquet readback descriptor differs from manifest")
    if len(readback.rows) != artifact.row_count or any(
        type(item) is not FactorHistoryRow for item in readback.rows
    ):
        raise LineageResolutionError("factor readback rows disagree with manifest type/count")
    return tuple(
        FactorQueryRow._from_cold_readback(
            item,
            source_snapshot_sha256=snapshot.cold_manifest_set_sha256,
            source_id_sha256=artifact.file_sha256,
        )
        for item in readback.rows
    )


def _open_store_subdirectory_no_symlinks(
    store_root: object,
    relative_parent_parts: tuple[str, ...],
) -> int:
    """Open an absolute store path and child directories one no-follow hop at a time."""

    try:
        raw_root = os.fspath(store_root)
    except TypeError as error:
        raise QueryValidationError("store_root must be an absolute path") from error
    if type(raw_root) is not str or not raw_root or "\x00" in raw_root:
        raise QueryValidationError("store_root must be a nonempty text path")
    if not raw_root.startswith("/"):
        raise QueryValidationError("store_root must be absolute")
    root_parts = [] if raw_root == "/" else raw_root.split("/")[1:]
    if any(part in ("", ".", "..") for part in root_parts):
        raise QueryValidationError("store_root must be a normalized absolute path")
    if any(part in ("", ".", "..") or "/" in part for part in relative_parent_parts):
        raise QueryValidationError("artifact parent path is not normalized")
    flags = os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_DIRECTORY", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        current = os.open("/", flags)
    except OSError as error:
        raise LineageResolutionError("cannot anchor filesystem root") from error
    try:
        for component in tuple(root_parts) + relative_parent_parts:
            try:
                child = os.open(component, flags, dir_fd=current)
            except OSError as error:
                raise LineageResolutionError(
                    "store/artifact directory cannot be opened without symlinks"
                ) from error
            os.close(current)
            current = child
            if not stat.S_ISDIR(os.fstat(current).st_mode):
                raise LineageResolutionError("store/artifact component is not a directory")
        return current
    except BaseException:
        os.close(current)
        raise


def _checked_clock(clock_ns: Callable[[], int], name: str) -> int:
    if not callable(clock_ns):
        raise QueryValidationError("clock_ns must be callable")
    value = clock_ns()
    _uint(value, 64, name)
    return value


def read_manifest_visible_canonical_rows(
    store_root: object,
    artifact_file_sha256: bytes,
    *,
    snapshot: FixedQuerySnapshot,
    lineage_catalog: CanonicalLineageCatalog,
    budget: QueryBudget,
    clock_ns: Callable[[], int] = time.monotonic_ns,
) -> tuple[CanonicalQueryRow, ...]:
    """Read one visible source with physical, decoded, and logical byte gates.

    This is deliberately a single-artifact primitive.  A merge applies its
    cumulative logical-wire budget, but independent calls do not share a
    process-memory or cumulative PyArrow decode session.
    """

    from pathlib import PurePosixPath
    from .parquet_backend import (
        MAX_PARQUET_DECODED_BYTES_V1,
        ParquetReadBudgetExceeded,
        read_canonical_parquet_part,
    )

    _validate_canonical_catalog(snapshot, lineage_catalog)
    if not isinstance(budget, QueryBudget):
        raise QueryValidationError("budget must be QueryBudget")
    _fixed_bytes(artifact_file_sha256, 32, "artifact_file_sha256")
    binding = lineage_catalog._artifact_index.get(artifact_file_sha256)
    if binding is None:
        raise LineageResolutionError("Canonical artifact is not visible")
    artifact = binding.artifact
    if artifact.row_count > budget.max_scanned_rows:
        raise QueryBudgetExceeded("artifact exceeds scanned-row budget before read")
    if artifact.file_size_bytes > budget.max_bytes:
        raise QueryBudgetExceeded("artifact exceeds physical-byte budget before read")
    event_type = {
        "snapshot": 1,
        "tick": 2,
        "quality": 3,
        "control": 4,
    }[artifact.partition.event_type]
    decoded_bytes = artifact.row_count * (
        24 + CANONICAL_RECORD_SIZE_BY_EVENT_TYPE_V1[event_type]
    )
    if decoded_bytes > budget.max_bytes:
        raise QueryBudgetExceeded("artifact exceeds decoded-byte budget before read")
    if _checked_clock(clock_ns, "clock_ns result") >= budget.deadline_monotonic_ns:
        raise QueryBudgetExceeded("query deadline exhausted before Parquet read")
    relative = PurePosixPath(artifact.relative_path)
    parent_parts = () if str(relative.parent) == "." else relative.parent.parts
    directory_fd = _open_store_subdirectory_no_symlinks(store_root, parent_parts)
    try:
        try:
            readback = read_canonical_parquet_part(
                f"/proc/self/fd/{directory_fd}/.",
                relative.name,
                expected_descriptor=artifact.parquet_descriptor(),
                maximum_decoded_bytes=min(
                    budget.max_bytes,
                    MAX_PARQUET_DECODED_BYTES_V1,
                ),
            )
        except ParquetReadBudgetExceeded as error:
            raise QueryBudgetExceeded(
                "artifact exceeds decoded-byte budget before decode"
            ) from error
    finally:
        os.close(directory_fd)
    if _checked_clock(clock_ns, "clock_ns result") >= budget.deadline_monotonic_ns:
        raise QueryBudgetExceeded("query deadline exhausted during Parquet read")
    rows = _canonical_rows_from_validated_readback(
        readback,
        artifact,
        snapshot=snapshot,
        lineage_catalog=lineage_catalog,
    )
    if sum(item.scan_bytes for item in rows) > budget.max_bytes:
        raise QueryBudgetExceeded("artifact exceeds decoded-byte budget")
    return rows


def read_manifest_visible_factor_rows(
    store_root: object,
    artifact_file_sha256: bytes,
    *,
    snapshot: FixedQuerySnapshot,
    lineage_catalog: FactorLineageCatalog,
    budget: QueryBudget,
    clock_ns: Callable[[], int] = time.monotonic_ns,
) -> tuple[FactorQueryRow, ...]:
    """Read one factor source with physical, decoded, and logical byte gates.

    This is a single-artifact primitive; separate calls do not share a
    cumulative PyArrow decode or process-memory budget.
    """

    from pathlib import PurePosixPath
    from .parquet_backend import (
        MAX_PARQUET_DECODED_BYTES_V1,
        ParquetReadBudgetExceeded,
        read_factor_parquet_part,
    )

    _validate_factor_catalog(snapshot, lineage_catalog, latest=False)
    if not isinstance(budget, QueryBudget):
        raise QueryValidationError("budget must be QueryBudget")
    _fixed_bytes(artifact_file_sha256, 32, "artifact_file_sha256")
    binding = lineage_catalog._artifact_index.get(artifact_file_sha256)
    if binding is None:
        raise LineageResolutionError("factor artifact is not visible")
    artifact = binding.artifact
    if artifact.row_count > budget.max_scanned_rows:
        raise QueryBudgetExceeded("artifact exceeds scanned-row budget before read")
    if artifact.file_size_bytes > budget.max_bytes:
        raise QueryBudgetExceeded("artifact exceeds physical-byte budget before read")
    # Conservative pre-read logical-scan upper bound.  It prevents a bounded
    # query from intentionally opening a part whose decoded rows cannot fit;
    # it does not claim to bound PyArrow's implementation memory overhead.
    factor_row_upper_bound = 301 + 4 * MAX_FACTOR_TEXT_BYTES_V1
    if artifact.row_count * factor_row_upper_bound > budget.max_bytes:
        raise QueryBudgetExceeded("artifact may exceed decoded-byte budget before read")
    if _checked_clock(clock_ns, "clock_ns result") >= budget.deadline_monotonic_ns:
        raise QueryBudgetExceeded("query deadline exhausted before Parquet read")
    relative = PurePosixPath(artifact.relative_path)
    parent_parts = () if str(relative.parent) == "." else relative.parent.parts
    directory_fd = _open_store_subdirectory_no_symlinks(store_root, parent_parts)
    try:
        try:
            readback = read_factor_parquet_part(
                f"/proc/self/fd/{directory_fd}/.",
                relative.name,
                expected_descriptor=artifact.parquet_descriptor(),
                maximum_decoded_bytes=min(
                    budget.max_bytes,
                    MAX_PARQUET_DECODED_BYTES_V1,
                ),
            )
        except ParquetReadBudgetExceeded as error:
            raise QueryBudgetExceeded(
                "artifact exceeds decoded-byte budget before decode"
            ) from error
    finally:
        os.close(directory_fd)
    if _checked_clock(clock_ns, "clock_ns result") >= budget.deadline_monotonic_ns:
        raise QueryBudgetExceeded("query deadline exhausted during Parquet read")
    rows = _factor_rows_from_validated_readback(
        readback,
        artifact,
        snapshot=snapshot,
        lineage_catalog=lineage_catalog,
    )
    if sum(item.scan_bytes for item in rows) > budget.max_bytes:
        raise QueryBudgetExceeded("artifact exceeds decoded-byte budget")
    return rows
