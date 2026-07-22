"""Deterministic, dry-run-only Phase 8 retention planning.

There is intentionally no unlink/rename API in this module.  Planning consumes
complete immutable evidence snapshots, records every gate separately, and
returns hashes that an external, separately reviewed executor could later bind
to.  Approval is a two-pass protocol: first build a proposal, then rebuild with
caller-authenticated approval evidence that names that exact proposal digest.
This module validates the binding and policy shape; it does not verify a
signature or authenticate an approver.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
import hashlib
import json
import re
from typing import Iterable


MAX_RETENTION_ARTIFACTS = 1_000_000
MAX_REGISTERED_CONSUMERS = 65_536
MAX_ACTIVE_REFERENCES = 1_000_000
MAX_SIDECAR_REFERENCES = 1_000_000
MAX_MANIFESTS_PER_ARTIFACT = 4_096
MAX_BACKUP_RECEIPTS = 4_096


class RetentionError(Exception):
    """Base class for retention planning failures."""


class RetentionValidationError(RetentionError, ValueError):
    """Retention evidence is malformed, ambiguous, or unbounded."""


class StaleInventoryError(RetentionError):
    """The caller's expected inventory digest is no longer current."""


def _uint(value: object, bits: int, name: str, *, positive: bool = False) -> int:
    if type(value) is not int:
        raise RetentionValidationError(f"{name} must be an integer")
    minimum = 1 if positive else 0
    maximum = (1 << bits) - 1
    if value < minimum or value > maximum:
        raise RetentionValidationError(f"{name} is outside uint{bits}")
    return value


def _fixed_bytes(
    value: object,
    width: int,
    name: str,
    *,
    nonzero: bool = True,
) -> bytes:
    if type(value) is not bytes or len(value) != width:
        raise RetentionValidationError(
            f"{name} must be exact immutable {width}-byte data"
        )
    if nonzero and not any(value):
        raise RetentionValidationError(f"{name} must not be all zero")
    return value


def _identifier(value: object, name: str) -> str:
    if (
        type(value) is not str
        or not value
        or len(value) > 160
        or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:+-]{0,159}", value) is None
    ):
        raise RetentionValidationError(f"{name} is not a stable bounded identifier")
    return value


def _hash_json(domain: bytes, value: object) -> bytes:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")
    return hashlib.sha256(domain + encoded).digest()


def _bounded_take(values: object, maximum: int, name: str) -> tuple[object, ...]:
    if isinstance(values, (str, bytes, bytearray, memoryview)):
        raise RetentionValidationError(f"{name} must be a typed iterable")
    try:
        iterator = iter(values)  # type: ignore[arg-type]
    except TypeError as error:
        raise RetentionValidationError(f"{name} must be a typed iterable") from error
    output: list[object] = []
    for item in iterator:
        if len(output) >= maximum:
            raise RetentionValidationError(f"{name} exceeds the hard bound")
        output.append(item)
    return tuple(output)


class ArtifactKind(str, Enum):
    RAW_SEGMENT = "raw_segment"
    CANONICAL_SEGMENT = "canonical_segment"
    PARQUET_PART = "parquet_part"
    WATERMARK_SIDECAR = "watermark_sidecar"
    MANIFEST = "manifest"


class ActiveReferenceKind(str, Enum):
    QUERY = "query"
    REPLAY = "replay"
    AUDIT = "audit"
    MANIFEST = "manifest"


class SidecarOwnerKind(str, Enum):
    HISTORY_ARTIFACT = "history_artifact"
    LATEST_CURRENT = "latest_current"


class RetentionGate(str, Enum):
    RETENTION_PERIOD = "retention_period"
    PUBLISHED_MANIFESTS = "published_manifests"
    DURABLE_CONSUMERS = "durable_consumers"
    INACTIVE_REFERENCES = "inactive_references"
    BACKUP_POLICY = "backup_policy"
    IMMUTABLE_PLAN_APPROVAL = "immutable_plan_approval"
    SIDECAR_REACHABILITY = "sidecar_reachability"


@dataclass(frozen=True, slots=True, order=True)
class RawWalScope:
    """Raw durable-WAL comparison scope preserved across validated takeover."""

    origin_capture_date: int
    source_stream_id: int
    origin_stream_day_id: bytes

    def __post_init__(self) -> None:
        _uint(
            self.origin_capture_date,
            32,
            "origin_capture_date",
            positive=True,
        )
        _uint(self.source_stream_id, 32, "source_stream_id", positive=True)
        _fixed_bytes(self.origin_stream_day_id, 16, "origin_stream_day_id")

    def canonical(self) -> dict[str, object]:
        return {
            "cursor_domain": "raw_wal",
            "origin_capture_date": self.origin_capture_date,
            "origin_stream_day_id": self.origin_stream_day_id.hex(),
            "source_stream_id": self.source_stream_id,
        }


@dataclass(frozen=True, slots=True, order=True)
class CanonicalCursorScope:
    """Generation/family/shard scope for a Canonical exclusive-next cursor."""

    origin_capture_date: int
    source_stream_id: int
    origin_stream_day_id: bytes
    origin_source_writer_instance: bytes
    origin_source_generation: int
    canonical_generation: int
    family: int
    shard_id: int

    def __post_init__(self) -> None:
        _uint(
            self.origin_capture_date,
            32,
            "origin_capture_date",
            positive=True,
        )
        _uint(self.source_stream_id, 32, "source_stream_id", positive=True)
        _fixed_bytes(self.origin_stream_day_id, 16, "origin_stream_day_id")
        _fixed_bytes(
            self.origin_source_writer_instance,
            16,
            "origin_source_writer_instance",
        )
        _uint(
            self.origin_source_generation,
            64,
            "origin_source_generation",
            positive=True,
        )
        _uint(
            self.canonical_generation,
            64,
            "canonical_generation",
            positive=True,
        )
        _uint(self.family, 16, "family", positive=True)
        _uint(self.shard_id, 32, "shard_id")

    def canonical(self) -> dict[str, object]:
        return {
            "canonical_generation": str(self.canonical_generation),
            "cursor_domain": "canonical",
            "family": self.family,
            "origin_capture_date": self.origin_capture_date,
            "origin_source_generation": str(self.origin_source_generation),
            "origin_source_writer_instance": self.origin_source_writer_instance.hex(),
            "origin_stream_day_id": self.origin_stream_day_id.hex(),
            "shard_id": self.shard_id,
            "source_stream_id": self.source_stream_id,
        }


@dataclass(frozen=True, slots=True)
class RetentionArtifact:
    artifact_id: str
    kind: ArtifactKind
    content_sha256: bytes
    retention_policy_sha256: bytes
    byte_size: int
    retain_until_ns: int
    required_manifest_sha256s: tuple[bytes, ...]
    scope: RawWalScope | CanonicalCursorScope | None = None
    begin_cursor: int | None = None
    end_cursor: int | None = None

    def __post_init__(self) -> None:
        _identifier(self.artifact_id, "artifact_id")
        if not isinstance(self.kind, ArtifactKind):
            raise RetentionValidationError("kind must be ArtifactKind")
        _fixed_bytes(self.content_sha256, 32, "content_sha256")
        _fixed_bytes(
            self.retention_policy_sha256,
            32,
            "retention_policy_sha256",
        )
        _uint(self.byte_size, 64, "byte_size", positive=True)
        _uint(self.retain_until_ns, 64, "retain_until_ns")
        if (
            type(self.required_manifest_sha256s) is not tuple
            or not self.required_manifest_sha256s
            or len(self.required_manifest_sha256s) > MAX_MANIFESTS_PER_ARTIFACT
        ):
            raise RetentionValidationError(
                "required_manifest_sha256s must be a bounded nonempty tuple"
            )
        manifests = tuple(
            sorted(
                {
                    _fixed_bytes(item, 32, "required manifest SHA-256")
                    for item in self.required_manifest_sha256s
                }
            )
        )
        if len(manifests) != len(self.required_manifest_sha256s):
            raise RetentionValidationError("required manifests must not repeat")
        object.__setattr__(self, "required_manifest_sha256s", manifests)
        cursor_fields = (self.scope, self.begin_cursor, self.end_cursor)
        if all(item is None for item in cursor_fields):
            if self.kind in (ArtifactKind.RAW_SEGMENT, ArtifactKind.CANONICAL_SEGMENT):
                raise RetentionValidationError("source segments require exact cursor scope")
        elif any(item is None for item in cursor_fields):
            raise RetentionValidationError("cursor scope and range must be all present or absent")
        else:
            if not isinstance(self.scope, (RawWalScope, CanonicalCursorScope)):
                raise RetentionValidationError(
                    "scope must be RawWalScope or CanonicalCursorScope"
                )
            assert self.begin_cursor is not None and self.end_cursor is not None
            _uint(self.begin_cursor, 64, "begin_cursor")
            _uint(self.end_cursor, 64, "end_cursor", positive=True)
            if self.begin_cursor >= self.end_cursor:
                raise RetentionValidationError("cursor coverage must be nonempty [begin,end)")
        if self.kind is ArtifactKind.RAW_SEGMENT and not isinstance(
            self.scope, RawWalScope
        ):
            raise RetentionValidationError("Raw segment requires exact RawWalScope")
        if self.kind is ArtifactKind.CANONICAL_SEGMENT and not isinstance(
            self.scope, CanonicalCursorScope
        ):
            raise RetentionValidationError(
                "Canonical segment requires exact CanonicalCursorScope"
            )
        if self.kind not in (
            ArtifactKind.RAW_SEGMENT,
            ArtifactKind.CANONICAL_SEGMENT,
        ) and any(item is not None for item in cursor_fields):
            raise RetentionValidationError(
                "non-segment artifacts cannot carry a consumer cursor domain"
            )

    def canonical(self) -> dict[str, object]:
        return {
            "artifact_id": self.artifact_id,
            "begin_cursor": None if self.begin_cursor is None else str(self.begin_cursor),
            "byte_size": str(self.byte_size),
            "content_sha256": self.content_sha256.hex(),
            "end_cursor": None if self.end_cursor is None else str(self.end_cursor),
            "kind": self.kind.value,
            "retention_policy_sha256": self.retention_policy_sha256.hex(),
            "required_manifest_sha256s": [
                value.hex() for value in self.required_manifest_sha256s
            ],
            "retain_until_ns": str(self.retain_until_ns),
            "scope": None if self.scope is None else self.scope.canonical(),
        }


@dataclass(frozen=True, slots=True)
class PublicationEvidence:
    artifact_id: str
    validated_manifest_sha256s: tuple[bytes, ...]
    complete: bool
    scope: RawWalScope | CanonicalCursorScope | None = None
    begin_cursor: int | None = None
    end_cursor: int | None = None

    def __post_init__(self) -> None:
        _identifier(self.artifact_id, "artifact_id")
        if type(self.validated_manifest_sha256s) is not tuple:
            raise RetentionValidationError("validated manifests must be a tuple")
        if len(self.validated_manifest_sha256s) > MAX_MANIFESTS_PER_ARTIFACT:
            raise RetentionValidationError("validated manifest evidence is unbounded")
        values = tuple(
            sorted(
                {
                    _fixed_bytes(item, 32, "validated manifest SHA-256")
                    for item in self.validated_manifest_sha256s
                }
            )
        )
        if len(values) != len(self.validated_manifest_sha256s):
            raise RetentionValidationError("validated manifests must not repeat")
        object.__setattr__(self, "validated_manifest_sha256s", values)
        if type(self.complete) is not bool:
            raise RetentionValidationError("publication completeness must be bool")
        cursor_fields = (self.scope, self.begin_cursor, self.end_cursor)
        if all(item is None for item in cursor_fields):
            pass
        elif any(item is None for item in cursor_fields):
            raise RetentionValidationError(
                "publication scope and range must be all present or absent"
            )
        else:
            if not isinstance(self.scope, (RawWalScope, CanonicalCursorScope)):
                raise RetentionValidationError("publication cursor scope is invalid")
            assert self.begin_cursor is not None and self.end_cursor is not None
            _uint(self.begin_cursor, 64, "publication begin_cursor")
            _uint(self.end_cursor, 64, "publication end_cursor", positive=True)
            if self.begin_cursor >= self.end_cursor:
                raise RetentionValidationError("publication coverage must be nonempty")

    def canonical(self) -> dict[str, object]:
        return {
            "artifact_id": self.artifact_id,
            "begin_cursor": (
                None if self.begin_cursor is None else str(self.begin_cursor)
            ),
            "complete": self.complete,
            "end_cursor": None if self.end_cursor is None else str(self.end_cursor),
            "scope": None if self.scope is None else self.scope.canonical(),
            "validated_manifest_sha256s": [item.hex() for item in self.validated_manifest_sha256s],
        }


@dataclass(frozen=True, slots=True)
class BackupEvidence:
    artifact_id: str
    source_sha256: bytes
    backup_policy_sha256: bytes
    verified_copy_receipt_sha256s: tuple[bytes, ...]
    complete: bool

    def __post_init__(self) -> None:
        _identifier(self.artifact_id, "artifact_id")
        _fixed_bytes(self.source_sha256, 32, "source_sha256")
        _fixed_bytes(self.backup_policy_sha256, 32, "backup_policy_sha256")
        if (
            type(self.verified_copy_receipt_sha256s) is not tuple
            or len(self.verified_copy_receipt_sha256s) > MAX_BACKUP_RECEIPTS
        ):
            raise RetentionValidationError("backup receipts must be a bounded tuple")
        receipts = tuple(
            sorted(
                {
                    _fixed_bytes(item, 32, "backup receipt SHA-256")
                    for item in self.verified_copy_receipt_sha256s
                }
            )
        )
        if len(receipts) != len(self.verified_copy_receipt_sha256s):
            raise RetentionValidationError("backup receipts must not repeat")
        object.__setattr__(self, "verified_copy_receipt_sha256s", receipts)
        if type(self.complete) is not bool:
            raise RetentionValidationError("backup completeness must be bool")

    def canonical(self) -> dict[str, object]:
        return {
            "artifact_id": self.artifact_id,
            "backup_policy_sha256": self.backup_policy_sha256.hex(),
            "complete": self.complete,
            "source_sha256": self.source_sha256.hex(),
            "verified_copy_receipt_sha256s": [
                item.hex() for item in self.verified_copy_receipt_sha256s
            ],
        }


@dataclass(frozen=True, slots=True, order=True)
class RegisteredConsumer:
    consumer_id: str
    scope: RawWalScope | CanonicalCursorScope

    def __post_init__(self) -> None:
        _identifier(self.consumer_id, "consumer_id")
        if not isinstance(self.scope, (RawWalScope, CanonicalCursorScope)):
            raise RetentionValidationError("consumer scope has the wrong cursor domain")


@dataclass(frozen=True, slots=True, order=True)
class ConsumerCheckpoint:
    consumer_id: str
    scope: RawWalScope | CanonicalCursorScope
    exclusive_next_cursor: int
    checkpoint_sha256: bytes

    def __post_init__(self) -> None:
        _identifier(self.consumer_id, "consumer_id")
        if not isinstance(self.scope, (RawWalScope, CanonicalCursorScope)):
            raise RetentionValidationError("checkpoint scope has the wrong cursor domain")
        _uint(self.exclusive_next_cursor, 64, "exclusive_next_cursor")
        _fixed_bytes(self.checkpoint_sha256, 32, "checkpoint_sha256")


@dataclass(frozen=True, slots=True)
class ConsumerRegistrySnapshot:
    registry_generation: int
    complete: bool
    registrations: tuple[RegisteredConsumer, ...]
    checkpoints: tuple[ConsumerCheckpoint, ...]
    registry_sha256: bytes = field(init=False)

    def __post_init__(self) -> None:
        _uint(
            self.registry_generation,
            64,
            "registry_generation",
            positive=True,
        )
        if type(self.complete) is not bool:
            raise RetentionValidationError("registry complete must be bool")
        if (
            type(self.registrations) is not tuple
            or type(self.checkpoints) is not tuple
            or len(self.registrations) > MAX_REGISTERED_CONSUMERS
            or len(self.checkpoints) > MAX_REGISTERED_CONSUMERS
        ):
            raise RetentionValidationError("consumer registry snapshot is unbounded")
        if any(not isinstance(item, RegisteredConsumer) for item in self.registrations):
            raise RetentionValidationError("registrations contain an invalid value")
        if any(not isinstance(item, ConsumerCheckpoint) for item in self.checkpoints):
            raise RetentionValidationError("checkpoints contain an invalid value")
        registrations = tuple(
            sorted(
                self.registrations,
                key=lambda item: (
                    item.consumer_id,
                    json.dumps(item.scope.canonical(), sort_keys=True),
                ),
            )
        )
        checkpoints = tuple(
            sorted(
                self.checkpoints,
                key=lambda item: (
                    item.consumer_id,
                    json.dumps(item.scope.canonical(), sort_keys=True),
                    item.exclusive_next_cursor,
                    item.checkpoint_sha256,
                ),
            )
        )
        registration_keys = [(item.consumer_id, item.scope) for item in registrations]
        checkpoint_keys = [(item.consumer_id, item.scope) for item in checkpoints]
        if len(registration_keys) != len(set(registration_keys)):
            raise RetentionValidationError("consumer registrations repeat")
        if len(checkpoint_keys) != len(set(checkpoint_keys)):
            raise RetentionValidationError("consumer checkpoints repeat")
        if not set(checkpoint_keys).issubset(set(registration_keys)):
            raise RetentionValidationError("checkpoint has no exact registered consumer scope")
        object.__setattr__(self, "registrations", registrations)
        object.__setattr__(self, "checkpoints", checkpoints)
        object.__setattr__(
            self,
            "registry_sha256",
            _hash_json(
                b"l2flow.retention.consumer-registry-snapshot.v1\x00",
                {
                    "checkpoints": [
                        {
                            "checkpoint_sha256": item.checkpoint_sha256.hex(),
                            "consumer_id": item.consumer_id,
                            "exclusive_next_cursor": str(item.exclusive_next_cursor),
                            "scope": item.scope.canonical(),
                        }
                        for item in checkpoints
                    ],
                    "complete": self.complete,
                    "registrations": [
                        {
                            "consumer_id": item.consumer_id,
                            "scope": item.scope.canonical(),
                        }
                        for item in registrations
                    ],
                    "registry_generation": str(self.registry_generation),
                },
            ),
        )

    def canonical(self) -> dict[str, object]:
        return {
            "checkpoints": [
                {
                    "checkpoint_sha256": item.checkpoint_sha256.hex(),
                    "consumer_id": item.consumer_id,
                    "exclusive_next_cursor": str(item.exclusive_next_cursor),
                    "scope": item.scope.canonical(),
                }
                for item in self.checkpoints
            ],
            "complete": self.complete,
            "registrations": [
                {"consumer_id": item.consumer_id, "scope": item.scope.canonical()}
                for item in self.registrations
            ],
            "registry_generation": str(self.registry_generation),
            "registry_sha256": self.registry_sha256.hex(),
        }


@dataclass(frozen=True, slots=True, order=True)
class ActiveReference:
    artifact_id: str
    kind: ActiveReferenceKind
    holder_id: str
    reference_sha256: bytes

    def __post_init__(self) -> None:
        _identifier(self.artifact_id, "artifact_id")
        if not isinstance(self.kind, ActiveReferenceKind):
            raise RetentionValidationError("reference kind must be ActiveReferenceKind")
        _identifier(self.holder_id, "holder_id")
        _fixed_bytes(self.reference_sha256, 32, "reference_sha256")


@dataclass(frozen=True, slots=True)
class ActiveReferenceSnapshot:
    complete: bool
    references: tuple[ActiveReference, ...]
    snapshot_sha256: bytes = field(init=False)

    def __post_init__(self) -> None:
        if type(self.complete) is not bool:
            raise RetentionValidationError("active-reference completeness must be bool")
        if (
            type(self.references) is not tuple
            or len(self.references) > MAX_ACTIVE_REFERENCES
            or any(not isinstance(item, ActiveReference) for item in self.references)
        ):
            raise RetentionValidationError("active references must be a bounded typed tuple")
        ordered = tuple(
            sorted(
                self.references,
                key=lambda item: (
                    item.artifact_id,
                    item.kind.value,
                    item.holder_id,
                    item.reference_sha256,
                ),
            )
        )
        if len(ordered) != len(set(ordered)):
            raise RetentionValidationError("active references repeat")
        object.__setattr__(self, "references", ordered)
        object.__setattr__(
            self,
            "snapshot_sha256",
            _hash_json(
                b"l2flow.retention.active-reference-snapshot.v1\x00",
                {
                    "complete": self.complete,
                    "references": [
                        {
                            "artifact_id": item.artifact_id,
                            "holder_id": item.holder_id,
                            "kind": item.kind.value,
                            "reference_sha256": item.reference_sha256.hex(),
                        }
                        for item in ordered
                    ],
                },
            ),
        )

    def canonical(self) -> dict[str, object]:
        return {
            "complete": self.complete,
            "references": [
                {
                    "artifact_id": item.artifact_id,
                    "holder_id": item.holder_id,
                    "kind": item.kind.value,
                    "reference_sha256": item.reference_sha256.hex(),
                }
                for item in self.references
            ],
            "snapshot_sha256": self.snapshot_sha256.hex(),
        }


@dataclass(frozen=True, slots=True, order=True)
class SidecarReference:
    sidecar_artifact_id: str
    owner_kind: SidecarOwnerKind
    owner_id: str
    reference_sha256: bytes

    def __post_init__(self) -> None:
        _identifier(self.sidecar_artifact_id, "sidecar_artifact_id")
        if not isinstance(self.owner_kind, SidecarOwnerKind):
            raise RetentionValidationError("owner_kind must be SidecarOwnerKind")
        _identifier(self.owner_id, "owner_id")
        _fixed_bytes(self.reference_sha256, 32, "sidecar reference SHA-256")


@dataclass(frozen=True, slots=True)
class SidecarReferenceSnapshot:
    complete: bool
    references: tuple[SidecarReference, ...]
    snapshot_sha256: bytes = field(init=False)

    def __post_init__(self) -> None:
        if type(self.complete) is not bool:
            raise RetentionValidationError("sidecar-reference completeness must be bool")
        if (
            type(self.references) is not tuple
            or len(self.references) > MAX_SIDECAR_REFERENCES
            or any(not isinstance(item, SidecarReference) for item in self.references)
        ):
            raise RetentionValidationError("sidecar references must be a bounded typed tuple")
        ordered = tuple(
            sorted(
                self.references,
                key=lambda item: (
                    item.sidecar_artifact_id,
                    item.owner_kind.value,
                    item.owner_id,
                    item.reference_sha256,
                ),
            )
        )
        if len(ordered) != len(set(ordered)):
            raise RetentionValidationError("sidecar references repeat")
        object.__setattr__(self, "references", ordered)
        object.__setattr__(
            self,
            "snapshot_sha256",
            _hash_json(
                b"l2flow.retention.sidecar-reference-snapshot.v1\x00",
                {
                    "complete": self.complete,
                    "references": [
                        {
                            "owner_id": item.owner_id,
                            "owner_kind": item.owner_kind.value,
                            "reference_sha256": item.reference_sha256.hex(),
                            "sidecar_artifact_id": item.sidecar_artifact_id,
                        }
                        for item in ordered
                    ],
                },
            ),
        )

    def canonical(self) -> dict[str, object]:
        return {
            "complete": self.complete,
            "references": [
                {
                    "owner_id": item.owner_id,
                    "owner_kind": item.owner_kind.value,
                    "reference_sha256": item.reference_sha256.hex(),
                    "sidecar_artifact_id": item.sidecar_artifact_id,
                }
                for item in self.references
            ],
            "snapshot_sha256": self.snapshot_sha256.hex(),
        }


@dataclass(frozen=True, slots=True)
class RetentionApproval:
    """Caller-authenticated evidence; no signature verification occurs here."""

    proposal_sha256: bytes
    policy_sha256: bytes
    approver_ids: tuple[str, ...]
    automated_policy: bool
    approved: bool

    def __post_init__(self) -> None:
        _fixed_bytes(self.proposal_sha256, 32, "proposal_sha256")
        _fixed_bytes(self.policy_sha256, 32, "policy_sha256")
        if type(self.approver_ids) is not tuple or len(self.approver_ids) > 64:
            raise RetentionValidationError("approver_ids must be a bounded tuple")
        approvers = tuple(sorted({_identifier(item, "approver_id") for item in self.approver_ids}))
        if len(approvers) != len(self.approver_ids):
            raise RetentionValidationError("approver_ids must not repeat")
        object.__setattr__(self, "approver_ids", approvers)
        if type(self.automated_policy) is not bool or type(self.approved) is not bool:
            raise RetentionValidationError("approval flags must be exact bools")

    def canonical(self) -> dict[str, object]:
        return {
            "approved": self.approved,
            "approver_ids": list(self.approver_ids),
            "automated_policy": self.automated_policy,
            "policy_sha256": self.policy_sha256.hex(),
            "proposal_sha256": self.proposal_sha256.hex(),
        }


@dataclass(frozen=True, slots=True)
class GateEvidence:
    gate: RetentionGate
    satisfied: bool
    evidence_sha256: bytes
    detail: str

    def __post_init__(self) -> None:
        if not isinstance(self.gate, RetentionGate):
            raise RetentionValidationError("gate must be RetentionGate")
        if type(self.satisfied) is not bool:
            raise RetentionValidationError("gate result must be bool")
        _fixed_bytes(self.evidence_sha256, 32, "gate evidence SHA-256")
        _identifier(self.detail, "gate detail")


@dataclass(frozen=True, slots=True)
class CandidateRetentionEvaluation:
    artifact: RetentionArtifact
    gates: tuple[GateEvidence, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.artifact, RetentionArtifact):
            raise RetentionValidationError("artifact evaluation is invalid")
        if type(self.gates) is not tuple or any(
            not isinstance(item, GateEvidence) for item in self.gates
        ):
            raise RetentionValidationError("gates must be a typed tuple")
        names = [item.gate for item in self.gates]
        if set(names) != set(RetentionGate) or len(names) != len(set(names)):
            raise RetentionValidationError("every retention gate must appear exactly once")

    @property
    def deletion_authorized(self) -> bool:
        return all(item.satisfied for item in self.gates)


@dataclass(frozen=True, slots=True)
class RetentionPlan:
    observed_at_ns: int
    policy_sha256: bytes
    inventory_sha256: bytes
    proposal_sha256: bytes
    plan_sha256: bytes
    evaluations: tuple[CandidateRetentionEvaluation, ...]
    proposed_artifact_ids: tuple[str, ...]
    authorized_artifact_ids: tuple[str, ...]
    dry_run: bool = field(init=False, default=True)
    mutation_performed: bool = field(init=False, default=False)


def inventory_sha256(artifacts: Iterable[RetentionArtifact]) -> bytes:
    values = _normalize_artifacts(artifacts)
    return _hash_json(
        b"l2flow.retention.inventory.v1\x00",
        [item.canonical() for item in values],
    )


def _normalize_artifacts(
    artifacts: Iterable[RetentionArtifact],
) -> tuple[RetentionArtifact, ...]:
    if isinstance(artifacts, (str, bytes, bytearray, memoryview)):
        raise RetentionValidationError("artifacts must be a typed iterable")
    values = _bounded_take(artifacts, MAX_RETENTION_ARTIFACTS, "artifact inventory")
    if not values or len(values) > MAX_RETENTION_ARTIFACTS:
        raise RetentionValidationError("artifact inventory must be nonempty and bounded")
    if any(not isinstance(item, RetentionArtifact) for item in values):
        raise RetentionValidationError("artifact inventory contains an invalid value")
    ordered = tuple(sorted(values, key=lambda item: item.artifact_id))
    ids = [item.artifact_id for item in ordered]
    if len(ids) != len(set(ids)):
        raise RetentionValidationError("artifact inventory contains duplicate IDs")
    return ordered


def _evidence_map(
    values: Iterable[object],
    expected_type: type,
    name: str,
    maximum: int,
) -> dict[str, object]:
    result: dict[str, object] = {}
    for value in _bounded_take(values, maximum, name):
        if not isinstance(value, expected_type):
            raise RetentionValidationError(f"{name} contains an invalid value")
        artifact_id = value.artifact_id
        if artifact_id in result:
            raise RetentionValidationError(f"{name} contains duplicate artifact evidence")
        result[artifact_id] = value
    return result


def _gate(
    gate: RetentionGate,
    satisfied: bool,
    detail: str,
    evidence: object,
) -> GateEvidence:
    return GateEvidence(
        gate=gate,
        satisfied=satisfied,
        evidence_sha256=_hash_json(
            b"l2flow.retention.gate.v1\x00",
            {
                "detail": detail,
                "evidence": evidence,
                "gate": gate.value,
                "satisfied": satisfied,
            },
        ),
        detail=detail,
    )


@dataclass(frozen=True, slots=True)
class _ConsumerScopeState:
    registration_count: int
    checkpoint_count: int
    minimum_exclusive_next_cursor: int | None


def _index_consumers(
    registry: ConsumerRegistrySnapshot,
) -> dict[RawWalScope | CanonicalCursorScope, _ConsumerScopeState]:
    """Summarize each exact cursor scope once for constant-time gate checks."""

    registration_counts: dict[RawWalScope | CanonicalCursorScope, int] = {}
    checkpoint_counts: dict[RawWalScope | CanonicalCursorScope, int] = {}
    minimum_cursors: dict[RawWalScope | CanonicalCursorScope, int] = {}
    for registration in registry.registrations:
        registration_counts[registration.scope] = (
            registration_counts.get(registration.scope, 0) + 1
        )
    for checkpoint in registry.checkpoints:
        checkpoint_counts[checkpoint.scope] = (
            checkpoint_counts.get(checkpoint.scope, 0) + 1
        )
        current = minimum_cursors.get(checkpoint.scope)
        if current is None or checkpoint.exclusive_next_cursor < current:
            minimum_cursors[checkpoint.scope] = checkpoint.exclusive_next_cursor
    return {
        scope: _ConsumerScopeState(
            registration_count=count,
            checkpoint_count=checkpoint_counts.get(scope, 0),
            minimum_exclusive_next_cursor=minimum_cursors.get(scope),
        )
        for scope, count in registration_counts.items()
    }


def _consumer_gate(
    artifact: RetentionArtifact,
    *,
    registry_complete: bool,
    registry_identity: dict[str, object],
    consumer_index: dict[
        RawWalScope | CanonicalCursorScope,
        _ConsumerScopeState,
    ],
    allow_no_applicable_consumers: bool,
) -> GateEvidence:
    if artifact.scope is None:
        return _gate(
            RetentionGate.DURABLE_CONSUMERS,
            registry_complete,
            "not_cursor_scoped" if registry_complete else "registry_incomplete",
            {"registry_identity": registry_identity},
        )
    assert artifact.end_cursor is not None
    state = consumer_index.get(artifact.scope)
    no_applicable = state is None
    registration_count = 0 if state is None else state.registration_count
    checkpoint_count = 0 if state is None else state.checkpoint_count
    missing_count = registration_count - checkpoint_count
    minimum_cursor = (
        None if state is None else state.minimum_exclusive_next_cursor
    )
    # Segment coverage is [begin,end).  Every exact-scope checkpoint must be
    # at least end; therefore the pre-indexed minimum is a sufficient and
    # necessary constant-time test once missing checkpoints are excluded.
    checkpoint_behind = (
        minimum_cursor is not None and minimum_cursor < artifact.end_cursor
    )
    satisfied = (
        registry_complete
        and (allow_no_applicable_consumers or not no_applicable)
        and missing_count == 0
        and not checkpoint_behind
    )
    detail = (
        "complete_and_past_exclusive_end"
        if satisfied
        else (
            "registry_incomplete"
            if not registry_complete
            else "no_applicable_consumer_policy_block"
            if no_applicable and not allow_no_applicable_consumers
            else "missing_checkpoint"
            if missing_count
            else "checkpoint_before_exclusive_end"
        )
    )
    return _gate(
        RetentionGate.DURABLE_CONSUMERS,
        satisfied,
        detail,
        {
            "artifact_scope": artifact.scope.canonical(),
            "allow_no_applicable_consumers": allow_no_applicable_consumers,
            "checkpoint_count": checkpoint_count,
            "minimum_exclusive_next_cursor": (
                None if minimum_cursor is None else str(minimum_cursor)
            ),
            "missing_checkpoint_count": missing_count,
            "registration_count": registration_count,
            "registry_identity": registry_identity,
            "required_end_cursor": str(artifact.end_cursor),
        },
    )


def plan_retention(
    artifacts: Iterable[RetentionArtifact],
    *,
    observed_at_ns: int,
    policy_sha256: bytes,
    backup_policy_sha256: bytes,
    required_backup_copies: int,
    minimum_human_approvers: int,
    allow_automated_approval: bool,
    publication_evidence: Iterable[PublicationEvidence],
    backup_evidence: Iterable[BackupEvidence],
    consumer_registry: ConsumerRegistrySnapshot,
    active_references: ActiveReferenceSnapshot,
    sidecar_references: SidecarReferenceSnapshot,
    allow_no_applicable_consumers: bool = False,
    approval: RetentionApproval | None = None,
    expected_inventory_sha256: bytes | None = None,
) -> RetentionPlan:
    """Build one deterministic plan and never mutate an artifact namespace."""

    inventory = _normalize_artifacts(artifacts)
    _uint(observed_at_ns, 64, "observed_at_ns")
    _fixed_bytes(policy_sha256, 32, "policy_sha256")
    _fixed_bytes(backup_policy_sha256, 32, "backup_policy_sha256")
    _uint(required_backup_copies, 16, "required_backup_copies", positive=True)
    _uint(minimum_human_approvers, 16, "minimum_human_approvers", positive=True)
    if type(allow_automated_approval) is not bool:
        raise RetentionValidationError("allow_automated_approval must be bool")
    if type(allow_no_applicable_consumers) is not bool:
        raise RetentionValidationError("allow_no_applicable_consumers must be bool")
    if not isinstance(consumer_registry, ConsumerRegistrySnapshot):
        raise RetentionValidationError("consumer_registry has the wrong type")
    if not isinstance(active_references, ActiveReferenceSnapshot):
        raise RetentionValidationError("active_references has the wrong type")
    if not isinstance(sidecar_references, SidecarReferenceSnapshot):
        raise RetentionValidationError("sidecar_references has the wrong type")
    if approval is not None and not isinstance(approval, RetentionApproval):
        raise RetentionValidationError("approval has the wrong type")

    # These complete models are intentionally materialized once.  Per-artifact
    # gate evidence carries the authenticated snapshot identity plus only its
    # relevant indexed slice, while the proposal still binds the exact same
    # complete canonical snapshots as before.
    consumer_registry_model = consumer_registry.canonical()
    active_reference_snapshot_model = active_references.canonical()
    sidecar_reference_snapshot_model = sidecar_references.canonical()
    consumer_registry_identity: dict[str, object] = {
        "complete": consumer_registry.complete,
        "registry_generation": str(consumer_registry.registry_generation),
        "registry_sha256": consumer_registry.registry_sha256.hex(),
    }
    active_reference_snapshot_identity: dict[str, object] = {
        "complete": active_references.complete,
        "snapshot_sha256": active_references.snapshot_sha256.hex(),
    }
    sidecar_reference_snapshot_identity: dict[str, object] = {
        "complete": sidecar_references.complete,
        "snapshot_sha256": sidecar_references.snapshot_sha256.hex(),
    }
    consumer_index = _index_consumers(consumer_registry)

    actual_inventory_sha256 = _hash_json(
        b"l2flow.retention.inventory.v1\x00",
        [item.canonical() for item in inventory],
    )
    if expected_inventory_sha256 is not None:
        _fixed_bytes(expected_inventory_sha256, 32, "expected_inventory_sha256")
        if expected_inventory_sha256 != actual_inventory_sha256:
            raise StaleInventoryError("retention inventory changed before planning")

    publication = _evidence_map(
        publication_evidence,
        PublicationEvidence,
        "publication_evidence",
        len(inventory),
    )
    backups = _evidence_map(
        backup_evidence,
        BackupEvidence,
        "backup_evidence",
        len(inventory),
    )
    known_ids = {item.artifact_id for item in inventory}
    inventory_by_id = {item.artifact_id: item for item in inventory}
    if not set(publication).issubset(known_ids) or not set(backups).issubset(known_ids):
        raise RetentionValidationError("evidence refers to an unknown artifact")
    active_by_artifact: dict[str, list[ActiveReference]] = {}
    for reference in active_references.references:
        if reference.artifact_id not in known_ids:
            raise RetentionValidationError("active reference names an unknown artifact")
        if reference.kind is ActiveReferenceKind.MANIFEST:
            if reference.holder_id == reference.artifact_id:
                raise RetentionValidationError("manifest reference cannot be self-held")
            holder = inventory_by_id.get(reference.holder_id)
            if holder is not None:
                if holder.kind is not ArtifactKind.MANIFEST:
                    raise RetentionValidationError(
                        "manifest reference holder must be a manifest artifact"
                    )
            elif not reference.holder_id.startswith("CURRENT_ROOT:"):
                raise RetentionValidationError(
                    "external manifest holder must be an explicit CURRENT_ROOT"
                )
        active_by_artifact.setdefault(reference.artifact_id, []).append(reference)
    sidecar_by_artifact: dict[str, list[SidecarReference]] = {}
    for reference in sidecar_references.references:
        sidecar = inventory_by_id.get(reference.sidecar_artifact_id)
        if sidecar is None or sidecar.kind is not ArtifactKind.WATERMARK_SIDECAR:
            raise RetentionValidationError(
                "sidecar reference target must be an inventory watermark sidecar"
            )
        if reference.owner_kind is SidecarOwnerKind.HISTORY_ARTIFACT:
            owner = inventory_by_id.get(reference.owner_id)
            if owner is None or owner.kind is not ArtifactKind.PARQUET_PART:
                raise RetentionValidationError(
                    "history sidecar owner must be an inventory Parquet part"
                )
            if reference.owner_id == reference.sidecar_artifact_id:
                raise RetentionValidationError("sidecar reference cannot be self-owned")
        sidecar_by_artifact.setdefault(
            reference.sidecar_artifact_id, []
        ).append(reference)

    base_gates: dict[str, list[GateEvidence]] = {}
    tentative_ids: set[str] = set()

    for artifact in inventory:
        gates: list[GateEvidence] = []
        policy_bound = artifact.retention_policy_sha256 == policy_sha256
        period_ok = policy_bound and observed_at_ns >= artifact.retain_until_ns
        gates.append(
            _gate(
                RetentionGate.RETENTION_PERIOD,
                period_ok,
                "retention_elapsed"
                if period_ok
                else "retention_policy_mismatch"
                if not policy_bound
                else "retention_not_elapsed",
                {
                    "observed_at_ns": str(observed_at_ns),
                    "policy_sha256": policy_sha256.hex(),
                    "artifact_policy_sha256": (
                        artifact.retention_policy_sha256.hex()
                    ),
                    "retain_until_ns": str(artifact.retain_until_ns),
                    "time_domain_note": "caller_supplied_policy_time_domain",
                },
            )
        )

        publication_item = publication.get(artifact.artifact_id)
        publication_ok = False
        publication_model: object = {"missing": True}
        if isinstance(publication_item, PublicationEvidence):
            publication_model = publication_item.canonical()
            publication_ok = (
                publication_item.complete
                and set(artifact.required_manifest_sha256s).issubset(
                    set(publication_item.validated_manifest_sha256s)
                )
                and publication_item.scope == artifact.scope
                and publication_item.begin_cursor == artifact.begin_cursor
                and publication_item.end_cursor == artifact.end_cursor
            )
        gates.append(
            _gate(
                RetentionGate.PUBLISHED_MANIFESTS,
                publication_ok,
                "all_required_manifests_validated"
                if publication_ok
                else "manifest_scope_or_evidence_incomplete",
                publication_model,
            )
        )

        gates.append(
            _consumer_gate(
                artifact,
                registry_complete=consumer_registry.complete,
                registry_identity=consumer_registry_identity,
                consumer_index=consumer_index,
                allow_no_applicable_consumers=allow_no_applicable_consumers,
            )
        )

        active = active_by_artifact.get(artifact.artifact_id, [])
        inactive_ok = active_references.complete and not active
        gates.append(
            _gate(
                RetentionGate.INACTIVE_REFERENCES,
                inactive_ok,
                "no_active_references"
                if inactive_ok
                else (
                    "reference_snapshot_incomplete"
                    if not active_references.complete
                    else "active_reference"
                ),
                {
                    "active": [
                        {
                            "holder_id": item.holder_id,
                            "kind": item.kind.value,
                            "sha256": item.reference_sha256.hex(),
                        }
                        for item in active
                    ],
                    "snapshot_identity": active_reference_snapshot_identity,
                },
            )
        )

        backup_item = backups.get(artifact.artifact_id)
        backup_ok = False
        backup_model: object = {"missing": True}
        if isinstance(backup_item, BackupEvidence):
            backup_model = backup_item.canonical()
            backup_ok = (
                backup_item.complete
                and backup_item.source_sha256 == artifact.content_sha256
                and backup_item.backup_policy_sha256 == backup_policy_sha256
                and len(backup_item.verified_copy_receipt_sha256s)
                >= required_backup_copies
            )
        gates.append(
            _gate(
                RetentionGate.BACKUP_POLICY,
                backup_ok,
                "backup_policy_satisfied" if backup_ok else "backup_evidence_incomplete",
                backup_model,
            )
        )
        base_gates[artifact.artifact_id] = gates
        if all(item.satisfied for item in gates):
            tentative_ids.add(artifact.artifact_id)

    # Non-sidecar owners have no dependency edge back to a sidecar.  Resolve
    # them first, then close each sidecar over its exact history owners.  The
    # strict owner type above makes cycles and self-authorization impossible.
    proposed_non_sidecars = {
        item.artifact_id
        for item in inventory
        if item.kind is not ArtifactKind.WATERMARK_SIDECAR
        and item.artifact_id in tentative_ids
        and sidecar_references.complete
    }
    proposed_ids: set[str] = set(proposed_non_sidecars)
    for artifact in inventory:
        references = sidecar_by_artifact.get(artifact.artifact_id, [])
        sidecar_ok = sidecar_references.complete
        if artifact.kind is ArtifactKind.WATERMARK_SIDECAR:
            for reference in references:
                if reference.owner_kind is SidecarOwnerKind.LATEST_CURRENT:
                    sidecar_ok = False
                elif reference.owner_id not in proposed_non_sidecars:
                    sidecar_ok = False
        detail = (
            "not_a_sidecar"
            if artifact.kind is not ArtifactKind.WATERMARK_SIDECAR and sidecar_ok
            else "all_referencing_history_retires_together"
            if sidecar_ok
            else (
                "sidecar_snapshot_incomplete"
                if not sidecar_references.complete
                else "sidecar_still_referenced"
            )
        )
        base_gates[artifact.artifact_id].append(
            _gate(
                RetentionGate.SIDECAR_REACHABILITY,
                sidecar_ok,
                detail,
                {
                    "references": [
                        {
                            "owner_id": item.owner_id,
                            "owner_kind": item.owner_kind.value,
                            "sha256": item.reference_sha256.hex(),
                        }
                        for item in references
                    ],
                    "snapshot_identity": sidecar_reference_snapshot_identity,
                },
            )
        )
        if (
            artifact.kind is ArtifactKind.WATERMARK_SIDECAR
            and artifact.artifact_id in tentative_ids
            and sidecar_ok
        ):
            proposed_ids.add(artifact.artifact_id)

    proposal_model = {
        "active_reference_snapshot": active_reference_snapshot_model,
        "backup_evidence": [
            backups[key].canonical() for key in sorted(backups)  # type: ignore[union-attr]
        ],
        "backup_policy_sha256": backup_policy_sha256.hex(),
        "allow_automated_approval": allow_automated_approval,
        "allow_no_applicable_consumers": allow_no_applicable_consumers,
        "consumer_registry": consumer_registry_model,
        "inventory_sha256": actual_inventory_sha256.hex(),
        "observed_at_ns": str(observed_at_ns),
        "policy_sha256": policy_sha256.hex(),
        "proposed_artifact_ids": sorted(proposed_ids),
        "publication_evidence": [
            publication[key].canonical() for key in sorted(publication)  # type: ignore[union-attr]
        ],
        "required_backup_copies": required_backup_copies,
        "minimum_human_approvers": minimum_human_approvers,
        "sidecar_reference_snapshot": sidecar_reference_snapshot_model,
    }
    proposal_sha256 = _hash_json(
        b"l2flow.retention.proposal.v1\x00", proposal_model
    )

    approval_ok = False
    approval_model: object = {"missing": True}
    if approval is not None:
        approval_model = approval.canonical()
        approval_ok = (
            approval.approved
            and approval.proposal_sha256 == proposal_sha256
            and approval.policy_sha256 == policy_sha256
            and (
                (approval.automated_policy and allow_automated_approval)
                or (
                    not approval.automated_policy
                    and len(approval.approver_ids) >= minimum_human_approvers
                )
            )
        )

    evaluations: list[CandidateRetentionEvaluation] = []
    authorized_ids: list[str] = []
    for artifact in inventory:
        proposed = artifact.artifact_id in proposed_ids
        gate_ok = proposed and approval_ok
        detail = (
            "approval_bound_to_exact_proposal"
            if gate_ok
            else "not_proposed" if not proposed else "approval_missing_or_mismatched"
        )
        gates = list(base_gates[artifact.artifact_id])
        gates.append(
            _gate(
                RetentionGate.IMMUTABLE_PLAN_APPROVAL,
                gate_ok,
                detail,
                approval_model,
            )
        )
        gates.sort(key=lambda item: item.gate.value)
        evaluation = CandidateRetentionEvaluation(artifact=artifact, gates=tuple(gates))
        evaluations.append(evaluation)
        if evaluation.deletion_authorized:
            authorized_ids.append(artifact.artifact_id)

    plan_model = {
        "approval": approval_model,
        "authorized_artifact_ids": sorted(authorized_ids),
        "evaluations": [
            {
                "artifact_id": item.artifact.artifact_id,
                "gates": [
                    {
                        "detail": gate.detail,
                        "evidence_sha256": gate.evidence_sha256.hex(),
                        "gate": gate.gate.value,
                        "satisfied": gate.satisfied,
                    }
                    for gate in item.gates
                ],
            }
            for item in evaluations
        ],
        "inventory_sha256": actual_inventory_sha256.hex(),
        "proposal_sha256": proposal_sha256.hex(),
    }
    plan_sha256 = _hash_json(b"l2flow.retention.plan.v1\x00", plan_model)
    return RetentionPlan(
        observed_at_ns=observed_at_ns,
        policy_sha256=policy_sha256,
        inventory_sha256=actual_inventory_sha256,
        proposal_sha256=proposal_sha256,
        plan_sha256=plan_sha256,
        evaluations=tuple(evaluations),
        proposed_artifact_ids=tuple(sorted(proposed_ids)),
        authorized_artifact_ids=tuple(sorted(authorized_ids)),
    )


build_retention_dry_run = plan_retention
