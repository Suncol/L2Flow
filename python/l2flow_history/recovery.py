"""Fail-closed Phase-8 recovery planning and certificate values.

This module deliberately has no repair, delete, checkpoint-restore, service,
or alias-switch executor.  It validates caller-supplied evidence and emits
immutable plans.  Hashes bind canonical bytes; they are integrity identities,
not signatures or proof that a caller's external observations are truthful.

The repository-local Canonical V1 path is intentionally a trading-day replay.
Raw writer/source/clock identity is copied exactly from each input route.  A
"fresh" build means a new recovery attempt and derived writer owner, a new
one-shot Canonical generation, fresh zero-processed frontier pages and
normalizers, and empty all-day sinks.  It never means rewriting Raw identity.
"""

from __future__ import annotations

from dataclasses import InitVar, dataclass, field
from enum import Enum
import hashlib
import json
from pathlib import PurePosixPath
import re
import struct
from typing import Any, Iterable

from l2flow_factor import (
    FactorCheckpoint,
    FactorInputWatermark,
    FactorInputWatermarkSet,
    FactorSpec,
    InputFamily,
    InputMode,
    RUNTIME_STATE_CODEC_V1,
    decode_runtime_checkpoint,
    encode_checkpoint,
)

from .manifest import SourceNamespace


MAX_RECOVERY_ITEMS_V1 = 65_536
MAX_CERTIFICATE_JSON_BYTES_V1 = 64 * 1024 * 1024
_CERTIFICATE_MAGIC = b"L2RCVJ1\x00"
_PLAN_TOKEN = object()


class RecoveryError(ValueError):
    """A recovery value or evidence set is invalid."""


class RecoveryConflictError(RecoveryError):
    """Individually valid recovery evidence conflicts or is incomplete."""


class RecoveryIntegrityError(RecoveryError):
    """Canonical certificate bytes or their expected identity disagree."""


def _uint(value: object, bits: int, name: str, *, positive: bool = False) -> int:
    minimum = 1 if positive else 0
    if type(value) is not int or value < minimum or value > (1 << bits) - 1:
        qualifier = "positive " if positive else ""
        raise RecoveryError(f"{name} must be an exact {qualifier}uint{bits}")
    return value


def _exact_bool(value: object, name: str) -> bool:
    if type(value) is not bool:
        raise RecoveryError(f"{name} must be an exact bool")
    return value


def _fixed_bytes(value: object, width: int, name: str) -> bytes:
    if type(value) is not bytes or len(value) != width or not any(value):
        raise RecoveryError(f"{name} must be exact immutable nonzero {width}-byte data")
    return value


def _token(value: object, name: str, *, maximum: int = 128) -> str:
    if (
        type(value) is not str
        or not value
        or len(value) > maximum
        or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._+-]*", value) is None
    ):
        raise RecoveryError(f"{name} must be a bounded safe token")
    return value


def _typed_tuple(
    value: object,
    item_type: type,
    name: str,
    *,
    nonempty: bool = True,
) -> tuple[Any, ...]:
    if (
        type(value) is not tuple
        or (nonempty and not value)
        or len(value) > MAX_RECOVERY_ITEMS_V1
        or any(type(item) is not item_type for item in value)
    ):
        qualifier = "nonempty " if nonempty else ""
        raise RecoveryError(f"{name} must be a bounded {qualifier}typed tuple")
    return value


def _canonical_json(value: object) -> bytes:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
    except (TypeError, ValueError, UnicodeEncodeError) as error:
        raise RecoveryError("recovery value is not canonical-JSON encodable") from error


def _object_sha256(domain: bytes, value: object) -> bytes:
    return hashlib.sha256(domain + _canonical_json(value)).digest()


def _enum(value: object, enum_type: type[Enum], name: str) -> Enum:
    if type(value) is not enum_type:
        raise RecoveryError(f"{name} must be an exact {enum_type.__name__}")
    return value


class SourceHealth(Enum):
    HEALTHY = "healthy"
    DISCONNECTED = "disconnected"
    FATAL = "fatal"


class RawReplayStage(Enum):
    API_SYS = "api_sys"
    MARKET = "market"


class RecoveryMode(Enum):
    CANONICAL_DAY_START = "canonical_day_start"
    LATEST_CHECKPOINT_TAIL = "latest_checkpoint_tail"
    LATEST_FULL_REPLAY = "latest_full_replay"
    FACTOR_CHECKPOINT_TAIL = "factor_checkpoint_tail"


class LatestStateInputFamily(Enum):
    SNAPSHOT = "snapshot"
    TICK_QUALITY = "tick_quality"


class NativeSafeMuxDecision(Enum):
    READY = "ready"
    NOT_READY = "not_ready"
    FATAL = "fatal"


@dataclass(frozen=True, slots=True, order=True)
class RawRouteIdentity:
    """One exact Raw input identity; derived recovery never rewrites it."""

    trade_date: int
    source_stream_id: int
    origin_capture_date: int
    origin_stream_day_id: bytes
    origin_source_writer_instance: bytes
    origin_source_generation: int
    clock_epoch_algorithm: int
    clock_epoch_digest: bytes
    day_begin_ingress_sequence: int
    day_begin_wal_pos: int
    replay_end_ingress_sequence: int
    replay_end_wal_pos: int
    current_raw_durable_wal_pos: int
    health: SourceHealth
    market: str
    configured_feed_role: str

    def __post_init__(self) -> None:
        _uint(self.trade_date, 32, "trade_date", positive=True)
        _uint(self.source_stream_id, 32, "source_stream_id", positive=True)
        _uint(self.origin_capture_date, 32, "origin_capture_date", positive=True)
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
            self.clock_epoch_algorithm,
            32,
            "clock_epoch_algorithm",
            positive=True,
        )
        _fixed_bytes(self.clock_epoch_digest, 32, "clock_epoch_digest")
        _uint(
            self.day_begin_ingress_sequence,
            64,
            "day_begin_ingress_sequence",
            positive=True,
        )
        _uint(self.day_begin_wal_pos, 64, "day_begin_wal_pos")
        _uint(
            self.replay_end_ingress_sequence,
            64,
            "replay_end_ingress_sequence",
            positive=True,
        )
        _uint(self.replay_end_wal_pos, 64, "replay_end_wal_pos", positive=True)
        _uint(
            self.current_raw_durable_wal_pos,
            64,
            "current_raw_durable_wal_pos",
            positive=True,
        )
        _enum(self.health, SourceHealth, "health")
        if self.market not in ("SH", "SZ", "GLOBAL"):
            raise RecoveryError("Raw route market must be SH, SZ, or GLOBAL")
        _token(self.configured_feed_role, "configured_feed_role")
        if self.replay_end_ingress_sequence < self.day_begin_ingress_sequence:
            raise RecoveryError("Raw replay ingress range regresses")
        if self.replay_end_wal_pos <= self.day_begin_wal_pos:
            raise RecoveryError("Raw replay WAL range must be nonempty")
        if self.current_raw_durable_wal_pos < self.replay_end_wal_pos:
            raise RecoveryError("Raw replay end is not covered by Raw durability")

    @property
    def route_key(self) -> tuple[object, ...]:
        return (
            self.trade_date,
            self.source_stream_id,
            self.origin_capture_date,
            self.origin_stream_day_id,
            self.origin_source_writer_instance,
            self.origin_source_generation,
            self.clock_epoch_algorithm,
            self.clock_epoch_digest,
            self.market,
            self.configured_feed_role,
        )

    def canonical_object(self) -> dict[str, object]:
        return {
            "clock_epoch_algorithm": self.clock_epoch_algorithm,
            "clock_epoch_digest": self.clock_epoch_digest.hex(),
            "current_raw_durable_wal_pos": self.current_raw_durable_wal_pos,
            "day_begin_ingress_sequence": self.day_begin_ingress_sequence,
            "day_begin_wal_pos": self.day_begin_wal_pos,
            "health": self.health.value,
            "market": self.market,
            "configured_feed_role": self.configured_feed_role,
            "origin_capture_date": self.origin_capture_date,
            "origin_source_generation": self.origin_source_generation,
            "origin_source_writer_instance": self.origin_source_writer_instance.hex(),
            "origin_stream_day_id": self.origin_stream_day_id.hex(),
            "replay_end_ingress_sequence": self.replay_end_ingress_sequence,
            "replay_end_wal_pos": self.replay_end_wal_pos,
            "source_stream_id": self.source_stream_id,
            "trade_date": self.trade_date,
        }


def configured_raw_routes_sha256(
    routes: tuple[RawRouteIdentity, ...],
) -> bytes:
    """Return the exact configured Raw-route manifest identity."""

    values = _typed_tuple(routes, RawRouteIdentity, "configured Raw routes")
    ordered = tuple(sorted(values, key=lambda item: item.route_key))
    keys = [item.route_key for item in ordered]
    if len(keys) != len(set(keys)):
        raise RecoveryConflictError("configured Raw route identity is duplicated")
    return _object_sha256(
        b"l2flow.recovery.configured-raw-routes.v1\x00",
        [item.canonical_object() for item in ordered],
    )


@dataclass(frozen=True, slots=True)
class RawReplaySpan:
    """A contiguous full Raw scan chunk for one replay pass.

    Both API_SYS and MARKET passes scan every Raw record in the complete
    trading-day range.  ``record_count`` and ingress continuity therefore
    describe all Raw records in the chunk, not a sparse stage-specific subset;
    the selected stage controls which envelopes are acted on during that pass.
    """

    route: RawRouteIdentity
    stage: RawReplayStage
    begin_wal_pos: int
    end_wal_pos: int
    first_ingress_sequence: int
    last_ingress_sequence: int
    record_count: int
    ordered_record_content_sha256: bytes
    envelope_verifier_sha256: bytes
    exact_record_boundaries_verified: bool

    def __post_init__(self) -> None:
        if type(self.route) is not RawRouteIdentity:
            raise RecoveryError("Raw replay span route must be exact RawRouteIdentity")
        _enum(self.stage, RawReplayStage, "stage")
        _uint(self.begin_wal_pos, 64, "begin_wal_pos")
        _uint(self.end_wal_pos, 64, "end_wal_pos", positive=True)
        _uint(
            self.first_ingress_sequence,
            64,
            "first_ingress_sequence",
            positive=True,
        )
        _uint(
            self.last_ingress_sequence,
            64,
            "last_ingress_sequence",
            positive=True,
        )
        _uint(self.record_count, 64, "record_count", positive=True)
        _fixed_bytes(
            self.ordered_record_content_sha256,
            32,
            "ordered_record_content_sha256",
        )
        _fixed_bytes(
            self.envelope_verifier_sha256,
            32,
            "envelope_verifier_sha256",
        )
        _exact_bool(
            self.exact_record_boundaries_verified,
            "exact_record_boundaries_verified",
        )
        if self.end_wal_pos <= self.begin_wal_pos:
            raise RecoveryError("Raw replay span WAL range must be nonempty")
        if self.last_ingress_sequence < self.first_ingress_sequence:
            raise RecoveryError("Raw replay span ingress range regresses")
        if self.record_count != (
            self.last_ingress_sequence - self.first_ingress_sequence + 1
        ):
            raise RecoveryError("Raw replay span record count is not exact")

    @property
    def sort_key(self) -> tuple[object, ...]:
        return self.route.route_key + (self.stage.value, self.begin_wal_pos)

    def canonical_object(self) -> dict[str, object]:
        return {
            "begin_wal_pos": self.begin_wal_pos,
            "end_wal_pos": self.end_wal_pos,
            "envelope_verifier_sha256": self.envelope_verifier_sha256.hex(),
            "exact_record_boundaries_verified": self.exact_record_boundaries_verified,
            "first_ingress_sequence": self.first_ingress_sequence,
            "last_ingress_sequence": self.last_ingress_sequence,
            "ordered_record_content_sha256": self.ordered_record_content_sha256.hex(),
            "record_count": self.record_count,
            "route": self.route.canonical_object(),
            "stage": self.stage.value,
        }


@dataclass(frozen=True, slots=True)
class CanonicalSinkSpec:
    route: RawRouteIdentity
    family: InputFamily
    shard_id: int
    canonical_generation: int
    capacity_records: int
    required_records_upper_bound: int
    descriptor_identity_sha256: bytes
    fresh_file: bool
    empty: bool

    def __post_init__(self) -> None:
        if type(self.route) is not RawRouteIdentity:
            raise RecoveryError("sink route must be exact RawRouteIdentity")
        if type(self.family) is not InputFamily:
            raise RecoveryError("sink family must be exact InputFamily")
        self.family.canonical_event_type
        _uint(self.shard_id, 32, "shard_id")
        _uint(
            self.canonical_generation,
            64,
            "canonical_generation",
            positive=True,
        )
        _uint(self.capacity_records, 64, "capacity_records", positive=True)
        _uint(
            self.required_records_upper_bound,
            64,
            "required_records_upper_bound",
        )
        _fixed_bytes(
            self.descriptor_identity_sha256,
            32,
            "descriptor_identity_sha256",
        )
        _exact_bool(self.fresh_file, "fresh_file")
        _exact_bool(self.empty, "empty")
        if self.capacity_records < self.required_records_upper_bound:
            raise RecoveryError("Canonical sink is not sized for its all-day bound")

    @property
    def sort_key(self) -> tuple[object, ...]:
        return self.route.route_key + (
            self.family.canonical_event_type,
            self.shard_id,
        )

    def canonical_object(self) -> dict[str, object]:
        return {
            "canonical_generation": self.canonical_generation,
            "capacity_records": self.capacity_records,
            "descriptor_identity_sha256": self.descriptor_identity_sha256.hex(),
            "empty": self.empty,
            "family": self.family.value,
            "fresh_file": self.fresh_file,
            "required_records_upper_bound": self.required_records_upper_bound,
            "route": self.route.canonical_object(),
            "shard_id": self.shard_id,
        }


@dataclass(frozen=True, slots=True)
class CanonicalGenerationIdentity:
    trade_date: int
    previous_canonical_generation: int
    candidate_canonical_generation: int
    previous_recovery_writer_instance: bytes
    candidate_recovery_writer_instance: bytes
    recovery_attempt_id: bytes
    shard_count: int
    canonical_schema_sha256: bytes
    canonical_dtype_sha256: bytes
    registry_version: int
    registry_sha256: bytes
    normalizer_build_sha256: bytes
    normalizer_config_sha256: bytes
    clock_epoch_algorithm: int
    clock_epoch_digest: bytes

    def __post_init__(self) -> None:
        _uint(self.trade_date, 32, "trade_date", positive=True)
        _uint(
            self.previous_canonical_generation,
            64,
            "previous_canonical_generation",
            positive=True,
        )
        _uint(
            self.candidate_canonical_generation,
            64,
            "candidate_canonical_generation",
            positive=True,
        )
        _fixed_bytes(
            self.previous_recovery_writer_instance,
            16,
            "previous_recovery_writer_instance",
        )
        _fixed_bytes(
            self.candidate_recovery_writer_instance,
            16,
            "candidate_recovery_writer_instance",
        )
        _fixed_bytes(self.recovery_attempt_id, 16, "recovery_attempt_id")
        _uint(self.shard_count, 32, "shard_count", positive=True)
        if self.shard_count > 65_536:
            raise RecoveryError("shard_count exceeds the recovery bound")
        for name in (
            "canonical_schema_sha256",
            "canonical_dtype_sha256",
            "registry_sha256",
            "normalizer_build_sha256",
            "normalizer_config_sha256",
            "clock_epoch_digest",
        ):
            _fixed_bytes(getattr(self, name), 32, name)
        _uint(self.registry_version, 64, "registry_version", positive=True)
        _uint(
            self.clock_epoch_algorithm,
            32,
            "clock_epoch_algorithm",
            positive=True,
        )
        if self.candidate_canonical_generation <= self.previous_canonical_generation:
            raise RecoveryError("candidate Canonical generation must be fresh and newer")
        if self.candidate_recovery_writer_instance == self.previous_recovery_writer_instance:
            raise RecoveryError("recovery writer ownership was reused")

    def canonical_object(self) -> dict[str, object]:
        return {
            "candidate_canonical_generation": self.candidate_canonical_generation,
            "candidate_recovery_writer_instance": self.candidate_recovery_writer_instance.hex(),
            "canonical_dtype_sha256": self.canonical_dtype_sha256.hex(),
            "canonical_schema_sha256": self.canonical_schema_sha256.hex(),
            "clock_epoch_algorithm": self.clock_epoch_algorithm,
            "clock_epoch_digest": self.clock_epoch_digest.hex(),
            "normalizer_build_sha256": self.normalizer_build_sha256.hex(),
            "normalizer_config_sha256": self.normalizer_config_sha256.hex(),
            "previous_canonical_generation": self.previous_canonical_generation,
            "previous_recovery_writer_instance": self.previous_recovery_writer_instance.hex(),
            "recovery_attempt_id": self.recovery_attempt_id.hex(),
            "registry_sha256": self.registry_sha256.hex(),
            "registry_version": self.registry_version,
            "shard_count": self.shard_count,
            "trade_date": self.trade_date,
        }


@dataclass(frozen=True, slots=True)
class CanonicalColdStartRequest:
    identity: CanonicalGenerationIdentity
    expected_raw_routes: tuple[RawRouteIdentity, ...]
    configured_route_manifest_sha256: bytes
    raw_routes: tuple[RawRouteIdentity, ...]
    replay_spans: tuple[RawReplaySpan, ...]
    sinks: tuple[CanonicalSinkSpec, ...]
    frontier_pages_fresh: bool
    normalizers_fresh: bool
    frontier_initial_processed_ingress_sequence: int
    frontier_initial_processed_wal_pos: int
    resume_from_old_generation_requested: bool = False
    old_processed_cursor: int | None = None
    normalizer_checkpoint_sha256: bytes | None = None

    def __post_init__(self) -> None:
        if type(self.identity) is not CanonicalGenerationIdentity:
            raise RecoveryError("identity must be CanonicalGenerationIdentity")
        _typed_tuple(
            self.expected_raw_routes,
            RawRouteIdentity,
            "expected_raw_routes",
        )
        _fixed_bytes(
            self.configured_route_manifest_sha256,
            32,
            "configured_route_manifest_sha256",
        )
        _typed_tuple(self.raw_routes, RawRouteIdentity, "raw_routes")
        _typed_tuple(self.replay_spans, RawReplaySpan, "replay_spans")
        _typed_tuple(self.sinks, CanonicalSinkSpec, "sinks")
        _exact_bool(self.frontier_pages_fresh, "frontier_pages_fresh")
        _exact_bool(self.normalizers_fresh, "normalizers_fresh")
        _uint(
            self.frontier_initial_processed_ingress_sequence,
            64,
            "frontier_initial_processed_ingress_sequence",
        )
        _uint(
            self.frontier_initial_processed_wal_pos,
            64,
            "frontier_initial_processed_wal_pos",
        )
        _exact_bool(
            self.resume_from_old_generation_requested,
            "resume_from_old_generation_requested",
        )
        if self.old_processed_cursor is not None:
            _uint(self.old_processed_cursor, 64, "old_processed_cursor")
        if self.normalizer_checkpoint_sha256 is not None:
            _fixed_bytes(
                self.normalizer_checkpoint_sha256,
                32,
                "normalizer_checkpoint_sha256",
            )


@dataclass(frozen=True, slots=True)
class CanonicalColdStartPlan:
    identity: CanonicalGenerationIdentity
    configured_route_manifest_sha256: bytes
    raw_routes: tuple[RawRouteIdentity, ...]
    replay_spans: tuple[RawReplaySpan, ...]
    sinks: tuple[CanonicalSinkSpec, ...]
    steps: tuple[str, ...]
    _construction_token: InitVar[object] = None
    plan_sha256: bytes = field(init=False)
    executes_repair: bool = field(init=False, default=False)
    deletes_data: bool = field(init=False, default=False)
    authorizes_alias_switch: bool = field(init=False, default=False)

    def __post_init__(self, _construction_token: object) -> None:
        if _construction_token is not _PLAN_TOKEN:
            raise RecoveryError("use build_canonical_cold_start_plan")
        value = {
            "authorizes_alias_switch": False,
            "deletes_data": False,
            "encoding": "l2flow-canonical-cold-start-plan-v1",
            "executes_repair": False,
            "identity": self.identity.canonical_object(),
            "configured_route_manifest_sha256": (
                self.configured_route_manifest_sha256.hex()
            ),
            "raw_routes": [item.canonical_object() for item in self.raw_routes],
            "replay_spans": [item.canonical_object() for item in self.replay_spans],
            "sinks": [item.canonical_object() for item in self.sinks],
            "steps": list(self.steps),
        }
        object.__setattr__(
            self,
            "plan_sha256",
            _object_sha256(b"l2flow.recovery.canonical-plan.v1\x00", value),
        )

    def canonical_object(self) -> dict[str, object]:
        return {
            "authorizes_alias_switch": False,
            "deletes_data": False,
            "encoding": "l2flow-canonical-cold-start-plan-v1",
            "executes_repair": False,
            "identity": self.identity.canonical_object(),
            "configured_route_manifest_sha256": (
                self.configured_route_manifest_sha256.hex()
            ),
            "plan_sha256": self.plan_sha256.hex(),
            "raw_routes": [item.canonical_object() for item in self.raw_routes],
            "replay_spans": [item.canonical_object() for item in self.replay_spans],
            "sinks": [item.canonical_object() for item in self.sinks],
            "steps": list(self.steps),
        }


def _validate_raw_stage_coverage(
    route: RawRouteIdentity,
    stage: RawReplayStage,
    selected_spans: tuple[RawReplaySpan, ...],
) -> tuple[RawReplaySpan, ...]:
    selected = tuple(
        sorted(
            selected_spans,
            key=lambda item: item.begin_wal_pos,
        )
    )
    if any(item.route != route or item.stage is not stage for item in selected):
        raise RecoveryConflictError("Raw replay coverage was grouped under a wrong route")
    if not selected:
        raise RecoveryConflictError(
            f"Raw route is missing complete {stage.value} replay coverage"
        )
    expected_wal = route.day_begin_wal_pos
    expected_ingress = route.day_begin_ingress_sequence
    for item in selected:
        if item.begin_wal_pos != expected_wal:
            raise RecoveryConflictError("Raw replay spans overlap or have a WAL gap")
        if item.first_ingress_sequence != expected_ingress:
            raise RecoveryConflictError("Raw replay spans overlap or have an ingress gap")
        if item.exact_record_boundaries_verified is not True:
            raise RecoveryConflictError("Raw replay span lacks exact boundary verification")
        expected_wal = item.end_wal_pos
        expected_ingress = item.last_ingress_sequence + 1
    if expected_wal != route.replay_end_wal_pos:
        raise RecoveryConflictError("Raw replay coverage does not reach exact end WAL")
    if expected_ingress != route.replay_end_ingress_sequence + 1:
        raise RecoveryConflictError("Raw replay coverage does not reach exact end ingress")
    return selected


def _raw_full_scan_receipts(
    values: tuple[RawReplaySpan, ...],
) -> tuple[tuple[object, ...], ...]:
    return tuple(
        (
            item.begin_wal_pos,
            item.end_wal_pos,
            item.first_ingress_sequence,
            item.last_ingress_sequence,
            item.record_count,
            item.ordered_record_content_sha256,
            item.exact_record_boundaries_verified,
        )
        for item in values
    )


def build_canonical_cold_start_plan(
    request: CanonicalColdStartRequest,
) -> CanonicalColdStartPlan:
    if type(request) is not CanonicalColdStartRequest:
        raise RecoveryError("request must be exact CanonicalColdStartRequest")
    identity = request.identity
    if (
        request.frontier_pages_fresh is not True
        or request.normalizers_fresh is not True
        or request.frontier_initial_processed_ingress_sequence != 0
        or request.frontier_initial_processed_wal_pos != 0
    ):
        raise RecoveryConflictError(
            "Canonical V1 requires fresh zero-processed frontiers and normalizers"
        )
    if (
        request.resume_from_old_generation_requested
        or request.old_processed_cursor is not None
        or request.normalizer_checkpoint_sha256 is not None
    ):
        raise RecoveryConflictError(
            "Canonical V1 cannot resume an old cursor/checkpoint/generation"
        )

    expected_routes = tuple(
        sorted(request.expected_raw_routes, key=lambda item: item.route_key)
    )
    expected_keys = [item.route_key for item in expected_routes]
    if len(expected_keys) != len(set(expected_keys)):
        raise RecoveryConflictError("configured Raw route identity is duplicated")
    expected_manifest = configured_raw_routes_sha256(expected_routes)
    if expected_manifest != request.configured_route_manifest_sha256:
        raise RecoveryIntegrityError("configured Raw route manifest hash mismatch")

    routes = tuple(sorted(request.raw_routes, key=lambda item: item.route_key))
    route_keys = [item.route_key for item in routes]
    if len(route_keys) != len(set(route_keys)):
        raise RecoveryConflictError("Raw route identity is duplicated")
    if routes != expected_routes:
        raise RecoveryConflictError(
            "supplied Raw routes do not exactly match the configured route manifest"
        )
    for route in routes:
        if route.trade_date != identity.trade_date:
            raise RecoveryConflictError("Raw route trade_date differs from candidate")
        if route.health is not SourceHealth.HEALTHY:
            raise RecoveryConflictError("missing/disconnected/FATAL Raw route fails recovery")
        if (
            route.clock_epoch_algorithm != identity.clock_epoch_algorithm
            or route.clock_epoch_digest != identity.clock_epoch_digest
        ):
            raise RecoveryConflictError("Raw route clock identity differs from candidate")

    spans = tuple(sorted(request.replay_spans, key=lambda item: item.sort_key))
    route_set = set(routes)
    if any(item.route not in route_set for item in spans):
        raise RecoveryConflictError("Raw replay span names an unconfigured route")
    span_identities = [
        (item.route.route_key, item.stage, item.begin_wal_pos, item.end_wal_pos)
        for item in spans
    ]
    if len(span_identities) != len(set(span_identities)):
        raise RecoveryConflictError("Raw replay span is duplicated")
    grouped_spans: dict[
        tuple[RawRouteIdentity, RawReplayStage], list[RawReplaySpan]
    ] = {}
    for span in spans:
        grouped_spans.setdefault((span.route, span.stage), []).append(span)
    for route in routes:
        api_sys = _validate_raw_stage_coverage(
            route,
            RawReplayStage.API_SYS,
            tuple(grouped_spans.get((route, RawReplayStage.API_SYS), ())),
        )
        market = _validate_raw_stage_coverage(
            route,
            RawReplayStage.MARKET,
            tuple(grouped_spans.get((route, RawReplayStage.MARKET), ())),
        )
        # Both passes are full scans, not sparse stage extracts.  Bind them to
        # identical chunks and identical ordered Raw bytes.
        if _raw_full_scan_receipts(api_sys) != _raw_full_scan_receipts(market):
            raise RecoveryConflictError(
                "API/SYS and MARKET passes did not scan identical ordered Raw bytes"
            )

    sinks = tuple(sorted(request.sinks, key=lambda item: item.sort_key))
    sink_keys = [item.sort_key for item in sinks]
    required_sink_count = len(routes) * (2 * identity.shard_count + 2)
    if required_sink_count > MAX_RECOVERY_ITEMS_V1:
        raise RecoveryConflictError(
            "configured route/shard sink cross-product exceeds the recovery bound"
        )
    expected_sinks: set[tuple[object, ...]] = set()
    for route in routes:
        expected_sinks.update(
            route.route_key
            + (InputFamily.SNAPSHOT.canonical_event_type, shard)
            for shard in range(identity.shard_count)
        )
        expected_sinks.update(
            route.route_key + (InputFamily.TICK.canonical_event_type, shard)
            for shard in range(identity.shard_count)
        )
        expected_sinks.add(
            route.route_key + (InputFamily.QUALITY.canonical_event_type, 0)
        )
        expected_sinks.add(
            route.route_key + (InputFamily.CONTROL.canonical_event_type, 0)
        )
    if len(sink_keys) != len(set(sink_keys)) or set(sink_keys) != expected_sinks:
        raise RecoveryConflictError("Canonical sink manifest is missing, duplicate, or extra")
    for sink in sinks:
        if (
            sink.route not in route_set
            or sink.canonical_generation
            != identity.candidate_canonical_generation
            or sink.fresh_file is not True
            or sink.empty is not True
        ):
            raise RecoveryConflictError(
                "Canonical V1 recovery requires fresh empty candidate-generation sinks"
            )

    return CanonicalColdStartPlan(
        identity=identity,
        configured_route_manifest_sha256=request.configured_route_manifest_sha256,
        raw_routes=routes,
        replay_spans=spans,
        sinks=sinks,
        steps=(
            "classify_previous_generation_unconsumable",
            "create_fresh_recovery_owner_and_zero_processed_frontiers",
            "create_fresh_normalizers_and_empty_all_day_sinks",
            "replay_api_sys_from_each_trading_day_raw_start",
            "replay_market_from_each_trading_day_raw_start",
            "verify_exact_envelopes_configured_routes_and_replay_hashes",
            "emit_isolated_candidate_only",
        ),
        _construction_token=_PLAN_TOKEN,
    )


def _source_namespace_key(namespace: SourceNamespace) -> tuple[object, ...]:
    return namespace.sort_key


def _source_namespace_object(namespace: SourceNamespace) -> dict[str, object]:
    return namespace.canonical_object()


def _validate_latest_family_namespace(
    family: LatestStateInputFamily,
    namespace: SourceNamespace,
) -> None:
    expected = (
        InputFamily.SNAPSHOT
        if family is LatestStateInputFamily.SNAPSHOT
        else InputFamily.TICK
    )
    if namespace.family is not expected:
        raise RecoveryError(
            "Latest Snapshot/TickQuality family disagrees with Canonical namespace"
        )


@dataclass(frozen=True, slots=True)
class LatestStateConfigIdentity:
    """The exact Phase-6 LatestStateConfig identity, plus codec identities."""

    state_generation: int
    state_writer_instance: bytes
    shard_id: int
    shard_count: int
    canonical_schema_sha256: bytes
    canonical_dtype_sha256: bytes
    registry_version: int
    registry_sha256: bytes
    state_schema_sha256: bytes
    state_config_sha256: bytes

    def __post_init__(self) -> None:
        _uint(self.state_generation, 64, "state_generation", positive=True)
        _fixed_bytes(self.state_writer_instance, 16, "state_writer_instance")
        _uint(self.shard_id, 32, "shard_id")
        _uint(self.shard_count, 32, "shard_count", positive=True)
        if self.shard_count > 65_536 or self.shard_id >= self.shard_count:
            raise RecoveryError("Latest State shard identity is outside its bound")
        for name in (
            "canonical_schema_sha256",
            "canonical_dtype_sha256",
            "registry_sha256",
            "state_schema_sha256",
            "state_config_sha256",
        ):
            _fixed_bytes(getattr(self, name), 32, name)
        _uint(self.registry_version, 64, "registry_version", positive=True)

    def canonical_object(self) -> dict[str, object]:
        return {
            "canonical_dtype_sha256": self.canonical_dtype_sha256.hex(),
            "canonical_schema_sha256": self.canonical_schema_sha256.hex(),
            "registry_sha256": self.registry_sha256.hex(),
            "registry_version": self.registry_version,
            "shard_count": self.shard_count,
            "shard_id": self.shard_id,
            "state_config_sha256": self.state_config_sha256.hex(),
            "state_generation": self.state_generation,
            "state_schema_sha256": self.state_schema_sha256.hex(),
            "state_writer_instance": self.state_writer_instance.hex(),
        }


@dataclass(frozen=True, slots=True)
class LatestStateCheckpointCursor:
    family: LatestStateInputFamily
    namespace: SourceNamespace
    exclusive_canonical_cursor: int
    max_consumed_origin_wal_end_pos: int
    external_cursor_receipt_sha256: bytes | None
    bound_checkpoint_sha256: bytes | None
    bound_common_cut_identity_sha256: bytes | None

    def __post_init__(self) -> None:
        _enum(self.family, LatestStateInputFamily, "family")
        if type(self.namespace) is not SourceNamespace:
            raise RecoveryError("Latest cursor namespace must be exact SourceNamespace")
        _validate_latest_family_namespace(self.family, self.namespace)
        _uint(
            self.exclusive_canonical_cursor,
            64,
            "exclusive_canonical_cursor",
        )
        _uint(
            self.max_consumed_origin_wal_end_pos,
            64,
            "max_consumed_origin_wal_end_pos",
        )
        if (self.exclusive_canonical_cursor == 0) != (
            self.max_consumed_origin_wal_end_pos == 0
        ):
            raise RecoveryError("Latest zero cursor and consumed WAL must agree")
        bindings = (
            self.external_cursor_receipt_sha256,
            self.bound_checkpoint_sha256,
            self.bound_common_cut_identity_sha256,
        )
        if self.exclusive_canonical_cursor == 0:
            if any(item is not None for item in bindings):
                raise RecoveryError("synthetic cursor zero cannot claim checkpoint coverage")
        else:
            for name, value in zip(
                (
                    "external_cursor_receipt_sha256",
                    "bound_checkpoint_sha256",
                    "bound_common_cut_identity_sha256",
                ),
                bindings,
            ):
                _fixed_bytes(value, 32, name)

    @property
    def route_key(self) -> tuple[object, ...]:
        return (self.family.value,) + _source_namespace_key(self.namespace)

    def canonical_object(self) -> dict[str, object]:
        return {
            "exclusive_canonical_cursor": self.exclusive_canonical_cursor,
            "family": self.family.value,
            "external_cursor_receipt_sha256": (
                None
                if self.external_cursor_receipt_sha256 is None
                else self.external_cursor_receipt_sha256.hex()
            ),
            "bound_checkpoint_sha256": (
                None
                if self.bound_checkpoint_sha256 is None
                else self.bound_checkpoint_sha256.hex()
            ),
            "bound_common_cut_identity_sha256": (
                None
                if self.bound_common_cut_identity_sha256 is None
                else self.bound_common_cut_identity_sha256.hex()
            ),
            "max_consumed_origin_wal_end_pos": self.max_consumed_origin_wal_end_pos,
            "namespace": _source_namespace_object(self.namespace),
        }


@dataclass(frozen=True, slots=True)
class LatestStateDurabilityBarrier:
    family: LatestStateInputFamily
    namespace: SourceNamespace
    checkpoint_exclusive_canonical_cursor: int
    max_consumed_shard_event_id: int
    max_consumed_origin_ingress_sequence: int
    max_consumed_origin_wal_end_pos: int
    current_raw_durable_wal_pos: int
    raw_authority_evidence_sha256: bytes

    def __post_init__(self) -> None:
        _enum(self.family, LatestStateInputFamily, "family")
        if type(self.namespace) is not SourceNamespace:
            raise RecoveryError("Latest barrier namespace must be exact SourceNamespace")
        _validate_latest_family_namespace(self.family, self.namespace)
        for name in (
            "checkpoint_exclusive_canonical_cursor",
            "max_consumed_shard_event_id",
            "max_consumed_origin_ingress_sequence",
            "max_consumed_origin_wal_end_pos",
            "current_raw_durable_wal_pos",
        ):
            _uint(getattr(self, name), 64, name)
        _fixed_bytes(
            self.raw_authority_evidence_sha256,
            32,
            "raw_authority_evidence_sha256",
        )
        if self.current_raw_durable_wal_pos < self.max_consumed_origin_wal_end_pos:
            raise RecoveryError("Latest checkpoint barrier is not Raw-durable")
        if (self.checkpoint_exclusive_canonical_cursor == 0) != (
            self.max_consumed_origin_wal_end_pos == 0
        ):
            raise RecoveryError("Latest barrier zero cursor/WAL must agree")
        # Phase-6 BarrierValid accepts only initialized checkpoint routes.
        if (
            self.checkpoint_exclusive_canonical_cursor == 0
            or self.max_consumed_shard_event_id == 0
            or self.max_consumed_origin_ingress_sequence == 0
            or self.max_consumed_origin_wal_end_pos == 0
        ):
            raise RecoveryError(
                "Latest checkpoint barrier requires nonzero initialized progress"
            )

    @property
    def route_key(self) -> tuple[object, ...]:
        return (self.family.value,) + _source_namespace_key(self.namespace)

    def canonical_object(self) -> dict[str, object]:
        return {
            "checkpoint_exclusive_canonical_cursor": (
                self.checkpoint_exclusive_canonical_cursor
            ),
            "current_raw_durable_wal_pos": self.current_raw_durable_wal_pos,
            "family": self.family.value,
            "max_consumed_origin_ingress_sequence": (
                self.max_consumed_origin_ingress_sequence
            ),
            "max_consumed_origin_wal_end_pos": (
                self.max_consumed_origin_wal_end_pos
            ),
            "max_consumed_shard_event_id": self.max_consumed_shard_event_id,
            "namespace": _source_namespace_object(self.namespace),
            "raw_authority_evidence_sha256": (
                self.raw_authority_evidence_sha256.hex()
            ),
        }


@dataclass(frozen=True, slots=True)
class LatestStateCheckpointEvidence:
    config: LatestStateConfigIdentity
    checkpoint_sha256: bytes
    payload_sha256: bytes
    logical_state_sha256: bytes
    common_cut_identity_sha256: bytes
    cursors: tuple[LatestStateCheckpointCursor, ...]
    durability_barriers: tuple[LatestStateDurabilityBarrier, ...]
    writer_quiesced: bool
    durability_barrier_satisfied: bool
    checkpoint_codec_validated: bool
    logical_hash_validated: bool

    def __post_init__(self) -> None:
        if type(self.config) is not LatestStateConfigIdentity:
            raise RecoveryError("checkpoint config must be LatestStateConfigIdentity")
        for name in (
            "checkpoint_sha256",
            "payload_sha256",
            "logical_state_sha256",
            "common_cut_identity_sha256",
        ):
            _fixed_bytes(getattr(self, name), 32, name)
        cursors = _typed_tuple(
            self.cursors,
            LatestStateCheckpointCursor,
            "checkpoint cursors",
        )
        barriers = _typed_tuple(
            self.durability_barriers,
            LatestStateDurabilityBarrier,
            "checkpoint durability barriers",
        )
        for name in (
            "writer_quiesced",
            "durability_barrier_satisfied",
            "checkpoint_codec_validated",
            "logical_hash_validated",
        ):
            _exact_bool(getattr(self, name), name)
        ordered_cursors = tuple(sorted(cursors, key=lambda item: item.route_key))
        ordered_barriers = tuple(sorted(barriers, key=lambda item: item.route_key))
        cursor_keys = [item.route_key for item in ordered_cursors]
        barrier_keys = [item.route_key for item in ordered_barriers]
        if (
            len(cursor_keys) != len(set(cursor_keys))
            or len(barrier_keys) != len(set(barrier_keys))
            or cursor_keys != barrier_keys
        ):
            raise RecoveryConflictError(
                "Latest checkpoint cursors/barriers must name the same exact routes"
            )
        for cursor, barrier in zip(ordered_cursors, ordered_barriers):
            if (
                cursor.exclusive_canonical_cursor
                != barrier.checkpoint_exclusive_canonical_cursor
                or cursor.max_consumed_origin_wal_end_pos
                != barrier.max_consumed_origin_wal_end_pos
                or barrier.raw_authority_evidence_sha256
                != self.common_cut_identity_sha256
                or cursor.bound_checkpoint_sha256 != self.checkpoint_sha256
                or cursor.bound_common_cut_identity_sha256
                != self.common_cut_identity_sha256
            ):
                raise RecoveryConflictError(
                    "Latest checkpoint cursor, Raw barrier, and common cut disagree"
                )
        if LatestStateInputFamily.SNAPSHOT not in {
            item.family for item in ordered_cursors
        }:
            raise RecoveryConflictError("Latest checkpoint must retain Snapshot input")
        object.__setattr__(self, "cursors", ordered_cursors)
        object.__setattr__(self, "durability_barriers", ordered_barriers)

    def canonical_object(self) -> dict[str, object]:
        return {
            "checkpoint_codec_validated": self.checkpoint_codec_validated,
            "checkpoint_sha256": self.checkpoint_sha256.hex(),
            "common_cut_identity_sha256": self.common_cut_identity_sha256.hex(),
            "config": self.config.canonical_object(),
            "cursors": [item.canonical_object() for item in self.cursors],
            "durability_barrier_satisfied": self.durability_barrier_satisfied,
            "durability_barriers": [
                item.canonical_object() for item in self.durability_barriers
            ],
            "logical_hash_validated": self.logical_hash_validated,
            "logical_state_sha256": self.logical_state_sha256.hex(),
            "payload_sha256": self.payload_sha256.hex(),
            "writer_quiesced": self.writer_quiesced,
        }


@dataclass(frozen=True, slots=True)
class LatestStateCheckpointExpectation:
    config: LatestStateConfigIdentity
    checkpoint_sha256: bytes
    payload_sha256: bytes
    logical_state_sha256: bytes

    def __post_init__(self) -> None:
        if type(self.config) is not LatestStateConfigIdentity:
            raise RecoveryError("expected config must be LatestStateConfigIdentity")
        for name in (
            "checkpoint_sha256",
            "payload_sha256",
            "logical_state_sha256",
        ):
            _fixed_bytes(getattr(self, name), 32, name)

    def canonical_object(self) -> dict[str, object]:
        return {
            "checkpoint_sha256": self.checkpoint_sha256.hex(),
            "config": self.config.canonical_object(),
            "logical_state_sha256": self.logical_state_sha256.hex(),
            "payload_sha256": self.payload_sha256.hex(),
        }


@dataclass(frozen=True, slots=True)
class LatestStateWriterReuseLease:
    state_generation: int
    state_writer_instance: bytes
    lease_and_fence_evidence_sha256: bytes
    exclusive_writer_lease_held: bool
    all_writers_quiesced: bool
    exact_identity_restore_authorized: bool
    alias_switch_authorized: bool = False

    def __post_init__(self) -> None:
        _uint(self.state_generation, 64, "state_generation", positive=True)
        _fixed_bytes(self.state_writer_instance, 16, "state_writer_instance")
        _fixed_bytes(
            self.lease_and_fence_evidence_sha256,
            32,
            "lease_and_fence_evidence_sha256",
        )
        for name in (
            "exclusive_writer_lease_held",
            "all_writers_quiesced",
            "exact_identity_restore_authorized",
            "alias_switch_authorized",
        ):
            _exact_bool(getattr(self, name), name)

    def canonical_object(self) -> dict[str, object]:
        return {
            "alias_switch_authorized": self.alias_switch_authorized,
            "all_writers_quiesced": self.all_writers_quiesced,
            "exact_identity_restore_authorized": (
                self.exact_identity_restore_authorized
            ),
            "exclusive_writer_lease_held": self.exclusive_writer_lease_held,
            "lease_and_fence_evidence_sha256": (
                self.lease_and_fence_evidence_sha256.hex()
            ),
            "state_generation": self.state_generation,
            "state_writer_instance": self.state_writer_instance.hex(),
        }


@dataclass(frozen=True, slots=True)
class LatestStateFullReplayReset:
    previous_state_generation: int
    previous_state_writer_instance: bytes
    candidate_config: LatestStateConfigIdentity
    targets_all_zero: bool
    isolated_generation: bool
    reset_and_warmup_required: bool
    alias_switch_authorized: bool = False

    def __post_init__(self) -> None:
        _uint(
            self.previous_state_generation,
            64,
            "previous_state_generation",
            positive=True,
        )
        _fixed_bytes(
            self.previous_state_writer_instance,
            16,
            "previous_state_writer_instance",
        )
        if type(self.candidate_config) is not LatestStateConfigIdentity:
            raise RecoveryError("candidate_config must be LatestStateConfigIdentity")
        for name in (
            "targets_all_zero",
            "isolated_generation",
            "reset_and_warmup_required",
            "alias_switch_authorized",
        ):
            _exact_bool(getattr(self, name), name)
        if self.candidate_config.state_generation <= self.previous_state_generation:
            raise RecoveryError("full replay requires a fresh state generation")
        if (
            self.candidate_config.state_writer_instance
            == self.previous_state_writer_instance
        ):
            raise RecoveryError("full replay requires fresh state writer ownership")

    def canonical_object(self) -> dict[str, object]:
        return {
            "alias_switch_authorized": self.alias_switch_authorized,
            "candidate_config": self.candidate_config.canonical_object(),
            "isolated_generation": self.isolated_generation,
            "previous_state_generation": self.previous_state_generation,
            "previous_state_writer_instance": (
                self.previous_state_writer_instance.hex()
            ),
            "reset_and_warmup_required": self.reset_and_warmup_required,
            "targets_all_zero": self.targets_all_zero,
        }


@dataclass(frozen=True, slots=True)
class LatestStateInputTarget:
    family: LatestStateInputFamily
    namespace: SourceNamespace
    end_exclusive_canonical_cursor: int
    end_max_consumed_origin_wal_end_pos: int
    health: SourceHealth

    def __post_init__(self) -> None:
        _enum(self.family, LatestStateInputFamily, "family")
        if type(self.namespace) is not SourceNamespace:
            raise RecoveryError("Latest target namespace must be exact SourceNamespace")
        _validate_latest_family_namespace(self.family, self.namespace)
        _uint(
            self.end_exclusive_canonical_cursor,
            64,
            "end_exclusive_canonical_cursor",
        )
        _uint(
            self.end_max_consumed_origin_wal_end_pos,
            64,
            "end_max_consumed_origin_wal_end_pos",
        )
        _enum(self.health, SourceHealth, "health")
        if (self.end_exclusive_canonical_cursor == 0) != (
            self.end_max_consumed_origin_wal_end_pos == 0
        ):
            raise RecoveryError("Latest target zero cursor/WAL must agree")

    @property
    def route_key(self) -> tuple[object, ...]:
        return (self.family.value,) + _source_namespace_key(self.namespace)

    def canonical_object(self) -> dict[str, object]:
        return {
            "end_exclusive_canonical_cursor": self.end_exclusive_canonical_cursor,
            "end_max_consumed_origin_wal_end_pos": (
                self.end_max_consumed_origin_wal_end_pos
            ),
            "family": self.family.value,
            "health": self.health.value,
            "namespace": _source_namespace_object(self.namespace),
        }


def latest_input_routes_sha256(
    targets: tuple[LatestStateInputTarget, ...],
) -> bytes:
    """Return the exact configured Latest input-route manifest identity."""

    values = _typed_tuple(
        targets,
        LatestStateInputTarget,
        "configured Latest input targets",
    )
    ordered = tuple(sorted(values, key=lambda item: item.route_key))
    keys = [item.route_key for item in ordered]
    if len(keys) != len(set(keys)):
        raise RecoveryConflictError("configured Latest input route is duplicated")
    return _object_sha256(
        b"l2flow.recovery.latest-configured-input-routes.v1\x00",
        [item.canonical_object() for item in ordered],
    )


@dataclass(frozen=True, slots=True)
class LatestStateReplaySpan:
    family: LatestStateInputFamily
    namespace: SourceNamespace
    begin_exclusive_canonical_cursor: int
    end_exclusive_canonical_cursor: int
    prior_max_consumed_origin_wal_end_pos: int
    end_max_consumed_origin_wal_end_pos: int
    current_raw_durable_wal_pos: int
    record_count: int
    ordered_content_sha256: bytes
    raw_authority_evidence_sha256: bytes
    exact_records_verified: bool
    authoritative_snapshot_payload: bool
    health: SourceHealth

    def __post_init__(self) -> None:
        _enum(self.family, LatestStateInputFamily, "family")
        if type(self.namespace) is not SourceNamespace:
            raise RecoveryError("Latest replay namespace must be exact SourceNamespace")
        _validate_latest_family_namespace(self.family, self.namespace)
        for name in (
            "begin_exclusive_canonical_cursor",
            "end_exclusive_canonical_cursor",
            "prior_max_consumed_origin_wal_end_pos",
            "end_max_consumed_origin_wal_end_pos",
            "current_raw_durable_wal_pos",
            "record_count",
        ):
            _uint(getattr(self, name), 64, name)
        _fixed_bytes(self.ordered_content_sha256, 32, "ordered_content_sha256")
        _fixed_bytes(
            self.raw_authority_evidence_sha256,
            32,
            "raw_authority_evidence_sha256",
        )
        _exact_bool(self.exact_records_verified, "exact_records_verified")
        _exact_bool(
            self.authoritative_snapshot_payload,
            "authoritative_snapshot_payload",
        )
        _enum(self.health, SourceHealth, "health")
        if self.end_exclusive_canonical_cursor < self.begin_exclusive_canonical_cursor:
            raise RecoveryError("Latest replay cursor range regresses")
        if self.record_count != (
            self.end_exclusive_canonical_cursor
            - self.begin_exclusive_canonical_cursor
        ):
            raise RecoveryError("Latest replay record count differs from cursor range")
        if (
            self.end_max_consumed_origin_wal_end_pos
            < self.prior_max_consumed_origin_wal_end_pos
            or self.current_raw_durable_wal_pos
            < self.end_max_consumed_origin_wal_end_pos
        ):
            raise RecoveryError("Latest replay WAL/durability range regresses")
        if self.family is LatestStateInputFamily.SNAPSHOT:
            if self.authoritative_snapshot_payload is not True:
                raise RecoveryError("Snapshot replay must be authoritative for book payload")
        elif self.authoritative_snapshot_payload is not False:
            raise RecoveryError("TickQuality must never claim book reconstruction authority")

    @property
    def route_key(self) -> tuple[object, ...]:
        return (self.family.value,) + _source_namespace_key(self.namespace)

    @property
    def sort_key(self) -> tuple[object, ...]:
        return self.route_key + (self.begin_exclusive_canonical_cursor,)

    def canonical_object(self) -> dict[str, object]:
        return {
            "authoritative_snapshot_payload": self.authoritative_snapshot_payload,
            "begin_exclusive_canonical_cursor": (
                self.begin_exclusive_canonical_cursor
            ),
            "current_raw_durable_wal_pos": self.current_raw_durable_wal_pos,
            "end_exclusive_canonical_cursor": self.end_exclusive_canonical_cursor,
            "end_max_consumed_origin_wal_end_pos": (
                self.end_max_consumed_origin_wal_end_pos
            ),
            "exact_records_verified": self.exact_records_verified,
            "family": self.family.value,
            "health": self.health.value,
            "namespace": _source_namespace_object(self.namespace),
            "ordered_content_sha256": self.ordered_content_sha256.hex(),
            "prior_max_consumed_origin_wal_end_pos": (
                self.prior_max_consumed_origin_wal_end_pos
            ),
            "raw_authority_evidence_sha256": (
                self.raw_authority_evidence_sha256.hex()
            ),
            "record_count": self.record_count,
        }


@dataclass(frozen=True, slots=True)
class LatestStateRecoveryRequest:
    trade_date: int
    recovery_run_id: bytes
    output_config: LatestStateConfigIdentity
    expected_input_targets: tuple[LatestStateInputTarget, ...]
    configured_input_route_manifest_sha256: bytes
    input_targets: tuple[LatestStateInputTarget, ...]
    replay_spans: tuple[LatestStateReplaySpan, ...]
    targets_all_zero: bool
    expected_final_logical_state_sha256: bytes
    checkpoint: LatestStateCheckpointEvidence | None = None
    checkpoint_expectation: LatestStateCheckpointExpectation | None = None
    writer_reuse_lease: LatestStateWriterReuseLease | None = None
    full_replay_reset: LatestStateFullReplayReset | None = None

    def __post_init__(self) -> None:
        _uint(self.trade_date, 32, "trade_date", positive=True)
        _fixed_bytes(self.recovery_run_id, 16, "recovery_run_id")
        if type(self.output_config) is not LatestStateConfigIdentity:
            raise RecoveryError("output_config must be LatestStateConfigIdentity")
        _typed_tuple(
            self.expected_input_targets,
            LatestStateInputTarget,
            "expected_input_targets",
        )
        _fixed_bytes(
            self.configured_input_route_manifest_sha256,
            32,
            "configured_input_route_manifest_sha256",
        )
        _typed_tuple(self.input_targets, LatestStateInputTarget, "input_targets")
        _typed_tuple(self.replay_spans, LatestStateReplaySpan, "replay_spans")
        _exact_bool(self.targets_all_zero, "targets_all_zero")
        _fixed_bytes(
            self.expected_final_logical_state_sha256,
            32,
            "expected_final_logical_state_sha256",
        )
        if self.checkpoint is not None and type(
            self.checkpoint
        ) is not LatestStateCheckpointEvidence:
            raise RecoveryError("checkpoint has the wrong type")
        if self.checkpoint_expectation is not None and type(
            self.checkpoint_expectation
        ) is not LatestStateCheckpointExpectation:
            raise RecoveryError("checkpoint_expectation has the wrong type")
        if self.writer_reuse_lease is not None and type(
            self.writer_reuse_lease
        ) is not LatestStateWriterReuseLease:
            raise RecoveryError("writer_reuse_lease has the wrong type")
        if self.full_replay_reset is not None and type(
            self.full_replay_reset
        ) is not LatestStateFullReplayReset:
            raise RecoveryError("full_replay_reset has the wrong type")


@dataclass(frozen=True, slots=True)
class LatestStateRecoveryPlan:
    mode: RecoveryMode
    trade_date: int
    recovery_run_id: bytes
    output_config: LatestStateConfigIdentity
    configured_input_route_manifest_sha256: bytes
    expected_final_logical_state_sha256: bytes
    request_evidence_sha256: bytes
    checkpoint_sha256: bytes | None
    start_cursors: tuple[LatestStateCheckpointCursor, ...]
    input_targets: tuple[LatestStateInputTarget, ...]
    replay_spans: tuple[LatestStateReplaySpan, ...]
    steps: tuple[str, ...]
    _construction_token: InitVar[object] = None
    plan_sha256: bytes = field(init=False)
    executes_restore: bool = field(init=False, default=False)
    authorizes_alias_switch: bool = field(init=False, default=False)

    def __post_init__(self, _construction_token: object) -> None:
        if _construction_token is not _PLAN_TOKEN:
            raise RecoveryError("use build_latest_state_recovery_plan")
        value = {
            "authorizes_alias_switch": False,
            "checkpoint_sha256": (
                None if self.checkpoint_sha256 is None else self.checkpoint_sha256.hex()
            ),
            "encoding": "l2flow-latest-state-recovery-plan-v1",
            "executes_restore": False,
            "input_targets": [item.canonical_object() for item in self.input_targets],
            "mode": self.mode.value,
            "trade_date": self.trade_date,
            "recovery_run_id": self.recovery_run_id.hex(),
            "configured_input_route_manifest_sha256": (
                self.configured_input_route_manifest_sha256.hex()
            ),
            "expected_final_logical_state_sha256": (
                self.expected_final_logical_state_sha256.hex()
            ),
            "request_evidence_sha256": self.request_evidence_sha256.hex(),
            "output_config": self.output_config.canonical_object(),
            "replay_spans": [item.canonical_object() for item in self.replay_spans],
            "start_cursors": [item.canonical_object() for item in self.start_cursors],
            "steps": list(self.steps),
        }
        object.__setattr__(
            self,
            "plan_sha256",
            _object_sha256(b"l2flow.recovery.latest-state-plan.v1\x00", value),
        )


def _validate_latest_replay_coverage(
    *,
    targets: tuple[LatestStateInputTarget, ...],
    spans: tuple[LatestStateReplaySpan, ...],
    starts: dict[tuple[object, ...], LatestStateCheckpointCursor],
) -> None:
    target_by_key = {item.route_key: item for item in targets}
    if len(target_by_key) != len(targets):
        raise RecoveryConflictError("Latest input target route is duplicated")
    if LatestStateInputFamily.SNAPSHOT not in {item.family for item in targets}:
        raise RecoveryConflictError("Latest recovery requires a Snapshot target route")
    grouped: dict[tuple[object, ...], list[LatestStateReplaySpan]] = {}
    for span in spans:
        if span.route_key not in target_by_key:
            raise RecoveryConflictError("Latest replay span names an extra route")
        grouped.setdefault(span.route_key, []).append(span)
    if set(grouped) != set(target_by_key) or set(starts) != set(target_by_key):
        raise RecoveryConflictError("Latest replay is missing an exact input route")
    for key, target in target_by_key.items():
        if target.health is not SourceHealth.HEALTHY:
            raise RecoveryConflictError("Latest input route is missing/disconnected/FATAL")
        start = starts[key]
        expected_cursor = start.exclusive_canonical_cursor
        expected_wal = start.max_consumed_origin_wal_end_pos
        selected = sorted(
            grouped[key], key=lambda item: item.begin_exclusive_canonical_cursor
        )
        for span in selected:
            if (
                span.health is not SourceHealth.HEALTHY
                or span.exact_records_verified is not True
            ):
                raise RecoveryConflictError("Latest replay span is not healthy/exact")
            if span.begin_exclusive_canonical_cursor != expected_cursor:
                raise RecoveryConflictError(
                    "Latest replay cursor ranges overlap, gap, or skip to latest"
                )
            if span.prior_max_consumed_origin_wal_end_pos != expected_wal:
                raise RecoveryConflictError("Latest replay WAL tail has a gap")
            expected_cursor = span.end_exclusive_canonical_cursor
            expected_wal = span.end_max_consumed_origin_wal_end_pos
        if (
            expected_cursor != target.end_exclusive_canonical_cursor
            or expected_wal != target.end_max_consumed_origin_wal_end_pos
        ):
            raise RecoveryConflictError("Latest replay does not reach its exact target")


def build_latest_state_recovery_plan(
    request: LatestStateRecoveryRequest,
) -> LatestStateRecoveryPlan:
    if type(request) is not LatestStateRecoveryRequest:
        raise RecoveryError("request must be exact LatestStateRecoveryRequest")
    if request.targets_all_zero is not True:
        raise RecoveryConflictError("Latest recovery requires an exact all-zero target set")
    expected_targets = tuple(
        sorted(request.expected_input_targets, key=lambda item: item.route_key)
    )
    expected_keys = [item.route_key for item in expected_targets]
    if len(expected_keys) != len(set(expected_keys)):
        raise RecoveryConflictError("configured Latest input route is duplicated")
    route_manifest = latest_input_routes_sha256(expected_targets)
    if route_manifest != request.configured_input_route_manifest_sha256:
        raise RecoveryIntegrityError("Latest configured input route manifest mismatch")
    targets = tuple(sorted(request.input_targets, key=lambda item: item.route_key))
    if targets != expected_targets:
        raise RecoveryConflictError(
            "Latest supplied targets do not exactly match configured input routes"
        )
    spans = tuple(sorted(request.replay_spans, key=lambda item: item.sort_key))
    for target in targets:
        namespace = target.namespace
        config = request.output_config
        if (
            namespace.trade_date != request.trade_date
            or namespace.shard_id != config.shard_id
            or namespace.registry_version != config.registry_version
            or namespace.registry_sha256 != config.registry_sha256
            or namespace.schema_sha256 != config.canonical_schema_sha256
            or namespace.dtype_sha256 != config.canonical_dtype_sha256
        ):
            raise RecoveryConflictError(
                "Latest input differs from trade/shard/schema/dtype/registry identity"
            )

    request_evidence = {
        "checkpoint": (
            None if request.checkpoint is None else request.checkpoint.canonical_object()
        ),
        "checkpoint_expectation": (
            None
            if request.checkpoint_expectation is None
            else request.checkpoint_expectation.canonical_object()
        ),
        "configured_input_route_manifest_sha256": (
            request.configured_input_route_manifest_sha256.hex()
        ),
        "expected_final_logical_state_sha256": (
            request.expected_final_logical_state_sha256.hex()
        ),
        "expected_input_targets": [
            item.canonical_object() for item in expected_targets
        ],
        "full_replay_reset": (
            None
            if request.full_replay_reset is None
            else request.full_replay_reset.canonical_object()
        ),
        "input_targets": [item.canonical_object() for item in targets],
        "output_config": request.output_config.canonical_object(),
        "recovery_run_id": request.recovery_run_id.hex(),
        "replay_spans": [item.canonical_object() for item in spans],
        "targets_all_zero": request.targets_all_zero,
        "trade_date": request.trade_date,
        "writer_reuse_lease": (
            None
            if request.writer_reuse_lease is None
            else request.writer_reuse_lease.canonical_object()
        ),
    }
    request_evidence_sha256 = _object_sha256(
        b"l2flow.recovery.latest-request-evidence.v1\x00",
        request_evidence,
    )

    if request.checkpoint is not None:
        checkpoint = request.checkpoint
        expected = request.checkpoint_expectation
        lease = request.writer_reuse_lease
        if (
            expected is None
            or lease is None
            or request.full_replay_reset is not None
        ):
            raise RecoveryConflictError(
                "checkpoint recovery requires exact expectation and reuse lease only"
            )
        if (
            checkpoint.writer_quiesced is not True
            or checkpoint.durability_barrier_satisfied is not True
            or checkpoint.checkpoint_codec_validated is not True
            or checkpoint.logical_hash_validated is not True
        ):
            raise RecoveryConflictError(
                "non-durable/unvalidated Latest checkpoint cannot be authoritative"
            )
        if (
            checkpoint.config != expected.config
            or checkpoint.checkpoint_sha256 != expected.checkpoint_sha256
            or checkpoint.payload_sha256 != expected.payload_sha256
            or checkpoint.logical_state_sha256 != expected.logical_state_sha256
        ):
            raise RecoveryConflictError("Latest checkpoint exact identity mismatch")
        # V1 RestoreLatestStateCheckpoint has no rebase API.  Logical hash
        # normalization of owner/generation does not relax this exact config.
        if request.output_config != checkpoint.config:
            raise RecoveryConflictError(
                "Latest V1 checkpoint cannot be rebased to a fresh config"
            )
        if (
            lease.state_generation != checkpoint.config.state_generation
            or lease.state_writer_instance != checkpoint.config.state_writer_instance
            or lease.exclusive_writer_lease_held is not True
            or lease.all_writers_quiesced is not True
            or lease.exact_identity_restore_authorized is not True
            or lease.alias_switch_authorized is not False
        ):
            raise RecoveryConflictError(
                "Latest exact-config restore lacks the external lease/fence"
            )
        starts = {item.route_key: item for item in checkpoint.cursors}
        if len(starts) != len(checkpoint.cursors):
            raise RecoveryConflictError("Latest checkpoint cursor is duplicated")
        _validate_latest_replay_coverage(
            targets=targets,
            spans=spans,
            starts=starts,
        )
        checkpoint_steps = (
            "validate_exact_durable_checkpoint_and_external_writer_lease",
            "plan_native_restore_into_exact_all_zero_targets_without_config_rebase",
            "replay_snapshot_tail_from_exclusive_checkpoint_cursors",
        )
        if any(
            item.family is LatestStateInputFamily.TICK_QUALITY for item in targets
        ):
            checkpoint_steps += (
                "replay_tick_quality_tail_from_exclusive_checkpoint_cursors",
                "never_use_tick_as_authoritative_book_payload",
            )
        checkpoint_steps += (
            "verify_logical_state_hash_in_recovering_generation",
            "emit_plan_only_without_alias_switch",
        )
        return LatestStateRecoveryPlan(
            mode=RecoveryMode.LATEST_CHECKPOINT_TAIL,
            trade_date=request.trade_date,
            recovery_run_id=request.recovery_run_id,
            output_config=request.output_config,
            configured_input_route_manifest_sha256=(
                request.configured_input_route_manifest_sha256
            ),
            expected_final_logical_state_sha256=(
                request.expected_final_logical_state_sha256
            ),
            request_evidence_sha256=request_evidence_sha256,
            checkpoint_sha256=checkpoint.checkpoint_sha256,
            start_cursors=tuple(sorted(starts.values(), key=lambda item: item.route_key)),
            input_targets=targets,
            replay_spans=spans,
            steps=checkpoint_steps,
            _construction_token=_PLAN_TOKEN,
        )

    if (
        request.checkpoint_expectation is not None
        or request.writer_reuse_lease is not None
        or request.full_replay_reset is None
    ):
        raise RecoveryConflictError(
            "full Latest replay requires no checkpoint claim and an explicit reset"
        )
    reset = request.full_replay_reset
    if (
        reset.candidate_config != request.output_config
        or reset.targets_all_zero is not True
        or reset.isolated_generation is not True
        or reset.reset_and_warmup_required is not True
        or reset.alias_switch_authorized is not False
    ):
        raise RecoveryConflictError("Latest full-replay reset contract is incomplete")
    starts = {
        item.route_key: LatestStateCheckpointCursor(
            family=item.family,
            namespace=item.namespace,
            exclusive_canonical_cursor=0,
            max_consumed_origin_wal_end_pos=0,
            external_cursor_receipt_sha256=None,
            bound_checkpoint_sha256=None,
            bound_common_cut_identity_sha256=None,
        )
        for item in targets
    }
    _validate_latest_replay_coverage(targets=targets, spans=spans, starts=starts)
    full_replay_steps = (
        "create_fresh_isolated_latest_state_config_and_zero_targets",
        "replay_full_authoritative_snapshot_inputs_from_cursor_zero",
    )
    if any(item.family is LatestStateInputFamily.TICK_QUALITY for item in targets):
        full_replay_steps += (
            "replay_full_tick_quality_lineage_from_cursor_zero",
            "never_use_tick_as_authoritative_book_payload",
        )
    full_replay_steps += (
        "verify_logical_state_hash_in_recovering_generation",
        "emit_plan_only_without_alias_switch",
    )
    return LatestStateRecoveryPlan(
        mode=RecoveryMode.LATEST_FULL_REPLAY,
        trade_date=request.trade_date,
        recovery_run_id=request.recovery_run_id,
        output_config=request.output_config,
        configured_input_route_manifest_sha256=(
            request.configured_input_route_manifest_sha256
        ),
        expected_final_logical_state_sha256=(
            request.expected_final_logical_state_sha256
        ),
        request_evidence_sha256=request_evidence_sha256,
        checkpoint_sha256=None,
        start_cursors=tuple(sorted(starts.values(), key=lambda item: item.route_key)),
        input_targets=targets,
        replay_spans=spans,
        steps=full_replay_steps,
        _construction_token=_PLAN_TOKEN,
    )


def _watermark_entry_object(entry: FactorInputWatermark) -> dict[str, object]:
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


def _watermark_object(value: FactorInputWatermarkSet) -> dict[str, object]:
    return {
        "entries": [_watermark_entry_object(item) for item in value.entries],
        "input_identity_sha256": value.input_identity_hex(),
        "trade_date": value.trade_date,
        "watermark_set_id": value.watermark_set_id,
    }


def _watermark_route_key(entry: FactorInputWatermark) -> tuple[object, ...]:
    return (
        entry.source_stream_id,
        entry.origin_capture_date,
        entry.origin_stream_day_id,
        entry.family.canonical_event_type,
        entry.shard_id,
    )


def _namespace_watermark_key(namespace: SourceNamespace) -> tuple[object, ...]:
    return (
        namespace.source_stream_id,
        namespace.origin_capture_date,
        namespace.origin_stream_day_id,
        namespace.family.canonical_event_type,
        namespace.shard_id,
    )


def factor_input_routes_sha256(
    namespaces: tuple[SourceNamespace, ...],
) -> bytes:
    """Return the exact expected Factor Canonical-input route identity."""

    values = _typed_tuple(
        namespaces,
        SourceNamespace,
        "expected Factor input namespaces",
    )
    ordered = tuple(sorted(values, key=lambda item: item.sort_key))
    keys = [_namespace_watermark_key(item) for item in ordered]
    if len(keys) != len(set(keys)):
        raise RecoveryConflictError("expected Factor input route is duplicated")
    return _object_sha256(
        b"l2flow.recovery.factor-configured-input-routes.v1\x00",
        [_source_namespace_object(item) for item in ordered],
    )


@dataclass(frozen=True, slots=True)
class FactorRecoveryIdentity:
    factor_id: str
    factor_version: str
    factor_code_sha256: bytes
    factor_config_sha256: bytes
    state_schema_sha256: bytes
    registry_version: int
    registry_sha256: bytes
    factor_spec: FactorSpec
    state_schema_version: int
    factor_shard_id: int
    trade_date: int
    clock_epoch_algorithm: int
    clock_epoch_digest: bytes
    canonical_schema_sha256: bytes
    canonical_dtype_sha256: bytes
    expected_input_namespaces: tuple[SourceNamespace, ...]
    expected_input_route_manifest_sha256: bytes
    expected_native_mux_binding_sha256: bytes

    def __post_init__(self) -> None:
        _token(self.factor_id, "factor_id")
        _token(self.factor_version, "factor_version")
        for name in (
            "factor_code_sha256",
            "factor_config_sha256",
            "state_schema_sha256",
            "registry_sha256",
        ):
            _fixed_bytes(getattr(self, name), 32, name)
        _uint(self.registry_version, 64, "registry_version", positive=True)
        if type(self.factor_spec) is not FactorSpec:
            raise RecoveryError("factor_spec must be exact FactorSpec")
        _uint(
            self.state_schema_version,
            32,
            "state_schema_version",
            positive=True,
        )
        _uint(self.factor_shard_id, 32, "factor_shard_id")
        if self.factor_shard_id >= 16:
            raise RecoveryError("factor_shard_id must be in [0, 15]")
        _uint(self.trade_date, 32, "trade_date", positive=True)
        _uint(
            self.clock_epoch_algorithm,
            32,
            "clock_epoch_algorithm",
            positive=True,
        )
        _fixed_bytes(self.clock_epoch_digest, 32, "clock_epoch_digest")
        _fixed_bytes(
            self.canonical_schema_sha256,
            32,
            "canonical_schema_sha256",
        )
        _fixed_bytes(
            self.canonical_dtype_sha256,
            32,
            "canonical_dtype_sha256",
        )
        namespaces = _typed_tuple(
            self.expected_input_namespaces,
            SourceNamespace,
            "expected_input_namespaces",
        )
        ordered_namespaces = tuple(sorted(namespaces, key=lambda item: item.sort_key))
        keys = [_namespace_watermark_key(item) for item in ordered_namespaces]
        if len(keys) != len(set(keys)):
            raise RecoveryConflictError("expected Factor input route is duplicated")
        object.__setattr__(self, "expected_input_namespaces", ordered_namespaces)
        _fixed_bytes(
            self.expected_input_route_manifest_sha256,
            32,
            "expected_input_route_manifest_sha256",
        )
        if (
            factor_input_routes_sha256(ordered_namespaces)
            != self.expected_input_route_manifest_sha256
        ):
            raise RecoveryIntegrityError(
                "expected Factor input route manifest hash mismatch"
            )
        _fixed_bytes(
            self.expected_native_mux_binding_sha256,
            32,
            "expected_native_mux_binding_sha256",
        )
        if (
            self.factor_spec.factor_id != self.factor_id
            or self.factor_spec.factor_version != self.factor_version
            or self.factor_spec.state_schema_version != self.state_schema_version
            or self.factor_spec.sha256() != self.factor_config_sha256
        ):
            raise RecoveryError("FactorSpec identity differs from recovery identity")

    def canonical_object(self) -> dict[str, object]:
        return {
            "factor_code_sha256": self.factor_code_sha256.hex(),
            "factor_config_sha256": self.factor_config_sha256.hex(),
            "factor_id": self.factor_id,
            "factor_version": self.factor_version,
            "registry_sha256": self.registry_sha256.hex(),
            "registry_version": self.registry_version,
            "state_schema_sha256": self.state_schema_sha256.hex(),
            "factor_spec": self.factor_spec.canonical_object(),
            "state_schema_version": self.state_schema_version,
            "factor_shard_id": self.factor_shard_id,
            "trade_date": self.trade_date,
            "clock_epoch_algorithm": self.clock_epoch_algorithm,
            "clock_epoch_digest": self.clock_epoch_digest.hex(),
            "canonical_schema_sha256": self.canonical_schema_sha256.hex(),
            "canonical_dtype_sha256": self.canonical_dtype_sha256.hex(),
            "expected_input_namespaces": [
                _source_namespace_object(item)
                for item in self.expected_input_namespaces
            ],
            "expected_input_route_manifest_sha256": (
                self.expected_input_route_manifest_sha256.hex()
            ),
            "expected_native_mux_binding_sha256": (
                self.expected_native_mux_binding_sha256.hex()
            ),
        }


@dataclass(frozen=True, slots=True)
class FactorCheckpointInputCoverage:
    namespace: SourceNamespace
    exclusive_canonical_cursor: int
    max_consumed_origin_wal_end_pos: int
    current_raw_durable_wal_pos: int
    coverage_certificate_sha256: bytes | None

    def __post_init__(self) -> None:
        if type(self.namespace) is not SourceNamespace:
            raise RecoveryError("factor coverage namespace must be SourceNamespace")
        for name in (
            "exclusive_canonical_cursor",
            "max_consumed_origin_wal_end_pos",
            "current_raw_durable_wal_pos",
        ):
            _uint(getattr(self, name), 64, name)
        if self.current_raw_durable_wal_pos < self.max_consumed_origin_wal_end_pos:
            raise RecoveryError("factor checkpoint input is not Raw-durable")
        zero = self.exclusive_canonical_cursor == 0
        if zero != (self.max_consumed_origin_wal_end_pos == 0):
            raise RecoveryError("factor checkpoint zero cursor/WAL must agree")
        if zero:
            if self.coverage_certificate_sha256 is not None:
                raise RecoveryError("zero-consumption factor input cannot claim coverage")
        else:
            _fixed_bytes(
                self.coverage_certificate_sha256,
                32,
                "coverage_certificate_sha256",
            )

    @property
    def route_key(self) -> tuple[object, ...]:
        return _namespace_watermark_key(self.namespace)

    def canonical_object(self) -> dict[str, object]:
        return {
            "coverage_certificate_sha256": (
                None
                if self.coverage_certificate_sha256 is None
                else self.coverage_certificate_sha256.hex()
            ),
            "current_raw_durable_wal_pos": self.current_raw_durable_wal_pos,
            "exclusive_canonical_cursor": self.exclusive_canonical_cursor,
            "max_consumed_origin_wal_end_pos": (
                self.max_consumed_origin_wal_end_pos
            ),
            "namespace": _source_namespace_object(self.namespace),
        }


@dataclass(frozen=True, slots=True)
class FactorInputTarget:
    namespace: SourceNamespace
    end_exclusive_canonical_cursor: int
    end_max_consumed_origin_wal_end_pos: int
    health: SourceHealth
    clock_compatible: bool

    def __post_init__(self) -> None:
        if type(self.namespace) is not SourceNamespace:
            raise RecoveryError("factor target namespace must be SourceNamespace")
        _uint(
            self.end_exclusive_canonical_cursor,
            64,
            "end_exclusive_canonical_cursor",
        )
        _uint(
            self.end_max_consumed_origin_wal_end_pos,
            64,
            "end_max_consumed_origin_wal_end_pos",
        )
        _enum(self.health, SourceHealth, "health")
        _exact_bool(self.clock_compatible, "clock_compatible")
        if (self.end_exclusive_canonical_cursor == 0) != (
            self.end_max_consumed_origin_wal_end_pos == 0
        ):
            raise RecoveryError("factor target zero cursor/WAL must agree")

    @property
    def route_key(self) -> tuple[object, ...]:
        return _namespace_watermark_key(self.namespace)

    def canonical_object(self) -> dict[str, object]:
        return {
            "clock_compatible": self.clock_compatible,
            "end_exclusive_canonical_cursor": self.end_exclusive_canonical_cursor,
            "end_max_consumed_origin_wal_end_pos": (
                self.end_max_consumed_origin_wal_end_pos
            ),
            "health": self.health.value,
            "namespace": _source_namespace_object(self.namespace),
        }


@dataclass(frozen=True, slots=True)
class FactorReplaySpan:
    namespace: SourceNamespace
    begin_exclusive_canonical_cursor: int
    end_exclusive_canonical_cursor: int
    prior_max_consumed_origin_wal_end_pos: int
    end_max_consumed_origin_wal_end_pos: int
    current_raw_durable_wal_pos: int
    record_count: int
    ordered_content_sha256: bytes
    exact_records_verified: bool
    health: SourceHealth
    clock_compatible: bool

    def __post_init__(self) -> None:
        if type(self.namespace) is not SourceNamespace:
            raise RecoveryError("factor replay namespace must be SourceNamespace")
        for name in (
            "begin_exclusive_canonical_cursor",
            "end_exclusive_canonical_cursor",
            "prior_max_consumed_origin_wal_end_pos",
            "end_max_consumed_origin_wal_end_pos",
            "current_raw_durable_wal_pos",
            "record_count",
        ):
            _uint(getattr(self, name), 64, name)
        _fixed_bytes(self.ordered_content_sha256, 32, "ordered_content_sha256")
        _exact_bool(self.exact_records_verified, "exact_records_verified")
        _enum(self.health, SourceHealth, "health")
        _exact_bool(self.clock_compatible, "clock_compatible")
        if self.end_exclusive_canonical_cursor < self.begin_exclusive_canonical_cursor:
            raise RecoveryError("factor replay cursor range regresses")
        if self.record_count != (
            self.end_exclusive_canonical_cursor
            - self.begin_exclusive_canonical_cursor
        ):
            raise RecoveryError("factor replay record count differs from cursor range")
        if (
            self.end_max_consumed_origin_wal_end_pos
            < self.prior_max_consumed_origin_wal_end_pos
            or self.current_raw_durable_wal_pos
            < self.end_max_consumed_origin_wal_end_pos
        ):
            raise RecoveryError("factor replay WAL/durability range regresses")

    @property
    def route_key(self) -> tuple[object, ...]:
        return _namespace_watermark_key(self.namespace)

    @property
    def sort_key(self) -> tuple[object, ...]:
        return self.route_key + (self.begin_exclusive_canonical_cursor,)

    def canonical_object(self) -> dict[str, object]:
        return {
            "begin_exclusive_canonical_cursor": (
                self.begin_exclusive_canonical_cursor
            ),
            "clock_compatible": self.clock_compatible,
            "current_raw_durable_wal_pos": self.current_raw_durable_wal_pos,
            "end_exclusive_canonical_cursor": self.end_exclusive_canonical_cursor,
            "end_max_consumed_origin_wal_end_pos": (
                self.end_max_consumed_origin_wal_end_pos
            ),
            "exact_records_verified": self.exact_records_verified,
            "health": self.health.value,
            "namespace": _source_namespace_object(self.namespace),
            "ordered_content_sha256": self.ordered_content_sha256.hex(),
            "prior_max_consumed_origin_wal_end_pos": (
                self.prior_max_consumed_origin_wal_end_pos
            ),
            "record_count": self.record_count,
        }


@dataclass(frozen=True, slots=True, order=True)
class NativeMuxInputRange:
    namespace: SourceNamespace
    begin_exclusive_canonical_cursor: int
    end_exclusive_canonical_cursor: int
    prior_max_consumed_origin_wal_end_pos: int
    end_max_consumed_origin_wal_end_pos: int
    ordered_content_sha256: bytes

    def __post_init__(self) -> None:
        if type(self.namespace) is not SourceNamespace:
            raise RecoveryError("native mux range namespace must be SourceNamespace")
        for name in (
            "begin_exclusive_canonical_cursor",
            "end_exclusive_canonical_cursor",
            "prior_max_consumed_origin_wal_end_pos",
            "end_max_consumed_origin_wal_end_pos",
        ):
            _uint(getattr(self, name), 64, name)
        _fixed_bytes(self.ordered_content_sha256, 32, "ordered_content_sha256")

    @classmethod
    def from_span(cls, span: FactorReplaySpan) -> "NativeMuxInputRange":
        if type(span) is not FactorReplaySpan:
            raise RecoveryError("span must be FactorReplaySpan")
        return cls(
            namespace=span.namespace,
            begin_exclusive_canonical_cursor=span.begin_exclusive_canonical_cursor,
            end_exclusive_canonical_cursor=span.end_exclusive_canonical_cursor,
            prior_max_consumed_origin_wal_end_pos=(
                span.prior_max_consumed_origin_wal_end_pos
            ),
            end_max_consumed_origin_wal_end_pos=(
                span.end_max_consumed_origin_wal_end_pos
            ),
            ordered_content_sha256=span.ordered_content_sha256,
        )

    @property
    def sort_key(self) -> tuple[object, ...]:
        return _namespace_watermark_key(self.namespace) + (
            self.begin_exclusive_canonical_cursor,
        )

    def canonical_object(self) -> dict[str, object]:
        return {
            "begin_exclusive_canonical_cursor": (
                self.begin_exclusive_canonical_cursor
            ),
            "end_exclusive_canonical_cursor": self.end_exclusive_canonical_cursor,
            "end_max_consumed_origin_wal_end_pos": (
                self.end_max_consumed_origin_wal_end_pos
            ),
            "namespace": _source_namespace_object(self.namespace),
            "ordered_content_sha256": self.ordered_content_sha256.hex(),
            "prior_max_consumed_origin_wal_end_pos": (
                self.prior_max_consumed_origin_wal_end_pos
            ),
        }


def native_safe_mux_binding_sha256(
    *,
    native_request_sha256: bytes,
    native_result_sha256: bytes,
    selection_receipt_sha256: bytes,
    input_ranges: tuple[NativeMuxInputRange, ...],
    decision: NativeSafeMuxDecision,
    live_frontiers_revalidated: bool,
    all_required_inputs_present: bool,
    clock_compatible: bool,
    fatal_observed: bool,
) -> bytes:
    """Bind the native mux implementation receipt to its request and result."""

    _fixed_bytes(native_request_sha256, 32, "native_request_sha256")
    _fixed_bytes(native_result_sha256, 32, "native_result_sha256")
    _fixed_bytes(selection_receipt_sha256, 32, "selection_receipt_sha256")
    ranges = _typed_tuple(
        input_ranges,
        NativeMuxInputRange,
        "native mux input_ranges",
    )
    ordered = tuple(sorted(ranges, key=lambda item: item.sort_key))
    keys = [item.sort_key for item in ordered]
    if len(keys) != len(set(keys)):
        raise RecoveryConflictError("native mux input range is duplicated")
    _enum(decision, NativeSafeMuxDecision, "decision")
    for name, value in (
        ("live_frontiers_revalidated", live_frontiers_revalidated),
        ("all_required_inputs_present", all_required_inputs_present),
        ("clock_compatible", clock_compatible),
        ("fatal_observed", fatal_observed),
    ):
        _exact_bool(value, name)
    return _object_sha256(
        b"l2flow.recovery.native-safe-mux-binding.v1\x00",
        {
            "all_required_inputs_present": all_required_inputs_present,
            "clock_compatible": clock_compatible,
            "decision": decision.value,
            "fatal_observed": fatal_observed,
            "input_ranges": [item.canonical_object() for item in ordered],
            "live_frontiers_revalidated": live_frontiers_revalidated,
            "native_request_sha256": native_request_sha256.hex(),
            "native_result_sha256": native_result_sha256.hex(),
            "selection_receipt_sha256": selection_receipt_sha256.hex(),
        },
    )


@dataclass(frozen=True, slots=True)
class NativeSafeMuxEvidence:
    native_binding_sha256: bytes
    native_request_sha256: bytes
    native_result_sha256: bytes
    selection_receipt_sha256: bytes
    input_ranges: tuple[NativeMuxInputRange, ...]
    decision: NativeSafeMuxDecision
    live_frontiers_revalidated: bool
    all_required_inputs_present: bool
    clock_compatible: bool
    fatal_observed: bool

    def __post_init__(self) -> None:
        _fixed_bytes(self.native_binding_sha256, 32, "native_binding_sha256")
        _fixed_bytes(self.native_request_sha256, 32, "native_request_sha256")
        _fixed_bytes(self.native_result_sha256, 32, "native_result_sha256")
        _fixed_bytes(
            self.selection_receipt_sha256,
            32,
            "selection_receipt_sha256",
        )
        ranges = _typed_tuple(
            self.input_ranges,
            NativeMuxInputRange,
            "native mux input_ranges",
        )
        ordered = tuple(sorted(ranges, key=lambda item: item.sort_key))
        keys = [item.sort_key for item in ordered]
        if len(keys) != len(set(keys)):
            raise RecoveryConflictError("native mux input range is duplicated")
        object.__setattr__(self, "input_ranges", ordered)
        _enum(self.decision, NativeSafeMuxDecision, "decision")
        for name in (
            "live_frontiers_revalidated",
            "all_required_inputs_present",
            "clock_compatible",
            "fatal_observed",
        ):
            _exact_bool(getattr(self, name), name)
        actual_binding = native_safe_mux_binding_sha256(
            native_request_sha256=self.native_request_sha256,
            native_result_sha256=self.native_result_sha256,
            selection_receipt_sha256=self.selection_receipt_sha256,
            input_ranges=self.input_ranges,
            decision=self.decision,
            live_frontiers_revalidated=self.live_frontiers_revalidated,
            all_required_inputs_present=self.all_required_inputs_present,
            clock_compatible=self.clock_compatible,
            fatal_observed=self.fatal_observed,
        )
        if self.native_binding_sha256 != actual_binding:
            raise RecoveryIntegrityError(
                "native mux binding does not bind its request/result/ranges/decision"
            )

    def canonical_object(self) -> dict[str, object]:
        return {
            "all_required_inputs_present": self.all_required_inputs_present,
            "clock_compatible": self.clock_compatible,
            "decision": self.decision.value,
            "fatal_observed": self.fatal_observed,
            "input_ranges": [item.canonical_object() for item in self.input_ranges],
            "live_frontiers_revalidated": self.live_frontiers_revalidated,
            "native_binding_sha256": self.native_binding_sha256.hex(),
            "native_request_sha256": self.native_request_sha256.hex(),
            "native_result_sha256": self.native_result_sha256.hex(),
            "selection_receipt_sha256": self.selection_receipt_sha256.hex(),
        }


@dataclass(frozen=True, slots=True)
class FactorRecoveryRequest:
    expected_identity: FactorRecoveryIdentity
    checkpoint: FactorCheckpoint
    checkpoint_sha256: bytes
    checkpoint_codec_validated: bool
    state_watermark_binding_validated: bool
    checkpoint_input_coverage: tuple[FactorCheckpointInputCoverage, ...]
    input_targets: tuple[FactorInputTarget, ...]
    replay_spans: tuple[FactorReplaySpan, ...]
    native_safe_mux: NativeSafeMuxEvidence
    skip_to_latest_requested: bool = False

    def __post_init__(self) -> None:
        if type(self.expected_identity) is not FactorRecoveryIdentity:
            raise RecoveryError("expected_identity must be FactorRecoveryIdentity")
        if type(self.checkpoint) is not FactorCheckpoint:
            raise RecoveryError("checkpoint must be exact FactorCheckpoint")
        _fixed_bytes(self.checkpoint_sha256, 32, "checkpoint_sha256")
        _exact_bool(self.checkpoint_codec_validated, "checkpoint_codec_validated")
        _exact_bool(
            self.state_watermark_binding_validated,
            "state_watermark_binding_validated",
        )
        _typed_tuple(
            self.checkpoint_input_coverage,
            FactorCheckpointInputCoverage,
            "checkpoint_input_coverage",
        )
        _typed_tuple(self.input_targets, FactorInputTarget, "input_targets")
        _typed_tuple(self.replay_spans, FactorReplaySpan, "replay_spans")
        if type(self.native_safe_mux) is not NativeSafeMuxEvidence:
            raise RecoveryError("native_safe_mux must be NativeSafeMuxEvidence")
        _exact_bool(self.skip_to_latest_requested, "skip_to_latest_requested")


@dataclass(frozen=True, slots=True)
class FactorRecoveryPlan:
    identity: FactorRecoveryIdentity
    checkpoint_sha256: bytes
    watermark_set: FactorInputWatermarkSet
    checkpoint_input_coverage: tuple[FactorCheckpointInputCoverage, ...]
    input_targets: tuple[FactorInputTarget, ...]
    replay_spans: tuple[FactorReplaySpan, ...]
    native_safe_mux: NativeSafeMuxEvidence
    steps: tuple[str, ...]
    _construction_token: InitVar[object] = None
    plan_sha256: bytes = field(init=False)
    executes_checkpoint_restore: bool = field(init=False, default=False)
    authorizes_alias_switch: bool = field(init=False, default=False)

    def __post_init__(self, _construction_token: object) -> None:
        if _construction_token is not _PLAN_TOKEN:
            raise RecoveryError("use build_factor_recovery_plan")
        value = {
            "authorizes_alias_switch": False,
            "checkpoint_input_coverage": [
                item.canonical_object() for item in self.checkpoint_input_coverage
            ],
            "checkpoint_sha256": self.checkpoint_sha256.hex(),
            "encoding": "l2flow-factor-recovery-plan-v1",
            "executes_checkpoint_restore": False,
            "identity": self.identity.canonical_object(),
            "input_targets": [item.canonical_object() for item in self.input_targets],
            "mode": RecoveryMode.FACTOR_CHECKPOINT_TAIL.value,
            "native_safe_mux": self.native_safe_mux.canonical_object(),
            "replay_spans": [item.canonical_object() for item in self.replay_spans],
            "steps": list(self.steps),
            "watermark_set": _watermark_object(self.watermark_set),
        }
        object.__setattr__(
            self,
            "plan_sha256",
            _object_sha256(b"l2flow.recovery.factor-plan.v1\x00", value),
        )


def _validate_factor_namespace(
    namespace: SourceNamespace,
    entry: FactorInputWatermark,
    checkpoint: FactorCheckpoint,
    expected: FactorRecoveryIdentity,
) -> None:
    if _namespace_watermark_key(namespace) != _watermark_route_key(entry):
        raise RecoveryConflictError("factor input namespace differs from watermark entry")
    if (
        namespace.trade_date != checkpoint.watermark_set.trade_date
        or namespace.clock_epoch_algorithm != entry.clock_epoch_algorithm
        or namespace.clock_epoch_digest != entry.clock_epoch_digest
        or namespace.registry_version != checkpoint.registry_version
        or namespace.registry_sha256 != checkpoint.registry_sha256
        or namespace.schema_sha256 != expected.canonical_schema_sha256
        or namespace.dtype_sha256 != expected.canonical_dtype_sha256
    ):
        raise RecoveryConflictError(
            "factor input trade/clock/registry identity differs from checkpoint"
        )


def build_factor_recovery_plan(request: FactorRecoveryRequest) -> FactorRecoveryPlan:
    if type(request) is not FactorRecoveryRequest:
        raise RecoveryError("request must be exact FactorRecoveryRequest")
    expected = request.expected_identity
    checkpoint = request.checkpoint
    if (
        checkpoint.factor_id != expected.factor_id
        or checkpoint.factor_version != expected.factor_version
        or checkpoint.factor_code_sha256 != expected.factor_code_sha256
        or checkpoint.factor_config_sha256 != expected.factor_config_sha256
        or checkpoint.state_schema_sha256 != expected.state_schema_sha256
        or checkpoint.registry_version != expected.registry_version
        or checkpoint.registry_sha256 != expected.registry_sha256
    ):
        raise RecoveryConflictError("factor checkpoint exact identity mismatch")
    if hashlib.sha256(encode_checkpoint(checkpoint)).digest() != request.checkpoint_sha256:
        raise RecoveryIntegrityError("factor checkpoint bytes/hash mismatch")
    if (
        request.checkpoint_codec_validated is not True
        or request.state_watermark_binding_validated is not True
    ):
        raise RecoveryConflictError("factor checkpoint/state/watermark is unvalidated")
    spec = expected.factor_spec
    if (
        spec.input_mode is InputMode.LIVE_LATEST
        or spec.nondeterministic_live_latest
        or any(item.family is InputFamily.LATEST_STATE for item in spec.inputs)
    ):
        raise RecoveryConflictError(
            "LIVE_LATEST is nondeterministic and absent from Canonical watermarks; "
            "checkpoint+tail recovery must reset/warm up instead"
        )
    if request.skip_to_latest_requested:
        raise RecoveryConflictError("factor recovery cannot skip to current Latest")

    if checkpoint.state_codec != RUNTIME_STATE_CODEC_V1:
        raise RecoveryConflictError(
            "deterministic recovery accepts only the repository runtime-state codec"
        )
    try:
        decoded = decode_runtime_checkpoint(checkpoint.state_bytes)
    except Exception as error:
        raise RecoveryIntegrityError("factor runtime checkpoint failed decoding") from error
    if (
        decoded.factor_id != expected.factor_id
        or decoded.factor_version != expected.factor_version
        or decoded.factor_spec_sha256 != spec.sha256()
        or decoded.shard_id != expected.factor_shard_id
        or decoded.run_identity.trade_date != expected.trade_date
        or decoded.run_identity.clock_epoch_algorithm
        != expected.clock_epoch_algorithm
        or decoded.run_identity.clock_epoch_digest != expected.clock_epoch_digest
        or decoded.run_identity.registry_version != expected.registry_version
        or decoded.run_identity.registry_sha256 != expected.registry_sha256
        or decoded.watermark_set != checkpoint.watermark_set
    ):
        raise RecoveryConflictError(
            "decoded runtime state differs from factor/spec/shard/run/watermark identity"
        )

    entries = tuple(checkpoint.watermark_set.entries)
    entry_by_key = {_watermark_route_key(item): item for item in entries}
    if len(entry_by_key) != len(entries):
        raise RecoveryConflictError("factor checkpoint watermark route is duplicated")
    declared_keys = {
        (item.source_stream_id, item.family.canonical_event_type, item.shard_id)
        for item in spec.inputs
    }
    watermark_declared_keys = {
        (item.source_stream_id, item.family.canonical_event_type, item.shard_id)
        for item in entries
    }
    if declared_keys != watermark_declared_keys:
        raise RecoveryConflictError(
            "complete runtime watermark differs from FactorSpec declared inputs"
        )
    expected_namespaces = tuple(
        sorted(expected.expected_input_namespaces, key=lambda item: item.sort_key)
    )
    expected_namespace_by_key = {
        _namespace_watermark_key(item): item for item in expected_namespaces
    }
    if (
        len(expected_namespace_by_key) != len(expected_namespaces)
        or set(expected_namespace_by_key) != set(entry_by_key)
    ):
        raise RecoveryConflictError(
            "expected Factor input route manifest differs from full watermark map"
        )
    generation_tags = dict(decoded.generation_tags)
    if set(generation_tags) != {
        (
            item.source_stream_id,
            item.origin_capture_date,
            item.origin_stream_day_id,
            item.family,
            item.shard_id,
        )
        for item in entries
    }:
        raise RecoveryConflictError("runtime generation tags are incomplete")

    coverages = tuple(
        sorted(request.checkpoint_input_coverage, key=lambda item: item.route_key)
    )
    coverage_by_key = {item.route_key: item for item in coverages}
    targets = tuple(sorted(request.input_targets, key=lambda item: item.route_key))
    target_by_key = {item.route_key: item for item in targets}
    if (
        len(coverage_by_key) != len(coverages)
        or len(target_by_key) != len(targets)
        or set(coverage_by_key) != set(entry_by_key)
        or set(target_by_key) != set(entry_by_key)
    ):
        raise RecoveryConflictError(
            "factor coverage/targets must exactly match the complete watermark map"
        )
    for key, entry in entry_by_key.items():
        coverage = coverage_by_key[key]
        target = target_by_key[key]
        if (
            coverage.namespace != expected_namespace_by_key[key]
            or target.namespace != expected_namespace_by_key[key]
        ):
            raise RecoveryConflictError(
                "factor input namespace differs from independently expected route "
                "including normalizer identity"
            )
        _validate_factor_namespace(coverage.namespace, entry, checkpoint, expected)
        _validate_factor_namespace(target.namespace, entry, checkpoint, expected)
        if coverage.namespace != target.namespace:
            raise RecoveryConflictError("factor checkpoint and tail namespace differ")
        tag_key = (
            entry.source_stream_id,
            entry.origin_capture_date,
            entry.origin_stream_day_id,
            entry.family,
            entry.shard_id,
        )
        tag = generation_tags[tag_key]
        if (
            coverage.namespace.origin_source_writer_instance
            != tag.origin_source_writer_instance
            or coverage.namespace.origin_source_generation
            != tag.origin_source_generation
            or coverage.namespace.canonical_generation != tag.canonical_generation
        ):
            raise RecoveryConflictError(
                "SourceNamespace differs from runtime checkpoint generation tag"
            )
        if (
            coverage.exclusive_canonical_cursor != entry.canonical_cursor
            or coverage.max_consumed_origin_wal_end_pos
            != entry.max_consumed_origin_wal_end_pos
        ):
            raise RecoveryConflictError("factor coverage differs from saved watermark")
        if (
            target.health is not SourceHealth.HEALTHY
            or target.clock_compatible is not True
            or target.end_exclusive_canonical_cursor < entry.canonical_cursor
            or target.end_max_consumed_origin_wal_end_pos
            < entry.max_consumed_origin_wal_end_pos
        ):
            raise RecoveryConflictError("factor input target is FATAL/incompatible/regressive")

    spans = tuple(sorted(request.replay_spans, key=lambda item: item.sort_key))
    grouped: dict[tuple[object, ...], list[FactorReplaySpan]] = {}
    for span in spans:
        if span.route_key not in entry_by_key:
            raise RecoveryConflictError("factor replay span names an extra input")
        grouped.setdefault(span.route_key, []).append(span)
    if set(grouped) != set(entry_by_key):
        raise RecoveryConflictError("factor replay is missing a watermark input")
    for key, entry in entry_by_key.items():
        coverage = coverage_by_key[key]
        target = target_by_key[key]
        expected_cursor = entry.canonical_cursor
        expected_wal = entry.max_consumed_origin_wal_end_pos
        for span in sorted(
            grouped[key], key=lambda item: item.begin_exclusive_canonical_cursor
        ):
            if span.namespace != coverage.namespace:
                raise RecoveryConflictError("factor replay namespace changed")
            if (
                span.health is not SourceHealth.HEALTHY
                or span.clock_compatible is not True
                or span.exact_records_verified is not True
            ):
                raise RecoveryConflictError("factor replay span is FATAL/incompatible/inexact")
            # canonical_cursor is exclusive-next.  Resume begins at cursor,
            # never at cursor+1 and never at a selected latest position.
            if span.begin_exclusive_canonical_cursor != expected_cursor:
                raise RecoveryConflictError(
                    "factor replay cursor overlaps, gaps, or skips the saved cursor"
                )
            if span.prior_max_consumed_origin_wal_end_pos != expected_wal:
                raise RecoveryConflictError("factor replay WAL coverage has a gap")
            expected_cursor = span.end_exclusive_canonical_cursor
            expected_wal = span.end_max_consumed_origin_wal_end_pos
        if (
            expected_cursor != target.end_exclusive_canonical_cursor
            or expected_wal != target.end_max_consumed_origin_wal_end_pos
        ):
            raise RecoveryConflictError("factor replay does not reach exact target")

    mux = request.native_safe_mux
    expected_mux_ranges = tuple(
        sorted(
            (NativeMuxInputRange.from_span(item) for item in spans),
            key=lambda item: item.sort_key,
        )
    )
    if (
        mux.native_binding_sha256
        != expected.expected_native_mux_binding_sha256
        or
        mux.input_ranges != expected_mux_ranges
        or mux.decision is not NativeSafeMuxDecision.READY
        or mux.live_frontiers_revalidated is not True
        or mux.all_required_inputs_present is not True
        or mux.clock_compatible is not True
        or mux.fatal_observed is not False
    ):
        raise RecoveryConflictError("native safe-mux barrier is incomplete or not READY")

    return FactorRecoveryPlan(
        identity=expected,
        checkpoint_sha256=request.checkpoint_sha256,
        watermark_set=checkpoint.watermark_set,
        checkpoint_input_coverage=coverages,
        input_targets=targets,
        replay_spans=spans,
        native_safe_mux=mux,
        steps=(
            "validate_factor_checkpoint_code_config_state_schema_and_registry",
            "resolve_complete_factor_input_watermark_set",
            "resume_every_input_at_its_exact_exclusive_canonical_cursor",
            "replay_every_batch_through_native_safe_mux_live_frontier_barrier",
            "wait_exact_raw_durability_barriers_before_durable_checkpoint",
            "hold_outputs_recovering_and_emit_plan_only_without_latest_skip",
        ),
        _construction_token=_PLAN_TOKEN,
    )


@dataclass(frozen=True, slots=True, order=True)
class SchemaBinding:
    component: str
    schema_sha256: bytes
    dtype_sha256: bytes | None

    def __post_init__(self) -> None:
        _token(self.component, "component")
        _fixed_bytes(self.schema_sha256, 32, "schema_sha256")
        if self.dtype_sha256 is not None:
            _fixed_bytes(self.dtype_sha256, 32, "dtype_sha256")

    def canonical_object(self) -> dict[str, object]:
        return {
            "component": self.component,
            "dtype_sha256": (
                None if self.dtype_sha256 is None else self.dtype_sha256.hex()
            ),
            "schema_sha256": self.schema_sha256.hex(),
        }


def _safe_relative_path(value: object) -> str:
    if type(value) is not str or not value or len(value) > 4096:
        raise RecoveryError("artifact path must be a bounded relative path")
    if value.startswith("/") or "\\" in value or "\x00" in value or "//" in value:
        raise RecoveryError("artifact path is unsafe")
    path = PurePosixPath(value)
    if str(path) != value or any(
        item in ("", ".", "..")
        or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._=+-]*", item) is None
        for item in path.parts
    ):
        raise RecoveryError("artifact path is not canonical/safe")
    return value


@dataclass(frozen=True, slots=True, order=True)
class ArtifactHashBinding:
    relative_path: str
    artifact_type: str
    file_size_bytes: int
    file_sha256: bytes
    logical_content_sha256: bytes

    def __post_init__(self) -> None:
        _safe_relative_path(self.relative_path)
        _token(self.artifact_type, "artifact_type")
        _uint(self.file_size_bytes, 64, "file_size_bytes", positive=True)
        _fixed_bytes(self.file_sha256, 32, "file_sha256")
        _fixed_bytes(
            self.logical_content_sha256,
            32,
            "logical_content_sha256",
        )

    def canonical_object(self) -> dict[str, object]:
        return {
            "artifact_type": self.artifact_type,
            "file_sha256": self.file_sha256.hex(),
            "file_size_bytes": self.file_size_bytes,
            "logical_content_sha256": self.logical_content_sha256.hex(),
            "relative_path": self.relative_path,
        }


@dataclass(frozen=True, slots=True)
class IsolatedRebuildRequest:
    canonical_plan: CanonicalColdStartPlan
    previous_generation: int
    candidate_generation: int
    candidate_identity_sha256: bytes
    schemas: tuple[SchemaBinding, ...]
    registry_version: int
    registry_sha256: bytes
    artifacts: tuple[ArtifactHashBinding, ...]
    dependent_recovery_plan_sha256s: tuple[bytes, ...]
    common_cut_identity_sha256: bytes
    expected_candidate_content_sha256: bytes
    candidate_isolated: bool
    artifacts_validated: bool
    common_cut_validated: bool
    alias_switch_authorized: bool = False

    def __post_init__(self) -> None:
        if type(self.canonical_plan) is not CanonicalColdStartPlan:
            raise RecoveryError("canonical_plan must be a built CanonicalColdStartPlan")
        _uint(self.previous_generation, 64, "previous_generation", positive=True)
        _uint(self.candidate_generation, 64, "candidate_generation", positive=True)
        if self.candidate_generation <= self.previous_generation:
            raise RecoveryError("isolated candidate generation must be newer")
        _fixed_bytes(
            self.candidate_identity_sha256,
            32,
            "candidate_identity_sha256",
        )
        _typed_tuple(self.schemas, SchemaBinding, "schemas")
        _uint(self.registry_version, 64, "registry_version", positive=True)
        _fixed_bytes(self.registry_sha256, 32, "registry_sha256")
        _typed_tuple(self.artifacts, ArtifactHashBinding, "artifacts")
        plans = _typed_tuple(
            self.dependent_recovery_plan_sha256s,
            bytes,
            "dependent_recovery_plan_sha256s",
            nonempty=False,
        )
        for value in plans:
            _fixed_bytes(value, 32, "dependent recovery plan SHA-256")
        _fixed_bytes(
            self.common_cut_identity_sha256,
            32,
            "common_cut_identity_sha256",
        )
        _fixed_bytes(
            self.expected_candidate_content_sha256,
            32,
            "expected_candidate_content_sha256",
        )
        for name in (
            "candidate_isolated",
            "artifacts_validated",
            "common_cut_validated",
            "alias_switch_authorized",
        ):
            _exact_bool(getattr(self, name), name)


@dataclass(frozen=True, slots=True)
class RebuildCutoverCertificate:
    previous_generation: int
    candidate_generation: int
    candidate_identity_sha256: bytes
    canonical_plan_sha256: bytes
    dependent_recovery_plan_sha256s: tuple[bytes, ...]
    schemas: tuple[SchemaBinding, ...]
    registry_version: int
    registry_sha256: bytes
    raw_routes: tuple[RawRouteIdentity, ...]
    raw_ranges: tuple[RawReplaySpan, ...]
    artifacts: tuple[ArtifactHashBinding, ...]
    common_cut_identity_sha256: bytes
    expected_candidate_content_sha256: bytes
    _construction_token: InitVar[object] = None
    certificate_sha256: bytes = field(init=False)
    integrity_kind: str = field(init=False, default="sha256_not_a_signature")
    caller_attested_evidence: bool = field(init=False, default=True)
    executes_cutover: bool = field(init=False, default=False)
    authorizes_alias_switch: bool = field(init=False, default=False)

    def __post_init__(self, _construction_token: object) -> None:
        if _construction_token is not _PLAN_TOKEN:
            raise RecoveryError("use build_isolated_rebuild_plan")
        object.__setattr__(
            self,
            "certificate_sha256",
            _object_sha256(
                b"l2flow.recovery.rebuild-cutover-certificate.v1\x00",
                self._identity_object(),
            ),
        )

    def _identity_object(self) -> dict[str, object]:
        return {
            "artifacts": [item.canonical_object() for item in self.artifacts],
            "authorizes_alias_switch": False,
            "caller_attested_evidence": True,
            "candidate_generation": self.candidate_generation,
            "candidate_identity_sha256": self.candidate_identity_sha256.hex(),
            "canonical_plan_sha256": self.canonical_plan_sha256.hex(),
            "common_cut_identity_sha256": self.common_cut_identity_sha256.hex(),
            "dependent_recovery_plan_sha256s": [
                item.hex() for item in self.dependent_recovery_plan_sha256s
            ],
            "encoding": "l2flow-rebuild-cutover-certificate-v1",
            "executes_cutover": False,
            "expected_candidate_content_sha256": (
                self.expected_candidate_content_sha256.hex()
            ),
            "integrity_kind": "sha256_not_a_signature",
            "previous_generation": self.previous_generation,
            "raw_ranges": [item.canonical_object() for item in self.raw_ranges],
            "raw_routes": [item.canonical_object() for item in self.raw_routes],
            "registry_sha256": self.registry_sha256.hex(),
            "registry_version": self.registry_version,
            "schemas": [item.canonical_object() for item in self.schemas],
        }

    def canonical_object(self) -> dict[str, object]:
        value = self._identity_object()
        value["certificate_sha256"] = self.certificate_sha256.hex()
        return value


@dataclass(frozen=True, slots=True)
class IsolatedRebuildPlan:
    certificate: RebuildCutoverCertificate
    steps: tuple[str, ...]
    _construction_token: InitVar[object] = None
    plan_sha256: bytes = field(init=False)
    executes_rebuild: bool = field(init=False, default=False)
    authorizes_alias_switch: bool = field(init=False, default=False)

    def __post_init__(self, _construction_token: object) -> None:
        if _construction_token is not _PLAN_TOKEN:
            raise RecoveryError("use build_isolated_rebuild_plan")
        object.__setattr__(
            self,
            "plan_sha256",
            _object_sha256(
                b"l2flow.recovery.isolated-rebuild-plan.v1\x00",
                {
                    "authorizes_alias_switch": False,
                    "certificate_sha256": self.certificate.certificate_sha256.hex(),
                    "encoding": "l2flow-isolated-rebuild-plan-v1",
                    "executes_rebuild": False,
                    "steps": list(self.steps),
                },
            ),
        )


def build_isolated_rebuild_plan(request: IsolatedRebuildRequest) -> IsolatedRebuildPlan:
    if type(request) is not IsolatedRebuildRequest:
        raise RecoveryError("request must be exact IsolatedRebuildRequest")
    canonical = request.canonical_plan
    identity = canonical.identity
    if (
        request.previous_generation != identity.previous_canonical_generation
        or request.candidate_generation != identity.candidate_canonical_generation
    ):
        raise RecoveryConflictError("rebuild generations differ from Canonical plan")
    if (
        request.candidate_isolated is not True
        or request.artifacts_validated is not True
        or request.common_cut_validated is not True
        or request.alias_switch_authorized is not False
    ):
        raise RecoveryConflictError(
            "rebuild certificate requires isolated validated evidence and no alias authority"
        )
    schemas = tuple(sorted(request.schemas, key=lambda item: item.component))
    schema_names = [item.component for item in schemas]
    if len(schema_names) != len(set(schema_names)):
        raise RecoveryConflictError("schema component is duplicated")
    canonical_schema = next(
        (item for item in schemas if item.component == "canonical"),
        None,
    )
    if (
        canonical_schema is None
        or canonical_schema.schema_sha256 != identity.canonical_schema_sha256
        or canonical_schema.dtype_sha256 != identity.canonical_dtype_sha256
        or request.registry_version != identity.registry_version
        or request.registry_sha256 != identity.registry_sha256
    ):
        raise RecoveryConflictError("rebuild schema/registry differs from Canonical plan")
    artifacts = tuple(sorted(request.artifacts, key=lambda item: item.relative_path))
    paths = [item.relative_path for item in artifacts]
    if len(paths) != len(set(paths)):
        raise RecoveryConflictError("rebuild artifact path is duplicated")
    dependent = tuple(sorted(request.dependent_recovery_plan_sha256s))
    if len(dependent) != len(set(dependent)):
        raise RecoveryConflictError("dependent recovery plan hash is duplicated")

    _validate_rebuild_certificate_components(
        previous_generation=request.previous_generation,
        candidate_generation=request.candidate_generation,
        schemas=schemas,
        registry_version=request.registry_version,
        raw_routes=canonical.raw_routes,
        raw_ranges=canonical.replay_spans,
        artifacts=artifacts,
        dependent_recovery_plan_sha256s=dependent,
    )

    certificate = RebuildCutoverCertificate(
        previous_generation=request.previous_generation,
        candidate_generation=request.candidate_generation,
        candidate_identity_sha256=request.candidate_identity_sha256,
        canonical_plan_sha256=canonical.plan_sha256,
        dependent_recovery_plan_sha256s=dependent,
        schemas=schemas,
        registry_version=request.registry_version,
        registry_sha256=request.registry_sha256,
        raw_routes=canonical.raw_routes,
        raw_ranges=canonical.replay_spans,
        artifacts=artifacts,
        common_cut_identity_sha256=request.common_cut_identity_sha256,
        expected_candidate_content_sha256=(
            request.expected_candidate_content_sha256
        ),
        _construction_token=_PLAN_TOKEN,
    )
    return IsolatedRebuildPlan(
        certificate=certificate,
        steps=(
            "validate_isolated_candidate_generation_and_all_plan_hashes",
            "validate_schema_registry_raw_route_range_and_artifact_evidence",
            "validate_caller_attested_common_cut_identity",
            "emit_sha256_integrity_certificate_not_signature",
            "require_separate_reviewed_alias_switch_authorization",
        ),
        _construction_token=_PLAN_TOKEN,
    )


def _validate_rebuild_certificate_components(
    *,
    previous_generation: int,
    candidate_generation: int,
    schemas: tuple[SchemaBinding, ...],
    registry_version: int,
    raw_routes: tuple[RawRouteIdentity, ...],
    raw_ranges: tuple[RawReplaySpan, ...],
    artifacts: tuple[ArtifactHashBinding, ...],
    dependent_recovery_plan_sha256s: tuple[bytes, ...],
) -> None:
    _uint(previous_generation, 64, "previous_generation", positive=True)
    _uint(candidate_generation, 64, "candidate_generation", positive=True)
    if candidate_generation <= previous_generation:
        raise RecoveryConflictError("certificate candidate generation is not newer")
    _uint(registry_version, 64, "registry_version", positive=True)

    _typed_tuple(schemas, SchemaBinding, "certificate schemas")
    if schemas != tuple(sorted(schemas, key=lambda item: item.component)):
        raise RecoveryConflictError("certificate schemas are not canonically ordered")
    schema_names = [item.component for item in schemas]
    if len(schema_names) != len(set(schema_names)) or "canonical" not in schema_names:
        raise RecoveryConflictError(
            "certificate schemas are duplicate or omit canonical"
        )
    canonical_schema = next(
        item for item in schemas if item.component == "canonical"
    )
    if canonical_schema.dtype_sha256 is None:
        raise RecoveryConflictError(
            "certificate canonical schema must bind its exact dtype"
        )

    _typed_tuple(raw_routes, RawRouteIdentity, "certificate Raw routes")
    if raw_routes != tuple(sorted(raw_routes, key=lambda item: item.route_key)):
        raise RecoveryConflictError("certificate Raw routes are not canonically ordered")
    route_keys = [item.route_key for item in raw_routes]
    if len(route_keys) != len(set(route_keys)):
        raise RecoveryConflictError("certificate Raw route is duplicated")
    first_route = raw_routes[0]
    for route in raw_routes:
        if route.health is not SourceHealth.HEALTHY:
            raise RecoveryConflictError("certificate Raw route is not healthy")
        if (
            route.trade_date != first_route.trade_date
            or route.clock_epoch_algorithm != first_route.clock_epoch_algorithm
            or route.clock_epoch_digest != first_route.clock_epoch_digest
        ):
            raise RecoveryConflictError(
                "certificate Raw routes disagree on trade date or clock identity"
            )

    _typed_tuple(raw_ranges, RawReplaySpan, "certificate Raw ranges")
    if raw_ranges != tuple(sorted(raw_ranges, key=lambda item: item.sort_key)):
        raise RecoveryConflictError("certificate Raw ranges are not canonically ordered")
    range_keys = [
        (item.route.route_key, item.stage, item.begin_wal_pos, item.end_wal_pos)
        for item in raw_ranges
    ]
    if len(range_keys) != len(set(range_keys)):
        raise RecoveryConflictError("certificate Raw range is duplicated")
    route_set = set(raw_routes)
    grouped_ranges: dict[
        tuple[RawRouteIdentity, RawReplayStage], list[RawReplaySpan]
    ] = {}
    for span in raw_ranges:
        if span.route not in route_set:
            raise RecoveryConflictError("certificate Raw range names an extra route")
        grouped_ranges.setdefault((span.route, span.stage), []).append(span)
    for route in raw_routes:
        api_sys = _validate_raw_stage_coverage(
            route,
            RawReplayStage.API_SYS,
            tuple(grouped_ranges.get((route, RawReplayStage.API_SYS), ())),
        )
        market = _validate_raw_stage_coverage(
            route,
            RawReplayStage.MARKET,
            tuple(grouped_ranges.get((route, RawReplayStage.MARKET), ())),
        )
        if _raw_full_scan_receipts(api_sys) != _raw_full_scan_receipts(market):
            raise RecoveryConflictError(
                "certificate Raw passes do not bind identical ordered bytes"
            )

    _typed_tuple(artifacts, ArtifactHashBinding, "certificate artifacts")
    if artifacts != tuple(sorted(artifacts, key=lambda item: item.relative_path)):
        raise RecoveryConflictError("certificate artifacts are not canonically ordered")
    paths = [item.relative_path for item in artifacts]
    if len(paths) != len(set(paths)):
        raise RecoveryConflictError("certificate artifact path is duplicated")

    plans = _typed_tuple(
        dependent_recovery_plan_sha256s,
        bytes,
        "certificate dependent recovery plans",
        nonempty=False,
    )
    for value in plans:
        _fixed_bytes(value, 32, "dependent recovery plan SHA-256")
    if plans != tuple(sorted(plans)) or len(plans) != len(set(plans)):
        raise RecoveryConflictError(
            "certificate dependent plans are duplicate or not canonically ordered"
        )


def _certificate_json_object(
    value: object,
    keys: set[str],
    name: str,
) -> dict[str, Any]:
    if type(value) is not dict or set(value) != keys:
        raise RecoveryIntegrityError(f"{name} fields are unknown or missing")
    return value


def _certificate_json_array(
    value: object,
    name: str,
    *,
    nonempty: bool = True,
) -> list[Any]:
    if (
        type(value) is not list
        or (nonempty and not value)
        or len(value) > MAX_RECOVERY_ITEMS_V1
    ):
        qualifier = "nonempty " if nonempty else ""
        raise RecoveryIntegrityError(
            f"{name} must be a bounded {qualifier}JSON array"
        )
    return value


def _certificate_json_hex(value: object, width: int, name: str) -> bytes:
    if (
        type(value) is not str
        or len(value) != width * 2
        or re.fullmatch(r"[0-9a-f]+", value) is None
    ):
        raise RecoveryIntegrityError(
            f"{name} must be exact lowercase {width}-byte hex"
        )
    result = bytes.fromhex(value)
    if not any(result):
        raise RecoveryIntegrityError(f"{name} must be nonzero")
    return result


def _decode_certificate_raw_route(value: object) -> RawRouteIdentity:
    item = _certificate_json_object(
        value,
        {
            "clock_epoch_algorithm",
            "clock_epoch_digest",
            "configured_feed_role",
            "current_raw_durable_wal_pos",
            "day_begin_ingress_sequence",
            "day_begin_wal_pos",
            "health",
            "market",
            "origin_capture_date",
            "origin_source_generation",
            "origin_source_writer_instance",
            "origin_stream_day_id",
            "replay_end_ingress_sequence",
            "replay_end_wal_pos",
            "source_stream_id",
            "trade_date",
        },
        "certificate Raw route",
    )
    if type(item["health"]) is not str:
        raise RecoveryIntegrityError("certificate Raw route health is not a string")
    try:
        health = SourceHealth(item["health"])
    except ValueError as error:
        raise RecoveryIntegrityError("certificate Raw route health is unknown") from error
    return RawRouteIdentity(
        trade_date=item["trade_date"],
        source_stream_id=item["source_stream_id"],
        origin_capture_date=item["origin_capture_date"],
        origin_stream_day_id=_certificate_json_hex(
            item["origin_stream_day_id"], 16, "origin_stream_day_id"
        ),
        origin_source_writer_instance=_certificate_json_hex(
            item["origin_source_writer_instance"],
            16,
            "origin_source_writer_instance",
        ),
        origin_source_generation=item["origin_source_generation"],
        clock_epoch_algorithm=item["clock_epoch_algorithm"],
        clock_epoch_digest=_certificate_json_hex(
            item["clock_epoch_digest"], 32, "clock_epoch_digest"
        ),
        day_begin_ingress_sequence=item["day_begin_ingress_sequence"],
        day_begin_wal_pos=item["day_begin_wal_pos"],
        replay_end_ingress_sequence=item["replay_end_ingress_sequence"],
        replay_end_wal_pos=item["replay_end_wal_pos"],
        current_raw_durable_wal_pos=item["current_raw_durable_wal_pos"],
        health=health,
        market=item["market"],
        configured_feed_role=item["configured_feed_role"],
    )


def _decode_certificate_raw_range(value: object) -> RawReplaySpan:
    item = _certificate_json_object(
        value,
        {
            "begin_wal_pos",
            "end_wal_pos",
            "envelope_verifier_sha256",
            "exact_record_boundaries_verified",
            "first_ingress_sequence",
            "last_ingress_sequence",
            "ordered_record_content_sha256",
            "record_count",
            "route",
            "stage",
        },
        "certificate Raw range",
    )
    if type(item["stage"]) is not str:
        raise RecoveryIntegrityError("certificate Raw range stage is not a string")
    try:
        stage = RawReplayStage(item["stage"])
    except ValueError as error:
        raise RecoveryIntegrityError("certificate Raw range stage is unknown") from error
    return RawReplaySpan(
        route=_decode_certificate_raw_route(item["route"]),
        stage=stage,
        begin_wal_pos=item["begin_wal_pos"],
        end_wal_pos=item["end_wal_pos"],
        first_ingress_sequence=item["first_ingress_sequence"],
        last_ingress_sequence=item["last_ingress_sequence"],
        record_count=item["record_count"],
        ordered_record_content_sha256=_certificate_json_hex(
            item["ordered_record_content_sha256"],
            32,
            "ordered_record_content_sha256",
        ),
        envelope_verifier_sha256=_certificate_json_hex(
            item["envelope_verifier_sha256"],
            32,
            "envelope_verifier_sha256",
        ),
        exact_record_boundaries_verified=item["exact_record_boundaries_verified"],
    )


def _decode_certificate_schema(value: object) -> SchemaBinding:
    item = _certificate_json_object(
        value,
        {"component", "dtype_sha256", "schema_sha256"},
        "certificate schema",
    )
    dtype = item["dtype_sha256"]
    return SchemaBinding(
        component=item["component"],
        schema_sha256=_certificate_json_hex(
            item["schema_sha256"], 32, "schema_sha256"
        ),
        dtype_sha256=(
            None
            if dtype is None
            else _certificate_json_hex(dtype, 32, "dtype_sha256")
        ),
    )


def _decode_certificate_artifact(value: object) -> ArtifactHashBinding:
    item = _certificate_json_object(
        value,
        {
            "artifact_type",
            "file_sha256",
            "file_size_bytes",
            "logical_content_sha256",
            "relative_path",
        },
        "certificate artifact",
    )
    return ArtifactHashBinding(
        relative_path=item["relative_path"],
        artifact_type=item["artifact_type"],
        file_size_bytes=item["file_size_bytes"],
        file_sha256=_certificate_json_hex(
            item["file_sha256"], 32, "file_sha256"
        ),
        logical_content_sha256=_certificate_json_hex(
            item["logical_content_sha256"],
            32,
            "logical_content_sha256",
        ),
    )


def encode_rebuild_cutover_certificate(
    certificate: RebuildCutoverCertificate,
) -> bytes:
    if type(certificate) is not RebuildCutoverCertificate:
        raise RecoveryError("certificate must be RebuildCutoverCertificate")
    payload = _canonical_json(certificate.canonical_object())
    if len(payload) > MAX_CERTIFICATE_JSON_BYTES_V1:
        raise RecoveryError("rebuild certificate exceeds its JSON bound")
    prefix = _CERTIFICATE_MAGIC + struct.pack("<Q", len(payload))
    return prefix + payload + hashlib.sha256(prefix + payload).digest()


def validate_rebuild_cutover_certificate(
    blob: bytes,
    *,
    expected_certificate_sha256: bytes,
) -> bytes:
    """Validate canonical transport bytes against an externally fixed digest.

    A caller who can replace both the blob and expected digest can replace this
    evidence.  This function verifies integrity/canonical form, not a signature
    or authority to switch any deployed alias.
    """

    if type(blob) is not bytes or len(blob) < 48:
        raise RecoveryIntegrityError("rebuild certificate is truncated")
    expected = _fixed_bytes(
        expected_certificate_sha256,
        32,
        "expected_certificate_sha256",
    )
    if blob[:8] != _CERTIFICATE_MAGIC:
        raise RecoveryIntegrityError("rebuild certificate magic/version mismatch")
    (length,) = struct.unpack_from("<Q", blob, 8)
    if (
        length > MAX_CERTIFICATE_JSON_BYTES_V1
        or len(blob) != 16 + length + 32
    ):
        raise RecoveryIntegrityError("rebuild certificate length is invalid")
    signed = blob[: 16 + length]
    if hashlib.sha256(signed).digest() != blob[-32:]:
        raise RecoveryIntegrityError("rebuild certificate envelope SHA-256 mismatch")
    payload = blob[16 : 16 + length]

    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise RecoveryIntegrityError("certificate JSON has a duplicate key")
            result[key] = value
        return result

    try:
        root = json.loads(payload.decode("ascii"), object_pairs_hook=unique_object)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RecoveryIntegrityError("certificate JSON is invalid") from error
    expected_keys = {
        "artifacts",
        "authorizes_alias_switch",
        "caller_attested_evidence",
        "candidate_generation",
        "candidate_identity_sha256",
        "canonical_plan_sha256",
        "certificate_sha256",
        "common_cut_identity_sha256",
        "dependent_recovery_plan_sha256s",
        "encoding",
        "executes_cutover",
        "expected_candidate_content_sha256",
        "integrity_kind",
        "previous_generation",
        "raw_ranges",
        "raw_routes",
        "registry_sha256",
        "registry_version",
        "schemas",
    }
    if type(root) is not dict or set(root) != expected_keys:
        raise RecoveryIntegrityError("certificate fields are unknown or missing")
    if _canonical_json(root) != payload:
        raise RecoveryIntegrityError("certificate JSON is not canonical")
    claimed = _certificate_json_hex(
        root["certificate_sha256"], 32, "certificate_sha256"
    )
    if (
        claimed != expected
        or root["encoding"] != "l2flow-rebuild-cutover-certificate-v1"
        or root["integrity_kind"] != "sha256_not_a_signature"
        or root["caller_attested_evidence"] is not True
        or root["executes_cutover"] is not False
        or root["authorizes_alias_switch"] is not False
    ):
        raise RecoveryIntegrityError("certificate identity/authority fields disagree")

    try:
        previous_generation = root["previous_generation"]
        candidate_generation = root["candidate_generation"]
        candidate_identity = _certificate_json_hex(
            root["candidate_identity_sha256"],
            32,
            "candidate_identity_sha256",
        )
        canonical_plan = _certificate_json_hex(
            root["canonical_plan_sha256"], 32, "canonical_plan_sha256"
        )
        common_cut = _certificate_json_hex(
            root["common_cut_identity_sha256"],
            32,
            "common_cut_identity_sha256",
        )
        expected_content = _certificate_json_hex(
            root["expected_candidate_content_sha256"],
            32,
            "expected_candidate_content_sha256",
        )
        registry_sha256 = _certificate_json_hex(
            root["registry_sha256"], 32, "registry_sha256"
        )
        registry_version = root["registry_version"]
        dependent = tuple(
            _certificate_json_hex(item, 32, "dependent recovery plan SHA-256")
            for item in _certificate_json_array(
                root["dependent_recovery_plan_sha256s"],
                "dependent_recovery_plan_sha256s",
                nonempty=False,
            )
        )
        schemas = tuple(
            _decode_certificate_schema(item)
            for item in _certificate_json_array(root["schemas"], "schemas")
        )
        raw_routes = tuple(
            _decode_certificate_raw_route(item)
            for item in _certificate_json_array(root["raw_routes"], "raw_routes")
        )
        raw_ranges = tuple(
            _decode_certificate_raw_range(item)
            for item in _certificate_json_array(root["raw_ranges"], "raw_ranges")
        )
        artifacts = tuple(
            _decode_certificate_artifact(item)
            for item in _certificate_json_array(root["artifacts"], "artifacts")
        )
        _validate_rebuild_certificate_components(
            previous_generation=previous_generation,
            candidate_generation=candidate_generation,
            schemas=schemas,
            registry_version=registry_version,
            raw_routes=raw_routes,
            raw_ranges=raw_ranges,
            artifacts=artifacts,
            dependent_recovery_plan_sha256s=dependent,
        )
        certificate = RebuildCutoverCertificate(
            previous_generation=previous_generation,
            candidate_generation=candidate_generation,
            candidate_identity_sha256=candidate_identity,
            canonical_plan_sha256=canonical_plan,
            dependent_recovery_plan_sha256s=dependent,
            schemas=schemas,
            registry_version=registry_version,
            registry_sha256=registry_sha256,
            raw_routes=raw_routes,
            raw_ranges=raw_ranges,
            artifacts=artifacts,
            common_cut_identity_sha256=common_cut,
            expected_candidate_content_sha256=expected_content,
            _construction_token=_PLAN_TOKEN,
        )
    except RecoveryIntegrityError:
        raise
    except RecoveryError as error:
        raise RecoveryIntegrityError(
            "certificate nested typed evidence is invalid"
        ) from error
    if (
        certificate.certificate_sha256 != expected
        or certificate.canonical_object() != root
    ):
        raise RecoveryIntegrityError("certificate canonical identity mismatch")
    return certificate.certificate_sha256


__all__ = [
    "ArtifactHashBinding",
    "CanonicalColdStartPlan",
    "CanonicalColdStartRequest",
    "CanonicalGenerationIdentity",
    "CanonicalSinkSpec",
    "FactorCheckpointInputCoverage",
    "FactorInputTarget",
    "FactorRecoveryIdentity",
    "FactorRecoveryPlan",
    "FactorRecoveryRequest",
    "FactorReplaySpan",
    "IsolatedRebuildPlan",
    "IsolatedRebuildRequest",
    "LatestStateCheckpointCursor",
    "LatestStateCheckpointEvidence",
    "LatestStateCheckpointExpectation",
    "LatestStateConfigIdentity",
    "LatestStateDurabilityBarrier",
    "LatestStateFullReplayReset",
    "LatestStateInputFamily",
    "LatestStateInputTarget",
    "LatestStateRecoveryPlan",
    "LatestStateRecoveryRequest",
    "LatestStateReplaySpan",
    "LatestStateWriterReuseLease",
    "NativeMuxInputRange",
    "NativeSafeMuxDecision",
    "NativeSafeMuxEvidence",
    "RawReplaySpan",
    "RawReplayStage",
    "RawRouteIdentity",
    "RebuildCutoverCertificate",
    "RecoveryConflictError",
    "RecoveryError",
    "RecoveryIntegrityError",
    "RecoveryMode",
    "SchemaBinding",
    "SourceHealth",
    "build_canonical_cold_start_plan",
    "build_factor_recovery_plan",
    "build_isolated_rebuild_plan",
    "build_latest_state_recovery_plan",
    "configured_raw_routes_sha256",
    "encode_rebuild_cutover_certificate",
    "factor_input_routes_sha256",
    "latest_input_routes_sha256",
    "native_safe_mux_binding_sha256",
    "validate_rebuild_cutover_certificate",
]
