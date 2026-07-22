from __future__ import annotations

import base64
from dataclasses import dataclass
import hashlib
import json
import re
from typing import Any, Callable, Protocol, Sequence

import numpy as np

from .canonical import BatchMetadata, InputFamily, MdlBatchView
from .errors import (
    CheckpointError,
    CursorError,
    OutputConflictError,
    PluginExecutionError,
    ShardOwnershipError,
    ValidationError,
)
from .spec import FactorSpec
from .watermark import FactorInputWatermark, FactorInputWatermarkSet


LOGICAL_FACTOR_SHARDS = 16
_RUNTIME_STATE_DOMAIN = b"l2flow.factor.runtime-state.v1\x00"
_MAX_RUNTIME_STATE_BYTES = 256 * 1024 * 1024
_MAX_PLUGIN_STATE_BYTES = 128 * 1024 * 1024
_MAX_CURSOR_ENTRIES = 65_536
RUNTIME_STATE_CODEC_V1 = "l2flow-factor-runtime-state-v1"


def _json_uint(value: Any, bits: int, name: str, *, positive: bool = False) -> int:
    minimum = 1 if positive else 0
    if type(value) is not int or value < minimum or value > (1 << bits) - 1:
        raise CheckpointError(f"{name} must be an exact uint{bits}")
    return value


def _json_hex(value: Any, width: int, name: str, *, nonzero: bool = True) -> bytes:
    if (
        not isinstance(value, str)
        or len(value) != width * 2
        or value.lower() != value
    ):
        raise CheckpointError(f"{name} must be lowercase fixed-width hex")
    try:
        result = bytes.fromhex(value)
    except ValueError as error:
        raise CheckpointError(f"{name} must be lowercase fixed-width hex") from error
    if len(result) != width or (nonzero and not any(result)):
        raise CheckpointError(f"{name} has an invalid byte identity")
    return result


def _json_factor_id(value: Any) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[a-z][a-z0-9_]{0,127}", value):
        raise CheckpointError("factor_id is invalid")
    return value


def _json_factor_version(value: Any) -> str:
    if not isinstance(value, str) or not re.fullmatch(
        r"[0-9A-Za-z][0-9A-Za-z._+-]{0,63}", value
    ):
        raise CheckpointError("factor_version is invalid")
    return value


class TransactionalFactorPlugin(Protocol):
    spec: FactorSpec

    def on_batch(self, batch: MdlBatchView) -> Sequence[Any]:
        ...

    def serialize_state(self) -> bytes:
        ...

    def restore_state(self, state: bytes) -> None:
        ...


class CommitRevalidator(Protocol):
    def __call__(self, batch: MdlBatchView) -> bool:
        ...


@dataclass(frozen=True, slots=True)
class RunIdentity:
    trade_date: int
    clock_epoch_algorithm: int
    clock_epoch_digest: bytes
    registry_version: int
    registry_sha256: bytes

    def __post_init__(self) -> None:
        for name, value, bits in (
            ("trade_date", self.trade_date, 32),
            ("clock_epoch_algorithm", self.clock_epoch_algorithm, 32),
            ("registry_version", self.registry_version, 64),
        ):
            if type(value) is not int or value <= 0 or value > (1 << bits) - 1:
                raise ValidationError(f"{name} must be a positive uint{bits}")
        for name, value, width in (
            ("clock_epoch_digest", self.clock_epoch_digest, 32),
            ("registry_sha256", self.registry_sha256, 32),
        ):
            if not isinstance(value, bytes) or len(value) != width or not any(value):
                raise ValidationError(f"{name} must be exactly {width} nonzero bytes")

    @classmethod
    def from_metadata(cls, metadata: BatchMetadata) -> "RunIdentity":
        return cls(
            trade_date=metadata.trade_date,
            clock_epoch_algorithm=metadata.clock_epoch_algorithm,
            clock_epoch_digest=metadata.clock_epoch_digest,
            registry_version=metadata.registry_version,
            registry_sha256=metadata.registry_sha256,
        )

    def canonical_object(self) -> dict[str, Any]:
        return {
            "clock_epoch_algorithm": self.clock_epoch_algorithm,
            "clock_epoch_digest": self.clock_epoch_digest.hex(),
            "registry_sha256": self.registry_sha256.hex(),
            "registry_version": self.registry_version,
            "trade_date": self.trade_date,
        }


@dataclass(frozen=True, slots=True)
class GenerationTag:
    """Generation identity scoped to one exact Canonical input."""

    origin_source_writer_instance: bytes
    origin_source_generation: int
    canonical_generation: int

    def __post_init__(self) -> None:
        for name, value in (
            ("origin_source_generation", self.origin_source_generation),
            ("canonical_generation", self.canonical_generation),
        ):
            if type(value) is not int or value <= 0 or value > (1 << 64) - 1:
                raise ValidationError(f"{name} must be a positive uint64")
        if (
            not isinstance(self.origin_source_writer_instance, bytes)
            or len(self.origin_source_writer_instance) != 16
            or not any(self.origin_source_writer_instance)
        ):
            raise ValidationError(
                "origin_source_writer_instance must be exactly 16 nonzero bytes"
            )

    @classmethod
    def from_metadata(cls, metadata: BatchMetadata) -> "GenerationTag":
        return cls(
            origin_source_writer_instance=metadata.origin_source_writer_instance,
            origin_source_generation=metadata.origin_source_generation,
            canonical_generation=metadata.canonical_generation,
        )

    def canonical_object(self) -> dict[str, Any]:
        return {
            "canonical_generation": self.canonical_generation,
            "origin_source_generation": self.origin_source_generation,
            "origin_source_writer_instance": self.origin_source_writer_instance.hex(),
        }


@dataclass(frozen=True, slots=True)
class DecodedRuntimeCheckpoint:
    factor_id: str
    factor_version: str
    factor_spec_sha256: bytes
    shard_id: int
    run_identity: RunIdentity
    watermark_set: FactorInputWatermarkSet
    generation_tags: tuple[
        tuple[tuple[int, int, bytes, InputFamily, int], GenerationTag], ...
    ]
    plugin_state: bytes


@dataclass(frozen=True, slots=True)
class RuntimeOutput:
    idempotency_key: tuple[str, ...]
    content: bytes

    def __post_init__(self) -> None:
        if type(self.idempotency_key) is not tuple or not self.idempotency_key:
            raise ValidationError("idempotency_key must be a non-empty tuple")
        if any(not isinstance(item, str) or not item for item in self.idempotency_key):
            raise ValidationError("idempotency_key components must be non-empty strings")
        if not isinstance(self.content, bytes):
            raise ValidationError("RuntimeOutput content must be bytes")

    @property
    def content_digest(self) -> bytes:
        return hashlib.sha256(self.content).digest()


class InstrumentShardRouter:
    shard_count = LOGICAL_FACTOR_SHARDS

    @staticmethod
    def owner(instrument_id: int) -> int:
        if type(instrument_id) is not int or instrument_id <= 0 or instrument_id > (1 << 32) - 1:
            raise ValidationError("instrument_id must be a positive uint32")
        return instrument_id % LOGICAL_FACTOR_SHARDS

    @staticmethod
    def assert_owner(instrument_ids: np.ndarray, shard_id: int) -> None:
        if type(shard_id) is not int or not 0 <= shard_id < LOGICAL_FACTOR_SHARDS:
            raise ValidationError("shard_id must be in [0, 15]")
        if instrument_ids.size == 0:
            return
        values = instrument_ids.astype(np.uint64, copy=False)
        if bool(np.any(values == 0)) or bool(
            np.any(values % LOGICAL_FACTOR_SHARDS != shard_id)
        ):
            raise ShardOwnershipError(
                "batch contains an instrument owned by another logical shard"
            )


def _output_identity(output: Any) -> tuple[tuple[str, ...], bytes]:
    key = getattr(output, "idempotency_key", None)
    digest = getattr(output, "content_digest", None)
    if callable(key):
        key = key()
    if callable(digest):
        digest = digest()
    if type(key) is not tuple or not key or any(
        not isinstance(item, str) or not item for item in key
    ):
        raise OutputConflictError("plugin output has no stable tuple idempotency_key")
    if not isinstance(digest, (bytes, bytearray, memoryview)) or len(bytes(digest)) != 32:
        raise OutputConflictError("plugin output has no exact 32-byte content_digest")
    return key, bytes(digest)


class FactorTransactionRuntime:
    """Single-shard transaction owner.

    Failed batches are rollback-safe and may be replayed.  A successful batch
    crosses the native commit frontier exactly once; replaying an already
    committed cursor is rejected.  Stable output keys are for the external
    sink's idempotency protocol and output objects are never retained here.
    """

    def __init__(
        self,
        plugin: TransactionalFactorPlugin,
        shard_id: int,
        commit_revalidator: CommitRevalidator,
    ) -> None:
        if not isinstance(getattr(plugin, "spec", None), FactorSpec):
            raise ValidationError("plugin.spec must be a FactorSpec")
        if type(shard_id) is not int or not 0 <= shard_id < LOGICAL_FACTOR_SHARDS:
            raise ValidationError("shard_id must be in [0, 15]")
        if not callable(commit_revalidator):
            raise ValidationError("commit_revalidator is mandatory and must be callable")
        for method in ("on_batch", "serialize_state", "restore_state"):
            if not callable(getattr(plugin, method, None)):
                raise ValidationError(f"plugin must implement {method}")
        for input_spec in plugin.spec.inputs:
            if input_spec.shard_id != shard_id:
                raise ValidationError(
                    "every FactorSpec input must belong to the runtime's one shard"
                )
        initial_state = plugin.serialize_state()
        if not isinstance(initial_state, bytes):
            raise ValidationError("plugin serialize_state must return bytes")
        self._plugin = plugin
        self._shard_id = shard_id
        self._commit_revalidator = commit_revalidator
        self._cursors: dict[tuple[int, int, bytes, InputFamily, int], int] = {}
        self._generation_tags: dict[
            tuple[int, int, bytes, InputFamily, int], GenerationTag
        ] = {}
        self._input_watermarks: dict[
            tuple[int, int, bytes, InputFamily, int], FactorInputWatermark
        ] = {}
        self._run_identity: RunIdentity | None = None
        self._watermark_set_id: int | None = None
        self._initial_plugin_state = initial_state
        self._committed_plugin_state = initial_state
        self._invalidated = False

    @property
    def shard_id(self) -> int:
        return self._shard_id

    @property
    def cursors(self) -> dict[tuple[int, int, bytes, InputFamily, int], int]:
        return dict(self._cursors)

    @property
    def run_identity(self) -> RunIdentity | None:
        return self._run_identity

    @property
    def watermark_set(self) -> FactorInputWatermarkSet:
        """Return the exact committed input map used by runtime checkpoints."""
        if (
            self._run_identity is None
            or self._watermark_set_id is None
            or not self._input_watermarks
        ):
            raise CheckpointError("runtime has no committed input watermark set")
        return FactorInputWatermarkSet(
            watermark_set_id=self._watermark_set_id,
            trade_date=self._run_identity.trade_date,
            entries=tuple(self._input_watermarks.values()),
        )

    def _assert_declared_input(self, metadata: BatchMetadata) -> None:
        declared = {
            (item.source_stream_id, item.family, item.shard_id)
            for item in self._plugin.spec.inputs
            if item.family is not InputFamily.LATEST_STATE
        }
        if (metadata.source_stream_id, metadata.family, metadata.shard_id) not in declared:
            raise CursorError("batch input was not declared by FactorSpec")

    @staticmethod
    def _instrument_ids(records: np.ndarray) -> np.ndarray:
        return records["header"]["instrument_id"]

    def process_batch(self, batch: MdlBatchView) -> tuple[Any, ...]:
        if self._invalidated:
            raise CursorError("runtime generation was invalidated and cannot be reused")
        if not isinstance(batch, MdlBatchView) or not batch.is_active:
            raise CursorError("process_batch requires an active MdlBatchView context")
        metadata = batch.metadata
        self._assert_declared_input(metadata)
        InstrumentShardRouter.assert_owner(
            self._instrument_ids(batch.records), self._shard_id
        )
        input_key = metadata.input_key
        run_identity = RunIdentity.from_metadata(metadata)
        if self._run_identity is not None and self._run_identity != run_identity:
            raise CursorError(
                "trade date, full clock identity, and registry identity are "
                "run-global and cannot be mixed across inputs"
            )
        generation_tag = GenerationTag.from_metadata(metadata)
        previous_tag = self._generation_tags.get(input_key)
        if previous_tag is not None and previous_tag != generation_tag:
            raise CursorError("per-input writer/source/canonical generation changed")
        expected_cursor = self._cursors.get(input_key)
        if expected_cursor is not None and metadata.begin_canonical_cursor != expected_cursor:
            raise CursorError(
                "batch begin cursor is not the committed exclusive-next cursor; "
                "already committed batches cannot be replayed"
            )
        previous_watermark = self._input_watermarks.get(input_key)
        if (
            previous_watermark is not None
            and metadata.max_consumed_origin_wal_end_pos
            < previous_watermark.max_consumed_origin_wal_end_pos
        ):
            raise CursorError("consumed Raw WAL position regressed for one input")
        if (
            self._watermark_set_id is not None
            and metadata.watermark_set_id <= self._watermark_set_id
        ):
            raise CursorError(
                "watermark_set_id must strictly increase for every committed "
                "runtime transaction"
            )

        before = self._plugin.serialize_state()
        if not isinstance(before, bytes):
            raise PluginExecutionError("plugin serialize_state must return bytes")
        if before != self._committed_plugin_state:
            raise PluginExecutionError(
                "plugin state changed outside the runtime transaction boundary"
            )
        try:
            staged_outputs = tuple(self._plugin.on_batch(batch))
            staged_state = self._plugin.serialize_state()
            if not isinstance(staged_state, bytes):
                raise PluginExecutionError("plugin serialize_state must return bytes")
            identities = tuple(_output_identity(output) for output in staged_outputs)
            staged_batch_outputs: dict[tuple[str, ...], bytes] = {}
            for key, digest in identities:
                if key in staged_batch_outputs:
                    raise OutputConflictError(
                        "plugin emitted one idempotency key more than once in a batch"
                    )
                staged_batch_outputs[key] = digest
            # Complete every allocation before native commit/frontier
            # revalidation.  After native authorization succeeds, commit is
            # only reference replacement plus immutable-byte assignment.
            staged_cursors = dict(self._cursors)
            staged_cursors[input_key] = metadata.end_canonical_cursor
            staged_generation_tags = dict(self._generation_tags)
            staged_generation_tags.setdefault(input_key, generation_tag)
            staged_input_watermarks = dict(self._input_watermarks)
            staged_input_watermarks[input_key] = FactorInputWatermark(
                source_stream_id=metadata.source_stream_id,
                origin_capture_date=metadata.origin_capture_date,
                origin_stream_day_id=metadata.origin_stream_day_id,
                family=metadata.family,
                shard_id=metadata.shard_id,
                canonical_cursor=metadata.end_canonical_cursor,
                max_consumed_origin_wal_end_pos=(
                    metadata.max_consumed_origin_wal_end_pos
                ),
                observed_raw_durable_wal_pos=(
                    metadata.observed_raw_durable_wal_pos
                ),
                clock_epoch_algorithm=metadata.clock_epoch_algorithm,
                clock_epoch_digest=metadata.clock_epoch_digest,
                clock_epoch_label=metadata.clock_epoch_label,
                # Preserve every quality condition observed over the consumed
                # prefix; a clean later batch must not erase an earlier flag.
                input_quality_flags=(
                    metadata.batch_quality_flags
                    | (
                        previous_watermark.input_quality_flags
                        if previous_watermark is not None
                        else 0
                    )
                ),
            )
            staged_run_identity = self._run_identity or run_identity
            staged_watermark_set_id = metadata.watermark_set_id
        except Exception as error:
            try:
                self._plugin.restore_state(before)
            except Exception as rollback_error:
                raise PluginExecutionError(
                    "plugin transaction failed and state rollback also failed"
                ) from rollback_error
            if isinstance(error, PluginExecutionError):
                raise
            raise PluginExecutionError(
                "plugin transaction failed; state, cursor, and outputs were not committed"
            ) from error

        try:
            authorized = self._commit_revalidator(batch)
        except Exception as error:
            try:
                self._plugin.restore_state(before)
            finally:
                self.invalidate_generation()
            raise PluginExecutionError(
                "native commit/frontier revalidation failed; generation was invalidated"
            ) from error
        if authorized is not True:
            try:
                self._plugin.restore_state(before)
            finally:
                self.invalidate_generation()
            raise PluginExecutionError(
                "native commit/frontier revalidation denied; generation was invalidated"
            )

        # The plugin's staged state becomes committed only after the final
        # native revalidation above.  All runtime-owned maps update afterward.
        self._cursors = staged_cursors
        self._generation_tags = staged_generation_tags
        self._input_watermarks = staged_input_watermarks
        self._run_identity = staged_run_identity
        self._watermark_set_id = staged_watermark_set_id
        self._committed_plugin_state = staged_state
        return staged_outputs

    def invalidate_generation(self) -> None:
        """Irreversibly revoke this runtime after live SourceFrontier FATAL."""
        try:
            self._plugin.restore_state(self._initial_plugin_state)
        finally:
            self._cursors.clear()
            self._generation_tags.clear()
            self._input_watermarks.clear()
            self._run_identity = None
            self._watermark_set_id = None
            self._committed_plugin_state = self._initial_plugin_state
            self._invalidated = True

    def checkpoint_bytes(self) -> bytes:
        if self._invalidated:
            raise CheckpointError("an invalidated generation cannot publish a checkpoint")
        state = self._plugin.serialize_state()
        if not isinstance(state, bytes) or state != self._committed_plugin_state:
            raise CheckpointError("plugin state is not the last committed transaction state")
        # Bound the raw state before base64 expansion allocates roughly 4/3x.
        if len(state) > _MAX_PLUGIN_STATE_BYTES:
            raise CheckpointError("raw plugin state exceeds the V1 size bound")
        watermark_set = self.watermark_set
        cursors = []
        for entry in watermark_set.entries:
            key = (
                entry.source_stream_id,
                entry.origin_capture_date,
                entry.origin_stream_day_id,
                entry.family,
                entry.shard_id,
            )
            cursors.append(
                {
                    "canonical_cursor": entry.canonical_cursor,
                    "clock_epoch_algorithm": entry.clock_epoch_algorithm,
                    "clock_epoch_digest": entry.clock_epoch_digest.hex(),
                    "clock_epoch_label": entry.clock_epoch_label,
                    "family": entry.family.value,
                    "generation_tag": self._generation_tags[key].canonical_object(),
                    "input_quality_flags": entry.input_quality_flags,
                    "max_consumed_origin_wal_end_pos": (
                        entry.max_consumed_origin_wal_end_pos
                    ),
                    "observed_raw_durable_wal_pos": (
                        entry.observed_raw_durable_wal_pos
                    ),
                    "origin_capture_date": entry.origin_capture_date,
                    "origin_stream_day_id": entry.origin_stream_day_id.hex(),
                    "shard_id": entry.shard_id,
                    "source_stream_id": entry.source_stream_id,
                }
            )
        body = {
            "cursors": cursors,
            "encoding": "l2flow-factor-runtime-state-v1",
            "factor_id": self._plugin.spec.factor_id,
            "factor_spec_sha256": self._plugin.spec.sha256_hex(),
            "factor_version": self._plugin.spec.factor_version,
            "input_identity_sha256": watermark_set.input_identity_hex(),
            "plugin_state_base64": base64.b64encode(state).decode("ascii"),
            "plugin_state_length": len(state),
            "plugin_state_sha256": hashlib.sha256(state).hexdigest(),
            "run_identity": self._run_identity.canonical_object(),
            "shard_id": self._shard_id,
            "watermark_set_id": watermark_set.watermark_set_id,
        }
        encoded = json.dumps(
            body,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
        if len(encoded) > _MAX_RUNTIME_STATE_BYTES:
            raise CheckpointError("runtime checkpoint exceeds the V1 size bound")
        return encoded + hashlib.sha256(_RUNTIME_STATE_DOMAIN + encoded).digest()

    def restore_checkpoint_bytes(self, blob: bytes) -> None:
        if self._invalidated:
            raise CheckpointError("an invalidated runtime cannot be restored")
        if (
            self._cursors
            or self._generation_tags
            or self._input_watermarks
            or self._run_identity is not None
            or self._watermark_set_id is not None
        ):
            raise CheckpointError("runtime restore requires a fresh runtime")
        decoded = decode_runtime_checkpoint(blob)
        if decoded.factor_id != self._plugin.spec.factor_id:
            raise CheckpointError("runtime checkpoint factor_id mismatch")
        if decoded.factor_version != self._plugin.spec.factor_version:
            raise CheckpointError("runtime checkpoint factor_version mismatch")
        if decoded.factor_spec_sha256 != self._plugin.spec.sha256():
            raise CheckpointError("runtime checkpoint FactorSpec identity mismatch")
        if decoded.shard_id != self._shard_id:
            raise CheckpointError("runtime checkpoint shard mismatch")
        declared = {
            (spec.source_stream_id, spec.family, spec.shard_id)
            for spec in self._plugin.spec.inputs
            if spec.family is not InputFamily.LATEST_STATE
        }
        staged_tags = dict(decoded.generation_tags)
        staged_cursors: dict[tuple[int, int, bytes, InputFamily, int], int] = {}
        staged_watermarks: dict[
            tuple[int, int, bytes, InputFamily, int], FactorInputWatermark
        ] = {}
        for entry in decoded.watermark_set.entries:
            key = (
                entry.source_stream_id,
                entry.origin_capture_date,
                entry.origin_stream_day_id,
                entry.family,
                entry.shard_id,
            )
            if (key[0], key[3], key[4]) not in declared or key[4] != self._shard_id:
                raise CheckpointError("runtime cursor was not declared for this shard")
            if key not in staged_tags:
                raise CheckpointError("runtime input generation tag is missing")
            staged_cursors[key] = entry.canonical_cursor
            staged_watermarks[key] = entry
        before = self._plugin.serialize_state()
        if not isinstance(before, bytes):
            raise CheckpointError("plugin serialize_state must return bytes")
        try:
            self._plugin.restore_state(decoded.plugin_state)
        except Exception as error:
            try:
                self._plugin.restore_state(before)
            except Exception as rollback_error:
                raise CheckpointError(
                    "plugin rejected restored state and rollback also failed"
                ) from rollback_error
            raise CheckpointError("plugin rejected restored state") from error
        self._committed_plugin_state = decoded.plugin_state
        self._cursors = staged_cursors
        self._generation_tags = staged_tags
        self._input_watermarks = staged_watermarks
        self._run_identity = decoded.run_identity
        self._watermark_set_id = decoded.watermark_set.watermark_set_id


def decode_runtime_checkpoint(blob: bytes) -> DecodedRuntimeCheckpoint:
    """Strictly decode runtime state without executing plugin code."""
    if (
        not isinstance(blob, bytes)
        or len(blob) < 32
        or len(blob) > _MAX_RUNTIME_STATE_BYTES + 32
    ):
        raise CheckpointError("runtime checkpoint is truncated or oversized")
    encoded, digest = blob[:-32], blob[-32:]
    if hashlib.sha256(_RUNTIME_STATE_DOMAIN + encoded).digest() != digest:
        raise CheckpointError("runtime checkpoint SHA-256 mismatch")

    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise CheckpointError("runtime checkpoint contains duplicate JSON keys")
            result[key] = value
        return result

    try:
        body = json.loads(encoded.decode("ascii"), object_pairs_hook=unique_object)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise CheckpointError("runtime checkpoint JSON is invalid") from error
    expected_keys = {
        "cursors",
        "encoding",
        "factor_id",
        "factor_spec_sha256",
        "factor_version",
        "input_identity_sha256",
        "plugin_state_base64",
        "plugin_state_length",
        "plugin_state_sha256",
        "run_identity",
        "shard_id",
        "watermark_set_id",
    }
    if not isinstance(body, dict) or set(body) != expected_keys:
        raise CheckpointError("runtime checkpoint fields are invalid")
    if body["encoding"] != "l2flow-factor-runtime-state-v1":
        raise CheckpointError("runtime checkpoint encoding mismatch")
    factor_id = _json_factor_id(body["factor_id"])
    factor_version = _json_factor_version(body["factor_version"])
    factor_spec_sha256 = _json_hex(
        body["factor_spec_sha256"], 32, "factor_spec_sha256"
    )
    shard_id = _json_uint(body["shard_id"], 32, "shard_id")
    if shard_id >= LOGICAL_FACTOR_SHARDS:
        raise CheckpointError("runtime checkpoint shard is outside [0, 15]")
    watermark_set_id = _json_uint(
        body["watermark_set_id"], 64, "watermark_set_id", positive=True
    )

    run_object = body["run_identity"]
    if not isinstance(run_object, dict) or set(run_object) != {
        "clock_epoch_algorithm",
        "clock_epoch_digest",
        "registry_sha256",
        "registry_version",
        "trade_date",
    }:
        raise CheckpointError("runtime run-global identity is invalid")
    try:
        run_identity = RunIdentity(
            trade_date=_json_uint(
                run_object["trade_date"], 32, "trade_date", positive=True
            ),
            clock_epoch_algorithm=_json_uint(
                run_object["clock_epoch_algorithm"],
                32,
                "clock_epoch_algorithm",
                positive=True,
            ),
            clock_epoch_digest=_json_hex(
                run_object["clock_epoch_digest"], 32, "clock_epoch_digest"
            ),
            registry_version=_json_uint(
                run_object["registry_version"],
                64,
                "registry_version",
                positive=True,
            ),
            registry_sha256=_json_hex(
                run_object["registry_sha256"], 32, "registry_sha256"
            ),
        )
    except ValidationError as error:
        raise CheckpointError("runtime run-global identity failed validation") from error

    state_length = _json_uint(
        body["plugin_state_length"], 64, "plugin_state_length"
    )
    if state_length > _MAX_PLUGIN_STATE_BYTES:
        raise CheckpointError("raw runtime plugin state exceeds the V1 size bound")
    state_sha256 = _json_hex(
        body["plugin_state_sha256"], 32, "plugin_state_sha256"
    )
    state_base64 = body["plugin_state_base64"]
    expected_base64_length = 4 * ((state_length + 2) // 3)
    if not isinstance(state_base64, str) or len(state_base64) != expected_base64_length:
        raise CheckpointError("runtime plugin state base64 length is invalid")
    try:
        plugin_state = base64.b64decode(state_base64, validate=True)
    except (ValueError, TypeError) as error:
        raise CheckpointError("runtime plugin state base64 is invalid") from error
    if (
        len(plugin_state) != state_length
        or base64.b64encode(plugin_state).decode("ascii") != state_base64
        or hashlib.sha256(plugin_state).digest() != state_sha256
    ):
        raise CheckpointError("runtime plugin state encoding/length/hash mismatch")

    cursor_items = body["cursors"]
    if (
        not isinstance(cursor_items, list)
        or not cursor_items
        or len(cursor_items) > _MAX_CURSOR_ENTRIES
    ):
        raise CheckpointError("runtime cursor array has an invalid entry count")
    cursor_keys = {
        "canonical_cursor",
        "clock_epoch_algorithm",
        "clock_epoch_digest",
        "clock_epoch_label",
        "family",
        "generation_tag",
        "input_quality_flags",
        "max_consumed_origin_wal_end_pos",
        "observed_raw_durable_wal_pos",
        "origin_capture_date",
        "origin_stream_day_id",
        "shard_id",
        "source_stream_id",
    }
    watermark_entries: list[FactorInputWatermark] = []
    generation_tags: list[
        tuple[tuple[int, int, bytes, InputFamily, int], GenerationTag]
    ] = []
    previous_sort_key: tuple[int, int, bytes, int, int] | None = None
    try:
        for item in cursor_items:
            if not isinstance(item, dict) or set(item) != cursor_keys:
                raise CheckpointError("runtime cursor entry fields are invalid")
            family = InputFamily(item["family"])
            entry = FactorInputWatermark(
                source_stream_id=_json_uint(
                    item["source_stream_id"], 32, "source_stream_id", positive=True
                ),
                origin_capture_date=_json_uint(
                    item["origin_capture_date"],
                    32,
                    "origin_capture_date",
                    positive=True,
                ),
                origin_stream_day_id=_json_hex(
                    item["origin_stream_day_id"], 16, "origin_stream_day_id"
                ),
                family=family,
                shard_id=_json_uint(item["shard_id"], 32, "shard_id"),
                canonical_cursor=_json_uint(
                    item["canonical_cursor"], 64, "canonical_cursor"
                ),
                max_consumed_origin_wal_end_pos=_json_uint(
                    item["max_consumed_origin_wal_end_pos"],
                    64,
                    "max_consumed_origin_wal_end_pos",
                ),
                observed_raw_durable_wal_pos=_json_uint(
                    item["observed_raw_durable_wal_pos"],
                    64,
                    "observed_raw_durable_wal_pos",
                ),
                clock_epoch_algorithm=_json_uint(
                    item["clock_epoch_algorithm"],
                    32,
                    "clock_epoch_algorithm",
                    positive=True,
                ),
                clock_epoch_digest=_json_hex(
                    item["clock_epoch_digest"], 32, "clock_epoch_digest"
                ),
                clock_epoch_label=_json_uint(
                    item["clock_epoch_label"], 64, "clock_epoch_label"
                ),
                input_quality_flags=_json_uint(
                    item["input_quality_flags"], 64, "input_quality_flags"
                ),
            )
            if entry.shard_id != shard_id:
                raise CheckpointError("runtime cursor shard differs from runtime shard")
            if (
                entry.clock_epoch_algorithm != run_identity.clock_epoch_algorithm
                or entry.clock_epoch_digest != run_identity.clock_epoch_digest
            ):
                raise CheckpointError(
                    "runtime cursor clock differs from run-global clock identity"
                )
            if previous_sort_key is not None and entry.sort_key <= previous_sort_key:
                raise CheckpointError(
                    "runtime cursor array is not in unique canonical key order"
                )
            previous_sort_key = entry.sort_key
            tag_object = item["generation_tag"]
            if not isinstance(tag_object, dict) or set(tag_object) != {
                "canonical_generation",
                "origin_source_generation",
                "origin_source_writer_instance",
            }:
                raise CheckpointError("runtime per-input generation tag is invalid")
            tag = GenerationTag(
                origin_source_writer_instance=_json_hex(
                    tag_object["origin_source_writer_instance"],
                    16,
                    "origin_source_writer_instance",
                ),
                origin_source_generation=_json_uint(
                    tag_object["origin_source_generation"],
                    64,
                    "origin_source_generation",
                    positive=True,
                ),
                canonical_generation=_json_uint(
                    tag_object["canonical_generation"],
                    64,
                    "canonical_generation",
                    positive=True,
                ),
            )
            key = (
                entry.source_stream_id,
                entry.origin_capture_date,
                entry.origin_stream_day_id,
                entry.family,
                entry.shard_id,
            )
            watermark_entries.append(entry)
            generation_tags.append((key, tag))
        watermark_set = FactorInputWatermarkSet(
            watermark_set_id=watermark_set_id,
            trade_date=run_identity.trade_date,
            entries=tuple(watermark_entries),
        )
    except (ValidationError, ValueError, TypeError, KeyError) as error:
        raise CheckpointError("runtime cursor map failed strict decoding") from error
    input_identity_sha256 = _json_hex(
        body["input_identity_sha256"], 32, "input_identity_sha256"
    )
    if watermark_set.input_identity_hash() != input_identity_sha256:
        raise CheckpointError("runtime frozen input identity does not match its cursor map")

    canonical = json.dumps(
        body,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    if canonical != encoded:
        raise CheckpointError("runtime checkpoint JSON is not canonical")
    return DecodedRuntimeCheckpoint(
        factor_id=factor_id,
        factor_version=factor_version,
        factor_spec_sha256=factor_spec_sha256,
        shard_id=shard_id,
        run_identity=run_identity,
        watermark_set=watermark_set,
        generation_tags=tuple(generation_tags),
        plugin_state=plugin_state,
    )


ShardRouter = InstrumentShardRouter
TransactionRuntime = FactorTransactionRuntime
