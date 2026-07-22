from __future__ import annotations

import base64
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import stat
import struct
import tempfile
from typing import Any, Mapping

from .canonical import InputFamily
from .errors import CheckpointError, DurabilityBarrierError, ValidationError
from .watermark import (
    FactorInputWatermark,
    FactorInputWatermarkSet,
    RawNamespace,
    check_durability_barrier,
)


_MAGIC = b"L2FCPJ1\x00"
_MAX_JSON_BYTES = 256 * 1024 * 1024
_MAX_STATE_BYTES = 128 * 1024 * 1024


def _fixed_bytes(value: object, width: int, name: str) -> bytes:
    if isinstance(value, str):
        if len(value) != width * 2 or value.lower() != value:
            raise ValidationError(f"{name} must be lowercase hex")
        try:
            result = bytes.fromhex(value)
        except ValueError as error:
            raise ValidationError(f"{name} must be lowercase hex") from error
    elif isinstance(value, (bytes, bytearray, memoryview)):
        result = bytes(value)
    else:
        raise ValidationError(f"{name} must be bytes or lowercase hex")
    if len(result) != width or not any(result):
        raise ValidationError(f"{name} must be exactly {width} nonzero bytes")
    return result


def _identifier(value: object, name: str) -> str:
    if not isinstance(value, str) or not value or len(value) > 128:
        raise ValidationError(f"{name} must be a non-empty bounded string")
    if any(ord(character) < 0x21 or ord(character) > 0x7E for character in value):
        raise ValidationError(f"{name} must contain printable ASCII without spaces")
    return value


@dataclass(frozen=True, slots=True)
class FactorCheckpoint:
    factor_id: str
    factor_version: str
    factor_code_sha256: bytes
    factor_config_sha256: bytes
    state_schema_sha256: bytes
    registry_version: int
    registry_sha256: bytes
    watermark_set: FactorInputWatermarkSet
    state_codec: str
    state_bytes: bytes

    def __post_init__(self) -> None:
        _identifier(self.factor_id, "factor_id")
        _identifier(self.factor_version, "factor_version")
        object.__setattr__(self, "factor_code_sha256", _fixed_bytes(
            self.factor_code_sha256, 32, "factor_code_sha256"
        ))
        object.__setattr__(self, "factor_config_sha256", _fixed_bytes(
            self.factor_config_sha256, 32, "factor_config_sha256"
        ))
        object.__setattr__(self, "state_schema_sha256", _fixed_bytes(
            self.state_schema_sha256, 32, "state_schema_sha256"
        ))
        if (
            type(self.registry_version) is not int
            or self.registry_version <= 0
            or self.registry_version > (1 << 64) - 1
        ):
            raise ValidationError("registry_version must be a positive uint64")
        object.__setattr__(self, "registry_sha256", _fixed_bytes(
            self.registry_sha256, 32, "registry_sha256"
        ))
        if not isinstance(self.watermark_set, FactorInputWatermarkSet):
            raise ValidationError("watermark_set must be FactorInputWatermarkSet")
        _identifier(self.state_codec, "state_codec")
        if not isinstance(self.state_bytes, bytes):
            raise ValidationError("state_bytes must be immutable bytes")
        if len(self.state_bytes) > _MAX_STATE_BYTES:
            raise ValidationError("state_bytes exceed the V1 checkpoint bound")


def _entry_object(entry: FactorInputWatermark) -> dict[str, Any]:
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


def _checkpoint_json(checkpoint: FactorCheckpoint) -> bytes:
    state_sha256 = hashlib.sha256(checkpoint.state_bytes).hexdigest()
    value = {
        "encoding": "l2flow-factor-checkpoint-v1",
        "factor_code_sha256": checkpoint.factor_code_sha256.hex(),
        "factor_config_sha256": checkpoint.factor_config_sha256.hex(),
        "factor_id": checkpoint.factor_id,
        "factor_version": checkpoint.factor_version,
        "input_identity_sha256": checkpoint.watermark_set.input_identity_hex(),
        "registry_sha256": checkpoint.registry_sha256.hex(),
        "registry_version": checkpoint.registry_version,
        "state_bytes_base64": base64.b64encode(checkpoint.state_bytes).decode("ascii"),
        "state_bytes_length": len(checkpoint.state_bytes),
        "state_bytes_sha256": state_sha256,
        "state_codec": checkpoint.state_codec,
        "state_schema_sha256": checkpoint.state_schema_sha256.hex(),
        "watermark_set": {
            "entries": [_entry_object(entry) for entry in checkpoint.watermark_set.entries],
            "trade_date": checkpoint.watermark_set.trade_date,
            "watermark_set_id": checkpoint.watermark_set.watermark_set_id,
        },
    }
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")


def encode_checkpoint(checkpoint: FactorCheckpoint) -> bytes:
    payload = _checkpoint_json(checkpoint)
    if len(payload) > _MAX_JSON_BYTES:
        raise CheckpointError("checkpoint canonical JSON exceeds the V1 bound")
    prefix = _MAGIC + struct.pack("<Q", len(payload))
    return prefix + payload + hashlib.sha256(prefix + payload).digest()


def _require_object(value: Any, keys: set[str], name: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != keys:
        raise CheckpointError(f"{name} has unknown or missing fields")
    return value


def decode_checkpoint(blob: bytes) -> FactorCheckpoint:
    if not isinstance(blob, bytes) or len(blob) < 16 + 32:
        raise CheckpointError("checkpoint is truncated")
    if blob[:8] != _MAGIC:
        raise CheckpointError("checkpoint magic/version mismatch")
    (payload_length,) = struct.unpack_from("<Q", blob, 8)
    if payload_length > _MAX_JSON_BYTES or len(blob) != 16 + payload_length + 32:
        raise CheckpointError("checkpoint payload length is invalid")
    signed = blob[: 16 + payload_length]
    if hashlib.sha256(signed).digest() != blob[-32:]:
        raise CheckpointError("checkpoint envelope SHA-256 mismatch")
    payload_bytes = blob[16 : 16 + payload_length]

    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise CheckpointError("checkpoint JSON contains a duplicate key")
            result[key] = value
        return result

    try:
        root = json.loads(
            payload_bytes.decode("ascii"), object_pairs_hook=unique_object
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise CheckpointError("checkpoint canonical JSON is invalid") from error
    root_keys = {
        "encoding", "factor_code_sha256", "factor_config_sha256", "factor_id", "factor_version",
        "input_identity_sha256", "registry_sha256", "registry_version",
        "state_bytes_base64", "state_bytes_length", "state_bytes_sha256",
        "state_codec", "state_schema_sha256", "watermark_set",
    }
    root = _require_object(root, root_keys, "checkpoint")
    if root["encoding"] != "l2flow-factor-checkpoint-v1":
        raise CheckpointError("checkpoint encoding identity mismatch")
    watermark_object = _require_object(
        root["watermark_set"],
        {"entries", "trade_date", "watermark_set_id"},
        "watermark_set",
    )
    if not isinstance(watermark_object["entries"], list):
        raise CheckpointError("watermark entries must be an array")
    entry_keys = {
        "canonical_cursor", "clock_epoch_algorithm", "clock_epoch_digest",
        "clock_epoch_label", "family", "input_quality_flags",
        "max_consumed_origin_wal_end_pos", "observed_raw_durable_wal_pos",
        "origin_capture_date", "origin_stream_day_id", "shard_id",
        "source_stream_id",
    }
    try:
        entries = tuple(
            FactorInputWatermark(
                source_stream_id=_require_object(item, entry_keys, "watermark entry")[
                    "source_stream_id"
                ],
                origin_capture_date=item["origin_capture_date"],
                origin_stream_day_id=item["origin_stream_day_id"],
                family=InputFamily(item["family"]),
                shard_id=item["shard_id"],
                canonical_cursor=item["canonical_cursor"],
                max_consumed_origin_wal_end_pos=item[
                    "max_consumed_origin_wal_end_pos"
                ],
                observed_raw_durable_wal_pos=item[
                    "observed_raw_durable_wal_pos"
                ],
                clock_epoch_algorithm=item["clock_epoch_algorithm"],
                clock_epoch_digest=item["clock_epoch_digest"],
                clock_epoch_label=item["clock_epoch_label"],
                input_quality_flags=item["input_quality_flags"],
            )
            for item in watermark_object["entries"]
        )
        watermark_set = FactorInputWatermarkSet(
            watermark_set_id=watermark_object["watermark_set_id"],
            trade_date=watermark_object["trade_date"],
            entries=entries,
        )
    except (ValidationError, ValueError, KeyError, TypeError) as error:
        raise CheckpointError("watermark set failed strict validation") from error
    if watermark_set.input_identity_hex() != root["input_identity_sha256"]:
        raise CheckpointError("checkpoint input identity does not match its full map")
    state_length = root["state_bytes_length"]
    if (
        type(state_length) is not int
        or state_length < 0
        or state_length > _MAX_STATE_BYTES
    ):
        raise CheckpointError("state byte length is outside the V1 bound")
    state_base64 = root["state_bytes_base64"]
    expected_base64_length = 4 * ((state_length + 2) // 3)
    if not isinstance(state_base64, str) or len(state_base64) != expected_base64_length:
        raise CheckpointError("state base64 length does not match its raw bound")
    try:
        state_bytes = base64.b64decode(state_base64, validate=True)
    except (ValueError, TypeError) as error:
        raise CheckpointError("state_bytes_base64 is invalid") from error
    if (
        len(state_bytes) != state_length
        or base64.b64encode(state_bytes).decode("ascii") != state_base64
    ):
        raise CheckpointError("state byte length mismatch")
    if hashlib.sha256(state_bytes).hexdigest() != root["state_bytes_sha256"]:
        raise CheckpointError("state byte SHA-256 mismatch")
    try:
        checkpoint = FactorCheckpoint(
            factor_id=root["factor_id"],
            factor_version=root["factor_version"],
            factor_code_sha256=root["factor_code_sha256"],
            factor_config_sha256=root["factor_config_sha256"],
            state_schema_sha256=root["state_schema_sha256"],
            registry_version=root["registry_version"],
            registry_sha256=root["registry_sha256"],
            watermark_set=watermark_set,
            state_codec=root["state_codec"],
            state_bytes=state_bytes,
        )
    except ValidationError as error:
        raise CheckpointError("checkpoint metadata failed strict validation") from error
    if _checkpoint_json(checkpoint) != payload_bytes:
        raise CheckpointError("checkpoint JSON is not the exact canonical encoding")
    return checkpoint


def _verify_state_watermark_binding(
    checkpoint: FactorCheckpoint,
    *,
    generic_state_watermark_binding_asserted: bool,
) -> None:
    """Verify known codecs, or require an explicit assertion for generic ones.

    A generic state codec is opaque to this store.  The assertion means the
    caller, not AtomicCheckpointStore, has verified that its state bytes and
    outer watermark set describe the same consumed input frontier.
    """
    if type(generic_state_watermark_binding_asserted) is not bool:
        raise CheckpointError(
            "generic_state_watermark_binding_asserted must be an exact bool"
        )
    # Lazy import keeps the generic checkpoint codec independent from plugin
    # execution while allowing strict inspection of the repository runtime's
    # non-executable canonical state format.
    from .runtime import RUNTIME_STATE_CODEC_V1, decode_runtime_checkpoint

    if checkpoint.state_codec == RUNTIME_STATE_CODEC_V1:
        decoded = decode_runtime_checkpoint(checkpoint.state_bytes)
        if decoded.factor_id != checkpoint.factor_id:
            raise CheckpointError("runtime state factor_id differs from its envelope")
        if decoded.factor_version != checkpoint.factor_version:
            raise CheckpointError(
                "runtime state factor_version differs from its envelope"
            )
        if decoded.factor_spec_sha256 != checkpoint.factor_config_sha256:
            raise CheckpointError(
                "runtime FactorSpec identity differs from factor_config_sha256"
            )
        if decoded.run_identity.registry_version != checkpoint.registry_version:
            raise CheckpointError(
                "runtime state registry version differs from its envelope"
            )
        if decoded.run_identity.registry_sha256 != checkpoint.registry_sha256:
            raise CheckpointError(
                "runtime state registry identity differs from its envelope"
            )
        if (
            decoded.watermark_set.input_identity_hash()
            != checkpoint.watermark_set.input_identity_hash()
        ):
            raise CheckpointError(
                "runtime state input identity differs from its envelope watermark"
            )
        if decoded.watermark_set != checkpoint.watermark_set:
            raise CheckpointError(
                "runtime state full cursor/WAL map differs from its envelope watermark"
            )
        return
    if generic_state_watermark_binding_asserted is not True:
        raise CheckpointError(
            "generic state codec requires the caller to assert state/watermark binding; "
            "AtomicCheckpointStore cannot verify opaque state bytes"
        )


class AtomicCheckpointStore:
    """Versioned checkpoint codec with per-namespace durability gating."""

    @staticmethod
    def publish(
        path: str | os.PathLike[str],
        checkpoint: FactorCheckpoint,
        current_durable_positions: Mapping[RawNamespace, int],
        *,
        generic_state_watermark_binding_asserted: bool = False,
    ) -> bytes:
        if not isinstance(checkpoint, FactorCheckpoint):
            raise CheckpointError("checkpoint must be a FactorCheckpoint")
        _verify_state_watermark_binding(
            checkpoint,
            generic_state_watermark_binding_asserted=(
                generic_state_watermark_binding_asserted
            ),
        )
        barrier = check_durability_barrier(
            checkpoint.watermark_set, current_durable_positions
        )
        if not barrier.satisfied:
            raise DurabilityBarrierError(
                f"checkpoint blocked by {len(barrier.failures)} Raw namespace(s)"
            )
        target = Path(path)
        parent = target.parent
        if not parent.is_dir():
            raise CheckpointError("checkpoint parent directory does not exist")
        try:
            existing = os.lstat(target)
        except FileNotFoundError:
            existing = None
        if existing is not None and not stat.S_ISREG(existing.st_mode):
            raise CheckpointError("existing checkpoint target is not a regular file")
        encoded = encode_checkpoint(checkpoint)
        temporary_name: str | None = None
        descriptor = -1
        try:
            descriptor, temporary_name = tempfile.mkstemp(
                prefix=f".{target.name}.tmp-", dir=str(parent)
            )
            os.fchmod(descriptor, 0o600)
            written = 0
            while written < len(encoded):
                count = os.write(descriptor, encoded[written:])
                if count <= 0:
                    raise CheckpointError("checkpoint write made no progress")
                written += count
            os.fsync(descriptor)
            os.close(descriptor)
            descriptor = -1
            os.replace(temporary_name, target)
            temporary_name = None
            directory_descriptor = os.open(parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory_descriptor)
            finally:
                os.close(directory_descriptor)
        except OSError as error:
            raise CheckpointError("atomic checkpoint publication failed") from error
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            if temporary_name is not None:
                try:
                    os.unlink(temporary_name)
                except FileNotFoundError:
                    pass
        return hashlib.sha256(encoded).digest()

    @staticmethod
    def load(
        path: str | os.PathLike[str],
        *,
        expected_factor_id: str,
        expected_factor_version: str,
        expected_factor_code_sha256: bytes,
        expected_factor_config_sha256: bytes,
        expected_state_schema_sha256: bytes,
        expected_registry_version: int,
        expected_registry_sha256: bytes,
        current_durable_positions: Mapping[RawNamespace, int],
        generic_state_watermark_binding_asserted: bool = False,
    ) -> FactorCheckpoint:
        try:
            expected_id = _identifier(expected_factor_id, "expected_factor_id")
            expected_version = _identifier(
                expected_factor_version, "expected_factor_version"
            )
        except ValidationError as error:
            raise CheckpointError("expected factor identity is invalid") from error
        if (
            type(expected_registry_version) is not int
            or expected_registry_version <= 0
            or expected_registry_version > (1 << 64) - 1
        ):
            raise CheckpointError("expected_registry_version must be a positive uint64")
        descriptor = -1
        try:
            flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
            descriptor = os.open(path, flags)
            status = os.fstat(descriptor)
            if not stat.S_ISREG(status.st_mode) or status.st_size > _MAX_JSON_BYTES + 48:
                raise CheckpointError("checkpoint is not a bounded regular file")
            chunks: list[bytes] = []
            remaining = status.st_size
            while remaining:
                chunk = os.read(descriptor, min(remaining, 1024 * 1024))
                if not chunk:
                    raise CheckpointError("checkpoint became truncated during read")
                chunks.append(chunk)
                remaining -= len(chunk)
            if os.read(descriptor, 1):
                raise CheckpointError("checkpoint grew beyond its validated bound")
            blob = b"".join(chunks)
        except OSError as error:
            raise CheckpointError("checkpoint read failed") from error
        finally:
            if descriptor >= 0:
                os.close(descriptor)
        checkpoint = decode_checkpoint(blob)
        try:
            expected_code = _fixed_bytes(
                expected_factor_code_sha256, 32, "expected_factor_code_sha256"
            )
            expected_config = _fixed_bytes(
                expected_factor_config_sha256, 32, "expected_factor_config_sha256"
            )
            expected_state = _fixed_bytes(
                expected_state_schema_sha256, 32, "expected_state_schema_sha256"
            )
            expected_registry = _fixed_bytes(
                expected_registry_sha256, 32, "expected_registry_sha256"
            )
        except ValidationError as error:
            raise CheckpointError("expected checkpoint identity is invalid") from error
        if checkpoint.factor_id != expected_id:
            raise CheckpointError("factor_id mismatch")
        if checkpoint.factor_version != expected_version:
            raise CheckpointError("factor_version mismatch")
        if checkpoint.factor_code_sha256 != expected_code:
            raise CheckpointError("factor code identity mismatch")
        if checkpoint.factor_config_sha256 != expected_config:
            raise CheckpointError("factor config identity mismatch")
        if checkpoint.state_schema_sha256 != expected_state:
            raise CheckpointError("state schema identity mismatch")
        if checkpoint.registry_version != expected_registry_version:
            raise CheckpointError("registry version mismatch")
        if checkpoint.registry_sha256 != expected_registry:
            raise CheckpointError("registry identity mismatch")
        _verify_state_watermark_binding(
            checkpoint,
            generic_state_watermark_binding_asserted=(
                generic_state_watermark_binding_asserted
            ),
        )
        barrier = check_durability_barrier(
            checkpoint.watermark_set, current_durable_positions
        )
        if not barrier.satisfied:
            raise DurabilityBarrierError(
                f"checkpoint restore blocked by {len(barrier.failures)} Raw namespace(s)"
            )
        return checkpoint
