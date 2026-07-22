from __future__ import annotations

import hashlib
import json
import struct
from dataclasses import dataclass
from typing import Any, Iterable

from l2flow_factor import (
    FactorInputWatermark,
    FactorInputWatermarkSet,
    InputFamily,
    ValidationError,
)


_SIDECAR_MAGIC = b"L2WSCJ1\x00"
_MAX_SIDECAR_JSON_BYTES = 256 * 1024 * 1024
_MAX_SIDECAR_SETS = 65_536
_MAX_WATERMARK_ENTRIES = 65_536


class SidecarError(ValueError):
    """A watermark sidecar or reference failed its V1 contract."""


class SidecarConflictError(SidecarError):
    """One run-local watermark ID was reused or disagreed with its map."""


class SidecarReferenceError(SidecarError):
    """A persistent reference is orphaned or has the wrong identity."""


def _uint(value: Any, bits: int, name: str, *, positive: bool = False) -> int:
    minimum = 1 if positive else 0
    if type(value) is not int or value < minimum or value > (1 << bits) - 1:
        qualifier = "positive " if positive else ""
        raise SidecarError(f"{name} must be an exact {qualifier}uint{bits}")
    return value


def _fixed_bytes(
    value: Any,
    width: int,
    name: str,
    *,
    nonzero: bool = True,
) -> bytes:
    if isinstance(value, str):
        if len(value) != width * 2 or value.lower() != value:
            raise SidecarError(f"{name} must be lowercase fixed-width hex")
        try:
            result = bytes.fromhex(value)
        except ValueError as error:
            raise SidecarError(f"{name} must be lowercase fixed-width hex") from error
    elif isinstance(value, (bytes, bytearray, memoryview)):
        result = bytes(value)
    else:
        raise SidecarError(f"{name} must be bytes or lowercase hex")
    if len(result) != width or (nonzero and not any(result)):
        raise SidecarError(f"{name} must be exactly {width} nonzero bytes")
    return result


def _canonical_json(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
    except (TypeError, ValueError, UnicodeEncodeError) as error:
        raise SidecarError("value cannot be represented as canonical JSON") from error


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise SidecarError("canonical JSON contains a duplicate object key")
        result[key] = value
    return result


def _decode_json(payload: bytes, maximum_bytes: int) -> Any:
    if not isinstance(payload, bytes) or len(payload) > maximum_bytes:
        raise SidecarError("canonical JSON payload is invalid or oversized")
    try:
        return json.loads(payload.decode("ascii"), object_pairs_hook=_unique_object)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise SidecarError("canonical JSON payload is invalid") from error


def _require_object(value: Any, keys: set[str], name: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != keys:
        raise SidecarError(f"{name} has unknown or missing fields")
    return value


def _encode_envelope(magic: bytes, payload: bytes, maximum_bytes: int) -> bytes:
    if not isinstance(magic, bytes) or len(magic) != 8:
        raise SidecarError("envelope magic must contain exactly 8 bytes")
    if not isinstance(payload, bytes) or len(payload) > maximum_bytes:
        raise SidecarError("envelope payload is oversized")
    prefix = magic + struct.pack("<Q", len(payload))
    return prefix + payload + hashlib.sha256(prefix + payload).digest()


def _decode_envelope(
    blob: bytes,
    magic: bytes,
    maximum_bytes: int,
) -> bytes:
    if not isinstance(blob, bytes) or len(blob) < 48:
        raise SidecarError("envelope is truncated")
    if blob[:8] != magic:
        raise SidecarError("envelope magic/version mismatch")
    (payload_length,) = struct.unpack_from("<Q", blob, 8)
    if payload_length > maximum_bytes or len(blob) != 16 + payload_length + 32:
        raise SidecarError("envelope length is invalid")
    signed = blob[: 16 + payload_length]
    if hashlib.sha256(signed).digest() != blob[-32:]:
        raise SidecarError("envelope SHA-256 mismatch")
    return blob[16 : 16 + payload_length]


@dataclass(frozen=True, slots=True, order=True)
class SidecarNamespace:
    run_id: bytes
    table_generation: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "run_id", _fixed_bytes(self.run_id, 16, "run_id"))
        _uint(self.table_generation, 64, "table_generation", positive=True)

    def canonical_object(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id.hex(),
            "table_generation": self.table_generation,
        }


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


def _watermark_object(watermark: FactorInputWatermarkSet) -> dict[str, Any]:
    if not isinstance(watermark, FactorInputWatermarkSet):
        raise SidecarError("watermark must be a FactorInputWatermarkSet")
    return {
        "entries": [_entry_object(entry) for entry in watermark.entries],
        "trade_date": watermark.trade_date,
        "watermark_set_id": watermark.watermark_set_id,
    }


def watermark_full_map_sha256(watermark: FactorInputWatermarkSet) -> bytes:
    """Hash the exact full map, including observation metadata and clock label."""
    return hashlib.sha256(_canonical_json(_watermark_object(watermark))).digest()


@dataclass(frozen=True, slots=True, order=True)
class SidecarReference:
    namespace: SidecarNamespace
    watermark_set_id: int
    input_identity_sha256: bytes
    full_map_sha256: bytes

    def __post_init__(self) -> None:
        if not isinstance(self.namespace, SidecarNamespace):
            raise SidecarError("namespace must be a SidecarNamespace")
        _uint(self.watermark_set_id, 64, "watermark_set_id", positive=True)
        object.__setattr__(
            self,
            "input_identity_sha256",
            _fixed_bytes(
                self.input_identity_sha256, 32, "input_identity_sha256"
            ),
        )
        object.__setattr__(
            self,
            "full_map_sha256",
            _fixed_bytes(self.full_map_sha256, 32, "full_map_sha256"),
        )

    @property
    def sort_key(self) -> tuple[bytes, int, int]:
        return (
            self.namespace.run_id,
            self.namespace.table_generation,
            self.watermark_set_id,
        )

    @classmethod
    def from_watermark(
        cls,
        namespace: SidecarNamespace,
        watermark: FactorInputWatermarkSet,
    ) -> "SidecarReference":
        if not isinstance(watermark, FactorInputWatermarkSet):
            raise SidecarError("watermark must be a FactorInputWatermarkSet")
        return cls(
            namespace=namespace,
            watermark_set_id=watermark.watermark_set_id,
            input_identity_sha256=watermark.input_identity_hash(),
            full_map_sha256=watermark_full_map_sha256(watermark),
        )

    def canonical_object(self) -> dict[str, Any]:
        return {
            "full_map_sha256": self.full_map_sha256.hex(),
            "input_identity_sha256": self.input_identity_sha256.hex(),
            "namespace": self.namespace.canonical_object(),
            "watermark_set_id": self.watermark_set_id,
        }


@dataclass(frozen=True, slots=True)
class WatermarkSidecar:
    namespace: SidecarNamespace
    watermark_sets: tuple[FactorInputWatermarkSet, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.namespace, SidecarNamespace):
            raise SidecarError("namespace must be a SidecarNamespace")
        if type(self.watermark_sets) is not tuple:
            raise SidecarError("watermark_sets must be a tuple")
        if len(self.watermark_sets) > _MAX_SIDECAR_SETS:
            raise SidecarError("watermark sidecar has too many sets")
        if any(
            not isinstance(item, FactorInputWatermarkSet)
            for item in self.watermark_sets
        ):
            raise SidecarError(
                "watermark_sets must contain FactorInputWatermarkSet values"
            )
        ordered = tuple(
            sorted(self.watermark_sets, key=lambda item: item.watermark_set_id)
        )
        previous_id: int | None = None
        for item in ordered:
            if previous_id == item.watermark_set_id:
                raise SidecarConflictError(
                    "one run-local watermark_set_id appears more than once"
                )
            previous_id = item.watermark_set_id
        object.__setattr__(self, "watermark_sets", ordered)

    def canonical_object(self) -> dict[str, Any]:
        return {
            "encoding": "l2flow-watermark-sidecar-v1",
            "namespace": self.namespace.canonical_object(),
            "sets": [
                {
                    "full_map_sha256": watermark_full_map_sha256(item).hex(),
                    "input_identity_sha256": item.input_identity_hex(),
                    "watermark": _watermark_object(item),
                }
                for item in self.watermark_sets
            ],
        }

    def reference_for(self, watermark_set_id: int) -> SidecarReference:
        watermark = self._lookup(watermark_set_id)
        return SidecarReference.from_watermark(self.namespace, watermark)

    def _lookup(self, watermark_set_id: int) -> FactorInputWatermarkSet:
        _uint(watermark_set_id, 64, "watermark_set_id", positive=True)
        low = 0
        high = len(self.watermark_sets)
        while low < high:
            middle = (low + high) // 2
            candidate = self.watermark_sets[middle]
            if candidate.watermark_set_id < watermark_set_id:
                low = middle + 1
            else:
                high = middle
        if (
            low == len(self.watermark_sets)
            or self.watermark_sets[low].watermark_set_id != watermark_set_id
        ):
            raise SidecarReferenceError("watermark reference is orphaned")
        return self.watermark_sets[low]

    def resolve(
        self,
        reference: SidecarReference,
        *,
        expected_full_map: FactorInputWatermarkSet | None = None,
    ) -> FactorInputWatermarkSet:
        if not isinstance(reference, SidecarReference):
            raise SidecarReferenceError("reference must be a SidecarReference")
        if reference.namespace != self.namespace:
            raise SidecarReferenceError("watermark sidecar namespace is orphaned")
        watermark = self._lookup(reference.watermark_set_id)
        if watermark.input_identity_hash() != reference.input_identity_sha256:
            raise SidecarReferenceError("watermark input identity mismatch")
        if watermark_full_map_sha256(watermark) != reference.full_map_sha256:
            raise SidecarReferenceError("watermark full-map identity mismatch")
        if expected_full_map is not None:
            if not isinstance(expected_full_map, FactorInputWatermarkSet):
                raise SidecarReferenceError(
                    "expected_full_map must be a FactorInputWatermarkSet"
                )
            if expected_full_map != watermark:
                raise SidecarReferenceError("resolved watermark full map mismatch")
        return watermark

    def with_appended(
        self,
        watermarks: Iterable[FactorInputWatermarkSet],
    ) -> "WatermarkSidecar":
        if isinstance(watermarks, (str, bytes, bytearray, memoryview)):
            raise SidecarError("appended watermarks must be a typed iterable")
        try:
            iterator = iter(watermarks)
        except TypeError as error:
            raise SidecarError(
                "appended watermarks must be a typed iterable"
            ) from error
        remaining = _MAX_SIDECAR_SETS - len(self.watermark_sets)
        additions: list[FactorInputWatermarkSet] = []
        for item in iterator:
            if len(additions) >= remaining:
                raise SidecarError("appended watermark sets exceed the V1 bound")
            if not isinstance(item, FactorInputWatermarkSet):
                raise SidecarError(
                    "appended value must be a FactorInputWatermarkSet"
                )
            additions.append(item)
        existing = {item.watermark_set_id: item for item in self.watermark_sets}
        for item in additions:
            if item.watermark_set_id in existing:
                raise SidecarConflictError(
                    "append attempted to reuse a run-local watermark_set_id"
                )
            existing[item.watermark_set_id] = item
        return WatermarkSidecar(self.namespace, tuple(existing.values()))


def encode_sidecar(sidecar: WatermarkSidecar) -> bytes:
    if not isinstance(sidecar, WatermarkSidecar):
        raise SidecarError("sidecar must be a WatermarkSidecar")
    payload = _canonical_json(sidecar.canonical_object())
    return _encode_envelope(
        _SIDECAR_MAGIC,
        payload,
        _MAX_SIDECAR_JSON_BYTES,
    )


def _decode_namespace(value: Any) -> SidecarNamespace:
    item = _require_object(value, {"run_id", "table_generation"}, "namespace")
    return SidecarNamespace(
        run_id=_fixed_bytes(item["run_id"], 16, "run_id"),
        table_generation=_uint(
            item["table_generation"], 64, "table_generation", positive=True
        ),
    )


def _decode_watermark(value: Any) -> FactorInputWatermarkSet:
    item = _require_object(
        value,
        {"entries", "trade_date", "watermark_set_id"},
        "watermark",
    )
    entries_value = item["entries"]
    if (
        not isinstance(entries_value, list)
        or not entries_value
        or len(entries_value) > _MAX_WATERMARK_ENTRIES
    ):
        raise SidecarError("watermark entries have an invalid count")
    entry_keys = {
        "canonical_cursor",
        "clock_epoch_algorithm",
        "clock_epoch_digest",
        "clock_epoch_label",
        "family",
        "input_quality_flags",
        "max_consumed_origin_wal_end_pos",
        "observed_raw_durable_wal_pos",
        "origin_capture_date",
        "origin_stream_day_id",
        "shard_id",
        "source_stream_id",
    }
    entries: list[FactorInputWatermark] = []
    previous_key: tuple[int, int, bytes, int, int] | None = None
    try:
        for raw_entry in entries_value:
            raw_entry = _require_object(raw_entry, entry_keys, "watermark entry")
            entry = FactorInputWatermark(
                source_stream_id=_uint(
                    raw_entry["source_stream_id"],
                    32,
                    "source_stream_id",
                    positive=True,
                ),
                origin_capture_date=_uint(
                    raw_entry["origin_capture_date"],
                    32,
                    "origin_capture_date",
                    positive=True,
                ),
                origin_stream_day_id=_fixed_bytes(
                    raw_entry["origin_stream_day_id"],
                    16,
                    "origin_stream_day_id",
                ),
                family=InputFamily(raw_entry["family"]),
                shard_id=_uint(raw_entry["shard_id"], 32, "shard_id"),
                canonical_cursor=_uint(
                    raw_entry["canonical_cursor"], 64, "canonical_cursor"
                ),
                max_consumed_origin_wal_end_pos=_uint(
                    raw_entry["max_consumed_origin_wal_end_pos"],
                    64,
                    "max_consumed_origin_wal_end_pos",
                ),
                observed_raw_durable_wal_pos=_uint(
                    raw_entry["observed_raw_durable_wal_pos"],
                    64,
                    "observed_raw_durable_wal_pos",
                ),
                clock_epoch_algorithm=_uint(
                    raw_entry["clock_epoch_algorithm"],
                    32,
                    "clock_epoch_algorithm",
                    positive=True,
                ),
                clock_epoch_digest=_fixed_bytes(
                    raw_entry["clock_epoch_digest"],
                    32,
                    "clock_epoch_digest",
                ),
                clock_epoch_label=_uint(
                    raw_entry["clock_epoch_label"], 64, "clock_epoch_label"
                ),
                input_quality_flags=_uint(
                    raw_entry["input_quality_flags"], 64, "input_quality_flags"
                ),
            )
            if previous_key is not None and entry.sort_key <= previous_key:
                raise SidecarError(
                    "watermark entries are not in unique canonical key order"
                )
            previous_key = entry.sort_key
            entries.append(entry)
        return FactorInputWatermarkSet(
            watermark_set_id=_uint(
                item["watermark_set_id"],
                64,
                "watermark_set_id",
                positive=True,
            ),
            trade_date=_uint(item["trade_date"], 32, "trade_date", positive=True),
            entries=tuple(entries),
        )
    except (ValidationError, ValueError, TypeError, KeyError) as error:
        if isinstance(error, SidecarError):
            raise
        raise SidecarError("watermark failed strict validation") from error


def decode_sidecar(blob: bytes) -> WatermarkSidecar:
    payload = _decode_envelope(
        blob,
        _SIDECAR_MAGIC,
        _MAX_SIDECAR_JSON_BYTES,
    )
    root = _decode_json(payload, _MAX_SIDECAR_JSON_BYTES)
    root = _require_object(root, {"encoding", "namespace", "sets"}, "sidecar")
    if root["encoding"] != "l2flow-watermark-sidecar-v1":
        raise SidecarError("watermark sidecar encoding mismatch")
    namespace = _decode_namespace(root["namespace"])
    sets_value = root["sets"]
    if not isinstance(sets_value, list) or len(sets_value) > _MAX_SIDECAR_SETS:
        raise SidecarError("watermark sidecar set array is invalid")
    sets: list[FactorInputWatermarkSet] = []
    previous_id: int | None = None
    for value in sets_value:
        value = _require_object(
            value,
            {"full_map_sha256", "input_identity_sha256", "watermark"},
            "sidecar set",
        )
        watermark = _decode_watermark(value["watermark"])
        if previous_id is not None and watermark.watermark_set_id <= previous_id:
            raise SidecarConflictError(
                "sidecar sets are not in unique increasing ID order"
            )
        previous_id = watermark.watermark_set_id
        expected_identity = _fixed_bytes(
            value["input_identity_sha256"], 32, "input_identity_sha256"
        )
        expected_full_map = _fixed_bytes(
            value["full_map_sha256"], 32, "full_map_sha256"
        )
        if watermark.input_identity_hash() != expected_identity:
            raise SidecarConflictError("sidecar input identity does not match its map")
        if watermark_full_map_sha256(watermark) != expected_full_map:
            raise SidecarConflictError("sidecar full-map hash does not match its map")
        sets.append(watermark)
    sidecar = WatermarkSidecar(namespace, tuple(sets))
    if _canonical_json(sidecar.canonical_object()) != payload:
        raise SidecarError("watermark sidecar JSON is not canonical")
    return sidecar


def sidecar_sha256(sidecar: WatermarkSidecar) -> bytes:
    return hashlib.sha256(encode_sidecar(sidecar)).digest()
