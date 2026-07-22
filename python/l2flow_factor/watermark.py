from __future__ import annotations

from dataclasses import dataclass
import hashlib
import struct
from typing import Mapping, Protocol

from .canonical import InputFamily
from .errors import ValidationError


INPUT_IDENTITY_DOMAIN_V1 = b"l2flow.factor.input-identity.v1\x00"


def _uint(value: object, bits: int, name: str, *, positive: bool = False) -> int:
    if type(value) is not int:
        raise ValidationError(f"{name} must be an integer")
    minimum = 1 if positive else 0
    maximum = (1 << bits) - 1
    if value < minimum or value > maximum:
        raise ValidationError(f"{name} is outside uint{bits}")
    return value


def _bytes(value: object, width: int, name: str, *, nonzero: bool = False) -> bytes:
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
    if len(result) != width:
        raise ValidationError(f"{name} must contain exactly {width} bytes")
    if nonzero and not any(result):
        raise ValidationError(f"{name} must not be all zero")
    return result


@dataclass(frozen=True, slots=True, order=True)
class RawNamespace:
    origin_capture_date: int
    source_stream_id: int
    origin_stream_day_id: bytes

    def __post_init__(self) -> None:
        _uint(self.origin_capture_date, 32, "origin_capture_date", positive=True)
        _uint(self.source_stream_id, 32, "source_stream_id", positive=True)
        object.__setattr__(self, "origin_stream_day_id", _bytes(
            self.origin_stream_day_id, 16, "origin_stream_day_id", nonzero=True
        ))


@dataclass(frozen=True, slots=True)
class FactorInputWatermark:
    source_stream_id: int
    origin_capture_date: int
    origin_stream_day_id: bytes
    family: InputFamily
    shard_id: int
    canonical_cursor: int
    max_consumed_origin_wal_end_pos: int
    observed_raw_durable_wal_pos: int
    clock_epoch_algorithm: int
    clock_epoch_digest: bytes
    clock_epoch_label: int
    input_quality_flags: int

    def __post_init__(self) -> None:
        _uint(self.source_stream_id, 32, "source_stream_id", positive=True)
        _uint(self.origin_capture_date, 32, "origin_capture_date", positive=True)
        object.__setattr__(self, "origin_stream_day_id", _bytes(
            self.origin_stream_day_id, 16, "origin_stream_day_id", nonzero=True
        ))
        if not isinstance(self.family, InputFamily):
            raise ValidationError("family must be an InputFamily")
        # The V1 input identity carries CanonicalEventTypeV1 numeric values.
        self.family.canonical_event_type
        _uint(self.shard_id, 32, "shard_id")
        _uint(self.canonical_cursor, 64, "canonical_cursor")
        _uint(
            self.max_consumed_origin_wal_end_pos,
            64,
            "max_consumed_origin_wal_end_pos",
        )
        _uint(self.observed_raw_durable_wal_pos, 64, "observed_raw_durable_wal_pos")
        _uint(self.clock_epoch_algorithm, 32, "clock_epoch_algorithm", positive=True)
        object.__setattr__(self, "clock_epoch_digest", _bytes(
            self.clock_epoch_digest, 32, "clock_epoch_digest", nonzero=True
        ))
        _uint(self.clock_epoch_label, 64, "clock_epoch_label")
        _uint(self.input_quality_flags, 64, "input_quality_flags")
        if self.input_quality_flags & ~0x0000000FFFFFFFFF:
            raise ValidationError("input_quality_flags contains unknown V1 bits")
        if (self.canonical_cursor == 0) != (
            self.max_consumed_origin_wal_end_pos == 0
        ):
            raise ValidationError(
                "zero canonical cursor and zero consumed WAL must agree"
            )

    @property
    def sort_key(self) -> tuple[int, int, bytes, int, int]:
        return (
            self.source_stream_id,
            self.origin_capture_date,
            self.origin_stream_day_id,
            self.family.canonical_event_type,
            self.shard_id,
        )

    @property
    def namespace(self) -> RawNamespace:
        return RawNamespace(
            origin_capture_date=self.origin_capture_date,
            source_stream_id=self.source_stream_id,
            origin_stream_day_id=self.origin_stream_day_id,
        )

    def input_identity_bytes(self) -> bytes:
        return b"".join(
            (
                struct.pack("<I", self.source_stream_id),
                struct.pack("<I", self.origin_capture_date),
                self.origin_stream_day_id,
                struct.pack("<H", self.family.canonical_event_type),
                struct.pack("<I", self.shard_id),
                struct.pack("<Q", self.canonical_cursor),
                struct.pack("<Q", self.max_consumed_origin_wal_end_pos),
                struct.pack("<I", self.clock_epoch_algorithm),
                self.clock_epoch_digest,
                struct.pack("<Q", self.input_quality_flags),
            )
        )


@dataclass(frozen=True, slots=True)
class FactorInputWatermarkSet:
    watermark_set_id: int
    trade_date: int
    entries: tuple[FactorInputWatermark, ...]

    def __post_init__(self) -> None:
        _uint(self.watermark_set_id, 64, "watermark_set_id", positive=True)
        _uint(self.trade_date, 32, "trade_date", positive=True)
        if type(self.entries) is not tuple or not self.entries or len(self.entries) > 65_536:
            raise ValidationError("entries must contain between 1 and 65,536 values")
        if any(not isinstance(entry, FactorInputWatermark) for entry in self.entries):
            raise ValidationError("entries must contain FactorInputWatermark values")
        ordered = tuple(sorted(self.entries, key=lambda entry: entry.sort_key))
        keys = [entry.sort_key for entry in ordered]
        if len(keys) != len(set(keys)):
            raise ValidationError("watermark entries must not contain duplicate input keys")
        object.__setattr__(self, "entries", ordered)

    def input_identity_preimage(self) -> bytes:
        return b"".join(
            (
                INPUT_IDENTITY_DOMAIN_V1,
                struct.pack("<I", self.trade_date),
                struct.pack("<I", len(self.entries)),
                *(entry.input_identity_bytes() for entry in self.entries),
            )
        )

    def input_identity_hash(self) -> bytes:
        return hashlib.sha256(self.input_identity_preimage()).digest()

    def input_identity_hex(self) -> str:
        return self.input_identity_hash().hex()


@dataclass(frozen=True, slots=True)
class DurabilityFailure:
    namespace: RawNamespace
    required_wal_pos: int
    current_durable_wal_pos: int | None


@dataclass(frozen=True, slots=True)
class DurabilityBarrierResult:
    satisfied: bool
    failures: tuple[DurabilityFailure, ...]


class DurablePositionProvider(Protocol):
    def __call__(self, namespace: RawNamespace) -> int | None:
        ...


def check_durability_barrier(
    watermark_set: FactorInputWatermarkSet,
    current_positions: Mapping[RawNamespace, int] | DurablePositionProvider,
) -> DurabilityBarrierResult:
    if not isinstance(watermark_set, FactorInputWatermarkSet):
        raise ValidationError("watermark_set must be a FactorInputWatermarkSet")
    failures: list[DurabilityFailure] = []
    for entry in watermark_set.entries:
        if callable(current_positions):
            observed = current_positions(entry.namespace)
        else:
            observed = current_positions.get(entry.namespace)
        if observed is not None:
            _uint(observed, 64, "current durable WAL position")
        if observed is None or entry.max_consumed_origin_wal_end_pos > observed:
            failures.append(
                DurabilityFailure(
                    namespace=entry.namespace,
                    required_wal_pos=entry.max_consumed_origin_wal_end_pos,
                    current_durable_wal_pos=observed,
                )
            )
    return DurabilityBarrierResult(satisfied=not failures, failures=tuple(failures))


WatermarkSet = FactorInputWatermarkSet
WatermarkEntry = FactorInputWatermark
