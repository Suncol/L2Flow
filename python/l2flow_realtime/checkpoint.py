"""EOF-verified checkpoints for Wire V2 instrument tick deltas."""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import Any, Mapping

from ._generation import (
    ENDPOINT_FLAG_COVERAGE_FROM_OPEN,
    ENDPOINT_FLAG_RECORD_COVERAGE_COMPLETE,
    GenerationEndpoint,
    pack_generation_endpoint,
    parse_generation_endpoint,
    validate_generation_endpoint,
)
from .models import (
    CatalogScope,
    SessionIdentity,
    StaleSessionError,
    WireFormatError,
)


CHECKPOINT_BYTES = 312
TICK_RECORD_COVERAGE_COMPLETE = 1 << 0
PAYLOAD_PROJECTION_CORE_V2 = 1

_CHECKPOINT_LOCAL = struct.Struct("<II4QQII8s")
assert _CHECKPOINT_LOCAL.size == 64


@dataclass(frozen=True, slots=True)
class InstrumentTickDeltaCheckpoint:
    """One explicit-EOF-verified, generation-exclusive tick boundary."""

    run_id: bytes
    session_epoch: int
    generation: int
    catalog_generation: int
    data_state_generation: int
    ingress_sequence_exclusive: int
    tick_stream_sequence_exclusive: int
    recv_monotonic_cut_ns: int
    history_published_monotonic_ns: int
    accepted_sequence: int
    applied_sequence: int
    catalog_digest: bytes
    input_identity_sha256: bytes
    source_stream_ids: tuple[int, int, int, int]
    source_sequence_exclusive: tuple[int, int, int, int]
    trade_date: int
    capacity: int
    bound_count: int
    available_count: int
    snapshot_available_count: int
    tick_available_count: int
    factor_eligible_count: int
    catalog_scope: CatalogScope
    coverage_complete: bool
    coverage_from_open: bool
    record_coverage_complete: bool
    instrument_id: int
    ordinal: int
    instrument_tick_source_record_counts: tuple[int, int, int, int]
    payload_projection: int
    tick_record_coverage_complete: bool

    def __post_init__(self) -> None:
        for name in (
            "coverage_complete",
            "coverage_from_open",
            "record_coverage_complete",
            "tick_record_coverage_complete",
        ):
            if not isinstance(getattr(self, name), bool):
                raise TypeError(f"{name} must be bool")
        if (
            not isinstance(self.payload_projection, int)
            or isinstance(self.payload_projection, bool)
        ):
            raise TypeError("payload_projection must be an integer")
        if not isinstance(self.catalog_scope, CatalogScope):
            raise TypeError("catalog_scope must be CatalogScope")
        source_ids = _quad(
            self.source_stream_ids,
            "source_stream_ids",
            maximum=0xFFFFFFFF,
            positive=True,
        )
        if len(set(source_ids)) != 4:
            raise ValueError(
                "source_stream_ids must be four distinct IDs"
            )
        source_endpoints = _quad(
            self.source_sequence_exclusive,
            "source_sequence_exclusive",
            positive=True,
        )
        object.__setattr__(self, "source_stream_ids", source_ids)
        object.__setattr__(
            self, "source_sequence_exclusive", source_endpoints
        )
        endpoint = self.endpoint
        validate_generation_endpoint(endpoint)
        if (
            not isinstance(self.instrument_id, int)
            or isinstance(self.instrument_id, bool)
        ):
            raise TypeError("instrument_id must be an integer")
        if (
            not isinstance(self.ordinal, int)
            or isinstance(self.ordinal, bool)
        ):
            raise TypeError("ordinal must be an integer")
        if (
            self.instrument_id <= 0
            or self.instrument_id > 0xFFFFFFFF
            or self.ordinal < 0
            or self.ordinal > 0xFFFFFFFF
            or self.ordinal != self.instrument_id - 1
            or self.instrument_id > self.bound_count
        ):
            raise ValueError(
                "instrument identity is outside the daily catalog"
            )
        counts = _quad(
            self.instrument_tick_source_record_counts,
            "instrument_tick_source_record_counts",
        )
        if counts[0] or counts[2]:
            raise ValueError(
                "snapshot source slots cannot contain tick counts"
            )
        for slot in (1, 3):
            if (
                counts[slot]
                > self.source_sequence_exclusive[slot] - 1
            ):
                raise ValueError(
                    "instrument tick count exceeds source endpoint"
                )
        if sum(counts) > self.tick_stream_sequence_exclusive - 1:
            raise ValueError(
                "instrument tick count exceeds global tick endpoint"
            )
        if self.payload_projection != PAYLOAD_PROJECTION_CORE_V2:
            raise ValueError("unsupported tick delta projection")
        if self.tick_record_coverage_complete is not True:
            raise ValueError(
                "verified checkpoint lacks complete tick record coverage"
            )
        object.__setattr__(self, "catalog_scope", endpoint.catalog_scope)
        object.__setattr__(
            self, "source_stream_ids", endpoint.source_stream_ids
        )
        object.__setattr__(
            self,
            "source_sequence_exclusive",
            endpoint.source_sequence_exclusive,
        )
        object.__setattr__(
            self,
            "instrument_tick_source_record_counts",
            counts,
        )

    @property
    def endpoint(self) -> GenerationEndpoint:
        flags = (
            ENDPOINT_FLAG_COVERAGE_FROM_OPEN
            if self.coverage_from_open
            else 0
        ) | (
            ENDPOINT_FLAG_RECORD_COVERAGE_COMPLETE
            if self.record_coverage_complete
            else 0
        )
        return GenerationEndpoint(
            run_id=self.run_id,
            session_epoch=self.session_epoch,
            generation=self.generation,
            catalog_generation=self.catalog_generation,
            data_state_generation=self.data_state_generation,
            ingress_sequence_exclusive=self.ingress_sequence_exclusive,
            tick_stream_sequence_exclusive=(
                self.tick_stream_sequence_exclusive
            ),
            recv_monotonic_cut_ns=self.recv_monotonic_cut_ns,
            history_published_monotonic_ns=(
                self.history_published_monotonic_ns
            ),
            accepted_sequence=self.accepted_sequence,
            applied_sequence=self.applied_sequence,
            catalog_digest=self.catalog_digest,
            input_identity_sha256=self.input_identity_sha256,
            source_stream_ids=self.source_stream_ids,
            source_sequence_exclusive=(
                self.source_sequence_exclusive
            ),
            trade_date=self.trade_date,
            capacity=self.capacity,
            bound_count=self.bound_count,
            available_count=self.available_count,
            snapshot_available_count=self.snapshot_available_count,
            tick_available_count=self.tick_available_count,
            factor_eligible_count=self.factor_eligible_count,
            catalog_scope=CatalogScope(self.catalog_scope),
            coverage_complete=self.coverage_complete,
            flags=flags,
        )

    @property
    def session_identity(self) -> SessionIdentity:
        return SessionIdentity(self.run_id, self.session_epoch)

    @property
    def instrument_tick_record_count(self) -> int:
        return sum(self.instrument_tick_source_record_counts)

    def ensure_session(
        self,
        *,
        run_id: bytes,
        session_epoch: int,
        trade_date: int,
        capacity: int,
    ) -> None:
        if (
            self.run_id != run_id
            or self.session_epoch != session_epoch
            or self.trade_date != trade_date
            or self.capacity != capacity
        ):
            raise StaleSessionError(
                "tick checkpoint belongs to another session/day/layout"
            )

    def ensure_successor_of(
        self, base: "InstrumentTickDeltaCheckpoint"
    ) -> None:
        if not isinstance(base, InstrumentTickDeltaCheckpoint):
            raise TypeError(
                "base must be an InstrumentTickDeltaCheckpoint"
            )
        base.ensure_session(
            run_id=self.run_id,
            session_epoch=self.session_epoch,
            trade_date=self.trade_date,
            capacity=self.capacity,
        )
        if (
            self.instrument_id != base.instrument_id
            or self.ordinal != base.ordinal
            or self.source_stream_ids != base.source_stream_ids
            or self.payload_projection != base.payload_projection
            or self.coverage_from_open != base.coverage_from_open
            or self.record_coverage_complete
            != base.record_coverage_complete
            or self.tick_record_coverage_complete
            != base.tick_record_coverage_complete
        ):
            raise StaleSessionError(
                "tick checkpoint static identity changed"
            )
        # The daily catalog is frozen for the whole session. Any identity
        # movement invalidates a rolling cursor instead of treating growth as
        # a normal successor.
        if (
            self.catalog_generation != base.catalog_generation
            or self.bound_count != base.bound_count
            or self.capacity != base.capacity
            or self.catalog_digest != base.catalog_digest
            or self.available_count < base.available_count
            or self.snapshot_available_count
            < base.snapshot_available_count
            or self.tick_available_count < base.tick_available_count
        ):
            raise StaleSessionError(
                "daily catalog checkpoint changed or data state moved "
                "backwards"
            )
        monotone = (
            self.generation >= base.generation
            and self.data_state_generation
            >= base.data_state_generation
            and self.accepted_sequence >= base.accepted_sequence
            and self.applied_sequence >= base.applied_sequence
            and self.ingress_sequence_exclusive
            >= base.ingress_sequence_exclusive
            and self.tick_stream_sequence_exclusive
            >= base.tick_stream_sequence_exclusive
            and self.recv_monotonic_cut_ns
            >= base.recv_monotonic_cut_ns
            and self.history_published_monotonic_ns
            >= base.history_published_monotonic_ns
            and all(
                target >= origin
                for target, origin in zip(
                    self.source_sequence_exclusive,
                    base.source_sequence_exclusive,
                )
            )
            and all(
                target >= origin
                for target, origin in zip(
                    self.instrument_tick_source_record_counts,
                    base.instrument_tick_source_record_counts,
                )
            )
        )
        if not monotone:
            raise StaleSessionError("tick checkpoint moved backwards")
        if self.generation == base.generation and self != base:
            raise StaleSessionError(
                "one generation has conflicting checkpoint metadata"
            )
        local_deltas = tuple(
            target - origin
            for target, origin in zip(
                self.instrument_tick_source_record_counts,
                base.instrument_tick_source_record_counts,
            )
        )
        source_deltas = tuple(
            target - origin
            for target, origin in zip(
                self.source_sequence_exclusive,
                base.source_sequence_exclusive,
            )
        )
        if any(
            local_deltas[slot] > source_deltas[slot]
            for slot in (1, 3)
        ):
            raise StaleSessionError(
                "local tick count exceeds its source delta"
            )

    def to_wire(self) -> bytes:
        return pack_generation_endpoint(self.endpoint) + _CHECKPOINT_LOCAL.pack(
            self.instrument_id,
            self.ordinal,
            *self.instrument_tick_source_record_counts,
            self.instrument_tick_record_count,
            self.payload_projection,
            TICK_RECORD_COVERAGE_COMPLETE,
            b"\x00" * 8,
        )

    @classmethod
    def from_wire(
        cls, value: bytes | memoryview, offset: int = 0
    ) -> "InstrumentTickDeltaCheckpoint":
        try:
            if not isinstance(value, (bytes, memoryview)):
                raise TypeError(
                    "tick checkpoint wire value must be bytes-like"
                )
            if (
                not isinstance(offset, int)
                or isinstance(offset, bool)
                or offset < 0
            ):
                raise ValueError(
                    "tick checkpoint offset must be nonnegative"
                )
            if len(value) < offset + CHECKPOINT_BYTES:
                raise ValueError(
                    "tick checkpoint wire value is truncated"
                )
            endpoint = parse_generation_endpoint(value, offset)
            fields = _CHECKPOINT_LOCAL.unpack_from(
                value, offset + 248
            )
            instrument_id, ordinal = fields[:2]
            counts = tuple(fields[2:6])
            record_count, projection, flags, reserved = fields[6:]
            if record_count != sum(counts):
                raise ValueError(
                    "tick checkpoint count does not reconcile"
                )
            if (
                flags != TICK_RECORD_COVERAGE_COMPLETE
                or any(reserved)
            ):
                raise ValueError(
                    "tick checkpoint flags/reserved bytes are invalid"
                )
            return cls.from_endpoint(
                endpoint,
                instrument_id=instrument_id,
                ordinal=ordinal,
                instrument_tick_source_record_counts=counts,  # type: ignore[arg-type]
                payload_projection=projection,
                tick_record_coverage_complete=True,
            )
        except WireFormatError:
            raise
        except (TypeError, ValueError, struct.error) as error:
            raise WireFormatError(
                f"invalid tick checkpoint wire value: {error}"
            ) from error

    @classmethod
    def from_endpoint(
        cls,
        endpoint: GenerationEndpoint,
        *,
        instrument_id: int,
        ordinal: int,
        instrument_tick_source_record_counts: tuple[
            int, int, int, int
        ],
        payload_projection: int,
        tick_record_coverage_complete: bool,
    ) -> "InstrumentTickDeltaCheckpoint":
        return cls(
            run_id=endpoint.run_id,
            session_epoch=endpoint.session_epoch,
            generation=endpoint.generation,
            catalog_generation=endpoint.catalog_generation,
            data_state_generation=endpoint.data_state_generation,
            ingress_sequence_exclusive=(
                endpoint.ingress_sequence_exclusive
            ),
            tick_stream_sequence_exclusive=(
                endpoint.tick_stream_sequence_exclusive
            ),
            recv_monotonic_cut_ns=endpoint.recv_monotonic_cut_ns,
            history_published_monotonic_ns=(
                endpoint.history_published_monotonic_ns
            ),
            accepted_sequence=endpoint.accepted_sequence,
            applied_sequence=endpoint.applied_sequence,
            catalog_digest=endpoint.catalog_digest,
            input_identity_sha256=endpoint.input_identity_sha256,
            source_stream_ids=endpoint.source_stream_ids,
            source_sequence_exclusive=(
                endpoint.source_sequence_exclusive
            ),
            trade_date=endpoint.trade_date,
            capacity=endpoint.capacity,
            bound_count=endpoint.bound_count,
            available_count=endpoint.available_count,
            snapshot_available_count=(
                endpoint.snapshot_available_count
            ),
            tick_available_count=endpoint.tick_available_count,
            factor_eligible_count=endpoint.factor_eligible_count,
            catalog_scope=endpoint.catalog_scope,
            coverage_complete=endpoint.coverage_complete,
            coverage_from_open=endpoint.coverage_from_open,
            record_coverage_complete=(
                endpoint.record_coverage_complete
            ),
            instrument_id=instrument_id,
            ordinal=ordinal,
            instrument_tick_source_record_counts=(
                instrument_tick_source_record_counts
            ),
            payload_projection=payload_projection,
            tick_record_coverage_complete=(
                tick_record_coverage_complete
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for name in self.__dataclass_fields__:
            item = getattr(self, name)
            if isinstance(item, bytes):
                result[name] = item.hex()
            elif isinstance(item, CatalogScope):
                result[name] = int(item)
            elif isinstance(item, tuple):
                result[name] = list(item)
            else:
                result[name] = item
        return result

    @classmethod
    def from_dict(
        cls, value: Mapping[str, Any]
    ) -> "InstrumentTickDeltaCheckpoint":
        if not isinstance(value, Mapping):
            raise TypeError("checkpoint representation must be a mapping")
        expected = set(cls.__dataclass_fields__)
        if set(value) != expected:
            raise ValueError("checkpoint representation fields differ")
        restored = dict(value)
        for name in (
            "run_id",
            "catalog_digest",
            "input_identity_sha256",
        ):
            encoded = restored[name]
            if not isinstance(encoded, str):
                raise TypeError(f"{name} must be a hexadecimal string")
            try:
                decoded = bytes.fromhex(encoded)
            except ValueError as error:
                raise ValueError(
                    f"{name} is not canonical hexadecimal"
                ) from error
            if decoded.hex() != encoded:
                raise ValueError(
                    f"{name} is not canonical hexadecimal"
                )
            restored[name] = decoded
        for name in (
            "source_stream_ids",
            "source_sequence_exclusive",
            "instrument_tick_source_record_counts",
        ):
            restored[name] = tuple(restored[name])
        if (
            not isinstance(restored["catalog_scope"], int)
            or isinstance(restored["catalog_scope"], bool)
        ):
            raise TypeError("catalog_scope must be an integer")
        try:
            restored["catalog_scope"] = CatalogScope(
                restored["catalog_scope"]
            )
        except (TypeError, ValueError) as error:
            raise ValueError(
                "catalog_scope is not DECLARED_DAILY_A_SHARE"
            ) from error
        return cls(**restored)


def _quad(
    value: object,
    field: str,
    *,
    maximum: int = (1 << 64) - 1,
    positive: bool = False,
) -> tuple[int, int, int, int]:
    if not isinstance(value, tuple):
        raise TypeError(f"{field} must contain four integers")
    result = value
    if len(result) != 4:
        raise ValueError(f"{field} must contain four integers")
    minimum = 1 if positive else 0
    if any(
        not isinstance(item, int)
        or isinstance(item, bool)
        or item < minimum
        or item > maximum
        for item in result
    ):
        raise ValueError(f"{field} contains an invalid unsigned value")
    return result  # type: ignore[return-value]


__all__ = ["InstrumentTickDeltaCheckpoint"]
