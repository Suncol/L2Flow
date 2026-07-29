"""Verified restart boundaries for instrument-local tick deltas.

A checkpoint is deliberately more than a generation number.  It binds the
Store cut to the service session, trading day, registry, source endpoints,
and the exact per-instrument tick counts that were reconciled at EOF.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Tuple

from .models import SessionIdentity, StaleSessionError


_UINT32_MAX = 0xFFFFFFFF
_UINT64_MAX = 0xFFFFFFFFFFFFFFFF


def _uint(
    value: object,
    field: str,
    *,
    maximum: int = _UINT64_MAX,
    positive: bool = False,
) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{field} must be an integer")
    minimum = 1 if positive else 0
    if value < minimum or value > maximum:
        qualifier = "positive " if positive else ""
        bits = 32 if maximum == _UINT32_MAX else 64
        raise ValueError(f"{field} must fit {qualifier}uint{bits}")
    return value


def _fixed_bytes(
    value: object,
    field: str,
    size: int,
    *,
    nonzero: bool,
) -> bytes:
    if not isinstance(value, bytes):
        raise TypeError(f"{field} must be bytes")
    if len(value) != size:
        raise ValueError(f"{field} must contain exactly {size} bytes")
    if nonzero and not any(value):
        raise ValueError(f"{field} must be nonzero")
    return value


def _quad(
    value: object,
    field: str,
    *,
    maximum: int = _UINT64_MAX,
    positive: bool = False,
) -> Tuple[int, int, int, int]:
    if isinstance(value, (str, bytes, bytearray)):
        raise TypeError(f"{field} must contain four integers")
    try:
        result = tuple(value)  # type: ignore[arg-type]
    except TypeError as error:
        raise TypeError(f"{field} must contain four integers") from error
    if len(result) != 4:
        raise ValueError(f"{field} must contain exactly four integers")
    return tuple(
        _uint(
            item,
            f"{field}[{index}]",
            maximum=maximum,
            positive=positive,
        )
        for index, item in enumerate(result)
    )  # type: ignore[return-value]


@dataclass(frozen=True, slots=True)
class InstrumentTickDeltaCheckpoint:
    """One EOF-verified, generation-exclusive instrument tick boundary."""

    run_id: bytes
    session_epoch: int
    trade_date: int
    instrument_count: int
    registry_version: int
    registry_sha256: bytes
    instrument_id: int
    registry_ordinal: int
    generation: int
    input_identity_sha256: bytes
    ingress_sequence_exclusive: int
    tick_stream_sequence_exclusive: int
    recv_monotonic_cut_ns: int
    source_stream_ids: Tuple[int, int, int, int]
    source_sequence_exclusive: Tuple[int, int, int, int]
    instrument_tick_counts: Tuple[int, int, int, int]
    coverage_from_open: bool
    record_coverage_complete: bool
    field_complete: bool
    payload_projection: int

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "run_id",
            _fixed_bytes(
                self.run_id, "run_id", 16, nonzero=True
            ),
        )
        _uint(self.session_epoch, "session_epoch", positive=True)
        _uint(
            self.trade_date,
            "trade_date",
            maximum=_UINT32_MAX,
            positive=True,
        )
        _uint(
            self.instrument_count,
            "instrument_count",
            maximum=_UINT32_MAX,
            positive=True,
        )
        _uint(self.registry_version, "registry_version", positive=True)
        object.__setattr__(
            self,
            "registry_sha256",
            _fixed_bytes(
                self.registry_sha256,
                "registry_sha256",
                32,
                nonzero=True,
            ),
        )
        _uint(
            self.instrument_id,
            "instrument_id",
            maximum=_UINT32_MAX,
            positive=True,
        )
        _uint(
            self.registry_ordinal,
            "registry_ordinal",
            maximum=_UINT32_MAX,
        )
        if self.registry_ordinal >= self.instrument_count:
            raise ValueError(
                "registry_ordinal must be smaller than instrument_count"
            )
        _uint(self.generation, "generation", positive=True)
        object.__setattr__(
            self,
            "input_identity_sha256",
            _fixed_bytes(
                self.input_identity_sha256,
                "input_identity_sha256",
                32,
                nonzero=True,
            ),
        )
        _uint(
            self.ingress_sequence_exclusive,
            "ingress_sequence_exclusive",
            positive=True,
        )
        _uint(
            self.tick_stream_sequence_exclusive,
            "tick_stream_sequence_exclusive",
            positive=True,
        )
        _uint(
            self.recv_monotonic_cut_ns,
            "recv_monotonic_cut_ns",
        )
        source_stream_ids = _quad(
            self.source_stream_ids,
            "source_stream_ids",
            maximum=_UINT32_MAX,
            positive=True,
        )
        if len(set(source_stream_ids)) != 4:
            raise ValueError("source_stream_ids must be distinct")
        object.__setattr__(
            self, "source_stream_ids", source_stream_ids
        )
        source_sequence_exclusive = _quad(
            self.source_sequence_exclusive,
            "source_sequence_exclusive",
            positive=True,
        )
        ingress_prefix = sum(
            endpoint - 1 for endpoint in source_sequence_exclusive
        )
        if (
            ingress_prefix > _UINT64_MAX
            or self.ingress_sequence_exclusive - 1 != ingress_prefix
        ):
            raise ValueError(
                "source endpoints do not reconcile with the ingress cut"
            )
        tick_endpoint = (
            1
            + source_sequence_exclusive[1]
            - 1
            + source_sequence_exclusive[3]
            - 1
        )
        if (
            tick_endpoint > _UINT64_MAX
            or self.tick_stream_sequence_exclusive != tick_endpoint
        ):
            raise ValueError(
                "tick source endpoints do not reconcile with the "
                "tick-stream cut"
            )
        object.__setattr__(
            self,
            "source_sequence_exclusive",
            source_sequence_exclusive,
        )
        instrument_tick_counts = _quad(
            self.instrument_tick_counts,
            "instrument_tick_counts",
        )
        if (
            instrument_tick_counts[0] != 0
            or instrument_tick_counts[2] != 0
        ):
            raise ValueError(
                "instrument_tick_counts must be zero for snapshot "
                "source slots 0 and 2"
            )
        for slot in (1, 3):
            if (
                instrument_tick_counts[slot]
                > source_sequence_exclusive[slot] - 1
            ):
                raise ValueError(
                    "instrument tick count exceeds its source endpoint"
                )
        if (
            sum(instrument_tick_counts)
            > self.tick_stream_sequence_exclusive - 1
        ):
            raise ValueError(
                "instrument tick count exceeds the global tick endpoint"
            )
        object.__setattr__(
            self, "instrument_tick_counts", instrument_tick_counts
        )
        for field in (
            "coverage_from_open",
            "record_coverage_complete",
            "field_complete",
        ):
            if not isinstance(getattr(self, field), bool):
                raise TypeError(f"{field} must be bool")
        if not self.record_coverage_complete:
            raise ValueError(
                "a verified checkpoint requires complete record coverage"
            )
        if self.field_complete:
            raise ValueError(
                "the instrument tick projection is not field-complete"
            )
        projection = _uint(
            self.payload_projection,
            "payload_projection",
            maximum=_UINT32_MAX,
            positive=True,
        )
        if projection != 1:
            raise ValueError(
                "unsupported instrument tick payload projection"
            )

    @property
    def session_identity(self) -> SessionIdentity:
        return SessionIdentity(self.run_id, self.session_epoch)

    @property
    def total_instrument_tick_count(self) -> int:
        return sum(self.instrument_tick_counts)

    @property
    def instrument_tick_source_record_counts(
        self,
    ) -> Tuple[int, int, int, int]:
        return self.instrument_tick_counts

    @property
    def instrument_tick_record_count(self) -> int:
        return self.total_instrument_tick_count

    @property
    def flags(self) -> int:
        return (
            (1 if self.coverage_from_open else 0)
            | (2 if self.record_coverage_complete else 0)
            | (4 if self.field_complete else 0)
        )

    def ensure_session(
        self,
        *,
        run_id: bytes,
        session_epoch: int,
        trade_date: int,
        instrument_count: int,
        registry_version: int,
        registry_sha256: bytes,
    ) -> None:
        """Raise when this checkpoint belongs to another anchored session."""

        if (
            self.run_id != run_id
            or self.session_epoch != session_epoch
            or self.trade_date != trade_date
            or self.instrument_count != instrument_count
            or self.registry_version != registry_version
            or self.registry_sha256 != registry_sha256
        ):
            raise StaleSessionError(
                "instrument checkpoint belongs to another "
                "session/day/registry"
            )

    def ensure_successor_of(
        self, base: "InstrumentTickDeltaCheckpoint"
    ) -> None:
        """Validate monotone half-open boundaries from ``base`` to ``self``."""

        if not isinstance(base, InstrumentTickDeltaCheckpoint):
            raise TypeError(
                "base must be an InstrumentTickDeltaCheckpoint"
            )
        base.ensure_session(
            run_id=self.run_id,
            session_epoch=self.session_epoch,
            trade_date=self.trade_date,
            instrument_count=self.instrument_count,
            registry_version=self.registry_version,
            registry_sha256=self.registry_sha256,
        )
        if (
            self.instrument_id != base.instrument_id
            or self.registry_ordinal != base.registry_ordinal
            or self.instrument_count != base.instrument_count
        ):
            raise StaleSessionError(
                "instrument checkpoint identity changed"
            )
        if self.source_stream_ids != base.source_stream_ids:
            raise StaleSessionError(
                "instrument checkpoint source identities changed"
            )
        if (
            self.payload_projection != base.payload_projection
            or self.coverage_from_open != base.coverage_from_open
            or self.record_coverage_complete
            != base.record_coverage_complete
            or self.field_complete != base.field_complete
        ):
            raise StaleSessionError(
                "instrument checkpoint coverage/projection changed"
            )
        monotone = (
            self.generation >= base.generation
            and self.ingress_sequence_exclusive
            >= base.ingress_sequence_exclusive
            and self.tick_stream_sequence_exclusive
            >= base.tick_stream_sequence_exclusive
            and self.recv_monotonic_cut_ns
            >= base.recv_monotonic_cut_ns
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
                    self.instrument_tick_counts,
                    base.instrument_tick_counts,
                )
            )
        )
        if not monotone:
            raise StaleSessionError(
                "instrument checkpoint boundaries moved backwards"
            )
        source_deltas = tuple(
            target - origin
            for target, origin in zip(
                self.source_sequence_exclusive,
                base.source_sequence_exclusive,
            )
        )
        if (
            self.ingress_sequence_exclusive
            - base.ingress_sequence_exclusive
            != sum(source_deltas)
        ):
            raise StaleSessionError(
                "instrument checkpoint ingress delta does not reconcile"
            )
        if (
            self.tick_stream_sequence_exclusive
            - base.tick_stream_sequence_exclusive
            != source_deltas[1] + source_deltas[3]
        ):
            raise StaleSessionError(
                "instrument checkpoint tick delta does not reconcile"
            )
        local_deltas = tuple(
            target - origin
            for target, origin in zip(
                self.instrument_tick_counts,
                base.instrument_tick_counts,
            )
        )
        if any(
            local_deltas[slot] > source_deltas[slot]
            for slot in (1, 3)
        ):
            raise StaleSessionError(
                "instrument-local count exceeds its source delta"
            )
        ingress_delta = (
            self.ingress_sequence_exclusive
            - base.ingress_sequence_exclusive
        )
        tick_delta = (
            self.tick_stream_sequence_exclusive
            - base.tick_stream_sequence_exclusive
        )
        if sum(local_deltas) > tick_delta or tick_delta > ingress_delta:
            raise StaleSessionError(
                "instrument/tick/ingress successor deltas do not nest"
            )
        if self.generation == base.generation and self != base:
            raise StaleSessionError(
                "one Store generation has conflicting checkpoint metadata"
            )

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation without weakening bytes."""

        return {
            "run_id": self.run_id.hex(),
            "session_epoch": self.session_epoch,
            "trade_date": self.trade_date,
            "instrument_count": self.instrument_count,
            "registry_version": self.registry_version,
            "registry_sha256": self.registry_sha256.hex(),
            "instrument_id": self.instrument_id,
            "registry_ordinal": self.registry_ordinal,
            "generation": self.generation,
            "input_identity_sha256": (
                self.input_identity_sha256.hex()
            ),
            "ingress_sequence_exclusive": (
                self.ingress_sequence_exclusive
            ),
            "tick_stream_sequence_exclusive": (
                self.tick_stream_sequence_exclusive
            ),
            "recv_monotonic_cut_ns": self.recv_monotonic_cut_ns,
            "source_stream_ids": list(self.source_stream_ids),
            "source_sequence_exclusive": list(
                self.source_sequence_exclusive
            ),
            "instrument_tick_counts": list(
                self.instrument_tick_counts
            ),
            "coverage_from_open": self.coverage_from_open,
            "record_coverage_complete": (
                self.record_coverage_complete
            ),
            "field_complete": self.field_complete,
            "payload_projection": self.payload_projection,
        }

    @classmethod
    def from_dict(
        cls, value: Mapping[str, Any]
    ) -> "InstrumentTickDeltaCheckpoint":
        """Restore the strict representation produced by :meth:`to_dict`."""

        if not isinstance(value, Mapping):
            raise TypeError("checkpoint representation must be a mapping")
        expected = {
            "run_id",
            "session_epoch",
            "trade_date",
            "instrument_count",
            "registry_version",
            "registry_sha256",
            "instrument_id",
            "registry_ordinal",
            "generation",
            "input_identity_sha256",
            "ingress_sequence_exclusive",
            "tick_stream_sequence_exclusive",
            "recv_monotonic_cut_ns",
            "source_stream_ids",
            "source_sequence_exclusive",
            "instrument_tick_counts",
            "coverage_from_open",
            "record_coverage_complete",
            "field_complete",
            "payload_projection",
        }
        if set(value) != expected:
            raise ValueError(
                "checkpoint representation has missing or unknown fields"
            )
        encoded_fields = (
            ("run_id", 16),
            ("registry_sha256", 32),
            ("input_identity_sha256", 32),
        )
        decoded: dict[str, Any] = dict(value)
        for field, size in encoded_fields:
            encoded = decoded[field]
            if not isinstance(encoded, str):
                raise TypeError(f"{field} must be a hexadecimal string")
            try:
                raw = bytes.fromhex(encoded)
            except ValueError as error:
                raise ValueError(
                    f"{field} must be valid hexadecimal"
                ) from error
            if len(raw) != size:
                raise ValueError(
                    f"{field} must decode to exactly {size} bytes"
                )
            decoded[field] = raw
        return cls(**decoded)


__all__ = ["InstrumentTickDeltaCheckpoint"]
