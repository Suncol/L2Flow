"""Exact parsing for the shared Wire V2 immutable-generation endpoint."""

from __future__ import annotations

import struct
from dataclasses import dataclass

from .models import CatalogScope, SessionIdentity, WireFormatError


ENDPOINT_BYTES = 248
ENDPOINT_FLAG_COVERAGE_FROM_OPEN = 1 << 0
ENDPOINT_FLAG_RECORD_COVERAGE_COMPLETE = 1 << 1
ENDPOINT_FLAGS_MASK = (
    ENDPOINT_FLAG_COVERAGE_FROM_OPEN
    | ENDPOINT_FLAG_RECORD_COVERAGE_COMPLETE
)

_ENDPOINT = struct.Struct("<16s10Q32s32s4I4Q10I")
assert _ENDPOINT.size == ENDPOINT_BYTES


def _fail(condition: bool, message: str) -> None:
    if condition:
        raise WireFormatError(message)


def _exact_bytes(
    value: object, expected_bytes: int, field: str
) -> bytes:
    if not isinstance(value, bytes) or len(value) != expected_bytes:
        raise WireFormatError(
            f"{field} is not exact {expected_bytes}-byte data"
        )
    return value


def _uint(
    value: object,
    bits: int,
    field: str,
    *,
    nonzero: bool = False,
) -> int:
    minimum = 1 if nonzero else 0
    maximum = (1 << bits) - 1
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or value < minimum
        or value > maximum
    ):
        qualifier = "nonzero " if nonzero else ""
        raise WireFormatError(
            f"{field} is not a {qualifier}uint{bits}"
        )
    return value


def _uint_quad(
    value: object,
    bits: int,
    field: str,
    *,
    nonzero: bool = False,
) -> tuple[int, int, int, int]:
    if not isinstance(value, tuple) or len(value) != 4:
        raise WireFormatError(
            f"{field} is not an exact four-item tuple"
        )
    for index, item in enumerate(value):
        _uint(
            item,
            bits,
            f"{field}[{index}]",
            nonzero=nonzero,
        )
    return value


@dataclass(frozen=True, slots=True)
class GenerationEndpoint:
    """One immutable, observed-universe Store generation endpoint."""

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
    flags: int

    def __post_init__(self) -> None:
        validate_generation_endpoint(self)

    @property
    def session_identity(self) -> SessionIdentity:
        return SessionIdentity(self.run_id, self.session_epoch)

    @property
    def coverage_from_open(self) -> bool:
        return bool(self.flags & ENDPOINT_FLAG_COVERAGE_FROM_OPEN)

    @property
    def record_coverage_complete(self) -> bool:
        return bool(
            self.flags & ENDPOINT_FLAG_RECORD_COVERAGE_COMPLETE
        )


def parse_generation_endpoint(
    data: bytes | memoryview, offset: int = 0
) -> GenerationEndpoint:
    if len(data) < offset + ENDPOINT_BYTES:
        raise WireFormatError("generation endpoint is truncated")
    values = _ENDPOINT.unpack_from(data, offset)
    result = GenerationEndpoint(
        run_id=values[0],
        session_epoch=values[1],
        generation=values[2],
        catalog_generation=values[3],
        data_state_generation=values[4],
        ingress_sequence_exclusive=values[5],
        tick_stream_sequence_exclusive=values[6],
        recv_monotonic_cut_ns=values[7],
        history_published_monotonic_ns=values[8],
        accepted_sequence=values[9],
        applied_sequence=values[10],
        catalog_digest=values[11],
        input_identity_sha256=values[12],
        source_stream_ids=tuple(values[13:17]),  # type: ignore[arg-type]
        source_sequence_exclusive=tuple(
            values[17:21]
        ),  # type: ignore[arg-type]
        trade_date=values[21],
        capacity=values[22],
        bound_count=values[23],
        available_count=values[24],
        snapshot_available_count=values[25],
        tick_available_count=values[26],
        factor_eligible_count=values[27],
        catalog_scope=CatalogScope(values[28])
        if values[28] == int(CatalogScope.OBSERVED_ONLY)
        else _unsupported_scope(values[28]),
        coverage_complete=_wire_bool(
            values[29], "generation.coverage_complete"
        ),
        flags=values[30],
    )
    return result


def _unsupported_scope(value: int) -> CatalogScope:
    raise WireFormatError(
        f"generation catalog_scope {value} is not OBSERVED_ONLY"
    )


def _wire_bool(value: int, field: str) -> bool:
    if value not in (0, 1):
        raise WireFormatError(f"{field} is not a canonical wire boolean")
    return bool(value)


def validate_generation_endpoint(value: GenerationEndpoint) -> None:
    if not isinstance(value, GenerationEndpoint):
        raise TypeError("generation endpoint has the wrong type")
    _exact_bytes(value.run_id, 16, "generation.run_id")
    for name in (
        "session_epoch",
        "generation",
        "catalog_generation",
        "data_state_generation",
        "ingress_sequence_exclusive",
        "tick_stream_sequence_exclusive",
        "recv_monotonic_cut_ns",
        "history_published_monotonic_ns",
        "accepted_sequence",
        "applied_sequence",
    ):
        _uint(getattr(value, name), 64, f"generation.{name}")
    _exact_bytes(
        value.catalog_digest, 32, "generation.catalog_digest"
    )
    _exact_bytes(
        value.input_identity_sha256,
        32,
        "generation.input_identity_sha256",
    )
    _uint_quad(
        value.source_stream_ids,
        32,
        "generation.source_stream_ids",
    )
    _uint_quad(
        value.source_sequence_exclusive,
        64,
        "generation.source_sequence_exclusive",
    )
    for name in (
        "trade_date",
        "capacity",
        "bound_count",
        "available_count",
        "snapshot_available_count",
        "tick_available_count",
        "factor_eligible_count",
        "flags",
    ):
        _uint(getattr(value, name), 32, f"generation.{name}")
    if not isinstance(value.catalog_scope, CatalogScope):
        raise WireFormatError(
            "generation.catalog_scope is not CatalogScope"
        )
    if not isinstance(value.coverage_complete, bool):
        raise WireFormatError(
            "generation.coverage_complete is not bool"
        )
    _fail(
        not any(value.run_id),
        "generation run_id is not a nonzero 16-byte value",
    )
    _fail(value.session_epoch == 0, "generation session_epoch is zero")
    _fail(value.generation == 0, "generation number is zero")
    _fail(
        value.catalog_generation != value.bound_count,
        "generation catalog_generation does not equal bound_count",
    )
    # Zero is the canonical early-session value when instruments are bound
    # but no snapshot/tick has advanced the observed data state yet.
    _fail(
        not any(value.catalog_digest),
        "generation catalog_digest is not a nonzero SHA-256",
    )
    _fail(
        not any(value.input_identity_sha256),
        "generation input_identity_sha256 is not a nonzero SHA-256",
    )
    _fail(value.trade_date == 0, "generation trade_date is zero")
    _fail(value.capacity == 0, "generation capacity is zero")
    _fail(
        not (
            value.factor_eligible_count
            <= value.snapshot_available_count
            <= value.available_count
            <= value.bound_count
            <= value.capacity
        ),
        "generation snapshot/count hierarchy is inconsistent",
    )
    _fail(
        value.tick_available_count > value.available_count,
        "generation tick_available_count exceeds available_count",
    )
    _fail(
        value.catalog_scope is not CatalogScope.OBSERVED_ONLY,
        "generation catalog_scope is not OBSERVED_ONLY",
    )
    _fail(
        value.coverage_complete,
        "observed-universe generation claims complete coverage",
    )
    _fail(
        value.flags & ~ENDPOINT_FLAGS_MASK != 0,
        "generation endpoint contains unknown flags",
    )
    _fail(
        not value.record_coverage_complete,
        "immutable history generation lacks record coverage",
    )
    _fail(
        value.applied_sequence > value.accepted_sequence,
        "applied_sequence exceeds accepted_sequence",
    )
    _fail(
        value.applied_sequence != value.accepted_sequence,
        "published history generation has not applied its accepted cut",
    )
    _fail(
        value.ingress_sequence_exclusive
        != value.accepted_sequence + 1,
        "ingress endpoint does not match accepted_sequence",
    )
    _fail(
        len(value.source_stream_ids) != 4
        or any(item == 0 for item in value.source_stream_ids)
        or len(set(value.source_stream_ids)) != 4,
        "source_stream_ids are not four distinct nonzero IDs",
    )
    _fail(
        len(value.source_sequence_exclusive) != 4
        or any(
            item == 0 for item in value.source_sequence_exclusive
        ),
        "source sequence endpoints are not four positive values",
    )
    _fail(
        sum(
            item - 1
            for item in value.source_sequence_exclusive
        )
        != value.accepted_sequence,
        "source sequence endpoints do not reconcile with accepted_sequence",
    )
    _fail(
        value.tick_stream_sequence_exclusive
        != value.source_sequence_exclusive[1]
        + value.source_sequence_exclusive[3]
        - 1,
        "tick endpoint does not reconcile with tick sources",
    )
    _fail(
        value.recv_monotonic_cut_ns == 0,
        "generation receive-time cut is zero",
    )
    _fail(
        value.history_published_monotonic_ns == 0,
        "history publication timestamp is zero",
    )
    _fail(
        value.history_published_monotonic_ns
        < value.recv_monotonic_cut_ns,
        "history publication precedes its receive-time cut",
    )


def pack_generation_endpoint(value: GenerationEndpoint) -> bytes:
    validate_generation_endpoint(value)
    return _ENDPOINT.pack(
        value.run_id,
        value.session_epoch,
        value.generation,
        value.catalog_generation,
        value.data_state_generation,
        value.ingress_sequence_exclusive,
        value.tick_stream_sequence_exclusive,
        value.recv_monotonic_cut_ns,
        value.history_published_monotonic_ns,
        value.accepted_sequence,
        value.applied_sequence,
        value.catalog_digest,
        value.input_identity_sha256,
        *value.source_stream_ids,
        *value.source_sequence_exclusive,
        value.trade_date,
        value.capacity,
        value.bound_count,
        value.available_count,
        value.snapshot_available_count,
        value.tick_available_count,
        value.factor_eligible_count,
        int(value.catalog_scope),
        int(value.coverage_complete),
        value.flags,
    )


def validate_same_session(
    endpoint: GenerationEndpoint,
    *,
    run_id: bytes,
    session_epoch: int,
    trade_date: int,
    capacity: int,
) -> None:
    if (
        endpoint.run_id != run_id
        or endpoint.session_epoch != session_epoch
        or endpoint.trade_date != trade_date
        or endpoint.capacity != capacity
    ):
        raise WireFormatError(
            "immutable generation belongs to another session/day/layout"
        )


__all__ = [
    "ENDPOINT_BYTES",
    "ENDPOINT_FLAG_COVERAGE_FROM_OPEN",
    "ENDPOINT_FLAG_RECORD_COVERAGE_COMPLETE",
    "GenerationEndpoint",
    "pack_generation_endpoint",
    "parse_generation_endpoint",
    "validate_generation_endpoint",
    "validate_same_session",
]
