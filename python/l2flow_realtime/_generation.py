"""Exact parsing for the shared Wire V2 immutable-generation endpoint."""

from __future__ import annotations

import struct
from dataclasses import dataclass

from .models import (
    CatalogScope,
    SessionIdentity,
    SessionInfo,
    WireFormatError,
)


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
    """One immutable declared-daily-catalog Store generation endpoint."""

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


@dataclass(frozen=True, slots=True)
class DailyCatalogSessionIdentity:
    """The exact live-session identity required to attach history."""

    run_id: bytes
    session_epoch: int
    trade_date: int
    capacity: int
    catalog_digest: bytes
    catalog_generation: int
    bound_count: int
    catalog_scope: CatalogScope
    coverage_complete: bool
    catalog_trade_date: int
    catalog_version: int

    def __post_init__(self) -> None:
        validate_daily_catalog_session_identity(self)

    @classmethod
    def from_session_info(
        cls, session: SessionInfo
    ) -> "DailyCatalogSessionIdentity":
        if not isinstance(session, SessionInfo):
            raise TypeError("session must be SessionInfo")
        return cls(
            run_id=session.run_id,
            session_epoch=session.session_epoch,
            trade_date=session.trade_date,
            capacity=session.capacity,
            catalog_digest=session.catalog_digest,
            catalog_generation=session.catalog_generation,
            bound_count=session.bound_count,
            catalog_scope=session.catalog_scope,
            coverage_complete=session.coverage_complete,
            catalog_trade_date=session.catalog_trade_date,
            catalog_version=session.catalog_version,
        )

    @property
    def session_identity(self) -> SessionIdentity:
        return SessionIdentity(self.run_id, self.session_epoch)


def validate_daily_catalog_session_identity(
    value: DailyCatalogSessionIdentity,
) -> None:
    _exact_bytes(value.run_id, 16, "expected_session.run_id")
    _exact_bytes(
        value.catalog_digest,
        32,
        "expected_session.catalog_digest",
    )
    _fail(
        not any(value.run_id),
        "expected_session.run_id is zero",
    )
    _fail(
        not any(value.catalog_digest),
        "expected_session.catalog_digest is zero",
    )
    _uint(
        value.session_epoch,
        64,
        "expected_session.session_epoch",
        nonzero=True,
    )
    _uint(
        value.trade_date,
        32,
        "expected_session.trade_date",
        nonzero=True,
    )
    _uint(
        value.capacity,
        32,
        "expected_session.capacity",
        nonzero=True,
    )
    _uint(
        value.catalog_generation,
        64,
        "expected_session.catalog_generation",
        nonzero=True,
    )
    _uint(
        value.bound_count,
        32,
        "expected_session.bound_count",
        nonzero=True,
    )
    _uint(
        value.catalog_trade_date,
        32,
        "expected_session.catalog_trade_date",
        nonzero=True,
    )
    _uint(
        value.catalog_version,
        64,
        "expected_session.catalog_version",
        nonzero=True,
    )
    _fail(
        not isinstance(value.catalog_scope, CatalogScope)
        or value.catalog_scope
        is not CatalogScope.DECLARED_DAILY_A_SHARE,
        "expected_session.catalog_scope is not DECLARED_DAILY_A_SHARE",
    )
    _fail(
        value.coverage_complete is not True,
        "expected_session.coverage_complete is not true",
    )
    _fail(
        value.catalog_generation != 1,
        "expected_session.catalog_generation is not frozen at one",
    )
    _fail(
        value.bound_count != value.capacity,
        "expected_session does not bind its full daily catalog",
    )
    _fail(
        value.catalog_trade_date != value.trade_date,
        "expected_session catalog trade date differs from session day",
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
        if values[28] == int(CatalogScope.DECLARED_DAILY_A_SHARE)
        else _unsupported_scope(values[28]),
        coverage_complete=_wire_bool(
            values[29], "generation.coverage_complete"
        ),
        flags=values[30],
    )
    return result


def _unsupported_scope(value: int) -> CatalogScope:
    raise WireFormatError(
        "generation catalog_scope "
        f"{value} is not DECLARED_DAILY_A_SHARE"
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
        value.catalog_generation != 1,
        "generation catalog_generation does not equal one",
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
        value.catalog_scope is not CatalogScope.DECLARED_DAILY_A_SHARE,
        "generation catalog_scope is not DECLARED_DAILY_A_SHARE",
    )
    _fail(
        not value.coverage_complete,
        "daily A-share generation lacks complete catalog coverage",
    )
    _fail(
        value.bound_count != value.capacity,
        "daily catalog generation does not bind its full capacity",
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
    expected: DailyCatalogSessionIdentity,
) -> None:
    if not isinstance(expected, DailyCatalogSessionIdentity):
        raise TypeError(
            "expected must be DailyCatalogSessionIdentity"
        )
    validate_daily_catalog_session_identity(expected)
    if (
        endpoint.run_id != expected.run_id
        or endpoint.session_epoch != expected.session_epoch
        or endpoint.trade_date != expected.trade_date
        or endpoint.capacity != expected.capacity
        or endpoint.catalog_digest != expected.catalog_digest
        or endpoint.catalog_generation
        != expected.catalog_generation
        or endpoint.bound_count != expected.bound_count
        or endpoint.catalog_scope is not expected.catalog_scope
        or endpoint.coverage_complete
        is not expected.coverage_complete
    ):
        raise WireFormatError(
            "immutable generation belongs to another "
            "session/day/daily catalog"
        )


__all__ = [
    "ENDPOINT_BYTES",
    "ENDPOINT_FLAG_COVERAGE_FROM_OPEN",
    "ENDPOINT_FLAG_RECORD_COVERAGE_COMPLETE",
    "DailyCatalogSessionIdentity",
    "GenerationEndpoint",
    "pack_generation_endpoint",
    "parse_generation_endpoint",
    "validate_daily_catalog_session_identity",
    "validate_generation_endpoint",
    "validate_same_session",
]
