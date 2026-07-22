from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import hashlib
from typing import ClassVar

import numpy as np

from .canonical import MdlBatchView
from .errors import CheckpointError, ValidationError
from .spec import FactorSpec


class PlaceholderStatus(Enum):
    PASSTHROUGH_PLACEHOLDER = "PASSTHROUGH_PLACEHOLDER"


@dataclass(frozen=True, slots=True)
class PlaceholderResult:
    factor_id: str
    factor_version: str
    records: np.ndarray
    status: PlaceholderStatus
    factor_value: None
    factor_value_valid: bool
    _idempotency_key: tuple[str, ...]
    _content_digest: bytes

    @property
    def idempotency_key(self) -> tuple[str, ...]:
        return self._idempotency_key

    @property
    def content_digest(self) -> bytes:
        return self._content_digest


class PassthroughPlaceholderFactor:
    """Explicit non-mathematical placeholder; it does not calculate a factor."""

    FACTOR_ID: ClassVar[str]
    IMPLEMENTATION_STATUS: ClassVar[str] = "PASSTHROUGH_PLACEHOLDER"

    def __init__(self, spec: FactorSpec) -> None:
        if not isinstance(spec, FactorSpec):
            raise ValidationError("placeholder requires a frozen FactorSpec")
        if spec.factor_id != self.FACTOR_ID:
            raise ValidationError(
                f"placeholder {self.FACTOR_ID} cannot use spec {spec.factor_id}"
            )
        self.spec = spec

    def serialize_state(self) -> bytes:
        return (
            b'{"encoding":"l2flow-passthrough-placeholder-state-v1",'
            b'"event_count":0}'
        )

    def restore_state(self, state: bytes) -> None:
        if state != self.serialize_state():
            raise CheckpointError("placeholder state bytes are not the exact V1 empty state")

    def on_batch(self, batch: MdlBatchView) -> tuple[PlaceholderResult, ...]:
        if not isinstance(batch, MdlBatchView) or not batch.is_active:
            raise ValidationError("placeholder requires an active MdlBatchView")
        metadata = batch.metadata
        records = batch.records
        # This digest records exact input bytes and layout.  It is provenance,
        # not a calculated factor value.
        digest = hashlib.sha256()
        digest.update(b"l2flow.factor.passthrough-placeholder.v1\x00")
        digest.update(self.spec.sha256())
        digest.update(metadata.schema_sha256)
        digest.update(metadata.dtype_sha256)
        digest.update(metadata.registry_sha256)
        digest.update(metadata.clock_epoch_digest)
        digest.update(metadata.origin_source_writer_instance)
        for value, width in (
            (metadata.source_stream_id, 4),
            (metadata.trade_date, 4),
            (metadata.origin_capture_date, 4),
            (metadata.shard_id, 4),
            (metadata.clock_epoch_algorithm, 4),
            (metadata.origin_source_generation, 8),
            (metadata.canonical_generation, 8),
            (metadata.registry_version, 8),
            (metadata.begin_canonical_cursor, 8),
            (metadata.end_canonical_cursor, 8),
            (metadata.max_consumed_origin_wal_end_pos, 8),
            (metadata.batch_quality_flags, 8),
        ):
            digest.update(value.to_bytes(width, "little"))
        digest.update(metadata.origin_stream_day_id)
        digest.update(metadata.family.value.encode("ascii") + b"\x00")
        digest.update(records.tobytes(order="C"))
        key = (
            self.FACTOR_ID,
            self.spec.factor_version,
            self.spec.sha256_hex(),
            str(metadata.source_stream_id),
            str(metadata.trade_date),
            str(metadata.origin_capture_date),
            metadata.origin_stream_day_id.hex(),
            metadata.family.value,
            str(metadata.shard_id),
            str(metadata.begin_canonical_cursor),
            str(metadata.end_canonical_cursor),
            str(metadata.clock_epoch_algorithm),
            metadata.clock_epoch_digest.hex(),
            metadata.origin_source_writer_instance.hex(),
            str(metadata.origin_source_generation),
            str(metadata.canonical_generation),
            str(metadata.registry_version),
            metadata.registry_sha256.hex(),
            metadata.schema_sha256.hex(),
            metadata.dtype_sha256.hex(),
        )
        return (
            PlaceholderResult(
                factor_id=self.FACTOR_ID,
                factor_version=self.spec.factor_version,
                records=records,
                status=PlaceholderStatus.PASSTHROUGH_PLACEHOLDER,
                factor_value=None,
                factor_value_valid=False,
                _idempotency_key=key,
                _content_digest=digest.digest(),
            ),
        )


class BookImbalancePassthroughPlaceholder(PassthroughPlaceholderFactor):
    FACTOR_ID = "book_imbalance"


class MicropricePassthroughPlaceholder(PassthroughPlaceholderFactor):
    FACTOR_ID = "microprice"


class TradeImbalancePassthroughPlaceholder(PassthroughPlaceholderFactor):
    # Deliberately trade_imbalance, not order_flow_imbalance.  No definition
    # of either mathematical factor is implied by this placeholder.
    FACTOR_ID = "trade_imbalance"


class CancelRatePassthroughPlaceholder(PassthroughPlaceholderFactor):
    FACTOR_ID = "cancel_rate"


class TradeIntensityPassthroughPlaceholder(PassthroughPlaceholderFactor):
    FACTOR_ID = "trade_intensity"


PLACEHOLDER_FACTOR_TYPES = (
    BookImbalancePassthroughPlaceholder,
    MicropricePassthroughPlaceholder,
    TradeImbalancePassthroughPlaceholder,
    CancelRatePassthroughPlaceholder,
    TradeIntensityPassthroughPlaceholder,
)

PLACEHOLDER_FACTOR_IDS = tuple(item.FACTOR_ID for item in PLACEHOLDER_FACTOR_TYPES)
