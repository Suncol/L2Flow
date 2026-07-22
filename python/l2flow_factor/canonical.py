from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import re
import sys
from typing import Any, Iterator, Mapping

import numpy as np

from .errors import AttachError, BatchClosedError, ValidationError


CANONICAL_SCHEMA_SHA256_HEX = (
    "f66cc65410a5c0862b87a2e467f13b02fe40c0910c17e418be82209b0ddf405e"
)
CANONICAL_DTYPE_SHA256_HEX = (
    "f92a990e174f4f1aae60244fef447f007c83292f8b8aeb375cc1913f7c8ba5bd"
)
CANONICAL_SCHEMA_SHA256 = bytes.fromhex(CANONICAL_SCHEMA_SHA256_HEX)
CANONICAL_DTYPE_SHA256 = bytes.fromhex(CANONICAL_DTYPE_SHA256_HEX)


class InputFamily(Enum):
    SNAPSHOT = "snapshot"
    TICK = "tick"
    QUALITY = "quality"
    CONTROL = "control"
    LATEST_STATE = "latest_state"

    @property
    def canonical_event_type(self) -> int:
        values = {
            InputFamily.SNAPSHOT: 1,
            InputFamily.TICK: 2,
            InputFamily.QUALITY: 3,
            InputFamily.CONTROL: 4,
        }
        if self not in values:
            raise ValidationError(
                "latest_state has no CanonicalEventTypeV1 numeric identity"
            )
        return values[self]


def _fixed_bytes(value: Any, width: int, name: str, *, nonzero: bool = False) -> bytes:
    if isinstance(value, str):
        if not re.fullmatch(r"[0-9a-f]{%d}" % (width * 2), value):
            raise ValidationError(f"{name} must be lowercase {width * 2}-digit hex")
        result = bytes.fromhex(value)
    elif isinstance(value, (bytes, bytearray, memoryview)):
        result = bytes(value)
    else:
        raise ValidationError(f"{name} must be bytes or lowercase hex")
    if len(result) != width:
        raise ValidationError(f"{name} must contain exactly {width} bytes")
    if nonzero and not any(result):
        raise ValidationError(f"{name} must not be all zero")
    return result


def _uint(value: Any, bits: int, name: str, *, positive: bool = False) -> int:
    if type(value) is not int:
        raise ValidationError(f"{name} must be an integer")
    minimum = 1 if positive else 0
    maximum = (1 << bits) - 1
    if value < minimum or value > maximum:
        qualifier = "positive " if positive else ""
        raise ValidationError(f"{name} must be a {qualifier}uint{bits}")
    return value


def _record_dtype(header: np.dtype, payload: np.dtype, itemsize: int) -> np.dtype:
    return np.dtype(
        {
            "names": ["header", "payload"],
            "formats": [header, payload],
            "offsets": [0, 112],
            "itemsize": itemsize,
        }
    )


CANONICAL_HEADER_DTYPE = np.dtype(
    {
        "names": [
            "magic", "schema_version", "event_type", "record_size",
            "source_stream_id", "connection_epoch", "trade_date",
            "quality_flags", "shard_event_id", "origin_ingress_sequence",
            "origin_wal_end_pos", "vendor_sequence_id", "exchange_sequence",
            "exchange_time_ns", "recv_realtime_ns", "recv_monotonic_ns",
            "instrument_id", "channel", "market", "origin_service_version",
            "origin_message_id", "origin_service_id", "sub_index",
        ],
        "formats": [
            "<u4", "<u2", "<u2", "<u4", "<u4", "<u4", "<u4", "<u8",
            "<u8", "<u8", "<u8", "<u8", "<u8", "<i8", "<i8", "<i8",
            "<u4", "<u4", "<u2", "<u2", "<u2", "u1", "u1",
        ],
        "offsets": [
            0, 4, 6, 8, 12, 16, 20, 24, 32, 40, 48, 56, 64, 72, 80, 88,
            96, 100, 104, 106, 108, 110, 111,
        ],
        "itemsize": 112,
    }
)

CANONICAL_TICK_PAYLOAD_DTYPE = np.dtype(
    {
        "names": [
            "price_p6", "quantity_native", "trade_amount_p6",
            "matched_quantity_native", "primary_order_id", "buy_order_id",
            "sell_order_id", "validity_bitmap", "business_flags",
            "source_enum_bits", "action", "side", "order_type", "aggressor",
            "quantity_unit", "phase", "reserved",
        ],
        "formats": [
            "<i8", "<i8", "<i8", "<i8", "<i8", "<i8", "<i8", "<u4",
            "<u4", "<u8", "u1", "u1", "u1", "u1", "u1", "u1", "V2",
        ],
        "offsets": [0, 8, 16, 24, 32, 40, 48, 56, 60, 64, 72, 73, 74, 75, 76, 77, 78],
        "itemsize": 80,
    }
)

CANONICAL_SNAPSHOT_PAYLOAD_DTYPE = np.dtype(
    {
        "names": [
            "pre_close_price_p6", "open_price_p6", "high_price_p6",
            "low_price_p6", "last_price_p6", "close_price_p6",
            "volume_native", "turnover_p6", "trade_count",
            "total_bid_quantity_native", "total_ask_quantity_native",
            "weighted_bid_price_p6", "weighted_ask_price_p6",
            "high_limit_price_p6", "low_limit_price_p6", "iopv_p6",
            "bid_price_p6", "bid_quantity_native", "ask_price_p6",
            "ask_quantity_native", "bid1_queue_quantity_native",
            "ask1_queue_quantity_native", "bid_order_count", "ask_order_count",
            "raw_phase_bits", "status_code_bits", "scalar_validity",
            "bid_queue_validity", "ask_queue_validity", "snapshot_flags",
            "image_status", "actual_bid_depth", "actual_ask_depth",
            "bid1_total_order_count", "bid1_revealed_count",
            "ask1_total_order_count", "ask1_revealed_count",
            "bid_price_validity", "bid_quantity_validity",
            "bid_order_count_validity", "ask_price_validity",
            "ask_quantity_validity", "ask_order_count_validity", "phase",
            "quantity_unit", "high_limit_semantics", "low_limit_semantics",
            "reserved",
        ],
        "formats": [
            "<i8", "<i8", "<i8", "<i8", "<i8", "<i8", "<i8", "<i8",
            "<i8", "<i8", "<i8", "<i8", "<i8", "<i8", "<i8", "<i8",
            ("<i8", (10,)), ("<i8", (10,)), ("<i8", (10,)),
            ("<i8", (10,)), ("<i8", (50,)), ("<i8", (50,)),
            ("<u4", (10,)), ("<u4", (10,)), "<u8", "<u8", "<u8", "<u8",
            "<u8", "<u4", "<u4", "<u4", "<u4", "<u4", "<u4", "<u4",
            "<u4", "<u2", "<u2", "<u2", "<u2", "<u2", "<u2", "u1",
            "u1", "u1", "u1", "V520",
        ],
        "offsets": [
            0, 8, 16, 24, 32, 40, 48, 56, 64, 72, 80, 88, 96, 104, 112,
            120, 128, 208, 288, 368, 448, 848, 1248, 1288, 1328, 1336, 1344,
            1352, 1360, 1368, 1372, 1376, 1380, 1384, 1388, 1392, 1396, 1400,
            1402, 1404, 1406, 1408, 1410, 1412, 1413, 1414, 1415, 1416,
        ],
        "itemsize": 1936,
    }
)

CANONICAL_TICK_DTYPE = _record_dtype(
    CANONICAL_HEADER_DTYPE, CANONICAL_TICK_PAYLOAD_DTYPE, 192
)
CANONICAL_SNAPSHOT_DTYPE = _record_dtype(
    CANONICAL_HEADER_DTYPE, CANONICAL_SNAPSHOT_PAYLOAD_DTYPE, 2048
)


def canonical_record_dtype(family: InputFamily) -> np.dtype:
    if family is InputFamily.TICK:
        return CANONICAL_TICK_DTYPE
    if family is InputFamily.SNAPSHOT:
        return CANONICAL_SNAPSHOT_DTYPE
    raise AttachError(f"Python V1 batch attach does not expose {family.value} records")


def _has_immutable_python_backing(records: np.ndarray) -> bool:
    """Accept only a bytes-backed manual/test view.

    Merely clearing an ndarray's writeable flag is insufficient: a retained
    writeable owner can still mutate the same storage after attach.  A real
    zero-copy native adapter must instead own a read-only mmap and a native
    view handle; that adapter is intentionally not impersonated here.
    """
    current: Any = records
    seen: set[int] = set()
    while isinstance(current, np.ndarray):
        if id(current) in seen or current.flags.writeable or current.flags.owndata:
            return False
        seen.add(id(current))
        current = current.base
    while isinstance(current, memoryview):
        if id(current) in seen or not current.readonly:
            return False
        seen.add(id(current))
        current = current.obj
    return isinstance(current, bytes)


@dataclass(frozen=True, slots=True)
class BatchMetadata:
    source_stream_id: int
    trade_date: int
    origin_capture_date: int
    origin_stream_day_id: bytes
    family: InputFamily
    shard_id: int
    begin_canonical_cursor: int
    end_canonical_cursor: int
    max_consumed_origin_wal_end_pos: int
    observed_raw_durable_wal_pos: int
    clock_epoch_algorithm: int
    clock_epoch_digest: bytes
    clock_epoch_label: int
    origin_source_writer_instance: bytes
    origin_source_generation: int
    canonical_generation: int
    registry_version: int
    registry_sha256: bytes
    schema_sha256: bytes = CANONICAL_SCHEMA_SHA256
    dtype_sha256: bytes = CANONICAL_DTYPE_SHA256
    batch_quality_flags: int = 0
    watermark_set_id: int = 0

    def __post_init__(self) -> None:
        _uint(self.source_stream_id, 32, "source_stream_id", positive=True)
        _uint(self.trade_date, 32, "trade_date", positive=True)
        _uint(self.origin_capture_date, 32, "origin_capture_date", positive=True)
        object.__setattr__(self, "origin_stream_day_id", _fixed_bytes(
            self.origin_stream_day_id, 16, "origin_stream_day_id", nonzero=True
        ))
        if not isinstance(self.family, InputFamily):
            raise ValidationError("family must be an InputFamily")
        if self.family is InputFamily.LATEST_STATE:
            raise ValidationError("MdlBatchView only attaches Canonical record families")
        _uint(self.shard_id, 32, "shard_id")
        begin = _uint(self.begin_canonical_cursor, 64, "begin_canonical_cursor")
        end = _uint(self.end_canonical_cursor, 64, "end_canonical_cursor")
        if end <= begin:
            raise ValidationError(
                "a native MdlBatchView cursor interval must be non-empty"
            )
        _uint(
            self.max_consumed_origin_wal_end_pos,
            64,
            "max_consumed_origin_wal_end_pos",
        )
        _uint(self.observed_raw_durable_wal_pos, 64, "observed_raw_durable_wal_pos")
        _uint(self.clock_epoch_algorithm, 32, "clock_epoch_algorithm", positive=True)
        object.__setattr__(self, "clock_epoch_digest", _fixed_bytes(
            self.clock_epoch_digest, 32, "clock_epoch_digest", nonzero=True
        ))
        _uint(self.clock_epoch_label, 64, "clock_epoch_label")
        object.__setattr__(self, "origin_source_writer_instance", _fixed_bytes(
            self.origin_source_writer_instance,
            16,
            "origin_source_writer_instance",
            nonzero=True,
        ))
        _uint(
            self.origin_source_generation,
            64,
            "origin_source_generation",
            positive=True,
        )
        _uint(self.canonical_generation, 64, "canonical_generation", positive=True)
        _uint(self.registry_version, 64, "registry_version", positive=True)
        object.__setattr__(self, "registry_sha256", _fixed_bytes(
            self.registry_sha256, 32, "registry_sha256", nonzero=True
        ))
        object.__setattr__(self, "schema_sha256", _fixed_bytes(
            self.schema_sha256, 32, "schema_sha256", nonzero=True
        ))
        object.__setattr__(self, "dtype_sha256", _fixed_bytes(
            self.dtype_sha256, 32, "dtype_sha256", nonzero=True
        ))
        _uint(self.batch_quality_flags, 64, "batch_quality_flags")
        _uint(self.watermark_set_id, 64, "watermark_set_id", positive=True)

    @property
    def input_key(self) -> tuple[int, int, bytes, InputFamily, int]:
        return (
            self.source_stream_id,
            self.origin_capture_date,
            self.origin_stream_day_id,
            self.family,
            self.shard_id,
        )


@dataclass(frozen=True, slots=True)
class ConsumerAttachSpec:
    source_stream_id: int
    trade_date: int
    origin_capture_date: int
    origin_stream_day_id: bytes
    family: InputFamily
    shard_id: int
    clock_epoch_algorithm: int
    clock_epoch_digest: bytes
    origin_source_writer_instance: bytes
    origin_source_generation: int
    canonical_generation: int
    registry_version: int
    registry_sha256: bytes
    schema_sha256: bytes = CANONICAL_SCHEMA_SHA256
    dtype_sha256: bytes = CANONICAL_DTYPE_SHA256

    def __post_init__(self) -> None:
        _uint(self.source_stream_id, 32, "source_stream_id", positive=True)
        _uint(self.trade_date, 32, "trade_date", positive=True)
        _uint(self.origin_capture_date, 32, "origin_capture_date", positive=True)
        object.__setattr__(self, "origin_stream_day_id", _fixed_bytes(
            self.origin_stream_day_id, 16, "origin_stream_day_id", nonzero=True
        ))
        if self.family not in (InputFamily.TICK, InputFamily.SNAPSHOT):
            raise ValidationError("consumer attach family must be tick or snapshot")
        _uint(self.shard_id, 32, "shard_id")
        _uint(self.clock_epoch_algorithm, 32, "clock_epoch_algorithm", positive=True)
        object.__setattr__(self, "clock_epoch_digest", _fixed_bytes(
            self.clock_epoch_digest, 32, "clock_epoch_digest", nonzero=True
        ))
        object.__setattr__(self, "origin_source_writer_instance", _fixed_bytes(
            self.origin_source_writer_instance,
            16,
            "origin_source_writer_instance",
            nonzero=True,
        ))
        _uint(
            self.origin_source_generation,
            64,
            "origin_source_generation",
            positive=True,
        )
        _uint(self.canonical_generation, 64, "canonical_generation", positive=True)
        _uint(self.registry_version, 64, "registry_version", positive=True)
        object.__setattr__(self, "registry_sha256", _fixed_bytes(
            self.registry_sha256, 32, "registry_sha256", nonzero=True
        ))
        object.__setattr__(self, "schema_sha256", _fixed_bytes(
            self.schema_sha256, 32, "schema_sha256", nonzero=True
        ))
        object.__setattr__(self, "dtype_sha256", _fixed_bytes(
            self.dtype_sha256, 32, "dtype_sha256", nonzero=True
        ))

    def validate_metadata(self, metadata: BatchMetadata) -> None:
        comparisons = (
            ("source_stream_id", self.source_stream_id, metadata.source_stream_id),
            ("trade_date", self.trade_date, metadata.trade_date),
            ("origin_capture_date", self.origin_capture_date,
             metadata.origin_capture_date),
            ("origin_stream_day_id", self.origin_stream_day_id,
             metadata.origin_stream_day_id),
            ("family", self.family, metadata.family),
            ("shard_id", self.shard_id, metadata.shard_id),
            ("clock_epoch_algorithm", self.clock_epoch_algorithm,
             metadata.clock_epoch_algorithm),
            ("clock_epoch_digest", self.clock_epoch_digest,
             metadata.clock_epoch_digest),
            ("origin_source_writer_instance", self.origin_source_writer_instance,
             metadata.origin_source_writer_instance),
            ("origin_source_generation", self.origin_source_generation,
             metadata.origin_source_generation),
            ("canonical_generation", self.canonical_generation,
             metadata.canonical_generation),
            ("registry_version", self.registry_version, metadata.registry_version),
            ("registry_sha256", self.registry_sha256, metadata.registry_sha256),
            ("schema_sha256", self.schema_sha256, metadata.schema_sha256),
            ("dtype_sha256", self.dtype_sha256, metadata.dtype_sha256),
        )
        for name, expected, observed in comparisons:
            if expected != observed:
                raise AttachError(f"{name} does not match the expected attach identity")


class _BatchLease:
    __slots__ = ("active",)

    def __init__(self) -> None:
        self.active = False

    def require_active(self) -> None:
        if not self.active:
            raise BatchClosedError("MdlBatchView records are outside their context lifetime")


class _LeaseCheckedArray(np.ndarray):
    """Read-only ndarray view whose ordinary data access checks a batch lease."""

    def __new__(cls, source: np.ndarray, lease: _BatchLease) -> "_LeaseCheckedArray":
        result = source.view(cls)
        result._l2flow_lease = lease
        result.flags.writeable = False
        return result

    def __array_finalize__(self, source: Any) -> None:
        self._l2flow_lease = getattr(source, "_l2flow_lease", None)

    def _require_active(self) -> None:
        lease = getattr(self, "_l2flow_lease", None)
        if lease is None:
            raise BatchClosedError("detached batch array has no lifetime authority")
        lease.require_active()

    def __getitem__(self, key: Any) -> Any:
        self._require_active()
        return super().__getitem__(key)

    def __setitem__(self, key: Any, value: Any) -> None:
        self._require_active()
        raise ValueError("MdlBatchView records are read-only")

    def __iter__(self) -> Iterator[Any]:
        self._require_active()
        return super().__iter__()

    def item(self, *args: Any) -> Any:
        self._require_active()
        return super().item(*args)

    def tobytes(self, *args: Any, **kwargs: Any) -> bytes:
        self._require_active()
        return super().tobytes(*args, **kwargs)

    def tolist(self) -> list[Any]:
        self._require_active()
        return super().tolist()

    def copy(self, *args: Any, **kwargs: Any) -> np.ndarray:
        self._require_active()
        result = np.ndarray.copy(self, *args, **kwargs).view(np.ndarray)
        result.flags.writeable = False
        return result

    def __array_ufunc__(
        self,
        ufunc: Any,
        method: str,
        *inputs: Any,
        **kwargs: Any,
    ) -> Any:
        self._require_active()
        converted_inputs = []
        for value in inputs:
            if isinstance(value, _LeaseCheckedArray):
                value._require_active()
                converted_inputs.append(value.view(np.ndarray))
            else:
                converted_inputs.append(value)
        if "out" in kwargs and kwargs["out"] is not None:
            converted_out = []
            for value in kwargs["out"]:
                if isinstance(value, _LeaseCheckedArray):
                    value._require_active()
                    raise ValueError("MdlBatchView records are read-only")
                converted_out.append(value)
            kwargs["out"] = tuple(converted_out)
        return getattr(ufunc, method)(*converted_inputs, **kwargs)

    def __array_function__(
        self,
        function: Any,
        types: Any,
        args: Any,
        kwargs: Any,
    ) -> Any:
        del types
        self._require_active()

        def unwrap(value: Any) -> Any:
            if isinstance(value, _LeaseCheckedArray):
                value._require_active()
                return value.view(np.ndarray)
            if isinstance(value, tuple):
                return tuple(unwrap(item) for item in value)
            if isinstance(value, list):
                return [unwrap(item) for item in value]
            if isinstance(value, dict):
                return {key: unwrap(item) for key, item in value.items()}
            return value

        return function(*unwrap(args), **unwrap(kwargs))


class MdlBatchView:
    """Context-bounded, read-only view over one validated Canonical batch.

    Access through this leased view fails after context exit.  A caller may
    explicitly call ``records.copy()`` while active; that independent,
    read-only copy owns its bytes and intentionally outlives the batch.
    """

    __slots__ = ("_records", "_metadata", "_lease", "_entered", "_closed")

    def __init__(
        self,
        records: np.ndarray,
        metadata: BatchMetadata,
        expected: ConsumerAttachSpec,
    ) -> None:
        if sys.byteorder != "little":
            raise AttachError("Canonical V1 direct mmap attach requires a little-endian host")
        if not isinstance(records, np.ndarray):
            raise AttachError("records must be a NumPy ndarray")
        if not isinstance(expected, ConsumerAttachSpec):
            raise AttachError("expected must be a ConsumerAttachSpec")
        expected.validate_metadata(metadata)
        if records.ndim != 1 or not records.flags.c_contiguous:
            raise AttachError("records must be a one-dimensional C-contiguous array")
        if records.flags.writeable:
            raise AttachError(
                "records must come from a read-only native mapping or immutable buffer"
            )
        if not _has_immutable_python_backing(records):
            raise AttachError(
                "manual attach requires bytes-backed immutable storage; "
                "native mmap views require the native adapter"
            )
        if metadata.schema_sha256 != CANONICAL_SCHEMA_SHA256:
            raise AttachError("Canonical schema identity mismatch")
        if metadata.dtype_sha256 != CANONICAL_DTYPE_SHA256:
            raise AttachError("Canonical dtype identity mismatch")
        expected_dtype = canonical_record_dtype(metadata.family)
        if records.dtype != expected_dtype:
            raise AttachError("NumPy dtype does not match the frozen Canonical V1 layout")
        if len(records) != metadata.end_canonical_cursor - metadata.begin_canonical_cursor:
            raise AttachError("record count does not match the exclusive cursor interval")
        self._validate_minimum_headers(records, metadata)
        self._lease = _BatchLease()
        self._records = _LeaseCheckedArray(records, self._lease)
        self._metadata = metadata
        self._entered = False
        self._closed = False

    @staticmethod
    def _validate_minimum_headers(
        records: np.ndarray,
        metadata: BatchMetadata,
    ) -> None:
        """Check ABI/namespace facts; full semantic validation remains native."""
        if len(records) == 0:
            return
        headers = records["header"]
        expected_event_type = metadata.family.canonical_event_type
        expected_record_size = canonical_record_dtype(metadata.family).itemsize
        checks = (
            (np.all(headers["magic"] == np.uint32(0x3145434D)), "magic"),
            (np.all(headers["schema_version"] == np.uint16(1)), "schema_version"),
            (np.all(headers["event_type"] == np.uint16(expected_event_type)), "event_type"),
            (np.all(headers["record_size"] == np.uint32(expected_record_size)), "record_size"),
            (
                np.all(headers["source_stream_id"] == np.uint32(metadata.source_stream_id)),
                "source_stream_id",
            ),
            (
                np.all(headers["trade_date"] == np.uint32(metadata.trade_date)),
                "trade_date",
            ),
            (np.all(headers["shard_event_id"] > 0), "shard_event_id"),
            (np.all(headers["origin_ingress_sequence"] > 0), "origin_ingress_sequence"),
            (np.all(headers["origin_wal_end_pos"] > 0), "origin_wal_end_pos"),
            (np.all(headers["instrument_id"] > 0), "instrument_id"),
            (
                np.all((headers["market"] == 1) | (headers["market"] == 2)),
                "market",
            ),
            (
                np.all((headers["quality_flags"] & ~np.uint64(0x0000000FFFFFFFFF)) == 0),
                "quality_flags",
            ),
        )
        for passed, name in checks:
            if not bool(passed):
                raise AttachError(f"Canonical header {name} failed minimum attach validation")
        origin_wal = headers["origin_wal_end_pos"]
        if bool(np.any(origin_wal[1:] < origin_wal[:-1])):
            raise AttachError("Canonical batch origin WAL cursor regressed")
        if int(np.max(origin_wal)) != metadata.max_consumed_origin_wal_end_pos:
            raise AttachError(
                "batch max origin WAL does not match max_consumed_origin_wal_end_pos"
            )

    def __enter__(self) -> "MdlBatchView":
        if self._entered or self._closed:
            raise BatchClosedError("MdlBatchView contexts are single-use")
        self._entered = True
        self._lease.active = True
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> bool:
        self._lease.active = False
        self._closed = True
        return False

    def _require_active(self) -> None:
        self._lease.require_active()

    @property
    def records(self) -> np.ndarray:
        self._require_active()
        return self._records

    @property
    def metadata(self) -> BatchMetadata:
        self._require_active()
        return self._metadata

    @property
    def is_active(self) -> bool:
        return self._lease.active

    @classmethod
    def attach(
        cls,
        records: np.ndarray,
        metadata: BatchMetadata,
        expected: ConsumerAttachSpec,
    ) -> "MdlBatchView":
        return cls(records, metadata, expected)
