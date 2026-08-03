"""Owned, dense CERTIFIED Tick history-to-tail batches.

The reader consumes only the append-only CERTIFIED Tick journal.  It never
touches the FAST callback or bounded FAST ring.  Sequence ``N`` is permanently
stored in journal slot ``N - 1``; the Python cursor advances only after a whole
owned batch passes ABI, session, trade-date, and dense-sequence validation.
"""

from __future__ import annotations

import ctypes
import math
import os
import struct
import threading
from dataclasses import dataclass
from enum import IntEnum
from typing import Iterator, Optional, Union

from .models import (
    CommonRecord,
    DecimalValue,
    HistoryCoverageInfo,
    InconsistentReadError,
    L2FlowRealtimeError,
    QuantityValue,
    SessionIdentity,
    SessionInfo,
    Side,
    TemporalCoverageKind,
    TickAction,
    TickProjectionFlag,
    UnavailableError,
    WireFormatError,
)
from .native import load_native_library
from .wire import TICK_BYTES, parse_tick_payload


_OPEN_OK = 0
_READ_OK = 0
_READ_INVALID_ARGUMENT = 1
_READ_NOT_YET_PUBLISHED = 2
_READ_END_OF_STREAM = 3
_READ_PRODUCER_FAILED = 4
_READ_OUT_OF_RANGE = 5
_READ_INCONSISTENT = 6
_READ_CORRUPT = 7
_STATUS_SCHEMA_VERSION = 1
_RESULT_SCHEMA_VERSION = 1
_SLOT_BYTES = 512
_PAYLOAD_OFFSET = 64
_ENVELOPE_BYTES = 368
_ENVELOPE_HEAD = struct.Struct("<QQQQ")
_PAYLOAD_PREFIX = struct.Struct("<IIII")
_TRADE_DATE = struct.Struct("<I")
_DEFAULT_BATCH_RECORDS = 4096
_MAX_BATCH_RECORDS = 65_536
_UINT64_MAX = (1 << 64) - 1


class CertifiedTickHistoryState(IntEnum):
    ACTIVE = 1
    COMPLETE = 2
    FAILED = 3


class CertifiedTickHistoryFailure(IntEnum):
    NONE = 0
    INVALID_ARGUMENT = 1
    CANONICAL_SEQUENCE = 2
    TICK_CAPACITY = 3
    BACKING_COMMIT = 4
    ENVELOPE_INVALID = 5
    PUBLICATION_INVARIANT = 6
    UPSTREAM = 7
    STOPPED = 8
    FAILED = 9
    SOURCE_RETENTION_LOST = 10
    INCOMPLETE_NATIVE_PREFIX = 11
    SOURCE_READ = 12


class _ExpectedSessionC(ctypes.Structure):
    _fields_ = [
        ("run_id", ctypes.c_uint8 * 16),
        ("session_epoch", ctypes.c_uint64),
        ("trade_date", ctypes.c_uint32),
        ("reserved", ctypes.c_uint32),
    ]


class _SessionC(ctypes.Structure):
    _fields_ = [
        ("run_id", ctypes.c_uint8 * 16),
        ("session_epoch", ctypes.c_uint64),
        ("total_mapping_bytes", ctypes.c_uint64),
        ("tick_capacity", ctypes.c_uint64),
        ("trade_date", ctypes.c_uint32),
        ("slot_bytes", ctypes.c_uint32),
        ("coverage_kind", ctypes.c_uint32),
        ("reserved_coverage", ctypes.c_uint32),
        ("coverage_start_unix_ns", ctypes.c_uint64),
    ]


class _StatusC(ctypes.Structure):
    _fields_ = [
        ("status_schema_version", ctypes.c_uint32),
        ("status_bytes", ctypes.c_uint32),
        ("publish_tag", ctypes.c_uint64),
        ("heartbeat_monotonic_ns", ctypes.c_uint64),
        ("canonical_apply_frontier", ctypes.c_uint64),
        ("generation", ctypes.c_uint64),
        ("published_tick_count", ctypes.c_uint64),
        ("committed_mapping_bytes", ctypes.c_uint64),
        ("state", ctypes.c_uint32),
        ("failure", ctypes.c_uint32),
        ("reserved", ctypes.c_uint8 * 32),
    ]


class _SlotC(ctypes.Structure):
    _fields_ = [
        ("publish_tag", ctypes.c_uint64),
        ("reserved0", ctypes.c_uint64 * 7),
        ("payload_words", ctypes.c_uint64 * 56),
    ]


class _ReadResultC(ctypes.Structure):
    _fields_ = [
        ("result_schema_version", ctypes.c_uint32),
        ("result_bytes", ctypes.c_uint32),
        ("records_written", ctypes.c_uint64),
        ("next_canonical_apply_sequence", ctypes.c_uint64),
        ("status", _StatusC),
        ("reserved", ctypes.c_uint8 * 40),
    ]


assert ctypes.sizeof(_ExpectedSessionC) == 32
assert ctypes.sizeof(_SessionC) == 64
assert ctypes.sizeof(_StatusC) == 96
assert ctypes.sizeof(_SlotC) == _SLOT_BYTES
assert _SlotC.payload_words.offset == _PAYLOAD_OFFSET
assert ctypes.sizeof(_ReadResultC) == 160
assert _ENVELOPE_HEAD.size + TICK_BYTES == _ENVELOPE_BYTES


@dataclass(frozen=True, slots=True)
class CertifiedTickHistorySession:
    run_id: bytes
    session_epoch: int
    trade_date: int
    tick_capacity: int
    total_mapping_bytes: int
    slot_bytes: int
    history_coverage: HistoryCoverageInfo

    @property
    def identity(self) -> SessionIdentity:
        return SessionIdentity(self.run_id, self.session_epoch)

    @property
    def coverage_from_open(self) -> bool:
        return self.history_coverage.coverage_from_open


@dataclass(frozen=True, slots=True)
class CertifiedTickHistoryStatus:
    publish_tag: int
    heartbeat_monotonic_ns: int
    canonical_apply_frontier: int
    generation: int
    published_tick_count: int
    committed_mapping_bytes: int
    state: CertifiedTickHistoryState
    failure: CertifiedTickHistoryFailure

    @property
    def terminal(self) -> bool:
        return self.state in (
            CertifiedTickHistoryState.COMPLETE,
            CertifiedTickHistoryState.FAILED,
        )


@dataclass(frozen=True, slots=True)
class CertifiedTick:
    canonical_apply_sequence: int
    correction_epoch: int
    feed_epoch: int
    certified_monotonic_ns: int
    common: CommonRecord
    price: DecimalValue
    quantity: QuantityValue
    action: TickAction
    side: Side
    projection_flags: TickProjectionFlag
    wire_payload: bytes


class CertifiedTickHistoryError(L2FlowRealtimeError):
    def __init__(
        self,
        operation: str,
        code: int,
        *,
        system_error_number: int = 0,
    ) -> None:
        self.operation = operation
        self.code = code
        self.system_error_number = system_error_number
        detail = f"{operation} failed with CERTIFIED Tick code {code}"
        if system_error_number:
            detail += (
                f": [errno {system_error_number}] "
                f"{os.strerror(system_error_number)}"
            )
        super().__init__(detail)


class CertifiedTickHistoryProducerFailedError(UnavailableError):
    def __init__(self, status: CertifiedTickHistoryStatus) -> None:
        self.status = status
        super().__init__(
            "CERTIFIED Tick producer failed at canonical frontier "
            f"{status.canonical_apply_frontier}: {status.failure.name}"
        )


class CertifiedTickHistoryCapacityError(UnavailableError):
    """The fixed full-day journal capacity has no addressable successor."""


class CertifiedTickHistoryBatch:
    """Owned fixed-stride slot copies plus one coherent journal status cut."""

    __slots__ = (
        "_bytes",
        "_count",
        "_first_sequence",
        "_rows",
        "history_coverage",
        "next_canonical_apply_sequence",
        "session_identity",
        "status",
    )

    def __init__(
        self,
        rows,
        count: int,
        *,
        first_sequence: int,
        next_sequence: int,
        status: CertifiedTickHistoryStatus,
        session: CertifiedTickHistorySession,
    ) -> None:
        self._rows = rows
        self._count = count
        self._first_sequence = first_sequence
        byte_type = ctypes.c_uint8 * (len(rows) * _SLOT_BYTES)
        self._bytes = byte_type.from_buffer(rows)
        self.next_canonical_apply_sequence = next_sequence
        self.status = status
        self.session_identity = session.identity
        self.history_coverage = session.history_coverage
        self._validate_owned_prefix(session.trade_date)

    def _validate_owned_prefix(self, trade_date: int) -> None:
        for index in range(self._count):
            slot = self._rows[index]
            if (
                slot.publish_tag == 0
                or slot.publish_tag & 1
                or any(slot.reserved0)
                or any(slot.payload_words[46:])
            ):
                raise WireFormatError(
                    "CERTIFIED Tick batch contains a noncanonical slot"
                )
            envelope = ctypes.string_at(
                ctypes.addressof(slot) + _PAYLOAD_OFFSET,
                _ENVELOPE_BYTES,
            )
            canonical, correction, feed, certified_ns = (
                _ENVELOPE_HEAD.unpack_from(envelope)
            )
            expected = self._first_sequence + index
            schema, record_bytes, instrument_id, ordinal = (
                _PAYLOAD_PREFIX.unpack_from(envelope, _ENVELOPE_HEAD.size)
            )
            payload_trade_date = _TRADE_DATE.unpack_from(
                envelope, _ENVELOPE_HEAD.size + 100
            )[0]
            if (
                canonical != expected
                or correction == 0
                or feed == 0
                or certified_ns == 0
                or schema != 2
                or record_bytes != TICK_BYTES
                or instrument_id == 0
                or ordinal != instrument_id - 1
                or payload_trade_date != trade_date
            ):
                raise WireFormatError(
                    "CERTIFIED Tick batch identity/sequence is noncanonical"
                )

    def __len__(self) -> int:
        return self._count

    @property
    def first_canonical_apply_sequence(self) -> int:
        return self._first_sequence

    @property
    def buffer(self) -> memoryview:
        return memoryview(self._bytes)[
            : self._count * _SLOT_BYTES
        ].toreadonly()

    @property
    def tail_idle(self) -> bool:
        return not self and self.status.state is CertifiedTickHistoryState.ACTIVE

    @property
    def end_of_stream(self) -> bool:
        return (
            not self
            and self.status.state is CertifiedTickHistoryState.COMPLETE
        )

    def envelope_bytes(self, index: int) -> bytes:
        index = self._checked_index(index)
        return ctypes.string_at(
            ctypes.addressof(self._rows[index]) + _PAYLOAD_OFFSET,
            _ENVELOPE_BYTES,
        )

    def _checked_index(self, index: int) -> int:
        if not isinstance(index, int) or isinstance(index, bool):
            raise TypeError("index must be an integer")
        if index < 0:
            index += self._count
        if index < 0 or index >= self._count:
            raise IndexError(index)
        return index

    def row(self, index: int) -> CertifiedTick:
        envelope = self.envelope_bytes(index)
        canonical, correction, feed, certified_ns = (
            _ENVELOPE_HEAD.unpack_from(envelope)
        )
        payload = envelope[_ENVELOPE_HEAD.size :]
        instrument_id = _PAYLOAD_PREFIX.unpack_from(payload)[2]
        common, price, quantity, action, side, projection_flags = (
            parse_tick_payload(payload, instrument_id)
        )
        return CertifiedTick(
            canonical_apply_sequence=canonical,
            correction_epoch=correction,
            feed_epoch=feed,
            certified_monotonic_ns=certified_ns,
            common=common,
            price=price,
            quantity=quantity,
            action=action,
            side=side,
            projection_flags=projection_flags,
            wire_payload=payload,
        )

    def __iter__(self) -> Iterator[CertifiedTick]:
        for index in range(self._count):
            yield self.row(index)


def _bind_library(library) -> None:
    if getattr(library, "_l2flow_certified_tick_history_bound_v1", False):
        return
    handle = ctypes.c_void_p
    try:
        open_ = library.l2flow_certified_tick_history_reader_open_v1
    except AttributeError as error:
        raise UnavailableError(
            "native library lacks CERTIFIED Tick history reader V1"
        ) from error
    open_.argtypes = [
        ctypes.c_char_p,
        ctypes.POINTER(_ExpectedSessionC),
        ctypes.c_uint32,
        ctypes.POINTER(handle),
        ctypes.POINTER(ctypes.c_int),
    ]
    open_.restype = ctypes.c_int
    library.l2flow_certified_tick_history_reader_close_v1.argtypes = [handle]
    library.l2flow_certified_tick_history_reader_close_v1.restype = None
    library.l2flow_certified_tick_history_reader_session_v1.argtypes = [
        handle,
        ctypes.POINTER(_SessionC),
    ]
    library.l2flow_certified_tick_history_reader_session_v1.restype = ctypes.c_int
    library.l2flow_certified_tick_history_reader_status_v1.argtypes = [
        handle,
        ctypes.POINTER(_StatusC),
    ]
    library.l2flow_certified_tick_history_reader_status_v1.restype = ctypes.c_int
    library.l2flow_certified_tick_history_reader_read_v1.argtypes = [
        handle,
        ctypes.c_uint64,
        ctypes.POINTER(_SlotC),
        ctypes.c_size_t,
        ctypes.POINTER(_ReadResultC),
    ]
    library.l2flow_certified_tick_history_reader_read_v1.restype = ctypes.c_int
    library._l2flow_certified_tick_history_bound_v1 = True


def _expected_session(
    run_id: bytes, session_epoch: int, trade_date: int
) -> _ExpectedSessionC:
    if not isinstance(run_id, bytes) or len(run_id) != 16 or not any(run_id):
        raise ValueError("run_id must be exactly 16 nonzero bytes")
    if (
        not isinstance(session_epoch, int)
        or isinstance(session_epoch, bool)
        or session_epoch <= 0
        or session_epoch > _UINT64_MAX
    ):
        raise ValueError("session_epoch must be a positive uint64")
    if (
        not isinstance(trade_date, int)
        or isinstance(trade_date, bool)
        or trade_date <= 0
        or trade_date > (1 << 32) - 1
    ):
        raise ValueError("trade_date must be a positive uint32")
    result = _ExpectedSessionC()
    result.run_id[:] = run_id
    result.session_epoch = session_epoch
    result.trade_date = trade_date
    return result


def _aligned_mapping_bytes(tick_capacity: int) -> int:
    logical = 4096 + tick_capacity * _SLOT_BYTES
    return (logical + 4095) & ~4095


def _session_from_c(
    value: _SessionC,
    history_coverage: HistoryCoverageInfo,
) -> CertifiedTickHistorySession:
    if (
        not any(value.run_id)
        or value.session_epoch == 0
        or value.trade_date == 0
        or value.tick_capacity == 0
        or value.slot_bytes != _SLOT_BYTES
        or value.total_mapping_bytes
        != _aligned_mapping_bytes(value.tick_capacity)
        or value.coverage_kind != TemporalCoverageKind.FROM_OPEN
        or value.reserved_coverage != 0
        or value.coverage_start_unix_ns != 0
    ):
        raise WireFormatError("invalid CERTIFIED Tick session ABI")
    session = CertifiedTickHistorySession(
        run_id=bytes(value.run_id),
        session_epoch=value.session_epoch,
        trade_date=value.trade_date,
        tick_capacity=value.tick_capacity,
        total_mapping_bytes=value.total_mapping_bytes,
        slot_bytes=value.slot_bytes,
        history_coverage=history_coverage,
    )
    if (
        not history_coverage.coverage_from_open
        or history_coverage.coverage_kind
        is not TemporalCoverageKind(value.coverage_kind)
        or history_coverage.identity != session.identity
        or history_coverage.trade_date != session.trade_date
    ):
        raise WireFormatError(
            "CERTIFIED Tick mapping and verified coverage disagree"
        )
    return session


def _status_from_c(
    value: _StatusC,
    session: CertifiedTickHistorySession,
) -> CertifiedTickHistoryStatus:
    try:
        state = CertifiedTickHistoryState(value.state)
        failure = CertifiedTickHistoryFailure(value.failure)
    except ValueError as error:
        raise WireFormatError("invalid CERTIFIED Tick lifecycle value") from error
    frontier = int(value.canonical_apply_frontier)
    required_mapping = _aligned_mapping_bytes(frontier)
    failure_valid = (
        failure not in (
            CertifiedTickHistoryFailure.NONE,
            CertifiedTickHistoryFailure.STOPPED,
        )
        if state is CertifiedTickHistoryState.FAILED
        else failure is CertifiedTickHistoryFailure.NONE
    )
    if (
        value.status_schema_version != _STATUS_SCHEMA_VERSION
        or value.status_bytes != ctypes.sizeof(_StatusC)
        or any(value.reserved)
        or value.publish_tag == 0
        or value.publish_tag & 1
        or value.heartbeat_monotonic_ns == 0
        or frontier > session.tick_capacity
        or value.generation != frontier
        or value.published_tick_count != frontier
        or value.committed_mapping_bytes < required_mapping
        or value.committed_mapping_bytes > session.total_mapping_bytes
        or value.committed_mapping_bytes % 4096
        or not failure_valid
    ):
        raise WireFormatError("invalid CERTIFIED Tick status ABI")
    return CertifiedTickHistoryStatus(
        publish_tag=value.publish_tag,
        heartbeat_monotonic_ns=value.heartbeat_monotonic_ns,
        canonical_apply_frontier=frontier,
        generation=value.generation,
        published_tick_count=value.published_tick_count,
        committed_mapping_bytes=value.committed_mapping_bytes,
        state=state,
        failure=failure,
    )


class CertifiedTickHistoryReader:
    """Stateful, fail-closed cursor over one full-day CERTIFIED Tick journal."""

    __slots__ = (
        "_batch_records",
        "_closed",
        "_handle",
        "_library",
        "_lock",
        "_last_status",
        "_next_sequence",
        "_session",
    )

    def __init__(
        self,
        library,
        handle,
        session: CertifiedTickHistorySession,
        *,
        start_canonical_apply_sequence: int,
        batch_records: int,
    ) -> None:
        self._library = library
        self._handle = handle
        self._session = session
        self._next_sequence = start_canonical_apply_sequence
        self._batch_records = batch_records
        self._lock = threading.Lock()
        self._closed = False
        self._last_status: Optional[CertifiedTickHistoryStatus] = None

    @classmethod
    def connect(
        cls,
        control_socket_path: Union[str, os.PathLike],
        *,
        run_id: bytes,
        session_epoch: int,
        trade_date: int,
        history_coverage: HistoryCoverageInfo,
        timeout: Optional[float] = 1.0,
        start_canonical_apply_sequence: int = 1,
        batch_records: int = _DEFAULT_BATCH_RECORDS,
        native_library=None,
        native_library_path=None,
    ) -> "CertifiedTickHistoryReader":
        if native_library is not None and native_library_path is not None:
            raise ValueError(
                "native_library and native_library_path are mutually exclusive"
            )
        path = os.fspath(control_socket_path)
        if not path or "\x00" in path or not os.path.isabs(path):
            raise ValueError("control_socket_path must be absolute")
        if timeout is not None and (
            not isinstance(timeout, (int, float))
            or isinstance(timeout, bool)
            or not math.isfinite(timeout)
            or timeout <= 0
            or timeout > 86_400
        ):
            raise ValueError("timeout must be positive, at most 86400, or None")
        if not isinstance(history_coverage, HistoryCoverageInfo):
            raise TypeError("history_coverage must be HistoryCoverageInfo")
        if not history_coverage.coverage_from_open:
            raise UnavailableError(
                "CERTIFIED Tick history requires verified from-open coverage"
            )
        if (
            not isinstance(start_canonical_apply_sequence, int)
            or isinstance(start_canonical_apply_sequence, bool)
            or start_canonical_apply_sequence <= 0
            or start_canonical_apply_sequence > _UINT64_MAX
        ):
            raise ValueError(
                "start_canonical_apply_sequence must be a positive uint64"
            )
        if (
            not isinstance(batch_records, int)
            or isinstance(batch_records, bool)
            or batch_records <= 0
            or batch_records > _MAX_BATCH_RECORDS
        ):
            raise ValueError(
                f"batch_records must be in [1, {_MAX_BATCH_RECORDS}]"
            )
        expected = _expected_session(run_id, session_epoch, trade_date)
        if (
            history_coverage.identity != SessionIdentity(run_id, session_epoch)
            or history_coverage.trade_date != trade_date
        ):
            raise ValueError("history_coverage belongs to another session")
        library = (
            native_library
            if native_library is not None
            else load_native_library(native_library_path)
        )
        _bind_library(library)
        handle = ctypes.c_void_p()
        system_error = ctypes.c_int()
        timeout_ms = 0 if timeout is None else max(1, int(timeout * 1000))
        code = library.l2flow_certified_tick_history_reader_open_v1(
            os.fsencode(path),
            ctypes.byref(expected),
            timeout_ms,
            ctypes.byref(handle),
            ctypes.byref(system_error),
        )
        if code != _OPEN_OK or not handle.value:
            raise CertifiedTickHistoryError(
                "open", code, system_error_number=system_error.value
            )
        try:
            native_session = _SessionC()
            read = library.l2flow_certified_tick_history_reader_session_v1(
                handle, ctypes.byref(native_session)
            )
            if read != _READ_OK:
                raise CertifiedTickHistoryError("session", read)
            session = _session_from_c(native_session, history_coverage)
            if (
                session.run_id != run_id
                or session.session_epoch != session_epoch
                or session.trade_date != trade_date
                or start_canonical_apply_sequence
                > session.tick_capacity + 1
            ):
                raise WireFormatError(
                    "CERTIFIED Tick mapping has the wrong session or cursor"
                )
            return cls(
                library,
                handle,
                session,
                start_canonical_apply_sequence=(
                    start_canonical_apply_sequence
                ),
                batch_records=batch_records,
            )
        except BaseException:
            library.l2flow_certified_tick_history_reader_close_v1(handle)
            raise

    @property
    def session(self) -> CertifiedTickHistorySession:
        return self._session

    @property
    def history_coverage(self) -> HistoryCoverageInfo:
        return self._session.history_coverage

    @property
    def next_canonical_apply_sequence(self) -> int:
        with self._lock:
            return self._next_sequence

    @property
    def closed(self) -> bool:
        return self._closed

    def _require_open(self) -> None:
        if self._closed:
            raise CertifiedTickHistoryError(
                "reader_closed", _READ_INVALID_ARGUMENT
            )

    def status(self) -> CertifiedTickHistoryStatus:
        with self._lock:
            self._require_open()
            status = self._read_status_locked()
            self._last_status = status
            return status

    def _read_status_locked(self) -> CertifiedTickHistoryStatus:
        value = _StatusC()
        code = self._library.l2flow_certified_tick_history_reader_status_v1(
            self._handle, ctypes.byref(value)
        )
        if code == _READ_INCONSISTENT:
            raise InconsistentReadError(
                "CERTIFIED Tick status raced its producer"
            )
        if code != _READ_OK:
            raise CertifiedTickHistoryError("status", code)
        return _status_from_c(value, self._session)

    def read_batch(self) -> CertifiedTickHistoryBatch:
        with self._lock:
            self._require_open()
            first = self._next_sequence
            # Once caught up, poll only the 96-byte coherent status cut.  This
            # avoids allocating the default 2 MiB owned output array on every
            # empty tail poll.  Backlog reads remain single-call, full-batch
            # copies after the first observed status.
            known = self._last_status
            natural_capacity_tail = (
                first == self._session.tick_capacity + 1
            )
            if natural_capacity_tail:
                # There is no slot at capacity + 1.  Only the native reader
                # can decide whether this is the natural cursor after a full
                # journal (ACTIVE/COMPLETE/FAILED) or an invalid future seek.
                request_records = 1
            else:
                if (
                    known is None
                    or first > known.canonical_apply_frontier
                ):
                    known = self._read_status_locked()
                    self._last_status = known
                    if first > known.canonical_apply_frontier:
                        if known.state is CertifiedTickHistoryState.FAILED:
                            raise CertifiedTickHistoryProducerFailedError(
                                known
                            )
                        empty_rows = (_SlotC * 0)()
                        return CertifiedTickHistoryBatch(
                            empty_rows,
                            0,
                            first_sequence=first,
                            next_sequence=first,
                            status=known,
                            session=self._session,
                        )
                available = known.canonical_apply_frontier - first + 1
                request_records = min(self._batch_records, available)
            rows = (_SlotC * request_records)()
            result = _ReadResultC()
            code = self._library.l2flow_certified_tick_history_reader_read_v1(
                self._handle,
                first,
                rows,
                request_records,
                ctypes.byref(result),
            )
            if code == _READ_INCONSISTENT:
                raise InconsistentReadError(
                    "CERTIFIED Tick batch raced its producer"
                )
            if code in (_READ_INVALID_ARGUMENT, _READ_CORRUPT):
                raise CertifiedTickHistoryError("read", code)
            if (
                result.result_schema_version != _RESULT_SCHEMA_VERSION
                or result.result_bytes != ctypes.sizeof(_ReadResultC)
                or any(result.reserved)
                or result.records_written > request_records
                or result.next_canonical_apply_sequence
                != first + result.records_written
            ):
                raise WireFormatError("invalid CERTIFIED Tick read result ABI")
            status = _status_from_c(result.status, self._session)
            self._last_status = status
            if code == _READ_PRODUCER_FAILED:
                if (
                    result.records_written != 0
                    or status.state
                    is not CertifiedTickHistoryState.FAILED
                    or first <= status.canonical_apply_frontier
                ):
                    raise WireFormatError(
                        "producer-failed result lacks FAILED status"
                    )
                raise CertifiedTickHistoryProducerFailedError(status)
            if code == _READ_OUT_OF_RANGE:
                if result.records_written != 0:
                    raise WireFormatError(
                        "out-of-range CERTIFIED Tick read returned rows"
                    )
                raise CertifiedTickHistoryCapacityError(
                    "CERTIFIED Tick cursor exceeds fixed journal capacity"
                )
            if code not in (
                _READ_OK,
                _READ_NOT_YET_PUBLISHED,
                _READ_END_OF_STREAM,
            ):
                raise CertifiedTickHistoryError("read", code)
            if code == _READ_OK and result.records_written == 0:
                raise WireFormatError(
                    "successful CERTIFIED Tick read returned no rows"
                )
            if code != _READ_OK and result.records_written != 0:
                raise WireFormatError(
                    "terminal/idle CERTIFIED Tick read returned rows"
                )
            if (
                code == _READ_OK
                and result.next_canonical_apply_sequence
                > status.canonical_apply_frontier + 1
            ):
                raise WireFormatError(
                    "CERTIFIED Tick rows exceed the coherent frontier"
                )
            if (
                code == _READ_NOT_YET_PUBLISHED
                and status.state is not CertifiedTickHistoryState.ACTIVE
            ) or (
                code == _READ_END_OF_STREAM
                and status.state is not CertifiedTickHistoryState.COMPLETE
            ) or (
                code
                in (_READ_NOT_YET_PUBLISHED, _READ_END_OF_STREAM)
                and first <= status.canonical_apply_frontier
            ):
                raise WireFormatError(
                    "CERTIFIED Tick read result disagrees with lifecycle"
                )
            batch = CertifiedTickHistoryBatch(
                rows,
                result.records_written,
                first_sequence=first,
                next_sequence=result.next_canonical_apply_sequence,
                status=status,
                session=self._session,
            )
            self._next_sequence = result.next_canonical_apply_sequence
            return batch

    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self._library.l2flow_certified_tick_history_reader_close_v1(
                    self._handle
                )
                self._handle = ctypes.c_void_p()
                self._closed = True

    def __enter__(self) -> "CertifiedTickHistoryReader":
        self._require_open()
        return self

    def __exit__(self, _type, _value, _traceback) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except BaseException:
            pass


def open_certified_tick_history(
    control_socket_path: Union[str, os.PathLike],
    *,
    expected_session: SessionInfo,
    history_coverage: HistoryCoverageInfo,
    **kwargs,
) -> CertifiedTickHistoryReader:
    if not isinstance(expected_session, SessionInfo):
        raise TypeError("expected_session must be SessionInfo")
    if not isinstance(history_coverage, HistoryCoverageInfo):
        raise TypeError("history_coverage must be HistoryCoverageInfo")
    if not expected_session.coverage_from_open:
        raise UnavailableError(
            "CERTIFIED Tick history requires from-open coverage"
        )
    if not expected_session.certified_prefix_valid:
        raise UnavailableError(
            "FAST session does not advertise a valid CERTIFIED prefix"
        )
    if (
        history_coverage.coverage_kind is not TemporalCoverageKind.FROM_OPEN
        or history_coverage.identity != expected_session.identity
        or history_coverage.trade_date != expected_session.trade_date
    ):
        raise UnavailableError(
            "FAST session lacks matching from-open history coverage"
        )
    return CertifiedTickHistoryReader.connect(
        control_socket_path,
        run_id=expected_session.run_id,
        session_epoch=expected_session.session_epoch,
        trade_date=expected_session.trade_date,
        history_coverage=history_coverage,
        **kwargs,
    )


__all__ = [
    "CertifiedTick",
    "CertifiedTickHistoryBatch",
    "CertifiedTickHistoryCapacityError",
    "CertifiedTickHistoryError",
    "CertifiedTickHistoryFailure",
    "CertifiedTickHistoryProducerFailedError",
    "CertifiedTickHistoryReader",
    "CertifiedTickHistorySession",
    "CertifiedTickHistoryState",
    "CertifiedTickHistoryStatus",
    "open_certified_tick_history",
]
