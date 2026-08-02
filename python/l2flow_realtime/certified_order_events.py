"""Append-only CERTIFIED order-event History plus low-latency live tail.

One reader starts at a caller-selected dense event sequence (one by default),
drains the already-published full-day journal, and then keeps the same cursor
for newly appended rows. Empty batches are temporary tail-idle results, not
EOF. Every returned row is bounded by the coherent Tick/Event canonical
frontier, including the online-recovery promotion boundary.
"""

from __future__ import annotations

import ctypes
import os
import threading
from dataclasses import dataclass
from enum import IntEnum
from typing import Iterator, Union

from .instrument_derived_event_history import (
    InstrumentDerivedEvent,
    _DerivedEventRowC,
    _event_from_c,
)
from .models import L2FlowRealtimeError, SessionInfo, UnavailableError, WireFormatError
from .native import load_native_library


_OPEN_OK = 0
_READ_OK = 0
_READ_OUT_OF_RANGE = 3
_COVERAGE_FROM_OPEN = 1 << 0
_STARTUP_PREFIX_RECOVERED = 1 << 1
_KNOWN_COVERAGE_FLAGS = _COVERAGE_FROM_OPEN | _STARTUP_PREFIX_RECOVERED
_STATUS_SCHEMA_VERSION = 1
_RESULT_SCHEMA_VERSION = 1
_DEFAULT_BATCH_RECORDS = 4096
_MAX_BATCH_RECORDS = 65_536
_UINT64_MAX = (1 << 64) - 1


class CertifiedOrderEventState(IntEnum):
    DISABLED = 1
    NO_DATA = 2
    CONTIGUOUS = 3
    GAP_OPEN = 4
    CATCHING_UP = 5
    FROZEN_CONFLICT = 6
    FROZEN_RESOURCE = 7
    STOPPED = 8


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
        ("event_capacity", ctypes.c_uint64),
        ("trade_date", ctypes.c_uint32),
        ("coverage_flags", ctypes.c_uint32),
        ("reserved", ctypes.c_uint8 * 24),
    ]


class _StatusC(ctypes.Structure):
    _fields_ = [
        ("status_schema_version", ctypes.c_uint32),
        ("status_bytes", ctypes.c_uint32),
        ("coverage_flags", ctypes.c_uint32),
        ("certified_state", ctypes.c_uint32),
        ("tick_publish_tag", ctypes.c_uint64),
        ("tick_heartbeat_monotonic_ns", ctypes.c_uint64),
        ("tick_canonical_apply_frontier", ctypes.c_uint64),
        ("correction_epoch", ctypes.c_uint64),
        ("observed_native_message_count", ctypes.c_uint64),
        ("certified_tick_count", ctypes.c_uint64),
        ("exact_duplicate_message_count", ctypes.c_uint64),
        ("gap_opened_count", ctypes.c_uint64),
        ("gap_recovered_count", ctypes.c_uint64),
        ("conflicting_duplicate_count", ctypes.c_uint64),
        ("resource_exhaustion_count", ctypes.c_uint64),
        ("pending_token_count", ctypes.c_uint64),
        ("event_publish_tag", ctypes.c_uint64),
        ("event_heartbeat_monotonic_ns", ctypes.c_uint64),
        ("event_canonical_apply_frontier", ctypes.c_uint64),
        ("event_published_sequence", ctypes.c_uint64),
        ("event_generation", ctypes.c_uint64),
        ("shanghai_order_state_count", ctypes.c_uint64),
        ("shenzhen_order_state_count", ctypes.c_uint64),
        ("committed_mapping_bytes", ctypes.c_uint64),
        ("coherent_canonical_apply_frontier", ctypes.c_uint64),
        ("channel_state_count", ctypes.c_uint32),
        ("gap_open_channel_count", ctypes.c_uint32),
        ("catching_up_channel_count", ctypes.c_uint32),
        ("frozen_channel_count", ctypes.c_uint32),
        ("reserved", ctypes.c_uint8 * 32),
    ]


class _EnvelopeC(ctypes.Structure):
    _fields_ = [
        ("canonical_apply_sequence", ctypes.c_uint64),
        ("event", _DerivedEventRowC),
    ]


class _ReadResultC(ctypes.Structure):
    _fields_ = [
        ("result_schema_version", ctypes.c_uint32),
        ("result_bytes", ctypes.c_uint32),
        ("records_written", ctypes.c_uint64),
        ("next_event_sequence", ctypes.c_uint64),
        ("status", _StatusC),
        ("reserved", ctypes.c_uint8 * 32),
    ]


assert ctypes.sizeof(_ExpectedSessionC) == 32
assert ctypes.sizeof(_SessionC) == 64
assert ctypes.sizeof(_StatusC) == 232
assert ctypes.sizeof(_EnvelopeC) == 328
assert ctypes.sizeof(_ReadResultC) == 288


@dataclass(frozen=True, slots=True)
class CertifiedOrderEventSession:
    run_id: bytes
    session_epoch: int
    trade_date: int
    event_capacity: int
    coverage_flags: int

    @property
    def coverage_from_open(self) -> bool:
        return bool(self.coverage_flags & _COVERAGE_FROM_OPEN)

    @property
    def startup_prefix_recovered(self) -> bool:
        return bool(self.coverage_flags & _STARTUP_PREFIX_RECOVERED)


@dataclass(frozen=True, slots=True)
class CertifiedOrderEventStatus:
    coverage_flags: int
    state: CertifiedOrderEventState
    tick_publish_tag: int
    tick_heartbeat_monotonic_ns: int
    tick_canonical_apply_frontier: int
    correction_epoch: int
    observed_native_message_count: int
    certified_tick_count: int
    exact_duplicate_message_count: int
    gap_opened_count: int
    gap_recovered_count: int
    conflicting_duplicate_count: int
    resource_exhaustion_count: int
    pending_token_count: int
    event_publish_tag: int
    event_heartbeat_monotonic_ns: int
    event_canonical_apply_frontier: int
    event_published_sequence: int
    event_generation: int
    shanghai_order_state_count: int
    shenzhen_order_state_count: int
    committed_mapping_bytes: int
    coherent_canonical_apply_frontier: int
    channel_state_count: int
    gap_open_channel_count: int
    catching_up_channel_count: int
    frozen_channel_count: int

    @property
    def coverage_from_open(self) -> bool:
        return bool(self.coverage_flags & _COVERAGE_FROM_OPEN)

    @property
    def startup_prefix_recovered(self) -> bool:
        return bool(self.coverage_flags & _STARTUP_PREFIX_RECOVERED)


@dataclass(frozen=True, slots=True)
class CertifiedOrderEvent:
    canonical_apply_sequence: int
    event: InstrumentDerivedEvent


class CertifiedOrderEventError(L2FlowRealtimeError):
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
        detail = f"{operation} failed with certified Event code {code}"
        if system_error_number:
            detail += (
                f": [errno {system_error_number}] "
                f"{os.strerror(system_error_number)}"
            )
        super().__init__(detail)


class CertifiedOrderEventBatch:
    """Owned contiguous 328-byte envelopes plus one coherent status cut."""

    __slots__ = ("_rows", "_bytes", "_count", "next_event_sequence", "status")

    def __init__(
        self,
        rows,
        count: int,
        next_event_sequence: int,
        status: CertifiedOrderEventStatus,
    ) -> None:
        self._rows = rows
        self._count = count
        byte_type = ctypes.c_uint8 * (len(rows) * ctypes.sizeof(_EnvelopeC))
        self._bytes = byte_type.from_buffer(rows)
        self.next_event_sequence = next_event_sequence
        self.status = status

    def __len__(self) -> int:
        return self._count

    @property
    def buffer(self) -> memoryview:
        return memoryview(self._bytes)[: self._count * 328].toreadonly()

    def row(self, index: int) -> CertifiedOrderEvent:
        if not isinstance(index, int) or isinstance(index, bool):
            raise TypeError("index must be an integer")
        if index < 0:
            index += self._count
        if index < 0 or index >= self._count:
            raise IndexError(index)
        row = self._rows[index]
        return CertifiedOrderEvent(
            canonical_apply_sequence=row.canonical_apply_sequence,
            event=_event_from_c(row.event),
        )

    def __iter__(self) -> Iterator[CertifiedOrderEvent]:
        for index in range(self._count):
            yield self.row(index)


def _bind_library(library) -> None:
    if getattr(library, "_l2flow_certified_order_event_bound_v1", False):
        return
    handle = ctypes.c_void_p
    try:
        open_ = library.l2flow_certified_order_event_reader_open_v1
    except AttributeError as error:
        raise UnavailableError(
            "native library lacks CERTIFIED order-event reader V1"
        ) from error
    open_.argtypes = [
        ctypes.c_char_p,
        ctypes.POINTER(_ExpectedSessionC),
        ctypes.c_uint32,
        ctypes.POINTER(handle),
        ctypes.POINTER(ctypes.c_int),
    ]
    open_.restype = ctypes.c_int
    library.l2flow_certified_order_event_reader_close_v1.argtypes = [handle]
    library.l2flow_certified_order_event_reader_close_v1.restype = None
    library.l2flow_certified_order_event_reader_session_v1.argtypes = [
        handle,
        ctypes.POINTER(_SessionC),
    ]
    library.l2flow_certified_order_event_reader_session_v1.restype = ctypes.c_int
    library.l2flow_certified_order_event_reader_status_v1.argtypes = [
        handle,
        ctypes.POINTER(_StatusC),
    ]
    library.l2flow_certified_order_event_reader_status_v1.restype = ctypes.c_int
    library.l2flow_certified_order_event_reader_read_v1.argtypes = [
        handle,
        ctypes.c_uint64,
        ctypes.POINTER(_EnvelopeC),
        ctypes.c_size_t,
        ctypes.POINTER(_ReadResultC),
    ]
    library.l2flow_certified_order_event_reader_read_v1.restype = ctypes.c_int
    library._l2flow_certified_order_event_bound_v1 = True


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


def _coverage_valid(flags: int) -> bool:
    return (
        flags & ~_KNOWN_COVERAGE_FLAGS == 0
        and bool(flags & _COVERAGE_FROM_OPEN)
        and not (
            flags & _STARTUP_PREFIX_RECOVERED
            and not flags & _COVERAGE_FROM_OPEN
        )
    )


def _status_from_c(value: _StatusC) -> CertifiedOrderEventStatus:
    if (
        value.status_schema_version != _STATUS_SCHEMA_VERSION
        or value.status_bytes != ctypes.sizeof(_StatusC)
        or any(value.reserved)
        or not _coverage_valid(value.coverage_flags)
        or value.tick_publish_tag == 0
        or value.tick_publish_tag & 1
        or value.event_publish_tag == 0
        or value.event_publish_tag & 1
        or value.tick_heartbeat_monotonic_ns == 0
        or value.event_heartbeat_monotonic_ns == 0
        or value.certified_tick_count != value.tick_canonical_apply_frontier
        or value.event_generation != value.event_canonical_apply_frontier
        or value.coherent_canonical_apply_frontier
        != min(
            value.tick_canonical_apply_frontier,
            value.event_canonical_apply_frontier,
        )
    ):
        raise WireFormatError("invalid CERTIFIED order-event status ABI")
    try:
        state = CertifiedOrderEventState(value.certified_state)
    except ValueError as error:
        raise WireFormatError("invalid CERTIFIED order-event state") from error
    fields = {
        name: getattr(value, name)
        for name, _ctype in _StatusC._fields_
        if name
        not in {
            "status_schema_version",
            "status_bytes",
            "certified_state",
            "reserved",
        }
    }
    return CertifiedOrderEventStatus(state=state, **fields)


def _session_from_c(value: _SessionC) -> CertifiedOrderEventSession:
    flags = int(value.coverage_flags)
    if (
        not any(value.run_id)
        or value.session_epoch == 0
        or value.trade_date == 0
        or value.event_capacity == 0
        or any(value.reserved)
        or not _coverage_valid(flags)
    ):
        raise WireFormatError("invalid CERTIFIED order-event session ABI")
    return CertifiedOrderEventSession(
        run_id=bytes(value.run_id),
        session_epoch=value.session_epoch,
        trade_date=value.trade_date,
        event_capacity=value.event_capacity,
        coverage_flags=flags,
    )


class CertifiedOrderEventReader:
    """Stateful history-to-tail cursor over one CERTIFIED event session."""

    __slots__ = (
        "_library",
        "_handle",
        "_session",
        "_next_event_sequence",
        "_batch_records",
        "_lock",
        "_closed",
    )

    def __init__(
        self,
        library,
        handle,
        session: CertifiedOrderEventSession,
        *,
        start_event_sequence: int,
        batch_records: int,
    ) -> None:
        self._library = library
        self._handle = handle
        self._session = session
        self._next_event_sequence = start_event_sequence
        self._batch_records = batch_records
        self._lock = threading.Lock()
        self._closed = False

    @classmethod
    def connect(
        cls,
        control_socket_path: Union[str, os.PathLike],
        *,
        run_id: bytes,
        session_epoch: int,
        trade_date: int,
        timeout: float = 1.0,
        start_event_sequence: int = 1,
        batch_records: int = _DEFAULT_BATCH_RECORDS,
        native_library=None,
        native_library_path=None,
    ) -> "CertifiedOrderEventReader":
        if native_library is not None and native_library_path is not None:
            raise ValueError(
                "native_library and native_library_path are mutually exclusive"
            )
        path = os.fspath(control_socket_path)
        if not path or "\x00" in path or not os.path.isabs(path):
            raise ValueError("control_socket_path must be absolute")
        if (
            not isinstance(timeout, (int, float))
            or isinstance(timeout, bool)
            or timeout <= 0
            or timeout > 86_400
        ):
            raise ValueError("timeout must be in (0, 86400] seconds")
        if (
            not isinstance(start_event_sequence, int)
            or isinstance(start_event_sequence, bool)
            or start_event_sequence <= 0
            or start_event_sequence > _UINT64_MAX
        ):
            raise ValueError("start_event_sequence must be a positive uint64")
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
        library = (
            native_library
            if native_library is not None
            else load_native_library(native_library_path)
        )
        _bind_library(library)
        handle = ctypes.c_void_p()
        system_error = ctypes.c_int()
        timeout_ms = max(1, int(timeout * 1000))
        code = library.l2flow_certified_order_event_reader_open_v1(
            os.fsencode(path),
            ctypes.byref(expected),
            timeout_ms,
            ctypes.byref(handle),
            ctypes.byref(system_error),
        )
        if code != _OPEN_OK or not handle.value:
            raise CertifiedOrderEventError(
                "open",
                code,
                system_error_number=system_error.value,
            )
        try:
            native_session = _SessionC()
            read = library.l2flow_certified_order_event_reader_session_v1(
                handle, ctypes.byref(native_session)
            )
            if read != _READ_OK:
                raise CertifiedOrderEventError("session", read)
            session = _session_from_c(native_session)
            if (
                session.run_id != run_id
                or session.session_epoch != session_epoch
                or session.trade_date != trade_date
                or start_event_sequence > session.event_capacity
            ):
                raise WireFormatError(
                    "CERTIFIED event mapping has the wrong session or cursor"
                )
            return cls(
                library,
                handle,
                session,
                start_event_sequence=start_event_sequence,
                batch_records=batch_records,
            )
        except BaseException:
            library.l2flow_certified_order_event_reader_close_v1(handle)
            raise

    @property
    def session(self) -> CertifiedOrderEventSession:
        return self._session

    @property
    def next_event_sequence(self) -> int:
        return self._next_event_sequence

    @property
    def closed(self) -> bool:
        return self._closed

    def _require_open(self) -> None:
        if self._closed:
            raise CertifiedOrderEventError("reader_closed", _READ_OUT_OF_RANGE)

    def status(self) -> CertifiedOrderEventStatus:
        with self._lock:
            self._require_open()
            value = _StatusC()
            code = self._library.l2flow_certified_order_event_reader_status_v1(
                self._handle, ctypes.byref(value)
            )
            if code != _READ_OK:
                raise CertifiedOrderEventError("status", code)
            status = _status_from_c(value)
            if status.coverage_flags != self._session.coverage_flags:
                raise WireFormatError(
                    "CERTIFIED event coverage changed after attachment"
                )
            return status

    def read_batch(self) -> CertifiedOrderEventBatch:
        with self._lock:
            self._require_open()
            rows = (_EnvelopeC * self._batch_records)()
            result = _ReadResultC()
            code = self._library.l2flow_certified_order_event_reader_read_v1(
                self._handle,
                self._next_event_sequence,
                rows,
                self._batch_records,
                ctypes.byref(result),
            )
            if code != _READ_OK:
                raise CertifiedOrderEventError("read", code)
            if (
                result.result_schema_version != _RESULT_SCHEMA_VERSION
                or result.result_bytes != ctypes.sizeof(_ReadResultC)
                or any(result.reserved)
                or result.records_written > self._batch_records
                or result.next_event_sequence
                != self._next_event_sequence + result.records_written
            ):
                raise WireFormatError("invalid CERTIFIED event read result ABI")
            status = _status_from_c(result.status)
            if status.coverage_flags != self._session.coverage_flags:
                raise WireFormatError(
                    "CERTIFIED event coverage changed after attachment"
                )
            self._next_event_sequence = result.next_event_sequence
            return CertifiedOrderEventBatch(
                rows,
                result.records_written,
                result.next_event_sequence,
                status,
            )

    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self._library.l2flow_certified_order_event_reader_close_v1(
                    self._handle
                )
                self._handle = ctypes.c_void_p()
                self._closed = True

    def __enter__(self) -> "CertifiedOrderEventReader":
        self._require_open()
        return self

    def __exit__(self, _type, _value, _traceback) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except BaseException:
            pass


def open_certified_order_events(
    control_socket_path: Union[str, os.PathLike],
    *,
    expected_session: SessionInfo,
    **kwargs,
) -> CertifiedOrderEventReader:
    if not isinstance(expected_session, SessionInfo):
        raise TypeError("expected_session must be SessionInfo")
    if not expected_session.coverage_from_open:
        raise UnavailableError(
            "CERTIFIED event history requires from-open coverage"
        )
    if not expected_session.certified_prefix_valid:
        raise UnavailableError(
            "FAST session does not advertise a valid CERTIFIED prefix"
        )
    return CertifiedOrderEventReader.connect(
        control_socket_path,
        run_id=expected_session.run_id,
        session_epoch=expected_session.session_epoch,
        trade_date=expected_session.trade_date,
        **kwargs,
    )
