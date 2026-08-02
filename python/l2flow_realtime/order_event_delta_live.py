"""Low-latency, fail-closed reader for the live order-event delta ring.

The native reader owns the stateful sequence cursor.  This module deliberately
does not expose an API for selecting or skipping to an arbitrary sequence:
every reader starts at one, and overrun is terminal.  It provides no history
catch-up, WAL, crash recovery, or Parquet writing.  Direct-fd attachment lives
here; the validated process control connector is
``order_event_delta_control.open_live_order_events``.

One native producer commit contains all rows caused by a source tick, but a
finite Python batch can split that committed tick.  Callers needing tick-level
transactional application must group on ``tick_stream_sequence`` and retain
the trailing group until a later tick is seen or the batch drains the published
prefix.  A Python ``read_batch`` boundary is not itself a tick boundary.
"""

from __future__ import annotations

import ctypes
import os
import threading
from dataclasses import dataclass
from enum import IntEnum
from typing import TYPE_CHECKING, Iterator, Optional

from .instrument_derived_event_history import _DerivedEventRowC


if TYPE_CHECKING:
    from .order_event_delta_control import (
        LiveOrderEventDeltaControlSnapshot,
    )


_OK = 0
_INVALID_ARGUMENT = 2
_UNAVAILABLE = 8
_INCONSISTENT_READ = 9
_OVERRUN = 10
_RESULT_SCHEMA_VERSION = 1
_ROW_BYTES = 320
_SESSION_BYTES = 64
_RESULT_BYTES = 80
_RING_HEADER_BYTES = 4096
_RING_SLOT_BYTES = 384
_SYSTEM_PAGE_BYTES = 4096
_DEFAULT_BATCH_RECORDS = 4096
_MAX_BATCH_RECORDS = 65_536
_UINT64_MAX = (1 << 64) - 1


class LiveOrderEventDeltaProducerState(IntEnum):
    INITIALIZING = 1
    ACTIVE = 2
    DRAINING = 3
    STOPPED_CLEAN = 4
    FAILED = 5


class LiveOrderEventDeltaTemporalCoverage(IntEnum):
    """Beginning of the ring's dense local source-tick sequence."""

    FROM_MARKET_OPEN = 1
    FROM_PROCESS_START = 2


class LiveOrderEventDeltaStreamQuality(IntEnum):
    """Local stream contract; it does not claim native gap backfill."""

    LOCAL_TICK_STREAM_CONTIGUOUS = 1


class _LiveSessionC(ctypes.Structure):
    _fields_ = [
        ("run_id", ctypes.c_uint8 * 16),
        ("session_epoch", ctypes.c_uint64),
        ("trade_date", ctypes.c_uint32),
        ("temporal_coverage", ctypes.c_uint32),
        ("ring_capacity", ctypes.c_uint64),
        ("total_mapping_bytes", ctypes.c_uint64),
        ("stream_quality", ctypes.c_uint32),
        ("reserved0", ctypes.c_uint32),
        ("reserved", ctypes.c_uint8 * 8),
    ]


class _LiveReadResultC(ctypes.Structure):
    _fields_ = [
        ("result_schema_version", ctypes.c_uint32),
        ("result_bytes", ctypes.c_uint32),
        ("records_written", ctypes.c_uint64),
        ("next_sequence", ctypes.c_uint64),
        ("observed_sequence", ctypes.c_uint64),
        ("published_event_sequence", ctypes.c_uint64),
        ("consumed_source_tick_sequence", ctypes.c_uint64),
        ("heartbeat_monotonic_ns", ctypes.c_uint64),
        ("producer_state", ctypes.c_uint32),
        ("header_flags", ctypes.c_uint32),
        ("reserved", ctypes.c_uint8 * 16),
    ]


assert ctypes.sizeof(_DerivedEventRowC) == _ROW_BYTES
assert ctypes.sizeof(_LiveSessionC) == _SESSION_BYTES
assert ctypes.sizeof(_LiveReadResultC) == _RESULT_BYTES


@dataclass(frozen=True, slots=True)
class LiveOrderEventDeltaSession:
    """Exact identity required to attach to one live ring mapping."""

    run_id: bytes
    session_epoch: int
    trade_date: int
    ring_capacity: int
    total_mapping_bytes: int
    temporal_coverage: LiveOrderEventDeltaTemporalCoverage = (
        LiveOrderEventDeltaTemporalCoverage.FROM_MARKET_OPEN
    )
    stream_quality: LiveOrderEventDeltaStreamQuality = (
        LiveOrderEventDeltaStreamQuality.LOCAL_TICK_STREAM_CONTIGUOUS
    )

    def __post_init__(self) -> None:
        if (
            not isinstance(self.run_id, bytes)
            or len(self.run_id) != 16
            or not any(self.run_id)
        ):
            raise ValueError("run_id must be exactly 16 nonzero bytes")
        for name, value, maximum in (
            ("session_epoch", self.session_epoch, _UINT64_MAX),
            ("trade_date", self.trade_date, (1 << 32) - 1),
            ("ring_capacity", self.ring_capacity, _UINT64_MAX),
            (
                "total_mapping_bytes",
                self.total_mapping_bytes,
                _UINT64_MAX,
            ),
        ):
            if (
                not isinstance(value, int)
                or isinstance(value, bool)
                or value <= 0
                or value > maximum
            ):
                raise ValueError(f"{name} is outside its ABI domain")
        logical_bytes = (
            _RING_HEADER_BYTES + self.ring_capacity * _RING_SLOT_BYTES
        )
        expected_mapping_bytes = (
            logical_bytes + _SYSTEM_PAGE_BYTES - 1
        ) & ~(_SYSTEM_PAGE_BYTES - 1)
        if self.total_mapping_bytes != expected_mapping_bytes:
            raise ValueError(
                "total_mapping_bytes does not match ring_capacity"
            )
        try:
            temporal_coverage = LiveOrderEventDeltaTemporalCoverage(
                self.temporal_coverage
            )
        except (TypeError, ValueError) as error:
            raise ValueError("temporal_coverage is invalid") from error
        try:
            stream_quality = LiveOrderEventDeltaStreamQuality(
                self.stream_quality
            )
        except (TypeError, ValueError) as error:
            raise ValueError("stream_quality is invalid") from error
        object.__setattr__(
            self, "temporal_coverage", temporal_coverage
        )
        object.__setattr__(self, "stream_quality", stream_quality)

    def _to_c(self) -> _LiveSessionC:
        result = _LiveSessionC()
        for index, value in enumerate(self.run_id):
            result.run_id[index] = value
        result.session_epoch = self.session_epoch
        result.trade_date = self.trade_date
        result.ring_capacity = self.ring_capacity
        result.total_mapping_bytes = self.total_mapping_bytes
        result.temporal_coverage = int(self.temporal_coverage)
        result.stream_quality = int(self.stream_quality)
        return result


@dataclass(frozen=True, slots=True)
class LiveOrderEventDeltaReadMetadata:
    records_written: int
    next_sequence: int
    observed_sequence: int
    published_event_sequence: int
    consumed_source_tick_sequence: int
    heartbeat_monotonic_ns: int
    producer_state: LiveOrderEventDeltaProducerState
    header_flags: int


class LiveOrderEventDeltaError(RuntimeError):
    def __init__(
        self,
        operation: str,
        code: int,
        *,
        system_error_number: int = 0,
        metadata: Optional[LiveOrderEventDeltaReadMetadata] = None,
        native_name: Optional[str] = None,
    ) -> None:
        self.operation = operation
        self.code = code
        self.system_error_number = system_error_number
        self.metadata = metadata
        self.native_name = native_name
        detail = f"{operation} failed with live event-delta code {code}"
        if native_name:
            detail += f" ({native_name})"
        if system_error_number:
            detail += (
                f": [errno {system_error_number}] "
                f"{os.strerror(system_error_number)}"
            )
        super().__init__(detail)


class LiveOrderEventDeltaOverrunError(LiveOrderEventDeltaError):
    """The requested dense prefix has been overwritten; reader is terminal."""


class LiveOrderEventDeltaUnavailableError(LiveOrderEventDeltaError):
    """The producer or the already-failed reader is not readable."""


class LiveOrderEventDeltaWireError(LiveOrderEventDeltaError):
    """The C ABI returned a structurally impossible result."""


def _metadata_from_c(
    value: _LiveReadResultC,
) -> LiveOrderEventDeltaReadMetadata:
    if (
        value.result_schema_version != _RESULT_SCHEMA_VERSION
        or value.result_bytes != _RESULT_BYTES
        or any(value.reserved)
        or value.next_sequence <= 0
        or value.header_flags & ~1
    ):
        raise LiveOrderEventDeltaWireError(
            "read",
            _INCONSISTENT_READ,
            native_name="invalid_result_abi",
        )
    try:
        state = LiveOrderEventDeltaProducerState(value.producer_state)
    except ValueError as error:
        raise LiveOrderEventDeltaWireError(
            "read",
            _INCONSISTENT_READ,
            native_name="invalid_producer_state",
        ) from error
    return LiveOrderEventDeltaReadMetadata(
        records_written=value.records_written,
        next_sequence=value.next_sequence,
        observed_sequence=value.observed_sequence,
        published_event_sequence=value.published_event_sequence,
        consumed_source_tick_sequence=(
            value.consumed_source_tick_sequence
        ),
        heartbeat_monotonic_ns=value.heartbeat_monotonic_ns,
        producer_state=state,
        header_flags=value.header_flags,
    )


class LiveOrderEventDeltaBatch:
    """Owned contiguous 320-byte rows plus the native cursor/status cut.

    ``buffer`` is a stable read-only byte memoryview. Accessing it constructs
    no per-record Python objects. ``row()`` is provided for cold-path
    inspection and returns a copied ctypes row.
    """

    __slots__ = ("_rows", "_bytes", "metadata")

    def __init__(
        self,
        rows,
        metadata: LiveOrderEventDeltaReadMetadata,
    ) -> None:
        self._rows = rows
        byte_count = metadata.records_written * _ROW_BYTES
        byte_array_type = ctypes.c_uint8 * (len(rows) * _ROW_BYTES)
        self._bytes = byte_array_type.from_buffer(rows)
        self.metadata = metadata
        if byte_count > len(self._bytes):
            raise ValueError("record count exceeds owned native buffer")

    def __len__(self) -> int:
        return self.metadata.records_written

    @property
    def drains_published_prefix(self) -> bool:
        """Whether this batch reached a committed source-tick boundary.

        The producer advances its public prefix only after a complete source
        tick, so ``True`` proves any trailing row group in this batch is
        complete. ``False`` means a finite output buffer may have split that
        tick and the caller must retain the trailing tick-stream group.
        """

        return (
            self.metadata.next_sequence
            == self.metadata.published_event_sequence + 1
        )

    @property
    def buffer(self) -> memoryview:
        byte_count = len(self) * _ROW_BYTES
        return memoryview(self._bytes).cast("B")[:byte_count].toreadonly()

    def row(self, index: int) -> _DerivedEventRowC:
        if not isinstance(index, int) or isinstance(index, bool):
            raise TypeError("index must be an integer")
        if index < 0:
            index += len(self)
        if index < 0 or index >= len(self):
            raise IndexError(index)
        return _DerivedEventRowC.from_buffer_copy(
            self.buffer,
            index * _ROW_BYTES,
        )


def _bind_library(library) -> None:
    if getattr(library, "_l2flow_order_event_delta_bound_v1", False):
        return
    try:
        open_ = library.l2flow_order_event_delta_reader_open_v1
        close = library.l2flow_order_event_delta_reader_close_v1
        read = library.l2flow_order_event_delta_reader_read_v1
        session = library.l2flow_order_event_delta_reader_session_v1
        state = library.l2flow_order_event_delta_reader_state_v1
        error_name = library.l2flow_order_event_delta_error_name_v1
    except AttributeError as error:
        raise LiveOrderEventDeltaUnavailableError(
            "bind",
            _UNAVAILABLE,
            native_name="missing_C_ABI_symbol",
        ) from error

    handle = ctypes.c_void_p
    open_.argtypes = [
        ctypes.c_int,
        ctypes.POINTER(_LiveSessionC),
        ctypes.POINTER(handle),
        ctypes.POINTER(ctypes.c_int),
    ]
    open_.restype = ctypes.c_int
    close.argtypes = [handle]
    close.restype = None
    read.argtypes = [
        handle,
        ctypes.POINTER(_DerivedEventRowC),
        ctypes.c_uint64,
        ctypes.POINTER(_LiveReadResultC),
    ]
    read.restype = ctypes.c_int
    session.argtypes = [handle, ctypes.POINTER(_LiveSessionC)]
    session.restype = ctypes.c_int
    state.argtypes = [handle, ctypes.POINTER(ctypes.c_uint32)]
    state.restype = ctypes.c_int
    error_name.argtypes = [ctypes.c_int]
    error_name.restype = ctypes.c_char_p
    setattr(library, "_l2flow_order_event_delta_bound_v1", True)


def _native_error_name(library, code: int) -> Optional[str]:
    try:
        value = library.l2flow_order_event_delta_error_name_v1(code)
        return None if not value else value.decode("ascii", "replace")
    except Exception:
        return None


class LiveOrderEventDeltaReader:
    """Stateful live ring reader with explicit descriptor ownership.

    The native open duplicates ``fd``. By default the caller keeps ownership
    and must close it. If ``take_fd_ownership=True``, this method owns the
    supplied descriptor immediately and closes it whether native open succeeds
    or fails; the returned reader uses only the native duplicate.
    """

    __slots__ = (
        "_library",
        "_handle",
        "_session",
        "_batch_records",
        "_next_sequence",
        "_failed",
        "_lock",
        "_control_snapshot",
    )

    def __init__(
        self,
        library,
        handle: ctypes.c_void_p,
        session: LiveOrderEventDeltaSession,
        batch_records: int,
        control_snapshot: Optional[
            "LiveOrderEventDeltaControlSnapshot"
        ] = None,
    ) -> None:
        self._library = library
        self._handle = handle
        self._session = session
        self._batch_records = batch_records
        self._next_sequence = 1
        self._failed = False
        self._lock = threading.RLock()
        self._control_snapshot = control_snapshot

    @classmethod
    def open(
        cls,
        library,
        fd: int,
        session: LiveOrderEventDeltaSession,
        *,
        batch_records: int = _DEFAULT_BATCH_RECORDS,
        take_fd_ownership: bool = False,
        _control_snapshot: Optional[
            "LiveOrderEventDeltaControlSnapshot"
        ] = None,
    ) -> "LiveOrderEventDeltaReader":
        if (
            not isinstance(fd, int)
            or isinstance(fd, bool)
            or fd < 0
        ):
            raise ValueError("fd must be a nonnegative descriptor")
        if not isinstance(session, LiveOrderEventDeltaSession):
            raise TypeError("session must be LiveOrderEventDeltaSession")
        cls._validate_batch_records(batch_records)
        _bind_library(library)
        native_session = session._to_c()
        handle = ctypes.c_void_p()
        system_error = ctypes.c_int()
        code: Optional[int] = None
        close_error: Optional[OSError] = None
        try:
            code = int(
                library.l2flow_order_event_delta_reader_open_v1(
                    fd,
                    ctypes.byref(native_session),
                    ctypes.byref(handle),
                    ctypes.byref(system_error),
                )
            )
        finally:
            if take_fd_ownership:
                try:
                    os.close(fd)
                except OSError as error:
                    close_error = error
        if close_error is not None:
            if handle.value:
                library.l2flow_order_event_delta_reader_close_v1(
                    handle
                )
            raise close_error
        if code != _OK or not handle.value:
            if handle.value:
                library.l2flow_order_event_delta_reader_close_v1(
                    handle
                )
            raise LiveOrderEventDeltaError(
                "open",
                _UNAVAILABLE if code is None else code,
                system_error_number=system_error.value,
                native_name=_native_error_name(
                    library, _UNAVAILABLE if code is None else code
                ),
            )
        return cls(
            library,
            handle,
            session,
            batch_records,
            control_snapshot=_control_snapshot,
        )

    @classmethod
    def open_library(
        cls,
        library_path: os.PathLike[str] | str,
        fd: int,
        session: LiveOrderEventDeltaSession,
        **kwargs,
    ) -> "LiveOrderEventDeltaReader":
        library = ctypes.CDLL(os.fspath(library_path), use_errno=True)
        return cls.open(library, fd, session, **kwargs)

    @staticmethod
    def _validate_batch_records(value: int) -> None:
        if (
            not isinstance(value, int)
            or isinstance(value, bool)
            or value <= 0
            or value > _MAX_BATCH_RECORDS
        ):
            raise ValueError(
                f"batch_records must be in [1, {_MAX_BATCH_RECORDS}]"
            )

    @property
    def session(self) -> LiveOrderEventDeltaSession:
        return self._session

    @property
    def control_snapshot(
        self,
    ) -> Optional["LiveOrderEventDeltaControlSnapshot"]:
        """Validated GET_SESSION snapshot, or ``None`` for direct-fd open."""

        return self._control_snapshot

    @property
    def next_sequence(self) -> int:
        with self._lock:
            return self._next_sequence

    @property
    def closed(self) -> bool:
        with self._lock:
            return not bool(self._handle and self._handle.value)

    @property
    def failed(self) -> bool:
        with self._lock:
            return self._failed

    def _require_readable(self) -> None:
        if self.closed:
            raise LiveOrderEventDeltaUnavailableError(
                "read", _UNAVAILABLE, native_name="reader_closed"
            )
        if self._failed:
            raise LiveOrderEventDeltaUnavailableError(
                "read",
                _UNAVAILABLE,
                native_name="reader_fail_closed",
            )

    def _read_native(
        self,
        capacity: int,
    ) -> tuple[object, LiveOrderEventDeltaReadMetadata]:
        with self._lock:
            self._require_readable()
            row_array_type = _DerivedEventRowC * capacity
            rows = row_array_type()
            result = _LiveReadResultC()
            row_pointer = None if capacity == 0 else rows
            code = int(
                self._library.l2flow_order_event_delta_reader_read_v1(
                    self._handle,
                    row_pointer,
                    capacity,
                    ctypes.byref(result),
                )
            )
            try:
                metadata = _metadata_from_c(result)
            except LiveOrderEventDeltaWireError:
                self._failed = True
                raise
            if metadata.records_written > capacity:
                self._failed = True
                raise LiveOrderEventDeltaWireError(
                    "read",
                    _INCONSISTENT_READ,
                    metadata=metadata,
                    native_name="record_count_exceeds_capacity",
                )
            if code == _OK:
                expected_next = (
                    self._next_sequence + metadata.records_written
                )
                if (
                    metadata.next_sequence != expected_next
                    or metadata.published_event_sequence
                        < metadata.next_sequence - 1
                    or metadata.header_flags != 0
                    or metadata.producer_state
                        not in (
                            LiveOrderEventDeltaProducerState.ACTIVE,
                            LiveOrderEventDeltaProducerState.DRAINING,
                            LiveOrderEventDeltaProducerState.STOPPED_CLEAN,
                        )
                ):
                    self._failed = True
                    raise LiveOrderEventDeltaWireError(
                        "read",
                        _INCONSISTENT_READ,
                        metadata=metadata,
                        native_name="incoherent_success_metadata",
                    )
                self._next_sequence = metadata.next_sequence
                return rows, metadata

            if code not in (_INVALID_ARGUMENT,):
                self._failed = True
            error_type = LiveOrderEventDeltaError
            if code == _OVERRUN:
                error_type = LiveOrderEventDeltaOverrunError
            elif code == _UNAVAILABLE:
                error_type = LiveOrderEventDeltaUnavailableError
            raise error_type(
                "read",
                code,
                metadata=metadata,
                native_name=_native_error_name(self._library, code),
            )

    def poll(self) -> LiveOrderEventDeltaReadMetadata:
        """Read producer progress without consuming event rows."""

        _, metadata = self._read_native(0)
        return metadata

    def read_batch(
        self,
        maximum_records: Optional[int] = None,
    ) -> LiveOrderEventDeltaBatch:
        """Read at most ``maximum_records`` into one owned native buffer.

        This limit is a transport boundary, not a source-tick boundary. Use
        each row's ``tick_stream_sequence`` together with
        :attr:`LiveOrderEventDeltaBatch.drains_published_prefix` when atomic
        tick-level application is required.
        """

        capacity = (
            self._batch_records
            if maximum_records is None
            else maximum_records
        )
        self._validate_batch_records(capacity)
        rows, metadata = self._read_native(capacity)
        return LiveOrderEventDeltaBatch(rows, metadata)

    def read_available(
        self,
        *,
        maximum_batches: int = 1,
        maximum_records: Optional[int] = None,
    ) -> Iterator[LiveOrderEventDeltaBatch]:
        """Yield bounded batches until empty or ``maximum_batches``."""

        if (
            not isinstance(maximum_batches, int)
            or isinstance(maximum_batches, bool)
            or maximum_batches <= 0
        ):
            raise ValueError("maximum_batches must be positive")
        for _ in range(maximum_batches):
            batch = self.read_batch(maximum_records)
            if not batch:
                return
            yield batch

    def producer_state(self) -> LiveOrderEventDeltaProducerState:
        with self._lock:
            self._require_readable()
            value = ctypes.c_uint32()
            code = int(
                self._library.l2flow_order_event_delta_reader_state_v1(
                    self._handle, ctypes.byref(value)
                )
            )
            if code != _OK:
                if code != _INVALID_ARGUMENT:
                    self._failed = True
                raise LiveOrderEventDeltaError(
                    "state",
                    code,
                    native_name=_native_error_name(
                        self._library, code
                    ),
                )
            try:
                return LiveOrderEventDeltaProducerState(value.value)
            except ValueError as error:
                self._failed = True
                raise LiveOrderEventDeltaWireError(
                    "state",
                    _INCONSISTENT_READ,
                    native_name="invalid_producer_state",
                ) from error

    def close(self) -> None:
        with self._lock:
            if not self.closed:
                self._library.l2flow_order_event_delta_reader_close_v1(
                    self._handle
                )
                self._handle = ctypes.c_void_p()

    def __enter__(self) -> "LiveOrderEventDeltaReader":
        with self._lock:
            self._require_readable()
            return self

    def __exit__(self, _type, _value, _traceback) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass


__all__ = [
    "LiveOrderEventDeltaBatch",
    "LiveOrderEventDeltaError",
    "LiveOrderEventDeltaOverrunError",
    "LiveOrderEventDeltaProducerState",
    "LiveOrderEventDeltaReadMetadata",
    "LiveOrderEventDeltaReader",
    "LiveOrderEventDeltaSession",
    "LiveOrderEventDeltaStreamQuality",
    "LiveOrderEventDeltaTemporalCoverage",
    "LiveOrderEventDeltaUnavailableError",
    "LiveOrderEventDeltaWireError",
]
