"""ctypes wrapper around the stable, Python-independent native reader C ABI."""

from __future__ import annotations

import ctypes
import ctypes.util
import os
import sys
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Sequence, Tuple, Union

from .models import (
    ClientClosedError,
    InconsistentReadError,
    InstrumentKey,
    InstrumentLookupStatus,
    LatestStatus,
    NativeReaderError,
    ServerState,
    SessionInfo,
    TickOverrunError,
    UnavailableError,
    WireFormatError,
)
from .wire import (
    INSTRUMENT_BYTES,
    KLINE_BYTES,
    SNAPSHOT_BYTES,
    TICK_BYTES,
    parse_instrument,
)


OK = 0
INVALID_ARGUMENT = 1
SYSTEM_ERROR = 2
ABI_MISMATCH = 3
LAYOUT_INVALID = 4
UNAVAILABLE = 5
OVERRUN = 6
BUFFER_TOO_SMALL = 7
INCONSISTENT_READ = 8
_UINT32_MAX = 0xFFFFFFFF
_UINT64_MAX = 0xFFFFFFFFFFFFFFFF
_C_SIZE_MAX = ctypes.c_size_t(-1).value


class _SessionInfoC(ctypes.Structure):
    _fields_ = [
        ("run_id", ctypes.c_uint8 * 16),
        ("session_epoch", ctypes.c_uint64),
        ("registry_version", ctypes.c_uint64),
        ("registry_sha256", ctypes.c_uint8 * 32),
        ("tick_ring_capacity", ctypes.c_uint64),
        ("tick_highest_published_sequence", ctypes.c_uint64),
        ("tick_contiguous_published_sequence", ctypes.c_uint64),
        ("kline_generation", ctypes.c_uint64),
        ("heartbeat_monotonic_ns", ctypes.c_uint64),
        ("trade_date", ctypes.c_uint32),
        ("server_state", ctypes.c_uint32),
        ("flags", ctypes.c_uint32),
        ("instrument_count", ctypes.c_uint32),
        ("window_count", ctypes.c_uint32),
        ("reserved", ctypes.c_uint32),
    ]


assert ctypes.sizeof(_SessionInfoC) == 128


@dataclass(frozen=True, slots=True)
class NativeTickRead:
    payloads: Tuple[bytes, ...]
    next_sequence: int
    observed_sequence: int


@dataclass(frozen=True, slots=True)
class NativeTickBlockRead:
    data: bytes
    record_count: int
    next_sequence: int
    observed_sequence: int


def _candidate_library_paths() -> Iterable[str]:
    configured = os.environ.get("L2FLOW_SHM_READER_LIBRARY")
    if configured:
        yield configured
    discovered = ctypes.util.find_library("l2flow_shm_reader")
    if discovered:
        yield discovered
    repository = Path(__file__).resolve().parents[2]
    yield str(repository / "build-live-latest" / "libl2flow_shm_reader.so")
    yield str(Path.cwd() / "build-live-latest" / "libl2flow_shm_reader.so")


def load_native_library(
    library_path: Optional[Union[str, os.PathLike]] = None,
):
    candidates = (
        [os.fspath(library_path)]
        if library_path is not None
        else list(dict.fromkeys(_candidate_library_paths()))
    )
    failures = []
    for candidate in candidates:
        try:
            library = ctypes.CDLL(candidate, use_errno=True)
            _bind_library(library)
            return library
        except (OSError, AttributeError) as error:
            failures.append(f"{candidate}: {error}")
    detail = "; ".join(failures) if failures else "no candidate paths"
    raise NativeReaderError("load_library", SYSTEM_ERROR, detail)


def _bind_library(library) -> None:
    handle = ctypes.c_void_p
    library.l2flow_shm_reader_open_fd_v1.argtypes = [
        ctypes.c_int,
        ctypes.POINTER(handle),
    ]
    library.l2flow_shm_reader_open_fd_v1.restype = ctypes.c_int
    library.l2flow_shm_reader_close_v1.argtypes = [handle]
    library.l2flow_shm_reader_close_v1.restype = None
    library.l2flow_shm_reader_session_v1.argtypes = [
        handle,
        ctypes.POINTER(_SessionInfoC),
    ]
    library.l2flow_shm_reader_session_v1.restype = ctypes.c_int
    library.l2flow_shm_reader_instrument_v1.argtypes = [
        handle,
        ctypes.c_uint32,
        ctypes.c_void_p,
        ctypes.c_size_t,
        ctypes.POINTER(ctypes.c_uint8),
        ctypes.c_size_t,
        ctypes.POINTER(ctypes.c_size_t),
        ctypes.POINTER(ctypes.c_uint8),
        ctypes.c_size_t,
        ctypes.POINTER(ctypes.c_size_t),
    ]
    library.l2flow_shm_reader_instrument_v1.restype = ctypes.c_int
    byte_pointer = ctypes.POINTER(ctypes.c_uint8)
    library.l2flow_shm_reader_resolve_instruments_v1.argtypes = [
        handle,
        ctypes.POINTER(ctypes.c_uint8),
        ctypes.POINTER(byte_pointer),
        ctypes.POINTER(ctypes.c_size_t),
        ctypes.POINTER(byte_pointer),
        ctypes.POINTER(ctypes.c_size_t),
        ctypes.c_size_t,
        ctypes.POINTER(ctypes.c_uint32),
        ctypes.POINTER(ctypes.c_uint8),
    ]
    library.l2flow_shm_reader_resolve_instruments_v1.restype = ctypes.c_int
    for name in (
        "l2flow_shm_reader_latest_snapshots_v1",
        "l2flow_shm_reader_latest_ticks_v1",
    ):
        function = getattr(library, name)
        function.argtypes = [
            handle,
            ctypes.POINTER(ctypes.c_uint32),
            ctypes.c_size_t,
            ctypes.c_void_p,
            ctypes.c_size_t,
            ctypes.POINTER(ctypes.c_uint8),
        ]
        function.restype = ctypes.c_int
    library.l2flow_shm_reader_latest_klines_v1.argtypes = [
        handle,
        ctypes.POINTER(ctypes.c_uint32),
        ctypes.POINTER(ctypes.c_uint32),
        ctypes.c_size_t,
        ctypes.c_void_p,
        ctypes.c_size_t,
        ctypes.POINTER(ctypes.c_uint8),
    ]
    library.l2flow_shm_reader_latest_klines_v1.restype = ctypes.c_int
    library.l2flow_shm_reader_ticks_v1.argtypes = [
        handle,
        ctypes.c_uint64,
        ctypes.c_void_p,
        ctypes.c_size_t,
        ctypes.c_size_t,
        ctypes.POINTER(ctypes.c_size_t),
        ctypes.POINTER(ctypes.c_uint64),
        ctypes.POINTER(ctypes.c_uint64),
    ]
    library.l2flow_shm_reader_ticks_v1.restype = ctypes.c_int


def _raise_native(operation: str, code: int) -> None:
    if code == OK:
        return
    if code == UNAVAILABLE:
        raise UnavailableError(
            f"{operation}: shared-memory coverage is unavailable"
        )
    if code == INCONSISTENT_READ:
        raise InconsistentReadError(
            f"{operation}: no stable copy within the native retry bound"
        )
    detail = ""
    if code == SYSTEM_ERROR:
        error_number = ctypes.get_errno()
        if error_number:
            detail = os.strerror(error_number)
    raise NativeReaderError(operation, code, detail)


def _validate_uint32(value, field: str, *, allow_zero: bool = True) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{field} must be an integer")
    minimum = 0 if allow_zero else 1
    if value < minimum or value > _UINT32_MAX:
        qualifier = "uint32" if allow_zero else "positive uint32"
        raise ValueError(f"{field} must be a {qualifier}")
    return value


def _validate_uint32_sequence(values: Sequence[int], field: str):
    if isinstance(values, (str, bytes, bytearray)):
        raise TypeError(f"{field} must be a sequence of integers")
    result = tuple(_validate_uint32(value, field) for value in values)
    if len(result) > _C_SIZE_MAX:
        raise ValueError(f"{field} is too large")
    return result


class NativeReader:
    """Owns one native read-only mmap handle."""

    def __init__(self, library, handle: ctypes.c_void_p) -> None:
        self._library = library
        self._handle = handle
        self._lock = threading.RLock()
        self._tick_output = None
        self._tick_output_bytes = 0

    @classmethod
    def open_fd(
        cls,
        fd: int,
        *,
        library_path: Optional[Union[str, os.PathLike]] = None,
        library=None,
    ) -> "NativeReader":
        if not isinstance(fd, int) or isinstance(fd, bool) or fd < 0:
            raise ValueError("fd must be a nonnegative integer")
        loaded = library if library is not None else load_native_library(
            library_path
        )
        if library is not None:
            _bind_library(loaded)
        handle = ctypes.c_void_p()
        code = loaded.l2flow_shm_reader_open_fd_v1(
            fd, ctypes.byref(handle)
        )
        _raise_native("open_fd", code)
        if not handle.value:
            raise NativeReaderError(
                "open_fd", LAYOUT_INVALID, "native handle is null"
            )
        return cls(loaded, handle)

    @property
    def closed(self) -> bool:
        return not bool(self._handle.value)

    def _require_open(self) -> None:
        if self.closed:
            raise ClientClosedError("native reader is closed")

    def close(self) -> None:
        with self._lock:
            if self._handle.value:
                self._library.l2flow_shm_reader_close_v1(self._handle)
                self._handle = ctypes.c_void_p()

    def __enter__(self) -> "NativeReader":
        self._require_open()
        return self

    def __exit__(self, _type, _value, _traceback) -> None:
        self.close()

    def session(self) -> SessionInfo:
        with self._lock:
            self._require_open()
            output = _SessionInfoC()
            code = self._library.l2flow_shm_reader_session_v1(
                self._handle, ctypes.byref(output)
            )
            _raise_native("session", code)
            try:
                server_state = ServerState(output.server_state)
            except ValueError as error:
                raise WireFormatError(
                    f"unsupported server_state {output.server_state}"
                ) from error
            if (
                output.reserved != 0
                or not any(output.run_id)
                or output.session_epoch == 0
                or output.registry_version == 0
                or output.tick_ring_capacity == 0
                or output.tick_contiguous_published_sequence
                > output.tick_highest_published_sequence
            ):
                raise WireFormatError("native session metadata is invalid")
            return SessionInfo(
                run_id=bytes(output.run_id),
                session_epoch=output.session_epoch,
                registry_version=output.registry_version,
                registry_sha256=bytes(output.registry_sha256),
                tick_ring_capacity=output.tick_ring_capacity,
                tick_highest_published_sequence=(
                    output.tick_highest_published_sequence
                ),
                tick_contiguous_published_sequence=(
                    output.tick_contiguous_published_sequence
                ),
                kline_generation=output.kline_generation,
                heartbeat_monotonic_ns=output.heartbeat_monotonic_ns,
                trade_date=output.trade_date,
                server_state=server_state,
                flags=output.flags,
                instrument_count=output.instrument_count,
                window_count=output.window_count,
            )

    def instrument(self, instrument_id: int):
        instrument_id = _validate_uint32(
            instrument_id, "instrument_id", allow_zero=False
        )
        with self._lock:
            self._require_open()
            row = ctypes.create_string_buffer(INSTRUMENT_BYTES)
            source_size = ctypes.c_size_t()
            security_size = ctypes.c_size_t()
            code = self._library.l2flow_shm_reader_instrument_v1(
                self._handle,
                instrument_id,
                row,
                INSTRUMENT_BYTES,
                None,
                0,
                ctypes.byref(source_size),
                None,
                0,
                ctypes.byref(security_size),
            )
            if code not in (OK, BUFFER_TOO_SMALL):
                _raise_native("instrument", code)
            source = (
                (ctypes.c_uint8 * source_size.value)()
                if source_size.value
                else None
            )
            security = (
                (ctypes.c_uint8 * security_size.value)()
                if security_size.value
                else None
            )
            if code == BUFFER_TOO_SMALL:
                code = self._library.l2flow_shm_reader_instrument_v1(
                    self._handle,
                    instrument_id,
                    row,
                    INSTRUMENT_BYTES,
                    source,
                    source_size.value,
                    ctypes.byref(source_size),
                    security,
                    security_size.value,
                    ctypes.byref(security_size),
                )
                _raise_native("instrument", code)
            return parse_instrument(
                row.raw,
                bytes(source) if source is not None else b"",
                bytes(security) if security is not None else b"",
            )

    def resolve_instruments(
        self, keys: Sequence[InstrumentKey]
    ) -> Tuple[Tuple[InstrumentLookupStatus, ...], Tuple[int, ...]]:
        if isinstance(keys, (str, bytes, bytearray)):
            raise TypeError("keys must be a sequence of InstrumentKey values")
        keys = tuple(keys)
        if len(keys) > _C_SIZE_MAX:
            raise ValueError("keys is too large")
        for key in keys:
            if not isinstance(key, InstrumentKey):
                raise TypeError("each key must be an InstrumentKey")
        count = len(keys)
        if count == 0:
            with self._lock:
                self._require_open()
            return (), ()

        byte_pointer = ctypes.POINTER(ctypes.c_uint8)
        markets = (ctypes.c_uint8 * count)(
            *(int(key.market) for key in keys)
        )
        source_pointers = (byte_pointer * count)()
        source_lengths = (ctypes.c_size_t * count)()
        security_pointers = (byte_pointer * count)()
        security_lengths = (ctypes.c_size_t * count)()
        # Keep every client-owned byte array alive through the native call.
        source_buffers = []
        security_buffers = []
        for index, key in enumerate(keys):
            source_lengths[index] = len(key.security_id_source)
            if key.security_id_source:
                source_buffer = (
                    ctypes.c_uint8 * len(key.security_id_source)
                ).from_buffer_copy(key.security_id_source)
                source_buffers.append(source_buffer)
                source_pointers[index] = ctypes.cast(
                    source_buffer, byte_pointer
                )
            security_lengths[index] = len(key.security_id)
            if key.security_id:
                security_buffer = (
                    ctypes.c_uint8 * len(key.security_id)
                ).from_buffer_copy(key.security_id)
                security_buffers.append(security_buffer)
                security_pointers[index] = ctypes.cast(
                    security_buffer, byte_pointer
                )

        instrument_ids = (ctypes.c_uint32 * count)()
        statuses = (ctypes.c_uint8 * count)()
        with self._lock:
            self._require_open()
            code = (
                self._library.l2flow_shm_reader_resolve_instruments_v1(
                    self._handle,
                    markets,
                    source_pointers,
                    source_lengths,
                    security_pointers,
                    security_lengths,
                    count,
                    instrument_ids,
                    statuses,
                )
            )
            _raise_native("resolve_instruments", code)

        parsed_statuses = []
        parsed_ids = []
        for status_value, instrument_id in zip(
            statuses, instrument_ids
        ):
            try:
                status = InstrumentLookupStatus(status_value)
            except ValueError as error:
                raise WireFormatError(
                    f"unsupported instrument lookup status {status_value}"
                ) from error
            if (
                status is InstrumentLookupStatus.FOUND
            ) != (instrument_id != 0):
                raise WireFormatError(
                    "instrument lookup status/ID mismatch"
                )
            parsed_statuses.append(status)
            parsed_ids.append(instrument_id)
        return tuple(parsed_statuses), tuple(parsed_ids)

    def _latest(
        self,
        function_name: str,
        instrument_ids: Sequence[int],
        payload_bytes: int,
    ) -> Tuple[Tuple[LatestStatus, ...], Tuple[Optional[bytes], ...]]:
        instrument_ids = _validate_uint32_sequence(
            instrument_ids, "instrument_id"
        )
        count = len(instrument_ids)
        if count == 0:
            with self._lock:
                self._require_open()
            return (), ()
        if count > _C_SIZE_MAX // payload_bytes:
            raise ValueError("latest batch byte size is too large")
        ids = (ctypes.c_uint32 * count)(*instrument_ids)
        outputs = (ctypes.c_uint8 * (count * payload_bytes))()
        statuses = (ctypes.c_uint8 * count)()
        with self._lock:
            self._require_open()
            function = getattr(self._library, function_name)
            code = function(
                self._handle,
                ids,
                count,
                outputs,
                payload_bytes,
                statuses,
            )
            _raise_native(function_name, code)
        parsed_statuses = []
        payloads = []
        raw = bytes(outputs)
        for index, status_value in enumerate(statuses):
            try:
                status = LatestStatus(status_value)
            except ValueError as error:
                raise WireFormatError(
                    f"unsupported latest status {status_value}"
                ) from error
            parsed_statuses.append(status)
            if status is LatestStatus.AVAILABLE:
                begin = index * payload_bytes
                payloads.append(raw[begin : begin + payload_bytes])
            else:
                payloads.append(None)
        return tuple(parsed_statuses), tuple(payloads)

    def latest_snapshots(self, instrument_ids: Sequence[int]):
        return self._latest(
            "l2flow_shm_reader_latest_snapshots_v1",
            instrument_ids,
            SNAPSHOT_BYTES,
        )

    def latest_ticks(self, instrument_ids: Sequence[int]):
        return self._latest(
            "l2flow_shm_reader_latest_ticks_v1",
            instrument_ids,
            TICK_BYTES,
        )

    def latest_klines(
        self,
        instrument_ids: Sequence[int],
        window_ids: Sequence[int],
    ):
        instrument_ids = _validate_uint32_sequence(
            instrument_ids, "instrument_id"
        )
        window_ids = _validate_uint32_sequence(window_ids, "window_id")
        count = len(instrument_ids)
        if count != len(window_ids):
            raise ValueError("instrument_ids and window_ids must match")
        if count == 0:
            with self._lock:
                self._require_open()
            return (), ()
        if count > _C_SIZE_MAX // KLINE_BYTES:
            raise ValueError("KLine batch byte size is too large")
        ids = (ctypes.c_uint32 * count)(*instrument_ids)
        windows = (ctypes.c_uint32 * count)(*window_ids)
        outputs = (ctypes.c_uint8 * (count * KLINE_BYTES))()
        statuses = (ctypes.c_uint8 * count)()
        with self._lock:
            self._require_open()
            code = self._library.l2flow_shm_reader_latest_klines_v1(
                self._handle,
                ids,
                windows,
                count,
                outputs,
                KLINE_BYTES,
                statuses,
            )
            _raise_native("latest_klines", code)
        raw = bytes(outputs)
        parsed_statuses = []
        payloads = []
        for index, status_value in enumerate(statuses):
            try:
                status = LatestStatus(status_value)
            except ValueError as error:
                raise WireFormatError(
                    f"unsupported latest status {status_value}"
                ) from error
            parsed_statuses.append(status)
            if status is LatestStatus.AVAILABLE:
                begin = index * KLINE_BYTES
                payloads.append(raw[begin : begin + KLINE_BYTES])
            else:
                payloads.append(None)
        return tuple(parsed_statuses), tuple(payloads)

    def read_tick_block(
        self, expected_sequence: int, maximum_records: int
    ) -> NativeTickBlockRead:
        if (
            not isinstance(expected_sequence, int)
            or isinstance(expected_sequence, bool)
            or expected_sequence <= 0
            or expected_sequence > _UINT64_MAX
        ):
            raise ValueError("expected_sequence must be a positive uint64")
        if (
            not isinstance(maximum_records, int)
            or isinstance(maximum_records, bool)
            or maximum_records < 0
        ):
            raise ValueError("maximum_records must be a nonnegative integer")
        if maximum_records == 0:
            with self._lock:
                self._require_open()
            return NativeTickBlockRead(b"", 0, expected_sequence, 0)
        if (
            maximum_records > sys.maxsize // TICK_BYTES
            or maximum_records > _C_SIZE_MAX // TICK_BYTES
        ):
            raise ValueError("maximum_records is too large")
        required_bytes = maximum_records * TICK_BYTES
        written = ctypes.c_size_t()
        next_sequence = ctypes.c_uint64(expected_sequence)
        observed_sequence = ctypes.c_uint64()
        with self._lock:
            self._require_open()
            if self._tick_output_bytes < required_bytes:
                self._tick_output = (
                    ctypes.c_uint8 * required_bytes
                )()
                self._tick_output_bytes = required_bytes
            code = self._library.l2flow_shm_reader_ticks_v1(
                self._handle,
                expected_sequence,
                self._tick_output,
                TICK_BYTES,
                maximum_records,
                ctypes.byref(written),
                ctypes.byref(next_sequence),
                ctypes.byref(observed_sequence),
            )
            if code == OVERRUN:
                raise TickOverrunError(
                    expected_sequence, observed_sequence.value
                )
            _raise_native("read_ticks", code)
            if written.value > maximum_records:
                raise WireFormatError(
                    "native tick reader exceeded output bound"
                )
            if next_sequence.value != expected_sequence + written.value:
                raise WireFormatError(
                    "native tick reader returned a bad cursor"
                )
            raw = ctypes.string_at(
                self._tick_output,
                written.value * TICK_BYTES,
            )
        return NativeTickBlockRead(
            raw,
            written.value,
            next_sequence.value,
            observed_sequence.value,
        )

    def read_ticks(
        self, expected_sequence: int, maximum_records: int
    ) -> NativeTickRead:
        block = self.read_tick_block(expected_sequence, maximum_records)
        payloads = tuple(
            block.data[
                index * TICK_BYTES : (index + 1) * TICK_BYTES
            ]
            for index in range(block.record_count)
        )
        return NativeTickRead(
            payloads,
            block.next_sequence,
            block.observed_sequence,
        )
