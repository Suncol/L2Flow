"""ctypes wrapper around the sealed realtime shared-memory C ABI V2."""

from __future__ import annotations

import ctypes
import ctypes.util
import os
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Sequence, Union

from .models import (
    CatalogScope,
    ClientClosedError,
    InconsistentReadError,
    Instrument,
    InstrumentKey,
    InstrumentLookupResult,
    InstrumentLookupStatus,
    InstrumentStatus,
    LatestKLine,
    LatestSnapshot,
    LatestStatus,
    LatestTick,
    NativeReaderError,
    SelectionEnvelope,
    SelectionScope,
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
    parse_kline_payload,
    parse_snapshot_payload,
    parse_tick_payload,
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

MAX_BATCH_RECORDS = 1_048_576


class _SessionInfoC(ctypes.Structure):
    _fields_ = [
        ("run_id", ctypes.c_uint8 * 16),
        ("layout_digest", ctypes.c_uint8 * 32),
        ("catalog_digest", ctypes.c_uint8 * 32),
        ("session_epoch", ctypes.c_uint64),
        ("catalog_generation", ctypes.c_uint64),
        ("data_state_generation", ctypes.c_uint64),
        ("accepted_sequence", ctypes.c_uint64),
        ("applied_sequence", ctypes.c_uint64),
        ("processing_lag_records", ctypes.c_uint64),
        ("tick_ring_capacity", ctypes.c_uint64),
        ("tick_highest_published_sequence", ctypes.c_uint64),
        ("tick_contiguous_published_sequence", ctypes.c_uint64),
        ("kline_generation", ctypes.c_uint64),
        ("heartbeat_monotonic_ns", ctypes.c_uint64),
        ("published_records", ctypes.c_uint64),
        ("trade_date", ctypes.c_uint32),
        ("server_state", ctypes.c_uint32),
        ("flags", ctypes.c_uint32),
        ("capacity", ctypes.c_uint32),
        ("window_count", ctypes.c_uint32),
        ("catalog_scope", ctypes.c_uint32),
        ("coverage_complete", ctypes.c_uint32),
        ("bound_count", ctypes.c_uint32),
        ("available_count", ctypes.c_uint32),
        ("snapshot_available_count", ctypes.c_uint32),
        ("tick_available_count", ctypes.c_uint32),
        ("factor_eligible_count", ctypes.c_uint32),
        ("reserved", ctypes.c_uint32 * 4),
    ]


class _SelectionEnvelopeC(ctypes.Structure):
    _fields_ = [
        ("run_id", ctypes.c_uint8 * 16),
        ("catalog_digest", ctypes.c_uint8 * 32),
        ("session_epoch", ctypes.c_uint64),
        ("catalog_generation", ctypes.c_uint64),
        ("data_state_generation", ctypes.c_uint64),
        ("accepted_sequence", ctypes.c_uint64),
        ("applied_sequence", ctypes.c_uint64),
        ("processing_lag_records", ctypes.c_uint64),
        ("capacity", ctypes.c_uint32),
        ("catalog_scope", ctypes.c_uint32),
        ("coverage_complete", ctypes.c_uint32),
        ("bound_count", ctypes.c_uint32),
        ("available_count", ctypes.c_uint32),
        ("snapshot_available_count", ctypes.c_uint32),
        ("tick_available_count", ctypes.c_uint32),
        ("factor_eligible_count", ctypes.c_uint32),
        ("selection_scope", ctypes.c_uint32),
        ("returned_row_count", ctypes.c_uint32),
        ("reserved", ctypes.c_uint32 * 2),
    ]


class _HealthC(ctypes.Structure):
    _fields_ = [
        ("session_epoch", ctypes.c_uint64),
        ("heartbeat_monotonic_ns", ctypes.c_uint64),
        ("server_state", ctypes.c_uint32),
        ("flags", ctypes.c_uint32),
        ("reserved", ctypes.c_uint32 * 2),
    ]


assert ctypes.sizeof(_SessionInfoC) == 240
assert ctypes.sizeof(_SelectionEnvelopeC) == 144
assert ctypes.sizeof(_HealthC) == 32


@dataclass(frozen=True, slots=True)
class NativeTickRead:
    payloads: tuple[bytes, ...]
    next_sequence: int
    observed_sequence: int


@dataclass(frozen=True, slots=True)
class NativeSessionHealth:
    session_epoch: int
    heartbeat_monotonic_ns: int
    server_state: ServerState
    flags: int

    @property
    def coverage_lost(self) -> bool:
        return bool(self.flags & 0x1)


def _candidate_library_paths() -> Iterable[str]:
    configured = os.environ.get("L2FLOW_SHM_READER_LIBRARY")
    if configured:
        yield configured
    discovered = ctypes.util.find_library("l2flow_shm_reader")
    if discovered:
        yield discovered
    repository = Path(__file__).resolve().parents[2]
    for build_name in ("build", "build-live-latest"):
        yield str(repository / build_name / "libl2flow_shm_reader.so")


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
    byte_pointer = ctypes.POINTER(ctypes.c_uint8)

    library.l2flow_shm_reader_open_fd_v2.argtypes = [
        ctypes.c_int,
        ctypes.POINTER(handle),
    ]
    library.l2flow_shm_reader_open_fd_v2.restype = ctypes.c_int
    library.l2flow_shm_reader_close_v2.argtypes = [handle]
    library.l2flow_shm_reader_close_v2.restype = None
    library.l2flow_shm_reader_session_v2.argtypes = [
        handle,
        ctypes.POINTER(_SessionInfoC),
    ]
    library.l2flow_shm_reader_session_v2.restype = ctypes.c_int
    library.l2flow_shm_reader_health_v2.argtypes = [
        handle,
        ctypes.POINTER(_HealthC),
    ]
    library.l2flow_shm_reader_health_v2.restype = ctypes.c_int
    library.l2flow_shm_reader_instrument_v2.argtypes = [
        handle,
        ctypes.c_uint32,
        ctypes.c_void_p,
        ctypes.c_size_t,
        byte_pointer,
        ctypes.c_size_t,
        ctypes.POINTER(ctypes.c_size_t),
        byte_pointer,
        ctypes.c_size_t,
        ctypes.POINTER(ctypes.c_size_t),
        ctypes.POINTER(ctypes.c_uint8),
    ]
    library.l2flow_shm_reader_instrument_v2.restype = ctypes.c_int
    library.l2flow_shm_reader_resolve_instruments_v2.argtypes = [
        handle,
        byte_pointer,
        ctypes.POINTER(byte_pointer),
        ctypes.POINTER(ctypes.c_size_t),
        ctypes.POINTER(byte_pointer),
        ctypes.POINTER(ctypes.c_size_t),
        ctypes.c_size_t,
        ctypes.POINTER(ctypes.c_uint32),
        byte_pointer,
    ]
    library.l2flow_shm_reader_resolve_instruments_v2.restype = ctypes.c_int
    for name in (
        "l2flow_shm_reader_latest_snapshots_v2",
        "l2flow_shm_reader_latest_ticks_v2",
    ):
        function = getattr(library, name)
        function.argtypes = [
            handle,
            ctypes.POINTER(ctypes.c_uint32),
            ctypes.c_size_t,
            ctypes.c_void_p,
            ctypes.c_size_t,
            byte_pointer,
        ]
        function.restype = ctypes.c_int
    library.l2flow_shm_reader_latest_klines_v2.argtypes = [
        handle,
        ctypes.POINTER(ctypes.c_uint32),
        ctypes.POINTER(ctypes.c_uint32),
        ctypes.c_size_t,
        ctypes.c_void_p,
        ctypes.c_size_t,
        byte_pointer,
    ]
    library.l2flow_shm_reader_latest_klines_v2.restype = ctypes.c_int
    library.l2flow_shm_reader_ticks_v2.argtypes = [
        handle,
        ctypes.c_uint64,
        ctypes.c_void_p,
        ctypes.c_size_t,
        ctypes.c_size_t,
        ctypes.POINTER(ctypes.c_size_t),
        ctypes.POINTER(ctypes.c_uint64),
        ctypes.POINTER(ctypes.c_uint64),
    ]
    library.l2flow_shm_reader_ticks_v2.restype = ctypes.c_int
    library.l2flow_shm_reader_select_instruments_v2.argtypes = [
        handle,
        ctypes.c_uint32,
        ctypes.POINTER(ctypes.c_uint32),
        ctypes.c_size_t,
        ctypes.POINTER(ctypes.c_size_t),
        ctypes.POINTER(_SelectionEnvelopeC),
    ]
    library.l2flow_shm_reader_select_instruments_v2.restype = ctypes.c_int


def _raise_native(operation: str, code: int) -> None:
    if code == OK:
        return
    if code == UNAVAILABLE:
        raise UnavailableError(
            f"{operation}: shared-memory coverage is unavailable"
        )
    if code == INCONSISTENT_READ:
        raise InconsistentReadError(
            f"{operation}: no coherent copy within the retry bound"
        )
    if code in (ABI_MISMATCH, LAYOUT_INVALID):
        raise WireFormatError(
            f"{operation}: native reader rejected Wire V2 layout ({code})"
        )
    raise NativeReaderError(operation, code)


def _uint32(value, field: str) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or value < 0
        or value > 0xFFFFFFFF
    ):
        raise ValueError(f"{field} must fit uint32")
    return value


def _batch(values: Sequence[int], field: str) -> tuple[int, ...]:
    if isinstance(values, (str, bytes, bytearray)):
        raise TypeError(f"{field} must be a sequence of uint32 values")
    result = tuple(_uint32(value, field) for value in values)
    if len(result) > MAX_BATCH_RECORDS:
        raise ValueError(
            f"{field} exceeds the {MAX_BATCH_RECORDS}-record limit"
        )
    return result


def _bytes(array) -> bytes:
    return bytes(bytearray(array))


def _session_from_c(value: _SessionInfoC) -> SessionInfo:
    if any(value.reserved):
        raise WireFormatError("session C result reserved fields are nonzero")
    if value.coverage_complete != 0:
        raise WireFormatError("Wire V2 coverage_complete must be zero")
    if value.flags & ~0x3:
        raise WireFormatError("session C result has unknown flags")
    try:
        return SessionInfo(
            run_id=_bytes(value.run_id),
            layout_digest=_bytes(value.layout_digest),
            catalog_digest=_bytes(value.catalog_digest),
            session_epoch=value.session_epoch,
            catalog_generation=value.catalog_generation,
            data_state_generation=value.data_state_generation,
            accepted_sequence=value.accepted_sequence,
            applied_sequence=value.applied_sequence,
            processing_lag_records=value.processing_lag_records,
            tick_ring_capacity=value.tick_ring_capacity,
            tick_highest_published_sequence=(
                value.tick_highest_published_sequence
            ),
            tick_contiguous_published_sequence=(
                value.tick_contiguous_published_sequence
            ),
            kline_generation=value.kline_generation,
            heartbeat_monotonic_ns=value.heartbeat_monotonic_ns,
            published_records=value.published_records,
            trade_date=value.trade_date,
            server_state=ServerState(value.server_state),
            flags=value.flags,
            capacity=value.capacity,
            window_count=value.window_count,
            catalog_scope=CatalogScope(value.catalog_scope),
            coverage_complete=False,
            bound_count=value.bound_count,
            available_count=value.available_count,
            snapshot_available_count=value.snapshot_available_count,
            tick_available_count=value.tick_available_count,
            factor_eligible_count=value.factor_eligible_count,
        )
    except ValueError as error:
        raise WireFormatError(f"invalid session C result: {error}") from error


def _selection_from_c(
    value: _SelectionEnvelopeC,
    instrument_ids: tuple[int, ...],
) -> SelectionEnvelope:
    if any(value.reserved):
        raise WireFormatError(
            "selection C result reserved fields are nonzero"
        )
    if value.coverage_complete != 0:
        raise WireFormatError("Wire V2 coverage_complete must be zero")
    try:
        return SelectionEnvelope(
            run_id=_bytes(value.run_id),
            catalog_digest=_bytes(value.catalog_digest),
            session_epoch=value.session_epoch,
            catalog_generation=value.catalog_generation,
            data_state_generation=value.data_state_generation,
            accepted_sequence=value.accepted_sequence,
            applied_sequence=value.applied_sequence,
            processing_lag_records=value.processing_lag_records,
            capacity=value.capacity,
            catalog_scope=CatalogScope(value.catalog_scope),
            coverage_complete=False,
            bound_count=value.bound_count,
            available_count=value.available_count,
            snapshot_available_count=value.snapshot_available_count,
            tick_available_count=value.tick_available_count,
            factor_eligible_count=value.factor_eligible_count,
            selection_scope=SelectionScope(value.selection_scope),
            returned_row_count=value.returned_row_count,
            instrument_ids=instrument_ids,
        )
    except ValueError as error:
        raise WireFormatError(
            f"invalid selection C result: {error}"
        ) from error


class NativeReader:
    """One read-only Wire V2 mapping; close waits for in-flight methods."""

    def __init__(self, library, handle: ctypes.c_void_p) -> None:
        self._library = library
        self._handle = handle
        self._lock = threading.RLock()
        self._closed = False
        self._session_epoch: Optional[int] = None

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
        native_library = (
            load_native_library(library_path)
            if library is None
            else library
        )
        if library is not None:
            _bind_library(native_library)
        handle = ctypes.c_void_p()
        code = native_library.l2flow_shm_reader_open_fd_v2(
            fd, ctypes.byref(handle)
        )
        _raise_native("open_fd_v2", code)
        if not handle.value:
            raise WireFormatError("open_fd_v2 returned a null reader")
        reader = cls(native_library, handle)
        try:
            reader.session()
            return reader
        except BaseException:
            reader.close()
            raise

    @property
    def closed(self) -> bool:
        return self._closed

    def _require_open(self) -> None:
        if self._closed:
            raise ClientClosedError("native reader is closed")

    def _epoch(self) -> int:
        if self._session_epoch is None:
            return self.session().session_epoch
        return self._session_epoch

    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self._library.l2flow_shm_reader_close_v2(self._handle)
                self._handle = ctypes.c_void_p()
                self._closed = True

    def __enter__(self) -> "NativeReader":
        self._require_open()
        return self

    def __exit__(self, _type, _value, _traceback) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except BaseException:
            pass

    def session(self) -> SessionInfo:
        with self._lock:
            self._require_open()
            output = _SessionInfoC()
            code = self._library.l2flow_shm_reader_session_v2(
                self._handle, ctypes.byref(output)
            )
            _raise_native("session_v2", code)
            result = _session_from_c(output)
            if (
                self._session_epoch is not None
                and result.session_epoch != self._session_epoch
            ):
                raise WireFormatError(
                    "mapped session_epoch changed in place"
                )
            self._session_epoch = result.session_epoch
            return result

    def health(self) -> NativeSessionHealth:
        with self._lock:
            self._require_open()
            output = _HealthC()
            code = self._library.l2flow_shm_reader_health_v2(
                self._handle, ctypes.byref(output)
            )
            _raise_native("health_v2", code)
            if (
                any(output.reserved)
                or output.flags & ~0x3
                or output.session_epoch == 0
            ):
                raise WireFormatError(
                    "health_v2 returned noncanonical fields"
                )
            try:
                state = ServerState(output.server_state)
            except ValueError as error:
                raise WireFormatError(
                    f"health_v2 returned state {output.server_state}"
                ) from error
            if (
                self._session_epoch is not None
                and output.session_epoch != self._session_epoch
            ):
                raise WireFormatError(
                    "mapped session_epoch changed in place"
                )
            self._session_epoch = output.session_epoch
            return NativeSessionHealth(
                session_epoch=output.session_epoch,
                heartbeat_monotonic_ns=output.heartbeat_monotonic_ns,
                server_state=state,
                flags=output.flags,
            )

    def instrument(self, instrument_id: int) -> Instrument:
        instrument_id = _uint32(instrument_id, "instrument_id")
        with self._lock:
            self._require_open()
            row = ctypes.create_string_buffer(INSTRUMENT_BYTES)
            source_written = ctypes.c_size_t()
            id_written = ctypes.c_size_t()
            item_status = ctypes.c_uint8()
            function = self._library.l2flow_shm_reader_instrument_v2
            code = function(
                self._handle,
                instrument_id,
                row,
                INSTRUMENT_BYTES,
                None,
                0,
                ctypes.byref(source_written),
                None,
                0,
                ctypes.byref(id_written),
                ctypes.byref(item_status),
            )
            if code == OK:
                try:
                    status = InstrumentStatus(item_status.value)
                except ValueError as error:
                    raise WireFormatError(
                        f"instrument_v2 returned status {item_status.value}"
                    ) from error
                if status not in (
                    InstrumentStatus.UNBOUND,
                    InstrumentStatus.INVALID_ID,
                ):
                    raise WireFormatError(
                        "bound instrument returned without its key buffers"
                    )
                return Instrument(
                    session_epoch=self._epoch(),
                    instrument_id=instrument_id,
                    status=status,
                )
            if code != BUFFER_TOO_SMALL:
                _raise_native("instrument_v2", code)
            try:
                status = InstrumentStatus(item_status.value)
            except ValueError as error:
                raise WireFormatError(
                    f"instrument_v2 returned status {item_status.value}"
                ) from error
            if status not in (
                InstrumentStatus.AVAILABLE,
                InstrumentStatus.BOUND_NO_DATA,
            ):
                raise WireFormatError(
                    "BUFFER_TOO_SMALL returned for an unbound instrument"
                )
            source = (
                (ctypes.c_uint8 * source_written.value)()
                if source_written.value
                else None
            )
            security_id = (
                (ctypes.c_uint8 * id_written.value)()
                if id_written.value
                else None
            )
            second_source_written = ctypes.c_size_t()
            second_id_written = ctypes.c_size_t()
            second_status = ctypes.c_uint8()
            code = function(
                self._handle,
                instrument_id,
                row,
                INSTRUMENT_BYTES,
                source,
                source_written.value,
                ctypes.byref(second_source_written),
                security_id,
                id_written.value,
                ctypes.byref(second_id_written),
                ctypes.byref(second_status),
            )
            _raise_native("instrument_v2", code)
            if (
                second_source_written.value != source_written.value
                or second_id_written.value != id_written.value
            ):
                raise WireFormatError(
                    "instrument identity changed between two-pass reads"
                )
            try:
                final_status = InstrumentStatus(second_status.value)
            except ValueError as error:
                raise WireFormatError(
                    "instrument_v2 returned an invalid second-pass status"
                ) from error
            if final_status not in (
                status,
                InstrumentStatus.AVAILABLE,
            ) or (
                status is InstrumentStatus.AVAILABLE
                and final_status is not InstrumentStatus.AVAILABLE
            ):
                raise WireFormatError(
                    "instrument binding state regressed between reads"
                )
            return parse_instrument(
                bytes(row.raw),
                b"" if source is None else _bytes(source),
                b"" if security_id is None else _bytes(security_id),
                session_epoch=self._epoch(),
                requested_instrument_id=instrument_id,
                status=final_status,
            )

    def resolve_instruments(
        self, keys: Sequence[InstrumentKey]
    ) -> tuple[InstrumentLookupResult, ...]:
        if isinstance(keys, (str, bytes, bytearray)):
            raise TypeError("keys must be a sequence of InstrumentKey values")
        keys = tuple(keys)
        if len(keys) > MAX_BATCH_RECORDS:
            raise ValueError("key batch exceeds the record limit")
        if any(not isinstance(key, InstrumentKey) for key in keys):
            raise TypeError("every key must be an InstrumentKey")
        if not keys:
            return ()
        count = len(keys)
        markets = (ctypes.c_uint8 * count)(
            *(key.market for key in keys)
        )
        byte_pointer = ctypes.POINTER(ctypes.c_uint8)
        source_buffers = [
            (ctypes.c_uint8 * len(key.security_id_source)).from_buffer_copy(
                key.security_id_source
            )
            if key.security_id_source
            else None
            for key in keys
        ]
        id_buffers = [
            (ctypes.c_uint8 * len(key.security_id)).from_buffer_copy(
                key.security_id
            )
            if key.security_id
            else None
            for key in keys
        ]
        source_pointers = (byte_pointer * count)(
            *(
                ctypes.cast(value, byte_pointer) if value is not None else None
                for value in source_buffers
            )
        )
        id_pointers = (byte_pointer * count)(
            *(
                ctypes.cast(value, byte_pointer) if value is not None else None
                for value in id_buffers
            )
        )
        source_lengths = (ctypes.c_size_t * count)(
            *(len(key.security_id_source) for key in keys)
        )
        id_lengths = (ctypes.c_size_t * count)(
            *(len(key.security_id) for key in keys)
        )
        instrument_ids = (ctypes.c_uint32 * count)()
        statuses = (ctypes.c_uint8 * count)()
        with self._lock:
            self._require_open()
            code = self._library.l2flow_shm_reader_resolve_instruments_v2(
                self._handle,
                markets,
                source_pointers,
                source_lengths,
                id_pointers,
                id_lengths,
                count,
                instrument_ids,
                statuses,
            )
            _raise_native("resolve_instruments_v2", code)
            epoch = self._epoch()
            results = []
            for index, key in enumerate(keys):
                try:
                    status = InstrumentLookupStatus(statuses[index])
                    results.append(
                        InstrumentLookupResult(
                            session_epoch=epoch,
                            key=key,
                            status=status,
                            instrument_id=instrument_ids[index],
                        )
                    )
                except ValueError as error:
                    raise WireFormatError(
                        "invalid resolve_instruments_v2 item result"
                    ) from error
            return tuple(results)

    def latest_snapshots(
        self, instrument_ids: Sequence[int]
    ) -> tuple[LatestSnapshot, ...]:
        ids = _batch(instrument_ids, "instrument_id")
        return self._latest_snapshots(ids)

    def _latest_snapshots(
        self, ids: tuple[int, ...]
    ) -> tuple[LatestSnapshot, ...]:
        raw, statuses, epoch = self._latest_records(
            "latest_snapshots_v2",
            self._library.l2flow_shm_reader_latest_snapshots_v2,
            ids,
            SNAPSHOT_BYTES,
        )
        results = []
        for index, instrument_id in enumerate(ids):
            status = _latest_status(statuses[index])
            if status is LatestStatus.AVAILABLE:
                payload = raw[
                    index * SNAPSHOT_BYTES : (index + 1) * SNAPSHOT_BYTES
                ]
                common, last_price = parse_snapshot_payload(
                    payload, instrument_id
                )
                results.append(
                    LatestSnapshot(
                        epoch,
                        instrument_id,
                        status,
                        common,
                        last_price,
                        payload,
                    )
                )
            else:
                results.append(
                    LatestSnapshot(epoch, instrument_id, status)
                )
        return tuple(results)

    def latest_ticks(
        self, instrument_ids: Sequence[int]
    ) -> tuple[LatestTick, ...]:
        ids = _batch(instrument_ids, "instrument_id")
        raw, statuses, epoch = self._latest_records(
            "latest_ticks_v2",
            self._library.l2flow_shm_reader_latest_ticks_v2,
            ids,
            TICK_BYTES,
        )
        results = []
        for index, instrument_id in enumerate(ids):
            status = _latest_status(statuses[index])
            if status is LatestStatus.AVAILABLE:
                payload = raw[index * TICK_BYTES : (index + 1) * TICK_BYTES]
                (
                    common,
                    price,
                    quantity,
                    action,
                    side,
                    projection_flags,
                ) = parse_tick_payload(payload, instrument_id)
                results.append(
                    LatestTick(
                        epoch,
                        instrument_id,
                        status,
                        common,
                        price,
                        quantity,
                        action,
                        side,
                        projection_flags,
                        payload,
                    )
                )
            else:
                results.append(LatestTick(epoch, instrument_id, status))
        return tuple(results)

    def latest_klines(
        self,
        instrument_ids: Sequence[int],
        window_ids: Sequence[int],
    ) -> tuple[LatestKLine, ...]:
        ids = _batch(instrument_ids, "instrument_id")
        windows = _batch(window_ids, "window_id")
        if len(ids) != len(windows):
            raise ValueError("instrument_ids and window_ids lengths differ")
        if not ids:
            return ()
        count = len(ids)
        ids_c = (ctypes.c_uint32 * count)(*ids)
        windows_c = (ctypes.c_uint32 * count)(*windows)
        outputs = (ctypes.c_uint8 * (count * KLINE_BYTES))()
        statuses = (ctypes.c_uint8 * count)()
        with self._lock:
            self._require_open()
            code = self._library.l2flow_shm_reader_latest_klines_v2(
                self._handle,
                ids_c,
                windows_c,
                count,
                outputs,
                KLINE_BYTES,
                statuses,
            )
            _raise_native("latest_klines_v2", code)
            epoch = self._epoch()
            raw = _bytes(outputs)
        results = []
        for index, (instrument_id, window_id) in enumerate(
            zip(ids, windows)
        ):
            status = _latest_status(statuses[index])
            if status is LatestStatus.AVAILABLE:
                payload = raw[
                    index * KLINE_BYTES : (index + 1) * KLINE_BYTES
                ]
                fields = parse_kline_payload(
                    payload, instrument_id, window_id
                )
                results.append(
                    LatestKLine(
                        session_epoch=epoch,
                        instrument_id=instrument_id,
                        window_id=window_id,
                        status=status,
                        wire_payload=payload,
                        **fields,
                    )
                )
            else:
                results.append(
                    LatestKLine(epoch, instrument_id, window_id, status)
                )
        return tuple(results)

    def _latest_records(
        self,
        operation: str,
        function,
        ids: tuple[int, ...],
        stride: int,
    ) -> tuple[bytes, ctypes.Array, int]:
        if not ids:
            return b"", (ctypes.c_uint8 * 0)(), self._epoch()
        count = len(ids)
        ids_c = (ctypes.c_uint32 * count)(*ids)
        outputs = (ctypes.c_uint8 * (count * stride))()
        statuses = (ctypes.c_uint8 * count)()
        with self._lock:
            self._require_open()
            code = function(
                self._handle,
                ids_c,
                count,
                outputs,
                stride,
                statuses,
            )
            _raise_native(operation, code)
            return _bytes(outputs), statuses, self._epoch()

    def select_instruments(
        self, scope: Union[SelectionScope, int]
    ) -> SelectionEnvelope:
        try:
            selection_scope = SelectionScope(scope)
        except (TypeError, ValueError) as error:
            raise ValueError("scope is not a Wire V2 selection") from error
        with self._lock:
            self._require_open()
            function = (
                self._library.l2flow_shm_reader_select_instruments_v2
            )
            for _attempt in range(4):
                required = ctypes.c_size_t()
                envelope = _SelectionEnvelopeC()
                code = function(
                    self._handle,
                    int(selection_scope),
                    None,
                    0,
                    ctypes.byref(required),
                    ctypes.byref(envelope),
                )
                if code == OK:
                    if required.value != 0:
                        raise WireFormatError(
                            "selection succeeded without a sufficient buffer"
                        )
                    return _selection_from_c(envelope, ())
                if code != BUFFER_TOO_SMALL:
                    _raise_native("select_instruments_v2", code)
                if required.value > MAX_BATCH_RECORDS:
                    raise WireFormatError(
                        "selection exceeds the Python record limit"
                    )
                ids = (ctypes.c_uint32 * required.value)()
                second_required = ctypes.c_size_t()
                second_envelope = _SelectionEnvelopeC()
                code = function(
                    self._handle,
                    int(selection_scope),
                    ids,
                    required.value,
                    ctypes.byref(second_required),
                    ctypes.byref(second_envelope),
                )
                if code == BUFFER_TOO_SMALL:
                    continue
                _raise_native("select_instruments_v2", code)
                if second_required.value > required.value:
                    raise WireFormatError(
                        "selection wrote beyond the supplied capacity"
                    )
                result_ids = tuple(
                    ids[index]
                    for index in range(second_required.value)
                )
                return _selection_from_c(second_envelope, result_ids)
            raise InconsistentReadError(
                "select_instruments_v2 changed across four two-pass reads"
            )

    def ticks(
        self, expected_sequence: int, maximum_records: int
    ) -> NativeTickRead:
        if (
            not isinstance(expected_sequence, int)
            or isinstance(expected_sequence, bool)
            or expected_sequence <= 0
            or expected_sequence > 0xFFFFFFFFFFFFFFFF
        ):
            raise ValueError("expected_sequence must be a positive uint64")
        if (
            not isinstance(maximum_records, int)
            or isinstance(maximum_records, bool)
            or maximum_records < 0
            or maximum_records > MAX_BATCH_RECORDS
        ):
            raise ValueError("maximum_records is outside the record limit")
        outputs = (
            (ctypes.c_uint8 * (maximum_records * TICK_BYTES))()
            if maximum_records
            else None
        )
        written = ctypes.c_size_t()
        next_sequence = ctypes.c_uint64(expected_sequence)
        observed_sequence = ctypes.c_uint64()
        with self._lock:
            self._require_open()
            code = self._library.l2flow_shm_reader_ticks_v2(
                self._handle,
                expected_sequence,
                outputs,
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
            _raise_native("ticks_v2", code)
            if written.value > maximum_records:
                raise WireFormatError("ticks_v2 exceeded output capacity")
            raw = b"" if outputs is None else _bytes(outputs)
            payloads = tuple(
                raw[index * TICK_BYTES : (index + 1) * TICK_BYTES]
                for index in range(written.value)
            )
            return NativeTickRead(
                payloads,
                next_sequence.value,
                observed_sequence.value,
            )


def _latest_status(value: int) -> LatestStatus:
    try:
        return LatestStatus(value)
    except ValueError as error:
        raise WireFormatError(
            f"latest reader returned unsupported status {value}"
        ) from error
