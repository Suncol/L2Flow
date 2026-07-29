"""High-level read-only client for the L2Flow realtime service."""

from __future__ import annotations

import os
import threading
import time
from typing import Optional, Sequence, Union

from .batch import LatestBatch, TickBatch, TickColumnBatch
from .control import discover_session_fd
from .history import open_history_cursor
from .instrument_delta import (
    DEFAULT_INSTRUMENT_TICK_DELTA_PAGE_RECORDS,
    open_instrument_tick_delta_session,
)
from .models import (
    ClientClosedError,
    InstrumentKey,
    InstrumentLookupResult,
    InstrumentLookupStatus,
    LatestResult,
    Market,
    ServerState,
    SessionIdentity,
    StaleSessionError,
    UnavailableError,
    WireFormatError,
)
from .native import NativeReader
from .wire import (
    parse_kline,
    parse_snapshot,
    parse_tick,
    validate_kline_identity,
    validate_snapshot_identity,
    validate_tick_identity,
)


DEFAULT_STALE_AFTER_NS = 3_000_000_000
MAX_BATCH_RECORDS = 1_048_576
_UINT32_MAX = 0xFFFFFFFF
_UINT64_MAX = 0xFFFFFFFFFFFFFFFF


def _uint32(value, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{field} must be an integer")
    if value < 0 or value > _UINT32_MAX:
        raise ValueError(f"{field} must fit uint32")
    return value


def _positive_uint64(value, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{field} must be an integer")
    if value <= 0 or value > _UINT64_MAX:
        raise ValueError(f"{field} must be a positive uint64")
    return value


def _ids(values: Sequence[int], field: str):
    if isinstance(values, (str, bytes, bytearray)):
        raise TypeError(f"{field} must be a sequence of integers")
    return tuple(_uint32(value, field) for value in values)


class L2FlowClient:
    """One mapped service session; it never silently reconnects."""

    def __init__(
        self,
        native_reader,
        *,
        stale_after_ns: Optional[int] = DEFAULT_STALE_AFTER_NS,
        expected_identity: Optional[SessionIdentity] = None,
        control_socket_path: Optional[Union[str, os.PathLike]] = None,
        control_timeout: Optional[float] = 1.0,
    ) -> None:
        if stale_after_ns is not None:
            _positive_uint64(stale_after_ns, "stale_after_ns")
        self._native = native_reader
        self._stale_after_ns = stale_after_ns
        self._lock = threading.RLock()
        self._closed = False
        self._instrument_cache = {}
        self._control_socket_path = (
            None
            if control_socket_path is None
            else os.fspath(control_socket_path)
        )
        self._control_timeout = control_timeout
        try:
            initial = native_reader.session()
            self._identity = initial.identity
            if (
                expected_identity is not None
                and expected_identity != self._identity
            ):
                raise StaleSessionError(
                    "control response and mapped session identity differ"
                )
            self._validate_session(initial)
        except Exception:
            self._closed = True
            native_reader.close()
            raise

    @classmethod
    def connect(
        cls,
        control_socket_path: Union[str, os.PathLike],
        *,
        native_library: Optional[Union[str, os.PathLike]] = None,
        timeout: Optional[float] = 1.0,
        stale_after_ns: Optional[int] = DEFAULT_STALE_AFTER_NS,
        _native_factory=None,
    ) -> "L2FlowClient":
        with discover_session_fd(
            control_socket_path, timeout=timeout
        ) as control:
            native = None
            try:
                descriptor_size = os.fstat(control.fd).st_size
                if descriptor_size != control.total_mapping_bytes:
                    raise StaleSessionError(
                        "control response mapping size does not match its fd"
                    )
                if _native_factory is None:
                    native = NativeReader.open_fd(
                        control.fd, library_path=native_library
                    )
                else:
                    native = _native_factory(control.fd)
                if native is None:
                    raise WireFormatError(
                        "native reader factory returned None"
                    )
                session = native.session()
                if session.session_epoch != control.session_epoch:
                    raise StaleSessionError(
                        "control response epoch does not match mapped "
                        "session"
                    )
                return cls(
                    native,
                    stale_after_ns=stale_after_ns,
                    expected_identity=session.identity,
                    control_socket_path=control_socket_path,
                    control_timeout=timeout,
                )
            except BaseException:
                if native is not None:
                    native.close()
                raise

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def session_identity(self) -> SessionIdentity:
        return self._identity

    def _require_open(self) -> None:
        if self._closed:
            raise ClientClosedError("realtime client is closed")

    def _validate_session(self, session) -> None:
        if session.identity != self._identity:
            raise StaleSessionError(
                "mapped realtime session identity changed"
            )
        if session.coverage_lost:
            raise UnavailableError("realtime shared-memory coverage was lost")
        if session.server_state in (
            ServerState.INITIALIZING,
            ServerState.FAILED,
        ):
            raise UnavailableError(
                f"realtime server state is {session.server_state.name}"
            )
        if (
            self._stale_after_ns is not None
            and session.server_state
            in (ServerState.ACTIVE, ServerState.DRAINING)
        ):
            heartbeat = session.heartbeat_monotonic_ns
            now = time.monotonic_ns()
            if heartbeat == 0 or (
                now >= heartbeat
                and now - heartbeat > self._stale_after_ns
            ):
                raise StaleSessionError(
                    "realtime server heartbeat is stale"
                )

    def _checked_session(self):
        self._require_open()
        session = self._native.session()
        self._validate_session(session)
        return session

    def session_info(self):
        with self._lock:
            return self._checked_session()

    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self._native.close()
                self._instrument_cache.clear()
                self._closed = True

    def __enter__(self) -> "L2FlowClient":
        with self._lock:
            self._checked_session()
        return self

    def __exit__(self, _type, _value, _traceback) -> None:
        self.close()

    def get_instrument(self, instrument_id: int):
        instrument_id = _uint32(instrument_id, "instrument_id")
        if instrument_id == 0:
            raise ValueError("instrument_id must be nonzero")
        with self._lock:
            self._checked_session()
            cached = self._instrument_cache.get(instrument_id)
            if cached is not None:
                return cached
            result = self._native.instrument(instrument_id)
            self._checked_session()
            self._instrument_cache[instrument_id] = result
            return result

    def resolve_instrument(
        self,
        market: Union[Market, int],
        security_id_source: bytes,
        security_id: bytes,
    ) -> InstrumentLookupResult:
        """Resolve one exact (market, source, security ID) byte key."""

        key = InstrumentKey(
            market=market,
            security_id_source=security_id_source,
            security_id=security_id,
        )
        return self.resolve_instruments((key,))[0]

    def resolve_instruments(
        self, keys: Sequence[InstrumentKey]
    ) -> tuple[InstrumentLookupResult, ...]:
        """Resolve exact registry keys while preserving order and duplicates."""

        if isinstance(keys, (str, bytes, bytearray)):
            raise TypeError("keys must be a sequence of InstrumentKey values")
        keys = tuple(keys)
        for key in keys:
            if not isinstance(key, InstrumentKey):
                raise TypeError("each key must be an InstrumentKey")
        with self._lock:
            self._checked_session()
            statuses, instrument_ids = self._native.resolve_instruments(
                keys
            )
            self._checked_session()
        if len(statuses) != len(keys) or len(instrument_ids) != len(keys):
            raise WireFormatError(
                "native instrument lookup batch length mismatch"
            )
        results = []
        for key, status, instrument_id in zip(
            keys, statuses, instrument_ids
        ):
            if not isinstance(status, InstrumentLookupStatus):
                raise WireFormatError(
                    "native instrument lookup returned an invalid status"
                )
            if (
                status is InstrumentLookupStatus.FOUND
            ) != (instrument_id != 0):
                raise WireFormatError(
                    "native instrument lookup status/ID mismatch"
                )
            results.append(
                InstrumentLookupResult(
                    key,
                    status,
                    instrument_id
                    if status is InstrumentLookupStatus.FOUND
                    else None,
                )
            )
        return tuple(results)

    def open_instrument_history(
        self,
        instrument_id: int,
        *,
        requested_page_records: int = 4096,
        timeout: Optional[float] = None,
    ):
        """Pin and stream one instrument's complete latest Store generation.

        The cursor is an independent read-only UDS session. Its generation is
        fixed at open time, so later realtime generation publication cannot
        mix records into an in-progress scan.
        """

        instrument_id = _uint32(instrument_id, "instrument_id")
        if instrument_id == 0:
            raise ValueError("instrument_id must be nonzero")
        with self._lock:
            session = self._checked_session()
            control_socket_path = self._control_socket_path
            if control_socket_path is None:
                raise UnavailableError(
                    "this client was not created from a control socket"
                )
            effective_timeout = (
                self._control_timeout if timeout is None else timeout
            )
        cursor = open_history_cursor(
            control_socket_path,
            instrument_id,
            requested_page_records=requested_page_records,
            timeout=effective_timeout,
            expected_run_id=session.run_id,
            expected_session_epoch=session.session_epoch,
            expected_trade_date=session.trade_date,
            expected_instrument_count=session.instrument_count,
            expected_registry_version=session.registry_version,
            expected_registry_sha256=session.registry_sha256,
        )
        try:
            with self._lock:
                current = self._checked_session()
                if current.identity != session.identity:
                    raise StaleSessionError(
                        "realtime session changed while opening history"
                    )
                if (
                    current.registry_version !=
                        session.registry_version
                    or current.registry_sha256 !=
                        session.registry_sha256
                ):
                    raise StaleSessionError(
                        "registry changed while opening history"
                    )
            return cursor
        except Exception:
            cursor.close()
            raise

    def open_history(
        self,
        instrument_id: int,
        *,
        requested_page_records: int = 4096,
        timeout: Optional[float] = None,
    ):
        """Alias for :meth:`open_instrument_history`."""

        return self.open_instrument_history(
            instrument_id,
            requested_page_records=requested_page_records,
            timeout=timeout,
        )

    def open_instrument_tick_delta_session(
        self,
        *,
        requested_page_records: int = (
            DEFAULT_INSTRUMENT_TICK_DELTA_PAGE_RECORDS
        ),
        timeout: Optional[float] = None,
    ):
        """Pin one Store target for sequential instrument tick deltas."""

        with self._lock:
            session = self._checked_session()
            control_socket_path = self._control_socket_path
            if control_socket_path is None:
                raise UnavailableError(
                    "this client was not created from a control socket"
                )
            effective_timeout = (
                self._control_timeout if timeout is None else timeout
            )
        delta_session = open_instrument_tick_delta_session(
            control_socket_path,
            requested_page_records=requested_page_records,
            timeout=effective_timeout,
            expected_run_id=session.run_id,
            expected_session_epoch=session.session_epoch,
            expected_trade_date=session.trade_date,
            expected_instrument_count=session.instrument_count,
            expected_registry_version=session.registry_version,
            expected_registry_sha256=session.registry_sha256,
        )
        try:
            with self._lock:
                current = self._checked_session()
                if (
                    current.identity != session.identity
                    or current.trade_date != session.trade_date
                    or current.instrument_count
                    != session.instrument_count
                    or current.registry_version
                    != session.registry_version
                    or current.registry_sha256
                    != session.registry_sha256
                ):
                    raise StaleSessionError(
                        "realtime session changed while opening "
                        "instrument delta session"
                    )
            return delta_session
        except Exception:
            delta_session.close()
            raise

    def get_latest_snapshot(self, instrument_id: int):
        return self.get_latest_snapshots((instrument_id,))[0]

    def get_latest_snapshots(
        self, instrument_ids: Sequence[int]
    ) -> LatestBatch:
        ids = _ids(instrument_ids, "instrument_id")
        with self._lock:
            self._checked_session()
            statuses, payloads = self._native.latest_snapshots(ids)
            self._checked_session()
        if len(statuses) != len(ids) or len(payloads) != len(ids):
            raise WireFormatError("native snapshot batch length mismatch")
        results = []
        for instrument_id, status, payload in zip(
            ids, statuses, payloads
        ):
            if (status.value == 0) != (payload is not None):
                raise WireFormatError(
                    "native snapshot status/payload mismatch"
                )
            value = parse_snapshot(payload) if payload is not None else None
            if value is not None:
                validate_snapshot_identity(value, instrument_id)
            results.append(LatestResult(instrument_id, status, value))
        return LatestBatch("snapshot", self._identity, tuple(results))

    def get_latest_tick(self, instrument_id: int):
        return self.get_latest_ticks((instrument_id,))[0]

    def get_latest_ticks(
        self, instrument_ids: Sequence[int]
    ) -> LatestBatch:
        ids = _ids(instrument_ids, "instrument_id")
        with self._lock:
            self._checked_session()
            statuses, payloads = self._native.latest_ticks(ids)
            self._checked_session()
        if len(statuses) != len(ids) or len(payloads) != len(ids):
            raise WireFormatError("native tick batch length mismatch")
        results = []
        for instrument_id, status, payload in zip(
            ids, statuses, payloads
        ):
            if (status.value == 0) != (payload is not None):
                raise WireFormatError("native tick status/payload mismatch")
            value = parse_tick(payload) if payload is not None else None
            if value is not None:
                validate_tick_identity(value, instrument_id)
            results.append(LatestResult(instrument_id, status, value))
        return LatestBatch("tick", self._identity, tuple(results))

    def get_latest_kline(self, instrument_id: int, window_id: int):
        return self.get_latest_klines(
            (instrument_id,), (window_id,)
        )[0]

    def get_latest_klines(
        self,
        instrument_ids: Sequence[int],
        window_ids,
    ) -> LatestBatch:
        ids = _ids(instrument_ids, "instrument_id")
        if isinstance(window_ids, int) and not isinstance(window_ids, bool):
            windows = tuple(
                _uint32(window_ids, "window_id") for _ in ids
            )
        else:
            windows = _ids(window_ids, "window_id")
        if len(ids) != len(windows):
            raise ValueError(
                "instrument_ids and window_ids must have equal length"
            )
        with self._lock:
            self._checked_session()
            statuses, payloads = self._native.latest_klines(ids, windows)
            self._checked_session()
        if len(statuses) != len(ids) or len(payloads) != len(ids):
            raise WireFormatError("native KLine batch length mismatch")
        results = []
        for instrument_id, window_id, status, payload in zip(
            ids, windows, statuses, payloads
        ):
            if (status.value == 0) != (payload is not None):
                raise WireFormatError("native KLine status/payload mismatch")
            value = parse_kline(payload) if payload is not None else None
            if value is not None:
                validate_kline_identity(value, instrument_id, window_id)
            results.append(
                LatestResult(
                    instrument_id,
                    status,
                    value,
                    requested_window_id=window_id,
                )
            )
        return LatestBatch("kline", self._identity, tuple(results))

    def open_tick_cursor(self, start="latest") -> "TickCursor":
        with self._lock:
            session = self._checked_session()
        if start == "earliest":
            sequence = session.oldest_tick_sequence
        elif start == "latest":
            if (
                session.tick_contiguous_published_sequence
                == _UINT64_MAX
            ):
                raise UnavailableError("tick sequence space is exhausted")
            sequence = session.tick_contiguous_published_sequence + 1
        elif isinstance(start, int) and not isinstance(start, bool):
            sequence = _positive_uint64(start, "start")
        else:
            raise ValueError(
                "start must be 'earliest', 'latest', or a positive sequence"
            )
        return TickCursor(self, sequence, session.identity)


class TickCursor:
    """A cursor over the bounded mixed tick ring.

    Reads are contiguous while retained; lagging past ring capacity raises
    TickOverrunError and never silently skips records.
    """

    def __init__(
        self,
        client: L2FlowClient,
        next_sequence: int,
        identity: SessionIdentity,
    ) -> None:
        self._client = client
        self._next_sequence = next_sequence
        self._identity = identity
        self._closed = False
        self._lock = threading.Lock()

    @property
    def session_identity(self) -> SessionIdentity:
        return self._identity

    @property
    def next_sequence(self) -> int:
        return self._next_sequence

    @property
    def closed(self) -> bool:
        return self._closed

    def close(self) -> None:
        with self._lock:
            self._closed = True

    def _require_open(self) -> None:
        if self._closed:
            raise ClientClosedError("tick cursor is closed")

    def read(self, max_records: int = 4096) -> TickBatch:
        if not isinstance(max_records, int) or isinstance(max_records, bool):
            raise TypeError("max_records must be an integer")
        if max_records < 0 or max_records > MAX_BATCH_RECORDS:
            raise ValueError(
                f"max_records must be between 0 and {MAX_BATCH_RECORDS}"
            )
        with self._lock:
            self._require_open()
            with self._client._lock:
                session = self._client._checked_session()
                if session.identity != self._identity:
                    raise StaleSessionError(
                        "tick cursor belongs to another session"
                    )
                first = self._next_sequence
                native_batch = self._client._native.read_ticks(
                    first, max_records
                )
                session = self._client._checked_session()
                if session.identity != self._identity:
                    raise StaleSessionError(
                        "tick cursor session changed during read"
                    )
            ticks = tuple(
                parse_tick(payload) for payload in native_batch.payloads
            )
            for index, tick in enumerate(ticks):
                if tick.common.tick_stream_sequence != first + index:
                    raise WireFormatError(
                        "tick stream returned a non-contiguous sequence"
                    )
            batch = TickBatch(
                self._identity,
                first,
                native_batch.next_sequence,
                ticks,
            )
            self._next_sequence = native_batch.next_sequence
            return batch

    def read_columns(self, max_records: int = 4096) -> TickColumnBatch:
        """Copy one contiguous block without per-row Python model creation.

        The returned client-owned block exposes a zero-copy structured NumPy
        view through :meth:`TickColumnBatch.numpy_records`. Cursor/session and
        overrun behavior is identical to :meth:`read`.
        """

        if not isinstance(max_records, int) or isinstance(max_records, bool):
            raise TypeError("max_records must be an integer")
        if max_records < 0 or max_records > MAX_BATCH_RECORDS:
            raise ValueError(
                f"max_records must be between 0 and {MAX_BATCH_RECORDS}"
            )
        with self._lock:
            self._require_open()
            with self._client._lock:
                session = self._client._checked_session()
                if session.identity != self._identity:
                    raise StaleSessionError(
                        "tick cursor belongs to another session"
                    )
                first = self._next_sequence
                native_batch = self._client._native.read_tick_block(
                    first, max_records
                )
                session = self._client._checked_session()
                if session.identity != self._identity:
                    raise StaleSessionError(
                        "tick cursor session changed during read"
                    )
            batch = TickColumnBatch(
                self._identity,
                first,
                native_batch.next_sequence,
                native_batch.data,
            )
            self._next_sequence = native_batch.next_sequence
            return batch
