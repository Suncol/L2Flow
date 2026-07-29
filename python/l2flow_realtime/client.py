"""Small, read-only client for one observed-universe Wire V2 session."""

from __future__ import annotations

import os
import threading
import time
from typing import TYPE_CHECKING, Optional, Sequence, Union

from ._stream_control import validate_socket_path, validate_timeout
from .control import discover_session_fd
from .models import (
    ClientClosedError,
    Instrument,
    InstrumentKey,
    InstrumentLookupResult,
    LatestKLine,
    LatestSnapshot,
    LatestTick,
    SelectionEnvelope,
    SelectionScope,
    ServerState,
    SessionIdentity,
    StaleSessionError,
    UnavailableError,
    WireFormatError,
)
from .native import MAX_BATCH_RECORDS, NativeReader


if TYPE_CHECKING:
    from .history import HistoryCursor
    from .instrument_delta import InstrumentTickDeltaSession


DEFAULT_STALE_AFTER_NS = 3_000_000_000
MAX_HEALTH_CHECK_INTERVAL_NS = 100_000_000


def _uint32(value, field: str) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or value < 0
        or value > 0xFFFFFFFF
    ):
        raise ValueError(f"{field} must fit uint32")
    return value


def _ids(values: Sequence[int], field: str) -> tuple[int, ...]:
    if isinstance(values, (str, bytes, bytearray)):
        raise TypeError(f"{field} must be a sequence of integers")
    result = tuple(_uint32(value, field) for value in values)
    if len(result) > MAX_BATCH_RECORDS:
        raise ValueError(
            f"{field} exceeds the {MAX_BATCH_RECORDS}-record limit"
        )
    return result


class L2FlowClient:
    """One mapped session; numeric instrument IDs never cross its epoch."""

    def __init__(
        self,
        native_reader,
        *,
        stale_after_ns: Optional[int] = DEFAULT_STALE_AFTER_NS,
        expected_identity: Optional[SessionIdentity] = None,
        control_socket_path: Optional[Union[str, os.PathLike]] = None,
        control_timeout: Optional[float] = 1.0,
    ) -> None:
        if (
            stale_after_ns is not None
            and (
                not isinstance(stale_after_ns, int)
                or isinstance(stale_after_ns, bool)
                or stale_after_ns <= 0
            )
        ):
            raise ValueError("stale_after_ns must be positive or None")
        self._native = native_reader
        self._stale_after_ns = stale_after_ns
        self._control_socket_path = (
            None
            if control_socket_path is None
            else validate_socket_path(control_socket_path)
        )
        self._control_timeout = validate_timeout(control_timeout)
        self._lock = threading.RLock()
        self._closed = False
        self._health_check_interval_ns = (
            None
            if stale_after_ns is None
            else min(
                MAX_HEALTH_CHECK_INTERVAL_NS,
                max(1, stale_after_ns // 4),
            )
        )
        self._next_health_check_ns: Optional[int] = None
        try:
            initial = native_reader.session()
            self._identity = initial.identity
            if (
                expected_identity is not None
                and expected_identity != self._identity
            ):
                raise StaleSessionError(
                    "control response and mapping identities differ"
                )
            self._validate_session(initial)
            if self._health_check_interval_ns is not None:
                self._next_health_check_ns = (
                    time.monotonic_ns()
                    + self._health_check_interval_ns
                )
        except BaseException:
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
                native = (
                    NativeReader.open_fd(
                        control.fd, library_path=native_library
                    )
                    if _native_factory is None
                    else _native_factory(control.fd)
                )
                if native is None:
                    raise WireFormatError(
                        "native reader factory returned None"
                    )
                session = native.session()
                if session.session_epoch != control.session_epoch:
                    raise StaleSessionError(
                        "control response epoch does not match mapping"
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
            raise StaleSessionError("mapped realtime session changed")
        self._validate_health(session)

    def _validate_health(self, health) -> None:
        if health.session_epoch != self._identity.session_epoch:
            raise StaleSessionError("mapped realtime session changed")
        if health.coverage_lost:
            raise UnavailableError("shared-memory coverage was lost")
        if health.server_state in (
            ServerState.INITIALIZING,
            ServerState.FAILED,
        ):
            raise UnavailableError(
                f"realtime server state is {health.server_state.name}"
            )
        if (
            self._stale_after_ns is not None
            and health.server_state
            in (ServerState.ACTIVE, ServerState.DRAINING)
        ):
            heartbeat = health.heartbeat_monotonic_ns
            now = time.monotonic_ns()
            if heartbeat == 0 or (
                now >= heartbeat
                and now - heartbeat > self._stale_after_ns
            ):
                raise StaleSessionError("realtime heartbeat is stale")

    def _checked_session(self):
        self._require_open()
        session = self._native.session()
        self._validate_session(session)
        if self._health_check_interval_ns is not None:
            self._next_health_check_ns = (
                time.monotonic_ns() + self._health_check_interval_ns
            )
        return session

    def _maybe_check_health(self) -> None:
        self._require_open()
        interval = self._health_check_interval_ns
        if interval is None:
            return
        now = time.monotonic_ns()
        if (
            self._next_health_check_ns is not None
            and now < self._next_health_check_ns
        ):
            return
        health = self._native.health()
        self._validate_health(health)
        self._next_health_check_ns = now + interval

    def session_info(self):
        with self._lock:
            return self._checked_session()

    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self._native.close()
                self._closed = True

    def __enter__(self) -> "L2FlowClient":
        with self._lock:
            self._checked_session()
        return self

    def __exit__(self, _type, _value, _traceback) -> None:
        self.close()

    def instrument(self, instrument_id: int) -> Instrument:
        instrument_id = _uint32(instrument_id, "instrument_id")
        with self._lock:
            self._checked_session()
            result = self._native.instrument(instrument_id)
            self._checked_session()
            if (
                not isinstance(result, Instrument)
                or result.session_epoch != self._identity.session_epoch
                or result.instrument_id != instrument_id
            ):
                raise WireFormatError(
                    "native instrument result has the wrong session or ID"
                )
            return result

    def resolve_key(self, key: InstrumentKey) -> InstrumentLookupResult:
        return self.resolve_keys((key,))[0]

    def resolve_keys(
        self, keys: Sequence[InstrumentKey]
    ) -> tuple[InstrumentLookupResult, ...]:
        if isinstance(keys, (str, bytes, bytearray)):
            raise TypeError("keys must be a sequence of InstrumentKey values")
        keys = tuple(keys)
        if any(not isinstance(key, InstrumentKey) for key in keys):
            raise TypeError("every key must be an InstrumentKey")
        with self._lock:
            self._checked_session()
            results = tuple(self._native.resolve_instruments(keys))
            self._checked_session()
            if len(results) != len(keys):
                raise WireFormatError(
                    "native key lookup batch length mismatch"
                )
            for key, result in zip(keys, results):
                if (
                    not isinstance(result, InstrumentLookupResult)
                    or result.key != key
                    or result.session_epoch
                    != self._identity.session_epoch
                ):
                    raise WireFormatError(
                        "native key lookup result has wrong identity"
                    )
            return results

    def select(
        self, scope: Union[SelectionScope, int]
    ) -> SelectionEnvelope:
        try:
            scope = SelectionScope(scope)
        except (TypeError, ValueError) as error:
            raise ValueError("scope is not a Wire V2 selection") from error
        with self._lock:
            self._checked_session()
            result = self._native.select_instruments(scope)
            self._checked_session()
            if (
                not isinstance(result, SelectionEnvelope)
                or result.session_epoch != self._identity.session_epoch
                or result.run_id != self._identity.run_id
                or result.selection_scope is not scope
            ):
                raise WireFormatError(
                    "native selection has the wrong session or scope"
                )
            return result

    def latest_snapshot(self, instrument_id: int) -> LatestSnapshot:
        return self.latest_snapshots((instrument_id,))[0]

    def latest_snapshots(
        self, instrument_ids: Sequence[int]
    ) -> tuple[LatestSnapshot, ...]:
        ids = _ids(instrument_ids, "instrument_id")
        with self._lock:
            # The native point read checks server state/coverage both before
            # and after copying. Throttled heartbeat validation reads only the
            # fixed health words, never catalog/count/progress status.
            self._require_open()
            results = tuple(self._native.latest_snapshots(ids))
            self._maybe_check_health()
            self._validate_latest(results, ids, LatestSnapshot)
            return results

    def latest_tick(self, instrument_id: int) -> LatestTick:
        return self.latest_ticks((instrument_id,))[0]

    def latest_ticks(
        self, instrument_ids: Sequence[int]
    ) -> tuple[LatestTick, ...]:
        ids = _ids(instrument_ids, "instrument_id")
        with self._lock:
            self._require_open()
            results = tuple(self._native.latest_ticks(ids))
            self._maybe_check_health()
            self._validate_latest(results, ids, LatestTick)
            return results

    def latest_kline(
        self, instrument_id: int, window_id: int
    ) -> LatestKLine:
        return self.latest_klines(
            (instrument_id,), (window_id,)
        )[0]

    def latest_klines(
        self,
        instrument_ids: Sequence[int],
        window_ids: Sequence[int],
    ) -> tuple[LatestKLine, ...]:
        ids = _ids(instrument_ids, "instrument_id")
        windows = _ids(window_ids, "window_id")
        if len(ids) != len(windows):
            raise ValueError("instrument_ids and window_ids lengths differ")
        with self._lock:
            self._require_open()
            results = tuple(self._native.latest_klines(ids, windows))
            self._maybe_check_health()
            self._validate_latest(results, ids, LatestKLine)
            if any(
                result.window_id != window_id
                for result, window_id in zip(results, windows)
            ):
                raise WireFormatError(
                    "native KLine result has wrong window ID"
                )
            return results

    def open_instrument_history(
        self,
        instrument_id: int,
        *,
        requested_page_records: int = 4096,
        expected_generation: Optional[int] = None,
    ) -> "HistoryCursor":
        """Pin one immutable V2 generation on an independent cursor socket."""

        from .history import _open_instrument_history

        with self._lock:
            session = self._checked_session()
            path = self._control_socket_path
            if path is None:
                raise UnavailableError(
                    "history requires a client opened through the "
                    "Wire V2 control socket"
                )
            run_id = session.run_id
            session_epoch = session.session_epoch
            trade_date = session.trade_date
            capacity = session.capacity
        # Socket connect and OPEN are deliberately outside the client lock:
        # a slow history service cannot stall latest_snapshot/latest_tick.
        cursor = _open_instrument_history(
            path,
            instrument_id=instrument_id,
            requested_page_records=requested_page_records,
            expected_generation=(
                0
                if expected_generation is None
                else expected_generation
            ),
            expected_run_id=run_id,
            expected_session_epoch=session_epoch,
            expected_trade_date=trade_date,
            expected_capacity=capacity,
            timeout=self._control_timeout,
        )
        try:
            with self._lock:
                self._checked_session()
                if self._identity != cursor.generation.session_identity:
                    raise StaleSessionError(
                        "history cursor belongs to another session"
                    )
            return cursor
        except BaseException:
            cursor.close()
            raise

    def open_instrument_tick_delta_session(
        self,
        *,
        expected_generation: Optional[int] = None,
    ) -> "InstrumentTickDeltaSession":
        """Pin one immutable V2 delta target on an independent socket."""

        from .instrument_delta import (
            _open_instrument_tick_delta_session,
        )

        with self._lock:
            session = self._checked_session()
            path = self._control_socket_path
            if path is None:
                raise UnavailableError(
                    "tick deltas require a client opened through the "
                    "Wire V2 control socket"
                )
            run_id = session.run_id
            session_epoch = session.session_epoch
            trade_date = session.trade_date
            capacity = session.capacity
        delta_session = _open_instrument_tick_delta_session(
            path,
            expected_generation=(
                0
                if expected_generation is None
                else expected_generation
            ),
            expected_run_id=run_id,
            expected_session_epoch=session_epoch,
            expected_trade_date=trade_date,
            expected_capacity=capacity,
            timeout=self._control_timeout,
        )
        try:
            with self._lock:
                self._checked_session()
                if (
                    self._identity
                    != delta_session.generation.session_identity
                ):
                    raise StaleSessionError(
                        "delta session belongs to another realtime session"
                    )
            return delta_session
        except BaseException:
            delta_session.close()
            raise

    def _validate_latest(self, results, ids, expected_type) -> None:
        if len(results) != len(ids):
            raise WireFormatError("native latest batch length mismatch")
        for result, instrument_id in zip(results, ids):
            if (
                not isinstance(result, expected_type)
                or result.session_epoch != self._identity.session_epoch
                or result.instrument_id != instrument_id
            ):
                raise WireFormatError(
                    "native latest result has wrong session or ID"
                )
