"""Validated Unix control-plane attachment for live order-event deltas."""

from __future__ import annotations

import ctypes
import fcntl
import math
import os
import socket
import stat
import struct
import time
from dataclasses import dataclass
from typing import Optional

from ._fd_owner import _recv_fds
from ._stream_control import (
    request_id as validated_request_id,
    validate_socket_path,
    validate_timeout,
)
from .models import (
    ProtocolError,
    StaleSessionError,
    UnavailableError,
)
from .order_event_delta_live import (
    LiveOrderEventDeltaProducerState,
    LiveOrderEventDeltaReader,
    LiveOrderEventDeltaSession,
)


CONTROL_MAGIC = b"L2FECT1\0"
CONTROL_MAJOR = 1
CONTROL_MINOR = 0
GET_SESSION = 1
CONTROL_OK = 0
CONTROL_INVALID_REQUEST = 1
CONTROL_UNSUPPORTED_VERSION = 2
CONTROL_UNAVAILABLE = 3
CONTROL_SOURCE_SESSION_MISMATCH = 4
CONTROL_INTERNAL_ERROR = 5

_REQUEST = struct.Struct("<8sHHHHIIQ16sQIIQQ")
_RESPONSE = struct.Struct(
    "<8sHHHHIIQ16sQII16sQII" + "Q" * 12
)
_UCRED = struct.Struct("3i")
_UINT32_MAX = (1 << 32) - 1
_UINT64_MAX = (1 << 64) - 1
_MAXIMUM_TIMEOUT_SECONDS = 60.0
_REQUIRED_SEALS = (
    getattr(fcntl, "F_SEAL_SEAL", 0x0001)
    | getattr(fcntl, "F_SEAL_SHRINK", 0x0002)
    | getattr(fcntl, "F_SEAL_GROW", 0x0004)
    | getattr(fcntl, "F_SEAL_FUTURE_WRITE", 0x0010)
)

assert _REQUEST.size == 80
assert _RESPONSE.size == 192
assert _UCRED.size == 12


class LiveOrderEventDeltaControlError(UnavailableError):
    """The event-aggregator control transport could not attach."""


class LiveOrderEventDeltaPeerCredentialError(
    LiveOrderEventDeltaControlError
):
    """The Unix control peer is not owned by the effective client UID."""


class LiveOrderEventDeltaSourceSessionMismatchError(
    StaleSessionError
):
    """The event aggregator is attached to another Wire V2 source session."""


def _positive_integer(value: object, field: str, maximum: int) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{field} must be an integer")
    if value <= 0 or value > maximum:
        raise ValueError(f"{field} is outside its ABI domain")
    return value


def _counter(value: object, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{field} must be an integer")
    if value < 0 or value > _UINT64_MAX:
        raise ValueError(f"{field} must fit uint64")
    return value


@dataclass(frozen=True, slots=True)
class LiveOrderEventDeltaSourceSession:
    run_id: bytes
    session_epoch: int
    trade_date: int

    def __post_init__(self) -> None:
        if (
            not isinstance(self.run_id, bytes)
            or len(self.run_id) != 16
            or not any(self.run_id)
        ):
            raise ValueError("source run_id must be 16 nonzero bytes")
        _positive_integer(
            self.session_epoch, "source session_epoch", _UINT64_MAX
        )
        _positive_integer(
            self.trade_date, "source trade_date", _UINT32_MAX
        )


@dataclass(frozen=True, slots=True)
class LiveOrderEventDeltaControlSnapshot:
    request_id: int
    source_session: LiveOrderEventDeltaSourceSession
    event_session: LiveOrderEventDeltaSession
    event_published_sequence: int
    source_tick_consumed_sequence: int
    heartbeat_monotonic_ns: int
    producer_started_monotonic_ns: int
    event_producer_state: LiveOrderEventDeltaProducerState
    event_header_flags: int
    peer_pid: int
    peer_uid: int
    peer_gid: int

    def __post_init__(self) -> None:
        _positive_integer(self.request_id, "request_id", _UINT64_MAX)
        if not isinstance(
            self.source_session, LiveOrderEventDeltaSourceSession
        ):
            raise TypeError("source_session has the wrong type")
        if not isinstance(self.event_session, LiveOrderEventDeltaSession):
            raise TypeError("event_session has the wrong type")
        if (
            self.event_session.trade_date
            != self.source_session.trade_date
        ):
            raise ValueError("source and event trading dates disagree")
        for field, value in (
            (
                "event_published_sequence",
                self.event_published_sequence,
            ),
            (
                "source_tick_consumed_sequence",
                self.source_tick_consumed_sequence,
            ),
            ("heartbeat_monotonic_ns", self.heartbeat_monotonic_ns),
            (
                "producer_started_monotonic_ns",
                self.producer_started_monotonic_ns,
            ),
        ):
            _counter(value, field)
        state = LiveOrderEventDeltaProducerState(
            self.event_producer_state
        )
        if state is not LiveOrderEventDeltaProducerState.ACTIVE:
            raise ValueError("control response event producer is not ACTIVE")
        if self.event_header_flags != 0:
            raise ValueError("control response reports event coverage loss")
        if (
            not isinstance(self.peer_pid, int)
            or self.peer_pid <= 0
            or not isinstance(self.peer_uid, int)
            or self.peer_uid < 0
            or not isinstance(self.peer_gid, int)
            or self.peer_gid < 0
        ):
            raise ValueError("invalid Unix peer credentials")
        object.__setattr__(self, "event_producer_state", state)


def build_live_order_event_get_session_request(
    expected_source_session: LiveOrderEventDeltaSourceSession,
    request_id: Optional[int] = None,
) -> tuple[int, bytes]:
    if not isinstance(
        expected_source_session, LiveOrderEventDeltaSourceSession
    ):
        raise TypeError(
            "expected_source_session has the wrong type"
        )
    identifier = validated_request_id(request_id)
    return identifier, _REQUEST.pack(
        CONTROL_MAGIC,
        CONTROL_MAJOR,
        CONTROL_MINOR,
        GET_SESSION,
        0,
        _REQUEST.size,
        0,
        identifier,
        expected_source_session.run_id,
        expected_source_session.session_epoch,
        expected_source_session.trade_date,
        0,
        0,
        0,
    )


def _peer_credentials(
    channel: socket.socket,
) -> tuple[int, int, int]:
    peer_option = getattr(socket, "SO_PEERCRED", None)
    if peer_option is None:
        raise LiveOrderEventDeltaPeerCredentialError(
            "SO_PEERCRED is unavailable on this platform"
        )
    try:
        payload = channel.getsockopt(
            socket.SOL_SOCKET, peer_option, _UCRED.size
        )
    except OSError as error:
        raise LiveOrderEventDeltaPeerCredentialError(
            f"SO_PEERCRED failed: {error}"
        ) from error
    if not isinstance(payload, bytes) or len(payload) != _UCRED.size:
        raise LiveOrderEventDeltaPeerCredentialError(
            "SO_PEERCRED returned a malformed credential record"
        )
    pid, uid, gid = _UCRED.unpack(payload)
    if pid <= 0 or uid != os.geteuid():
        raise LiveOrderEventDeltaPeerCredentialError(
            "event control peer UID does not match effective UID"
        )
    return pid, uid, gid


def _validate_ring_descriptor(
    descriptor: int,
    session: LiveOrderEventDeltaSession,
) -> None:
    try:
        access_flags = fcntl.fcntl(descriptor, fcntl.F_GETFL)
        descriptor_flags = fcntl.fcntl(descriptor, fcntl.F_GETFD)
        seals = fcntl.fcntl(
            descriptor, getattr(fcntl, "F_GET_SEALS", 1034)
        )
        descriptor_stat = os.fstat(descriptor)
    except OSError as error:
        raise ProtocolError(
            f"event ring descriptor validation failed: {error}"
        ) from error
    if (
        (access_flags & os.O_ACCMODE) != os.O_RDONLY
        or not (descriptor_flags & fcntl.FD_CLOEXEC)
        or (seals & _REQUIRED_SEALS) != _REQUIRED_SEALS
        or not stat.S_ISREG(descriptor_stat.st_mode)
        or descriptor_stat.st_size != session.total_mapping_bytes
    ):
        raise ProtocolError(
            "event control supplied a noncanonical ring descriptor"
        )


def _raise_control_status(status: int) -> None:
    if status == CONTROL_OK:
        return
    if status == CONTROL_INVALID_REQUEST:
        raise ProtocolError("event control rejected a valid request")
    if status == CONTROL_UNSUPPORTED_VERSION:
        raise ProtocolError("event control protocol version unsupported")
    if status == CONTROL_UNAVAILABLE:
        raise LiveOrderEventDeltaControlError(
            "event producer is unavailable"
        )
    if status == CONTROL_SOURCE_SESSION_MISMATCH:
        raise LiveOrderEventDeltaSourceSessionMismatchError(
            "event producer source session does not match Wire V2"
        )
    if status == CONTROL_INTERNAL_ERROR:
        raise LiveOrderEventDeltaControlError(
            "event control reported an internal failure"
        )
    raise ProtocolError(f"unknown event control status {status}")


def _snapshot_from_response(
    fields: tuple,
    expected_source_session: LiveOrderEventDeltaSourceSession,
    expected_request_id: int,
    peer_credentials: tuple[int, int, int],
) -> LiveOrderEventDeltaControlSnapshot:
    (
        magic,
        major,
        minor,
        status,
        flags,
        message_bytes,
        reserved0,
        response_request_id,
        source_run_id,
        source_session_epoch,
        source_trade_date,
        event_producer_state,
        event_run_id,
        event_session_epoch,
        event_trade_date,
        event_header_flags,
        event_ring_capacity,
        event_total_mapping_bytes,
        event_published_sequence,
        source_tick_consumed_sequence,
        heartbeat_monotonic_ns,
        producer_started_monotonic_ns,
        *reserved,
    ) = fields
    if (
        magic != CONTROL_MAGIC
        or (major, minor) != (CONTROL_MAJOR, CONTROL_MINOR)
        or flags != 0
        or message_bytes != _RESPONSE.size
        or reserved0 != 0
        or response_request_id != expected_request_id
        or any(reserved)
    ):
        raise ProtocolError("malformed event control response envelope")
    _raise_control_status(status)
    if (
        source_run_id != expected_source_session.run_id
        or source_session_epoch
        != expected_source_session.session_epoch
        or source_trade_date != expected_source_session.trade_date
    ):
        raise LiveOrderEventDeltaSourceSessionMismatchError(
            "event control response source identity changed"
        )
    source_session = expected_source_session
    try:
        event_session = LiveOrderEventDeltaSession(
            run_id=event_run_id,
            session_epoch=event_session_epoch,
            trade_date=event_trade_date,
            ring_capacity=event_ring_capacity,
            total_mapping_bytes=event_total_mapping_bytes,
        )
        return LiveOrderEventDeltaControlSnapshot(
            request_id=response_request_id,
            source_session=source_session,
            event_session=event_session,
            event_published_sequence=event_published_sequence,
            source_tick_consumed_sequence=(
                source_tick_consumed_sequence
            ),
            heartbeat_monotonic_ns=heartbeat_monotonic_ns,
            producer_started_monotonic_ns=(
                producer_started_monotonic_ns
            ),
            event_producer_state=event_producer_state,
            event_header_flags=event_header_flags,
            peer_pid=peer_credentials[0],
            peer_uid=peer_credentials[1],
            peer_gid=peer_credentials[2],
        )
    except (TypeError, ValueError) as error:
        raise ProtocolError(
            f"invalid successful event control response: {error}"
        ) from error


def _resolve_native_library(
    native_library,
    native_library_path,
):
    if native_library is not None and native_library_path is not None:
        raise ValueError(
            "native_library and native_library_path are mutually exclusive"
        )
    if native_library is not None:
        return native_library
    if native_library_path is None:
        raise ValueError(
            "native_library or native_library_path is required"
        )
    return ctypes.CDLL(os.fspath(native_library_path), use_errno=True)


def open_live_order_events(
    control_socket_path,
    *,
    expected_source_session: LiveOrderEventDeltaSourceSession,
    native_library=None,
    native_library_path=None,
    timeout: float = 1.0,
    batch_records: int = 4096,
    request_id: Optional[int] = None,
    _socket_factory=socket.socket,
) -> LiveOrderEventDeltaReader:
    """Connect, validate one event session, and map its O_RDONLY ring fd."""

    path = validate_socket_path(control_socket_path)
    timeout = validate_timeout(timeout)
    if (
        timeout is None
        or not math.isfinite(timeout)
        or timeout > _MAXIMUM_TIMEOUT_SECONDS
    ):
        raise ValueError(
            "event control timeout must be finite and at most 60 seconds"
        )
    identifier, request = build_live_order_event_get_session_request(
        expected_source_session, request_id
    )
    LiveOrderEventDeltaReader._validate_batch_records(batch_records)
    library = _resolve_native_library(
        native_library, native_library_path
    )
    try:
        channel = _socket_factory(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    except OSError as error:
        raise LiveOrderEventDeltaControlError(
            f"event control socket creation failed: {error}"
        ) from error
    try:
        with channel:
            deadline = time.monotonic() + timeout

            def set_remaining_timeout() -> None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise socket.timeout(
                        "event control request deadline expired"
                    )
                channel.settimeout(remaining)

            set_remaining_timeout()
            channel.connect(path)
            peer = _peer_credentials(channel)
            set_remaining_timeout()
            sent = channel.send(request)
            if sent != len(request):
                raise ProtocolError(
                    "short event control GET_SESSION request"
                )
            set_remaining_timeout()
            packet = _recv_fds(
                channel,
                _RESPONSE.size,
                response_name="event control GET_SESSION response",
            )
    except socket.timeout as error:
        raise LiveOrderEventDeltaControlError(
            "event control request timed out"
        ) from error
    except OSError as error:
        raise LiveOrderEventDeltaControlError(
            f"event control transport failed: {error}"
        ) from error

    with packet:
        fields = _RESPONSE.unpack(packet.data)
        status = fields[3]
        if status == CONTROL_OK:
            if len(packet.fds) != 1:
                raise ProtocolError(
                    "successful event control response requires one fd"
                )
        elif packet.fds:
            raise ProtocolError(
                "failed event control response unexpectedly carried an fd"
            )
        snapshot = _snapshot_from_response(
            fields,
            expected_source_session,
            identifier,
            peer,
        )
        descriptor = packet.only_fd
        _validate_ring_descriptor(
            descriptor, snapshot.event_session
        )
        reader = LiveOrderEventDeltaReader.open(
            library,
            descriptor,
            snapshot.event_session,
            batch_records=batch_records,
            take_fd_ownership=False,
            _control_snapshot=snapshot,
        )
        try:
            if (
                reader.session != snapshot.event_session
                or reader.producer_state()
                is not LiveOrderEventDeltaProducerState.ACTIVE
            ):
                raise LiveOrderEventDeltaControlError(
                    "event ring changed during control attachment"
                )
            return reader
        except BaseException:
            reader.close()
            raise


__all__ = [
    "LiveOrderEventDeltaControlError",
    "LiveOrderEventDeltaControlSnapshot",
    "LiveOrderEventDeltaPeerCredentialError",
    "LiveOrderEventDeltaSourceSession",
    "LiveOrderEventDeltaSourceSessionMismatchError",
    "build_live_order_event_get_session_request",
    "open_live_order_events",
]
