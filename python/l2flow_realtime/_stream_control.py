"""Shared V2-only helpers for stateful history control connections."""

from __future__ import annotations

import fcntl
import os
import secrets
import socket
import stat
import struct
import sys
from typing import Optional, Union

from ._fd_owner import _ReceivedPacket, _recv_fds
from .models import (
    L2FlowRealtimeError,
    ProtocolError,
    StaleSessionError,
    UnavailableError,
    WireFormatError,
)
from .wire import CONTROL_MAGIC, WIRE_MAJOR, WIRE_MINOR


OK = 0
INVALID_REQUEST = 1
UNSUPPORTED_VERSION = 2
UNAVAILABLE = 3
NOT_FOUND = 4
RESOURCE_EXHAUSTED = 5
INTERNAL_FAILURE = 6
CHECKPOINT_MISMATCH = 7

TERMINAL_FLAG = 1 << 0
CONTROL_RESPONSE_PREFIX = struct.Struct("<8sHHHHIIQ")
UINT32_MAX = (1 << 32) - 1
UINT64_MAX = (1 << 64) - 1
MAXIMUM_PAGE_RECORDS = 1_048_576


class StreamNotFoundError(L2FlowRealtimeError):
    """The pinned observed generation does not contain the requested ID."""


class StreamResourceExhaustedError(L2FlowRealtimeError):
    """The service could not allocate a cursor or immutable page."""


class StreamInternalFailureError(UnavailableError):
    """The service failed an immutable-history projection invariant."""


class StreamCheckpointMismatchError(StaleSessionError):
    """A supplied checkpoint is not a predecessor of the pinned target."""


def positive_uint32(value: object, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{field} must be an integer")
    if value <= 0 or value > UINT32_MAX:
        raise ValueError(f"{field} must be a positive uint32")
    return value


def positive_uint64(value: object, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{field} must be an integer")
    if value <= 0 or value > UINT64_MAX:
        raise ValueError(f"{field} must be a positive uint64")
    return value


def request_id(value: Optional[int] = None) -> int:
    return (
        positive_uint64(value, "request_id")
        if value is not None
        else (secrets.randbits(64) or 1)
    )


def validate_socket_path(
    value: Union[str, os.PathLike],
) -> Union[str, bytes]:
    path = os.fspath(value)
    if not isinstance(path, (str, bytes)):
        raise TypeError("control_socket_path must be path-like")
    nul = b"\x00" if isinstance(path, bytes) else "\x00"
    if not path or nul in path:
        raise ValueError("control_socket_path must be non-empty")
    if not os.path.isabs(path):
        raise ValueError("control_socket_path must be absolute")
    return path


def validate_timeout(timeout: Optional[float]) -> Optional[float]:
    if timeout is None:
        return None
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
        raise TypeError("timeout must be a number or None")
    if timeout <= 0:
        raise ValueError("timeout must be positive or None")
    return float(timeout)


def validate_page_records(value: object) -> int:
    result = positive_uint32(value, "requested_page_records")
    if result > MAXIMUM_PAGE_RECORDS:
        raise ValueError(
            "requested_page_records exceeds the absolute V2 limit"
        )
    return result


def send_packet(channel: socket.socket, payload: bytes, name: str) -> None:
    sent = channel.send(payload)
    if sent != len(payload):
        raise ProtocolError(
            f"short {name} request send: {sent} of {len(payload)}"
        )


def recv_packet(
    channel: socket.socket, expected_bytes: int, name: str
) -> _ReceivedPacket:
    return _recv_fds(
        channel, expected_bytes, response_name=f"{name} response"
    )


def validate_response_prefix(
    data: bytes,
    *,
    expected_bytes: int,
    expected_request_id: int,
    allowed_flags: int,
    operation: str,
    record_count_field: bool = False,
) -> tuple[int, int, int]:
    (
        magic,
        major,
        minor,
        status,
        flags,
        message_bytes,
        reserved_or_count,
        response_id,
    ) = CONTROL_RESPONSE_PREFIX.unpack_from(data)
    if magic != CONTROL_MAGIC:
        raise ProtocolError(f"{operation}: control magic mismatch")
    if (major, minor) != (WIRE_MAJOR, WIRE_MINOR):
        raise ProtocolError(
            f"{operation}: protocol is not Wire "
            f"{WIRE_MAJOR}.{WIRE_MINOR}"
        )
    if message_bytes != expected_bytes:
        raise ProtocolError(f"{operation}: message_bytes mismatch")
    if response_id != expected_request_id:
        raise ProtocolError(f"{operation}: request_id mismatch")
    if flags & ~allowed_flags:
        raise ProtocolError(f"{operation}: unknown response flags")
    if not record_count_field and reserved_or_count != 0:
        raise ProtocolError(f"{operation}: reserved field is nonzero")
    return status, flags, reserved_or_count


def raise_status(operation: str, status: int) -> None:
    if status == OK:
        return
    if status == INVALID_REQUEST:
        raise ProtocolError(f"{operation}: invalid request rejected")
    if status == UNSUPPORTED_VERSION:
        raise ProtocolError(f"{operation}: Wire V2 operation unsupported")
    if status == UNAVAILABLE:
        raise UnavailableError(
            f"{operation}: no healthy immutable generation is available"
        )
    if status == NOT_FOUND:
        raise StreamNotFoundError(
            f"{operation}: ID is absent from the pinned observed generation"
        )
    if status == RESOURCE_EXHAUSTED:
        raise StreamResourceExhaustedError(
            f"{operation}: cursor/page resources are exhausted"
        )
    if status == INTERNAL_FAILURE:
        raise StreamInternalFailureError(
            f"{operation}: service failed an internal invariant"
        )
    if status == CHECKPOINT_MISMATCH:
        raise StreamCheckpointMismatchError(
            f"{operation}: checkpoint does not match the pinned target"
        )
    raise ProtocolError(f"{operation}: unknown control status {status}")


def validate_page_fd(fd: int, expected_bytes: int) -> None:
    if expected_bytes < 4096:
        raise WireFormatError("immutable page is smaller than its header")
    if expected_bytes > sys.maxsize:
        raise WireFormatError("immutable page exceeds local mmap limits")
    descriptor_flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    if descriptor_flags & os.O_ACCMODE != os.O_RDONLY:
        raise WireFormatError("immutable page fd is not read-only")
    seals = fcntl.fcntl(
        fd, getattr(fcntl, "F_GET_SEALS", 1034)
    )
    required = (
        getattr(fcntl, "F_SEAL_WRITE", 0x0008)
        | getattr(fcntl, "F_SEAL_GROW", 0x0004)
        | getattr(fcntl, "F_SEAL_SHRINK", 0x0002)
        | getattr(fcntl, "F_SEAL_SEAL", 0x0001)
    )
    if seals & required != required:
        raise WireFormatError("immutable page fd lacks required seals")
    descriptor_stat = os.fstat(fd)
    if (
        not stat.S_ISREG(descriptor_stat.st_mode)
        or descriptor_stat.st_size != expected_bytes
    ):
        raise WireFormatError("immutable page fd size or type is invalid")


__all__ = [
    "CHECKPOINT_MISMATCH",
    "CONTROL_RESPONSE_PREFIX",
    "INTERNAL_FAILURE",
    "INVALID_REQUEST",
    "MAXIMUM_PAGE_RECORDS",
    "NOT_FOUND",
    "OK",
    "RESOURCE_EXHAUSTED",
    "StreamCheckpointMismatchError",
    "StreamInternalFailureError",
    "StreamNotFoundError",
    "StreamResourceExhaustedError",
    "TERMINAL_FLAG",
    "UINT32_MAX",
    "UINT64_MAX",
    "UNAVAILABLE",
    "UNSUPPORTED_VERSION",
    "positive_uint32",
    "positive_uint64",
    "raise_status",
    "recv_packet",
    "request_id",
    "send_packet",
    "validate_page_fd",
    "validate_page_records",
    "validate_response_prefix",
    "validate_socket_path",
    "validate_timeout",
]
