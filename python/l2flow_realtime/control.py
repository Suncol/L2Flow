"""AF_UNIX/SOCK_SEQPACKET discovery for the realtime memfd."""

from __future__ import annotations

import os
import secrets
import socket
import struct
from dataclasses import dataclass, field
from typing import Optional, Union

from ._fd_owner import _ReceivedPacket, _recv_fds
from .models import ProtocolError, UnavailableError
from .wire import CONTROL_MAGIC, WIRE_MAJOR, WIRE_MINOR


_REQUEST = struct.Struct("<8sHHHHIIQQ")
_RESPONSE = struct.Struct("<8sHHHHIIQQQQQ")

REQUEST_BYTES = _REQUEST.size
RESPONSE_BYTES = _RESPONSE.size
GET_SESSION_OPCODE = 1
CONTROL_OK = 0
CONTROL_INVALID_REQUEST = 1
CONTROL_UNSUPPORTED_VERSION = 2
CONTROL_UNAVAILABLE = 3

assert REQUEST_BYTES == 40
assert RESPONSE_BYTES == 64


@dataclass(frozen=True, slots=True)
class ControlSession:
    _packet: _ReceivedPacket = field(repr=False)
    request_id: int
    session_epoch: int
    total_mapping_bytes: int

    @property
    def fd(self) -> int:
        """Borrow the mapped-session fd while this object remains open."""

        return self._packet.only_fd

    @property
    def closed(self) -> bool:
        return self._packet.closed

    def close(self) -> None:
        self._packet.close()

    def __enter__(self) -> "ControlSession":
        if self.closed:
            raise RuntimeError("control session is closed")
        return self

    def __exit__(self, _type, _value, _traceback) -> None:
        self.close()


def build_get_session_request(request_id: int) -> bytes:
    if not isinstance(request_id, int) or isinstance(request_id, bool):
        raise TypeError("request_id must be an integer")
    if request_id <= 0 or request_id > 0xFFFFFFFFFFFFFFFF:
        raise ValueError("request_id must be a positive uint64")
    return _REQUEST.pack(
        CONTROL_MAGIC,
        WIRE_MAJOR,
        WIRE_MINOR,
        GET_SESSION_OPCODE,
        0,
        REQUEST_BYTES,
        0,
        request_id,
        0,
    )


def receive_session_fd(
    control_socket: socket.socket, request_id: int
) -> ControlSession:
    """Receive and validate one control response from an existing socket."""

    packet = _recv_fds(
        control_socket,
        RESPONSE_BYTES,
        response_name="control response",
    )
    try:
        data = packet.data
        received_fds = packet.fds
        fields = _RESPONSE.unpack(data)
        (
            magic,
            protocol_major,
            protocol_minor,
            status,
            flags,
            message_bytes,
            reserved0,
            response_request_id,
            session_epoch,
            total_mapping_bytes,
            reserved1,
            reserved2,
        ) = fields
        if magic != CONTROL_MAGIC:
            raise ProtocolError("control response magic mismatch")
        if protocol_major != WIRE_MAJOR or protocol_minor != WIRE_MINOR:
            raise ProtocolError(
                "control response protocol version is unsupported"
            )
        if message_bytes != RESPONSE_BYTES:
            raise ProtocolError("control response message_bytes mismatch")
        if response_request_id != request_id:
            raise ProtocolError("control response request_id mismatch")
        if flags != 0 or reserved0 != 0 or reserved1 != 0 or reserved2 != 0:
            raise ProtocolError("control response reserved fields are nonzero")

        if status != CONTROL_OK:
            if received_fds:
                raise ProtocolError(
                    "failed control response unexpectedly carried an fd"
                )
            if status == CONTROL_UNAVAILABLE:
                raise UnavailableError("realtime control service unavailable")
            if status == CONTROL_INVALID_REQUEST:
                raise ProtocolError("control request was rejected as invalid")
            if status == CONTROL_UNSUPPORTED_VERSION:
                raise ProtocolError(
                    "control service does not support wire protocol V1"
                )
            raise ProtocolError(f"unknown control response status {status}")

        if len(received_fds) != 1:
            raise ProtocolError(
                "successful control response must carry exactly one fd"
            )
        if session_epoch == 0:
            raise ProtocolError("control response session_epoch is zero")
        if total_mapping_bytes < 4096:
            raise ProtocolError("control response mapping is too small")
        return ControlSession(
            _packet=packet,
            request_id=request_id,
            session_epoch=session_epoch,
            total_mapping_bytes=total_mapping_bytes,
        )
    except BaseException:
        packet.close()
        raise


def discover_session_fd(
    control_socket_path: Union[str, os.PathLike],
    *,
    timeout: Optional[float] = 1.0,
    request_id: Optional[int] = None,
) -> ControlSession:
    path = os.fspath(control_socket_path)
    nul = b"\x00" if isinstance(path, bytes) else "\x00"
    if not path or nul in path:
        raise ValueError("control_socket_path must be non-empty")
    if not os.path.isabs(path):
        raise ValueError("control_socket_path must be absolute")
    if timeout is not None and timeout <= 0:
        raise ValueError("timeout must be positive or None")
    if request_id is None:
        request_id = secrets.randbits(64) or 1
    request = build_get_session_request(request_id)
    with socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET) as channel:
        channel.settimeout(timeout)
        channel.connect(path)
        sent = channel.send(request)
        if sent != REQUEST_BYTES:
            raise ProtocolError(
                f"short control request send: {sent} of {REQUEST_BYTES}"
            )
        return receive_session_fd(channel, request_id)
