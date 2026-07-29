"""Internal ownership for file descriptors received through SCM_RIGHTS."""

from __future__ import annotations

import array
import os
import socket
from typing import Iterable, Tuple

from .models import ProtocolError


class _ReceivedPacket:
    """One received packet that exclusively owns all attached descriptors."""

    __slots__ = ("data", "_fds")

    def __init__(
        self, data: bytes = b"", fds: Iterable[int] = ()
    ) -> None:
        self.data = data
        self._fds = array.array("i", fds)

    @property
    def fds(self) -> Tuple[int, ...]:
        """Borrow the owned descriptors for the packet's context lifetime."""

        return tuple(self._fds)

    @property
    def only_fd(self) -> int:
        if len(self._fds) != 1:
            raise RuntimeError("packet does not own exactly one descriptor")
        return self._fds[0]

    @property
    def closed(self) -> bool:
        return not self._fds

    def _adopt_bytes(self, payload: bytes) -> None:
        self._fds.frombytes(payload)

    def close(self) -> None:
        while self._fds:
            descriptor = self._fds.pop()
            try:
                os.close(descriptor)
            except OSError:
                pass

    def __enter__(self) -> "_ReceivedPacket":
        return self

    def __exit__(self, _type, _value, _traceback) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except BaseException:
            # Finalizers must not mask interpreter shutdown or an active error.
            pass


def _recv_fds(
    channel: socket.socket,
    expected_bytes: int,
    *,
    response_name: str,
) -> _ReceivedPacket:
    """Receive one exact packet and retain ownership of every attached fd."""

    packet = _ReceivedPacket()
    descriptor_bytes = array.array("i").itemsize
    ancillary_capacity = socket.CMSG_SPACE(descriptor_bytes)
    recv_flags = getattr(socket, "MSG_CMSG_CLOEXEC", 0)
    try:
        data, ancillary, message_flags, _address = channel.recvmsg(
            expected_bytes, ancillary_capacity, recv_flags
        )
        packet.data = data
        unexpected_ancillary = False
        for level, kind, payload in ancillary:
            if level != socket.SOL_SOCKET or kind != socket.SCM_RIGHTS:
                unexpected_ancillary = True
                continue
            complete_bytes = len(payload) - len(payload) % descriptor_bytes
            if complete_bytes != len(payload):
                unexpected_ancillary = True
            if complete_bytes:
                packet._adopt_bytes(payload[:complete_bytes])

        truncation_flags = getattr(socket, "MSG_TRUNC", 0) | getattr(
            socket, "MSG_CTRUNC", 0
        )
        if message_flags & truncation_flags:
            raise ProtocolError(f"truncated {response_name}")
        if unexpected_ancillary:
            raise ProtocolError(
                f"unexpected or malformed {response_name} ancillary message"
            )
        if len(data) != expected_bytes:
            raise ProtocolError(
                f"{response_name} has {len(data)} bytes; "
                f"expected {expected_bytes}"
            )
        for descriptor in packet._fds:
            os.set_inheritable(descriptor, False)
        return packet
    except BaseException:
        packet.close()
        raise
