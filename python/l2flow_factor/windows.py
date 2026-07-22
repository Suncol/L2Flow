from __future__ import annotations

from collections import deque
import hashlib
import math
import struct
from typing import Deque

from .errors import CheckpointError, ValidationError


_EVENT_MAGIC = b"L2FEW1\x00\x00"
_TIME_MAGIC = b"L2FTW1\x00\x00"


def _finite(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValidationError("window values must be real numbers")
    result = float(value)
    if not math.isfinite(result):
        raise ValidationError("window values must be finite; nulls use validity")
    return result


def _seal(payload: bytes) -> bytes:
    return payload + hashlib.sha256(payload).digest()


def _open(blob: bytes, magic: bytes) -> bytes:
    if not isinstance(blob, bytes) or len(blob) < len(magic) + 32:
        raise CheckpointError("window checkpoint is truncated")
    payload, digest = blob[:-32], blob[-32:]
    if not payload.startswith(magic):
        raise CheckpointError("window checkpoint magic/version mismatch")
    if hashlib.sha256(payload).digest() != digest:
        raise CheckpointError("window checkpoint SHA-256 mismatch")
    return payload


class IncrementalEventWindow:
    """Fixed-count float64 window with O(1) sum/count updates."""

    def __init__(self, capacity: int) -> None:
        if type(capacity) is not int or capacity <= 0 or capacity > (1 << 32) - 1:
            raise ValidationError("capacity must be a positive uint32")
        self._capacity = capacity
        self._values: Deque[float] = deque()
        self._sum = 0.0
        self._sum_squares = 0.0

    @property
    def capacity(self) -> int:
        return self._capacity

    @property
    def count(self) -> int:
        return len(self._values)

    @property
    def total(self) -> float:
        return self._sum

    @property
    def mean(self) -> float | None:
        return None if not self._values else self._sum / len(self._values)

    def append(self, value: float) -> None:
        number = _finite(value)
        if len(self._values) == self._capacity:
            removed = self._values.popleft()
            self._sum -= removed
            self._sum_squares -= removed * removed
        self._values.append(number)
        self._sum += number
        self._sum_squares += number * number

    def values(self) -> tuple[float, ...]:
        return tuple(self._values)

    def checkpoint_bytes(self) -> bytes:
        header = _EVENT_MAGIC + struct.pack(
            "<IIdd",
            self._capacity,
            len(self._values),
            self._sum,
            self._sum_squares,
        )
        body = b"".join(struct.pack("<d", value) for value in self._values)
        return _seal(header + body)

    @classmethod
    def restore_bytes(cls, blob: bytes) -> "IncrementalEventWindow":
        payload = _open(blob, _EVENT_MAGIC)
        if len(payload) < 32:
            raise CheckpointError("event window header is truncated")
        capacity, count, saved_sum, saved_sum_squares = struct.unpack_from(
            "<IIdd", payload, 8
        )
        expected = 32 + count * 8
        if capacity == 0 or count > capacity or len(payload) != expected:
            raise CheckpointError("event window length/count is invalid")
        if not math.isfinite(saved_sum) or not math.isfinite(saved_sum_squares):
            raise CheckpointError("event window accumulators are non-finite")
        result = cls(capacity)
        for index in range(count):
            (value,) = struct.unpack_from("<d", payload, 32 + index * 8)
            try:
                result.append(value)
            except ValidationError as error:
                raise CheckpointError("event window contains a non-finite value") from error
        if not math.isclose(result._sum, saved_sum, rel_tol=1e-12, abs_tol=1e-12) or not math.isclose(
            result._sum_squares,
            saved_sum_squares,
            rel_tol=1e-12,
            abs_tol=1e-12,
        ):
            raise CheckpointError("event window accumulators contradict stored entries")
        result._sum = saved_sum
        result._sum_squares = saved_sum_squares
        return result


class IncrementalTimeWindow:
    """Inclusive ``[now-duration, now]`` float64 event-time window."""

    def __init__(self, duration_ns: int) -> None:
        if type(duration_ns) is not int or duration_ns <= 0 or duration_ns > (1 << 63) - 1:
            raise ValidationError("duration_ns must be a positive signed-64 value")
        self._duration_ns = duration_ns
        self._entries: Deque[tuple[int, float]] = deque()
        self._sum = 0.0
        self._last_timestamp_ns: int | None = None

    @property
    def duration_ns(self) -> int:
        return self._duration_ns

    @property
    def count(self) -> int:
        return len(self._entries)

    @property
    def total(self) -> float:
        return self._sum

    @property
    def mean(self) -> float | None:
        return None if not self._entries else self._sum / len(self._entries)

    def append(self, timestamp_ns: int, value: float) -> None:
        if type(timestamp_ns) is not int or timestamp_ns < 0 or timestamp_ns > (1 << 63) - 1:
            raise ValidationError("timestamp_ns must be a nonnegative signed-64 value")
        if self._last_timestamp_ns is not None and timestamp_ns < self._last_timestamp_ns:
            raise ValidationError("time-window timestamps must be nondecreasing")
        number = _finite(value)
        self._last_timestamp_ns = timestamp_ns
        self._entries.append((timestamp_ns, number))
        self._sum += number
        cutoff = max(0, timestamp_ns - self._duration_ns)
        while self._entries and self._entries[0][0] < cutoff:
            _, removed = self._entries.popleft()
            self._sum -= removed

    def entries(self) -> tuple[tuple[int, float], ...]:
        return tuple(self._entries)

    def checkpoint_bytes(self) -> bytes:
        last_timestamp = (
            (1 << 64) - 1 if self._last_timestamp_ns is None else self._last_timestamp_ns
        )
        header = _TIME_MAGIC + struct.pack(
            "<QIdQ",
            self._duration_ns,
            len(self._entries),
            self._sum,
            last_timestamp,
        )
        body = b"".join(
            struct.pack("<Qd", timestamp_ns, value)
            for timestamp_ns, value in self._entries
        )
        return _seal(header + body)

    @classmethod
    def restore_bytes(cls, blob: bytes) -> "IncrementalTimeWindow":
        payload = _open(blob, _TIME_MAGIC)
        if len(payload) < 36:
            raise CheckpointError("time window header is truncated")
        duration_ns, count, saved_sum, saved_last_timestamp = struct.unpack_from(
            "<QIdQ", payload, 8
        )
        expected = 36 + count * 16
        if duration_ns == 0 or len(payload) != expected:
            raise CheckpointError("time window length/count is invalid")
        if not math.isfinite(saved_sum):
            raise CheckpointError("time window accumulator is non-finite")
        if (count == 0) != (saved_last_timestamp == (1 << 64) - 1):
            raise CheckpointError("time window last timestamp marker is inconsistent")
        try:
            result = cls(duration_ns)
        except ValidationError as error:
            raise CheckpointError("time window duration is outside the V1 range") from error
        for index in range(count):
            timestamp_ns, value = struct.unpack_from("<Qd", payload, 36 + index * 16)
            try:
                result.append(timestamp_ns, value)
            except ValidationError as error:
                raise CheckpointError("time window entries are invalid") from error
        if count and result._last_timestamp_ns != saved_last_timestamp:
            raise CheckpointError("time window last timestamp contradicts entries")
        if not math.isclose(result._sum, saved_sum, rel_tol=1e-12, abs_tol=1e-12):
            raise CheckpointError("time window accumulator contradicts stored entries")
        result._sum = saved_sum
        result._last_timestamp_ns = (
            None if saved_last_timestamp == (1 << 64) - 1 else saved_last_timestamp
        )
        return result


EventWindow = IncrementalEventWindow
TimeWindow = IncrementalTimeWindow
