"""Small session-anchored microbatch runner for Python or Polars factors."""

from __future__ import annotations

import math
import time
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Callable, Optional

from .batch import TickBatch
from .models import SessionIdentity, StaleSessionError


MAX_FACTOR_BATCH_ROWS = 1_048_576


@dataclass(frozen=True, slots=True)
class FactorResult:
    input_batch: TickBatch
    value: Any


class TickFactorRunner:
    """Collect contiguous tick microbatches and invoke one factor callback.

    The callback receives :class:`TickBatch` by default. With
    ``as_polars=True`` it receives a Polars DataFrame created from the
    already-consistent client-owned batch copy.
    """

    def __init__(
        self,
        cursor,
        transform: Callable[[Any], Any],
        *,
        max_rows: int = 4096,
        max_latency: Any = 0.010,
        poll_interval: float = 0.0005,
        as_polars: bool = False,
        _clock: Callable[[], float] = time.monotonic,
        _sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if not callable(transform):
            raise TypeError("transform must be callable")
        if not isinstance(max_rows, int) or isinstance(max_rows, bool):
            raise TypeError("max_rows must be an integer")
        if max_rows <= 0 or max_rows > MAX_FACTOR_BATCH_ROWS:
            raise ValueError(
                "max_rows must be between 1 and "
                f"{MAX_FACTOR_BATCH_ROWS}"
            )
        if isinstance(max_latency, timedelta):
            max_latency = max_latency.total_seconds()
        if (
            not isinstance(max_latency, (int, float))
            or isinstance(max_latency, bool)
        ):
            raise TypeError("max_latency must be seconds or timedelta")
        if not math.isfinite(max_latency) or max_latency < 0:
            raise ValueError("max_latency must be finite and nonnegative")
        if (
            not isinstance(poll_interval, (int, float))
            or isinstance(poll_interval, bool)
        ):
            raise TypeError("poll_interval must be numeric seconds")
        if not math.isfinite(poll_interval) or poll_interval < 0:
            raise ValueError("poll_interval must be finite and nonnegative")
        self._cursor = cursor
        self._transform = transform
        self._max_rows = max_rows
        self._max_latency = float(max_latency)
        self._poll_interval = poll_interval
        self._as_polars = bool(as_polars)
        self._clock = _clock
        self._sleep = _sleep
        self._identity: SessionIdentity = cursor.session_identity

    @property
    def session_identity(self) -> SessionIdentity:
        return self._identity

    def run_once(self) -> Optional[FactorResult]:
        deadline = self._clock() + self._max_latency
        collected = []
        first_sequence = self._cursor.next_sequence
        next_sequence = first_sequence
        while len(collected) < self._max_rows:
            batch = self._cursor.read(self._max_rows - len(collected))
            if batch.session_identity != self._identity:
                raise StaleSessionError(
                    "factor runner input session changed"
                )
            if batch.ticks:
                if batch.first_sequence != next_sequence:
                    raise StaleSessionError(
                        "factor runner observed a tick sequence gap"
                    )
                collected.extend(batch.ticks)
                next_sequence = batch.next_sequence
                if len(collected) >= self._max_rows:
                    break
                # Drain already-published rows without sleeping, but keep
                # latency as a real upper bound even under a continuously
                # non-empty stream.
                if self._clock() >= deadline:
                    break
                continue
            now = self._clock()
            if now >= deadline:
                break
            sleep_for = min(self._poll_interval, deadline - now)
            if sleep_for > 0:
                self._sleep(sleep_for)
        if not collected:
            return None
        combined = TickBatch(
            self._identity,
            first_sequence,
            next_sequence,
            tuple(collected),
        )
        factor_input = (
            combined.to_polars() if self._as_polars else combined
        )
        return FactorResult(combined, self._transform(factor_input))
