"""Optional stateful reader for the FAST global tick-ring tail.

The shared-memory ring is deliberately bounded.  This reader therefore does
not pretend that an overrun can be repaired from the ring itself: callers that
need a complete instrument history must reconcile through the immutable
instrument full/delta endpoint before resuming at the recovered generation's
``tick_stream_sequence_exclusive`` frontier.
"""

from __future__ import annotations

import struct
import threading
from dataclasses import dataclass, field
from typing import Mapping

from ._history_columns import LazyWireColumns, tick_columns
from .models import (
    ClientClosedError,
    SessionIdentity,
    StaleSessionError,
    WireFormatError,
)
from .native import MAX_BATCH_RECORDS, NativeTickRead
from .wire import TICK_BYTES


_UINT32_MAX = (1 << 32) - 1
_UINT64_MAX = (1 << 64) - 1
_TICK_IDENTITY = struct.Struct("<II")
_TICK_SEQUENCE = struct.Struct("<Q")
_TICK_TRADE_DATE = struct.Struct("<I")


def _positive_uint64(value: object, field_name: str) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or value <= 0
        or value > _UINT64_MAX
    ):
        raise ValueError(f"{field_name} must be a positive uint64")
    return value


def _positive_batch_records(value: object) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or value <= 0
        or value > MAX_BATCH_RECORDS
    ):
        raise ValueError(
            "batch_records must be in the inclusive range "
            f"[1, {MAX_BATCH_RECORDS}]"
        )
    return value


@dataclass(frozen=True, slots=True)
class FastTickBatch:
    """One owned, dense global FAST tick-stream interval.

    ``wire_records`` owns its bytes and remains valid after the next ring read.
    The interval is global: use :meth:`instrument_wire_records` or selected
    columns to obtain one instrument.  Empty reads have
    ``first_sequence == next_sequence``.
    """

    session_identity: SessionIdentity
    trade_date: int
    first_sequence: int
    next_sequence: int
    wire_records: bytes = field(repr=False)
    _columns: LazyWireColumns = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        if not isinstance(self.session_identity, SessionIdentity):
            raise TypeError("session_identity must be SessionIdentity")
        if (
            not isinstance(self.trade_date, int)
            or isinstance(self.trade_date, bool)
            or self.trade_date <= 0
            or self.trade_date > _UINT32_MAX
        ):
            raise ValueError("trade_date must be a nonzero uint32")
        _positive_uint64(self.first_sequence, "first_sequence")
        _positive_uint64(self.next_sequence, "next_sequence")
        if self.next_sequence < self.first_sequence:
            raise ValueError("next_sequence precedes first_sequence")
        if not isinstance(self.wire_records, bytes):
            raise TypeError("wire_records must be exact owned bytes")
        if len(self.wire_records) % TICK_BYTES:
            raise ValueError("wire_records are not tick-record aligned")
        record_count = len(self.wire_records) // TICK_BYTES
        if self.first_sequence + record_count != self.next_sequence:
            raise ValueError(
                "wire record count disagrees with the sequence interval"
            )

        # The native C reader has already performed full canonical validation
        # and a stable-copy sequence check.  Recheck the adapter boundary so a
        # mismatched/fake native implementation cannot manufacture a dense
        # Python batch or cross a session's trade date.
        for index in range(record_count):
            offset = index * TICK_BYTES
            instrument_id, ordinal = _TICK_IDENTITY.unpack_from(
                self.wire_records, offset + 8
            )
            sequence = _TICK_SEQUENCE.unpack_from(
                self.wire_records, offset + 32
            )[0]
            trade_date = _TICK_TRADE_DATE.unpack_from(
                self.wire_records, offset + 100
            )[0]
            if (
                instrument_id == 0
                or ordinal != instrument_id - 1
                or sequence != self.first_sequence + index
                or trade_date != self.trade_date
            ):
                raise WireFormatError(
                    "FAST tick batch identity/sequence is noncanonical"
                )
        object.__setattr__(self, "_columns", tick_columns(self.wire_records))

    def __len__(self) -> int:
        return len(self.wire_records) // TICK_BYTES

    @property
    def columns(self) -> LazyWireColumns:
        """Lazily materialized columns over this owned batch."""

        return self._columns

    def read_columns(self, *names: str) -> Mapping[str, tuple[object, ...]]:
        return self._columns.read_columns(*names)

    def instrument_wire_records(self, instrument_id: int) -> bytes:
        """Return owned dense wire rows for one instrument, in stream order."""

        if (
            not isinstance(instrument_id, int)
            or isinstance(instrument_id, bool)
            or instrument_id <= 0
            or instrument_id > _UINT32_MAX
        ):
            raise ValueError("instrument_id must be a positive uint32")
        pieces = []
        for offset in range(0, len(self.wire_records), TICK_BYTES):
            current = _TICK_IDENTITY.unpack_from(
                self.wire_records, offset + 8
            )[0]
            if current == instrument_id:
                pieces.append(
                    self.wire_records[offset : offset + TICK_BYTES]
                )
        return b"".join(pieces)


class FastTickStreamReader:
    """Stateful cursor over the bounded FAST tick ring.

    Construction and reads add no work to the producer callback.  The cursor
    advances only after a successful native read; overrun and all other
    failures leave ``next_sequence`` unchanged so a history reconciler can
    repair the missing interval deterministically.
    """

    __slots__ = (
        "_batch_records",
        "_client",
        "_closed",
        "_identity",
        "_lock",
        "_next_sequence",
        "_trade_date",
    )

    def __init__(
        self,
        client,
        expected_sequence: int,
        *,
        batch_records: int = 4096,
    ) -> None:
        expected_sequence = _positive_uint64(
            expected_sequence, "expected_sequence"
        )
        batch_records = _positive_batch_records(batch_records)
        session = client.session_info()
        self._client = client
        self._identity = session.identity
        self._trade_date = session.trade_date
        self._next_sequence = expected_sequence
        self._batch_records = batch_records
        self._lock = threading.Lock()
        self._closed = False

    @classmethod
    def after_latest(
        cls, client, *, batch_records: int = 4096
    ) -> "FastTickStreamReader":
        """Open a tail-only cursor immediately after the current FAST cut."""

        session = client.session_info()
        contiguous = session.tick_contiguous_published_sequence
        if contiguous == _UINT64_MAX:
            raise OverflowError("FAST tick sequence has no successor")
        reader = cls(
            client,
            contiguous + 1,
            batch_records=batch_records,
        )
        # The constructor samples the session again.  Do not combine a
        # frontier from one mapping with the identity of a replacement that
        # appeared between those two samples.
        if (
            reader.session_identity != session.identity
            or reader.trade_date != session.trade_date
        ):
            reader.close()
            raise StaleSessionError(
                "FAST session changed while opening the live tail"
            )
        return reader

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def next_sequence(self) -> int:
        with self._lock:
            return self._next_sequence

    @property
    def session_identity(self) -> SessionIdentity:
        return self._identity

    @property
    def trade_date(self) -> int:
        return self._trade_date

    def read(self, maximum_records: int | None = None) -> FastTickBatch:
        if maximum_records is None:
            maximum_records = self._batch_records
        maximum_records = _positive_batch_records(maximum_records)
        with self._lock:
            if self._closed:
                raise ClientClosedError("FAST tick stream reader is closed")
            expected = self._next_sequence
            result = self._client._read_fast_ticks(  # noqa: SLF001
                expected, maximum_records
            )
            if not isinstance(result, NativeTickRead):
                raise WireFormatError(
                    "native FAST tick reader returned the wrong result type"
                )
            if (
                result.observed_sequence != 0
                or result.next_sequence < expected
                or result.next_sequence - expected != len(result.payloads)
            ):
                raise WireFormatError(
                    "native FAST tick result has noncanonical frontiers"
                )
            wire_records = b"".join(result.payloads)
            batch = FastTickBatch(
                session_identity=self._identity,
                trade_date=self._trade_date,
                first_sequence=expected,
                next_sequence=result.next_sequence,
                wire_records=wire_records,
            )
            # Publish cursor progress only after all Python-side validation and
            # owned-copy construction succeeds.
            self._next_sequence = result.next_sequence
            return batch

    def close(self) -> None:
        with self._lock:
            self._closed = True

    def __enter__(self) -> "FastTickStreamReader":
        if self.closed:
            raise ClientClosedError("FAST tick stream reader is closed")
        return self

    def __exit__(self, _type, _value, _traceback) -> None:
        self.close()


__all__ = ["FastTickBatch", "FastTickStreamReader"]
