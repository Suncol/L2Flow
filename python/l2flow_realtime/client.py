"""Transport-neutral Python client for the instrument-local V3 service."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, Sequence

from .models import (
    CursorMismatchError,
    Dataset,
    DerivedEvent,
    EventChangeCursor,
    EventMutation,
    FastTickCursor,
    FastTickRow,
    InstrumentStableStatus,
    KLineBar,
    KLineChangeCursor,
    KLineMutation,
)


@dataclass(frozen=True, slots=True)
class FastTickDelta:
    next_cursor: FastTickCursor
    captured_tail: int
    coverage_from_open: bool
    coverage_complete: bool
    rows: tuple[FastTickRow, ...]


@dataclass(frozen=True, slots=True)
class EventStableView:
    rows: tuple[DerivedEvent, ...]
    next_changes: EventChangeCursor
    status: InstrumentStableStatus


@dataclass(frozen=True, slots=True)
class KLineStableView:
    bars: tuple[KLineBar, ...]
    next_changes: KLineChangeCursor
    status: InstrumentStableStatus


class InstrumentDataTransportV3(Protocol):
    """Binding implemented by the process-local or cross-process adapter."""

    @property
    def session_id(self) -> bytes: ...

    def read_fast_delta(
        self, cursor: FastTickCursor, maximum_rows: int
    ) -> FastTickDelta: ...

    def acquire_event_stable(self, instrument_id: int) -> EventStableView: ...

    def read_event_changes(
        self, cursor: EventChangeCursor, maximum_changes: int
    ) -> tuple[tuple[EventMutation, ...], EventChangeCursor]: ...

    def acquire_kline_stable(self, instrument_id: int) -> KLineStableView: ...

    def read_kline_changes(
        self, cursor: KLineChangeCursor, maximum_changes: int
    ) -> tuple[tuple[KLineMutation, ...], KLineChangeCursor]: ...


class L2FlowClient:
    """Thin validated facade with no global market or cross-plane cursor."""

    def __init__(self, transport: InstrumentDataTransportV3) -> None:
        session_id = transport.session_id
        if (
            not isinstance(session_id, bytes)
            or len(session_id) != 16
            or not any(session_id)
        ):
            raise ValueError("transport has an invalid session_id")
        self._transport = transport
        self._session_id = session_id

    @property
    def session_id(self) -> bytes:
        return self._session_id

    def open_fast_tick_cursor(
        self, instrument_id: int, next_arrival_row: int = 1
    ) -> FastTickCursor:
        return FastTickCursor(
            self._session_id, instrument_id, next_arrival_row
        )

    def read_fast_tick_delta(
        self, cursor: FastTickCursor, maximum_rows: int
    ) -> FastTickDelta:
        self._validate_cursor(cursor.session_id)
        self._validate_positive_limit(maximum_rows, "maximum_rows")
        result = self._transport.read_fast_delta(cursor, maximum_rows)
        expected_next = cursor.next_arrival_row + len(result.rows)
        if (
            len(result.rows) > maximum_rows
            or result.next_cursor.session_id != self._session_id
            or result.next_cursor.instrument_id != cursor.instrument_id
            or result.next_cursor.next_arrival_row != expected_next
            or result.captured_tail + 1 < expected_next
            or any(
                row.instrument_tick_sequence
                != cursor.next_arrival_row + offset
                for offset, row in enumerate(result.rows)
            )
        ):
            raise CursorMismatchError("transport returned a foreign FAST cursor")
        return result

    def read_fast_tick_batch(
        self,
        cursors: Sequence[FastTickCursor],
        maximum_rows_per_instrument: int,
    ) -> tuple[FastTickDelta, ...]:
        """Sample each instrument independently; this is not an atomic cut."""

        return tuple(
            self.read_fast_tick_delta(cursor, maximum_rows_per_instrument)
            for cursor in cursors
        )

    def acquire_event_stable(self, instrument_id: int) -> EventStableView:
        result = self._transport.acquire_event_stable(instrument_id)
        self._validate_change_view(
            instrument_id,
            result.next_changes.session_id,
            result.next_changes.instrument_id,
            result.status.instrument_id,
            result.status.dataset,
            Dataset.DERIVED_EVENT,
        )
        if any(
            row.uid.instrument_id != instrument_id for row in result.rows
        ):
            raise CursorMismatchError("Event stable view contains another instrument")
        if result.status.stable_tail != len(result.rows):
            raise CursorMismatchError("Event stable cardinality mismatch")
        if any(
            result.rows[index - 1].order_key >= result.rows[index].order_key
            for index in range(1, len(result.rows))
        ) or len({row.uid for row in result.rows}) != len(result.rows):
            raise CursorMismatchError("Event stable view is not strictly ordered")
        return result

    def read_event_changes(
        self, cursor: EventChangeCursor, maximum_changes: int
    ) -> tuple[tuple[EventMutation, ...], EventChangeCursor]:
        self._validate_cursor(cursor.session_id)
        self._validate_positive_limit(maximum_changes, "maximum_changes")
        mutations, next_cursor = self._transport.read_event_changes(
            cursor, maximum_changes
        )
        if len(mutations) > maximum_changes:
            raise CursorMismatchError("transport exceeded the Event batch limit")
        for offset, mutation in enumerate(mutations):
            if (
                mutation.change_sequence
                != cursor.next_change_sequence + offset
            ):
                raise CursorMismatchError("non-contiguous Event CDC batch")
            identities = [mutation.uid]
            if mutation.row is not None:
                identities.append(mutation.row.uid)
            identities.extend(row.uid for row in mutation.replacement_rows)
            if any(
                uid is not None and uid.instrument_id != cursor.instrument_id
                for uid in identities
            ):
                raise CursorMismatchError("Event CDC contains another instrument")
        self._validate_next_change_cursor(cursor, next_cursor, len(mutations))
        return mutations, next_cursor

    def acquire_kline_stable(self, instrument_id: int) -> KLineStableView:
        result = self._transport.acquire_kline_stable(instrument_id)
        self._validate_change_view(
            instrument_id,
            result.next_changes.session_id,
            result.next_changes.instrument_id,
            result.status.instrument_id,
            result.status.dataset,
            Dataset.KLINE,
        )
        if any(
            bar.key.instrument_id != instrument_id for bar in result.bars
        ):
            raise CursorMismatchError("KLine stable view contains another instrument")
        if result.status.stable_tail != len(result.bars):
            raise CursorMismatchError("KLine stable cardinality mismatch")
        if any(
            result.bars[index - 1].key >= result.bars[index].key
            for index in range(1, len(result.bars))
        ):
            raise CursorMismatchError("KLine stable view is not strictly ordered")
        return result

    def read_kline_changes(
        self, cursor: KLineChangeCursor, maximum_changes: int
    ) -> tuple[tuple[KLineMutation, ...], KLineChangeCursor]:
        self._validate_cursor(cursor.session_id)
        self._validate_positive_limit(maximum_changes, "maximum_changes")
        mutations, next_cursor = self._transport.read_kline_changes(
            cursor, maximum_changes
        )
        if len(mutations) > maximum_changes:
            raise CursorMismatchError("transport exceeded the KLine batch limit")
        for offset, mutation in enumerate(mutations):
            if (
                mutation.change_sequence
                != cursor.next_change_sequence + offset
            ):
                raise CursorMismatchError("non-contiguous KLine CDC batch")
            keys = [mutation.key]
            if mutation.bar is not None:
                keys.append(mutation.bar.key)
            keys.extend(bar.key for bar in mutation.replacement_bars)
            if any(
                key is not None and key.instrument_id != cursor.instrument_id
                for key in keys
            ):
                raise CursorMismatchError("KLine CDC contains another instrument")
        self._validate_next_change_cursor(cursor, next_cursor, len(mutations))
        return mutations, next_cursor

    def _validate_cursor(self, session_id: bytes) -> None:
        if session_id != self._session_id:
            raise CursorMismatchError("cursor belongs to another session")

    @staticmethod
    def _validate_positive_limit(value: int, name: str) -> None:
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            raise ValueError(f"{name} must be a positive integer")

    def _validate_change_view(
        self,
        requested_instrument: int,
        session_id: bytes,
        cursor_instrument: int,
        status_instrument: int,
        actual_dataset: Dataset,
        expected_dataset: Dataset,
    ) -> None:
        if (
            session_id != self._session_id
            or cursor_instrument != requested_instrument
            or status_instrument != requested_instrument
            or actual_dataset != expected_dataset
        ):
            raise CursorMismatchError("stable view identity mismatch")

    def _validate_next_change_cursor(
        self, current, next_cursor, mutation_count: int
    ) -> None:
        if (
            next_cursor.session_id != self._session_id
            or next_cursor.instrument_id != current.instrument_id
            or next_cursor.next_change_sequence
            != current.next_change_sequence + mutation_count
        ):
            raise CursorMismatchError("transport returned a foreign CDC cursor")
