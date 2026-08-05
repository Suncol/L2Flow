"""Atomic, instrument-local consumers for Event and KLine CDC streams."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional

from .models import (
    CdcProtocolError,
    DerivedEvent,
    EventMutation,
    EventMutationKind,
    EventOrderKey,
    EventRangeReplaceScope,
    EventUid,
    KLineBar,
    KLineBarKey,
    KLineMutation,
    KLineMutationKind,
)


def _validate_event_rows(rows: Iterable[DerivedEvent]) -> tuple[DerivedEvent, ...]:
    result = tuple(rows)
    if any(
        result[index - 1].order_key >= result[index].order_key
        for index in range(1, len(result))
    ):
        raise CdcProtocolError("Event rows are not strictly ordered")
    if len({row.uid for row in result}) != len(result):
        raise CdcProtocolError("Event rows contain duplicate EventUid values")
    if len({row.uid.instrument_id for row in result}) > 1:
        raise CdcProtocolError("Event rows contain multiple instruments")
    return result


@dataclass(slots=True)
class _PendingRange:
    transaction_id: int
    range_scope: EventRangeReplaceScope
    range_channel: int
    range_begin_business_sequence: int
    replace_entire_instrument: bool
    range_begin: Optional[EventOrderKey]
    range_end_exclusive: Optional[EventOrderKey]
    rows: list[DerivedEvent]


@dataclass(slots=True)
class _PendingKLineRange:
    transaction_id: int
    bars: list[KLineBar]


class EventCdcApplier:
    """Maintain one immutable stable Event view.

    RANGE_REPLACE chunks remain private until COMMIT. ``rows`` therefore
    always returns either the old complete root or the new complete root.
    """

    def __init__(
        self,
        rows: Iterable[DerivedEvent] = (),
        *,
        next_change_sequence: int = 1,
    ) -> None:
        if next_change_sequence <= 0:
            raise ValueError("next_change_sequence must be positive")
        self._rows = _validate_event_rows(rows)
        self._instrument_id = (
            None if not self._rows else self._rows[0].uid.instrument_id
        )
        self._next_change_sequence = next_change_sequence
        self._pending: Optional[_PendingRange] = None

    @property
    def rows(self) -> tuple[DerivedEvent, ...]:
        return self._rows

    @property
    def next_change_sequence(self) -> int:
        return self._next_change_sequence

    @property
    def transaction_pending(self) -> bool:
        return self._pending is not None

    def apply(self, mutations: Iterable[EventMutation]) -> None:
        candidate = self.fork()
        candidate._apply_in_place(mutations)
        self._rows = candidate._rows
        self._instrument_id = candidate._instrument_id
        self._next_change_sequence = candidate._next_change_sequence
        self._pending = candidate._pending

    def fork(self) -> "EventCdcApplier":
        """Return a cheap private candidate with copied transaction scratch."""

        candidate = object.__new__(EventCdcApplier)
        candidate._rows = self._rows
        candidate._instrument_id = self._instrument_id
        candidate._next_change_sequence = self._next_change_sequence
        pending = self._pending
        candidate._pending = (
            None
            if pending is None
            else _PendingRange(
                pending.transaction_id,
                pending.range_scope,
                pending.range_channel,
                pending.range_begin_business_sequence,
                pending.replace_entire_instrument,
                pending.range_begin,
                pending.range_end_exclusive,
                list(pending.rows),
            )
        )
        return candidate

    def _apply_in_place(self, mutations: Iterable[EventMutation]) -> None:
        for mutation in mutations:
            if mutation.change_sequence != self._next_change_sequence:
                raise CdcProtocolError(
                    "non-contiguous Event change sequence: expected "
                    f"{self._next_change_sequence}, got "
                    f"{mutation.change_sequence}"
                )
            self._apply_one(mutation)
            self._next_change_sequence += 1

    def _apply_one(self, mutation: EventMutation) -> None:
        kind = mutation.kind
        if kind is EventMutationKind.RANGE_REPLACE_BEGIN:
            if self._pending is not None or mutation.transaction_id == 0:
                raise CdcProtocolError("invalid RANGE_REPLACE_BEGIN")
            suffix = (
                mutation.range_scope
                is EventRangeReplaceScope.CHANNEL_SUFFIX
            )
            if suffix:
                if (
                    mutation.replace_entire_instrument
                    or mutation.range_channel <= 0
                    or mutation.range_begin_business_sequence <= 0
                    or mutation.range_begin is not None
                    or mutation.range_end_exclusive is not None
                ):
                    raise CdcProtocolError(
                        "invalid channel-suffix replacement"
                    )
            elif not mutation.replace_entire_instrument:
                if (
                    mutation.range_begin is None
                    or mutation.range_end_exclusive is None
                    or mutation.range_begin >= mutation.range_end_exclusive
                ):
                    raise CdcProtocolError("invalid replacement key range")
            self._pending = _PendingRange(
                mutation.transaction_id,
                mutation.range_scope,
                mutation.range_channel,
                mutation.range_begin_business_sequence,
                mutation.replace_entire_instrument,
                mutation.range_begin,
                mutation.range_end_exclusive,
                [],
            )
            return

        if kind is EventMutationKind.RANGE_REPLACE_CHUNK:
            pending = self._require_transaction(mutation)
            for row in mutation.replacement_rows:
                self._accept_instrument(row.uid.instrument_id)
            pending.rows.extend(mutation.replacement_rows)
            return

        if kind is EventMutationKind.RANGE_REPLACE_COMMIT:
            pending = self._require_transaction(mutation)
            replacement = _validate_event_rows(pending.rows)
            if (
                pending.range_scope
                is EventRangeReplaceScope.CHANNEL_SUFFIX
            ):
                if any(
                    row.order_key.channel != pending.range_channel
                    or row.order_key.business_sequence
                    < pending.range_begin_business_sequence
                    for row in replacement
                ):
                    raise CdcProtocolError(
                        "replacement Event lies outside its channel suffix"
                    )
                retained = tuple(
                    row
                    for row in self._rows
                    if row.order_key.channel != pending.range_channel
                    or row.order_key.business_sequence
                    < pending.range_begin_business_sequence
                )
                candidate = _validate_event_rows(
                    sorted(
                        (*retained, *replacement),
                        key=lambda row: row.order_key,
                    )
                )
            elif pending.replace_entire_instrument:
                candidate = replacement
            else:
                assert pending.range_begin is not None
                assert pending.range_end_exclusive is not None
                retained = tuple(
                    row
                    for row in self._rows
                    if not (
                        pending.range_begin
                        <= row.order_key
                        < pending.range_end_exclusive
                    )
                )
                if any(
                    not (
                        pending.range_begin
                        <= row.order_key
                        < pending.range_end_exclusive
                    )
                    for row in replacement
                ):
                    raise CdcProtocolError(
                        "replacement Event lies outside its key range"
                    )
                candidate = tuple(
                    sorted(
                        (*retained, *replacement),
                        key=lambda row: row.order_key,
                    )
                )
                candidate = _validate_event_rows(candidate)
            self._rows = candidate
            self._pending = None
            return

        if self._pending is not None:
            raise CdcProtocolError(
                "point mutation interleaves a range transaction"
            )
        if kind is EventMutationKind.INSERT:
            if mutation.row is None or mutation.uid is None:
                raise CdcProtocolError("INSERT requires EventUid and row")
            if mutation.uid != mutation.row.uid:
                raise CdcProtocolError("INSERT uid and row disagree")
            self._accept_instrument(mutation.uid.instrument_id)
            self._point_upsert(mutation.row, insert_only=True)
        elif kind is EventMutationKind.UPDATE:
            if mutation.row is None or mutation.uid is None:
                raise CdcProtocolError("UPDATE requires EventUid and row")
            if mutation.uid != mutation.row.uid:
                raise CdcProtocolError("UPDATE uid and row disagree")
            self._accept_instrument(mutation.uid.instrument_id)
            self._point_upsert(mutation.row, insert_only=False)
        elif kind is EventMutationKind.DELETE:
            if mutation.uid is None:
                raise CdcProtocolError("DELETE has no EventUid")
            self._accept_instrument(mutation.uid.instrument_id)
            self._delete(mutation.uid)
        else:
            raise CdcProtocolError(f"unsupported Event mutation {kind!r}")

    def _require_transaction(self, mutation: EventMutation) -> _PendingRange:
        if (
            self._pending is None
            or mutation.transaction_id != self._pending.transaction_id
        ):
            raise CdcProtocolError("range transaction id mismatch")
        return self._pending

    def _point_upsert(self, row: DerivedEvent, *, insert_only: bool) -> None:
        by_uid = {value.uid: value for value in self._rows}
        exists = row.uid in by_uid
        if insert_only and exists:
            raise CdcProtocolError("INSERT repeats an EventUid")
        if not insert_only and not exists:
            raise CdcProtocolError("UPDATE references an unknown EventUid")
        by_uid[row.uid] = row
        self._rows = _validate_event_rows(
            sorted(by_uid.values(), key=lambda value: value.order_key)
        )

    def _accept_instrument(self, instrument_id: int) -> None:
        if self._instrument_id is None:
            self._instrument_id = instrument_id
        elif self._instrument_id != instrument_id:
            raise CdcProtocolError("Event CDC contains another instrument")

    def _delete(self, uid: EventUid) -> None:
        retained = tuple(row for row in self._rows if row.uid != uid)
        if len(retained) == len(self._rows):
            raise CdcProtocolError("DELETE references an unknown EventUid")
        self._rows = retained


class KLineCdcApplier:
    """Maintain current bars by stable key with monotonic revisions."""

    def __init__(
        self,
        bars: Iterable[KLineBar] = (),
        *,
        next_change_sequence: int = 1,
    ) -> None:
        if next_change_sequence <= 0:
            raise ValueError("next_change_sequence must be positive")
        values = tuple(bars)
        if len({bar.key for bar in values}) != len(values):
            raise CdcProtocolError("initial KLine bars contain duplicate keys")
        if len({bar.key.instrument_id for bar in values}) > 1:
            raise CdcProtocolError(
                "initial KLine bars contain multiple instruments"
            )
        self._bars = {bar.key: bar for bar in values}
        self._instrument_id = (
            None if not values else values[0].key.instrument_id
        )
        self._next_change_sequence = next_change_sequence
        self._pending: Optional[_PendingKLineRange] = None

    @property
    def bars(self) -> tuple[KLineBar, ...]:
        return tuple(self._bars[key] for key in sorted(self._bars))

    @property
    def next_change_sequence(self) -> int:
        return self._next_change_sequence

    @property
    def transaction_pending(self) -> bool:
        return self._pending is not None

    def apply(self, mutations: Iterable[KLineMutation]) -> None:
        candidate = self.fork()
        candidate._apply_in_place(mutations)
        self._bars = candidate._bars
        self._instrument_id = candidate._instrument_id
        self._next_change_sequence = candidate._next_change_sequence
        self._pending = candidate._pending

    def fork(self) -> "KLineCdcApplier":
        """Return a cheap private candidate with copied transaction scratch."""

        candidate = object.__new__(KLineCdcApplier)
        candidate._bars = dict(self._bars)
        candidate._instrument_id = self._instrument_id
        candidate._next_change_sequence = self._next_change_sequence
        pending = self._pending
        candidate._pending = (
            None
            if pending is None
            else _PendingKLineRange(
                pending.transaction_id, list(pending.bars)
            )
        )
        return candidate

    def _apply_in_place(self, mutations: Iterable[KLineMutation]) -> None:
        for mutation in mutations:
            if mutation.change_sequence != self._next_change_sequence:
                raise CdcProtocolError(
                    "non-contiguous KLine change sequence: expected "
                    f"{self._next_change_sequence}, got "
                    f"{mutation.change_sequence}"
                )
            if mutation.kind is KLineMutationKind.RANGE_REPLACE_BEGIN:
                if (
                    self._pending is not None
                    or mutation.transaction_id == 0
                    or not mutation.replace_entire_instrument
                ):
                    raise CdcProtocolError(
                        "invalid KLine RANGE_REPLACE_BEGIN"
                    )
                self._pending = _PendingKLineRange(
                    mutation.transaction_id, []
                )
            elif mutation.kind is KLineMutationKind.RANGE_REPLACE_CHUNK:
                pending = self._require_transaction(mutation)
                for bar in mutation.replacement_bars:
                    self._accept_instrument(bar.key.instrument_id)
                pending.bars.extend(mutation.replacement_bars)
            elif mutation.kind is KLineMutationKind.RANGE_REPLACE_COMMIT:
                pending = self._require_transaction(mutation)
                self._commit_range(pending)
                self._pending = None
            elif self._pending is not None:
                raise CdcProtocolError(
                    "point KLine mutation interleaves a range transaction"
                )
            elif mutation.kind is KLineMutationKind.UPSERT:
                self._upsert(mutation)
            elif mutation.kind is KLineMutationKind.DELETE:
                if mutation.key is None:
                    raise CdcProtocolError("KLine DELETE has no key")
                self._accept_instrument(mutation.key.instrument_id)
                if mutation.key not in self._bars:
                    raise CdcProtocolError(
                        "KLine DELETE references an unknown bar"
                    )
                del self._bars[mutation.key]
            else:
                raise CdcProtocolError(
                    f"unsupported KLine mutation {mutation.kind!r}"
                )
            self._next_change_sequence += 1

    def _require_transaction(
        self, mutation: KLineMutation
    ) -> _PendingKLineRange:
        if (
            self._pending is None
            or mutation.transaction_id != self._pending.transaction_id
        ):
            raise CdcProtocolError("KLine range transaction id mismatch")
        return self._pending

    def _commit_range(self, pending: _PendingKLineRange) -> None:
        candidate: dict[KLineBarKey, KLineBar] = {}
        for bar in pending.bars:
            if bar.key in candidate:
                raise CdcProtocolError(
                    "KLine replacement contains duplicate keys"
                )
            old = self._bars.get(bar.key)
            if old is not None and (
                bar.revision < old.revision
                or (bar.revision == old.revision and bar != old)
            ):
                raise CdcProtocolError(
                    "KLine replacement violates revision monotonicity"
                )
            candidate[bar.key] = bar
        self._bars = candidate

    def _upsert(self, mutation: KLineMutation) -> None:
        bar = mutation.bar
        if mutation.key is None or bar is None or bar.key != mutation.key:
            raise CdcProtocolError("KLine UPSERT key and row disagree")
        self._accept_instrument(mutation.key.instrument_id)
        old = self._bars.get(mutation.key)
        if old is not None and bar.revision <= old.revision:
            raise CdcProtocolError("KLine revision did not increase")
        self._bars[mutation.key] = bar

    def _accept_instrument(self, instrument_id: int) -> None:
        if self._instrument_id is None:
            self._instrument_id = instrument_id
        elif self._instrument_id != instrument_id:
            raise CdcProtocolError("KLine CDC contains another instrument")
