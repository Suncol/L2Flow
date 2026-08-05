"""Immutable key-range Polars blocks for FAST, Event, and KLine V3 data."""

from __future__ import annotations

import bisect
import importlib
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Optional, Sequence

from .cdc import EventCdcApplier, KLineCdcApplier
from .models import (
    CdcProtocolError,
    DerivedEvent,
    EventMutation,
    EventMutationKind,
    EventRangeReplaceScope,
    FastTickRow,
    KLineBar,
    KLineMutation,
)


class PolarsUnavailableError(ImportError):
    """Polars is not installed in the selected Python environment."""


class PolarsBlockError(RuntimeError):
    """A block update violates key ordering or CDC atomicity."""


def _require_polars():
    try:
        return importlib.import_module("polars")
    except ModuleNotFoundError as error:
        if error.name != "polars":
            raise
        raise PolarsUnavailableError(
            "install l2flow-realtime[polars] to use Polars blocks"
        ) from error


@dataclass(frozen=True, slots=True)
class _Block:
    rows: tuple[dict[str, Any], ...]
    minimum_key: tuple[Any, ...]
    maximum_key: tuple[Any, ...]
    frame: Any


class ImmutablePolarsBlockTable:
    """A sorted directory of immutable DataFrame blocks.

    Updates construct replacement blocks privately and publish one new block
    tuple. Unaffected block objects and their DataFrames are structurally
    shared. ``frame()`` concatenates in directory order and never sorts the
    full history.
    """

    def __init__(
        self,
        key_columns: Sequence[str],
        *,
        rows_per_block: int = 1024,
        schema: Optional[Mapping[str, Any]] = None,
    ) -> None:
        columns = tuple(key_columns)
        if not columns or len(set(columns)) != len(columns):
            raise ValueError("key_columns must be a nonempty unique sequence")
        if rows_per_block <= 0:
            raise ValueError("rows_per_block must be positive")
        self._key_columns = columns
        self._rows_per_block = rows_per_block
        self._schema = dict(schema) if schema is not None else None
        self._blocks: tuple[_Block, ...] = ()

    @property
    def block_count(self) -> int:
        return len(self._blocks)

    @property
    def row_count(self) -> int:
        return sum(len(block.rows) for block in self._blocks)

    @property
    def block_identities(self) -> tuple[int, ...]:
        """Diagnostic identities used to verify structural sharing."""

        return tuple(id(block) for block in self._blocks)

    def rows(self) -> tuple[dict[str, Any], ...]:
        return tuple(row for block in self._blocks for row in block.rows)

    def frame(self):
        pl = _require_polars()
        if not self._blocks:
            return pl.DataFrame(schema=self._schema)
        return pl.concat(
            [block.frame for block in self._blocks],
            how="vertical_relaxed",
            rechunk=False,
        )

    def replace_all(self, rows: Iterable[Mapping[str, Any]]) -> None:
        candidate = self._normalize(rows)
        self._blocks = self._build_blocks(candidate)

    def append(self, rows: Iterable[Mapping[str, Any]]) -> None:
        incoming = self._normalize(rows)
        if not incoming:
            return
        if self._blocks and self._key(incoming[0]) <= self._blocks[-1].maximum_key:
            raise PolarsBlockError("append keys do not follow the stable tail")
        blocks = list(self._blocks)
        if blocks and len(blocks[-1].rows) < self._rows_per_block:
            available = self._rows_per_block - len(blocks[-1].rows)
            prefix = tuple(incoming[:available])
            combined = (*blocks[-1].rows, *prefix)
            blocks[-1] = self._make_block(combined)
            incoming = incoming[available:]
        blocks.extend(self._build_blocks(incoming))
        self._blocks = tuple(blocks)

    def upsert(self, row: Mapping[str, Any]) -> None:
        value = dict(row)
        key = self._key(value)
        if not self._blocks or key > self._blocks[-1].maximum_key:
            self.append((value,))
            return
        index = bisect.bisect_left(
            [block.maximum_key for block in self._blocks], key
        )
        if index == len(self._blocks):
            self.append((value,))
            return
        block = self._blocks[index]
        values = list(block.rows)
        keys = [self._key(candidate) for candidate in values]
        position = bisect.bisect_left(keys, key)
        if position < len(values) and keys[position] == key:
            values[position] = value
        else:
            values.insert(position, value)
        replacement = self._build_blocks(tuple(values))
        self._blocks = (
            *self._blocks[:index],
            *replacement,
            *self._blocks[index + 1 :],
        )

    def delete(self, key: Sequence[Any]) -> None:
        target = tuple(key)
        if not self._blocks:
            raise PolarsBlockError("delete references an unknown key")
        index = bisect.bisect_left(
            [block.maximum_key for block in self._blocks], target
        )
        if index == len(self._blocks):
            raise PolarsBlockError("delete references an unknown key")
        values = list(self._blocks[index].rows)
        keys = [self._key(candidate) for candidate in values]
        position = bisect.bisect_left(keys, target)
        if position == len(values) or keys[position] != target:
            raise PolarsBlockError("delete references an unknown key")
        del values[position]
        replacement = self._build_blocks(tuple(values))
        self._blocks = (
            *self._blocks[:index],
            *replacement,
            *self._blocks[index + 1 :],
        )

    def replace_range(
        self,
        begin: Sequence[Any],
        end_exclusive: Sequence[Any],
        rows: Iterable[Mapping[str, Any]],
    ) -> None:
        lower = tuple(begin)
        upper = tuple(end_exclusive)
        if lower >= upper:
            raise ValueError("replacement range must be nonempty")
        incoming = self._normalize(rows)
        if any(not (lower <= self._key(row) < upper) for row in incoming):
            raise PolarsBlockError("replacement row lies outside its key range")

        if not self._blocks:
            self._blocks = self._build_blocks(incoming)
            return
        maximums = [block.maximum_key for block in self._blocks]
        first = bisect.bisect_left(maximums, lower)
        first = min(first, len(self._blocks))
        last = first
        while last < len(self._blocks) and self._blocks[last].minimum_key < upper:
            last += 1
        retained: list[dict[str, Any]] = []
        for block in self._blocks[first:last]:
            retained.extend(
                row
                for row in block.rows
                if not (lower <= self._key(row) < upper)
            )
        merged = self._normalize((*retained, *incoming))
        replacement = self._build_blocks(merged)
        self._blocks = (
            *self._blocks[:first],
            *replacement,
            *self._blocks[last:],
        )

    def replace_channel_suffix(
        self,
        channel: int,
        begin_business_sequence: int,
        rows: Iterable[Mapping[str, Any]],
    ) -> None:
        """Replace one channel's suffix without an artificial end key."""

        if channel <= 0 or begin_business_sequence <= 0:
            raise ValueError("channel suffix bounds must be positive")
        if self._key_columns[:2] != (
            "channel",
            "business_sequence",
        ):
            raise ValueError("table key does not begin with Event channel order")
        incoming = self._normalize(rows)

        def in_suffix(row: Mapping[str, Any]) -> bool:
            key = self._key(row)
            return key[0] == channel and key[1] >= begin_business_sequence

        if any(not in_suffix(row) for row in incoming):
            raise PolarsBlockError(
                "replacement row lies outside its channel suffix"
            )
        if not self._blocks:
            self._blocks = self._build_blocks(incoming)
            return

        first = 0
        while first < len(self._blocks):
            maximum = self._blocks[first].maximum_key
            if maximum[0] > channel or (
                maximum[0] == channel
                and maximum[1] >= begin_business_sequence
            ):
                break
            first += 1
        last = first
        while (
            last < len(self._blocks)
            and self._blocks[last].minimum_key[0] <= channel
        ):
            last += 1
        retained: list[dict[str, Any]] = []
        for block in self._blocks[first:last]:
            retained.extend(row for row in block.rows if not in_suffix(row))
        replacement = self._build_blocks(
            self._normalize((*retained, *incoming))
        )
        self._blocks = (
            *self._blocks[:first],
            *replacement,
            *self._blocks[last:],
        )

    def fork(self) -> "ImmutablePolarsBlockTable":
        result = ImmutablePolarsBlockTable(
            self._key_columns,
            rows_per_block=self._rows_per_block,
            schema=self._schema,
        )
        result._blocks = self._blocks
        return result

    def publish_from(self, candidate: "ImmutablePolarsBlockTable") -> None:
        if candidate._key_columns != self._key_columns:
            raise ValueError("cannot publish a table with another key")
        self._blocks = candidate._blocks

    def _key(self, row: Mapping[str, Any]) -> tuple[Any, ...]:
        try:
            return tuple(row[column] for column in self._key_columns)
        except KeyError as error:
            raise PolarsBlockError(f"row is missing key column {error.args[0]!r}") from None

    def _normalize(
        self, rows: Iterable[Mapping[str, Any]]
    ) -> tuple[dict[str, Any], ...]:
        result = tuple(dict(row) for row in rows)
        result = tuple(sorted(result, key=self._key))
        keys = tuple(self._key(row) for row in result)
        if any(keys[index - 1] >= keys[index] for index in range(1, len(keys))):
            raise PolarsBlockError("rows contain duplicate or unordered keys")
        return result

    def _build_blocks(
        self, rows: Sequence[dict[str, Any]]
    ) -> tuple[_Block, ...]:
        return tuple(
            self._make_block(tuple(rows[offset : offset + self._rows_per_block]))
            for offset in range(0, len(rows), self._rows_per_block)
        )

    def _make_block(self, rows: tuple[dict[str, Any], ...]) -> _Block:
        if not rows:
            raise ValueError("an immutable block cannot be empty")
        pl = _require_polars()
        frame = pl.DataFrame(rows, schema=self._schema, orient="row")
        return _Block(rows, self._key(rows[0]), self._key(rows[-1]), frame)


class FastTickPolarsHistory:
    """Append-only per-instrument FAST table; no global live reconciliation."""

    def __init__(self, *, rows_per_block: int = 4096) -> None:
        self._table = ImmutablePolarsBlockTable(
            ("instrument_tick_sequence",),
            rows_per_block=rows_per_block,
        )
        self._next_arrival_row = 1

    @property
    def next_arrival_row(self) -> int:
        return self._next_arrival_row

    @property
    def blocks(self) -> ImmutablePolarsBlockTable:
        return self._table

    def append(self, rows: Iterable[FastTickRow]) -> None:
        values = tuple(rows)
        for offset, row in enumerate(values):
            expected = self._next_arrival_row + offset
            if row.instrument_tick_sequence != expected:
                raise PolarsBlockError(
                    f"FAST append expected row {expected}, got "
                    f"{row.instrument_tick_sequence}"
                )
        self._table.append(row.as_dict() for row in values)
        self._next_arrival_row += len(values)


class EventPolarsHistory:
    """Apply Event CDC to immutable blocks with one atomic directory swap."""

    _KEY_COLUMNS = (
        "channel",
        "business_sequence",
        "source_event_ordinal",
        "derived_event_ordinal",
        "affected_order_id",
    )

    def __init__(
        self,
        rows: Iterable[DerivedEvent] = (),
        *,
        next_change_sequence: int = 1,
        rows_per_block: int = 1024,
    ) -> None:
        initial = tuple(rows)
        self._cdc = EventCdcApplier(
            initial, next_change_sequence=next_change_sequence
        )
        self._table = ImmutablePolarsBlockTable(
            self._KEY_COLUMNS, rows_per_block=rows_per_block
        )
        self._table.replace_all(row.as_dict() for row in initial)

    @property
    def rows(self) -> tuple[DerivedEvent, ...]:
        return self._cdc.rows

    @property
    def blocks(self) -> ImmutablePolarsBlockTable:
        return self._table

    @property
    def next_change_sequence(self) -> int:
        return self._cdc.next_change_sequence

    def apply(self, mutations: Iterable[EventMutation]) -> None:
        batch = tuple(mutations)
        candidate_cdc = self._cdc.fork()
        candidate_table = self._table.fork()
        for mutation in batch:
            rows_before = candidate_cdc.rows
            pending_before = candidate_cdc._pending
            # Both objects are private candidates. Stepping one record at a
            # time lets the block directory mirror multiple transactions in
            # one batch while the public history remains batch-atomic.
            candidate_cdc._apply_in_place((mutation,))

            if mutation.kind is EventMutationKind.RANGE_REPLACE_COMMIT:
                if pending_before is None:
                    raise CdcProtocolError("range commit has no pending transaction")
                replacement = tuple(pending_before.rows)
                if (
                    pending_before.range_scope
                    is EventRangeReplaceScope.CHANNEL_SUFFIX
                ):
                    candidate_table.replace_channel_suffix(
                        pending_before.range_channel,
                        pending_before.range_begin_business_sequence,
                        (row.as_dict() for row in replacement),
                    )
                elif pending_before.replace_entire_instrument:
                    candidate_table.replace_all(
                        row.as_dict() for row in replacement
                    )
                else:
                    assert pending_before.range_begin is not None
                    assert pending_before.range_end_exclusive is not None
                    candidate_table.replace_range(
                        self._key(pending_before.range_begin),
                        self._key(pending_before.range_end_exclusive),
                        (row.as_dict() for row in replacement),
                    )
                continue

            if candidate_cdc.rows is rows_before:
                # BEGIN and CHUNK advance only private CDC state.
                continue
            before_by_uid = {row.uid: row for row in rows_before}
            if mutation.kind is EventMutationKind.DELETE:
                assert mutation.uid is not None
                candidate_table.delete(
                    self._key(before_by_uid[mutation.uid].order_key)
                )
            elif mutation.kind in (
                EventMutationKind.INSERT,
                EventMutationKind.UPDATE,
            ):
                assert mutation.row is not None
                old = before_by_uid.get(mutation.row.uid)
                if (
                    old is not None
                    and old.order_key != mutation.row.order_key
                ):
                    candidate_table.delete(self._key(old.order_key))
                candidate_table.upsert(mutation.row.as_dict())
        self._table.publish_from(candidate_table)
        self._cdc = candidate_cdc

    @staticmethod
    def _key(key) -> tuple[int, int, int, int, int]:
        return (
            key.channel,
            key.business_sequence,
            key.source_event_ordinal,
            key.derived_event_ordinal,
            key.affected_order_id,
        )


class KLinePolarsHistory:
    """Apply revisioned KLine UPSERT/DELETE mutations by stable bar key."""

    _KEY_COLUMNS = (
        "instrument_id",
        "window_id",
        "window_start_ns_since_midnight",
    )

    def __init__(
        self,
        bars: Iterable[KLineBar] = (),
        *,
        next_change_sequence: int = 1,
        rows_per_block: int = 1024,
    ) -> None:
        initial = tuple(bars)
        self._cdc = KLineCdcApplier(
            initial, next_change_sequence=next_change_sequence
        )
        self._table = ImmutablePolarsBlockTable(
            self._KEY_COLUMNS, rows_per_block=rows_per_block
        )
        self._table.replace_all(bar.as_dict() for bar in initial)

    @property
    def bars(self) -> tuple[KLineBar, ...]:
        return self._cdc.bars

    @property
    def blocks(self) -> ImmutablePolarsBlockTable:
        return self._table

    @property
    def next_change_sequence(self) -> int:
        return self._cdc.next_change_sequence

    def apply(self, mutations: Iterable[KLineMutation]) -> None:
        batch = tuple(mutations)
        candidate_cdc = self._cdc.fork()
        candidate_cdc.apply(batch)
        if candidate_cdc.bars == self._cdc.bars:
            # BEGIN/CHUNK advance the cursor while retaining the old visible
            # bars. Keep the private transaction state for a later COMMIT.
            self._cdc = candidate_cdc
            return
        candidate_table = self._table.fork()
        old = {bar.key: bar for bar in self._cdc.bars}
        new = {bar.key: bar for bar in candidate_cdc.bars}
        for key in old.keys() - new.keys():
            candidate_table.delete(
                (
                    key.instrument_id,
                    key.window_id,
                    key.window_start_ns_since_midnight,
                )
            )
        for key, bar in new.items():
            if old.get(key) != bar:
                candidate_table.upsert(bar.as_dict())
        self._table.publish_from(candidate_table)
        self._cdc = candidate_cdc


__all__ = [
    "EventPolarsHistory",
    "FastTickPolarsHistory",
    "ImmutablePolarsBlockTable",
    "KLinePolarsHistory",
    "PolarsBlockError",
    "PolarsUnavailableError",
]
