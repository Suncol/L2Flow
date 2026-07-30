"""Public raw instrument-event history API over the isolated V2 worker.

The underlying Wire V2 delta service names these records "ticks".  At the
Python API boundary they are raw/normalized instrument events from source
slots 1 and 3: Shanghai tick records and Shenzhen order/transaction records.
Snapshot records and derived canonical order-lifecycle events are
intentionally not part of this API.  The stream is suitable as deterministic
aggregator replay input.

One :class:`InstrumentRawEventHistoryReader` owns one reusable child process.
``read_all`` scans from that instrument's retained origin to one immutable
generation.  ``read_updates`` scans the finite difference from an
explicit-EOF-verified checkpoint to a newer immutable generation.
"""

from __future__ import annotations

from typing import Iterator, Optional, Sequence, TYPE_CHECKING, Union

from ._history_worker_protocol import ALL_RESULT_COLUMNS
from ._stream_control import positive_uint32
from .checkpoint import InstrumentTickDeltaCheckpoint
from .history_worker import (
    InstrumentTickDeltaResultBatch,
    InstrumentTickDeltaResultColumns,
    InstrumentTickDeltaResultCursor,
    InstrumentTickDeltaWorker,
)
from .models import (
    InstrumentKey,
    InstrumentLookupStatus,
    L2FlowRealtimeError,
    SessionIdentity,
)


if TYPE_CHECKING:
    from .client import L2FlowClient


# The public raw-event schema is exactly the fixed numeric result schema
# supported by the isolated worker.  Callers on a latency-sensitive path may
# select a strict subset when opening the reader; omitted columns are then not
# copied by the worker.
INSTRUMENT_RAW_EVENT_COLUMNS = ALL_RESULT_COLUMNS
DEFAULT_INSTRUMENT_RAW_EVENT_COLUMNS = INSTRUMENT_RAW_EVENT_COLUMNS

InstrumentRawEventReference = Union[int, InstrumentKey]
InstrumentRawEventCheckpoint = InstrumentTickDeltaCheckpoint
InstrumentRawEventColumns = InstrumentTickDeltaResultColumns


class InstrumentRawEventLookupError(L2FlowRealtimeError):
    """An exact instrument key is absent from this session's daily catalog."""

    def __init__(
        self,
        key: InstrumentKey,
        status: InstrumentLookupStatus,
    ) -> None:
        if not isinstance(key, InstrumentKey):
            raise TypeError("key must be InstrumentKey")
        try:
            status = InstrumentLookupStatus(status)
        except (TypeError, ValueError) as error:
            raise ValueError(
                "status is not an instrument lookup status"
            ) from error
        if status is InstrumentLookupStatus.FOUND:
            raise ValueError("FOUND is not a lookup failure")
        self.key = key
        self.status = status
        super().__init__(
            "instrument raw-event history key lookup failed: "
            f"{status.name}"
        )


class InstrumentRawEventBatch:
    """One leased columnar event batch from the worker result ring.

    The batch must be closed before its ring slot can be reused.  ``batches()``
    and the context-manager protocol close it automatically.  ``read_columns``
    materializes owned tuples; ``borrow_column`` provides a zero-copy,
    context-bounded view.
    """

    __slots__ = ("_batch",)

    def __init__(self, batch: InstrumentTickDeltaResultBatch) -> None:
        if not isinstance(batch, InstrumentTickDeltaResultBatch):
            raise TypeError(
                "batch must be InstrumentTickDeltaResultBatch"
            )
        self._batch = batch

    @property
    def columns(self) -> InstrumentRawEventColumns:
        return self._batch.columns

    @property
    def closed(self) -> bool:
        return self._batch.closed

    @property
    def slot_index(self) -> int:
        return self._batch.slot_index

    @property
    def transfer_sequence(self) -> int:
        return self._batch.transfer_sequence

    @property
    def batch_index(self) -> int:
        """Zero-based index of this batch within its finite read."""

        return self._batch.page_index

    @property
    def generation(self) -> int:
        return self._batch.generation

    @property
    def record_count(self) -> int:
        return self._batch.record_count

    @property
    def cumulative_record_count(self) -> int:
        return self._batch.cumulative_record_count

    @property
    def cumulative_source_record_counts(
        self,
    ) -> tuple[int, int, int, int]:
        return self._batch.cumulative_source_record_counts

    @property
    def first_ingress_sequence(self) -> int:
        return self._batch.first_ingress_sequence

    @property
    def last_ingress_sequence(self) -> int:
        return self._batch.last_ingress_sequence

    @property
    def first_tick_stream_sequence(self) -> int:
        return self._batch.first_tick_stream_sequence

    @property
    def last_tick_stream_sequence(self) -> int:
        return self._batch.last_tick_stream_sequence

    @property
    def worker_read_start_ns(self) -> int:
        return self._batch.worker_read_start_ns

    @property
    def worker_read_return_ns(self) -> int:
        return self._batch.worker_read_return_ns

    @property
    def worker_publish_begin_ns(self) -> int:
        return self._batch.worker_publish_begin_ns

    @property
    def result_ready_ns(self) -> int:
        return self._batch.result_ready_ns

    @property
    def worker_ring_publish_return_ns(self) -> int:
        return self._batch.worker_ring_publish_return_ns

    def __len__(self) -> int:
        return self.record_count

    def read_columns(
        self, *names: str
    ) -> dict[str, tuple[object, ...]]:
        return self._batch.read_columns(*names)

    def materialize_all(self) -> dict[str, tuple[object, ...]]:
        return self._batch.materialize_all()

    def borrow_column(self, name: str):
        return self._batch.borrow_column(name)

    def close(self) -> None:
        self._batch.close()

    def __enter__(self) -> "InstrumentRawEventBatch":
        self._batch.__enter__()
        return self

    def __exit__(self, _type, _value, _traceback) -> None:
        self.close()


class InstrumentRawEventReadCursor:
    """One finite full or incremental event read.

    ``verified_checkpoint`` is deliberately unavailable until the cursor has
    consumed the separate explicit EOF notification.  Exhausting ``batches``
    or receiving ``None`` from ``read_batch`` performs that verification.
    Closing a cursor early cancels the read and never creates a checkpoint.
    """

    __slots__ = ("_cursor",)

    def __init__(
        self, cursor: InstrumentTickDeltaResultCursor
    ) -> None:
        if not isinstance(cursor, InstrumentTickDeltaResultCursor):
            raise TypeError(
                "cursor must be InstrumentTickDeltaResultCursor"
            )
        self._cursor = cursor

    @property
    def worker_pid(self) -> int:
        return self._cursor.worker_pid

    @property
    def instrument_id(self) -> int:
        return self._cursor.instrument_id

    @property
    def base_checkpoint(
        self,
    ) -> Optional[InstrumentRawEventCheckpoint]:
        return self._cursor.base_checkpoint

    @property
    def is_full_read(self) -> bool:
        return self.base_checkpoint is None

    @property
    def generation(self) -> int:
        return self._cursor.generation

    @property
    def expected_record_count(self) -> int:
        return self._cursor.expected_record_count

    @property
    def target_record_count(self) -> int:
        """Raw instrument-event count at the pinned target generation."""

        return self._cursor.target_tick_record_count

    @property
    def history_published_monotonic_ns(self) -> int:
        return self._cursor.history_published_monotonic_ns

    @property
    def next_batch_index(self) -> int:
        return self._cursor.next_page_index

    @property
    def cumulative_record_count(self) -> int:
        return self._cursor.cumulative_record_count

    @property
    def cumulative_source_record_counts(
        self,
    ) -> tuple[int, int, int, int]:
        return self._cursor.cumulative_source_record_counts

    @property
    def done(self) -> bool:
        return self._cursor.done

    @property
    def eof(self) -> bool:
        return self._cursor.done

    @property
    def closed(self) -> bool:
        return self._cursor.closed

    @property
    def verified_checkpoint(self) -> InstrumentRawEventCheckpoint:
        return self._cursor.verified_checkpoint

    @property
    def eof_worker_read_start_ns(self) -> int:
        return self._cursor.eof_worker_read_start_ns

    @property
    def eof_worker_read_return_ns(self) -> int:
        return self._cursor.eof_worker_read_return_ns

    @property
    def complete_publish_begin_ns(self) -> int:
        return self._cursor.complete_publish_begin_ns

    @property
    def complete_ready_ns(self) -> int:
        return self._cursor.complete_ready_ns

    @property
    def complete_ring_publish_return_ns(self) -> int:
        return self._cursor.complete_ring_publish_return_ns

    def read_batch(self) -> Optional[InstrumentRawEventBatch]:
        batch = self._cursor.read_batch()
        if batch is None:
            return None
        return InstrumentRawEventBatch(batch)

    def batches(self) -> Iterator[InstrumentRawEventBatch]:
        while not self.done:
            batch = self.read_batch()
            if batch is None:
                break
            try:
                yield batch
            finally:
                batch.close()

    def close(self) -> None:
        self._cursor.close()

    def __enter__(self) -> "InstrumentRawEventReadCursor":
        self._cursor.__enter__()
        return self

    def __exit__(self, _type, _value, _traceback) -> None:
        self.close()


class InstrumentRawEventHistoryReader:
    """Reusable isolated-process reader for raw instrument-event history.

    A reader supports one active cursor at a time, matching the stateful
    worker protocol. Reader, cursor, and batch methods are serial-only and
    must not be called concurrently. Sequential full and update reads reuse
    ``worker_pid``; they do not create a child process per generation.
    """

    __slots__ = ("_client", "_worker")

    def __init__(
        self,
        client: "L2FlowClient",
        worker: InstrumentTickDeltaWorker,
    ) -> None:
        if not isinstance(worker, InstrumentTickDeltaWorker):
            raise TypeError(
                "worker must be InstrumentTickDeltaWorker"
            )
        self._client = client
        self._worker = worker

    @property
    def closed(self) -> bool:
        return self._worker.closed

    @property
    def failed(self) -> bool:
        return self._worker.failed

    @property
    def worker_pid(self) -> int:
        return self._worker.pid

    @property
    def session_identity(self) -> SessionIdentity:
        return self._worker.session_identity

    @property
    def trade_date(self) -> int:
        return self._worker.trade_date

    @property
    def capacity(self) -> int:
        return self._worker.capacity

    @property
    def ring_slots(self) -> int:
        return self._worker.ring_slots

    @property
    def batch_capacity(self) -> int:
        return self._worker.result_batch_records

    @property
    def raw_event_columns(self) -> tuple[str, ...]:
        return self._worker.result_columns

    def _resolve_instrument(
        self, instrument: InstrumentRawEventReference
    ) -> int:
        if isinstance(instrument, InstrumentKey):
            result = self._client.resolve_key(instrument)
            if result.status is not InstrumentLookupStatus.FOUND:
                raise InstrumentRawEventLookupError(
                    instrument, result.status
                )
            return positive_uint32(
                result.instrument_id, "instrument_id"
            )
        return positive_uint32(instrument, "instrument_id")

    def read_all(
        self,
        instrument: InstrumentRawEventReference,
        *,
        batch_records: Optional[int] = None,
        expected_generation: Optional[int] = None,
    ) -> InstrumentRawEventReadCursor:
        """Read all retained tick/events for one pinned generation."""

        instrument_id = self._resolve_instrument(instrument)
        cursor = self._worker.open_instrument(
            instrument_id,
            base_checkpoint=None,
            requested_page_records=batch_records,
            expected_generation=expected_generation,
        )
        return InstrumentRawEventReadCursor(cursor)

    def read_updates(
        self,
        instrument: InstrumentRawEventReference,
        checkpoint: InstrumentRawEventCheckpoint,
        *,
        batch_records: Optional[int] = None,
        expected_generation: Optional[int] = None,
    ) -> InstrumentRawEventReadCursor:
        """Read the finite event delta after one verified checkpoint."""

        if not isinstance(checkpoint, InstrumentTickDeltaCheckpoint):
            raise TypeError(
                "checkpoint must be an EOF-verified "
                "InstrumentRawEventCheckpoint"
            )
        instrument_id = self._resolve_instrument(instrument)
        if checkpoint.instrument_id != instrument_id:
            raise ValueError(
                "checkpoint belongs to another instrument"
            )
        cursor = self._worker.open_instrument(
            instrument_id,
            base_checkpoint=checkpoint,
            requested_page_records=batch_records,
            expected_generation=expected_generation,
        )
        return InstrumentRawEventReadCursor(cursor)

    def close(self) -> None:
        self._worker.close()

    def __enter__(self) -> "InstrumentRawEventHistoryReader":
        self._worker.__enter__()
        return self

    def __exit__(self, _type, _value, _traceback) -> None:
        self.close()


__all__ = [
    "DEFAULT_INSTRUMENT_RAW_EVENT_COLUMNS",
    "INSTRUMENT_RAW_EVENT_COLUMNS",
    "InstrumentRawEventBatch",
    "InstrumentRawEventCheckpoint",
    "InstrumentRawEventColumns",
    "InstrumentRawEventHistoryReader",
    "InstrumentRawEventLookupError",
    "InstrumentRawEventReadCursor",
    "InstrumentRawEventReference",
]
