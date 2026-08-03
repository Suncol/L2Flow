"""Independent-GIL Wire V2 tick-delta worker and fixed result ring."""

from __future__ import annotations

import array
import fcntl
import mmap
import os
import secrets
import socket
import struct
import subprocess
import sys
import threading
import time
from collections.abc import Iterator, Mapping, Sequence
from typing import Optional

from ._generation import (
    DailyCatalogSessionIdentity,
    validate_same_session,
)
from ._history_worker_protocol import (
    DEFAULT_RESULT_COLUMNS,
    NO_SLOT,
    NO_TIMEOUT_NS,
    RingLayout,
    SlotHeader,
    WorkerOpcode,
    WorkerStatus,
    column_mask,
    column_names,
    column_region,
    make_ring_layout,
    pack_control,
    pack_init_payload,
    pack_open_payload,
    pack_ring_header,
    parse_error_payload,
    parse_slot_header,
    recv_control,
    send_control,
    validate_ring_header,
)
from ._stream_control import (
    UINT64_MAX,
    positive_uint32,
    validate_page_records,
    validate_socket_path,
    validate_timeout,
)
from .checkpoint import InstrumentTickDeltaCheckpoint
from .instrument_delta import DeltaCheckpointUnverifiedError
from .models import (
    ClientClosedError,
    L2FlowRealtimeError,
    ProtocolError,
    SessionIdentity,
    StaleSessionError,
    UnavailableError,
    WireFormatError,
)


class HistoryWorkerError(L2FlowRealtimeError):
    """The isolated history worker failed its fixed protocol."""


class HistoryWorkerClosedError(ClientClosedError):
    """The isolated history worker or result cursor is closed."""


class HistoryWorkerInternalError(HistoryWorkerError):
    """The isolated process reported an unexpected internal failure."""


def _request_id() -> int:
    return secrets.randbits(64) or 1


def _expected_generation(value: Optional[int]) -> int:
    if value is None:
        return 0
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or value <= 0
        or value > UINT64_MAX
    ):
        raise ValueError(
            "expected_generation must be a positive uint64 or None"
        )
    return value


def _timeout_ns(timeout: Optional[float]) -> int:
    timeout = validate_timeout(timeout)
    if timeout is None:
        return NO_TIMEOUT_NS
    result = int(timeout * 1_000_000_000)
    if result <= 0 or result >= NO_TIMEOUT_NS:
        raise ValueError("worker timeout cannot be represented in ns")
    return result


def _raise_worker_error(packet) -> None:
    if packet.opcode is not WorkerOpcode.ERROR:
        raise ProtocolError(
            f"unexpected history worker opcode {packet.opcode.name}"
        )
    if (
        packet.status is WorkerStatus.OK
        or packet.transfer_sequence
        or packet.slot_index != NO_SLOT
        or packet.record_count
        or any(packet.args)
    ):
        raise ProtocolError("history worker ERROR is noncanonical")
    detail = parse_error_payload(packet.payload)
    if packet.status is WorkerStatus.STALE_SESSION:
        raise StaleSessionError(detail)
    if packet.status is WorkerStatus.UNAVAILABLE:
        raise UnavailableError(detail)
    if packet.status is WorkerStatus.WIRE_FAILURE:
        raise WireFormatError(detail)
    if packet.status is WorkerStatus.INVALID_REQUEST:
        raise ProtocolError(detail)
    raise HistoryWorkerInternalError(detail)


class _LeasedResultColumnView(Sequence[int]):
    """Zero-copy scalar access that cannot export the mmap buffer."""

    __slots__ = (
        "_batch",
        "_begin",
        "_count",
        "_width",
        "_unpacker",
        "_active",
    )

    def __init__(
        self,
        batch: "InstrumentTickDeltaResultBatch",
        begin: int,
        count: int,
        format_: str,
        width: int,
    ) -> None:
        self._batch = batch
        self._begin = begin
        self._count = count
        self._width = width
        self._unpacker = struct.Struct("<" + format_)
        self._active = True

    def _require_active(self) -> None:
        if not self._active:
            raise HistoryWorkerClosedError(
                "borrowed result column lease has ended"
            )
        self._batch._require_open()

    def _deactivate(self) -> None:
        self._active = False

    def __len__(self) -> int:
        self._require_active()
        return self._count

    def __getitem__(self, index):
        self._require_active()
        if isinstance(index, slice):
            return tuple(
                self[position]
                for position in range(*index.indices(self._count))
            )
        if (
            not isinstance(index, int)
            or isinstance(index, bool)
        ):
            raise TypeError("result column index must be an integer")
        if index < 0:
            index += self._count
        if index < 0 or index >= self._count:
            raise IndexError("result column index is out of range")
        return self._unpacker.unpack_from(
            self._batch._worker._mapping,
            self._begin + index * self._width,
        )[0]


class _BorrowedResultColumn:
    __slots__ = ("_batch", "_name", "_view", "_entered")

    def __init__(
        self, batch: "InstrumentTickDeltaResultBatch", name: str
    ) -> None:
        self._batch = batch
        self._name = name
        self._view: Optional[_LeasedResultColumnView] = None
        self._entered = False

    def __enter__(self) -> _LeasedResultColumnView:
        if self._entered:
            raise RuntimeError("result column lease is not reentrant")
        batch = self._batch
        batch._require_open()
        if self._name not in batch.columns:
            raise KeyError(self._name)
        begin, _end, spec = column_region(
            batch._worker._layout,
            batch.slot_index,
            self._name,
            batch.record_count,
        )
        view = _LeasedResultColumnView(
            batch,
            begin,
            batch.record_count,
            spec.format,
            spec.width,
        )
        batch._borrow_count += 1
        self._view = view
        self._entered = True
        return view

    def __exit__(self, _type, _value, _traceback) -> None:
        if not self._entered:
            return
        view = self._view
        self._view = None
        self._entered = False
        try:
            if view is not None:
                view._deactivate()
        finally:
            self._batch._borrow_count -= 1


class InstrumentTickDeltaResultColumns(
    Mapping[str, tuple[object, ...]]
):
    """Lazy selected columns over one leased columnar result slot."""

    __slots__ = ("_batch", "_names", "_cache")

    def __init__(
        self,
        batch: "InstrumentTickDeltaResultBatch",
        names: tuple[str, ...],
    ) -> None:
        self._batch = batch
        self._names = names
        self._cache: dict[str, tuple[object, ...]] = {}

    def __len__(self) -> int:
        return len(self._names)

    def __iter__(self) -> Iterator[str]:
        return iter(self._names)

    def __getitem__(self, name: str) -> tuple[object, ...]:
        return self.read_columns(name)[name]

    @property
    def row_count(self) -> int:
        return self._batch.record_count

    @property
    def materialized_column_count(self) -> int:
        return len(self._cache)

    @property
    def column_names(self) -> tuple[str, ...]:
        return self._names

    def read_columns(
        self, *names: str
    ) -> dict[str, tuple[object, ...]]:
        self._batch._require_open()
        if not names:
            raise ValueError("at least one result column is required")
        if any(not isinstance(name, str) for name in names):
            raise TypeError("result column names must be strings")
        if len(set(names)) != len(names):
            raise ValueError("result column names must be unique")
        for name in names:
            if name not in self._names:
                raise KeyError(name)
            if name in self._cache:
                continue
            begin, end, spec = column_region(
                self._batch._worker._layout,
                self._batch.slot_index,
                name,
                self._batch.record_count,
            )
            raw = memoryview(
                self._batch._worker._mapping
            )[begin:end]
            typed: Optional[memoryview] = None
            try:
                typed = raw.cast(spec.format)
                # The result ring is native little-endian and contiguous.
                # Iterating the typed view avoids a Python generator and a
                # per-element struct tuple while retaining explicit tuple
                # ownership after the slot is released.
                self._cache[name] = tuple(typed)
            finally:
                if typed is not None:
                    typed.release()
                raw.release()
        return {name: self._cache[name] for name in names}

    def materialize_all(self) -> dict[str, tuple[object, ...]]:
        return self.read_columns(*self._names)

    def copy_arrays(
        self, *names: str
    ) -> dict[str, array.array]:
        """Bulk-copy selected ring columns into owned native arrays.

        Unlike :meth:`read_columns`, this path does not create one Python
        integer per cell.  It is intended for Arrow/Polars adapters that need
        independent ownership before the worker slot is released.  A fresh
        array is returned on every call; callers may safely retain or mutate
        it after this batch closes.
        """

        self._batch._require_open()
        if not names:
            raise ValueError("at least one result column is required")
        if any(not isinstance(name, str) for name in names):
            raise TypeError("result column names must be strings")
        if len(set(names)) != len(names):
            raise ValueError("result column names must be unique")
        result: dict[str, array.array] = {}
        for name in names:
            if name not in self._names:
                raise KeyError(name)
            begin, end, spec = column_region(
                self._batch._worker._layout,
                self._batch.slot_index,
                name,
                self._batch.record_count,
            )
            raw = memoryview(self._batch._worker._mapping)[begin:end]
            try:
                owned = array.array(spec.format)
                owned.frombytes(raw)
            finally:
                raw.release()
            if len(owned) != self._batch.record_count:
                raise WireFormatError(
                    "owned result column length changed during copy"
                )
            result[name] = owned
        return result

    def borrow(self, name: str) -> _BorrowedResultColumn:
        """Borrow zero-copy scalar access for a bounded ``with`` block.

        The returned sequence intentionally does not implement Python's
        buffer protocol, so a derived memoryview cannot outlive the slot
        lease.  Use ``read_columns`` for fast owned tuple construction.
        """

        if not isinstance(name, str):
            raise TypeError("result column name must be a string")
        if name not in self._names:
            raise KeyError(name)
        return _BorrowedResultColumn(self._batch, name)


class InstrumentTickDeltaResultBatch:
    """One fixed-schema result slot leased from the worker-owned ring."""

    __slots__ = (
        "_worker",
        "_cursor",
        "_header",
        "_closed",
        "_borrow_count",
        "_result_ready_ns",
        "_worker_ring_publish_return_ns",
        "columns",
    )

    def __init__(
        self,
        worker: "InstrumentTickDeltaWorker",
        cursor: "InstrumentTickDeltaResultCursor",
        header: SlotHeader,
    ) -> None:
        self._worker = worker
        self._cursor = cursor
        self._header = header
        self._closed = False
        self._borrow_count = 0
        self._result_ready_ns = cursor._ready_times[
            header.transfer_sequence
        ]
        self._worker_ring_publish_return_ns = (
            cursor._ring_publish_return_times[
                header.transfer_sequence
            ]
        )
        self.columns = InstrumentTickDeltaResultColumns(
            self, worker.result_columns
        )

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def slot_index(self) -> int:
        return self._header.slot_index

    @property
    def transfer_sequence(self) -> int:
        return self._header.transfer_sequence

    @property
    def page_index(self) -> int:
        return self._header.page_index

    @property
    def generation(self) -> int:
        return self._header.generation

    @property
    def record_count(self) -> int:
        return self._header.record_count

    @property
    def cumulative_record_count(self) -> int:
        return self._header.cumulative_record_count

    @property
    def cumulative_source_record_counts(
        self,
    ) -> tuple[int, int, int, int]:
        return self._header.cumulative_source_record_counts

    @property
    def first_ingress_sequence(self) -> int:
        return self._header.first_ingress_sequence

    @property
    def last_ingress_sequence(self) -> int:
        return self._header.last_ingress_sequence

    @property
    def first_tick_stream_sequence(self) -> int:
        return self._header.first_tick_stream_sequence

    @property
    def last_tick_stream_sequence(self) -> int:
        return self._header.last_tick_stream_sequence

    @property
    def worker_read_start_ns(self) -> int:
        return self._header.worker_read_start_ns

    @property
    def worker_read_return_ns(self) -> int:
        return self._header.worker_read_return_ns

    @property
    def worker_publish_begin_ns(self) -> int:
        """Time after column writes and before header/notification publish."""

        return self._header.worker_publish_begin_ns

    @property
    def result_ready_ns(self) -> int:
        return self._result_ready_ns

    @property
    def worker_ring_publish_return_ns(self) -> int:
        """Time after the complete fixed slot became worker-visible."""

        return self._worker_ring_publish_return_ns

    def __len__(self) -> int:
        return self.record_count

    def _require_open(self) -> None:
        if self._closed:
            raise HistoryWorkerClosedError(
                "history worker result batch is closed"
            )

    def read_columns(
        self, *names: str
    ) -> dict[str, tuple[object, ...]]:
        return self.columns.read_columns(*names)

    def copy_column_arrays(
        self, *names: str
    ) -> dict[str, array.array]:
        """Bulk-copy selected columns without Python scalar materialization."""

        return self.columns.copy_arrays(*names)

    def materialize_all(self) -> dict[str, tuple[object, ...]]:
        return self.columns.materialize_all()

    def borrow_column(self, name: str) -> _BorrowedResultColumn:
        return self.columns.borrow(name)

    def close(self) -> None:
        if self._closed:
            return
        if self._borrow_count:
            raise RuntimeError(
                "cannot release a result slot with borrowed views"
            )
        self._cursor._release_batch(self)
        self._closed = True

    def __enter__(self) -> "InstrumentTickDeltaResultBatch":
        self._require_open()
        return self

    def __exit__(self, _type, _value, _traceback) -> None:
        self.close()


class InstrumentTickDeltaResultCursor:
    """Forward-only derived-result cursor verified at explicit worker EOF."""

    __slots__ = (
        "_worker",
        "_request_id",
        "_instrument_id",
        "_base_checkpoint",
        "_generation",
        "_expected_record_count",
        "_target_tick_record_count",
        "_history_published_monotonic_ns",
        "_next_page_index",
        "_cumulative_record_count",
        "_source_counts",
        "_verified_checkpoint",
        "_eof_worker_read_start_ns",
        "_eof_worker_read_return_ns",
        "_complete_publish_begin_ns",
        "_complete_ring_publish_return_ns",
        "_complete_ready_ns",
        "_outstanding",
        "_ready_times",
        "_ring_publish_return_times",
        "_done",
        "_closed",
    )

    def __init__(
        self,
        *,
        worker: "InstrumentTickDeltaWorker",
        request_id: int,
        instrument_id: int,
        base_checkpoint: Optional[
            InstrumentTickDeltaCheckpoint
        ],
        generation: int,
        expected_record_count: int,
        target_tick_record_count: int,
        history_published_monotonic_ns: int,
    ) -> None:
        self._worker = worker
        self._request_id = request_id
        self._instrument_id = instrument_id
        self._base_checkpoint = base_checkpoint
        self._generation = generation
        self._expected_record_count = expected_record_count
        self._target_tick_record_count = target_tick_record_count
        self._history_published_monotonic_ns = (
            history_published_monotonic_ns
        )
        self._next_page_index = 0
        self._cumulative_record_count = 0
        self._source_counts = (0, 0, 0, 0)
        self._verified_checkpoint: Optional[
            InstrumentTickDeltaCheckpoint
        ] = None
        self._eof_worker_read_start_ns = 0
        self._eof_worker_read_return_ns = 0
        self._complete_publish_begin_ns = 0
        self._complete_ring_publish_return_ns = 0
        self._complete_ready_ns = 0
        self._outstanding: dict[
            int, InstrumentTickDeltaResultBatch
        ] = {}
        self._ready_times: dict[int, int] = {}
        self._ring_publish_return_times: dict[int, int] = {}
        self._done = False
        self._closed = False

    @property
    def worker_pid(self) -> int:
        return self._worker.pid

    @property
    def instrument_id(self) -> int:
        return self._instrument_id

    @property
    def base_checkpoint(
        self,
    ) -> Optional[InstrumentTickDeltaCheckpoint]:
        return self._base_checkpoint

    @property
    def generation(self) -> int:
        return self._generation

    @property
    def expected_record_count(self) -> int:
        return self._expected_record_count

    @property
    def target_tick_record_count(self) -> int:
        return self._target_tick_record_count

    @property
    def history_published_monotonic_ns(self) -> int:
        return self._history_published_monotonic_ns

    @property
    def next_page_index(self) -> int:
        return self._next_page_index

    @property
    def cumulative_record_count(self) -> int:
        return self._cumulative_record_count

    @property
    def cumulative_source_record_counts(
        self,
    ) -> tuple[int, int, int, int]:
        return self._source_counts

    @property
    def done(self) -> bool:
        return self._done

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def verified_checkpoint(self) -> InstrumentTickDeltaCheckpoint:
        if self._verified_checkpoint is None:
            raise DeltaCheckpointUnverifiedError(
                "worker checkpoint is available only after explicit EOF"
            )
        return self._verified_checkpoint

    @property
    def eof_worker_read_start_ns(self) -> int:
        if not self._done:
            raise DeltaCheckpointUnverifiedError(
                "worker EOF timing is unavailable before explicit EOF"
            )
        return self._eof_worker_read_start_ns

    @property
    def eof_worker_read_return_ns(self) -> int:
        if not self._done:
            raise DeltaCheckpointUnverifiedError(
                "worker EOF timing is unavailable before explicit EOF"
            )
        return self._eof_worker_read_return_ns

    @property
    def complete_publish_begin_ns(self) -> int:
        """EOF column/checkpoint-write boundary before its notification."""

        if not self._done:
            raise DeltaCheckpointUnverifiedError(
                "worker completion timing is unavailable before EOF"
            )
        return self._complete_publish_begin_ns

    @property
    def complete_ready_ns(self) -> int:
        if not self._done:
            raise DeltaCheckpointUnverifiedError(
                "worker completion timing is unavailable before EOF"
            )
        return self._complete_ready_ns

    @property
    def complete_ring_publish_return_ns(self) -> int:
        """Time after the terminal slot write and before notification."""

        if not self._done:
            raise DeltaCheckpointUnverifiedError(
                "worker completion timing is unavailable before EOF"
            )
        return self._complete_ring_publish_return_ns

    def _require_open(self) -> None:
        if self._closed:
            raise HistoryWorkerClosedError(
                "history worker result cursor is closed"
            )
        self._worker._require_open()

    def _validate_transfer(self, packet) -> SlotHeader:
        worker = self._worker
        expected_sequence = worker._next_transfer_sequence + 1
        expected_slot = (
            expected_sequence - 1
        ) % worker._layout.slot_count
        if (
            packet.status is not WorkerStatus.OK
            or packet.request_id != self._request_id
            or packet.transfer_sequence != expected_sequence
            or packet.slot_index != expected_slot
            or packet.args[0] == 0
            or any(packet.args[1:])
            or any(packet.payload)
        ):
            raise ProtocolError(
                "history worker result notification is out of order"
            )
        header = parse_slot_header(
            worker._mapping,
            worker._layout,
            expected_slot_index=expected_slot,
            expected_request_id=self._request_id,
            expected_transfer_sequence=expected_sequence,
            expected_record_count=packet.record_count,
            configured_column_mask=worker._result_column_mask,
        )
        if (
            header.generation != self._generation
            or header.page_index != self._next_page_index
            or header.cumulative_record_count
            < self._cumulative_record_count
            or any(
                current < prior
                for current, prior in zip(
                    header.cumulative_source_record_counts,
                    self._source_counts,
                )
            )
        ):
            raise WireFormatError(
                "history worker result generation/page/count moved"
            )
        result_ready_ns = time.monotonic_ns()
        ring_publish_return_ns = packet.args[0]
        if (
            ring_publish_return_ns
            < header.worker_publish_begin_ns
            or result_ready_ns < ring_publish_return_ns
        ):
            raise WireFormatError(
                "history worker result timing is non-monotonic"
            )
        worker._next_transfer_sequence = expected_sequence
        self._ready_times[expected_sequence] = result_ready_ns
        self._ring_publish_return_times[
            expected_sequence
        ] = ring_publish_return_ns
        return header

    def _send_release(self, header: SlotHeader) -> None:
        self._worker._send(
            pack_control(
                WorkerOpcode.RELEASE,
                request_id=self._request_id,
                transfer_sequence=header.transfer_sequence,
                slot_index=header.slot_index,
            )
        )

    def _release_batch(
        self, batch: InstrumentTickDeltaResultBatch
    ) -> None:
        current = self._outstanding.get(batch.transfer_sequence)
        if current is not batch:
            if self._closed:
                return
            raise ProtocolError(
                "result batch does not own an outstanding slot"
            )
        self._send_release(batch._header)
        del self._outstanding[batch.transfer_sequence]
        self._ready_times.pop(batch.transfer_sequence, None)
        self._ring_publish_return_times.pop(
            batch.transfer_sequence, None
        )

    def _read_batch_impl(
        self,
    ) -> Optional[InstrumentTickDeltaResultBatch]:
        self._require_open()
        if self._done:
            return None
        packet = self._worker._recv()
        if packet.opcode is WorkerOpcode.ERROR:
            self._worker._fail()
            _raise_worker_error(packet)
        if packet.opcode not in (
            WorkerOpcode.RESULT_READY,
            WorkerOpcode.COMPLETE,
        ):
            raise ProtocolError(
                "history worker returned an unexpected scan response"
            )
        header = self._validate_transfer(packet)
        if packet.opcode is WorkerOpcode.RESULT_READY:
            if (
                header.eof
                or header.record_count == 0
                or header.cumulative_record_count
                != self._cumulative_record_count
                + header.record_count
                or sum(header.cumulative_source_record_counts)
                != header.cumulative_record_count
                or header.cumulative_source_record_counts[0]
                or header.cumulative_source_record_counts[2]
            ):
                raise WireFormatError(
                    "history worker data batch counts do not reconcile"
                )
            self._next_page_index += 1
            self._cumulative_record_count = (
                header.cumulative_record_count
            )
            self._source_counts = (
                header.cumulative_source_record_counts
            )
            batch = InstrumentTickDeltaResultBatch(
                self._worker, self, header
            )
            self._outstanding[header.transfer_sequence] = batch
            return batch

        if (
            not header.eof
            or header.record_count
            or header.cumulative_record_count
            != self._cumulative_record_count
            or header.cumulative_source_record_counts
            != self._source_counts
            or self._cumulative_record_count
            != self._expected_record_count
            or header.checkpoint_wire is None
        ):
            raise WireFormatError(
                "history worker explicit EOF did not reconcile"
            )
        checkpoint = InstrumentTickDeltaCheckpoint.from_wire(
            header.checkpoint_wire
        )
        validate_same_session(
            checkpoint.endpoint,
            expected=self._worker.expected_session,
        )
        checkpoint.ensure_session(
            run_id=self._worker.session_identity.run_id,
            session_epoch=(
                self._worker.session_identity.session_epoch
            ),
            trade_date=self._worker.trade_date,
            capacity=self._worker.capacity,
        )
        if (
            checkpoint.instrument_id != self._instrument_id
            or checkpoint.generation != self._generation
            or checkpoint.instrument_tick_record_count
            != self._target_tick_record_count
        ):
            raise WireFormatError(
                "history worker checkpoint identity/count changed"
            )
        if self._base_checkpoint is None:
            base_tick_count = 0
            base_source_counts = (0, 0, 0, 0)
        else:
            checkpoint.ensure_successor_of(self._base_checkpoint)
            base_tick_count = (
                self._base_checkpoint.instrument_tick_record_count
            )
            base_source_counts = (
                self._base_checkpoint
                .instrument_tick_source_record_counts
            )
        expected_delta_source_counts = tuple(
            target - base
            for target, base in zip(
                checkpoint.instrument_tick_source_record_counts,
                base_source_counts,
            )
        )
        if (
            checkpoint.instrument_tick_record_count - base_tick_count
            != self._expected_record_count
            or expected_delta_source_counts != self._source_counts
        ):
            raise WireFormatError(
                "history worker checkpoint delta counts are inconsistent"
            )
        complete_ready_ns = self._ready_times[
            header.transfer_sequence
        ]
        complete_ring_publish_return_ns = (
            self._ring_publish_return_times[
                header.transfer_sequence
            ]
        )
        self._send_release(header)
        self._ready_times.pop(header.transfer_sequence, None)
        self._ring_publish_return_times.pop(
            header.transfer_sequence, None
        )
        self._verified_checkpoint = checkpoint
        self._eof_worker_read_start_ns = (
            header.worker_read_start_ns
        )
        self._eof_worker_read_return_ns = (
            header.worker_read_return_ns
        )
        self._complete_publish_begin_ns = (
            header.worker_publish_begin_ns
        )
        self._complete_ring_publish_return_ns = (
            complete_ring_publish_return_ns
        )
        self._complete_ready_ns = complete_ready_ns
        self._next_page_index += 1
        self._done = True
        return None

    def read_batch(
        self,
    ) -> Optional[InstrumentTickDeltaResultBatch]:
        try:
            return self._read_batch_impl()
        except BaseException:
            # Any invalid result notification or slot makes ownership and
            # subsequent sequence reconciliation unknowable.  There is no
            # compatibility/recovery path for this replacement protocol.
            self._worker._fail()
            raise

    def batches(self) -> Iterator[InstrumentTickDeltaResultBatch]:
        while not self._done:
            batch = self.read_batch()
            if batch is None:
                break
            try:
                yield batch
            finally:
                batch.close()

    def close(self) -> None:
        if self._closed:
            return
        first_error: Optional[BaseException] = None
        for batch in tuple(self._outstanding.values()):
            try:
                batch.close()
            except BaseException as error:
                first_error = first_error or error
        if not self._done and not self._worker.closed:
            try:
                self._worker._send(
                    pack_control(
                        WorkerOpcode.CANCEL,
                        request_id=self._request_id,
                    )
                )
                while True:
                    packet = self._worker._recv()
                    if packet.opcode is WorkerOpcode.ERROR:
                        _raise_worker_error(packet)
                    if packet.opcode is WorkerOpcode.CANCELED:
                        if (
                            packet.status is not WorkerStatus.OK
                            or packet.request_id != self._request_id
                            or packet.transfer_sequence
                            or packet.slot_index != NO_SLOT
                            or packet.record_count
                            or any(packet.args)
                            or any(packet.payload)
                        ):
                            raise ProtocolError(
                                "worker CANCELED is noncanonical"
                            )
                        break
                    if packet.opcode not in (
                        WorkerOpcode.RESULT_READY,
                        WorkerOpcode.COMPLETE,
                    ):
                        raise ProtocolError(
                            "worker cancel drain returned an opcode"
                        )
                    header = self._validate_transfer(packet)
                    if packet.opcode is WorkerOpcode.RESULT_READY:
                        if (
                            header.eof
                            or header.record_count == 0
                            or header.cumulative_record_count
                            != self._cumulative_record_count
                            + header.record_count
                            or sum(
                                header.cumulative_source_record_counts
                            )
                            != header.cumulative_record_count
                            or header.cumulative_source_record_counts[
                                0
                            ]
                            or header.cumulative_source_record_counts[
                                2
                            ]
                        ):
                            raise WireFormatError(
                                "discarded worker batch counts differ"
                            )
                        self._cumulative_record_count = (
                            header.cumulative_record_count
                        )
                        self._source_counts = (
                            header.cumulative_source_record_counts
                        )
                    elif (
                        not header.eof
                        or header.cumulative_record_count
                        != self._cumulative_record_count
                        or header.cumulative_source_record_counts
                        != self._source_counts
                    ):
                        raise WireFormatError(
                            "discarded worker EOF counts differ"
                        )
                    self._next_page_index += 1
                    self._send_release(header)
                    self._ready_times.pop(
                        header.transfer_sequence, None
                    )
                    self._ring_publish_return_times.pop(
                        header.transfer_sequence, None
                    )
            except BaseException as error:
                first_error = first_error or error
                self._worker._fail()
        self._closed = True
        self._worker._detach(self)
        if first_error is not None:
            raise first_error

    def __enter__(self) -> "InstrumentTickDeltaResultCursor":
        self._require_open()
        return self

    def __exit__(self, _type, _value, _traceback) -> None:
        self.close()


class InstrumentTickDeltaWorker:
    """A separate CPython process that alone maps raw delta history pages."""

    __slots__ = (
        "_channel",
        "_mapping",
        "_layout",
        "_result_column_mask",
        "_result_columns",
        "_process",
        "_pid",
        "_expected_session",
        "_timeout",
        "_active",
        "_next_transfer_sequence",
        "_lock",
        "_send_lock",
        "_closed",
        "_failed",
    )

    def __init__(
        self,
        *,
        channel: socket.socket,
        mapping: mmap.mmap,
        layout: RingLayout,
        result_column_mask: int,
        process: subprocess.Popen,
        pid: int,
        expected_session: DailyCatalogSessionIdentity,
        timeout: Optional[float],
    ) -> None:
        self._channel = channel
        self._mapping = mapping
        self._layout = layout
        self._result_column_mask = result_column_mask
        self._result_columns = column_names(result_column_mask)
        self._process = process
        self._pid = pid
        self._expected_session = expected_session
        self._timeout = timeout
        self._active: Optional[
            InstrumentTickDeltaResultCursor
        ] = None
        self._next_transfer_sequence = 0
        self._lock = threading.RLock()
        self._send_lock = threading.Lock()
        self._closed = False
        self._failed = False

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def failed(self) -> bool:
        return self._failed

    @property
    def pid(self) -> int:
        return self._pid

    @property
    def session_identity(self) -> SessionIdentity:
        return self._expected_session.session_identity

    @property
    def trade_date(self) -> int:
        return self._expected_session.trade_date

    @property
    def capacity(self) -> int:
        return self._expected_session.capacity

    @property
    def expected_session(self) -> DailyCatalogSessionIdentity:
        return self._expected_session

    @property
    def ring_slots(self) -> int:
        return self._layout.slot_count

    @property
    def result_batch_records(self) -> int:
        return self._layout.batch_capacity

    @property
    def result_columns(self) -> tuple[str, ...]:
        return self._result_columns

    def _require_open(self) -> None:
        if self._closed:
            raise HistoryWorkerClosedError(
                "history worker is closed"
            )
        if self._failed or self._process.poll() is not None:
            self._failed = True
            raise HistoryWorkerInternalError(
                "history worker process is not running"
            )

    def _send(self, packet: bytes) -> None:
        self._require_open()
        with self._send_lock:
            send_control(self._channel, packet)

    def _recv(self):
        self._require_open()
        try:
            return recv_control(self._channel)
        except BaseException:
            self._fail()
            raise

    def _fail(self) -> None:
        self._failed = True

    def _detach(
        self, cursor: InstrumentTickDeltaResultCursor
    ) -> None:
        with self._lock:
            if self._active is cursor:
                self._active = None

    def open_instrument(
        self,
        instrument_id: int,
        *,
        base_checkpoint: Optional[
            InstrumentTickDeltaCheckpoint
        ] = None,
        requested_page_records: Optional[int] = None,
        expected_generation: Optional[int] = None,
    ) -> InstrumentTickDeltaResultCursor:
        instrument_id = positive_uint32(
            instrument_id, "instrument_id"
        )
        if requested_page_records is None:
            requested_page_records = self._layout.batch_capacity
        requested_page_records = validate_page_records(
            requested_page_records
        )
        if requested_page_records > self._layout.batch_capacity:
            raise ValueError(
                "requested_page_records exceeds result slot capacity"
            )
        generation = _expected_generation(expected_generation)
        if base_checkpoint is not None:
            if not isinstance(
                base_checkpoint, InstrumentTickDeltaCheckpoint
            ):
                raise TypeError(
                    "base_checkpoint must be a verified V2 checkpoint"
                )
            base_checkpoint.ensure_session(
                run_id=self.session_identity.run_id,
                session_epoch=self.session_identity.session_epoch,
                trade_date=self.trade_date,
                capacity=self.capacity,
            )
            validate_same_session(
                base_checkpoint.endpoint,
                expected=self._expected_session,
            )
            if base_checkpoint.instrument_id != instrument_id:
                raise ValueError(
                    "base checkpoint belongs to another instrument"
                )
        with self._lock:
            self._require_open()
            if self._active is not None:
                raise L2FlowRealtimeError(
                    "history worker already has an active cursor"
                )
            request_id = _request_id()
            self._send(
                pack_control(
                    WorkerOpcode.OPEN,
                    request_id=request_id,
                    payload=pack_open_payload(
                        instrument_id=instrument_id,
                        requested_page_records=(
                            requested_page_records
                        ),
                        expected_generation=generation,
                        base_checkpoint=(
                            None
                            if base_checkpoint is None
                            else base_checkpoint.to_wire()
                        ),
                    ),
                )
            )
            response = self._recv()
            if response.opcode is WorkerOpcode.ERROR:
                self._fail()
                _raise_worker_error(response)
            if (
                response.opcode is not WorkerOpcode.OPENED
                or response.status is not WorkerStatus.OK
                or response.request_id != request_id
                or response.transfer_sequence
                or response.slot_index != NO_SLOT
                or response.record_count
                or any(response.payload)
            ):
                raise ProtocolError(
                    "history worker OPENED response is noncanonical"
                )
            (
                target_generation,
                expected_record_count,
                target_tick_record_count,
                history_published_ns,
            ) = response.args
            base_tick_count = (
                0
                if base_checkpoint is None
                else base_checkpoint.instrument_tick_record_count
            )
            if (
                target_generation == 0
                or (
                    generation != 0
                    and target_generation != generation
                )
                or target_tick_record_count < base_tick_count
                or target_tick_record_count - base_tick_count
                != expected_record_count
                or history_published_ns == 0
            ):
                raise WireFormatError(
                    "history worker OPENED metadata is inconsistent"
                )
            cursor = InstrumentTickDeltaResultCursor(
                worker=self,
                request_id=request_id,
                instrument_id=instrument_id,
                base_checkpoint=base_checkpoint,
                generation=target_generation,
                expected_record_count=expected_record_count,
                target_tick_record_count=target_tick_record_count,
                history_published_monotonic_ns=history_published_ns,
            )
            self._active = cursor
            return cursor

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            active = self._active
        first_error: Optional[BaseException] = None
        if active is not None:
            try:
                active.close()
            except BaseException as error:
                first_error = error
        if not self._failed and self._process.poll() is None:
            request_id = _request_id()
            try:
                self._send(
                    pack_control(
                        WorkerOpcode.STOP,
                        request_id=request_id,
                    )
                )
                response = self._recv()
                if (
                    response.opcode is not WorkerOpcode.STOPPED
                    or response.status is not WorkerStatus.OK
                    or response.request_id != request_id
                    or response.transfer_sequence
                    or response.slot_index != NO_SLOT
                    or response.record_count
                    or any(response.args)
                    or any(response.payload)
                ):
                    raise ProtocolError(
                        "history worker STOPPED is noncanonical"
                    )
            except BaseException as error:
                first_error = first_error or error
                self._failed = True
        self._closed = True
        try:
            self._channel.close()
        except BaseException as error:
            first_error = first_error or error
        try:
            self._mapping.close()
        except BaseException as error:
            first_error = first_error or error
        try:
            self._process.wait(timeout=1.0)
        except subprocess.TimeoutExpired:
            self._process.terminate()
            try:
                self._process.wait(timeout=1.0)
            except subprocess.TimeoutExpired:
                self._process.kill()
                self._process.wait(timeout=1.0)
        if first_error is not None:
            raise first_error

    def __enter__(self) -> "InstrumentTickDeltaWorker":
        self._require_open()
        return self

    def __exit__(self, _type, _value, _traceback) -> None:
        self.close()


def _start_instrument_tick_delta_worker(
    control_socket_path,
    *,
    expected_session: DailyCatalogSessionIdentity,
    result_columns: Sequence[str] = DEFAULT_RESULT_COLUMNS,
    ring_slots: int = 4,
    result_batch_records: int = 4096,
    timeout: Optional[float] = 1.0,
) -> InstrumentTickDeltaWorker:
    path = validate_socket_path(control_socket_path)
    if not isinstance(
        expected_session, DailyCatalogSessionIdentity
    ):
        raise TypeError(
            "expected_session must be DailyCatalogSessionIdentity"
        )
    timeout = validate_timeout(timeout)
    mask = column_mask(result_columns)
    layout = make_ring_layout(ring_slots, result_batch_records)
    ring_fd = -1
    read_fd = -1
    mapping: Optional[mmap.mmap] = None
    parent_channel: Optional[socket.socket] = None
    child_channel: Optional[socket.socket] = None
    process: Optional[subprocess.Popen] = None
    try:
        ring_fd = os.memfd_create(
            "l2flow-history-result-v2",
            getattr(os, "MFD_CLOEXEC", 0x0001)
            | getattr(os, "MFD_ALLOW_SEALING", 0x0002),
        )
        os.ftruncate(ring_fd, layout.total_bytes)
        header = pack_ring_header(layout, mask)
        written = os.pwrite(ring_fd, header, 0)
        if written != len(header):
            raise OSError("short history worker ring header write")
        seals = (
            getattr(fcntl, "F_SEAL_GROW", 0x0004)
            | getattr(fcntl, "F_SEAL_SHRINK", 0x0002)
            | getattr(fcntl, "F_SEAL_SEAL", 0x0001)
        )
        fcntl.fcntl(
            ring_fd, getattr(fcntl, "F_ADD_SEALS", 1033), seals
        )
        parent_channel, child_channel = socket.socketpair(
            socket.AF_UNIX,
            socket.SOCK_SEQPACKET
            | getattr(socket, "SOCK_CLOEXEC", 0),
        )
        parent_channel.settimeout(timeout)
        child_fd = child_channel.fileno()
        environment = os.environ.copy()
        package_root = os.path.dirname(
            os.path.dirname(os.path.abspath(__file__))
        )
        inherited_python_path = environment.get("PYTHONPATH")
        environment["PYTHONPATH"] = (
            package_root
            if not inherited_python_path
            else package_root
            + os.pathsep
            + inherited_python_path
        )
        process = subprocess.Popen(
            (
                sys.executable,
                "-m",
                "l2flow_realtime._history_worker_process",
                str(child_fd),
                str(ring_fd),
            ),
            close_fds=True,
            pass_fds=(child_fd, ring_fd),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=environment,
        )
        child_channel.close()
        child_channel = None
        read_fd = os.open(
            f"/proc/self/fd/{ring_fd}",
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0),
        )
        mapping = mmap.mmap(
            read_fd, layout.total_bytes, access=mmap.ACCESS_READ
        )
        checked_layout, checked_mask = validate_ring_header(mapping)
        if checked_layout != layout or checked_mask != mask:
            raise WireFormatError(
                "created history worker ring changed before startup"
            )
        request_id = _request_id()
        send_control(
            parent_channel,
            pack_control(
                WorkerOpcode.INIT,
                request_id=request_id,
                payload=pack_init_payload(
                    expected_session=expected_session,
                    timeout_ns=_timeout_ns(timeout),
                    result_column_mask=mask,
                    control_socket_path=path,
                ),
            ),
        )
        response = recv_control(parent_channel)
        if response.opcode is WorkerOpcode.ERROR:
            _raise_worker_error(response)
        if (
            response.opcode is not WorkerOpcode.READY
            or response.status is not WorkerStatus.OK
            or response.request_id != request_id
            or response.transfer_sequence
            or response.slot_index != NO_SLOT
            or response.record_count
            or any(response.payload)
            or response.args
            != (
                process.pid,
                layout.slot_count,
                layout.batch_capacity,
                layout.total_bytes,
            )
        ):
            raise ProtocolError(
                "history worker READY response is noncanonical"
            )
        worker = InstrumentTickDeltaWorker(
            channel=parent_channel,
            mapping=mapping,
            layout=layout,
            result_column_mask=mask,
            process=process,
            pid=process.pid,
            expected_session=expected_session,
            timeout=timeout,
        )
        parent_channel = None
        mapping = None
        process = None
        return worker
    finally:
        if child_channel is not None:
            child_channel.close()
        if parent_channel is not None:
            parent_channel.close()
        if mapping is not None:
            mapping.close()
        if read_fd >= 0:
            os.close(read_fd)
        if ring_fd >= 0:
            os.close(ring_fd)
        if process is not None:
            process.terminate()
            try:
                process.wait(timeout=1.0)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=1.0)


__all__ = [
    "HistoryWorkerClosedError",
    "HistoryWorkerError",
    "HistoryWorkerInternalError",
    "InstrumentTickDeltaResultBatch",
    "InstrumentTickDeltaResultColumns",
    "InstrumentTickDeltaResultCursor",
    "InstrumentTickDeltaWorker",
]
