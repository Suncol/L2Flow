#!/usr/bin/env python3
"""Process-boundary tests for the fixed Wire V2 history result ring."""

from __future__ import annotations

import array
import mmap
import os
import select
import socket
import struct
import sys
import tempfile
import threading
import unittest
from pathlib import Path


REPOSITORY = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPOSITORY / "python"))

from l2flow_realtime import (  # noqa: E402
    DeltaCheckpointUnverifiedError,
    HistoryWorkerClosedError,
    InstrumentRawEventBatch,
    InstrumentRawEventHistoryReader,
    InstrumentRawEventLookupError,
    InstrumentKey,
    InstrumentLookupResult,
    InstrumentLookupStatus,
    InstrumentTickDeltaResultBatch,
    L2FlowClient,
    ProtocolError,
    SessionIdentity,
    WireFormatError,
)
from l2flow_realtime._history_worker_protocol import (  # noqa: E402
    CONTROL_PACKET_BYTES,
    SLOT_FLAG_DATA,
    WorkerOpcode,
    column_mask,
    column_region,
    make_ring_layout,
    pack_control,
    pack_ring_header,
    parse_control,
    parse_slot_header,
    publish_slot,
    recv_control,
    validate_ring_header,
)
from l2flow_realtime.history_worker import (  # noqa: E402
    _start_instrument_tick_delta_worker,
)
from l2flow_realtime.instrument_delta import (  # noqa: E402
    DELTA_SELECTED_SOURCE_MASK,
    DELTA_PAGE_ENDIAN_MARKER,
    DELTA_PAGE_HEADER_BYTES,
    DELTA_PAGE_MAGIC,
    _METADATA_TAIL,
    _OPEN_SESSION_REQUEST,
    _PAGE_PREFIX,
    _READ_REQUEST,
    _READ_RESPONSE,
)
from l2flow_realtime.wire import (  # noqa: E402
    CONTROL_MAGIC,
    TICK_BYTES,
    WIRE_MAJOR,
    WIRE_MINOR,
)
from test_l2flow_history_v2 import (  # noqa: E402
    CAPACITY,
    RUN_ID,
    SESSION_EPOCH,
    TICKS,
    TRADE_DATE,
    _delta_metadata,
    _delta_server,
    _endpoint,
    _send_with_fd,
    _target_checkpoint,
)
from l2flow_realtime._generation import (  # noqa: E402
    pack_generation_endpoint,
)


class _UnixDeltaServer:
    def __init__(self, handler, *, connections: int = 1) -> None:
        self._temporary = tempfile.TemporaryDirectory(
            prefix="l2flow-worker-"
        )
        self.path = os.path.join(
            self._temporary.name, "delta.sock"
        )
        self._listener = socket.socket(
            socket.AF_UNIX, socket.SOCK_SEQPACKET
        )
        self._listener.bind(self.path)
        self._listener.listen(1)
        self._handler = handler
        self._connections = connections
        self._thread = threading.Thread(
            target=self._run, name="worker-delta-test-server"
        )
        self.error = None
        self.peer_pid = None

    def _run(self) -> None:
        try:
            for _index in range(self._connections):
                channel, _address = self._listener.accept()
                credentials = channel.getsockopt(
                    socket.SOL_SOCKET, socket.SO_PEERCRED, 12
                )
                self.peer_pid = struct.unpack(
                    "3i", credentials
                )[0]
                with channel:
                    self._handler(channel)
        except BaseException as error:
            self.error = error

    def __enter__(self) -> "_UnixDeltaServer":
        self._thread.start()
        return self

    def __exit__(self, _type, _value, _traceback) -> None:
        self._listener.close()
        self._thread.join(3)
        try:
            if self._thread.is_alive():
                raise RuntimeError(
                    "worker delta test server did not stop"
                )
            if self.error is not None:
                raise self.error
        finally:
            self._temporary.cleanup()


def _split_delta_page(
    page_index: int, tick: bytes
) -> bytes:
    (
        _schema,
        _record_bytes,
        _instrument,
        _ordinal,
        _source_sequence,
        ingress_sequence,
        tick_stream_sequence,
    ) = struct.unpack_from("<IIIIQQQ", tick)
    total_bytes = DELTA_PAGE_HEADER_BYTES + len(tick)
    header = bytearray(DELTA_PAGE_HEADER_BYTES)
    _PAGE_PREFIX.pack_into(
        header,
        0,
        DELTA_PAGE_MAGIC,
        WIRE_MAJOR,
        WIRE_MINOR,
        DELTA_PAGE_HEADER_BYTES,
        DELTA_PAGE_ENDIAN_MARKER,
        0,
        total_bytes,
        page_index,
        1,
        TICK_BYTES,
        DELTA_PAGE_HEADER_BYTES,
        ingress_sequence,
        ingress_sequence,
        tick_stream_sequence,
        tick_stream_sequence,
    )
    header[88:808] = _delta_metadata()
    return bytes(header) + tick


def _split_delta_server(channel: socket.socket) -> None:
    request = channel.recv(_OPEN_SESSION_REQUEST.size)
    if not request:
        return
    request_id = struct.unpack_from("<Q", request, 24)[0]
    channel.send(
        struct.pack(
            "<8sHHHHIIQ",
            CONTROL_MAGIC,
            WIRE_MAJOR,
            WIRE_MINOR,
            0,
            0,
            288,
            0,
            request_id,
        )
        + pack_generation_endpoint(_endpoint())
        + struct.pack("<Q", 201)
    )
    request = channel.recv(376)
    if not request:
        return
    request_id = struct.unpack_from("<Q", request, 24)[0]
    channel.send(
        struct.pack(
            "<8sHHHHIIQQ",
            CONTROL_MAGIC,
            WIRE_MAJOR,
            WIRE_MINOR,
            0,
            0,
            760,
            0,
            request_id,
            202,
        )
        + _delta_metadata()
    )
    for page_index, tick in enumerate(TICKS):
        request = channel.recv(_READ_REQUEST.size)
        if not request:
            return
        request_id = struct.unpack_from("<Q", request, 24)[0]
        payload = _split_delta_page(page_index, tick)
        _send_with_fd(
            channel,
            _READ_RESPONSE.pack(
                CONTROL_MAGIC,
                WIRE_MAJOR,
                WIRE_MINOR,
                0,
                0,
                _READ_RESPONSE.size,
                1,
                request_id,
                len(payload),
                page_index,
                _endpoint().generation,
                203 + page_index,
            ),
            payload,
        )
    request = channel.recv(_READ_REQUEST.size)
    if not request:
        return
    request_id = struct.unpack_from("<Q", request, 24)[0]
    channel.send(
        _READ_RESPONSE.pack(
            CONTROL_MAGIC,
            WIRE_MAJOR,
            WIRE_MINOR,
            0,
            1,
            _READ_RESPONSE.size,
            0,
            request_id,
            0,
            len(TICKS),
            _endpoint().generation,
            0,
        )
    )


def _empty_successor_delta_server(channel: socket.socket) -> None:
    """Serve an empty finite delta based on the full-read checkpoint."""

    request = channel.recv(_OPEN_SESSION_REQUEST.size)
    if not request:
        return
    request_id = struct.unpack_from("<Q", request, 24)[0]
    channel.send(
        struct.pack(
            "<8sHHHHIIQ",
            CONTROL_MAGIC,
            WIRE_MAJOR,
            WIRE_MINOR,
            0,
            0,
            288,
            0,
            request_id,
        )
        + pack_generation_endpoint(_endpoint())
        + struct.pack("<Q", 301)
    )
    request = channel.recv(376)
    if not request:
        return
    if len(request) != 376:
        raise RuntimeError("short successor instrument OPEN")
    if struct.unpack_from("<I", request, 40)[0] != 2:
        raise RuntimeError("successor OPEN did not use checkpoint base")
    checkpoint = _target_checkpoint()
    if request[56:368] != checkpoint.to_wire():
        raise RuntimeError("successor OPEN changed its base checkpoint")
    metadata = (
        struct.pack("<II", 2, DELTA_SELECTED_SOURCE_MASK)
        + checkpoint.to_wire()
        + checkpoint.to_wire()
        + _METADATA_TAIL.pack(
            0,
            0,
            0,
            0,
            0,
            checkpoint.ingress_sequence_exclusive,
            checkpoint.ingress_sequence_exclusive,
            checkpoint.tick_stream_sequence_exclusive,
            checkpoint.tick_stream_sequence_exclusive,
            1,
            1,
            b"\x00" * 8,
        )
    )
    request_id = struct.unpack_from("<Q", request, 24)[0]
    channel.send(
        struct.pack(
            "<8sHHHHIIQQ",
            CONTROL_MAGIC,
            WIRE_MAJOR,
            WIRE_MINOR,
            0,
            0,
            760,
            0,
            request_id,
            302,
        )
        + metadata
    )
    request = channel.recv(_READ_REQUEST.size)
    if not request:
        return
    request_id = struct.unpack_from("<Q", request, 24)[0]
    channel.send(
        _READ_RESPONSE.pack(
            CONTROL_MAGIC,
            WIRE_MAJOR,
            WIRE_MINOR,
            0,
            1,
            _READ_RESPONSE.size,
            0,
            request_id,
            0,
            0,
            _endpoint().generation,
            0,
        )
    )


class FixedWorkerProtocolTests(unittest.TestCase):
    def test_control_packets_are_fixed_and_reject_file_descriptors(self):
        packet = pack_control(
            WorkerOpcode.RESULT_READY,
            request_id=7,
            transfer_sequence=3,
            slot_index=0,
            record_count=11,
        )
        self.assertEqual(len(packet), CONTROL_PACKET_BYTES)
        parsed = parse_control(packet)
        self.assertIs(parsed.opcode, WorkerOpcode.RESULT_READY)
        self.assertEqual(parsed.request_id, 7)
        self.assertEqual(parsed.transfer_sequence, 3)
        self.assertEqual(parsed.record_count, 11)

        left, right = socket.socketpair(
            socket.AF_UNIX, socket.SOCK_SEQPACKET
        )
        descriptor = os.memfd_create("unexpected-worker-fd")
        try:
            rights = array.array("i", [descriptor])
            left.sendmsg(
                [packet],
                [(socket.SOL_SOCKET, socket.SCM_RIGHTS, rights)],
            )
            with self.assertRaises(ProtocolError):
                recv_control(right)
        finally:
            os.close(descriptor)
            left.close()
            right.close()

    def test_fixed_slot_schema_detects_stale_transfer_sequence(self):
        layout = make_ring_layout(2, 4)
        mask = column_mask(("ingress_sequence", "price_p6"))
        descriptor = os.memfd_create("worker-ring-test")
        os.ftruncate(descriptor, layout.total_bytes)
        writer = mmap.mmap(
            descriptor, layout.total_bytes, access=mmap.ACCESS_WRITE
        )
        reader = mmap.mmap(
            descriptor, layout.total_bytes, access=mmap.ACCESS_READ
        )
        try:
            writer[:4096] = pack_ring_header(layout, mask)
            self.assertEqual(
                validate_ring_header(reader), (layout, mask)
            )
            publish_slot(
                writer,
                layout,
                slot_index=0,
                flags=SLOT_FLAG_DATA,
                request_id=17,
                transfer_sequence=1,
                page_index=0,
                generation=9,
                record_count=2,
                cumulative_record_count=2,
                first_ingress_sequence=1,
                last_ingress_sequence=2,
                first_tick_stream_sequence=1,
                last_tick_stream_sequence=2,
                cumulative_source_record_counts=(0, 2, 0, 0),
                worker_read_start_ns=1,
                worker_read_return_ns=2,
                worker_publish_begin_ns=3,
                result_column_mask=mask,
                columns={
                    "ingress_sequence": (1, 2),
                    "price_p6": (101_000_000, 102_000_000),
                },
            )
            header = parse_slot_header(
                reader,
                layout,
                expected_slot_index=0,
                expected_request_id=17,
                expected_transfer_sequence=1,
                expected_record_count=2,
                configured_column_mask=mask,
            )
            self.assertEqual(header.cumulative_record_count, 2)
            begin, end, spec = column_region(
                layout, 0, "price_p6", 2
            )
            view = memoryview(reader)[begin:end].cast(spec.format)
            try:
                self.assertEqual(
                    tuple(view), (101_000_000, 102_000_000)
                )
            finally:
                view.release()
            with self.assertRaises(WireFormatError):
                parse_slot_header(
                    reader,
                    layout,
                    expected_slot_index=0,
                    expected_request_id=17,
                    expected_transfer_sequence=3,
                    expected_record_count=2,
                    configured_column_mask=mask,
                )
        finally:
            reader.close()
            writer.close()
            os.close(descriptor)


class IsolatedDeltaWorkerTests(unittest.TestCase):
    def _start(self, path, **kwargs):
        return _start_instrument_tick_delta_worker(
            path,
            session_identity=SessionIdentity(
                RUN_ID, SESSION_EPOCH
            ),
            trade_date=TRADE_DATE,
            capacity=CAPACITY,
            timeout=2.0,
            **kwargs,
        )

    def test_raw_delta_peer_is_the_worker_and_results_are_lazy(self):
        with _UnixDeltaServer(_delta_server) as server:
            with self._start(
                server.path,
                result_columns=(
                    "ingress_sequence",
                    "price_p6",
                    "price_valid",
                ),
                ring_slots=2,
                result_batch_records=2,
            ) as worker:
                self.assertNotEqual(worker.pid, os.getpid())
                with worker.open_instrument(
                    1, expected_generation=9
                ) as cursor:
                    with self.assertRaises(
                        DeltaCheckpointUnverifiedError
                    ):
                        _ = cursor.verified_checkpoint
                    batch = cursor.read_batch()
                    self.assertIsInstance(
                        batch, InstrumentTickDeltaResultBatch
                    )
                    assert batch is not None
                    self.assertFalse(hasattr(batch, "wire_records"))
                    self.assertEqual(
                        batch.columns.materialized_column_count, 0
                    )
                    self.assertEqual(
                        batch.read_columns("price_p6")["price_p6"],
                        (101_000_000, 102_000_000),
                    )
                    self.assertEqual(
                        batch.columns.materialized_column_count, 1
                    )
                    with batch.borrow_column(
                        "ingress_sequence"
                    ) as ingress:
                        self.assertEqual(tuple(ingress), (1, 2))
                        self.assertEqual(ingress[:1], (1,))
                        with self.assertRaises(TypeError):
                            memoryview(ingress)
                        expired = ingress
                    with self.assertRaises(HistoryWorkerClosedError):
                        len(expired)
                    batch.close()
                    self.assertIsNone(cursor.read_batch())
                    checkpoint = cursor.verified_checkpoint
                    self.assertEqual(
                        checkpoint, _target_checkpoint()
                    )
                    self.assertEqual(
                        cursor.cumulative_record_count, 2
                    )
        self.assertEqual(server.peer_pid, worker.pid)

    def test_one_slot_backpressures_until_exact_release(self):
        with _UnixDeltaServer(_split_delta_server) as server:
            with self._start(
                server.path,
                result_columns=("price_p6",),
                ring_slots=1,
                result_batch_records=1,
            ) as worker:
                with worker.open_instrument(
                    1, expected_generation=9
                ) as cursor:
                    first = cursor.read_batch()
                    assert first is not None
                    self.assertEqual(
                        first.read_columns("price_p6")["price_p6"],
                        (101_000_000,),
                    )
                    # The worker may already have read the next raw page, but
                    # it cannot overwrite or announce slot 0 before RELEASE.
                    readable, _writable, _errors = select.select(
                        (worker._channel,), (), (), 0.05
                    )
                    self.assertEqual(readable, [])
                    first.close()
                    second = cursor.read_batch()
                    assert second is not None
                    self.assertEqual(second.slot_index, 0)
                    self.assertEqual(
                        second.transfer_sequence,
                        first.transfer_sequence + 1,
                    )
                    self.assertEqual(
                        second.read_columns("price_p6")["price_p6"],
                        (102_000_000,),
                    )
                    second.close()
                    self.assertIsNone(cursor.read_batch())
                    self.assertEqual(
                        cursor.verified_checkpoint,
                        _target_checkpoint(),
                    )

    def test_pages_release_slots_and_verify_empty_delta_boundary(self):
        with _UnixDeltaServer(_delta_server) as server:
            with self._start(
                server.path,
                result_columns=("price_p6",),
                ring_slots=2,
                result_batch_records=2,
            ) as worker:
                with worker.open_instrument(
                    1, expected_generation=9
                ) as cursor:
                    values = []
                    for batch in cursor.batches():
                        with batch.borrow_column(
                            "price_p6"
                        ) as column:
                            values.extend(column)
                    self.assertEqual(
                        values, [101_000_000, 102_000_000]
                    )
                    self.assertTrue(cursor.done)
                    self.assertEqual(
                        cursor.verified_checkpoint.generation, 9
                    )

    def test_public_raw_event_history_reuses_worker_for_full_and_updates(self):
        handlers = iter(
            (_delta_server, _empty_successor_delta_server)
        )

        def serve_next(channel):
            next(handlers)(channel)

        key = InstrumentKey(1, b"XSHG", b"600010")
        unknown_key = InstrumentKey(1, b"XSHG", b"600011")

        class KeyResolver:
            def __init__(self, worker):
                self.worker = worker
                self.worker_open_args = None

            def resolve_key(self, requested):
                if requested == unknown_key:
                    return InstrumentLookupResult(
                        SESSION_EPOCH,
                        requested,
                        InstrumentLookupStatus.UNKNOWN,
                        0,
                    )
                if requested != key:
                    raise AssertionError("unexpected instrument key")
                return InstrumentLookupResult(
                    SESSION_EPOCH,
                    requested,
                    InstrumentLookupStatus.FOUND,
                    1,
                )

            def open_instrument_tick_delta_worker(self, **kwargs):
                self.worker_open_args = kwargs
                return self.worker

        with _UnixDeltaServer(
            serve_next, connections=2
        ) as server:
            worker = self._start(
                server.path,
                result_columns=(
                    "ingress_sequence",
                    "action",
                    "primary_order_id",
                    "price_p6",
                ),
                ring_slots=2,
                result_batch_records=2,
            )
            resolver = KeyResolver(worker)
            with L2FlowClient.open_instrument_raw_event_history(
                resolver,
                raw_event_columns=(
                    "ingress_sequence",
                    "action",
                    "primary_order_id",
                    "price_p6",
                ),
                ring_slots=2,
                batch_capacity=2,
            ) as history:
                self.assertIsInstance(
                    history, InstrumentRawEventHistoryReader
                )
                self.assertEqual(
                    resolver.worker_open_args,
                    {
                        "result_columns": (
                            "ingress_sequence",
                            "action",
                            "primary_order_id",
                            "price_p6",
                        ),
                        "ring_slots": 2,
                        "result_batch_records": 2,
                    },
                )
                self.assertEqual(
                    history.raw_event_columns,
                    (
                        "ingress_sequence",
                        "action",
                        "primary_order_id",
                        "price_p6",
                    ),
                )
                with self.assertRaises(
                    InstrumentRawEventLookupError
                ) as lookup_error:
                    history.read_all(unknown_key)
                self.assertIs(
                    lookup_error.exception.status,
                    InstrumentLookupStatus.UNKNOWN,
                )
                worker_pid = history.worker_pid
                with history.read_all(
                    key, expected_generation=9
                ) as full:
                    self.assertTrue(full.is_full_read)
                    self.assertEqual(full.worker_pid, worker_pid)
                    with self.assertRaises(
                        DeltaCheckpointUnverifiedError
                    ):
                        _ = full.verified_checkpoint
                    seen_ingress = []
                    batches = []
                    for batch in full.batches():
                        batches.append(batch)
                        self.assertIsInstance(
                            batch, InstrumentRawEventBatch
                        )
                        seen_ingress.extend(
                            batch.read_columns(
                                "ingress_sequence"
                            )["ingress_sequence"]
                        )
                    self.assertEqual(seen_ingress, [1, 2])
                    self.assertEqual(len(batches), 1)
                    self.assertTrue(batches[0].closed)
                    checkpoint = full.verified_checkpoint
                    self.assertEqual(
                        full.cumulative_record_count, 2
                    )

                with history.read_updates(
                    1,
                    checkpoint,
                    expected_generation=9,
                ) as updates:
                    self.assertFalse(updates.is_full_read)
                    self.assertEqual(updates.worker_pid, worker_pid)
                    self.assertIsNone(updates.read_batch())
                    self.assertTrue(updates.eof)
                    self.assertEqual(
                        updates.expected_record_count, 0
                    )
                    self.assertEqual(
                        updates.verified_checkpoint,
                        checkpoint,
                    )
                self.assertEqual(history.worker_pid, worker_pid)
        self.assertEqual(server.peer_pid, worker_pid)

    def test_early_cursor_close_cancels_once_and_worker_is_reusable(self):
        handlers = iter((_split_delta_server, _delta_server))

        def serve_next(channel):
            next(handlers)(channel)

        with _UnixDeltaServer(
            serve_next, connections=2
        ) as server:
            worker = self._start(
                server.path,
                result_columns=("ingress_sequence",),
                ring_slots=1,
                result_batch_records=2,
            )

            class NumericOnlyClient:
                pass

            with InstrumentRawEventHistoryReader(
                NumericOnlyClient(), worker
            ) as history:
                worker_pid = history.worker_pid
                canceled = history.read_all(
                    1, expected_generation=9
                )
                canceled.close()
                self.assertTrue(canceled.closed)
                self.assertFalse(history.closed)
                self.assertFalse(history.failed)

                with history.read_all(
                    1, expected_generation=9
                ) as retry:
                    ingress = []
                    for batch in retry.batches():
                        ingress.extend(
                            batch.read_columns(
                                "ingress_sequence"
                            )["ingress_sequence"]
                        )
                    self.assertEqual(ingress, [1, 2])
                    self.assertEqual(
                        retry.verified_checkpoint,
                        _target_checkpoint(),
                    )
                    self.assertEqual(retry.worker_pid, worker_pid)
        self.assertEqual(server.peer_pid, worker_pid)


if __name__ == "__main__":
    unittest.main()
