#!/usr/bin/env python3
"""State-machine tests for Wire V2 history and tick-delta Python cursors."""

from __future__ import annotations

import array
import fcntl
import os
import socket
import struct
import sys
import threading
import time
import unittest
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


REPOSITORY = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPOSITORY))
sys.path.insert(0, str(REPOSITORY / "python"))

from l2flow_realtime import (  # noqa: E402
    CatalogScope,
    ClientClosedError,
    DeltaCheckpointUnverifiedError,
    HistoryCursorClosedError,
    HistoryGeneration,
    HistoryGenerationChangedError,
    HistoryPage,
    InstrumentTickDeltaBaseKind,
    InstrumentTickDeltaCursorClosedError,
    InstrumentTickDeltaGeneration,
    InstrumentTickDeltaPage,
    InstrumentTickDeltaSessionClosedError,
    L2FlowClient,
    ProtocolError,
    StreamNotFoundError,
    StreamInternalFailureError,
    UnavailableError,
)
from l2flow_realtime._generation import (  # noqa: E402
    ENDPOINT_FLAG_COVERAGE_FROM_OPEN,
    ENDPOINT_FLAG_RECORD_COVERAGE_COMPLETE,
    GenerationEndpoint,
    pack_generation_endpoint,
    validate_generation_endpoint,
)
from l2flow_realtime.checkpoint import (  # noqa: E402
    InstrumentTickDeltaCheckpoint,
)
from l2flow_realtime.history import (  # noqa: E402
    HISTORY_PAGE_ENDIAN_MARKER,
    HISTORY_PAGE_HEADER_BYTES,
    HISTORY_PAGE_MAGIC,
    _DESCRIPTOR,
    _HISTORY_LOCAL,
    _OPEN_REQUEST,
    _PAGE_PREFIX,
    _READ_REQUEST,
    _READ_RESPONSE,
    _open_instrument_history,
)
from l2flow_realtime.instrument_delta import (  # noqa: E402
    DELTA_PAGE_ENDIAN_MARKER,
    DELTA_PAGE_HEADER_BYTES,
    DELTA_PAGE_MAGIC,
    DELTA_SELECTED_SOURCE_MASK,
    _METADATA_TAIL,
    _OPEN_SESSION_REQUEST,
    _PAGE_PREFIX as _DELTA_PAGE_PREFIX,
    _READ_REQUEST as _DELTA_READ_REQUEST,
    _READ_RESPONSE as _DELTA_READ_RESPONSE,
    _open_instrument_tick_delta_session,
)
from l2flow_realtime.wire import (  # noqa: E402
    CONTROL_MAGIC,
    SNAPSHOT_BYTES,
    TICK_BYTES,
    WIRE_MAJOR,
    WIRE_MINOR,
)
from l2flow_realtime.models import WireFormatError  # noqa: E402


RUN_ID = b"R" * 16
CATALOG_DIGEST = b"C" * 32
INPUT_DIGEST = b"I" * 32
SESSION_EPOCH = 17
TRADE_DATE = 20260729
CAPACITY = 65_536
SOURCE_IDS = (11, 12, 13, 14)
_COMMON = struct.Struct("<IIIIQQQQqqqQQQIIII6B10x")
_DECIMAL = struct.Struct("<qqBBB5x")


def _assert_endpoint_properties(
    case: unittest.TestCase,
    generation: HistoryGeneration | InstrumentTickDeltaGeneration,
) -> None:
    endpoint = _endpoint()
    case.assertEqual(generation.endpoint, endpoint)
    case.assertEqual(generation.session_identity, endpoint.session_identity)
    for name in (
        "run_id",
        "session_epoch",
        "generation",
        "catalog_generation",
        "data_state_generation",
        "ingress_sequence_exclusive",
        "tick_stream_sequence_exclusive",
        "recv_monotonic_cut_ns",
        "history_published_monotonic_ns",
        "accepted_sequence",
        "durable_sequence",
        "applied_sequence",
        "catalog_digest",
        "input_identity_sha256",
        "source_stream_ids",
        "source_sequence_exclusive",
        "trade_date",
        "capacity",
        "bound_count",
        "available_count",
        "snapshot_available_count",
        "tick_available_count",
        "factor_eligible_count",
        "catalog_scope",
        "coverage_complete",
        "coverage_from_open",
        "record_coverage_complete",
    ):
        with case.subTest(generation=type(generation).__name__, property=name):
            case.assertEqual(getattr(generation, name), getattr(endpoint, name))
    case.assertEqual(
        generation.processing_lag_records,
        endpoint.accepted_sequence - endpoint.applied_sequence,
    )
    case.assertEqual(
        generation.durability_lag_records,
        endpoint.accepted_sequence - endpoint.durable_sequence,
    )


def _endpoint() -> GenerationEndpoint:
    return GenerationEndpoint(
        run_id=RUN_ID,
        session_epoch=SESSION_EPOCH,
        generation=9,
        catalog_generation=1,
        data_state_generation=9,
        ingress_sequence_exclusive=3,
        tick_stream_sequence_exclusive=3,
        recv_monotonic_cut_ns=10_000,
        history_published_monotonic_ns=11_000,
        accepted_sequence=2,
        durable_sequence=0,
        applied_sequence=2,
        catalog_digest=CATALOG_DIGEST,
        input_identity_sha256=INPUT_DIGEST,
        source_stream_ids=SOURCE_IDS,
        source_sequence_exclusive=(1, 3, 1, 1),
        trade_date=TRADE_DATE,
        capacity=CAPACITY,
        bound_count=1,
        available_count=1,
        snapshot_available_count=0,
        tick_available_count=1,
        factor_eligible_count=0,
        catalog_scope=CatalogScope.OBSERVED_ONLY,
        coverage_complete=False,
        flags=(
            ENDPOINT_FLAG_COVERAGE_FROM_OPEN
            | ENDPOINT_FLAG_RECORD_COVERAGE_COMPLETE
        ),
    )


def _tick(ingress: int, price_p6: int) -> bytes:
    payload = bytearray(TICK_BYTES)
    _COMMON.pack_into(
        payload,
        0,
        2,
        TICK_BYTES,
        1,
        0,
        ingress,
        ingress,
        ingress,
        0,
        1_000,
        2_000,
        3_000 + ingress,
        0,
        0,
        0,
        SOURCE_IDS[1],
        TRADE_DATE,
        0,
        0,
        1,
        2,
        1,
        0,
        0,
        0,
    )
    struct.pack_into("<I", payload, 132, 0)
    _DECIMAL.pack_into(
        payload, 168, price_p6, price_p6, 6, 1, 0
    )
    return bytes(payload)


TICKS = (_tick(1, 101_000_000), _tick(2, 102_000_000))


def _history_generation_bytes() -> bytes:
    return pack_generation_endpoint(_endpoint()) + _HISTORY_LOCAL.pack(
        1,
        0,
        0,
        2,
        0,
        0,
        2,
        0,
        2,
        1,
        0,
        b"\x00" * 8,
    )


def _history_page() -> bytes:
    descriptors = b"".join(
        _DESCRIPTOR.pack(
            index,
            index,
            index,
            index - 1,
            0,
            2,
            2,
            1,
            0,
            0,
        )
        for index in (1, 2)
    )
    tick_block = b"".join(TICKS)
    descriptors_offset = HISTORY_PAGE_HEADER_BYTES
    snapshots_offset = descriptors_offset + len(descriptors)
    ticks_offset = snapshots_offset
    total_bytes = ticks_offset + len(tick_block)
    header = bytearray(HISTORY_PAGE_HEADER_BYTES)
    _PAGE_PREFIX.pack_into(
        header,
        0,
        HISTORY_PAGE_MAGIC,
        WIRE_MAJOR,
        WIRE_MINOR,
        HISTORY_PAGE_HEADER_BYTES,
        HISTORY_PAGE_ENDIAN_MARKER,
        0,
        total_bytes,
        0,
        2,
        _DESCRIPTOR.size,
        descriptors_offset,
        snapshots_offset,
        0,
        SNAPSHOT_BYTES,
        ticks_offset,
        2,
        TICK_BYTES,
        1,
        2,
    )
    header[104:440] = _history_generation_bytes()
    return bytes(header) + descriptors + tick_block


def _target_checkpoint() -> InstrumentTickDeltaCheckpoint:
    return InstrumentTickDeltaCheckpoint.from_endpoint(
        _endpoint(),
        instrument_id=1,
        ordinal=0,
        instrument_tick_source_record_counts=(0, 2, 0, 0),
        payload_projection=1,
        tick_record_coverage_complete=True,
    )


def _delta_metadata() -> bytes:
    return (
        struct.pack(
            "<II", 1, DELTA_SELECTED_SOURCE_MASK
        )
        + b"\x00" * 320
        + _target_checkpoint().to_wire()
        + _METADATA_TAIL.pack(
            0,
            2,
            0,
            0,
            2,
            1,
            3,
            1,
            3,
            1,
            1,
            b"\x00" * 8,
        )
    )


def _delta_page() -> bytes:
    metadata = _delta_metadata()
    tick_block = b"".join(TICKS)
    total_bytes = DELTA_PAGE_HEADER_BYTES + len(tick_block)
    header = bytearray(DELTA_PAGE_HEADER_BYTES)
    _DELTA_PAGE_PREFIX.pack_into(
        header,
        0,
        DELTA_PAGE_MAGIC,
        WIRE_MAJOR,
        WIRE_MINOR,
        DELTA_PAGE_HEADER_BYTES,
        DELTA_PAGE_ENDIAN_MARKER,
        0,
        total_bytes,
        0,
        2,
        TICK_BYTES,
        DELTA_PAGE_HEADER_BYTES,
        1,
        2,
        1,
        2,
    )
    header[88:824] = metadata
    return bytes(header) + tick_block


def _sealed_read_only_fd(payload: bytes) -> int:
    fd = os.memfd_create(
        "l2flow-test-page",
        getattr(os, "MFD_CLOEXEC", 0x0001)
        | getattr(os, "MFD_ALLOW_SEALING", 0x0002),
    )
    try:
        os.write(fd, payload)
        seals = (
            getattr(fcntl, "F_SEAL_WRITE", 0x0008)
            | getattr(fcntl, "F_SEAL_GROW", 0x0004)
            | getattr(fcntl, "F_SEAL_SHRINK", 0x0002)
            | getattr(fcntl, "F_SEAL_SEAL", 0x0001)
        )
        fcntl.fcntl(
            fd, getattr(fcntl, "F_ADD_SEALS", 1033), seals
        )
        return os.open(
            f"/proc/self/fd/{fd}",
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0),
        )
    finally:
        os.close(fd)


def _send_with_fd(
    channel: socket.socket, response: bytes, payload: bytes
) -> None:
    fd = _sealed_read_only_fd(payload)
    try:
        descriptors = array.array("i", [fd])
        sent = channel.sendmsg(
            [response],
            [(socket.SOL_SOCKET, socket.SCM_RIGHTS, descriptors)],
        )
        if sent != len(response):
            raise RuntimeError("test server short send")
    finally:
        os.close(fd)


class _ConnectedSocket:
    """Socket-pair endpoint whose AF_UNIX connect is intentionally a no-op."""

    def __init__(self, channel: socket.socket) -> None:
        self._channel = channel

    def connect(self, _path) -> None:
        return None

    def __getattr__(self, name):
        return getattr(self._channel, name)


class _SocketPairServer:
    def __init__(self, handler) -> None:
        client, server = socket.socketpair(
            socket.AF_UNIX, socket.SOCK_SEQPACKET
        )
        self.client = _ConnectedSocket(client)
        self._server = server
        self.path = "/tmp/l2flow-test-control.sock"
        self._handler = handler
        self._error = None
        self._thread = threading.Thread(
            target=self._run, name="history-v2-test-server"
        )

    def _run(self) -> None:
        try:
            with self._server:
                self._handler(self._server)
        except BaseException as error:
            self._error = error

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, _type, _value, _traceback) -> None:
        self.client._channel.close()
        self._thread.join(2)
        if self._thread.is_alive():
            raise RuntimeError("test server did not terminate")
        if self._error is not None:
            raise self._error


def _history_server(channel: socket.socket, block_first_read=None) -> None:
    request = channel.recv(_OPEN_REQUEST.size)
    if not request:
        return
    request_id = struct.unpack_from("<Q", request, 24)[0]
    response = struct.pack(
        "<8sHHHHIIQQ",
        CONTROL_MAGIC,
        WIRE_MAJOR,
        WIRE_MINOR,
        0,
        0,
        376,
        0,
        request_id,
        101,
    ) + _history_generation_bytes()
    channel.send(response)

    request = channel.recv(_READ_REQUEST.size)
    if not request:
        return
    if block_first_read is not None:
        block_first_read()
        return
    request_id = struct.unpack_from("<Q", request, 24)[0]
    payload = _history_page()
    response = _READ_RESPONSE.pack(
        CONTROL_MAGIC,
        WIRE_MAJOR,
        WIRE_MINOR,
        0,
        0,
        _READ_RESPONSE.size,
        2,
        request_id,
        len(payload),
        0,
        _endpoint().generation,
        102,
    )
    _send_with_fd(channel, response, payload)

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
            1,
            _endpoint().generation,
            0,
        )
    )


def _delta_server(channel: socket.socket, block_first_read=None) -> None:
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
            296,
            0,
            request_id,
        )
        + pack_generation_endpoint(_endpoint())
        + struct.pack("<Q", 201)
    )

    request = channel.recv(384)
    if not request:
        return
    if len(request) != 384:
        raise RuntimeError("short delta instrument OPEN")
    request_id = struct.unpack_from("<Q", request, 24)[0]
    channel.send(
        struct.pack(
            "<8sHHHHIIQQ",
            CONTROL_MAGIC,
            WIRE_MAJOR,
            WIRE_MINOR,
            0,
            0,
            776,
            0,
            request_id,
            202,
        )
        + _delta_metadata()
    )

    request = channel.recv(_DELTA_READ_REQUEST.size)
    if not request:
        return
    if block_first_read is not None:
        block_first_read()
        return
    request_id = struct.unpack_from("<Q", request, 24)[0]
    payload = _delta_page()
    _send_with_fd(
        channel,
        _DELTA_READ_RESPONSE.pack(
            CONTROL_MAGIC,
            WIRE_MAJOR,
            WIRE_MINOR,
            0,
            0,
            _DELTA_READ_RESPONSE.size,
            2,
            request_id,
            len(payload),
            0,
            _endpoint().generation,
            203,
        ),
        payload,
    )

    request = channel.recv(_DELTA_READ_REQUEST.size)
    if not request:
        return
    request_id = struct.unpack_from("<Q", request, 24)[0]
    channel.send(
        _DELTA_READ_RESPONSE.pack(
            CONTROL_MAGIC,
            WIRE_MAJOR,
            WIRE_MINOR,
            0,
            1,
            _DELTA_READ_RESPONSE.size,
            0,
            request_id,
            0,
            1,
            _endpoint().generation,
            0,
        )
    )


def _history_read_server(channel: socket.socket, responder) -> None:
    request = channel.recv(_OPEN_REQUEST.size)
    if not request:
        return
    open_id = struct.unpack_from("<Q", request, 24)[0]
    channel.send(
        struct.pack(
            "<8sHHHHIIQQ",
            CONTROL_MAGIC,
            WIRE_MAJOR,
            WIRE_MINOR,
            0,
            0,
            376,
            0,
            open_id,
            101,
        )
        + _history_generation_bytes()
    )
    request = channel.recv(_READ_REQUEST.size)
    if request:
        responder(channel, request)


def _delta_read_server(channel: socket.socket, responder) -> None:
    request = channel.recv(_OPEN_SESSION_REQUEST.size)
    if not request:
        return
    open_id = struct.unpack_from("<Q", request, 24)[0]
    channel.send(
        struct.pack(
            "<8sHHHHIIQ",
            CONTROL_MAGIC,
            WIRE_MAJOR,
            WIRE_MINOR,
            0,
            0,
            296,
            0,
            open_id,
        )
        + pack_generation_endpoint(_endpoint())
        + struct.pack("<Q", 201)
    )
    request = channel.recv(384)
    if not request:
        return
    instrument_open_id = struct.unpack_from("<Q", request, 24)[0]
    channel.send(
        struct.pack(
            "<8sHHHHIIQQ",
            CONTROL_MAGIC,
            WIRE_MAJOR,
            WIRE_MINOR,
            0,
            0,
            776,
            0,
            instrument_open_id,
            202,
        )
        + _delta_metadata()
    )
    request = channel.recv(_DELTA_READ_REQUEST.size)
    if request:
        responder(channel, request)


def _send_history_data_page(
    channel: socket.socket, request: bytes, payload: bytes
) -> None:
    read_id = struct.unpack_from("<Q", request, 24)[0]
    _send_with_fd(
        channel,
        _READ_RESPONSE.pack(
            CONTROL_MAGIC,
            WIRE_MAJOR,
            WIRE_MINOR,
            0,
            0,
            _READ_RESPONSE.size,
            2,
            read_id,
            len(payload),
            0,
            _endpoint().generation,
            102,
        ),
        payload,
    )


def _send_delta_data_page(
    channel: socket.socket, request: bytes, payload: bytes
) -> None:
    read_id = struct.unpack_from("<Q", request, 24)[0]
    _send_with_fd(
        channel,
        _DELTA_READ_RESPONSE.pack(
            CONTROL_MAGIC,
            WIRE_MAJOR,
            WIRE_MINOR,
            0,
            0,
            _DELTA_READ_RESPONSE.size,
            2,
            read_id,
            len(payload),
            0,
            _endpoint().generation,
            203,
        ),
        payload,
    )


class HistoryCursorTests(unittest.TestCase):
    def test_lazy_tick_column_and_rich_explicit_eof(self):
        with _SocketPairServer(_history_server) as server, mock.patch(
            "l2flow_realtime.history.socket.socket",
            return_value=server.client,
        ):
            with _open_instrument_history(
                server.path,
                instrument_id=1,
                requested_page_records=4096,
                expected_generation=9,
                expected_run_id=RUN_ID,
                expected_session_epoch=SESSION_EPOCH,
                expected_trade_date=TRADE_DATE,
                expected_capacity=CAPACITY,
                timeout=1.0,
            ) as cursor:
                self.assertIsInstance(cursor.generation, HistoryGeneration)
                _assert_endpoint_properties(self, cursor.generation)
                self.assertEqual(
                    cursor.session_identity,
                    cursor.generation.session_identity,
                )
                self.assertEqual(cursor.instrument_id, 1)
                self.assertEqual(cursor.ordinal, 0)
                self.assertEqual(cursor.next_page_index, 0)
                self.assertEqual(cursor.cumulative_record_count, 0)
                self.assertEqual(
                    cursor.cumulative_source_record_counts,
                    (0, 0, 0, 0),
                )
                self.assertFalse(cursor.closed)
                self.assertFalse(cursor.eof)
                self.assertFalse(cursor.done)
                page = cursor.read_page()
                self.assertIsNotNone(page)
                assert page is not None
                self.assertIsInstance(page, HistoryPage)
                self.assertEqual(page.generation, cursor.generation)
                self.assertFalse(page.eof)
                self.assertEqual(page.record_count, 2)
                self.assertEqual(page.snapshot_count, 0)
                self.assertEqual(page.tick_count, 2)
                self.assertEqual(page.cumulative_record_count, 2)
                self.assertEqual(
                    page.cumulative_source_record_counts,
                    (0, 2, 0, 0),
                )
                self.assertEqual(cursor.next_page_index, 1)
                self.assertEqual(cursor.cumulative_record_count, 2)
                self.assertEqual(
                    cursor.cumulative_source_record_counts,
                    (0, 2, 0, 0),
                )
                self.assertEqual(
                    page.tick_columns.materialized_column_count, 0
                )
                self.assertEqual(
                    page.tick_columns["price_p6"],
                    (101_000_000, 102_000_000),
                )
                self.assertEqual(
                    page.tick_columns.materialized_column_count, 1
                )
                all_columns = page.materialize_all()
                self.assertIn("price_raw", all_columns["tick_columns"])
                terminal = cursor.read_page()
                self.assertIsNotNone(terminal)
                assert terminal is not None
                self.assertTrue(terminal.eof)
                self.assertEqual(terminal.record_count, 0)
                self.assertEqual(terminal.mapping_bytes, 0)
                self.assertEqual(terminal.cumulative_record_count, 2)
                self.assertEqual(
                    terminal.cumulative_source_record_counts,
                    (0, 2, 0, 0),
                )
                self.assertEqual(cursor.next_page_index, 2)
                self.assertEqual(cursor.cumulative_record_count, 2)
                self.assertEqual(
                    cursor.cumulative_source_record_counts,
                    (0, 2, 0, 0),
                )
                self.assertTrue(cursor.eof)
                self.assertTrue(cursor.done)
                self.assertIsNone(cursor.read_page())

    def test_pages_consumes_terminal_even_when_not_yielded(self):
        with _SocketPairServer(_history_server) as server, mock.patch(
            "l2flow_realtime.history.socket.socket",
            return_value=server.client,
        ):
            with _open_instrument_history(
                server.path,
                instrument_id=1,
                requested_page_records=2,
                expected_generation=0,
                expected_run_id=RUN_ID,
                expected_session_epoch=SESSION_EPOCH,
                expected_trade_date=TRADE_DATE,
                expected_capacity=CAPACITY,
                timeout=1.0,
            ) as cursor:
                self.assertEqual(len(tuple(cursor.pages())), 1)
                self.assertTrue(cursor.done)


class DeltaCursorTests(unittest.TestCase):
    def test_checkpoint_is_published_only_after_terminal_eof(self):
        with _SocketPairServer(_delta_server) as server, mock.patch(
            "l2flow_realtime.instrument_delta.socket.socket",
            return_value=server.client,
        ):
            with _open_instrument_tick_delta_session(
                server.path,
                expected_generation=9,
                expected_run_id=RUN_ID,
                expected_session_epoch=SESSION_EPOCH,
                expected_trade_date=TRADE_DATE,
                expected_capacity=CAPACITY,
                timeout=1.0,
            ) as session:
                self.assertIsInstance(
                    session.generation, InstrumentTickDeltaGeneration
                )
                _assert_endpoint_properties(self, session.generation)
                self.assertEqual(
                    session.session_identity,
                    session.generation.session_identity,
                )
                self.assertFalse(session.closed)
                with session.open_instrument(
                    1, requested_page_records=4096
                ) as cursor:
                    self.assertEqual(cursor.instrument_id, 1)
                    self.assertEqual(
                        cursor.session_identity,
                        session.session_identity,
                    )
                    self.assertIs(
                        cursor.base_kind,
                        InstrumentTickDeltaBaseKind.ORIGIN,
                    )
                    self.assertIsNone(cursor.base_checkpoint)
                    self.assertEqual(cursor.expected_record_count, 2)
                    self.assertEqual(cursor.next_page_index, 0)
                    self.assertEqual(cursor.cumulative_record_count, 0)
                    self.assertEqual(
                        cursor.cumulative_source_record_counts,
                        (0, 0, 0, 0),
                    )
                    self.assertFalse(cursor.closed)
                    self.assertFalse(cursor.eof)
                    self.assertFalse(cursor.done)
                    with self.assertRaises(
                        DeltaCheckpointUnverifiedError
                    ):
                        _ = cursor.verified_checkpoint
                    page = cursor.read_page()
                    self.assertIsNotNone(page)
                    assert page is not None
                    self.assertIsInstance(
                        page, InstrumentTickDeltaPage
                    )
                    self.assertEqual(page.generation, session.generation)
                    self.assertEqual(page.metadata, cursor.metadata)
                    self.assertEqual(page.instrument_id, 1)
                    self.assertEqual(len(page), 2)
                    self.assertFalse(page.eof)
                    self.assertEqual(page.cumulative_record_count, 2)
                    self.assertEqual(
                        page.cumulative_source_record_counts,
                        (0, 2, 0, 0),
                    )
                    self.assertEqual(cursor.next_page_index, 1)
                    self.assertEqual(cursor.cumulative_record_count, 2)
                    self.assertEqual(
                        cursor.cumulative_source_record_counts,
                        (0, 2, 0, 0),
                    )
                    self.assertEqual(
                        page.tick_columns["price_p6"],
                        (101_000_000, 102_000_000),
                    )
                    with self.assertRaises(
                        DeltaCheckpointUnverifiedError
                    ):
                        _ = cursor.verified_checkpoint
                    terminal = cursor.read_page()
                    self.assertIsNotNone(terminal)
                    assert terminal is not None
                    self.assertTrue(terminal.eof)
                    self.assertEqual(terminal.instrument_id, 1)
                    self.assertEqual(len(terminal), 0)
                    self.assertEqual(
                        terminal.cumulative_record_count, 2
                    )
                    self.assertEqual(
                        terminal.cumulative_source_record_counts,
                        (0, 2, 0, 0),
                    )
                    self.assertEqual(cursor.next_page_index, 2)
                    self.assertEqual(cursor.cumulative_record_count, 2)
                    self.assertEqual(
                        cursor.cumulative_source_record_counts,
                        (0, 2, 0, 0),
                    )
                    self.assertTrue(cursor.eof)
                    self.assertTrue(cursor.done)
                    checkpoint = cursor.verified_checkpoint
                    self.assertEqual(
                        checkpoint.instrument_tick_source_record_counts,
                        (0, 2, 0, 0),
                    )
                    self.assertFalse(
                        hasattr(checkpoint, "instrument_tick_counts")
                    )
                    encoded = checkpoint.to_dict()
                    self.assertIn(
                        "instrument_tick_source_record_counts", encoded
                    )
                    self.assertNotIn("instrument_tick_counts", encoded)
                    self.assertEqual(
                        InstrumentTickDeltaCheckpoint.from_dict(
                            encoded
                        ),
                        checkpoint,
                    )


class CursorCancellationTests(unittest.TestCase):
    def test_history_close_cancels_a_concurrent_blocking_read(self):
        read_received = threading.Event()
        release_server = threading.Event()

        def block_read() -> None:
            read_received.set()
            release_server.wait(2)

        cancelled_before_server_release = False
        read_errors: list[BaseException] = []
        with _SocketPairServer(
            lambda channel: _history_server(channel, block_read)
        ) as server, mock.patch(
            "l2flow_realtime.history.socket.socket",
            return_value=server.client,
        ):
            cursor = _open_instrument_history(
                server.path,
                instrument_id=1,
                requested_page_records=4096,
                expected_generation=9,
                expected_run_id=RUN_ID,
                expected_session_epoch=SESSION_EPOCH,
                expected_trade_date=TRADE_DATE,
                expected_capacity=CAPACITY,
                timeout=None,
            )

            def read() -> None:
                try:
                    cursor.read_page()
                except BaseException as error:
                    read_errors.append(error)

            thread = threading.Thread(target=read)
            thread.start()
            self.assertTrue(read_received.wait(1))
            cursor.close()
            thread.join(0.5)
            cancelled_before_server_release = not thread.is_alive()
            release_server.set()
            thread.join(2)
            self.assertFalse(thread.is_alive())
            self.assertTrue(cursor.closed)
            self.assertTrue(read_errors)
            with self.assertRaises(ClientClosedError):
                cursor.read_page()
        self.assertTrue(cancelled_before_server_release)

    def test_delta_close_cancels_a_concurrent_blocking_read(self):
        read_received = threading.Event()
        release_server = threading.Event()

        def block_read() -> None:
            read_received.set()
            release_server.wait(2)

        cancelled_before_server_release = False
        read_errors: list[BaseException] = []
        with _SocketPairServer(
            lambda channel: _delta_server(channel, block_read)
        ) as server, mock.patch(
            "l2flow_realtime.instrument_delta.socket.socket",
            return_value=server.client,
        ):
            session = _open_instrument_tick_delta_session(
                server.path,
                expected_generation=9,
                expected_run_id=RUN_ID,
                expected_session_epoch=SESSION_EPOCH,
                expected_trade_date=TRADE_DATE,
                expected_capacity=CAPACITY,
                timeout=None,
            )
            cursor = session.open_instrument(
                1, requested_page_records=4096
            )

            def read() -> None:
                try:
                    cursor.read_page()
                except BaseException as error:
                    read_errors.append(error)

            thread = threading.Thread(target=read)
            thread.start()
            self.assertTrue(read_received.wait(1))
            cursor.close()
            thread.join(0.5)
            cancelled_before_server_release = not thread.is_alive()
            release_server.set()
            thread.join(2)
            self.assertFalse(thread.is_alive())
            self.assertTrue(cursor.closed)
            self.assertTrue(session.closed)
            self.assertTrue(read_errors)
            with self.assertRaises(ClientClosedError):
                cursor.read_page()
            with self.assertRaises(ClientClosedError):
                session.open_instrument(1)
        self.assertTrue(cancelled_before_server_release)


class StrictPageValidationTests(unittest.TestCase):
    def test_unmaterialized_tick_corruption_cannot_be_verified(self):
        def responder(channel: socket.socket, request: bytes) -> None:
            payload = bytearray(_delta_page())
            # raw_type_length is not one of the columns accessed by this
            # test. Eager page validation must still reject it.
            payload[DELTA_PAGE_HEADER_BYTES + 165] = 33
            _send_delta_data_page(channel, request, bytes(payload))

        with _SocketPairServer(
            lambda channel: _delta_read_server(
                channel, responder
            )
        ) as server, mock.patch(
            "l2flow_realtime.instrument_delta.socket.socket",
            return_value=server.client,
        ):
            session = _open_instrument_tick_delta_session(
                server.path,
                expected_generation=9,
                expected_run_id=RUN_ID,
                expected_session_epoch=SESSION_EPOCH,
                expected_trade_date=TRADE_DATE,
                expected_capacity=CAPACITY,
                timeout=1.0,
            )
            cursor = session.open_instrument(1)
            with self.assertRaises(WireFormatError):
                cursor.read_page()
            self.assertTrue(session.closed)
            self.assertTrue(cursor.closed)
            with self.assertRaises(DeltaCheckpointUnverifiedError):
                _ = cursor.verified_checkpoint

    def test_unmaterialized_decimal_corruption_closes_history(self):
        def responder(channel: socket.socket, request: bytes) -> None:
            payload = bytearray(_history_page())
            first_tick = (
                HISTORY_PAGE_HEADER_BYTES
                + 2 * _DESCRIPTOR.size
            )
            # price.reserved is never requested as a column.
            payload[first_tick + 168 + 19] = 1
            _send_history_data_page(channel, request, bytes(payload))

        with _SocketPairServer(
            lambda channel: _history_read_server(
                channel, responder
            )
        ) as server, mock.patch(
            "l2flow_realtime.history.socket.socket",
            return_value=server.client,
        ):
            cursor = _open_instrument_history(
                server.path,
                instrument_id=1,
                requested_page_records=4096,
                expected_generation=9,
                expected_run_id=RUN_ID,
                expected_session_epoch=SESSION_EPOCH,
                expected_trade_date=TRADE_DATE,
                expected_capacity=CAPACITY,
                timeout=1.0,
            )
            with self.assertRaises(WireFormatError):
                cursor.read_page()
            self.assertTrue(cursor.closed)
            with self.assertRaises(ClientClosedError):
                cursor.read_page()

    def test_tick_sequence_cannot_exceed_ingress_sequence(self):
        def responder(channel: socket.socket, request: bytes) -> None:
            payload = bytearray(_delta_page())
            first_tick = DELTA_PAGE_HEADER_BYTES
            struct.pack_into("<Q", payload, first_tick + 32, 2)
            _send_delta_data_page(channel, request, bytes(payload))

        with _SocketPairServer(
            lambda channel: _delta_read_server(
                channel, responder
            )
        ) as server, mock.patch(
            "l2flow_realtime.instrument_delta.socket.socket",
            return_value=server.client,
        ):
            session = _open_instrument_tick_delta_session(
                server.path,
                expected_generation=9,
                expected_run_id=RUN_ID,
                expected_session_epoch=SESSION_EPOCH,
                expected_trade_date=TRADE_DATE,
                expected_capacity=CAPACITY,
                timeout=1.0,
            )
            cursor = session.open_instrument(1)
            with self.assertRaises(WireFormatError):
                cursor.read_page()
            self.assertTrue(session.closed)
            with self.assertRaises(DeltaCheckpointUnverifiedError):
                _ = cursor.verified_checkpoint


class PageAndCumulativeLimitTests(unittest.TestCase):
    def test_history_rejects_page_larger_than_request(self):
        with _SocketPairServer(_history_server) as server, mock.patch(
            "l2flow_realtime.history.socket.socket",
            return_value=server.client,
        ):
            cursor = _open_instrument_history(
                server.path,
                instrument_id=1,
                requested_page_records=1,
                expected_generation=9,
                expected_run_id=RUN_ID,
                expected_session_epoch=SESSION_EPOCH,
                expected_trade_date=TRADE_DATE,
                expected_capacity=CAPACITY,
                timeout=1.0,
            )
            with self.assertRaises(ProtocolError):
                cursor.read_page()
            self.assertTrue(cursor.closed)

    def test_delta_rejects_page_larger_than_request(self):
        with _SocketPairServer(_delta_server) as server, mock.patch(
            "l2flow_realtime.instrument_delta.socket.socket",
            return_value=server.client,
        ):
            session = _open_instrument_tick_delta_session(
                server.path,
                expected_generation=9,
                expected_run_id=RUN_ID,
                expected_session_epoch=SESSION_EPOCH,
                expected_trade_date=TRADE_DATE,
                expected_capacity=CAPACITY,
                timeout=1.0,
            )
            cursor = session.open_instrument(
                1, requested_page_records=1
            )
            with self.assertRaises(ProtocolError):
                cursor.read_page()
            self.assertTrue(session.closed)
            with self.assertRaises(DeltaCheckpointUnverifiedError):
                _ = cursor.verified_checkpoint

    def test_delta_cumulative_count_cannot_exceed_metadata(self):
        with _SocketPairServer(_delta_server) as server, mock.patch(
            "l2flow_realtime.instrument_delta.socket.socket",
            return_value=server.client,
        ):
            session = _open_instrument_tick_delta_session(
                server.path,
                expected_generation=9,
                expected_run_id=RUN_ID,
                expected_session_epoch=SESSION_EPOCH,
                expected_trade_date=TRADE_DATE,
                expected_capacity=CAPACITY,
                timeout=1.0,
            )
            cursor = session.open_instrument(1)
            # Model a prior page count at the declared limit while keeping
            # source counters independent; the next otherwise-valid page
            # must be rejected before an EOF checkpoint can be published.
            cursor._records_read = (
                cursor.metadata.delta_tick_record_count
            )
            with self.assertRaises(WireFormatError):
                cursor.read_page()
            self.assertTrue(session.closed)
            with self.assertRaises(DeltaCheckpointUnverifiedError):
                _ = cursor.verified_checkpoint

    def test_delta_cumulative_source_count_cannot_exceed_metadata(self):
        with _SocketPairServer(_delta_server) as server, mock.patch(
            "l2flow_realtime.instrument_delta.socket.socket",
            return_value=server.client,
        ):
            session = _open_instrument_tick_delta_session(
                server.path,
                expected_generation=9,
                expected_run_id=RUN_ID,
                expected_session_epoch=SESSION_EPOCH,
                expected_trade_date=TRADE_DATE,
                expected_capacity=CAPACITY,
                timeout=1.0,
            )
            cursor = session.open_instrument(1)
            cursor._source_counts[1] = (
                cursor.metadata.delta_tick_source_record_counts[1]
            )
            with self.assertRaises(WireFormatError):
                cursor.read_page()
            self.assertTrue(session.closed)
            with self.assertRaises(DeltaCheckpointUnverifiedError):
                _ = cursor.verified_checkpoint


class ProtocolFailureTests(unittest.TestCase):
    def test_history_error_frame_with_success_metadata_fails_closed(self):
        def responder(channel: socket.socket, request: bytes) -> None:
            read_id = struct.unpack_from("<Q", request, 24)[0]
            channel.send(
                _READ_RESPONSE.pack(
                    CONTROL_MAGIC,
                    WIRE_MAJOR,
                    WIRE_MINOR,
                    4,
                    0,
                    _READ_RESPONSE.size,
                    0,
                    read_id,
                    0,
                    0,
                    0,
                    999,
                )
            )

        with _SocketPairServer(
            lambda channel: _history_read_server(
                channel, responder
            )
        ) as server, mock.patch(
            "l2flow_realtime.history.socket.socket",
            return_value=server.client,
        ):
            cursor = _open_instrument_history(
                server.path,
                instrument_id=1,
                requested_page_records=4096,
                expected_generation=9,
                expected_run_id=RUN_ID,
                expected_session_epoch=SESSION_EPOCH,
                expected_trade_date=TRADE_DATE,
                expected_capacity=CAPACITY,
                timeout=1.0,
            )
            with self.assertRaises(ProtocolError):
                cursor.read_page()
            self.assertTrue(cursor.closed)
            with self.assertRaises(ClientClosedError):
                cursor.read_page()

    def test_delta_error_frame_with_success_metadata_fails_closed(self):
        def responder(channel: socket.socket, request: bytes) -> None:
            read_id = struct.unpack_from("<Q", request, 24)[0]
            channel.send(
                _DELTA_READ_RESPONSE.pack(
                    CONTROL_MAGIC,
                    WIRE_MAJOR,
                    WIRE_MINOR,
                    4,
                    0,
                    _DELTA_READ_RESPONSE.size,
                    0,
                    read_id,
                    0,
                    0,
                    0,
                    999,
                )
            )

        with _SocketPairServer(
            lambda channel: _delta_read_server(
                channel, responder
            )
        ) as server, mock.patch(
            "l2flow_realtime.instrument_delta.socket.socket",
            return_value=server.client,
        ):
            session = _open_instrument_tick_delta_session(
                server.path,
                expected_generation=9,
                expected_run_id=RUN_ID,
                expected_session_epoch=SESSION_EPOCH,
                expected_trade_date=TRADE_DATE,
                expected_capacity=CAPACITY,
                timeout=1.0,
            )
            cursor = session.open_instrument(1)
            with self.assertRaises(ProtocolError):
                cursor.read_page()
            self.assertTrue(session.closed)
            self.assertTrue(cursor.closed)

    def test_delta_instrument_open_status_closes_server_owned_session(self):
        def handler(channel: socket.socket) -> None:
            request = channel.recv(_OPEN_SESSION_REQUEST.size)
            open_id = struct.unpack_from("<Q", request, 24)[0]
            channel.send(
                struct.pack(
                    "<8sHHHHIIQ",
                    CONTROL_MAGIC,
                    WIRE_MAJOR,
                    WIRE_MINOR,
                    0,
                    0,
                    296,
                    0,
                    open_id,
                )
                + pack_generation_endpoint(_endpoint())
                + struct.pack("<Q", 201)
            )
            request = channel.recv(384)
            instrument_open_id = struct.unpack_from(
                "<Q", request, 24
            )[0]
            channel.send(
                struct.pack(
                    "<8sHHHHIIQ",
                    CONTROL_MAGIC,
                    WIRE_MAJOR,
                    WIRE_MINOR,
                    4,
                    0,
                    776,
                    0,
                    instrument_open_id,
                )
                + b"\x00" * (776 - 32)
            )

        with _SocketPairServer(handler) as server, mock.patch(
            "l2flow_realtime.instrument_delta.socket.socket",
            return_value=server.client,
        ):
            session = _open_instrument_tick_delta_session(
                server.path,
                expected_generation=9,
                expected_run_id=RUN_ID,
                expected_session_epoch=SESSION_EPOCH,
                expected_trade_date=TRADE_DATE,
                expected_capacity=CAPACITY,
                timeout=1.0,
            )
            with self.assertRaises(StreamNotFoundError):
                session.open_instrument(1)
            self.assertTrue(session.closed)
            with self.assertRaises(ClientClosedError):
                session.open_instrument(1)

    def test_two_concurrent_history_reads_are_serialized(self):
        with _SocketPairServer(_history_server) as server, mock.patch(
            "l2flow_realtime.history.socket.socket",
            return_value=server.client,
        ):
            cursor = _open_instrument_history(
                server.path,
                instrument_id=1,
                requested_page_records=4096,
                expected_generation=9,
                expected_run_id=RUN_ID,
                expected_session_epoch=SESSION_EPOCH,
                expected_trade_date=TRADE_DATE,
                expected_capacity=CAPACITY,
                timeout=1.0,
            )
            barrier = threading.Barrier(3)
            pages: list[HistoryPage] = []
            errors: list[BaseException] = []

            def read() -> None:
                barrier.wait()
                try:
                    page = cursor.read_page()
                    if page is not None:
                        pages.append(page)
                except BaseException as error:
                    errors.append(error)

            threads = [
                threading.Thread(target=read),
                threading.Thread(target=read),
            ]
            for thread in threads:
                thread.start()
            barrier.wait()
            for thread in threads:
                thread.join(2)
                self.assertFalse(thread.is_alive())
            self.assertEqual(errors, [])
            self.assertEqual(
                sorted(page.eof for page in pages),
                [False, True],
            )
            self.assertTrue(cursor.done)
            cursor.close()


class StrictPublicModelTests(unittest.TestCase):
    _REJECTION_ERRORS = (TypeError, ValueError, WireFormatError)

    def test_generation_endpoint_rejects_invalid_unsigned_values(self):
        invalid = (
            ("session_epoch", -1),
            ("durable_sequence", -1),
            ("durable_sequence", True),
            ("history_published_monotonic_ns", 1 << 64),
            ("capacity", 1 << 32),
            ("source_stream_ids", (11, 12, 13, 1 << 32)),
            ("source_sequence_exclusive", (1, 3, 1, True)),
        )
        for field, value in invalid:
            with self.subTest(field=field, value=value):
                with self.assertRaises(self._REJECTION_ERRORS):
                    validate_generation_endpoint(
                        replace(_endpoint(), **{field: value})
                    )

    def test_generation_endpoint_rejects_mutable_or_wrong_size_bytes(self):
        invalid = (
            ("run_id", bytearray(RUN_ID)),
            ("run_id", RUN_ID[:-1]),
            ("catalog_digest", bytearray(CATALOG_DIGEST)),
            ("catalog_digest", CATALOG_DIGEST[:-1]),
            (
                "input_identity_sha256",
                bytearray(INPUT_DIGEST),
            ),
            ("input_identity_sha256", INPUT_DIGEST + b"x"),
        )
        for field, value in invalid:
            with self.subTest(field=field, value=value):
                with self.assertRaises(self._REJECTION_ERRORS):
                    validate_generation_endpoint(
                        replace(_endpoint(), **{field: value})
                    )

    def test_checkpoint_rejects_invalid_scalars_and_count_values(self):
        checkpoint = _target_checkpoint()
        invalid = (
            ("session_epoch", -1),
            ("durable_sequence", True),
            ("capacity", 1 << 32),
            ("instrument_id", -1),
            ("instrument_id", True),
            ("instrument_id", 1 << 32),
            ("ordinal", -1),
            ("ordinal", True),
            ("ordinal", 1 << 32),
            (
                "instrument_tick_source_record_counts",
                (0, -1, 0, 0),
            ),
            (
                "instrument_tick_source_record_counts",
                (0, True, 0, 0),
            ),
            (
                "instrument_tick_source_record_counts",
                (0, 1 << 64, 0, 0),
            ),
        )
        for field, value in invalid:
            with self.subTest(field=field, value=value):
                with self.assertRaises(self._REJECTION_ERRORS):
                    replace(checkpoint, **{field: value})

    def test_checkpoint_rejects_mutable_or_wrong_size_bytes(self):
        checkpoint = _target_checkpoint()
        invalid = (
            ("run_id", bytearray(RUN_ID)),
            ("run_id", RUN_ID[:-1]),
            ("catalog_digest", bytearray(CATALOG_DIGEST)),
            ("catalog_digest", CATALOG_DIGEST[:-1]),
            (
                "input_identity_sha256",
                bytearray(INPUT_DIGEST),
            ),
            ("input_identity_sha256", INPUT_DIGEST + b"x"),
        )
        for field, value in invalid:
            with self.subTest(field=field, value=value):
                with self.assertRaises(self._REJECTION_ERRORS):
                    replace(checkpoint, **{field: value})

    def test_checkpoint_has_only_the_formal_source_count_field(self):
        checkpoint = _target_checkpoint()
        self.assertEqual(
            checkpoint.instrument_tick_source_record_counts,
            (0, 2, 0, 0),
        )
        self.assertFalse(hasattr(checkpoint, "instrument_tick_counts"))
        self.assertIn(
            "instrument_tick_source_record_counts",
            checkpoint.__dataclass_fields__,
        )
        self.assertNotIn(
            "instrument_tick_counts", checkpoint.__dataclass_fields__
        )

    def test_public_stream_exception_inheritance(self):
        for error in (
            HistoryCursorClosedError,
            InstrumentTickDeltaCursorClosedError,
            InstrumentTickDeltaSessionClosedError,
        ):
            with self.subTest(error=error.__name__):
                self.assertTrue(issubclass(error, ClientClosedError))
        for error in (
            HistoryGenerationChangedError,
            DeltaCheckpointUnverifiedError,
            StreamInternalFailureError,
        ):
            with self.subTest(error=error.__name__):
                self.assertTrue(issubclass(error, UnavailableError))


class GenerationValidationTests(unittest.TestCase):
    def test_bound_no_data_generation_accepts_zero_data_state(self):
        endpoint = replace(
            _endpoint(),
            data_state_generation=0,
            ingress_sequence_exclusive=1,
            tick_stream_sequence_exclusive=1,
            accepted_sequence=0,
            durable_sequence=0,
            applied_sequence=0,
            source_sequence_exclusive=(1, 1, 1, 1),
            available_count=0,
            snapshot_available_count=0,
            tick_available_count=0,
            factor_eligible_count=0,
        )
        validate_generation_endpoint(endpoint)
        self.assertEqual(len(pack_generation_endpoint(endpoint)), 256)

    def test_catalog_generation_must_equal_bound_count(self):
        with self.assertRaises(WireFormatError):
            validate_generation_endpoint(
                replace(_endpoint(), catalog_generation=2)
            )

    def test_publication_cannot_precede_receive_cut(self):
        with self.assertRaises(WireFormatError):
            validate_generation_endpoint(
                replace(
                    _endpoint(),
                    recv_monotonic_cut_ns=12_000,
                    history_published_monotonic_ns=11_000,
                )
            )


class ClientLockIsolationTests(unittest.TestCase):
    def test_history_open_does_not_hold_latest_lock(self):
        from tests.python.test_l2flow_realtime import (
            FakeV2Library,
        )
        from l2flow_realtime import native

        reader = native.NativeReader.open_fd(
            9, library=FakeV2Library()
        )
        client = L2FlowClient(
            reader,
            stale_after_ns=None,
            control_socket_path="/tmp/not-used.sock",
        )
        entered = threading.Event()
        release = threading.Event()
        fake_cursor = SimpleNamespace(
            generation=SimpleNamespace(
                session_identity=client.session_identity
            ),
            close=lambda: None,
        )

        def slow_open(*_args, **_kwargs):
            entered.set()
            if not release.wait(2):
                raise RuntimeError("test did not release history OPEN")
            return fake_cursor

        result: list[object] = []

        def open_history() -> None:
            result.append(client.open_instrument_history(1))

        try:
            with mock.patch.object(
                client,
                "_checked_session",
                wraps=client._checked_session,
            ) as checked_session, mock.patch(
                "l2flow_realtime.history._open_instrument_history",
                slow_open,
            ):
                thread = threading.Thread(target=open_history)
                thread.start()
                self.assertTrue(entered.wait(1))
                started = time.monotonic()
                latest = client.latest_snapshot(1)
                elapsed = time.monotonic() - started
                self.assertEqual(latest.instrument_id, 1)
                self.assertLess(elapsed, 0.25)
                release.set()
                thread.join(2)
                self.assertFalse(thread.is_alive())
                self.assertEqual(result, [fake_cursor])
                self.assertEqual(checked_session.call_count, 2)
        finally:
            release.set()
            client.close()
            reader.close()


if __name__ == "__main__":
    unittest.main()
