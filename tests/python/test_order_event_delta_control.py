#!/usr/bin/env python3
"""Contract tests for the live order-event Unix control connector."""

from __future__ import annotations

import array
import ctypes
import fcntl
import os
import socket
import struct
import time
import unittest
from dataclasses import replace
from types import SimpleNamespace
from unittest import mock

import l2flow_realtime
from l2flow_realtime.client import L2FlowClient
from l2flow_realtime.models import (
    CatalogScope,
    ProtocolError,
    ServerState,
    SessionInfo,
    StaleSessionError,
)
from l2flow_realtime.order_event_delta_control import (
    CONTROL_MAGIC,
    CONTROL_MINOR,
    CONTROL_OK,
    CONTROL_SOURCE_SESSION_MISMATCH,
    CONTROL_UNAVAILABLE,
    LiveOrderEventDeltaControlError,
    LiveOrderEventDeltaPeerCredentialError,
    LiveOrderEventDeltaSourceSession,
    LiveOrderEventDeltaSourceSessionMismatchError,
    _REQUEST,
    _RESPONSE,
    open_live_order_events,
)
from l2flow_realtime.order_event_delta_live import (
    LiveOrderEventDeltaProducerState,
    LiveOrderEventDeltaStreamQuality,
    LiveOrderEventDeltaTemporalCoverage,
    _DerivedEventRowC,
    _LiveReadResultC,
    _LiveSessionC,
)


_SOURCE_RUN_ID = bytes(range(1, 17))
_EVENT_RUN_ID = bytes(range(17, 33))
_SOURCE_EPOCH = 71
_EVENT_EPOCH = 19
_TRADE_DATE = 20260730
_CATALOG_DIGEST = bytes(range(33, 65))
_CATALOG_VERSION = 2026073001
_CATALOG_CAPACITY = 4
_RING_CAPACITY = 8
_MAPPING_BYTES = 8192
_REQUIRED_SEALS = (
    getattr(fcntl, "F_SEAL_SEAL", 0x0001)
    | getattr(fcntl, "F_SEAL_SHRINK", 0x0002)
    | getattr(fcntl, "F_SEAL_GROW", 0x0004)
    | getattr(fcntl, "F_SEAL_FUTURE_WRITE", 0x0010)
)


class _Function:
    def __init__(self, callback):
        self.callback = callback
        self.argtypes = None
        self.restype = None

    def __call__(self, *arguments):
        return self.callback(*arguments)


class _FakeEventLibrary:
    """Minimal stand-in for the event-delta C reader ABI."""

    def __init__(self, *, state=LiveOrderEventDeltaProducerState.ACTIVE):
        self.state = state
        self.opened_fd = None
        self.open_calls = 0
        self.closed = False
        self.l2flow_order_event_delta_reader_open_v1 = _Function(
            self._open
        )
        self.l2flow_order_event_delta_reader_close_v1 = _Function(
            self._close
        )
        self.l2flow_order_event_delta_reader_read_v1 = _Function(
            lambda *_arguments: 8
        )
        self.l2flow_order_event_delta_reader_session_v1 = _Function(
            lambda *_arguments: 0
        )
        self.l2flow_order_event_delta_reader_state_v1 = _Function(
            self._state
        )
        self.l2flow_order_event_delta_error_name_v1 = _Function(
            lambda _code: b"test_error"
        )

    def _open(self, descriptor, session_pointer, output, system_error):
        self.open_calls += 1
        self.opened_fd = int(descriptor)
        session = session_pointer._obj
        if (
            bytes(session.run_id) != _EVENT_RUN_ID
            or session.session_epoch != _EVENT_EPOCH
            or session.trade_date != _TRADE_DATE
            or session.ring_capacity != _RING_CAPACITY
            or session.total_mapping_bytes != _MAPPING_BYTES
            or session.temporal_coverage
            != int(
                LiveOrderEventDeltaTemporalCoverage.FROM_MARKET_OPEN
            )
            or session.stream_quality
            != int(
                LiveOrderEventDeltaStreamQuality.
                LOCAL_TICK_STREAM_CONTIGUOUS
            )
        ):
            return 7
        os.fstat(self.opened_fd)
        output._obj.value = 0x1234
        system_error._obj.value = 0
        return 0

    def _close(self, _handle):
        self.closed = True

    def _state(self, _handle, output):
        output._obj.value = int(self.state)
        return 0


class _ScriptedSocket:
    """In-memory SOCK_SEQPACKET/SCM_RIGHTS test double.

    The execution sandbox can reject Unix socket syscalls. ``recvmsg`` still
    exposes the exact ancillary tuple consumed by ``_recv_fds`` and duplicates
    every source descriptor to model SCM_RIGHTS receiver ownership.
    """

    def __init__(
        self,
        response_builder,
        attached_fds,
        captured,
        *,
        peer_credentials=None,
    ):
        self._response_builder = response_builder
        self._attached_fds = tuple(attached_fds)
        self._captured = captured
        self._peer_credentials = (
            (os.getpid(), os.geteuid(), os.getegid())
            if peer_credentials is None
            else peer_credentials
        )
        self.closed = False

    def settimeout(self, value):
        self._captured.setdefault("timeouts", []).append(value)

    def connect(self, path):
        self._captured["path"] = path

    def getsockopt(self, level, option, length=0):
        return struct.pack("3i", *self._peer_credentials)

    def send(self, payload):
        self._captured["request"] = bytes(payload)
        return len(payload)

    def recvmsg(self, _data_bytes, _ancillary_bytes, _flags):
        response = self._response_builder(self._captured["request"])
        received = tuple(os.dup(fd) for fd in self._attached_fds)
        self._captured["received_fds"] = received
        ancillary = (
            [
                (
                    socket.SOL_SOCKET,
                    socket.SCM_RIGHTS,
                    array.array("i", received).tobytes(),
                )
            ]
            if received
            else []
        )
        return response, ancillary, 0, None

    def close(self):
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, _type, _value, _traceback):
        self.close()


class _CompletedThread:
    def join(self, _timeout=None):
        return None

    @staticmethod
    def is_alive():
        return False


def _source_session(
    *,
    run_id=_SOURCE_RUN_ID,
    catalog_digest=_CATALOG_DIGEST,
    session_epoch=_SOURCE_EPOCH,
    catalog_generation=1,
    catalog_version=_CATALOG_VERSION,
    trade_date=_TRADE_DATE,
    catalog_trade_date=_TRADE_DATE,
    capacity=_CATALOG_CAPACITY,
    bound_count=_CATALOG_CAPACITY,
    catalog_scope=CatalogScope.DECLARED_DAILY_A_SHARE,
    coverage_complete=True,
    temporal_coverage=(
        LiveOrderEventDeltaTemporalCoverage.FROM_MARKET_OPEN
    ),
    stream_quality=(
        LiveOrderEventDeltaStreamQuality.LOCAL_TICK_STREAM_CONTIGUOUS
    ),
):
    return LiveOrderEventDeltaSourceSession(
        run_id=run_id,
        catalog_digest=catalog_digest,
        session_epoch=session_epoch,
        catalog_generation=catalog_generation,
        catalog_version=catalog_version,
        trade_date=trade_date,
        catalog_trade_date=catalog_trade_date,
        capacity=capacity,
        bound_count=bound_count,
        catalog_scope=catalog_scope,
        coverage_complete=coverage_complete,
        temporal_coverage=temporal_coverage,
        stream_quality=stream_quality,
    )


def _response(
    request: bytes,
    *,
    status=CONTROL_OK,
    magic=CONTROL_MAGIC,
    minor=CONTROL_MINOR,
    response_request_id=None,
    source_run_id=_SOURCE_RUN_ID,
    source_catalog_digest=_CATALOG_DIGEST,
    source_session_epoch=_SOURCE_EPOCH,
    source_catalog_generation=1,
    source_catalog_version=_CATALOG_VERSION,
    source_trade_date=_TRADE_DATE,
    source_catalog_trade_date=_TRADE_DATE,
    source_capacity=_CATALOG_CAPACITY,
    source_bound_count=_CATALOG_CAPACITY,
    source_catalog_scope=CatalogScope.DECLARED_DAILY_A_SHARE,
    source_coverage_complete=1,
    source_temporal_coverage=(
        LiveOrderEventDeltaTemporalCoverage.FROM_MARKET_OPEN
    ),
    source_stream_quality=(
        LiveOrderEventDeltaStreamQuality.LOCAL_TICK_STREAM_CONTIGUOUS
    ),
    producer_state=LiveOrderEventDeltaProducerState.ACTIVE,
    event_run_id=_EVENT_RUN_ID,
    event_session_epoch=_EVENT_EPOCH,
    event_trade_date=_TRADE_DATE,
    event_header_flags=0,
    event_ring_capacity=_RING_CAPACITY,
    event_total_mapping_bytes=_MAPPING_BYTES,
    event_temporal_coverage=(
        LiveOrderEventDeltaTemporalCoverage.FROM_MARKET_OPEN
    ),
    event_stream_quality=(
        LiveOrderEventDeltaStreamQuality.LOCAL_TICK_STREAM_CONTIGUOUS
    ),
    reserved_event=0,
    reserved=None,
):
    request_fields = _REQUEST.unpack(request)
    identifier = (
        request_fields[7]
        if response_request_id is None
        else response_request_id
    )
    return _RESPONSE.pack(
        magic,
        1,
        minor,
        status,
        0,
        _RESPONSE.size,
        0,
        identifier,
        source_run_id,
        source_catalog_digest,
        source_session_epoch,
        source_catalog_generation,
        source_catalog_version,
        source_trade_date,
        source_catalog_trade_date,
        source_capacity,
        source_bound_count,
        int(source_catalog_scope),
        source_coverage_complete,
        event_run_id,
        event_session_epoch,
        event_trade_date,
        int(producer_state),
        event_header_flags,
        reserved_event,
        event_ring_capacity,
        event_total_mapping_bytes,
        23,
        17,
        999,
        111,
        int(source_temporal_coverage),
        int(source_stream_quality),
        int(event_temporal_coverage),
        int(event_stream_quality),
        *(reserved if reserved is not None else (0,) * 4),
    )


def _sealed_read_only_memfd(size=_MAPPING_BYTES, *, sealed=True):
    writable = os.memfd_create(
        "l2flow-python-event-control-test",
        os.MFD_CLOEXEC | os.MFD_ALLOW_SEALING,
    )
    try:
        os.ftruncate(writable, size)
        if sealed:
            fcntl.fcntl(
                writable,
                getattr(fcntl, "F_ADD_SEALS", 1033),
                _REQUIRED_SEALS,
            )
        return os.open(
            f"/proc/self/fd/{writable}",
            os.O_RDONLY | os.O_CLOEXEC,
        )
    finally:
        os.close(writable)


def _sealed_writable_memfd(size=_MAPPING_BYTES):
    descriptor = os.memfd_create(
        "l2flow-python-event-control-writable-test",
        os.MFD_CLOEXEC | os.MFD_ALLOW_SEALING,
    )
    os.ftruncate(descriptor, size)
    fcntl.fcntl(
        descriptor,
        getattr(fcntl, "F_ADD_SEALS", 1033),
        _REQUIRED_SEALS,
    )
    return descriptor


def _assert_received_descriptors_closed(test, captured):
    for descriptor in captured.get("received_fds", ()):
        with test.assertRaises(OSError):
            os.fstat(descriptor)


def _exchange(response_builder, attached_fds=()):
    captured = {}
    failures = []
    return (
        _ScriptedSocket(
            response_builder, attached_fds, captured
        ),
        captured,
        failures,
        _CompletedThread(),
    )


def _session_info(
    *,
    trade_date=_TRADE_DATE,
    catalog_digest=_CATALOG_DIGEST,
    catalog_version=_CATALOG_VERSION,
):
    return SessionInfo(
        run_id=_SOURCE_RUN_ID,
        layout_digest=b"L" * 32,
        catalog_digest=catalog_digest,
        session_epoch=_SOURCE_EPOCH,
        catalog_generation=1,
        data_state_generation=1,
        accepted_sequence=5,
        applied_sequence=5,
        processing_lag_records=0,
        tick_ring_capacity=8,
        tick_highest_published_sequence=5,
        tick_contiguous_published_sequence=5,
        kline_generation=1,
        heartbeat_monotonic_ns=time.monotonic_ns(),
        published_records=5,
        trade_date=trade_date,
        server_state=ServerState.ACTIVE,
        flags=1 << 2,
        capacity=4,
        window_count=0,
        catalog_scope=CatalogScope.DECLARED_DAILY_A_SHARE,
        coverage_complete=True,
        bound_count=4,
        available_count=0,
        snapshot_available_count=0,
        tick_available_count=0,
        factor_eligible_count=0,
        catalog_trade_date=trade_date,
        catalog_version=catalog_version,
    )


class _ClientNative:
    def __init__(self):
        self._library = object()
        self.current = _session_info()
        self.closed = False

    def session(self):
        return self.current

    def close(self):
        self.closed = True


class _FakeConnectedReader:
    def __init__(self, source):
        self.control_snapshot = SimpleNamespace(source_session=source)
        self.closed = False

    def close(self):
        self.closed = True


class LiveOrderEventDeltaControlTests(unittest.TestCase):
    def test_ctypes_and_control_wire_sizes_match_native_abi(self):
        self.assertEqual(_REQUEST.size, 144)
        self.assertEqual(_RESPONSE.size, 264)
        self.assertEqual(CONTROL_MINOR, 2)
        self.assertEqual(ctypes.sizeof(_LiveSessionC), 64)
        self.assertEqual(ctypes.sizeof(_LiveReadResultC), 80)
        self.assertEqual(ctypes.sizeof(_DerivedEventRowC), 320)
        self.assertIs(
            l2flow_realtime.LiveOrderEventDeltaSourceSession,
            LiveOrderEventDeltaSourceSession,
        )
        self.assertIs(
            l2flow_realtime.open_live_order_events,
            open_live_order_events,
        )
        for timeout in (None, float("inf"), float("nan"), 60.1):
            with self.subTest(timeout=timeout):
                with self.assertRaises(ValueError):
                    open_live_order_events(
                        "/unused/event-control.sock",
                        expected_source_session=_source_session(),
                        native_library=_FakeEventLibrary(),
                        timeout=timeout,
                    )

    def test_success_validates_request_peer_fd_and_snapshot(self):
        ring_fd = _sealed_read_only_memfd()
        library = _FakeEventLibrary()
        channel, captured, failures, thread = _exchange(
            _response, (ring_fd,)
        )
        try:
            reader = open_live_order_events(
                "/unused/event-control.sock",
                expected_source_session=_source_session(),
                native_library=library,
                timeout=1.0,
                batch_records=31,
                request_id=1234,
                _socket_factory=lambda *_arguments: channel,
            )
            self.assertEqual(reader.control_snapshot.request_id, 1234)
            self.assertEqual(
                reader.control_snapshot.source_session,
                _source_session(),
            )
            self.assertEqual(
                reader.control_snapshot.event_session.run_id,
                _EVENT_RUN_ID,
            )
            self.assertEqual(
                reader.control_snapshot.event_published_sequence, 23
            )
            self.assertEqual(
                reader.control_snapshot.source_tick_consumed_sequence,
                17,
            )
            self.assertEqual(
                reader.control_snapshot.peer_uid, os.geteuid()
            )
            self.assertEqual(
                reader.producer_state(),
                LiveOrderEventDeltaProducerState.ACTIVE,
            )

            request = _REQUEST.unpack(captured["request"])
            self.assertEqual(request[0], CONTROL_MAGIC)
            self.assertEqual(request[1:7], (1, 2, 1, 0, 144, 0))
            self.assertEqual(request[7], 1234)
            self.assertEqual(request[8], _SOURCE_RUN_ID)
            self.assertEqual(request[9], _CATALOG_DIGEST)
            self.assertEqual(request[10], _SOURCE_EPOCH)
            self.assertEqual(request[11], 1)
            self.assertEqual(request[12], _CATALOG_VERSION)
            self.assertEqual(request[13:19], (
                _TRADE_DATE,
                _TRADE_DATE,
                _CATALOG_CAPACITY,
                _CATALOG_CAPACITY,
                int(CatalogScope.DECLARED_DAILY_A_SHARE),
                1,
            ))
            self.assertEqual(request[19:], (1, 1, 0))
            self.assertEqual(len(captured["timeouts"]), 3)
            self.assertTrue(
                all(
                    0 < value <= 1.0
                    for value in captured["timeouts"]
                )
            )

            # The SCM_RIGHTS fd belongs to the packet only. Native open must
            # duplicate it; returning from the connector closes that packet fd.
            with self.assertRaises(OSError):
                os.fstat(library.opened_fd)
            reader.close()
            self.assertTrue(library.closed)
        finally:
            os.close(ring_fd)
            thread.join(2.0)
        self.assertFalse(thread.is_alive())
        self.assertEqual(failures, [])

    def test_success_requires_full_source_identity(self):
        variants = (
            {"source_run_id": b"X" * 16},
            {"source_catalog_digest": b"X" * 32},
            {"source_session_epoch": _SOURCE_EPOCH + 1},
            {"source_catalog_generation": 2},
            {"source_catalog_version": _CATALOG_VERSION + 1},
            {"source_trade_date": _TRADE_DATE + 1},
            {"source_catalog_trade_date": _TRADE_DATE + 1},
            {"source_capacity": _CATALOG_CAPACITY + 1},
            {"source_bound_count": _CATALOG_CAPACITY - 1},
            {"source_catalog_scope": 1},
            {"source_coverage_complete": 0},
            {"source_temporal_coverage": 2},
            {"source_stream_quality": 2},
        )
        for override in variants:
            with self.subTest(override=override):
                ring_fd = _sealed_read_only_memfd()
                library = _FakeEventLibrary()
                channel, captured, failures, thread = _exchange(
                    lambda request, values=override: _response(
                        request, **values
                    ),
                    (ring_fd,),
                )
                try:
                    with self.assertRaises(
                        LiveOrderEventDeltaSourceSessionMismatchError
                    ):
                        open_live_order_events(
                            "/unused/event-control.sock",
                            expected_source_session=_source_session(),
                            native_library=library,
                            request_id=91,
                            _socket_factory=lambda *_arguments: channel,
                        )
                    self.assertEqual(library.open_calls, 0)
                    _assert_received_descriptors_closed(self, captured)
                finally:
                    os.close(ring_fd)
                    thread.join(2.0)
                self.assertEqual(failures, [])

    def test_native_library_path_is_loaded_explicitly(self):
        ring_fd = _sealed_read_only_memfd()
        library = _FakeEventLibrary()
        channel, captured, failures, thread = _exchange(
            _response, (ring_fd,)
        )
        try:
            with mock.patch(
                "l2flow_realtime.order_event_delta_control.ctypes.CDLL",
                return_value=library,
            ) as loader:
                reader = open_live_order_events(
                    "/unused/event-control.sock",
                    expected_source_session=_source_session(),
                    native_library_path="/tmp/libl2flow-reader.so",
                    request_id=92,
                    _socket_factory=lambda *_arguments: channel,
                )
            loader.assert_called_once_with(
                "/tmp/libl2flow-reader.so", use_errno=True
            )
            reader.close()
            _assert_received_descriptors_closed(self, captured)
        finally:
            os.close(ring_fd)
            thread.join(2.0)
        self.assertEqual(failures, [])

    def test_same_effective_uid_is_mandatory_before_request(self):
        channel, captured, _failures, _thread = _exchange(
            _response
        )
        channel._peer_credentials = (
            os.getpid(),
            os.geteuid() + 1,
            os.getegid(),
        )
        with self.assertRaises(
            LiveOrderEventDeltaPeerCredentialError
        ):
            open_live_order_events(
                "/unused/event-control.sock",
                expected_source_session=_source_session(),
                native_library=_FakeEventLibrary(),
                _socket_factory=lambda *_arguments: channel,
            )
        self.assertNotIn("request", captured)

    def test_status_fd_shape_and_descriptor_seals_fail_closed(self):
        cases = (
            (
                "unavailable",
                lambda request: _response(
                    request, status=CONTROL_UNAVAILABLE
                ),
                (),
                LiveOrderEventDeltaControlError,
            ),
            (
                "source_mismatch_status",
                lambda request: _response(
                    request, status=CONTROL_SOURCE_SESSION_MISMATCH
                ),
                (),
                LiveOrderEventDeltaSourceSessionMismatchError,
            ),
        )
        for name, builder, fds, error_type in cases:
            with self.subTest(name=name):
                channel, _captured, failures, thread = _exchange(
                    builder, fds
                )
                with self.assertRaises(error_type):
                    open_live_order_events(
                        "/unused/event-control.sock",
                        expected_source_session=_source_session(),
                        native_library=_FakeEventLibrary(),
                        request_id=7,
                        _socket_factory=lambda *_arguments: channel,
                    )
                thread.join(2.0)
                self.assertEqual(failures, [])

        bad_descriptors = (
            ("missing_seals", _sealed_read_only_memfd(sealed=False)),
            ("writable", _sealed_writable_memfd()),
            ("wrong_size", _sealed_read_only_memfd(4096)),
        )
        try:
            for name, descriptor in bad_descriptors:
                with self.subTest(name=name):
                    library = _FakeEventLibrary()
                    channel, captured, failures, thread = _exchange(
                        _response, (descriptor,)
                    )
                    with self.assertRaises(ProtocolError):
                        open_live_order_events(
                            "/unused/event-control.sock",
                            expected_source_session=_source_session(),
                            native_library=library,
                            request_id=8,
                            _socket_factory=(
                                lambda *_arguments, item=channel: item
                            ),
                        )
                    self.assertEqual(library.open_calls, 0)
                    _assert_received_descriptors_closed(self, captured)
                    thread.join(2.0)
                    self.assertEqual(failures, [])
        finally:
            for _name, descriptor in bad_descriptors:
                os.close(descriptor)

    def test_response_fd_cardinality_is_exact(self):
        ring_fds = (
            _sealed_read_only_memfd(),
            _sealed_read_only_memfd(),
        )
        cases = (
            (
                "success_without_fd",
                _response,
                (),
            ),
            (
                "failure_with_fd",
                lambda request: _response(
                    request, status=CONTROL_UNAVAILABLE
                ),
                (ring_fds[0],),
            ),
            (
                "success_with_two_fds",
                _response,
                ring_fds,
            ),
        )
        try:
            for name, builder, attached in cases:
                with self.subTest(name=name):
                    channel, _captured, failures, thread = _exchange(
                        builder, attached
                    )
                    with self.assertRaises(ProtocolError):
                        open_live_order_events(
                            "/unused/event-control.sock",
                            expected_source_session=_source_session(),
                            native_library=_FakeEventLibrary(),
                            request_id=44,
                            _socket_factory=(
                                lambda *_arguments, item=channel: item
                            ),
                        )
                    _assert_received_descriptors_closed(
                        self, _captured
                    )
                    thread.join(2.0)
                    self.assertEqual(failures, [])
        finally:
            for descriptor in ring_fds:
                os.close(descriptor)

    def test_received_descriptor_is_closed_when_validation_raises(self):
        ring_fd = _sealed_read_only_memfd()
        channel, _captured, failures, thread = _exchange(
            _response, (ring_fd,)
        )
        received = []

        def fail_validation(descriptor, _session):
            received.append(descriptor)
            os.fstat(descriptor)
            raise ProtocolError("injected descriptor validation failure")

        try:
            with mock.patch(
                "l2flow_realtime.order_event_delta_control."
                "_validate_ring_descriptor",
                side_effect=fail_validation,
            ):
                with self.assertRaises(ProtocolError):
                    open_live_order_events(
                        "/unused/event-control.sock",
                        expected_source_session=_source_session(),
                        native_library=_FakeEventLibrary(),
                        request_id=45,
                        _socket_factory=lambda *_arguments: channel,
                    )
            self.assertEqual(len(received), 1)
            with self.assertRaises(OSError):
                os.fstat(received[0])
        finally:
            os.close(ring_fd)
            thread.join(2.0)
        self.assertEqual(failures, [])

    def test_malformed_envelope_and_non_active_ring_are_rejected(self):
        for name, builder in (
            (
                "old_minor",
                lambda request: _response(
                    request, minor=0
                ),
            ),
            (
                "magic",
                lambda request: _response(
                    request, magic=b"NOT-EVT\0"
                ),
            ),
            (
                "request_id",
                lambda request: _response(
                    request, response_request_id=999
                ),
            ),
            (
                "reserved_event",
                lambda request: _response(
                    request, reserved_event=1
                ),
            ),
            (
                "reserved",
                lambda request: _response(
                    request, reserved=(0, 0, 1, 0)
                ),
            ),
            (
                "producer_state",
                lambda request: _response(
                    request,
                    producer_state=(
                        LiveOrderEventDeltaProducerState.DRAINING
                    ),
                ),
            ),
            (
                "coverage_lost",
                lambda request: _response(
                    request, event_header_flags=1
                ),
            ),
            (
                "event_temporal_coverage_mismatch",
                lambda request: _response(
                    request, event_temporal_coverage=2
                ),
            ),
            (
                "unknown_event_stream_quality",
                lambda request: _response(
                    request, event_stream_quality=2
                ),
            ),
            (
                "event_mapping_geometry",
                lambda request: _response(
                    request, event_total_mapping_bytes=12_288
                ),
            ),
        ):
            with self.subTest(name=name):
                ring_fd = _sealed_read_only_memfd()
                channel, captured, failures, thread = _exchange(
                    builder, (ring_fd,)
                )
                try:
                    with self.assertRaises(ProtocolError):
                        open_live_order_events(
                            "/unused/event-control.sock",
                            expected_source_session=_source_session(),
                            native_library=_FakeEventLibrary(),
                            request_id=71,
                            _socket_factory=lambda *_arguments: channel,
                        )
                    _assert_received_descriptors_closed(
                        self, captured
                    )
                finally:
                    os.close(ring_fd)
                    thread.join(2.0)
                self.assertEqual(failures, [])

    def test_native_state_change_during_attach_closes_reader(self):
        ring_fd = _sealed_read_only_memfd()
        library = _FakeEventLibrary(
            state=LiveOrderEventDeltaProducerState.DRAINING
        )
        channel, _captured, failures, thread = _exchange(
            _response, (ring_fd,)
        )
        try:
            with self.assertRaises(LiveOrderEventDeltaControlError):
                open_live_order_events(
                    "/unused/event-control.sock",
                    expected_source_session=_source_session(),
                    native_library=library,
                    _socket_factory=lambda *_arguments: channel,
                )
            self.assertTrue(library.closed)
            _assert_received_descriptors_closed(self, _captured)
        finally:
            os.close(ring_fd)
            thread.join(2.0)
        self.assertEqual(failures, [])

    def test_client_helper_reuses_library_and_rechecks_source_session(self):
        native = _ClientNative()
        client = L2FlowClient(native, stale_after_ns=None)
        connected = _FakeConnectedReader(_source_session())

        def connector(path, **arguments):
            self.assertEqual(path, "/event.sock")
            self.assertEqual(
                arguments["expected_source_session"], _source_session()
            )
            self.assertIs(arguments["native_library"], native._library)
            self.assertIsNone(arguments["native_library_path"])
            self.assertEqual(arguments["timeout"], 1.0)
            self.assertEqual(arguments["batch_records"], 17)
            return connected

        with mock.patch(
            "l2flow_realtime.order_event_delta_control."
            "open_live_order_events",
            side_effect=connector,
        ):
            result = client.open_live_order_events(
                "/event.sock", batch_records=17
            )
        self.assertIs(result, connected)
        self.assertFalse(connected.closed)
        client.close()

    def test_client_helper_closes_reader_if_trade_date_changes(self):
        native = _ClientNative()
        client = L2FlowClient(native, stale_after_ns=None)
        connected = _FakeConnectedReader(_source_session())

        def connector(_path, **_arguments):
            native.current = _session_info(
                trade_date=_TRADE_DATE + 1
            )
            return connected

        with mock.patch(
            "l2flow_realtime.order_event_delta_control."
            "open_live_order_events",
            side_effect=connector,
        ):
            with self.assertRaises(StaleSessionError):
                client.open_live_order_events("/event.sock")
        self.assertTrue(connected.closed)
        client.close()

    def test_client_helper_closes_reader_if_catalog_changes(self):
        native = _ClientNative()
        client = L2FlowClient(native, stale_after_ns=None)
        connected = _FakeConnectedReader(
            LiveOrderEventDeltaSourceSession.from_session_info(
                native.current
            )
        )

        def connector(_path, **_arguments):
            native.current = _session_info(
                catalog_digest=b"D" * 32
            )
            return connected

        with mock.patch(
            "l2flow_realtime.order_event_delta_control."
            "open_live_order_events",
            side_effect=connector,
        ):
            with self.assertRaises(StaleSessionError):
                client.open_live_order_events("/event.sock")
        self.assertTrue(connected.closed)
        client.close()

    def test_source_session_rejects_noncanonical_catalog_identity(self):
        source = _source_session()
        for override in (
            {"catalog_digest": bytes(32)},
            {"catalog_generation": 2},
            {"catalog_version": 0},
            {"catalog_trade_date": _TRADE_DATE + 1},
            {"bound_count": _CATALOG_CAPACITY - 1},
            {"catalog_scope": 1},
            {"coverage_complete": False},
        ):
            with self.subTest(override=override):
                with self.assertRaises(ValueError):
                    replace(source, **override)


if __name__ == "__main__":
    unittest.main()
