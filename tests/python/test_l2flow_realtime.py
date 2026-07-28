from __future__ import annotations

import array
import ctypes
import importlib.util
import os
import socket
import struct
import time
import unittest
from pathlib import Path
from unittest import mock

from l2flow_realtime import (
    L2FlowClient,
    LatestBatch,
    LatestResult,
    LatestStatus,
    MarketEventKind,
    OptionalDependencyError,
    ServerState,
    SessionIdentity,
    SessionInfo,
    StaleSessionError,
    TickAction,
    TickBatch,
    TickFactorRunner,
    TickOverrunError,
)
from l2flow_realtime.control import (
    CONTROL_MAGIC,
    ControlSession,
    RESPONSE_BYTES,
    discover_session_fd,
    receive_session_fd,
)
from l2flow_realtime.models import Instrument
from l2flow_realtime.native import NativeReader, NativeTickRead
from l2flow_realtime.wire import (
    KLINE_BYTES,
    SNAPSHOT_BYTES,
    TICK_BYTES,
    _COMMON,
    _INSTRUMENT,
    _KLINE,
    _TICK_HEAD,
    parse_instrument,
    parse_kline,
    parse_snapshot,
    parse_tick,
)


def common_payload(
    payload_bytes,
    instrument_id,
    event_kind,
    tick_sequence,
    *,
    ingress_sequence=9,
):
    source_slot = {1: 0, 2: 1, 3: 2, 4: 3, 5: 3}[event_kind]
    values = (
        1,
        payload_bytes,
        instrument_id,
        0,
        7,
        ingress_sequence,
        tick_sequence,
        11,
        1_700_000_000_000_000_000,
        1_700_000_000_000_000_100,
        time.monotonic_ns(),
        34_200_000_000_000,
        0,
        0,
        31,
        20260727,
        93000000,
        0,
        source_slot,
        event_kind,
        1 if event_kind in (1, 2) else 2,
        1,
        1,
        1,
    )
    return _COMMON.pack(*values)


def snapshot_payload(instrument_id=7):
    result = bytearray(SNAPSHOT_BYTES)
    result[:128] = common_payload(
        SNAPSHOT_BYTES, instrument_id, 1, 0
    )
    struct.pack_into("<qiI", result, 128, 19, 2, 0)
    # last_price: DecimalValueV1(raw, normalized_p6, scale, valid, null)
    struct.pack_into("<qqBBB5x", result, 240, 1234, 12_340_000, 2, 1, 0)
    return bytes(result)


def tick_payload(sequence=1, instrument_id=7, kind=2, action=3):
    result = bytearray(TICK_BYTES)
    result[:128] = common_payload(
        TICK_BYTES,
        instrument_id,
        kind,
        sequence,
        ingress_sequence=sequence + 10,
    )
    _TICK_HEAD.pack_into(
        result,
        128,
        0x3,
        0,
        5,
        77,
        0,
        0,
        action,
        1,
        2,
        1,
        3,
        1,
        1,
        0,
    )
    struct.pack_into("<qqBBB5x", result, 168, 1234, 12_340_000, 2, 1, 0)
    struct.pack_into("<qBBB5x", result, 192, 100, 0, 1, 0)
    result[272] = ord("T")
    result[304] = ord("B")
    return bytes(result)


def kline_payload(instrument_id=7, window_id=60_000):
    values = [
        3,
        20260727,
        instrument_id,
        window_id,
        0,
        60_000_000_000,
        34_200_000_000_000,
        34_260_000_000_000,
        1_700_000_000_000_000_000,
        1_700_000_060_000_000_000,
        10_000_000,
        11_000_000,
        9_000_000,
        10_500_000,
        500,
        4,
        2,
        34_201_000_000_000,
        1,
        7,
        8,
        34_250_000_000_000,
        4,
        10,
        11,
        0,
        1,
        1,
    ]
    return _KLINE.pack(*values)


def session_info(
    *,
    state=ServerState.ACTIVE,
    heartbeat=None,
    capacity=8,
    contiguous=2,
):
    return SessionInfo(
        run_id=b"0123456789abcdef",
        session_epoch=5,
        registry_version=9,
        registry_sha256=b"x" * 32,
        tick_ring_capacity=capacity,
        tick_highest_published_sequence=contiguous,
        tick_contiguous_published_sequence=contiguous,
        kline_generation=3,
        heartbeat_monotonic_ns=(
            time.monotonic_ns() if heartbeat is None else heartbeat
        ),
        trade_date=20260727,
        server_state=state,
        flags=2,
        instrument_count=1,
        window_count=1,
    )


class FakeNative:
    def __init__(self, session=None):
        self.current_session = session or session_info()
        self.closed = False
        self.tick_payloads = {
            1: tick_payload(1),
            2: tick_payload(2, kind=4, action=1),
        }

    def session(self):
        return self.current_session

    def close(self):
        self.closed = True

    def instrument(self, instrument_id):
        if instrument_id != 7:
            raise ValueError("unknown")
        return Instrument(7, 1, 1, 1, 1, b"XSHG", b"600000")

    def latest_snapshots(self, instrument_ids):
        statuses = []
        payloads = []
        for value in instrument_ids:
            if value == 0:
                statuses.append(LatestStatus.INVALID_INSTRUMENT_ID)
                payloads.append(None)
            elif value != 7:
                if value == 6:
                    statuses.append(LatestStatus.NOT_YET_OBSERVED)
                    payloads.append(None)
                else:
                    statuses.append(LatestStatus.UNKNOWN_INSTRUMENT)
                    payloads.append(None)
            else:
                statuses.append(LatestStatus.AVAILABLE)
                payloads.append(snapshot_payload())
        return tuple(statuses), tuple(payloads)

    def latest_ticks(self, instrument_ids):
        statuses = tuple(
            LatestStatus.AVAILABLE
            if value == 7
            else LatestStatus.UNKNOWN_INSTRUMENT
            for value in instrument_ids
        )
        payloads = tuple(
            tick_payload(2, kind=4, action=1)
            if status is LatestStatus.AVAILABLE
            else None
            for status in statuses
        )
        return statuses, payloads

    def latest_klines(self, instrument_ids, window_ids):
        statuses = []
        payloads = []
        for instrument_id, window_id in zip(instrument_ids, window_ids):
            if instrument_id == 0:
                status = LatestStatus.INVALID_INSTRUMENT_ID
            elif instrument_id != 7:
                status = LatestStatus.UNKNOWN_INSTRUMENT
            elif window_id == 0:
                status = LatestStatus.INVALID_WINDOW_ID
            elif window_id != 60_000:
                status = LatestStatus.UNKNOWN_WINDOW
            else:
                status = LatestStatus.AVAILABLE
            statuses.append(status)
            payloads.append(
                kline_payload() if status is LatestStatus.AVAILABLE else None
            )
        return tuple(statuses), tuple(payloads)

    def read_ticks(self, expected_sequence, maximum_records):
        payloads = []
        sequence = expected_sequence
        while (
            len(payloads) < maximum_records
            and sequence in self.tick_payloads
        ):
            payloads.append(self.tick_payloads[sequence])
            sequence += 1
        return NativeTickRead(tuple(payloads), sequence, 0)


class WireModelTests(unittest.TestCase):
    def test_instrument_opaque_keys_and_reserved_validation(self):
        source = b"XSHG"
        security = b"\xff600000"
        row = _INSTRUMENT.pack(
            7,
            1,
            1,
            1,
            1,
            0,
            len(source),
            0,
            len(source),
            len(security),
            0,
            0,
            0,
            0,
        )
        instrument = parse_instrument(row, source, security)
        self.assertEqual(instrument.security_id_source, source)
        self.assertEqual(instrument.security_id, security)
        corrupt = bytearray(row)
        struct.pack_into("<I", corrupt, 20, 1)
        with self.assertRaisesRegex(Exception, "reserved"):
            parse_instrument(bytes(corrupt), source, security)

    def test_snapshot_keeps_exact_p6_integer(self):
        snapshot = parse_snapshot(snapshot_payload())
        self.assertEqual(snapshot.common.event_kind, MarketEventKind.SHANGHAI_SNAPSHOT)
        self.assertEqual(snapshot.last_price.p6, 12_340_000)
        self.assertIsInstance(snapshot.last_price.p6, int)

    def test_tick_preserves_mixed_kind_action_and_raw_bytes(self):
        tick = parse_tick(tick_payload(2, kind=4, action=1))
        self.assertEqual(tick.common.event_kind, MarketEventKind.SHENZHEN_ORDER)
        self.assertEqual(tick.action, TickAction.ADD)
        self.assertEqual(tick.raw_type, b"T")
        self.assertEqual(tick.raw_tick_flag, b"B")

    def test_common_rejects_event_kind_source_slot_mismatch(self):
        malformed = bytearray(tick_payload())
        malformed[112] = 0
        with self.assertRaisesRegex(Exception, "source_slot"):
            parse_tick(bytes(malformed))

    def test_kline_parser(self):
        bar = parse_kline(kline_payload())
        self.assertEqual(bar.window_id, 60_000)
        self.assertEqual(bar.close_price_p6, 10_500_000)


class ClientTests(unittest.TestCase):
    def test_latest_batch_order_duplicates_and_status(self):
        client = L2FlowClient(FakeNative())
        self.addCleanup(client.close)
        batch = client.get_latest_snapshots([7, 0, 6, 8, 7])
        self.assertEqual(
            [item.status for item in batch],
            [
                LatestStatus.AVAILABLE,
                LatestStatus.INVALID_INSTRUMENT_ID,
                LatestStatus.NOT_YET_OBSERVED,
                LatestStatus.UNKNOWN_INSTRUMENT,
                LatestStatus.AVAILABLE,
            ],
        )
        columns = batch.to_dict()
        self.assertEqual(columns["instrument_id"], [7, 0, 6, 8, 7])
        self.assertEqual(
            columns["last_price_p6"],
            [12_340_000, None, None, None, 12_340_000],
        )

    def test_kline_window_statuses_are_distinct(self):
        client = L2FlowClient(FakeNative())
        self.addCleanup(client.close)
        batch = client.get_latest_klines(
            [7, 7, 8], [0, 99, 60_000]
        )
        self.assertEqual(
            [item.status for item in batch],
            [
                LatestStatus.INVALID_WINDOW_ID,
                LatestStatus.UNKNOWN_WINDOW,
                LatestStatus.UNKNOWN_INSTRUMENT,
            ],
        )

    def test_cursor_is_contiguous_and_latest_starts_after_prefix(self):
        client = L2FlowClient(FakeNative())
        self.addCleanup(client.close)
        earliest = client.open_tick_cursor("earliest")
        batch = earliest.read(8)
        self.assertEqual(
            [tick.common.tick_stream_sequence for tick in batch], [1, 2]
        )
        self.assertEqual(earliest.next_sequence, 3)
        latest = client.open_tick_cursor("latest")
        self.assertEqual(latest.next_sequence, 3)
        self.assertEqual(len(latest.read(8)), 0)

    def test_overrun_does_not_advance_cursor(self):
        native = FakeNative()

        def overrun(expected_sequence, _maximum_records):
            raise TickOverrunError(expected_sequence, 2)

        native.read_ticks = overrun
        client = L2FlowClient(native)
        self.addCleanup(client.close)
        cursor = client.open_tick_cursor(1)
        with self.assertRaises(TickOverrunError):
            cursor.read(8)
        self.assertEqual(cursor.next_sequence, 1)

    def test_stale_live_heartbeat_fails_closed_but_clean_stop_is_readable(self):
        stale = time.monotonic_ns() - 10_000_000_000
        native = FakeNative(session_info(heartbeat=stale))
        with self.assertRaises(StaleSessionError):
            L2FlowClient(native, stale_after_ns=1_000_000)
        self.assertTrue(native.closed)
        stopped = L2FlowClient(
            FakeNative(
                session_info(
                    state=ServerState.STOPPED_CLEAN, heartbeat=stale
                )
            ),
            stale_after_ns=1_000_000,
        )
        stopped.close()

    def test_optional_dependencies_are_lazy_and_explicit(self):
        batch = LatestBatch(
            "tick", SessionIdentity(b"0123456789abcdef", 5), ()
        )
        original_import = __import__("importlib").import_module

        def missing(name, *args, **kwargs):
            if name in ("polars", "pyarrow"):
                raise ImportError(name)
            return original_import(name, *args, **kwargs)

        with mock.patch(
            "l2flow_realtime.batch.importlib.import_module",
            side_effect=missing,
        ):
            with self.assertRaises(OptionalDependencyError):
                batch.to_arrow()
            with self.assertRaises(OptionalDependencyError):
                batch.to_polars()

    @unittest.skipUnless(
        importlib.util.find_spec("pyarrow") is not None
        and importlib.util.find_spec("polars") is not None,
        "PyArrow and Polars optional dependencies are not installed",
    )
    def test_real_arrow_polars_adapters_and_factor_runner(self):
        import pyarrow as pa
        import polars as pl

        identity = SessionIdentity(b"0123456789abcdef", 5)
        ticks = tuple(
            parse_tick(tick_payload(sequence))
            for sequence in (1, 2)
        )
        tick_batch = TickBatch(identity, 1, 3, ticks)

        arrow_batch = tick_batch.to_arrow()
        self.assertIsInstance(arrow_batch, pa.RecordBatch)
        self.assertEqual(arrow_batch.num_rows, 2)
        self.assertEqual(
            arrow_batch.schema.field("price_p6").type,
            pa.int64(),
        )

        tick_frame = tick_batch.to_polars()
        self.assertEqual(
            tick_frame.schema["tick_stream_sequence"],
            pl.UInt64,
        )
        self.assertEqual(tick_frame["price_p6"].sum(), 24_680_000)

        snapshot = parse_snapshot(snapshot_payload())
        snapshot_batch = LatestBatch(
            "snapshot",
            identity,
            (
                LatestResult(
                    7,
                    LatestStatus.AVAILABLE,
                    snapshot,
                ),
            ),
        )
        snapshot_frame = snapshot_batch.to_polars()
        self.assertEqual(
            snapshot_frame["last_price_p6"].item(),
            12_340_000,
        )
        example_path = (
            Path(__file__).resolve().parents[2]
            / "python"
            / "examples"
            / "latest_snapshot_factor.py"
        )
        example_spec = importlib.util.spec_from_file_location(
            "latest_snapshot_factor_example",
            example_path,
        )
        self.assertIsNotNone(example_spec)
        self.assertIsNotNone(example_spec.loader)
        example_module = importlib.util.module_from_spec(example_spec)
        example_spec.loader.exec_module(example_module)
        factor_input = LatestBatch(
            "snapshot",
            identity,
            (
                LatestResult(
                    7,
                    LatestStatus.AVAILABLE,
                    snapshot,
                ),
                LatestResult(
                    8,
                    LatestStatus.NOT_YET_OBSERVED,
                    None,
                ),
            ),
        )
        factor_frame = example_module.calculate_placeholder_factor(
            factor_input,
            time.monotonic_ns(),
        )
        self.assertEqual(
            factor_frame["placeholder_factor_p6"].to_list(),
            [12_340_000, None],
        )
        self.assertEqual(
            factor_frame.schema["placeholder_factor_p6"],
            pl.Int64,
        )

        kline = parse_kline(kline_payload())
        kline_arrow = LatestBatch(
            "kline",
            identity,
            (
                LatestResult(
                    7,
                    LatestStatus.AVAILABLE,
                    kline,
                    60_000,
                ),
            ),
        ).to_arrow()
        self.assertEqual(
            kline_arrow.column("close_price_p6")[0].as_py(),
            10_500_000,
        )

        class SingleBatchCursor:
            session_identity = identity
            next_sequence = 1

            def read(self, maximum):
                if maximum < 2:
                    raise AssertionError("runner requested too few rows")
                self.next_sequence = 3
                return tick_batch

        cursor = SingleBatchCursor()
        runner = TickFactorRunner(
            cursor,
            lambda frame: frame.select(
                pl.col("price_p6").sum()
            ).item(),
            max_rows=2,
            max_latency=0,
            as_polars=True,
        )
        result = runner.run_once()
        self.assertIsNotNone(result)
        self.assertEqual(result.value, 24_680_000)

    def test_factor_runner_batches_by_row_limit(self):
        client = L2FlowClient(FakeNative())
        self.addCleanup(client.close)
        runner = TickFactorRunner(
            client.open_tick_cursor("earliest"),
            lambda batch: len(batch),
            max_rows=2,
            max_latency=0,
        )
        result = runner.run_once()
        self.assertIsNotNone(result)
        self.assertEqual(result.value, 2)
        self.assertEqual(result.input_batch.next_sequence, 3)

    def test_factor_runner_honors_latency_while_stream_is_nonempty(self):
        identity = SessionIdentity(b"0123456789abcdef", 5)

        class NonemptyCursor:
            session_identity = identity
            next_sequence = 1

            def read(self, _maximum):
                first = self.next_sequence
                self.next_sequence += 1
                return TickBatch(
                    identity,
                    first,
                    self.next_sequence,
                    (parse_tick(tick_payload(first)),),
                )

        clock_values = iter((10.0, 10.2))
        runner = TickFactorRunner(
            NonemptyCursor(),
            lambda batch: len(batch),
            max_rows=10,
            max_latency=0.1,
            _clock=lambda: next(clock_values),
        )
        result = runner.run_once()
        self.assertIsNotNone(result)
        self.assertEqual(result.value, 1)
        self.assertEqual(result.input_batch.next_sequence, 2)

    def test_factor_runner_rejects_unbounded_or_boolean_limits(self):
        client = L2FlowClient(FakeNative())
        self.addCleanup(client.close)
        cursor = client.open_tick_cursor("earliest")
        with self.assertRaises(ValueError):
            TickFactorRunner(
                cursor,
                lambda batch: batch,
                max_rows=1_048_577,
            )
        with self.assertRaises(TypeError):
            TickFactorRunner(
                cursor,
                lambda batch: batch,
                max_latency=True,
            )
        with self.assertRaises(TypeError):
            TickFactorRunner(
                cursor,
                lambda batch: batch,
                poll_interval=False,
            )

    def test_native_wrapper_rejects_values_before_ctypes_wrap(self):
        closed = NativeReader(None, ctypes.c_void_p())
        with self.assertRaises(ValueError):
            closed.instrument(0)
        with self.assertRaises(ValueError):
            closed.latest_snapshots([-1])
        with self.assertRaises(ValueError):
            closed.latest_klines([7], [0x1_0000_0000])
        with self.assertRaises(ValueError):
            closed.read_ticks(0, 1)


class ControlTests(unittest.TestCase):
    _RESPONSE = struct.Struct("<8sHHHHIIQQQQQ")

    def test_receive_exact_response_and_one_fd(self):
        left, right = socket.socketpair(
            socket.AF_UNIX, socket.SOCK_SEQPACKET
        )
        self.addCleanup(left.close)
        self.addCleanup(right.close)
        descriptor = os.open("/dev/null", os.O_RDONLY)
        self.addCleanup(lambda: os.close(descriptor))
        request_id = 91
        response = self._RESPONSE.pack(
            CONTROL_MAGIC,
            1,
            0,
            0,
            0,
            RESPONSE_BYTES,
            0,
            request_id,
            5,
            4096,
            0,
            0,
        )
        rights = array.array("i", [descriptor])
        right.sendmsg(
            [response],
            [(socket.SOL_SOCKET, socket.SCM_RIGHTS, rights)],
        )
        discovered = receive_session_fd(left, request_id)
        try:
            self.assertEqual(discovered.session_epoch, 5)
            self.assertFalse(os.get_inheritable(discovered.fd))
            self.assertEqual(os.fstat(discovered.fd).st_rdev, os.fstat(descriptor).st_rdev)
        finally:
            os.close(discovered.fd)

    def test_discovery_rejects_relative_socket_path(self):
        with self.assertRaisesRegex(ValueError, "absolute"):
            discover_session_fd("relative.sock")

    def test_connect_fake_native_seam_closes_received_python_fd(self):
        mapping = os.memfd_create("l2flow-python-test")
        os.ftruncate(mapping, 4096)
        factory_fd = []

        def factory(fd):
            os.fstat(fd)
            factory_fd.append(fd)
            return FakeNative()

        control = ControlSession(mapping, 11, 5, 4096)
        with mock.patch(
            "l2flow_realtime.client.discover_session_fd",
            return_value=control,
        ):
            client = L2FlowClient.connect(
                "/unused/control.sock", _native_factory=factory
            )
        with self.assertRaises(OSError):
            os.fstat(factory_fd[0])
        client.close()


if __name__ == "__main__":
    unittest.main()
