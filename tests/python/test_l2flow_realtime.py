from __future__ import annotations

import array
import csv
import ctypes
import fcntl
import gc
import importlib.util
import json
import os
import socket
import struct
import sys
import tempfile
import threading
import time
import unittest
import weakref
from pathlib import Path
from unittest import mock

from l2flow_realtime import (
    ClientClosedError,
    InstrumentKey,
    InstrumentLookupStatus,
    InstrumentTickDeltaCheckpoint,
    InstrumentTickDeltaCheckpointUnavailableError,
    InstrumentTickDeltaCursor,
    InstrumentTickDeltaPage,
    InstrumentTickDeltaSession,
    InstrumentTickColumns,
    InstrumentTickRollingStore,
    L2FlowClient,
    LatestBatch,
    LatestResult,
    LatestStatus,
    Market,
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
    TickProjectionFlag,
    WireFormatError,
)
import l2flow_realtime.instrument_delta as instrument_delta
import l2flow_realtime.native as native_module
from l2flow_realtime._fd_owner import _ReceivedPacket
from l2flow_realtime.control import (
    CONTROL_MAGIC,
    ControlSession,
    RESPONSE_BYTES,
    discover_session_fd,
    receive_session_fd,
)
from l2flow_realtime.history import (
    HISTORY_GENERATION_RECORD_COVERAGE_COMPLETE,
    READ_HISTORY_REQUEST_BYTES,
    HistoryCursor,
    HistoryGeneration,
    _validate_expected_generation,
)
from l2flow_realtime.models import Instrument
from l2flow_realtime.native import (
    NativeReader,
    NativeTickBlockRead,
    NativeTickRead,
)
from l2flow_realtime.wire import (
    KLINE_BYTES,
    SNAPSHOT_BYTES,
    TICK_BYTES,
    TICK_PROJECTION_RAW_TICK_FLAG_OMITTED,
    TICK_PROJECTION_RAW_TYPE_OMITTED,
    WIRE_MAJOR,
    WIRE_MINOR,
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


def tick_payload(
    sequence=1,
    instrument_id=7,
    kind=2,
    action=3,
    *,
    projection_flags=0,
    raw_type=None,
    raw_tick_flag=None,
):
    if raw_type is None:
        raw_type = b"T" if kind == 2 else b""
    if raw_tick_flag is None:
        raw_tick_flag = b"B" if kind == 2 else b""
    if len(raw_type) > 32 or len(raw_tick_flag) > 32:
        raise ValueError("test raw payload exceeds fixed wire capacity")
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
        projection_flags,
        5,
        77,
        0,
        0,
        action,
        1,
        2,
        1,
        3,
        len(raw_type),
        len(raw_tick_flag),
        0,
    )
    struct.pack_into("<qqBBB5x", result, 168, 1234, 12_340_000, 2, 1, 0)
    struct.pack_into("<qBBB5x", result, 192, 100, 0, 1, 0)
    result[272 : 272 + len(raw_type)] = raw_type
    result[304 : 304 + len(raw_tick_flag)] = raw_tick_flag
    return bytes(result)


def instrument_delta_tick_payload(
    tick_sequence,
    ingress_sequence,
    source_sequence,
    source_slot,
):
    kind = 2 if source_slot == 1 else 4
    payload = bytearray(
        tick_payload(
            tick_sequence,
            instrument_id=7,
            kind=kind,
            action=3 if source_slot == 1 else 1,
        )
    )
    common = list(_COMMON.unpack_from(payload))
    common[4] = source_sequence
    common[5] = ingress_sequence
    common[6] = tick_sequence
    common[14] = (30, 31, 32, 33)[source_slot]
    _COMMON.pack_into(payload, 0, *common)
    return bytes(payload)


def instrument_delta_checkpoint(
    *,
    generation=1,
    source_endpoints=(1, 6, 1, 5),
    counts=(0, 2, 0, 1),
    run_id=b"0123456789abcdef",
):
    ingress_exclusive = 1 + sum(
        endpoint - 1 for endpoint in source_endpoints
    )
    tick_exclusive = (
        1 + source_endpoints[1] - 1 + source_endpoints[3] - 1
    )
    return InstrumentTickDeltaCheckpoint(
        run_id=run_id,
        session_epoch=5,
        trade_date=20260727,
        instrument_count=1,
        registry_version=9,
        registry_sha256=b"x" * 32,
        instrument_id=7,
        registry_ordinal=0,
        generation=generation,
        input_identity_sha256=bytes([generation]) * 32,
        ingress_sequence_exclusive=ingress_exclusive,
        tick_stream_sequence_exclusive=tick_exclusive,
        recv_monotonic_cut_ns=generation * 100,
        source_stream_ids=(30, 31, 32, 33),
        source_sequence_exclusive=source_endpoints,
        instrument_tick_counts=counts,
        coverage_from_open=True,
        record_coverage_complete=True,
        field_complete=False,
        payload_projection=1,
    )


def instrument_delta_metadata_bytes(target, base=None):
    result = bytearray(
        instrument_delta.INSTRUMENT_TICK_DELTA_METADATA_BYTES_V2
    )
    base_kind = 1 if base is None else 2
    instrument_delta._METADATA_HEAD.pack_into(
        result,
        0,
        base_kind,
        instrument_delta.INSTRUMENT_TICK_DELTA_SOURCE_MASK_V2,
    )
    if base is not None:
        result[8:328] = instrument_delta._pack_checkpoint(base)
        base_counts = base.instrument_tick_counts
        ingress_begin = base.ingress_sequence_exclusive
        tick_begin = base.tick_stream_sequence_exclusive
    else:
        base_counts = (0, 0, 0, 0)
        ingress_begin = 1
        tick_begin = 1
    result[328:648] = instrument_delta._pack_checkpoint(target)
    delta_counts = tuple(
        target_count - base_count
        for target_count, base_count in zip(
            target.instrument_tick_counts, base_counts
        )
    )
    instrument_delta._METADATA_TAIL.pack_into(
        result,
        648,
        *delta_counts,
        sum(delta_counts),
        ingress_begin,
        target.ingress_sequence_exclusive,
        tick_begin,
        target.tick_stream_sequence_exclusive,
        target.flags,
        target.payload_projection,
        bytes(8),
    )
    return bytes(result)


def instrument_delta_page_bytes(metadata_bytes, payloads, page_index=0):
    ticks = tuple(parse_tick(payload) for payload in payloads)
    total_bytes = (
        instrument_delta.INSTRUMENT_TICK_DELTA_PAGE_HEADER_BYTES_V2
        + len(payloads) * TICK_BYTES
    )
    result = bytearray(total_bytes)
    instrument_delta._PAGE_PREFIX.pack_into(
        result,
        0,
        instrument_delta.INSTRUMENT_TICK_DELTA_PAGE_MAGIC_V2,
        WIRE_MAJOR,
        WIRE_MINOR,
        instrument_delta.INSTRUMENT_TICK_DELTA_PAGE_HEADER_BYTES_V2,
        instrument_delta.INSTRUMENT_TICK_DELTA_ENDIAN_MARKER_V2,
        0,
        total_bytes,
        page_index,
        len(payloads),
        TICK_BYTES,
        instrument_delta.INSTRUMENT_TICK_DELTA_PAGE_HEADER_BYTES_V2,
        ticks[0].common.ingress_sequence,
        ticks[-1].common.ingress_sequence,
        ticks[0].common.tick_stream_sequence,
        ticks[-1].common.tick_stream_sequence,
    )
    result[88:824] = metadata_bytes
    for index, payload in enumerate(payloads):
        offset = (
            instrument_delta
            .INSTRUMENT_TICK_DELTA_PAGE_HEADER_BYTES_V2
            + index * TICK_BYTES
        )
        result[offset : offset + TICK_BYTES] = payload
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


def assert_scm_fd_closed_on_base_exception(
    test_case, payload, receiver
):
    left, right = socket.socketpair(
        socket.AF_UNIX, socket.SOCK_SEQPACKET
    )
    test_case.addCleanup(left.close)
    test_case.addCleanup(right.close)
    descriptor = os.open("/dev/null", os.O_RDONLY)
    test_case.addCleanup(lambda: os.close(descriptor))
    rights = array.array("i", [descriptor])
    right.sendmsg(
        [payload],
        [(socket.SOL_SOCKET, socket.SCM_RIGHTS, rights)],
    )
    installed = []

    def interrupt(descriptor_value, _inheritable):
        installed.append(descriptor_value)
        raise KeyboardInterrupt("injected fd handoff interruption")

    with (
        mock.patch(
            "l2flow_realtime._fd_owner.os.set_inheritable",
            side_effect=interrupt,
        ),
        test_case.assertRaisesRegex(
            KeyboardInterrupt, "fd handoff interruption"
        ),
    ):
        receiver(left)
    test_case.assertEqual(len(installed), 1)
    with test_case.assertRaises(OSError):
        os.fstat(installed[0])


class FakeNative:
    def __init__(self, session=None):
        self.current_session = session or session_info()
        self.closed = False
        self.tick_payloads = {
            1: tick_payload(1),
            2: tick_payload(2, kind=4, action=1),
        }
        self.resolved_key_batches = []

    def session(self):
        return self.current_session

    def close(self):
        self.closed = True

    def instrument(self, instrument_id):
        if instrument_id != 7:
            raise ValueError("unknown")
        return Instrument(7, 1, 1, 1, 1, b"XSHG", b"600000")

    def resolve_instruments(self, keys):
        self.resolved_key_batches.append(tuple(keys))
        statuses = []
        instrument_ids = []
        for key in keys:
            if key.market not in (Market.SHANGHAI, Market.SHENZHEN):
                status = InstrumentLookupStatus.INVALID_MARKET
            elif not key.security_id:
                status = InstrumentLookupStatus.EMPTY_SECURITY_ID
            elif key == InstrumentKey(
                Market.SHANGHAI, b"XSHG", b"600000"
            ):
                status = InstrumentLookupStatus.FOUND
            else:
                status = InstrumentLookupStatus.UNKNOWN
            statuses.append(status)
            instrument_ids.append(
                7 if status is InstrumentLookupStatus.FOUND else 0
            )
        return tuple(statuses), tuple(instrument_ids)

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

    def read_tick_block(self, expected_sequence, maximum_records):
        result = self.read_ticks(expected_sequence, maximum_records)
        return NativeTickBlockRead(
            b"".join(result.payloads),
            len(result.payloads),
            result.next_sequence,
            result.observed_sequence,
        )


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
        tick = parse_tick(tick_payload(2, kind=2, action=1))
        self.assertEqual(
            tick.common.event_kind, MarketEventKind.SHANGHAI_TICK
        )
        self.assertEqual(tick.action, TickAction.ADD)
        self.assertEqual(tick.projection_flags, TickProjectionFlag.NONE)
        self.assertEqual(tick.raw_type, b"T")
        self.assertEqual(tick.raw_tick_flag, b"B")

        order = parse_tick(tick_payload(2, kind=4, action=1))
        self.assertEqual(
            order.common.event_kind, MarketEventKind.SHENZHEN_ORDER
        )
        self.assertEqual(order.raw_type, b"")
        self.assertEqual(order.raw_tick_flag, b"")

    def test_tick_projection_flags_are_explicit_and_strict(self):
        flags = (
            TICK_PROJECTION_RAW_TYPE_OMITTED
            | TICK_PROJECTION_RAW_TICK_FLAG_OMITTED
        )
        omitted = parse_tick(
            tick_payload(
                projection_flags=flags,
                raw_type=b"",
                raw_tick_flag=b"",
            )
        )
        self.assertTrue(omitted.raw_type_omitted)
        self.assertTrue(omitted.raw_tick_flag_omitted)
        self.assertEqual(
            omitted.projection_flags,
            TickProjectionFlag.RAW_TYPE_OMITTED
            | TickProjectionFlag.RAW_TICK_FLAG_OMITTED,
        )

        with self.assertRaisesRegex(WireFormatError, "unknown"):
            parse_tick(tick_payload(projection_flags=1 << 31))
        with self.assertRaisesRegex(WireFormatError, "conflicts"):
            parse_tick(
                tick_payload(
                    projection_flags=(
                        TICK_PROJECTION_RAW_TYPE_OMITTED
                    )
                )
            )
        with self.assertRaisesRegex(WireFormatError, "require Shanghai"):
            parse_tick(
                tick_payload(
                    kind=4,
                    projection_flags=(
                        TICK_PROJECTION_RAW_TYPE_OMITTED
                    ),
                    raw_type=b"",
                    raw_tick_flag=b"",
                )
            )
        with self.assertRaisesRegex(WireFormatError, "Shanghai raw"):
            parse_tick(
                tick_payload(
                    kind=4,
                    raw_type=b"T",
                    raw_tick_flag=b"",
                )
            )

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
    def test_exact_symbol_resolution_batch_and_point(self):
        native = FakeNative()
        client = L2FlowClient(native)
        self.addCleanup(client.close)
        exact = InstrumentKey(
            Market.SHANGHAI, b"XSHG", b"600000"
        )
        results = client.resolve_instruments(
            [
                exact,
                exact,
                InstrumentKey(
                    Market.SHANGHAI, b"XSHG ", b"600000"
                ),
                InstrumentKey(
                    Market.SHANGHAI, b"XSHG", b"600000\x00"
                ),
                InstrumentKey(Market.UNKNOWN, b"", b"600000"),
                InstrumentKey(Market.SHANGHAI, b"XSHG", b""),
            ]
        )
        self.assertEqual(
            [result.status for result in results],
            [
                InstrumentLookupStatus.FOUND,
                InstrumentLookupStatus.FOUND,
                InstrumentLookupStatus.UNKNOWN,
                InstrumentLookupStatus.UNKNOWN,
                InstrumentLookupStatus.INVALID_MARKET,
                InstrumentLookupStatus.EMPTY_SECURITY_ID,
            ],
        )
        self.assertEqual(
            [result.instrument_id for result in results],
            [7, 7, None, None, None, None],
        )
        point = client.resolve_instrument(
            Market.SHANGHAI, b"XSHG", b"600000"
        )
        self.assertTrue(point.found)
        self.assertEqual(point.instrument_id, 7)
        self.assertEqual(len(native.resolved_key_batches), 2)
        self.assertEqual(len(native.resolved_key_batches[-1]), 1)

        with self.assertRaisesRegex(TypeError, "exact bytes"):
            client.resolve_instrument(
                Market.SHANGHAI, "XSHG", b"600000"
            )
        with self.assertRaisesRegex(TypeError, "InstrumentKey"):
            client.resolve_instruments(
                [(Market.SHANGHAI, b"XSHG", b"600000")]
            )

    def test_symbol_resolution_empty_opaque_and_closed_boundaries(self):
        native = FakeNative()
        client = L2FlowClient(native)
        self.assertEqual(client.resolve_instruments(()), ())
        opaque = InstrumentKey(
            Market.SHANGHAI, b"\xff\x00", b"600000"
        )
        result = client.resolve_instruments((opaque,))[0]
        self.assertEqual(result.status, InstrumentLookupStatus.UNKNOWN)
        self.assertEqual(
            native.resolved_key_batches[-1],
            (opaque,),
        )
        client.close()
        with self.assertRaises(ClientClosedError):
            client.resolve_instruments((opaque,))

    def test_symbol_resolution_rejects_native_status_id_corruption(self):
        native = FakeNative()
        native.resolve_instruments = lambda _keys: (
            (InstrumentLookupStatus.FOUND,),
            (0,),
        )
        client = L2FlowClient(native)
        self.addCleanup(client.close)
        with self.assertRaises(WireFormatError):
            client.resolve_instruments(
                (
                    InstrumentKey(
                        Market.SHANGHAI, b"XSHG", b"600000"
                    ),
                )
            )

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

    @unittest.skipUnless(
        importlib.util.find_spec("numpy") is not None,
        "NumPy is optional",
    )
    def test_column_cursor_exposes_exact_wire_offsets_without_objects(self):
        client = L2FlowClient(FakeNative())
        self.addCleanup(client.close)
        cursor = client.open_tick_cursor("earliest")
        batch = cursor.read_columns(8)
        records = batch.numpy_records()
        self.assertEqual(len(batch), 2)
        self.assertEqual(batch.first_sequence, 1)
        self.assertEqual(batch.next_sequence, 3)
        self.assertEqual(cursor.next_sequence, 3)
        self.assertFalse(records.flags.writeable)
        self.assertEqual(records.dtype.itemsize, TICK_BYTES)
        self.assertEqual(records["record_schema_version"].tolist(), [1, 1])
        self.assertEqual(records["record_bytes"].tolist(), [336, 336])
        self.assertEqual(records["tick_stream_sequence"].tolist(), [1, 2])
        self.assertEqual(records["ingress_sequence"].tolist(), [11, 12])
        self.assertEqual(records["event_kind"].tolist(), [2, 4])
        self.assertEqual(records["projection_flags"].tolist(), [0, 0])
        self.assertEqual(records["action"].tolist(), [3, 1])
        self.assertEqual(records["price_raw"].tolist(), [1234, 1234])
        self.assertEqual(records["price_p6"].tolist(), [12_340_000] * 2)
        self.assertEqual(records["quantity_raw"].tolist(), [100, 100])

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

    def test_client_opens_session_anchored_instrument_delta(self):
        native = FakeNative()
        client = L2FlowClient(
            native,
            control_socket_path="/tmp/l2flow-control.sock",
            control_timeout=0.25,
        )
        self.addCleanup(client.close)
        delta_session = mock.Mock()
        with mock.patch(
            "l2flow_realtime.client."
            "open_instrument_tick_delta_session",
            return_value=delta_session,
        ) as opened:
            result = client.open_instrument_tick_delta_session(
                requested_page_records=17
            )
        self.assertIs(result, delta_session)
        opened.assert_called_once_with(
            "/tmp/l2flow-control.sock",
            requested_page_records=17,
            timeout=0.25,
            expected_run_id=b"0123456789abcdef",
            expected_session_epoch=5,
            expected_trade_date=20260727,
            expected_instrument_count=1,
            expected_registry_version=9,
            expected_registry_sha256=b"x" * 32,
        )

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

    def test_close_commits_client_state_after_native_close(self):
        native = FakeNative()
        native.close = mock.Mock(
            side_effect=(RuntimeError("close failed"), None)
        )
        client = L2FlowClient(native)
        client._instrument_cache[7] = object()

        with self.assertRaisesRegex(RuntimeError, "close failed"):
            client.close()
        self.assertFalse(client.closed)
        self.assertIn(7, client._instrument_cache)

        client.close()
        client.close()
        self.assertTrue(client.closed)
        self.assertEqual(client._instrument_cache, {})
        self.assertEqual(native.close.call_count, 2)


class NativeReaderOwnershipTests(unittest.TestCase):
    @staticmethod
    def _library():
        library = mock.Mock()

        def open_reader(_fd, output):
            ctypes.cast(
                output, ctypes.POINTER(ctypes.c_void_p)
            ).contents.value = 0x1234
            return 0

        library.l2flow_shm_reader_open_fd_v1.side_effect = open_reader
        return library

    def _open(self, reader_type=NativeReader):
        library = self._library()
        with mock.patch.object(native_module, "_bind_library"):
            reader = reader_type.open_fd(0, library=library)
        return reader, library

    def test_explicit_close_is_idempotent_and_releases_tick_buffer(self):
        reader, library = self._open()
        reader._tick_output = (ctypes.c_uint8 * 4096)()
        reader._tick_output_bytes = 4096

        reader.close()
        reader.close()

        self.assertTrue(reader.closed)
        self.assertIsNone(reader._tick_output)
        self.assertEqual(reader._tick_output_bytes, 0)
        library.l2flow_shm_reader_close_v1.assert_called_once()

    def test_abandoned_client_releases_its_native_reader(self):
        reader, library = self._open()
        reader.session = mock.Mock(return_value=session_info())
        client = L2FlowClient(reader)
        reader_reference = weakref.ref(reader)
        client_reference = weakref.ref(client)

        del reader
        del client
        gc.collect()

        self.assertIsNone(client_reference())
        self.assertIsNone(reader_reference())
        library.l2flow_shm_reader_close_v1.assert_called_once()

    def test_open_rolls_back_handle_when_reader_construction_fails(self):
        class FailingReader(NativeReader):
            def __init__(self, library, handle):
                super().__init__(library, handle)
                raise RuntimeError("reader construction failed")

        library = self._library()
        with (
            mock.patch.object(native_module, "_bind_library"),
            self.assertRaisesRegex(
                RuntimeError, "reader construction failed"
            ),
        ):
            FailingReader.open_fd(0, library=library)
        gc.collect()

        library.l2flow_shm_reader_close_v1.assert_called_once()


class HistoryProtocolTests(unittest.TestCase):
    def test_recv_packet_closes_scm_fd_on_baseexception(self):
        from l2flow_realtime import history as history_module

        payload = bytes(8)
        assert_scm_fd_closed_on_base_exception(
            self,
            payload,
            lambda channel: history_module._recv_packet(
                channel, len(payload)
            ),
        )

    def test_read_baseexception_closes_packet_and_cursor(self):
        from l2flow_realtime import history as history_module

        client_socket, server_socket = socket.socketpair(
            socket.AF_UNIX, socket.SOCK_SEQPACKET
        )
        self.addCleanup(server_socket.close)
        cursor = HistoryCursor(
            _channel=client_socket,
            generation=self.generation(),
            requested_page_records=1,
        )
        descriptor = os.open("/dev/null", os.O_RDONLY)
        packet = _ReceivedPacket(
            bytes(history_module.READ_HISTORY_RESPONSE_BYTES),
            (descriptor,),
        )
        with (
            mock.patch.object(history_module, "_send_packet"),
            mock.patch.object(
                history_module, "_recv_packet", return_value=packet
            ),
            mock.patch.object(
                history_module,
                "_validate_response_prefix",
                side_effect=KeyboardInterrupt("injected page validation"),
            ),
            self.assertRaisesRegex(
                KeyboardInterrupt, "page validation"
            ),
        ):
            cursor.read()
        self.assertTrue(cursor.closed)
        self.assertTrue(packet.closed)
        with self.assertRaises(OSError):
            os.fstat(descriptor)

    @staticmethod
    def generation():
        return HistoryGeneration(
            run_id=b"0123456789abcdef",
            session_epoch=5,
            generation=1,
            trade_date=20260727,
            instrument_id=7,
            registry_ordinal=0,
            instrument_count=1,
            ingress_sequence_exclusive=1,
            recv_monotonic_cut_ns=123,
            registry_version=3,
            registry_sha256=b"r" * 32,
            input_identity_sha256=b"i" * 32,
            source_stream_ids=(31, 32, 33, 34),
            source_sequence_exclusive=(1, 1, 1, 1),
            source_record_counts=(0, 0, 0, 0),
            total_record_count=0,
            flags=HISTORY_GENERATION_RECORD_COVERAGE_COMPLETE,
            payload_projection=1,
        )

    @staticmethod
    def populated_generation():
        return HistoryGeneration(
            run_id=b"0123456789abcdef",
            session_epoch=5,
            generation=2,
            trade_date=20260727,
            instrument_id=7,
            registry_ordinal=0,
            instrument_count=1,
            ingress_sequence_exclusive=2,
            recv_monotonic_cut_ns=123,
            registry_version=3,
            registry_sha256=b"r" * 32,
            input_identity_sha256=b"i" * 32,
            source_stream_ids=(31, 32, 33, 34),
            source_sequence_exclusive=(1, 2, 1, 1),
            source_record_counts=(0, 1, 0, 0),
            total_record_count=1,
            flags=HISTORY_GENERATION_RECORD_COVERAGE_COMPLETE,
            payload_projection=1,
        )

    @staticmethod
    def history_tick_page(generation):
        from l2flow_realtime import history as history_module

        payload = bytearray(tick_payload(1, instrument_id=7))
        struct.pack_into("<Q", payload, 16, 1)
        struct.pack_into("<Q", payload, 24, 1)
        struct.pack_into("<I", payload, 96, 32)
        descriptor_offset = history_module.HISTORY_PAGE_HEADER_BYTES
        snapshot_offset = (
            descriptor_offset + history_module.HISTORY_DESCRIPTOR_BYTES
        )
        tick_offset = snapshot_offset
        total_bytes = tick_offset + TICK_BYTES
        page = bytearray(total_bytes)
        history_module._PAGE_PREFIX.pack_into(
            page,
            0,
            history_module.HISTORY_MAGIC,
            WIRE_MAJOR,
            WIRE_MINOR,
            history_module.HISTORY_PAGE_HEADER_BYTES,
            history_module.HISTORY_ENDIAN_MARKER,
            0,
            total_bytes,
            0,
            1,
            history_module.HISTORY_DESCRIPTOR_BYTES,
            descriptor_offset,
            snapshot_offset,
            0,
            SNAPSHOT_BYTES,
            tick_offset,
            1,
            TICK_BYTES,
            1,
            1,
        )
        history_module._GENERATION_INFO.pack_into(
            page,
            104,
            generation.run_id,
            generation.session_epoch,
            generation.generation,
            generation.trade_date,
            generation.instrument_id,
            generation.registry_ordinal,
            generation.instrument_count,
            generation.ingress_sequence_exclusive,
            generation.recv_monotonic_cut_ns,
            generation.registry_version,
            generation.registry_sha256,
            generation.input_identity_sha256,
            *generation.source_stream_ids,
            *generation.source_sequence_exclusive,
            *generation.source_record_counts,
            generation.total_record_count,
            generation.flags,
            generation.payload_projection,
            0,
            0,
            0,
        )
        history_module._DESCRIPTOR.pack_into(
            page,
            descriptor_offset,
            1,
            1,
            1,
            0,
            0,
            history_module.HISTORY_PAYLOAD_TICK,
            int(MarketEventKind.SHANGHAI_TICK),
            1,
            0,
            0,
        )
        page[tick_offset:] = payload
        return bytes(page)

    def test_history_page_refactor_preserves_decode_result_and_boundary(self):
        from l2flow_realtime import history as history_module

        if not hasattr(os, "memfd_create"):
            self.skipTest("memfd_create is unavailable")
        generation = self.populated_generation()
        page = self.history_tick_page(generation)
        layout = history_module._validate_page_header(
            page,
            expected_bytes=len(page),
            expected_records=1,
            expected_page_index=0,
            generation=generation,
            prior_ingress_sequence=0,
        )
        expected = history_module._decode_page_objects(
            page,
            layout=layout,
            generation=generation,
            prior_ingress_sequence=0,
            prior_source_sequences=(0, 0, 0, 0),
        )

        writable_fd = os.memfd_create(
            "l2flow-history-python-test",
            getattr(os, "MFD_CLOEXEC", 0x0001)
            | getattr(os, "MFD_ALLOW_SEALING", 0x0002),
        )
        self.addCleanup(lambda: os.close(writable_fd))
        self.assertEqual(os.write(writable_fd, page), len(page))
        seals = (
            getattr(fcntl, "F_SEAL_WRITE", 0x0008)
            | getattr(fcntl, "F_SEAL_GROW", 0x0004)
            | getattr(fcntl, "F_SEAL_SHRINK", 0x0002)
            | getattr(fcntl, "F_SEAL_SEAL", 0x0001)
        )
        fcntl.fcntl(
            writable_fd,
            getattr(fcntl, "F_ADD_SEALS", 1033),
            seals,
        )
        readonly_fd = os.open(
            f"/proc/self/fd/{writable_fd}",
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0),
        )
        self.addCleanup(lambda: os.close(readonly_fd))
        with mock.patch.object(
            history_module,
            "_decode_page_objects",
            wraps=history_module._decode_page_objects,
        ) as decode:
            actual = history_module._parse_page(
                readonly_fd,
                expected_bytes=len(page),
                expected_records=1,
                expected_page_index=0,
                generation=generation,
                prior_ingress_sequence=0,
                prior_source_sequences=(0, 0, 0, 0),
            )
        self.assertEqual(actual, expected)
        decode.assert_called_once()

        malformed = bytearray(page)
        malformed[0] ^= 0xFF
        with self.assertRaisesRegex(WireFormatError, "magic mismatch"):
            history_module._validate_page_header(
                malformed,
                expected_bytes=len(malformed),
                expected_records=1,
                expected_page_index=0,
                generation=generation,
                prior_ingress_sequence=0,
            )

    def test_close_interrupts_timeout_none_blocking_read(self):
        client_socket, server_socket = socket.socketpair(
            socket.AF_UNIX, socket.SOCK_SEQPACKET
        )
        self.addCleanup(server_socket.close)
        cursor = HistoryCursor(
            _channel=client_socket,
            generation=self.generation(),
            requested_page_records=1,
        )
        self.addCleanup(cursor.close)
        read_finished = threading.Event()
        close_finished = threading.Event()
        read_errors = []
        read_pages = []
        close_errors = []

        def blocking_read():
            try:
                read_pages.append(cursor.read())
            except BaseException as error:  # preserve the worker failure
                read_errors.append(error)
            finally:
                read_finished.set()

        def close_cursor():
            try:
                cursor.close()
            except BaseException as error:  # preserve the worker failure
                close_errors.append(error)
            finally:
                close_finished.set()

        reader = threading.Thread(target=blocking_read, daemon=True)
        reader.start()
        server_socket.settimeout(1.0)
        request = server_socket.recv(READ_HISTORY_REQUEST_BYTES)
        if not request:
            read_finished.wait(0.2)
        self.assertEqual(
            len(request),
            READ_HISTORY_REQUEST_BYTES,
            f"reader exited before sending a request: {read_errors!r}",
        )

        closer = threading.Thread(target=close_cursor, daemon=True)
        closer.start()
        closed_without_peer_response = close_finished.wait(1.0)
        if not closed_without_peer_response:
            # Ensure a regression fails promptly instead of leaving unittest
            # blocked forever behind the cursor lock.
            server_socket.close()
            close_finished.wait(1.0)
            read_finished.wait(1.0)
            self.fail(
                "close() did not interrupt a timeout=None history read"
            )

        self.assertTrue(
            read_finished.wait(1.0),
            "history read did not exit after concurrent close",
        )
        reader.join(1.0)
        closer.join(1.0)
        self.assertFalse(reader.is_alive())
        self.assertFalse(closer.is_alive())
        self.assertFalse(close_errors)
        self.assertEqual(len(read_errors), 1)
        self.assertFalse(read_pages)
        self.assertTrue(cursor.closed)
        self.assertFalse(cursor.done)
        cursor.close()  # repeated close remains harmless

    def test_expected_generation_identity_mismatch_is_stale(self):
        generation = self.generation()
        expected = {
            "instrument_id": generation.instrument_id,
            "expected_run_id": generation.run_id,
            "expected_session_epoch": generation.session_epoch,
            "expected_trade_date": generation.trade_date,
            "expected_instrument_count": generation.instrument_count,
            "expected_registry_version": generation.registry_version,
            "expected_registry_sha256": generation.registry_sha256,
        }
        _validate_expected_generation(generation, **expected)
        mismatches = {
            "expected_run_id": b"fedcba9876543210",
            "expected_session_epoch": generation.session_epoch + 1,
            "expected_trade_date": generation.trade_date + 1,
            "expected_instrument_count": generation.instrument_count + 1,
            "expected_registry_version": generation.registry_version + 1,
            "expected_registry_sha256": b"s" * 32,
        }
        for field, value in mismatches.items():
            with self.subTest(field=field):
                changed = dict(expected)
                changed[field] = value
                with self.assertRaises(StaleSessionError):
                    _validate_expected_generation(
                        generation, **changed
                    )


class HistoryBenchmarkToolTests(unittest.TestCase):
    @staticmethod
    def module():
        tool_path = (
            Path(__file__).resolve().parents[2]
            / "tools"
            / "benchmark_instrument_history_stages.py"
        )
        module_name = "benchmark_instrument_history_stages_test"
        spec = importlib.util.spec_from_file_location(module_name, tool_path)
        if spec is None or spec.loader is None:
            raise AssertionError("cannot load history benchmark tool")
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        return module

    def test_history_benchmark_arguments_and_csv_contract(self):
        benchmark = self.module()
        with tempfile.TemporaryDirectory() as directory:
            output_dir = Path(directory)
            args = benchmark._parse_args(
                [
                    "/tmp/l2flow-history.sock",
                    str(Path(__file__).resolve().parents[2] / "python"),
                    str(output_dir),
                    "--instrument-id",
                    "7",
                    "--benchmark-run-id",
                    "91",
                    "--page-records",
                    "64",
                    "--rounds",
                    "2",
                    "--warmups",
                    "0",
                    "--records",
                    "17",
                ]
            )
            self.assertEqual(args.instrument_id, 7)
            self.assertEqual(args.benchmark_run_id, 91)
            self.assertEqual(args.page_records, 64)
            self.assertEqual(args.rounds, 2)
            self.assertEqual(args.warmup_rounds, 0)
            self.assertEqual(args.expected_records, 17)

            page_path = output_dir / "client_pages.csv"
            row = {name: 0 for name in benchmark.CLIENT_PAGE_FIELDS}
            row.update(
                {
                    "benchmark_run_id": 91,
                    "scan_id": 0,
                    "phase": "measure",
                    "read_request_id": 123,
                }
            )
            benchmark._write_csv(
                page_path,
                benchmark.CLIENT_PAGE_FIELDS,
                [row],
            )
            with page_path.open(newline="", encoding="utf-8") as source:
                rows = list(csv.DictReader(source))
            self.assertEqual(
                tuple(rows[0]),
                benchmark.CLIENT_PAGE_FIELDS,
            )
            self.assertEqual(rows[0]["benchmark_run_id"], "91")
            self.assertEqual(rows[0]["read_request_id"], "123")


class HistoryBenchmarkAnalyzerTests(unittest.TestCase):
    @staticmethod
    def module():
        analyzer_path = (
            Path(__file__).resolve().parents[2]
            / "benchmarks"
            / "analyze_single_instrument_history_stages.py"
        )
        module_name = "analyze_single_instrument_history_stages_test"
        spec = importlib.util.spec_from_file_location(
            module_name, analyzer_path
        )
        if spec is None or spec.loader is None:
            raise AssertionError("cannot load history benchmark analyzer")
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        return module

    @staticmethod
    def _rows():
        config = [
            {
                "schema_version": 1,
                "benchmark_run_id": 42,
                "records": 2,
                "page_records": 2,
                "rounds": 2,
                "warmup_rounds": 1,
                "snapshot_every": 0,
                "seed": 7,
                "workload": "tick_only",
                "trade_date": 20260728,
                "instrument_id": 1001,
            }
        ]
        pages = []
        scans = []
        servers = []
        phases = (("warmup", 0), ("measure", 0), ("measure", 1))
        for scan_id, (phase, round_index) in enumerate(phases):
            open_request_id = 100 + scan_id
            read_request_id = 1000 + scan_id * 10
            pages.extend(
                [
                    {
                        "schema_version": 1,
                        "benchmark_run_id": 42,
                        "scan_id": scan_id,
                        "phase": phase,
                        "round_index": round_index,
                        "open_request_id": open_request_id,
                        "read_request_id": read_request_id,
                        "generation": 1,
                        "instrument_id": 1001,
                        "page_index": 0,
                        "eof": 0,
                        "record_count": 2,
                        "snapshot_count": 0,
                        "tick_count": 2,
                        "page_mapping_bytes": 4848,
                        "read_wall_ns": 1000,
                        "object_decode_ns": 200,
                        "column_build_ns": 100,
                        "cumulative_record_count": 2,
                        "cumulative_source_count_0": 0,
                        "cumulative_source_count_1": 2,
                        "cumulative_source_count_2": 0,
                        "cumulative_source_count_3": 0,
                        "first_ingress_sequence": 1,
                        "last_ingress_sequence": 2,
                        "ingress_sequence_sum": 3,
                        "ingress_sequence_xor": 3,
                    },
                    {
                        "schema_version": 1,
                        "benchmark_run_id": 42,
                        "scan_id": scan_id,
                        "phase": phase,
                        "round_index": round_index,
                        "open_request_id": open_request_id,
                        "read_request_id": read_request_id + 1,
                        "generation": 1,
                        "instrument_id": 1001,
                        "page_index": 1,
                        "eof": 1,
                        "record_count": 0,
                        "snapshot_count": 0,
                        "tick_count": 0,
                        "page_mapping_bytes": 0,
                        "read_wall_ns": 50,
                        "object_decode_ns": 0,
                        "column_build_ns": 0,
                        "cumulative_record_count": 2,
                        "cumulative_source_count_0": 0,
                        "cumulative_source_count_1": 2,
                        "cumulative_source_count_2": 0,
                        "cumulative_source_count_3": 0,
                        "first_ingress_sequence": 0,
                        "last_ingress_sequence": 0,
                        "ingress_sequence_sum": 0,
                        "ingress_sequence_xor": 0,
                    },
                ]
            )
            scans.append(
                {
                    "schema_version": 1,
                    "benchmark_run_id": 42,
                    "scan_id": scan_id,
                    "phase": phase,
                    "round_index": round_index,
                    "open_request_id": open_request_id,
                    "generation": 1,
                    "instrument_id": 1001,
                    "requested_page_records": 2,
                    "open_wall_ns": 100,
                    "scan_wall_ns": 1300,
                    "data_page_read_wall_ns": 1000,
                    "eof_read_wall_ns": 50,
                    "object_decode_ns": 200,
                    "column_build_ns": 100,
                    "data_page_count": 1,
                    "read_request_count": 2,
                    "page_mapping_bytes": 4848,
                    "total_record_count": 2,
                    "source_record_count_0": 0,
                    "source_record_count_1": 2,
                    "source_record_count_2": 0,
                    "source_record_count_3": 0,
                    "generation_total_record_count": 2,
                    "generation_source_record_count_0": 0,
                    "generation_source_record_count_1": 2,
                    "generation_source_record_count_2": 0,
                    "generation_source_record_count_3": 0,
                    "first_ingress_sequence": 1,
                    "last_ingress_sequence": 2,
                    "ingress_sequence_sum": 3,
                    "ingress_sequence_xor": 3,
                    "eof_seen": 1,
                }
            )
            servers.append(
                {
                    "schema_version": 1,
                    "benchmark_run_id": 42,
                    "open_request_id": open_request_id,
                    "read_request_id": read_request_id,
                    "generation": 1,
                    "instrument_id": 1001,
                    "page_index": 0,
                    "record_count": 2,
                    "snapshot_count": 0,
                    "tick_count": 2,
                    "page_mapping_bytes": 4848,
                    "clock_read_failures": 0,
                    "cursor_read_ns": 10,
                    "classify_layout_ns": 10,
                    "memfd_prepare_ns": 50,
                    "projection_ns": 20,
                    "memfd_finalize_ns": 50,
                    "build_total_ns": 150,
                    "token_ns": 10,
                    "send_ns": 10,
                }
            )
        return config, pages, scans, servers

    @staticmethod
    def _write(module, directory, rows):
        config, pages, scans, servers = rows
        module._write_csv(
            directory / "benchmark_config.csv",
            module.CONFIG_FIELDS,
            config,
        )
        module._write_csv(
            directory / "client_pages.csv",
            module.CLIENT_PAGE_FIELDS,
            pages,
        )
        module._write_csv(
            directory / "client_scans.csv",
            module.CLIENT_SCAN_FIELDS,
            scans,
        )
        module._write_csv(
            directory / "server_pages.csv",
            module.SERVER_PAGE_FIELDS,
            servers,
        )

    def test_analyzer_validates_all_phases_and_excludes_warmup(self):
        analyzer = self.module()
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            self._write(analyzer, directory, self._rows())
            result = analyzer.analyze(directory)
            self.assertEqual(result["benchmark"]["measured_scans"], 2)
            self.assertEqual(result["aggregate"]["total_records"], 4)
            self.assertEqual(
                result["distributions"]["scan_wall_ns"]["samples"], 2
            )
            self.assertEqual(
                result["benchmark"]["config"]["warmup_rounds"], 1
            )

    def test_analyzer_rejects_identity_cumulative_fixture_and_join_tampering(self):
        analyzer = self.module()
        cases = (
            "page_identity",
            "page_cumulative",
            "fixture_fingerprint",
            "missing_warmup_server",
            "negative_data_residual",
        )
        for case in cases:
            with self.subTest(case=case), tempfile.TemporaryDirectory() as temporary:
                config, pages, scans, servers = self._rows()
                if case == "page_identity":
                    pages[2]["scan_id"] = 99
                elif case == "page_cumulative":
                    pages[2]["cumulative_record_count"] = 1
                elif case == "fixture_fingerprint":
                    pages[2]["ingress_sequence_sum"] = 4
                    scans[1]["ingress_sequence_sum"] = 4
                elif case == "missing_warmup_server":
                    del servers[0]
                elif case == "negative_data_residual":
                    servers[1]["memfd_prepare_ns"] = 800
                    servers[1]["memfd_finalize_ns"] = 300
                    servers[1]["build_total_ns"] = 1150
                    scans[1]["scan_wall_ns"] = 3000
                directory = Path(temporary)
                self._write(
                    analyzer,
                    directory,
                    (config, pages, scans, servers),
                )
                with self.assertRaises(ValueError):
                    analyzer.analyze(directory)


class HistoryBenchmarkSummarizerTests(unittest.TestCase):
    @staticmethod
    def module():
        summarizer_path = (
            Path(__file__).resolve().parents[2]
            / "benchmarks"
            / "summarize_single_instrument_history_runs.py"
        )
        module_name = "summarize_single_instrument_history_runs_test"
        spec = importlib.util.spec_from_file_location(
            module_name, summarizer_path
        )
        if spec is None or spec.loader is None:
            raise AssertionError("cannot load history benchmark summarizer")
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        return module

    @staticmethod
    def _document(run_id):
        scans = []
        for round_index in range(2):
            scans.append(
                {
                    "benchmark_run_id": run_id,
                    "generation": 1,
                    "instrument_id": 1001,
                    "scan_id": round_index + 1,
                    "round_index": round_index,
                    "workload": "tick_only",
                    "record_count": 2,
                    "snapshot_count": 0,
                    "tick_count": 2,
                    "scan_wall_ns": 1000,
                    "memfd_ns": 0,
                    "object_decode_ns": 200,
                    "column_build_ns": 100,
                    "requested_stage_ns": 300,
                    "requested_residual_full_ns": 700,
                }
            )
        return {
            "schema_version": 1,
            "benchmark": {
                "benchmark_run_id": run_id,
                "workload": "tick_only",
                "measured_scans": 2,
                "records_per_scan": 2,
                "pages_per_scan": [1],
                "requested_page_records": [2],
                "snapshot_count_per_scan": [0],
                "tick_count_per_scan": [2],
                "config": {
                    "benchmark_run_id": run_id,
                    "records": 2,
                    "page_records": 2,
                    "rounds": 2,
                    "warmup_rounds": 1,
                    "snapshot_every": 0,
                    "seed": run_id,
                    "workload": "tick_only",
                    "trade_date": 20260728,
                    "instrument_id": 1001,
                },
            },
            "definitions": {"distribution_quantiles": "R-7"},
            "aggregate": {
                "total_records": 4,
                "total_scan_wall_ns": 2000,
                "total_requested_stage_ns": 600,
                "requested_residual_full_ns": 1400,
                "requested_residual_share_full_scan": 0.7,
                "stages": [
                    {
                        "stage": "memfd",
                        "total_ns": 0,
                        "ns_per_record": 0.0,
                        "share_full_scan": 0.0,
                        "share_requested_mix": 0.0,
                    },
                    {
                        "stage": "object_decode",
                        "total_ns": 400,
                        "ns_per_record": 100.0,
                        "share_full_scan": 0.2,
                        "share_requested_mix": 2.0 / 3.0,
                    },
                    {
                        "stage": "column_build",
                        "total_ns": 200,
                        "ns_per_record": 50.0,
                        "share_full_scan": 0.1,
                        "share_requested_mix": 1.0 / 3.0,
                    },
                ],
            },
            "scans": scans,
            "sources": {
                "fixture.cpp": {
                    "bytes": 1,
                    "sha256": "a" * 64,
                }
            },
        }

    @staticmethod
    def _write(root, documents):
        for document in documents:
            run_id = document["benchmark"]["benchmark_run_id"]
            directory = root / f"run-{run_id}"
            directory.mkdir()
            (directory / "analysis.json").write_text(
                json.dumps(document, sort_keys=True) + "\n",
                encoding="utf-8",
            )

    def test_summarizer_reconciles_runs_and_accepts_zero_stage(self):
        summarizer = self.module()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self._write(
                root,
                (self._document(41), self._document(42)),
            )
            result = summarizer.summarize(root, 2)
            workload = result["workloads"][0]
            self.assertEqual(workload["process_runs"], 2)
            self.assertEqual(workload["measured_scans"], 4)
            self.assertEqual(workload["scan_wall_p50_ns"], 1000.0)
            stages = {
                stage["stage"]: stage for stage in workload["stages"]
            }
            self.assertEqual(stages["memfd"]["total_ns"], 0)
            csv_path = root / "comparison.csv"
            json_path = root / "comparison.json"
            self.assertTrue(csv_path.is_file())
            self.assertTrue(json_path.is_file())
            completed = json.loads(json_path.read_text(encoding="utf-8"))
            self.assertEqual(
                completed["outputs"]["comparison_csv"]["sha256"],
                summarizer._sha256(csv_path),
            )
            self.assertFalse(list(root.glob(".comparison.*.tmp")))

    def test_summarizer_rejects_scan_and_aggregate_tampering(self):
        summarizer = self.module()
        cases = (
            "scan_count",
            "scan_wall",
            "scan_records",
            "stage_share",
            "requested_total",
        )
        for case in cases:
            with self.subTest(case=case), tempfile.TemporaryDirectory() as temporary:
                first = self._document(41)
                second = self._document(42)
                if case == "scan_count":
                    second["scans"].pop()
                elif case == "scan_wall":
                    second["scans"][0]["scan_wall_ns"] = 999
                elif case == "scan_records":
                    second["scans"][0]["record_count"] = 3
                elif case == "stage_share":
                    second["aggregate"]["stages"][1][
                        "share_full_scan"
                    ] = 0.9
                elif case == "requested_total":
                    second["aggregate"]["total_requested_stage_ns"] = 601
                root = Path(temporary)
                self._write(root, (first, second))
                with self.assertRaises(ValueError):
                    summarizer.summarize(root, 2)


class FakeInstrumentDeltaCursor:
    def __init__(self, base_checkpoint, target_checkpoint, pages):
        self.instrument_id = target_checkpoint.instrument_id
        self.base_checkpoint = base_checkpoint
        self._target_checkpoint = target_checkpoint
        self._pages = tuple(pages)
        self.done = False
        self.closed = False

    @property
    def verified_checkpoint(self):
        if not self.done:
            raise InstrumentTickDeltaCheckpointUnavailableError(
                "checkpoint unavailable before EOF"
            )
        return self._target_checkpoint

    def pages(self):
        for page in self._pages:
            if page.eof:
                self.done = True
            yield page

    def close(self):
        self.closed = True


class RecordingRollingFactor:
    factor_schema = "factor-v1"

    def __init__(self, fail_call=None):
        self.fail_call = fail_call
        self.changes = []

    def update(self, state, change):
        self.changes.append(change)
        if len(self.changes) == self.fail_call:
            raise RuntimeError("factor failure")
        return state + len(change.appended), (
            len(change.appended),
            len(change.evicted),
        )


class InstrumentDeltaV2Tests(unittest.TestCase):
    def test_recv_packet_closes_scm_fd_on_baseexception(self):
        payload = bytes(8)
        assert_scm_fd_closed_on_base_exception(
            self,
            payload,
            lambda channel: instrument_delta._recv_packet(
                channel, len(payload)
            ),
        )

    def test_read_baseexception_closes_packet_and_session(self):
        target = instrument_delta_checkpoint()
        metadata_wire = instrument_delta_metadata_bytes(target)
        metadata = instrument_delta._parse_metadata(metadata_wire)
        session = InstrumentTickDeltaSession(
            mock.Mock(),
            instrument_delta._checkpoint_endpoint(target),
            55,
            16,
        )
        cursor = InstrumentTickDeltaCursor(
            _session=session,
            _metadata=metadata,
            requested_page_records=16,
            _read_token=91,
        )
        session._active_cursor = cursor
        descriptor = os.open("/dev/null", os.O_RDONLY)
        packet = _ReceivedPacket(
            bytes(
                instrument_delta
                .READ_INSTRUMENT_TICK_DELTA_RESPONSE_BYTES_V2
            ),
            (descriptor,),
        )
        with (
            mock.patch.object(instrument_delta, "_send_packet"),
            mock.patch.object(
                instrument_delta, "_recv_packet", return_value=packet
            ),
            mock.patch.object(
                instrument_delta,
                "_validate_response_prefix",
                side_effect=KeyboardInterrupt("injected page validation"),
            ),
            self.assertRaisesRegex(
                KeyboardInterrupt, "page validation"
            ),
        ):
            cursor.read()
        self.assertTrue(cursor.closed)
        self.assertTrue(session.closed)
        self.assertTrue(packet.closed)
        with self.assertRaises(OSError):
            os.fstat(descriptor)

    def metadata(self, target=None, base=None):
        target = target or instrument_delta_checkpoint()
        wire = instrument_delta_metadata_bytes(target, base)
        return wire, instrument_delta._parse_metadata(wire)

    def sparse_payloads(self):
        return (
            instrument_delta_tick_payload(2, 2, 2, 1),
            instrument_delta_tick_payload(6, 6, 3, 3),
            instrument_delta_tick_payload(9, 9, 5, 1),
        )

    def sealed_page_fd(self, page):
        if not hasattr(os, "memfd_create"):
            self.skipTest("memfd_create is unavailable")
        writable_fd = os.memfd_create(
            "l2flow-instrument-delta-test",
            getattr(os, "MFD_CLOEXEC", 1)
            | getattr(os, "MFD_ALLOW_SEALING", 2),
        )
        try:
            view = memoryview(page)
            while view:
                written = os.write(writable_fd, view)
                if written <= 0:
                    raise RuntimeError("short instrument delta page write")
                view = view[written:]
            fcntl.fcntl(
                writable_fd,
                getattr(fcntl, "F_ADD_SEALS", 1033),
                getattr(fcntl, "F_SEAL_WRITE", 0x0008)
                | getattr(fcntl, "F_SEAL_GROW", 0x0004)
                | getattr(fcntl, "F_SEAL_SHRINK", 0x0002)
                | getattr(fcntl, "F_SEAL_SEAL", 0x0001),
            )
            return os.open(
                f"/proc/self/fd/{writable_fd}",
                os.O_RDONLY | getattr(os, "O_CLOEXEC", 0),
            )
        finally:
            os.close(writable_fd)

    def validate_page(self, page, metadata_wire, record_count):
        fd = self.sealed_page_fd(page)
        try:
            return instrument_delta._parse_page(
                instrument_delta
                .NativeInstrumentTickDeltaPageValidator(),
                fd,
                expected_bytes=len(page),
                expected_records=record_count,
                expected_page_index=0,
                metadata_wire=metadata_wire,
                prior_ingress_sequence=0,
                prior_tick_stream_sequence=0,
                prior_source_sequences=(0, 0, 0, 0),
            )
        finally:
            os.close(fd)

    def test_v2_abi_sizes_and_checkpoint_parser(self):
        self.assertEqual(
            instrument_delta
            .DEFAULT_INSTRUMENT_TICK_DELTA_PAGE_RECORDS,
            16_384,
        )
        self.assertEqual(
            instrument_delta
            .open_instrument_tick_delta_session.__kwdefaults__[
                "requested_page_records"
            ],
            16_384,
        )
        self.assertEqual(
            L2FlowClient
            .open_instrument_tick_delta_session.__kwdefaults__[
                "requested_page_records"
            ],
            16_384,
        )
        target = instrument_delta_checkpoint()
        packed = instrument_delta._pack_checkpoint(target)
        self.assertEqual(len(packed), 320)
        self.assertEqual(
            instrument_delta._parse_checkpoint(packed), target
        )
        self.assertEqual(instrument_delta._ENDPOINT.size, 256)
        self.assertEqual(instrument_delta._CHECKPOINT_TAIL.size, 64)
        self.assertEqual(instrument_delta._METADATA_TAIL.size, 88)
        self.assertEqual(instrument_delta._PAGE_PREFIX.size, 88)
        self.assertEqual(
            instrument_delta._OPEN_SESSION_REQUEST.size, 40
        )
        self.assertEqual(
            instrument_delta._OPEN_INSTRUMENT_PREFIX.size, 56
        )
        self.assertEqual(instrument_delta._READ_REQUEST.size, 48)
        self.assertEqual(instrument_delta._READ_RESPONSE.size, 64)
        metadata_wire, metadata = self.metadata(target)
        self.assertEqual(len(metadata_wire), 736)
        self.assertIsNone(metadata.base_checkpoint)
        self.assertEqual(metadata.target_checkpoint, target)
        restored = InstrumentTickDeltaCheckpoint.from_dict(
            json.loads(json.dumps(target.to_dict()))
        )
        self.assertEqual(restored, target)

    def test_checkpoint_rejects_impossible_local_successor(self):
        base = instrument_delta_checkpoint(
            source_endpoints=(1, 5, 1, 1),
            counts=(0, 0, 0, 0),
        )
        impossible = instrument_delta_checkpoint(
            generation=2,
            source_endpoints=(1, 6, 1, 1),
            counts=(0, 2, 0, 0),
        )
        with self.assertRaisesRegex(
            StaleSessionError, "local count"
        ):
            impossible.ensure_successor_of(base)

    def test_sparse_tick_and_source_sequences_are_accepted(self):
        metadata_wire, metadata = self.metadata()
        self.assertEqual(
            instrument_delta._pack_metadata(metadata), metadata_wire
        )
        payloads = self.sparse_payloads()
        page = instrument_delta_page_bytes(
            metadata_wire, payloads
        )
        result = self.validate_page(page, metadata_wire, 3)
        self.assertEqual(result.wire_records, b"".join(payloads))
        self.assertEqual(result.source_counts, (0, 2, 0, 1))
        self.assertEqual(
            (
                result.last_ingress_sequence,
                result.last_tick_stream_sequence,
            ),
            (9, 9),
        )
        self.assertEqual(
            result.last_source_sequences, (0, 5, 0, 3)
        )
        column_page = InstrumentTickDeltaPage(
            instrument_id=7,
            page_index=0,
            wire_records=result.wire_records,
            eof=False,
            cumulative_record_count=3,
            cumulative_source_record_counts=(0, 2, 0, 1),
        )
        records = column_page.numpy_records()
        self.assertEqual(len(column_page), 3)
        self.assertFalse(records.flags.writeable)
        self.assertEqual(
            records["tick_stream_sequence"].tolist(),
            [2, 6, 9],
        )
        self.assertEqual(
            records["source_sequence"].tolist(),
            [2, 3, 5],
        )

    def test_page_rejects_half_open_target_boundary(self):
        metadata_wire, _metadata = self.metadata()
        payload = instrument_delta_tick_payload(10, 9, 5, 1)
        page = instrument_delta_page_bytes(
            metadata_wire, (payload,)
        )
        with self.assertRaisesRegex(
            WireFormatError, "native wire validation"
        ):
            self.validate_page(page, metadata_wire, 1)

    def test_checkpoint_unavailable_until_explicit_empty_eof(self):
        target = instrument_delta_checkpoint(
            source_endpoints=(2, 1, 2, 1),
            counts=(0, 0, 0, 0),
        )
        metadata_wire = instrument_delta_metadata_bytes(target)
        metadata = instrument_delta._parse_metadata(metadata_wire)
        channel = mock.Mock()
        session = InstrumentTickDeltaSession(
            channel,
            instrument_delta._checkpoint_endpoint(target),
            55,
            16,
        )
        cursor = InstrumentTickDeltaCursor(
            _session=session,
            _metadata=metadata,
            requested_page_records=16,
            _read_token=91,
        )
        session._active_cursor = cursor
        with self.assertRaises(
            InstrumentTickDeltaCheckpointUnavailableError
        ):
            _ = cursor.verified_checkpoint
        response = instrument_delta._READ_RESPONSE.pack(
            CONTROL_MAGIC,
            WIRE_MAJOR,
            WIRE_MINOR,
            0,
            instrument_delta
            .INSTRUMENT_TICK_DELTA_RESPONSE_TERMINAL_V2,
            instrument_delta
            .READ_INSTRUMENT_TICK_DELTA_RESPONSE_BYTES_V2,
            0,
            77,
            0,
            0,
            target.generation,
            0,
        )
        with (
            mock.patch.object(
                instrument_delta,
                "_request_id",
                return_value=77,
            ),
            mock.patch.object(instrument_delta, "_send_packet"),
            mock.patch.object(
                instrument_delta,
                "_recv_packet",
                return_value=instrument_delta._ReceivedPacket(response),
            ),
        ):
            page = cursor.read()
        self.assertTrue(page.eof)
        self.assertEqual(page.wire_records, b"")
        self.assertTrue(cursor.done)
        self.assertEqual(cursor.verified_checkpoint, target)
        self.assertIsNone(session._active_cursor)

    def test_cursor_commits_column_page_after_native_validation(self):
        target = instrument_delta_checkpoint()
        _metadata_wire, metadata = self.metadata(target)
        payloads = self.sparse_payloads()
        channel = mock.Mock()
        session = InstrumentTickDeltaSession(
            channel,
            instrument_delta._checkpoint_endpoint(target),
            55,
            16,
        )
        session._validate_page = mock.Mock(
            return_value=mock.Mock(
                wire_records=b"".join(payloads),
                source_counts=(0, 2, 0, 1),
                last_ingress_sequence=9,
                last_tick_stream_sequence=9,
                last_source_sequences=(0, 5, 0, 3),
            )
        )
        cursor = InstrumentTickDeltaCursor(
            _session=session,
            _metadata=metadata,
            requested_page_records=16,
            _read_token=91,
        )
        session._active_cursor = cursor
        response = instrument_delta._READ_RESPONSE.pack(
            CONTROL_MAGIC,
            WIRE_MAJOR,
            WIRE_MINOR,
            0,
            0,
            instrument_delta
            .READ_INSTRUMENT_TICK_DELTA_RESPONSE_BYTES_V2,
            3,
            77,
            (
                instrument_delta
                .INSTRUMENT_TICK_DELTA_PAGE_HEADER_BYTES_V2
                + 3 * TICK_BYTES
            ),
            0,
            target.generation,
            92,
        )
        read_fd, write_fd = os.pipe()
        try:
            with (
                mock.patch.object(
                    instrument_delta,
                    "_request_id",
                    return_value=77,
                ),
                mock.patch.object(instrument_delta, "_send_packet"),
                mock.patch.object(
                    instrument_delta,
                    "_recv_packet",
                    return_value=instrument_delta._ReceivedPacket(
                        response, (read_fd,)
                    ),
                ),
            ):
                page = cursor.read()
        finally:
            os.close(write_fd)
        self.assertEqual(page.wire_records, b"".join(payloads))
        self.assertEqual(len(page), 3)
        self.assertFalse(hasattr(page, "records"))
        self.assertEqual(cursor.next_page_index, 1)
        self.assertEqual(cursor.cumulative_record_count, 3)
        self.assertEqual(cursor._read_token, 92)
        self.assertEqual(
            cursor._cumulative_source_counts, (0, 2, 0, 1)
        )
        self.assertEqual(session._validate_page.call_count, 1)
        cursor.close()

    def test_cursor_validation_failure_does_not_advance_state(self):
        target = instrument_delta_checkpoint()
        _metadata_wire, metadata = self.metadata(target)
        channel = mock.Mock()
        session = InstrumentTickDeltaSession(
            channel,
            instrument_delta._checkpoint_endpoint(target),
            55,
            16,
        )
        session._validate_page = mock.Mock(
            side_effect=WireFormatError("invalid page")
        )
        cursor = InstrumentTickDeltaCursor(
            _session=session,
            _metadata=metadata,
            requested_page_records=16,
            _read_token=91,
        )
        session._active_cursor = cursor
        response = instrument_delta._READ_RESPONSE.pack(
            CONTROL_MAGIC,
            WIRE_MAJOR,
            WIRE_MINOR,
            0,
            0,
            instrument_delta
            .READ_INSTRUMENT_TICK_DELTA_RESPONSE_BYTES_V2,
            3,
            77,
            (
                instrument_delta
                .INSTRUMENT_TICK_DELTA_PAGE_HEADER_BYTES_V2
                + 3 * TICK_BYTES
            ),
            0,
            target.generation,
            92,
        )
        read_fd, write_fd = os.pipe()
        try:
            with (
                mock.patch.object(
                    instrument_delta,
                    "_request_id",
                    return_value=77,
                ),
                mock.patch.object(instrument_delta, "_send_packet"),
                mock.patch.object(
                    instrument_delta,
                    "_recv_packet",
                    return_value=instrument_delta._ReceivedPacket(
                        response, (read_fd,)
                    ),
                ),
                self.assertRaises(WireFormatError),
            ):
                cursor.read()
        finally:
            os.close(write_fd)
        self.assertTrue(session.closed)
        self.assertTrue(cursor.closed)
        self.assertEqual(cursor.next_page_index, 0)
        self.assertEqual(cursor.cumulative_record_count, 0)
        self.assertEqual(cursor._read_token, 91)
        self.assertEqual(session._validate_page.call_count, 1)

    def test_open_instrument_rejects_checkpoint_session_mismatch(self):
        target = instrument_delta_checkpoint()
        foreign = instrument_delta_checkpoint(
            run_id=b"fedcba9876543210"
        )
        session = InstrumentTickDeltaSession(
            mock.Mock(),
            instrument_delta._checkpoint_endpoint(target),
            55,
            16,
        )
        with self.assertRaises(StaleSessionError):
            session.open_instrument(7, after=foreign)


class InstrumentRollingTests(unittest.TestCase):
    def payloads(self):
        return (
            instrument_delta_tick_payload(2, 2, 2, 1),
            instrument_delta_tick_payload(6, 6, 3, 3),
            instrument_delta_tick_payload(9, 9, 5, 1),
        )

    def pages(self):
        payloads = self.payloads()
        return (
            InstrumentTickDeltaPage(
                instrument_id=7,
                page_index=0,
                wire_records=b"".join(payloads[:2]),
                eof=False,
                cumulative_record_count=2,
                cumulative_source_record_counts=(0, 1, 0, 1),
            ),
            InstrumentTickDeltaPage(
                instrument_id=7,
                page_index=1,
                wire_records=payloads[2],
                eof=False,
                cumulative_record_count=3,
                cumulative_source_record_counts=(0, 2, 0, 1),
            ),
            InstrumentTickDeltaPage(
                instrument_id=7,
                page_index=2,
                wire_records=b"",
                eof=True,
                cumulative_record_count=3,
                cumulative_source_record_counts=(0, 2, 0, 1),
            ),
        )

    def store(self):
        return InstrumentTickRollingStore(
            7,
            2,
            state_schema="ticks-v1",
            factor_schema="factor-v1",
            factor_state=0,
        )

    def test_columns_require_immutable_aligned_wire_bytes(self):
        with self.assertRaises(TypeError):
            InstrumentTickColumns(bytearray(TICK_BYTES))
        with self.assertRaisesRegex(ValueError, "record-aligned"):
            InstrumentTickColumns(b"\x00")

    def test_page_staging_commit_and_window_eviction(self):
        target = instrument_delta_checkpoint()
        cursor = FakeInstrumentDeltaCursor(
            None, target, self.pages()
        )
        factor = RecordingRollingFactor()
        store = self.store()
        committed = store.update(cursor, factor)
        self.assertEqual(
            committed.state.numpy_records()[
                "tick_stream_sequence"
            ].tolist(),
            [6, 9],
        )
        self.assertFalse(
            committed.state.numpy_records().flags.writeable
        )
        self.assertFalse(hasattr(committed.state, "ticks"))
        self.assertEqual(committed.state.seen_count, 3)
        self.assertEqual(committed.state.factor_state, 3)
        self.assertEqual(committed.checkpoint, target)
        self.assertEqual(
            [len(change.appended) for change in factor.changes],
            [2, 1],
        )
        self.assertEqual(
            [len(change.evicted) for change in factor.changes],
            [0, 1],
        )
        self.assertEqual(
            factor.changes[0].appended.numpy_records()[
                "tick_stream_sequence"
            ].tolist(),
            [2, 6],
        )
        self.assertEqual(
            factor.changes[1].evicted.numpy_records()[
                "tick_stream_sequence"
            ].tolist(),
            [2],
        )

    def test_page_larger_than_window_evicts_early_page_columns(self):
        cursor = FakeInstrumentDeltaCursor(
            None, instrument_delta_checkpoint(), self.pages()
        )
        factor = RecordingRollingFactor()
        store = InstrumentTickRollingStore(
            7,
            1,
            state_schema="ticks-v1",
            factor_schema="factor-v1",
            factor_state=0,
        )
        committed = store.update(cursor, factor)
        self.assertEqual(
            factor.changes[0].evicted.numpy_records()[
                "tick_stream_sequence"
            ].tolist(),
            [2],
        )
        self.assertEqual(
            factor.changes[0].after.numpy_records()[
                "tick_stream_sequence"
            ].tolist(),
            [6],
        )
        self.assertEqual(
            committed.state.numpy_records()[
                "tick_stream_sequence"
            ].tolist(),
            [9],
        )

    def test_factor_failure_aborts_all_committed_fields(self):
        target = instrument_delta_checkpoint()
        cursor = FakeInstrumentDeltaCursor(
            None, target, self.pages()
        )
        store = self.store()
        before = store.snapshot()
        with self.assertRaisesRegex(RuntimeError, "factor failure"):
            store.update(
                cursor, RecordingRollingFactor(fail_call=2)
            )
        self.assertEqual(store.snapshot(), before)
        self.assertTrue(cursor.closed)

    def test_transaction_context_aborts_on_block_exception(self):
        cursor = FakeInstrumentDeltaCursor(
            None, instrument_delta_checkpoint(), self.pages()
        )
        store = self.store()
        before = store.snapshot()
        with self.assertRaisesRegex(ValueError, "block failure"):
            with store.begin(
                cursor, RecordingRollingFactor()
            ) as transaction:
                transaction.apply_page(self.pages()[0])
                raise ValueError("block failure")
        self.assertFalse(transaction.active)
        self.assertTrue(cursor.closed)
        self.assertEqual(store.snapshot(), before)

    def test_context_commit_result_survives_reference_cleanup(self):
        target = instrument_delta_checkpoint()
        cursor = FakeInstrumentDeltaCursor(
            None, target, self.pages()
        )
        store = self.store()
        with store.begin(
            cursor, RecordingRollingFactor()
        ) as transaction:
            for page in cursor.pages():
                transaction.apply_page(page)
            committed = transaction.commit()
            self.assertFalse(transaction.active)
            self.assertIsNone(transaction._cursor)
            self.assertIsNone(transaction._base)
            self.assertEqual(transaction._shadow_wire_records, b"")
            self.assertIsNone(transaction._shadow_factor_state)
        self.assertEqual(committed.checkpoint, target)
        self.assertEqual(committed.state.seen_count, 3)
        self.assertEqual(committed.state.factor_state, 3)

    def test_store_update_aborts_on_keyboard_interrupt(self):
        class InterruptingCursor(FakeInstrumentDeltaCursor):
            def pages(self):
                raise KeyboardInterrupt("cursor interrupted")
                yield

        cursor = InterruptingCursor(
            None, instrument_delta_checkpoint(), self.pages()
        )
        store = self.store()
        before = store.snapshot()
        with self.assertRaisesRegex(
            KeyboardInterrupt, "cursor interrupted"
        ):
            store.update(cursor, RecordingRollingFactor())
        self.assertTrue(cursor.closed)
        self.assertEqual(store.snapshot(), before)

        replacement = FakeInstrumentDeltaCursor(
            None, instrument_delta_checkpoint(), self.pages()
        )
        store.begin(
            replacement, RecordingRollingFactor()
        ).abort()

    def test_apply_page_aborts_on_keyboard_interrupt(self):
        class InterruptingFactor:
            factor_schema = "factor-v1"

            def update(self, state, change):
                raise KeyboardInterrupt("factor interrupted")

        cursor = FakeInstrumentDeltaCursor(
            None, instrument_delta_checkpoint(), self.pages()
        )
        store = self.store()
        before = store.snapshot()
        transaction = store.begin(cursor, InterruptingFactor())
        with self.assertRaisesRegex(
            KeyboardInterrupt, "factor interrupted"
        ):
            transaction.apply_page(self.pages()[0])
        self.assertFalse(transaction.active)
        self.assertTrue(cursor.closed)
        self.assertIsNone(transaction._cursor)
        self.assertIsNone(transaction._factor)
        self.assertIsNone(transaction._base)
        self.assertEqual(transaction._shadow_wire_records, b"")
        self.assertIsNone(transaction._shadow_factor_state)
        self.assertEqual(store.snapshot(), before)

    def test_commit_aborts_on_keyboard_interrupt(self):
        cursor = FakeInstrumentDeltaCursor(
            None, instrument_delta_checkpoint(), self.pages()
        )
        store = self.store()
        before = store.snapshot()
        transaction = store.begin(
            cursor, RecordingRollingFactor()
        )
        for page in cursor.pages():
            transaction.apply_page(page)

        with mock.patch(
            "l2flow_realtime.rolling.copy.deepcopy",
            side_effect=KeyboardInterrupt("commit interrupted"),
        ), self.assertRaisesRegex(
            KeyboardInterrupt, "commit interrupted"
        ):
            transaction.commit()

        self.assertFalse(transaction.active)
        self.assertIsNone(transaction._cursor)
        self.assertIsNone(transaction._base)
        self.assertEqual(transaction._shadow_wire_records, b"")
        self.assertIsNone(transaction._shadow_factor_state)
        self.assertEqual(store.snapshot(), before)

        replacement = FakeInstrumentDeltaCursor(
            None, instrument_delta_checkpoint(), self.pages()
        )
        store.begin(
            replacement, RecordingRollingFactor()
        ).abort()

    def test_abandoned_transaction_finalizer_is_safe(self):
        class InterruptingCloseCursor(FakeInstrumentDeltaCursor):
            def close(self):
                self.closed = True
                raise KeyboardInterrupt("close interrupted")

        cursor = InterruptingCloseCursor(
            None, instrument_delta_checkpoint(), self.pages()
        )
        store = self.store()
        before = store.snapshot()
        transaction = store.begin(
            cursor, RecordingRollingFactor()
        )
        transaction.apply_page(self.pages()[0])
        del transaction
        gc.collect()

        self.assertTrue(cursor.closed)
        self.assertEqual(store.snapshot(), before)
        replacement = FakeInstrumentDeltaCursor(
            None, instrument_delta_checkpoint(), self.pages()
        )
        store.begin(
            replacement, RecordingRollingFactor()
        ).abort()

    def test_mutating_factor_failure_cannot_change_committed_state(self):
        class MutatingFailure:
            factor_schema = "factor-v1"

            def update(self, state, change):
                state["count"] += len(change.appended)
                raise RuntimeError("mutating factor failure")

        cursor = FakeInstrumentDeltaCursor(
            None, instrument_delta_checkpoint(), self.pages()
        )
        store = InstrumentTickRollingStore(
            7,
            2,
            state_schema="ticks-v1",
            factor_schema="factor-v1",
            factor_state={"count": 0},
        )
        before = store.snapshot()
        with self.assertRaisesRegex(
            RuntimeError, "mutating factor failure"
        ):
            store.update(cursor, MutatingFailure())
        self.assertEqual(store.snapshot(), before)

    def test_source_counts_must_reconcile_at_eof(self):
        pages = tuple(
            InstrumentTickDeltaPage(
                instrument_id=page.instrument_id,
                page_index=page.page_index,
                wire_records=page.wire_records,
                eof=page.eof,
                cumulative_record_count=page.cumulative_record_count,
                cumulative_source_record_counts=(
                    (0, 2, 0, 0)
                    if page.page_index == 0
                    else (0, 3, 0, 0)
                ),
            )
            for page in self.pages()
        )
        cursor = FakeInstrumentDeltaCursor(
            None, instrument_delta_checkpoint(), pages
        )
        store = self.store()
        before = store.snapshot()
        with self.assertRaisesRegex(
            WireFormatError, "counts do not reconcile"
        ):
            store.update(cursor, RecordingRollingFactor())
        self.assertEqual(store.snapshot(), before)

    def test_empty_delta_advances_checkpoint_and_generation_hook(self):
        base = instrument_delta_checkpoint()
        target = instrument_delta_checkpoint(
            generation=2,
            source_endpoints=(2, 6, 2, 5),
            counts=base.instrument_tick_counts,
        )
        wire_records = b"".join(self.payloads())
        store = InstrumentTickRollingStore(
            7,
            3,
            state_schema="ticks-v1",
            factor_schema="factor-v1",
            factor_state=3,
            checkpoint=base,
            wire_records=wire_records,
        )
        eof = InstrumentTickDeltaPage(
            instrument_id=7,
            page_index=0,
            wire_records=b"",
            eof=True,
            cumulative_record_count=0,
            cumulative_source_record_counts=(0, 0, 0, 0),
        )
        cursor = FakeInstrumentDeltaCursor(
            base, target, (eof,)
        )

        class GenerationFactor(RecordingRollingFactor):
            def on_generation(self, state, generation):
                return state + 10, generation.checkpoint.generation

        factor = GenerationFactor()
        committed = store.update(cursor, factor)
        self.assertEqual(factor.changes, [])
        self.assertEqual(committed.state.factor_state, 13)
        self.assertEqual(committed.factor_value, 2)
        self.assertEqual(committed.checkpoint, target)
        self.assertEqual(
            committed.state.wire_records, wire_records
        )

    def test_cursor_base_session_mismatch_is_rejected(self):
        target = instrument_delta_checkpoint()
        cursor = FakeInstrumentDeltaCursor(
            target, target, self.pages()
        )
        store = self.store()
        with self.assertRaises(StaleSessionError):
            store.begin(cursor, RecordingRollingFactor())

    def test_restore_rejects_incomplete_retained_tail(self):
        checkpoint = instrument_delta_checkpoint()
        with self.assertRaisesRegex(ValueError, "exact count tail"):
            InstrumentTickRollingStore(
                7,
                2,
                state_schema="ticks-v1",
                factor_schema="factor-v1",
                factor_state=0,
                checkpoint=checkpoint,
                wire_records=self.payloads()[-1],
            )


class ControlTests(unittest.TestCase):
    _RESPONSE = struct.Struct("<8sHHHHIIQQQQQ")

    def test_unclaimed_control_session_finalizer_closes_fd(self):
        descriptor = os.open("/dev/null", os.O_RDONLY)
        session = ControlSession(
            _ReceivedPacket(fds=(descriptor,)), 11, 5, 4096
        )
        del session
        gc.collect()
        with self.assertRaises(OSError):
            os.fstat(descriptor)

    def test_receive_closes_scm_fd_on_baseexception(self):
        request_id = 91
        response = self._RESPONSE.pack(
            CONTROL_MAGIC,
            WIRE_MAJOR,
            WIRE_MINOR,
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
        assert_scm_fd_closed_on_base_exception(
            self,
            response,
            lambda channel: receive_session_fd(channel, request_id),
        )

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
            WIRE_MAJOR,
            WIRE_MINOR,
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
        with receive_session_fd(left, request_id) as discovered:
            received_fd = discovered.fd
            self.assertEqual(discovered.session_epoch, 5)
            self.assertFalse(os.get_inheritable(discovered.fd))
            self.assertEqual(
                os.fstat(discovered.fd).st_rdev,
                os.fstat(descriptor).st_rdev,
            )
        with self.assertRaises(OSError):
            os.fstat(received_fd)

    def test_discovery_rejects_relative_socket_path(self):
        with self.assertRaisesRegex(ValueError, "absolute"):
            discover_session_fd("relative.sock")

    def test_connect_fake_native_seam_closes_received_python_fd(self):
        with tempfile.TemporaryFile() as mapping:
            mapping.truncate(4096)
            control_fd = os.dup(mapping.fileno())
            factory_fd = []

            def factory(fd):
                os.fstat(fd)
                factory_fd.append(fd)
                return FakeNative()

            control = ControlSession(
                _ReceivedPacket(fds=(control_fd,)), 11, 5, 4096
            )
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

    def test_connect_baseexception_closes_control_and_native(self):
        with tempfile.TemporaryFile() as mapping:
            mapping.truncate(4096)
            control_fd = os.dup(mapping.fileno())
            control = ControlSession(
                _ReceivedPacket(fds=(control_fd,)), 11, 5, 4096
            )
            native = FakeNative()
            native.session = mock.Mock(
                side_effect=KeyboardInterrupt("session interrupted")
            )
            native.close = mock.Mock()

            with (
                mock.patch(
                    "l2flow_realtime.client.discover_session_fd",
                    return_value=control,
                ),
                self.assertRaisesRegex(
                    KeyboardInterrupt, "session interrupted"
                ),
            ):
                L2FlowClient.connect(
                    "/unused/control.sock",
                    _native_factory=lambda _fd: native,
                )

            self.assertTrue(control.closed)
            with self.assertRaises(OSError):
                os.fstat(control_fd)
            native.close.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
