#!/usr/bin/env python3
"""Stdlib-only contract tests for the Python Wire V2 replacement."""

from __future__ import annotations

import ctypes
import dataclasses
import importlib
import struct
import sys
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


REPOSITORY = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPOSITORY / "python"))

import l2flow_realtime
from l2flow_realtime import (
    CatalogScope,
    EventUid,
    EventUidScope,
    HistoryCoverageInfo,
    InstrumentKey,
    InstrumentLookupStatus,
    InstrumentStatus,
    KLineCoverageFlag,
    KLineTemporalCoverage,
    L2FlowClient,
    LatestKLine,
    LatestStatus,
    SelectionScope,
    ServerState,
    SessionIdentity,
    SessionInfo,
    TemporalCoverageKind,
    WireFormatError,
)
from l2flow_realtime import native
from l2flow_realtime.control import build_get_session_request
from l2flow_realtime.wire import (
    CONTROL_MAGIC,
    KLINE_BYTES,
    SNAPSHOT_BYTES,
    TICK_BYTES,
    WIRE_MAJOR,
    WIRE_MINOR,
    parse_kline_payload,
)


RUN_ID = b"R" * 16
LAYOUT_DIGEST = b"L" * 32
CATALOG_DIGEST = b"C" * 32
SESSION_EPOCH = 17
CAPACITY = 4
_COMMON = struct.Struct("<IIIIQQQQqqqQQQIIII6B10x")
_DECIMAL = struct.Struct("<qqBBB5x")
_QUANTITY = struct.Struct("<qBBB5x")
_INSTRUMENT = struct.Struct("<QIIII4BIQQIIQQ7Q")
_KLINE = struct.Struct(
    "<QIIIIQQQ" + "q" * 6 + "Q" * 11 + "BBB5x"
)


def session_info(**overrides) -> SessionInfo:
    values = {
        "run_id": RUN_ID,
        "layout_digest": LAYOUT_DIGEST,
        "catalog_digest": CATALOG_DIGEST,
        "session_epoch": SESSION_EPOCH,
        "catalog_generation": 1,
        "data_state_generation": 4,
        "accepted_sequence": 100,
        "applied_sequence": 99,
        "processing_lag_records": 1,
        "tick_ring_capacity": 1024,
        "tick_highest_published_sequence": 7,
        "tick_contiguous_published_sequence": 7,
        "kline_generation": 8,
        "heartbeat_monotonic_ns": time.monotonic_ns(),
        "published_records": 50,
        "trade_date": 20260729,
        "server_state": ServerState.ACTIVE,
        "flags": 6,
        "capacity": CAPACITY,
        "window_count": 2,
        "catalog_scope": CatalogScope.DECLARED_DAILY_A_SHARE,
        "coverage_complete": True,
        "bound_count": CAPACITY,
        "available_count": 1,
        "snapshot_available_count": 1,
        "tick_available_count": 1,
        "factor_eligible_count": 1,
        "catalog_trade_date": 20260729,
        "catalog_version": 7,
    }
    values.update(overrides)
    return SessionInfo(**values)


def _set_scalar(pointer, c_type, value) -> None:
    ctypes.cast(pointer, ctypes.POINTER(c_type)).contents.value = value


def _fill_common(
    payload: bytearray,
    *,
    record_bytes: int,
    instrument_id: int,
    ingress_sequence: int,
    tick_sequence: int,
    source_slot: int,
    event_kind: int,
    market: int,
) -> None:
    _COMMON.pack_into(
        payload,
        0,
        2,
        record_bytes,
        instrument_id,
        instrument_id - 1,
        ingress_sequence,
        ingress_sequence,
        tick_sequence,
        0,
        1_000,
        2_000,
        3_000,
        4_000,
        0,
        0,
        5,
        20260729,
        93000000,
        0,
        source_slot,
        event_kind,
        market,
        2,
        1,
        1,
    )


def snapshot_payload(instrument_id: int = 1) -> bytes:
    payload = bytearray(SNAPSHOT_BYTES)
    _fill_common(
        payload,
        record_bytes=SNAPSHOT_BYTES,
        instrument_id=instrument_id,
        ingress_sequence=10,
        tick_sequence=0,
        source_slot=0,
        event_kind=1,
        market=1,
    )
    _DECIMAL.pack_into(payload, 240, 12345, 123450000, 2, 1, 0)
    return bytes(payload)


def tick_payload(instrument_id: int = 1) -> bytes:
    payload = bytearray(TICK_BYTES)
    _fill_common(
        payload,
        record_bytes=TICK_BYTES,
        instrument_id=instrument_id,
        ingress_sequence=11,
        tick_sequence=7,
        source_slot=1,
        event_kind=2,
        market=1,
    )
    struct.pack_into(
        "<IIqqii8B",
        payload,
        128,
        0,
        0,
        5,
        6,
        0,
        0,
        3,
        1,
        2,
        1,
        3,
        0,
        0,
        0,
    )
    _DECIMAL.pack_into(payload, 168, 12345, 123450000, 2, 1, 0)
    _QUANTITY.pack_into(payload, 192, 800, 0, 1, 0)
    return bytes(payload)


def kline_payload(
    instrument_id: int = 1,
    window_id: int = 1,
    coverage_flags: int = 0,
) -> bytes:
    return _KLINE.pack(
        8,
        20260729,
        instrument_id,
        window_id,
        coverage_flags,
        60_000_000_000,
        1,
        2,
        3,
        4,
        10,
        12,
        9,
        11,
        100,
        2,
        1,
        10,
        1,
        1,
        10,
        20,
        2,
        2,
        11,
        0,
        2,
        1,
    )


class TemporalCoverageAndEventUidTests(unittest.TestCase):
    def test_history_coverage_keeps_partial_boundary_optional(self):
        from_open = HistoryCoverageInfo(
            run_id=RUN_ID,
            session_epoch=SESSION_EPOCH,
            trade_date=20260729,
            coverage_kind=TemporalCoverageKind.FROM_OPEN,
        )
        self.assertTrue(from_open.available)
        self.assertTrue(from_open.coverage_from_open)
        self.assertFalse(from_open.process_start_partial)
        self.assertEqual(
            from_open.identity,
            SessionIdentity(RUN_ID, SESSION_EPOCH),
        )

        unknown_partial = dataclasses.replace(
            from_open,
            coverage_kind=TemporalCoverageKind.PROCESS_START_PARTIAL,
        )
        self.assertTrue(unknown_partial.process_start_partial)
        self.assertIsNone(unknown_partial.coverage_start_unix_ns)
        exact_partial = dataclasses.replace(
            unknown_partial,
            coverage_start_unix_ns=1_700_000_000_000_000_000,
        )
        self.assertEqual(
            exact_partial.coverage_start_unix_ns,
            1_700_000_000_000_000_000,
        )

        unavailable = dataclasses.replace(
            from_open,
            coverage_kind=TemporalCoverageKind.UNAVAILABLE,
        )
        self.assertFalse(unavailable.available)
        self.assertIs(
            TemporalCoverageKind.FROM_MARKET_OPEN,
            TemporalCoverageKind.FROM_OPEN,
        )
        self.assertIs(
            TemporalCoverageKind.FROM_PROCESS_START,
            TemporalCoverageKind.PROCESS_START_PARTIAL,
        )

        for kind, start in (
            (TemporalCoverageKind.FROM_OPEN, 1),
            (TemporalCoverageKind.UNAVAILABLE, 1),
            (TemporalCoverageKind.PROCESS_START_PARTIAL, 0),
        ):
            with self.subTest(kind=kind, start=start):
                with self.assertRaises(ValueError):
                    dataclasses.replace(
                        from_open,
                        coverage_kind=kind,
                        coverage_start_unix_ns=start,
                    )

    def test_event_uid_is_structural_and_scope_aware(self):
        base = EventUid(
            scope=EventUidScope.SESSION_SOURCE_TICK,
            session_identity=SessionIdentity(RUN_ID, SESSION_EPOCH),
            instrument_id=3,
            tick_stream_sequence=91,
            source_tick_event_ordinal=0,
        )
        self.assertEqual(len(bytes(base)), 48)
        self.assertEqual(EventUid.from_wire(bytes(base)), base)
        self.assertEqual(
            EventUid.from_wire(bytes(base)).wire,
            bytes(base),
        )

        variants = (
            base,
            dataclasses.replace(
                base,
                session_identity=SessionIdentity(
                    b"S" * 16, SESSION_EPOCH
                ),
            ),
            dataclasses.replace(
                base,
                session_identity=SessionIdentity(
                    RUN_ID, SESSION_EPOCH + 1
                ),
            ),
            dataclasses.replace(base, instrument_id=4),
            dataclasses.replace(base, tick_stream_sequence=92),
            dataclasses.replace(base, source_tick_event_ordinal=1),
        )
        self.assertEqual(len({bytes(value) for value in variants}), 6)

        invalid_scope = bytearray(bytes(base))
        invalid_scope[5] = 255
        with self.assertRaises(ValueError):
            EventUid.from_wire(bytes(invalid_scope))
        invalid_reserved = bytearray(bytes(base))
        invalid_reserved[7] = 1
        with self.assertRaises(ValueError):
            EventUid.from_wire(bytes(invalid_reserved))
        for field, value in (
            ("instrument_id", 0),
            ("tick_stream_sequence", 0),
            ("source_tick_event_ordinal", True),
            ("source_tick_event_ordinal", 1 << 32),
        ):
            with self.subTest(field=field, value=value):
                with self.assertRaises(ValueError):
                    dataclasses.replace(base, **{field: value})

    def test_dense_event_sequence_is_not_an_event_uid_coordinate(self):
        from l2flow_realtime.instrument_derived_event_history import (
            InstrumentDerivedEventKind,
            _DerivedEventRowC,
            _event_from_c,
        )

        row = _DerivedEventRowC()
        row.record_schema_version = 2
        row.record_bytes = ctypes.sizeof(_DerivedEventRowC)
        row.derived_event_sequence = 1
        row.trade_date = 20260729
        row.instrument_id = 3
        row.market = 1
        row.event_kind = int(InstrumentDerivedEventKind.TRADE)
        row.tick_stream_sequence = 91
        row.reserved0 = 0
        row.reserved1[0] = 1
        identity = SessionIdentity(RUN_ID, SESSION_EPOCH)

        first = _event_from_c(
            row,
            session_identity=identity,
        )
        self.assertEqual(first.source_tick_event_ordinal, 0)
        self.assertEqual(first.event_uid.session_identity, identity)
        self.assertEqual(first.require_event_uid(), first.event_uid)

        ordinal_without_session = _event_from_c(
            row,
        )
        self.assertEqual(
            ordinal_without_session.source_tick_event_ordinal, 0
        )
        self.assertIsNone(ordinal_without_session.event_uid)

        row.derived_event_sequence = 999
        same_source_event = _event_from_c(
            row,
            session_identity=identity,
        )
        row.reserved0 = 1
        next_source_event = _event_from_c(
            row,
            session_identity=identity,
        )
        self.assertEqual(first.event_uid, same_source_event.event_uid)
        self.assertNotEqual(first.event_uid, next_source_event.event_uid)
        self.assertEqual(first.require_event_uid(), first.event_uid)
        with self.assertRaises(ValueError):
            dataclasses.replace(first, source_tick_event_ordinal=1)

        row.reserved0 = 0
        row.reserved1[0] = 0
        row.tick_stream_sequence = 0
        row.event_kind = int(
            InstrumentDerivedEventKind.ORDER_REVISION
        )
        row.operation = 2
        source_free_finalize = _event_from_c(
            row, session_identity=identity
        )
        self.assertIsNone(
            source_free_finalize.source_tick_event_ordinal
        )
        self.assertIsNone(source_free_finalize.event_uid)
        with self.assertRaises(l2flow_realtime.UnavailableError):
            source_free_finalize.require_event_uid()

        row.event_kind = int(InstrumentDerivedEventKind.TRADE)
        with self.assertRaises(l2flow_realtime.WireFormatError):
            _event_from_c(row, session_identity=identity)
        row.reserved1[0] = 2
        with self.assertRaises(l2flow_realtime.WireFormatError):
            _event_from_c(row, session_identity=identity)


class FakeFunction:
    def __init__(self, callback):
        self.callback = callback
        self.calls = []
        self.argtypes = None
        self.restype = None

    def __call__(self, *arguments):
        self.calls.append(arguments)
        return self.callback(*arguments)


class FakeV2Library:
    """An in-process stand-in for every function in reader_c_v2.h."""

    def __init__(self):
        self.promote_instrument_2_on_copy = False
        self.session_server_state = ServerState.ACTIVE
        self.session_flags = 6
        self.kline_coverage_result = native.OK
        self.kline_coverage_session_epoch = SESSION_EPOCH
        self.kline_coverage_start_unix_ns = 0
        self.kline_coverage_kind = KLineTemporalCoverage.FROM_OPEN
        self.kline_coverage_reserved0 = 0
        self.kline_coverage_reserved = 0
        self.history_coverage_result = native.OK
        self.history_coverage_session_epoch = SESSION_EPOCH
        self.history_coverage_start_unix_ns = 0
        self.history_coverage_kind = TemporalCoverageKind.FROM_OPEN
        self.history_coverage_reserved0 = 0
        self.history_coverage_reserved = 0
        self.kline_payload_coverage_flags = KLineCoverageFlag.NONE
        self.l2flow_shm_reader_open_fd_v2 = FakeFunction(self._open)
        self.l2flow_shm_reader_close_v2 = FakeFunction(
            lambda _handle: None
        )
        self.l2flow_shm_reader_session_v2 = FakeFunction(self._session)
        self.l2flow_shm_reader_health_v2 = FakeFunction(self._health)
        self.l2flow_shm_reader_kline_coverage_v2 = FakeFunction(
            self._kline_coverage
        )
        self.l2flow_shm_reader_history_coverage_v2 = FakeFunction(
            self._history_coverage
        )
        self.l2flow_shm_reader_instrument_v2 = FakeFunction(
            self._instrument
        )
        self.l2flow_shm_reader_resolve_instruments_v2 = FakeFunction(
            self._resolve
        )
        self.l2flow_shm_reader_latest_snapshots_v2 = FakeFunction(
            self._latest_snapshots
        )
        self.l2flow_shm_reader_latest_ticks_v2 = FakeFunction(
            self._latest_ticks
        )
        self.l2flow_shm_reader_latest_klines_v2 = FakeFunction(
            self._latest_klines
        )
        self.l2flow_shm_reader_ticks_v2 = FakeFunction(self._ticks)
        self.l2flow_shm_reader_select_instruments_v2 = FakeFunction(
            self._select
        )

    @staticmethod
    def _open(_fd, output):
        _set_scalar(output, ctypes.c_void_p, 0x1234)
        return native.OK

    def _session(self, _handle, output):
        result = ctypes.cast(
            output, ctypes.POINTER(native._SessionInfoC)
        ).contents
        for target, source in (
            (result.run_id, RUN_ID),
            (result.layout_digest, LAYOUT_DIGEST),
            (result.catalog_digest, CATALOG_DIGEST),
        ):
            for index, byte in enumerate(source):
                target[index] = byte
        result.session_epoch = SESSION_EPOCH
        result.catalog_generation = 1
        result.data_state_generation = 4
        result.accepted_sequence = 100
        result.applied_sequence = 99
        result.processing_lag_records = 1
        result.tick_ring_capacity = 1024
        result.tick_highest_published_sequence = 7
        result.tick_contiguous_published_sequence = 7
        result.kline_generation = 8
        result.heartbeat_monotonic_ns = time.monotonic_ns()
        result.published_records = 50
        result.trade_date = 20260729
        result.server_state = int(self.session_server_state)
        result.flags = self.session_flags
        result.capacity = CAPACITY
        result.window_count = 2
        result.catalog_scope = int(
            CatalogScope.DECLARED_DAILY_A_SHARE
        )
        result.coverage_complete = 1
        result.bound_count = CAPACITY
        result.available_count = 1
        result.snapshot_available_count = 1
        result.tick_available_count = 1
        result.factor_eligible_count = 1
        result.catalog_trade_date = 20260729
        result.catalog_version = 7
        return native.OK

    def _health(self, _handle, output):
        result = ctypes.cast(
            output, ctypes.POINTER(native._HealthC)
        ).contents
        result.session_epoch = SESSION_EPOCH
        result.heartbeat_monotonic_ns = time.monotonic_ns()
        result.server_state = int(self.session_server_state)
        result.flags = self.session_flags
        return native.OK

    def _kline_coverage(self, _handle, output):
        if self.kline_coverage_result != native.OK:
            return self.kline_coverage_result
        result = ctypes.cast(
            output, ctypes.POINTER(native._KLineCoverageInfoC)
        ).contents
        result.session_epoch = self.kline_coverage_session_epoch
        result.coverage_start_unix_ns = (
            self.kline_coverage_start_unix_ns
        )
        result.coverage_kind = int(self.kline_coverage_kind)
        result.reserved0 = self.kline_coverage_reserved0
        result.reserved[0] = self.kline_coverage_reserved
        return native.OK

    def _history_coverage(self, _handle, output):
        if self.history_coverage_result != native.OK:
            return self.history_coverage_result
        result = ctypes.cast(
            output, ctypes.POINTER(native._HistoryCoverageInfoC)
        ).contents
        result.session_epoch = self.history_coverage_session_epoch
        result.coverage_start_unix_ns = (
            self.history_coverage_start_unix_ns
        )
        result.coverage_kind = int(self.history_coverage_kind)
        result.reserved0 = self.history_coverage_reserved0
        result.reserved[0] = self.history_coverage_reserved
        return native.OK

    def _instrument(
        self,
        _handle,
        instrument_id,
        row_output,
        _row_output_bytes,
        source_output,
        source_capacity,
        source_written,
        id_output,
        id_capacity,
        id_written,
        item_status,
    ):
        if instrument_id == 0 or instrument_id > CAPACITY:
            _set_scalar(source_written, ctypes.c_size_t, 0)
            _set_scalar(id_written, ctypes.c_size_t, 0)
            _set_scalar(
                item_status,
                ctypes.c_uint8,
                int(InstrumentStatus.INVALID_ID),
            )
            return native.OK
        source = b"XSHG"
        security_id = f"{599999 + instrument_id:06d}".encode("ascii")
        status = (
            InstrumentStatus.AVAILABLE
            if instrument_id == 1
            or (
                instrument_id == 2
                and self.promote_instrument_2_on_copy
                and source_capacity >= len(source)
                and id_capacity >= len(security_id)
            )
            else InstrumentStatus.BOUND_NO_DATA
        )
        _set_scalar(source_written, ctypes.c_size_t, len(source))
        _set_scalar(id_written, ctypes.c_size_t, len(security_id))
        _set_scalar(item_status, ctypes.c_uint8, int(status))
        if source_capacity < len(source) or id_capacity < len(security_id):
            return native.BUFFER_TOO_SMALL
        flags = 0xB if status is InstrumentStatus.AVAILABLE else 0
        binding = 3 if status is InstrumentStatus.AVAILABLE else 2
        first = 10 if status is InstrumentStatus.AVAILABLE else 0
        last = 20 if status is InstrumentStatus.AVAILABLE else 0
        row = _INSTRUMENT.pack(
            2,
            instrument_id,
            instrument_id - 1,
            binding,
            flags,
            1,
            2,
            1,
            1,
            0,
            0,
            4,
            len(source),
            len(security_id),
            first,
            last,
            *([0] * 7),
        )
        ctypes.memmove(row_output, row, len(row))
        if source:
            ctypes.memmove(source_output, source, len(source))
        ctypes.memmove(id_output, security_id, len(security_id))
        return native.OK

    @staticmethod
    def _resolve(
        _handle,
        markets,
        source_pointers,
        source_lengths,
        id_pointers,
        id_lengths,
        count,
        instrument_ids,
        statuses,
    ):
        for index in range(count):
            security_id = (
                ctypes.string_at(id_pointers[index], id_lengths[index])
                if id_lengths[index]
                else b""
            )
            if markets[index] == 0:
                statuses[index] = int(
                    InstrumentLookupStatus.INVALID_MARKET
                )
                instrument_ids[index] = 0
            elif not security_id:
                statuses[index] = int(
                    InstrumentLookupStatus.EMPTY_SECURITY_ID
                )
                instrument_ids[index] = 0
            elif markets[index] == 1 and security_id == b"600000":
                statuses[index] = int(InstrumentLookupStatus.FOUND)
                instrument_ids[index] = 1
            else:
                statuses[index] = int(InstrumentLookupStatus.UNKNOWN)
                instrument_ids[index] = 0
        return native.OK

    @staticmethod
    def _latest_status(instrument_id):
        return {
            0: LatestStatus.INVALID_INSTRUMENT_ID,
            1: LatestStatus.AVAILABLE,
            2: LatestStatus.BOUND_NO_DATA,
            3: LatestStatus.BOUND_NO_DATA,
            4: LatestStatus.TYPE_UNAVAILABLE,
        }.get(instrument_id, LatestStatus.INVALID_INSTRUMENT_ID)

    @classmethod
    def _latest_snapshots(
        cls, _handle, instrument_ids, count, outputs, stride, statuses
    ):
        if stride != SNAPSHOT_BYTES:
            return native.INVALID_ARGUMENT
        for index in range(count):
            status = cls._latest_status(instrument_ids[index])
            statuses[index] = int(status)
            if status is LatestStatus.AVAILABLE:
                ctypes.memmove(
                    ctypes.addressof(outputs) + index * stride,
                    snapshot_payload(instrument_ids[index]),
                    stride,
                )
        return native.OK

    @classmethod
    def _latest_ticks(
        cls, _handle, instrument_ids, count, outputs, stride, statuses
    ):
        if stride != TICK_BYTES:
            return native.INVALID_ARGUMENT
        for index in range(count):
            status = cls._latest_status(instrument_ids[index])
            statuses[index] = int(status)
            if status is LatestStatus.AVAILABLE:
                ctypes.memmove(
                    ctypes.addressof(outputs) + index * stride,
                    tick_payload(instrument_ids[index]),
                    stride,
                )
        return native.OK

    def _latest_klines(
        self,
        _handle,
        instrument_ids,
        window_ids,
        count,
        outputs,
        stride,
        statuses,
    ):
        if stride != KLINE_BYTES:
            return native.INVALID_ARGUMENT
        for index in range(count):
            instrument_id = instrument_ids[index]
            window_id = window_ids[index]
            status = self._latest_status(instrument_id)
            if window_id == 0:
                status = LatestStatus.INVALID_WINDOW_ID
            elif window_id > 2:
                status = LatestStatus.UNKNOWN_WINDOW
            statuses[index] = int(status)
            if status is LatestStatus.AVAILABLE:
                ctypes.memmove(
                    ctypes.addressof(outputs) + index * stride,
                    kline_payload(
                        instrument_id,
                        window_id,
                        int(self.kline_payload_coverage_flags),
                    ),
                    stride,
                )
        return native.OK

    @staticmethod
    def _ticks(
        _handle,
        expected_sequence,
        _outputs,
        _stride,
        _maximum_records,
        written,
        next_sequence,
        observed_sequence,
    ):
        _set_scalar(written, ctypes.c_size_t, 0)
        _set_scalar(next_sequence, ctypes.c_uint64, expected_sequence)
        _set_scalar(observed_sequence, ctypes.c_uint64, 7)
        return native.OK

    @staticmethod
    def _select(
        _handle,
        selection_scope,
        instrument_ids,
        instrument_id_capacity,
        required_count,
        envelope_output,
    ):
        counts = {
            int(SelectionScope.CATALOG_ALL): CAPACITY,
            int(SelectionScope.AVAILABLE_ANY): 1,
            int(SelectionScope.SNAPSHOT_AVAILABLE): 1,
            int(SelectionScope.TICK_AVAILABLE): 1,
            int(SelectionScope.FACTOR_ELIGIBLE): 1,
        }
        required = counts[selection_scope]
        _set_scalar(required_count, ctypes.c_size_t, required)
        envelope = ctypes.cast(
            envelope_output, ctypes.POINTER(native._SelectionEnvelopeC)
        ).contents
        for target, source in (
            (envelope.run_id, RUN_ID),
            (envelope.catalog_digest, CATALOG_DIGEST),
        ):
            for index, byte in enumerate(source):
                target[index] = byte
        envelope.session_epoch = SESSION_EPOCH
        envelope.catalog_generation = 1
        envelope.data_state_generation = 4
        envelope.accepted_sequence = 100
        envelope.applied_sequence = 99
        envelope.processing_lag_records = 1
        envelope.capacity = CAPACITY
        envelope.catalog_scope = int(
            CatalogScope.DECLARED_DAILY_A_SHARE
        )
        envelope.coverage_complete = 1
        envelope.bound_count = CAPACITY
        envelope.available_count = 1
        envelope.snapshot_available_count = 1
        envelope.tick_available_count = 1
        envelope.factor_eligible_count = 1
        envelope.selection_scope = selection_scope
        envelope.returned_row_count = required
        if instrument_id_capacity < required:
            return native.BUFFER_TOO_SMALL
        for index in range(required):
            instrument_ids[index] = index + 1
        return native.OK


class AbiContractTests(unittest.TestCase):
    def test_v2_ctypes_layout_and_symbol_binding(self):
        self.assertEqual(ctypes.sizeof(native._SessionInfoC), 240)
        self.assertEqual(ctypes.sizeof(native._SelectionEnvelopeC), 144)
        self.assertEqual(ctypes.sizeof(native._HealthC), 32)
        self.assertEqual(ctypes.sizeof(native._KLineCoverageInfoC), 32)
        self.assertEqual(native._SessionInfoC.session_epoch.offset, 80)
        self.assertEqual(native._SessionInfoC.accepted_sequence.offset, 104)
        self.assertEqual(native._SessionInfoC.trade_date.offset, 176)
        self.assertEqual(
            native._SessionInfoC.catalog_trade_date.offset, 224
        )
        self.assertEqual(native._SessionInfoC.catalog_version.offset, 232)
        self.assertEqual(native._SelectionEnvelopeC.session_epoch.offset, 48)
        self.assertEqual(
            native._SelectionEnvelopeC.accepted_sequence.offset, 72
        )
        self.assertEqual(native._SelectionEnvelopeC.capacity.offset, 96)
        self.assertEqual(
            native._KLineCoverageInfoC.coverage_start_unix_ns.offset,
            8,
        )
        self.assertEqual(
            native._KLineCoverageInfoC.coverage_kind.offset,
            16,
        )
        self.assertEqual(ctypes.sizeof(native._HistoryCoverageInfoC), 32)
        self.assertEqual(
            native._HistoryCoverageInfoC.coverage_start_unix_ns.offset,
            8,
        )
        self.assertEqual(
            native._HistoryCoverageInfoC.coverage_kind.offset,
            16,
        )

        library = FakeV2Library()
        native._bind_library(library)
        functions = [
            value
            for name, value in vars(library).items()
            if name.startswith("l2flow_shm_reader_")
        ]
        self.assertEqual(len(functions), 13)
        self.assertTrue(all(function.argtypes for function in functions))
        self.assertTrue(
            all(
                name.endswith("_v2")
                for name in vars(library)
                if name.startswith("l2flow_shm_reader_")
            )
        )

    def test_control_request_is_v2_only(self):
        request = build_get_session_request(9)
        magic, major, minor = struct.unpack_from("<8sHH", request)
        self.assertEqual(magic, CONTROL_MAGIC)
        self.assertEqual((major, minor), (WIRE_MAJOR, WIRE_MINOR))
        self.assertEqual((major, minor), (2, 5))

    def test_v24_live_partial_and_kline_coverage(self):
        self.assertEqual(int(ServerState.LIVE_PARTIAL), 6)
        partial = session_info(
            server_state=ServerState.LIVE_PARTIAL,
            flags=0,
        )
        self.assertFalse(partial.coverage_lost)
        self.assertFalse(partial.coverage_from_open)
        self.assertFalse(partial.startup_prefix_recovered)
        self.assertFalse(partial.full_day_kline_valid)
        self.assertFalse(partial.full_day_factor_valid)
        self.assertFalse(partial.certified_prefix_valid)
        self.assertIs(
            partial.kline_temporal_coverage,
            KLineTemporalCoverage.DISABLED,
        )

        partial_kline = session_info(
            server_state=ServerState.LIVE_PARTIAL,
            flags=2,
        )
        self.assertTrue(partial_kline.kline_enabled)
        self.assertFalse(partial_kline.coverage_from_open)
        self.assertFalse(partial_kline.full_day_kline_valid)
        self.assertIs(
            partial_kline.kline_temporal_coverage,
            KLineTemporalCoverage.PROCESS_START_PARTIAL,
        )

        recovered = session_info(flags=0x7E)
        self.assertFalse(recovered.coverage_lost)
        self.assertTrue(recovered.kline_enabled)
        self.assertTrue(recovered.coverage_from_open)
        self.assertTrue(recovered.startup_prefix_recovered)
        self.assertTrue(recovered.full_day_kline_valid)
        self.assertTrue(recovered.full_day_factor_valid)
        self.assertTrue(recovered.certified_prefix_valid)
        self.assertIs(
            recovered.kline_temporal_coverage,
            KLineTemporalCoverage.FROM_OPEN,
        )

        with self.assertRaises(ValueError):
            session_info(flags=2)
        with self.assertRaises(ValueError):
            session_info(
                server_state=ServerState.LIVE_PARTIAL,
                flags=4,
            )
        with self.assertRaises(ValueError):
            session_info(flags=0x80)

    def test_kline_payload_coverage_flags_fail_closed(self):
        partial = parse_kline_payload(
            kline_payload(coverage_flags=3), 1, 1
        )
        self.assertEqual(
            partial["coverage_flags"],
            KLineCoverageFlag.PROCESS_START_PARTIAL
            | KLineCoverageFlag.NATURAL_WINDOW_LEFT_TRUNCATED,
        )
        with self.assertRaises(WireFormatError):
            parse_kline_payload(kline_payload(coverage_flags=4), 1, 1)
        with self.assertRaises(WireFormatError):
            parse_kline_payload(kline_payload(coverage_flags=2), 1, 1)

    def test_v2_history_modules_replace_removed_v1_surface(self):
        self.assertTrue(hasattr(l2flow_realtime, "HistoryCursor"))
        self.assertTrue(
            hasattr(l2flow_realtime, "InstrumentTickDeltaCursor")
        )
        self.assertTrue(
            hasattr(l2flow_realtime, "InstrumentTickRollingStore")
        )
        self.assertFalse(hasattr(l2flow_realtime, "LatestResult"))
        self.assertFalse(hasattr(L2FlowClient, "open_tick_cursor"))
        history = importlib.import_module("l2flow_realtime.history")
        delta = importlib.import_module(
            "l2flow_realtime.instrument_delta"
        )
        rolling = importlib.import_module(
            "l2flow_realtime.rolling"
        )
        self.assertFalse(hasattr(history, "connect"))
        self.assertFalse(hasattr(history, "open_history"))
        self.assertFalse(hasattr(delta, "connect"))
        self.assertFalse(hasattr(delta, "open_instrument_delta"))
        self.assertFalse(
            hasattr(rolling.InstrumentTickRollingState, "state_schema")
        )
        self.assertFalse(
            hasattr(rolling.InstrumentTickRollingState, "factor_schema")
        )


class NativeReaderTests(unittest.TestCase):
    def setUp(self):
        self.library = FakeV2Library()
        self.reader = native.NativeReader.open_fd(9, library=self.library)

    def tearDown(self):
        self.reader.close()

    def test_session_has_daily_catalog_scope_counts_and_watermarks(self):
        session = self.reader.session()
        self.assertEqual(session.identity.session_epoch, SESSION_EPOCH)
        self.assertIs(
            session.catalog_scope,
            CatalogScope.DECLARED_DAILY_A_SHARE,
        )
        self.assertTrue(session.coverage_complete)
        self.assertEqual(session.catalog_generation, 1)
        self.assertEqual(session.catalog_trade_date, session.trade_date)
        self.assertEqual(session.catalog_version, 7)
        self.assertEqual(session.processing_lag_records, 1)
        self.assertEqual(
            (
                session.factor_eligible_count,
                session.snapshot_available_count,
                session.available_count,
                session.bound_count,
                session.capacity,
            ),
            (1, 1, 1, CAPACITY, CAPACITY),
        )

    def test_health_reads_only_fixed_session_fields(self):
        session_calls = len(
            self.library.l2flow_shm_reader_session_v2.calls
        )
        health = self.reader.health()
        self.assertEqual(health.session_epoch, SESSION_EPOCH)
        self.assertIs(health.server_state, ServerState.ACTIVE)
        self.assertEqual(health.flags, 6)
        self.assertTrue(health.kline_enabled)
        self.assertIs(
            health.kline_temporal_coverage,
            KLineTemporalCoverage.FROM_OPEN,
        )
        self.assertTrue(health.coverage_from_open)
        self.assertFalse(health.startup_prefix_recovered)
        self.assertFalse(health.full_day_kline_valid)
        self.assertFalse(health.full_day_factor_valid)
        self.assertFalse(health.certified_prefix_valid)
        self.assertGreater(health.heartbeat_monotonic_ns, 0)
        self.assertEqual(
            len(self.library.l2flow_shm_reader_session_v2.calls),
            session_calls,
        )
        self.assertEqual(
            len(self.library.l2flow_shm_reader_health_v2.calls), 1
        )

    def test_kline_coverage_getter_is_separate_and_immutable(self):
        session_calls = len(
            self.library.l2flow_shm_reader_session_v2.calls
        )
        coverage = self.reader.kline_coverage()
        self.assertEqual(coverage.session_epoch, SESSION_EPOCH)
        self.assertEqual(coverage.coverage_start_unix_ns, 0)
        self.assertIs(
            coverage.coverage_kind,
            KLineTemporalCoverage.FROM_OPEN,
        )
        self.assertEqual(
            len(self.library.l2flow_shm_reader_session_v2.calls),
            session_calls,
        )
        self.assertEqual(
            len(
                self.library
                .l2flow_shm_reader_kline_coverage_v2.calls
            ),
            1,
        )
        with self.assertRaises(dataclasses.FrozenInstanceError):
            coverage.coverage_start_unix_ns = 1

        self.library.kline_coverage_kind = (
            KLineTemporalCoverage.PROCESS_START_PARTIAL
        )
        self.library.kline_coverage_start_unix_ns = 123
        partial = self.reader.kline_coverage()
        self.assertEqual(partial.coverage_start_unix_ns, 123)
        self.assertIs(
            partial.coverage_kind,
            KLineTemporalCoverage.PROCESS_START_PARTIAL,
        )
        self.library.kline_coverage_kind = (
            KLineTemporalCoverage.DISABLED
        )
        self.library.kline_coverage_start_unix_ns = 0
        disabled = self.reader.kline_coverage()
        self.assertIs(
            disabled.coverage_kind,
            KLineTemporalCoverage.DISABLED,
        )

    def test_kline_coverage_getter_rejects_noncanonical_results(self):
        self.library.kline_coverage_reserved0 = 1
        with self.assertRaises(WireFormatError):
            self.reader.kline_coverage()
        self.library.kline_coverage_reserved0 = 0
        self.library.kline_coverage_kind = 99
        with self.assertRaises(WireFormatError):
            self.reader.kline_coverage()
        self.library.kline_coverage_kind = (
            KLineTemporalCoverage.PROCESS_START_PARTIAL
        )
        self.library.kline_coverage_start_unix_ns = 0
        with self.assertRaises(WireFormatError):
            self.reader.kline_coverage()
        self.library.kline_coverage_kind = (
            KLineTemporalCoverage.FROM_OPEN
        )
        self.library.kline_coverage_start_unix_ns = 1
        with self.assertRaises(WireFormatError):
            self.reader.kline_coverage()

    def test_kline_coverage_getter_propagates_unavailable(self):
        self.library.kline_coverage_result = native.UNAVAILABLE
        with self.assertRaises(l2flow_realtime.UnavailableError):
            self.reader.kline_coverage()

    def test_history_coverage_getter_is_independent_of_kline(self):
        coverage = self.reader.history_coverage()
        self.assertEqual(coverage.session_epoch, SESSION_EPOCH)
        self.assertIs(
            coverage.coverage_kind,
            TemporalCoverageKind.FROM_OPEN,
        )
        self.assertIsNone(coverage.coverage_start_unix_ns)

        self.library.history_coverage_kind = (
            TemporalCoverageKind.PROCESS_START_PARTIAL
        )
        self.library.history_coverage_start_unix_ns = 123
        self.library.kline_coverage_kind = KLineTemporalCoverage.DISABLED
        partial = self.reader.history_coverage()
        self.assertEqual(partial.coverage_start_unix_ns, 123)
        self.assertIs(
            partial.coverage_kind,
            TemporalCoverageKind.PROCESS_START_PARTIAL,
        )

    def test_history_coverage_getter_rejects_noncanonical_results(self):
        self.library.history_coverage_reserved0 = 1
        with self.assertRaises(WireFormatError):
            self.reader.history_coverage()
        self.library.history_coverage_reserved0 = 0
        self.library.history_coverage_kind = 99
        with self.assertRaises(WireFormatError):
            self.reader.history_coverage()
        self.library.history_coverage_kind = (
            TemporalCoverageKind.PROCESS_START_PARTIAL
        )
        self.library.history_coverage_start_unix_ns = 0
        with self.assertRaises(WireFormatError):
            self.reader.history_coverage()

    def test_instrument_two_pass_and_semantic_statuses(self):
        available = self.reader.instrument(1)
        self.assertIs(available.status, InstrumentStatus.AVAILABLE)
        self.assertEqual(available.session_epoch, SESSION_EPOCH)
        self.assertEqual(available.ordinal, 0)
        self.assertEqual(available.security_id, b"600000")
        self.assertEqual(
            len(self.library.l2flow_shm_reader_instrument_v2.calls), 2
        )

        bound = self.reader.instrument(2)
        self.assertIs(bound.status, InstrumentStatus.BOUND_NO_DATA)
        self.assertEqual(bound.first_ingress_sequence, 0)
        self.assertIs(
            self.reader.instrument(3).status,
            InstrumentStatus.BOUND_NO_DATA,
        )
        self.assertIs(
            self.reader.instrument(0).status, InstrumentStatus.INVALID_ID
        )

        self.library.promote_instrument_2_on_copy = True
        promoted = self.reader.instrument(2)
        self.assertIs(promoted.status, InstrumentStatus.AVAILABLE)
        self.assertEqual(promoted.first_ingress_sequence, 10)

    def test_key_resolution_is_separate_and_status_explicit(self):
        keys = (
            InstrumentKey(1, b"XSHG", b"600000"),
            InstrumentKey(1, b"XSHG", b"999999"),
            InstrumentKey(0, b"XSHG", b"600000"),
            InstrumentKey(1, b"XSHG", b""),
        )
        results = self.reader.resolve_instruments(keys)
        self.assertEqual(
            tuple(result.status for result in results),
            (
                InstrumentLookupStatus.FOUND,
                InstrumentLookupStatus.UNKNOWN,
                InstrumentLookupStatus.INVALID_MARKET,
                InstrumentLookupStatus.EMPTY_SECURITY_ID,
            ),
        )
        self.assertEqual(
            tuple(result.instrument_id for result in results),
            (1, 0, 0, 0),
        )
        self.assertTrue(
            all(result.session_epoch == SESSION_EPOCH for result in results)
        )

    def test_latest_status_and_payload_mapping(self):
        snapshots = self.reader.latest_snapshots((1, 2, 3, 4, 0))
        self.assertEqual(
            tuple(result.status for result in snapshots),
            (
                LatestStatus.AVAILABLE,
                LatestStatus.BOUND_NO_DATA,
                LatestStatus.BOUND_NO_DATA,
                LatestStatus.TYPE_UNAVAILABLE,
                LatestStatus.INVALID_INSTRUMENT_ID,
            ),
        )
        self.assertEqual(snapshots[0].last_price.raw, 12345)
        self.assertEqual(snapshots[0].common.ordinal, 0)
        self.assertIsNone(snapshots[1].wire_payload)

        tick = self.reader.latest_ticks((1,))[0]
        self.assertIs(tick.status, LatestStatus.AVAILABLE)
        self.assertEqual(tick.quantity.raw, 800)
        self.assertEqual(tick.common.tick_stream_sequence, 7)

        klines = self.reader.latest_klines((1, 1, 1), (1, 0, 9))
        self.assertEqual(
            tuple(result.status for result in klines),
            (
                LatestStatus.AVAILABLE,
                LatestStatus.INVALID_WINDOW_ID,
                LatestStatus.UNKNOWN_WINDOW,
            ),
        )
        self.assertEqual(klines[0].close_price_p6, 11)
        self.assertEqual(
            klines[0].coverage_flags,
            KLineCoverageFlag.NONE,
        )
        self.assertIs(
            klines[0].temporal_coverage,
            KLineTemporalCoverage.FROM_OPEN,
        )
        self.assertFalse(klines[0].process_start_partial)
        self.assertFalse(klines[0].natural_window_left_truncated)
        self.assertIsNone(klines[1].coverage_flags)
        self.assertIsNone(klines[1].temporal_coverage)

        self.library.kline_payload_coverage_flags = (
            KLineCoverageFlag.PROCESS_START_PARTIAL
            | KLineCoverageFlag.NATURAL_WINDOW_LEFT_TRUNCATED
        )
        partial = self.reader.latest_klines((1,), (1,))[0]
        self.assertEqual(
            partial.coverage_flags,
            KLineCoverageFlag.PROCESS_START_PARTIAL
            | KLineCoverageFlag.NATURAL_WINDOW_LEFT_TRUNCATED,
        )
        self.assertIs(
            partial.temporal_coverage,
            KLineTemporalCoverage.PROCESS_START_PARTIAL,
        )
        self.assertTrue(partial.process_start_partial)
        self.assertTrue(partial.natural_window_left_truncated)

        with self.assertRaises(ValueError):
            LatestKLine(
                SESSION_EPOCH,
                1,
                1,
                LatestStatus.TYPE_UNAVAILABLE,
                coverage_flags=KLineCoverageFlag.PROCESS_START_PARTIAL,
            )
        self.assertFalse(klines[1].process_start_partial)
        self.assertFalse(klines[1].natural_window_left_truncated)

    def test_selection_is_one_coherent_daily_catalog_envelope(self):
        selection = self.reader.select_instruments(
            SelectionScope.CATALOG_ALL
        )
        self.assertEqual(selection.instrument_ids, (1, 2, 3, 4))
        self.assertEqual(selection.returned_row_count, CAPACITY)
        self.assertIs(
            selection.catalog_scope,
            CatalogScope.DECLARED_DAILY_A_SHARE,
        )
        self.assertTrue(selection.coverage_complete)
        self.assertEqual(selection.catalog_generation, 1)
        self.assertEqual(selection.bound_count, CAPACITY)
        self.assertEqual(selection.processing_lag_records, 1)


class ClientHotPathTests(unittest.TestCase):
    def test_independent_background_reader_uses_another_client(self):
        library = FakeV2Library()
        reader = native.NativeReader.open_fd(9, library=library)
        client = L2FlowClient(
            reader,
            stale_after_ns=None,
            control_socket_path="/tmp/l2flow-test.sock",
        )
        independent = SimpleNamespace(
            session_info=lambda: session_info(),
            close=mock.Mock(),
        )
        try:
            with mock.patch.object(
                L2FlowClient,
                "connect",
                return_value=independent,
            ) as connect:
                self.assertIs(
                    client._open_independent_read_client(), independent
                )
            self.assertEqual(connect.call_count, 1)
            self.assertEqual(
                connect.call_args.args, ("/tmp/l2flow-test.sock",)
            )
            self.assertIn("_native_factory", connect.call_args.kwargs)
            independent.close.assert_not_called()
        finally:
            client.close()

    def test_independent_background_reader_rejects_session_change(self):
        library = FakeV2Library()
        reader = native.NativeReader.open_fd(9, library=library)
        client = L2FlowClient(
            reader,
            stale_after_ns=None,
            control_socket_path="/tmp/l2flow-test.sock",
        )
        independent = SimpleNamespace(
            session_info=lambda: session_info(
                run_id=b"S" * 16,
            ),
            close=mock.Mock(),
        )
        try:
            with mock.patch.object(
                L2FlowClient,
                "connect",
                return_value=independent,
            ):
                with self.assertRaises(
                    l2flow_realtime.StaleSessionError
                ):
                    client._open_independent_read_client()
            independent.close.assert_called_once_with()
        finally:
            client.close()

    def test_independent_background_reader_rejects_catalog_change(self):
        for overrides in (
            {"catalog_digest": b"D" * 32},
            {"catalog_version": 8},
        ):
            with self.subTest(overrides=overrides):
                library = FakeV2Library()
                reader = native.NativeReader.open_fd(9, library=library)
                client = L2FlowClient(
                    reader,
                    stale_after_ns=None,
                    control_socket_path="/tmp/l2flow-test.sock",
                )
                independent = SimpleNamespace(
                    session_info=lambda: session_info(**overrides),
                    close=mock.Mock(),
                )
                try:
                    with mock.patch.object(
                        L2FlowClient,
                        "connect",
                        return_value=independent,
                    ):
                        with self.assertRaises(
                            l2flow_realtime.StaleSessionError
                        ):
                            client._open_independent_read_client()
                    independent.close.assert_called_once_with()
                finally:
                    client.close()

    def test_client_exposes_exact_history_coverage(self):
        library = FakeV2Library()
        reader = native.NativeReader.open_fd(9, library=library)
        try:
            client = L2FlowClient(reader, stale_after_ns=None)
            from_open = client.history_coverage()
            self.assertEqual(from_open.run_id, RUN_ID)
            self.assertEqual(from_open.trade_date, 20260729)
            self.assertTrue(from_open.coverage_from_open)
            self.assertIsNone(from_open.coverage_start_unix_ns)

            library.session_server_state = ServerState.LIVE_PARTIAL
            library.session_flags = 0
            library.history_coverage_kind = (
                TemporalCoverageKind.PROCESS_START_PARTIAL
            )
            library.history_coverage_start_unix_ns = 456
            partial = client.history_coverage()
            self.assertTrue(partial.process_start_partial)
            self.assertEqual(partial.coverage_start_unix_ns, 456)

            library.history_coverage_kind = (
                TemporalCoverageKind.FROM_OPEN
            )
            library.history_coverage_start_unix_ns = 0
            with self.assertRaises(WireFormatError):
                client.history_coverage()
            client.close()
        finally:
            reader.close()

    def test_client_exposes_identity_checked_kline_coverage(self):
        library = FakeV2Library()
        reader = native.NativeReader.open_fd(9, library=library)
        try:
            client = L2FlowClient(reader, stale_after_ns=None)
            coverage = client.kline_coverage()
            self.assertEqual(coverage.session_epoch, SESSION_EPOCH)
            self.assertIs(
                coverage.coverage_kind,
                KLineTemporalCoverage.FROM_OPEN,
            )

            library.session_server_state = ServerState.LIVE_PARTIAL
            library.session_flags = 2
            library.kline_coverage_kind = (
                KLineTemporalCoverage.PROCESS_START_PARTIAL
            )
            library.kline_coverage_start_unix_ns = 123
            partial = client.kline_coverage()
            self.assertEqual(partial.coverage_start_unix_ns, 123)
            self.assertIs(
                partial.coverage_kind,
                KLineTemporalCoverage.PROCESS_START_PARTIAL,
            )

            library.kline_coverage_kind = (
                KLineTemporalCoverage.FROM_OPEN
            )
            library.kline_coverage_start_unix_ns = 0
            with self.assertRaises(WireFormatError):
                client.kline_coverage()

            library.kline_coverage_session_epoch = SESSION_EPOCH + 1
            with self.assertRaises(WireFormatError):
                client.kline_coverage()
            client.close()
        finally:
            reader.close()

    def test_live_partial_is_readable_but_heartbeat_is_still_enforced(self):
        library = FakeV2Library()
        reader = native.NativeReader.open_fd(9, library=library)
        try:
            client = L2FlowClient(reader, stale_after_ns=1_000_000)
            current = native.NativeSessionHealth(
                SESSION_EPOCH,
                time.monotonic_ns(),
                ServerState.LIVE_PARTIAL,
                0,
            )
            client._validate_health(current)
            stale = native.NativeSessionHealth(
                SESSION_EPOCH,
                time.monotonic_ns() - 2_000_000,
                ServerState.LIVE_PARTIAL,
                0,
            )
            with self.assertRaises(l2flow_realtime.StaleSessionError):
                client._validate_health(stale)
            client.close()
        finally:
            reader.close()

    def test_known_id_latest_never_resolves_or_fetches_instrument(self):
        library = FakeV2Library()
        reader = native.NativeReader.open_fd(9, library=library)
        try:
            client = L2FlowClient(reader, stale_after_ns=None)
            sessions_before_latest = len(
                library.l2flow_shm_reader_session_v2.calls
            )
            result = client.latest_snapshot(1)
            self.assertIs(result.status, LatestStatus.AVAILABLE)
            self.assertEqual(
                len(
                    library
                    .l2flow_shm_reader_latest_snapshots_v2
                    .calls
                ),
                1,
            )
            self.assertEqual(
                len(
                    library
                    .l2flow_shm_reader_resolve_instruments_v2
                    .calls
                ),
                0,
            )
            self.assertEqual(
                len(library.l2flow_shm_reader_instrument_v2.calls), 0
            )
            self.assertEqual(
                len(library.l2flow_shm_reader_session_v2.calls)
                - sessions_before_latest,
                0,
                "known-ID latest performs no full session read",
            )
            self.assertEqual(
                result.session_epoch,
                client.session_identity.session_epoch,
            )
            client.close()
        finally:
            reader.close()

    def test_known_id_heartbeat_check_is_throttled(self):
        library = FakeV2Library()
        reader = native.NativeReader.open_fd(9, library=library)
        try:
            client = L2FlowClient(reader)
            sessions_before = len(
                library.l2flow_shm_reader_session_v2.calls
            )
            client.latest_snapshot(1)
            self.assertEqual(
                len(library.l2flow_shm_reader_session_v2.calls),
                sessions_before,
            )
            self.assertEqual(
                len(library.l2flow_shm_reader_health_v2.calls), 0
            )
            client._next_health_check_ns = 0
            client.latest_snapshot(1)
            self.assertEqual(
                len(library.l2flow_shm_reader_session_v2.calls),
                sessions_before,
                "periodic health must not read the full session envelope",
            )
            self.assertEqual(
                len(library.l2flow_shm_reader_health_v2.calls), 1
            )
            client.close()
        finally:
            reader.close()

    def test_daily_catalog_contract_rejects_legacy_or_partial_claims(self):
        self.assertTrue(session_info().coverage_complete)
        with self.assertRaises(ValueError):
            session_info(coverage_complete=False)
        with self.assertRaises(ValueError):
            session_info(catalog_scope=1)
        with self.assertRaises(ValueError):
            session_info(catalog_generation=2)
        with self.assertRaises(ValueError):
            session_info(bound_count=CAPACITY - 1)
        with self.assertRaises(ValueError):
            session_info(catalog_trade_date=20260730)
        with self.assertRaises(ValueError):
            session_info(catalog_version=0)
        with self.assertRaises(ValueError):
            session_info(catalog_digest=b"\x00" * 32)
        with self.assertRaises(ValueError):
            session_info(
                accepted_sequence=5,
                applied_sequence=6,
                processing_lag_records=0,
            )
        with self.assertRaises(ValueError):
            session_info(
                available_count=3,
                bound_count=2,
            )
        with self.assertRaises(ValueError):
            session_info(
                accepted_sequence=(1 << 64) - 1,
                applied_sequence=99,
                processing_lag_records=(1 << 64) - 100,
            )


if __name__ == "__main__":
    unittest.main()
