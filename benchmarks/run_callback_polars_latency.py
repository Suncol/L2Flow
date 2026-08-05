#!/usr/bin/env python3
"""Measure callback-admission to Event Polars visibility under paced load.

The benchmark bridge injects real binary 4.101.24 and 6.101.36 messages into
``FastTickPipelineV1.IngestForTest``. That seam calls the same admission,
ownership, instrument-sharded Tick-worker decode, and independent derived
worker implementation as the serialized SDK callback. The scope intentionally
excludes vendor network/dispatch and a
cross-process transport, neither of which the current V3 implementation can
provide. It includes the native Event CDC copy, Python model construction,
immutable Polars block update, cumulative DataFrame concat, and a tail read.
"""

from __future__ import annotations

import argparse
import ctypes
import json
import math
import os
import platform
import socket
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import polars as pl

from l2flow_realtime.models import DerivedEvent, EventOrderKey, EventUid
from l2flow_realtime.polars import EventPolarsHistory, ImmutablePolarsBlockTable


OK = 0
UNSUPPORTED_MUTATION = 7


class EventRow(ctypes.Structure):
    _fields_ = [
        ("change_sequence", ctypes.c_uint64),
        ("callback_start_ns", ctypes.c_uint64),
        ("source_arrival_id", ctypes.c_uint64),
        ("business_sequence", ctypes.c_int64),
        ("affected_order_id", ctypes.c_int64),
        ("price_p6", ctypes.c_int64),
        ("quantity", ctypes.c_int64),
        ("trade_amount_p6", ctypes.c_int64),
        ("buy_order_id", ctypes.c_int64),
        ("sell_order_id", ctypes.c_int64),
        ("recv_realtime_ns", ctypes.c_int64),
        ("recv_monotonic_ns", ctypes.c_int64),
        ("event_time_ns_since_midnight", ctypes.c_uint64),
        ("source_quality_flags", ctypes.c_uint64),
        ("source_market_notices", ctypes.c_uint64),
        ("event_quality_flags", ctypes.c_uint64),
        ("instrument_id", ctypes.c_uint32),
        ("channel", ctypes.c_int32),
        ("source_event_ordinal", ctypes.c_uint32),
        ("derived_event_ordinal", ctypes.c_uint32),
        ("occurrence", ctypes.c_uint32),
        ("payload_validity", ctypes.c_uint32),
        ("event_kind", ctypes.c_uint8),
        ("mutation_kind", ctypes.c_uint8),
        ("reserved", ctypes.c_uint8 * 6),
    ]


class NativeSnapshot(ctypes.Structure):
    _fields_ = [
        ("scheduled_messages", ctypes.c_uint64),
        ("attempted_messages", ctypes.c_uint64),
        ("accepted_messages", ctypes.c_uint64),
        ("ingress_errors", ctypes.c_uint64),
        ("raw_tick_queue_full_errors", ctypes.c_uint64),
        ("owned_message_rejected_errors", ctypes.c_uint64),
        ("other_ingress_errors", ctypes.c_uint64),
        ("first_callback_start_ns", ctypes.c_uint64),
        ("last_callback_end_ns", ctypes.c_uint64),
        ("producer_end_ns", ctypes.c_uint64),
        ("accepted_shanghai", ctypes.c_uint64),
        ("accepted_shenzhen", ctypes.c_uint64),
        ("decoded_messages", ctypes.c_uint64),
        ("decode_failures", ctypes.c_uint64),
        ("rejected_messages", ctypes.c_uint64),
        ("fast_append_failures", ctypes.c_uint64),
        ("fast_unrecoverable_drops", ctypes.c_uint64),
        ("event_queue_failures", ctypes.c_uint64),
        ("kline_queue_failures", ctypes.c_uint64),
        ("fast_applied", ctypes.c_uint64),
        ("event_applied", ctypes.c_uint64),
        ("kline_applied", ctypes.c_uint64),
        ("event_rebuild_attempts", ctypes.c_uint64),
        ("kline_rebuild_attempts", ctypes.c_uint64),
        ("fast_stable_rows", ctypes.c_uint64),
        ("event_stable_rows", ctypes.c_uint64),
        ("event_non_live_instruments", ctypes.c_uint32),
        ("kline_non_live_instruments", ctypes.c_uint32),
        ("incomplete_fast_instruments", ctypes.c_uint32),
        ("producer_done", ctypes.c_uint32),
        ("producer_running", ctypes.c_uint32),
        ("pipeline_fatal", ctypes.c_uint32),
        ("reserved", ctypes.c_uint32),
    ]


class Bridge:
    def __init__(self, path: Path) -> None:
        self.path = path.resolve()
        self.lib = ctypes.CDLL(str(self.path))
        self._bind()
        native_row_size = self.lib.l2flow_benchmark_event_row_size_v1()
        native_snapshot_size = self.lib.l2flow_benchmark_snapshot_size_v1()
        if native_row_size != ctypes.sizeof(EventRow):
            raise RuntimeError(
                f"EventRow ABI mismatch: C++={native_row_size}, "
                f"ctypes={ctypes.sizeof(EventRow)}"
            )
        if native_snapshot_size != ctypes.sizeof(NativeSnapshot):
            raise RuntimeError(
                f"Snapshot ABI mismatch: C++={native_snapshot_size}, "
                f"ctypes={ctypes.sizeof(NativeSnapshot)}"
            )

    def _bind(self) -> None:
        lib = self.lib
        lib.l2flow_benchmark_create_v1.argtypes = [
            ctypes.c_uint64,
            ctypes.c_uint32,
            ctypes.c_uint32,
            ctypes.c_uint32,
            ctypes.c_uint32,
            ctypes.c_char_p,
            ctypes.c_size_t,
        ]
        lib.l2flow_benchmark_create_v1.restype = ctypes.c_void_p
        lib.l2flow_benchmark_start_v1.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint64,
            ctypes.c_uint64,
        ]
        lib.l2flow_benchmark_start_v1.restype = ctypes.c_int
        lib.l2flow_benchmark_snapshot_v1.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(NativeSnapshot),
        ]
        lib.l2flow_benchmark_snapshot_v1.restype = ctypes.c_int
        lib.l2flow_benchmark_instrument_count_v1.argtypes = [ctypes.c_void_p]
        lib.l2flow_benchmark_instrument_count_v1.restype = ctypes.c_uint32
        lib.l2flow_benchmark_instrument_id_v1.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint32,
        ]
        lib.l2flow_benchmark_instrument_id_v1.restype = ctypes.c_uint32
        lib.l2flow_benchmark_read_event_changes_v1.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint32,
            ctypes.c_uint64,
            ctypes.POINTER(EventRow),
            ctypes.c_size_t,
            ctypes.POINTER(ctypes.c_size_t),
            ctypes.POINTER(ctypes.c_uint64),
        ]
        lib.l2flow_benchmark_read_event_changes_v1.restype = ctypes.c_int
        lib.l2flow_benchmark_event_stable_size_v1.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint32,
            ctypes.POINTER(ctypes.c_uint64),
            ctypes.POINTER(ctypes.c_uint64),
            ctypes.POINTER(ctypes.c_uint32),
        ]
        lib.l2flow_benchmark_event_stable_size_v1.restype = ctypes.c_int
        lib.l2flow_benchmark_copy_event_stable_v1.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint32,
            ctypes.POINTER(EventRow),
            ctypes.c_size_t,
            ctypes.POINTER(ctypes.c_size_t),
            ctypes.POINTER(ctypes.c_uint64),
            ctypes.POINTER(ctypes.c_uint32),
        ]
        lib.l2flow_benchmark_copy_event_stable_v1.restype = ctypes.c_int
        lib.l2flow_benchmark_monotonic_now_ns_v1.argtypes = []
        lib.l2flow_benchmark_monotonic_now_ns_v1.restype = ctypes.c_uint64
        lib.l2flow_benchmark_event_row_size_v1.argtypes = []
        lib.l2flow_benchmark_event_row_size_v1.restype = ctypes.c_size_t
        lib.l2flow_benchmark_snapshot_size_v1.argtypes = []
        lib.l2flow_benchmark_snapshot_size_v1.restype = ctypes.c_size_t
        lib.l2flow_benchmark_destroy_v1.argtypes = [ctypes.c_void_p]
        lib.l2flow_benchmark_destroy_v1.restype = None

    def now_ns(self) -> int:
        return int(self.lib.l2flow_benchmark_monotonic_now_ns_v1())


@dataclass
class ProbeState:
    name: str
    instrument_id: int
    next_change_sequence: int
    table: ImmutablePolarsBlockTable
    rolling_latencies_ns: list[int]
    rolling_batches: int = 0
    rolling_rows: int = 0
    last_visible_arrival_id: int = 0


KEY_COLUMNS = (
    "channel",
    "business_sequence",
    "source_event_ordinal",
    "derived_event_ordinal",
    "affected_order_id",
)


def check(error: int, operation: str) -> None:
    if error != OK:
        raise RuntimeError(f"{operation} failed with bridge error {error}")


def row_to_event(row: EventRow) -> DerivedEvent:
    validity = int(row.payload_validity)
    values = {
        "price_p6": int(row.price_p6),
        "quantity": int(row.quantity),
        "trade_amount_p6": int(row.trade_amount_p6),
        "buy_order_id": int(row.buy_order_id),
        "sell_order_id": int(row.sell_order_id),
        "recv_realtime_ns": int(row.recv_realtime_ns),
        "recv_monotonic_ns": int(row.recv_monotonic_ns),
        "event_time_ns_since_midnight": int(
            row.event_time_ns_since_midnight
        ),
        "source_quality_flags": int(row.source_quality_flags),
        "source_market_notices": int(row.source_market_notices),
        "event_quality_flags": int(row.event_quality_flags),
        "price_valid": bool(validity & (1 << 0)),
        "quantity_valid": bool(validity & (1 << 1)),
        "trade_amount_valid": bool(validity & (1 << 2)),
        "event_time_valid": bool(validity & (1 << 3)),
        "receive_time_valid": bool(validity & (1 << 4)),
    }
    uid = EventUid(
        instrument_id=int(row.instrument_id),
        channel=int(row.channel),
        business_sequence=int(row.business_sequence),
        kind=int(row.event_kind),
        affected_order_id=int(row.affected_order_id),
        occurrence=int(row.occurrence),
    )
    order_key = EventOrderKey(
        channel=int(row.channel),
        business_sequence=int(row.business_sequence),
        source_event_ordinal=int(row.source_event_ordinal),
        derived_event_ordinal=int(row.derived_event_ordinal),
        affected_order_id=int(row.affected_order_id),
    )
    return DerivedEvent(
        uid=uid,
        order_key=order_key,
        source_arrival_id=int(row.source_arrival_id),
        values=values,
    )


def nearest_rank(values: Iterable[int], percentile: float) -> int | None:
    ordered = sorted(values)
    if not ordered:
        return None
    rank = max(1, math.ceil(len(ordered) * percentile))
    return int(ordered[rank - 1])


def latency_summary(values: list[int]) -> dict[str, int | None]:
    return {
        "samples": len(values),
        "min_ns": min(values) if values else None,
        "p50_ns": nearest_rank(values, 0.50),
        "p90_ns": nearest_rank(values, 0.90),
        "p99_ns": nearest_rank(values, 0.99),
        "p999_ns": nearest_rank(values, 0.999),
        "max_ns": max(values) if values else None,
    }


def native_snapshot(bridge: Bridge, handle: int) -> NativeSnapshot:
    result = NativeSnapshot()
    check(
        bridge.lib.l2flow_benchmark_snapshot_v1(
            handle, ctypes.byref(result)
        ),
        "snapshot",
    )
    return result


def stable_size(
    bridge: Bridge, handle: int, instrument_id: int
) -> tuple[int, int, int]:
    rows = ctypes.c_uint64()
    included = ctypes.c_uint64()
    repair = ctypes.c_uint32()
    check(
        bridge.lib.l2flow_benchmark_event_stable_size_v1(
            handle,
            instrument_id,
            ctypes.byref(rows),
            ctypes.byref(included),
            ctypes.byref(repair),
        ),
        "event_stable_size",
    )
    return int(rows.value), int(included.value), int(repair.value)


def poll_probe(
    bridge: Bridge,
    handle: int,
    state: ProbeState,
    buffer: Any,
    batch_size: int,
) -> bool:
    written = ctypes.c_size_t()
    next_cursor = ctypes.c_uint64()
    error = bridge.lib.l2flow_benchmark_read_event_changes_v1(
        handle,
        state.instrument_id,
        state.next_change_sequence,
        buffer,
        batch_size,
        ctypes.byref(written),
        ctypes.byref(next_cursor),
    )
    if error == UNSUPPORTED_MUTATION:
        raise RuntimeError(
            f"{state.name} entered a range-repair CDC transaction; "
            "the ordered live INSERT latency sample is no longer valid"
        )
    check(error, f"read_event_changes({state.name})")
    count = int(written.value)
    if count == 0:
        if int(next_cursor.value) != state.next_change_sequence:
            raise RuntimeError("empty Event CDC read advanced the cursor")
        return False

    events: list[DerivedEvent] = []
    callback_starts: list[int] = []
    for offset in range(count):
        row = buffer[offset]
        expected_change = state.next_change_sequence + offset
        if int(row.change_sequence) != expected_change:
            raise RuntimeError(
                f"non-contiguous Event CDC: expected {expected_change}, "
                f"got {row.change_sequence}"
            )
        if int(row.instrument_id) != state.instrument_id:
            raise RuntimeError("Event CDC crossed an instrument boundary")
        if int(row.mutation_kind) != 0:
            raise RuntimeError("rolling normal path returned a non-INSERT")
        events.append(row_to_event(row))
        callback_starts.append(int(row.callback_start_ns))

    state.table.append(event.as_dict() for event in events)
    frame = state.table.frame()
    tail_arrival = int(frame["source_arrival_id"][-1])
    visible_ns = bridge.now_ns()
    if any(visible_ns < callback_ns for callback_ns in callback_starts):
        raise RuntimeError("monotonic callback/reader clocks disagree")
    state.rolling_latencies_ns.extend(
        visible_ns - callback_ns for callback_ns in callback_starts
    )
    state.rolling_batches += 1
    state.rolling_rows += count
    state.last_visible_arrival_id = tail_arrival
    state.next_change_sequence = int(next_cursor.value)
    if state.next_change_sequence != expected_change + 1:
        raise RuntimeError("Event CDC returned the wrong next cursor")
    return True


def full_history_measurement(
    bridge: Bridge,
    handle: int,
    name: str,
    instrument_id: int,
    rows_per_block: int,
) -> dict[str, Any]:
    begin_ns = bridge.now_ns()
    row_count, expected_included, expected_repair = stable_size(
        bridge, handle, instrument_id
    )
    capacity = max(1, row_count)
    buffer = (EventRow * capacity)()
    written = ctypes.c_size_t()
    included = ctypes.c_uint64()
    repair = ctypes.c_uint32()

    preflight_done_ns = bridge.now_ns()
    check(
        bridge.lib.l2flow_benchmark_copy_event_stable_v1(
            handle,
            instrument_id,
            buffer,
            capacity,
            ctypes.byref(written),
            ctypes.byref(included),
            ctypes.byref(repair),
        ),
        f"copy_event_stable({name})",
    )
    native_copy_done_ns = bridge.now_ns()
    count = int(written.value)
    events = tuple(row_to_event(buffer[index]) for index in range(count))
    callback_starts = [
        int(buffer[index].callback_start_ns) for index in range(count)
    ]
    history = EventPolarsHistory(
        events,
        next_change_sequence=int(included.value) + 1,
        rows_per_block=rows_per_block,
    )
    frame = history.blocks.frame()
    tail_arrival = (
        int(frame["source_arrival_id"][-1]) if frame.height else None
    )
    end_ns = bridge.now_ns()
    if count != row_count or history.blocks.row_count != count:
        raise RuntimeError("full Event snapshot cardinality changed")
    if (
        int(included.value) != expected_included
        or int(repair.value) != expected_repair
    ):
        raise RuntimeError("full Event root changed during stable measurement")
    callback_to_full = [end_ns - value for value in callback_starts]
    return {
        "name": name,
        "instrument_id": instrument_id,
        "rows": count,
        "included_change_sequence": int(included.value),
        "repair_state": int(repair.value),
        "polars_blocks": history.blocks.block_count,
        "tail_source_arrival_id": tail_arrival,
        "preflight_and_output_allocation_ns": (
            preflight_done_ns - begin_ns
        ),
        "native_copy_ns": native_copy_done_ns - preflight_done_ns,
        "python_models_and_polars_ns": end_ns - native_copy_done_ns,
        "full_read_total_ns": end_ns - begin_ns,
        "callback_to_full_read": latency_summary(callback_to_full),
        "latest_callback_to_full_read_ns": (
            end_ns - max(callback_starts) if callback_starts else None
        ),
    }


def trial(
    bridge: Bridge,
    *,
    rate: int,
    message_count: int,
    instruments: int,
    workers: int,
    queue_capacity: int,
    batch_size: int,
    rows_per_block: int,
    probe_count: int,
    poll_sleep_us: int,
    catchup_timeout_seconds: float,
    repetition: int,
) -> dict[str, Any]:
    detail = ctypes.create_string_buffer(1024)
    handle = bridge.lib.l2flow_benchmark_create_v1(
        message_count,
        instruments,
        workers,
        queue_capacity,
        batch_size,
        detail,
        len(detail),
    )
    if not handle:
        raise RuntimeError(
            "benchmark bridge create failed: "
            + detail.value.decode("utf-8", errors="replace")
        )

    rolling_error: str | None = None
    native_stable_visible_ns: int | None = None
    native_all_planes_visible_ns: int | None = None
    polars_caught_up_ns: int | None = None
    try:
        native_count = int(
            bridge.lib.l2flow_benchmark_instrument_count_v1(handle)
        )
        if native_count != instruments:
            raise RuntimeError("bridge returned another instrument count")
        probe_ids = [
            int(bridge.lib.l2flow_benchmark_instrument_id_v1(handle, slot))
            for slot in range(probe_count)
        ]
        if any(value == 0 for value in probe_ids) or len(set(probe_ids)) != len(
            probe_ids
        ):
            raise RuntimeError("bridge returned invalid probe instruments")
        probes = [
            ProbeState(
                name=(
                    "shanghai_probe"
                    if slot % 2 == 0
                    else "shenzhen_probe"
                ),
                instrument_id=instrument_id,
                next_change_sequence=1,
                table=ImmutablePolarsBlockTable(
                    KEY_COLUMNS, rows_per_block=rows_per_block
                ),
                rolling_latencies_ns=[],
            )
            for slot, instrument_id in enumerate(probe_ids)
        ]
        buffers = [(EventRow * batch_size)() for _ in probes]
        check(
            bridge.lib.l2flow_benchmark_start_v1(
                handle, rate, message_count
            ),
            "start",
        )

        deadline = time.monotonic() + catchup_timeout_seconds
        last_status_check = 0.0
        final_snapshot = NativeSnapshot()
        while time.monotonic() < deadline:
            worked = False
            if rolling_error is None:
                try:
                    for state, buffer in zip(probes, buffers):
                        worked |= poll_probe(
                            bridge, handle, state, buffer, batch_size
                        )
                except Exception as error:  # preserve native load result
                    rolling_error = str(error)

            now = time.monotonic()
            if now - last_status_check >= 0.005:
                final_snapshot = native_snapshot(bridge, handle)
                last_status_check = now
                if final_snapshot.producer_done:
                    if (
                        final_snapshot.pipeline_fatal
                        or final_snapshot.incomplete_fast_instruments > 0
                    ):
                        # FAST coverage loss is monotonic and makes a complete
                        # Event/KLine catch-up mathematically impossible.
                        break
                    probes_caught_up = rolling_error is None
                    if rolling_error is None:
                        for state in probes:
                            stable_rows, included, _ = stable_size(
                                bridge, handle, state.instrument_id
                            )
                            probes_caught_up &= (
                                state.next_change_sequence == included + 1
                                and state.table.row_count == stable_rows
                            )
                    native_caught_up = (
                        final_snapshot.decoded_messages
                        == final_snapshot.accepted_messages
                        and final_snapshot.fast_stable_rows
                        == final_snapshot.accepted_messages
                        and final_snapshot.event_stable_rows
                        == final_snapshot.accepted_messages
                        and final_snapshot.event_non_live_instruments == 0
                        and final_snapshot.incomplete_fast_instruments == 0
                    )
                    if native_caught_up and native_stable_visible_ns is None:
                        native_stable_visible_ns = bridge.now_ns()
                    all_planes_caught_up = (
                        native_caught_up
                        and final_snapshot.kline_applied
                        == final_snapshot.accepted_messages
                        and final_snapshot.kline_non_live_instruments == 0
                    )
                    if (
                        all_planes_caught_up
                        and native_all_planes_visible_ns is None
                    ):
                        native_all_planes_visible_ns = bridge.now_ns()
                    if (
                        native_caught_up
                        and probes_caught_up
                        and polars_caught_up_ns is None
                    ):
                        polars_caught_up_ns = bridge.now_ns()
                    if all_planes_caught_up and probes_caught_up:
                        break
            if not worked:
                time.sleep(poll_sleep_us / 1_000_000)
        else:
            final_snapshot = native_snapshot(bridge, handle)

        # One final drain closes a race between a zero-length read and the
        # stable-root sample that ended the loop.
        if rolling_error is None:
            for state, buffer in zip(probes, buffers):
                while poll_probe(bridge, handle, state, buffer, batch_size):
                    pass
        final_snapshot = native_snapshot(bridge, handle)

        full_history: list[dict[str, Any]] = []
        for state in probes:
            full_history.append(
                full_history_measurement(
                    bridge,
                    handle,
                    state.name,
                    state.instrument_id,
                    rows_per_block,
                )
            )

        snapshot_values = {
            name: int(getattr(final_snapshot, name))
            for name, _ in final_snapshot._fields_
        }
        first_ns = int(final_snapshot.first_callback_start_ns)
        producer_end_ns = int(final_snapshot.producer_end_ns)
        accepted = int(final_snapshot.accepted_messages)
        callback_elapsed_ns = max(0, producer_end_ns - first_ns)
        native_stable_elapsed_ns = (
            max(0, native_stable_visible_ns - first_ns)
            if native_stable_visible_ns is not None and first_ns != 0
            else None
        )
        polars_caught_up_elapsed_ns = (
            max(0, polars_caught_up_ns - first_ns)
            if polars_caught_up_ns is not None and first_ns != 0
            else None
        )
        native_all_planes_elapsed_ns = (
            max(0, native_all_planes_visible_ns - first_ns)
            if native_all_planes_visible_ns is not None and first_ns != 0
            else None
        )
        actual_callback_rate = (
            accepted * 1_000_000_000 / callback_elapsed_ns
            if callback_elapsed_ns > 0
            else 0.0
        )
        native_event_stable_rate = (
            accepted * 1_000_000_000 / native_stable_elapsed_ns
            if native_stable_elapsed_ns is not None
            and native_stable_elapsed_ns > 0
            else 0.0
        )
        polars_caught_up_rate = (
            accepted * 1_000_000_000 / polars_caught_up_elapsed_ns
            if polars_caught_up_elapsed_ns is not None
            and polars_caught_up_elapsed_ns > 0
            else 0.0
        )
        native_all_planes_rate = (
            accepted * 1_000_000_000 / native_all_planes_elapsed_ns
            if native_all_planes_elapsed_ns is not None
            and native_all_planes_elapsed_ns > 0
            else 0.0
        )
        all_rolling_latencies = [
            value for state in probes for value in state.rolling_latencies_ns
        ]
        correctness_complete = (
            final_snapshot.producer_done == 1
            and final_snapshot.attempted_messages == message_count
            and final_snapshot.accepted_messages == message_count
            and final_snapshot.ingress_errors == 0
            and final_snapshot.decode_failures == 0
            and final_snapshot.fast_stable_rows == message_count
            and final_snapshot.event_stable_rows == message_count
            and final_snapshot.kline_applied == message_count
            and final_snapshot.event_non_live_instruments == 0
            and final_snapshot.kline_non_live_instruments == 0
            and final_snapshot.incomplete_fast_instruments == 0
        )
        normal_path_clean = (
            correctness_complete
            and final_snapshot.fast_append_failures == 0
            and final_snapshot.fast_unrecoverable_drops == 0
            and final_snapshot.event_queue_failures == 0
            and final_snapshot.kline_queue_failures == 0
            and final_snapshot.event_rebuild_attempts == 0
            and final_snapshot.kline_rebuild_attempts == 0
        )
        throughput_target_met = (
            normal_path_clean
            and actual_callback_rate >= rate * 0.98
            and native_all_planes_rate >= rate * 0.98
        )
        return {
            "target_messages_per_second": rate,
            "message_count": message_count,
            "repetition": repetition,
            "actual_callback_messages_per_second": actual_callback_rate,
            "native_event_stable_messages_per_second": (
                native_event_stable_rate
            ),
            "native_all_planes_messages_per_second": (
                native_all_planes_rate
            ),
            "polars_caught_up_messages_per_second": polars_caught_up_rate,
            "callback_elapsed_ns": callback_elapsed_ns,
            "native_event_stable_elapsed_ns": native_stable_elapsed_ns,
            "native_all_planes_elapsed_ns": native_all_planes_elapsed_ns,
            "polars_caught_up_elapsed_ns": polars_caught_up_elapsed_ns,
            "correctness_complete": correctness_complete,
            "terminal_fast_coverage_loss": (
                final_snapshot.pipeline_fatal == 1
                or final_snapshot.incomplete_fast_instruments > 0
            ),
            "normal_path_clean": normal_path_clean,
            "throughput_target_met": throughput_target_met,
            "rolling_error": rolling_error,
            "rolling_callback_to_polars": latency_summary(
                all_rolling_latencies
            ),
            "rolling_probes": [
                {
                    "name": state.name,
                    "instrument_id": state.instrument_id,
                    "batches": state.rolling_batches,
                    "rows": state.rolling_rows,
                    "polars_blocks": state.table.block_count,
                    "last_visible_arrival_id": state.last_visible_arrival_id,
                    "callback_to_polars": latency_summary(
                        state.rolling_latencies_ns
                    ),
                }
                for state in probes
            ],
            "full_history": full_history,
            "native_snapshot": snapshot_values,
        }
    finally:
        bridge.lib.l2flow_benchmark_destroy_v1(handle)


def command_text(command: list[str]) -> str | None:
    try:
        return subprocess.run(
            command,
            check=True,
            text=True,
            capture_output=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def cpu_model() -> str | None:
    try:
        for line in Path("/proc/cpuinfo").read_text(encoding="utf-8").splitlines():
            if line.startswith("model name"):
                return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return None


def parse_rates(text: str) -> list[int]:
    values = [int(part.strip()) for part in text.split(",") if part.strip()]
    if not values or any(value <= 0 for value in values):
        raise argparse.ArgumentTypeError("rates must be positive integers")
    return values


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--library", type=Path, required=True)
    parser.add_argument(
        "--rates",
        type=parse_rates,
        default=parse_rates("400000,500000,600000,700000,800000"),
    )
    parser.add_argument("--duration-seconds", type=float, default=1.0)
    parser.add_argument("--instruments", type=int, default=32)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--queue-capacity", type=int, default=65_536)
    parser.add_argument("--batch-size", type=int, default=1_024)
    parser.add_argument("--rows-per-block", type=int, default=4_096)
    parser.add_argument("--probe-count", type=int, default=2)
    parser.add_argument("--poll-sleep-us", type=int, default=50)
    parser.add_argument("--catchup-timeout-seconds", type=float, default=60.0)
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    if (
        args.duration_seconds <= 0
        or args.instruments < 2
        or args.instruments % 2
        or args.workers <= 0
        or args.workers > args.instruments
        or args.queue_capacity <= 0
        or args.batch_size <= 0
        or args.rows_per_block <= 0
        or args.probe_count <= 0
        or args.probe_count > args.instruments
        or args.poll_sleep_us < 0
        or args.catchup_timeout_seconds <= 0
        or args.repetitions <= 0
    ):
        parser.error("invalid benchmark dimensions")
    if not args.library.is_file():
        parser.error(f"benchmark library does not exist: {args.library}")

    bridge = Bridge(args.library)
    trials: list[dict[str, Any]] = []
    for repetition in range(1, args.repetitions + 1):
        for rate in args.rates:
            message_count = int(round(rate * args.duration_seconds))
            if message_count <= 0:
                parser.error("duration produces an empty trial")
            print(
                f"rate={rate}/s messages={message_count} "
                f"repetition={repetition}",
                flush=True,
            )
            result = trial(
                bridge,
                rate=rate,
                message_count=message_count,
                instruments=args.instruments,
                workers=args.workers,
                queue_capacity=args.queue_capacity,
                batch_size=args.batch_size,
                rows_per_block=args.rows_per_block,
                probe_count=args.probe_count,
                poll_sleep_us=args.poll_sleep_us,
                catchup_timeout_seconds=args.catchup_timeout_seconds,
                repetition=repetition,
            )
            trials.append(result)
            print(
                "  actual_callback="
                f"{result['actual_callback_messages_per_second']:.0f}/s "
                "native_event_stable="
                f"{result['native_event_stable_messages_per_second']:.0f}/s "
                "native_all_planes="
                f"{result['native_all_planes_messages_per_second']:.0f}/s "
                "polars_caught_up="
                f"{result['polars_caught_up_messages_per_second']:.0f}/s "
                f"target_met={result['throughput_target_met']}",
                flush=True,
            )

    report = {
        "schema_version": 2,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "scope": {
            "start": "FastTickPipelineV1 IngestForTest callback-admission seam",
            "end_rolling": (
                "current ImmutablePolarsBlockTable cumulative DataFrame tail read"
            ),
            "end_full_history": (
                "current EventPolarsHistory full stable snapshot DataFrame tail read"
            ),
            "included": [
                "binary MDL inspection and owned ingress copy",
                "source-by-Tick-worker raw queues and sharded decoders",
                "Tick-worker FAST publish before compact Event/KLine fan-out",
                "Event stable root or INSERT CDC native copy",
                "Python DerivedEvent model construction",
                "Polars immutable block materialization and tail read",
            ],
            "excluded": [
                "Vendor SDK network and callback dispatch",
                "cross-process IPC serialization/transport (not implemented in V3)",
                "late/range-repair CDC latency",
            ],
        },
        "configuration": {
            "rates": args.rates,
            "duration_seconds": args.duration_seconds,
            "instruments": args.instruments,
            "workers_per_plane": args.workers,
            "queue_capacity_per_raw_or_derived_shard": args.queue_capacity,
            "event_change_batch": args.batch_size,
            "polars_rows_per_block": args.rows_per_block,
            "probe_instruments": args.probe_count,
            "poll_sleep_us": args.poll_sleep_us,
            "catchup_timeout_seconds": args.catchup_timeout_seconds,
            "repetitions": args.repetitions,
            "target_tolerance": (
                "actual callback and native all-plane stable rates >= 98% "
                "of target"
            ),
            "native_affinity_policy": (
                "one exclusive CPU per Tick/Event/KLine worker; remaining "
                "taskset CPUs reserved for callback/Python"
            ),
        },
        "environment": {
            "hostname": socket.gethostname(),
            "platform": platform.platform(),
            "cpu_model": cpu_model(),
            "logical_cpus": os.cpu_count(),
            "process_affinity": sorted(os.sched_getaffinity(0)),
            "python": platform.python_version(),
            "polars": pl.__version__,
            "git_branch": command_text(["git", "branch", "--show-current"]),
            "git_commit": command_text(["git", "rev-parse", "HEAD"]),
            "library": str(bridge.path),
        },
        "trials": trials,
    }
    output = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output is None:
        print(output, end="")
    else:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(output, encoding="utf-8")
        print(f"wrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
