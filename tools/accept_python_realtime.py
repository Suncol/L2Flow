#!/usr/bin/env python3
"""Long-running live acceptance for the Python/Polars realtime interface.

The primary latency is measured in one host CLOCK_MONOTONIC domain:

    Python cursor read completion - router callback recv_monotonic_ns

Ticks reach the shared-memory ring only after the C++ Store append, KLine
application, handoff release, and in-process latest publication have all
succeeded.  The measurement does not wait for an immutable Store generation.
It intentionally includes IPC publication, contiguous-prefix closure, native
block copy, consumer polling, scheduling, and batching before Python can
actually use the row. Per-row Python dataclass construction is not part of the
primary columnar latency; that public API is exercised separately.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
import traceback
from array import array
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence


_UINT32_MAX = (1 << 32) - 1
_NANOSECONDS_PER_SECOND = 1_000_000_000


def _positive_int(value: str) -> int:
    parsed = int(value, 10)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _instrument_ids(value: str) -> tuple[int, ...]:
    parsed = tuple(int(item, 10) for item in value.split(","))
    if not parsed or any(item <= 0 or item > _UINT32_MAX for item in parsed):
        raise argparse.ArgumentTypeError(
            "instrument IDs must be positive uint32 values"
        )
    if len(set(parsed)) != len(parsed):
        raise argparse.ArgumentTypeError("instrument IDs must be unique")
    return parsed


def _window_ids(value: str) -> tuple[int, ...]:
    parsed = _instrument_ids(value)
    if any(item > 86_400_000 for item in parsed):
        raise argparse.ArgumentTypeError(
            "window IDs must be duration-ms values no greater than 86400000"
        )
    return parsed


def _parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="accept the live L2Flow Python/Polars IPC path"
    )
    parser.add_argument("--control-socket", required=True)
    parser.add_argument("--native-reader", required=True)
    parser.add_argument("--source-python", required=True)
    parser.add_argument("--report-json", required=True)
    parser.add_argument("--duration-seconds", required=True, type=_positive_int)
    parser.add_argument(
        "--instrument-ids", required=True, type=_instrument_ids
    )
    parser.add_argument(
        "--kline-window-ids", required=True, type=_window_ids
    )
    parser.add_argument("--max-batch-rows", type=_positive_int, default=16_384)
    parser.add_argument(
        "--latest-interval-ms", type=_positive_int, default=1_000
    )
    parser.add_argument(
        "--progress-interval-seconds", type=_positive_int, default=60
    )
    parser.add_argument("--empty-poll-sleep-us", type=int, default=100)
    args = parser.parse_args(argv)
    for name in (
        "control_socket",
        "native_reader",
        "source_python",
        "report_json",
    ):
        if not os.path.isabs(getattr(args, name)):
            parser.error(f"--{name.replace('_', '-')} must be absolute")
    if args.max_batch_rows > 1_048_576:
        parser.error("--max-batch-rows must not exceed 1048576")
    if args.duration_seconds > 86_400:
        parser.error("--duration-seconds must not exceed 86400")
    if args.empty_poll_sleep_us < 0 or args.empty_poll_sleep_us > 1_000_000:
        parser.error("--empty-poll-sleep-us must be in 0..1000000")
    if not os.path.isfile(args.native_reader):
        parser.error("--native-reader does not name a file")
    if not os.path.isdir(args.source_python):
        parser.error("--source-python does not name a directory")
    return args


@dataclass
class LinearHistogram:
    name: str
    maximum_exclusive_ns: int
    bucket_width_ns: int

    def __post_init__(self) -> None:
        if self.maximum_exclusive_ns <= 0 or self.bucket_width_ns <= 0:
            raise ValueError("histogram limits must be positive")
        self.buckets = array(
            "Q",
            [0]
            * math.ceil(
                self.maximum_exclusive_ns / self.bucket_width_ns
            ),
        )
        self.samples = 0
        self.invalid_samples = 0
        self.above_range = 0
        self.minimum_ns: int | None = None
        self.maximum_ns: int | None = None
        self.sum_ns = 0

    def add(self, value_ns: int) -> None:
        if value_ns < 0:
            self.invalid_samples += 1
            return
        self.samples += 1
        self.sum_ns += value_ns
        if self.minimum_ns is None or value_ns < self.minimum_ns:
            self.minimum_ns = value_ns
        if self.maximum_ns is None or value_ns > self.maximum_ns:
            self.maximum_ns = value_ns
        if value_ns >= self.maximum_exclusive_ns:
            self.above_range += 1
            return
        self.buckets[value_ns // self.bucket_width_ns] += 1

    def add_many(self, values, numpy) -> None:
        raw = numpy.asarray(values, dtype=numpy.int64)
        if raw.ndim != 1:
            raise ValueError(f"{self.name} batch must be one-dimensional")
        invalid = raw < 0
        invalid_count = int(numpy.count_nonzero(invalid))
        self.invalid_samples += invalid_count
        valid = raw[~invalid]
        if valid.size == 0:
            return
        count = int(valid.size)
        minimum = int(valid.min())
        maximum = int(valid.max())
        self.samples += count
        self.sum_ns += int(valid.sum(dtype=numpy.int64))
        if self.minimum_ns is None or minimum < self.minimum_ns:
            self.minimum_ns = minimum
        if self.maximum_ns is None or maximum > self.maximum_ns:
            self.maximum_ns = maximum
        in_range = valid < self.maximum_exclusive_ns
        self.above_range += count - int(numpy.count_nonzero(in_range))
        if not numpy.any(in_range):
            return
        indices = valid[in_range] // self.bucket_width_ns
        unique, counts = numpy.unique(indices, return_counts=True)
        for index, bucket_count in zip(unique, counts):
            self.buckets[int(index)] += int(bucket_count)

    def _quantile(self, numerator: int, denominator: int) -> dict[str, Any]:
        if self.samples == 0:
            return {
                "estimate_ns": 0,
                "lower_bound_ns": 0,
                "upper_bound_ns": 0,
                "clipped_above": False,
            }
        rank = (self.samples * numerator + denominator - 1) // denominator
        in_range = self.samples - self.above_range
        if rank > in_range:
            return {
                "estimate_ns": self.maximum_exclusive_ns,
                "lower_bound_ns": self.maximum_exclusive_ns,
                "upper_bound_ns": None,
                "clipped_above": True,
            }
        cumulative = 0
        for index, count in enumerate(self.buckets):
            cumulative += count
            if cumulative >= rank:
                lower = index * self.bucket_width_ns
                upper = min(
                    self.maximum_exclusive_ns - 1,
                    lower + self.bucket_width_ns - 1,
                )
                return {
                    "estimate_ns": lower + (upper - lower) // 2,
                    "lower_bound_ns": lower,
                    "upper_bound_ns": upper,
                    "clipped_above": False,
                }
        raise RuntimeError(f"{self.name} histogram accounting mismatch")

    def report(self) -> dict[str, Any]:
        return {
            "clock": "CLOCK_MONOTONIC",
            "samples": self.samples,
            "invalid_samples": self.invalid_samples,
            "above_histogram_range": self.above_range,
            "histogram_minimum_ns": 0,
            "histogram_maximum_ns": self.maximum_exclusive_ns - 1,
            "histogram_bucket_width_ns": self.bucket_width_ns,
            "minimum_ns": self.minimum_ns or 0,
            "maximum_ns": self.maximum_ns or 0,
            "mean_ns": self.sum_ns // self.samples if self.samples else 0,
            "p50": self._quantile(50, 100),
            "p90": self._quantile(90, 100),
            "p95": self._quantile(95, 100),
            "p99": self._quantile(99, 100),
            "p999": self._quantile(999, 1_000),
        }


def _session_dict(session) -> dict[str, Any]:
    return {
        "run_id": session.run_id.hex(),
        "session_epoch": session.session_epoch,
        "registry_version": session.registry_version,
        "registry_sha256": session.registry_sha256.hex(),
        "trade_date": session.trade_date,
        "server_state": session.server_state.name,
        "coverage_lost": session.coverage_lost,
        "kline_enabled": session.kline_enabled,
        "instrument_count": session.instrument_count,
        "window_count": session.window_count,
        "tick_ring_capacity": session.tick_ring_capacity,
        "tick_highest_published_sequence": (
            session.tick_highest_published_sequence
        ),
        "tick_contiguous_published_sequence": (
            session.tick_contiguous_published_sequence
        ),
        "oldest_tick_sequence": session.oldest_tick_sequence,
        "kline_generation": session.kline_generation,
        "heartbeat_monotonic_ns": session.heartbeat_monotonic_ns,
    }


def _latest_functional_check(
    client,
    instrument_ids: tuple[int, ...],
    window_ids: tuple[int, ...],
    polars,
) -> dict[str, int]:
    from l2flow_realtime import LatestStatus

    result: Counter[str] = Counter()
    snapshots = client.get_latest_snapshots(instrument_ids)
    snapshot_columns = snapshots.to_columns()
    if snapshot_columns["instrument_id"] != list(instrument_ids):
        raise RuntimeError("latest snapshot batch did not preserve request order")
    snapshot_frame = snapshots.to_polars().with_columns(
        polars.when(
            polars.col("status") == int(LatestStatus.AVAILABLE)
        )
        .then(polars.col("last_price_p6"))
        .otherwise(None)
        .alias("placeholder_factor_p6")
    )
    if snapshot_frame.height != len(instrument_ids):
        raise RuntimeError("snapshot Polars row count mismatch")
    if snapshot_frame.schema["instrument_id"] != polars.UInt32:
        raise RuntimeError("snapshot instrument_id dtype is not UInt32")
    if snapshot_frame.schema["placeholder_factor_p6"] != polars.Int64:
        raise RuntimeError("snapshot factor dtype is not nullable Int64")
    for item in snapshots:
        result[f"snapshot_status_{item.status.name.lower()}"] += 1
        if item.available:
            if item.value.common.instrument_id != item.requested_instrument_id:
                raise RuntimeError("snapshot instrument identity mismatch")
            if item.value.common.recv_monotonic_ns <= 0:
                raise RuntimeError("snapshot lacks callback monotonic time")
            if item.value.last_price.p6 != snapshot_frame.filter(
                polars.col("instrument_id") == item.requested_instrument_id
            )["placeholder_factor_p6"][0]:
                raise RuntimeError("snapshot placeholder factor mismatch")

    latest_ticks = client.get_latest_ticks(instrument_ids)
    tick_frame = latest_ticks.to_polars()
    if tick_frame.height != len(instrument_ids):
        raise RuntimeError("latest tick Polars row count mismatch")
    for item in latest_ticks:
        result[f"latest_tick_status_{item.status.name.lower()}"] += 1
        if item.available and (
            item.value.common.instrument_id != item.requested_instrument_id
            or item.value.common.tick_stream_sequence <= 0
        ):
            raise RuntimeError("latest tick identity/sequence mismatch")

    kline_instruments = []
    kline_windows = []
    for instrument_id in instrument_ids:
        for window_id in window_ids:
            kline_instruments.append(instrument_id)
            kline_windows.append(window_id)
    klines = client.get_latest_klines(kline_instruments, kline_windows)
    kline_frame = klines.to_polars()
    if kline_frame.height != len(kline_instruments):
        raise RuntimeError("latest KLine Polars row count mismatch")
    for item in klines:
        result[f"kline_status_{item.status.name.lower()}"] += 1
        if not item.available:
            continue
        bar = item.value
        if (
            bar.instrument_id != item.requested_instrument_id
            or bar.window_id != item.requested_window_id
            or bar.window_duration_ns != item.requested_window_id * 1_000_000
            or bar.window_start_ns_since_midnight
            >= bar.window_end_ns_since_midnight
            or bar.low_price_p6
            > min(bar.open_price_p6, bar.close_price_p6)
            or bar.high_price_p6
            < max(bar.open_price_p6, bar.close_price_p6)
            or bar.low_price_p6 > bar.high_price_p6
            or bar.trade_count <= 0
        ):
            raise RuntimeError("latest KLine invariant mismatch")
    result["latest_polls"] += 1
    return dict(result)


def _polars_tick_factor(frame, polars):
    quantity_present = (
        polars.col("quantity_present")
        if "quantity_present" in frame.columns
        else polars.col("quantity_raw").is_not_null()
    )
    expected = (
        polars.when(
            (polars.col("side") == 1)
            & quantity_present
        )
        .then(polars.col("quantity_raw"))
        .when(
            (polars.col("side") == 2)
            & quantity_present
        )
        .then(-polars.col("quantity_raw"))
        .otherwise(None)
    )
    factor = frame.with_columns(expected.alias("signed_quantity_raw"))
    if factor.height != frame.height:
        raise RuntimeError("tick factor row count mismatch")
    if factor.schema["signed_quantity_raw"] != polars.Int64:
        raise RuntimeError("signed quantity factor dtype is not Int64")
    mismatch = factor.select(
        (
            (
                polars.col("signed_quantity_raw").is_null()
                != expected.is_null()
            )
            | (
                polars.col("signed_quantity_raw").is_not_null()
                & (polars.col("signed_quantity_raw") != expected)
            )
        )
        .fill_null(False)
        .sum()
        .alias("mismatches"),
        polars.col("signed_quantity_raw")
        .is_not_null()
        .sum()
        .alias("non_null"),
    ).row(0)
    if mismatch[0] != 0:
        raise RuntimeError("signed quantity factor formula mismatch")
    return factor, int(mismatch[1])


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    temporary = Path(str(path) + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True)
        + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def run(args: argparse.Namespace) -> dict[str, Any]:
    sys.path.insert(0, args.source_python)
    import numpy as np
    import polars as pl
    from l2flow_realtime import (
        L2FlowClient,
        Market,
        MarketEventKind,
        ServerState,
        TickFactorRunner,
    )

    callback_to_python_read = LinearHistogram(
        "callback_to_python_read", 5_000_000_000, 5_000
    )
    callback_to_factor_complete = LinearHistogram(
        "callback_to_factor_complete", 5_000_000_000, 5_000
    )
    cursor_read_call = LinearHistogram(
        "cursor_read_call", 2_000_000_000, 1_000
    )
    polars_factor_call = LinearHistogram(
        "polars_factor_call", 2_000_000_000, 1_000
    )

    client = L2FlowClient.connect(
        args.control_socket,
        native_library=args.native_reader,
        timeout=5.0,
        stale_after_ns=5_000_000_000,
    )
    latest_client = L2FlowClient.connect(
        args.control_socket,
        native_library=args.native_reader,
        timeout=5.0,
        stale_after_ns=5_000_000_000,
    )
    try:
        start_session = client.session_info()
        if (
            start_session.server_state is not ServerState.ACTIVE
            or start_session.coverage_lost
            or not start_session.kline_enabled
        ):
            raise RuntimeError("router session is not healthy ACTIVE+KLine")
        if start_session.identity != latest_client.session_info().identity:
            raise RuntimeError("Python clients mapped different sessions")
        for instrument_id in args.instrument_ids:
            instrument = latest_client.get_instrument(instrument_id)
            if instrument.instrument_id != instrument_id:
                raise RuntimeError("instrument registry lookup mismatch")

        # Prepare an independent public TickFactorRunner.  It is exercised
        # after the formal clock starts and only once the primary cursor has
        # proved that live ticks exist.  This permits a before-open test window
        # without delaying the requested duration until the first auction
        # tick.
        runner_cursor = latest_client.open_tick_cursor(start="latest")
        runner = TickFactorRunner(
            runner_cursor,
            lambda frame: frame.with_columns(
                pl.when(pl.col("side") == 1)
                .then(pl.col("quantity_raw"))
                .when(pl.col("side") == 2)
                .then(-pl.col("quantity_raw"))
                .otherwise(None)
                .alias("signed_quantity_raw")
            ),
            max_rows=256,
            max_latency=1.0,
            poll_interval=0.0001,
            as_polars=True,
        )
        runner_rows = 0
        model_cursor = latest_client.open_tick_cursor(start="latest")

        cursor = client.open_tick_cursor(start="latest")
        start_monotonic_ns = time.monotonic_ns()
        start_realtime_ns = time.time_ns()
        deadline_ns = (
            start_monotonic_ns
            + args.duration_seconds * _NANOSECONDS_PER_SECOND
        )
        next_latest_ns = start_monotonic_ns
        next_progress_ns = (
            start_monotonic_ns
            + args.progress_interval_seconds * _NANOSECONDS_PER_SECOND
        )
        latest_totals: Counter[str] = Counter()
        event_kind_counts: Counter[int] = Counter()
        source_counts: Counter[int] = Counter()
        factor_non_null_rows = 0
        batch_count = 0
        empty_reads = 0
        tick_count = 0
        arrow_rows_checked = 0
        progress_lag_samples: list[int] = []
        previous_ingress_sequence = 0
        previous_source_sequences: dict[int, int] = {}
        first_tick_sequence = cursor.next_sequence
        last_tick_sequence = first_tick_sequence - 1

        while time.monotonic_ns() < deadline_ns:
            read_start_ns = time.monotonic_ns()
            batch = cursor.read_columns(args.max_batch_rows)
            read_complete_ns = time.monotonic_ns()
            cursor_read_call.add(read_complete_ns - read_start_ns)
            if not batch:
                empty_reads += 1
                if args.empty_poll_sleep_us:
                    time.sleep(args.empty_poll_sleep_us / 1_000_000)
            else:
                batch_count += 1
                if batch.first_sequence != last_tick_sequence + 1:
                    raise RuntimeError("Python cursor returned a sequence gap")
                records = batch.numpy_records()
                raw_bytes = np.frombuffer(
                    batch.wire_records, dtype=np.uint8
                ).reshape(len(batch), 336)
                if (
                    np.any(records["record_schema_version"] != 1)
                    or np.any(records["record_bytes"] != 336)
                    or np.any(records["instrument_id"] == 0)
                    or np.any(records["ingress_sequence"] == 0)
                    or np.any(records["tick_stream_sequence"] == 0)
                    or np.any(records["common_reserved"] != 0)
                    or np.any(records["tick_reserved_u32"] != 0)
                    or np.any(records["tick_reserved_u8"] != 0)
                    or np.any(raw_bytes[:, 118:128] != 0)
                    or np.any(raw_bytes[:, 187:192] != 0)
                    or np.any(raw_bytes[:, 203:208] != 0)
                ):
                    raise RuntimeError(
                        "column tick wire schema/reserved invariant failed"
                    )
                tick_sequences = records["tick_stream_sequence"]
                if (
                    int(tick_sequences[0]) != last_tick_sequence + 1
                    or (
                        len(batch) > 1
                        and np.any(
                            tick_sequences[1:]
                            != tick_sequences[:-1] + np.uint64(1)
                        )
                    )
                ):
                    raise RuntimeError("tick stream sequence is not dense")
                ingress_sequences = records["ingress_sequence"]
                if (
                    int(ingress_sequences[0])
                    <= previous_ingress_sequence
                    or (
                        len(batch) > 1
                        and np.any(
                            ingress_sequences[1:]
                            <= ingress_sequences[:-1]
                        )
                    )
                ):
                    raise RuntimeError("tick ingress sequence regressed")
                for source_slot in np.unique(records["source_slot"]):
                    source_mask = records["source_slot"] == source_slot
                    sequences = records["source_sequence"][source_mask]
                    source = int(source_slot)
                    if (
                        int(sequences[0])
                        <= previous_source_sequences.get(source, 0)
                        or (
                            sequences.size > 1
                            and np.any(sequences[1:] <= sequences[:-1])
                        )
                    ):
                        raise RuntimeError("per-source sequence regressed")
                    previous_source_sequences[source] = int(sequences[-1])
                    source_counts[source] += int(sequences.size)

                event_kinds = records["event_kind"]
                markets = records["market"]
                shanghai_tick = int(MarketEventKind.SHANGHAI_TICK)
                shenzhen_order = int(MarketEventKind.SHENZHEN_ORDER)
                shenzhen_transaction = int(
                    MarketEventKind.SHENZHEN_TRANSACTION
                )
                compatible = (
                    (event_kinds == shanghai_tick)
                    & (markets == int(Market.SHANGHAI))
                ) | (
                    (
                        (event_kinds == shenzhen_order)
                        | (event_kinds == shenzhen_transaction)
                    )
                    & (markets == int(Market.SHENZHEN))
                )
                if not np.all(compatible):
                    raise RuntimeError(
                        "tick event-kind/market compatibility mismatch"
                    )
                for kind, rows in zip(
                    *np.unique(event_kinds, return_counts=True)
                ):
                    event_kind_counts[int(kind)] += int(rows)

                recv_monotonic_ns = records["recv_monotonic_ns"]
                if np.any(recv_monotonic_ns <= 0):
                    raise RuntimeError(
                        "tick lacks callback monotonic time"
                    )
                quantity_present = (
                    records["quantity_valid"] == 1
                ) & (records["quantity_is_null"] == 0)
                if (
                    np.any(records["quantity_valid"] > 1)
                    or np.any(records["quantity_is_null"] > 1)
                ):
                    raise RuntimeError(
                        "tick quantity contains a non-canonical wire boolean"
                    )

                factor_start_ns = time.monotonic_ns()
                factor_input = pl.DataFrame(
                    [
                        pl.Series(
                            "side", records["side"], dtype=pl.UInt8
                        ),
                        pl.Series(
                            "quantity_raw",
                            records["quantity_raw"],
                            dtype=pl.Int64,
                        ),
                        pl.Series(
                            "quantity_present",
                            quantity_present,
                            dtype=pl.Boolean,
                        ),
                    ]
                )
                factor_frame, non_null = _polars_tick_factor(
                    factor_input, pl
                )
                factor_complete_ns = time.monotonic_ns()
                polars_factor_call.add(factor_complete_ns - factor_start_ns)
                if factor_frame.height != len(batch):
                    raise RuntimeError("Polars factor silently lost rows")
                callback_to_python_read.add_many(
                    read_complete_ns - recv_monotonic_ns,
                    np,
                )
                factor_non_null_rows += non_null
                callback_to_factor_complete.add_many(
                    factor_complete_ns - recv_monotonic_ns,
                    np,
                )
                previous_ingress_sequence = int(ingress_sequences[-1])
                last_tick_sequence = int(tick_sequences[-1])
                tick_count += len(batch)
                if arrow_rows_checked == 0:
                    model_batch = model_cursor.read(256)
                    if not model_batch:
                        continue
                    public_frame = model_batch.to_polars()
                    if (
                        public_frame.height != len(model_batch)
                        or public_frame.schema["instrument_id"]
                        != pl.UInt32
                        or public_frame.schema["quantity_raw"]
                        != pl.Int64
                    ):
                        raise RuntimeError(
                            "public tick Polars conversion mismatch"
                        )
                    arrow = model_batch.to_arrow()
                    if arrow.num_rows != len(model_batch):
                        raise RuntimeError("Arrow tick batch row count mismatch")
                    arrow_rows_checked = arrow.num_rows
                    model_cursor.close()
                if runner_rows == 0:
                    runner_result = runner.run_once()
                    if (
                        runner_result is None
                        or runner_result.value.height == 0
                    ):
                        raise RuntimeError(
                            "TickFactorRunner did not receive a live batch"
                        )
                    runner_rows = len(runner_result.input_batch)
                    runner_cursor.close()

            now_ns = time.monotonic_ns()
            if now_ns >= next_latest_ns:
                latest_totals.update(
                    _latest_functional_check(
                        latest_client,
                        args.instrument_ids,
                        args.kline_window_ids,
                        pl,
                    )
                )
                next_latest_ns = now_ns + (
                    args.latest_interval_ms * 1_000_000
                )
            if now_ns >= next_progress_ns:
                progress_session = client.session_info()
                producer_sequence = (
                    progress_session.tick_contiguous_published_sequence
                )
                consumer_lag_rows = max(
                    0, producer_sequence - last_tick_sequence
                )
                progress_lag_samples.append(consumer_lag_rows)
                progress = {
                    "type": "progress",
                    "elapsed_seconds": (
                        now_ns - start_monotonic_ns
                    )
                    / _NANOSECONDS_PER_SECOND,
                    "ticks": tick_count,
                    "batches": batch_count,
                    "next_sequence": cursor.next_sequence,
                    "producer_contiguous_sequence": producer_sequence,
                    "consumer_lag_rows": consumer_lag_rows,
                    "callback_to_python_read_p99_ns": (
                        callback_to_python_read.report()["p99"][
                            "estimate_ns"
                        ]
                    ),
                }
                print(json.dumps(progress, separators=(",", ":")), flush=True)
                next_progress_ns = now_ns + (
                    args.progress_interval_seconds
                    * _NANOSECONDS_PER_SECOND
                )

        end_monotonic_ns = time.monotonic_ns()
        end_realtime_ns = time.time_ns()
        end_session = client.session_info()
        if end_session.identity != start_session.identity:
            raise RuntimeError("router session changed during acceptance")
        if end_session.coverage_lost or end_session.server_state not in (
            ServerState.ACTIVE,
            ServerState.DRAINING,
            ServerState.STOPPED_CLEAN,
        ):
            raise RuntimeError("router became unhealthy during acceptance")
        measured_ns = end_monotonic_ns - start_monotonic_ns
        ending_consumer_lag_rows = max(
            0,
            end_session.tick_contiguous_published_sequence
            - last_tick_sequence,
        )
        required_ns = args.duration_seconds * _NANOSECONDS_PER_SECOND
        if measured_ns < required_ns:
            raise RuntimeError("Python test did not cover the requested duration")
        if tick_count == 0 or factor_non_null_rows == 0:
            raise RuntimeError("Python tick/factor path observed no usable data")
        if runner_rows == 0:
            raise RuntimeError("TickFactorRunner functional coverage missing")
        required_event_kinds = {
            int(MarketEventKind.SHANGHAI_TICK),
            int(MarketEventKind.SHENZHEN_ORDER),
            int(MarketEventKind.SHENZHEN_TRANSACTION),
        }
        if not required_event_kinds.issubset(event_kind_counts):
            raise RuntimeError("Python cursor did not observe all three tick kinds")
        if callback_to_python_read.invalid_samples != 0:
            raise RuntimeError("callback-to-Python latency contained invalid rows")
        if callback_to_factor_complete.invalid_samples != 0:
            raise RuntimeError("callback-to-factor latency contained invalid rows")
        if callback_to_python_read.samples != tick_count:
            raise RuntimeError("callback-to-Python latency sample count mismatch")
        if callback_to_factor_complete.samples != tick_count:
            raise RuntimeError("callback-to-factor latency sample count mismatch")
        if ending_consumer_lag_rows > args.max_batch_rows * 4:
            raise RuntimeError(
                "columnar Python consumer ended with excessive producer lag"
            )
        if arrow_rows_checked == 0 or latest_totals["latest_polls"] == 0:
            raise RuntimeError("Python conversion/latest functional coverage missing")
        if latest_totals["snapshot_status_available"] == 0:
            raise RuntimeError(
                "representative latest snapshots never became AVAILABLE"
            )
        if latest_totals["latest_tick_status_available"] == 0:
            raise RuntimeError(
                "representative latest ticks never became AVAILABLE"
            )
        if latest_totals["kline_status_available"] == 0:
            raise RuntimeError(
                "representative latest KLines never became AVAILABLE"
            )

        return {
            "schema_version": 1,
            "passed": True,
            "requested_duration_seconds": args.duration_seconds,
            "measured_duration_ns": measured_ns,
            "start_realtime_ns": start_realtime_ns,
            "end_realtime_ns": end_realtime_ns,
            "start_monotonic_ns": start_monotonic_ns,
            "end_monotonic_ns": end_monotonic_ns,
            "start_session": _session_dict(start_session),
            "end_session": _session_dict(end_session),
            "cursor": {
                "start_mode": "latest",
                "read_mode": "columnar_wire_block",
                "first_tick_sequence": first_tick_sequence,
                "last_tick_sequence": last_tick_sequence,
                "next_tick_sequence": cursor.next_sequence,
                "ticks": tick_count,
                "batches": batch_count,
                "empty_reads": empty_reads,
                "maximum_batch_rows": args.max_batch_rows,
                "sequence_gaps": 0,
                "overruns": 0,
                "ending_consumer_lag_rows": ending_consumer_lag_rows,
                "maximum_progress_consumer_lag_rows": (
                    max(progress_lag_samples) if progress_lag_samples else 0
                ),
                "progress_consumer_lag_rows": progress_lag_samples,
                "last_ingress_sequence": previous_ingress_sequence,
                "last_source_sequences": previous_source_sequences,
                "event_kind_counts": dict(event_kind_counts),
                "source_counts": dict(source_counts),
            },
            "factors": {
                "tick_factor": "per-row signed_quantity_raw; BUY=+raw, SELL=-raw, other/null=null",
                "economic_interpretation": "none; raw quantities are not aggregated across scale, unit, or instrument",
                "rows": tick_count,
                "non_null_rows": factor_non_null_rows,
                "formula_mismatches": 0,
                "tick_factor_runner_smoke_rows": runner_rows,
                "latest_snapshot_factor": "placeholder_factor_p6 equals nullable last_price_p6",
            },
            "functional": {
                "arrow_rows_checked": arrow_rows_checked,
                "public_polars_rows_checked": arrow_rows_checked,
                "latest": dict(latest_totals),
                "instrument_ids": list(args.instrument_ids),
                "kline_window_ids": list(args.kline_window_ids),
            },
            "latency_ns": {
                "callback_to_python_cursor_read": (
                    callback_to_python_read.report()
                ),
                "callback_to_polars_factor_complete": (
                    callback_to_factor_complete.report()
                ),
                "python_cursor_read_call": cursor_read_call.report(),
                "polars_factor_call": polars_factor_call.report(),
            },
            "latency_semantics": {
                "callback_to_python_cursor_read": "same-host Python CLOCK_MONOTONIC immediately after TickCursor.read_columns returned minus each wire record recv_monotonic_ns captured at router callback entry; Store/IPC/contiguous-prefix/native block copy/polling/batching are included; immutable generation publication and per-row Python dataclass construction are excluded",
                "callback_to_polars_factor_complete": "same callback entry timestamp through TickCursor.read_columns return and completion plus validation of the minimal Polars signed-quantity expression for the containing batch",
                "python_cursor_read_call": "Python CLOCK_MONOTONIC around TickCursor.read_columns, including session checks and one native contiguous wire-block copy; NumPy exposes a zero-copy view over the returned Python-owned bytes",
                "polars_factor_call": "Python CLOCK_MONOTONIC from the NumPy structured column view through construction of the minimal side/quantity Polars input and completion plus validation of the vectorized factor expression; full public TickBatch.to_polars and to_arrow conversions are separately checked on a live object batch",
                "batch_endpoint": "all rows in one batch use the clock observed after that batch operation completed, so older rows include their real wait inside the consumer batch",
            },
        }
    finally:
        latest_client.close()
        client.close()


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    report_path = Path(args.report_json)
    try:
        report = run(args)
    except BaseException as error:  # ensure a long run always leaves evidence
        report = {
            "schema_version": 1,
            "passed": False,
            "error_type": type(error).__name__,
            "error": str(error),
            "traceback": traceback.format_exc(),
        }
        _atomic_json(report_path, report)
        print(json.dumps(report, separators=(",", ":")), file=sys.stderr)
        return 1
    _atomic_json(report_path, report)
    print(json.dumps(report, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
