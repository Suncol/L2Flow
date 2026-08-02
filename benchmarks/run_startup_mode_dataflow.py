#!/usr/bin/env python3
"""Benchmark from-open and no-recovery partial dataflow contracts.

Each trial is a fresh process. Throughput trials use the production queue,
worker, segment, and generation-interval defaults unless explicitly
overridden. Latency trials reuse the established callback-to-Polars protocol;
only sample zero is a callback-latency observation because later samples read
the same immutable generation again.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import importlib.util
import json
import math
import platform
import statistics
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable


SCENARIOS = ("from_open", "live_partial_no_recovery")
THROUGHPUT_ENV_PREFIX = "THROUGHPUT_ENV "
THROUGHPUT_RESULT_PREFIX = "THROUGHPUT_RESULT "
THROUGHPUT_TARGET_FRACTION = 0.98
# C++ emits both rates with fixed three-decimal formatting.  The small
# epsilon covers a binary floating-point representation at an exact half-unit
# rounding boundary; it is not a measurement tolerance.
REPORTED_RATE_ROUNDING_TOLERANCE_RPS = 0.000_500_1


def _fields(line: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    for token in line.split()[1:]:
        key, separator, value = token.partition("=")
        if not separator or not key:
            continue
        if key in fields:
            raise ValueError(f"duplicate field {key!r}")
        fields[key] = value
    return fields


def _single(
    lines: list[str], prefix: str, **required: str
) -> dict[str, str]:
    matches: list[dict[str, str]] = []
    for line in lines:
        if not line.startswith(prefix):
            continue
        fields = _fields(line)
        if all(fields.get(key) == value for key, value in required.items()):
            matches.append(fields)
    if len(matches) != 1:
        raise ValueError(
            f"expected one {prefix!r} line matching {required!r}; "
            f"got {len(matches)}"
        )
    return matches[0]


def _unsigned(fields: dict[str, str], key: str) -> int:
    value = fields.get(key)
    if value is None or not value.isascii() or not value.isdecimal():
        raise ValueError(f"{key} is not an unsigned decimal integer")
    return int(value, 10)


def _float(fields: dict[str, str], key: str) -> float:
    value = fields.get(key)
    if value is None:
        raise ValueError(f"missing floating-point field {key}")
    parsed = float(value)
    if not math.isfinite(parsed) or parsed < 0.0:
        raise ValueError(f"invalid non-negative finite field {key}")
    return parsed


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _requested_cpu_set(value: str) -> frozenset[int]:
    cpus: set[int] = set()
    for field in value.split(","):
        if not field:
            raise ValueError("CPU list contains an empty field")
        range_text, stride_separator, stride_text = field.partition(":")
        stride = 1
        if stride_separator:
            if not stride_text.isdecimal() or int(stride_text, 10) == 0:
                raise ValueError(f"invalid CPU stride: {field!r}")
            stride = int(stride_text, 10)
        first_text, range_separator, last_text = range_text.partition("-")
        if not first_text.isdecimal():
            raise ValueError(f"invalid CPU field: {field!r}")
        first = int(first_text, 10)
        last = first
        if range_separator:
            if not last_text.isdecimal():
                raise ValueError(f"invalid CPU range: {field!r}")
            last = int(last_text, 10)
            if last < first:
                raise ValueError(f"descending CPU range: {field!r}")
        cpus.update(range(first, last + 1, stride))
    if not cpus:
        raise ValueError("CPU list is empty")
    return frozenset(cpus)


def _reported_cpu_set(value: str) -> frozenset[int]:
    cpus_text, separator, count_text = value.partition(";count=")
    if not separator or not count_text.isdecimal():
        raise ValueError(f"invalid affinity telemetry: {value!r}")
    values = [] if not cpus_text else cpus_text.split(",")
    if any(not value.isdecimal() for value in values):
        raise ValueError(f"invalid affinity CPU: {value!r}")
    cpus = [int(value, 10) for value in values]
    if len(set(cpus)) != len(cpus) or len(cpus) != int(count_text, 10):
        raise ValueError(f"inconsistent affinity telemetry: {value!r}")
    return frozenset(cpus)


def _percentile_r7(values: list[float], probability: float) -> float:
    if not values or not 0.0 <= probability <= 1.0:
        raise ValueError("invalid percentile input")
    ordered = sorted(values)
    position = (len(ordered) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def _distribution(values: list[float]) -> dict[str, float | int]:
    if not values:
        raise ValueError("empty distribution")
    return {
        "n": len(values),
        "min": min(values),
        "p50": statistics.median(values),
        "mean": statistics.fmean(values),
        "p95_r7": _percentile_r7(values, 0.95),
        "max": max(values),
    }


def _expected_tuple_counts(workload: str, planned: int) -> list[int]:
    cycles = {
        "single_instrument": (2,),
        "five_tuple_uniform": (0, 1, 2, 3, 4),
        "four_source_balanced": (0, 1, 2, 3, 0, 1, 2, 4),
        "hot_shenzhen_tick_source": (3, 4),
    }
    cycle = cycles.get(workload)
    if cycle is None:
        raise ValueError(f"unsupported throughput workload {workload!r}")
    complete_cycles, remainder = divmod(planned, len(cycle))
    counts = [complete_cycles * cycle.count(index) for index in range(5)]
    for tuple_index in cycle[:remainder]:
        counts[tuple_index] += 1
    return counts


def _validate_reported_rate(
    path: Path,
    *,
    label: str,
    reported: float,
    numerator: int,
    elapsed_ns: int,
) -> float:
    if elapsed_ns <= 0:
        raise ValueError(f"{path}: {label} elapsed time is zero")
    recomputed = numerator * 1_000_000_000.0 / elapsed_ns
    if (
        abs(reported - recomputed)
        > REPORTED_RATE_ROUNDING_TOLERANCE_RPS
    ):
        raise ValueError(
            f"{path}: reported {label} {reported:.3f} differs from "
            f"integer-derived {recomputed:.9f}"
        )
    return recomputed


def _parse_throughput_log(
    path: Path,
    *,
    scenario: str,
    rate: int,
    duration_ms: int,
    instruments_per_market: int,
    store_workers: int,
    parallel_workers: int,
    decoder_queue: int,
    store_queue: int,
    segment_kib: int,
    workload: str,
    generation_interval_ms: int,
    requested_affinity: frozenset[int],
) -> dict[str, object]:
    lines = path.read_text(encoding="utf-8").splitlines()
    if any(line.startswith("FAIL:") for line in lines):
        raise ValueError(f"benchmark reported failure: {path}")
    env = _single(lines, THROUGHPUT_ENV_PREFIX)
    result = _single(lines, THROUGHPUT_RESULT_PREFIX)
    expected_state = "ACTIVE" if scenario == "from_open" else "LIVE_PARTIAL"
    expected_coverage = 1 if scenario == "from_open" else 0
    # Wire V2 generation flags: bit 0 is coverage-from-open and bit 1 is
    # record-coverage-complete. Partial generations therefore carry 2, while
    # from-open generations carry both bits (3).
    expected_flags = 3 if expected_coverage else 2
    expected_planned = rate * duration_ms // 1_000

    expected_text = {
        "scenario": scenario,
        "server_state": expected_state,
        "workload": workload,
        "sink": "fast",
    }
    for key, expected in expected_text.items():
        if env.get(key) != expected or result.get(key) != expected:
            raise ValueError(f"{path}: {key} request/telemetry mismatch")
    expected_numeric = {
        "target_rps": rate,
        "duration_ms": duration_ms,
        "planned_callbacks": expected_planned,
        "instruments_per_market": instruments_per_market,
        "store_worker_count": store_workers,
        "parallel_decoder_workers": parallel_workers,
        "decoder_queue_capacity_per_source": decoder_queue,
        "store_queue_capacity_per_source_worker": store_queue,
        "store_segment_kib": segment_kib,
        "coverage_from_open": expected_coverage,
        "online_recovery": 0,
        "generation_interval_ms": generation_interval_ms,
        "factor_generation_enabled": expected_coverage,
    }
    for key, expected in expected_numeric.items():
        if _unsigned(env, key) != expected:
            raise ValueError(f"{path}: environment {key} differs")
        if _unsigned(result, key) != expected:
            raise ValueError(f"{path}: result {key} differs")
    expected_env_only = {
        "native_sequence_base": (
            0 if scenario == "from_open" else 10_000_000
        ),
        "production_tuple_count": 5,
        "parallel_idle_inline": 1,
        "parallel_farm_activation_configured": 8_192,
        "parallel_farm_activation_effective": min(
            8_192,
            decoder_queue - max(1, decoder_queue // 4),
        ),
        "tick_ring_capacity": 262_144,
    }
    for key, expected in expected_env_only.items():
        if _unsigned(env, key) != expected:
            raise ValueError(f"{path}: environment {key} differs")
    if _unsigned(result, "parallel_idle_inline") != int(
        parallel_workers != 0
    ):
        raise ValueError(f"{path}: result parallel_idle_inline differs")
    if env.get("callback_contract") != "serialized":
        raise ValueError(f"{path}: callback contract differs")
    if env.get("pacing") != "absolute_deadline_one_based_no_batch_wait":
        raise ValueError(f"{path}: pacing contract differs")
    if env.get("clock") != "CLOCK_MONOTONIC":
        raise ValueError(f"{path}: clock contract differs")
    if _reported_cpu_set(env.get("affinity", "")) != requested_affinity:
        raise ValueError(f"{path}: CPU affinity differs from request")

    required_ones = (
        "target_met",
        "process_survived",
        "accepting_before_drain",
        "steady_state_met",
        "complete_prefix",
        "stopped_clean",
        "certified_idle",
        "certified_healthy",
        "final_cut_published",
        "generation_sequence_valid",
        "history_endpoint_valid",
        "history_generation_valid",
        "history_scan_cursor_opened",
        "history_scan_explicit_eof",
        "history_source_counts_valid",
        "history_lossless",
    )
    required_zeros = (
        "fatal_during_offer",
        "fatal_final",
        "message_patch_failed",
        "rejected",
        "post_cut",
        "store_failed_appends",
        "store_coverage_lost",
        "decoder_full_count",
        "service_failed",
        "periodic_generation_failed",
        "periodic_cut_error",
        "periodic_generation_error",
        "final_cut_error",
        "final_generation_error",
        "history_control_status",
        "history_duplicate_ingress",
        "history_out_of_range_ingress",
        "history_invalid_source_slots",
        "certified_dropped",
        "certified_frozen_channels",
        "certified_global_frozen",
    )
    for key in required_ones:
        if _unsigned(result, key) != 1:
            raise ValueError(f"{path}: {key} is not one")
    for key in required_zeros:
        if _unsigned(result, key) != 0:
            raise ValueError(f"{path}: {key} is not zero")
    if (
        _unsigned(result, "final_factor_generation_present")
        != expected_coverage
    ):
        raise ValueError(f"{path}: final Factor-generation contract differs")

    for key in (
        "invoked_callbacks",
        "accepted",
        "decoded",
        "applied",
        "store_appended",
        "history_scan_records",
        "history_unique_ingress",
    ):
        if _unsigned(result, key) != expected_planned:
            raise ValueError(f"{path}: {key} does not close to planned")
    source_counts = [
        _unsigned(result, f"history_source{source}_records")
        for source in range(4)
    ]
    tuple_counts = [
        _unsigned(result, f"tuple{index}_offered") for index in range(5)
    ]
    expected_tuples = _expected_tuple_counts(workload, expected_planned)
    if tuple_counts != expected_tuples:
        raise ValueError(
            f"{path}: tuple distribution {tuple_counts} differs from "
            f"independent workload oracle {expected_tuples}"
        )
    expected_sources = [
        tuple_counts[0],
        tuple_counts[1],
        tuple_counts[2],
        tuple_counts[3] + tuple_counts[4],
    ]
    if source_counts != expected_sources or sum(source_counts) != expected_planned:
        raise ValueError(f"{path}: source/tuple cardinalities do not close")
    if _unsigned(result, "history_endpoint_flags") != expected_flags:
        raise ValueError(f"{path}: generation coverage flags differ")
    periodic_cuts = _unsigned(result, "periodic_generation_cuts")
    if generation_interval_ms > 0 and duration_ms >= generation_interval_ms:
        if periodic_cuts == 0:
            raise ValueError(f"{path}: periodic generation cut was not exercised")
    if _unsigned(result, "final_generation") != periodic_cuts + 1:
        raise ValueError(f"{path}: final generation is not contiguous")

    invoked = _unsigned(result, "invoked_callbacks")
    producer_elapsed_ns = _unsigned(result, "producer_elapsed_ns")
    history_ready_elapsed_ns = _unsigned(
        result, "history_ready_elapsed_ns"
    )
    reported_offered_rps = _float(result, "achieved_offered_rps")
    reported_history_ready_rps = _float(result, "history_ready_rps")
    achieved_offered_rps = _validate_reported_rate(
        path,
        label="achieved_offered_rps",
        reported=reported_offered_rps,
        numerator=invoked,
        elapsed_ns=producer_elapsed_ns,
    )
    history_ready_rps = _validate_reported_rate(
        path,
        label="history_ready_rps",
        reported=reported_history_ready_rps,
        numerator=expected_planned,
        elapsed_ns=history_ready_elapsed_ns,
    )
    recomputed_target_met = (
        invoked == expected_planned
        and achieved_offered_rps
        >= rate * THROUGHPUT_TARGET_FRACTION
        and _unsigned(result, "complete_prefix") == 1
        and _unsigned(result, "decoder_full_count") == 0
        and _unsigned(result, "message_patch_failed") == 0
        and _unsigned(result, "steady_state_met") == 1
    )
    if _unsigned(result, "target_met") != int(recomputed_target_met):
        raise ValueError(f"{path}: target_met differs from its integer oracle")

    row: dict[str, object] = dict(result)
    row.update(
        {
            "scenario": scenario,
            "target_rps": rate,
            "duration_ms": duration_ms,
            "planned_callbacks": expected_planned,
            "achieved_offered_rps": achieved_offered_rps,
            "reported_achieved_offered_rps": reported_offered_rps,
            "history_ready_rps": history_ready_rps,
            "reported_history_ready_rps": reported_history_ready_rps,
            "producer_elapsed_ns": producer_elapsed_ns,
            "history_ready_elapsed_ns": history_ready_elapsed_ns,
            "backlog_before_drain": _unsigned(result, "backlog_before_drain"),
            "final_drain_and_cut_elapsed_ns": _unsigned(
                result, "final_drain_and_cut_elapsed_ns"
            ),
            "history_integrity_validation_elapsed_ns": _unsigned(
                result, "history_integrity_validation_elapsed_ns"
            ),
            "periodic_generation_cuts": periodic_cuts,
        }
    )
    return row


def _load_latency_parser() -> Callable[..., tuple[dict, dict]]:
    path = Path(__file__).resolve().with_name("compare_callback_polars_ab.py")
    specification = importlib.util.spec_from_file_location(
        "l2flow_callback_polars_parser", path
    )
    if specification is None or specification.loader is None:
        raise RuntimeError(f"cannot load latency parser {path}")
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module._parse_latency_log  # type: ignore[attr-defined]


def _latency_decomposition(path: Path) -> dict[str, int]:
    lines = path.read_text(encoding="utf-8").splitlines()
    raw = _single(lines, "PYTHON_RAW_POLARS_SAMPLE ", sample="0")
    order = _single(lines, "PYTHON_DERIVED_POLARS_SAMPLE ")
    raw_boundary = _single(
        lines,
        "POLARS_BOUNDARY ",
        workload="raw_batch_4096_all_columns",
    )
    order_boundary = _single(
        lines,
        "POLARS_BOUNDARY ",
        workload="derived_complete_order_lifecycle",
    )
    result: dict[str, int] = {}
    for label, sample, boundary in (
        ("raw", raw, raw_boundary),
        ("order", order, order_boundary),
    ):
        published = _unsigned(sample, "history_published_monotonic_ns")
        ready = _unsigned(sample, "polars_ready_ns")
        first_origin = _unsigned(boundary, "first_caller_before_callback_ns")
        last_origin = _unsigned(boundary, "last_caller_before_callback_ns")
        if not 0 < first_origin <= last_origin <= published <= ready:
            raise ValueError(f"{path}: invalid strict {label} time ordering")
        result[f"{label}_strict_first_to_publication_ns"] = (
            published - first_origin
        )
        result[f"{label}_strict_last_to_publication_ns"] = (
            published - last_origin
        )
        result[f"{label}_publication_to_polars_ns"] = ready - published
        result[f"{label}_record_first_to_polars_ns"] = _unsigned(
            sample, "first_callback_to_polars_ns"
        )
        result[f"{label}_record_last_to_polars_ns"] = _unsigned(
            sample, "last_callback_to_polars_ns"
        )
    return result


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    temporary = path.with_name(path.name + ".tmp")
    keys = sorted({key for row in rows for key in row})
    with temporary.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)
    temporary.replace(path)


def _write_text_atomic(path: Path, content: str) -> None:
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(content, encoding="utf-8")
    temporary.replace(path)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--cpu-list", default="8-15")
    parser.add_argument(
        "--rates", type=int, nargs="+", default=[50_000, 100_000, 200_000, 300_000, 500_000]
    )
    parser.add_argument("--throughput-duration-ms", type=int, default=2_000)
    parser.add_argument("--throughput-repeats", type=int, default=3)
    parser.add_argument("--latency-repeats", type=int, default=10)
    parser.add_argument("--instruments-per-market", type=int, default=256)
    parser.add_argument("--store-workers", type=int, default=4)
    parser.add_argument("--parallel-workers", type=int, default=0)
    parser.add_argument("--decoder-queue", type=int, default=65_536)
    parser.add_argument("--store-queue", type=int, default=32_768)
    parser.add_argument("--segment-kib", type=int, default=64)
    parser.add_argument("--workload", default="five_tuple_uniform")
    parser.add_argument("--generation-interval-ms", type=int, default=1_000)
    parser.add_argument("--timeout-seconds", type=int, default=240)
    return parser


def main() -> int:
    args = _parser().parse_args()
    binary = args.binary.resolve()
    output_dir = args.output_dir.resolve()
    if not binary.is_file():
        raise SystemExit("benchmark binary must be a file")
    positive = (
        args.throughput_duration_ms,
        args.throughput_repeats,
        args.latency_repeats,
        args.instruments_per_market,
        args.store_workers,
        args.decoder_queue,
        args.store_queue,
        args.segment_kib,
        args.generation_interval_ms,
        args.timeout_seconds,
    )
    if any(value <= 0 for value in positive) or any(rate <= 0 for rate in args.rates):
        raise SystemExit("rates, durations, repeats, capacities, and timeout must be positive")
    if args.parallel_workers < 0:
        raise SystemExit("parallel worker count cannot be negative")
    affinity = _requested_cpu_set(args.cpu_list)
    output_dir.mkdir(parents=True, exist_ok=True)
    if any(output_dir.iterdir()):
        raise SystemExit(
            "output directory must be empty so an interrupted run cannot "
            "be mistaken for a complete campaign"
        )
    binary_sha256 = _sha256(binary)

    throughput_rows: list[dict[str, object]] = []
    for rate_index, rate in enumerate(args.rates):
        for repeat in range(1, args.throughput_repeats + 1):
            order = list(SCENARIOS)
            if (rate_index + repeat) % 2 == 0:
                order.reverse()
            for scenario in order:
                log_path = output_dir / (
                    f"throughput_{rate}_{scenario}_run{repeat}.log"
                )
                command = [
                    "taskset",
                    "-c",
                    args.cpu_list,
                    str(binary),
                    "--throughput-profile-benchmark",
                    str(rate),
                    str(args.throughput_duration_ms),
                    str(args.instruments_per_market),
                    str(args.store_workers),
                    str(args.parallel_workers),
                    "1",
                    str(args.decoder_queue),
                    str(args.store_queue),
                    str(args.segment_kib),
                    args.workload,
                    "fast",
                    scenario,
                    str(args.generation_interval_ms),
                ]
                completed = subprocess.run(
                    command,
                    check=False,
                    capture_output=True,
                    text=True,
                    timeout=args.timeout_seconds,
                )
                log_path.write_text(
                    completed.stdout + completed.stderr,
                    encoding="utf-8",
                )
                if completed.returncode != 0:
                    raise ValueError(
                        f"throughput trial exited {completed.returncode}: {log_path}"
                    )
                row = _parse_throughput_log(
                    log_path,
                    scenario=scenario,
                    rate=rate,
                    duration_ms=args.throughput_duration_ms,
                    instruments_per_market=args.instruments_per_market,
                    store_workers=args.store_workers,
                    parallel_workers=args.parallel_workers,
                    decoder_queue=args.decoder_queue,
                    store_queue=args.store_queue,
                    segment_kib=args.segment_kib,
                    workload=args.workload,
                    generation_interval_ms=args.generation_interval_ms,
                    requested_affinity=affinity,
                )
                row.update({"repeat": repeat, "log": log_path.name})
                throughput_rows.append(row)
                print(
                    f"throughput scenario={scenario} rate={rate} repeat={repeat} "
                    f"offered={row['achieved_offered_rps']:.3f} "
                    f"history_ready={row['history_ready_rps']:.3f} lossless=1"
                )

    parse_latency_log = _load_latency_parser()
    latency_rows: list[dict[str, object]] = []
    for repeat in range(1, args.latency_repeats + 1):
        order = list(SCENARIOS)
        if repeat % 2 == 0:
            order.reverse()
        for scenario in order:
            log_path = output_dir / f"latency_{scenario}_run{repeat}.log"
            command = [
                "taskset",
                "-c",
                args.cpu_list,
                str(binary),
                "--callback-polars-latency-benchmark-workers",
                str(args.parallel_workers),
                scenario,
            ]
            completed = subprocess.run(
                command,
                check=False,
                capture_output=True,
                text=True,
                timeout=args.timeout_seconds,
            )
            log_path.write_text(
                completed.stdout + completed.stderr,
                encoding="utf-8",
            )
            if completed.returncode != 0:
                raise ValueError(
                    f"latency trial exited {completed.returncode}: {log_path}"
                )
            metrics, metadata = parse_latency_log(
                log_path,
                expected_workers=args.parallel_workers,
                expected_affinity=affinity,
                expected_scenario=scenario,
            )
            row: dict[str, object] = {
                "scenario": scenario,
                "repeat": repeat,
                "log": log_path.name,
                **metrics,
                **_latency_decomposition(log_path),
            }
            for key, value in metadata.items():
                row[f"meta_{key}"] = value
            latency_rows.append(row)
            print(
                f"latency scenario={scenario} repeat={repeat} "
                f"raw_strict_last_ns={metrics['batch_strict_last_callback_to_polars_ns']} "
                f"order_strict_last_ns={metrics['order_strict_last_callback_to_polars_ns']}"
            )

    if _sha256(binary) != binary_sha256:
        raise ValueError("benchmark binary changed during the campaign")
    stable_latency_provenance: dict[str, str] = {}
    for key in (
        "meta_python_executable_sha256",
        "meta_native_library_sha256",
        "meta_probe_sha256",
        "meta_python_version",
        "meta_python_implementation",
        "meta_polars_version",
    ):
        values = {str(row[key]) for row in latency_rows}
        if len(values) != 1:
            raise ValueError(f"latency provenance changed during campaign: {key}")
        stable_latency_provenance[key.removeprefix("meta_")] = values.pop()

    throughput_summary: list[dict[str, object]] = []
    for scenario in SCENARIOS:
        for rate in args.rates:
            rows = [
                row
                for row in throughput_rows
                if row["scenario"] == scenario and row["target_rps"] == rate
            ]
            if len(rows) != args.throughput_repeats:
                raise ValueError("incomplete throughput group")
            offered = [float(row["achieved_offered_rps"]) for row in rows]
            ready = [float(row["history_ready_rps"]) for row in rows]
            throughput_summary.append(
                {
                    "scenario": scenario,
                    "target_rps": rate,
                    "n": len(rows),
                    "planned_callbacks_per_run": rows[0]["planned_callbacks"],
                    "lossless_runs": len(rows),
                    "target_met_runs": len(rows),
                    "offered_rps_p50": statistics.median(offered),
                    "offered_rps_min": min(offered),
                    "history_ready_rps_p50": statistics.median(ready),
                    "history_ready_rps_min": min(ready),
                    "backlog_at_offer_end_max": max(
                        int(row["backlog_before_drain"]) for row in rows
                    ),
                    "final_pipeline_drain_and_generation_cut_p95_ms": _percentile_r7(
                        [
                            int(row["final_drain_and_cut_elapsed_ns"])
                            / 1_000_000.0
                            for row in rows
                        ],
                        0.95,
                    ),
                    "history_integrity_validation_p95_ms": _percentile_r7(
                        [
                            int(
                                row[
                                    "history_integrity_validation_elapsed_ns"
                                ]
                            )
                            / 1_000_000.0
                            for row in rows
                        ],
                        0.95,
                    ),
                    "periodic_cut_min": min(
                        int(row["periodic_generation_cuts"]) for row in rows
                    ),
                }
            )

    latency_summary: dict[str, dict[str, object]] = {}
    latency_metrics = (
        "batch_strict_first_callback_to_polars_ns",
        "batch_strict_last_callback_to_polars_ns",
        "batch_first_callback_to_polars_ns",
        "batch_last_callback_to_polars_ns",
        "raw_strict_last_to_publication_ns",
        "raw_publication_to_polars_ns",
        "order_strict_first_callback_to_polars_ns",
        "order_strict_last_callback_to_polars_ns",
        "order_first_callback_to_polars_ns",
        "order_last_callback_to_polars_ns",
        "order_strict_last_to_publication_ns",
        "order_publication_to_polars_ns",
    )
    for scenario in SCENARIOS:
        rows = [row for row in latency_rows if row["scenario"] == scenario]
        if len(rows) != args.latency_repeats:
            raise ValueError("incomplete latency group")
        latency_summary[scenario] = {
            metric: _distribution([float(row[metric]) for row in rows])
            for metric in latency_metrics
        }

    provenance = {
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "binary": str(binary),
        "binary_sha256": binary_sha256,
        **stable_latency_provenance,
        "host": platform.node(),
        "platform": platform.platform(),
        "cpu_list": args.cpu_list,
        "scenarios": list(SCENARIOS),
        "rates": args.rates,
        "throughput_duration_ms": args.throughput_duration_ms,
        "throughput_repeats": args.throughput_repeats,
        "latency_repeats": args.latency_repeats,
        "instruments_per_market": args.instruments_per_market,
        "store_workers": args.store_workers,
        "parallel_workers": args.parallel_workers,
        "decoder_queue": args.decoder_queue,
        "store_queue": args.store_queue,
        "segment_kib": args.segment_kib,
        "workload": args.workload,
        "sink": "fast",
        "generation_interval_ms": args.generation_interval_ms,
        "throughput_metric": "paced serialized callback offer rate",
        "throughput_pacing": (
            "callback i uses one-based absolute deadline "
            "start + (i + 1) / target_rate"
        ),
        "throughput_target_fraction": THROUGHPUT_TARGET_FRACTION,
        "reported_rate_rounding_tolerance_rps": (
            REPORTED_RATE_ROUNDING_TOLERANCE_RPS
        ),
        "history_ready_metric": (
            "planned records divided by pacing-epoch through final immutable "
            "generation publication duration"
        ),
        "latency_visibility": "forced generation cut, warm worker/connection",
        "throughput_and_latency_workloads": "separate fresh processes",
        "latency_sample_independence": "one sample-zero per fresh process",
    }
    report = {
        "provenance": provenance,
        "throughput_summary": throughput_summary,
        "latency_summary_ns": latency_summary,
    }
    _write_text_atomic(
        output_dir / "throughput_runs.json",
        json.dumps(throughput_rows, indent=2, sort_keys=True) + "\n",
    )
    _write_text_atomic(
        output_dir / "latency_runs.json",
        json.dumps(latency_rows, indent=2, sort_keys=True) + "\n",
    )
    _write_csv(output_dir / "throughput_runs.csv", throughput_rows)
    _write_csv(output_dir / "latency_runs.csv", latency_rows)
    _write_csv(output_dir / "throughput_summary.csv", throughput_summary)
    # summary.json is the completion marker and is published last.
    _write_text_atomic(
        output_dir / "summary.json",
        json.dumps(report, indent=2, sort_keys=True) + "\n",
    )
    print(f"wrote validated report to {output_dir / 'summary.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
