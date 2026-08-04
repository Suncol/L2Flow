#!/usr/bin/env python3
"""Run one sustained callback workload with same-service Polars reads."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import selectors
import statistics
import subprocess
import time
from pathlib import Path


def _fields(line: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for token in line.split()[1:]:
        key, separator, value = token.partition("=")
        if separator:
            if not key or key in result:
                raise ValueError(f"invalid line field in {line!r}")
            result[key] = value
    return result


def _unsigned(fields: dict[str, str], key: str) -> int:
    value = fields.get(key, "")
    if not value.isascii() or not value.isdecimal():
        raise ValueError(f"{key} is not an unsigned integer")
    return int(value, 10)


def _percentile(values: list[float], probability: float) -> float:
    ordered = sorted(values)
    if not ordered:
        raise ValueError("empty latency distribution")
    position = (len(ordered) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def _distribution_ns(values: list[int]) -> dict[str, float | int]:
    return {
        "n": len(values),
        "min_ns": min(values),
        "mean_ns": statistics.fmean(values),
        "p50_ns": statistics.median(values),
        "p95_r7_ns": _percentile([float(value) for value in values], 0.95),
        "p99_r7_ns": _percentile([float(value) for value in values], 0.99),
        "max_ns": max(values),
    }


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _process_rss_kib(pid: int) -> int:
    try:
        for line in Path(f"/proc/{pid}/status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1], 10)
    except (FileNotFoundError, PermissionError, ValueError):
        pass
    return 0


def _mem_available_kib() -> int:
    try:
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemAvailable:"):
                return int(line.split()[1], 10)
    except (FileNotFoundError, PermissionError, ValueError):
        pass
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--cpu-list", default="8-15")
    parser.add_argument("--rate", type=int, default=500_000)
    parser.add_argument("--duration-ms", type=int, default=300_000)
    parser.add_argument("--generation-interval-ms", type=int, default=1_000)
    parser.add_argument("--parallel-decoder-workers", type=int, default=0)
    parser.add_argument("--event-agg", action="store_true")
    parser.add_argument("--scenario", default="from_open")
    parser.add_argument("--timeout-seconds", type=int, default=900)
    return parser


def main() -> int:
    args = _parser().parse_args()
    binary = args.binary.resolve()
    output_dir = args.output_dir.resolve()
    if not binary.is_file():
        raise SystemExit("benchmark binary must be a file")
    if any(
        value <= 0
        for value in (
            args.rate,
            args.duration_ms,
            args.generation_interval_ms,
            args.timeout_seconds,
        )
    ):
        raise SystemExit("rate, durations, and timeout must be positive")
    if args.parallel_decoder_workers < 0:
        raise SystemExit("parallel decoder workers must be non-negative")
    if args.scenario not in ("from_open", "live_partial_no_recovery"):
        raise SystemExit("unsupported startup scenario")
    output_dir.mkdir(parents=True, exist_ok=True)
    if any(output_dir.iterdir()):
        raise SystemExit("output directory must be empty")

    command = [
        "taskset",
        "-c",
        args.cpu_list,
        str(binary),
        (
            "--throughput-event-polars-loaded-benchmark"
            if args.event_agg
            else "--throughput-polars-loaded-benchmark"
        ),
        str(args.rate),
        str(args.duration_ms),
        "256",
        "4",
        str(args.parallel_decoder_workers),
        "1",
        "65536",
        "32768",
        "64",
        "five_tuple_uniform",
        "fast_certified" if args.event_agg else "fast",
        args.scenario,
        str(args.generation_interval_ms),
    ]
    log_path = output_dir / "run.log"
    started = time.monotonic()
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env=os.environ.copy(),
    )
    if process.stdout is None:
        raise RuntimeError("benchmark stdout pipe was not created")
    selector = selectors.DefaultSelector()
    selector.register(process.stdout, selectors.EVENT_READ)
    lines: list[str] = []
    sample_count = 0
    peak_rss_kib = 0
    minimum_mem_available_kib = _mem_available_kib()
    last_progress = started
    timed_out = False
    with log_path.open("w", encoding="utf-8") as log:
        while True:
            now = time.monotonic()
            if now - started > args.timeout_seconds:
                timed_out = True
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                break
            events = selector.select(timeout=1.0)
            for key, _ in events:
                line = key.fileobj.readline()
                if not line:
                    continue
                log.write(line)
                log.flush()
                stripped = line.rstrip("\r\n")
                lines.append(stripped)
                if stripped.startswith(
                    "LOADED_EVENT_POLARS_SAMPLE "
                    if args.event_agg
                    else "LOADED_POLARS_SAMPLE "
                ):
                    sample_count += 1
            rss_kib = _process_rss_kib(process.pid)
            peak_rss_kib = max(peak_rss_kib, rss_kib)
            available_kib = _mem_available_kib()
            if available_kib:
                minimum_mem_available_kib = min(
                    minimum_mem_available_kib or available_kib,
                    available_kib,
                )
            now = time.monotonic()
            if now - last_progress >= 30.0:
                print(
                    f"elapsed_s={now - started:.1f} samples={sample_count} "
                    f"rss_gib={rss_kib / 1024 / 1024:.2f} "
                    f"mem_available_gib={available_kib / 1024 / 1024:.2f}",
                    flush=True,
                )
                last_progress = now
            if process.poll() is not None:
                remainder = process.stdout.read()
                if remainder:
                    log.write(remainder)
                    log.flush()
                    for line in remainder.splitlines():
                        lines.append(line)
                        if line.startswith(
                            "LOADED_EVENT_POLARS_SAMPLE "
                            if args.event_agg
                            else "LOADED_POLARS_SAMPLE "
                        ):
                            sample_count += 1
                break
    selector.close()
    elapsed_seconds = time.monotonic() - started
    if timed_out:
        raise ValueError(f"benchmark timed out after {elapsed_seconds:.1f}s")
    if process.returncode not in (0, 1):
        raise ValueError(
            f"benchmark exited {process.returncode}; inspect {log_path}"
        )

    env_lines = [line for line in lines if line.startswith("THROUGHPUT_ENV ")]
    result_lines = [
        line for line in lines if line.startswith("THROUGHPUT_RESULT ")
    ]
    ready_lines = [line for line in lines if line.startswith("PYTHON_READY ")]
    sample_prefix = "LOADED_POLARS_SAMPLE "
    sample_lines = [line for line in lines if line.startswith(sample_prefix)]
    event_load_lines = [
        line
        for line in lines
        if line.startswith("PYTHON_CERTIFIED_EVENT_POLARS_LOAD ")
    ]
    if not (len(env_lines) == len(result_lines) == len(ready_lines) == 1):
        raise ValueError("benchmark log lacks a unique env/result/ready line")
    env_fields = _fields(env_lines[0])
    result = _fields(result_lines[0])
    ready = _fields(ready_lines[0])
    planned = args.rate * args.duration_ms // 1_000
    target_met = _unsigned(result, "target_met") == 1
    expected_result = {
        "duration_ms": args.duration_ms,
        "planned_callbacks": planned,
        "loaded_polars_latency": 0 if args.event_agg else 1,
        "loaded_event_polars_latency": 1 if args.event_agg else 0,
        "loaded_polars_failed": 0,
        "loaded_polars_python_stopped": 1,
        "store_failed_appends": 0,
    }
    if target_met:
        expected_result.update(
            {
                "invoked_callbacks": planned,
                "accepted": planned,
                "decoded": planned,
                "applied": planned,
                "store_appended": planned,
                "history_scan_records": planned,
                "history_unique_ingress": planned,
                "complete_prefix": 1,
                "history_lossless": 1,
                "decoder_full_count": 0,
                "rejected": 0,
                "fatal_final": 0,
            }
        )
    elif not args.event_agg:
        raise ValueError("raw loaded benchmark did not meet its target")
    elif not (
        0 < _unsigned(result, "invoked_callbacks") < planned
        and _unsigned(result, "accepted")
        <= _unsigned(result, "invoked_callbacks")
        and _unsigned(result, "fatal_final") == 1
        and _unsigned(result, "complete_prefix") == 0
        and _unsigned(result, "decoder_full_count") > 0
    ):
        raise ValueError("target failure lacks a bounded decoder-capacity cause")
    if args.event_agg:
        expected_result.update(
            {
                "certified_event_generation_valid": 1,
                "loaded_event_final_valid": 1,
                "loaded_event_polars_load_valid": 1,
            }
        )
    for key, expected in expected_result.items():
        if _unsigned(result, key) != expected:
            raise ValueError(f"THROUGHPUT_RESULT {key} differs from {expected}")
    if _unsigned(
        env_fields,
        "loaded_event_polars_latency"
        if args.event_agg
        else "loaded_polars_latency",
    ) != 1:
        raise ValueError("requested loaded Polars mode was not reported")
    if env_fields.get("affinity") != ready.get("python_affinity"):
        raise ValueError("C++ and Python CPU affinity differ")
    samples: list[dict[str, int]] = []
    total_records = 0
    event_load: dict[str, int] = {}
    if args.event_agg:
        if len(event_load_lines) != 1 or sample_lines:
            raise ValueError(
                "benchmark log lacks one continuous Event Polars load"
            )
        event_fields = _fields(event_load_lines[0])
        event_keys = (
            "duration_ms",
            "elapsed_ns",
            "rows",
            "batches",
            "nonempty_batches",
            "first_event_sequence",
            "last_event_sequence",
            "next_event_sequence",
            "advertised_event_sequence",
            "event_backlog",
            "caught_up",
            "materialized_events_per_second",
            "strict_first_min_ns",
            "strict_first_p50_ns",
            "strict_first_p95_ns",
            "strict_first_p99_ns",
            "strict_first_max_ns",
            "strict_last_min_ns",
            "strict_last_p50_ns",
            "strict_last_p95_ns",
            "strict_last_p99_ns",
            "strict_last_max_ns",
        )
        event_load = {
            key: _unsigned(event_fields, key) for key in event_keys
        }
        total_records = event_load["rows"]
        if not (
            event_load["duration_ms"] == args.duration_ms
            and event_load["elapsed_ns"] >= args.duration_ms * 1_000_000
            and event_load["rows"] > 0
            and event_load["first_event_sequence"] == 1
            and event_load["last_event_sequence"] == event_load["rows"]
            and event_load["next_event_sequence"] == event_load["rows"] + 1
            and event_load["advertised_event_sequence"]
            >= event_load["rows"]
            and event_load["event_backlog"]
            == event_load["advertised_event_sequence"] - event_load["rows"]
            and event_load["caught_up"]
            == int(event_load["event_backlog"] == 0)
            and _unsigned(result, "loaded_polars_samples")
            == event_load["nonempty_batches"]
            and _unsigned(result, "loaded_event_polars_rows")
            == event_load["rows"]
            and _unsigned(result, "loaded_event_polars_final_backlog")
            == _unsigned(result, "certified_event_count")
            - event_load["rows"]
        ):
            raise ValueError("continuous Event Polars prefix does not reconcile")
    else:
        minimum_samples = max(
            1,
            args.duration_ms // args.generation_interval_ms - 1,
        )
        if len(sample_lines) < minimum_samples:
            raise ValueError("too few loaded Polars latency samples")
        if _unsigned(result, "loaded_polars_samples") != len(sample_lines):
            raise ValueError("loaded Polars sample count does not reconcile")
        previous_last = 0
        for expected_index, line in enumerate(sample_lines):
            fields = _fields(line)
            common_keys = (
                "sample",
                "first_ingress",
                "last_ingress",
                "strict_first_callback_to_polars_ns",
                "strict_last_callback_to_polars_ns",
            )
            mode_keys = (
                "generation",
                "records",
                "publication_to_polars_ns",
                "open_to_polars_ns",
            )
            row = {
                key: _unsigned(fields, key) for key in common_keys + mode_keys
            }
            if row["sample"] != expected_index:
                raise ValueError(
                    "loaded Polars sample indexes are not contiguous"
                )
            if row["records"] == 0 or not (
                previous_last
                < row["first_ingress"]
                <= row["last_ingress"]
            ):
                raise ValueError("loaded Polars ingress boundaries are invalid")
            if not (
                row["strict_first_callback_to_polars_ns"]
                >= row["strict_last_callback_to_polars_ns"]
                >= row["publication_to_polars_ns"]
                > 0
                and row["publication_to_polars_ns"]
                >= row["open_to_polars_ns"]
            ):
                raise ValueError("loaded Polars latency decomposition is invalid")
            previous_last = row["last_ingress"]
            total_records += row["records"]
            samples.append(row)
        if total_records != _unsigned(result, "loaded_polars_records"):
            raise ValueError("loaded Polars record count does not reconcile")

    achieved = float(result["achieved_offered_rps"])
    producer_elapsed_ns = _unsigned(result, "producer_elapsed_ns")
    invoked = _unsigned(result, "invoked_callbacks")
    recomputed = invoked * 1_000_000_000.0 / producer_elapsed_ns
    if abs(achieved - recomputed) > 0.001:
        raise ValueError("reported throughput does not match integer oracle")
    if process.returncode != (0 if target_met else 1):
        raise ValueError("benchmark exit status disagrees with target_met")
    if args.event_agg:
        metrics = {}
        for output_key, prefix in (
            ("strict_first_callback_to_polars_ns", "strict_first"),
            ("strict_last_callback_to_polars_ns", "strict_last"),
        ):
            metrics[output_key] = {
                "n": event_load["nonempty_batches"],
                "min_ns": event_load[f"{prefix}_min_ns"],
                "p50_ns": event_load[f"{prefix}_p50_ns"],
                "p95_r7_ns": event_load[f"{prefix}_p95_ns"],
                "p99_r7_ns": event_load[f"{prefix}_p99_ns"],
                "max_ns": event_load[f"{prefix}_max_ns"],
            }
    else:
        latency_keys = (
            "strict_first_callback_to_polars_ns",
            "strict_last_callback_to_polars_ns",
            "publication_to_polars_ns",
            "open_to_polars_ns",
        )
        metrics = {
            key: _distribution_ns([sample[key] for sample in samples])
            for key in latency_keys
        }
    report = {
        "provenance": {
            "binary": str(binary),
            "binary_sha256": _sha256(binary),
            "cpu_list": args.cpu_list,
            "scenario": args.scenario,
            "python_version": ready.get("python_version"),
            "polars_version": ready.get("polars_version"),
            "native_library_sha256": ready.get("native_library_sha256"),
            "probe_sha256": ready.get("probe_sha256"),
        },
        "configuration": {
            "target_rps": args.rate,
            "duration_ms": args.duration_ms,
            "generation_interval_ms": args.generation_interval_ms,
            "planned_callbacks": planned,
            "workload": "five_tuple_uniform",
            "sink": "fast_certified" if args.event_agg else "fast",
            "instruments_per_market": 256,
            "store_workers": 4,
            "parallel_decoder_workers": args.parallel_decoder_workers,
            "latency_visibility": (
                "same-service CERTIFIED Event history-to-tail Polars under "
                "callback load"
                if args.event_agg
                else "same-service periodic generation under callback load"
            ),
        },
        "throughput": {
            "target_met": target_met,
            "achieved_offered_rps": achieved,
            "history_ready_rps": float(result["history_ready_rps"]),
            "backlog_before_drain": _unsigned(result, "backlog_before_drain"),
            "decoder_high_water_max": _unsigned(result, "decoder_high_water_max"),
            "periodic_generation_cuts": _unsigned(
                result, "periodic_generation_cuts"
            ),
            "invoked_callbacks": _unsigned(result, "invoked_callbacks"),
            "accepted_callbacks": _unsigned(result, "accepted"),
            "fatal_final": _unsigned(result, "fatal_final") == 1,
            "decoder_full_count": _unsigned(result, "decoder_full_count"),
            "decoder_depth_before_drain": _unsigned(
                result, "decoder_depth_before_drain"
            ),
            "store_coverage_lost": (
                _unsigned(result, "store_coverage_lost") == 1
            ),
            "store_accounted_record_bytes": _unsigned(
                result, "store_accounted_record_bytes"
            ),
            "store_allocated_index_bytes": _unsigned(
                result, "store_allocated_index_bytes"
            ),
            "certified_event_count": (
                _unsigned(result, "certified_event_count")
                if args.event_agg
                else 0
            ),
            "certified_events_per_offer_second": (
                _unsigned(result, "certified_event_count")
                * 1_000_000_000.0
                / producer_elapsed_ns
                if args.event_agg
                else 0.0
            ),
            "certified_dropped_handoffs": (
                _unsigned(result, "certified_dropped")
                if args.event_agg
                else 0
            ),
            "certified_global_frozen": (
                _unsigned(result, "certified_global_frozen") == 1
                if args.event_agg
                else False
            ),
        },
        "loaded_polars": {
            "samples": (
                event_load["nonempty_batches"]
                if args.event_agg
                else len(samples)
            ),
            "records": (
                _unsigned(result, "certified_event_count")
                if args.event_agg
                else total_records
            ),
            "sample_drained_records": total_records,
            "unsampled_tail_records": (
                _unsigned(result, "loaded_event_polars_final_backlog")
                if args.event_agg
                else _unsigned(result, "loaded_polars_boundary_count")
                - total_records
            ),
            "backlog_at_load_stop": (
                event_load["event_backlog"] if args.event_agg else 0
            ),
            "materialized_events_per_second": (
                event_load["materialized_events_per_second"]
                if args.event_agg
                else 0
            ),
            "latency_ns": metrics,
        },
        "resource_observation": {
            "campaign_elapsed_seconds": elapsed_seconds,
            "peak_main_process_rss_kib": peak_rss_kib,
            "minimum_host_mem_available_kib": minimum_mem_available_kib,
        },
    }
    temporary = output_dir / "summary.json.tmp"
    temporary.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(output_dir / "summary.json")
    print(
        f"validated target_rps={args.rate} achieved_rps={achieved:.3f} "
        f"target_met={int(target_met)} "
        f"samples={report['loaded_polars']['samples']} "
        f"elapsed_s={elapsed_seconds:.1f}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
