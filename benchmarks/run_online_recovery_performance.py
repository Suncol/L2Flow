#!/usr/bin/env python3
"""Run and validate online-recovery throughput and Polars latency trials."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import platform
import statistics
import subprocess
from datetime import datetime, timezone
from pathlib import Path


def _fields(line: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for token in line.split()[1:]:
        key, separator, value = token.partition("=")
        if separator:
            if key in result:
                raise ValueError(f"duplicate field {key}")
            result[key] = value
    return result


def _single(lines: list[str], prefix: str) -> dict[str, str]:
    matches = [_fields(line) for line in lines if line.startswith(prefix)]
    if len(matches) != 1:
        raise ValueError(f"expected one {prefix!r} line, got {len(matches)}")
    return matches[0]


def _unsigned(fields: dict[str, str], key: str) -> int:
    value = fields.get(key, "")
    if not value.isascii() or not value.isdecimal():
        raise ValueError(f"{key} is not an unsigned decimal integer")
    return int(value, 10)


def _finite(fields: dict[str, str], key: str) -> float:
    value = float(fields.get(key, "nan"))
    if not math.isfinite(value) or value < 0.0:
        raise ValueError(f"{key} is not non-negative and finite")
    return value


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _r7(values: list[float], probability: float) -> float:
    ordered = sorted(values)
    position = (len(ordered) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (
        position - lower
    )


def _distribution(values: list[float]) -> dict[str, float | int]:
    if not values:
        raise ValueError("empty distribution")
    return {
        "n": len(values),
        "min": min(values),
        "p50": statistics.median(values),
        "mean": statistics.fmean(values),
        "p95_r7": _r7(values, 0.95),
        "p99_r7": _r7(values, 0.99),
        "max": max(values),
    }


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    keys = sorted({key for row in rows for key in row})
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)
    temporary.replace(path)


def _write_json(path: Path, value: object) -> None:
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _validate_throughput_failure(
    path: Path,
    *,
    rate: int,
    duration_ms: int,
    replay_records: int,
    parallel_workers: int,
    returncode: int,
) -> dict[str, object]:
    lines = path.read_text(encoding="utf-8").splitlines()
    env = _single(lines, "ONLINE_RECOVERY_THROUGHPUT_ENV ")
    result = _single(lines, "ONLINE_RECOVERY_THROUGHPUT_RESULT ")
    planned = rate * duration_ms // 1_000
    expected_env = {
        "mode": "active",
        "target_rps": str(rate),
        "duration_ms": str(duration_ms),
        "planned_callbacks": str(planned),
        "replay_records": str(replay_records),
        "parallel_decoder_workers": str(parallel_workers),
        "callback_contract": "serialized",
        "pacing": "absolute_deadline_one_based_no_batch_wait",
        "preview_state": "LIVE_PARTIAL",
        "recovered_state": "ACTIVE_after_promotion",
        "live_journal_queue_capacity": "65536",
        "clock": "CLOCK_MONOTONIC",
    }
    for key, expected in expected_env.items():
        if env.get(key) != expected:
            raise ValueError(f"{path}: environment {key} differs")
    if result.get("mode") != "active":
        raise ValueError(f"{path}: failure mode differs")
    expected_result = {
        "target_rps": rate,
        "duration_ms": duration_ms,
        "planned_callbacks": planned,
        "offer_target_met": 0,
        "scenario_pass": 0,
        "process_survived": 1,
        "fatal_during_offer": 1,
        "steady_state_met": 0,
        "preview_fatal": 1,
        "preview_store_failed": 0,
        "history_complete": 0,
        "exact_preview_prefix": 0,
        "exact_journal": 0,
    }
    for key, expected in expected_result.items():
        if _unsigned(result, key) != expected:
            raise ValueError(f"{path}: failure field {key} differs")
    invoked = _unsigned(result, "invoked_callbacks")
    accepted = _unsigned(result, "preview_accepted")
    rejected = _unsigned(result, "preview_rejected")
    journal_accepted = _unsigned(result, "journal_accepted")
    elapsed_ns = _unsigned(result, "producer_elapsed_ns")
    achieved = _finite(result, "achieved_offered_rps")
    recomputed = invoked * 1_000_000_000.0 / elapsed_ns
    if (
        returncode != 1
        or not 0 < accepted < invoked < planned
        or accepted + rejected != invoked
        or journal_accepted != accepted
        or _unsigned(result, "journal_queue_high_water") != 65_536
        or abs(achieved - recomputed) > 1.0
    ):
        raise ValueError(f"{path}: journal-capacity failure did not reconcile")
    if not any("first fatal reason=live_ingress_capture" in line for line in lines):
        raise ValueError(f"{path}: missing capture-failure diagnostic")
    offer_pacing_met = recomputed >= rate * 0.98
    return {
        **result,
        "target_rps": rate,
        "planned_callbacks": planned,
        "invoked_callbacks": invoked,
        "preview_accepted": accepted,
        "preview_rejected": rejected,
        "producer_elapsed_ns": elapsed_ns,
        "achieved_offered_rps": recomputed,
        "offer_pacing_met": offer_pacing_met,
        "failure_class": (
            "live_journal_queue_capacity_exhausted"
            if offer_pacing_met
            else "offer_rate_shortfall_and_live_journal_queue_capacity_exhausted"
        ),
        "full_path_pass": False,
    }


def _validate_latency(
    path: Path,
    *,
    replay_records: int,
    warmup: int,
    samples: int,
    parallel_workers: int,
    returncode: int,
) -> dict[str, object]:
    lines = path.read_text(encoding="utf-8").splitlines()
    env = _single(lines, "ONLINE_RECOVERY_FAST_ENV ")
    probe = _single(lines, "ONLINE_RECOVERY_POLARS_RESULT ")
    boundary = _single(lines, "ONLINE_RECOVERY_POLARS_BOUNDARY ")
    result = _single(lines, "ONLINE_RECOVERY_FAST_RESULT ")
    expected_records = replay_records + warmup + samples
    expected_env = {
        "mode": "active",
        "warmup_samples": str(warmup),
        "measured_samples": str(samples),
        "replay_records": str(replay_records),
        "parallel_decoder_workers": str(parallel_workers),
        "polars": "1",
        "clock": "CLOCK_MONOTONIC",
    }
    for key, expected in expected_env.items():
        if env.get(key) != expected:
            raise ValueError(f"{path}: latency environment {key} differs")
    for key, expected in {
        "success": 1,
        "measured_samples": samples,
        "history_records": expected_records,
        "history_expected": expected_records,
        "history_complete": 1,
        "polars_complete": 1,
        "promotion_ready": 1,
        "csv_published": replay_records,
        "journal_accepted": warmup + samples,
        "journal_committed": warmup + samples,
    }.items():
        if _unsigned(result, key) != expected:
            raise ValueError(f"{path}: latency result {key} differs")
    if returncode != 0 or _unsigned(result, "promotion_overlap_samples") == 0:
        raise ValueError(f"{path}: latency run did not overlap recovery")
    generation = _unsigned(boundary, "generation")
    ready = _unsigned(boundary, "polars_ready_ns")
    published = _unsigned(boundary, "history_published_monotonic_ns")
    first = _unsigned(boundary, "first_measured_caller_before_callback_ns")
    last = _unsigned(boundary, "last_measured_caller_before_callback_ns")
    first_latency = _unsigned(boundary, "strict_first_callback_to_polars_ns")
    last_latency = _unsigned(boundary, "strict_last_callback_to_polars_ns")
    publication_latency = _unsigned(boundary, "publication_to_polars_ns")
    if (
        not 0 < first <= last <= published <= ready
        or first_latency != ready - first
        or last_latency != ready - last
        or publication_latency != ready - published
        or _unsigned(boundary, "records") != expected_records
        or _unsigned(boundary, "columns") != 55
        or _unsigned(boundary, "measured_records") != samples
        or _unsigned(probe, "generation") != generation
        or _unsigned(probe, "records") != expected_records
        or _unsigned(probe, "columns") != 55
        or _unsigned(probe, "unique_ingress") != expected_records
        or _unsigned(probe, "first_ingress") != 1
        or _unsigned(probe, "last_ingress") != expected_records
        or _unsigned(probe, "polars_ready_ns") != ready
    ):
        raise ValueError(f"{path}: callback-to-Polars boundary does not reconcile")
    return {
        "replay_records": replay_records,
        "warmup_samples": warmup,
        "measured_samples": samples,
        "expected_records": expected_records,
        "strict_first_callback_to_polars_ns": first_latency,
        "strict_last_callback_to_polars_ns": last_latency,
        "publication_to_polars_ns": publication_latency,
        "probe_elapsed_ns": _unsigned(boundary, "probe_elapsed_ns"),
        "dataframe_estimated_bytes": _unsigned(
            boundary, "dataframe_estimated_bytes"
        ),
        "recovery_duration_ns": _unsigned(result, "recovery_duration_ns"),
        "promotion_overlap_samples": _unsigned(
            result, "promotion_overlap_samples"
        ),
        "polars_version": probe.get("polars_version", ""),
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--cpu-list", default="8-23")
    parser.add_argument("--rates", nargs="+", type=int, default=[300_000, 400_000, 500_000])
    parser.add_argument("--throughput-duration-ms", type=int, default=3_000)
    parser.add_argument("--throughput-repeats", type=int, default=3)
    parser.add_argument("--throughput-replay-records", type=int, default=50_000)
    parser.add_argument("--latency-backlogs", nargs="+", type=int, default=[50_000, 500_000])
    parser.add_argument("--latency-repeats", type=int, default=5)
    parser.add_argument("--warmup-samples", type=int, default=16)
    parser.add_argument("--measured-samples", type=int, default=64)
    parser.add_argument("--parallel-workers", type=int, default=0)
    parser.add_argument("--timeout-seconds", type=int, default=300)
    return parser


def main() -> int:
    args = _parser().parse_args()
    binary = args.binary.resolve()
    output_dir = args.output_dir.resolve()
    if not binary.is_file():
        raise SystemExit("benchmark binary must exist")
    if output_dir.exists() and any(output_dir.iterdir()):
        raise SystemExit("output directory must be empty")
    output_dir.mkdir(parents=True, exist_ok=True)
    binary_hash = _sha256(binary)

    throughput_rows: list[dict[str, object]] = []
    for rate in args.rates:
        for repeat in range(1, args.throughput_repeats + 1):
            log_path = output_dir / f"throughput_{rate}_run{repeat}.log"
            command = [
                "taskset", "-c", args.cpu_list, str(binary),
                "--mode", "active",
                "--replay-records", str(args.throughput_replay_records),
                "--parallel-decoder-workers", str(args.parallel_workers),
                "--throughput-rate", str(rate),
                "--throughput-duration-ms", str(args.throughput_duration_ms),
            ]
            completed = subprocess.run(
                command, capture_output=True, text=True,
                timeout=args.timeout_seconds, check=False,
            )
            log_path.write_text(completed.stdout + completed.stderr, encoding="utf-8")
            row = _validate_throughput_failure(
                log_path,
                rate=rate,
                duration_ms=args.throughput_duration_ms,
                replay_records=args.throughput_replay_records,
                parallel_workers=args.parallel_workers,
                returncode=completed.returncode,
            )
            row.update({"repeat": repeat, "log": log_path.name})
            throughput_rows.append(row)
            print(
                f"recovery throughput rate={rate} repeat={repeat} "
                f"accepted={row['preview_accepted']} "
                f"time_to_fail_ms={int(row['producer_elapsed_ns']) / 1e6:.3f}"
            )

    latency_rows: list[dict[str, object]] = []
    for repeat in range(1, args.latency_repeats + 1):
        order = list(args.latency_backlogs)
        if repeat % 2 == 0:
            order.reverse()
        for backlog in order:
            log_path = output_dir / f"latency_backlog{backlog}_run{repeat}.log"
            command = [
                "taskset", "-c", args.cpu_list, str(binary),
                "--mode", "active",
                "--warmup-samples", str(args.warmup_samples),
                "--samples", str(args.measured_samples),
                "--replay-records", str(backlog),
                "--parallel-decoder-workers", str(args.parallel_workers),
                "--polars",
            ]
            completed = subprocess.run(
                command, capture_output=True, text=True,
                timeout=args.timeout_seconds, check=False,
            )
            log_path.write_text(completed.stdout + completed.stderr, encoding="utf-8")
            row = _validate_latency(
                log_path,
                replay_records=backlog,
                warmup=args.warmup_samples,
                samples=args.measured_samples,
                parallel_workers=args.parallel_workers,
                returncode=completed.returncode,
            )
            row.update({"repeat": repeat, "log": log_path.name})
            latency_rows.append(row)
            print(
                f"recovery latency backlog={backlog} repeat={repeat} "
                f"strict_last_ms={int(row['strict_last_callback_to_polars_ns']) / 1e6:.3f}"
            )

    if _sha256(binary) != binary_hash:
        raise ValueError("benchmark binary changed during campaign")
    throughput_summary = []
    for rate in args.rates:
        rows = [row for row in throughput_rows if row["target_rps"] == rate]
        throughput_summary.append(
            {
                "target_rps": rate,
                "n": len(rows),
                "full_path_pass_runs": 0,
                "failure_classes": sorted(
                    {str(row["failure_class"]) for row in rows}
                ),
                "offer_pacing_met_runs": sum(
                    int(bool(row["offer_pacing_met"])) for row in rows
                ),
                "accepted_before_failure": _distribution(
                    [float(row["preview_accepted"]) for row in rows]
                ),
                "time_to_failure_ms": _distribution(
                    [int(row["producer_elapsed_ns"]) / 1_000_000.0 for row in rows]
                ),
                "achieved_offer_rps": _distribution(
                    [float(row["achieved_offered_rps"]) for row in rows]
                ),
            }
        )
    latency_summary: dict[str, object] = {}
    for backlog in args.latency_backlogs:
        rows = [row for row in latency_rows if row["replay_records"] == backlog]
        latency_summary[str(backlog)] = {
            key: _distribution([float(row[key]) for row in rows])
            for key in (
                "strict_first_callback_to_polars_ns",
                "strict_last_callback_to_polars_ns",
                "publication_to_polars_ns",
                "probe_elapsed_ns",
                "recovery_duration_ns",
            )
        }
    report = {
        "provenance": {
            "completed_at_utc": datetime.now(timezone.utc).isoformat(),
            "binary": str(binary),
            "binary_sha256": binary_hash,
            "host": platform.node(),
            "platform": platform.platform(),
            "cpu_list": args.cpu_list,
            "parallel_workers": args.parallel_workers,
            "rates": args.rates,
            "throughput_duration_ms": args.throughput_duration_ms,
            "throughput_repeats": args.throughput_repeats,
            "throughput_replay_records": args.throughput_replay_records,
            "latency_backlogs": args.latency_backlogs,
            "latency_repeats": args.latency_repeats,
            "warmup_samples": args.warmup_samples,
            "measured_samples": args.measured_samples,
            "throughput_failure_gate": (
                "target offer continues only while every callback is retained "
                "by the 65,536-record production-default live journal queue"
            ),
            "latency_endpoint": (
                "recovered ACTIVE History explicit EOF, eager 55-column Polars "
                "DataFrame, dense-ingress validation complete"
            ),
            "latency_sample_independence": "one observation per fresh process",
        },
        "throughput_summary": throughput_summary,
        "latency_summary_ns": latency_summary,
    }
    _write_json(output_dir / "throughput_runs.json", throughput_rows)
    _write_json(output_dir / "latency_runs.json", latency_rows)
    _write_csv(output_dir / "throughput_runs.csv", throughput_rows)
    _write_csv(output_dir / "latency_runs.csv", latency_rows)
    _write_json(output_dir / "summary.json", report)
    print(f"wrote validated report to {output_dir / 'summary.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
