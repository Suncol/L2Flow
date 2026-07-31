#!/usr/bin/env python3
"""Summarize callback-to-Polars latency and isolated throughput trials."""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from collections import Counter
from pathlib import Path


def _fields(line: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for token in line.split()[1:]:
        key, separator, value = token.partition("=")
        if separator:
            result[key] = value
    return result


def _percentile(values: list[float], probability: float) -> float:
    ordered = sorted(values)
    if not ordered:
        raise ValueError("percentile of empty sample")
    position = (len(ordered) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def _distribution_ms(values_ns: list[int]) -> dict[str, float | int]:
    values_ms = [value / 1_000_000.0 for value in values_ns]
    return {
        "n": len(values_ms),
        "min_ms": min(values_ms),
        "median_ms": statistics.median(values_ms),
        "mean_ms": statistics.fmean(values_ms),
        "p95_r7_ms": _percentile(values_ms, 0.95),
        "max_ms": max(values_ms),
    }


def _single_line(
    lines: list[str], prefix: str, workload: str | None = None
) -> dict[str, str]:
    matches = []
    for line in lines:
        if not line.startswith(prefix):
            continue
        parsed = _fields(line)
        if workload is None or parsed.get("workload") == workload:
            matches.append(parsed)
    if len(matches) != 1:
        raise ValueError(
            f"expected one {prefix!r}/{workload!r} line; got {len(matches)}"
        )
    return matches[0]


def _latency_rows(log_paths: list[Path]) -> tuple[list[dict[str, object]], dict]:
    rows: list[dict[str, object]] = []
    raw_scan_ns: list[int] = []
    for run, path in enumerate(log_paths, start=1):
        lines = path.read_text(encoding="utf-8").splitlines()
        if any(line.startswith("FAIL:") for line in lines):
            raise ValueError(f"failed latency run: {path}")
        if not any(line.startswith("PYTHON_BYE ") for line in lines):
            raise ValueError(f"incomplete latency run: {path}")
        raw_boundary = _single_line(
            lines, "POLARS_BOUNDARY ", "raw_batch_4096_all_columns"
        )
        derived_boundary = _single_line(
            lines,
            "POLARS_BOUNDARY ",
            "derived_complete_order_lifecycle",
        )
        raw_samples = [
            _fields(line)
            for line in lines
            if line.startswith("PYTHON_RAW_POLARS_SAMPLE ")
        ]
        if len(raw_samples) != 20:
            raise ValueError(f"expected 20 raw samples in {path}")
        raw_sample_zero = next(
            sample for sample in raw_samples if sample["sample"] == "0"
        )
        raw_scan_ns.extend(int(sample["open_to_polars_ns"]) for sample in raw_samples)
        derived = _single_line(lines, "PYTHON_DERIVED_POLARS_SAMPLE ")
        rows.append(
            {
                "run": run,
                "log": path.name,
                "raw_records": int(raw_boundary["records"]),
                "raw_columns": int(raw_boundary["columns"]),
                "raw_first_callback_to_polars_ms": int(
                    raw_sample_zero["first_callback_to_polars_ns"]
                )
                / 1_000_000.0,
                "raw_last_callback_to_polars_ms": int(
                    raw_sample_zero["last_callback_to_polars_ns"]
                )
                / 1_000_000.0,
                "raw_strict_caller_before_first_callback_to_polars_ms": int(
                    raw_boundary["strict_first_callback_to_polars_ns"]
                )
                / 1_000_000.0,
                "raw_strict_caller_before_last_callback_to_polars_ms": int(
                    raw_boundary["strict_last_callback_to_polars_ns"]
                )
                / 1_000_000.0,
                "raw_publication_to_polars_ms": int(
                    raw_sample_zero["publication_to_polars_ns"]
                )
                / 1_000_000.0,
                "raw_open_to_polars_ms": int(
                    raw_sample_zero["open_to_polars_ns"]
                )
                / 1_000_000.0,
                "raw_dataframe_estimated_bytes": int(
                    raw_sample_zero["dataframe_estimated_bytes"]
                ),
                "order_raw_callbacks": int(derived_boundary["raw_records"]),
                "order_derived_events": int(derived_boundary["derived_events"]),
                "order_sequence_events": int(
                    derived_boundary["order_sequence_events"]
                ),
                "order_first_callback_to_polars_ms": int(
                    derived["first_callback_to_polars_ns"]
                )
                / 1_000_000.0,
                "order_last_callback_to_polars_ms": int(
                    derived["last_callback_to_polars_ns"]
                )
                / 1_000_000.0,
                "order_strict_caller_before_first_callback_to_polars_ms": int(
                    derived_boundary["strict_first_callback_to_polars_ns"]
                )
                / 1_000_000.0,
                "order_strict_caller_before_last_callback_to_polars_ms": int(
                    derived_boundary["strict_last_callback_to_polars_ns"]
                )
                / 1_000_000.0,
                "order_publication_to_polars_ms": int(
                    derived["publication_to_polars_ns"]
                )
                / 1_000_000.0,
                "order_open_scan_to_polars_ms": int(derived["elapsed_ns"])
                / 1_000_000.0,
                "order_final_revision": int(derived["final_revision"]),
                "order_final_remaining_quantity": int(
                    derived["final_remaining_quantity"]
                ),
            }
        )

    def ns_from_ms(field: str) -> list[int]:
        return [round(float(row[field]) * 1_000_000.0) for row in rows]

    summary = {
        "independent_runs": len(rows),
        "raw_batch": {
            "records": rows[0]["raw_records"],
            "columns": rows[0]["raw_columns"],
            "first_callback_to_polars": _distribution_ms(
                ns_from_ms("raw_first_callback_to_polars_ms")
            ),
            "last_callback_to_polars": _distribution_ms(
                ns_from_ms("raw_last_callback_to_polars_ms")
            ),
            "strict_caller_before_first_callback_to_polars": _distribution_ms(
                ns_from_ms(
                    "raw_strict_caller_before_first_callback_to_polars_ms"
                )
            ),
            "generation_publication_to_polars": _distribution_ms(
                ns_from_ms("raw_publication_to_polars_ms")
            ),
            "repeated_open_to_polars": _distribution_ms(raw_scan_ns),
        },
        "complete_order_sequence": {
            "raw_callbacks": rows[0]["order_raw_callbacks"],
            "derived_events_total": rows[0]["order_derived_events"],
            "filtered_sequence_events": rows[0]["order_sequence_events"],
            "first_order_callback_to_polars": _distribution_ms(
                ns_from_ms("order_first_callback_to_polars_ms")
            ),
            "last_order_callback_to_polars": _distribution_ms(
                ns_from_ms("order_last_callback_to_polars_ms")
            ),
            "strict_caller_before_first_order_callback_to_polars": (
                _distribution_ms(
                    ns_from_ms(
                        "order_strict_caller_before_first_callback_to_polars_ms"
                    )
                )
            ),
            "generation_publication_to_polars": _distribution_ms(
                ns_from_ms("order_publication_to_polars_ms")
            ),
            "reader_open_scan_to_polars": _distribution_ms(
                ns_from_ms("order_open_scan_to_polars_ms")
            ),
            "validated_final_revision": rows[0]["order_final_revision"],
            "validated_final_remaining_quantity": rows[0][
                "order_final_remaining_quantity"
            ],
        },
    }
    return rows, summary


def _throughput_summary(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    by_rate: dict[int, list[dict[str, object]]] = {}
    for row in rows:
        by_rate.setdefault(int(row["target_rps"]), []).append(row)
    summary: list[dict[str, object]] = []
    for rate, trials in sorted(by_rate.items()):
        fatal_reasons = Counter(str(trial["fatal_reason"]) for trial in trials)
        fatal_reasons.pop("", None)
        fatal_sequences = [
            int(trial["fatal_ingress_sequence"])
            for trial in trials
            if int(trial["fatal_ingress_sequence"]) != 0
        ]
        achieved = [float(trial["achieved_offered_rps"]) for trial in trials]
        clean = sum(int(trial["target_met"]) for trial in trials)
        summary.append(
            {
                "target_rps": rate,
                "trials": len(trials),
                "clean_target_met_trials": clean,
                "short_run_classification": (
                    "PASS" if clean == len(trials) else "FAIL_CLOSED"
                ),
                "median_achieved_offered_rps": statistics.median(achieved),
                "min_achieved_offered_rps": min(achieved),
                "max_achieved_offered_rps": max(achieved),
                "os_signal_terminations": sum(
                    int(trial["terminated_by_signal"]) != 0 for trial in trials
                ),
                "nonzero_exit_codes": sum(
                    int(trial["exit_code"]) != 0 for trial in trials
                ),
                "fatal_reasons": dict(sorted(fatal_reasons.items())),
                "fatal_ingress_sequence_min": (
                    min(fatal_sequences) if fatal_sequences else 0
                ),
                "fatal_ingress_sequence_median": (
                    statistics.median(fatal_sequences) if fatal_sequences else 0
                ),
                "fatal_ingress_sequence_max": (
                    max(fatal_sequences) if fatal_sequences else 0
                ),
                "max_decoder_queue_high_water": max(
                    int(trial["decoder_high_water_max"]) for trial in trials
                ),
                "total_decoder_queue_full_count": sum(
                    int(trial["decoder_full_count"]) for trial in trials
                ),
            }
        )
    return summary


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    input_dir = args.input_dir.resolve()
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    latency_logs = sorted(input_dir.glob("latency_run*.log"))
    if len(latency_logs) != 3:
        raise SystemExit(f"expected 3 latency logs, got {len(latency_logs)}")
    latency_rows, latency_summary = _latency_rows(latency_logs)
    throughput_rows = json.loads(
        (input_dir / "throughput_trials.json").read_text(encoding="utf-8")
    )
    throughput_summary = _throughput_summary(throughput_rows)

    with (output_dir / "latency_samples.csv").open(
        "w", newline="", encoding="utf-8"
    ) as output:
        writer = csv.DictWriter(output, fieldnames=list(latency_rows[0]))
        writer.writeheader()
        writer.writerows(latency_rows)
    with (output_dir / "throughput_summary.csv").open(
        "w", newline="", encoding="utf-8"
    ) as output:
        writer = csv.DictWriter(output, fieldnames=list(throughput_summary[0]))
        writer.writeheader()
        writer.writerows(throughput_summary)
    combined = {
        "latency": latency_summary,
        "throughput": throughput_summary,
        "method": {
            "latency_runs": 3,
            "raw_repeated_scans": 60,
            "throughput_trials_per_rate": 3,
            "throughput_trial_duration_ms": 1_000,
            "percentile_method": "R-7 linear interpolation",
        },
    }
    (output_dir / "summary.json").write_text(
        json.dumps(combined, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(combined, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
