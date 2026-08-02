#!/usr/bin/env python3
"""Interleave legacy/parallel callback-to-Polars latency runs and gate them."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import random
import statistics
import subprocess
from pathlib import Path


RAW_PREFIX = "PYTHON_RAW_POLARS_SAMPLE "
DERIVED_PREFIX = "PYTHON_DERIVED_POLARS_SAMPLE "
BOUNDARY_PREFIX = "POLARS_BOUNDARY "
HISTORY_ENV_PREFIX = "HISTORY_ENV "
TOPOLOGY_PREFIX = "CALLBACK_POLARS_TOPOLOGY "
PYTHON_READY_PREFIX = "PYTHON_READY "
PYTHON_BYE_PREFIX = "PYTHON_BYE "
EXPECTED_RAW_RECORDS = 4_096
EXPECTED_RAW_COLUMNS = 55
EXPECTED_DERIVED_RECORDS = 6
EXPECTED_ORDER_EVENTS = 5


def _fields(line: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    for token in line.split()[1:]:
        key, separator, value = token.partition("=")
        if separator:
            if not key:
                raise ValueError("empty latency field name")
            if key in fields:
                raise ValueError(f"duplicate latency field: {key}")
            fields[key] = value
    return fields


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _unsigned(fields: dict[str, str], key: str) -> int:
    raw = fields.get(key)
    if raw is None or not raw.isascii() or not raw.isdecimal():
        raise ValueError(f"{key} is not an unsigned decimal integer")
    return int(raw, 10)


def _sha256_field(fields: dict[str, str], key: str) -> str:
    value = fields.get(key, "")
    if len(value) != 64 or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise ValueError(f"{key} is not a lowercase SHA-256 digest")
    return value


def _affinity_set(value: str) -> frozenset[int]:
    cpus_text, separator, count_text = value.partition(";count=")
    if not separator or not count_text.isdecimal():
        raise ValueError(f"invalid affinity telemetry: {value!r}")
    fields = [] if not cpus_text else cpus_text.split(",")
    if any(not field.isdecimal() for field in fields):
        raise ValueError(f"invalid affinity CPU: {value!r}")
    cpus = [int(field, 10) for field in fields]
    if len(set(cpus)) != len(cpus) or int(count_text, 10) != len(cpus):
        raise ValueError(f"inconsistent affinity telemetry: {value!r}")
    return frozenset(cpus)


def _requested_cpu_set(value: str) -> frozenset[int] | None:
    if not value:
        return None
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
            raise ValueError(f"invalid CPU list field: {field!r}")
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


def _percentile(values: list[float], probability: float) -> float:
    if not values:
        raise ValueError("percentile requires at least one value")
    ordered = sorted(values)
    position = (len(ordered) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


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


def _parse_latency_log(
    path: Path,
    *,
    expected_workers: int,
    expected_affinity: frozenset[int] | None,
    allow_legacy_worker_field_absent: bool = False,
    expected_scenario: str | None = None,
) -> tuple[dict[str, int], dict[str, str | int]]:
    lines = path.read_text(encoding="utf-8").splitlines()
    if any(line.startswith("FAIL:") for line in lines):
        raise ValueError(f"benchmark reported failure: {path}")
    history_env = _single(lines, HISTORY_ENV_PREFIX)
    topology = _single(lines, TOPOLOGY_PREFIX)
    python_ready = _single(lines, PYTHON_READY_PREFIX)
    _single(lines, PYTHON_BYE_PREFIX)
    raw = _single(lines, RAW_PREFIX, sample="0")
    derived = _single(lines, DERIVED_PREFIX)
    raw_boundary = _single(
        lines,
        BOUNDARY_PREFIX,
        workload="raw_batch_4096_all_columns",
    )
    derived_boundary = _single(
        lines,
        BOUNDARY_PREFIX,
        workload="derived_complete_order_lifecycle",
    )

    expected_history_fields = {
        "capacity": 12_000,
        "bound_instruments": 12_000,
        "snapshot_fill": 11_997,
        "worker_count": 4,
        "tick_ring_capacity": 262_144,
        "requested_page_records": 4_096,
        "price_repeats": 20,
        "all_column_repeats": 10,
        "raw_polars_records": EXPECTED_RAW_RECORDS,
        "raw_polars_repeats": 20,
        "parallel_decoder_farm_activation_queue_depth": 8_192,
        "parallel_decoder_farm_activation_effective_depth": 6_144,
    }
    for key, expected in expected_history_fields.items():
        if _unsigned(history_env, key) != expected:
            raise ValueError(
                f"{path}: HISTORY_ENV {key} differs from {expected}"
            )
    worker_field = history_env.get("parallel_decoder_workers")
    if worker_field is None:
        if not allow_legacy_worker_field_absent or expected_workers != 0:
            raise ValueError(
                f"{path}: HISTORY_ENV lacks parallel_decoder_workers"
            )
        topology_evidence = "legacy_command_without_worker_field"
    else:
        if _unsigned(history_env, "parallel_decoder_workers") != expected_workers:
            raise ValueError(
                f"{path}: HISTORY_ENV parallel_decoder_workers differs "
                f"from {expected_workers}"
            )
        topology_evidence = "history_env_worker_field"
    if _unsigned(topology, "configured_workers") != expected_workers:
        raise ValueError(f"{path}: topology configured worker count differs")
    if _unsigned(topology, "activation_configured_depth") != 8_192:
        raise ValueError(f"{path}: configured activation threshold differs")
    if _unsigned(topology, "activation_effective_depth") != 6_144:
        raise ValueError(f"{path}: effective activation threshold differs")
    topology_enabled = _unsigned(topology, "enabled")
    topology_reported_workers = _unsigned(topology, "reported_workers")
    topology_inline_messages = _unsigned(topology, "inline_messages")
    topology_farm_messages = _unsigned(topology, "farm_messages")
    topology_active_workers = _unsigned(topology, "active_parse_workers")
    if expected_workers == 0:
        if any(
            (
                topology_enabled,
                topology_reported_workers,
                topology_inline_messages,
                topology_farm_messages,
                topology_active_workers,
            )
        ):
            raise ValueError(f"{path}: workers=0 topology is not disabled")
    else:
        if topology_enabled != 1 or topology_reported_workers != expected_workers:
            raise ValueError(f"{path}: candidate topology is not enabled")
        if topology_farm_messages != 0 or topology_active_workers != 0:
            raise ValueError(
                f"{path}: low-load latency run activated the decode farm"
            )
        if topology_inline_messages == 0:
            raise ValueError(f"{path}: candidate topology has no inline work")
    if history_env.get("clock") != "CLOCK_MONOTONIC":
        raise ValueError(f"{path}: HISTORY_ENV clock is not CLOCK_MONOTONIC")
    if expected_scenario is not None:
        expected_contract = {
            "from_open": ("ACTIVE", 1),
            "live_partial_no_recovery": ("LIVE_PARTIAL", 0),
        }.get(expected_scenario)
        if expected_contract is None:
            raise ValueError(
                f"unsupported expected startup scenario: {expected_scenario}"
            )
        expected_state, expected_coverage = expected_contract
        if history_env.get("scenario") != expected_scenario:
            raise ValueError(f"{path}: HISTORY_ENV scenario differs")
        if history_env.get("server_state") != expected_state:
            raise ValueError(f"{path}: HISTORY_ENV server state differs")
        if _unsigned(history_env, "coverage_from_open") != expected_coverage:
            raise ValueError(f"{path}: HISTORY_ENV coverage differs")
        if _unsigned(history_env, "online_recovery") != 0:
            raise ValueError(f"{path}: benchmark unexpectedly used recovery")
        if (
            _unsigned(history_env, "factor_generation_enabled")
            != expected_coverage
        ):
            raise ValueError(f"{path}: Factor-generation contract differs")
        if history_env.get("generation_visibility") != "forced_cut":
            raise ValueError(f"{path}: generation visibility mode differs")
        if topology.get("scenario") != expected_scenario:
            raise ValueError(f"{path}: topology scenario differs")
        if raw_boundary.get("scenario") != expected_scenario:
            raise ValueError(f"{path}: raw boundary scenario differs")
        if derived_boundary.get("scenario") != expected_scenario:
            raise ValueError(f"{path}: derived boundary scenario differs")
        if _unsigned(topology, "coverage_from_open") != expected_coverage:
            raise ValueError(f"{path}: topology coverage differs")
        if _unsigned(topology, "online_recovery") != 0:
            raise ValueError(f"{path}: topology unexpectedly used recovery")
        if (
            _unsigned(topology, "factor_generation_enabled")
            != expected_coverage
            or _unsigned(topology, "final_factor_generation_present")
            != expected_coverage
        ):
            raise ValueError(f"{path}: topology Factor contract differs")
        if python_ready.get("server_state") != expected_state:
            raise ValueError(f"{path}: Python observed a different state")
        if _unsigned(python_ready, "coverage_from_open") != expected_coverage:
            raise ValueError(f"{path}: Python observed different coverage")
    history_affinity_text = history_env.get("affinity", "")
    history_affinity = _affinity_set(history_affinity_text)
    if expected_affinity is not None and history_affinity != expected_affinity:
        raise ValueError(f"{path}: HISTORY_ENV affinity differs from request")

    if python_ready.get("protocol") != "wire_v2_history_latency_2":
        raise ValueError(f"{path}: unexpected Python probe protocol")
    if _unsigned(python_ready, "capacity") != expected_history_fields["capacity"]:
        raise ValueError(f"{path}: Python capacity differs from HISTORY_ENV")
    if (
        _unsigned(python_ready, "bound_count")
        != expected_history_fields["bound_instruments"]
    ):
        raise ValueError(f"{path}: Python bound count differs from HISTORY_ENV")
    if _unsigned(python_ready, "raw_polars_columns") != EXPECTED_RAW_COLUMNS:
        raise ValueError(f"{path}: raw Polars schema is not 55 columns")
    if _unsigned(python_ready, "worker_ring_slots") != 4:
        raise ValueError(f"{path}: unexpected Python worker ring size")
    if _unsigned(python_ready, "worker_result_batch_records") != 4_096:
        raise ValueError(f"{path}: unexpected Python worker batch size")
    if _unsigned(python_ready, "monotonic_resolution_ns") == 0:
        raise ValueError(f"{path}: invalid Python monotonic resolution")
    for key in (
        "monotonic_implementation",
        "python_version",
        "python_implementation",
        "polars_version",
    ):
        if not python_ready.get(key):
            raise ValueError(f"{path}: missing Python provenance field {key}")
    python_affinity_text = python_ready.get("python_affinity", "")
    python_affinity = _affinity_set(python_affinity_text)
    if python_affinity != history_affinity:
        raise ValueError(f"{path}: C++ and Python affinity differ")
    python_executable_sha256 = _sha256_field(
        python_ready, "python_executable_sha256"
    )
    native_library_sha256 = _sha256_field(
        python_ready, "native_library_sha256"
    )
    probe_sha256 = _sha256_field(python_ready, "probe_sha256")

    for fields, label, expected_records, expected_columns in (
        (
            raw,
            "raw sample",
            EXPECTED_RAW_RECORDS,
            EXPECTED_RAW_COLUMNS,
        ),
        (
            raw_boundary,
            "raw boundary",
            EXPECTED_RAW_RECORDS,
            EXPECTED_RAW_COLUMNS,
        ),
    ):
        if _unsigned(fields, "records") != expected_records:
            raise ValueError(f"{path}: {label} record count differs")
        if _unsigned(fields, "columns") != expected_columns:
            raise ValueError(f"{path}: {label} column count differs")
    if _unsigned(raw, "batches") != 1:
        raise ValueError(f"{path}: raw Polars read is not one batch")
    first_ingress = _unsigned(raw, "first_ingress")
    last_ingress = _unsigned(raw, "last_ingress")
    unique_ingress = _unsigned(raw, "unique_ingress")
    if last_ingress - first_ingress + 1 != EXPECTED_RAW_RECORDS:
        raise ValueError(f"{path}: raw ingress range is not dense")
    if unique_ingress != EXPECTED_RAW_RECORDS:
        raise ValueError(f"{path}: raw ingress values are not unique")
    if _unsigned(raw, "ingress_sum") != (
        first_ingress + last_ingress
    ) * EXPECTED_RAW_RECORDS // 2:
        raise ValueError(f"{path}: raw ingress range is not complete")
    if (
        _unsigned(raw_boundary, "first_ingress_sequence")
        != first_ingress
        or _unsigned(raw_boundary, "last_ingress_sequence")
        != last_ingress
    ):
        raise ValueError(f"{path}: raw ingress boundary differs")
    if _unsigned(raw, "generation") != _unsigned(
        raw_boundary, "generation"
    ):
        raise ValueError(f"{path}: raw generation telemetry differs")

    derived_expected = {
        "records": EXPECTED_DERIVED_RECORDS,
        "order_sequence_records": EXPECTED_ORDER_EVENTS,
        "batches": 1,
        "order_id": 11_001,
        "order_revision_count": 3,
        "final_revision": 3,
        "final_remaining_quantity": 0,
    }
    for key, expected in derived_expected.items():
        if _unsigned(derived, key) != expected:
            raise ValueError(
                f"{path}: derived {key} differs from {expected}"
            )
    if _unsigned(derived, "columns") == 0:
        raise ValueError(f"{path}: derived Polars frame has no columns")
    if (
        _unsigned(derived, "last_derived_sequence")
        - _unsigned(derived, "first_derived_sequence")
        + 1
        != EXPECTED_ORDER_EVENTS
    ):
        raise ValueError(f"{path}: derived order sequence is not dense")
    derived_boundary_expected = {
        "raw_records": 4,
        "derived_events": EXPECTED_DERIVED_RECORDS,
        "order_sequence_events": EXPECTED_ORDER_EVENTS,
        "order_id": 11_001,
        "final_revision": 3,
        "final_remaining_quantity": 0,
    }
    for key, expected in derived_boundary_expected.items():
        if _unsigned(derived_boundary, key) != expected:
            raise ValueError(
                f"{path}: derived boundary {key} differs from {expected}"
            )
    if _unsigned(derived, "generation") != _unsigned(
        derived_boundary, "generation"
    ):
        raise ValueError(f"{path}: derived generation telemetry differs")

    for sample, boundary, label in (
        (raw, raw_boundary, "raw"),
        (derived, derived_boundary, "derived"),
    ):
        first_entry = _unsigned(sample, "first_callback_entry_ns")
        last_entry = _unsigned(sample, "last_callback_entry_ns")
        published = _unsigned(sample, "history_published_monotonic_ns")
        ready = _unsigned(sample, "polars_ready_ns")
        if not 0 < first_entry <= last_entry <= published <= ready:
            raise ValueError(f"{path}: {label} clock ordering is invalid")
        if _unsigned(sample, "first_callback_to_polars_ns") != (
            ready - first_entry
        ):
            raise ValueError(f"{path}: {label} first latency arithmetic differs")
        if _unsigned(sample, "last_callback_to_polars_ns") != (
            ready - last_entry
        ):
            raise ValueError(f"{path}: {label} last latency arithmetic differs")
        if _unsigned(boundary, "first_callback_entry_ns") != first_entry:
            raise ValueError(f"{path}: {label} first callback boundary differs")
        if _unsigned(boundary, "last_callback_entry_ns") != last_entry:
            raise ValueError(f"{path}: {label} last callback boundary differs")
        if _unsigned(boundary, "polars_ready_ns") != ready:
            raise ValueError(f"{path}: {label} ready boundary differs")
        strict_first_origin = _unsigned(
            boundary, "first_caller_before_callback_ns"
        )
        strict_last_origin = _unsigned(
            boundary, "last_caller_before_callback_ns"
        )
        if not (
            0 < strict_first_origin <= first_entry
            and strict_last_origin <= last_entry
        ):
            raise ValueError(f"{path}: {label} strict origins are invalid")
        if _unsigned(
            boundary, "strict_first_callback_to_polars_ns"
        ) != ready - strict_first_origin:
            raise ValueError(f"{path}: {label} strict first latency differs")
        if _unsigned(
            boundary, "strict_last_callback_to_polars_ns"
        ) != ready - strict_last_origin:
            raise ValueError(f"{path}: {label} strict last latency differs")

    result = {
        "batch_first_callback_to_polars_ns": int(
            raw["first_callback_to_polars_ns"]
        ),
        "batch_last_callback_to_polars_ns": int(
            raw["last_callback_to_polars_ns"]
        ),
        "batch_strict_first_callback_to_polars_ns": int(
            raw_boundary["strict_first_callback_to_polars_ns"]
        ),
        "batch_strict_last_callback_to_polars_ns": int(
            raw_boundary["strict_last_callback_to_polars_ns"]
        ),
        "order_first_callback_to_polars_ns": int(
            derived["first_callback_to_polars_ns"]
        ),
        "order_last_callback_to_polars_ns": int(
            derived["last_callback_to_polars_ns"]
        ),
        "order_strict_first_callback_to_polars_ns": int(
            derived_boundary["strict_first_callback_to_polars_ns"]
        ),
        "order_strict_last_callback_to_polars_ns": int(
            derived_boundary["strict_last_callback_to_polars_ns"]
        ),
    }
    if any(value < 0 for value in result.values()):
        raise ValueError(f"negative latency in {path}")
    metadata: dict[str, str | int] = {
        "history_affinity": history_affinity_text,
        "python_affinity": python_affinity_text,
        "python_version": python_ready["python_version"],
        "python_implementation": python_ready["python_implementation"],
        "polars_version": python_ready["polars_version"],
        "python_executable_sha256": python_executable_sha256,
        "native_library_sha256": native_library_sha256,
        "probe_sha256": probe_sha256,
        "raw_records": EXPECTED_RAW_RECORDS,
        "raw_unique_ingress": unique_ingress,
        "raw_first_ingress": first_ingress,
        "raw_last_ingress": last_ingress,
        "raw_columns": EXPECTED_RAW_COLUMNS,
        "derived_records": EXPECTED_DERIVED_RECORDS,
        "order_sequence_records": EXPECTED_ORDER_EVENTS,
        "parallel_decoder_workers": expected_workers,
        "topology_evidence": topology_evidence,
        "topology_inline_messages": topology_inline_messages,
        "topology_farm_messages": topology_farm_messages,
        "topology_active_parse_workers": topology_active_workers,
        "topology_activation_queue_depth": _unsigned(
            topology, "activation_configured_depth"
        ),
        "topology_effective_activation_queue_depth": _unsigned(
            topology, "activation_effective_depth"
        ),
    }
    if expected_scenario is not None:
        metadata["scenario"] = expected_scenario
        metadata["coverage_from_open"] = _unsigned(
            history_env, "coverage_from_open"
        )
        metadata["server_state"] = history_env["server_state"]
        metadata["generation_visibility"] = history_env[
            "generation_visibility"
        ]
    return result, metadata


def _bootstrap_upper_bounds(
    baseline: list[float],
    candidate: list[float],
    *,
    confidence: float,
    resamples: int,
    seed: int,
) -> dict[str, float]:
    if len(baseline) != len(candidate) or len(baseline) < 3:
        raise ValueError("paired bootstrap needs at least three complete pairs")
    randomizer = random.Random(seed)
    median_deltas: list[float] = []
    p95_deltas: list[float] = []
    pair_count = len(baseline)
    for _ in range(resamples):
        indexes = [randomizer.randrange(pair_count) for _ in range(pair_count)]
        base_sample = [baseline[index] for index in indexes]
        candidate_sample = [candidate[index] for index in indexes]
        median_deltas.append(
            statistics.median(candidate_sample)
            - statistics.median(base_sample)
        )
        p95_deltas.append(
            _percentile(candidate_sample, 0.95)
            - _percentile(base_sample, 0.95)
        )
    return {
        "median_delta_ns": statistics.median(candidate)
        - statistics.median(baseline),
        "median_delta_upper_ns": _percentile(median_deltas, confidence),
        "p95_delta_ns": _percentile(candidate, 0.95)
        - _percentile(baseline, 0.95),
        "p95_delta_upper_ns": _percentile(p95_deltas, confidence),
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run a legacy topology and candidate topology in alternating "
            "order. Supply --baseline-binary to cover the complete code "
            "change; otherwise this is a topology-only feature-switch A/B. "
            "The verdict is a paired bootstrap non-regression gate."
        )
    )
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument(
        "--baseline-binary",
        type=Path,
        help=(
            "Optional immutable pre-optimization binary. Without it, the "
            "candidate binary with workers=0 is a topology-only baseline."
        ),
    )
    parser.add_argument(
        "--baseline-command",
        choices=("workers-zero", "legacy"),
        default="workers-zero",
        help=(
            "Use the focused callback-to-Polars worker entry point with "
            "zero, or the broad --history-latency-benchmark flag for a "
            "baseline binary already instrumented with the exact Polars "
            "measurement protocol."
        ),
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--candidate-workers", type=int, required=True)
    parser.add_argument("--pairs", type=int, default=7)
    parser.add_argument("--cpu-list", default="")
    parser.add_argument("--timeout-seconds", type=int, default=1_800)
    parser.add_argument("--confidence", type=float, default=0.95)
    parser.add_argument("--bootstrap-resamples", type=int, default=20_000)
    parser.add_argument("--seed", type=int, default=20260731)
    parser.add_argument(
        "--max-regression-ms",
        type=float,
        default=0.0,
        help=(
            "Allowed one-sided upper confidence bound. The default zero "
            "implements the requested no-regression gate."
        ),
    )
    return parser


def main() -> int:
    args = _parser().parse_args()
    if args.candidate_workers < 0:
        raise SystemExit("candidate workers must be nonnegative")
    if args.pairs < 3:
        raise SystemExit("at least three interleaved pairs are required")
    if args.timeout_seconds <= 0 or args.bootstrap_resamples <= 0:
        raise SystemExit("timeout and bootstrap resamples must be positive")
    if not 0.5 < args.confidence < 1.0:
        raise SystemExit("confidence must be between 0.5 and 1")
    if args.max_regression_ms < 0.0:
        raise SystemExit("maximum regression must be nonnegative")

    candidate_binary = args.binary.resolve()
    baseline_binary = (
        args.baseline_binary.resolve()
        if args.baseline_binary is not None
        else candidate_binary
    )
    if not candidate_binary.is_file() or not baseline_binary.is_file():
        raise SystemExit("baseline and candidate binaries must be files")
    candidate_binary_sha256 = _sha256_file(candidate_binary)
    baseline_binary_sha256 = _sha256_file(baseline_binary)
    try:
        requested_affinity = _requested_cpu_set(args.cpu_list)
    except ValueError as error:
        raise SystemExit(str(error)) from error
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    environment = os.environ.copy()
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    parsed_runs: dict[str, list[dict[str, int]]] = {
        "baseline": [],
        "candidate": [],
    }
    rows: list[dict[str, object]] = []

    for pair in range(1, args.pairs + 1):
        order = (
            ("baseline", 0),
            ("candidate", args.candidate_workers),
        )
        if pair % 2 == 0:
            order = tuple(reversed(order))
        for position, (variant, workers) in enumerate(order, start=1):
            if variant == "baseline" and args.baseline_command == "legacy":
                command = [
                    str(baseline_binary),
                    "--history-latency-benchmark",
                ]
            else:
                command = [
                    str(
                        baseline_binary
                        if variant == "baseline"
                        else candidate_binary
                    ),
                    "--callback-polars-latency-benchmark-workers",
                    str(workers),
                ]
            if args.cpu_list:
                command = ["taskset", "-c", args.cpu_list, *command]
            log_path = output_dir / (
                f"pair{pair:02d}_position{position}_{variant}_workers{workers}.log"
            )
            completed = subprocess.run(
                command,
                check=False,
                capture_output=True,
                text=True,
                env=environment,
                timeout=args.timeout_seconds,
            )
            log_path.write_text(
                completed.stdout + completed.stderr,
                encoding="utf-8",
            )
            if completed.returncode != 0:
                raise SystemExit(
                    f"{variant} pair {pair} exited {completed.returncode}; "
                    f"see {log_path}"
                )
            try:
                metrics, run_metadata = _parse_latency_log(
                    log_path,
                    expected_workers=workers,
                    expected_affinity=requested_affinity,
                    allow_legacy_worker_field_absent=(
                        variant == "baseline"
                        and args.baseline_command == "legacy"
                    ),
                )
            except ValueError as error:
                raise SystemExit(str(error)) from error
            parsed_runs[variant].append(metrics)
            row: dict[str, object] = {
                "pair": pair,
                "position": position,
                "variant": variant,
                "parallel_decoder_workers": workers,
                "binary_sha256": (
                    baseline_binary_sha256
                    if variant == "baseline"
                    else candidate_binary_sha256
                ),
                "log": log_path.name,
            }
            row.update(run_metadata)
            row.update(
                {
                    key.removesuffix("_ns") + "_ms": value / 1_000_000.0
                    for key, value in metrics.items()
                }
            )
            rows.append(row)
            print(
                f"pair={pair} position={position} variant={variant} "
                f"workers={workers} complete=1"
            )

    if _sha256_file(candidate_binary) != candidate_binary_sha256:
        raise SystemExit("candidate binary changed during the A/B run")
    if _sha256_file(baseline_binary) != baseline_binary_sha256:
        raise SystemExit("baseline binary changed during the A/B run")

    common_provenance_fields = (
        "history_affinity",
        "python_affinity",
        "python_version",
        "python_implementation",
        "polars_version",
        "python_executable_sha256",
        "probe_sha256",
        "raw_records",
        "raw_columns",
        "derived_records",
        "order_sequence_records",
        "topology_activation_queue_depth",
        "topology_effective_activation_queue_depth",
    )
    common_provenance: dict[str, object] = {}
    for field in common_provenance_fields:
        values = {row[field] for row in rows}
        if len(values) != 1:
            raise SystemExit(f"A/B runs disagree on provenance field {field}")
        common_provenance[field] = next(iter(values))
    native_library_hashes: dict[str, str] = {}
    topology_evidence: dict[str, str] = {}
    for variant in ("baseline", "candidate"):
        values = {
            str(row["native_library_sha256"])
            for row in rows
            if row["variant"] == variant
        }
        if len(values) != 1:
            raise SystemExit(
                f"{variant} runs disagree on native library hash"
            )
        native_library_hashes[variant] = next(iter(values))
        evidence_values = {
            str(row["topology_evidence"])
            for row in rows
            if row["variant"] == variant
        }
        if len(evidence_values) != 1:
            raise SystemExit(
                f"{variant} runs disagree on topology evidence"
            )
        topology_evidence[variant] = next(iter(evidence_values))

    metric_names = tuple(parsed_runs["baseline"][0])
    budget_ns = args.max_regression_ms * 1_000_000.0
    metric_results: dict[str, dict[str, float | bool | int]] = {}
    all_pass = True
    for metric_index, metric in enumerate(metric_names):
        baseline = [float(row[metric]) for row in parsed_runs["baseline"]]
        candidate = [float(row[metric]) for row in parsed_runs["candidate"]]
        bounds = _bootstrap_upper_bounds(
            baseline,
            candidate,
            confidence=args.confidence,
            resamples=args.bootstrap_resamples,
            seed=args.seed + metric_index,
        )
        passed = (
            bounds["median_delta_upper_ns"] <= budget_ns
            and bounds["p95_delta_upper_ns"] <= budget_ns
        )
        all_pass = all_pass and passed
        metric_results[metric] = {
            "pairs": args.pairs,
            "baseline_median_ms": statistics.median(baseline) / 1_000_000.0,
            "candidate_median_ms": statistics.median(candidate) / 1_000_000.0,
            "baseline_p95_ms": _percentile(baseline, 0.95) / 1_000_000.0,
            "candidate_p95_ms": _percentile(candidate, 0.95) / 1_000_000.0,
            "median_delta_ms": bounds["median_delta_ns"] / 1_000_000.0,
            "median_delta_upper_ms": bounds["median_delta_upper_ns"]
            / 1_000_000.0,
            "p95_delta_ms": bounds["p95_delta_ns"] / 1_000_000.0,
            "p95_delta_upper_ms": bounds["p95_delta_upper_ns"]
            / 1_000_000.0,
            "maximum_allowed_regression_ms": args.max_regression_ms,
            "pass": passed,
        }

    report = {
        "verdict": "PASS" if all_pass else "FAIL",
        "method": {
            "design": "paired_interleaved_ab",
            "baseline_binary": str(baseline_binary),
            "candidate_binary": str(candidate_binary),
            "baseline_binary_sha256": baseline_binary_sha256,
            "candidate_binary_sha256": candidate_binary_sha256,
            "baseline_native_library_sha256": (
                native_library_hashes["baseline"]
            ),
            "candidate_native_library_sha256": (
                native_library_hashes["candidate"]
            ),
            "baseline_command": args.baseline_command,
            "comparison_scope": (
                "full_binary_change"
                if args.baseline_binary is not None
                and baseline_binary_sha256 != candidate_binary_sha256
                else "topology_feature_switch_only"
            ),
            "baseline_parallel_decoder_workers": 0,
            "candidate_parallel_decoder_workers": args.candidate_workers,
            "baseline_topology_evidence": topology_evidence["baseline"],
            "candidate_topology_evidence": topology_evidence["candidate"],
            "pairs": args.pairs,
            "confidence": args.confidence,
            "bootstrap_resamples": args.bootstrap_resamples,
            "seed": args.seed,
            "gate": (
                "one-sided bootstrap upper bounds for both median and p95 "
                "candidate-minus-baseline deltas"
            ),
            "maximum_allowed_regression_ms": args.max_regression_ms,
            "clock_unit": "milliseconds",
            "cpu_list_request": args.cpu_list,
            "validated_provenance": common_provenance,
        },
        "metrics": metric_results,
    }
    (output_dir / "callback_polars_ab.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    with (output_dir / "callback_polars_ab_runs.csv").open(
        "w", newline="", encoding="utf-8"
    ) as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if all_pass else 1


if __name__ == "__main__":
    raise SystemExit(main())
