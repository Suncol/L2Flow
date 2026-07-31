#!/usr/bin/env python3
"""Run isolated parallel-decoder throughput trials with strict invariants."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import subprocess
from pathlib import Path


RESULT_PREFIX = "THROUGHPUT_RESULT "
ENV_PREFIX = "THROUGHPUT_ENV "
DEFAULT_RATES = (500_000,)
DEFAULT_PARALLEL_WORKERS = (0, 1, 2, 4, 8)
DEFAULT_STORE_WORKERS = (4, 8)
DEFAULT_WORKLOADS = (
    "five_tuple_uniform",
    "four_source_balanced",
    "hot_shenzhen_tick_source",
)
DEFAULT_SINKS = ("fast",)
PARALLEL_ISSUE_HIGH_WATER_SEMANTICS = (
    "maximum_across_workers_of_sum_of_four_per_shard_"
    "conservative_high_water_upper_bounds;"
    "conservative_simultaneous_depth_upper_bound"
)
PARALLEL_COMPLETION_HIGH_WATER_SEMANTICS = (
    "periodically_sampled_simultaneous_depth;"
    "true_high_water_lower_bound"
)


def _fields(line: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    for token in line.split()[1:]:
        key, separator, value = token.partition("=")
        if separator:
            if not key:
                raise ValueError("empty benchmark field name")
            if key in fields:
                raise ValueError(f"duplicate benchmark field: {key}")
            fields[key] = value
    return fields


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


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


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run each rate/topology/workload in a fresh process. The default "
            "queue and segment values match mdl-production-router defaults."
        )
    )
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--duration-ms", type=int, default=1_000)
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--instruments-per-market", type=int, default=256)
    parser.add_argument("--rates", type=int, nargs="+", default=DEFAULT_RATES)
    parser.add_argument(
        "--parallel-workers",
        type=int,
        nargs="+",
        default=DEFAULT_PARALLEL_WORKERS,
    )
    parser.add_argument(
        "--store-workers", type=int, nargs="+", default=DEFAULT_STORE_WORKERS
    )
    parser.add_argument(
        "--workloads", nargs="+", default=DEFAULT_WORKLOADS
    )
    parser.add_argument("--sinks", nargs="+", default=DEFAULT_SINKS)
    parser.add_argument("--decoder-queue", type=int, default=65_536)
    parser.add_argument("--store-queue", type=int, default=32_768)
    parser.add_argument("--segment-kib", type=int, default=64)
    parser.add_argument(
        "--idle-inline",
        type=int,
        choices=(0, 1),
        required=True,
        help=(
            "Explicit parallel idle-inline mode. Use 1 for the "
            "latency-oriented adaptive candidate and 0 for farm-only "
            "scaling."
        ),
    )
    parser.add_argument("--cpu-list", default="")
    parser.add_argument(
        "--max-backlog-records",
        type=int,
        default=1_024,
        help=(
            "Maximum accepted-minus-applied records at the instant offering "
            "ends; this prevents post-offer drain from hiding an unstable run."
        ),
    )
    parser.add_argument("--timeout-seconds", type=int, default=180)
    parser.add_argument(
        "--require-farm",
        action="store_true",
        help=(
            "Require a positive worker-farm sample. Leave unset when an "
            "adaptive low-load run is allowed to remain inline."
        ),
    )
    parser.add_argument(
        "--require-capacity-headroom",
        action="store_true",
        help=(
            "Fail closed for positive-worker trials because the current "
            "issue and completion high-water telemetry cannot prove exact "
            "simultaneous capacity headroom."
        ),
    )
    return parser


def _int(row: dict[str, object], key: str) -> int:
    return int(row.get(key, -1))


def _require_integer_match(
    row: dict[str, object],
    actual: str,
    expected: str,
    failures: list[str],
) -> None:
    if _int(row, actual) != _int(row, expected):
        failures.append(f"{actual}_request_mismatch")


def _require_text_match(
    row: dict[str, object],
    actual: str,
    expected: str,
    failures: list[str],
) -> None:
    if str(row.get(actual, "")) != str(row.get(expected, "")):
        failures.append(f"{actual}_request_mismatch")


def _parallel_worker_counts(
    row: dict[str, object],
    worker_count: int,
    failures: list[str],
) -> list[int] | None:
    raw = row.get("parallel_worker_parsed_csv")
    if not isinstance(raw, str):
        failures.append("parallel_worker_telemetry_missing")
        return None
    if worker_count == 0:
        if raw:
            failures.append("parallel_worker_csv_not_empty")
        return []
    if not raw:
        failures.append("parallel_worker_telemetry_missing")
        return None
    fields = raw.split(",")
    if len(fields) != worker_count:
        failures.append("parallel_worker_csv_length_mismatch")
        return None
    if any(not value.isascii() or not value.isdecimal() for value in fields):
        failures.append("parallel_worker_csv_invalid")
        return None
    return [int(value, 10) for value in fields]


def _classify(
    row: dict[str, object],
    max_backlog: int,
    *,
    require_farm: bool = False,
    require_capacity_headroom: bool = False,
) -> list[str]:
    failures: list[str] = []
    required_ones = (
        "target_met",
        "process_survived",
        "complete_prefix",
        "stopped_clean",
        "steady_state_met",
        "certified_healthy",
    )
    required_zeros = (
        "fatal_during_offer",
        "fatal_final",
        "message_patch_failed",
        "rejected",
        "post_cut",
        "store_failed_appends",
        "decoder_full_count",
        "service_failed",
        "parallel_parse_failures",
        "parallel_completion_publish_failures",
        "parallel_discarded",
        "parallel_farm_outstanding",
        "certified_dropped",
        "certified_frozen_channels",
        "certified_global_frozen",
    )
    if _int(row, "exit_code") != 0:
        failures.append("nonzero_exit")
    if _int(row, "env_line_count") != 1:
        failures.append("missing_or_duplicate_environment")
    if _int(row, "result_line_count") != 1:
        failures.append("missing_or_duplicate_result")
        return failures
    if row.get("env_parse_error"):
        failures.append("invalid_environment_fields")
    if row.get("result_parse_error"):
        failures.append("invalid_result_fields")
    if failures and (
        row.get("env_parse_error") or row.get("result_parse_error")
    ):
        return failures

    for actual, expected in (
        ("target_rps", "requested_rate"),
        ("duration_ms", "requested_duration_ms"),
        ("instruments_per_market", "requested_instruments_per_market"),
        ("store_worker_count", "requested_store_workers"),
        ("parallel_decoder_workers", "requested_parallel_workers"),
        ("env_target_rps", "requested_rate"),
        ("env_duration_ms", "requested_duration_ms"),
        ("env_instruments_per_market", "requested_instruments_per_market"),
        ("env_store_worker_count", "requested_store_workers"),
        ("env_parallel_decoder_workers", "requested_parallel_workers"),
        ("env_parallel_idle_inline", "requested_idle_inline"),
        (
            "env_decoder_queue_capacity_per_source",
            "requested_decoder_queue",
        ),
        (
            "env_store_queue_capacity_per_source_worker",
            "requested_store_queue",
        ),
        ("env_store_segment_kib", "requested_segment_kib"),
    ):
        _require_integer_match(row, actual, expected, failures)
    for actual, expected in (
        ("workload", "requested_workload"),
        ("sink", "requested_sink"),
        ("env_workload", "requested_workload"),
        ("env_sink", "requested_sink"),
    ):
        _require_text_match(row, actual, expected, failures)
    if row.get("env_callback_contract") != "serialized":
        failures.append("callback_contract_mismatch")
    if row.get("env_pacing") != "absolute_deadline_no_batch_wait":
        failures.append("pacing_contract_mismatch")
    if row.get("env_clock") != "CLOCK_MONOTONIC":
        failures.append("clock_contract_mismatch")
    if _int(row, "env_parallel_farm_activation_configured") != 8_192:
        failures.append("parallel_farm_activation_config_mismatch")
    requested_decoder_queue = _int(row, "requested_decoder_queue")
    expected_effective_activation = min(
        8_192,
        requested_decoder_queue - max(1, requested_decoder_queue // 4),
    )
    if (
        _int(row, "env_parallel_farm_activation_effective")
        != expected_effective_activation
    ):
        failures.append("parallel_farm_activation_effective_mismatch")
    requested_affinity = str(row.get("requested_affinity", ""))
    if requested_affinity:
        try:
            if _affinity_set(str(row.get("env_affinity", ""))) != (
                _affinity_set(requested_affinity)
            ):
                failures.append("affinity_request_mismatch")
        except ValueError:
            failures.append("invalid_affinity_telemetry")

    expected_planned = (
        _int(row, "requested_rate")
        * _int(row, "requested_duration_ms")
        // 1_000
    )
    if _int(row, "planned_callbacks") != expected_planned:
        failures.append("planned_callbacks_request_mismatch")
    if _int(row, "env_planned_callbacks") != expected_planned:
        failures.append("env_planned_callbacks_request_mismatch")
    for key in required_ones:
        if _int(row, key) != 1:
            failures.append(f"{key}_not_one")
    for key in required_zeros:
        if _int(row, key) != 0:
            failures.append(f"{key}_not_zero")
    planned = _int(row, "planned_callbacks")
    for key in (
        "invoked_callbacks",
        "accepted",
        "decoded",
        "applied",
        "store_appended",
    ):
        if _int(row, key) != planned:
            failures.append(f"{key}_not_planned")
    backlog = _int(row, "backlog_before_drain")
    if backlog < 0 or backlog > max_backlog:
        failures.append("final_backlog_over_budget")
    # A queue can remain bounded while fluctuating. This only rejects clear
    # second-half accumulation larger than the same explicit backlog budget.
    if _int(row, "backlog_q100") - _int(row, "backlog_q50") > max_backlog:
        failures.append("second_half_backlog_growth")
    parallel_workers = _int(row, "requested_parallel_workers")
    if row.get("requested_workload") != "single_instrument" and (
        _int(row, "covered_store_worker_count")
        != _int(row, "store_worker_count")
    ):
        failures.append("workload_does_not_cover_all_store_workers")
    if parallel_workers > 0:
        if _int(row, "parallel_enabled") != 1:
            failures.append("parallel_path_not_enabled")
        expected_inline = _int(row, "requested_idle_inline")
        if _int(row, "parallel_idle_inline") != expected_inline:
            failures.append("parallel_idle_inline_mode_mismatch")
        for key in (
            "parallel_dispatched",
            "parallel_completed",
            "parallel_committed",
        ):
            if _int(row, key) != planned:
                failures.append(f"{key}_not_planned")
        if _int(row, "parallel_parsed") != _int(row, "parallel_farm"):
            failures.append("parallel_parsed_not_farm")
        if (
            _int(row, "parallel_inline") + _int(row, "parallel_farm")
            != planned
        ):
            failures.append("parallel_inline_plus_farm_not_planned")
        farm_messages = _int(row, "parallel_farm")
        if require_farm and farm_messages <= 0:
            failures.append("parallel_farm_not_exercised")

        worker_counts = _parallel_worker_counts(
            row, parallel_workers, failures
        )
        if worker_counts is not None:
            active_workers = sum(count > 0 for count in worker_counts)
            if sum(worker_counts) != _int(row, "parallel_parsed"):
                failures.append("parallel_worker_sum_mismatch")
            if (
                _int(row, "parallel_active_worker_count")
                != active_workers
            ):
                failures.append("parallel_active_worker_count_mismatch")
            if farm_messages > 0 and active_workers < 2:
                failures.append("parallel_active_worker_count_below_two")

        slots = _int(row, "parallel_slots_per_source_worker")
        issue_capacity = _int(row, "parallel_issue_capacity_per_worker")
        completion_capacity = _int(
            row, "parallel_completion_capacity_per_source"
        )
        if slots <= 0:
            failures.append("parallel_slot_count_invalid")
        if issue_capacity != 4 * slots:
            failures.append("parallel_issue_capacity_mismatch")
        if completion_capacity != parallel_workers * slots:
            failures.append("parallel_completion_capacity_mismatch")
        issue_high_water = _int(row, "parallel_issue_high_water_max")
        completion_high_water = _int(
            row, "parallel_completion_high_water_max"
        )
        lease_waits = _int(row, "parallel_lease_waits")
        if issue_high_water < 0 or issue_high_water > issue_capacity:
            failures.append("parallel_issue_high_water_invalid")
        if (
            completion_high_water < 0
            or completion_high_water > completion_capacity
        ):
            failures.append("parallel_completion_high_water_invalid")
        if lease_waits < 0:
            failures.append("parallel_lease_waits_invalid")
        # A shard producer samples the consumer head before publishing its
        # tail, so a concurrent pop can make the retained shard high-water a
        # conservative upper bound. The issue field is the maximum across
        # workers of four such independently timed shard bounds. Completion
        # is sampled periodically and is a lower bound on its true peak.
        # Neither field can prove exact simultaneous headroom.
        if require_capacity_headroom:
            failures.append("parallel_capacity_headroom_unproven")
    else:
        worker_counts = _parallel_worker_counts(row, 0, failures)
        if _int(row, "parallel_enabled") != 0:
            failures.append("parallel_path_enabled_for_zero_workers")
        for key in (
            "parallel_parsed",
            "parallel_dispatched",
            "parallel_inline",
            "parallel_farm",
            "parallel_completed",
            "parallel_committed",
        ):
            if _int(row, key) != 0:
                failures.append(f"{key}_not_zero")
        if worker_counts is not None and _int(
            row, "parallel_active_worker_count"
        ) != 0:
            failures.append("parallel_active_worker_count_not_zero")
    return failures


def _capacity_pressure(row: dict[str, object]) -> str:
    actual_failure_fields = (
        "fatal_during_offer",
        "fatal_final",
        "decoder_full_count",
        "parallel_completion_publish_failures",
        "parallel_parse_failures",
        "store_failed_appends",
        "service_failed",
    )
    if any(_int(row, key) > 0 for key in actual_failure_fields):
        return "ACTUAL_OPERATION_FAILURE"
    workers = _int(row, "requested_parallel_workers")
    if workers <= 0:
        return "NOT_APPLICABLE"
    issue_capacity = _int(row, "parallel_issue_capacity_per_worker")
    completion_capacity = _int(
        row, "parallel_completion_capacity_per_source"
    )
    slots = _int(row, "parallel_slots_per_source_worker")
    issue_high_water = _int(row, "parallel_issue_high_water_max")
    completion_high_water = _int(
        row, "parallel_completion_high_water_max"
    )
    lease_waits = _int(row, "parallel_lease_waits")
    if (
        slots <= 0
        or issue_capacity != 4 * slots
        or completion_capacity != workers * slots
        or issue_high_water < 0
        or issue_high_water > issue_capacity
        or completion_high_water < 0
        or completion_high_water > completion_capacity
        or lease_waits < 0
    ):
        return "TELEMETRY_INVALID"
    if (
        issue_high_water == issue_capacity
        or completion_high_water == completion_capacity
        or lease_waits > 0
    ):
        # Equality is retained as a pressure signal, not as proof that the
        # aggregate queues were simultaneously full. Lease waits are likewise
        # evidence of pressure without being an operation failure.
        return "PRESSURE_OBSERVED_NO_FAILURE"
    return "HEADROOM_UNPROVEN"


def main() -> int:
    args = _parser().parse_args()
    positive = (
        args.duration_ms,
        args.repeats,
        args.instruments_per_market,
        args.decoder_queue,
        args.store_queue,
        args.segment_kib,
        args.timeout_seconds,
    )
    if any(value <= 0 for value in positive) or args.max_backlog_records < 0:
        raise SystemExit("durations, counts, capacities, and timeout must be positive")
    if any(rate <= 0 for rate in args.rates):
        raise SystemExit("rates must be positive")
    if any(worker <= 0 for worker in args.store_workers):
        raise SystemExit("store worker counts must be positive")
    if any(worker < 0 for worker in args.parallel_workers):
        raise SystemExit("parallel worker counts must be nonnegative")
    allowed_workloads = {
        "single_instrument",
        "five_tuple_uniform",
        "four_source_balanced",
        "hot_shenzhen_tick_source",
    }
    allowed_sinks = {"fast", "fast_certified"}
    if not set(args.workloads) <= allowed_workloads:
        raise SystemExit("unknown workload")
    if not set(args.sinks) <= allowed_sinks:
        raise SystemExit("unknown sink")

    binary = args.binary.resolve()
    if not binary.is_file():
        raise SystemExit("benchmark binary must be a file")
    binary_sha256 = _sha256_file(binary)
    try:
        requested_cpu_set = _requested_cpu_set(args.cpu_list)
    except ValueError as error:
        raise SystemExit(str(error)) from error
    requested_affinity = (
        ""
        if requested_cpu_set is None
        else ",".join(str(cpu) for cpu in sorted(requested_cpu_set))
        + f";count={len(requested_cpu_set)}"
    )
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, object]] = []
    environment = os.environ.copy()
    environment["PYTHONDONTWRITEBYTECODE"] = "1"

    for rate in args.rates:
        for workload in args.workloads:
            for sink in args.sinks:
                for store_workers in args.store_workers:
                    for parallel_workers in args.parallel_workers:
                        for repeat in range(1, args.repeats + 1):
                            command = [
                                str(binary),
                                "--throughput-profile-benchmark",
                                str(rate),
                                str(args.duration_ms),
                                str(args.instruments_per_market),
                                str(store_workers),
                                str(parallel_workers),
                                str(args.idle_inline),
                                str(args.decoder_queue),
                                str(args.store_queue),
                                str(args.segment_kib),
                                workload,
                                sink,
                            ]
                            if args.cpu_list:
                                command = ["taskset", "-c", args.cpu_list, *command]
                            name = (
                                f"rate{rate}_{workload}_{sink}_store{store_workers}_"
                                f"parallel{parallel_workers}_run{repeat}"
                            )
                            try:
                                completed = subprocess.run(
                                    command,
                                    check=False,
                                    capture_output=True,
                                    text=True,
                                    env=environment,
                                    timeout=args.timeout_seconds,
                                )
                                stdout = completed.stdout
                                stderr = completed.stderr
                                exit_code = completed.returncode
                                timed_out = 0
                            except subprocess.TimeoutExpired as error:
                                stdout = error.stdout or ""
                                stderr = error.stderr or ""
                                exit_code = 124
                                timed_out = 1
                            log_path = output_dir / f"{name}.log"
                            log_path.write_text(stdout + stderr, encoding="utf-8")
                            result_lines = [
                                line
                                for line in stdout.splitlines()
                                if line.startswith(RESULT_PREFIX)
                            ]
                            environment_lines = [
                                line
                                for line in stdout.splitlines()
                                if line.startswith(ENV_PREFIX)
                            ]
                            row: dict[str, object] = {
                                "requested_rate": rate,
                                "requested_duration_ms": args.duration_ms,
                                "requested_instruments_per_market": (
                                    args.instruments_per_market
                                ),
                                "requested_workload": workload,
                                "requested_sink": sink,
                                "requested_store_workers": store_workers,
                                "requested_parallel_workers": parallel_workers,
                                "requested_idle_inline": args.idle_inline,
                                "requested_decoder_queue": args.decoder_queue,
                                "requested_store_queue": args.store_queue,
                                "requested_segment_kib": args.segment_kib,
                                "requested_affinity": requested_affinity,
                                "binary_sha256": binary_sha256,
                                "repeat": repeat,
                                "exit_code": exit_code,
                                "timed_out": timed_out,
                                "env_line_count": len(environment_lines),
                                "result_line_count": len(result_lines),
                                "log": log_path.name,
                            }
                            if len(environment_lines) == 1:
                                try:
                                    row.update(
                                        {
                                            f"env_{key}": value
                                            for key, value in _fields(
                                                environment_lines[0]
                                            ).items()
                                        }
                                    )
                                except ValueError as error:
                                    row["env_parse_error"] = str(error)
                            if len(result_lines) == 1:
                                try:
                                    result_fields = _fields(result_lines[0])
                                    collisions = set(result_fields) & set(row)
                                    if collisions:
                                        raise ValueError(
                                            "result fields overwrite runner "
                                            "metadata: "
                                            + ",".join(sorted(collisions))
                                        )
                                    row.update(result_fields)
                                except ValueError as error:
                                    row["result_parse_error"] = str(error)
                            try:
                                functional_failures = _classify(
                                    row, args.max_backlog_records
                                )
                                failures = _classify(
                                    row,
                                    args.max_backlog_records,
                                    require_farm=args.require_farm,
                                    require_capacity_headroom=(
                                        args.require_capacity_headroom
                                    ),
                                )
                            except (TypeError, ValueError) as error:
                                row["classification_error"] = str(error)
                                functional_failures = [
                                    "invalid_numeric_field"
                                ]
                                failures = ["invalid_numeric_field"]
                            row["functional_verdict"] = (
                                "PASS" if not functional_failures else "FAIL"
                            )
                            row["functional_failure_reasons"] = ",".join(
                                functional_failures
                            )
                            row["capacity_pressure_verdict"] = (
                                _capacity_pressure(row)
                            )
                            row["verdict"] = "PASS" if not failures else "FAIL"
                            row["failure_reasons"] = ",".join(failures)
                            rows.append(row)
                            print(
                                f"{name} verdict={row['verdict']} "
                                f"failures={row['failure_reasons'] or 'none'}"
                            )

    if _sha256_file(binary) != binary_sha256:
        raise SystemExit("benchmark binary changed during the matrix run")

    metadata = {
        "binary": str(binary),
        "binary_sha256": binary_sha256,
        "duration_ms": args.duration_ms,
        "instruments_per_market": args.instruments_per_market,
        "decoder_queue": args.decoder_queue,
        "store_queue": args.store_queue,
        "segment_kib": args.segment_kib,
        "idle_inline": args.idle_inline,
        "max_backlog_records": args.max_backlog_records,
        "require_farm": args.require_farm,
        "require_capacity_headroom": args.require_capacity_headroom,
        "parallel_issue_high_water_semantics": (
            PARALLEL_ISSUE_HIGH_WATER_SEMANTICS
        ),
        "parallel_completion_high_water_semantics": (
            PARALLEL_COMPLETION_HIGH_WATER_SEMANTICS
        ),
        "cpu_list": args.cpu_list,
        "trial_count": len(rows),
        "passed_trials": sum(row["verdict"] == "PASS" for row in rows),
    }
    (output_dir / "throughput_matrix.json").write_text(
        json.dumps({"metadata": metadata, "trials": rows}, indent=2, sort_keys=True)
        + "\n",
        encoding="utf-8",
    )
    fieldnames = sorted({key for row in rows for key in row})
    with (output_dir / "throughput_matrix.csv").open(
        "w", newline="", encoding="utf-8"
    ) as output:
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(json.dumps(metadata, sort_keys=True))
    return 0 if metadata["passed_trials"] == metadata["trial_count"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
