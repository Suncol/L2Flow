#!/usr/bin/env python3
"""Combine independently validated history stage benchmark processes."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import sys
import tempfile
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping, Sequence


SCHEMA_VERSION = 1
REQUESTED_STAGES = ("memfd", "object_decode", "column_build")
CONFIG_IDENTITY_FIELDS = (
    "records",
    "page_records",
    "rounds",
    "warmup_rounds",
    "snapshot_every",
    "workload",
    "trade_date",
    "instrument_id",
)
CSV_FIELDS = (
    "schema_version",
    "workload",
    "process_runs",
    "measured_scans",
    "records_per_scan",
    "page_records",
    "snapshots_per_scan",
    "ticks_per_scan",
    "stage",
    "total_ns",
    "ns_per_record",
    "share_full_scan",
    "share_requested_mix",
    "between_run_share_full_min",
    "between_run_share_full_mean",
    "between_run_share_full_max",
    "scan_wall_p50_ns",
)


def _parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "combine validated single-instrument history analysis.json files"
        )
    )
    parser.add_argument("artifact_root", type=Path)
    parser.add_argument(
        "--expected-runs-per-workload",
        type=int,
        default=0,
        help="fail unless every workload has this many independent runs",
    )
    return parser.parse_args(argv)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _positive_integer(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _nonnegative_integer(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a nonnegative integer")
    return value


def _finite_number(value: object, name: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
    ):
        raise ValueError(f"{name} must be a finite number")
    return float(value)


def _require_ratio(
    value: object,
    expected: float,
    name: str,
) -> None:
    observed = _finite_number(value, name)
    if not math.isclose(observed, expected, rel_tol=1e-12, abs_tol=1e-15):
        raise ValueError(
            f"{name} does not reconcile: {observed} != {expected}"
        )


def _ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        raise ValueError("ratio denominator must be positive")
    return numerator / denominator


def _r7(values: Sequence[int], probability: float) -> float:
    if not values:
        raise ValueError("cannot take a quantile of an empty sequence")
    ordered = sorted(values)
    if len(ordered) == 1:
        return float(ordered[0])
    position = (len(ordered) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def _only_integer(values: object, name: str) -> int:
    if (
        not isinstance(values, list)
        or len(values) != 1
        or isinstance(values[0], bool)
        or not isinstance(values[0], int)
        or values[0] < 0
    ):
        raise ValueError(f"{name} must contain one nonnegative integer")
    return values[0]


def _load_analysis(path: Path) -> dict[str, object]:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"cannot read analysis JSON {path}: {error}") from error
    if not isinstance(document, dict) or document.get("schema_version") != 1:
        raise ValueError(f"unsupported analysis schema in {path}")
    for field in ("aggregate", "benchmark", "definitions", "scans", "sources"):
        if field not in document:
            raise ValueError(f"{path} is missing {field}")
    if not isinstance(document["scans"], list) or not document["scans"]:
        raise ValueError(f"{path} has no measured scans")
    return document


def _stage_index(
    aggregate: Mapping[str, object], path: Path
) -> dict[str, Mapping[str, object]]:
    stages = aggregate.get("stages")
    if not isinstance(stages, list):
        raise ValueError(f"{path} aggregate.stages must be a list")
    result: dict[str, Mapping[str, object]] = {}
    for stage in stages:
        if not isinstance(stage, dict) or not isinstance(
            stage.get("stage"), str
        ):
            raise ValueError(f"{path} has an invalid aggregate stage")
        name = stage["stage"]
        if name in result:
            raise ValueError(f"{path} has duplicate stage {name}")
        result[name] = stage
    if set(result) != set(REQUESTED_STAGES):
        raise ValueError(
            f"{path} stage set mismatch: {sorted(result)!r}"
        )
    return result


def _atomic_csv(
    path: Path, rows: Sequence[Mapping[str, object]]
) -> None:
    temporary_name = None
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            newline="",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary_name = handle.name
            writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
            writer.writeheader()
            writer.writerows(rows)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, path)
        temporary_name = None
    finally:
        if temporary_name is not None:
            try:
                os.unlink(temporary_name)
            except FileNotFoundError:
                pass


def _atomic_json(path: Path, document: Mapping[str, object]) -> None:
    temporary_name = None
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            newline="",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary_name = handle.name
            json.dump(document, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, path)
        temporary_name = None
    finally:
        if temporary_name is not None:
            try:
                os.unlink(temporary_name)
            except FileNotFoundError:
                pass


def summarize(
    artifact_root: Path, expected_runs_per_workload: int
) -> dict[str, object]:
    artifact_root = artifact_root.resolve()
    if not artifact_root.is_dir():
        raise ValueError(f"artifact root does not exist: {artifact_root}")
    if expected_runs_per_workload < 0:
        raise ValueError("expected runs per workload cannot be negative")

    paths = sorted(artifact_root.glob("*/analysis.json"))
    if not paths:
        raise ValueError(f"no child analysis.json files under {artifact_root}")

    loaded = [(path, _load_analysis(path)) for path in paths]
    definitions = loaded[0][1]["definitions"]
    sources = loaded[0][1]["sources"]
    run_ids: set[int] = set()
    grouped: dict[str, list[tuple[Path, dict[str, object]]]] = defaultdict(list)
    inputs: list[dict[str, object]] = []
    for path, document in loaded:
        if document["definitions"] != definitions:
            raise ValueError(f"measurement definitions differ in {path}")
        if document["sources"] != sources:
            raise ValueError(f"measured source hashes differ in {path}")
        benchmark = document["benchmark"]
        if not isinstance(benchmark, dict):
            raise ValueError(f"{path} benchmark must be an object")
        workload = benchmark.get("workload")
        if not isinstance(workload, str) or not workload:
            raise ValueError(f"{path} has no workload")
        run_id = _positive_integer(
            benchmark.get("benchmark_run_id"),
            f"{path} benchmark_run_id",
        )
        if run_id in run_ids:
            raise ValueError(f"duplicate benchmark_run_id {run_id}")
        run_ids.add(run_id)
        grouped[workload].append((path, document))
        inputs.append(
            {
                "path": str(path.relative_to(artifact_root)),
                "sha256": _sha256(path),
                "benchmark_run_id": run_id,
                "workload": workload,
            }
        )

    if expected_runs_per_workload and any(
        len(runs) != expected_runs_per_workload
        for runs in grouped.values()
    ):
        counts = {name: len(runs) for name, runs in grouped.items()}
        raise ValueError(
            "unexpected independent run count per workload: "
            f"expected={expected_runs_per_workload} actual={counts}"
        )

    workload_summaries: list[dict[str, object]] = []
    csv_rows: list[dict[str, object]] = []
    for workload, runs in sorted(grouped.items()):
        first_benchmark = runs[0][1]["benchmark"]
        assert isinstance(first_benchmark, dict)
        config = first_benchmark.get("config")
        if not isinstance(config, dict):
            raise ValueError(f"{runs[0][0]} benchmark.config must be an object")
        config_identity = {
            field: config.get(field) for field in CONFIG_IDENTITY_FIELDS
        }

        scan_wall_values: list[int] = []
        total_records = 0
        total_scan_wall_ns = 0
        total_residual_ns = 0
        stage_totals = {stage: 0 for stage in REQUESTED_STAGES}
        per_run_stage_shares = {
            stage: [] for stage in REQUESTED_STAGES
        }
        run_summaries: list[dict[str, object]] = []
        for path, document in runs:
            benchmark = document["benchmark"]
            aggregate = document["aggregate"]
            scans = document["scans"]
            assert isinstance(benchmark, dict)
            if not isinstance(aggregate, dict) or not isinstance(scans, list):
                raise ValueError(f"{path} has malformed aggregate/scans")
            run_config = benchmark.get("config")
            if not isinstance(run_config, dict) or {
                field: run_config.get(field)
                for field in CONFIG_IDENTITY_FIELDS
            } != config_identity:
                raise ValueError(
                    f"workload configuration differs in {path}"
                )
            run_id = _positive_integer(
                benchmark.get("benchmark_run_id"),
                f"{path} benchmark_run_id",
            )
            config_run_id = _positive_integer(
                run_config.get("benchmark_run_id"),
                f"{path} config benchmark_run_id",
            )
            measured_scan_count = _positive_integer(
                benchmark.get("measured_scans"),
                f"{path} measured_scans",
            )
            config_rounds = _positive_integer(
                run_config.get("rounds"), f"{path} config rounds"
            )
            config_warmups = _nonnegative_integer(
                run_config.get("warmup_rounds"),
                f"{path} config warmup_rounds",
            )
            records_per_scan = _positive_integer(
                benchmark.get("records_per_scan"),
                f"{path} records_per_scan",
            )
            config_records = _positive_integer(
                run_config.get("records"), f"{path} config records"
            )
            config_instrument = _positive_integer(
                run_config.get("instrument_id"),
                f"{path} config instrument_id",
            )
            config_page_records = _positive_integer(
                run_config.get("page_records"),
                f"{path} config page_records",
            )
            requested_page_records = _only_integer(
                benchmark.get("requested_page_records"),
                f"{path} requested_page_records",
            )
            snapshots_per_scan = _only_integer(
                benchmark.get("snapshot_count_per_scan"),
                f"{path} snapshot_count_per_scan",
            )
            ticks_per_scan = _only_integer(
                benchmark.get("tick_count_per_scan"),
                f"{path} tick_count_per_scan",
            )
            expected_workload = {
                "tick_only": "tick_only",
                "snapshot_only": "snapshot_only",
                "fixed_mixed": "mixed",
            }.get(run_config.get("workload"))
            if (
                run_id != config_run_id
                or measured_scan_count != config_rounds
                or len(scans) != measured_scan_count
                or records_per_scan != config_records
                or requested_page_records != config_page_records
                or snapshots_per_scan + ticks_per_scan != records_per_scan
                or benchmark.get("workload") != workload
                or expected_workload != workload
            ):
                raise ValueError(
                    f"{path} benchmark/config identity does not reconcile"
                )

            stage_index = _stage_index(aggregate, path)
            run_records = _positive_integer(
                aggregate.get("total_records"),
                f"{path} total_records",
            )
            run_wall = _positive_integer(
                aggregate.get("total_scan_wall_ns"),
                f"{path} total_scan_wall_ns",
            )
            if run_records != records_per_scan * measured_scan_count:
                raise ValueError(
                    f"{path} total_records does not reconcile to scans"
                )

            run_scan_walls: list[int] = []
            scan_stage_totals = {
                stage: 0 for stage in REQUESTED_STAGES
            }
            scan_residual_total = 0
            observed_rounds: set[int] = set()
            observed_scan_ids: set[int] = set()
            observed_generations: set[int] = set()
            for scan in scans:
                if not isinstance(scan, dict):
                    raise ValueError(f"{path} contains a malformed scan")
                scan_wall = _positive_integer(
                    scan.get("scan_wall_ns"),
                    f"{path} scan_wall_ns",
                )
                scan_record_count = _positive_integer(
                    scan.get("record_count"),
                    f"{path} scan record_count",
                )
                scan_round = _nonnegative_integer(
                    scan.get("round_index"),
                    f"{path} scan round_index",
                )
                scan_id = _nonnegative_integer(
                    scan.get("scan_id"), f"{path} scan_id"
                )
                generation = _positive_integer(
                    scan.get("generation"), f"{path} generation"
                )
                if (
                    _positive_integer(
                        scan.get("benchmark_run_id"),
                        f"{path} scan benchmark_run_id",
                    )
                    != run_id
                    or _positive_integer(
                        scan.get("instrument_id"),
                        f"{path} scan instrument_id",
                    )
                    != config_instrument
                    or scan.get("workload") != workload
                    or scan_record_count != records_per_scan
                    or _nonnegative_integer(
                        scan.get("snapshot_count"),
                        f"{path} scan snapshot_count",
                    )
                    != snapshots_per_scan
                    or _nonnegative_integer(
                        scan.get("tick_count"),
                        f"{path} scan tick_count",
                    )
                    != ticks_per_scan
                ):
                    raise ValueError(
                        f"{path} scan identity/counts do not reconcile"
                    )
                observed_rounds.add(scan_round)
                observed_scan_ids.add(scan_id)
                observed_generations.add(generation)

                scan_requested = 0
                for stage_name in REQUESTED_STAGES:
                    stage_ns = _nonnegative_integer(
                        scan.get(f"{stage_name}_ns"),
                        f"{path} scan {stage_name}_ns",
                    )
                    scan_stage_totals[stage_name] += stage_ns
                    scan_requested += stage_ns
                if scan_requested <= 0:
                    raise ValueError(
                        f"{path} scan requested stage total must be positive"
                    )
                if (
                    _positive_integer(
                        scan.get("requested_stage_ns"),
                        f"{path} scan requested_stage_ns",
                    )
                    != scan_requested
                ):
                    raise ValueError(
                        f"{path} scan requested stages do not reconcile"
                    )
                scan_residual = _nonnegative_integer(
                    scan.get("requested_residual_full_ns"),
                    f"{path} scan requested_residual_full_ns",
                )
                if scan_requested + scan_residual != scan_wall:
                    raise ValueError(
                        f"{path} scan stages do not reconcile to wall"
                    )
                run_scan_walls.append(scan_wall)
                scan_residual_total += scan_residual

            if (
                observed_rounds != set(range(config_rounds))
                or observed_scan_ids
                != set(
                    range(
                        config_warmups,
                        config_warmups + config_rounds,
                    )
                )
                or len(observed_generations) != 1
                or sum(run_scan_walls) != run_wall
            ):
                raise ValueError(
                    f"{path} scan sequence/wall total does not reconcile"
                )

            run_requested_ns = 0
            for stage_name in REQUESTED_STAGES:
                stage = stage_index[stage_name]
                stage_ns = _nonnegative_integer(
                    stage.get("total_ns"),
                    f"{path} {stage_name}.total_ns",
                )
                if stage_ns != scan_stage_totals[stage_name]:
                    raise ValueError(
                        f"{path} {stage_name} total differs from scans"
                    )
                share = _ratio(stage_ns, run_wall)
                _require_ratio(
                    stage.get("share_full_scan"),
                    share,
                    f"{path} {stage_name}.share_full_scan",
                )
                _require_ratio(
                    stage.get("ns_per_record"),
                    _ratio(stage_ns, run_records),
                    f"{path} {stage_name}.ns_per_record",
                )
                stage_totals[stage_name] += stage_ns
                per_run_stage_shares[stage_name].append(float(share))
                run_requested_ns += stage_ns
            if run_requested_ns <= 0:
                raise ValueError(
                    f"{path} requested stage total must be positive"
                )
            if (
                _positive_integer(
                    aggregate.get("total_requested_stage_ns"),
                    f"{path} total_requested_stage_ns",
                )
                != run_requested_ns
            ):
                raise ValueError(
                    f"{path} requested stage aggregate does not reconcile"
                )
            for stage_name in REQUESTED_STAGES:
                _require_ratio(
                    stage_index[stage_name].get("share_requested_mix"),
                    _ratio(
                        scan_stage_totals[stage_name],
                        run_requested_ns,
                    ),
                    f"{path} {stage_name}.share_requested_mix",
                )

            run_residual = _nonnegative_integer(
                aggregate.get("requested_residual_full_ns"),
                f"{path} requested_residual_full_ns",
            )
            if (
                run_residual != scan_residual_total
                or run_requested_ns + run_residual != run_wall
            ):
                raise ValueError(
                    f"{path} requested stages do not reconcile to scan wall"
                )
            _require_ratio(
                aggregate.get("requested_residual_share_full_scan"),
                _ratio(run_residual, run_wall),
                f"{path} requested_residual_share_full_scan",
            )
            scan_wall_values.extend(run_scan_walls)
            total_records += run_records
            total_scan_wall_ns += run_wall
            total_residual_ns += run_residual
            run_summaries.append(
                {
                    "analysis": str(path.relative_to(artifact_root)),
                    "benchmark_run_id": run_id,
                    "measured_scans": measured_scan_count,
                    "total_records": run_records,
                    "total_scan_wall_ns": run_wall,
                }
            )

        total_requested_ns = sum(stage_totals.values())
        if total_requested_ns + total_residual_ns != total_scan_wall_ns:
            raise ValueError(f"{workload} totals do not reconcile")
        records_per_scan = _positive_integer(
            first_benchmark.get("records_per_scan"),
            f"{workload} records_per_scan",
        )
        snapshots_per_scan = _only_integer(
            first_benchmark.get("snapshot_count_per_scan"),
            f"{workload} snapshot_count_per_scan",
        )
        ticks_per_scan = _only_integer(
            first_benchmark.get("tick_count_per_scan"),
            f"{workload} tick_count_per_scan",
        )
        page_records = _positive_integer(
            config.get("page_records"), f"{workload} page_records"
        )
        stage_summaries: list[dict[str, object]] = []
        scan_p50_ns = _r7(scan_wall_values, 0.50)
        measured_scans = len(scan_wall_values)
        for stage_name in REQUESTED_STAGES:
            stage_ns = stage_totals[stage_name]
            run_shares = per_run_stage_shares[stage_name]
            stage_summary = {
                "stage": stage_name,
                "total_ns": stage_ns,
                "ns_per_record": _ratio(stage_ns, total_records),
                "share_full_scan": _ratio(stage_ns, total_scan_wall_ns),
                "share_requested_mix": _ratio(
                    stage_ns, total_requested_ns
                ),
                "between_run_share_full": {
                    "minimum": min(run_shares),
                    "mean": sum(run_shares) / len(run_shares),
                    "maximum": max(run_shares),
                },
            }
            stage_summaries.append(stage_summary)
            csv_rows.append(
                {
                    "schema_version": SCHEMA_VERSION,
                    "workload": workload,
                    "process_runs": len(runs),
                    "measured_scans": measured_scans,
                    "records_per_scan": records_per_scan,
                    "page_records": page_records,
                    "snapshots_per_scan": snapshots_per_scan,
                    "ticks_per_scan": ticks_per_scan,
                    "stage": stage_name,
                    "total_ns": stage_ns,
                    "ns_per_record": stage_summary["ns_per_record"],
                    "share_full_scan": stage_summary["share_full_scan"],
                    "share_requested_mix": (
                        stage_summary["share_requested_mix"]
                    ),
                    "between_run_share_full_min": min(run_shares),
                    "between_run_share_full_mean": (
                        sum(run_shares) / len(run_shares)
                    ),
                    "between_run_share_full_max": max(run_shares),
                    "scan_wall_p50_ns": scan_p50_ns,
                }
            )

        residual_share = _ratio(total_residual_ns, total_scan_wall_ns)
        workload_summaries.append(
            {
                "workload": workload,
                "process_runs": len(runs),
                "measured_scans": measured_scans,
                "records_per_scan": records_per_scan,
                "page_records": page_records,
                "snapshots_per_scan": snapshots_per_scan,
                "ticks_per_scan": ticks_per_scan,
                "total_records": total_records,
                "total_scan_wall_ns": total_scan_wall_ns,
                "scan_wall_p50_ns": scan_p50_ns,
                "requested_residual_full_ns": total_residual_ns,
                "requested_residual_share_full_scan": residual_share,
                "stages": stage_summaries,
                "runs": run_summaries,
            }
        )
        csv_rows.append(
            {
                "schema_version": SCHEMA_VERSION,
                "workload": workload,
                "process_runs": len(runs),
                "measured_scans": measured_scans,
                "records_per_scan": records_per_scan,
                "page_records": page_records,
                "snapshots_per_scan": snapshots_per_scan,
                "ticks_per_scan": ticks_per_scan,
                "stage": "requested_residual",
                "total_ns": total_residual_ns,
                "ns_per_record": _ratio(total_residual_ns, total_records),
                "share_full_scan": residual_share,
                "share_requested_mix": "",
                "between_run_share_full_min": "",
                "between_run_share_full_mean": "",
                "between_run_share_full_max": "",
                "scan_wall_p50_ns": scan_p50_ns,
            }
        )

    result: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "artifact_root": str(artifact_root),
        "definitions": definitions,
        "inputs": inputs,
        "summarizer": {
            "path": str(Path(__file__).resolve()),
            "sha256": _sha256(Path(__file__).resolve()),
        },
        "sources": sources,
        "workloads": workload_summaries,
    }
    json_path = artifact_root / "comparison.json"
    csv_path = artifact_root / "comparison.csv"
    # CSV is committed first. The JSON is the final completion marker and
    # binds the exact CSV bytes, so an interrupted rerun cannot silently pair
    # a stale JSON document with a newly replaced CSV.
    _atomic_csv(csv_path, csv_rows)
    result["outputs"] = {
        "comparison_csv": {
            "path": csv_path.name,
            "bytes": csv_path.stat().st_size,
            "sha256": _sha256(csv_path),
        }
    }
    _atomic_json(json_path, result)
    return result


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    try:
        result = summarize(
            args.artifact_root, args.expected_runs_per_workload
        )
    except (OSError, ValueError) as error:
        print(f"benchmark summary failed: {error}", file=sys.stderr)
        return 1
    concise = {
        workload["workload"]: {
            stage["stage"]: {
                "ns_per_record": stage["ns_per_record"],
                "share_full_scan": stage["share_full_scan"],
                "share_requested_mix": stage["share_requested_mix"],
            }
            for stage in workload["stages"]
        }
        for workload in result["workloads"]
    }
    print(json.dumps(concise, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
