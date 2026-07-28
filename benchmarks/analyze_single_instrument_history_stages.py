#!/usr/bin/env python3
"""Validate and summarize single-instrument history stage benchmarks."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import platform
import subprocess
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping, Sequence


SCHEMA_VERSION = 1
HISTORY_PAGE_HEADER_BYTES = 4096
HISTORY_DESCRIPTOR_BYTES = 40
SNAPSHOT_PAYLOAD_BYTES = 3104
TICK_PAYLOAD_BYTES = 336

CLIENT_PAGE_FIELDS = (
    "schema_version",
    "benchmark_run_id",
    "scan_id",
    "phase",
    "round_index",
    "open_request_id",
    "read_request_id",
    "generation",
    "instrument_id",
    "page_index",
    "eof",
    "record_count",
    "snapshot_count",
    "tick_count",
    "page_mapping_bytes",
    "read_wall_ns",
    "object_decode_ns",
    "column_build_ns",
    "cumulative_record_count",
    "cumulative_source_count_0",
    "cumulative_source_count_1",
    "cumulative_source_count_2",
    "cumulative_source_count_3",
    "first_ingress_sequence",
    "last_ingress_sequence",
    "ingress_sequence_sum",
    "ingress_sequence_xor",
)

CLIENT_SCAN_FIELDS = (
    "schema_version",
    "benchmark_run_id",
    "scan_id",
    "phase",
    "round_index",
    "open_request_id",
    "generation",
    "instrument_id",
    "requested_page_records",
    "open_wall_ns",
    "scan_wall_ns",
    "data_page_read_wall_ns",
    "eof_read_wall_ns",
    "object_decode_ns",
    "column_build_ns",
    "data_page_count",
    "read_request_count",
    "page_mapping_bytes",
    "total_record_count",
    "source_record_count_0",
    "source_record_count_1",
    "source_record_count_2",
    "source_record_count_3",
    "generation_total_record_count",
    "generation_source_record_count_0",
    "generation_source_record_count_1",
    "generation_source_record_count_2",
    "generation_source_record_count_3",
    "first_ingress_sequence",
    "last_ingress_sequence",
    "ingress_sequence_sum",
    "ingress_sequence_xor",
    "eof_seen",
)

SERVER_PAGE_FIELDS = (
    "schema_version",
    "benchmark_run_id",
    "open_request_id",
    "read_request_id",
    "generation",
    "instrument_id",
    "page_index",
    "record_count",
    "snapshot_count",
    "tick_count",
    "page_mapping_bytes",
    "clock_read_failures",
    "cursor_read_ns",
    "classify_layout_ns",
    "memfd_prepare_ns",
    "projection_ns",
    "memfd_finalize_ns",
    "build_total_ns",
    "token_ns",
    "send_ns",
)

CONFIG_FIELDS = (
    "schema_version",
    "benchmark_run_id",
    "records",
    "page_records",
    "rounds",
    "warmup_rounds",
    "snapshot_every",
    "seed",
    "workload",
    "trade_date",
    "instrument_id",
)

STAGE_FIELDS = (
    "cursor_read_ns",
    "classify_layout_ns",
    "memfd_prepare_ns",
    "projection_ns",
    "memfd_finalize_ns",
    "build_total_ns",
    "token_ns",
    "send_ns",
)


def _parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "validate and summarize single-instrument history stage CSVs"
        )
    )
    parser.add_argument("artifact_directory", type=Path)
    parser.add_argument(
        "--output",
        type=Path,
        help="analysis JSON path (default: ARTIFACT_DIRECTORY/analysis.json)",
    )
    return parser.parse_args(argv)


def _read_csv(path: Path, expected_fields: Sequence[str]) -> list[dict[str, str]]:
    if not path.is_file():
        raise ValueError(f"missing benchmark input: {path}")
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        actual = tuple(reader.fieldnames or ())
        if actual != tuple(expected_fields):
            raise ValueError(
                f"{path.name} schema mismatch:\n"
                f"expected={tuple(expected_fields)!r}\nactual={actual!r}"
            )
        rows = list(reader)
    if not rows:
        raise ValueError(f"{path.name} contains no rows")
    return rows


def _integer(row: Mapping[str, str], field: str) -> int:
    text = row[field]
    if not text or text.strip() != text:
        raise ValueError(f"{field} is not a canonical integer: {text!r}")
    try:
        value = int(text, 10)
    except ValueError as error:
        raise ValueError(f"{field} is not an integer: {text!r}") from error
    if value < 0 or text != str(value):
        raise ValueError(f"{field} is not a canonical nonnegative integer")
    return value


def _integers(
    rows: Iterable[Mapping[str, str]], fields: Iterable[str]
) -> None:
    for row in rows:
        if _integer(row, "schema_version") != SCHEMA_VERSION:
            raise ValueError("unsupported benchmark schema_version")
        for field in fields:
            _integer(row, field)


def _page_key(row: Mapping[str, str]) -> tuple[int, int, int, int]:
    return (
        _integer(row, "benchmark_run_id"),
        _integer(row, "open_request_id"),
        _integer(row, "generation"),
        _integer(row, "page_index"),
    )


def _scan_key(row: Mapping[str, str]) -> tuple[int, int, int]:
    return (
        _integer(row, "benchmark_run_id"),
        _integer(row, "open_request_id"),
        _integer(row, "generation"),
    )


def _unique_index(
    rows: Iterable[dict[str, str]],
    key_function,
    name: str,
) -> dict[tuple[int, ...], dict[str, str]]:
    result: dict[tuple[int, ...], dict[str, str]] = {}
    for row in rows:
        key = key_function(row)
        if key in result:
            raise ValueError(f"duplicate {name} key: {key}")
        result[key] = row
    return result


def _r7(values: Sequence[float], probability: float) -> float:
    if not values:
        raise ValueError("cannot take a quantile of an empty sequence")
    if probability < 0.0 or probability > 1.0:
        raise ValueError("quantile probability must be in [0, 1]")
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def _distribution(values: Sequence[int | float]) -> dict[str, float | int]:
    if not values:
        raise ValueError("cannot summarize an empty sequence")
    floating = [float(value) for value in values]
    return {
        "samples": len(values),
        "minimum": min(values),
        "mean": sum(floating) / len(floating),
        "p50": _r7(floating, 0.50),
        "p90": _r7(floating, 0.90),
        "p95": _r7(floating, 0.95),
        "p99": _r7(floating, 0.99),
        "maximum": max(values),
    }


def _ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        raise ValueError("ratio denominator must be positive")
    return numerator / denominator


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _git(command: Sequence[str], cwd: Path) -> str | None:
    try:
        result = subprocess.run(
            ["git", *command],
            cwd=cwd,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    return result.stdout.strip()


def _write_csv(
    path: Path, fieldnames: Sequence[str], rows: Iterable[Mapping[str, object]]
) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _workload(snapshot_count: int, tick_count: int) -> str:
    if snapshot_count == 0 and tick_count > 0:
        return "tick_only"
    if tick_count == 0 and snapshot_count > 0:
        return "snapshot_only"
    if snapshot_count > 0 and tick_count > 0:
        return "mixed"
    return "empty"


def _xor_one_to(value: int) -> int:
    """Return 1 ^ 2 ^ ... ^ value for a nonnegative integer."""

    remainder = value & 3
    if remainder == 0:
        return value
    if remainder == 1:
        return 1
    if remainder == 2:
        return value + 1
    return 0


def _expected_snapshot_count(records: int, snapshot_every: int) -> int:
    if snapshot_every == 0:
        return 0
    if snapshot_every == 1:
        return records
    return (records - 1) // snapshot_every + 1


def analyze(artifact_directory: Path) -> dict[str, object]:
    artifact_directory = artifact_directory.resolve()
    client_pages_path = artifact_directory / "client_pages.csv"
    client_scans_path = artifact_directory / "client_scans.csv"
    server_pages_path = artifact_directory / "server_pages.csv"
    config_path = artifact_directory / "benchmark_config.csv"

    client_pages = _read_csv(client_pages_path, CLIENT_PAGE_FIELDS)
    client_scans = _read_csv(client_scans_path, CLIENT_SCAN_FIELDS)
    server_pages = _read_csv(server_pages_path, SERVER_PAGE_FIELDS)
    config_rows = _read_csv(config_path, CONFIG_FIELDS)
    if len(config_rows) != 1:
        raise ValueError("benchmark_config.csv must contain exactly one row")
    config = config_rows[0]

    numeric_page_fields = set(CLIENT_PAGE_FIELDS) - {"phase"}
    numeric_scan_fields = set(CLIENT_SCAN_FIELDS) - {"phase"}
    _integers(client_pages, numeric_page_fields)
    _integers(client_scans, numeric_scan_fields)
    _integers(server_pages, SERVER_PAGE_FIELDS)
    _integers(config_rows, set(CONFIG_FIELDS) - {"workload"})

    config_run_id = _integer(config, "benchmark_run_id")
    config_records = _integer(config, "records")
    config_page_records = _integer(config, "page_records")
    config_rounds = _integer(config, "rounds")
    config_warmups = _integer(config, "warmup_rounds")
    config_snapshot_every = _integer(config, "snapshot_every")
    config_instrument_id = _integer(config, "instrument_id")
    expected_config_workload = (
        "tick_only"
        if config_snapshot_every == 0
        else "snapshot_only"
        if config_snapshot_every == 1
        else "fixed_mixed"
    )
    if (
        config_run_id == 0
        or config_records == 0
        or config_page_records == 0
        or config_rounds == 0
        or config_instrument_id == 0
        or _integer(config, "trade_date") == 0
        or config["workload"] != expected_config_workload
    ):
        raise ValueError("benchmark_config.csv contains invalid values")

    measured_scans = [
        row for row in client_scans if row["phase"] == "measure"
    ]
    if not measured_scans:
        raise ValueError("client_scans.csv has no measure rows")
    if any(
        row["phase"] not in {"warmup", "measure"} for row in client_scans
    ) or any(
        row["phase"] not in {"warmup", "measure"} for row in client_pages
    ):
        raise ValueError("client phase must be warmup or measure")

    scan_index = _unique_index(client_scans, _scan_key, "client scan")
    page_index = _unique_index(client_pages, _page_key, "client page")
    server_index = _unique_index(
        server_pages, _page_key, "server page"
    )
    expected_phase_rounds = {
        *(("warmup", index) for index in range(config_warmups)),
        *(("measure", index) for index in range(config_rounds)),
    }
    actual_phase_rounds = {
        (row["phase"], _integer(row, "round_index"))
        for row in client_scans
    }
    scan_ids = {_integer(row, "scan_id") for row in client_scans}
    generations = {_integer(row, "generation") for row in client_scans}
    open_request_ids = {
        _integer(row, "open_request_id") for row in client_scans
    }
    read_request_ids = {
        _integer(row, "read_request_id") for row in client_pages
    }
    if (
        len(client_scans) != config_warmups + config_rounds
        or actual_phase_rounds != expected_phase_rounds
        or len(actual_phase_rounds) != len(client_scans)
        or scan_ids != set(range(config_warmups + config_rounds))
        or len(open_request_ids) != len(client_scans)
        or 0 in open_request_ids
        or len(read_request_ids) != len(client_pages)
        or 0 in read_request_ids
        or len(generations) != 1
        or 0 in generations
        or {
            _integer(row, "benchmark_run_id") for row in client_scans
        }
        != {config_run_id}
        or {
            _integer(row, "instrument_id") for row in client_scans
        }
        != {config_instrument_id}
        or {
            _integer(row, "requested_page_records")
            for row in client_scans
        }
        != {config_page_records}
    ):
        raise ValueError(
            "benchmark_config.csv does not match scan identity/configuration"
        )

    pages_by_scan: dict[
        tuple[int, int, int], list[dict[str, str]]
    ] = defaultdict(list)
    for row in client_pages:
        key = _scan_key(row)
        scan = scan_index.get(key)
        if scan is None:
            raise ValueError(f"client page has no matching scan: {_page_key(row)}")
        for field in ("scan_id", "round_index", "instrument_id"):
            if _integer(row, field) != _integer(scan, field):
                raise ValueError(
                    f"page/scan {field} mismatch for {_page_key(row)}"
                )
        if row["phase"] != scan["phase"]:
            raise ValueError(
                f"page/scan phase mismatch for {_page_key(row)}"
            )
        pages_by_scan[key].append(row)

    scan_summaries: list[dict[str, int | float | str]] = []
    matched_server_keys: set[tuple[int, int, int, int]] = set()
    for key, scan in scan_index.items():
        pages = sorted(
            pages_by_scan.get(key, []),
            key=lambda row: _integer(row, "page_index"),
        )
        if not pages:
            raise ValueError(f"scan has no client page rows: {key}")
        eof_pages = [row for row in pages if _integer(row, "eof") == 1]
        data_pages = [row for row in pages if _integer(row, "eof") == 0]
        if len(eof_pages) != 1 or pages[-1] is not eof_pages[0]:
            raise ValueError(f"scan does not end in exactly one EOF: {key}")
        if any(_integer(row, "eof") not in (0, 1) for row in pages):
            raise ValueError(f"noncanonical EOF flag in scan: {key}")
        expected_page_indices = list(range(len(pages)))
        actual_page_indices = [
            _integer(row, "page_index") for row in pages
        ]
        if actual_page_indices != expected_page_indices:
            raise ValueError(
                f"noncontiguous page indices for {key}: "
                f"{actual_page_indices!r}"
            )

        eof = eof_pages[0]
        for field in (
            "record_count",
            "snapshot_count",
            "tick_count",
            "page_mapping_bytes",
            "object_decode_ns",
            "column_build_ns",
            "first_ingress_sequence",
            "last_ingress_sequence",
            "ingress_sequence_sum",
            "ingress_sequence_xor",
        ):
            if _integer(eof, field) != 0:
                raise ValueError(f"EOF has nonzero {field}: {key}")

        server_totals = {field: 0 for field in STAGE_FIELDS}
        snapshot_count = 0
        tick_count = 0
        data_read_wall_ns = 0
        object_decode_ns = 0
        column_build_ns = 0
        page_mapping_bytes = 0
        page_ingress_sum = 0
        page_ingress_xor = 0
        page_first_ingress = 0
        page_last_ingress = 0
        running_record_count = 0
        running_source_counts = [0, 0, 0, 0]
        for page in data_pages:
            page_key = _page_key(page)
            server = server_index.get(page_key)
            if server is None:
                raise ValueError(f"missing server page timing: {page_key}")
            matched_server_keys.add(page_key)
            for field in (
                "read_request_id",
                "instrument_id",
                "record_count",
                "snapshot_count",
                "tick_count",
                "page_mapping_bytes",
            ):
                if _integer(page, field) != _integer(server, field):
                    raise ValueError(
                        f"client/server {field} mismatch for {page_key}"
                    )
            if _integer(server, "clock_read_failures") != 0:
                raise ValueError(f"server clock failure for {page_key}")
            if (
                _integer(page, "snapshot_count")
                + _integer(page, "tick_count")
                != _integer(page, "record_count")
            ):
                raise ValueError(f"payload count mismatch for {page_key}")
            record_count = _integer(page, "record_count")
            if record_count == 0 or record_count > config_page_records:
                raise ValueError(f"invalid data-page record count for {page_key}")
            expected_mapping_bytes = (
                HISTORY_PAGE_HEADER_BYTES
                + record_count * HISTORY_DESCRIPTOR_BYTES
                + _integer(page, "snapshot_count")
                * SNAPSHOT_PAYLOAD_BYTES
                + _integer(page, "tick_count") * TICK_PAYLOAD_BYTES
            )
            if (
                _integer(page, "page_mapping_bytes")
                != expected_mapping_bytes
            ):
                raise ValueError(
                    f"noncanonical data-page mapping size for {page_key}"
                )
            if _integer(page, "object_decode_ns") > _integer(
                page, "read_wall_ns"
            ):
                raise ValueError(
                    f"object decode exceeds page read wall for {page_key}"
                )
            known_build_ns = sum(
                _integer(server, field)
                for field in (
                    "cursor_read_ns",
                    "classify_layout_ns",
                    "memfd_prepare_ns",
                    "projection_ns",
                    "memfd_finalize_ns",
                )
            )
            if known_build_ns > _integer(server, "build_total_ns"):
                raise ValueError(
                    f"exclusive server stages exceed build wall for {page_key}"
                )
            for field in STAGE_FIELDS:
                server_totals[field] += _integer(server, field)
            snapshot_count += _integer(page, "snapshot_count")
            tick_count += _integer(page, "tick_count")
            data_read_wall_ns += _integer(page, "read_wall_ns")
            object_decode_ns += _integer(page, "object_decode_ns")
            column_build_ns += _integer(page, "column_build_ns")
            page_mapping_bytes += _integer(page, "page_mapping_bytes")
            current_first_ingress = _integer(
                page, "first_ingress_sequence"
            )
            current_last_ingress = _integer(
                page, "last_ingress_sequence"
            )
            expected_first_ingress = running_record_count + 1
            expected_last_ingress = running_record_count + record_count
            expected_page_sum = (
                (expected_first_ingress + expected_last_ingress)
                * record_count
                // 2
            )
            expected_page_xor = _xor_one_to(expected_last_ingress) ^ (
                _xor_one_to(expected_first_ingress - 1)
            )
            if (
                current_first_ingress != expected_first_ingress
                or current_last_ingress != expected_last_ingress
                or _integer(page, "ingress_sequence_sum")
                != expected_page_sum
                or _integer(page, "ingress_sequence_xor")
                != expected_page_xor
            ):
                raise ValueError(
                    f"data-page ingress fingerprint mismatch for {page_key}"
                )
            if page_first_ingress == 0:
                page_first_ingress = current_first_ingress
            page_last_ingress = current_last_ingress
            page_ingress_sum += _integer(
                page, "ingress_sequence_sum"
            )
            page_ingress_xor ^= _integer(
                page, "ingress_sequence_xor"
            )
            running_record_count += record_count
            running_source_counts[0] += _integer(
                page, "snapshot_count"
            )
            running_source_counts[1] += _integer(page, "tick_count")
            if (
                _integer(page, "cumulative_record_count")
                != running_record_count
            ):
                raise ValueError(
                    f"data-page cumulative record count mismatch for {page_key}"
                )
            for source in range(4):
                if _integer(
                    page, f"cumulative_source_count_{source}"
                ) != running_source_counts[source]:
                    raise ValueError(
                        "data-page cumulative source count mismatch for "
                        f"{page_key}, source={source}"
                    )

        total_record_count = snapshot_count + tick_count
        scan_expectations = {
            "data_page_read_wall_ns": data_read_wall_ns,
            "object_decode_ns": object_decode_ns,
            "column_build_ns": column_build_ns,
            "data_page_count": len(data_pages),
            "read_request_count": len(pages),
            "page_mapping_bytes": page_mapping_bytes,
            "total_record_count": total_record_count,
            "eof_read_wall_ns": _integer(eof, "read_wall_ns"),
        }
        for field, expected in scan_expectations.items():
            if _integer(scan, field) != expected:
                raise ValueError(
                    f"scan/page {field} mismatch for {key}: "
                    f"{_integer(scan, field)} != {expected}"
                )
        if (
            _integer(scan, "eof_seen") != 1
            or _integer(scan, "generation_total_record_count")
            != total_record_count
            or _integer(scan, "generation_total_record_count")
            != _integer(scan, "total_record_count")
        ):
            raise ValueError(f"generation/EOF reconciliation failed: {key}")
        semantic_expectations = {
            "first_ingress_sequence": page_first_ingress,
            "last_ingress_sequence": page_last_ingress,
            "ingress_sequence_sum": page_ingress_sum,
            "ingress_sequence_xor": page_ingress_xor,
        }
        for field, expected in semantic_expectations.items():
            if _integer(scan, field) != expected:
                raise ValueError(
                    f"scan/page semantic {field} mismatch for {key}"
                )
        if (
            _integer(eof, "cumulative_record_count")
            != total_record_count
        ):
            raise ValueError(f"EOF cumulative record count mismatch: {key}")
        for source in range(4):
            if _integer(scan, f"source_record_count_{source}") != _integer(
                scan, f"generation_source_record_count_{source}"
            ):
                raise ValueError(
                    f"source {source} generation count mismatch for {key}"
                )
            if _integer(
                eof, f"cumulative_source_count_{source}"
            ) != _integer(scan, f"source_record_count_{source}"):
                raise ValueError(
                    f"EOF cumulative source {source} mismatch for {key}"
                )

        expected_snapshots = _expected_snapshot_count(
            config_records, config_snapshot_every
        )
        expected_ticks = config_records - expected_snapshots
        expected_source_counts = (
            expected_snapshots,
            expected_ticks,
            0,
            0,
        )
        expected_fingerprint = {
            "first_ingress_sequence": 1,
            "last_ingress_sequence": config_records,
            "ingress_sequence_sum": (
                config_records * (config_records + 1) // 2
            ),
            "ingress_sequence_xor": _xor_one_to(config_records),
        }
        if (
            total_record_count != config_records
            or snapshot_count != expected_snapshots
            or tick_count != expected_ticks
            or tuple(
                _integer(scan, f"source_record_count_{source}")
                for source in range(4)
            )
            != expected_source_counts
            or any(
                _integer(scan, field) != expected
                for field, expected in expected_fingerprint.items()
            )
        ):
            raise ValueError(f"scan differs from synthetic fixture: {key}")

        memfd_ns = (
            server_totals["memfd_prepare_ns"]
            + server_totals["memfd_finalize_ns"]
        )
        requested_stage_ns = (
            memfd_ns + object_decode_ns + column_build_ns
        )
        scan_wall_ns = _integer(scan, "scan_wall_ns")
        data_pipeline_wall_ns = data_read_wall_ns + column_build_ns
        server_handling_ns = (
            server_totals["build_total_ns"]
            + server_totals["token_ns"]
            + server_totals["send_ns"]
        )
        requested_residual_full_ns = (
            scan_wall_ns - requested_stage_ns
        )
        requested_residual_data_ns = (
            data_pipeline_wall_ns - requested_stage_ns
        )
        fully_accounted_residual_ns = (
            data_pipeline_wall_ns
            - server_handling_ns
            - object_decode_ns
            - column_build_ns
        )
        if requested_stage_ns <= 0 or scan_wall_ns <= 0:
            raise ValueError(f"scan has no timed work: {key}")
        if requested_residual_full_ns < 0:
            raise ValueError(
                f"requested stages exceed full scan wall for {key}"
            )
        if requested_residual_data_ns < 0:
            raise ValueError(
                f"requested stages exceed data-pipeline wall for {key}"
            )
        sequential_minimum_ns = (
            _integer(scan, "open_wall_ns")
            + data_read_wall_ns
            + _integer(scan, "eof_read_wall_ns")
            + column_build_ns
        )
        if sequential_minimum_ns > scan_wall_ns:
            raise ValueError(
                f"exclusive client stages exceed scan wall for {key}"
            )

        if scan["phase"] != "measure":
            continue

        summary: dict[str, int | float | str] = {
            "benchmark_run_id": key[0],
            "open_request_id": key[1],
            "generation": key[2],
            "scan_id": _integer(scan, "scan_id"),
            "round_index": _integer(scan, "round_index"),
            "instrument_id": _integer(scan, "instrument_id"),
            "workload": _workload(snapshot_count, tick_count),
            "record_count": total_record_count,
            "snapshot_count": snapshot_count,
            "tick_count": tick_count,
            "page_count": len(data_pages),
            "page_mapping_bytes": page_mapping_bytes,
            "open_wall_ns": _integer(scan, "open_wall_ns"),
            "eof_read_wall_ns": _integer(scan, "eof_read_wall_ns"),
            "scan_wall_ns": scan_wall_ns,
            "data_page_read_wall_ns": data_read_wall_ns,
            "data_pipeline_wall_ns": data_pipeline_wall_ns,
            "object_decode_ns": object_decode_ns,
            "column_build_ns": column_build_ns,
            "memfd_ns": memfd_ns,
            "server_handling_ns": server_handling_ns,
            "requested_stage_ns": requested_stage_ns,
            "requested_residual_full_ns": requested_residual_full_ns,
            "requested_residual_data_ns": requested_residual_data_ns,
            "fully_accounted_residual_ns": fully_accounted_residual_ns,
            "memfd_share_full_scan": _ratio(memfd_ns, scan_wall_ns),
            "object_decode_share_full_scan": _ratio(
                object_decode_ns, scan_wall_ns
            ),
            "column_build_share_full_scan": _ratio(
                column_build_ns, scan_wall_ns
            ),
            "requested_residual_share_full_scan": _ratio(
                requested_residual_full_ns, scan_wall_ns
            ),
            "memfd_share_requested_mix": _ratio(
                memfd_ns, requested_stage_ns
            ),
            "object_decode_share_requested_mix": _ratio(
                object_decode_ns, requested_stage_ns
            ),
            "column_build_share_requested_mix": _ratio(
                column_build_ns, requested_stage_ns
            ),
        }
        summary.update(server_totals)
        scan_summaries.append(summary)

    data_page_keys = {
        key
        for key, row in page_index.items()
        if _integer(row, "eof") == 0
    }
    if set(server_index) != data_page_keys or matched_server_keys != data_page_keys:
        missing = set(server_index) - data_page_keys
        extra = data_page_keys - set(server_index)
        raise ValueError(
            "server/client data-page set mismatch: "
            f"server_only={sorted(missing)!r}, "
            f"client_only={sorted(extra)!r}"
        )

    workloads = {str(row["workload"]) for row in scan_summaries}
    if len(workloads) != 1:
        raise ValueError(
            f"one artifact directory must contain one workload: {workloads}"
        )
    record_counts = {int(row["record_count"]) for row in scan_summaries}
    if len(record_counts) != 1:
        raise ValueError(
            f"measured scans disagree on record count: {record_counts}"
        )

    if (
        {int(row["benchmark_run_id"]) for row in scan_summaries}
        != {config_run_id}
        or record_counts != {config_records}
        or len(measured_scans) != config_rounds
    ):
        raise ValueError("benchmark_config.csv does not match client scans")
    expected_workload = {
        "tick_only": "tick_only",
        "snapshot_only": "snapshot_only",
        "fixed_mixed": "mixed",
    }.get(config["workload"])
    if expected_workload is None or workloads != {expected_workload}:
        raise ValueError("benchmark_config.csv workload does not match pages")

    aggregate_fields = (
        "scan_wall_ns",
        "data_pipeline_wall_ns",
        "memfd_ns",
        "object_decode_ns",
        "column_build_ns",
        "requested_residual_full_ns",
        "requested_residual_data_ns",
        "fully_accounted_residual_ns",
        "server_handling_ns",
        *STAGE_FIELDS,
    )
    distributions = {
        field: _distribution([int(row[field]) for row in scan_summaries])
        for field in aggregate_fields
    }
    totals = {
        field: sum(int(row[field]) for row in scan_summaries)
        for field in aggregate_fields
    }
    total_requested = (
        totals["memfd_ns"]
        + totals["object_decode_ns"]
        + totals["column_build_ns"]
    )
    total_scan_wall = totals["scan_wall_ns"]
    total_records = sum(
        int(row["record_count"]) for row in scan_summaries
    )

    stage_summary = []
    for name, total_ns in (
        ("memfd", totals["memfd_ns"]),
        ("object_decode", totals["object_decode_ns"]),
        ("column_build", totals["column_build_ns"]),
    ):
        stage_summary.append(
            {
                "stage": name,
                "total_ns": total_ns,
                "ns_per_record": total_ns / total_records,
                "share_full_scan": total_ns / total_scan_wall,
                "share_requested_mix": total_ns / total_requested,
            }
        )

    scan_fields = tuple(scan_summaries[0])
    _write_csv(
        artifact_directory / "summary_scans.csv",
        scan_fields,
        scan_summaries,
    )
    _write_csv(
        artifact_directory / "summary_stages.csv",
        (
            "stage",
            "total_ns",
            "ns_per_record",
            "share_full_scan",
            "share_requested_mix",
        ),
        stage_summary,
    )

    repository_root = Path(__file__).resolve().parents[1]
    inputs = {
        path.name: {
            "bytes": path.stat().st_size,
            "sha256": _sha256(path),
        }
        for path in (
            config_path,
            client_pages_path,
            client_scans_path,
            server_pages_path,
        )
    }
    source_paths = (
        repository_root / "CMakeLists.txt",
        repository_root
        / "benchmarks"
        / "benchmark_single_instrument_history_stages.cpp",
        repository_root
        / "benchmarks"
        / "analyze_single_instrument_history_stages.py",
        repository_root
        / "tools"
        / "benchmark_instrument_history_stages.py",
        repository_root / "python" / "l2flow_realtime" / "history.py",
        repository_root / "python" / "l2flow_realtime" / "batch.py",
        repository_root
        / "include"
        / "l2flow"
        / "ipc"
        / "realtime_shared_service_v1.h",
        repository_root
        / "include"
        / "l2flow"
        / "ipc"
        / "realtime_history_wire_v1.h",
        repository_root
        / "src"
        / "ipc"
        / "realtime_shared_service_v1.cpp",
    )
    sources = {
        str(path.relative_to(repository_root)): {
            "bytes": path.stat().st_size,
            "sha256": _sha256(path),
        }
        for path in source_paths
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "benchmark": {
            "benchmark_run_id": config_run_id,
            "workload": next(iter(workloads)),
            "measured_scans": len(scan_summaries),
            "records_per_scan": next(iter(record_counts)),
            "pages_per_scan": sorted(
                {int(row["page_count"]) for row in scan_summaries}
            ),
            "requested_page_records": sorted(
                {
                    _integer(row, "requested_page_records")
                    for row in measured_scans
                }
            ),
            "snapshot_count_per_scan": sorted(
                {int(row["snapshot_count"]) for row in scan_summaries}
            ),
            "tick_count_per_scan": sorted(
                {int(row["tick_count"]) for row in scan_summaries}
            ),
            "config": {
                field: (
                    config[field]
                    if field == "workload"
                    else _integer(config, field)
                )
                for field in CONFIG_FIELDS
                if field != "schema_version"
            },
        },
        "definitions": {
            "memfd_ns": "memfd_prepare_ns + memfd_finalize_ns",
            "object_decode_ns": (
                "exclusive Python descriptor/payload validation, payload "
                "bytes copy, Snapshot/Tick parsing, and HistoryRecord creation"
            ),
            "column_build_ns": (
                "exclusive _history_columns_by_kind Python list construction; "
                "Arrow/Polars materialization excluded"
            ),
            "share_full_scan": (
                "stage total / open-through-explicit-EOF scan wall; "
                "server stages and object decode are nested inside read wall"
            ),
            "share_requested_mix": (
                "stage / (memfd + object_decode + column_build)"
            ),
            "requested_residual_data_ns": (
                "data-page read wall + column build - memfd - object decode "
                "- column build; these requested stages are non-overlapping "
                "subintervals, so a negative value invalidates the run"
            ),
            "fully_accounted_residual_ns": (
                "data-page read wall + column build - complete measured "
                "server handling - object decode - column build; this is a "
                "duration-sum diagnostic, not a strict wall-time partition, "
                "because sendmsg and recvmsg tails can overlap"
            ),
            "distribution_quantiles": (
                "p50/p90/p95/p99 use Hyndman-Fan type 7 (R-7) linear "
                "interpolation across measured scans only; warmup scans are "
                "fully validated but excluded"
            ),
            "small_sample_warning": (
                "percentiles from few measured scans are descriptive "
                "interpolations, not evidence of production tail-latency "
                "confidence; increase --rounds for distribution work"
            ),
        },
        "aggregate": {
            "total_records": total_records,
            "total_scan_wall_ns": total_scan_wall,
            "total_requested_stage_ns": total_requested,
            "stages": stage_summary,
            "requested_residual_full_ns": (
                total_scan_wall - total_requested
            ),
            "requested_residual_share_full_scan": (
                (total_scan_wall - total_requested) / total_scan_wall
            ),
        },
        "distributions": distributions,
        "scans": scan_summaries,
        "inputs": inputs,
        "sources": sources,
        "environment": {
            "platform": platform.platform(),
            "python": sys.version,
            "cpu_count": os.cpu_count(),
            "git_branch": _git(
                ("rev-parse", "--abbrev-ref", "HEAD"), repository_root
            ),
            "git_head": _git(("rev-parse", "HEAD"), repository_root),
            "git_status_porcelain": _git(
                ("status", "--porcelain"), repository_root
            ),
        },
    }


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    artifact_directory = args.artifact_directory.resolve()
    output = (
        args.output.resolve()
        if args.output is not None
        else artifact_directory / "analysis.json"
    )
    try:
        result = analyze(artifact_directory)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(
            json.dumps(result, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    except (OSError, ValueError) as error:
        print(f"benchmark analysis failed: {error}", file=sys.stderr)
        return 1

    aggregate = result["aggregate"]
    print(
        json.dumps(
            {
                "analysis": str(output),
                "workload": result["benchmark"]["workload"],
                "measured_scans": result["benchmark"]["measured_scans"],
                "records_per_scan": result["benchmark"][
                    "records_per_scan"
                ],
                "stages": aggregate["stages"],
                "requested_residual_share_full_scan": aggregate[
                    "requested_residual_share_full_scan"
                ],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
