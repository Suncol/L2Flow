#!/usr/bin/env python3
"""Benchmark Python stages of one instrument's complete Store history read.

The service must already be running.  Every cursor pins one real immutable
generation and is consumed through the public ``open_history_cursor`` API up to
its explicit zero-row EOF response.

``client_pages.csv`` contains one row per READ request, including EOF.
``client_scans.csv`` contains one row per complete cursor scan.  The
``open_request_id``/``read_request_id`` and generation/page identity columns
are intended to join with server-side history-stage measurements.
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence


_UINT32_MAX = (1 << 32) - 1
_UINT64_MAX = (1 << 64) - 1
_CSV_SCHEMA_VERSION = 1

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


def _positive_uint32(value: str) -> int:
    try:
        parsed = int(value, 10)
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            "value must be a positive uint32"
        ) from error
    if parsed <= 0 or parsed > _UINT32_MAX:
        raise argparse.ArgumentTypeError("value must be a positive uint32")
    return parsed


def _positive_uint64(value: str) -> int:
    try:
        parsed = int(value, 10)
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            "value must be a positive uint64"
        ) from error
    if parsed <= 0 or parsed > _UINT64_MAX:
        raise argparse.ArgumentTypeError("value must be a positive uint64")
    return parsed


def _positive_int(value: str) -> int:
    try:
        parsed = int(value, 10)
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            "value must be a positive integer"
        ) from error
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be a positive integer")
    return parsed


def _nonnegative_int(value: str) -> int:
    try:
        parsed = int(value, 10)
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            "value must be a nonnegative integer"
        ) from error
    if parsed < 0:
        raise argparse.ArgumentTypeError(
            "value must be a nonnegative integer"
        )
    return parsed


def _positive_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            "value must be a positive number"
        ) from error
    if not parsed > 0:
        raise argparse.ArgumentTypeError("value must be a positive number")
    return parsed


def _parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "benchmark object decode and Python column construction while "
            "reading one instrument's complete immutable Store history"
        )
    )
    parser.add_argument("control_socket")
    parser.add_argument("source_python", type=Path)
    parser.add_argument("output_dir", type=Path)
    parser.add_argument(
        "--instrument-id", required=True, type=_positive_uint32
    )
    parser.add_argument(
        "--benchmark-run-id", required=True, type=_positive_uint64
    )
    parser.add_argument(
        "--page-records", type=_positive_int, default=4096
    )
    parser.add_argument("--rounds", type=_positive_int, default=5)
    parser.add_argument(
        "--warmup-rounds",
        "--warmups",
        dest="warmup_rounds",
        type=_nonnegative_int,
        default=1,
    )
    parser.add_argument(
        "--expected-records",
        "--records",
        dest="expected_records",
        type=_nonnegative_int,
    )
    parser.add_argument("--timeout", type=_positive_float, default=30.0)
    parsed = parser.parse_args(argv)
    if parsed.page_records > 1024 * 1024:
        parser.error("--page-records exceeds the Store absolute batch limit")
    return parsed


@dataclass(frozen=True, slots=True)
class _DecodeMeasurement:
    elapsed_ns: int
    page_index: int
    record_count: int
    snapshot_count: int
    tick_count: int
    page_mapping_bytes: int


class _RequestIdProbe:
    def __init__(self, original):
        self._original = original
        self._pending: int | None = None

    def begin(self) -> None:
        if self._pending is not None:
            raise RuntimeError("unconsumed history request ID")

    def __call__(self, value=None) -> int:
        request_id = self._original(value)
        if self._pending is not None:
            raise RuntimeError("multiple request IDs in one benchmark operation")
        self._pending = request_id
        return request_id

    def take(self) -> int:
        if self._pending is None:
            raise RuntimeError("history operation did not allocate a request ID")
        request_id = self._pending
        self._pending = None
        return request_id


class _ObjectDecodeProbe:
    def __init__(self, original):
        self._original = original
        self._pending: _DecodeMeasurement | None = None

    def begin(self) -> None:
        if self._pending is not None:
            raise RuntimeError("unconsumed object-decode measurement")

    def __call__(self, data, **kwargs):
        layout = kwargs["layout"]
        started_ns = time.perf_counter_ns()
        decoded = self._original(data, **kwargs)
        elapsed_ns = time.perf_counter_ns() - started_ns
        if self._pending is not None:
            raise RuntimeError("multiple object decodes in one history read")
        self._pending = _DecodeMeasurement(
            elapsed_ns=elapsed_ns,
            page_index=layout.page_index,
            record_count=layout.record_count,
            snapshot_count=layout.snapshot_count,
            tick_count=layout.tick_count,
            page_mapping_bytes=layout.total_bytes,
        )
        return decoded

    def take_data_page(self) -> _DecodeMeasurement:
        if self._pending is None:
            raise RuntimeError("history data page was not object-decoded")
        result = self._pending
        self._pending = None
        return result

    def require_eof(self) -> None:
        if self._pending is not None:
            raise RuntimeError("history EOF unexpectedly decoded page objects")


def _write_csv(
    path: Path,
    fieldnames: Sequence[str],
    rows: Sequence[Mapping[str, Any]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
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
        ) as output:
            temporary_name = output.name
            writer = csv.DictWriter(
                output,
                fieldnames=fieldnames,
                extrasaction="raise",
            )
            writer.writeheader()
            writer.writerows(rows)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary_name, path)
        temporary_name = None
    finally:
        if temporary_name is not None:
            try:
                os.unlink(temporary_name)
            except FileNotFoundError:
                pass


def _validate_columns(
    built,
    records,
    generation,
    page_index: int,
) -> tuple[int, int]:
    if set(built) != {"snapshots", "ticks"}:
        raise RuntimeError("history columns returned unexpected table kinds")
    merged_ingress = []
    snapshot_count = 0
    tick_count = 0
    for kind in ("snapshots", "ticks"):
        columns, _types, selected = built[kind]
        count = len(selected)
        if kind == "snapshots":
            snapshot_count = count
        else:
            tick_count = count
        if len(columns["ingress_sequence"]) != count:
            raise RuntimeError("history column row count is inconsistent")
        if columns["projection_flags"] != [
            record.projection_flags for record in selected
        ]:
            raise RuntimeError(
                "history column projection flags differ from records"
            )
        expected_constants = {
            "store_generation": generation.generation,
            "registry_version": generation.registry_version,
            "registry_sha256": generation.registry_sha256,
            "input_identity_sha256": generation.input_identity_sha256,
            "payload_projection": generation.payload_projection,
            "history_page_index": page_index,
            "record_coverage_complete": int(
                generation.record_coverage_complete
            ),
            "field_complete": int(generation.field_complete),
            "coverage_from_open": int(generation.coverage_from_open),
        }
        for name, expected in expected_constants.items():
            if columns[name] != [expected] * count:
                raise RuntimeError(
                    f"history column {name} differs from page metadata"
                )
        merged_ingress.extend(columns["ingress_sequence"])
    if snapshot_count + tick_count != len(records):
        raise RuntimeError("history columns did not cover every page record")
    if sorted(merged_ingress) != [
        record.ingress_sequence for record in records
    ]:
        raise RuntimeError(
            "history columns cannot reconstruct mixed ingress order"
        )
    return snapshot_count, tick_count


def _run_scan(
    *,
    history_module,
    batch_module,
    request_probe: _RequestIdProbe,
    decode_probe: _ObjectDecodeProbe,
    benchmark_run_id: int,
    scan_id: int,
    phase: str,
    round_index: int,
    control_socket: str,
    instrument_id: int,
    requested_page_records: int,
    timeout: float,
    expected_records: int | None,
) -> tuple[list[dict[str, Any]], dict[str, Any], tuple[Any, ...]]:
    page_rows: list[dict[str, Any]] = []
    cursor = None
    scan_started_ns = time.perf_counter_ns()
    request_probe.begin()
    open_started_ns = time.perf_counter_ns()
    try:
        cursor = history_module.open_history_cursor(
            control_socket,
            instrument_id,
            requested_page_records=requested_page_records,
            timeout=timeout,
        )
        open_wall_ns = time.perf_counter_ns() - open_started_ns
        open_request_id = request_probe.take()
        generation = cursor.generation
        if (
            expected_records is not None
            and generation.total_record_count != expected_records
        ):
            raise RuntimeError(
                "pinned generation contains "
                f"{generation.total_record_count} records; expected "
                f"{expected_records}"
            )

        data_page_read_wall_ns = 0
        eof_read_wall_ns = 0
        object_decode_ns = 0
        column_build_ns = 0
        data_page_count = 0
        read_request_count = 0
        page_mapping_bytes = 0
        total_record_count = 0
        source_record_counts = [0, 0, 0, 0]
        first_ingress_sequence = 0
        last_ingress_sequence = 0
        ingress_sequence_sum = 0
        ingress_sequence_xor = 0
        eof_seen = False
        eof_completed_ns = 0

        while not cursor.done:
            request_probe.begin()
            decode_probe.begin()
            read_started_ns = time.perf_counter_ns()
            page = cursor.read()
            read_completed_ns = time.perf_counter_ns()
            read_wall_ns = read_completed_ns - read_started_ns
            read_request_id = request_probe.take()
            read_request_count += 1

            if page.eof:
                decode_probe.require_eof()
                eof_read_wall_ns += read_wall_ns
                eof_seen = True
                eof_completed_ns = read_completed_ns
                page_rows.append(
                    {
                        "schema_version": _CSV_SCHEMA_VERSION,
                        "benchmark_run_id": benchmark_run_id,
                        "scan_id": scan_id,
                        "phase": phase,
                        "round_index": round_index,
                        "open_request_id": open_request_id,
                        "read_request_id": read_request_id,
                        "generation": generation.generation,
                        "instrument_id": instrument_id,
                        "page_index": page.page_index,
                        "eof": 1,
                        "record_count": 0,
                        "snapshot_count": 0,
                        "tick_count": 0,
                        "page_mapping_bytes": 0,
                        "read_wall_ns": read_wall_ns,
                        "object_decode_ns": 0,
                        "column_build_ns": 0,
                        "cumulative_record_count": (
                            page.cumulative_record_count
                        ),
                        "cumulative_source_count_0": (
                            page.cumulative_source_record_counts[0]
                        ),
                        "cumulative_source_count_1": (
                            page.cumulative_source_record_counts[1]
                        ),
                        "cumulative_source_count_2": (
                            page.cumulative_source_record_counts[2]
                        ),
                        "cumulative_source_count_3": (
                            page.cumulative_source_record_counts[3]
                        ),
                        "first_ingress_sequence": 0,
                        "last_ingress_sequence": 0,
                        "ingress_sequence_sum": 0,
                        "ingress_sequence_xor": 0,
                    }
                )
                break

            decode = decode_probe.take_data_page()
            if (
                decode.page_index != page.page_index
                or decode.record_count != len(page.records)
            ):
                raise RuntimeError(
                    "object-decode measurement does not match history page"
                )

            column_started_ns = time.perf_counter_ns()
            built = batch_module._history_columns_by_kind(
                page.records,
                page.generation,
                page.page_index,
            )
            page_column_build_ns = (
                time.perf_counter_ns() - column_started_ns
            )
            snapshot_count, tick_count = _validate_columns(
                built,
                page.records,
                generation,
                page.page_index,
            )
            if (
                snapshot_count != decode.snapshot_count
                or tick_count != decode.tick_count
            ):
                raise RuntimeError(
                    "decoded payload counts differ from constructed columns"
                )

            page_ingress_sum = 0
            page_ingress_xor = 0
            page_source_counts = [0, 0, 0, 0]
            for record in page.records:
                ingress = record.ingress_sequence
                page_ingress_sum += ingress
                page_ingress_xor ^= ingress
                page_source_counts[record.source_slot] += 1
            page_first_ingress = page.records[0].ingress_sequence
            page_last_ingress = page.records[-1].ingress_sequence
            if first_ingress_sequence == 0:
                first_ingress_sequence = page_first_ingress
            last_ingress_sequence = page_last_ingress
            ingress_sequence_sum += page_ingress_sum
            ingress_sequence_xor ^= page_ingress_xor
            total_record_count += len(page.records)
            for source_slot, count in enumerate(page_source_counts):
                source_record_counts[source_slot] += count

            data_page_count += 1
            data_page_read_wall_ns += read_wall_ns
            object_decode_ns += decode.elapsed_ns
            column_build_ns += page_column_build_ns
            page_mapping_bytes += decode.page_mapping_bytes
            page_rows.append(
                {
                    "schema_version": _CSV_SCHEMA_VERSION,
                    "benchmark_run_id": benchmark_run_id,
                    "scan_id": scan_id,
                    "phase": phase,
                    "round_index": round_index,
                    "open_request_id": open_request_id,
                    "read_request_id": read_request_id,
                    "generation": generation.generation,
                    "instrument_id": instrument_id,
                    "page_index": page.page_index,
                    "eof": 0,
                    "record_count": len(page.records),
                    "snapshot_count": snapshot_count,
                    "tick_count": tick_count,
                    "page_mapping_bytes": decode.page_mapping_bytes,
                    "read_wall_ns": read_wall_ns,
                    "object_decode_ns": decode.elapsed_ns,
                    "column_build_ns": page_column_build_ns,
                    "cumulative_record_count": (
                        page.cumulative_record_count
                    ),
                    "cumulative_source_count_0": (
                        page.cumulative_source_record_counts[0]
                    ),
                    "cumulative_source_count_1": (
                        page.cumulative_source_record_counts[1]
                    ),
                    "cumulative_source_count_2": (
                        page.cumulative_source_record_counts[2]
                    ),
                    "cumulative_source_count_3": (
                        page.cumulative_source_record_counts[3]
                    ),
                    "first_ingress_sequence": page_first_ingress,
                    "last_ingress_sequence": page_last_ingress,
                    "ingress_sequence_sum": page_ingress_sum,
                    "ingress_sequence_xor": page_ingress_xor,
                }
            )
            del built

        scan_wall_ns = eof_completed_ns - scan_started_ns
        if not eof_seen or not cursor.done or not cursor.closed:
            raise RuntimeError("history scan did not finish at explicit EOF")
        if total_record_count != generation.total_record_count:
            raise RuntimeError(
                "history scan count differs from pinned generation"
            )
        if tuple(source_record_counts) != generation.source_record_counts:
            raise RuntimeError(
                "history scan source counts differ from pinned generation"
            )
        scan_row = {
            "schema_version": _CSV_SCHEMA_VERSION,
            "benchmark_run_id": benchmark_run_id,
            "scan_id": scan_id,
            "phase": phase,
            "round_index": round_index,
            "open_request_id": open_request_id,
            "generation": generation.generation,
            "instrument_id": instrument_id,
            "requested_page_records": requested_page_records,
            "open_wall_ns": open_wall_ns,
            "scan_wall_ns": scan_wall_ns,
            "data_page_read_wall_ns": data_page_read_wall_ns,
            "eof_read_wall_ns": eof_read_wall_ns,
            "object_decode_ns": object_decode_ns,
            "column_build_ns": column_build_ns,
            "data_page_count": data_page_count,
            "read_request_count": read_request_count,
            "page_mapping_bytes": page_mapping_bytes,
            "total_record_count": total_record_count,
            "source_record_count_0": source_record_counts[0],
            "source_record_count_1": source_record_counts[1],
            "source_record_count_2": source_record_counts[2],
            "source_record_count_3": source_record_counts[3],
            "generation_total_record_count": (
                generation.total_record_count
            ),
            "generation_source_record_count_0": (
                generation.source_record_counts[0]
            ),
            "generation_source_record_count_1": (
                generation.source_record_counts[1]
            ),
            "generation_source_record_count_2": (
                generation.source_record_counts[2]
            ),
            "generation_source_record_count_3": (
                generation.source_record_counts[3]
            ),
            "first_ingress_sequence": first_ingress_sequence,
            "last_ingress_sequence": last_ingress_sequence,
            "ingress_sequence_sum": ingress_sequence_sum,
            "ingress_sequence_xor": ingress_sequence_xor,
            "eof_seen": 1,
        }
        generation_key = (
            generation.run_id,
            generation.session_epoch,
            generation.generation,
            generation.instrument_id,
        )
        generation_signature = (
            generation_key,
            total_record_count,
            tuple(source_record_counts),
            first_ingress_sequence,
            last_ingress_sequence,
            ingress_sequence_sum,
            ingress_sequence_xor,
        )
        return page_rows, scan_row, generation_signature
    finally:
        if cursor is not None:
            cursor.close()


def _load_runtime(source_python: Path):
    source_python = source_python.resolve()
    if not source_python.is_dir():
        raise RuntimeError(
            f"Python source directory does not exist: {source_python}"
        )
    sys.path.insert(0, str(source_python))
    from l2flow_realtime import batch as batch_module
    from l2flow_realtime import history as history_module

    return history_module, batch_module


def run(args: argparse.Namespace) -> tuple[Path, Path]:
    history_module, batch_module = _load_runtime(args.source_python)
    request_probe = _RequestIdProbe(history_module._request_id)
    decode_probe = _ObjectDecodeProbe(
        history_module._decode_page_objects
    )
    original_request_id = history_module._request_id
    original_decode = history_module._decode_page_objects
    benchmark_run_id = args.benchmark_run_id
    page_rows: list[dict[str, Any]] = []
    scan_rows: list[dict[str, Any]] = []
    generation_signatures: dict[tuple[Any, ...], tuple[Any, ...]] = {}
    try:
        history_module._request_id = request_probe
        history_module._decode_page_objects = decode_probe
        phases = (
            ("warmup", args.warmup_rounds),
            ("measure", args.rounds),
        )
        scan_id = 0
        for phase, count in phases:
            for round_index in range(count):
                pages, scan, signature = _run_scan(
                    history_module=history_module,
                    batch_module=batch_module,
                    request_probe=request_probe,
                    decode_probe=decode_probe,
                    benchmark_run_id=benchmark_run_id,
                    scan_id=scan_id,
                    phase=phase,
                    round_index=round_index,
                    control_socket=args.control_socket,
                    instrument_id=args.instrument_id,
                    requested_page_records=args.page_records,
                    timeout=args.timeout,
                    expected_records=args.expected_records,
                )
                generation_key = signature[0]
                prior = generation_signatures.setdefault(
                    generation_key, signature
                )
                if prior != signature:
                    raise RuntimeError(
                        "repeated scan of one pinned generation is "
                        "semantically inconsistent"
                    )
                page_rows.extend(pages)
                scan_rows.append(scan)
                scan_id += 1
    finally:
        history_module._decode_page_objects = original_decode
        history_module._request_id = original_request_id

    page_path = args.output_dir / "client_pages.csv"
    scan_path = args.output_dir / "client_scans.csv"
    _write_csv(page_path, CLIENT_PAGE_FIELDS, page_rows)
    _write_csv(scan_path, CLIENT_SCAN_FIELDS, scan_rows)
    return page_path, scan_path


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    page_path, scan_path = run(args)
    print(page_path)
    print(scan_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
