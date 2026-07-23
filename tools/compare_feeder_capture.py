#!/usr/bin/env python3
"""Compare mdl-sdk-feeder-probe CSV output with feeder msg_backup CSVs.

The probe intentionally writes only the normalized fields needed by L2Flow.
This tool uses SeqNo as the per-service identity and compares every normalized
field that has a corresponding column in the feeder's primary (``*_0.csv``)
backup file.  Backup files may still be growing: each comparison reads only
the byte size observed when that file is opened and ignores an incomplete
trailing line.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


@dataclass(frozen=True)
class StreamSpec:
    backup_name: str
    fields: tuple[tuple[str, str], ...]


STREAM_SPECS: dict[str, StreamSpec] = {
    "4.101.4": StreamSpec(
        "mdl_4_4_0.csv",
        (
            ("EventTime", "UpdateTime"),
            ("SecurityID", "SecurityID"),
            ("EventType", "InstruStatus"),
            ("TradeCount", "TradNumber"),
            ("Volume", "TradVolume"),
            ("Turnover", "Turnover"),
            ("PreClosePrice", "PreCloPrice"),
            ("OpenPrice", "OpenPrice"),
            ("HighPrice", "HighPrice"),
            ("LowPrice", "LowPrice"),
            ("LastPrice", "LastPrice"),
            ("TotalBidQuantity", "TotalBidVol"),
            ("WeightedAverageBidPrice", "WAvgBidPri"),
            ("TotalOfferQuantity", "TotalAskVol"),
            ("WeightedAverageOfferPrice", "WAvgAskPri"),
            ("LocalTime", "LocalTime"),
        ),
    ),
    "4.101.24": StreamSpec(
        "mdl_4_24_0.csv",
        (
            ("ApplicationSequence", "BizIndex"),
            ("ChannelNo", "Channel"),
            ("SecurityID", "SecurityID"),
            ("EventTime", "TickTime"),
            ("EventType", "Type"),
            ("BidApplicationSequence", "BuyOrderNO"),
            ("OfferApplicationSequence", "SellOrderNO"),
            ("Price", "Price"),
            ("Quantity", "Qty"),
            ("Turnover", "TradeMoney"),
            ("TickBSFlag", "TickBSFlag"),
            ("LocalTime", "LocalTime"),
        ),
    ),
    "6.101.28": StreamSpec(
        "mdl_6_28_0.csv",
        (
            ("EventTime", "UpdateTime"),
            ("MDStreamID", "MDStreamID"),
            ("SecurityID", "SecurityID"),
            ("SecurityIDSource", "SecurityIDSource"),
            ("TradingPhaseCode", "TradingPhaseCode"),
            ("TradeCount", "TurnNum"),
            ("Volume", "Volume"),
            ("Turnover", "Turnover"),
            ("PreClosePrice", "PreCloPrice"),
            ("OpenPrice", "OpenPrice"),
            ("HighPrice", "HighPrice"),
            ("LowPrice", "LowPrice"),
            ("LastPrice", "LastPrice"),
            ("TotalBidQuantity", "TotalBidQty"),
            ("WeightedAverageBidPrice", "WeightedAvgBidPx"),
            ("TotalOfferQuantity", "TotalOfferQty"),
            ("WeightedAverageOfferPrice", "WeightedAvgOfferPx"),
            ("LocalTime", "LocalTime"),
        ),
    ),
    "6.101.33": StreamSpec(
        "mdl_6_33_0.csv",
        (
            ("ChannelNo", "ChannelNo"),
            ("ApplicationSequence", "ApplSeqNum"),
            ("MDStreamID", "MDStreamID"),
            ("SecurityID", "SecurityID"),
            ("SecurityIDSource", "SecurityIDSource"),
            ("Price", "Price"),
            ("Quantity", "OrderQty"),
            ("Side", "Side"),
            ("EventTime", "TransactTime"),
            ("OrderType", "OrdType"),
            ("LocalTime", "LocalTime"),
        ),
    ),
    "6.101.36": StreamSpec(
        "mdl_6_36_0.csv",
        (
            ("ChannelNo", "ChannelNo"),
            ("ApplicationSequence", "ApplSeqNum"),
            ("MDStreamID", "MDStreamID"),
            ("BidApplicationSequence", "BidApplSeqNum"),
            ("OfferApplicationSequence", "OfferApplSeqNum"),
            ("SecurityID", "SecurityID"),
            ("SecurityIDSource", "SecurityIDSource"),
            ("Price", "LastPx"),
            ("Quantity", "LastQty"),
            ("ExecutionType", "ExecType"),
            ("EventTime", "TransactTime"),
            ("LocalTime", "LocalTime"),
        ),
    ),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare a normalized feeder capture with msg_backup"
    )
    parser.add_argument("capture", type=Path)
    parser.add_argument("--backup-root", type=Path, required=True)
    parser.add_argument("--date", required=True, help="YYYYMMDD backup directory")
    parser.add_argument("--report-json", type=Path)
    parser.add_argument("--report-csv", type=Path)
    parser.add_argument("--max-examples", type=int, default=10)
    parser.add_argument(
        "--tail-wait-seconds",
        type=float,
        default=0.0,
        help="retry once after this delay if captured SeqNo values are missing",
    )
    args = parser.parse_args()
    if args.max_examples < 0:
        parser.error("--max-examples must be non-negative")
    if args.tail_wait_seconds < 0:
        parser.error("--tail-wait-seconds must be non-negative")
    if len(args.date) != 8 or not args.date.isdigit():
        parser.error("--date must be YYYYMMDD")
    return args


def stream_key(row: dict[str, str]) -> str:
    return ".".join((row["ServiceID"], row["ServiceVersion"], row["MessageID"]))


def load_capture(path: Path) -> tuple[list[str], dict[str, list[dict[str, str]]]]:
    with path.open("r", encoding="utf-8", newline="") as source:
        reader = csv.DictReader(source)
        if reader.fieldnames is None:
            raise ValueError("capture CSV has no header")
        required = {"ServiceID", "ServiceVersion", "MessageID", "SeqNo"}
        missing = sorted(required.difference(reader.fieldnames))
        if missing:
            raise ValueError(f"capture CSV is missing columns: {missing}")
        rows_by_stream: dict[str, list[dict[str, str]]] = defaultdict(list)
        order: list[str] = []
        for row in reader:
            key = stream_key(row)
            if key not in rows_by_stream:
                order.append(key)
            rows_by_stream[key].append(row)
    if not rows_by_stream:
        raise ValueError("capture CSV has no data rows")
    return order, rows_by_stream


def snapshot_lines(path: Path, size: int) -> Iterable[str]:
    """Yield complete UTF-8 CSV lines from a fixed-size file snapshot."""
    with path.open("rb") as source:
        remaining = size
        while remaining > 0:
            line = source.readline(remaining)
            if not line:
                break
            remaining -= len(line)
            if not line.endswith(b"\n"):
                break
            yield line.decode("utf-8")


def snapshot_csv(path: Path) -> tuple[list[str], Iterable[dict[str, str]], dict[str, object]]:
    stat_result = path.stat()
    complete = False
    if stat_result.st_size > 0:
        with path.open("rb") as source:
            source.seek(stat_result.st_size - 1)
            complete = source.read(1) == b"\n"
    reader = csv.DictReader(snapshot_lines(path, stat_result.st_size))
    if reader.fieldnames is None:
        raise ValueError(f"backup CSV has no complete header: {path}")
    metadata = {
        "snapshot_size_bytes": stat_result.st_size,
        "snapshot_mtime_ns": stat_result.st_mtime_ns,
        "incomplete_trailing_line_ignored": not complete,
    }
    return reader.fieldnames, reader, metadata


def compare_stream(
    key: str,
    capture_rows: list[dict[str, str]],
    backup_path: Path,
    spec: StreamSpec,
    max_examples: int,
) -> dict[str, object]:
    wanted = Counter(row["SeqNo"] for row in capture_rows)
    duplicate_capture_seqnos = sum(count - 1 for count in wanted.values() if count > 1)
    capture_sequence_values: list[int] = []
    invalid_capture_seqnos = 0
    for row in capture_rows:
        try:
            capture_sequence_values.append(int(row["SeqNo"]))
        except ValueError:
            invalid_capture_seqnos += 1
    non_increasing_pairs = sum(
        right <= left
        for left, right in zip(capture_sequence_values, capture_sequence_values[1:])
    )
    sequence_gap_count = sum(
        max(0, right - left - 1)
        for left, right in zip(capture_sequence_values, capture_sequence_values[1:])
    )

    fieldnames, backup_rows, metadata = snapshot_csv(backup_path)
    required_backup_fields = {"SeqNo", *(backup for _, backup in spec.fields)}
    missing_backup_columns = sorted(required_backup_fields.difference(fieldnames))
    if missing_backup_columns:
        raise ValueError(
            f"backup CSV {backup_path} is missing columns: {missing_backup_columns}"
        )
    required_capture_fields = {capture for capture, _ in spec.fields}
    missing_capture_columns = sorted(required_capture_fields.difference(capture_rows[0]))
    if missing_capture_columns:
        raise ValueError(
            f"capture CSV is missing columns for {key}: {missing_capture_columns}"
        )

    found: dict[str, tuple[int, dict[str, str]]] = {}
    duplicate_backup_matches = 0
    for line_number, backup_row in enumerate(backup_rows, start=2):
        sequence = backup_row.get("SeqNo", "")
        if sequence not in wanted:
            continue
        if sequence in found:
            duplicate_backup_matches += 1
            continue
        found[sequence] = (line_number, backup_row)

    missing_seqnos: list[str] = []
    differing_rows = 0
    field_differences = 0
    difference_examples: list[dict[str, object]] = []
    matched_line_numbers: list[int] = []
    compared_rows = 0
    for capture_row in capture_rows:
        sequence = capture_row["SeqNo"]
        match = found.get(sequence)
        if match is None:
            missing_seqnos.append(sequence)
            continue
        line_number, backup_row = match
        matched_line_numbers.append(line_number)
        compared_rows += 1
        row_differences = []
        for capture_field, backup_field in spec.fields:
            capture_value = capture_row[capture_field]
            backup_value = backup_row[backup_field]
            if capture_value != backup_value:
                row_differences.append(
                    {
                        "capture_field": capture_field,
                        "backup_field": backup_field,
                        "capture_value": capture_value,
                        "backup_value": backup_value,
                    }
                )
        if row_differences:
            differing_rows += 1
            field_differences += len(row_differences)
            if len(difference_examples) < max_examples:
                difference_examples.append(
                    {
                        "seqno": sequence,
                        "capture_index": capture_row.get("CaptureIndex", ""),
                        "backup_line": line_number,
                        "differences": row_differences,
                    }
                )

    backup_order_violations = sum(
        right <= left
        for left, right in zip(matched_line_numbers, matched_line_numbers[1:])
    )
    success = (
        invalid_capture_seqnos == 0
        and duplicate_capture_seqnos == 0
        and non_increasing_pairs == 0
        and sequence_gap_count == 0
        and not missing_seqnos
        and duplicate_backup_matches == 0
        and differing_rows == 0
        and backup_order_violations == 0
    )
    result: dict[str, object] = {
        "service_key": key,
        "result": "IDENTICAL_FOR_ALL_MAPPED_FIELDS" if success else "MISMATCH",
        "capture_rows": len(capture_rows),
        "capture_first_seqno": capture_rows[0]["SeqNo"],
        "capture_last_seqno": capture_rows[-1]["SeqNo"],
        "capture_first_local_time": capture_rows[0].get("LocalTime", ""),
        "capture_last_local_time": capture_rows[-1].get("LocalTime", ""),
        "mapped_fields_per_match": len(spec.fields),
        "compared_rows": compared_rows,
        "missing_rows": len(missing_seqnos),
        "differing_rows": differing_rows,
        "field_differences": field_differences,
        "duplicate_capture_seqnos": duplicate_capture_seqnos,
        "invalid_capture_seqnos": invalid_capture_seqnos,
        "capture_non_increasing_pairs": non_increasing_pairs,
        "capture_sequence_gap_count": sequence_gap_count,
        "duplicate_backup_matches": duplicate_backup_matches,
        "backup_order_violations": backup_order_violations,
        "backup_file": str(backup_path),
        **metadata,
        "missing_seqno_examples": missing_seqnos[:max_examples],
        "difference_examples": difference_examples,
    }
    return result


def build_report(args: argparse.Namespace) -> dict[str, object]:
    order, rows_by_stream = load_capture(args.capture)
    unknown_streams = [key for key in order if key not in STREAM_SPECS]
    if unknown_streams:
        raise ValueError(f"capture contains unsupported service keys: {unknown_streams}")

    backup_day = args.backup_root / args.date

    def run_once() -> list[dict[str, object]]:
        results = []
        for key in order:
            spec = STREAM_SPECS[key]
            backup_path = backup_day / spec.backup_name
            if not backup_path.is_file():
                results.append(
                    {
                        "service_key": key,
                        "result": "BACKUP_FILE_MISSING",
                        "capture_rows": len(rows_by_stream[key]),
                        "backup_file": str(backup_path),
                        "missing_rows": len(rows_by_stream[key]),
                        "differing_rows": 0,
                        "field_differences": 0,
                        "mapped_fields_per_match": len(spec.fields),
                    }
                )
                continue
            results.append(
                compare_stream(
                    key,
                    rows_by_stream[key],
                    backup_path,
                    spec,
                    args.max_examples,
                )
            )
        return results

    results = run_once()
    retried_for_online_tail = False
    if args.tail_wait_seconds > 0 and any(result["missing_rows"] for result in results):
        time.sleep(args.tail_wait_seconds)
        results = run_once()
        retried_for_online_tail = True

    success = all(
        result["result"] == "IDENTICAL_FOR_ALL_MAPPED_FIELDS"
        for result in results
    )
    return {
        "schema_version": 1,
        "capture_file": str(args.capture.resolve()),
        "backup_root": str(args.backup_root.resolve()),
        "date": args.date,
        "comparison_identity": "service-key plus SeqNo",
        "online_snapshot_semantics": (
            "fixed backup byte size per stream; incomplete trailing line ignored"
        ),
        "retried_for_online_tail": retried_for_online_tail,
        "success": success,
        "capture_rows": sum(len(rows) for rows in rows_by_stream.values()),
        "streams": results,
    }


def write_json(path: Path, report: dict[str, object]) -> None:
    with path.open("x", encoding="utf-8") as output:
        json.dump(report, output, ensure_ascii=False, indent=2, sort_keys=True)
        output.write("\n")


def write_csv(path: Path, report: dict[str, object]) -> None:
    columns = (
        "service_key",
        "capture_rows",
        "capture_first_seqno",
        "capture_last_seqno",
        "capture_first_local_time",
        "capture_last_local_time",
        "backup_file",
        "compared_rows",
        "missing_rows",
        "mapped_fields_per_match",
        "differing_rows",
        "field_differences",
        "capture_sequence_gap_count",
        "duplicate_capture_seqnos",
        "duplicate_backup_matches",
        "backup_order_violations",
        "result",
    )
    with path.open("x", encoding="utf-8", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(report["streams"])


def main() -> int:
    args = parse_args()
    try:
        report = build_report(args)
        if args.report_json is not None:
            write_json(args.report_json, report)
        if args.report_csv is not None:
            write_csv(args.report_csv, report)
    except (OSError, UnicodeError, ValueError, csv.Error) as error:
        print(f"compare-feeder-capture: {error}", file=sys.stderr)
        return 2
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0 if report["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
