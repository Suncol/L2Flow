#!/usr/bin/env python3
"""Stream-compare a Tick-only mdl-sdk-feeder-probe capture with backups.

The capture can contain tens of millions of records.  This implementation
therefore keeps only per-stream counters, a compact SeqNo bitmap, and one
current backup row in memory.  Every capture row is compared; this is not a
sample.  Each growing backup file is read through a fixed byte-size snapshot,
and an incomplete trailing line is ignored.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator


@dataclass(frozen=True)
class StreamSpec:
    backup_name: str
    fields: tuple[tuple[str, str], ...]


STREAM_SPECS: dict[str, StreamSpec] = {
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
        description="Stream-compare every normalized feeder capture row with msg_backup"
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
        help="retry the complete comparison once after this delay if rows are missing",
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


def snapshot_lines(path: Path, size: int) -> Iterable[str]:
    """Yield complete UTF-8 CSV lines from exactly the first ``size`` bytes."""
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


def snapshot_csv(
    path: Path,
) -> tuple[list[str], Iterator[dict[str, str]], dict[str, object]]:
    stat_result = path.stat()
    complete = False
    if stat_result.st_size > 0:
        with path.open("rb") as source:
            source.seek(stat_result.st_size - 1)
            complete = source.read(1) == b"\n"
    reader = csv.DictReader(snapshot_lines(path, stat_result.st_size))
    if reader.fieldnames is None:
        raise ValueError(f"backup CSV has no complete header: {path}")
    metadata: dict[str, object] = {
        "snapshot_size_bytes": stat_result.st_size,
        "snapshot_mtime_ns": stat_result.st_mtime_ns,
        "incomplete_trailing_line_ignored": not complete,
    }
    return reader.fieldnames, reader, metadata


class BackupCursor:
    """Forward-only cursor over a fixed snapshot of one ordered backup CSV."""

    def __init__(self, path: Path, spec: StreamSpec) -> None:
        fieldnames, rows, metadata = snapshot_csv(path)
        required = {"SeqNo", *(backup for _, backup in spec.fields)}
        missing = sorted(required.difference(fieldnames))
        if missing:
            raise ValueError(f"backup CSV {path} is missing columns: {missing}")
        self.path = path
        self.metadata = metadata
        self._rows = rows
        self._line_number = 1
        self._previous_sequence: int | None = None
        self.current: tuple[int, int, dict[str, str]] | None = None
        self.invalid_seqno_rows_scanned = 0
        self.non_increasing_seqno_pairs_scanned = 0
        self._advance()

    def _advance(self) -> None:
        while True:
            try:
                row = next(self._rows)
            except StopIteration:
                self.current = None
                return
            self._line_number += 1
            raw_sequence = row.get("SeqNo", "")
            try:
                sequence = int(raw_sequence)
            except ValueError:
                self.invalid_seqno_rows_scanned += 1
                continue
            if (
                self._previous_sequence is not None
                and sequence <= self._previous_sequence
            ):
                self.non_increasing_seqno_pairs_scanned += 1
            self._previous_sequence = sequence
            self.current = (sequence, self._line_number, row)
            return

    def find(
        self, sequence: int
    ) -> tuple[int, dict[str, str], int] | None:
        while self.current is not None and self.current[0] < sequence:
            self._advance()
        if self.current is None or self.current[0] != sequence:
            return None
        _, line_number, row = self.current
        duplicate_matches = 0
        self._advance()
        while self.current is not None and self.current[0] == sequence:
            duplicate_matches += 1
            self._advance()
        return line_number, row, duplicate_matches


class StreamComparison:
    """Incremental counters and exact mapped-field comparison for one stream."""

    _MAX_BITMAP_BYTES = 32 * 1024 * 1024

    def __init__(
        self,
        key: str,
        spec: StreamSpec,
        backup_path: Path,
        cursor: BackupCursor | None,
        max_examples: int,
    ) -> None:
        self.key = key
        self.spec = spec
        self.backup_path = backup_path
        self.cursor = cursor
        self.max_examples = max_examples
        self.capture_rows = 0
        self.capture_first_seqno = ""
        self.capture_last_seqno = ""
        self.capture_first_local_time = ""
        self.capture_last_local_time = ""
        self.invalid_capture_seqnos = 0
        self.duplicate_capture_seqnos = 0
        self.capture_non_increasing_pairs = 0
        self.capture_sequence_gap_events = 0
        self.capture_sequence_gap_count = 0
        self.compared_rows = 0
        self.missing_rows = 0
        self.differing_rows = 0
        self.field_differences = 0
        self.duplicate_backup_matches = 0
        self.backup_order_violations = 0
        self.missing_seqno_examples: list[str] = []
        self.difference_examples: list[dict[str, object]] = []
        self._previous_capture_sequence: int | None = None
        self._last_matched_backup_line: int | None = None
        self._seen_base: int | None = None
        self._seen_bits = bytearray()
        self._seen_sparse: set[int] = set()

    def _is_duplicate(self, sequence: int) -> bool:
        if self._seen_base is None:
            self._seen_base = sequence
        offset = sequence - self._seen_base
        if offset < 0:
            duplicate = sequence in self._seen_sparse
            self._seen_sparse.add(sequence)
            return duplicate
        byte_index = offset >> 3
        if byte_index >= self._MAX_BITMAP_BYTES:
            duplicate = sequence in self._seen_sparse
            self._seen_sparse.add(sequence)
            return duplicate
        if byte_index >= len(self._seen_bits):
            self._seen_bits.extend(b"\0" * (byte_index + 1 - len(self._seen_bits)))
        mask = 1 << (offset & 7)
        duplicate = bool(self._seen_bits[byte_index] & mask)
        self._seen_bits[byte_index] |= mask
        return duplicate

    def _record_missing(self, raw_sequence: str) -> None:
        self.missing_rows += 1
        if len(self.missing_seqno_examples) < self.max_examples:
            self.missing_seqno_examples.append(raw_sequence)

    def observe(self, row: dict[str, str]) -> None:
        raw_sequence = row["SeqNo"]
        self.capture_rows += 1
        if self.capture_rows == 1:
            self.capture_first_seqno = raw_sequence
            self.capture_first_local_time = row.get("LocalTime", "")
        self.capture_last_seqno = raw_sequence
        self.capture_last_local_time = row.get("LocalTime", "")

        try:
            sequence = int(raw_sequence)
        except ValueError:
            self.invalid_capture_seqnos += 1
            self._record_missing(raw_sequence)
            return

        if self._is_duplicate(sequence):
            self.duplicate_capture_seqnos += 1
        previous = self._previous_capture_sequence
        if previous is not None:
            if sequence <= previous:
                self.capture_non_increasing_pairs += 1
            elif sequence > previous + 1:
                self.capture_sequence_gap_events += 1
                self.capture_sequence_gap_count += sequence - previous - 1
        self._previous_capture_sequence = sequence

        if self.cursor is None:
            self._record_missing(raw_sequence)
            return
        match = self.cursor.find(sequence)
        if match is None:
            self._record_missing(raw_sequence)
            return
        line_number, backup_row, duplicate_matches = match
        self.duplicate_backup_matches += duplicate_matches
        if (
            self._last_matched_backup_line is not None
            and line_number <= self._last_matched_backup_line
        ):
            self.backup_order_violations += 1
        self._last_matched_backup_line = line_number
        self.compared_rows += 1

        row_differences: list[dict[str, str]] = []
        for capture_field, backup_field in self.spec.fields:
            capture_value = row[capture_field]
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
            self.differing_rows += 1
            self.field_differences += len(row_differences)
            if len(self.difference_examples) < self.max_examples:
                self.difference_examples.append(
                    {
                        "seqno": raw_sequence,
                        "capture_index": row.get("CaptureIndex", ""),
                        "backup_line": line_number,
                        "differences": row_differences,
                    }
                )

    def result(self) -> dict[str, object]:
        backup_exists = self.cursor is not None
        success = (
            self.capture_rows > 0
            and backup_exists
            and self.invalid_capture_seqnos == 0
            and self.duplicate_capture_seqnos == 0
            and self.capture_non_increasing_pairs == 0
            and self.capture_sequence_gap_count == 0
            and self.missing_rows == 0
            and self.duplicate_backup_matches == 0
            and self.differing_rows == 0
            and self.backup_order_violations == 0
        )
        if success:
            verdict = "IDENTICAL_FOR_ALL_MAPPED_FIELDS"
        elif self.capture_rows == 0:
            verdict = "NO_CAPTURE_ROWS"
        elif not backup_exists:
            verdict = "BACKUP_FILE_MISSING"
        else:
            verdict = "MISMATCH"
        result: dict[str, object] = {
            "service_key": self.key,
            "result": verdict,
            "capture_rows": self.capture_rows,
            "capture_first_seqno": self.capture_first_seqno,
            "capture_last_seqno": self.capture_last_seqno,
            "capture_first_local_time": self.capture_first_local_time,
            "capture_last_local_time": self.capture_last_local_time,
            "mapped_fields_per_match": len(self.spec.fields),
            "compared_rows": self.compared_rows,
            "missing_rows": self.missing_rows,
            "differing_rows": self.differing_rows,
            "field_differences": self.field_differences,
            "duplicate_capture_seqnos": self.duplicate_capture_seqnos,
            "invalid_capture_seqnos": self.invalid_capture_seqnos,
            "capture_non_increasing_pairs": self.capture_non_increasing_pairs,
            "capture_sequence_gap_events": self.capture_sequence_gap_events,
            "capture_sequence_gap_count": self.capture_sequence_gap_count,
            "duplicate_backup_matches": self.duplicate_backup_matches,
            "backup_order_violations": self.backup_order_violations,
            "backup_file": str(self.backup_path),
            "missing_seqno_examples": self.missing_seqno_examples,
            "difference_examples": self.difference_examples,
        }
        if self.cursor is not None:
            result.update(self.cursor.metadata)
            result["backup_invalid_seqno_rows_scanned"] = (
                self.cursor.invalid_seqno_rows_scanned
            )
            result["backup_non_increasing_seqno_pairs_scanned"] = (
                self.cursor.non_increasing_seqno_pairs_scanned
            )
        return result


def capture_has_complete_tail(path: Path) -> bool:
    size = path.stat().st_size
    if size == 0:
        return False
    with path.open("rb") as source:
        source.seek(size - 1)
        return source.read(1) == b"\n"


def compare_once(args: argparse.Namespace) -> list[dict[str, object]]:
    backup_day = args.backup_root / args.date
    comparisons: dict[str, StreamComparison] = {}
    for key, spec in STREAM_SPECS.items():
        backup_path = backup_day / spec.backup_name
        cursor = BackupCursor(backup_path, spec) if backup_path.is_file() else None
        comparisons[key] = StreamComparison(
            key, spec, backup_path, cursor, args.max_examples
        )

    with args.capture.open("r", encoding="utf-8", newline="") as source:
        reader = csv.DictReader(source)
        if reader.fieldnames is None:
            raise ValueError("capture CSV has no header")
        required_capture_fields = {
            "ServiceID",
            "ServiceVersion",
            "MessageID",
            "SeqNo",
            *(capture for spec in STREAM_SPECS.values() for capture, _ in spec.fields),
        }
        missing = sorted(required_capture_fields.difference(reader.fieldnames))
        if missing:
            raise ValueError(f"capture CSV is missing columns: {missing}")
        for row in reader:
            key = stream_key(row)
            comparison = comparisons.get(key)
            if comparison is None:
                raise ValueError(f"capture contains unsupported service key: {key}")
            comparison.observe(row)
    return [comparisons[key].result() for key in STREAM_SPECS]


def build_report(args: argparse.Namespace) -> dict[str, object]:
    results = compare_once(args)
    retried_for_online_tail = False
    if args.tail_wait_seconds > 0 and any(result["missing_rows"] for result in results):
        time.sleep(args.tail_wait_seconds)
        results = compare_once(args)
        retried_for_online_tail = True
    success = all(
        result["result"] == "IDENTICAL_FOR_ALL_MAPPED_FIELDS"
        for result in results
    )
    capture_stat = args.capture.stat()
    return {
        "schema_version": 2,
        "capture_file": str(args.capture.resolve()),
        "capture_size_bytes": capture_stat.st_size,
        "capture_mtime_ns": capture_stat.st_mtime_ns,
        "capture_complete_trailing_line": capture_has_complete_tail(args.capture),
        "backup_root": str(args.backup_root.resolve()),
        "date": args.date,
        "comparison_identity": "service-key plus SeqNo",
        "comparison_scope": "every capture row; all mapped normalized fields",
        "online_snapshot_semantics": (
            "fixed backup byte size per stream; incomplete trailing line ignored"
        ),
        "retried_for_online_tail": retried_for_online_tail,
        "success": success,
        "required_streams": len(STREAM_SPECS),
        "streams_with_capture_rows": sum(result["capture_rows"] > 0 for result in results),
        "capture_rows": sum(result["capture_rows"] for result in results),
        "compared_rows": sum(result["compared_rows"] for result in results),
        "missing_rows": sum(result["missing_rows"] for result in results),
        "differing_rows": sum(result["differing_rows"] for result in results),
        "field_differences": sum(result["field_differences"] for result in results),
        "duplicate_backup_matches": sum(
            result["duplicate_backup_matches"] for result in results
        ),
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
        "invalid_capture_seqnos",
        "duplicate_capture_seqnos",
        "capture_non_increasing_pairs",
        "capture_sequence_gap_events",
        "capture_sequence_gap_count",
        "duplicate_backup_matches",
        "backup_order_violations",
        "snapshot_size_bytes",
        "snapshot_mtime_ns",
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
