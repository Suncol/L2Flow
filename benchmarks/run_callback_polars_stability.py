#!/usr/bin/env python3
"""Run isolated callback throughput trials and preserve parseable evidence."""

from __future__ import annotations

import argparse
import csv
import json
import re
import subprocess
from pathlib import Path


DEFAULT_RATES = (50_000, 100_000, 200_000, 300_000, 500_000)
RESULT_PREFIX = "THROUGHPUT_RESULT "
FATAL_RE = re.compile(
    r"first fatal reason=(?P<reason>\S+) source_slot=(?P<source>\d+) "
    r"ingress_sequence=(?P<sequence>\d+) detail=(?P<detail>\d+)"
)


def _fields(line: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for token in line.split()[1:]:
        key, separator, value = token.partition("=")
        if separator:
            result[key] = value
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--duration-ms", type=int, default=1_000)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--cpu-list", default="8-15")
    parser.add_argument(
        "--rates", type=int, nargs="+", default=list(DEFAULT_RATES)
    )
    return parser


def main() -> int:
    args = _parser().parse_args()
    if args.duration_ms <= 0 or args.repeats <= 0:
        raise SystemExit("duration and repeats must be positive")
    binary = args.binary.resolve()
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, object]] = []

    for rate in args.rates:
        for repeat in range(1, args.repeats + 1):
            command = [
                "taskset",
                "-c",
                args.cpu_list,
                str(binary),
                "--throughput-stability-benchmark",
                str(rate),
                str(args.duration_ms),
            ]
            completed = subprocess.run(
                command,
                check=False,
                capture_output=True,
                text=True,
            )
            log_text = completed.stdout + completed.stderr
            log_path = output_dir / f"throughput_{rate}_run{repeat}.log"
            log_path.write_text(log_text, encoding="utf-8")
            result_lines = [
                line
                for line in completed.stdout.splitlines()
                if line.startswith(RESULT_PREFIX)
            ]
            parsed: dict[str, object] = {
                "target_rps": rate,
                "repeat": repeat,
                "duration_ms": args.duration_ms,
                "exit_code": completed.returncode,
                "terminated_by_signal": (
                    -completed.returncode if completed.returncode < 0 else 0
                ),
                "result_line_count": len(result_lines),
                "log": log_path.name,
            }
            if len(result_lines) == 1:
                parsed.update(_fields(result_lines[0]))
            fatal_match = FATAL_RE.search(log_text)
            parsed.update(
                {
                    "fatal_reason": (
                        fatal_match.group("reason") if fatal_match else ""
                    ),
                    "fatal_source_slot": (
                        int(fatal_match.group("source")) if fatal_match else -1
                    ),
                    "fatal_ingress_sequence": (
                        int(fatal_match.group("sequence")) if fatal_match else 0
                    ),
                    "fatal_detail": (
                        int(fatal_match.group("detail")) if fatal_match else 0
                    ),
                }
            )
            rows.append(parsed)
            print(
                f"rate={rate} repeat={repeat} exit={completed.returncode} "
                f"target_met={parsed.get('target_met', 'missing')} "
                f"fatal={parsed.get('fatal_reason', '') or 'none'}"
            )

    json_path = output_dir / "throughput_trials.json"
    json_path.write_text(
        json.dumps(rows, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    keys = sorted({key for row in rows for key in row})
    with (output_dir / "throughput_trials.csv").open(
        "w", newline="", encoding="utf-8"
    ) as output:
        writer = csv.DictWriter(output, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)

    valid = all(
        row["exit_code"] == 0 and row["result_line_count"] == 1 for row in rows
    )
    return 0 if valid else 1


if __name__ == "__main__":
    raise SystemExit(main())
