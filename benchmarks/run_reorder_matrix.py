#!/usr/bin/env python3
"""Run ordered, sparse-disorder, heavy-disorder, and earliest-late trials."""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--records", type=int, default=100_000)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    scenarios = (
        ("ordered", 0, False),
        ("disorder_0_01_percent", 1, False),
        ("disorder_1_percent", 100, False),
        ("disorder_10_percent", 1_000, False),
        ("earliest_late", 0, True),
    )
    rows: list[dict[str, object]] = []
    for name, basis_points, earliest in scenarios:
        command = [
            str(args.binary),
            "--records",
            str(args.records),
            "--disorder-bps",
            str(basis_points),
        ]
        if earliest:
            command.append("--earliest-late")
        completed = subprocess.run(
            command,
            check=True,
            text=True,
            capture_output=True,
        )
        row = json.loads(completed.stdout)
        row["scenario"] = name
        rows.append(row)

    text = json.dumps(rows, indent=2, sort_keys=True) + "\n"
    if args.output is None:
        print(text, end="")
    else:
        args.output.write_text(text, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
