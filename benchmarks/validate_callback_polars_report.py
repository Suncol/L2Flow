#!/usr/bin/env python3
"""Execute report source SQL and compare it with the packaged snapshot rows."""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path


SOURCE_DATASETS = {
    "benchmark_summary": "headline_metrics",
    "latency_evidence": "latency_runs",
    "throughput_evidence": "throughput_rates",
}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("artifact", type=Path)
    args = parser.parse_args()
    artifact = json.loads(args.artifact.read_text(encoding="utf-8"))
    sources = {
        source["id"]: source for source in artifact["manifest"]["sources"]
    }
    datasets = artifact["snapshot"]["datasets"]
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    try:
        for source_id, dataset_id in SOURCE_DATASETS.items():
            query = sources[source_id]["query"]["sql"]
            actual = [dict(row) for row in connection.execute(query)]
            expected = datasets[dataset_id]
            if actual != expected:
                raise RuntimeError(
                    f"source SQL mismatch for {source_id}/{dataset_id}: "
                    f"actual={actual!r} expected={expected!r}"
                )
            print(
                f"REPORT_SOURCE_OK source={source_id} "
                f"dataset={dataset_id} rows={len(actual)}"
            )
    finally:
        connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
