"""Process-level latency/correctness probe for one valid Wire V2 instrument ID."""

from __future__ import annotations

import os
import sys
import time


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def main(argv: list[str]) -> int:
    if len(argv) != 6:
        raise RuntimeError(
            "usage: realtime_hot_read_probe.py "
            "CONTROL_SOCKET NATIVE_READER SOURCE_PYTHON INSTRUMENT_ID ITERATIONS"
        )
    control_socket, native_reader, source_python, raw_id, raw_iterations = (
        argv[1:]
    )
    instrument_id = int(raw_id, 10)
    iterations = int(raw_iterations, 10)
    _require(os.path.isabs(control_socket), "control socket must be absolute")
    _require(os.path.isfile(native_reader), "native reader is missing")
    _require(os.path.isdir(source_python), "Python source directory is missing")
    _require(instrument_id > 0, "instrument ID must be positive")
    _require(iterations > 0, "iterations must be positive")
    sys.path.insert(0, source_python)

    from l2flow_realtime import (  # pylint: disable=import-outside-toplevel
        L2FlowClient,
        LatestStatus,
    )

    with L2FlowClient.connect(
        control_socket,
        native_library=native_reader,
        timeout=2.0,
    ) as client:
        before = client.session_info()
        first = client.latest_snapshot(instrument_id)
        _require(
            first.status is LatestStatus.AVAILABLE,
            "probe instrument has no available snapshot",
        )
        begin = time.perf_counter_ns()
        for _ in range(iterations):
            snapshot = client.latest_snapshot(instrument_id)
            _require(
                snapshot.status is LatestStatus.AVAILABLE
                and snapshot.common.instrument_id == instrument_id,
                "known-ID latest read returned inconsistent data",
            )
        elapsed = time.perf_counter_ns() - begin
        after = client.session_info()
        _require(before.identity == after.identity, "session identity changed")
        _require(
            before.catalog_generation == after.catalog_generation
            and before.catalog_digest == after.catalog_digest,
            "known-ID reads changed or refreshed the catalog",
        )
        print(
            "python known-id latest snapshot "
            f"mean_ns={elapsed // iterations} iterations={iterations}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
