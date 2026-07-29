#!/usr/bin/env python3
"""Read known observed-universe instrument IDs through the Wire V2 hot path."""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections.abc import Sequence

from l2flow_realtime import L2FlowClient, LatestStatus, ServerState


def _instrument_ids(value: str) -> tuple[int, ...]:
    try:
        result = tuple(int(item, 10) for item in value.split(","))
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            "instrument IDs must be comma-separated decimal integers"
        ) from error
    if not result or any(
        instrument_id <= 0 or instrument_id > 0xFFFFFFFF
        for instrument_id in result
    ):
        raise argparse.ArgumentTypeError(
            "instrument IDs must be positive uint32 values"
        )
    return result


def _parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Read latest snapshots by session-scoped instrument ID. "
            "This path never resolves keys or scans the observed catalog."
        )
    )
    parser.add_argument("--control-socket", required=True)
    parser.add_argument("--instrument-ids", required=True, type=_instrument_ids)
    parser.add_argument("--native-reader")
    parser.add_argument("--interval-ms", type=int, default=100)
    parser.add_argument(
        "--iterations",
        type=int,
        default=1,
        help="zero continues until the service stops or the process is interrupted",
    )
    args = parser.parse_args(argv)
    if args.interval_ms < 0:
        parser.error("--interval-ms must be nonnegative")
    if args.iterations < 0:
        parser.error("--iterations must be nonnegative")
    return args


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    connect_options = {}
    if args.native_reader is not None:
        connect_options["native_library"] = args.native_reader

    try:
        with L2FlowClient.connect(
            args.control_socket, **connect_options
        ) as client:
            completed = 0
            while args.iterations == 0 or completed < args.iterations:
                for snapshot in client.latest_snapshots(args.instrument_ids):
                    row = {
                        "session_epoch": snapshot.session_epoch,
                        "instrument_id": snapshot.instrument_id,
                        "status": snapshot.status.name,
                    }
                    if snapshot.status is LatestStatus.AVAILABLE:
                        row.update(
                            ingress_sequence=snapshot.common.ingress_sequence,
                            recv_monotonic_ns=snapshot.common.recv_monotonic_ns,
                            last_price_raw=snapshot.last_price.raw,
                            last_price_normalized_p6=(
                                snapshot.last_price.normalized_p6
                            ),
                            last_price_valid=snapshot.last_price.valid,
                            last_price_is_null=snapshot.last_price.is_null,
                        )
                    print(json.dumps(row, separators=(",", ":")))
                completed += 1
                session = client.session_info()
                if session.server_state is ServerState.STOPPED_CLEAN:
                    break
                if args.iterations == 0 or completed < args.iterations:
                    time.sleep(args.interval_ms / 1000.0)
    except KeyboardInterrupt:
        return 130
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
