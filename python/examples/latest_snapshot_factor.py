#!/usr/bin/env python3
"""Read latest snapshots and use last_price_p6 as a placeholder factor."""

from __future__ import annotations

import argparse
import sys
import time
from typing import Sequence

import polars as pl

from l2flow_realtime import L2FlowClient, LatestStatus, ServerState


_UINT32_MAX = 0xFFFFFFFF


def _instrument_ids(value: str) -> tuple[int, ...]:
    result = []
    for item in value.split(","):
        try:
            instrument_id = int(item, 10)
        except ValueError as error:
            raise argparse.ArgumentTypeError(
                f"invalid instrument id {item!r}"
            ) from error
        if instrument_id <= 0 or instrument_id > _UINT32_MAX:
            raise argparse.ArgumentTypeError(
                "instrument ids must be positive uint32 values"
            )
        result.append(instrument_id)
    if not result:
        raise argparse.ArgumentTypeError(
            "at least one instrument id is required"
        )
    return tuple(result)


def calculate_placeholder_factor(
    batch, read_monotonic_ns: int
) -> pl.DataFrame:
    """Return a Polars frame with latest price copied into a factor column."""

    return (
        batch.to_polars()
        .with_columns(
            pl.when(
                pl.col("status") == int(LatestStatus.AVAILABLE)
            )
            .then(pl.col("last_price_p6"))
            .otherwise(None)
            .alias("placeholder_factor_p6"),
            pl.lit(read_monotonic_ns, dtype=pl.Int64).alias(
                "factor_read_monotonic_ns"
            ),
        )
        .with_columns(
            (
                pl.col("factor_read_monotonic_ns")
                - pl.col("recv_monotonic_ns")
            ).alias("snapshot_age_ns")
        )
        .select(
            "instrument_id",
            "status",
            "ingress_sequence",
            "event_time_unix_ns",
            "recv_monotonic_ns",
            "factor_read_monotonic_ns",
            "snapshot_age_ns",
            "last_price_p6",
            "placeholder_factor_p6",
        )
    )


def _parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Read latest snapshots from L2Flow and copy last_price_p6 "
            "into a placeholder Polars factor."
        )
    )
    parser.add_argument(
        "--control-socket",
        required=True,
        help="absolute path to the router Unix-domain control socket",
    )
    parser.add_argument(
        "--native-reader",
        help=(
            "path to libl2flow_shm_reader.so; otherwise use "
            "L2FLOW_SHM_READER_LIBRARY or native reader discovery"
        ),
    )
    parser.add_argument(
        "--instrument-ids",
        required=True,
        type=_instrument_ids,
        help="comma-separated registry instrument_id values",
    )
    parser.add_argument(
        "--interval-ms",
        type=int,
        default=100,
        help="poll interval in milliseconds; default 100",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=1,
        help="number of reads; 0 runs until interrupted, default 1",
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
            args.control_socket,
            **connect_options,
        ) as client:
            session = client.session_info()
            print(
                "connected"
                f" run_id={session.run_id.hex()}"
                f" session_epoch={session.session_epoch}"
                f" registry_version={session.registry_version}",
                file=sys.stderr,
            )
            completed = 0
            while args.iterations == 0 or completed < args.iterations:
                batch = client.get_latest_snapshots(
                    args.instrument_ids
                )
                frame = calculate_placeholder_factor(
                    batch, time.monotonic_ns()
                )
                print(frame)
                completed += 1
                if (
                    client.session_info().server_state
                    is ServerState.STOPPED_CLEAN
                ):
                    break
                if (
                    args.iterations == 0
                    or completed < args.iterations
                ):
                    time.sleep(args.interval_ms / 1000.0)
    except KeyboardInterrupt:
        return 130
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
