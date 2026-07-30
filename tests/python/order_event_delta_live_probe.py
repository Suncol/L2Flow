#!/usr/bin/env python3
"""End-to-end probe for an inherited event-delta ring O_RDONLY fd."""

from __future__ import annotations

import argparse
import json

from l2flow_realtime.order_event_delta_live import (
    LiveOrderEventDeltaProducerState,
    LiveOrderEventDeltaReader,
    LiveOrderEventDeltaSession,
)


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--library", required=True)
    parser.add_argument("--fd", required=True, type=int)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--session-epoch", required=True, type=int)
    parser.add_argument("--trade-date", required=True, type=int)
    parser.add_argument("--capacity", required=True, type=int)
    parser.add_argument("--mapping-bytes", required=True, type=int)
    parser.add_argument("--expected-rows", required=True, type=int)
    parser.add_argument("--expected-source-tick", required=True, type=int)
    parser.add_argument("--expected-heartbeat", required=True, type=int)
    parser.add_argument("--expected-state", required=True, type=int)
    return parser.parse_args()


def main() -> int:
    arguments = _arguments()
    session = LiveOrderEventDeltaSession(
        run_id=bytes.fromhex(arguments.run_id),
        session_epoch=arguments.session_epoch,
        trade_date=arguments.trade_date,
        ring_capacity=arguments.capacity,
        total_mapping_bytes=arguments.mapping_bytes,
    )
    sequences: list[int] = []
    order_ids: list[int] = []
    with LiveOrderEventDeltaReader.open_library(
        arguments.library,
        arguments.fd,
        session,
        batch_records=1,
        take_fd_ownership=True,
    ) as reader:
        initial = reader.poll()
        if (
            initial.consumed_source_tick_sequence
            != arguments.expected_source_tick
            or initial.heartbeat_monotonic_ns
            != arguments.expected_heartbeat
        ):
            raise RuntimeError("initial producer progress mismatch")
        for batch in reader.read_available(
            maximum_batches=arguments.expected_rows + 1
        ):
            if len(batch.buffer) != len(batch) * 320:
                raise RuntimeError("unstable batch byte extent")
            for index in range(len(batch)):
                row = batch.row(index)
                sequences.append(row.derived_event_sequence)
                order_ids.append(row.order_id)
        final = reader.poll()
        state = reader.producer_state()
        if state != LiveOrderEventDeltaProducerState(
            arguments.expected_state
        ):
            raise RuntimeError("producer state mismatch")
        if (
            len(sequences) != arguments.expected_rows
            or sequences != list(range(1, len(sequences) + 1))
            or final.next_sequence != len(sequences) + 1
            or final.records_written != 0
        ):
            raise RuntimeError("dense event stream mismatch")

    print(
        json.dumps(
            {
                "rows": len(sequences),
                "sequences": sequences,
                "order_ids": order_ids,
                "source_tick": final.consumed_source_tick_sequence,
                "heartbeat": final.heartbeat_monotonic_ns,
                "state": int(state),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
