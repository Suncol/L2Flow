"""Process-level smoke for the public Python derived-history reader."""

from __future__ import annotations

import os
import sys


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def main(argv: list[str]) -> int:
    if len(argv) != 6:
        raise RuntimeError(
            "usage: derived_event_history_probe.py "
            "CONTROL_SOCKET NATIVE_READER SOURCE_PYTHON "
            "INSTRUMENT_ID GENERATION"
        )
    control_socket, native_reader, source_python, raw_id, raw_generation = (
        argv[1:]
    )
    instrument_id = int(raw_id, 10)
    generation = int(raw_generation, 10)
    _require(os.path.isabs(control_socket), "control socket must be absolute")
    _require(os.path.isfile(native_reader), "native reader is missing")
    _require(os.path.isdir(source_python), "Python source directory is missing")
    _require(instrument_id > 0, "instrument ID must be positive")
    _require(generation > 0, "generation must be positive")
    sys.path.insert(0, source_python)

    from l2flow_realtime import (  # pylint: disable=import-outside-toplevel
        InstrumentDerivedEventKind,
        L2FlowClient,
    )

    with L2FlowClient.connect(
        control_socket,
        native_library=native_reader,
        timeout=2.0,
    ) as client:
        with client.open_instrument_derived_event_history(
            instrument_id,
            maximum_order_states=100,
            page_records=1,
        ) as history:
            with history.read_all(
                expected_generation=generation
            ) as initial:
                events = tuple(
                    event
                    for batch in initial.batches()
                    for event in batch
                )
                checkpoint = initial.verified_checkpoint

            trades = tuple(
                event
                for event in events
                if event.event_kind is InstrumentDerivedEventKind.TRADE
            )
            orders = tuple(
                event
                for event in events
                if event.event_kind
                is InstrumentDerivedEventKind.ORDER_REVISION
            )
            _require(len(events) == 3, "derived full replay row count differs")
            _require(
                len(trades) == 1 and trades[0].quantity == 101,
                "derived trade projection differs",
            )
            _require(
                len(orders) == 2
                and [event.revision for event in orders] == [1, 2],
                "derived order revisions are not T then A",
            )
            exact = orders[-1]
            _require(
                exact.order_id == 11_001
                and exact.original_quantity_valid
                and exact.original_quantity == 151
                and exact.source_matched_quantity_valid
                and exact.source_matched_quantity == 101
                and exact.observed_pre_add_trade_quantity == 101
                and exact.remaining_quantity_valid
                and exact.remaining_quantity == 50
                and exact.apply_to_book,
                "derived A-backed exact order state differs",
            )
            _require(
                checkpoint.derived_event_sequence_exclusive == 4
                and checkpoint.order_state_count == 1,
                "derived EOF checkpoint differs",
            )
            with history.read_updates(
                checkpoint,
                expected_generation=generation,
            ) as empty:
                _require(
                    tuple(empty.batches()) == (),
                    "same-generation update is not empty",
                )
                next_checkpoint = empty.verified_checkpoint
            _require(
                next_checkpoint.derived_event_sequence_exclusive
                == checkpoint.derived_event_sequence_exclusive,
                "empty update advanced derived sequence",
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
