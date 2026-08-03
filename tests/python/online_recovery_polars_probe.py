"""One-shot recovered History to eager Polars latency probe."""

from __future__ import annotations

import os
import sys
import time


def _decimal(text: str, name: str) -> int:
    if not text.isascii() or not text.isdecimal():
        raise RuntimeError(f"{name} must be an unsigned decimal integer")
    value = int(text, 10)
    if value <= 0:
        raise RuntimeError(f"{name} must be positive")
    return value


def _emit(**fields: object) -> None:
    encoded = []
    for name, value in fields.items():
        text = str(value)
        if not text or any(character.isspace() for character in text):
            raise RuntimeError(f"invalid output field {name}")
        encoded.append(f"{name}={text}")
    print("ONLINE_RECOVERY_POLARS_RESULT " + " ".join(encoded), flush=True)


def main(argv: list[str]) -> int:
    if len(argv) != 7:
        raise RuntimeError(
            "usage: online_recovery_polars_probe.py CONTROL_SOCKET "
            "NATIVE_LIBRARY SOURCE_PYTHON GENERATION EXPECTED_RECORDS "
            "INSTRUMENT_ID"
        )
    control_socket, native_library, source_python = argv[1:4]
    generation = _decimal(argv[4], "generation")
    expected_records = _decimal(argv[5], "expected_records")
    instrument_id = _decimal(argv[6], "instrument_id")
    if not os.path.isabs(control_socket):
        raise RuntimeError("control socket path must be absolute")
    if not os.path.isfile(native_library):
        raise RuntimeError("native reader library does not exist")
    if not os.path.isdir(source_python):
        raise RuntimeError("Python source directory does not exist")
    sys.path.insert(0, source_python)

    import polars as pl  # pylint: disable=import-outside-toplevel

    from l2flow_realtime import (  # pylint: disable=import-outside-toplevel
        INSTRUMENT_RAW_EVENT_COLUMNS,
        L2FlowClient,
    )
    from l2flow_realtime.polars import (  # pylint: disable=import-outside-toplevel
        raw_event_batch_frame,
    )

    start_ns = time.clock_gettime_ns(time.CLOCK_MONOTONIC)
    frames: list[pl.DataFrame] = []
    batch_count = 0
    with L2FlowClient.connect(
        control_socket,
        native_library=native_library,
        timeout=10.0,
        stale_after_ns=None,
    ) as client:
        session = client.session_info()
        if session.server_state.name != "ACTIVE" or not session.coverage_from_open:
            raise RuntimeError("recovered service is not ACTIVE/from-open")
        raw_history = client.open_instrument_raw_event_history(
            raw_event_columns=INSTRUMENT_RAW_EVENT_COLUMNS,
            ring_slots=4,
            batch_capacity=4_096,
        )
        with raw_history.read_all(
            instrument_id,
            batch_records=4_096,
            expected_generation=generation,
        ) as cursor:
            open_return_ns = time.clock_gettime_ns(time.CLOCK_MONOTONIC)
            for batch in cursor.batches():
                batch_count += 1
                frames.append(
                    raw_event_batch_frame(
                        batch,
                        columns=raw_history.raw_event_columns,
                    )
                )
            checkpoint = cursor.verified_checkpoint

    if not frames:
        raise RuntimeError("recovered History returned no records")
    frame = (
        frames[0].rechunk()
        if len(frames) == 1
        else pl.concat(frames, how="vertical", rechunk=True)
    )
    row_count = frame.height
    column_count = frame.width
    first_ingress, last_ingress, unique_ingress, ingress_sum = frame.select(
        pl.col("ingress_sequence").min().alias("first_ingress"),
        pl.col("ingress_sequence").max().alias("last_ingress"),
        pl.col("ingress_sequence").n_unique().alias("unique_ingress"),
        pl.col("ingress_sequence").sum().alias("ingress_sum"),
    ).row(0)
    ready_ns = time.clock_gettime_ns(time.CLOCK_MONOTONIC)
    expected_sum = expected_records * (expected_records + 1) // 2
    if (
        row_count != expected_records
        or column_count != len(INSTRUMENT_RAW_EVENT_COLUMNS)
        or first_ingress != 1
        or last_ingress != expected_records
        or unique_ingress != expected_records
        or ingress_sum != expected_sum
        or checkpoint.generation != generation
        or checkpoint.history_published_monotonic_ns > ready_ns
    ):
        raise RuntimeError("recovered Polars frame failed integrity checks")
    _emit(
        generation=generation,
        records=row_count,
        columns=column_count,
        batches=batch_count,
        first_ingress=first_ingress,
        last_ingress=last_ingress,
        unique_ingress=unique_ingress,
        ingress_sum=ingress_sum,
        history_published_monotonic_ns=(
            checkpoint.history_published_monotonic_ns
        ),
        open_return_ns=open_return_ns,
        polars_ready_ns=ready_ns,
        probe_elapsed_ns=ready_ns - start_ns,
        dataframe_estimated_bytes=frame.estimated_size(),
        polars_version=pl.__version__,
        python_pid=os.getpid(),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
