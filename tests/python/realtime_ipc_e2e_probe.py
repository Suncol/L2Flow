from __future__ import annotations

import os
import sys


SHANGHAI_INSTRUMENT_ID = 1001
SHENZHEN_INSTRUMENT_ID = 2002


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def main(argv: list[str]) -> int:
    if len(argv) != 4:
        raise RuntimeError(
            "usage: realtime_ipc_e2e_probe.py "
            "CONTROL_SOCKET NATIVE_READER SOURCE_PYTHON"
        )
    control_socket, native_reader, source_python = argv[1:]
    _require(os.path.isabs(control_socket), "control socket is not absolute")
    _require(os.path.isfile(native_reader), "native reader does not exist")
    _require(os.path.isdir(source_python), "source Python path does not exist")
    sys.path.insert(0, source_python)

    from l2flow_realtime import (  # pylint: disable=import-outside-toplevel
        L2FlowClient,
        LatestStatus,
        MarketEventKind,
        ServerState,
    )

    client = None
    cursor = None
    try:
        client = L2FlowClient.connect(
            control_socket,
            native_library=native_reader,
            timeout=2.0,
        )
        session = client.session_info()
        _require(
            session.server_state is ServerState.ACTIVE,
            "service is not ACTIVE during Python probe",
        )
        _require(not session.coverage_lost, "service coverage is lost")

        instrument_ids = [
            SHANGHAI_INSTRUMENT_ID,
            SHENZHEN_INSTRUMENT_ID,
        ]
        snapshots = client.get_latest_snapshots(instrument_ids)
        _require(
            [item.status for item in snapshots]
            == [LatestStatus.AVAILABLE, LatestStatus.AVAILABLE],
            "SH/SZ latest snapshots are not both available",
        )
        _require(
            [item.value.common.event_kind for item in snapshots]
            == [
                MarketEventKind.SHANGHAI_SNAPSHOT,
                MarketEventKind.SHENZHEN_SNAPSHOT,
            ],
            "SH/SZ latest snapshot kinds are wrong",
        )
        snapshot_columns = snapshots.to_columns()
        _require(
            snapshot_columns["instrument_id"] == instrument_ids
            and snapshot_columns["last_price_p6"]
            == [1_234_000, 2_234_000],
            "SH/SZ snapshot columns are wrong",
        )

        latest_ticks = client.get_latest_ticks(instrument_ids)
        _require(
            [item.status for item in latest_ticks]
            == [LatestStatus.AVAILABLE, LatestStatus.AVAILABLE],
            "SH/SZ mixed latest ticks are not both available",
        )
        tick_columns = latest_ticks.to_columns()
        _require(
            tick_columns["instrument_id"] == instrument_ids
            and tick_columns["tick_stream_sequence"] == [1, 3]
            and tick_columns["event_kind"]
            == [
                int(MarketEventKind.SHANGHAI_TICK),
                int(MarketEventKind.SHENZHEN_TRANSACTION),
            ],
            "mixed latest-tick columns are wrong",
        )

        bar = client.get_latest_kline(SHENZHEN_INSTRUMENT_ID, 60_000)
        _require(
            bar.status is LatestStatus.AVAILABLE
            and bar.value.generation == 1
            and bar.value.close_price_p6 == 2_236_000,
            "latest immutable-generation KLine is wrong",
        )

        cursor = client.open_tick_cursor(start="earliest")
        tick_batch = cursor.read(8)
        cursor_columns = tick_batch.to_columns()
        _require(
            tick_batch.first_sequence == 2
            and tick_batch.next_sequence == 4
            and cursor_columns["tick_stream_sequence"] == [2, 3]
            and cursor_columns["event_kind"]
            == [
                int(MarketEventKind.SHENZHEN_ORDER),
                int(MarketEventKind.SHENZHEN_TRANSACTION),
            ],
            "earliest mixed-tick cursor is not contiguous",
        )
    finally:
        if cursor is not None:
            cursor.close()
        if client is not None:
            client.close()

    _require(client is not None and client.closed, "client did not close")
    _require(cursor is not None and cursor.closed, "cursor did not close")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
