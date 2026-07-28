from __future__ import annotations

import os
import sys


SHANGHAI_INSTRUMENT_ID = 1001
SHENZHEN_INSTRUMENT_ID = 2002


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def main(argv: list[str]) -> int:
    if len(argv) != 6:
        raise RuntimeError(
            "usage: realtime_ipc_e2e_probe.py "
            "CONTROL_SOCKET NATIVE_READER SOURCE_PYTHON "
            "READY_FD RELEASE_FD"
        )
    (
        control_socket,
        native_reader,
        source_python,
        ready_descriptor,
        release_descriptor,
    ) = argv[1:]
    ready_fd = int(ready_descriptor)
    release_fd = int(release_descriptor)
    _require(ready_fd >= 0 and release_fd >= 0, "invalid synchronization fd")
    _require(os.path.isabs(control_socket), "control socket is not absolute")
    _require(os.path.isfile(native_reader), "native reader does not exist")
    _require(os.path.isdir(source_python), "source Python path does not exist")
    sys.path.insert(0, source_python)

    from l2flow_realtime import (  # pylint: disable=import-outside-toplevel
        HistoryNotFoundError,
        InstrumentKey,
        InstrumentLookupStatus,
        L2FlowClient,
        LatestStatus,
        Market,
        MarketEventKind,
        ServerState,
        TickProjectionFlag,
    )

    client = None
    cursor = None
    history_cursor = None
    second_history_cursor = None
    empty_history_cursor = None
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

        resolved = client.resolve_instruments(
            [
                InstrumentKey(
                    Market.SHENZHEN, b"102", b"000002"
                ),
                InstrumentKey(
                    Market.SHANGHAI, b"101", b"600001"
                ),
                InstrumentKey(
                    Market.SHENZHEN, b"102 ", b"000002"
                ),
            ]
        )
        _require(
            [item.status for item in resolved]
            == [
                InstrumentLookupStatus.FOUND,
                InstrumentLookupStatus.FOUND,
                InstrumentLookupStatus.UNKNOWN,
            ]
            and [item.instrument_id for item in resolved]
            == [
                SHENZHEN_INSTRUMENT_ID,
                SHANGHAI_INSTRUMENT_ID,
                None,
            ],
            "exact-byte registry resolution is wrong",
        )
        one_resolved = client.resolve_instrument(
            Market.SHANGHAI, b"101", b"600001"
        )
        _require(
            one_resolved.found
            and one_resolved.instrument_id == SHANGHAI_INSTRUMENT_ID,
            "point registry resolution is wrong",
        )

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
        require_polars = (
            os.environ.get("L2FLOW_IPC_E2E_REQUIRE_POLARS") == "1"
        )
        try:
            import polars as pl  # pylint: disable=import-outside-toplevel
        except ImportError:
            _require(
                not require_polars,
                "Polars is required for the latest-value factor probe",
            )
        else:
            factor_frame = snapshots.to_polars().with_columns(
                pl.when(
                    pl.col("status") == int(LatestStatus.AVAILABLE)
                )
                .then(pl.col("last_price_p6"))
                .otherwise(None)
                .alias("placeholder_factor_p6")
            )
            _require(
                factor_frame["instrument_id"].to_list()
                == instrument_ids
                and factor_frame["placeholder_factor_p6"].to_list()
                == [1_234_000, 2_234_000],
                "latest snapshot Polars placeholder factor is wrong",
            )
            _require(
                factor_frame.schema["instrument_id"] == pl.UInt32
                and factor_frame.schema["placeholder_factor_p6"]
                == pl.Int64,
                "latest snapshot Polars factor dtypes are wrong",
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

        history_cursor = client.open_instrument_history(
            SHANGHAI_INSTRUMENT_ID,
            requested_page_records=2,
            timeout=2.0,
        )
        _require(
            history_cursor.generation.generation == 1,
            "history cursor did not pin generation 1 before handoff",
        )
        _require(
            os.write(ready_fd, b"R") == 1,
            "failed to report pinned history cursor",
        )
        os.close(ready_fd)
        ready_fd = -1
        publication_status = os.read(release_fd, 1)
        os.close(release_fd)
        release_fd = -1
        _require(
            publication_status == b"P",
            "service did not publish the empty-increment generation",
        )

        history_pages = list(history_cursor.pages())
        history_records = [
            record
            for page in history_pages
            for record in page.records
        ]
        _require(
            history_cursor.generation.generation == 1
            and history_cursor.generation.recv_monotonic_cut_ns == 60_000
            and history_cursor.generation.record_coverage_complete
            and not history_cursor.generation.field_complete
            and history_cursor.generation.payload_projection == 1
            and history_cursor.generation.total_record_count == 4
            and history_cursor.generation.source_record_counts
            == (1, 3, 0, 0),
            "history generation completeness metadata is wrong",
        )
        _require(
            [len(page.records) for page in history_pages] == [2, 2, 0]
            and [page.eof for page in history_pages]
            == [False, False, True],
            "history pagination or explicit EOF is wrong",
        )
        _require(
            history_cursor.done and history_cursor.closed,
            "history cursor did not close after verified EOF",
        )
        _require(
            [record.ingress_sequence for record in history_records]
            == [1, 2, 6, 7]
            and [record.tick_stream_sequence for record in history_records]
            == [0, 1, 0, 4]
            and [record.event_kind for record in history_records]
            == [
                MarketEventKind.SHANGHAI_SNAPSHOT,
                MarketEventKind.SHANGHAI_TICK,
                MarketEventKind.SHANGHAI_TICK,
                MarketEventKind.SHANGHAI_TICK,
            ]
            and history_records[-1].projection_flags == 3,
            "complete Store history order/core projection is wrong",
        )
        long_raw_tick = history_records[-1].value
        _require(
            long_raw_tick.projection_flags
            == (
                TickProjectionFlag.RAW_TYPE_OMITTED
                | TickProjectionFlag.RAW_TICK_FLAG_OMITTED
            )
            and long_raw_tick.raw_type_omitted
            and long_raw_tick.raw_tick_flag_omitted
            and long_raw_tick.raw_type == b""
            and long_raw_tick.raw_tick_flag == b"",
            "history tick payload lost oversized-raw omission metadata",
        )
        if "pl" in locals():
            history_frames = history_pages[0].to_polars_by_kind()
            _require(
                history_frames["snapshots"][
                    "store_generation"
                ].to_list()
                == [1]
                and history_frames["ticks"][
                    "ingress_sequence"
                ].to_list()
                == [2]
                and history_frames["ticks"][
                    "payload_projection"
                ].to_list()
                == [1]
                and history_frames["ticks"][
                    "field_complete"
                ].to_list()
                == [0],
                "history Polars by-kind projection is wrong",
            )

        second_history_cursor = client.open_instrument_history(
            SHANGHAI_INSTRUMENT_ID,
            requested_page_records=2,
            timeout=2.0,
        )
        second_history_pages = list(second_history_cursor.pages())
        second_history_records = [
            record
            for page in second_history_pages
            for record in page.records
        ]
        _require(
            second_history_cursor.generation.generation == 2
            and second_history_cursor.generation.recv_monotonic_cut_ns
            == 60_001
            and second_history_cursor.generation.total_record_count == 4
            and second_history_cursor.generation.source_record_counts
            == (1, 3, 0, 0),
            "new history cursor did not observe generation 2",
        )
        _require(
            [len(page.records) for page in second_history_pages]
            == [2, 2, 0]
            and [record.ingress_sequence for record in second_history_records]
            == [1, 2, 6, 7]
            and second_history_cursor.done
            and second_history_cursor.closed,
            "empty-increment generation changed history content or EOF",
        )

        empty_history_cursor = client.open_history(
            3003, requested_page_records=2, timeout=2.0
        )
        empty_page = empty_history_cursor.read()
        _require(
            empty_page.eof
            and not empty_page.records
            and empty_history_cursor.closed
            and empty_history_cursor.generation.generation == 2
            and empty_history_cursor.generation.total_record_count == 0,
            "registered empty instrument is not a clean generation-2 EOF",
        )
        try:
            client.open_history(9009, timeout=2.0)
        except HistoryNotFoundError:
            pass
        else:
            raise RuntimeError(
                "unknown instrument history did not report not-found"
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
        if release_fd >= 0:
            os.close(release_fd)
        if ready_fd >= 0:
            os.close(ready_fd)
        if empty_history_cursor is not None:
            empty_history_cursor.close()
        if second_history_cursor is not None:
            second_history_cursor.close()
        if history_cursor is not None:
            history_cursor.close()
        if cursor is not None:
            cursor.close()
        if client is not None:
            client.close()

    _require(client is not None and client.closed, "client did not close")
    _require(cursor is not None and cursor.closed, "cursor did not close")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
