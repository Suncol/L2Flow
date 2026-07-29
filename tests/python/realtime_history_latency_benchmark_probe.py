"""Interactive Wire V2 history and tick-delta latency probe.

Invocation:

    realtime_history_latency_benchmark_probe.py \
        CONTROL_SOCKET NATIVE_LIBRARY SOURCE_PYTHON

After one ``READY`` line, stdin accepts these exact commands:

    HISTORY INSTRUMENT GENERATION price|all REPEATS EXPECTED_RECORDS
    DELTA_ORIGIN INSTRUMENT GENERATION price|all REPEATS EXPECTED_RECORDS
    DELTA_FROM_VERIFIED \
        INSTRUMENT GENERATION price|all REPEATS EXPECTED_RECORDS
    START_HISTORY_LOOP \
        INSTRUMENT GENERATION price|all EXPECTED_RECORDS
    STOP_HISTORY_LOOP
    LATEST_SERIES INSTRUMENT FIRST_INGRESS COUNT
    QUIT

Every measured repetition emits one ``*_SAMPLE`` key/value line.  A ``DONE``
line terminates each command.  DELTA_ORIGIN stores its EOF-verified target
checkpoint.  DELTA_FROM_VERIFIED uses the checkpoint stored before that
command for every repetition, then advances the stored checkpoint only after
all repetitions have reached explicit EOF and returned the same verified
target.

All timestamps use CLOCK_MONOTONIC, matching the service's
``history_published_monotonic_ns`` clock.  ``price`` constructs only
``tick_columns["price_p6"]`` and, on mixed pages, the corresponding
``snapshot_columns["last_price_p6"]``.  ``all`` calls ``materialize_all()``
before reading those already materialized price columns for the checksum.
"""

from __future__ import annotations

import gc
import os
import sys
import threading
import time
from dataclasses import dataclass
from typing import Iterable, Optional


_UINT32_MAX = (1 << 32) - 1
_UINT64_MAX = (1 << 64) - 1
_PAGE_RECORDS = 4096
_CHECKSUM_MASK = _UINT64_MAX
_CHECKSUM_OFFSET = 0xCBF29CE484222325
_CHECKSUM_PRIME = 0x100000001B3
_LATEST_SERIES_TIMEOUT_NS = 30_000_000_000


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def _now_ns() -> int:
    return time.clock_gettime_ns(time.CLOCK_MONOTONIC)


def _decimal(
    raw: str,
    field: str,
    *,
    maximum: int,
    allow_zero: bool,
) -> int:
    _require(
        bool(raw) and raw.isascii() and raw.isdecimal(),
        f"{field} must be an unsigned decimal integer",
    )
    value = int(raw, 10)
    lower = 0 if allow_zero else 1
    _require(
        lower <= value <= maximum,
        f"{field} is outside its valid range",
    )
    return value


def _projection(raw: str) -> str:
    _require(raw in ("price", "all"), "projection must be price or all")
    return raw


def _fold_column(
    checksum: int,
    values: Iterable[object],
    tag: int,
) -> tuple[int, int]:
    count = 0
    total = 0
    for value in values:
        _require(
            isinstance(value, int) and not isinstance(value, bool),
            "price_p6 column contains a non-integer",
        )
        count += 1
        total = (total + value) & _CHECKSUM_MASK
    checksum ^= tag & _CHECKSUM_MASK
    checksum = (checksum * _CHECKSUM_PRIME) & _CHECKSUM_MASK
    checksum ^= count
    checksum = (checksum * _CHECKSUM_PRIME) & _CHECKSUM_MASK
    checksum ^= total
    return checksum & _CHECKSUM_MASK, count


def _emit(kind: str, **fields: object) -> None:
    encoded: list[str] = []
    for name, value in fields.items():
        if isinstance(value, bool):
            text = "1" if value else "0"
        else:
            text = str(value)
        _require(
            text != "" and not any(character.isspace() for character in text),
            f"output field {name} is not line-protocol safe",
        )
        encoded.append(f"{name}={text}")
    print(kind + (" " if encoded else "") + " ".join(encoded), flush=True)


@dataclass(frozen=True, slots=True)
class _Scan:
    data_page_count: int
    terminal_page_count: int
    mapping_bytes: int
    record_count: int
    snapshot_count: int
    tick_count: int
    projected_value_count: int
    checksum: int
    scan_start_ns: int
    eof_columns_complete_ns: int

    @property
    def page_count(self) -> int:
        return self.data_page_count + self.terminal_page_count


@dataclass(frozen=True, slots=True)
class _LoopSnapshot:
    scans: int
    records: int
    data_page_count: int
    terminal_page_count: int
    mapping_bytes: int
    projected_value_count: int
    checksum: int
    scan_ns: int
    started_ns: int
    updated_ns: int


def _consume_history_page(
    page,
    projection: str,
    checksum: int,
) -> tuple[int, int]:
    if projection == "all":
        page.materialize_all()
    projected = 0
    if page.tick_count:
        checksum, count = _fold_column(
            checksum,
            page.tick_columns["price_p6"],
            0x5449434B,
        )
        projected += count
    if page.snapshot_count:
        checksum, count = _fold_column(
            checksum,
            page.snapshot_columns["last_price_p6"],
            0x534E4150,
        )
        projected += count
    _require(
        projected == page.record_count,
        "history price projection did not cover every mixed-history row",
    )
    return checksum, projected


def _scan_history(cursor, projection: str) -> _Scan:
    scan_start_ns = _now_ns()
    data_pages = 0
    terminal_pages = 0
    mapping_bytes = 0
    records = 0
    snapshots = 0
    ticks = 0
    projected = 0
    checksum = _CHECKSUM_OFFSET
    while True:
        page = cursor.read_page()
        _require(
            page is not None,
            "history cursor returned None before yielding explicit EOF",
        )
        if page.eof:
            terminal_pages += 1
            _require(
                page.record_count == 0
                and page.mapping_bytes == 0
                and cursor.eof
                and cursor.done,
                "history terminal page or cursor state is noncanonical",
            )
            eof_ns = _now_ns()
            break
        _require(
            page.record_count > 0 and page.mapping_bytes > 0,
            "history data page is empty or has no mapping",
        )
        data_pages += 1
        mapping_bytes += page.mapping_bytes
        records += page.record_count
        snapshots += page.snapshot_count
        ticks += page.tick_count
        checksum, count = _consume_history_page(
            page, projection, checksum
        )
        projected += count
    _require(terminal_pages == 1, "history did not yield exactly one EOF")
    return _Scan(
        data_page_count=data_pages,
        terminal_page_count=terminal_pages,
        mapping_bytes=mapping_bytes,
        record_count=records,
        snapshot_count=snapshots,
        tick_count=ticks,
        projected_value_count=projected,
        checksum=checksum,
        scan_start_ns=scan_start_ns,
        eof_columns_complete_ns=eof_ns,
    )


class _HistoryLoop:
    """Repeated complete scans whose cursor sockets never hold the client."""

    def __init__(
        self,
        client,
        *,
        instrument_id: int,
        expected_generation: int,
        projection: str,
        expected_records: int,
    ) -> None:
        self.instrument_id = instrument_id
        self.expected_generation = expected_generation
        self.projection = projection
        self.expected_records = expected_records
        self._client = client
        self._stop = threading.Event()
        self._first_result = threading.Event()
        self._continue_after_first = threading.Event()
        self._lock = threading.Lock()
        now = _now_ns()
        self._snapshot = _LoopSnapshot(
            scans=0,
            records=0,
            data_page_count=0,
            terminal_page_count=0,
            mapping_bytes=0,
            projected_value_count=0,
            checksum=_CHECKSUM_OFFSET,
            scan_ns=0,
            started_ns=now,
            updated_ns=now,
        )
        self._error: Optional[BaseException] = None
        self._thread = threading.Thread(
            target=self._run,
            name="l2flow-history-latency-loop",
            daemon=False,
        )

    def start_and_wait_for_first_scan(self) -> _LoopSnapshot:
        self._thread.start()
        self._first_result.wait()
        with self._lock:
            error = self._error
            snapshot = self._snapshot
        if error is not None:
            self._continue_after_first.set()
            self._thread.join()
            raise RuntimeError("history loop first scan failed") from error
        _require(
            snapshot.scans == 1,
            "history loop did not pause after its first complete scan",
        )
        return snapshot

    def continue_after_started_response(self) -> None:
        self._continue_after_first.set()

    def ensure_healthy(self) -> None:
        with self._lock:
            error = self._error
        if error is not None:
            raise RuntimeError("history loop failed") from error

    def stop_and_join(self) -> _LoopSnapshot:
        self._stop.set()
        self._continue_after_first.set()
        self._thread.join()
        self.ensure_healthy()
        with self._lock:
            return self._snapshot

    def _run(self) -> None:
        try:
            first = True
            while not self._stop.is_set():
                scan = self._scan_once()
                with self._lock:
                    prior = self._snapshot
                    self._snapshot = _LoopSnapshot(
                        scans=prior.scans + 1,
                        records=prior.records + scan.record_count,
                        data_page_count=(
                            prior.data_page_count
                            + scan.data_page_count
                        ),
                        terminal_page_count=(
                            prior.terminal_page_count
                            + scan.terminal_page_count
                        ),
                        mapping_bytes=(
                            prior.mapping_bytes + scan.mapping_bytes
                        ),
                        projected_value_count=(
                            prior.projected_value_count
                            + scan.projected_value_count
                        ),
                        checksum=(
                            (
                                prior.checksum
                                * _CHECKSUM_PRIME
                            )
                            ^ scan.checksum
                        )
                        & _CHECKSUM_MASK,
                        scan_ns=(
                            prior.scan_ns
                            + scan.eof_columns_complete_ns
                            - scan.scan_start_ns
                        ),
                        started_ns=prior.started_ns,
                        updated_ns=scan.eof_columns_complete_ns,
                    )
                if first:
                    first = False
                    self._first_result.set()
                    self._continue_after_first.wait()
        except BaseException as error:  # noqa: BLE001 - thread boundary
            with self._lock:
                self._error = error
            self._first_result.set()

    def _scan_once(self) -> _Scan:
        cursor = self._client.open_instrument_history(
            self.instrument_id,
            requested_page_records=_PAGE_RECORDS,
            expected_generation=self.expected_generation,
        )
        _require(
            cursor.generation.generation
            == self.expected_generation,
            "history loop OPEN returned an unexpected generation",
        )
        _require(
            cursor.generation.instrument_record_count
            == self.expected_records,
            "history loop OPEN count differs from expected_records",
        )
        # The complete scan uses only the cursor-owned SOCK_SEQPACKET
        # connection. L2FlowClient's latest-value lock is not held here.
        with cursor:
            scan = _scan_history(cursor, self.projection)
        _require(
            scan.record_count == self.expected_records,
            "history loop EOF count differs from expected_records",
        )
        return scan


def _consume_delta_page(
    page,
    projection: str,
    checksum: int,
) -> tuple[int, int]:
    if projection == "all":
        page.materialize_all()
    checksum, count = _fold_column(
        checksum,
        page.tick_columns["price_p6"],
        0x44454C5441,
    )
    _require(
        count == page.record_count,
        "delta price projection did not cover every row",
    )
    return checksum, count


def _scan_delta(cursor, projection: str) -> _Scan:
    scan_start_ns = _now_ns()
    data_pages = 0
    terminal_pages = 0
    mapping_bytes = 0
    records = 0
    projected = 0
    checksum = _CHECKSUM_OFFSET
    while True:
        page = cursor.read_page()
        _require(
            page is not None,
            "delta cursor returned None before yielding explicit EOF",
        )
        if page.eof:
            terminal_pages += 1
            _require(
                page.record_count == 0
                and page.mapping_bytes == 0
                and cursor.eof
                and cursor.done,
                "delta terminal page or cursor state is noncanonical",
            )
            eof_ns = _now_ns()
            break
        _require(
            page.record_count > 0 and page.mapping_bytes > 0,
            "delta data page is empty or has no mapping",
        )
        data_pages += 1
        mapping_bytes += page.mapping_bytes
        records += page.record_count
        checksum, count = _consume_delta_page(
            page, projection, checksum
        )
        projected += count
    _require(terminal_pages == 1, "delta did not yield exactly one EOF")
    return _Scan(
        data_page_count=data_pages,
        terminal_page_count=terminal_pages,
        mapping_bytes=mapping_bytes,
        record_count=records,
        snapshot_count=0,
        tick_count=records,
        projected_value_count=projected,
        checksum=checksum,
        scan_start_ns=scan_start_ns,
        eof_columns_complete_ns=eof_ns,
    )


def _parse_measurement(
    words: list[str],
) -> tuple[int, int, str, int, int]:
    _require(
        len(words) == 6,
        f"{words[0]} requires exactly five arguments",
    )
    instrument_id = _decimal(
        words[1],
        "instrument",
        maximum=_UINT32_MAX,
        allow_zero=False,
    )
    generation = _decimal(
        words[2],
        "generation",
        maximum=_UINT64_MAX,
        allow_zero=False,
    )
    projection = _projection(words[3])
    repeats = _decimal(
        words[4],
        "repeats",
        maximum=_UINT32_MAX,
        allow_zero=False,
    )
    expected_records = _decimal(
        words[5],
        "expected_records",
        maximum=_UINT64_MAX,
        allow_zero=True,
    )
    return (
        instrument_id,
        generation,
        projection,
        repeats,
        expected_records,
    )


def _parse_history_loop(
    words: list[str],
) -> tuple[int, int, str, int]:
    _require(
        len(words) == 5,
        "START_HISTORY_LOOP requires exactly four arguments",
    )
    instrument_id = _decimal(
        words[1],
        "instrument",
        maximum=_UINT32_MAX,
        allow_zero=False,
    )
    generation = _decimal(
        words[2],
        "generation",
        maximum=_UINT64_MAX,
        allow_zero=False,
    )
    projection = _projection(words[3])
    expected_records = _decimal(
        words[4],
        "expected_records",
        maximum=_UINT64_MAX,
        allow_zero=True,
    )
    return instrument_id, generation, projection, expected_records


def _run_history(
    client,
    *,
    command_index: int,
    instrument_id: int,
    expected_generation: int,
    projection: str,
    repeats: int,
    expected_records: int,
) -> None:
    for sample in range(repeats):
        open_start_ns = _now_ns()
        cursor = client.open_instrument_history(
            instrument_id,
            requested_page_records=_PAGE_RECORDS,
            expected_generation=expected_generation,
        )
        open_return_ns = _now_ns()
        published_ns = (
            cursor.generation.history_published_monotonic_ns
        )
        _require(
            published_ns <= open_return_ns,
            "history publication timestamp is after Python OPEN return",
        )
        _require(
            cursor.generation.generation == expected_generation,
            "history OPEN returned an unexpected generation",
        )
        _require(
            cursor.generation.instrument_record_count
            == expected_records,
            "history OPEN count differs from expected_records",
        )
        with cursor:
            scan = _scan_history(cursor, projection)
        _require(
            scan.record_count == expected_records,
            "history EOF scan count differs from expected_records",
        )
        _require(
            scan.snapshot_count + scan.tick_count
            == scan.record_count,
            "history scanned payload counts do not reconcile",
        )
        _emit(
            "HISTORY_SAMPLE",
            command=command_index,
            sample=sample,
            instrument_id=instrument_id,
            expected_generation=expected_generation,
            actual_generation=cursor.generation.generation,
            projection=projection,
            requested_page_records=_PAGE_RECORDS,
            expected_records=expected_records,
            generation_record_count=(
                cursor.generation.instrument_record_count
            ),
            page_count=scan.page_count,
            data_page_count=scan.data_page_count,
            terminal_page_count=scan.terminal_page_count,
            mapping_bytes=scan.mapping_bytes,
            record_count=scan.record_count,
            snapshot_count=scan.snapshot_count,
            tick_count=scan.tick_count,
            projected_value_count=scan.projected_value_count,
            checksum=scan.checksum,
            history_published_monotonic_ns=published_ns,
            open_call_start_ns=open_start_ns,
            open_return_ns=open_return_ns,
            scan_start_ns=scan.scan_start_ns,
            eof_columns_complete_ns=(
                scan.eof_columns_complete_ns
            ),
            python_open_ns=open_return_ns - open_start_ns,
            publication_to_open_return_ns=(
                open_return_ns - published_ns
            ),
            complete_scan_ns=(
                scan.eof_columns_complete_ns - scan.scan_start_ns
            ),
            open_return_to_eof_columns_complete_ns=(
                scan.eof_columns_complete_ns - open_return_ns
            ),
            publication_to_complete_ns=(
                scan.eof_columns_complete_ns - published_ns
            ),
        )
    _emit(
        "DONE",
        command=command_index,
        operation="HISTORY",
        samples=repeats,
    )


def _run_delta(
    client,
    checkpoints: dict[int, object],
    *,
    command_index: int,
    origin: bool,
    instrument_id: int,
    expected_generation: int,
    projection: str,
    repeats: int,
    expected_records: int,
) -> None:
    base_checkpoint: Optional[object]
    if origin:
        base_checkpoint = None
    else:
        _require(
            instrument_id in checkpoints,
            "DELTA_FROM_VERIFIED has no saved checkpoint",
        )
        base_checkpoint = checkpoints[instrument_id]

    verified_targets: list[object] = []
    for sample in range(repeats):
        session_open_start_ns = _now_ns()
        session = client.open_instrument_tick_delta_session(
            expected_generation=expected_generation
        )
        session_open_return_ns = _now_ns()
        with session:
            cursor_open_start_ns = _now_ns()
            cursor = session.open_instrument(
                instrument_id,
                base_checkpoint=base_checkpoint,
                requested_page_records=_PAGE_RECORDS,
            )
            cursor_open_return_ns = _now_ns()
            with cursor:
                scan = _scan_delta(cursor, projection)
                _require(
                    scan.record_count == expected_records,
                    "delta EOF scan count differs from expected_records",
                )
                checkpoint_access_start_ns = _now_ns()
                checkpoint = cursor.verified_checkpoint
                checkpoint_return_ns = _now_ns()
        _require(
            checkpoint.generation == expected_generation,
            "delta verified checkpoint generation is unexpected",
        )
        base_generation = (
            0 if base_checkpoint is None else base_checkpoint.generation
        )
        base_tick_count = (
            0
            if base_checkpoint is None
            else base_checkpoint.instrument_tick_record_count
        )
        _require(
            checkpoint.instrument_tick_record_count - base_tick_count
            == expected_records,
            "verified checkpoint delta differs from expected_records",
        )
        published_ns = checkpoint.history_published_monotonic_ns
        _require(
            published_ns <= session_open_return_ns,
            "delta publication timestamp is after session OPEN return",
        )
        verified_targets.append(checkpoint)
        _emit(
            "DELTA_SAMPLE",
            command=command_index,
            sample=sample,
            mode="origin" if origin else "verified",
            instrument_id=instrument_id,
            base_generation=base_generation,
            expected_generation=expected_generation,
            checkpoint_generation=checkpoint.generation,
            projection=projection,
            requested_page_records=_PAGE_RECORDS,
            expected_records=expected_records,
            base_tick_record_count=base_tick_count,
            checkpoint_tick_record_count=(
                checkpoint.instrument_tick_record_count
            ),
            page_count=scan.page_count,
            data_page_count=scan.data_page_count,
            terminal_page_count=scan.terminal_page_count,
            mapping_bytes=scan.mapping_bytes,
            record_count=scan.record_count,
            tick_count=scan.tick_count,
            projected_value_count=scan.projected_value_count,
            checksum=scan.checksum,
            history_published_monotonic_ns=published_ns,
            session_open_call_start_ns=session_open_start_ns,
            session_open_return_ns=session_open_return_ns,
            cursor_open_call_start_ns=cursor_open_start_ns,
            cursor_open_return_ns=cursor_open_return_ns,
            scan_start_ns=scan.scan_start_ns,
            eof_return_ns=scan.eof_columns_complete_ns,
            checkpoint_access_start_ns=checkpoint_access_start_ns,
            checkpoint_return_ns=checkpoint_return_ns,
            session_open_ns=(
                session_open_return_ns - session_open_start_ns
            ),
            cursor_open_ns=(
                cursor_open_return_ns - cursor_open_start_ns
            ),
            scan_to_eof_ns=(
                scan.eof_columns_complete_ns - scan.scan_start_ns
            ),
            checkpoint_access_ns=(
                checkpoint_return_ns - checkpoint_access_start_ns
            ),
            cursor_open_return_to_checkpoint_ns=(
                checkpoint_return_ns - cursor_open_return_ns
            ),
            publication_to_checkpoint_ns=(
                checkpoint_return_ns - published_ns
            ),
        )

    first_target = verified_targets[0]
    _require(
        all(target == first_target for target in verified_targets),
        "repeated delta scans returned conflicting verified checkpoints",
    )
    checkpoints[instrument_id] = first_target
    _emit(
        "DONE",
        command=command_index,
        operation=(
            "DELTA_ORIGIN" if origin else "DELTA_FROM_VERIFIED"
        ),
        samples=repeats,
        saved_generation=first_target.generation,
        saved_tick_record_count=(
            first_target.instrument_tick_record_count
        ),
    )


def _start_history_loop(
    client,
    *,
    command_index: int,
    instrument_id: int,
    expected_generation: int,
    projection: str,
    expected_records: int,
) -> _HistoryLoop:
    loop = _HistoryLoop(
        client,
        instrument_id=instrument_id,
        expected_generation=expected_generation,
        projection=projection,
        expected_records=expected_records,
    )
    first = loop.start_and_wait_for_first_scan()
    try:
        _emit(
            "HISTORY_LOOP_STARTED",
            command=command_index,
            instrument_id=instrument_id,
            generation=expected_generation,
            projection=projection,
            expected_records=expected_records,
            requested_page_records=_PAGE_RECORDS,
            scans=first.scans,
            records=first.records,
            data_page_count=first.data_page_count,
            terminal_page_count=first.terminal_page_count,
            mapping_bytes=first.mapping_bytes,
            projected_value_count=first.projected_value_count,
            checksum=first.checksum,
            scan_ns=first.scan_ns,
            started_ns=first.started_ns,
            first_complete_ns=first.updated_ns,
        )
    except BaseException:
        loop.stop_and_join()
        raise
    loop.continue_after_started_response()
    return loop


def _stop_history_loop(
    loop: _HistoryLoop,
    *,
    command_index: int,
) -> None:
    stopped_ns = _now_ns()
    final = loop.stop_and_join()
    joined_ns = _now_ns()
    _require(
        final.scans >= 1,
        "history loop stopped without one complete scan",
    )
    scan_records_per_second = (
        0
        if final.scan_ns == 0
        else final.records * 1_000_000_000 // final.scan_ns
    )
    _emit(
        "HISTORY_LOOP_STOPPED",
        command=command_index,
        instrument_id=loop.instrument_id,
        generation=loop.expected_generation,
        projection=loop.projection,
        expected_records=loop.expected_records,
        scans=final.scans,
        records=final.records,
        data_page_count=final.data_page_count,
        terminal_page_count=final.terminal_page_count,
        mapping_bytes=final.mapping_bytes,
        projected_value_count=final.projected_value_count,
        checksum=final.checksum,
        scan_ns=final.scan_ns,
        scan_records_per_second=scan_records_per_second,
        started_ns=final.started_ns,
        stop_call_ns=stopped_ns,
        joined_ns=joined_ns,
        wall_elapsed_ns=joined_ns - final.started_ns,
        join_wait_ns=joined_ns - stopped_ns,
    )


def _parse_latest_series(
    words: list[str],
) -> tuple[int, int, int]:
    _require(
        len(words) == 4,
        "LATEST_SERIES requires exactly three arguments",
    )
    instrument_id = _decimal(
        words[1],
        "instrument",
        maximum=_UINT32_MAX,
        allow_zero=False,
    )
    first_ingress = _decimal(
        words[2],
        "first_ingress",
        maximum=_UINT64_MAX,
        allow_zero=False,
    )
    count = _decimal(
        words[3],
        "count",
        maximum=_UINT64_MAX,
        allow_zero=False,
    )
    _require(
        count - 1 <= _UINT64_MAX - first_ingress,
        "LATEST_SERIES ingress range overflows uint64",
    )
    return instrument_id, first_ingress, count


def _run_latest_series(
    client,
    latest_status,
    inconsistent_read_error,
    history_loop: Optional[_HistoryLoop],
    *,
    command_index: int,
    instrument_id: int,
    first_ingress: int,
    count: int,
) -> None:
    preflight_retries = 0
    while True:
        try:
            before = client.latest_snapshot(instrument_id)
            break
        except inconsistent_read_error:
            preflight_retries += 1
    _require(
        before.status is latest_status.AVAILABLE,
        "LATEST_SERIES preflight snapshot is unavailable",
    )
    _require(
        before.common is not None,
        "LATEST_SERIES preflight snapshot has no common record",
    )
    _require(
        before.common.ingress_sequence < first_ingress,
        "LATEST_SERIES first ingress is already visible",
    )
    _emit(
        "LATEST_ARMED",
        command=command_index,
        instrument_id=instrument_id,
        expected=first_ingress,
        count=count,
        preflight_ingress=before.common.ingress_sequence,
        preflight_inconsistent_retries=preflight_retries,
    )

    series_start_ns = _now_ns()
    total_polls = 0
    total_stale = 0
    total_inconsistent = 0
    for sample in range(count):
        expected = first_ingress + sample
        polls = 0
        stale_polls = 0
        inconsistent_retries = 0
        while True:
            polls += 1
            try:
                snapshot = client.latest_snapshot(instrument_id)
            except inconsistent_read_error:
                inconsistent_retries += 1
                _require(
                    _now_ns() - series_start_ns
                    <= _LATEST_SERIES_TIMEOUT_NS,
                    "LATEST_SERIES timed out after inconsistent reads",
                )
                continue
            seen_ns = _now_ns()
            _require(
                seen_ns - series_start_ns
                <= _LATEST_SERIES_TIMEOUT_NS,
                "LATEST_SERIES timed out waiting for callback",
            )
            _require(
                snapshot.status is latest_status.AVAILABLE,
                "LATEST_SERIES snapshot became unavailable",
            )
            _require(
                snapshot.common is not None,
                "LATEST_SERIES snapshot has no common record",
            )
            observed = snapshot.common.ingress_sequence
            _require(
                observed <= expected,
                "Python latest skipped an expected ingress sequence",
            )
            if observed < expected:
                stale_polls += 1
                continue
            recv_ns = snapshot.common.recv_monotonic_ns
            _require(
                0 < recv_ns <= seen_ns,
                "LATEST_SERIES callback monotonic timestamp is invalid",
            )
            if history_loop is not None:
                history_loop.ensure_healthy()
            break

        total_polls += polls
        total_stale += stale_polls
        total_inconsistent += inconsistent_retries
        _emit(
            "LATEST_SAMPLE",
            command=command_index,
            sample=sample,
            instrument_id=instrument_id,
            expected=expected,
            seen_ns=seen_ns,
            wire_recv_monotonic_ns=recv_ns,
            callback_to_python_ns=seen_ns - recv_ns,
            polls=polls,
            stale_polls=stale_polls,
            inconsistent_retries=inconsistent_retries,
        )
    completed_ns = _now_ns()
    _emit(
        "DONE",
        command=command_index,
        operation="LATEST_SERIES",
        samples=count,
        first_ingress=first_ingress,
        last_ingress=first_ingress + count - 1,
        total_polls=total_polls,
        total_stale_polls=total_stale,
        total_inconsistent_retries=total_inconsistent,
        elapsed_ns=completed_ns - series_start_ns,
    )


def _validate_paths(
    control_socket: str,
    native_library: str,
    source_python: str,
) -> None:
    _require(
        os.path.isabs(control_socket),
        "control socket path must be absolute",
    )
    _require(
        os.path.isabs(native_library),
        "native library path must be absolute",
    )
    _require(
        os.path.isabs(source_python),
        "Python source path must be absolute",
    )
    _require(
        os.path.isfile(native_library),
        "native library does not exist",
    )
    _require(
        os.path.isdir(source_python),
        "Python source directory does not exist",
    )


def main(argv: list[str]) -> int:
    if len(argv) != 4:
        raise RuntimeError(
            "usage: realtime_history_latency_benchmark_probe.py "
            "CONTROL_SOCKET NATIVE_LIBRARY SOURCE_PYTHON"
        )
    control_socket, native_library, source_python = argv[1:]
    _validate_paths(control_socket, native_library, source_python)
    sys.path.insert(0, source_python)

    from l2flow_realtime import (  # pylint: disable=import-outside-toplevel
        InconsistentReadError,
        L2FlowClient,
        LatestStatus,
    )

    checkpoints: dict[int, object] = {}
    with L2FlowClient.connect(
        control_socket,
        native_library=native_library,
        timeout=5.0,
        stale_after_ns=None,
    ) as client:
        session = client.session_info()
        clock = time.get_clock_info("monotonic")
        _emit(
            "READY",
            protocol="wire_v2_history_latency_1",
            run_id=session.run_id.hex(),
            session_epoch=session.session_epoch,
            trade_date=session.trade_date,
            capacity=session.capacity,
            bound_count=session.bound_count,
            monotonic_implementation=(
                clock.implementation.replace(" ", "_")
            ),
            monotonic_resolution_ns=max(
                1, round(clock.resolution * 1_000_000_000)
            ),
            gc_enabled=gc.isenabled(),
        )

        command_index = 0
        history_loop: Optional[_HistoryLoop] = None
        try:
            for line in sys.stdin:
                words = line.strip().split()
                _require(bool(words), "blank command is not allowed")
                operation = words[0]
                if operation == "QUIT":
                    _require(
                        len(words) == 1,
                        "QUIT takes no arguments",
                    )
                    _require(
                        history_loop is None,
                        "STOP_HISTORY_LOOP is required before QUIT",
                    )
                    _emit("BYE", commands=command_index)
                    return 0
                if operation == "STOP_HISTORY_LOOP":
                    _require(
                        len(words) == 1,
                        "STOP_HISTORY_LOOP takes no arguments",
                    )
                    _require(
                        history_loop is not None,
                        "no history loop is running",
                    )
                    command_index += 1
                    _stop_history_loop(
                        history_loop,
                        command_index=command_index,
                    )
                    history_loop = None
                    continue
                if history_loop is not None:
                    history_loop.ensure_healthy()
                if operation == "START_HISTORY_LOOP":
                    _require(
                        history_loop is None,
                        "a history loop is already running",
                    )
                    (
                        instrument_id,
                        generation,
                        projection,
                        expected_records,
                    ) = _parse_history_loop(words)
                    command_index += 1
                    history_loop = _start_history_loop(
                        client,
                        command_index=command_index,
                        instrument_id=instrument_id,
                        expected_generation=generation,
                        projection=projection,
                        expected_records=expected_records,
                    )
                    continue
                if operation == "LATEST_SERIES":
                    (
                        instrument_id,
                        first_ingress,
                        count,
                    ) = _parse_latest_series(words)
                    command_index += 1
                    _run_latest_series(
                        client,
                        LatestStatus,
                        InconsistentReadError,
                        history_loop,
                        command_index=command_index,
                        instrument_id=instrument_id,
                        first_ingress=first_ingress,
                        count=count,
                    )
                    continue
                _require(
                    operation
                    in (
                        "HISTORY",
                        "DELTA_ORIGIN",
                        "DELTA_FROM_VERIFIED",
                    ),
                    "unknown probe command",
                )
                command_index += 1
                (
                    instrument_id,
                    generation,
                    projection,
                    repeats,
                    expected_records,
                ) = _parse_measurement(words)
                if operation == "HISTORY":
                    _run_history(
                        client,
                        command_index=command_index,
                        instrument_id=instrument_id,
                        expected_generation=generation,
                        projection=projection,
                        repeats=repeats,
                        expected_records=expected_records,
                    )
                else:
                    _run_delta(
                        client,
                        checkpoints,
                        command_index=command_index,
                        origin=operation == "DELTA_ORIGIN",
                        instrument_id=instrument_id,
                        expected_generation=generation,
                        projection=projection,
                        repeats=repeats,
                        expected_records=expected_records,
                    )
        finally:
            if history_loop is not None:
                try:
                    history_loop.stop_and_join()
                except BaseException:
                    pass
    raise RuntimeError("stdin reached EOF before QUIT")


def _entrypoint() -> int:
    try:
        return main(sys.argv)
    except Exception as error:  # noqa: BLE001 - process protocol boundary
        print(
            f"ERROR {type(error).__name__}: {error}",
            file=sys.stderr,
            flush=True,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(_entrypoint())
