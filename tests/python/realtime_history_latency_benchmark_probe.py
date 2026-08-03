"""Interactive Wire V2 history and tick-delta latency probe.

Invocation:

    realtime_history_latency_benchmark_probe.py \
        CONTROL_SOCKET NATIVE_LIBRARY SOURCE_PYTHON

After one ``READY`` line, stdin accepts these exact commands:

    HISTORY INSTRUMENT GENERATION price|all REPEATS EXPECTED_RECORDS
    DELTA_ORIGIN \
        INSTRUMENT GENERATION validate|price|all REPEATS EXPECTED_RECORDS
    DELTA_FROM_VERIFIED \
        INSTRUMENT GENERATION validate|price|all REPEATS EXPECTED_RECORDS
    ROLLING_ORIGIN \
        INSTRUMENT GENERATION count|price|all \
        REPEATS EXPECTED_RECORDS WINDOW_RECORDS
    ROLLING_FROM_VERIFIED \
        INSTRUMENT GENERATION count|price|all \
        REPEATS EXPECTED_RECORDS WINDOW_RECORDS
    START_HISTORY_LOOP \
        INSTRUMENT GENERATION price|all EXPECTED_RECORDS
    STOP_HISTORY_LOOP
    START_WORKER_DELTA_LOOP INSTRUMENT GENERATION EXPECTED_RECORDS
    STOP_WORKER_DELTA_LOOP
    WORKER_DELTA_ORIGIN \
        INSTRUMENT GENERATION validate|price REPEATS EXPECTED_RECORDS
    WORKER_DELTA_FROM_VERIFIED \
        INSTRUMENT GENERATION validate|price REPEATS EXPECTED_RECORDS
    LATEST_SERIES INSTRUMENT FIRST_INGRESS COUNT
    RAW_POLARS_BASELINE INSTRUMENT GENERATION
    RAW_POLARS_UPDATE \
        INSTRUMENT GENERATION REPEATS EXPECTED_RECORDS \
        EXPECTED_FIRST_INGRESS EXPECTED_LAST_INGRESS
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
import hashlib
import os
import platform
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
_EXPECTED_DERIVED_POLARS_TOTAL_RECORDS = 6


def _sha256_file(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _cpu_affinity_text() -> str:
    try:
        cpus = sorted(os.sched_getaffinity(0))
    except (AttributeError, OSError):
        return "unavailable"
    return ",".join(str(cpu) for cpu in cpus) + f";count={len(cpus)}"


def _raw_polars_frame(polars, frames: list[object]):
    _require(bool(frames), "raw Polars scan returned no data frames")
    if len(frames) == 1:
        return frames[0].rechunk()
    return polars.concat(frames, how="vertical", rechunk=True)


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
    _require(
        raw in ("validate", "price", "all"),
        "projection must be validate, price, or all",
    )
    return raw


def _factor_mode(raw: str) -> str:
    _require(
        raw in ("count", "price", "all"),
        "factor must be count, price, or all",
    )
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


@dataclass(slots=True)
class _TimedRollingFactor:
    mode: str
    update_ns: int = 0
    column_ns: int = 0
    math_ns: int = 0
    appended_records: int = 0
    evicted_records: int = 0

    def update(self, state, change):
        update_start_ns = _now_ns()
        appended_count = len(change.appended)
        evicted_count = len(change.evicted)
        column_start_ns = _now_ns()
        if self.mode == "count":
            appended_prices = ()
            evicted_prices = ()
        else:
            if self.mode == "all":
                change.appended.materialize_all()
                change.evicted.materialize_all()
            appended_prices = change.appended.read_columns(
                "price_p6"
            )["price_p6"]
            evicted_prices = change.evicted.read_columns(
                "price_p6"
            )["price_p6"]
        column_return_ns = _now_ns()

        math_start_ns = column_return_ns
        prior_count, prior_sum, prior_checksum = state
        next_count = (
            prior_count + appended_count - evicted_count
        )
        _require(
            next_count == len(change.after),
            "rolling factor count disagrees with the bounded window",
        )
        if self.mode == "count":
            next_sum = 0
            next_checksum = (
                (
                    prior_checksum * _CHECKSUM_PRIME
                )
                ^ appended_count
                ^ (evicted_count << 32)
            ) & _CHECKSUM_MASK
        else:
            next_sum = (
                prior_sum
                + sum(appended_prices)
                - sum(evicted_prices)
            )
            next_checksum, appended_values = _fold_column(
                prior_checksum,
                appended_prices,
                0x524F4C4C,
            )
            next_checksum, evicted_values = _fold_column(
                next_checksum,
                evicted_prices,
                0x45564943,
            )
            _require(
                appended_values == appended_count
                and evicted_values == evicted_count,
                "rolling factor price columns have the wrong size",
            )
        math_return_ns = _now_ns()
        self.appended_records += appended_count
        self.evicted_records += evicted_count
        self.column_ns += column_return_ns - column_start_ns
        self.math_ns += math_return_ns - math_start_ns
        self.update_ns += math_return_ns - update_start_ns
        next_state = (next_count, next_sum, next_checksum)
        return next_state, next_checksum


@dataclass(slots=True)
class _RollingLane:
    store: object
    factor: _TimedRollingFactor


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


class _WorkerDeltaLoop(_HistoryLoop):
    """Repeated delta scans with raw pages confined to another CPython."""

    def __init__(
        self,
        worker,
        *,
        instrument_id: int,
        expected_generation: int,
        expected_records: int,
    ) -> None:
        super().__init__(
            None,
            instrument_id=instrument_id,
            expected_generation=expected_generation,
            projection="price",
            expected_records=expected_records,
        )
        self._worker = worker
        self._thread = threading.Thread(
            target=self._run,
            name="l2flow-worker-delta-result-loop",
            daemon=False,
        )

    def _scan_once(self) -> _Scan:
        cursor = self._worker.open_instrument(
            self.instrument_id,
            expected_generation=self.expected_generation,
            requested_page_records=_PAGE_RECORDS,
        )
        _require(
            cursor.generation == self.expected_generation
            and cursor.expected_record_count
            == self.expected_records,
            "worker delta loop OPEN metadata differs",
        )
        scan_start_ns = _now_ns()
        records = 0
        pages = 0
        projected = 0
        checksum = _CHECKSUM_OFFSET
        with cursor:
            for batch in cursor.batches():
                pages += 1
                records += len(batch)
                prices = batch.read_columns("price_p6")[
                    "price_p6"
                ]
                count = len(prices)
                _require(
                    count == len(batch),
                    "worker result tuple has the wrong row count",
                )
                first = 0 if count == 0 else prices[0]
                last = 0 if count == 0 else prices[-1]
                _require(
                    isinstance(first, int)
                    and not isinstance(first, bool)
                    and isinstance(last, int)
                    and not isinstance(last, bool),
                    "worker result boundaries are not integers",
                )
                # Every selected value is materialized into an owned tuple.
                # The page bound keeps each main-GIL hold short; O(1)
                # boundary/count validation avoids adding an unrelated
                # Python factor loop to this column-construction workload.
                checksum ^= 0x574B444C
                checksum = (
                    checksum * _CHECKSUM_PRIME
                ) & _CHECKSUM_MASK
                checksum ^= count
                checksum = (
                    checksum * _CHECKSUM_PRIME
                ) & _CHECKSUM_MASK
                checksum ^= first & _CHECKSUM_MASK
                checksum = (
                    checksum * _CHECKSUM_PRIME
                ) & _CHECKSUM_MASK
                checksum ^= last & _CHECKSUM_MASK
                checksum &= _CHECKSUM_MASK
                projected += count
            checkpoint = cursor.verified_checkpoint
        complete_ns = _now_ns()
        _require(
            records == self.expected_records
            and projected == records
            and checkpoint.generation == self.expected_generation,
            "worker delta loop did not consume its complete target",
        )
        return _Scan(
            data_page_count=pages,
            terminal_page_count=1,
            mapping_bytes=records * 8,
            record_count=records,
            snapshot_count=0,
            tick_count=records,
            projected_value_count=projected,
            checksum=checksum,
            scan_start_ns=scan_start_ns,
            eof_columns_complete_ns=complete_ns,
        )


def _consume_delta_page(
    page,
    projection: str,
    checksum: int,
) -> tuple[int, int]:
    if projection == "validate":
        return checksum, 0
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
    _require(
        projection in ("price", "all"),
        "history loop projection must be price or all",
    )
    expected_records = _decimal(
        words[4],
        "expected_records",
        maximum=_UINT64_MAX,
        allow_zero=True,
    )
    return instrument_id, generation, projection, expected_records


def _parse_worker_delta_loop(
    words: list[str],
) -> tuple[int, int, int]:
    _require(
        len(words) == 4,
        "START_WORKER_DELTA_LOOP requires three arguments",
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
    expected_records = _decimal(
        words[3],
        "expected_records",
        maximum=_UINT64_MAX,
        allow_zero=True,
    )
    return instrument_id, generation, expected_records


def _parse_rolling_measurement(
    words: list[str],
) -> tuple[int, int, str, int, int, int]:
    _require(
        len(words) == 7,
        f"{words[0]} requires exactly six arguments",
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
    mode = _factor_mode(words[3])
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
    window_records = _decimal(
        words[6],
        "window_records",
        maximum=_UINT64_MAX,
        allow_zero=False,
    )
    return (
        instrument_id,
        generation,
        mode,
        repeats,
        expected_records,
        window_records,
    )


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
    _require(
        projection in ("price", "all"),
        "history projection must be price or all",
    )
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


def _run_worker_delta(
    worker,
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
    _require(
        projection in ("validate", "price"),
        "worker delta projection must be validate or price",
    )
    if origin:
        base_checkpoint = None
    else:
        _require(
            instrument_id in checkpoints,
            "WORKER_DELTA_FROM_VERIFIED has no checkpoint",
        )
        base_checkpoint = checkpoints[instrument_id]

    targets: list[object] = []
    for sample in range(repeats):
        open_start_ns = _now_ns()
        cursor = worker.open_instrument(
            instrument_id,
            base_checkpoint=base_checkpoint,
            requested_page_records=_PAGE_RECORDS,
            expected_generation=expected_generation,
        )
        open_return_ns = _now_ns()
        records = 0
        page_count = 0
        projected = 0
        checksum = _CHECKSUM_OFFSET
        selected_column_tuple_materialize_ns = 0
        summed_publish_to_validated_ready_ns = 0
        first_worker_read_ns = 0
        worker_page_read_ns = 0
        with cursor:
            scan_start_ns = _now_ns()
            for batch in cursor.batches():
                if first_worker_read_ns == 0:
                    first_worker_read_ns = (
                        batch.worker_read_start_ns
                    )
                worker_page_read_ns += (
                    batch.worker_read_return_ns
                    - batch.worker_read_start_ns
                )
                _require(
                    batch.result_ready_ns
                    >= batch.worker_publish_begin_ns,
                    "worker result notification precedes publication",
                )
                summed_publish_to_validated_ready_ns += (
                    batch.result_ready_ns
                    - batch.worker_ring_publish_return_ns
                )
                page_count += 1
                records += len(batch)
                if projection == "price":
                    column_start_ns = _now_ns()
                    values = batch.read_columns("price_p6")[
                        "price_p6"
                    ]
                    column_return_ns = _now_ns()
                    selected_column_tuple_materialize_ns += (
                        column_return_ns - column_start_ns
                    )
                    checksum, count = _fold_column(
                        checksum, values, 0x574B4450
                    )
                    projected += count
            checkpoint_start_ns = _now_ns()
            checkpoint = cursor.verified_checkpoint
            checkpoint_return_ns = _now_ns()
            summed_publish_to_validated_ready_ns += (
                cursor.complete_ready_ns
                - cursor.complete_ring_publish_return_ns
            )
            worker_pipeline_begin_ns = (
                cursor.eof_worker_read_start_ns
                if first_worker_read_ns == 0
                else first_worker_read_ns
            )
            worker_page_read_ns += (
                cursor.eof_worker_read_return_ns
                - cursor.eof_worker_read_start_ns
            )
            worker_pipeline_ns = (
                cursor.complete_ring_publish_return_ns
                - worker_pipeline_begin_ns
            )
        _require(
            records == expected_records
            and cursor.cumulative_record_count == expected_records,
            "worker delta result count differs from expected",
        )
        _require(
            (projection == "validate" and projected == 0)
            or (projection == "price" and projected == records),
            "worker delta projected count is inconsistent",
        )
        base_tick_count = (
            0
            if base_checkpoint is None
            else base_checkpoint.instrument_tick_record_count
        )
        _require(
            checkpoint.instrument_tick_record_count
            - base_tick_count
            == expected_records,
            "worker verified checkpoint delta is inconsistent",
        )
        published_ns = checkpoint.history_published_monotonic_ns
        _require(
            published_ns <= open_start_ns
            and scan_start_ns >= open_return_ns
            and checkpoint_return_ns >= scan_start_ns
            and worker_pipeline_begin_ns >= open_start_ns
            and cursor.complete_ring_publish_return_ns
            >= worker_pipeline_begin_ns
            and cursor.complete_ready_ns
            >= cursor.complete_ring_publish_return_ns
            and checkpoint_return_ns >= cursor.complete_ready_ns
            and worker_page_read_ns <= worker_pipeline_ns
            and selected_column_tuple_materialize_ns
            <= checkpoint_return_ns - scan_start_ns,
            "worker delta parent timestamps are non-monotonic",
        )
        targets.append(checkpoint)
        _emit(
            "WORKER_DELTA_SAMPLE",
            command=command_index,
            sample=sample,
            mode="origin" if origin else "verified",
            projection=projection,
            main_pid=os.getpid(),
            worker_pid=worker.pid,
            instrument_id=instrument_id,
            expected_generation=expected_generation,
            checkpoint_generation=checkpoint.generation,
            expected_records=expected_records,
            record_count=records,
            projected_value_count=projected,
            page_count=page_count,
            checksum=checksum,
            history_published_monotonic_ns=published_ns,
            open_call_start_ns=open_start_ns,
            open_return_ns=open_return_ns,
            cursor_open_call_start_ns=open_start_ns,
            cursor_open_return_ns=open_return_ns,
            scan_start_ns=scan_start_ns,
            checkpoint_return_ns=checkpoint_return_ns,
            worker_pipeline_begin_ns=worker_pipeline_begin_ns,
            worker_eof_read_return_ns=(
                cursor.eof_worker_read_return_ns
            ),
            worker_complete_publish_begin_ns=(
                cursor.complete_publish_begin_ns
            ),
            worker_complete_ring_publish_return_ns=(
                cursor.complete_ring_publish_return_ns
            ),
            parent_complete_ready_ns=cursor.complete_ready_ns,
            worker_page_read_ns=worker_page_read_ns,
            worker_pipeline_ns=worker_pipeline_ns,
            selected_column_tuple_materialize_ns=(
                selected_column_tuple_materialize_ns
            ),
            summed_ring_publish_to_validated_ready_ns=(
                summed_publish_to_validated_ready_ns
            ),
            parent_complete_consumption_ns=(
                checkpoint_return_ns - scan_start_ns
            ),
            checkpoint_access_ns=(
                checkpoint_return_ns - checkpoint_start_ns
            ),
            cursor_open_return_to_checkpoint_ns=(
                checkpoint_return_ns - open_return_ns
            ),
        )

    first_target = targets[0]
    _require(
        all(target == first_target for target in targets),
        "repeated worker deltas returned conflicting checkpoints",
    )
    checkpoints[instrument_id] = first_target
    _emit(
        "DONE",
        command=command_index,
        operation=(
            "WORKER_DELTA_ORIGIN"
            if origin
            else "WORKER_DELTA_FROM_VERIFIED"
        ),
        samples=repeats,
        worker_pid=worker.pid,
        saved_generation=first_target.generation,
        saved_tick_record_count=(
            first_target.instrument_tick_record_count
        ),
    )


def _run_rolling(
    client,
    rolling_lanes: dict[
        tuple[int, str, int], list[_RollingLane]
    ],
    rolling_store_class,
    *,
    command_index: int,
    origin: bool,
    instrument_id: int,
    expected_generation: int,
    factor_mode: str,
    repeats: int,
    expected_records: int,
    window_records: int,
) -> None:
    key = (instrument_id, factor_mode, window_records)
    if origin:
        _require(
            key not in rolling_lanes,
            "ROLLING_ORIGIN cannot replace existing rolling lanes",
        )
        lanes: list[_RollingLane] = []
        for _sample in range(repeats):
            factor = _TimedRollingFactor(factor_mode)
            lanes.append(
                _RollingLane(
                    store=rolling_store_class(
                        instrument_id,
                        window_records,
                        factor=factor,
                        initial_factor_state=(
                            0,
                            0,
                            _CHECKSUM_OFFSET,
                        ),
                    ),
                    factor=factor,
                )
            )
        rolling_lanes[key] = lanes
    else:
        _require(
            key in rolling_lanes,
            "ROLLING_FROM_VERIFIED has no initialized lanes",
        )
        lanes = rolling_lanes[key]
        _require(
            len(lanes) == repeats,
            "rolling repeat count changed after origin",
        )

    targets: list[object] = []
    for sample, lane in enumerate(lanes):
        before = lane.store.state
        base_checkpoint = before.checkpoint
        _require(
            (base_checkpoint is None) == origin,
            "rolling lane base kind disagrees with the command",
        )
        factor_update_before = lane.factor.update_ns
        factor_column_before = lane.factor.column_ns
        factor_math_before = lane.factor.math_ns
        appended_before = lane.factor.appended_records
        evicted_before = lane.factor.evicted_records

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
                begin_start_ns = _now_ns()
                transaction = lane.store.begin(cursor)
                begin_return_ns = _now_ns()
                with transaction:
                    consume_start_ns = _now_ns()
                    transaction.consume()
                    eof_verified_ns = _now_ns()
                    commit_start_ns = _now_ns()
                    commit = transaction.commit()
                    commit_return_ns = _now_ns()

        checkpoint = commit.checkpoint
        _require(
            checkpoint.generation == expected_generation,
            "rolling checkpoint generation is unexpected",
        )
        base_tick_count = (
            0
            if base_checkpoint is None
            else base_checkpoint.instrument_tick_record_count
        )
        _require(
            checkpoint.instrument_tick_record_count
            - base_tick_count
            == expected_records,
            "rolling checkpoint delta differs from expected_records",
        )
        _require(
            transaction.delta_record_count == expected_records,
            "rolling transaction count differs from expected_records",
        )
        expected_window_count = min(
            checkpoint.instrument_tick_record_count,
            window_records,
        )
        _require(
            len(commit.state.columns) == expected_window_count
            and commit.state.seen_count
            == checkpoint.instrument_tick_record_count,
            "rolling committed window/checkpoint counts disagree",
        )
        published_ns = checkpoint.history_published_monotonic_ns
        _require(
            published_ns <= session_open_return_ns,
            "rolling publication timestamp is after session OPEN",
        )

        factor_update_ns = (
            lane.factor.update_ns - factor_update_before
        )
        factor_column_ns = (
            lane.factor.column_ns - factor_column_before
        )
        factor_math_ns = lane.factor.math_ns - factor_math_before
        appended_records = (
            lane.factor.appended_records - appended_before
        )
        evicted_records = (
            lane.factor.evicted_records - evicted_before
        )
        consume_ns = eof_verified_ns - consume_start_ns
        _require(
            factor_update_ns <= consume_ns,
            "rolling factor time exceeds transaction consume time",
        )
        data_page_count = cursor.next_page_index - 1
        _require(
            data_page_count >= 0 and cursor.done,
            "rolling cursor did not consume one explicit EOF",
        )
        factor_value = (
            commit.state.factor_state[2]
            if commit.factor_value is None
            else commit.factor_value
        )
        expected_evicted_records = max(
            0,
            len(before.columns)
            + expected_records
            - window_records,
        )
        _require(
            appended_records == expected_records
            and evicted_records == expected_evicted_records,
            "rolling factor appended/evicted counts disagree",
        )
        targets.append(checkpoint)
        _emit(
            "ROLLING_SAMPLE",
            command=command_index,
            sample=sample,
            mode="origin" if origin else "verified",
            factor=factor_mode,
            instrument_id=instrument_id,
            window_records=window_records,
            base_generation=(
                0
                if base_checkpoint is None
                else base_checkpoint.generation
            ),
            expected_generation=expected_generation,
            checkpoint_generation=checkpoint.generation,
            expected_records=expected_records,
            record_count=expected_records,
            base_tick_record_count=base_tick_count,
            checkpoint_tick_record_count=(
                checkpoint.instrument_tick_record_count
            ),
            before_window_count=len(before.columns),
            after_window_count=len(commit.state.columns),
            appended_record_count=appended_records,
            evicted_record_count=evicted_records,
            page_count=cursor.next_page_index,
            data_page_count=data_page_count,
            terminal_page_count=1,
            history_published_monotonic_ns=published_ns,
            session_open_call_start_ns=session_open_start_ns,
            session_open_return_ns=session_open_return_ns,
            cursor_open_call_start_ns=cursor_open_start_ns,
            cursor_open_return_ns=cursor_open_return_ns,
            transaction_begin_start_ns=begin_start_ns,
            transaction_begin_return_ns=begin_return_ns,
            consume_start_ns=consume_start_ns,
            eof_verified_ns=eof_verified_ns,
            commit_start_ns=commit_start_ns,
            commit_return_ns=commit_return_ns,
            session_open_ns=(
                session_open_return_ns - session_open_start_ns
            ),
            cursor_open_ns=(
                cursor_open_return_ns - cursor_open_start_ns
            ),
            transaction_begin_ns=(
                begin_return_ns - begin_start_ns
            ),
            consume_to_eof_ns=consume_ns,
            atomic_commit_ns=(
                commit_return_ns - commit_start_ns
            ),
            factor_update_ns=factor_update_ns,
            factor_column_ns=factor_column_ns,
            factor_math_ns=factor_math_ns,
            consume_nonfactor_ns=consume_ns - factor_update_ns,
            cursor_open_return_to_commit_ns=(
                commit_return_ns - cursor_open_return_ns
            ),
            version=commit.state.version,
            advanced=commit.advanced,
            checksum=factor_value,
        )

    first_target = targets[0]
    _require(
        all(target == first_target for target in targets),
        "repeated rolling lanes returned conflicting checkpoints",
    )
    _emit(
        "DONE",
        command=command_index,
        operation=(
            "ROLLING_ORIGIN"
            if origin
            else "ROLLING_FROM_VERIFIED"
        ),
        samples=repeats,
        factor=factor_mode,
        window_records=window_records,
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


def _start_worker_delta_loop(
    worker,
    *,
    command_index: int,
    instrument_id: int,
    expected_generation: int,
    expected_records: int,
) -> _WorkerDeltaLoop:
    loop = _WorkerDeltaLoop(
        worker,
        instrument_id=instrument_id,
        expected_generation=expected_generation,
        expected_records=expected_records,
    )
    first = loop.start_and_wait_for_first_scan()
    try:
        _emit(
            "WORKER_DELTA_LOOP_STARTED",
            command=command_index,
            main_pid=os.getpid(),
            worker_pid=worker.pid,
            instrument_id=instrument_id,
            generation=expected_generation,
            expected_records=expected_records,
            requested_page_records=_PAGE_RECORDS,
            scans=first.scans,
            records=first.records,
            data_page_count=first.data_page_count,
            terminal_page_count=first.terminal_page_count,
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


def _stop_worker_delta_loop(
    loop: _WorkerDeltaLoop,
    *,
    command_index: int,
) -> None:
    stopped_ns = _now_ns()
    final = loop.stop_and_join()
    joined_ns = _now_ns()
    _require(
        final.scans >= 1,
        "worker delta loop stopped without one complete scan",
    )
    rate = (
        0
        if final.scan_ns == 0
        else final.records * 1_000_000_000 // final.scan_ns
    )
    _emit(
        "WORKER_DELTA_LOOP_STOPPED",
        command=command_index,
        main_pid=os.getpid(),
        worker_pid=loop._worker.pid,
        instrument_id=loop.instrument_id,
        generation=loop.expected_generation,
        expected_records=loop.expected_records,
        scans=final.scans,
        records=final.records,
        data_page_count=final.data_page_count,
        terminal_page_count=final.terminal_page_count,
        projected_value_count=final.projected_value_count,
        checksum=final.checksum,
        scan_ns=final.scan_ns,
        scan_records_per_second=rate,
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


def _run_raw_polars_baseline(
    raw_history,
    checkpoints: dict[int, object],
    *,
    command_index: int,
    instrument_id: int,
    expected_generation: int,
) -> None:
    start_ns = _now_ns()
    with raw_history.read_all(
        instrument_id,
        batch_records=_PAGE_RECORDS,
        expected_generation=expected_generation,
    ) as cursor:
        batches = 0
        records = 0
        for batch in cursor.batches():
            batches += 1
            records += len(batch)
        checkpoint = cursor.verified_checkpoint
    ready_ns = _now_ns()
    _require(records == 0, "raw Polars baseline is not empty")
    _require(batches == 0, "empty raw baseline returned a data batch")
    checkpoints[instrument_id] = checkpoint
    _emit(
        "RAW_POLARS_BASELINE",
        command=command_index,
        instrument_id=instrument_id,
        generation=checkpoint.generation,
        history_published_monotonic_ns=(
            checkpoint.history_published_monotonic_ns
        ),
        ready_ns=ready_ns,
        elapsed_ns=ready_ns - start_ns,
        records=records,
    )
    _emit(
        "DONE",
        command=command_index,
        operation="RAW_POLARS_BASELINE",
        samples=1,
    )


def _run_raw_polars_update(
    polars,
    raw_event_batch_frame,
    raw_history,
    checkpoints: dict[int, object],
    *,
    command_index: int,
    instrument_id: int,
    expected_generation: int,
    repeats: int,
    expected_records: int,
    expected_first_ingress: int,
    expected_last_ingress: int,
) -> None:
    _require(
        instrument_id in checkpoints,
        "raw Polars update has no verified baseline",
    )
    _require(
        expected_last_ingress >= expected_first_ingress
        and expected_last_ingress - expected_first_ingress + 1
        == expected_records,
        "raw Polars expected ingress interval is inconsistent",
    )
    base = checkpoints[instrument_id]
    target_checkpoint = None
    target_checksum = None
    for repeat in range(repeats):
        start_ns = _now_ns()
        frames: list[object] = []
        batch_count = 0
        with raw_history.read_updates(
            instrument_id,
            base,
            batch_records=_PAGE_RECORDS,
            expected_generation=expected_generation,
        ) as cursor:
            open_return_ns = _now_ns()
            for batch in cursor.batches():
                batch_count += 1
                frames.append(
                    raw_event_batch_frame(
                        batch,
                        columns=raw_history.raw_event_columns,
                    )
                )
            checkpoint = cursor.verified_checkpoint
        frame = _raw_polars_frame(polars, frames)
        row_count = frame.height
        column_count = frame.width
        ingress_sum = frame.select(
            polars.col("ingress_sequence").sum()
        ).item()
        unique_ingress = frame.select(
            polars.col("ingress_sequence").n_unique()
        ).item()
        first_recv_ns, last_recv_ns = frame.select(
            polars.col("recv_monotonic_ns").min().alias("first_recv"),
            polars.col("recv_monotonic_ns").max().alias("last_recv"),
        ).row(0)
        first_ingress, last_ingress = frame.select(
            polars.col("ingress_sequence").min().alias("first_ingress"),
            polars.col("ingress_sequence").max().alias("last_ingress"),
        ).row(0)
        ready_ns = _now_ns()
        _require(
            row_count == expected_records,
            "raw Polars row count differs from expected",
        )
        _require(
            column_count == len(raw_history.raw_event_columns),
            "raw Polars column count differs from selected schema",
        )
        _require(
            isinstance(ingress_sum, int)
            and isinstance(unique_ingress, int)
            and isinstance(first_recv_ns, int)
            and isinstance(last_recv_ns, int)
            and isinstance(first_ingress, int)
            and isinstance(last_ingress, int),
            "raw Polars aggregate returned a non-integer",
        )
        _require(
            unique_ingress == expected_records,
            "raw Polars ingress sequence is not unique",
        )
        _require(
            first_ingress == expected_first_ingress
            and last_ingress == expected_last_ingress,
            "raw Polars ingress interval differs from the offered interval",
        )
        expected_ingress_sum = (
            (expected_first_ingress + expected_last_ingress)
            * expected_records
            // 2
        )
        _require(
            ingress_sum == expected_ingress_sum,
            "raw Polars ingress interval is not complete",
        )
        _require(
            isinstance(checkpoint.history_published_monotonic_ns, int)
            and 0 < first_recv_ns <= last_recv_ns
            <= checkpoint.history_published_monotonic_ns
            <= ready_ns,
            "raw callback/publication/Polars timestamps are non-monotonic",
        )
        if target_checkpoint is None:
            target_checkpoint = checkpoint
            target_checksum = ingress_sum
        else:
            _require(
                checkpoint == target_checkpoint
                and ingress_sum == target_checksum,
                "repeated raw Polars reads disagree",
            )
        _emit(
            "RAW_POLARS_SAMPLE",
            command=command_index,
            sample=repeat,
            instrument_id=instrument_id,
            generation=checkpoint.generation,
            records=row_count,
            columns=column_count,
            batches=batch_count,
            first_ingress=first_ingress,
            last_ingress=last_ingress,
            unique_ingress=unique_ingress,
            ingress_sum=ingress_sum,
            first_callback_entry_ns=first_recv_ns,
            last_callback_entry_ns=last_recv_ns,
            history_published_monotonic_ns=(
                checkpoint.history_published_monotonic_ns
            ),
            open_return_ns=open_return_ns,
            polars_ready_ns=ready_ns,
            open_to_polars_ns=ready_ns - open_return_ns,
            publication_to_polars_ns=(
                ready_ns
                - checkpoint.history_published_monotonic_ns
            ),
            first_callback_to_polars_ns=ready_ns - first_recv_ns,
            last_callback_to_polars_ns=ready_ns - last_recv_ns,
            elapsed_ns=ready_ns - start_ns,
            dataframe_estimated_bytes=frame.estimated_size(),
        )
    _require(target_checkpoint is not None, "raw Polars produced no sample")
    checkpoints[instrument_id] = target_checkpoint
    _emit(
        "DONE",
        command=command_index,
        operation="RAW_POLARS_UPDATE",
        samples=repeats,
    )


def _run_derived_polars(
    polars,
    derived_event_batch_frame,
    reader,
    derived_event_kind,
    revision_operation,
    order_finality,
    *,
    command_index: int,
    expected_generation: int,
    expected_events: int,
    expected_order_id: int,
) -> None:
    start_ns = _now_ns()
    frames: list[object] = []
    batch_count = 0
    with reader.read_all(
        expected_generation=expected_generation
    ) as cursor:
        for batch in cursor.batches():
            batch_count += 1
            frames.append(derived_event_batch_frame(batch))
        checkpoint = cursor.verified_checkpoint
    frame = _raw_polars_frame(polars, frames)
    relevant = frame.filter(
        (
            (
                polars.col("event_kind")
                == int(derived_event_kind.ORDER_REVISION)
            )
            & (polars.col("order_id") == expected_order_id)
        )
        | (
            (polars.col("event_kind") == int(derived_event_kind.TRADE))
            & (polars.col("buy_order_id") == expected_order_id)
        )
    ).sort("derived_event_sequence")
    sequence_values = relevant.get_column(
        "derived_event_sequence"
    ).to_list()
    revisions = (
        frame.filter(
            (polars.col("event_kind")
             == int(derived_event_kind.ORDER_REVISION))
            & (polars.col("order_id") == expected_order_id)
        )
        .sort("revision")
        .select(
            "revision",
            "operation",
            "finality",
            "remaining_quantity_valid",
            "remaining_quantity",
            "quality_flags",
            "source_quality_flags",
            "source_matched_quantity",
            "observed_pre_add_trade_quantity",
        )
        .to_dicts()
    )
    ready_ns = _now_ns()
    _require(
        frame.height == _EXPECTED_DERIVED_POLARS_TOTAL_RECORDS
        and relevant.height == expected_events,
        "derived Polars event count differs from expected: "
        f"frame={frame.height} relevant={relevant.height} "
        f"kinds={','.join(str(value) for value in frame['event_kind'])} "
        "revisions="
        + ",".join(
            f"{row['revision']}:{row['operation']}:{row['finality']}:"
            f"{row['remaining_quantity']}"
            for row in revisions
        ),
    )
    _require(bool(sequence_values), "derived Polars sequence is empty")
    _require(
        sequence_values
        == list(
            range(
                sequence_values[0],
                sequence_values[0] + expected_events,
            )
        ),
        "derived Polars sequence is not dense",
    )
    _require(
        [row["revision"] for row in revisions] == [1, 2, 3],
        "derived order revision sequence is not 1,2,3",
    )
    final_revision = revisions[-1]
    _require(
        final_revision["operation"]
        == int(revision_operation.FINALIZE)
        and final_revision["finality"] == int(order_finality.FINAL)
        and final_revision["remaining_quantity_valid"]
        and final_revision["remaining_quantity"] == 0,
        "derived order sequence does not end in a clean zero-balance "
        "finalization: "
        f"operation={final_revision['operation']} "
        f"finality={final_revision['finality']} "
        f"remaining={final_revision['remaining_quantity']} "
        f"quality={final_revision['quality_flags']} "
        f"source_quality={final_revision['source_quality_flags']} "
        f"matched={final_revision['source_matched_quantity']} "
        f"observed_pre_add="
        f"{final_revision['observed_pre_add_trade_quantity']}",
    )
    first_recv_ns, last_recv_ns = relevant.select(
        polars.col("recv_monotonic_ns").min().alias("first_recv"),
        polars.col("recv_monotonic_ns").max().alias("last_recv"),
    ).row(0)
    published_ns = checkpoint.raw_checkpoint.history_published_monotonic_ns
    _require(
        isinstance(first_recv_ns, int)
        and isinstance(last_recv_ns, int)
        and 0 < first_recv_ns <= last_recv_ns <= published_ns <= ready_ns,
        "derived callback/publication/Polars timestamps are non-monotonic",
    )
    _emit(
        "DERIVED_POLARS_SAMPLE",
        command=command_index,
        instrument_id=reader.instrument_id,
        generation=checkpoint.raw_checkpoint.generation,
        records=frame.height,
        order_sequence_records=relevant.height,
        columns=frame.width,
        batches=batch_count,
        order_id=expected_order_id,
        order_revision_count=len(revisions),
        first_derived_sequence=sequence_values[0],
        last_derived_sequence=sequence_values[-1],
        first_callback_entry_ns=first_recv_ns,
        last_callback_entry_ns=last_recv_ns,
        history_published_monotonic_ns=published_ns,
        polars_ready_ns=ready_ns,
        publication_to_polars_ns=ready_ns - published_ns,
        first_callback_to_polars_ns=ready_ns - first_recv_ns,
        last_callback_to_polars_ns=ready_ns - last_recv_ns,
        elapsed_ns=ready_ns - start_ns,
        dataframe_estimated_bytes=frame.estimated_size(),
        final_revision=final_revision["revision"],
        final_remaining_quantity=final_revision[
            "remaining_quantity"
        ],
    )
    _emit(
        "DONE",
        command=command_index,
        operation="DERIVED_POLARS",
        samples=1,
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

    import polars as pl  # pylint: disable=import-outside-toplevel

    from l2flow_realtime import (  # pylint: disable=import-outside-toplevel
        InconsistentReadError,
        INSTRUMENT_RAW_EVENT_COLUMNS,
        InstrumentDerivedEventKind,
        InstrumentOrderFinality,
        InstrumentOrderRevisionOperation,
        InstrumentTickRollingStore,
        L2FlowClient,
        LatestStatus,
    )
    from l2flow_realtime.polars import (  # pylint: disable=import-outside-toplevel
        derived_event_batch_frame,
        raw_event_batch_frame,
    )

    checkpoints: dict[int, object] = {}
    worker_checkpoints: dict[int, object] = {}
    raw_polars_checkpoints: dict[int, object] = {}
    derived_readers: dict[int, object] = {}
    rolling_lanes: dict[
        tuple[int, str, int], list[_RollingLane]
    ] = {}
    with L2FlowClient.connect(
        control_socket,
        native_library=native_library,
        timeout=5.0,
        stale_after_ns=None,
    ) as client:
        delta_worker = client.open_instrument_tick_delta_worker(
            result_columns=("price_p6",),
            ring_slots=4,
            result_batch_records=_PAGE_RECORDS,
        )
        raw_history = client.open_instrument_raw_event_history(
            raw_event_columns=INSTRUMENT_RAW_EVENT_COLUMNS,
            ring_slots=4,
            batch_capacity=_PAGE_RECORDS,
        )
        session = client.session_info()
        clock = time.get_clock_info("monotonic")
        _emit(
            "READY",
            protocol="wire_v2_history_latency_2",
            run_id=session.run_id.hex(),
            session_epoch=session.session_epoch,
            trade_date=session.trade_date,
            server_state=session.server_state.name,
            coverage_from_open=int(session.coverage_from_open),
            capacity=session.capacity,
            bound_count=session.bound_count,
            monotonic_implementation=(
                clock.implementation.replace(" ", "_")
            ),
            monotonic_resolution_ns=max(
                1, round(clock.resolution * 1_000_000_000)
            ),
            gc_enabled=gc.isenabled(),
            main_pid=os.getpid(),
            worker_pid=delta_worker.pid,
            worker_ring_slots=delta_worker.ring_slots,
            worker_result_batch_records=(
                delta_worker.result_batch_records
            ),
            raw_polars_worker_pid=raw_history.worker_pid,
            raw_polars_columns=len(raw_history.raw_event_columns),
            python_version=platform.python_version(),
            python_implementation=platform.python_implementation(),
            python_affinity=_cpu_affinity_text(),
            python_executable_sha256=_sha256_file(sys.executable),
            native_library_sha256=_sha256_file(native_library),
            probe_sha256=_sha256_file(__file__),
            polars_version=pl.__version__,
        )

        command_index = 0
        history_loop: Optional[_HistoryLoop] = None
        worker_loop: Optional[_WorkerDeltaLoop] = None
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
                        history_loop is None and worker_loop is None,
                        "all scan loops must stop before QUIT",
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
                if operation == "STOP_WORKER_DELTA_LOOP":
                    _require(
                        len(words) == 1,
                        "STOP_WORKER_DELTA_LOOP takes no arguments",
                    )
                    _require(
                        worker_loop is not None,
                        "no worker delta loop is running",
                    )
                    command_index += 1
                    _stop_worker_delta_loop(
                        worker_loop,
                        command_index=command_index,
                    )
                    worker_loop = None
                    continue
                if history_loop is not None:
                    history_loop.ensure_healthy()
                if worker_loop is not None:
                    worker_loop.ensure_healthy()
                if operation == "START_HISTORY_LOOP":
                    _require(
                        history_loop is None and worker_loop is None,
                        "a scan loop is already running",
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
                if operation == "START_WORKER_DELTA_LOOP":
                    _require(
                        history_loop is None and worker_loop is None,
                        "a scan loop is already running",
                    )
                    (
                        instrument_id,
                        generation,
                        expected_records,
                    ) = _parse_worker_delta_loop(words)
                    command_index += 1
                    worker_loop = _start_worker_delta_loop(
                        delta_worker,
                        command_index=command_index,
                        instrument_id=instrument_id,
                        expected_generation=generation,
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
                        (
                            history_loop
                            if history_loop is not None
                            else worker_loop
                        ),
                        command_index=command_index,
                        instrument_id=instrument_id,
                        first_ingress=first_ingress,
                        count=count,
                    )
                    continue
                if operation == "PREPARE_DERIVED_POLARS":
                    _require(
                        len(words) == 2,
                        "PREPARE_DERIVED_POLARS requires one argument",
                    )
                    instrument_id = _decimal(
                        words[1],
                        "instrument_id",
                        maximum=_UINT32_MAX,
                        allow_zero=False,
                    )
                    _require(
                        instrument_id not in derived_readers,
                        "derived Polars reader is already prepared",
                    )
                    command_index += 1
                    reader = client.open_instrument_derived_event_history(
                        instrument_id,
                        maximum_order_states=100,
                        page_records=_PAGE_RECORDS,
                    )
                    derived_readers[instrument_id] = reader
                    _emit(
                        "DERIVED_POLARS_PREPARED",
                        command=command_index,
                        instrument_id=instrument_id,
                        main_pid=os.getpid(),
                    )
                    _emit(
                        "DONE",
                        command=command_index,
                        operation="PREPARE_DERIVED_POLARS",
                        samples=1,
                    )
                    continue
                if operation == "RAW_POLARS_BASELINE":
                    _require(
                        len(words) == 3,
                        "RAW_POLARS_BASELINE requires two arguments",
                    )
                    instrument_id = _decimal(
                        words[1],
                        "instrument_id",
                        maximum=_UINT32_MAX,
                        allow_zero=False,
                    )
                    generation = _decimal(
                        words[2],
                        "generation",
                        maximum=_UINT64_MAX,
                        allow_zero=False,
                    )
                    command_index += 1
                    _run_raw_polars_baseline(
                        raw_history,
                        raw_polars_checkpoints,
                        command_index=command_index,
                        instrument_id=instrument_id,
                        expected_generation=generation,
                    )
                    continue
                if operation == "RAW_POLARS_UPDATE":
                    _require(
                        len(words) == 7,
                        "RAW_POLARS_UPDATE requires six arguments",
                    )
                    instrument_id = _decimal(
                        words[1],
                        "instrument_id",
                        maximum=_UINT32_MAX,
                        allow_zero=False,
                    )
                    generation = _decimal(
                        words[2],
                        "generation",
                        maximum=_UINT64_MAX,
                        allow_zero=False,
                    )
                    repeats = _decimal(
                        words[3],
                        "repeats",
                        maximum=_UINT32_MAX,
                        allow_zero=False,
                    )
                    expected_records = _decimal(
                        words[4],
                        "expected_records",
                        maximum=_UINT64_MAX,
                        allow_zero=False,
                    )
                    expected_first_ingress = _decimal(
                        words[5],
                        "expected_first_ingress",
                        maximum=_UINT64_MAX,
                        allow_zero=False,
                    )
                    expected_last_ingress = _decimal(
                        words[6],
                        "expected_last_ingress",
                        maximum=_UINT64_MAX,
                        allow_zero=False,
                    )
                    command_index += 1
                    _run_raw_polars_update(
                        pl,
                        raw_event_batch_frame,
                        raw_history,
                        raw_polars_checkpoints,
                        command_index=command_index,
                        instrument_id=instrument_id,
                        expected_generation=generation,
                        repeats=repeats,
                        expected_records=expected_records,
                        expected_first_ingress=expected_first_ingress,
                        expected_last_ingress=expected_last_ingress,
                    )
                    continue
                if operation == "DERIVED_POLARS":
                    _require(
                        len(words) == 5,
                        "DERIVED_POLARS requires four arguments",
                    )
                    instrument_id = _decimal(
                        words[1],
                        "instrument_id",
                        maximum=_UINT32_MAX,
                        allow_zero=False,
                    )
                    generation = _decimal(
                        words[2],
                        "generation",
                        maximum=_UINT64_MAX,
                        allow_zero=False,
                    )
                    expected_events = _decimal(
                        words[3],
                        "expected_events",
                        maximum=_UINT64_MAX,
                        allow_zero=False,
                    )
                    expected_order_id = _decimal(
                        words[4],
                        "expected_order_id",
                        maximum=_UINT64_MAX,
                        allow_zero=False,
                    )
                    _require(
                        instrument_id in derived_readers,
                        "derived Polars reader was not prepared",
                    )
                    command_index += 1
                    _run_derived_polars(
                        pl,
                        derived_event_batch_frame,
                        derived_readers[instrument_id],
                        InstrumentDerivedEventKind,
                        InstrumentOrderRevisionOperation,
                        InstrumentOrderFinality,
                        command_index=command_index,
                        expected_generation=generation,
                        expected_events=expected_events,
                        expected_order_id=expected_order_id,
                    )
                    continue
                _require(
                    operation
                    in (
                        "HISTORY",
                        "DELTA_ORIGIN",
                        "DELTA_FROM_VERIFIED",
                        "ROLLING_ORIGIN",
                        "ROLLING_FROM_VERIFIED",
                        "WORKER_DELTA_ORIGIN",
                        "WORKER_DELTA_FROM_VERIFIED",
                    ),
                    "unknown probe command",
                )
                command_index += 1
                if operation.startswith("WORKER_DELTA_"):
                    (
                        instrument_id,
                        generation,
                        projection,
                        repeats,
                        expected_records,
                    ) = _parse_measurement(words)
                    _run_worker_delta(
                        delta_worker,
                        worker_checkpoints,
                        command_index=command_index,
                        origin=(
                            operation == "WORKER_DELTA_ORIGIN"
                        ),
                        instrument_id=instrument_id,
                        expected_generation=generation,
                        projection=projection,
                        repeats=repeats,
                        expected_records=expected_records,
                    )
                    continue
                if operation.startswith("ROLLING_"):
                    (
                        instrument_id,
                        generation,
                        factor_mode,
                        repeats,
                        expected_records,
                        window_records,
                    ) = _parse_rolling_measurement(words)
                    _run_rolling(
                        client,
                        rolling_lanes,
                        InstrumentTickRollingStore,
                        command_index=command_index,
                        origin=operation == "ROLLING_ORIGIN",
                        instrument_id=instrument_id,
                        expected_generation=generation,
                        factor_mode=factor_mode,
                        repeats=repeats,
                        expected_records=expected_records,
                        window_records=window_records,
                    )
                    continue
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
            if worker_loop is not None:
                try:
                    worker_loop.stop_and_join()
                except BaseException:
                    pass
            for reader in derived_readers.values():
                try:
                    reader.close()
                except BaseException:
                    pass
            raw_history.close()
            delta_worker.close()
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
