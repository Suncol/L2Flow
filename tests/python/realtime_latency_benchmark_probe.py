"""Process-level Python read and callback-to-consumption latency probe.

Reported quantiles use nearest rank: for N sorted samples and quantile q, the
selected element is ``ceil(q * N) - 1`` (clamped to the valid sample range).
No sample is adjusted by the separately reported clock-pair baseline.
"""

from __future__ import annotations

import gc
import math
import os
import sys
import time
from typing import Callable, Iterable, Sequence


_UINT64_MAX = (1 << 64) - 1
_CONNECT_WARMUP_SAMPLES = 10
_CONNECT_MEASURED_SAMPLES = 100
_STATIC_WARMUP_CALLS = 20_000
_STATIC_MEASURED_CALLS = 100_000
_WORKING_SET_STRIDE = 7_919
_WORKING_SET_MIN_WARMUP_CALLS = 60_000
_BATCH_MIN_WARMUP_RECORDS = 60_000
_BATCH_MIN_MEASURED_RECORDS = 100_000
_DISTINCT_BATCH_SIZES = (8, 32, 128)
_BURST_TIMEOUT_NS = 30_000_000_000


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def _positive_decimal(raw: str, field: str, maximum: int) -> int:
    _require(bool(raw) and raw.isascii() and raw.isdecimal(), f"{field} must be decimal")
    value = int(raw, 10)
    _require(0 < value <= maximum, f"{field} is outside its valid range")
    return value


def _nonnegative_decimal(raw: str, field: str) -> int:
    _require(bool(raw) and raw.isascii() and raw.isdecimal(), f"{field} must be decimal")
    value = int(raw, 10)
    _require(value >= 0, f"{field} must be nonnegative")
    return value


def _now_ns() -> int:
    return time.clock_gettime_ns(time.CLOCK_MONOTONIC)


def _nearest_rank(sorted_values: Sequence[int], quantile: float) -> int:
    _require(bool(sorted_values), "latency sample set is empty")
    rank = max(1, min(len(sorted_values), math.ceil(quantile * len(sorted_values))))
    return sorted_values[rank - 1]


def _stats_fields(prefix: str, values: Iterable[int]) -> list[str]:
    samples = list(values)
    _require(bool(samples), f"{prefix or 'latency'} sample set is empty")
    _require(all(value >= 0 for value in samples), "negative latency sample")
    ordered = sorted(samples)
    stem = f"{prefix}_" if prefix else ""
    return [
        f"{stem}samples={len(ordered)}",
        f"{stem}min_ns={ordered[0]}",
        f"{stem}mean_ns={sum(ordered) // len(ordered)}",
        f"{stem}p50_ns={_nearest_rank(ordered, 0.50)}",
        f"{stem}p90_ns={_nearest_rank(ordered, 0.90)}",
        f"{stem}p95_ns={_nearest_rank(ordered, 0.95)}",
        f"{stem}p99_ns={_nearest_rank(ordered, 0.99)}",
        f"{stem}p999_ns={_nearest_rank(ordered, 0.999)}",
        f"{stem}max_ns={ordered[-1]}",
    ]


def _snapshot_sequence(snapshot, latest_status, instrument_id: int) -> int:
    _require(snapshot.status is latest_status.AVAILABLE, "snapshot is not available")
    _require(snapshot.instrument_id == instrument_id, "snapshot instrument ID changed")
    _require(snapshot.common is not None, "available snapshot has no common record")
    _require(
        snapshot.common.instrument_id == instrument_id,
        "snapshot common instrument ID changed",
    )
    sequence = snapshot.common.ingress_sequence
    _require(0 < sequence <= _UINT64_MAX, "snapshot ingress sequence is invalid")
    return sequence


def _validate_snapshot(snapshot, latest_status, instrument_id: int, sequence: int) -> None:
    _require(
        _snapshot_sequence(snapshot, latest_status, instrument_id) == sequence,
        "static snapshot changed during hot-read benchmark",
    )


def _measure_calls(
    operation: Callable[[], object],
    validate: Callable[[object], None],
    warmup_calls: int,
    measured_calls: int,
) -> list[int]:
    for _ in range(warmup_calls):
        validate(operation())
    latencies: list[int] = []
    append = latencies.append
    for _ in range(measured_calls):
        begin = _now_ns()
        result = operation()
        end = _now_ns()
        validate(result)
        append(end - begin)
    return latencies


def _measure_clock_pair(warmup_calls: int, measured_calls: int) -> list[int]:
    for _ in range(warmup_calls):
        _now_ns()
        _now_ns()
    samples: list[int] = []
    append = samples.append
    for _ in range(measured_calls):
        begin = _now_ns()
        end = _now_ns()
        append(end - begin)
    return samples


def _affinity_text(cpus: Iterable[int]) -> str:
    values = sorted(cpus)
    _require(bool(values), "process CPU affinity is empty")
    return ",".join(str(cpu) for cpu in values)


def _emit_environment() -> None:
    requested_cpu = os.environ.get("L2FLOW_PYTHON_CPU")
    if requested_cpu is not None:
        cpu = _nonnegative_decimal(requested_cpu, "L2FLOW_PYTHON_CPU")
        os.sched_setaffinity(0, {cpu})
    affinity = os.sched_getaffinity(0)
    clock = time.get_clock_info("monotonic")
    print(
        "ENV "
        f"affinity={_affinity_text(affinity)} "
        f"python={sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro} "
        f"monotonic_implementation={clock.implementation.replace(' ', '_')} "
        f"monotonic_resolution_ns={max(1, round(clock.resolution * 1_000_000_000))} "
        f"gc_enabled={str(gc.isenabled()).lower()} "
        f"gc_thresholds={','.join(str(value) for value in gc.get_threshold())}",
        flush=True,
    )


def _validate_active_universe(session, active_count: int) -> None:
    _require(
        session.capacity >= active_count,
        "Wire V2 capacity is smaller than the requested active working set",
    )
    for field in (
        "bound_count",
        "available_count",
        "snapshot_available_count",
        "factor_eligible_count",
    ):
        _require(
            getattr(session, field) == active_count,
            f"session {field} does not equal active_count",
        )
    _require(
        session.tick_available_count == 0,
        "snapshot-only latency fixture unexpectedly has tick availability",
    )


def _benchmark_connections(
    client_type,
    control_socket: str,
    native_reader: str,
    validated_session,
    active_count: int,
) -> None:
    def connect_once(measured: bool) -> tuple[int, int, int] | None:
        total_begin = _now_ns()
        client = client_type.connect(
            control_socket,
            native_library=native_reader,
            timeout=2.0,
        )
        connected = _now_ns()
        close_begin = _now_ns()
        client.close()
        closed = _now_ns()
        if not measured:
            return None
        return connected - total_begin, closed - close_begin, closed - total_begin

    for _ in range(_CONNECT_WARMUP_SAMPLES):
        connect_once(False)
    connect_samples: list[int] = []
    close_samples: list[int] = []
    total_samples: list[int] = []
    for _ in range(_CONNECT_MEASURED_SAMPLES):
        result = connect_once(True)
        _require(result is not None, "measured connection produced no timings")
        connect_ns, close_ns, total_ns = result
        connect_samples.append(connect_ns)
        close_samples.append(close_ns)
        total_samples.append(total_ns)
    fields = [
        f"warmup_samples={_CONNECT_WARMUP_SAMPLES}",
        "scope=control_scm_rights_mmap_and_bound_identity_prefix_validation",
        f"active_count={active_count}",
        f"capacity={validated_session.capacity}",
        f"bound_count={validated_session.bound_count}",
        f"available_count={validated_session.available_count}",
        "snapshot_available_count="
        f"{validated_session.snapshot_available_count}",
        f"tick_available_count={validated_session.tick_available_count}",
        f"factor_eligible_count={validated_session.factor_eligible_count}",
        "quantile=nearest_rank",
    ]
    fields.extend(_stats_fields("connect", connect_samples))
    fields.extend(_stats_fields("close", close_samples))
    fields.extend(_stats_fields("total", total_samples))
    print("CONNECT " + " ".join(fields), flush=True)


def _permutation_ids(count: int, active_count: int, offset: int = 0) -> tuple[int, ...]:
    return tuple(
        (((offset + index) * _WORKING_SET_STRIDE) % active_count) + 1
        for index in range(count)
    )


def _static_point_latencies(
    operation: Callable[[], object],
    latest_status,
    instrument_id: int,
    expected_sequence: int,
    warmup_calls: int,
    measured_calls: int,
) -> list[int]:
    return _measure_calls(
        operation,
        lambda value: _validate_snapshot(
            value, latest_status, instrument_id, expected_sequence
        ),
        warmup_calls,
        measured_calls,
    )


def _point_fields(
    latencies: Sequence[int],
    instrument_id: int,
    warmup_calls: int,
) -> list[str]:
    fields = [
        f"instrument_id={instrument_id}",
        f"warmup_calls={warmup_calls}",
        "quantile=nearest_rank",
    ]
    fields.extend(_stats_fields("", latencies))
    return fields


def _run_static_benchmarks(
    client,
    staleness_disabled_client,
    latest_status,
    e2e_instrument_id: int,
    active_count: int,
    requested_warmup: int,
    requested_samples: int,
) -> int:
    warmup_calls = max(_STATIC_WARMUP_CALLS, requested_warmup)
    measured_calls = max(_STATIC_MEASURED_CALLS, requested_samples)
    head_id = 1
    tail_id = active_count
    head_sequence = _snapshot_sequence(
        client.latest_snapshot(head_id), latest_status, head_id
    )
    tail_sequence = _snapshot_sequence(
        client.latest_snapshot(tail_id), latest_status, tail_id
    )

    head_public = _static_point_latencies(
        lambda: client.latest_snapshot(head_id),
        latest_status,
        head_id,
        head_sequence,
        warmup_calls,
        measured_calls,
    )
    tail_public = _static_point_latencies(
        lambda: client.latest_snapshot(tail_id),
        latest_status,
        tail_id,
        tail_sequence,
        warmup_calls,
        measured_calls,
    )
    clock_pair = _measure_clock_pair(warmup_calls, measured_calls)
    throughput_begin = _now_ns()
    for _ in range(measured_calls):
        _validate_snapshot(
            client.latest_snapshot(head_id),
            latest_status,
            head_id,
            head_sequence,
        )
    throughput_elapsed = _now_ns() - throughput_begin
    _require(throughput_elapsed > 0, "validated throughput duration is zero")

    head_fields = _point_fields(head_public, head_id, warmup_calls)
    head_fields.extend(_stats_fields("timer_pair", clock_pair))
    head_fields.extend(
        (
            f"validated_throughput_calls={measured_calls}",
            f"validated_throughput_elapsed_ns={throughput_elapsed}",
            "validated_throughput_calls_per_second="
            f"{(measured_calls * 1_000_000_000) // throughput_elapsed}",
            "validated_throughput_mean_ns="
            f"{throughput_elapsed // measured_calls}",
            "access_pattern=repeated_id_cache_floor",
        )
    )
    print("HEAD_PUBLIC " + " ".join(head_fields), flush=True)
    print(
        "TAIL_PUBLIC "
        + " ".join(_point_fields(tail_public, tail_id, warmup_calls))
        + " access_pattern=repeated_id_cache_floor",
        flush=True,
    )

    native_head = _static_point_latencies(
        lambda: client._native.latest_snapshots((head_id,))[0],
        latest_status,
        head_id,
        head_sequence,
        warmup_calls,
        measured_calls,
    )
    native_tail = _static_point_latencies(
        lambda: client._native.latest_snapshots((tail_id,))[0],
        latest_status,
        tail_id,
        tail_sequence,
        warmup_calls,
        measured_calls,
    )
    native_fields = [
        "scope=python_ctypes_native_reader_wrapper_and_wire_parse",
        "quantile=nearest_rank",
    ]
    native_fields.extend(_stats_fields("head", native_head))
    native_fields.extend(_stats_fields("tail", native_tail))
    print(
        "PYTHON_NATIVE_WRAPPER " + " ".join(native_fields),
        flush=True,
    )

    no_staleness_head = _static_point_latencies(
        lambda: staleness_disabled_client.latest_snapshot(head_id),
        latest_status,
        head_id,
        head_sequence,
        warmup_calls,
        measured_calls,
    )
    no_staleness_tail = _static_point_latencies(
        lambda: staleness_disabled_client.latest_snapshot(tail_id),
        latest_status,
        tail_id,
        tail_sequence,
        warmup_calls,
        measured_calls,
    )
    no_staleness_fields = [
        "scope=public_client_stale_after_ns_none",
        "native_state_and_coverage_checks=enabled",
        "quantile=nearest_rank",
    ]
    no_staleness_fields.extend(_stats_fields("head", no_staleness_head))
    no_staleness_fields.extend(_stats_fields("tail", no_staleness_tail))
    print(
        "PUBLIC_STALENESS_DISABLED " + " ".join(no_staleness_fields),
        flush=True,
    )

    working_warmup_calls = max(
        _WORKING_SET_MIN_WARMUP_CALLS, active_count, requested_warmup
    )
    working_measured_calls = max(_STATIC_MEASURED_CALLS, requested_samples)
    warmup_ids = _permutation_ids(working_warmup_calls, active_count)
    measured_ids = _permutation_ids(
        working_measured_calls, active_count, working_warmup_calls
    )
    expected_sequences = [0] * (active_count + 1)
    for instrument_id in warmup_ids:
        sequence = _snapshot_sequence(
            client.latest_snapshot(instrument_id),
            latest_status,
            instrument_id,
        )
        expected = expected_sequences[instrument_id]
        if expected == 0:
            expected_sequences[instrument_id] = sequence
        else:
            _require(
                sequence == expected,
                "working-set snapshot changed during warmup",
            )
    _require(
        all(expected_sequences[1:]),
        "working-set warmup did not visit every active instrument",
    )

    working_set_latencies: list[int] = []
    append_working = working_set_latencies.append
    for instrument_id in measured_ids:
        begin = _now_ns()
        snapshot = client.latest_snapshot(instrument_id)
        end = _now_ns()
        _validate_snapshot(
            snapshot,
            latest_status,
            instrument_id,
            expected_sequences[instrument_id],
        )
        append_working(end - begin)
    working_fields = [
        f"active_count={active_count}",
        f"stride={_WORKING_SET_STRIDE}",
        f"warmup_calls={working_warmup_calls}",
        f"measured_distinct_ids={min(active_count, working_measured_calls)}",
        "coverage_cycles="
        f"{working_measured_calls / active_count:.6f}",
        "id_schedule_tuple_construction=outside_timed_region",
        "quantile=nearest_rank",
    ]
    working_fields.extend(_stats_fields("", working_set_latencies))
    print("WORKING_SET " + " ".join(working_fields), flush=True)

    for batch_size in _DISTINCT_BATCH_SIZES:
        _require(
            batch_size <= active_count,
            "active_count is too small for a distinct-ID batch",
        )
        warmup_records = max(_BATCH_MIN_WARMUP_RECORDS, active_count)
        measured_records = max(_BATCH_MIN_MEASURED_RECORDS, active_count)
        batch_warmup_calls = math.ceil(warmup_records / batch_size)
        batch_measured_calls = math.ceil(measured_records / batch_size)
        warmup_records = batch_warmup_calls * batch_size
        measured_records = batch_measured_calls * batch_size
        warm_flat = _permutation_ids(warmup_records, active_count)
        measured_flat = _permutation_ids(
            measured_records, active_count, warmup_records
        )
        warm_batches = tuple(
            warm_flat[index : index + batch_size]
            for index in range(0, warmup_records, batch_size)
        )
        measured_batches = tuple(
            measured_flat[index : index + batch_size]
            for index in range(0, measured_records, batch_size)
        )

        def validate_batch(values, ids) -> None:
            _require(len(values) == len(ids), "latest batch length changed")
            for value, instrument_id in zip(values, ids):
                _validate_snapshot(
                    value,
                    latest_status,
                    instrument_id,
                    expected_sequences[instrument_id],
                )

        for ids in warm_batches:
            validate_batch(client.latest_snapshots(ids), ids)
        call_latencies: list[int] = []
        append_call = call_latencies.append
        for ids in measured_batches:
            begin = _now_ns()
            values = client.latest_snapshots(ids)
            end = _now_ns()
            validate_batch(values, ids)
            append_call(end - begin)
        amortized_per_record_latencies = [
            latency // batch_size for latency in call_latencies
        ]
        batch_fields = [
            f"size={batch_size}",
            "ids=distinct",
            f"stride={_WORKING_SET_STRIDE}",
            f"warmup_calls={batch_warmup_calls}",
            f"warmup_records={warmup_records}",
            f"measured_records={measured_records}",
            f"measured_distinct_ids={min(active_count, measured_records)}",
            f"coverage_cycles={measured_records / active_count:.6f}",
            "id_tuple_construction=outside_timed_region",
            "quantile=nearest_rank",
        ]
        batch_fields.extend(_stats_fields("call", call_latencies))
        batch_fields.extend(
            _stats_fields(
                "amortized_per_record",
                amortized_per_record_latencies,
            )
        )
        print("DISTINCT_BATCH " + " ".join(batch_fields), flush=True)

    return expected_sequences[e2e_instrument_id]


def _read_command() -> list[str] | None:
    line = sys.stdin.readline()
    if line == "":
        return None
    _require(line.endswith("\n"), "stdin command is not newline terminated")
    fields = line.strip().split()
    _require(bool(fields), "stdin command is empty")
    return fields


def _run_expected_sample(
    client,
    latest_status,
    inconsistent_read_error,
    instrument_id: int,
    expected_sequence: int,
) -> None:
    before = client.latest_snapshot(instrument_id)
    _require(before.status is latest_status.AVAILABLE, "pre-arm snapshot unavailable")
    _require(before.common is not None, "pre-arm snapshot has no common record")
    _require(
        before.common.ingress_sequence < expected_sequence,
        "expected sequence was already visible before ARMED",
    )
    print(f"ARMED {expected_sequence}", flush=True)
    polls = 0
    inconsistent_retries = 0
    while True:
        polls += 1
        try:
            snapshot = client.latest_snapshot(instrument_id)
        except inconsistent_read_error:
            inconsistent_retries += 1
            continue
        seen_ns = _now_ns()
        _require(snapshot.status is latest_status.AVAILABLE, "polled snapshot unavailable")
        _require(snapshot.common is not None, "polled snapshot has no common record")
        observed = snapshot.common.ingress_sequence
        _require(
            observed <= expected_sequence,
            "Python latest skipped past the expected ingress sequence",
        )
        if observed != expected_sequence:
            continue
        recv_ns = snapshot.common.recv_monotonic_ns
        _require(0 < recv_ns <= seen_ns, "invalid callback receive monotonic timestamp")
        print(
            f"SEEN {expected_sequence} {seen_ns} {recv_ns} "
            f"{polls} {inconsistent_retries}",
            flush=True,
        )
        return


def _run_burst(
    client,
    latest_status,
    inconsistent_read_error,
    instrument_id: int,
    start_sequence: int,
    count: int,
) -> None:
    final_sequence = start_sequence + count - 1
    _require(final_sequence <= _UINT64_MAX, "BURST sequence range overflows uint64")
    before = client.latest_snapshot(instrument_id)
    _require(before.status is latest_status.AVAILABLE, "pre-burst snapshot unavailable")
    _require(before.common is not None, "pre-burst snapshot has no common record")
    _require(
        before.common.ingress_sequence < start_sequence,
        "BURST start sequence was already visible",
    )
    print(f"BURST_ARMED {start_sequence} {count}", flush=True)

    polls = 0
    duplicates = 0
    stale = 0
    inconsistent_retries = 0
    skipped = 0
    last_new: int | None = None
    observed_latencies: list[int] = []
    read_call_latencies: list[int] = []
    begin_ns = _now_ns()
    while True:
        call_begin = _now_ns()
        polls += 1
        try:
            snapshot = client.latest_snapshot(instrument_id)
        except inconsistent_read_error:
            inconsistent_retries += 1
            _require(
                _now_ns() - begin_ns <= _BURST_TIMEOUT_NS,
                "BURST timed out before the final sequence became visible",
            )
            continue
        seen_ns = _now_ns()
        _require(
            seen_ns - begin_ns <= _BURST_TIMEOUT_NS,
            "BURST timed out before the final sequence became visible",
        )
        _require(snapshot.status is latest_status.AVAILABLE, "burst snapshot unavailable")
        _require(snapshot.common is not None, "burst snapshot has no common record")
        sequence = snapshot.common.ingress_sequence
        _require(sequence <= final_sequence, "burst read passed final sequence")

        if sequence < start_sequence:
            stale += 1
            continue
        if last_new is not None and sequence == last_new:
            duplicates += 1
            continue
        if last_new is not None and sequence < last_new:
            raise RuntimeError("latest snapshot ingress sequence regressed")

        skipped += (
            sequence - start_sequence
            if last_new is None
            else sequence - last_new - 1
        )
        last_new = sequence
        recv_ns = snapshot.common.recv_monotonic_ns
        _require(0 < recv_ns <= seen_ns, "invalid burst callback timestamp")
        observed_latencies.append(seen_ns - recv_ns)
        read_call_latencies.append(seen_ns - call_begin)
        if sequence == final_sequence:
            break

    observed = len(observed_latencies)
    _require(observed + skipped == count, "BURST accounting does not match offered count")
    _require(
        polls == inconsistent_retries + stale + duplicates + observed,
        "BURST polling accounting is inconsistent",
    )
    fields = [
        f"offered={count}",
        f"survivors_observed={observed}",
        f"overwritten_or_skipped={skipped}",
        f"polls={polls}",
        f"duplicate_polls={duplicates}",
        f"stale_polls={stale}",
        f"inconsistent_retries={inconsistent_retries}",
        f"elapsed_ns={_now_ns() - begin_ns}",
        "quantile=nearest_rank",
    ]
    fields.extend(
        _stats_fields(
            "survivor_ingress_timestamp_age", observed_latencies
        )
    )
    fields.extend(
        _stats_fields(
            "new_sequence_successful_read_call", read_call_latencies
        )
    )
    print("BURST_RESULT " + " ".join(fields), flush=True)


def main(argv: list[str]) -> int:
    if len(argv) != 8:
        raise RuntimeError(
            "usage: realtime_latency_benchmark_probe.py "
            "CONTROL_SOCKET NATIVE_READER SOURCE_PYTHON INSTRUMENT_ID "
            "ACTIVE_COUNT WARMUP_SAMPLES MEASURED_SAMPLES"
        )
    (
        control_socket,
        native_reader,
        source_python,
        raw_id,
        raw_active_count,
        raw_warmup,
        raw_measured,
    ) = argv[1:]
    instrument_id = _positive_decimal(raw_id, "instrument ID", 0xFFFFFFFF)
    active_count = _positive_decimal(
        raw_active_count, "active count", 0xFFFFFFFF
    )
    warmup_samples = _positive_decimal(raw_warmup, "warmup samples", 0xFFFFFFFF)
    measured_samples = _positive_decimal(
        raw_measured, "measured samples", 0xFFFFFFFF
    )
    _require(os.path.isabs(control_socket), "control socket must be absolute")
    _require(os.path.isabs(native_reader), "native reader must be absolute")
    _require(os.path.isabs(source_python), "Python source must be absolute")
    _require(os.path.isfile(native_reader), "native reader is missing")
    _require(os.path.isdir(source_python), "Python source directory is missing")
    _require(
        instrument_id <= active_count,
        "E2E instrument ID is outside the active working set",
    )
    _require(
        active_count >= max(_DISTINCT_BATCH_SIZES),
        "active count is too small for the requested distinct batches",
    )
    _require(
        math.gcd(_WORKING_SET_STRIDE, active_count) == 1,
        "working-set stride must be coprime with active_count",
    )
    _emit_environment()
    sys.path.insert(0, source_python)

    from l2flow_realtime import (  # pylint: disable=import-outside-toplevel
        InconsistentReadError,
        L2FlowClient,
        LatestStatus,
    )

    with L2FlowClient.connect(
        control_socket,
        native_library=native_reader,
        timeout=2.0,
    ) as preflight_client:
        preflight_session = preflight_client.session_info()
        _validate_active_universe(preflight_session, active_count)
    _benchmark_connections(
        L2FlowClient,
        control_socket,
        native_reader,
        preflight_session,
        active_count,
    )

    with L2FlowClient.connect(
        control_socket,
        native_library=native_reader,
        timeout=2.0,
    ) as client:
        session = client.session_info()
        _validate_active_universe(session, active_count)
        with L2FlowClient.connect(
            control_socket,
            native_library=native_reader,
            timeout=2.0,
            stale_after_ns=None,
        ) as staleness_disabled_client:
            staleness_disabled_session = (
                staleness_disabled_client.session_info()
            )
            _validate_active_universe(
                staleness_disabled_session, active_count
            )
            _require(
                staleness_disabled_session.identity == session.identity,
                "default and staleness-disabled clients mapped different sessions",
            )
            last_sequence = _run_static_benchmarks(
                client,
                staleness_disabled_client,
                LatestStatus,
                instrument_id,
                active_count,
                warmup_samples,
                measured_samples,
            )

        print("READY", flush=True)
        for _ in range(warmup_samples + measured_samples):
            command = _read_command()
            _require(command is not None, "stdin ended before all EXPECT samples")
            _require(
                len(command) == 2 and command[0] == "EXPECT",
                "expected `EXPECT seq` command",
            )
            expected = _positive_decimal(command[1], "EXPECT sequence", _UINT64_MAX)
            _require(expected > last_sequence, "EXPECT sequences must strictly increase")
            _run_expected_sample(
                client,
                LatestStatus,
                InconsistentReadError,
                instrument_id,
                expected,
            )
            last_sequence = expected

        optional = _read_command()
        if optional is not None:
            _require(
                len(optional) == 3 and optional[0] == "BURST",
                "expected optional `BURST start_seq count` command",
            )
            start_sequence = _positive_decimal(
                optional[1], "BURST start sequence", _UINT64_MAX
            )
            count = _positive_decimal(optional[2], "BURST count", 0xFFFFFFFF)
            _require(
                start_sequence > last_sequence,
                "BURST start must follow prior EXPECT sequences",
            )
            _run_burst(
                client,
                LatestStatus,
                InconsistentReadError,
                instrument_id,
                start_sequence,
                count,
            )
    print("DONE", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
