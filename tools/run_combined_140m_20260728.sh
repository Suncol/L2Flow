#!/usr/bin/env bash

set -u

readonly root=/home/sunc/code/L2Flow
readonly artifacts="${root}/artifacts/live-20260728-140m"
# Run through the complete afternoon and retain a 30-minute post-close
# observation window. C++ receives another two minutes to publish its final
# immutable Store/KLine generations and execute the terminal full scan.
readonly target_epoch=$(date --date='2026-07-28 15:30:00 +08:00' +%s)
readonly launch_epoch=$(date +%s)
readonly python_duration_seconds=$((target_epoch - launch_epoch))
readonly cpp_duration_seconds=$((python_duration_seconds + 120))
readonly registry_sha=21d48018080745a42db1ff7adaba641dfc18818dd953fb94059a843b28feb3b4
readonly ipc_dir="${artifacts}/ipc-combined-close-1530"
readonly ipc_socket="${ipc_dir}/market-v1.sock"

if [[ "${python_duration_seconds}" -le 0 ]]; then
    echo "combined_error=target_time_has_passed"
    exit 1
fi

cpp_pid=
python_pid=
monitor_pid=

stop_one() {
    local pid="${1:-}"
    if [[ -n "${pid}" ]]; then
        kill -TERM "${pid}" 2>/dev/null || true
    fi
}

stop_children() {
    stop_one "${python_pid}"
    stop_one "${cpp_pid}"
    stop_one "${monitor_pid}"
}

trap stop_children INT TERM HUP

install -d -m 0700 "${ipc_dir}"
if [[ -e "${ipc_socket}" ]]; then
    echo "combined_error=preexisting_ipc_socket"
    exit 1
fi

echo "combined_start=$(date --iso-8601=ns)"
echo "python_target_realtime=2026-07-28T15:30:00+08:00"
echo "cpp_duration_seconds=${cpp_duration_seconds}"
echo "python_duration_seconds=${python_duration_seconds}"
echo "requested_active_market_seconds=8400"
echo "store_record_capacity=1200000000"
echo "store_logical_capacity_gib=1000"
echo "ipc_tick_ring_capacity=8388608"

"${root}/build-acceptance/accept-realtime-pipeline" \
    --sdk-library /home/sunc/L2Flow/MDL/libmdl_api.so \
    --registry-directory "${artifacts}" \
    --registry-file instrument-registry-v1.tsv \
    --registry-version 20260728 \
    --registry-sha256 "${registry_sha}" \
    --trade-date 20260728 \
    --server-address 127.0.0.1:9112 \
    --user-name-file \
        "${root}/artifacts/stage-latency-live-20260727/.accept-user-name-120m.tmp" \
    --sdk-log-prefix /tmp/l2flow-combined-20260728-close-1530 \
    --report-json "${artifacts}/close-1530-cpp-report.json" \
    --samples-csv "${artifacts}/close-1530-cpp-generation-samples.csv" \
    --per-instrument-read-timings-csv \
        "${artifacts}/close-1530-per-instrument-read.csv" \
    --duration-seconds "${cpp_duration_seconds}" \
    --generation-interval-ms 1000 \
    --generation-timeout-ms 10000 \
    --instrument-store-workers 4 \
    --acquire-repetitions 8 \
    --intraday-store-max-records 1200000000 \
    --intraday-store-memory-gib 1000 \
    --intraday-store-segment-kib 64 \
    --intraday-store-batch-records 65536 \
    --intraday-scan-batch-records 4096 \
    --intraday-scan-workers 4 \
    --intraday-reader-cpus 0,1,2,3 \
    --kline-windows-ms 1000,60000,300000 \
    --kline-min-consecutive-bars 10 \
    --measure-stage-latency \
    --partial-session \
    --ipc-socket "${ipc_socket}" \
    --ipc-tick-ring-records 8388608 \
    --ipc-max-mapping-mib 8192 \
    >"${artifacts}/close-1530-cpp-summary.json" \
    2>"${artifacts}/close-1530-cpp-stderr.log" &
cpp_pid=$!

socket_wait=0
while [[ ! -S "${ipc_socket}" && "${socket_wait}" -lt 600 ]]; do
    if ! kill -0 "${cpp_pid}" 2>/dev/null; then
        echo "combined_error=cpp_exited_before_ipc_ready"
        wait "${cpp_pid}" 2>/dev/null || true
        exit 1
    fi
    sleep 0.1
    socket_wait=$((socket_wait + 1))
done
if [[ ! -S "${ipc_socket}" ]]; then
    echo "combined_error=ipc_socket_timeout"
    stop_children
    exit 1
fi

PYTHONPATH="${root}/python" /opt/anaconda3/bin/python \
    "${root}/tools/accept_python_realtime.py" \
    --control-socket "${ipc_socket}" \
    --native-reader "${root}/build-acceptance/libl2flow_shm_reader.so" \
    --source-python "${root}/python" \
    --report-json "${artifacts}/close-1530-python-report.json" \
    --duration-seconds "${python_duration_seconds}" \
    --instrument-ids 23213,26040 \
    --kline-window-ids 1000,60000,300000 \
    --max-batch-rows 16384 \
    --latest-interval-ms 1000 \
    --progress-interval-seconds 60 \
    --empty-poll-sleep-us 100 \
    >"${artifacts}/close-1530-python-stdout.log" \
    2>"${artifacts}/close-1530-python-stderr.log" &
python_pid=$!

(
    while true; do
        date --iso-8601=ns
        ps -o pid,ppid,stat,psr,pcpu,pmem,rss,vsz,etime,comm \
            -p "${cpp_pid},${python_pid}" 2>&1 || true
        find /home/sunc/L2Flow/MDL/msg_backup/20260728 \
            -maxdepth 1 -type f -printf '%f %s %T@\n' | sort
        free -b | sed -n '1,2p'
        df -B1 "${artifacts}" | tail -n 1
        sleep 30
    done
) >"${artifacts}/close-1530-resource-monitor.log" 2>&1 &
monitor_pid=$!

echo "cpp_pid=${cpp_pid}"
echo "python_pid=${python_pid}"
echo "monitor_pid=${monitor_pid}"
echo "ipc_ready=$(date --iso-8601=ns)"

python_status=0
wait "${python_pid}" || python_status=$?
python_pid=

cpp_status=0
wait "${cpp_pid}" || cpp_status=$?
cpp_pid=

stop_one "${monitor_pid}"
wait "${monitor_pid}" 2>/dev/null || true
monitor_pid=

echo "combined_end=$(date --iso-8601=ns)"
echo "cpp_status=${cpp_status}"
echo "python_status=${python_status}"

if [[ "${cpp_status}" -ne 0 || "${python_status}" -ne 0 ]]; then
    exit 1
fi
