#!/usr/bin/env bash

set -uo pipefail

readonly root=/home/sunc/code/L2Flow
readonly registry_artifacts="${root}/artifacts/live-20260728-140m"
readonly artifacts="${root}/artifacts/live-20260728-5m"
readonly duration_seconds="${L2FLOW_TEST_DURATION_SECONDS:-300}"
readonly run_label="${L2FLOW_TEST_LABEL:-formal-5m}"
readonly enable_probe="${L2FLOW_TEST_ENABLE_PROBE:-1}"
readonly registry_sha=21d48018080745a42db1ff7adaba641dfc18818dd953fb94059a843b28feb3b4
readonly ipc_dir="${artifacts}/ipc-${run_label}"
readonly ipc_socket="${ipc_dir}/market-v1.sock"
readonly user_name_file="${root}/artifacts/stage-latency-live-20260727/.accept-user-name-120m.tmp"

if [[ ! "${duration_seconds}" =~ ^[1-9][0-9]*$ ]] ||
   [[ "${duration_seconds}" -gt 86400 ]]; then
    echo "run_error=invalid_duration_seconds"
    exit 2
fi
if [[ "${enable_probe}" != 0 && "${enable_probe}" != 1 ]]; then
    echo "run_error=invalid_enable_probe"
    exit 2
fi
if [[ ! "${run_label}" =~ ^[a-z0-9][a-z0-9-]*$ ]]; then
    echo "run_error=invalid_run_label"
    exit 2
fi

cpp_pid=
python_pid=
probe_pid=
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
    stop_one "${probe_pid}"
    stop_one "${monitor_pid}"
}

trap stop_children INT TERM HUP

install -d -m 0700 "${artifacts}" "${ipc_dir}"
if [[ -e "${ipc_socket}" ||
      -e "${artifacts}/${run_label}-cpp-report.json" ||
      -e "${artifacts}/${run_label}-python-report.json" ]]; then
    echo "run_error=preexisting_output"
    exit 1
fi
if [[ ! -r "${user_name_file}" ]]; then
    echo "run_error=missing_user_name_file"
    exit 1
fi

echo "run_start=$(date --iso-8601=ns)"
echo "run_label=${run_label}"
echo "duration_seconds=${duration_seconds}"
echo "store_record_capacity=50000000"
echo "capture_record_capacity=50000000"
echo "ipc_tick_ring_capacity=8388608"
echo "coverage_mode=partial_session"
echo "enable_probe=${enable_probe}"

"${root}/build-acceptance/accept-realtime-pipeline" \
    --sdk-library /home/sunc/L2Flow/MDL/libmdl_api.so \
    --registry-directory "${registry_artifacts}" \
    --registry-file instrument-registry-v1.tsv \
    --registry-version 20260728 \
    --registry-sha256 "${registry_sha}" \
    --trade-date 20260728 \
    --server-address 127.0.0.1:9112 \
    --user-name-file "${user_name_file}" \
    --sdk-log-prefix "/tmp/l2flow-${run_label}-cpp" \
    --report-json "${artifacts}/${run_label}-cpp-report.json" \
    --samples-csv "${artifacts}/${run_label}-cpp-generation-samples.csv" \
    --per-instrument-read-timings-csv \
        "${artifacts}/${run_label}-per-instrument-read.csv" \
    --duration-seconds "${duration_seconds}" \
    --generation-interval-ms 1000 \
    --generation-timeout-ms 10000 \
    --instrument-store-workers 4 \
    --acquire-repetitions 8 \
    --intraday-store-max-records 50000000 \
    --intraday-store-memory-gib 200 \
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
    >"${artifacts}/${run_label}-cpp-summary.json" \
    2>"${artifacts}/${run_label}-cpp-stderr.log" &
cpp_pid=$!

socket_wait=0
while [[ ! -S "${ipc_socket}" && "${socket_wait}" -lt 600 ]]; do
    if ! kill -0 "${cpp_pid}" 2>/dev/null; then
        echo "run_error=cpp_exited_before_ipc_ready"
        wait "${cpp_pid}" 2>/dev/null || true
        exit 1
    fi
    sleep 0.1
    socket_wait=$((socket_wait + 1))
done
if [[ ! -S "${ipc_socket}" ]]; then
    echo "run_error=ipc_socket_timeout"
    stop_children
    exit 1
fi

if [[ "${enable_probe}" == 1 ]]; then
    "${root}/build-probe/mdl-sdk-feeder-probe" \
        --library /home/sunc/L2Flow/MDL/libmdl_api.so \
        --address 127.0.0.1:9112 \
        --sdk-log-prefix "/tmp/l2flow-${run_label}-feeder-probe" \
        --timeout-seconds 10 \
        --market-timeout-seconds 60 \
        --monitor-seconds "${duration_seconds}" \
        --minimum-market-messages 1 \
        --maximum-captured-records 50000000 \
        --capture-csv "${artifacts}/${run_label}-feeder-capture.csv" \
        >"${artifacts}/${run_label}-feeder-summary.json" \
        2>"${artifacts}/${run_label}-feeder-stderr.log" &
    probe_pid=$!
fi

PYTHONPATH="${root}/python" /opt/anaconda3/bin/python \
    "${root}/tools/accept_python_realtime.py" \
    --control-socket "${ipc_socket}" \
    --native-reader "${root}/build-acceptance/libl2flow_shm_reader.so" \
    --source-python "${root}/python" \
    --report-json "${artifacts}/${run_label}-python-report.json" \
    --duration-seconds "${duration_seconds}" \
    --instrument-ids 23213,26040 \
    --kline-window-ids 1000,60000,300000 \
    --max-batch-rows 16384 \
    --latest-interval-ms 1000 \
    --progress-interval-seconds 30 \
    --empty-poll-sleep-us 100 \
    >"${artifacts}/${run_label}-python-stdout.log" \
    2>"${artifacts}/${run_label}-python-stderr.log" &
python_pid=$!

(
    while true; do
        date --iso-8601=ns
        ps -o pid,ppid,stat,psr,pcpu,pmem,rss,vsz,etime,comm \
            -p "${cpp_pid},${python_pid}${probe_pid:+,${probe_pid}}" \
            2>&1 || true
        find /home/sunc/L2Flow/MDL/msg_backup/20260728 \
            -maxdepth 1 -type f -printf '%f %s %T@\n' | sort
        free -b | sed -n '1,2p'
        df -B1 "${artifacts}" | tail -n 1
        sleep 15
    done
) >"${artifacts}/${run_label}-resource-monitor.log" 2>&1 &
monitor_pid=$!

echo "cpp_pid=${cpp_pid}"
echo "python_pid=${python_pid}"
echo "probe_pid=${probe_pid}"
echo "monitor_pid=${monitor_pid}"
echo "ipc_ready=$(date --iso-8601=ns)"

python_status=0
wait "${python_pid}" || python_status=$?
python_pid=

cpp_status=0
wait "${cpp_pid}" || cpp_status=$?
cpp_pid=

probe_status=0
if [[ -n "${probe_pid}" ]]; then
    wait "${probe_pid}" || probe_status=$?
    probe_pid=
fi

stop_one "${monitor_pid}"
wait "${monitor_pid}" 2>/dev/null || true
monitor_pid=

comparison_status=0
if [[ "${enable_probe}" == 1 && "${probe_status}" -eq 0 ]]; then
    python3 "${root}/tools/compare_feeder_capture.py" \
        "${artifacts}/${run_label}-feeder-capture.csv" \
        --backup-root /home/sunc/L2Flow/MDL/msg_backup \
        --date 20260728 \
        --report-json \
            "${artifacts}/${run_label}-backup-comparison.json" \
        --report-csv \
            "${artifacts}/${run_label}-backup-comparison.csv" \
        --max-examples 20 \
        --tail-wait-seconds 30 \
        >"${artifacts}/${run_label}-backup-comparison.stdout.log" \
        2>"${artifacts}/${run_label}-backup-comparison.stderr.log" ||
        comparison_status=$?
fi

echo "run_end=$(date --iso-8601=ns)"
echo "cpp_status=${cpp_status}"
echo "python_status=${python_status}"
echo "probe_status=${probe_status}"
echo "comparison_status=${comparison_status}"

if [[ "${cpp_status}" -ne 0 || "${python_status}" -ne 0 ||
      "${probe_status}" -ne 0 || "${comparison_status}" -ne 0 ]]; then
    exit 1
fi
