#!/usr/bin/env bash

set -u

readonly root=/home/sunc/code/L2Flow
readonly artifacts="${root}/artifacts/live-20260728-140m"
# This retry starts after the open and crosses the 11:30-13:00 lunch break.
# Four wall-clock hours therefore cover about 150 minutes of active market
# time, preserving ten minutes of live-data margin beyond the requested 140.
readonly duration_seconds=14400
readonly registry_sha=21d48018080745a42db1ff7adaba641dfc18818dd953fb94059a843b28feb3b4
readonly ipc_dir="${artifacts}/ipc-python-valid"
readonly ipc_socket="${ipc_dir}/market-v1.sock"

router_pid=
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
    stop_one "${router_pid}"
    stop_one "${monitor_pid}"
}

trap stop_children INT TERM HUP

install -d -m 0700 "${ipc_dir}"
if [[ -e "${ipc_socket}" ]]; then
    echo "retry_error=preexisting_ipc_socket"
    exit 1
fi

echo "retry_start=$(date --iso-8601=ns)"
echo "duration_seconds=${duration_seconds}"
echo "expected_active_market_seconds=9000"
echo "requested_active_market_seconds=8400"
echo "active_market_margin_seconds=600"
echo "store_record_capacity=600000000"
echo "ipc_tick_ring_capacity=8388608"

"${root}/build-acceptance/mdl-production-router" \
    --sdk-library /home/sunc/L2Flow/MDL/libmdl_api.so \
    --registry-directory "${artifacts}" \
    --registry-file instrument-registry-v1.tsv \
    --registry-version 20260728 \
    --registry-sha256 "${registry_sha}" \
    --trade-date 20260728 \
    --server-address 127.0.0.1:9112 \
    --user-name l2flow-local-feeder-probe \
    --sdk-log-prefix /tmp/l2flow-router-20260728-python-valid \
    --instrument-store-workers 4 \
    --intraday-store-max-records 600000000 \
    --intraday-store-memory-gib 600 \
    --intraday-store-segment-kib 64 \
    --intraday-store-batch-records 65536 \
    --partial-session \
    --kline-windows-ms 1000,60000,300000 \
    --generation-interval-ms 1000 \
    --generation-timeout-ms 10000 \
    --ipc-socket "${ipc_socket}" \
    --ipc-tick-ring-records 8388608 \
    --ipc-max-mapping-mib 8192 \
    >"${artifacts}/valid-140m-router-stdout.log" \
    2>"${artifacts}/valid-140m-router-stderr.log" &
router_pid=$!

socket_wait=0
while [[ ! -S "${ipc_socket}" && "${socket_wait}" -lt 600 ]]; do
    if ! kill -0 "${router_pid}" 2>/dev/null; then
        echo "retry_error=router_exited_before_ipc_ready"
        wait "${router_pid}" 2>/dev/null || true
        exit 1
    fi
    sleep 0.1
    socket_wait=$((socket_wait + 1))
done
if [[ ! -S "${ipc_socket}" ]]; then
    echo "retry_error=ipc_socket_timeout"
    stop_children
    exit 1
fi

PYTHONPATH="${root}/python" /opt/anaconda3/bin/python \
    "${root}/tools/accept_python_realtime.py" \
    --control-socket "${ipc_socket}" \
    --native-reader "${root}/build-acceptance/libl2flow_shm_reader.so" \
    --source-python "${root}/python" \
    --report-json "${artifacts}/valid-140m-python-report.json" \
    --duration-seconds "${duration_seconds}" \
    --instrument-ids 23213,26040 \
    --kline-window-ids 1000,60000,300000 \
    --max-batch-rows 16384 \
    --latest-interval-ms 1000 \
    --progress-interval-seconds 60 \
    --empty-poll-sleep-us 100 \
    >"${artifacts}/valid-140m-python-stdout.log" \
    2>"${artifacts}/valid-140m-python-stderr.log" &
python_pid=$!

(
    while true; do
        date --iso-8601=ns
        ps -o pid,ppid,stat,psr,pcpu,pmem,rss,vsz,etime,comm \
            -p "${router_pid},${python_pid}" 2>&1 || true
        find /home/sunc/L2Flow/MDL/msg_backup/20260728 \
            -maxdepth 1 -type f -printf '%f %s %T@\n' | sort
        free -b | sed -n '1,2p'
        df -B1 "${artifacts}" | tail -n 1
        sleep 30
    done
) >"${artifacts}/valid-140m-resource-monitor.log" 2>&1 &
monitor_pid=$!

echo "router_pid=${router_pid}"
echo "python_pid=${python_pid}"
echo "monitor_pid=${monitor_pid}"
echo "ipc_ready=$(date --iso-8601=ns)"

python_status=0
wait "${python_pid}" || python_status=$?
python_pid=

stop_one "${router_pid}"
router_status=0
wait "${router_pid}" || router_status=$?
router_pid=

stop_one "${monitor_pid}"
wait "${monitor_pid}" 2>/dev/null || true
monitor_pid=

echo "retry_end=$(date --iso-8601=ns)"
echo "router_status=${router_status}"
echo "python_status=${python_status}"

if [[ "${router_status}" -ne 0 || "${python_status}" -ne 0 ]]; then
    exit 1
fi
