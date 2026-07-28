#!/usr/bin/env bash

set -u

readonly root=/home/sunc/code/L2Flow
readonly artifacts="${root}/artifacts/stage-latency-live-20260727"
readonly backup_capture="${artifacts}/formal-45m-direct-read-feeder-capture.csv"

main_pid=
probe_pid=

stop_children() {
    if [[ -n "${main_pid}" ]]; then
        kill -TERM "${main_pid}" 2>/dev/null || true
    fi
    if [[ -n "${probe_pid}" ]]; then
        kill -TERM "${probe_pid}" 2>/dev/null || true
    fi
    if [[ -n "${main_pid}" ]]; then
        wait "${main_pid}" 2>/dev/null || true
    fi
    if [[ -n "${probe_pid}" ]]; then
        wait "${probe_pid}" 2>/dev/null || true
    fi
}

trap stop_children INT TERM HUP

echo "formal_start=$(date --iso-8601=ns)"

"${root}/build-acceptance/accept-realtime-pipeline" \
    --sdk-library /home/sunc/L2Flow/MDL/libmdl_api.so \
    --registry-directory "${artifacts}" \
    --registry-file instrument-registry-v1.tsv \
    --registry-version 20260727 \
    --registry-sha256 e9c8140a6a5c1572a9d1487fbf5aca79b74d1eedce1b6499ac8d9bede4923c38 \
    --trade-date 20260727 \
    --server-address 127.0.0.1:9112 \
    --user-name-file "${artifacts}/.accept-user-name-45m-direct-read.tmp" \
    --sdk-log-prefix /tmp/l2flow-stage-latency-20260727-formal-45m-direct-read \
    --report-json "${artifacts}/formal-45m-direct-read-report.json" \
    --samples-csv "${artifacts}/formal-45m-direct-read-samples.csv" \
    --per-instrument-read-timings-csv "${artifacts}/formal-45m-direct-read-per-instrument.csv" \
    --duration-seconds 2700 \
    --generation-interval-ms 1000 \
    --generation-timeout-ms 10000 \
    --instrument-store-workers 4 \
    --acquire-repetitions 8 \
    --intraday-store-max-records 300000000 \
    --intraday-store-memory-gib 384 \
    --intraday-store-segment-kib 64 \
    --intraday-store-batch-records 65536 \
    --intraday-scan-batch-records 4096 \
    --intraday-scan-workers 1 \
    --intraday-reader-cpus 0 \
    --measure-stage-latency \
    --partial-session \
    >"${artifacts}/formal-45m-direct-read-main-summary.json" \
    2>"${artifacts}/formal-45m-direct-read-main-stderr.log" &
main_pid=$!

"${root}/build-probe/mdl-sdk-feeder-probe" \
    --library /home/sunc/L2Flow/MDL/libmdl_api.so \
    --address 127.0.0.1:9112 \
    --sdk-log-prefix /tmp/l2flow-mdl-sdk-feeder-probe-20260727-formal-45m-direct-read \
    --timeout-seconds 10 \
    --market-timeout-seconds 10 \
    --monitor-seconds 2700 \
    --minimum-market-messages 1 \
    --maximum-captured-records 300000000 \
    --capture-csv "${backup_capture}" \
    >"${artifacts}/formal-45m-direct-read-feeder-summary.json" \
    2>"${artifacts}/formal-45m-direct-read-feeder-stderr.log" &
probe_pid=$!

echo "main_pid=${main_pid}"
echo "probe_pid=${probe_pid}"

main_status=0
wait "${main_pid}" || main_status=$?
main_pid=

probe_status=0
wait "${probe_pid}" || probe_status=$?
probe_pid=

echo "formal_end=$(date --iso-8601=ns)"
echo "main_status=${main_status}"
echo "probe_status=${probe_status}"

if [[ "${main_status}" -ne 0 || "${probe_status}" -ne 0 ]]; then
    exit 1
fi
