if(NOT DEFINED ROUTER OR ROUTER STREQUAL "")
    message(FATAL_ERROR "ROUTER must name mdl-production-router")
endif()

function(run_case name expected_result expected_text)
    execute_process(
        COMMAND "${ROUTER}" ${ARGN}
        RESULT_VARIABLE actual_result
        OUTPUT_VARIABLE stdout
        ERROR_VARIABLE stderr)
    set(combined "${stdout}${stderr}")
    if(NOT actual_result EQUAL expected_result)
        message(FATAL_ERROR
            "${name}: expected exit ${expected_result}, got "
            "${actual_result}\n${combined}")
    endif()
    string(FIND "${combined}" "${expected_text}" found)
    if(found EQUAL -1)
        message(FATAL_ERROR
            "${name}: missing expected text '${expected_text}'\n${combined}")
    endif()
endfunction()

run_case(
    help_lists_partial_mode
    0
    "--intraday-live-partial"
    --help)

set(base
    --sdk-library /nonexistent/libmdl_api.so
    --session-epoch 1
    --trade-date 20000101
    --daily-catalog /nonexistent/daily.catalog
    --catalog-version 1
    --server-address 127.0.0.1:1
    --user-name cli-test
    --ipc-socket /tmp/l2flow-cli-test.sock
    --intraday-store-max-records 1
    --intraday-store-memory-gib 1)

run_case(
    missing_startup_mode
    2
    "production requires exactly one of"
    ${base})

# Reaching the fixed-date runtime check (exit 1), rather than option parsing
# (exit 2), proves that partial mode is valid without explicitly disabling the
# default CERTIFIED sidecar.  ParseOptions disables that semantically invalid
# sidecar for this mode before Run is entered.
run_case(
    valid_partial_mode_reaches_runtime
    1
    "--trade-date must equal the current"
    ${base}
    --intraday-live-partial)

run_case(
    partial_and_from_open_conflict
    2
    "production requires exactly one of"
    ${base}
    --intraday-live-partial
    --intraday-store-from-open)

run_case(
    partial_and_csv_conflict
    2
    "production requires exactly one of"
    ${base}
    --intraday-live-partial
    --intraday-recovery-csv-dir /nonexistent/csv)

run_case(
    partial_rejects_kline
    2
    "cannot publish full-day KLine"
    ${base}
    --intraday-live-partial
    --kline-windows-ms 60000)

run_case(
    partial_rejects_certified_socket
    2
    "does not expose CERTIFIED"
    ${base}
    --intraday-live-partial
    --certified-ipc-socket /tmp/l2flow-cli-certified.sock)

run_case(
    partial_rejects_event_aggregator
    2
    "process-start partial-coverage contract"
    ${base}
    --intraday-live-partial
    --event-aggregator-socket /tmp/l2flow-cli-events.sock)

run_case(
    partial_rejects_recovery_mode
    2
    "requires --intraday-recovery-csv-dir"
    ${base}
    --intraday-live-partial
    --intraday-recovery-mode online)
