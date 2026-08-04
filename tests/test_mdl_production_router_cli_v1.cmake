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

run_case(
    help_lists_parallel_decoder_default
    0
    "--parallel-decoder-workers N  0..64, default 0"
    --help)

run_case(
    help_lists_online_as_only_csv_recovery_mode
    0
    "online (the only supported mode)"
    --help)

run_case(
    help_lists_boundary_alignment_timeout
    0
    "--intraday-recovery-boundary-alignment-ms N"
    --help)

run_case(
    help_lists_recovery_progress_interval
    0
    "--intraday-recovery-progress-interval-seconds N"
    --help)

run_case(
    help_lists_partial_native_disorder_bound
    0
    "--native-maximum-backward-displacement N"
    --help)

run_case(
    help_describes_partial_canonical_event
    0
    "mid-session FAST plus bounded canonical Event"
    --help)

run_case(
    help_describes_partial_kline_coverage
    0
    "published bars strictly spanning the boundary are"
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

run_case(
    partial_requires_declared_disorder_bound
    2
    "requires --native-maximum-backward-displacement"
    ${base}
    --intraday-live-partial)

run_case(
    valid_partial_canonical_mode_reaches_runtime
    1
    "--trade-date must equal the current"
    ${base}
    --intraday-live-partial
    --native-maximum-backward-displacement 2)

run_case(
    partial_rejects_unbounded_disorder_declaration
    2
    "must be below both the reorder span and per-channel pending capacity"
    ${base}
    --intraday-live-partial
    --native-maximum-backward-displacement 16384)

run_case(
    partial_fast_only_mode_reaches_runtime
    1
    "--trade-date must equal the current"
    ${base}
    --intraday-live-partial
    --disable-native-gap-recovery)

run_case(
    parallel_decoder_zero_reaches_runtime
    1
    "--trade-date must equal the current"
    ${base}
    --intraday-live-partial
    --native-maximum-backward-displacement 2
    --parallel-decoder-workers 0)

run_case(
    parallel_decoder_four_reaches_runtime
    1
    "--trade-date must equal the current"
    ${base}
    --intraday-live-partial
    --native-maximum-backward-displacement 2
    --parallel-decoder-workers 4)

run_case(
    parallel_decoder_above_bound_is_rejected
    2
    "--parallel-decoder-workers must be 0..64"
    ${base}
    --intraday-live-partial
    --native-maximum-backward-displacement 2
    --parallel-decoder-workers 65)

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
    partial_accepts_kline
    1
    "--trade-date must equal the current"
    ${base}
    --intraday-live-partial
    --native-maximum-backward-displacement 2
    --kline-windows-ms 60000)

run_case(
    partial_accepts_canonical_event_socket
    1
    "--trade-date must equal the current"
    ${base}
    --intraday-live-partial
    --native-maximum-backward-displacement 2
    --certified-ipc-socket /tmp/l2flow-cli-certified.sock)

run_case(
    removed_arrival_order_event_socket_is_unknown
    2
    "unknown option: --event-aggregator-socket"
    ${base}
    --intraday-live-partial
    --event-aggregator-socket /tmp/l2flow-cli-events.sock)

run_case(
    partial_managed_event_poll_option_is_removed
    2
    "unknown option: --event-aggregator-poll-ms"
    ${base}
    --intraday-live-partial
    --event-aggregator-poll-ms 0)

run_case(
    partial_rejects_invalid_event_cpu_set
    2
    "--event-cpu-set is invalid: invalid_syntax"
    ${base}
    --intraday-live-partial
    --native-maximum-backward-displacement 2
    --event-cpu-set "8, 9")

run_case(
    partial_disorder_bound_rejects_fast_only_mode
    2
    "cannot be combined with --disable-native-gap-recovery"
    ${base}
    --intraday-live-partial
    --disable-native-gap-recovery
    --native-maximum-backward-displacement 2)

run_case(
    partial_fast_only_rejects_event_cpu_set
    2
    "--event-cpu-set requires the canonical Event service"
    ${base}
    --intraday-live-partial
    --disable-native-gap-recovery
    --event-cpu-set 8)

run_case(
    partial_managed_event_executable_option_is_removed
    2
    "unknown option: --event-aggregator-executable"
    ${base}
    --intraday-live-partial
    --event-aggregator-executable relative-event-aggregator)

run_case(
    native_disorder_bound_requires_partial_mode
    2
    "requires --intraday-live-partial"
    ${base}
    --intraday-store-from-open
    --native-maximum-backward-displacement 2)

run_case(
    partial_rejects_recovery_mode
    2
    "requires --intraday-recovery-csv-dir"
    ${base}
    --intraday-live-partial
    --disable-native-gap-recovery
    --intraday-recovery-mode online)

run_case(
    blocking_csv_recovery_is_rejected
    2
    "blocking CSV recovery is no longer supported"
    ${base}
    --intraday-recovery-csv-dir /nonexistent/csv
    --intraday-recovery-mode blocking)

run_case(
    blocking_live_buffer_option_is_removed
    2
    "unknown option: --intraday-recovery-live-buffer-messages"
    ${base}
    --intraday-recovery-csv-dir /nonexistent/csv
    --intraday-recovery-live-buffer-messages 1024)

run_case(
    blocking_live_buffer_mib_option_is_removed
    2
    "unknown option: --intraday-recovery-live-buffer-mib"
    ${base}
    --intraday-recovery-csv-dir /nonexistent/csv
    --intraday-recovery-live-buffer-mib 512)

run_case(
    csv_recovery_defaults_to_online_requirements
    2
    "online recovery requires"
    ${base}
    --intraday-recovery-csv-dir /nonexistent/csv)

run_case(
    implicit_online_csv_recovery_reaches_runtime
    1
    "--trade-date must equal the current"
    ${base}
    --intraday-recovery-csv-dir /nonexistent/csv
    --intraday-recovery-journal-dir /tmp/l2flow-cli-journal
    --live-preview-ipc-socket /tmp/l2flow-cli-preview.sock)

run_case(
    explicit_online_csv_recovery_reaches_runtime
    1
    "--trade-date must equal the current"
    ${base}
    --intraday-recovery-csv-dir /nonexistent/csv
    --intraday-recovery-mode online
    --intraday-recovery-journal-dir /tmp/l2flow-cli-journal
    --live-preview-ipc-socket /tmp/l2flow-cli-preview.sock)

run_case(
    invalid_boundary_alignment_timeout_is_rejected
    2
    "--intraday-recovery-boundary-alignment-ms must be 1..60000"
    ${base}
    --intraday-recovery-csv-dir /nonexistent/csv
    --intraday-recovery-journal-dir /tmp/l2flow-cli-journal
    --live-preview-ipc-socket /tmp/l2flow-cli-preview.sock
    --intraday-recovery-boundary-alignment-ms 0)

run_case(
    recovery_progress_interval_reaches_runtime
    1
    "--trade-date must equal the current"
    ${base}
    --intraday-recovery-csv-dir /nonexistent/csv
    --intraday-recovery-journal-dir /tmp/l2flow-cli-journal
    --live-preview-ipc-socket /tmp/l2flow-cli-preview.sock
    --intraday-recovery-progress-interval-seconds 1)

run_case(
    invalid_recovery_progress_interval_is_rejected
    2
    "--intraday-recovery-progress-interval-seconds must be 1..3600"
    ${base}
    --intraday-recovery-csv-dir /nonexistent/csv
    --intraday-recovery-journal-dir /tmp/l2flow-cli-journal
    --live-preview-ipc-socket /tmp/l2flow-cli-preview.sock
    --intraday-recovery-progress-interval-seconds 0)

run_case(
    recovery_progress_interval_requires_csv
    2
    "journal/live-preview options require"
    ${base}
    --intraday-live-partial
    --disable-native-gap-recovery
    --intraday-recovery-progress-interval-seconds 1)
