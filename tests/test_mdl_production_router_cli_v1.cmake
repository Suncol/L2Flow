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
    help_lists_three_worker_planes
    0
    "--tick-workers N --event-workers N --kline-workers N"
    --help)

run_case(
    help_lists_fast_append_capacity
    0
    "--fast-max-records N"
    --help)

run_case(
    help_lists_raw_tick_shards
    0
    "--raw-tick-queue N --raw-tick-prewarm-per-shard N"
    --help)

run_case(
    help_lists_bounded_derived_cdc
    0
    "--event-changes-per-instrument N"
    --help)

run_case(
    removed_certified_option_is_unknown
    2
    "unknown option: --certified-maximum-events"
    --certified-maximum-events 1)

run_case(
    removed_generation_option_is_unknown
    2
    "unknown option: --generation-interval-ms"
    --generation-interval-ms 1)

run_case(
    removed_decoder_lane_option_is_unknown
    2
    "unknown option: --decoder-queue"
    --decoder-queue 1)

run_case(
    removed_decoded_tick_queue_option_is_unknown
    2
    "unknown option: --tick-queue"
    --tick-queue 1)

run_case(
    removed_global_ingress_pool_option_is_unknown
    2
    "unknown option: --maximum-inflight-messages"
    --maximum-inflight-messages 1)

run_case(
    sdk_callback_is_serialized
    2
    "--sdk-work-threads must be 1 for serialized ingress"
    --sdk-work-threads 2)

run_case(
    missing_required_options_fail_closed
    2
    "one or more required options are missing"
    --sdk-library /nonexistent/libmdl.so)

run_case(
    each_plane_requires_its_own_cpu_set
    2
    "supply exactly one CPU set per configured worker"
    --sdk-library /nonexistent/libmdl.so
    --daily-catalog /nonexistent/catalog.csv
    --trade-date 20260805
    --catalog-version 1
    --session-epoch 1
    --server 127.0.0.1:1
    --user test
    --fast-max-records 100
    --tick-cpu-set 0
    --event-cpu-set 1)
