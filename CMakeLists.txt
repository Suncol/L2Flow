cmake_minimum_required(VERSION 3.16)

project(L2Flow VERSION 1.0.0 LANGUAGES CXX)

# Keep GNU builds on the production compiler major version while allowing any
# GCC 13 minor or patch release.
set(L2FLOW_REQUIRED_GCC_MAJOR_VERSION "13")
message(STATUS
    "L2Flow C++ compiler: ${CMAKE_CXX_COMPILER_ID} "
    "${CMAKE_CXX_COMPILER_VERSION} (${CMAKE_CXX_COMPILER})")
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    string(REGEX MATCH "^[0-9]+" L2FLOW_DETECTED_GCC_MAJOR_VERSION
        "${CMAKE_CXX_COMPILER_VERSION}")
    if(NOT L2FLOW_DETECTED_GCC_MAJOR_VERSION STREQUAL
            L2FLOW_REQUIRED_GCC_MAJOR_VERSION)
        message(FATAL_ERROR
            "L2Flow GNU builds require GCC "
            "${L2FLOW_REQUIRED_GCC_MAJOR_VERSION}.x; detected GCC "
            "${CMAKE_CXX_COMPILER_VERSION} at ${CMAKE_CXX_COMPILER}. "
            "Select GCC ${L2FLOW_REQUIRED_GCC_MAJOR_VERSION} with "
            "-DCMAKE_CXX_COMPILER=/path/to/g++-13.")
    endif()
    message(STATUS
        "L2Flow required GCC major version: "
        "${L2FLOW_REQUIRED_GCC_MAJOR_VERSION}")
else()
    message(STATUS
        "L2Flow GCC major-version baseline: "
        "${L2FLOW_REQUIRED_GCC_MAJOR_VERSION} "
        "(not enforced for ${CMAKE_CXX_COMPILER_ID})")
endif()

option(L2FLOW_BUILD_TESTS "Build the realtime production-chain tests" ON)
option(L2MOCK_BUILD_TESTS "Build the isolated vendor mock tests" OFF)
option(L2FLOW_BUILD_FEEDER_PROBE
       "Build the opt-in standalone SDK feeder diagnostic" OFF)
option(L2FLOW_BUILD_MARKET_DECODER_LIBFUZZER
       "Build the optional market decoder libFuzzer target" OFF)
option(L2FLOW_ENABLE_ASAN_UBSAN
       "Enable AddressSanitizer and UndefinedBehaviorSanitizer" OFF)
option(L2FLOW_ENABLE_TSAN "Enable ThreadSanitizer" OFF)

if(L2FLOW_ENABLE_ASAN_UBSAN AND L2FLOW_ENABLE_TSAN)
    message(FATAL_ERROR
        "ASan+UBSan and TSan are separate build modes")
endif()

if(MSVC AND (L2FLOW_ENABLE_ASAN_UBSAN OR L2FLOW_ENABLE_TSAN))
    message(FATAL_ERROR
        "The configured sanitizer modes require GCC or Clang")
endif()

find_package(Threads REQUIRED)

set(L2FLOW_SDK_INCLUDE_DIR
    "${CMAKE_CURRENT_SOURCE_DIR}/mdl_sdk_2_13_234/include"
    CACHE PATH "Directory containing the vendor MDL SDK headers")

if(NOT EXISTS "${L2FLOW_SDK_INCLUDE_DIR}/mdl_api.h")
    message(FATAL_ERROR
        "L2FLOW_SDK_INCLUDE_DIR must contain mdl_api.h")
endif()

set(L2FLOW_STRICT_COMPILE_FLAGS)
if(MSVC)
    set(L2FLOW_STRICT_COMPILE_FLAGS /W4 /permissive- /WX)
else()
    set(L2FLOW_STRICT_COMPILE_FLAGS
        -Wall -Wextra -Wpedantic -Wconversion -Wshadow -Werror)
endif()

set(L2FLOW_SANITIZER_COMPILE_FLAGS)
set(L2FLOW_SANITIZER_LINK_FLAGS)
if(L2FLOW_ENABLE_ASAN_UBSAN)
    set(L2FLOW_SANITIZER_COMPILE_FLAGS
        -fno-omit-frame-pointer -fsanitize=address,undefined)
    set(L2FLOW_SANITIZER_LINK_FLAGS
        -fno-omit-frame-pointer -fsanitize=address,undefined)
elseif(L2FLOW_ENABLE_TSAN)
    set(L2FLOW_SANITIZER_COMPILE_FLAGS
        -fno-omit-frame-pointer -fsanitize=thread)
    set(L2FLOW_SANITIZER_LINK_FLAGS
        -fno-omit-frame-pointer -fsanitize=thread)
endif()

function(l2flow_configure_target target)
    set_target_properties(${target} PROPERTIES
        CXX_STANDARD 20
        CXX_STANDARD_REQUIRED YES
        CXX_EXTENSIONS NO)
    target_compile_features(${target} PUBLIC cxx_std_20)
    target_include_directories(${target}
        PUBLIC
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)
    target_include_directories(${target}
        SYSTEM PUBLIC
            $<BUILD_INTERFACE:${L2FLOW_SDK_INCLUDE_DIR}>)
    target_compile_options(${target}
        PRIVATE
            ${L2FLOW_STRICT_COMPILE_FLAGS}
            ${L2FLOW_SANITIZER_COMPILE_FLAGS})
    target_link_options(${target}
        PUBLIC ${L2FLOW_SANITIZER_LINK_FLAGS})
endfunction()

# Leaf utilities shared by the new production components. There is no build
# manifest or SDK baseline input: runtime SDK selection is a direct path load.
add_library(l2flow_common STATIC
    src/common/crc32c.cpp
    src/common/identity128.cpp
    src/common/sha256.cpp)
add_library(L2Flow::common ALIAS l2flow_common)
l2flow_configure_target(l2flow_common)
set_target_properties(l2flow_common PROPERTIES
    POSITION_INDEPENDENT_CODE ON)

# Physical SDK lifecycle and the single five-message subscription catalog.
add_library(l2flow_sdk STATIC
    src/sdk/direct_sdk_runtime_v1.cpp
    src/sdk/production_subscription_v1.cpp)
add_library(L2Flow::sdk ALIAS l2flow_sdk)
l2flow_configure_target(l2flow_sdk)
target_link_libraries(l2flow_sdk PUBLIC Threads::Threads)
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    target_link_libraries(l2flow_sdk PUBLIC ${CMAKE_DL_LIBS})
endif()

# Strict reconstruction of the vendor client's from-open CSV files into the
# same five MDL message tuples consumed by the realtime pipeline.
add_library(l2flow_recovery STATIC
    src/recovery/mdl_csv_startup_replay_v1.cpp)
add_library(L2Flow::recovery ALIAS l2flow_recovery)
l2flow_configure_target(l2flow_recovery)
target_link_libraries(l2flow_recovery PUBLIC L2Flow::sdk)

# Decoder, frozen daily runtime state, fixed-worker history, and
# whole-generation market publication.
add_library(l2flow_market STATIC
    src/control/checked_body_view.cpp
    src/market/daily_instrument_catalog_v2.cpp
    src/market/daily_instrument_catalog_loader_v2.cpp
    src/market/intraday_instrument_store_v1.cpp
    src/market/kline_aggregator_v1.cpp
    src/market/mainland_a_share_filter_v1.cpp
    src/market/market_decoder.cpp
    src/market/market_types_v1.cpp
    src/market/instrument_runtime_state_v2.cpp
    src/market/realtime_history_v1.cpp
    src/market/realtime_kline_v1.cpp
    src/market/realtime_latest_read_model_v1.cpp
    src/market/shanghai_order_event_aggregator_v1.cpp
    src/market/shenzhen_order_event_projector_v1.cpp)
add_library(L2Flow::market ALIAS l2flow_market)
l2flow_configure_target(l2flow_market)
set_target_properties(l2flow_market PROPERTIES
    POSITION_INDEPENDENT_CODE ON)
target_link_libraries(l2flow_market
    PUBLIC L2Flow::common Threads::Threads)

# The single-copy ownership boundary and contiguous completion tracker.
add_library(l2flow_realtime_ingress STATIC
    src/realtime/contiguous_sequence_tracker_v2.cpp
    src/realtime/native_sequence_recovery_v1.cpp
    src/realtime/owned_ingress_message_v1.cpp)
add_library(L2Flow::realtime_ingress ALIAS l2flow_realtime_ingress)
l2flow_configure_target(l2flow_realtime_ingress)
target_link_libraries(l2flow_realtime_ingress
    PUBLIC L2Flow::common L2Flow::sdk Threads::Threads)

# Generic history calculator boundary and one whole-generation atomic factor
# publisher. No Python passthrough placeholder is part of this target.
add_library(l2flow_factor STATIC
    src/factor/realtime_factor_engine_v1.cpp)
add_library(L2Flow::factor ALIAS l2flow_factor)
l2flow_configure_target(l2flow_factor)
target_link_libraries(l2flow_factor PUBLIC L2Flow::market)

# The production composition: serialized callback admission gives one
# immutable owned message directly to one of four source decoder FIFOs.
# Decoders drive fixed-worker History; parked all-lane fences establish exact
# generation watermarks before whole-generation Factor publication.
add_library(l2flow_realtime STATIC
    src/recovery/live_journal_v1.cpp
    src/recovery/online_recovery_v1.cpp
    src/runtime/realtime_pipeline_v1.cpp)
add_library(L2Flow::realtime ALIAS l2flow_realtime)
l2flow_configure_target(l2flow_realtime)
target_link_libraries(l2flow_realtime
    PUBLIC
        L2Flow::sdk
        L2Flow::recovery
        L2Flow::realtime_ingress
        L2Flow::market
        L2Flow::factor
        Threads::Threads)

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    # Cross-process read-only market data. This remains an application-layer
    # composition dependency; the core realtime pipeline has no edge back to
    # sockets, memfd, Python, Arrow, or Polars.
    add_library(l2flow_ipc STATIC
        src/ipc/certified_order_event_journal_v1.cpp
        src/ipc/certified_order_event_reader_v1.cpp
        src/ipc/certified_order_event_history_v1.cpp
        src/ipc/certified_order_event_wire_v1.cpp
        src/ipc/instrument_derived_event_history_v1.cpp
        src/ipc/instrument_derived_event_wire_v1.cpp
        src/ipc/order_event_delta_control_v1.cpp
        src/ipc/order_event_delta_ring_v1.cpp
        src/ipc/order_event_live_aggregation_engine_v1.cpp
        src/ipc/order_event_wire_adapter_v2.cpp
        src/ipc/realtime_certified_reader_v1.cpp
        src/ipc/realtime_certified_service_v1.cpp
        src/ipc/realtime_shared_service_v2.cpp)
    add_library(L2Flow::ipc ALIAS l2flow_ipc)
    l2flow_configure_target(l2flow_ipc)
    set_target_properties(l2flow_ipc PROPERTIES
        POSITION_INDEPENDENT_CODE ON)
    target_compile_definitions(l2flow_ipc PRIVATE _GNU_SOURCE)
    target_link_libraries(l2flow_ipc
        PUBLIC
            L2Flow::market
            L2Flow::realtime_ingress
            L2Flow::common
            Threads::Threads)

    # Stable native acquire/seqcount helper. Python loads this with ctypes;
    # it has no Python, Arrow, or Polars build dependency.
    add_library(l2flow_shm_reader SHARED
        src/ipc/instrument_derived_event_history_c_v1.cpp
        src/ipc/order_event_delta_ring_c_v1.cpp
        src/ipc/realtime_shm_reader_c_v2.cpp)
    l2flow_configure_target(l2flow_shm_reader)
    target_compile_definitions(l2flow_shm_reader PRIVATE _GNU_SOURCE)
    target_link_libraries(l2flow_shm_reader PRIVATE L2Flow::ipc)
    set_target_properties(l2flow_shm_reader PROPERTIES
        OUTPUT_NAME "l2flow_shm_reader"
        VERSION 2.0.0
        SOVERSION 2
        CXX_VISIBILITY_PRESET hidden
        VISIBILITY_INLINES_HIDDEN YES)

    # Formal C++ link surface for the stateful per-instrument derived-history
    # session.  The implementation reduces immutable raw Wire pages in C++;
    # the shared reader supplies the stable raw-history/C ABI used by both C++
    # and Python clients.
    add_library(l2flow_derived_history INTERFACE)
    add_library(L2Flow::derived_history ALIAS l2flow_derived_history)
    target_link_libraries(l2flow_derived_history
        INTERFACE L2Flow::ipc l2flow_shm_reader)
endif()

add_executable(mdl_production_router apps/mdl_production_main.cpp)
l2flow_configure_target(mdl_production_router)
target_link_libraries(mdl_production_router PRIVATE L2Flow::realtime)
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    target_compile_definitions(
        mdl_production_router
        PRIVATE L2FLOW_HAS_LINUX_REALTIME_IPC_V2=1)
    target_link_libraries(mdl_production_router PRIVATE L2Flow::ipc)
endif()
set_target_properties(mdl_production_router PROPERTIES
    OUTPUT_NAME "mdl-production-router")

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    add_executable(mdl_order_event_aggregator
        apps/mdl_order_event_aggregator_main.cpp)
    l2flow_configure_target(mdl_order_event_aggregator)
    target_compile_definitions(
        mdl_order_event_aggregator PRIVATE _GNU_SOURCE)
    target_link_libraries(
        mdl_order_event_aggregator
        PRIVATE L2Flow::ipc l2flow_shm_reader)
    set_target_properties(mdl_order_event_aggregator PROPERTIES
        OUTPUT_NAME "mdl-order-event-aggregator")
endif()

if(L2FLOW_BUILD_FEEDER_PROBE)
    add_executable(mdl_sdk_feeder_probe tools/mdl_sdk_feeder_probe.cpp)
    l2flow_configure_target(mdl_sdk_feeder_probe)
    target_link_libraries(mdl_sdk_feeder_probe PRIVATE L2Flow::sdk)
    set_target_properties(mdl_sdk_feeder_probe PROPERTIES
        OUTPUT_NAME "mdl-sdk-feeder-probe")
endif()

if(L2FLOW_BUILD_MARKET_DECODER_LIBFUZZER)
    if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
        message(FATAL_ERROR
            "L2FLOW_BUILD_MARKET_DECODER_LIBFUZZER requires Clang")
    endif()
    add_executable(fuzz_market_decoder
        tests/fuzz/market_decoder_fuzz_target.cpp)
    l2flow_configure_target(fuzz_market_decoder)
    target_link_libraries(fuzz_market_decoder PRIVATE L2Flow::market)
    target_compile_options(fuzz_market_decoder PRIVATE -fsanitize=fuzzer)
    target_link_options(fuzz_market_decoder PRIVATE -fsanitize=fuzzer)
endif()

if(L2MOCK_BUILD_TESTS)
    add_library(l2mock STATIC src/l2_mock.cpp src/sdk_compat.cpp)
    add_library(L2Flow::l2mock ALIAS l2mock)
    l2flow_configure_target(l2mock)
    target_compile_definitions(l2mock
        PRIVATE L2MOCK_BUILDING_STANDALONE_SDK_COMPAT=1)
    target_link_libraries(l2mock PUBLIC Threads::Threads)
endif()

if(L2FLOW_BUILD_TESTS OR L2MOCK_BUILD_TESTS)
    enable_testing()
endif()

function(l2flow_add_test target source library)
    add_executable(${target} ${source})
    l2flow_configure_target(${target})
    target_link_libraries(${target} PRIVATE ${library})
    add_test(NAME ${target} COMMAND ${target})
endfunction()

if(L2FLOW_BUILD_TESTS)
    l2flow_add_test(test_crc32c
        tests/test_crc32c.cpp L2Flow::common)
    l2flow_add_test(test_identity128
        tests/test_phase2_identity128.cpp L2Flow::common)
    l2flow_add_test(test_sha256
        tests/test_sha256.cpp L2Flow::common)
    l2flow_add_test(test_market_decoder
        tests/test_phase4_market_decoder.cpp L2Flow::market)
    l2flow_add_test(test_mainland_a_share_filter
        tests/test_mainland_a_share_filter_v1.cpp L2Flow::market)
    l2flow_add_test(test_daily_instrument_catalog
        tests/test_daily_instrument_catalog_v2.cpp L2Flow::market)
    l2flow_add_test(test_instrument_runtime_state
        tests/test_instrument_runtime_state_v2.cpp L2Flow::market)
    l2flow_add_test(test_observed_instrument_router
        tests/test_observed_instrument_router_v2.cpp L2Flow::market)

    # A deterministic, compiler-independent fuzz smoke keeps all five decoder
    # selectors, schema gates, fixed-size boundaries, and 50k pseudorandom
    # bodies in the normal regression suite. The separate optional libFuzzer
    # target above supplies its own main and therefore intentionally excludes
    # this harness.
    add_executable(test_market_decoder_fuzz_smoke
        tests/fuzz/market_decoder_fuzz_harness.cpp
        tests/fuzz/market_decoder_fuzz_target.cpp)
    l2flow_configure_target(test_market_decoder_fuzz_smoke)
    target_link_libraries(test_market_decoder_fuzz_smoke
        PRIVATE L2Flow::market)
    add_test(NAME test_market_decoder_fuzz_smoke
        COMMAND test_market_decoder_fuzz_smoke)

    # Exercise the production dlopen/dlsym adapter against a real test-only
    # DSO.  Passing the generated path at test time avoids embedding or
    # approving any SDK bytes in the production target.
    add_library(test_sdk_direct_runtime_dso SHARED
        tests/test_sdk_direct_runtime_dso.cpp)
    l2flow_configure_target(test_sdk_direct_runtime_dso)
    set_target_properties(test_sdk_direct_runtime_dso PROPERTIES
        CXX_VISIBILITY_PRESET hidden
        VISIBILITY_INLINES_HIDDEN YES)
    target_link_libraries(test_sdk_direct_runtime_dso
        PRIVATE Threads::Threads)

    add_executable(test_sdk_direct_runtime
        tests/test_sdk_direct_runtime_v1.cpp)
    l2flow_configure_target(test_sdk_direct_runtime)
    target_link_libraries(test_sdk_direct_runtime PRIVATE L2Flow::sdk)
    add_dependencies(test_sdk_direct_runtime test_sdk_direct_runtime_dso)
    add_test(NAME test_sdk_direct_runtime
        COMMAND test_sdk_direct_runtime
            $<TARGET_FILE:test_sdk_direct_runtime_dso>)

    l2flow_add_test(test_contiguous_sequence_tracker
        tests/test_contiguous_sequence_tracker_v2.cpp
        L2Flow::realtime_ingress)
    l2flow_add_test(test_native_sequence_recovery
        tests/test_native_sequence_recovery_v1.cpp
        L2Flow::realtime_ingress)
    l2flow_add_test(test_owned_ingress_message_pool
        tests/test_owned_ingress_message_pool_v1.cpp
        L2Flow::realtime_ingress)
    l2flow_add_test(test_closeable_publication_gate
        tests/test_closeable_publication_gate.cpp Threads::Threads)
    l2flow_add_test(test_realtime_history
        tests/test_realtime_history_v1.cpp L2Flow::market)
    l2flow_add_test(test_realtime_latest_read_model
        tests/test_realtime_latest_read_model_v1.cpp L2Flow::market)
    l2flow_add_test(test_kline_aggregator
        tests/test_kline_aggregator_v1.cpp L2Flow::market)
    l2flow_add_test(test_intraday_instrument_store
        tests/test_intraday_instrument_store_v1.cpp L2Flow::market)
    l2flow_add_test(test_shanghai_order_event_aggregator
        tests/test_shanghai_order_event_aggregator_v1.cpp L2Flow::market)
    l2flow_add_test(test_shenzhen_order_event_projector
        tests/test_shenzhen_order_event_projector_v1.cpp L2Flow::market)
    l2flow_add_test(test_realtime_factor_engine
        tests/test_realtime_factor_engine_v1.cpp L2Flow::factor)
    l2flow_add_test(test_realtime_pipeline
        tests/test_daily_catalog_pipeline_v2.cpp L2Flow::realtime)
    l2flow_add_test(test_realtime_pipeline_lifecycle_concurrency
        tests/test_realtime_pipeline_lifecycle_concurrency_v1.cpp
        L2Flow::realtime)
    l2flow_add_test(test_live_journal_v1
        tests/test_live_journal_v1.cpp L2Flow::realtime)
    l2flow_add_test(test_online_recovery_v1
        tests/test_online_recovery_v1.cpp L2Flow::realtime)
    l2flow_add_test(test_mdl_csv_startup_replay
        tests/test_mdl_csv_replay_v1.cpp
        L2Flow::recovery)
    target_link_libraries(test_mdl_csv_startup_replay
        PRIVATE L2Flow::market)
    if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
        l2flow_add_test(test_realtime_wire_v2
            tests/test_realtime_wire_v2.cpp L2Flow::common)
        l2flow_add_test(test_realtime_certified_wire_v1
            tests/test_realtime_certified_wire_v1.cpp
            L2Flow::common)
        l2flow_add_test(test_realtime_certified_reader_v1
            tests/test_realtime_certified_reader_v1.cpp
            L2Flow::ipc)
        l2flow_add_test(test_certified_order_event_journal_v1
            tests/test_certified_order_event_journal_v1.cpp
            L2Flow::ipc)
        l2flow_add_test(test_realtime_certified_service_v1
            tests/test_realtime_certified_service_v1.cpp
            L2Flow::ipc)
        target_link_libraries(test_realtime_certified_service_v1
            PRIVATE L2Flow::realtime)
        # Explicit, bounded latency probe. It is intentionally not registered
        # with ctest because stable measurements require operator-selected CPU
        # affinity and repeated/interleaved runs.
        add_executable(benchmark_realtime_certified_v1
            tests/benchmark_realtime_certified_v1.cpp)
        l2flow_configure_target(benchmark_realtime_certified_v1)
        target_link_libraries(benchmark_realtime_certified_v1
            PRIVATE L2Flow::ipc L2Flow::realtime)
        l2flow_add_test(test_certified_order_event_history_v1
            tests/test_certified_order_event_history_v1.cpp
            L2Flow::ipc)
        l2flow_add_test(test_realtime_history_wire_v2
            tests/test_realtime_history_wire_v2.cpp L2Flow::common)
        l2flow_add_test(test_order_event_wire_adapter_v2
            tests/test_order_event_wire_adapter_v2.cpp L2Flow::ipc)
        l2flow_add_test(test_order_event_delta_ring_v1
            tests/test_order_event_delta_ring_v1.cpp L2Flow::ipc)
        l2flow_add_test(test_order_event_delta_control_v1
            tests/test_order_event_delta_control_v1.cpp L2Flow::ipc)
        l2flow_add_test(test_order_event_delta_ring_c_v1
            tests/test_order_event_delta_ring_c_v1.cpp
            L2Flow::derived_history)
        l2flow_add_test(test_order_event_live_aggregation_engine_v1
            tests/test_order_event_live_aggregation_engine_v1.cpp
            L2Flow::ipc)
        l2flow_add_test(test_mdl_order_event_aggregator_cli_v1
            tests/test_mdl_order_event_aggregator_cli_v1.cpp
            L2Flow::ipc)
        add_executable(test_mdl_order_event_aggregator_process_v1
            tests/test_mdl_order_event_aggregator_process_v1.cpp)
        l2flow_configure_target(
            test_mdl_order_event_aggregator_process_v1)
        target_compile_definitions(
            test_mdl_order_event_aggregator_process_v1
            PRIVATE _GNU_SOURCE)
        target_link_libraries(
            test_mdl_order_event_aggregator_process_v1
            PRIVATE L2Flow::ipc)
        add_dependencies(
            test_mdl_order_event_aggregator_process_v1
            mdl_order_event_aggregator)
        add_test(
            NAME test_mdl_order_event_aggregator_process_v1
            COMMAND
                test_mdl_order_event_aggregator_process_v1
                $<TARGET_FILE:mdl_order_event_aggregator>)
        add_test(
            NAME test_mdl_production_router_cli_v1
            COMMAND
                ${CMAKE_COMMAND}
                -DROUTER=$<TARGET_FILE:mdl_production_router>
                -P
                ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_mdl_production_router_cli_v1.cmake)
        l2flow_add_test(test_realtime_shm_reader_v2
            tests/test_realtime_shm_reader_v2.cpp
            l2flow_shm_reader)
        l2flow_add_test(test_realtime_shared_service_v2
            tests/test_realtime_shared_service_v2.cpp
            L2Flow::derived_history)
        target_link_libraries(test_realtime_shared_service_v2
            PRIVATE L2Flow::realtime)

        # The Python client core and its regression suite are stdlib-only.
        # Missing Python, PyArrow, or Polars never blocks the C++ build.
        find_package(Python3 COMPONENTS Interpreter QUIET)
        if(Python3_Interpreter_FOUND)
            add_executable(test_order_event_delta_live_python_host_v1
                tests/order_event_delta_live_python_host_v1.cpp)
            l2flow_configure_target(
                test_order_event_delta_live_python_host_v1)
            target_link_libraries(
                test_order_event_delta_live_python_host_v1
                PRIVATE L2Flow::ipc)
            add_test(
                NAME test_order_event_delta_live_python_e2e
                COMMAND
                    test_order_event_delta_live_python_host_v1
                    ${Python3_EXECUTABLE}
                    ${CMAKE_CURRENT_SOURCE_DIR}/tests/python/order_event_delta_live_probe.py
                    $<TARGET_FILE:l2flow_shm_reader>)
            set_tests_properties(
                test_order_event_delta_live_python_e2e
                PROPERTIES
                    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
                    ENVIRONMENT
                        "PYTHONDONTWRITEBYTECODE=1;PYTHONPATH=${CMAKE_CURRENT_SOURCE_DIR}/python")
            target_compile_definitions(
                test_realtime_shared_service_v2
                PRIVATE
                    L2FLOW_V2_PYTHON_PROBE_EXECUTABLE="${Python3_EXECUTABLE}"
                    L2FLOW_V2_PYTHON_PROBE_SCRIPT="${CMAKE_CURRENT_SOURCE_DIR}/tests/python/realtime_hot_read_probe.py"
                    L2FLOW_V2_PYTHON_LATENCY_SCRIPT="${CMAKE_CURRENT_SOURCE_DIR}/tests/python/realtime_latency_benchmark_probe.py"
                    L2FLOW_V2_PYTHON_HISTORY_LATENCY_SCRIPT="${CMAKE_CURRENT_SOURCE_DIR}/tests/python/realtime_history_latency_benchmark_probe.py"
                    L2FLOW_V2_PYTHON_DERIVED_HISTORY_SCRIPT="${CMAKE_CURRENT_SOURCE_DIR}/tests/python/derived_event_history_probe.py"
                    L2FLOW_V2_PYTHON_SOURCE="${CMAKE_CURRENT_SOURCE_DIR}/python"
                    L2FLOW_V2_PYTHON_READER_LIBRARY="$<TARGET_FILE:l2flow_shm_reader>")
            add_test(
                NAME test_l2flow_realtime_python
                COMMAND
                    ${Python3_EXECUTABLE} -m unittest discover
                    -s tests/python
                    -p "test_l2flow_*.py"
                    -v)
            set_tests_properties(
                test_l2flow_realtime_python
                PROPERTIES
                    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
            set_property(
                TEST test_l2flow_realtime_python
                PROPERTY ENVIRONMENT
                    "PYTHONDONTWRITEBYTECODE=1"
                    "PYTHONPATH=${CMAKE_CURRENT_SOURCE_DIR}/python"
                    "L2FLOW_SHM_READER_LIBRARY=$<TARGET_FILE:l2flow_shm_reader>")
        endif()
    endif()
endif()

if(L2MOCK_BUILD_TESTS)
    foreach(mock_test IN ITEMS
            test_abi
            test_messages
            test_subscription
            test_determinism_stress
            test_config
            test_numeric_edges
            test_concurrency_edges
            test_review_runtime
            test_review_subscription)
        l2flow_add_test(${mock_test}
            tests/${mock_test}.cpp L2Flow::l2mock)
    endforeach()
endif()
