cmake_minimum_required(VERSION 3.16)

project(L2Flow VERSION 3.0.0 LANGUAGES CXX)

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
option(L2FLOW_BUILD_BENCHMARKS
       "Build opt-in FAST/Event/KLine reorder benchmarks" OFF)
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
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    target_sources(l2flow_common PRIVATE
        src/common/linux_thread_affinity_v1.cpp)
    target_compile_definitions(l2flow_common PRIVATE _GNU_SOURCE)
    target_link_libraries(l2flow_common PUBLIC Threads::Threads)
endif()

# Physical SDK lifecycle and the single three-message Tick subscription
# catalog. FAST Tick is the sole original fact source.
add_library(l2flow_sdk STATIC
    src/sdk/direct_sdk_runtime_v1.cpp
    src/sdk/production_subscription_v1.cpp)
add_library(L2Flow::sdk ALIAS l2flow_sdk)
l2flow_configure_target(l2flow_sdk)
target_link_libraries(l2flow_sdk PUBLIC Threads::Threads)
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    target_link_libraries(l2flow_sdk PUBLIC ${CMAKE_DL_LIBS})
endif()

# Tick-only decoder plus the three instrument-local stores. FAST Tick history
# is the sole recovery source; there is no Snapshot or generation target.
add_library(l2flow_market STATIC
    src/control/checked_body_view.cpp
    src/market/daily_instrument_catalog_v2.cpp
    src/market/daily_instrument_catalog_loader_v2.cpp
    src/market/fast_tick_store_v1.cpp
    src/market/kline_projection_v1.cpp
    src/market/mainland_a_share_filter_v1.cpp
    src/market/market_decoder.cpp
    src/market/market_types_v1.cpp
    src/market/mutable_kline_history_v1.cpp
    src/market/ordered_event_history_v1.cpp
    src/market/shanghai_order_event_aggregator_v1.cpp
    src/market/shenzhen_order_event_projector_v1.cpp)
add_library(L2Flow::market ALIAS l2flow_market)
l2flow_configure_target(l2flow_market)
set_target_properties(l2flow_market PROPERTIES
    POSITION_INDEPENDENT_CODE ON)
target_link_libraries(l2flow_market
    PUBLIC L2Flow::common Threads::Threads)

# The single-copy ownership boundary for source-by-Tick-worker raw shards.
add_library(l2flow_realtime_ingress STATIC
    src/realtime/owned_ingress_message_v1.cpp)
add_library(L2Flow::realtime_ingress ALIAS l2flow_realtime_ingress)
l2flow_configure_target(l2flow_realtime_ingress)
target_link_libraries(l2flow_realtime_ingress
    PUBLIC L2Flow::common L2Flow::sdk Threads::Threads)

# The production composition: serialized callback admission gives each
# immutable owned Tick directly to its fixed Tick-worker shard. That worker
# decodes once, publishes FAST, then nonblockingly fans compact rows to the
# independent Event and KLine planes.
add_library(l2flow_realtime STATIC
    src/runtime/fast_tick_pipeline_v1.cpp
    src/runtime/realtime_planes_v1.cpp)
add_library(L2Flow::realtime ALIAS l2flow_realtime)
l2flow_configure_target(l2flow_realtime)
target_link_libraries(l2flow_realtime
    PUBLIC
        L2Flow::market
        L2Flow::common
        L2Flow::sdk
        L2Flow::realtime_ingress
        Threads::Threads)

# V3 is intentionally instrument-local. It exposes independent FAST append,
# Event CDC, and KLine CDC cursors and contains no global live ring or cut.
add_library(l2flow_ipc STATIC
    src/ipc/instrument_data_service_v3.cpp
    src/ipc/realtime_wire_v3.cpp)
add_library(L2Flow::ipc ALIAS l2flow_ipc)
l2flow_configure_target(l2flow_ipc)
set_target_properties(l2flow_ipc PROPERTIES
    POSITION_INDEPENDENT_CODE ON)
target_link_libraries(l2flow_ipc
    PUBLIC L2Flow::realtime L2Flow::market L2Flow::common Threads::Threads)

add_executable(mdl_production_router
    apps/mdl_production_main.cpp)
l2flow_configure_target(mdl_production_router)
target_link_libraries(mdl_production_router PRIVATE L2Flow::realtime)
set_target_properties(mdl_production_router PROPERTIES
    OUTPUT_NAME "mdl-production-router")

if(L2FLOW_BUILD_BENCHMARKS)
    # ctypes loads this benchmark-only bridge into the repository-local
    # Python process so callback admission, V3 Event reads, and real Polars
    # materialization can be timed on one monotonic clock. It is deliberately
    # not installed and is not a production transport ABI.
    set_target_properties(
        l2flow_sdk
        l2flow_realtime_ingress
        l2flow_realtime
        PROPERTIES POSITION_INDEPENDENT_CODE ON)
    add_library(l2flow_callback_polars_benchmark SHARED
        benchmarks/callback_polars_bridge_v1.cpp)
    l2flow_configure_target(l2flow_callback_polars_benchmark)
    target_include_directories(l2flow_callback_polars_benchmark
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks)
    target_link_libraries(l2flow_callback_polars_benchmark
        PRIVATE L2Flow::ipc)
    if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        # The benchmark is dlopened by the repository-local Python process.
        # Keeping the GCC runtime private prevents whichever libstdc++ loaded
        # Python extensions first from changing this benchmark's ABI/runtime.
        target_link_options(l2flow_callback_polars_benchmark
            PRIVATE -static-libstdc++ -static-libgcc)
    endif()
    set_target_properties(l2flow_callback_polars_benchmark PROPERTIES
        OUTPUT_NAME "l2flow_callback_polars_benchmark")

    add_executable(benchmark_realtime_planes
        benchmarks/benchmark_realtime_planes_v1.cpp)
    l2flow_configure_target(benchmark_realtime_planes)
    target_link_libraries(benchmark_realtime_planes
        PRIVATE L2Flow::realtime)
    set_target_properties(benchmark_realtime_planes PROPERTIES
        OUTPUT_NAME "benchmark-realtime-planes")
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
    l2flow_add_test(test_fast_source_derived_history
        tests/test_fast_source_derived_history_v1.cpp L2Flow::market)
    l2flow_add_test(test_business_order_event_rules
        tests/test_business_order_event_rules_v1.cpp L2Flow::market)
    l2flow_add_test(test_fast_tick_pipeline
        tests/test_fast_tick_pipeline_v1.cpp L2Flow::realtime)
    l2flow_add_test(test_instrument_data_service_v3
        tests/test_instrument_data_service_v3.cpp L2Flow::ipc)
    l2flow_add_test(test_realtime_plane_recovery
        tests/test_realtime_plane_recovery_v1.cpp L2Flow::realtime)
    l2flow_add_test(test_crc32c
        tests/test_crc32c.cpp L2Flow::common)
    l2flow_add_test(test_identity128
        tests/test_phase2_identity128.cpp L2Flow::common)
    l2flow_add_test(test_sha256
        tests/test_sha256.cpp L2Flow::common)
    l2flow_add_test(test_mainland_a_share_filter
        tests/test_mainland_a_share_filter_v1.cpp L2Flow::market)
    l2flow_add_test(test_owned_ingress_message_pool
        tests/test_owned_ingress_message_pool_v1.cpp
        L2Flow::realtime_ingress)

    # A deterministic, compiler-independent fuzz smoke keeps all three decoder
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

    add_test(
        NAME test_mdl_production_router_cli_v1
        COMMAND
            ${CMAKE_COMMAND}
            -DROUTER=$<TARGET_FILE:mdl_production_router>
            -P
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_mdl_production_router_cli_v1.cmake)

    find_package(Python3 COMPONENTS Interpreter QUIET)
    if(Python3_Interpreter_FOUND)
        add_test(
            NAME test_l2flow_realtime_python_v3
            COMMAND
                ${Python3_EXECUTABLE} -m unittest discover
                -s tests/python
                -p "test_l2flow_realtime_v3.py"
                -v)
        set_tests_properties(
            test_l2flow_realtime_python_v3
            PROPERTIES
                WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
                ENVIRONMENT
                    "PYTHONDONTWRITEBYTECODE=1;PYTHONPATH=${CMAKE_CURRENT_SOURCE_DIR}/python")
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
