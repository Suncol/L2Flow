cmake_minimum_required(VERSION 3.16)

project(L2Flow VERSION 1.0.0 LANGUAGES CXX)

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

# Decoder, immutable registry, fixed-worker history, and whole-generation
# market publication. It has no dependency on WAL or factor code.
add_library(l2flow_market STATIC
    src/control/checked_body_view.cpp
    src/market/instrument_registry.cpp
    src/market/instrument_registry_loader_v1.cpp
    src/market/intraday_instrument_store_v1.cpp
    src/market/market_decoder.cpp
    src/market/market_types_v1.cpp
    src/market/realtime_history_v1.cpp)
add_library(L2Flow::market ALIAS l2flow_market)
l2flow_configure_target(l2flow_market)
target_link_libraries(l2flow_market
    PUBLIC L2Flow::common Threads::Threads)

# The ownership boundary and optional audit side branch. WAL consumes the
# immutable ingress handle and has no edge back into decoder/history.
add_library(l2flow_realtime_ingress STATIC
    src/realtime/owned_ingress_message_v1.cpp
    src/realtime/optional_wal_sink_v1.cpp)
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

# The only production composition:
# SDK -> owned ingress -> optional WAL + decoder -> fixed worker history
# -> generation barrier/watermark -> factor -> atomic publication.
add_library(l2flow_realtime STATIC
    src/runtime/realtime_pipeline_v1.cpp)
add_library(L2Flow::realtime ALIAS l2flow_realtime)
l2flow_configure_target(l2flow_realtime)
target_link_libraries(l2flow_realtime
    PUBLIC
        L2Flow::sdk
        L2Flow::realtime_ingress
        L2Flow::market
        L2Flow::factor
        Threads::Threads)

add_executable(mdl_production_router apps/mdl_production_main.cpp)
l2flow_configure_target(mdl_production_router)
target_link_libraries(mdl_production_router PRIVATE L2Flow::realtime)
set_target_properties(mdl_production_router PROPERTIES
    OUTPUT_NAME "mdl-production-router")

# Operator-run acceptance probe. It uses the exact production composition but
# keeps measurement/reporting code out of the latency-sensitive runtime.
add_executable(accept_realtime_pipeline
    tools/accept_realtime_pipeline.cpp)
l2flow_configure_target(accept_realtime_pipeline)
target_link_libraries(accept_realtime_pipeline PRIVATE L2Flow::realtime)
set_target_properties(accept_realtime_pipeline PROPERTIES
    OUTPUT_NAME "accept-realtime-pipeline")

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
    l2flow_add_test(test_instrument_registry
        tests/test_phase4_instrument_registry.cpp L2Flow::market)
    l2flow_add_test(test_instrument_registry_loader
        tests/test_phase4_instrument_registry_loader_v1.cpp L2Flow::market)
    l2flow_add_test(test_market_decoder
        tests/test_phase4_market_decoder.cpp L2Flow::market)

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

    l2flow_add_test(test_owned_ingress_wal
        tests/test_owned_ingress_wal_v1.cpp L2Flow::realtime_ingress)
    l2flow_add_test(test_realtime_history
        tests/test_realtime_history_v1.cpp L2Flow::market)
    l2flow_add_test(test_intraday_instrument_store
        tests/test_intraday_instrument_store_v1.cpp L2Flow::market)
    l2flow_add_test(test_realtime_factor_engine
        tests/test_realtime_factor_engine_v1.cpp L2Flow::factor)
    l2flow_add_test(test_realtime_pipeline
        tests/test_realtime_pipeline_v1.cpp L2Flow::realtime)
    l2flow_add_test(test_realtime_pipeline_lifecycle_concurrency
        tests/test_realtime_pipeline_lifecycle_concurrency_v1.cpp
        L2Flow::realtime)
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
