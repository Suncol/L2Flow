cmake_minimum_required(VERSION 3.16)

project(L2Flow VERSION 0.1.0 LANGUAGES CXX)

option(L2MOCK_BUILD_TESTS "Build the L2 mock verification suite" ON)
option(L2MOCK_ENABLE_SANITIZERS "Enable Address/Undefined sanitizers" OFF)
option(L2FLOW_BUILD_TESTS "Build the Phase 0/1/2 verification suite" ON)
option(
    L2FLOW_BUILD_RAW_V1_LIBFUZZER
    "Build the optional Clang libFuzzer target for Raw V1 input safety"
    OFF)
option(
    L2FLOW_ENABLE_ASAN_UBSAN
    "Enable AddressSanitizer and UndefinedBehaviorSanitizer"
    OFF)
option(L2FLOW_ENABLE_TSAN "Enable ThreadSanitizer" OFF)

if(L2FLOW_ENABLE_TSAN AND
   (L2FLOW_ENABLE_ASAN_UBSAN OR L2MOCK_ENABLE_SANITIZERS))
    message(FATAL_ERROR
        "ASan+UBSan and TSan are separate build modes and cannot be enabled "
        "together")
endif()

if(MSVC AND
   (L2FLOW_ENABLE_ASAN_UBSAN OR
    L2FLOW_ENABLE_TSAN OR
    L2MOCK_ENABLE_SANITIZERS))
    message(FATAL_ERROR
        "The configured L2Flow sanitizer modes require GCC or Clang")
endif()

set(L2FLOW_STRICT_COMPILE_FLAGS)
if(MSVC)
    set(L2FLOW_STRICT_COMPILE_FLAGS /W4 /permissive- /WX)
else()
    set(L2FLOW_STRICT_COMPILE_FLAGS
        -Wall
        -Wextra
        -Wpedantic
        -Wconversion
        -Wshadow
        -Werror)
endif()

set(L2FLOW_SANITIZER_MODE "none")
set(L2FLOW_SANITIZER_COMPILE_FLAGS)
set(L2FLOW_SANITIZER_LINK_FLAGS)
if(L2FLOW_ENABLE_ASAN_UBSAN OR L2MOCK_ENABLE_SANITIZERS)
    set(L2FLOW_SANITIZER_MODE "address+undefined")
    set(L2FLOW_SANITIZER_COMPILE_FLAGS
        -fno-omit-frame-pointer
        -fsanitize=address,undefined)
    set(L2FLOW_SANITIZER_LINK_FLAGS
        -fno-omit-frame-pointer
        -fsanitize=address,undefined)
elseif(L2FLOW_ENABLE_TSAN)
    set(L2FLOW_SANITIZER_MODE "thread")
    set(L2FLOW_SANITIZER_COMPILE_FLAGS
        -fno-omit-frame-pointer
        -fsanitize=thread)
    # GCC diagnoses atomic_thread_fence because TSan does not instrument that
    # primitive. Keep the diagnostic visible, but do not let the project's
    # global -Werror make the dedicated TSan build unusable; acquire/release
    # atomic operations and the controller mutex/thread interactions remain
    # instrumented.
    if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        list(APPEND L2FLOW_SANITIZER_COMPILE_FLAGS -Wno-error=tsan)
    endif()
    set(L2FLOW_SANITIZER_LINK_FLAGS
        -fno-omit-frame-pointer
        -fsanitize=thread)
endif()

find_package(Threads REQUIRED)
find_package(Python3 REQUIRED COMPONENTS Interpreter)

set(L2FLOW_VENDOR_LIBRARY_PATH
    "${CMAKE_CURRENT_SOURCE_DIR}/mdl_sdk_2_13_234/libs/linux/libmdl_api.so"
    CACHE FILEPATH
    "DataYes libmdl_api.so used by optional direct-link probes")

# Git LFS leaves a small pointer file at the configured path when the vendor
# object has not been materialized.  Merely checking EXISTS would then create
# optional direct-link targets and make the linker consume that text file.
# Keep those targets optional unless the candidate starts with the ELF magic;
# the Phase-0 runtime gate still examines the configured repository artifact
# independently and therefore continues to fail closed for pointers or other
# malformed files.
set(L2FLOW_VENDOR_LIBRARY_IS_ELF OFF)
if(EXISTS "${L2FLOW_VENDOR_LIBRARY_PATH}" AND
   NOT IS_DIRECTORY "${L2FLOW_VENDOR_LIBRARY_PATH}")
    file(READ
        "${L2FLOW_VENDOR_LIBRARY_PATH}"
        L2FLOW_VENDOR_LIBRARY_MAGIC
        OFFSET 0
        LIMIT 4
        HEX)
    string(TOLOWER
        "${L2FLOW_VENDOR_LIBRARY_MAGIC}"
        L2FLOW_VENDOR_LIBRARY_MAGIC)
    if(L2FLOW_VENDOR_LIBRARY_MAGIC STREQUAL "7f454c46")
        set(L2FLOW_VENDOR_LIBRARY_IS_ELF ON)
    endif()
endif()

include(cmake/L2FlowBuildManifest.cmake)
set(L2FLOW_GENERATED_INCLUDE_DIR
    "${CMAKE_CURRENT_BINARY_DIR}/generated/include")
l2flow_generate_build_manifest(
    OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/build_manifest.json"
    HEADER
        "${L2FLOW_GENERATED_INCLUDE_DIR}/l2flow/build_manifest.h"
    HEADER_TEMPLATE
        "${CMAKE_CURRENT_SOURCE_DIR}/cmake/build_manifest.h.in"
    BASELINE
        "${CMAKE_CURRENT_SOURCE_DIR}/configs/vendor_baseline.json"
    BASELINE_LABEL "configs/vendor_baseline.json"
    CXX_STANDARD 20
    SANITIZER_MODE "${L2FLOW_SANITIZER_MODE}"
    STRICT_COMPILE_FLAGS ${L2FLOW_STRICT_COMPILE_FLAGS}
    SANITIZER_COMPILE_FLAGS ${L2FLOW_SANITIZER_COMPILE_FLAGS}
    SANITIZER_LINK_FLAGS ${L2FLOW_SANITIZER_LINK_FLAGS})

add_library(l2mock STATIC
    src/l2_mock.cpp)
add_library(L2Flow::l2mock ALIAS l2mock)

target_compile_features(l2mock PUBLIC cxx_std_17)
target_include_directories(l2mock
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)
target_include_directories(l2mock
    SYSTEM PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/mdl_sdk_2_13_234/include>)
target_link_libraries(l2mock PUBLIC Threads::Threads)

# The SDK headers expose a few non-inline helpers implemented by
# libmdl_api.so.  Mock-only programs can opt in to local fallbacks through the
# standalone target below.  They are deliberately not part of `l2mock`: a
# process that switches between the real and mock factories must link
# L2Flow::l2mock plus the vendor library, without executable-level fallback
# symbols that could preempt the vendor's encoding conversions.
add_library(l2mock_sdk_compat STATIC
    src/sdk_compat.cpp)
target_compile_features(l2mock_sdk_compat PUBLIC cxx_std_17)
target_compile_definitions(l2mock_sdk_compat
    PRIVATE L2MOCK_BUILDING_STANDALONE_SDK_COMPAT=1)
target_include_directories(l2mock_sdk_compat
    SYSTEM PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/mdl_sdk_2_13_234/include)

add_library(l2mock_standalone INTERFACE)
add_library(L2Flow::l2mock_standalone ALIAS l2mock_standalone)
target_link_libraries(l2mock_standalone
    INTERFACE
        L2Flow::l2mock
        l2mock_sdk_compat)

function(l2mock_enable_strict_warnings target)
    if(MSVC)
        target_compile_options(${target} PRIVATE /W4 /permissive-)
    else()
        target_compile_options(${target} PRIVATE
            -Wall
            -Wextra
            -Wpedantic
            -Werror)
    endif()
endfunction()

function(l2flow_enable_strict_warnings target)
    target_compile_options(
        ${target}
        PRIVATE
            ${L2FLOW_STRICT_COMPILE_FLAGS})
endfunction()

function(l2flow_enable_sanitizers target)
    if(NOT L2FLOW_SANITIZER_MODE STREQUAL "none")
        target_compile_options(
            ${target}
            PUBLIC
                ${L2FLOW_SANITIZER_COMPILE_FLAGS})
        target_link_options(
            ${target}
            PUBLIC
                ${L2FLOW_SANITIZER_LINK_FLAGS})
    endif()
endfunction()

l2mock_enable_strict_warnings(l2mock)
l2mock_enable_strict_warnings(l2mock_sdk_compat)

l2flow_enable_sanitizers(l2mock)
l2flow_enable_sanitizers(l2mock_sdk_compat)

set(L2FLOW_PHASE01_SOURCES
    src/common/sealed_file_snapshot.cpp
    src/common/sha256.cpp
    src/baseline/vendor_baseline.cpp
    src/ingress/byte_ring.cpp
    src/ingress/callback_handler.cpp
    src/ingress/capture_clock.cpp
    src/ingress/capture_metrics.cpp
    src/ingress/clock_epoch.cpp
    src/ingress/ingress_app.cpp
    src/ingress/shadow_capture.cpp
    src/ops/credential.cpp
    src/ops/metrics_textfile.cpp
    src/ops/metrics_worker.cpp
    src/ops/stable_output_prefix.cpp
    src/ops/systemd_notify.cpp
    src/sdk/endpoint_contract.cpp
    src/sdk/ingress_config.cpp
    src/sdk/sdk_runtime.cpp
    src/sdk/subscription_manifest.cpp)

add_library(l2flow_phase01 STATIC ${L2FLOW_PHASE01_SOURCES})
add_library(L2Flow::phase01 ALIAS l2flow_phase01)
set_target_properties(l2flow_phase01 PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED YES
    CXX_EXTENSIONS NO)
target_compile_features(l2flow_phase01 PUBLIC cxx_std_20)
target_include_directories(l2flow_phase01
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${L2FLOW_GENERATED_INCLUDE_DIR}>)
target_include_directories(l2flow_phase01
    SYSTEM PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/mdl_sdk_2_13_234/include>)
target_link_libraries(l2flow_phase01 PUBLIC Threads::Threads)
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    target_link_libraries(l2flow_phase01 PRIVATE ${CMAKE_DL_LIBS})
endif()
l2flow_enable_strict_warnings(l2flow_phase01)
l2flow_enable_sanitizers(l2flow_phase01)

set(L2FLOW_PHASE2_SOURCES
    src/common/crc32c.cpp
    src/common/identity128.cpp
    src/ingress/empty_anchor_tombstone_store.cpp
    src/ingress/empty_anchor_tombstone_v1.cpp
    src/ingress/finalization_archive_posix_store.cpp
    src/ingress/finalization_archive_source_cleanup_posix.cpp
    src/ingress/finalization_archive_v1.cpp
    src/ingress/finalization_report_store.cpp
    src/ingress/finalization_report_v1.cpp
    src/ingress/raw_control_page.cpp
    src/ingress/raw_control_file.cpp
    src/ingress/raw_capture_worker.cpp
    src/ingress/raw_clean_stop_gate.cpp
    src/ingress/raw_emergency_writer_ack.cpp
    src/ingress/raw_fault_transform.cpp
    src/ingress/raw_finalization_continuation_posix.cpp
    src/ingress/raw_fresh_route_posix.cpp
    src/ingress/injected_raw_reader.cpp
    src/ingress/injected_raw_v1.cpp
    src/ingress/raw_ingress_app.cpp
    src/ingress/raw_ingress_config.cpp
    src/ingress/raw_phase2_startup_plan.cpp
    src/ingress/raw_namespace.cpp
    src/ingress/raw_posix_io.cpp
    src/ingress/raw_production_runtime.cpp
    src/ingress/raw_reader.cpp
    src/ingress/raw_recovery.cpp
    src/ingress/raw_recovery_authorized.cpp
    src/ingress/raw_recovery_executor.cpp
    src/ingress/raw_recovery_maintenance_report_v1.cpp
    src/ingress/raw_recovery_maintenance_report_store.cpp
    src/ingress/raw_recovery_posix.cpp
    src/ingress/raw_readiness_observer.cpp
    src/ingress/raw_readiness_worker.cpp
    src/ingress/raw_emergency_reserve_capacity_probe_posix.cpp
    src/ingress/raw_emergency_reserve_posix.cpp
    src/ingress/raw_replay.cpp
    src/ingress/raw_reserve_coordinator.cpp
    src/ingress/raw_reserve_coordinator_gate.cpp
    src/ingress/raw_reserve_authorized_wal.cpp
    src/ingress/raw_reserve_registry.cpp
    src/ingress/raw_reserve_state_posix.cpp
    src/ingress/raw_schema.cpp
    src/ingress/raw_sealed_certificate_v1.cpp
    src/ingress/raw_sealed_certificate_store.cpp
    src/ingress/raw_segment_accumulator.cpp
    src/ingress/raw_segment_artifacts.cpp
    src/ingress/raw_v1.cpp
    src/ingress/raw_index_v1.cpp
    src/ingress/raw_live_tail.cpp
    src/ingress/raw_live_tail_posix.cpp
    src/ingress/raw_manifest_transition.cpp
    src/ingress/raw_manifest_v1.cpp
    src/ingress/raw_manifest_store.cpp
    src/ingress/reserve_headers_v1.cpp
    src/ingress/reserve_state_v1.cpp
    src/ingress/reserve_emergency_transition_v1.cpp
    src/ingress/raw_wal_writer.cpp
    src/ingress/raw_wal_stream.cpp
    src/ingress/raw_wal_stream_posix.cpp
    src/ingress/raw_writer_lease.cpp
    src/ingress/run_manifest_posix_store.cpp
    src/ingress/run_manifest_v1.cpp
    src/ingress/scaffolding_finalization_report_v1.cpp)

add_library(l2flow_phase2 STATIC ${L2FLOW_PHASE2_SOURCES})
add_library(L2Flow::phase2 ALIAS l2flow_phase2)
# The Phase-2/3 controller chain is available explicitly through
# L2Flow::phase3, but the deployed four-stream service, approved vendor
# artifact gate, and external cutover evidence are still pending. Keep the
# generic production alias on the last deployed slice until those separate
# service/cutover gates are accepted.
add_library(L2Flow::production ALIAS l2flow_phase01)
set_target_properties(l2flow_phase2 PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED YES
    CXX_EXTENSIONS NO)
target_compile_features(l2flow_phase2 PUBLIC cxx_std_20)
target_include_directories(l2flow_phase2
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${L2FLOW_GENERATED_INCLUDE_DIR}>)
target_link_libraries(l2flow_phase2
    PUBLIC L2Flow::phase01)
l2flow_enable_strict_warnings(l2flow_phase2)
l2flow_enable_sanitizers(l2flow_phase2)

set(L2FLOW_PHASE3_SOURCES
    src/control/api_decoder.cpp
    src/control/checked_body_view.cpp
    src/control/control_checkpoint_posix_store.cpp
    src/control/control_checkpoint_v1.cpp
    src/control/control_decoder.cpp
    src/control/control_hash.cpp
    src/control/control_live_worker.cpp
    src/control/control_production_controller.cpp
    src/control/control_readiness_gate.cpp
    src/control/control_record_posix_sink.cpp
    src/control/control_record_v1.cpp
    src/control/resubscribe_guard.cpp
    src/control/sys_decoder.cpp)

set(L2FLOW_PHASE3_SCHEMA_HEADER
    "${L2FLOW_GENERATED_INCLUDE_DIR}/l2flow/control/phase3_schema_v1_generated.h")
add_custom_command(
    OUTPUT "${L2FLOW_PHASE3_SCHEMA_HEADER}"
    COMMAND "${CMAKE_COMMAND}" -E make_directory
        "${L2FLOW_GENERATED_INCLUDE_DIR}/l2flow/control"
    COMMAND "${Python3_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/tools/generate_phase3_control_schema.py"
        "${CMAKE_CURRENT_SOURCE_DIR}/schemas/control_record_v1.json"
        "${CMAKE_CURRENT_SOURCE_DIR}/schemas/control_checkpoint_v1.json"
        "${L2FLOW_PHASE3_SCHEMA_HEADER}"
    DEPENDS
        "${CMAKE_CURRENT_SOURCE_DIR}/tools/generate_phase3_control_schema.py"
        "${CMAKE_CURRENT_SOURCE_DIR}/schemas/control_record_v1.json"
        "${CMAKE_CURRENT_SOURCE_DIR}/schemas/control_checkpoint_v1.json"
    COMMENT "Validating and generating Phase 3 control schema constants"
    VERBATIM)
add_custom_target(l2flow_phase3_schema_codegen
    DEPENDS "${L2FLOW_PHASE3_SCHEMA_HEADER}")

add_library(l2flow_phase3 STATIC
    ${L2FLOW_PHASE3_SOURCES}
    "${L2FLOW_PHASE3_SCHEMA_HEADER}")
add_library(L2Flow::phase3 ALIAS l2flow_phase3)
add_dependencies(l2flow_phase3 l2flow_phase3_schema_codegen)
set_target_properties(l2flow_phase3 PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED YES
    CXX_EXTENSIONS NO)
target_compile_features(l2flow_phase3 PUBLIC cxx_std_20)
target_include_directories(l2flow_phase3
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${L2FLOW_GENERATED_INCLUDE_DIR}>)
target_link_libraries(l2flow_phase3 PUBLIC L2Flow::phase2)
l2flow_enable_strict_warnings(l2flow_phase3)
l2flow_enable_sanitizers(l2flow_phase3)

if(L2FLOW_BUILD_RAW_V1_LIBFUZZER)
    if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
        message(FATAL_ERROR
            "L2FLOW_BUILD_RAW_V1_LIBFUZZER requires Clang or AppleClang")
    endif()
    add_executable(
        fuzz_phase2_raw_v1
        tests/fuzz/raw_v1_fuzz_harness.cpp
        tests/fuzz/raw_v1_fuzz_target.cpp)
    target_link_libraries(
        fuzz_phase2_raw_v1
        PRIVATE L2Flow::phase2)
    set_target_properties(
        fuzz_phase2_raw_v1
        PROPERTIES
            CXX_STANDARD 20
            CXX_STANDARD_REQUIRED YES
            CXX_EXTENSIONS NO)
    target_compile_options(
        fuzz_phase2_raw_v1
        PRIVATE -fsanitize=fuzzer)
    target_link_options(
        fuzz_phase2_raw_v1
        PRIVATE -fsanitize=fuzzer)
    l2flow_enable_strict_warnings(fuzz_phase2_raw_v1)
    l2flow_enable_sanitizers(fuzz_phase2_raw_v1)
endif()

add_executable(
    l2flow_raw_replay
    tools/l2flow_raw_replay.cpp)
target_link_libraries(
    l2flow_raw_replay
    PRIVATE L2Flow::phase2)
set_target_properties(
    l2flow_raw_replay
    PROPERTIES
        OUTPUT_NAME "l2flow-raw-replay"
        CXX_STANDARD 20
        CXX_STANDARD_REQUIRED YES
        CXX_EXTENSIONS NO)
l2flow_enable_strict_warnings(l2flow_raw_replay)
l2flow_enable_sanitizers(l2flow_raw_replay)

add_library(l2flow_ingress_service STATIC
    src/apps/ingress_service.cpp
    src/apps/monitor_policy.cpp
    src/apps/service_path_policy.cpp)
add_library(L2Flow::ingress_service ALIAS l2flow_ingress_service)
set_target_properties(l2flow_ingress_service PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED YES
    CXX_EXTENSIONS NO)
target_link_libraries(l2flow_ingress_service
    PUBLIC L2Flow::phase01)
l2flow_enable_strict_warnings(l2flow_ingress_service)
l2flow_enable_sanitizers(l2flow_ingress_service)

function(l2flow_add_ingress_executable target output_name kind_value)
    add_executable(${target} apps/mdl_ingress_main.cpp)
    target_compile_definitions(${target}
        PRIVATE L2FLOW_INGRESS_KIND=${kind_value})
    target_link_libraries(${target}
        PRIVATE L2Flow::ingress_service)
    set_target_properties(${target} PROPERTIES
        OUTPUT_NAME "${output_name}"
        CXX_STANDARD 20
        CXX_STANDARD_REQUIRED YES
        CXX_EXTENSIONS NO)
    l2flow_enable_strict_warnings(${target})
    l2flow_enable_sanitizers(${target})
endfunction()

l2flow_add_ingress_executable(
    mdl_ingress_sh_snapshot
    mdl-ingress-sh-snapshot
    0)
l2flow_add_ingress_executable(
    mdl_ingress_sh_tick
    mdl-ingress-sh-tick
    1)
l2flow_add_ingress_executable(
    mdl_ingress_sz_snapshot
    mdl-ingress-sz-snapshot
    2)
l2flow_add_ingress_executable(
    mdl_ingress_sz_tick
    mdl-ingress-sz-tick
    3)

add_executable(mdl_abi_preflight tools/mdl_abi_preflight.cpp)
target_link_libraries(mdl_abi_preflight PRIVATE L2Flow::phase01)
l2flow_enable_strict_warnings(mdl_abi_preflight)

add_executable(
    mdl_sdk_feeder_probe
    tools/mdl_sdk_feeder_probe.cpp)
target_link_libraries(
    mdl_sdk_feeder_probe
    PRIVATE L2Flow::phase01)
set_target_properties(
    mdl_sdk_feeder_probe
    PROPERTIES
        OUTPUT_NAME "mdl-sdk-feeder-probe"
        CXX_STANDARD 20
        CXX_STANDARD_REQUIRED YES
        CXX_EXTENSIONS NO)
l2flow_enable_strict_warnings(mdl_sdk_feeder_probe)
l2flow_enable_sanitizers(mdl_sdk_feeder_probe)

add_executable(
    mdl_phase2_live_probe
    tools/mdl_phase2_live_probe.cpp)
target_link_libraries(
    mdl_phase2_live_probe
    PRIVATE L2Flow::phase2)
set_target_properties(
    mdl_phase2_live_probe
    PROPERTIES
        OUTPUT_NAME "mdl-phase2-live-probe"
        CXX_STANDARD 20
        CXX_STANDARD_REQUIRED YES
        CXX_EXTENSIONS NO)
l2flow_enable_strict_warnings(mdl_phase2_live_probe)
l2flow_enable_sanitizers(mdl_phase2_live_probe)

add_executable(
    mdl_phase3_live_probe
    tools/mdl_phase3_live_probe.cpp)
target_link_libraries(
    mdl_phase3_live_probe
    PRIVATE L2Flow::phase3)
set_target_properties(
    mdl_phase3_live_probe
    PROPERTIES
        OUTPUT_NAME "mdl-phase3-live-probe"
        CXX_STANDARD 20
        CXX_STANDARD_REQUIRED YES
        CXX_EXTENSIONS NO)
l2flow_enable_strict_warnings(mdl_phase3_live_probe)
l2flow_enable_sanitizers(mdl_phase3_live_probe)

if(CMAKE_SYSTEM_NAME STREQUAL "Linux" AND
   L2FLOW_SANITIZER_MODE STREQUAL "none" AND
   L2FLOW_VENDOR_LIBRARY_IS_ELF)
    get_filename_component(
        L2FLOW_VENDOR_LIBRARY_DIRECTORY
        "${L2FLOW_VENDOR_LIBRARY_PATH}"
        DIRECTORY)
    add_executable(
        mdl_vendor_link_probe
        tools/mdl_vendor_link_probe.cpp)
    target_include_directories(
        mdl_vendor_link_probe
        SYSTEM PRIVATE
            "${CMAKE_CURRENT_SOURCE_DIR}/mdl_sdk_2_13_234/include")
    target_link_libraries(
        mdl_vendor_link_probe
        PRIVATE
            Threads::Threads
            "${L2FLOW_VENDOR_LIBRARY_PATH}")
    set_target_properties(
        mdl_vendor_link_probe
        PROPERTIES
            OUTPUT_NAME "mdl-vendor-link-probe"
            BUILD_RPATH
                "${L2FLOW_VENDOR_LIBRARY_DIRECTORY}"
            CXX_STANDARD 20
            CXX_STANDARD_REQUIRED YES
            CXX_EXTENSIONS NO)
    l2flow_enable_strict_warnings(mdl_vendor_link_probe)
endif()

add_executable(l2_mock_demo tools/l2_mock_demo.cpp)
target_link_libraries(l2_mock_demo PRIVATE L2Flow::l2mock_standalone)
l2mock_enable_strict_warnings(l2_mock_demo)

if(L2MOCK_BUILD_TESTS OR L2FLOW_BUILD_TESTS)
    enable_testing()
endif()

if(L2MOCK_BUILD_TESTS)
    add_executable(test_abi tests/test_abi.cpp)
    target_link_libraries(test_abi PRIVATE L2Flow::l2mock_standalone)
    l2mock_enable_strict_warnings(test_abi)
    add_test(NAME abi_contract COMMAND test_abi)

    add_executable(test_messages tests/test_messages.cpp)
    target_link_libraries(test_messages PRIVATE L2Flow::l2mock_standalone)
    l2mock_enable_strict_warnings(test_messages)
    add_test(NAME message_contract COMMAND test_messages)

    add_executable(test_subscription tests/test_subscription.cpp)
    target_link_libraries(test_subscription PRIVATE L2Flow::l2mock_standalone)
    l2mock_enable_strict_warnings(test_subscription)
    add_test(NAME subscription_contract COMMAND test_subscription)

    add_executable(test_determinism_stress
        tests/test_determinism_stress.cpp)
    target_link_libraries(test_determinism_stress
        PRIVATE L2Flow::l2mock_standalone)
    l2mock_enable_strict_warnings(test_determinism_stress)
    add_test(NAME determinism_and_stress COMMAND test_determinism_stress)

    add_executable(test_config tests/test_config.cpp)
    target_link_libraries(test_config PRIVATE L2Flow::l2mock_standalone)
    l2mock_enable_strict_warnings(test_config)
    add_test(NAME config_and_clock_edges COMMAND test_config)

    add_executable(test_numeric_edges tests/test_numeric_edges.cpp)
    target_link_libraries(test_numeric_edges
        PRIVATE L2Flow::l2mock_standalone)
    l2mock_enable_strict_warnings(test_numeric_edges)
    add_test(NAME numeric_edges COMMAND test_numeric_edges)

    add_executable(test_concurrency_edges tests/test_concurrency_edges.cpp)
    target_link_libraries(test_concurrency_edges
        PRIVATE L2Flow::l2mock_standalone)
    l2mock_enable_strict_warnings(test_concurrency_edges)
    add_test(NAME concurrency_edges COMMAND test_concurrency_edges)

    add_executable(test_review_runtime tests/test_review_runtime.cpp)
    target_link_libraries(test_review_runtime
        PRIVATE L2Flow::l2mock_standalone)
    l2mock_enable_strict_warnings(test_review_runtime)
    add_test(NAME reviewed_runtime_regressions
        COMMAND test_review_runtime)

    add_executable(test_review_subscription
        tests/test_review_subscription.cpp)
    target_link_libraries(test_review_subscription
        PRIVATE L2Flow::l2mock_standalone)
    l2mock_enable_strict_warnings(test_review_subscription)
    add_test(NAME reviewed_subscription_regressions
        COMMAND test_review_subscription)

    # Compile the same probe in both supported linkage modes.  The standalone
    # executable must use the byte-copy mock fallback.  The mixed real/mock
    # executable must leave DllConvert* undefined in the executable and bind
    # them to the vendor shared library, whose non-ASCII conversion is
    # intentionally observably different.
    add_executable(test_sdk_compat_standalone
        tests/test_sdk_link_isolation.cpp)
    target_link_libraries(test_sdk_compat_standalone
        PRIVATE L2Flow::l2mock_standalone)
    l2mock_enable_strict_warnings(test_sdk_compat_standalone)
    add_test(NAME sdk_compat_standalone COMMAND test_sdk_compat_standalone)

    if(CMAKE_SYSTEM_NAME STREQUAL "Linux" AND
       L2FLOW_SANITIZER_MODE STREQUAL "none" AND
       L2FLOW_VENDOR_LIBRARY_IS_ELF)
        add_executable(test_sdk_vendor_isolation
            tests/test_sdk_link_isolation.cpp)
        target_compile_definitions(test_sdk_vendor_isolation
            PRIVATE L2MOCK_EXPECT_VENDOR_CONVERSION=1)
        target_link_libraries(test_sdk_vendor_isolation
            PRIVATE
                L2Flow::l2mock
                "${L2FLOW_VENDOR_LIBRARY_PATH}")
        set_target_properties(test_sdk_vendor_isolation PROPERTIES
            BUILD_RPATH
                "${CMAKE_CURRENT_SOURCE_DIR}/mdl_sdk_2_13_234/libs/linux")
        l2mock_enable_strict_warnings(test_sdk_vendor_isolation)
        # Never execute this vendor-linked target from CTest. The dynamic
        # loader resolves its RPATH dependency before an in-process identity
        # check could run, and a configure-time hash becomes stale if the
        # source artifact later changes. Runtime vendor calls are exercised
        # only by the sealed production preflight.
    endif()
endif()

if(L2FLOW_BUILD_TESTS)
    function(l2flow_add_phase_test target source)
        add_executable(${target} "${source}")
        target_link_libraries(${target} PRIVATE L2Flow::phase01)
        l2flow_enable_strict_warnings(${target})
        add_test(NAME ${target} COMMAND ${target})
    endfunction()

    function(l2flow_add_phase2_test target source)
        add_executable(${target} "${source}")
        target_link_libraries(${target} PRIVATE L2Flow::phase2)
        l2flow_enable_strict_warnings(${target})
        add_test(NAME ${target} COMMAND ${target})
    endfunction()

    function(l2flow_add_phase3_test target source)
        add_executable(${target} "${source}")
        target_link_libraries(${target} PRIVATE L2Flow::phase3)
        l2flow_enable_strict_warnings(${target})
        add_test(NAME ${target} COMMAND ${target})
        set_tests_properties(${target} PROPERTIES LABELS "phase3")
    endfunction()

    l2flow_add_phase3_test(
        test_phase3_control_decoder
        tests/test_phase3_control_decoder.cpp)
    l2flow_add_phase3_test(
        test_phase3_control_core
        tests/test_phase3_control_core.cpp)
    l2flow_add_phase3_test(
        test_phase3_control_live_worker
        tests/test_phase3_control_live_worker.cpp)
    l2flow_add_phase3_test(
        test_phase3_control_production_controller
        tests/test_phase3_control_production_controller.cpp)
    l2flow_add_phase3_test(
        test_phase3_control_checkpoint_reachability
        tests/test_phase3_control_checkpoint_reachability.cpp)
    if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
        l2flow_add_phase3_test(
            test_phase3_control_checkpoint_posix_store
            tests/test_phase3_control_checkpoint_posix_store.cpp)
        l2flow_add_phase3_test(
            test_phase3_control_record_posix_sink
            tests/test_phase3_control_record_posix_sink.cpp)
    endif()

    add_test(
        NAME test_phase2_live_probe_cli_help
        COMMAND mdl_phase2_live_probe --help)
    set_tests_properties(
        test_phase2_live_probe_cli_help
        PROPERTIES
            LABELS "phase2;live-probe-cli"
            TIMEOUT 10)
    add_test(
        NAME test_phase3_live_probe_cli_help
        COMMAND mdl_phase3_live_probe --help)
    set_tests_properties(
        test_phase3_live_probe_cli_help
        PROPERTIES
            LABELS "phase3;live-probe-cli"
            TIMEOUT 10)

    l2flow_add_phase2_test(
        test_crc32c
        tests/test_crc32c.cpp)
    l2flow_add_phase2_test(
        test_sha256
        tests/test_sha256.cpp)
    l2flow_add_phase2_test(
        test_phase2_identity128
        tests/test_phase2_identity128.cpp)
    l2flow_add_phase2_test(
        test_phase2_empty_anchor_tombstone_v1
        tests/test_phase2_empty_anchor_tombstone_v1.cpp)
    l2flow_add_phase2_test(
        test_phase2_empty_anchor_tombstone_store
        tests/test_phase2_empty_anchor_tombstone_store.cpp)
    l2flow_add_phase2_test(
        test_phase2_finalization_report_v1
        tests/test_phase2_finalization_report_v1.cpp)
    target_compile_definitions(
        test_phase2_finalization_report_v1
        PRIVATE
            L2FLOW_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}")
    l2flow_add_phase2_test(
        test_phase2_finalization_report_store
        tests/test_phase2_finalization_report_store.cpp)
    l2flow_add_phase2_test(
        test_phase2_finalization_archive_v1
        tests/test_phase2_finalization_archive_v1.cpp)
    target_compile_definitions(
        test_phase2_finalization_archive_v1
        PRIVATE
            L2FLOW_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}")
    l2flow_add_phase2_test(
        test_phase2_finalization_archive_source_cleanup_posix
        tests/test_phase2_finalization_archive_source_cleanup_posix.cpp)
    target_compile_definitions(
        test_phase2_finalization_archive_source_cleanup_posix
        PRIVATE
            L2FLOW_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}")
    l2flow_add_phase2_test(
        test_phase2_control_page
        tests/test_phase2_control_page.cpp)
    l2flow_add_phase2_test(
        test_phase2_raw_control_file
        tests/test_phase2_raw_control_file.cpp)
    l2flow_add_phase2_test(
        test_phase2_raw_clean_stop_gate
        tests/test_phase2_raw_clean_stop_gate.cpp)
    l2flow_add_phase2_test(
        test_phase2_raw_capture_worker
        tests/test_phase2_raw_capture_worker.cpp)
    l2flow_add_phase2_test(
        test_phase2_raw_fault_transform
        tests/test_phase2_raw_fault_transform.cpp)
    add_executable(
        test_phase2_injected_raw_v1
        tests/test_phase2_injected_raw_v1.cpp)
    target_link_libraries(
        test_phase2_injected_raw_v1
        PRIVATE L2Flow::phase2)
    l2flow_enable_strict_warnings(
        test_phase2_injected_raw_v1)
    add_test(
        NAME test_phase2_injected_raw_v1
        COMMAND
            test_phase2_injected_raw_v1
            "${CMAKE_CURRENT_SOURCE_DIR}/schemas/injected_raw_v1.json")
    l2flow_add_phase2_test(
        test_phase2_raw_ingress_config
        tests/test_phase2_raw_ingress_config.cpp)
    l2flow_add_phase2_test(
        test_phase2_raw_ingress_app
        tests/test_phase2_raw_ingress_app.cpp)
    l2flow_add_phase2_test(
        test_phase2_raw_emergency_writer_ack
        tests/test_phase2_raw_emergency_writer_ack.cpp)
    l2flow_add_phase2_test(
        test_phase2_raw_phase2_startup_plan
        tests/test_phase2_raw_phase2_startup_plan.cpp)
    l2flow_add_phase2_test(
        test_phase2_raw_posix_io
        tests/test_phase2_raw_posix_io.cpp)
    l2flow_add_phase2_test(
        test_phase2_raw_production_runtime
        tests/test_phase2_raw_production_runtime.cpp)
    target_link_libraries(
        test_phase2_raw_production_runtime
        PRIVATE L2Flow::phase3)
    set_tests_properties(
        test_phase2_raw_production_runtime
        PROPERTIES LABELS "phase2;phase3;production-controller")
    l2flow_add_phase2_test(
        test_phase2_raw_namespace
        tests/test_phase2_raw_namespace.cpp)
    l2flow_add_phase2_test(
        test_phase2_raw_reader
        tests/test_phase2_raw_reader.cpp)
    l2flow_add_phase2_test(
        test_phase2_raw_recovery
        tests/test_phase2_raw_recovery.cpp)
    add_executable(
        test_phase2_raw_v1_fuzz_corpus
        tests/fuzz/raw_v1_fuzz_harness.cpp
        tests/test_phase2_raw_v1_fuzz_corpus.cpp)
    target_link_libraries(
        test_phase2_raw_v1_fuzz_corpus
        PRIVATE L2Flow::phase2)
    set_target_properties(
        test_phase2_raw_v1_fuzz_corpus
        PROPERTIES
            CXX_STANDARD 20
            CXX_STANDARD_REQUIRED YES
            CXX_EXTENSIONS NO)
    l2flow_enable_strict_warnings(
        test_phase2_raw_v1_fuzz_corpus)
    add_test(
        NAME test_phase2_raw_v1_fuzz_corpus
        COMMAND
            test_phase2_raw_v1_fuzz_corpus
            "${CMAKE_CURRENT_SOURCE_DIR}/tests/fuzz/corpus/raw_v1")
    l2flow_add_phase2_test(
        test_phase2_raw_recovery_executor
        tests/test_phase2_raw_recovery_executor.cpp)
    l2flow_add_phase2_test(
        test_phase2_raw_recovery_maintenance_report_v1
        tests/test_phase2_raw_recovery_maintenance_report_v1.cpp)
    target_compile_definitions(
        test_phase2_raw_recovery_maintenance_report_v1
        PRIVATE
            L2FLOW_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}")
    l2flow_add_phase2_test(
        test_phase2_raw_recovery_maintenance_report_store
        tests/test_phase2_raw_recovery_maintenance_report_store.cpp)
    l2flow_add_phase2_test(
        test_phase2_raw_recovery_posix
        tests/test_phase2_raw_recovery_posix.cpp)
    l2flow_add_phase2_test(
        test_phase2_scaffolding_finalization_report_v1
        tests/test_phase2_scaffolding_finalization_report_v1.cpp)
    target_compile_definitions(
        test_phase2_scaffolding_finalization_report_v1
        PRIVATE
            L2FLOW_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}")
    l2flow_add_phase2_test(
        test_phase2_raw_emergency_reserve_posix
        tests/test_phase2_raw_emergency_reserve_posix.cpp)
    l2flow_add_phase2_test(
        test_phase2_raw_emergency_reserve_capacity_probe_posix
        tests/test_phase2_raw_emergency_reserve_capacity_probe_posix.cpp)
    l2flow_add_phase2_test(
        test_phase2_raw_emergency_reserve_reprovision_posix
        tests/test_phase2_raw_emergency_reserve_reprovision_posix.cpp)
    target_compile_definitions(
        test_phase2_raw_emergency_reserve_reprovision_posix
        PRIVATE
            L2FLOW_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}")
    l2flow_add_phase2_test(
        test_phase2_raw_replay
        tests/test_phase2_raw_replay.cpp)
    l2flow_add_phase2_test(
        test_phase2_raw_replay_cli
        tests/test_phase2_raw_replay_cli.cpp)
    target_compile_definitions(
        test_phase2_raw_replay_cli
        PRIVATE
            L2FLOW_RAW_REPLAY_EXE="$<TARGET_FILE:l2flow_raw_replay>")
    add_dependencies(
        test_phase2_raw_replay_cli
        l2flow_raw_replay)
    l2flow_add_phase2_test(
        test_phase2_raw_reserve_state_posix
        tests/test_phase2_raw_reserve_state_posix.cpp)
    l2flow_add_phase2_test(
        test_phase2_raw_reserve_registry
        tests/test_phase2_raw_reserve_registry.cpp)
    l2flow_add_phase2_test(
        test_phase2_raw_reserve_coordinator_gate
        tests/test_phase2_raw_reserve_coordinator_gate.cpp)
    l2flow_add_phase2_test(
        test_phase2_raw_reserve_coordinator
        tests/test_phase2_raw_reserve_coordinator.cpp)
    l2flow_add_phase2_test(
        test_phase2_raw_readiness_observer
        tests/test_phase2_raw_readiness_observer.cpp)
    l2flow_add_phase2_test(
        test_phase2_raw_readiness_worker
        tests/test_phase2_raw_readiness_worker.cpp)
    l2flow_add_phase2_test(
        test_phase2_raw_segment_accumulator
        tests/test_phase2_raw_segment_accumulator.cpp)
    l2flow_add_phase2_test(
        test_phase2_raw_segment_artifacts
        tests/test_phase2_raw_segment_artifacts.cpp)
    l2flow_add_phase2_test(
        test_phase2_raw_sealed_certificate_v1
        tests/test_phase2_raw_sealed_certificate_v1.cpp)
    l2flow_add_phase2_test(
        test_phase2_raw_sealed_certificate_store
        tests/test_phase2_raw_sealed_certificate_store.cpp)
    l2flow_add_phase2_test(
        test_phase2_raw_index_v1
        tests/test_phase2_raw_index_v1.cpp)
    l2flow_add_phase2_test(
        test_phase2_raw_live_tail
        tests/test_phase2_raw_live_tail.cpp)
    l2flow_add_phase2_test(
        test_phase2_raw_live_tail_posix
        tests/test_phase2_raw_live_tail_posix.cpp)
    l2flow_add_phase2_test(
        test_phase2_raw_manifest_v1
        tests/test_phase2_raw_manifest_v1.cpp)
    l2flow_add_phase2_test(
        test_phase2_raw_manifest_store
        tests/test_phase2_raw_manifest_store.cpp)
    l2flow_add_phase2_test(
        test_phase2_raw_manifest_transition
        tests/test_phase2_raw_manifest_transition.cpp)
    l2flow_add_phase2_test(
        test_phase2_raw_v1
        tests/test_phase2_raw_v1.cpp)
    l2flow_add_phase2_test(
        test_phase2_raw_wal_writer
        tests/test_phase2_raw_wal_writer.cpp)
    l2flow_add_phase2_test(
        test_phase2_raw_writer_lease
        tests/test_phase2_raw_writer_lease.cpp)
    l2flow_add_phase2_test(
        test_phase2_reserve_state_v1
        tests/test_phase2_reserve_state_v1.cpp)
    l2flow_add_phase2_test(
        test_phase2_reserve_emergency_transition_v1
        tests/test_phase2_reserve_emergency_transition_v1.cpp)
    l2flow_add_phase2_test(
        test_phase2_raw_wal_stream
        tests/test_phase2_raw_wal_stream.cpp)
    l2flow_add_phase2_test(
        test_phase2_raw_wal_stream_posix
        tests/test_phase2_raw_wal_stream_posix.cpp)
    l2flow_add_phase2_test(
        test_phase2_run_manifest_v1
        tests/test_phase2_run_manifest_v1.cpp)
    l2flow_add_phase2_test(
        test_phase2_run_manifest_posix_store
        tests/test_phase2_run_manifest_posix_store.cpp)

    add_executable(
        test_phase2_reserve_headers_v1
        tests/test_phase2_reserve_headers_v1.cpp)
    target_link_libraries(
        test_phase2_reserve_headers_v1
        PRIVATE L2Flow::phase2)
    l2flow_enable_strict_warnings(
        test_phase2_reserve_headers_v1)
    add_test(
        NAME test_phase2_reserve_headers_v1
        COMMAND
            test_phase2_reserve_headers_v1
            "${CMAKE_CURRENT_SOURCE_DIR}/schemas/reserve_headers_v1.json")

    add_executable(
        test_phase2_raw_schema
        tests/test_phase2_raw_schema.cpp)
    target_link_libraries(
        test_phase2_raw_schema
        PRIVATE L2Flow::phase2)
    l2flow_enable_strict_warnings(
        test_phase2_raw_schema)
    add_test(
        NAME test_phase2_raw_schema
        COMMAND
            test_phase2_raw_schema
            "${CMAKE_CURRENT_SOURCE_DIR}/schemas/raw_v1.json")

    l2flow_add_phase_test(
        test_phase0_baseline
        tests/test_phase0_baseline.cpp)
    set_tests_properties(
        test_phase0_baseline
        PROPERTIES TIMEOUT 120)
    l2flow_add_phase_test(
        test_phase0_sealed_file_snapshot
        tests/test_phase0_sealed_file_snapshot.cpp)
    if(NOT L2FLOW_SANITIZER_MODE STREQUAL "none")
        target_compile_definitions(
            test_phase0_baseline
            PRIVATE L2FLOW_SANITIZER_BUILD=1)
    endif()
    l2flow_add_phase_test(
        test_phase1_callback
        tests/test_phase1_callback.cpp)
    l2flow_add_phase_test(
        test_phase1_callback_stress
        tests/test_phase1_callback_stress.cpp)
    set_tests_properties(
        test_phase1_callback_stress
        PROPERTIES
            LABELS "stress"
            TIMEOUT 300)
    l2flow_add_phase_test(
        test_phase1_config_clock
        tests/test_phase1_config_clock.cpp)
    l2flow_add_phase_test(
        test_phase1_endpoint_contract
        tests/test_phase1_endpoint_contract.cpp)
    l2flow_add_phase_test(
        test_phase1_ring
        tests/test_phase1_ring.cpp)
    l2flow_add_phase_test(
        test_phase1_shadow
        tests/test_phase1_shadow.cpp)
    l2flow_add_phase_test(
        test_phase1_subscription_ops
        tests/test_phase1_subscription_ops.cpp)
    l2flow_add_phase_test(
        test_phase1_metrics_textfile
        tests/test_phase1_metrics_textfile.cpp)
    l2flow_add_phase_test(
        test_phase1_metrics_worker
        tests/test_phase1_metrics_worker.cpp)
    set_tests_properties(
        test_phase1_metrics_worker
        PROPERTIES TIMEOUT 30)
    l2flow_add_phase_test(
        test_phase1_stable_output_prefix
        tests/test_phase1_stable_output_prefix.cpp)

    l2flow_add_phase_test(
        test_phase1_ingress_app
        tests/test_phase1_ingress_app.cpp)

    add_executable(
        test_phase1_service_config
        tests/test_phase1_service_config.cpp)
    target_link_libraries(
        test_phase1_service_config
        PRIVATE L2Flow::ingress_service)
    l2flow_enable_strict_warnings(test_phase1_service_config)
    add_test(
        NAME test_phase1_service_config
        COMMAND test_phase1_service_config)

    add_executable(
        test_phase1_monitor_policy
        tests/test_phase1_monitor_policy.cpp)
    target_link_libraries(
        test_phase1_monitor_policy
        PRIVATE L2Flow::ingress_service)
    l2flow_enable_strict_warnings(test_phase1_monitor_policy)
    add_test(
        NAME test_phase1_monitor_policy
        COMMAND test_phase1_monitor_policy)

    add_executable(
        test_phase1_service_path_policy
        tests/test_phase1_service_path_policy.cpp)
    target_link_libraries(
        test_phase1_service_path_policy
        PRIVATE L2Flow::ingress_service)
    l2flow_enable_strict_warnings(
        test_phase1_service_path_policy)
    add_test(
        NAME test_phase1_service_path_policy
        COMMAND test_phase1_service_path_policy)

    if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
        find_program(L2FLOW_READELF_EXECUTABLE readelf)
        if(NOT L2FLOW_READELF_EXECUTABLE)
            message(FATAL_ERROR
                "readelf is required to verify ingress SDK link isolation")
        endif()
        foreach(ingress_target IN ITEMS
                mdl_ingress_sh_snapshot
                mdl_ingress_sh_tick
                mdl_ingress_sz_snapshot
                mdl_ingress_sz_tick)
            add_test(
                NAME ${ingress_target}_help
                COMMAND $<TARGET_FILE:${ingress_target}> --help)
            add_test(
                NAME ${ingress_target}_no_vendor_dependency
                COMMAND
                    ${CMAKE_COMMAND}
                    "-DREADELF=${L2FLOW_READELF_EXECUTABLE}"
                    "-DBINARY=$<TARGET_FILE:${ingress_target}>"
                    -P
                    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/CheckNoVendorDependency.cmake")
        endforeach()
    endif()

    add_executable(
        test_build_manifest
        tests/test_build_manifest.cpp)
    target_link_libraries(
        test_build_manifest
        PRIVATE
            L2Flow::phase01)
    l2flow_enable_strict_warnings(test_build_manifest)
    add_test(
        NAME test_build_manifest
        COMMAND
            test_build_manifest
            "${L2FLOW_BUILD_MANIFEST_PATH}"
            "${CMAKE_CURRENT_SOURCE_DIR}/configs/vendor_baseline.json")
    if(TARGET mdl_vendor_link_probe)
        add_test(
            NAME mdl_vendor_minimal_link
            COMMAND
                ${CMAKE_COMMAND}
                "-DREADELF=${L2FLOW_READELF_EXECUTABLE}"
                "-DBINARY=$<TARGET_FILE:mdl_vendor_link_probe>"
                -P
                "${CMAKE_CURRENT_SOURCE_DIR}/cmake/CheckVendorDependency.cmake")
    endif()
endif()
