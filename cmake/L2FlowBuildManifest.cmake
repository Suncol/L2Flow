include_guard(GLOBAL)

function(_l2flow_json_escape output value)
    set(escaped "${value}")
    string(REPLACE "\\" "\\\\" escaped "${escaped}")
    string(REPLACE "\"" "\\\"" escaped "${escaped}")
    string(REPLACE "\n" "\\n" escaped "${escaped}")
    string(REPLACE "\r" "\\r" escaped "${escaped}")
    string(REPLACE "\t" "\\t" escaped "${escaped}")
    set(${output} "${escaped}" PARENT_SCOPE)
endfunction()

function(_l2flow_json_array output)
    set(json "")
    set(separator "")
    foreach(value IN LISTS ARGN)
        _l2flow_json_escape(escaped "${value}")
        string(APPEND json "${separator}\"${escaped}\"")
        set(separator ", ")
    endforeach()
    set(${output} "[${json}]" PARENT_SCOPE)
endfunction()

function(l2flow_generate_build_manifest)
    set(one_value_arguments
        OUTPUT
        HEADER
        HEADER_TEMPLATE
        BASELINE
        BASELINE_LABEL
        CXX_STANDARD
        SANITIZER_MODE)
    set(multi_value_arguments
        STRICT_COMPILE_FLAGS
        SANITIZER_COMPILE_FLAGS
        SANITIZER_LINK_FLAGS)
    cmake_parse_arguments(
        MANIFEST
        ""
        "${one_value_arguments}"
        "${multi_value_arguments}"
        ${ARGN})

    foreach(required_argument
            OUTPUT
            HEADER
            HEADER_TEMPLATE
            BASELINE
            BASELINE_LABEL
            CXX_STANDARD
            SANITIZER_MODE)
        if(NOT DEFINED MANIFEST_${required_argument} OR
           MANIFEST_${required_argument} STREQUAL "")
            message(FATAL_ERROR
                "l2flow_generate_build_manifest requires ${required_argument}")
        endif()
    endforeach()

    if(NOT EXISTS "${MANIFEST_BASELINE}")
        message(FATAL_ERROR
            "SDK baseline JSON does not exist: ${MANIFEST_BASELINE}")
    endif()
    set_property(
        DIRECTORY
        APPEND
        PROPERTY CMAKE_CONFIGURE_DEPENDS "${MANIFEST_BASELINE}")
    if(NOT EXISTS "${MANIFEST_HEADER_TEMPLATE}")
        message(FATAL_ERROR
            "build manifest header template does not exist: "
            "${MANIFEST_HEADER_TEMPLATE}")
    endif()

    file(SHA256 "${MANIFEST_BASELINE}" baseline_sha256)

    if(CMAKE_CONFIGURATION_TYPES)
        set(build_type "multi-config")
    elseif(CMAKE_BUILD_TYPE)
        set(build_type "${CMAKE_BUILD_TYPE}")
    else()
        set(build_type "unspecified")
    endif()

    set(system_name "${CMAKE_SYSTEM_NAME}")
    if(system_name STREQUAL "")
        set(system_name "unknown")
    endif()
    set(system_processor "${CMAKE_SYSTEM_PROCESSOR}")
    if(system_processor STREQUAL "")
        set(system_processor "unknown")
    endif()

    set(source_revision_status "unavailable")
    set(source_revision_json "null")
    find_package(Git QUIET)
    if(Git_FOUND)
        execute_process(
            COMMAND
                "${GIT_EXECUTABLE}" -C "${PROJECT_SOURCE_DIR}"
                rev-parse --verify HEAD
            RESULT_VARIABLE revision_result
            OUTPUT_VARIABLE source_revision
            ERROR_QUIET
            OUTPUT_STRIP_TRAILING_WHITESPACE)
        string(LENGTH "${source_revision}" source_revision_length)
        if(revision_result EQUAL 0 AND
           source_revision_length EQUAL 40 AND
           source_revision MATCHES "^[0-9A-Fa-f]+$")
            string(TOLOWER "${source_revision}" source_revision)
            set(source_revision_status "available")
            set(source_revision_json "\"${source_revision}\"")
        endif()
    endif()

    _l2flow_json_escape(project_name "${PROJECT_NAME}")
    _l2flow_json_escape(project_version "${PROJECT_VERSION}")
    _l2flow_json_escape(compiler_id "${CMAKE_CXX_COMPILER_ID}")
    _l2flow_json_escape(compiler_version "${CMAKE_CXX_COMPILER_VERSION}")
    _l2flow_json_escape(compiler_path "${CMAKE_CXX_COMPILER}")
    _l2flow_json_escape(system_name_json "${system_name}")
    _l2flow_json_escape(system_processor_json "${system_processor}")
    _l2flow_json_escape(build_type_json "${build_type}")
    _l2flow_json_escape(baseline_label_json "${MANIFEST_BASELINE_LABEL}")
    _l2flow_json_escape(
        source_revision_status_json "${source_revision_status}")
    _l2flow_json_escape(sanitizer_mode_json "${MANIFEST_SANITIZER_MODE}")

    _l2flow_json_array(
        strict_compile_flags_json ${MANIFEST_STRICT_COMPILE_FLAGS})
    _l2flow_json_array(
        sanitizer_compile_flags_json
        ${MANIFEST_SANITIZER_COMPILE_FLAGS})
    _l2flow_json_array(
        sanitizer_link_flags_json ${MANIFEST_SANITIZER_LINK_FLAGS})

    set(L2FLOW_BUILD_MANIFEST_JSON "{\n")
    string(APPEND L2FLOW_BUILD_MANIFEST_JSON
        "  \"schema_version\": 1,\n"
        "  \"project\": {\n"
        "    \"name\": \"${project_name}\",\n"
        "    \"version\": \"${project_version}\"\n"
        "  },\n"
        "  \"compiler\": {\n"
        "    \"id\": \"${compiler_id}\",\n"
        "    \"version\": \"${compiler_version}\",\n"
        "    \"path\": \"${compiler_path}\"\n"
        "  },\n"
        "  \"cxx_standard\": ${MANIFEST_CXX_STANDARD},\n"
        "  \"system\": {\n"
        "    \"name\": \"${system_name_json}\",\n"
        "    \"processor\": \"${system_processor_json}\"\n"
        "  },\n"
        "  \"build_type\": \"${build_type_json}\",\n"
        "  \"effective_flags\": {\n"
        "    \"strict_compile\": ${strict_compile_flags_json},\n"
        "    \"sanitizer\": {\n"
        "      \"mode\": \"${sanitizer_mode_json}\",\n"
        "      \"compile\": ${sanitizer_compile_flags_json},\n"
        "      \"link\": ${sanitizer_link_flags_json}\n"
        "    }\n"
        "  },\n"
        "  \"sdk_baseline\": {\n"
        "    \"path\": \"${baseline_label_json}\",\n"
        "    \"sha256\": \"${baseline_sha256}\"\n"
        "  },\n"
        "  \"source_revision\": {\n"
        "    \"status\": \"${source_revision_status_json}\",\n"
        "    \"revision\": ${source_revision_json}\n"
        "  }\n"
        "}\n")

    get_filename_component(manifest_directory "${MANIFEST_OUTPUT}" DIRECTORY)
    get_filename_component(header_directory "${MANIFEST_HEADER}" DIRECTORY)
    file(MAKE_DIRECTORY "${manifest_directory}" "${header_directory}")
    set(write_manifest TRUE)
    if(EXISTS "${MANIFEST_OUTPUT}")
        file(READ "${MANIFEST_OUTPUT}" existing_manifest)
        if(existing_manifest STREQUAL L2FLOW_BUILD_MANIFEST_JSON)
            set(write_manifest FALSE)
        endif()
    endif()
    if(write_manifest)
        file(WRITE "${MANIFEST_OUTPUT}" "${L2FLOW_BUILD_MANIFEST_JSON}")
    endif()
    file(SHA256 "${MANIFEST_OUTPUT}" L2FLOW_BUILD_MANIFEST_SHA256)

    set(L2FLOW_BUILD_MANIFEST_SCHEMA_VERSION 1)
    set(
        L2FLOW_BUILD_MANIFEST_CXX_STANDARD
        "${MANIFEST_CXX_STANDARD}")
    set(
        L2FLOW_BUILD_MANIFEST_SANITIZER_MODE
        "${MANIFEST_SANITIZER_MODE}")
    set(L2FLOW_SDK_BASELINE_SHA256 "${baseline_sha256}")
    set(L2FLOW_SOURCE_REVISION_STATUS "${source_revision_status}")
    set(L2FLOW_SOURCE_REVISION "${source_revision}")
    configure_file(
        "${MANIFEST_HEADER_TEMPLATE}"
        "${MANIFEST_HEADER}"
        @ONLY)

    set(L2FLOW_BUILD_MANIFEST_PATH "${MANIFEST_OUTPUT}" PARENT_SCOPE)
    set(
        L2FLOW_BUILD_MANIFEST_HEADER
        "${MANIFEST_HEADER}"
        PARENT_SCOPE)
    set(
        L2FLOW_BUILD_MANIFEST_SHA256
        "${L2FLOW_BUILD_MANIFEST_SHA256}"
        PARENT_SCOPE)
    set(
        L2FLOW_SDK_BASELINE_SHA256
        "${baseline_sha256}"
        PARENT_SCOPE)
endfunction()
