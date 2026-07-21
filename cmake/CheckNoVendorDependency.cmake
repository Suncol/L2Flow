if(NOT DEFINED READELF OR NOT DEFINED BINARY)
    message(FATAL_ERROR "READELF and BINARY are required")
endif()

execute_process(
    COMMAND "${READELF}" -d "${BINARY}"
    RESULT_VARIABLE readelf_status
    OUTPUT_VARIABLE dynamic_section
    ERROR_VARIABLE readelf_error)

if(NOT readelf_status EQUAL 0)
    message(FATAL_ERROR
        "readelf failed for ${BINARY}: ${readelf_error}")
endif()

if(dynamic_section MATCHES "libmdl_api")
    message(FATAL_ERROR
        "${BINARY} has a forbidden link-time libmdl_api dependency")
endif()
