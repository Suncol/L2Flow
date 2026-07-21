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

if(NOT dynamic_section MATCHES
   "Shared library: \\[libmdl_api\\.so\\]")
    message(FATAL_ERROR
        "${BINARY} is missing its required link-time libmdl_api.so dependency")
endif()

execute_process(
    COMMAND "${READELF}" --wide --syms "${BINARY}"
    RESULT_VARIABLE symbols_status
    OUTPUT_VARIABLE symbols
    ERROR_VARIABLE symbols_error)

if(NOT symbols_status EQUAL 0)
    message(FATAL_ERROR
        "readelf symbol inspection failed for ${BINARY}: ${symbols_error}")
endif()

if(NOT symbols MATCHES "CompileReviewedConnectionSurface")
    message(FATAL_ERROR
        "${BINARY} omitted the reviewed subscriber/connection compile surface")
endif()
