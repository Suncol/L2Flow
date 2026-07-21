# Only builds mdl_latency_output demo.

cmake_minimum_required(VERSION 3.16)
set(CMAKE_C_COMPILER gcc-4.8)
set(CMAKE_CXX_COMPILER g++-4.8)

project(mdl_latency_output)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/../bin)
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED on)
set(CMAKE_CXX_EXTENSIONS off)
set(EXE ${PROJECT_NAME})

add_executable(${EXE} mdl_latency_output.cpp)

target_include_directories(${EXE} PRIVATE
    ${CMAKE_SOURCE_DIR}/../3rd
    ${CMAKE_SOURCE_DIR}/../include)

set(LIB_DIR ${CMAKE_SOURCE_DIR}/../libs)
if(WIN32)
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_DEBUG ${CMAKE_RUNTIME_OUTPUT_DIRECTORY})
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_RELEASE ${CMAKE_RUNTIME_OUTPUT_DIRECTORY})
    set(LIB_DIR ${LIB_DIR}/win64)
    file(GLOB dll_files ${LIB_DIR}/*.dll)
    file(COPY ${dll_files} DESTINATION ${CMAKE_RUNTIME_OUTPUT_DIRECTORY})
    if (CMAKE_BUILD_TYPE STREQUAL "Debug")
        target_link_libraries(${EXE} mdl_api jsoncppd)
    else()
        target_link_libraries(${EXE} mdl_api jsoncpp)
    endif()
elseif(UNIX)
    set(LIB_DIR ${LIB_DIR}/linux)
    target_link_libraries(${EXE} mdl_api dl pthread rt json)
endif()
target_link_directories(${EXE} PRIVATE ${LIB_DIR})
