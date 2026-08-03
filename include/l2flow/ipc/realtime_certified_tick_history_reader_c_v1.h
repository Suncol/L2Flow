#pragma once

#include <stddef.h>
#include <stdint.h>

#if defined(_WIN32)
#define L2FLOW_CERTIFIED_TICK_HISTORY_API_V1
#elif defined(__GNUC__) || defined(__clang__)
#define L2FLOW_CERTIFIED_TICK_HISTORY_API_V1 \
    __attribute__((visibility("default")))
#else
#define L2FLOW_CERTIFIED_TICK_HISTORY_API_V1
#endif

#ifdef __cplusplus
extern "C" {
#endif

typedef struct l2flow_certified_tick_history_reader_v1
    l2flow_certified_tick_history_reader_v1;

enum l2flow_certified_tick_history_open_error_v1 {
    L2FLOW_CERTIFIED_TICK_HISTORY_OPEN_OK_V1 = 0,
    L2FLOW_CERTIFIED_TICK_HISTORY_OPEN_NULL_OUTPUT_V1 = 1,
    L2FLOW_CERTIFIED_TICK_HISTORY_OPEN_INVALID_ARGUMENT_V1 = 2,
    L2FLOW_CERTIFIED_TICK_HISTORY_OPEN_UNSUPPORTED_ENDIAN_V1 = 3,
    L2FLOW_CERTIFIED_TICK_HISTORY_OPEN_SOCKET_PATH_INVALID_V1 = 4,
    L2FLOW_CERTIFIED_TICK_HISTORY_OPEN_SOCKET_PATH_UNSAFE_V1 = 5,
    L2FLOW_CERTIFIED_TICK_HISTORY_OPEN_SOCKET_CREATE_FAILED_V1 = 6,
    L2FLOW_CERTIFIED_TICK_HISTORY_OPEN_SOCKET_CONNECT_FAILED_V1 = 7,
    L2FLOW_CERTIFIED_TICK_HISTORY_OPEN_REQUEST_SEND_FAILED_V1 = 8,
    L2FLOW_CERTIFIED_TICK_HISTORY_OPEN_RESPONSE_RECEIVE_FAILED_V1 = 9,
    L2FLOW_CERTIFIED_TICK_HISTORY_OPEN_PROTOCOL_ERROR_V1 = 10,
    L2FLOW_CERTIFIED_TICK_HISTORY_OPEN_UNAVAILABLE_V1 = 11,
    L2FLOW_CERTIFIED_TICK_HISTORY_OPEN_SERVICE_INTERNAL_V1 = 12,
    L2FLOW_CERTIFIED_TICK_HISTORY_OPEN_DESCRIPTOR_INVALID_V1 = 13,
    L2FLOW_CERTIFIED_TICK_HISTORY_OPEN_MAPPING_FAILED_V1 = 14,
    L2FLOW_CERTIFIED_TICK_HISTORY_OPEN_LAYOUT_INVALID_V1 = 15,
    L2FLOW_CERTIFIED_TICK_HISTORY_OPEN_SESSION_MISMATCH_V1 = 16,
    L2FLOW_CERTIFIED_TICK_HISTORY_OPEN_RESOURCE_EXHAUSTED_V1 = 17,
    L2FLOW_CERTIFIED_TICK_HISTORY_OPEN_UNEXPECTED_FAILURE_V1 = 18,
};

enum l2flow_certified_tick_history_read_result_v1 {
    L2FLOW_CERTIFIED_TICK_HISTORY_READ_OK_V1 = 0,
    L2FLOW_CERTIFIED_TICK_HISTORY_READ_INVALID_ARGUMENT_V1 = 1,
    L2FLOW_CERTIFIED_TICK_HISTORY_READ_NOT_YET_PUBLISHED_V1 = 2,
    L2FLOW_CERTIFIED_TICK_HISTORY_READ_END_OF_STREAM_V1 = 3,
    L2FLOW_CERTIFIED_TICK_HISTORY_READ_PRODUCER_FAILED_V1 = 4,
    L2FLOW_CERTIFIED_TICK_HISTORY_READ_OUT_OF_RANGE_V1 = 5,
    L2FLOW_CERTIFIED_TICK_HISTORY_READ_INCONSISTENT_V1 = 6,
    L2FLOW_CERTIFIED_TICK_HISTORY_READ_CORRUPT_V1 = 7,
};

enum l2flow_certified_tick_history_state_v1 {
    L2FLOW_CERTIFIED_TICK_HISTORY_STATE_ACTIVE_V1 = 1,
    L2FLOW_CERTIFIED_TICK_HISTORY_STATE_COMPLETE_V1 = 2,
    L2FLOW_CERTIFIED_TICK_HISTORY_STATE_FAILED_V1 = 3,
};

enum l2flow_certified_tick_history_coverage_kind_v1 {
    L2FLOW_CERTIFIED_TICK_HISTORY_COVERAGE_FROM_OPEN_V1 = 1,
};

enum l2flow_certified_tick_history_failure_v1 {
    L2FLOW_CERTIFIED_TICK_HISTORY_FAILURE_NONE_V1 = 0,
    L2FLOW_CERTIFIED_TICK_HISTORY_FAILURE_INVALID_ARGUMENT_V1 = 1,
    L2FLOW_CERTIFIED_TICK_HISTORY_FAILURE_CANONICAL_SEQUENCE_V1 = 2,
    L2FLOW_CERTIFIED_TICK_HISTORY_FAILURE_TICK_CAPACITY_V1 = 3,
    L2FLOW_CERTIFIED_TICK_HISTORY_FAILURE_BACKING_COMMIT_V1 = 4,
    L2FLOW_CERTIFIED_TICK_HISTORY_FAILURE_ENVELOPE_INVALID_V1 = 5,
    L2FLOW_CERTIFIED_TICK_HISTORY_FAILURE_PUBLICATION_INVARIANT_V1 = 6,
    L2FLOW_CERTIFIED_TICK_HISTORY_FAILURE_UPSTREAM_V1 = 7,
    L2FLOW_CERTIFIED_TICK_HISTORY_FAILURE_STOPPED_V1 = 8,
    L2FLOW_CERTIFIED_TICK_HISTORY_FAILURE_FAILED_V1 = 9,
    L2FLOW_CERTIFIED_TICK_HISTORY_FAILURE_SOURCE_RETENTION_LOST_V1 = 10,
    L2FLOW_CERTIFIED_TICK_HISTORY_FAILURE_INCOMPLETE_NATIVE_PREFIX_V1 = 11,
    L2FLOW_CERTIFIED_TICK_HISTORY_FAILURE_SOURCE_READ_V1 = 12,
};

enum {
    L2FLOW_CERTIFIED_TICK_HISTORY_SLOT_BYTES_V1 = 512,
    L2FLOW_CERTIFIED_TICK_HISTORY_PAYLOAD_OFFSET_V1 = 64,
    L2FLOW_CERTIFIED_TICK_HISTORY_ENVELOPE_BYTES_V1 = 368,
};

typedef struct l2flow_certified_tick_history_expected_session_v1 {
    uint8_t run_id[16];
    uint64_t session_epoch;
    uint32_t trade_date;
    uint32_t reserved;
} l2flow_certified_tick_history_expected_session_v1;

typedef struct l2flow_certified_tick_history_session_v1 {
    uint8_t run_id[16];
    uint64_t session_epoch;
    uint64_t total_mapping_bytes;
    uint64_t tick_capacity;
    uint32_t trade_date;
    uint32_t slot_bytes;
    uint32_t coverage_kind;
    uint32_t reserved_coverage;
    uint64_t coverage_start_unix_ns;
} l2flow_certified_tick_history_session_v1;

typedef struct l2flow_certified_tick_history_status_v1 {
    uint32_t status_schema_version;
    uint32_t status_bytes;
    uint64_t publish_tag;
    uint64_t heartbeat_monotonic_ns;
    uint64_t canonical_apply_frontier;
    uint64_t generation;
    uint64_t published_tick_count;
    uint64_t committed_mapping_bytes;
    uint32_t state;
    uint32_t failure;
    uint8_t reserved[32];
} l2flow_certified_tick_history_status_v1;

// Exact stable copy of one 512-byte journal slot. payload_words[0..45] is the
// bit representation of the 368-byte RealtimeCertifiedTickEnvelopeV1;
// payload_words[46..55] is zero. Consumers must use payload offset 64 rather
// than treating the seqlock prefix as part of the envelope.
typedef struct l2flow_certified_tick_history_slot_v1 {
    uint64_t publish_tag;
    uint64_t reserved0[7];
    uint64_t payload_words[56];
} l2flow_certified_tick_history_slot_v1;

typedef struct l2flow_certified_tick_history_read_batch_result_v1 {
    uint32_t result_schema_version;
    uint32_t result_bytes;
    uint64_t records_written;
    uint64_t next_canonical_apply_sequence;
    l2flow_certified_tick_history_status_v1 status;
    uint8_t reserved[40];
} l2flow_certified_tick_history_read_batch_result_v1;

L2FLOW_CERTIFIED_TICK_HISTORY_API_V1 int
l2flow_certified_tick_history_reader_open_v1(
    const char* absolute_control_socket_path,
    const l2flow_certified_tick_history_expected_session_v1*
        expected_session,
    uint32_t timeout_ms,
    l2flow_certified_tick_history_reader_v1** output,
    int* system_error_number);

L2FLOW_CERTIFIED_TICK_HISTORY_API_V1 void
l2flow_certified_tick_history_reader_close_v1(
    l2flow_certified_tick_history_reader_v1* reader);

L2FLOW_CERTIFIED_TICK_HISTORY_API_V1 int
l2flow_certified_tick_history_reader_session_v1(
    const l2flow_certified_tick_history_reader_v1* reader,
    l2flow_certified_tick_history_session_v1* output);

L2FLOW_CERTIFIED_TICK_HISTORY_API_V1 int
l2flow_certified_tick_history_reader_status_v1(
    const l2flow_certified_tick_history_reader_v1* reader,
    l2flow_certified_tick_history_status_v1* output);

L2FLOW_CERTIFIED_TICK_HISTORY_API_V1 int
l2flow_certified_tick_history_reader_read_v1(
    const l2flow_certified_tick_history_reader_v1* reader,
    uint64_t first_canonical_apply_sequence,
    l2flow_certified_tick_history_slot_v1* output,
    size_t capacity,
    l2flow_certified_tick_history_read_batch_result_v1* result);

L2FLOW_CERTIFIED_TICK_HISTORY_API_V1 const char*
l2flow_certified_tick_history_open_error_name_v1(int error);

L2FLOW_CERTIFIED_TICK_HISTORY_API_V1 const char*
l2flow_certified_tick_history_read_result_name_v1(int result);

#ifdef __cplusplus
}
#endif

#undef L2FLOW_CERTIFIED_TICK_HISTORY_API_V1
