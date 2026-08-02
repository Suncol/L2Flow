#pragma once

#include "l2flow/ipc/instrument_derived_event_history_c_v1.h"

#include <stddef.h>
#include <stdint.h>

#if defined(_WIN32)
#define L2FLOW_CERTIFIED_ORDER_EVENT_API_V1
#elif defined(__GNUC__) || defined(__clang__)
#define L2FLOW_CERTIFIED_ORDER_EVENT_API_V1 \
    __attribute__((visibility("default")))
#else
#define L2FLOW_CERTIFIED_ORDER_EVENT_API_V1
#endif

#ifdef __cplusplus
extern "C" {
#endif

typedef struct l2flow_certified_order_event_reader_v1
    l2flow_certified_order_event_reader_v1;

enum l2flow_certified_order_event_open_error_v1 {
    L2FLOW_CERTIFIED_ORDER_EVENT_OPEN_OK_V1 = 0,
    L2FLOW_CERTIFIED_ORDER_EVENT_OPEN_NULL_OUTPUT_V1 = 1,
    L2FLOW_CERTIFIED_ORDER_EVENT_OPEN_INVALID_ARGUMENT_V1 = 2,
    L2FLOW_CERTIFIED_ORDER_EVENT_OPEN_TICK_READER_FAILED_V1 = 3,
    L2FLOW_CERTIFIED_ORDER_EVENT_OPEN_SOCKET_PATH_INVALID_V1 = 4,
    L2FLOW_CERTIFIED_ORDER_EVENT_OPEN_SOCKET_PATH_UNSAFE_V1 = 5,
    L2FLOW_CERTIFIED_ORDER_EVENT_OPEN_SOCKET_CREATE_FAILED_V1 = 6,
    L2FLOW_CERTIFIED_ORDER_EVENT_OPEN_SOCKET_CONNECT_FAILED_V1 = 7,
    L2FLOW_CERTIFIED_ORDER_EVENT_OPEN_REQUEST_SEND_FAILED_V1 = 8,
    L2FLOW_CERTIFIED_ORDER_EVENT_OPEN_RESPONSE_RECEIVE_FAILED_V1 = 9,
    L2FLOW_CERTIFIED_ORDER_EVENT_OPEN_PROTOCOL_ERROR_V1 = 10,
    L2FLOW_CERTIFIED_ORDER_EVENT_OPEN_UNAVAILABLE_V1 = 11,
    L2FLOW_CERTIFIED_ORDER_EVENT_OPEN_SERVICE_INTERNAL_V1 = 12,
    L2FLOW_CERTIFIED_ORDER_EVENT_OPEN_DESCRIPTOR_INVALID_V1 = 13,
    L2FLOW_CERTIFIED_ORDER_EVENT_OPEN_MAPPING_FAILED_V1 = 14,
    L2FLOW_CERTIFIED_ORDER_EVENT_OPEN_LAYOUT_INVALID_V1 = 15,
    L2FLOW_CERTIFIED_ORDER_EVENT_OPEN_SESSION_MISMATCH_V1 = 16,
    L2FLOW_CERTIFIED_ORDER_EVENT_OPEN_RESOURCE_EXHAUSTED_V1 = 17,
    L2FLOW_CERTIFIED_ORDER_EVENT_OPEN_UNEXPECTED_FAILURE_V1 = 18,
};

enum l2flow_certified_order_event_read_result_v1 {
    L2FLOW_CERTIFIED_ORDER_EVENT_READ_OK_V1 = 0,
    L2FLOW_CERTIFIED_ORDER_EVENT_READ_NO_DATA_V1 = 1,
    L2FLOW_CERTIFIED_ORDER_EVENT_READ_NOT_YET_PUBLISHED_V1 = 2,
    L2FLOW_CERTIFIED_ORDER_EVENT_READ_OUT_OF_RANGE_V1 = 3,
    L2FLOW_CERTIFIED_ORDER_EVENT_READ_INCONSISTENT_V1 = 4,
    L2FLOW_CERTIFIED_ORDER_EVENT_READ_CORRUPT_V1 = 5,
};

enum l2flow_certified_order_event_state_v1 {
    L2FLOW_CERTIFIED_ORDER_EVENT_STATE_DISABLED_V1 = 1,
    L2FLOW_CERTIFIED_ORDER_EVENT_STATE_NO_DATA_V1 = 2,
    L2FLOW_CERTIFIED_ORDER_EVENT_STATE_CONTIGUOUS_V1 = 3,
    L2FLOW_CERTIFIED_ORDER_EVENT_STATE_GAP_OPEN_V1 = 4,
    L2FLOW_CERTIFIED_ORDER_EVENT_STATE_CATCHING_UP_V1 = 5,
    L2FLOW_CERTIFIED_ORDER_EVENT_STATE_FROZEN_CONFLICT_V1 = 6,
    L2FLOW_CERTIFIED_ORDER_EVENT_STATE_FROZEN_RESOURCE_V1 = 7,
    L2FLOW_CERTIFIED_ORDER_EVENT_STATE_STOPPED_V1 = 8,
};

enum l2flow_certified_order_event_coverage_flag_v1 {
    L2FLOW_CERTIFIED_ORDER_EVENT_COVERAGE_FROM_OPEN_V1 = 1U << 0U,
    L2FLOW_CERTIFIED_ORDER_EVENT_STARTUP_PREFIX_RECOVERED_V1 = 1U << 1U,
};

typedef struct l2flow_certified_order_event_expected_session_v1 {
    uint8_t run_id[16];
    uint64_t session_epoch;
    uint32_t trade_date;
    uint32_t reserved;
} l2flow_certified_order_event_expected_session_v1;

typedef struct l2flow_certified_order_event_session_v1 {
    uint8_t run_id[16];
    uint64_t session_epoch;
    uint64_t event_capacity;
    uint32_t trade_date;
    uint32_t coverage_flags;
    uint8_t reserved[24];
} l2flow_certified_order_event_session_v1;

// One coherent Tick/Event status cut. Quality and native-gap counters come
// from the CERTIFIED Tick header; the Event fields describe the append-only
// full-day journal. Only rows at or below coherent_canonical_apply_frontier
// are returned by read APIs.
typedef struct l2flow_certified_order_event_status_v1 {
    uint32_t status_schema_version;
    uint32_t status_bytes;
    uint32_t coverage_flags;
    uint32_t certified_state;

    uint64_t tick_publish_tag;
    uint64_t tick_heartbeat_monotonic_ns;
    uint64_t tick_canonical_apply_frontier;
    uint64_t correction_epoch;
    uint64_t observed_native_message_count;
    uint64_t certified_tick_count;
    uint64_t exact_duplicate_message_count;
    uint64_t gap_opened_count;
    uint64_t gap_recovered_count;
    uint64_t conflicting_duplicate_count;
    uint64_t resource_exhaustion_count;
    uint64_t pending_token_count;

    uint64_t event_publish_tag;
    uint64_t event_heartbeat_monotonic_ns;
    uint64_t event_canonical_apply_frontier;
    uint64_t event_published_sequence;
    uint64_t event_generation;
    uint64_t shanghai_order_state_count;
    uint64_t shenzhen_order_state_count;
    uint64_t committed_mapping_bytes;
    uint64_t coherent_canonical_apply_frontier;

    uint32_t channel_state_count;
    uint32_t gap_open_channel_count;
    uint32_t catching_up_channel_count;
    uint32_t frozen_channel_count;
    uint8_t reserved[32];
} l2flow_certified_order_event_status_v1;

typedef struct l2flow_certified_order_event_envelope_v1 {
    uint64_t canonical_apply_sequence;
    l2flow_instrument_derived_event_row_v1 event;
} l2flow_certified_order_event_envelope_v1;

typedef struct l2flow_certified_order_event_read_batch_result_v1 {
    uint32_t result_schema_version;
    uint32_t result_bytes;
    uint64_t records_written;
    uint64_t next_event_sequence;
    l2flow_certified_order_event_status_v1 status;
    uint8_t reserved[32];
} l2flow_certified_order_event_read_batch_result_v1;

L2FLOW_CERTIFIED_ORDER_EVENT_API_V1 int
l2flow_certified_order_event_reader_open_v1(
    const char* absolute_control_socket_path,
    const l2flow_certified_order_event_expected_session_v1*
        expected_session,
    uint32_t timeout_ms,
    l2flow_certified_order_event_reader_v1** output,
    int* system_error_number);

L2FLOW_CERTIFIED_ORDER_EVENT_API_V1 void
l2flow_certified_order_event_reader_close_v1(
    l2flow_certified_order_event_reader_v1* reader);

L2FLOW_CERTIFIED_ORDER_EVENT_API_V1 int
l2flow_certified_order_event_reader_session_v1(
    const l2flow_certified_order_event_reader_v1* reader,
    l2flow_certified_order_event_session_v1* output);

L2FLOW_CERTIFIED_ORDER_EVENT_API_V1 int
l2flow_certified_order_event_reader_status_v1(
    const l2flow_certified_order_event_reader_v1* reader,
    l2flow_certified_order_event_status_v1* output);

L2FLOW_CERTIFIED_ORDER_EVENT_API_V1 int
l2flow_certified_order_event_reader_event_v1(
    const l2flow_certified_order_event_reader_v1* reader,
    uint64_t derived_event_sequence,
    l2flow_certified_order_event_envelope_v1* output);

L2FLOW_CERTIFIED_ORDER_EVENT_API_V1 int
l2flow_certified_order_event_reader_read_v1(
    const l2flow_certified_order_event_reader_v1* reader,
    uint64_t expected_event_sequence,
    l2flow_certified_order_event_envelope_v1* output,
    size_t capacity,
    l2flow_certified_order_event_read_batch_result_v1* result);

L2FLOW_CERTIFIED_ORDER_EVENT_API_V1 const char*
l2flow_certified_order_event_open_error_name_v1(int error);

L2FLOW_CERTIFIED_ORDER_EVENT_API_V1 const char*
l2flow_certified_order_event_read_result_name_v1(int result);

#ifdef __cplusplus
}
#endif

#undef L2FLOW_CERTIFIED_ORDER_EVENT_API_V1
