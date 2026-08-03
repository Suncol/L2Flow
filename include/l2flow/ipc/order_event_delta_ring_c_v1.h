#pragma once

#include "l2flow/ipc/instrument_derived_event_history_c_v1.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
#include <type_traits>
#endif

#if defined(_WIN32)
#define L2FLOW_ORDER_EVENT_DELTA_API_V1
#elif defined(__GNUC__) || defined(__clang__)
#define L2FLOW_ORDER_EVENT_DELTA_API_V1 \
    __attribute__((visibility("default")))
#else
#define L2FLOW_ORDER_EVENT_DELTA_API_V1
#endif

#ifdef __cplusplus
extern "C" {
#endif

typedef struct l2flow_order_event_delta_reader_v1
    l2flow_order_event_delta_reader_v1;

enum l2flow_order_event_delta_error_v1 {
    L2FLOW_ORDER_EVENT_DELTA_OK_V1 = 0,
    L2FLOW_ORDER_EVENT_DELTA_NULL_OUTPUT_V1 = 1,
    L2FLOW_ORDER_EVENT_DELTA_INVALID_ARGUMENT_V1 = 2,
    L2FLOW_ORDER_EVENT_DELTA_DESCRIPTOR_INVALID_V1 = 3,
    L2FLOW_ORDER_EVENT_DELTA_DESCRIPTOR_NOT_READ_ONLY_V1 = 4,
    L2FLOW_ORDER_EVENT_DELTA_MAPPING_FAILED_V1 = 5,
    L2FLOW_ORDER_EVENT_DELTA_LAYOUT_INVALID_V1 = 6,
    L2FLOW_ORDER_EVENT_DELTA_SESSION_MISMATCH_V1 = 7,
    L2FLOW_ORDER_EVENT_DELTA_UNAVAILABLE_V1 = 8,
    L2FLOW_ORDER_EVENT_DELTA_INCONSISTENT_READ_V1 = 9,
    L2FLOW_ORDER_EVENT_DELTA_OVERRUN_V1 = 10,
    L2FLOW_ORDER_EVENT_DELTA_RESOURCE_EXHAUSTED_V1 = 11,
    L2FLOW_ORDER_EVENT_DELTA_UNEXPECTED_FAILURE_V1 = 12,
};

enum l2flow_order_event_delta_producer_state_v1 {
    L2FLOW_ORDER_EVENT_DELTA_INITIALIZING_V1 = 1,
    L2FLOW_ORDER_EVENT_DELTA_ACTIVE_V1 = 2,
    L2FLOW_ORDER_EVENT_DELTA_DRAINING_V1 = 3,
    L2FLOW_ORDER_EVENT_DELTA_STOPPED_CLEAN_V1 = 4,
    L2FLOW_ORDER_EVENT_DELTA_FAILED_V1 = 5,
};

enum l2flow_order_event_delta_header_flag_v1 {
    L2FLOW_ORDER_EVENT_DELTA_COVERAGE_LOST_V1 = 1U << 0U,
};

enum l2flow_order_event_delta_temporal_coverage_v1 {
    L2FLOW_ORDER_EVENT_DELTA_FROM_MARKET_OPEN_V1 = 1,
    L2FLOW_ORDER_EVENT_DELTA_FROM_PROCESS_START_V1 = 2,
};

enum l2flow_order_event_delta_stream_quality_v1 {
    // Dense local tick_stream_sequence only. This does not assert native
    // vendor-sequence completeness.
    L2FLOW_ORDER_EVENT_DELTA_LOCAL_TICK_STREAM_CONTIGUOUS_V1 = 1,
};

// Exact identity expected from the descriptor. V1.1 reuses former reserved
// storage for temporal_coverage and stream_quality without changing this
// 64-byte ABI. Every remaining reserved byte must be zero. There is no
// wildcard field: every semantic and physical identity field must match.
typedef struct l2flow_order_event_delta_session_v1 {
    uint8_t run_id[16];
    uint64_t session_epoch;
    uint32_t trade_date;
    uint32_t temporal_coverage;
    uint64_t ring_capacity;
    uint64_t total_mapping_bytes;
    uint32_t stream_quality;
    uint32_t reserved0;
    uint8_t reserved[8];
} l2flow_order_event_delta_session_v1;

// Fixed-width result returned for every syntactically valid read call. On
// success, records_written rows are valid and next_sequence is committed as
// the reader's next stateful cursor. On overrun or any other stream failure,
// records_written is zero, the cursor is not advanced, and the reader remains
// permanently fail-closed.
typedef struct l2flow_order_event_delta_read_result_v1 {
    uint32_t result_schema_version;
    uint32_t result_bytes;
    uint64_t records_written;
    uint64_t next_sequence;
    uint64_t observed_sequence;
    uint64_t published_event_sequence;
    uint64_t consumed_source_tick_sequence;
    uint64_t heartbeat_monotonic_ns;
    uint32_t producer_state;
    uint32_t header_flags;
    uint8_t reserved[16];
} l2flow_order_event_delta_read_result_v1;

#ifdef __cplusplus
static_assert(
    std::is_trivially_copyable_v<
        l2flow_order_event_delta_session_v1>);
static_assert(
    std::is_trivially_copyable_v<
        l2flow_order_event_delta_read_result_v1>);
static_assert(
    sizeof(l2flow_order_event_delta_session_v1) == 64U);
static_assert(
    sizeof(l2flow_order_event_delta_read_result_v1) == 80U);
static_assert(
    offsetof(
        l2flow_order_event_delta_session_v1,
        session_epoch) == 16U);
static_assert(
    offsetof(
        l2flow_order_event_delta_session_v1,
        temporal_coverage) == 28U);
static_assert(
    offsetof(
        l2flow_order_event_delta_session_v1,
        stream_quality) == 48U);
static_assert(
    offsetof(
        l2flow_order_event_delta_read_result_v1,
        records_written) == 8U);
static_assert(
    sizeof(l2flow_instrument_derived_event_row_v1) == 320U);
#endif

// The descriptor must be O_RDONLY. Open duplicates it; ownership of the input
// descriptor remains with the caller, and the returned reader owns only its
// private duplicate. expected_session is copied, not retained. The original
// entry point always starts at event sequence one.
L2FLOW_ORDER_EVENT_DELTA_API_V1 int
l2flow_order_event_delta_reader_open_v1(
    int read_only_descriptor,
    const l2flow_order_event_delta_session_v1* expected_session,
    l2flow_order_event_delta_reader_v1** output,
    int* system_error_number);

// Additive history-to-live attachment. start_event_sequence is the first
// dense event sequence requested by this new reader and must be positive.
// The start is fixed at construction: no operation can move a failed reader
// to another sequence. The first read performs the normal retention check and
// returns OVERRUN if this explicit history boundary is no longer retained.
L2FLOW_ORDER_EVENT_DELTA_API_V1 int
l2flow_order_event_delta_reader_open_at_v1(
    int read_only_descriptor,
    const l2flow_order_event_delta_session_v1* expected_session,
    uint64_t start_event_sequence,
    l2flow_order_event_delta_reader_v1** output,
    int* system_error_number);

L2FLOW_ORDER_EVENT_DELTA_API_V1 void
l2flow_order_event_delta_reader_close_v1(
    l2flow_order_event_delta_reader_v1* reader);

// Serial-only and stateful. No expected cursor is accepted from the caller:
// the reader begins at its construction sequence and commits its own next
// cursor after each successful call. capacity is a fixed uint64 ABI value and
// may be zero, in which case rows may be NULL and the call acts as a
// non-consuming poll.
//
// One producer commit always contains every row caused by one source tick,
// but a finite caller buffer may split that already-committed tick across
// multiple read calls. A call boundary is therefore not a source-tick
// transaction boundary. A consumer which applies one source tick atomically
// must group rows by row.tick_stream_sequence and retain the last group until
// either a later row has a different tick_stream_sequence or
// next_sequence == published_event_sequence + 1 proves that the committed
// prefix (which ends only between complete source-tick batches) was drained.
L2FLOW_ORDER_EVENT_DELTA_API_V1 int
l2flow_order_event_delta_reader_read_v1(
    l2flow_order_event_delta_reader_v1* reader,
    l2flow_instrument_derived_event_row_v1* rows,
    uint64_t capacity,
    l2flow_order_event_delta_read_result_v1* result);

L2FLOW_ORDER_EVENT_DELTA_API_V1 int
l2flow_order_event_delta_reader_session_v1(
    const l2flow_order_event_delta_reader_v1* reader,
    l2flow_order_event_delta_session_v1* output);

L2FLOW_ORDER_EVENT_DELTA_API_V1 int
l2flow_order_event_delta_reader_state_v1(
    const l2flow_order_event_delta_reader_v1* reader,
    uint32_t* output);

L2FLOW_ORDER_EVENT_DELTA_API_V1 const char*
l2flow_order_event_delta_error_name_v1(int error);

#ifdef __cplusplus
}
#endif

#undef L2FLOW_ORDER_EVENT_DELTA_API_V1
