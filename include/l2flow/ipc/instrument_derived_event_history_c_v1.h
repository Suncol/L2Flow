#pragma once

#include "l2flow/ipc/realtime_shm_reader_c_v2.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
#include <type_traits>
#endif

#if defined(_WIN32)
#define L2FLOW_DERIVED_HISTORY_API_V1
#elif defined(__GNUC__) || defined(__clang__)
#define L2FLOW_DERIVED_HISTORY_API_V1 \
    __attribute__((visibility("default")))
#else
#define L2FLOW_DERIVED_HISTORY_API_V1
#endif

#ifdef __cplusplus
extern "C" {
#endif

typedef struct l2flow_instrument_derived_event_history_session_v1
    l2flow_instrument_derived_event_history_session_v1;

// One opaque session is bound permanently to one instrument and one market.
// All functions on it are serial-only. begin_full constructs the in-memory
// order state; every begin_update must use the exact EOF checkpoint returned
// by the preceding read on the same object. Switching instruments or
// recreating state from only this checkpoint is deliberately unsupported.

enum l2flow_instrument_derived_event_history_error_v1 {
    L2FLOW_INSTRUMENT_DERIVED_EVENT_HISTORY_OK_V1 = 0,
    L2FLOW_INSTRUMENT_DERIVED_EVENT_HISTORY_NULL_OUTPUT_V1 = 1,
    L2FLOW_INSTRUMENT_DERIVED_EVENT_HISTORY_INVALID_CONFIGURATION_V1 = 2,
    L2FLOW_INSTRUMENT_DERIVED_EVENT_HISTORY_INVALID_STATE_V1 = 3,
    L2FLOW_INSTRUMENT_DERIVED_EVENT_HISTORY_CHECKPOINT_MISMATCH_V1 = 4,
    L2FLOW_INSTRUMENT_DERIVED_EVENT_HISTORY_RAW_HISTORY_ERROR_V1 = 5,
    L2FLOW_INSTRUMENT_DERIVED_EVENT_HISTORY_WIRE_PROJECTION_ERROR_V1 = 6,
    L2FLOW_INSTRUMENT_DERIVED_EVENT_HISTORY_AGGREGATION_ERROR_V1 = 7,
    L2FLOW_INSTRUMENT_DERIVED_EVENT_HISTORY_RESOURCE_EXHAUSTED_V1 = 8,
    L2FLOW_INSTRUMENT_DERIVED_EVENT_HISTORY_FAILED_V1 = 9,
    L2FLOW_INSTRUMENT_DERIVED_EVENT_HISTORY_BUFFER_TOO_SMALL_V1 = 10,
};

enum l2flow_instrument_derived_event_market_v1 {
    L2FLOW_INSTRUMENT_DERIVED_EVENT_MARKET_SHANGHAI_V1 = 1,
    L2FLOW_INSTRUMENT_DERIVED_EVENT_MARKET_SHENZHEN_V1 = 2,
};

enum l2flow_instrument_derived_event_kind_v1 {
    L2FLOW_INSTRUMENT_DERIVED_EVENT_ORDER_REVISION_V1 = 1,
    L2FLOW_INSTRUMENT_DERIVED_EVENT_TRADE_V1 = 2,
    L2FLOW_INSTRUMENT_DERIVED_EVENT_CANCEL_V1 = 3,
    L2FLOW_INSTRUMENT_DERIVED_EVENT_STATUS_V1 = 4,
};

// Record schema 2 assigns semantic meaning to bytes which schema 1 required
// to be zero, without changing the fixed 320-byte row layout. Old readers
// must reject schema 2 rather than silently interpreting these bytes as
// reserved. The enclosing C function ABI remains V1.
enum l2flow_instrument_derived_event_row_schema_v1 {
    L2FLOW_INSTRUMENT_DERIVED_EVENT_ROW_SCHEMA_V2 = 2,
};

enum l2flow_instrument_derived_event_identity_v1 {
    L2FLOW_INSTRUMENT_DERIVED_EVENT_SOURCE_TICK_ORDINAL_INVALID_V2 = 0,
    L2FLOW_INSTRUMENT_DERIVED_EVENT_SOURCE_TICK_ORDINAL_VALID_V2 = 1,
};

// Cross-market meanings for fields which are present on the common flat row.
// The numeric values intentionally match the Shanghai core enums; Shenzhen
// source orders use SOURCE_EVENT/DIRECT/EXACT explicitly rather than leaving
// applicable facts encoded as UNKNOWN.
enum l2flow_instrument_derived_event_side_source_v1 {
    L2FLOW_INSTRUMENT_DERIVED_EVENT_SIDE_SOURCE_UNKNOWN_V1 = 0,
    L2FLOW_INSTRUMENT_DERIVED_EVENT_SIDE_SOURCE_DIRECT_V1 = 1,
    L2FLOW_INSTRUMENT_DERIVED_EVENT_SIDE_SOURCE_AGGRESSOR_V1 = 2,
};

enum l2flow_instrument_derived_event_order_source_v1 {
    L2FLOW_INSTRUMENT_DERIVED_EVENT_ORDER_SOURCE_UNKNOWN_V1 = 0,
    L2FLOW_INSTRUMENT_DERIVED_EVENT_ORDER_SOURCE_EVENT_V1 = 1,
    L2FLOW_INSTRUMENT_DERIVED_EVENT_ORDER_SOURCE_RECONSTRUCTED_V1 = 2,
};

enum l2flow_instrument_derived_event_price_source_v1 {
    L2FLOW_INSTRUMENT_DERIVED_EVENT_PRICE_SOURCE_UNKNOWN_V1 = 0,
    L2FLOW_INSTRUMENT_DERIVED_EVENT_PRICE_SOURCE_EVENT_V1 = 1,
    L2FLOW_INSTRUMENT_DERIVED_EVENT_PRICE_SOURCE_BUY_MAX_FILL_V1 = 2,
    L2FLOW_INSTRUMENT_DERIVED_EVENT_PRICE_SOURCE_SELL_MIN_FILL_V1 = 3,
};

enum l2flow_instrument_derived_event_original_quantity_status_v1 {
    L2FLOW_INSTRUMENT_DERIVED_EVENT_ORIGINAL_QUANTITY_UNKNOWN_V1 = 0,
    L2FLOW_INSTRUMENT_DERIVED_EVENT_ORIGINAL_QUANTITY_EXACT_V1 = 1,
    L2FLOW_INSTRUMENT_DERIVED_EVENT_ORIGINAL_QUANTITY_LOWER_BOUND_V1 = 2,
};

// All enum-valued byte fields retain the corresponding public market-core
// enum values. Applicability is explicit: consumers must use event_kind and
// the validity bytes rather than interpreting an inapplicable zero as fact.
typedef struct l2flow_instrument_derived_event_row_v1 {
    uint32_t record_schema_version;
    uint32_t record_bytes;
    uint64_t derived_event_sequence;

    uint32_t trade_date;
    uint32_t instrument_id;
    int64_t channel;
    int64_t order_id;
    int64_t buy_order_id;
    int64_t sell_order_id;

    int64_t price_p6;
    int64_t execution_boundary_price_p6;
    int64_t quantity;
    int64_t trade_amount_p6;
    int64_t published_quantity;
    int64_t original_quantity;
    int64_t remaining_quantity;
    int64_t source_matched_quantity;
    int64_t observed_pre_add_trade_quantity;
    int64_t post_add_trade_quantity;
    int64_t total_trade_quantity;
    int64_t total_cancel_quantity;

    uint64_t revision;
    uint64_t trade_count;
    uint64_t cancel_count;
    uint64_t quality_flags;
    uint64_t source_quality_flags;
    uint64_t source_market_notices;

    int64_t native_event_sequence;
    uint64_t source_sequence;
    uint64_t ingress_sequence;
    uint64_t tick_stream_sequence;
    uint64_t vendor_sequence_id;
    uint64_t event_time_ns_since_midnight;
    int64_t event_time_unix_ns;
    int64_t recv_realtime_ns;
    int64_t recv_monotonic_ns;
    uint32_t vendor_local_time_raw;
    // Schema 2: zero-based source_tick_event_ordinal. It is meaningful only
    // when reserved1[0] is VALID. Schema 1 required this scalar to be zero.
    uint32_t reserved0;
    uint64_t vendor_local_time_ns_since_midnight;

    uint8_t market;
    uint8_t event_kind;
    uint8_t operation;
    uint8_t finality;
    uint8_t side;
    uint8_t side_source;
    uint8_t aggressor;
    uint8_t phase;
    uint8_t phase_at_first;
    uint8_t phase_at_add;
    uint8_t phase_at_last;
    uint8_t order_type;
    uint8_t order_source;
    uint8_t price_source;
    uint8_t original_quantity_status;
    uint8_t price_valid;
    uint8_t execution_boundary_price_valid;
    uint8_t trade_amount_valid;
    uint8_t published_quantity_valid;
    uint8_t original_quantity_valid;
    uint8_t remaining_quantity_valid;
    uint8_t source_matched_quantity_valid;
    uint8_t add_seen;
    uint8_t apply_to_book;
    uint8_t referenced_order_found;
    uint8_t side_from_order;
    uint8_t event_time_valid;
    uint8_t event_time_unix_ns_valid;
    uint8_t vendor_local_time_valid;
    // Schema 2: [0] is source_tick_event_ordinal_valid; [1..2] remain zero.
    // A source-free trading-day Finalize row has [0]==INVALID, reserved0==0,
    // and tick_stream_sequence==0. It has no source-tick event UID.
    uint8_t reserved1[3];
} l2flow_instrument_derived_event_row_v1;

typedef struct l2flow_instrument_derived_event_checkpoint_v1 {
    l2flow_instrument_raw_event_history_checkpoint_v2 raw_checkpoint;
    uint64_t derived_event_sequence_exclusive;
    uint64_t order_state_count;
    uint32_t instrument_id;
    uint32_t trade_date;
    uint8_t market;
    uint8_t finalized;
    uint8_t reserved[6];
} l2flow_instrument_derived_event_checkpoint_v1;

#ifdef __cplusplus
static_assert(
    std::is_trivially_copyable_v<
        l2flow_instrument_derived_event_row_v1>);
static_assert(
    sizeof(l2flow_instrument_derived_event_row_v1) == 320U);
static_assert(
    sizeof(l2flow_instrument_derived_event_checkpoint_v1) == 344U);
static_assert(
    offsetof(
        l2flow_instrument_derived_event_row_v1,
        derived_event_sequence) == 8U);
static_assert(
    offsetof(
        l2flow_instrument_derived_event_row_v1,
        native_event_sequence) == 200U);
static_assert(
    offsetof(
        l2flow_instrument_derived_event_row_v1,
        market) == 288U);
#endif

L2FLOW_DERIVED_HISTORY_API_V1 int
l2flow_instrument_derived_event_history_session_open_v1(
    const char* absolute_control_socket_path,
    const l2flow_shm_session_info_v2* expected_session,
    uint32_t instrument_id,
    uint8_t market,
    size_t maximum_order_states,
    uint32_t timeout_ms,
    l2flow_instrument_derived_event_history_session_v1** output);

L2FLOW_DERIVED_HISTORY_API_V1 void
l2flow_instrument_derived_event_history_session_close_v1(
    l2flow_instrument_derived_event_history_session_v1* session);

L2FLOW_DERIVED_HISTORY_API_V1 int
l2flow_instrument_derived_event_history_begin_full_v1(
    l2flow_instrument_derived_event_history_session_v1* session,
    uint64_t expected_generation,
    uint32_t requested_raw_page_records);

L2FLOW_DERIVED_HISTORY_API_V1 int
l2flow_instrument_derived_event_history_begin_update_v1(
    l2flow_instrument_derived_event_history_session_v1* session,
    const l2flow_instrument_derived_event_checkpoint_v1*
        base_checkpoint,
    uint64_t expected_generation,
    uint32_t requested_raw_page_records);

// Reads one derived page into caller-owned rows. If capacity is insufficient,
// record_count receives the required size and BUFFER_TOO_SMALL is returned;
// the page is retained and the caller retries without advancing state.
// eof is a separate successful zero-row result.
L2FLOW_DERIVED_HISTORY_API_V1 int
l2flow_instrument_derived_event_history_read_v1(
    l2flow_instrument_derived_event_history_session_v1* session,
    l2flow_instrument_derived_event_row_v1* rows,
    size_t capacity,
    size_t* record_count,
    uint32_t* eof);

L2FLOW_DERIVED_HISTORY_API_V1 int
l2flow_instrument_derived_event_history_verified_checkpoint_v1(
    const l2flow_instrument_derived_event_history_session_v1* session,
    l2flow_instrument_derived_event_checkpoint_v1* output);

// Explicit clean-day finalization. BUFFER_TOO_SMALL has the same retry
// contract as read_v1. This call does not advance the raw checkpoint.
L2FLOW_DERIVED_HISTORY_API_V1 int
l2flow_instrument_derived_event_history_finalize_v1(
    l2flow_instrument_derived_event_history_session_v1* session,
    l2flow_instrument_derived_event_row_v1* rows,
    size_t capacity,
    size_t* record_count);

L2FLOW_DERIVED_HISTORY_API_V1 int
l2flow_instrument_derived_event_history_last_raw_error_v1(
    const l2flow_instrument_derived_event_history_session_v1* session);

L2FLOW_DERIVED_HISTORY_API_V1 const char*
l2flow_instrument_derived_event_history_error_name_v1(int error);

#ifdef __cplusplus
}
#endif

#undef L2FLOW_DERIVED_HISTORY_API_V1
