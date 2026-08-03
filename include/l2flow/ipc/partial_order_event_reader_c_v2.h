#pragma once

#include "l2flow/ipc/instrument_derived_event_history_c_v1.h"

#include <stddef.h>
#include <stdint.h>

#if defined(_WIN32)
#define L2FLOW_PARTIAL_ORDER_EVENT_API_V2
#elif defined(__GNUC__) || defined(__clang__)
#define L2FLOW_PARTIAL_ORDER_EVENT_API_V2 \
    __attribute__((visibility("default")))
#else
#define L2FLOW_PARTIAL_ORDER_EVENT_API_V2
#endif

#ifdef __cplusplus
extern "C" {
#endif

typedef struct l2flow_partial_order_event_reader_v2
    l2flow_partial_order_event_reader_v2;

enum l2flow_partial_order_event_open_error_v2 {
    L2FLOW_PARTIAL_ORDER_EVENT_OPEN_OK_V2 = 0,
    L2FLOW_PARTIAL_ORDER_EVENT_OPEN_NULL_OUTPUT_V2 = 1,
    L2FLOW_PARTIAL_ORDER_EVENT_OPEN_INVALID_ARGUMENT_V2 = 2,
    L2FLOW_PARTIAL_ORDER_EVENT_OPEN_BROKER_UNAVAILABLE_V2 = 3,
    L2FLOW_PARTIAL_ORDER_EVENT_OPEN_BROKER_PROTOCOL_ERROR_V2 = 4,
    L2FLOW_PARTIAL_ORDER_EVENT_OPEN_BROKER_PEER_REJECTED_V2 = 5,
    L2FLOW_PARTIAL_ORDER_EVENT_OPEN_BROKER_SESSION_MISMATCH_V2 = 6,
    L2FLOW_PARTIAL_ORDER_EVENT_OPEN_BROKER_GENERATION_REJECTED_V2 = 7,
    L2FLOW_PARTIAL_ORDER_EVENT_OPEN_BROKER_DESCRIPTOR_REJECTED_V2 = 8,
    L2FLOW_PARTIAL_ORDER_EVENT_OPEN_BROKER_CONTINUITY_REJECTED_V2 = 9,
    L2FLOW_PARTIAL_ORDER_EVENT_OPEN_BROKER_INTERNAL_ERROR_V2 = 10,
    L2FLOW_PARTIAL_ORDER_EVENT_OPEN_DESCRIPTOR_STAT_FAILED_V2 = 11,
    L2FLOW_PARTIAL_ORDER_EVENT_OPEN_DESCRIPTOR_SEAL_MISMATCH_V2 = 12,
    L2FLOW_PARTIAL_ORDER_EVENT_OPEN_MAPPING_FAILED_V2 = 13,
    L2FLOW_PARTIAL_ORDER_EVENT_OPEN_INCOMPATIBLE_LAYOUT_V2 = 14,
    L2FLOW_PARTIAL_ORDER_EVENT_OPEN_MAPPING_SESSION_MISMATCH_V2 = 15,
    L2FLOW_PARTIAL_ORDER_EVENT_OPEN_NO_STABLE_CUT_V2 = 16,
    // A persisted cursor names one immutable publication/correction pair.
    // Reattaching it to any other pair requires replacement from that
    // generation's process-start prefix; the API never silently resumes it.
    L2FLOW_PARTIAL_ORDER_EVENT_OPEN_FULL_REPLACEMENT_REQUIRED_V2 = 17,
    L2FLOW_PARTIAL_ORDER_EVENT_OPEN_RESOURCE_EXHAUSTED_V2 = 18,
    L2FLOW_PARTIAL_ORDER_EVENT_OPEN_UNEXPECTED_FAILURE_V2 = 19,
    L2FLOW_PARTIAL_ORDER_EVENT_OPEN_UNSUPPORTED_ORDERING_QUALITY_V2 = 20,
};

enum l2flow_partial_order_event_read_result_v2 {
    L2FLOW_PARTIAL_ORDER_EVENT_READ_OK_V2 = 0,
    L2FLOW_PARTIAL_ORDER_EVENT_READ_INVALID_ARGUMENT_V2 = 1,
    L2FLOW_PARTIAL_ORDER_EVENT_READ_INCONSISTENT_V2 = 2,
    L2FLOW_PARTIAL_ORDER_EVENT_READ_NOT_YET_PUBLISHED_V2 = 3,
    L2FLOW_PARTIAL_ORDER_EVENT_READ_OUTPUT_TOO_SMALL_V2 = 4,
    L2FLOW_PARTIAL_ORDER_EVENT_READ_NOT_FOUND_V2 = 5,
    L2FLOW_PARTIAL_ORDER_EVENT_READ_CORRUPT_V2 = 6,
    L2FLOW_PARTIAL_ORDER_EVENT_READ_FULL_REPLACEMENT_REQUIRED_V2 = 7,
    L2FLOW_PARTIAL_ORDER_EVENT_READ_RESOURCE_EXHAUSTED_V2 = 8,
    L2FLOW_PARTIAL_ORDER_EVENT_READ_UNEXPECTED_FAILURE_V2 = 9,
};

enum l2flow_partial_order_event_temporal_coverage_v2 {
    // This is deliberately distinct from CERTIFIED FROM_OPEN coverage.
    L2FLOW_PARTIAL_ORDER_EVENT_COVERAGE_PROCESS_START_V2 = 1,
};

enum l2flow_partial_order_event_ordering_quality_v2 {
    // V2 supports only this process-owned bounded partial quality. The
    // current feeder path exposes no authoritative ordered marker, writer
    // ACK, manifest barrier, or other native-completeness proof, so no
    // proof-valued enumerator exists.
    L2FLOW_PARTIAL_ORDER_EVENT_ORDERING_BOUNDED_REORDERED_PARTIAL_V2 = 1,
};

enum l2flow_partial_order_event_service_state_v2 {
    L2FLOW_PARTIAL_ORDER_EVENT_STATE_INITIALIZING_V2 = 1,
    L2FLOW_PARTIAL_ORDER_EVENT_STATE_CONTIGUOUS_V2 = 2,
    L2FLOW_PARTIAL_ORDER_EVENT_STATE_REORDERING_V2 = 3,
    L2FLOW_PARTIAL_ORDER_EVENT_STATE_CATCHING_UP_V2 = 4,
    L2FLOW_PARTIAL_ORDER_EVENT_STATE_FROZEN_CONFLICT_V2 = 5,
    L2FLOW_PARTIAL_ORDER_EVENT_STATE_FROZEN_RESOURCE_V2 = 6,
    L2FLOW_PARTIAL_ORDER_EVENT_STATE_RESTARTING_V2 = 7,
    L2FLOW_PARTIAL_ORDER_EVENT_STATE_CORRECTION_PENDING_V2 = 8,
    L2FLOW_PARTIAL_ORDER_EVENT_STATE_STOPPED_CLEAN_V2 = 9,
};

enum l2flow_partial_order_event_last_error_v2 {
    L2FLOW_PARTIAL_ORDER_EVENT_ERROR_NONE_V2 = 0,
    L2FLOW_PARTIAL_ORDER_EVENT_ERROR_OUT_OF_ORDER_INPUT_V2 = 1,
    L2FLOW_PARTIAL_ORDER_EVENT_ERROR_CONFLICTING_DUPLICATE_V2 = 2,
    L2FLOW_PARTIAL_ORDER_EVENT_ERROR_RESOURCE_EXHAUSTED_V2 = 3,
    L2FLOW_PARTIAL_ORDER_EVENT_ERROR_WORKER_EXITED_V2 = 4,
    L2FLOW_PARTIAL_ORDER_EVENT_ERROR_PERMANENT_GAP_V2 = 5,
    L2FLOW_PARTIAL_ORDER_EVENT_ERROR_PROJECTION_FAILURE_V2 = 6,
    L2FLOW_PARTIAL_ORDER_EVENT_ERROR_PUBLICATION_FAILURE_V2 = 7,
    L2FLOW_PARTIAL_ORDER_EVENT_ERROR_PUBLICATION_INVARIANT_V2 = 8,
};

enum l2flow_partial_order_event_broker_state_v2 {
    L2FLOW_PARTIAL_ORDER_EVENT_BROKER_UNAVAILABLE_V2 = 1,
    L2FLOW_PARTIAL_ORDER_EVENT_BROKER_READY_V2 = 2,
    L2FLOW_PARTIAL_ORDER_EVENT_BROKER_STALE_V2 = 3,
    L2FLOW_PARTIAL_ORDER_EVENT_BROKER_RESTARTING_V2 = 4,
    L2FLOW_PARTIAL_ORDER_EVENT_BROKER_STOPPED_CLEAN_V2 = 5,
};

enum l2flow_partial_order_event_channel_flag_v2 {
    L2FLOW_PARTIAL_ORDER_EVENT_CHANNEL_ORIGIN_ESTABLISHED_V2 = 1U << 0U,
    L2FLOW_PARTIAL_ORDER_EVENT_CHANNEL_AFFECTED_V2 = 1U << 1U,
    L2FLOW_PARTIAL_ORDER_EVENT_CHANNEL_STALE_V2 = 1U << 2U,
};

typedef struct l2flow_partial_order_event_expected_session_v2 {
    uint8_t run_id[16];
    uint64_t session_epoch;
    uint32_t trade_date;
    uint32_t reserved;
} l2flow_partial_order_event_expected_session_v2;

// Portable only inside the named process-start generation. This is a cursor,
// not a retained checkpoint, feeder watermark, writer ACK, or manifest.
typedef struct l2flow_partial_order_event_checkpoint_v2 {
    uint8_t run_id[16];
    uint64_t session_epoch;
    uint64_t publication_generation;
    uint64_t correction_epoch;
    uint64_t next_event_sequence;
    uint64_t next_order_state_physical_slot;
    uint32_t trade_date;
    uint32_t reserved;
} l2flow_partial_order_event_checkpoint_v2;

typedef struct l2flow_partial_order_event_session_v2 {
    uint8_t run_id[16];
    uint64_t session_epoch;
    uint64_t publication_generation;
    uint64_t correction_epoch;
    uint64_t coverage_start_unix_ns;
    uint64_t event_capacity;
    uint64_t order_state_capacity;
    uint64_t total_mapping_bytes;
    uint32_t trade_date;
    uint32_t temporal_coverage;
    uint32_t ordering_quality;
    uint32_t affected_channel_capacity;
    uint32_t broker_state;
    uint32_t broker_stale;
    uint8_t reserved[8];
} l2flow_partial_order_event_session_v2;

// One coherent, CRC-verified journal cut combined with a live broker lifecycle
// snapshot. broker_state/broker_stale come from the independent broker-owned
// lifecycle mapping; journal fields remain the pinned generation's last-good
// values after a worker or journal failure. A corrupt or inconsistent snapshot,
// and an expired heartbeat outside the clean terminal state, fail closed as
// broker_stale without hiding same-identity last-good data. Explicit status and
// empty Event reads evaluate heartbeat age. A nonempty Event batch avoids a
// local clock read but still checks mapped broker state, publication identity,
// and the current worker lease's async-failure signal before copying rows. A
// heartbeat-only broker disappearance during a nonempty backlog therefore
// becomes stale at the next empty read or explicit status/query call.
typedef struct l2flow_partial_order_event_status_v2 {
    uint32_t status_schema_version;
    uint32_t status_bytes;
    uint8_t run_id[16];
    uint64_t session_epoch;
    uint64_t publication_generation;
    uint64_t correction_epoch;
    uint64_t coverage_start_unix_ns;
    uint64_t event_capacity;
    uint64_t order_state_capacity;
    uint64_t commit_sequence;
    uint64_t heartbeat_monotonic_ns;
    uint64_t captured_source_frontier;
    uint64_t canonical_apply_frontier;
    uint64_t event_published_frontier;
    uint64_t history_generation;
    uint64_t order_state_generation;
    uint64_t order_state_canonical_frontier;
    uint64_t committed_event_region_bytes;
    uint64_t shanghai_order_state_count;
    uint64_t shenzhen_order_state_count;
    uint64_t pending_count;
    uint64_t reorder_high_water;
    uint64_t oldest_gap_age_ns;
    uint32_t trade_date;
    uint32_t temporal_coverage;
    uint32_t ordering_quality;
    uint32_t service_state;
    uint32_t stale;
    uint32_t last_error;
    uint32_t affected_channel_count;
    uint32_t channel_health_count;
    uint32_t affected_channel_capacity;
    uint32_t broker_state;
    uint32_t broker_stale;
    uint8_t reserved[24];
} l2flow_partial_order_event_status_v2;

typedef struct l2flow_partial_order_event_envelope_v2 {
    uint64_t canonical_apply_sequence;
    l2flow_instrument_derived_event_row_v1 event;
} l2flow_partial_order_event_envelope_v2;

typedef struct l2flow_partial_order_event_order_state_v2 {
    uint64_t canonical_apply_sequence;
    l2flow_instrument_derived_event_row_v1 order_revision;
} l2flow_partial_order_event_order_state_v2;

typedef struct l2flow_partial_order_event_order_key_v2 {
    uint32_t market;
    uint32_t instrument_id;
    int64_t channel;
    int64_t order_id;
} l2flow_partial_order_event_order_key_v2;

typedef struct l2flow_partial_order_event_channel_health_v2 {
    uint64_t commit_sequence;
    int64_t channel;
    int64_t expected_native_sequence;
    int64_t contiguous_native_sequence;
    int64_t highest_observed_native_sequence;
    int64_t oldest_missing_native_sequence;
    uint64_t pending_count;
    uint64_t oldest_gap_age_ns;
    uint32_t market;
    uint32_t flags;
    uint32_t service_state;
    uint32_t last_error;
    uint8_t reserved[48];
} l2flow_partial_order_event_channel_health_v2;

typedef struct l2flow_partial_order_event_read_batch_result_v2 {
    uint32_t result_schema_version;
    uint32_t result_bytes;
    uint64_t records_written;
    l2flow_partial_order_event_checkpoint_v2 checkpoint;
    l2flow_partial_order_event_status_v2 status;
    uint8_t reserved[32];
} l2flow_partial_order_event_read_batch_result_v2;

typedef struct l2flow_partial_order_event_channel_batch_result_v2 {
    uint32_t result_schema_version;
    uint32_t result_bytes;
    uint64_t records_written;
    uint64_t required_capacity;
    l2flow_partial_order_event_status_v2 status;
    uint8_t reserved[32];
} l2flow_partial_order_event_channel_batch_result_v2;

typedef struct l2flow_partial_order_event_order_state_batch_result_v2 {
    uint32_t result_schema_version;
    uint32_t result_bytes;
    uint64_t records_written;
    l2flow_partial_order_event_checkpoint_v2 checkpoint;
    l2flow_partial_order_event_status_v2 status;
    uint8_t reserved[32];
} l2flow_partial_order_event_order_state_batch_result_v2;

// checkpoint is optional. When supplied, every identity field must match the
// descriptor returned by the broker exactly. The reader pins that mapping and
// publication/correction identity, while status and rows inside the mapping
// remain live. Open and every read return FULL_REPLACEMENT_REQUIRED once the
// lifecycle page names a newer generation or correction; cursors never move
// implicitly. Same-identity stale/restarting/failure states remain readable.
L2FLOW_PARTIAL_ORDER_EVENT_API_V2 int
l2flow_partial_order_event_reader_open_v2(
    const char* absolute_control_socket_path,
    const l2flow_partial_order_event_expected_session_v2* expected_session,
    const l2flow_partial_order_event_checkpoint_v2* checkpoint,
    uint32_t timeout_ms,
    l2flow_partial_order_event_reader_v2** output,
    int* system_error_number);

L2FLOW_PARTIAL_ORDER_EVENT_API_V2 void
l2flow_partial_order_event_reader_close_v2(
    l2flow_partial_order_event_reader_v2* reader);

L2FLOW_PARTIAL_ORDER_EVENT_API_V2 int
l2flow_partial_order_event_reader_session_v2(
    const l2flow_partial_order_event_reader_v2* reader,
    l2flow_partial_order_event_session_v2* output);

L2FLOW_PARTIAL_ORDER_EVENT_API_V2 int
l2flow_partial_order_event_reader_status_v2(
    const l2flow_partial_order_event_reader_v2* reader,
    l2flow_partial_order_event_status_v2* output);

L2FLOW_PARTIAL_ORDER_EVENT_API_V2 int
l2flow_partial_order_event_reader_checkpoint_v2(
    const l2flow_partial_order_event_reader_v2* reader,
    uint64_t next_event_sequence,
    uint64_t next_order_state_physical_slot,
    l2flow_partial_order_event_checkpoint_v2* output);

L2FLOW_PARTIAL_ORDER_EVENT_API_V2 int
l2flow_partial_order_event_reader_read_v2(
    const l2flow_partial_order_event_reader_v2* reader,
    const l2flow_partial_order_event_checkpoint_v2* checkpoint,
    l2flow_partial_order_event_envelope_v2* output,
    size_t capacity,
    l2flow_partial_order_event_read_batch_result_v2* result);

L2FLOW_PARTIAL_ORDER_EVENT_API_V2 int
l2flow_partial_order_event_reader_affected_channels_v2(
    const l2flow_partial_order_event_reader_v2* reader,
    l2flow_partial_order_event_channel_health_v2* output,
    size_t capacity,
    l2flow_partial_order_event_channel_batch_result_v2* result);

L2FLOW_PARTIAL_ORDER_EVENT_API_V2 int
l2flow_partial_order_event_reader_find_order_state_v2(
    const l2flow_partial_order_event_reader_v2* reader,
    const l2flow_partial_order_event_order_key_v2* key,
    l2flow_partial_order_event_order_state_v2* output,
    l2flow_partial_order_event_status_v2* status);

L2FLOW_PARTIAL_ORDER_EVENT_API_V2 int
l2flow_partial_order_event_reader_order_states_v2(
    const l2flow_partial_order_event_reader_v2* reader,
    const l2flow_partial_order_event_checkpoint_v2* checkpoint,
    l2flow_partial_order_event_order_state_v2* output,
    size_t capacity,
    l2flow_partial_order_event_order_state_batch_result_v2* result);

L2FLOW_PARTIAL_ORDER_EVENT_API_V2 const char*
l2flow_partial_order_event_open_error_name_v2(int error);

L2FLOW_PARTIAL_ORDER_EVENT_API_V2 const char*
l2flow_partial_order_event_read_result_name_v2(int result);

#ifdef __cplusplus
}
#endif

#undef L2FLOW_PARTIAL_ORDER_EVENT_API_V2
