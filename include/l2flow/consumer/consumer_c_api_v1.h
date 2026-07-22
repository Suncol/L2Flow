#pragma once

// This header is intentionally C-compatible.  It exposes only fixed-width
// values and opaque borrowed page pointers; the implementation translates to
// the reviewed Phase-5 helpers rather than recreating frontier proof logic.

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

enum l2flow_consumer_c_error_v1 {
    L2FLOW_CONSUMER_C_OK_V1 = 0,
    L2FLOW_CONSUMER_C_NULL_ARGUMENT_V1 = 1,
    L2FLOW_CONSUMER_C_INVALID_ARGUMENT_V1 = 2,
    L2FLOW_CONSUMER_C_UNSUPPORTED_ABI_V1 = 3,
    L2FLOW_CONSUMER_C_EVENT_TYPE_MISMATCH_V1 = 4,
    L2FLOW_CONSUMER_C_RECORD_SIZE_MISMATCH_V1 = 5,
    L2FLOW_CONSUMER_C_SCHEMA_MISMATCH_V1 = 6,
    L2FLOW_CONSUMER_C_DTYPE_MISMATCH_V1 = 7,
    L2FLOW_CONSUMER_C_REGISTRY_MISMATCH_V1 = 8,
    L2FLOW_CONSUMER_C_RESOURCE_EXHAUSTED_V1 = 9,
    L2FLOW_CONSUMER_C_OPEN_FAILED_V1 = 10,
    L2FLOW_CONSUMER_C_BATCH_WOULD_BLOCK_V1 = 11,
    L2FLOW_CONSUMER_C_BATCH_FAILED_V1 = 12,
};

struct l2flow_consumer_attach_identity_v1 {
    uint16_t event_type;
    uint16_t reserved0;
    uint32_t record_size;
    uint8_t schema_sha256[32];
    uint8_t dtype_sha256[32];
    uint64_t registry_version;
    uint8_t registry_sha256[32];
};

// Both arguments are caller-owned fixed identities.  The helper validates the
// canonical fixed size for the event family and then requires exact equality.
int l2flow_consumer_validate_attach_v1(
    const struct l2flow_consumer_attach_identity_v1* expected,
    const struct l2flow_consumer_attach_identity_v1* actual);

struct l2flow_consumer_clock_epoch_v1 {
    uint32_t algorithm;
    uint8_t digest[32];
    uint64_t label;
};

struct l2flow_consumer_event_key_v1 {
    int64_t recv_monotonic_ns;
    uint32_t source_stream_id;
    uint64_t origin_ingress_sequence;
    uint8_t sub_index;
};

struct l2flow_consumer_mux_input_v1 {
    uint8_t required;
    uint8_t has_next;
    uint16_t reserved0;
    struct l2flow_consumer_event_key_v1 next_key;
    uint64_t next_origin_wal_end_pos;
    struct l2flow_consumer_clock_epoch_v1 next_clock_epoch;
    uint32_t next_capture_date;
    uint8_t next_stream_day_id[16];
    uint8_t next_writer_instance[16];
    uint64_t next_generation;
    // Borrowed pointer to one live SourceFrontierPageV1.  Required inputs must
    // provide it.  The pointed page must remain alive for the call.
    const void* frontier_page;
};

struct l2flow_consumer_mux_selection_v1 {
    uint8_t decision;
    uint8_t reserved0[7];
    size_t input_index;
    struct l2flow_consumer_event_key_v1 key;
    struct l2flow_consumer_clock_epoch_v1 clock_epoch;
};

int l2flow_consumer_safe_mux_select_v1(
    const struct l2flow_consumer_mux_input_v1* inputs,
    size_t input_count,
    struct l2flow_consumer_mux_selection_v1* output);

struct l2flow_consumer_snapshot_asof_proof_v1 {
    uint8_t proof;
};

int l2flow_consumer_snapshot_asof_prove_v1(
    const struct l2flow_consumer_event_key_v1* tick,
    const struct l2flow_consumer_clock_epoch_v1* tick_clock_epoch,
    const struct l2flow_consumer_mux_input_v1* snapshot_input,
    struct l2flow_consumer_snapshot_asof_proof_v1* output);

struct l2flow_consumer_snapshot_asof_selection_v1 {
    uint8_t found;
    uint8_t reserved0[7];
    size_t index;
};

int l2flow_consumer_snapshot_asof_select_v1(
    const struct l2flow_consumer_event_key_v1* consumed_snapshots,
    size_t snapshot_count,
    const struct l2flow_consumer_event_key_v1* tick,
    struct l2flow_consumer_snapshot_asof_selection_v1* output);

struct l2flow_consumer_segment_descriptor_v1 {
    uint16_t event_type;
    uint16_t reserved0;
    uint32_t record_size;
    uint32_t source_stream_id;
    uint32_t shard_id;
    uint32_t trade_date;
    uint32_t origin_capture_date;
    uint8_t origin_stream_day_id[16];
    uint8_t origin_source_writer_instance[16];
    uint64_t origin_source_generation;
    struct l2flow_consumer_clock_epoch_v1 clock_epoch;
    uint8_t schema_sha256[32];
    uint8_t dtype_sha256[32];
    uint64_t registry_version;
    uint8_t registry_sha256[32];
    uint8_t normalizer_build_sha256[32];
    uint8_t normalizer_config_sha256[32];
    uint64_t canonical_generation;
    uint64_t segment_sequence;
    uint64_t capacity_records;
};

struct l2flow_consumer_raw_control_snapshot_v1 {
    uint8_t writer_instance[16];
    uint8_t stream_day_id[16];
    uint32_t source_stream_id;
    uint32_t capture_date;
    uint32_t segment_sequence;
    uint32_t fatal_state;
    uint64_t append_global_wal_pos;
    uint64_t append_ingress_sequence;
    uint64_t append_segment_offset;
    uint64_t durable_global_wal_pos;
    uint64_t durable_ingress_sequence;
    uint64_t durable_segment_offset;
    uint64_t clock_epoch_label;
    uint64_t heartbeat_monotonic_ns;
};

// Returns nonzero on a coherent observation and zero on failure.
typedef int (*l2flow_consumer_raw_durability_observer_v1)(
    void* context,
    struct l2flow_consumer_raw_control_snapshot_v1* snapshot);

struct l2flow_consumer_batch_open_config_v1 {
    const char* segment_path;
    struct l2flow_consumer_segment_descriptor_v1 expected_segment;
    struct l2flow_consumer_attach_identity_v1 expected_attach;
    const void* source_frontier_page;
    uint64_t initial_canonical_cursor;
    l2flow_consumer_raw_durability_observer_v1 raw_durability_observer;
    void* raw_durability_observer_context;
};

struct l2flow_consumer_batch_metadata_v1 {
    uint32_t source_stream_id;
    uint32_t origin_capture_date;
    uint32_t trade_date;
    uint8_t origin_stream_day_id[16];
    uint16_t family;
    uint16_t reserved0;
    uint32_t shard_id;
    uint64_t begin_canonical_cursor;
    uint64_t end_canonical_cursor;
    uint64_t max_consumed_origin_wal_end_pos;
    uint64_t observed_raw_durable_wal_pos;
    struct l2flow_consumer_clock_epoch_v1 clock_epoch;
    uint8_t schema_sha256[32];
    uint8_t dtype_sha256[32];
    uint64_t registry_version;
    uint8_t registry_sha256[32];
    uint64_t batch_quality_flags;
    uint64_t watermark_set_id;
    uint8_t origin_source_writer_instance[16];
    uint64_t origin_source_generation;
    uint64_t canonical_generation;
};

struct l2flow_consumer_batch_reader_handle_v1;
struct l2flow_consumer_batch_view_handle_v1;

int l2flow_consumer_batch_open_v1(
    const struct l2flow_consumer_batch_open_config_v1* config,
    struct l2flow_consumer_batch_reader_handle_v1** output);

int l2flow_consumer_batch_peek_v1(
    struct l2flow_consumer_batch_reader_handle_v1* reader,
    size_t maximum_records,
    uint64_t watermark_set_id,
    struct l2flow_consumer_batch_view_handle_v1** output);

int l2flow_consumer_batch_view_records_v1(
    const struct l2flow_consumer_batch_view_handle_v1* view,
    const uint8_t** records,
    size_t* record_count,
    uint32_t* record_size);

int l2flow_consumer_batch_view_metadata_v1(
    const struct l2flow_consumer_batch_view_handle_v1* view,
    struct l2flow_consumer_batch_metadata_v1* output);

int l2flow_consumer_batch_commit_v1(
    struct l2flow_consumer_batch_reader_handle_v1* reader,
    const struct l2flow_consumer_batch_view_handle_v1* view);

uint64_t l2flow_consumer_batch_cursor_v1(
    const struct l2flow_consumer_batch_reader_handle_v1* reader);

void l2flow_consumer_batch_view_release_v1(
    struct l2flow_consumer_batch_view_handle_v1* view);

void l2flow_consumer_batch_reader_release_v1(
    struct l2flow_consumer_batch_reader_handle_v1* reader);

#ifdef __cplusplus
}
#endif
