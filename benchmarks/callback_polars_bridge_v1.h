#pragma once

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct L2FlowBenchmarkEventRowV1 {
    uint64_t change_sequence;
    uint64_t callback_start_ns;
    uint64_t source_arrival_id;
    int64_t business_sequence;
    int64_t affected_order_id;
    int64_t price_p6;
    int64_t quantity;
    int64_t trade_amount_p6;
    int64_t buy_order_id;
    int64_t sell_order_id;
    int64_t recv_realtime_ns;
    int64_t recv_monotonic_ns;
    uint64_t event_time_ns_since_midnight;
    uint64_t source_quality_flags;
    uint64_t source_market_notices;
    uint64_t event_quality_flags;
    uint32_t instrument_id;
    int32_t channel;
    uint32_t source_event_ordinal;
    uint32_t derived_event_ordinal;
    uint32_t occurrence;
    uint32_t payload_validity;
    uint8_t event_kind;
    uint8_t mutation_kind;
    uint8_t reserved[6];
} L2FlowBenchmarkEventRowV1;

typedef struct L2FlowBenchmarkSnapshotV1 {
    uint64_t scheduled_messages;
    uint64_t attempted_messages;
    uint64_t accepted_messages;
    uint64_t ingress_errors;
    uint64_t raw_tick_queue_full_errors;
    uint64_t owned_message_rejected_errors;
    uint64_t other_ingress_errors;
    uint64_t first_callback_start_ns;
    uint64_t last_callback_end_ns;
    uint64_t producer_end_ns;
    uint64_t accepted_shanghai;
    uint64_t accepted_shenzhen;
    uint64_t decoded_messages;
    uint64_t decode_failures;
    uint64_t rejected_messages;
    uint64_t fast_append_failures;
    uint64_t fast_unrecoverable_drops;
    uint64_t event_queue_failures;
    uint64_t kline_queue_failures;
    uint64_t fast_applied;
    uint64_t event_applied;
    uint64_t kline_applied;
    uint64_t event_rebuild_attempts;
    uint64_t kline_rebuild_attempts;
    uint64_t fast_stable_rows;
    uint64_t event_stable_rows;
    uint32_t event_non_live_instruments;
    uint32_t kline_non_live_instruments;
    uint32_t incomplete_fast_instruments;
    uint32_t producer_done;
    uint32_t producer_running;
    uint32_t pipeline_fatal;
    uint32_t reserved;
} L2FlowBenchmarkSnapshotV1;

enum L2FlowBenchmarkErrorV1 {
    L2FLOW_BENCHMARK_OK_V1 = 0,
    L2FLOW_BENCHMARK_INVALID_ARGUMENT_V1 = 1,
    L2FLOW_BENCHMARK_CREATE_FAILED_V1 = 2,
    L2FLOW_BENCHMARK_ALREADY_RUNNING_V1 = 3,
    L2FLOW_BENCHMARK_THREAD_FAILED_V1 = 4,
    L2FLOW_BENCHMARK_READ_FAILED_V1 = 5,
    L2FLOW_BENCHMARK_CURSOR_MISMATCH_V1 = 6,
    L2FLOW_BENCHMARK_UNSUPPORTED_MUTATION_V1 = 7,
    L2FLOW_BENCHMARK_BUFFER_TOO_SMALL_V1 = 8
};

/*
 * This bridge exists only for a benchmark process. It owns the current
 * injection-only FastTickPipelineV1 and exposes POD rows to ctypes. It is not
 * a production Wire V3 transport.
 */
void* l2flow_benchmark_create_v1(
    uint64_t maximum_messages,
    uint32_t instrument_count,
    uint32_t worker_count,
    uint32_t queue_capacity,
    uint32_t maximum_change_batch,
    char* detail,
    size_t detail_capacity);

int l2flow_benchmark_start_v1(
    void* handle,
    uint64_t target_messages_per_second,
    uint64_t message_count);

int l2flow_benchmark_snapshot_v1(
    const void* handle,
    L2FlowBenchmarkSnapshotV1* output);

uint32_t l2flow_benchmark_instrument_count_v1(const void* handle);
uint32_t l2flow_benchmark_instrument_id_v1(
    const void* handle,
    uint32_t producer_slot);

int l2flow_benchmark_read_event_changes_v1(
    void* handle,
    uint32_t instrument_id,
    uint64_t next_change_sequence,
    L2FlowBenchmarkEventRowV1* output,
    size_t output_capacity,
    size_t* written,
    uint64_t* next_change_sequence_out);

int l2flow_benchmark_event_stable_size_v1(
    const void* handle,
    uint32_t instrument_id,
    uint64_t* row_count,
    uint64_t* included_change_sequence,
    uint32_t* repair_state);

int l2flow_benchmark_copy_event_stable_v1(
    const void* handle,
    uint32_t instrument_id,
    L2FlowBenchmarkEventRowV1* output,
    size_t output_capacity,
    size_t* written,
    uint64_t* included_change_sequence,
    uint32_t* repair_state);

uint64_t l2flow_benchmark_monotonic_now_ns_v1(void);
size_t l2flow_benchmark_event_row_size_v1(void);
size_t l2flow_benchmark_snapshot_size_v1(void);

void l2flow_benchmark_destroy_v1(void* handle);

#ifdef __cplusplus
}
#endif
