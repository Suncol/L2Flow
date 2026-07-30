#pragma once

#include "l2flow/common/identity128.h"
#include "l2flow/factor/realtime_factor_engine_v1.h"
#include "l2flow/ipc/realtime_store_generation_sink_v2.h"
#include "l2flow/market/daily_instrument_catalog_v2.h"
#include "l2flow/market/market_decoder.h"
#include "l2flow/market/instrument_runtime_state_v2.h"
#include "l2flow/market/realtime_history_v1.h"
#include "l2flow/realtime/contiguous_sequence_tracker_v2.h"
#include "l2flow/realtime/owned_ingress_message_v1.h"
#include "l2flow/realtime/processing_progress_v2.h"
#include "l2flow/sdk/sdk_runtime.h"

#include "mdl_api.h"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <span>
#include <string>
#include <string_view>

namespace l2flow::runtime {

struct RealtimePipelineSdkConfigV1 final {
    // Disabled is an explicit injection-only mode for deterministic tests.
    // Production enables this and supplies library_path.
    bool enabled = false;
    std::filesystem::path library_path;
    int work_threads = 1;
    // V1 production capture requires exactly one SDK I/O thread and creates
    // the Subscriber with multithread_callback=false. This preserves the
    // already-merged callback order required by the Shenzhen 6.33/6.36
    // channel ApplSeqNum contract; the downstream projector additionally
    // fail-closes on a non-increasing sequence.
    int io_threads = 1;
    std::string log_prefix = "l2flow-realtime";
    bool log_to_console = false;
    std::string server_address;
    // The vendor calls this user name; current deployments place their
    // runtime credential token in this field, matching the narrow SDK API.
    std::string user_name;
    std::uint32_t heartbeat_interval_seconds = 10U;
    std::uint32_t heartbeat_timeout_seconds = 30U;
    datayes::mdl::MDLMessageEncoding message_encoding =
        datayes::mdl::MDLEID_BINARY;
    bool merge_message = false;
    bool send_mac_auth = false;
    bool server_select = false;
};

struct RealtimePipelineConfigV1 final {
    l2flow::common::Identity128 run_id{};
    std::uint32_t trade_date = 0U;
    // Frozen before Create and retained for the pipeline lifetime. Callback
    // admission performs its sole exact-key lookup here.
    std::shared_ptr<const l2flow::market::DailyInstrumentCatalogV2>
        daily_catalog;
    // Borrowed dense runtime availability state created from daily_catalog.
    // It is shared with History/Store and must outlive the pipeline.
    l2flow::market::InstrumentRuntimeStateV2* runtime_state = nullptr;
    std::array<std::uint32_t,
               l2flow::market::kRealtimeHistorySourceCountV1>
        source_stream_ids{};

    std::uint32_t maximum_sdk_message_bytes =
        16U * 1024U * 1024U;
    std::size_t decoder_queue_capacity_per_source = 4096U;
    // The decoder gate window D is min(sum(queue capacities)+source_count,
    // completion_tracker_capacity-1, tick_ring_capacity-1). Both backing
    // capacities must therefore be at least two and strictly exceed D.
    std::size_t completion_tracker_capacity = 262'144U;
    std::size_t tick_ring_capacity = 262'144U;
    l2flow::market::MarketDecoderLimitsV1 decoder_limits{};

    std::uint32_t store_worker_count = 1U;
    std::size_t store_queue_capacity_per_source_worker = 4096U;

    // Real SDK production must pin one process to one fixed UTC+08:00 civil
    // trade date. Historical injection tests may disable this explicitly;
    // Create() with a real SDK rejects a disabled guard.
    bool enforce_receive_trade_date = false;

    // Null selects the literal SnapshotLastPriceProjectionV1. Production may
    // supply any calculator implementing the full-generation contract.
    std::shared_ptr<const l2flow::factor::RealtimeFactorCalculatorV1>
        factor_calculator;
    RealtimePipelineSdkConfigV1 sdk{};
    l2flow::market::IntradayInstrumentStoreConfigV1 intraday_store{};
    // Empty windows disable aggregation. Pipeline creation supplies
    // trade_date from the process/server date and derives maximum_bars from
    // the retained store bound when it is zero.
    l2flow::market::KLineAggregatorConfigV1 kline{};
    // Optional required applied-record projection. The application owns the
    // concrete transport; Pipeline only forwards this generic market-layer
    // boundary to History.
    std::shared_ptr<l2flow::market::RealtimeAppliedRecordSinkV1>
        applied_record_sink;
    std::shared_ptr<l2flow::realtime::ProcessingProgressSinkV2>
        processing_progress_sink;
    // Optional required Wire V2 immutable-generation publication. When
    // configured, CutAndPublishGeneration invokes this exact sink after the
    // Store generation is current and healthy, and before Factor calculation.
    // Sink failure is fatal. The sink publishes the applied cut carried by
    // the generation.
    std::shared_ptr<l2flow::ipc::RealtimeStoreGenerationSinkV2>
        store_generation_sink;
    // Explicit test/diagnostic mode.  Disabled by default because the extra
    // clock reads and atomic histogram updates perturb the measured system.
    // When enabled, LatencySnapshot() exposes the SDK-header-to-callback and
    // append-stage distributions defined below.
    bool measure_stage_latency = false;
    // Supported SDK messages are unconditionally admitted only when their
    // source market and exact SecurityID match the centralized Mainland
    // A-share rules. A filtered callback consumes no sequence and enters no
    // owned pool or queue. This invariant is intentionally not configurable.
};

// Returns the finite completion window enforced after each source-local
// decoder pop and before full decode/History submission:
//
//   0 < s - applied_sequence <= returned_capacity
//
// Accepted-applied may be larger because messages waiting in source queues
// have not crossed this gate. The returned D is strictly smaller than both
// configured completion-tracker and tick-ring capacities.
[[nodiscard]] bool RealtimePipelineAppliedWindowCapacityV1(
    const RealtimePipelineConfigV1& config,
    std::size_t* output) noexcept;

struct RealtimeLatencyQuantileV1 final {
    // The estimate is the midpoint of the containing linear histogram bucket.
    // lower/upper are inclusive bounds.  A clipped quantile fell outside the
    // configured histogram range; its estimate is then only a boundary value.
    std::int64_t estimate_ns = 0;
    std::int64_t lower_bound_ns = 0;
    std::int64_t upper_bound_ns = 0;
    bool clipped_below = false;
    bool clipped_above = false;
};

struct RealtimeLatencyDistributionV1 final {
    std::uint64_t samples = 0U;
    std::uint64_t invalid_samples = 0U;
    std::uint64_t below_histogram_range = 0U;
    std::uint64_t above_histogram_range = 0U;
    std::int64_t minimum_ns = 0;
    std::int64_t maximum_ns = 0;
    std::int64_t mean_ns = 0;
    std::int64_t histogram_minimum_ns = 0;
    std::int64_t histogram_maximum_ns = 0;
    std::uint64_t histogram_bucket_width_ns = 0U;
    bool sum_saturated = false;
    RealtimeLatencyQuantileV1 p50{};
    RealtimeLatencyQuantileV1 p90{};
    RealtimeLatencyQuantileV1 p95{};
    RealtimeLatencyQuantileV1 p99{};
    RealtimeLatencyQuantileV1 p999{};
};

struct RealtimePipelineStageLatencySnapshotV1 final {
    bool enabled = false;
    std::uint32_t sdk_local_time_trade_date = 0U;
    std::array<std::uint64_t,
               l2flow::market::kRealtimeHistorySourceCountV1>
        callback_samples_by_source{};
    std::array<std::uint64_t,
               l2flow::market::kRealtimeHistorySourceCountV1>
        append_samples_by_source{};

    // Signed CLOCK_REALTIME observation minus MDLMessageHead::LocalTime after
    // assigning the configured fixed UTC+08 capture trade date.  This contains
    // upstream/feed/network delay and realtime-clock offset; LocalTime itself
    // has only one-millisecond resolution and carries no date.
    RealtimeLatencyDistributionV1 sdk_local_to_callback_success{};
    RealtimeLatencyDistributionV1 sdk_local_to_append_complete{};

    // Same-host CLOCK_MONOTONIC measurements. callback_entry is the first
    // clock observation made by OnMessage (or the injection seam); callback
    // success is the first observation after successful direct source-lane
    // admission. append_complete is the first observation after Store Append
    // returned kNone. inprocess_latest_read is observed only after an
    // allocation-free acquire-read through the live latest model has returned
    // and been verified to expose the exact Store-owned record just appended;
    // it does not wait for or acquire an immutable generation.
    RealtimeLatencyDistributionV1 callback_entry_to_success{};
    RealtimeLatencyDistributionV1 callback_entry_to_append_complete{};
    RealtimeLatencyDistributionV1
        callback_entry_to_inprocess_latest_read{};
    // Starts immediately before the successful-path input/route validation
    // and ends at the first observation after Append returns.  This is a tight
    // upper bound for the store call rather than an isolated function-body
    // measurement.
    RealtimeLatencyDistributionV1 append_call{};
    std::array<RealtimeLatencyDistributionV1,
               l2flow::market::kRealtimeHistorySourceCountV1>
        callback_to_decoder_publish{};
    std::array<RealtimeLatencyDistributionV1,
               l2flow::market::kRealtimeHistorySourceCountV1>
        decoder_queue_dwell{};
    std::array<RealtimeLatencyDistributionV1,
               l2flow::market::kRealtimeHistorySourceCountV1>
        decode_duration{};
    std::array<RealtimeLatencyDistributionV1,
               l2flow::market::kRealtimeHistorySourceCountV1>
        decode_to_history_submit{};
    std::array<RealtimeLatencyDistributionV1,
               l2flow::market::kRealtimeHistorySourceCountV1>
        decode_to_applied{};
    // Store-applied is sampled immediately after Store Append succeeds.
    // IPC-visible is sampled immediately after the configured required
    // applied sink returns success; without such a sink its sample is invalid.
    RealtimeLatencyDistributionV1 callback_to_store_applied{};
    RealtimeLatencyDistributionV1 callback_to_ipc_visible{};
};

enum class RealtimePipelineCreateErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidConfiguration,
    kStoreRuntimeCreateFailed,
    kKLineRuntimeCreateFailed,
    kFactorCreateFailed,
    kProgressThreadStartFailed,
    kDecoderThreadStartFailed,
    kSdkLoadFailed,
    kSdkManagerCreateFailed,
    kSdkSubscriberCreateFailed,
    kSdkConfigurationFailed,
    kSdkConnectFailed,
    kSdkCallbackFailed,
    kResourceExhausted,
    kUnexpectedFailure,
    kLatestReadModelCreateFailed,
};

[[nodiscard]] std::string_view RealtimePipelineCreateErrorNameV1(
    RealtimePipelineCreateErrorV1 error) noexcept;

enum class RealtimePipelineIngressErrorV1 : std::uint8_t {
    kNone = 0U,
    // API/SYS and every tuple outside the five-message production catalog do
    // not acquire ingress sequence numbers and do not enter processing.
    kIgnoredUnsupported,
    kNullMessage,
    kClockFailure,
    kTradeDateBoundary,
    kSequenceExhausted,
    kOwnedMessageRejected,
    kForbiddenCombinedTick,
    kDecoderAdmissionFailed,
    kStopped,
    kFatal,
    // A well-formed supported message whose source-market SecurityID is
    // outside the configured Mainland A-share rules. It consumes no sequence.
    kFilteredNonAShare,
    // A supported body could not yield a structurally valid exact instrument
    // key. This is malformed required data and fails closed; it is never
    // counted as a normal filter decision.
    kInstrumentKeyRejected,
    // Structurally valid A-share identity missing from the declared complete
    // daily catalog. No sequence or pool slot has been committed.
    kCatalogMiss,
};

[[nodiscard]] std::string_view RealtimePipelineIngressErrorNameV1(
    RealtimePipelineIngressErrorV1 error) noexcept;

struct RealtimePipelineIngressResultV1 final {
    RealtimePipelineIngressErrorV1 error =
        RealtimePipelineIngressErrorV1::kNone;
    l2flow::realtime::OwnedIngressMessageErrorV1 owned_error =
        l2flow::realtime::OwnedIngressMessageErrorV1::kNone;
    std::uint64_t global_ingress_sequence = 0U;
    std::uint64_t source_sequence = 0U;
    // Zero for snapshots. Tick, order, and transaction share one dense
    // process-admission sequence.
    std::uint64_t tick_stream_sequence = 0U;
    std::uint8_t source_slot = 0U;
    std::uint32_t vendor_local_time_raw = 0U;

    [[nodiscard]] bool accepted() const noexcept {
        return error == RealtimePipelineIngressErrorV1::kNone &&
               global_ingress_sequence != 0U && source_sequence != 0U;
    }
};

enum class RealtimePipelineCutErrorV1 : std::uint8_t {
    kNone = 0U,
    kInvalidTimeout,
    kStopped,
    kFatal,
    kSequenceExhausted,
    kClockFailure,
    kWatermarkFailed,
    kFenceArrivalFailed,
    kGenerationBeginFailed,
    kFenceAdmissionFailed,
    kFenceSealFailed,
    kGenerationWaitFailed,
    kStoreGenerationPublishFailed,
    kFactorPublishFailed,
    kUnexpectedFailure,
};

[[nodiscard]] std::string_view RealtimePipelineCutErrorNameV1(
    RealtimePipelineCutErrorV1 error) noexcept;

struct RealtimePipelineCutResultV1 final {
    RealtimePipelineCutErrorV1 error =
        RealtimePipelineCutErrorV1::kNone;
    l2flow::market::RealtimeHistoryWatermarkErrorV1 watermark_error =
        l2flow::market::RealtimeHistoryWatermarkErrorV1::kNone;
    l2flow::market::RealtimeHistoryGenerationErrorV1 generation_error =
        l2flow::market::RealtimeHistoryGenerationErrorV1::kNone;
    l2flow::factor::RealtimeFactorPublishResultV1 factor_result{};
    std::shared_ptr<
        const l2flow::market::IntradayInstrumentStoreGenerationV1>
        store_generation;
    std::shared_ptr<const l2flow::factor::RealtimeFactorGenerationV1>
        factor_generation;
    std::shared_ptr<const l2flow::market::RealtimeKLineGenerationV1>
        kline_generation;
    bool kline_enabled = false;

    [[nodiscard]] bool published() const noexcept {
        return error == RealtimePipelineCutErrorV1::kNone &&
               store_generation != nullptr &&
               factor_generation != nullptr &&
               (!kline_enabled || kline_generation != nullptr);
    }
};

struct RealtimeDecoderQueueSnapshotV1 final {
    std::size_t message_capacity = 0U;
    std::size_t message_depth = 0U;
    std::size_t total_depth = 0U;
    std::size_t message_high_water = 0U;
    std::uint64_t full_count = 0U;
};

struct RealtimePipelineSnapshotV1 final {
    // Exact count committed to one source decoder queue. It always
    // matches global_ingress_sequence.
    std::uint64_t accepted_messages = 0U;
    std::uint64_t ignored_messages = 0U;
    // SDK callbacks that crossed the clean terminal admission cut. They are
    // outside the published prefix and are not malformed/rejected data.
    std::uint64_t post_cut_messages = 0U;
    std::uint64_t rejected_messages = 0U;
    std::uint64_t decoded_messages = 0U;
    std::uint64_t global_ingress_sequence = 0U;
    // Latest successfully admitted mixed-tick sequence. Snapshot admission
    // does not advance it.
    std::uint64_t tick_stream_sequence = 0U;
    std::array<std::uint64_t,
               l2flow::market::kRealtimeHistorySourceCountV1>
        source_sequences{};
    std::uint64_t last_started_generation = 0U;
    std::uint64_t last_published_generation = 0U;
    l2flow::market::MarketDecodeErrorV1 last_decode_error =
        l2flow::market::MarketDecodeErrorV1::kNone;
    l2flow::realtime::OwnedIngressMessagePoolSnapshotV1 ingress_pool{};
    l2flow::realtime::ProcessingProgressV2 processing_progress{};
    bool accepting = false;
    bool fatal = false;
    bool stopped = false;
    bool trade_date_boundary_reached = false;
    l2flow::market::IntradayInstrumentStoreSnapshotV1 store{};
    // Well-formed supported callbacks excluded before sequence allocation,
    // owned copy, and queue admission. Appended to preserve the offsets of
    // the pre-existing snapshot fields.
    std::uint64_t filtered_messages = 0U;
    std::array<std::uint64_t,
               l2flow::market::kRealtimeHistorySourceCountV1>
        filtered_messages_by_source{};
    std::array<RealtimeDecoderQueueSnapshotV1,
               l2flow::market::kRealtimeHistorySourceCountV1>
        decoder_queues{};
};

// Owns the single production data chain. The frozen catalog, runtime state and
// calculator backing objects referenced by config must outlive this runtime.
// One physical SDK manager and one physical Subscriber are created when
// sdk.enabled is true.
class RealtimePipelineV1 final {
public:
    RealtimePipelineV1(const RealtimePipelineV1&) = delete;
    RealtimePipelineV1& operator=(const RealtimePipelineV1&) = delete;
    RealtimePipelineV1(RealtimePipelineV1&&) = delete;
    RealtimePipelineV1& operator=(RealtimePipelineV1&&) = delete;
    ~RealtimePipelineV1();

    [[nodiscard]] static RealtimePipelineCreateErrorV1 Create(
        RealtimePipelineConfigV1 config,
        std::unique_ptr<RealtimePipelineV1>* output,
        std::string* detail = nullptr) noexcept;

    // Test seam for lifecycle/counting fakes. The supplied factory is used as
    // the physical factory; no dlopen is performed. sdk.enabled must be true.
    [[nodiscard]] static RealtimePipelineCreateErrorV1 CreateForTest(
        RealtimePipelineConfigV1 config,
        std::shared_ptr<l2flow::sdk::SdkFactory> physical_factory,
        std::unique_ptr<RealtimePipelineV1>* output,
        std::string* detail = nullptr) noexcept;

    // Executes the same serialized admission path as the SDK callback. The
    // vendor message may be destroyed immediately after this method returns.
    [[nodiscard]] RealtimePipelineIngressResultV1 InjectSdkMessageForTest(
        const datayes::mdl::MDLMessage* message) noexcept;

    // Establishes an exclusive ingress prefix, inserts one reserved-slot
    // parked fence into each decoder FIFO, snapshots availability only after
    // every lane is parked and the cut is applied, then seals History and
    // releases all lanes together. timeout is shared by fence arrival and the
    // generation condition wait; it is not an API completion deadline. It cannot
    // preempt setup/allocation or arbitrary user calculator code, and waiting
    // to serialize behind an already-running cut/stop is outside the budget.
    [[nodiscard]] RealtimePipelineCutResultV1 CutAndPublishGeneration(
        std::chrono::nanoseconds timeout) noexcept;

    // Terminal publication path. It first closes callback admission and
    // performs SDK Shutdown so no accepted message can appear after the cut.
    // It then releases the quiesced SDK objects, publishes the exact final
    // accepted ingress prefix, drains decoder/Store workers, and
    // leaves the runtime stopped. This is the production shutdown path when
    // a final complete generation is required. Every normal return is
    // destructive and leaves the runtime stopped, including invalid timeout
    // or publication failure; a failed terminal publication cannot be
    // retried in place. SDK Shutdown, user calculator work, and worker joins
    // are lifecycle operations outside the barrier timeout and must have
    // deployment-enforced execution bounds.
    [[nodiscard]] RealtimePipelineCutResultV1
    StopAndPublishFinalGeneration(
        std::chrono::nanoseconds timeout) noexcept;

    [[nodiscard]] std::shared_ptr<
        const l2flow::market::IntradayInstrumentStoreGenerationV1>
    AcquireLatestStoreGeneration() const noexcept;
    // The returned object owns its exact matching store generation and
    // exposes all non-empty bars since coverage began.
    [[nodiscard]] std::shared_ptr<
        const l2flow::market::RealtimeKLineGenerationV1>
    AcquireLatestKLineGeneration() const noexcept;
    // Live, read-only point access. These records become visible only after
    // Store append, every enabled KLine update, and the history handoff have
    // all succeeded. Latest is the greatest process ingress_sequence for the
    // instrument/category; it is not exchange-event-time ordering.
    [[nodiscard]] l2flow::market::RealtimeLatestQueryErrorV1
    GetLatestSnapshot(
        std::uint32_t instrument_id,
        l2flow::market::RealtimeLatestRecordViewV1* output)
        const noexcept;
    [[nodiscard]] l2flow::market::RealtimeLatestQueryErrorV1
    GetLatestSnapshots(
        std::span<const std::uint32_t> instrument_ids,
        std::span<l2flow::market::RealtimeLatestRecordViewV1> output)
        const noexcept;
    [[nodiscard]] l2flow::market::RealtimeLatestQueryErrorV1
    GetLatestTick(
        std::uint32_t instrument_id,
        l2flow::market::RealtimeLatestRecordViewV1* output)
        const noexcept;
    [[nodiscard]] l2flow::market::RealtimeLatestQueryErrorV1
    GetLatestTicks(
        std::span<const std::uint32_t> instrument_ids,
        std::span<l2flow::market::RealtimeLatestRecordViewV1> output)
        const noexcept;
    // Store N is published before factor N. A consistent consumer must acquire
    // the factor once and obtain its exact matching store through
    // factor->input_store(). The direct store accessor is for store-only
    // diagnostics.
    [[nodiscard]] std::shared_ptr<
        const l2flow::factor::RealtimeFactorGenerationV1>
    AcquireLatestFactorGeneration() const noexcept;
    [[nodiscard]] RealtimePipelineSnapshotV1 Snapshot() const noexcept;
    // Histogram summaries are coherent after StopAndPublishFinalGeneration or
    // StopAndDrain.  A live call is safe but may combine adjacent in-flight
    // observations and is intended only for diagnostics.
    [[nodiscard]] RealtimePipelineStageLatencySnapshotV1 LatencySnapshot()
        const noexcept;
    [[nodiscard]] bool fatal() const noexcept;

    // Idempotent terminal shutdown without creating another generation. SDK
    // callbacks are stopped first, then decoder queues are drained and joined
    // before the Store runtime stops.
    void StopAndDrain() noexcept;

private:
    class Impl;
    [[nodiscard]] static RealtimePipelineCreateErrorV1 CreateImpl(
        RealtimePipelineConfigV1 config,
        std::shared_ptr<l2flow::sdk::SdkFactory> physical_factory,
        bool factory_is_test_override,
        std::unique_ptr<RealtimePipelineV1>* output,
        std::string* detail) noexcept;
    explicit RealtimePipelineV1(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::runtime
