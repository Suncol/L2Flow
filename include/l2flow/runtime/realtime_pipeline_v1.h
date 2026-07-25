#pragma once

#include "l2flow/common/identity128.h"
#include "l2flow/factor/realtime_factor_engine_v1.h"
#include "l2flow/market/market_decoder.h"
#include "l2flow/market/realtime_history_v1.h"
#include "l2flow/realtime/optional_wal_sink_v1.h"
#include "l2flow/realtime/owned_ingress_message_v1.h"
#include "l2flow/sdk/sdk_runtime.h"

#include "mdl_api.h"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>

namespace l2flow::runtime {

struct RealtimePipelineSdkConfigV1 final {
    // Disabled is an explicit injection-only mode for deterministic tests and
    // offline replay. Production enables this and supplies library_path.
    bool enabled = false;
    std::filesystem::path library_path;
    int work_threads = 1;
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
    const l2flow::market::InstrumentRegistryV1* registry = nullptr;
    std::array<std::uint32_t,
               l2flow::market::kRealtimeHistorySourceCountV1>
        source_stream_ids{};

    std::uint32_t maximum_sdk_message_bytes =
        16U * 1024U * 1024U;
    std::size_t decoder_queue_capacity_per_source = 4096U;
    l2flow::market::MarketDecoderLimitsV1 decoder_limits{};

    std::uint32_t store_worker_count = 1U;
    std::size_t store_queue_capacity_per_source_worker = 4096U;

    // Real SDK production must pin one process to one fixed UTC+08:00 civil
    // trade date. Historical injection tests may disable this explicitly;
    // Create() with a real SDK rejects a disabled guard.
    bool enforce_receive_trade_date = false;

    l2flow::realtime::OptionalWalSinkConfigV1 wal{};
    // Null selects the literal SnapshotLastPriceProjectionV1. Production may
    // supply any calculator implementing the full-generation contract.
    std::shared_ptr<const l2flow::factor::RealtimeFactorCalculatorV1>
        factor_calculator;
    RealtimePipelineSdkConfigV1 sdk{};
    l2flow::market::IntradayInstrumentStoreConfigV1 intraday_store{};
};

enum class RealtimePipelineCreateErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidConfiguration,
    kStoreRuntimeCreateFailed,
    kWalCreateFailed,
    kFactorCreateFailed,
    kDecoderThreadStartFailed,
    kSdkLoadFailed,
    kSdkManagerCreateFailed,
    kSdkSubscriberCreateFailed,
    kSdkConfigurationFailed,
    kSdkConnectFailed,
    kSdkCallbackFailed,
    kResourceExhausted,
    kUnexpectedFailure,
};

[[nodiscard]] std::string_view RealtimePipelineCreateErrorNameV1(
    RealtimePipelineCreateErrorV1 error) noexcept;

enum class RealtimePipelineIngressErrorV1 : std::uint8_t {
    kNone = 0U,
    // API/SYS and every tuple outside the five-message production catalog do
    // not acquire ingress sequence numbers and do not enter WAL/store.
    kIgnoredUnsupported,
    kNullMessage,
    kClockFailure,
    kTradeDateBoundary,
    kSequenceExhausted,
    kOwnedMessageRejected,
    kForbiddenCombinedTick,
    kDecoderQueueFull,
    kStopped,
    kFatal,
};

[[nodiscard]] std::string_view RealtimePipelineIngressErrorNameV1(
    RealtimePipelineIngressErrorV1 error) noexcept;

struct RealtimePipelineIngressResultV1 final {
    RealtimePipelineIngressErrorV1 error =
        RealtimePipelineIngressErrorV1::kNone;
    l2flow::realtime::OwnedIngressMessageErrorV1 owned_error =
        l2flow::realtime::OwnedIngressMessageErrorV1::kNone;
    l2flow::realtime::OptionalWalEnqueueResultV1 wal_result =
        l2flow::realtime::OptionalWalEnqueueResultV1::kDisabled;
    std::uint64_t global_ingress_sequence = 0U;
    std::uint64_t source_sequence = 0U;
    std::uint8_t source_slot = 0U;

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
    kGenerationBeginFailed,
    kMarkerAdmissionFailed,
    kGenerationWaitFailed,
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

    [[nodiscard]] bool published() const noexcept {
        return error == RealtimePipelineCutErrorV1::kNone &&
               store_generation != nullptr &&
               factor_generation != nullptr;
    }
};

struct RealtimePipelineSnapshotV1 final {
    std::uint64_t accepted_messages = 0U;
    std::uint64_t ignored_messages = 0U;
    std::uint64_t rejected_messages = 0U;
    std::uint64_t decoded_messages = 0U;
    std::uint64_t global_ingress_sequence = 0U;
    std::array<std::uint64_t,
               l2flow::market::kRealtimeHistorySourceCountV1>
        source_sequences{};
    std::uint64_t last_started_generation = 0U;
    std::uint64_t last_published_generation = 0U;
    l2flow::market::MarketDecodeErrorV1 last_decode_error =
        l2flow::market::MarketDecodeErrorV1::kNone;
    l2flow::realtime::OwnedIngressMessagePoolSnapshotV1 ingress_pool{};
    l2flow::realtime::OptionalWalSnapshotV1 wal{};
    bool accepting = false;
    bool fatal = false;
    bool stopped = false;
    bool trade_date_boundary_reached = false;
    l2flow::market::IntradayInstrumentStoreSnapshotV1 store{};
};

// Owns the single production data chain. The registry and calculator backing
// objects referenced by config must outlive this runtime. One physical SDK
// manager and one physical Subscriber are created when sdk.enabled is true.
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

    // Establishes an exclusive process-owned ingress prefix, admits one
    // marker into each serial decoder queue, waits for every store worker
    // slice, then performs one atomic full-generation factor publication.
    // timeout is a shared wait budget for decoder-marker backpressure and the
    // generation condition wait; it is not an API completion deadline. It cannot
    // preempt setup/allocation or arbitrary user calculator code, and waiting
    // to serialize behind an already-running cut/stop is outside the budget.
    [[nodiscard]] RealtimePipelineCutResultV1 CutAndPublishGeneration(
        std::chrono::nanoseconds timeout) noexcept;

    // Terminal publication path. It first closes callback admission and
    // performs SDK Shutdown so no accepted message can appear after the cut.
    // It then releases the quiesced SDK objects, publishes the exact final
    // accepted ingress prefix, drains the decoder/store/WAL workers, and
    // leaves the runtime stopped. This is the production shutdown path when a final
    // complete generation is required. Every normal return is destructive and
    // leaves the runtime stopped, including invalid timeout or publication
    // failure; a failed terminal publication cannot be retried in place. SDK Shutdown, user calculator work,
    // worker joins, and optional WAL sync are lifecycle operations outside the
    // barrier timeout and must have deployment-enforced execution bounds.
    [[nodiscard]] RealtimePipelineCutResultV1
    StopAndPublishFinalGeneration(
        std::chrono::nanoseconds timeout) noexcept;

    [[nodiscard]] std::shared_ptr<
        const l2flow::market::IntradayInstrumentStoreGenerationV1>
    AcquireLatestStoreGeneration() const noexcept;
    // Store N is published before factor N. A consistent consumer must acquire
    // the factor once and obtain its exact matching store through
    // factor->input_store(). The direct store accessor is for store-only
    // diagnostics.
    [[nodiscard]] std::shared_ptr<
        const l2flow::factor::RealtimeFactorGenerationV1>
    AcquireLatestFactorGeneration() const noexcept;
    [[nodiscard]] RealtimePipelineSnapshotV1 Snapshot() const noexcept;
    [[nodiscard]] bool fatal() const noexcept;

    // Idempotent terminal shutdown without creating another generation. SDK
    // callbacks are stopped first, decoder queues are drained and joined
    // next, then the store runtime and independent optional WAL stop.
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
