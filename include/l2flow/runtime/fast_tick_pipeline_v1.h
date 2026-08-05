#pragma once

#include "l2flow/common/identity128.h"
#include "l2flow/market/daily_instrument_catalog_v2.h"
#include "l2flow/market/market_decoder.h"
#include "l2flow/realtime/owned_ingress_message_v1.h"
#include "l2flow/runtime/realtime_planes_v1.h"
#include "l2flow/sdk/sdk_runtime.h"

#include "mdl_api.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>

namespace l2flow::runtime {

struct FastTickPipelineSdkConfigV1 final {
    // Disabled means deterministic injection-only operation.
    bool enabled = false;
    std::filesystem::path library_path;
    // Exactly one SDK work thread preserves the serialized callback contract.
    int work_threads = 1;
    int io_threads = 1;
    std::string log_prefix = "l2flow-fast";
    bool log_to_console = false;
    std::string server_address;
    std::string user_name;
    std::uint32_t heartbeat_interval_seconds = 10U;
    std::uint32_t heartbeat_timeout_seconds = 30U;
};

struct FastTickPipelineConfigV1 final {
    l2flow::common::Identity128 run_id{};
    std::uint32_t trade_date = 0U;
    std::shared_ptr<const l2flow::market::DailyInstrumentCatalogV2>
        daily_catalog;
    std::array<std::uint32_t,
               l2flow::realtime::kOwnedIngressSourceCountV1>
        source_stream_ids{};
    l2flow::market::MarketDecoderLimitsV1 decoder_limits{};
    std::uint32_t maximum_sdk_message_bytes =
        l2flow::realtime::kOwnedIngressMaximumMessageBytesV1;
    // One raw SPSC queue and one owned-message pool exist for every
    // source-by-Tick-worker shard. The pool capacity is queue capacity + one
    // message currently being decoded + one callback candidate, so a full
    // queue is reported as such instead of being masked by pool exhaustion.
    std::size_t raw_tick_queue_capacity_per_source_worker = 4096U;
    std::size_t raw_tick_batch_budget = 256U;
    std::uint32_t prewarm_message_bytes = 4096U;
    std::size_t prewarm_message_count_per_source_worker = 512U;
    bool enforce_receive_trade_date = false;
    RealtimePlanesConfigV1 planes{};
    FastTickPipelineSdkConfigV1 sdk{};
};

enum class FastTickPipelineCreateErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidConfiguration,
    kPlaneCreateFailed,
    kIngressPoolCreateFailed,
    kTickWorkerThreadStartFailed,
    kTickWorkerAffinityFailed,
    kSdkLoadFailed,
    kSdkManagerCreateFailed,
    kSdkSubscriberCreateFailed,
    kSdkConfigurationFailed,
    kSdkConnectFailed,
    kSdkCallbackFailed,
    kResourceExhausted,
};

enum class FastTickPipelineIngressErrorV1 : std::uint8_t {
    kNone = 0U,
    kIgnoredUnsupported,
    kFilteredNonAShare,
    kForbiddenCombinedTick,
    kNullMessage,
    kClockFailure,
    kTradeDateBoundary,
    kConcurrentCallback,
    kSequenceExhausted,
    kOwnedMessageRejected,
    kInstrumentKeyRejected,
    kCatalogMiss,
    kRawTickQueueFull,
    kStopped,
    kFatal,
};

[[nodiscard]] std::string_view FastTickPipelineCreateErrorNameV1(
    FastTickPipelineCreateErrorV1 error) noexcept;
[[nodiscard]] std::string_view FastTickPipelineIngressErrorNameV1(
    FastTickPipelineIngressErrorV1 error) noexcept;

struct FastTickPipelineIngressResultV1 final {
    FastTickPipelineIngressErrorV1 error =
        FastTickPipelineIngressErrorV1::kNone;
    l2flow::realtime::OwnedIngressMessageErrorV1 owned_error =
        l2flow::realtime::OwnedIngressMessageErrorV1::kNone;
    std::uint64_t arrival_id = 0U;
    std::uint64_t source_sequence = 0U;
    std::uint32_t tick_worker = 0U;
    std::uint8_t source_slot = 0U;

    [[nodiscard]] bool accepted() const noexcept {
        return error == FastTickPipelineIngressErrorV1::kNone &&
               arrival_id != 0U && source_sequence != 0U;
    }
};

struct FastTickPipelineSnapshotV1 final {
    std::uint64_t accepted_messages = 0U;
    std::uint64_t ignored_messages = 0U;
    std::uint64_t filtered_messages = 0U;
    std::uint64_t rejected_messages = 0U;
    std::uint64_t decoded_messages = 0U;
    std::uint64_t decode_failures = 0U;
    std::uint64_t raw_tick_queue_failures = 0U;
    std::array<std::uint64_t,
               l2flow::realtime::kOwnedIngressSourceCountV1>
        accepted_by_source{};
    bool accepting = false;
    bool fatal = false;
    bool stopped = false;
    RealtimePlanesSnapshotV1 planes{};
};

// Production owner for the sole three-message subscription. The serialized
// SDK callback extracts and catalog-resolves the exact instrument key, then
// copies into raw source-by-Tick-worker SPSC storage. Every Tick worker owns
// one Shanghai and one Shenzhen decoder, performs the only complete decode,
// publishes FAST, then nonblockingly fans out compact derived envelopes.
class FastTickPipelineV1 final {
public:
    FastTickPipelineV1(const FastTickPipelineV1&) = delete;
    FastTickPipelineV1& operator=(const FastTickPipelineV1&) = delete;
    FastTickPipelineV1(FastTickPipelineV1&&) = delete;
    FastTickPipelineV1& operator=(FastTickPipelineV1&&) = delete;
    ~FastTickPipelineV1();

    [[nodiscard]] static FastTickPipelineCreateErrorV1 Create(
        FastTickPipelineConfigV1 config,
        std::unique_ptr<FastTickPipelineV1>* output,
        std::string* detail = nullptr,
        std::shared_ptr<l2flow::sdk::SdkFactory> sdk_factory_for_test =
            nullptr) noexcept;

    // Injection seam uses caller-supplied clock observations and the exact
    // same nonblocking admission path as the serialized SDK callback.
    [[nodiscard]] FastTickPipelineIngressResultV1 IngestForTest(
        const datayes::mdl::MDLMessage* message,
        std::uint64_t recv_realtime_ns,
        std::uint64_t recv_monotonic_ns) noexcept;

    [[nodiscard]] const RealtimePlanesV1& planes() const noexcept;
    [[nodiscard]] RealtimePlanesV1& planes() noexcept;
    [[nodiscard]] FastTickPipelineSnapshotV1 Snapshot() const noexcept;
    [[nodiscard]] const FastTickPipelineConfigV1& config()
        const noexcept;

    void StopAndDrain() noexcept;

private:
    class Impl;
    explicit FastTickPipelineV1(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::runtime
