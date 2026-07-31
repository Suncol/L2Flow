#pragma once

#include "l2flow/common/sha256.h"
#include "l2flow/market/market_decoder.h"
#include "l2flow/recovery/live_journal_v1.h"
#include "l2flow/recovery/startup_replay_v1.h"
#include "l2flow/runtime/realtime_pipeline_v1.h"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>

namespace l2flow::recovery {

enum class OnlineRecoveryErrorV1 : std::uint8_t {
    kNone = 0U,
    kInvalidConfiguration,
    kReplayFailed,
    kJournalFailed,
    kJournalReadFailed,
    kReplayPublicationInvalid,
    kReplayDuplicate,
    kOverlapMissing,
    kOverlapConflict,
    kShadowAdmissionFailed,
    kCertifiedFailed,
    kBackpressureTimeout,
    kWarmupTimeout,
    kCancelled,
    kResourceExhausted,
    kUnexpectedFailure,
};

[[nodiscard]] std::string_view OnlineRecoveryErrorNameV1(
    OnlineRecoveryErrorV1 error) noexcept;

struct OnlineRecoveryConfigV1 final {
    std::shared_ptr<MdlLiveJournalV1> live_journal;
    std::shared_ptr<StartupReplaySourceV1> csv_replay_source;
    // Borrowed SDK-less pipeline.  The owner must keep it alive until this
    // handoff is destroyed and no Pump operation is running.
    l2flow::runtime::RealtimePipelineV1* shadow_pipeline = nullptr;
    std::uint32_t trade_date = 0U;
    std::array<std::uint32_t,
               l2flow::market::kRealtimeHistorySourceCountV1>
        source_stream_ids{};
    l2flow::market::MarketDecoderLimitsV1 decoder_limits{};
    std::uint32_t maximum_message_bytes = 16U * 1024U * 1024U;
    std::size_t overlap_retention_per_tuple = 262'144U;
    std::chrono::nanoseconds warmup_timeout =
        std::chrono::minutes(30);
    std::chrono::nanoseconds per_record_admission_timeout =
        std::chrono::seconds(30);
    std::function<bool()> cancel_requested;

    // Optional current CERTIFIED handoff queue utilization in [0,100].  CSV
    // replay is cooperatively slowed above low/high watermarks and paused at
    // pause_watermark.  Journal suffix records skip the low-water sleeps but
    // still wait at the pause watermark, preventing a lossy CERTIFIED queue.
    std::function<std::uint32_t()> certified_queue_utilization_percent;
    // Optional terminal-health probe paired with the utilization sampler.
    // Returning false aborts recovery immediately: a frozen worker, dropped
    // handoff, resource exhaustion, or conflicting duplicate cannot be cured
    // by merely pausing CSV replay and must never reach promotion.
    std::function<bool()> certified_handoff_healthy;
    std::uint32_t certified_low_watermark_percent = 50U;
    std::uint32_t certified_high_watermark_percent = 75U;
    std::uint32_t certified_pause_watermark_percent = 90U;
};

struct OnlineRecoverySnapshotV1 final {
    OnlineRecoveryErrorV1 error = OnlineRecoveryErrorV1::kNone;
    std::uint64_t csv_publications = 0U;
    std::uint64_t csv_filtered_publications = 0U;
    std::uint64_t journal_records_read = 0U;
    std::uint64_t journal_duplicates_suppressed = 0U;
    std::uint64_t journal_suffix_publications = 0U;
    std::uint64_t journal_filtered_publications = 0U;
    std::uint64_t last_journal_serial = 0U;
    std::uint64_t promotion_journal_frontier = 0U;
    std::uint64_t promotion_shadow_ingress_frontier = 0U;
    std::uint64_t replay_throttle_events = 0U;
    std::uint64_t replay_pause_events = 0U;
    std::uint64_t promotion_realtime_ns = 0U;
    bool csv_complete = false;
    bool promotion_boundary_ready = false;
    bool promoted = false;
};

struct OnlineRecoveryBoundaryV1 final {
    OnlineRecoveryErrorV1 error = OnlineRecoveryErrorV1::kNone;
    std::uint64_t journal_frontier = 0U;
    std::uint64_t shadow_ingress_frontier = 0U;
    bool boundary_ready = false;
    std::string detail;

    [[nodiscard]] bool ready() const noexcept {
        return error == OnlineRecoveryErrorV1::kNone &&
               boundary_ready;
    }
};

enum class OnlineRecoveryPumpDispositionV1 : std::uint8_t {
    kRecord = 0U,
    kIdle,
    kEnd,
    kFailed,
};

struct OnlineRecoveryPumpResultV1 final {
    OnlineRecoveryPumpDispositionV1 disposition =
        OnlineRecoveryPumpDispositionV1::kFailed;
    OnlineRecoveryErrorV1 error = OnlineRecoveryErrorV1::kNone;
    std::uint64_t journal_serial = 0U;
    std::string detail;
};

// Stateful CSV/journal seam validator.  It is deliberately single-consumer:
// one recovery thread calls RecoverToPromotionBoundary(), then keeps calling
// PumpNext() for the remainder of the session.  The SDK callback only appends
// to MdlLiveJournalV1 and never enters this object.
class OnlineRecoveryHandoffV1 final : public StartupReplaySinkV1 {
public:
    OnlineRecoveryHandoffV1(const OnlineRecoveryHandoffV1&) = delete;
    OnlineRecoveryHandoffV1& operator=(
        const OnlineRecoveryHandoffV1&) = delete;
    OnlineRecoveryHandoffV1(OnlineRecoveryHandoffV1&&) = delete;
    OnlineRecoveryHandoffV1& operator=(
        OnlineRecoveryHandoffV1&&) = delete;
    ~OnlineRecoveryHandoffV1() override;

    [[nodiscard]] static OnlineRecoveryErrorV1 Create(
        OnlineRecoveryConfigV1 config,
        std::unique_ptr<OnlineRecoveryHandoffV1>* output,
        std::string* detail = nullptr) noexcept;

    // Replays the fixed CSV cuts, snapshots accepted journal frontier B, and
    // consumes exactly through B.  It intentionally does not cut/publish a
    // generation: the application performs that barrier and activates FAST
    // and CERTIFIED before allowing this consumer to continue beyond B.
    [[nodiscard]] OnlineRecoveryBoundaryV1
    RecoverToPromotionBoundary() noexcept;

    // Continues the permanent cutoff guard and shadow suffix after promotion.
    [[nodiscard]] OnlineRecoveryPumpResultV1 PumpNext(
        std::chrono::steady_clock::time_point deadline) noexcept;

    // Records the instant at which generation publication, recovered FAST
    // activation and the CERTIFIED prefix barrier have all succeeded.
    [[nodiscard]] bool MarkPromoted(
        std::uint64_t promotion_realtime_ns) noexcept;

    [[nodiscard]] OnlineRecoverySnapshotV1 Snapshot() const noexcept;

    [[nodiscard]] bool CaptureTupleFence(
        const l2flow::sdk::MessageKey& key,
        std::string* detail) noexcept override;
    [[nodiscard]] bool Publish(
        const StartupReplayPublicationV1& publication,
        std::string* detail) noexcept override;

private:
    class Impl;
    explicit OnlineRecoveryHandoffV1(
        std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::recovery
