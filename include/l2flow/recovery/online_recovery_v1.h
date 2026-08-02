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

// One coherent CERTIFIED handoff-pressure observation.  Production obtains
// both fields from one service Snapshot so recovery does not perform two
// independent status scans per publication.  Tests and legacy callers may
// continue supplying the two callbacks below instead.
struct OnlineRecoveryCertifiedPressureV1 final {
    bool healthy = true;
    std::uint32_t utilization_percent = 0U;
};

enum class OnlineRecoveryPhaseV1 : std::uint8_t {
    kCreated = 0U,
    kCsvReplay,
    kCandidateCatchUp,
    kCandidateReady,
    kPromotionFrozen,
    kPromoted,
    kCleanShutdownTail,
    kEnded,
    kFailed,
};

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

    // Optional mutex-free state sampler for the already-exposed LIVE_PARTIAL
    // owner.  Recovery pauses before beginning another expensive record when
    // accepted callback admission reaches the configured outstanding high
    // water, then drains through the low water.  A terminal/unhealthy sample
    // aborts the handoff rather than outliving the sole live SDK owner.
    std::function<l2flow::runtime::RealtimePipelineLiveStatusV1()>
        preview_live_status;

    // Preferred coherent CERTIFIED pressure sampler.  When present it takes
    // precedence over the two legacy callbacks below.
    std::function<OnlineRecoveryCertifiedPressureV1()>
        certified_pressure_sample;

    // Optional current CERTIFIED handoff queue utilization in [0,100].  Bulk
    // CSV/journal recovery is cooperatively slowed above low/high watermarks
    // and paused at pause_watermark.  The permanent post-promotion journal
    // tail skips low/high cooldowns but still waits at pause_watermark,
    // preventing a lossy CERTIFIED queue.
    std::function<std::uint32_t()> certified_queue_utilization_percent;
    // Optional terminal-health probe paired with the utilization sampler.
    // Returning false aborts recovery immediately: a frozen worker, dropped
    // handoff, resource exhaustion, or conflicting duplicate cannot be cured
    // by merely pausing CSV replay and must never reach promotion.
    std::function<bool()> certified_handoff_healthy;
    std::uint32_t certified_low_watermark_percent = 50U;
    std::uint32_t certified_high_watermark_percent = 75U;
    std::uint32_t certified_pause_watermark_percent = 90U;

    // Work-conserving recovery governor.  With no preview, shadow, or
    // CERTIFIED pressure it never sleeps.  Under ordinary low/high CERTIFIED
    // pressure it applies at most one cooldown per bulk quantum rather than
    // one sleep per record.  The permanent post-promotion journal tail is not
    // subject to either cooldown.
    std::size_t governor_quantum_records = 64U;
    std::uint64_t preview_outstanding_low_water_records = 0U;
    std::uint64_t preview_outstanding_high_water_records = 64U;
    std::uint64_t shadow_outstanding_low_water_records = 256U;
    std::uint64_t shadow_outstanding_high_water_records = 1'024U;
    std::chrono::nanoseconds pressure_poll_interval =
        std::chrono::microseconds(50);
    std::chrono::nanoseconds certified_low_pressure_cooldown =
        std::chrono::microseconds(50);
    std::chrono::nanoseconds certified_high_pressure_cooldown =
        std::chrono::microseconds(500);

    // Optional lock-free health probe for application control-plane owners
    // outside the two Pipelines (the preview and recovered FAST services in
    // production). A false result is terminal and is sampled before every
    // pressure wait. It is appended so existing aggregate member offsets are
    // unchanged.
    std::function<bool()> control_planes_healthy;
};

struct OnlineRecoverySnapshotV1 final {
    OnlineRecoveryErrorV1 error = OnlineRecoveryErrorV1::kNone;
    std::uint64_t csv_publications = 0U;
    std::uint64_t csv_filtered_publications = 0U;
    std::uint64_t journal_records_read = 0U;
    std::uint64_t journal_duplicates_suppressed = 0U;
    // A digest is required only when a journal identity actually intersects
    // retained CSV overlap state.  Provably-new suffix records skip this
    // extra full decode and are still decoded by the shadow pipeline.
    std::uint64_t journal_overlap_digests = 0U;
    std::uint64_t journal_live_suffix_digests_skipped = 0U;
    std::uint64_t journal_suffix_publications = 0U;
    std::uint64_t journal_filtered_publications = 0U;
    std::uint64_t last_journal_serial = 0U;
    std::uint64_t promotion_journal_frontier = 0U;
    std::uint64_t promotion_shadow_ingress_frontier = 0U;
    std::uint64_t replay_throttle_events = 0U;
    std::uint64_t replay_pause_events = 0U;
    // Number of bounded CSV parser/candidate checkpoints admitted to the
    // online handoff gate, including a final checkpoint that aborts.
    std::uint64_t csv_parser_checkpoint_events = 0U;
    std::uint64_t preview_pause_events = 0U;
    std::uint64_t shadow_pause_events = 0U;
    std::uint64_t cooperative_yield_events = 0U;
    std::uint64_t promotion_realtime_ns = 0U;
    bool csv_complete = false;
    bool promotion_boundary_ready = false;
    bool promoted = false;
    // A candidate is the exact journal/shadow prefix currently admitted to the
    // hidden recovery owners.  It may advance before promotion and is distinct
    // from the immutable promotion_* fields above, which are written only by
    // FreezePromotionBoundary().
    std::uint64_t candidate_journal_frontier = 0U;
    std::uint64_t candidate_shadow_ingress_frontier = 0U;
    OnlineRecoveryPhaseV1 phase = OnlineRecoveryPhaseV1::kCreated;
    bool candidate_boundary_ready = false;
};

struct OnlineRecoveryCandidateV1 final {
    OnlineRecoveryErrorV1 error = OnlineRecoveryErrorV1::kNone;
    std::uint64_t journal_frontier = 0U;
    std::uint64_t shadow_ingress_frontier = 0U;
    std::chrono::steady_clock::time_point warmup_deadline{};
    bool candidate_ready = false;
    std::string detail;

    [[nodiscard]] bool ready() const noexcept {
        return error == OnlineRecoveryErrorV1::kNone && candidate_ready;
    }
};

enum class OnlineRecoveryCandidateAdvanceDispositionV1 : std::uint8_t {
    kAdvanced = 0U,
    kIdle,
    kFailed,
};

struct OnlineRecoveryCandidateAdvanceV1 final {
    OnlineRecoveryCandidateAdvanceDispositionV1 disposition =
        OnlineRecoveryCandidateAdvanceDispositionV1::kFailed;
    OnlineRecoveryErrorV1 error = OnlineRecoveryErrorV1::kNone;
    OnlineRecoveryCandidateV1 candidate{};
    std::string detail;
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
// one recovery thread prepares and may advance a candidate, freezes it at
// promotion, then keeps calling PumpNext() for the remainder of the session.
// RecoverToPromotionBoundary() preserves the original one-round entry point.
// The SDK callback only appends to MdlLiveJournalV1 and never enters this
// object.
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

    // Replays CSV and establishes the first finite candidate prefix.  The
    // effective warmup deadline is fixed exactly once as the earlier of the
    // caller deadline and now + config.warmup_timeout, then returned so the
    // application can use the same budget for its cut and prefix probe.
    [[nodiscard]] OnlineRecoveryCandidateV1 PrepareInitialCandidate(
        std::chrono::steady_clock::time_point absolute_deadline) noexcept;

    // Before promotion, consumes at most the next durable global journal
    // serial and returns the updated candidate.  Timeout/idle leaves the prior
    // candidate intact.  Every wait and shadow admission is bounded by the
    // fixed warmup deadline established above.
    [[nodiscard]] OnlineRecoveryCandidateAdvanceV1
    CatchUpOneBeforePromotion(
        std::chrono::steady_clock::time_point operation_deadline) noexcept;

    // Freezes the current candidate as the one immutable promotion boundary.
    // MarkPromoted() and post-promotion PumpNext() remain unavailable until
    // this succeeds.
    [[nodiscard]] OnlineRecoveryBoundaryV1
    FreezePromotionBoundary() noexcept;

    // Replays the fixed CSV cuts, snapshots accepted journal frontier B, and
    // consumes exactly through B.  It intentionally does not cut/publish a
    // generation: the application performs that barrier and activates FAST
    // and CERTIFIED before allowing this consumer to continue beyond B.  This
    // compatibility wrapper prepares one candidate and immediately freezes it;
    // multi-round production recovery uses the three methods above instead.
    [[nodiscard]] OnlineRecoveryBoundaryV1
    RecoverToPromotionBoundary() noexcept;

    // Continues the permanent cutoff guard and shadow suffix after promotion.
    [[nodiscard]] OnlineRecoveryPumpResultV1 PumpNext(
        std::chrono::steady_clock::time_point deadline) noexcept;

    // Records the instant at which generation publication, recovered FAST
    // activation and the CERTIFIED prefix barrier have all succeeded.
    [[nodiscard]] bool MarkPromoted(
        std::uint64_t promotion_realtime_ns) noexcept;

    // Begins the one-way clean-shutdown tail drain after promotion.  The
    // application calls this before quiescing the sole SDK/preview owner and
    // then closes the journal.  PumpNext() continues consuming every durable
    // suffix record through journal End until the absolute deadline; only the
    // preview's expected non-accepting/stopped lifecycle state is relaxed.
    // Fatal, invalid, trade-date-boundary, and drain-timeout states still fail
    // closed.  The deadline prevents shutdown from waiting forever on a
    // permanently saturated downstream owner.
    [[nodiscard]] bool BeginCleanShutdownTailDrain(
        std::chrono::steady_clock::time_point deadline) noexcept;

    [[nodiscard]] OnlineRecoverySnapshotV1 Snapshot() const noexcept;

    [[nodiscard]] bool CaptureTupleFence(
        const l2flow::sdk::MessageKey& key,
        std::string* detail) noexcept override;
    [[nodiscard]] bool CooperativeCheckpoint(
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
