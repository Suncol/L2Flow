#include "l2flow/recovery/online_recovery_v1.h"

#include "l2flow/market/market_types_v1.h"
#include "l2flow/realtime/owned_ingress_message_v1.h"
#include "l2flow/sdk/vendor_head_view.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <functional>
#include <limits>
#include <mutex>
#include <optional>
#include <queue>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace l2flow::recovery {
namespace {

namespace common = l2flow::common;
namespace market = l2flow::market;
namespace realtime = l2flow::realtime;
namespace runtime = l2flow::runtime;
namespace sdk = l2flow::sdk;

constexpr auto kMaximumTimeout = std::chrono::hours(24);
constexpr auto kMaximumGovernorInterval = std::chrono::seconds(1);
constexpr std::size_t kMaximumGovernorQuantumRecords = 1'048'576U;

[[nodiscard]] std::optional<std::size_t> TupleIndex(
    const sdk::MessageKey& key) noexcept {
    for (std::size_t index = 0U;
         index < sdk::kProductionMessageKeysV1.size();
         ++index) {
        if (sdk::kProductionMessageKeysV1[index] == key) {
            return index;
        }
    }
    return std::nullopt;
}

void SetDetail(std::string* output, std::string_view value) noexcept {
    if (output == nullptr) {
        return;
    }
    try {
        output->assign(value);
    } catch (...) {
    }
}

[[nodiscard]] bool CurrentClocks(
    std::uint64_t* realtime_ns,
    std::uint64_t* monotonic_ns) noexcept {
    if (realtime_ns == nullptr || monotonic_ns == nullptr) {
        return false;
    }
    const auto realtime_count =
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count();
    const auto monotonic_count =
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count();
    if (realtime_count <= 0 || monotonic_count <= 0) {
        return false;
    }
    *realtime_ns = static_cast<std::uint64_t>(realtime_count);
    *monotonic_ns = static_cast<std::uint64_t>(monotonic_count);
    return true;
}

[[nodiscard]] bool ConfigValid(const OnlineRecoveryConfigV1& config) {
    if (config.live_journal == nullptr ||
        config.csv_replay_source == nullptr ||
        config.shadow_pipeline == nullptr || config.trade_date == 0U ||
        config.maximum_message_bytes < sdk::kVendorHeadBytes ||
        config.maximum_message_bytes >
            realtime::kOwnedIngressMaximumMessageBytesV1 ||
        config.overlap_retention_per_tuple == 0U ||
        config.overlap_retention_per_tuple ==
            std::numeric_limits<std::size_t>::max() ||
        config.warmup_timeout <= std::chrono::nanoseconds::zero() ||
        config.warmup_timeout > kMaximumTimeout ||
        config.per_record_admission_timeout <=
            std::chrono::nanoseconds::zero() ||
        config.per_record_admission_timeout > kMaximumTimeout ||
        config.governor_quantum_records == 0U ||
        config.governor_quantum_records >
            kMaximumGovernorQuantumRecords ||
        config.preview_outstanding_low_water_records >=
            config.preview_outstanding_high_water_records ||
        config.shadow_outstanding_low_water_records >=
            config.shadow_outstanding_high_water_records ||
        config.pressure_poll_interval <=
            std::chrono::nanoseconds::zero() ||
        config.pressure_poll_interval > kMaximumGovernorInterval ||
        config.certified_low_pressure_cooldown <
            std::chrono::nanoseconds::zero() ||
        config.certified_low_pressure_cooldown >
            kMaximumGovernorInterval ||
        config.certified_high_pressure_cooldown <
            config.certified_low_pressure_cooldown ||
        config.certified_high_pressure_cooldown >
            kMaximumGovernorInterval ||
        config.certified_low_watermark_percent >=
            config.certified_high_watermark_percent ||
        config.certified_high_watermark_percent >=
            config.certified_pause_watermark_percent ||
        config.certified_pause_watermark_percent > 100U) {
        return false;
    }
    for (std::size_t index = 0U;
         index < config.source_stream_ids.size();
         ++index) {
        if (config.source_stream_ids[index] == 0U) {
            return false;
        }
        for (std::size_t prior = 0U; prior < index; ++prior) {
            if (config.source_stream_ids[prior] ==
                config.source_stream_ids[index]) {
                return false;
            }
        }
    }
    return true;
}

}  // namespace

std::string_view OnlineRecoveryErrorNameV1(
    OnlineRecoveryErrorV1 error) noexcept {
    switch (error) {
        case OnlineRecoveryErrorV1::kNone:
            return "none";
        case OnlineRecoveryErrorV1::kInvalidConfiguration:
            return "invalid_configuration";
        case OnlineRecoveryErrorV1::kReplayFailed:
            return "replay_failed";
        case OnlineRecoveryErrorV1::kJournalFailed:
            return "journal_failed";
        case OnlineRecoveryErrorV1::kJournalReadFailed:
            return "journal_read_failed";
        case OnlineRecoveryErrorV1::kReplayPublicationInvalid:
            return "replay_publication_invalid";
        case OnlineRecoveryErrorV1::kReplayDuplicate:
            return "replay_duplicate";
        case OnlineRecoveryErrorV1::kOverlapMissing:
            return "overlap_missing";
        case OnlineRecoveryErrorV1::kOverlapConflict:
            return "overlap_conflict";
        case OnlineRecoveryErrorV1::kShadowAdmissionFailed:
            return "shadow_admission_failed";
        case OnlineRecoveryErrorV1::kCertifiedFailed:
            return "certified_failed";
        case OnlineRecoveryErrorV1::kBackpressureTimeout:
            return "backpressure_timeout";
        case OnlineRecoveryErrorV1::kWarmupTimeout:
            return "warmup_timeout";
        case OnlineRecoveryErrorV1::kCancelled:
            return "cancelled";
        case OnlineRecoveryErrorV1::kResourceExhausted:
            return "resource_exhausted";
        case OnlineRecoveryErrorV1::kUnexpectedFailure:
            return "unexpected_failure";
    }
    return "unknown";
}

class OnlineRecoveryHandoffV1::Impl final {
public:
    enum class HandoffPhase : std::uint8_t {
        kSeekingOverlap = 0U,
        kOverlap,
        kLiveSuffix,
    };

    enum class ThrottleResult : std::uint8_t {
        kReady = 0U,
        kDeadline,
        kCancelled,
        kPreviewUnhealthy,
        kShadowUnhealthy,
        kCertifiedUnhealthy,
        kJournalUnhealthy,
        kControlPlaneUnhealthy,
        kFailed,
    };

    enum class GovernorPhase : std::uint8_t {
        kCsv = 0U,
        kJournalRecovery,
        kJournalTail,
    };

    enum class PumpPhase : std::uint8_t {
        kCandidateCatchUp = 0U,
        kJournalTail,
    };

    struct CertifiedPressure final {
        bool configured = false;
        bool healthy = true;
        bool valid = true;
        std::uint32_t utilization_percent = 0U;
    };

    struct DrainDecision final {
        bool entered = false;
        bool pause = false;
    };

    [[nodiscard]] static DrainDecision AdvanceDrainState(
        std::uint64_t outstanding,
        std::uint64_t low_water,
        std::uint64_t high_water,
        bool* draining) noexcept {
        DrainDecision result{};
        if (!*draining && outstanding >= high_water) {
            *draining = true;
            result.entered = true;
        }
        if (*draining && outstanding > low_water) {
            result.pause = true;
        } else {
            *draining = false;
        }
        return result;
    }

    struct Fingerprint final {
        std::uint64_t vendor_sequence_id = 0U;
        bool normalize_shenzhen_snapshot_channel = false;
        common::Sha256Digest digest{};
    };

    using RetainedSequenceHeap = std::priority_queue<
        std::uint64_t,
        std::vector<std::uint64_t>,
        std::greater<std::uint64_t>>;

    explicit Impl(OnlineRecoveryConfigV1 config)
        : config_(std::move(config)) {}

    [[nodiscard]] OnlineRecoveryErrorV1 Initialize(
        std::string* detail) noexcept {
        if (!ConfigValid(config_)) {
            SetDetail(detail, "invalid online recovery configuration");
            return OnlineRecoveryErrorV1::kInvalidConfiguration;
        }
        try {
            if (!config_.live_journal->CreateReader(&reader_) ||
                reader_ == nullptr) {
                SetDetail(detail, "live journal reader create failed");
                return OnlineRecoveryErrorV1::kJournalReadFailed;
            }
            for (std::uint8_t source = 0U;
                 source < market::kRealtimeHistorySourceCountV1;
                 ++source) {
                market::MarketDecoderConfigV1 decoder_config{};
                decoder_config.trade_date = config_.trade_date;
                decoder_config.source_stream_id =
                    config_.source_stream_ids[source];
                decoder_config.limits = config_.decoder_limits;
                fingerprint_decoders_[source] =
                    std::make_unique<market::MarketDecoderV1>(
                        decoder_config);
                if (!fingerprint_decoders_[source]
                         ->configuration_valid()) {
                    SetDetail(
                        detail,
                        "online recovery fingerprint decoder is invalid");
                    return OnlineRecoveryErrorV1::kInvalidConfiguration;
                }
            }
            // Handoff creation still precedes LIVE_PARTIAL exposure.  Reserve
            // the bounded overlap tables here so bulk replay cannot trigger
            // a large unordered-map rehash while FAST readers are active.
            const std::size_t retained_capacity =
                config_.overlap_retention_per_tuple + 1U;
            for (std::size_t tuple = 0U;
                 tuple < fingerprints_.size();
                 ++tuple) {
                fingerprints_[tuple].reserve(retained_capacity);
                std::vector<std::uint64_t> heap_storage;
                heap_storage.reserve(retained_capacity);
                retained_sequences_[tuple] = RetainedSequenceHeap(
                    std::greater<std::uint64_t>{},
                    std::move(heap_storage));
            }
            SetDetail(detail, {});
            return OnlineRecoveryErrorV1::kNone;
        } catch (const std::bad_alloc&) {
            SetDetail(detail, "online recovery allocation failed");
            return OnlineRecoveryErrorV1::kResourceExhausted;
        } catch (...) {
            SetDetail(detail, "online recovery initialization failed");
            return OnlineRecoveryErrorV1::kUnexpectedFailure;
        }
    }

    [[nodiscard]] OnlineRecoveryCandidateV1 PrepareInitialCandidate(
        StartupReplaySinkV1& sink,
        std::chrono::steady_clock::time_point absolute_deadline) noexcept {
        try {
            const auto now = std::chrono::steady_clock::now();
            if (absolute_deadline <= now) {
                return CandidateFailure(
                    OnlineRecoveryErrorV1::kWarmupTimeout,
                    "online recovery initial deadline already expired");
            }
            bool initial_state_valid = false;
            {
                std::lock_guard<std::mutex> lock(snapshot_mutex_);
                initial_state_valid =
                    snapshot_.phase == OnlineRecoveryPhaseV1::kCreated &&
                    !warmup_deadline_initialized_;
                if (initial_state_valid) {
                    warmup_deadline_ = std::min(
                        absolute_deadline,
                        now + config_.warmup_timeout);
                    warmup_deadline_initialized_ = true;
                    snapshot_.phase =
                        OnlineRecoveryPhaseV1::kCsvReplay;
                }
            }
            if (!initial_state_valid) {
                return CandidateStateFailure(
                    OnlineRecoveryErrorV1::kInvalidConfiguration,
                    "online recovery initial candidate was already prepared");
            }
            if (CancellationRequested()) {
                return CandidateFailure(
                    OnlineRecoveryErrorV1::kCancelled,
                    "online recovery was cancelled before CSV replay");
            }
            const StartupReplayResultV1 replay =
                config_.csv_replay_source->Replay(sink);
            if (!replay.ok()) {
                std::string detail = "online CSV replay failed: ";
                detail += StartupReplayErrorNameV1(replay.error);
                if (!replay.detail.empty()) {
                    detail += ": ";
                    detail += replay.detail;
                }
                return CandidateFailure(
                    CurrentError() == OnlineRecoveryErrorV1::kNone
                        ? OnlineRecoveryErrorV1::kReplayFailed
                        : CurrentError(),
                    detail);
            }
            if (!std::all_of(
                    tuple_fence_captured_.begin(),
                    tuple_fence_captured_.end(),
                    [](bool captured) noexcept { return captured; })) {
                return CandidateFailure(
                    OnlineRecoveryErrorV1::kReplayPublicationInvalid,
                    "online CSV replay did not capture every production tuple fence");
            }
            {
                std::lock_guard<std::mutex> lock(snapshot_mutex_);
                snapshot_.csv_complete = true;
                snapshot_.phase =
                    OnlineRecoveryPhaseV1::kCandidateCatchUp;
            }
            const LiveJournalSnapshotV1 journal =
                config_.live_journal->Snapshot();
            if (!journal.healthy() ||
                journal.state != LiveJournalStateV1::kWriting) {
                return CandidateFailure(
                    OnlineRecoveryErrorV1::kJournalFailed,
                    "live journal failed before promotion frontier capture");
            }
            const std::uint64_t frontier = journal.accepted_serial;
            while (LastJournalSerial() < frontier) {
                const OnlineRecoveryPumpResultV1 pumped =
                    PumpNextInternal(
                        warmup_deadline_, PumpPhase::kCandidateCatchUp);
                if (pumped.disposition !=
                    OnlineRecoveryPumpDispositionV1::kRecord) {
                    return CandidateFailure(
                        pumped.error == OnlineRecoveryErrorV1::kNone
                            ? OnlineRecoveryErrorV1::kWarmupTimeout
                            : pumped.error,
                        pumped.detail.empty()
                            ? "journal catch-up did not reach the promotion frontier"
                            : pumped.detail);
                }
            }
            return PublishCandidate(frontier);
        } catch (const std::bad_alloc&) {
            return CandidateFailure(
                OnlineRecoveryErrorV1::kResourceExhausted,
                "online recovery allocation failed");
        } catch (...) {
            return CandidateFailure(
                OnlineRecoveryErrorV1::kUnexpectedFailure,
                "online recovery failed unexpectedly");
        }
    }

    [[nodiscard]] OnlineRecoveryCandidateAdvanceV1
    CatchUpOneBeforePromotion(
        std::chrono::steady_clock::time_point operation_deadline) noexcept {
        try {
            OnlineRecoveryCandidateV1 prior = CurrentCandidate();
            if (!prior.ready() ||
                CurrentPhase() != OnlineRecoveryPhaseV1::kCandidateReady) {
                return CandidateAdvanceStateFailure(
                    OnlineRecoveryErrorV1::kInvalidConfiguration,
                    "pre-promotion catch-up requires a ready candidate");
            }
            const auto now = std::chrono::steady_clock::now();
            if (now >= warmup_deadline_) {
                return CandidateAdvanceFailure(
                    OnlineRecoveryErrorV1::kWarmupTimeout,
                    "online recovery exceeded the fixed warmup deadline");
            }
            if (operation_deadline <= now) {
                return CandidateAdvanceIdle();
            }
            {
                std::lock_guard<std::mutex> lock(snapshot_mutex_);
                snapshot_.candidate_boundary_ready = false;
                snapshot_.phase =
                    OnlineRecoveryPhaseV1::kCandidateCatchUp;
            }
            const OnlineRecoveryPumpResultV1 pumped = PumpNextInternal(
                operation_deadline, PumpPhase::kCandidateCatchUp);
            if (pumped.disposition ==
                OnlineRecoveryPumpDispositionV1::kIdle) {
                RestoreCandidateReady();
                return CandidateAdvanceIdle();
            }
            if (pumped.disposition !=
                OnlineRecoveryPumpDispositionV1::kRecord) {
                return CandidateAdvanceFailure(
                    pumped.error == OnlineRecoveryErrorV1::kNone
                        ? OnlineRecoveryErrorV1::kJournalFailed
                        : pumped.error,
                    pumped.detail.empty()
                        ? "pre-promotion journal catch-up ended before the next serial"
                        : pumped.detail);
            }
            if (prior.journal_frontier ==
                    std::numeric_limits<std::uint64_t>::max() ||
                pumped.journal_serial != prior.journal_frontier + 1U) {
                return CandidateAdvanceFailure(
                    OnlineRecoveryErrorV1::kJournalReadFailed,
                    "pre-promotion journal catch-up was not exactly one serial");
            }
            const OnlineRecoveryCandidateV1 candidate =
                PublishCandidate(pumped.journal_serial);
            if (!candidate.ready()) {
                return CandidateAdvanceFailure(
                    candidate.error,
                    candidate.detail.empty()
                        ? "pre-promotion candidate publication failed"
                        : candidate.detail);
            }
            OnlineRecoveryCandidateAdvanceV1 result{};
            result.disposition =
                OnlineRecoveryCandidateAdvanceDispositionV1::kAdvanced;
            result.error = OnlineRecoveryErrorV1::kNone;
            result.candidate = candidate;
            return result;
        } catch (const std::bad_alloc&) {
            return CandidateAdvanceFailure(
                OnlineRecoveryErrorV1::kResourceExhausted,
                "pre-promotion catch-up allocation failed");
        } catch (...) {
            return CandidateAdvanceFailure(
                OnlineRecoveryErrorV1::kUnexpectedFailure,
                "pre-promotion catch-up failed unexpectedly");
        }
    }

    [[nodiscard]] OnlineRecoveryBoundaryV1
    FreezePromotionBoundary() noexcept {
        OnlineRecoveryBoundaryV1 result{};
        try {
            std::lock_guard<std::mutex> lock(snapshot_mutex_);
            if (snapshot_.error != OnlineRecoveryErrorV1::kNone ||
                snapshot_.phase !=
                    OnlineRecoveryPhaseV1::kCandidateReady ||
                !snapshot_.candidate_boundary_ready ||
                snapshot_.promotion_boundary_ready) {
                result.error =
                    OnlineRecoveryErrorV1::kInvalidConfiguration;
                result.detail =
                    "promotion freeze requires one unfrozen ready candidate";
                return result;
            }
            snapshot_.promotion_journal_frontier =
                snapshot_.candidate_journal_frontier;
            snapshot_.promotion_shadow_ingress_frontier =
                snapshot_.candidate_shadow_ingress_frontier;
            snapshot_.promotion_boundary_ready = true;
            snapshot_.phase = OnlineRecoveryPhaseV1::kPromotionFrozen;
            result.error = OnlineRecoveryErrorV1::kNone;
            result.journal_frontier =
                snapshot_.promotion_journal_frontier;
            result.shadow_ingress_frontier =
                snapshot_.promotion_shadow_ingress_frontier;
            result.boundary_ready = true;
            return result;
        } catch (const std::bad_alloc&) {
            result.error = OnlineRecoveryErrorV1::kResourceExhausted;
            return result;
        } catch (...) {
            result.error = OnlineRecoveryErrorV1::kUnexpectedFailure;
            return result;
        }
    }

    [[nodiscard]] OnlineRecoveryBoundaryV1 Recover(
        StartupReplaySinkV1& sink) noexcept {
        if (CurrentPhase() == OnlineRecoveryPhaseV1::kCreated) {
            const OnlineRecoveryCandidateV1 candidate =
                PrepareInitialCandidate(
                    sink,
                    std::chrono::steady_clock::now() +
                        config_.warmup_timeout);
            if (!candidate.ready()) {
                OnlineRecoveryBoundaryV1 failed{};
                failed.error = candidate.error;
                failed.detail = candidate.detail;
                return failed;
            }
        }
        return FreezePromotionBoundary();
    }

    [[nodiscard]] bool CaptureTupleFence(
        const sdk::MessageKey& key,
        std::string* detail) noexcept {
        const std::optional<std::size_t> tuple = TupleIndex(key);
        if (!tuple.has_value() || tuple_fence_captured_[*tuple]) {
            Fail(
                OnlineRecoveryErrorV1::kReplayPublicationInvalid,
                "CSV replay requested an invalid or duplicate tuple fence",
                detail);
            return false;
        }
        if (CancellationRequested()) {
            Fail(
                OnlineRecoveryErrorV1::kCancelled,
                "online recovery was cancelled before tuple fence",
                detail);
            return false;
        }
        LiveJournalTupleFenceV1 fence{};
        if (!config_.live_journal->CaptureTupleFence(key, &fence)) {
            Fail(
                OnlineRecoveryErrorV1::kJournalFailed,
                "live journal could not capture the CSV tuple fence",
                detail);
            return false;
        }
        tuple_fence_serials_[*tuple] = fence.accepted_tuple_serial;
        tuple_fence_captured_[*tuple] = true;
        SetDetail(detail, {});
        return true;
    }

    [[nodiscard]] bool PublishCsv(
        const StartupReplayPublicationV1& publication,
        std::string* detail) noexcept {
        try {
            if (std::chrono::steady_clock::now() >= warmup_deadline_) {
                Fail(
                    OnlineRecoveryErrorV1::kWarmupTimeout,
                    "online CSV replay exceeded the warmup timeout",
                    detail);
                return false;
            }
            if (CancellationRequested()) {
                Fail(
                    OnlineRecoveryErrorV1::kCancelled,
                    "online recovery was cancelled during CSV replay",
                    detail);
                return false;
            }
            constexpr std::uint64_t known_notices =
                kStartupReplayNoticeShenzhenSnapshotChannelUnavailableV1 |
                kStartupReplayNoticeShanghaiOrderQueueMetadataUnavailableV1;
            if (publication.message == nullptr ||
                (publication.provenance_flags &
                 kStartupReplayProvenanceCsvV1) == 0U ||
                (publication.market_notice_flags & ~known_notices) != 0U) {
                Fail(
                    OnlineRecoveryErrorV1::kReplayPublicationInvalid,
                    "CSV replay supplied an invalid publication",
                    detail);
                return false;
            }
            // Run the governor before inspection and semantic decoding.  A
            // gate after those operations would still let bulk recovery steal
            // CPU and cache from an already-lagging exposed preview.
            const ThrottleResult throttle = Throttle(
                GovernorPhase::kCsv, warmup_deadline_, true);
            if (throttle != ThrottleResult::kReady) {
                Fail(
                    throttle == ThrottleResult::kCancelled
                        ? OnlineRecoveryErrorV1::kCancelled
                        : throttle == ThrottleResult::kJournalUnhealthy
                        ? OnlineRecoveryErrorV1::kJournalFailed
                        : throttle ==
                                  ThrottleResult::kCertifiedUnhealthy
                        ? OnlineRecoveryErrorV1::kCertifiedFailed
                        : throttle == ThrottleResult::kShadowUnhealthy
                        ? OnlineRecoveryErrorV1::kShadowAdmissionFailed
                        : throttle == ThrottleResult::kDeadline
                        ? OnlineRecoveryErrorV1::kWarmupTimeout
                        : OnlineRecoveryErrorV1::kUnexpectedFailure,
                    throttle == ThrottleResult::kPreviewUnhealthy
                        ? "CSV recovery preview owner became unhealthy"
                        : throttle == ThrottleResult::kJournalUnhealthy
                        ? "CSV recovery live journal became unhealthy"
                        : throttle ==
                                  ThrottleResult::kControlPlaneUnhealthy
                        ? "CSV recovery control plane became unhealthy"
                        : throttle == ThrottleResult::kShadowUnhealthy
                        ? "CSV recovery shadow owner became unhealthy"
                        : throttle ==
                                  ThrottleResult::kCertifiedUnhealthy
                        ? "CSV recovery CERTIFIED handoff became unhealthy"
                        : "CSV replay could not pass the recovery pressure gate",
                    detail);
                return false;
            }
            realtime::OwnedIngressMessageInspectionV1 inspection{};
            const realtime::OwnedIngressMessageErrorV1 inspect_error =
                realtime::InspectOwnedIngressMessageV1(
                    publication.message,
                    config_.maximum_message_bytes,
                    &inspection);
            const std::optional<std::size_t> tuple =
                inspect_error ==
                            realtime::OwnedIngressMessageErrorV1::kNone &&
                        inspection && inspection.key() == publication.key
                    ? TupleIndex(publication.key)
                    : std::nullopt;
            if (!tuple.has_value() || publication.csv_sequence == 0U ||
                inspection.vendor_head().sequence_id() !=
                    publication.csv_sequence) {
                Fail(
                    OnlineRecoveryErrorV1::kReplayPublicationInvalid,
                    "CSV replay message identity is invalid",
                    detail);
                return false;
            }
            Fingerprint fingerprint{};
            fingerprint.vendor_sequence_id = publication.csv_sequence;
            fingerprint.normalize_shenzhen_snapshot_channel =
                (publication.market_notice_flags &
                 kStartupReplayNoticeShenzhenSnapshotChannelUnavailableV1) !=
                0U;
            if (!SemanticDigest(
                    inspection,
                    fingerprint.normalize_shenzhen_snapshot_channel,
                    &fingerprint.digest)) {
                Fail(
                    OnlineRecoveryErrorV1::kReplayPublicationInvalid,
                    "CSV replay semantic fingerprint failed",
                    detail);
                return false;
            }
            const Fingerprint* existing = FindFingerprint(
                *tuple, fingerprint.vendor_sequence_id);
            if (existing != nullptr) {
                Fail(
                    existing->digest == fingerprint.digest
                        ? OnlineRecoveryErrorV1::kReplayDuplicate
                        : OnlineRecoveryErrorV1::kOverlapConflict,
                    existing->digest == fingerprint.digest
                        ? "CSV replay contains a duplicate tuple/SequenceID"
                        : "CSV replay reuses tuple/SequenceID with a different payload",
                    detail);
                return false;
            }
            std::uint64_t realtime_ns = 0U;
            std::uint64_t monotonic_ns = 0U;
            if (!CurrentClocks(&realtime_ns, &monotonic_ns)) {
                Fail(
                    OnlineRecoveryErrorV1::kUnexpectedFailure,
                    "CSV replay receive clock failed",
                    detail);
                return false;
            }
            std::uint64_t notices = market::MarketNoticeBitV1(
                market::MarketNoticeV1::kRecoveredFromCsv);
            if ((publication.market_notice_flags & known_notices) != 0U) {
                notices |= market::MarketNoticeBitV1(
                    market::MarketNoticeV1::kCsvSourceFieldUnavailable);
            }
            const auto admission_now =
                std::chrono::steady_clock::now();
            if (admission_now >= warmup_deadline_) {
                Fail(
                    OnlineRecoveryErrorV1::kWarmupTimeout,
                    "online CSV replay exceeded the warmup timeout",
                    detail);
                return false;
            }
            const auto admission_remaining = std::max(
                std::chrono::nanoseconds(1),
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    warmup_deadline_ - admission_now));
            const runtime::RealtimePipelineIngressResultV1 ingress =
                IngestShadow(
                    publication.message,
                    realtime_ns,
                    monotonic_ns,
                    notices,
                    std::min(
                        config_.per_record_admission_timeout,
                        admission_remaining));
            if (ingress.error !=
                    runtime::RealtimePipelineIngressErrorV1::kNone &&
                ingress.error !=
                    runtime::RealtimePipelineIngressErrorV1::
                        kFilteredNonAShare) {
                const bool admission_expired =
                    std::chrono::steady_clock::now() >=
                    warmup_deadline_;
                Fail(
                    admission_expired
                        ? OnlineRecoveryErrorV1::kWarmupTimeout
                        : OnlineRecoveryErrorV1::
                              kShadowAdmissionFailed,
                    admission_expired
                        ? "online CSV replay exceeded the warmup timeout"
                        : "shadow rejected a CSV replay publication",
                    detail);
                return false;
            }
            RetainFingerprint(*tuple, fingerprint);
            replay_cutoff_[*tuple] = replay_cutoff_seen_[*tuple]
                                         ? std::max(
                                               replay_cutoff_[*tuple],
                                               fingerprint.vendor_sequence_id)
                                         : fingerprint.vendor_sequence_id;
            replay_cutoff_seen_[*tuple] = true;
            {
                std::lock_guard<std::mutex> lock(snapshot_mutex_);
                ++snapshot_.csv_publications;
                if (ingress.error ==
                    runtime::RealtimePipelineIngressErrorV1::
                        kFilteredNonAShare) {
                    ++snapshot_.csv_filtered_publications;
                }
            }
            SetDetail(detail, {});
            return true;
        } catch (const std::bad_alloc&) {
            Fail(
                OnlineRecoveryErrorV1::kResourceExhausted,
                "CSV replay sink allocation failed",
                detail);
            return false;
        } catch (...) {
            Fail(
                OnlineRecoveryErrorV1::kUnexpectedFailure,
                "CSV replay sink failed unexpectedly",
                detail);
            return false;
        }
    }

    [[nodiscard]] bool CooperativeCheckpoint(
        std::string* detail) noexcept {
        try {
            {
                std::lock_guard<std::mutex> lock(snapshot_mutex_);
                ++snapshot_.csv_parser_checkpoint_events;
            }
            // Parsing and native-sequence repair can consume CPU and retain
            // many pending rows before producing a publication.  Sample the
            // same health/backpressure gate here, but do not account a
            // synthetic publication in the bulk-record quantum.
            const ThrottleResult throttle = Throttle(
                GovernorPhase::kCsv, warmup_deadline_, false);
            if (throttle == ThrottleResult::kReady) {
                SetDetail(detail, {});
                return true;
            }
            Fail(
                throttle == ThrottleResult::kCancelled
                    ? OnlineRecoveryErrorV1::kCancelled
                    : throttle == ThrottleResult::kJournalUnhealthy
                    ? OnlineRecoveryErrorV1::kJournalFailed
                    : throttle == ThrottleResult::kCertifiedUnhealthy
                    ? OnlineRecoveryErrorV1::kCertifiedFailed
                    : throttle == ThrottleResult::kShadowUnhealthy
                    ? OnlineRecoveryErrorV1::kShadowAdmissionFailed
                    : throttle == ThrottleResult::kDeadline
                    ? OnlineRecoveryErrorV1::kWarmupTimeout
                    : OnlineRecoveryErrorV1::kUnexpectedFailure,
                throttle == ThrottleResult::kPreviewUnhealthy
                    ? "CSV parser preview owner became unhealthy"
                    : throttle == ThrottleResult::kJournalUnhealthy
                    ? "CSV parser live journal became unhealthy"
                    : throttle ==
                              ThrottleResult::kControlPlaneUnhealthy
                    ? "CSV parser control plane became unhealthy"
                    : throttle == ThrottleResult::kShadowUnhealthy
                    ? "CSV parser shadow owner became unhealthy"
                    : throttle ==
                              ThrottleResult::kCertifiedUnhealthy
                    ? "CSV parser CERTIFIED handoff became unhealthy"
                    : "CSV parser could not pass the recovery pressure gate",
                detail);
            return false;
        } catch (...) {
            Fail(
                OnlineRecoveryErrorV1::kUnexpectedFailure,
                "CSV parser pressure checkpoint failed unexpectedly",
                detail);
            return false;
        }
    }

    [[nodiscard]] OnlineRecoveryPumpResultV1 PumpNext(
        std::chrono::steady_clock::time_point deadline) noexcept {
        const OnlineRecoveryPhaseV1 phase = CurrentPhase();
        if (phase == OnlineRecoveryPhaseV1::kEnded) {
            OnlineRecoveryPumpResultV1 result{};
            result.disposition = OnlineRecoveryPumpDispositionV1::kEnd;
            result.error = OnlineRecoveryErrorV1::kNone;
            return result;
        }
        if (phase != OnlineRecoveryPhaseV1::kPromoted &&
            phase != OnlineRecoveryPhaseV1::kCleanShutdownTail) {
            return PumpStateFailure(
                OnlineRecoveryErrorV1::kInvalidConfiguration,
                "journal tail pump requires completed promotion");
        }
        OnlineRecoveryPumpResultV1 result = PumpNextInternal(
            deadline, PumpPhase::kJournalTail);
        if (result.disposition ==
            OnlineRecoveryPumpDispositionV1::kEnd) {
            std::lock_guard<std::mutex> lock(snapshot_mutex_);
            if (snapshot_.error == OnlineRecoveryErrorV1::kNone) {
                snapshot_.phase = OnlineRecoveryPhaseV1::kEnded;
            }
        }
        return result;
    }

    [[nodiscard]] bool MarkPromoted(
        std::uint64_t promotion_realtime_ns) noexcept {
        if (promotion_realtime_ns == 0U) {
            return false;
        }
        try {
            std::lock_guard<std::mutex> lock(snapshot_mutex_);
            if (!snapshot_.promotion_boundary_ready || snapshot_.promoted ||
                snapshot_.error != OnlineRecoveryErrorV1::kNone ||
                snapshot_.phase !=
                    OnlineRecoveryPhaseV1::kPromotionFrozen) {
                return false;
            }
            snapshot_.promotion_realtime_ns = promotion_realtime_ns;
            snapshot_.promoted = true;
            snapshot_.phase = OnlineRecoveryPhaseV1::kPromoted;
            return true;
        } catch (...) {
            return false;
        }
    }

    [[nodiscard]] bool BeginCleanShutdownTailDrain(
        std::chrono::steady_clock::time_point deadline) noexcept {
        const auto now = std::chrono::steady_clock::now();
        const auto deadline_count =
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                deadline.time_since_epoch())
                .count();
        if (deadline <= now || deadline_count <= 0) {
            return false;
        }
        try {
            std::lock_guard<std::mutex> lock(snapshot_mutex_);
            if (!snapshot_.promoted ||
                snapshot_.phase != OnlineRecoveryPhaseV1::kPromoted ||
                snapshot_.error != OnlineRecoveryErrorV1::kNone ||
                clean_shutdown_tail_drain_.load(
                    std::memory_order_relaxed)) {
                return false;
            }
            clean_shutdown_tail_deadline_ns_.store(
                deadline_count, std::memory_order_relaxed);
            clean_shutdown_tail_drain_.store(
                true, std::memory_order_release);
            snapshot_.phase =
                OnlineRecoveryPhaseV1::kCleanShutdownTail;
            return true;
        } catch (...) {
            return false;
        }
    }

    [[nodiscard]] OnlineRecoverySnapshotV1 Snapshot() const noexcept {
        try {
            std::lock_guard<std::mutex> lock(snapshot_mutex_);
            return snapshot_;
        } catch (...) {
            OnlineRecoverySnapshotV1 result{};
            result.error = OnlineRecoveryErrorV1::kUnexpectedFailure;
            return result;
        }
    }

private:
    [[nodiscard]] OnlineRecoveryPumpResultV1 PumpNextInternal(
        std::chrono::steady_clock::time_point deadline,
        PumpPhase pump_phase) noexcept {
        OnlineRecoveryPumpResultV1 result{};
        const bool recovery_phase =
            pump_phase == PumpPhase::kCandidateCatchUp;
        if (CancellationRequested()) {
            if (recovery_phase) {
                return PumpFailure(
                    OnlineRecoveryErrorV1::kCancelled,
                    "online recovery was cancelled");
            }
            result.disposition = OnlineRecoveryPumpDispositionV1::kEnd;
            result.error = OnlineRecoveryErrorV1::kNone;
            result.detail = "online recovery was cancelled";
            return result;
        }
        const std::chrono::steady_clock::time_point effective_deadline =
            recovery_phase
                ? std::min(deadline, warmup_deadline_)
                : std::min(deadline, CleanShutdownTailDeadline(deadline));
        if (std::chrono::steady_clock::now() >= effective_deadline) {
            if (recovery_phase &&
                std::chrono::steady_clock::now() >= warmup_deadline_) {
                return PumpFailure(
                    OnlineRecoveryErrorV1::kWarmupTimeout,
                    "online recovery exceeded the fixed warmup deadline");
            }
            result.disposition = OnlineRecoveryPumpDispositionV1::kIdle;
            return result;
        }
        const ThrottleResult throttle = Throttle(
            recovery_phase ? GovernorPhase::kJournalRecovery
                           : GovernorPhase::kJournalTail,
            effective_deadline,
            true);
        // Shutdown can race after Throttle's first cancellation sample and
        // make the preview/control owners look unhealthy.  Before translating
        // such a post-wait state into a terminal recovery failure, give the
        // explicit pre-promotion cancellation request priority.
        if (recovery_phase && CancellationRequested()) {
            return PumpFailure(
                OnlineRecoveryErrorV1::kCancelled,
                "online recovery was cancelled");
        }
        if (throttle == ThrottleResult::kDeadline) {
            if (recovery_phase &&
                std::chrono::steady_clock::now() >= warmup_deadline_) {
                return PumpFailure(
                    OnlineRecoveryErrorV1::kWarmupTimeout,
                    "online recovery exceeded the fixed warmup deadline");
            }
            if (!recovery_phase && CleanShutdownTailDrainExpired()) {
                return PumpFailure(
                    OnlineRecoveryErrorV1::kBackpressureTimeout,
                    "clean shutdown journal tail drain timed out");
            }
            result.disposition = OnlineRecoveryPumpDispositionV1::kIdle;
            return result;
        }
        if (throttle == ThrottleResult::kCancelled) {
            if (recovery_phase) {
                return PumpFailure(
                    OnlineRecoveryErrorV1::kCancelled,
                    "online recovery was cancelled");
            }
            result.disposition = OnlineRecoveryPumpDispositionV1::kEnd;
            result.error = OnlineRecoveryErrorV1::kNone;
            return result;
        }
        if (throttle == ThrottleResult::kCertifiedUnhealthy) {
            return PumpFailure(
                OnlineRecoveryErrorV1::kCertifiedFailed,
                "CERTIFIED handoff became terminally unhealthy");
        }
        if (throttle == ThrottleResult::kJournalUnhealthy) {
            return PumpFailure(
                OnlineRecoveryErrorV1::kJournalFailed,
                "live journal became terminally unhealthy");
        }
        if (throttle == ThrottleResult::kControlPlaneUnhealthy) {
            return PumpFailure(
                OnlineRecoveryErrorV1::kUnexpectedFailure,
                "online recovery control plane became terminally unhealthy");
        }
        if (throttle == ThrottleResult::kShadowUnhealthy) {
            return PumpFailure(
                OnlineRecoveryErrorV1::kShadowAdmissionFailed,
                "shadow owner became terminally unhealthy");
        }
        if (throttle == ThrottleResult::kPreviewUnhealthy) {
            return PumpFailure(
                OnlineRecoveryErrorV1::kUnexpectedFailure,
                "LIVE_PARTIAL owner became terminally unhealthy");
        }
        if (throttle != ThrottleResult::kReady) {
            return PumpFailure(
                OnlineRecoveryErrorV1::kUnexpectedFailure,
                "online recovery pressure provider failed");
        }
        LiveJournalReadResultV1 read =
            reader_->ReadNext(effective_deadline);
        // StopAndFlush wakes a reader with End. If cancellation linearized
        // while ReadNext was blocked, End is an expected shutdown result, not
        // evidence that the recovery journal ended unexpectedly.
        if (recovery_phase && CancellationRequested()) {
            return PumpFailure(
                OnlineRecoveryErrorV1::kCancelled,
                "online recovery was cancelled");
        }
        if (read.disposition == LiveJournalReadDispositionV1::kTimeout) {
            if (recovery_phase &&
                std::chrono::steady_clock::now() >= warmup_deadline_) {
                return PumpFailure(
                    OnlineRecoveryErrorV1::kWarmupTimeout,
                    "online recovery exceeded the fixed warmup deadline");
            }
            if (!recovery_phase && CleanShutdownTailDrainExpired()) {
                return PumpFailure(
                    OnlineRecoveryErrorV1::kBackpressureTimeout,
                    "clean shutdown journal tail drain timed out");
            }
            result.disposition = OnlineRecoveryPumpDispositionV1::kIdle;
            return result;
        }
        if (read.disposition == LiveJournalReadDispositionV1::kEnd) {
            result.disposition = OnlineRecoveryPumpDispositionV1::kEnd;
            return result;
        }
        if (!read.has_record()) {
            return PumpFailure(
                read.error == LiveJournalErrorV1::kNone
                    ? OnlineRecoveryErrorV1::kJournalReadFailed
                    : OnlineRecoveryErrorV1::kJournalFailed,
                "live journal read or integrity validation failed");
        }
        MdlLiveJournalRecordV1& record = *read.record;
        realtime::OwnedIngressMessageInspectionV1 inspection{};
        const realtime::OwnedIngressMessageErrorV1 inspect_error =
            realtime::InspectOwnedIngressMessageV1(
                &record,
                config_.maximum_message_bytes,
                &inspection);
        const std::optional<std::size_t> tuple =
            inspect_error == realtime::OwnedIngressMessageErrorV1::kNone &&
                    inspection && inspection.key() == record.key()
                ? TupleIndex(record.key())
                : std::nullopt;
        if (!tuple.has_value() || record.global_serial() == 0U ||
            record.tuple_serial() == 0U) {
            return PumpFailure(
                OnlineRecoveryErrorV1::kJournalReadFailed,
                "live journal record identity is invalid");
        }
        const std::uint64_t vendor_sequence =
            inspection.vendor_head().sequence_id();
        const Fingerprint* replay =
            FindFingerprint(*tuple, vendor_sequence);
        HandoffPhase& phase = phases_[*tuple];
        if (replay != nullptr) {
            if (phase == HandoffPhase::kLiveSuffix) {
                return PumpFailure(
                    OnlineRecoveryErrorV1::kOverlapConflict,
                    "live journal returned to the CSV prefix after the live suffix");
            }
            common::Sha256Digest digest{};
            if (!SemanticDigest(
                    inspection,
                    replay->normalize_shenzhen_snapshot_channel,
                    &digest)) {
                return PumpFailure(
                    OnlineRecoveryErrorV1::kJournalReadFailed,
                    "live journal semantic fingerprint failed");
            }
            {
                std::lock_guard<std::mutex> lock(snapshot_mutex_);
                ++snapshot_.journal_overlap_digests;
            }
            if (replay->digest != digest) {
                return PumpFailure(
                    OnlineRecoveryErrorV1::kOverlapConflict,
                    "CSV/live tuple/SequenceID payloads conflict");
            }
            phase = HandoffPhase::kOverlap;
            {
                std::lock_guard<std::mutex> lock(snapshot_mutex_);
                ++snapshot_.journal_records_read;
                ++snapshot_.journal_duplicates_suppressed;
                snapshot_.last_journal_serial = record.global_serial();
            }
            result.disposition = OnlineRecoveryPumpDispositionV1::kRecord;
            result.error = OnlineRecoveryErrorV1::kNone;
            result.journal_serial = record.global_serial();
            return result;
        }
        if (vendor_sequence == 0U ||
            (replay_cutoff_seen_[*tuple] &&
             vendor_sequence <= replay_cutoff_[*tuple]) ||
            (tuple_fence_captured_[*tuple] &&
             record.tuple_serial() <= tuple_fence_serials_[*tuple])) {
            return PumpFailure(
                OnlineRecoveryErrorV1::kOverlapMissing,
                "an unmatched journal callback is not provably beyond its CSV tuple fence/cutoff");
        }
        phase = HandoffPhase::kLiveSuffix;
        std::chrono::nanoseconds shadow_admission_timeout =
            config_.per_record_admission_timeout;
        if (recovery_phase) {
            const auto now = std::chrono::steady_clock::now();
            if (now >= warmup_deadline_) {
                return PumpFailure(
                    OnlineRecoveryErrorV1::kWarmupTimeout,
                    "online recovery exceeded the fixed warmup deadline");
            }
            const auto remaining = std::max(
                std::chrono::nanoseconds(1),
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    warmup_deadline_ - now));
            shadow_admission_timeout = std::min(
                shadow_admission_timeout, remaining);
        } else if (clean_shutdown_tail_drain_.load(
                std::memory_order_acquire)) {
            const auto now = std::chrono::steady_clock::now();
            const auto clean_deadline =
                CleanShutdownTailDeadline(effective_deadline);
            if (now >= clean_deadline) {
                return PumpFailure(
                    OnlineRecoveryErrorV1::kBackpressureTimeout,
                    "clean shutdown journal tail drain timed out");
            }
            const auto remaining = std::max(
                std::chrono::nanoseconds(1),
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    clean_deadline - now));
            shadow_admission_timeout = std::min(
                shadow_admission_timeout, remaining);
        }
        const runtime::RealtimePipelineIngressResultV1 ingress =
            IngestShadow(
                &record,
                record.recv_realtime_ns(),
                record.recv_monotonic_ns(),
                0U,
                shadow_admission_timeout);
        if (ingress.error !=
                runtime::RealtimePipelineIngressErrorV1::kNone &&
            ingress.error !=
                runtime::RealtimePipelineIngressErrorV1::
                    kFilteredNonAShare) {
            if (recovery_phase && CancellationRequested()) {
                return PumpFailure(
                    OnlineRecoveryErrorV1::kCancelled,
                    "online recovery was cancelled");
            }
            if (recovery_phase &&
                std::chrono::steady_clock::now() >= warmup_deadline_) {
                return PumpFailure(
                    OnlineRecoveryErrorV1::kWarmupTimeout,
                    "online recovery exceeded the fixed warmup deadline");
            }
            if (!recovery_phase && CleanShutdownTailDrainExpired()) {
                return PumpFailure(
                    OnlineRecoveryErrorV1::kBackpressureTimeout,
                    "clean shutdown journal tail drain timed out");
            }
            return PumpFailure(
                OnlineRecoveryErrorV1::kShadowAdmissionFailed,
                "shadow rejected a journal suffix record");
        }
        {
            std::lock_guard<std::mutex> lock(snapshot_mutex_);
            ++snapshot_.journal_records_read;
            ++snapshot_.journal_live_suffix_digests_skipped;
            ++snapshot_.journal_suffix_publications;
            if (ingress.error ==
                runtime::RealtimePipelineIngressErrorV1::
                    kFilteredNonAShare) {
                ++snapshot_.journal_filtered_publications;
            }
            snapshot_.last_journal_serial = record.global_serial();
        }
        result.disposition = OnlineRecoveryPumpDispositionV1::kRecord;
        result.error = OnlineRecoveryErrorV1::kNone;
        result.journal_serial = record.global_serial();
        return result;
    }

    [[nodiscard]] bool SemanticDigest(
        const realtime::OwnedIngressMessageInspectionV1& inspection,
        bool normalize_shenzhen_snapshot_channel,
        common::Sha256Digest* output) noexcept {
        const std::uint8_t source = inspection.source_slot();
        if (source >= market::kRealtimeHistorySourceCountV1 ||
            output == nullptr || fingerprint_decoders_[source] == nullptr ||
            fingerprint_source_sequences_[source] ==
                std::numeric_limits<std::uint64_t>::max()) {
            return false;
        }
        const sdk::VendorHeadView head = inspection.vendor_head();
        market::MarketMessageViewV1 view{};
        view.source_stream_id = config_.source_stream_ids[source];
        view.trade_date = config_.trade_date;
        view.source_sequence = ++fingerprint_source_sequences_[source];
        view.service_id = inspection.key().service_id;
        view.service_version = inspection.key().service_version;
        view.message_id = inspection.key().message_id;
        view.message_encoding = head.message_encoding();
        view.vendor_local_time_raw = head.local_time_raw();
        view.vendor_sequence_id = head.sequence_id();
        view.recv_realtime_ns = 1;
        view.recv_monotonic_ns = 1;
        view.body = inspection.body();
        market::DecodedMarketEventV1 decoded;
        return fingerprint_decoders_[source]->Decode(view, &decoded) ==
                   market::MarketDecodeErrorV1::kNone &&
               runtime::RealtimePipelineStartupSemanticDigestV1(
                   decoded,
                   normalize_shenzhen_snapshot_channel,
                   output);
    }

    [[nodiscard]] runtime::RealtimePipelineIngressResultV1 IngestShadow(
        const datayes::mdl::MDLMessage* message,
        std::uint64_t realtime_ns,
        std::uint64_t monotonic_ns,
        std::uint64_t notices,
        std::chrono::nanoseconds admission_timeout =
            std::chrono::nanoseconds::max()) noexcept {
        runtime::RealtimePipelineExternalIngressV1 input{};
        input.message = message;
        input.recv_realtime_ns = realtime_ns;
        input.recv_monotonic_ns = monotonic_ns;
        input.additional_market_notices = notices;
        input.admission_timeout =
            admission_timeout == std::chrono::nanoseconds::max()
                ? config_.per_record_admission_timeout
                : admission_timeout;
        return config_.shadow_pipeline->IngestExternalMessage(input);
    }

    [[nodiscard]] std::chrono::steady_clock::time_point
    CleanShutdownTailDeadline(
        std::chrono::steady_clock::time_point fallback) const noexcept {
        if (!clean_shutdown_tail_drain_.load(
                std::memory_order_acquire)) {
            return fallback;
        }
        const auto count = clean_shutdown_tail_deadline_ns_.load(
            std::memory_order_relaxed);
        if (count <= 0) {
            return fallback;
        }
        return std::chrono::steady_clock::time_point(
            std::chrono::nanoseconds(count));
    }

    [[nodiscard]] bool CleanShutdownTailDrainExpired() const noexcept {
        if (!clean_shutdown_tail_drain_.load(
                std::memory_order_acquire)) {
            return false;
        }
        const auto count = clean_shutdown_tail_deadline_ns_.load(
            std::memory_order_relaxed);
        return count > 0 && std::chrono::steady_clock::now() >=
                                std::chrono::steady_clock::time_point(
                                    std::chrono::nanoseconds(count));
    }

    [[nodiscard]] const Fingerprint* FindFingerprint(
        std::size_t tuple,
        std::uint64_t vendor_sequence) const noexcept {
        if (tuple >= fingerprints_.size()) {
            return nullptr;
        }
        const auto found = fingerprints_[tuple].find(vendor_sequence);
        return found == fingerprints_[tuple].end() ? nullptr
                                                    : &found->second;
    }

    void RetainFingerprint(
        std::size_t tuple,
        const Fingerprint& fingerprint) {
        auto inserted = fingerprints_[tuple].emplace(
            fingerprint.vendor_sequence_id, fingerprint);
        if (!inserted.second) {
            return;
        }
        retained_sequences_[tuple].push(
            fingerprint.vendor_sequence_id);
        if (retained_sequences_[tuple].size() >
            config_.overlap_retention_per_tuple) {
            const std::uint64_t smallest =
                retained_sequences_[tuple].top();
            fingerprints_[tuple].erase(smallest);
            retained_sequences_[tuple].pop();
        }
    }

    [[nodiscard]] CertifiedPressure SampleCertifiedPressure()
        const noexcept {
        CertifiedPressure result{};
        if (config_.certified_pressure_sample) {
            result.configured = true;
            try {
                const OnlineRecoveryCertifiedPressureV1 sample =
                    config_.certified_pressure_sample();
                result.healthy = sample.healthy;
                result.utilization_percent =
                    sample.utilization_percent;
                result.valid = sample.utilization_percent <= 100U;
            } catch (...) {
                result.valid = false;
            }
            return result;
        }
        result.configured =
            static_cast<bool>(
                config_.certified_queue_utilization_percent) ||
            static_cast<bool>(config_.certified_handoff_healthy);
        try {
            if (config_.certified_handoff_healthy) {
                result.healthy =
                    config_.certified_handoff_healthy();
                if (!result.healthy) {
                    return result;
                }
            }
            if (config_.certified_queue_utilization_percent) {
                result.utilization_percent =
                    config_.certified_queue_utilization_percent();
                result.valid =
                    result.utilization_percent <= 100U;
            }
        } catch (...) {
            result.valid = false;
        }
        return result;
    }

    void SleepBounded(
        std::chrono::nanoseconds duration,
        std::chrono::steady_clock::time_point deadline) const noexcept {
        if (duration <= std::chrono::nanoseconds::zero()) {
            return;
        }
        const auto now = std::chrono::steady_clock::now();
        const auto requested = now + duration;
        std::this_thread::sleep_until(
            std::min(requested, deadline));
    }

    [[nodiscard]] ThrottleResult Throttle(
        GovernorPhase phase,
        std::chrono::steady_clock::time_point deadline,
        bool account_bulk_record) noexcept {
        const bool bulk = phase != GovernorPhase::kJournalTail;
        bool certified_pause_counted = false;
        bool quantum_accounted = false;
        bool quantum_action_completed = false;
        for (;;) {
            if (CancellationRequested()) {
                return ThrottleResult::kCancelled;
            }
            if (std::chrono::steady_clock::now() >= deadline) {
                return ThrottleResult::kDeadline;
            }

            const bool preview_configured =
                static_cast<bool>(config_.preview_live_status);
            runtime::RealtimePipelineLiveStatusV1 preview{};
            if (preview_configured) {
                try {
                    preview = config_.preview_live_status();
                } catch (...) {
                    return ThrottleResult::kFailed;
                }
            }
            const runtime::RealtimePipelineLiveStatusV1 shadow =
                config_.shadow_pipeline->LiveStatus();
            const CertifiedPressure certified =
                SampleCertifiedPressure();
            bool control_planes_healthy = true;
            if (config_.control_planes_healthy) {
                try {
                    control_planes_healthy =
                        config_.control_planes_healthy();
                } catch (...) {
                    return ThrottleResult::kFailed;
                }
            }

            // Sample every terminal owner before entering any pressure wait.
            // Otherwise sustained preview/shadow backlog can hide a terminal
            // downstream failure until the whole warmup deadline expires.
            if (preview_configured) {
                if (!preview.healthy()) {
                    const bool expected_quiesce =
                        phase == GovernorPhase::kJournalTail &&
                        clean_shutdown_tail_drain_.load(
                            std::memory_order_acquire) &&
                        preview.processing_progress.valid() &&
                        !preview.fatal &&
                        !preview.trade_date_boundary_reached;
                    if (!expected_quiesce) {
                        return ThrottleResult::kPreviewUnhealthy;
                    }
                }
            }
            if (!shadow.healthy()) {
                return ThrottleResult::kShadowUnhealthy;
            }
            if (config_.live_journal->failed()) {
                return ThrottleResult::kJournalUnhealthy;
            }
            if (!control_planes_healthy) {
                return ThrottleResult::kControlPlaneUnhealthy;
            }
            if (!certified.valid) {
                return ThrottleResult::kFailed;
            }
            if (!certified.healthy) {
                return ThrottleResult::kCertifiedUnhealthy;
            }

            if (preview_configured) {
                if (!preview_sample_seen_) {
                    last_preview_accepted_sequence_ =
                        preview.processing_progress.accepted_sequence;
                    preview_sample_seen_ = true;
                } else if (
                    preview.processing_progress.accepted_sequence >
                    last_preview_accepted_sequence_) {
                    if (bulk) {
                        preview_activity_in_quantum_ = true;
                    }
                    last_preview_accepted_sequence_ =
                        preview.processing_progress.accepted_sequence;
                }
                const std::uint64_t preview_outstanding =
                    preview.processing_progress
                        .processing_lag_records();
                const DrainDecision preview_drain =
                    AdvanceDrainState(
                        preview_outstanding,
                        config_
                            .preview_outstanding_low_water_records,
                        config_
                            .preview_outstanding_high_water_records,
                        &preview_draining_);
                if (preview_drain.entered) {
                    std::lock_guard<std::mutex> lock(snapshot_mutex_);
                    ++snapshot_.preview_pause_events;
                }
                if (preview_drain.pause) {
                    SleepBounded(
                        config_.pressure_poll_interval, deadline);
                    continue;
                }
            }

            const std::uint64_t shadow_outstanding =
                shadow.processing_progress.processing_lag_records();
            const DrainDecision shadow_drain =
                AdvanceDrainState(
                    shadow_outstanding,
                    config_.shadow_outstanding_low_water_records,
                    config_.shadow_outstanding_high_water_records,
                    &shadow_draining_);
            if (shadow_drain.entered) {
                std::lock_guard<std::mutex> lock(snapshot_mutex_);
                ++snapshot_.shadow_pause_events;
            }
            if (shadow_drain.pause) {
                SleepBounded(
                    config_.pressure_poll_interval, deadline);
                continue;
            }
            if (certified.configured &&
                certified.utilization_percent >=
                    config_.certified_pause_watermark_percent) {
                if (!certified_pause_counted) {
                    std::lock_guard<std::mutex> lock(snapshot_mutex_);
                    ++snapshot_.replay_pause_events;
                    certified_pause_counted = true;
                }
                SleepBounded(
                    config_.pressure_poll_interval, deadline);
                continue;
            }

            if (!bulk || !account_bulk_record) {
                // The permanent durable tail is already one-for-one with
                // live traffic.  Do not add the bulk replay quantum cooldown
                // to its freshness path.  Parser checkpoints similarly
                // sample pressure without pretending that a publication was
                // admitted to the bulk quantum.
                return ThrottleResult::kReady;
            }
            if (!quantum_accounted) {
                ++bulk_records_in_quantum_;
                maximum_certified_pressure_in_quantum_ = std::max(
                    maximum_certified_pressure_in_quantum_,
                    certified.utilization_percent);
                quantum_accounted = true;
            }
            if (bulk_records_in_quantum_ <
                    config_.governor_quantum_records ||
                quantum_action_completed) {
                return ThrottleResult::kReady;
            }

            const std::uint32_t quantum_pressure =
                maximum_certified_pressure_in_quantum_;
            const bool preview_active =
                preview_activity_in_quantum_;
            bulk_records_in_quantum_ = 0U;
            maximum_certified_pressure_in_quantum_ = 0U;
            preview_activity_in_quantum_ = false;
            quantum_action_completed = true;
            if (quantum_pressure >=
                config_.certified_low_watermark_percent) {
                {
                    std::lock_guard<std::mutex> lock(snapshot_mutex_);
                    ++snapshot_.replay_throttle_events;
                }
                SleepBounded(
                    quantum_pressure >=
                            config_
                                .certified_high_watermark_percent
                        ? config_.certified_high_pressure_cooldown
                        : config_.certified_low_pressure_cooldown,
                    deadline);
                continue;
            }
            if (preview_active) {
                {
                    std::lock_guard<std::mutex> lock(snapshot_mutex_);
                    ++snapshot_.cooperative_yield_events;
                }
                std::this_thread::yield();
            }
            return ThrottleResult::kReady;
        }
    }

    [[nodiscard]] bool CancellationRequested() const noexcept {
        if (!config_.cancel_requested) {
            return false;
        }
        try {
            return config_.cancel_requested();
        } catch (...) {
            return true;
        }
    }

    [[nodiscard]] std::uint64_t LastJournalSerial() const noexcept {
        std::lock_guard<std::mutex> lock(snapshot_mutex_);
        return snapshot_.last_journal_serial;
    }

    [[nodiscard]] OnlineRecoveryErrorV1 CurrentError() const noexcept {
        std::lock_guard<std::mutex> lock(snapshot_mutex_);
        return snapshot_.error;
    }

    [[nodiscard]] OnlineRecoveryPhaseV1 CurrentPhase() const noexcept {
        std::lock_guard<std::mutex> lock(snapshot_mutex_);
        return snapshot_.phase;
    }

    [[nodiscard]] OnlineRecoveryCandidateV1 CurrentCandidate() const
        noexcept {
        OnlineRecoveryCandidateV1 result{};
        try {
            std::lock_guard<std::mutex> lock(snapshot_mutex_);
            result.error = snapshot_.error;
            result.journal_frontier =
                snapshot_.candidate_journal_frontier;
            result.shadow_ingress_frontier =
                snapshot_.candidate_shadow_ingress_frontier;
            result.warmup_deadline = warmup_deadline_;
            result.candidate_ready =
                snapshot_.candidate_boundary_ready &&
                snapshot_.error == OnlineRecoveryErrorV1::kNone;
        } catch (...) {
            result.error = OnlineRecoveryErrorV1::kUnexpectedFailure;
        }
        return result;
    }

    [[nodiscard]] OnlineRecoveryCandidateV1 PublishCandidate(
        std::uint64_t journal_frontier) noexcept {
        if (LastJournalSerial() != journal_frontier) {
            return CandidateFailure(
                OnlineRecoveryErrorV1::kJournalReadFailed,
                "candidate frontier does not match the sequential journal reader");
        }
        const runtime::RealtimePipelineLiveStatusV1 shadow =
            config_.shadow_pipeline->LiveStatus();
        if (!shadow.healthy()) {
            return CandidateFailure(
                OnlineRecoveryErrorV1::kShadowAdmissionFailed,
                "shadow pipeline failed before promotion candidate publication");
        }
        bool candidate_state_valid = false;
        {
            std::lock_guard<std::mutex> lock(snapshot_mutex_);
            candidate_state_valid =
                snapshot_.error == OnlineRecoveryErrorV1::kNone &&
                !snapshot_.promotion_boundary_ready && !snapshot_.promoted;
            if (candidate_state_valid) {
                snapshot_.candidate_journal_frontier = journal_frontier;
                snapshot_.candidate_shadow_ingress_frontier =
                    shadow.processing_progress.accepted_sequence;
                snapshot_.candidate_boundary_ready = true;
                snapshot_.phase =
                    OnlineRecoveryPhaseV1::kCandidateReady;
            }
        }
        if (!candidate_state_valid) {
            return CandidateStateFailure(
                OnlineRecoveryErrorV1::kInvalidConfiguration,
                "candidate cannot advance after promotion freeze");
        }
        return CurrentCandidate();
    }

    void RestoreCandidateReady() noexcept {
        try {
            std::lock_guard<std::mutex> lock(snapshot_mutex_);
            if (snapshot_.error == OnlineRecoveryErrorV1::kNone &&
                !snapshot_.promotion_boundary_ready &&
                !snapshot_.promoted) {
                snapshot_.candidate_boundary_ready = true;
                snapshot_.phase =
                    OnlineRecoveryPhaseV1::kCandidateReady;
            }
        } catch (...) {
        }
    }

    [[nodiscard]] OnlineRecoveryCandidateV1 CandidateFailure(
        OnlineRecoveryErrorV1 error,
        std::string_view detail) noexcept {
        Fail(error, detail, nullptr);
        OnlineRecoveryCandidateV1 result = CurrentCandidate();
        result.error = error;
        result.candidate_ready = false;
        try {
            result.detail.assign(detail);
        } catch (...) {
        }
        return result;
    }

    [[nodiscard]] OnlineRecoveryCandidateV1 CandidateStateFailure(
        OnlineRecoveryErrorV1 error,
        std::string_view detail) const noexcept {
        OnlineRecoveryCandidateV1 result = CurrentCandidate();
        result.error = error;
        result.candidate_ready = false;
        try {
            result.detail.assign(detail);
        } catch (...) {
        }
        return result;
    }

    [[nodiscard]] OnlineRecoveryCandidateAdvanceV1
    CandidateAdvanceIdle() const noexcept {
        OnlineRecoveryCandidateAdvanceV1 result{};
        result.disposition =
            OnlineRecoveryCandidateAdvanceDispositionV1::kIdle;
        result.error = OnlineRecoveryErrorV1::kNone;
        result.candidate = CurrentCandidate();
        return result;
    }

    [[nodiscard]] OnlineRecoveryCandidateAdvanceV1
    CandidateAdvanceFailure(
        OnlineRecoveryErrorV1 error,
        std::string_view detail) noexcept {
        Fail(error, detail, nullptr);
        OnlineRecoveryCandidateAdvanceV1 result{};
        result.disposition =
            OnlineRecoveryCandidateAdvanceDispositionV1::kFailed;
        result.error = error;
        result.candidate = CurrentCandidate();
        result.candidate.error = error;
        result.candidate.candidate_ready = false;
        try {
            result.detail.assign(detail);
            result.candidate.detail.assign(detail);
        } catch (...) {
        }
        return result;
    }

    [[nodiscard]] OnlineRecoveryCandidateAdvanceV1
    CandidateAdvanceStateFailure(
        OnlineRecoveryErrorV1 error,
        std::string_view detail) const noexcept {
        OnlineRecoveryCandidateAdvanceV1 result{};
        result.disposition =
            OnlineRecoveryCandidateAdvanceDispositionV1::kFailed;
        result.error = error;
        result.candidate = CurrentCandidate();
        result.candidate.error = error;
        result.candidate.candidate_ready = false;
        try {
            result.detail.assign(detail);
            result.candidate.detail.assign(detail);
        } catch (...) {
        }
        return result;
    }

    void Fail(
        OnlineRecoveryErrorV1 error,
        std::string_view message,
        std::string* detail) noexcept {
        try {
            std::lock_guard<std::mutex> lock(snapshot_mutex_);
            if (snapshot_.error == OnlineRecoveryErrorV1::kNone) {
                snapshot_.error = error;
            }
            snapshot_.candidate_boundary_ready = false;
            snapshot_.phase = OnlineRecoveryPhaseV1::kFailed;
        } catch (...) {
        }
        SetDetail(detail, message);
    }

    [[nodiscard]] OnlineRecoveryBoundaryV1 BoundaryFailure(
        OnlineRecoveryErrorV1 error,
        std::string_view detail) noexcept {
        Fail(error, detail, nullptr);
        OnlineRecoveryBoundaryV1 result{};
        result.error = error;
        try {
            result.detail.assign(detail);
        } catch (...) {
        }
        return result;
    }

    [[nodiscard]] OnlineRecoveryPumpResultV1 PumpFailure(
        OnlineRecoveryErrorV1 error,
        std::string_view detail) noexcept {
        Fail(error, detail, nullptr);
        OnlineRecoveryPumpResultV1 result{};
        result.disposition = OnlineRecoveryPumpDispositionV1::kFailed;
        result.error = error;
        try {
            result.detail.assign(detail);
        } catch (...) {
        }
        return result;
    }

    [[nodiscard]] OnlineRecoveryPumpResultV1 PumpStateFailure(
        OnlineRecoveryErrorV1 error,
        std::string_view detail) const noexcept {
        OnlineRecoveryPumpResultV1 result{};
        result.disposition = OnlineRecoveryPumpDispositionV1::kFailed;
        result.error = error;
        try {
            result.detail.assign(detail);
        } catch (...) {
        }
        return result;
    }

    OnlineRecoveryConfigV1 config_;
    std::unique_ptr<MdlLiveJournalReaderV1> reader_;
    std::array<std::unique_ptr<market::MarketDecoderV1>,
               market::kRealtimeHistorySourceCountV1>
        fingerprint_decoders_{};
    std::array<std::uint64_t,
               market::kRealtimeHistorySourceCountV1>
        fingerprint_source_sequences_{};
    std::array<RetainedSequenceHeap,
               sdk::kProductionMessageCountV1>
        retained_sequences_{};
    std::array<std::unordered_map<std::uint64_t, Fingerprint>,
               sdk::kProductionMessageCountV1>
        fingerprints_{};
    std::array<std::uint64_t, sdk::kProductionMessageCountV1>
        replay_cutoff_{};
    std::array<bool, sdk::kProductionMessageCountV1>
        replay_cutoff_seen_{};
    std::array<std::uint64_t, sdk::kProductionMessageCountV1>
        tuple_fence_serials_{};
    std::array<bool, sdk::kProductionMessageCountV1>
        tuple_fence_captured_{};
    std::array<HandoffPhase, sdk::kProductionMessageCountV1> phases_{};
    std::chrono::steady_clock::time_point warmup_deadline_{};
    bool warmup_deadline_initialized_ = false;
    std::uint64_t last_preview_accepted_sequence_ = 0U;
    std::size_t bulk_records_in_quantum_ = 0U;
    std::uint32_t maximum_certified_pressure_in_quantum_ = 0U;
    bool preview_activity_in_quantum_ = false;
    bool preview_sample_seen_ = false;
    // These are single-consumer governor states.  They intentionally survive
    // a bounded PumpNext deadline so a high-water episode must still drain
    // through low water on the next tail poll.
    bool preview_draining_ = false;
    bool shadow_draining_ = false;
    std::atomic<std::int64_t> clean_shutdown_tail_deadline_ns_{0};
    std::atomic<bool> clean_shutdown_tail_drain_{false};
    mutable std::mutex snapshot_mutex_;
    OnlineRecoverySnapshotV1 snapshot_{};
};

OnlineRecoveryHandoffV1::OnlineRecoveryHandoffV1(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

OnlineRecoveryHandoffV1::~OnlineRecoveryHandoffV1() = default;

OnlineRecoveryErrorV1 OnlineRecoveryHandoffV1::Create(
    OnlineRecoveryConfigV1 config,
    std::unique_ptr<OnlineRecoveryHandoffV1>* output,
    std::string* detail) noexcept {
    if (output == nullptr || !ConfigValid(config)) {
        SetDetail(detail, "invalid online recovery configuration");
        return OnlineRecoveryErrorV1::kInvalidConfiguration;
    }
    output->reset();
    try {
        auto impl = std::make_unique<Impl>(std::move(config));
        const OnlineRecoveryErrorV1 initialize_error =
            impl->Initialize(detail);
        if (initialize_error != OnlineRecoveryErrorV1::kNone) {
            return initialize_error;
        }
        output->reset(new OnlineRecoveryHandoffV1(std::move(impl)));
        SetDetail(detail, {});
        return OnlineRecoveryErrorV1::kNone;
    } catch (const std::bad_alloc&) {
        SetDetail(detail, "online recovery allocation failed");
        return OnlineRecoveryErrorV1::kResourceExhausted;
    } catch (...) {
        SetDetail(detail, "online recovery create failed unexpectedly");
        return OnlineRecoveryErrorV1::kUnexpectedFailure;
    }
}

OnlineRecoveryCandidateV1
OnlineRecoveryHandoffV1::PrepareInitialCandidate(
    std::chrono::steady_clock::time_point absolute_deadline) noexcept {
    if (impl_ != nullptr) {
        return impl_->PrepareInitialCandidate(*this, absolute_deadline);
    }
    OnlineRecoveryCandidateV1 result{};
    result.error = OnlineRecoveryErrorV1::kInvalidConfiguration;
    return result;
}

OnlineRecoveryCandidateAdvanceV1
OnlineRecoveryHandoffV1::CatchUpOneBeforePromotion(
    std::chrono::steady_clock::time_point operation_deadline) noexcept {
    if (impl_ != nullptr) {
        return impl_->CatchUpOneBeforePromotion(operation_deadline);
    }
    OnlineRecoveryCandidateAdvanceV1 result{};
    result.disposition =
        OnlineRecoveryCandidateAdvanceDispositionV1::kFailed;
    result.error = OnlineRecoveryErrorV1::kInvalidConfiguration;
    result.candidate.error =
        OnlineRecoveryErrorV1::kInvalidConfiguration;
    return result;
}

OnlineRecoveryBoundaryV1
OnlineRecoveryHandoffV1::FreezePromotionBoundary() noexcept {
    if (impl_ != nullptr) {
        return impl_->FreezePromotionBoundary();
    }
    OnlineRecoveryBoundaryV1 result{};
    result.error = OnlineRecoveryErrorV1::kInvalidConfiguration;
    return result;
}

OnlineRecoveryBoundaryV1
OnlineRecoveryHandoffV1::RecoverToPromotionBoundary() noexcept {
    if (impl_ != nullptr) {
        return impl_->Recover(*this);
    }
    OnlineRecoveryBoundaryV1 result{};
    result.error = OnlineRecoveryErrorV1::kInvalidConfiguration;
    return result;
}

OnlineRecoveryPumpResultV1 OnlineRecoveryHandoffV1::PumpNext(
    std::chrono::steady_clock::time_point deadline) noexcept {
    if (impl_ != nullptr) {
        return impl_->PumpNext(deadline);
    }
    OnlineRecoveryPumpResultV1 result{};
    result.disposition = OnlineRecoveryPumpDispositionV1::kFailed;
    result.error = OnlineRecoveryErrorV1::kInvalidConfiguration;
    return result;
}

bool OnlineRecoveryHandoffV1::MarkPromoted(
    std::uint64_t promotion_realtime_ns) noexcept {
    return impl_ != nullptr && impl_->MarkPromoted(promotion_realtime_ns);
}

bool OnlineRecoveryHandoffV1::BeginCleanShutdownTailDrain(
    std::chrono::steady_clock::time_point deadline) noexcept {
    return impl_ != nullptr &&
           impl_->BeginCleanShutdownTailDrain(deadline);
}

OnlineRecoverySnapshotV1 OnlineRecoveryHandoffV1::Snapshot() const noexcept {
    return impl_ == nullptr ? OnlineRecoverySnapshotV1{} : impl_->Snapshot();
}

bool OnlineRecoveryHandoffV1::CaptureTupleFence(
    const sdk::MessageKey& key,
    std::string* detail) noexcept {
    return impl_ != nullptr && impl_->CaptureTupleFence(key, detail);
}

bool OnlineRecoveryHandoffV1::CooperativeCheckpoint(
    std::string* detail) noexcept {
    return impl_ != nullptr && impl_->CooperativeCheckpoint(detail);
}

bool OnlineRecoveryHandoffV1::Publish(
    const StartupReplayPublicationV1& publication,
    std::string* detail) noexcept {
    return impl_ != nullptr && impl_->PublishCsv(publication, detail);
}

}  // namespace l2flow::recovery
