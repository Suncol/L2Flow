#include "l2flow/recovery/online_recovery_v1.h"

#include "l2flow/market/market_types_v1.h"
#include "l2flow/realtime/owned_ingress_message_v1.h"
#include "l2flow/sdk/vendor_head_view.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <limits>
#include <mutex>
#include <optional>
#include <set>
#include <thread>
#include <unordered_map>
#include <utility>

namespace l2flow::recovery {
namespace {

namespace common = l2flow::common;
namespace market = l2flow::market;
namespace realtime = l2flow::realtime;
namespace runtime = l2flow::runtime;
namespace sdk = l2flow::sdk;

constexpr auto kMaximumTimeout = std::chrono::hours(24);

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
        config.warmup_timeout <= std::chrono::nanoseconds::zero() ||
        config.warmup_timeout > kMaximumTimeout ||
        config.per_record_admission_timeout <=
            std::chrono::nanoseconds::zero() ||
        config.per_record_admission_timeout > kMaximumTimeout ||
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
        kUnhealthy,
        kFailed,
    };

    struct Fingerprint final {
        std::uint64_t vendor_sequence_id = 0U;
        bool normalize_shenzhen_snapshot_channel = false;
        common::Sha256Digest digest{};
    };

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

    [[nodiscard]] OnlineRecoveryBoundaryV1 Recover(
        StartupReplaySinkV1& sink) noexcept {
        OnlineRecoveryBoundaryV1 result{};
        try {
            warmup_deadline_ = std::chrono::steady_clock::now() +
                               config_.warmup_timeout;
            if (CancellationRequested()) {
                return BoundaryFailure(
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
                return BoundaryFailure(
                    CurrentError() == OnlineRecoveryErrorV1::kNone
                        ? OnlineRecoveryErrorV1::kReplayFailed
                        : CurrentError(),
                    detail);
            }
            if (!std::all_of(
                    tuple_fence_captured_.begin(),
                    tuple_fence_captured_.end(),
                    [](bool captured) noexcept { return captured; })) {
                return BoundaryFailure(
                    OnlineRecoveryErrorV1::kReplayPublicationInvalid,
                    "online CSV replay did not capture every production tuple fence");
            }
            {
                std::lock_guard<std::mutex> lock(snapshot_mutex_);
                snapshot_.csv_complete = true;
            }
            const LiveJournalSnapshotV1 journal =
                config_.live_journal->Snapshot();
            if (!journal.healthy()) {
                return BoundaryFailure(
                    OnlineRecoveryErrorV1::kJournalFailed,
                    "live journal failed before promotion frontier capture");
            }
            const std::uint64_t frontier = journal.accepted_serial;
            while (LastJournalSerial() < frontier) {
                const OnlineRecoveryPumpResultV1 pumped =
                    PumpNextInternal(warmup_deadline_, true);
                if (pumped.disposition !=
                    OnlineRecoveryPumpDispositionV1::kRecord) {
                    return BoundaryFailure(
                        pumped.error == OnlineRecoveryErrorV1::kNone
                            ? OnlineRecoveryErrorV1::kWarmupTimeout
                            : pumped.error,
                        pumped.detail.empty()
                            ? "journal catch-up did not reach the promotion frontier"
                            : pumped.detail);
                }
            }
            const runtime::RealtimePipelineSnapshotV1 shadow =
                config_.shadow_pipeline->Snapshot();
            if (shadow.fatal || shadow.stopped ||
                shadow.processing_progress.applied_sequence >
                    shadow.processing_progress.accepted_sequence) {
                return BoundaryFailure(
                    OnlineRecoveryErrorV1::kShadowAdmissionFailed,
                    "shadow pipeline failed before promotion barrier");
            }
            {
                std::lock_guard<std::mutex> lock(snapshot_mutex_);
                snapshot_.promotion_journal_frontier = frontier;
                snapshot_.promotion_shadow_ingress_frontier =
                    shadow.processing_progress.accepted_sequence;
                snapshot_.promotion_boundary_ready = true;
            }
            result.error = OnlineRecoveryErrorV1::kNone;
            result.journal_frontier = frontier;
            result.shadow_ingress_frontier =
                shadow.processing_progress.accepted_sequence;
            result.boundary_ready = true;
            return result;
        } catch (const std::bad_alloc&) {
            return BoundaryFailure(
                OnlineRecoveryErrorV1::kResourceExhausted,
                "online recovery allocation failed");
        } catch (...) {
            return BoundaryFailure(
                OnlineRecoveryErrorV1::kUnexpectedFailure,
                "online recovery failed unexpectedly");
        }
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
            const ThrottleResult throttle =
                Throttle(true, warmup_deadline_);
            if (throttle != ThrottleResult::kReady) {
                Fail(
                    throttle == ThrottleResult::kCancelled
                        ? OnlineRecoveryErrorV1::kCancelled
                        : throttle == ThrottleResult::kUnhealthy
                        ? OnlineRecoveryErrorV1::kCertifiedFailed
                        : throttle == ThrottleResult::kDeadline
                        ? OnlineRecoveryErrorV1::kWarmupTimeout
                        : OnlineRecoveryErrorV1::kUnexpectedFailure,
                    "CSV replay could not pass the CERTIFIED pressure gate",
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
            const runtime::RealtimePipelineIngressResultV1 ingress =
                IngestShadow(
                    publication.message,
                    realtime_ns,
                    monotonic_ns,
                    notices);
            if (ingress.error !=
                    runtime::RealtimePipelineIngressErrorV1::kNone &&
                ingress.error !=
                    runtime::RealtimePipelineIngressErrorV1::
                        kFilteredNonAShare) {
                Fail(
                    OnlineRecoveryErrorV1::kShadowAdmissionFailed,
                    "shadow rejected a CSV replay publication",
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

    [[nodiscard]] OnlineRecoveryPumpResultV1 PumpNext(
        std::chrono::steady_clock::time_point deadline) noexcept {
        return PumpNextInternal(deadline, false);
    }

    [[nodiscard]] bool MarkPromoted(
        std::uint64_t promotion_realtime_ns) noexcept {
        if (promotion_realtime_ns == 0U) {
            return false;
        }
        try {
            std::lock_guard<std::mutex> lock(snapshot_mutex_);
            if (!snapshot_.promotion_boundary_ready || snapshot_.promoted ||
                snapshot_.error != OnlineRecoveryErrorV1::kNone) {
                return false;
            }
            snapshot_.promotion_realtime_ns = promotion_realtime_ns;
            snapshot_.promoted = true;
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
        bool recovery_phase) noexcept {
        OnlineRecoveryPumpResultV1 result{};
        if (CancellationRequested()) {
            result.disposition = recovery_phase
                                     ? OnlineRecoveryPumpDispositionV1::
                                           kFailed
                                     : OnlineRecoveryPumpDispositionV1::kEnd;
            result.error = recovery_phase
                               ? OnlineRecoveryErrorV1::kCancelled
                               : OnlineRecoveryErrorV1::kNone;
            result.detail = "online recovery was cancelled";
            if (recovery_phase) {
                Fail(result.error, result.detail, nullptr);
            }
            return result;
        }
        const ThrottleResult throttle = Throttle(false, deadline);
        if (throttle == ThrottleResult::kDeadline) {
            result.disposition = OnlineRecoveryPumpDispositionV1::kIdle;
            return result;
        }
        if (throttle == ThrottleResult::kCancelled) {
            result.disposition = recovery_phase
                                     ? OnlineRecoveryPumpDispositionV1::
                                           kFailed
                                     : OnlineRecoveryPumpDispositionV1::kEnd;
            result.error = recovery_phase
                               ? OnlineRecoveryErrorV1::kCancelled
                               : OnlineRecoveryErrorV1::kNone;
            return result;
        }
        if (throttle == ThrottleResult::kUnhealthy) {
            return PumpFailure(
                OnlineRecoveryErrorV1::kCertifiedFailed,
                "CERTIFIED handoff became terminally unhealthy");
        }
        if (throttle != ThrottleResult::kReady) {
            return PumpFailure(
                OnlineRecoveryErrorV1::kUnexpectedFailure,
                "CERTIFIED pressure provider failed");
        }
        LiveJournalReadResultV1 read = reader_->ReadNext(deadline);
        if (read.disposition == LiveJournalReadDispositionV1::kTimeout) {
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
        common::Sha256Digest digest{};
        if (!SemanticDigest(
                inspection,
                replay != nullptr &&
                    replay->normalize_shenzhen_snapshot_channel,
                &digest)) {
            return PumpFailure(
                OnlineRecoveryErrorV1::kJournalReadFailed,
                "live journal semantic fingerprint failed");
        }
        HandoffPhase& phase = phases_[*tuple];
        if (replay != nullptr) {
            if (phase == HandoffPhase::kLiveSuffix ||
                replay->digest != digest) {
                return PumpFailure(
                    OnlineRecoveryErrorV1::kOverlapConflict,
                    phase == HandoffPhase::kLiveSuffix
                        ? "live journal returned to the CSV prefix after the live suffix"
                        : "CSV/live tuple/SequenceID payloads conflict");
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
        const runtime::RealtimePipelineIngressResultV1 ingress =
            IngestShadow(
                &record,
                record.recv_realtime_ns(),
                record.recv_monotonic_ns(),
                0U);
        if (ingress.error !=
                runtime::RealtimePipelineIngressErrorV1::kNone &&
            ingress.error !=
                runtime::RealtimePipelineIngressErrorV1::
                    kFilteredNonAShare) {
            return PumpFailure(
                OnlineRecoveryErrorV1::kShadowAdmissionFailed,
                "shadow rejected a journal suffix record");
        }
        {
            std::lock_guard<std::mutex> lock(snapshot_mutex_);
            ++snapshot_.journal_records_read;
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
        std::uint64_t notices) noexcept {
        runtime::RealtimePipelineExternalIngressV1 input{};
        input.message = message;
        input.recv_realtime_ns = realtime_ns;
        input.recv_monotonic_ns = monotonic_ns;
        input.additional_market_notices = notices;
        input.admission_timeout = config_.per_record_admission_timeout;
        return config_.shadow_pipeline->IngestExternalMessage(input);
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
        retained_sequences_[tuple].insert(
            fingerprint.vendor_sequence_id);
        if (retained_sequences_[tuple].size() >
            config_.overlap_retention_per_tuple) {
            const auto smallest = retained_sequences_[tuple].begin();
            fingerprints_[tuple].erase(*smallest);
            retained_sequences_[tuple].erase(smallest);
        }
    }

    [[nodiscard]] ThrottleResult Throttle(
        bool csv,
        std::chrono::steady_clock::time_point deadline) noexcept {
        if (!config_.certified_queue_utilization_percent &&
            !config_.certified_handoff_healthy) {
            return ThrottleResult::kReady;
        }
        bool pause_counted = false;
        for (;;) {
            if (CancellationRequested()) {
                return ThrottleResult::kCancelled;
            }
            if (std::chrono::steady_clock::now() >= deadline) {
                return ThrottleResult::kDeadline;
            }
            if (config_.certified_handoff_healthy) {
                try {
                    if (!config_.certified_handoff_healthy()) {
                        return ThrottleResult::kUnhealthy;
                    }
                } catch (...) {
                    return ThrottleResult::kFailed;
                }
            }
            if (!config_.certified_queue_utilization_percent) {
                return ThrottleResult::kReady;
            }
            std::uint32_t pressure = 0U;
            try {
                pressure =
                    config_.certified_queue_utilization_percent();
            } catch (...) {
                return ThrottleResult::kFailed;
            }
            if (pressure > 100U) {
                return ThrottleResult::kFailed;
            }
            if (pressure >=
                config_.certified_pause_watermark_percent) {
                {
                    std::lock_guard<std::mutex> lock(snapshot_mutex_);
                    if (!pause_counted) {
                        ++snapshot_.replay_pause_events;
                        pause_counted = true;
                    }
                }
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(1));
                continue;
            }
            if (!csv ||
                pressure < config_.certified_low_watermark_percent) {
                return ThrottleResult::kReady;
            }
            {
                std::lock_guard<std::mutex> lock(snapshot_mutex_);
                ++snapshot_.replay_throttle_events;
            }
            std::this_thread::sleep_for(
                pressure >= config_.certified_high_watermark_percent
                    ? std::chrono::microseconds(500)
                    : std::chrono::microseconds(50));
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

    void Fail(
        OnlineRecoveryErrorV1 error,
        std::string_view message,
        std::string* detail) noexcept {
        try {
            std::lock_guard<std::mutex> lock(snapshot_mutex_);
            if (snapshot_.error == OnlineRecoveryErrorV1::kNone) {
                snapshot_.error = error;
            }
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

    OnlineRecoveryConfigV1 config_;
    std::unique_ptr<MdlLiveJournalReaderV1> reader_;
    std::array<std::unique_ptr<market::MarketDecoderV1>,
               market::kRealtimeHistorySourceCountV1>
        fingerprint_decoders_{};
    std::array<std::uint64_t,
               market::kRealtimeHistorySourceCountV1>
        fingerprint_source_sequences_{};
    std::array<std::set<std::uint64_t>,
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

OnlineRecoverySnapshotV1 OnlineRecoveryHandoffV1::Snapshot() const noexcept {
    return impl_ == nullptr ? OnlineRecoverySnapshotV1{} : impl_->Snapshot();
}

bool OnlineRecoveryHandoffV1::CaptureTupleFence(
    const sdk::MessageKey& key,
    std::string* detail) noexcept {
    return impl_ != nullptr && impl_->CaptureTupleFence(key, detail);
}

bool OnlineRecoveryHandoffV1::Publish(
    const StartupReplayPublicationV1& publication,
    std::string* detail) noexcept {
    return impl_ != nullptr && impl_->PublishCsv(publication, detail);
}

}  // namespace l2flow::recovery
