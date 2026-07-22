#include "l2flow/runtime/production_source_pipeline_v1.h"

#include "l2flow/common/identity128.h"
#include "l2flow/ingress/raw_schema.h"
#include "l2flow/market/instrument_registry.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <limits>
#include <mutex>
#include <new>
#include <tuple>
#include <utility>

namespace l2flow::runtime {
namespace {

using l2flow::canonical::CanonicalBundleErrorV1;
using l2flow::canonical::CanonicalNoOutputReasonV1;
using l2flow::canonical::CanonicalRawContextV1;
using l2flow::canonical::ClockEpochIdentityV1;
using l2flow::canonical::SourceFrontierErrorV1;
using l2flow::canonical::SourceFrontierV1;
using l2flow::control::ControlProcessErrorV1;
using l2flow::ingress::RawLiveRecord;
using l2flow::market::MarketDecodeErrorV1;
using l2flow::market::MarketMessageViewV1;

constexpr std::uint8_t kApiServiceId = 1U;
constexpr std::uint8_t kSysServiceId = 2U;

[[nodiscard]] bool DigestNonzero(
    const l2flow::common::Sha256Digest& digest) noexcept {
    return std::any_of(
        digest.begin(), digest.end(), [](std::byte value) {
            return value != std::byte{0U};
        });
}

[[nodiscard]] ClockEpochIdentityV1 SegmentClockEpoch(
    const l2flow::ingress::SegmentHeaderV1& segment) noexcept {
    return ClockEpochIdentityV1{
        segment.clock_epoch_algorithm,
        segment.clock_epoch_digest,
        segment.clock_epoch_label};
}

[[nodiscard]] l2flow::sdk::MessageKey MessageKeyOf(
    const MarketMessageViewV1& message) noexcept {
    return l2flow::sdk::MessageKey{
        message.service_id,
        message.service_version,
        message.message_id};
}

[[nodiscard]] bool ContainsKey(
    const std::vector<l2flow::sdk::MessageKey>& keys,
    const l2flow::sdk::MessageKey& wanted) noexcept {
    return std::find(keys.begin(), keys.end(), wanted) != keys.end();
}

[[nodiscard]] bool ContainsTupleIgnoringVersion(
    const std::vector<l2flow::sdk::MessageKey>& keys,
    const l2flow::sdk::MessageKey& wanted) noexcept {
    return std::any_of(
        keys.begin(), keys.end(), [&wanted](const auto& key) {
            return key.service_id == wanted.service_id &&
                   key.message_id == wanted.message_id;
        });
}

[[nodiscard]] bool ControlManifestMatchesSpec(
    const l2flow::control::ControlDecoderSnapshotV1& control,
    const l2flow::sdk::IngressSpec& spec) noexcept {
    std::size_t required_count = 0U;
    for (const auto& subscription : control.subscriptions) {
        if (subscription.policy ==
            l2flow::control::SubscriptionPolicyV1::kRequired) {
            ++required_count;
            if (!ContainsKey(spec.required, subscription.key)) {
                return false;
            }
        } else if (subscription.policy ==
                   l2flow::control::SubscriptionPolicyV1::kOptional) {
            if (!ContainsKey(spec.optional, subscription.key)) {
                return false;
            }
        } else {
            return false;
        }
    }
    if (required_count != spec.required.size()) {
        return false;
    }
    return std::all_of(
        spec.required.begin(), spec.required.end(),
        [&control](const l2flow::sdk::MessageKey& required) noexcept {
            return std::count_if(
                       control.subscriptions.begin(),
                       control.subscriptions.end(),
                       [&required](const auto& subscription) noexcept {
                           return subscription.policy == l2flow::control::
                                      SubscriptionPolicyV1::kRequired &&
                                  subscription.key == required;
                       }) == 1;
        });
}

[[nodiscard]] bool IsRecordableDecodeError(
    MarketDecodeErrorV1 error) noexcept {
    switch (error) {
        case MarketDecodeErrorV1::kInvalidInput:
        case MarketDecodeErrorV1::kTruncated:
        case MarketDecodeErrorV1::kOffsetInvalid:
        case MarketDecodeErrorV1::kRangeOverlap:
        case MarketDecodeErrorV1::kCountExceeded:
        case MarketDecodeErrorV1::kCountMismatch:
        case MarketDecodeErrorV1::kTextInvalid:
        case MarketDecodeErrorV1::kFixedPointOverflow:
            return true;
        case MarketDecodeErrorV1::kNone:
        case MarketDecodeErrorV1::kNullOutput:
        case MarketDecodeErrorV1::kInvalidConfiguration:
        case MarketDecodeErrorV1::kUnsupportedMessage:
        case MarketDecodeErrorV1::kUnsupportedServiceVersion:
        case MarketDecodeErrorV1::kPhaseProductLimitExceeded:
        case MarketDecodeErrorV1::kResourceExhausted:
        case MarketDecodeErrorV1::kUnexpectedFailure:
            return false;
    }
    return false;
}

enum class AppendCoverageV1 : std::uint8_t {
    kCovered = 0U,
    kProducerLagging,
    kInconsistent,
};

[[nodiscard]] AppendCoverageV1 RecordAppendCoverage(
    const RawLiveRecord& record,
    const SourceFrontierV1& frontier) noexcept {
    const std::uint64_t sequence =
        record.view.header().ingress_sequence;
    const std::uint64_t wal_end = record.view.record_end_wal_pos();
    if (sequence > frontier.append_ingress_sequence) {
        return AppendCoverageV1::kProducerLagging;
    }
    if (sequence == frontier.append_ingress_sequence) {
        return wal_end <= frontier.append_global_wal_pos
            ? AppendCoverageV1::kCovered
            : AppendCoverageV1::kProducerLagging;
    }
    return wal_end < frontier.append_global_wal_pos
        ? AppendCoverageV1::kCovered
        : AppendCoverageV1::kInconsistent;
}

[[nodiscard]] AppendCoverageV1 TransitionAppendCoverage(
    const l2flow::ingress::RawLiveSegmentTransitionV1& transition,
    const SourceFrontierV1& frontier) noexcept {
    if (transition.next_data_begin_wal_pos >
        frontier.append_global_wal_pos) {
        return AppendCoverageV1::kProducerLagging;
    }
    if (transition.next_data_begin_wal_pos <
        frontier.append_global_wal_pos) {
        return AppendCoverageV1::kCovered;
    }
    return frontier.processed_ingress_sequence ==
                   frontier.append_ingress_sequence
        ? AppendCoverageV1::kCovered
        : AppendCoverageV1::kInconsistent;
}

[[nodiscard]] bool MessageMatchesRecord(
    const MarketMessageViewV1& message,
    const RawLiveRecord& record) noexcept {
    const auto& header = record.view.header();
    const auto body = record.view.vendor_body();
    return message.source_stream_id == header.source_stream_id &&
           message.source_sequence == header.ingress_sequence &&
           message.service_id == header.vendor_service_id &&
           message.service_version == header.vendor_service_version &&
           message.message_id == header.vendor_message_id &&
           message.message_encoding == header.vendor_message_encoding &&
           message.vendor_local_time_raw == header.vendor_local_time_raw &&
           message.vendor_sequence_id == header.vendor_sequence_id &&
           message.recv_realtime_ns ==
               static_cast<std::int64_t>(header.recv_realtime_ns) &&
           message.recv_monotonic_ns ==
               static_cast<std::int64_t>(header.recv_monotonic_ns) &&
           message.body.data() == body.data() &&
           message.body.size() == body.size();
}

}  // namespace

class ProductionSourcePipelineV1::Impl final {
public:
    struct PendingHistory final {
        PendingHistory(
            l2flow::market::OwnedInstrumentEventEnvelopeV1&& value,
            std::uint64_t origin_wal_end_pos) noexcept
            : envelope(std::move(value)), wal_pos(origin_wal_end_pos) {}

        l2flow::market::OwnedInstrumentEventEnvelopeV1 envelope;
        std::uint64_t wal_pos = 0U;
    };

    struct Authenticator final {
        const Impl* owner = nullptr;
        const RawLiveRecord* record = nullptr;
        const MarketMessageViewV1* message = nullptr;
        const l2flow::control::ControlRecordV1* control = nullptr;
        const l2flow::ingress::RawLiveSegmentTransitionV1* transition =
            nullptr;
        l2flow::control::ControlRecordAttributionV1 attribution{};
        CanonicalNoOutputReasonV1 no_output_reason =
            CanonicalNoOutputReasonV1::kOptionalMarketMessage;

        void Clear() noexcept {
            record = nullptr;
            message = nullptr;
            control = nullptr;
            transition = nullptr;
            attribution = {};
        }
    };

    Impl(
        ProductionSourcePipelineConfigV1 pipeline_config,
        const l2flow::sdk::IngressSpec* ingress_spec,
        std::unique_ptr<l2flow::ingress::RawLiveTail> tail,
        std::unique_ptr<l2flow::control::ControlDecoderV1> control,
        std::unique_ptr<l2flow::market::MarketDecoderV1> market,
        std::unique_ptr<l2flow::canonical::CanonicalNormalizerV1> normalizer,
        l2flow::canonical::SourceFrontierPageV1* frontier,
        SourceFrontierV1 initial_frontier,
        std::vector<ProductionCanonicalSinkV1> sink_owners,
        l2flow::market::InstrumentHistoryRuntimeV1* history_runtime,
        ProductionInstrumentRegistryIdentityV1 registry,
        l2flow::common::Sha256Digest stable_config_sha256) noexcept
        : config(std::move(pipeline_config)),
          spec(ingress_spec),
          source_frontier(frontier),
          expected_frontier_identity(std::move(initial_frontier)),
          history(history_runtime),
          sinks(std::move(sink_owners)),
          normalizer_owner(std::move(normalizer)),
          control_decoder(std::move(control)),
          market_decoder(std::move(market)),
          live_tail(std::move(tail)),
          registry_identity(std::move(registry)),
          expected_stable_config_sha256(stable_config_sha256) {
        authenticator.owner = this;
    }

    [[nodiscard]] static bool VerifyMarket(
        void* context,
        const SourceFrontierV1& processed,
        const CanonicalRawContextV1& raw,
        const MarketMessageViewV1& message) noexcept {
        auto* auth = static_cast<Authenticator*>(context);
        return auth != nullptr && auth->owner != nullptr &&
               auth->record != nullptr && auth->message == &message &&
               auth->owner->VerifyRaw(processed, raw) &&
               MessageMatchesRecord(message, *auth->record);
    }

    [[nodiscard]] static bool VerifyControl(
        void* context,
        const SourceFrontierV1& processed,
        const CanonicalRawContextV1& raw,
        const l2flow::control::ControlRecordV1& control,
        std::int64_t recv_realtime_ns,
        std::int64_t recv_monotonic_ns) noexcept {
        auto* auth = static_cast<Authenticator*>(context);
        if (auth == nullptr || auth->owner == nullptr ||
            auth->record == nullptr || auth->control != &control ||
            !auth->owner->VerifyRaw(processed, raw)) {
            return false;
        }
        const auto& header = auth->record->view.header();
        return recv_realtime_ns ==
                   static_cast<std::int64_t>(header.recv_realtime_ns) &&
               recv_monotonic_ns ==
                   static_cast<std::int64_t>(header.recv_monotonic_ns) &&
               control.source_stream_id == header.source_stream_id &&
               control.capture_date == header.capture_date &&
               control.stream_day_id == auth->record->segment.stream_day_id &&
               control.vendor_service_id == header.vendor_service_id &&
               control.vendor_service_version ==
                   header.vendor_service_version &&
               control.vendor_message_id == header.vendor_message_id &&
               control.origin_ingress_sequence == header.ingress_sequence &&
               control.origin_record_end_wal_pos ==
                   auth->record->view.record_end_wal_pos() &&
               control.connection_epoch ==
                   auth->attribution.connection_epoch &&
               control.subscription_epoch ==
                   auth->attribution.subscription_epoch &&
               control.quality_flags == auth->attribution.quality_flags;
    }

    [[nodiscard]] static bool VerifyNoOutput(
        void* context,
        const SourceFrontierV1& processed,
        const CanonicalRawContextV1& raw,
        const MarketMessageViewV1& message,
        CanonicalNoOutputReasonV1 reason) noexcept {
        auto* auth = static_cast<Authenticator*>(context);
        return auth != nullptr && auth->no_output_reason == reason &&
               VerifyMarket(context, processed, raw, message);
    }

    [[nodiscard]] static bool VerifyTransition(
        void* context,
        const SourceFrontierV1& processed,
        const l2flow::ingress::RawLiveSegmentTransitionV1& value) noexcept {
        auto* auth = static_cast<Authenticator*>(context);
        return auth != nullptr && auth->owner != nullptr &&
               auth->transition == &value && value.control_generation != 0U &&
               value.writer_instance == processed.writer_instance &&
               value.previous_segment.source_stream_id ==
                   auth->owner->spec->source_stream_id &&
               value.previous_segment.capture_date ==
                   processed.capture_date &&
               value.previous_segment.stream_day_id ==
                   processed.stream_day_id &&
               value.previous_segment.config_sha256 ==
                   auth->owner->expected_stable_config_sha256 &&
               value.next_segment.config_sha256 ==
                   auth->owner->expected_stable_config_sha256 &&
               value.previous_segment.raw_schema_sha256 ==
                   l2flow::ingress::RawSchemaSha256Digest() &&
               value.next_segment.raw_schema_sha256 ==
                   l2flow::ingress::RawSchemaSha256Digest() &&
               value.previous_segment.sdk_archive_sha256 ==
                   value.next_segment.sdk_archive_sha256 &&
               value.previous_segment.libmdl_api_sha256 ==
                   value.next_segment.libmdl_api_sha256 &&
               value.previous_segment.endpoint_contract_sha256 ==
                   value.next_segment.endpoint_contract_sha256 &&
               value.previous_segment.build_manifest_sha256 ==
                   value.next_segment.build_manifest_sha256 &&
               value.previous_segment.host_uuid ==
                   value.next_segment.host_uuid &&
               value.previous_segment.linux_boot_id ==
                   value.next_segment.linux_boot_id;
    }

    [[nodiscard]] bool VerifyRaw(
        const SourceFrontierV1& processed,
        const CanonicalRawContextV1& raw) const noexcept {
        if (authenticator.record == nullptr ||
            processed.processed_ingress_sequence ==
                std::numeric_limits<std::uint64_t>::max()) {
            return false;
        }
        const RawLiveRecord& current = *authenticator.record;
        const auto& header = current.view.header();
        return header.ingress_sequence ==
                   processed.processed_ingress_sequence + 1U &&
               current.writer_instance == processed.writer_instance &&
               current.segment.source_stream_id ==
                   processed.source_stream_id &&
               current.segment.capture_date == processed.capture_date &&
               current.segment.stream_day_id == processed.stream_day_id &&
               raw.capture_date == header.capture_date &&
               raw.trade_date == config.trade_date &&
               raw.source_stream_id == header.source_stream_id &&
               raw.stream_day_id == current.segment.stream_day_id &&
               raw.source_writer_instance == current.writer_instance &&
               raw.source_generation == config.source_generation &&
               raw.origin_ingress_sequence == header.ingress_sequence &&
               raw.origin_wal_end_pos ==
                   current.view.record_end_wal_pos() &&
               raw.authoritative_connection_epoch ==
                   authenticator.attribution.connection_epoch &&
               raw.upstream_quality_flags ==
                   authenticator.attribution.quality_flags &&
               raw.clock_epoch == SegmentClockEpoch(current.segment);
    }

    [[nodiscard]] CanonicalRawContextV1 MakeRawContext(
        const RawLiveRecord& record,
        const l2flow::control::ControlRecordAttributionV1& attribution)
        const noexcept {
        return CanonicalRawContextV1{
            record.segment.capture_date,
            config.trade_date,
            record.segment.source_stream_id,
            record.segment.stream_day_id,
            record.writer_instance,
            config.source_generation,
            record.view.header().ingress_sequence,
            record.view.record_end_wal_pos(),
            attribution.connection_epoch,
            SegmentClockEpoch(record.segment),
            attribution.quality_flags};
    }

    [[nodiscard]] ProductionSourceStepResultV1 Fail(
        ProductionSourceStepResultV1 result) noexcept {
        result.kind = ProductionSourceStepKindV1::kFatal;
        if (result.failure == ProductionSourceFailureV1::kNone) {
            result.failure = ProductionSourceFailureV1::kUnexpectedFailure;
        }
        LatchFatal(result);
        return result;
    }

    [[nodiscard]] bool ReadValidatedFrontier(
        SourceFrontierV1* frontier,
        ProductionSourceStepResultV1* result) const noexcept {
        if (frontier == nullptr || result == nullptr) {
            return false;
        }
        result->frontier_error = l2flow::canonical::ReadSourceFrontierV1(
            *source_frontier, frontier);
        if (result->frontier_error != SourceFrontierErrorV1::kNone) {
            return false;
        }
        result->ingress_sequence = frontier->processed_ingress_sequence;
        result->wal_pos = frontier->processed_global_wal_pos;
        if (frontier->source_stream_id !=
                expected_frontier_identity.source_stream_id ||
            frontier->capture_date !=
                expected_frontier_identity.capture_date ||
            frontier->stream_day_id !=
                expected_frontier_identity.stream_day_id ||
            frontier->writer_instance !=
                expected_frontier_identity.writer_instance ||
            frontier->generation !=
                expected_frontier_identity.generation ||
            !(frontier->clock_epoch ==
              expected_frontier_identity.clock_epoch)) {
            result->frontier_error =
                SourceFrontierErrorV1::kIdentityChanged;
            return false;
        }
        if (frontier->source_state ==
            l2flow::canonical::SourceStateV1::kFatal) {
            result->frontier_error = SourceFrontierErrorV1::kInvalidState;
            return false;
        }
        return true;
    }

    void LatchFatal(const ProductionSourceStepResultV1& result) noexcept {
        if (fatal.load(std::memory_order_acquire)) {
            return;
        }
        SourceFrontierV1 observed{};
        const bool observed_valid =
            l2flow::canonical::ReadSourceFrontierV1(
                *source_frontier, &observed) ==
                SourceFrontierErrorV1::kNone &&
            observed.writer_instance ==
                expected_frontier_identity.writer_instance &&
            observed.generation == expected_frontier_identity.generation;
        static_cast<void>(l2flow::canonical::PublishSourceStateV1(
            source_frontier,
            expected_frontier_identity.writer_instance,
            expected_frontier_identity.generation,
            l2flow::canonical::SourceStateV1::kFatal,
            observed_valid
                ? observed.quality_flags
                : expected_frontier_identity.quality_flags));
        static_cast<void>(normalizer_owner->FailStop(nullptr));
        for (auto& sink : sinks) {
            if (sink.writer != nullptr) {
                static_cast<void>(sink.writer->MarkGenerationFatal());
            }
        }
        history->MarkSourceFatal(config.source_slot);
        {
            std::lock_guard<std::mutex> lock(terminal_mutex);
            terminal = result;
            terminal.kind = ProductionSourceStepKindV1::kFatal;
        }
        // Release-publish only after terminal and every revocation action are
        // complete, so a Snapshot acquire cannot observe a default terminal.
        fatal.store(true, std::memory_order_release);
    }

    [[nodiscard]] ProductionSourceStepResultV1 RetryHistory() noexcept {
        ProductionSourceStepResultV1 result{};
        if (!pending_history.has_value()) {
            result.kind = ProductionSourceStepKindV1::kProgress;
            return result;
        }
        result.ingress_sequence = pending_history->envelope.source_sequence();
        result.wal_pos = pending_history->wal_pos;
        result.history_submit_error =
            history->TrySubmit(std::move(pending_history->envelope));
        if (result.history_submit_error ==
            l2flow::market::InstrumentHistorySubmitErrorV1::kNone) {
            pending_history.reset();
            history_pending.store(false, std::memory_order_release);
            history_submissions.fetch_add(1U, std::memory_order_relaxed);
            if (history->Frontier(config.source_slot).fatal) {
                result.failure =
                    ProductionSourceFailureV1::kHistoryCommitFailure;
                return Fail(result);
            }
            result.kind = ProductionSourceStepKindV1::kProgress;
            return result;
        }
        if (result.history_submit_error ==
                l2flow::market::InstrumentHistorySubmitErrorV1::kQueueFull ||
            result.history_submit_error == l2flow::market::
                InstrumentHistorySubmitErrorV1::kInflightLimit) {
            result.kind = ProductionSourceStepKindV1::kBackpressure;
            return result;
        }
        result.failure = ProductionSourceFailureV1::kHistorySubmitFailure;
        return Fail(result);
    }

    [[nodiscard]] ProductionSourceStepResultV1 AwaitHistoryDrain()
        noexcept {
        ProductionSourceStepResultV1 result{};
        if (!end_barrier.has_value()) {
            result.failure =
                ProductionSourceFailureV1::kUnexpectedFailure;
            return Fail(result);
        }
        result.ingress_sequence = end_barrier->source_sequence;
        result.history_barrier_error = history->WaitForBarrier(
            *end_barrier, std::chrono::nanoseconds::zero());
        if (result.history_barrier_error == l2flow::market::
                InstrumentHistoryBarrierWaitErrorV1::kTimeout) {
            result.kind = ProductionSourceStepKindV1::kBackpressure;
            return result;
        }
        if (result.history_barrier_error != l2flow::market::
                InstrumentHistoryBarrierWaitErrorV1::kNone) {
            result.failure =
                ProductionSourceFailureV1::kHistoryCommitFailure;
            return Fail(result);
        }
        end_barrier.reset();
        history_draining.store(false, std::memory_order_release);
        ended.store(true, std::memory_order_release);
        result.kind = ProductionSourceStepKindV1::kEnd;
        return result;
    }

    [[nodiscard]] ProductionSourceStepResultV1 ProcessPendingInput()
        noexcept {
        if (pending_record.has_value()) {
            ProductionSourceStepResultV1 result =
                ProcessRecord(*pending_record);
            if (result.kind != ProductionSourceStepKindV1::kWouldBlock) {
                pending_record.reset();
            }
            return result;
        }
        if (pending_transition.has_value()) {
            ProductionSourceStepResultV1 result =
                ProcessTransition(*pending_transition);
            if (result.kind != ProductionSourceStepKindV1::kWouldBlock) {
                pending_transition.reset();
            }
            return result;
        }
        ProductionSourceStepResultV1 result{};
        result.failure = ProductionSourceFailureV1::kUnexpectedFailure;
        return Fail(result);
    }

    [[nodiscard]] ProductionSourceStepResultV1 ProcessTransition(
        const l2flow::ingress::RawLiveSegmentTransitionV1& transition)
        noexcept {
        ProductionSourceStepResultV1 result{};
        result.wal_pos = transition.next_data_begin_wal_pos;
        SourceFrontierV1 frontier{};
        if (!ReadValidatedFrontier(&frontier, &result)) {
            if (result.frontier_error == SourceFrontierErrorV1::kBusy) {
                result.kind = ProductionSourceStepKindV1::kWouldBlock;
                return result;
            }
            result.failure = ProductionSourceFailureV1::kFrontierFailure;
            return Fail(result);
        }
        result.ingress_sequence = frontier.processed_ingress_sequence;
        const AppendCoverageV1 coverage =
            TransitionAppendCoverage(transition, frontier);
        if (coverage == AppendCoverageV1::kProducerLagging) {
            result.kind = ProductionSourceStepKindV1::kWouldBlock;
            return result;
        }
        if (coverage != AppendCoverageV1::kCovered) {
            result.failure = ProductionSourceFailureV1::kFrontierFailure;
            result.frontier_error = SourceFrontierErrorV1::kNotAppended;
            return Fail(result);
        }

        authenticator.Clear();
        authenticator.transition = &transition;
        const auto canonical =
            coordinator->ProcessSegmentTransition(transition);
        authenticator.Clear();
        result.canonical_error = canonical.error;
        if (!canonical.ok()) {
            result.failure = ProductionSourceFailureV1::kCanonicalFailure;
            return Fail(result);
        }
        segment_transitions.fetch_add(1U, std::memory_order_relaxed);
        result.kind = ProductionSourceStepKindV1::kProgress;
        return result;
    }

    [[nodiscard]] ProductionSourceStepResultV1 ProcessRecord(
        const RawLiveRecord& record) noexcept {
        ProductionSourceStepResultV1 result{};
        const auto& header = record.view.header();
        result.ingress_sequence = header.ingress_sequence;
        result.wal_pos = record.view.record_end_wal_pos();

        MarketMessageViewV1 message{};
        result.raw_adapter_error =
            l2flow::market::MakeMarketMessageViewFromRawV1(
                record, config.trade_date, &message);
        if (result.raw_adapter_error !=
            l2flow::market::RawMarketAdapterErrorV1::kNone) {
            result.failure = ProductionSourceFailureV1::kRawAdapterFailure;
            return Fail(result);
        }

        SourceFrontierV1 frontier{};
        if (!ReadValidatedFrontier(&frontier, &result)) {
            if (result.frontier_error == SourceFrontierErrorV1::kBusy) {
                result.kind = ProductionSourceStepKindV1::kWouldBlock;
                return result;
            }
            result.failure = ProductionSourceFailureV1::kFrontierFailure;
            return Fail(result);
        }
        if (frontier.source_stream_id != record.segment.source_stream_id ||
            frontier.capture_date != record.segment.capture_date ||
            frontier.stream_day_id != record.segment.stream_day_id ||
            frontier.writer_instance != record.writer_instance ||
            frontier.generation != config.source_generation ||
            !(frontier.clock_epoch == SegmentClockEpoch(record.segment))) {
            result.failure = ProductionSourceFailureV1::kFrontierFailure;
            result.frontier_error = SourceFrontierErrorV1::kIdentityChanged;
            return Fail(result);
        }
        if (frontier.processed_ingress_sequence ==
                std::numeric_limits<std::uint64_t>::max() ||
            header.ingress_sequence !=
                frontier.processed_ingress_sequence + 1U) {
            result.failure = ProductionSourceFailureV1::kFrontierFailure;
            result.frontier_error =
                SourceFrontierErrorV1::kIngressRegression;
            return Fail(result);
        }
        if (record.view.record_end_wal_pos() <=
            frontier.processed_global_wal_pos) {
            result.failure = ProductionSourceFailureV1::kFrontierFailure;
            result.frontier_error = SourceFrontierErrorV1::kWalRegression;
            return Fail(result);
        }
        const AppendCoverageV1 coverage =
            RecordAppendCoverage(record, frontier);
        if (coverage == AppendCoverageV1::kProducerLagging) {
            result.kind = ProductionSourceStepKindV1::kWouldBlock;
            return result;
        }
        if (coverage != AppendCoverageV1::kCovered) {
            result.failure = ProductionSourceFailureV1::kFrontierFailure;
            result.frontier_error = SourceFrontierErrorV1::kNotAppended;
            return Fail(result);
        }

        const l2flow::control::ControlProcessResultV1 control =
            control_decoder->Process(record);
        result.control_error = control.error;
        if (!control.cursor_committed) {
            result.failure =
                ProductionSourceFailureV1::kControlDecoderFailure;
            return Fail(result);
        }
        // A malformed API/SYS record carries an auditable ControlRecord and
        // is committed below before fail-stop.  An internal/control-state
        // failure without such a record has no authenticated durable output
        // and must not be disguised as an ordinary no-output message.
        if (control.error != ControlProcessErrorV1::kNone &&
            !control.control_record.has_value()) {
            result.failure =
                ProductionSourceFailureV1::kControlDecoderFailure;
            return Fail(result);
        }

        const CanonicalRawContextV1 raw =
            MakeRawContext(record, control.attribution);
        authenticator.Clear();
        authenticator.record = &record;
        authenticator.message = &message;
        authenticator.attribution = control.attribution;

        l2flow::canonical::CanonicalBundleResultV1 canonical{};
        bool retained_for_history = false;
        bool committed_market = false;
        bool committed_control = false;
        bool committed_no_output = false;
        bool committed_decode_quality = false;
        l2flow::market::RetainedMarketEventV1 retained{};
        if (control.control_record.has_value()) {
            authenticator.control = &*control.control_record;
            canonical = coordinator->ProcessControl(
                raw,
                *control.control_record,
                message.recv_realtime_ns,
                message.recv_monotonic_ns);
            committed_control = true;
        } else if (message.service_id == kApiServiceId ||
                   message.service_id == kSysServiceId) {
            authenticator.no_output_reason =
                CanonicalNoOutputReasonV1::kUnmodeledControlMessage;
            canonical = coordinator->ProcessNoOutput(
                raw, message, authenticator.no_output_reason);
            committed_no_output = true;
        } else {
            const l2flow::sdk::MessageKey key = MessageKeyOf(message);
            if (ContainsKey(spec->optional, key)) {
                authenticator.no_output_reason =
                    CanonicalNoOutputReasonV1::kOptionalMarketMessage;
                canonical = coordinator->ProcessNoOutput(
                    raw, message, authenticator.no_output_reason);
                committed_no_output = true;
            } else if (ContainsKey(spec->required, key) ||
                       ContainsTupleIgnoringVersion(spec->required, key)) {
                l2flow::market::DecodedMarketEventV1 decoded{};
                result.decode_error = market_decoder->Decode(
                    message, &decoded);
                if (result.decode_error == MarketDecodeErrorV1::kNone) {
                    if (!ContainsKey(spec->required, key)) {
                        authenticator.Clear();
                        result.failure =
                            ProductionSourceFailureV1::kUnexpectedMessage;
                        return Fail(result);
                    }
                    result.retain_error =
                        l2flow::market::RetainMarketEventV1(
                            std::move(decoded), &retained);
                    if (result.retain_error != l2flow::market::
                            RetainedMarketEventCreateErrorV1::kNone) {
                        authenticator.Clear();
                        result.failure =
                            ProductionSourceFailureV1::kMarketRetainFailure;
                        return Fail(result);
                    }
                    canonical = coordinator->ProcessDecodedMarket(
                        raw, message, retained);
                    retained_for_history =
                        canonical.normalize.business_record_planned;
                    committed_market = true;
                } else if (IsRecordableDecodeError(result.decode_error)) {
                    canonical = coordinator->ProcessMarketDecodeFailure(
                        raw, message, result.decode_error);
                    committed_decode_quality = true;
                } else {
                    authenticator.Clear();
                    result.failure =
                        ProductionSourceFailureV1::kMarketDecoderFailure;
                    return Fail(result);
                }
            } else {
                authenticator.Clear();
                result.failure =
                    ProductionSourceFailureV1::kUnexpectedMessage;
                return Fail(result);
            }
        }
        authenticator.Clear();

        result.canonical_error = canonical.error;
        if (!canonical.ok()) {
            result.failure = ProductionSourceFailureV1::kCanonicalFailure;
            return Fail(result);
        }
        raw_records.fetch_add(1U, std::memory_order_relaxed);
        if (committed_market) {
            market_records.fetch_add(1U, std::memory_order_relaxed);
        }
        if (committed_control) {
            control_records.fetch_add(1U, std::memory_order_relaxed);
        }
        if (committed_no_output) {
            no_output_records.fetch_add(1U, std::memory_order_relaxed);
        }
        if (committed_decode_quality) {
            decode_quality_records.fetch_add(
                1U, std::memory_order_relaxed);
        }

        if (control.error != ControlProcessErrorV1::kNone ||
            control.control_state_poisoned) {
            result.failure =
                ProductionSourceFailureV1::kControlDecoderFailure;
            return Fail(result);
        }
        if (!retained_for_history) {
            result.kind = ProductionSourceStepKindV1::kProgress;
            return result;
        }

        std::optional<l2flow::market::OwnedInstrumentEventEnvelopeV1>
            history_envelope;
        result.history_create_error =
            l2flow::market::OwnedInstrumentEventEnvelopeV1::Create(
                config.source_slot,
                std::move(retained),
                &history_envelope);
        if (result.history_create_error != l2flow::market::
                OwnedInstrumentEventCreateErrorV1::kNone ||
            !history_envelope.has_value()) {
            result.failure =
                ProductionSourceFailureV1::kHistoryEnvelopeFailure;
            return Fail(result);
        }
        pending_history.emplace(
            std::move(*history_envelope), raw.origin_wal_end_pos);
        history_pending.store(true, std::memory_order_release);
        return RetryHistory();
    }

    [[nodiscard]] ProductionSourceStepResultV1 Step() noexcept {
        if (fatal.load(std::memory_order_acquire)) {
            std::lock_guard<std::mutex> lock(terminal_mutex);
            return terminal;
        }
        SourceFrontierV1 current_frontier{};
        ProductionSourceStepResultV1 frontier_result{};
        if (!ReadValidatedFrontier(
                &current_frontier, &frontier_result)) {
            if (frontier_result.frontier_error ==
                SourceFrontierErrorV1::kBusy) {
                frontier_result.kind =
                    ProductionSourceStepKindV1::kWouldBlock;
                return frontier_result;
            }
            frontier_result.failure =
                ProductionSourceFailureV1::kFrontierFailure;
            return Fail(frontier_result);
        }
        if (history->Frontier(config.source_slot).fatal) {
            ProductionSourceStepResultV1 result{};
            result.failure =
                ProductionSourceFailureV1::kHistoryCommitFailure;
            return Fail(result);
        }
        if (pending_history.has_value()) {
            return RetryHistory();
        }
        if (pending_record.has_value() ||
            pending_transition.has_value()) {
            return ProcessPendingInput();
        }
        if (end_barrier.has_value()) {
            return AwaitHistoryDrain();
        }
        if (ended.load(std::memory_order_acquire)) {
            ProductionSourceStepResultV1 result{};
            result.kind = ProductionSourceStepKindV1::kEnd;
            return result;
        }

        try {
            l2flow::ingress::RawLiveTailStep step = live_tail->Next();
            if (step.kind == l2flow::ingress::RawLiveTailStepKind::kRecord) {
                if (!step.record.has_value()) {
                    ProductionSourceStepResultV1 result{};
                    result.failure = ProductionSourceFailureV1::kRawRecordMissing;
                    return Fail(result);
                }
                pending_record.emplace(std::move(*step.record));
                return ProcessPendingInput();
            }
            if (step.kind == l2flow::ingress::RawLiveTailStepKind::
                    kSegmentTransition) {
                if (!step.segment_transition.has_value()) {
                    ProductionSourceStepResultV1 result{};
                    result.failure = ProductionSourceFailureV1::kRawRecordMissing;
                    return Fail(result);
                }
                pending_transition.emplace(
                    std::move(*step.segment_transition));
                return ProcessPendingInput();
            }
            if (step.kind ==
                l2flow::ingress::RawLiveTailStepKind::kWouldBlock) {
                ProductionSourceStepResultV1 result{};
                result.kind = ProductionSourceStepKindV1::kWouldBlock;
                return result;
            }
            if (step.kind == l2flow::ingress::RawLiveTailStepKind::kEnd) {
                end_barrier = history->CaptureBarrier(config.source_slot);
                history_draining.store(true, std::memory_order_release);
                return AwaitHistoryDrain();
            }
            ProductionSourceStepResultV1 result{};
            result.failure = ProductionSourceFailureV1::kRawTailFailure;
            result.raw_tail_error = step.error;
            return Fail(result);
        } catch (...) {
            ProductionSourceStepResultV1 result{};
            result.failure = ProductionSourceFailureV1::kUnexpectedFailure;
            return Fail(result);
        }
    }

    ProductionSourcePipelineConfigV1 config{};
    const l2flow::sdk::IngressSpec* spec = nullptr;
    l2flow::canonical::SourceFrontierPageV1* source_frontier = nullptr;
    SourceFrontierV1 expected_frontier_identity{};
    l2flow::market::InstrumentHistoryRuntimeV1* history = nullptr;
    std::vector<ProductionCanonicalSinkV1> sinks;
    std::unique_ptr<l2flow::canonical::CanonicalNormalizerV1>
        normalizer_owner;
    Authenticator authenticator{};
    std::unique_ptr<l2flow::canonical::CanonicalBundleCoordinatorV1>
        coordinator;
    std::unique_ptr<l2flow::control::ControlDecoderV1> control_decoder;
    std::unique_ptr<l2flow::market::MarketDecoderV1> market_decoder;
    std::unique_ptr<l2flow::ingress::RawLiveTail> live_tail;
    ProductionInstrumentRegistryIdentityV1 registry_identity{};
    l2flow::common::Sha256Digest expected_stable_config_sha256{};
    std::optional<PendingHistory> pending_history;
    std::optional<l2flow::ingress::RawLiveRecord> pending_record;
    std::optional<l2flow::ingress::RawLiveSegmentTransitionV1>
        pending_transition;
    std::optional<l2flow::market::InstrumentHistoryBarrierV1>
        end_barrier;

    std::atomic<std::uint64_t> raw_records{0U};
    std::atomic<std::uint64_t> market_records{0U};
    std::atomic<std::uint64_t> control_records{0U};
    std::atomic<std::uint64_t> no_output_records{0U};
    std::atomic<std::uint64_t> decode_quality_records{0U};
    std::atomic<std::uint64_t> segment_transitions{0U};
    std::atomic<std::uint64_t> history_submissions{0U};
    std::atomic<bool> history_pending{false};
    std::atomic<bool> history_draining{false};
    std::atomic<bool> ended{false};
    std::atomic<bool> fatal{false};
    mutable std::mutex execution_mutex;
    mutable std::mutex terminal_mutex;
    ProductionSourceStepResultV1 terminal{};
};

std::string_view ProductionSourceCreateErrorNameV1(
    ProductionSourceCreateErrorV1 error) noexcept {
    switch (error) {
        case ProductionSourceCreateErrorV1::kNone:
            return "none";
        case ProductionSourceCreateErrorV1::kNullOutput:
            return "null_output";
        case ProductionSourceCreateErrorV1::kNullDependency:
            return "null_dependency";
        case ProductionSourceCreateErrorV1::kInvalidConfiguration:
            return "invalid_configuration";
        case ProductionSourceCreateErrorV1::kSourceIdentityMismatch:
            return "source_identity_mismatch";
        case ProductionSourceCreateErrorV1::kFrontierMismatch:
            return "frontier_mismatch";
        case ProductionSourceCreateErrorV1::kControlDecoderMismatch:
            return "control_decoder_mismatch";
        case ProductionSourceCreateErrorV1::kMarketDecoderInvalid:
            return "market_decoder_invalid";
        case ProductionSourceCreateErrorV1::kCanonicalCoordinatorCreateFailed:
            return "canonical_coordinator_create_failed";
        case ProductionSourceCreateErrorV1::kResourceExhausted:
            return "resource_exhausted";
    }
    return "invalid_production_source_create_error";
}

std::string_view ProductionSourceFailureNameV1(
    ProductionSourceFailureV1 failure) noexcept {
    switch (failure) {
        case ProductionSourceFailureV1::kNone:
            return "none";
        case ProductionSourceFailureV1::kRawTailFailure:
            return "raw_tail_failure";
        case ProductionSourceFailureV1::kRawRecordMissing:
            return "raw_record_missing";
        case ProductionSourceFailureV1::kRawAdapterFailure:
            return "raw_adapter_failure";
        case ProductionSourceFailureV1::kFrontierFailure:
            return "frontier_failure";
        case ProductionSourceFailureV1::kControlDecoderFailure:
            return "control_decoder_failure";
        case ProductionSourceFailureV1::kUnexpectedMessage:
            return "unexpected_message";
        case ProductionSourceFailureV1::kMarketDecoderFailure:
            return "market_decoder_failure";
        case ProductionSourceFailureV1::kMarketRetainFailure:
            return "market_retain_failure";
        case ProductionSourceFailureV1::kCanonicalFailure:
            return "canonical_failure";
        case ProductionSourceFailureV1::kHistoryEnvelopeFailure:
            return "history_envelope_failure";
        case ProductionSourceFailureV1::kHistorySubmitFailure:
            return "history_submit_failure";
        case ProductionSourceFailureV1::kHistoryCommitFailure:
            return "history_commit_failure";
        case ProductionSourceFailureV1::kLifecycleAbort:
            return "lifecycle_abort";
        case ProductionSourceFailureV1::kCoordinatedFailStop:
            return "coordinated_fail_stop";
        case ProductionSourceFailureV1::kUnexpectedFailure:
            return "unexpected_failure";
    }
    return "invalid_production_source_failure";
}

ProductionSourcePipelineV1::ProductionSourcePipelineV1(
    std::unique_ptr<Impl> impl) noexcept : impl_(std::move(impl)) {}

ProductionSourcePipelineV1::~ProductionSourcePipelineV1() {
    if (impl_ == nullptr) {
        return;
    }
    std::lock_guard<std::mutex> lock(impl_->execution_mutex);
    if (!impl_->fatal.load(std::memory_order_acquire) &&
        !impl_->ended.load(std::memory_order_acquire)) {
        ProductionSourceStepResultV1 result{};
        result.kind = ProductionSourceStepKindV1::kFatal;
        result.failure =
            ProductionSourceFailureV1::kLifecycleAbort;
        impl_->LatchFatal(result);
    }
}

ProductionSourceCreateErrorV1 ProductionSourcePipelineV1::Create(
    ProductionSourcePipelineConfigV1 config,
    std::unique_ptr<l2flow::ingress::RawLiveTail> live_tail,
    std::unique_ptr<l2flow::control::ControlDecoderV1> control_decoder,
    std::unique_ptr<l2flow::canonical::CanonicalNormalizerV1> normalizer,
    l2flow::canonical::SourceFrontierPageV1* source_frontier,
    std::vector<ProductionCanonicalSinkV1> sinks,
    l2flow::market::InstrumentHistoryRuntimeV1* history,
    std::unique_ptr<ProductionSourcePipelineV1>* output) noexcept {
    if (output == nullptr) {
        return ProductionSourceCreateErrorV1::kNullOutput;
    }
    output->reset();
    if (live_tail == nullptr || control_decoder == nullptr ||
        normalizer == nullptr || source_frontier == nullptr ||
        history == nullptr || sinks.empty()) {
        return ProductionSourceCreateErrorV1::kNullDependency;
    }
    if (config.source_slot >=
            l2flow::market::kInstrumentHistorySourceCountV1 ||
        config.trade_date == 0U || config.source_generation == 0U ||
        config.canonical_generation == 0U ||
        !DigestNonzero(config.normalizer_build_sha256) ||
        !DigestNonzero(config.normalizer_config_sha256)) {
        return ProductionSourceCreateErrorV1::kInvalidConfiguration;
    }

    try {
        const l2flow::sdk::IngressSpec& spec =
            l2flow::sdk::GetIngressSpec(config.ingress_kind);
        const auto history_config = history->config();
        const auto history_frontier = history->Frontier(config.source_slot);
        if (history_config.source_stream_ids[config.source_slot] !=
                spec.source_stream_id ||
            live_tail->source_stream_id() != spec.source_stream_id ||
            live_tail->next_ingress_sequence() != 1U ||
            history_frontier.submitted_ticket != 0U ||
            history_frontier.acknowledged_ticket != 0U ||
            history_frontier.submitted_source_sequence != 0U ||
            history_frontier.acknowledged_source_sequence != 0U ||
            history_frontier.completed_out_of_order != 0U ||
            history_frontier.fatal) {
            return ProductionSourceCreateErrorV1::kSourceIdentityMismatch;
        }

        l2flow::canonical::SourceFrontierV1 frontier{};
        if (l2flow::canonical::ReadSourceFrontierV1(
                *source_frontier, &frontier) !=
                SourceFrontierErrorV1::kNone ||
            frontier.source_stream_id != spec.source_stream_id ||
            frontier.capture_date != live_tail->capture_date() ||
            frontier.stream_day_id != live_tail->stream_day_id() ||
            frontier.writer_instance != live_tail->writer_instance() ||
            frontier.generation != config.source_generation ||
            frontier.processed_ingress_sequence != 0U ||
            frontier.processed_global_wal_pos !=
                live_tail->initial_global_wal_pos() ||
            frontier.source_state ==
                l2flow::canonical::SourceStateV1::kFatal) {
            return ProductionSourceCreateErrorV1::kFrontierMismatch;
        }

        const auto& normalizer_config = normalizer->config();
        if (normalizer_config.source_stream_id != spec.source_stream_id ||
            normalizer_config.capture_date != live_tail->capture_date() ||
            normalizer_config.trade_date != config.trade_date ||
            normalizer_config.stream_day_id != live_tail->stream_day_id() ||
            normalizer_config.instrument_registry == nullptr) {
            return ProductionSourceCreateErrorV1::kInvalidConfiguration;
        }
        const auto control = control_decoder->Snapshot();
        if (control.source_stream_id != spec.source_stream_id ||
            control.capture_date != live_tail->capture_date() ||
            control.stream_day_id != live_tail->stream_day_id() ||
            control.next_ingress_sequence != 1U ||
            control.processed_ingress_sequence != 0U ||
            !ControlManifestMatchesSpec(control, spec)) {
            return ProductionSourceCreateErrorV1::kControlDecoderMismatch;
        }

        l2flow::market::MarketDecoderConfigV1 decoder_config{};
        decoder_config.trade_date = config.trade_date;
        decoder_config.source_stream_id = spec.source_stream_id;
        decoder_config.instrument_registry =
            normalizer_config.instrument_registry;
        decoder_config.shanghai_phase_attribution = l2flow::market::
            ShanghaiPhaseAttributionModeV1::kDeferred;
        decoder_config.limits = normalizer_config.decoder_limits;
        auto market_decoder = std::make_unique<
            l2flow::market::MarketDecoderV1>(decoder_config);
        if (!market_decoder->configuration_valid()) {
            return ProductionSourceCreateErrorV1::kMarketDecoderInvalid;
        }

        auto impl = std::make_unique<Impl>(
            config,
            &spec,
            std::move(live_tail),
            std::move(control_decoder),
            std::move(market_decoder),
            std::move(normalizer),
            source_frontier,
            frontier,
            std::move(sinks),
            history,
            ProductionInstrumentRegistryIdentityV1{
                normalizer_config.instrument_registry->registry_version(),
                normalizer_config.instrument_registry->registry_sha256()},
            control.stable_config_sha256);

        l2flow::canonical::CanonicalBundleCoordinatorConfigV1
            coordinator_config{};
        coordinator_config.normalizer = impl->normalizer_owner.get();
        coordinator_config.source_frontier = source_frontier;
        coordinator_config.canonical_generation =
            config.canonical_generation;
        coordinator_config.normalizer_build_sha256 =
            config.normalizer_build_sha256;
        coordinator_config.normalizer_config_sha256 =
            config.normalizer_config_sha256;
        coordinator_config.market_envelope_verifier = &Impl::VerifyMarket;
        coordinator_config.control_envelope_verifier = &Impl::VerifyControl;
        coordinator_config.no_output_envelope_verifier =
            &Impl::VerifyNoOutput;
        coordinator_config.segment_transition_verifier =
            &Impl::VerifyTransition;
        coordinator_config.envelope_verifier_context =
            &impl->authenticator;
        coordinator_config.operation_hook = config.operation_hook;
        coordinator_config.operation_hook_context =
            config.operation_hook_context;
        coordinator_config.sinks.reserve(impl->sinks.size());
        for (const auto& sink : impl->sinks) {
            if (sink.writer == nullptr) {
                return ProductionSourceCreateErrorV1::kNullDependency;
            }
            coordinator_config.sinks.push_back(
                l2flow::canonical::CanonicalBundleSinkV1{
                    sink.family, sink.shard, sink.writer.get()});
        }
        if (l2flow::canonical::CanonicalBundleCoordinatorV1::Create(
                std::move(coordinator_config), &impl->coordinator) !=
            CanonicalBundleErrorV1::kNone) {
            return ProductionSourceCreateErrorV1::
                kCanonicalCoordinatorCreateFailed;
        }
        output->reset(new ProductionSourcePipelineV1(std::move(impl)));
        return ProductionSourceCreateErrorV1::kNone;
    } catch (const std::bad_alloc&) {
        return ProductionSourceCreateErrorV1::kResourceExhausted;
    } catch (...) {
        return ProductionSourceCreateErrorV1::kInvalidConfiguration;
    }
}

ProductionSourceStepResultV1 ProductionSourcePipelineV1::Step() noexcept {
    std::lock_guard<std::mutex> lock(impl_->execution_mutex);
    return impl_->Step();
}

ProductionSourceSnapshotV1 ProductionSourcePipelineV1::Snapshot()
    const noexcept {
    ProductionSourceSnapshotV1 result{};
    result.raw_records = impl_->raw_records.load(std::memory_order_acquire);
    result.market_records =
        impl_->market_records.load(std::memory_order_acquire);
    result.control_records =
        impl_->control_records.load(std::memory_order_acquire);
    result.no_output_records =
        impl_->no_output_records.load(std::memory_order_acquire);
    result.decode_quality_records =
        impl_->decode_quality_records.load(std::memory_order_acquire);
    result.segment_transitions =
        impl_->segment_transitions.load(std::memory_order_acquire);
    result.history_submissions =
        impl_->history_submissions.load(std::memory_order_acquire);
    result.history_frontier =
        impl_->history->Frontier(impl_->config.source_slot);
    result.history_pending =
        impl_->history_pending.load(std::memory_order_acquire);
    result.history_draining =
        impl_->history_draining.load(std::memory_order_acquire);
    result.ended = impl_->ended.load(std::memory_order_acquire);
    result.fatal = impl_->fatal.load(std::memory_order_acquire);
    if (result.fatal) {
        std::lock_guard<std::mutex> lock(impl_->terminal_mutex);
        result.terminal = impl_->terminal;
    }
    return result;
}

l2flow::control::ControlDecoderSnapshotV1
ProductionSourcePipelineV1::ControlSnapshot() const {
    return impl_->control_decoder->Snapshot();
}

l2flow::canonical::SourceFrontierV1
ProductionSourcePipelineV1::Frontier() const noexcept {
    l2flow::canonical::SourceFrontierV1 result{};
    if (l2flow::canonical::ReadSourceFrontierV1(
            *impl_->source_frontier, &result) !=
        SourceFrontierErrorV1::kNone) {
        return {};
    }
    return result;
}

ProductionSourceActivationEvidenceV1
ProductionSourcePipelineV1::CaptureActivationEvidence() const {
    std::lock_guard<std::mutex> lock(impl_->execution_mutex);
    ProductionSourceActivationEvidenceV1 result{};
    result.pipeline = Snapshot();
    result.control = impl_->control_decoder->Snapshot();
    result.raw_control = impl_->live_tail->SampleControlFresh();
    result.source_frontier_read_error =
        l2flow::canonical::ReadSourceFrontierV1(
            *impl_->source_frontier, &result.source_frontier);
    result.history_barrier = impl_->history->CaptureBarrier(
        impl_->config.source_slot);
    return result;
}

ProductionSourceActiveValidityEvidenceV1
ProductionSourcePipelineV1::CaptureActiveValidityEvidence() const noexcept {
    std::lock_guard<std::mutex> lock(impl_->execution_mutex);
    ProductionSourceActiveValidityEvidenceV1 result{};
    result.control = impl_->control_decoder->ReadinessSummary();
    result.raw_control = impl_->live_tail->SampleControlFresh();
    result.source_frontier_read_error =
        l2flow::canonical::ReadSourceFrontierV1(
            *impl_->source_frontier, &result.source_frontier);
    result.history_frontier =
        impl_->history->Frontier(impl_->config.source_slot);
    result.pipeline_ended = impl_->ended.load(std::memory_order_acquire);
    result.pipeline_fatal = impl_->fatal.load(std::memory_order_acquire);
    return result;
}

ProductionSourceActiveLocalEvidenceV1
ProductionSourcePipelineV1::CaptureActiveLocalEvidence() const noexcept {
    std::lock_guard<std::mutex> lock(impl_->execution_mutex);
    ProductionSourceActiveLocalEvidenceV1 result{};
    result.control = impl_->control_decoder->ReadinessSummary();
    result.source_frontier_read_error =
        l2flow::canonical::ReadSourceFrontierV1(
            *impl_->source_frontier, &result.source_frontier);
    result.history_frontier =
        impl_->history->Frontier(impl_->config.source_slot);
    result.pipeline_ended = impl_->ended.load(std::memory_order_acquire);
    result.pipeline_fatal = impl_->fatal.load(std::memory_order_acquire);
    return result;
}

void ProductionSourcePipelineV1::MarkFatal(
    ProductionSourceFailureV1 failure) noexcept {
    std::lock_guard<std::mutex> lock(impl_->execution_mutex);
    ProductionSourceStepResultV1 result{};
    result.kind = ProductionSourceStepKindV1::kFatal;
    result.failure = failure == ProductionSourceFailureV1::kNone
        ? ProductionSourceFailureV1::kUnexpectedFailure
        : failure;
    impl_->LatchFatal(result);
}

const ProductionSourcePipelineConfigV1&
ProductionSourcePipelineV1::config() const noexcept {
    return impl_->config;
}

const ProductionInstrumentRegistryIdentityV1&
ProductionSourcePipelineV1::registry_identity() const noexcept {
    return impl_->registry_identity;
}

l2flow::market::InstrumentHistoryRuntimeV1*
ProductionSourcePipelineV1::history_runtime() const noexcept {
    return impl_->history;
}

const l2flow::canonical::SourceFrontierPageV1*
ProductionSourcePipelineV1::source_frontier_page() const noexcept {
    return impl_->source_frontier;
}

}  // namespace l2flow::runtime
