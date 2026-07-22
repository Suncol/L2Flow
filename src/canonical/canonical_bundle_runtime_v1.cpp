#include "l2flow/canonical/canonical_bundle_runtime_v1.h"

#include "l2flow/market/instrument_registry.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <limits>
#include <new>
#include <utility>
#include <variant>

namespace l2flow::canonical {
namespace {

template <typename Operation>
[[nodiscard]] SourceFrontierErrorV1 RetrySourceFrontierBusy(
    std::chrono::nanoseconds timeout,
    Operation&& operation) noexcept {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    for (;;) {
        const SourceFrontierErrorV1 error = operation();
        if (error != SourceFrontierErrorV1::kBusy ||
            std::chrono::steady_clock::now() >= deadline) {
            return error;
        }
    }
}

[[nodiscard]] SourceFrontierErrorV1 ReadSourceFrontierWithRetry(
    const SourceFrontierPageV1& page,
    std::chrono::nanoseconds timeout,
    SourceFrontierV1* output) noexcept {
    return RetrySourceFrontierBusy(timeout, [&]() noexcept {
        return ReadSourceFrontierV1(page, output);
    });
}

[[nodiscard]] SourceFrontierErrorV1 PublishProcessedProgressWithRetry(
    SourceFrontierPageV1* page,
    const l2flow::common::Identity128& writer_instance,
    std::uint64_t generation,
    std::uint64_t ingress_sequence,
    std::uint64_t global_wal_pos,
    std::int64_t recv_monotonic_ns,
    std::chrono::nanoseconds timeout) noexcept {
    return RetrySourceFrontierBusy(timeout, [&]() noexcept {
        return PublishProcessedProgressV1(
            page,
            writer_instance,
            generation,
            ingress_sequence,
            global_wal_pos,
            recv_monotonic_ns);
    });
}

[[nodiscard]] bool DigestNonzero(
    const l2flow::common::Sha256Digest& digest) noexcept {
    return std::any_of(
        digest.begin(), digest.end(),
        [](std::byte value) { return value != std::byte{0U}; });
}

[[nodiscard]] CanonicalEventTypeV1 EventTypeForFamily(
    CanonicalFamilyV1 family) noexcept {
    switch (family) {
        case CanonicalFamilyV1::kSnapshot:
            return CanonicalEventTypeV1::kSnapshot;
        case CanonicalFamilyV1::kTick:
            return CanonicalEventTypeV1::kTick;
        case CanonicalFamilyV1::kQuality:
            return CanonicalEventTypeV1::kQuality;
        case CanonicalFamilyV1::kControl:
            return CanonicalEventTypeV1::kControl;
    }
    return CanonicalEventTypeV1::kUnknown;
}

[[nodiscard]] std::span<const std::byte> RecordBytes(
    const CanonicalRecordVariantV1& record) noexcept {
    return std::visit(
        [](const auto& value) noexcept {
            return std::as_bytes(std::span(&value, 1U));
        },
        record);
}

[[nodiscard]] CanonicalHeaderV1 RecordHeader(
    const CanonicalRecordVariantV1& record) noexcept {
    return std::visit(
        [](const auto& value) noexcept { return value.header; },
        record);
}

[[nodiscard]] bool SameFrontierIdentity(
    const SourceFrontierV1& left,
    const SourceFrontierV1& right) noexcept {
    return left.source_stream_id == right.source_stream_id &&
           left.capture_date == right.capture_date &&
           left.stream_day_id == right.stream_day_id &&
           left.clock_epoch == right.clock_epoch &&
           left.writer_instance == right.writer_instance &&
           left.generation == right.generation;
}

[[nodiscard]] bool DescriptorMatchesFrontier(
    const CanonicalSegmentDescriptorV1& descriptor,
    const SourceFrontierV1& frontier) noexcept {
    return descriptor.source_stream_id == frontier.source_stream_id &&
           descriptor.origin_capture_date == frontier.capture_date &&
           descriptor.origin_stream_day_id == frontier.stream_day_id &&
           descriptor.origin_source_writer_instance ==
               frontier.writer_instance &&
           descriptor.origin_source_generation == frontier.generation &&
           descriptor.clock_epoch == frontier.clock_epoch;
}

[[nodiscard]] bool CompleteRouteManifest(
    std::span<const CanonicalBundleSinkV1> sinks,
    std::uint32_t shard_count) noexcept {
    const std::uint64_t expected_count =
        static_cast<std::uint64_t>(shard_count) * 2U + 2U;
    if (shard_count == 0U ||
        expected_count >
            static_cast<std::uint64_t>(
                std::numeric_limits<std::size_t>::max()) ||
        sinks.size() != static_cast<std::size_t>(expected_count)) {
        return false;
    }
    for (std::size_t index = 0U; index < sinks.size(); ++index) {
        const CanonicalBundleSinkV1& sink = sinks[index];
        const bool legal_route =
            (sink.family == CanonicalFamilyV1::kSnapshot &&
             sink.shard < shard_count) ||
            (sink.family == CanonicalFamilyV1::kTick &&
             sink.shard < shard_count) ||
            (sink.family == CanonicalFamilyV1::kQuality &&
             sink.shard == 0U) ||
            (sink.family == CanonicalFamilyV1::kControl &&
             sink.shard == 0U);
        if (!legal_route) {
            return false;
        }
        for (std::size_t prior = 0U; prior < index; ++prior) {
            if (sinks[prior].family == sink.family &&
                sinks[prior].shard == sink.shard) {
                return false;
            }
        }
    }
    // There are exactly 2*shard_count+2 legal routes.  An equally sized set
    // of unique legal entries therefore contains every required route.
    return true;
}

[[nodiscard]] bool RawCoveredByAppend(
    const CanonicalRawContextV1& raw,
    const SourceFrontierV1& frontier) noexcept {
    if (raw.origin_ingress_sequence == frontier.append_ingress_sequence) {
        return raw.origin_wal_end_pos <= frontier.append_global_wal_pos;
    }
    return raw.origin_ingress_sequence < frontier.append_ingress_sequence &&
           raw.origin_wal_end_pos < frontier.append_global_wal_pos;
}

[[nodiscard]] bool CheckedAddU64(
    std::uint64_t left,
    std::uint64_t right,
    std::uint64_t* result) noexcept {
    if (result == nullptr ||
        right > std::numeric_limits<std::uint64_t>::max() - left) {
        return false;
    }
    *result = left + right;
    return true;
}

[[nodiscard]] ClockEpochIdentityV1 SegmentClockEpoch(
    const l2flow::ingress::SegmentHeaderV1& header) noexcept {
    return ClockEpochIdentityV1{
        header.clock_epoch_algorithm,
        header.clock_epoch_digest,
        header.clock_epoch_label};
}

[[nodiscard]] bool ValidNoOutputReason(
    CanonicalNoOutputReasonV1 reason) noexcept {
    return reason == CanonicalNoOutputReasonV1::kOptionalMarketMessage ||
           reason == CanonicalNoOutputReasonV1::kUnmodeledControlMessage;
}

[[nodiscard]] bool RawIdentityMatchesFrontier(
    const CanonicalRawContextV1& raw,
    const SourceFrontierV1& frontier) noexcept {
    return raw.source_stream_id == frontier.source_stream_id &&
           raw.capture_date == frontier.capture_date &&
           raw.stream_day_id == frontier.stream_day_id &&
           raw.source_writer_instance == frontier.writer_instance &&
           raw.source_generation == frontier.generation &&
           raw.clock_epoch == frontier.clock_epoch;
}

[[nodiscard]] bool RawIsNext(
    const CanonicalRawContextV1& raw,
    const SourceFrontierV1& frontier) noexcept {
    return frontier.processed_ingress_sequence !=
               std::numeric_limits<std::uint64_t>::max() &&
           raw.origin_ingress_sequence ==
               frontier.processed_ingress_sequence + 1U &&
           raw.origin_wal_end_pos > frontier.processed_global_wal_pos;
}

[[nodiscard]] bool NoOutputEnvelopeShapeValid(
    const CanonicalNormalizerConfigV1& config,
    const CanonicalRawContextV1& raw,
    const l2flow::market::MarketMessageViewV1& message) noexcept {
    return raw.trade_date == config.trade_date &&
           (raw.upstream_quality_flags &
            ~kCanonicalQualityFlagsMaskV1) == 0U &&
           message.source_stream_id == raw.source_stream_id &&
           message.trade_date == raw.trade_date &&
           message.source_sequence == raw.origin_ingress_sequence &&
           message.service_id != 0U &&
           message.service_version != 0U &&
           message.message_id != 0U &&
           message.recv_realtime_ns >= 0 &&
           message.recv_monotonic_ns >= 0 &&
           (message.body.empty() || message.body.data() != nullptr);
}

[[nodiscard]] bool RecordCoveredByProcessed(
    const CanonicalHeaderV1& header,
    const SourceFrontierV1& frontier) noexcept {
    if (header.origin_ingress_sequence ==
        frontier.processed_ingress_sequence) {
        return header.origin_wal_end_pos <=
               frontier.processed_global_wal_pos;
    }
    return header.origin_ingress_sequence <
               frontier.processed_ingress_sequence &&
           header.origin_wal_end_pos <
               frontier.processed_global_wal_pos;
}

[[nodiscard]] bool ValidateRecordBytes(
    CanonicalEventTypeV1 event_type,
    std::span<const std::byte> bytes) noexcept {
    switch (event_type) {
        case CanonicalEventTypeV1::kTick: {
            if (bytes.size() != sizeof(CanonicalTickRecordV1)) {
                return false;
            }
            CanonicalTickRecordV1 record{};
            std::memcpy(&record, bytes.data(), bytes.size());
            return ValidateCanonicalTickRecordV1(record) ==
                   CanonicalValidationErrorV1::kNone;
        }
        case CanonicalEventTypeV1::kSnapshot: {
            if (bytes.size() != sizeof(CanonicalSnapshotRecordV1)) {
                return false;
            }
            CanonicalSnapshotRecordV1 record{};
            std::memcpy(&record, bytes.data(), bytes.size());
            return ValidateCanonicalSnapshotRecordV1(record) ==
                   CanonicalValidationErrorV1::kNone;
        }
        case CanonicalEventTypeV1::kQuality: {
            if (bytes.size() != sizeof(CanonicalQualityRecordV1)) {
                return false;
            }
            CanonicalQualityRecordV1 record{};
            std::memcpy(&record, bytes.data(), bytes.size());
            return ValidateCanonicalQualityRecordV1(record) ==
                   CanonicalValidationErrorV1::kNone;
        }
        case CanonicalEventTypeV1::kControl: {
            if (bytes.size() != sizeof(CanonicalControlRecordV1)) {
                return false;
            }
            CanonicalControlRecordV1 record{};
            std::memcpy(&record, bytes.data(), bytes.size());
            return ValidateCanonicalControlRecordV1(record) ==
                   CanonicalValidationErrorV1::kNone;
        }
        case CanonicalEventTypeV1::kUnknown:
            return false;
    }
    return false;
}

}  // namespace

class CanonicalBundleCoordinatorV1::Impl final {
public:
    explicit Impl(CanonicalBundleCoordinatorConfigV1 value,
                  SourceFrontierV1 identity)
        : config(std::move(value)), expected_identity(std::move(identity)) {}

    [[nodiscard]] bool Hook(
        CanonicalBundleOperationV1 operation,
        std::size_t ordinal) const noexcept {
        return config.operation_hook == nullptr ||
               config.operation_hook(
                   config.operation_hook_context, operation, ordinal);
    }

    void FailGeneration(
        CanonicalNormalizationTransactionV1* transaction,
        const SourceFrontierV1& observed) noexcept {
        // This lock-free sticky latch is the global revocation anchor.  Publish
        // it before touching process-local normalizer state or individual
        // segment controls, so a crash during fan-out cannot leave another
        // family readable under a still-healthy SourceFrontier.
        static_cast<void>(PublishSourceStateV1(
            config.source_frontier,
            observed.writer_instance,
            observed.generation,
            SourceStateV1::kFatal,
            observed.quality_flags));
        static_cast<void>(config.normalizer->FailStop(transaction));
        for (CanonicalBundleSinkV1& sink : config.sinks) {
            if (sink.writer != nullptr) {
                static_cast<void>(sink.writer->MarkGenerationFatal());
            }
        }
    }

    [[nodiscard]] bool AbortPrepared(
        CanonicalNormalizationTransactionV1* transaction,
        const SourceFrontierV1& observed) noexcept {
        if (config.normalizer->Abort(transaction)) {
            return true;
        }
        FailGeneration(transaction, observed);
        return false;
    }

    [[nodiscard]] CanonicalBundleResultV1 PublishPrepared(
        const CanonicalRawContextV1& raw,
        std::int64_t recv_monotonic_ns,
        CanonicalBundleResultV1 result,
        CanonicalNormalizationTransactionV1* transaction,
        const SourceFrontierV1& frontier) noexcept;

    [[nodiscard]] CanonicalBundleResultV1 PublishProgressOnly(
        std::uint64_t ingress_sequence,
        std::uint64_t wal_pos,
        std::int64_t recv_monotonic_ns,
        CanonicalBundleResultV1 result,
        const SourceFrontierV1& frontier) noexcept;

    CanonicalBundleCoordinatorConfigV1 config;
    SourceFrontierV1 expected_identity;
};

std::string_view CanonicalBundleErrorNameV1(
    CanonicalBundleErrorV1 error) noexcept {
    switch (error) {
        case CanonicalBundleErrorV1::kNone:
            return "none";
        case CanonicalBundleErrorV1::kNullOutput:
            return "null_output";
        case CanonicalBundleErrorV1::kInvalidConfiguration:
            return "invalid_configuration";
        case CanonicalBundleErrorV1::kResourceExhausted:
            return "resource_exhausted";
        case CanonicalBundleErrorV1::kInvalidFrontier:
            return "invalid_frontier";
        case CanonicalBundleErrorV1::kIdentityMismatch:
            return "identity_mismatch";
        case CanonicalBundleErrorV1::kSourceFatal:
            return "source_fatal";
        case CanonicalBundleErrorV1::kDayStartRequired:
            return "day_start_required";
        case CanonicalBundleErrorV1::kRawNotNext:
            return "raw_not_next";
        case CanonicalBundleErrorV1::kRawNotAppended:
            return "raw_not_appended";
        case CanonicalBundleErrorV1::kEnvelopeNotVerified:
            return "envelope_not_verified";
        case CanonicalBundleErrorV1::kInvalidNoOutputReason:
            return "invalid_no_output_reason";
        case CanonicalBundleErrorV1::kInvalidSegmentTransition:
            return "invalid_segment_transition";
        case CanonicalBundleErrorV1::kNormalizePrepareFailed:
            return "normalize_prepare_failed";
        case CanonicalBundleErrorV1::kMissingSink:
            return "missing_sink";
        case CanonicalBundleErrorV1::kSinkPreflightFailed:
            return "sink_preflight_failed";
        case CanonicalBundleErrorV1::kInjectedFailure:
            return "injected_failure";
        case CanonicalBundleErrorV1::kSinkPublishFailed:
            return "sink_publish_failed";
        case CanonicalBundleErrorV1::kReceiptFailure:
            return "receipt_failure";
        case CanonicalBundleErrorV1::kNormalizerCommitFailed:
            return "normalizer_commit_failed";
        case CanonicalBundleErrorV1::kSegmentProgressFailed:
            return "segment_progress_failed";
        case CanonicalBundleErrorV1::kFrontierCommitFailed:
            return "frontier_commit_failed";
    }
    return "invalid_bundle_error";
}

CanonicalBundleCoordinatorV1::CanonicalBundleCoordinatorV1(
    std::unique_ptr<Impl> impl) noexcept : impl_(std::move(impl)) {}

CanonicalBundleCoordinatorV1::~CanonicalBundleCoordinatorV1() = default;

CanonicalBundleErrorV1 CanonicalBundleCoordinatorV1::Create(
    CanonicalBundleCoordinatorConfigV1 config,
    std::unique_ptr<CanonicalBundleCoordinatorV1>* output) noexcept {
    if (output == nullptr) {
        return CanonicalBundleErrorV1::kNullOutput;
    }
    output->reset();
    if (config.normalizer == nullptr ||
        config.source_frontier == nullptr || config.sinks.empty() ||
        config.market_envelope_verifier == nullptr ||
        config.control_envelope_verifier == nullptr ||
        config.canonical_generation == 0U ||
        config.source_frontier_busy_timeout.count() <= 0 ||
        config.source_frontier_busy_timeout >
            kSourceFrontierMaximumBusyTimeoutV1 ||
        !DigestNonzero(config.normalizer_build_sha256) ||
        !DigestNonzero(config.normalizer_config_sha256)) {
        return CanonicalBundleErrorV1::kInvalidConfiguration;
    }
    SourceFrontierV1 frontier{};
    if (ReadSourceFrontierV1(*config.source_frontier, &frontier) !=
        SourceFrontierErrorV1::kNone) {
        return CanonicalBundleErrorV1::kInvalidFrontier;
    }
    const CanonicalNormalizerConfigV1& normalizer_config =
        config.normalizer->config();
    if (!CompleteRouteManifest(
            config.sinks, normalizer_config.shard_count)) {
        return CanonicalBundleErrorV1::kInvalidConfiguration;
    }
    if (frontier.source_stream_id !=
            normalizer_config.source_stream_id ||
        frontier.capture_date != normalizer_config.capture_date ||
        frontier.stream_day_id != normalizer_config.stream_day_id ||
        frontier.source_state == SourceStateV1::kFatal) {
        return CanonicalBundleErrorV1::kIdentityMismatch;
    }
    const CanonicalNormalizerSnapshotV1 normalizer_snapshot =
        config.normalizer->Snapshot();
    // V1 has no normalizer checkpoint codec.  A fresh object may therefore
    // only attach before the first Raw record of the day and to empty output
    // segments.  Midday restart must replay from day start into a new output
    // generation; silently reconstructing sequence/phase state is forbidden.
    if (frontier.processed_ingress_sequence != 0U ||
        normalizer_snapshot.vendor_scope_count != 0U ||
        normalizer_snapshot.exchange_scope_count != 0U ||
        normalizer_snapshot.phase_product_count != 0U ||
        normalizer_snapshot.snapshot_records_committed != 0U ||
        normalizer_snapshot.tick_records_committed != 0U ||
        normalizer_snapshot.quality_records_committed != 0U ||
        normalizer_snapshot.control_records_committed != 0U ||
        normalizer_snapshot.transaction_active ||
        normalizer_snapshot.fatal) {
        return CanonicalBundleErrorV1::kDayStartRequired;
    }

    for (std::size_t index = 0U; index < config.sinks.size(); ++index) {
        const CanonicalBundleSinkV1& sink = config.sinks[index];
        if (sink.writer == nullptr ||
            EventTypeForFamily(sink.family) ==
                CanonicalEventTypeV1::kUnknown) {
            return CanonicalBundleErrorV1::kInvalidConfiguration;
        }
        const CanonicalSegmentDescriptorV1& descriptor =
            sink.writer->descriptor();
        if (descriptor.event_type != EventTypeForFamily(sink.family) ||
            descriptor.shard != sink.shard ||
            descriptor.trade_date != normalizer_config.trade_date ||
            descriptor.registry_version !=
                normalizer_config.instrument_registry->registry_version() ||
            descriptor.registry_sha256 !=
                normalizer_config.instrument_registry->registry_sha256() ||
            descriptor.normalizer_build_sha256 !=
                config.normalizer_build_sha256 ||
            descriptor.normalizer_config_sha256 !=
                config.normalizer_config_sha256 ||
            descriptor.generation != config.canonical_generation ||
            !DescriptorMatchesFrontier(descriptor, frontier) ||
            sink.writer->header().published_records != 0U ||
            sink.writer->header().last_shard_event_id != 0U ||
            sink.writer->header().processed_raw_ingress_sequence !=
                frontier.processed_ingress_sequence ||
            sink.writer->header().processed_raw_wal_pos !=
                frontier.processed_global_wal_pos) {
            return CanonicalBundleErrorV1::kIdentityMismatch;
        }
        CanonicalSegmentControlSnapshotV1 control{};
        if (sink.writer->ReadControl(&control) !=
                CanonicalSegmentErrorV1::kNone ||
            control.closed || control.generation_fatal) {
            return CanonicalBundleErrorV1::kInvalidConfiguration;
        }
    }
    try {
        auto impl = std::make_unique<Impl>(
            std::move(config), frontier);
        output->reset(new CanonicalBundleCoordinatorV1(std::move(impl)));
        return CanonicalBundleErrorV1::kNone;
    } catch (...) {
        return CanonicalBundleErrorV1::kResourceExhausted;
    }
}

CanonicalBundleResultV1 CanonicalBundleCoordinatorV1::ProcessMarket(
    const CanonicalRawContextV1& raw,
    const l2flow::market::MarketMessageViewV1& message) noexcept {
    return ProcessMarketImpl(
        raw, message, nullptr,
        l2flow::market::MarketDecodeErrorV1::kNone, false);
}

CanonicalBundleResultV1
CanonicalBundleCoordinatorV1::ProcessDecodedMarket(
    const CanonicalRawContextV1& raw,
    const l2flow::market::MarketMessageViewV1& message,
    const l2flow::market::RetainedMarketEventV1& decoded) noexcept {
    return ProcessMarketImpl(
        raw, message, &decoded,
        l2flow::market::MarketDecodeErrorV1::kNone, false);
}

CanonicalBundleResultV1
CanonicalBundleCoordinatorV1::ProcessMarketDecodeFailure(
    const CanonicalRawContextV1& raw,
    const l2flow::market::MarketMessageViewV1& message,
    l2flow::market::MarketDecodeErrorV1 decode_error) noexcept {
    return ProcessMarketImpl(raw, message, nullptr, decode_error, true);
}

CanonicalBundleResultV1 CanonicalBundleCoordinatorV1::ProcessMarketImpl(
    const CanonicalRawContextV1& raw,
    const l2flow::market::MarketMessageViewV1& message,
    const l2flow::market::RetainedMarketEventV1* decoded,
    l2flow::market::MarketDecodeErrorV1 decoded_failure,
    bool use_decoded_failure) noexcept {
    CanonicalBundleResultV1 result{};
    SourceFrontierV1 frontier{};
    result.frontier_error = ReadSourceFrontierWithRetry(
        *impl_->config.source_frontier,
        impl_->config.source_frontier_busy_timeout,
        &frontier);
    if (result.frontier_error != SourceFrontierErrorV1::kNone) {
        result.error = CanonicalBundleErrorV1::kInvalidFrontier;
        return result;
    }
    if (!SameFrontierIdentity(frontier, impl_->expected_identity)) {
        result.error = CanonicalBundleErrorV1::kIdentityMismatch;
        return result;
    }
    if (frontier.source_state == SourceStateV1::kFatal ||
        impl_->config.normalizer->Snapshot().fatal) {
        impl_->FailGeneration(nullptr, frontier);
        result.error = CanonicalBundleErrorV1::kSourceFatal;
        return result;
    }
    if (!RawIdentityMatchesFrontier(raw, frontier)) {
        result.error = CanonicalBundleErrorV1::kIdentityMismatch;
        return result;
    }
    if (!RawIsNext(raw, frontier)) {
        result.error = CanonicalBundleErrorV1::kRawNotNext;
        return result;
    }
    if (!RawCoveredByAppend(raw, frontier)) {
        result.error = CanonicalBundleErrorV1::kRawNotAppended;
        return result;
    }
    if (!impl_->config.market_envelope_verifier(
            impl_->config.envelope_verifier_context,
            frontier,
            raw,
            message)) {
        result.error = CanonicalBundleErrorV1::kEnvelopeNotVerified;
        return result;
    }

    CanonicalNormalizationTransactionV1 transaction;
    if (decoded != nullptr) {
        result.normalize = impl_->config.normalizer->PrepareDecoded(
            raw, message, *decoded, &transaction);
    } else if (use_decoded_failure) {
        result.normalize =
            impl_->config.normalizer->PrepareDecodedFailure(
                raw, message, decoded_failure, &transaction);
    } else {
        result.normalize = impl_->config.normalizer->Prepare(
            raw, message, &transaction);
    }
    if (!result.normalize.ok() || !transaction.active()) {
        if (result.normalize.error ==
                CanonicalNormalizePrepareErrorV1::kNormalizerFatal ||
            impl_->config.normalizer->Snapshot().fatal ||
            (result.normalize.ok() && !transaction.active()) ||
            (!result.normalize.ok() && transaction.active())) {
            impl_->FailGeneration(
                transaction.active() ? &transaction : nullptr,
                frontier);
        }
        result.error = CanonicalBundleErrorV1::kNormalizePrepareFailed;
        return result;
    }

    return impl_->PublishPrepared(
        raw,
        message.recv_monotonic_ns,
        result,
        &transaction,
        frontier);
}

CanonicalBundleResultV1 CanonicalBundleCoordinatorV1::ProcessControl(
    const CanonicalRawContextV1& raw,
    const l2flow::control::ControlRecordV1& control,
    std::int64_t recv_realtime_ns,
    std::int64_t recv_monotonic_ns) noexcept {
    CanonicalBundleResultV1 result{};
    SourceFrontierV1 frontier{};
    result.frontier_error = ReadSourceFrontierWithRetry(
        *impl_->config.source_frontier,
        impl_->config.source_frontier_busy_timeout,
        &frontier);
    if (result.frontier_error != SourceFrontierErrorV1::kNone) {
        result.error = CanonicalBundleErrorV1::kInvalidFrontier;
        return result;
    }
    if (!SameFrontierIdentity(frontier, impl_->expected_identity)) {
        result.error = CanonicalBundleErrorV1::kIdentityMismatch;
        return result;
    }
    if (frontier.source_state == SourceStateV1::kFatal ||
        impl_->config.normalizer->Snapshot().fatal) {
        impl_->FailGeneration(nullptr, frontier);
        result.error = CanonicalBundleErrorV1::kSourceFatal;
        return result;
    }
    if (raw.source_stream_id != frontier.source_stream_id ||
        raw.capture_date != frontier.capture_date ||
        raw.stream_day_id != frontier.stream_day_id ||
        raw.source_writer_instance != frontier.writer_instance ||
        raw.source_generation != frontier.generation ||
        !(raw.clock_epoch == frontier.clock_epoch)) {
        result.error = CanonicalBundleErrorV1::kIdentityMismatch;
        return result;
    }
    if (frontier.processed_ingress_sequence ==
            std::numeric_limits<std::uint64_t>::max() ||
        raw.origin_ingress_sequence !=
            frontier.processed_ingress_sequence + 1U ||
        raw.origin_wal_end_pos <= frontier.processed_global_wal_pos) {
        result.error = CanonicalBundleErrorV1::kRawNotNext;
        return result;
    }
    if (!RawCoveredByAppend(raw, frontier)) {
        result.error = CanonicalBundleErrorV1::kRawNotAppended;
        return result;
    }
    if (!impl_->config.control_envelope_verifier(
            impl_->config.envelope_verifier_context,
            frontier,
            raw,
            control,
            recv_realtime_ns,
            recv_monotonic_ns)) {
        result.error = CanonicalBundleErrorV1::kEnvelopeNotVerified;
        return result;
    }

    CanonicalNormalizationTransactionV1 transaction;
    result.normalize = impl_->config.normalizer->PrepareControl(
        raw,
        control,
        recv_realtime_ns,
        recv_monotonic_ns,
        &transaction);
    if (!result.normalize.ok() || !transaction.active()) {
        if (result.normalize.error ==
                CanonicalNormalizePrepareErrorV1::kNormalizerFatal ||
            impl_->config.normalizer->Snapshot().fatal ||
            (result.normalize.ok() && !transaction.active()) ||
            (!result.normalize.ok() && transaction.active())) {
            impl_->FailGeneration(
                transaction.active() ? &transaction : nullptr,
                frontier);
        }
        result.error = CanonicalBundleErrorV1::kNormalizePrepareFailed;
        return result;
    }
    return impl_->PublishPrepared(
        raw,
        recv_monotonic_ns,
        result,
        &transaction,
        frontier);
}

CanonicalBundleResultV1 CanonicalBundleCoordinatorV1::ProcessNoOutput(
    const CanonicalRawContextV1& raw,
    const l2flow::market::MarketMessageViewV1& message,
    CanonicalNoOutputReasonV1 reason) noexcept {
    CanonicalBundleResultV1 result{};
    if (!ValidNoOutputReason(reason)) {
        result.error = CanonicalBundleErrorV1::kInvalidNoOutputReason;
        return result;
    }
    SourceFrontierV1 frontier{};
    result.frontier_error = ReadSourceFrontierWithRetry(
        *impl_->config.source_frontier,
        impl_->config.source_frontier_busy_timeout,
        &frontier);
    if (result.frontier_error != SourceFrontierErrorV1::kNone) {
        result.error = CanonicalBundleErrorV1::kInvalidFrontier;
        return result;
    }
    if (!SameFrontierIdentity(frontier, impl_->expected_identity)) {
        result.error = CanonicalBundleErrorV1::kIdentityMismatch;
        return result;
    }
    if (frontier.source_state == SourceStateV1::kFatal ||
        impl_->config.normalizer->Snapshot().fatal) {
        impl_->FailGeneration(nullptr, frontier);
        result.error = CanonicalBundleErrorV1::kSourceFatal;
        return result;
    }
    if (!RawIdentityMatchesFrontier(raw, frontier)) {
        result.error = CanonicalBundleErrorV1::kIdentityMismatch;
        return result;
    }
    if (!RawIsNext(raw, frontier)) {
        result.error = CanonicalBundleErrorV1::kRawNotNext;
        return result;
    }
    if (!RawCoveredByAppend(raw, frontier)) {
        result.error = CanonicalBundleErrorV1::kRawNotAppended;
        return result;
    }
    if (!NoOutputEnvelopeShapeValid(
            impl_->config.normalizer->config(), raw, message) ||
        impl_->config.no_output_envelope_verifier == nullptr ||
        !impl_->config.no_output_envelope_verifier(
            impl_->config.envelope_verifier_context,
            frontier,
            raw,
            message,
            reason)) {
        result.error = CanonicalBundleErrorV1::kEnvelopeNotVerified;
        return result;
    }
    return impl_->PublishProgressOnly(
        raw.origin_ingress_sequence,
        raw.origin_wal_end_pos,
        message.recv_monotonic_ns,
        result,
        frontier);
}

CanonicalBundleResultV1
CanonicalBundleCoordinatorV1::ProcessSegmentTransition(
    const l2flow::ingress::RawLiveSegmentTransitionV1& transition)
    noexcept {
    CanonicalBundleResultV1 result{};
    SourceFrontierV1 frontier{};
    result.frontier_error = ReadSourceFrontierWithRetry(
        *impl_->config.source_frontier,
        impl_->config.source_frontier_busy_timeout,
        &frontier);
    if (result.frontier_error != SourceFrontierErrorV1::kNone) {
        result.error = CanonicalBundleErrorV1::kInvalidFrontier;
        return result;
    }
    if (!SameFrontierIdentity(frontier, impl_->expected_identity)) {
        result.error = CanonicalBundleErrorV1::kIdentityMismatch;
        return result;
    }
    if (frontier.source_state == SourceStateV1::kFatal ||
        impl_->config.normalizer->Snapshot().fatal) {
        impl_->FailGeneration(nullptr, frontier);
        result.error = CanonicalBundleErrorV1::kSourceFatal;
        return result;
    }

    const l2flow::ingress::SegmentHeaderV1& previous =
        transition.previous_segment;
    const l2flow::ingress::SegmentHeaderV1& next =
        transition.next_segment;
    std::uint64_t previous_end_wal_pos = 0U;
    std::uint64_t next_data_begin_wal_pos = 0U;
    const bool sequence_available =
        frontier.processed_ingress_sequence !=
        std::numeric_limits<std::uint64_t>::max();
    const bool appended =
        transition.next_data_begin_wal_pos <=
            frontier.append_global_wal_pos &&
        (frontier.processed_ingress_sequence ==
             frontier.append_ingress_sequence ||
         transition.next_data_begin_wal_pos <
             frontier.append_global_wal_pos);
    if (transition.writer_instance != frontier.writer_instance ||
        previous.source_stream_id != frontier.source_stream_id ||
        next.source_stream_id != frontier.source_stream_id ||
        previous.capture_date != frontier.capture_date ||
        next.capture_date != frontier.capture_date ||
        previous.stream_day_id != frontier.stream_day_id ||
        next.stream_day_id != frontier.stream_day_id ||
        !(SegmentClockEpoch(previous) == frontier.clock_epoch) ||
        !(SegmentClockEpoch(next) == frontier.clock_epoch) ||
        previous.segment_sequence ==
            std::numeric_limits<std::uint32_t>::max() ||
        next.segment_sequence != previous.segment_sequence + 1U ||
        transition.previous_segment_end_offset <
            l2flow::ingress::kRawV1SegmentHeaderBytes ||
        !CheckedAddU64(
            previous.segment_base_wal_pos,
            transition.previous_segment_end_offset,
            &previous_end_wal_pos) ||
        previous_end_wal_pos != frontier.processed_global_wal_pos ||
        next.segment_base_wal_pos != previous_end_wal_pos ||
        !CheckedAddU64(
            next.segment_base_wal_pos,
            l2flow::ingress::kRawV1SegmentHeaderBytes,
            &next_data_begin_wal_pos) ||
        transition.next_data_begin_wal_pos != next_data_begin_wal_pos ||
        transition.next_data_begin_wal_pos <=
            frontier.processed_global_wal_pos ||
        !sequence_available ||
        next.first_ingress_sequence !=
            frontier.processed_ingress_sequence + 1U ||
        transition.next_ingress_sequence != next.first_ingress_sequence ||
        !appended) {
        result.error = CanonicalBundleErrorV1::kInvalidSegmentTransition;
        return result;
    }
    // control_generation is the changing coherent control-page snapshot
    // generation, not SourceFrontier's immutable route generation.  Its
    // authentication is deliberately delegated to this mandatory verifier.
    if (impl_->config.segment_transition_verifier == nullptr ||
        !impl_->config.segment_transition_verifier(
            impl_->config.envelope_verifier_context,
            frontier,
            transition)) {
        result.error = CanonicalBundleErrorV1::kEnvelopeNotVerified;
        return result;
    }
    return impl_->PublishProgressOnly(
        frontier.processed_ingress_sequence,
        transition.next_data_begin_wal_pos,
        // A segment header has no receive timestamp.  Preserve the last
        // appended record time while advancing only the WAL cursor.  The
        // independently published idle-safe frontier may legitimately be
        // newer than last_appended_recv_monotonic_ns and is therefore not a
        // valid argument to PublishProcessedProgressV1.
        frontier.last_appended_recv_monotonic_ns,
        result,
        frontier);
}

CanonicalBundleResultV1
CanonicalBundleCoordinatorV1::Impl::PublishPrepared(
    const CanonicalRawContextV1& raw,
    std::int64_t recv_monotonic_ns,
    CanonicalBundleResultV1 result,
    CanonicalNormalizationTransactionV1* transaction_pointer,
    const SourceFrontierV1& frontier) noexcept {
    Impl* const impl_ = this;
    CanonicalNormalizationTransactionV1& transaction =
        *transaction_pointer;
    try {
        std::vector<std::vector<std::span<const std::byte>>> per_sink(
            impl_->config.sinks.size());
        const CanonicalSegmentContextV1 context =
            transaction.segment_context();
        if (context.capture_date != raw.capture_date ||
            context.trade_date != raw.trade_date ||
            context.source_stream_id != raw.source_stream_id ||
            context.stream_day_id != raw.stream_day_id ||
            !(context.clock_epoch == raw.clock_epoch)) {
            impl_->FailGeneration(&transaction, frontier);
            result.error = CanonicalBundleErrorV1::kIdentityMismatch;
            return result;
        }

        for (const RoutedCanonicalRecordV1& routed :
             transaction.records()) {
            const CanonicalHeaderV1 header = RecordHeader(routed.record);
            if (header.origin_ingress_sequence !=
                    raw.origin_ingress_sequence ||
                header.origin_wal_end_pos != raw.origin_wal_end_pos ||
                header.source_stream_id != raw.source_stream_id ||
                header.trade_date != raw.trade_date ||
                header.event_type != EventTypeForFamily(routed.family)) {
                impl_->FailGeneration(&transaction, frontier);
                result.error = CanonicalBundleErrorV1::kIdentityMismatch;
                return result;
            }
            std::size_t sink_index = impl_->config.sinks.size();
            for (std::size_t index = 0U;
                 index < impl_->config.sinks.size(); ++index) {
                if (impl_->config.sinks[index].family == routed.family &&
                    impl_->config.sinks[index].shard == routed.shard) {
                    sink_index = index;
                    break;
                }
            }
            if (sink_index == impl_->config.sinks.size()) {
                impl_->FailGeneration(&transaction, frontier);
                result.error = CanonicalBundleErrorV1::kMissingSink;
                return result;
            }
            per_sink[sink_index].push_back(RecordBytes(routed.record));
        }

        for (std::size_t index = 0U;
             index < impl_->config.sinks.size(); ++index) {
            const CanonicalSegmentWriterV1& writer =
                *impl_->config.sinks[index].writer;
            if (writer.header().processed_raw_ingress_sequence !=
                    frontier.processed_ingress_sequence ||
                writer.header().processed_raw_wal_pos !=
                    frontier.processed_global_wal_pos ||
                !DescriptorMatchesFrontier(
                    writer.descriptor(), frontier)) {
                impl_->FailGeneration(&transaction, frontier);
                result.error = CanonicalBundleErrorV1::kIdentityMismatch;
                return result;
            }
            result.segment_error = writer.PreflightRecords(per_sink[index]);
            if (result.segment_error != CanonicalSegmentErrorV1::kNone) {
                if (result.segment_error ==
                    CanonicalSegmentErrorV1::kSegmentFull) {
                    static_cast<void>(impl_->AbortPrepared(
                        &transaction, frontier));
                } else {
                    impl_->FailGeneration(&transaction, frontier);
                }
                result.error = CanonicalBundleErrorV1::kSinkPreflightFailed;
                return result;
            }
        }

        std::size_t publish_ordinal = 0U;
        for (const RoutedCanonicalRecordV1& routed :
             transaction.records()) {
            std::size_t sink_index = 0U;
            while (impl_->config.sinks[sink_index].family != routed.family ||
                   impl_->config.sinks[sink_index].shard != routed.shard) {
                ++sink_index;
            }
            if (!impl_->Hook(
                    CanonicalBundleOperationV1::kPublishRecord,
                    publish_ordinal)) {
                impl_->FailGeneration(&transaction, frontier);
                result.error = CanonicalBundleErrorV1::kInjectedFailure;
                return result;
            }
            result.segment_error =
                impl_->config.sinks[sink_index].writer->PublishRecord(
                    RecordBytes(routed.record));
            if (result.segment_error != CanonicalSegmentErrorV1::kNone) {
                impl_->FailGeneration(&transaction, frontier);
                result.error = CanonicalBundleErrorV1::kSinkPublishFailed;
                return result;
            }
            ++publish_ordinal;
            ++result.published_records;
        }

        CanonicalPublicationContractV1 receipt{};
        if (!ComputeCanonicalPublicationContractV1(
                transaction.records(), &receipt)) {
            impl_->FailGeneration(&transaction, frontier);
            result.error = CanonicalBundleErrorV1::kReceiptFailure;
            return result;
        }
        if (!impl_->Hook(
                CanonicalBundleOperationV1::kCommitNormalizer, 0U)) {
            impl_->FailGeneration(&transaction, frontier);
            result.error = CanonicalBundleErrorV1::kInjectedFailure;
            return result;
        }
        result.commit_error = impl_->config.normalizer->CommitPublished(
            &transaction, receipt);
        if (result.commit_error !=
            CanonicalNormalizeCommitErrorV1::kNone) {
            impl_->FailGeneration(&transaction, frontier);
            result.error = CanonicalBundleErrorV1::kNormalizerCommitFailed;
            return result;
        }

        for (std::size_t index = 0U;
             index < impl_->config.sinks.size(); ++index) {
            if (!impl_->Hook(
                    CanonicalBundleOperationV1::kAdvanceSegmentProgress,
                    index)) {
                impl_->FailGeneration(nullptr, frontier);
                result.error = CanonicalBundleErrorV1::kInjectedFailure;
                return result;
            }
            result.segment_error =
                impl_->config.sinks[index].writer->AdvanceProcessedRaw(
                    raw.origin_ingress_sequence,
                    raw.origin_wal_end_pos);
            if (result.segment_error != CanonicalSegmentErrorV1::kNone) {
                impl_->FailGeneration(nullptr, frontier);
                result.error = CanonicalBundleErrorV1::kSegmentProgressFailed;
                return result;
            }
        }

        if (!impl_->Hook(
                CanonicalBundleOperationV1::kCommitSourceFrontier, 0U)) {
            impl_->FailGeneration(nullptr, frontier);
            result.error = CanonicalBundleErrorV1::kInjectedFailure;
            return result;
        }
        result.frontier_error = PublishProcessedProgressWithRetry(
            impl_->config.source_frontier,
            raw.source_writer_instance,
            raw.source_generation,
            raw.origin_ingress_sequence,
            raw.origin_wal_end_pos,
            recv_monotonic_ns,
            impl_->config.source_frontier_busy_timeout);
        if (result.frontier_error != SourceFrontierErrorV1::kNone) {
            impl_->FailGeneration(nullptr, frontier);
            result.error = CanonicalBundleErrorV1::kFrontierCommitFailed;
            return result;
        }
        return result;
    } catch (const std::bad_alloc&) {
        if (!impl_->config.normalizer->Abort(&transaction)) {
            impl_->FailGeneration(&transaction, frontier);
        }
        result.error = CanonicalBundleErrorV1::kResourceExhausted;
        return result;
    } catch (...) {
        if (!impl_->config.normalizer->Abort(&transaction)) {
            impl_->FailGeneration(&transaction, frontier);
        }
        result.error = CanonicalBundleErrorV1::kInvalidConfiguration;
        return result;
    }
}

CanonicalBundleResultV1
CanonicalBundleCoordinatorV1::Impl::PublishProgressOnly(
    std::uint64_t ingress_sequence,
    std::uint64_t wal_pos,
    std::int64_t recv_monotonic_ns,
    CanonicalBundleResultV1 result,
    const SourceFrontierV1& frontier) noexcept {
    // Complete every non-mutating check before the first sink cursor moves.
    // Once fan-out starts, any failure revokes the whole generation because
    // sibling sinks may no longer expose an identical processed prefix.
    for (std::size_t index = 0U; index < config.sinks.size(); ++index) {
        const CanonicalSegmentWriterV1& writer =
            *config.sinks[index].writer;
        if (writer.header().processed_raw_ingress_sequence !=
                frontier.processed_ingress_sequence ||
            writer.header().processed_raw_wal_pos !=
                frontier.processed_global_wal_pos ||
            !DescriptorMatchesFrontier(writer.descriptor(), frontier)) {
            FailGeneration(nullptr, frontier);
            result.error = CanonicalBundleErrorV1::kIdentityMismatch;
            return result;
        }
        result.segment_error = writer.PreflightRecords({});
        if (result.segment_error != CanonicalSegmentErrorV1::kNone) {
            FailGeneration(nullptr, frontier);
            result.error = CanonicalBundleErrorV1::kSinkPreflightFailed;
            return result;
        }
    }

    for (std::size_t index = 0U; index < config.sinks.size(); ++index) {
        if (!Hook(
                CanonicalBundleOperationV1::kAdvanceSegmentProgress,
                index)) {
            FailGeneration(nullptr, frontier);
            result.error = CanonicalBundleErrorV1::kInjectedFailure;
            return result;
        }
        result.segment_error = config.sinks[index].writer->AdvanceProcessedRaw(
            ingress_sequence, wal_pos);
        if (result.segment_error != CanonicalSegmentErrorV1::kNone) {
            FailGeneration(nullptr, frontier);
            result.error = CanonicalBundleErrorV1::kSegmentProgressFailed;
            return result;
        }
    }

    if (!Hook(CanonicalBundleOperationV1::kCommitSourceFrontier, 0U)) {
        FailGeneration(nullptr, frontier);
        result.error = CanonicalBundleErrorV1::kInjectedFailure;
        return result;
    }
    result.frontier_error = PublishProcessedProgressWithRetry(
        config.source_frontier,
        frontier.writer_instance,
        frontier.generation,
        ingress_sequence,
        wal_pos,
        recv_monotonic_ns,
        config.source_frontier_busy_timeout);
    if (result.frontier_error != SourceFrontierErrorV1::kNone) {
        FailGeneration(nullptr, frontier);
        result.error = CanonicalBundleErrorV1::kFrontierCommitFailed;
    }
    return result;
}

std::string_view CanonicalCommittedReadErrorNameV1(
    CanonicalCommittedReadErrorV1 error) noexcept {
    switch (error) {
        case CanonicalCommittedReadErrorV1::kNone:
            return "none";
        case CanonicalCommittedReadErrorV1::kNullArgument:
            return "null_argument";
        case CanonicalCommittedReadErrorV1::kInvalidFrontier:
            return "invalid_frontier";
        case CanonicalCommittedReadErrorV1::kSegmentReadFailed:
            return "segment_read_failed";
        case CanonicalCommittedReadErrorV1::kIdentityMismatch:
            return "identity_mismatch";
        case CanonicalCommittedReadErrorV1::kRecordCorrupt:
            return "record_corrupt";
        case CanonicalCommittedReadErrorV1::kNotCommitted:
            return "not_committed";
    }
    return "invalid_committed_read_error";
}

CanonicalCommittedReadErrorV1 ReadCommittedCanonicalRecordV1(
    const SourceFrontierPageV1& frontier_page,
    const CanonicalSegmentReaderV1& reader,
    std::uint64_t record_index,
    std::span<const std::byte>* record,
    SourceFrontierV1* proof) noexcept {
    if (record == nullptr || proof == nullptr) {
        return CanonicalCommittedReadErrorV1::kNullArgument;
    }
    *record = {};
    *proof = {};
    SourceFrontierV1 first{};
    if (ReadSourceFrontierV1(frontier_page, &first) !=
        SourceFrontierErrorV1::kNone) {
        return CanonicalCommittedReadErrorV1::kInvalidFrontier;
    }
    if (!DescriptorMatchesFrontier(reader.header().descriptor, first)) {
        return CanonicalCommittedReadErrorV1::kIdentityMismatch;
    }
    if (first.source_state == SourceStateV1::kFatal) {
        return CanonicalCommittedReadErrorV1::kNotCommitted;
    }
    CanonicalSegmentControlSnapshotV1 segment_control{};
    if (reader.ReadControl(&segment_control) !=
        CanonicalSegmentErrorV1::kNone) {
        return CanonicalCommittedReadErrorV1::kSegmentReadFailed;
    }
    if (segment_control.generation_fatal) {
        return CanonicalCommittedReadErrorV1::kNotCommitted;
    }
    std::span<const std::byte> candidate{};
    if (reader.PublishedRecord(record_index, &candidate) !=
        CanonicalSegmentErrorV1::kNone) {
        return CanonicalCommittedReadErrorV1::kSegmentReadFailed;
    }
    if (!ValidateRecordBytes(
            reader.header().descriptor.event_type, candidate) ||
        candidate.size() < sizeof(CanonicalHeaderV1)) {
        return CanonicalCommittedReadErrorV1::kRecordCorrupt;
    }
    CanonicalHeaderV1 header{};
    std::memcpy(&header, candidate.data(), sizeof(header));
    if (header.source_stream_id != first.source_stream_id ||
        header.event_type != reader.header().descriptor.event_type) {
        return CanonicalCommittedReadErrorV1::kIdentityMismatch;
    }
    SourceFrontierV1 second{};
    if (ReadSourceFrontierV1(frontier_page, &second) !=
        SourceFrontierErrorV1::kNone) {
        return CanonicalCommittedReadErrorV1::kInvalidFrontier;
    }
    if (!SameFrontierIdentity(first, second) ||
        !DescriptorMatchesFrontier(reader.header().descriptor, second)) {
        return CanonicalCommittedReadErrorV1::kIdentityMismatch;
    }
    if (second.source_state == SourceStateV1::kFatal) {
        return CanonicalCommittedReadErrorV1::kNotCommitted;
    }
    if (!RecordCoveredByProcessed(header, second)) {
        return CanonicalCommittedReadErrorV1::kNotCommitted;
    }
    // Re-read the independent segment latch so a segment-local failure cannot
    // be hidden by an earlier healthy control observation.
    if (reader.ReadControl(&segment_control) !=
        CanonicalSegmentErrorV1::kNone) {
        return CanonicalCommittedReadErrorV1::kSegmentReadFailed;
    }
    if (segment_control.generation_fatal) {
        return CanonicalCommittedReadErrorV1::kNotCommitted;
    }
    // FailGeneration publishes SourceFrontier FATAL before it fans out to the
    // segments.  A final frontier read after the final segment read closes
    // that ordering: a coordinator fail-stop that started after `second` but
    // before the segment observation cannot return a stale healthy view.
    SourceFrontierV1 third{};
    if (ReadSourceFrontierV1(frontier_page, &third) !=
        SourceFrontierErrorV1::kNone) {
        return CanonicalCommittedReadErrorV1::kInvalidFrontier;
    }
    if (!SameFrontierIdentity(first, third) ||
        !DescriptorMatchesFrontier(reader.header().descriptor, third)) {
        return CanonicalCommittedReadErrorV1::kIdentityMismatch;
    }
    if (third.source_state == SourceStateV1::kFatal ||
        !RecordCoveredByProcessed(header, third)) {
        return CanonicalCommittedReadErrorV1::kNotCommitted;
    }
    *record = candidate;
    *proof = third;
    return CanonicalCommittedReadErrorV1::kNone;
}

}  // namespace l2flow::canonical
