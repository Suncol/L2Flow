#include "l2flow/ingress/raw_fault_transform.h"

#include "l2flow/common/identity128.h"

#include <algorithm>
#include <limits>
#include <new>
#include <set>
#include <tuple>
#include <utility>

namespace l2flow::ingress {
namespace {

constexpr std::uint64_t kDropPurpose =
    0x64726f705f763031ULL;
constexpr std::uint64_t kDuplicatePurpose =
    0x6475706c5f763031ULL;
constexpr std::uint64_t kMutationPurpose =
    0x6d7574615f763031ULL;
constexpr std::uint64_t kMutationOffsetPurpose =
    0x6d75746f5f763031ULL;
constexpr std::uint64_t kReorderPurpose =
    0x72656f72645f7631ULL;

[[nodiscard]] bool AddU64(
    std::uint64_t left,
    std::uint64_t right,
    std::uint64_t* result) noexcept {
    if (result == nullptr ||
        left > std::numeric_limits<std::uint64_t>::max() -
            right) {
        return false;
    }
    *result = left + right;
    return true;
}

[[nodiscard]] bool MulU64(
    std::uint64_t left,
    std::uint64_t right,
    std::uint64_t* result) noexcept {
    if (result == nullptr ||
        (left != 0U &&
         right >
             std::numeric_limits<std::uint64_t>::max() /
                 left)) {
        return false;
    }
    *result = left * right;
    return true;
}

[[nodiscard]] bool SizeToU64(
    std::size_t value,
    std::uint64_t* result) noexcept {
    if (result == nullptr) {
        return false;
    }
    if constexpr (sizeof(std::size_t) >
                  sizeof(std::uint64_t)) {
        if (value >
            static_cast<std::size_t>(
                std::numeric_limits<std::uint64_t>::max())) {
            return false;
        }
    }
    *result = static_cast<std::uint64_t>(value);
    return true;
}

[[nodiscard]] bool U64ToSize(
    std::uint64_t value,
    std::size_t* result) noexcept {
    if (result == nullptr) {
        return false;
    }
    if constexpr (sizeof(std::size_t) <
                  sizeof(std::uint64_t)) {
        if (value >
            static_cast<std::uint64_t>(
                std::numeric_limits<std::size_t>::max())) {
            return false;
        }
    }
    *result = static_cast<std::size_t>(value);
    return true;
}

template <std::size_t Size>
[[nodiscard]] bool IsAllZero(
    const std::array<std::byte, Size>& bytes) noexcept {
    return std::all_of(
        bytes.begin(),
        bytes.end(),
        [](std::byte value) {
            return value == std::byte{0U};
        });
}

// This is the published SplitMix64 finalizer.  Unsigned overflow is the
// specified modulo-2^64 operation and therefore identical on every platform.
[[nodiscard]] std::uint64_t SplitMix64(
    std::uint64_t value) noexcept {
    value += 0x9e3779b97f4a7c15ULL;
    value =
        (value ^ (value >> 30U)) *
        0xbf58476d1ce4e5b9ULL;
    value =
        (value ^ (value >> 27U)) *
        0x94d049bb133111ebULL;
    return value ^ (value >> 31U);
}

[[nodiscard]] std::uint64_t StableDraw(
    std::uint64_t seed,
    std::uint64_t ordinal,
    std::uint64_t purpose,
    std::uint64_t lane = 0U) noexcept {
    const std::uint64_t ordinal_component =
        SplitMix64(ordinal ^ 0xa0761d6478bd642fULL);
    const std::uint64_t lane_component =
        SplitMix64(lane ^ 0xe7037ed1a0b428dbULL);
    return SplitMix64(
        seed ^ purpose ^ ordinal_component ^
        lane_component);
}

[[nodiscard]] bool Selected(
    std::uint64_t draw,
    std::uint64_t one_in) noexcept {
    return one_in != 0U && draw % one_in == 0U;
}

[[nodiscard]] bool KnownProvenance(
    RawReplayProvenance provenance) noexcept {
    switch (provenance) {
        case RawReplayProvenance::kDurable:
        case RawReplayProvenance::kRecoveredAppendOnly:
            return true;
    }
    return false;
}

[[nodiscard]] bool IsValidLogicalIdentity(
    const InjectedRawPlanIdentity& identity) noexcept {
    return identity.plan_magic ==
               kInjectedRawUnframedPlanMagic &&
        identity.synthetic &&
        !common::IsZeroIdentity(identity.run_id) &&
        !common::IsZeroIdentity(
            identity.synthetic_namespace_id) &&
        identity.run_id != identity.synthetic_namespace_id &&
        !IsAllZero(identity.raw_schema_sha256) &&
        !IsAllZero(
            identity.injected_schema_identity_sha256) &&
        !IsAllZero(identity.parent_raw_identity_sha256) &&
        !IsAllZero(identity.fault_rule_sha256);
}

[[nodiscard]] bool ValidatedContextMatches(
    const RawReplayRecord& input,
    const InjectedRawPlanIdentity& identity) noexcept {
    const RawRecordHeaderV1& header = input.view.header();
    std::uint64_t wire_size = 0U;
    if (header.source_stream_id == 0U ||
        header.capture_date == 0U ||
        header.ingress_sequence == 0U ||
        input.segment.source_stream_id !=
            header.source_stream_id ||
        input.segment.capture_date != header.capture_date ||
        input.segment.segment_sequence == 0U ||
        common::IsZeroIdentity(input.segment.stream_day_id) ||
        input.segment.stream_day_id ==
            identity.synthetic_namespace_id ||
        input.segment.stream_day_id == identity.run_id ||
        input.view.vendor_head().size() !=
            kVendorMessageHeadBytes ||
        input.view.vendor_body().size() !=
            static_cast<std::size_t>(
                header.vendor_body_size) ||
        input.view.record_start_wal_pos() >=
            input.view.record_end_wal_pos() ||
        !KnownProvenance(input.provenance) ||
        !SizeToU64(
            static_cast<std::size_t>(header.record_size),
            &wire_size)) {
        return false;
    }
    return input.view.record_end_wal_pos() -
               input.view.record_start_wal_pos() ==
           wire_size;
}

using ParentKey = std::tuple<
    std::uint32_t,
    std::uint32_t,
    RawV1Identity,
    std::uint64_t,
    std::uint64_t,
    std::uint64_t>;

[[nodiscard]] ParentKey MakeParentKey(
    const RawReplayRecord& input) {
    return ParentKey{
        input.segment.capture_date,
        input.segment.source_stream_id,
        input.segment.stream_day_id,
        input.view.header().ingress_sequence,
        input.view.record_start_wal_pos(),
        input.view.record_end_wal_pos()};
}

[[nodiscard]] InjectedRawParentLocator MakeParentLocator(
    const RawReplayRecord& input,
    std::uint32_t occurrence) noexcept {
    return InjectedRawParentLocator{
        input.segment.capture_date,
        input.segment.source_stream_id,
        input.segment.stream_day_id,
        input.view.header().ingress_sequence,
        input.view.record_start_wal_pos(),
        input.view.record_end_wal_pos(),
        occurrence};
}

struct LogicalDecision final {
    bool dropped = false;
    std::uint64_t occurrences = 0U;
    bool mutate = false;
    std::size_t mutation_offset = 0U;
};

[[nodiscard]] RawLogicalFaultError ValidateLogicalRule(
    const RawLogicalFaultRule& rule,
    const RawLogicalFaultLimits& limits) noexcept {
    switch (rule.algorithm) {
        case RawLogicalSelectionAlgorithm::
            kSplitMix64StatelessV1:
            break;
        default:
            return RawLogicalFaultError::kUnknownAlgorithm;
    }
    switch (rule.mutation) {
        case RawLogicalMutationKind::kNone:
        case RawLogicalMutationKind::kVendorBodyByteXor:
            break;
        default:
            return RawLogicalFaultError::kUnknownMutation;
    }
    if (limits.max_input_records == 0U ||
        limits.max_output_records == 0U ||
        limits.max_total_vendor_bytes == 0U ||
        limits.max_reorder_window == 0U) {
        return RawLogicalFaultError::kInvalidLimits;
    }
    if (rule.duplicate_additional_copies >
            limits.max_duplicate_additional_copies ||
        rule.reorder_window > limits.max_reorder_window) {
        return RawLogicalFaultError::kResourceLimitExceeded;
    }
    if ((rule.duplicate_one_in == 0U) !=
            (rule.duplicate_additional_copies == 0U) ||
        (rule.mutate_one_in == 0U) !=
            (rule.mutation ==
             RawLogicalMutationKind::kNone) ||
        (rule.mutation ==
             RawLogicalMutationKind::kVendorBodyByteXor &&
         rule.mutation_xor_mask == std::byte{0U}) ||
        rule.duplicate_additional_copies ==
            std::numeric_limits<std::uint32_t>::max()) {
        return RawLogicalFaultError::kInvalidRule;
    }
    return RawLogicalFaultError::kNone;
}

[[nodiscard]] bool IsResourceReaderError(
    const RawSegmentScanResult& result) noexcept {
    return result.error ==
        RawReaderError::kResourceExhausted;
}

}  // namespace

std::string_view RawLogicalFaultErrorName(
    RawLogicalFaultError error) noexcept {
    switch (error) {
        case RawLogicalFaultError::kNone:
            return "none";
        case RawLogicalFaultError::kNullOutput:
            return "null_output";
        case RawLogicalFaultError::kNullInput:
            return "null_input";
        case RawLogicalFaultError::kUnknownAlgorithm:
            return "unknown_algorithm";
        case RawLogicalFaultError::kUnknownMutation:
            return "unknown_mutation";
        case RawLogicalFaultError::kInvalidRule:
            return "invalid_rule";
        case RawLogicalFaultError::kInvalidLimits:
            return "invalid_limits";
        case RawLogicalFaultError::kInvalidPlanIdentity:
            return "invalid_plan_identity";
        case RawLogicalFaultError::
            kRawAndInjectedSchemaIdentityAlias:
            return "raw_and_injected_schema_identity_alias";
        case RawLogicalFaultError::
            kInvalidValidatedInputContext:
            return "invalid_validated_input_context";
        case RawLogicalFaultError::kDuplicateParentLocator:
            return "duplicate_parent_locator";
        case RawLogicalFaultError::kEmptyMutationTarget:
            return "empty_mutation_target";
        case RawLogicalFaultError::kArithmeticOverflow:
            return "arithmetic_overflow";
        case RawLogicalFaultError::kResourceLimitExceeded:
            return "resource_limit_exceeded";
        case RawLogicalFaultError::kResourceExhausted:
            return "resource_exhausted";
    }
    return "unknown";
}

RawLogicalFaultError BuildRawLogicalFaultPlan(
    std::span<const RawReplayRecord> input,
    const InjectedRawPlanIdentity& identity,
    const RawLogicalFaultRule& rule,
    const RawLogicalFaultLimits& limits,
    InjectedRawTransformPlan* output) noexcept {
    if (output == nullptr) {
        return RawLogicalFaultError::kNullOutput;
    }
    *output = InjectedRawTransformPlan{};
    if (input.data() == nullptr && !input.empty()) {
        return RawLogicalFaultError::kNullInput;
    }
    const RawLogicalFaultError rule_error =
        ValidateLogicalRule(rule, limits);
    if (rule_error != RawLogicalFaultError::kNone) {
        return rule_error;
    }
    if (!IsValidLogicalIdentity(identity)) {
        return RawLogicalFaultError::kInvalidPlanIdentity;
    }
    if (identity.raw_schema_sha256 ==
        identity.injected_schema_identity_sha256) {
        return RawLogicalFaultError::
            kRawAndInjectedSchemaIdentityAlias;
    }

    std::uint64_t input_count = 0U;
    if (!SizeToU64(input.size(), &input_count)) {
        return RawLogicalFaultError::kArithmeticOverflow;
    }
    if (input_count > limits.max_input_records) {
        return RawLogicalFaultError::kResourceLimitExceeded;
    }

    try {
        std::vector<LogicalDecision> decisions;
        decisions.reserve(input.size());
        std::set<ParentKey> parent_keys;

        std::uint64_t output_count = 0U;
        std::uint64_t total_vendor_bytes = 0U;
        std::optional<RawReplaySegmentContext>
            first_context;

        for (std::size_t index = 0U;
             index < input.size();
             ++index) {
            const RawReplayRecord& record = input[index];
            if (!ValidatedContextMatches(record, identity)) {
                return RawLogicalFaultError::
                    kInvalidValidatedInputContext;
            }
            if (!first_context.has_value()) {
                first_context = record.segment;
            } else if (
                record.segment.source_stream_id !=
                    first_context->source_stream_id ||
                record.segment.capture_date !=
                    first_context->capture_date ||
                record.segment.stream_day_id !=
                    first_context->stream_day_id) {
                return RawLogicalFaultError::
                    kInvalidValidatedInputContext;
            }
            if (!parent_keys.insert(MakeParentKey(record)).
                    second) {
                return RawLogicalFaultError::
                    kDuplicateParentLocator;
            }

            const std::uint64_t ordinal =
                static_cast<std::uint64_t>(index);
            LogicalDecision decision;
            decision.dropped = Selected(
                StableDraw(
                    rule.seed, ordinal, kDropPurpose),
                rule.drop_one_in);
            if (!decision.dropped) {
                decision.occurrences = 1U;
                if (Selected(
                        StableDraw(
                            rule.seed,
                            ordinal,
                            kDuplicatePurpose),
                        rule.duplicate_one_in)) {
                    if (!AddU64(
                            decision.occurrences,
                            static_cast<std::uint64_t>(
                                rule.
                                    duplicate_additional_copies),
                            &decision.occurrences)) {
                        return RawLogicalFaultError::
                            kArithmeticOverflow;
                    }
                }
                decision.mutate = Selected(
                    StableDraw(
                        rule.seed,
                        ordinal,
                        kMutationPurpose),
                    rule.mutate_one_in);
                if (decision.mutate) {
                    if (record.view.vendor_body().empty()) {
                        return RawLogicalFaultError::
                            kEmptyMutationTarget;
                    }
                    const std::uint64_t body_size =
                        static_cast<std::uint64_t>(
                            record.view.vendor_body().size());
                    const std::uint64_t selected_offset =
                        StableDraw(
                            rule.seed,
                            ordinal,
                            kMutationOffsetPurpose) %
                        body_size;
                    if (!U64ToSize(
                            selected_offset,
                            &decision.mutation_offset)) {
                        return RawLogicalFaultError::
                            kArithmeticOverflow;
                    }
                }

                if (!AddU64(
                        output_count,
                        decision.occurrences,
                        &output_count)) {
                    return RawLogicalFaultError::
                        kArithmeticOverflow;
                }
                std::uint64_t per_occurrence = 0U;
                if (!AddU64(
                        static_cast<std::uint64_t>(
                            record.view.vendor_head().size()),
                        static_cast<std::uint64_t>(
                            record.view.vendor_body().size()),
                        &per_occurrence)) {
                    return RawLogicalFaultError::
                        kArithmeticOverflow;
                }
                std::uint64_t record_total = 0U;
                if (!MulU64(
                        per_occurrence,
                        decision.occurrences,
                        &record_total) ||
                    !AddU64(
                        total_vendor_bytes,
                        record_total,
                        &total_vendor_bytes)) {
                    return RawLogicalFaultError::
                        kArithmeticOverflow;
                }
            }
            decisions.push_back(decision);
        }

        if (output_count > limits.max_output_records ||
            total_vendor_bytes >
                limits.max_total_vendor_bytes) {
            return RawLogicalFaultError::
                kResourceLimitExceeded;
        }

        std::size_t output_size = 0U;
        if (!U64ToSize(output_count, &output_size)) {
            return RawLogicalFaultError::
                kArithmeticOverflow;
        }

        InjectedRawTransformPlan candidate;
        candidate.identity = identity;
        candidate.rule = rule;
        candidate.records.reserve(output_size);
        candidate.dropped_locators.reserve(
            input.size() - std::min(input.size(), output_size));

        for (std::size_t index = 0U;
             index < input.size();
             ++index) {
            const RawReplayRecord& source = input[index];
            const LogicalDecision& decision =
                decisions[index];
            if (decision.dropped) {
                candidate.dropped_locators.push_back(
                    MakeParentLocator(source, 1U));
                continue;
            }

            for (std::uint64_t occurrence = 1U;
                 occurrence <= decision.occurrences;
                 ++occurrence) {
                InjectedRawPlanRecord transformed;
                const RawRecordHeaderV1& header =
                    source.view.header();
                transformed.capture_meta.source_stream_id =
                    header.source_stream_id;
                transformed.capture_meta.
                    connection_epoch_hint =
                        header.connection_epoch_hint;
                transformed.capture_meta.recv_realtime_ns =
                    header.recv_realtime_ns;
                transformed.capture_meta.recv_monotonic_ns =
                    header.recv_monotonic_ns;
                transformed.capture_meta.capture_date =
                    header.capture_date;
                transformed.capture_meta.flags = 0U;
                std::copy(
                    source.view.vendor_head().begin(),
                    source.view.vendor_head().end(),
                    transformed.vendor_head.begin());
                transformed.vendor_body.assign(
                    source.view.vendor_body().begin(),
                    source.view.vendor_body().end());
                transformed.parent = MakeParentLocator(
                    source,
                    static_cast<std::uint32_t>(
                        occurrence));
                transformed.parent_provenance =
                    source.provenance;

                if (decision.mutate) {
                    const std::byte before =
                        transformed.vendor_body[
                            decision.mutation_offset];
                    const std::byte after =
                        before ^ rule.mutation_xor_mask;
                    transformed.vendor_body[
                        decision.mutation_offset] = after;
                    transformed.mutation =
                        InjectedRawMutationEvidence{
                            rule.mutation,
                            static_cast<std::uint64_t>(
                                decision.mutation_offset),
                            before,
                            after};
                }
                candidate.records.push_back(
                    std::move(transformed));
            }
        }

        const std::size_t window =
            static_cast<std::size_t>(
                rule.reorder_window);
        if (window >= 2U) {
            std::uint64_t chunk_ordinal = 0U;
            for (std::size_t begin = 0U;
                 begin < candidate.records.size();
                 ++chunk_ordinal) {
                const std::size_t remaining =
                    candidate.records.size() - begin;
                const std::size_t chunk_size =
                    std::min(window, remaining);
                for (std::size_t count = chunk_size;
                     count > 1U;
                     --count) {
                    const std::uint64_t draw =
                        StableDraw(
                            rule.seed,
                            chunk_ordinal,
                            kReorderPurpose,
                            static_cast<std::uint64_t>(
                                count));
                    const std::size_t selected =
                        static_cast<std::size_t>(
                            draw %
                            static_cast<std::uint64_t>(
                                count));
                    std::swap(
                        candidate.records[
                            begin + count - 1U],
                        candidate.records[
                            begin + selected]);
                }
                begin += chunk_size;
            }
        }

        for (std::size_t index = 0U;
             index < candidate.records.size();
             ++index) {
            std::uint64_t zero_based_sequence = 0U;
            std::uint64_t sequence = 0U;
            if (!SizeToU64(index, &zero_based_sequence) ||
                !AddU64(
                    zero_based_sequence, 1U, &sequence)) {
                return RawLogicalFaultError::
                    kArithmeticOverflow;
            }
            InjectedRawPlanRecord& record =
                candidate.records[index];
            record.synthetic_ingress_sequence =
                sequence;
            record.capture_meta.ingress_sequence =
                sequence;
        }

        *output = std::move(candidate);
    } catch (const std::bad_alloc&) {
        return RawLogicalFaultError::kResourceExhausted;
    } catch (...) {
        return RawLogicalFaultError::kResourceExhausted;
    }
    return RawLogicalFaultError::kNone;
}

std::string_view RawPhysicalFaultErrorName(
    RawPhysicalFaultError error) noexcept {
    switch (error) {
        case RawPhysicalFaultError::kNone:
            return "none";
        case RawPhysicalFaultError::kNullOutput:
            return "null_output";
        case RawPhysicalFaultError::kNullSource:
            return "null_source";
        case RawPhysicalFaultError::kUnknownSegmentState:
            return "unknown_segment_state";
        case RawPhysicalFaultError::kUnknownRewriteKind:
            return "unknown_rewrite_kind";
        case RawPhysicalFaultError::kInvalidLimits:
            return "invalid_limits";
        case RawPhysicalFaultError::kInvalidBoundary:
            return "invalid_boundary";
        case RawPhysicalFaultError::kInvalidRewriteRange:
            return "invalid_rewrite_range";
        case RawPhysicalFaultError::kZeroRewriteMask:
            return "zero_rewrite_mask";
        case RawPhysicalFaultError::kOriginalRawInvalid:
            return "original_raw_invalid";
        case RawPhysicalFaultError::
            kRewriteDidNotInvalidateObject:
            return "rewrite_did_not_invalidate_object";
        case RawPhysicalFaultError::kOracleMismatch:
            return "oracle_mismatch";
        case RawPhysicalFaultError::kArithmeticOverflow:
            return "arithmetic_overflow";
        case RawPhysicalFaultError::kResourceLimitExceeded:
            return "resource_limit_exceeded";
        case RawPhysicalFaultError::kResourceExhausted:
            return "resource_exhausted";
    }
    return "unknown";
}

RawPhysicalFaultError CreateRawPhysicalCorruptionFixture(
    const RawPhysicalCorruptionRequest& request,
    const RawPhysicalFaultLimits& limits,
    RawPhysicalCorruptionFixture* output) noexcept {
    if (output == nullptr) {
        return RawPhysicalFaultError::kNullOutput;
    }
    *output = RawPhysicalCorruptionFixture{};
    if (request.source_bytes == nullptr) {
        return RawPhysicalFaultError::kNullSource;
    }
    switch (request.segment_state) {
        case RawPhysicalSegmentState::kSealed:
        case RawPhysicalSegmentState::kHighestOpen:
            break;
        default:
            return RawPhysicalFaultError::
                kUnknownSegmentState;
    }
    switch (request.rewrite_kind) {
        case RawPhysicalRewriteKind::kXorMask:
            break;
        default:
            return RawPhysicalFaultError::
                kUnknownRewriteKind;
    }
    if (limits.max_source_bytes <
            kRawV1SegmentHeaderBytes ||
        limits.max_rewrite_bytes == 0U) {
        return RawPhysicalFaultError::kInvalidLimits;
    }
    if (request.xor_mask == std::byte{0U}) {
        return RawPhysicalFaultError::kZeroRewriteMask;
    }

    std::uint64_t source_size = 0U;
    if (!SizeToU64(
            request.source_bytes->size(),
            &source_size)) {
        return RawPhysicalFaultError::kArithmeticOverflow;
    }
    if (source_size > limits.max_source_bytes ||
        request.rewrite_length >
            limits.max_rewrite_bytes) {
        return RawPhysicalFaultError::
            kResourceLimitExceeded;
    }
    if (request.durable_end_offset <
            kRawV1SegmentHeaderBytes ||
        request.logical_end_offset <
            request.durable_end_offset ||
        request.logical_end_offset > source_size) {
        return RawPhysicalFaultError::kInvalidBoundary;
    }
    if (request.segment_state ==
            RawPhysicalSegmentState::kSealed &&
        (request.durable_end_offset !=
             request.logical_end_offset ||
         request.logical_end_offset != source_size)) {
        return RawPhysicalFaultError::kInvalidBoundary;
    }

    std::uint64_t rewrite_end = 0U;
    if (request.rewrite_length == 0U) {
        return RawPhysicalFaultError::
            kInvalidRewriteRange;
    }
    if (!AddU64(
            request.rewrite_offset,
            request.rewrite_length,
            &rewrite_end)) {
        return RawPhysicalFaultError::kArithmeticOverflow;
    }
    if (rewrite_end > request.logical_end_offset) {
        return RawPhysicalFaultError::
            kInvalidRewriteRange;
    }

    const RawSegmentScanResult original_durable =
        ScanRawSegmentV1(
            request.source_bytes,
            request.durable_end_offset);
    const RawSegmentScanResult original_logical =
        ScanRawSegmentV1(
            request.source_bytes,
            request.logical_end_offset);
    if (!original_durable.ok() ||
        !original_logical.ok()) {
        if (IsResourceReaderError(original_durable) ||
            IsResourceReaderError(original_logical)) {
            return RawPhysicalFaultError::
                kResourceExhausted;
        }
        return RawPhysicalFaultError::kOriginalRawInvalid;
    }

    RawPhysicalOriginalDurability original_class =
        RawPhysicalOriginalDurability::
            kHighestOpenDurable;
    if (request.segment_state ==
        RawPhysicalSegmentState::kSealed) {
        original_class =
            RawPhysicalOriginalDurability::
                kSealedDurable;
    } else if (
        rewrite_end <= request.durable_end_offset) {
        original_class =
            RawPhysicalOriginalDurability::
                kHighestOpenDurable;
    } else if (
        request.rewrite_offset >=
        request.durable_end_offset) {
        original_class =
            RawPhysicalOriginalDurability::
                kHighestOpenAppendOnly;
    } else {
        original_class =
            RawPhysicalOriginalDurability::
                kHighestOpenDurableBoundaryOverlap;
    }

    try {
        auto derived =
            std::make_shared<std::vector<std::byte>>(
                *request.source_bytes);
        std::size_t rewrite_begin_size = 0U;
        std::size_t rewrite_end_size = 0U;
        if (!U64ToSize(
                request.rewrite_offset,
                &rewrite_begin_size) ||
            !U64ToSize(
                rewrite_end, &rewrite_end_size)) {
            return RawPhysicalFaultError::
                kArithmeticOverflow;
        }
        for (std::size_t offset = rewrite_begin_size;
             offset < rewrite_end_size;
             ++offset) {
            (*derived)[offset] ^= request.xor_mask;
        }

        std::shared_ptr<const std::vector<std::byte>>
            immutable = derived;
        RawPhysicalCorruptionOracle oracle;
        oracle.corrupted_begin_offset =
            request.rewrite_offset;
        oracle.corrupted_end_offset = rewrite_end;
        oracle.durable_end_offset =
            request.durable_end_offset;
        oracle.original_logical_end_offset =
            request.logical_end_offset;
        oracle.original_durability = original_class;

        if (original_class ==
            RawPhysicalOriginalDurability::
                kHighestOpenAppendOnly) {
            const RawSegmentScanResult durable_after =
                ScanRawSegmentV1(
                    immutable,
                    request.durable_end_offset);
            const RawSegmentScanResult logical_after =
                ScanRawSegmentV1(
                    immutable,
                    request.logical_end_offset);
            if (IsResourceReaderError(durable_after) ||
                IsResourceReaderError(logical_after)) {
                return RawPhysicalFaultError::
                    kResourceExhausted;
            }
            if (!durable_after.ok()) {
                return RawPhysicalFaultError::
                    kOracleMismatch;
            }
            if (logical_after.ok()) {
                return RawPhysicalFaultError::
                    kRewriteDidNotInvalidateObject;
            }
            if (logical_after.validated_end_offset <
                request.durable_end_offset) {
                return RawPhysicalFaultError::
                    kOracleMismatch;
            }
            oracle.recovery_expectation =
                RawPhysicalRecoveryExpectation::
                    kTruncatedInvalidTail;
            oracle.durable_reader_expectation =
                RawPhysicalDurableReaderExpectation::
                    kHiddenByDurableBoundary;
            oracle.expected_invalid_tail_begin_offset =
                logical_after.validated_end_offset;
            oracle.expected_invalid_tail_end_offset =
                source_size;
            oracle.validating_reader_error =
                logical_after.error;
            oracle.validating_codec_error =
                logical_after.codec_error;
        } else {
            const RawSegmentScanResult durable_after =
                ScanRawSegmentV1(
                    immutable,
                    request.durable_end_offset);
            if (IsResourceReaderError(durable_after)) {
                return RawPhysicalFaultError::
                    kResourceExhausted;
            }
            if (durable_after.ok()) {
                return RawPhysicalFaultError::
                    kRewriteDidNotInvalidateObject;
            }
            oracle.recovery_expectation =
                RawPhysicalRecoveryExpectation::
                    kFatalRawCorruption;
            oracle.durable_reader_expectation =
                RawPhysicalDurableReaderExpectation::
                    kRejectCorruption;
            oracle.validating_reader_error =
                durable_after.error;
            oracle.validating_codec_error =
                durable_after.codec_error;
        }

        RawPhysicalCorruptionFixture candidate;
        candidate.derived_bytes = std::move(immutable);
        candidate.oracle = oracle;
        *output = std::move(candidate);
    } catch (const std::bad_alloc&) {
        return RawPhysicalFaultError::kResourceExhausted;
    } catch (...) {
        return RawPhysicalFaultError::kResourceExhausted;
    }
    return RawPhysicalFaultError::kNone;
}

}  // namespace l2flow::ingress
