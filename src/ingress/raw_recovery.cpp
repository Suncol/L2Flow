#include "l2flow/ingress/raw_recovery.h"

#include <algorithm>
#include <array>
#include <limits>
#include <new>
#include <span>
#include <utility>

namespace l2flow::ingress {
namespace {

struct ParsedSegment final {
    std::shared_ptr<const std::vector<std::byte>> bytes;
    SegmentHeaderV1 header{};
};

struct CandidateMarker final {
    DurableMarkerV1 marker{};
    RawV1DurableMarkerWire wire{};
    std::uint64_t journal_offset = 0U;
};

[[nodiscard]] bool CheckedAdd(
    std::uint64_t left,
    std::uint64_t right,
    std::uint64_t* result) noexcept {
    if (result == nullptr ||
        left > std::numeric_limits<std::uint64_t>::max() - right) {
        return false;
    }
    *result = left + right;
    return true;
}

[[nodiscard]] bool SizeToU64(
    std::size_t value,
    std::uint64_t* result) noexcept {
    if (result == nullptr) {
        return false;
    }
    if constexpr (sizeof(std::size_t) > sizeof(std::uint64_t)) {
        if (value >
            static_cast<std::size_t>(
                std::numeric_limits<std::uint64_t>::max())) {
            return false;
        }
    }
    *result = static_cast<std::uint64_t>(value);
    return true;
}

void SetFatal(
    RawRecoveryPlanV1* plan,
    RawRecoveryFatalV1 fatal,
    std::uint64_t evidence_offset,
    RawV1Error codec_error = RawV1Error::kNone,
    RawReaderError reader_error = RawReaderError::kNone) noexcept {
    if (plan == nullptr ||
        plan->fatal != RawRecoveryFatalV1::kNone) {
        return;
    }
    plan->fatal = fatal;
    plan->codec_error = codec_error;
    plan->reader_error = reader_error;
    plan->evidence_offset = evidence_offset;
}

[[nodiscard]] const ParsedSegment* FindSegment(
    const std::vector<ParsedSegment>& segments,
    std::uint32_t sequence) noexcept {
    const auto found = std::lower_bound(
        segments.begin(),
        segments.end(),
        sequence,
        [](const ParsedSegment& candidate, std::uint32_t value) {
            return candidate.header.segment_sequence < value;
        });
    if (found == segments.end() ||
        found->header.segment_sequence != sequence) {
        return nullptr;
    }
    return &*found;
}

[[nodiscard]] RawRecoverySegmentPlanV1* FindSegmentPlan(
    std::vector<RawRecoverySegmentPlanV1>* segments,
    std::uint32_t sequence) noexcept {
    if (segments == nullptr) {
        return nullptr;
    }
    const auto found = std::find_if(
        segments->begin(),
        segments->end(),
        [sequence](const RawRecoverySegmentPlanV1& candidate) {
            return candidate.segment_sequence == sequence;
        });
    return found == segments->end() ? nullptr : &*found;
}

[[nodiscard]] bool MarkerBytesEqual(
    const RawV1DurableMarkerWire& left,
    const RawV1DurableMarkerWire& right) noexcept {
    return std::equal(
        left.begin(), left.end(), right.begin(), right.end());
}

[[nodiscard]] bool IsBoundaryReaderError(
    RawReaderError error) noexcept {
    switch (error) {
        case RawReaderError::kDurableLimitBeforeData:
        case RawReaderError::kDurableLimitPastBuffer:
        case RawReaderError::kCursorOverflow:
        case RawReaderError::kRecordPastDurableLimit:
        case RawReaderError::kDurableLimitNotRecordBoundary:
            return true;
        case RawReaderError::kNone:
        case RawReaderError::kNullBuffer:
        case RawReaderError::kSegmentHeaderTruncated:
        case RawReaderError::kSegmentHeaderInvalid:
        case RawReaderError::kRecordHeaderInvalid:
        case RawReaderError::kRecordSizeInvalid:
        case RawReaderError::kPayloadCrcMismatch:
        case RawReaderError::kNonZeroPadding:
        case RawReaderError::kTrailerInvalid:
        case RawReaderError::kNamespaceMismatch:
        case RawReaderError::kIngressSequenceMismatch:
        case RawReaderError::kVendorHeadMismatch:
        case RawReaderError::kResourceExhausted:
            return false;
    }
    return false;
}

[[nodiscard]] bool IsPartialTailReaderError(
    RawReaderError error) noexcept {
    return error ==
               RawReaderError::kRecordPastDurableLimit ||
           error ==
               RawReaderError::kDurableLimitNotRecordBoundary;
}

[[nodiscard]] bool IsAllZero(
    std::span<const std::byte> bytes) noexcept {
    return std::all_of(
        bytes.begin(), bytes.end(), [](std::byte value) {
            return value == std::byte{0};
        });
}

[[nodiscard]] RawRecoveryPlanV1 AnalyzeImpl(
    const RawRecoveryInputV1& input) {
    RawRecoveryPlanV1 plan;
    if (input.journal == nullptr) {
        SetFatal(
            &plan,
            RawRecoveryFatalV1::kJournalBufferMissing,
            0U);
        return plan;
    }
    if (input.journal->size() < kRawV1JournalHeaderBytes) {
        SetFatal(
            &plan,
            RawRecoveryFatalV1::kJournalHeaderTruncated,
            static_cast<std::uint64_t>(
                input.journal->size()));
        return plan;
    }

    const RawV1Error journal_error =
        DecodeDurableJournalHeaderV1(
            std::span<const std::byte>(
                input.journal->data(),
                kRawV1JournalHeaderBytes),
            &plan.journal_header);
    if (journal_error != RawV1Error::kNone) {
        SetFatal(
            &plan,
            RawRecoveryFatalV1::kJournalHeaderInvalid,
            0U,
            journal_error);
        return plan;
    }
    plan.accepted_journal_size = kRawV1JournalHeaderBytes;

    std::vector<ParsedSegment> parsed_segments;
    parsed_segments.reserve(input.segments.size());
    for (const RawRecoverySegmentInputV1& input_segment :
         input.segments) {
        if (input_segment.bytes == nullptr) {
            SetFatal(
                &plan,
                RawRecoveryFatalV1::kSegmentBufferMissing,
                0U);
            return plan;
        }
        if (input_segment.bytes->size() <
            kRawV1SegmentHeaderBytes) {
            SetFatal(
                &plan,
                RawRecoveryFatalV1::kSegmentHeaderTruncated,
                static_cast<std::uint64_t>(
                    input_segment.bytes->size()));
            return plan;
        }

        ParsedSegment parsed;
        parsed.bytes = input_segment.bytes;
        const RawV1Error segment_error =
            DecodeSegmentHeaderV1(
                std::span<const std::byte>(
                    parsed.bytes->data(),
                    kRawV1SegmentHeaderBytes),
                &parsed.header);
        if (segment_error != RawV1Error::kNone) {
            SetFatal(
                &plan,
                RawRecoveryFatalV1::kSegmentHeaderInvalid,
                0U,
                segment_error);
            return plan;
        }
        if (parsed.header.source_stream_id !=
                plan.journal_header.source_stream_id ||
            parsed.header.capture_date !=
                plan.journal_header.capture_date ||
            parsed.header.stream_day_id !=
                plan.journal_header.stream_day_id ||
            parsed.header.raw_schema_sha256 !=
                plan.journal_header.raw_schema_sha256) {
            SetFatal(
                &plan,
                RawRecoveryFatalV1::kNamespaceMismatch,
                0U);
            return plan;
        }
        if (!parsed_segments.empty() &&
            parsed.header.segment_sequence <=
                parsed_segments.back()
                    .header.segment_sequence) {
            SetFatal(
                &plan,
                RawRecoveryFatalV1::kSegmentOrderInvalid,
                0U);
            return plan;
        }
        parsed_segments.push_back(std::move(parsed));
    }

    const std::size_t marker_bytes =
        input.journal->size() - kRawV1JournalHeaderBytes;
    const std::size_t full_marker_count =
        marker_bytes / kRawV1DurableMarkerBytes;
    const std::size_t marker_remainder =
        marker_bytes % kRawV1DurableMarkerBytes;
    std::vector<CandidateMarker> candidates;
    candidates.reserve(full_marker_count);

    bool terminal_crc_rollback = false;
    for (std::size_t index = 0U;
         index < full_marker_count;
         ++index) {
        const std::size_t marker_offset =
            kRawV1JournalHeaderBytes +
            index * kRawV1DurableMarkerBytes;
        CandidateMarker candidate;
        std::copy_n(
            input.journal->data() + marker_offset,
            kRawV1DurableMarkerBytes,
            candidate.wire.begin());
        candidate.journal_offset =
            static_cast<std::uint64_t>(marker_offset);
        const RawV1Error marker_error =
            DecodeDurableMarkerV1(
                candidate.wire, &candidate.marker);
        if (marker_error != RawV1Error::kNone) {
            const bool is_exact_terminal =
                index + 1U == full_marker_count &&
                marker_remainder == 0U;
            if (is_exact_terminal &&
                marker_error ==
                    RawV1Error::kHeaderCrcMismatch) {
                plan.journal_tail =
                    RawRecoveryJournalTailV1::
                        kTerminalCrcInvalidMarker;
                terminal_crc_rollback = true;
                break;
            }
            SetFatal(
                &plan,
                RawRecoveryFatalV1::
                    kJournalMarkerCorruption,
                candidate.journal_offset,
                marker_error);
            return plan;
        }

        if (candidate.marker.source_stream_id !=
            plan.journal_header.source_stream_id) {
            SetFatal(
                &plan,
                RawRecoveryFatalV1::kNamespaceMismatch,
                candidate.journal_offset);
            return plan;
        }
        const ParsedSegment* current_segment =
            FindSegment(
                parsed_segments,
                candidate.marker.segment_sequence);
        if (current_segment == nullptr) {
            SetFatal(
                &plan,
                RawRecoveryFatalV1::kSegmentMissing,
                candidate.journal_offset);
            return plan;
        }

        std::uint64_t expected_global = 0U;
        if (!CheckedAdd(
                current_segment->header.segment_base_wal_pos,
                candidate.marker.durable_segment_offset,
                &expected_global) ||
            expected_global !=
                candidate.marker.durable_global_wal_pos) {
            SetFatal(
                &plan,
                RawRecoveryFatalV1::
                    kJournalChainViolation,
                candidate.journal_offset);
            return plan;
        }

        if (candidates.empty()) {
            if (candidate.marker.segment_sequence != 1U ||
                current_segment->header.segment_sequence != 1U ||
                current_segment->header.segment_base_wal_pos !=
                    0U ||
                current_segment->header.first_ingress_sequence !=
                    1U ||
                candidate.marker.durable_segment_offset !=
                    kRawV1SegmentHeaderBytes ||
                candidate.marker.durable_global_wal_pos !=
                    kRawV1SegmentHeaderBytes ||
                candidate.marker.durable_ingress_sequence != 0U ||
                candidate.marker.marker_flags != 0U) {
                SetFatal(
                    &plan,
                    RawRecoveryFatalV1::
                        kJournalChainViolation,
                    candidate.journal_offset);
                return plan;
            }
        } else {
            const CandidateMarker& previous =
                candidates.back();
            if (candidate.marker.segment_sequence ==
                previous.marker.segment_sequence) {
                const bool same_cursor =
                    candidate.marker
                            .durable_segment_offset ==
                        previous.marker
                            .durable_segment_offset &&
                    candidate.marker
                            .durable_global_wal_pos ==
                        previous.marker
                            .durable_global_wal_pos &&
                    candidate.marker
                            .durable_ingress_sequence ==
                        previous.marker
                            .durable_ingress_sequence;
                if (same_cursor) {
                    const bool identical =
                        MarkerBytesEqual(
                            candidate.wire,
                            previous.wire);
                    const bool seal_transition =
                        previous.marker.marker_flags == 0U &&
                        candidate.marker.marker_flags ==
                            kRawV1SegmentSealed;
                    if (!identical && !seal_transition) {
                        SetFatal(
                            &plan,
                            RawRecoveryFatalV1::
                                kJournalChainViolation,
                            candidate.journal_offset);
                        return plan;
                    }
                } else if (
                    previous.marker.marker_flags ==
                        kRawV1SegmentSealed ||
                    candidate.marker
                            .durable_segment_offset <=
                        previous.marker
                            .durable_segment_offset ||
                    candidate.marker.durable_global_wal_pos <=
                        previous.marker
                            .durable_global_wal_pos ||
                    candidate.marker
                            .durable_ingress_sequence <
                        previous.marker
                            .durable_ingress_sequence) {
                    SetFatal(
                        &plan,
                        RawRecoveryFatalV1::
                            kJournalChainViolation,
                        candidate.journal_offset);
                    return plan;
                }
            } else {
                std::uint32_t next_sequence = 0U;
                if (previous.marker.segment_sequence ==
                        std::numeric_limits<
                            std::uint32_t>::max() ||
                    (next_sequence =
                         previous.marker.segment_sequence +
                         1U,
                     candidate.marker.segment_sequence !=
                         next_sequence) ||
                    previous.marker.marker_flags !=
                        kRawV1SegmentSealed ||
                    candidate.marker.durable_segment_offset !=
                        kRawV1SegmentHeaderBytes ||
                    candidate.marker
                            .durable_ingress_sequence !=
                        previous.marker
                            .durable_ingress_sequence ||
                    candidate.marker.marker_flags != 0U) {
                    SetFatal(
                        &plan,
                        RawRecoveryFatalV1::
                            kJournalChainViolation,
                        candidate.journal_offset);
                    return plan;
                }

                const ParsedSegment* previous_segment =
                    FindSegment(
                        parsed_segments,
                        previous.marker.segment_sequence);
                std::uint64_t expected_base = 0U;
                if (previous_segment == nullptr ||
                    !CheckedAdd(
                        previous_segment->header
                            .segment_base_wal_pos,
                        previous.marker
                            .durable_segment_offset,
                        &expected_base) ||
                    current_segment->header
                            .segment_base_wal_pos !=
                        expected_base ||
                    previous.marker
                            .durable_ingress_sequence ==
                        std::numeric_limits<
                            std::uint64_t>::max() ||
                    current_segment->header
                            .first_ingress_sequence !=
                        previous.marker
                                .durable_ingress_sequence +
                            1U) {
                    SetFatal(
                        &plan,
                        RawRecoveryFatalV1::
                            kSegmentBaseMismatch,
                        candidate.journal_offset);
                    return plan;
                }
            }
        }
        candidates.push_back(std::move(candidate));
    }

    if (!terminal_crc_rollback && marker_remainder != 0U) {
        plan.journal_tail =
            RawRecoveryJournalTailV1::kPartialMarker;
    }

    if (candidates.empty()) {
        if (parsed_segments.empty()) {
            plan.initial_anchor =
                RawRecoveryInitialAnchorV1::kJournalOnly;
            return plan;
        }
        if (parsed_segments.size() == 1U &&
            parsed_segments.front()
                    .header.segment_sequence == 1U &&
            parsed_segments.front()
                    .header.segment_base_wal_pos == 0U &&
            parsed_segments.front()
                    .header.first_ingress_sequence == 1U &&
            IsAllZero(
                std::span<const std::byte>(
                    parsed_segments.front().bytes->data() +
                        kRawV1SegmentHeaderBytes,
                    parsed_segments.front().bytes->size() -
                        kRawV1SegmentHeaderBytes))) {
            plan.initial_anchor =
                RawRecoveryInitialAnchorV1::
                    kSegmentHeaderOnly;
            RawRecoverySegmentPlanV1 initial;
            initial.segment_sequence = 1U;
            initial.segment_base_wal_pos = 0U;
            initial.validated_logical_end_offset =
                kRawV1SegmentHeaderBytes;
            initial.tail_begin_offset =
                kRawV1SegmentHeaderBytes;
            initial.tail_end_offset =
                static_cast<std::uint64_t>(
                    parsed_segments.front().bytes->size());
            if (initial.tail_end_offset >
                initial.tail_begin_offset) {
                initial.tail =
                    RawRecoverySegmentTailV1::
                        kZeroPreallocation;
            }
            plan.segments.push_back(initial);
        } else {
            SetFatal(
                &plan,
                RawRecoveryFatalV1::kUnexpectedSegment,
                0U);
        }
        return plan;
    }

    plan.segments.reserve(parsed_segments.size());
    for (const CandidateMarker& candidate : candidates) {
        const ParsedSegment* parsed =
            FindSegment(
                parsed_segments,
                candidate.marker.segment_sequence);
        if (parsed == nullptr) {
            SetFatal(
                &plan,
                RawRecoveryFatalV1::kSegmentMissing,
                candidate.journal_offset);
            return plan;
        }

        const RawSegmentScanResult scan =
            ScanRawSegmentV1(
                parsed->bytes,
                candidate.marker.durable_segment_offset);
        if (!scan.ok()) {
            if (scan.error ==
                RawReaderError::kResourceExhausted) {
                SetFatal(
                    &plan,
                    RawRecoveryFatalV1::
                        kResourceExhausted,
                    scan.error_offset,
                    scan.codec_error,
                    scan.error);
            } else if (IsBoundaryReaderError(scan.error)) {
                SetFatal(
                    &plan,
                    RawRecoveryFatalV1::
                        kJournalSemanticViolation,
                    candidate.journal_offset,
                    scan.codec_error,
                    scan.error);
            } else {
                SetFatal(
                    &plan,
                    RawRecoveryFatalV1::
                        kRawDurableCorruption,
                    scan.error_offset,
                    scan.codec_error,
                    scan.error);
            }
            return plan;
        }

        std::uint64_t expected_ingress = 0U;
        if (candidate.marker.durable_segment_offset ==
            kRawV1SegmentHeaderBytes) {
            if (scan.segment.first_ingress_sequence == 0U) {
                SetFatal(
                    &plan,
                    RawRecoveryFatalV1::
                        kJournalSemanticViolation,
                    candidate.journal_offset);
                return plan;
            }
            expected_ingress =
                scan.segment.first_ingress_sequence - 1U;
        } else if (scan.records.empty()) {
            SetFatal(
                &plan,
                RawRecoveryFatalV1::
                    kJournalSemanticViolation,
                candidate.journal_offset);
            return plan;
        } else {
            expected_ingress =
                scan.records.back()
                    .metadata().ingress_sequence;
        }
        if (expected_ingress !=
                candidate.marker
                    .durable_ingress_sequence ||
            scan.validated_end_offset !=
                candidate.marker
                    .durable_segment_offset ||
            scan.validated_end_wal_pos !=
                candidate.marker
                    .durable_global_wal_pos) {
            SetFatal(
                &plan,
                RawRecoveryFatalV1::
                    kJournalSemanticViolation,
                candidate.journal_offset);
            return plan;
        }

        const bool sealed =
            candidate.marker.marker_flags ==
            kRawV1SegmentSealed;
        if (sealed &&
            parsed->bytes->size() !=
                static_cast<std::size_t>(
                    candidate.marker
                        .durable_segment_offset)) {
            SetFatal(
                &plan,
                RawRecoveryFatalV1::
                    kSealedSegmentHasTail,
                candidate.marker
                    .durable_segment_offset);
            return plan;
        }

        RawRecoverySegmentPlanV1* segment_plan =
            FindSegmentPlan(
                &plan.segments,
                candidate.marker.segment_sequence);
        if (segment_plan == nullptr) {
            RawRecoverySegmentPlanV1 created;
            created.segment_sequence =
                candidate.marker.segment_sequence;
            created.segment_base_wal_pos =
                parsed->header.segment_base_wal_pos;
            created.has_accepted_marker = true;
            created.accepted_marker_wire =
                candidate.wire;
            created.durable_end_offset =
                candidate.marker.durable_segment_offset;
            created.validated_logical_end_offset =
                candidate.marker.durable_segment_offset;
            created.validated_last_ingress_sequence =
                candidate.marker.durable_ingress_sequence;
            created.append_only_begin_offset =
                candidate.marker.durable_segment_offset;
            created.append_only_end_offset =
                candidate.marker.durable_segment_offset;
            created.tail_begin_offset =
                candidate.marker.durable_segment_offset;
            created.tail_end_offset =
                candidate.marker.durable_segment_offset;
            created.sealed = sealed;
            plan.segments.push_back(created);
        } else {
            segment_plan->has_accepted_marker = true;
            segment_plan->accepted_marker_wire =
                candidate.wire;
            segment_plan->durable_end_offset =
                candidate.marker.durable_segment_offset;
            segment_plan->validated_logical_end_offset =
                candidate.marker.durable_segment_offset;
            segment_plan->validated_last_ingress_sequence =
                candidate.marker.durable_ingress_sequence;
            segment_plan->append_only_begin_offset =
                candidate.marker.durable_segment_offset;
            segment_plan->append_only_end_offset =
                candidate.marker.durable_segment_offset;
            segment_plan->tail_begin_offset =
                candidate.marker.durable_segment_offset;
            segment_plan->tail_end_offset =
                candidate.marker.durable_segment_offset;
            segment_plan->sealed =
                segment_plan->sealed || sealed;
        }

        plan.accepted_journal_size =
            candidate.journal_offset +
            kRawV1DurableMarkerBytes;
        plan.has_accepted_cursor = true;
        plan.accepted_cursor = {
            candidate.marker.segment_sequence,
            candidate.marker.durable_global_wal_pos,
            candidate.marker.durable_ingress_sequence,
            candidate.marker.durable_segment_offset,
            candidate.marker.marker_flags};
    }

    const std::uint32_t highest_sequence =
        candidates.back().marker.segment_sequence;
    const ParsedSegment* orphan = nullptr;
    for (const ParsedSegment& parsed : parsed_segments) {
        if (parsed.header.segment_sequence >
                highest_sequence ||
            FindSegmentPlan(
                &plan.segments,
                parsed.header.segment_sequence) ==
                nullptr) {
            if (orphan != nullptr) {
                SetFatal(
                    &plan,
                    RawRecoveryFatalV1::kUnexpectedSegment,
                    0U);
                return plan;
            }
            orphan = &parsed;
        }
    }

    RawRecoverySegmentPlanV1* highest_plan =
        FindSegmentPlan(
            &plan.segments, highest_sequence);
    const ParsedSegment* highest_segment =
        FindSegment(parsed_segments, highest_sequence);
    if (highest_plan == nullptr ||
        highest_segment == nullptr) {
        SetFatal(
            &plan,
            RawRecoveryFatalV1::kSegmentMissing,
            0U);
        return plan;
    }
    if (orphan != nullptr) {
        const CandidateMarker& preceding =
            candidates.back();
        std::uint32_t expected_sequence = 0U;
        std::uint64_t expected_first_ingress = 0U;
        const bool normal =
            orphan->header.segment_flags == 0U;
        const bool continuation =
            orphan->header.segment_flags ==
            kRawV1FinalizationContinuation;
        if (!highest_plan->sealed ||
            preceding.marker.marker_flags !=
                kRawV1SegmentSealed ||
            highest_sequence ==
                std::numeric_limits<std::uint32_t>::max() ||
            (expected_sequence = highest_sequence + 1U,
             orphan->header.segment_sequence !=
                 expected_sequence) ||
            preceding.marker.durable_ingress_sequence ==
                std::numeric_limits<std::uint64_t>::max() ||
            (expected_first_ingress =
                 preceding.marker
                         .durable_ingress_sequence +
                 1U,
             orphan->header.first_ingress_sequence !=
                 expected_first_ingress) ||
            orphan->header.segment_base_wal_pos !=
                preceding.marker.durable_global_wal_pos ||
            (!normal && !continuation) ||
            !IsAllZero(
                std::span<const std::byte>(
                    orphan->bytes->data() +
                        kRawV1SegmentHeaderBytes,
                    orphan->bytes->size() -
                        kRawV1SegmentHeaderBytes))) {
            SetFatal(
                &plan,
                RawRecoveryFatalV1::kUnexpectedSegment,
                0U);
            return plan;
        }

        RawRecoverySegmentPlanV1 orphan_plan;
        orphan_plan.segment_sequence =
            orphan->header.segment_sequence;
        orphan_plan.segment_base_wal_pos =
            orphan->header.segment_base_wal_pos;
        orphan_plan.validated_logical_end_offset =
            kRawV1SegmentHeaderBytes;
        orphan_plan.validated_last_ingress_sequence =
            preceding.marker.durable_ingress_sequence;
        orphan_plan.append_only_begin_offset =
            kRawV1SegmentHeaderBytes;
        orphan_plan.append_only_end_offset =
            kRawV1SegmentHeaderBytes;
        orphan_plan.tail_begin_offset =
            kRawV1SegmentHeaderBytes;
        orphan_plan.tail_end_offset =
            static_cast<std::uint64_t>(
                orphan->bytes->size());
        if (orphan_plan.tail_end_offset >
            orphan_plan.tail_begin_offset) {
            orphan_plan.tail =
                RawRecoverySegmentTailV1::
                    kZeroPreallocation;
        }
        plan.segments.push_back(orphan_plan);
        plan.r11_orphan =
            normal
                ? RawRecoveryR11OrphanV1::
                      kNormalHeaderOnly
                : RawRecoveryR11OrphanV1::
                      kFinalizationContinuationHeaderOnly;
        return plan;
    }
    if (highest_plan->sealed) {
        return plan;
    }

    std::uint64_t physical_size = 0U;
    if (!SizeToU64(
            highest_segment->bytes->size(),
            &physical_size)) {
        SetFatal(
            &plan,
            RawRecoveryFatalV1::kResourceExhausted,
            0U);
        return plan;
    }
    const RawSegmentScanResult full_scan =
        ScanRawSegmentV1(
            highest_segment->bytes, physical_size);
    if (full_scan.error ==
        RawReaderError::kResourceExhausted) {
        SetFatal(
            &plan,
            RawRecoveryFatalV1::kResourceExhausted,
            full_scan.error_offset,
            full_scan.codec_error,
            full_scan.error);
        return plan;
    }

    const std::uint64_t validated_end =
        full_scan.ok()
            ? physical_size
            : full_scan.validated_end_offset;
    if (validated_end <
        highest_plan->durable_end_offset) {
        SetFatal(
            &plan,
            RawRecoveryFatalV1::kRawDurableCorruption,
            full_scan.error_offset,
            full_scan.codec_error,
            full_scan.error);
        return plan;
    }

    highest_plan->validated_logical_end_offset =
        validated_end;
    if (!full_scan.records.empty()) {
        highest_plan->validated_last_ingress_sequence =
            full_scan.records.back()
                .metadata().ingress_sequence;
    }
    highest_plan->append_only_begin_offset =
        highest_plan->durable_end_offset;
    highest_plan->append_only_end_offset =
        validated_end;
    highest_plan->tail_begin_offset = validated_end;
    highest_plan->tail_end_offset = validated_end;
    if (full_scan.ok()) {
        return plan;
    }

    std::size_t tail_begin = 0U;
    if (validated_end >
        static_cast<std::uint64_t>(
            std::numeric_limits<std::size_t>::max())) {
        SetFatal(
            &plan,
            RawRecoveryFatalV1::kResourceExhausted,
            validated_end);
        return plan;
    }
    tail_begin = static_cast<std::size_t>(validated_end);
    highest_plan->tail_end_offset = physical_size;
    const std::span<const std::byte> tail_bytes(
        highest_segment->bytes->data() + tail_begin,
        highest_segment->bytes->size() - tail_begin);
    if (IsAllZero(tail_bytes)) {
        highest_plan->tail =
            RawRecoverySegmentTailV1::kZeroPreallocation;
    } else if (IsPartialTailReaderError(full_scan.error)) {
        highest_plan->tail =
            RawRecoverySegmentTailV1::kPartialRecord;
    } else {
        highest_plan->tail =
            RawRecoverySegmentTailV1::kInvalidRecord;
    }
    return plan;
}

}  // namespace

std::string_view RawRecoveryFatalV1Name(
    RawRecoveryFatalV1 fatal) noexcept {
    switch (fatal) {
        case RawRecoveryFatalV1::kNone:
            return "none";
        case RawRecoveryFatalV1::kJournalBufferMissing:
            return "journal buffer missing";
        case RawRecoveryFatalV1::kJournalHeaderTruncated:
            return "journal header truncated";
        case RawRecoveryFatalV1::kJournalHeaderInvalid:
            return "journal header invalid";
        case RawRecoveryFatalV1::kSegmentBufferMissing:
            return "segment buffer missing";
        case RawRecoveryFatalV1::kSegmentHeaderTruncated:
            return "segment header truncated";
        case RawRecoveryFatalV1::kSegmentHeaderInvalid:
            return "segment header invalid";
        case RawRecoveryFatalV1::kSegmentOrderInvalid:
            return "segment order invalid";
        case RawRecoveryFatalV1::kNamespaceMismatch:
            return "namespace mismatch";
        case RawRecoveryFatalV1::kSegmentMissing:
            return "segment missing";
        case RawRecoveryFatalV1::kUnexpectedSegment:
            return "unexpected segment";
        case RawRecoveryFatalV1::kSegmentBaseMismatch:
            return "segment base mismatch";
        case RawRecoveryFatalV1::kJournalMarkerCorruption:
            return "journal marker corruption";
        case RawRecoveryFatalV1::kJournalChainViolation:
            return "journal chain violation";
        case RawRecoveryFatalV1::kJournalSemanticViolation:
            return "journal semantic violation";
        case RawRecoveryFatalV1::kRawDurableCorruption:
            return "Raw durable corruption";
        case RawRecoveryFatalV1::kSealedSegmentHasTail:
            return "sealed segment has tail";
        case RawRecoveryFatalV1::kResourceExhausted:
            return "resource exhausted";
    }
    return "unknown Raw recovery fatal";
}

RawRecoveryPlanV1 AnalyzeRawRecoveryV1(
    const RawRecoveryInputV1& input) noexcept {
    try {
        return AnalyzeImpl(input);
    } catch (const std::bad_alloc&) {
        RawRecoveryPlanV1 plan;
        plan.fatal = RawRecoveryFatalV1::kResourceExhausted;
        return plan;
    } catch (...) {
        RawRecoveryPlanV1 plan;
        plan.fatal = RawRecoveryFatalV1::kResourceExhausted;
        return plan;
    }
}

}  // namespace l2flow::ingress
