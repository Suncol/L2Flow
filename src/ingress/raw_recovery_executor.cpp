#include "l2flow/ingress/raw_recovery_executor.h"

#include <array>
#include <cerrno>
#include <limits>
#include <span>

namespace l2flow::ingress {
namespace {

void Fail(
    RawRecoveryExecutionResultV1* result,
    RawRecoveryExecutionFailureV1 failure,
    int error_number) noexcept {
    if (result->failure !=
        RawRecoveryExecutionFailureV1::kNone) {
        return;
    }
    result->failure = failure;
    result->error_number =
        error_number == 0 ? EIO : error_number;
    result->cursor_publishable = false;
}

bool RetrySimple(
    auto&& operation,
    RawRecoveryExecutionResultV1* result,
    RawRecoveryExecutionFailureV1 failure) noexcept {
    for (;;) {
        const int error_number = operation();
        if (error_number == 0) {
            return true;
        }
        if (error_number != EINTR) {
            Fail(result, failure, error_number);
            return false;
        }
    }
}

bool CheckedAdd(
    std::uint64_t left,
    std::uint64_t right,
    std::uint64_t* result) noexcept {
    if (left >
        std::numeric_limits<std::uint64_t>::max() - right) {
        return false;
    }
    *result = left + right;
    return true;
}

bool WriteAll(
    RawRecoveryIo& io,
    std::uint64_t offset,
    std::span<const std::byte> bytes,
    RawRecoveryExecutionResultV1* result) noexcept {
    std::size_t written = 0U;
    while (written < bytes.size()) {
        std::uint64_t current = 0U;
        if (!CheckedAdd(
                offset,
                static_cast<std::uint64_t>(written),
                &current)) {
            Fail(
                result,
                RawRecoveryExecutionFailureV1::
                    kCursorOverflow,
                EOVERFLOW);
            return false;
        }
        const RawRecoveryWriteResult step =
            io.WriteJournalSome(
                current, bytes.subspan(written));
        if (step.error_number == EINTR &&
            step.bytes_written == 0U) {
            continue;
        }
        if (step.error_number != 0) {
            Fail(
                result,
                RawRecoveryExecutionFailureV1::
                    kJournalWrite,
                step.error_number);
            return false;
        }
        if (step.bytes_written == 0U ||
            step.bytes_written >
                bytes.size() - written) {
            Fail(
                result,
                RawRecoveryExecutionFailureV1::
                    kJournalWrite,
                EIO);
            return false;
        }
        written += step.bytes_written;
    }
    result->mutated = true;
    return true;
}

bool AppendPromotionMarker(
    const RawRecoveryPlanV1& plan,
    const RawRecoverySegmentPlanV1& segment,
    std::uint64_t ingress_sequence,
    RawRecoveryIo& io,
    RawRecoveryExecutionResultV1* result) noexcept {
    std::uint64_t global = 0U;
    if (!CheckedAdd(
            segment.segment_base_wal_pos,
            segment.append_only_end_offset,
            &global)) {
        Fail(
            result,
            RawRecoveryExecutionFailureV1::kCursorOverflow,
            EOVERFLOW);
        return false;
    }
    DurableMarkerV1 marker;
    marker.source_stream_id =
        plan.journal_header.source_stream_id;
    marker.segment_sequence =
        segment.segment_sequence;
    marker.durable_global_wal_pos = global;
    marker.durable_ingress_sequence = ingress_sequence;
    marker.durable_segment_offset =
        segment.append_only_end_offset;
    marker.marker_flags = 0U;
    RawV1DurableMarkerWire wire{};
    if (EncodeDurableMarkerV1(marker, &wire) !=
        RawV1Error::kNone) {
        Fail(
            result,
            RawRecoveryExecutionFailureV1::kMarkerEncode,
            EINVAL);
        return false;
    }
    if (!WriteAll(
            io,
            result->retained_journal_size,
            wire,
            result)) {
        return false;
    }
    if (!CheckedAdd(
            result->retained_journal_size,
            kRawV1DurableMarkerBytes,
            &result->retained_journal_size)) {
        Fail(
            result,
            RawRecoveryExecutionFailureV1::kCursorOverflow,
            EOVERFLOW);
        return false;
    }
    if (!RetrySimple(
            [&io]() noexcept {
                return io.SyncJournal();
            },
            result,
            RawRecoveryExecutionFailureV1::kJournalSync)) {
        return false;
    }
    result->recovered_cursor = {
        segment.segment_sequence,
        global,
        ingress_sequence,
        segment.append_only_end_offset,
        0U};
    return true;
}

}  // namespace

RawRecoveryExecutionResultV1 ExecuteRawRecoveryPlanV1(
    const RawRecoveryPlanV1& plan,
    RawRecoveryIo& io) noexcept {
    RawRecoveryExecutionResultV1 result;
    result.retained_journal_size =
        plan.accepted_journal_size;
    if (plan.has_accepted_cursor) {
        result.recovered_cursor =
            plan.accepted_cursor;
    }
    if (!plan.ok()) {
        Fail(
            &result,
            RawRecoveryExecutionFailureV1::kPlanFatal,
            EINVAL);
        return result;
    }
    if (plan.journal_tail !=
            RawRecoveryJournalTailV1::kNone &&
        !io.DependentArtifactsAbsentProven()) {
        Fail(
            &result,
            RawRecoveryExecutionFailureV1::
                kDependentArtifactProofMissing,
            EPERM);
        return result;
    }
    if (plan.r11_orphan !=
            RawRecoveryR11OrphanV1::kNone &&
        !io.R11OrphanAdoptionAuthorized(plan)) {
        Fail(
            &result,
            RawRecoveryExecutionFailureV1::
                kR11OrphanAuthorizationMissing,
            EPERM);
        return result;
    }
    if (plan.accepted_journal_size <
            kRawV1JournalHeaderBytes ||
        (plan.accepted_journal_size -
         kRawV1JournalHeaderBytes) %
                kRawV1DurableMarkerBytes !=
            0U) {
        Fail(
            &result,
            RawRecoveryExecutionFailureV1::kInvalidPlan,
            EINVAL);
        return result;
    }

    bool requires_mutation =
        plan.journal_tail !=
            RawRecoveryJournalTailV1::kNone;
    for (const RawRecoverySegmentPlanV1& segment :
         plan.segments) {
        requires_mutation =
            requires_mutation ||
            segment.tail != RawRecoverySegmentTailV1::kNone ||
            segment.append_only_end_offset >
                segment.append_only_begin_offset;
    }
    if (plan.initial_anchor ==
        RawRecoveryInitialAnchorV1::kSegmentHeaderOnly) {
        requires_mutation = true;
    }
    if (plan.r11_orphan !=
        RawRecoveryR11OrphanV1::kNone) {
        requires_mutation = true;
    }

    if (!RetrySimple(
            [&io]() noexcept {
                return io.SyncParentDirectories();
            },
            &result,
            RawRecoveryExecutionFailureV1::
                kParentDirectorySync)) {
        return result;
    }

    if (plan.journal_tail !=
        RawRecoveryJournalTailV1::kNone) {
        if (!RetrySimple(
                [&io, &plan]() noexcept {
                    return io.TruncateJournal(
                        plan.accepted_journal_size);
                },
                &result,
                RawRecoveryExecutionFailureV1::
                    kJournalTruncate)) {
            return result;
        }
        result.mutated = true;
        if (!RetrySimple(
                [&io]() noexcept {
                    return io.SyncJournal();
                },
                &result,
                RawRecoveryExecutionFailureV1::
                    kJournalSync)) {
            return result;
        }
    }

    for (const RawRecoverySegmentPlanV1& segment :
         plan.segments) {
        if (segment.tail !=
                RawRecoverySegmentTailV1::kNone &&
            !(plan.initial_anchor ==
                  RawRecoveryInitialAnchorV1::
                      kSegmentHeaderOnly &&
              segment.segment_sequence == 1U) &&
            !(plan.r11_orphan !=
                  RawRecoveryR11OrphanV1::kNone &&
              &segment == &plan.segments.back())) {
            if (segment.sealed ||
                segment.tail_begin_offset !=
                    segment.validated_logical_end_offset ||
                segment.tail_end_offset <
                    segment.tail_begin_offset) {
                Fail(
                    &result,
                    RawRecoveryExecutionFailureV1::
                        kInvalidPlan,
                    EINVAL);
                return result;
            }
            if (!RetrySimple(
                    [&io, &segment]() noexcept {
                        return io.TruncateSegment(
                            segment.segment_sequence,
                            segment.validated_logical_end_offset);
                    },
                    &result,
                    RawRecoveryExecutionFailureV1::
                        kSegmentTruncate)) {
                return result;
            }
            result.mutated = true;
            if (!RetrySimple(
                    [&io, &segment]() noexcept {
                        return io.SyncSegment(
                            segment.segment_sequence,
                            false);
                    },
                    &result,
                    RawRecoveryExecutionFailureV1::
                        kSegmentSync)) {
                return result;
            }
        }
    }

    if (plan.initial_anchor ==
        RawRecoveryInitialAnchorV1::kSegmentHeaderOnly) {
        if (plan.segments.size() != 1U ||
            plan.segments.front().segment_sequence != 1U ||
            !RetrySimple(
                [&io]() noexcept {
                    return io.SyncSegment(1U, true);
                },
                &result,
                RawRecoveryExecutionFailureV1::
                    kSegmentSync)) {
            if (result.failure ==
                RawRecoveryExecutionFailureV1::kNone) {
                Fail(
                    &result,
                    RawRecoveryExecutionFailureV1::
                        kInvalidPlan,
                    EINVAL);
            }
                return result;
        }
        if (!RetrySimple(
                [&io]() noexcept {
                    return io.SyncParentDirectories();
                },
                &result,
                RawRecoveryExecutionFailureV1::
                    kParentDirectorySync)) {
            return result;
        }
        RawRecoverySegmentPlanV1 initial =
            plan.segments.front();
        initial.append_only_end_offset =
            kRawV1SegmentHeaderBytes;
        if (!AppendPromotionMarker(
                plan, initial, 0U, io, &result)) {
            return result;
        }
    }

    if (plan.r11_orphan !=
        RawRecoveryR11OrphanV1::kNone) {
        if (!plan.has_accepted_cursor ||
            plan.accepted_cursor.marker_flags !=
                kRawV1SegmentSealed ||
            plan.segments.size() < 2U) {
            Fail(
                &result,
                RawRecoveryExecutionFailureV1::kInvalidPlan,
                EINVAL);
            return result;
        }
        const RawRecoverySegmentPlanV1& orphan =
            plan.segments.back();
        if (orphan.segment_sequence !=
                    plan.accepted_cursor.segment_sequence +
                        1U ||
            orphan.validated_logical_end_offset !=
                kRawV1SegmentHeaderBytes ||
            orphan.append_only_begin_offset !=
                kRawV1SegmentHeaderBytes ||
            orphan.append_only_end_offset !=
                kRawV1SegmentHeaderBytes ||
            orphan.sealed ||
            !RetrySimple(
                [&io, &orphan]() noexcept {
                    return io.SyncSegment(
                        orphan.segment_sequence,
                        true);
                },
                &result,
                RawRecoveryExecutionFailureV1::
                    kSegmentSync) ||
            !RetrySimple(
                [&io]() noexcept {
                    return io.SyncParentDirectories();
                },
                &result,
                RawRecoveryExecutionFailureV1::
                    kParentDirectorySync) ||
            !AppendPromotionMarker(
                plan,
                orphan,
                plan.accepted_cursor.ingress_sequence,
                io,
                &result)) {
            if (result.failure ==
                RawRecoveryExecutionFailureV1::kNone) {
                Fail(
                    &result,
                    RawRecoveryExecutionFailureV1::
                        kInvalidPlan,
                    EINVAL);
            }
            return result;
        }
    }

    for (const RawRecoverySegmentPlanV1& segment :
         plan.segments) {
        if (plan.r11_orphan !=
                RawRecoveryR11OrphanV1::kNone &&
            &segment == &plan.segments.back()) {
            continue;
        }
        if (segment.append_only_end_offset <=
            segment.append_only_begin_offset) {
            continue;
        }
        if (segment.sealed ||
            segment.append_only_begin_offset !=
                segment.durable_end_offset ||
            segment.append_only_end_offset !=
                segment.validated_logical_end_offset) {
            Fail(
                &result,
                RawRecoveryExecutionFailureV1::kInvalidPlan,
                EINVAL);
            return result;
        }
        if (!RetrySimple(
                [&io, &segment]() noexcept {
                    return io.SyncSegment(
                        segment.segment_sequence,
                        false);
                },
                &result,
                RawRecoveryExecutionFailureV1::
                    kSegmentSync)) {
            return result;
        }

        // The analyzer obtained this value from the same complete validating
        // scan that established append_only_end_offset.
        if (!plan.has_accepted_cursor ||
            plan.accepted_cursor.segment_sequence !=
                segment.segment_sequence) {
            Fail(
                &result,
                RawRecoveryExecutionFailureV1::kInvalidPlan,
                EINVAL);
            return result;
        }
        const std::uint64_t promoted_ingress =
            segment.validated_last_ingress_sequence;
        if (promoted_ingress <=
                plan.accepted_cursor.ingress_sequence ||
            !AppendPromotionMarker(
                plan,
                segment,
                promoted_ingress,
                io,
                &result)) {
            if (result.failure ==
                RawRecoveryExecutionFailureV1::kNone) {
                Fail(
                    &result,
                    RawRecoveryExecutionFailureV1::
                        kCursorOverflow,
                    EOVERFLOW);
            }
            return result;
        }
    }

    if (!RetrySimple(
            [&io]() noexcept {
                return io.SyncJournal();
            },
            &result,
            RawRecoveryExecutionFailureV1::kJournalSync)) {
        return result;
    }
    result.cursor_publishable =
        plan.has_accepted_cursor ||
        plan.initial_anchor ==
            RawRecoveryInitialAnchorV1::kSegmentHeaderOnly ||
        plan.r11_orphan !=
            RawRecoveryR11OrphanV1::kNone;
    if (!requires_mutation) {
        result.mutated = false;
    }
    return result;
}

}  // namespace l2flow::ingress
