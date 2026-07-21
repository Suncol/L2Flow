#pragma once

#include "l2flow/ingress/raw_reader.h"
#include "l2flow/ingress/raw_v1.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>
#include <vector>

namespace l2flow::ingress {

// Recovery analysis is deliberately read-only. Each input buffer must remain
// immutable for the duration of AnalyzeRawRecoveryV1(); the resulting plan
// contains values and byte ranges only, never writable file handles or spans.
struct RawRecoverySegmentInputV1 final {
    std::shared_ptr<const std::vector<std::byte>> bytes;
};

struct RawRecoveryInputV1 final {
    std::shared_ptr<const std::vector<std::byte>> journal;
    // Segment headers must be in strictly increasing segment_sequence order.
    std::vector<RawRecoverySegmentInputV1> segments;
};

enum class RawRecoveryFatalV1 : std::uint8_t {
    kNone = 0U,
    kJournalBufferMissing,
    kJournalHeaderTruncated,
    kJournalHeaderInvalid,
    kSegmentBufferMissing,
    kSegmentHeaderTruncated,
    kSegmentHeaderInvalid,
    kSegmentOrderInvalid,
    kNamespaceMismatch,
    kSegmentMissing,
    kUnexpectedSegment,
    kSegmentBaseMismatch,
    kJournalMarkerCorruption,
    kJournalChainViolation,
    kJournalSemanticViolation,
    kRawDurableCorruption,
    kSealedSegmentHasTail,
    kResourceExhausted,
};

[[nodiscard]] std::string_view RawRecoveryFatalV1Name(
    RawRecoveryFatalV1 fatal) noexcept;

enum class RawRecoveryJournalTailV1 : std::uint8_t {
    kNone = 0U,
    // A terminal suffix shorter than one 48-byte marker may be rolled back.
    kPartialMarker,
    // Exactly one terminal, full-size marker failed only its CRC check.
    kTerminalCrcInvalidMarker,
};

enum class RawRecoveryInitialAnchorV1 : std::uint8_t {
    kNone = 0U,
    // Valid durable journal header with no marker and no final segment.
    kJournalOnly,
    // Valid journal header plus exactly one sequence-1/base-0 segment whose
    // bytes after the complete header are all zero preallocation.
    kSegmentHeaderOnly,
};

enum class RawRecoveryR11OrphanV1 : std::uint8_t {
    kNone = 0U,
    // A highest normal segment was durably published after the preceding
    // seal, but its mandatory header-only marker was not yet appended.
    kNormalHeaderOnly,
    // The same physical R11 window for a finalization continuation. It is
    // classified here, but execution additionally requires the coordinator's
    // matching CONSUMED/ACTIVE grant and executor token.
    kFinalizationContinuationHeaderOnly,
};

enum class RawRecoverySegmentTailV1 : std::uint8_t {
    kNone = 0U,
    // The suffix begins with an incomplete header, record, or trailer.
    kPartialRecord,
    // The suffix begins with a complete-looking but invalid Raw record.
    kInvalidRecord,
    // A highest-open segment may contain zero preallocation after its last
    // complete record. The range is not Raw data and is never promoted.
    kZeroPreallocation,
};

struct RawRecoveryCursorV1 final {
    std::uint32_t segment_sequence = 0U;
    std::uint64_t global_wal_pos = 0U;
    std::uint64_t ingress_sequence = 0U;
    std::uint64_t segment_offset = 0U;
    std::uint32_t marker_flags = 0U;
};

struct RawRecoverySegmentPlanV1 final {
    std::uint32_t segment_sequence = 0U;
    std::uint64_t segment_base_wal_pos = 0U;
    // Exact bytes of the last journal marker accepted for this segment,
    // including its CRC. For a sealed segment this is the authoritative
    // SEGMENT_SEALED marker used to rebuild dependent artifacts. Initial
    // anchors and an R11 header-only orphan have no accepted marker yet.
    bool has_accepted_marker = false;
    RawV1DurableMarkerWire accepted_marker_wire{};
    // End proven by the last accepted journal marker for this segment.
    std::uint64_t durable_end_offset = 0U;
    // End of the complete validating-reader prefix. For a sealed segment it
    // equals durable_end_offset. For the highest open segment it can include
    // complete records that survived after the durable marker.
    std::uint64_t validated_logical_end_offset = 0U;
    // Last ingress sequence at validated_logical_end_offset. Header-only
    // segment 1 uses zero; an empty rotated segment carries the preceding
    // segment's durable sequence.
    std::uint64_t validated_last_ingress_sequence = 0U;
    // Complete append-only records are exactly [begin, end). An empty range
    // has equal endpoints.
    std::uint64_t append_only_begin_offset = 0U;
    std::uint64_t append_only_end_offset = 0U;
    // Bytes requiring truncation are exactly [tail_begin, tail_end).
    std::uint64_t tail_begin_offset = 0U;
    std::uint64_t tail_end_offset = 0U;
    RawRecoverySegmentTailV1 tail =
        RawRecoverySegmentTailV1::kNone;
    bool sealed = false;
};

struct RawRecoveryPlanV1 final {
    RawRecoveryFatalV1 fatal = RawRecoveryFatalV1::kNone;
    RawV1Error codec_error = RawV1Error::kNone;
    RawReaderError reader_error = RawReaderError::kNone;
    // Byte offset in the journal for journal failures, or segment-file offset
    // for segment/Raw failures.
    std::uint64_t evidence_offset = 0U;

    DurableJournalHeaderV1 journal_header{};
    // Exclusive journal cursor retaining only the accepted marker prefix.
    std::uint64_t accepted_journal_size = 0U;
    bool has_accepted_cursor = false;
    RawRecoveryCursorV1 accepted_cursor{};
    RawRecoveryJournalTailV1 journal_tail =
        RawRecoveryJournalTailV1::kNone;
    RawRecoveryInitialAnchorV1 initial_anchor =
        RawRecoveryInitialAnchorV1::kNone;
    RawRecoveryR11OrphanV1 r11_orphan =
        RawRecoveryR11OrphanV1::kNone;
    std::vector<RawRecoverySegmentPlanV1> segments;

    [[nodiscard]] bool ok() const noexcept {
        return fatal == RawRecoveryFatalV1::kNone;
    }
};

// Validates the journal in two conceptual passes: marker framing/chain first,
// then every candidate marker against a full Raw scan from segment data-begin.
// It performs no truncate, sync, open, close, rename, or other mutation.
[[nodiscard]] RawRecoveryPlanV1 AnalyzeRawRecoveryV1(
    const RawRecoveryInputV1& input) noexcept;

}  // namespace l2flow::ingress
