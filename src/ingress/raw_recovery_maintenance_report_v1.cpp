#include "l2flow/ingress/raw_recovery_maintenance_report_v1.h"

#include "l2flow/common/identity128.h"
#include "l2flow/common/sha256.h"
#include "l2flow/ingress/raw_schema.h"

#include <algorithm>
#include <array>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <new>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

namespace l2flow::ingress {
namespace {

template <std::size_t Size>
[[nodiscard]] bool IsZero(
    const std::array<std::byte, Size>& value) noexcept {
    return std::all_of(
        value.begin(),
        value.end(),
        [](std::byte byte) noexcept {
            return byte == std::byte{0};
        });
}

[[nodiscard]] bool SameNamespace(
    const RawManifestNamespaceV1& left,
    const RawManifestNamespaceV1& right) noexcept {
    return left.capture_date == right.capture_date &&
           left.source_stream_id == right.source_stream_id &&
           left.stream_day_id == right.stream_day_id;
}

[[nodiscard]] bool SameCursor(
    const RecoveryMaintenanceCursorV1& left,
    const RecoveryMaintenanceCursorV1& right) noexcept {
    return left == right;
}

[[nodiscard]] bool SameCursorIgnoringFlags(
    const RecoveryMaintenanceCursorV1& left,
    const RecoveryMaintenanceCursorV1& right) noexcept {
    return left.segment_sequence == right.segment_sequence &&
           left.global_wal_pos == right.global_wal_pos &&
           left.ingress_sequence == right.ingress_sequence &&
           left.segment_offset == right.segment_offset;
}

[[nodiscard]] RecoveryMaintenanceCursorV1 CursorFrom(
    const RawRecoveryCursorV1& cursor) noexcept {
    return {
        cursor.segment_sequence,
        cursor.global_wal_pos,
        cursor.ingress_sequence,
        cursor.marker_flags,
        cursor.segment_offset};
}

[[nodiscard]] RecoveryMaintenanceCursorV1 CursorFrom(
    std::uint32_t segment_sequence,
    const RawWalCursor& cursor,
    std::uint32_t marker_flags) noexcept {
    return {
        segment_sequence,
        cursor.global_wal_pos,
        cursor.ingress_sequence,
        marker_flags,
        cursor.segment_offset};
}

[[nodiscard]] RecoveryMaintenanceCursorV1 CursorFrom(
    const DurableMarkerV1& marker) noexcept {
    return {
        marker.segment_sequence,
        marker.durable_global_wal_pos,
        marker.durable_ingress_sequence,
        marker.marker_flags,
        marker.durable_segment_offset};
}

[[nodiscard]] bool CheckedAdd(
    std::uint64_t left,
    std::uint64_t right,
    std::uint64_t* output) noexcept {
    if (output == nullptr ||
        right >
            std::numeric_limits<std::uint64_t>::max() -
                left) {
        return false;
    }
    *output = left + right;
    return true;
}

template <typename Integer>
void AppendInteger(
    std::string* output,
    Integer value) {
    static_assert(std::is_integral_v<Integer>);
    std::array<char, 32U> buffer{};
    const auto converted = std::to_chars(
        buffer.data(),
        buffer.data() + buffer.size(),
        value);
    if (converted.ec != std::errc{}) {
        throw std::bad_alloc();
    }
    output->append(buffer.data(), converted.ptr);
}

void AppendQuoted(
    std::string* output,
    std::string_view value) {
    output->push_back('"');
    output->append(value);
    output->push_back('"');
}

void AppendQuotedU64(
    std::string* output,
    std::uint64_t value) {
    std::array<char, 20U> buffer{};
    const auto converted = std::to_chars(
        buffer.data(),
        buffer.data() + buffer.size(),
        value);
    if (converted.ec != std::errc{}) {
        throw std::bad_alloc();
    }
    output->push_back('"');
    output->append(buffer.data(), converted.ptr);
    output->push_back('"');
}

void AppendNullableQuotedU64(
    std::string* output,
    const std::optional<std::uint64_t>& value) {
    if (!value.has_value()) {
        output->append("null");
        return;
    }
    AppendQuotedU64(output, *value);
}

template <std::size_t Size>
void AppendHex(
    std::string* output,
    const std::array<std::byte, Size>& value) {
    static constexpr std::string_view kDigits =
        "0123456789abcdef";
    output->push_back('"');
    for (const std::byte byte : value) {
        const unsigned octet =
            std::to_integer<unsigned>(byte);
        output->push_back(
            kDigits[(octet >> 4U) & 0x0fU]);
        output->push_back(kDigits[octet & 0x0fU]);
    }
    output->push_back('"');
}

template <std::size_t Size>
void AppendNullableHex(
    std::string* output,
    const std::array<std::byte, Size>& value) {
    if (IsZero(value)) {
        output->append("null");
    } else {
        AppendHex(output, value);
    }
}

void AppendCursor(
    std::string* output,
    const RecoveryMaintenanceCursorV1& cursor) {
    output->append("{\"global_wal_pos\":");
    AppendQuotedU64(output, cursor.global_wal_pos);
    output->append(",\"ingress_sequence\":");
    AppendQuotedU64(output, cursor.ingress_sequence);
    output->append(",\"marker_flags\":");
    AppendInteger(output, cursor.marker_flags);
    output->append(",\"segment_offset\":");
    AppendQuotedU64(output, cursor.segment_offset);
    output->append(",\"segment_sequence\":");
    AppendInteger(output, cursor.segment_sequence);
    output->push_back('}');
}

[[nodiscard]] std::string_view JournalTailName(
    RawRecoveryJournalTailV1 tail) noexcept {
    switch (tail) {
    case RawRecoveryJournalTailV1::kNone:
        return "NONE";
    case RawRecoveryJournalTailV1::kPartialMarker:
        return "PARTIAL_MARKER";
    case RawRecoveryJournalTailV1::
        kTerminalCrcInvalidMarker:
        return "TERMINAL_CRC_INVALID_MARKER";
    }
    return {};
}

[[nodiscard]] std::string_view SegmentTailName(
    RawRecoverySegmentTailV1 tail) noexcept {
    switch (tail) {
    case RawRecoverySegmentTailV1::kNone:
        return "NONE";
    case RawRecoverySegmentTailV1::kPartialRecord:
        return "PARTIAL_RECORD";
    case RawRecoverySegmentTailV1::kInvalidRecord:
        return "INVALID_RECORD";
    case RawRecoverySegmentTailV1::kZeroPreallocation:
        return "ZERO_PREALLOCATION";
    }
    return {};
}

[[nodiscard]] std::string_view IntentName(
    RecoveryMaintenanceIntentV1 intent) noexcept {
    switch (intent) {
    case RecoveryMaintenanceIntentV1::kResumeConnect:
        return "RESUME_CONNECT";
    case RecoveryMaintenanceIntentV1::kRecoverSealOnly:
        return "RECOVER_SEAL_ONLY";
    }
    return {};
}

[[nodiscard]] std::string_view ResultName(
    RecoveryMaintenanceResultV1 result) noexcept {
    switch (result) {
    case RecoveryMaintenanceResultV1::kResumedOpen:
        return "RESUMED_OPEN";
    case RecoveryMaintenanceResultV1::kSealedRaw:
        return "SEALED_RAW";
    case RecoveryMaintenanceResultV1::kEmptyAnchorOnly:
        return "EMPTY_ANCHOR_ONLY";
    }
    return {};
}

[[nodiscard]] bool IsExactAnalyzedJournalSize(
    std::uint64_t accepted_size,
    std::uint64_t analyzed_size,
    RawRecoveryJournalTailV1 tail) noexcept {
    if (tail == RawRecoveryJournalTailV1::kNone) {
        return analyzed_size == accepted_size;
    }
    std::uint64_t one_marker_later = 0U;
    if (!CheckedAdd(
            accepted_size,
            kRawV1DurableMarkerBytes,
            &one_marker_later)) {
        return false;
    }
    if (tail ==
        RawRecoveryJournalTailV1::kPartialMarker) {
        return analyzed_size > accepted_size &&
               analyzed_size < one_marker_later;
    }
    return tail ==
               RawRecoveryJournalTailV1::
                   kTerminalCrcInvalidMarker &&
           analyzed_size == one_marker_later;
}

class CanonicalParser final {
public:
    explicit CanonicalParser(
        std::string_view input) noexcept
        : input_(input) {}

    [[nodiscard]] bool Consume(
        std::string_view token) noexcept {
        if (input_.substr(position_, token.size()) !=
            token) {
            return false;
        }
        position_ += token.size();
        return true;
    }

    [[nodiscard]] bool ParseU32(
        std::uint32_t* output) noexcept {
        if (output == nullptr ||
            position_ >= input_.size() ||
            input_[position_] < '0' ||
            input_[position_] > '9') {
            return false;
        }
        const std::size_t begin = position_;
        if (input_[position_] == '0' &&
            position_ + 1U < input_.size() &&
            input_[position_ + 1U] >= '0' &&
            input_[position_ + 1U] <= '9') {
            return false;
        }
        while (position_ < input_.size() &&
               input_[position_] >= '0' &&
               input_[position_] <= '9') {
            ++position_;
        }
        std::uint32_t candidate = 0U;
        const char* const first =
            input_.data() + begin;
        const char* const last =
            input_.data() + position_;
        const auto converted =
            std::from_chars(first, last, candidate);
        if (converted.ec != std::errc{} ||
            converted.ptr != last) {
            return false;
        }
        *output = candidate;
        return true;
    }

    [[nodiscard]] bool ParseQuotedU64(
        std::uint64_t* output) noexcept {
        if (output == nullptr || !Consume("\"")) {
            return false;
        }
        const std::size_t begin = position_;
        if (position_ >= input_.size() ||
            input_[position_] < '0' ||
            input_[position_] > '9' ||
            (input_[position_] == '0' &&
             position_ + 1U < input_.size() &&
             input_[position_ + 1U] != '"')) {
            return false;
        }
        while (position_ < input_.size() &&
               input_[position_] >= '0' &&
               input_[position_] <= '9') {
            ++position_;
        }
        std::uint64_t candidate = 0U;
        const char* const first =
            input_.data() + begin;
        const char* const last =
            input_.data() + position_;
        const auto converted =
            std::from_chars(first, last, candidate);
        if (converted.ec != std::errc{} ||
            converted.ptr != last ||
            !Consume("\"")) {
            return false;
        }
        *output = candidate;
        return true;
    }

    [[nodiscard]] bool ParseNullableQuotedU64(
        std::optional<std::uint64_t>* output) noexcept {
        if (output == nullptr) {
            return false;
        }
        if (Consume("null")) {
            output->reset();
            return true;
        }
        std::uint64_t candidate = 0U;
        if (!ParseQuotedU64(&candidate)) {
            return false;
        }
        *output = candidate;
        return true;
    }

    template <std::size_t Size>
    [[nodiscard]] bool ParseHex(
        std::array<std::byte, Size>* output) noexcept {
        if (output == nullptr || !Consume("\"") ||
            input_.size() - position_ <
                Size * 2U + 1U) {
            return false;
        }
        std::array<std::byte, Size> candidate{};
        for (std::size_t index = 0U;
             index < Size;
             ++index) {
            const int high = HexNibble(
                input_[position_ + index * 2U]);
            const int low = HexNibble(
                input_[position_ + index * 2U + 1U]);
            if (high < 0 || low < 0) {
                return false;
            }
            candidate[index] =
                static_cast<std::byte>(
                    static_cast<unsigned>(
                        high * 16 + low));
        }
        position_ += Size * 2U;
        if (!Consume("\"")) {
            return false;
        }
        *output = candidate;
        return true;
    }

    template <std::size_t Size>
    [[nodiscard]] bool ParseNullableHex(
        std::array<std::byte, Size>* output) noexcept {
        if (output == nullptr) {
            return false;
        }
        if (Consume("null")) {
            output->fill(std::byte{0});
            return true;
        }
        return ParseHex(output);
    }

    [[nodiscard]] bool done() const noexcept {
        return position_ == input_.size();
    }

private:
    [[nodiscard]] static int HexNibble(
        char value) noexcept {
        if (value >= '0' && value <= '9') {
            return value - '0';
        }
        if (value >= 'a' && value <= 'f') {
            return 10 + value - 'a';
        }
        return -1;
    }

    std::string_view input_;
    std::size_t position_ = 0U;
};

[[nodiscard]] bool ParseCursor(
    CanonicalParser* parser,
    RecoveryMaintenanceCursorV1* output) noexcept {
    RecoveryMaintenanceCursorV1 candidate{};
    if (parser == nullptr || output == nullptr ||
        !parser->Consume("{\"global_wal_pos\":") ||
        !parser->ParseQuotedU64(
            &candidate.global_wal_pos) ||
        !parser->Consume(",\"ingress_sequence\":") ||
        !parser->ParseQuotedU64(
            &candidate.ingress_sequence) ||
        !parser->Consume(",\"marker_flags\":") ||
        !parser->ParseU32(&candidate.marker_flags) ||
        !parser->Consume(",\"segment_offset\":") ||
        !parser->ParseQuotedU64(
            &candidate.segment_offset) ||
        !parser->Consume(",\"segment_sequence\":") ||
        !parser->ParseU32(
            &candidate.segment_sequence) ||
        !parser->Consume("}")) {
        return false;
    }
    *output = candidate;
    return true;
}

[[nodiscard]] bool ParseEndpointMarker(
    CanonicalParser* parser,
    RecoveryMaintenanceEndpointMarkerV1*
        output) noexcept {
    RecoveryMaintenanceEndpointMarkerV1 candidate{};
    if (parser == nullptr || output == nullptr ||
        !parser->Consume("{\"global_wal_pos\":") ||
        !parser->ParseQuotedU64(
            &candidate.cursor.global_wal_pos) ||
        !parser->Consume(",\"ingress_sequence\":") ||
        !parser->ParseQuotedU64(
            &candidate.cursor.ingress_sequence) ||
        !parser->Consume(",\"marker_bytes\":") ||
        !parser->ParseHex(&candidate.marker_bytes) ||
        !parser->Consume(",\"marker_flags\":") ||
        !parser->ParseU32(
            &candidate.cursor.marker_flags) ||
        !parser->Consume(",\"marker_sha256\":") ||
        !parser->ParseHex(&candidate.marker_sha256) ||
        !parser->Consume(",\"segment_offset\":") ||
        !parser->ParseQuotedU64(
            &candidate.cursor.segment_offset) ||
        !parser->Consume(",\"segment_sequence\":") ||
        !parser->ParseU32(
            &candidate.cursor.segment_sequence) ||
        !parser->Consume("}")) {
        return false;
    }
    *output = candidate;
    return true;
}

[[nodiscard]] bool IsZeroCursor(
    const RecoveryMaintenanceCursorV1& cursor) noexcept {
    return cursor.segment_sequence == 0U &&
           cursor.global_wal_pos == 0U &&
           cursor.ingress_sequence == 0U &&
           cursor.marker_flags == 0U &&
           cursor.segment_offset == 0U;
}

[[nodiscard]] bool IsZeroNewSegment(
    const RecoveryMaintenanceNewSegmentV1& segment) noexcept {
    return segment.segment_base_wal_pos == 0U &&
           IsZero(segment.segment_header_sha256) &&
           segment.segment_sequence == 0U;
}

[[nodiscard]] bool IsZeroReuseSegment(
    const RecoveryMaintenanceReuseSegmentV1& segment) noexcept {
    return segment.reported_logical_end_offset == 0U &&
           segment.segment_base_wal_pos == 0U &&
           IsZero(segment.segment_header_sha256) &&
           IsZero(segment.segment_prefix_sha256) &&
           segment.segment_sequence == 0U;
}

[[nodiscard]] bool IsZeroPreviousTerminal(
    const RecoveryMaintenancePreviousTerminalV1&
        terminal) noexcept {
    return terminal.kind ==
               RecoveryMaintenancePreviousTerminalKindV1::
                   kNone &&
           IsZero(terminal.marker_bytes) &&
           IsZero(terminal.marker_sha256) &&
           terminal.segment_sequence == 0U &&
           IsZero(terminal.segment_sha256);
}

[[nodiscard]] bool IsValidInitialCursor(
    const RecoveryMaintenanceCursorV1& cursor) noexcept {
    if (cursor.segment_sequence == 0U) {
        return IsZeroCursor(cursor);
    }
    return cursor.segment_offset >=
               kRawV1SegmentHeaderBytes &&
           (cursor.marker_flags &
            ~kRawV1MarkerFlagsMask) == 0U;
}

[[nodiscard]] bool IsZeroClosedFrontier(
    const RecoveryMaintenanceClosedFrontierV1&
        frontier) noexcept {
    return frontier.closed_entry_count == 0U &&
           IsZero(frontier.closed_prefix_sha256) &&
           IsZero(frontier.frontier_seal_marker_sha256) &&
           frontier.frontier_segment_sequence == 0U &&
           IsZero(frontier.frontier_segment_sha256);
}

[[nodiscard]] bool IsZeroRange(
    const RecoveryMaintenanceRangeV1& range) noexcept {
    return range.analyzed_journal_size == 0U &&
           range.final_durable_journal_size == 0U &&
           range.initial_accepted_journal_size == 0U &&
           IsZeroCursor(range.initial_durable_cursor) &&
           range.post_repair_journal_size == 0U &&
           range.range_segment_sequence == 0U &&
           range.raw_append_begin_offset == 0U &&
           range.raw_append_end_offset == 0U &&
           range.raw_repair_begin_offset == 0U &&
           range.raw_repair_end_offset == 0U;
}

[[nodiscard]] bool IsZeroEndpointMarker(
    const RecoveryMaintenanceEndpointMarkerV1&
        marker) noexcept {
    return IsZeroCursor(marker.cursor) &&
           IsZero(marker.marker_bytes) &&
           IsZero(marker.marker_sha256);
}

[[nodiscard]] bool IsZeroOpenBoundary(
    const RecoveryMaintenanceOpenBoundaryV1&
        boundary) noexcept {
    return IsZeroEndpointMarker(boundary.endpoint_marker) &&
           IsZero(
               boundary.manifest_entry_commitment_sha256) &&
           boundary.manifest_generation == 0U &&
           boundary.open_variant ==
               RecoveryMaintenanceOpenVariantV1::
                   kReuseOpen &&
           IsZeroNewSegment(boundary.new_segment) &&
           IsZeroPreviousTerminal(
               boundary.previous_terminal) &&
           IsZero(
               boundary.reopens_empty_tombstone_sha256) &&
           IsZero(
               boundary
                   .reopens_sealed_raw_certificate_sha256) &&
           IsZeroReuseSegment(boundary.reuse_segment);
}

[[nodiscard]] bool IsZeroSealedBoundary(
    const RecoveryMaintenanceSealedBoundaryV1&
        boundary) noexcept {
    return IsZero(
               boundary.accepted_sealed_marker_bytes) &&
           IsZero(
               boundary.accepted_sealed_marker_sha256) &&
           boundary.last_segment_base_wal_pos == 0U &&
           boundary.last_segment_flags == 0U &&
           boundary.last_segment_logical_length == 0U &&
           boundary.last_segment_sequence == 0U &&
           IsZero(boundary.last_segment_sha256);
}

[[nodiscard]] RecoveryMaintenanceReportV1Error
ValidateRange(
    const RecoveryMaintenanceRangeV1& range) noexcept {
    if (range.analyzed_journal_size <
            kRawV1JournalHeaderBytes ||
        range.initial_accepted_journal_size <
            kRawV1JournalHeaderBytes ||
        range.post_repair_journal_size <
            kRawV1JournalHeaderBytes ||
        range.final_durable_journal_size <
            range.post_repair_journal_size ||
        (range.initial_accepted_journal_size -
         kRawV1JournalHeaderBytes) %
                kRawV1DurableMarkerBytes !=
            0U ||
        (range.post_repair_journal_size -
         kRawV1JournalHeaderBytes) %
                kRawV1DurableMarkerBytes !=
            0U ||
        (range.final_durable_journal_size -
         kRawV1JournalHeaderBytes) %
                kRawV1DurableMarkerBytes !=
            0U ||
        range.initial_accepted_journal_size >
            range.analyzed_journal_size ||
        range.range_segment_sequence == 0U ||
        range.raw_append_begin_offset >
            range.raw_append_end_offset ||
        range.raw_repair_begin_offset >
            range.raw_repair_end_offset ||
        !IsValidInitialCursor(
            range.initial_durable_cursor)) {
        return RecoveryMaintenanceReportV1Error::
            kRangeInvalid;
    }
    return RecoveryMaintenanceReportV1Error::kNone;
}

[[nodiscard]] RecoveryMaintenanceReportV1Error
ValidateClosedFrontier(
    const RecoveryMaintenanceClosedFrontierV1&
        frontier) noexcept {
    if (frontier.closed_entry_count == 0U) {
        if (IsZero(frontier.closed_prefix_sha256) ||
            !IsZero(
                frontier.frontier_seal_marker_sha256) ||
            frontier.frontier_segment_sequence != 0U ||
            !IsZero(frontier.frontier_segment_sha256)) {
            return RecoveryMaintenanceReportV1Error::
                kFrontierMismatch;
        }
    } else if (
        IsZero(frontier.closed_prefix_sha256) ||
        IsZero(frontier.frontier_seal_marker_sha256) ||
        frontier.frontier_segment_sequence == 0U ||
        IsZero(frontier.frontier_segment_sha256)) {
        return RecoveryMaintenanceReportV1Error::
            kFrontierMismatch;
    }
    return RecoveryMaintenanceReportV1Error::kNone;
}

[[nodiscard]] RecoveryMaintenanceReportV1Error
ValidateResumedOpenPayload(
    const RecoveryMaintenanceReportV1& report) noexcept {
    if (report.intent !=
            RecoveryMaintenanceIntentV1::kResumeConnect ||
        !IsZero(
            report
                .current_empty_anchor_tombstone_sha256) ||
        !IsZero(
            report
                .current_sealed_raw_certificate_sha256) ||
        report.marker_count.has_value() ||
        report.record_count.has_value() ||
        report.segment_count.has_value() ||
        !IsZeroSealedBoundary(report.sealed_boundary)) {
        return RecoveryMaintenanceReportV1Error::
            kVariantMismatch;
    }
    RecoveryMaintenanceReportV1Error error =
        ValidateClosedFrontier(report.closed_frontier);
    if (error != RecoveryMaintenanceReportV1Error::kNone) {
        return error;
    }
    error = ValidateRange(report.recovery_range);
    if (error != RecoveryMaintenanceReportV1Error::kNone) {
        return error;
    }

    const RecoveryMaintenanceOpenBoundaryV1& boundary =
        report.open_boundary;
    if (boundary.manifest_generation == 0U ||
        IsZero(
            boundary
                .manifest_entry_commitment_sha256) ||
        report.final_durable_cursor.segment_sequence ==
            0U ||
        report.final_durable_cursor.marker_flags != 0U ||
        report.final_durable_cursor.segment_offset <
            kRawV1SegmentHeaderBytes ||
        !SameCursor(
            report.final_durable_cursor,
            boundary.endpoint_marker.cursor)) {
        return RecoveryMaintenanceReportV1Error::
            kCursorMismatch;
    }
    DurableMarkerV1 endpoint{};
    if (DecodeDurableMarkerV1(
            boundary.endpoint_marker.marker_bytes,
            &endpoint) != RawV1Error::kNone ||
        endpoint.source_stream_id !=
            report.namespace_identity.source_stream_id ||
        !SameCursor(
            CursorFrom(endpoint),
            boundary.endpoint_marker.cursor)) {
        return RecoveryMaintenanceReportV1Error::
            kMarkerInvalid;
    }
    if (ComputeAcceptedMarkerSha256(
            boundary.endpoint_marker.marker_bytes) !=
            boundary.endpoint_marker.marker_sha256 ||
        IsZero(boundary.endpoint_marker.marker_sha256)) {
        return RecoveryMaintenanceReportV1Error::
            kCommitmentMismatch;
    }

    const bool has_empty_reopen =
        !IsZero(
            boundary
                .reopens_empty_tombstone_sha256);
    const bool has_sealed_reopen =
        !IsZero(
            boundary
                .reopens_sealed_raw_certificate_sha256);
    if (has_empty_reopen && has_sealed_reopen) {
        return RecoveryMaintenanceReportV1Error::
            kVariantMismatch;
    }

    if (boundary.open_variant ==
        RecoveryMaintenanceOpenVariantV1::kReuseOpen) {
        const RecoveryMaintenanceReuseSegmentV1& reuse =
            boundary.reuse_segment;
        std::uint64_t expected_global = 0U;
        if (reuse.segment_sequence == 0U ||
            reuse.reported_logical_end_offset <
                kRawV1SegmentHeaderBytes ||
            IsZero(reuse.segment_header_sha256) ||
            IsZero(reuse.segment_prefix_sha256) ||
            reuse.segment_sequence !=
                report.final_durable_cursor
                    .segment_sequence ||
            reuse.reported_logical_end_offset !=
                report.final_durable_cursor
                    .segment_offset ||
            !CheckedAdd(
                reuse.segment_base_wal_pos,
                reuse.reported_logical_end_offset,
                &expected_global) ||
            expected_global !=
                report.final_durable_cursor
                    .global_wal_pos ||
            !IsZeroNewSegment(boundary.new_segment) ||
            !IsZeroPreviousTerminal(
                boundary.previous_terminal) ||
            has_empty_reopen || has_sealed_reopen) {
            return RecoveryMaintenanceReportV1Error::
                kVariantMismatch;
        }
        return RecoveryMaintenanceReportV1Error::kNone;
    }

    if (boundary.open_variant !=
        RecoveryMaintenanceOpenVariantV1::
            kNewOpenAfterSealed ||
        !IsZeroReuseSegment(boundary.reuse_segment)) {
        return RecoveryMaintenanceReportV1Error::
            kVariantMismatch;
    }
    const RecoveryMaintenanceNewSegmentV1& opened =
        boundary.new_segment;
    std::uint64_t expected_global = 0U;
    if (opened.segment_sequence == 0U ||
        IsZero(opened.segment_header_sha256) ||
        opened.segment_sequence !=
            report.final_durable_cursor
                .segment_sequence ||
        report.final_durable_cursor.segment_offset !=
            kRawV1SegmentHeaderBytes ||
        !CheckedAdd(
            opened.segment_base_wal_pos,
            kRawV1SegmentHeaderBytes,
            &expected_global) ||
        expected_global !=
            report.final_durable_cursor.global_wal_pos) {
        return RecoveryMaintenanceReportV1Error::
            kVariantMismatch;
    }

    const RecoveryMaintenancePreviousTerminalV1&
        previous = boundary.previous_terminal;
    if (previous.kind ==
        RecoveryMaintenancePreviousTerminalKindV1::
            kCanonicalZero) {
        if (!IsZero(previous.marker_bytes) ||
            !IsZero(previous.marker_sha256) ||
            previous.segment_sequence != 0U ||
            !IsZero(previous.segment_sha256) ||
            report.closed_frontier
                    .closed_entry_count != 0U ||
            opened.segment_sequence != 1U ||
            opened.segment_base_wal_pos != 0U ||
            report.final_durable_cursor
                    .ingress_sequence != 0U ||
            has_sealed_reopen) {
            return RecoveryMaintenanceReportV1Error::
                kVariantMismatch;
        }
        return RecoveryMaintenanceReportV1Error::kNone;
    }
    if (previous.kind !=
        RecoveryMaintenancePreviousTerminalKindV1::
            kSealed) {
        return RecoveryMaintenanceReportV1Error::
            kVariantMismatch;
    }
    DurableMarkerV1 sealed{};
    if (previous.segment_sequence == 0U ||
        IsZero(previous.segment_sha256) ||
        DecodeDurableMarkerV1(
            previous.marker_bytes,
            &sealed) != RawV1Error::kNone ||
        sealed.marker_flags != kRawV1SegmentSealed ||
        sealed.source_stream_id !=
            report.namespace_identity.source_stream_id ||
        sealed.segment_sequence !=
            previous.segment_sequence ||
        ComputeAcceptedMarkerSha256(
            previous.marker_bytes) !=
            previous.marker_sha256 ||
        previous.marker_sha256 !=
            report.closed_frontier
                .frontier_seal_marker_sha256 ||
        previous.segment_sequence !=
            report.closed_frontier
                .frontier_segment_sequence ||
        previous.segment_sha256 !=
            report.closed_frontier
                .frontier_segment_sha256 ||
        has_empty_reopen ||
        previous.segment_sequence ==
            std::numeric_limits<std::uint32_t>::max() ||
        opened.segment_sequence !=
            previous.segment_sequence + 1U ||
        opened.segment_base_wal_pos !=
            sealed.durable_global_wal_pos ||
        report.final_durable_cursor
                .ingress_sequence !=
            sealed.durable_ingress_sequence) {
        return RecoveryMaintenanceReportV1Error::
            kFrontierMismatch;
    }
    return RecoveryMaintenanceReportV1Error::kNone;
}

[[nodiscard]] RecoveryMaintenanceReportV1Error
ValidateSealedRawPayload(
    const RecoveryMaintenanceReportV1& report) noexcept {
    if (report.intent !=
            RecoveryMaintenanceIntentV1::
                kRecoverSealOnly ||
        !IsZero(
            report
                .current_empty_anchor_tombstone_sha256) ||
        IsZero(
            report
                .current_sealed_raw_certificate_sha256) ||
        report.marker_count.has_value() ||
        report.record_count.has_value() ||
        report.segment_count.has_value() ||
        !IsZeroOpenBoundary(report.open_boundary) ||
        report.closed_frontier.closed_entry_count == 0U ||
        report.segment_tail !=
            RawRecoverySegmentTailV1::kNone) {
        return RecoveryMaintenanceReportV1Error::
            kVariantMismatch;
    }
    RecoveryMaintenanceReportV1Error error =
        ValidateClosedFrontier(report.closed_frontier);
    if (error != RecoveryMaintenanceReportV1Error::kNone) {
        return error;
    }
    error = ValidateRange(report.recovery_range);
    if (error != RecoveryMaintenanceReportV1Error::kNone) {
        return error;
    }

    const auto& boundary = report.sealed_boundary;
    const auto& cursor = report.final_durable_cursor;
    DurableMarkerV1 marker{};
    std::uint64_t expected_global = 0U;
    if (cursor.segment_sequence == 0U ||
        cursor.marker_flags != kRawV1SegmentSealed ||
        cursor.segment_offset < kRawV1SegmentHeaderBytes ||
        boundary.last_segment_sequence == 0U ||
        (boundary.last_segment_flags &
         ~kRawV1SegmentFlagsMask) != 0U ||
        boundary.last_segment_logical_length <
            kRawV1SegmentHeaderBytes ||
        IsZero(boundary.last_segment_sha256) ||
        IsZero(
            boundary.accepted_sealed_marker_sha256) ||
        DecodeDurableMarkerV1(
            boundary.accepted_sealed_marker_bytes,
            &marker) != RawV1Error::kNone ||
        marker.source_stream_id !=
            report.namespace_identity.source_stream_id ||
        marker.marker_flags != kRawV1SegmentSealed ||
        !SameCursor(CursorFrom(marker), cursor) ||
        ComputeAcceptedMarkerSha256(
            boundary.accepted_sealed_marker_bytes) !=
            boundary.accepted_sealed_marker_sha256 ||
        boundary.last_segment_sequence !=
            cursor.segment_sequence ||
        boundary.last_segment_logical_length !=
            cursor.segment_offset ||
        !CheckedAdd(
            boundary.last_segment_base_wal_pos,
            boundary.last_segment_logical_length,
            &expected_global) ||
        expected_global != cursor.global_wal_pos ||
        report.closed_frontier
                .frontier_segment_sequence !=
            boundary.last_segment_sequence ||
        report.closed_frontier
                .frontier_segment_sha256 !=
            boundary.last_segment_sha256 ||
        report.closed_frontier
                .frontier_seal_marker_sha256 !=
            boundary.accepted_sealed_marker_sha256 ||
        report.recovery_range.range_segment_sequence !=
            boundary.last_segment_sequence ||
        !SameCursor(
            report.recovery_range
                .initial_durable_cursor,
            cursor) ||
        report.recovery_range
                .initial_accepted_journal_size !=
            report.recovery_range
                .post_repair_journal_size ||
        report.recovery_range
                .post_repair_journal_size !=
            report.recovery_range
                .final_durable_journal_size ||
        !IsExactAnalyzedJournalSize(
            report.recovery_range
                .initial_accepted_journal_size,
            report.recovery_range
                .analyzed_journal_size,
            report.journal_tail) ||
        report.recovery_range.raw_append_begin_offset !=
            cursor.segment_offset ||
        report.recovery_range.raw_append_end_offset !=
            cursor.segment_offset ||
        report.recovery_range.raw_repair_begin_offset !=
            cursor.segment_offset ||
        report.recovery_range.raw_repair_end_offset !=
            cursor.segment_offset) {
        return RecoveryMaintenanceReportV1Error::
            kFrontierMismatch;
    }
    return RecoveryMaintenanceReportV1Error::kNone;
}

[[nodiscard]] RecoveryMaintenanceReportV1Error
ValidateEmptyAnchorOnlyPayload(
    const RecoveryMaintenanceReportV1& report) noexcept {
    if (report.intent !=
            RecoveryMaintenanceIntentV1::
                kRecoverSealOnly ||
        IsZero(
            report
                .current_empty_anchor_tombstone_sha256) ||
        !IsZero(
            report
                .current_sealed_raw_certificate_sha256) ||
        !report.marker_count.has_value() ||
        *report.marker_count != 0U ||
        !report.record_count.has_value() ||
        *report.record_count != 0U ||
        !report.segment_count.has_value() ||
        *report.segment_count != 0U ||
        !IsZeroClosedFrontier(report.closed_frontier) ||
        !IsZeroCursor(report.final_durable_cursor) ||
        !IsZeroOpenBoundary(report.open_boundary) ||
        !IsZeroRange(report.recovery_range) ||
        !IsZeroSealedBoundary(report.sealed_boundary) ||
        report.segment_tail !=
            RawRecoverySegmentTailV1::kNone) {
        return RecoveryMaintenanceReportV1Error::
            kVariantMismatch;
    }
    return RecoveryMaintenanceReportV1Error::kNone;
}

[[nodiscard]] RecoveryMaintenanceReportV1Error
ValidateIntrinsic(
    const RecoveryMaintenanceReportV1& report) noexcept {
    if (report.schema_version !=
        kRecoveryMaintenanceReportV1SchemaVersion) {
        return RecoveryMaintenanceReportV1Error::
            kInvalidArgument;
    }
    if (report.namespace_identity.capture_date == 0U ||
        report.namespace_identity.source_stream_id == 0U ||
        l2flow::common::IsZeroIdentity(
            report.namespace_identity.stream_day_id)) {
        return RecoveryMaintenanceReportV1Error::
            kInvalidNamespace;
    }
    if (l2flow::common::IsZeroIdentity(
            report.recovery_attempt_id)) {
        return RecoveryMaintenanceReportV1Error::
            kInvalidRecoveryAttempt;
    }
    if (IsZero(report.journal_header_sha256)) {
        return RecoveryMaintenanceReportV1Error::
            kInvalidJournalHeader;
    }
    if (IntentName(report.intent).empty() ||
        ResultName(report.result).empty() ||
        JournalTailName(report.journal_tail).empty() ||
        SegmentTailName(report.segment_tail).empty()) {
        return RecoveryMaintenanceReportV1Error::
            kInvalidArgument;
    }
    switch (report.result) {
    case RecoveryMaintenanceResultV1::kResumedOpen:
        return ValidateResumedOpenPayload(report);
    case RecoveryMaintenanceResultV1::kSealedRaw:
        return ValidateSealedRawPayload(report);
    case RecoveryMaintenanceResultV1::kEmptyAnchorOnly:
        return ValidateEmptyAnchorOnlyPayload(report);
    }
    return RecoveryMaintenanceReportV1Error::
        kVariantMismatch;
}

void AppendEndpointMarker(
    std::string* output,
    const RecoveryMaintenanceEndpointMarkerV1& marker) {
    output->append("{\"global_wal_pos\":");
    AppendQuotedU64(output, marker.cursor.global_wal_pos);
    output->append(",\"ingress_sequence\":");
    AppendQuotedU64(
        output, marker.cursor.ingress_sequence);
    output->append(",\"marker_bytes\":");
    AppendHex(output, marker.marker_bytes);
    output->append(",\"marker_flags\":");
    AppendInteger(output, marker.cursor.marker_flags);
    output->append(",\"marker_sha256\":");
    AppendHex(output, marker.marker_sha256);
    output->append(",\"segment_offset\":");
    AppendQuotedU64(
        output, marker.cursor.segment_offset);
    output->append(",\"segment_sequence\":");
    AppendInteger(
        output, marker.cursor.segment_sequence);
    output->push_back('}');
}

void AppendClosedFrontier(
    std::string* output,
    const RecoveryMaintenanceClosedFrontierV1&
        frontier) {
    output->append("{\"closed_entry_count\":");
    AppendQuotedU64(output, frontier.closed_entry_count);
    output->append(",\"closed_prefix_sha256\":");
    AppendHex(output, frontier.closed_prefix_sha256);
    output->append(
        ",\"frontier_seal_marker_sha256\":");
    AppendNullableHex(
        output, frontier.frontier_seal_marker_sha256);
    output->append(",\"frontier_segment_sequence\":");
    if (frontier.frontier_segment_sequence == 0U) {
        output->append("null");
    } else {
        AppendInteger(
            output, frontier.frontier_segment_sequence);
    }
    output->append(",\"frontier_segment_sha256\":");
    AppendNullableHex(
        output, frontier.frontier_segment_sha256);
    output->push_back('}');
}

void AppendOpenBoundary(
    std::string* output,
    const RecoveryMaintenanceOpenBoundaryV1& boundary) {
    output->append("{\"endpoint_marker\":");
    AppendEndpointMarker(output, boundary.endpoint_marker);
    output->append(
        ",\"manifest_entry_commitment_sha256\":");
    AppendHex(
        output,
        boundary.manifest_entry_commitment_sha256);
    output->append(",\"manifest_generation\":");
    AppendQuotedU64(output, boundary.manifest_generation);
    output->append(",\"new_segment\":");
    if (boundary.open_variant ==
        RecoveryMaintenanceOpenVariantV1::kReuseOpen) {
        output->append("null");
    } else {
        const auto& segment = boundary.new_segment;
        output->append("{\"segment_base_wal_pos\":");
        AppendQuotedU64(
            output, segment.segment_base_wal_pos);
        output->append(",\"segment_header_sha256\":");
        AppendHex(output, segment.segment_header_sha256);
        output->append(",\"segment_sequence\":");
        AppendInteger(output, segment.segment_sequence);
        output->push_back('}');
    }
    output->append(",\"open_variant\":");
    AppendQuoted(
        output,
        boundary.open_variant ==
                RecoveryMaintenanceOpenVariantV1::
                    kReuseOpen
            ? "REUSE_OPEN"
            : "NEW_OPEN_AFTER_SEALED");
    output->append(",\"previous_terminal\":");
    if (boundary.open_variant ==
        RecoveryMaintenanceOpenVariantV1::kReuseOpen) {
        output->append("null");
    } else {
        const auto& terminal = boundary.previous_terminal;
        output->append("{\"kind\":");
        AppendQuoted(
            output,
            terminal.kind ==
                    RecoveryMaintenancePreviousTerminalKindV1::
                        kCanonicalZero
                ? "CANONICAL_ZERO"
                : "SEALED");
        output->append(",\"marker_bytes\":");
        AppendNullableHex(output, terminal.marker_bytes);
        output->append(",\"marker_sha256\":");
        AppendNullableHex(output, terminal.marker_sha256);
        output->append(",\"segment_sequence\":");
        if (terminal.segment_sequence == 0U) {
            output->append("null");
        } else {
            AppendInteger(
                output, terminal.segment_sequence);
        }
        output->append(",\"segment_sha256\":");
        AppendNullableHex(output, terminal.segment_sha256);
        output->push_back('}');
    }
    output->append(
        ",\"reopens_empty_tombstone_sha256\":");
    AppendNullableHex(
        output,
        boundary.reopens_empty_tombstone_sha256);
    output->append(
        ",\"reopens_sealed_raw_certificate_sha256\":");
    AppendNullableHex(
        output,
        boundary
            .reopens_sealed_raw_certificate_sha256);
    output->append(",\"reuse_segment\":");
    if (boundary.open_variant ==
        RecoveryMaintenanceOpenVariantV1::
            kNewOpenAfterSealed) {
        output->append("null");
    } else {
        const auto& segment = boundary.reuse_segment;
        output->append(
            "{\"reported_logical_end_offset\":");
        AppendQuotedU64(
            output,
            segment.reported_logical_end_offset);
        output->append(",\"segment_base_wal_pos\":");
        AppendQuotedU64(
            output, segment.segment_base_wal_pos);
        output->append(",\"segment_header_sha256\":");
        AppendHex(
            output, segment.segment_header_sha256);
        output->append(",\"segment_prefix_sha256\":");
        AppendHex(
            output, segment.segment_prefix_sha256);
        output->append(",\"segment_sequence\":");
        AppendInteger(output, segment.segment_sequence);
        output->push_back('}');
    }
    output->push_back('}');
}

void AppendRecoveryRange(
    std::string* output,
    const RecoveryMaintenanceRangeV1& range) {
    output->append("{\"analyzed_journal_size\":");
    AppendQuotedU64(output, range.analyzed_journal_size);
    output->append(",\"final_durable_journal_size\":");
    AppendQuotedU64(
        output, range.final_durable_journal_size);
    output->append(
        ",\"initial_accepted_journal_size\":");
    AppendQuotedU64(
        output, range.initial_accepted_journal_size);
    output->append(",\"initial_durable_cursor\":");
    AppendCursor(output, range.initial_durable_cursor);
    output->append(",\"post_repair_journal_size\":");
    AppendQuotedU64(
        output, range.post_repair_journal_size);
    output->append(",\"range_segment_sequence\":");
    AppendInteger(output, range.range_segment_sequence);
    output->append(",\"raw_append_begin_offset\":");
    AppendQuotedU64(
        output, range.raw_append_begin_offset);
    output->append(",\"raw_append_end_offset\":");
    AppendQuotedU64(output, range.raw_append_end_offset);
    output->append(",\"raw_repair_begin_offset\":");
    AppendQuotedU64(
        output, range.raw_repair_begin_offset);
    output->append(",\"raw_repair_end_offset\":");
    AppendQuotedU64(output, range.raw_repair_end_offset);
    output->push_back('}');
}

void AppendSealedBoundary(
    std::string* output,
    const RecoveryMaintenanceSealedBoundaryV1&
        boundary) {
    output->append(
        "{\"accepted_sealed_marker_bytes\":");
    AppendHex(
        output, boundary.accepted_sealed_marker_bytes);
    output->append(
        ",\"accepted_sealed_marker_sha256\":");
    AppendHex(
        output, boundary.accepted_sealed_marker_sha256);
    output->append(",\"last_segment_base_wal_pos\":");
    AppendQuotedU64(
        output, boundary.last_segment_base_wal_pos);
    output->append(",\"last_segment_flags\":");
    AppendInteger(output, boundary.last_segment_flags);
    output->append(
        ",\"last_segment_logical_length\":");
    AppendQuotedU64(
        output, boundary.last_segment_logical_length);
    output->append(",\"last_segment_sequence\":");
    AppendInteger(output, boundary.last_segment_sequence);
    output->append(",\"last_segment_sha256\":");
    AppendHex(output, boundary.last_segment_sha256);
    output->push_back('}');
}

[[nodiscard]] bool ParseClosedFrontier(
    CanonicalParser* parser,
    RecoveryMaintenanceClosedFrontierV1*
        output) noexcept {
    RecoveryMaintenanceClosedFrontierV1 candidate{};
    if (parser == nullptr || output == nullptr ||
        !parser->Consume(
            "{\"closed_entry_count\":") ||
        !parser->ParseQuotedU64(
            &candidate.closed_entry_count) ||
        !parser->Consume(
            ",\"closed_prefix_sha256\":") ||
        !parser->ParseHex(
            &candidate.closed_prefix_sha256) ||
        !parser->Consume(
            ",\"frontier_seal_marker_sha256\":") ||
        !parser->ParseNullableHex(
            &candidate
                 .frontier_seal_marker_sha256) ||
        !parser->Consume(
            ",\"frontier_segment_sequence\":")) {
        return false;
    }
    if (parser->Consume("null")) {
        candidate.frontier_segment_sequence = 0U;
    } else if (!parser->ParseU32(
                   &candidate
                        .frontier_segment_sequence)) {
        return false;
    }
    if (!parser->Consume(
            ",\"frontier_segment_sha256\":") ||
        !parser->ParseNullableHex(
            &candidate.frontier_segment_sha256) ||
        !parser->Consume("}")) {
        return false;
    }
    *output = candidate;
    return true;
}

[[nodiscard]] bool ParseRecoveryRange(
    CanonicalParser* parser,
    RecoveryMaintenanceRangeV1* output) noexcept {
    RecoveryMaintenanceRangeV1 candidate{};
    if (parser == nullptr || output == nullptr ||
        !parser->Consume(
            "{\"analyzed_journal_size\":") ||
        !parser->ParseQuotedU64(
            &candidate.analyzed_journal_size) ||
        !parser->Consume(
            ",\"final_durable_journal_size\":") ||
        !parser->ParseQuotedU64(
            &candidate.final_durable_journal_size) ||
        !parser->Consume(
            ",\"initial_accepted_journal_size\":") ||
        !parser->ParseQuotedU64(
            &candidate.initial_accepted_journal_size) ||
        !parser->Consume(
            ",\"initial_durable_cursor\":") ||
        !ParseCursor(
            parser,
            &candidate.initial_durable_cursor) ||
        !parser->Consume(
            ",\"post_repair_journal_size\":") ||
        !parser->ParseQuotedU64(
            &candidate.post_repair_journal_size) ||
        !parser->Consume(
            ",\"range_segment_sequence\":") ||
        !parser->ParseU32(
            &candidate.range_segment_sequence) ||
        !parser->Consume(
            ",\"raw_append_begin_offset\":") ||
        !parser->ParseQuotedU64(
            &candidate.raw_append_begin_offset) ||
        !parser->Consume(
            ",\"raw_append_end_offset\":") ||
        !parser->ParseQuotedU64(
            &candidate.raw_append_end_offset) ||
        !parser->Consume(
            ",\"raw_repair_begin_offset\":") ||
        !parser->ParseQuotedU64(
            &candidate.raw_repair_begin_offset) ||
        !parser->Consume(
            ",\"raw_repair_end_offset\":") ||
        !parser->ParseQuotedU64(
            &candidate.raw_repair_end_offset) ||
        !parser->Consume("}")) {
        return false;
    }
    *output = candidate;
    return true;
}

[[nodiscard]] bool ParseSealedBoundary(
    CanonicalParser* parser,
    RecoveryMaintenanceSealedBoundaryV1*
        output) noexcept {
    RecoveryMaintenanceSealedBoundaryV1 candidate{};
    if (parser == nullptr || output == nullptr ||
        !parser->Consume(
            "{\"accepted_sealed_marker_bytes\":") ||
        !parser->ParseHex(
            &candidate.accepted_sealed_marker_bytes) ||
        !parser->Consume(
            ",\"accepted_sealed_marker_sha256\":") ||
        !parser->ParseHex(
            &candidate.accepted_sealed_marker_sha256) ||
        !parser->Consume(
            ",\"last_segment_base_wal_pos\":") ||
        !parser->ParseQuotedU64(
            &candidate.last_segment_base_wal_pos) ||
        !parser->Consume(",\"last_segment_flags\":") ||
        !parser->ParseU32(
            &candidate.last_segment_flags) ||
        !parser->Consume(
            ",\"last_segment_logical_length\":") ||
        !parser->ParseQuotedU64(
            &candidate.last_segment_logical_length) ||
        !parser->Consume(
            ",\"last_segment_sequence\":") ||
        !parser->ParseU32(
            &candidate.last_segment_sequence) ||
        !parser->Consume(
            ",\"last_segment_sha256\":") ||
        !parser->ParseHex(
            &candidate.last_segment_sha256) ||
        !parser->Consume("}")) {
        return false;
    }
    *output = candidate;
    return true;
}

[[nodiscard]] bool ParseOpenBoundary(
    CanonicalParser* parser,
    RecoveryMaintenanceOpenBoundaryV1*
        output) noexcept {
    RecoveryMaintenanceOpenBoundaryV1 candidate{};
    if (parser == nullptr || output == nullptr ||
        !parser->Consume("{\"endpoint_marker\":") ||
        !ParseEndpointMarker(
            parser, &candidate.endpoint_marker) ||
        !parser->Consume(
            ",\"manifest_entry_commitment_sha256\":") ||
        !parser->ParseHex(
            &candidate
                 .manifest_entry_commitment_sha256) ||
        !parser->Consume(
            ",\"manifest_generation\":") ||
        !parser->ParseQuotedU64(
            &candidate.manifest_generation) ||
        !parser->Consume(",\"new_segment\":")) {
        return false;
    }
    if (!parser->Consume("null")) {
        if (!parser->Consume(
                "{\"segment_base_wal_pos\":") ||
            !parser->ParseQuotedU64(
                &candidate.new_segment
                     .segment_base_wal_pos) ||
            !parser->Consume(
                ",\"segment_header_sha256\":") ||
            !parser->ParseHex(
                &candidate.new_segment
                     .segment_header_sha256) ||
            !parser->Consume(
                ",\"segment_sequence\":") ||
            !parser->ParseU32(
                &candidate.new_segment
                     .segment_sequence) ||
            !parser->Consume("}")) {
            return false;
        }
    }
    if (!parser->Consume(",\"open_variant\":")) {
        return false;
    }
    if (parser->Consume("\"REUSE_OPEN\"")) {
        candidate.open_variant =
            RecoveryMaintenanceOpenVariantV1::
                kReuseOpen;
    } else if (
        parser->Consume(
            "\"NEW_OPEN_AFTER_SEALED\"")) {
        candidate.open_variant =
            RecoveryMaintenanceOpenVariantV1::
                kNewOpenAfterSealed;
    } else {
        return false;
    }
    if (!parser->Consume(",\"previous_terminal\":")) {
        return false;
    }
    if (!parser->Consume("null")) {
        if (!parser->Consume("{\"kind\":")) {
            return false;
        }
        if (parser->Consume("\"CANONICAL_ZERO\"")) {
            candidate.previous_terminal.kind =
                RecoveryMaintenancePreviousTerminalKindV1::
                    kCanonicalZero;
        } else if (parser->Consume("\"SEALED\"")) {
            candidate.previous_terminal.kind =
                RecoveryMaintenancePreviousTerminalKindV1::
                    kSealed;
        } else {
            return false;
        }
        if (!parser->Consume(",\"marker_bytes\":") ||
            !parser->ParseNullableHex(
                &candidate.previous_terminal
                     .marker_bytes) ||
            !parser->Consume(",\"marker_sha256\":") ||
            !parser->ParseNullableHex(
                &candidate.previous_terminal
                     .marker_sha256) ||
            !parser->Consume(
                ",\"segment_sequence\":")) {
            return false;
        }
        if (parser->Consume("null")) {
            candidate.previous_terminal
                .segment_sequence = 0U;
        } else if (!parser->ParseU32(
                       &candidate.previous_terminal
                            .segment_sequence)) {
            return false;
        }
        if (!parser->Consume(
                ",\"segment_sha256\":") ||
            !parser->ParseNullableHex(
                &candidate.previous_terminal
                     .segment_sha256) ||
            !parser->Consume("}")) {
            return false;
        }
    }
    if (!parser->Consume(
            ",\"reopens_empty_tombstone_sha256\":") ||
        !parser->ParseNullableHex(
            &candidate
                 .reopens_empty_tombstone_sha256) ||
        !parser->Consume(
            ",\"reopens_sealed_raw_certificate_sha256\":") ||
        !parser->ParseNullableHex(
            &candidate
                 .reopens_sealed_raw_certificate_sha256) ||
        !parser->Consume(",\"reuse_segment\":")) {
        return false;
    }
    if (!parser->Consume("null")) {
        if (!parser->Consume(
                "{\"reported_logical_end_offset\":") ||
            !parser->ParseQuotedU64(
                &candidate.reuse_segment
                     .reported_logical_end_offset) ||
            !parser->Consume(
                ",\"segment_base_wal_pos\":") ||
            !parser->ParseQuotedU64(
                &candidate.reuse_segment
                     .segment_base_wal_pos) ||
            !parser->Consume(
                ",\"segment_header_sha256\":") ||
            !parser->ParseHex(
                &candidate.reuse_segment
                     .segment_header_sha256) ||
            !parser->Consume(
                ",\"segment_prefix_sha256\":") ||
            !parser->ParseHex(
                &candidate.reuse_segment
                     .segment_prefix_sha256) ||
            !parser->Consume(
                ",\"segment_sequence\":") ||
            !parser->ParseU32(
                &candidate.reuse_segment
                     .segment_sequence) ||
            !parser->Consume("}")) {
            return false;
        }
    }
    if (!parser->Consume("}")) {
        return false;
    }
    *output = candidate;
    return true;
}

}  // namespace

std::string_view
RecoveryMaintenanceReportV1ErrorName(
    RecoveryMaintenanceReportV1Error error) noexcept {
    switch (error) {
    case RecoveryMaintenanceReportV1Error::kNone:
        return "none";
    case RecoveryMaintenanceReportV1Error::kNullOutput:
        return "null_output";
    case RecoveryMaintenanceReportV1Error::kInvalidArgument:
        return "invalid_argument";
    case RecoveryMaintenanceReportV1Error::kInvalidNamespace:
        return "invalid_namespace";
    case RecoveryMaintenanceReportV1Error::
        kInvalidRecoveryAttempt:
        return "invalid_recovery_attempt";
    case RecoveryMaintenanceReportV1Error::
        kInvalidJournalHeader:
        return "invalid_journal_header";
    case RecoveryMaintenanceReportV1Error::
        kRecoveryPlanFatal:
        return "recovery_plan_fatal";
    case RecoveryMaintenanceReportV1Error::
        kRecoveryExecutionInvalid:
        return "recovery_execution_invalid";
    case RecoveryMaintenanceReportV1Error::kInvalidManifest:
        return "invalid_manifest";
    case RecoveryMaintenanceReportV1Error::
        kMissingOpenManifest:
        return "missing_open_manifest";
    case RecoveryMaintenanceReportV1Error::
        kOpenSegmentInvalid:
        return "open_segment_invalid";
    case RecoveryMaintenanceReportV1Error::
        kOpenSegmentMismatch:
        return "open_segment_mismatch";
    case RecoveryMaintenanceReportV1Error::kMarkerInvalid:
        return "marker_invalid";
    case RecoveryMaintenanceReportV1Error::kCursorMismatch:
        return "cursor_mismatch";
    case RecoveryMaintenanceReportV1Error::kRangeInvalid:
        return "range_invalid";
    case RecoveryMaintenanceReportV1Error::
        kFrontierMismatch:
        return "frontier_mismatch";
    case RecoveryMaintenanceReportV1Error::
        kVariantMismatch:
        return "variant_mismatch";
    case RecoveryMaintenanceReportV1Error::
        kSidecarMismatch:
        return "sidecar_mismatch";
    case RecoveryMaintenanceReportV1Error::
        kCommitmentMismatch:
        return "commitment_mismatch";
    case RecoveryMaintenanceReportV1Error::
        kInvalidCanonicalJson:
        return "invalid_canonical_json";
    case RecoveryMaintenanceReportV1Error::kFilenameInvalid:
        return "filename_invalid";
    case RecoveryMaintenanceReportV1Error::
        kEncodedSizeExceeded:
        return "encoded_size_exceeded";
    case RecoveryMaintenanceReportV1Error::
        kAllocationFailure:
        return "allocation_failure";
    }
    return "unknown";
}

RecoveryMaintenanceReportV1Error
ValidateRecoveryMaintenanceReportV1(
    const RecoveryMaintenanceReportV1& report) noexcept {
    return ValidateIntrinsic(report);
}

RecoveryMaintenanceReportV1Error
EncodeRecoveryMaintenanceReportV1Jcs(
    const RecoveryMaintenanceReportV1& report,
    std::string* output) noexcept {
    if (output == nullptr) {
        return RecoveryMaintenanceReportV1Error::
            kNullOutput;
    }
    const RecoveryMaintenanceReportV1Error validation =
        ValidateIntrinsic(report);
    if (validation !=
        RecoveryMaintenanceReportV1Error::kNone) {
        return validation;
    }
    try {
        std::string candidate;
        candidate.reserve(4096U);
        candidate.append("{\"capture_date\":");
        AppendInteger(
            &candidate,
            report.namespace_identity.capture_date);
        candidate.append(",\"closed_frontier\":");
        if (report.result ==
            RecoveryMaintenanceResultV1::
                kEmptyAnchorOnly) {
            candidate.append("null");
        } else {
            AppendClosedFrontier(
                &candidate, report.closed_frontier);
        }
        candidate.append(
            ",\"current_empty_anchor_tombstone_sha256\":");
        AppendNullableHex(
            &candidate,
            report
                .current_empty_anchor_tombstone_sha256);
        candidate.append(
            ",\"current_sealed_raw_certificate_sha256\":");
        AppendNullableHex(
            &candidate,
            report
                .current_sealed_raw_certificate_sha256);
        candidate.append(
            ",\"fatal_classification\":\"NONE\","
            "\"final_durable_cursor\":");
        if (report.result ==
            RecoveryMaintenanceResultV1::
                kEmptyAnchorOnly) {
            candidate.append("null");
        } else {
            AppendCursor(
                &candidate, report.final_durable_cursor);
        }
        candidate.append(",\"intent\":");
        AppendQuoted(&candidate, IntentName(report.intent));
        candidate.append(",\"journal_header_sha256\":");
        AppendHex(
            &candidate, report.journal_header_sha256);
        candidate.append(
            ",\"journal_tail_classification\":");
        AppendQuoted(
            &candidate, JournalTailName(report.journal_tail));
        candidate.append(",\"marker_count\":");
        AppendNullableQuotedU64(
            &candidate, report.marker_count);
        candidate.append(",\"open_boundary\":");
        if (report.result !=
            RecoveryMaintenanceResultV1::kResumedOpen) {
            candidate.append("null");
        } else {
            AppendOpenBoundary(
                &candidate, report.open_boundary);
        }
        candidate.append(",\"record_count\":");
        AppendNullableQuotedU64(
            &candidate, report.record_count);
        candidate.append(",\"recovery_attempt_id\":");
        AppendHex(
            &candidate, report.recovery_attempt_id);
        candidate.append(",\"recovery_range\":");
        if (report.result ==
            RecoveryMaintenanceResultV1::
                kEmptyAnchorOnly) {
            candidate.append("null");
        } else {
            AppendRecoveryRange(
                &candidate, report.recovery_range);
        }
        candidate.append(",\"result\":");
        AppendQuoted(&candidate, ResultName(report.result));
        candidate.append(",\"schema_version\":");
        AppendInteger(&candidate, report.schema_version);
        candidate.append(",\"sealed_boundary\":");
        if (report.result ==
            RecoveryMaintenanceResultV1::kSealedRaw) {
            AppendSealedBoundary(
                &candidate, report.sealed_boundary);
        } else {
            candidate.append("null");
        }
        candidate.append(",\"segment_count\":");
        AppendNullableQuotedU64(
            &candidate, report.segment_count);
        candidate.append(
            ",\"segment_tail_classification\":");
        if (report.result ==
            RecoveryMaintenanceResultV1::
                kEmptyAnchorOnly) {
            candidate.append("null");
        } else {
            AppendQuoted(
                &candidate,
                SegmentTailName(report.segment_tail));
        }
        candidate.append(",\"source_stream_id\":");
        AppendInteger(
            &candidate,
            report.namespace_identity.source_stream_id);
        candidate.append(",\"stream_day_id\":");
        AppendHex(
            &candidate,
            report.namespace_identity.stream_day_id);
        candidate.push_back('}');
        if (candidate.size() >
            kRecoveryMaintenanceReportV1MaximumBytes) {
            return RecoveryMaintenanceReportV1Error::
                kEncodedSizeExceeded;
        }
        output->swap(candidate);
        return RecoveryMaintenanceReportV1Error::kNone;
    } catch (const std::bad_alloc&) {
        return RecoveryMaintenanceReportV1Error::
            kAllocationFailure;
    } catch (...) {
        return RecoveryMaintenanceReportV1Error::
            kAllocationFailure;
    }
}

RecoveryMaintenanceReportV1Error
ParseRecoveryMaintenanceReportV1Jcs(
    std::string_view exact_bytes,
    RecoveryMaintenanceReportV1* output) noexcept {
    if (output == nullptr) {
        return RecoveryMaintenanceReportV1Error::
            kNullOutput;
    }
    if (exact_bytes.empty() ||
        exact_bytes.size() >
            kRecoveryMaintenanceReportV1MaximumBytes) {
        return RecoveryMaintenanceReportV1Error::
            kInvalidCanonicalJson;
    }
    try {
        CanonicalParser parser(exact_bytes);
        RecoveryMaintenanceReportV1 candidate{};
        if (!parser.Consume("{\"capture_date\":") ||
            !parser.ParseU32(
                &candidate.namespace_identity
                     .capture_date) ||
            !parser.Consume(",\"closed_frontier\":")) {
            return RecoveryMaintenanceReportV1Error::
                kInvalidCanonicalJson;
        }
        if (!parser.Consume("null") &&
            !ParseClosedFrontier(
                &parser, &candidate.closed_frontier)) {
            return RecoveryMaintenanceReportV1Error::
                kInvalidCanonicalJson;
        }
        if (!parser.Consume(
                ",\"current_empty_anchor_tombstone_sha256\":") ||
            !parser.ParseNullableHex(
                &candidate
                     .current_empty_anchor_tombstone_sha256) ||
            !parser.Consume(
                ",\"current_sealed_raw_certificate_sha256\":") ||
            !parser.ParseNullableHex(
                &candidate
                     .current_sealed_raw_certificate_sha256) ||
            !parser.Consume(
                ",\"fatal_classification\":\"NONE\","
                "\"final_durable_cursor\":")) {
            return RecoveryMaintenanceReportV1Error::
                kInvalidCanonicalJson;
        }
        if (!parser.Consume("null") &&
            !ParseCursor(
                &parser,
                &candidate.final_durable_cursor)) {
            return RecoveryMaintenanceReportV1Error::
                kInvalidCanonicalJson;
        }
        if (!parser.Consume(",\"intent\":")) {
            return RecoveryMaintenanceReportV1Error::
                kInvalidCanonicalJson;
        }
        if (parser.Consume("\"RESUME_CONNECT\"")) {
            candidate.intent =
                RecoveryMaintenanceIntentV1::
                    kResumeConnect;
        } else if (
            parser.Consume("\"RECOVER_SEAL_ONLY\"")) {
            candidate.intent =
                RecoveryMaintenanceIntentV1::
                    kRecoverSealOnly;
        } else {
            return RecoveryMaintenanceReportV1Error::
                kInvalidCanonicalJson;
        }
        if (!parser.Consume(
                ",\"journal_header_sha256\":") ||
            !parser.ParseHex(
                &candidate.journal_header_sha256) ||
            !parser.Consume(
                ",\"journal_tail_classification\":")) {
            return RecoveryMaintenanceReportV1Error::
                kInvalidCanonicalJson;
        }
        if (parser.Consume("\"NONE\"")) {
            candidate.journal_tail =
                RawRecoveryJournalTailV1::kNone;
        } else if (
            parser.Consume("\"PARTIAL_MARKER\"")) {
            candidate.journal_tail =
                RawRecoveryJournalTailV1::
                    kPartialMarker;
        } else if (
            parser.Consume(
                "\"TERMINAL_CRC_INVALID_MARKER\"")) {
            candidate.journal_tail =
                RawRecoveryJournalTailV1::
                    kTerminalCrcInvalidMarker;
        } else {
            return RecoveryMaintenanceReportV1Error::
                kInvalidCanonicalJson;
        }
        if (!parser.Consume(",\"marker_count\":") ||
            !parser.ParseNullableQuotedU64(
                &candidate.marker_count) ||
            !parser.Consume(",\"open_boundary\":")) {
            return RecoveryMaintenanceReportV1Error::
                kInvalidCanonicalJson;
        }
        if (!parser.Consume("null") &&
            !ParseOpenBoundary(
                &parser, &candidate.open_boundary)) {
            return RecoveryMaintenanceReportV1Error::
                kInvalidCanonicalJson;
        }
        if (!parser.Consume(",\"record_count\":") ||
            !parser.ParseNullableQuotedU64(
                &candidate.record_count) ||
            !parser.Consume(
                ",\"recovery_attempt_id\":") ||
            !parser.ParseHex(
                &candidate.recovery_attempt_id) ||
            !parser.Consume(",\"recovery_range\":")) {
            return RecoveryMaintenanceReportV1Error::
                kInvalidCanonicalJson;
        }
        if (!parser.Consume("null") &&
            !ParseRecoveryRange(
                &parser, &candidate.recovery_range)) {
            return RecoveryMaintenanceReportV1Error::
                kInvalidCanonicalJson;
        }
        if (!parser.Consume(",\"result\":")) {
            return RecoveryMaintenanceReportV1Error::
                kInvalidCanonicalJson;
        }
        if (parser.Consume("\"RESUMED_OPEN\"")) {
            candidate.result =
                RecoveryMaintenanceResultV1::
                    kResumedOpen;
        } else if (parser.Consume("\"SEALED_RAW\"")) {
            candidate.result =
                RecoveryMaintenanceResultV1::kSealedRaw;
        } else if (
            parser.Consume("\"EMPTY_ANCHOR_ONLY\"")) {
            candidate.result =
                RecoveryMaintenanceResultV1::
                    kEmptyAnchorOnly;
        } else {
            return RecoveryMaintenanceReportV1Error::
                kInvalidCanonicalJson;
        }
        if (!parser.Consume(",\"schema_version\":") ||
            !parser.ParseU32(&candidate.schema_version) ||
            !parser.Consume(",\"sealed_boundary\":")) {
            return RecoveryMaintenanceReportV1Error::
                kInvalidCanonicalJson;
        }
        if (!parser.Consume("null") &&
            !ParseSealedBoundary(
                &parser, &candidate.sealed_boundary)) {
            return RecoveryMaintenanceReportV1Error::
                kInvalidCanonicalJson;
        }
        if (!parser.Consume(",\"segment_count\":") ||
            !parser.ParseNullableQuotedU64(
                &candidate.segment_count) ||
            !parser.Consume(
                ",\"segment_tail_classification\":")) {
            return RecoveryMaintenanceReportV1Error::
                kInvalidCanonicalJson;
        }
        if (parser.Consume("null") ||
            parser.Consume("\"NONE\"")) {
            candidate.segment_tail =
                RawRecoverySegmentTailV1::kNone;
        } else if (
            parser.Consume("\"PARTIAL_RECORD\"")) {
            candidate.segment_tail =
                RawRecoverySegmentTailV1::
                    kPartialRecord;
        } else if (
            parser.Consume("\"INVALID_RECORD\"")) {
            candidate.segment_tail =
                RawRecoverySegmentTailV1::
                    kInvalidRecord;
        } else if (
            parser.Consume("\"ZERO_PREALLOCATION\"")) {
            candidate.segment_tail =
                RawRecoverySegmentTailV1::
                    kZeroPreallocation;
        } else {
            return RecoveryMaintenanceReportV1Error::
                kInvalidCanonicalJson;
        }
        if (!parser.Consume(",\"source_stream_id\":") ||
            !parser.ParseU32(
                &candidate.namespace_identity
                     .source_stream_id) ||
            !parser.Consume(",\"stream_day_id\":") ||
            !parser.ParseHex(
                &candidate.namespace_identity
                     .stream_day_id) ||
            !parser.Consume("}") ||
            !parser.done()) {
            return RecoveryMaintenanceReportV1Error::
                kInvalidCanonicalJson;
        }
        const RecoveryMaintenanceReportV1Error validation =
            ValidateIntrinsic(candidate);
        if (validation !=
            RecoveryMaintenanceReportV1Error::kNone) {
            return validation;
        }
        std::string canonical;
        if (EncodeRecoveryMaintenanceReportV1Jcs(
                candidate, &canonical) !=
                RecoveryMaintenanceReportV1Error::kNone ||
            canonical != exact_bytes) {
            return RecoveryMaintenanceReportV1Error::
                kInvalidCanonicalJson;
        }
        using std::swap;
        swap(*output, candidate);
        return RecoveryMaintenanceReportV1Error::kNone;
    } catch (const std::bad_alloc&) {
        return RecoveryMaintenanceReportV1Error::
            kAllocationFailure;
    } catch (...) {
        return RecoveryMaintenanceReportV1Error::
            kInvalidCanonicalJson;
    }
}

RecoveryMaintenanceReportV1Error
RecoveryMaintenanceReportV1Filename(
    const RecoveryMaintenanceReportV1& report,
    std::string* output) noexcept {
    if (output == nullptr) {
        return RecoveryMaintenanceReportV1Error::
            kNullOutput;
    }
    const RecoveryMaintenanceReportV1Error validation =
        ValidateIntrinsic(report);
    if (validation !=
        RecoveryMaintenanceReportV1Error::kNone) {
        return validation;
    }
    try {
        std::string candidate = "recovery-";
        candidate.append(
            l2flow::common::Identity128Hex(
                report.recovery_attempt_id));
        candidate.append(".json");
        if (candidate.size() !=
            std::string_view("recovery-").size() +
                32U +
                std::string_view(".json").size()) {
            return RecoveryMaintenanceReportV1Error::
                kFilenameInvalid;
        }
        output->swap(candidate);
        return RecoveryMaintenanceReportV1Error::kNone;
    } catch (...) {
        return RecoveryMaintenanceReportV1Error::
            kAllocationFailure;
    }
}

RecoveryMaintenanceReportV1Error
BuildResumedOpenRecoveryMaintenanceReportV1(
    const RawReserveRegistryEntryKeyV1& recovery_key,
    const RawV1JournalHeaderWire& journal_header_bytes,
    const RawRecoveryPlanV1& analysis,
    const RawRecoveryExecutionResultV1& execution,
    std::uint64_t analyzed_journal_size,
    const RawManifestV1& final_open_manifest,
    std::shared_ptr<const std::vector<std::byte>>
        final_open_segment,
    const RawWalSinkIdentityV1& final_sink_identity,
    const RawWalWriterSnapshot& final_wal,
    const RecoveryMaintenanceNewOpenPredecessorV1*
        new_open_predecessor,
    std::unique_ptr<
        BuiltRecoveryMaintenanceReportV1>* output) noexcept {
    if (output == nullptr) {
        return RecoveryMaintenanceReportV1Error::
            kNullOutput;
    }
    try {
        if (!analysis.ok()) {
            return RecoveryMaintenanceReportV1Error::
                kRecoveryPlanFatal;
        }
        if (execution.failure !=
                RawRecoveryExecutionFailureV1::kNone ||
            !execution.cursor_publishable) {
            return RecoveryMaintenanceReportV1Error::
                kRecoveryExecutionInvalid;
        }
        if (analyzed_journal_size <
                analysis.accepted_journal_size ||
            analysis.accepted_journal_size <
                kRawV1JournalHeaderBytes ||
            execution.retained_journal_size <
                kRawV1JournalHeaderBytes ||
            (execution.retained_journal_size -
             kRawV1JournalHeaderBytes) %
                    kRawV1DurableMarkerBytes !=
                0U) {
            return RecoveryMaintenanceReportV1Error::
                kRangeInvalid;
        }

        DurableJournalHeaderV1 decoded_journal{};
        RawV1JournalHeaderWire reencoded_journal{};
        if (DecodeDurableJournalHeaderV1(
                journal_header_bytes,
                &decoded_journal) != RawV1Error::kNone ||
            decoded_journal.raw_schema_sha256 !=
                kFrozenRawSchemaSha256 ||
            EncodeDurableJournalHeaderV1(
                analysis.journal_header,
                &reencoded_journal) !=
                RawV1Error::kNone ||
            reencoded_journal != journal_header_bytes) {
            return RecoveryMaintenanceReportV1Error::
                kInvalidJournalHeader;
        }
        const RawManifestNamespaceV1 journal_namespace{
            decoded_journal.capture_date,
            decoded_journal.source_stream_id,
            decoded_journal.stream_day_id};
        const RawManifestNamespaceV1 key_namespace{
            recovery_key.route.capture_date,
            recovery_key.route.source_stream_id,
            recovery_key.stream_day_id};
        if (!SameNamespace(
                journal_namespace, key_namespace)) {
            return RecoveryMaintenanceReportV1Error::
                kInvalidNamespace;
        }
        if (l2flow::common::IsZeroIdentity(
                recovery_key.recovery_attempt_id)) {
            return RecoveryMaintenanceReportV1Error::
                kInvalidRecoveryAttempt;
        }
        if (ValidateManifestModel(final_open_manifest) !=
            RawManifestV1Error::kNone) {
            return RecoveryMaintenanceReportV1Error::
                kInvalidManifest;
        }
        if (!final_open_manifest.open_entry.has_value()) {
            return RecoveryMaintenanceReportV1Error::
                kMissingOpenManifest;
        }
        if (!SameNamespace(
                final_open_manifest.namespace_identity,
                key_namespace)) {
            return RecoveryMaintenanceReportV1Error::
                kInvalidNamespace;
        }
        const RawManifestSegmentEntryV1& opened =
            *final_open_manifest.open_entry;
        if (final_open_segment == nullptr ||
            opened.segment_flags != 0U ||
            !l2flow::common::IsZeroIdentity(
                opened.reserve_state_uuid) ||
            !l2flow::common::IsZeroIdentity(
                opened.finalization_cycle_id) ||
            !IsZero(opened.immutable_grant_sha256) ||
            opened.maintenance_report_locator
                .has_value() ||
            opened.archive_locator.has_value() ||
            analysis.r11_orphan ==
                RawRecoveryR11OrphanV1::
                    kFinalizationContinuationHeaderOnly ||
            opened.segment_logical_length <
                kRawV1SegmentHeaderBytes ||
            opened.segment_logical_length >
                final_open_segment->size()) {
            return RecoveryMaintenanceReportV1Error::
                kOpenSegmentInvalid;
        }

        const std::span<const std::byte> segment_bytes{
            final_open_segment->data(),
            final_open_segment->size()};
        SegmentHeaderV1 decoded_segment{};
        if (DecodeSegmentHeaderV1(
                segment_bytes.first(
                    kRawV1SegmentHeaderBytes),
                &decoded_segment) != RawV1Error::kNone) {
            return RecoveryMaintenanceReportV1Error::
                kOpenSegmentInvalid;
        }
        if (decoded_segment.source_stream_id !=
                key_namespace.source_stream_id ||
            decoded_segment.capture_date !=
                key_namespace.capture_date ||
            decoded_segment.stream_day_id !=
                key_namespace.stream_day_id ||
            decoded_segment.segment_sequence !=
                opened.segment_sequence ||
            decoded_segment.segment_flags !=
                opened.segment_flags ||
            decoded_segment.segment_base_wal_pos !=
                opened.segment_base_wal_pos ||
            decoded_segment.first_ingress_sequence !=
                opened.next_expected_first_ingress_sequence ||
            final_sink_identity.source_stream_id !=
                key_namespace.source_stream_id ||
            final_sink_identity.capture_date !=
                key_namespace.capture_date ||
            final_sink_identity.stream_day_id !=
                key_namespace.stream_day_id ||
            final_sink_identity.segment_sequence !=
                opened.segment_sequence ||
            final_sink_identity.segment_base_wal_pos !=
                opened.segment_base_wal_pos ||
            final_sink_identity.first_ingress_sequence !=
                decoded_segment.first_ingress_sequence ||
            l2flow::common::IsZeroIdentity(
                final_sink_identity.writer_instance)) {
            return RecoveryMaintenanceReportV1Error::
                kOpenSegmentMismatch;
        }
        if (!final_wal.initialized ||
            final_wal.sealed ||
            final_wal.closed ||
            final_wal.fatal ||
            final_wal.append != final_wal.durable ||
            final_wal.journal_logical_size <
                execution.retained_journal_size ||
            (final_wal.journal_logical_size -
             kRawV1JournalHeaderBytes) %
                    kRawV1DurableMarkerBytes !=
                0U) {
            return RecoveryMaintenanceReportV1Error::
                kCursorMismatch;
        }

        DurableMarkerV1 endpoint_marker{};
        if (DecodeDurableMarkerV1(
                opened.accepted_marker_bytes,
                &endpoint_marker) != RawV1Error::kNone ||
            endpoint_marker.marker_flags != 0U ||
            endpoint_marker.source_stream_id !=
                key_namespace.source_stream_id ||
            endpoint_marker.segment_sequence !=
                opened.segment_sequence ||
            opened.accepted_marker_sha256 !=
                ComputeAcceptedMarkerSha256(
                    opened.accepted_marker_bytes)) {
            return RecoveryMaintenanceReportV1Error::
                kMarkerInvalid;
        }
        const RecoveryMaintenanceCursorV1 final_cursor =
            CursorFrom(
                opened.segment_sequence,
                final_wal.durable,
                0U);
        if (!SameCursor(
                final_cursor,
                CursorFrom(endpoint_marker)) ||
            final_cursor.segment_offset !=
                opened.segment_logical_length) {
            return RecoveryMaintenanceReportV1Error::
                kCursorMismatch;
        }

        const auto logical_prefix =
            segment_bytes.first(
                static_cast<std::size_t>(
                    opened.segment_logical_length));
        const RawV1Digest prefix_sha =
            l2flow::common::ComputeSha256(logical_prefix);
        const RawV1Digest header_sha =
            l2flow::common::ComputeSha256(
                segment_bytes.first(
                    kRawV1SegmentHeaderBytes));
        if (opened.segment_sha256 != prefix_sha) {
            return RecoveryMaintenanceReportV1Error::
                kOpenSegmentMismatch;
        }
        RawV1Digest open_commitment{};
        if (ComputeRawManifestOpenEntryCommitmentV1(
                final_open_manifest,
                &open_commitment,
                nullptr) != RawManifestV1Error::kNone) {
            return RecoveryMaintenanceReportV1Error::
                kCommitmentMismatch;
        }

        const RawRecoverySegmentPlanV1* final_plan =
            nullptr;
        for (const RawRecoverySegmentPlanV1& plan :
             analysis.segments) {
            if (plan.segment_sequence ==
                opened.segment_sequence) {
                final_plan = &plan;
                break;
            }
        }
        const RawRecoverySegmentPlanV1* range_plan =
            final_plan;
        if (range_plan == nullptr &&
            !analysis.segments.empty()) {
            range_plan = &analysis.segments.back();
        }
        if (range_plan == nullptr) {
            return RecoveryMaintenanceReportV1Error::
                kRangeInvalid;
        }

        const RecoveryMaintenanceCursorV1
            execution_cursor =
                CursorFrom(execution.recovered_cursor);
        if (new_open_predecessor == nullptr) {
            if (!analysis.has_accepted_cursor ||
                final_plan == nullptr ||
                final_plan->sealed ||
                final_plan
                        ->validated_logical_end_offset !=
                    opened.segment_logical_length ||
                final_plan
                        ->validated_last_ingress_sequence !=
                    endpoint_marker
                        .durable_ingress_sequence ||
                !SameCursor(
                    execution_cursor, final_cursor) ||
                final_wal.journal_logical_size !=
                    execution.retained_journal_size) {
                return RecoveryMaintenanceReportV1Error::
                    kVariantMismatch;
            }
        }

        RecoveryMaintenanceReportV1 report{};
        report.intent =
            RecoveryMaintenanceIntentV1::kResumeConnect;
        report.result =
            RecoveryMaintenanceResultV1::kResumedOpen;
        report.namespace_identity = key_namespace;
        report.recovery_attempt_id =
            recovery_key.recovery_attempt_id;
        report.journal_header_sha256 =
            l2flow::common::ComputeSha256(
                journal_header_bytes);
        report.journal_tail = analysis.journal_tail;
        report.segment_tail = range_plan->tail;
        report.final_durable_cursor = final_cursor;
        report.closed_frontier.closed_entry_count =
            final_open_manifest.closed_entry_count;
        report.closed_frontier.closed_prefix_sha256 =
            final_open_manifest.closed_prefix_sha256;
        if (!final_open_manifest.closed_entries.empty()) {
            const RawManifestSegmentEntryV1& frontier =
                final_open_manifest.closed_entries.back();
            report.closed_frontier
                .frontier_seal_marker_sha256 =
                frontier.accepted_marker_sha256;
            report.closed_frontier
                .frontier_segment_sequence =
                frontier.segment_sequence;
            report.closed_frontier
                .frontier_segment_sha256 =
                frontier.segment_sha256;
        }
        report.open_boundary.endpoint_marker.cursor =
            final_cursor;
        report.open_boundary.endpoint_marker.marker_bytes =
            opened.accepted_marker_bytes;
        report.open_boundary.endpoint_marker.marker_sha256 =
            opened.accepted_marker_sha256;
        report.open_boundary
            .manifest_entry_commitment_sha256 =
            open_commitment;
        report.open_boundary.manifest_generation =
            final_open_manifest.manifest_generation;

        report.recovery_range.analyzed_journal_size =
            analyzed_journal_size;
        report.recovery_range
            .initial_accepted_journal_size =
            analysis.accepted_journal_size;
        report.recovery_range.post_repair_journal_size =
            execution.retained_journal_size;
        report.recovery_range.final_durable_journal_size =
            final_wal.journal_logical_size;
        if (analysis.has_accepted_cursor) {
            report.recovery_range
                .initial_durable_cursor =
                CursorFrom(analysis.accepted_cursor);
        }
        report.recovery_range.range_segment_sequence =
            range_plan->segment_sequence;
        report.recovery_range.raw_append_begin_offset =
            range_plan->append_only_begin_offset;
        report.recovery_range.raw_append_end_offset =
            range_plan->append_only_end_offset;
        report.recovery_range.raw_repair_begin_offset =
            range_plan->tail_begin_offset;
        report.recovery_range.raw_repair_end_offset =
            range_plan->tail_end_offset;

        if (new_open_predecessor == nullptr) {
            report.open_boundary.open_variant =
                RecoveryMaintenanceOpenVariantV1::
                    kReuseOpen;
            report.open_boundary.reuse_segment = {
                opened.segment_logical_length,
                opened.segment_base_wal_pos,
                header_sha,
                prefix_sha,
                opened.segment_sequence};
        } else {
            report.open_boundary.open_variant =
                RecoveryMaintenanceOpenVariantV1::
                    kNewOpenAfterSealed;
            report.open_boundary.new_segment = {
                opened.segment_base_wal_pos,
                header_sha,
                opened.segment_sequence};
            report.open_boundary.previous_terminal =
                new_open_predecessor->terminal;
            report.open_boundary
                .reopens_empty_tombstone_sha256 =
                new_open_predecessor
                    ->reopens_empty_tombstone_sha256;
            report.open_boundary
                .reopens_sealed_raw_certificate_sha256 =
                new_open_predecessor
                    ->reopens_sealed_raw_certificate_sha256;

            const RecoveryMaintenancePreviousTerminalV1&
                previous =
                    new_open_predecessor->terminal;
            if (previous.kind ==
                RecoveryMaintenancePreviousTerminalKindV1::
                    kCanonicalZero) {
                if (opened.record_count != 0U ||
                    opened.actual_first_ingress_sequence
                        .has_value() ||
                    opened.actual_last_ingress_sequence
                        .has_value() ||
                    decoded_segment
                            .first_ingress_sequence !=
                        1U ||
                    !SameCursor(
                        execution_cursor,
                        final_cursor)) {
                    return RecoveryMaintenanceReportV1Error::
                        kVariantMismatch;
                }
            } else if (
                previous.kind ==
                RecoveryMaintenancePreviousTerminalKindV1::
                    kSealed) {
                DurableMarkerV1 sealed{};
                if (DecodeDurableMarkerV1(
                        previous.marker_bytes,
                        &sealed) != RawV1Error::kNone ||
                    sealed.durable_ingress_sequence ==
                        std::numeric_limits<
                            std::uint64_t>::max() ||
                    decoded_segment
                            .first_ingress_sequence !=
                        sealed.durable_ingress_sequence +
                            1U ||
                    opened.record_count != 0U ||
                    opened.actual_first_ingress_sequence
                        .has_value() ||
                    opened.actual_last_ingress_sequence
                        .has_value()) {
                    return RecoveryMaintenanceReportV1Error::
                        kVariantMismatch;
                }
                const RecoveryMaintenanceCursorV1
                    sealed_cursor = CursorFrom(sealed);
                if (!SameCursor(
                        execution_cursor,
                        final_cursor) &&
                    !SameCursorIgnoringFlags(
                        execution_cursor,
                        sealed_cursor)) {
                    return RecoveryMaintenanceReportV1Error::
                        kVariantMismatch;
                }
            }
        }

        const RecoveryMaintenanceReportV1Error validation =
            ValidateIntrinsic(report);
        if (validation !=
            RecoveryMaintenanceReportV1Error::kNone) {
            return validation;
        }
        std::string canonical_jcs;
        RecoveryMaintenanceReportV1Error error =
            EncodeRecoveryMaintenanceReportV1Jcs(
                report, &canonical_jcs);
        if (error !=
            RecoveryMaintenanceReportV1Error::kNone) {
            return error;
        }
        std::string filename;
        error = RecoveryMaintenanceReportV1Filename(
            report, &filename);
        if (error !=
            RecoveryMaintenanceReportV1Error::kNone) {
            return error;
        }
        const RawV1Digest report_sha256 =
            l2flow::common::ComputeSha256(canonical_jcs);
        auto built = std::unique_ptr<
            BuiltRecoveryMaintenanceReportV1>(
            new BuiltRecoveryMaintenanceReportV1(
                std::move(report),
                std::move(canonical_jcs),
                std::move(filename),
                report_sha256,
                final_sink_identity.writer_instance,
                final_cursor));
        *output = std::move(built);
        return RecoveryMaintenanceReportV1Error::kNone;
    } catch (const std::bad_alloc&) {
        return RecoveryMaintenanceReportV1Error::
            kAllocationFailure;
    } catch (...) {
        return RecoveryMaintenanceReportV1Error::
            kAllocationFailure;
    }
}

RecoveryMaintenanceReportV1Error
BuildSealedRawRecoveryMaintenanceReportV1(
    const RawReserveRegistryEntryKeyV1& recovery_key,
    const RawV1JournalHeaderWire& journal_header_bytes,
    const RawRecoveryPlanV1& analysis,
    const RawRecoveryExecutionResultV1& execution,
    std::uint64_t analyzed_journal_size,
    const BuiltSealedRawCertificateV1&
        sealed_certificate,
    std::unique_ptr<
        BuiltRecoveryMaintenanceReportV1>* output) noexcept {
    if (output == nullptr) {
        return RecoveryMaintenanceReportV1Error::
            kNullOutput;
    }
    try {
        if (!analysis.ok()) {
            return RecoveryMaintenanceReportV1Error::
                kRecoveryPlanFatal;
        }
        if (execution.failure !=
                RawRecoveryExecutionFailureV1::kNone ||
            execution.error_number != 0 ||
            !execution.cursor_publishable ||
            execution.mutated !=
                (analysis.journal_tail !=
                 RawRecoveryJournalTailV1::kNone)) {
            return RecoveryMaintenanceReportV1Error::
                kRecoveryExecutionInvalid;
        }
        if (analysis.accepted_journal_size <
                kRawV1JournalHeaderBytes ||
            (analysis.accepted_journal_size -
             kRawV1JournalHeaderBytes) %
                    kRawV1DurableMarkerBytes !=
                0U ||
            analyzed_journal_size <
                analysis.accepted_journal_size ||
            execution.retained_journal_size !=
                analysis.accepted_journal_size) {
            return RecoveryMaintenanceReportV1Error::
                kRangeInvalid;
        }
        if (!IsExactAnalyzedJournalSize(
                analysis.accepted_journal_size,
                analyzed_journal_size,
                analysis.journal_tail)) {
            return RecoveryMaintenanceReportV1Error::
                kRangeInvalid;
        }

        DurableJournalHeaderV1 decoded_journal{};
        RawV1JournalHeaderWire reencoded_journal{};
        if (DecodeDurableJournalHeaderV1(
                journal_header_bytes,
                &decoded_journal) != RawV1Error::kNone ||
            decoded_journal.raw_schema_sha256 !=
                kFrozenRawSchemaSha256 ||
            EncodeDurableJournalHeaderV1(
                analysis.journal_header,
                &reencoded_journal) !=
                RawV1Error::kNone ||
            reencoded_journal != journal_header_bytes) {
            return RecoveryMaintenanceReportV1Error::
                kInvalidJournalHeader;
        }
        const RawManifestNamespaceV1 journal_namespace{
            decoded_journal.capture_date,
            decoded_journal.source_stream_id,
            decoded_journal.stream_day_id};
        const RawManifestNamespaceV1 key_namespace{
            recovery_key.route.capture_date,
            recovery_key.route.source_stream_id,
            recovery_key.stream_day_id};
        if (!SameNamespace(
                journal_namespace, key_namespace)) {
            return RecoveryMaintenanceReportV1Error::
                kInvalidNamespace;
        }
        if (l2flow::common::IsZeroIdentity(
                recovery_key.recovery_attempt_id)) {
            return RecoveryMaintenanceReportV1Error::
                kInvalidRecoveryAttempt;
        }

        const SealedRawCertificateV1& certificate =
            sealed_certificate.model();
        const RawV1Digest journal_sha256 =
            l2flow::common::ComputeSha256(
                journal_header_bytes);
        if (ValidateSealedRawCertificateV1(certificate) !=
                SealedRawCertificateV1Error::kNone ||
            IsZero(
                sealed_certificate
                    .certificate_sha256()) ||
            l2flow::common::ComputeSha256(
                sealed_certificate.canonical_jcs()) !=
                sealed_certificate
                    .certificate_sha256() ||
            !SameNamespace(
                certificate.namespace_identity,
                key_namespace) ||
            certificate.journal_header_sha256 !=
                journal_sha256) {
            return RecoveryMaintenanceReportV1Error::
                kSidecarMismatch;
        }

        const auto& append_cursor =
            certificate.terminal_append_cursor;
        const auto& durable_cursor =
            certificate.terminal_durable_cursor;
        if (append_cursor.segment_sequence !=
                durable_cursor.segment_sequence ||
            append_cursor.global_wal_pos !=
                durable_cursor.global_wal_pos ||
            append_cursor.ingress_sequence !=
                durable_cursor.ingress_sequence ||
            append_cursor.segment_offset !=
                durable_cursor.segment_offset) {
            return RecoveryMaintenanceReportV1Error::
                kSidecarMismatch;
        }
        const RecoveryMaintenanceCursorV1 final_cursor{
            durable_cursor.segment_sequence,
            durable_cursor.global_wal_pos,
            durable_cursor.ingress_sequence,
            kRawV1SegmentSealed,
            durable_cursor.segment_offset};
        const RecoveryMaintenanceCursorV1
            analysis_cursor =
                CursorFrom(analysis.accepted_cursor);
        const RecoveryMaintenanceCursorV1
            execution_cursor =
                CursorFrom(execution.recovered_cursor);
        if (!analysis.has_accepted_cursor ||
            analysis.initial_anchor !=
                RawRecoveryInitialAnchorV1::kNone ||
            analysis.r11_orphan !=
                RawRecoveryR11OrphanV1::kNone ||
            analysis.segments.empty() ||
            analysis.segments.size() !=
                certificate.closed_entry_count ||
            !SameCursor(analysis_cursor, final_cursor) ||
            !SameCursor(execution_cursor, final_cursor)) {
            return RecoveryMaintenanceReportV1Error::
                kCursorMismatch;
        }
        for (const RawRecoverySegmentPlanV1& plan :
             analysis.segments) {
            if (!plan.sealed ||
                plan.tail !=
                    RawRecoverySegmentTailV1::kNone) {
                return RecoveryMaintenanceReportV1Error::
                    kFrontierMismatch;
            }
        }
        const RawRecoverySegmentPlanV1& terminal_plan =
            analysis.segments.back();
        if (terminal_plan.segment_sequence !=
                certificate.last_segment_sequence ||
            terminal_plan.segment_base_wal_pos !=
                certificate.last_segment_base_wal_pos ||
            !terminal_plan.has_accepted_marker ||
            terminal_plan.accepted_marker_wire !=
                certificate
                    .accepted_sealed_marker_bytes ||
            terminal_plan.durable_end_offset !=
                certificate
                    .last_segment_logical_length ||
            terminal_plan.validated_logical_end_offset !=
                certificate
                    .last_segment_logical_length ||
            terminal_plan.append_only_begin_offset !=
                certificate
                    .last_segment_logical_length ||
            terminal_plan.append_only_end_offset !=
                certificate
                    .last_segment_logical_length ||
            terminal_plan.tail_begin_offset !=
                certificate
                    .last_segment_logical_length ||
            terminal_plan.tail_end_offset !=
                certificate
                    .last_segment_logical_length ||
            terminal_plan
                    .validated_last_ingress_sequence !=
                durable_cursor.ingress_sequence ||
            certificate.accepted_sealed_marker_sha256 !=
                ComputeAcceptedMarkerSha256(
                    terminal_plan
                        .accepted_marker_wire)) {
            return RecoveryMaintenanceReportV1Error::
                kFrontierMismatch;
        }

        RecoveryMaintenanceReportV1 report{};
        report.intent =
            RecoveryMaintenanceIntentV1::
                kRecoverSealOnly;
        report.result =
            RecoveryMaintenanceResultV1::kSealedRaw;
        report.namespace_identity = key_namespace;
        report.current_sealed_raw_certificate_sha256 =
            sealed_certificate.certificate_sha256();
        report.final_durable_cursor = final_cursor;
        report.journal_header_sha256 = journal_sha256;
        report.journal_tail = analysis.journal_tail;
        report.recovery_attempt_id =
            recovery_key.recovery_attempt_id;
        report.segment_tail = terminal_plan.tail;

        report.closed_frontier.closed_entry_count =
            certificate.closed_entry_count;
        report.closed_frontier.closed_prefix_sha256 =
            certificate.closed_prefix_sha256;
        report.closed_frontier
            .frontier_seal_marker_sha256 =
            certificate.accepted_sealed_marker_sha256;
        report.closed_frontier
            .frontier_segment_sequence =
            certificate.last_segment_sequence;
        report.closed_frontier
            .frontier_segment_sha256 =
            certificate.last_segment_sha256;

        report.recovery_range.analyzed_journal_size =
            analyzed_journal_size;
        report.recovery_range
            .initial_accepted_journal_size =
            analysis.accepted_journal_size;
        report.recovery_range.initial_durable_cursor =
            analysis_cursor;
        report.recovery_range.post_repair_journal_size =
            execution.retained_journal_size;
        report.recovery_range
            .final_durable_journal_size =
            execution.retained_journal_size;
        report.recovery_range.range_segment_sequence =
            terminal_plan.segment_sequence;
        report.recovery_range.raw_append_begin_offset =
            terminal_plan.append_only_begin_offset;
        report.recovery_range.raw_append_end_offset =
            terminal_plan.append_only_end_offset;
        report.recovery_range.raw_repair_begin_offset =
            terminal_plan.tail_begin_offset;
        report.recovery_range.raw_repair_end_offset =
            terminal_plan.tail_end_offset;

        report.sealed_boundary
            .accepted_sealed_marker_bytes =
            certificate.accepted_sealed_marker_bytes;
        report.sealed_boundary
            .accepted_sealed_marker_sha256 =
            certificate.accepted_sealed_marker_sha256;
        report.sealed_boundary.last_segment_base_wal_pos =
            certificate.last_segment_base_wal_pos;
        report.sealed_boundary.last_segment_flags =
            certificate.last_segment_flags;
        report.sealed_boundary
            .last_segment_logical_length =
            certificate.last_segment_logical_length;
        report.sealed_boundary.last_segment_sequence =
            certificate.last_segment_sequence;
        report.sealed_boundary.last_segment_sha256 =
            certificate.last_segment_sha256;

        RecoveryMaintenanceReportV1Error error =
            ValidateIntrinsic(report);
        if (error !=
            RecoveryMaintenanceReportV1Error::kNone) {
            return error;
        }
        std::string canonical_jcs;
        error = EncodeRecoveryMaintenanceReportV1Jcs(
            report, &canonical_jcs);
        if (error !=
            RecoveryMaintenanceReportV1Error::kNone) {
            return error;
        }
        std::string filename;
        error = RecoveryMaintenanceReportV1Filename(
            report, &filename);
        if (error !=
            RecoveryMaintenanceReportV1Error::kNone) {
            return error;
        }
        const RawV1Digest report_sha256 =
            l2flow::common::ComputeSha256(canonical_jcs);
        auto built = std::unique_ptr<
            BuiltRecoveryMaintenanceReportV1>(
            new BuiltRecoveryMaintenanceReportV1(
                std::move(report),
                std::move(canonical_jcs),
                std::move(filename),
                report_sha256,
                RawV1Identity{},
                RecoveryMaintenanceCursorV1{}));
        *output = std::move(built);
        return RecoveryMaintenanceReportV1Error::kNone;
    } catch (const std::bad_alloc&) {
        return RecoveryMaintenanceReportV1Error::
            kAllocationFailure;
    } catch (...) {
        return RecoveryMaintenanceReportV1Error::
            kAllocationFailure;
    }
}

RecoveryMaintenanceReportV1Error
BuildEmptyAnchorOnlyRecoveryMaintenanceReportV1(
    const RawReserveRegistryEntryKeyV1& recovery_key,
    const RawV1JournalHeaderWire& journal_header_bytes,
    const RawRecoveryPlanV1& analysis,
    const RawRecoveryExecutionResultV1& execution,
    std::uint64_t analyzed_journal_size,
    const BuiltEmptyAnchorTombstoneV1&
        empty_anchor_tombstone,
    std::unique_ptr<
        BuiltRecoveryMaintenanceReportV1>* output) noexcept {
    if (output == nullptr) {
        return RecoveryMaintenanceReportV1Error::
            kNullOutput;
    }
    try {
        if (!analysis.ok()) {
            return RecoveryMaintenanceReportV1Error::
                kRecoveryPlanFatal;
        }
        if (execution.failure !=
                RawRecoveryExecutionFailureV1::kNone ||
            execution.error_number != 0 ||
            execution.cursor_publishable ||
            execution.retained_journal_size !=
                kRawV1JournalHeaderBytes ||
            !IsZeroCursor(
                CursorFrom(execution.recovered_cursor)) ||
            execution.mutated !=
                (analysis.journal_tail !=
                 RawRecoveryJournalTailV1::kNone)) {
            return RecoveryMaintenanceReportV1Error::
                kRecoveryExecutionInvalid;
        }
        if (analysis.accepted_journal_size !=
                kRawV1JournalHeaderBytes ||
            analysis.has_accepted_cursor ||
            !IsZeroCursor(
                CursorFrom(analysis.accepted_cursor)) ||
            analysis.initial_anchor !=
                RawRecoveryInitialAnchorV1::kJournalOnly ||
            analysis.r11_orphan !=
                RawRecoveryR11OrphanV1::kNone ||
            !analysis.segments.empty()) {
            return RecoveryMaintenanceReportV1Error::
                kVariantMismatch;
        }
        if (!IsExactAnalyzedJournalSize(
                kRawV1JournalHeaderBytes,
                analyzed_journal_size,
                analysis.journal_tail)) {
            return RecoveryMaintenanceReportV1Error::
                kRangeInvalid;
        }

        DurableJournalHeaderV1 decoded_journal{};
        RawV1JournalHeaderWire reencoded_journal{};
        if (DecodeDurableJournalHeaderV1(
                journal_header_bytes,
                &decoded_journal) != RawV1Error::kNone ||
            decoded_journal.raw_schema_sha256 !=
                kFrozenRawSchemaSha256 ||
            EncodeDurableJournalHeaderV1(
                analysis.journal_header,
                &reencoded_journal) !=
                RawV1Error::kNone ||
            reencoded_journal != journal_header_bytes) {
            return RecoveryMaintenanceReportV1Error::
                kInvalidJournalHeader;
        }
        const RawManifestNamespaceV1 journal_namespace{
            decoded_journal.capture_date,
            decoded_journal.source_stream_id,
            decoded_journal.stream_day_id};
        const RawManifestNamespaceV1 key_namespace{
            recovery_key.route.capture_date,
            recovery_key.route.source_stream_id,
            recovery_key.stream_day_id};
        if (!SameNamespace(
                journal_namespace, key_namespace)) {
            return RecoveryMaintenanceReportV1Error::
                kInvalidNamespace;
        }
        if (l2flow::common::IsZeroIdentity(
                recovery_key.recovery_attempt_id)) {
            return RecoveryMaintenanceReportV1Error::
                kInvalidRecoveryAttempt;
        }

        const EmptyAnchorTombstoneV1& tombstone =
            empty_anchor_tombstone.model();
        const RawV1Digest journal_sha256 =
            l2flow::common::ComputeSha256(
                journal_header_bytes);
        if (ValidateEmptyAnchorTombstoneV1(tombstone) !=
                EmptyAnchorTombstoneV1Error::kNone ||
            IsZero(
                empty_anchor_tombstone
                    .tombstone_sha256()) ||
            l2flow::common::ComputeSha256(
                empty_anchor_tombstone.canonical_jcs()) !=
                empty_anchor_tombstone
                    .tombstone_sha256() ||
            tombstone.namespace_identity.capture_date !=
                key_namespace.capture_date ||
            tombstone.namespace_identity.source_stream_id !=
                key_namespace.source_stream_id ||
            tombstone.namespace_identity.stream_day_id !=
                key_namespace.stream_day_id ||
            tombstone.journal_header_sha256 !=
                journal_sha256 ||
            tombstone.marker_count != 0U ||
            tombstone.segment_count != 0U ||
            tombstone.record_count != 0U) {
            return RecoveryMaintenanceReportV1Error::
                kSidecarMismatch;
        }

        RecoveryMaintenanceReportV1 report{};
        report.intent =
            RecoveryMaintenanceIntentV1::
                kRecoverSealOnly;
        report.result =
            RecoveryMaintenanceResultV1::
                kEmptyAnchorOnly;
        report.namespace_identity = key_namespace;
        report.current_empty_anchor_tombstone_sha256 =
            empty_anchor_tombstone.tombstone_sha256();
        report.journal_header_sha256 = journal_sha256;
        report.journal_tail = analysis.journal_tail;
        report.marker_count = 0U;
        report.record_count = 0U;
        report.recovery_attempt_id =
            recovery_key.recovery_attempt_id;
        report.segment_count = 0U;
        report.segment_tail =
            RawRecoverySegmentTailV1::kNone;

        RecoveryMaintenanceReportV1Error error =
            ValidateIntrinsic(report);
        if (error !=
            RecoveryMaintenanceReportV1Error::kNone) {
            return error;
        }
        std::string canonical_jcs;
        error = EncodeRecoveryMaintenanceReportV1Jcs(
            report, &canonical_jcs);
        if (error !=
            RecoveryMaintenanceReportV1Error::kNone) {
            return error;
        }
        std::string filename;
        error = RecoveryMaintenanceReportV1Filename(
            report, &filename);
        if (error !=
            RecoveryMaintenanceReportV1Error::kNone) {
            return error;
        }
        const RawV1Digest report_sha256 =
            l2flow::common::ComputeSha256(canonical_jcs);
        auto built = std::unique_ptr<
            BuiltRecoveryMaintenanceReportV1>(
            new BuiltRecoveryMaintenanceReportV1(
                std::move(report),
                std::move(canonical_jcs),
                std::move(filename),
                report_sha256,
                RawV1Identity{},
                RecoveryMaintenanceCursorV1{}));
        *output = std::move(built);
        return RecoveryMaintenanceReportV1Error::kNone;
    } catch (const std::bad_alloc&) {
        return RecoveryMaintenanceReportV1Error::
            kAllocationFailure;
    } catch (...) {
        return RecoveryMaintenanceReportV1Error::
            kAllocationFailure;
    }
}

}  // namespace l2flow::ingress
