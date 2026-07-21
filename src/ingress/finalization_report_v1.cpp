#include "l2flow/ingress/finalization_report_v1.h"

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
#include <string>
#include <string_view>
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

[[nodiscard]] bool SameNamespace(
    const RawManifestNamespaceV1& left,
    const EmptyAnchorNamespaceV1& right) noexcept {
    return left.capture_date == right.capture_date &&
           left.source_stream_id == right.source_stream_id &&
           left.stream_day_id == right.stream_day_id;
}

[[nodiscard]] bool IsValidCursor(
    const FinalizationReportCursorV1& cursor) noexcept {
    return cursor.segment_sequence != 0U &&
           cursor.segment_offset >= kRawV1SegmentHeaderBytes;
}

[[nodiscard]] bool CursorNotAfter(
    const FinalizationReportCursorV1& left,
    const FinalizationReportCursorV1& right) noexcept {
    return left.global_wal_pos <= right.global_wal_pos &&
           left.ingress_sequence <= right.ingress_sequence;
}

[[nodiscard]] std::string_view AckName(
    ReserveAckStatusV1 value) noexcept {
    switch (value) {
    case ReserveAckStatusV1::kAcked:
        return "ACKED";
    case ReserveAckStatusV1::kFencedNoAck:
        return "FENCED_NO_ACK";
    case ReserveAckStatusV1::kUnused:
        break;
    }
    return {};
}

[[nodiscard]] std::string_view ResultName(
    FinalizationReportResultV1 value) noexcept {
    switch (value) {
    case FinalizationReportResultV1::kSealedRaw:
        return "SEALED_RAW";
    case FinalizationReportResultV1::kEmptyAnchorOnly:
        return "EMPTY_ANCHOR_ONLY";
    }
    return {};
}

[[nodiscard]] std::string_view TailName(
    FinalizationReportTailClassificationV1 value) noexcept {
    switch (value) {
    case FinalizationReportTailClassificationV1::kNone:
        return "NONE";
    case FinalizationReportTailClassificationV1::
        kPartialRecordDiscarded:
        return "PARTIAL_RECORD_DISCARDED";
    case FinalizationReportTailClassificationV1::
        kInvalidRecordDiscarded:
        return "INVALID_RECORD_DISCARDED";
    case FinalizationReportTailClassificationV1::kUnknown:
        return "UNKNOWN";
    }
    return {};
}

[[nodiscard]] std::string_view GapName(
    FinalizationReportGapClassificationV1 value) noexcept {
    switch (value) {
    case FinalizationReportGapClassificationV1::kNone:
        return "NONE";
    case FinalizationReportGapClassificationV1::
        kAckedRingDrained:
        return "ACKED_RING_DRAINED";
    case FinalizationReportGapClassificationV1::
        kFencedRingLostUnknown:
        return "FENCED_RING_LOST_UNKNOWN";
    case FinalizationReportGapClassificationV1::kUnknown:
        return "UNKNOWN";
    }
    return {};
}

[[nodiscard]] std::string_view SegmentKindName(
    FinalizationReportSegmentKindV1 value) noexcept {
    switch (value) {
    case FinalizationReportSegmentKindV1::kCurrent:
        return "CURRENT";
    case FinalizationReportSegmentKindV1::
        kFinalizationContinuation:
        return "FINALIZATION_CONTINUATION";
    }
    return {};
}

void AppendU32(std::string* output, std::uint32_t value) {
    std::array<char, 10U> bytes{};
    const auto result = std::to_chars(
        bytes.data(), bytes.data() + bytes.size(), value);
    if (result.ec != std::errc{}) {
        throw std::bad_alloc();
    }
    output->append(bytes.data(), result.ptr);
}

void AppendQuotedU64(
    std::string* output,
    std::uint64_t value) {
    std::array<char, 20U> bytes{};
    const auto result = std::to_chars(
        bytes.data(), bytes.data() + bytes.size(), value);
    if (result.ec != std::errc{}) {
        throw std::bad_alloc();
    }
    output->push_back('"');
    output->append(bytes.data(), result.ptr);
    output->push_back('"');
}

void AppendQuoted(
    std::string* output,
    std::string_view value) {
    output->push_back('"');
    output->append(value);
    output->push_back('"');
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
        output->push_back(kDigits[(octet >> 4U) & 0x0fU]);
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
    const FinalizationReportCursorV1& cursor) {
    output->append("{\"global_wal_pos\":");
    AppendQuotedU64(output, cursor.global_wal_pos);
    output->append(",\"ingress_sequence\":");
    AppendQuotedU64(output, cursor.ingress_sequence);
    output->append(",\"segment_offset\":");
    AppendQuotedU64(output, cursor.segment_offset);
    output->append(",\"segment_sequence\":");
    AppendU32(output, cursor.segment_sequence);
    output->push_back('}');
}

void AppendNullableCursor(
    std::string* output,
    const std::optional<FinalizationReportCursorV1>&
        cursor) {
    if (!cursor.has_value()) {
        output->append("null");
    } else {
        AppendCursor(output, *cursor);
    }
}

class Parser final {
public:
    explicit Parser(std::string_view input) noexcept
        : input_(input) {}

    [[nodiscard]] bool Consume(
        std::string_view token) noexcept {
        if (input_.substr(position_, token.size()) != token) {
            return false;
        }
        position_ += token.size();
        return true;
    }

    [[nodiscard]] bool ParseBool(bool* output) noexcept {
        if (output == nullptr) {
            return false;
        }
        if (Consume("true")) {
            *output = true;
            return true;
        }
        if (Consume("false")) {
            *output = false;
            return true;
        }
        return false;
    }

    [[nodiscard]] bool ParseU32(
        std::uint32_t* output) noexcept {
        if (output == nullptr || position_ >= input_.size() ||
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
        std::uint32_t value = 0U;
        const auto result = std::from_chars(
            input_.data() + begin,
            input_.data() + position_,
            value);
        if (result.ec != std::errc{} ||
            result.ptr != input_.data() + position_) {
            return false;
        }
        *output = value;
        return true;
    }

    [[nodiscard]] bool ParseQuotedU64(
        std::uint64_t* output) noexcept {
        if (output == nullptr || !Consume("\"") ||
            position_ >= input_.size() ||
            input_[position_] < '0' ||
            input_[position_] > '9') {
            return false;
        }
        const std::size_t begin = position_;
        if (input_[position_] == '0' &&
            position_ + 1U < input_.size() &&
            input_[position_ + 1U] != '"') {
            return false;
        }
        while (position_ < input_.size() &&
               input_[position_] >= '0' &&
               input_[position_] <= '9') {
            ++position_;
        }
        std::uint64_t value = 0U;
        const auto result = std::from_chars(
            input_.data() + begin,
            input_.data() + position_,
            value);
        if (result.ec != std::errc{} ||
            result.ptr != input_.data() + position_ ||
            !Consume("\"")) {
            return false;
        }
        *output = value;
        return true;
    }

    template <std::size_t Size>
    [[nodiscard]] bool ParseHex(
        std::array<std::byte, Size>* output) noexcept {
        if (output == nullptr || !Consume("\"") ||
            input_.size() - position_ < (Size * 2U + 1U)) {
            return false;
        }
        std::array<std::byte, Size> value{};
        for (std::size_t index = 0U; index < Size; ++index) {
            const int high =
                HexNibble(input_[position_ + index * 2U]);
            const int low =
                HexNibble(input_[position_ + index * 2U + 1U]);
            if (high < 0 || low < 0) {
                return false;
            }
            value[index] = static_cast<std::byte>(
                static_cast<unsigned>(high * 16 + low));
        }
        position_ += Size * 2U;
        if (!Consume("\"")) {
            return false;
        }
        *output = value;
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

    [[nodiscard]] bool ParseQuoted(
        std::string_view* value) noexcept {
        if (value == nullptr || !Consume("\"")) {
            return false;
        }
        const std::size_t begin = position_;
        while (position_ < input_.size() &&
               input_[position_] != '"') {
            const unsigned char byte =
                static_cast<unsigned char>(input_[position_]);
            if (byte < 0x20U || input_[position_] == '\\') {
                return false;
            }
            ++position_;
        }
        if (position_ >= input_.size()) {
            return false;
        }
        *value = input_.substr(begin, position_ - begin);
        ++position_;
        return true;
    }

    [[nodiscard]] bool done() const noexcept {
        return position_ == input_.size();
    }

private:
    [[nodiscard]] static int HexNibble(char value) noexcept {
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

[[nodiscard]] bool ParseNullableCursor(
    Parser* parser,
    std::optional<FinalizationReportCursorV1>*
        output) noexcept {
    if (parser == nullptr || output == nullptr) {
        return false;
    }
    if (parser->Consume("null")) {
        output->reset();
        return true;
    }
    FinalizationReportCursorV1 value{};
    if (!parser->Consume("{\"global_wal_pos\":") ||
        !parser->ParseQuotedU64(&value.global_wal_pos) ||
        !parser->Consume(",\"ingress_sequence\":") ||
        !parser->ParseQuotedU64(&value.ingress_sequence) ||
        !parser->Consume(",\"segment_offset\":") ||
        !parser->ParseQuotedU64(&value.segment_offset) ||
        !parser->Consume(",\"segment_sequence\":") ||
        !parser->ParseU32(&value.segment_sequence) ||
        !parser->Consume("}")) {
        return false;
    }
    *output = value;
    return true;
}

[[nodiscard]] bool ParseAck(
    Parser* parser,
    ReserveAckStatusV1* output) noexcept {
    std::string_view text;
    if (parser == nullptr || output == nullptr ||
        !parser->ParseQuoted(&text)) {
        return false;
    }
    if (text == "ACKED") {
        *output = ReserveAckStatusV1::kAcked;
        return true;
    }
    if (text == "FENCED_NO_ACK") {
        *output = ReserveAckStatusV1::kFencedNoAck;
        return true;
    }
    return false;
}

[[nodiscard]] bool ParseResult(
    Parser* parser,
    FinalizationReportResultV1* output) noexcept {
    std::string_view text;
    if (parser == nullptr || output == nullptr ||
        !parser->ParseQuoted(&text)) {
        return false;
    }
    if (text == "SEALED_RAW") {
        *output = FinalizationReportResultV1::kSealedRaw;
        return true;
    }
    if (text == "EMPTY_ANCHOR_ONLY") {
        *output =
            FinalizationReportResultV1::kEmptyAnchorOnly;
        return true;
    }
    return false;
}

[[nodiscard]] bool ParseTail(
    Parser* parser,
    FinalizationReportTailClassificationV1*
        output) noexcept {
    std::string_view text;
    if (parser == nullptr || output == nullptr ||
        !parser->ParseQuoted(&text)) {
        return false;
    }
    if (text == "NONE") {
        *output = FinalizationReportTailClassificationV1::kNone;
    } else if (text == "PARTIAL_RECORD_DISCARDED") {
        *output = FinalizationReportTailClassificationV1::
            kPartialRecordDiscarded;
    } else if (text == "INVALID_RECORD_DISCARDED") {
        *output = FinalizationReportTailClassificationV1::
            kInvalidRecordDiscarded;
    } else if (text == "UNKNOWN") {
        *output =
            FinalizationReportTailClassificationV1::kUnknown;
    } else {
        return false;
    }
    return true;
}

[[nodiscard]] bool ParseGap(
    Parser* parser,
    FinalizationReportGapClassificationV1*
        output) noexcept {
    std::string_view text;
    if (parser == nullptr || output == nullptr ||
        !parser->ParseQuoted(&text)) {
        return false;
    }
    if (text == "NONE") {
        *output = FinalizationReportGapClassificationV1::kNone;
    } else if (text == "ACKED_RING_DRAINED") {
        *output = FinalizationReportGapClassificationV1::
            kAckedRingDrained;
    } else if (text == "FENCED_RING_LOST_UNKNOWN") {
        *output = FinalizationReportGapClassificationV1::
            kFencedRingLostUnknown;
    } else if (text == "UNKNOWN") {
        *output =
            FinalizationReportGapClassificationV1::kUnknown;
    } else {
        return false;
    }
    return true;
}

[[nodiscard]] bool ParseSegmentKind(
    Parser* parser,
    FinalizationReportSegmentKindV1* output) noexcept {
    std::string_view text;
    if (parser == nullptr || output == nullptr ||
        !parser->ParseQuoted(&text)) {
        return false;
    }
    if (text == "CURRENT") {
        *output = FinalizationReportSegmentKindV1::kCurrent;
        return true;
    }
    if (text == "FINALIZATION_CONTINUATION") {
        *output = FinalizationReportSegmentKindV1::
            kFinalizationContinuation;
        return true;
    }
    return false;
}

[[nodiscard]] bool ParseNullableSeal(
    Parser* parser,
    std::optional<FinalizationReportSealV1>*
        output) noexcept {
    if (parser == nullptr || output == nullptr) {
        return false;
    }
    if (parser->Consume("null")) {
        output->reset();
        return true;
    }
    FinalizationReportSealV1 value{};
    if (!parser->Consume(
            "{\"accepted_sealed_marker_bytes\":") ||
        !parser->ParseHex(
            &value.accepted_sealed_marker_bytes) ||
        !parser->Consume(
            ",\"accepted_sealed_marker_sha256\":") ||
        !parser->ParseHex(
            &value.accepted_sealed_marker_sha256) ||
        !parser->Consume(",\"segment_sequence\":") ||
        !parser->ParseU32(&value.segment_sequence) ||
        !parser->Consume("}")) {
        return false;
    }
    *output = value;
    return true;
}

[[nodiscard]] bool ParseNullableFrontier(
    Parser* parser,
    std::optional<
        FinalizationReportManifestFrontierV1>*
        output) noexcept {
    if (parser == nullptr || output == nullptr) {
        return false;
    }
    if (parser->Consume("null")) {
        output->reset();
        return true;
    }
    FinalizationReportManifestFrontierV1 value{};
    if (!parser->Consume("{\"closed_entry_count\":") ||
        !parser->ParseQuotedU64(
            &value.closed_entry_count) ||
        !parser->Consume(",\"closed_prefix_sha256\":") ||
        !parser->ParseHex(&value.closed_prefix_sha256) ||
        !parser->Consume(
            ",\"frontier_accepted_seal_sha256\":") ||
        !parser->ParseHex(
            &value.frontier_accepted_seal_sha256) ||
        !parser->Consume(
            ",\"frontier_segment_sequence\":") ||
        !parser->ParseU32(
            &value.frontier_segment_sequence) ||
        !parser->Consume(
            ",\"frontier_segment_sha256\":") ||
        !parser->ParseHex(
            &value.frontier_segment_sha256) ||
        !parser->Consume("}")) {
        return false;
    }
    *output = value;
    return true;
}

[[nodiscard]] bool ParseNullableU64(
    Parser* parser,
    std::optional<std::uint64_t>* output) noexcept {
    if (parser == nullptr || output == nullptr) {
        return false;
    }
    if (parser->Consume("null")) {
        output->reset();
        return true;
    }
    std::uint64_t value = 0U;
    if (!parser->ParseQuotedU64(&value)) {
        return false;
    }
    *output = value;
    return true;
}

[[nodiscard]] bool ParsePreexisting(
    Parser* parser,
    std::optional<
        FinalizationPreexistingRecoveryReportV1>*
        output) noexcept {
    if (parser == nullptr || output == nullptr) {
        return false;
    }
    if (parser->Consume("null")) {
        output->reset();
        return true;
    }
    FinalizationPreexistingRecoveryReportV1 value{};
    if (!parser->Consume("{\"recovery_attempt_id\":") ||
        !parser->ParseHex(&value.recovery_attempt_id) ||
        !parser->Consume(",\"report_sha256\":") ||
        !parser->ParseHex(&value.report_sha256) ||
        !parser->Consume("}")) {
        return false;
    }
    *output = value;
    return true;
}

[[nodiscard]] FinalizationReportV1Error ValidateSealedShape(
    const FinalizationReportV1& report) noexcept {
    if (!report.final_append_cursor.has_value() ||
        !report.final_durable_cursor.has_value() ||
        *report.final_append_cursor !=
            *report.final_durable_cursor ||
        !IsValidCursor(*report.final_append_cursor)) {
        return FinalizationReportV1Error::kInvalidCursor;
    }
    if (report.ack_status == ReserveAckStatusV1::kAcked &&
        (!report.initial_append_cursor.has_value() ||
         !report.initial_durable_cursor.has_value())) {
        return FinalizationReportV1Error::kInvalidCursor;
    }
    if ((report.initial_append_cursor.has_value() &&
         (!IsValidCursor(*report.initial_append_cursor) ||
          !CursorNotAfter(
              *report.initial_append_cursor,
              *report.final_append_cursor))) ||
        (report.initial_durable_cursor.has_value() &&
         (!IsValidCursor(*report.initial_durable_cursor) ||
          !CursorNotAfter(
              *report.initial_durable_cursor,
              *report.final_durable_cursor)))) {
        return FinalizationReportV1Error::kInvalidCursor;
    }
    if (report.segments.empty() ||
        report.segments.size() >
            kFinalizationReportV1MaximumSegments ||
        !report.final_seal.has_value() ||
        !report.manifest_frontier.has_value() ||
        IsZero(report.sealed_raw_certificate_sha256) ||
        !IsZero(report.journal_header_sha256) ||
        report.marker_count.has_value() ||
        !IsZero(report.empty_anchor_tombstone_sha256)) {
        return FinalizationReportV1Error::kInvalidResultShape;
    }

    for (std::size_t index = 0U;
         index < report.segments.size();
         ++index) {
        const FinalizationReportSegmentV1& segment =
            report.segments[index];
        const auto expected_kind =
            index == 0U
                ? FinalizationReportSegmentKindV1::kCurrent
                : FinalizationReportSegmentKindV1::
                      kFinalizationContinuation;
        if (segment.kind != expected_kind ||
            segment.segment_sequence == 0U ||
            segment.logical_length <
                kRawV1SegmentHeaderBytes ||
            IsZero(segment.segment_sha256)) {
            return FinalizationReportV1Error::kInvalidSegment;
        }
        if (index != 0U) {
            const FinalizationReportSegmentV1& previous =
                report.segments[index - 1U];
            if (previous.segment_sequence ==
                    std::numeric_limits<std::uint32_t>::max() ||
                segment.segment_sequence !=
                    previous.segment_sequence + 1U ||
                previous.logical_length >
                    std::numeric_limits<std::uint64_t>::max() -
                        previous.segment_base_wal_pos ||
                segment.segment_base_wal_pos !=
                    previous.segment_base_wal_pos +
                        previous.logical_length) {
                return FinalizationReportV1Error::kInvalidSegment;
            }
        }
    }
    const FinalizationReportSegmentV1& terminal =
        report.segments.back();
    if (terminal.logical_length >
            std::numeric_limits<std::uint64_t>::max() -
                terminal.segment_base_wal_pos ||
        report.final_append_cursor->segment_sequence !=
            terminal.segment_sequence ||
        report.final_append_cursor->segment_offset !=
            terminal.logical_length ||
        report.final_append_cursor->global_wal_pos !=
            terminal.segment_base_wal_pos +
                terminal.logical_length) {
        return FinalizationReportV1Error::kInvalidSegment;
    }

    const FinalizationReportSealV1& seal =
        *report.final_seal;
    DurableMarkerV1 marker{};
    if (seal.segment_sequence != terminal.segment_sequence ||
        common::ComputeSha256(
            seal.accepted_sealed_marker_bytes) !=
            seal.accepted_sealed_marker_sha256 ||
        DecodeDurableMarkerV1(
            seal.accepted_sealed_marker_bytes,
            &marker) != RawV1Error::kNone ||
        marker.source_stream_id !=
            report.namespace_identity.source_stream_id ||
        marker.segment_sequence != terminal.segment_sequence ||
        marker.marker_flags != kRawV1SegmentSealed ||
        marker.durable_global_wal_pos !=
            report.final_durable_cursor->global_wal_pos ||
        marker.durable_ingress_sequence !=
            report.final_durable_cursor->ingress_sequence ||
        marker.durable_segment_offset !=
            report.final_durable_cursor->segment_offset) {
        return FinalizationReportV1Error::kInvalidSeal;
    }

    const FinalizationReportManifestFrontierV1& frontier =
        *report.manifest_frontier;
    if (frontier.closed_entry_count == 0U ||
        frontier.frontier_segment_sequence !=
            terminal.segment_sequence ||
        frontier.frontier_segment_sha256 !=
            terminal.segment_sha256 ||
        frontier.frontier_accepted_seal_sha256 !=
            seal.accepted_sealed_marker_sha256 ||
        IsZero(frontier.closed_prefix_sha256)) {
        return FinalizationReportV1Error::kInvalidFrontier;
    }
    return FinalizationReportV1Error::kNone;
}

[[nodiscard]] FinalizationReportV1Error ValidateEmptyShape(
    const FinalizationReportV1& report) noexcept {
    if (report.initial_append_cursor.has_value() ||
        report.initial_durable_cursor.has_value() ||
        report.final_append_cursor.has_value() ||
        report.final_durable_cursor.has_value() ||
        !report.segments.empty() ||
        report.final_seal.has_value() ||
        report.manifest_frontier.has_value() ||
        !IsZero(report.sealed_raw_certificate_sha256) ||
        IsZero(report.journal_header_sha256) ||
        !report.marker_count.has_value() ||
        *report.marker_count != 0U ||
        IsZero(report.empty_anchor_tombstone_sha256) ||
        report.tail_classification !=
            FinalizationReportTailClassificationV1::kNone ||
        !report.tail_classification_valid ||
        report.gap_classification !=
            FinalizationReportGapClassificationV1::kNone ||
        !report.gap_classification_valid) {
        return FinalizationReportV1Error::kInvalidResultShape;
    }
    return FinalizationReportV1Error::kNone;
}

}  // namespace

std::string_view FinalizationReportV1ErrorName(
    FinalizationReportV1Error error) noexcept {
    switch (error) {
    case FinalizationReportV1Error::kNone:
        return "none";
    case FinalizationReportV1Error::kNullOutput:
        return "null_output";
    case FinalizationReportV1Error::kInvalidArgument:
        return "invalid_argument";
    case FinalizationReportV1Error::kInvalidNamespace:
        return "invalid_namespace";
    case FinalizationReportV1Error::kInvalidIdentity:
        return "invalid_identity";
    case FinalizationReportV1Error::kInvalidAck:
        return "invalid_ack";
    case FinalizationReportV1Error::kInvalidClassification:
        return "invalid_classification";
    case FinalizationReportV1Error::kInvalidCursor:
        return "invalid_cursor";
    case FinalizationReportV1Error::kInvalidSegment:
        return "invalid_segment";
    case FinalizationReportV1Error::kInvalidSeal:
        return "invalid_seal";
    case FinalizationReportV1Error::kInvalidFrontier:
        return "invalid_frontier";
    case FinalizationReportV1Error::kInvalidResultShape:
        return "invalid_result_shape";
    case FinalizationReportV1Error::kGrantKindMismatch:
        return "grant_kind_mismatch";
    case FinalizationReportV1Error::kSidecarMismatch:
        return "sidecar_mismatch";
    case FinalizationReportV1Error::kInvalidPreexistingReport:
        return "invalid_preexisting_report";
    case FinalizationReportV1Error::kInvalidCanonicalJson:
        return "invalid_canonical_json";
    case FinalizationReportV1Error::kFilenameInvalid:
        return "filename_invalid";
    case FinalizationReportV1Error::kEncodedSizeExceeded:
        return "encoded_size_exceeded";
    case FinalizationReportV1Error::kAllocationFailure:
        return "allocation_failure";
    }
    return "unknown";
}

FinalizationReportV1Error ValidateFinalizationReportV1(
    const FinalizationReportV1& report) noexcept {
    if (report.schema_version !=
        kFinalizationReportV1SchemaVersion) {
        return FinalizationReportV1Error::kInvalidArgument;
    }
    if (report.namespace_identity.capture_date == 0U ||
        report.namespace_identity.source_stream_id == 0U ||
        common::IsZeroIdentity(
            report.namespace_identity.stream_day_id)) {
        return FinalizationReportV1Error::kInvalidNamespace;
    }
    if (common::IsZeroIdentity(report.reserve_state_uuid) ||
        common::IsZeroIdentity(
            report.finalization_cycle_id) ||
        IsZero(report.immutable_grant_sha256)) {
        return FinalizationReportV1Error::kInvalidIdentity;
    }
    if (AckName(report.ack_status).empty() ||
        (report.ack_status == ReserveAckStatusV1::kAcked &&
         common::IsZeroIdentity(
             report.ack_writer_instance)) ||
        (report.ack_status ==
             ReserveAckStatusV1::kFencedNoAck &&
         !common::IsZeroIdentity(
             report.ack_writer_instance))) {
        return FinalizationReportV1Error::kInvalidAck;
    }
    if (TailName(report.tail_classification).empty() ||
        GapName(report.gap_classification).empty() ||
        (report.tail_classification_valid ==
         (report.tail_classification ==
          FinalizationReportTailClassificationV1::kUnknown)) ||
        (report.gap_classification_valid ==
         (report.gap_classification ==
          FinalizationReportGapClassificationV1::kUnknown)) ||
        (report.ack_status == ReserveAckStatusV1::kAcked &&
         report.gap_classification ==
             FinalizationReportGapClassificationV1::
                 kFencedRingLostUnknown) ||
        (report.ack_status ==
             ReserveAckStatusV1::kFencedNoAck &&
         report.gap_classification ==
             FinalizationReportGapClassificationV1::
                 kAckedRingDrained)) {
        return FinalizationReportV1Error::
            kInvalidClassification;
    }
    if (report.preexisting_recovery_report.has_value() &&
        (common::IsZeroIdentity(
             report.preexisting_recovery_report
                 ->recovery_attempt_id) ||
         IsZero(
             report.preexisting_recovery_report
                 ->report_sha256))) {
        return FinalizationReportV1Error::
            kInvalidPreexistingReport;
    }

    switch (report.result) {
    case FinalizationReportResultV1::kSealedRaw:
        return ValidateSealedShape(report);
    case FinalizationReportResultV1::kEmptyAnchorOnly:
        return ValidateEmptyShape(report);
    }
    return FinalizationReportV1Error::kInvalidResultShape;
}

FinalizationReportV1Error EncodeFinalizationReportV1Jcs(
    const FinalizationReportV1& report,
    std::string* output) noexcept {
    if (output == nullptr) {
        return FinalizationReportV1Error::kNullOutput;
    }
    const FinalizationReportV1Error validation =
        ValidateFinalizationReportV1(report);
    if (validation != FinalizationReportV1Error::kNone) {
        return validation;
    }
    try {
        std::string encoded;
        encoded.reserve(4096U);
        encoded.append("{\"ack_status\":");
        AppendQuoted(&encoded, AckName(report.ack_status));
        encoded.append(",\"ack_writer_instance\":");
        AppendNullableHex(
            &encoded, report.ack_writer_instance);
        encoded.append(",\"capture_date\":");
        AppendU32(
            &encoded, report.namespace_identity.capture_date);
        encoded.append(
            ",\"empty_anchor_tombstone_sha256\":");
        AppendNullableHex(
            &encoded,
            report.empty_anchor_tombstone_sha256);
        encoded.append(",\"final_append_cursor\":");
        AppendNullableCursor(
            &encoded, report.final_append_cursor);
        encoded.append(",\"final_durable_cursor\":");
        AppendNullableCursor(
            &encoded, report.final_durable_cursor);
        encoded.append(",\"final_seal\":");
        if (!report.final_seal.has_value()) {
            encoded.append("null");
        } else {
            encoded.append(
                "{\"accepted_sealed_marker_bytes\":");
            AppendHex(
                &encoded,
                report.final_seal
                    ->accepted_sealed_marker_bytes);
            encoded.append(
                ",\"accepted_sealed_marker_sha256\":");
            AppendHex(
                &encoded,
                report.final_seal
                    ->accepted_sealed_marker_sha256);
            encoded.append(",\"segment_sequence\":");
            AppendU32(
                &encoded,
                report.final_seal->segment_sequence);
            encoded.push_back('}');
        }
        encoded.append(",\"finalization_cycle_id\":");
        AppendHex(
            &encoded, report.finalization_cycle_id);
        encoded.append(",\"gap_classification\":");
        AppendQuoted(
            &encoded, GapName(report.gap_classification));
        encoded.append(
            ",\"gap_classification_valid\":");
        encoded.append(
            report.gap_classification_valid
                ? "true"
                : "false");
        encoded.append(",\"immutable_grant_sha256\":");
        AppendHex(
            &encoded, report.immutable_grant_sha256);
        encoded.append(",\"initial_append_cursor\":");
        AppendNullableCursor(
            &encoded, report.initial_append_cursor);
        encoded.append(",\"initial_durable_cursor\":");
        AppendNullableCursor(
            &encoded, report.initial_durable_cursor);
        encoded.append(",\"journal_header_sha256\":");
        AppendNullableHex(
            &encoded, report.journal_header_sha256);
        encoded.append(",\"manifest_frontier\":");
        if (!report.manifest_frontier.has_value()) {
            encoded.append("null");
        } else {
            encoded.append("{\"closed_entry_count\":");
            AppendQuotedU64(
                &encoded,
                report.manifest_frontier
                    ->closed_entry_count);
            encoded.append(
                ",\"closed_prefix_sha256\":");
            AppendHex(
                &encoded,
                report.manifest_frontier
                    ->closed_prefix_sha256);
            encoded.append(
                ",\"frontier_accepted_seal_sha256\":");
            AppendHex(
                &encoded,
                report.manifest_frontier
                    ->frontier_accepted_seal_sha256);
            encoded.append(
                ",\"frontier_segment_sequence\":");
            AppendU32(
                &encoded,
                report.manifest_frontier
                    ->frontier_segment_sequence);
            encoded.append(
                ",\"frontier_segment_sha256\":");
            AppendHex(
                &encoded,
                report.manifest_frontier
                    ->frontier_segment_sha256);
            encoded.push_back('}');
        }
        encoded.append(",\"marker_count\":");
        if (!report.marker_count.has_value()) {
            encoded.append("null");
        } else {
            AppendQuotedU64(&encoded, *report.marker_count);
        }
        encoded.append(
            ",\"preexisting_recovery_report\":");
        if (!report.preexisting_recovery_report.has_value()) {
            encoded.append("null");
        } else {
            encoded.append("{\"recovery_attempt_id\":");
            AppendHex(
                &encoded,
                report.preexisting_recovery_report
                    ->recovery_attempt_id);
            encoded.append(",\"report_sha256\":");
            AppendHex(
                &encoded,
                report.preexisting_recovery_report
                    ->report_sha256);
            encoded.push_back('}');
        }
        encoded.append(
            ",\"raw_allocation_delta_before_report\":"
            "{\"allocated_bytes\":");
        AppendQuotedU64(
            &encoded,
            report.raw_allocation_delta_before_report
                .allocated_bytes);
        encoded.append(",\"allocated_inodes\":");
        AppendQuotedU64(
            &encoded,
            report.raw_allocation_delta_before_report
                .allocated_inodes);
        encoded.append(",\"released_bytes\":");
        AppendQuotedU64(
            &encoded,
            report.raw_allocation_delta_before_report
                .released_bytes);
        encoded.append(",\"released_inodes\":");
        AppendQuotedU64(
            &encoded,
            report.raw_allocation_delta_before_report
                .released_inodes);
        encoded.append("},\"reserve_state_uuid\":");
        AppendHex(&encoded, report.reserve_state_uuid);
        encoded.append(",\"result\":");
        AppendQuoted(&encoded, ResultName(report.result));
        encoded.append(",\"schema_version\":");
        AppendU32(&encoded, report.schema_version);
        encoded.append(
            ",\"sealed_raw_certificate_sha256\":");
        AppendNullableHex(
            &encoded,
            report.sealed_raw_certificate_sha256);
        encoded.append(",\"segments\":");
        if (report.result ==
            FinalizationReportResultV1::
                kEmptyAnchorOnly) {
            encoded.append("null");
        } else {
            encoded.push_back('[');
            for (std::size_t index = 0U;
                 index < report.segments.size();
                 ++index) {
                if (index != 0U) {
                    encoded.push_back(',');
                }
                const auto& segment =
                    report.segments[index];
                encoded.append("{\"kind\":");
                AppendQuoted(
                    &encoded,
                    SegmentKindName(segment.kind));
                encoded.append(",\"logical_length\":");
                AppendQuotedU64(
                    &encoded, segment.logical_length);
                encoded.append(
                    ",\"segment_base_wal_pos\":");
                AppendQuotedU64(
                    &encoded,
                    segment.segment_base_wal_pos);
                encoded.append(
                    ",\"segment_sequence\":");
                AppendU32(
                    &encoded,
                    segment.segment_sequence);
                encoded.append(",\"segment_sha256\":");
                AppendHex(
                    &encoded, segment.segment_sha256);
                encoded.push_back('}');
            }
            encoded.push_back(']');
        }
        encoded.append(",\"source_stream_id\":");
        AppendU32(
            &encoded,
            report.namespace_identity.source_stream_id);
        encoded.append(",\"stream_day_id\":");
        AppendHex(
            &encoded,
            report.namespace_identity.stream_day_id);
        encoded.append(",\"tail_classification\":");
        AppendQuoted(
            &encoded,
            TailName(report.tail_classification));
        encoded.append(
            ",\"tail_classification_valid\":");
        encoded.append(
            report.tail_classification_valid
                ? "true}"
                : "false}");
        if (encoded.size() >
            kFinalizationReportV1MaximumBytes) {
            return FinalizationReportV1Error::
                kEncodedSizeExceeded;
        }
        output->swap(encoded);
        return FinalizationReportV1Error::kNone;
    } catch (const std::bad_alloc&) {
        return FinalizationReportV1Error::kAllocationFailure;
    }
}

FinalizationReportV1Error ParseFinalizationReportV1Jcs(
    std::string_view exact_bytes,
    FinalizationReportV1* output) noexcept {
    if (output == nullptr) {
        return FinalizationReportV1Error::kNullOutput;
    }
    if (exact_bytes.empty() ||
        exact_bytes.size() >
            kFinalizationReportV1MaximumBytes) {
        return exact_bytes.size() >
                       kFinalizationReportV1MaximumBytes
                   ? FinalizationReportV1Error::
                         kEncodedSizeExceeded
                   : FinalizationReportV1Error::
                         kInvalidCanonicalJson;
    }
    try {
        Parser parser(exact_bytes);
        FinalizationReportV1 value{};
        if (!parser.Consume("{\"ack_status\":") ||
            !ParseAck(&parser, &value.ack_status) ||
            !parser.Consume(",\"ack_writer_instance\":") ||
            !parser.ParseNullableHex(
                &value.ack_writer_instance) ||
            !parser.Consume(",\"capture_date\":") ||
            !parser.ParseU32(
                &value.namespace_identity.capture_date) ||
            !parser.Consume(
                ",\"empty_anchor_tombstone_sha256\":") ||
            !parser.ParseNullableHex(
                &value.empty_anchor_tombstone_sha256) ||
            !parser.Consume(
                ",\"final_append_cursor\":") ||
            !ParseNullableCursor(
                &parser, &value.final_append_cursor) ||
            !parser.Consume(
                ",\"final_durable_cursor\":") ||
            !ParseNullableCursor(
                &parser, &value.final_durable_cursor) ||
            !parser.Consume(",\"final_seal\":") ||
            !ParseNullableSeal(
                &parser, &value.final_seal) ||
            !parser.Consume(
                ",\"finalization_cycle_id\":") ||
            !parser.ParseHex(
                &value.finalization_cycle_id) ||
            !parser.Consume(",\"gap_classification\":") ||
            !ParseGap(
                &parser, &value.gap_classification) ||
            !parser.Consume(
                ",\"gap_classification_valid\":") ||
            !parser.ParseBool(
                &value.gap_classification_valid) ||
            !parser.Consume(
                ",\"immutable_grant_sha256\":") ||
            !parser.ParseHex(
                &value.immutable_grant_sha256) ||
            !parser.Consume(
                ",\"initial_append_cursor\":") ||
            !ParseNullableCursor(
                &parser, &value.initial_append_cursor) ||
            !parser.Consume(
                ",\"initial_durable_cursor\":") ||
            !ParseNullableCursor(
                &parser, &value.initial_durable_cursor) ||
            !parser.Consume(
                ",\"journal_header_sha256\":") ||
            !parser.ParseNullableHex(
                &value.journal_header_sha256) ||
            !parser.Consume(",\"manifest_frontier\":") ||
            !ParseNullableFrontier(
                &parser, &value.manifest_frontier) ||
            !parser.Consume(",\"marker_count\":") ||
            !ParseNullableU64(
                &parser, &value.marker_count) ||
            !parser.Consume(
                ",\"preexisting_recovery_report\":") ||
            !ParsePreexisting(
                &parser,
                &value.preexisting_recovery_report) ||
            !parser.Consume(
                ",\"raw_allocation_delta_before_report\":"
                "{\"allocated_bytes\":") ||
            !parser.ParseQuotedU64(
                &value.raw_allocation_delta_before_report
                     .allocated_bytes) ||
            !parser.Consume(",\"allocated_inodes\":") ||
            !parser.ParseQuotedU64(
                &value.raw_allocation_delta_before_report
                     .allocated_inodes) ||
            !parser.Consume(",\"released_bytes\":") ||
            !parser.ParseQuotedU64(
                &value.raw_allocation_delta_before_report
                     .released_bytes) ||
            !parser.Consume(",\"released_inodes\":") ||
            !parser.ParseQuotedU64(
                &value.raw_allocation_delta_before_report
                     .released_inodes) ||
            !parser.Consume("},\"reserve_state_uuid\":") ||
            !parser.ParseHex(&value.reserve_state_uuid) ||
            !parser.Consume(",\"result\":") ||
            !ParseResult(&parser, &value.result) ||
            !parser.Consume(",\"schema_version\":") ||
            !parser.ParseU32(&value.schema_version) ||
            !parser.Consume(
                ",\"sealed_raw_certificate_sha256\":") ||
            !parser.ParseNullableHex(
                &value.sealed_raw_certificate_sha256) ||
            !parser.Consume(",\"segments\":")) {
            return FinalizationReportV1Error::
                kInvalidCanonicalJson;
        }
        if (parser.Consume("null")) {
            value.segments.clear();
        } else {
            if (!parser.Consume("[")) {
                return FinalizationReportV1Error::
                    kInvalidCanonicalJson;
            }
            if (!parser.Consume("]")) {
                for (;;) {
                    if (value.segments.size() >=
                        kFinalizationReportV1MaximumSegments) {
                        return FinalizationReportV1Error::
                            kInvalidCanonicalJson;
                    }
                    FinalizationReportSegmentV1 segment{};
                    if (!parser.Consume("{\"kind\":") ||
                        !ParseSegmentKind(
                            &parser, &segment.kind) ||
                        !parser.Consume(
                            ",\"logical_length\":") ||
                        !parser.ParseQuotedU64(
                            &segment.logical_length) ||
                        !parser.Consume(
                            ",\"segment_base_wal_pos\":") ||
                        !parser.ParseQuotedU64(
                            &segment.segment_base_wal_pos) ||
                        !parser.Consume(
                            ",\"segment_sequence\":") ||
                        !parser.ParseU32(
                            &segment.segment_sequence) ||
                        !parser.Consume(
                            ",\"segment_sha256\":") ||
                        !parser.ParseHex(
                            &segment.segment_sha256) ||
                        !parser.Consume("}")) {
                        return FinalizationReportV1Error::
                            kInvalidCanonicalJson;
                    }
                    value.segments.push_back(segment);
                    if (parser.Consume("]")) {
                        break;
                    }
                    if (!parser.Consume(",")) {
                        return FinalizationReportV1Error::
                            kInvalidCanonicalJson;
                    }
                }
            }
        }
        if (!parser.Consume(",\"source_stream_id\":") ||
            !parser.ParseU32(
                &value.namespace_identity.source_stream_id) ||
            !parser.Consume(",\"stream_day_id\":") ||
            !parser.ParseHex(
                &value.namespace_identity.stream_day_id) ||
            !parser.Consume(",\"tail_classification\":") ||
            !ParseTail(
                &parser, &value.tail_classification) ||
            !parser.Consume(
                ",\"tail_classification_valid\":") ||
            !parser.ParseBool(
                &value.tail_classification_valid) ||
            !parser.Consume("}") || !parser.done()) {
            return FinalizationReportV1Error::
                kInvalidCanonicalJson;
        }

        const FinalizationReportV1Error validation =
            ValidateFinalizationReportV1(value);
        if (validation != FinalizationReportV1Error::kNone) {
            return validation;
        }
        std::string canonical;
        const FinalizationReportV1Error encoded =
            EncodeFinalizationReportV1Jcs(value, &canonical);
        if (encoded != FinalizationReportV1Error::kNone) {
            return encoded;
        }
        if (canonical != exact_bytes) {
            return FinalizationReportV1Error::
                kInvalidCanonicalJson;
        }
        *output = std::move(value);
        return FinalizationReportV1Error::kNone;
    } catch (const std::bad_alloc&) {
        return FinalizationReportV1Error::kAllocationFailure;
    }
}

FinalizationReportV1Error FinalizationReportV1Filename(
    const FinalizationReportV1& report,
    std::string* output) noexcept {
    if (output == nullptr) {
        return FinalizationReportV1Error::kNullOutput;
    }
    if (common::IsZeroIdentity(
            report.finalization_cycle_id)) {
        return FinalizationReportV1Error::kFilenameInvalid;
    }
    try {
        std::string filename("finalization-");
        filename.append(
            common::Identity128Hex(
                report.finalization_cycle_id));
        filename.append(".json");
        if (filename.size() != 50U) {
            return FinalizationReportV1Error::kFilenameInvalid;
        }
        output->swap(filename);
        return FinalizationReportV1Error::kNone;
    } catch (const std::bad_alloc&) {
        return FinalizationReportV1Error::kAllocationFailure;
    }
}

FinalizationReportV1Error
BuildFinalizationReportCapabilityV1(
    const FinalizationReportV1& report,
    std::uint8_t immutable_grant_flags,
    const BuiltSealedRawCertificateV1* sealed_certificate,
    const BuiltEmptyAnchorTombstoneV1* empty_tombstone,
    std::unique_ptr<BuiltFinalizationReportV1>*
        output) noexcept {
    if (output == nullptr) {
        return FinalizationReportV1Error::kNullOutput;
    }
    const FinalizationReportV1Error validation =
        ValidateFinalizationReportV1(report);
    if (validation != FinalizationReportV1Error::kNone) {
        return validation;
    }

    if (report.result ==
        FinalizationReportResultV1::kSealedRaw) {
        if (immutable_grant_flags !=
            kReserveGrantRawFinalization) {
            return FinalizationReportV1Error::
                kGrantKindMismatch;
        }
        if (sealed_certificate == nullptr ||
            empty_tombstone != nullptr) {
            return FinalizationReportV1Error::
                kSidecarMismatch;
        }
        const auto& certificate =
            sealed_certificate->model();
        const auto& terminal = report.segments.back();
        const auto& seal = *report.final_seal;
        const auto& frontier = *report.manifest_frontier;
        if (!SameNamespace(
                report.namespace_identity,
                certificate.namespace_identity) ||
            sealed_certificate->certificate_sha256() !=
                report.sealed_raw_certificate_sha256 ||
            certificate.terminal_append_cursor
                    .segment_sequence !=
                report.final_append_cursor
                    ->segment_sequence ||
            certificate.terminal_append_cursor
                    .global_wal_pos !=
                report.final_append_cursor
                    ->global_wal_pos ||
            certificate.terminal_append_cursor
                    .ingress_sequence !=
                report.final_append_cursor
                    ->ingress_sequence ||
            certificate.terminal_append_cursor
                    .segment_offset !=
                report.final_append_cursor
                    ->segment_offset ||
            certificate.terminal_durable_cursor
                    .segment_sequence !=
                report.final_durable_cursor
                    ->segment_sequence ||
            certificate.terminal_durable_cursor
                    .global_wal_pos !=
                report.final_durable_cursor
                    ->global_wal_pos ||
            certificate.terminal_durable_cursor
                    .ingress_sequence !=
                report.final_durable_cursor
                    ->ingress_sequence ||
            certificate.terminal_durable_cursor
                    .segment_offset !=
                report.final_durable_cursor
                    ->segment_offset ||
            certificate.last_segment_sequence !=
                terminal.segment_sequence ||
            certificate.last_segment_base_wal_pos !=
                terminal.segment_base_wal_pos ||
            certificate.last_segment_logical_length !=
                terminal.logical_length ||
            certificate.last_segment_sha256 !=
                terminal.segment_sha256 ||
            certificate.accepted_sealed_marker_bytes !=
                seal.accepted_sealed_marker_bytes ||
            certificate.accepted_sealed_marker_sha256 !=
                seal.accepted_sealed_marker_sha256 ||
            certificate.closed_entry_count !=
                frontier.closed_entry_count ||
            certificate.closed_prefix_sha256 !=
                frontier.closed_prefix_sha256) {
            return FinalizationReportV1Error::
                kSidecarMismatch;
        }
    } else {
        if (immutable_grant_flags !=
            kReserveGrantRawAnchorOnly) {
            return FinalizationReportV1Error::
                kGrantKindMismatch;
        }
        if (sealed_certificate != nullptr ||
            empty_tombstone == nullptr) {
            return FinalizationReportV1Error::
                kSidecarMismatch;
        }
        const auto& tombstone = empty_tombstone->model();
        if (!SameNamespace(
                report.namespace_identity,
                tombstone.namespace_identity) ||
            report.journal_header_sha256 !=
                tombstone.journal_header_sha256 ||
            *report.marker_count != tombstone.marker_count ||
            tombstone.segment_count != 0U ||
            tombstone.record_count != 0U ||
            empty_tombstone->tombstone_sha256() !=
                report.empty_anchor_tombstone_sha256) {
            return FinalizationReportV1Error::
                kSidecarMismatch;
        }
    }

    try {
        std::string canonical;
        FinalizationReportV1Error error =
            EncodeFinalizationReportV1Jcs(
                report, &canonical);
        if (error != FinalizationReportV1Error::kNone) {
            return error;
        }
        std::string filename;
        error = FinalizationReportV1Filename(
            report, &filename);
        if (error != FinalizationReportV1Error::kNone) {
            return error;
        }
        const RawV1Digest digest =
            common::ComputeSha256(canonical);
        auto built = std::unique_ptr<BuiltFinalizationReportV1>(
            new BuiltFinalizationReportV1(
                report,
                std::move(canonical),
                std::move(filename),
                digest));
        output->swap(built);
        return FinalizationReportV1Error::kNone;
    } catch (const std::bad_alloc&) {
        return FinalizationReportV1Error::kAllocationFailure;
    }
}

}  // namespace l2flow::ingress
