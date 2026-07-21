#include "l2flow/ingress/raw_sealed_certificate_v1.h"

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

[[nodiscard]] bool CheckedAdd(
    std::uint64_t left,
    std::uint64_t right,
    std::uint64_t* result) noexcept {
    if (result == nullptr ||
        left >
            std::numeric_limits<std::uint64_t>::max() -
                right) {
        return false;
    }
    *result = left + right;
    return true;
}

[[nodiscard]] bool SameNamespace(
    const RawManifestNamespaceV1& left,
    const RawManifestNamespaceV1& right) noexcept {
    return left.capture_date == right.capture_date &&
           left.source_stream_id ==
               right.source_stream_id &&
           left.stream_day_id == right.stream_day_id;
}

[[nodiscard]] bool SameCursor(
    const SealedRawCertificateCursorV1& left,
    const SealedRawCertificateCursorV1& right) noexcept {
    return left.segment_sequence == right.segment_sequence &&
           left.global_wal_pos == right.global_wal_pos &&
           left.ingress_sequence == right.ingress_sequence &&
           left.segment_offset == right.segment_offset;
}

[[nodiscard]] SealedRawCertificateCursorV1 CursorFrom(
    std::uint32_t segment_sequence,
    const RawWalCursor& cursor) noexcept {
    return {
        segment_sequence,
        cursor.global_wal_pos,
        cursor.ingress_sequence,
        cursor.segment_offset};
}

[[nodiscard]] SealedRawCertificateCursorV1 CursorFrom(
    const DurableMarkerV1& marker) noexcept {
    return {
        marker.segment_sequence,
        marker.durable_global_wal_pos,
        marker.durable_ingress_sequence,
        marker.durable_segment_offset};
}

template <typename Integer>
void AppendInteger(
    std::string* output,
    Integer value) {
    static_assert(std::is_integral_v<Integer>);
    std::array<char, 32U> bytes{};
    const auto converted = std::to_chars(
        bytes.data(), bytes.data() + bytes.size(), value);
    if (converted.ec != std::errc{}) {
        throw std::bad_alloc();
    }
    output->append(bytes.data(), converted.ptr);
}

void AppendQuoted(
    std::string* output,
    std::string_view text) {
    output->push_back('"');
    output->append(text);
    output->push_back('"');
}

template <std::size_t Size>
[[nodiscard]] std::string LowerHex(
    const std::array<std::byte, Size>& bytes) {
    static constexpr char digits[] =
        "0123456789abcdef";
    std::string output;
    output.resize(Size * 2U);
    for (std::size_t index = 0U;
         index < Size;
         ++index) {
        const unsigned value =
            std::to_integer<unsigned>(bytes[index]);
        output[index * 2U] =
            digits[(value >> 4U) & 0x0fU];
        output[index * 2U + 1U] =
            digits[value & 0x0fU];
    }
    return output;
}

void AppendCursor(
    std::string* output,
    const SealedRawCertificateCursorV1& cursor) {
    output->append("{\"global_wal_pos\":");
    AppendQuoted(
        output, std::to_string(cursor.global_wal_pos));
    output->append(",\"ingress_sequence\":");
    AppendQuoted(
        output, std::to_string(cursor.ingress_sequence));
    output->append(",\"segment_offset\":");
    AppendQuoted(
        output, std::to_string(cursor.segment_offset));
    output->append(",\"segment_sequence\":");
    AppendInteger(output, cursor.segment_sequence);
    output->push_back('}');
}

class CanonicalParser final {
public:
    explicit CanonicalParser(
        std::string_view input) noexcept
        : input_(input) {}

    [[nodiscard]] bool Consume(
        std::string_view token) noexcept {
        if (input_.substr(position_, token.size()) != token) {
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
        const char* const first =
            input_.data() + begin;
        const char* const last =
            input_.data() + position_;
        std::uint32_t candidate = 0U;
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
        const char* const first =
            input_.data() + begin;
        const char* const last =
            input_.data() + position_;
        std::uint64_t candidate = 0U;
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

    [[nodiscard]] bool done() const noexcept {
        return position_ == input_.size();
    }

private:
    [[nodiscard]] static int HexNibble(
        char character) noexcept {
        if (character >= '0' && character <= '9') {
            return character - '0';
        }
        if (character >= 'a' && character <= 'f') {
            return 10 + character - 'a';
        }
        return -1;
    }

    std::string_view input_;
    std::size_t position_ = 0U;
};

[[nodiscard]] bool ParseCursor(
    CanonicalParser* parser,
    SealedRawCertificateCursorV1* output) noexcept {
    SealedRawCertificateCursorV1 candidate{};
    if (parser == nullptr || output == nullptr ||
        !parser->Consume("{\"global_wal_pos\":") ||
        !parser->ParseQuotedU64(
            &candidate.global_wal_pos) ||
        !parser->Consume(",\"ingress_sequence\":") ||
        !parser->ParseQuotedU64(
            &candidate.ingress_sequence) ||
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

[[nodiscard]] SealedRawCertificateV1Error
ValidateIntrinsic(
    const SealedRawCertificateV1& certificate,
    DurableMarkerV1* decoded_marker) noexcept {
    if (certificate.schema_version !=
        kSealedRawCertificateV1SchemaVersion) {
        return SealedRawCertificateV1Error::
            kUnsupportedSchemaVersion;
    }
    if (certificate.namespace_identity.capture_date == 0U ||
        certificate.namespace_identity.source_stream_id == 0U ||
        l2flow::common::IsZeroIdentity(
            certificate.namespace_identity.stream_day_id) ||
        IsZero(certificate.journal_header_sha256) ||
        certificate.last_segment_sequence == 0U ||
        certificate.last_segment_logical_length <
            kRawV1SegmentHeaderBytes ||
        IsZero(certificate.last_segment_sha256) ||
        certificate.closed_entry_count == 0U ||
        IsZero(certificate.closed_prefix_sha256)) {
        return SealedRawCertificateV1Error::
            kInvalidNamespace;
    }
    if ((certificate.last_segment_flags &
         ~kRawV1SegmentFlagsMask) != 0U) {
        return SealedRawCertificateV1Error::
            kTerminalSegmentMismatch;
    }

    DurableMarkerV1 marker{};
    if (DecodeDurableMarkerV1(
            certificate.accepted_sealed_marker_bytes,
            &marker) != RawV1Error::kNone ||
        marker.marker_flags != kRawV1SegmentSealed ||
        marker.source_stream_id !=
            certificate.namespace_identity.source_stream_id ||
        marker.segment_sequence !=
            certificate.last_segment_sequence) {
        return SealedRawCertificateV1Error::
            kInvalidTerminalMarker;
    }
    if (ComputeAcceptedMarkerSha256(
            certificate.accepted_sealed_marker_bytes) !=
            certificate.accepted_sealed_marker_sha256 ||
        IsZero(
            certificate.accepted_sealed_marker_sha256)) {
        return SealedRawCertificateV1Error::kHashMismatch;
    }
    std::uint64_t expected_global = 0U;
    if (!CheckedAdd(
            certificate.last_segment_base_wal_pos,
            certificate.last_segment_logical_length,
            &expected_global) ||
        marker.durable_global_wal_pos != expected_global ||
        marker.durable_segment_offset !=
            certificate.last_segment_logical_length) {
        return SealedRawCertificateV1Error::
            kTerminalSegmentMismatch;
    }
    const SealedRawCertificateCursorV1 expected =
        CursorFrom(marker);
    if (!SameCursor(
            certificate.terminal_append_cursor,
            expected) ||
        !SameCursor(
            certificate.terminal_durable_cursor,
            expected)) {
        return SealedRawCertificateV1Error::
            kTerminalCursorMismatch;
    }
    if (decoded_marker != nullptr) {
        *decoded_marker = marker;
    }
    return SealedRawCertificateV1Error::kNone;
}

}  // namespace

std::string_view SealedRawCertificateV1ErrorName(
    SealedRawCertificateV1Error error) noexcept {
    switch (error) {
    case SealedRawCertificateV1Error::kNone:
        return "none";
    case SealedRawCertificateV1Error::kNullOutput:
        return "null_output";
    case SealedRawCertificateV1Error::
        kUnsupportedSchemaVersion:
        return "unsupported_schema_version";
    case SealedRawCertificateV1Error::kInvalidNamespace:
        return "invalid_namespace";
    case SealedRawCertificateV1Error::
        kInvalidJournalHeader:
        return "invalid_journal_header";
    case SealedRawCertificateV1Error::
        kJournalNamespaceMismatch:
        return "journal_namespace_mismatch";
    case SealedRawCertificateV1Error::kInvalidManifest:
        return "invalid_manifest";
    case SealedRawCertificateV1Error::
        kManifestNotTerminal:
        return "manifest_not_terminal";
    case SealedRawCertificateV1Error::
        kInvalidTerminalMarker:
        return "invalid_terminal_marker";
    case SealedRawCertificateV1Error::
        kTerminalCursorMismatch:
        return "terminal_cursor_mismatch";
    case SealedRawCertificateV1Error::
        kTerminalSegmentMismatch:
        return "terminal_segment_mismatch";
    case SealedRawCertificateV1Error::kHashMismatch:
        return "hash_mismatch";
    case SealedRawCertificateV1Error::
        kInvalidCanonicalJson:
        return "invalid_canonical_json";
    case SealedRawCertificateV1Error::kFilenameInvalid:
        return "filename_invalid";
    case SealedRawCertificateV1Error::
        kEncodedSizeExceeded:
        return "encoded_size_exceeded";
    case SealedRawCertificateV1Error::
        kAllocationFailure:
        return "allocation_failure";
    }
    return "unknown";
}

SealedRawCertificateV1Error
BuildSealedRawCertificateV1(
    const RawV1JournalHeaderWire& journal_header_bytes,
    const RawManifestV1& manifest,
    const RawWalSinkIdentityV1& final_sink_identity,
    const RawWalWriterSnapshot& final_wal,
    SealedRawCertificateV1* output) noexcept {
    if (output == nullptr) {
        return SealedRawCertificateV1Error::kNullOutput;
    }
    if (ValidateManifestModel(manifest) !=
        RawManifestV1Error::kNone) {
        return SealedRawCertificateV1Error::
            kInvalidManifest;
    }
    if (manifest.open_entry.has_value() ||
        manifest.closed_entries.empty() ||
        manifest.closed_entry_count !=
            manifest.closed_entries.size()) {
        return SealedRawCertificateV1Error::
            kManifestNotTerminal;
    }

    DurableJournalHeaderV1 journal_header{};
    if (DecodeDurableJournalHeaderV1(
            journal_header_bytes,
            &journal_header) != RawV1Error::kNone ||
        journal_header.raw_schema_sha256 !=
            kFrozenRawSchemaSha256) {
        return SealedRawCertificateV1Error::
            kInvalidJournalHeader;
    }
    const RawManifestNamespaceV1 journal_namespace{
        journal_header.capture_date,
        journal_header.source_stream_id,
        journal_header.stream_day_id};
    if (!SameNamespace(
            journal_namespace,
            manifest.namespace_identity)) {
        return SealedRawCertificateV1Error::
            kJournalNamespaceMismatch;
    }

    const RawManifestSegmentEntryV1& terminal =
        manifest.closed_entries.back();
    DurableMarkerV1 marker{};
    if (DecodeDurableMarkerV1(
            terminal.accepted_marker_bytes,
            &marker) != RawV1Error::kNone ||
        marker.marker_flags != kRawV1SegmentSealed) {
        return SealedRawCertificateV1Error::
            kInvalidTerminalMarker;
    }
    if (!final_wal.initialized ||
        !final_wal.sealed ||
        !final_wal.closed ||
        final_wal.fatal ||
        final_wal.append != final_wal.durable) {
        return SealedRawCertificateV1Error::
            kTerminalCursorMismatch;
    }
    if (final_sink_identity.source_stream_id !=
            manifest.namespace_identity.source_stream_id ||
        final_sink_identity.capture_date !=
            manifest.namespace_identity.capture_date ||
        final_sink_identity.stream_day_id !=
            manifest.namespace_identity.stream_day_id ||
        final_sink_identity.segment_sequence !=
            terminal.segment_sequence ||
        final_sink_identity.segment_base_wal_pos !=
            terminal.segment_base_wal_pos ||
        l2flow::common::IsZeroIdentity(
            final_sink_identity.writer_instance)) {
        return SealedRawCertificateV1Error::
            kTerminalSegmentMismatch;
    }
    const SealedRawCertificateCursorV1 marker_cursor =
        CursorFrom(marker);
    const SealedRawCertificateCursorV1 append_cursor =
        CursorFrom(
            final_sink_identity.segment_sequence,
            final_wal.append);
    if (!SameCursor(marker_cursor, append_cursor) ||
        terminal.accepted_marker_sha256 !=
            ComputeAcceptedMarkerSha256(
                terminal.accepted_marker_bytes)) {
        return SealedRawCertificateV1Error::
            kTerminalCursorMismatch;
    }

    SealedRawCertificateV1 candidate{};
    candidate.namespace_identity =
        manifest.namespace_identity;
    candidate.journal_header_sha256 =
        l2flow::common::ComputeSha256(
            journal_header_bytes);
    candidate.terminal_append_cursor = append_cursor;
    candidate.terminal_durable_cursor = append_cursor;
    candidate.last_segment_sequence =
        terminal.segment_sequence;
    candidate.last_segment_flags =
        terminal.segment_flags;
    candidate.last_segment_base_wal_pos =
        terminal.segment_base_wal_pos;
    candidate.last_segment_logical_length =
        terminal.segment_logical_length;
    candidate.last_segment_sha256 =
        terminal.segment_sha256;
    candidate.accepted_sealed_marker_bytes =
        terminal.accepted_marker_bytes;
    candidate.accepted_sealed_marker_sha256 =
        terminal.accepted_marker_sha256;
    candidate.closed_entry_count =
        manifest.closed_entry_count;
    candidate.closed_prefix_sha256 =
        manifest.closed_prefix_sha256;

    const SealedRawCertificateV1Error validation =
        ValidateIntrinsic(candidate, nullptr);
    if (validation != SealedRawCertificateV1Error::kNone) {
        return validation;
    }
    using std::swap;
    swap(*output, candidate);
    return SealedRawCertificateV1Error::kNone;
}

SealedRawCertificateV1Error
BuildSealedRawCertificateCapabilityV1(
    const RawV1JournalHeaderWire& journal_header_bytes,
    const RawManifestV1& manifest,
    const RawWalSinkIdentityV1& final_sink_identity,
    const RawWalWriterSnapshot& final_wal,
    std::unique_ptr<
        BuiltSealedRawCertificateV1>* output) noexcept {
    if (output == nullptr) {
        return SealedRawCertificateV1Error::kNullOutput;
    }
    try {
        SealedRawCertificateV1 model{};
        SealedRawCertificateV1Error error =
            BuildSealedRawCertificateV1(
                journal_header_bytes,
                manifest,
                final_sink_identity,
                final_wal,
                &model);
        if (error != SealedRawCertificateV1Error::kNone) {
            return error;
        }
        std::string canonical_jcs;
        error = EncodeSealedRawCertificateV1Jcs(
            model, &canonical_jcs);
        if (error != SealedRawCertificateV1Error::kNone) {
            return error;
        }
        std::string filename;
        error = SealedRawCertificateV1Filename(
            model, &filename);
        if (error != SealedRawCertificateV1Error::kNone) {
            return error;
        }
        const RawV1Digest certificate_sha256 =
            l2flow::common::ComputeSha256(canonical_jcs);
        auto candidate = std::unique_ptr<
            BuiltSealedRawCertificateV1>(
            new BuiltSealedRawCertificateV1(
                std::move(model),
                std::move(canonical_jcs),
                std::move(filename),
                certificate_sha256));
        *output = std::move(candidate);
        return SealedRawCertificateV1Error::kNone;
    } catch (const std::bad_alloc&) {
        return SealedRawCertificateV1Error::
            kAllocationFailure;
    } catch (...) {
        return SealedRawCertificateV1Error::
            kAllocationFailure;
    }
}

SealedRawCertificateV1Error
ValidateSealedRawCertificateV1(
    const SealedRawCertificateV1& certificate) noexcept {
    return ValidateIntrinsic(certificate, nullptr);
}

SealedRawCertificateV1Error
EncodeSealedRawCertificateV1Jcs(
    const SealedRawCertificateV1& certificate,
    std::string* output) noexcept {
    if (output == nullptr) {
        return SealedRawCertificateV1Error::kNullOutput;
    }
    const SealedRawCertificateV1Error validation =
        ValidateIntrinsic(certificate, nullptr);
    if (validation != SealedRawCertificateV1Error::kNone) {
        return validation;
    }

    try {
        std::string candidate;
        candidate.reserve(2048U);
        candidate.append(
            "{\"accepted_segment_sealed_marker\":{"
            "\"global_wal_pos\":");
        AppendQuoted(
            &candidate,
            std::to_string(
                certificate.terminal_durable_cursor
                    .global_wal_pos));
        candidate.append(",\"ingress_sequence\":");
        AppendQuoted(
            &candidate,
            std::to_string(
                certificate.terminal_durable_cursor
                    .ingress_sequence));
        candidate.append(",\"marker_bytes\":");
        AppendQuoted(
            &candidate,
            LowerHex(
                certificate
                    .accepted_sealed_marker_bytes));
        candidate.append(",\"marker_flags\":");
        AppendInteger(
            &candidate,
            kRawV1SegmentSealed);
        candidate.append(",\"marker_sha256\":");
        AppendQuoted(
            &candidate,
            l2flow::common::Sha256Hex(
                certificate
                    .accepted_sealed_marker_sha256));
        candidate.append(",\"segment_offset\":");
        AppendQuoted(
            &candidate,
            std::to_string(
                certificate.terminal_durable_cursor
                    .segment_offset));
        candidate.append(",\"segment_sequence\":");
        AppendInteger(
            &candidate,
            certificate.last_segment_sequence);
        candidate.append("},\"capture_date\":");
        AppendInteger(
            &candidate,
            certificate.namespace_identity.capture_date);
        candidate.append(",\"closed_frontier\":{"
                         "\"closed_entry_count\":");
        AppendQuoted(
            &candidate,
            std::to_string(
                certificate.closed_entry_count));
        candidate.append(",\"closed_prefix_sha256\":");
        AppendQuoted(
            &candidate,
            l2flow::common::Sha256Hex(
                certificate.closed_prefix_sha256));
        candidate.append(
            ",\"frontier_seal_marker_sha256\":");
        AppendQuoted(
            &candidate,
            l2flow::common::Sha256Hex(
                certificate
                    .accepted_sealed_marker_sha256));
        candidate.append(
            ",\"frontier_segment_sequence\":");
        AppendInteger(
            &candidate,
            certificate.last_segment_sequence);
        candidate.append(
            ",\"frontier_segment_sha256\":");
        AppendQuoted(
            &candidate,
            l2flow::common::Sha256Hex(
                certificate.last_segment_sha256));
        candidate.append("},\"journal_header_sha256\":");
        AppendQuoted(
            &candidate,
            l2flow::common::Sha256Hex(
                certificate.journal_header_sha256));
        candidate.append(",\"last_segment\":{"
                         "\"logical_length\":");
        AppendQuoted(
            &candidate,
            std::to_string(
                certificate
                    .last_segment_logical_length));
        candidate.append(",\"segment_base_wal_pos\":");
        AppendQuoted(
            &candidate,
            std::to_string(
                certificate
                    .last_segment_base_wal_pos));
        candidate.append(",\"segment_flags\":");
        AppendInteger(
            &candidate,
            certificate.last_segment_flags);
        candidate.append(",\"segment_sequence\":");
        AppendInteger(
            &candidate,
            certificate.last_segment_sequence);
        candidate.append(",\"segment_sha256\":");
        AppendQuoted(
            &candidate,
            l2flow::common::Sha256Hex(
                certificate.last_segment_sha256));
        candidate.append("},\"schema_version\":");
        AppendInteger(
            &candidate,
            certificate.schema_version);
        candidate.append(",\"source_stream_id\":");
        AppendInteger(
            &candidate,
            certificate.namespace_identity
                .source_stream_id);
        candidate.append(",\"stream_day_id\":");
        AppendQuoted(
            &candidate,
            l2flow::common::Identity128Hex(
                certificate.namespace_identity
                    .stream_day_id));
        candidate.append(",\"terminal_append_cursor\":");
        AppendCursor(
            &candidate,
            certificate.terminal_append_cursor);
        candidate.append(
            ",\"terminal_durable_cursor\":");
        AppendCursor(
            &candidate,
            certificate.terminal_durable_cursor);
        candidate.push_back('}');
        if (candidate.size() >
            kSealedRawCertificateV1MaximumBytes) {
            return SealedRawCertificateV1Error::
                kEncodedSizeExceeded;
        }
        output->swap(candidate);
        return SealedRawCertificateV1Error::kNone;
    } catch (const std::bad_alloc&) {
        return SealedRawCertificateV1Error::
            kAllocationFailure;
    } catch (...) {
        return SealedRawCertificateV1Error::
            kAllocationFailure;
    }
}

SealedRawCertificateV1Error
ParseSealedRawCertificateV1Jcs(
    std::string_view exact_bytes,
    SealedRawCertificateV1* output) noexcept {
    if (output == nullptr) {
        return SealedRawCertificateV1Error::kNullOutput;
    }
    if (exact_bytes.empty() ||
        exact_bytes.size() >
            kSealedRawCertificateV1MaximumBytes) {
        return SealedRawCertificateV1Error::
            kInvalidCanonicalJson;
    }
    try {
        CanonicalParser parser(exact_bytes);
        SealedRawCertificateV1 candidate{};
        std::uint64_t marker_global = 0U;
        std::uint64_t marker_ingress = 0U;
        std::uint32_t marker_flags = 0U;
        std::uint64_t marker_offset = 0U;
        std::uint32_t marker_sequence = 0U;
        RawV1Digest frontier_marker_sha{};
        std::uint32_t frontier_sequence = 0U;
        RawV1Digest frontier_segment_sha{};

        if (!parser.Consume(
                "{\"accepted_segment_sealed_marker\":{"
                "\"global_wal_pos\":") ||
            !parser.ParseQuotedU64(&marker_global) ||
            !parser.Consume(",\"ingress_sequence\":") ||
            !parser.ParseQuotedU64(&marker_ingress) ||
            !parser.Consume(",\"marker_bytes\":") ||
            !parser.ParseHex(
                &candidate
                     .accepted_sealed_marker_bytes) ||
            !parser.Consume(",\"marker_flags\":") ||
            !parser.ParseU32(&marker_flags) ||
            !parser.Consume(",\"marker_sha256\":") ||
            !parser.ParseHex(
                &candidate
                     .accepted_sealed_marker_sha256) ||
            !parser.Consume(",\"segment_offset\":") ||
            !parser.ParseQuotedU64(&marker_offset) ||
            !parser.Consume(",\"segment_sequence\":") ||
            !parser.ParseU32(&marker_sequence) ||
            !parser.Consume("},\"capture_date\":") ||
            !parser.ParseU32(
                &candidate.namespace_identity
                     .capture_date) ||
            !parser.Consume(
                ",\"closed_frontier\":{"
                "\"closed_entry_count\":") ||
            !parser.ParseQuotedU64(
                &candidate.closed_entry_count) ||
            !parser.Consume(
                ",\"closed_prefix_sha256\":") ||
            !parser.ParseHex(
                &candidate.closed_prefix_sha256) ||
            !parser.Consume(
                ",\"frontier_seal_marker_sha256\":") ||
            !parser.ParseHex(&frontier_marker_sha) ||
            !parser.Consume(
                ",\"frontier_segment_sequence\":") ||
            !parser.ParseU32(&frontier_sequence) ||
            !parser.Consume(
                ",\"frontier_segment_sha256\":") ||
            !parser.ParseHex(&frontier_segment_sha) ||
            !parser.Consume(
                "},\"journal_header_sha256\":") ||
            !parser.ParseHex(
                &candidate.journal_header_sha256) ||
            !parser.Consume(
                ",\"last_segment\":{"
                "\"logical_length\":") ||
            !parser.ParseQuotedU64(
                &candidate
                     .last_segment_logical_length) ||
            !parser.Consume(
                ",\"segment_base_wal_pos\":") ||
            !parser.ParseQuotedU64(
                &candidate
                     .last_segment_base_wal_pos) ||
            !parser.Consume(",\"segment_flags\":") ||
            !parser.ParseU32(
                &candidate.last_segment_flags) ||
            !parser.Consume(
                ",\"segment_sequence\":") ||
            !parser.ParseU32(
                &candidate.last_segment_sequence) ||
            !parser.Consume(",\"segment_sha256\":") ||
            !parser.ParseHex(
                &candidate.last_segment_sha256) ||
            !parser.Consume("},\"schema_version\":") ||
            !parser.ParseU32(&candidate.schema_version) ||
            !parser.Consume(",\"source_stream_id\":") ||
            !parser.ParseU32(
                &candidate.namespace_identity
                     .source_stream_id) ||
            !parser.Consume(",\"stream_day_id\":") ||
            !parser.ParseHex(
                &candidate.namespace_identity
                     .stream_day_id) ||
            !parser.Consume(
                ",\"terminal_append_cursor\":") ||
            !ParseCursor(
                &parser,
                &candidate.terminal_append_cursor) ||
            !parser.Consume(
                ",\"terminal_durable_cursor\":") ||
            !ParseCursor(
                &parser,
                &candidate.terminal_durable_cursor) ||
            !parser.Consume("}") ||
            !parser.done()) {
            return SealedRawCertificateV1Error::
                kInvalidCanonicalJson;
        }
        if (marker_flags != kRawV1SegmentSealed ||
            marker_global !=
                candidate.terminal_durable_cursor
                    .global_wal_pos ||
            marker_ingress !=
                candidate.terminal_durable_cursor
                    .ingress_sequence ||
            marker_offset !=
                candidate.terminal_durable_cursor
                    .segment_offset ||
            marker_sequence !=
                candidate.last_segment_sequence ||
            frontier_marker_sha !=
                candidate
                    .accepted_sealed_marker_sha256 ||
            frontier_sequence !=
                candidate.last_segment_sequence ||
            frontier_segment_sha !=
                candidate.last_segment_sha256) {
            return SealedRawCertificateV1Error::
                kInvalidCanonicalJson;
        }
        const SealedRawCertificateV1Error validation =
            ValidateIntrinsic(candidate, nullptr);
        if (validation !=
            SealedRawCertificateV1Error::kNone) {
            return validation;
        }
        std::string canonical;
        if (EncodeSealedRawCertificateV1Jcs(
                candidate,
                &canonical) !=
                SealedRawCertificateV1Error::kNone ||
            canonical != exact_bytes) {
            return SealedRawCertificateV1Error::
                kInvalidCanonicalJson;
        }
        using std::swap;
        swap(*output, candidate);
        return SealedRawCertificateV1Error::kNone;
    } catch (const std::bad_alloc&) {
        return SealedRawCertificateV1Error::
            kAllocationFailure;
    } catch (...) {
        return SealedRawCertificateV1Error::
            kInvalidCanonicalJson;
    }
}

SealedRawCertificateV1Error
SealedRawCertificateV1Filename(
    const SealedRawCertificateV1& certificate,
    std::string* output) noexcept {
    if (output == nullptr) {
        return SealedRawCertificateV1Error::kNullOutput;
    }
    const SealedRawCertificateV1Error validation =
        ValidateIntrinsic(certificate, nullptr);
    if (validation != SealedRawCertificateV1Error::kNone) {
        return validation;
    }
    try {
        std::string candidate = "sealed-raw-";
        candidate.append(
            l2flow::common::Identity128Hex(
                certificate.namespace_identity
                    .stream_day_id));
        candidate.push_back('-');
        candidate.append(
            l2flow::common::Sha256Hex(
                certificate.closed_prefix_sha256));
        candidate.append(".json");
        if (candidate.size() !=
            std::string_view(
                "sealed-raw-").size() +
                32U + 1U + 64U +
                std::string_view(".json").size()) {
            return SealedRawCertificateV1Error::
                kFilenameInvalid;
        }
        output->swap(candidate);
        return SealedRawCertificateV1Error::kNone;
    } catch (...) {
        return SealedRawCertificateV1Error::
            kAllocationFailure;
    }
}

}  // namespace l2flow::ingress
