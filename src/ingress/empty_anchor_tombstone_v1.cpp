#include "l2flow/ingress/empty_anchor_tombstone_v1.h"

#include "l2flow/common/identity128.h"
#include "l2flow/common/sha256.h"

#include <algorithm>
#include <array>
#include <charconv>
#include <cstddef>
#include <cstdint>
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
            return byte == std::byte{0U};
        });
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

void AppendQuotedU64(
    std::string* output,
    std::uint64_t value) {
    output->push_back('"');
    AppendInteger(output, value);
    output->push_back('"');
}

template <std::size_t Size>
void AppendLowerHex(
    std::string* output,
    const std::array<std::byte, Size>& bytes) {
    static constexpr char digits[] =
        "0123456789abcdef";
    output->push_back('"');
    for (const std::byte byte : bytes) {
        const unsigned value =
            std::to_integer<unsigned>(byte);
        output->push_back(
            digits[(value >> 4U) & 0x0fU]);
        output->push_back(digits[value & 0x0fU]);
    }
    output->push_back('"');
}

template <std::size_t Size>
[[nodiscard]] std::string LowerHex(
    const std::array<std::byte, Size>& bytes) {
    std::string output;
    output.reserve(Size * 2U);
    static constexpr char digits[] =
        "0123456789abcdef";
    for (const std::byte byte : bytes) {
        const unsigned value =
            std::to_integer<unsigned>(byte);
        output.push_back(
            digits[(value >> 4U) & 0x0fU]);
        output.push_back(digits[value & 0x0fU]);
    }
    return output;
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

}  // namespace

std::string_view EmptyAnchorTombstoneV1ErrorName(
    EmptyAnchorTombstoneV1Error error) noexcept {
    switch (error) {
        case EmptyAnchorTombstoneV1Error::kNone:
            return "none";
        case EmptyAnchorTombstoneV1Error::kNullOutput:
            return "null_output";
        case EmptyAnchorTombstoneV1Error::
            kUnsupportedSchemaVersion:
            return "unsupported_schema_version";
        case EmptyAnchorTombstoneV1Error::
            kInvalidNamespace:
            return "invalid_namespace";
        case EmptyAnchorTombstoneV1Error::
            kInvalidJournalHeader:
            return "invalid_journal_header";
        case EmptyAnchorTombstoneV1Error::
            kNotEmptyAnchor:
            return "not_empty_anchor";
        case EmptyAnchorTombstoneV1Error::kHashInvalid:
            return "hash_invalid";
        case EmptyAnchorTombstoneV1Error::
            kInvalidCanonicalJson:
            return "invalid_canonical_json";
        case EmptyAnchorTombstoneV1Error::
            kFilenameInvalid:
            return "filename_invalid";
        case EmptyAnchorTombstoneV1Error::
            kEncodedSizeExceeded:
            return "encoded_size_exceeded";
        case EmptyAnchorTombstoneV1Error::
            kAllocationFailure:
            return "allocation_failure";
    }
    return "unknown";
}

EmptyAnchorTombstoneV1Error
ValidateEmptyAnchorTombstoneV1(
    const EmptyAnchorTombstoneV1& tombstone) noexcept {
    if (tombstone.schema_version !=
        kEmptyAnchorTombstoneV1SchemaVersion) {
        return EmptyAnchorTombstoneV1Error::
            kUnsupportedSchemaVersion;
    }
    if (tombstone.namespace_identity.capture_date == 0U ||
        tombstone.namespace_identity.source_stream_id ==
            0U ||
        common::IsZeroIdentity(
            tombstone.namespace_identity.stream_day_id)) {
        return EmptyAnchorTombstoneV1Error::
            kInvalidNamespace;
    }
    if (IsZero(tombstone.journal_header_sha256)) {
        return EmptyAnchorTombstoneV1Error::kHashInvalid;
    }
    if (tombstone.marker_count != 0U ||
        tombstone.segment_count != 0U ||
        tombstone.record_count != 0U) {
        return EmptyAnchorTombstoneV1Error::
            kNotEmptyAnchor;
    }
    return EmptyAnchorTombstoneV1Error::kNone;
}

EmptyAnchorTombstoneV1Error
BuildEmptyAnchorTombstoneV1(
    const EmptyAnchorObservationV1& observation,
    EmptyAnchorTombstoneV1* output) noexcept {
    if (output == nullptr) {
        return EmptyAnchorTombstoneV1Error::kNullOutput;
    }
    if (observation.journal_logical_size !=
            kRawV1JournalHeaderBytes ||
        observation.marker_count != 0U ||
        observation.segment_count != 0U ||
        observation.record_count != 0U) {
        return EmptyAnchorTombstoneV1Error::
            kNotEmptyAnchor;
    }
    DurableJournalHeaderV1 decoded{};
    if (DecodeDurableJournalHeaderV1(
            observation.journal_header_bytes,
            &decoded) != RawV1Error::kNone) {
        return EmptyAnchorTombstoneV1Error::
            kInvalidJournalHeader;
    }

    EmptyAnchorTombstoneV1 candidate{};
    candidate.namespace_identity.capture_date =
        decoded.capture_date;
    candidate.namespace_identity.source_stream_id =
        decoded.source_stream_id;
    candidate.namespace_identity.stream_day_id =
        decoded.stream_day_id;
    candidate.journal_header_sha256 =
        common::ComputeSha256(
            observation.journal_header_bytes);
    const EmptyAnchorTombstoneV1Error validation =
        ValidateEmptyAnchorTombstoneV1(candidate);
    if (validation !=
        EmptyAnchorTombstoneV1Error::kNone) {
        return validation;
    }
    *output = candidate;
    return EmptyAnchorTombstoneV1Error::kNone;
}

EmptyAnchorTombstoneV1Error
EncodeEmptyAnchorTombstoneV1Jcs(
    const EmptyAnchorTombstoneV1& tombstone,
    std::string* output) noexcept {
    if (output == nullptr) {
        return EmptyAnchorTombstoneV1Error::kNullOutput;
    }
    const EmptyAnchorTombstoneV1Error validation =
        ValidateEmptyAnchorTombstoneV1(tombstone);
    if (validation !=
        EmptyAnchorTombstoneV1Error::kNone) {
        return validation;
    }
    try {
        std::string encoded;
        encoded.reserve(512U);
        encoded.append("{\"capture_date\":");
        AppendInteger(
            &encoded,
            tombstone.namespace_identity.capture_date);
        encoded.append(",\"journal_header_sha256\":");
        AppendLowerHex(
            &encoded, tombstone.journal_header_sha256);
        encoded.append(",\"marker_count\":");
        AppendQuotedU64(
            &encoded, tombstone.marker_count);
        encoded.append(",\"record_count\":");
        AppendQuotedU64(
            &encoded, tombstone.record_count);
        encoded.append(",\"schema_version\":");
        AppendInteger(
            &encoded, tombstone.schema_version);
        encoded.append(",\"segment_count\":");
        AppendQuotedU64(
            &encoded, tombstone.segment_count);
        encoded.append(",\"source_stream_id\":");
        AppendInteger(
            &encoded,
            tombstone.namespace_identity.source_stream_id);
        encoded.append(",\"stream_day_id\":");
        AppendLowerHex(
            &encoded,
            tombstone.namespace_identity.stream_day_id);
        encoded.push_back('}');
        if (encoded.size() >
            kEmptyAnchorTombstoneV1MaximumBytes) {
            return EmptyAnchorTombstoneV1Error::
                kEncodedSizeExceeded;
        }
        *output = std::move(encoded);
        return EmptyAnchorTombstoneV1Error::kNone;
    } catch (...) {
        return EmptyAnchorTombstoneV1Error::
            kAllocationFailure;
    }
}

EmptyAnchorTombstoneV1Error
ParseEmptyAnchorTombstoneV1Jcs(
    std::string_view exact_bytes,
    EmptyAnchorTombstoneV1* output) noexcept {
    if (output == nullptr) {
        return EmptyAnchorTombstoneV1Error::kNullOutput;
    }
    if (exact_bytes.empty() ||
        exact_bytes.size() >
            kEmptyAnchorTombstoneV1MaximumBytes) {
        return EmptyAnchorTombstoneV1Error::
            kInvalidCanonicalJson;
    }
    EmptyAnchorTombstoneV1 candidate{};
    CanonicalParser parser(exact_bytes);
    if (!parser.Consume("{\"capture_date\":") ||
        !parser.ParseU32(
            &candidate.namespace_identity.capture_date) ||
        !parser.Consume(
            ",\"journal_header_sha256\":") ||
        !parser.ParseHex(
            &candidate.journal_header_sha256) ||
        !parser.Consume(",\"marker_count\":") ||
        !parser.ParseQuotedU64(
            &candidate.marker_count) ||
        !parser.Consume(",\"record_count\":") ||
        !parser.ParseQuotedU64(
            &candidate.record_count) ||
        !parser.Consume(",\"schema_version\":") ||
        !parser.ParseU32(&candidate.schema_version) ||
        !parser.Consume(",\"segment_count\":") ||
        !parser.ParseQuotedU64(
            &candidate.segment_count) ||
        !parser.Consume(",\"source_stream_id\":") ||
        !parser.ParseU32(
            &candidate.namespace_identity
                 .source_stream_id) ||
        !parser.Consume(",\"stream_day_id\":") ||
        !parser.ParseHex(
            &candidate.namespace_identity
                 .stream_day_id) ||
        !parser.Consume("}") || !parser.done()) {
        return EmptyAnchorTombstoneV1Error::
            kInvalidCanonicalJson;
    }
    const EmptyAnchorTombstoneV1Error validation =
        ValidateEmptyAnchorTombstoneV1(candidate);
    if (validation !=
        EmptyAnchorTombstoneV1Error::kNone) {
        return validation;
    }
    std::string canonical;
    const EmptyAnchorTombstoneV1Error encode_error =
        EncodeEmptyAnchorTombstoneV1Jcs(
            candidate, &canonical);
    if (encode_error !=
        EmptyAnchorTombstoneV1Error::kNone) {
        return encode_error;
    }
    if (canonical != exact_bytes) {
        return EmptyAnchorTombstoneV1Error::
            kInvalidCanonicalJson;
    }
    *output = candidate;
    return EmptyAnchorTombstoneV1Error::kNone;
}

EmptyAnchorTombstoneV1Error
EmptyAnchorTombstoneV1Filename(
    const EmptyAnchorTombstoneV1& tombstone,
    std::string* output) noexcept {
    if (output == nullptr) {
        return EmptyAnchorTombstoneV1Error::kNullOutput;
    }
    const EmptyAnchorTombstoneV1Error validation =
        ValidateEmptyAnchorTombstoneV1(tombstone);
    if (validation !=
        EmptyAnchorTombstoneV1Error::kNone) {
        return validation;
    }
    try {
        std::string filename("empty-anchor-");
        filename.append(LowerHex(
            tombstone.namespace_identity.stream_day_id));
        filename.append(".json");
        if (filename.size() != 50U) {
            return EmptyAnchorTombstoneV1Error::
                kFilenameInvalid;
        }
        output->swap(filename);
        return EmptyAnchorTombstoneV1Error::kNone;
    } catch (...) {
        return EmptyAnchorTombstoneV1Error::
            kAllocationFailure;
    }
}

EmptyAnchorTombstoneV1Error
BuildEmptyAnchorTombstoneCapabilityV1(
    const EmptyAnchorObservationV1& observation,
    std::unique_ptr<
        BuiltEmptyAnchorTombstoneV1>* output) noexcept {
    if (output == nullptr) {
        return EmptyAnchorTombstoneV1Error::kNullOutput;
    }
    EmptyAnchorTombstoneV1 model{};
    const EmptyAnchorTombstoneV1Error build_error =
        BuildEmptyAnchorTombstoneV1(
            observation, &model);
    if (build_error !=
        EmptyAnchorTombstoneV1Error::kNone) {
        return build_error;
    }
    try {
        std::string canonical;
        EmptyAnchorTombstoneV1Error error =
            EncodeEmptyAnchorTombstoneV1Jcs(
                model, &canonical);
        if (error !=
            EmptyAnchorTombstoneV1Error::kNone) {
            return error;
        }
        std::string filename;
        error = EmptyAnchorTombstoneV1Filename(
            model, &filename);
        if (error !=
            EmptyAnchorTombstoneV1Error::kNone) {
            return error;
        }
        const RawV1Digest digest =
            common::ComputeSha256(canonical);
        auto candidate = std::unique_ptr<
            BuiltEmptyAnchorTombstoneV1>(
            new BuiltEmptyAnchorTombstoneV1(
                std::move(model),
                std::move(canonical),
                std::move(filename),
                digest));
        *output = std::move(candidate);
        return EmptyAnchorTombstoneV1Error::kNone;
    } catch (...) {
        return EmptyAnchorTombstoneV1Error::
            kAllocationFailure;
    }
}

}  // namespace l2flow::ingress
