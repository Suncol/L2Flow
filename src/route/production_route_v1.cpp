#include "l2flow/route/production_route_v1.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <new>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace l2flow::route {

namespace {

constexpr std::array<std::byte, 8U> kWireMagic{{
    std::byte{'L'}, std::byte{'2'}, std::byte{'P'}, std::byte{'R'},
    std::byte{'V'}, std::byte{'1'}, std::byte{0}, std::byte{0}}};
constexpr char kDigestDomainBytes[] =
    "l2flow.production-route.v1\0";
constexpr std::string_view kDigestDomain{
    kDigestDomainBytes, sizeof(kDigestDomainBytes) - 1U};
constexpr char kClockIdentityDomainBytes[] =
    "l2flow.production-route.clock-epoch-identity.v1\0";
constexpr std::string_view kClockIdentityDomain{
    kClockIdentityDomainBytes,
    sizeof(kClockIdentityDomainBytes) - 1U};
constexpr std::size_t kDigestBytes = 32U;
constexpr std::size_t kTotalBytesOffset = 12U;

template <std::size_t Size>
[[nodiscard]] bool IsZero(
    const std::array<std::byte, Size>& value) noexcept {
    return std::all_of(
        value.begin(), value.end(),
        [](std::byte byte) noexcept {
            return byte == std::byte{0};
        });
}

[[nodiscard]] bool IsLeapYear(std::uint32_t year) noexcept {
    return year % 400U == 0U ||
           (year % 4U == 0U && year % 100U != 0U);
}

[[nodiscard]] bool IsValidDate(std::uint32_t date) noexcept {
    const std::uint32_t year = date / 10'000U;
    const std::uint32_t month = (date / 100U) % 100U;
    const std::uint32_t day = date % 100U;
    if (year < 1992U || year > 9999U || month == 0U || month > 12U) {
        return false;
    }
    constexpr std::array<std::uint32_t, 12U> days{{
        31U, 28U, 31U, 30U, 31U, 30U,
        31U, 31U, 30U, 31U, 30U, 31U}};
    std::uint32_t maximum = days[month - 1U];
    if (month == 2U && IsLeapYear(year)) {
        maximum = 29U;
    }
    return day != 0U && day <= maximum;
}

[[nodiscard]] bool IsNormalizedAbsoluteEndpoint(
    std::string_view value) noexcept {
    if (value.size() < 2U ||
        value.size() > kProductionRouteMaximumEndpointBytesV1 ||
        value.front() != '/' || value.back() == '/') {
        return false;
    }
    std::size_t segment_start = 1U;
    for (std::size_t index = 1U; index <= value.size(); ++index) {
        if (index != value.size() && value[index] != '/') {
            const unsigned char character =
                static_cast<unsigned char>(value[index]);
            if (character < 0x21U || character > 0x7eU) {
                return false;
            }
            continue;
        }
        if (index == segment_start) {
            return false;
        }
        const std::string_view segment =
            value.substr(segment_start, index - segment_start);
        if (segment == "." || segment == "..") {
            return false;
        }
        segment_start = index + 1U;
    }
    return true;
}

template <typename Integer>
void AppendLittleEndian(Integer value, std::vector<std::byte>* output) {
    static_assert(std::is_unsigned_v<Integer>);
    for (std::size_t index = 0U; index < sizeof(Integer); ++index) {
        output->push_back(std::byte{
            static_cast<unsigned char>(value & static_cast<Integer>(0xffU))});
        value >>= 8U;
    }
}

template <std::size_t Size>
void AppendArray(
    const std::array<std::byte, Size>& value,
    std::vector<std::byte>* output) {
    output->insert(output->end(), value.begin(), value.end());
}

void AppendString(
    std::string_view value,
    std::vector<std::byte>* output) {
    AppendLittleEndian(
        static_cast<std::uint32_t>(value.size()), output);
    const auto* const bytes =
        reinterpret_cast<const std::byte*>(value.data());
    output->insert(output->end(), bytes, bytes + value.size());
}

void PatchU32LittleEndian(
    std::uint32_t value,
    std::size_t offset,
    std::vector<std::byte>* output) noexcept {
    for (std::size_t index = 0U; index < sizeof(value); ++index) {
        (*output)[offset + index] = std::byte{
            static_cast<unsigned char>(value & 0xffU)};
        value >>= 8U;
    }
}

[[nodiscard]] l2flow::common::Sha256Digest WireDigest(
    std::span<const std::byte> body) noexcept {
    l2flow::common::Sha256Hasher hasher;
    const auto* const domain =
        reinterpret_cast<const std::byte*>(kDigestDomain.data());
    static_cast<void>(hasher.Update(
        std::span<const std::byte>(domain, kDigestDomain.size())));
    static_cast<void>(hasher.Update(body));
    l2flow::common::Sha256Digest digest{};
    static_cast<void>(hasher.Finalize(&digest));
    return digest;
}

class Reader final {
public:
    explicit Reader(std::span<const std::byte> input) noexcept
        : input_(input) {}

    template <typename Integer>
    [[nodiscard]] bool ReadInteger(Integer* output) noexcept {
        static_assert(std::is_unsigned_v<Integer>);
        if (output == nullptr || Remaining() < sizeof(Integer)) {
            return false;
        }
        std::uint64_t wide = 0U;
        for (std::size_t index = 0U; index < sizeof(Integer); ++index) {
            wide |= static_cast<std::uint64_t>(
                        std::to_integer<unsigned char>(
                            input_[offset_ + index]))
                    << (index * 8U);
        }
        offset_ += sizeof(Integer);
        *output = static_cast<Integer>(wide);
        return true;
    }

    template <std::size_t Size>
    [[nodiscard]] bool ReadArray(
        std::array<std::byte, Size>* output) noexcept {
        if (output == nullptr || Remaining() < Size) {
            return false;
        }
        std::copy_n(input_.begin() + static_cast<std::ptrdiff_t>(offset_),
                    Size, output->begin());
        offset_ += Size;
        return true;
    }

    [[nodiscard]] bool ReadString(std::string* output) {
        std::uint32_t size = 0U;
        if (!ReadInteger(&size) ||
            size > kProductionRouteMaximumEndpointBytesV1 ||
            Remaining() < size) {
            return false;
        }
        const auto* const begin =
            reinterpret_cast<const char*>(input_.data() + offset_);
        output->assign(begin, static_cast<std::size_t>(size));
        offset_ += size;
        return true;
    }

    [[nodiscard]] std::size_t offset() const noexcept {
        return offset_;
    }

private:
    [[nodiscard]] std::size_t Remaining() const noexcept {
        return input_.size() - offset_;
    }

    std::span<const std::byte> input_;
    std::size_t offset_ = 0U;
};

}  // namespace

std::string_view ProductionRouteManifestErrorNameV1(
    ProductionRouteManifestErrorV1 error) noexcept {
    switch (error) {
        case ProductionRouteManifestErrorV1::kNone:
            return "none";
        case ProductionRouteManifestErrorV1::kNullOutput:
            return "null_output";
        case ProductionRouteManifestErrorV1::kUnsupportedVersion:
            return "unsupported_version";
        case ProductionRouteManifestErrorV1::kInvalidState:
            return "invalid_state";
        case ProductionRouteManifestErrorV1::kInvalidGeneration:
            return "invalid_generation";
        case ProductionRouteManifestErrorV1::kInvalidIdentity:
            return "invalid_identity";
        case ProductionRouteManifestErrorV1::kInvalidDate:
            return "invalid_date";
        case ProductionRouteManifestErrorV1::kInvalidFatalReason:
            return "invalid_fatal_reason";
        case ProductionRouteManifestErrorV1::kInvalidRegistry:
            return "invalid_registry";
        case ProductionRouteManifestErrorV1::kInvalidDigest:
            return "invalid_digest";
        case ProductionRouteManifestErrorV1::kInvalidSourceSet:
            return "invalid_source_set";
        case ProductionRouteManifestErrorV1::kInvalidSource:
            return "invalid_source";
        case ProductionRouteManifestErrorV1::kInvalidEndpoint:
            return "invalid_endpoint";
        case ProductionRouteManifestErrorV1::kDuplicateEndpoint:
            return "duplicate_endpoint";
        case ProductionRouteManifestErrorV1::kEncodedSizeExceeded:
            return "encoded_size_exceeded";
        case ProductionRouteManifestErrorV1::kTruncated:
            return "truncated";
        case ProductionRouteManifestErrorV1::kTrailingBytes:
            return "trailing_bytes";
        case ProductionRouteManifestErrorV1::kInvalidMagic:
            return "invalid_magic";
        case ProductionRouteManifestErrorV1::kInvalidReservedField:
            return "invalid_reserved_field";
        case ProductionRouteManifestErrorV1::kLengthMismatch:
            return "length_mismatch";
        case ProductionRouteManifestErrorV1::kDigestMismatch:
            return "digest_mismatch";
        case ProductionRouteManifestErrorV1::kAllocationFailure:
            return "allocation_failure";
    }
    return "unknown";
}

l2flow::common::Sha256Digest
ComputeProductionRouteClockEpochIdentitySha256V1(
    const l2flow::canonical::ClockEpochIdentityV1& identity) noexcept {
    std::array<std::byte, sizeof(identity.algorithm)> algorithm{};
    std::uint32_t value = identity.algorithm;
    for (std::size_t index = 0U; index < algorithm.size(); ++index) {
        algorithm[index] = std::byte{
            static_cast<unsigned char>(value & 0xffU)};
        value >>= 8U;
    }

    l2flow::common::Sha256Hasher hasher;
    const auto* const domain = reinterpret_cast<const std::byte*>(
        kClockIdentityDomain.data());
    static_cast<void>(hasher.Update(std::span<const std::byte>(
        domain, kClockIdentityDomain.size())));
    static_cast<void>(hasher.Update(algorithm));
    static_cast<void>(hasher.Update(identity.digest));
    l2flow::common::Sha256Digest digest{};
    static_cast<void>(hasher.Finalize(&digest));
    return digest;
}

ProductionRouteManifestErrorV1 ValidateProductionRouteManifestV1(
    const ProductionRouteManifestV1& manifest) noexcept {
    if (manifest.wire_version != kProductionRouteWireVersionV1) {
        return ProductionRouteManifestErrorV1::kUnsupportedVersion;
    }
    if (manifest.state != ProductionRouteStateV1::kActive &&
        manifest.state != ProductionRouteStateV1::kFatal) {
        return ProductionRouteManifestErrorV1::kInvalidState;
    }
    if (manifest.generation == 0U ||
        manifest.previous_generation >= manifest.generation ||
        (manifest.state == ProductionRouteStateV1::kActive &&
         manifest.generation ==
             std::numeric_limits<std::uint64_t>::max())) {
        return ProductionRouteManifestErrorV1::kInvalidGeneration;
    }
    if (IsZero(manifest.route_instance)) {
        return ProductionRouteManifestErrorV1::kInvalidIdentity;
    }
    if (!IsValidDate(manifest.trade_date)) {
        return ProductionRouteManifestErrorV1::kInvalidDate;
    }
    if ((manifest.state == ProductionRouteStateV1::kActive &&
         manifest.fatal_reason_code != 0U) ||
        (manifest.state == ProductionRouteStateV1::kFatal &&
         manifest.fatal_reason_code == 0U)) {
        return ProductionRouteManifestErrorV1::kInvalidFatalReason;
    }
    if (manifest.registry_version == 0U ||
        IsZero(manifest.registry_sha256)) {
        return ProductionRouteManifestErrorV1::kInvalidRegistry;
    }
    if (IsZero(manifest.schema_sha256) ||
        IsZero(manifest.build_sha256) ||
        IsZero(manifest.config_sha256)) {
        return ProductionRouteManifestErrorV1::kInvalidDigest;
    }

    for (std::size_t index = 0U; index < manifest.sources.size(); ++index) {
        const ProductionRouteSourceV1& source = manifest.sources[index];
        if (source.source_stream_id !=
            kProductionRouteSourceStreamIdsV1[index]) {
            return ProductionRouteManifestErrorV1::kInvalidSourceSet;
        }
        const bool cursor_zero = source.durable_ingress_sequence == 0U;
        if (!IsValidDate(source.capture_date) ||
            IsZero(source.stream_day_id) ||
            IsZero(source.writer_instance) ||
            source.source_generation == 0U ||
            source.canonical_generation == 0U ||
            cursor_zero != (source.durable_global_wal_pos == 0U) ||
            IsZero(source.clock_epoch_identity_sha256)) {
            return ProductionRouteManifestErrorV1::kInvalidSource;
        }
    }

    const std::array<std::string_view,
                     kProductionRouteEndpointCountV1>
        endpoints{{manifest.endpoints.canonical,
                   manifest.endpoints.history,
                   manifest.endpoints.state,
                   manifest.endpoints.factor}};
    for (std::size_t index = 0U; index < endpoints.size(); ++index) {
        const bool required = index == 0U;
        if ((required || !endpoints[index].empty()) &&
            !IsNormalizedAbsoluteEndpoint(endpoints[index])) {
            return ProductionRouteManifestErrorV1::kInvalidEndpoint;
        }
        for (std::size_t prior = 0U; prior < index; ++prior) {
            if (!endpoints[index].empty() &&
                endpoints[index] == endpoints[prior]) {
                return ProductionRouteManifestErrorV1::kDuplicateEndpoint;
            }
        }
    }
    return ProductionRouteManifestErrorV1::kNone;
}

ProductionRouteManifestErrorV1 EncodeProductionRouteManifestV1(
    const ProductionRouteManifestV1& manifest,
    std::vector<std::byte>* output) noexcept {
    if (output == nullptr) {
        return ProductionRouteManifestErrorV1::kNullOutput;
    }
    const ProductionRouteManifestErrorV1 validation =
        ValidateProductionRouteManifestV1(manifest);
    if (validation != ProductionRouteManifestErrorV1::kNone) {
        return validation;
    }

    try {
        std::vector<std::byte> encoded;
        encoded.reserve(1024U);
        AppendArray(kWireMagic, &encoded);
        AppendLittleEndian(manifest.wire_version, &encoded);
        AppendLittleEndian<std::uint16_t>(0U, &encoded);
        AppendLittleEndian<std::uint32_t>(0U, &encoded);
        encoded.push_back(std::byte{
            static_cast<unsigned char>(manifest.state)});
        encoded.push_back(std::byte{
            static_cast<unsigned char>(manifest.sources.size())});
        encoded.push_back(std::byte{
            static_cast<unsigned char>(kProductionRouteEndpointCountV1)});
        encoded.push_back(std::byte{0});
        AppendLittleEndian(manifest.generation, &encoded);
        AppendLittleEndian(manifest.previous_generation, &encoded);
        AppendArray(manifest.route_instance, &encoded);
        AppendLittleEndian(manifest.trade_date, &encoded);
        AppendLittleEndian(manifest.fatal_reason_code, &encoded);
        AppendLittleEndian(manifest.registry_version, &encoded);
        AppendArray(manifest.registry_sha256, &encoded);
        AppendArray(manifest.schema_sha256, &encoded);
        AppendArray(manifest.build_sha256, &encoded);
        AppendArray(manifest.config_sha256, &encoded);

        for (const ProductionRouteSourceV1& source : manifest.sources) {
            AppendLittleEndian(source.source_stream_id, &encoded);
            AppendLittleEndian(source.capture_date, &encoded);
            AppendArray(source.stream_day_id, &encoded);
            AppendArray(source.writer_instance, &encoded);
            AppendLittleEndian(source.source_generation, &encoded);
            AppendLittleEndian(source.canonical_generation, &encoded);
            AppendLittleEndian(source.durable_ingress_sequence, &encoded);
            AppendLittleEndian(source.durable_global_wal_pos, &encoded);
            AppendArray(source.clock_epoch_identity_sha256, &encoded);
        }

        AppendString(manifest.endpoints.canonical, &encoded);
        AppendString(manifest.endpoints.history, &encoded);
        AppendString(manifest.endpoints.state, &encoded);
        AppendString(manifest.endpoints.factor, &encoded);

        if (encoded.size() >
            kProductionRouteMaximumEncodedBytesV1 - kDigestBytes ||
            encoded.size() + kDigestBytes >
                std::numeric_limits<std::uint32_t>::max()) {
            return ProductionRouteManifestErrorV1::kEncodedSizeExceeded;
        }
        const std::uint32_t total_bytes = static_cast<std::uint32_t>(
            encoded.size() + kDigestBytes);
        PatchU32LittleEndian(total_bytes, kTotalBytesOffset, &encoded);
        const l2flow::common::Sha256Digest digest = WireDigest(encoded);
        AppendArray(digest, &encoded);
        output->swap(encoded);
        return ProductionRouteManifestErrorV1::kNone;
    } catch (const std::bad_alloc&) {
        return ProductionRouteManifestErrorV1::kAllocationFailure;
    } catch (...) {
        return ProductionRouteManifestErrorV1::kAllocationFailure;
    }
}

ProductionRouteManifestErrorV1 DecodeProductionRouteManifestV1(
    std::span<const std::byte> encoded,
    ProductionRouteManifestV1* output) noexcept {
    if (output == nullptr) {
        return ProductionRouteManifestErrorV1::kNullOutput;
    }
    constexpr std::size_t minimum_bytes =
        8U + 2U + 2U + 4U + 4U + 8U + 8U + 16U + 4U + 4U +
        8U + (4U * 32U) + (4U * 104U) + (4U * 4U) + kDigestBytes;
    if (encoded.size() < minimum_bytes) {
        return ProductionRouteManifestErrorV1::kTruncated;
    }
    if (encoded.size() > kProductionRouteMaximumEncodedBytesV1) {
        return ProductionRouteManifestErrorV1::kEncodedSizeExceeded;
    }

    const std::span<const std::byte> body =
        encoded.first(encoded.size() - kDigestBytes);
    const std::span<const std::byte> digest_bytes =
        encoded.last(kDigestBytes);
    const l2flow::common::Sha256Digest expected = WireDigest(body);
    if (!std::equal(
            expected.begin(), expected.end(), digest_bytes.begin())) {
        return ProductionRouteManifestErrorV1::kDigestMismatch;
    }

    try {
        Reader reader(body);
        std::array<std::byte, 8U> magic{};
        std::uint16_t reserved16 = 0U;
        std::uint32_t total_bytes = 0U;
        std::uint8_t state = 0U;
        std::uint8_t source_count = 0U;
        std::uint8_t endpoint_count = 0U;
        std::uint8_t reserved8 = 0U;
        ProductionRouteManifestV1 decoded;
        if (!reader.ReadArray(&magic) ||
            !reader.ReadInteger(&decoded.wire_version) ||
            !reader.ReadInteger(&reserved16) ||
            !reader.ReadInteger(&total_bytes) ||
            !reader.ReadInteger(&state) ||
            !reader.ReadInteger(&source_count) ||
            !reader.ReadInteger(&endpoint_count) ||
            !reader.ReadInteger(&reserved8)) {
            return ProductionRouteManifestErrorV1::kTruncated;
        }
        if (magic != kWireMagic) {
            return ProductionRouteManifestErrorV1::kInvalidMagic;
        }
        if (reserved16 != 0U || reserved8 != 0U) {
            return ProductionRouteManifestErrorV1::kInvalidReservedField;
        }
        if (total_bytes != encoded.size()) {
            return ProductionRouteManifestErrorV1::kLengthMismatch;
        }
        if (source_count != kProductionRouteSourceCountV1 ||
            endpoint_count != kProductionRouteEndpointCountV1) {
            return ProductionRouteManifestErrorV1::kLengthMismatch;
        }
        decoded.state = static_cast<ProductionRouteStateV1>(state);
        if (!reader.ReadInteger(&decoded.generation) ||
            !reader.ReadInteger(&decoded.previous_generation) ||
            !reader.ReadArray(&decoded.route_instance) ||
            !reader.ReadInteger(&decoded.trade_date) ||
            !reader.ReadInteger(&decoded.fatal_reason_code) ||
            !reader.ReadInteger(&decoded.registry_version) ||
            !reader.ReadArray(&decoded.registry_sha256) ||
            !reader.ReadArray(&decoded.schema_sha256) ||
            !reader.ReadArray(&decoded.build_sha256) ||
            !reader.ReadArray(&decoded.config_sha256)) {
            return ProductionRouteManifestErrorV1::kTruncated;
        }
        for (ProductionRouteSourceV1& source : decoded.sources) {
            if (!reader.ReadInteger(&source.source_stream_id) ||
                !reader.ReadInteger(&source.capture_date) ||
                !reader.ReadArray(&source.stream_day_id) ||
                !reader.ReadArray(&source.writer_instance) ||
                !reader.ReadInteger(&source.source_generation) ||
                !reader.ReadInteger(&source.canonical_generation) ||
                !reader.ReadInteger(&source.durable_ingress_sequence) ||
                !reader.ReadInteger(&source.durable_global_wal_pos) ||
                !reader.ReadArray(&source.clock_epoch_identity_sha256)) {
                return ProductionRouteManifestErrorV1::kTruncated;
            }
        }
        if (!reader.ReadString(&decoded.endpoints.canonical) ||
            !reader.ReadString(&decoded.endpoints.history) ||
            !reader.ReadString(&decoded.endpoints.state) ||
            !reader.ReadString(&decoded.endpoints.factor)) {
            return ProductionRouteManifestErrorV1::kTruncated;
        }
        if (reader.offset() != body.size()) {
            return ProductionRouteManifestErrorV1::kTrailingBytes;
        }
        const ProductionRouteManifestErrorV1 validation =
            ValidateProductionRouteManifestV1(decoded);
        if (validation != ProductionRouteManifestErrorV1::kNone) {
            return validation;
        }
        *output = std::move(decoded);
        return ProductionRouteManifestErrorV1::kNone;
    } catch (const std::bad_alloc&) {
        return ProductionRouteManifestErrorV1::kAllocationFailure;
    } catch (...) {
        return ProductionRouteManifestErrorV1::kAllocationFailure;
    }
}

}  // namespace l2flow::route
