#include "l2flow/ingress/reserve_headers_v1.h"

#include "l2flow/common/crc32c.h"

#include <algorithm>
#include <array>
#include <optional>
#include <utility>

namespace l2flow::ingress {
namespace {

constexpr std::size_t kCrcBytes = sizeof(std::uint32_t);
constexpr std::string_view kInodeFilenamePrefix = "inode-";
constexpr std::string_view kInodeFilenameSuffix = ".reserve";
constexpr std::size_t kIdentityHexBytes = 32U;
constexpr std::size_t kIndexDecimalBytes = 8U;

static_assert(
    reserve_headers_v1_offset::file::kReservedTail <
    kReserveHeaderV1Bytes);
static_assert(
    reserve_headers_v1_offset::inode::kReservedTail <
    kReserveHeaderV1Bytes);
static_assert(
    kInodeFilenamePrefix.size() + kIdentityHexBytes + 1U +
        kIndexDecimalBytes + kInodeFilenameSuffix.size() ==
    kReserveInodeFilenameV1Bytes);

void StoreU16Le(
    std::uint16_t value,
    std::span<std::byte> output,
    std::size_t offset) noexcept {
    output[offset] =
        static_cast<std::byte>(value & 0xffU);
    output[offset + 1U] =
        static_cast<std::byte>((value >> 8U) & 0xffU);
}

void StoreU32Le(
    std::uint32_t value,
    std::span<std::byte> output,
    std::size_t offset) noexcept {
    for (std::size_t index = 0U;
         index < sizeof(value);
         ++index) {
        const unsigned int shift =
            static_cast<unsigned int>(index * 8U);
        output[offset + index] =
            static_cast<std::byte>(
                (value >> shift) & 0xffU);
    }
}

void StoreU64Le(
    std::uint64_t value,
    std::span<std::byte> output,
    std::size_t offset) noexcept {
    for (std::size_t index = 0U;
         index < sizeof(value);
         ++index) {
        const unsigned int shift =
            static_cast<unsigned int>(index * 8U);
        output[offset + index] =
            static_cast<std::byte>(
                (value >> shift) & 0xffU);
    }
}

[[nodiscard]] std::uint16_t LoadU16Le(
    std::span<const std::byte> input,
    std::size_t offset) noexcept {
    return static_cast<std::uint16_t>(
        std::to_integer<std::uint16_t>(input[offset]) |
        static_cast<std::uint16_t>(
            std::to_integer<std::uint16_t>(
                input[offset + 1U])
            << 8U));
}

[[nodiscard]] std::uint32_t LoadU32Le(
    std::span<const std::byte> input,
    std::size_t offset) noexcept {
    std::uint32_t value = 0U;
    for (std::size_t index = 0U;
         index < sizeof(value);
         ++index) {
        const unsigned int shift =
            static_cast<unsigned int>(index * 8U);
        value |=
            std::to_integer<std::uint32_t>(
                input[offset + index])
            << shift;
    }
    return value;
}

[[nodiscard]] std::uint64_t LoadU64Le(
    std::span<const std::byte> input,
    std::size_t offset) noexcept {
    std::uint64_t value = 0U;
    for (std::size_t index = 0U;
         index < sizeof(value);
         ++index) {
        const unsigned int shift =
            static_cast<unsigned int>(index * 8U);
        value |=
            std::to_integer<std::uint64_t>(
                input[offset + index])
            << shift;
    }
    return value;
}

template <std::size_t Size>
void StoreBytes(
    const std::array<std::byte, Size>& value,
    std::span<std::byte> output,
    std::size_t offset) noexcept {
    std::copy(
        value.begin(),
        value.end(),
        output.data() + offset);
}

template <std::size_t Size>
void LoadBytes(
    std::span<const std::byte> input,
    std::size_t offset,
    std::array<std::byte, Size>* output) noexcept {
    std::copy_n(
        input.data() + offset,
        Size,
        output->begin());
}

[[nodiscard]] bool IsZero(
    std::span<const std::byte> bytes) noexcept {
    return std::all_of(
        bytes.begin(),
        bytes.end(),
        [](std::byte value) {
            return value == std::byte{0};
        });
}

template <std::size_t Size>
[[nodiscard]] bool IsZero(
    const std::array<std::byte, Size>& value) noexcept {
    return IsZero(std::span<const std::byte>(value));
}

[[nodiscard]] bool IsZeroRange(
    std::span<const std::byte> wire,
    std::size_t offset,
    std::size_t size) noexcept {
    return IsZero(wire.subspan(offset, size));
}

[[nodiscard]] std::uint32_t ComputeCrcWithZeroedField(
    std::span<const std::byte> wire,
    std::size_t crc_offset) noexcept {
    constexpr std::array<std::byte, kCrcBytes> zero_crc{};
    l2flow::common::Crc32cState state;
    state.Update(wire.first(crc_offset));
    state.Update(zero_crc);
    state.Update(wire.subspan(crc_offset + kCrcBytes));
    return state.Finalize();
}

[[nodiscard]] ReserveHeaderV1Error ValidatePoolIdentity(
    const ReserveAllocationPoolIdentityV1& pool) noexcept {
    if (l2flow::common::IsZeroIdentity(
            pool.reserve_state_uuid)) {
        return ReserveHeaderV1Error::kInvalidReserveUuid;
    }
    if (pool.device_id == 0U) {
        return ReserveHeaderV1Error::
            kInvalidDeviceIdentity;
    }
    if (IsZero(pool.quota_identity_sha256)) {
        return ReserveHeaderV1Error::
            kInvalidQuotaIdentity;
    }
    if (IsZero(pool.mount_identity_sha256)) {
        return ReserveHeaderV1Error::
            kInvalidMountIdentity;
    }
    return ReserveHeaderV1Error::kNone;
}

[[nodiscard]] ReserveHeaderV1Error
ValidateFileLogical(
    const ReserveFileHeaderV1& header) noexcept {
    const ReserveHeaderV1Error identity =
        ValidatePoolIdentity(header.pool);
    if (identity != ReserveHeaderV1Error::kNone) {
        return identity;
    }
    if (header.declared_bytes < kReserveHeaderV1Bytes) {
        return ReserveHeaderV1Error::
            kInvalidDeclaredBytes;
    }
    return ReserveHeaderV1Error::kNone;
}

[[nodiscard]] ReserveHeaderV1Error
ValidateInodeLogical(
    const ReserveInodeHeaderV1& header) noexcept {
    const ReserveHeaderV1Error identity =
        ValidatePoolIdentity(header.pool);
    if (identity != ReserveHeaderV1Error::kNone) {
        return identity;
    }
    if (header.declared_inode_reserve_count == 0U ||
        header.declared_inode_reserve_count >
            kReserveInodeV1MaxCount) {
        return ReserveHeaderV1Error::
            kInvalidInodeCount;
    }
    if (header.inode_index >=
        header.declared_inode_reserve_count) {
        return ReserveHeaderV1Error::
            kInvalidInodeIndex;
    }
    return ReserveHeaderV1Error::kNone;
}

void StorePoolIdentity(
    const ReserveAllocationPoolIdentityV1& pool,
    std::span<std::byte> wire) noexcept {
    using namespace reserve_headers_v1_offset::common;
    StoreBytes(
        pool.reserve_state_uuid,
        wire,
        kReserveStateUuid);
    StoreU64Le(pool.device_id, wire, kDeviceId);
    StoreBytes(
        pool.quota_identity_sha256,
        wire,
        kQuotaIdentitySha256);
    StoreBytes(
        pool.mount_identity_sha256,
        wire,
        kMountIdentitySha256);
}

void LoadPoolIdentity(
    std::span<const std::byte> wire,
    ReserveAllocationPoolIdentityV1* pool) noexcept {
    using namespace reserve_headers_v1_offset::common;
    LoadBytes(
        wire,
        kReserveStateUuid,
        &pool->reserve_state_uuid);
    pool->device_id = LoadU64Le(wire, kDeviceId);
    LoadBytes(
        wire,
        kQuotaIdentitySha256,
        &pool->quota_identity_sha256);
    LoadBytes(
        wire,
        kMountIdentitySha256,
        &pool->mount_identity_sha256);
}

void StoreCommonPrefix(
    const std::array<std::byte, 8U>& magic,
    std::span<std::byte> wire) noexcept {
    using namespace reserve_headers_v1_offset::common;
    StoreBytes(magic, wire, kMagic);
    StoreU16Le(kReserveHeadersV1Version, wire, kVersion);
    wire[kEndian] =
        static_cast<std::byte>(
            kReserveHeadersV1LittleEndian);
    StoreU32Le(
        static_cast<std::uint32_t>(
            kReserveHeaderV1Bytes),
        wire,
        kHeaderSize);
}

[[nodiscard]] ReserveHeaderV1Error ValidateCommonPrefix(
    std::span<const std::byte> wire,
    const std::array<std::byte, 8U>& magic) noexcept {
    using namespace reserve_headers_v1_offset::common;
    if (!std::equal(
            magic.begin(),
            magic.end(),
            wire.begin() + kMagic)) {
        return ReserveHeaderV1Error::kInvalidMagic;
    }
    if (LoadU16Le(wire, kVersion) !=
        kReserveHeadersV1Version) {
        return ReserveHeaderV1Error::
            kUnsupportedVersion;
    }
    if (std::to_integer<std::uint8_t>(
            wire[kEndian]) !=
        kReserveHeadersV1LittleEndian) {
        return ReserveHeaderV1Error::kInvalidEndian;
    }
    if (LoadU32Le(wire, kHeaderSize) !=
        kReserveHeaderV1Bytes) {
        return ReserveHeaderV1Error::
            kInvalidHeaderSize;
    }
    if (wire[kReserved0] != std::byte{0}) {
        return ReserveHeaderV1Error::
            kNonzeroReserved;
    }
    return ReserveHeaderV1Error::kNone;
}

void SetError(
    std::string* error,
    std::string message) noexcept {
    if (error == nullptr) {
        return;
    }
    try {
        *error = std::move(message);
    } catch (...) {
    }
}

}  // namespace

std::string_view ReserveHeaderV1ErrorName(
    ReserveHeaderV1Error error) noexcept {
    switch (error) {
        case ReserveHeaderV1Error::kNone:
            return "none";
        case ReserveHeaderV1Error::kNullOutput:
            return "null output";
        case ReserveHeaderV1Error::kInvalidWireSize:
            return "invalid wire size";
        case ReserveHeaderV1Error::kInvalidMagic:
            return "invalid magic";
        case ReserveHeaderV1Error::kUnsupportedVersion:
            return "unsupported version";
        case ReserveHeaderV1Error::kInvalidEndian:
            return "invalid endian";
        case ReserveHeaderV1Error::kInvalidHeaderSize:
            return "invalid header size";
        case ReserveHeaderV1Error::kNonzeroReserved:
            return "nonzero reserved";
        case ReserveHeaderV1Error::kCrcMismatch:
            return "CRC mismatch";
        case ReserveHeaderV1Error::kInvalidReserveUuid:
            return "invalid reserve UUID";
        case ReserveHeaderV1Error::kInvalidDeviceIdentity:
            return "invalid device identity";
        case ReserveHeaderV1Error::kInvalidQuotaIdentity:
            return "invalid quota identity";
        case ReserveHeaderV1Error::kInvalidMountIdentity:
            return "invalid mount identity";
        case ReserveHeaderV1Error::kInvalidDeclaredBytes:
            return "invalid declared bytes";
        case ReserveHeaderV1Error::kInvalidInodeCount:
            return "invalid inode count";
        case ReserveHeaderV1Error::kInvalidInodeIndex:
            return "invalid inode index";
        case ReserveHeaderV1Error::kIdentityMismatch:
            return "identity mismatch";
        case ReserveHeaderV1Error::kDeclaredBytesMismatch:
            return "declared bytes mismatch";
        case ReserveHeaderV1Error::kInodeCountMismatch:
            return "inode count mismatch";
        case ReserveHeaderV1Error::kFilenameMismatch:
            return "filename mismatch";
        case ReserveHeaderV1Error::kAllocationFailure:
            return "allocation failure";
    }
    return "unknown reserve header error";
}

ReserveHeaderV1Error EncodeReserveFileHeaderV1(
    const ReserveFileHeaderV1& header,
    ReserveHeaderWireV1* wire) noexcept {
    if (wire == nullptr) {
        return ReserveHeaderV1Error::kNullOutput;
    }
    const ReserveHeaderV1Error validation =
        ValidateFileLogical(header);
    if (validation != ReserveHeaderV1Error::kNone) {
        return validation;
    }

    ReserveHeaderWireV1 encoded{};
    std::span<std::byte> bytes(encoded);
    StoreCommonPrefix(
        kReserveFileHeaderV1Magic,
        bytes);
    StorePoolIdentity(header.pool, bytes);
    StoreU64Le(
        header.declared_bytes,
        bytes,
        reserve_headers_v1_offset::file::
            kDeclaredBytes);
    const std::uint32_t crc =
        ComputeCrcWithZeroedField(
            encoded,
            reserve_headers_v1_offset::file::
                kHeaderCrc32c);
    StoreU32Le(
        crc,
        bytes,
        reserve_headers_v1_offset::file::
            kHeaderCrc32c);
    *wire = encoded;
    return ReserveHeaderV1Error::kNone;
}

ReserveHeaderV1Error DecodeReserveFileHeaderV1(
    std::span<const std::byte> wire,
    ReserveFileHeaderV1* header) noexcept {
    if (header == nullptr) {
        return ReserveHeaderV1Error::kNullOutput;
    }
    if (wire.size() != kReserveHeaderV1Bytes) {
        return ReserveHeaderV1Error::
            kInvalidWireSize;
    }
    const ReserveHeaderV1Error common =
        ValidateCommonPrefix(
            wire,
            kReserveFileHeaderV1Magic);
    if (common != ReserveHeaderV1Error::kNone) {
        return common;
    }
    if (!IsZeroRange(
            wire,
            reserve_headers_v1_offset::file::
                kReservedTail,
            kReserveHeaderV1Bytes -
                reserve_headers_v1_offset::file::
                    kReservedTail)) {
        return ReserveHeaderV1Error::
            kNonzeroReserved;
    }
    const std::uint32_t stored_crc =
        LoadU32Le(
            wire,
            reserve_headers_v1_offset::file::
                kHeaderCrc32c);
    if (stored_crc !=
        ComputeCrcWithZeroedField(
            wire,
            reserve_headers_v1_offset::file::
                kHeaderCrc32c)) {
        return ReserveHeaderV1Error::kCrcMismatch;
    }

    ReserveFileHeaderV1 decoded{};
    LoadPoolIdentity(wire, &decoded.pool);
    decoded.declared_bytes =
        LoadU64Le(
            wire,
            reserve_headers_v1_offset::file::
                kDeclaredBytes);
    const ReserveHeaderV1Error validation =
        ValidateFileLogical(decoded);
    if (validation != ReserveHeaderV1Error::kNone) {
        return validation;
    }
    *header = decoded;
    return ReserveHeaderV1Error::kNone;
}

ReserveHeaderV1Error ValidateReserveFileHeaderV1(
    std::span<const std::byte> wire) noexcept {
    ReserveFileHeaderV1 decoded{};
    return DecodeReserveFileHeaderV1(wire, &decoded);
}

ReserveHeaderV1Error
ValidateReserveFileHeaderBindingV1(
    const ReserveFileHeaderV1& header,
    const ReserveAllocationPoolIdentityV1& expected_pool,
    std::uint64_t actual_file_size) noexcept {
    const ReserveHeaderV1Error header_error =
        ValidateFileLogical(header);
    if (header_error != ReserveHeaderV1Error::kNone) {
        return header_error;
    }
    const ReserveHeaderV1Error expected_error =
        ValidatePoolIdentity(expected_pool);
    if (expected_error != ReserveHeaderV1Error::kNone) {
        return expected_error;
    }
    if (header.pool != expected_pool) {
        return ReserveHeaderV1Error::
            kIdentityMismatch;
    }
    if (header.declared_bytes != actual_file_size) {
        return ReserveHeaderV1Error::
            kDeclaredBytesMismatch;
    }
    return ReserveHeaderV1Error::kNone;
}

ReserveHeaderV1Error EncodeReserveInodeHeaderV1(
    const ReserveInodeHeaderV1& header,
    ReserveHeaderWireV1* wire) noexcept {
    if (wire == nullptr) {
        return ReserveHeaderV1Error::kNullOutput;
    }
    const ReserveHeaderV1Error validation =
        ValidateInodeLogical(header);
    if (validation != ReserveHeaderV1Error::kNone) {
        return validation;
    }

    ReserveHeaderWireV1 encoded{};
    std::span<std::byte> bytes(encoded);
    StoreCommonPrefix(
        kReserveInodeHeaderV1Magic,
        bytes);
    StorePoolIdentity(header.pool, bytes);
    StoreU32Le(
        header.inode_index,
        bytes,
        reserve_headers_v1_offset::inode::kIndex);
    StoreU32Le(
        header.declared_inode_reserve_count,
        bytes,
        reserve_headers_v1_offset::inode::kCount);
    const std::uint32_t crc =
        ComputeCrcWithZeroedField(
            encoded,
            reserve_headers_v1_offset::inode::
                kHeaderCrc32c);
    StoreU32Le(
        crc,
        bytes,
        reserve_headers_v1_offset::inode::
            kHeaderCrc32c);
    *wire = encoded;
    return ReserveHeaderV1Error::kNone;
}

ReserveHeaderV1Error DecodeReserveInodeHeaderV1(
    std::span<const std::byte> wire,
    ReserveInodeHeaderV1* header) noexcept {
    if (header == nullptr) {
        return ReserveHeaderV1Error::kNullOutput;
    }
    if (wire.size() != kReserveHeaderV1Bytes) {
        return ReserveHeaderV1Error::
            kInvalidWireSize;
    }
    const ReserveHeaderV1Error common =
        ValidateCommonPrefix(
            wire,
            kReserveInodeHeaderV1Magic);
    if (common != ReserveHeaderV1Error::kNone) {
        return common;
    }
    if (!IsZeroRange(
            wire,
            reserve_headers_v1_offset::inode::
                kReservedTail,
            kReserveHeaderV1Bytes -
                reserve_headers_v1_offset::inode::
                    kReservedTail)) {
        return ReserveHeaderV1Error::
            kNonzeroReserved;
    }
    const std::uint32_t stored_crc =
        LoadU32Le(
            wire,
            reserve_headers_v1_offset::inode::
                kHeaderCrc32c);
    if (stored_crc !=
        ComputeCrcWithZeroedField(
            wire,
            reserve_headers_v1_offset::inode::
                kHeaderCrc32c)) {
        return ReserveHeaderV1Error::kCrcMismatch;
    }

    ReserveInodeHeaderV1 decoded{};
    LoadPoolIdentity(wire, &decoded.pool);
    decoded.inode_index =
        LoadU32Le(
            wire,
            reserve_headers_v1_offset::inode::kIndex);
    decoded.declared_inode_reserve_count =
        LoadU32Le(
            wire,
            reserve_headers_v1_offset::inode::kCount);
    const ReserveHeaderV1Error validation =
        ValidateInodeLogical(decoded);
    if (validation != ReserveHeaderV1Error::kNone) {
        return validation;
    }
    *header = decoded;
    return ReserveHeaderV1Error::kNone;
}

ReserveHeaderV1Error ValidateReserveInodeHeaderV1(
    std::span<const std::byte> wire) noexcept {
    ReserveInodeHeaderV1 decoded{};
    return DecodeReserveInodeHeaderV1(wire, &decoded);
}

ReserveHeaderV1Error
ValidateReserveInodeHeaderBindingV1(
    const ReserveInodeHeaderV1& header,
    const ReserveAllocationPoolIdentityV1& expected_pool,
    std::uint32_t expected_count,
    std::string_view exact_filename) noexcept {
    const ReserveHeaderV1Error header_error =
        ValidateInodeLogical(header);
    if (header_error != ReserveHeaderV1Error::kNone) {
        return header_error;
    }
    const ReserveHeaderV1Error expected_error =
        ValidatePoolIdentity(expected_pool);
    if (expected_error != ReserveHeaderV1Error::kNone) {
        return expected_error;
    }
    if (expected_count == 0U ||
        expected_count > kReserveInodeV1MaxCount) {
        return ReserveHeaderV1Error::
            kInvalidInodeCount;
    }
    if (header.pool != expected_pool) {
        return ReserveHeaderV1Error::
            kIdentityMismatch;
    }
    if (header.declared_inode_reserve_count !=
        expected_count) {
        return ReserveHeaderV1Error::
            kInodeCountMismatch;
    }

    l2flow::common::Identity128 parsed_uuid{};
    std::uint32_t parsed_index = 0U;
    if (ParseReserveInodeFilenameV1(
            exact_filename,
            &parsed_uuid,
            &parsed_index) !=
            ReserveHeaderV1Error::kNone ||
        parsed_uuid != header.pool.reserve_state_uuid ||
        parsed_index != header.inode_index) {
        return ReserveHeaderV1Error::
            kFilenameMismatch;
    }
    return ReserveHeaderV1Error::kNone;
}

ReserveHeaderV1Error FormatReserveInodeFilenameV1(
    const l2flow::common::Identity128& reserve_state_uuid,
    std::uint32_t inode_index,
    std::string* filename) noexcept {
    if (filename == nullptr) {
        return ReserveHeaderV1Error::kNullOutput;
    }
    if (l2flow::common::IsZeroIdentity(
            reserve_state_uuid)) {
        return ReserveHeaderV1Error::
            kInvalidReserveUuid;
    }
    if (inode_index >= kReserveInodeV1MaxCount) {
        return ReserveHeaderV1Error::
            kInvalidInodeIndex;
    }
    try {
        std::string encoded;
        encoded.reserve(kReserveInodeFilenameV1Bytes);
        encoded.append(kInodeFilenamePrefix);
        encoded.append(
            l2flow::common::Identity128Hex(
                reserve_state_uuid));
        encoded.push_back('-');
        std::array<char, kIndexDecimalBytes> digits{};
        std::uint32_t value = inode_index;
        for (std::size_t offset = 0U;
             offset < digits.size();
             ++offset) {
            const std::size_t index =
                digits.size() - 1U - offset;
            digits[index] = static_cast<char>(
                '0' + (value % 10U));
            value /= 10U;
        }
        encoded.append(digits.data(), digits.size());
        encoded.append(kInodeFilenameSuffix);
        if (encoded.size() !=
            kReserveInodeFilenameV1Bytes) {
            return ReserveHeaderV1Error::
                kFilenameMismatch;
        }
        filename->swap(encoded);
        return ReserveHeaderV1Error::kNone;
    } catch (...) {
        return ReserveHeaderV1Error::
            kAllocationFailure;
    }
}

ReserveHeaderV1Error ParseReserveInodeFilenameV1(
    std::string_view filename,
    l2flow::common::Identity128* reserve_state_uuid,
    std::uint32_t* inode_index) noexcept {
    if (reserve_state_uuid == nullptr ||
        inode_index == nullptr) {
        return ReserveHeaderV1Error::kNullOutput;
    }
    if (filename.size() !=
            kReserveInodeFilenameV1Bytes ||
        filename.substr(
            0U,
            kInodeFilenamePrefix.size()) !=
            kInodeFilenamePrefix ||
        filename.substr(
            filename.size() -
                kInodeFilenameSuffix.size()) !=
            kInodeFilenameSuffix) {
        return ReserveHeaderV1Error::
            kFilenameMismatch;
    }
    const std::size_t uuid_offset =
        kInodeFilenamePrefix.size();
    if (filename[uuid_offset + kIdentityHexBytes] != '-') {
        return ReserveHeaderV1Error::
            kFilenameMismatch;
    }

    l2flow::common::Identity128 parsed_uuid{};
    if (!l2flow::common::ParseIdentity128Hex(
            filename.substr(
                uuid_offset,
                kIdentityHexBytes),
            &parsed_uuid)) {
        return ReserveHeaderV1Error::
            kFilenameMismatch;
    }

    const std::size_t index_offset =
        uuid_offset + kIdentityHexBytes + 1U;
    std::uint32_t parsed_index = 0U;
    for (std::size_t index = 0U;
         index < kIndexDecimalBytes;
         ++index) {
        const char digit = filename[index_offset + index];
        if (digit < '0' || digit > '9') {
            return ReserveHeaderV1Error::
                kFilenameMismatch;
        }
        parsed_index =
            (parsed_index * 10U) +
            static_cast<std::uint32_t>(digit - '0');
    }
    if (parsed_index >= kReserveInodeV1MaxCount) {
        return ReserveHeaderV1Error::
            kFilenameMismatch;
    }
    *reserve_state_uuid = parsed_uuid;
    *inode_index = parsed_index;
    return ReserveHeaderV1Error::kNone;
}

const l2flow::common::Sha256Digest&
ReserveHeadersV1SchemaSha256Digest() noexcept {
    return kReserveHeadersV1SchemaSha256;
}

std::string_view
ReserveHeadersV1SchemaSha256Hex() noexcept {
    return kReserveHeadersV1SchemaSha256Hex;
}

bool VerifyReserveHeadersV1SchemaFile(
    const std::filesystem::path& path,
    std::string* error) noexcept {
    l2flow::common::Sha256Digest actual{};
    std::string hash_error;
    if (!l2flow::common::ComputeFileSha256(
            path,
            &actual,
            &hash_error,
            std::optional<std::uint64_t>{
                static_cast<std::uint64_t>(
                    kReserveHeadersV1SchemaBytes)})) {
        SetError(error, std::move(hash_error));
        return false;
    }
    if (actual != kReserveHeadersV1SchemaSha256) {
        SetError(
            error,
            "reserve headers V1 schema SHA-256 mismatch");
        return false;
    }
    if (error != nullptr) {
        try {
            error->clear();
        } catch (...) {
        }
    }
    return true;
}

}  // namespace l2flow::ingress
