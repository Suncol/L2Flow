#pragma once

#include "l2flow/common/identity128.h"
#include "l2flow/common/sha256.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <span>
#include <string>
#include <string_view>

namespace l2flow::ingress {

// ReserveFileHeaderV1 and ReserveInodeHeaderV1 are L2Flow-private,
// explicitly little-endian disk formats. They are never persisted by writing
// a C++ object representation.
inline constexpr std::uint16_t kReserveHeadersV1Version = 1U;
inline constexpr std::uint8_t kReserveHeadersV1LittleEndian = 1U;
inline constexpr std::size_t kReserveHeaderV1Bytes = 4096U;
inline constexpr std::uint32_t kReserveInodeV1MaxCount =
    100000000U;
inline constexpr std::size_t kReserveInodeFilenameV1Bytes = 55U;

inline constexpr std::array<std::byte, 8U>
    kReserveFileHeaderV1Magic{
        std::byte{'L'}, std::byte{'2'}, std::byte{'R'},
        std::byte{'D'}, std::byte{'F'}, std::byte{'1'},
        std::byte{0}, std::byte{0}};
inline constexpr std::array<std::byte, 8U>
    kReserveInodeHeaderV1Magic{
        std::byte{'L'}, std::byte{'2'}, std::byte{'R'},
        std::byte{'I'}, std::byte{'F'}, std::byte{'1'},
        std::byte{0}, std::byte{0}};

// SHA-256 over the exact UTF-8 bytes of schemas/reserve_headers_v1.json,
// including its single trailing LF.
inline constexpr std::size_t kReserveHeadersV1SchemaBytes = 7915U;
inline constexpr std::string_view
    kReserveHeadersV1SchemaSha256Hex =
        "d29546ccd11a2e39ddb522277627c751"
        "0d2e6733eaa8a2c483a826d967bb789a";
inline constexpr l2flow::common::Sha256Digest
    kReserveHeadersV1SchemaSha256{
        std::byte{0xd2}, std::byte{0x95}, std::byte{0x46},
        std::byte{0xcc}, std::byte{0xd1}, std::byte{0x1a},
        std::byte{0x2e}, std::byte{0x39}, std::byte{0xdd},
        std::byte{0xb5}, std::byte{0x22}, std::byte{0x27},
        std::byte{0x76}, std::byte{0x27}, std::byte{0xc7},
        std::byte{0x51}, std::byte{0x0d}, std::byte{0x2e},
        std::byte{0x67}, std::byte{0x33}, std::byte{0xea},
        std::byte{0xa8}, std::byte{0xa2}, std::byte{0xc4},
        std::byte{0x83}, std::byte{0xa8}, std::byte{0x26},
        std::byte{0xd9}, std::byte{0x67}, std::byte{0xbb},
        std::byte{0x78}, std::byte{0x9a}};

// Complete 4096-byte golden wire digests. The input values are documented in
// schemas/reserve_headers_v1.json and the codec test.
inline constexpr std::string_view
    kReserveFileHeaderV1GoldenSha256Hex =
        "d06086dd203b7fa511fedae4da8d0b5e"
        "110b88f0884e16f82c151e23a446b969";
inline constexpr std::string_view
    kReserveInodeHeaderV1GoldenSha256Hex =
        "9d6b81e5861bf170d8127b8603775740"
        "29d0cbebb075e25397c72a0b1d03ac12";

namespace reserve_headers_v1_offset {

namespace common {
inline constexpr std::size_t kMagic = 0U;
inline constexpr std::size_t kVersion = 8U;
inline constexpr std::size_t kEndian = 10U;
inline constexpr std::size_t kReserved0 = 11U;
inline constexpr std::size_t kHeaderSize = 12U;
inline constexpr std::size_t kReserveStateUuid = 16U;
inline constexpr std::size_t kDeviceId = 32U;
inline constexpr std::size_t kQuotaIdentitySha256 = 40U;
inline constexpr std::size_t kMountIdentitySha256 = 72U;
}  // namespace common

namespace file {
inline constexpr std::size_t kDeclaredBytes = 104U;
inline constexpr std::size_t kHeaderCrc32c = 112U;
inline constexpr std::size_t kReservedTail = 116U;
}  // namespace file

namespace inode {
inline constexpr std::size_t kIndex = 104U;
inline constexpr std::size_t kCount = 108U;
inline constexpr std::size_t kHeaderCrc32c = 112U;
inline constexpr std::size_t kReservedTail = 116U;
}  // namespace inode

}  // namespace reserve_headers_v1_offset

using ReserveHeaderDigestV1 = l2flow::common::Sha256Digest;
using ReserveHeaderWireV1 =
    std::array<std::byte, kReserveHeaderV1Bytes>;

struct ReserveAllocationPoolIdentityV1 final {
    l2flow::common::Identity128 reserve_state_uuid{};
    std::uint64_t device_id = 0U;
    ReserveHeaderDigestV1 quota_identity_sha256{};
    ReserveHeaderDigestV1 mount_identity_sha256{};

    friend bool operator==(
        const ReserveAllocationPoolIdentityV1&,
        const ReserveAllocationPoolIdentityV1&) = default;
};

struct ReserveFileHeaderV1 final {
    ReserveAllocationPoolIdentityV1 pool{};
    std::uint64_t declared_bytes = 0U;

    friend bool operator==(
        const ReserveFileHeaderV1&,
        const ReserveFileHeaderV1&) = default;
};

struct ReserveInodeHeaderV1 final {
    ReserveAllocationPoolIdentityV1 pool{};
    std::uint32_t inode_index = 0U;
    std::uint32_t declared_inode_reserve_count = 0U;

    friend bool operator==(
        const ReserveInodeHeaderV1&,
        const ReserveInodeHeaderV1&) = default;
};

enum class ReserveHeaderV1Error : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidWireSize,
    kInvalidMagic,
    kUnsupportedVersion,
    kInvalidEndian,
    kInvalidHeaderSize,
    kNonzeroReserved,
    kCrcMismatch,
    kInvalidReserveUuid,
    kInvalidDeviceIdentity,
    kInvalidQuotaIdentity,
    kInvalidMountIdentity,
    kInvalidDeclaredBytes,
    kInvalidInodeCount,
    kInvalidInodeIndex,
    kIdentityMismatch,
    kDeclaredBytesMismatch,
    kInodeCountMismatch,
    kFilenameMismatch,
    kAllocationFailure,
};

[[nodiscard]] std::string_view ReserveHeaderV1ErrorName(
    ReserveHeaderV1Error error) noexcept;

[[nodiscard]] ReserveHeaderV1Error EncodeReserveFileHeaderV1(
    const ReserveFileHeaderV1& header,
    ReserveHeaderWireV1* wire) noexcept;

[[nodiscard]] ReserveHeaderV1Error DecodeReserveFileHeaderV1(
    std::span<const std::byte> wire,
    ReserveFileHeaderV1* header) noexcept;

[[nodiscard]] ReserveHeaderV1Error ValidateReserveFileHeaderV1(
    std::span<const std::byte> wire) noexcept;

[[nodiscard]] ReserveHeaderV1Error
ValidateReserveFileHeaderBindingV1(
    const ReserveFileHeaderV1& header,
    const ReserveAllocationPoolIdentityV1& expected_pool,
    std::uint64_t actual_file_size) noexcept;

[[nodiscard]] ReserveHeaderV1Error EncodeReserveInodeHeaderV1(
    const ReserveInodeHeaderV1& header,
    ReserveHeaderWireV1* wire) noexcept;

[[nodiscard]] ReserveHeaderV1Error DecodeReserveInodeHeaderV1(
    std::span<const std::byte> wire,
    ReserveInodeHeaderV1* header) noexcept;

[[nodiscard]] ReserveHeaderV1Error ValidateReserveInodeHeaderV1(
    std::span<const std::byte> wire) noexcept;

[[nodiscard]] ReserveHeaderV1Error
ValidateReserveInodeHeaderBindingV1(
    const ReserveInodeHeaderV1& header,
    const ReserveAllocationPoolIdentityV1& expected_pool,
    std::uint32_t expected_count,
    std::string_view exact_filename) noexcept;

[[nodiscard]] ReserveHeaderV1Error
FormatReserveInodeFilenameV1(
    const l2flow::common::Identity128& reserve_state_uuid,
    std::uint32_t inode_index,
    std::string* filename) noexcept;

[[nodiscard]] ReserveHeaderV1Error
ParseReserveInodeFilenameV1(
    std::string_view filename,
    l2flow::common::Identity128* reserve_state_uuid,
    std::uint32_t* inode_index) noexcept;

[[nodiscard]] const l2flow::common::Sha256Digest&
ReserveHeadersV1SchemaSha256Digest() noexcept;

[[nodiscard]] std::string_view
ReserveHeadersV1SchemaSha256Hex() noexcept;

// Securely hashes one exact regular-file snapshot and compares it with the
// frozen schema digest. Errors never include the caller-provided path.
[[nodiscard]] bool VerifyReserveHeadersV1SchemaFile(
    const std::filesystem::path& path,
    std::string* error = nullptr) noexcept;

}  // namespace l2flow::ingress
