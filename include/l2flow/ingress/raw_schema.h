#pragma once

#include "l2flow/common/sha256.h"

#include <cstddef>
#include <filesystem>
#include <string>
#include <string_view>

namespace l2flow::ingress {

// SHA-256 over the exact UTF-8 bytes in schemas/raw_v1.json, including its
// single trailing LF. The schema has no BOM or generated timestamp.
inline constexpr std::size_t kFrozenRawSchemaBytes = 24499U;
inline constexpr std::string_view kFrozenRawSchemaSha256Hex =
    "cc8c858ac3b7ba2424a6bda87c961574"
    "fb7f8b73e9135391dd316672c85789c3";
inline constexpr l2flow::common::Sha256Digest
    kFrozenRawSchemaSha256{
        std::byte{0xcc}, std::byte{0x8c}, std::byte{0x85},
        std::byte{0x8a}, std::byte{0xc3}, std::byte{0xb7},
        std::byte{0xba}, std::byte{0x24}, std::byte{0x24},
        std::byte{0xa6}, std::byte{0xbd}, std::byte{0xa8},
        std::byte{0x7c}, std::byte{0x96}, std::byte{0x15},
        std::byte{0x74}, std::byte{0xfb}, std::byte{0x7f},
        std::byte{0x8b}, std::byte{0x73}, std::byte{0xe9},
        std::byte{0x13}, std::byte{0x53}, std::byte{0x91},
        std::byte{0xdd}, std::byte{0x31}, std::byte{0x66},
        std::byte{0x72}, std::byte{0xc8}, std::byte{0x57},
        std::byte{0x89}, std::byte{0xc3}};

[[nodiscard]] const l2flow::common::Sha256Digest&
RawSchemaSha256Digest() noexcept;

[[nodiscard]] std::string_view RawSchemaSha256Hex() noexcept;

// Securely hashes one exact regular-file snapshot and compares it with the
// frozen schema digest. Errors never include the caller-provided path.
[[nodiscard]] bool VerifyRawSchemaFile(
    const std::filesystem::path& path,
    std::string* error = nullptr) noexcept;

}  // namespace l2flow::ingress
