#pragma once

#include <array>
#include <cstddef>
#include <span>
#include <string>
#include <string_view>

namespace l2flow::common {

using Identity128 = std::array<std::byte, 16U>;

struct EntropyReadResult final {
    std::size_t bytes_read = 0U;
    int error_number = 0;
};

// A syscall-shaped seam for deterministic short-read and failure testing.
class IdentityEntropySource {
public:
    virtual ~IdentityEntropySource() = default;

    [[nodiscard]] virtual EntropyReadResult ReadSome(
        std::span<std::byte> output) noexcept = 0;
};

// Fills exactly 16 bytes, retrying EINTR and legal short reads. Zero-byte
// progress, an all-zero result, or any other error fails closed. The output is
// changed only after a complete non-zero identity has been obtained.
[[nodiscard]] bool GenerateIdentity128(
    IdentityEntropySource& source,
    Identity128* output,
    int* error_number = nullptr) noexcept;

// Linux getrandom(2) implementation of the same contract.
[[nodiscard]] bool GenerateIdentity128(
    Identity128* output,
    int* error_number = nullptr) noexcept;

[[nodiscard]] bool IsZeroIdentity(
    const Identity128& identity) noexcept;
[[nodiscard]] std::string Identity128Hex(
    const Identity128& identity);
[[nodiscard]] bool ParseIdentity128Hex(
    std::string_view text,
    Identity128* identity) noexcept;
// Accepts exactly lowercase 8-4-4-4-12 UUID text and rejects all zero.
[[nodiscard]] bool ParseCanonicalUuid128(
    std::string_view text,
    Identity128* identity) noexcept;

}  // namespace l2flow::common
