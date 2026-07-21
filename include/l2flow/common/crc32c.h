#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>

namespace l2flow::common {

inline constexpr std::uint32_t kCrc32cInitial = 0xffffffffU;
inline constexpr std::uint32_t kCrc32cXorOut = 0xffffffffU;

// Incremental CRC-32C/Castagnoli using the standard reflected parameters:
// poly=0x1EDC6F41, init=0xFFFFFFFF, refin/refout=true,
// xorout=0xFFFFFFFF.
class Crc32cState final {
public:
    Crc32cState() noexcept = default;

    void Reset() noexcept;
    void Update(std::span<const std::byte> bytes) noexcept;
    void Update(std::span<const std::uint8_t> bytes) noexcept;
    void Update(std::string_view bytes) noexcept;

    // Finalize does not consume the state. More input may be appended after
    // reading the current checksum.
    [[nodiscard]] std::uint32_t Finalize() const noexcept;

private:
    std::uint32_t state_ = kCrc32cInitial;
};

[[nodiscard]] std::uint32_t ComputeCrc32c(
    std::span<const std::byte> bytes) noexcept;
[[nodiscard]] std::uint32_t ComputeCrc32c(
    std::span<const std::uint8_t> bytes) noexcept;
[[nodiscard]] std::uint32_t ComputeCrc32c(
    std::string_view bytes) noexcept;

}  // namespace l2flow::common
