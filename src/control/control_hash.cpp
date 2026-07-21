#include "l2flow/control/control_hash.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>

namespace l2flow::control {

l2flow::common::Sha256Digest HashControlTextV1(
    std::string_view domain,
    std::span<const std::byte> bytes) noexcept {
    l2flow::common::Sha256Hasher hasher;
    const std::span<const char> domain_chars(
        domain.data(), domain.size());
    static constexpr std::array<std::byte, 1U> separator{
        std::byte{0}};
    std::array<std::byte, 8U> length{};
    const std::uint64_t byte_count =
        static_cast<std::uint64_t>(bytes.size());
    for (std::size_t index = 0U; index < length.size(); ++index) {
        length[index] = static_cast<std::byte>(
            (byte_count >> (index * 8U)) & 0xffU);
    }

    l2flow::common::Sha256Digest digest{};
    if (!hasher.Update(std::as_bytes(domain_chars)) ||
        !hasher.Update(separator) ||
        !hasher.Update(length) ||
        !hasher.Update(bytes) ||
        !hasher.Finalize(&digest)) {
        return {};
    }
    return digest;
}

}  // namespace l2flow::control
