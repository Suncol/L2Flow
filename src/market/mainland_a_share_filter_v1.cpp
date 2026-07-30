#include "l2flow/market/mainland_a_share_filter_v1.h"

#include <cstddef>
#include <span>
#include <string_view>

namespace l2flow::market {
namespace {

constexpr std::size_t kSecurityIdSize = 6U;

[[nodiscard]] bool IsAsciiDigit(std::byte value) noexcept {
    const unsigned char character = std::to_integer<unsigned char>(value);
    return character >= static_cast<unsigned char>('0') &&
           character <= static_cast<unsigned char>('9');
}

[[nodiscard]] bool IsSixAsciiDigits(
    std::span<const std::byte> security_id) noexcept {
    if (security_id.size() != kSecurityIdSize) {
        return false;
    }
    for (const std::byte value : security_id) {
        if (!IsAsciiDigit(value)) {
            return false;
        }
    }
    return true;
}

[[nodiscard]] bool HasPrefix(
    std::span<const std::byte> security_id,
    std::string_view prefix) noexcept {
    for (std::size_t index = 0U; index < prefix.size(); ++index) {
        if (std::to_integer<unsigned char>(security_id[index]) !=
            static_cast<unsigned char>(prefix[index])) {
            return false;
        }
    }
    return true;
}

[[nodiscard]] int CompareSecurityId(
    std::span<const std::byte> security_id,
    std::string_view boundary) noexcept {
    for (std::size_t index = 0U; index < kSecurityIdSize; ++index) {
        const unsigned char actual =
            std::to_integer<unsigned char>(security_id[index]);
        const unsigned char expected =
            static_cast<unsigned char>(boundary[index]);
        if (actual < expected) {
            return -1;
        }
        if (actual > expected) {
            return 1;
        }
    }
    return 0;
}

[[nodiscard]] bool InClosedRange(
    std::span<const std::byte> security_id,
    std::string_view lower,
    std::string_view upper) noexcept {
    return CompareSecurityId(security_id, lower) >= 0 &&
           CompareSecurityId(security_id, upper) <= 0;
}

[[nodiscard]] bool IsShanghaiAShare(
    std::span<const std::byte> security_id) noexcept {
    return HasPrefix(security_id, "600") ||
           HasPrefix(security_id, "601") ||
           HasPrefix(security_id, "603") ||
           HasPrefix(security_id, "605") ||
           HasPrefix(security_id, "688");
}

[[nodiscard]] bool IsShenzhenAShare(
    std::span<const std::byte> security_id) noexcept {
    return InClosedRange(security_id, "000001", "000999") ||
           InClosedRange(security_id, "001200", "004999") ||
           InClosedRange(security_id, "300000", "309799");
}

[[nodiscard]] bool IsBeijingAShare(
    std::span<const std::byte> security_id) noexcept {
    return InClosedRange(security_id, "920000", "920999");
}

}  // namespace

bool IsMainlandAShareSecurityIdV1(
    MainlandExchangeV1 exchange,
    std::span<const std::byte> security_id) noexcept {
    if (!IsSixAsciiDigits(security_id)) {
        return false;
    }

    switch (exchange) {
    case MainlandExchangeV1::kShanghai:
        return IsShanghaiAShare(security_id);
    case MainlandExchangeV1::kShenzhen:
        return IsShenzhenAShare(security_id);
    case MainlandExchangeV1::kBeijing:
        return IsBeijingAShare(security_id);
    case MainlandExchangeV1::kUnknown:
        return false;
    }
    return false;
}

}  // namespace l2flow::market
