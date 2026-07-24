#pragma once

#include "mdl_api_types.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <type_traits>

namespace l2flow::sdk {

inline constexpr std::size_t kVendorHeadBytes = 23U;
using VendorHeadBytes = std::array<std::byte, kVendorHeadBytes>;

template <typename T>
[[nodiscard]] T LoadUnaligned(const std::byte* source) noexcept {
    static_assert(std::is_trivially_copyable_v<T>);
    T value{};
    std::memcpy(&value, source, sizeof(value));
    return value;
}

// Reads the vendor-defined 23-byte wire header from an owned byte copy. It never
// dereferences the vendor's packed MDLMessageHead as a C++ object.
class VendorHeadView {
public:
    explicit VendorHeadView(const VendorHeadBytes& bytes) noexcept
        : bytes_(bytes) {}

    [[nodiscard]] std::uint8_t head_size() const noexcept {
        return LoadUnaligned<std::uint8_t>(bytes_.data() + 0U);
    }
    [[nodiscard]] std::uint32_t message_size() const noexcept {
        return LoadUnaligned<std::uint32_t>(bytes_.data() + 1U);
    }
    [[nodiscard]] std::uint8_t message_encoding() const noexcept {
        return LoadUnaligned<std::uint8_t>(bytes_.data() + 5U);
    }
    [[nodiscard]] std::uint8_t service_id() const noexcept {
        return LoadUnaligned<std::uint8_t>(bytes_.data() + 6U);
    }
    [[nodiscard]] std::uint16_t service_version() const noexcept {
        return LoadUnaligned<std::uint16_t>(bytes_.data() + 7U);
    }
    [[nodiscard]] std::uint16_t message_id() const noexcept {
        return LoadUnaligned<std::uint16_t>(bytes_.data() + 9U);
    }
    [[nodiscard]] std::uint32_t local_time_raw() const noexcept {
        return LoadUnaligned<std::uint32_t>(bytes_.data() + 11U);
    }
    [[nodiscard]] std::uint64_t sequence_id() const noexcept {
        return LoadUnaligned<std::uint64_t>(bytes_.data() + 15U);
    }

private:
    const VendorHeadBytes& bytes_;
};

static_assert(sizeof(datayes::mdl::MDLMessageHead) == kVendorHeadBytes);
static_assert(alignof(datayes::mdl::MDLMessageHead) == 1U);
static_assert(offsetof(datayes::mdl::MDLMessageHead, HeadSize) == 0U);
static_assert(offsetof(datayes::mdl::MDLMessageHead, MessageSize) == 1U);
static_assert(offsetof(datayes::mdl::MDLMessageHead, MessageEncoding) == 5U);
static_assert(offsetof(datayes::mdl::MDLMessageHead, ServiceID) == 6U);
static_assert(offsetof(datayes::mdl::MDLMessageHead, ServiceVersion) == 7U);
static_assert(offsetof(datayes::mdl::MDLMessageHead, MessageID) == 9U);
static_assert(offsetof(datayes::mdl::MDLMessageHead, LocalTime) == 11U);
static_assert(offsetof(datayes::mdl::MDLMessageHead, SequenceID) == 15U);

}  // namespace l2flow::sdk
