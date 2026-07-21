#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

namespace l2flow::control {

enum class CheckedBodyErrorV1 : std::uint8_t {
    kNone = 0U,
    kTruncated,
    kArithmeticOverflow,
    kOffsetInvalid,
    kRangeOverlap,
    kCountExceeded,
    kResourceExhausted,
};

struct CheckedBodyRangeV1 final {
    std::size_t start = 0U;
    std::size_t count = 0U;
    std::size_t item_bytes = 0U;
};

// Bounds-checks little-endian fixed values and MDL relative string/list
// descriptors without invoking any packed vendor accessor. Every non-empty
// dynamic target is registered and required not to overlap the fixed region
// or another registered target.
class CheckedBodyViewV1 final {
public:
    explicit CheckedBodyViewV1(
        std::span<const std::byte> body) noexcept;

    [[nodiscard]] CheckedBodyErrorV1 RequireFixed(
        std::size_t fixed_bytes) noexcept;
    [[nodiscard]] CheckedBodyErrorV1 ReadU16(
        std::size_t offset,
        std::uint16_t* value) const noexcept;
    [[nodiscard]] CheckedBodyErrorV1 ReadU32(
        std::size_t offset,
        std::uint32_t* value) const noexcept;
    [[nodiscard]] CheckedBodyErrorV1 ReadU64(
        std::size_t offset,
        std::uint64_t* value) const noexcept;

    [[nodiscard]] CheckedBodyErrorV1 ReadString(
        std::size_t descriptor_offset,
        std::size_t minimum_data_start,
        std::span<const std::byte>* value) noexcept;
    [[nodiscard]] CheckedBodyErrorV1 ReadList(
        std::size_t descriptor_offset,
        std::size_t item_bytes,
        std::size_t minimum_data_start,
        std::size_t maximum_count,
        CheckedBodyRangeV1* range) noexcept;

    [[nodiscard]] std::span<const std::byte> body()
        const noexcept {
        return body_;
    }
    [[nodiscard]] bool noncanonical_empty_offset()
        const noexcept {
        return noncanonical_empty_offset_;
    }

private:
    struct ProtectedRange final {
        std::size_t begin = 0U;
        std::size_t end = 0U;
    };

    [[nodiscard]] CheckedBodyErrorV1 Protect(
        std::size_t begin,
        std::size_t size) noexcept;

    std::span<const std::byte> body_;
    std::vector<ProtectedRange> protected_ranges_;
    bool fixed_registered_ = false;
    bool noncanonical_empty_offset_ = false;
};

}  // namespace l2flow::control
