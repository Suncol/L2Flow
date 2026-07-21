#include "l2flow/control/checked_body_view.h"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <new>

namespace l2flow::control {
namespace {

bool CheckedAdd(
    std::size_t left,
    std::size_t right,
    std::size_t* output) noexcept {
    if (output == nullptr ||
        left > std::numeric_limits<std::size_t>::max() - right) {
        return false;
    }
    *output = left + right;
    return true;
}

}  // namespace

CheckedBodyViewV1::CheckedBodyViewV1(
    std::span<const std::byte> body) noexcept
    : body_(body) {}

CheckedBodyErrorV1 CheckedBodyViewV1::RequireFixed(
    std::size_t fixed_bytes) noexcept {
    if (fixed_registered_) {
        return CheckedBodyErrorV1::kOffsetInvalid;
    }
    if (body_.size() < fixed_bytes) {
        return CheckedBodyErrorV1::kTruncated;
    }
    const CheckedBodyErrorV1 result = Protect(0U, fixed_bytes);
    if (result == CheckedBodyErrorV1::kNone) {
        fixed_registered_ = true;
    }
    return result;
}

CheckedBodyErrorV1 CheckedBodyViewV1::ReadU16(
    std::size_t offset,
    std::uint16_t* value) const noexcept {
    if (value == nullptr || offset > body_.size() ||
        sizeof(std::uint16_t) > body_.size() - offset) {
        return CheckedBodyErrorV1::kTruncated;
    }
    *value = static_cast<std::uint16_t>(
        std::to_integer<std::uint16_t>(body_[offset]) |
        static_cast<std::uint16_t>(
            std::to_integer<std::uint16_t>(body_[offset + 1U])
            << 8U));
    return CheckedBodyErrorV1::kNone;
}

CheckedBodyErrorV1 CheckedBodyViewV1::ReadU32(
    std::size_t offset,
    std::uint32_t* value) const noexcept {
    if (value == nullptr || offset > body_.size() ||
        sizeof(std::uint32_t) > body_.size() - offset) {
        return CheckedBodyErrorV1::kTruncated;
    }
    std::uint32_t decoded = 0U;
    for (std::size_t index = 0U; index < 4U; ++index) {
        decoded |= std::to_integer<std::uint32_t>(
                       body_[offset + index])
                   << (index * 8U);
    }
    *value = decoded;
    return CheckedBodyErrorV1::kNone;
}

CheckedBodyErrorV1 CheckedBodyViewV1::ReadU64(
    std::size_t offset,
    std::uint64_t* value) const noexcept {
    if (value == nullptr || offset > body_.size() ||
        sizeof(std::uint64_t) > body_.size() - offset) {
        return CheckedBodyErrorV1::kTruncated;
    }
    std::uint64_t decoded = 0U;
    for (std::size_t index = 0U; index < 8U; ++index) {
        decoded |= std::to_integer<std::uint64_t>(
                       body_[offset + index])
                   << (index * 8U);
    }
    *value = decoded;
    return CheckedBodyErrorV1::kNone;
}

CheckedBodyErrorV1 CheckedBodyViewV1::ReadString(
    std::size_t descriptor_offset,
    std::size_t minimum_data_start,
    std::span<const std::byte>* value) noexcept {
    if (value == nullptr) {
        return CheckedBodyErrorV1::kOffsetInvalid;
    }
    std::uint16_t length = 0U;
    std::uint32_t relative_offset = 0U;
    const CheckedBodyErrorV1 length_error =
        ReadU16(descriptor_offset, &length);
    if (length_error != CheckedBodyErrorV1::kNone) {
        return length_error;
    }
    std::size_t offset_field = 0U;
    if (!CheckedAdd(descriptor_offset, 2U, &offset_field)) {
        return CheckedBodyErrorV1::kArithmeticOverflow;
    }
    const CheckedBodyErrorV1 offset_error =
        ReadU32(offset_field, &relative_offset);
    if (offset_error != CheckedBodyErrorV1::kNone) {
        return offset_error;
    }
    if (length == 0U) {
        if (relative_offset != 0U) {
            std::size_t empty_start = 0U;
            if (!CheckedAdd(
                    descriptor_offset,
                    static_cast<std::size_t>(relative_offset),
                    &empty_start) ||
                empty_start > body_.size()) {
                return CheckedBodyErrorV1::kOffsetInvalid;
            }
            noncanonical_empty_offset_ = true;
        }
        *value = {};
        return CheckedBodyErrorV1::kNone;
    }
    if (relative_offset < 6U) {
        return CheckedBodyErrorV1::kOffsetInvalid;
    }
    std::size_t start = 0U;
    if (!CheckedAdd(
            descriptor_offset,
            static_cast<std::size_t>(relative_offset),
            &start) ||
        start < minimum_data_start ||
        start > body_.size() ||
        static_cast<std::size_t>(length) > body_.size() - start) {
        return CheckedBodyErrorV1::kOffsetInvalid;
    }
    const CheckedBodyErrorV1 protected_result =
        Protect(start, static_cast<std::size_t>(length));
    if (protected_result != CheckedBodyErrorV1::kNone) {
        return protected_result;
    }
    *value = body_.subspan(start, length);
    return CheckedBodyErrorV1::kNone;
}

CheckedBodyErrorV1 CheckedBodyViewV1::ReadList(
    std::size_t descriptor_offset,
    std::size_t item_bytes,
    std::size_t minimum_data_start,
    std::size_t maximum_count,
    CheckedBodyRangeV1* range) noexcept {
    if (range == nullptr || item_bytes == 0U) {
        return CheckedBodyErrorV1::kOffsetInvalid;
    }
    std::uint32_t length = 0U;
    std::uint32_t relative_offset = 0U;
    const CheckedBodyErrorV1 length_error =
        ReadU32(descriptor_offset, &length);
    if (length_error != CheckedBodyErrorV1::kNone) {
        return length_error;
    }
    std::size_t offset_field = 0U;
    if (!CheckedAdd(descriptor_offset, 4U, &offset_field)) {
        return CheckedBodyErrorV1::kArithmeticOverflow;
    }
    const CheckedBodyErrorV1 offset_error =
        ReadU32(offset_field, &relative_offset);
    if (offset_error != CheckedBodyErrorV1::kNone) {
        return offset_error;
    }
    const std::size_t count = static_cast<std::size_t>(length);
    if (count > maximum_count) {
        return CheckedBodyErrorV1::kCountExceeded;
    }
    if (count == 0U) {
        if (relative_offset != 0U) {
            std::size_t empty_start = 0U;
            if (!CheckedAdd(
                    descriptor_offset,
                    static_cast<std::size_t>(relative_offset),
                    &empty_start) ||
                empty_start > body_.size()) {
                return CheckedBodyErrorV1::kOffsetInvalid;
            }
            noncanonical_empty_offset_ = true;
        }
        *range = CheckedBodyRangeV1{
            descriptor_offset, 0U, item_bytes};
        return CheckedBodyErrorV1::kNone;
    }
    if (relative_offset < 8U ||
        count > std::numeric_limits<std::size_t>::max() /
                    item_bytes) {
        return CheckedBodyErrorV1::kArithmeticOverflow;
    }
    std::size_t start = 0U;
    const std::size_t total_bytes = count * item_bytes;
    if (!CheckedAdd(
            descriptor_offset,
            static_cast<std::size_t>(relative_offset),
            &start) ||
        start < minimum_data_start || start > body_.size() ||
        total_bytes > body_.size() - start) {
        return CheckedBodyErrorV1::kOffsetInvalid;
    }
    const CheckedBodyErrorV1 protected_result =
        Protect(start, total_bytes);
    if (protected_result != CheckedBodyErrorV1::kNone) {
        return protected_result;
    }
    *range = CheckedBodyRangeV1{start, count, item_bytes};
    return CheckedBodyErrorV1::kNone;
}

CheckedBodyErrorV1 CheckedBodyViewV1::Protect(
    std::size_t begin,
    std::size_t size) noexcept {
    if (size == 0U) {
        return CheckedBodyErrorV1::kNone;
    }
    std::size_t end = 0U;
    if (!CheckedAdd(begin, size, &end) || end > body_.size()) {
        return CheckedBodyErrorV1::kArithmeticOverflow;
    }
    for (const ProtectedRange& existing : protected_ranges_) {
        if (begin < existing.end && existing.begin < end) {
            return CheckedBodyErrorV1::kRangeOverlap;
        }
    }
    try {
        protected_ranges_.push_back(ProtectedRange{begin, end});
    } catch (const std::bad_alloc&) {
        return CheckedBodyErrorV1::kResourceExhausted;
    }
    return CheckedBodyErrorV1::kNone;
}

}  // namespace l2flow::control
