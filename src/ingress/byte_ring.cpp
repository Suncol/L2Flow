#include "l2flow/ingress/byte_ring.h"

#include <algorithm>
#include <cstring>
#include <limits>
#include <stdexcept>

namespace l2flow::ingress {
namespace {

constexpr std::size_t kHeadSizeOffset = 0U;
constexpr std::size_t kMessageSizeOffset = 1U;
constexpr std::size_t kU32Bytes = sizeof(std::uint32_t);

static_assert(kMessageSizeOffset + kU32Bytes <=
              kVendorMessageHeadBytes);
static_assert(std::atomic<std::uint64_t>::is_always_lock_free);

bool checked_add_size(std::size_t left,
                      std::size_t right,
                      std::size_t* result) noexcept {
    if (left > std::numeric_limits<std::size_t>::max() - right) {
        return false;
    }
    *result = left + right;
    return true;
}

bool checked_add_u64(std::uint64_t left,
                     std::uint64_t right,
                     std::uint64_t* result) noexcept {
    if (left > std::numeric_limits<std::uint64_t>::max() - right) {
        return false;
    }
    *result = left + right;
    return true;
}

std::uint8_t byte_value(std::byte value) noexcept {
    return std::to_integer<std::uint8_t>(value);
}

}  // namespace

ByteRingRecord::ByteRingRecord(std::size_t body_capacity) {
    body.reserve(body_capacity);
}

ByteRing::ByteRing(std::size_t capacity_bytes,
                   std::uint32_t max_message_bytes)
    : capacity_bytes_(capacity_bytes),
      max_message_bytes_(max_message_bytes),
      warning_threshold_bytes_(
          ceil_percent(capacity_bytes, kWarningPercent)),
      protect_threshold_bytes_(
          ceil_percent(capacity_bytes, kProtectPercent)) {
    if (capacity_bytes_ < kMinimumByteRingEntryBytes) {
        throw std::invalid_argument(
            "byte ring capacity is smaller than one empty-body entry");
    }
    if (max_message_bytes_ < kVendorMessageHeadBytes) {
        throw std::invalid_argument(
            "max_message_bytes excludes the complete vendor head");
    }

    std::size_t maximum_entry_bytes = 0U;
    if (!checked_add_size(sizeof(CaptureMetaV1),
                          static_cast<std::size_t>(max_message_bytes_),
                          &maximum_entry_bytes) ||
        !checked_add_size(maximum_entry_bytes,
                          kEntryCommitLengthBytes,
                          &maximum_entry_bytes) ||
        maximum_entry_bytes >
            static_cast<std::size_t>(
                std::numeric_limits<std::uint32_t>::max())) {
        throw std::invalid_argument(
            "largest byte ring entry cannot be represented by commit length");
    }
    if (maximum_entry_bytes > capacity_bytes_) {
        throw std::invalid_argument(
            "byte ring capacity cannot hold the configured maximum message");
    }

    storage_ = std::make_unique<std::byte[]>(capacity_bytes_);
}

ByteRingPushResult ByteRing::try_push_copy(
    const CaptureMetaV1& meta,
    std::span<const std::byte, kVendorMessageHeadBytes> head,
    std::span<const std::byte> body) noexcept {
    if (byte_value(head[kHeadSizeOffset]) != kVendorMessageHeadBytes) {
        return ByteRingPushResult::INVALID_ARGUMENT;
    }

    if (body.size() > max_body_bytes()) {
        return ByteRingPushResult::MESSAGE_TOO_LARGE;
    }

    std::size_t message_bytes = 0U;
    if (!checked_add_size(kVendorMessageHeadBytes,
                          body.size(),
                          &message_bytes) ||
        message_bytes > max_message_bytes_) {
        return ByteRingPushResult::MESSAGE_TOO_LARGE;
    }
    if (load_u32_le(head, kMessageSizeOffset) != message_bytes) {
        return ByteRingPushResult::INVALID_ARGUMENT;
    }

    std::size_t entry_bytes = 0U;
    if (!checked_add_size(sizeof(CaptureMetaV1),
                          message_bytes,
                          &entry_bytes) ||
        !checked_add_size(entry_bytes,
                          kEntryCommitLengthBytes,
                          &entry_bytes) ||
        entry_bytes >
            static_cast<std::size_t>(
                std::numeric_limits<std::uint32_t>::max())) {
        return ByteRingPushResult::MESSAGE_TOO_LARGE;
    }

    const std::uint64_t consumed =
        consumed_position_.load(std::memory_order_acquire);
    if (consumed > producer_position_) {
        return ByteRingPushResult::CURSOR_EXHAUSTED;
    }
    const std::uint64_t used = producer_position_ - consumed;
    if (used > capacity_bytes_) {
        return ByteRingPushResult::CURSOR_EXHAUSTED;
    }
    if (entry_bytes >
        capacity_bytes_ - static_cast<std::size_t>(used)) {
        return ByteRingPushResult::FULL;
    }

    std::uint64_t next_position = 0U;
    if (!checked_add_u64(
            producer_position_,
            static_cast<std::uint64_t>(entry_bytes),
            &next_position)) {
        return ByteRingPushResult::CURSOR_EXHAUSTED;
    }

    std::uint64_t cursor = producer_position_;
    copy_in(cursor, &meta, sizeof(meta));
    cursor += sizeof(meta);
    copy_in(cursor, head.data(), head.size());
    cursor += head.size();
    copy_in(cursor, body.data(), body.size());
    cursor += body.size();

    std::array<std::byte, kU32Bytes> commit_bytes{};
    store_u32_le(static_cast<std::uint32_t>(entry_bytes),
                 commit_bytes);
    copy_in(cursor, commit_bytes.data(), commit_bytes.size());

    // The cursor is published only after every payload byte and the wrapped
    // commit field have been copied.  The consumer's acquire load makes the
    // complete entry visible.
    producer_position_ = next_position;
    published_position_.store(next_position, std::memory_order_release);
    return ByteRingPushResult::PUBLISHED;
}

ByteRingPopResult ByteRing::try_pop(ByteRingRecord& record) {
    const std::uint64_t published =
        published_position_.load(std::memory_order_acquire);
    if (published == consumer_position_) {
        return ByteRingPopResult::EMPTY;
    }
    if (published < consumer_position_) {
        return ByteRingPopResult::CORRUPT;
    }

    const std::uint64_t available = published - consumer_position_;
    if (available > capacity_bytes_ ||
        available < kMinimumByteRingEntryBytes) {
        return ByteRingPopResult::CORRUPT;
    }

    CaptureMetaV1 meta{};
    std::array<std::byte, kVendorMessageHeadBytes> head{};
    std::uint64_t cursor = consumer_position_;
    copy_out(cursor, &meta, sizeof(meta));
    cursor += sizeof(meta);
    copy_out(cursor, head.data(), head.size());

    if (byte_value(head[kHeadSizeOffset]) != kVendorMessageHeadBytes) {
        return ByteRingPopResult::CORRUPT;
    }

    const std::uint32_t message_bytes_u32 =
        load_u32_le(head, kMessageSizeOffset);
    if (message_bytes_u32 < kVendorMessageHeadBytes ||
        message_bytes_u32 > max_message_bytes_) {
        return ByteRingPopResult::CORRUPT;
    }
    const std::size_t message_bytes =
        static_cast<std::size_t>(message_bytes_u32);
    const std::size_t body_bytes =
        message_bytes - kVendorMessageHeadBytes;

    std::size_t entry_bytes = 0U;
    if (!checked_add_size(sizeof(CaptureMetaV1),
                          message_bytes,
                          &entry_bytes) ||
        !checked_add_size(entry_bytes,
                          kEntryCommitLengthBytes,
                          &entry_bytes) ||
        entry_bytes > available ||
        entry_bytes >
            static_cast<std::size_t>(
                std::numeric_limits<std::uint32_t>::max())) {
        return ByteRingPopResult::CORRUPT;
    }

    const std::uint64_t commit_position =
        consumer_position_ +
        static_cast<std::uint64_t>(
            entry_bytes - kEntryCommitLengthBytes);
    if (load_ring_u32_le(commit_position) != entry_bytes) {
        return ByteRingPopResult::CORRUPT;
    }

    if (record.body.capacity() < body_bytes) {
        return ByteRingPopResult::OUTPUT_TOO_SMALL;
    }

    record.body.resize(body_bytes);
    if (body_bytes != 0U) {
        const std::uint64_t body_position =
            consumer_position_ + sizeof(CaptureMetaV1) +
            kVendorMessageHeadBytes;
        copy_out(body_position, record.body.data(), body_bytes);
    }
    record.meta = meta;
    record.head = head;

    const std::uint64_t next_position =
        consumer_position_ + static_cast<std::uint64_t>(entry_bytes);
    consumer_position_ = next_position;
    consumed_position_.store(next_position, std::memory_order_release);
    return ByteRingPopResult::RECORD;
}

std::size_t ByteRing::used_bytes() const noexcept {
    // Loading consumed first prevents a later consumer advancement from
    // appearing ahead of a stale published cursor in this observation.
    const std::uint64_t consumed =
        consumed_position_.load(std::memory_order_acquire);
    const std::uint64_t published =
        published_position_.load(std::memory_order_acquire);
    if (published < consumed) {
        return capacity_bytes_;
    }
    const std::uint64_t used = published - consumed;
    if (used > capacity_bytes_) {
        return capacity_bytes_;
    }
    return static_cast<std::size_t>(used);
}

std::size_t ByteRing::free_bytes() const noexcept {
    return capacity_bytes_ - used_bytes();
}

ByteRingPressure ByteRing::pressure() const noexcept {
    const std::size_t used = used_bytes();
    if (used >= protect_threshold_bytes_) {
        return ByteRingPressure::PROTECT;
    }
    if (used >= warning_threshold_bytes_) {
        return ByteRingPressure::WARNING;
    }
    return ByteRingPressure::NORMAL;
}

std::size_t ByteRing::ceil_percent(std::size_t value,
                                   std::size_t percent) noexcept {
    const std::size_t quotient = value / 100U;
    const std::size_t remainder = value % 100U;
    return quotient * percent +
           (remainder * percent + 99U) / 100U;
}

std::uint32_t ByteRing::load_u32_le(
    std::span<const std::byte, kVendorMessageHeadBytes> bytes,
    std::size_t offset) noexcept {
    return static_cast<std::uint32_t>(byte_value(bytes[offset])) |
           (static_cast<std::uint32_t>(byte_value(bytes[offset + 1U]))
            << 8U) |
           (static_cast<std::uint32_t>(byte_value(bytes[offset + 2U]))
            << 16U) |
           (static_cast<std::uint32_t>(byte_value(bytes[offset + 3U]))
            << 24U);
}

void ByteRing::store_u32_le(
    std::uint32_t value,
    std::array<std::byte, 4U>& bytes) noexcept {
    bytes[0] = static_cast<std::byte>(value & 0xffU);
    bytes[1] = static_cast<std::byte>((value >> 8U) & 0xffU);
    bytes[2] = static_cast<std::byte>((value >> 16U) & 0xffU);
    bytes[3] = static_cast<std::byte>((value >> 24U) & 0xffU);
}

void ByteRing::copy_in(std::uint64_t absolute_position,
                       const void* source,
                       std::size_t size) noexcept {
    if (size == 0U) {
        return;
    }
    const std::size_t index = static_cast<std::size_t>(
        absolute_position % capacity_bytes_);
    const std::size_t first =
        std::min(size, capacity_bytes_ - index);
    std::memcpy(storage_.get() + index, source, first);
    if (first != size) {
        const auto* const source_bytes =
            static_cast<const std::byte*>(source);
        std::memcpy(storage_.get(), source_bytes + first, size - first);
    }
}

void ByteRing::copy_out(std::uint64_t absolute_position,
                        void* destination,
                        std::size_t size) const noexcept {
    if (size == 0U) {
        return;
    }
    const std::size_t index = static_cast<std::size_t>(
        absolute_position % capacity_bytes_);
    const std::size_t first =
        std::min(size, capacity_bytes_ - index);
    std::memcpy(destination, storage_.get() + index, first);
    if (first != size) {
        auto* const destination_bytes =
            static_cast<std::byte*>(destination);
        std::memcpy(destination_bytes + first,
                    storage_.get(),
                    size - first);
    }
}

std::uint32_t ByteRing::load_ring_u32_le(
    std::uint64_t absolute_position) const noexcept {
    std::array<std::byte, kU32Bytes> bytes{};
    copy_out(absolute_position, bytes.data(), bytes.size());
    return static_cast<std::uint32_t>(byte_value(bytes[0])) |
           (static_cast<std::uint32_t>(byte_value(bytes[1])) << 8U) |
           (static_cast<std::uint32_t>(byte_value(bytes[2])) << 16U) |
           (static_cast<std::uint32_t>(byte_value(bytes[3])) << 24U);
}

}  // namespace l2flow::ingress
