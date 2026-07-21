#ifndef L2FLOW_INGRESS_BYTE_RING_H_
#define L2FLOW_INGRESS_BYTE_RING_H_

#include "l2flow/ingress/capture_meta.h"

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <vector>

namespace l2flow::ingress {

enum class ByteRingPushResult : std::uint8_t {
    PUBLISHED = 0U,
    FULL,
    INVALID_ARGUMENT,
    MESSAGE_TOO_LARGE,
    CURSOR_EXHAUSTED,
};

enum class ByteRingPopResult : std::uint8_t {
    RECORD = 0U,
    EMPTY,
    OUTPUT_TOO_SMALL,
    CORRUPT,
};

enum class ByteRingPressure : std::uint8_t {
    NORMAL = 0U,
    WARNING,
    PROTECT,
};

// Consumer-owned reusable output.  Reserve max_body_bytes() once before the
// consume loop.  try_pop() never grows the vector: insufficient capacity is
// reported without consuming the entry, so steady-state pop performs no
// allocation.
struct ByteRingRecord final {
    explicit ByteRingRecord(std::size_t body_capacity = 0U);

    CaptureMetaV1 meta{};
    std::array<std::byte, kVendorMessageHeadBytes> head{};
    std::vector<std::byte> body;
};

class ByteRingTestPeer;
class RawFinalizationContinuationPosixV1;

// Bounded variable-length SPSC byte ring.
//
// Physical entry bytes are exactly:
//   CaptureMetaV1 + 23-byte vendor head + body + uint32 commit_length
//
// commit_length is little-endian and includes the trailing uint32 itself.
// Entries may cross the physical end of the allocation.  A monotonically
// increasing release-published cursor, rather than an unaligned atomic inside
// the byte array, makes wrapped commits safe for the acquire-loading consumer.
class ByteRing final {
public:
    static constexpr std::size_t kWarningPercent = 70U;
    static constexpr std::size_t kProtectPercent = 85U;

    // max_message_bytes includes the 23-byte vendor head.  Construction is
    // the ring's only allocation point and rejects a configuration in which
    // the largest legal entry cannot fit once.
    ByteRing(std::size_t capacity_bytes,
             std::uint32_t max_message_bytes);
    ~ByteRing() = default;

    ByteRing(const ByteRing&) = delete;
    ByteRing& operator=(const ByteRing&) = delete;
    ByteRing(ByteRing&&) = delete;
    ByteRing& operator=(ByteRing&&) = delete;

    [[nodiscard]] ByteRingPushResult try_push_copy(
        const CaptureMetaV1& meta,
        std::span<const std::byte, kVendorMessageHeadBytes> head,
        std::span<const std::byte> body) noexcept;

    // RECORD consumes exactly one entry.  EMPTY, OUTPUT_TOO_SMALL and CORRUPT
    // leave both the consumer cursor and the supplied output unchanged.
    [[nodiscard]] ByteRingPopResult try_pop(ByteRingRecord& record);

    [[nodiscard]] std::size_t capacity_bytes() const noexcept {
        return capacity_bytes_;
    }

    [[nodiscard]] std::uint32_t max_message_bytes() const noexcept {
        return max_message_bytes_;
    }

    [[nodiscard]] std::size_t max_body_bytes() const noexcept {
        return static_cast<std::size_t>(max_message_bytes_) -
               kVendorMessageHeadBytes;
    }

    [[nodiscard]] std::size_t used_bytes() const noexcept;
    [[nodiscard]] std::size_t free_bytes() const noexcept;

    [[nodiscard]] std::size_t warning_threshold_bytes() const noexcept {
        return warning_threshold_bytes_;
    }

    [[nodiscard]] std::size_t protect_threshold_bytes() const noexcept {
        return protect_threshold_bytes_;
    }

    [[nodiscard]] ByteRingPressure pressure() const noexcept;

    [[nodiscard]] std::uint64_t published_position() const noexcept {
        return published_position_.load(std::memory_order_acquire);
    }

    [[nodiscard]] std::uint64_t consumed_position() const noexcept {
        return consumed_position_.load(std::memory_order_acquire);
    }

private:
    friend class ByteRingTestPeer;
    friend class RawFinalizationContinuationPosixV1;

    static std::size_t ceil_percent(std::size_t value,
                                    std::size_t percent) noexcept;
    static std::uint32_t load_u32_le(
        std::span<const std::byte, kVendorMessageHeadBytes> bytes,
        std::size_t offset) noexcept;
    static void store_u32_le(std::uint32_t value,
                             std::array<std::byte, 4U>& bytes) noexcept;

    void copy_in(std::uint64_t absolute_position,
                 const void* source,
                 std::size_t size) noexcept;
    void copy_out(std::uint64_t absolute_position,
                  void* destination,
                  std::size_t size) const noexcept;
    [[nodiscard]] std::uint32_t load_ring_u32_le(
        std::uint64_t absolute_position) const noexcept;

    const std::size_t capacity_bytes_;
    const std::uint32_t max_message_bytes_;
    const std::size_t warning_threshold_bytes_;
    const std::size_t protect_threshold_bytes_;
    std::unique_ptr<std::byte[]> storage_;

    // Written only by the producer/consumer respectively.
    std::uint64_t producer_position_ = 0U;
    std::uint64_t consumer_position_ = 0U;

    // Release/acquire publication in each direction.
    alignas(64) std::atomic<std::uint64_t> published_position_{0U};
    alignas(64) std::atomic<std::uint64_t> consumed_position_{0U};
};

}  // namespace l2flow::ingress

#endif  // L2FLOW_INGRESS_BYTE_RING_H_
