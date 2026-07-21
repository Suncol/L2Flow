#pragma once

#include "l2flow/common/identity128.h"

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>

namespace l2flow::ingress {

inline constexpr std::size_t kRawControlPageBytes = 4096U;
inline constexpr std::uint32_t kRawControlPageVersion = 1U;
inline constexpr std::array<std::byte, 8U> kRawControlPageMagic{
    std::byte{'L'}, std::byte{'2'}, std::byte{'R'}, std::byte{'C'},
    std::byte{'T'}, std::byte{'L'}, std::byte{'1'}, std::byte{0}};

struct RawControlSnapshot final {
    l2flow::common::Identity128 writer_instance{};
    l2flow::common::Identity128 stream_day_id{};
    std::uint32_t source_stream_id = 0U;
    std::uint32_t capture_date = 0U;
    std::uint32_t segment_sequence = 0U;
    std::uint32_t fatal_state = 0U;
    std::uint64_t append_global_wal_pos = 0U;
    std::uint64_t append_ingress_sequence = 0U;
    std::uint64_t append_segment_offset = 0U;
    std::uint64_t durable_global_wal_pos = 0U;
    std::uint64_t durable_ingress_sequence = 0U;
    std::uint64_t durable_segment_offset = 0U;
    std::uint64_t clock_epoch_label = 0U;
    std::uint64_t heartbeat_monotonic_ns = 0U;

    friend bool operator==(
        const RawControlSnapshot&,
        const RawControlSnapshot&) = default;
};

// Host-local mmap ABI. It is deliberately not a portable or durable wire
// object. Only the writer transaction and reader snapshot functions below
// may access its mutable payload.
struct alignas(kRawControlPageBytes) RawControlPageV1 final {
    std::array<std::byte, 8U> magic{};
    std::uint32_t version = 0U;
    std::uint32_t page_size = 0U;
    std::uint32_t endian_tag = 0U;
    std::uint32_t atomic_u64_size = 0U;
    std::atomic<std::uint64_t> generation{0U};
    std::atomic<std::uint64_t> writer_instance_low{0U};
    std::atomic<std::uint64_t> writer_instance_high{0U};
    std::atomic<std::uint64_t> stream_day_id_low{0U};
    std::atomic<std::uint64_t> stream_day_id_high{0U};
    std::atomic<std::uint64_t> source_stream_id{0U};
    std::atomic<std::uint64_t> capture_date{0U};
    std::atomic<std::uint64_t> segment_sequence{0U};
    std::atomic<std::uint64_t> fatal_state{0U};
    std::atomic<std::uint64_t> append_global_wal_pos{0U};
    std::atomic<std::uint64_t> append_ingress_sequence{0U};
    std::atomic<std::uint64_t> append_segment_offset{0U};
    std::atomic<std::uint64_t> durable_global_wal_pos{0U};
    std::atomic<std::uint64_t> durable_ingress_sequence{0U};
    std::atomic<std::uint64_t> durable_segment_offset{0U};
    std::atomic<std::uint64_t> clock_epoch_label{0U};
    std::atomic<std::uint64_t> heartbeat_monotonic_ns{0U};
    std::array<std::byte, 3936U> reserved{};
};

static_assert(sizeof(std::atomic<std::uint64_t>) == 8U);
static_assert(alignof(std::atomic<std::uint64_t>) == 8U);
static_assert(std::atomic<std::uint64_t>::is_always_lock_free);
static_assert(sizeof(RawControlPageV1) == kRawControlPageBytes);
static_assert(alignof(RawControlPageV1) == kRawControlPageBytes);
static_assert(offsetof(RawControlPageV1, generation) == 24U);
static_assert(offsetof(RawControlPageV1, reserved) == 160U);

// Constructs a first even publication in page-aligned storage.
[[nodiscard]] bool InitializeRawControlPage(
    void* storage,
    std::size_t storage_size,
    RawControlPageV1** page) noexcept;

class RawControlPageWriter final {
public:
    explicit RawControlPageWriter(
        RawControlPageV1& page) noexcept
        : page_(page) {}

    // Returns false if the page ABI is invalid, another writer is active, or
    // generation would wrap. On success the complete snapshot is published
    // under one odd/even generation transaction.
    [[nodiscard]] bool Publish(
        const RawControlSnapshot& snapshot) noexcept;

private:
    RawControlPageV1& page_;
};

// Retries a bounded number of times. A false result means the page is stale,
// currently being written, ABI-incompatible, or changed during the read.
[[nodiscard]] bool ReadRawControlPage(
    const RawControlPageV1& page,
    RawControlSnapshot* snapshot,
    std::uint64_t* generation = nullptr,
    std::size_t maximum_attempts = 1000U) noexcept;

}  // namespace l2flow::ingress
