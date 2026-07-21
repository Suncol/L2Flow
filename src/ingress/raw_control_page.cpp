#include "l2flow/ingress/raw_control_page.h"

#include <bit>
#include <cstring>
#include <memory>
#include <new>

namespace l2flow::ingress {
namespace {

constexpr std::uint32_t kLittleEndianTag = 0x04030201U;

std::uint64_t LoadIdentityHalf(
    const l2flow::common::Identity128& identity,
    std::size_t offset) noexcept {
    std::uint64_t value = 0U;
    for (std::size_t index = 0U; index < 8U; ++index) {
        value |=
            static_cast<std::uint64_t>(
                std::to_integer<std::uint8_t>(
                    identity[offset + index]))
            << static_cast<unsigned int>(index * 8U);
    }
    return value;
}

void StoreIdentityHalf(
    std::uint64_t value,
    l2flow::common::Identity128* identity,
    std::size_t offset) noexcept {
    for (std::size_t index = 0U; index < 8U; ++index) {
        (*identity)[offset + index] =
            static_cast<std::byte>(
                (value >>
                 static_cast<unsigned int>(index * 8U)) &
                0xffU);
    }
}

bool HasValidAbi(
    const RawControlPageV1& page) noexcept {
    return std::endian::native == std::endian::little &&
           page.magic == kRawControlPageMagic &&
           page.version == kRawControlPageVersion &&
           page.page_size == kRawControlPageBytes &&
           page.endian_tag == kLittleEndianTag &&
           page.atomic_u64_size ==
               sizeof(std::atomic<std::uint64_t>) &&
           page.generation.is_lock_free();
}

std::uint32_t CheckedU32(
    std::uint64_t value,
    bool* valid) noexcept {
    if (value >
        static_cast<std::uint64_t>(
            std::numeric_limits<std::uint32_t>::max())) {
        *valid = false;
        return 0U;
    }
    return static_cast<std::uint32_t>(value);
}

}  // namespace

bool InitializeRawControlPage(
    void* storage,
    std::size_t storage_size,
    RawControlPageV1** page) noexcept {
    if (storage == nullptr ||
        page == nullptr ||
        storage_size < sizeof(RawControlPageV1) ||
        reinterpret_cast<std::uintptr_t>(storage) %
                alignof(RawControlPageV1) !=
            0U ||
        std::endian::native != std::endian::little ||
        !std::atomic<std::uint64_t>::is_always_lock_free) {
        return false;
    }

    auto* initialized =
        ::new (storage) RawControlPageV1();
    initialized->magic = kRawControlPageMagic;
    initialized->version = kRawControlPageVersion;
    initialized->page_size =
        static_cast<std::uint32_t>(kRawControlPageBytes);
    initialized->endian_tag = kLittleEndianTag;
    initialized->atomic_u64_size =
        static_cast<std::uint32_t>(
            sizeof(std::atomic<std::uint64_t>));
    initialized->generation.store(
        0U, std::memory_order_release);
    *page = initialized;
    return true;
}

bool RawControlPageWriter::Publish(
    const RawControlSnapshot& snapshot) noexcept {
    if (!HasValidAbi(page_)) {
        return false;
    }

    std::uint64_t expected =
        page_.generation.load(std::memory_order_acquire);
    if ((expected & 1U) != 0U ||
        expected >
            std::numeric_limits<std::uint64_t>::max() - 2U ||
        !page_.generation.compare_exchange_strong(
            expected,
            expected + 1U,
            std::memory_order_acq_rel,
            std::memory_order_acquire)) {
        return false;
    }

    page_.writer_instance_low.store(
        LoadIdentityHalf(snapshot.writer_instance, 0U),
        std::memory_order_relaxed);
    page_.writer_instance_high.store(
        LoadIdentityHalf(snapshot.writer_instance, 8U),
        std::memory_order_relaxed);
    page_.stream_day_id_low.store(
        LoadIdentityHalf(snapshot.stream_day_id, 0U),
        std::memory_order_relaxed);
    page_.stream_day_id_high.store(
        LoadIdentityHalf(snapshot.stream_day_id, 8U),
        std::memory_order_relaxed);
    page_.source_stream_id.store(
        snapshot.source_stream_id,
        std::memory_order_relaxed);
    page_.capture_date.store(
        snapshot.capture_date,
        std::memory_order_relaxed);
    page_.segment_sequence.store(
        snapshot.segment_sequence,
        std::memory_order_relaxed);
    page_.fatal_state.store(
        snapshot.fatal_state,
        std::memory_order_relaxed);
    page_.append_global_wal_pos.store(
        snapshot.append_global_wal_pos,
        std::memory_order_relaxed);
    page_.append_ingress_sequence.store(
        snapshot.append_ingress_sequence,
        std::memory_order_relaxed);
    page_.append_segment_offset.store(
        snapshot.append_segment_offset,
        std::memory_order_relaxed);
    page_.durable_global_wal_pos.store(
        snapshot.durable_global_wal_pos,
        std::memory_order_relaxed);
    page_.durable_ingress_sequence.store(
        snapshot.durable_ingress_sequence,
        std::memory_order_relaxed);
    page_.durable_segment_offset.store(
        snapshot.durable_segment_offset,
        std::memory_order_relaxed);
    page_.clock_epoch_label.store(
        snapshot.clock_epoch_label,
        std::memory_order_relaxed);
    page_.heartbeat_monotonic_ns.store(
        snapshot.heartbeat_monotonic_ns,
        std::memory_order_relaxed);

    const std::uint64_t previous =
        page_.generation.fetch_add(
            1U, std::memory_order_release);
    return previous == expected + 1U;
}

bool ReadRawControlPage(
    const RawControlPageV1& page,
    RawControlSnapshot* snapshot,
    std::uint64_t* generation,
    std::size_t maximum_attempts) noexcept {
    if (snapshot == nullptr ||
        maximum_attempts == 0U ||
        !HasValidAbi(page)) {
        return false;
    }

    for (std::size_t attempt = 0U;
         attempt < maximum_attempts;
         ++attempt) {
        const std::uint64_t before =
            page.generation.load(std::memory_order_acquire);
        if ((before & 1U) != 0U) {
            continue;
        }

        RawControlSnapshot candidate;
        StoreIdentityHalf(
            page.writer_instance_low.load(
                std::memory_order_relaxed),
            &candidate.writer_instance,
            0U);
        StoreIdentityHalf(
            page.writer_instance_high.load(
                std::memory_order_relaxed),
            &candidate.writer_instance,
            8U);
        StoreIdentityHalf(
            page.stream_day_id_low.load(
                std::memory_order_relaxed),
            &candidate.stream_day_id,
            0U);
        StoreIdentityHalf(
            page.stream_day_id_high.load(
                std::memory_order_relaxed),
            &candidate.stream_day_id,
            8U);
        bool valid = true;
        candidate.source_stream_id = CheckedU32(
            page.source_stream_id.load(
                std::memory_order_relaxed),
            &valid);
        candidate.capture_date = CheckedU32(
            page.capture_date.load(
                std::memory_order_relaxed),
            &valid);
        candidate.segment_sequence = CheckedU32(
            page.segment_sequence.load(
                std::memory_order_relaxed),
            &valid);
        candidate.fatal_state = CheckedU32(
            page.fatal_state.load(
                std::memory_order_relaxed),
            &valid);
        candidate.append_global_wal_pos =
            page.append_global_wal_pos.load(
                std::memory_order_relaxed);
        candidate.append_ingress_sequence =
            page.append_ingress_sequence.load(
                std::memory_order_relaxed);
        candidate.append_segment_offset =
            page.append_segment_offset.load(
                std::memory_order_relaxed);
        candidate.durable_global_wal_pos =
            page.durable_global_wal_pos.load(
                std::memory_order_relaxed);
        candidate.durable_ingress_sequence =
            page.durable_ingress_sequence.load(
                std::memory_order_relaxed);
        candidate.durable_segment_offset =
            page.durable_segment_offset.load(
                std::memory_order_relaxed);
        candidate.clock_epoch_label =
            page.clock_epoch_label.load(
                std::memory_order_relaxed);
        candidate.heartbeat_monotonic_ns =
            page.heartbeat_monotonic_ns.load(
                std::memory_order_relaxed);

        std::atomic_thread_fence(
            std::memory_order_seq_cst);
        const std::uint64_t after =
            page.generation.load(std::memory_order_acquire);
        if (valid && before == after &&
            (after & 1U) == 0U) {
            *snapshot = candidate;
            if (generation != nullptr) {
                *generation = after;
            }
            return true;
        }
    }
    return false;
}

}  // namespace l2flow::ingress
