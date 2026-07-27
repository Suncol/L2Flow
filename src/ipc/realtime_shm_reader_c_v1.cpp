#include "l2flow/ipc/realtime_shm_reader_c_v1.h"

#include "l2flow/ipc/realtime_wire_v1.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <new>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#ifndef F_SEAL_FUTURE_WRITE
#define F_SEAL_FUTURE_WRITE 0x0010
#endif

static_assert(sizeof(l2flow_shm_session_info_v1) == 128U);

namespace {

using l2flow::ipc::RealtimeHeaderFlagV1;
using l2flow::ipc::RealtimeRegionKindV1;
using l2flow::ipc::RealtimeServerStateV1;
using l2flow::ipc::RealtimeWireHeaderV1;
using l2flow::ipc::RealtimeWireCommonRecordV1;
using l2flow::ipc::RealtimeWireInstrumentV1;
using l2flow::ipc::RealtimeWireKLinePayloadV1;
using l2flow::ipc::RealtimeWireKLineSlotV1;
using l2flow::ipc::RealtimeWireKLineWindowV1;
using l2flow::ipc::RealtimeWireRegionDescriptorV1;
using l2flow::ipc::RealtimeWireSnapshotPayloadV1;
using l2flow::ipc::RealtimeWireSnapshotSlotV1;
using l2flow::ipc::RealtimeWireTickPayloadV1;
using l2flow::ipc::RealtimeWireTickSlotV1;

template <typename Integer>
std::atomic_ref<Integer> Atomic(const Integer& value) noexcept {
    return std::atomic_ref<Integer>(const_cast<Integer&>(value));
}

bool CheckedEnd(
    std::uint64_t offset,
    std::uint64_t length,
    std::uint64_t mapping_bytes) noexcept {
    return offset <= mapping_bytes &&
           length <= mapping_bytes - offset;
}

template <std::size_t Size>
bool AnyNonzero(
    const std::array<std::uint8_t, Size>& value) noexcept {
    return std::any_of(
        value.begin(), value.end(),
        [](std::uint8_t byte) noexcept { return byte != 0U; });
}

bool HealthyForRead(const RealtimeWireHeaderV1& header) noexcept {
    const std::uint32_t flags =
        Atomic(header.flags).load(std::memory_order_acquire);
    const std::uint32_t state =
        Atomic(header.server_state).load(std::memory_order_acquire);
    const bool readable_state =
        state == static_cast<std::uint32_t>(
                     RealtimeServerStateV1::kActive) ||
        state == static_cast<std::uint32_t>(
                     RealtimeServerStateV1::kDraining) ||
        state == static_cast<std::uint32_t>(
                     RealtimeServerStateV1::kStoppedClean);
    return (flags &
            l2flow::ipc::kRealtimeHeaderCoverageLostV1) == 0U &&
           readable_state;
}

bool CommonRecordIdentityValid(
    const RealtimeWireCommonRecordV1& common) noexcept {
    if (common.instrument_id == 0U ||
        common.source_sequence == 0U ||
        common.ingress_sequence == 0U ||
        common.source_stream_id == 0U ||
        common.trade_date == 0U ||
        common.reserved0 != 0U ||
        AnyNonzero(common.reserved)) {
        return false;
    }
    switch (common.event_kind) {
        case 1U:
            return common.source_slot == 0U &&
                   common.market == 1U &&
                   common.tick_stream_sequence == 0U;
        case 2U:
            return common.source_slot == 1U &&
                   common.market == 1U &&
                   common.tick_stream_sequence != 0U;
        case 3U:
            return common.source_slot == 2U &&
                   common.market == 2U &&
                   common.tick_stream_sequence == 0U;
        case 4U:
        case 5U:
            return common.source_slot == 3U &&
                   common.market == 2U &&
                   common.tick_stream_sequence != 0U;
        default:
            return false;
    }
}

enum class SlotCopyResult : std::uint8_t {
    kCopied = 0U,
    kNeverPublished,
    kInconsistent,
};

template <typename Slot, typename Payload>
SlotCopyResult CopySlot(const Slot& slot, Payload* output) noexcept {
    if (output == nullptr ||
        sizeof(Payload) > sizeof(slot.payload_words)) {
        return SlotCopyResult::kInconsistent;
    }
    constexpr std::size_t word_count =
        (sizeof(Payload) + sizeof(std::uint64_t) - 1U) /
        sizeof(std::uint64_t);
    for (std::size_t attempt = 0U; attempt < 5U; ++attempt) {
        const std::uint64_t begin =
            Atomic(slot.publish_tag).load(std::memory_order_acquire);
        if (begin == 0U) {
            return SlotCopyResult::kNeverPublished;
        }
        if ((begin & 1U) != 0U) {
            continue;
        }
        std::array<std::uint64_t, word_count> words{};
        for (std::size_t index = 0U; index < word_count; ++index) {
            words[index] = Atomic(slot.payload_words[index])
                               .load(std::memory_order_relaxed);
        }
        std::atomic_thread_fence(std::memory_order_acq_rel);
        const std::uint64_t end =
            Atomic(slot.publish_tag).load(std::memory_order_acquire);
        if (begin == end && (end & 1U) == 0U) {
            *output = std::bit_cast<Payload>(words);
            return SlotCopyResult::kCopied;
        }
    }
    return SlotCopyResult::kInconsistent;
}

}  // namespace

struct l2flow_shm_reader_v1 final {
    const void* mapping = MAP_FAILED;
    std::size_t mapping_bytes = 0U;
    const RealtimeWireHeaderV1* header = nullptr;
    const RealtimeWireInstrumentV1* instruments = nullptr;
    const std::byte* key_blob = nullptr;
    std::size_t key_blob_bytes = 0U;
    const RealtimeWireKLineWindowV1* windows = nullptr;
    const RealtimeWireSnapshotSlotV1* snapshots = nullptr;
    const RealtimeWireTickSlotV1* latest_ticks = nullptr;
    const RealtimeWireKLineSlotV1* klines = nullptr;
    const RealtimeWireTickSlotV1* tick_ring = nullptr;
    std::uint64_t kline_slots_per_table = 0U;
    std::uint64_t tick_ring_capacity = 0U;
};

namespace {

const RealtimeWireRegionDescriptorV1* FindRegion(
    const RealtimeWireHeaderV1& header,
    RealtimeRegionKindV1 kind) noexcept {
    for (const RealtimeWireRegionDescriptorV1& region :
         header.regions) {
        if (region.kind == static_cast<std::uint32_t>(kind)) {
            return &region;
        }
    }
    return nullptr;
}

std::size_t CountRegion(
    const RealtimeWireHeaderV1& header,
    RealtimeRegionKindV1 kind) noexcept {
    return static_cast<std::size_t>(std::count_if(
        header.regions.begin(),
        header.regions.end(),
        [kind](const RealtimeWireRegionDescriptorV1& region) noexcept {
            return region.kind == static_cast<std::uint32_t>(kind);
        }));
}

bool RegionBefore(
    const RealtimeWireRegionDescriptorV1* left,
    const RealtimeWireRegionDescriptorV1* right) noexcept {
    return left != nullptr && right != nullptr &&
           left->offset <= right->offset &&
           left->length <= right->offset - left->offset;
}

bool RegionValid(
    const RealtimeWireRegionDescriptorV1* region,
    std::uint64_t mapping_bytes,
    std::uint64_t expected_stride,
    std::uint64_t expected_count,
    std::uint64_t expected_alignment) noexcept {
    if (region == nullptr || region->schema_major !=
                                 l2flow::ipc::kRealtimeWireMajorV1 ||
        region->schema_minor !=
            l2flow::ipc::kRealtimeWireMinorV1 ||
        region->element_stride != expected_stride ||
        region->element_count != expected_count ||
        region->capacity != expected_count ||
        region->alignment != expected_alignment ||
        region->offset % expected_alignment != 0U ||
        region->flags != 0U || region->reserved != 0U ||
        !CheckedEnd(region->offset, region->length, mapping_bytes)) {
        return false;
    }
    std::uint64_t expected_length = 0U;
    if (expected_count != 0U &&
        expected_stride >
            std::numeric_limits<std::uint64_t>::max() /
                expected_count) {
        return false;
    }
    expected_length = expected_stride * expected_count;
    return region->length == expected_length;
}

bool ReservedRegionValid(
    const RealtimeWireRegionDescriptorV1* region,
    std::uint64_t mapping_bytes) noexcept {
    return region != nullptr &&
           region->schema_major ==
               l2flow::ipc::kRealtimeWireMajorV1 &&
           region->schema_minor ==
               l2flow::ipc::kRealtimeWireMinorV1 &&
           region->offset == mapping_bytes && region->length == 0U &&
           region->element_stride == 0U &&
           region->element_count == 0U && region->capacity == 0U &&
           region->alignment == 1U && region->flags == 0U &&
           region->reserved == 0U;
}

std::size_t FindInstrumentOrdinal(
    const l2flow_shm_reader_v1& reader,
    std::uint32_t instrument_id) noexcept {
    std::size_t begin = 0U;
    std::size_t end = reader.header->instrument_count;
    while (begin < end) {
        const std::size_t middle = begin + (end - begin) / 2U;
        const std::uint32_t value =
            reader.instruments[middle].instrument_id;
        if (value < instrument_id) {
            begin = middle + 1U;
        } else {
            end = middle;
        }
    }
    return begin < reader.header->instrument_count &&
                   reader.instruments[begin].instrument_id ==
                       instrument_id
               ? begin
               : std::numeric_limits<std::size_t>::max();
}

std::size_t FindWindowIndex(
    const l2flow_shm_reader_v1& reader,
    std::uint32_t window_id) noexcept {
    std::size_t begin = 0U;
    std::size_t end = reader.header->window_count;
    while (begin < end) {
        const std::size_t middle = begin + (end - begin) / 2U;
        const std::uint32_t value =
            reader.windows[middle].window_id;
        if (value < window_id) {
            begin = middle + 1U;
        } else {
            end = middle;
        }
    }
    return begin < reader.header->window_count &&
                   reader.windows[begin].window_id == window_id
               ? begin
               : std::numeric_limits<std::size_t>::max();
}

template <typename Slot, typename Payload, typename Validator>
int LatestBatch(
    const l2flow_shm_reader_v1* reader,
    const std::uint32_t* instrument_ids,
    std::size_t count,
    void* outputs,
    std::size_t output_stride,
    std::uint8_t* statuses,
    const Slot* slots,
    Validator validator) noexcept {
    if (reader == nullptr ||
        (count != 0U &&
         (instrument_ids == nullptr || outputs == nullptr ||
          statuses == nullptr)) ||
        output_stride < sizeof(Payload) ||
        (count != 0U &&
         output_stride >
             std::numeric_limits<std::size_t>::max() / count)) {
        return L2FLOW_SHM_READER_INVALID_ARGUMENT_V1;
    }
    if (!HealthyForRead(*reader->header)) {
        return L2FLOW_SHM_READER_UNAVAILABLE_V1;
    }
    auto* const bytes = static_cast<std::byte*>(outputs);
    for (std::size_t index = 0U; index < count; ++index) {
        if (instrument_ids[index] == 0U) {
            statuses[index] =
                L2FLOW_LATEST_INVALID_INSTRUMENT_ID_V1;
            continue;
        }
        const std::size_t ordinal =
            FindInstrumentOrdinal(*reader, instrument_ids[index]);
        if (ordinal == std::numeric_limits<std::size_t>::max()) {
            statuses[index] =
                L2FLOW_LATEST_UNKNOWN_INSTRUMENT_V1;
            continue;
        }
        Payload payload{};
        const SlotCopyResult copy_result =
            CopySlot(slots[ordinal], &payload);
        if (copy_result == SlotCopyResult::kNeverPublished) {
            statuses[index] =
                L2FLOW_LATEST_NOT_YET_OBSERVED_V1;
            continue;
        }
        if (copy_result != SlotCopyResult::kCopied) {
            return L2FLOW_SHM_READER_INCONSISTENT_READ_V1;
        }
        if (!validator(payload, ordinal, instrument_ids[index])) {
            return L2FLOW_SHM_READER_LAYOUT_INVALID_V1;
        }
        std::memcpy(
            bytes + index * output_stride,
            &payload,
            sizeof(payload));
        statuses[index] = L2FLOW_LATEST_AVAILABLE_V1;
    }
    std::atomic_thread_fence(std::memory_order_acquire);
    return HealthyForRead(*reader->header)
               ? L2FLOW_SHM_READER_OK_V1
               : L2FLOW_SHM_READER_UNAVAILABLE_V1;
}

}  // namespace

extern "C" int l2flow_shm_reader_open_fd_v1(
    int fd,
    l2flow_shm_reader_v1** output) {
    if (output == nullptr || fd < 0) {
        return L2FLOW_SHM_READER_INVALID_ARGUMENT_V1;
    }
    *output = nullptr;
    if constexpr (std::endian::native != std::endian::little) {
        return L2FLOW_SHM_READER_ABI_MISMATCH_V1;
    }
    const int descriptor_flags = ::fcntl(fd, F_GETFL);
    const int seals = ::fcntl(fd, F_GET_SEALS);
    const int required_seals =
        F_SEAL_GROW | F_SEAL_SHRINK | F_SEAL_FUTURE_WRITE |
        F_SEAL_SEAL;
    if (descriptor_flags < 0 || seals < 0 ||
        (descriptor_flags & O_ACCMODE) != O_RDONLY ||
        (seals & required_seals) != required_seals) {
        return L2FLOW_SHM_READER_SYSTEM_ERROR_V1;
    }
    struct stat descriptor_stat {};
    if (::fstat(fd, &descriptor_stat) != 0 ||
        descriptor_stat.st_size <
            static_cast<off_t>(sizeof(RealtimeWireHeaderV1))) {
        return L2FLOW_SHM_READER_SYSTEM_ERROR_V1;
    }
    const std::uint64_t mapped_bytes =
        static_cast<std::uint64_t>(descriptor_stat.st_size);
    if (mapped_bytes >
        static_cast<std::uint64_t>(
            std::numeric_limits<std::size_t>::max())) {
        return L2FLOW_SHM_READER_LAYOUT_INVALID_V1;
    }
    void* const mapping = ::mmap(
        nullptr,
        static_cast<std::size_t>(mapped_bytes),
        PROT_READ,
        MAP_SHARED,
        fd,
        0);
    if (mapping == MAP_FAILED) {
        return L2FLOW_SHM_READER_SYSTEM_ERROR_V1;
    }
    const auto* const header =
        static_cast<const RealtimeWireHeaderV1*>(mapping);
    // The publisher initializes all immutable header/registry metadata before
    // release-publishing the state. Acquire that state before interpreting
    // any of those ordinary fields.
    const std::uint32_t initial_state =
        Atomic(header->server_state).load(std::memory_order_acquire);
    const std::uint32_t initial_flags =
        Atomic(header->flags).load(std::memory_order_acquire);
    const std::uint64_t instrument_count = header->instrument_count;
    const std::uint64_t window_count = header->window_count;
    const std::uint64_t maximum_u64 =
        std::numeric_limits<std::uint64_t>::max();
    const std::uint64_t logical_kline_count =
        instrument_count != 0U &&
                window_count <= maximum_u64 / instrument_count
            ? instrument_count * window_count
            : maximum_u64;
    const std::uint64_t physical_kline_count =
        logical_kline_count != maximum_u64 &&
                logical_kline_count <=
                    maximum_u64 /
                        l2flow::ipc::kRealtimeKLineTableCountV1
            ? logical_kline_count *
                  l2flow::ipc::kRealtimeKLineTableCountV1
            : maximum_u64;
    const auto* const instruments = FindRegion(
        *header, RealtimeRegionKindV1::kInstrumentRows);
    const auto* const blob = FindRegion(
        *header, RealtimeRegionKindV1::kInstrumentKeyBlob);
    const auto* const windows = FindRegion(
        *header, RealtimeRegionKindV1::kKLineWindows);
    const auto* const snapshots = FindRegion(
        *header, RealtimeRegionKindV1::kLatestSnapshots);
    const auto* const latest_ticks = FindRegion(
        *header, RealtimeRegionKindV1::kLatestTicks);
    const auto* const klines = FindRegion(
        *header, RealtimeRegionKindV1::kLatestKLines);
    const auto* const ring = FindRegion(
        *header, RealtimeRegionKindV1::kTickRingSlots);
    const auto* const reserved8 = FindRegion(
        *header, RealtimeRegionKindV1::kReserved8);
    const auto* const reserved9 = FindRegion(
        *header, RealtimeRegionKindV1::kReserved9);
    constexpr std::uint32_t known_flags =
        l2flow::ipc::kRealtimeHeaderCoverageLostV1 |
        l2flow::ipc::kRealtimeHeaderKLineEnabledV1;
    const bool valid =
        header->magic == l2flow::ipc::kRealtimeShmMagicV1 &&
        header->abi_major ==
            l2flow::ipc::kRealtimeWireMajorV1 &&
        header->abi_minor ==
            l2flow::ipc::kRealtimeWireMinorV1 &&
        header->header_bytes == sizeof(RealtimeWireHeaderV1) &&
        header->endian_marker ==
            l2flow::ipc::kRealtimeLittleEndianMarkerV1 &&
        header->total_mapping_bytes == mapped_bytes &&
        header->session_epoch != 0U &&
        header->trade_date != 0U &&
        AnyNonzero(header->run_id) &&
        header->registry_version != 0U &&
        AnyNonzero(header->registry_sha256) &&
        header->instrument_count != 0U &&
        initial_state >= static_cast<std::uint32_t>(
                             RealtimeServerStateV1::kInitializing) &&
        initial_state <= static_cast<std::uint32_t>(
                             RealtimeServerStateV1::kFailed) &&
        (initial_flags & ~known_flags) == 0U &&
        ((header->window_count != 0U) ==
         ((initial_flags &
           l2flow::ipc::kRealtimeHeaderKLineEnabledV1) != 0U)) &&
        header->region_count ==
            l2flow::ipc::kRealtimeWireRegionCountV1 &&
        header->region_descriptor_bytes ==
            sizeof(RealtimeWireRegionDescriptorV1) &&
        header->reserved_scalar == 0U &&
        !AnyNonzero(header->reserved_schema_identity) &&
        !AnyNonzero(header->reserved) &&
        instruments != nullptr &&
        instruments->offset >= sizeof(RealtimeWireHeaderV1) &&
        CountRegion(
            *header, RealtimeRegionKindV1::kInstrumentRows) == 1U &&
        CountRegion(
            *header, RealtimeRegionKindV1::kInstrumentKeyBlob) == 1U &&
        CountRegion(
            *header, RealtimeRegionKindV1::kKLineWindows) == 1U &&
        CountRegion(
            *header, RealtimeRegionKindV1::kLatestSnapshots) == 1U &&
        CountRegion(
            *header, RealtimeRegionKindV1::kLatestTicks) == 1U &&
        CountRegion(
            *header, RealtimeRegionKindV1::kLatestKLines) == 1U &&
        CountRegion(
            *header, RealtimeRegionKindV1::kTickRingSlots) == 1U &&
        CountRegion(
            *header, RealtimeRegionKindV1::kReserved8) == 1U &&
        CountRegion(
            *header, RealtimeRegionKindV1::kReserved9) == 1U &&
        RegionBefore(instruments, blob) &&
        RegionBefore(blob, windows) &&
        RegionBefore(windows, snapshots) &&
        RegionBefore(snapshots, latest_ticks) &&
        RegionBefore(latest_ticks, klines) &&
        RegionBefore(klines, ring) &&
        RegionBefore(ring, reserved8) &&
        RegionBefore(reserved8, reserved9) &&
        RegionValid(
            instruments,
            mapped_bytes,
            sizeof(RealtimeWireInstrumentV1),
            instrument_count,
            alignof(RealtimeWireInstrumentV1)) &&
        RegionValid(
            blob,
            mapped_bytes,
            1U,
            blob == nullptr ? 0U : blob->element_count,
            1U) &&
        RegionValid(
            windows,
            mapped_bytes,
            sizeof(RealtimeWireKLineWindowV1),
            window_count,
            alignof(RealtimeWireKLineWindowV1)) &&
        RegionValid(
            snapshots,
            mapped_bytes,
            sizeof(RealtimeWireSnapshotSlotV1),
            instrument_count,
            alignof(RealtimeWireSnapshotSlotV1)) &&
        RegionValid(
            latest_ticks,
            mapped_bytes,
            sizeof(RealtimeWireTickSlotV1),
            instrument_count,
            alignof(RealtimeWireTickSlotV1)) &&
        physical_kline_count != maximum_u64 &&
        RegionValid(
            klines,
            mapped_bytes,
            sizeof(RealtimeWireKLineSlotV1),
            physical_kline_count,
            alignof(RealtimeWireKLineSlotV1)) &&
        ring != nullptr && ring->element_count != 0U &&
        RegionValid(
            ring,
            mapped_bytes,
            sizeof(RealtimeWireTickSlotV1),
            ring->element_count,
            alignof(RealtimeWireTickSlotV1)) &&
        ReservedRegionValid(reserved8, mapped_bytes) &&
        ReservedRegionValid(reserved9, mapped_bytes);
    if (!valid) {
        static_cast<void>(::munmap(
            mapping, static_cast<std::size_t>(mapped_bytes)));
        return L2FLOW_SHM_READER_LAYOUT_INVALID_V1;
    }
    auto* reader = new (std::nothrow) l2flow_shm_reader_v1();
    if (reader == nullptr) {
        static_cast<void>(::munmap(
            mapping, static_cast<std::size_t>(mapped_bytes)));
        return L2FLOW_SHM_READER_SYSTEM_ERROR_V1;
    }
    reader->mapping = mapping;
    reader->mapping_bytes = static_cast<std::size_t>(mapped_bytes);
    reader->header = header;
    const auto* const base = static_cast<const std::byte*>(mapping);
    reader->instruments =
        reinterpret_cast<const RealtimeWireInstrumentV1*>(
            base + instruments->offset);
    reader->key_blob = base + blob->offset;
    reader->key_blob_bytes =
        static_cast<std::size_t>(blob->length);
    reader->windows =
        reinterpret_cast<const RealtimeWireKLineWindowV1*>(
            base + windows->offset);
    reader->snapshots =
        reinterpret_cast<const RealtimeWireSnapshotSlotV1*>(
            base + snapshots->offset);
    reader->latest_ticks =
        reinterpret_cast<const RealtimeWireTickSlotV1*>(
            base + latest_ticks->offset);
    reader->klines =
        reinterpret_cast<const RealtimeWireKLineSlotV1*>(
            base + klines->offset);
    reader->kline_slots_per_table = logical_kline_count;
    reader->tick_ring =
        reinterpret_cast<const RealtimeWireTickSlotV1*>(
            base + ring->offset);
    reader->tick_ring_capacity = ring->element_count;
    std::uint64_t key_cursor = 0U;
    for (std::size_t index = 0U;
         index < header->instrument_count;
         ++index) {
        const RealtimeWireInstrumentV1& row =
            reader->instruments[index];
        if (row.instrument_id == 0U ||
            (index != 0U &&
             reader->instruments[index - 1U].instrument_id >=
                 row.instrument_id) ||
            row.reserved0 != 0U || row.reserved1 != 0U ||
            std::any_of(
                row.reserved.begin(),
                row.reserved.end(),
                [](std::uint64_t value) noexcept {
                    return value != 0U;
                }) ||
            row.security_id_source_offset != key_cursor ||
            row.security_id_source_offset >
                reader->key_blob_bytes ||
            row.security_id_source_length >
                reader->key_blob_bytes -
                    row.security_id_source_offset ||
            row.security_id_offset > reader->key_blob_bytes ||
            row.security_id_length >
                reader->key_blob_bytes - row.security_id_offset) {
            l2flow_shm_reader_close_v1(reader);
            return L2FLOW_SHM_READER_LAYOUT_INVALID_V1;
        }
        key_cursor += row.security_id_source_length;
        if (row.security_id_offset != key_cursor) {
            l2flow_shm_reader_close_v1(reader);
            return L2FLOW_SHM_READER_LAYOUT_INVALID_V1;
        }
        key_cursor += row.security_id_length;
    }
    if (key_cursor != reader->key_blob_bytes) {
        l2flow_shm_reader_close_v1(reader);
        return L2FLOW_SHM_READER_LAYOUT_INVALID_V1;
    }
    for (std::size_t index = 0U; index < header->window_count;
         ++index) {
        if (reader->windows[index].window_id == 0U ||
            reader->windows[index].duration_ns == 0U ||
            reader->windows[index].reserved != 0U ||
            (index != 0U &&
             reader->windows[index - 1U].window_id >=
                 reader->windows[index].window_id)) {
            l2flow_shm_reader_close_v1(reader);
            return L2FLOW_SHM_READER_LAYOUT_INVALID_V1;
        }
    }
    *output = reader;
    return L2FLOW_SHM_READER_OK_V1;
}

extern "C" void l2flow_shm_reader_close_v1(
    l2flow_shm_reader_v1* reader) {
    if (reader == nullptr) {
        return;
    }
    if (reader->mapping != MAP_FAILED) {
        static_cast<void>(::munmap(
            const_cast<void*>(reader->mapping),
            reader->mapping_bytes));
    }
    delete reader;
}

extern "C" int l2flow_shm_reader_session_v1(
    const l2flow_shm_reader_v1* reader,
    l2flow_shm_session_info_v1* output) {
    if (reader == nullptr || output == nullptr) {
        return L2FLOW_SHM_READER_INVALID_ARGUMENT_V1;
    }
    l2flow_shm_session_info_v1 result{};
    std::memcpy(
        result.run_id,
        reader->header->run_id.data(),
        sizeof(result.run_id));
    result.session_epoch = reader->header->session_epoch;
    result.registry_version = reader->header->registry_version;
    std::memcpy(
        result.registry_sha256,
        reader->header->registry_sha256.data(),
        sizeof(result.registry_sha256));
    result.tick_ring_capacity = reader->tick_ring_capacity;
    // The publisher stores highest before it advances contiguous. Read the
    // pair in the opposite order so a concurrent advance cannot fabricate
    // contiguous > highest in one returned observation.
    result.tick_contiguous_published_sequence =
        Atomic(reader->header->tick_contiguous_published_sequence)
            .load(std::memory_order_acquire);
    result.tick_highest_published_sequence =
        Atomic(reader->header->tick_highest_published_sequence)
            .load(std::memory_order_acquire);
    result.kline_generation =
        Atomic(reader->header->kline_generation)
            .load(std::memory_order_acquire);
    result.heartbeat_monotonic_ns =
        Atomic(reader->header->heartbeat_monotonic_ns)
            .load(std::memory_order_acquire);
    result.trade_date = reader->header->trade_date;
    result.server_state =
        Atomic(reader->header->server_state)
            .load(std::memory_order_acquire);
    result.flags =
        Atomic(reader->header->flags)
            .load(std::memory_order_acquire);
    result.instrument_count = reader->header->instrument_count;
    result.window_count = reader->header->window_count;
    *output = result;
    return L2FLOW_SHM_READER_OK_V1;
}

extern "C" int l2flow_shm_reader_instrument_v1(
    const l2flow_shm_reader_v1* reader,
    std::uint32_t instrument_id,
    void* row_output,
    std::size_t row_output_bytes,
    std::uint8_t* security_id_source_output,
    std::size_t security_id_source_capacity,
    std::size_t* security_id_source_written,
    std::uint8_t* security_id_output,
    std::size_t security_id_capacity,
    std::size_t* security_id_written) {
    if (reader == nullptr || instrument_id == 0U ||
        row_output == nullptr ||
        row_output_bytes < sizeof(RealtimeWireInstrumentV1) ||
        security_id_source_written == nullptr ||
        security_id_written == nullptr ||
        (security_id_source_capacity != 0U &&
         security_id_source_output == nullptr) ||
        (security_id_capacity != 0U &&
         security_id_output == nullptr)) {
        return L2FLOW_SHM_READER_INVALID_ARGUMENT_V1;
    }
    const std::size_t ordinal =
        FindInstrumentOrdinal(*reader, instrument_id);
    if (ordinal == std::numeric_limits<std::size_t>::max()) {
        return L2FLOW_SHM_READER_INVALID_ARGUMENT_V1;
    }
    const RealtimeWireInstrumentV1& row =
        reader->instruments[ordinal];
    *security_id_source_written = row.security_id_source_length;
    *security_id_written = row.security_id_length;
    if (security_id_source_capacity <
            row.security_id_source_length ||
        security_id_capacity < row.security_id_length) {
        return L2FLOW_SHM_READER_BUFFER_TOO_SMALL_V1;
    }
    std::memcpy(row_output, &row, sizeof(row));
    if (row.security_id_source_length != 0U) {
        std::memcpy(
            security_id_source_output,
            reader->key_blob + row.security_id_source_offset,
            row.security_id_source_length);
    }
    if (row.security_id_length != 0U) {
        std::memcpy(
            security_id_output,
            reader->key_blob + row.security_id_offset,
            row.security_id_length);
    }
    return L2FLOW_SHM_READER_OK_V1;
}

extern "C" int l2flow_shm_reader_latest_snapshots_v1(
    const l2flow_shm_reader_v1* reader,
    const std::uint32_t* instrument_ids,
    std::size_t count,
    void* outputs,
    std::size_t output_stride,
    std::uint8_t* item_statuses) {
    return LatestBatch<
        RealtimeWireSnapshotSlotV1,
        RealtimeWireSnapshotPayloadV1>(
        reader,
        instrument_ids,
        count,
        outputs,
        output_stride,
        item_statuses,
        reader == nullptr ? nullptr : reader->snapshots,
        [](const RealtimeWireSnapshotPayloadV1& payload,
           std::size_t ordinal,
           std::uint32_t instrument_id) noexcept {
            return payload.common.record_schema_version == 1U &&
                   payload.common.record_bytes ==
                       sizeof(RealtimeWireSnapshotPayloadV1) &&
                   CommonRecordIdentityValid(payload.common) &&
                   (payload.common.event_kind == 1U ||
                    payload.common.event_kind == 3U) &&
                   payload.common.instrument_id == instrument_id &&
                   payload.common.registry_ordinal == ordinal &&
                   payload.common.tick_stream_sequence == 0U;
        });
}

extern "C" int l2flow_shm_reader_latest_ticks_v1(
    const l2flow_shm_reader_v1* reader,
    const std::uint32_t* instrument_ids,
    std::size_t count,
    void* outputs,
    std::size_t output_stride,
    std::uint8_t* item_statuses) {
    return LatestBatch<
        RealtimeWireTickSlotV1,
        RealtimeWireTickPayloadV1>(
        reader,
        instrument_ids,
        count,
        outputs,
        output_stride,
        item_statuses,
        reader == nullptr ? nullptr : reader->latest_ticks,
        [](const RealtimeWireTickPayloadV1& payload,
           std::size_t ordinal,
           std::uint32_t instrument_id) noexcept {
            return payload.common.record_schema_version == 1U &&
                   payload.common.record_bytes ==
                       sizeof(RealtimeWireTickPayloadV1) &&
                   CommonRecordIdentityValid(payload.common) &&
                   (payload.common.event_kind == 2U ||
                    payload.common.event_kind == 4U ||
                    payload.common.event_kind == 5U) &&
                   payload.common.instrument_id == instrument_id &&
                   payload.common.registry_ordinal == ordinal &&
                   payload.common.tick_stream_sequence != 0U;
        });
}

extern "C" int l2flow_shm_reader_latest_klines_v1(
    const l2flow_shm_reader_v1* reader,
    const std::uint32_t* instrument_ids,
    const std::uint32_t* window_ids,
    std::size_t count,
    void* outputs,
    std::size_t output_stride,
    std::uint8_t* item_statuses) {
    if (reader == nullptr ||
        (count != 0U &&
         (instrument_ids == nullptr || window_ids == nullptr ||
          outputs == nullptr || item_statuses == nullptr)) ||
        output_stride < sizeof(RealtimeWireKLinePayloadV1) ||
        (count != 0U &&
         output_stride >
             std::numeric_limits<std::size_t>::max() / count)) {
        return L2FLOW_SHM_READER_INVALID_ARGUMENT_V1;
    }
    if (!HealthyForRead(*reader->header)) {
        return L2FLOW_SHM_READER_UNAVAILABLE_V1;
    }
    // The writer release-publishes kline_generation only after every slot
    // for that immutable generation has been updated. Anchor this call to
    // that completed cut. A slot from the next in-progress generation is a
    // transient consistency conflict, never an available latest row.
    const std::uint64_t completed_generation =
        Atomic(reader->header->kline_generation)
            .load(std::memory_order_acquire);
    const std::uint64_t table =
        completed_generation %
        l2flow::ipc::kRealtimeKLineTableCountV1;
    const std::uint64_t table_offset =
        table * reader->kline_slots_per_table;
    auto* const bytes = static_cast<std::byte*>(outputs);
    for (std::size_t index = 0U; index < count; ++index) {
        if (instrument_ids[index] == 0U) {
            item_statuses[index] =
                L2FLOW_LATEST_INVALID_INSTRUMENT_ID_V1;
            continue;
        }
        if (window_ids[index] == 0U) {
            item_statuses[index] =
                L2FLOW_LATEST_INVALID_WINDOW_ID_V1;
            continue;
        }
        const std::size_t ordinal =
            FindInstrumentOrdinal(*reader, instrument_ids[index]);
        const std::size_t window =
            FindWindowIndex(*reader, window_ids[index]);
        if (ordinal == std::numeric_limits<std::size_t>::max()) {
            item_statuses[index] =
                L2FLOW_LATEST_UNKNOWN_INSTRUMENT_V1;
            continue;
        }
        if (window == std::numeric_limits<std::size_t>::max()) {
            item_statuses[index] =
                L2FLOW_LATEST_UNKNOWN_WINDOW_V1;
            continue;
        }
        RealtimeWireKLinePayloadV1 payload{};
        const std::uint64_t slot =
            table_offset +
            static_cast<std::uint64_t>(ordinal) *
                reader->header->window_count +
            static_cast<std::uint64_t>(window);
        const SlotCopyResult copy_result =
            CopySlot(
                reader->klines[static_cast<std::size_t>(slot)],
                &payload);
        if (copy_result == SlotCopyResult::kNeverPublished) {
            if (completed_generation != 0U) {
                return L2FLOW_SHM_READER_INCONSISTENT_READ_V1;
            }
            item_statuses[index] =
                L2FLOW_LATEST_NOT_YET_OBSERVED_V1;
            continue;
        }
        if (copy_result != SlotCopyResult::kCopied) {
            return L2FLOW_SHM_READER_INCONSISTENT_READ_V1;
        }
        if (payload.generation != completed_generation) {
            return L2FLOW_SHM_READER_INCONSISTENT_READ_V1;
        }
        if (payload.generation == 0U ||
            payload.trade_date != reader->header->trade_date ||
            payload.instrument_id != instrument_ids[index] ||
            payload.window_id != window_ids[index] ||
            payload.window_duration_ns !=
                reader->windows[window].duration_ns) {
            return L2FLOW_SHM_READER_LAYOUT_INVALID_V1;
        }
        if (payload.present == 0U) {
            item_statuses[index] =
                L2FLOW_LATEST_NOT_YET_OBSERVED_V1;
            continue;
        }
        if (payload.present != 1U) {
            return L2FLOW_SHM_READER_LAYOUT_INVALID_V1;
        }
        std::memcpy(
            bytes + index * output_stride,
            &payload,
            sizeof(payload));
        item_statuses[index] = L2FLOW_LATEST_AVAILABLE_V1;
    }
    if (!HealthyForRead(*reader->header)) {
        return L2FLOW_SHM_READER_UNAVAILABLE_V1;
    }
    return Atomic(reader->header->kline_generation)
                       .load(std::memory_order_acquire) ==
                   completed_generation
               ? L2FLOW_SHM_READER_OK_V1
               : L2FLOW_SHM_READER_INCONSISTENT_READ_V1;
}

extern "C" int l2flow_shm_reader_ticks_v1(
    const l2flow_shm_reader_v1* reader,
    std::uint64_t expected_sequence,
    void* outputs,
    std::size_t output_stride,
    std::size_t maximum_records,
    std::size_t* written,
    std::uint64_t* next_sequence,
    std::uint64_t* observed_sequence) {
    if (reader == nullptr || expected_sequence == 0U ||
        written == nullptr || next_sequence == nullptr ||
        observed_sequence == nullptr ||
        (maximum_records != 0U && outputs == nullptr) ||
        output_stride < sizeof(RealtimeWireTickPayloadV1) ||
        (maximum_records != 0U &&
         output_stride >
             std::numeric_limits<std::size_t>::max() /
                 maximum_records)) {
        return L2FLOW_SHM_READER_INVALID_ARGUMENT_V1;
    }
    *written = 0U;
    *next_sequence = expected_sequence;
    *observed_sequence = 0U;
    if (!HealthyForRead(*reader->header)) {
        return L2FLOW_SHM_READER_UNAVAILABLE_V1;
    }
    const std::uint64_t contiguous =
        Atomic(reader->header->tick_contiguous_published_sequence)
            .load(std::memory_order_acquire);
    const std::uint64_t oldest =
        contiguous >= reader->tick_ring_capacity
            ? contiguous - reader->tick_ring_capacity + 1U
            : 1U;
    if (expected_sequence < oldest) {
        *observed_sequence = oldest;
        return L2FLOW_SHM_READER_OVERRUN_V1;
    }
    auto* const bytes = static_cast<std::byte*>(outputs);
    std::uint64_t sequence = expected_sequence;
    while (*written < maximum_records && sequence <= contiguous) {
        const std::uint64_t index =
            (sequence - 1U) % reader->tick_ring_capacity;
        RealtimeWireTickPayloadV1 payload{};
        if (CopySlot(
                reader->tick_ring[static_cast<std::size_t>(index)],
                &payload) != SlotCopyResult::kCopied) {
            return L2FLOW_SHM_READER_INCONSISTENT_READ_V1;
        }
        if (payload.common.tick_stream_sequence != sequence) {
            *observed_sequence =
                payload.common.tick_stream_sequence;
            return payload.common.tick_stream_sequence > sequence
                       ? L2FLOW_SHM_READER_OVERRUN_V1
                       : L2FLOW_SHM_READER_INCONSISTENT_READ_V1;
        }
        if (payload.common.record_schema_version != 1U ||
            payload.common.record_bytes !=
                sizeof(RealtimeWireTickPayloadV1) ||
            !CommonRecordIdentityValid(payload.common) ||
            (payload.common.event_kind != 2U &&
             payload.common.event_kind != 4U &&
             payload.common.event_kind != 5U) ||
            payload.common.registry_ordinal >=
                reader->header->instrument_count ||
            reader
                    ->instruments[payload.common.registry_ordinal]
                    .instrument_id != payload.common.instrument_id) {
            return L2FLOW_SHM_READER_LAYOUT_INVALID_V1;
        }
        std::memcpy(
            bytes + *written * output_stride,
            &payload,
            sizeof(payload));
        ++(*written);
        ++sequence;
    }
    *next_sequence = sequence;
    return HealthyForRead(*reader->header)
               ? L2FLOW_SHM_READER_OK_V1
               : L2FLOW_SHM_READER_UNAVAILABLE_V1;
}
