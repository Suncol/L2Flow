#include "l2flow/ipc/realtime_shared_service_v1.h"

#include "l2flow/ipc/realtime_wire_projection_v1.h"
#include "l2flow/ipc/realtime_wire_v1.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <limits>
#include <memory>
#include <new>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>

#include <fcntl.h>
#include <poll.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

#ifndef F_SEAL_FUTURE_WRITE
#define F_SEAL_FUTURE_WRITE 0x0010
#endif

namespace l2flow::ipc {
namespace {

namespace common = l2flow::common;
namespace market = l2flow::market;

constexpr std::uint64_t kPageBytes = 4096U;
constexpr std::size_t kHotPrefixAdvanceBudget = 64U;
constexpr std::size_t kControlPrefixAdvanceBudget = 4096U;

void SetSystemError(int* output, int value) noexcept {
    if (output != nullptr) {
        *output = value;
    }
}

bool CheckedAdd(
    std::uint64_t left,
    std::uint64_t right,
    std::uint64_t* output) noexcept {
    if (output == nullptr ||
        left > std::numeric_limits<std::uint64_t>::max() - right) {
        return false;
    }
    *output = left + right;
    return true;
}

bool CheckedMultiply(
    std::uint64_t left,
    std::uint64_t right,
    std::uint64_t* output) noexcept {
    if (output == nullptr ||
        (right != 0U &&
         left > std::numeric_limits<std::uint64_t>::max() / right)) {
        return false;
    }
    *output = left * right;
    return true;
}

bool AlignUp(
    std::uint64_t value,
    std::uint64_t alignment,
    std::uint64_t* output) noexcept {
    if (output == nullptr || alignment == 0U ||
        (alignment & (alignment - 1U)) != 0U) {
        return false;
    }
    const std::uint64_t remainder = value & (alignment - 1U);
    if (remainder == 0U) {
        *output = value;
        return true;
    }
    return CheckedAdd(value, alignment - remainder, output);
}

template <typename Integer>
std::atomic_ref<Integer> Atomic(Integer& value) noexcept {
    static_assert(std::is_integral_v<Integer>);
    return std::atomic_ref<Integer>(value);
}

template <typename Integer>
std::atomic_ref<Integer> Atomic(const Integer& value) noexcept {
    static_assert(std::is_integral_v<Integer>);
    return std::atomic_ref<Integer>(const_cast<Integer&>(value));
}

bool StateAcceptsPublication(std::uint32_t state) noexcept {
    return state ==
               static_cast<std::uint32_t>(
                   RealtimeServerStateV1::kInitializing) ||
           state ==
               static_cast<std::uint32_t>(
                   RealtimeServerStateV1::kActive) ||
           state ==
               static_cast<std::uint32_t>(
                   RealtimeServerStateV1::kDraining);
}

template <typename Slot, typename Payload>
bool PublishSlot(Slot* slot, const Payload& payload) noexcept {
    if (slot == nullptr ||
        sizeof(Payload) > sizeof(slot->payload_words)) {
        return false;
    }
    std::atomic_ref<std::uint64_t> tag = Atomic(slot->publish_tag);
    std::uint64_t stable = tag.load(std::memory_order_acquire);
    if ((stable & 1U) != 0U ||
        stable > std::numeric_limits<std::uint64_t>::max() - 2U) {
        return false;
    }
    if (!tag.compare_exchange_strong(
            stable,
            stable + 1U,
            std::memory_order_acq_rel,
            std::memory_order_acquire)) {
        return false;
    }
    constexpr std::size_t word_count =
        (sizeof(Payload) + sizeof(std::uint64_t) - 1U) /
        sizeof(std::uint64_t);
    static_assert(
        word_count <=
        std::tuple_size_v<decltype(slot->payload_words)>);
    std::array<std::uint64_t, word_count> words{};
    std::memcpy(words.data(), &payload, sizeof(payload));
    for (std::size_t index = 0U; index < words.size(); ++index) {
        Atomic(slot->payload_words[index])
            .store(words[index], std::memory_order_relaxed);
    }
    tag.store(stable + 2U, std::memory_order_release);
    return true;
}

bool TickSlotContainsSequence(
    const RealtimeWireTickSlotV1& slot,
    std::uint64_t expected_sequence) noexcept {
    constexpr std::size_t sequence_word =
        offsetof(
            RealtimeWireCommonRecordV1,
            tick_stream_sequence) /
        sizeof(std::uint64_t);
    static_assert(sequence_word < 56U);
    for (std::size_t attempt = 0U; attempt < 3U; ++attempt) {
        const std::uint64_t begin =
            Atomic(slot.publish_tag).load(std::memory_order_acquire);
        if (begin == 0U || (begin & 1U) != 0U) {
            return false;
        }
        const std::uint64_t sequence =
            Atomic(slot.payload_words[sequence_word])
                .load(std::memory_order_relaxed);
        std::atomic_thread_fence(std::memory_order_acq_rel);
        const std::uint64_t end =
            Atomic(slot.publish_tag).load(std::memory_order_acquire);
        if (begin == end && (end & 1U) == 0U) {
            return sequence == expected_sequence;
        }
    }
    return false;
}

void AtomicMaximum(
    std::uint64_t* destination,
    std::uint64_t value) noexcept {
    std::atomic_ref<std::uint64_t> target = Atomic(*destination);
    std::uint64_t current = target.load(std::memory_order_acquire);
    while (current < value &&
           !target.compare_exchange_weak(
               current,
               value,
               std::memory_order_release,
               std::memory_order_acquire)) {
    }
}

struct LayoutRegion final {
    RealtimeRegionKindV1 kind = RealtimeRegionKindV1::kReserved8;
    std::uint64_t offset = 0U;
    std::uint64_t length = 0U;
    std::uint64_t stride = 0U;
    std::uint64_t count = 0U;
    std::uint64_t capacity = 0U;
    std::uint32_t alignment = 0U;
};

struct alignas(64) RingSlotLock final {
    std::atomic_flag flag = ATOMIC_FLAG_INIT;
};
static_assert(sizeof(RingSlotLock) == 64U);

bool AppendRegion(
    RealtimeRegionKindV1 kind,
    std::uint64_t stride,
    std::uint64_t count,
    std::uint32_t alignment,
    std::uint64_t* cursor,
    LayoutRegion* output) noexcept {
    if (cursor == nullptr || output == nullptr || stride == 0U ||
        alignment == 0U) {
        return false;
    }
    std::uint64_t offset = 0U;
    std::uint64_t length = 0U;
    std::uint64_t end = 0U;
    if (!AlignUp(
            *cursor,
            static_cast<std::uint64_t>(alignment),
            &offset) ||
        !CheckedMultiply(stride, count, &length) ||
        !CheckedAdd(offset, length, &end)) {
        return false;
    }
    output->kind = kind;
    output->offset = offset;
    output->length = length;
    output->stride = stride;
    output->count = count;
    output->capacity = count;
    output->alignment = alignment;
    *cursor = end;
    return true;
}

bool ValidSocketPath(const std::filesystem::path& path) noexcept {
    try {
        if (!path.is_absolute() || path.filename().empty() ||
            path.filename() == "." || path.filename() == "..") {
            return false;
        }
        const std::string native = path.string();
        sockaddr_un address{};
        return native.size() < sizeof(address.sun_path);
    } catch (...) {
        return false;
    }
}

}  // namespace

std::string_view RealtimeSharedServiceCreateErrorNameV1(
    RealtimeSharedServiceCreateErrorV1 error) noexcept {
    switch (error) {
        case RealtimeSharedServiceCreateErrorV1::kNone:
            return "none";
        case RealtimeSharedServiceCreateErrorV1::kNullOutput:
            return "null_output";
        case RealtimeSharedServiceCreateErrorV1::kInvalidConfiguration:
            return "invalid_configuration";
        case RealtimeSharedServiceCreateErrorV1::kLayoutOverflow:
            return "layout_overflow";
        case RealtimeSharedServiceCreateErrorV1::kMappingCreateFailed:
            return "mapping_create_failed";
        case RealtimeSharedServiceCreateErrorV1::kReadOnlyHandleFailed:
            return "read_only_handle_failed";
        case RealtimeSharedServiceCreateErrorV1::kSealFailed:
            return "seal_failed";
        case RealtimeSharedServiceCreateErrorV1::kSocketCreateFailed:
            return "socket_create_failed";
        case RealtimeSharedServiceCreateErrorV1::kSocketPathExists:
            return "socket_path_exists";
        case RealtimeSharedServiceCreateErrorV1::kSocketBindFailed:
            return "socket_bind_failed";
        case RealtimeSharedServiceCreateErrorV1::kResourceExhausted:
            return "resource_exhausted";
        case RealtimeSharedServiceCreateErrorV1::kUnexpectedFailure:
            return "unexpected_failure";
    }
    return "unknown";
}

class RealtimeSharedMarketServiceV1::Impl final {
public:
    explicit Impl(RealtimeSharedServiceConfigV1 config)
        : config_(std::move(config)) {}

    ~Impl() {
        StopControl();
        SafeUnlinkSocket();
        if (listener_fd_ >= 0) {
            static_cast<void>(::close(listener_fd_));
        }
        if (stop_event_fd_ >= 0) {
            static_cast<void>(::close(stop_event_fd_));
        }
        if (prefix_event_fd_ >= 0) {
            static_cast<void>(::close(prefix_event_fd_));
        }
        if (read_only_fd_ >= 0) {
            static_cast<void>(::close(read_only_fd_));
        }
        if (mapping_ != MAP_FAILED) {
            static_cast<void>(::munmap(
                mapping_, static_cast<std::size_t>(mapping_bytes_)));
        }
        if (memfd_ >= 0) {
            static_cast<void>(::close(memfd_));
        }
    }

    [[nodiscard]] RealtimeSharedServiceCreateErrorV1 Initialize(
        int* system_error_number) noexcept {
        SetSystemError(system_error_number, 0);
        if constexpr (std::endian::native != std::endian::little) {
            return RealtimeSharedServiceCreateErrorV1::
                kInvalidConfiguration;
        }
        if (config_.registry == nullptr || config_.registry->empty() ||
            common::IsZeroIdentity(config_.run_id) ||
            config_.session_epoch == 0U || config_.trade_date == 0U ||
            config_.tick_ring_capacity == 0U ||
            config_.maximum_mapping_bytes <
                sizeof(RealtimeWireHeaderV1) ||
            !ValidSocketPath(config_.control_socket_path) ||
            config_.registry->size() >
                static_cast<std::size_t>(
                    std::numeric_limits<std::uint32_t>::max()) ||
            config_.kline_windows.size() >
                market::kKLineMaximumWindowsV1) {
            return RealtimeSharedServiceCreateErrorV1::
                kInvalidConfiguration;
        }
        for (std::size_t index = 0U;
             index < config_.kline_windows.size();
             ++index) {
            if (config_.kline_windows[index].window_id == 0U ||
                config_.kline_windows[index].duration_ns == 0U) {
                return RealtimeSharedServiceCreateErrorV1::
                    kInvalidConfiguration;
            }
            for (std::size_t prior = 0U; prior < index; ++prior) {
                if (config_.kline_windows[prior].window_id ==
                    config_.kline_windows[index].window_id) {
                    return RealtimeSharedServiceCreateErrorV1::
                        kInvalidConfiguration;
                }
            }
        }
        std::sort(
            config_.kline_windows.begin(),
            config_.kline_windows.end(),
            [](const market::KLineWindowSpecV1& left,
               const market::KLineWindowSpecV1& right) noexcept {
                return left.window_id < right.window_id;
            });

        const std::uint64_t instrument_count =
            static_cast<std::uint64_t>(config_.registry->size());
        const std::uint64_t window_count =
            static_cast<std::uint64_t>(
                config_.kline_windows.size());
        std::uint64_t logical_kline_count = 0U;
        std::uint64_t physical_kline_count = 0U;
        if (!CheckedMultiply(
                instrument_count,
                window_count,
                &logical_kline_count) ||
            !CheckedMultiply(
                logical_kline_count,
                kRealtimeKLineTableCountV1,
                &physical_kline_count)) {
            return RealtimeSharedServiceCreateErrorV1::
                kLayoutOverflow;
        }
        std::uint64_t blob_bytes = 0U;
        for (const market::InstrumentRegistryEntryV1& entry :
             config_.registry->entries()) {
            const std::uint64_t source_bytes =
                static_cast<std::uint64_t>(
                    entry.key.security_id_source.size());
            const std::uint64_t id_bytes =
                static_cast<std::uint64_t>(
                    entry.key.security_id.size());
            if (entry.key.security_id_source.size() >
                    static_cast<std::size_t>(
                        std::numeric_limits<std::uint32_t>::max()) ||
                entry.key.security_id.size() >
                    static_cast<std::size_t>(
                        std::numeric_limits<std::uint32_t>::max()) ||
                !CheckedAdd(blob_bytes, source_bytes, &blob_bytes) ||
                !CheckedAdd(blob_bytes, id_bytes, &blob_bytes)) {
                return RealtimeSharedServiceCreateErrorV1::
                    kLayoutOverflow;
            }
        }

        std::uint64_t cursor = sizeof(RealtimeWireHeaderV1);
        if (!AppendRegion(
                RealtimeRegionKindV1::kInstrumentRows,
                sizeof(RealtimeWireInstrumentV1),
                instrument_count,
                alignof(RealtimeWireInstrumentV1),
                &cursor,
                &regions_[0U]) ||
            !AppendRegion(
                RealtimeRegionKindV1::kInstrumentKeyBlob,
                1U,
                blob_bytes,
                1U,
                &cursor,
                &regions_[1U]) ||
            !AppendRegion(
                RealtimeRegionKindV1::kKLineWindows,
                sizeof(RealtimeWireKLineWindowV1),
                window_count,
                alignof(RealtimeWireKLineWindowV1),
                &cursor,
                &regions_[2U]) ||
            !AppendRegion(
                RealtimeRegionKindV1::kLatestSnapshots,
                sizeof(RealtimeWireSnapshotSlotV1),
                instrument_count,
                alignof(RealtimeWireSnapshotSlotV1),
                &cursor,
                &regions_[3U]) ||
            !AppendRegion(
                RealtimeRegionKindV1::kLatestTicks,
                sizeof(RealtimeWireTickSlotV1),
                instrument_count,
                alignof(RealtimeWireTickSlotV1),
                &cursor,
                &regions_[4U]) ||
            !AppendRegion(
                RealtimeRegionKindV1::kLatestKLines,
                sizeof(RealtimeWireKLineSlotV1),
                physical_kline_count,
                alignof(RealtimeWireKLineSlotV1),
                &cursor,
                &regions_[5U]) ||
            !AppendRegion(
                RealtimeRegionKindV1::kTickRingSlots,
                sizeof(RealtimeWireTickSlotV1),
                config_.tick_ring_capacity,
                alignof(RealtimeWireTickSlotV1),
                &cursor,
                &regions_[6U]) ||
            !AlignUp(cursor, kPageBytes, &mapping_bytes_) ||
            mapping_bytes_ > config_.maximum_mapping_bytes ||
            mapping_bytes_ >
                static_cast<std::uint64_t>(
                    std::numeric_limits<std::size_t>::max()) ||
            mapping_bytes_ >
                static_cast<std::uint64_t>(
                    std::numeric_limits<off_t>::max())) {
            return RealtimeSharedServiceCreateErrorV1::
                kLayoutOverflow;
        }
        regions_[7U].kind = RealtimeRegionKindV1::kReserved8;
        regions_[7U].offset = mapping_bytes_;
        regions_[7U].alignment = 1U;
        regions_[8U].kind = RealtimeRegionKindV1::kReserved9;
        regions_[8U].offset = mapping_bytes_;
        regions_[8U].alignment = 1U;

        memfd_ = ::memfd_create(
            "l2flow-realtime-v1", MFD_CLOEXEC | MFD_ALLOW_SEALING);
        if (memfd_ < 0 ||
            ::ftruncate(memfd_, static_cast<off_t>(mapping_bytes_)) !=
                0) {
            SetSystemError(system_error_number, errno);
            return RealtimeSharedServiceCreateErrorV1::
                kMappingCreateFailed;
        }
        mapping_ = ::mmap(
            nullptr,
            static_cast<std::size_t>(mapping_bytes_),
            PROT_READ | PROT_WRITE,
            MAP_SHARED,
            memfd_,
            0);
        if (mapping_ == MAP_FAILED) {
            SetSystemError(system_error_number, errno);
            return RealtimeSharedServiceCreateErrorV1::
                kMappingCreateFailed;
        }
        std::memset(
            mapping_, 0, static_cast<std::size_t>(mapping_bytes_));

        const std::string proc_path =
            "/proc/self/fd/" + std::to_string(memfd_);
        read_only_fd_ = ::open(proc_path.c_str(), O_RDONLY | O_CLOEXEC);
        if (read_only_fd_ < 0) {
            SetSystemError(system_error_number, errno);
            return RealtimeSharedServiceCreateErrorV1::
                kReadOnlyHandleFailed;
        }
        const int seals = F_SEAL_GROW | F_SEAL_SHRINK |
                          F_SEAL_FUTURE_WRITE | F_SEAL_SEAL;
        if (::fcntl(memfd_, F_ADD_SEALS, seals) != 0) {
            SetSystemError(system_error_number, errno);
            return RealtimeSharedServiceCreateErrorV1::kSealFailed;
        }

        header_ = static_cast<RealtimeWireHeaderV1*>(mapping_);
        InitializeHeader();
        if (!InitializeRegistry() || !InitializeWindows()) {
            return RealtimeSharedServiceCreateErrorV1::
                kUnexpectedFailure;
        }
        try {
            ring_locks_ = std::make_unique<RingSlotLock[]>(
                static_cast<std::size_t>(
                    config_.tick_ring_capacity));
            for (std::uint64_t index = 0U;
                 index < config_.tick_ring_capacity;
                 ++index) {
                ring_locks_[static_cast<std::size_t>(index)]
                    .flag.clear(std::memory_order_relaxed);
            }
        } catch (...) {
            return RealtimeSharedServiceCreateErrorV1::
                kResourceExhausted;
        }
        return BindSocket(system_error_number);
    }

    [[nodiscard]] bool Start(int* system_error_number) noexcept {
        SetSystemError(system_error_number, 0);
        if (header_ == nullptr || listener_fd_ < 0 ||
            stop_event_fd_ < 0 || prefix_event_fd_ < 0 ||
            control_thread_.joinable() ||
            control_stop_requested_.load(std::memory_order_acquire)) {
            SetSystemError(system_error_number, EINVAL);
            return false;
        }
        if ((Atomic(header_->flags).load(std::memory_order_acquire) &
             kRealtimeHeaderCoverageLostV1) != 0U) {
            SetSystemError(system_error_number, EIO);
            return false;
        }
        std::uint32_t expected_state =
            static_cast<std::uint32_t>(
                RealtimeServerStateV1::kInitializing);
        if (!Atomic(header_->server_state)
                 .compare_exchange_strong(
                     expected_state,
                     static_cast<std::uint32_t>(
                         RealtimeServerStateV1::kActive),
                     std::memory_order_release,
                     std::memory_order_acquire)) {
            SetSystemError(system_error_number, EINVAL);
            return false;
        }
        UpdateHeartbeatNow();
        if (Atomic(header_->server_state)
                .load(std::memory_order_acquire) ==
            static_cast<std::uint32_t>(
                RealtimeServerStateV1::kFailed)) {
            SetSystemError(system_error_number, EIO);
            return false;
        }
        try {
            control_thread_ = std::thread([this] { ControlLoop(); });
            return true;
        } catch (...) {
            MarkFailed();
            SetSystemError(system_error_number, EAGAIN);
            return false;
        }
    }

    [[nodiscard]] bool PublishApplied(
        std::size_t registry_ordinal,
        const market::RealtimeHistoryRecordV1& record) noexcept {
        if (header_ == nullptr ||
            registry_ordinal >= config_.registry->size() ||
            !StateAcceptsPublication(
                Atomic(header_->server_state)
                    .load(std::memory_order_acquire))) {
            return false;
        }
        const RealtimeWireInstrumentV1& instrument =
            instrument_rows()[registry_ordinal];
        if (instrument.instrument_id != record.instrument_id()) {
            MarkCoverageLost();
            return false;
        }

        bool published = false;
        if (market::IsSnapshotEventKindV1(record.kind())) {
            RealtimeWireSnapshotPayloadV1 payload{};
            published =
                record.tick_stream_sequence() == 0U &&
                ProjectSnapshotWireV1(
                    record, registry_ordinal, &payload) &&
                PublishSlot(
                    &snapshot_slots()[registry_ordinal], payload);
        } else if (market::IsTickEventKindV1(record.kind())) {
            RealtimeWireTickPayloadV1 payload{};
            published =
                record.tick_stream_sequence() != 0U &&
                ProjectTickWireV1(
                    record, registry_ordinal, &payload) &&
                PublishRing(payload) &&
                PublishSlot(
                    &latest_tick_slots()[registry_ordinal],
                    payload);
        }
        if (!published) {
            MarkCoverageLost();
            return false;
        }
        Atomic(header_->published_records)
            .fetch_add(1U, std::memory_order_release);
        return true;
    }

    void MarkCoverageLost() noexcept {
        if (header_ == nullptr) {
            return;
        }
        Atomic(header_->flags)
            .fetch_or(
                kRealtimeHeaderCoverageLostV1,
                std::memory_order_release);
        Atomic(header_->server_state)
            .store(
                static_cast<std::uint32_t>(
                    RealtimeServerStateV1::kFailed),
                std::memory_order_release);
    }

    [[nodiscard]] bool PublishKLineGeneration(
        const market::RealtimeKLineGenerationV1& generation) noexcept {
        if (kline_publication_in_progress_.test_and_set(
                std::memory_order_acquire)) {
            MarkCoverageLost();
            return false;
        }
        struct PublicationGuard final {
            std::atomic_flag& flag;
            ~PublicationGuard() {
                flag.clear(std::memory_order_release);
            }
        } publication_guard{kline_publication_in_progress_};

        if (header_ == nullptr ||
            !StateAcceptsPublication(
                Atomic(header_->server_state)
                    .load(std::memory_order_acquire)) ||
            generation.watermark().run_id != config_.run_id ||
            generation.watermark().trade_date != config_.trade_date ||
            generation.watermark().registry_version !=
                config_.registry->registry_version() ||
            generation.watermark().registry_sha256 !=
                config_.registry->registry_sha256() ||
            generation.windows().size() !=
                config_.kline_windows.size()) {
            MarkCoverageLost();
            return false;
        }
        const std::span<const market::KLineWindowSpecV1>
            generation_windows = generation.windows();
        for (std::size_t index = 0U;
             index < generation_windows.size();
             ++index) {
            if (generation_windows[index].window_id !=
                    config_.kline_windows[index].window_id ||
                generation_windows[index].duration_ns !=
                    config_.kline_windows[index].duration_ns) {
                MarkCoverageLost();
                return false;
            }
        }
        const std::uint64_t generation_number =
            generation.watermark().generation;
        const std::uint64_t completed_generation =
            Atomic(header_->kline_generation)
                .load(std::memory_order_acquire);
        if (completed_generation ==
                std::numeric_limits<std::uint64_t>::max() ||
            generation_number != completed_generation + 1U) {
            MarkCoverageLost();
            return false;
        }
        const std::size_t logical_slot_count =
            config_.registry->size() * config_.kline_windows.size();
        const std::size_t table_index =
            static_cast<std::size_t>(
                generation_number % kRealtimeKLineTableCountV1);
        const std::size_t table_offset =
            table_index * logical_slot_count;
        for (std::size_t ordinal = 0U;
             ordinal < config_.registry->size();
             ++ordinal) {
            const std::uint32_t instrument_id =
                instrument_rows()[ordinal].instrument_id;
            for (std::size_t window_index = 0U;
                 window_index < config_.kline_windows.size();
                 ++window_index) {
                market::KLineBarV1 bar{};
                const market::KLineQueryErrorV1 error =
                    generation.GetLatestBar(
                        instrument_id,
                        config_.kline_windows[window_index].window_id,
                        &bar);
                RealtimeWireKLinePayloadV1 payload{};
                const std::size_t slot_index =
                    table_offset +
                    ordinal * config_.kline_windows.size() +
                    window_index;
                if (error == market::KLineQueryErrorV1::kNotFound) {
                    // Publish an explicit empty row into the inactive table
                    // so data from generation-2 can never reappear if an
                    // upstream invariant is violated.
                    payload.generation = generation_number;
                    payload.trade_date = config_.trade_date;
                    payload.instrument_id = instrument_id;
                    payload.window_id =
                        config_.kline_windows[window_index].window_id;
                    payload.window_duration_ns =
                        config_.kline_windows[window_index].duration_ns;
                } else if (
                    error != market::KLineQueryErrorV1::kNone ||
                    !ProjectKLineWireV1(
                        generation_number, bar, &payload)) {
                    MarkCoverageLost();
                    return false;
                }
                if (
                    !PublishSlot(
                        &kline_slots()[slot_index], payload)) {
                    MarkCoverageLost();
                    return false;
                }
            }
        }
        if (!StateAcceptsPublication(
                Atomic(header_->server_state)
                    .load(std::memory_order_acquire)) ||
            (Atomic(header_->flags).load(std::memory_order_acquire) &
             kRealtimeHeaderCoverageLostV1) != 0U) {
            MarkCoverageLost();
            return false;
        }
        Atomic(header_->kline_generation)
            .store(generation_number, std::memory_order_release);
        return true;
    }

    void MarkDraining() noexcept {
        if (header_ == nullptr) {
            return;
        }
        std::uint32_t expected =
            static_cast<std::uint32_t>(
                RealtimeServerStateV1::kActive);
        static_cast<void>(
            Atomic(header_->server_state)
                .compare_exchange_strong(
                    expected,
                    static_cast<std::uint32_t>(
                        RealtimeServerStateV1::kDraining),
                    std::memory_order_release,
                    std::memory_order_acquire));
    }

    [[nodiscard]] bool MarkStoppedClean(
        std::uint64_t final_admitted_tick_sequence) noexcept {
        // Pipeline shutdown has already joined every producer. Finish any
        // budgeted prefix work synchronously before validating the terminal
        // sequence; this is off the tick hot path.
        while (header_ != nullptr &&
               AdvanceContiguousTickPrefix(
                   kControlPrefixAdvanceBudget)) {
        }
        if (header_ == nullptr ||
            (Atomic(header_->flags).load(std::memory_order_acquire) &
             kRealtimeHeaderCoverageLostV1) != 0U ||
            Atomic(header_->tick_highest_published_sequence)
                    .load(std::memory_order_acquire) !=
                final_admitted_tick_sequence ||
            Atomic(header_->tick_contiguous_published_sequence)
                    .load(std::memory_order_acquire) !=
                final_admitted_tick_sequence) {
            MarkCoverageLost();
            return false;
        }
        Atomic(header_->server_state)
            .store(
                static_cast<std::uint32_t>(
                    RealtimeServerStateV1::kStoppedClean),
                std::memory_order_release);
        return true;
    }

    void MarkFailed() noexcept { MarkCoverageLost(); }

    void StopControl() noexcept {
        if (header_ != nullptr) {
            const std::uint32_t state =
                Atomic(header_->server_state)
                    .load(std::memory_order_acquire);
            if (state ==
                    static_cast<std::uint32_t>(
                        RealtimeServerStateV1::kInitializing) ||
                state ==
                    static_cast<std::uint32_t>(
                        RealtimeServerStateV1::kActive) ||
                state ==
                    static_cast<std::uint32_t>(
                        RealtimeServerStateV1::kDraining)) {
                // Stopping discovery/heartbeat before a terminal state would
                // leave a deceptively readable live mapping.
                MarkCoverageLost();
            }
        }
        bool expected = false;
        if (!control_stop_requested_.compare_exchange_strong(
                expected, true, std::memory_order_acq_rel)) {
            if (control_thread_.joinable()) {
                control_thread_.join();
            }
            return;
        }
        if (stop_event_fd_ >= 0) {
            const std::uint64_t one = 1U;
            ssize_t written = -1;
            do {
                written =
                    ::write(stop_event_fd_, &one, sizeof(one));
            } while (written < 0 && errno == EINTR);
            if (written != static_cast<ssize_t>(sizeof(one)) &&
                !(written < 0 && errno == EAGAIN)) {
                MarkCoverageLost();
            }
        }
        if (control_thread_.joinable()) {
            control_thread_.join();
        }
    }

    [[nodiscard]] std::uint64_t mapping_bytes() const noexcept {
        return mapping_bytes_;
    }

    [[nodiscard]] bool failed() const noexcept {
        return header_ == nullptr ||
               (Atomic(header_->flags).load(std::memory_order_acquire) &
                kRealtimeHeaderCoverageLostV1) != 0U ||
               Atomic(header_->server_state)
                       .load(std::memory_order_acquire) ==
                   static_cast<std::uint32_t>(
                       RealtimeServerStateV1::kFailed);
    }

    [[nodiscard]] const std::filesystem::path& socket_path()
        const noexcept {
        return config_.control_socket_path;
    }

private:
    void InitializeHeader() noexcept {
        header_->magic = kRealtimeShmMagicV1;
        header_->abi_major = kRealtimeWireMajorV1;
        header_->abi_minor = kRealtimeWireMinorV1;
        header_->header_bytes =
            static_cast<std::uint32_t>(
                sizeof(RealtimeWireHeaderV1));
        header_->endian_marker = kRealtimeLittleEndianMarkerV1;
        header_->server_state = static_cast<std::uint32_t>(
            RealtimeServerStateV1::kInitializing);
        header_->total_mapping_bytes = mapping_bytes_;
        for (std::size_t index = 0U; index < config_.run_id.size();
             ++index) {
            header_->run_id[index] =
                std::to_integer<std::uint8_t>(
                    config_.run_id[index]);
        }
        header_->session_epoch = config_.session_epoch;
        header_->trade_date = config_.trade_date;
        header_->flags = config_.kline_windows.empty()
                             ? 0U
                             : kRealtimeHeaderKLineEnabledV1;
        header_->registry_version =
            config_.registry->registry_version();
        for (std::size_t index = 0U;
             index < header_->registry_sha256.size();
             ++index) {
            header_->registry_sha256[index] =
                std::to_integer<std::uint8_t>(
                    config_.registry->registry_sha256()[index]);
        }
        header_->instrument_count =
            static_cast<std::uint32_t>(
                config_.registry->size());
        header_->window_count =
            static_cast<std::uint32_t>(
                config_.kline_windows.size());
        header_->region_count =
            static_cast<std::uint32_t>(
                kRealtimeWireRegionCountV1);
        header_->region_descriptor_bytes =
            static_cast<std::uint32_t>(
                sizeof(RealtimeWireRegionDescriptorV1));
        for (std::size_t index = 0U; index < regions_.size(); ++index) {
            const LayoutRegion& source = regions_[index];
            RealtimeWireRegionDescriptorV1& destination =
                header_->regions[index];
            destination.kind =
                static_cast<std::uint32_t>(source.kind);
            destination.schema_major = kRealtimeWireMajorV1;
            destination.schema_minor = kRealtimeWireMinorV1;
            destination.offset = source.offset;
            destination.length = source.length;
            destination.element_stride = source.stride;
            destination.element_count = source.count;
            destination.capacity = source.capacity;
            destination.alignment = source.alignment;
        }
    }

    [[nodiscard]] bool InitializeRegistry() noexcept {
        std::vector<const market::InstrumentRegistryEntryV1*>
            ordinal_entries;
        try {
            ordinal_entries.assign(config_.registry->size(), nullptr);
        } catch (...) {
            return false;
        }
        for (const market::InstrumentRegistryEntryV1& entry :
             config_.registry->entries()) {
            const market::InstrumentRegistryLookupResultV1 lookup =
                config_.registry->LookupById(entry.instrument_id);
            if (!lookup.known() ||
                lookup.registry_ordinal >= ordinal_entries.size() ||
                ordinal_entries[lookup.registry_ordinal] != nullptr) {
                return false;
            }
            ordinal_entries[lookup.registry_ordinal] = &entry;
        }
        std::uint64_t blob_cursor = 0U;
        auto* const blob =
            static_cast<std::byte*>(mapping_) + regions_[1U].offset;
        for (std::size_t ordinal = 0U;
             ordinal < ordinal_entries.size();
             ++ordinal) {
            const market::InstrumentRegistryEntryV1* const entry =
                ordinal_entries[ordinal];
            if (entry == nullptr) {
                return false;
            }
            RealtimeWireInstrumentV1& row =
                instrument_rows()[ordinal];
            row.instrument_id = entry->instrument_id;
            row.market =
                static_cast<std::uint8_t>(entry->key.market);
            row.quantity_unit =
                static_cast<std::uint8_t>(entry->quantity_unit);
            row.security_type =
                static_cast<std::uint8_t>(entry->security_type);
            row.asset_scope =
                static_cast<std::uint8_t>(entry->asset_scope);
            row.security_id_source_offset = blob_cursor;
            row.security_id_source_length =
                static_cast<std::uint32_t>(
                    entry->key.security_id_source.size());
            if (!entry->key.security_id_source.empty()) {
                std::memcpy(
                    blob + blob_cursor,
                    entry->key.security_id_source.data(),
                    entry->key.security_id_source.size());
            }
            blob_cursor += static_cast<std::uint64_t>(
                entry->key.security_id_source.size());
            row.security_id_offset = blob_cursor;
            row.security_id_length =
                static_cast<std::uint32_t>(
                    entry->key.security_id.size());
            std::memcpy(
                blob + blob_cursor,
                entry->key.security_id.data(),
                entry->key.security_id.size());
            blob_cursor += static_cast<std::uint64_t>(
                entry->key.security_id.size());
        }
        return blob_cursor == regions_[1U].length;
    }

    [[nodiscard]] bool InitializeWindows() noexcept {
        for (std::size_t index = 0U;
             index < config_.kline_windows.size();
             ++index) {
            window_rows()[index].window_id =
                config_.kline_windows[index].window_id;
            window_rows()[index].duration_ns =
                config_.kline_windows[index].duration_ns;
        }
        return true;
    }

    [[nodiscard]] RealtimeSharedServiceCreateErrorV1 BindSocket(
        int* system_error_number) noexcept {
        const std::filesystem::path parent =
            config_.control_socket_path.parent_path();
        int directory_flags = O_RDONLY | O_DIRECTORY | O_CLOEXEC;
#ifdef O_NOFOLLOW
        directory_flags |= O_NOFOLLOW;
#endif
        const int directory_fd =
            ::open(parent.c_str(), directory_flags);
        struct stat directory_stat {};
        const bool directory_ok =
            directory_fd >= 0 &&
            ::fstat(directory_fd, &directory_stat) == 0 &&
            S_ISDIR(directory_stat.st_mode) &&
            directory_stat.st_uid == ::geteuid() &&
            (directory_stat.st_mode & 0077) == 0;
        const int directory_error =
            directory_fd < 0 ? errno : EACCES;
        if (directory_fd >= 0) {
            static_cast<void>(::close(directory_fd));
        }
        if (!directory_ok) {
            SetSystemError(system_error_number, directory_error);
            return RealtimeSharedServiceCreateErrorV1::
                kInvalidConfiguration;
        }
        struct stat existing {};
        if (::lstat(config_.control_socket_path.c_str(), &existing) ==
            0) {
            SetSystemError(system_error_number, EEXIST);
            return RealtimeSharedServiceCreateErrorV1::
                kSocketPathExists;
        }
        if (errno != ENOENT) {
            SetSystemError(system_error_number, errno);
            return RealtimeSharedServiceCreateErrorV1::
                kSocketBindFailed;
        }

        listener_fd_ = ::socket(
            AF_UNIX,
            SOCK_SEQPACKET | SOCK_NONBLOCK | SOCK_CLOEXEC,
            0);
        if (listener_fd_ < 0) {
            SetSystemError(system_error_number, errno);
            return RealtimeSharedServiceCreateErrorV1::
                kSocketCreateFailed;
        }
        sockaddr_un address{};
        address.sun_family = AF_UNIX;
        const std::string path = config_.control_socket_path.string();
        std::memcpy(
            address.sun_path, path.c_str(), path.size() + 1U);
        const mode_t prior_mask = ::umask(0077);
        const int bind_result = ::bind(
            listener_fd_,
            reinterpret_cast<const sockaddr*>(&address),
            static_cast<socklen_t>(
                offsetof(sockaddr_un, sun_path) + path.size() + 1U));
        static_cast<void>(::umask(prior_mask));
        if (bind_result != 0) {
            SetSystemError(system_error_number, errno);
            return RealtimeSharedServiceCreateErrorV1::
                kSocketBindFailed;
        }
        struct stat socket_stat {};
        if (::lstat(path.c_str(), &socket_stat) != 0 ||
            !S_ISSOCK(socket_stat.st_mode) ||
            socket_stat.st_uid != ::geteuid()) {
            SetSystemError(system_error_number, errno == 0 ? EACCES : errno);
            return RealtimeSharedServiceCreateErrorV1::
                kSocketBindFailed;
        }
        socket_device_ = socket_stat.st_dev;
        socket_inode_ = socket_stat.st_ino;
        socket_bound_ = true;
        if (::chmod(path.c_str(), 0600) != 0 ||
            ::listen(listener_fd_, 16) != 0) {
            SetSystemError(system_error_number, errno);
            SafeUnlinkSocket();
            return RealtimeSharedServiceCreateErrorV1::
                kSocketBindFailed;
        }
        stop_event_fd_ = ::eventfd(
            0U, EFD_CLOEXEC | EFD_NONBLOCK);
        if (stop_event_fd_ < 0) {
            SetSystemError(system_error_number, errno);
            return RealtimeSharedServiceCreateErrorV1::
                kSocketCreateFailed;
        }
        prefix_event_fd_ = ::eventfd(
            0U, EFD_CLOEXEC | EFD_NONBLOCK);
        if (prefix_event_fd_ < 0) {
            SetSystemError(system_error_number, errno);
            return RealtimeSharedServiceCreateErrorV1::
                kSocketCreateFailed;
        }
        return RealtimeSharedServiceCreateErrorV1::kNone;
    }

    void SafeUnlinkSocket() noexcept {
        if (!socket_bound_) {
            return;
        }
        struct stat current {};
        if (::lstat(config_.control_socket_path.c_str(), &current) ==
                0 &&
            current.st_dev == socket_device_ &&
            current.st_ino == socket_inode_ &&
            S_ISSOCK(current.st_mode)) {
            static_cast<void>(
                ::unlink(config_.control_socket_path.c_str()));
        }
        socket_bound_ = false;
    }

    [[nodiscard]] bool PublishRing(
        const RealtimeWireTickPayloadV1& payload) noexcept {
        const std::uint64_t sequence =
            payload.common.tick_stream_sequence;
        if (sequence == 0U || ring_locks_ == nullptr) {
            return false;
        }
        const std::uint64_t index =
            (sequence - 1U) % config_.tick_ring_capacity;
        std::atomic_flag& lock =
            ring_locks_[static_cast<std::size_t>(index)].flag;
        if (lock.test_and_set(std::memory_order_acquire)) {
            return false;
        }
        RealtimeWireTickSlotV1& slot =
            ring_slots()[static_cast<std::size_t>(index)];
        bool result = false;
        const std::uint64_t stable =
            Atomic(slot.publish_tag).load(std::memory_order_acquire);
        std::uint64_t existing_sequence = 0U;
        if (stable != 0U && (stable & 1U) == 0U) {
            constexpr std::size_t sequence_word =
                offsetof(
                    RealtimeWireCommonRecordV1,
                    tick_stream_sequence) /
                sizeof(std::uint64_t);
            existing_sequence =
                Atomic(slot.payload_words[sequence_word])
                    .load(std::memory_order_acquire);
        }
        if ((stable & 1U) == 0U &&
            existing_sequence < sequence &&
            PublishSlot(&slot, payload)) {
            result = true;
        }
        lock.clear(std::memory_order_release);
        if (!result) {
            return false;
        }
        AtomicMaximum(
            &header_->tick_highest_published_sequence, sequence);
        if (AdvanceContiguousTickPrefix(kHotPrefixAdvanceBudget) &&
            !RequestPrefixAdvance()) {
            MarkCoverageLost();
            return false;
        }
        return true;
    }

    // Returns true only when the budget was exhausted and another immediate
    // pass may be useful.
    [[nodiscard]] bool AdvanceContiguousTickPrefix(
        std::size_t budget) noexcept {
        if (budget == 0U) {
            return true;
        }
        std::atomic_ref<std::uint64_t> contiguous =
            Atomic(header_->tick_contiguous_published_sequence);
        std::uint64_t current =
            contiguous.load(std::memory_order_acquire);
        std::size_t advanced = 0U;
        while (advanced < budget) {
            if (current == std::numeric_limits<std::uint64_t>::max()) {
                return false;
            }
            const std::uint64_t expected = current + 1U;
            // A producer publishes its slot before release-advancing
            // highest. Another producer must not use that early-visible slot
            // to move contiguous beyond highest. The producer that owns
            // expected will call this function again after publishing
            // highest, so stopping here cannot strand a complete prefix.
            if (Atomic(header_->tick_highest_published_sequence)
                    .load(std::memory_order_acquire) < expected) {
                return false;
            }
            const std::uint64_t index =
                (expected - 1U) % config_.tick_ring_capacity;
            if (!TickSlotContainsSequence(
                    ring_slots()[static_cast<std::size_t>(index)],
                    expected)) {
                return false;
            }
            if (!contiguous.compare_exchange_weak(
                    current,
                    expected,
                    std::memory_order_release,
                    std::memory_order_acquire)) {
                continue;
            }
            current = expected;
            ++advanced;
        }
        return true;
    }

    [[nodiscard]] bool RequestPrefixAdvance() noexcept {
        bool expected = false;
        if (!prefix_notification_pending_.compare_exchange_strong(
                expected, true, std::memory_order_acq_rel)) {
            return true;
        }
        const std::uint64_t one = 1U;
        ssize_t written = -1;
        do {
            written = ::write(prefix_event_fd_, &one, sizeof(one));
        } while (written < 0 && errno == EINTR);
        if (written == static_cast<ssize_t>(sizeof(one))) {
            return true;
        }
        prefix_notification_pending_.store(
            false, std::memory_order_release);
        return false;
    }

    void ControlLoop() noexcept {
        std::array<pollfd, 3U> descriptors{};
        descriptors[0U].fd = listener_fd_;
        descriptors[0U].events = static_cast<short>(POLLIN);
        descriptors[1U].fd = stop_event_fd_;
        descriptors[1U].events = static_cast<short>(POLLIN);
        descriptors[2U].fd = prefix_event_fd_;
        descriptors[2U].events = static_cast<short>(POLLIN);
        while (!control_stop_requested_.load(
            std::memory_order_acquire)) {
            const int ready = ::poll(
                descriptors.data(),
                static_cast<nfds_t>(descriptors.size()),
                1000);
            if (ready < 0) {
                if (errno == EINTR) {
                    continue;
                }
                MarkFailed();
                return;
            }
            if (ready == 0) {
                UpdateHeartbeatNow();
                continue;
            }
            if ((descriptors[1U].revents & POLLIN) != 0) {
                return;
            }
            if ((descriptors[1U].revents &
                 (POLLERR | POLLHUP | POLLNVAL)) != 0) {
                MarkFailed();
                return;
            }
            if ((descriptors[2U].revents &
                 (POLLERR | POLLHUP | POLLNVAL)) != 0) {
                MarkFailed();
                return;
            }
            if ((descriptors[0U].revents &
                 (POLLERR | POLLHUP | POLLNVAL)) != 0) {
                MarkFailed();
                return;
            }
            if ((descriptors[2U].revents & POLLIN) != 0) {
                std::uint64_t notifications = 0U;
                ssize_t read_result = -1;
                do {
                    read_result = ::read(
                        prefix_event_fd_,
                        &notifications,
                        sizeof(notifications));
                } while (read_result < 0 && errno == EINTR);
                if (read_result !=
                    static_cast<ssize_t>(sizeof(notifications))) {
                    MarkFailed();
                    return;
                }
                prefix_notification_pending_.store(
                    false, std::memory_order_release);
                if (AdvanceContiguousTickPrefix(
                        kControlPrefixAdvanceBudget) &&
                    !RequestPrefixAdvance()) {
                    MarkFailed();
                    return;
                }
                UpdateHeartbeatNow();
            }
            if ((descriptors[0U].revents & POLLIN) == 0) {
                continue;
            }
            for (;;) {
                const int client = ::accept4(
                    listener_fd_,
                    nullptr,
                    nullptr,
                    SOCK_NONBLOCK | SOCK_CLOEXEC);
                if (client < 0) {
                    if (errno == EAGAIN || errno == EWOULDBLOCK) {
                        break;
                    }
                    if (errno == EINTR) {
                        continue;
                    }
                    MarkFailed();
                    return;
                }
                UpdateHeartbeatNow();
                HandleClient(client);
                static_cast<void>(::close(client));
                UpdateHeartbeatNow();
            }
        }
    }

    void UpdateHeartbeatNow() noexcept {
        if (header_ == nullptr) {
            return;
        }
        timespec now{};
        if (::clock_gettime(CLOCK_MONOTONIC, &now) != 0 ||
            now.tv_sec < 0 || now.tv_nsec < 0) {
            MarkFailed();
            return;
        }
        constexpr std::uint64_t nanoseconds_per_second =
            1'000'000'000ULL;
        const std::uint64_t seconds =
            static_cast<std::uint64_t>(now.tv_sec);
        const std::uint64_t nanoseconds =
            static_cast<std::uint64_t>(now.tv_nsec);
        if (seconds >
            (std::numeric_limits<std::uint64_t>::max() -
             nanoseconds) /
                nanoseconds_per_second) {
            MarkFailed();
            return;
        }
        Atomic(header_->heartbeat_monotonic_ns)
            .store(
                seconds * nanoseconds_per_second + nanoseconds,
                std::memory_order_release);
    }

    void HandleClient(int client) noexcept {
        ucred credentials{};
        socklen_t credentials_size =
            static_cast<socklen_t>(sizeof(credentials));
        if (::getsockopt(
                client,
                SOL_SOCKET,
                SO_PEERCRED,
                &credentials,
                &credentials_size) != 0 ||
            credentials_size != sizeof(credentials) ||
            credentials.uid != ::geteuid()) {
            return;
        }
        pollfd descriptor{};
        descriptor.fd = client;
        descriptor.events = static_cast<short>(POLLIN);
        const int ready = ::poll(&descriptor, 1U, 250);
        if (ready <= 0 || (descriptor.revents & POLLIN) == 0) {
            return;
        }
        RealtimeControlRequestV1 request{};
        const ssize_t received = ::recv(
            client, &request, sizeof(request), MSG_TRUNC);
        RealtimeControlResponseV1 response{};
        response.magic = kRealtimeControlMagicV1;
        response.protocol_major = kRealtimeWireMajorV1;
        response.protocol_minor = kRealtimeWireMinorV1;
        response.message_bytes =
            static_cast<std::uint32_t>(sizeof(response));
        response.request_id = request.request_id;
        response.session_epoch = config_.session_epoch;
        response.total_mapping_bytes = mapping_bytes_;
        bool send_fd = false;
        if (received !=
                static_cast<ssize_t>(sizeof(request)) ||
            request.magic != kRealtimeControlMagicV1 ||
            request.message_bytes != sizeof(request) ||
            request.opcode != static_cast<std::uint16_t>(
                                  RealtimeControlOpcodeV1::
                                      kGetSession) ||
            request.flags != 0U || request.reserved0 != 0U ||
            request.reserved1 != 0U) {
            response.status = static_cast<std::uint16_t>(
                RealtimeControlStatusV1::kInvalidRequest);
        } else if (
            request.protocol_major != kRealtimeWireMajorV1 ||
            request.protocol_minor != kRealtimeWireMinorV1) {
            response.status = static_cast<std::uint16_t>(
                RealtimeControlStatusV1::kUnsupportedVersion);
        } else {
            response.status = static_cast<std::uint16_t>(
                RealtimeControlStatusV1::kOk);
            send_fd = true;
        }
        iovec vector{};
        vector.iov_base = &response;
        vector.iov_len = sizeof(response);
        msghdr message{};
        message.msg_iov = &vector;
        message.msg_iovlen = 1U;
        std::array<std::byte, CMSG_SPACE(sizeof(int))> control{};
        if (send_fd) {
            message.msg_control = control.data();
            message.msg_controllen = control.size();
            cmsghdr* const rights = CMSG_FIRSTHDR(&message);
            if (rights == nullptr) {
                return;
            }
            rights->cmsg_level = SOL_SOCKET;
            rights->cmsg_type = SCM_RIGHTS;
            rights->cmsg_len = CMSG_LEN(sizeof(int));
            std::memcpy(CMSG_DATA(rights), &read_only_fd_, sizeof(int));
        }
        static_cast<void>(
            ::sendmsg(client, &message, MSG_NOSIGNAL));
    }

    [[nodiscard]] RealtimeWireInstrumentV1* instrument_rows()
        const noexcept {
        return reinterpret_cast<RealtimeWireInstrumentV1*>(
            static_cast<std::byte*>(mapping_) + regions_[0U].offset);
    }

    [[nodiscard]] RealtimeWireKLineWindowV1* window_rows()
        const noexcept {
        return reinterpret_cast<RealtimeWireKLineWindowV1*>(
            static_cast<std::byte*>(mapping_) + regions_[2U].offset);
    }

    [[nodiscard]] RealtimeWireSnapshotSlotV1* snapshot_slots()
        const noexcept {
        return reinterpret_cast<RealtimeWireSnapshotSlotV1*>(
            static_cast<std::byte*>(mapping_) + regions_[3U].offset);
    }

    [[nodiscard]] RealtimeWireTickSlotV1* latest_tick_slots()
        const noexcept {
        return reinterpret_cast<RealtimeWireTickSlotV1*>(
            static_cast<std::byte*>(mapping_) + regions_[4U].offset);
    }

    [[nodiscard]] RealtimeWireKLineSlotV1* kline_slots()
        const noexcept {
        return reinterpret_cast<RealtimeWireKLineSlotV1*>(
            static_cast<std::byte*>(mapping_) + regions_[5U].offset);
    }

    [[nodiscard]] RealtimeWireTickSlotV1* ring_slots()
        const noexcept {
        return reinterpret_cast<RealtimeWireTickSlotV1*>(
            static_cast<std::byte*>(mapping_) + regions_[6U].offset);
    }

    RealtimeSharedServiceConfigV1 config_;
    std::array<LayoutRegion, kRealtimeWireRegionCountV1> regions_{};
    int memfd_ = -1;
    int read_only_fd_ = -1;
    void* mapping_ = MAP_FAILED;
    std::uint64_t mapping_bytes_ = 0U;
    RealtimeWireHeaderV1* header_ = nullptr;
    std::unique_ptr<RingSlotLock[]> ring_locks_;
    std::atomic_flag kline_publication_in_progress_ =
        ATOMIC_FLAG_INIT;

    int listener_fd_ = -1;
    int stop_event_fd_ = -1;
    int prefix_event_fd_ = -1;
    dev_t socket_device_ = 0;
    ino_t socket_inode_ = 0;
    bool socket_bound_ = false;
    std::atomic<bool> prefix_notification_pending_{false};
    std::atomic<bool> control_stop_requested_{false};
    std::thread control_thread_;
};

RealtimeSharedMarketServiceV1::RealtimeSharedMarketServiceV1(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

RealtimeSharedMarketServiceV1::~RealtimeSharedMarketServiceV1() =
    default;

RealtimeSharedServiceCreateErrorV1
RealtimeSharedMarketServiceV1::Create(
    RealtimeSharedServiceConfigV1 config,
    std::shared_ptr<RealtimeSharedMarketServiceV1>* output,
    int* system_error_number) noexcept {
    if (output == nullptr) {
        return RealtimeSharedServiceCreateErrorV1::kNullOutput;
    }
    output->reset();
    try {
        auto impl = std::make_unique<Impl>(std::move(config));
        const RealtimeSharedServiceCreateErrorV1 error =
            impl->Initialize(system_error_number);
        if (error != RealtimeSharedServiceCreateErrorV1::kNone) {
            return error;
        }
        output->reset(
            new RealtimeSharedMarketServiceV1(std::move(impl)));
        return RealtimeSharedServiceCreateErrorV1::kNone;
    } catch (const std::bad_alloc&) {
        return RealtimeSharedServiceCreateErrorV1::
            kResourceExhausted;
    } catch (...) {
        return RealtimeSharedServiceCreateErrorV1::
            kUnexpectedFailure;
    }
}

bool RealtimeSharedMarketServiceV1::Start(
    int* system_error_number) noexcept {
    return impl_ != nullptr &&
           impl_->Start(system_error_number);
}

bool RealtimeSharedMarketServiceV1::PublishApplied(
    std::size_t registry_ordinal,
    const market::RealtimeHistoryRecordV1& record) noexcept {
    return impl_ != nullptr &&
           impl_->PublishApplied(registry_ordinal, record);
}

void RealtimeSharedMarketServiceV1::MarkCoverageLost() noexcept {
    if (impl_ != nullptr) {
        impl_->MarkCoverageLost();
    }
}

bool RealtimeSharedMarketServiceV1::PublishKLineGeneration(
    const market::RealtimeKLineGenerationV1& generation) noexcept {
    return impl_ != nullptr &&
           impl_->PublishKLineGeneration(generation);
}

void RealtimeSharedMarketServiceV1::MarkDraining() noexcept {
    if (impl_ != nullptr) {
        impl_->MarkDraining();
    }
}

bool RealtimeSharedMarketServiceV1::MarkStoppedClean(
    std::uint64_t final_admitted_tick_sequence) noexcept {
    return impl_ != nullptr &&
           impl_->MarkStoppedClean(final_admitted_tick_sequence);
}

void RealtimeSharedMarketServiceV1::MarkFailed() noexcept {
    if (impl_ != nullptr) {
        impl_->MarkFailed();
    }
}

void RealtimeSharedMarketServiceV1::StopControl() noexcept {
    if (impl_ != nullptr) {
        impl_->StopControl();
    }
}

std::uint64_t RealtimeSharedMarketServiceV1::mapping_bytes()
    const noexcept {
    return impl_ == nullptr ? 0U : impl_->mapping_bytes();
}

bool RealtimeSharedMarketServiceV1::failed() const noexcept {
    return impl_ == nullptr || impl_->failed();
}

const std::filesystem::path&
RealtimeSharedMarketServiceV1::control_socket_path() const noexcept {
    return impl_->socket_path();
}

}  // namespace l2flow::ipc
