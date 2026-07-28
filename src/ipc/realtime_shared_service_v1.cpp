#include "l2flow/ipc/realtime_shared_service_v1.h"

#include "l2flow/ipc/realtime_history_wire_v1.h"
#include "l2flow/ipc/realtime_wire_projection_v1.h"
#include "l2flow/ipc/realtime_wire_v1.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cerrno>
#include <cstddef>
#include <cstdio>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <span>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>

#include <fcntl.h>
#include <poll.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <sys/random.h>
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
constexpr std::uint32_t kMaximumHistoryReadersHardLimit = 256U;
constexpr std::uint32_t kMaximumHistoryPageRecordsHardLimit =
    1024U * 1024U;
constexpr std::uint64_t kMinimumHistoryPageBytes = 8192U;

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

bool StateAcceptsHistoryRead(std::uint32_t state) noexcept {
    return state ==
               static_cast<std::uint32_t>(
                   RealtimeServerStateV1::kActive) ||
           state ==
               static_cast<std::uint32_t>(
                   RealtimeServerStateV1::kDraining) ||
           state ==
               static_cast<std::uint32_t>(
                   RealtimeServerStateV1::kStoppedClean);
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

bool MonotonicNowNanoseconds(std::uint64_t* output) noexcept {
    if (output == nullptr) {
        return false;
    }
    timespec now{};
    if (::clock_gettime(CLOCK_MONOTONIC, &now) != 0 ||
        now.tv_sec < 0 || now.tv_nsec < 0) {
        return false;
    }
    constexpr std::uint64_t nanoseconds_per_second =
        1'000'000'000ULL;
    const std::uint64_t seconds =
        static_cast<std::uint64_t>(now.tv_sec);
    const std::uint64_t nanoseconds =
        static_cast<std::uint64_t>(now.tv_nsec);
    if (seconds >
        (std::numeric_limits<std::uint64_t>::max() - nanoseconds) /
            nanoseconds_per_second) {
        return false;
    }
    *output = seconds * nanoseconds_per_second + nanoseconds;
    return true;
}

class HistoryStageTimer final {
public:
    using DurationMember =
        std::uint64_t RealtimeHistoryPageStageTimingV1::*;

    HistoryStageTimer(
        RealtimeHistoryPageStageTimingV1* timing,
        DurationMember duration_member) noexcept
        : timing_(timing),
          duration_(
              timing == nullptr ? nullptr
                                : &(timing->*duration_member)) {
        if (timing_ == nullptr) {
            return;
        }
        if (!MonotonicNowNanoseconds(&start_ns_)) {
            RecordClockFailure();
            return;
        }
        active_ = true;
    }

    HistoryStageTimer(const HistoryStageTimer&) = delete;
    HistoryStageTimer& operator=(const HistoryStageTimer&) = delete;

    ~HistoryStageTimer() {
        if (!active_) {
            return;
        }
        std::uint64_t end_ns = 0U;
        if (!MonotonicNowNanoseconds(&end_ns) ||
            end_ns < start_ns_) {
            RecordClockFailure();
            return;
        }
        const std::uint64_t elapsed_ns = end_ns - start_ns_;
        if (*duration_ >
            std::numeric_limits<std::uint64_t>::max() - elapsed_ns) {
            *duration_ = std::numeric_limits<std::uint64_t>::max();
            RecordClockFailure();
            return;
        }
        *duration_ += elapsed_ns;
    }

private:
    void RecordClockFailure() noexcept {
        if (timing_ != nullptr &&
            timing_->clock_read_failures !=
                std::numeric_limits<std::uint32_t>::max()) {
            ++timing_->clock_read_failures;
        }
    }

    RealtimeHistoryPageStageTimingV1* timing_ = nullptr;
    std::uint64_t* duration_ = nullptr;
    std::uint64_t start_ns_ = 0U;
    bool active_ = false;
};

bool MonotonicDeadline(
    int timeout_ms,
    std::uint64_t* output) noexcept {
    std::uint64_t now = 0U;
    if (timeout_ms <= 0 || output == nullptr ||
        !MonotonicNowNanoseconds(&now)) {
        return false;
    }
    constexpr std::uint64_t nanoseconds_per_millisecond =
        1'000'000ULL;
    const std::uint64_t delta =
        static_cast<std::uint64_t>(timeout_ms) *
        nanoseconds_per_millisecond;
    *output =
        now > std::numeric_limits<std::uint64_t>::max() - delta
            ? std::numeric_limits<std::uint64_t>::max()
            : now + delta;
    return true;
}

int RemainingPollMilliseconds(
    std::uint64_t deadline_ns) noexcept {
    std::uint64_t now = 0U;
    if (!MonotonicNowNanoseconds(&now) || now >= deadline_ns) {
        return 0;
    }
    constexpr std::uint64_t nanoseconds_per_millisecond =
        1'000'000ULL;
    const std::uint64_t remaining_ns = deadline_ns - now;
    const std::uint64_t rounded_up =
        remaining_ns / nanoseconds_per_millisecond +
        (remaining_ns % nanoseconds_per_millisecond != 0U ? 1U : 0U);
    return static_cast<int>(std::min<std::uint64_t>(
        rounded_up,
        static_cast<std::uint64_t>(
            std::numeric_limits<int>::max())));
}

bool SendPacket(
    int socket_fd,
    const void* data,
    std::size_t bytes,
    int attached_fd,
    int timeout_ms) noexcept {
    if (socket_fd < 0 || data == nullptr || bytes == 0U ||
        timeout_ms <= 0) {
        return false;
    }
    std::uint64_t deadline_ns = 0U;
    if (!MonotonicDeadline(timeout_ms, &deadline_ns)) {
        return false;
    }
    iovec vector{};
    vector.iov_base = const_cast<void*>(data);
    vector.iov_len = bytes;
    msghdr message{};
    message.msg_iov = &vector;
    message.msg_iovlen = 1U;
    std::array<std::byte, CMSG_SPACE(sizeof(int))> control{};
    if (attached_fd >= 0) {
        message.msg_control = control.data();
        message.msg_controllen = control.size();
        cmsghdr* const rights = CMSG_FIRSTHDR(&message);
        if (rights == nullptr) {
            return false;
        }
        rights->cmsg_level = SOL_SOCKET;
        rights->cmsg_type = SCM_RIGHTS;
        rights->cmsg_len = CMSG_LEN(sizeof(int));
        std::memcpy(CMSG_DATA(rights), &attached_fd, sizeof(attached_fd));
    }
    for (;;) {
        const ssize_t sent =
            ::sendmsg(socket_fd, &message, MSG_NOSIGNAL);
        if (sent == static_cast<ssize_t>(bytes)) {
            return true;
        }
        if (sent >= 0) {
            return false;
        }
        if (errno == EINTR) {
            if (RemainingPollMilliseconds(deadline_ns) == 0) {
                return false;
            }
            continue;
        }
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            return false;
        }
        pollfd descriptor{};
        descriptor.fd = socket_fd;
        descriptor.events = static_cast<short>(POLLOUT);
        int ready = -1;
        do {
            const int remaining_ms =
                RemainingPollMilliseconds(deadline_ns);
            if (remaining_ms == 0) {
                return false;
            }
            ready = ::poll(
                &descriptor, 1U, remaining_ms);
        } while (ready < 0 && errno == EINTR);
        if (ready <= 0 ||
            (descriptor.revents &
             (POLLERR | POLLHUP | POLLNVAL)) != 0 ||
            (descriptor.revents & POLLOUT) == 0) {
            return false;
        }
    }
}

void CloseDescriptor(int* descriptor) noexcept {
    if (descriptor != nullptr && *descriptor >= 0) {
        static_cast<void>(::close(*descriptor));
        *descriptor = -1;
    }
}

bool GenerateReadToken(
    std::uint64_t forbidden,
    std::uint64_t* output) noexcept {
    if (output == nullptr) {
        return false;
    }
    for (std::size_t attempt = 0U; attempt < 4U; ++attempt) {
        std::uint64_t token = 0U;
        auto* bytes = reinterpret_cast<std::byte*>(&token);
        std::size_t filled = 0U;
        while (filled < sizeof(token)) {
            const ssize_t result = ::getrandom(
                bytes + filled, sizeof(token) - filled, 0U);
            if (result > 0) {
                filled += static_cast<std::size_t>(result);
                continue;
            }
            if (result < 0 && errno == EINTR) {
                continue;
            }
            return false;
        }
        if (token != 0U && token != forbidden) {
            *output = token;
            return true;
        }
    }
    return false;
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
    struct HistoryWorkerSlot final {
        std::mutex socket_mutex;
        int client_fd = -1;
        std::atomic<bool> running{false};
        std::thread thread;
    };

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
            config_.maximum_history_readers == 0U ||
            config_.maximum_history_readers >
                kMaximumHistoryReadersHardLimit ||
            config_.maximum_history_page_records == 0U ||
            config_.maximum_history_page_records >
                kMaximumHistoryPageRecordsHardLimit ||
            config_.maximum_history_page_bytes <
                kMinimumHistoryPageBytes ||
            config_.maximum_history_page_bytes >
                static_cast<std::uint64_t>(
                    std::numeric_limits<std::size_t>::max()) ||
            config_.maximum_history_page_bytes >
                static_cast<std::uint64_t>(
                    std::numeric_limits<off_t>::max()) ||
            config_.history_reader_idle_timeout.count() <= 0 ||
            config_.history_reader_idle_timeout.count() >
                std::numeric_limits<int>::max() ||
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
            history_workers_ = std::make_unique<HistoryWorkerSlot[]>(
                config_.maximum_history_readers);
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

    [[nodiscard]] bool PublishStoreGeneration(
        std::shared_ptr<const market::
                            IntradayInstrumentStoreGenerationV1>
            generation) noexcept {
        if (store_publication_in_progress_.test_and_set(
                std::memory_order_acquire)) {
            MarkCoverageLost();
            return false;
        }
        struct PublicationGuard final {
            std::atomic_flag& flag;
            ~PublicationGuard() {
                flag.clear(std::memory_order_release);
            }
        } publication_guard{store_publication_in_progress_};

        if (generation == nullptr || header_ == nullptr ||
            !StateAcceptsPublication(
                Atomic(header_->server_state)
                    .load(std::memory_order_acquire))) {
            MarkCoverageLost();
            return false;
        }
        const market::RealtimeHistoryWatermarkV1& watermark =
            generation->watermark();
        if (watermark.run_id != config_.run_id ||
            watermark.trade_date != config_.trade_date ||
            watermark.registry_version !=
                config_.registry->registry_version() ||
            watermark.registry_sha256 !=
                config_.registry->registry_sha256() ||
            watermark.generation == 0U ||
            watermark.ingress_sequence_exclusive == 0U ||
            generation->store_session_epoch() == 0U ||
            generation->instrument_count() !=
                config_.registry->size() ||
            generation->record_count() !=
                watermark.ingress_sequence_exclusive - 1U) {
            MarkCoverageLost();
            return false;
        }
        const std::shared_ptr<const market::
                                  IntradayInstrumentStoreGenerationV1>
            previous =
                std::atomic_load_explicit(
                    &store_generation_, std::memory_order_acquire);
        const std::uint64_t previous_generation =
            previous == nullptr ? 0U : previous->watermark().generation;
        if (previous_generation ==
                std::numeric_limits<std::uint64_t>::max() ||
            watermark.generation != previous_generation + 1U) {
            MarkCoverageLost();
            return false;
        }
        if (previous != nullptr) {
            const market::RealtimeHistoryWatermarkV1&
                previous_watermark = previous->watermark();
            if (generation->store_session_epoch() !=
                    previous->store_session_epoch() ||
                generation->coverage_from_open() !=
                    previous->coverage_from_open() ||
                watermark.ingress_sequence_exclusive <
                    previous_watermark.ingress_sequence_exclusive ||
                watermark.recv_monotonic_cut_ns <
                    previous_watermark.recv_monotonic_cut_ns ||
                generation->record_count() <
                    previous->record_count()) {
                MarkCoverageLost();
                return false;
            }
            for (std::size_t source = 0U;
                 source < watermark.sources.size();
                 ++source) {
                if (watermark.sources[source].source_stream_id !=
                        previous_watermark.sources[source]
                            .source_stream_id ||
                    watermark.sources[source].sequence_exclusive <
                        previous_watermark.sources[source]
                            .sequence_exclusive) {
                    MarkCoverageLost();
                    return false;
                }
            }
        }

        std::array<std::uint64_t,
                   market::kIntradayInstrumentStoreSourceCountV1>
            source_totals{};
        std::uint64_t record_total = 0U;
        for (std::size_t ordinal = 0U;
             ordinal < config_.registry->size();
             ++ordinal) {
            market::IntradayInstrumentSummaryV1 summary{};
            if (generation->SummaryAt(ordinal, &summary) !=
                    market::IntradayInstrumentStoreQueryErrorV1::kNone ||
                summary.instrument_id !=
                    instrument_rows()[ordinal].instrument_id) {
                MarkCoverageLost();
                return false;
            }
            market::IntradayInstrumentSummaryV1 previous_summary{};
            if (previous != nullptr &&
                (previous->SummaryAt(ordinal, &previous_summary) !=
                     market::IntradayInstrumentStoreQueryErrorV1::kNone ||
                 previous_summary.instrument_id !=
                     summary.instrument_id ||
                 previous_summary.record_count >
                     summary.record_count)) {
                MarkCoverageLost();
                return false;
            }
            std::uint64_t row_total = 0U;
            for (std::size_t source = 0U;
                 source < source_totals.size();
                 ++source) {
                if ((previous != nullptr &&
                     previous_summary.source_record_counts[source] >
                         summary.source_record_counts[source]) ||
                    !CheckedAdd(
                        row_total,
                        summary.source_record_counts[source],
                        &row_total) ||
                    !CheckedAdd(
                        source_totals[source],
                        summary.source_record_counts[source],
                        &source_totals[source])) {
                    MarkCoverageLost();
                    return false;
                }
            }
            if (row_total != summary.record_count ||
                !CheckedAdd(
                    record_total,
                    summary.record_count,
                    &record_total)) {
                MarkCoverageLost();
                return false;
            }
        }
        if (record_total != generation->record_count()) {
            MarkCoverageLost();
            return false;
        }
        for (std::size_t source = 0U;
             source < source_totals.size();
             ++source) {
            if (watermark.sources[source].source_stream_id == 0U ||
                watermark.sources[source].sequence_exclusive == 0U ||
                source_totals[source] !=
                    watermark.sources[source].sequence_exclusive - 1U) {
                MarkCoverageLost();
                return false;
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
        std::atomic_store_explicit(
            &store_generation_,
            std::move(generation),
            std::memory_order_release);
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
        std::uint32_t expected_state =
            static_cast<std::uint32_t>(
                RealtimeServerStateV1::kDraining);
        if (!Atomic(header_->server_state)
                 .compare_exchange_strong(
                     expected_state,
                     static_cast<std::uint32_t>(
                         RealtimeServerStateV1::kStoppedClean),
                     std::memory_order_release,
                     std::memory_order_acquire)) {
            MarkCoverageLost();
            return false;
        }
        if ((Atomic(header_->flags).load(std::memory_order_acquire) &
             kRealtimeHeaderCoverageLostV1) != 0U ||
            Atomic(header_->server_state)
                    .load(std::memory_order_acquire) !=
                static_cast<std::uint32_t>(
                    RealtimeServerStateV1::kStoppedClean)) {
            MarkCoverageLost();
            return false;
        }
        return true;
    }

    void MarkFailed() noexcept { MarkCoverageLost(); }

    void StopControl() noexcept {
        std::lock_guard<std::mutex> stop_lock(
            control_stop_mutex_);
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
            StopHistoryWorkers();
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
        StopHistoryWorkers();
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
    struct HistoryPageLayout final {
        std::uint64_t descriptor_offset = 0U;
        std::uint64_t snapshot_offset = 0U;
        std::uint64_t tick_offset = 0U;
        std::uint64_t total_bytes = 0U;
    };

    struct BuiltHistoryPage final {
        int read_only_fd = -1;
        std::uint64_t mapping_bytes = 0U;
        std::uint32_t record_count = 0U;
        std::array<std::uint64_t,
                   market::kIntradayInstrumentStoreSourceCountV1>
            source_counts{};
        std::array<std::uint64_t,
                   market::kIntradayInstrumentStoreSourceCountV1>
            last_source_sequences{};
        std::uint64_t first_ingress_sequence = 0U;
        std::uint64_t last_ingress_sequence = 0U;
    };

    enum class HistoryPageBuildError : std::uint8_t {
        kNone = 0U,
        kResourceExhausted,
        kQueryFailure,
        kProjectionFailure,
    };

    [[nodiscard]] bool HistoryHealthy() const noexcept {
        return header_ != nullptr &&
               StateAcceptsHistoryRead(
                   Atomic(header_->server_state)
                       .load(std::memory_order_acquire)) &&
               (Atomic(header_->flags).load(std::memory_order_acquire) &
                kRealtimeHeaderCoverageLostV1) == 0U;
    }

    [[nodiscard]] bool ComputeHistoryPageLayout(
        std::uint64_t record_count,
        std::uint64_t snapshot_count,
        std::uint64_t tick_count,
        HistoryPageLayout* output) const noexcept {
        std::uint64_t combined_count = 0U;
        if (output == nullptr ||
            snapshot_count > record_count ||
            tick_count > record_count ||
            !CheckedAdd(
                snapshot_count, tick_count, &combined_count) ||
            combined_count != record_count) {
            return false;
        }
        std::uint64_t descriptor_bytes = 0U;
        std::uint64_t snapshot_bytes = 0U;
        std::uint64_t tick_bytes = 0U;
        std::uint64_t descriptor_end = 0U;
        std::uint64_t snapshot_end = 0U;
        std::uint64_t tick_end = 0U;
        HistoryPageLayout result{};
        result.descriptor_offset =
            sizeof(RealtimeHistoryPageHeaderV1);
        if (!CheckedMultiply(
                record_count,
                sizeof(RealtimeHistoryRecordDescriptorV1),
                &descriptor_bytes) ||
            !CheckedAdd(
                result.descriptor_offset,
                descriptor_bytes,
                &descriptor_end)) {
            return false;
        }
        result.snapshot_offset = descriptor_end;
        if (!CheckedMultiply(
                snapshot_count,
                sizeof(RealtimeWireSnapshotPayloadV1),
                &snapshot_bytes) ||
            !CheckedAdd(
                result.snapshot_offset,
                snapshot_bytes,
                &snapshot_end)) {
            return false;
        }
        result.tick_offset = snapshot_end;
        if (!CheckedMultiply(
                tick_count,
                sizeof(RealtimeWireTickPayloadV1),
                &tick_bytes) ||
            !CheckedAdd(result.tick_offset, tick_bytes, &tick_end)) {
            return false;
        }
        result.total_bytes = tick_end;
        if (
            result.total_bytes >
                config_.maximum_history_page_bytes ||
            result.total_bytes >
                static_cast<std::uint64_t>(
                    std::numeric_limits<std::size_t>::max()) ||
            result.total_bytes >
                static_cast<std::uint64_t>(
                    std::numeric_limits<off_t>::max())) {
            return false;
        }
        *output = result;
        return true;
    }

    [[nodiscard]] bool FillHistoryGenerationInfo(
        const market::IntradayInstrumentStoreGenerationV1& generation,
        std::size_t registry_ordinal,
        const market::IntradayInstrumentSummaryV1& summary,
        RealtimeHistoryGenerationInfoV1* output) const noexcept {
        if (output == nullptr ||
            registry_ordinal >= config_.registry->size() ||
            registry_ordinal >
                static_cast<std::size_t>(
                    std::numeric_limits<std::uint32_t>::max()) ||
            summary.instrument_id == 0U ||
            instrument_rows()[registry_ordinal].instrument_id !=
                summary.instrument_id) {
            return false;
        }
        std::uint64_t summary_total = 0U;
        for (const std::uint64_t count :
             summary.source_record_counts) {
            if (!CheckedAdd(summary_total, count, &summary_total)) {
                return false;
            }
        }
        if (summary_total != summary.record_count) {
            return false;
        }
        const market::RealtimeHistoryWatermarkV1& watermark =
            generation.watermark();
        RealtimeHistoryGenerationInfoV1 result{};
        for (std::size_t index = 0U;
             index < watermark.run_id.size();
             ++index) {
            result.run_id[index] =
                std::to_integer<std::uint8_t>(
                    watermark.run_id[index]);
        }
        result.session_epoch = config_.session_epoch;
        result.generation = watermark.generation;
        result.trade_date = watermark.trade_date;
        result.instrument_id = summary.instrument_id;
        result.registry_ordinal =
            static_cast<std::uint32_t>(registry_ordinal);
        result.instrument_count =
            static_cast<std::uint32_t>(
                generation.instrument_count());
        result.ingress_sequence_exclusive =
            watermark.ingress_sequence_exclusive;
        result.recv_monotonic_cut_ns =
            watermark.recv_monotonic_cut_ns;
        result.registry_version = watermark.registry_version;
        for (std::size_t index = 0U;
             index < result.registry_sha256.size();
             ++index) {
            result.registry_sha256[index] =
                std::to_integer<std::uint8_t>(
                    watermark.registry_sha256[index]);
            result.input_identity_sha256[index] =
                std::to_integer<std::uint8_t>(
                    watermark.input_identity_sha256[index]);
        }
        for (std::size_t source = 0U;
             source < summary.source_record_counts.size();
             ++source) {
            result.source_stream_ids[source] =
                watermark.sources[source].source_stream_id;
            result.source_sequence_exclusive[source] =
                watermark.sources[source].sequence_exclusive;
            result.instrument_source_record_counts[source] =
                summary.source_record_counts[source];
        }
        result.instrument_record_count = summary.record_count;
        result.flags =
            kRealtimeHistoryRecordCoverageCompleteV1 |
            (generation.coverage_from_open()
                 ? kRealtimeHistoryCoverageFromOpenV1
                 : 0U);
        result.payload_projection =
            static_cast<std::uint32_t>(
                RealtimeHistoryPayloadProjectionV1::kCoreV1);
        *output = result;
        return true;
    }

    [[nodiscard]] std::size_t HistoryPageRecordCapacity(
        std::uint32_t requested) const noexcept {
        const std::uint64_t upper = std::min<std::uint64_t>(
            requested, config_.maximum_history_page_records);
        std::uint64_t low = 0U;
        std::uint64_t high = upper + 1U;
        while (low + 1U < high) {
            const std::uint64_t middle =
                low + (high - low) / 2U;
            HistoryPageLayout ignored{};
            if (ComputeHistoryPageLayout(
                    middle, middle, 0U, &ignored)) {
                low = middle;
            } else {
                high = middle;
            }
        }
        return static_cast<std::size_t>(low);
    }

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

    void StopHistoryWorkers() noexcept {
        if (history_workers_ == nullptr) {
            return;
        }
        for (std::uint32_t index = 0U;
             index < config_.maximum_history_readers;
             ++index) {
            HistoryWorkerSlot& slot = history_workers_[index];
            std::lock_guard<std::mutex> lock(slot.socket_mutex);
            if (slot.client_fd >= 0) {
                static_cast<void>(
                    ::shutdown(slot.client_fd, SHUT_RDWR));
            }
        }
        for (std::uint32_t index = 0U;
             index < config_.maximum_history_readers;
             ++index) {
            if (history_workers_[index].thread.joinable()) {
                history_workers_[index].thread.join();
            }
        }
    }

    [[nodiscard]] HistoryPageBuildError BuildHistoryPage(
        market::IntradayInstrumentCursorV1& cursor,
        std::size_t registry_ordinal,
        const RealtimeHistoryGenerationInfoV1& generation_info,
        std::uint64_t page_index,
        std::size_t* page_capacity,
        std::uint64_t previous_ingress_sequence,
        const std::array<
            std::uint64_t,
            market::kIntradayInstrumentStoreSourceCountV1>&
            previous_source_sequences,
        BuiltHistoryPage* output,
        RealtimeHistoryPageStageTimingV1* timing) noexcept {
        if (page_capacity == nullptr || *page_capacity == 0U ||
            output == nullptr || output->read_only_fd >= 0) {
            return HistoryPageBuildError::kQueryFailure;
        }
        *output = BuiltHistoryPage{};
        std::vector<const market::RealtimeHistoryRecordV1*> records;
        try {
            records.resize(*page_capacity);
        } catch (...) {
            return HistoryPageBuildError::kResourceExhausted;
        }

        std::size_t written = 0U;
        market::IntradayInstrumentStoreQueryErrorV1 query_error =
            market::IntradayInstrumentStoreQueryErrorV1::
                kBatchLimitExceeded;
        while (query_error ==
               market::IntradayInstrumentStoreQueryErrorV1::
                   kBatchLimitExceeded) {
            {
                HistoryStageTimer cursor_read_timer(
                    timing,
                    &RealtimeHistoryPageStageTimingV1::
                        cursor_read_ns);
                query_error = cursor.ReadBatch(
                    std::span<
                        const market::RealtimeHistoryRecordV1*>(
                        records.data(), *page_capacity),
                    &written);
            }
            if (query_error !=
                    market::IntradayInstrumentStoreQueryErrorV1::
                        kBatchLimitExceeded) {
                break;
            }
            if (*page_capacity == 1U) {
                return HistoryPageBuildError::kQueryFailure;
            }
            *page_capacity = std::max<std::size_t>(
                1U, *page_capacity / 2U);
            records.resize(*page_capacity);
        }
        if (query_error ==
            market::IntradayInstrumentStoreQueryErrorV1::
                kResourceExhausted) {
            return HistoryPageBuildError::kResourceExhausted;
        }
        if (query_error !=
            market::IntradayInstrumentStoreQueryErrorV1::kNone ||
            written > *page_capacity) {
            return HistoryPageBuildError::kQueryFailure;
        }
        if (written == 0U) {
            return cursor.done()
                       ? HistoryPageBuildError::kNone
                       : HistoryPageBuildError::kQueryFailure;
        }

        std::uint64_t snapshot_count = 0U;
        std::uint64_t tick_count = 0U;
        HistoryPageLayout layout{};
        {
            HistoryStageTimer classify_layout_timer(
                timing,
                &RealtimeHistoryPageStageTimingV1::
                    classify_layout_ns);
            for (std::size_t index = 0U; index < written; ++index) {
                if (records[index] == nullptr) {
                    MarkCoverageLost();
                    return HistoryPageBuildError::kProjectionFailure;
                }
                if (market::IsSnapshotEventKindV1(
                        records[index]->kind())) {
                    ++snapshot_count;
                } else if (market::IsTickEventKindV1(
                               records[index]->kind())) {
                    ++tick_count;
                } else {
                    MarkCoverageLost();
                    return HistoryPageBuildError::kProjectionFailure;
                }
            }
            if (!ComputeHistoryPageLayout(
                    written,
                    snapshot_count,
                    tick_count,
                    &layout)) {
                return HistoryPageBuildError::kResourceExhausted;
            }
        }
        if (timing != nullptr) {
            timing->record_count =
                static_cast<std::uint32_t>(written);
            timing->snapshot_count =
                static_cast<std::uint32_t>(snapshot_count);
            timing->tick_count =
                static_cast<std::uint32_t>(tick_count);
            timing->page_mapping_bytes = layout.total_bytes;
        }

        struct PageResources final {
            int writable_fd = -1;
            void* mapping = MAP_FAILED;
            std::size_t mapping_bytes = 0U;
            ~PageResources() {
                if (mapping != MAP_FAILED) {
                    static_cast<void>(
                        ::munmap(mapping, mapping_bytes));
                }
                CloseDescriptor(&writable_fd);
            }
        } resources;
        {
            HistoryStageTimer memfd_prepare_timer(
                timing,
                &RealtimeHistoryPageStageTimingV1::
                    memfd_prepare_ns);
            resources.mapping_bytes =
                static_cast<std::size_t>(layout.total_bytes);
            resources.writable_fd = ::memfd_create(
                "l2flow-history-page-v1",
                MFD_CLOEXEC | MFD_ALLOW_SEALING);
            if (resources.writable_fd < 0 ||
                ::ftruncate(
                    resources.writable_fd,
                    static_cast<off_t>(layout.total_bytes)) != 0) {
                return HistoryPageBuildError::kResourceExhausted;
            }
            resources.mapping = ::mmap(
                nullptr,
                resources.mapping_bytes,
                PROT_READ | PROT_WRITE,
                MAP_SHARED,
                resources.writable_fd,
                0);
            if (resources.mapping == MAP_FAILED) {
                return HistoryPageBuildError::kResourceExhausted;
            }
            std::memset(
                resources.mapping, 0, resources.mapping_bytes);
        }

        {
            HistoryStageTimer projection_timer(
                timing,
                &RealtimeHistoryPageStageTimingV1::projection_ns);
            auto* const page = static_cast<RealtimeHistoryPageHeaderV1*>(
                resources.mapping);
            page->magic = kRealtimeHistoryMagicV1;
            page->abi_major = kRealtimeWireMajorV1;
            page->abi_minor = kRealtimeWireMinorV1;
            page->header_bytes =
                static_cast<std::uint32_t>(
                    sizeof(RealtimeHistoryPageHeaderV1));
            page->endian_marker = kRealtimeLittleEndianMarkerV1;
            page->total_mapping_bytes = layout.total_bytes;
            page->page_index = page_index;
            page->record_count =
                static_cast<std::uint32_t>(written);
            page->record_descriptor_bytes =
                static_cast<std::uint32_t>(
                    sizeof(RealtimeHistoryRecordDescriptorV1));
            page->record_descriptors_offset =
                layout.descriptor_offset;
            page->snapshot_payloads_offset =
                layout.snapshot_offset;
            page->snapshot_count =
                static_cast<std::uint32_t>(snapshot_count);
            page->snapshot_payload_bytes =
                static_cast<std::uint32_t>(
                    sizeof(RealtimeWireSnapshotPayloadV1));
            page->tick_payloads_offset = layout.tick_offset;
            page->tick_count =
                static_cast<std::uint32_t>(tick_count);
            page->tick_payload_bytes =
                static_cast<std::uint32_t>(
                    sizeof(RealtimeWireTickPayloadV1));
            page->generation = generation_info;

            auto* const descriptors =
                reinterpret_cast<RealtimeHistoryRecordDescriptorV1*>(
                    static_cast<std::byte*>(resources.mapping) +
                    layout.descriptor_offset);
            auto* const snapshots =
                reinterpret_cast<RealtimeWireSnapshotPayloadV1*>(
                    static_cast<std::byte*>(resources.mapping) +
                    layout.snapshot_offset);
            auto* const ticks =
                reinterpret_cast<RealtimeWireTickPayloadV1*>(
                    static_cast<std::byte*>(resources.mapping) +
                    layout.tick_offset);
            std::uint32_t snapshot_index = 0U;
            std::uint32_t tick_index = 0U;
            std::uint64_t prior_ingress =
                previous_ingress_sequence;
            auto prior_source_sequences =
                previous_source_sequences;
            for (std::size_t index = 0U; index < written; ++index) {
                const market::RealtimeHistoryRecordV1& record =
                    *records[index];
                if (record.instrument_id() !=
                        generation_info.instrument_id ||
                    record.source_slot() >=
                        generation_info.source_stream_ids.size() ||
                    record.source_stream_id() !=
                        generation_info.source_stream_ids[
                            record.source_slot()] ||
                    record.source_sequence() == 0U ||
                    record.source_sequence() >=
                        generation_info.source_sequence_exclusive[
                            record.source_slot()] ||
                    record.source_sequence() <=
                        prior_source_sequences[record.source_slot()] ||
                    record.ingress_sequence() <= prior_ingress ||
                    record.ingress_sequence() >=
                        generation_info.ingress_sequence_exclusive) {
                    MarkCoverageLost();
                    return HistoryPageBuildError::kProjectionFailure;
                }
                RealtimeHistoryRecordDescriptorV1& descriptor =
                    descriptors[index];
                descriptor.ingress_sequence =
                    record.ingress_sequence();
                descriptor.source_sequence =
                    record.source_sequence();
                descriptor.tick_stream_sequence =
                    record.tick_stream_sequence();
                descriptor.event_kind =
                    static_cast<std::uint8_t>(record.kind());
                descriptor.source_slot = record.source_slot();
                const std::size_t source = record.source_slot();
                if (output->source_counts[source] ==
                    std::numeric_limits<std::uint64_t>::max()) {
                    MarkCoverageLost();
                    return HistoryPageBuildError::kProjectionFailure;
                }
                ++output->source_counts[source];

                const RealtimeWireCommonRecordV1* common = nullptr;
                if (market::IsSnapshotEventKindV1(record.kind())) {
                    descriptor.payload_kind =
                        static_cast<std::uint8_t>(
                            RealtimeHistoryPayloadKindV1::kSnapshot);
                    descriptor.payload_index = snapshot_index;
                    if (!ProjectSnapshotWireV1(
                            record,
                            registry_ordinal,
                            &snapshots[snapshot_index])) {
                        MarkCoverageLost();
                        return HistoryPageBuildError::kProjectionFailure;
                    }
                    common = &snapshots[snapshot_index].common;
                    ++snapshot_index;
                } else {
                    descriptor.payload_kind =
                        static_cast<std::uint8_t>(
                            RealtimeHistoryPayloadKindV1::kTick);
                    descriptor.payload_index = tick_index;
                    std::uint32_t projection_flags = 0U;
                    if (!ProjectHistoryTickWireV1(
                            record,
                            registry_ordinal,
                            &ticks[tick_index],
                            &projection_flags) ||
                        ticks[tick_index].projection_flags !=
                            projection_flags) {
                        MarkCoverageLost();
                        return HistoryPageBuildError::kProjectionFailure;
                    }
                    descriptor.projection_flags = projection_flags;
                    common = &ticks[tick_index].common;
                    ++tick_index;
                }
                if (common == nullptr ||
                    common->instrument_id != record.instrument_id() ||
                    common->registry_ordinal != registry_ordinal ||
                    common->source_sequence != record.source_sequence() ||
                    common->ingress_sequence !=
                        record.ingress_sequence() ||
                    common->tick_stream_sequence !=
                        record.tick_stream_sequence() ||
                    common->source_slot != record.source_slot() ||
                    common->event_kind != descriptor.event_kind ||
                    common->trade_date != generation_info.trade_date) {
                    MarkCoverageLost();
                    return HistoryPageBuildError::kProjectionFailure;
                }
                prior_source_sequences[source] =
                    record.source_sequence();
                prior_ingress = record.ingress_sequence();
            }
            if (snapshot_index != snapshot_count ||
                tick_index != tick_count) {
                MarkCoverageLost();
                return HistoryPageBuildError::kProjectionFailure;
            }
            page->first_ingress_sequence =
                descriptors[0U].ingress_sequence;
            page->last_ingress_sequence =
                descriptors[written - 1U].ingress_sequence;
            output->first_ingress_sequence =
                page->first_ingress_sequence;
            output->last_ingress_sequence =
                page->last_ingress_sequence;
            output->last_source_sequences =
                prior_source_sequences;
        }

        int read_only_fd = -1;
        {
            HistoryStageTimer memfd_finalize_timer(
                timing,
                &RealtimeHistoryPageStageTimingV1::
                    memfd_finalize_ns);
            if (::munmap(
                    resources.mapping, resources.mapping_bytes) != 0) {
                return HistoryPageBuildError::kResourceExhausted;
            }
            resources.mapping = MAP_FAILED;
            const int seals =
                F_SEAL_WRITE | F_SEAL_GROW | F_SEAL_SHRINK |
                F_SEAL_SEAL;
            if (::fcntl(
                    resources.writable_fd, F_ADD_SEALS, seals) != 0 ||
                ::fcntl(resources.writable_fd, F_GET_SEALS) != seals) {
                return HistoryPageBuildError::kResourceExhausted;
            }
            std::array<char, 64U> proc_path{};
            const int path_bytes = std::snprintf(
                proc_path.data(),
                proc_path.size(),
                "/proc/self/fd/%d",
                resources.writable_fd);
            if (path_bytes <= 0 ||
                static_cast<std::size_t>(path_bytes) >=
                    proc_path.size()) {
                return HistoryPageBuildError::kResourceExhausted;
            }
            read_only_fd =
                ::open(proc_path.data(), O_RDONLY | O_CLOEXEC);
            if (read_only_fd < 0) {
                return HistoryPageBuildError::kResourceExhausted;
            }
        }
        output->read_only_fd = read_only_fd;
        output->mapping_bytes = layout.total_bytes;
        output->record_count =
            static_cast<std::uint32_t>(written);
        return HistoryPageBuildError::kNone;
    }

    [[nodiscard]] bool SendHistoryOpenResponse(
        int client,
        std::uint64_t request_id,
        RealtimeHistoryControlStatusV1 status,
        const RealtimeHistoryGenerationInfoV1* generation,
        std::uint64_t initial_read_token = 0U) const
        noexcept {
        RealtimeHistoryOpenResponseV1 response{};
        response.magic = kRealtimeControlMagicV1;
        response.protocol_major = kRealtimeWireMajorV1;
        response.protocol_minor = kRealtimeWireMinorV1;
        response.status = static_cast<std::uint16_t>(status);
        response.message_bytes =
            static_cast<std::uint32_t>(sizeof(response));
        response.request_id = request_id;
        if (status == RealtimeHistoryControlStatusV1::kOk) {
            if (generation == nullptr || initial_read_token == 0U) {
                return false;
            }
            response.initial_read_token = initial_read_token;
            response.generation = *generation;
        } else if (
            generation != nullptr || initial_read_token != 0U) {
            return false;
        }
        return SendPacket(
            client,
            &response,
            sizeof(response),
            -1,
            static_cast<int>(
                config_.history_reader_idle_timeout.count()));
    }

    [[nodiscard]] bool SendHistoryReadResponse(
        int client,
        std::uint64_t request_id,
        RealtimeHistoryControlStatusV1 status,
        std::uint16_t response_flags,
        std::uint32_t record_count,
        std::uint64_t page_mapping_bytes,
        std::uint64_t page_index,
        std::uint64_t generation,
        int page_fd,
        std::uint64_t next_read_token = 0U) const noexcept {
        const bool success =
            status == RealtimeHistoryControlStatusV1::kOk;
        const bool terminal =
            (response_flags &
             kRealtimeHistoryResponseTerminalV1) != 0U;
        if ((!success &&
             (response_flags != 0U || record_count != 0U ||
              page_mapping_bytes != 0U || page_index != 0U ||
              generation != 0U || page_fd >= 0 ||
              next_read_token != 0U)) ||
            (success && terminal &&
             (response_flags !=
                  kRealtimeHistoryResponseTerminalV1 ||
              record_count != 0U || page_mapping_bytes != 0U ||
              page_fd >= 0 || next_read_token != 0U)) ||
            (success && !terminal &&
             (response_flags != 0U || record_count == 0U ||
              page_mapping_bytes <
                  sizeof(RealtimeHistoryPageHeaderV1) ||
              page_fd < 0 || next_read_token == 0U))) {
            return false;
        }
        RealtimeHistoryReadResponseV1 response{};
        response.magic = kRealtimeControlMagicV1;
        response.protocol_major = kRealtimeWireMajorV1;
        response.protocol_minor = kRealtimeWireMinorV1;
        response.status = static_cast<std::uint16_t>(status);
        response.flags = response_flags;
        response.message_bytes =
            static_cast<std::uint32_t>(sizeof(response));
        response.record_count = record_count;
        response.request_id = request_id;
        response.page_mapping_bytes = page_mapping_bytes;
        response.page_index = page_index;
        response.generation = generation;
        response.next_read_token = next_read_token;
        return SendPacket(
            client,
            &response,
            sizeof(response),
            page_fd,
            static_cast<int>(
                config_.history_reader_idle_timeout.count()));
    }

    [[nodiscard]] bool ReceiveHistoryReadRequest(
        int client,
        RealtimeHistoryReadRequestV1* output) const noexcept {
        if (output == nullptr) {
            return false;
        }
        *output = RealtimeHistoryReadRequestV1{};
        pollfd descriptor{};
        descriptor.fd = client;
        descriptor.events = static_cast<short>(POLLIN);
        std::uint64_t deadline_ns = 0U;
        if (!MonotonicDeadline(
                static_cast<int>(
                    config_.history_reader_idle_timeout.count()),
                &deadline_ns)) {
            return false;
        }
        int ready = -1;
        do {
            const int remaining_ms =
                RemainingPollMilliseconds(deadline_ns);
            if (remaining_ms == 0) {
                return false;
            }
            ready = ::poll(
                &descriptor,
                1U,
                remaining_ms);
        } while (ready < 0 && errno == EINTR);
        if (ready <= 0 ||
            (descriptor.revents &
             (POLLERR | POLLHUP | POLLNVAL)) != 0 ||
            (descriptor.revents & POLLIN) == 0) {
            return false;
        }
        ssize_t received = -1;
        do {
            received =
                ::recv(client, output, sizeof(*output), MSG_TRUNC);
        } while (
            received < 0 && errno == EINTR &&
            RemainingPollMilliseconds(deadline_ns) != 0);
        return received ==
               static_cast<ssize_t>(sizeof(*output));
    }

    void HistoryClientLoop(
        int client,
        RealtimeHistoryOpenRequestV1 request) noexcept {
        const auto send_open_error =
            [&](RealtimeHistoryControlStatusV1 status) noexcept {
                static_cast<void>(SendHistoryOpenResponse(
                    client,
                    request.request_id,
                    status,
                    nullptr));
            };
        if (request.magic != kRealtimeControlMagicV1 ||
            request.message_bytes != sizeof(request) ||
            request.opcode != static_cast<std::uint16_t>(
                                  RealtimeHistoryControlOpcodeV1::
                                      kOpenHistory) ||
            request.flags != 0U || request.reserved0 != 0U ||
            request.reserved1 != 0U ||
            std::any_of(
                request.reserved2.begin(),
                request.reserved2.end(),
                [](std::uint64_t value) noexcept {
                    return value != 0U;
                }) ||
            request.request_id == 0U ||
            request.instrument_id == 0U ||
            request.requested_page_records == 0U) {
            send_open_error(
                RealtimeHistoryControlStatusV1::kInvalidRequest);
            return;
        }
        if (request.protocol_major != kRealtimeWireMajorV1 ||
            request.protocol_minor != kRealtimeWireMinorV1) {
            send_open_error(
                RealtimeHistoryControlStatusV1::
                    kUnsupportedVersion);
            return;
        }
        if (!HistoryHealthy()) {
            send_open_error(
                RealtimeHistoryControlStatusV1::kUnavailable);
            return;
        }
        const std::shared_ptr<const market::
                                  IntradayInstrumentStoreGenerationV1>
            generation = std::atomic_load_explicit(
                &store_generation_, std::memory_order_acquire);
        if (generation == nullptr) {
            send_open_error(
                RealtimeHistoryControlStatusV1::kUnavailable);
            return;
        }
        const market::InstrumentRegistryLookupResultV1 lookup =
            config_.registry->LookupById(request.instrument_id);
        if (!lookup.known()) {
            send_open_error(
                RealtimeHistoryControlStatusV1::kNotFound);
            return;
        }
        market::IntradayInstrumentSummaryV1 summary{};
        const market::IntradayInstrumentStoreQueryErrorV1 find_error =
            generation->Find(request.instrument_id, &summary);
        if (find_error ==
            market::IntradayInstrumentStoreQueryErrorV1::kNotFound) {
            MarkCoverageLost();
            send_open_error(
                RealtimeHistoryControlStatusV1::kInternalFailure);
            return;
        }
        if (find_error !=
            market::IntradayInstrumentStoreQueryErrorV1::kNone) {
            if (find_error !=
                market::IntradayInstrumentStoreQueryErrorV1::
                    kResourceExhausted) {
                MarkCoverageLost();
            }
            send_open_error(
                find_error ==
                        market::IntradayInstrumentStoreQueryErrorV1::
                            kResourceExhausted
                    ? RealtimeHistoryControlStatusV1::
                          kResourceExhausted
                    : RealtimeHistoryControlStatusV1::
                          kInternalFailure);
            return;
        }
        RealtimeHistoryGenerationInfoV1 generation_info{};
        if (!FillHistoryGenerationInfo(
                *generation,
                lookup.registry_ordinal,
                summary,
                &generation_info)) {
            MarkCoverageLost();
            send_open_error(
                RealtimeHistoryControlStatusV1::kInternalFailure);
            return;
        }
        std::size_t page_capacity =
            HistoryPageRecordCapacity(
                request.requested_page_records);
        if (page_capacity == 0U) {
            send_open_error(
                RealtimeHistoryControlStatusV1::
                    kResourceExhausted);
            return;
        }
        market::IntradayInstrumentScanOptionsV1 options{};
        options.ingress_sequence_begin_inclusive = 1U;
        options.ingress_sequence_end_exclusive =
            std::numeric_limits<std::uint64_t>::max();
        options.maximum_records =
            std::numeric_limits<std::uint64_t>::max();
        options.direction =
            market::IntradayInstrumentScanDirectionV1::
                kOldestFirst;
        std::unique_ptr<market::IntradayInstrumentCursorV1> cursor;
        const market::IntradayInstrumentStoreQueryErrorV1 open_error =
            generation->OpenInstrumentCursor(
                request.instrument_id, options, &cursor);
        if (open_error !=
                market::IntradayInstrumentStoreQueryErrorV1::kNone ||
            cursor == nullptr) {
            if ((open_error ==
                     market::IntradayInstrumentStoreQueryErrorV1::kNone &&
                 cursor == nullptr) ||
                (open_error !=
                     market::IntradayInstrumentStoreQueryErrorV1::kNone &&
                 open_error !=
                     market::IntradayInstrumentStoreQueryErrorV1::
                         kResourceExhausted)) {
                MarkCoverageLost();
            }
            send_open_error(
                open_error ==
                        market::IntradayInstrumentStoreQueryErrorV1::
                            kResourceExhausted
                    ? RealtimeHistoryControlStatusV1::
                          kResourceExhausted
                    : RealtimeHistoryControlStatusV1::
                          kInternalFailure);
            return;
        }
        if (!HistoryHealthy()) {
            send_open_error(
                RealtimeHistoryControlStatusV1::kUnavailable);
            return;
        }
        std::uint64_t read_token = 0U;
        if (!GenerateReadToken(0U, &read_token)) {
            send_open_error(
                RealtimeHistoryControlStatusV1::
                    kResourceExhausted);
            return;
        }
        if (!SendHistoryOpenResponse(
                client,
                request.request_id,
                RealtimeHistoryControlStatusV1::kOk,
                &generation_info,
                read_token)) {
            return;
        }

        std::uint64_t page_index = 0U;
        std::uint64_t emitted_records = 0U;
        std::array<std::uint64_t,
                   market::kIntradayInstrumentStoreSourceCountV1>
            emitted_source_counts{};
        std::array<std::uint64_t,
                   market::kIntradayInstrumentStoreSourceCountV1>
            last_source_sequences{};
        std::uint64_t last_ingress_sequence = 0U;
        for (;;) {
            if (control_stop_requested_.load(
                    std::memory_order_acquire)) {
                return;
            }
            RealtimeHistoryReadRequestV1 read_request{};
            if (!ReceiveHistoryReadRequest(
                    client, &read_request)) {
                return;
            }
            const auto send_read_error =
                [&](RealtimeHistoryControlStatusV1 status) noexcept {
                    static_cast<void>(SendHistoryReadResponse(
                        client,
                        read_request.request_id,
                        status,
                        0U,
                        0U,
                        0U,
                        0U,
                        0U,
                        -1));
                };
            if (read_request.magic !=
                    kRealtimeControlMagicV1 ||
                read_request.message_bytes !=
                    sizeof(read_request) ||
                read_request.opcode !=
                    static_cast<std::uint16_t>(
                        RealtimeHistoryControlOpcodeV1::
                            kReadHistory) ||
                read_request.flags != 0U ||
                read_request.reserved0 != 0U ||
                read_request.request_id == 0U ||
                read_request.expected_page_index != page_index ||
                read_request.read_token != read_token) {
                send_read_error(
                    RealtimeHistoryControlStatusV1::
                        kInvalidRequest);
                return;
            }
            if (read_request.protocol_major !=
                    kRealtimeWireMajorV1 ||
                read_request.protocol_minor !=
                    kRealtimeWireMinorV1) {
                send_read_error(
                    RealtimeHistoryControlStatusV1::
                        kUnsupportedVersion);
                return;
            }
            if (!HistoryHealthy()) {
                send_read_error(
                    RealtimeHistoryControlStatusV1::
                        kUnavailable);
                return;
            }
            if (cursor->done()) {
                if (emitted_records != summary.record_count ||
                    emitted_source_counts !=
                        summary.source_record_counts) {
                    MarkCoverageLost();
                    send_read_error(
                        RealtimeHistoryControlStatusV1::
                            kInternalFailure);
                    return;
                }
                if (!HistoryHealthy()) {
                    send_read_error(
                        RealtimeHistoryControlStatusV1::
                            kUnavailable);
                    return;
                }
                static_cast<void>(SendHistoryReadResponse(
                    client,
                    read_request.request_id,
                    RealtimeHistoryControlStatusV1::kOk,
                    kRealtimeHistoryResponseTerminalV1,
                    0U,
                    0U,
                    page_index,
                    generation_info.generation,
                    -1));
                return;
            }

            BuiltHistoryPage page{};
            RealtimeHistoryPageStageTimingV1 page_timing{};
            RealtimeHistoryPageStageTimingV1* const timing =
                config_.history_stage_observer == nullptr
                    ? nullptr
                    : &page_timing;
            if (timing != nullptr) {
                timing->open_request_id = request.request_id;
                timing->read_request_id = read_request.request_id;
                timing->generation = generation_info.generation;
                timing->instrument_id =
                    generation_info.instrument_id;
                timing->page_index = page_index;
            }
            HistoryPageBuildError build_error =
                HistoryPageBuildError::kQueryFailure;
            {
                HistoryStageTimer build_total_timer(
                    timing,
                    &RealtimeHistoryPageStageTimingV1::
                        build_total_ns);
                build_error = BuildHistoryPage(
                    *cursor,
                    lookup.registry_ordinal,
                    generation_info,
                    page_index,
                    &page_capacity,
                    last_ingress_sequence,
                    last_source_sequences,
                    &page,
                    timing);
            }
            if (build_error != HistoryPageBuildError::kNone) {
                if (build_error ==
                    HistoryPageBuildError::
                        kResourceExhausted) {
                    send_read_error(
                        RealtimeHistoryControlStatusV1::
                            kResourceExhausted);
                } else {
                    MarkCoverageLost();
                    send_read_error(
                        RealtimeHistoryControlStatusV1::
                            kInternalFailure);
                }
                CloseDescriptor(&page.read_only_fd);
                return;
            }
            if (page.record_count == 0U) {
                if (!cursor->done() ||
                    emitted_records != summary.record_count ||
                    emitted_source_counts !=
                        summary.source_record_counts) {
                    MarkCoverageLost();
                    send_read_error(
                        RealtimeHistoryControlStatusV1::
                            kInternalFailure);
                    return;
                }
                if (!HistoryHealthy()) {
                    send_read_error(
                        RealtimeHistoryControlStatusV1::
                            kUnavailable);
                    return;
                }
                static_cast<void>(SendHistoryReadResponse(
                    client,
                    read_request.request_id,
                    RealtimeHistoryControlStatusV1::kOk,
                    kRealtimeHistoryResponseTerminalV1,
                    0U,
                    0U,
                    page_index,
                    generation_info.generation,
                    -1));
                return;
            }
            if (!HistoryHealthy()) {
                CloseDescriptor(&page.read_only_fd);
                send_read_error(
                    RealtimeHistoryControlStatusV1::
                        kUnavailable);
                return;
            }
            std::uint64_t next_emitted = 0U;
            if (!CheckedAdd(
                    emitted_records,
                    page.record_count,
                    &next_emitted) ||
                next_emitted > summary.record_count) {
                MarkCoverageLost();
                CloseDescriptor(&page.read_only_fd);
                send_read_error(
                    RealtimeHistoryControlStatusV1::
                        kInternalFailure);
                return;
            }
            auto next_source_counts = emitted_source_counts;
            for (std::size_t source = 0U;
                 source < next_source_counts.size();
                 ++source) {
                if (!CheckedAdd(
                        next_source_counts[source],
                        page.source_counts[source],
                        &next_source_counts[source]) ||
                    next_source_counts[source] >
                        summary.source_record_counts[source]) {
                    MarkCoverageLost();
                    CloseDescriptor(&page.read_only_fd);
                    send_read_error(
                        RealtimeHistoryControlStatusV1::
                            kInternalFailure);
                    return;
                }
            }
            std::uint64_t next_read_token = 0U;
            bool token_generated = false;
            {
                HistoryStageTimer token_timer(
                    timing,
                    &RealtimeHistoryPageStageTimingV1::token_ns);
                token_generated = GenerateReadToken(
                    read_token, &next_read_token);
            }
            if (!token_generated) {
                CloseDescriptor(&page.read_only_fd);
                send_read_error(
                    RealtimeHistoryControlStatusV1::
                        kResourceExhausted);
                return;
            }
            if (!HistoryHealthy()) {
                CloseDescriptor(&page.read_only_fd);
                send_read_error(
                    RealtimeHistoryControlStatusV1::
                        kUnavailable);
                return;
            }
            bool sent = false;
            {
                HistoryStageTimer send_timer(
                    timing,
                    &RealtimeHistoryPageStageTimingV1::send_ns);
                sent = SendHistoryReadResponse(
                    client,
                    read_request.request_id,
                    RealtimeHistoryControlStatusV1::kOk,
                    0U,
                    page.record_count,
                    page.mapping_bytes,
                    page_index,
                    generation_info.generation,
                    page.read_only_fd,
                    next_read_token);
            }
            CloseDescriptor(&page.read_only_fd);
            if (!sent) {
                return;
            }
            emitted_records = next_emitted;
            emitted_source_counts = next_source_counts;
            last_ingress_sequence =
                page.last_ingress_sequence;
            last_source_sequences =
                page.last_source_sequences;
            read_token = next_read_token;
            if (timing != nullptr) {
                config_.history_stage_observer
                    ->ObserveHistoryPageStageTiming(page_timing);
            }
            if (page_index ==
                std::numeric_limits<std::uint64_t>::max()) {
                MarkCoverageLost();
                return;
            }
            ++page_index;
        }
    }

    [[nodiscard]] bool DispatchHistoryClient(
        int client,
        const RealtimeHistoryOpenRequestV1& request) noexcept {
        if (client < 0 || history_workers_ == nullptr) {
            return false;
        }
        for (std::uint32_t index = 0U;
             index < config_.maximum_history_readers;
             ++index) {
            HistoryWorkerSlot& slot = history_workers_[index];
            if (slot.running.load(std::memory_order_acquire)) {
                continue;
            }
            if (slot.thread.joinable()) {
                slot.thread.join();
            }
            {
                std::lock_guard<std::mutex> lock(slot.socket_mutex);
                if (slot.client_fd >= 0) {
                    continue;
                }
                slot.client_fd = client;
            }
            slot.running.store(true, std::memory_order_release);
            try {
                slot.thread = std::thread(
                    [this, &slot, client, request]() noexcept {
                        HistoryClientLoop(client, request);
                        {
                            std::lock_guard<std::mutex> lock(
                                slot.socket_mutex);
                            if (slot.client_fd == client) {
                                CloseDescriptor(&slot.client_fd);
                            }
                        }
                        slot.running.store(
                            false, std::memory_order_release);
                    });
                return true;
            } catch (...) {
                slot.running.store(
                    false, std::memory_order_release);
                std::lock_guard<std::mutex> lock(slot.socket_mutex);
                if (slot.client_fd == client) {
                    slot.client_fd = -1;
                }
                static_cast<void>(SendHistoryOpenResponse(
                    client,
                    request.request_id,
                    RealtimeHistoryControlStatusV1::
                        kResourceExhausted,
                    nullptr));
                return false;
            }
        }
        static_cast<void>(SendHistoryOpenResponse(
            client,
            request.request_id,
            RealtimeHistoryControlStatusV1::kResourceExhausted,
            nullptr));
        return false;
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
                if (!HandleClient(client)) {
                    static_cast<void>(::close(client));
                }
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

    [[nodiscard]] bool HandleClient(int client) noexcept {
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
            return false;
        }
        pollfd descriptor{};
        descriptor.fd = client;
        descriptor.events = static_cast<short>(POLLIN);
        const int ready = ::poll(&descriptor, 1U, 250);
        if (ready <= 0 || (descriptor.revents & POLLIN) == 0) {
            return false;
        }
        std::array<std::byte,
                   sizeof(RealtimeHistoryOpenRequestV1)>
            request_bytes{};
        const ssize_t received = ::recv(
            client,
            request_bytes.data(),
            request_bytes.size(),
            MSG_TRUNC);
        RealtimeControlRequestV1 request{};
        if (received >=
            static_cast<ssize_t>(sizeof(request))) {
            std::memcpy(
                &request, request_bytes.data(), sizeof(request));
        }
        if (request.opcode ==
            static_cast<std::uint16_t>(
                RealtimeHistoryControlOpcodeV1::kOpenHistory)) {
            if (received != static_cast<ssize_t>(
                                sizeof(
                                    RealtimeHistoryOpenRequestV1))) {
                static_cast<void>(SendHistoryOpenResponse(
                    client,
                    request.request_id,
                    RealtimeHistoryControlStatusV1::
                        kInvalidRequest,
                    nullptr));
                return false;
            }
            RealtimeHistoryOpenRequestV1 history_request{};
            std::memcpy(
                &history_request,
                request_bytes.data(),
                sizeof(history_request));
            return DispatchHistoryClient(
                client, history_request);
        }
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
        static_cast<void>(
            SendPacket(
                client,
                &response,
                sizeof(response),
                send_fd ? read_only_fd_ : -1,
                250));
        return false;
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
    std::atomic_flag store_publication_in_progress_ =
        ATOMIC_FLAG_INIT;
    std::shared_ptr<const market::IntradayInstrumentStoreGenerationV1>
        store_generation_;
    std::unique_ptr<HistoryWorkerSlot[]> history_workers_;

    int listener_fd_ = -1;
    int stop_event_fd_ = -1;
    int prefix_event_fd_ = -1;
    dev_t socket_device_ = 0;
    ino_t socket_inode_ = 0;
    bool socket_bound_ = false;
    std::atomic<bool> prefix_notification_pending_{false};
    std::atomic<bool> control_stop_requested_{false};
    std::mutex control_stop_mutex_;
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

bool RealtimeSharedMarketServiceV1::PublishStoreGeneration(
    std::shared_ptr<const market::
                        IntradayInstrumentStoreGenerationV1>
        generation) noexcept {
    return impl_ != nullptr &&
           impl_->PublishStoreGeneration(std::move(generation));
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
