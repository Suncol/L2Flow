#include "l2flow/ipc/realtime_certified_service_v1.h"

#include "l2flow/ipc/certified_order_event_journal_v1.h"
#include "l2flow/ipc/certified_order_event_history_v1.h"
#include "l2flow/ipc/certified_tick_journal_v1.h"
#include "l2flow/ipc/realtime_wire_projection_v2.h"
#include "l2flow/control/quality_flags_v1.h"
#include "l2flow/market/market_types_v1.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <new>
#include <span>
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

namespace realtime = l2flow::realtime;
namespace market = l2flow::market;
namespace sdk = l2flow::sdk;
namespace common = l2flow::common;

static_assert(sizeof(std::size_t) <= sizeof(std::uint64_t));

enum class ThreadStartupStateV1 : std::uint8_t {
    kNotStarted = 0U,
    kStarting,
    kReady,
    kFailed,
};

template <typename T>
[[nodiscard]] std::atomic_ref<T> Atomic(T& value) noexcept {
    return std::atomic_ref<T>(value);
}

template <typename T>
[[nodiscard]] std::atomic_ref<T> Atomic(const T& value) noexcept {
    return std::atomic_ref<T>(const_cast<T&>(value));
}

void SetSystemError(int* output, int value) noexcept {
    if (output != nullptr) {
        *output = value;
    }
}

[[nodiscard]] bool ApplyOptionalCurrentThreadCpuSet(
    const common::LinuxCpuSetV1& requested,
    int* system_error_number) noexcept {
    SetSystemError(system_error_number, 0);
    if (requested.empty()) {
        return true;
    }
    const auto error =
        common::ApplyCurrentLinuxThreadAffinityExactV1(
            requested, nullptr, system_error_number);
    if (error == common::LinuxThreadAffinityErrorV1::kNone) {
        return true;
    }
    if (system_error_number != nullptr &&
        *system_error_number == 0) {
        *system_error_number = EINVAL;
    }
    return false;
}

void CloseDescriptor(int* descriptor) noexcept {
    if (descriptor == nullptr || *descriptor < 0) {
        return;
    }
    int result = -1;
    do {
        result = ::close(*descriptor);
    } while (result < 0 && errno == EINTR);
    *descriptor = -1;
}

[[nodiscard]] bool IncrementSaturating(
    std::atomic<std::uint64_t>* counter,
    std::memory_order success_order =
        std::memory_order_relaxed) noexcept {
    if (counter == nullptr) {
        return false;
    }
    std::uint64_t current =
        counter->load(std::memory_order_relaxed);
    for (;;) {
        if (current == std::numeric_limits<std::uint64_t>::max()) {
            return false;
        }
        if (counter->compare_exchange_weak(
                current,
                current + 1U,
                success_order,
                std::memory_order_relaxed)) {
            return true;
        }
    }
}

[[nodiscard]] bool IdentityNonzero(
    const common::Identity128& identity) noexcept {
    return std::any_of(
        identity.begin(), identity.end(), [](std::byte value) {
            return value != std::byte{0};
        });
}

void CopyIdentity(
    const common::Identity128& source,
    std::array<std::uint8_t, 16U>* output) noexcept {
    if (output != nullptr) {
        std::memcpy(output->data(), source.data(), output->size());
    }
}

[[nodiscard]] bool CheckedMultiply(
    std::uint64_t left,
    std::uint64_t right,
    std::uint64_t* output) noexcept {
    if (output == nullptr ||
        (left != 0U &&
         right > std::numeric_limits<std::uint64_t>::max() / left)) {
        return false;
    }
    *output = left * right;
    return true;
}

[[nodiscard]] bool CheckedAdd(
    std::uint64_t left,
    std::uint64_t right,
    std::uint64_t* output) noexcept {
    if (output == nullptr ||
        right > std::numeric_limits<std::uint64_t>::max() - left) {
        return false;
    }
    *output = left + right;
    return true;
}

[[nodiscard]] bool IsPowerOfTwo(std::uint64_t value) noexcept {
    return value >= 2U && (value & (value - 1U)) == 0U;
}

[[nodiscard]] bool ReadMonotonicNs(
    std::uint64_t* output) noexcept {
    if (output == nullptr) {
        return false;
    }
    timespec value{};
    if (::clock_gettime(CLOCK_MONOTONIC, &value) != 0 ||
        value.tv_sec < 0 || value.tv_nsec < 0 ||
        value.tv_nsec >= 1'000'000'000L) {
        return false;
    }
    const std::uint64_t seconds =
        static_cast<std::uint64_t>(value.tv_sec);
    constexpr std::uint64_t billion = 1'000'000'000U;
    if (seconds >
        (std::numeric_limits<std::uint64_t>::max() -
         static_cast<std::uint64_t>(value.tv_nsec)) /
            billion) {
        return false;
    }
    *output =
        seconds * billion + static_cast<std::uint64_t>(value.tv_nsec);
    return *output != 0U;
}

[[nodiscard]] bool ValidSocketPath(
    const std::filesystem::path& path) {
    if (path.empty() || !path.is_absolute()) {
        return false;
    }
    const std::string native = path.string();
    return !native.empty() &&
           native.size() < sizeof(sockaddr_un::sun_path);
}

enum class HandoffKind : std::uint8_t {
    kObservation = 1U,
    kApplied = 2U,
    kPrefixProbe = 3U,
    kPrefixCommit = 4U,
    kProcessStartCoverage = 5U,
};

enum class PrefixCommitPhase : std::uint8_t {
    kNotStarted = 0U,
    kPending,
    kCompleting,
    kSucceeded,
    kFailed,
    kCancelled,
    kWorkerFailed,
};

enum class ProcessStartCoveragePhase : std::uint8_t {
    kNotStarted = 0U,
    kPending,
    kCompleting,
    kReady,
    kFailed,
    kCancelled,
};

struct HandoffEvent final {
    HandoffKind kind = HandoffKind::kObservation;
    std::array<std::uint8_t, 7U> reserved{};
    std::uint64_t barrier_id = 0U;
    realtime::NativeSequenceObservationV1 observation{};
    std::size_t ordinal = 0U;
    const market::RealtimeHistoryRecordV1* record = nullptr;
};
static_assert(std::is_trivially_copyable_v<HandoffEvent>);

// One worker owns this pool. Atomics are used only for lock-free operational
// snapshots; slot/free-list mutation remains serialized. A generation-tagged
// cookie prevents a stale coordinator reference from resolving after reuse.
class CertifiedPayloadLeasePool final {
public:
    struct Slot final {
        RealtimeWireTickPayloadV2 payload{};
        std::uint32_t generation = 0U;
        std::uint32_t free_next =
            std::numeric_limits<std::uint32_t>::max();
        bool occupied = false;
    };

    explicit CertifiedPayloadLeasePool(std::size_t capacity)
        : slots_(capacity) {
        for (std::size_t index = 0U; index < slots_.size(); ++index) {
            slots_[index].free_next =
                index + 1U < slots_.size()
                    ? static_cast<std::uint32_t>(index + 1U)
                    : std::numeric_limits<std::uint32_t>::max();
        }
        if (!slots_.empty()) {
            free_head_ = 0U;
        }
    }

    CertifiedPayloadLeasePool(const CertifiedPayloadLeasePool&) = delete;
    CertifiedPayloadLeasePool& operator=(
        const CertifiedPayloadLeasePool&) = delete;

    [[nodiscard]] bool Acquire(
        RealtimeWireTickPayloadV2** output_payload,
        std::uint64_t* output_cookie) noexcept {
        if (output_payload == nullptr || output_cookie == nullptr ||
            corrupt_.load(std::memory_order_relaxed) ||
            free_head_ == std::numeric_limits<std::uint32_t>::max()) {
            failed_acquires_.fetch_add(1U, std::memory_order_relaxed);
            return false;
        }
        *output_payload = nullptr;
        *output_cookie = 0U;
        const std::uint32_t index = free_head_;
        Slot& slot = slots_[index];
        free_head_ = slot.free_next;
        std::uint32_t generation = slot.generation + 1U;
        if (generation == 0U) {
            generation = 1U;
        }
        slot.generation = generation;
        slot.free_next = std::numeric_limits<std::uint32_t>::max();
        slot.occupied = true;
        const std::uint64_t cookie =
            (static_cast<std::uint64_t>(generation) << 32U) |
            (static_cast<std::uint64_t>(index) + 1U);
        *output_payload = &slot.payload;
        *output_cookie = cookie;
        const std::uint64_t in_use =
            in_use_.fetch_add(1U, std::memory_order_relaxed) + 1U;
        std::uint64_t high_water =
            high_water_.load(std::memory_order_relaxed);
        while (high_water < in_use &&
               !high_water_.compare_exchange_weak(
                   high_water,
                   in_use,
                   std::memory_order_relaxed,
                   std::memory_order_relaxed)) {
        }
        return true;
    }

    [[nodiscard]] const RealtimeWireTickPayloadV2* Resolve(
        std::uint64_t cookie) const noexcept {
        std::uint32_t index = 0U;
        std::uint32_t generation = 0U;
        if (!Decode(cookie, &index, &generation)) {
            return nullptr;
        }
        const Slot& slot = slots_[index];
        return slot.occupied && slot.generation == generation
                   ? &slot.payload
                   : nullptr;
    }

    [[nodiscard]] bool Release(std::uint64_t cookie) noexcept {
        std::uint32_t index = 0U;
        std::uint32_t generation = 0U;
        if (!Decode(cookie, &index, &generation)) {
            MarkCorrupt();
            return false;
        }
        Slot& slot = slots_[index];
        if (!slot.occupied || slot.generation != generation ||
            in_use_.load(std::memory_order_relaxed) == 0U) {
            MarkCorrupt();
            return false;
        }
        slot.occupied = false;
        slot.free_next = free_head_;
        free_head_ = index;
        in_use_.fetch_sub(1U, std::memory_order_relaxed);
        return true;
    }

    // A coordinator failure may have invoked the reclamation callback before
    // MarkTargetApplied returns. In that one serialized interval, an
    // unoccupied slot with the same generation is proof of prior release.
    [[nodiscard]] bool ReleaseIfLive(std::uint64_t cookie) noexcept {
        std::uint32_t index = 0U;
        std::uint32_t generation = 0U;
        if (!Decode(cookie, &index, &generation)) {
            MarkCorrupt();
            return false;
        }
        const Slot& slot = slots_[index];
        if (!slot.occupied && slot.generation == generation) {
            return true;
        }
        return Release(cookie);
    }

    [[nodiscard]] std::uint64_t capacity() const noexcept {
        return static_cast<std::uint64_t>(slots_.size());
    }
    [[nodiscard]] std::uint64_t in_use() const noexcept {
        return in_use_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] std::uint64_t high_water() const noexcept {
        return high_water_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] std::uint64_t failed_acquires() const noexcept {
        return failed_acquires_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] bool corrupt() const noexcept {
        return corrupt_.load(std::memory_order_relaxed);
    }

private:
    [[nodiscard]] bool Decode(
        std::uint64_t cookie,
        std::uint32_t* output_index,
        std::uint32_t* output_generation) const noexcept {
        if (output_index == nullptr || output_generation == nullptr) {
            return false;
        }
        const std::uint32_t encoded_index =
            static_cast<std::uint32_t>(cookie);
        const std::uint32_t generation =
            static_cast<std::uint32_t>(cookie >> 32U);
        if (encoded_index == 0U || generation == 0U) {
            return false;
        }
        const std::uint32_t index = encoded_index - 1U;
        if (index >= slots_.size()) {
            return false;
        }
        *output_index = index;
        *output_generation = generation;
        return true;
    }

    void MarkCorrupt() noexcept {
        corrupt_.store(true, std::memory_order_relaxed);
    }

    std::vector<Slot> slots_;
    std::uint32_t free_head_ =
        std::numeric_limits<std::uint32_t>::max();
    std::atomic<std::uint64_t> in_use_{0U};
    std::atomic<std::uint64_t> high_water_{0U};
    std::atomic<std::uint64_t> failed_acquires_{0U};
    std::atomic<bool> corrupt_{false};
};

void ReleaseCertifiedPayloadLease(
    void* context,
    std::uint64_t cookie) noexcept {
    auto* const pool =
        static_cast<CertifiedPayloadLeasePool*>(context);
    if (pool != nullptr) {
        static_cast<void>(pool->Release(cookie));
    }
}

class CertifiedPayloadLeaseGuard final {
public:
    CertifiedPayloadLeaseGuard(
        CertifiedPayloadLeasePool* pool,
        std::uint64_t cookie) noexcept
        : pool_(pool), cookie_(cookie) {}

    CertifiedPayloadLeaseGuard(const CertifiedPayloadLeaseGuard&) = delete;
    CertifiedPayloadLeaseGuard& operator=(
        const CertifiedPayloadLeaseGuard&) = delete;

    ~CertifiedPayloadLeaseGuard() {
        if (pool_ != nullptr) {
            static_cast<void>(pool_->Release(cookie_));
        }
    }

    [[nodiscard]] bool Release() noexcept {
        if (pool_ == nullptr || !pool_->Release(cookie_)) {
            return false;
        }
        pool_ = nullptr;
        return true;
    }

private:
    CertifiedPayloadLeasePool* pool_ = nullptr;
    std::uint64_t cookie_ = 0U;
};

template <typename Value>
class BoundedMpmcQueue final {
public:
    explicit BoundedMpmcQueue(std::size_t capacity)
        : capacity_(capacity),
          mask_(capacity - 1U),
          cells_(std::make_unique<Cell[]>(capacity)) {
        for (std::size_t index = 0U; index < capacity_; ++index) {
            cells_[index].sequence.store(
                index, std::memory_order_relaxed);
        }
    }

    BoundedMpmcQueue(const BoundedMpmcQueue&) = delete;
    BoundedMpmcQueue& operator=(const BoundedMpmcQueue&) = delete;

    [[nodiscard]] bool TryPush(
        const Value& value,
        bool* became_nonempty = nullptr) noexcept {
        if (became_nonempty != nullptr) {
            *became_nonempty = false;
        }
        std::size_t position =
            enqueue_position_.load(std::memory_order_relaxed);
        for (;;) {
            Cell& cell = cells_[position & mask_];
            const std::size_t sequence =
                cell.sequence.load(std::memory_order_acquire);
            const std::intptr_t difference =
                static_cast<std::intptr_t>(sequence) -
                static_cast<std::intptr_t>(position);
            if (difference == 0) {
                if (enqueue_position_.compare_exchange_weak(
                        position,
                        position + 1U,
                        std::memory_order_relaxed,
                        std::memory_order_relaxed)) {
                    cell.value = value;
                    cell.sequence.store(
                        position + 1U, std::memory_order_release);
                    if (became_nonempty != nullptr) {
                        // Evaluate after publishing the cell. If the consumer
                        // already advanced through it, it is active and needs
                        // no notification; otherwise this cell is the current
                        // queue head and transitions consumable work from
                        // empty to non-empty.
                        *became_nonempty =
                            dequeue_position_.load(
                                std::memory_order_acquire) == position;
                    }
                    return true;
                }
            } else if (difference < 0) {
                return false;
            } else {
                position =
                    enqueue_position_.load(
                        std::memory_order_relaxed);
            }
        }
    }

    [[nodiscard]] bool TryPop(Value* output) noexcept {
        if (output == nullptr) {
            return false;
        }
        std::size_t position =
            dequeue_position_.load(std::memory_order_relaxed);
        for (;;) {
            Cell& cell = cells_[position & mask_];
            const std::size_t sequence =
                cell.sequence.load(std::memory_order_acquire);
            const std::intptr_t difference =
                static_cast<std::intptr_t>(sequence) -
                static_cast<std::intptr_t>(position + 1U);
            if (difference == 0) {
                if (dequeue_position_.compare_exchange_weak(
                        position,
                        position + 1U,
                        std::memory_order_relaxed,
                        std::memory_order_relaxed)) {
                    *output = cell.value;
                    cell.sequence.store(
                        position + capacity_,
                        std::memory_order_release);
                    return true;
                }
            } else if (difference < 0) {
                return false;
            } else {
                position =
                    dequeue_position_.load(
                        std::memory_order_relaxed);
            }
        }
    }

    [[nodiscard]] bool Empty() const noexcept {
        return dequeue_position_.load(std::memory_order_acquire) ==
               enqueue_position_.load(std::memory_order_acquire);
    }

private:
    struct Cell final {
        std::atomic<std::size_t> sequence{0U};
        Value value{};
    };

    const std::size_t capacity_;
    const std::size_t mask_;
    std::unique_ptr<Cell[]> cells_;
    alignas(64) std::atomic<std::size_t> enqueue_position_{0U};
    alignas(64) std::atomic<std::size_t> dequeue_position_{0U};
};

[[nodiscard]] realtime::NativeSequenceDescriptorV1
DescriptorFromPayload(
    const RealtimeWireTickPayloadV2& payload) noexcept {
    realtime::NativeSequenceDescriptorV1 result{};
    result.domain.market =
        payload.common.market == 1U
            ? realtime::NativeSequenceMarketV1::kShanghai
            : realtime::NativeSequenceMarketV1::kShenzhen;
    result.domain.channel =
        static_cast<std::uint32_t>(payload.channel);
    result.sequence =
        static_cast<std::uint64_t>(payload.native_event_sequence);
    return result;
}

[[nodiscard]] sdk::MessageKey MessageKeyFromPayload(
    const RealtimeWireTickPayloadV2& payload) noexcept {
    if (payload.common.market == 1U) {
        return sdk::MessageKey{4U, 101U, 24U};
    }
    return payload.common.event_kind == 4U
               ? sdk::MessageKey{6U, 101U, 33U}
               : sdk::MessageKey{6U, 101U, 36U};
}

[[nodiscard]] RealtimeWireTickPayloadV2 CanonicalBusinessPayload(
    const RealtimeWireTickPayloadV2& payload) noexcept {
    RealtimeWireTickPayloadV2 result = payload;
    result.common.source_sequence = 0U;
    result.common.ingress_sequence = 0U;
    result.common.tick_stream_sequence = 0U;
    result.common.vendor_sequence_id = 0U;
    result.common.recv_realtime_ns = 0;
    result.common.recv_monotonic_ns = 0;
    result.common.vendor_local_time_raw = 0U;
    result.common.source_stream_id = 0U;
    result.common.source_slot = 0U;
    // SDK-header LocalTime is arrival metadata and can legitimately differ
    // across replay deliveries. DecodeTime can contribute only these two
    // diagnostics from that field. All other body-derived quality/notice
    // bits remain part of exact canonical identity. Business nullness is
    // still compared byte-exactly through each projected field's explicit
    // valid/is_null representation.
    result.common.quality_flags &=
        ~l2flow::control::QualityBit(
            l2flow::control::QualityFlagV1::kNullValuePresent);
    result.common.market_notices &=
        ~market::MarketNoticeBitV1(
            market::MarketNoticeV1::kVendorLocalTimeInvalid);
    return result;
}

[[nodiscard]] bool CanPublishSlot(
    const RealtimeCertifiedTickSlotV1& slot) noexcept {
    const std::uint64_t tag =
        Atomic(slot.publish_tag).load(std::memory_order_acquire);
    return (tag & 1U) == 0U &&
           tag <= std::numeric_limits<std::uint64_t>::max() - 2U;
}

[[nodiscard]] bool PublishSlot(
    RealtimeCertifiedTickSlotV1* slot,
    const RealtimeCertifiedTickEnvelopeV1& envelope) noexcept {
    if (slot == nullptr ||
        !RealtimeCertifiedTickEnvelopeCanonicalV1(envelope)) {
        return false;
    }
    std::atomic_ref<std::uint64_t> tag = Atomic(slot->publish_tag);
    std::uint64_t stable = tag.load(std::memory_order_acquire);
    if ((stable & 1U) != 0U ||
        stable > std::numeric_limits<std::uint64_t>::max() - 2U ||
        !tag.compare_exchange_strong(
            stable,
            stable + 1U,
            std::memory_order_acq_rel,
            std::memory_order_acquire)) {
        return false;
    }
    std::atomic_thread_fence(std::memory_order_release);
    const auto words = std::bit_cast<
        std::array<
            std::uint64_t,
            kRealtimeCertifiedTickEnvelopeWordsV1>>(envelope);
    for (std::size_t index = 0U;
         index < slot->payload_words.size();
         ++index) {
        const std::uint64_t value =
            index < words.size() ? words[index] : 0U;
        Atomic(slot->payload_words[index]).store(
            value, std::memory_order_relaxed);
    }
    tag.store(stable + 2U, std::memory_order_release);
    return true;
}

[[nodiscard]] bool CopySlot(
    const RealtimeCertifiedTickSlotV1& slot,
    RealtimeCertifiedTickEnvelopeV1* output) noexcept {
    if (output == nullptr) {
        return false;
    }
    for (std::size_t attempt = 0U; attempt < 5U; ++attempt) {
        const std::uint64_t begin =
            Atomic(slot.publish_tag).load(std::memory_order_acquire);
        if (begin == 0U || (begin & 1U) != 0U) {
            continue;
        }
        RealtimeCertifiedTickSlotV1 copy{};
        copy.publish_tag = begin;
        for (std::size_t index = 0U;
             index < copy.reserved0.size();
             ++index) {
            copy.reserved0[index] =
                Atomic(slot.reserved0[index]).load(
                    std::memory_order_relaxed);
        }
        for (std::size_t index = 0U;
             index < copy.payload_words.size();
             ++index) {
            copy.payload_words[index] =
                Atomic(slot.payload_words[index]).load(
                std::memory_order_relaxed);
        }
        std::atomic_thread_fence(std::memory_order_acq_rel);
        const std::uint64_t end =
            Atomic(slot.publish_tag).load(std::memory_order_acquire);
        if (begin == end && (end & 1U) == 0U) {
            RealtimeCertifiedTickEnvelopeV1 envelope{};
            if (!RealtimeCertifiedTickSlotDecodeV1(
                    copy, &envelope)) {
                return false;
            }
            *output = envelope;
            return true;
        }
    }
    return false;
}

}  // namespace

std::string_view RealtimeCertifiedServiceCreateErrorNameV1(
    RealtimeCertifiedServiceCreateErrorV1 error) noexcept {
    switch (error) {
        case RealtimeCertifiedServiceCreateErrorV1::kNone:
            return "none";
        case RealtimeCertifiedServiceCreateErrorV1::kNullOutput:
            return "null_output";
        case RealtimeCertifiedServiceCreateErrorV1::kInvalidConfiguration:
            return "invalid_configuration";
        case RealtimeCertifiedServiceCreateErrorV1::kLayoutOverflow:
            return "layout_overflow";
        case RealtimeCertifiedServiceCreateErrorV1::kMappingCreateFailed:
            return "mapping_create_failed";
        case RealtimeCertifiedServiceCreateErrorV1::kReadOnlyHandleFailed:
            return "read_only_handle_failed";
        case RealtimeCertifiedServiceCreateErrorV1::kSealFailed:
            return "seal_failed";
        case RealtimeCertifiedServiceCreateErrorV1::kSocketCreateFailed:
            return "socket_create_failed";
        case RealtimeCertifiedServiceCreateErrorV1::kSocketPathExists:
            return "socket_path_exists";
        case RealtimeCertifiedServiceCreateErrorV1::kSocketBindFailed:
            return "socket_bind_failed";
        case RealtimeCertifiedServiceCreateErrorV1::kRecoveryCreateFailed:
            return "recovery_create_failed";
        case RealtimeCertifiedServiceCreateErrorV1::
            kTickJournalCreateFailed:
            return "tick_journal_create_failed";
        case RealtimeCertifiedServiceCreateErrorV1::
            kEventProjectorCreateFailed:
            return "event_projector_create_failed";
        case RealtimeCertifiedServiceCreateErrorV1::kResourceExhausted:
            return "resource_exhausted";
        case RealtimeCertifiedServiceCreateErrorV1::kUnexpectedFailure:
            return "unexpected_failure";
    }
    return "unknown";
}

std::string_view RealtimeCertifiedPrefixFenceOperationErrorNameV1(
    RealtimeCertifiedPrefixFenceOperationErrorV1 error) noexcept {
    switch (error) {
        case RealtimeCertifiedPrefixFenceOperationErrorV1::kNone:
            return "none";
        case RealtimeCertifiedPrefixFenceOperationErrorV1::
            kInvalidArgument:
            return "invalid_argument";
        case RealtimeCertifiedPrefixFenceOperationErrorV1::
            kInvalidLifecycle:
            return "invalid_lifecycle";
        case RealtimeCertifiedPrefixFenceOperationErrorV1::kTimedOut:
            return "timed_out";
        case RealtimeCertifiedPrefixFenceOperationErrorV1::
            kWorkerFailed:
            return "worker_failed";
        case RealtimeCertifiedPrefixFenceOperationErrorV1::
            kInternalFailure:
            return "internal_failure";
    }
    return "unknown";
}

class RealtimeCertifiedMarketServiceV1::Impl final {
private:
    struct ChannelLess final {
        [[nodiscard]] bool operator()(
            const realtime::NativeSequenceChannelV1& left,
            const realtime::NativeSequenceChannelV1& right)
            const noexcept {
            const auto left_market =
                static_cast<std::uint8_t>(left.market);
            const auto right_market =
                static_cast<std::uint8_t>(right.market);
            return left_market < right_market ||
                   (left_market == right_market &&
                    left.channel < right.channel);
        }
    };

    struct ChannelRuntime final {
        std::uint32_t row_index = 0U;
        realtime::NativeSequenceRecoveryChannelSnapshotV1
            snapshot{};
        std::uint64_t certified_tick_count = 0U;
        std::uint64_t last_canonical_apply_sequence = 0U;
        std::uint64_t gap_opened_count = 0U;
        std::uint64_t gap_recovered_count = 0U;
        bool initialized = false;
        bool wire_dirty = false;
        bool gap_active = false;
        bool resource_freeze_counted = false;
        bool conflict_freeze_counted = false;
    };

    struct DrainCertifiedResult final {
        bool made_progress = false;
        bool published_header = false;
        bool state_dirty_after_last_publish = false;
    };

    enum class ChannelAggregateClass : std::uint8_t {
        kHealthy = 0U,
        kGap,
        kCatchingUp,
        kFrozen,
    };

public:
    explicit Impl(RealtimeCertifiedServiceConfigV1 config)
        : config_(std::move(config)) {}

    ~Impl() {
        StopControl();
        SafeUnlinkSocket();
        CloseDescriptor(&listener_fd_);
        CloseDescriptor(&stop_event_fd_);
        CloseDescriptor(&read_only_fd_);
        if (mapping_ != MAP_FAILED) {
            static_cast<void>(::munmap(
                mapping_, static_cast<std::size_t>(mapping_bytes_)));
        }
        CloseDescriptor(&memfd_);
    }

    [[nodiscard]] RealtimeCertifiedServiceCreateErrorV1 Initialize(
        int* system_error_number) {
        SetSystemError(system_error_number, 0);
        if constexpr (std::endian::native != std::endian::little) {
            return RealtimeCertifiedServiceCreateErrorV1::
                kInvalidConfiguration;
        }
        if (!IdentityNonzero(config_.run_id) ||
            config_.session_epoch == 0U ||
            config_.trade_date == 0U ||
            config_.daily_catalog == nullptr ||
            config_.daily_catalog->trade_date() !=
                config_.trade_date ||
            !config_.daily_catalog->coverage_complete() ||
            config_.daily_catalog->instrument_count() == 0U ||
            config_.daily_catalog->instrument_count() >
                std::numeric_limits<std::uint32_t>::max() ||
            config_.fast_sink == nullptr ||
            config_.certified_tick_ring_capacity == 0U ||
            config_.certified_tick_ring_capacity >
                std::numeric_limits<std::uint32_t>::max() ||
            config_.maximum_certified_ticks == 0U ||
            (config_.maximum_certified_tick_mapping_bytes != 0U &&
             config_.maximum_certified_tick_mapping_bytes <
                 kCertifiedTickJournalHeaderBytesV1) ||
            config_.certified_tick_lazy_commit_chunk_bytes < 4096U ||
            config_.certified_tick_lazy_commit_chunk_bytes % 4096U !=
                0U ||
            config_.channel_capacity == 0U ||
            !IsPowerOfTwo(config_.handoff_queue_capacity) ||
            config_.handoff_queue_capacity >
                std::numeric_limits<std::size_t>::max() ||
            config_.maximum_pending_entries == 0U ||
            config_.maximum_pending_entries >
                std::numeric_limits<std::size_t>::max() ||
            config_.maximum_pending_entries >=
                std::numeric_limits<std::uint32_t>::max() ||
            config_.maximum_pending_entries_per_channel == 0U ||
            config_.maximum_pending_entries_per_channel >
                config_.maximum_pending_entries ||
            config_.certified_duplicate_retention_entries >
                std::numeric_limits<std::size_t>::max() ||
            config_.maximum_reorder_span == 0U ||
            (config_.origin_policy !=
                 realtime::NativeSequenceOriginPolicyV1::
                     kExplicitOrigin &&
             config_.origin_policy !=
                 realtime::NativeSequenceOriginPolicyV1::
                     kBoundedProcessStart) ||
            (config_.origin_policy ==
                     realtime::NativeSequenceOriginPolicyV1::
                         kExplicitOrigin
                 ? (config_.maximum_backward_displacement != 0U ||
                    config_.event_temporal_coverage !=
                        CertifiedOrderEventTemporalCoverageV1::
                            kFromOpen ||
                    config_.event_coverage_start_unix_ns != 0U)
                 : (config_.maximum_backward_displacement >=
                        config_.maximum_reorder_span ||
                    config_.maximum_backward_displacement >=
                        config_.maximum_pending_entries_per_channel ||
                    config_.event_temporal_coverage !=
                        CertifiedOrderEventTemporalCoverageV1::
                            kFromProcessStart ||
                    config_.event_coverage_start_unix_ns == 0U ||
                    config_.control_exposure_gate == nullptr ||
                    config_.control_exposure_gate->load(
                        std::memory_order_acquire) ||
                    config_.control_requires_prefix_commit)) ||
            config_.maximum_order_states == 0U ||
            config_.maximum_derived_events == 0U ||
            (config_.maximum_derived_event_mapping_bytes != 0U &&
             config_.maximum_derived_event_mapping_bytes <
                 kCertifiedOrderEventHeaderBytesV1) ||
            config_.derived_event_lazy_commit_chunk_bytes <
                4096U ||
            config_.derived_event_lazy_commit_chunk_bytes %
                    4096U !=
                0U ||
            !ValidSocketPath(config_.control_socket_path)) {
            return RealtimeCertifiedServiceCreateErrorV1::
                kInvalidConfiguration;
        }

        // A runtime can occur at most once in the dirty set. Reserving the
        // configured channel bound keeps UpdateChannel allocation-free.
        dirty_channels_.reserve(config_.channel_capacity);

        worker_cpu_set_.Clear();
        control_cpu_set_.Clear();
        tick_history_worker_cpu_set_.Clear();
        const std::string& tick_history_cpu_set =
            config_.tick_history_worker_cpu_set.empty()
                ? config_.worker_cpu_set
                : config_.tick_history_worker_cpu_set;
        if ((!config_.worker_cpu_set.empty() &&
             common::ParseLinuxCpuSetV1(
                 config_.worker_cpu_set, &worker_cpu_set_) !=
                 common::LinuxCpuSetParseErrorV1::kNone) ||
            (!config_.control_cpu_set.empty() &&
             common::ParseLinuxCpuSetV1(
                 config_.control_cpu_set, &control_cpu_set_) !=
                 common::LinuxCpuSetParseErrorV1::kNone) ||
            (!tick_history_cpu_set.empty() &&
             common::ParseLinuxCpuSetV1(
                 tick_history_cpu_set,
                 &tick_history_worker_cpu_set_) !=
                 common::LinuxCpuSetParseErrorV1::kNone)) {
            SetSystemError(system_error_number, EINVAL);
            return RealtimeCertifiedServiceCreateErrorV1::
                kInvalidConfiguration;
        }

        const std::uint64_t latest_capacity =
            static_cast<std::uint64_t>(
                config_.daily_catalog->instrument_count());
        std::uint64_t latest_bytes = 0U;
        std::uint64_t ring_bytes = 0U;
        std::uint64_t channel_bytes = 0U;
        std::uint64_t next_offset = 0U;
        if (!CheckedMultiply(
                latest_capacity,
                kRealtimeCertifiedTickSlotBytesV1,
                &latest_bytes) ||
            !CheckedMultiply(
                config_.certified_tick_ring_capacity,
                kRealtimeCertifiedTickSlotBytesV1,
                &ring_bytes) ||
            !CheckedMultiply(
                config_.channel_capacity,
                kRealtimeCertifiedChannelStateBytesV1,
                &channel_bytes) ||
            !CheckedAdd(
                kRealtimeCertifiedHeaderBytesV1,
                latest_bytes,
                &certified_ring_offset_) ||
            !CheckedAdd(
                certified_ring_offset_,
                ring_bytes,
                &channel_state_offset_) ||
            !CheckedAdd(
                channel_state_offset_,
                channel_bytes,
                &next_offset) ||
            next_offset > config_.maximum_mapping_bytes ||
            next_offset >
                static_cast<std::uint64_t>(
                    std::numeric_limits<std::size_t>::max()) ||
            next_offset >
                static_cast<std::uint64_t>(
                    std::numeric_limits<off_t>::max())) {
            return RealtimeCertifiedServiceCreateErrorV1::
                kLayoutOverflow;
        }
        mapping_bytes_ = next_offset;

        // At most maximum_pending_entries canonical payloads can be retained
        // by pending coordinator entries. One additional transient slot is
        // required to compare an application for an existing entry while all
        // canonical slots are occupied. The configuration validation above
        // deliberately keeps maximum_pending_entries below UINT32_MAX, so the
        // generation-tagged 32-bit slot identity remains representable.
        payload_leases_ =
            std::make_unique<CertifiedPayloadLeasePool>(
                static_cast<std::size_t>(
                    config_.maximum_pending_entries) + 1U);

        realtime::NativeSequenceRecoveryConfigV1 recovery_config{};
        recovery_config.maximum_channels = config_.channel_capacity;
        recovery_config.maximum_pending_entries =
            static_cast<std::size_t>(
                config_.maximum_pending_entries);
        recovery_config.maximum_pending_entries_per_channel =
            static_cast<std::size_t>(
                config_.maximum_pending_entries_per_channel);
        recovery_config.certified_duplicate_retention_entries =
            static_cast<std::size_t>(
                config_.certified_duplicate_retention_entries);
        recovery_config.maximum_canonical_payload_bytes_per_entry =
            sizeof(RealtimeWireTickPayloadV2);
        std::uint64_t payload_entry_count = 0U;
        std::uint64_t payload_bytes = 0U;
        if (!CheckedAdd(
                config_.maximum_pending_entries,
                config_.certified_duplicate_retention_entries,
                &payload_entry_count) ||
            !CheckedMultiply(
                payload_entry_count,
                sizeof(RealtimeWireTickPayloadV2),
                &payload_bytes) ||
            payload_bytes >
                std::numeric_limits<std::size_t>::max()) {
            return RealtimeCertifiedServiceCreateErrorV1::
                kInvalidConfiguration;
        }
        recovery_config.maximum_total_canonical_payload_bytes =
            static_cast<std::size_t>(payload_bytes);
        recovery_config.release_applied_cookie =
            &ReleaseCertifiedPayloadLease;
        recovery_config.release_applied_cookie_context =
            payload_leases_.get();
        recovery_config.maximum_reorder_span =
            config_.maximum_reorder_span;
        recovery_config.origin_policy = config_.origin_policy;
        recovery_config.maximum_backward_displacement =
            config_.maximum_backward_displacement;
        recovery_config.expected_origin_sequence = 1U;
        recovery_config.trusted_checkpoint_sequence = 0U;
        if (realtime::NativeSequenceRecoveryCoordinatorV1::Create(
                recovery_config, &recovery_) !=
                realtime::NativeSequenceRecoveryCreateErrorV1::kNone ||
            recovery_ == nullptr) {
            return RealtimeCertifiedServiceCreateErrorV1::
                kRecoveryCreateFailed;
        }

        CertifiedTickJournalConfigV1 tick_journal_config{};
        tick_journal_config.run_id = config_.run_id;
        tick_journal_config.session_epoch = config_.session_epoch;
        tick_journal_config.trade_date = config_.trade_date;
        tick_journal_config.tick_capacity =
            config_.maximum_certified_ticks;
        tick_journal_config.maximum_mapping_bytes =
            config_.maximum_certified_tick_mapping_bytes;
        tick_journal_config.lazy_commit_chunk_bytes =
            config_.certified_tick_lazy_commit_chunk_bytes;
        const CertifiedTickJournalCreateErrorV1 tick_journal_error =
            CertifiedTickJournalProducerV1::Create(
                tick_journal_config,
                &tick_journal_,
                system_error_number);
        if (tick_journal_error !=
                CertifiedTickJournalCreateErrorV1::kNone ||
            tick_journal_ == nullptr) {
            switch (tick_journal_error) {
                case CertifiedTickJournalCreateErrorV1::
                    kInvalidConfiguration:
                    return RealtimeCertifiedServiceCreateErrorV1::
                        kInvalidConfiguration;
                case CertifiedTickJournalCreateErrorV1::
                    kLayoutOverflow:
                    return RealtimeCertifiedServiceCreateErrorV1::
                        kLayoutOverflow;
                case CertifiedTickJournalCreateErrorV1::
                    kResourceExhausted:
                    return RealtimeCertifiedServiceCreateErrorV1::
                        kResourceExhausted;
                case CertifiedTickJournalCreateErrorV1::kNone:
                case CertifiedTickJournalCreateErrorV1::kNullOutput:
                case CertifiedTickJournalCreateErrorV1::
                    kMappingCreateFailed:
                case CertifiedTickJournalCreateErrorV1::
                    kReadOnlyHandleFailed:
                case CertifiedTickJournalCreateErrorV1::kSealFailed:
                case CertifiedTickJournalCreateErrorV1::
                    kUnexpectedFailure:
                    return RealtimeCertifiedServiceCreateErrorV1::
                        kTickJournalCreateFailed;
            }
            return RealtimeCertifiedServiceCreateErrorV1::
                kTickJournalCreateFailed;
        }

        std::uint64_t event_slot_bytes = 0U;
        std::uint64_t event_logical_bytes = 0U;
        std::uint64_t exact_event_mapping_bytes = 0U;
        constexpr std::uint64_t page_mask = 4096U - 1U;
        if (!CheckedMultiply(
                static_cast<std::uint64_t>(
                    config_.maximum_derived_events),
                kCertifiedOrderEventSlotBytesV1,
                &event_slot_bytes) ||
            !CheckedAdd(
                kCertifiedOrderEventHeaderBytesV1,
                event_slot_bytes,
                &event_logical_bytes) ||
            event_logical_bytes >
                std::numeric_limits<std::uint64_t>::max() -
                    page_mask) {
            return RealtimeCertifiedServiceCreateErrorV1::
                kInvalidConfiguration;
        }
        exact_event_mapping_bytes =
            (event_logical_bytes + page_mask) & ~page_mask;
        if (config_.maximum_derived_event_mapping_bytes != 0U &&
            exact_event_mapping_bytes >
                config_.maximum_derived_event_mapping_bytes) {
            return RealtimeCertifiedServiceCreateErrorV1::
                kLayoutOverflow;
        }

        CertifiedOrderEventJournalConfigV1 event_wire_config{};
        event_wire_config.run_id = config_.run_id;
        event_wire_config.session_epoch = config_.session_epoch;
        event_wire_config.trade_date = config_.trade_date;
        event_wire_config.event_capacity =
            static_cast<std::uint64_t>(
                config_.maximum_derived_events);
        event_wire_config.maximum_mapping_bytes =
            exact_event_mapping_bytes;
        event_wire_config.lazy_commit_chunk_bytes =
            config_.derived_event_lazy_commit_chunk_bytes;
        event_wire_config.temporal_coverage =
            config_.event_temporal_coverage;
        event_wire_config.coverage_start_unix_ns =
            config_.event_coverage_start_unix_ns;
        if (CertifiedOrderEventJournalProducerV1::Create(
                event_wire_config,
                &event_journal_,
                system_error_number) !=
                CertifiedOrderEventJournalCreateErrorV1::kNone ||
            event_journal_ == nullptr) {
            return RealtimeCertifiedServiceCreateErrorV1::
                kEventProjectorCreateFailed;
        }

        CertifiedOrderEventHistoryConfigV1 event_config{};
        event_config.trade_date = config_.trade_date;
        event_config.maximum_shanghai_order_states =
            config_.maximum_order_states;
        event_config.maximum_shenzhen_order_states =
            config_.maximum_order_states;
        event_config.maximum_events =
            config_.maximum_derived_events;
        event_config.external_journal = event_journal_;
        if (CertifiedOrderEventHistoryV1::Create(
                event_config, &event_history_) !=
                CertifiedOrderEventHistoryErrorV1::kNone ||
            event_history_ == nullptr) {
            return RealtimeCertifiedServiceCreateErrorV1::
                kEventProjectorCreateFailed;
        }
        if (event_history_->AcquireGeneration(
                &committed_event_generation_) !=
                CertifiedOrderEventHistoryErrorV1::kNone ||
            !committed_event_generation_.valid() ||
            committed_event_generation_.generation()
                    .input_frontier.canonical_apply_sequence !=
                0U) {
            return RealtimeCertifiedServiceCreateErrorV1::
                kEventProjectorCreateFailed;
        }

        queue_ = std::make_unique<BoundedMpmcQueue<HandoffEvent>>(
            static_cast<std::size_t>(
                config_.handoff_queue_capacity));
        memfd_ = ::memfd_create(
            "l2flow-certified-v1",
            MFD_CLOEXEC | MFD_ALLOW_SEALING);
        if (memfd_ < 0 ||
            ::ftruncate(
                memfd_, static_cast<off_t>(mapping_bytes_)) != 0) {
            SetSystemError(system_error_number, errno);
            return RealtimeCertifiedServiceCreateErrorV1::
                kMappingCreateFailed;
        }
        // ftruncate alone leaves a sparse shmem object whose later first
        // write may SIGBUS if backing space is exhausted. Reserve the bounded
        // Tick mapping before mmap/memset so ordinary ENOSPC/EDQUOT is a
        // recoverable Create failure and production can retain FAST-only
        // operation.
        int allocation_result = -1;
        do {
            allocation_result = ::fallocate(
                memfd_,
                0,
                0,
                static_cast<off_t>(mapping_bytes_));
        } while (allocation_result != 0 && errno == EINTR);
        if (allocation_result != 0) {
            SetSystemError(system_error_number, errno);
            return RealtimeCertifiedServiceCreateErrorV1::
                kResourceExhausted;
        }
        mapping_ = ::mmap(
            nullptr,
            static_cast<std::size_t>(mapping_bytes_),
            PROT_READ | PROT_WRITE,
            MAP_SHARED,
            memfd_,
            0);
        if (mapping_ == MAP_FAILED ||
            ::madvise(
                mapping_,
                static_cast<std::size_t>(mapping_bytes_),
                MADV_DONTFORK) != 0) {
            SetSystemError(system_error_number, errno);
            return RealtimeCertifiedServiceCreateErrorV1::
                kMappingCreateFailed;
        }
        std::memset(
            mapping_, 0, static_cast<std::size_t>(mapping_bytes_));
        const std::string proc_path =
            "/proc/self/fd/" + std::to_string(memfd_);
        read_only_fd_ =
            ::open(proc_path.c_str(), O_RDONLY | O_CLOEXEC);
        if (read_only_fd_ < 0) {
            SetSystemError(system_error_number, errno);
            return RealtimeCertifiedServiceCreateErrorV1::
                kReadOnlyHandleFailed;
        }
        constexpr int seals =
            F_SEAL_GROW | F_SEAL_SHRINK | F_SEAL_FUTURE_WRITE |
            F_SEAL_SEAL;
        if (::fcntl(memfd_, F_ADD_SEALS, seals) != 0) {
            SetSystemError(system_error_number, errno);
            return RealtimeCertifiedServiceCreateErrorV1::kSealFailed;
        }

        header_ = static_cast<RealtimeCertifiedHeaderV1*>(mapping_);
        latest_ = reinterpret_cast<RealtimeCertifiedTickSlotV1*>(
            static_cast<std::byte*>(mapping_) +
            kRealtimeCertifiedHeaderBytesV1);
        ring_ = reinterpret_cast<RealtimeCertifiedTickSlotV1*>(
            static_cast<std::byte*>(mapping_) +
            certified_ring_offset_);
        channel_rows_ =
            reinterpret_cast<RealtimeCertifiedChannelStateV1*>(
                static_cast<std::byte*>(mapping_) +
                channel_state_offset_);

        std::uint64_t now = 0U;
        if (!ReadMonotonicNs(&now)) {
            return RealtimeCertifiedServiceCreateErrorV1::
                kUnexpectedFailure;
        }
        header_->magic = kRealtimeCertifiedShmMagicV1;
        header_->abi_major = kRealtimeCertifiedWireMajorV1;
        header_->abi_minor = kRealtimeCertifiedWireMinorV1;
        header_->header_bytes = kRealtimeCertifiedHeaderBytesV1;
        header_->endian_marker =
            kRealtimeCertifiedLittleEndianMarkerV1;
        header_->flags = 0U;
        header_->total_mapping_bytes = mapping_bytes_;
        CopyIdentity(config_.run_id, &header_->run_id);
        header_->session_epoch = config_.session_epoch;
        header_->trade_date = config_.trade_date;
        header_->latest_offset =
            kRealtimeCertifiedHeaderBytesV1;
        header_->certified_ring_offset =
            certified_ring_offset_;
        header_->channel_state_offset = channel_state_offset_;
        header_->latest_capacity =
            static_cast<std::uint32_t>(latest_capacity);
        header_->certified_ring_capacity =
            static_cast<std::uint32_t>(
                config_.certified_tick_ring_capacity);
        header_->channel_state_capacity =
            config_.channel_capacity;
        header_->latest_stride =
            kRealtimeCertifiedTickSlotBytesV1;
        header_->certified_ring_stride =
            kRealtimeCertifiedTickSlotBytesV1;
        header_->channel_state_stride =
            kRealtimeCertifiedChannelStateBytesV1;
        header_->region_alignment =
            kRealtimeCertifiedRegionAlignmentV1;
        header_->status_publish_tag = 2U;
        header_->heartbeat_monotonic_ns = now;
        header_->correction_epoch = correction_epoch_;
        header_->aggregate_state = static_cast<std::uint32_t>(
            RealtimeCertifiedStateV1::kNoData);
        if (!RealtimeCertifiedHeaderCanonicalV1(*header_)) {
            return RealtimeCertifiedServiceCreateErrorV1::
                kUnexpectedFailure;
        }

        stop_event_fd_ =
            ::eventfd(0U, EFD_CLOEXEC | EFD_NONBLOCK);
        if (stop_event_fd_ < 0) {
            SetSystemError(system_error_number, errno);
            return RealtimeCertifiedServiceCreateErrorV1::
                kSocketCreateFailed;
        }
        return CreateListener(system_error_number);
    }

    [[nodiscard]] bool StartWorker(
        int* system_error_number) noexcept {
        SetSystemError(system_error_number, 0);
        bool expected = false;
        if (!started_.compare_exchange_strong(
                expected, true, std::memory_order_acq_rel)) {
            return false;
        }
        tick_history_stop_requested_.store(
            false, std::memory_order_relaxed);
        tick_history_affinity_system_error_.store(
            0, std::memory_order_relaxed);
        tick_history_startup_state_.store(
            ThreadStartupStateV1::kStarting,
            std::memory_order_release);
        try {
            tick_history_thread_ = std::thread([this]() noexcept {
                int affinity_error = 0;
                if (!ApplyOptionalCurrentThreadCpuSet(
                        tick_history_worker_cpu_set_,
                        &affinity_error)) {
                    tick_history_affinity_system_error_.store(
                        affinity_error, std::memory_order_relaxed);
                    tick_history_failed_.store(
                        true, std::memory_order_release);
                    tick_history_startup_state_.store(
                        ThreadStartupStateV1::kFailed,
                        std::memory_order_release);
                    tick_history_startup_state_.notify_all();
                    return;
                }
                tick_history_writer_running_.store(
                    true, std::memory_order_release);
                tick_history_startup_state_.store(
                    ThreadStartupStateV1::kReady,
                    std::memory_order_release);
                tick_history_startup_state_.notify_all();
                TickHistoryWriterLoop();
                tick_history_writer_running_.store(
                    false, std::memory_order_release);
            });
        } catch (...) {
            tick_history_affinity_system_error_.store(
                EAGAIN, std::memory_order_relaxed);
            tick_history_failed_.store(
                true, std::memory_order_release);
            tick_history_startup_state_.store(
                ThreadStartupStateV1::kFailed,
                std::memory_order_release);
            tick_history_startup_state_.notify_all();
        }
        ThreadStartupStateV1 tick_history_startup =
            tick_history_startup_state_.load(
                std::memory_order_acquire);
        while (tick_history_startup ==
               ThreadStartupStateV1::kStarting) {
            tick_history_startup_state_.wait(
                tick_history_startup, std::memory_order_acquire);
            tick_history_startup =
                tick_history_startup_state_.load(
                    std::memory_order_acquire);
        }
        if (tick_history_startup != ThreadStartupStateV1::kReady) {
            tick_history_stop_requested_.store(
                true, std::memory_order_release);
            if (tick_history_thread_.joinable()) {
                tick_history_thread_.join();
            }
            if (tick_journal_ != nullptr) {
                static_cast<void>(
                    tick_journal_->MarkUpstreamFailed());
            }
            const int affinity_error =
                tick_history_affinity_system_error_.load(
                    std::memory_order_relaxed);
            SetSystemError(
                system_error_number,
                affinity_error == 0 ? EINVAL : affinity_error);
            return false;
        }
        worker_affinity_system_error_.store(
            0, std::memory_order_relaxed);
        worker_startup_state_.store(
            ThreadStartupStateV1::kStarting,
            std::memory_order_release);
        try {
            worker_thread_ = std::thread([this]() noexcept {
                int affinity_error = 0;
                if (!ApplyOptionalCurrentThreadCpuSet(
                        worker_cpu_set_, &affinity_error)) {
                    worker_affinity_system_error_.store(
                        affinity_error, std::memory_order_relaxed);
                    worker_startup_state_.store(
                        ThreadStartupStateV1::kFailed,
                        std::memory_order_release);
                    worker_startup_state_.notify_all();
                    return;
                }
                accepting_.store(true, std::memory_order_release);
                worker_running_.store(true, std::memory_order_release);
                worker_startup_state_.store(
                    ThreadStartupStateV1::kReady,
                    std::memory_order_release);
                worker_startup_state_.notify_all();
                WorkerLoop();
            });
        } catch (...) {
            worker_affinity_system_error_.store(
                EAGAIN, std::memory_order_relaxed);
            worker_startup_state_.store(
                ThreadStartupStateV1::kFailed,
                std::memory_order_release);
            worker_startup_state_.notify_all();
        }
        ThreadStartupStateV1 startup =
            worker_startup_state_.load(std::memory_order_acquire);
        while (startup == ThreadStartupStateV1::kStarting) {
            worker_startup_state_.wait(
                startup, std::memory_order_acquire);
            startup = worker_startup_state_.load(
                std::memory_order_acquire);
        }
        if (startup == ThreadStartupStateV1::kReady) {
            return true;
        }
        accepting_.store(false, std::memory_order_release);
        worker_stop_requested_.store(true, std::memory_order_release);
        worker_running_.store(false, std::memory_order_release);
        WakeWorker();
        if (worker_thread_.joinable()) {
            worker_thread_.join();
        }
        // The append-only journal must not advertise a clean empty stream
        // when its owning CERTIFIED worker never became usable.  Serialize
        // the terminal transition against the already-running asynchronous
        // writer before asking that thread to exit.
        {
            const std::lock_guard<std::mutex> writer(
                tick_history_writer_mutex_);
            if (tick_journal_ != nullptr) {
                static_cast<void>(
                    tick_journal_->MarkUpstreamFailed());
            }
            tick_history_failed_.store(
                true, std::memory_order_release);
        }
        StopTickHistoryWriter();
        const int affinity_error =
            worker_affinity_system_error_.load(
                std::memory_order_relaxed);
        SetSystemError(
            system_error_number,
            affinity_error == 0 ? EINVAL : affinity_error);
        return false;
    }

    [[nodiscard]] bool StartControl(
        int* system_error_number) noexcept {
        std::lock_guard<std::mutex> lifecycle(
            control_lifecycle_mutex_);
        SetSystemError(system_error_number, 0);
        if (!started_.load(std::memory_order_acquire) ||
            !worker_running_.load(std::memory_order_acquire)) {
            SetSystemError(system_error_number, EINVAL);
            return false;
        }
        if (prefix_probe_active_.load(std::memory_order_acquire)) {
            SetSystemError(system_error_number, EBUSY);
            return false;
        }
        if ((config_.control_requires_prefix_commit ||
             prefix_barrier_started_.load(std::memory_order_acquire)) &&
            !prefix_barrier_completed_.load(
                std::memory_order_acquire)) {
            SetSystemError(system_error_number, EBUSY);
            return false;
        }
        auto expected =
            RealtimeCertifiedServiceSnapshotV1::ControlState::
                kNotStarted;
        if (!control_state_.compare_exchange_strong(
                expected,
                RealtimeCertifiedServiceSnapshotV1::ControlState::
                    kRunning,
                std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            return false;
        }
        return StartClaimedControl(system_error_number);
    }

    [[nodiscard]] RealtimeCertifiedPrefixFenceOperationErrorV1
    ProbePrefixFence(
        std::chrono::milliseconds timeout,
        RealtimeCertifiedPrefixFenceResultV1* output,
        int* system_error_number) noexcept {
        SetSystemError(system_error_number, 0);
        if (output == nullptr) {
            SetSystemError(system_error_number, EINVAL);
            return RealtimeCertifiedPrefixFenceOperationErrorV1::
                kInvalidArgument;
        }
        *output = {};
        if (timeout <= std::chrono::milliseconds::zero() ||
            timeout > std::chrono::hours(24)) {
            output->operation_error =
                RealtimeCertifiedPrefixFenceOperationErrorV1::
                    kInvalidArgument;
            SetSystemError(system_error_number, EINVAL);
            return output->operation_error;
        }
        const auto deadline =
            std::chrono::steady_clock::now() + timeout;
        // Only fence callers serialize for the potentially long queue/wait
        // interval. The control lifecycle lock is held just long enough to
        // establish whether this probe or StartControl linearizes first, so
        // StopControl is never blocked by a caller-supplied fence timeout.
        std::lock_guard<std::mutex> prefix_call(prefix_call_mutex_);
        {
            std::lock_guard<std::mutex> lifecycle(
                control_lifecycle_mutex_);
            if (!started_.load(std::memory_order_acquire) ||
                control_state_.load(std::memory_order_acquire) !=
                    RealtimeCertifiedServiceSnapshotV1::ControlState::
                        kNotStarted ||
                prefix_commit_phase_.load(
                    std::memory_order_acquire) !=
                    PrefixCommitPhase::kNotStarted) {
                output->operation_error =
                    RealtimeCertifiedPrefixFenceOperationErrorV1::
                        kInvalidLifecycle;
                SetSystemError(system_error_number, EINVAL);
                return output->operation_error;
            }
            if (!worker_running_.load(std::memory_order_acquire) ||
                worker_stop_requested_.load(
                    std::memory_order_acquire)) {
                output->operation_error =
                    RealtimeCertifiedPrefixFenceOperationErrorV1::
                        kWorkerFailed;
                SetSystemError(system_error_number, EPIPE);
                return output->operation_error;
            }
            prefix_probe_active_.store(true, std::memory_order_release);
        }
        const auto finish = [this](
            RealtimeCertifiedPrefixFenceOperationErrorV1 error) noexcept {
            prefix_probe_waiting_for_prior_ack_for_test_.store(
                false, std::memory_order_release);
            prefix_probe_active_.store(false, std::memory_order_release);
            prefix_probe_active_.notify_all();
            return error;
        };
        if (outstanding_probe_fence_id_ != 0U) {
            prefix_probe_waiting_for_prior_ack_for_test_.store(
                true, std::memory_order_release);
            for (;;) {
                if (completed_prefix_fence_id_.load(
                        std::memory_order_acquire) >=
                    outstanding_probe_fence_id_) {
                    outstanding_probe_fence_id_ = 0U;
                    prefix_probe_waiting_for_prior_ack_for_test_.store(
                        false, std::memory_order_release);
                    break;
                }
                if (!worker_running_.load(std::memory_order_acquire) ||
                    worker_stop_requested_.load(
                        std::memory_order_acquire)) {
                    output->operation_error =
                        RealtimeCertifiedPrefixFenceOperationErrorV1::
                            kWorkerFailed;
                    SetSystemError(system_error_number, EPIPE);
                    return finish(output->operation_error);
                }
                if (std::chrono::steady_clock::now() >= deadline) {
                    output->operation_error =
                        RealtimeCertifiedPrefixFenceOperationErrorV1::
                            kTimedOut;
                    SetSystemError(system_error_number, ETIMEDOUT);
                    return finish(output->operation_error);
                }
                std::this_thread::yield();
            }
        }
        if (std::chrono::steady_clock::now() >= deadline) {
            output->operation_error =
                RealtimeCertifiedPrefixFenceOperationErrorV1::kTimedOut;
            SetSystemError(system_error_number, ETIMEDOUT);
            return finish(output->operation_error);
        }
        std::uint64_t fence_id = 0U;
        if (!AllocatePrefixFenceId(&fence_id)) {
            output->operation_error =
                RealtimeCertifiedPrefixFenceOperationErrorV1::
                    kInternalFailure;
            SetSystemError(system_error_number, EOVERFLOW);
            return finish(output->operation_error);
        }
        output->fence_id = fence_id;
        HandoffEvent fence{};
        fence.kind = HandoffKind::kPrefixProbe;
        fence.barrier_id = fence_id;
        for (;;) {
            bool became_nonempty = false;
            if (queue_ != nullptr &&
                queue_->TryPush(fence, &became_nonempty)) {
                outstanding_probe_fence_id_ = fence_id;
                if (became_nonempty) {
                    WakeWorker();
                }
                break;
            }
            if (!worker_running_.load(std::memory_order_acquire) ||
                worker_stop_requested_.load(
                    std::memory_order_acquire)) {
                output->operation_error =
                    RealtimeCertifiedPrefixFenceOperationErrorV1::
                        kWorkerFailed;
                SetSystemError(system_error_number, EPIPE);
                return finish(output->operation_error);
            }
            if (std::chrono::steady_clock::now() >= deadline) {
                output->operation_error =
                    RealtimeCertifiedPrefixFenceOperationErrorV1::
                        kTimedOut;
                SetSystemError(system_error_number, ETIMEDOUT);
                return finish(output->operation_error);
            }
            std::this_thread::yield();
        }
        for (;;) {
            if (completed_prefix_fence_id_.load(
                    std::memory_order_acquire) >= fence_id) {
                if (!CopyCompletedPrefixFence(fence_id, output)) {
                    output->operation_error =
                        RealtimeCertifiedPrefixFenceOperationErrorV1::
                            kInternalFailure;
                    SetSystemError(system_error_number, EIO);
                } else if (output->operation_error ==
                           RealtimeCertifiedPrefixFenceOperationErrorV1::
                               kInternalFailure) {
                    SetSystemError(system_error_number, EIO);
                }
                if (outstanding_probe_fence_id_ == fence_id) {
                    outstanding_probe_fence_id_ = 0U;
                }
                return finish(output->operation_error);
            }
            if (!worker_running_.load(std::memory_order_acquire) ||
                worker_stop_requested_.load(
                    std::memory_order_acquire)) {
                output->operation_error =
                    RealtimeCertifiedPrefixFenceOperationErrorV1::
                        kWorkerFailed;
                SetSystemError(system_error_number, EPIPE);
                return finish(output->operation_error);
            }
            if (std::chrono::steady_clock::now() >= deadline) {
                output->operation_error =
                    RealtimeCertifiedPrefixFenceOperationErrorV1::
                        kTimedOut;
                SetSystemError(system_error_number, ETIMEDOUT);
                return finish(output->operation_error);
            }
            std::this_thread::yield();
        }
    }

    [[nodiscard]] bool WaitForPrefixBarrier(
        std::chrono::milliseconds timeout,
        RealtimeCertifiedPrefixFenceResultV1* output,
        int* system_error_number) noexcept {
        SetSystemError(system_error_number, 0);
        if (output != nullptr) {
            *output = {};
        }
        if (timeout <= std::chrono::milliseconds::zero() ||
            timeout > std::chrono::hours(24)) {
            SetFenceOperationError(
                output,
                RealtimeCertifiedPrefixFenceOperationErrorV1::
                    kInvalidArgument);
            SetSystemError(system_error_number, EINVAL);
            return false;
        }
        // Fence calls serialize independently. The lifecycle lock protects
        // only the transition that orders this final commit against both
        // StartControl and StopControl; the queue/wait interval holds no
        // control lock.
        std::lock_guard<std::mutex> prefix_call(prefix_call_mutex_);
        {
            std::lock_guard<std::mutex> lifecycle(
                control_lifecycle_mutex_);
            if (!started_.load(std::memory_order_acquire)) {
                SetFenceOperationError(
                    output,
                    RealtimeCertifiedPrefixFenceOperationErrorV1::
                        kInvalidArgument);
                SetSystemError(system_error_number, EINVAL);
                return false;
            }
            if (!worker_running_.load(std::memory_order_acquire) ||
                worker_stop_requested_.load(
                    std::memory_order_acquire)) {
                SetFenceOperationError(
                    output,
                    RealtimeCertifiedPrefixFenceOperationErrorV1::
                        kWorkerFailed);
                SetSystemError(system_error_number, EPIPE);
                return false;
            }
            if (control_state_.load(std::memory_order_acquire) !=
                RealtimeCertifiedServiceSnapshotV1::ControlState::
                    kNotStarted) {
                SetFenceOperationError(
                    output,
                    RealtimeCertifiedPrefixFenceOperationErrorV1::
                        kInvalidLifecycle);
                SetSystemError(system_error_number, EALREADY);
                return false;
            }
            bool expected_started = false;
            if (!prefix_barrier_started_.compare_exchange_strong(
                    expected_started,
                    true,
                    std::memory_order_acq_rel)) {
                SetFenceOperationError(
                    output,
                    RealtimeCertifiedPrefixFenceOperationErrorV1::
                        kInvalidLifecycle);
                SetSystemError(system_error_number, EALREADY);
                return false;
            }
            auto expected_phase = PrefixCommitPhase::kNotStarted;
            if (!prefix_commit_phase_.compare_exchange_strong(
                    expected_phase,
                    PrefixCommitPhase::kPending,
                    std::memory_order_acq_rel,
                    std::memory_order_acquire)) {
                SetFenceOperationError(
                    output,
                    RealtimeCertifiedPrefixFenceOperationErrorV1::
                        kInvalidLifecycle);
                SetSystemError(system_error_number, EALREADY);
                return false;
            }
            prefix_commit_finished_for_test_.store(
                false, std::memory_order_release);
        }
        std::uint64_t fence_id = 0U;
        if (!AllocatePrefixFenceId(&fence_id)) {
            prefix_commit_phase_.store(
                PrefixCommitPhase::kFailed,
                std::memory_order_release);
            SetFenceOperationError(
                output,
                RealtimeCertifiedPrefixFenceOperationErrorV1::
                    kInternalFailure);
            SetSystemError(system_error_number, EOVERFLOW);
            return false;
        }
        if (output != nullptr) {
            output->fence_id = fence_id;
        }
        HandoffEvent fence{};
        fence.kind = HandoffKind::kPrefixCommit;
        fence.barrier_id = fence_id;
        const auto deadline =
            std::chrono::steady_clock::now() + timeout;
        for (;;) {
            bool became_nonempty = false;
            if (queue_ != nullptr &&
                queue_->TryPush(fence, &became_nonempty)) {
                if (became_nonempty) {
                    WakeWorker();
                }
                break;
            }
            if (!worker_running_.load(std::memory_order_acquire) ||
                worker_stop_requested_.load(
                    std::memory_order_acquire)) {
                auto pending = PrefixCommitPhase::kPending;
                static_cast<void>(prefix_commit_phase_
                                      .compare_exchange_strong(
                                          pending,
                                          PrefixCommitPhase::kWorkerFailed,
                                          std::memory_order_acq_rel,
                                          std::memory_order_acquire));
                SetFenceOperationError(
                    output,
                    RealtimeCertifiedPrefixFenceOperationErrorV1::
                        kWorkerFailed);
                SetSystemError(system_error_number, EPIPE);
                return false;
            }
            if (std::chrono::steady_clock::now() >= deadline) {
                auto pending = PrefixCommitPhase::kPending;
                if (prefix_commit_phase_.compare_exchange_strong(
                        pending,
                        PrefixCommitPhase::kCancelled,
                        std::memory_order_acq_rel,
                        std::memory_order_acquire)) {
                    SetFenceOperationError(
                        output,
                        RealtimeCertifiedPrefixFenceOperationErrorV1::
                            kTimedOut);
                    SetSystemError(system_error_number, ETIMEDOUT);
                    return false;
                }
            }
            std::this_thread::yield();
        }
        for (;;) {
            const PrefixCommitPhase phase =
                prefix_commit_phase_.load(std::memory_order_acquire);
            if (phase == PrefixCommitPhase::kSucceeded ||
                phase == PrefixCommitPhase::kFailed) {
                if (!CopyCompletedPrefixFence(fence_id, output)) {
                    SetFenceOperationError(
                        output,
                        RealtimeCertifiedPrefixFenceOperationErrorV1::
                            kInternalFailure);
                    SetSystemError(system_error_number, EIO);
                    return false;
                }
                if (phase == PrefixCommitPhase::kSucceeded) {
                    return true;
                }
                SetSystemError(
                    system_error_number,
                    output != nullptr &&
                            output->operation_error ==
                                RealtimeCertifiedPrefixFenceOperationErrorV1::
                                    kWorkerFailed
                        ? EPIPE
                        : EIO);
                return false;
            }
            if (phase == PrefixCommitPhase::kCancelled) {
                SetFenceOperationError(
                    output,
                    RealtimeCertifiedPrefixFenceOperationErrorV1::
                        kTimedOut);
                SetSystemError(system_error_number, ETIMEDOUT);
                return false;
            }
            if (phase == PrefixCommitPhase::kWorkerFailed) {
                SetFenceOperationError(
                    output,
                    RealtimeCertifiedPrefixFenceOperationErrorV1::
                        kWorkerFailed);
                SetSystemError(system_error_number, EPIPE);
                return false;
            }
            if ((!worker_running_.load(std::memory_order_acquire) ||
                 worker_stop_requested_.load(
                     std::memory_order_acquire)) &&
                phase == PrefixCommitPhase::kPending) {
                auto pending = PrefixCommitPhase::kPending;
                if (prefix_commit_phase_.compare_exchange_strong(
                        pending,
                        PrefixCommitPhase::kWorkerFailed,
                        std::memory_order_acq_rel,
                        std::memory_order_acquire)) {
                    SetFenceOperationError(
                        output,
                        RealtimeCertifiedPrefixFenceOperationErrorV1::
                            kWorkerFailed);
                    SetSystemError(system_error_number, EPIPE);
                    return false;
                }
                continue;
            }
            if (phase == PrefixCommitPhase::kPending &&
                std::chrono::steady_clock::now() >= deadline) {
                auto pending = PrefixCommitPhase::kPending;
                if (prefix_commit_phase_.compare_exchange_strong(
                        pending,
                        PrefixCommitPhase::kCancelled,
                        std::memory_order_acq_rel,
                        std::memory_order_acquire)) {
                    SetFenceOperationError(
                        output,
                        RealtimeCertifiedPrefixFenceOperationErrorV1::
                            kTimedOut);
                    SetSystemError(system_error_number, ETIMEDOUT);
                    return false;
                }
                // The worker owns kCompleting. It performs only the no-throw
                // metadata commit and release publications, so wait for its
                // terminal result instead of returning a false timeout.
            }
            std::this_thread::yield();
        }
    }

private:
    [[nodiscard]] bool StartClaimedControl(
        int* system_error_number) noexcept {
        control_affinity_system_error_.store(
            0, std::memory_order_relaxed);
        control_startup_state_.store(
            ThreadStartupStateV1::kStarting,
            std::memory_order_release);
        try {
            control_thread_ = std::thread([this]() noexcept {
                int affinity_error = 0;
                if (!ApplyOptionalCurrentThreadCpuSet(
                        control_cpu_set_, &affinity_error)) {
                    control_affinity_system_error_.store(
                        affinity_error, std::memory_order_relaxed);
                    MarkControlFailedIfRunning();
                    control_startup_state_.store(
                        ThreadStartupStateV1::kFailed,
                        std::memory_order_release);
                    control_startup_state_.notify_all();
                    return;
                }
                control_startup_state_.store(
                    ThreadStartupStateV1::kReady,
                    std::memory_order_release);
                control_startup_state_.notify_all();
                ControlLoop();
            });
        } catch (...) {
            control_affinity_system_error_.store(
                EAGAIN, std::memory_order_relaxed);
            auto expected =
                RealtimeCertifiedServiceSnapshotV1::ControlState::
                    kRunning;
            static_cast<void>(control_state_.compare_exchange_strong(
                expected,
                RealtimeCertifiedServiceSnapshotV1::ControlState::
                    kFailed,
                    std::memory_order_acq_rel,
                    std::memory_order_acquire));
            control_startup_state_.store(
                ThreadStartupStateV1::kFailed,
                std::memory_order_release);
            control_startup_state_.notify_all();
        }
        ThreadStartupStateV1 startup =
            control_startup_state_.load(std::memory_order_acquire);
        while (startup == ThreadStartupStateV1::kStarting) {
            control_startup_state_.wait(
                startup, std::memory_order_acquire);
            startup = control_startup_state_.load(
                std::memory_order_acquire);
        }
        if (startup == ThreadStartupStateV1::kReady) {
            return true;
        }
        if (control_thread_.joinable()) {
            control_thread_.join();
        }
        const int affinity_error =
            control_affinity_system_error_.load(
                std::memory_order_relaxed);
        SetSystemError(
            system_error_number,
            affinity_error == 0 ? EINVAL : affinity_error);
        return false;
    }

public:
    [[nodiscard]] bool Start(int* system_error_number) noexcept {
        if (!StartWorker(system_error_number)) {
            return false;
        }
        if (StartControl(system_error_number)) {
            return true;
        }
        StopControl();
        return false;
    }

    [[nodiscard]] bool FinalizeProcessStartCoverage(
        std::uint64_t coverage_start_unix_ns,
        std::chrono::milliseconds timeout,
        int* system_error_number) noexcept {
        SetSystemError(system_error_number, 0);
        if (coverage_start_unix_ns == 0U || timeout.count() <= 0 ||
            timeout > std::chrono::hours(24) ||
            config_.event_temporal_coverage !=
                CertifiedOrderEventTemporalCoverageV1::
                    kFromProcessStart) {
            SetSystemError(system_error_number, EINVAL);
            return false;
        }
        std::lock_guard<std::mutex> call(
            process_start_coverage_call_mutex_);
        if (config_.control_exposure_gate == nullptr ||
            config_.control_exposure_gate->load(
                std::memory_order_acquire)) {
            SetSystemError(system_error_number, EALREADY);
            return false;
        }
        if (!worker_running_.load(std::memory_order_acquire) ||
            worker_stop_requested_.load(std::memory_order_acquire)) {
            SetSystemError(system_error_number, EPIPE);
            return false;
        }
        auto expected = ProcessStartCoveragePhase::kNotStarted;
        if (!process_start_coverage_phase_.compare_exchange_strong(
                expected,
                ProcessStartCoveragePhase::kPending,
                std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            SetSystemError(system_error_number, EALREADY);
            return false;
        }

        HandoffEvent barrier{};
        barrier.kind = HandoffKind::kProcessStartCoverage;
        barrier.barrier_id = coverage_start_unix_ns;
        const auto deadline =
            std::chrono::steady_clock::now() + timeout;
        for (;;) {
            bool became_nonempty = false;
            if (queue_ != nullptr &&
                queue_->TryPush(barrier, &became_nonempty)) {
                if (became_nonempty) {
                    WakeWorker();
                }
                break;
            }
            if (!worker_running_.load(std::memory_order_acquire) ||
                worker_stop_requested_.load(
                    std::memory_order_acquire) ||
                globally_frozen_resource_.load(
                    std::memory_order_acquire)) {
                auto pending = ProcessStartCoveragePhase::kPending;
                static_cast<void>(process_start_coverage_phase_
                                      .compare_exchange_strong(
                                          pending,
                                          ProcessStartCoveragePhase::kFailed,
                                          std::memory_order_acq_rel,
                                          std::memory_order_acquire));
                SetSystemError(system_error_number, EPIPE);
                return false;
            }
            if (std::chrono::steady_clock::now() >= deadline) {
                auto pending = ProcessStartCoveragePhase::kPending;
                if (process_start_coverage_phase_
                        .compare_exchange_strong(
                            pending,
                            ProcessStartCoveragePhase::kCancelled,
                            std::memory_order_acq_rel,
                            std::memory_order_acquire)) {
                    SetSystemError(system_error_number, ETIMEDOUT);
                    return false;
                }
            }
            std::this_thread::yield();
        }

        for (;;) {
            const ProcessStartCoveragePhase phase =
                process_start_coverage_phase_.load(
                    std::memory_order_acquire);
            if (phase == ProcessStartCoveragePhase::kReady) {
                return true;
            }
            if (phase == ProcessStartCoveragePhase::kFailed) {
                SetSystemError(system_error_number, EIO);
                return false;
            }
            if (phase == ProcessStartCoveragePhase::kCancelled) {
                SetSystemError(system_error_number, ETIMEDOUT);
                return false;
            }
            if ((!worker_running_.load(std::memory_order_acquire) ||
                 worker_stop_requested_.load(
                     std::memory_order_acquire) ||
                 globally_frozen_resource_.load(
                     std::memory_order_acquire)) &&
                phase == ProcessStartCoveragePhase::kPending) {
                auto pending = ProcessStartCoveragePhase::kPending;
                static_cast<void>(process_start_coverage_phase_
                                      .compare_exchange_strong(
                                          pending,
                                          ProcessStartCoveragePhase::kFailed,
                                          std::memory_order_acq_rel,
                                          std::memory_order_acquire));
                continue;
            }
            if (phase == ProcessStartCoveragePhase::kPending &&
                std::chrono::steady_clock::now() >= deadline) {
                auto pending = ProcessStartCoveragePhase::kPending;
                static_cast<void>(process_start_coverage_phase_
                                      .compare_exchange_strong(
                                          pending,
                                          ProcessStartCoveragePhase::kCancelled,
                                          std::memory_order_acq_rel,
                                          std::memory_order_acquire));
                continue;
            }
            std::this_thread::yield();
        }
    }

    [[nodiscard]] bool PublishApplied(
        std::size_t ordinal,
        const market::RealtimeHistoryRecordV1& record) noexcept {
        // This ordering is the central FAST availability invariant. No
        // certification work, allocation, lock, or queue access occurs until
        // the existing required FAST sink has succeeded.
        if (!config_.fast_sink->PublishApplied(ordinal, record)) {
            return false;
        }
        // Native continuity is defined only for the three tracked tick
        // messages. Snapshots remain fully available through FAST but must
        // never enter the tick-only CERTIFIED projector.
        if (!market::IsTickEventKindV1(record.kind())) {
            return true;
        }
        if (!accepting_.load(std::memory_order_acquire) ||
            globally_frozen_resource_.load(
                std::memory_order_acquire)) {
            return true;
        }
        HandoffEvent event{};
        event.kind = HandoffKind::kApplied;
        event.ordinal = ordinal;
        event.record = &record;
        bool became_nonempty = false;
        if (!queue_->TryPush(event, &became_nonempty)) {
            FreezeGlobalResource();
            return true;
        }
        if (!IncrementSaturating(&enqueued_applied_records_)) {
            FreezeGlobalResource();
        }
        if (became_nonempty) {
            WakeWorker();
        }
        return true;
    }

    void MarkCoverageLost() noexcept {
        // This method is called by the pre-existing History failure path.
        // Propagating it to FAST preserves the original required-sink
        // contract; native gaps never call this method.
        config_.fast_sink->MarkCoverageLost();
        FreezeGlobalResource();
    }

    void QuiesceRecordReferences() noexcept {
        // HandoffEvent deliberately borrows immutable records from History's
        // append-only Store so the normal producer path need copy only a
        // pointer. History invokes this barrier after all publishers stop and
        // before releasing that Store, including partial pipeline-Create
        // failure. Draining here makes every queued pointer lifetime-safe.
        AbortPendingPrefixCommitForWorkerStop();
        accepting_.store(false, std::memory_order_release);
        seal_input_requested_.store(true, std::memory_order_release);
        worker_stop_requested_.store(true, std::memory_order_release);
        WakeWorker();
        if (worker_thread_.joinable()) {
            worker_thread_.join();
        }
        StopTickHistoryWriter();
        worker_running_.store(false, std::memory_order_release);
    }

    void ObserveNativeSequence(
        const realtime::NativeSequenceObservationV1& observation)
        noexcept {
        if (!accepting_.load(std::memory_order_acquire) ||
            globally_frozen_resource_.load(
                std::memory_order_acquire)) {
            return;
        }
        const bool valid =
            observation.descriptor.sequence != 0U &&
            (observation.descriptor.domain.market ==
                     realtime::NativeSequenceMarketV1::kShenzhen ||
             observation.descriptor.domain.channel != 0U) &&
            (observation.record_class ==
                 realtime::NativeSequenceRecoveryRecordClassV1::
                     kFiltered
                 ? observation.ingress_sequence == 0U
                 : observation.ingress_sequence != 0U);
        if (!valid) {
            FreezeGlobalResource();
            return;
        }
        HandoffEvent event{};
        event.kind = HandoffKind::kObservation;
        event.observation = observation;
        bool became_nonempty = false;
        if (!queue_->TryPush(event, &became_nonempty)) {
            FreezeGlobalResource();
            return;
        }
        if (!IncrementSaturating(&enqueued_observations_)) {
            FreezeGlobalResource();
        }
        if (became_nonempty) {
            WakeWorker();
        }
    }

    void MarkNativeSequenceObservationFailure(
        realtime::NativeSequenceObservationFailureV1,
        const sdk::MessageKey&) noexcept {
        FreezeGlobalResource();
    }

    void MarkDraining() noexcept {
        draining_.store(true, std::memory_order_release);
    }

    void MarkStoppedClean() noexcept {
        AbortPendingPrefixCommitForWorkerStop();
        accepting_.store(false, std::memory_order_release);
        seal_input_requested_.store(true, std::memory_order_release);
        worker_stop_requested_.store(true, std::memory_order_release);
        WakeWorker();
        if (worker_thread_.joinable()) {
            worker_thread_.join();
        }
        StopTickHistoryWriter();
        worker_running_.store(false, std::memory_order_release);
        PublishHeader(RealtimeCertifiedStateV1::kStopped);
    }

    void StopControl() noexcept {
        std::lock_guard<std::mutex> lifecycle(
            control_lifecycle_mutex_);
        // Order shutdown against a final prefix commit without holding this
        // lifecycle mutex while the caller waits. If the worker already owns
        // kCompleting, promotion linearized first and shutdown waits for that
        // no-throw commit; otherwise shutdown wins and promotion is forbidden.
        AbortPendingPrefixCommitForWorkerStop();
        accepting_.store(false, std::memory_order_release);
        worker_stop_requested_.store(true, std::memory_order_release);
        auto control_state =
            control_state_.load(std::memory_order_acquire);
        for (;;) {
            using ControlState =
                RealtimeCertifiedServiceSnapshotV1::ControlState;
            if (control_state != ControlState::kRunning &&
                control_state != ControlState::kNotStarted) {
                break;
            }
            const ControlState desired =
                control_state == ControlState::kRunning
                    ? ControlState::kStopping
                    : ControlState::kStopped;
            if (control_state_.compare_exchange_weak(
                    control_state,
                    desired,
                    std::memory_order_acq_rel,
                    std::memory_order_acquire)) {
                break;
            }
        }
        WakeWorker();
        SignalStopEvent();
        if (worker_thread_.joinable()) {
            worker_thread_.join();
        }
        StopTickHistoryWriter();
        if (control_thread_.joinable()) {
            control_thread_.join();
        }
        auto stopping =
            RealtimeCertifiedServiceSnapshotV1::ControlState::
                kStopping;
        static_cast<void>(control_state_.compare_exchange_strong(
            stopping,
            RealtimeCertifiedServiceSnapshotV1::ControlState::
                kStopped,
            std::memory_order_acq_rel,
            std::memory_order_acquire));
        worker_running_.store(false, std::memory_order_release);
        if (started_.load(std::memory_order_acquire) &&
            header_ != nullptr) {
            PublishHeader(RealtimeCertifiedStateV1::kStopped);
        }
    }

    [[nodiscard]] RealtimeCertifiedServiceSnapshotV1 Snapshot()
        const noexcept {
        RealtimeCertifiedServiceSnapshotV1 result{};
        bool stable_snapshot = header_ == nullptr;
        if (header_ != nullptr) {
            for (std::size_t attempt = 0U; attempt < 5U; ++attempt) {
                const std::uint64_t begin =
                    Atomic(header_->status_publish_tag)
                        .load(std::memory_order_acquire);
                if (begin == 0U || (begin & 1U) != 0U) {
                    continue;
                }
                RealtimeCertifiedServiceSnapshotV1 candidate{};
                candidate.state = static_cast<RealtimeCertifiedStateV1>(
                    Atomic(header_->aggregate_state)
                        .load(std::memory_order_relaxed));
                candidate.canonical_apply_frontier =
                    Atomic(header_->canonical_apply_frontier)
                        .load(std::memory_order_relaxed);
                candidate.correction_epoch =
                    Atomic(header_->correction_epoch)
                        .load(std::memory_order_relaxed);
                candidate.observed_native_message_count =
                    Atomic(header_->observed_native_message_count)
                        .load(std::memory_order_relaxed);
                candidate.certified_tick_count =
                    Atomic(header_->certified_tick_count)
                        .load(std::memory_order_relaxed);
                candidate.exact_duplicate_message_count =
                    Atomic(header_->exact_duplicate_message_count)
                        .load(std::memory_order_relaxed);
                candidate.gap_opened_count =
                    Atomic(header_->gap_opened_count)
                        .load(std::memory_order_relaxed);
                candidate.gap_recovered_count =
                    Atomic(header_->gap_recovered_count)
                        .load(std::memory_order_relaxed);
                candidate.conflicting_duplicate_count =
                    Atomic(header_->conflicting_duplicate_count)
                        .load(std::memory_order_relaxed);
                candidate.resource_exhaustion_count =
                    Atomic(header_->resource_exhaustion_count)
                        .load(std::memory_order_relaxed);
                candidate.pending_token_count =
                    Atomic(header_->pending_token_count)
                        .load(std::memory_order_relaxed);
                candidate.channel_count =
                    Atomic(header_->channel_state_count)
                        .load(std::memory_order_relaxed);
                candidate.gap_open_channel_count =
                    Atomic(header_->gap_open_channel_count)
                        .load(std::memory_order_relaxed);
                candidate.catching_up_channel_count =
                    Atomic(header_->catching_up_channel_count)
                        .load(std::memory_order_relaxed);
                candidate.frozen_channel_count =
                    Atomic(header_->frozen_channel_count)
                        .load(std::memory_order_relaxed);
                std::atomic_thread_fence(std::memory_order_acq_rel);
                const std::uint64_t end =
                    Atomic(header_->status_publish_tag)
                        .load(std::memory_order_acquire);
                if (begin == end && (end & 1U) == 0U) {
                    result = candidate;
                    stable_snapshot = true;
                    break;
                }
            }
        }
        result.enqueued_observations =
            enqueued_observations_.load(std::memory_order_relaxed);
        result.enqueued_applied_records =
            enqueued_applied_records_.load(
                std::memory_order_relaxed);
        result.processed_handoffs =
            processed_handoffs_.load(std::memory_order_relaxed);
        result.dropped_handoffs =
            dropped_handoffs_.load(std::memory_order_relaxed);
        result.worker_running =
            worker_running_.load(std::memory_order_acquire);
        result.globally_frozen_resource =
            globally_frozen_resource_.load(
                std::memory_order_acquire);
        result.tick_history_writer_running =
            tick_history_writer_running_.load(
                std::memory_order_acquire);
        result.tick_history_failed =
            tick_history_failed_.load(std::memory_order_acquire);
        result.tick_history_frontier =
            tick_history_frontier_.load(std::memory_order_acquire);
        result.tick_history_lag =
            result.canonical_apply_frontier >
                    result.tick_history_frontier
                ? result.canonical_apply_frontier -
                      result.tick_history_frontier
                : 0U;
        result.maximum_tick_history_lag =
            maximum_tick_history_lag_.load(
                std::memory_order_relaxed);
        result.wire_snapshot_consistent = stable_snapshot;
        result.control_state =
            control_state_.load(std::memory_order_acquire);
        if (payload_leases_ != nullptr) {
            result.payload_lease_capacity =
                payload_leases_->capacity();
            result.payload_leases_in_use =
                payload_leases_->in_use();
            result.payload_lease_high_water =
                payload_leases_->high_water();
            result.payload_lease_failed_acquires =
                payload_leases_->failed_acquires();
        }
        return result;
    }

    [[nodiscard]] bool ReadLatestForTest(
        std::size_t ordinal,
        RealtimeCertifiedTickEnvelopeV1* output) const noexcept {
        return output != nullptr && header_ != nullptr &&
               ordinal < header_->latest_capacity &&
               CopySlot(latest_[ordinal], output) &&
               output->canonical_apply_sequence <=
                   Atomic(header_->canonical_apply_frontier)
                       .load(std::memory_order_acquire);
    }

    [[nodiscard]] bool ReadCanonicalForTest(
        std::uint64_t sequence,
        RealtimeCertifiedTickEnvelopeV1* output) const noexcept {
        if (output == nullptr || header_ == nullptr ||
            sequence == 0U ||
            sequence >
                Atomic(header_->canonical_apply_frontier)
                    .load(std::memory_order_acquire)) {
            return false;
        }
        std::uint32_t index = 0U;
        return RealtimeCertifiedRingSlotIndexV1(
                   sequence,
                   header_->certified_ring_capacity,
                   &index) &&
               CopySlot(ring_[index], output) &&
               output->canonical_apply_sequence == sequence;
    }

    [[nodiscard]] bool DuplicateReadOnlyDescriptorForTest(
        int* output) const noexcept {
        if (output == nullptr || read_only_fd_ < 0) {
            return false;
        }
        *output = -1;
        int duplicate = -1;
        do {
            duplicate =
                ::fcntl(read_only_fd_, F_DUPFD_CLOEXEC, 0);
        } while (duplicate < 0 && errno == EINTR);
        if (duplicate < 0) {
            return false;
        }
        *output = duplicate;
        return true;
    }

    [[nodiscard]] std::uint64_t HeaderPublishTagForTest()
        const noexcept {
        return header_ == nullptr
                   ? 0U
                   : Atomic(header_->status_publish_tag)
                         .load(std::memory_order_acquire);
    }

    [[nodiscard]] std::uint64_t WorkerWakeEpochForTest()
        const noexcept {
        return wake_epoch_.load(std::memory_order_acquire);
    }

    [[nodiscard]] bool WaitUntilIdle(
        std::chrono::milliseconds timeout) const noexcept {
        if (timeout.count() < 0) {
            return false;
        }
        const auto deadline =
            std::chrono::steady_clock::now() + timeout;
        for (;;) {
            const std::uint64_t expected =
                enqueued_observations_.load(
                    std::memory_order_acquire) +
                enqueued_applied_records_.load(
                    std::memory_order_acquire);
            if (processed_handoffs_.load(
                    std::memory_order_acquire) >= expected &&
                (queue_ == nullptr || queue_->Empty()) &&
                (tick_history_failed_.load(
                     std::memory_order_acquire) ||
                 header_ == nullptr ||
                 tick_history_frontier_.load(
                     std::memory_order_acquire) >=
                     Atomic(header_->canonical_apply_frontier)
                         .load(std::memory_order_acquire))) {
                return true;
            }
            if (std::chrono::steady_clock::now() >= deadline) {
                return false;
            }
            std::this_thread::yield();
        }
    }

    [[nodiscard]] CertifiedOrderEventHistoryErrorV1
    AcquireEventGeneration(
        CertifiedOrderEventHistorySnapshotV1* output) const noexcept {
        if (output == nullptr) {
            return CertifiedOrderEventHistoryErrorV1::kNullOutput;
        }
        *output = {};
        // Avoid an artificial one-call lag in the narrow interval after the
        // Tick header commits and before the worker refreshes the cached
        // mirror. A current history generation is safe exactly when a stable
        // public Tick snapshot has already reached its input frontier.
        if (event_history_ != nullptr) {
            CertifiedOrderEventHistorySnapshotV1 candidate{};
            const RealtimeCertifiedServiceSnapshotV1 tick =
                Snapshot();
            if (tick.wire_snapshot_consistent &&
                event_history_->AcquireGeneration(&candidate) ==
                    CertifiedOrderEventHistoryErrorV1::kNone &&
                candidate.valid() &&
                candidate.generation()
                        .input_frontier
                        .canonical_apply_sequence <=
                    tick.canonical_apply_frontier) {
                *output = std::move(candidate);
                return CertifiedOrderEventHistoryErrorV1::kNone;
            }
        }
        const std::lock_guard<std::mutex> lock(
            committed_event_generation_mutex_);
        *output = committed_event_generation_;
        return output->valid()
                   ? CertifiedOrderEventHistoryErrorV1::kNone
                   : CertifiedOrderEventHistoryErrorV1::kFailed;
    }

    [[nodiscard]] std::uint64_t mapping_bytes() const noexcept {
        return mapping_bytes_;
    }

    [[nodiscard]] std::uint64_t handoff_queue_capacity()
        const noexcept {
        return config_.handoff_queue_capacity;
    }

    [[nodiscard]] const std::filesystem::path& socket_path()
        const noexcept {
        return config_.control_socket_path;
    }

private:
    enum class TickHistoryDrainResult : std::uint8_t {
        kOk = 0U,
        kJournalFailed,
        kSourceRetentionLost,
        kSourceReadFailed,
    };

    void UpdateMaximumTickHistoryLag(std::uint64_t lag) noexcept {
        std::uint64_t maximum =
            maximum_tick_history_lag_.load(std::memory_order_relaxed);
        while (maximum < lag &&
               !maximum_tick_history_lag_.compare_exchange_weak(
                   maximum,
                   lag,
                   std::memory_order_relaxed,
                   std::memory_order_relaxed)) {
        }
    }

    [[nodiscard]] TickHistoryDrainResult DrainTickHistoryThrough(
        std::uint64_t requested_frontier) noexcept {
        if (tick_journal_ == nullptr || header_ == nullptr ||
            ring_ == nullptr) {
            tick_history_failed_.store(
                true, std::memory_order_release);
            return TickHistoryDrainResult::kJournalFailed;
        }
        const std::lock_guard<std::mutex> writer(
            tick_history_writer_mutex_);
        CertifiedTickJournalStatusV1 status =
            tick_journal_->status();
        if (status.state == CertifiedTickJournalStateV1::kFailed) {
            tick_history_failed_.store(
                true, std::memory_order_release);
            return TickHistoryDrainResult::kJournalFailed;
        }
        if (status.state == CertifiedTickJournalStateV1::kComplete) {
            const bool covered =
                status.canonical_apply_frontier >= requested_frontier;
            if (!covered) {
                tick_history_failed_.store(
                    true, std::memory_order_release);
            }
            return covered ? TickHistoryDrainResult::kOk
                           : TickHistoryDrainResult::kJournalFailed;
        }

        // Amortize journal status publication and lazy backing commits while
        // catching up a recovery prefix.  This writer consumes only already
        // published bounded-CERTIFIED slots on its isolated thread, so the
        // larger cold-path batch does not add work to FAST or bounded
        // CERTIFIED publication.  Keep the buffer small enough for the
        // default pthread stack and for prompt stop/failure observation.
        constexpr std::size_t batch_capacity = 1024U;
        std::array<RealtimeCertifiedTickEnvelopeV1, batch_capacity>
            batch{};
        std::uint64_t next = status.canonical_apply_frontier + 1U;
        while (next <= requested_frontier) {
            const std::uint64_t visible =
                Atomic(header_->canonical_apply_frontier)
                    .load(std::memory_order_acquire);
            const std::uint64_t ring_capacity =
                header_->certified_ring_capacity;
            const std::uint64_t oldest =
                visible > ring_capacity
                    ? visible - ring_capacity + 1U
                    : 1U;
            if (next < oldest) {
                static_cast<void>(
                    tick_journal_->MarkHistoryFailed(
                        CertifiedTickJournalAppendErrorV1::
                            kSourceRetentionLost));
                tick_history_failed_.store(
                    true, std::memory_order_release);
                return TickHistoryDrainResult::kSourceRetentionLost;
            }
            const std::uint64_t remaining =
                requested_frontier - next + 1U;
            const std::uint64_t count64 = std::min(
                remaining,
                static_cast<std::uint64_t>(batch.size()));
            const std::size_t count =
                static_cast<std::size_t>(count64);
            for (std::size_t offset = 0U; offset < count; ++offset) {
                const std::uint64_t sequence =
                    next + static_cast<std::uint64_t>(offset);
                std::uint32_t ring_index = 0U;
                const bool index_valid =
                    RealtimeCertifiedRingSlotIndexV1(
                    sequence,
                    header_->certified_ring_capacity,
                    &ring_index);
                bool copied = false;
                // At the exact retention edge the sole writer may already
                // have made the slot odd for its replacement while the
                // public header still describes the old frontier. Retry
                // through that short seqcount window; after the new header
                // commit the check below classifies it as retention loss.
                bool retention_lost = false;
                while (index_valid) {
                    if (CopySlot(
                            ring_[ring_index], &batch[offset]) &&
                        batch[offset].canonical_apply_sequence ==
                            sequence) {
                        copied = true;
                        break;
                    }
                    const std::uint64_t after =
                        Atomic(header_->canonical_apply_frontier)
                            .load(std::memory_order_acquire);
                    const std::uint64_t after_oldest =
                        after > ring_capacity
                            ? after - ring_capacity + 1U
                            : 1U;
                    if (sequence < after_oldest) {
                        retention_lost = true;
                        break;
                    }
                    if (!worker_running_.load(
                            std::memory_order_acquire) ||
                        worker_stop_requested_.load(
                            std::memory_order_acquire) ||
                        globally_frozen_resource_.load(
                            std::memory_order_acquire)) {
                        break;
                    }
                    std::this_thread::yield();
                }
                if (!copied) {
                    const auto failure =
                        retention_lost
                            ? CertifiedTickJournalAppendErrorV1::
                                  kSourceRetentionLost
                            : CertifiedTickJournalAppendErrorV1::
                                  kSourceReadFailed;
                    static_cast<void>(
                        tick_journal_->MarkHistoryFailed(failure));
                    tick_history_failed_.store(
                        true, std::memory_order_release);
                    return retention_lost
                               ? TickHistoryDrainResult::
                                     kSourceRetentionLost
                               : TickHistoryDrainResult::
                                     kSourceReadFailed;
                }
            }
            const auto append = tick_journal_->Append(
                std::span<const RealtimeCertifiedTickEnvelopeV1>(
                    batch.data(), count));
            if (append != CertifiedTickJournalAppendErrorV1::kNone) {
                tick_history_failed_.store(
                    true, std::memory_order_release);
                return TickHistoryDrainResult::kJournalFailed;
            }
            next += count64;
            tick_history_frontier_.store(
                next - 1U, std::memory_order_release);
        }
        const std::uint64_t visible =
            Atomic(header_->canonical_apply_frontier)
                .load(std::memory_order_acquire);
        const std::uint64_t frontier =
            tick_history_frontier_.load(std::memory_order_acquire);
        const std::uint64_t lag =
            visible > frontier ? visible - frontier : 0U;
        UpdateMaximumTickHistoryLag(lag);
        return TickHistoryDrainResult::kOk;
    }

    void FinalizeTickHistoryWriter() noexcept {
        if (tick_journal_ == nullptr || header_ == nullptr) {
            tick_history_failed_.store(
                true, std::memory_order_release);
            return;
        }
        const std::uint64_t final_frontier =
            Atomic(header_->canonical_apply_frontier)
                .load(std::memory_order_acquire);
        const TickHistoryDrainResult drain =
            DrainTickHistoryThrough(final_frontier);
        const auto state = static_cast<RealtimeCertifiedStateV1>(
            Atomic(header_->aggregate_state)
                .load(std::memory_order_acquire));
        const bool complete_state =
            state == RealtimeCertifiedStateV1::kNoData ||
            state == RealtimeCertifiedStateV1::kContiguous;
        const bool incomplete_native_prefix =
            state == RealtimeCertifiedStateV1::kGapOpen ||
            state == RealtimeCertifiedStateV1::kCatchingUp ||
            state == RealtimeCertifiedStateV1::kDegraded;
        const bool complete =
            drain == TickHistoryDrainResult::kOk && complete_state &&
            tick_history_frontier_.load(std::memory_order_acquire) ==
                final_frontier &&
            !globally_frozen_resource_.load(
                std::memory_order_acquire);
        const std::lock_guard<std::mutex> writer(
            tick_history_writer_mutex_);
        if (complete) {
            if (!tick_journal_->Stop()) {
                tick_history_failed_.store(
                    true, std::memory_order_release);
            }
            return;
        }
        if (drain == TickHistoryDrainResult::kOk &&
            incomplete_native_prefix) {
            static_cast<void>(tick_journal_->MarkHistoryFailed(
                CertifiedTickJournalAppendErrorV1::
                    kIncompleteNativePrefix));
        } else if (drain == TickHistoryDrainResult::kOk) {
            static_cast<void>(
                tick_journal_->MarkUpstreamFailed());
        }
        tick_history_failed_.store(
            true, std::memory_order_release);
    }

    void TickHistoryWriterLoop() noexcept {
        try {
            for (;;) {
                if (tick_history_paused_for_test_.load(
                        std::memory_order_acquire)) {
                    tick_history_pause_reached_for_test_.store(
                        true, std::memory_order_release);
                    while (tick_history_paused_for_test_.load(
                               std::memory_order_acquire) &&
                           !tick_history_stop_requested_.load(
                               std::memory_order_acquire)) {
                        std::this_thread::yield();
                    }
                }
                const std::uint64_t visible =
                    Atomic(header_->canonical_apply_frontier)
                        .load(std::memory_order_acquire);
                const std::uint64_t frontier =
                    tick_history_frontier_.load(
                        std::memory_order_acquire);
                const std::uint64_t lag =
                    visible > frontier ? visible - frontier : 0U;
                UpdateMaximumTickHistoryLag(lag);
                if (DrainTickHistoryThrough(visible) !=
                    TickHistoryDrainResult::kOk) {
                    return;
                }
                const auto state =
                    static_cast<RealtimeCertifiedStateV1>(
                        Atomic(header_->aggregate_state)
                            .load(std::memory_order_acquire));
                if (tick_history_stop_requested_.load(
                        std::memory_order_acquire) ||
                    state ==
                        RealtimeCertifiedStateV1::kFrozenConflict ||
                    state ==
                        RealtimeCertifiedStateV1::kFrozenResource) {
                    FinalizeTickHistoryWriter();
                    return;
                }
                if (visible == frontier) {
                    std::this_thread::sleep_for(
                        std::chrono::microseconds(50));
                }
            }
        } catch (...) {
            const std::lock_guard<std::mutex> writer(
                tick_history_writer_mutex_);
            if (tick_journal_ != nullptr) {
                static_cast<void>(
                    tick_journal_->MarkHistoryFailed(
                        CertifiedTickJournalAppendErrorV1::
                            kSourceReadFailed));
            }
            tick_history_failed_.store(
                true, std::memory_order_release);
        }
    }

    void StopTickHistoryWriter() noexcept {
        tick_history_stop_requested_.store(
            true, std::memory_order_release);
        if (tick_history_thread_.joinable()) {
            tick_history_thread_.join();
        }
        tick_history_writer_running_.store(
            false, std::memory_order_release);
    }

    void AbortPendingPrefixCommitForWorkerStop() noexcept {
        auto pending = PrefixCommitPhase::kPending;
        if (prefix_commit_phase_.compare_exchange_strong(
                pending,
                PrefixCommitPhase::kWorkerFailed,
                std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            prefix_commit_phase_.notify_all();
        }
    }

    static void SetFenceOperationError(
        RealtimeCertifiedPrefixFenceResultV1* output,
        RealtimeCertifiedPrefixFenceOperationErrorV1 error) noexcept {
        if (output != nullptr) {
            output->operation_error = error;
        }
    }

    [[nodiscard]] bool AllocatePrefixFenceId(
        std::uint64_t* output) noexcept {
        if (output == nullptr) {
            return false;
        }
        std::uint64_t previous =
            next_prefix_fence_id_.load(std::memory_order_acquire);
        for (;;) {
            if (previous ==
                std::numeric_limits<std::uint64_t>::max()) {
                return false;
            }
            if (next_prefix_fence_id_.compare_exchange_weak(
                    previous,
                    previous + 1U,
                    std::memory_order_acq_rel,
                    std::memory_order_acquire)) {
                *output = previous + 1U;
                return true;
            }
        }
    }

    void ApplyGlobalResourceFreezeToFence(
        RealtimeCertifiedPrefixFenceResultV1* result) const noexcept {
        if (result != nullptr &&
            globally_frozen_resource_.load(
                std::memory_order_acquire)) {
            // Global handoff/resource loss is monotonic and is not represented
            // by a channel row. It must dominate an older healthy header cut,
            // including the PublishHeader-to-capture race window.
            result->state =
                RealtimeCertifiedStateV1::kFrozenResource;
        }
    }

    [[nodiscard]] RealtimeCertifiedPrefixFenceResultV1
    CapturePrefixFenceResult(
        std::uint64_t fence_id,
        bool header_committed) noexcept {
        RealtimeCertifiedPrefixFenceResultV1 result{};
        result.fence_id = fence_id;
        if (!header_committed || header_ == nullptr ||
            tick_journal_ == nullptr || event_journal_ == nullptr) {
            result.operation_error =
                RealtimeCertifiedPrefixFenceOperationErrorV1::
                    kInternalFailure;
            result.state = RealtimeCertifiedStateV1::kFrozenResource;
            ApplyGlobalResourceFreezeToFence(&result);
            return result;
        }
        result.header_publish_tag =
            Atomic(header_->status_publish_tag)
                .load(std::memory_order_acquire);
        result.state = static_cast<RealtimeCertifiedStateV1>(
            Atomic(header_->aggregate_state)
                .load(std::memory_order_acquire));
        result.canonical_apply_frontier =
            Atomic(header_->canonical_apply_frontier)
                .load(std::memory_order_acquire);
        result.correction_epoch =
            Atomic(header_->correction_epoch)
                .load(std::memory_order_acquire);
        result.gap_open_channel_count =
            Atomic(header_->gap_open_channel_count)
                .load(std::memory_order_acquire);
        result.catching_up_channel_count =
            Atomic(header_->catching_up_channel_count)
                .load(std::memory_order_acquire);
        result.frozen_channel_count =
            Atomic(header_->frozen_channel_count)
                .load(std::memory_order_acquire);
        {
            const std::lock_guard<std::mutex> lock(
                committed_event_generation_mutex_);
            result.event_history = committed_event_generation_;
        }
        const CertifiedTickJournalStatusV1 tick_history_status =
            tick_journal_->status();
        result.tick_journal_frontier =
            tick_history_status.canonical_apply_frontier;
        result.event_journal_frontier =
            event_journal_->canonical_apply_frontier();
        result.event_published_sequence =
            event_journal_->published_event_sequence();

        const CertifiedOrderEventHistoryGenerationV1 generation =
            result.event_history.generation();
        const bool state_valid =
            result.state == RealtimeCertifiedStateV1::kNoData ||
            result.state == RealtimeCertifiedStateV1::kContiguous ||
            result.state == RealtimeCertifiedStateV1::kGapOpen ||
            result.state == RealtimeCertifiedStateV1::kCatchingUp ||
            result.state == RealtimeCertifiedStateV1::kFrozenConflict ||
            result.state == RealtimeCertifiedStateV1::kFrozenResource ||
            result.state == RealtimeCertifiedStateV1::kDegraded;
        const bool event_count_valid =
            generation.derived_event_sequence_exclusive != 0U &&
            generation.event_count <=
                std::numeric_limits<std::uint64_t>::max() &&
            generation.derived_event_sequence_exclusive - 1U ==
                static_cast<std::uint64_t>(generation.event_count);
        const bool terminal_state =
            result.state ==
                RealtimeCertifiedStateV1::kFrozenConflict ||
            result.state ==
                RealtimeCertifiedStateV1::kFrozenResource ||
            globally_frozen_resource_.load(
                std::memory_order_acquire);
        if (result.header_publish_tag == 0U ||
            (result.header_publish_tag & 1U) != 0U ||
            !state_valid || !result.event_history.valid() ||
            generation.input_frontier.canonical_apply_sequence !=
                result.canonical_apply_frontier ||
            (!terminal_state &&
             (result.tick_journal_frontier !=
                  result.canonical_apply_frontier ||
              tick_history_status.state !=
                  CertifiedTickJournalStateV1::kActive)) ||
            result.event_journal_frontier !=
                result.canonical_apply_frontier ||
            !event_count_valid ||
            result.event_published_sequence !=
                static_cast<std::uint64_t>(generation.event_count)) {
            result.operation_error =
                RealtimeCertifiedPrefixFenceOperationErrorV1::
                    kInternalFailure;
            result.state = RealtimeCertifiedStateV1::kFrozenResource;
        }
        ApplyGlobalResourceFreezeToFence(&result);
        return result;
    }

    void PublishCompletedPrefixFence(
        RealtimeCertifiedPrefixFenceResultV1 result) noexcept {
        const std::uint64_t fence_id = result.fence_id;
        {
            const std::lock_guard<std::mutex> lock(
                prefix_fence_result_mutex_);
            completed_prefix_fence_result_ = std::move(result);
        }
        completed_prefix_fence_id_.store(
            fence_id, std::memory_order_release);
        completed_prefix_fence_id_.notify_all();
    }

    [[nodiscard]] bool CopyCompletedPrefixFence(
        std::uint64_t fence_id,
        RealtimeCertifiedPrefixFenceResultV1* output) const noexcept {
        if (output == nullptr) {
            return true;
        }
        const std::lock_guard<std::mutex> lock(
            prefix_fence_result_mutex_);
        if (completed_prefix_fence_result_.fence_id != fence_id) {
            return false;
        }
        *output = completed_prefix_fence_result_;
        return true;
    }

    void WakeWorker() noexcept {
        wake_epoch_.fetch_add(1U, std::memory_order_release);
        wake_epoch_.notify_one();
    }

    void FreezeGlobalResource() noexcept {
        // Publish the nonzero reason before the monotonic freeze flag. A
        // header writer that observes the flag with acquire semantics can
        // therefore never publish FROZEN_RESOURCE with a zero reason count.
        std::uint64_t expected_count = 0U;
        static_cast<void>(
            resource_exhaustion_count_.compare_exchange_strong(
                expected_count,
                1U,
                std::memory_order_acq_rel,
                std::memory_order_acquire));
        globally_frozen_resource_.store(
            true, std::memory_order_release);
        static_cast<void>(
            IncrementSaturating(&dropped_handoffs_));
        WakeWorker();
    }

    void WorkerLoop() noexcept {
        try {
            for (;;) {
                // Sample before doing work. A state-only producer wake which
                // races this iteration then changes the value and makes the
                // final atomic wait return immediately; queue emptiness alone
                // is not enough to protect those notifications.
                const std::uint64_t iteration_wake_epoch =
                    wake_epoch_.load(std::memory_order_acquire);
                std::size_t batch = 0U;
                bool seal_state_changed = false;
                HandoffEvent completed_fence{};
                HandoffEvent event{};
                while (batch < 1024U && queue_->TryPop(&event)) {
                    if (event.kind == HandoffKind::kPrefixProbe ||
                        event.kind == HandoffKind::kPrefixCommit) {
                        completed_fence = event;
                        ++batch;
                        // A fence is an exact FIFO prefix boundary. Do not
                        // pop any later live handoff into the same header
                        // publication that acknowledges this fence.
                        break;
                    }
                    if (event.kind ==
                        HandoffKind::kProcessStartCoverage) {
                        if (!globally_frozen_resource_.load(
                                std::memory_order_acquire)) {
                            HandleHandoff(event);
                        }
                        ++batch;
                        // Finalize one exact pre-exposure FIFO cut before
                        // processing callbacks admitted after the boundary.
                        break;
                    }
                    if (!globally_frozen_resource_.load(
                            std::memory_order_acquire)) {
                        HandleHandoff(event);
                    }
                    if (!IncrementSaturating(
                            &processed_handoffs_,
                            std::memory_order_release)) {
                        FreezeGlobalResource();
                    }
                    ++batch;
                }
                if (!globally_frozen_resource_.load(
                        std::memory_order_acquire) &&
                    !input_sealed_by_worker_ &&
                    seal_input_requested_.load(
                        std::memory_order_acquire) &&
                    queue_->Empty()) {
                    seal_state_changed = true;
                    const auto seal_error = recovery_->SealInput();
                    if (seal_error !=
                        realtime::NativeSequenceRecoverySealErrorV1::
                            kNone) {
                        FreezeGlobalResource();
                    } else {
                        input_sealed_by_worker_ = true;
                        for (auto& [domain, runtime] : channels_) {
                            static_cast<void>(runtime);
                            UpdateChannel(domain);
                        }
                    }
                }
                DrainCertifiedResult drain{};
                if (!globally_frozen_resource_.load(
                        std::memory_order_acquire)) {
                    drain = DrainCertified();
                }
                const RealtimeCertifiedStateV1 barrier_state =
                    DeriveAggregateState();
                const bool header_has_current_state =
                    Atomic(header_->aggregate_state)
                            .load(std::memory_order_acquire) ==
                        static_cast<std::uint32_t>(barrier_state);
                const bool drain_committed_current_state =
                    completed_fence.barrier_id == 0U &&
                    drain.made_progress &&
                    drain.published_header &&
                    !drain.state_dirty_after_last_publish &&
                    header_has_current_state;
                const bool outer_header_required =
                    completed_fence.barrier_id != 0U || batch != 0U ||
                    seal_state_changed ||
                    drain.state_dirty_after_last_publish ||
                    !dirty_channels_.empty() ||
                    !header_has_current_state;
                const bool header_committed =
                    drain_committed_current_state ||
                    !outer_header_required ||
                    PublishHeader(barrier_state);
                if (completed_fence.barrier_id != 0U) {
                    HandleCompletedFence(
                        completed_fence,
                        barrier_state,
                        header_committed);
                }
                if (worker_stop_requested_.load(
                        std::memory_order_acquire) &&
                    queue_->Empty()) {
                    break;
                }
                if (batch == 0U && header_committed) {
                    if (!worker_stop_requested_.load(
                            std::memory_order_acquire) &&
                        queue_->Empty()) {
                        wake_epoch_.wait(
                            iteration_wake_epoch,
                            std::memory_order_acquire);
                    }
                }
            }
        } catch (...) {
            FreezeGlobalResource();
            PublishHeader(
                RealtimeCertifiedStateV1::kFrozenResource);
        }
        auto pending = ProcessStartCoveragePhase::kPending;
        static_cast<void>(process_start_coverage_phase_
                              .compare_exchange_strong(
                                  pending,
                                  ProcessStartCoveragePhase::kFailed,
                                  std::memory_order_acq_rel,
                                  std::memory_order_acquire));
        auto completing = ProcessStartCoveragePhase::kCompleting;
        static_cast<void>(process_start_coverage_phase_
                              .compare_exchange_strong(
                                  completing,
                                  ProcessStartCoveragePhase::kFailed,
                                  std::memory_order_acq_rel,
                                  std::memory_order_acquire));
        process_start_coverage_phase_.notify_all();
        worker_running_.store(false, std::memory_order_release);
    }

    void HandleCompletedFence(
        const HandoffEvent& completed_fence,
        RealtimeCertifiedStateV1 barrier_state,
        bool header_committed) {
        if (completed_fence.kind == HandoffKind::kPrefixProbe) {
            prefix_probe_reached_for_test_.store(
                true, std::memory_order_release);
            while (prefix_probe_paused_for_test_.load(
                       std::memory_order_acquire) &&
                   !worker_stop_requested_.load(
                       std::memory_order_acquire)) {
                std::this_thread::yield();
            }
        } else if (completed_fence.kind == HandoffKind::kPrefixCommit) {
            prefix_commit_reached_for_test_.store(
                true, std::memory_order_release);
            while (prefix_commit_paused_for_test_.load(
                       std::memory_order_acquire) &&
                   !worker_stop_requested_.load(
                       std::memory_order_acquire)) {
                std::this_thread::yield();
            }
        }
        // A prefix fence is a cold control operation. Let the independent
        // writer catch the exact public Tick cut here so temporary
        // asynchronous lag cannot make an otherwise valid recovery
        // probe/promotion fail. Ordinary Tick publication never takes this
        // mutex or waits for History.
        const std::uint64_t fence_frontier =
            Atomic(header_->canonical_apply_frontier)
                .load(std::memory_order_acquire);
        const bool terminal_barrier_state =
            barrier_state == RealtimeCertifiedStateV1::kFrozenConflict ||
            barrier_state == RealtimeCertifiedStateV1::kFrozenResource ||
            globally_frozen_resource_.load(std::memory_order_acquire);
        const bool tick_history_committed =
            header_committed &&
            (terminal_barrier_state ||
             DrainTickHistoryThrough(fence_frontier) ==
                 TickHistoryDrainResult::kOk);
        RealtimeCertifiedPrefixFenceResultV1 result =
            CapturePrefixFenceResult(
                completed_fence.barrier_id,
                tick_history_committed);
        if (completed_fence.kind == HandoffKind::kPrefixProbe) {
            // Probe completion deliberately has no coverage or
            // control-lifecycle side effect.
            PublishCompletedPrefixFence(std::move(result));
            return;
        }

        // Acquire every potentially-throwing resource before claiming
        // irreversible commit ownership. Once the Pending->Completing CAS
        // wins, only noexcept Event metadata and atomic publications remain.
        std::unique_lock<std::mutex> result_lock(
            prefix_fence_result_mutex_);
        // Capture already checks the monotonic global freeze, but it can race
        // immediately after capture. Recheck at the final Pending->Completing
        // linearization point so an already-published resource loss can never
        // be promoted as recovered coverage.
        ApplyGlobalResourceFreezeToFence(&result);
        if (worker_stop_requested_.load(std::memory_order_acquire)) {
            result.operation_error =
                RealtimeCertifiedPrefixFenceOperationErrorV1::
                    kWorkerFailed;
            result.state = RealtimeCertifiedStateV1::kFrozenResource;
        }
        auto pending = PrefixCommitPhase::kPending;
        if (prefix_commit_phase_.compare_exchange_strong(
                pending,
                PrefixCommitPhase::kCompleting,
                std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            const bool coverage_published =
                result.ready() &&
                event_journal_->MarkStartupPrefixRecovered();
            if (result.ready() && !coverage_published) {
                result.operation_error =
                    RealtimeCertifiedPrefixFenceOperationErrorV1::
                        kInternalFailure;
                result.state =
                    RealtimeCertifiedStateV1::kFrozenResource;
            }
            completed_prefix_fence_result_ = std::move(result);
            result_lock.unlock();
            completed_prefix_fence_id_.store(
                completed_fence.barrier_id,
                std::memory_order_release);
            completed_prefix_fence_id_.notify_all();
            if (coverage_published) {
                prefix_barrier_completed_.store(
                    true, std::memory_order_release);
                prefix_commit_phase_.store(
                    PrefixCommitPhase::kSucceeded,
                    std::memory_order_release);
            } else {
                prefix_commit_phase_.store(
                    PrefixCommitPhase::kFailed,
                    std::memory_order_release);
            }
            prefix_commit_phase_.notify_all();
        } else {
            result_lock.unlock();
        }
        prefix_commit_finished_for_test_.store(
            true, std::memory_order_release);
        prefix_commit_finished_for_test_.notify_all();
        // If timeout cancellation won Pending->Cancelled, the worker must not
        // publish recovered coverage later.
    }

    void HandleHandoff(const HandoffEvent& event) {
        if (event.kind == HandoffKind::kProcessStartCoverage) {
            auto pending = ProcessStartCoveragePhase::kPending;
            if (!process_start_coverage_phase_.compare_exchange_strong(
                    pending,
                    ProcessStartCoveragePhase::kCompleting,
                    std::memory_order_acq_rel,
                    std::memory_order_acquire)) {
                return;
            }
            const bool finalized =
                event_journal_ != nullptr &&
                event_journal_->FinalizeProcessStartCoverage(
                    event.barrier_id);
            process_start_coverage_phase_.store(
                finalized ? ProcessStartCoveragePhase::kReady
                          : ProcessStartCoveragePhase::kFailed,
                std::memory_order_release);
            process_start_coverage_phase_.notify_all();
            if (!finalized) {
                FreezeGlobalResource();
            }
            return;
        }
        if (event.kind == HandoffKind::kObservation) {
            HandleObservation(event.observation);
            return;
        }
        if (event.kind == HandoffKind::kApplied) {
            HandleApplied(event.ordinal, event.record);
            return;
        }
        FreezeGlobalResource();
    }

    void HandleObservation(
        const realtime::NativeSequenceObservationV1& observation) {
        if (!IncrementSaturating(
                &observed_native_message_count_)) {
            FreezeGlobalResource();
            return;
        }
        realtime::NativeSequenceRecoveryObserveResultV1 result{};
        const realtime::NativeSequenceRecoveryObserveErrorV1 error =
            recovery_->Observe(
                observation.descriptor,
                observation.message_key,
                observation.record_class,
                &result);
        if (error !=
            realtime::NativeSequenceRecoveryObserveErrorV1::kNone) {
            FreezeGlobalResource();
            return;
        }
        if (result.disposition ==
            realtime::NativeSequenceRecoveryObserveDispositionV1::
                kChannelCapacity) {
            unrepresented_resource_failure_ = true;
            // Without a row/coordinator slot this channel cannot be isolated
            // or described to readers. Freeze the aggregate sidecar at its
            // last-good prefix; FAST has already remained independent.
            FreezeGlobalResource();
            return;
        }
        EnsureChannel(observation.descriptor.domain);
        UpdateChannel(observation.descriptor.domain);
    }

    void HandleApplied(
        std::size_t ordinal,
        const market::RealtimeHistoryRecordV1* record) {
        if (record == nullptr) {
            FreezeGlobalResource();
            return;
        }
        std::uint64_t payload_lease_cookie = 0U;
        RealtimeWireTickPayloadV2* leased_payload = nullptr;
        if (payload_leases_ == nullptr ||
            !payload_leases_->Acquire(
                &leased_payload, &payload_lease_cookie) ||
            leased_payload == nullptr || payload_lease_cookie == 0U) {
            FreezeGlobalResource();
            return;
        }
        if (!ProjectRealtimeWireTickPayloadV2(
                *record, ordinal, leased_payload) ||
            !RealtimeCertifiedTickPayloadCanonicalV1(
                *leased_payload) ||
            leased_payload->common.ingress_sequence == 0U) {
            static_cast<void>(
                payload_leases_->ReleaseIfLive(
                    payload_lease_cookie));
            FreezeGlobalResource();
            return;
        }
        const realtime::NativeSequenceDescriptorV1 descriptor =
            DescriptorFromPayload(*leased_payload);
        const sdk::MessageKey message_key =
            MessageKeyFromPayload(*leased_payload);
        const RealtimeWireTickPayloadV2 canonical =
            CanonicalBusinessPayload(*leased_payload);
        realtime::NativeSequenceRecoveryApplyResultV1 result{};
        const realtime::NativeSequenceRecoveryApplyErrorV1 error =
            recovery_->MarkTargetApplied(
                descriptor,
                message_key,
                std::as_bytes(std::span(&canonical, 1U)),
                payload_lease_cookie,
                &result);
        if (error !=
                realtime::NativeSequenceRecoveryApplyErrorV1::kNone &&
            error !=
                realtime::NativeSequenceRecoveryApplyErrorV1::
                    kChannelFrozen) {
            static_cast<void>(
                payload_leases_->ReleaseIfLive(
                    payload_lease_cookie));
            FreezeGlobalResource();
            return;
        }
        const bool lease_became_canonical =
            error ==
                realtime::NativeSequenceRecoveryApplyErrorV1::kNone &&
            result.disposition ==
                realtime::NativeSequenceRecoveryApplyDispositionV1::
                    kApplied &&
            result.canonical_applied_cookie == payload_lease_cookie;
        if (!lease_became_canonical &&
            !payload_leases_->ReleaseIfLive(
                payload_lease_cookie)) {
            FreezeGlobalResource();
            return;
        }
        if (error ==
                realtime::NativeSequenceRecoveryApplyErrorV1::kNone &&
            result.disposition ==
                realtime::NativeSequenceRecoveryApplyDispositionV1::
                    kApplied &&
            !lease_became_canonical) {
            FreezeGlobalResource();
            return;
        }
        if (result.disposition ==
            realtime::NativeSequenceRecoveryApplyDispositionV1::
                kChannelCapacity) {
            unrepresented_resource_failure_ = true;
            FreezeGlobalResource();
            return;
        }
        EnsureChannel(descriptor.domain);
        UpdateChannel(descriptor.domain);
    }

    [[nodiscard]] DrainCertifiedResult DrainCertified() {
        DrainCertifiedResult result{};
        for (;;) {
            realtime::NativeSequenceCertifiedReadyV1 ready{};
            const realtime::NativeSequenceRecoveryPollErrorV1 error =
                recovery_->PollCertified(&ready);
            if (error ==
                realtime::NativeSequenceRecoveryPollErrorV1::
                    kNotReady) {
                return result;
            }
            if (error !=
                realtime::NativeSequenceRecoveryPollErrorV1::kNone) {
                FreezeGlobalResource();
                result.state_dirty_after_last_publish = true;
                return result;
            }
            result.made_progress = true;
            if (ready.record_class ==
                realtime::NativeSequenceRecoveryRecordClassV1::
                    kFiltered) {
                const auto commit_error =
                    recovery_->CommitCertified(ready.token);
                if (commit_error !=
                    realtime::
                        NativeSequenceRecoveryCommitErrorV1::kNone) {
                    FreezeGlobalResource();
                    result.state_dirty_after_last_publish = true;
                    return result;
                }
                EnsureChannel(ready.descriptor.domain);
                UpdateChannel(ready.descriptor.domain);
                result.state_dirty_after_last_publish = true;
                if (PublishHeader(DeriveAggregateState())) {
                    result.published_header = true;
                    result.state_dirty_after_last_publish = false;
                }
                continue;
            }
            if (ready.record_class !=
                realtime::NativeSequenceRecoveryRecordClassV1::
                    kTarget) {
                FreezeGlobalResource();
                result.state_dirty_after_last_publish = true;
                return result;
            }
            if (payload_leases_ == nullptr) {
                FreezeGlobalResource();
                result.state_dirty_after_last_publish = true;
                return result;
            }
            const RealtimeWireTickPayloadV2* const leased_payload =
                payload_leases_->Resolve(ready.applied_cookie);
            if (leased_payload == nullptr) {
                FreezeGlobalResource();
                result.state_dirty_after_last_publish = true;
                return result;
            }
            CertifiedPayloadLeaseGuard payload_lease(
                payload_leases_.get(), ready.applied_cookie);
            const RealtimeWireTickPayloadV2& payload =
                *leased_payload;
            if (payload.common.instrument_id == 0U ||
                payload.common.ordinal !=
                    payload.common.instrument_id - 1U ||
                !RealtimeCertifiedTickPayloadCanonicalV1(payload) ||
                DescriptorFromPayload(payload) != ready.descriptor ||
                !(MessageKeyFromPayload(payload) ==
                  ready.message_key) ||
                canonical_apply_frontier_ ==
                    std::numeric_limits<std::uint64_t>::max()) {
                FreezeGlobalResource();
                result.state_dirty_after_last_publish = true;
                return result;
            }
            const std::uint64_t next =
                canonical_apply_frontier_ + 1U;
            std::uint64_t certified_ns = 0U;
            if (!ReadMonotonicNs(&certified_ns)) {
                FreezeGlobalResource();
                result.state_dirty_after_last_publish = true;
                return result;
            }
            RealtimeCertifiedTickEnvelopeV1 envelope{};
            envelope.canonical_apply_sequence = next;
            envelope.correction_epoch = correction_epoch_;
            envelope.feed_epoch = 1U;
            envelope.certified_monotonic_ns = certified_ns;
            envelope.payload = payload;
            std::uint32_t ring_index = 0U;
            if (!RealtimeCertifiedRingSlotIndexV1(
                    next,
                    header_->certified_ring_capacity,
                    &ring_index) ||
                payload.common.ordinal >=
                    header_->latest_capacity ||
                !CanPublishSlot(ring_[ring_index]) ||
                !CanPublishSlot(
                    latest_[payload.common.ordinal]) ||
                !RealtimeCertifiedTickEnvelopeCanonicalV1(
                    envelope)) {
                FreezeGlobalResource();
                result.state_dirty_after_last_publish = true;
                return result;
            }
            // PollCertified returned the exact next ready token and this
            // serialized worker has not touched the coordinator since. Commit
            // therefore cannot fail for a valid ready token; check it anyway
            // and expose nothing if an internal invariant is violated.
            const realtime::NativeSequenceRecoveryCommitErrorV1
                commit_error =
                    recovery_->CommitCertified(ready.token);
            if (commit_error !=
                realtime::NativeSequenceRecoveryCommitErrorV1::
                    kNone) {
                FreezeGlobalResource();
                result.state_dirty_after_last_publish = true;
                return result;
            }
            // Event projection/storage is the final resource-fallible part of
            // the transaction. Run it before touching a previously published
            // latest/ring slot. On failure both public Tick and Event views
            // therefore retain their same last-good canonical frontier; the
            // coordinator is internal and the service freezes terminally.
            CertifiedOrderEventHistorySnapshotV1 next_event_generation{};
            if (event_history_->AppendCertifiedTick(
                    payload,
                    next,
                    &next_event_generation) !=
                    CertifiedOrderEventHistoryErrorV1::kNone ||
                !next_event_generation.valid() ||
                next_event_generation.generation()
                        .input_frontier
                        .canonical_apply_sequence !=
                    next) {
                FreezeGlobalResource();
                result.state_dirty_after_last_publish = true;
                return result;
            }
            // Both bounded slots were preflighted above and this is their sole
            // writer, so readers cannot make either publish fail. The header
            // frontier remains the external CERTIFIED visibility gate until
            // both stores finish. The independent Tick History writer copies
            // this already-published ring prefix later; History failure never
            // delays or freezes bounded CERTIFIED publication.
            if (!PublishSlot(&ring_[ring_index], envelope) ||
                !PublishSlot(
                    &latest_[payload.common.ordinal],
                    envelope)) {
                FreezeGlobalResource();
                result.state_dirty_after_last_publish = true;
                return result;
            }
            canonical_apply_frontier_ = next;
            EnsureChannel(ready.descriptor.domain);
            auto channel = channels_.find(ready.descriptor.domain);
            if (channel != channels_.end()) {
                ++channel->second.certified_tick_count;
                channel->second.last_canonical_apply_sequence =
                    next;
                MarkChannelWireDirty(&channel->second);
            }
            UpdateChannel(ready.descriptor.domain);
            // Commit each recovered Tick as one externally visible prefix
            // step. Besides lowering reader catch-up latency, this prevents a
            // long drain from overwriting multiple ring positions while the
            // public retention frontier still describes the old window.
            result.state_dirty_after_last_publish = true;
            if (PublishHeader(DeriveAggregateState())) {
                result.published_header = true;
                result.state_dirty_after_last_publish = false;
            }
            // PublishHeader is the external Tick visibility commit. Only
            // after its stable even tag reaches `next` may the legacy
            // process-local Event mirror expose the matching generation.
            const std::uint64_t public_tag =
                Atomic(header_->status_publish_tag)
                    .load(std::memory_order_acquire);
            const std::uint64_t public_frontier =
                Atomic(header_->canonical_apply_frontier)
                    .load(std::memory_order_acquire);
            if (public_tag == 0U || (public_tag & 1U) != 0U ||
                public_frontier != next) {
                FreezeGlobalResource();
                result.state_dirty_after_last_publish = true;
                return result;
            }
            {
                const std::lock_guard<std::mutex> lock(
                    committed_event_generation_mutex_);
                committed_event_generation_ =
                    std::move(next_event_generation);
            }
            if (!payload_lease.Release()) {
                FreezeGlobalResource();
                result.state_dirty_after_last_publish = true;
                return result;
            }
        }
    }

    void EnsureChannel(
        const realtime::NativeSequenceChannelV1& domain) {
        if (channels_.find(domain) != channels_.end()) {
            return;
        }
        if (channels_.size() >= config_.channel_capacity) {
            unrepresented_resource_failure_ = true;
            FreezeGlobalResource();
            return;
        }
        ChannelRuntime value{};
        value.row_index =
            static_cast<std::uint32_t>(channels_.size());
        value.snapshot.domain = domain;
        channels_.emplace(domain, value);
    }

    [[nodiscard]] static ChannelAggregateClass AggregateClass(
        realtime::NativeSequenceRecoveryChannelStateV1 state)
        noexcept {
        switch (state) {
            case realtime::NativeSequenceRecoveryChannelStateV1::
                kHealthy:
                return ChannelAggregateClass::kHealthy;
            case realtime::NativeSequenceRecoveryChannelStateV1::
                kBootstrapping:
            case realtime::NativeSequenceRecoveryChannelStateV1::
                kRepairing:
                return ChannelAggregateClass::kGap;
            case realtime::NativeSequenceRecoveryChannelStateV1::
                kCatchingUp:
                return ChannelAggregateClass::kCatchingUp;
            case realtime::NativeSequenceRecoveryChannelStateV1::
                kFrozenConflict:
            case realtime::NativeSequenceRecoveryChannelStateV1::
                kFrozenResource:
                return ChannelAggregateClass::kFrozen;
        }
        return ChannelAggregateClass::kFrozen;
    }

    [[nodiscard]] static bool PublishedSnapshotEqual(
        const realtime::NativeSequenceRecoveryChannelSnapshotV1& left,
        const realtime::NativeSequenceRecoveryChannelSnapshotV1& right)
        noexcept {
        return left.domain == right.domain &&
               left.state == right.state &&
               left.origin_sequence == right.origin_sequence &&
               left.certified_sequence == right.certified_sequence &&
               left.observed_contiguous_sequence ==
                   right.observed_contiguous_sequence &&
               left.highest_observed_sequence ==
                   right.highest_observed_sequence &&
               left.unique_sequences == right.unique_sequences &&
               left.duplicate_arrivals == right.duplicate_arrivals &&
               left.exact_duplicate_applications ==
                   right.exact_duplicate_applications &&
               left.pending_entries == right.pending_entries &&
               left.channel_correction_epoch ==
                   right.channel_correction_epoch &&
               (left.missing_sequences != 0U) ==
                   (right.missing_sequences != 0U);
    }

    void MarkChannelWireDirty(ChannelRuntime* runtime) {
        if (runtime == nullptr || runtime->wire_dirty) {
            return;
        }
        runtime->wire_dirty = true;
        dirty_channels_.push_back(runtime);
    }

    [[nodiscard]] bool ReplaceChannelAggregate(
        const ChannelRuntime& runtime,
        const realtime::NativeSequenceRecoveryChannelSnapshotV1& snapshot)
        noexcept {
        std::uint32_t healthy = healthy_channel_count_;
        std::uint32_t gap = gap_channel_count_;
        std::uint32_t catching = catching_up_channel_count_;
        std::uint32_t frozen = frozen_channel_count_;
        const auto adjust = [](
                                ChannelAggregateClass aggregate_class,
                                bool increment,
                                std::uint32_t* healthy_count,
                                std::uint32_t* gap_count,
                                std::uint32_t* catching_count,
                                std::uint32_t* frozen_count) noexcept {
            std::uint32_t* value = nullptr;
            switch (aggregate_class) {
                case ChannelAggregateClass::kHealthy:
                    value = healthy_count;
                    break;
                case ChannelAggregateClass::kGap:
                    value = gap_count;
                    break;
                case ChannelAggregateClass::kCatchingUp:
                    value = catching_count;
                    break;
                case ChannelAggregateClass::kFrozen:
                    value = frozen_count;
                    break;
            }
            if (value == nullptr ||
                (increment &&
                 *value == std::numeric_limits<std::uint32_t>::max()) ||
                (!increment && *value == 0U)) {
                return false;
            }
            if (increment) {
                ++(*value);
            } else {
                --(*value);
            }
            return true;
        };
        if (runtime.initialized &&
            !adjust(
                AggregateClass(runtime.snapshot.state),
                false,
                &healthy,
                &gap,
                &catching,
                &frozen)) {
            return false;
        }
        if (!adjust(
                AggregateClass(snapshot.state),
                true,
                &healthy,
                &gap,
                &catching,
                &frozen)) {
            return false;
        }

        const std::uint64_t old_duplicates =
            runtime.initialized
                ? runtime.snapshot.exact_duplicate_applications
                : 0U;
        const std::uint64_t old_pending =
            runtime.initialized
                ? static_cast<std::uint64_t>(
                      runtime.snapshot.pending_entries)
                : 0U;
        const std::uint64_t new_pending =
            static_cast<std::uint64_t>(snapshot.pending_entries);
        if (exact_duplicate_message_count_ < old_duplicates ||
            pending_token_count_ < old_pending) {
            return false;
        }
        const std::uint64_t duplicate_base =
            exact_duplicate_message_count_ - old_duplicates;
        const std::uint64_t pending_base =
            pending_token_count_ - old_pending;
        if (snapshot.exact_duplicate_applications >
                std::numeric_limits<std::uint64_t>::max() -
                    duplicate_base ||
            new_pending >
                std::numeric_limits<std::uint64_t>::max() -
                    pending_base) {
            return false;
        }

        healthy_channel_count_ = healthy;
        gap_channel_count_ = gap;
        catching_up_channel_count_ = catching;
        frozen_channel_count_ = frozen;
        exact_duplicate_message_count_ =
            duplicate_base +
            snapshot.exact_duplicate_applications;
        pending_token_count_ = pending_base + new_pending;
        return true;
    }

    void UpdateChannel(
        const realtime::NativeSequenceChannelV1& domain) noexcept {
        auto position = channels_.find(domain);
        if (position == channels_.end()) {
            return;
        }
        realtime::NativeSequenceRecoveryChannelSnapshotV1 snapshot{};
        if (!recovery_->ChannelSnapshot(domain, &snapshot)) {
            return;
        }
        ChannelRuntime& runtime = position->second;
        const bool new_gap = snapshot.missing_sequences != 0U;
        if (runtime.initialized &&
            PublishedSnapshotEqual(runtime.snapshot, snapshot)) {
            // Retain non-Wire diagnostic fields as the latest local view
            // without turning an identical public row into dirty work.
            runtime.snapshot = snapshot;
            return;
        }
        const bool opens_gap = !runtime.gap_active && new_gap;
        const bool recovers_gap = runtime.gap_active && !new_gap;
        // Preflight every worker-owned monotonic counter before replacing
        // cached aggregate totals. A terminal numeric failure must leave the
        // last published/cacheable channel cut internally self-consistent.
        if ((opens_gap &&
             (gap_opened_count_ ==
                  std::numeric_limits<std::uint64_t>::max() ||
              runtime.gap_opened_count ==
                  std::numeric_limits<std::uint64_t>::max())) ||
            (recovers_gap &&
             (gap_recovered_count_ ==
                  std::numeric_limits<std::uint64_t>::max() ||
              runtime.gap_recovered_count ==
                  std::numeric_limits<std::uint64_t>::max() ||
              correction_epoch_ ==
                  std::numeric_limits<std::uint64_t>::max()))) {
            FreezeGlobalResource();
            return;
        }
        if (!ReplaceChannelAggregate(runtime, snapshot)) {
            FreezeGlobalResource();
            return;
        }
        if (opens_gap) {
            ++gap_opened_count_;
            ++runtime.gap_opened_count;
        } else if (recovers_gap) {
            ++gap_recovered_count_;
            ++runtime.gap_recovered_count;
            ++correction_epoch_;
        }
        runtime.gap_active = new_gap;
        if (!runtime.conflict_freeze_counted &&
            snapshot.state ==
                realtime::NativeSequenceRecoveryChannelStateV1::
                    kFrozenConflict) {
            if (!IncrementSaturating(
                    &conflicting_duplicate_count_)) {
                FreezeGlobalResource();
                return;
            }
            runtime.conflict_freeze_counted = true;
        }
        if (!runtime.resource_freeze_counted &&
            snapshot.state ==
                realtime::NativeSequenceRecoveryChannelStateV1::
                    kFrozenResource) {
            if (!IncrementSaturating(
                    &resource_exhaustion_count_)) {
                FreezeGlobalResource();
                return;
            }
            runtime.resource_freeze_counted = true;
        }
        runtime.snapshot = snapshot;
        runtime.initialized = true;
        MarkChannelWireDirty(&runtime);
    }

    [[nodiscard]] static RealtimeCertifiedStateV1 WireState(
        realtime::NativeSequenceRecoveryChannelStateV1 state)
        noexcept {
        switch (state) {
            case realtime::NativeSequenceRecoveryChannelStateV1::
                kHealthy:
                return RealtimeCertifiedStateV1::kContiguous;
            case realtime::NativeSequenceRecoveryChannelStateV1::
                kCatchingUp:
                return RealtimeCertifiedStateV1::kCatchingUp;
            case realtime::NativeSequenceRecoveryChannelStateV1::
                kRepairing:
                return RealtimeCertifiedStateV1::kGapOpen;
            case realtime::NativeSequenceRecoveryChannelStateV1::
                kFrozenConflict:
                return RealtimeCertifiedStateV1::kFrozenConflict;
            case realtime::NativeSequenceRecoveryChannelStateV1::
                kFrozenResource:
                return RealtimeCertifiedStateV1::kFrozenResource;
            case realtime::NativeSequenceRecoveryChannelStateV1::
                kBootstrapping:
                return RealtimeCertifiedStateV1::kGapOpen;
        }
        return RealtimeCertifiedStateV1::kFrozenResource;
    }

    [[nodiscard]] bool PublishChannel(
        ChannelRuntime& runtime,
        std::uint64_t aggregate_commit_tag) noexcept {
        if (channel_rows_ == nullptr ||
            runtime.row_index >= config_.channel_capacity ||
            !runtime.initialized ||
            !runtime.wire_dirty ||
            runtime.snapshot.channel_correction_epoch == 0U ||
            aggregate_commit_tag < 2U ||
            (aggregate_commit_tag & 1U) != 0U) {
            return false;
        }
        RealtimeCertifiedChannelStateV1& row =
            channel_rows_[runtime.row_index];
        std::atomic_ref<std::uint64_t> tag =
            Atomic(row.publish_tag);
        std::uint64_t stable =
            tag.load(std::memory_order_acquire);
        if ((stable & 1U) != 0U ||
            stable > aggregate_commit_tag - 2U) {
            return false;
        }
        if (!tag.compare_exchange_strong(
                stable,
                aggregate_commit_tag - 1U,
                std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            // Readers never modify the tag and this service has one row writer.
            // A failed transition is therefore an internal publication
            // invariant, not permission to write through another odd epoch.
            return false;
        }
        std::atomic_thread_fence(std::memory_order_release);
        const auto state = WireState(runtime.snapshot.state);
        Atomic(row.feed_epoch).store(
            1U, std::memory_order_relaxed);
        Atomic(row.origin_sequence).store(
            static_cast<std::int64_t>(
                runtime.snapshot.origin_sequence),
            std::memory_order_relaxed);
        Atomic(row.observed_contiguous_frontier).store(
            static_cast<std::int64_t>(
                runtime.snapshot.observed_contiguous_sequence),
            std::memory_order_relaxed);
        Atomic(row.certified_published_frontier).store(
            static_cast<std::int64_t>(
                runtime.snapshot.certified_sequence),
            std::memory_order_relaxed);
        Atomic(row.highest_observed_sequence).store(
            static_cast<std::int64_t>(
                runtime.snapshot.highest_observed_sequence),
            std::memory_order_relaxed);
        Atomic(row.canonical_apply_frontier).store(
            runtime.last_canonical_apply_sequence,
            std::memory_order_relaxed);
        std::uint64_t observed = 0U;
        if (!CheckedAdd(
                runtime.snapshot.unique_sequences,
                runtime.snapshot.duplicate_arrivals,
                &observed)) {
            return false;
        }
        Atomic(row.observed_native_message_count).store(
            observed, std::memory_order_relaxed);
        Atomic(row.certified_tick_count).store(
            runtime.certified_tick_count,
            std::memory_order_relaxed);
        Atomic(row.exact_duplicate_message_count).store(
            runtime.snapshot.exact_duplicate_applications,
            std::memory_order_relaxed);
        Atomic(row.pending_token_count).store(
            static_cast<std::uint64_t>(
                runtime.snapshot.pending_entries),
            std::memory_order_relaxed);
        Atomic(row.gap_opened_count).store(
            runtime.gap_opened_count,
            std::memory_order_relaxed);
        Atomic(row.gap_recovered_count).store(
            runtime.gap_recovered_count,
            std::memory_order_relaxed);
        Atomic(row.state).store(
            static_cast<std::uint32_t>(state),
            std::memory_order_relaxed);
        Atomic(row.trade_date).store(
            config_.trade_date, std::memory_order_relaxed);
        Atomic(row.channel).store(
            runtime.snapshot.domain.channel,
            std::memory_order_relaxed);
        Atomic(row.market).store(
            static_cast<std::uint8_t>(
                runtime.snapshot.domain.market),
            std::memory_order_relaxed);
        Atomic(row.channel_correction_epoch).store(
            runtime.snapshot.channel_correction_epoch,
            std::memory_order_relaxed);
        tag.store(
            aggregate_commit_tag, std::memory_order_release);
        return true;
    }

    [[nodiscard]] RealtimeCertifiedStateV1
    DeriveAggregateState() const noexcept {
        if (globally_frozen_resource_.load(
                std::memory_order_acquire) ||
            unrepresented_resource_failure_) {
            return RealtimeCertifiedStateV1::kFrozenResource;
        }
        if (frozen_channel_count_ != 0U) {
            return RealtimeCertifiedStateV1::kDegraded;
        }
        if (gap_channel_count_ != 0U) {
            return RealtimeCertifiedStateV1::kGapOpen;
        }
        if (catching_up_channel_count_ != 0U) {
            return RealtimeCertifiedStateV1::kCatchingUp;
        }
        return channels_.empty()
                   ? RealtimeCertifiedStateV1::kNoData
                   : RealtimeCertifiedStateV1::kContiguous;
    }

    bool PublishHeader(
        RealtimeCertifiedStateV1 requested_state) noexcept {
        if (header_ == nullptr) {
            return false;
        }
        std::uint64_t heartbeat = 0U;
        if (!ReadMonotonicNs(&heartbeat)) {
            heartbeat = Atomic(header_->heartbeat_monotonic_ns)
                            .load(std::memory_order_relaxed);
        }
        std::atomic_ref<std::uint64_t> tag =
            Atomic(header_->status_publish_tag);
        const std::uint64_t stable =
            tag.load(std::memory_order_acquire);
        if ((stable & 1U) != 0U ||
            stable >
                std::numeric_limits<std::uint64_t>::max() - 2U) {
            return false;
        }
        std::uint64_t expected = stable;
        if (!tag.compare_exchange_strong(
                expected,
                stable + 1U,
                std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            return false;
        }
        std::atomic_thread_fence(std::memory_order_release);
        const std::uint64_t aggregate_commit_tag = stable + 2U;
        for (ChannelRuntime* runtime : dirty_channels_) {
            if (runtime == nullptr || !runtime->wire_dirty ||
                !PublishChannel(*runtime, aggregate_commit_tag)) {
                // The header is already in its private odd epoch. Preserve a
                // coherent external cut by committing a terminal resource
                // state; no partially prepared row is visible until the
                // header reaches aggregate_commit_tag below.
                FreezeGlobalResource();
                requested_state =
                    RealtimeCertifiedStateV1::kFrozenResource;
                break;
            }
            runtime->wire_dirty = false;
        }
        dirty_channels_.erase(
            std::remove_if(
                dirty_channels_.begin(),
                dirty_channels_.end(),
                [](const ChannelRuntime* runtime) noexcept {
                    return runtime != nullptr &&
                           !runtime->wire_dirty;
                }),
            dirty_channels_.end());
        // Freeze is monotonic. Re-evaluate it inside this acquired write epoch
        // rather than trusting DeriveAggregateState() evaluated by the caller.
        // The counter and state below are taken from the same local decision,
        // so a concurrent producer freeze can yield either the prior coherent
        // snapshot or FROZEN_RESOURCE, never CONTIGUOUS with a nonzero resource
        // count in one stable header publication.
        const bool globally_frozen_resource =
            globally_frozen_resource_.load(std::memory_order_acquire);
        const std::uint64_t resource_exhaustion_count =
            resource_exhaustion_count_.load(std::memory_order_acquire);
        if (globally_frozen_resource ||
            unrepresented_resource_failure_) {
            requested_state =
                RealtimeCertifiedStateV1::kFrozenResource;
        }
        Atomic(header_->heartbeat_monotonic_ns).store(
            heartbeat, std::memory_order_relaxed);
        Atomic(header_->canonical_apply_frontier).store(
            canonical_apply_frontier_,
            std::memory_order_relaxed);
        Atomic(header_->correction_epoch).store(
            correction_epoch_, std::memory_order_relaxed);
        Atomic(header_->observed_native_message_count).store(
            observed_native_message_count_.load(
                std::memory_order_relaxed),
            std::memory_order_relaxed);
        Atomic(header_->certified_tick_count).store(
            canonical_apply_frontier_,
            std::memory_order_relaxed);
        Atomic(header_->exact_duplicate_message_count).store(
            exact_duplicate_message_count_,
            std::memory_order_relaxed);
        Atomic(header_->gap_opened_count).store(
            gap_opened_count_, std::memory_order_relaxed);
        Atomic(header_->gap_recovered_count).store(
            gap_recovered_count_, std::memory_order_relaxed);
        Atomic(header_->conflicting_duplicate_count).store(
            conflicting_duplicate_count_.load(
                std::memory_order_relaxed),
            std::memory_order_relaxed);
        Atomic(header_->resource_exhaustion_count).store(
            resource_exhaustion_count,
            std::memory_order_relaxed);
        Atomic(header_->pending_token_count).store(
            pending_token_count_, std::memory_order_relaxed);
        Atomic(header_->aggregate_state).store(
            static_cast<std::uint32_t>(requested_state),
            std::memory_order_relaxed);
        Atomic(header_->channel_state_count).store(
            static_cast<std::uint32_t>(channels_.size()),
            std::memory_order_relaxed);
        Atomic(header_->gap_open_channel_count).store(
            gap_channel_count_, std::memory_order_relaxed);
        Atomic(header_->catching_up_channel_count).store(
            catching_up_channel_count_,
            std::memory_order_relaxed);
        Atomic(header_->frozen_channel_count).store(
            frozen_channel_count_, std::memory_order_relaxed);
        tag.store(aggregate_commit_tag, std::memory_order_release);
        return true;
    }

    [[nodiscard]] RealtimeCertifiedServiceCreateErrorV1
    CreateListener(int* system_error_number) {
        struct stat existing {};
        if (::lstat(
                config_.control_socket_path.c_str(),
                &existing) == 0) {
            return RealtimeCertifiedServiceCreateErrorV1::
                kSocketPathExists;
        }
        if (errno != ENOENT) {
            SetSystemError(system_error_number, errno);
            return RealtimeCertifiedServiceCreateErrorV1::
                kSocketBindFailed;
        }
        listener_fd_ = ::socket(
            AF_UNIX,
            SOCK_SEQPACKET | SOCK_CLOEXEC | SOCK_NONBLOCK,
            0);
        if (listener_fd_ < 0) {
            SetSystemError(system_error_number, errno);
            return RealtimeCertifiedServiceCreateErrorV1::
                kSocketCreateFailed;
        }
        sockaddr_un address{};
        address.sun_family = AF_UNIX;
        const std::string path =
            config_.control_socket_path.string();
        std::memcpy(
            address.sun_path, path.c_str(), path.size() + 1U);
        if (::bind(
                listener_fd_,
                reinterpret_cast<const sockaddr*>(&address),
                sizeof(address)) != 0 ||
            ::chmod(path.c_str(), S_IRUSR | S_IWUSR) != 0 ||
            ::listen(listener_fd_, 16) != 0) {
            SetSystemError(system_error_number, errno);
            return RealtimeCertifiedServiceCreateErrorV1::
                kSocketBindFailed;
        }
        struct stat bound {};
        if (::lstat(path.c_str(), &bound) != 0 ||
            !S_ISSOCK(bound.st_mode) ||
            bound.st_uid != ::geteuid()) {
            SetSystemError(system_error_number, errno);
            return RealtimeCertifiedServiceCreateErrorV1::
                kSocketBindFailed;
        }
        socket_device_ = bound.st_dev;
        socket_inode_ = bound.st_ino;
        socket_bound_ = true;
        return RealtimeCertifiedServiceCreateErrorV1::kNone;
    }

    void SafeUnlinkSocket() noexcept {
        if (!socket_bound_) {
            return;
        }
        struct stat current {};
        if (::lstat(
                config_.control_socket_path.c_str(),
                &current) == 0 &&
            current.st_dev == socket_device_ &&
            current.st_ino == socket_inode_ &&
            S_ISSOCK(current.st_mode) &&
            current.st_uid == ::geteuid()) {
            static_cast<void>(
                ::unlink(config_.control_socket_path.c_str()));
        }
        socket_bound_ = false;
    }

    void SignalStopEvent() noexcept {
        if (stop_event_fd_ < 0) {
            return;
        }
        const std::uint64_t one = 1U;
        ssize_t result = -1;
        do {
            result = ::write(stop_event_fd_, &one, sizeof(one));
        } while (result < 0 && errno == EINTR);
    }

    void MarkControlFailedIfRunning() noexcept {
        auto expected =
            RealtimeCertifiedServiceSnapshotV1::ControlState::
                kRunning;
        static_cast<void>(control_state_.compare_exchange_strong(
            expected,
            RealtimeCertifiedServiceSnapshotV1::ControlState::
                kFailed,
            std::memory_order_acq_rel,
            std::memory_order_acquire));
    }

    [[nodiscard]] bool ControlExposureReady() const noexcept {
        return config_.control_exposure_gate == nullptr ||
               config_.control_exposure_gate->load(
                   std::memory_order_acquire);
    }

    void ControlLoop() noexcept {
        using ControlState =
            RealtimeCertifiedServiceSnapshotV1::ControlState;
        std::array<pollfd, 2U> descriptors{};
        descriptors[0].fd = listener_fd_;
        descriptors[0].events = POLLIN;
        descriptors[1].fd = stop_event_fd_;
        descriptors[1].events = POLLIN;
        while (control_state_.load(std::memory_order_acquire) ==
               ControlState::kRunning) {
            int result = -1;
            do {
                result = ::poll(
                    descriptors.data(),
                    static_cast<nfds_t>(descriptors.size()),
                    -1);
            } while (result < 0 && errno == EINTR);
            if (result <= 0) {
                MarkControlFailedIfRunning();
                break;
            }
            if ((descriptors[1].revents &
                 (POLLIN | POLLERR | POLLHUP | POLLNVAL)) != 0) {
                MarkControlFailedIfRunning();
                break;
            }
            if ((descriptors[0].revents &
                 (POLLERR | POLLHUP | POLLNVAL)) != 0) {
                MarkControlFailedIfRunning();
                break;
            }
            if ((descriptors[0].revents & POLLIN) == 0) {
                continue;
            }
            constexpr std::size_t kMaximumAcceptBatch = 64U;
            std::size_t attempts = 0U;
            while (attempts < kMaximumAcceptBatch &&
                   control_state_.load(std::memory_order_acquire) ==
                       ControlState::kRunning) {
                ++attempts;
                const int client = ::accept4(
                    listener_fd_,
                    nullptr,
                    nullptr,
                    SOCK_CLOEXEC);
                if (client < 0) {
                    if (errno == EINTR) {
                        continue;
                    }
                    if (errno == ECONNABORTED) {
                        continue;
                    }
                    if (errno != EAGAIN && errno != EWOULDBLOCK) {
                        MarkControlFailedIfRunning();
                    }
                    break;
                }
                const timeval timeout{1, 0};
                static_cast<void>(::setsockopt(
                    client,
                    SOL_SOCKET,
                    SO_RCVTIMEO,
                    &timeout,
                    sizeof(timeout)));
                static_cast<void>(::setsockopt(
                    client,
                    SOL_SOCKET,
                    SO_SNDTIMEO,
                    &timeout,
                    sizeof(timeout)));
                if (ControlExposureReady()) {
                    HandleClient(client);
                }
                int close_result = -1;
                do {
                    close_result = ::close(client);
                } while (close_result < 0 && errno == EINTR);
            }
        }
    }

public:
    [[nodiscard]] bool FailControlForTest() noexcept {
        if (control_state_.load(std::memory_order_acquire) !=
            RealtimeCertifiedServiceSnapshotV1::ControlState::
                kRunning) {
            return false;
        }
        SignalStopEvent();
        return true;
    }

    void SetPrefixProbePausedForTest(bool paused) noexcept {
        if (paused) {
            prefix_probe_reached_for_test_.store(
                false, std::memory_order_release);
        }
        prefix_probe_paused_for_test_.store(
            paused, std::memory_order_release);
        if (!paused) {
            WakeWorker();
        }
    }

    [[nodiscard]] bool PrefixProbeReachedForTest() const noexcept {
        return prefix_probe_reached_for_test_.load(
            std::memory_order_acquire);
    }

    [[nodiscard]] bool PrefixProbeWaitingForPriorAckForTest()
        const noexcept {
        return prefix_probe_waiting_for_prior_ack_for_test_.load(
            std::memory_order_acquire);
    }

    void SetPrefixCommitPausedForTest(bool paused) noexcept {
        prefix_commit_paused_for_test_.store(
            paused, std::memory_order_release);
        if (!paused) {
            WakeWorker();
        }
    }

    [[nodiscard]] bool PrefixCommitReachedForTest() const noexcept {
        return prefix_commit_reached_for_test_.load(
            std::memory_order_acquire);
    }

    [[nodiscard]] bool PrefixCommitFinishedForTest() const noexcept {
        return prefix_commit_finished_for_test_.load(
            std::memory_order_acquire);
    }

    [[nodiscard]] bool StartupPrefixRecoveredForTest() const noexcept {
        return event_journal_ != nullptr &&
               (event_journal_->coverage_flags() &
                kCertifiedOrderEventStartupPrefixRecoveredV1) != 0U;
    }

    [[nodiscard]] bool ReadWorkerCpuSetForTest(
        common::LinuxCpuSetV1* output,
        int* system_error_number) noexcept {
        SetSystemError(system_error_number, 0);
        if (output == nullptr || !worker_thread_.joinable()) {
            SetSystemError(system_error_number, EINVAL);
            return false;
        }
        return common::ReadLinuxThreadAffinityV1(
                   worker_thread_.native_handle(),
                   output,
                   system_error_number) ==
               common::LinuxThreadAffinityErrorV1::kNone;
    }

    [[nodiscard]] bool ReadControlCpuSetForTest(
        common::LinuxCpuSetV1* output,
        int* system_error_number) noexcept {
        SetSystemError(system_error_number, 0);
        if (output == nullptr || !control_thread_.joinable()) {
            SetSystemError(system_error_number, EINVAL);
            return false;
        }
        return common::ReadLinuxThreadAffinityV1(
                   control_thread_.native_handle(),
                   output,
                   system_error_number) ==
               common::LinuxThreadAffinityErrorV1::kNone;
    }

    [[nodiscard]] bool ReadTickHistoryWorkerCpuSetForTest(
        common::LinuxCpuSetV1* output,
        int* system_error_number) noexcept {
        SetSystemError(system_error_number, 0);
        if (output == nullptr || !tick_history_thread_.joinable()) {
            SetSystemError(system_error_number, EINVAL);
            return false;
        }
        return common::ReadLinuxThreadAffinityV1(
                   tick_history_thread_.native_handle(),
                   output,
                   system_error_number) ==
               common::LinuxThreadAffinityErrorV1::kNone;
    }

    void SetTickHistoryWriterPausedForTest(bool paused) noexcept {
        if (paused) {
            tick_history_pause_reached_for_test_.store(
                false, std::memory_order_release);
        }
        tick_history_paused_for_test_.store(
            paused, std::memory_order_release);
    }

    [[nodiscard]] bool TickHistoryWriterPauseReachedForTest()
        const noexcept {
        return tick_history_pause_reached_for_test_.load(
            std::memory_order_acquire);
    }

    [[nodiscard]] bool ControlRunningConfirmed() noexcept {
        auto expected =
            RealtimeCertifiedServiceSnapshotV1::ControlState::
                kRunning;
        return control_state_.compare_exchange_strong(
            expected,
            RealtimeCertifiedServiceSnapshotV1::ControlState::
                kRunning,
            std::memory_order_acq_rel,
            std::memory_order_acquire);
    }

    [[nodiscard]] RealtimeCertifiedServiceSnapshotV1::ControlState
    ControlStateSnapshot() const noexcept {
        return control_state_.load(std::memory_order_acquire);
    }

private:
    void HandleClient(int client) noexcept {
        if (client < 0) {
            return;
        }
        ucred credentials{};
        socklen_t credential_bytes = sizeof(credentials);
        if (::getsockopt(
                client,
                SOL_SOCKET,
                SO_PEERCRED,
                &credentials,
                &credential_bytes) != 0 ||
            credential_bytes != sizeof(credentials) ||
            credentials.uid != ::geteuid()) {
            return;
        }
        RealtimeCertifiedControlRequestV1 request{};
        const ssize_t received = ::recv(
            client, &request, sizeof(request), MSG_TRUNC);
        RealtimeCertifiedControlResponseV1 response{};
        response.nonce = request.nonce;
        response.mapping_bytes = mapping_bytes_;
        CopyIdentity(config_.run_id, &response.run_id);
        response.session_epoch = config_.session_epoch;
        response.trade_date = config_.trade_date;
        const bool request_prefix_valid =
            received == static_cast<ssize_t>(sizeof(request)) &&
            request.magic ==
                kRealtimeCertifiedControlRequestMagicV1 &&
            request.abi_major ==
                kRealtimeCertifiedWireMajorV1 &&
            request.abi_minor <=
                kRealtimeCertifiedWireMinorV1 &&
            request.request_bytes == sizeof(request) &&
            request.reserved == 0U;
        if (request_prefix_valid &&
            request.opcode == static_cast<std::uint16_t>(
                RealtimeCertifiedControlOpcodeV1::
                    kGetTickHistory)) {
            HandleTickHistoryClient(client, request);
            return;
        }
        if (request_prefix_valid &&
            request.opcode == static_cast<std::uint16_t>(
                RealtimeCertifiedControlOpcodeV1::
                    kGetEventHistory)) {
            HandleEventHistoryClient(client, request);
            return;
        }
        const bool valid =
            request_prefix_valid &&
            request.opcode == static_cast<std::uint16_t>(
                RealtimeCertifiedControlOpcodeV1::kGetSession);
        response.status = static_cast<std::uint16_t>(
            valid
                ? RealtimeCertifiedControlStatusV1::kOk
                : RealtimeCertifiedControlStatusV1::
                      kInvalidRequest);
        if (!valid) {
            static_cast<void>(::send(
                client,
                &response,
                sizeof(response),
                MSG_NOSIGNAL));
            return;
        }
        int descriptor = -1;
        if (!DuplicateReadOnlyDescriptorForTest(&descriptor)) {
            response.status = static_cast<std::uint16_t>(
                RealtimeCertifiedControlStatusV1::kInternal);
            static_cast<void>(::send(
                client,
                &response,
                sizeof(response),
                MSG_NOSIGNAL));
            return;
        }
        std::array<std::byte, CMSG_SPACE(sizeof(int))> control{};
        iovec vector{};
        vector.iov_base = &response;
        vector.iov_len = sizeof(response);
        msghdr message{};
        message.msg_iov = &vector;
        message.msg_iovlen = 1U;
        message.msg_control = control.data();
        message.msg_controllen = control.size();
        cmsghdr* const ancillary = CMSG_FIRSTHDR(&message);
        ancillary->cmsg_level = SOL_SOCKET;
        ancillary->cmsg_type = SCM_RIGHTS;
        ancillary->cmsg_len = CMSG_LEN(sizeof(int));
        std::memcpy(CMSG_DATA(ancillary), &descriptor, sizeof(int));
        static_cast<void>(
            ::sendmsg(client, &message, MSG_NOSIGNAL));
        CloseDescriptor(&descriptor);
    }

    void HandleTickHistoryClient(
        int client,
        const RealtimeCertifiedControlRequestV1& request) noexcept {
        RealtimeCertifiedTickHistoryControlResponseV1 response{};
        response.nonce = request.nonce;
        CopyIdentity(config_.run_id, &response.run_id);
        response.session_epoch = config_.session_epoch;
        response.trade_date = config_.trade_date;
        if (tick_journal_ == nullptr) {
            response.status = static_cast<std::uint16_t>(
                RealtimeCertifiedControlStatusV1::kUnavailable);
            static_cast<void>(::send(
                client,
                &response,
                sizeof(response),
                MSG_NOSIGNAL));
            return;
        }
        const CertifiedTickJournalSessionV1 session =
            tick_journal_->session();
        response.mapping_bytes = session.total_mapping_bytes;
        response.tick_capacity = session.tick_capacity;
        int descriptor = -1;
        if (!tick_journal_->DuplicateReadOnlyDescriptor(
                &descriptor)) {
            response.status = static_cast<std::uint16_t>(
                RealtimeCertifiedControlStatusV1::kInternal);
            static_cast<void>(::send(
                client,
                &response,
                sizeof(response),
                MSG_NOSIGNAL));
            return;
        }
        response.status = static_cast<std::uint16_t>(
            RealtimeCertifiedControlStatusV1::kOk);
        std::array<std::byte, CMSG_SPACE(sizeof(int))> control{};
        iovec vector{};
        vector.iov_base = &response;
        vector.iov_len = sizeof(response);
        msghdr message{};
        message.msg_iov = &vector;
        message.msg_iovlen = 1U;
        message.msg_control = control.data();
        message.msg_controllen = control.size();
        cmsghdr* const ancillary = CMSG_FIRSTHDR(&message);
        if (ancillary == nullptr) {
            CloseDescriptor(&descriptor);
            return;
        }
        ancillary->cmsg_level = SOL_SOCKET;
        ancillary->cmsg_type = SCM_RIGHTS;
        ancillary->cmsg_len = CMSG_LEN(sizeof(int));
        std::memcpy(
            CMSG_DATA(ancillary), &descriptor, sizeof(descriptor));
        static_cast<void>(
            ::sendmsg(client, &message, MSG_NOSIGNAL));
        CloseDescriptor(&descriptor);
    }

    void HandleEventHistoryClient(
        int client,
        const RealtimeCertifiedControlRequestV1& request) noexcept {
        CertifiedOrderEventControlResponseV1 response{};
        response.nonce = request.nonce;
        CopyIdentity(config_.run_id, &response.run_id);
        response.session_epoch = config_.session_epoch;
        response.trade_date = config_.trade_date;
        if (event_journal_ == nullptr) {
            response.status = static_cast<std::uint16_t>(
                RealtimeCertifiedControlStatusV1::kUnavailable);
            static_cast<void>(::send(
                client,
                &response,
                sizeof(response),
                MSG_NOSIGNAL));
            return;
        }
        const CertifiedOrderEventJournalSessionV1 session =
            event_journal_->session();
        response.mapping_bytes = session.total_mapping_bytes;
        response.event_capacity = session.event_capacity;
        response.coverage_flags =
            event_journal_->coverage_flags();
        response.coverage_start_unix_ns =
            event_journal_->coverage_start_unix_ns();
        int descriptor = -1;
        if (!event_journal_->DuplicateReadOnlyDescriptor(
                &descriptor)) {
            response.status = static_cast<std::uint16_t>(
                RealtimeCertifiedControlStatusV1::kInternal);
            static_cast<void>(::send(
                client,
                &response,
                sizeof(response),
                MSG_NOSIGNAL));
            return;
        }
        response.status = static_cast<std::uint16_t>(
            RealtimeCertifiedControlStatusV1::kOk);
        std::array<std::byte, CMSG_SPACE(sizeof(int))> control{};
        iovec vector{};
        vector.iov_base = &response;
        vector.iov_len = sizeof(response);
        msghdr message{};
        message.msg_iov = &vector;
        message.msg_iovlen = 1U;
        message.msg_control = control.data();
        message.msg_controllen = control.size();
        cmsghdr* const ancillary = CMSG_FIRSTHDR(&message);
        if (ancillary == nullptr) {
            CloseDescriptor(&descriptor);
            return;
        }
        ancillary->cmsg_level = SOL_SOCKET;
        ancillary->cmsg_type = SCM_RIGHTS;
        ancillary->cmsg_len = CMSG_LEN(sizeof(int));
        std::memcpy(
            CMSG_DATA(ancillary), &descriptor, sizeof(descriptor));
        static_cast<void>(
            ::sendmsg(client, &message, MSG_NOSIGNAL));
        CloseDescriptor(&descriptor);
    }

    RealtimeCertifiedServiceConfigV1 config_{};
    common::LinuxCpuSetV1 worker_cpu_set_{};
    common::LinuxCpuSetV1 control_cpu_set_{};
    common::LinuxCpuSetV1 tick_history_worker_cpu_set_{};
    std::unique_ptr<BoundedMpmcQueue<HandoffEvent>> queue_;
    std::unique_ptr<CertifiedPayloadLeasePool> payload_leases_;
    std::unique_ptr<
        realtime::NativeSequenceRecoveryCoordinatorV1>
        recovery_;
    std::shared_ptr<CertifiedTickJournalProducerV1>
        tick_journal_;
    std::shared_ptr<CertifiedOrderEventJournalProducerV1>
        event_journal_;
    std::unique_ptr<CertifiedOrderEventHistoryV1> event_history_;
    mutable std::mutex committed_event_generation_mutex_;
    CertifiedOrderEventHistorySnapshotV1
        committed_event_generation_{};

    int memfd_ = -1;
    int read_only_fd_ = -1;
    void* mapping_ = MAP_FAILED;
    std::uint64_t mapping_bytes_ = 0U;
    std::uint64_t certified_ring_offset_ = 0U;
    std::uint64_t channel_state_offset_ = 0U;
    RealtimeCertifiedHeaderV1* header_ = nullptr;
    RealtimeCertifiedTickSlotV1* latest_ = nullptr;
    RealtimeCertifiedTickSlotV1* ring_ = nullptr;
    RealtimeCertifiedChannelStateV1* channel_rows_ = nullptr;

    int listener_fd_ = -1;
    int stop_event_fd_ = -1;
    dev_t socket_device_ = 0;
    ino_t socket_inode_ = 0;
    bool socket_bound_ = false;
    std::thread worker_thread_;
    std::thread tick_history_thread_;
    std::thread control_thread_;
    std::mutex tick_history_writer_mutex_;
    std::mutex control_lifecycle_mutex_;
    std::mutex prefix_call_mutex_;
    std::mutex process_start_coverage_call_mutex_;

    std::map<
        realtime::NativeSequenceChannelV1,
        ChannelRuntime,
        ChannelLess>
        channels_;
    std::vector<ChannelRuntime*> dirty_channels_;
    std::uint64_t canonical_apply_frontier_ = 0U;
    std::uint64_t correction_epoch_ = 1U;
    std::uint64_t gap_opened_count_ = 0U;
    std::uint64_t gap_recovered_count_ = 0U;
    std::uint64_t exact_duplicate_message_count_ = 0U;
    std::uint64_t pending_token_count_ = 0U;
    std::uint32_t healthy_channel_count_ = 0U;
    std::uint32_t gap_channel_count_ = 0U;
    std::uint32_t catching_up_channel_count_ = 0U;
    std::uint32_t frozen_channel_count_ = 0U;
    bool unrepresented_resource_failure_ = false;

    std::atomic<bool> started_{false};
    std::atomic<ThreadStartupStateV1> worker_startup_state_{
        ThreadStartupStateV1::kNotStarted};
    std::atomic<ThreadStartupStateV1> control_startup_state_{
        ThreadStartupStateV1::kNotStarted};
    std::atomic<ThreadStartupStateV1> tick_history_startup_state_{
        ThreadStartupStateV1::kNotStarted};
    std::atomic<int> worker_affinity_system_error_{0};
    std::atomic<int> control_affinity_system_error_{0};
    std::atomic<int> tick_history_affinity_system_error_{0};
    std::atomic<RealtimeCertifiedServiceSnapshotV1::ControlState>
        control_state_{
            RealtimeCertifiedServiceSnapshotV1::ControlState::
                kNotStarted};
    std::atomic<bool> prefix_barrier_started_{false};
    std::atomic<bool> prefix_barrier_completed_{false};
    std::atomic<PrefixCommitPhase> prefix_commit_phase_{
        PrefixCommitPhase::kNotStarted};
    std::atomic<ProcessStartCoveragePhase>
        process_start_coverage_phase_{
            ProcessStartCoveragePhase::kNotStarted};
    std::atomic<bool> prefix_probe_active_{false};
    std::atomic<bool> accepting_{false};
    std::atomic<bool> draining_{false};
    std::atomic<bool> worker_running_{false};
    std::atomic<bool> worker_stop_requested_{false};
    std::atomic<bool> seal_input_requested_{false};
    std::atomic<bool> tick_history_writer_running_{false};
    std::atomic<bool> tick_history_stop_requested_{false};
    std::atomic<bool> tick_history_failed_{false};
    std::atomic<bool> tick_history_paused_for_test_{false};
    std::atomic<bool> tick_history_pause_reached_for_test_{false};
    std::atomic<std::uint64_t> tick_history_frontier_{0U};
    std::atomic<std::uint64_t> maximum_tick_history_lag_{0U};
    std::atomic<bool> globally_frozen_resource_{false};
    std::atomic<std::uint64_t> next_prefix_fence_id_{0U};
    std::atomic<std::uint64_t> completed_prefix_fence_id_{0U};
    // Protected by prefix_call_mutex_. A timed-out marker remains here until
    // a later Probe observes its worker acknowledgement; no second probe is
    // enqueued while this id is nonzero.
    std::uint64_t outstanding_probe_fence_id_ = 0U;
    mutable std::mutex prefix_fence_result_mutex_;
    RealtimeCertifiedPrefixFenceResultV1
        completed_prefix_fence_result_{};
    std::atomic<bool> prefix_commit_paused_for_test_{false};
    std::atomic<bool> prefix_commit_reached_for_test_{false};
    std::atomic<bool> prefix_commit_finished_for_test_{false};
    std::atomic<bool> prefix_probe_paused_for_test_{false};
    std::atomic<bool> prefix_probe_reached_for_test_{false};
    std::atomic<bool>
        prefix_probe_waiting_for_prior_ack_for_test_{false};
    std::atomic<std::uint64_t> wake_epoch_{0U};
    std::atomic<std::uint64_t> enqueued_observations_{0U};
    std::atomic<std::uint64_t> enqueued_applied_records_{0U};
    std::atomic<std::uint64_t> processed_handoffs_{0U};
    std::atomic<std::uint64_t> dropped_handoffs_{0U};
    std::atomic<std::uint64_t> observed_native_message_count_{0U};
    std::atomic<std::uint64_t> conflicting_duplicate_count_{0U};
    std::atomic<std::uint64_t> resource_exhaustion_count_{0U};
    bool input_sealed_by_worker_ = false;
};

RealtimeCertifiedMarketServiceV1::
    RealtimeCertifiedMarketServiceV1(
        std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

RealtimeCertifiedMarketServiceV1::
    ~RealtimeCertifiedMarketServiceV1() = default;

RealtimeCertifiedServiceCreateErrorV1
RealtimeCertifiedMarketServiceV1::Create(
    RealtimeCertifiedServiceConfigV1 config,
    std::shared_ptr<RealtimeCertifiedMarketServiceV1>* output,
    int* system_error_number) noexcept {
    SetSystemError(system_error_number, 0);
    if (output == nullptr) {
        return RealtimeCertifiedServiceCreateErrorV1::kNullOutput;
    }
    output->reset();
    try {
        auto impl = std::make_unique<Impl>(std::move(config));
        const RealtimeCertifiedServiceCreateErrorV1 error =
            impl->Initialize(system_error_number);
        if (error != RealtimeCertifiedServiceCreateErrorV1::kNone) {
            return error;
        }
        output->reset(new RealtimeCertifiedMarketServiceV1(
            std::move(impl)));
        return RealtimeCertifiedServiceCreateErrorV1::kNone;
    } catch (const std::bad_alloc&) {
        return RealtimeCertifiedServiceCreateErrorV1::
            kResourceExhausted;
    } catch (...) {
        return RealtimeCertifiedServiceCreateErrorV1::
            kUnexpectedFailure;
    }
}

bool RealtimeCertifiedMarketServiceV1::Start(
    int* system_error_number) noexcept {
    return impl_ != nullptr &&
           impl_->Start(system_error_number);
}

bool RealtimeCertifiedMarketServiceV1::StartWorker(
    int* system_error_number) noexcept {
    return impl_ != nullptr &&
           impl_->StartWorker(system_error_number);
}

bool RealtimeCertifiedMarketServiceV1::StartControl(
    int* system_error_number) noexcept {
    return impl_ != nullptr &&
           impl_->StartControl(system_error_number);
}

bool RealtimeCertifiedMarketServiceV1::
    FinalizeProcessStartCoverage(
        std::uint64_t coverage_start_unix_ns,
        std::chrono::milliseconds timeout,
        int* system_error_number) noexcept {
    return impl_ != nullptr &&
           impl_->FinalizeProcessStartCoverage(
               coverage_start_unix_ns,
               timeout,
               system_error_number);
}

RealtimeCertifiedPrefixFenceOperationErrorV1
RealtimeCertifiedMarketServiceV1::ProbePrefixFence(
    std::chrono::milliseconds timeout,
    RealtimeCertifiedPrefixFenceResultV1* output,
    int* system_error_number) noexcept {
    if (impl_ == nullptr) {
        if (output != nullptr) {
            *output = {};
            output->operation_error =
                RealtimeCertifiedPrefixFenceOperationErrorV1::
                    kInvalidLifecycle;
        }
        SetSystemError(system_error_number, EINVAL);
        return RealtimeCertifiedPrefixFenceOperationErrorV1::
            kInvalidLifecycle;
    }
    return impl_->ProbePrefixFence(
        timeout, output, system_error_number);
}

bool RealtimeCertifiedMarketServiceV1::WaitForPrefixBarrier(
    std::chrono::milliseconds timeout,
    int* system_error_number) noexcept {
    return impl_ != nullptr &&
           impl_->WaitForPrefixBarrier(
               timeout, nullptr, system_error_number);
}

bool RealtimeCertifiedMarketServiceV1::WaitForPrefixBarrier(
    std::chrono::milliseconds timeout,
    RealtimeCertifiedPrefixFenceResultV1* output,
    int* system_error_number) noexcept {
    if (impl_ == nullptr) {
        if (output != nullptr) {
            *output = {};
            output->operation_error =
                RealtimeCertifiedPrefixFenceOperationErrorV1::
                    kInvalidLifecycle;
        }
        SetSystemError(system_error_number, EINVAL);
        return false;
    }
    return impl_->WaitForPrefixBarrier(
        timeout, output, system_error_number);
}

bool RealtimeCertifiedMarketServiceV1::PublishApplied(
    std::size_t ordinal,
    const market::RealtimeHistoryRecordV1& record) noexcept {
    return impl_ != nullptr &&
           impl_->PublishApplied(ordinal, record);
}

void RealtimeCertifiedMarketServiceV1::MarkCoverageLost() noexcept {
    if (impl_ != nullptr) {
        impl_->MarkCoverageLost();
    }
}

void RealtimeCertifiedMarketServiceV1::
    QuiesceRecordReferences() noexcept {
    if (impl_ != nullptr) {
        impl_->QuiesceRecordReferences();
    }
}

void RealtimeCertifiedMarketServiceV1::ObserveNativeSequence(
    const realtime::NativeSequenceObservationV1& observation)
    noexcept {
    if (impl_ != nullptr) {
        impl_->ObserveNativeSequence(observation);
    }
}

void RealtimeCertifiedMarketServiceV1::
    MarkNativeSequenceObservationFailure(
        realtime::NativeSequenceObservationFailureV1 failure,
        const sdk::MessageKey& message_key) noexcept {
    if (impl_ != nullptr) {
        impl_->MarkNativeSequenceObservationFailure(
            failure, message_key);
    }
}

void RealtimeCertifiedMarketServiceV1::MarkDraining() noexcept {
    if (impl_ != nullptr) {
        impl_->MarkDraining();
    }
}

void RealtimeCertifiedMarketServiceV1::MarkStoppedClean() noexcept {
    if (impl_ != nullptr) {
        impl_->MarkStoppedClean();
    }
}

void RealtimeCertifiedMarketServiceV1::StopControl() noexcept {
    if (impl_ != nullptr) {
        impl_->StopControl();
    }
}

bool RealtimeCertifiedMarketServiceV1::ControlRunningConfirmed()
    noexcept {
    return impl_ != nullptr && impl_->ControlRunningConfirmed();
}

RealtimeCertifiedServiceSnapshotV1::ControlState
RealtimeCertifiedMarketServiceV1::ControlStateSnapshot()
    const noexcept {
    return impl_ == nullptr
               ? RealtimeCertifiedServiceSnapshotV1::ControlState::
                     kFailed
               : impl_->ControlStateSnapshot();
}

RealtimeCertifiedServiceSnapshotV1
RealtimeCertifiedMarketServiceV1::Snapshot() const noexcept {
    return impl_ == nullptr
               ? RealtimeCertifiedServiceSnapshotV1{}
               : impl_->Snapshot();
}

bool RealtimeCertifiedMarketServiceV1::ReadLatestForTest(
    std::size_t ordinal,
    RealtimeCertifiedTickEnvelopeV1* output) const noexcept {
    return impl_ != nullptr &&
           impl_->ReadLatestForTest(ordinal, output);
}

bool RealtimeCertifiedMarketServiceV1::ReadCanonicalForTest(
    std::uint64_t sequence,
    RealtimeCertifiedTickEnvelopeV1* output) const noexcept {
    return impl_ != nullptr &&
           impl_->ReadCanonicalForTest(sequence, output);
}

bool RealtimeCertifiedMarketServiceV1::
    DuplicateReadOnlyDescriptorForTest(int* output) const noexcept {
    return impl_ != nullptr &&
           impl_->DuplicateReadOnlyDescriptorForTest(output);
}

std::uint64_t RealtimeCertifiedMarketServiceV1::
    HeaderPublishTagForTest() const noexcept {
    return impl_ == nullptr ? 0U : impl_->HeaderPublishTagForTest();
}

std::uint64_t RealtimeCertifiedMarketServiceV1::
    WorkerWakeEpochForTest() const noexcept {
    return impl_ == nullptr ? 0U : impl_->WorkerWakeEpochForTest();
}

bool RealtimeCertifiedMarketServiceV1::WaitUntilIdleForTest(
    std::chrono::milliseconds timeout) const noexcept {
    return impl_ != nullptr &&
           impl_->WaitUntilIdle(timeout);
}

bool RealtimeCertifiedMarketServiceV1::FailControlForTest() noexcept {
    return impl_ != nullptr && impl_->FailControlForTest();
}

void RealtimeCertifiedMarketServiceV1::SetPrefixProbePausedForTest(
    bool paused) noexcept {
    if (impl_ != nullptr) {
        impl_->SetPrefixProbePausedForTest(paused);
    }
}

bool RealtimeCertifiedMarketServiceV1::PrefixProbeReachedForTest()
    const noexcept {
    return impl_ != nullptr && impl_->PrefixProbeReachedForTest();
}

bool RealtimeCertifiedMarketServiceV1::
    PrefixProbeWaitingForPriorAckForTest() const noexcept {
    return impl_ != nullptr &&
           impl_->PrefixProbeWaitingForPriorAckForTest();
}

void RealtimeCertifiedMarketServiceV1::SetPrefixCommitPausedForTest(
    bool paused) noexcept {
    if (impl_ != nullptr) {
        impl_->SetPrefixCommitPausedForTest(paused);
    }
}

bool RealtimeCertifiedMarketServiceV1::PrefixCommitReachedForTest()
    const noexcept {
    return impl_ != nullptr && impl_->PrefixCommitReachedForTest();
}

bool RealtimeCertifiedMarketServiceV1::PrefixCommitFinishedForTest()
    const noexcept {
    return impl_ != nullptr && impl_->PrefixCommitFinishedForTest();
}

bool RealtimeCertifiedMarketServiceV1::StartupPrefixRecoveredForTest()
    const noexcept {
    return impl_ != nullptr && impl_->StartupPrefixRecoveredForTest();
}

bool RealtimeCertifiedMarketServiceV1::ReadWorkerCpuSetForTest(
    common::LinuxCpuSetV1* output,
    int* system_error_number) noexcept {
    return impl_ != nullptr &&
           impl_->ReadWorkerCpuSetForTest(
               output, system_error_number);
}

bool RealtimeCertifiedMarketServiceV1::ReadControlCpuSetForTest(
    common::LinuxCpuSetV1* output,
    int* system_error_number) noexcept {
    return impl_ != nullptr &&
           impl_->ReadControlCpuSetForTest(
               output, system_error_number);
}

bool RealtimeCertifiedMarketServiceV1::
    ReadTickHistoryWorkerCpuSetForTest(
        common::LinuxCpuSetV1* output,
        int* system_error_number) noexcept {
    return impl_ != nullptr &&
           impl_->ReadTickHistoryWorkerCpuSetForTest(
               output, system_error_number);
}

void RealtimeCertifiedMarketServiceV1::
    SetTickHistoryWriterPausedForTest(bool paused) noexcept {
    if (impl_ != nullptr) {
        impl_->SetTickHistoryWriterPausedForTest(paused);
    }
}

bool RealtimeCertifiedMarketServiceV1::
    TickHistoryWriterPauseReachedForTest() const noexcept {
    return impl_ != nullptr &&
           impl_->TickHistoryWriterPauseReachedForTest();
}

CertifiedOrderEventHistoryErrorV1
RealtimeCertifiedMarketServiceV1::AcquireEventGeneration(
    CertifiedOrderEventHistorySnapshotV1* output) const noexcept {
    return impl_ == nullptr
               ? CertifiedOrderEventHistoryErrorV1::kFailed
               : impl_->AcquireEventGeneration(output);
}

std::uint64_t
RealtimeCertifiedMarketServiceV1::mapping_bytes() const noexcept {
    return impl_ == nullptr ? 0U : impl_->mapping_bytes();
}

std::uint64_t
RealtimeCertifiedMarketServiceV1::handoff_queue_capacity()
    const noexcept {
    return impl_ == nullptr ? 0U : impl_->handoff_queue_capacity();
}

const std::filesystem::path&
RealtimeCertifiedMarketServiceV1::control_socket_path()
    const noexcept {
    static const std::filesystem::path empty;
    return impl_ == nullptr ? empty : impl_->socket_path();
}

}  // namespace l2flow::ipc
