#include "l2flow/ipc/realtime_certified_service_v1.h"

#include "l2flow/ipc/certified_order_event_journal_v1.h"
#include "l2flow/ipc/certified_order_event_history_v1.h"
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

static_assert(sizeof(std::size_t) <= sizeof(std::uint64_t));

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
};

struct HandoffEvent final {
    HandoffKind kind = HandoffKind::kObservation;
    std::array<std::uint8_t, 7U> reserved{};
    realtime::NativeSequenceObservationV1 observation{};
    std::size_t ordinal = 0U;
    const market::RealtimeHistoryRecordV1* record = nullptr;
};
static_assert(std::is_trivially_copyable_v<HandoffEvent>);

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

    [[nodiscard]] bool TryPush(const Value& value) noexcept {
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
            kEventProjectorCreateFailed:
            return "event_projector_create_failed";
        case RealtimeCertifiedServiceCreateErrorV1::kResourceExhausted:
            return "resource_exhausted";
        case RealtimeCertifiedServiceCreateErrorV1::kUnexpectedFailure:
            return "unexpected_failure";
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
            config_.channel_capacity == 0U ||
            !IsPowerOfTwo(config_.handoff_queue_capacity) ||
            config_.handoff_queue_capacity >
                std::numeric_limits<std::size_t>::max() ||
            config_.maximum_pending_entries == 0U ||
            config_.maximum_pending_entries >
                std::numeric_limits<std::size_t>::max() ||
            config_.maximum_pending_entries_per_channel == 0U ||
            config_.maximum_pending_entries_per_channel >
                config_.maximum_pending_entries ||
            config_.certified_duplicate_retention_entries >
                std::numeric_limits<std::size_t>::max() ||
            config_.maximum_reorder_span == 0U ||
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
        recovery_config.maximum_reorder_span =
            config_.maximum_reorder_span;
        // Both supported exchange-native domains are documented as starting
        // from one and production requires capture from market open. Never
        // bless a later first packet as complete: it is a gap until 1..N-1
        // arrive. A resumed trusted checkpoint would also require restored
        // Event projection state, so this fresh service supplies none.
        recovery_config.expected_origin_sequence = 1U;
        recovery_config.trusted_checkpoint_sequence = 0U;
        if (realtime::NativeSequenceRecoveryCoordinatorV1::Create(
                recovery_config, &recovery_) !=
                realtime::NativeSequenceRecoveryCreateErrorV1::kNone ||
            recovery_ == nullptr) {
            return RealtimeCertifiedServiceCreateErrorV1::
                kRecoveryCreateFailed;
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

    [[nodiscard]] bool Start(int* system_error_number) noexcept {
        SetSystemError(system_error_number, 0);
        bool expected = false;
        if (!started_.compare_exchange_strong(
                expected, true, std::memory_order_acq_rel)) {
            return false;
        }
        accepting_.store(true, std::memory_order_release);
        worker_running_.store(true, std::memory_order_release);
        control_running_.store(true, std::memory_order_release);
        try {
            worker_thread_ = std::thread([this]() noexcept {
                WorkerLoop();
            });
            control_thread_ = std::thread([this]() noexcept {
                ControlLoop();
            });
            return true;
        } catch (...) {
            accepting_.store(false, std::memory_order_release);
            worker_stop_requested_.store(
                true, std::memory_order_release);
            worker_running_.store(false, std::memory_order_release);
            control_running_.store(false, std::memory_order_release);
            wake_epoch_.fetch_add(1U, std::memory_order_release);
            wake_epoch_.notify_all();
            SignalStopEvent();
            if (worker_thread_.joinable()) {
                worker_thread_.join();
            }
            if (control_thread_.joinable()) {
                control_thread_.join();
            }
            SetSystemError(system_error_number, EAGAIN);
            return false;
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
        if (!queue_->TryPush(event)) {
            FreezeGlobalResource();
            return true;
        }
        if (!IncrementSaturating(&enqueued_applied_records_)) {
            FreezeGlobalResource();
        }
        WakeWorker();
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
        accepting_.store(false, std::memory_order_release);
        worker_stop_requested_.store(true, std::memory_order_release);
        WakeWorker();
        if (worker_thread_.joinable()) {
            worker_thread_.join();
        }
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
        if (!queue_->TryPush(event)) {
            FreezeGlobalResource();
            return;
        }
        if (!IncrementSaturating(&enqueued_observations_)) {
            FreezeGlobalResource();
        }
        WakeWorker();
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
        accepting_.store(false, std::memory_order_release);
        worker_stop_requested_.store(true, std::memory_order_release);
        WakeWorker();
        if (worker_thread_.joinable()) {
            worker_thread_.join();
        }
        worker_running_.store(false, std::memory_order_release);
        PublishHeader(RealtimeCertifiedStateV1::kStopped);
    }

    void StopControl() noexcept {
        accepting_.store(false, std::memory_order_release);
        worker_stop_requested_.store(true, std::memory_order_release);
        control_running_.store(false, std::memory_order_release);
        WakeWorker();
        SignalStopEvent();
        if (worker_thread_.joinable()) {
            worker_thread_.join();
        }
        if (control_thread_.joinable()) {
            control_thread_.join();
        }
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
        result.wire_snapshot_consistent = stable_snapshot;
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

    [[nodiscard]] bool WaitUntilIdleForTest(
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
                (queue_ == nullptr || queue_->Empty())) {
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

    [[nodiscard]] const std::filesystem::path& socket_path()
        const noexcept {
        return config_.control_socket_path;
    }

private:
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
                std::size_t batch = 0U;
                HandoffEvent event{};
                while (batch < 1024U && queue_->TryPop(&event)) {
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
                        std::memory_order_acquire)) {
                    DrainCertified();
                }
                PublishHeader(DeriveAggregateState());
                if (worker_stop_requested_.load(
                        std::memory_order_acquire) &&
                    queue_->Empty()) {
                    break;
                }
                if (batch == 0U) {
                    const std::uint64_t epoch =
                        wake_epoch_.load(std::memory_order_acquire);
                    if (!worker_stop_requested_.load(
                            std::memory_order_acquire) &&
                        queue_->Empty()) {
                        wake_epoch_.wait(
                            epoch, std::memory_order_acquire);
                    }
                }
            }
        } catch (...) {
            FreezeGlobalResource();
            PublishHeader(
                RealtimeCertifiedStateV1::kFrozenResource);
        }
        worker_running_.store(false, std::memory_order_release);
    }

    void HandleHandoff(const HandoffEvent& event) {
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
        RealtimeWireTickPayloadV2 payload{};
        if (!ProjectRealtimeWireTickPayloadV2(
                *record, ordinal, &payload) ||
            !RealtimeCertifiedTickPayloadCanonicalV1(payload) ||
            payload.common.ingress_sequence == 0U) {
            FreezeGlobalResource();
            return;
        }
        const realtime::NativeSequenceDescriptorV1 descriptor =
            DescriptorFromPayload(payload);
        const sdk::MessageKey message_key =
            MessageKeyFromPayload(payload);
        static_assert(
            sizeof(std::uintptr_t) <= sizeof(std::uint64_t));
        const std::uint64_t cookie = static_cast<std::uint64_t>(
            reinterpret_cast<std::uintptr_t>(record));
        if (cookie == 0U) {
            FreezeGlobalResource();
            return;
        }

        const RealtimeWireTickPayloadV2 canonical =
            CanonicalBusinessPayload(payload);
        realtime::NativeSequenceRecoveryApplyResultV1 result{};
        const realtime::NativeSequenceRecoveryApplyErrorV1 error =
            recovery_->MarkTargetApplied(
                descriptor,
                message_key,
                std::as_bytes(std::span(&canonical, 1U)),
                cookie,
                &result);
        if (error !=
                realtime::NativeSequenceRecoveryApplyErrorV1::kNone &&
            error !=
                realtime::NativeSequenceRecoveryApplyErrorV1::
                    kChannelFrozen) {
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

    void DrainCertified() {
        for (;;) {
            realtime::NativeSequenceCertifiedReadyV1 ready{};
            const realtime::NativeSequenceRecoveryPollErrorV1 error =
                recovery_->PollCertified(&ready);
            if (error ==
                realtime::NativeSequenceRecoveryPollErrorV1::
                    kNotReady) {
                return;
            }
            if (error !=
                realtime::NativeSequenceRecoveryPollErrorV1::kNone) {
                FreezeGlobalResource();
                return;
            }
            if (ready.record_class ==
                realtime::NativeSequenceRecoveryRecordClassV1::
                    kFiltered) {
                const auto commit_error =
                    recovery_->CommitCertified(ready.token);
                if (commit_error !=
                    realtime::
                        NativeSequenceRecoveryCommitErrorV1::kNone) {
                    FreezeGlobalResource();
                    return;
                }
                EnsureChannel(ready.descriptor.domain);
                UpdateChannel(ready.descriptor.domain);
                PublishHeader(DeriveAggregateState());
                continue;
            }
            if (ready.record_class !=
                realtime::NativeSequenceRecoveryRecordClassV1::
                    kTarget) {
                FreezeGlobalResource();
                return;
            }
            const auto pointer_value = static_cast<std::uintptr_t>(
                ready.applied_cookie);
            const auto* const record =
                reinterpret_cast<
                    const market::RealtimeHistoryRecordV1*>(
                    pointer_value);
            if (record == nullptr ||
                record->instrument_id() == 0U) {
                FreezeGlobalResource();
                return;
            }
            const std::size_t ordinal =
                static_cast<std::size_t>(
                    record->instrument_id() - 1U);
            RealtimeWireTickPayloadV2 payload{};
            if (!ProjectRealtimeWireTickPayloadV2(
                    *record,
                    ordinal,
                    &payload) ||
                !RealtimeCertifiedTickPayloadCanonicalV1(payload) ||
                DescriptorFromPayload(payload) != ready.descriptor ||
                !(MessageKeyFromPayload(payload) ==
                  ready.message_key) ||
                canonical_apply_frontier_ ==
                    std::numeric_limits<std::uint64_t>::max()) {
                FreezeGlobalResource();
                return;
            }
            const std::uint64_t next =
                canonical_apply_frontier_ + 1U;
            std::uint64_t certified_ns = 0U;
            if (!ReadMonotonicNs(&certified_ns)) {
                FreezeGlobalResource();
                return;
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
                return;
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
                return;
            }
            // Event projection/storage is the final resource-fallible part of
            // the transaction. Run it before touching a previously published
            // latest/ring slot. On failure both public Tick and Event views
            // therefore retain their same last-good canonical frontier; the
            // coordinator is internal and the service freezes terminally.
            if (event_history_->AppendCertifiedTick(payload, next) !=
                CertifiedOrderEventHistoryErrorV1::kNone) {
                FreezeGlobalResource();
                return;
            }
            CertifiedOrderEventHistorySnapshotV1 next_event_generation{};
            if (event_history_->AcquireGeneration(
                    &next_event_generation) !=
                    CertifiedOrderEventHistoryErrorV1::kNone ||
                !next_event_generation.valid() ||
                next_event_generation.generation()
                        .input_frontier
                        .canonical_apply_sequence !=
                    next) {
                FreezeGlobalResource();
                return;
            }
            // Both slots were preflighted above and this is their sole writer,
            // so readers cannot make either publish fail. The header frontier
            // remains the external visibility gate until both stores finish.
            if (!PublishSlot(&ring_[ring_index], envelope) ||
                !PublishSlot(
                    &latest_[payload.common.ordinal],
                    envelope)) {
                FreezeGlobalResource();
                return;
            }
            canonical_apply_frontier_ = next;
            EnsureChannel(ready.descriptor.domain);
            auto channel = channels_.find(ready.descriptor.domain);
            if (channel != channels_.end()) {
                ++channel->second.certified_tick_count;
                channel->second.last_canonical_apply_sequence =
                    next;
            }
            UpdateChannel(ready.descriptor.domain);
            // Commit each recovered Tick as one externally visible prefix
            // step. Besides lowering reader catch-up latency, this prevents a
            // long drain from overwriting multiple ring positions while the
            // public retention frontier still describes the old window.
            PublishHeader(DeriveAggregateState());
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
                return;
            }
            {
                const std::lock_guard<std::mutex> lock(
                    committed_event_generation_mutex_);
                committed_event_generation_ =
                    std::move(next_event_generation);
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
        if (!runtime.gap_active && new_gap) {
            if (gap_opened_count_ ==
                    std::numeric_limits<std::uint64_t>::max() ||
                runtime.gap_opened_count ==
                    std::numeric_limits<std::uint64_t>::max()) {
                FreezeGlobalResource();
                return;
            }
            ++gap_opened_count_;
            ++runtime.gap_opened_count;
        } else if (runtime.gap_active && !new_gap) {
            if (gap_recovered_count_ ==
                    std::numeric_limits<std::uint64_t>::max() ||
                runtime.gap_recovered_count ==
                    std::numeric_limits<std::uint64_t>::max() ||
                correction_epoch_ ==
                    std::numeric_limits<std::uint64_t>::max()) {
                FreezeGlobalResource();
                return;
            }
            ++gap_recovered_count_;
            ++runtime.gap_recovered_count;
            ++correction_epoch_;
        }
        runtime.gap_active = new_gap;
        if (!runtime.conflict_freeze_counted &&
            snapshot.state ==
                realtime::NativeSequenceRecoveryChannelStateV1::
                    kFrozenConflict) {
            runtime.conflict_freeze_counted = true;
            if (!IncrementSaturating(
                    &conflicting_duplicate_count_)) {
                FreezeGlobalResource();
                return;
            }
        }
        if (!runtime.resource_freeze_counted &&
            snapshot.state ==
                realtime::NativeSequenceRecoveryChannelStateV1::
                    kFrozenResource) {
            runtime.resource_freeze_counted = true;
            if (!IncrementSaturating(
                    &resource_exhaustion_count_)) {
                FreezeGlobalResource();
                return;
            }
        }
        runtime.snapshot = snapshot;
        runtime.initialized = true;
        runtime.wire_dirty = true;
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
            static_cast<std::int64_t>(
                runtime.snapshot.domain.channel),
            std::memory_order_relaxed);
        Atomic(row.market).store(
            static_cast<std::uint8_t>(
                runtime.snapshot.domain.market),
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
        bool conflict = false;
        bool resource = false;
        bool gap = false;
        bool catching_up = false;
        for (const auto& [domain, runtime] : channels_) {
            static_cast<void>(domain);
            switch (runtime.snapshot.state) {
                case realtime::
                    NativeSequenceRecoveryChannelStateV1::
                        kFrozenConflict:
                    conflict = true;
                    break;
                case realtime::
                    NativeSequenceRecoveryChannelStateV1::
                        kFrozenResource:
                    resource = true;
                    break;
                case realtime::
                    NativeSequenceRecoveryChannelStateV1::
                        kRepairing:
                    gap = true;
                    break;
                case realtime::
                    NativeSequenceRecoveryChannelStateV1::
                        kCatchingUp:
                    catching_up = true;
                    break;
                case realtime::
                    NativeSequenceRecoveryChannelStateV1::
                        kHealthy:
                    break;
            }
        }
        if (resource) {
            return RealtimeCertifiedStateV1::kFrozenResource;
        }
        if (conflict) {
            return RealtimeCertifiedStateV1::kFrozenConflict;
        }
        if (gap) {
            return RealtimeCertifiedStateV1::kGapOpen;
        }
        if (catching_up) {
            return RealtimeCertifiedStateV1::kCatchingUp;
        }
        return channels_.empty()
                   ? RealtimeCertifiedStateV1::kNoData
                   : RealtimeCertifiedStateV1::kContiguous;
    }

    void PublishHeader(
        RealtimeCertifiedStateV1 requested_state) noexcept {
        if (header_ == nullptr) {
            return;
        }
        std::uint64_t heartbeat = 0U;
        if (!ReadMonotonicNs(&heartbeat)) {
            heartbeat = Atomic(header_->heartbeat_monotonic_ns)
                            .load(std::memory_order_relaxed);
        }
        const realtime::NativeSequenceRecoverySnapshotV1 recovery =
            recovery_ == nullptr
                ? realtime::NativeSequenceRecoverySnapshotV1{}
                : recovery_->Snapshot();
        std::uint32_t gap_count = 0U;
        std::uint32_t catching_count = 0U;
        std::uint32_t frozen_count = 0U;
        std::uint64_t duplicate_count = 0U;
        bool duplicate_count_overflow = false;
        std::uint64_t pending_count =
            static_cast<std::uint64_t>(recovery.pending_entries);
        for (const auto& [domain, runtime] : channels_) {
            static_cast<void>(domain);
            if (!CheckedAdd(
                    duplicate_count,
                    runtime.snapshot.exact_duplicate_applications,
                    &duplicate_count)) {
                duplicate_count_overflow = true;
                break;
            }
            switch (runtime.snapshot.state) {
                case realtime::
                    NativeSequenceRecoveryChannelStateV1::
                        kRepairing:
                    ++gap_count;
                    break;
                case realtime::
                    NativeSequenceRecoveryChannelStateV1::
                        kCatchingUp:
                    ++catching_count;
                    break;
                case realtime::
                    NativeSequenceRecoveryChannelStateV1::
                        kFrozenConflict:
                case realtime::
                    NativeSequenceRecoveryChannelStateV1::
                        kFrozenResource:
                    ++frozen_count;
                    break;
                case realtime::
                    NativeSequenceRecoveryChannelStateV1::
                        kHealthy:
                    break;
            }
        }
        std::atomic_ref<std::uint64_t> tag =
            Atomic(header_->status_publish_tag);
        const std::uint64_t stable =
            tag.load(std::memory_order_acquire);
        if ((stable & 1U) != 0U ||
            stable >
                std::numeric_limits<std::uint64_t>::max() - 2U) {
            return;
        }
        std::uint64_t expected = stable;
        if (!tag.compare_exchange_strong(
                expected,
                stable + 1U,
                std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            return;
        }
        std::atomic_thread_fence(std::memory_order_release);
        const std::uint64_t aggregate_commit_tag = stable + 2U;
        if (duplicate_count_overflow) {
            // Preserve the previously canonical counter rather than
            // publishing a wrapped value. The terminal resource state makes
            // the exhausted diagnostic range explicit.
            duplicate_count =
                Atomic(header_->exact_duplicate_message_count)
                    .load(std::memory_order_relaxed);
            FreezeGlobalResource();
            requested_state =
                RealtimeCertifiedStateV1::kFrozenResource;
        }
        for (auto& [domain, runtime] : channels_) {
            static_cast<void>(domain);
            if (!runtime.wire_dirty) {
                continue;
            }
            if (!PublishChannel(runtime, aggregate_commit_tag)) {
                // The header is already in its private odd epoch. Preserve a
                // coherent external cut by committing a terminal resource
                // state; no partially prepared row is visible until the
                // header reaches aggregate_commit_tag below.
                FreezeGlobalResource();
                requested_state =
                    RealtimeCertifiedStateV1::kFrozenResource;
                break;
            }
            runtime.wire_dirty = false;
        }
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
            unrepresented_resource_failure_ ||
            resource_exhaustion_count != 0U) {
            requested_state =
                RealtimeCertifiedStateV1::kFrozenResource;
        } else if (requested_state ==
                   RealtimeCertifiedStateV1::kStopped) {
            // A clean stop is terminal only for a healthy/no-data stream.
            // An unresolved correctness condition remains the reader-facing
            // terminal state so shutdown cannot erase why certification
            // stopped at its last correct prefix.
            bool conflict = false;
            bool resource = false;
            for (const auto& [domain, runtime] : channels_) {
                static_cast<void>(domain);
                conflict = conflict ||
                    runtime.snapshot.state ==
                        realtime::
                            NativeSequenceRecoveryChannelStateV1::
                                kFrozenConflict;
                resource = resource ||
                    runtime.snapshot.state ==
                        realtime::
                            NativeSequenceRecoveryChannelStateV1::
                                kFrozenResource;
            }
            if (resource) {
                requested_state =
                    RealtimeCertifiedStateV1::kFrozenResource;
            } else if (conflict) {
                requested_state =
                    RealtimeCertifiedStateV1::kFrozenConflict;
            } else if (gap_count != 0U) {
                requested_state =
                    RealtimeCertifiedStateV1::kGapOpen;
            } else if (catching_count != 0U) {
                requested_state =
                    RealtimeCertifiedStateV1::kCatchingUp;
            }
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
            duplicate_count, std::memory_order_relaxed);
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
            pending_count, std::memory_order_relaxed);
        Atomic(header_->aggregate_state).store(
            static_cast<std::uint32_t>(requested_state),
            std::memory_order_relaxed);
        Atomic(header_->channel_state_count).store(
            static_cast<std::uint32_t>(channels_.size()),
            std::memory_order_relaxed);
        Atomic(header_->gap_open_channel_count).store(
            gap_count, std::memory_order_relaxed);
        Atomic(header_->catching_up_channel_count).store(
            catching_count, std::memory_order_relaxed);
        Atomic(header_->frozen_channel_count).store(
            frozen_count, std::memory_order_relaxed);
        tag.store(aggregate_commit_tag, std::memory_order_release);
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

    void ControlLoop() noexcept {
        std::array<pollfd, 2U> descriptors{};
        descriptors[0].fd = listener_fd_;
        descriptors[0].events = POLLIN;
        descriptors[1].fd = stop_event_fd_;
        descriptors[1].events = POLLIN;
        while (control_running_.load(std::memory_order_acquire)) {
            int result = -1;
            do {
                result = ::poll(
                    descriptors.data(),
                    static_cast<nfds_t>(descriptors.size()),
                    -1);
            } while (result < 0 && errno == EINTR);
            if (result <= 0 ||
                (descriptors[1].revents &
                 (POLLIN | POLLERR | POLLHUP)) != 0) {
                break;
            }
            if ((descriptors[0].revents & POLLIN) == 0) {
                continue;
            }
            for (;;) {
                const int client = ::accept4(
                    listener_fd_,
                    nullptr,
                    nullptr,
                    SOCK_CLOEXEC);
                if (client < 0) {
                    if (errno == EINTR) {
                        continue;
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
                HandleClient(client);
                int close_result = -1;
                do {
                    close_result = ::close(client);
                } while (close_result < 0 && errno == EINTR);
            }
        }
    }

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
    std::unique_ptr<BoundedMpmcQueue<HandoffEvent>> queue_;
    std::unique_ptr<
        realtime::NativeSequenceRecoveryCoordinatorV1>
        recovery_;
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
    std::thread control_thread_;

    std::map<
        realtime::NativeSequenceChannelV1,
        ChannelRuntime,
        ChannelLess>
        channels_;
    std::uint64_t canonical_apply_frontier_ = 0U;
    std::uint64_t correction_epoch_ = 1U;
    std::uint64_t gap_opened_count_ = 0U;
    std::uint64_t gap_recovered_count_ = 0U;
    bool unrepresented_resource_failure_ = false;

    std::atomic<bool> started_{false};
    std::atomic<bool> accepting_{false};
    std::atomic<bool> draining_{false};
    std::atomic<bool> worker_running_{false};
    std::atomic<bool> worker_stop_requested_{false};
    std::atomic<bool> control_running_{false};
    std::atomic<bool> globally_frozen_resource_{false};
    std::atomic<std::uint64_t> wake_epoch_{0U};
    std::atomic<std::uint64_t> enqueued_observations_{0U};
    std::atomic<std::uint64_t> enqueued_applied_records_{0U};
    std::atomic<std::uint64_t> processed_handoffs_{0U};
    std::atomic<std::uint64_t> dropped_handoffs_{0U};
    std::atomic<std::uint64_t> observed_native_message_count_{0U};
    std::atomic<std::uint64_t> conflicting_duplicate_count_{0U};
    std::atomic<std::uint64_t> resource_exhaustion_count_{0U};
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

bool RealtimeCertifiedMarketServiceV1::WaitUntilIdleForTest(
    std::chrono::milliseconds timeout) const noexcept {
    return impl_ != nullptr &&
           impl_->WaitUntilIdleForTest(timeout);
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

const std::filesystem::path&
RealtimeCertifiedMarketServiceV1::control_socket_path()
    const noexcept {
    static const std::filesystem::path empty;
    return impl_ == nullptr ? empty : impl_->socket_path();
}

}  // namespace l2flow::ipc
