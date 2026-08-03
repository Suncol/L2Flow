#include "l2flow/ipc/realtime_partial_order_event_service_v2.h"

#include "l2flow/common/linux_thread_affinity_v1.h"
#include "l2flow/control/quality_flags_v1.h"
#include "l2flow/ipc/certified_order_event_history_v1.h"
#include "l2flow/ipc/order_event_wire_adapter_v2.h"
#include "l2flow/ipc/realtime_certified_wire_v1.h"
#include "l2flow/ipc/realtime_wire_projection_v2.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <mutex>
#include <span>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace l2flow::ipc {
namespace {

using Clock = std::chrono::steady_clock;

static_assert(
    std::atomic<PartialOrderEventLastErrorV2>::is_always_lock_free);
static_assert(std::atomic<std::uint64_t>::is_always_lock_free);

void SetSystemError(int* output, int value) noexcept {
    if (output != nullptr) {
        *output = value;
    }
}

[[nodiscard]] bool IdentityNonzero(
    const common::Identity128& identity) noexcept {
    return std::any_of(
        identity.begin(), identity.end(), [](std::byte value) noexcept {
            return value != std::byte{0};
        });
}

[[nodiscard]] bool IsPowerOfTwo(std::uint64_t value) noexcept {
    return value != 0U && (value & (value - 1U)) == 0U;
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

[[nodiscard]] bool ReadMonotonicNs(std::uint64_t* output) noexcept {
    if (output == nullptr) {
        return false;
    }
    const auto now = Clock::now().time_since_epoch();
    const auto nanos =
        std::chrono::duration_cast<std::chrono::nanoseconds>(now).count();
    if (nanos <= 0) {
        return false;
    }
    *output = static_cast<std::uint64_t>(nanos);
    return true;
}

[[nodiscard]] constexpr std::uint64_t PackPublishedStatus(
    PartialOrderEventServiceStateV2 state,
    PartialOrderEventLastErrorV2 error) noexcept {
    return (static_cast<std::uint64_t>(state) << 32U) |
           static_cast<std::uint64_t>(error);
}

[[nodiscard]] constexpr PartialOrderEventServiceStateV2
UnpackPublishedState(std::uint64_t packed) noexcept {
    return static_cast<PartialOrderEventServiceStateV2>(
        static_cast<std::uint32_t>(packed >> 32U));
}

[[nodiscard]] constexpr PartialOrderEventLastErrorV2
UnpackPublishedError(std::uint64_t packed) noexcept {
    return static_cast<PartialOrderEventLastErrorV2>(
        static_cast<std::uint32_t>(packed));
}

[[nodiscard]] bool AddSaturating(
    std::atomic<std::uint64_t>* value,
    std::uint64_t increment) noexcept {
    std::uint64_t current = value->load(std::memory_order_relaxed);
    for (;;) {
        if (increment >
            std::numeric_limits<std::uint64_t>::max() - current) {
            return false;
        }
        if (value->compare_exchange_weak(
                current,
                current + increment,
                std::memory_order_release,
                std::memory_order_relaxed)) {
            return true;
        }
    }
}

[[nodiscard]] bool IncrementSaturating(
    std::atomic<std::uint64_t>* value) noexcept {
    return AddSaturating(value, 1U);
}

void StoreMaximum(
    std::atomic<std::uint64_t>* value,
    std::uint64_t candidate) noexcept {
    std::uint64_t current = value->load(std::memory_order_relaxed);
    while (current < candidate &&
           !value->compare_exchange_weak(
               current,
               candidate,
               std::memory_order_release,
               std::memory_order_relaxed)) {
    }
}

[[nodiscard]] bool ApplyOptionalCurrentThreadCpuSet(
    const common::LinuxCpuSetV1& requested,
    int* system_error_number) noexcept {
    SetSystemError(system_error_number, 0);
    if (requested.empty()) {
        return true;
    }
    return common::ApplyCurrentLinuxThreadAffinityExactV1(
               requested, nullptr, system_error_number) ==
           common::LinuxThreadAffinityErrorV1::kNone;
}

[[nodiscard]] realtime::NativeSequenceDescriptorV1 DescriptorFromPayload(
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

void CanonicalizeBusinessPayload(
    RealtimeWireTickPayloadV2* payload) noexcept {
    payload->common.source_sequence = 0U;
    payload->common.ingress_sequence = 0U;
    payload->common.tick_stream_sequence = 0U;
    payload->common.vendor_sequence_id = 0U;
    payload->common.recv_realtime_ns = 0;
    payload->common.recv_monotonic_ns = 0;
    payload->common.vendor_local_time_raw = 0U;
    payload->common.source_stream_id = 0U;
    payload->common.source_slot = 0U;
    payload->common.quality_flags &=
        ~l2flow::control::QualityBit(
            l2flow::control::QualityFlagV1::kNullValuePresent);
    payload->common.market_notices &=
        ~market::MarketNoticeBitV1(
            market::MarketNoticeV1::kVendorLocalTimeInvalid);
}

[[nodiscard]] const market::DecodedMarketCommonV1* StoredCommon(
    const market::RealtimeHistoryRecordV1& record) noexcept {
    return std::visit(
        [](const auto* event) noexcept
            -> const market::DecodedMarketCommonV1* {
            return event == nullptr ? nullptr : &event->common;
        },
        record.event());
}

[[nodiscard]] bool RestoreArrivalMetadata(
    const market::RealtimeHistoryRecordV1& record,
    RealtimeWireTickPayloadV2* payload) noexcept {
    constexpr std::uint64_t null_value_mask =
        l2flow::control::QualityBit(
            l2flow::control::QualityFlagV1::kNullValuePresent);
    constexpr std::uint64_t vendor_time_mask =
        market::MarketNoticeBitV1(
            market::MarketNoticeV1::kVendorLocalTimeInvalid);
    if (payload == nullptr) {
        return false;
    }
    const market::DecodedMarketCommonV1* const common =
        StoredCommon(record);
    if (common == nullptr ||
        common->instrument_id != record.instrument_id() ||
        !market::IsTickEventKindV1(record.kind()) ||
        payload->common.instrument_id != record.instrument_id() ||
        common->ordinal !=
            static_cast<std::size_t>(payload->common.ordinal) ||
        common->kind != record.kind() ||
        payload->common.event_kind !=
            static_cast<std::uint8_t>(record.kind()) ||
        common->origin.source_stream_id !=
            record.source_stream_id() ||
        common->origin.source_sequence !=
            record.source_sequence() ||
        (payload->common.quality_flags & ~null_value_mask) !=
            (common->quality_flags & ~null_value_mask) ||
        (payload->common.market_notices & ~vendor_time_mask) !=
            (common->market_notices & ~vendor_time_mask)) {
        return false;
    }
    payload->common.source_sequence = record.source_sequence();
    payload->common.ingress_sequence = record.ingress_sequence();
    payload->common.tick_stream_sequence =
        record.tick_stream_sequence();
    payload->common.vendor_sequence_id =
        common->origin.vendor_sequence_id;
    payload->common.recv_realtime_ns = record.recv_realtime_ns();
    payload->common.recv_monotonic_ns = record.recv_monotonic_ns();
    payload->common.quality_flags |=
        common->quality_flags & null_value_mask;
    payload->common.market_notices |=
        common->market_notices & vendor_time_mask;
    payload->common.source_stream_id = record.source_stream_id();
    payload->common.vendor_local_time_raw =
        common->origin.vendor_local_time_raw;
    payload->common.source_slot = record.source_slot();
    return true;
}

struct AppliedHandoff final {
    std::size_t ordinal = 0U;
    const market::RealtimeHistoryRecordV1* record = nullptr;
};
static_assert(std::is_trivially_copyable_v<AppliedHandoff>);
static_assert(std::is_trivially_copyable_v<
              realtime::NativeSequenceObservationV1>);

// Native observations have exactly one producer: the serialized SDK
// admission path. Keep that lane SPSC so the callback does not pay for an
// MPMC reservation and a second per-cell synchronization protocol.
template <typename Value>
class BoundedSpscQueue final {
public:
    explicit BoundedSpscQueue(std::size_t capacity)
        : capacity_(capacity),
          mask_(capacity - 1U),
          cells_(std::make_unique<Value[]>(capacity)) {}

    BoundedSpscQueue(const BoundedSpscQueue&) = delete;
    BoundedSpscQueue& operator=(const BoundedSpscQueue&) = delete;

    [[nodiscard]] bool TryPush(const Value& value) noexcept {
        const std::size_t position =
            enqueue_position_.load(std::memory_order_relaxed);
        if (position - cached_dequeue_position_ >= capacity_) {
            cached_dequeue_position_ =
                dequeue_position_.load(std::memory_order_acquire);
            if (position - cached_dequeue_position_ >= capacity_) {
                return false;
            }
        }
        cells_[position & mask_] = value;
        // The worker's sleep handshake samples this position with seq_cst.
        // Preserve that total-order edge while removing the MPMC CAS/cell
        // sequence from the serialized producer.
        enqueue_position_.store(
            position + 1U, std::memory_order_seq_cst);
        return true;
    }

    [[nodiscard]] bool TryPop(Value* output) noexcept {
        if (output == nullptr) {
            return false;
        }
        const std::size_t position =
            dequeue_position_.load(std::memory_order_relaxed);
        if (position == cached_enqueue_position_) {
            cached_enqueue_position_ =
                enqueue_position_.load(std::memory_order_acquire);
            if (position == cached_enqueue_position_) {
                return false;
            }
        }
        *output = cells_[position & mask_];
        dequeue_position_.store(
            position + 1U, std::memory_order_release);
        return true;
    }

    [[nodiscard]] bool Empty() const noexcept {
        return dequeue_position_.load(std::memory_order_acquire) ==
               enqueue_position_.load(std::memory_order_acquire);
    }

    [[nodiscard]] bool EmptyForWait() const noexcept {
        const std::size_t dequeue =
            dequeue_position_.load(std::memory_order_relaxed);
        const std::size_t enqueue =
            enqueue_position_.load(std::memory_order_seq_cst);
        return dequeue == enqueue;
    }

    [[nodiscard]] std::size_t Depth() const noexcept {
        const std::size_t dequeue =
            dequeue_position_.load(std::memory_order_acquire);
        const std::size_t enqueue =
            enqueue_position_.load(std::memory_order_acquire);
        return enqueue - dequeue;
    }

    [[nodiscard]] std::size_t EnqueuePosition() const noexcept {
        return enqueue_position_.load(std::memory_order_acquire);
    }

    [[nodiscard]] std::size_t DequeuePosition() const noexcept {
        return dequeue_position_.load(std::memory_order_acquire);
    }

private:
    const std::size_t capacity_;
    const std::size_t mask_;
    std::unique_ptr<Value[]> cells_;
    alignas(64) std::atomic<std::size_t> enqueue_position_{0U};
    std::size_t cached_dequeue_position_ = 0U;
    alignas(64) std::atomic<std::size_t> dequeue_position_{0U};
    std::size_t cached_enqueue_position_ = 0U;
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
                        std::memory_order_seq_cst,
                        std::memory_order_relaxed)) {
                    cell.value = value;
                    cell.sequence.store(
                        position + 1U, std::memory_order_release);
                    return true;
                }
            } else if (difference < 0) {
                return false;
            } else {
                position = enqueue_position_.load(
                    std::memory_order_relaxed);
            }
        }
    }

    [[nodiscard]] bool TryPop(Value* output) noexcept {
        if (output == nullptr) {
            return false;
        }
        const std::size_t position =
            dequeue_position_.load(std::memory_order_relaxed);
        Cell& cell = cells_[position & mask_];
        const std::size_t sequence =
            cell.sequence.load(std::memory_order_acquire);
        const std::intptr_t difference =
            static_cast<std::intptr_t>(sequence) -
            static_cast<std::intptr_t>(position + 1U);
        if (difference != 0) {
            return false;
        }
        // This queue has one Event consumer. Avoiding a redundant CAS here
        // removes one contended read-modify-write from every handoff while
        // retaining the per-cell release that gates producer reuse.
        *output = cell.value;
        cell.sequence.store(
            position + capacity_, std::memory_order_release);
        dequeue_position_.store(
            position + 1U, std::memory_order_release);
        return true;
    }

    [[nodiscard]] bool Empty() const noexcept {
        return dequeue_position_.load(std::memory_order_acquire) ==
               enqueue_position_.load(std::memory_order_acquire);
    }

    [[nodiscard]] bool EmptyForWait() const noexcept {
        const std::size_t dequeue =
            dequeue_position_.load(std::memory_order_relaxed);
        const std::size_t enqueue =
            enqueue_position_.load(std::memory_order_seq_cst);
        return dequeue == enqueue;
    }

    [[nodiscard]] std::size_t Depth() const noexcept {
        const std::size_t dequeue =
            dequeue_position_.load(std::memory_order_acquire);
        const std::size_t enqueue =
            enqueue_position_.load(std::memory_order_acquire);
        // Unsigned subtraction remains valid across position wrap because
        // live depth is bounded by capacity_. A producer reservation that has
        // not published its cell may be included, which is conservative for
        // this diagnostic high-water sample.
        return enqueue - dequeue;
    }

    [[nodiscard]] std::size_t EnqueuePosition() const noexcept {
        return enqueue_position_.load(std::memory_order_acquire);
    }

    [[nodiscard]] std::size_t DequeuePosition() const noexcept {
        return dequeue_position_.load(std::memory_order_acquire);
    }

private:
    struct alignas(64) Cell final {
        std::atomic<std::size_t> sequence{0U};
        Value value{};
    };

    const std::size_t capacity_;
    const std::size_t mask_;
    std::unique_ptr<Cell[]> cells_;
    alignas(64) std::atomic<std::size_t> enqueue_position_{0U};
    alignas(64) std::atomic<std::size_t> dequeue_position_{0U};
};

enum class WorkerStartupState : std::uint8_t {
    kNotStarted = 0U,
    kStarting,
    kReady,
    kFailed,
};

}  // namespace

class RealtimePartialOrderEventServiceV2::Impl final {
private:
    static constexpr std::size_t kCanonicalBatchSize = 2'048U;
    // Bound the interval in which the worker is not checking its input lanes.
    // Journal slices still accumulate across turns to kCanonicalBatchSize, so
    // this responsiveness quantum does not give up commit batching.
    static constexpr std::size_t kCanonicalDrainQuantum = 512U;
    // A target tick contributes one observation and one applied handoff.
    static constexpr std::size_t kHandoffBatchSize =
        kCanonicalDrainQuantum * 2U;
    static constexpr std::uint64_t kMaximumOrdinaryEventsPerTick = 3U;
    // Full 500-625k/s target streams reach the size bound first. The age
    // bound prevents a sparse target tail from being hidden indefinitely by
    // sustained filtered traffic without adding another worker or timer.
    static constexpr std::uint64_t kCanonicalBatchMaximumAgeNs =
        4'000'000U;
    static constexpr std::uint64_t kStatusPublishMaximumAgeNs = 4'000'000U;

    struct ChannelRuntime final {
        realtime::NativeSequenceChannelV1 domain{};
        realtime::NativeSequenceRecoveryChannelSnapshotV1 snapshot{};
        std::uint64_t candidate_origin = 0U;
        std::uint64_t first_seen_monotonic_ns = 0U;
        std::uint64_t affected_since_monotonic_ns = 0U;
        bool active = false;
        bool snapshot_dirty = false;
    };

    enum class OutputBoundResult : std::uint8_t {
        kOk = 0U,
        kProjectionFailure,
        kResourceExhausted,
    };

public:
    explicit Impl(RealtimePartialOrderEventServiceConfigV2 config)
        : config_(std::move(config)) {}

    ~Impl() { Stop(); }

    [[nodiscard]] RealtimePartialOrderEventServiceCreateErrorV2
    Initialize(int* system_error_number) {
        SetSystemError(system_error_number, 0);
        if (!IdentityNonzero(config_.run_id) ||
            config_.session_epoch == 0U || config_.trade_date == 0U ||
            config_.coverage_start_unix_ns == 0U ||
            config_.publication_generation == 0U ||
            config_.correction_epoch == 0U ||
            config_.fast_sink == nullptr || config_.channel_capacity == 0U ||
            config_.handoff_queue_capacity < 2U ||
            !IsPowerOfTwo(config_.handoff_queue_capacity) ||
            config_.handoff_queue_capacity >
                static_cast<std::uint64_t>(
                    std::numeric_limits<std::intptr_t>::max()) ||
            config_.handoff_queue_capacity >
                static_cast<std::uint64_t>(
                    std::numeric_limits<std::size_t>::max()) ||
            config_.maximum_pending_entries == 0U ||
            config_.maximum_pending_entries >
                static_cast<std::uint64_t>(
                    std::numeric_limits<std::size_t>::max()) ||
            config_.maximum_pending_entries_per_channel == 0U ||
            config_.maximum_pending_entries_per_channel >
                config_.maximum_pending_entries ||
            config_.duplicate_retention_entries >
                static_cast<std::uint64_t>(
                    std::numeric_limits<std::size_t>::max()) ||
            config_.maximum_reorder_span == 0U ||
            config_.discovery_horizon <= std::chrono::nanoseconds::zero() ||
            config_.discovery_horizon > std::chrono::minutes(10) ||
            config_.maximum_shanghai_order_states == 0U ||
            config_.maximum_shenzhen_order_states == 0U ||
            config_.maximum_derived_events == 0U ||
            config_.event_journal_capacity == 0U ||
            config_.event_journal_capacity <
                config_.maximum_derived_events ||
            !IsPowerOfTwo(config_.order_state_capacity) ||
            config_.lazy_commit_chunk_bytes < 4096U ||
            config_.lazy_commit_chunk_bytes % 4096U != 0U) {
            SetSystemError(system_error_number, EINVAL);
            return RealtimePartialOrderEventServiceCreateErrorV2::
                kInvalidConfiguration;
        }

        worker_cpu_set_.Clear();
        if (!config_.worker_cpu_set.empty() &&
            common::ParseLinuxCpuSetV1(
                config_.worker_cpu_set, &worker_cpu_set_) !=
                common::LinuxCpuSetParseErrorV1::kNone) {
            SetSystemError(system_error_number, EINVAL);
            return RealtimePartialOrderEventServiceCreateErrorV2::
                kInvalidConfiguration;
        }

        realtime::NativeSequenceRecoveryConfigV1 recovery_config{};
        recovery_config.coverage_mode =
            realtime::NativeSequenceRecoveryCoverageModeV1::
                kProcessStartPartial;
        recovery_config.maximum_channels = config_.channel_capacity;
        recovery_config.maximum_pending_entries =
            static_cast<std::size_t>(config_.maximum_pending_entries);
        recovery_config.maximum_pending_entries_per_channel =
            static_cast<std::size_t>(
                config_.maximum_pending_entries_per_channel);
        recovery_config.certified_duplicate_retention_entries =
            static_cast<std::size_t>(
                config_.duplicate_retention_entries);
        recovery_config.maximum_canonical_payload_bytes_per_entry =
            sizeof(RealtimeWireTickPayloadV2);
        std::uint64_t payload_entries = 0U;
        std::uint64_t payload_bytes = 0U;
        if (!CheckedAdd(
                config_.maximum_pending_entries,
                config_.duplicate_retention_entries,
                &payload_entries) ||
            !CheckedMultiply(
                payload_entries,
                sizeof(RealtimeWireTickPayloadV2),
                &payload_bytes) ||
            payload_bytes >
                static_cast<std::uint64_t>(
                    std::numeric_limits<std::size_t>::max())) {
            return RealtimePartialOrderEventServiceCreateErrorV2::
                kInvalidConfiguration;
        }
        recovery_config.maximum_total_canonical_payload_bytes =
            static_cast<std::size_t>(payload_bytes);
        recovery_config.preallocate_canonical_payload_arena = true;
        recovery_config.maximum_reorder_span =
            config_.maximum_reorder_span;
        recovery_config.expected_origin_sequence = 1U;
        if (realtime::NativeSequenceRecoveryCoordinatorV1::Create(
                recovery_config, &recovery_) !=
                realtime::NativeSequenceRecoveryCreateErrorV1::kNone ||
            recovery_ == nullptr) {
            return RealtimePartialOrderEventServiceCreateErrorV2::
                kRecoveryCreateFailed;
        }

        CertifiedOrderEventHistoryConfigV1 history_config{};
        history_config.trade_date = config_.trade_date;
        history_config.maximum_shanghai_order_states =
            config_.maximum_shanghai_order_states;
        history_config.maximum_shenzhen_order_states =
            config_.maximum_shenzhen_order_states;
        history_config.maximum_events = config_.maximum_derived_events;
        history_config.preallocate_event_storage = true;
        history_config.publish_process_snapshots = false;
        if (CertifiedOrderEventHistoryV1::Create(
                history_config, &history_) !=
                CertifiedOrderEventHistoryErrorV1::kNone ||
            history_ == nullptr) {
            return RealtimePartialOrderEventServiceCreateErrorV2::
                kHistoryCreateFailed;
        }

        PartialOrderEventJournalConfigV2 journal_config{};
        journal_config.run_id = config_.run_id;
        journal_config.session_epoch = config_.session_epoch;
        journal_config.trade_date = config_.trade_date;
        journal_config.publication_generation =
            config_.publication_generation;
        journal_config.correction_epoch = config_.correction_epoch;
        journal_config.coverage_start_unix_ns =
            config_.coverage_start_unix_ns;
        journal_config.ordering_quality =
            PartialOrderEventOrderingQualityV2::
                kBoundedReorderedPartial;
        journal_config.event_capacity = config_.event_journal_capacity;
        journal_config.affected_channel_capacity =
            config_.channel_capacity;
        journal_config.order_state_capacity = config_.order_state_capacity;
        // This service promises that the Event worker never grows journal
        // backing. Populate the randomly accessed order-state pages before
        // the worker starts so first-touch faults cannot become steady-state
        // latency on the publication path.
        journal_config.prefault_order_state_pages = true;
        journal_config.prefault_event_pages = true;
        std::uint64_t ordinary_batch_events = 0U;
        const std::uint64_t maximum_single_tick_state_updates =
            config_.maximum_order_state_updates_per_commit == 0U
                ? static_cast<std::uint64_t>(
                      config_.maximum_shanghai_order_states)
                : static_cast<std::uint64_t>(
                      config_.maximum_order_state_updates_per_commit);
        std::uint64_t maximum_single_tick_events = 0U;
        if (!CheckedMultiply(
                kMaximumOrdinaryEventsPerTick,
                static_cast<std::uint64_t>(kCanonicalBatchSize),
                &ordinary_batch_events) ||
            !CheckedAdd(
                maximum_single_tick_state_updates,
                1U,
                &maximum_single_tick_events)) {
            return RealtimePartialOrderEventServiceCreateErrorV2::
                kInvalidConfiguration;
        }
        maximum_events_per_journal_commit_ = std::min(
            config_.event_journal_capacity,
            std::max(ordinary_batch_events, maximum_single_tick_events));
        if (maximum_events_per_journal_commit_ == 0U) {
            return RealtimePartialOrderEventServiceCreateErrorV2::
                kInvalidConfiguration;
        }
        journal_config.maximum_events_per_commit =
            maximum_events_per_journal_commit_;
        if (config_.maximum_order_state_updates_per_commit != 0U) {
            std::uint64_t batch_state_updates = 0U;
            if (!CheckedMultiply(
                    config_.maximum_order_state_updates_per_commit,
                    static_cast<std::uint64_t>(kCanonicalBatchSize),
                    &batch_state_updates)) {
                return RealtimePartialOrderEventServiceCreateErrorV2::
                    kInvalidConfiguration;
            }
            journal_config.maximum_order_state_updates_per_commit =
                std::min(
                    batch_state_updates,
                    config_.order_state_capacity);
        }
        journal_config.maximum_mapping_bytes =
            config_.maximum_mapping_bytes;
        journal_config.lazy_commit_chunk_bytes =
            config_.lazy_commit_chunk_bytes;
        if (PartialOrderEventJournalProducerV2::Create(
                journal_config, &journal_, system_error_number) !=
                PartialOrderEventJournalCreateErrorV2::kNone ||
            journal_ == nullptr) {
            return RealtimePartialOrderEventServiceCreateErrorV2::
                kJournalCreateFailed;
        }
        if (journal_->PreallocateBacking(system_error_number) !=
            PartialOrderEventJournalPublishErrorV2::kNone) {
            return RealtimePartialOrderEventServiceCreateErrorV2::
                kJournalCreateFailed;
        }

        try {
            const auto handoff_capacity = static_cast<std::size_t>(
                config_.handoff_queue_capacity);
            observation_queue_ = std::make_unique<BoundedSpscQueue<
                realtime::NativeSequenceObservationV1>>(
                handoff_capacity);
            applied_queue_ =
                std::make_unique<BoundedMpmcQueue<AppliedHandoff>>(
                    handoff_capacity);
            channels_.resize(config_.channel_capacity);
            std::size_t index_capacity = 1U;
            while (index_capacity <
                   static_cast<std::size_t>(config_.channel_capacity) * 2U) {
                if (index_capacity >
                    std::numeric_limits<std::size_t>::max() / 2U) {
                    return RealtimePartialOrderEventServiceCreateErrorV2::
                        kInvalidConfiguration;
                }
                index_capacity *= 2U;
            }
            channel_index_.assign(
                index_capacity,
                std::numeric_limits<std::uint32_t>::max());
            affected_channels_.reserve(config_.channel_capacity);
            canonical_batch_slices_.reserve(kCanonicalBatchSize);
        } catch (const std::bad_alloc&) {
            return RealtimePartialOrderEventServiceCreateErrorV2::
                kResourceExhausted;
        }
        return RealtimePartialOrderEventServiceCreateErrorV2::kNone;
    }

    [[nodiscard]] bool StartWorker(int* system_error_number) noexcept {
        const std::lock_guard<std::mutex> lock(lifecycle_mutex_);
        SetSystemError(system_error_number, 0);
        if (worker_startup_.load(std::memory_order_acquire) !=
                WorkerStartupState::kNotStarted ||
            worker_thread_.joinable() || observation_queue_ == nullptr ||
            applied_queue_ == nullptr) {
            SetSystemError(system_error_number, EINVAL);
            return false;
        }
        worker_startup_.store(
            WorkerStartupState::kStarting, std::memory_order_release);
        try {
            worker_thread_ = std::thread([this]() noexcept {
                int affinity_error = 0;
                if (!ApplyOptionalCurrentThreadCpuSet(
                        worker_cpu_set_, &affinity_error)) {
                    worker_affinity_error_.store(
                        affinity_error, std::memory_order_relaxed);
                    worker_startup_.store(
                        WorkerStartupState::kFailed,
                        std::memory_order_release);
                    worker_startup_.notify_all();
                    return;
                }
                accepting_.store(true, std::memory_order_release);
                worker_running_.store(true, std::memory_order_release);
                worker_startup_.store(
                    WorkerStartupState::kReady,
                    std::memory_order_release);
                worker_startup_.notify_all();
                WorkerLoop();
                worker_running_.store(false, std::memory_order_release);
            });
        } catch (...) {
            worker_affinity_error_.store(EAGAIN, std::memory_order_relaxed);
            worker_startup_.store(
                WorkerStartupState::kFailed, std::memory_order_release);
            worker_startup_.notify_all();
        }
        WorkerStartupState state =
            worker_startup_.load(std::memory_order_acquire);
        while (state == WorkerStartupState::kStarting) {
            worker_startup_.wait(state, std::memory_order_acquire);
            state = worker_startup_.load(std::memory_order_acquire);
        }
        if (state == WorkerStartupState::kReady) {
            return true;
        }
        accepting_.store(false, std::memory_order_release);
        stop_requested_.store(true, std::memory_order_release);
        WakeWorker();
        if (worker_thread_.joinable()) {
            worker_thread_.join();
        }
        const int affinity_error =
            worker_affinity_error_.load(std::memory_order_relaxed);
        SetSystemError(
            system_error_number,
            affinity_error == 0 ? EINVAL : affinity_error);
        return false;
    }

    [[nodiscard]] bool PublishApplied(
        std::size_t ordinal,
        const market::RealtimeHistoryRecordV1& record) noexcept {
        if (!config_.fast_sink->PublishApplied(ordinal, record)) {
            return false;
        }
        if (!market::IsTickEventKindV1(record.kind())) {
            return true;
        }
        if (!accepting_.load(std::memory_order_acquire) ||
            globally_frozen_.load(std::memory_order_acquire)) {
            return true;
        }
        AppliedHandoff handoff{};
        handoff.ordinal = ordinal;
        handoff.record = &record;
        if (!applied_queue_->TryPush(handoff)) {
            StoreMaximum(
                &handoff_queue_high_water_,
                CombinedQueueDepth());
            static_cast<void>(IncrementSaturating(&dropped_handoffs_));
            RequestGlobalFailure(
                PartialOrderEventLastErrorV2::kResourceExhausted);
            return true;
        }
        WakeWorkerAfterEnqueue();
        return true;
    }

    void MarkCoverageLost() noexcept {
        config_.fast_sink->MarkCoverageLost();
        RequestGlobalFailure(
            PartialOrderEventLastErrorV2::kPermanentGap);
    }

    void ObserveNativeSequence(
        const realtime::NativeSequenceObservationV1& observation)
        noexcept {
        if (!accepting_.load(std::memory_order_acquire) ||
            globally_frozen_.load(std::memory_order_acquire)) {
            return;
        }
        if (!observation_queue_->TryPush(observation)) {
            StoreMaximum(
                &handoff_queue_high_water_,
                CombinedQueueDepth());
            static_cast<void>(IncrementSaturating(&dropped_handoffs_));
            RequestGlobalFailure(
                PartialOrderEventLastErrorV2::kResourceExhausted);
            return;
        }
        WakeWorkerAfterEnqueue();
    }

    void MarkNativeSequenceObservationFailure(
        realtime::NativeSequenceObservationFailureV1 failure,
        const sdk::MessageKey&) noexcept {
        switch (failure) {
            case realtime::NativeSequenceObservationFailureV1::kExtraction:
                RequestGlobalFailure(
                    PartialOrderEventLastErrorV2::kProjectionFailure);
                return;
            case realtime::NativeSequenceObservationFailureV1::kHandoff:
                RequestGlobalFailure(
                    PartialOrderEventLastErrorV2::kResourceExhausted);
                return;
        }
        RequestGlobalFailure(
            PartialOrderEventLastErrorV2::kPublicationInvariant);
    }

    void MarkDraining() noexcept {
        draining_.store(true, std::memory_order_release);
        status_dirty_.store(true, std::memory_order_release);
        WakeWorker();
    }

    void MarkStoppedClean() noexcept {
        {
            const std::lock_guard<std::mutex> lock(lifecycle_mutex_);
            accepting_.store(false, std::memory_order_release);
            clean_stop_requested_.store(true, std::memory_order_release);
            stop_requested_.store(true, std::memory_order_release);
            WakeWorker();
        }
        JoinWorker();
        // History may already have invoked QuiesceRecordReferences and joined
        // the worker. Status-only publication is still safe here: it neither
        // polls the coordinator nor dereferences a borrowed Store record.
        status_dirty_.store(true, std::memory_order_release);
        PublishStatusIfDirty(true);
    }

    void QuiesceRecordReferences() noexcept {
        {
            const std::lock_guard<std::mutex> lock(lifecycle_mutex_);
            accepting_.store(false, std::memory_order_release);
            // The production clean path calls MarkDraining before History's
            // terminal quiesce. Force-seal/drain while the Store is still
            // guaranteed alive, so MarkStoppedClean may later be status-only.
            if (draining_.load(std::memory_order_acquire)) {
                clean_stop_requested_.store(
                    true, std::memory_order_release);
            }
            stop_requested_.store(true, std::memory_order_release);
            WakeWorker();
        }
        JoinWorker();
    }

    void Stop() noexcept {
        {
            const std::lock_guard<std::mutex> lock(lifecycle_mutex_);
            accepting_.store(false, std::memory_order_release);
            stop_requested_.store(true, std::memory_order_release);
            WakeWorker();
        }
        JoinWorker();
    }

    [[nodiscard]] RealtimePartialOrderEventServiceSnapshotV2 Snapshot()
        const noexcept {
        RealtimePartialOrderEventServiceSnapshotV2 result{};
        PartialOrderEventLastErrorV2 terminal_error =
            terminal_error_.load(std::memory_order_acquire);
        const std::uint64_t published_status =
            published_status_.load(std::memory_order_acquire);
        if (terminal_error == PartialOrderEventLastErrorV2::kNone) {
            terminal_error =
                terminal_error_.load(std::memory_order_acquire);
        }
        result.globally_frozen =
            terminal_error != PartialOrderEventLastErrorV2::kNone;
        result.state = result.globally_frozen
                           ? PartialOrderEventServiceStateV2::kFrozenResource
                           : UnpackPublishedState(published_status);
        result.last_error = result.globally_frozen
                                ? terminal_error
                                : UnpackPublishedError(published_status);
        result.canonical_apply_frontier =
            canonical_apply_frontier_.load(std::memory_order_acquire);
        result.published_event_frontier =
            published_event_frontier_.load(std::memory_order_acquire);
        result.captured_source_frontier =
            captured_source_frontier_.load(std::memory_order_acquire);
        static_assert(sizeof(std::size_t) <= sizeof(std::uint64_t));
        const std::uint64_t enqueued_observations =
            static_cast<std::uint64_t>(
                observation_queue_->EnqueuePosition());
        const std::uint64_t enqueued_applied =
            static_cast<std::uint64_t>(
                applied_queue_->EnqueuePosition());
        result.observed_native_messages = enqueued_observations;
        result.applied_records = enqueued_applied;
        result.enqueued_handoffs =
            enqueued_observations <=
                    std::numeric_limits<std::uint64_t>::max() -
                        enqueued_applied
                ? enqueued_observations + enqueued_applied
                : std::numeric_limits<std::uint64_t>::max();
        result.processed_handoffs =
            processed_handoffs_.load(std::memory_order_acquire);
        result.dropped_handoffs =
            dropped_handoffs_.load(std::memory_order_acquire);
        // processed_handoffs_ advances once per worker batch. Use the queue
        // cursors for instantaneous depth so already-popped in-flight work is
        // not reported as backlog for up to kHandoffBatchSize records.
        result.handoff_queue_depth = CombinedQueueDepth();
        result.handoff_queue_high_water =
            handoff_queue_high_water_.load(std::memory_order_acquire);
        result.journal_canonical_commits =
            journal_canonical_commits_.load(std::memory_order_acquire);
        result.journal_status_commits =
            journal_status_commits_.load(std::memory_order_acquire);
        result.journal_published_slices =
            journal_published_slices_.load(std::memory_order_acquire);
        result.journal_maximum_batch_slices =
            journal_maximum_batch_slices_.load(
                std::memory_order_acquire);
        result.reorder_high_water =
            reorder_high_water_.load(std::memory_order_acquire);
        result.pending_entries =
            pending_entries_.load(std::memory_order_acquire);
        result.channel_count = channel_count_snapshot_.load(
            std::memory_order_acquire);
        result.unsealed_channel_count = unsealed_channel_count_.load(
            std::memory_order_acquire);
        result.gap_channel_count = gap_channel_count_.load(
            std::memory_order_acquire);
        result.correction_pending_channel_count =
            correction_channel_count_.load(std::memory_order_acquire);
        result.frozen_channel_count = frozen_channel_count_.load(
            std::memory_order_acquire);
        result.worker_running =
            worker_running_.load(std::memory_order_acquire);
        result.accepting = !result.globally_frozen &&
                           accepting_.load(std::memory_order_acquire);
        result.journal_failed = journal_ == nullptr ||
                                journal_failed_.load(
                                    std::memory_order_acquire);
        result.history_failed = history_ == nullptr || history_->failed();
        return result;
    }

    [[nodiscard]] bool DuplicateReadOnlyDescriptor(
        int* output_fd,
        int* system_error_number) const noexcept {
        return journal_ != nullptr &&
               journal_->DuplicateReadOnlyDescriptor(
                   output_fd, system_error_number);
    }

    [[nodiscard]] PartialOrderEventJournalSessionV2 session()
        const noexcept {
        return journal_ == nullptr
                   ? PartialOrderEventJournalSessionV2{}
                   : journal_->session();
    }

    [[nodiscard]] PartialOrderEventJournalResourceSnapshotV2
    JournalResourceSnapshot() const noexcept {
        return journal_ == nullptr
                   ? PartialOrderEventJournalResourceSnapshotV2{}
                   : journal_->ResourceSnapshot();
    }

    [[nodiscard]] bool WaitUntilIdleForTest(
        std::chrono::milliseconds timeout) const noexcept {
        if (timeout < std::chrono::milliseconds::zero()) {
            return false;
        }
        const auto deadline = Clock::now() + timeout;
        for (;;) {
            if (observation_queue_ != nullptr &&
                applied_queue_ != nullptr && QueuesEmpty() &&
                processed_handoffs_.load(std::memory_order_acquire) >=
                    EnqueuedHandoffCount() &&
                !status_dirty_.load(std::memory_order_acquire) &&
                !journal_publication_in_progress_.load(
                    std::memory_order_acquire)) {
                return true;
            }
            if (Clock::now() >= deadline) {
                return false;
            }
            std::this_thread::yield();
        }
    }

    void SetWorkerPausedForTest(bool paused) noexcept {
        worker_paused_for_test_.store(paused, std::memory_order_release);
        if (!paused) {
            worker_pause_reached_for_test_.store(
                false, std::memory_order_release);
        }
        WakeWorker();
    }

    [[nodiscard]] bool WorkerPauseReachedForTest() const noexcept {
        return worker_pause_reached_for_test_.load(
            std::memory_order_acquire);
    }

    void SetJournalPublicationPausedForTest(bool paused) noexcept {
        journal_publication_paused_for_test_.store(
            paused, std::memory_order_release);
        if (!paused) {
            journal_publication_pause_reached_for_test_.store(
                false, std::memory_order_release);
        }
        WakeWorker();
    }

    [[nodiscard]] bool JournalPublicationPauseReachedForTest()
        const noexcept {
        return journal_publication_pause_reached_for_test_.load(
            std::memory_order_acquire);
    }

private:
    [[nodiscard]] bool QueuesEmpty() const noexcept {
        return observation_queue_->Empty() && applied_queue_->Empty();
    }

    [[nodiscard]] bool QueuesEmptyForWait() const noexcept {
        return observation_queue_->EmptyForWait() &&
               applied_queue_->EmptyForWait();
    }

    [[nodiscard]] std::uint64_t EnqueuedHandoffCount() const noexcept {
        const std::uint64_t observations = static_cast<std::uint64_t>(
            observation_queue_->EnqueuePosition());
        const std::uint64_t applied = static_cast<std::uint64_t>(
            applied_queue_->EnqueuePosition());
        return observations <=
                       std::numeric_limits<std::uint64_t>::max() - applied
                   ? observations + applied
                   : std::numeric_limits<std::uint64_t>::max();
    }

    [[nodiscard]] std::uint64_t CombinedQueueDepth() const noexcept {
        const std::uint64_t observations = static_cast<std::uint64_t>(
            observation_queue_->Depth());
        const std::uint64_t applied = static_cast<std::uint64_t>(
            applied_queue_->Depth());
        return observations + applied;
    }

    [[nodiscard]] bool SealBarrierReached() const noexcept {
        return observation_queue_->DequeuePosition() >=
                   observation_seal_barrier_target_ &&
               applied_queue_->DequeuePosition() >=
                   applied_seal_barrier_target_;
    }

    void UpdateHandoffQueueHighWater(std::uint64_t depth) noexcept {
        StoreMaximum(&handoff_queue_high_water_, depth);
    }

    void JoinWorker() noexcept {
        if (worker_thread_.joinable() &&
            worker_thread_.get_id() != std::this_thread::get_id()) {
            worker_thread_.join();
        }
        worker_running_.store(false, std::memory_order_release);
    }

    void WakeWorker() noexcept {
        if (!worker_waiting_.exchange(
                false, std::memory_order_seq_cst)) {
            return;
        }
        NotifyWaitingWorker();
    }

    void WakeWorkerAfterEnqueue() noexcept {
        if (!worker_waiting_.load(std::memory_order_seq_cst) ||
            !worker_waiting_.exchange(
                false, std::memory_order_seq_cst)) {
            return;
        }
        NotifyWaitingWorker();
    }

    void NotifyWaitingWorker() noexcept {
        {
            const std::lock_guard<std::mutex> lock(wake_mutex_);
            wake_epoch_.fetch_add(1U, std::memory_order_release);
        }
        wake_cv_.notify_one();
    }

    void NotifyFailure() noexcept {
        if (config_.failure_notifier != nullptr &&
            !failure_notification_sent_.exchange(
                true, std::memory_order_acq_rel)) {
            config_.failure_notifier(config_.failure_notifier_context);
        }
    }

    void RecordGlobalFailure(
        PartialOrderEventLastErrorV2 error) noexcept {
        if (error == PartialOrderEventLastErrorV2::kNone) {
            error = PartialOrderEventLastErrorV2::kWorkerExited;
        }
        PartialOrderEventLastErrorV2 expected =
            PartialOrderEventLastErrorV2::kNone;
        static_cast<void>(terminal_error_.compare_exchange_strong(
            expected,
            error,
            std::memory_order_acq_rel,
            std::memory_order_acquire));
        globally_frozen_.store(true, std::memory_order_release);
    }

    void RecordPublishedStatus(
        PartialOrderEventServiceStateV2 state,
        PartialOrderEventLastErrorV2 error) noexcept {
        if (terminal_error_.load(std::memory_order_acquire) ==
            PartialOrderEventLastErrorV2::kNone) {
            published_status_.store(
                PackPublishedStatus(state, error),
                std::memory_order_release);
        }
    }

    void PauseBeforeJournalPublicationForTest() noexcept {
        if (!journal_publication_paused_for_test_.load(
                std::memory_order_acquire)) {
            return;
        }
        journal_publication_pause_reached_for_test_.store(
            true, std::memory_order_release);
        while (journal_publication_paused_for_test_.load(
                   std::memory_order_acquire) &&
               !stop_requested_.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
    }

    void RequestGlobalFailure(
        PartialOrderEventLastErrorV2 error) noexcept {
        RecordGlobalFailure(error);
        accepting_.store(false, std::memory_order_release);
        status_dirty_.store(true, std::memory_order_release);
        NotifyFailure();
        WakeWorker();
    }

    [[nodiscard]] static std::uint64_t ChannelHash(
        const realtime::NativeSequenceChannelV1& domain) noexcept {
        std::uint64_t value =
            static_cast<std::uint64_t>(domain.channel) |
            (static_cast<std::uint64_t>(domain.market) << 32U);
        value ^= value >> 30U;
        value *= 0xbf58476d1ce4e5b9ULL;
        value ^= value >> 27U;
        value *= 0x94d049bb133111ebULL;
        value ^= value >> 31U;
        return value;
    }

    [[nodiscard]] ChannelRuntime* EnsureChannel(
        const realtime::NativeSequenceDescriptorV1& descriptor) noexcept {
        if (descriptor.sequence == 0U ||
            descriptor.sequence >=
                static_cast<std::uint64_t>(
                    std::numeric_limits<std::int64_t>::max()) ||
            (descriptor.domain.market !=
                 realtime::NativeSequenceMarketV1::kShanghai &&
             descriptor.domain.market !=
                 realtime::NativeSequenceMarketV1::kShenzhen) ||
            (descriptor.domain.market ==
                 realtime::NativeSequenceMarketV1::kShanghai &&
             descriptor.domain.channel == 0U) ||
            channel_index_.empty()) {
            RequestGlobalFailure(
                PartialOrderEventLastErrorV2::kProjectionFailure);
            return nullptr;
        }
        if (last_channel_index_ < channel_count_) {
            ChannelRuntime& cached = channels_[last_channel_index_];
            if (cached.domain == descriptor.domain) {
                cached.candidate_origin =
                    cached.candidate_origin == 0U
                        ? descriptor.sequence
                        : std::min(
                              cached.candidate_origin,
                              descriptor.sequence);
                return &cached;
            }
        }
        const std::size_t mask = channel_index_.size() - 1U;
        std::size_t slot = static_cast<std::size_t>(
            ChannelHash(descriptor.domain)) & mask;
        for (std::size_t probe = 0U; probe < channel_index_.size();
             ++probe) {
            const std::uint32_t index = channel_index_[slot];
            if (index == std::numeric_limits<std::uint32_t>::max()) {
                if (channel_count_ >= channels_.size()) {
                    RequestGlobalFailure(
                        PartialOrderEventLastErrorV2::
                            kResourceExhausted);
                    return nullptr;
                }
                std::uint64_t now_ns = 0U;
                if (!ReadMonotonicNs(&now_ns)) {
                    RequestGlobalFailure(
                        PartialOrderEventLastErrorV2::kWorkerExited);
                    return nullptr;
                }
                ChannelRuntime& runtime = channels_[channel_count_];
                runtime = {};
                runtime.active = true;
                runtime.domain = descriptor.domain;
                runtime.snapshot.domain = descriptor.domain;
                runtime.candidate_origin = descriptor.sequence;
                runtime.first_seen_monotonic_ns = now_ns;
                channel_index_[slot] =
                    static_cast<std::uint32_t>(channel_count_);
                last_channel_index_ = channel_count_;
                ++channel_count_;
                status_dirty_.store(true, std::memory_order_release);
                return &runtime;
            }
            ChannelRuntime& runtime = channels_[index];
            if (runtime.domain == descriptor.domain) {
                last_channel_index_ = index;
                runtime.candidate_origin =
                    runtime.candidate_origin == 0U
                        ? descriptor.sequence
                        : std::min(
                              runtime.candidate_origin,
                              descriptor.sequence);
                return &runtime;
            }
            slot = (slot + 1U) & mask;
        }
        RequestGlobalFailure(
            PartialOrderEventLastErrorV2::kResourceExhausted);
        return nullptr;
    }

    void MarkChannelDirty(ChannelRuntime* runtime) noexcept {
        if (runtime == nullptr) {
            RequestGlobalFailure(
                PartialOrderEventLastErrorV2::
                    kPublicationInvariant);
            return;
        }
        runtime->snapshot_dirty = true;
        if (!status_dirty_.load(std::memory_order_relaxed)) {
            status_dirty_.store(true, std::memory_order_release);
        }
    }

    void RefreshDirtyChannels() noexcept {
        if (recovery_ == nullptr) {
            RequestGlobalFailure(
                PartialOrderEventLastErrorV2::kPublicationInvariant);
            return;
        }
        for (std::size_t index = 0U; index < channel_count_; ++index) {
            ChannelRuntime& runtime = channels_[index];
            if (!runtime.snapshot_dirty) {
                continue;
            }
            if (!recovery_->ChannelSnapshot(
                    runtime.domain, &runtime.snapshot)) {
                RequestGlobalFailure(
                    PartialOrderEventLastErrorV2::
                        kPublicationInvariant);
                return;
            }
            runtime.snapshot_dirty = false;
        }
    }

    void HandleObservation(
        const realtime::NativeSequenceObservationV1& observation) noexcept {
        ChannelRuntime* runtime =
            EnsureChannel(observation.descriptor);
        if (runtime == nullptr) {
            return;
        }
        realtime::NativeSequenceRecoveryObserveResultV1 result{};
        const auto error = recovery_->Observe(
            observation.descriptor,
            observation.message_key,
            observation.record_class,
            &result);
        if (error != realtime::NativeSequenceRecoveryObserveErrorV1::kNone) {
            RequestGlobalFailure(
                PartialOrderEventLastErrorV2::kProjectionFailure);
            return;
        }
        if (result.disposition ==
            realtime::NativeSequenceRecoveryObserveDispositionV1::
                kChannelCapacity) {
            RequestGlobalFailure(
                PartialOrderEventLastErrorV2::kResourceExhausted);
            return;
        }
        MarkChannelDirty(runtime);
    }

    void HandleApplied(
        std::size_t ordinal,
        const market::RealtimeHistoryRecordV1* record) noexcept {
        if (record == nullptr) {
            RequestGlobalFailure(
                PartialOrderEventLastErrorV2::kProjectionFailure);
            return;
        }
        RealtimeWireTickPayloadV2 payload{};
        if (!ProjectRealtimeWireTickPayloadV2(
                *record, ordinal, &payload) ||
            !RealtimeCertifiedTickPayloadCanonicalV1(payload) ||
            payload.common.ingress_sequence == 0U) {
            RequestGlobalFailure(
                PartialOrderEventLastErrorV2::kProjectionFailure);
            return;
        }
        captured_source_frontier_local_ = std::max(
            captured_source_frontier_local_,
            payload.common.ingress_sequence);
        const auto descriptor = DescriptorFromPayload(payload);
        ChannelRuntime* runtime = EnsureChannel(descriptor);
        if (runtime == nullptr) {
            return;
        }
        const sdk::MessageKey message_key =
            MessageKeyFromPayload(payload);
        CanonicalizeBusinessPayload(&payload);
        static_assert(sizeof(std::uintptr_t) <= sizeof(std::uint64_t));
        const std::uint64_t cookie = static_cast<std::uint64_t>(
            reinterpret_cast<std::uintptr_t>(record));
        realtime::NativeSequenceRecoveryApplyResultV1 result{};
        const auto error = recovery_->MarkTargetApplied(
            descriptor,
            message_key,
            std::as_bytes(std::span(&payload, 1U)),
            cookie,
            &result);
        if (error != realtime::NativeSequenceRecoveryApplyErrorV1::kNone &&
            error !=
                realtime::NativeSequenceRecoveryApplyErrorV1::
                    kChannelFrozen) {
            RequestGlobalFailure(
                PartialOrderEventLastErrorV2::kProjectionFailure);
            return;
        }
        if (result.disposition ==
            realtime::NativeSequenceRecoveryApplyDispositionV1::
                kChannelCapacity) {
            RequestGlobalFailure(
                PartialOrderEventLastErrorV2::kResourceExhausted);
            return;
        }
        MarkChannelDirty(runtime);
    }

    [[nodiscard]] ChannelRuntime* FindChannel(
        const realtime::NativeSequenceChannelV1& domain) noexcept {
        if (channel_index_.empty()) {
            return nullptr;
        }
        if (last_channel_index_ < channel_count_ &&
            channels_[last_channel_index_].domain == domain) {
            return &channels_[last_channel_index_];
        }
        const std::size_t mask = channel_index_.size() - 1U;
        std::size_t slot =
            static_cast<std::size_t>(ChannelHash(domain)) & mask;
        for (std::size_t probe = 0U; probe < channel_index_.size();
             ++probe) {
            const std::uint32_t index = channel_index_[slot];
            if (index == std::numeric_limits<std::uint32_t>::max()) {
                return nullptr;
            }
            if (channels_[index].domain == domain) {
                last_channel_index_ = index;
                return &channels_[index];
            }
            slot = (slot + 1U) & mask;
        }
        return nullptr;
    }

    void SealOrigins(std::uint64_t now_ns, bool force) noexcept {
        const std::uint64_t horizon = static_cast<std::uint64_t>(
            config_.discovery_horizon.count());
        for (std::size_t index = 0U; index < channel_count_; ++index) {
            ChannelRuntime& runtime = channels_[index];
            if (runtime.snapshot.origin_sealed ||
                runtime.candidate_origin == 0U ||
                (!force &&
                 (now_ns < runtime.first_seen_monotonic_ns ||
                  now_ns - runtime.first_seen_monotonic_ns < horizon))) {
                continue;
            }
            realtime::NativeSequenceRecoverySealResultV1 result{};
            const auto error = recovery_->SealBoundedOrigin(
                runtime.domain, runtime.candidate_origin, &result);
            if (error !=
                    realtime::NativeSequenceRecoverySealErrorV1::kNone ||
                (result.disposition !=
                     realtime::NativeSequenceRecoverySealDispositionV1::
                         kSealed &&
                 result.disposition !=
                     realtime::NativeSequenceRecoverySealDispositionV1::
                         kAlreadySealed)) {
                RequestGlobalFailure(
                    PartialOrderEventLastErrorV2::
                        kPublicationInvariant);
                return;
            }
            MarkChannelDirty(&runtime);
        }
    }

    [[nodiscard]] OutputBoundResult MaximumOutputForPayload(
        const RealtimeWireTickPayloadV2& payload,
        std::uint64_t shanghai_order_state_count,
        std::uint64_t* output) const noexcept {
        if (output == nullptr) {
            return OutputBoundResult::kProjectionFailure;
        }
        *output = 0U;
        const auto kind = static_cast<market::MarketEventKindV1>(
            payload.common.event_kind);
        market::TickActionV1 action = market::TickActionV1::kUnknown;
        bool shanghai_end = false;
        if (kind == market::MarketEventKindV1::kShanghaiTick) {
            market::ShanghaiOrderEventInputV1 input{};
            if (ProjectShanghaiOrderEventInputFromWireV2(
                    payload, &input) !=
                WireOrderEventProjectionResultV2::kProjected) {
                return OutputBoundResult::kProjectionFailure;
            }
            action = input.action;
            shanghai_end =
                input.action == market::TickActionV1::kStatus &&
                input.phase_valid &&
                input.phase == market::TradingPhaseV1::kEnd;
        } else if (
            kind == market::MarketEventKindV1::kShenzhenOrder ||
            kind == market::MarketEventKindV1::kShenzhenTransaction) {
            market::ShenzhenOrderEventInputV1 input{};
            if (ProjectShenzhenOrderEventInputFromWireV2(
                    payload, &input) !=
                WireOrderEventProjectionResultV2::kProjected) {
                return OutputBoundResult::kProjectionFailure;
            }
            action = input.action;
        } else {
            return OutputBoundResult::kProjectionFailure;
        }
        if (shanghai_end) {
            if (shanghai_order_state_count ==
                std::numeric_limits<std::uint64_t>::max()) {
                return OutputBoundResult::kResourceExhausted;
            }
            *output = shanghai_order_state_count + 1U;
            return OutputBoundResult::kOk;
        }
        switch (action) {
            case market::TickActionV1::kAdd:
            case market::TickActionV1::kStatus:
                *output = 1U;
                return OutputBoundResult::kOk;
            case market::TickActionV1::kCancel:
                *output = 2U;
                return OutputBoundResult::kOk;
            case market::TickActionV1::kTrade:
                *output = 3U;
                return OutputBoundResult::kOk;
            case market::TickActionV1::kUnknown:
                return OutputBoundResult::kProjectionFailure;
        }
        return OutputBoundResult::kProjectionFailure;
    }

    [[nodiscard]] PartialOrderEventLastErrorV2 MapHistoryFailure(
        CertifiedOrderEventHistoryErrorV1 error,
        market::MarketEventKindV1 kind) const noexcept {
        switch (error) {
            case CertifiedOrderEventHistoryErrorV1::kNone:
                return PartialOrderEventLastErrorV2::kNone;
            case CertifiedOrderEventHistoryErrorV1::kWireProjectionError:
                return PartialOrderEventLastErrorV2::kProjectionFailure;
            case CertifiedOrderEventHistoryErrorV1::kEventCapacity:
            case CertifiedOrderEventHistoryErrorV1::kResourceExhausted:
                return PartialOrderEventLastErrorV2::kResourceExhausted;
            case CertifiedOrderEventHistoryErrorV1::kExternalJournalError:
                return PartialOrderEventLastErrorV2::kPublicationFailure;
            case CertifiedOrderEventHistoryErrorV1::kCanonicalSequence:
            case CertifiedOrderEventHistoryErrorV1::
                kNativeSequenceRegression:
            case CertifiedOrderEventHistoryErrorV1::kNullOutput:
            case CertifiedOrderEventHistoryErrorV1::
                kInvalidConfiguration:
            case CertifiedOrderEventHistoryErrorV1::kFailed:
                return PartialOrderEventLastErrorV2::
                    kPublicationInvariant;
            case CertifiedOrderEventHistoryErrorV1::kAggregationError:
                break;
        }

        if (kind == market::MarketEventKindV1::kShanghaiTick) {
            switch (history_->last_shanghai_error()) {
                case market::ShanghaiOrderAggregatorConsumeErrorV1::
                    kOrderCapacity:
                case market::ShanghaiOrderAggregatorConsumeErrorV1::
                    kResourceExhausted:
                    return PartialOrderEventLastErrorV2::
                        kResourceExhausted;
                case market::ShanghaiOrderAggregatorConsumeErrorV1::
                    kInvalidInput:
                case market::ShanghaiOrderAggregatorConsumeErrorV1::
                    kWrongTradeDate:
                case market::ShanghaiOrderAggregatorConsumeErrorV1::
                    kNumericOverflow:
                    return PartialOrderEventLastErrorV2::
                        kProjectionFailure;
                case market::ShanghaiOrderAggregatorConsumeErrorV1::
                    kNone:
                case market::ShanghaiOrderAggregatorConsumeErrorV1::
                    kNullOutput:
                case market::ShanghaiOrderAggregatorConsumeErrorV1::
                    kAlreadyFinalized:
                case market::ShanghaiOrderAggregatorConsumeErrorV1::
                    kOutOfOrderInput:
                case market::ShanghaiOrderAggregatorConsumeErrorV1::
                    kFailed:
                    return PartialOrderEventLastErrorV2::
                        kPublicationInvariant;
            }
        }
        if (kind == market::MarketEventKindV1::kShenzhenOrder ||
            kind == market::MarketEventKindV1::kShenzhenTransaction) {
            switch (history_->last_shenzhen_error()) {
                case market::ShenzhenOrderProjectorConsumeErrorV1::
                    kOrderCapacity:
                case market::ShenzhenOrderProjectorConsumeErrorV1::
                    kResourceExhausted:
                    return PartialOrderEventLastErrorV2::
                        kResourceExhausted;
                case market::ShenzhenOrderProjectorConsumeErrorV1::
                    kInvalidInput:
                case market::ShenzhenOrderProjectorConsumeErrorV1::
                    kWrongTradeDate:
                    return PartialOrderEventLastErrorV2::
                        kProjectionFailure;
                case market::ShenzhenOrderProjectorConsumeErrorV1::kNone:
                case market::ShenzhenOrderProjectorConsumeErrorV1::
                    kNullOutput:
                case market::ShenzhenOrderProjectorConsumeErrorV1::
                    kAlreadyFinalized:
                case market::ShenzhenOrderProjectorConsumeErrorV1::
                    kOutOfOrderInput:
                case market::ShenzhenOrderProjectorConsumeErrorV1::kFailed:
                    return PartialOrderEventLastErrorV2::
                        kPublicationInvariant;
            }
        }
        return PartialOrderEventLastErrorV2::kProjectionFailure;
    }

    void AbandonCanonicalBatch() noexcept {
        canonical_batch_slices_.clear();
        canonical_batch_event_begin_ = nullptr;
        canonical_batch_event_count_ = 0U;
        canonical_batch_started_monotonic_ns_ = 0U;
        journal_publication_in_progress_.store(
            false, std::memory_order_release);
    }

    [[nodiscard]] bool FlushCanonicalBatch() noexcept {
        if (canonical_batch_slices_.empty()) {
            return true;
        }
        std::uint64_t expected_event_frontier = 0U;
        if ((canonical_batch_event_count_ != 0U &&
             canonical_batch_event_begin_ == nullptr) ||
            !CheckedAdd(
                published_event_frontier_.load(
                    std::memory_order_relaxed),
                static_cast<std::uint64_t>(
                    canonical_batch_event_count_),
                &expected_event_frontier) ||
            history_generation_.event_count !=
                expected_event_frontier) {
            AbandonCanonicalBatch();
            FreezeFromWorker(
                PartialOrderEventLastErrorV2::kPublicationInvariant);
            return false;
        }

        static_cast<void>(
            status_dirty_.exchange(false, std::memory_order_acq_rel));
        BuildStatusAndChannels();
        PauseBeforeJournalPublicationForTest();
        const std::span<const l2flow_instrument_derived_event_row_v1> events =
            canonical_batch_event_count_ == 0U
                ? std::span<const l2flow_instrument_derived_event_row_v1>{}
                : std::span<const l2flow_instrument_derived_event_row_v1>{
                      canonical_batch_event_begin_,
                      canonical_batch_event_count_};
        if (journal_->PublishCanonicalBatchProjected(
                canonical_batch_slices_,
                current_status_,
                events,
                affected_channels_) !=
            PartialOrderEventJournalPublishErrorV2::kNone) {
            journal_failed_.store(true, std::memory_order_release);
            AbandonCanonicalBatch();
            FreezeFromWorker(
                PartialOrderEventLastErrorV2::kPublicationFailure);
            return false;
        }

        const std::uint64_t published_slices =
            static_cast<std::uint64_t>(canonical_batch_slices_.size());
        static_cast<void>(
            IncrementSaturating(&journal_canonical_commits_));
        static_cast<void>(AddSaturating(
            &journal_published_slices_, published_slices));
        StoreMaximum(
            &journal_maximum_batch_slices_, published_slices);

        canonical_apply_frontier_.store(
            canonical_batch_slices_.back()
                .canonical_apply_sequence,
            std::memory_order_release);
        published_event_frontier_.store(
            static_cast<std::uint64_t>(
                history_generation_.event_count),
            std::memory_order_release);
        std::uint64_t published_ns = 0U;
        if (ReadMonotonicNs(&published_ns)) {
            last_journal_publication_monotonic_ns_ = published_ns;
        }
        AbandonCanonicalBatch();
        return true;
    }

    [[nodiscard]] bool CanonicalBatchAgeExpired(
        std::uint64_t now_ns) const noexcept {
        return canonical_batch_started_monotonic_ns_ == 0U ||
               now_ns < canonical_batch_started_monotonic_ns_ ||
               now_ns - canonical_batch_started_monotonic_ns_ >=
                   kCanonicalBatchMaximumAgeNs;
    }

    void DrainCanonical(std::size_t maximum_records) noexcept {
        canonical_drain_quantum_exhausted_ = false;
        std::size_t drained_records = 0U;
        while (drained_records < maximum_records &&
               !globally_frozen_.load(std::memory_order_acquire)) {
            realtime::NativeSequenceCertifiedReadyV1 ready{};
            const auto poll = recovery_->PollCertified(&ready);
            if (poll ==
                realtime::NativeSequenceRecoveryPollErrorV1::kNotReady) {
                if (!canonical_batch_slices_.empty()) {
                    std::uint64_t now_ns = 0U;
                    if (!ReadMonotonicNs(&now_ns)) {
                        AbandonCanonicalBatch();
                        RequestGlobalFailure(
                            PartialOrderEventLastErrorV2::kWorkerExited);
                        return;
                    }
                    if (CanonicalBatchAgeExpired(now_ns) ||
                        stop_requested_.load(std::memory_order_acquire)) {
                        static_cast<void>(FlushCanonicalBatch());
                    }
                }
                return;
            }
            if (poll != realtime::NativeSequenceRecoveryPollErrorV1::kNone) {
                AbandonCanonicalBatch();
                RequestGlobalFailure(
                    PartialOrderEventLastErrorV2::
                        kPublicationInvariant);
                return;
            }
            ChannelRuntime* runtime = FindChannel(ready.descriptor.domain);
            if (runtime == nullptr) {
                AbandonCanonicalBatch();
                RequestGlobalFailure(
                    PartialOrderEventLastErrorV2::
                        kPublicationInvariant);
                return;
            }
            if (ready.record_class ==
                realtime::NativeSequenceRecoveryRecordClassV1::kFiltered) {
                if (recovery_->CommitCertified(ready.token) !=
                    realtime::NativeSequenceRecoveryCommitErrorV1::kNone) {
                    AbandonCanonicalBatch();
                    RequestGlobalFailure(
                        PartialOrderEventLastErrorV2::
                            kPublicationInvariant);
                    return;
                }
                MarkChannelDirty(runtime);
                ++drained_records;
                continue;
            }
            if (ready.record_class !=
                realtime::NativeSequenceRecoveryRecordClassV1::kTarget) {
                AbandonCanonicalBatch();
                RequestGlobalFailure(
                    PartialOrderEventLastErrorV2::
                        kPublicationInvariant);
                return;
            }
            const auto pointer = static_cast<std::uintptr_t>(
                ready.applied_cookie);
            const auto* record = reinterpret_cast<
                const market::RealtimeHistoryRecordV1*>(pointer);
            if (record == nullptr || record->instrument_id() == 0U) {
                AbandonCanonicalBatch();
                RequestGlobalFailure(
                    PartialOrderEventLastErrorV2::kProjectionFailure);
                return;
            }
            RealtimeWireTickPayloadV2 payload{};
            if (ready.canonical_payload.size() != sizeof(payload)) {
                AbandonCanonicalBatch();
                RequestGlobalFailure(
                    PartialOrderEventLastErrorV2::kProjectionFailure);
                return;
            }
            market::ShenzhenOrderEventInputV1 shenzhen_input{};
            const auto event_kind = record->kind();
            const bool shenzhen_prepared =
                event_kind == market::MarketEventKindV1::kShenzhenOrder ||
                event_kind ==
                    market::MarketEventKindV1::kShenzhenTransaction;
            if (shenzhen_prepared) {
                const sdk::MessageKey expected_key =
                    event_kind ==
                            market::MarketEventKindV1::kShenzhenOrder
                        ? sdk::MessageKey{6U, 101U, 33U}
                        : sdk::MessageKey{6U, 101U, 36U};
                const auto stored_event = record->event();
                if (market::ProjectShenzhenOrderEventInputV1(
                        stored_event,
                        record->ingress_sequence(),
                        record->tick_stream_sequence(),
                        &shenzhen_input) !=
                        market::ShenzhenOrderEventProjectionV1::kProjected ||
                    shenzhen_input.instrument_id !=
                        record->instrument_id() ||
                    shenzhen_input.anchor.native_event_sequence <= 0 ||
                    static_cast<std::uint64_t>(
                        shenzhen_input.anchor.native_event_sequence) !=
                        ready.descriptor.sequence ||
                    shenzhen_input.channel !=
                        ready.descriptor.domain.channel ||
                    ready.descriptor.domain.market !=
                        realtime::NativeSequenceMarketV1::kShenzhen ||
                    !(expected_key == ready.message_key)) {
                    AbandonCanonicalBatch();
                    RequestGlobalFailure(
                        PartialOrderEventLastErrorV2::kProjectionFailure);
                    return;
                }
            } else {
                static_assert(
                    std::is_trivially_copyable_v<
                        RealtimeWireTickPayloadV2>);
                std::memcpy(
                    &payload,
                    ready.canonical_payload.data(),
                    sizeof(payload));
                if (!RestoreArrivalMetadata(*record, &payload) ||
                    !RealtimeCertifiedTickPayloadCanonicalV1(payload) ||
                    DescriptorFromPayload(payload) != ready.descriptor ||
                    !(MessageKeyFromPayload(payload) == ready.message_key)) {
                    AbandonCanonicalBatch();
                    RequestGlobalFailure(
                        PartialOrderEventLastErrorV2::kProjectionFailure);
                    return;
                }
            }
            const std::uint64_t current_events =
                static_cast<std::uint64_t>(
                    history_generation_.event_count);
            std::uint64_t expected_staged_events = 0U;
            if (!CheckedAdd(
                    published_event_frontier_.load(
                        std::memory_order_relaxed),
                    static_cast<std::uint64_t>(
                        canonical_batch_event_count_),
                    &expected_staged_events) ||
                current_events != expected_staged_events) {
                AbandonCanonicalBatch();
                FreezeFromWorker(
                    PartialOrderEventLastErrorV2::
                        kPublicationInvariant);
                return;
            }
            std::uint64_t maximum_output = 0U;
            OutputBoundResult output_bound = OutputBoundResult::kOk;
            if (shenzhen_prepared) {
                switch (shenzhen_input.action) {
                    case market::TickActionV1::kAdd:
                        maximum_output = 1U;
                        break;
                    case market::TickActionV1::kCancel:
                        maximum_output = 2U;
                        break;
                    case market::TickActionV1::kTrade:
                        maximum_output = 3U;
                        break;
                    case market::TickActionV1::kStatus:
                    case market::TickActionV1::kUnknown:
                        output_bound =
                            OutputBoundResult::kProjectionFailure;
                        break;
                }
            } else {
                output_bound = MaximumOutputForPayload(
                    payload,
                    history_generation_.shanghai_order_state_count,
                    &maximum_output);
            }
            if (output_bound ==
                OutputBoundResult::kProjectionFailure) {
                AbandonCanonicalBatch();
                FreezeFromWorker(
                    PartialOrderEventLastErrorV2::kProjectionFailure);
                return;
            }
            if (output_bound ==
                    OutputBoundResult::kResourceExhausted ||
                maximum_output == 0U ||
                maximum_output > maximum_events_per_journal_commit_) {
                AbandonCanonicalBatch();
                FreezeFromWorker(
                    PartialOrderEventLastErrorV2::kResourceExhausted);
                return;
            }
            if (maximum_output >
                    maximum_events_per_journal_commit_ -
                        static_cast<std::uint64_t>(
                            canonical_batch_event_count_) &&
                !FlushCanonicalBatch()) {
                return;
            }
            std::uint64_t writable_events = 0U;
            if (!CheckedAdd(
                    current_events, maximum_output, &writable_events) ||
                writable_events < current_events) {
                AbandonCanonicalBatch();
                FreezeFromWorker(
                    PartialOrderEventLastErrorV2::kResourceExhausted);
                return;
            }
            if (writable_events > config_.event_journal_capacity) {
                AbandonCanonicalBatch();
                FreezeFromWorker(
                    PartialOrderEventLastErrorV2::kResourceExhausted);
                return;
            }
            const std::uint64_t current_canonical =
                history_generation_.input_frontier
                    .canonical_apply_sequence;
            std::uint64_t expected_staged_canonical = 0U;
            if (!CheckedAdd(
                    canonical_apply_frontier_.load(
                        std::memory_order_relaxed),
                    static_cast<std::uint64_t>(
                        canonical_batch_slices_.size()),
                    &expected_staged_canonical) ||
                current_canonical != expected_staged_canonical ||
                current_canonical >=
                    std::numeric_limits<std::uint64_t>::max() / 2U) {
                AbandonCanonicalBatch();
                FreezeFromWorker(
                    PartialOrderEventLastErrorV2::
                        kPublicationInvariant);
                return;
            }
            const std::uint64_t next = current_canonical + 1U;
            if (canonical_batch_slices_.empty()) {
                if (!ReadMonotonicNs(
                        &canonical_batch_started_monotonic_ns_)) {
                    AbandonCanonicalBatch();
                    FreezeFromWorker(
                        PartialOrderEventLastErrorV2::kWorkerExited);
                    return;
                }
                journal_publication_in_progress_.store(
                    true, std::memory_order_release);
            }
            if (recovery_->CommitCertified(ready.token) !=
                realtime::NativeSequenceRecoveryCommitErrorV1::kNone) {
                AbandonCanonicalBatch();
                FreezeFromWorker(
                    PartialOrderEventLastErrorV2::
                        kPublicationInvariant);
                return;
            }
            CertifiedOrderEventHistoryAppendResultV1 append_result{};
            const CertifiedOrderEventHistoryErrorV1 history_error =
                shenzhen_prepared
                    ? history_->AppendCertifiedTick(
                          shenzhen_input, next, &append_result)
                    : history_->AppendCertifiedTick(
                          payload, next, &append_result);
            if (history_error !=
                CertifiedOrderEventHistoryErrorV1::kNone) {
                AbandonCanonicalBatch();
                FreezeFromWorker(
                    MapHistoryFailure(history_error, event_kind));
                return;
            }
            if (append_result.generation
                        .input_frontier.canonical_apply_sequence != next ||
                append_result.generation.event_count < current_events ||
                !append_result.appended_events.empty() ||
                append_result.appended_wire_events.size() !=
                    append_result.generation.event_count - current_events) {
                AbandonCanonicalBatch();
                FreezeFromWorker(
                    PartialOrderEventLastErrorV2::
                        kPublicationInvariant);
                return;
            }
            history_generation_ = append_result.generation;
            MarkChannelDirty(runtime);
            if (!append_result.appended_wire_events.empty()) {
                const l2flow_instrument_derived_event_row_v1* const
                    append_begin =
                        append_result.appended_wire_events.data();
                if (canonical_batch_event_begin_ == nullptr) {
                    canonical_batch_event_begin_ = append_begin;
                } else if (
                    canonical_batch_event_begin_ +
                            canonical_batch_event_count_ !=
                        append_begin) {
                    AbandonCanonicalBatch();
                    FreezeFromWorker(
                        PartialOrderEventLastErrorV2::
                            kPublicationInvariant);
                    return;
                }
            }
            if (append_result.appended_wire_events.size() >
                std::numeric_limits<std::size_t>::max() -
                    canonical_batch_event_count_) {
                AbandonCanonicalBatch();
                FreezeFromWorker(
                    PartialOrderEventLastErrorV2::kResourceExhausted);
                return;
            }
            canonical_batch_event_count_ +=
                append_result.appended_wire_events.size();
            canonical_batch_slices_.push_back({
                next, append_result.appended_wire_events.size()});
            ++drained_records;
            if (canonical_batch_slices_.size() ==
                    kCanonicalBatchSize &&
                !FlushCanonicalBatch()) {
                return;
            }
        }
        canonical_drain_quantum_exhausted_ =
            drained_records == maximum_records &&
            maximum_records !=
                std::numeric_limits<std::size_t>::max();
        if (canonical_drain_quantum_exhausted_ &&
            !canonical_batch_slices_.empty()) {
            std::uint64_t now_ns = 0U;
            if (!ReadMonotonicNs(&now_ns)) {
                AbandonCanonicalBatch();
                RequestGlobalFailure(
                    PartialOrderEventLastErrorV2::kWorkerExited);
            } else if (CanonicalBatchAgeExpired(now_ns)) {
                static_cast<void>(FlushCanonicalBatch());
            }
        }
    }

    void FreezeFromWorker(
        PartialOrderEventLastErrorV2 error) noexcept {
        RecordGlobalFailure(error);
        accepting_.store(false, std::memory_order_release);
        status_dirty_.store(true, std::memory_order_release);
        NotifyFailure();
        PublishStatusIfDirty(true);
    }

    [[nodiscard]] static bool HealthLess(
        const PartialOrderEventChannelHealthV2& left,
        const PartialOrderEventChannelHealthV2& right) noexcept {
        return left.market < right.market ||
               (left.market == right.market &&
                left.channel < right.channel);
    }

    void BuildStatusAndChannels() noexcept {
        RefreshDirtyChannels();
        affected_channels_.clear();
        std::uint64_t total_pending = 0U;
        std::uint32_t unsealed = 0U;
        std::uint32_t gaps = 0U;
        std::uint32_t corrections = 0U;
        std::uint32_t frozen = 0U;
        bool conflict = false;
        bool resource = false;
        std::uint64_t now_ns = 0U;
        if (!ReadMonotonicNs(&now_ns)) {
            RequestGlobalFailure(
                PartialOrderEventLastErrorV2::kWorkerExited);
            now_ns = 1U;
        }
        for (std::size_t index = 0U; index < channel_count_; ++index) {
            ChannelRuntime& runtime = channels_[index];
            const auto& snapshot = runtime.snapshot;
            const bool is_unsealed = !snapshot.origin_sealed;
            const bool is_correction = snapshot.correction_required;
            const bool is_conflict =
                snapshot.state ==
                realtime::NativeSequenceRecoveryChannelStateV1::
                    kFrozenConflict;
            const bool is_resource =
                snapshot.state ==
                realtime::NativeSequenceRecoveryChannelStateV1::
                    kFrozenResource;
            const bool has_gap =
                snapshot.origin_sealed &&
                (snapshot.missing_sequences != 0U ||
                 snapshot.pending_entries != 0U ||
                 snapshot.state ==
                     realtime::NativeSequenceRecoveryChannelStateV1::
                         kCatchingUp);
            const bool affected = is_unsealed || is_correction ||
                                  is_conflict || is_resource || has_gap;
            const bool active_correction =
                is_correction && !is_conflict && !is_resource;
            unsealed += is_unsealed ? 1U : 0U;
            gaps += has_gap ? 1U : 0U;
            corrections += active_correction ? 1U : 0U;
            frozen += (is_conflict || is_resource) ? 1U : 0U;
            conflict = conflict || is_conflict;
            resource = resource || is_resource;
            const std::uint64_t channel_pending =
                static_cast<std::uint64_t>(snapshot.pending_entries);
            if (channel_pending >
                std::numeric_limits<std::uint64_t>::max() -
                    total_pending) {
                RequestGlobalFailure(
                    PartialOrderEventLastErrorV2::
                        kPublicationInvariant);
                total_pending =
                    std::numeric_limits<std::uint64_t>::max();
            } else {
                total_pending += channel_pending;
            }
            if (!affected) {
                runtime.affected_since_monotonic_ns = 0U;
                continue;
            }
            if (runtime.affected_since_monotonic_ns == 0U) {
                runtime.affected_since_monotonic_ns =
                    is_unsealed
                        ? runtime.first_seen_monotonic_ns
                        : now_ns;
            }
            PartialOrderEventChannelHealthV2 health{};
            health.channel = static_cast<std::int64_t>(
                snapshot.domain.channel);
            health.market = static_cast<std::uint32_t>(
                snapshot.domain.market);
            health.pending_count = static_cast<std::uint64_t>(
                snapshot.pending_entries);
            health.flags = kPartialOrderEventChannelAffectedV2 |
                           kPartialOrderEventChannelStaleV2;
            if (snapshot.origin_sealed) {
                health.flags |=
                    kPartialOrderEventChannelOriginEstablishedV2;
                const std::uint64_t maximum_native =
                    static_cast<std::uint64_t>(
                        std::numeric_limits<std::int64_t>::max());
                if (snapshot.certified_sequence >= maximum_native) {
                    RequestGlobalFailure(
                        PartialOrderEventLastErrorV2::
                            kPublicationInvariant);
                } else {
                    health.contiguous_native_sequence =
                        static_cast<std::int64_t>(
                            snapshot.certified_sequence);
                    health.expected_native_sequence =
                        static_cast<std::int64_t>(
                            snapshot.certified_sequence + 1U);
                }
                health.highest_observed_native_sequence =
                    static_cast<std::int64_t>(
                        snapshot.highest_observed_sequence);
                if (snapshot.missing_sequences != 0U) {
                    if (snapshot.observed_contiguous_sequence >=
                        maximum_native) {
                        RequestGlobalFailure(
                            PartialOrderEventLastErrorV2::
                                kPublicationInvariant);
                    } else {
                        health.oldest_missing_native_sequence =
                            static_cast<std::int64_t>(
                                snapshot.observed_contiguous_sequence +
                                1U);
                    }
                }
            } else {
                health.highest_observed_native_sequence =
                    static_cast<std::int64_t>(
                        snapshot.highest_observed_sequence);
            }
            if (runtime.affected_since_monotonic_ns != 0U &&
                now_ns >= runtime.affected_since_monotonic_ns) {
                health.oldest_gap_age_ns =
                    now_ns - runtime.affected_since_monotonic_ns;
            }
            if (is_conflict) {
                health.state =
                    PartialOrderEventServiceStateV2::kFrozenConflict;
                health.last_error =
                    PartialOrderEventLastErrorV2::kConflictingDuplicate;
            } else if (is_resource) {
                health.state =
                    PartialOrderEventServiceStateV2::kFrozenResource;
                health.last_error =
                    PartialOrderEventLastErrorV2::kResourceExhausted;
            } else if (active_correction) {
                health.state =
                    PartialOrderEventServiceStateV2::kCorrectionPending;
                health.last_error =
                    PartialOrderEventLastErrorV2::kOutOfOrderInput;
            } else if (is_unsealed) {
                health.state =
                    PartialOrderEventServiceStateV2::kReordering;
            } else {
                health.state =
                    PartialOrderEventServiceStateV2::kCatchingUp;
            }
            affected_channels_.push_back(health);
        }
        std::sort(
            affected_channels_.begin(),
            affected_channels_.end(),
            HealthLess);

        const std::uint64_t prior_high =
            reorder_high_water_.load(std::memory_order_relaxed);
        const std::uint64_t next_high =
            std::max(prior_high, total_pending);
        reorder_high_water_.store(next_high, std::memory_order_release);
        pending_entries_.store(total_pending, std::memory_order_release);
        channel_count_snapshot_.store(
            static_cast<std::uint32_t>(channel_count_),
            std::memory_order_release);
        unsealed_channel_count_.store(unsealed, std::memory_order_release);
        gap_channel_count_.store(gaps, std::memory_order_release);
        correction_channel_count_.store(
            corrections, std::memory_order_release);
        frozen_channel_count_.store(frozen, std::memory_order_release);

        current_status_ = {};
        current_status_.captured_source_frontier =
            captured_source_frontier_.load(std::memory_order_relaxed);
        current_status_.reorder_high_water = next_high;
        const bool globally_frozen =
            globally_frozen_.load(std::memory_order_acquire);
        const PartialOrderEventLastErrorV2 terminal_error =
            terminal_error_.load(std::memory_order_acquire);
        if (terminal_error != PartialOrderEventLastErrorV2::kNone ||
            globally_frozen) {
            current_status_.state =
                PartialOrderEventServiceStateV2::kFrozenResource;
            current_status_.stale = true;
            current_status_.last_error =
                terminal_error == PartialOrderEventLastErrorV2::kNone
                    ? PartialOrderEventLastErrorV2::kWorkerExited
                    : terminal_error;
        } else if (conflict) {
            current_status_.state =
                PartialOrderEventServiceStateV2::kFrozenConflict;
            current_status_.stale = true;
            current_status_.last_error =
                PartialOrderEventLastErrorV2::kConflictingDuplicate;
        } else if (resource) {
            current_status_.state =
                PartialOrderEventServiceStateV2::kFrozenResource;
            current_status_.stale = true;
            current_status_.last_error =
                PartialOrderEventLastErrorV2::kResourceExhausted;
        } else if (corrections != 0U) {
            current_status_.state =
                PartialOrderEventServiceStateV2::kCorrectionPending;
            current_status_.stale = true;
            current_status_.last_error =
                PartialOrderEventLastErrorV2::kOutOfOrderInput;
        } else if (clean_stop_requested_.load(
                       std::memory_order_acquire)) {
            current_status_.state =
                PartialOrderEventServiceStateV2::kStoppedClean;
            current_status_.stale = !affected_channels_.empty();
            if (!affected_channels_.empty()) {
                current_status_.last_error =
                    PartialOrderEventLastErrorV2::kPermanentGap;
            }
        } else if (unsealed != 0U) {
            current_status_.state =
                PartialOrderEventServiceStateV2::kReordering;
            current_status_.stale = true;
        } else if (gaps != 0U || total_pending != 0U) {
            current_status_.state =
                PartialOrderEventServiceStateV2::kCatchingUp;
            current_status_.stale = true;
        } else if (draining_.load(std::memory_order_acquire)) {
            current_status_.state =
                PartialOrderEventServiceStateV2::kRestarting;
            current_status_.stale = true;
            current_status_.last_error =
                PartialOrderEventLastErrorV2::kWorkerExited;
        } else if (channel_count_ == 0U) {
            current_status_.state =
                PartialOrderEventServiceStateV2::kInitializing;
            current_status_.stale = true;
        } else {
            current_status_.state =
                PartialOrderEventServiceStateV2::kContiguous;
            current_status_.stale = false;
        }
        RecordPublishedStatus(
            current_status_.state, current_status_.last_error);
    }

    void PublishStatusIfDirty(bool force = false) noexcept {
        if (journal_ == nullptr || journal_->failed() ||
            !status_dirty_.load(std::memory_order_acquire)) {
            return;
        }
        if (!force) {
            std::uint64_t now_ns = 0U;
            if (!ReadMonotonicNs(&now_ns)) {
                RequestGlobalFailure(
                    PartialOrderEventLastErrorV2::kWorkerExited);
                return;
            }
            if (last_journal_publication_monotonic_ns_ != 0U &&
                now_ns >= last_journal_publication_monotonic_ns_ &&
                now_ns - last_journal_publication_monotonic_ns_ <
                    kStatusPublishMaximumAgeNs) {
                return;
            }
        }
        if (!canonical_batch_slices_.empty()) {
            // A canonical commit already carries the latest status. During a
            // sustained stream, let size-bounded batches fill instead of
            // turning the status freshness timer into a smaller commit timer.
            // Terminal publication remains forced and the idle-tail deadline
            // still bounds publication once both handoff lanes drain.
            if (!force && !QueuesEmpty()) {
                return;
            }
            static_cast<void>(FlushCanonicalBatch());
            return;
        }
        journal_publication_in_progress_.store(
            true, std::memory_order_release);
        if (!status_dirty_.exchange(false, std::memory_order_acq_rel)) {
            journal_publication_in_progress_.store(
                false, std::memory_order_release);
            return;
        }
        BuildStatusAndChannels();
        PauseBeforeJournalPublicationForTest();
        if (journal_->PublishStatus(
                current_status_, affected_channels_) !=
            PartialOrderEventJournalPublishErrorV2::kNone) {
            journal_failed_.store(true, std::memory_order_release);
            RecordGlobalFailure(
                PartialOrderEventLastErrorV2::kPublicationFailure);
            accepting_.store(false, std::memory_order_release);
            NotifyFailure();
        } else {
            static_cast<void>(IncrementSaturating(
                &journal_status_commits_));
            std::uint64_t published_ns = 0U;
            if (ReadMonotonicNs(&published_ns)) {
                last_journal_publication_monotonic_ns_ = published_ns;
            }
        }
        journal_publication_in_progress_.store(
            false, std::memory_order_release);
    }

    [[nodiscard]] std::uint64_t NextSealDeadlineNs() const noexcept {
        std::uint64_t result =
            std::numeric_limits<std::uint64_t>::max();
        const std::uint64_t horizon = static_cast<std::uint64_t>(
            config_.discovery_horizon.count());
        for (std::size_t index = 0U; index < channel_count_; ++index) {
            const ChannelRuntime& runtime = channels_[index];
            if (runtime.snapshot.origin_sealed ||
                runtime.first_seen_monotonic_ns == 0U) {
                continue;
            }
            const std::uint64_t deadline =
                runtime.first_seen_monotonic_ns >
                        std::numeric_limits<std::uint64_t>::max() - horizon
                    ? std::numeric_limits<std::uint64_t>::max()
                    : runtime.first_seen_monotonic_ns + horizon;
            result = std::min(result, deadline);
        }
        return result;
    }

    [[nodiscard]] std::uint64_t NextStatusDeadlineNs() const noexcept {
        if (!status_dirty_.load(std::memory_order_acquire) ||
            journal_failed_.load(std::memory_order_acquire)) {
            return std::numeric_limits<std::uint64_t>::max();
        }
        if (last_journal_publication_monotonic_ns_ == 0U) {
            return 0U;
        }
        if (last_journal_publication_monotonic_ns_ >
            std::numeric_limits<std::uint64_t>::max() -
                kStatusPublishMaximumAgeNs) {
            return std::numeric_limits<std::uint64_t>::max();
        }
        return last_journal_publication_monotonic_ns_ +
               kStatusPublishMaximumAgeNs;
    }

    [[nodiscard]] std::uint64_t
    NextCanonicalBatchDeadlineNs() const noexcept {
        if (canonical_batch_slices_.empty() ||
            canonical_batch_started_monotonic_ns_ == 0U) {
            return std::numeric_limits<std::uint64_t>::max();
        }
        if (canonical_batch_started_monotonic_ns_ >
            std::numeric_limits<std::uint64_t>::max() -
                kCanonicalBatchMaximumAgeNs) {
            return std::numeric_limits<std::uint64_t>::max();
        }
        return canonical_batch_started_monotonic_ns_ +
               kCanonicalBatchMaximumAgeNs;
    }

    [[nodiscard]] std::size_t DrainHandoffBatch() noexcept {
        constexpr std::size_t kLaneBatchSize =
            kHandoffBatchSize / 2U;
        std::size_t batch = 0U;
        std::size_t observations = 0U;
        std::size_t applied = 0U;
        const auto pop_observation = [this, &batch, &observations]() {
            if (seal_barrier_active_ &&
                observation_queue_->DequeuePosition() >=
                    observation_seal_barrier_target_) {
                return false;
            }
            realtime::NativeSequenceObservationV1 observation{};
            if (!observation_queue_->TryPop(&observation)) {
                return false;
            }
            if (!globally_frozen_.load(std::memory_order_acquire)) {
                HandleObservation(observation);
            }
            ++batch;
            ++observations;
            return true;
        };
        const auto pop_applied = [this, &batch, &applied]() {
            if (seal_barrier_active_ &&
                applied_queue_->DequeuePosition() >=
                    applied_seal_barrier_target_) {
                return false;
            }
            AppliedHandoff handoff{};
            if (!applied_queue_->TryPop(&handoff)) {
                return false;
            }
            if (!globally_frozen_.load(std::memory_order_acquire)) {
                HandleApplied(handoff.ordinal, handoff.record);
            }
            ++batch;
            ++applied;
            return true;
        };

        // Give both producer classes an equal bounded share. Recovery's join
        // contract permits either side to arrive first, so this removes the
        // producer CAS hotspot without inventing cross-lane ordering.
        while (batch < kHandoffBatchSize) {
            bool progressed = false;
            if (observations < kLaneBatchSize) {
                progressed = pop_observation();
                if (seal_barrier_active_ && SealBarrierReached()) {
                    return batch;
                }
            }
            if (applied < kLaneBatchSize) {
                progressed = pop_applied() || progressed;
                if (seal_barrier_active_ && SealBarrierReached()) {
                    return batch;
                }
            }
            if (!progressed) {
                break;
            }
        }
        return batch;
    }

    void WorkerLoop() noexcept {
        try {
            for (;;) {
                if (worker_paused_for_test_.load(
                        std::memory_order_acquire)) {
                    worker_pause_reached_for_test_.store(
                        true, std::memory_order_release);
                    while (worker_paused_for_test_.load(
                               std::memory_order_acquire) &&
                           !stop_requested_.load(
                               std::memory_order_acquire)) {
                        std::this_thread::yield();
                    }
                }
                UpdateHandoffQueueHighWater(CombinedQueueDepth());
                const std::size_t batch = DrainHandoffBatch();
                if (batch != 0U) {
                    static_cast<void>(AddSaturating(
                        &processed_handoffs_,
                        static_cast<std::uint64_t>(batch)));
                    captured_source_frontier_.store(
                        captured_source_frontier_local_,
                        std::memory_order_release);
                }

                const bool queue_empty = QueuesEmpty();
                std::uint64_t now_ns = 0U;
                if (!ReadMonotonicNs(&now_ns)) {
                    RequestGlobalFailure(
                        PartialOrderEventLastErrorV2::kWorkerExited);
                }
                if (!globally_frozen_.load(std::memory_order_acquire)) {
                    RefreshDirtyChannels();
                    const bool force_seal =
                        clean_stop_requested_.load(
                            std::memory_order_acquire) &&
                        queue_empty;
                    if (force_seal) {
                        SealOrigins(now_ns, true);
                        seal_barrier_active_ = false;
                    } else {
                        const std::uint64_t deadline =
                            NextSealDeadlineNs();
                        if (!seal_barrier_active_ &&
                            deadline !=
                                std::numeric_limits<
                                    std::uint64_t>::max() &&
                            now_ns >= deadline) {
                            // These queue-position fences consume no handoff
                            // capacity. Every reservation before both acquire
                            // snapshots is processed before sealing; later
                            // traffic cannot postpone the bounded cut.
                            observation_seal_barrier_target_ =
                                observation_queue_->EnqueuePosition();
                            applied_seal_barrier_target_ =
                                applied_queue_->EnqueuePosition();
                            seal_barrier_monotonic_ns_ = now_ns;
                            seal_barrier_active_ = true;
                        }
                        if (seal_barrier_active_ &&
                            SealBarrierReached()) {
                            SealOrigins(
                                seal_barrier_monotonic_ns_, false);
                            seal_barrier_active_ = false;
                        }
                    }
                    // Already sealed channels must advance on every bounded
                    // batch. A busy unrelated lane cannot hold their ready
                    // canonical prefix until the entire global queue empties.
                    DrainCanonical(
                        stop_requested_.load(
                            std::memory_order_acquire) &&
                                queue_empty
                            ? std::numeric_limits<std::size_t>::max()
                            : kCanonicalDrainQuantum);
                }
                const bool stop_when_empty =
                    stop_requested_.load(std::memory_order_acquire) &&
                    queue_empty;
                const bool force_status =
                    globally_frozen_.load(std::memory_order_acquire) ||
                    stop_when_empty;
                if (force_status || !canonical_drain_quantum_exhausted_ ||
                    canonical_batch_slices_.empty()) {
                    PublishStatusIfDirty(force_status);
                }

                if (stop_when_empty) {
                    if (!clean_stop_requested_.load(
                            std::memory_order_acquire) &&
                        !globally_frozen_.load(
                            std::memory_order_acquire)) {
                        draining_.store(true, std::memory_order_release);
                        status_dirty_.store(
                            true, std::memory_order_release);
                        PublishStatusIfDirty(true);
                    }
                    return;
                }
                if (!QueuesEmpty() ||
                    canonical_drain_quantum_exhausted_) {
                    continue;
                }

                std::unique_lock<std::mutex> lock(wake_mutex_);
                static_cast<void>(worker_waiting_.exchange(
                    true, std::memory_order_seq_cst));
                const std::uint64_t observed_wake =
                    wake_epoch_.load(std::memory_order_acquire);
                const std::uint64_t deadline = std::min(
                    NextCanonicalBatchDeadlineNs(),
                    std::min(
                        NextSealDeadlineNs(),
                        NextStatusDeadlineNs()));
                const auto predicate = [this, observed_wake]() noexcept {
                    return wake_epoch_.load(std::memory_order_acquire) !=
                               observed_wake ||
                           stop_requested_.load(
                               std::memory_order_acquire) ||
                           !QueuesEmptyForWait();
                };
                if (deadline ==
                    std::numeric_limits<std::uint64_t>::max()) {
                    wake_cv_.wait(lock, predicate);
                } else {
                    std::uint64_t wait_now_ns = 0U;
                    static_cast<void>(ReadMonotonicNs(&wait_now_ns));
                    const std::uint64_t wait_ns =
                        deadline > wait_now_ns
                            ? deadline - wait_now_ns
                            : 0U;
                    static_cast<void>(
                        wake_cv_.wait_for(
                            lock,
                            std::chrono::nanoseconds(wait_ns),
                            predicate));
                }
                worker_waiting_.store(false, std::memory_order_seq_cst);
            }
        } catch (...) {
            RequestGlobalFailure(
                PartialOrderEventLastErrorV2::kWorkerExited);
            PublishStatusIfDirty(true);
        }
    }

    RealtimePartialOrderEventServiceConfigV2 config_{};
    std::unique_ptr<realtime::NativeSequenceRecoveryCoordinatorV1>
        recovery_;
    std::unique_ptr<CertifiedOrderEventHistoryV1> history_;
    std::shared_ptr<PartialOrderEventJournalProducerV2> journal_;
    std::unique_ptr<BoundedSpscQueue<
        realtime::NativeSequenceObservationV1>> observation_queue_;
    std::unique_ptr<BoundedMpmcQueue<AppliedHandoff>> applied_queue_;
    std::vector<ChannelRuntime> channels_;
    std::vector<std::uint32_t> channel_index_;
    std::vector<PartialOrderEventChannelHealthV2> affected_channels_;
    std::size_t channel_count_ = 0U;
    std::size_t last_channel_index_ =
        std::numeric_limits<std::size_t>::max();
    std::size_t observation_seal_barrier_target_ = 0U;
    std::size_t applied_seal_barrier_target_ = 0U;
    std::uint64_t seal_barrier_monotonic_ns_ = 0U;
    bool seal_barrier_active_ = false;
    PartialOrderEventStatusUpdateV2 current_status_{};
    CertifiedOrderEventHistoryGenerationV1 history_generation_{};
    std::uint64_t captured_source_frontier_local_ = 0U;
    std::vector<PartialOrderEventCanonicalSliceV2>
        canonical_batch_slices_;
    const l2flow_instrument_derived_event_row_v1*
        canonical_batch_event_begin_ = nullptr;
    std::size_t canonical_batch_event_count_ = 0U;
    bool canonical_drain_quantum_exhausted_ = false;
    std::uint64_t canonical_batch_started_monotonic_ns_ = 0U;
    std::uint64_t last_journal_publication_monotonic_ns_ = 0U;
    std::uint64_t maximum_events_per_journal_commit_ = 0U;

    common::LinuxCpuSetV1 worker_cpu_set_{};
    std::thread worker_thread_;
    mutable std::mutex lifecycle_mutex_;
    std::mutex wake_mutex_;
    std::condition_variable wake_cv_;
    std::atomic<std::uint64_t> wake_epoch_{0U};
    // Producers read this on every successful handoff. Keep worker sleep/wake
    // writes off the lifecycle/admission cache line used by the same paths.
    alignas(64) std::atomic<bool> worker_waiting_{false};
    std::atomic<WorkerStartupState> worker_startup_{
        WorkerStartupState::kNotStarted};
    std::atomic<int> worker_affinity_error_{0};
    std::atomic<bool> worker_running_{false};
    alignas(64) std::atomic<bool> accepting_{false};
    std::atomic<bool> stop_requested_{false};
    std::atomic<bool> clean_stop_requested_{false};
    std::atomic<bool> draining_{false};
    std::atomic<bool> globally_frozen_{false};
    std::atomic<bool> journal_failed_{false};
    alignas(64) std::atomic<bool> status_dirty_{false};
    std::atomic<bool> journal_publication_in_progress_{false};
    std::atomic<bool> failure_notification_sent_{false};
    std::atomic<bool> worker_paused_for_test_{false};
    std::atomic<bool> worker_pause_reached_for_test_{false};
    std::atomic<bool> journal_publication_paused_for_test_{false};
    std::atomic<bool> journal_publication_pause_reached_for_test_{false};

    std::atomic<std::uint64_t> published_status_{
        PackPublishedStatus(
            PartialOrderEventServiceStateV2::kInitializing,
            PartialOrderEventLastErrorV2::kNone)};
    std::atomic<PartialOrderEventLastErrorV2> terminal_error_{
        PartialOrderEventLastErrorV2::kNone};
    std::atomic<std::uint64_t> canonical_apply_frontier_{0U};
    std::atomic<std::uint64_t> published_event_frontier_{0U};
    std::atomic<std::uint64_t> captured_source_frontier_{0U};
    alignas(64) std::atomic<std::uint64_t> processed_handoffs_{0U};
    std::atomic<std::uint64_t> dropped_handoffs_{0U};
    alignas(64) std::atomic<std::uint64_t>
        handoff_queue_high_water_{0U};
    alignas(64) std::atomic<std::uint64_t>
        journal_canonical_commits_{0U};
    std::atomic<std::uint64_t> journal_status_commits_{0U};
    std::atomic<std::uint64_t> journal_published_slices_{0U};
    std::atomic<std::uint64_t> journal_maximum_batch_slices_{0U};
    std::atomic<std::uint64_t> reorder_high_water_{0U};
    std::atomic<std::uint64_t> pending_entries_{0U};
    std::atomic<std::uint32_t> channel_count_snapshot_{0U};
    std::atomic<std::uint32_t> unsealed_channel_count_{0U};
    std::atomic<std::uint32_t> gap_channel_count_{0U};
    std::atomic<std::uint32_t> correction_channel_count_{0U};
    std::atomic<std::uint32_t> frozen_channel_count_{0U};
};

std::string_view RealtimePartialOrderEventServiceCreateErrorNameV2(
    RealtimePartialOrderEventServiceCreateErrorV2 error) noexcept {
    switch (error) {
        case RealtimePartialOrderEventServiceCreateErrorV2::kNone:
            return "none";
        case RealtimePartialOrderEventServiceCreateErrorV2::kNullOutput:
            return "null_output";
        case RealtimePartialOrderEventServiceCreateErrorV2::
            kInvalidConfiguration:
            return "invalid_configuration";
        case RealtimePartialOrderEventServiceCreateErrorV2::
            kRecoveryCreateFailed:
            return "recovery_create_failed";
        case RealtimePartialOrderEventServiceCreateErrorV2::
            kHistoryCreateFailed:
            return "history_create_failed";
        case RealtimePartialOrderEventServiceCreateErrorV2::
            kJournalCreateFailed:
            return "journal_create_failed";
        case RealtimePartialOrderEventServiceCreateErrorV2::
            kResourceExhausted:
            return "resource_exhausted";
        case RealtimePartialOrderEventServiceCreateErrorV2::
            kUnexpectedFailure:
            return "unexpected_failure";
    }
    return "unknown";
}

RealtimePartialOrderEventServiceV2::RealtimePartialOrderEventServiceV2(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

RealtimePartialOrderEventServiceV2::~RealtimePartialOrderEventServiceV2() =
    default;

RealtimePartialOrderEventServiceCreateErrorV2
RealtimePartialOrderEventServiceV2::Create(
    RealtimePartialOrderEventServiceConfigV2 config,
    std::shared_ptr<RealtimePartialOrderEventServiceV2>* output,
    int* system_error_number) noexcept {
    SetSystemError(system_error_number, 0);
    if (output == nullptr) {
        return RealtimePartialOrderEventServiceCreateErrorV2::kNullOutput;
    }
    output->reset();
    try {
        auto impl = std::make_unique<Impl>(std::move(config));
        const auto error = impl->Initialize(system_error_number);
        if (error !=
            RealtimePartialOrderEventServiceCreateErrorV2::kNone) {
            return error;
        }
        output->reset(new RealtimePartialOrderEventServiceV2(
            std::move(impl)));
        return RealtimePartialOrderEventServiceCreateErrorV2::kNone;
    } catch (const std::bad_alloc&) {
        return RealtimePartialOrderEventServiceCreateErrorV2::
            kResourceExhausted;
    } catch (...) {
        return RealtimePartialOrderEventServiceCreateErrorV2::
            kUnexpectedFailure;
    }
}

bool RealtimePartialOrderEventServiceV2::StartWorker(
    int* system_error_number) noexcept {
    return impl_ != nullptr && impl_->StartWorker(system_error_number);
}

bool RealtimePartialOrderEventServiceV2::PublishApplied(
    std::size_t ordinal,
    const market::RealtimeHistoryRecordV1& record) noexcept {
    return impl_ != nullptr && impl_->PublishApplied(ordinal, record);
}

void RealtimePartialOrderEventServiceV2::MarkCoverageLost() noexcept {
    if (impl_ != nullptr) {
        impl_->MarkCoverageLost();
    }
}

void RealtimePartialOrderEventServiceV2::QuiesceRecordReferences()
    noexcept {
    if (impl_ != nullptr) {
        impl_->QuiesceRecordReferences();
    }
}

void RealtimePartialOrderEventServiceV2::ObserveNativeSequence(
    const realtime::NativeSequenceObservationV1& observation) noexcept {
    if (impl_ != nullptr) {
        impl_->ObserveNativeSequence(observation);
    }
}

void RealtimePartialOrderEventServiceV2::
MarkNativeSequenceObservationFailure(
    realtime::NativeSequenceObservationFailureV1 failure,
    const sdk::MessageKey& message_key) noexcept {
    if (impl_ != nullptr) {
        impl_->MarkNativeSequenceObservationFailure(
            failure, message_key);
    }
}

void RealtimePartialOrderEventServiceV2::MarkDraining() noexcept {
    if (impl_ != nullptr) {
        impl_->MarkDraining();
    }
}

void RealtimePartialOrderEventServiceV2::MarkStoppedClean() noexcept {
    if (impl_ != nullptr) {
        impl_->MarkStoppedClean();
    }
}

void RealtimePartialOrderEventServiceV2::Stop() noexcept {
    if (impl_ != nullptr) {
        impl_->Stop();
    }
}

RealtimePartialOrderEventServiceSnapshotV2
RealtimePartialOrderEventServiceV2::Snapshot() const noexcept {
    return impl_ == nullptr
               ? RealtimePartialOrderEventServiceSnapshotV2{}
               : impl_->Snapshot();
}

bool RealtimePartialOrderEventServiceV2::DuplicateReadOnlyDescriptor(
    int* output_fd,
    int* system_error_number) const noexcept {
    return impl_ != nullptr && impl_->DuplicateReadOnlyDescriptor(
                                   output_fd, system_error_number);
}

PartialOrderEventJournalSessionV2
RealtimePartialOrderEventServiceV2::session() const noexcept {
    return impl_ == nullptr ? PartialOrderEventJournalSessionV2{}
                            : impl_->session();
}

PartialOrderEventJournalResourceSnapshotV2
RealtimePartialOrderEventServiceV2::JournalResourceSnapshot()
    const noexcept {
    return impl_ == nullptr
               ? PartialOrderEventJournalResourceSnapshotV2{}
               : impl_->JournalResourceSnapshot();
}

bool RealtimePartialOrderEventServiceV2::WaitUntilIdleForTest(
    std::chrono::milliseconds timeout) const noexcept {
    return impl_ != nullptr && impl_->WaitUntilIdleForTest(timeout);
}

void RealtimePartialOrderEventServiceV2::SetWorkerPausedForTest(
    bool paused) noexcept {
    if (impl_ != nullptr) {
        impl_->SetWorkerPausedForTest(paused);
    }
}

bool RealtimePartialOrderEventServiceV2::WorkerPauseReachedForTest()
    const noexcept {
    return impl_ != nullptr && impl_->WorkerPauseReachedForTest();
}

void RealtimePartialOrderEventServiceV2::
SetJournalPublicationPausedForTest(bool paused) noexcept {
    if (impl_ != nullptr) {
        impl_->SetJournalPublicationPausedForTest(paused);
    }
}

bool RealtimePartialOrderEventServiceV2::
JournalPublicationPauseReachedForTest() const noexcept {
    return impl_ != nullptr &&
           impl_->JournalPublicationPauseReachedForTest();
}

}  // namespace l2flow::ipc
