#include "l2flow/ipc/order_event_delta_ring_v1.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <new>
#include <span>
#include <string>
#include <type_traits>
#include <utility>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#ifndef F_SEAL_FUTURE_WRITE
#define F_SEAL_FUTURE_WRITE 0x0010
#endif

namespace l2flow::ipc {
namespace {

constexpr std::uint64_t kPageBytes = 4096U;
constexpr std::size_t kStableCopyAttempts = 3U;
// UINT64_MAX is reserved for the natural next cursor after the final
// representable event.  Publishing an event at UINT64_MAX would make the
// reader's serial cursor wrap to zero after a successful read.
constexpr std::uint64_t kMaximumPublishedEventSequence =
    std::numeric_limits<std::uint64_t>::max() - 1U;

[[nodiscard]] constexpr bool EventSequenceBatchFits(
    std::uint64_t published,
    std::uint64_t event_count) noexcept {
    return published <= kMaximumPublishedEventSequence &&
           event_count <=
               kMaximumPublishedEventSequence - published;
}

static_assert(EventSequenceBatchFits(
    kMaximumPublishedEventSequence - 1U, 1U));
static_assert(EventSequenceBatchFits(
    kMaximumPublishedEventSequence, 0U));
static_assert(!EventSequenceBatchFits(
    kMaximumPublishedEventSequence, 1U));
static_assert(!EventSequenceBatchFits(
    std::numeric_limits<std::uint64_t>::max(), 0U));

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

[[nodiscard]] bool AlignUp(
    std::uint64_t value,
    std::uint64_t alignment,
    std::uint64_t* output) noexcept {
    if (output == nullptr || alignment == 0U ||
        (alignment & (alignment - 1U)) != 0U) {
        return false;
    }
    const std::uint64_t mask = alignment - 1U;
    if (value > std::numeric_limits<std::uint64_t>::max() - mask) {
        return false;
    }
    *output = (value + mask) & ~mask;
    return true;
}

[[nodiscard]] bool AllZero(
    std::span<const std::uint8_t> bytes) noexcept {
    return std::all_of(
        bytes.begin(), bytes.end(), [](std::uint8_t value) {
            return value == 0U;
        });
}

[[nodiscard]] bool IdentityNonzero(
    const common::Identity128& identity) noexcept {
    return std::any_of(
        identity.begin(), identity.end(), [](std::byte value) {
            return value != std::byte{0};
        });
}

[[nodiscard]] common::Identity128 IdentityFromBytes(
    const std::array<std::uint8_t, 16U>& bytes) noexcept {
    common::Identity128 result{};
    std::memcpy(result.data(), bytes.data(), result.size());
    return result;
}

void CopyIdentity(
    const common::Identity128& source,
    std::array<std::uint8_t, 16U>* output) noexcept {
    std::memcpy(output->data(), source.data(), output->size());
}

[[nodiscard]] bool BoolByte(std::uint8_t value) noexcept {
    return value <= 1U;
}

[[nodiscard]] bool KnownState(std::uint32_t state) noexcept {
    return state >= static_cast<std::uint32_t>(
                        OrderEventDeltaProducerStateV1::kInitializing) &&
           state <= static_cast<std::uint32_t>(
                        OrderEventDeltaProducerStateV1::kFailed);
}

[[nodiscard]] bool KnownFlags(std::uint32_t flags) noexcept {
    return (flags & ~kOrderEventDeltaCoverageLostV1) == 0U;
}

[[nodiscard]] bool KnownTemporalCoverage(
    std::uint32_t value) noexcept {
    return value == static_cast<std::uint32_t>(
                        OrderEventDeltaTemporalCoverageV1::
                            kFromMarketOpen) ||
           value == static_cast<std::uint32_t>(
                        OrderEventDeltaTemporalCoverageV1::
                            kFromProcessStart);
}

[[nodiscard]] bool KnownStreamQuality(
    std::uint32_t value) noexcept {
    return value == static_cast<std::uint32_t>(
                        OrderEventDeltaStreamQualityV1::
                            kLocalTickStreamContiguous);
}

[[nodiscard]] bool ReadableState(std::uint32_t state) noexcept {
    return state ==
               static_cast<std::uint32_t>(
                   OrderEventDeltaProducerStateV1::kActive) ||
           state ==
               static_cast<std::uint32_t>(
                   OrderEventDeltaProducerStateV1::kDraining) ||
           state ==
               static_cast<std::uint32_t>(
                   OrderEventDeltaProducerStateV1::kStoppedClean);
}

[[nodiscard]] bool HeaderHealthy(
    const OrderEventDeltaHeaderV1& header) noexcept {
    const std::uint32_t flags =
        Atomic(header.flags).load(std::memory_order_acquire);
    const std::uint32_t state =
        Atomic(header.producer_state).load(std::memory_order_acquire);
    return KnownFlags(flags) &&
           (flags & kOrderEventDeltaCoverageLostV1) == 0U &&
           ReadableState(state);
}

[[nodiscard]] bool HeaderLayoutValid(
    const OrderEventDeltaHeaderV1& header,
    std::uint64_t descriptor_bytes) noexcept {
    if (header.magic != kOrderEventDeltaMagicV1 ||
        header.abi_major != kOrderEventDeltaWireMajorV1 ||
        header.abi_minor != kOrderEventDeltaWireMinorV1 ||
        header.header_bytes != sizeof(OrderEventDeltaHeaderV1) ||
        header.endian_marker != kOrderEventDeltaEndianMarkerV1 ||
        !KnownState(
            Atomic(header.producer_state)
                .load(std::memory_order_acquire)) ||
        !KnownFlags(
            Atomic(header.flags).load(std::memory_order_acquire)) ||
        header.total_mapping_bytes != descriptor_bytes ||
        header.total_mapping_bytes < sizeof(OrderEventDeltaHeaderV1) ||
        !IdentityNonzero(IdentityFromBytes(header.run_id)) ||
        header.session_epoch == 0U || header.trade_date == 0U ||
        header.ring_capacity == 0U ||
        header.slot_stride != sizeof(OrderEventDeltaSlotV1) ||
        header.slots_offset != sizeof(OrderEventDeltaHeaderV1) ||
        !KnownTemporalCoverage(header.temporal_coverage) ||
        !KnownStreamQuality(header.stream_quality) ||
        !AllZero(header.reserved)) {
        return false;
    }
    std::uint64_t slots_bytes = 0U;
    std::uint64_t logical_end = 0U;
    std::uint64_t expected_bytes = 0U;
    return CheckedMultiply(
               header.ring_capacity,
               sizeof(OrderEventDeltaSlotV1),
               &slots_bytes) &&
           CheckedAdd(
               header.slots_offset, slots_bytes, &logical_end) &&
           AlignUp(logical_end, kPageBytes, &expected_bytes) &&
           expected_bytes == descriptor_bytes;
}

[[nodiscard]] OrderEventDeltaSessionV1 SessionFromHeader(
    const OrderEventDeltaHeaderV1& header) noexcept {
    OrderEventDeltaSessionV1 result{};
    result.run_id = IdentityFromBytes(header.run_id);
    result.session_epoch = header.session_epoch;
    result.trade_date = header.trade_date;
    result.ring_capacity = header.ring_capacity;
    result.total_mapping_bytes = header.total_mapping_bytes;
    result.temporal_coverage =
        static_cast<OrderEventDeltaTemporalCoverageV1>(
            header.temporal_coverage);
    result.stream_quality =
        static_cast<OrderEventDeltaStreamQualityV1>(
            header.stream_quality);
    return result;
}

[[nodiscard]] bool PublishSlot(
    OrderEventDeltaSlotV1* slot,
    const OrderEventDeltaPayloadV1& payload) noexcept {
    if (slot == nullptr) {
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
    std::array<std::uint64_t, 40U> words{};
    std::memcpy(words.data(), &payload, sizeof(payload));
    for (std::size_t index = 0U; index < words.size(); ++index) {
        Atomic(slot->payload_words[index])
            .store(words[index], std::memory_order_relaxed);
    }
    tag.store(stable + 2U, std::memory_order_release);
    return true;
}

enum class StableCopyResult : std::uint8_t {
    kCopied = 0U,
    kInconsistent,
};

[[nodiscard]] StableCopyResult CopySlot(
    const OrderEventDeltaSlotV1& slot,
    OrderEventDeltaPayloadV1* output) noexcept {
    for (std::size_t attempt = 0U; attempt < kStableCopyAttempts;
         ++attempt) {
        const std::uint64_t begin =
            Atomic(slot.publish_tag).load(std::memory_order_acquire);
        if (begin == 0U || (begin & 1U) != 0U) {
            continue;
        }
        std::array<std::uint64_t, 40U> words{};
        for (std::size_t index = 0U; index < words.size(); ++index) {
            words[index] =
                Atomic(slot.payload_words[index])
                    .load(std::memory_order_relaxed);
        }
        std::atomic_thread_fence(std::memory_order_acq_rel);
        const std::uint64_t end =
            Atomic(slot.publish_tag).load(std::memory_order_acquire);
        if (begin == end && (end & 1U) == 0U) {
            std::memcpy(output, words.data(), sizeof(*output));
            return StableCopyResult::kCopied;
        }
    }
    return StableCopyResult::kInconsistent;
}

}  // namespace

bool OrderEventDeltaPayloadCanonicalV1(
    const OrderEventDeltaPayloadV1& payload,
    std::uint32_t expected_trade_date,
    std::uint64_t expected_source_tick_sequence) noexcept {
    if (expected_trade_date == 0U ||
        expected_source_tick_sequence == 0U ||
        payload.record_schema_version !=
            kOrderEventDeltaPayloadSchemaV1 ||
        payload.record_bytes != sizeof(OrderEventDeltaPayloadV1) ||
        payload.trade_date != expected_trade_date ||
        payload.instrument_id == 0U || payload.channel <= 0 ||
        payload.tick_stream_sequence !=
            expected_source_tick_sequence ||
        payload.reserved1[0U] !=
            L2FLOW_INSTRUMENT_DERIVED_EVENT_SOURCE_TICK_ORDINAL_VALID_V2 ||
        payload.reserved1[1U] != 0U ||
        payload.reserved1[2U] != 0U ||
        (payload.market !=
             L2FLOW_INSTRUMENT_DERIVED_EVENT_MARKET_SHANGHAI_V1 &&
         payload.market !=
             L2FLOW_INSTRUMENT_DERIVED_EVENT_MARKET_SHENZHEN_V1) ||
        payload.event_kind <
            L2FLOW_INSTRUMENT_DERIVED_EVENT_ORDER_REVISION_V1 ||
        payload.event_kind >
            L2FLOW_INSTRUMENT_DERIVED_EVENT_STATUS_V1 ||
        payload.operation > 2U || payload.finality > 2U ||
        payload.side > 4U || payload.side_source > 2U ||
        payload.aggressor > 3U || payload.phase > 7U ||
        payload.phase_at_first > 7U ||
        payload.phase_at_add > 7U ||
        payload.phase_at_last > 7U || payload.order_type > 3U ||
        payload.order_source > 2U || payload.price_source > 3U ||
        payload.original_quantity_status > 2U ||
        !BoolByte(payload.price_valid) ||
        !BoolByte(payload.execution_boundary_price_valid) ||
        !BoolByte(payload.trade_amount_valid) ||
        !BoolByte(payload.published_quantity_valid) ||
        !BoolByte(payload.original_quantity_valid) ||
        !BoolByte(payload.remaining_quantity_valid) ||
        !BoolByte(payload.source_matched_quantity_valid) ||
        !BoolByte(payload.add_seen) ||
        !BoolByte(payload.apply_to_book) ||
        !BoolByte(payload.referenced_order_found) ||
        !BoolByte(payload.side_from_order) ||
        !BoolByte(payload.event_time_valid) ||
        !BoolByte(payload.event_time_unix_ns_valid) ||
        !BoolByte(payload.vendor_local_time_valid)) {
        return false;
    }

    switch (payload.event_kind) {
        case L2FLOW_INSTRUMENT_DERIVED_EVENT_ORDER_REVISION_V1:
            return payload.order_id > 0 && payload.revision > 0U;
        case L2FLOW_INSTRUMENT_DERIVED_EVENT_TRADE_V1:
            return payload.quantity > 0 && payload.price_valid == 1U;
        case L2FLOW_INSTRUMENT_DERIVED_EVENT_CANCEL_V1:
            return payload.order_id > 0 && payload.quantity > 0;
        case L2FLOW_INSTRUMENT_DERIVED_EVENT_STATUS_V1:
            return payload.market ==
                   L2FLOW_INSTRUMENT_DERIVED_EVENT_MARKET_SHANGHAI_V1;
        default:
            return false;
    }
}

std::string_view OrderEventDeltaRingCreateErrorNameV1(
    OrderEventDeltaRingCreateErrorV1 error) noexcept {
    switch (error) {
        case OrderEventDeltaRingCreateErrorV1::kNone:
            return "none";
        case OrderEventDeltaRingCreateErrorV1::kNullOutput:
            return "null_output";
        case OrderEventDeltaRingCreateErrorV1::
            kInvalidConfiguration:
            return "invalid_configuration";
        case OrderEventDeltaRingCreateErrorV1::kLayoutOverflow:
            return "layout_overflow";
        case OrderEventDeltaRingCreateErrorV1::
            kMappingCreateFailed:
            return "mapping_create_failed";
        case OrderEventDeltaRingCreateErrorV1::
            kReadOnlyHandleFailed:
            return "read_only_handle_failed";
        case OrderEventDeltaRingCreateErrorV1::kSealFailed:
            return "seal_failed";
        case OrderEventDeltaRingCreateErrorV1::
            kResourceExhausted:
            return "resource_exhausted";
        case OrderEventDeltaRingCreateErrorV1::
            kUnexpectedFailure:
            return "unexpected_failure";
    }
    return "unknown";
}

std::string_view OrderEventDeltaPublishErrorNameV1(
    OrderEventDeltaPublishErrorV1 error) noexcept {
    switch (error) {
        case OrderEventDeltaPublishErrorV1::kNone:
            return "none";
        case OrderEventDeltaPublishErrorV1::kInvalidArgument:
            return "invalid_argument";
        case OrderEventDeltaPublishErrorV1::kUnavailable:
            return "unavailable";
        case OrderEventDeltaPublishErrorV1::kSourceSequenceGap:
            return "source_sequence_gap";
        case OrderEventDeltaPublishErrorV1::kBatchTooLarge:
            return "batch_too_large";
        case OrderEventDeltaPublishErrorV1::kSequenceExhausted:
            return "sequence_exhausted";
        case OrderEventDeltaPublishErrorV1::kSlotTagExhausted:
            return "slot_tag_exhausted";
        case OrderEventDeltaPublishErrorV1::kFailed:
            return "failed";
    }
    return "unknown";
}

std::string_view OrderEventDeltaReaderOpenErrorNameV1(
    OrderEventDeltaReaderOpenErrorV1 error) noexcept {
    switch (error) {
        case OrderEventDeltaReaderOpenErrorV1::kNone:
            return "none";
        case OrderEventDeltaReaderOpenErrorV1::kNullOutput:
            return "null_output";
        case OrderEventDeltaReaderOpenErrorV1::kInvalidArgument:
            return "invalid_argument";
        case OrderEventDeltaReaderOpenErrorV1::kDescriptorInvalid:
            return "descriptor_invalid";
        case OrderEventDeltaReaderOpenErrorV1::
            kDescriptorNotReadOnly:
            return "descriptor_not_read_only";
        case OrderEventDeltaReaderOpenErrorV1::kMappingFailed:
            return "mapping_failed";
        case OrderEventDeltaReaderOpenErrorV1::kLayoutInvalid:
            return "layout_invalid";
        case OrderEventDeltaReaderOpenErrorV1::kSessionMismatch:
            return "session_mismatch";
        case OrderEventDeltaReaderOpenErrorV1::kUnavailable:
            return "unavailable";
        case OrderEventDeltaReaderOpenErrorV1::
            kResourceExhausted:
            return "resource_exhausted";
        case OrderEventDeltaReaderOpenErrorV1::
            kUnexpectedFailure:
            return "unexpected_failure";
    }
    return "unknown";
}

std::string_view OrderEventDeltaReadErrorNameV1(
    OrderEventDeltaReadErrorV1 error) noexcept {
    switch (error) {
        case OrderEventDeltaReadErrorV1::kNone:
            return "none";
        case OrderEventDeltaReadErrorV1::kInvalidArgument:
            return "invalid_argument";
        case OrderEventDeltaReadErrorV1::kUnavailable:
            return "unavailable";
        case OrderEventDeltaReadErrorV1::kLayoutInvalid:
            return "layout_invalid";
        case OrderEventDeltaReadErrorV1::kInconsistentRead:
            return "inconsistent_read";
        case OrderEventDeltaReadErrorV1::kOverrun:
            return "overrun";
    }
    return "unknown";
}

class OrderEventDeltaRingProducerV1::Impl final {
public:
    explicit Impl(OrderEventDeltaRingConfigV1 config)
        : config_(std::move(config)) {}

    ~Impl() {
        CloseDescriptor(&read_only_fd_);
        if (mapping_ != MAP_FAILED) {
            static_cast<void>(::munmap(
                mapping_, static_cast<std::size_t>(mapping_bytes_)));
        }
        CloseDescriptor(&memfd_);
    }

    [[nodiscard]] OrderEventDeltaRingCreateErrorV1 Initialize(
        int* system_error_number) noexcept {
        SetSystemError(system_error_number, 0);
        if constexpr (std::endian::native != std::endian::little) {
            return OrderEventDeltaRingCreateErrorV1::
                kInvalidConfiguration;
        }
        if (!IdentityNonzero(config_.run_id) ||
            config_.session_epoch == 0U || config_.trade_date == 0U ||
            config_.ring_capacity == 0U ||
            !KnownTemporalCoverage(static_cast<std::uint32_t>(
                config_.temporal_coverage)) ||
            !KnownStreamQuality(static_cast<std::uint32_t>(
                config_.stream_quality)) ||
            config_.maximum_mapping_bytes <
                sizeof(OrderEventDeltaHeaderV1)) {
            return OrderEventDeltaRingCreateErrorV1::
                kInvalidConfiguration;
        }

        std::uint64_t slots_bytes = 0U;
        std::uint64_t logical_end = 0U;
        if (!CheckedMultiply(
                config_.ring_capacity,
                sizeof(OrderEventDeltaSlotV1),
                &slots_bytes) ||
            !CheckedAdd(
                sizeof(OrderEventDeltaHeaderV1),
                slots_bytes,
                &logical_end) ||
            !AlignUp(logical_end, kPageBytes, &mapping_bytes_) ||
            mapping_bytes_ > config_.maximum_mapping_bytes ||
            mapping_bytes_ >
                static_cast<std::uint64_t>(
                    std::numeric_limits<std::size_t>::max()) ||
            mapping_bytes_ >
                static_cast<std::uint64_t>(
                    std::numeric_limits<off_t>::max())) {
            return OrderEventDeltaRingCreateErrorV1::kLayoutOverflow;
        }

        memfd_ = ::memfd_create(
            "l2flow-order-event-delta-v1",
            MFD_CLOEXEC | MFD_ALLOW_SEALING);
        if (memfd_ < 0 ||
            ::ftruncate(memfd_, static_cast<off_t>(mapping_bytes_)) !=
                0) {
            SetSystemError(system_error_number, errno);
            return OrderEventDeltaRingCreateErrorV1::
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
            return OrderEventDeltaRingCreateErrorV1::
                kMappingCreateFailed;
        }
        // A later fork must not inherit the producer's already-authorized
        // writable mapping. F_SEAL_FUTURE_WRITE blocks new writable mappings,
        // but does not revoke mappings which existed when the seal was added.
        if (::madvise(
                mapping_,
                static_cast<std::size_t>(mapping_bytes_),
                MADV_DONTFORK) != 0) {
            SetSystemError(system_error_number, errno);
            return OrderEventDeltaRingCreateErrorV1::
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
            return OrderEventDeltaRingCreateErrorV1::
                kReadOnlyHandleFailed;
        }
        constexpr int seals =
            F_SEAL_GROW | F_SEAL_SHRINK | F_SEAL_FUTURE_WRITE |
            F_SEAL_SEAL;
        if (::fcntl(memfd_, F_ADD_SEALS, seals) != 0) {
            SetSystemError(system_error_number, errno);
            return OrderEventDeltaRingCreateErrorV1::kSealFailed;
        }

        header_ = static_cast<OrderEventDeltaHeaderV1*>(mapping_);
        slots_ = reinterpret_cast<OrderEventDeltaSlotV1*>(
            static_cast<std::byte*>(mapping_) +
            sizeof(OrderEventDeltaHeaderV1));
        header_->magic = kOrderEventDeltaMagicV1;
        header_->abi_major = kOrderEventDeltaWireMajorV1;
        header_->abi_minor = kOrderEventDeltaWireMinorV1;
        header_->header_bytes = sizeof(OrderEventDeltaHeaderV1);
        header_->endian_marker = kOrderEventDeltaEndianMarkerV1;
        header_->producer_state = static_cast<std::uint32_t>(
            OrderEventDeltaProducerStateV1::kInitializing);
        header_->total_mapping_bytes = mapping_bytes_;
        CopyIdentity(config_.run_id, &header_->run_id);
        header_->session_epoch = config_.session_epoch;
        header_->trade_date = config_.trade_date;
        header_->ring_capacity = config_.ring_capacity;
        header_->slot_stride = sizeof(OrderEventDeltaSlotV1);
        header_->slots_offset = sizeof(OrderEventDeltaHeaderV1);
        header_->producer_started_monotonic_ns =
            config_.producer_started_monotonic_ns;
        header_->temporal_coverage = static_cast<std::uint32_t>(
            config_.temporal_coverage);
        header_->stream_quality = static_cast<std::uint32_t>(
            config_.stream_quality);
        Atomic(header_->producer_state)
            .store(
                static_cast<std::uint32_t>(
                    OrderEventDeltaProducerStateV1::kActive),
                std::memory_order_release);
        return OrderEventDeltaRingCreateErrorV1::kNone;
    }

    [[nodiscard]] OrderEventDeltaPublishErrorV1 PublishSourceTick(
        std::uint64_t source_tick_sequence,
        std::span<const OrderEventDeltaPayloadV1> events) noexcept {
        if (header_ == nullptr || slots_ == nullptr) {
            return OrderEventDeltaPublishErrorV1::kFailed;
        }
        if (state() != OrderEventDeltaProducerStateV1::kActive ||
            (Atomic(header_->flags).load(std::memory_order_acquire) &
             kOrderEventDeltaCoverageLostV1) != 0U) {
            return OrderEventDeltaPublishErrorV1::kUnavailable;
        }
        const std::uint64_t consumed =
            Atomic(header_->source_tick_consumed_sequence)
                .load(std::memory_order_acquire);
        if (consumed == std::numeric_limits<std::uint64_t>::max()) {
            MarkFailed();
            return OrderEventDeltaPublishErrorV1::kSequenceExhausted;
        }
        if (source_tick_sequence != consumed + 1U) {
            MarkFailed();
            return OrderEventDeltaPublishErrorV1::kSourceSequenceGap;
        }
        if (events.size() >
                static_cast<std::size_t>(config_.ring_capacity) ||
            (!events.empty() &&
             events.size() - 1U >
                 static_cast<std::size_t>(
                     std::numeric_limits<std::uint32_t>::max()))) {
            MarkFailed();
            return OrderEventDeltaPublishErrorV1::kBatchTooLarge;
        }

        const std::uint64_t published =
            Atomic(header_->event_published_sequence)
                .load(std::memory_order_acquire);
        if (!EventSequenceBatchFits(
                published,
                static_cast<std::uint64_t>(events.size()))) {
            MarkFailed();
            return OrderEventDeltaPublishErrorV1::kSequenceExhausted;
        }
        for (std::size_t index = 0U; index < events.size(); ++index) {
            const OrderEventDeltaPayloadV1& event = events[index];
            if (!OrderEventDeltaPayloadCanonicalV1(
                    event,
                    config_.trade_date,
                    source_tick_sequence) ||
                event.reserved0 !=
                    static_cast<std::uint32_t>(index)) {
                return OrderEventDeltaPublishErrorV1::
                    kInvalidArgument;
            }
        }

        // events.size() <= capacity, so this consecutive batch touches every
        // physical slot at most once. Checking every target tag here keeps
        // tag exhaustion from causing partial publication.
        for (std::size_t offset = 0U; offset < events.size();
             ++offset) {
            const std::uint64_t sequence =
                published + static_cast<std::uint64_t>(offset) + 1U;
            const std::uint64_t index =
                (sequence - 1U) % config_.ring_capacity;
            const std::uint64_t tag =
                Atomic(
                    slots_[static_cast<std::size_t>(index)]
                        .publish_tag)
                    .load(std::memory_order_acquire);
            if ((tag & 1U) != 0U ||
                tag >
                    std::numeric_limits<std::uint64_t>::max() - 2U) {
                MarkFailed();
                return OrderEventDeltaPublishErrorV1::
                    kSlotTagExhausted;
            }
        }

        for (std::size_t offset = 0U; offset < events.size();
             ++offset) {
            const std::uint64_t sequence =
                published + static_cast<std::uint64_t>(offset) + 1U;
            const std::uint64_t index =
                (sequence - 1U) % config_.ring_capacity;
            OrderEventDeltaPayloadV1 payload = events[offset];
            payload.derived_event_sequence = sequence;
            if (!PublishSlot(
                    &slots_[static_cast<std::size_t>(index)],
                    payload)) {
                MarkFailed();
                return OrderEventDeltaPublishErrorV1::kFailed;
            }
        }

        // Publish only the complete source-tick batch. Individual slot tags
        // may already be stable, but readers are bounded by this committed
        // prefix and cannot expose a partial batch.
        const std::uint64_t committed =
            published + static_cast<std::uint64_t>(events.size());
        Atomic(header_->event_published_sequence)
            .store(committed, std::memory_order_release);
        // This release is deliberately after the complete event-prefix
        // release.
        Atomic(header_->source_tick_consumed_sequence)
            .store(source_tick_sequence, std::memory_order_release);
        return OrderEventDeltaPublishErrorV1::kNone;
    }

    [[nodiscard]] bool DuplicateReadOnlyDescriptor(
        int* output,
        int* system_error_number) const noexcept {
        SetSystemError(system_error_number, 0);
        if (output == nullptr || read_only_fd_ < 0) {
            SetSystemError(system_error_number, EINVAL);
            return false;
        }
        *output = -1;
        int duplicate = -1;
        do {
            duplicate =
                ::fcntl(read_only_fd_, F_DUPFD_CLOEXEC, 0);
        } while (duplicate < 0 && errno == EINTR);
        if (duplicate < 0) {
            SetSystemError(system_error_number, errno);
            return false;
        }
        *output = duplicate;
        return true;
    }

    [[nodiscard]] bool UpdateHeartbeat(
        std::uint64_t monotonic_ns) noexcept {
        if (monotonic_ns == 0U || header_ == nullptr) {
            return false;
        }
        const OrderEventDeltaProducerStateV1 current = state();
        if (current != OrderEventDeltaProducerStateV1::kActive &&
            current != OrderEventDeltaProducerStateV1::kDraining) {
            return false;
        }
        Atomic(header_->heartbeat_monotonic_ns)
            .store(monotonic_ns, std::memory_order_release);
        return true;
    }

    [[nodiscard]] bool BeginDraining() noexcept {
        if (header_ == nullptr) {
            return false;
        }
        std::uint32_t expected = static_cast<std::uint32_t>(
            OrderEventDeltaProducerStateV1::kActive);
        return Atomic(header_->producer_state)
            .compare_exchange_strong(
                expected,
                static_cast<std::uint32_t>(
                    OrderEventDeltaProducerStateV1::kDraining),
                std::memory_order_release,
                std::memory_order_acquire);
    }

    [[nodiscard]] bool StopClean() noexcept {
        if (header_ == nullptr ||
            (Atomic(header_->flags).load(std::memory_order_acquire) &
             kOrderEventDeltaCoverageLostV1) != 0U) {
            return false;
        }
        std::atomic_ref<std::uint32_t> state_ref =
            Atomic(header_->producer_state);
        std::uint32_t current =
            state_ref.load(std::memory_order_acquire);
        while (current ==
                   static_cast<std::uint32_t>(
                       OrderEventDeltaProducerStateV1::kActive) ||
               current ==
                   static_cast<std::uint32_t>(
                       OrderEventDeltaProducerStateV1::kDraining)) {
            if (state_ref.compare_exchange_weak(
                    current,
                    static_cast<std::uint32_t>(
                        OrderEventDeltaProducerStateV1::
                            kStoppedClean),
                    std::memory_order_release,
                    std::memory_order_acquire)) {
                return true;
            }
        }
        return current ==
               static_cast<std::uint32_t>(
                   OrderEventDeltaProducerStateV1::kStoppedClean);
    }

    void MarkFailed() noexcept {
        if (header_ == nullptr) {
            return;
        }
        Atomic(header_->flags)
            .fetch_or(
                kOrderEventDeltaCoverageLostV1,
                std::memory_order_acq_rel);
        Atomic(header_->producer_state)
            .store(
                static_cast<std::uint32_t>(
                    OrderEventDeltaProducerStateV1::kFailed),
                std::memory_order_release);
    }

    [[nodiscard]] OrderEventDeltaSessionV1 session() const noexcept {
        return header_ == nullptr ? OrderEventDeltaSessionV1{}
                                  : SessionFromHeader(*header_);
    }

    [[nodiscard]] std::uint64_t published_event_sequence()
        const noexcept {
        return header_ == nullptr
                   ? 0U
                   : Atomic(header_->event_published_sequence)
                         .load(std::memory_order_acquire);
    }

    [[nodiscard]] std::uint64_t consumed_source_tick_sequence()
        const noexcept {
        return header_ == nullptr
                   ? 0U
                   : Atomic(header_->source_tick_consumed_sequence)
                         .load(std::memory_order_acquire);
    }

    [[nodiscard]] OrderEventDeltaProducerStateV1 state()
        const noexcept {
        if (header_ == nullptr) {
            return OrderEventDeltaProducerStateV1::kFailed;
        }
        const std::uint32_t raw =
            Atomic(header_->producer_state)
                .load(std::memory_order_acquire);
        return KnownState(raw)
                   ? static_cast<OrderEventDeltaProducerStateV1>(raw)
                   : OrderEventDeltaProducerStateV1::kFailed;
    }

private:
    OrderEventDeltaRingConfigV1 config_;
    int memfd_ = -1;
    int read_only_fd_ = -1;
    void* mapping_ = MAP_FAILED;
    std::uint64_t mapping_bytes_ = 0U;
    OrderEventDeltaHeaderV1* header_ = nullptr;
    OrderEventDeltaSlotV1* slots_ = nullptr;
};

OrderEventDeltaRingProducerV1::OrderEventDeltaRingProducerV1(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

OrderEventDeltaRingProducerV1::~OrderEventDeltaRingProducerV1() =
    default;

OrderEventDeltaRingCreateErrorV1
OrderEventDeltaRingProducerV1::Create(
    OrderEventDeltaRingConfigV1 config,
    std::unique_ptr<OrderEventDeltaRingProducerV1>* output,
    int* system_error_number) noexcept {
    SetSystemError(system_error_number, 0);
    if (output == nullptr) {
        return OrderEventDeltaRingCreateErrorV1::kNullOutput;
    }
    output->reset();
    try {
        auto impl = std::make_unique<Impl>(std::move(config));
        const OrderEventDeltaRingCreateErrorV1 error =
            impl->Initialize(system_error_number);
        if (error != OrderEventDeltaRingCreateErrorV1::kNone) {
            return error;
        }
        output->reset(new OrderEventDeltaRingProducerV1(
            std::move(impl)));
        return OrderEventDeltaRingCreateErrorV1::kNone;
    } catch (const std::bad_alloc&) {
        return OrderEventDeltaRingCreateErrorV1::
            kResourceExhausted;
    } catch (...) {
        return OrderEventDeltaRingCreateErrorV1::
            kUnexpectedFailure;
    }
}

OrderEventDeltaPublishErrorV1
OrderEventDeltaRingProducerV1::PublishSourceTick(
    std::uint64_t source_tick_sequence,
    std::span<const OrderEventDeltaPayloadV1> events) noexcept {
    return impl_ == nullptr
               ? OrderEventDeltaPublishErrorV1::kFailed
               : impl_->PublishSourceTick(
                     source_tick_sequence, events);
}

bool OrderEventDeltaRingProducerV1::DuplicateReadOnlyDescriptor(
    int* output,
    int* system_error_number) const noexcept {
    return impl_ != nullptr &&
           impl_->DuplicateReadOnlyDescriptor(
               output, system_error_number);
}

bool OrderEventDeltaRingProducerV1::UpdateHeartbeat(
    std::uint64_t monotonic_ns) noexcept {
    return impl_ != nullptr &&
           impl_->UpdateHeartbeat(monotonic_ns);
}

bool OrderEventDeltaRingProducerV1::BeginDraining() noexcept {
    return impl_ != nullptr && impl_->BeginDraining();
}

bool OrderEventDeltaRingProducerV1::StopClean() noexcept {
    return impl_ != nullptr && impl_->StopClean();
}

void OrderEventDeltaRingProducerV1::MarkFailed() noexcept {
    if (impl_ != nullptr) {
        impl_->MarkFailed();
    }
}

OrderEventDeltaSessionV1
OrderEventDeltaRingProducerV1::session() const noexcept {
    return impl_ == nullptr ? OrderEventDeltaSessionV1{}
                            : impl_->session();
}

std::uint64_t
OrderEventDeltaRingProducerV1::published_event_sequence()
    const noexcept {
    return impl_ == nullptr ? 0U
                            : impl_->published_event_sequence();
}

std::uint64_t
OrderEventDeltaRingProducerV1::consumed_source_tick_sequence()
    const noexcept {
    return impl_ == nullptr
               ? 0U
               : impl_->consumed_source_tick_sequence();
}

OrderEventDeltaProducerStateV1
OrderEventDeltaRingProducerV1::state() const noexcept {
    return impl_ == nullptr
               ? OrderEventDeltaProducerStateV1::kFailed
               : impl_->state();
}

class OrderEventDeltaRingReaderV1::Impl final {
public:
    explicit Impl(std::uint64_t start_event_sequence) noexcept
        : next_sequence_(start_event_sequence) {}

    ~Impl() {
        if (mapping_ != MAP_FAILED) {
            static_cast<void>(::munmap(
                mapping_, static_cast<std::size_t>(mapping_bytes_)));
        }
        CloseDescriptor(&descriptor_);
    }

    [[nodiscard]] OrderEventDeltaReaderOpenErrorV1 Initialize(
        int descriptor,
        const OrderEventDeltaSessionV1& expected_session,
        int* system_error_number) noexcept {
        SetSystemError(system_error_number, 0);
        if constexpr (std::endian::native != std::endian::little) {
            return OrderEventDeltaReaderOpenErrorV1::
                kInvalidArgument;
        }
        if (descriptor < 0 ||
            !IdentityNonzero(expected_session.run_id) ||
            expected_session.session_epoch == 0U ||
            expected_session.trade_date == 0U ||
            expected_session.ring_capacity == 0U ||
            !KnownTemporalCoverage(static_cast<std::uint32_t>(
                expected_session.temporal_coverage)) ||
            !KnownStreamQuality(static_cast<std::uint32_t>(
                expected_session.stream_quality)) ||
            expected_session.total_mapping_bytes <
                sizeof(OrderEventDeltaHeaderV1)) {
            return OrderEventDeltaReaderOpenErrorV1::
                kInvalidArgument;
        }
        const int descriptor_flags = ::fcntl(descriptor, F_GETFL);
        if (descriptor_flags < 0) {
            SetSystemError(system_error_number, errno);
            return OrderEventDeltaReaderOpenErrorV1::
                kDescriptorInvalid;
        }
        if ((descriptor_flags & O_ACCMODE) != O_RDONLY) {
            return OrderEventDeltaReaderOpenErrorV1::
                kDescriptorNotReadOnly;
        }
        const int seals = ::fcntl(descriptor, F_GET_SEALS);
        constexpr int required_seals =
            F_SEAL_GROW | F_SEAL_SHRINK | F_SEAL_FUTURE_WRITE |
            F_SEAL_SEAL;
        if (seals < 0 || (seals & required_seals) != required_seals) {
            if (seals < 0) {
                SetSystemError(system_error_number, errno);
            }
            return OrderEventDeltaReaderOpenErrorV1::
                kDescriptorInvalid;
        }

        struct stat descriptor_stat {};
        if (::fstat(descriptor, &descriptor_stat) != 0 ||
            descriptor_stat.st_size <= 0) {
            SetSystemError(system_error_number, errno);
            return OrderEventDeltaReaderOpenErrorV1::
                kDescriptorInvalid;
        }
        mapping_bytes_ =
            static_cast<std::uint64_t>(descriptor_stat.st_size);
        if (mapping_bytes_ >
                static_cast<std::uint64_t>(
                    std::numeric_limits<std::size_t>::max()) ||
            mapping_bytes_ !=
                expected_session.total_mapping_bytes) {
            return OrderEventDeltaReaderOpenErrorV1::
                kSessionMismatch;
        }

        do {
            descriptor_ =
                ::fcntl(descriptor, F_DUPFD_CLOEXEC, 0);
        } while (descriptor_ < 0 && errno == EINTR);
        if (descriptor_ < 0) {
            SetSystemError(system_error_number, errno);
            return OrderEventDeltaReaderOpenErrorV1::
                kDescriptorInvalid;
        }
        mapping_ = ::mmap(
            nullptr,
            static_cast<std::size_t>(mapping_bytes_),
            PROT_READ,
            MAP_SHARED,
            descriptor_,
            0);
        if (mapping_ == MAP_FAILED) {
            SetSystemError(system_error_number, errno);
            return OrderEventDeltaReaderOpenErrorV1::kMappingFailed;
        }
        header_ =
            static_cast<const OrderEventDeltaHeaderV1*>(mapping_);
        if (!HeaderLayoutValid(*header_, mapping_bytes_)) {
            return OrderEventDeltaReaderOpenErrorV1::kLayoutInvalid;
        }
        if (SessionFromHeader(*header_) != expected_session) {
            return OrderEventDeltaReaderOpenErrorV1::
                kSessionMismatch;
        }
        if (!HeaderHealthy(*header_)) {
            return OrderEventDeltaReaderOpenErrorV1::kUnavailable;
        }
        slots_ = reinterpret_cast<const OrderEventDeltaSlotV1*>(
            static_cast<const std::byte*>(mapping_) +
            header_->slots_offset);
        session_ = expected_session;
        return OrderEventDeltaReaderOpenErrorV1::kNone;
    }

    [[nodiscard]] OrderEventDeltaReadErrorV1 Read(
        std::uint64_t expected_sequence,
        std::span<OrderEventDeltaPayloadV1> output,
        OrderEventDeltaReadResultV1* result) const noexcept {
        if (result == nullptr || expected_sequence == 0U ||
            header_ == nullptr || slots_ == nullptr) {
            return OrderEventDeltaReadErrorV1::kInvalidArgument;
        }
        *result = {};
        result->producer_state =
            Atomic(header_->producer_state)
                .load(std::memory_order_acquire);
        result->header_flags =
            Atomic(header_->flags).load(std::memory_order_acquire);
        if (failed_ || !HeaderHealthy(*header_)) {
            failed_ = true;
            result->next_sequence = next_sequence_;
            return OrderEventDeltaReadErrorV1::kUnavailable;
        }
        if (expected_sequence != next_sequence_) {
            result->next_sequence = next_sequence_;
            return OrderEventDeltaReadErrorV1::kInvalidArgument;
        }
        result->next_sequence = expected_sequence;

        // Loading the source cursor first is intentional. If it observes a
        // completed source batch, its acquire synchronizes with the release
        // after that batch; the following event-prefix load cannot precede
        // any event publication covered by that source cursor.
        const std::uint64_t source_consumed =
            Atomic(header_->source_tick_consumed_sequence)
                .load(std::memory_order_acquire);
        const std::uint64_t published =
            Atomic(header_->event_published_sequence)
                .load(std::memory_order_acquire);
        result->published_event_sequence = published;
        result->consumed_source_tick_sequence = source_consumed;
        result->heartbeat_monotonic_ns =
            Atomic(header_->heartbeat_monotonic_ns)
                .load(std::memory_order_acquire);

        const std::uint64_t oldest =
            published >= session_.ring_capacity
                ? published - session_.ring_capacity + 1U
                : 1U;
        if (expected_sequence < oldest) {
            result->observed_sequence = oldest;
            failed_ = true;
            return OrderEventDeltaReadErrorV1::kOverrun;
        }

        std::uint64_t sequence = expected_sequence;
        while (result->written < output.size() &&
               sequence <= published) {
            const std::uint64_t index =
                (sequence - 1U) % session_.ring_capacity;
            const OrderEventDeltaSlotV1& slot =
                slots_[static_cast<std::size_t>(index)];
            if (!AllZero(slot.reserved)) {
                result->written = 0U;
                result->next_sequence = expected_sequence;
                failed_ = true;
                return OrderEventDeltaReadErrorV1::kLayoutInvalid;
            }
            OrderEventDeltaPayloadV1 payload{};
            if (CopySlot(slot, &payload) !=
                StableCopyResult::kCopied) {
                result->written = 0U;
                result->next_sequence = expected_sequence;
                failed_ = true;
                return OrderEventDeltaReadErrorV1::
                    kInconsistentRead;
            }
            if (payload.derived_event_sequence != sequence) {
                result->observed_sequence =
                    payload.derived_event_sequence;
                result->written = 0U;
                result->next_sequence = expected_sequence;
                failed_ = true;
                return payload.derived_event_sequence > sequence
                           ? OrderEventDeltaReadErrorV1::kOverrun
                           : OrderEventDeltaReadErrorV1::
                                 kInconsistentRead;
            }
            if (!OrderEventDeltaPayloadCanonicalV1(
                    payload,
                    session_.trade_date,
                    payload.tick_stream_sequence)) {
                result->written = 0U;
                result->next_sequence = expected_sequence;
                failed_ = true;
                return OrderEventDeltaReadErrorV1::kLayoutInvalid;
            }
            output[result->written] = payload;
            ++result->written;
            ++sequence;
        }
        result->next_sequence = sequence;
        if (!HeaderHealthy(*header_)) {
            result->written = 0U;
            result->next_sequence = expected_sequence;
            failed_ = true;
            return OrderEventDeltaReadErrorV1::kUnavailable;
        }
        next_sequence_ = sequence;
        return OrderEventDeltaReadErrorV1::kNone;
    }

    [[nodiscard]] OrderEventDeltaSessionV1 session() const noexcept {
        return session_;
    }

    [[nodiscard]] OrderEventDeltaProducerStateV1 state()
        const noexcept {
        if (header_ == nullptr) {
            return OrderEventDeltaProducerStateV1::kFailed;
        }
        const std::uint32_t raw =
            Atomic(header_->producer_state)
                .load(std::memory_order_acquire);
        return KnownState(raw)
                   ? static_cast<OrderEventDeltaProducerStateV1>(raw)
                   : OrderEventDeltaProducerStateV1::kFailed;
    }

private:
    int descriptor_ = -1;
    void* mapping_ = MAP_FAILED;
    std::uint64_t mapping_bytes_ = 0U;
    const OrderEventDeltaHeaderV1* header_ = nullptr;
    const OrderEventDeltaSlotV1* slots_ = nullptr;
    OrderEventDeltaSessionV1 session_{};
    mutable bool failed_ = false;
    mutable std::uint64_t next_sequence_;
};

OrderEventDeltaRingReaderV1::OrderEventDeltaRingReaderV1(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

OrderEventDeltaRingReaderV1::~OrderEventDeltaRingReaderV1() =
    default;

OrderEventDeltaReaderOpenErrorV1
OrderEventDeltaRingReaderV1::Open(
    int read_only_descriptor,
    const OrderEventDeltaSessionV1& expected_session,
    std::unique_ptr<OrderEventDeltaRingReaderV1>* output,
    int* system_error_number) noexcept {
    return OpenAt(
        read_only_descriptor,
        expected_session,
        1U,
        output,
        system_error_number);
}

OrderEventDeltaReaderOpenErrorV1
OrderEventDeltaRingReaderV1::OpenAt(
    int read_only_descriptor,
    const OrderEventDeltaSessionV1& expected_session,
    std::uint64_t start_event_sequence,
    std::unique_ptr<OrderEventDeltaRingReaderV1>* output,
    int* system_error_number) noexcept {
    SetSystemError(system_error_number, 0);
    if (output == nullptr) {
        return OrderEventDeltaReaderOpenErrorV1::kNullOutput;
    }
    output->reset();
    if (start_event_sequence == 0U) {
        return OrderEventDeltaReaderOpenErrorV1::kInvalidArgument;
    }
    try {
        auto impl =
            std::make_unique<Impl>(start_event_sequence);
        const OrderEventDeltaReaderOpenErrorV1 error =
            impl->Initialize(
                read_only_descriptor,
                expected_session,
                system_error_number);
        if (error != OrderEventDeltaReaderOpenErrorV1::kNone) {
            return error;
        }
        output->reset(
            new OrderEventDeltaRingReaderV1(std::move(impl)));
        return OrderEventDeltaReaderOpenErrorV1::kNone;
    } catch (const std::bad_alloc&) {
        return OrderEventDeltaReaderOpenErrorV1::
            kResourceExhausted;
    } catch (...) {
        return OrderEventDeltaReaderOpenErrorV1::
            kUnexpectedFailure;
    }
}

OrderEventDeltaReadErrorV1 OrderEventDeltaRingReaderV1::Read(
    std::uint64_t expected_sequence,
    std::span<OrderEventDeltaPayloadV1> output,
    OrderEventDeltaReadResultV1* result) const noexcept {
    return impl_ == nullptr
               ? OrderEventDeltaReadErrorV1::kUnavailable
               : impl_->Read(expected_sequence, output, result);
}

OrderEventDeltaSessionV1
OrderEventDeltaRingReaderV1::session() const noexcept {
    return impl_ == nullptr ? OrderEventDeltaSessionV1{}
                            : impl_->session();
}

OrderEventDeltaProducerStateV1
OrderEventDeltaRingReaderV1::state() const noexcept {
    return impl_ == nullptr
               ? OrderEventDeltaProducerStateV1::kFailed
               : impl_->state();
}

}  // namespace l2flow::ipc
