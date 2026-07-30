#include "l2flow/ipc/realtime_shm_reader_c_v2.h"

#include "l2flow/ipc/realtime_instrument_tick_delta_wire_v2.h"
#include "l2flow/ipc/realtime_wire_v2.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cerrno>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <mutex>
#include <new>
#include <type_traits>
#include <vector>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/un.h>
#include <unistd.h>

#ifndef F_SEAL_FUTURE_WRITE
#define F_SEAL_FUTURE_WRITE 0x0010
#endif

static_assert(sizeof(l2flow_shm_session_info_v2) == 240U);
static_assert(
    offsetof(l2flow_shm_session_info_v2, session_epoch) == 80U);
static_assert(
    offsetof(l2flow_shm_session_info_v2, accepted_sequence) == 104U);
static_assert(
    offsetof(l2flow_shm_session_info_v2, tick_ring_capacity) == 128U);
static_assert(
    offsetof(l2flow_shm_session_info_v2, trade_date) == 176U);
static_assert(
    offsetof(l2flow_shm_session_info_v2, capacity) == 188U);
static_assert(
    offsetof(l2flow_shm_session_info_v2, bound_count) == 204U);
static_assert(
    offsetof(
        l2flow_shm_session_info_v2, catalog_trade_date) == 224U);
static_assert(
    offsetof(l2flow_shm_session_info_v2, catalog_version) == 232U);
static_assert(sizeof(l2flow_selection_envelope_v2) == 144U);
static_assert(
    offsetof(l2flow_selection_envelope_v2, session_epoch) == 48U);
static_assert(
    offsetof(
        l2flow_selection_envelope_v2, accepted_sequence) == 72U);
static_assert(
    offsetof(l2flow_selection_envelope_v2, capacity) == 96U);
static_assert(
    offsetof(l2flow_selection_envelope_v2, selection_scope) ==
    128U);
static_assert(
    offsetof(l2flow_selection_envelope_v2, returned_row_count) ==
    132U);
static_assert(
    offsetof(l2flow_selection_envelope_v2, reserved) == 136U);
static_assert(
    sizeof(l2flow_instrument_raw_event_history_endpoint_v2) == 248U);
static_assert(
    offsetof(
        l2flow_instrument_raw_event_history_endpoint_v2,
        session_epoch) == 16U);
static_assert(
    offsetof(
        l2flow_instrument_raw_event_history_endpoint_v2,
        catalog_digest) == 96U);
static_assert(
    offsetof(
        l2flow_instrument_raw_event_history_endpoint_v2,
        source_stream_ids) == 160U);
static_assert(
    offsetof(
        l2flow_instrument_raw_event_history_endpoint_v2,
        source_sequence_exclusive) == 176U);
static_assert(
    offsetof(
        l2flow_instrument_raw_event_history_endpoint_v2,
        trade_date) == 208U);
static_assert(
    sizeof(l2flow_instrument_raw_event_history_checkpoint_v2) == 312U);
static_assert(
    offsetof(
        l2flow_instrument_raw_event_history_checkpoint_v2,
        instrument_id) == 248U);
static_assert(
    offsetof(
        l2flow_instrument_raw_event_history_checkpoint_v2,
        instrument_event_source_record_counts) == 256U);
static_assert(
    offsetof(
        l2flow_instrument_raw_event_history_checkpoint_v2,
        reserved) == 304U);
static_assert(
    sizeof(l2flow_instrument_raw_event_history_metadata_v2) == 720U);
static_assert(
    offsetof(
        l2flow_instrument_raw_event_history_metadata_v2,
        base_checkpoint) == 8U);
static_assert(
    offsetof(
        l2flow_instrument_raw_event_history_metadata_v2,
        target_checkpoint) == 320U);
static_assert(
    offsetof(
        l2flow_instrument_raw_event_history_metadata_v2,
        delta_event_source_record_counts) == 632U);
static_assert(
    offsetof(
        l2flow_instrument_raw_event_history_metadata_v2,
        reserved) == 712U);
static_assert(
    std::is_trivially_copyable_v<
        l2flow_instrument_raw_event_history_endpoint_v2>);
static_assert(
    std::is_trivially_copyable_v<
        l2flow_instrument_raw_event_history_checkpoint_v2>);
static_assert(
    std::is_trivially_copyable_v<
        l2flow_instrument_raw_event_history_metadata_v2>);
static_assert(
    static_cast<std::uint32_t>(
        L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_SOURCE_MASK_V2) ==
    l2flow::ipc::kRealtimeInstrumentTickDeltaSourceMaskV2);
static_assert(
    static_cast<std::uint32_t>(
        L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_EVENT_COVERAGE_COMPLETE_V2) ==
    static_cast<std::uint32_t>(
        l2flow::ipc::
            kRealtimeInstrumentTickDeltaTickRecordCoverageCompleteV2));
static_assert(
    L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_CORE_WIRE_V2 ==
    static_cast<int>(
        l2flow::ipc::
            RealtimeInstrumentTickDeltaPayloadProjectionV2::
                kCoreV2));
static_assert(
    std::atomic_ref<std::uint8_t>::is_always_lock_free,
    "Wire V2 row publication requires lock-free byte atomic_ref");
static_assert(
    std::atomic_ref<std::uint32_t>::is_always_lock_free,
    "Wire V2 status publication requires lock-free 32-bit atomic_ref");
static_assert(
    std::atomic_ref<std::uint64_t>::is_always_lock_free,
    "Wire V2 publication requires lock-free 64-bit atomic_ref");
static_assert(
    alignof(l2flow::ipc::RealtimeWireHeaderV2) >=
        alignof(std::uint64_t));
static_assert(
    alignof(l2flow::ipc::RealtimeWireInstrumentV2) >=
        alignof(std::uint64_t));

namespace {

using l2flow::ipc::RealtimeCatalogScopeV2;
using l2flow::ipc::RealtimeGenerationEndpointV2;
using l2flow::ipc::RealtimeInstrumentBindingStateV2;
using l2flow::ipc::RealtimeInstrumentTickDeltaBaseKindV2;
using l2flow::ipc::RealtimeInstrumentTickDeltaCheckpointV2;
using l2flow::ipc::RealtimeInstrumentTickDeltaControlOpcodeV2;
using l2flow::ipc::RealtimeInstrumentTickDeltaControlStatusV2;
using l2flow::ipc::RealtimeInstrumentTickDeltaMetadataV2;
using l2flow::ipc::RealtimeInstrumentTickDeltaOpenInstrumentRequestV2;
using l2flow::ipc::RealtimeInstrumentTickDeltaOpenInstrumentResponseV2;
using l2flow::ipc::RealtimeInstrumentTickDeltaOpenSessionRequestV2;
using l2flow::ipc::RealtimeInstrumentTickDeltaOpenSessionResponseV2;
using l2flow::ipc::RealtimeInstrumentTickDeltaPageHeaderV2;
using l2flow::ipc::RealtimeInstrumentTickDeltaReadRequestV2;
using l2flow::ipc::RealtimeInstrumentTickDeltaReadResponseV2;
using l2flow::ipc::RealtimeRegionKindV2;
using l2flow::ipc::RealtimeSelectionScopeV2;
using l2flow::ipc::RealtimeServerStateV2;
using l2flow::ipc::RealtimeWireCommonRecordV2;
using l2flow::ipc::RealtimeWireDecimalV2;
using l2flow::ipc::RealtimeWireDigest256V2;
using l2flow::ipc::RealtimeWireHeaderV2;
using l2flow::ipc::RealtimeWireInstrumentV2;
using l2flow::ipc::RealtimeWireBookLevelV2;
using l2flow::ipc::RealtimeWireKLinePayloadV2;
using l2flow::ipc::RealtimeWireKLineSlotV2;
using l2flow::ipc::RealtimeWireKLineWindowV2;
using l2flow::ipc::RealtimeWireQuantityV2;
using l2flow::ipc::RealtimeWireQueueHeaderV2;
using l2flow::ipc::RealtimeWireRegionDescriptorV2;
using l2flow::ipc::RealtimeWireSnapshotPayloadV2;
using l2flow::ipc::RealtimeWireSnapshotSlotV2;
using l2flow::ipc::RealtimeWireTickPayloadV2;
using l2flow::ipc::RealtimeWireTickSlotV2;

constexpr std::size_t kReadAttempts = 5U;

template <typename Integer>
std::atomic_ref<Integer> Atomic(const Integer& value) noexcept {
    return std::atomic_ref<Integer>(const_cast<Integer&>(value));
}

template <std::size_t Size>
bool AnyNonzero(
    const std::array<std::uint8_t, Size>& value) noexcept {
    return std::any_of(
        value.begin(),
        value.end(),
        [](std::uint8_t byte) noexcept { return byte != 0U; });
}

bool AnyNonzero(const RealtimeWireDigest256V2& value) noexcept {
    return std::any_of(
        value.words.begin(),
        value.words.end(),
        [](std::uint64_t word) noexcept { return word != 0U; });
}

template <typename Value, std::size_t Size>
bool AllZero(const std::array<Value, Size>& value) noexcept {
    return std::all_of(
        value.begin(),
        value.end(),
        [](Value item) noexcept { return item == Value{}; });
}

bool CheckedEnd(
    std::uint64_t offset,
    std::uint64_t length,
    std::uint64_t extent) noexcept {
    return offset <= extent && length <= extent - offset;
}

bool KnownServerState(std::uint32_t state) noexcept {
    return state >= static_cast<std::uint32_t>(
                        RealtimeServerStateV2::kInitializing) &&
           state <=
               static_cast<std::uint32_t>(
                   RealtimeServerStateV2::kFailed);
}

bool HeaderFlagsValid(std::uint32_t flags) noexcept {
    constexpr std::uint32_t known =
        l2flow::ipc::kRealtimeHeaderCoverageLostV2 |
        l2flow::ipc::kRealtimeHeaderKLineEnabledV2;
    return (flags & ~known) == 0U;
}

bool HealthyForRead(const RealtimeWireHeaderV2& header) noexcept {
    const std::uint32_t flags =
        Atomic(header.flags).load(std::memory_order_acquire);
    const std::uint32_t state =
        Atomic(header.server_state).load(std::memory_order_acquire);
    const bool readable_state =
        state ==
            static_cast<std::uint32_t>(
                RealtimeServerStateV2::kActive) ||
        state ==
            static_cast<std::uint32_t>(
                RealtimeServerStateV2::kDraining) ||
        state ==
            static_cast<std::uint32_t>(
                RealtimeServerStateV2::kStoppedClean);
    return HeaderFlagsValid(flags) &&
           (flags &
            l2flow::ipc::kRealtimeHeaderCoverageLostV2) == 0U &&
           readable_state;
}

struct StatusSnapshot final {
    std::uint64_t publish_tag = 0U;
    std::uint64_t catalog_generation = 0U;
    std::uint64_t data_state_generation = 0U;
    RealtimeWireDigest256V2 catalog_digest{};
    std::uint32_t bound_count = 0U;
    std::uint32_t available_count = 0U;
    std::uint32_t snapshot_available_count = 0U;
    std::uint32_t tick_available_count = 0U;
    std::uint32_t factor_eligible_count = 0U;
    std::uint32_t reserved_count = 0U;
    std::uint64_t accepted_sequence = 0U;
    std::uint64_t applied_sequence = 0U;
};

enum class StableCopyResult : std::uint8_t {
    kCopied = 0U,
    kNeverPublished,
    kInconsistent,
    kInvalid,
};

StableCopyResult CopyStatus(
    const RealtimeWireHeaderV2& header,
    StatusSnapshot* output) noexcept {
    if (output == nullptr) {
        return StableCopyResult::kInvalid;
    }
    for (std::size_t attempt = 0U; attempt < kReadAttempts;
         ++attempt) {
        const std::uint64_t begin =
            Atomic(header.status_publish_tag)
                .load(std::memory_order_acquire);
        if (!l2flow::ipc::RealtimeStatusPublishTagStableV2(begin)) {
            continue;
        }
        StatusSnapshot snapshot{};
        snapshot.publish_tag = begin;
        snapshot.catalog_generation =
            Atomic(header.catalog_generation)
                .load(std::memory_order_relaxed);
        snapshot.data_state_generation =
            Atomic(header.data_state_generation)
                .load(std::memory_order_relaxed);
        for (std::size_t index = 0U;
             index < snapshot.catalog_digest.words.size();
             ++index) {
            snapshot.catalog_digest.words[index] =
                Atomic(header.catalog_digest.words[index])
                    .load(std::memory_order_relaxed);
        }
        snapshot.bound_count =
            Atomic(header.bound_count).load(std::memory_order_relaxed);
        snapshot.available_count =
            Atomic(header.available_count)
                .load(std::memory_order_relaxed);
        snapshot.snapshot_available_count =
            Atomic(header.snapshot_available_count)
                .load(std::memory_order_relaxed);
        snapshot.tick_available_count =
            Atomic(header.tick_available_count)
                .load(std::memory_order_relaxed);
        snapshot.factor_eligible_count =
            Atomic(header.factor_eligible_count)
                .load(std::memory_order_relaxed);
        snapshot.reserved_count =
            Atomic(header.reserved_count)
                .load(std::memory_order_relaxed);
        snapshot.accepted_sequence =
            Atomic(header.accepted_sequence)
                .load(std::memory_order_relaxed);
        snapshot.applied_sequence =
            Atomic(header.applied_sequence)
                .load(std::memory_order_relaxed);
        std::atomic_thread_fence(std::memory_order_acq_rel);
        const std::uint64_t end =
            Atomic(header.status_publish_tag)
                .load(std::memory_order_acquire);
        if (begin != end ||
            !l2flow::ipc::RealtimeStatusPublishTagStableV2(end)) {
            continue;
        }
        if (snapshot.reserved_count != 0U ||
            !l2flow::ipc::RealtimeWireCountsValidV2(
                header.capacity,
                snapshot.bound_count,
                snapshot.available_count,
                snapshot.snapshot_available_count,
                snapshot.tick_available_count,
                snapshot.factor_eligible_count) ||
            !l2flow::ipc::RealtimeWireProcessingSequencesValidV2(
                snapshot.accepted_sequence,
                snapshot.applied_sequence) ||
            (snapshot.bound_count != 0U &&
             !AnyNonzero(snapshot.catalog_digest))) {
            return StableCopyResult::kInvalid;
        }
        *output = snapshot;
        return StableCopyResult::kCopied;
    }
    return StableCopyResult::kInconsistent;
}

StableCopyResult CopyCatalogCut(
    const RealtimeWireHeaderV2& header,
    StatusSnapshot* output) noexcept {
    if (output == nullptr) {
        return StableCopyResult::kInvalid;
    }
    for (std::size_t attempt = 0U; attempt < kReadAttempts;
         ++attempt) {
        const std::uint32_t begin =
            Atomic(header.bound_count).load(std::memory_order_acquire);
        if (begin > header.capacity) {
            return StableCopyResult::kInvalid;
        }
        StatusSnapshot snapshot{};
        snapshot.bound_count = begin;
        snapshot.catalog_generation =
            Atomic(header.catalog_generation)
                .load(std::memory_order_relaxed);
        for (std::size_t index = 0U;
             index < snapshot.catalog_digest.words.size();
             ++index) {
            snapshot.catalog_digest.words[index] =
                Atomic(header.catalog_digest.words[index])
                    .load(std::memory_order_relaxed);
        }
        std::atomic_thread_fence(std::memory_order_acq_rel);
        const std::uint32_t end =
            Atomic(header.bound_count).load(std::memory_order_acquire);
        if (begin != end) {
            continue;
        }
        // Wire V2.2 publishes the complete dense daily catalog before
        // ACTIVE. Identity never grows or renumbers during the session.
        if (begin != header.capacity ||
            snapshot.catalog_generation != 1U ||
            !AnyNonzero(snapshot.catalog_digest)) {
            return StableCopyResult::kInvalid;
        }
        *output = snapshot;
        return StableCopyResult::kCopied;
    }
    return StableCopyResult::kInconsistent;
}

bool CatalogCutEqual(
    const StatusSnapshot& left,
    const StatusSnapshot& right) noexcept {
    return left.catalog_generation == right.catalog_generation &&
           left.bound_count == right.bound_count &&
           left.catalog_digest.words ==
               right.catalog_digest.words;
}

bool SelectionStateCutEqual(
    const StatusSnapshot& left,
    const StatusSnapshot& right) noexcept {
    return CatalogCutEqual(left, right) &&
           left.data_state_generation ==
               right.data_state_generation &&
           left.available_count == right.available_count &&
           left.snapshot_available_count ==
               right.snapshot_available_count &&
           left.tick_available_count ==
               right.tick_available_count &&
           left.factor_eligible_count ==
               right.factor_eligible_count;
}

StableCopyResult CopyInstrumentRow(
    const RealtimeWireInstrumentV2& source,
    RealtimeWireInstrumentV2* output) noexcept {
    if (output == nullptr) {
        return StableCopyResult::kInvalid;
    }
    for (std::size_t attempt = 0U; attempt < kReadAttempts;
         ++attempt) {
        const std::uint64_t begin =
            Atomic(source.publish_tag)
                .load(std::memory_order_acquire);
        if ((begin & 1U) != 0U) {
            continue;
        }
        RealtimeWireInstrumentV2 row{};
        row.publish_tag = begin;
        row.instrument_id =
            Atomic(source.instrument_id)
                .load(std::memory_order_relaxed);
        row.ordinal =
            Atomic(source.ordinal).load(std::memory_order_relaxed);
        row.binding_state =
            Atomic(source.binding_state)
                .load(std::memory_order_relaxed);
        row.availability_flags =
            Atomic(source.availability_flags)
                .load(std::memory_order_relaxed);
        row.market =
            Atomic(source.market).load(std::memory_order_relaxed);
        row.quantity_unit =
            Atomic(source.quantity_unit)
                .load(std::memory_order_relaxed);
        row.security_type =
            Atomic(source.security_type)
                .load(std::memory_order_relaxed);
        row.asset_scope =
            Atomic(source.asset_scope)
                .load(std::memory_order_relaxed);
        row.reserved0 =
            Atomic(source.reserved0).load(std::memory_order_relaxed);
        row.security_id_source_offset =
            Atomic(source.security_id_source_offset)
                .load(std::memory_order_relaxed);
        row.security_id_offset =
            Atomic(source.security_id_offset)
                .load(std::memory_order_relaxed);
        row.security_id_source_length =
            Atomic(source.security_id_source_length)
                .load(std::memory_order_relaxed);
        row.security_id_length =
            Atomic(source.security_id_length)
                .load(std::memory_order_relaxed);
        row.first_ingress_sequence =
            Atomic(source.first_ingress_sequence)
                .load(std::memory_order_relaxed);
        row.last_ingress_sequence =
            Atomic(source.last_ingress_sequence)
                .load(std::memory_order_relaxed);
        for (std::size_t index = 0U; index < row.reserved.size();
             ++index) {
            row.reserved[index] =
                Atomic(source.reserved[index])
                    .load(std::memory_order_relaxed);
        }
        std::atomic_thread_fence(std::memory_order_acq_rel);
        const std::uint64_t end =
            Atomic(source.publish_tag)
                .load(std::memory_order_acquire);
        if (begin == end && (end & 1U) == 0U) {
            *output = row;
            return StableCopyResult::kCopied;
        }
    }
    return StableCopyResult::kInconsistent;
}

template <typename Slot, typename Payload>
StableCopyResult CopySlot(
    const Slot& slot,
    Payload* output) noexcept {
    if (output == nullptr ||
        sizeof(Payload) > sizeof(slot.payload_words)) {
        return StableCopyResult::kInvalid;
    }
    constexpr std::size_t word_count =
        (sizeof(Payload) + sizeof(std::uint64_t) - 1U) /
        sizeof(std::uint64_t);
    for (std::size_t attempt = 0U; attempt < kReadAttempts;
         ++attempt) {
        const std::uint64_t begin =
            Atomic(slot.publish_tag)
                .load(std::memory_order_acquire);
        if (begin == 0U) {
            return StableCopyResult::kNeverPublished;
        }
        if ((begin & 1U) != 0U) {
            continue;
        }
        std::array<std::uint64_t, word_count> words{};
        for (std::size_t index = 0U; index < word_count; ++index) {
            words[index] =
                Atomic(slot.payload_words[index])
                    .load(std::memory_order_relaxed);
        }
        bool trailing_nonzero = false;
        for (std::size_t index = word_count;
             index < slot.payload_words.size();
             ++index) {
            trailing_nonzero =
                trailing_nonzero ||
                Atomic(slot.payload_words[index])
                        .load(std::memory_order_relaxed) != 0U;
        }
        std::atomic_thread_fence(std::memory_order_acq_rel);
        const std::uint64_t end =
            Atomic(slot.publish_tag)
                .load(std::memory_order_acquire);
        if (begin == end && (end & 1U) == 0U) {
            if (trailing_nonzero) {
                return StableCopyResult::kInvalid;
            }
            *output = std::bit_cast<Payload>(words);
            return StableCopyResult::kCopied;
        }
    }
    return StableCopyResult::kInconsistent;
}

bool CommonRecordIdentityValid(
    const RealtimeWireCommonRecordV2& common) noexcept {
    if (common.record_schema_version != 2U ||
        common.instrument_id == 0U ||
        common.ordinal ==
            std::numeric_limits<std::uint32_t>::max() ||
        common.instrument_id != common.ordinal + 1U ||
        common.source_sequence == 0U ||
        common.ingress_sequence == 0U ||
        common.source_stream_id == 0U ||
        common.trade_date == 0U || common.reserved0 != 0U ||
        common.quantity_unit > 5U ||
        common.security_type > 7U || common.asset_scope > 2U ||
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

bool TickPayloadProjectionValid(
    const RealtimeWireTickPayloadV2& payload) noexcept {
    constexpr std::uint32_t known_flags =
        l2flow::ipc::kRealtimeWireTickRawTypeOmittedV2 |
        l2flow::ipc::kRealtimeWireTickRawTickFlagOmittedV2;
    if ((payload.projection_flags & ~known_flags) != 0U ||
        payload.raw_type_length > payload.raw_type.size() ||
        payload.raw_tick_flag_length >
            payload.raw_tick_flag.size() ||
        payload.reserved0 != 0U) {
        return false;
    }
    const bool raw_type_zero = !AnyNonzero(payload.raw_type);
    const bool raw_tick_flag_zero =
        !AnyNonzero(payload.raw_tick_flag);
    if (payload.common.event_kind != 2U) {
        return payload.projection_flags == 0U &&
               payload.raw_type_length == 0U &&
               payload.raw_tick_flag_length == 0U &&
               raw_type_zero && raw_tick_flag_zero;
    }
    const bool raw_type_omitted =
        (payload.projection_flags &
         l2flow::ipc::kRealtimeWireTickRawTypeOmittedV2) != 0U;
    const bool raw_tick_flag_omitted =
        (payload.projection_flags &
         l2flow::ipc::kRealtimeWireTickRawTickFlagOmittedV2) != 0U;
    if ((raw_type_omitted &&
         (payload.raw_type_length != 0U || !raw_type_zero)) ||
        (raw_tick_flag_omitted &&
         (payload.raw_tick_flag_length != 0U ||
          !raw_tick_flag_zero))) {
        return false;
    }
    return std::all_of(
               payload.raw_type.begin() +
                   payload.raw_type_length,
               payload.raw_type.end(),
               [](std::uint8_t byte) noexcept {
                   return byte == 0U;
               }) &&
           std::all_of(
               payload.raw_tick_flag.begin() +
                   payload.raw_tick_flag_length,
               payload.raw_tick_flag.end(),
               [](std::uint8_t byte) noexcept {
                   return byte == 0U;
               });
}

bool WireDecimalValid(const RealtimeWireDecimalV2& value) noexcept {
    return value.valid <= 1U && value.is_null <= 1U &&
           !AnyNonzero(value.reserved);
}

bool WireQuantityValid(
    const RealtimeWireQuantityV2& value) noexcept {
    return value.valid <= 1U && value.is_null <= 1U &&
           !AnyNonzero(value.reserved);
}

bool BookLevelValid(
    const RealtimeWireBookLevelV2& level) noexcept {
    return WireDecimalValid(level.price) &&
           WireQuantityValid(level.quantity) &&
           level.order_count_valid <= 1U &&
           !AnyNonzero(level.reserved);
}

bool QueueHeaderValid(
    const RealtimeWireQueueHeaderV2& queue) noexcept {
    return queue.reserved == 0U &&
           queue.actual_revealed_count <=
               queue.total_order_count &&
           queue.retained_count <=
               queue.actual_revealed_count &&
           queue.retained_count <= 50U;
}

bool SnapshotPayloadCanonical(
    const RealtimeWireSnapshotPayloadV2& payload) noexcept {
    if (payload.common.record_bytes != sizeof(payload) ||
        !CommonRecordIdentityValid(payload.common) ||
        (payload.common.event_kind != 1U &&
         payload.common.event_kind != 3U) ||
        payload.common.tick_stream_sequence != 0U ||
        payload.retained_bid_depth > payload.bids.size() ||
        payload.retained_ask_depth > payload.asks.size() ||
        payload.retained_bid_depth > payload.actual_bid_depth ||
        payload.retained_ask_depth > payload.actual_ask_depth ||
        !WireDecimalValid(payload.pre_close_price) ||
        !WireDecimalValid(payload.open_price) ||
        !WireDecimalValid(payload.high_price) ||
        !WireDecimalValid(payload.low_price) ||
        !WireDecimalValid(payload.last_price) ||
        !WireDecimalValid(payload.close_price) ||
        !WireQuantityValid(payload.trade_volume) ||
        !WireDecimalValid(payload.turnover) ||
        !WireQuantityValid(payload.total_bid_quantity) ||
        !WireDecimalValid(
            payload.weighted_average_bid_price) ||
        !WireQuantityValid(payload.total_ask_quantity) ||
        !WireDecimalValid(
            payload.weighted_average_ask_price) ||
        !WireDecimalValid(payload.high_limit_price) ||
        !WireDecimalValid(payload.low_limit_price) ||
        !WireDecimalValid(payload.iopv) ||
        !WireQuantityValid(payload.open_interest) ||
        !QueueHeaderValid(payload.bid1_queue) ||
        !QueueHeaderValid(payload.ask1_queue)) {
        return false;
    }
    return std::all_of(
               payload.bids.begin(),
               payload.bids.end(),
               BookLevelValid) &&
           std::all_of(
               payload.asks.begin(),
               payload.asks.end(),
               BookLevelValid) &&
           std::all_of(
               payload.bid1_queue_quantities.begin(),
               payload.bid1_queue_quantities.end(),
               WireQuantityValid) &&
           std::all_of(
               payload.ask1_queue_quantities.begin(),
               payload.ask1_queue_quantities.end(),
               WireQuantityValid);
}

bool TickPayloadCanonical(
    const RealtimeWireTickPayloadV2& payload) noexcept {
    return payload.common.record_bytes ==
               sizeof(RealtimeWireTickPayloadV2) &&
           CommonRecordIdentityValid(payload.common) &&
           (payload.common.event_kind == 2U ||
            payload.common.event_kind == 4U ||
            payload.common.event_kind == 5U) &&
           TickPayloadProjectionValid(payload) &&
           payload.action <= 4U && payload.side <= 4U &&
           payload.order_type <= 3U && payload.aggressor <= 3U &&
           payload.phase <= 7U &&
           WireDecimalValid(payload.price) &&
           WireQuantityValid(payload.quantity) &&
           WireDecimalValid(payload.trade_amount) &&
           WireQuantityValid(payload.matched_quantity);
}

}  // namespace

struct l2flow_shm_reader_v2 final {
    const void* mapping = MAP_FAILED;
    std::size_t mapping_bytes = 0U;
    const RealtimeWireHeaderV2* header = nullptr;
    const RealtimeWireInstrumentV2* instruments = nullptr;
    const std::byte* key_arena = nullptr;
    std::size_t key_arena_bytes = 0U;
    const RealtimeWireKLineWindowV2* windows = nullptr;
    const RealtimeWireSnapshotSlotV2* snapshots = nullptr;
    const RealtimeWireTickSlotV2* latest_ticks = nullptr;
    const RealtimeWireKLineSlotV2* klines = nullptr;
    const RealtimeWireTickSlotV2* tick_ring = nullptr;
    std::uint64_t kline_slots_per_table = 0U;
    std::uint64_t tick_ring_capacity = 0U;

    mutable std::mutex key_index_mutex;
    mutable std::vector<std::uint32_t> instrument_key_ordinals;
    mutable std::uint64_t key_index_catalog_generation =
        std::numeric_limits<std::uint64_t>::max();
    mutable std::uint32_t key_index_bound_count = 0U;
    mutable RealtimeWireDigest256V2 key_index_catalog_digest{};
};

namespace {

const RealtimeWireRegionDescriptorV2* FindRegion(
    const RealtimeWireHeaderV2& header,
    RealtimeRegionKindV2 kind) noexcept {
    for (const RealtimeWireRegionDescriptorV2& region :
         header.regions) {
        if (region.kind == static_cast<std::uint32_t>(kind)) {
            return &region;
        }
    }
    return nullptr;
}

std::size_t CountRegion(
    const RealtimeWireHeaderV2& header,
    RealtimeRegionKindV2 kind) noexcept {
    return static_cast<std::size_t>(std::count_if(
        header.regions.begin(),
        header.regions.end(),
        [kind](const RealtimeWireRegionDescriptorV2& region) noexcept {
            return region.kind == static_cast<std::uint32_t>(kind);
        }));
}

bool RegionBefore(
    const RealtimeWireRegionDescriptorV2* left,
    const RealtimeWireRegionDescriptorV2* right) noexcept {
    return left != nullptr && right != nullptr &&
           left->offset <= right->offset &&
           left->length <= right->offset - left->offset;
}

bool RegionValid(
    const RealtimeWireRegionDescriptorV2* region,
    std::uint64_t mapping_bytes,
    std::uint64_t expected_stride,
    std::uint64_t expected_count,
    std::uint64_t expected_alignment) noexcept {
    if (region == nullptr ||
        region->schema_major !=
            l2flow::ipc::kRealtimeWireMajorV2 ||
        region->schema_minor !=
            l2flow::ipc::kRealtimeWireMinorV2 ||
        region->element_stride != expected_stride ||
        region->element_count != expected_count ||
        region->capacity != expected_count ||
        region->alignment != expected_alignment ||
        expected_alignment == 0U ||
        region->offset % expected_alignment != 0U ||
        region->flags != 0U || region->reserved != 0U ||
        !CheckedEnd(region->offset, region->length, mapping_bytes)) {
        return false;
    }
    if (expected_count != 0U &&
        expected_stride >
            std::numeric_limits<std::uint64_t>::max() /
                expected_count) {
        return false;
    }
    return region->length == expected_stride * expected_count;
}

bool ReservedRegionValid(
    const RealtimeWireRegionDescriptorV2* region,
    std::uint64_t mapping_bytes) noexcept {
    return region != nullptr &&
           region->schema_major ==
               l2flow::ipc::kRealtimeWireMajorV2 &&
           region->schema_minor ==
               l2flow::ipc::kRealtimeWireMinorV2 &&
           region->offset == mapping_bytes && region->length == 0U &&
           region->element_stride == 0U &&
           region->element_count == 0U && region->capacity == 0U &&
           region->alignment == 1U && region->flags == 0U &&
           region->reserved == 0U;
}

bool KeyRangeValid(
    std::uint64_t offset,
    std::uint32_t length,
    std::size_t arena_bytes) noexcept {
    return offset <= arena_bytes &&
           static_cast<std::uint64_t>(length) <=
               static_cast<std::uint64_t>(arena_bytes) - offset;
}

bool BoundRowValid(
    const l2flow_shm_reader_v2& reader,
    const RealtimeWireInstrumentV2& row,
    std::size_t physical_ordinal) noexcept {
    if (row.publish_tag == 0U || (row.publish_tag & 1U) != 0U ||
        physical_ordinal >= reader.header->capacity ||
        row.ordinal != physical_ordinal ||
        !l2flow::ipc::RealtimeWireInstrumentStateValidV2(
            row, reader.header->capacity) ||
        (row.binding_state !=
             static_cast<std::uint32_t>(
                 RealtimeInstrumentBindingStateV2::kBoundNoData) &&
         row.binding_state !=
             static_cast<std::uint32_t>(
                 RealtimeInstrumentBindingStateV2::kAvailable)) ||
        (row.market != 1U && row.market != 2U) ||
        row.quantity_unit > 5U || row.security_type > 7U ||
        row.asset_scope > 2U ||
        !KeyRangeValid(
            row.security_id_source_offset,
            row.security_id_source_length,
            reader.key_arena_bytes) ||
        !KeyRangeValid(
            row.security_id_offset,
            row.security_id_length,
            reader.key_arena_bytes)) {
        return false;
    }
    return true;
}

struct ScanCounts final {
    std::uint32_t bound = 0U;
    std::uint32_t available = 0U;
    std::uint32_t snapshot = 0U;
    std::uint32_t tick = 0U;
    std::uint32_t factor = 0U;
};

void AddBoundRow(
    const RealtimeWireInstrumentV2& row,
    ScanCounts* counts) noexcept {
    ++counts->bound;
    if (row.binding_state ==
        static_cast<std::uint32_t>(
            RealtimeInstrumentBindingStateV2::kAvailable)) {
        ++counts->available;
    }
    if ((row.availability_flags &
         l2flow::ipc::kRealtimeInstrumentHasSnapshotV2) != 0U) {
        ++counts->snapshot;
    }
    if ((row.availability_flags &
         l2flow::ipc::kRealtimeInstrumentHasTickV2) != 0U) {
        ++counts->tick;
    }
    if ((row.availability_flags &
         l2flow::ipc::kRealtimeInstrumentFactorEligibleV2) != 0U) {
        ++counts->factor;
    }
}

bool CountsMatch(
    const ScanCounts& counts,
    const StatusSnapshot& status) noexcept {
    return counts.bound == status.bound_count &&
           counts.available == status.available_count &&
           counts.snapshot == status.snapshot_available_count &&
           counts.tick == status.tick_available_count &&
           counts.factor == status.factor_eligible_count;
}

enum class CatalogValidationResult : std::uint8_t {
    kValid = 0U,
    kInvalid,
};

bool PublishedBoundIdentityValid(
    const l2flow_shm_reader_v2& reader,
    std::size_t physical_ordinal) noexcept {
    if (physical_ordinal >= reader.header->capacity) {
        return false;
    }
    const RealtimeWireInstrumentV2& source =
        reader.instruments[physical_ordinal];
    RealtimeWireInstrumentV2 identity{};
    // bound_count is release-published only after this immutable identity
    // and its key bytes. Once an ordinal is in that prefix these fields are
    // never changed, so open need not contend with the mutable row seqcount.
    identity.instrument_id =
        Atomic(source.instrument_id).load(std::memory_order_relaxed);
    identity.ordinal =
        Atomic(source.ordinal).load(std::memory_order_relaxed);
    identity.market =
        Atomic(source.market).load(std::memory_order_relaxed);
    identity.quantity_unit =
        Atomic(source.quantity_unit).load(std::memory_order_relaxed);
    identity.security_type =
        Atomic(source.security_type).load(std::memory_order_relaxed);
    identity.asset_scope =
        Atomic(source.asset_scope).load(std::memory_order_relaxed);
    identity.reserved0 =
        Atomic(source.reserved0).load(std::memory_order_relaxed);
    identity.security_id_source_offset =
        Atomic(source.security_id_source_offset)
            .load(std::memory_order_relaxed);
    identity.security_id_offset =
        Atomic(source.security_id_offset)
            .load(std::memory_order_relaxed);
    identity.security_id_source_length =
        Atomic(source.security_id_source_length)
            .load(std::memory_order_relaxed);
    identity.security_id_length =
        Atomic(source.security_id_length)
            .load(std::memory_order_relaxed);
    for (std::size_t index = 0U; index < identity.reserved.size();
         ++index) {
        identity.reserved[index] =
            Atomic(source.reserved[index])
                .load(std::memory_order_relaxed);
    }
    return identity.ordinal == physical_ordinal &&
           l2flow::ipc::RealtimeWireInstrumentIdentityValidV2(
               identity, reader.header->capacity) &&
           (identity.market == 1U || identity.market == 2U) &&
           identity.quantity_unit <= 5U &&
           identity.security_type <= 7U &&
           identity.asset_scope <= 2U &&
           identity.reserved0 == 0U &&
           AllZero(identity.reserved) &&
           KeyRangeValid(
               identity.security_id_source_offset,
               identity.security_id_source_length,
               reader.key_arena_bytes) &&
           KeyRangeValid(
               identity.security_id_offset,
               identity.security_id_length,
               reader.key_arena_bytes);
}

CatalogValidationResult ValidatePublishedIdentityPrefix(
    const l2flow_shm_reader_v2& reader) noexcept {
    // Connecting validates only the immutable layout and the identity prefix
    // visible at this acquire. Data availability, counts, progress, and the
    // global status seqcount may be changing continuously in a live market;
    // the APIs that consume those values validate their own exact cuts.
    const std::uint32_t bound_count =
        Atomic(reader.header->bound_count)
            .load(std::memory_order_acquire);
    if (bound_count > reader.header->capacity) {
        return CatalogValidationResult::kInvalid;
    }
    for (std::size_t ordinal = 0U; ordinal < bound_count; ++ordinal) {
        if (!PublishedBoundIdentityValid(reader, ordinal)) {
            return CatalogValidationResult::kInvalid;
        }
    }
    return CatalogValidationResult::kValid;
}

enum class PointRowResult : std::uint8_t {
    kBound = 0U,
    kUnbound,
    kInvalidId,
    kInconsistent,
    kInvalidLayout,
};

PointRowResult ResolvePointOrdinal(
    const l2flow_shm_reader_v2& reader,
    std::uint32_t instrument_id,
    std::size_t* output) noexcept {
    if (output == nullptr) {
        return PointRowResult::kInvalidLayout;
    }
    if (instrument_id == 0U ||
        instrument_id > reader.header->capacity) {
        return PointRowResult::kInvalidId;
    }
    const std::size_t ordinal =
        static_cast<std::size_t>(instrument_id - 1U);
    // Wire V2.2 prepublishes every dense daily identity before ACTIVE.
    // A partial bound prefix is therefore corruption, not an UNBOUND
    // instrument state.
    const std::uint32_t bound_count =
        Atomic(reader.header->bound_count)
            .load(std::memory_order_acquire);
    if (bound_count != reader.header->capacity) {
        return PointRowResult::kInvalidLayout;
    }
    *output = ordinal;
    return PointRowResult::kBound;
}

PointRowResult ReadPointRow(
    const l2flow_shm_reader_v2& reader,
    std::uint32_t instrument_id,
    RealtimeWireInstrumentV2* output) noexcept {
    if (output == nullptr) {
        return PointRowResult::kInvalidLayout;
    }
    std::size_t ordinal = 0U;
    const PointRowResult ordinal_result =
        ResolvePointOrdinal(reader, instrument_id, &ordinal);
    if (ordinal_result != PointRowResult::kBound) {
        return ordinal_result;
    }
    for (std::size_t attempt = 0U; attempt < kReadAttempts;
         ++attempt) {
        RealtimeWireInstrumentV2 row{};
        if (CopyInstrumentRow(reader.instruments[ordinal], &row) !=
            StableCopyResult::kCopied) {
            continue;
        }
        if (!BoundRowValid(reader, row, ordinal)) {
            return PointRowResult::kInvalidLayout;
        }
        *output = row;
        return PointRowResult::kBound;
    }
    return PointRowResult::kInconsistent;
}

StableCopyResult CopyBoundRowByOrdinal(
    const l2flow_shm_reader_v2& reader,
    std::size_t ordinal,
    RealtimeWireInstrumentV2* output) noexcept {
    if (ordinal >= reader.header->capacity || output == nullptr) {
        return StableCopyResult::kInvalid;
    }
    RealtimeWireInstrumentV2 row{};
    const StableCopyResult copy_result =
        CopyInstrumentRow(reader.instruments[ordinal], &row);
    if (copy_result != StableCopyResult::kCopied) {
        return copy_result;
    }
    if (!BoundRowValid(reader, row, ordinal)) {
        return StableCopyResult::kInvalid;
    }
    *output = row;
    return StableCopyResult::kCopied;
}

int CompareOpaqueBytes(
    const std::byte* left,
    std::size_t left_size,
    const std::byte* right,
    std::size_t right_size) noexcept {
    const std::size_t common = std::min(left_size, right_size);
    for (std::size_t index = 0U; index < common; ++index) {
        const std::uint8_t left_byte =
            std::to_integer<std::uint8_t>(left[index]);
        const std::uint8_t right_byte =
            std::to_integer<std::uint8_t>(right[index]);
        if (left_byte < right_byte) {
            return -1;
        }
        if (left_byte > right_byte) {
            return 1;
        }
    }
    if (left_size < right_size) {
        return -1;
    }
    if (left_size > right_size) {
        return 1;
    }
    return 0;
}

int CompareInstrumentRowToKey(
    const l2flow_shm_reader_v2& reader,
    std::size_t ordinal,
    std::uint8_t market,
    const std::uint8_t* security_id_source,
    std::size_t security_id_source_length,
    const std::uint8_t* security_id,
    std::size_t security_id_length) noexcept {
    const RealtimeWireInstrumentV2& row =
        reader.instruments[ordinal];
    if (row.market < market) {
        return -1;
    }
    if (row.market > market) {
        return 1;
    }
    const int source_order = CompareOpaqueBytes(
        reader.key_arena + row.security_id_source_offset,
        row.security_id_source_length,
        reinterpret_cast<const std::byte*>(security_id_source),
        security_id_source_length);
    if (source_order != 0) {
        return source_order;
    }
    return CompareOpaqueBytes(
        reader.key_arena + row.security_id_offset,
        row.security_id_length,
        reinterpret_cast<const std::byte*>(security_id),
        security_id_length);
}

int CompareInstrumentRows(
    const l2flow_shm_reader_v2& reader,
    std::size_t left,
    std::size_t right) noexcept {
    const RealtimeWireInstrumentV2& right_row =
        reader.instruments[right];
    return CompareInstrumentRowToKey(
        reader,
        left,
        right_row.market,
        reinterpret_cast<const std::uint8_t*>(
            reader.key_arena +
            right_row.security_id_source_offset),
        right_row.security_id_source_length,
        reinterpret_cast<const std::uint8_t*>(
            reader.key_arena + right_row.security_id_offset),
        right_row.security_id_length);
}

enum class KeyIndexResult : std::uint8_t {
    kOk = 0U,
    kChanged,
    kResourceExhausted,
    kInvalid,
};

KeyIndexResult RefreshKeyIndexLocked(
    const l2flow_shm_reader_v2& reader,
    const StatusSnapshot& status) noexcept {
    const bool has_cached_generation =
        reader.key_index_catalog_generation !=
        std::numeric_limits<std::uint64_t>::max();
    if (has_cached_generation &&
        reader.key_index_catalog_generation ==
            status.catalog_generation) {
        return reader.key_index_bound_count == status.bound_count &&
                       reader.key_index_catalog_digest.words ==
                           status.catalog_digest.words
                   ? KeyIndexResult::kOk
                   : KeyIndexResult::kInvalid;
    }
    if (has_cached_generation &&
        (status.catalog_generation <
             reader.key_index_catalog_generation ||
         status.bound_count <= reader.key_index_bound_count)) {
        return KeyIndexResult::kInvalid;
    }
    try {
        std::vector<std::uint32_t> ordinals;
        ordinals.reserve(status.bound_count);
        for (std::size_t ordinal = 0U;
             ordinal < status.bound_count;
             ++ordinal) {
            if (!PublishedBoundIdentityValid(reader, ordinal)) {
                return KeyIndexResult::kInvalid;
            }
            ordinals.push_back(
                static_cast<std::uint32_t>(ordinal));
        }
        std::sort(
            ordinals.begin(),
            ordinals.end(),
            [&reader](
                std::uint32_t left,
                std::uint32_t right) noexcept {
                return CompareInstrumentRows(
                           reader, left, right) < 0;
            });
        for (std::size_t index = 1U; index < ordinals.size();
             ++index) {
            if (CompareInstrumentRows(
                    reader,
                    ordinals[index - 1U],
                    ordinals[index]) >= 0) {
                return KeyIndexResult::kInvalid;
            }
        }
        reader.instrument_key_ordinals.swap(ordinals);
        reader.key_index_catalog_generation =
            status.catalog_generation;
        reader.key_index_bound_count = status.bound_count;
        reader.key_index_catalog_digest = status.catalog_digest;
        return KeyIndexResult::kOk;
    } catch (...) {
        return KeyIndexResult::kResourceExhausted;
    }
}

std::size_t FindInstrumentOrdinalByKey(
    const l2flow_shm_reader_v2& reader,
    std::uint8_t market,
    const std::uint8_t* security_id_source,
    std::size_t security_id_source_length,
    const std::uint8_t* security_id,
    std::size_t security_id_length) noexcept {
    std::size_t begin = 0U;
    std::size_t end = reader.instrument_key_ordinals.size();
    while (begin < end) {
        const std::size_t middle = begin + (end - begin) / 2U;
        const std::size_t ordinal =
            reader.instrument_key_ordinals[middle];
        const int order = CompareInstrumentRowToKey(
            reader,
            ordinal,
            market,
            security_id_source,
            security_id_source_length,
            security_id,
            security_id_length);
        if (order < 0) {
            begin = middle + 1U;
        } else {
            end = middle;
        }
    }
    if (begin >= reader.instrument_key_ordinals.size()) {
        return std::numeric_limits<std::size_t>::max();
    }
    const std::size_t ordinal =
        reader.instrument_key_ordinals[begin];
    return CompareInstrumentRowToKey(
               reader,
               ordinal,
               market,
               security_id_source,
               security_id_source_length,
               security_id,
               security_id_length) == 0
               ? ordinal
               : std::numeric_limits<std::size_t>::max();
}

std::size_t FindWindowIndex(
    const l2flow_shm_reader_v2& reader,
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
    const l2flow_shm_reader_v2* reader,
    const std::uint32_t* instrument_ids,
    std::size_t count,
    void* outputs,
    std::size_t output_stride,
    std::uint8_t* statuses,
    const Slot* slots,
    std::uint32_t required_flag,
    Validator validator) noexcept {
    if (reader == nullptr ||
        (count != 0U &&
         (instrument_ids == nullptr || outputs == nullptr ||
          statuses == nullptr || slots == nullptr)) ||
        output_stride < sizeof(Payload) ||
        (count != 0U &&
         output_stride >
             std::numeric_limits<std::size_t>::max() / count)) {
        return L2FLOW_SHM_READER_INVALID_ARGUMENT_V2;
    }
    if (!HealthyForRead(*reader->header)) {
        return L2FLOW_SHM_READER_UNAVAILABLE_V2;
    }
    auto* const bytes = static_cast<std::byte*>(outputs);
    for (std::size_t index = 0U; index < count; ++index) {
        std::size_t ordinal = 0U;
        const PointRowResult ordinal_result =
            ResolvePointOrdinal(
                *reader, instrument_ids[index], &ordinal);
        if (ordinal_result == PointRowResult::kInvalidId) {
            statuses[index] =
                L2FLOW_LATEST_INVALID_INSTRUMENT_ID_V2;
            continue;
        }
        if (ordinal_result == PointRowResult::kUnbound) {
            statuses[index] = L2FLOW_LATEST_UNBOUND_V2;
            continue;
        }
        if (ordinal_result != PointRowResult::kBound) {
            return L2FLOW_SHM_READER_LAYOUT_INVALID_V2;
        }
        if (!AllZero(slots[ordinal].reserved)) {
            return L2FLOW_SHM_READER_LAYOUT_INVALID_V2;
        }

        // A successfully published, self-identifying fixed-ordinal slot is the
        // complete AVAILABLE fast path. Do not copy the instrument row here:
        // its last_ingress_sequence changes on every event for the instrument,
        // so doing so would make an otherwise stable snapshot read contend
        // with unrelated tick updates to that row.
        Payload payload{};
        StableCopyResult copy_result =
            CopySlot(slots[ordinal], &payload);
        if (copy_result == StableCopyResult::kInconsistent) {
            return L2FLOW_SHM_READER_INCONSISTENT_READ_V2;
        }
        if (copy_result == StableCopyResult::kInvalid) {
            return L2FLOW_SHM_READER_LAYOUT_INVALID_V2;
        }
        if (copy_result == StableCopyResult::kCopied) {
            if (!validator(
                    payload, ordinal, instrument_ids[index])) {
                return L2FLOW_SHM_READER_LAYOUT_INVALID_V2;
            }
            std::memcpy(
                bytes + index * output_stride,
                &payload,
                sizeof(payload));
            statuses[index] = L2FLOW_LATEST_AVAILABLE_V2;
            continue;
        }

        // An unpublished type slot cannot by itself distinguish BOUND_NO_DATA
        // from an instrument that has another data type. Only this cold/status
        // path reads the mutable instrument row.
        RealtimeWireInstrumentV2 row{};
        const PointRowResult row_result =
            ReadPointRow(*reader, instrument_ids[index], &row);
        if (row_result == PointRowResult::kInvalidId) {
            statuses[index] =
                L2FLOW_LATEST_INVALID_INSTRUMENT_ID_V2;
            continue;
        }
        if (row_result == PointRowResult::kUnbound) {
            statuses[index] = L2FLOW_LATEST_UNBOUND_V2;
            continue;
        }
        if (row_result == PointRowResult::kInconsistent) {
            return L2FLOW_SHM_READER_INCONSISTENT_READ_V2;
        }
        if (row_result != PointRowResult::kBound) {
            return L2FLOW_SHM_READER_LAYOUT_INVALID_V2;
        }
        if (row.binding_state ==
            static_cast<std::uint32_t>(
                RealtimeInstrumentBindingStateV2::kBoundNoData)) {
            statuses[index] =
                L2FLOW_LATEST_BOUND_NO_DATA_V2;
            continue;
        }
        if ((row.availability_flags & required_flag) == 0U) {
            statuses[index] =
                L2FLOW_LATEST_TYPE_UNAVAILABLE_V2;
            continue;
        }

        // The first zero tag may have raced the publication that made the row
        // advertise this type. Re-read the slot once through the normal
        // bounded seqcount copy before diagnosing an impossible wire state.
        payload = {};
        copy_result = CopySlot(slots[ordinal], &payload);
        if (copy_result == StableCopyResult::kInconsistent) {
            return L2FLOW_SHM_READER_INCONSISTENT_READ_V2;
        }
        if (copy_result != StableCopyResult::kCopied ||
            !validator(
                payload, ordinal, instrument_ids[index])) {
            return L2FLOW_SHM_READER_LAYOUT_INVALID_V2;
        }
        std::memcpy(
            bytes + index * output_stride,
            &payload,
            sizeof(payload));
        statuses[index] = L2FLOW_LATEST_AVAILABLE_V2;
    }
    std::atomic_thread_fence(std::memory_order_acquire);
    return HealthyForRead(*reader->header)
               ? L2FLOW_SHM_READER_OK_V2
               : L2FLOW_SHM_READER_UNAVAILABLE_V2;
}

void CopyDigestBytes(
    const RealtimeWireDigest256V2& digest,
    std::uint8_t* output) noexcept {
    std::memcpy(output, digest.words.data(), 32U);
}

void FillSelectionEnvelope(
    const l2flow_shm_reader_v2& reader,
    const StatusSnapshot& status,
    std::uint32_t selection_scope,
    std::uint32_t returned_count,
    l2flow_selection_envelope_v2* output) noexcept {
    l2flow_selection_envelope_v2 envelope{};
    std::memcpy(
        envelope.run_id,
        reader.header->run_id.data(),
        sizeof(envelope.run_id));
    CopyDigestBytes(status.catalog_digest, envelope.catalog_digest);
    envelope.session_epoch = reader.header->session_epoch;
    envelope.catalog_generation = status.catalog_generation;
    envelope.data_state_generation =
        status.data_state_generation;
    envelope.accepted_sequence = status.accepted_sequence;
    envelope.applied_sequence = status.applied_sequence;
    envelope.processing_lag_records =
        status.accepted_sequence - status.applied_sequence;
    envelope.capacity = reader.header->capacity;
    envelope.catalog_scope = reader.header->catalog_scope;
    envelope.coverage_complete =
        reader.header->coverage_complete;
    envelope.bound_count = status.bound_count;
    envelope.available_count = status.available_count;
    envelope.snapshot_available_count =
        status.snapshot_available_count;
    envelope.tick_available_count =
        status.tick_available_count;
    envelope.factor_eligible_count =
        status.factor_eligible_count;
    envelope.selection_scope = selection_scope;
    envelope.returned_row_count = returned_count;
    *output = envelope;
}

bool SelectionScopeValid(std::uint32_t scope) noexcept {
    return scope >=
               static_cast<std::uint32_t>(
                   RealtimeSelectionScopeV2::kCatalogAll) &&
           scope <=
               static_cast<std::uint32_t>(
                   RealtimeSelectionScopeV2::kFactorEligible);
}

bool RowMatchesSelection(
    const RealtimeWireInstrumentV2& row,
    std::uint32_t scope) noexcept {
    switch (static_cast<RealtimeSelectionScopeV2>(scope)) {
        case RealtimeSelectionScopeV2::kCatalogAll:
            return true;
        case RealtimeSelectionScopeV2::kAvailableAny:
            return row.binding_state ==
                   static_cast<std::uint32_t>(
                       RealtimeInstrumentBindingStateV2::kAvailable);
        case RealtimeSelectionScopeV2::kSnapshotAvailable:
            return (row.availability_flags &
                    l2flow::ipc::
                        kRealtimeInstrumentHasSnapshotV2) != 0U;
        case RealtimeSelectionScopeV2::kTickAvailable:
            return (row.availability_flags &
                    l2flow::ipc::kRealtimeInstrumentHasTickV2) !=
                   0U;
        case RealtimeSelectionScopeV2::kFactorEligible:
            return (row.availability_flags &
                    l2flow::ipc::
                        kRealtimeInstrumentFactorEligibleV2) != 0U;
    }
    return false;
}

std::uint32_t SelectionCount(
    const StatusSnapshot& status,
    std::uint32_t scope) noexcept {
    switch (static_cast<RealtimeSelectionScopeV2>(scope)) {
        case RealtimeSelectionScopeV2::kCatalogAll:
            return status.bound_count;
        case RealtimeSelectionScopeV2::kAvailableAny:
            return status.available_count;
        case RealtimeSelectionScopeV2::kSnapshotAvailable:
            return status.snapshot_available_count;
        case RealtimeSelectionScopeV2::kTickAvailable:
            return status.tick_available_count;
        case RealtimeSelectionScopeV2::kFactorEligible:
            return status.factor_eligible_count;
    }
    return 0U;
}

}  // namespace

extern "C" int l2flow_shm_reader_open_fd_v2(
    int fd,
    l2flow_shm_reader_v2** output) {
    if (output == nullptr || fd < 0) {
        return L2FLOW_SHM_READER_INVALID_ARGUMENT_V2;
    }
    *output = nullptr;
    if constexpr (std::endian::native != std::endian::little) {
        return L2FLOW_SHM_READER_ABI_MISMATCH_V2;
    }
    const int descriptor_flags = ::fcntl(fd, F_GETFL);
    const int seals = ::fcntl(fd, F_GET_SEALS);
    constexpr int required_seals =
        F_SEAL_GROW | F_SEAL_SHRINK | F_SEAL_FUTURE_WRITE |
        F_SEAL_SEAL;
    if (descriptor_flags < 0 || seals < 0 ||
        (descriptor_flags & O_ACCMODE) != O_RDONLY ||
        (seals & required_seals) != required_seals) {
        return L2FLOW_SHM_READER_SYSTEM_ERROR_V2;
    }
    struct stat descriptor_stat {};
    if (::fstat(fd, &descriptor_stat) != 0 ||
        descriptor_stat.st_size <
            static_cast<off_t>(sizeof(RealtimeWireHeaderV2))) {
        return L2FLOW_SHM_READER_SYSTEM_ERROR_V2;
    }
    const std::uint64_t mapped_bytes =
        static_cast<std::uint64_t>(descriptor_stat.st_size);
    if (mapped_bytes >
        static_cast<std::uint64_t>(
            std::numeric_limits<std::size_t>::max())) {
        return L2FLOW_SHM_READER_LAYOUT_INVALID_V2;
    }
    void* const mapping = ::mmap(
        nullptr,
        static_cast<std::size_t>(mapped_bytes),
        PROT_READ,
        MAP_SHARED,
        fd,
        0);
    if (mapping == MAP_FAILED) {
        return L2FLOW_SHM_READER_SYSTEM_ERROR_V2;
    }
    const auto* const header =
        static_cast<const RealtimeWireHeaderV2*>(mapping);
    const std::uint32_t initial_state =
        Atomic(header->server_state).load(std::memory_order_acquire);
    const std::uint32_t initial_flags =
        Atomic(header->flags).load(std::memory_order_acquire);
    const std::uint64_t capacity = header->capacity;
    const std::uint64_t window_count = header->window_count;
    const std::uint64_t maximum =
        std::numeric_limits<std::uint64_t>::max();
    const std::uint64_t logical_kline_count =
        capacity != 0U && window_count <= maximum / capacity
            ? capacity * window_count
            : maximum;
    const std::uint64_t physical_kline_count =
        logical_kline_count != maximum &&
                logical_kline_count <=
                    maximum /
                        l2flow::ipc::kRealtimeKLineTableCountV2
            ? logical_kline_count *
                  l2flow::ipc::kRealtimeKLineTableCountV2
            : maximum;

    const auto* const instruments = FindRegion(
        *header, RealtimeRegionKindV2::kInstrumentRows);
    const auto* const arena = FindRegion(
        *header, RealtimeRegionKindV2::kInstrumentKeyArena);
    const auto* const windows = FindRegion(
        *header, RealtimeRegionKindV2::kKLineWindows);
    const auto* const snapshots = FindRegion(
        *header, RealtimeRegionKindV2::kLatestSnapshots);
    const auto* const latest_ticks = FindRegion(
        *header, RealtimeRegionKindV2::kLatestTicks);
    const auto* const klines = FindRegion(
        *header, RealtimeRegionKindV2::kLatestKLines);
    const auto* const ring = FindRegion(
        *header, RealtimeRegionKindV2::kTickRingSlots);
    const auto* const reserved8 = FindRegion(
        *header, RealtimeRegionKindV2::kReserved8);
    const auto* const reserved9 = FindRegion(
        *header, RealtimeRegionKindV2::kReserved9);

    const bool valid =
        header->magic == l2flow::ipc::kRealtimeShmMagicV2 &&
        header->abi_major ==
            l2flow::ipc::kRealtimeWireMajorV2 &&
        header->abi_minor ==
            l2flow::ipc::kRealtimeWireMinorV2 &&
        header->header_bytes == sizeof(RealtimeWireHeaderV2) &&
        header->endian_marker ==
            l2flow::ipc::kRealtimeLittleEndianMarkerV2 &&
        header->total_mapping_bytes == mapped_bytes &&
        AnyNonzero(header->run_id) &&
        header->session_epoch != 0U && header->trade_date != 0U &&
        header->capacity != 0U &&
        header->capacity !=
            std::numeric_limits<std::uint32_t>::max() &&
        header->catalog_scope ==
            static_cast<std::uint32_t>(
                RealtimeCatalogScopeV2::kDeclaredDailyAShare) &&
        header->coverage_complete == 1U &&
        header->catalog_trade_date == header->trade_date &&
        header->reserved_catalog == 0U &&
        header->catalog_version != 0U &&
        AnyNonzero(header->layout_digest) &&
        KnownServerState(initial_state) &&
        HeaderFlagsValid(initial_flags) &&
        ((header->window_count != 0U) ==
         ((initial_flags &
           l2flow::ipc::kRealtimeHeaderKLineEnabledV2) != 0U)) &&
        header->region_count ==
            l2flow::ipc::kRealtimeWireRegionCountV2 &&
        header->region_descriptor_bytes ==
            sizeof(RealtimeWireRegionDescriptorV2) &&
        header->reserved_scalar == 0U &&
        AllZero(header->reserved) &&
        instruments != nullptr &&
        instruments->offset >= sizeof(RealtimeWireHeaderV2) &&
        CountRegion(
            *header, RealtimeRegionKindV2::kInstrumentRows) == 1U &&
        CountRegion(
            *header,
            RealtimeRegionKindV2::kInstrumentKeyArena) == 1U &&
        CountRegion(
            *header, RealtimeRegionKindV2::kKLineWindows) == 1U &&
        CountRegion(
            *header, RealtimeRegionKindV2::kLatestSnapshots) == 1U &&
        CountRegion(
            *header, RealtimeRegionKindV2::kLatestTicks) == 1U &&
        CountRegion(
            *header, RealtimeRegionKindV2::kLatestKLines) == 1U &&
        CountRegion(
            *header, RealtimeRegionKindV2::kTickRingSlots) == 1U &&
        CountRegion(
            *header, RealtimeRegionKindV2::kReserved8) == 1U &&
        CountRegion(
            *header, RealtimeRegionKindV2::kReserved9) == 1U &&
        RegionBefore(instruments, arena) &&
        RegionBefore(arena, windows) &&
        RegionBefore(windows, snapshots) &&
        RegionBefore(snapshots, latest_ticks) &&
        RegionBefore(latest_ticks, klines) &&
        RegionBefore(klines, ring) &&
        RegionBefore(ring, reserved8) &&
        RegionBefore(reserved8, reserved9) &&
        RegionValid(
            instruments,
            mapped_bytes,
            sizeof(RealtimeWireInstrumentV2),
            capacity,
            alignof(RealtimeWireInstrumentV2)) &&
        arena != nullptr && arena->element_count != 0U &&
        RegionValid(
            arena,
            mapped_bytes,
            1U,
            arena == nullptr ? 0U : arena->element_count,
            1U) &&
        RegionValid(
            windows,
            mapped_bytes,
            sizeof(RealtimeWireKLineWindowV2),
            window_count,
            alignof(RealtimeWireKLineWindowV2)) &&
        RegionValid(
            snapshots,
            mapped_bytes,
            sizeof(RealtimeWireSnapshotSlotV2),
            capacity,
            alignof(RealtimeWireSnapshotSlotV2)) &&
        RegionValid(
            latest_ticks,
            mapped_bytes,
            sizeof(RealtimeWireTickSlotV2),
            capacity,
            alignof(RealtimeWireTickSlotV2)) &&
        physical_kline_count != maximum &&
        RegionValid(
            klines,
            mapped_bytes,
            sizeof(RealtimeWireKLineSlotV2),
            physical_kline_count,
            alignof(RealtimeWireKLineSlotV2)) &&
        ring != nullptr && ring->element_count != 0U &&
        RegionValid(
            ring,
            mapped_bytes,
            sizeof(RealtimeWireTickSlotV2),
            ring == nullptr ? 0U : ring->element_count,
            alignof(RealtimeWireTickSlotV2)) &&
        ReservedRegionValid(reserved8, mapped_bytes) &&
        ReservedRegionValid(reserved9, mapped_bytes);
    if (!valid) {
        static_cast<void>(::munmap(
            mapping, static_cast<std::size_t>(mapped_bytes)));
        return L2FLOW_SHM_READER_LAYOUT_INVALID_V2;
    }

    auto* reader = new (std::nothrow) l2flow_shm_reader_v2();
    if (reader == nullptr) {
        static_cast<void>(::munmap(
            mapping, static_cast<std::size_t>(mapped_bytes)));
        return L2FLOW_SHM_READER_SYSTEM_ERROR_V2;
    }
    reader->mapping = mapping;
    reader->mapping_bytes = static_cast<std::size_t>(mapped_bytes);
    reader->header = header;
    const auto* const base = static_cast<const std::byte*>(mapping);
    reader->instruments =
        reinterpret_cast<const RealtimeWireInstrumentV2*>(
            base + instruments->offset);
    reader->key_arena = base + arena->offset;
    reader->key_arena_bytes =
        static_cast<std::size_t>(arena->length);
    reader->windows =
        reinterpret_cast<const RealtimeWireKLineWindowV2*>(
            base + windows->offset);
    reader->snapshots =
        reinterpret_cast<const RealtimeWireSnapshotSlotV2*>(
            base + snapshots->offset);
    reader->latest_ticks =
        reinterpret_cast<const RealtimeWireTickSlotV2*>(
            base + latest_ticks->offset);
    reader->klines =
        reinterpret_cast<const RealtimeWireKLineSlotV2*>(
            base + klines->offset);
    reader->kline_slots_per_table = logical_kline_count;
    reader->tick_ring =
        reinterpret_cast<const RealtimeWireTickSlotV2*>(
            base + ring->offset);
    reader->tick_ring_capacity = ring->element_count;

    const CatalogValidationResult catalog_result =
        ValidatePublishedIdentityPrefix(*reader);
    if (catalog_result != CatalogValidationResult::kValid) {
        l2flow_shm_reader_close_v2(reader);
        return L2FLOW_SHM_READER_LAYOUT_INVALID_V2;
    }
    for (std::size_t index = 0U; index < header->window_count;
         ++index) {
        if (reader->windows[index].window_id == 0U ||
            reader->windows[index].duration_ns == 0U ||
            reader->windows[index].reserved != 0U ||
            (index != 0U &&
             reader->windows[index - 1U].window_id >=
                 reader->windows[index].window_id)) {
            l2flow_shm_reader_close_v2(reader);
            return L2FLOW_SHM_READER_LAYOUT_INVALID_V2;
        }
    }
    *output = reader;
    return L2FLOW_SHM_READER_OK_V2;
}

extern "C" void l2flow_shm_reader_close_v2(
    l2flow_shm_reader_v2* reader) {
    if (reader == nullptr) {
        return;
    }
    if (reader->mapping != MAP_FAILED) {
        static_cast<void>(::munmap(
            const_cast<void*>(reader->mapping),
            reader->mapping_bytes));
        reader->mapping = MAP_FAILED;
    }
    delete reader;
}

extern "C" int l2flow_shm_reader_session_v2(
    const l2flow_shm_reader_v2* reader,
    l2flow_shm_session_info_v2* output) {
    if (reader == nullptr || output == nullptr) {
        return L2FLOW_SHM_READER_INVALID_ARGUMENT_V2;
    }
    StatusSnapshot status{};
    const StableCopyResult status_result =
        CopyStatus(*reader->header, &status);
    if (status_result == StableCopyResult::kInconsistent) {
        return L2FLOW_SHM_READER_INCONSISTENT_READ_V2;
    }
    if (status_result != StableCopyResult::kCopied) {
        return L2FLOW_SHM_READER_LAYOUT_INVALID_V2;
    }
    l2flow_shm_session_info_v2 result{};
    std::memcpy(
        result.run_id,
        reader->header->run_id.data(),
        sizeof(result.run_id));
    CopyDigestBytes(
        reader->header->layout_digest, result.layout_digest);
    CopyDigestBytes(status.catalog_digest, result.catalog_digest);
    result.session_epoch = reader->header->session_epoch;
    result.catalog_generation = status.catalog_generation;
    result.data_state_generation = status.data_state_generation;
    result.accepted_sequence = status.accepted_sequence;
    result.applied_sequence = status.applied_sequence;
    result.processing_lag_records =
        status.accepted_sequence - status.applied_sequence;
    result.tick_ring_capacity = reader->tick_ring_capacity;
    result.tick_contiguous_published_sequence =
        Atomic(
            reader->header
                ->tick_contiguous_published_sequence)
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
    result.published_records =
        Atomic(reader->header->published_records)
            .load(std::memory_order_acquire);
    result.trade_date = reader->header->trade_date;
    result.server_state =
        Atomic(reader->header->server_state)
            .load(std::memory_order_acquire);
    result.flags =
        Atomic(reader->header->flags)
            .load(std::memory_order_acquire);
    result.capacity = reader->header->capacity;
    result.window_count = reader->header->window_count;
    result.catalog_scope = reader->header->catalog_scope;
    result.coverage_complete =
        reader->header->coverage_complete;
    result.bound_count = status.bound_count;
    result.available_count = status.available_count;
    result.snapshot_available_count =
        status.snapshot_available_count;
    result.tick_available_count = status.tick_available_count;
    result.factor_eligible_count =
        status.factor_eligible_count;
    result.catalog_trade_date =
        reader->header->catalog_trade_date;
    result.catalog_version =
        reader->header->catalog_version;
    if (!KnownServerState(result.server_state) ||
        !HeaderFlagsValid(result.flags) ||
        result.tick_contiguous_published_sequence >
            result.tick_highest_published_sequence) {
        return L2FLOW_SHM_READER_LAYOUT_INVALID_V2;
    }
    *output = result;
    return L2FLOW_SHM_READER_OK_V2;
}

extern "C" int l2flow_shm_reader_health_v2(
    const l2flow_shm_reader_v2* reader,
    l2flow_shm_health_v2* output) {
    static_assert(sizeof(l2flow_shm_health_v2) == 32U);
    if (reader == nullptr || output == nullptr) {
        return L2FLOW_SHM_READER_INVALID_ARGUMENT_V2;
    }
    for (std::size_t attempt = 0U; attempt < kReadAttempts; ++attempt) {
        const std::uint32_t state_begin =
            Atomic(reader->header->server_state)
                .load(std::memory_order_acquire);
        l2flow_shm_health_v2 result{};
        result.session_epoch = reader->header->session_epoch;
        result.heartbeat_monotonic_ns =
            Atomic(reader->header->heartbeat_monotonic_ns)
                .load(std::memory_order_acquire);
        result.flags =
            Atomic(reader->header->flags)
                .load(std::memory_order_acquire);
        result.server_state =
            Atomic(reader->header->server_state)
                .load(std::memory_order_acquire);
        if (state_begin != result.server_state) {
            continue;
        }
        if (!KnownServerState(result.server_state) ||
            !HeaderFlagsValid(result.flags)) {
            return L2FLOW_SHM_READER_LAYOUT_INVALID_V2;
        }
        *output = result;
        return L2FLOW_SHM_READER_OK_V2;
    }
    return L2FLOW_SHM_READER_INCONSISTENT_READ_V2;
}

extern "C" int l2flow_shm_reader_instrument_v2(
    const l2flow_shm_reader_v2* reader,
    std::uint32_t instrument_id,
    void* row_output,
    std::size_t row_output_bytes,
    std::uint8_t* security_id_source_output,
    std::size_t security_id_source_capacity,
    std::size_t* security_id_source_written,
    std::uint8_t* security_id_output,
    std::size_t security_id_capacity,
    std::size_t* security_id_written,
    std::uint8_t* item_status) {
    if (reader == nullptr || row_output == nullptr ||
        row_output_bytes < sizeof(RealtimeWireInstrumentV2) ||
        security_id_source_written == nullptr ||
        security_id_written == nullptr || item_status == nullptr ||
        (security_id_source_capacity != 0U &&
         security_id_source_output == nullptr) ||
        (security_id_capacity != 0U &&
         security_id_output == nullptr)) {
        return L2FLOW_SHM_READER_INVALID_ARGUMENT_V2;
    }
    if (!HealthyForRead(*reader->header)) {
        return L2FLOW_SHM_READER_UNAVAILABLE_V2;
    }
    RealtimeWireInstrumentV2 row{};
    const PointRowResult result =
        ReadPointRow(*reader, instrument_id, &row);
    if (result == PointRowResult::kInconsistent) {
        return L2FLOW_SHM_READER_INCONSISTENT_READ_V2;
    }
    if (result == PointRowResult::kInvalidLayout) {
        return L2FLOW_SHM_READER_LAYOUT_INVALID_V2;
    }
    if (result == PointRowResult::kInvalidId ||
        result == PointRowResult::kUnbound) {
        *security_id_source_written = 0U;
        *security_id_written = 0U;
        *item_status =
            result == PointRowResult::kInvalidId
                ? L2FLOW_INSTRUMENT_INVALID_ID_V2
                : L2FLOW_INSTRUMENT_UNBOUND_V2;
        return L2FLOW_SHM_READER_OK_V2;
    }
    *security_id_source_written =
        row.security_id_source_length;
    *security_id_written = row.security_id_length;
    *item_status =
        row.binding_state ==
                static_cast<std::uint32_t>(
                    RealtimeInstrumentBindingStateV2::kAvailable)
            ? L2FLOW_INSTRUMENT_AVAILABLE_V2
            : L2FLOW_INSTRUMENT_BOUND_NO_DATA_V2;
    if (security_id_source_capacity <
            row.security_id_source_length ||
        security_id_capacity < row.security_id_length) {
        return L2FLOW_SHM_READER_BUFFER_TOO_SMALL_V2;
    }
    std::memcpy(row_output, &row, sizeof(row));
    if (row.security_id_source_length != 0U) {
        std::memcpy(
            security_id_source_output,
            reader->key_arena +
                row.security_id_source_offset,
            row.security_id_source_length);
    }
    std::memcpy(
        security_id_output,
        reader->key_arena + row.security_id_offset,
        row.security_id_length);
    return HealthyForRead(*reader->header)
               ? L2FLOW_SHM_READER_OK_V2
               : L2FLOW_SHM_READER_UNAVAILABLE_V2;
}

extern "C" int l2flow_shm_reader_resolve_instruments_v2(
    const l2flow_shm_reader_v2* reader,
    const std::uint8_t* markets,
    const std::uint8_t* const* security_id_sources,
    const std::size_t* security_id_source_lengths,
    const std::uint8_t* const* security_ids,
    const std::size_t* security_id_lengths,
    std::size_t count,
    std::uint32_t* instrument_ids,
    std::uint8_t* item_statuses) {
    if (reader == nullptr ||
        (count != 0U &&
         (markets == nullptr || security_id_sources == nullptr ||
          security_id_source_lengths == nullptr ||
          security_ids == nullptr ||
          security_id_lengths == nullptr ||
          instrument_ids == nullptr || item_statuses == nullptr))) {
        return L2FLOW_SHM_READER_INVALID_ARGUMENT_V2;
    }
    for (std::size_t index = 0U; index < count; ++index) {
        if ((security_id_source_lengths[index] != 0U &&
             security_id_sources[index] == nullptr) ||
            (security_id_lengths[index] != 0U &&
             security_ids[index] == nullptr)) {
            return L2FLOW_SHM_READER_INVALID_ARGUMENT_V2;
        }
    }
    if (!HealthyForRead(*reader->header)) {
        return L2FLOW_SHM_READER_UNAVAILABLE_V2;
    }
    try {
        std::vector<std::uint32_t> resolved(count, 0U);
        std::vector<std::uint8_t> statuses(count, 0U);
        std::scoped_lock lock(reader->key_index_mutex);
        for (std::size_t attempt = 0U; attempt < kReadAttempts;
             ++attempt) {
            StatusSnapshot status{};
            const StableCopyResult status_result =
                CopyCatalogCut(*reader->header, &status);
            if (status_result == StableCopyResult::kInconsistent) {
                continue;
            }
            if (status_result != StableCopyResult::kCopied) {
                return L2FLOW_SHM_READER_LAYOUT_INVALID_V2;
            }
            const KeyIndexResult refresh =
                RefreshKeyIndexLocked(*reader, status);
            if (refresh == KeyIndexResult::kChanged) {
                continue;
            }
            if (refresh == KeyIndexResult::kResourceExhausted) {
                return L2FLOW_SHM_READER_SYSTEM_ERROR_V2;
            }
            if (refresh != KeyIndexResult::kOk) {
                return L2FLOW_SHM_READER_LAYOUT_INVALID_V2;
            }
            for (std::size_t index = 0U; index < count; ++index) {
                resolved[index] = 0U;
                if (markets[index] != 1U &&
                    markets[index] != 2U) {
                    statuses[index] =
                        L2FLOW_INSTRUMENT_LOOKUP_INVALID_MARKET_V2;
                    continue;
                }
                if (security_id_lengths[index] == 0U) {
                    statuses[index] =
                        L2FLOW_INSTRUMENT_LOOKUP_EMPTY_SECURITY_ID_V2;
                    continue;
                }
                const std::size_t ordinal =
                    FindInstrumentOrdinalByKey(
                        *reader,
                        markets[index],
                        security_id_sources[index],
                        security_id_source_lengths[index],
                        security_ids[index],
                        security_id_lengths[index]);
                if (ordinal ==
                    std::numeric_limits<std::size_t>::max()) {
                    statuses[index] =
                        L2FLOW_INSTRUMENT_LOOKUP_UNKNOWN_V2;
                    continue;
                }
                resolved[index] =
                    static_cast<std::uint32_t>(ordinal + 1U);
                statuses[index] =
                    L2FLOW_INSTRUMENT_LOOKUP_FOUND_V2;
            }
            StatusSnapshot end_status{};
            const StableCopyResult end_result =
                CopyCatalogCut(*reader->header, &end_status);
            if (end_result == StableCopyResult::kInconsistent) {
                continue;
            }
            if (end_result != StableCopyResult::kCopied) {
                return L2FLOW_SHM_READER_LAYOUT_INVALID_V2;
            }
            if (!CatalogCutEqual(status, end_status)) {
                continue;
            }
            if (count != 0U) {
                std::memcpy(
                    instrument_ids,
                    resolved.data(),
                    count * sizeof(resolved[0U]));
                std::memcpy(
                    item_statuses,
                    statuses.data(),
                    count * sizeof(statuses[0U]));
            }
            return HealthyForRead(*reader->header)
                       ? L2FLOW_SHM_READER_OK_V2
                       : L2FLOW_SHM_READER_UNAVAILABLE_V2;
        }
        return L2FLOW_SHM_READER_INCONSISTENT_READ_V2;
    } catch (...) {
        return L2FLOW_SHM_READER_SYSTEM_ERROR_V2;
    }
}

extern "C" int l2flow_shm_reader_latest_snapshots_v2(
    const l2flow_shm_reader_v2* reader,
    const std::uint32_t* instrument_ids,
    std::size_t count,
    void* outputs,
    std::size_t output_stride,
    std::uint8_t* item_statuses) {
    return LatestBatch<
        RealtimeWireSnapshotSlotV2,
        RealtimeWireSnapshotPayloadV2>(
        reader,
        instrument_ids,
        count,
        outputs,
        output_stride,
        item_statuses,
        reader == nullptr ? nullptr : reader->snapshots,
        l2flow::ipc::kRealtimeInstrumentHasSnapshotV2,
        [](const RealtimeWireSnapshotPayloadV2& payload,
           std::size_t ordinal,
           std::uint32_t instrument_id) noexcept {
            return SnapshotPayloadCanonical(payload) &&
                   payload.common.instrument_id == instrument_id &&
                   payload.common.ordinal == ordinal;
        });
}

extern "C" int l2flow_shm_reader_latest_ticks_v2(
    const l2flow_shm_reader_v2* reader,
    const std::uint32_t* instrument_ids,
    std::size_t count,
    void* outputs,
    std::size_t output_stride,
    std::uint8_t* item_statuses) {
    return LatestBatch<
        RealtimeWireTickSlotV2,
        RealtimeWireTickPayloadV2>(
        reader,
        instrument_ids,
        count,
        outputs,
        output_stride,
        item_statuses,
        reader == nullptr ? nullptr : reader->latest_ticks,
        l2flow::ipc::kRealtimeInstrumentHasTickV2,
        [](const RealtimeWireTickPayloadV2& payload,
           std::size_t ordinal,
           std::uint32_t instrument_id) noexcept {
            return TickPayloadCanonical(payload) &&
                   payload.common.instrument_id == instrument_id &&
                   payload.common.ordinal == ordinal;
        });
}

extern "C" int l2flow_shm_reader_latest_klines_v2(
    const l2flow_shm_reader_v2* reader,
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
        output_stride < sizeof(RealtimeWireKLinePayloadV2) ||
        (count != 0U &&
         output_stride >
             std::numeric_limits<std::size_t>::max() / count)) {
        return L2FLOW_SHM_READER_INVALID_ARGUMENT_V2;
    }
    if (!HealthyForRead(*reader->header)) {
        return L2FLOW_SHM_READER_UNAVAILABLE_V2;
    }
    const std::uint64_t completed_generation =
        Atomic(reader->header->kline_generation)
            .load(std::memory_order_acquire);
    const std::uint64_t table =
        completed_generation %
        l2flow::ipc::kRealtimeKLineTableCountV2;
    const std::uint64_t table_offset =
        table * reader->kline_slots_per_table;
    auto* const bytes = static_cast<std::byte*>(outputs);
    for (std::size_t index = 0U; index < count; ++index) {
        std::size_t ordinal = 0U;
        const PointRowResult ordinal_result =
            ResolvePointOrdinal(
                *reader, instrument_ids[index], &ordinal);
        if (ordinal_result == PointRowResult::kInvalidId) {
            item_statuses[index] =
                L2FLOW_LATEST_INVALID_INSTRUMENT_ID_V2;
            continue;
        }
        if (window_ids[index] == 0U) {
            item_statuses[index] =
                L2FLOW_LATEST_INVALID_WINDOW_ID_V2;
            continue;
        }
        if (ordinal_result == PointRowResult::kUnbound) {
            item_statuses[index] = L2FLOW_LATEST_UNBOUND_V2;
            continue;
        }
        if (ordinal_result != PointRowResult::kBound) {
            return L2FLOW_SHM_READER_LAYOUT_INVALID_V2;
        }
        const std::size_t window =
            FindWindowIndex(*reader, window_ids[index]);
        if (window == std::numeric_limits<std::size_t>::max()) {
            item_statuses[index] =
                L2FLOW_LATEST_UNKNOWN_WINDOW_V2;
            continue;
        }

        RealtimeWireKLinePayloadV2 payload{};
        StableCopyResult copy_result =
            StableCopyResult::kNeverPublished;
        if (completed_generation != 0U) {
            const std::uint64_t slot =
                table_offset +
                static_cast<std::uint64_t>(ordinal) *
                    reader->header->window_count +
                static_cast<std::uint64_t>(window);
            const RealtimeWireKLineSlotV2& source =
                reader->klines[static_cast<std::size_t>(slot)];
            if (!AllZero(source.reserved)) {
                return L2FLOW_SHM_READER_LAYOUT_INVALID_V2;
            }
            copy_result = CopySlot(source, &payload);
            if (copy_result == StableCopyResult::kInconsistent) {
                return L2FLOW_SHM_READER_INCONSISTENT_READ_V2;
            }
            if (copy_result == StableCopyResult::kInvalid) {
                return L2FLOW_SHM_READER_LAYOUT_INVALID_V2;
            }
            if (copy_result == StableCopyResult::kCopied) {
                if (payload.generation != completed_generation) {
                    return L2FLOW_SHM_READER_INCONSISTENT_READ_V2;
                }
                if (payload.generation == 0U ||
                    payload.trade_date !=
                        reader->header->trade_date ||
                    payload.instrument_id !=
                        instrument_ids[index] ||
                    payload.window_id != window_ids[index] ||
                    payload.window_duration_ns !=
                        reader->windows[window].duration_ns ||
                    payload.reserved0 != 0U ||
                    !AllZero(payload.reserved) ||
                    payload.present > 1U) {
                    return L2FLOW_SHM_READER_LAYOUT_INVALID_V2;
                }
                if (payload.present == 1U) {
                    // The completed-generation release store follows every
                    // slot publication. A present, self-identifying payload is
                    // therefore the complete AVAILABLE path and need not
                    // contend with the per-event instrument row seqcount.
                    std::memcpy(
                        bytes + index * output_stride,
                        &payload,
                        sizeof(payload));
                    item_statuses[index] =
                        L2FLOW_LATEST_AVAILABLE_V2;
                    continue;
                }
            }
        }

        // No completed present payload exists for this fixed ordinal/window.
        // Read the instrument row only to preserve the exact BOUND_NO_DATA
        // versus type-unavailable status distinction.
        RealtimeWireInstrumentV2 row{};
        const PointRowResult row_result =
            ReadPointRow(*reader, instrument_ids[index], &row);
        if (row_result == PointRowResult::kInvalidId) {
            item_statuses[index] =
                L2FLOW_LATEST_INVALID_INSTRUMENT_ID_V2;
            continue;
        }
        if (row_result == PointRowResult::kUnbound) {
            item_statuses[index] = L2FLOW_LATEST_UNBOUND_V2;
            continue;
        }
        if (row_result == PointRowResult::kInconsistent) {
            return L2FLOW_SHM_READER_INCONSISTENT_READ_V2;
        }
        if (row_result != PointRowResult::kBound) {
            return L2FLOW_SHM_READER_LAYOUT_INVALID_V2;
        }
        if (row.binding_state ==
            static_cast<std::uint32_t>(
                RealtimeInstrumentBindingStateV2::kBoundNoData)) {
            item_statuses[index] =
                L2FLOW_LATEST_BOUND_NO_DATA_V2;
            continue;
        }
        if ((row.availability_flags &
             l2flow::ipc::kRealtimeInstrumentHasKLineV2) == 0U ||
            completed_generation == 0U ||
            (copy_result == StableCopyResult::kCopied &&
             payload.present == 0U)) {
            item_statuses[index] =
                L2FLOW_LATEST_TYPE_UNAVAILABLE_V2;
            continue;
        }

        // A HAS_KLINE row without a slot in the generation sampled above is
        // valid only if generation publication raced this read. Preserve the
        // existing retry signal for that race; a stable generation would make
        // the mapping internally inconsistent.
        if (Atomic(reader->header->kline_generation)
                .load(std::memory_order_acquire) !=
            completed_generation) {
            return L2FLOW_SHM_READER_INCONSISTENT_READ_V2;
        }
        return L2FLOW_SHM_READER_LAYOUT_INVALID_V2;
    }
    if (!HealthyForRead(*reader->header)) {
        return L2FLOW_SHM_READER_UNAVAILABLE_V2;
    }
    return Atomic(reader->header->kline_generation)
                       .load(std::memory_order_acquire) ==
                   completed_generation
               ? L2FLOW_SHM_READER_OK_V2
               : L2FLOW_SHM_READER_INCONSISTENT_READ_V2;
}

extern "C" int l2flow_shm_reader_ticks_v2(
    const l2flow_shm_reader_v2* reader,
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
        output_stride < sizeof(RealtimeWireTickPayloadV2) ||
        (maximum_records != 0U &&
         output_stride >
             std::numeric_limits<std::size_t>::max() /
                 maximum_records)) {
        return L2FLOW_SHM_READER_INVALID_ARGUMENT_V2;
    }
    *written = 0U;
    *next_sequence = expected_sequence;
    *observed_sequence = 0U;
    if (!HealthyForRead(*reader->header)) {
        return L2FLOW_SHM_READER_UNAVAILABLE_V2;
    }
    const std::uint64_t contiguous =
        Atomic(reader->header->tick_contiguous_published_sequence)
            .load(std::memory_order_acquire);
    const std::uint64_t highest =
        Atomic(reader->header->tick_highest_published_sequence)
            .load(std::memory_order_acquire);
    if (contiguous > highest) {
        return L2FLOW_SHM_READER_LAYOUT_INVALID_V2;
    }
    const std::uint64_t oldest =
        contiguous >= reader->tick_ring_capacity
            ? contiguous - reader->tick_ring_capacity + 1U
            : 1U;
    if (expected_sequence < oldest) {
        *observed_sequence = oldest;
        return L2FLOW_SHM_READER_OVERRUN_V2;
    }
    auto* const bytes = static_cast<std::byte*>(outputs);
    std::uint64_t sequence = expected_sequence;
    while (*written < maximum_records && sequence <= contiguous) {
        const std::uint64_t slot_index =
            (sequence - 1U) % reader->tick_ring_capacity;
        const RealtimeWireTickSlotV2& slot =
            reader->tick_ring[static_cast<std::size_t>(slot_index)];
        if (!AllZero(slot.reserved)) {
            return L2FLOW_SHM_READER_LAYOUT_INVALID_V2;
        }
        RealtimeWireTickPayloadV2 payload{};
        if (CopySlot(slot, &payload) !=
            StableCopyResult::kCopied) {
            return L2FLOW_SHM_READER_INCONSISTENT_READ_V2;
        }
        if (payload.common.tick_stream_sequence != sequence) {
            *observed_sequence =
                payload.common.tick_stream_sequence;
            return payload.common.tick_stream_sequence > sequence
                       ? L2FLOW_SHM_READER_OVERRUN_V2
                       : L2FLOW_SHM_READER_INCONSISTENT_READ_V2;
        }
        if (!TickPayloadCanonical(payload) ||
            payload.common.ordinal >= reader->header->capacity) {
            return L2FLOW_SHM_READER_LAYOUT_INVALID_V2;
        }
        RealtimeWireInstrumentV2 row{};
        const StableCopyResult row_result =
            CopyBoundRowByOrdinal(
                *reader, payload.common.ordinal, &row);
        if (row_result == StableCopyResult::kInconsistent) {
            return L2FLOW_SHM_READER_INCONSISTENT_READ_V2;
        }
        if (row_result != StableCopyResult::kCopied ||
            row.instrument_id != payload.common.instrument_id) {
            return L2FLOW_SHM_READER_LAYOUT_INVALID_V2;
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
               ? L2FLOW_SHM_READER_OK_V2
               : L2FLOW_SHM_READER_UNAVAILABLE_V2;
}

extern "C" int l2flow_shm_reader_select_instruments_v2(
    const l2flow_shm_reader_v2* reader,
    std::uint32_t selection_scope,
    std::uint32_t* instrument_ids,
    std::size_t instrument_id_capacity,
    std::size_t* required_count,
    l2flow_selection_envelope_v2* envelope) {
    if (reader == nullptr ||
        !SelectionScopeValid(selection_scope) ||
        required_count == nullptr || envelope == nullptr ||
        (instrument_id_capacity != 0U &&
         instrument_ids == nullptr)) {
        return L2FLOW_SHM_READER_INVALID_ARGUMENT_V2;
    }
    if (!HealthyForRead(*reader->header)) {
        return L2FLOW_SHM_READER_UNAVAILABLE_V2;
    }
    try {
        for (std::size_t attempt = 0U; attempt < kReadAttempts;
             ++attempt) {
            StatusSnapshot status{};
            const StableCopyResult status_result =
                CopyStatus(*reader->header, &status);
            if (status_result == StableCopyResult::kInconsistent) {
                continue;
            }
            if (status_result != StableCopyResult::kCopied) {
                return L2FLOW_SHM_READER_LAYOUT_INVALID_V2;
            }
            std::vector<std::uint32_t> selected;
            selected.reserve(SelectionCount(status, selection_scope));
            ScanCounts counts{};
            bool changed = false;
            for (std::size_t ordinal = 0U;
                 ordinal < status.bound_count;
                 ++ordinal) {
                if (selection_scope ==
                    static_cast<std::uint32_t>(
                        RealtimeSelectionScopeV2::kCatalogAll)) {
                    if (!PublishedBoundIdentityValid(
                            *reader, ordinal)) {
                        return L2FLOW_SHM_READER_LAYOUT_INVALID_V2;
                    }
                    selected.push_back(
                        static_cast<std::uint32_t>(ordinal + 1U));
                    continue;
                }
                RealtimeWireInstrumentV2 row{};
                if (CopyInstrumentRow(
                        reader->instruments[ordinal], &row) !=
                    StableCopyResult::kCopied) {
                    changed = true;
                    break;
                }
                if (!BoundRowValid(*reader, row, ordinal)) {
                    return L2FLOW_SHM_READER_LAYOUT_INVALID_V2;
                }
                AddBoundRow(row, &counts);
                if (RowMatchesSelection(row, selection_scope)) {
                    selected.push_back(row.instrument_id);
                }
            }
            if (changed) {
                continue;
            }
            StatusSnapshot end_status{};
            const StableCopyResult end_result =
                CopyStatus(*reader->header, &end_status);
            if (end_result == StableCopyResult::kInconsistent) {
                continue;
            }
            if (end_result != StableCopyResult::kCopied) {
                return L2FLOW_SHM_READER_LAYOUT_INVALID_V2;
            }
            const bool bound_selection =
                selection_scope ==
                static_cast<std::uint32_t>(
                    RealtimeSelectionScopeV2::kCatalogAll);
            if (!(bound_selection
                      ? CatalogCutEqual(status, end_status)
                      : SelectionStateCutEqual(
                            status, end_status))) {
                continue;
            }
            const std::uint32_t expected =
                SelectionCount(end_status, selection_scope);
            if ((!bound_selection &&
                 !CountsMatch(counts, end_status)) ||
                selected.size() != expected) {
                return L2FLOW_SHM_READER_LAYOUT_INVALID_V2;
            }
            l2flow_selection_envelope_v2 local_envelope{};
            FillSelectionEnvelope(
                *reader,
                end_status,
                selection_scope,
                expected,
                &local_envelope);
            *required_count = expected;
            *envelope = local_envelope;
            if (instrument_id_capacity < selected.size()) {
                return L2FLOW_SHM_READER_BUFFER_TOO_SMALL_V2;
            }
            if (!selected.empty()) {
                std::memcpy(
                    instrument_ids,
                    selected.data(),
                    selected.size() *
                        sizeof(selected.front()));
            }
            return HealthyForRead(*reader->header)
                       ? L2FLOW_SHM_READER_OK_V2
                       : L2FLOW_SHM_READER_UNAVAILABLE_V2;
        }
        return L2FLOW_SHM_READER_INCONSISTENT_READ_V2;
    } catch (...) {
        return L2FLOW_SHM_READER_SYSTEM_ERROR_V2;
    }
}

struct l2flow_instrument_raw_event_history_cursor_v2;

struct l2flow_instrument_raw_event_history_session_v2 final {
    int socket_fd = -1;
    RealtimeGenerationEndpointV2 target{};
    std::uint64_t session_token = 0U;
    l2flow_instrument_raw_event_history_cursor_v2* active_cursor =
        nullptr;
};

struct l2flow_instrument_raw_event_history_cursor_v2 final {
    l2flow_instrument_raw_event_history_session_v2* session = nullptr;
    RealtimeInstrumentTickDeltaMetadataV2 metadata{};
    std::uint32_t requested_page_records = 0U;
    std::uint64_t read_token = 0U;
    std::uint64_t next_page_index = 0U;
    std::uint64_t cumulative_record_count = 0U;
    std::array<std::uint64_t, 4U>
        cumulative_source_record_counts{};
    std::array<std::uint64_t, 4U> last_source_sequences{};
    std::uint64_t last_ingress_sequence = 0U;
    std::uint64_t last_tick_stream_sequence = 0U;
    const void* page_mapping = MAP_FAILED;
    std::size_t page_mapping_bytes = 0U;
    bool eof = false;
    bool failed = false;
};

namespace {

constexpr std::uint32_t kMaximumEventHistoryPageRecords =
    1024U * 1024U;

static_assert(
    sizeof(RealtimeGenerationEndpointV2) ==
    sizeof(l2flow_instrument_raw_event_history_endpoint_v2));
static_assert(
    sizeof(RealtimeInstrumentTickDeltaCheckpointV2) ==
    sizeof(l2flow_instrument_raw_event_history_checkpoint_v2));
static_assert(
    sizeof(RealtimeInstrumentTickDeltaMetadataV2) ==
    sizeof(l2flow_instrument_raw_event_history_metadata_v2));

template <typename Destination, typename Source>
void CopyObjectBytes(
    Destination* destination,
    const Source& source) noexcept {
    static_assert(sizeof(Destination) == sizeof(Source));
    static_assert(std::is_trivially_copyable_v<Destination>);
    static_assert(std::is_trivially_copyable_v<Source>);
    std::memcpy(
        static_cast<void*>(destination),
        static_cast<const void*>(&source),
        sizeof(source));
}

template <typename Value>
bool ObjectBytesZero(const Value& value) noexcept {
    const auto* const bytes =
        reinterpret_cast<const std::uint8_t*>(&value);
    return std::all_of(
        bytes,
        bytes + sizeof(value),
        [](std::uint8_t byte) noexcept { return byte == 0U; });
}

template <std::size_t Size>
bool CByteArrayAnyNonzero(
    const std::uint8_t (&value)[Size]) noexcept {
    return std::any_of(
        value,
        value + Size,
        [](std::uint8_t byte) noexcept { return byte != 0U; });
}

bool SessionExpectationValid(
    const l2flow_shm_session_info_v2& session) noexcept {
    return CByteArrayAnyNonzero(session.run_id) &&
           CByteArrayAnyNonzero(session.catalog_digest) &&
           session.session_epoch != 0U && session.trade_date != 0U &&
           session.capacity != 0U &&
           session.catalog_scope ==
               static_cast<std::uint32_t>(
                   RealtimeCatalogScopeV2::kDeclaredDailyAShare) &&
           session.coverage_complete == 1U &&
           session.catalog_generation == 1U &&
           session.bound_count == session.capacity &&
           session.catalog_trade_date == session.trade_date &&
           session.reserved_catalog == 0U &&
           session.catalog_version != 0U;
}

bool EndpointMatchesSession(
    const RealtimeGenerationEndpointV2& endpoint,
    const l2flow_shm_session_info_v2& session) noexcept {
    return std::equal(
               endpoint.run_id.begin(),
               endpoint.run_id.end(),
               session.run_id) &&
           endpoint.session_epoch == session.session_epoch &&
           endpoint.trade_date == session.trade_date &&
           endpoint.capacity == session.capacity &&
           endpoint.catalog_generation ==
               session.catalog_generation &&
           endpoint.bound_count == session.bound_count &&
           endpoint.catalog_scope == session.catalog_scope &&
           endpoint.coverage_complete ==
               session.coverage_complete &&
           std::memcmp(
               endpoint.catalog_digest.data(),
               session.catalog_digest,
               sizeof(session.catalog_digest)) == 0;
}

void ReleaseHistoryPage(
    l2flow_instrument_raw_event_history_cursor_v2* cursor) noexcept {
    if (cursor == nullptr) {
        return;
    }
    if (cursor->page_mapping != MAP_FAILED &&
        cursor->page_mapping_bytes != 0U) {
        static_cast<void>(::munmap(
            const_cast<void*>(cursor->page_mapping),
            cursor->page_mapping_bytes));
    }
    cursor->page_mapping = MAP_FAILED;
    cursor->page_mapping_bytes = 0U;
}

void CloseHistoryTransport(
    l2flow_instrument_raw_event_history_session_v2* session) noexcept {
    if (session == nullptr || session->socket_fd < 0) {
        return;
    }
    static_cast<void>(::shutdown(session->socket_fd, SHUT_RDWR));
    static_cast<void>(::close(session->socket_fd));
    session->socket_fd = -1;
    session->session_token = 0U;
}

void FailHistoryCursor(
    l2flow_instrument_raw_event_history_cursor_v2* cursor) noexcept {
    if (cursor == nullptr) {
        return;
    }
    ReleaseHistoryPage(cursor);
    cursor->failed = true;
    cursor->read_token = 0U;
    if (cursor->session != nullptr) {
        if (cursor->session->active_cursor == cursor) {
            cursor->session->active_cursor = nullptr;
        }
        CloseHistoryTransport(cursor->session);
        cursor->session = nullptr;
    }
}

std::uint64_t NextHistoryRequestId() noexcept {
    static std::atomic<std::uint64_t> next{1U};
    const std::uint64_t value =
        next.fetch_add(1U, std::memory_order_relaxed);
    if (value != 0U) {
        return value;
    }
    return next.fetch_add(1U, std::memory_order_relaxed);
}

bool SendHistoryRequest(
    int socket_fd,
    const void* request,
    std::size_t request_bytes) noexcept {
    if (socket_fd < 0 || request == nullptr || request_bytes == 0U) {
        return false;
    }
    ssize_t result = -1;
    do {
        result = ::send(
            socket_fd,
            request,
            request_bytes,
            MSG_NOSIGNAL);
    } while (result < 0 && errno == EINTR);
    return result >= 0 &&
           static_cast<std::size_t>(result) == request_bytes;
}

template <typename Response>
int ReceiveHistoryResponse(
    int socket_fd,
    Response* output,
    int* received_fd,
    std::size_t* received_fd_count) noexcept {
    if (socket_fd < 0 || output == nullptr ||
        received_fd == nullptr || received_fd_count == nullptr) {
        return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_INVALID_ARGUMENT_V2;
    }
    *output = {};
    *received_fd = -1;
    *received_fd_count = 0U;
    std::array<std::byte, CMSG_SPACE(sizeof(int) * 2U)> control{};
    iovec vector{};
    vector.iov_base = output;
    vector.iov_len = sizeof(Response);
    msghdr message{};
    message.msg_iov = &vector;
    message.msg_iovlen = 1U;
    message.msg_control = control.data();
    message.msg_controllen = control.size();
    ssize_t result = -1;
    do {
        result = ::recvmsg(
            socket_fd,
            &message,
#ifdef MSG_CMSG_CLOEXEC
            MSG_CMSG_CLOEXEC
#else
            0
#endif
        );
    } while (result < 0 && errno == EINTR);
    if (result <= 0) {
        return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_SYSTEM_ERROR_V2;
    }
    const bool packet_shape_valid =
        result >= 0 &&
        static_cast<std::size_t>(result) == sizeof(Response) &&
        (message.msg_flags & (MSG_TRUNC | MSG_CTRUNC)) == 0;
    bool ancillary_valid = true;
    if (result >= 0) {
        for (cmsghdr* header = CMSG_FIRSTHDR(&message);
             header != nullptr;
             header = CMSG_NXTHDR(&message, header)) {
            if (header->cmsg_level != SOL_SOCKET ||
                header->cmsg_type != SCM_RIGHTS ||
                header->cmsg_len < CMSG_LEN(sizeof(int))) {
                ancillary_valid = false;
                continue;
            }
            const std::size_t payload_bytes =
                header->cmsg_len - CMSG_LEN(0U);
            if (payload_bytes % sizeof(int) != 0U) {
                ancillary_valid = false;
                continue;
            }
            const std::size_t count =
                payload_bytes / sizeof(int);
            const auto* const descriptors =
                reinterpret_cast<const int*>(CMSG_DATA(header));
            for (std::size_t index = 0U; index < count; ++index) {
                if (*received_fd_count == 0U) {
                    *received_fd = descriptors[index];
                } else {
                    static_cast<void>(::close(descriptors[index]));
                }
                ++(*received_fd_count);
            }
        }
    }
    if (!packet_shape_valid || !ancillary_valid ||
        *received_fd_count > 1U) {
        if (*received_fd >= 0) {
            static_cast<void>(::close(*received_fd));
            *received_fd = -1;
        }
        return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_PROTOCOL_ERROR_V2;
    }
#ifndef MSG_CMSG_CLOEXEC
    if (*received_fd >= 0) {
        const int flags = ::fcntl(*received_fd, F_GETFD);
        if (flags < 0 ||
            ::fcntl(*received_fd, F_SETFD, flags | FD_CLOEXEC) !=
                0) {
            static_cast<void>(::close(*received_fd));
            *received_fd = -1;
            return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_SYSTEM_ERROR_V2;
        }
    }
#endif
    return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_OK_V2;
}

template <typename Response>
bool HistoryResponsePrefixValid(
    const Response& response,
    std::uint64_t request_id,
    std::uint16_t known_flags) noexcept {
    return response.magic == l2flow::ipc::kRealtimeControlMagicV2 &&
           response.protocol_major ==
               l2flow::ipc::kRealtimeWireMajorV2 &&
           response.protocol_minor ==
               l2flow::ipc::kRealtimeWireMinorV2 &&
           response.message_bytes == sizeof(Response) &&
           response.request_id == request_id &&
           (response.flags & ~known_flags) == 0U;
}

int MapHistoryStatus(std::uint16_t status) noexcept {
    switch (
        static_cast<RealtimeInstrumentTickDeltaControlStatusV2>(
            status)) {
        case RealtimeInstrumentTickDeltaControlStatusV2::kOk:
            return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_OK_V2;
        case RealtimeInstrumentTickDeltaControlStatusV2::
            kInvalidRequest:
            return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_PROTOCOL_ERROR_V2;
        case RealtimeInstrumentTickDeltaControlStatusV2::
            kUnsupportedVersion:
            return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_ABI_MISMATCH_V2;
        case RealtimeInstrumentTickDeltaControlStatusV2::kUnavailable:
            return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_UNAVAILABLE_V2;
        case RealtimeInstrumentTickDeltaControlStatusV2::kNotFound:
            return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_NOT_FOUND_V2;
        case RealtimeInstrumentTickDeltaControlStatusV2::
            kResourceExhausted:
            return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_RESOURCE_EXHAUSTED_V2;
        case RealtimeInstrumentTickDeltaControlStatusV2::
            kInternalFailure:
            return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_PROTOCOL_ERROR_V2;
        case RealtimeInstrumentTickDeltaControlStatusV2::
            kCheckpointMismatch:
            return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_CHECKPOINT_MISMATCH_V2;
    }
    return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_PROTOCOL_ERROR_V2;
}

bool ErrorResponseCanonical(
    const RealtimeInstrumentTickDeltaOpenSessionResponseV2&
        response) noexcept {
    return response.flags == 0U && response.reserved0 == 0U &&
           ObjectBytesZero(response.target_generation) &&
           response.delta_session_token == 0U;
}

bool ErrorResponseCanonical(
    const RealtimeInstrumentTickDeltaOpenInstrumentResponseV2&
        response) noexcept {
    return response.flags == 0U && response.reserved0 == 0U &&
           response.initial_read_token == 0U &&
           ObjectBytesZero(response.metadata);
}

bool ErrorResponseCanonical(
    const RealtimeInstrumentTickDeltaReadResponseV2&
        response) noexcept {
    return response.flags == 0U && response.record_count == 0U &&
           response.page_mapping_bytes == 0U &&
           response.page_index == 0U &&
           response.target_generation == 0U &&
           response.next_read_token == 0U;
}

bool ConfigureHistoryTimeout(
    int socket_fd,
    std::uint32_t timeout_ms) noexcept {
    if (timeout_ms == 0U) {
        return true;
    }
    timeval timeout{};
    timeout.tv_sec =
        static_cast<time_t>(timeout_ms / 1000U);
    timeout.tv_usec =
        static_cast<suseconds_t>(
            (timeout_ms % 1000U) * 1000U);
    return ::setsockopt(
               socket_fd,
               SOL_SOCKET,
               SO_RCVTIMEO,
               &timeout,
               sizeof(timeout)) == 0 &&
           ::setsockopt(
               socket_fd,
               SOL_SOCKET,
               SO_SNDTIMEO,
               &timeout,
           sizeof(timeout)) == 0;
}

bool HistorySocketPathSyntaxValid(const char* path) noexcept {
    if (path == nullptr || path[0U] != '/') {
        return false;
    }
    sockaddr_un address{};
    const std::size_t path_length =
        ::strnlen(path, sizeof(address.sun_path));
    return path_length != 0U &&
           path_length < sizeof(address.sun_path);
}

int ConnectHistorySocket(
    const char* path,
    std::uint32_t timeout_ms) noexcept {
    if (!HistorySocketPathSyntaxValid(path)) {
        return -1;
    }
    sockaddr_un address{};
    const std::size_t path_length =
        ::strnlen(path, sizeof(address.sun_path));
    if (path_length == 0U ||
        path_length >= sizeof(address.sun_path)) {
        return -1;
    }
    struct stat path_stat {};
    if (::lstat(path, &path_stat) != 0 ||
        !S_ISSOCK(path_stat.st_mode) ||
        path_stat.st_uid != ::geteuid()) {
        return -1;
    }
    const int socket_fd =
        ::socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0);
    if (socket_fd < 0 ||
        !ConfigureHistoryTimeout(socket_fd, timeout_ms)) {
        if (socket_fd >= 0) {
            static_cast<void>(::close(socket_fd));
        }
        return -1;
    }
    address.sun_family = AF_UNIX;
    std::memcpy(address.sun_path, path, path_length + 1U);
    if (::connect(
            socket_fd,
            reinterpret_cast<const sockaddr*>(&address),
            static_cast<socklen_t>(sizeof(address))) != 0) {
        static_cast<void>(::close(socket_fd));
        return -1;
    }
    return socket_fd;
}

bool HistoryPageDescriptorValid(
    int descriptor,
    std::uint64_t mapping_bytes) noexcept {
    if (descriptor < 0 ||
        mapping_bytes <
            l2flow::ipc::
                kRealtimeInstrumentTickDeltaPageHeaderBytesV2 ||
        mapping_bytes >
            static_cast<std::uint64_t>(
                std::numeric_limits<std::size_t>::max())) {
        return false;
    }
    const int descriptor_flags = ::fcntl(descriptor, F_GETFL);
    const int seals = ::fcntl(descriptor, F_GET_SEALS);
    constexpr int required_seals =
        F_SEAL_WRITE | F_SEAL_GROW | F_SEAL_SHRINK | F_SEAL_SEAL;
    struct stat descriptor_stat {};
    return descriptor_flags >= 0 && seals >= 0 &&
           (descriptor_flags & O_ACCMODE) == O_RDONLY &&
           (seals & required_seals) == required_seals &&
           ::fstat(descriptor, &descriptor_stat) == 0 &&
           S_ISREG(descriptor_stat.st_mode) &&
           descriptor_stat.st_size >= 0 &&
           static_cast<std::uint64_t>(descriptor_stat.st_size) ==
               mapping_bytes;
}

bool DeltaMetadataEqual(
    const RealtimeInstrumentTickDeltaMetadataV2& left,
    const RealtimeInstrumentTickDeltaMetadataV2& right) noexcept {
    return std::memcmp(&left, &right, sizeof(left)) == 0;
}

bool ValidateHistoryPayloads(
    l2flow_instrument_raw_event_history_cursor_v2* cursor,
    const RealtimeWireTickPayloadV2* records,
    std::size_t record_count,
    const RealtimeInstrumentTickDeltaPageHeaderV2& header) noexcept {
    if (cursor == nullptr || records == nullptr ||
        record_count == 0U) {
        return false;
    }
    const auto& metadata = cursor->metadata;
    const auto& target = metadata.target_checkpoint;
    std::array<std::uint64_t, 4U> source_counts =
        cursor->cumulative_source_record_counts;
    std::array<std::uint64_t, 4U> last_sources =
        cursor->last_source_sequences;
    std::uint64_t prior_ingress = cursor->last_ingress_sequence;
    std::uint64_t prior_tick =
        cursor->last_tick_stream_sequence;
    std::array<std::uint64_t, 4U> base_source_ends{
        1U, 1U, 1U, 1U};
    if (metadata.base_kind ==
        static_cast<std::uint32_t>(
            RealtimeInstrumentTickDeltaBaseKindV2::
                kCheckpoint)) {
        base_source_ends =
            metadata.base_checkpoint.generation
                .source_sequence_exclusive;
    }

    std::uint64_t first_ingress = 0U;
    std::uint64_t last_ingress = 0U;
    std::uint64_t first_tick = 0U;
    std::uint64_t last_tick = 0U;
    for (std::size_t index = 0U; index < record_count; ++index) {
        const RealtimeWireTickPayloadV2& payload = records[index];
        const auto& common = payload.common;
        std::uint8_t expected_slot = 0xffU;
        std::uint8_t expected_market = 0xffU;
        if (common.event_kind == 2U) {
            expected_slot = 1U;
            expected_market = 1U;
        } else if (
            common.event_kind == 4U ||
            common.event_kind == 5U) {
            expected_slot = 3U;
            expected_market = 2U;
        }
        if (!TickPayloadCanonical(payload) ||
            expected_slot == 0xffU ||
            common.instrument_id != target.instrument_id ||
            common.ordinal != target.ordinal ||
            common.source_slot != expected_slot ||
            common.market != expected_market ||
            common.source_stream_id !=
                target.generation
                    .source_stream_ids[expected_slot] ||
            common.trade_date != target.generation.trade_date ||
            common.ingress_sequence <
                metadata.ingress_sequence_begin_inclusive ||
            common.ingress_sequence >=
                metadata.ingress_sequence_end_exclusive ||
            common.tick_stream_sequence <
                metadata
                    .tick_stream_sequence_begin_inclusive ||
            common.tick_stream_sequence >=
                metadata.tick_stream_sequence_end_exclusive ||
            common.tick_stream_sequence >
                common.ingress_sequence ||
            common.source_sequence <
                base_source_ends[expected_slot] ||
            common.source_sequence >=
                target.generation
                    .source_sequence_exclusive[expected_slot] ||
            common.ingress_sequence <= prior_ingress ||
            common.tick_stream_sequence <= prior_tick ||
            common.source_sequence <=
                last_sources[expected_slot]) {
            return false;
        }
        prior_ingress = common.ingress_sequence;
        prior_tick = common.tick_stream_sequence;
        last_sources[expected_slot] = common.source_sequence;
        if (source_counts[expected_slot] ==
            std::numeric_limits<std::uint64_t>::max()) {
            return false;
        }
        ++source_counts[expected_slot];
        if (source_counts[expected_slot] >
            metadata
                .delta_tick_source_record_counts[expected_slot]) {
            return false;
        }
        if (index == 0U) {
            first_ingress = common.ingress_sequence;
            first_tick = common.tick_stream_sequence;
        }
        last_ingress = common.ingress_sequence;
        last_tick = common.tick_stream_sequence;
    }
    if (header.first_ingress_sequence != first_ingress ||
        header.last_ingress_sequence != last_ingress ||
        header.first_tick_stream_sequence != first_tick ||
        header.last_tick_stream_sequence != last_tick ||
        cursor->cumulative_record_count >
            std::numeric_limits<std::uint64_t>::max() -
                record_count ||
        cursor->cumulative_record_count + record_count >
            metadata.delta_tick_record_count) {
        return false;
    }
    cursor->cumulative_record_count += record_count;
    cursor->cumulative_source_record_counts = source_counts;
    cursor->last_source_sequences = last_sources;
    cursor->last_ingress_sequence = prior_ingress;
    cursor->last_tick_stream_sequence = prior_tick;
    return true;
}

bool ValidateMappedHistoryPage(
    l2flow_instrument_raw_event_history_cursor_v2* cursor,
    const void* mapping,
    std::size_t mapping_bytes,
    const RealtimeInstrumentTickDeltaReadResponseV2& response,
    const RealtimeWireTickPayloadV2** records) noexcept {
    if (cursor == nullptr || mapping == nullptr ||
        mapping == MAP_FAILED || records == nullptr ||
        mapping_bytes <
            sizeof(RealtimeInstrumentTickDeltaPageHeaderV2)) {
        return false;
    }
    const auto* const header =
        static_cast<const RealtimeInstrumentTickDeltaPageHeaderV2*>(
            mapping);
    const std::uint64_t payload_bytes =
        static_cast<std::uint64_t>(response.record_count) *
        sizeof(RealtimeWireTickPayloadV2);
    const std::uint64_t expected_mapping_bytes =
        l2flow::ipc::
            kRealtimeInstrumentTickDeltaPageHeaderBytesV2 +
        payload_bytes;
    if (header->magic !=
            l2flow::ipc::
                kRealtimeInstrumentTickDeltaPageMagicV2 ||
        header->abi_major !=
            l2flow::ipc::kRealtimeWireMajorV2 ||
        header->abi_minor !=
            l2flow::ipc::kRealtimeWireMinorV2 ||
        header->header_bytes !=
            l2flow::ipc::
                kRealtimeInstrumentTickDeltaPageHeaderBytesV2 ||
        header->endian_marker !=
            l2flow::ipc::kRealtimeLittleEndianMarkerV2 ||
        header->flags != 0U ||
        header->total_mapping_bytes != mapping_bytes ||
        header->total_mapping_bytes != expected_mapping_bytes ||
        header->page_index != response.page_index ||
        header->record_count != response.record_count ||
        header->tick_payload_bytes !=
            sizeof(RealtimeWireTickPayloadV2) ||
        header->tick_payloads_offset !=
            l2flow::ipc::
                kRealtimeInstrumentTickDeltaPageHeaderBytesV2 ||
        !DeltaMetadataEqual(header->metadata, cursor->metadata) ||
        !AllZero(header->reserved)) {
        return false;
    }
    const auto* const bytes =
        static_cast<const std::byte*>(mapping);
    *records =
        reinterpret_cast<const RealtimeWireTickPayloadV2*>(
            bytes + header->tick_payloads_offset);
    return ValidateHistoryPayloads(
        cursor,
        *records,
        response.record_count,
        *header);
}

int OpenHistoryCursor(
    l2flow_instrument_raw_event_history_session_v2* session,
    std::uint32_t instrument_id,
    std::uint32_t requested_page_records,
    const RealtimeInstrumentTickDeltaCheckpointV2* checkpoint,
    l2flow_instrument_raw_event_history_cursor_v2** output) noexcept {
    if (output == nullptr) {
        return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_INVALID_ARGUMENT_V2;
    }
    *output = nullptr;
    if (session == nullptr || session->socket_fd < 0 ||
        session->session_token == 0U) {
        return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_CLOSED_V2;
    }
    if (instrument_id == 0U || requested_page_records == 0U ||
        requested_page_records >
            kMaximumEventHistoryPageRecords ||
        session->active_cursor != nullptr) {
        return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_INVALID_ARGUMENT_V2;
    }
    const bool update = checkpoint != nullptr;
    if (update &&
        (!l2flow::ipc::
             RealtimeInstrumentTickDeltaCheckpointCanonicalV2(
                 *checkpoint) ||
         checkpoint->instrument_id != instrument_id)) {
        return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_INVALID_ARGUMENT_V2;
    }

    RealtimeInstrumentTickDeltaOpenInstrumentRequestV2 request{};
    request.magic = l2flow::ipc::kRealtimeControlMagicV2;
    request.protocol_major = l2flow::ipc::kRealtimeWireMajorV2;
    request.protocol_minor = l2flow::ipc::kRealtimeWireMinorV2;
    request.opcode = static_cast<std::uint16_t>(
        RealtimeInstrumentTickDeltaControlOpcodeV2::
            kOpenInstrumentDelta);
    request.message_bytes =
        static_cast<std::uint32_t>(sizeof(request));
    request.request_id = NextHistoryRequestId();
    request.instrument_id = instrument_id;
    request.requested_page_records = requested_page_records;
    request.base_kind = static_cast<std::uint32_t>(
        update
            ? RealtimeInstrumentTickDeltaBaseKindV2::kCheckpoint
            : RealtimeInstrumentTickDeltaBaseKindV2::kOrigin);
    request.delta_session_token = session->session_token;
    if (update) {
        request.base_checkpoint = *checkpoint;
    }
    if (!SendHistoryRequest(
            session->socket_fd, &request, sizeof(request))) {
        CloseHistoryTransport(session);
        return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_SYSTEM_ERROR_V2;
    }
    RealtimeInstrumentTickDeltaOpenInstrumentResponseV2 response{};
    int received_fd = -1;
    std::size_t fd_count = 0U;
    const int receive_error = ReceiveHistoryResponse(
        session->socket_fd,
        &response,
        &received_fd,
        &fd_count);
    if (receive_error !=
        L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_OK_V2) {
        CloseHistoryTransport(session);
        return receive_error;
    }
    if (received_fd >= 0) {
        static_cast<void>(::close(received_fd));
    }
    if (!HistoryResponsePrefixValid(response, request.request_id, 0U) ||
        fd_count != 0U) {
        CloseHistoryTransport(session);
        return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_PROTOCOL_ERROR_V2;
    }
    const int status = MapHistoryStatus(response.status);
    if (status != L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_OK_V2) {
        const bool canonical = ErrorResponseCanonical(response);
        CloseHistoryTransport(session);
        return canonical
                   ? status
                   : L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_PROTOCOL_ERROR_V2;
    }
    if (response.flags != 0U || response.reserved0 != 0U ||
        response.initial_read_token == 0U ||
        !l2flow::ipc::
             RealtimeInstrumentTickDeltaMetadataCanonicalV2(
                 response.metadata) ||
        response.metadata.target_checkpoint.instrument_id !=
            instrument_id ||
        !l2flow::ipc::
             realtime_instrument_tick_delta_wire_v2_detail::
                 EndpointsEqual(
                     response.metadata.target_checkpoint.generation,
                     session->target) ||
        response.metadata.base_kind != request.base_kind ||
        (update &&
         !l2flow::ipc::
              realtime_instrument_tick_delta_wire_v2_detail::
                  CheckpointsEqual(
                      response.metadata.base_checkpoint,
                      *checkpoint))) {
        CloseHistoryTransport(session);
        return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_PROTOCOL_ERROR_V2;
    }
    try {
        auto* const cursor =
            new l2flow_instrument_raw_event_history_cursor_v2{};
        cursor->session = session;
        cursor->metadata = response.metadata;
        cursor->requested_page_records = requested_page_records;
        cursor->read_token = response.initial_read_token;
        session->active_cursor = cursor;
        *output = cursor;
        return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_OK_V2;
    } catch (...) {
        CloseHistoryTransport(session);
        return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_RESOURCE_EXHAUSTED_V2;
    }
}

void FillHistoryPageView(
    const l2flow_instrument_raw_event_history_cursor_v2& cursor,
    const RealtimeWireTickPayloadV2* records,
    std::size_t record_count,
    std::uint64_t mapping_bytes,
    std::uint64_t page_index,
    std::uint64_t first_ingress,
    std::uint64_t last_ingress,
    std::uint64_t first_tick,
    std::uint64_t last_tick,
    bool eof,
    l2flow_instrument_raw_event_history_page_v2* output) noexcept {
    l2flow_instrument_raw_event_history_page_v2 page{};
    page.event_records = records;
    page.event_record_count = record_count;
    page.event_record_stride = sizeof(RealtimeWireTickPayloadV2);
    page.mapping_bytes = mapping_bytes;
    page.page_index = page_index;
    page.first_ingress_sequence = first_ingress;
    page.last_ingress_sequence = last_ingress;
    page.first_tick_stream_sequence = first_tick;
    page.last_tick_stream_sequence = last_tick;
    page.cumulative_record_count =
        cursor.cumulative_record_count;
    std::copy(
        cursor.cumulative_source_record_counts.begin(),
        cursor.cumulative_source_record_counts.end(),
        page.cumulative_source_record_counts);
    page.eof = eof ? 1U : 0U;
    *output = page;
}

}  // namespace

extern "C" int
l2flow_instrument_raw_event_history_session_open_v2(
    const char* absolute_control_socket_path,
    const l2flow_shm_session_info_v2* expected_session,
    std::uint64_t expected_generation,
    std::uint32_t timeout_ms,
    l2flow_instrument_raw_event_history_session_v2** output) {
    if (output == nullptr) {
        return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_INVALID_ARGUMENT_V2;
    }
    *output = nullptr;
    if (absolute_control_socket_path == nullptr ||
        expected_session == nullptr ||
        !SessionExpectationValid(*expected_session) ||
        !HistorySocketPathSyntaxValid(
            absolute_control_socket_path)) {
        return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_INVALID_ARGUMENT_V2;
    }
    if constexpr (std::endian::native != std::endian::little) {
        return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_ABI_MISMATCH_V2;
    }
    const int socket_fd =
        ConnectHistorySocket(
            absolute_control_socket_path, timeout_ms);
    if (socket_fd < 0) {
        return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_SYSTEM_ERROR_V2;
    }

    RealtimeInstrumentTickDeltaOpenSessionRequestV2 request{};
    request.magic = l2flow::ipc::kRealtimeControlMagicV2;
    request.protocol_major = l2flow::ipc::kRealtimeWireMajorV2;
    request.protocol_minor = l2flow::ipc::kRealtimeWireMinorV2;
    request.opcode = static_cast<std::uint16_t>(
        RealtimeInstrumentTickDeltaControlOpcodeV2::
            kOpenDeltaSession);
    request.message_bytes =
        static_cast<std::uint32_t>(sizeof(request));
    request.request_id = NextHistoryRequestId();
    request.expected_generation = expected_generation;
    if (!SendHistoryRequest(socket_fd, &request, sizeof(request))) {
        static_cast<void>(::close(socket_fd));
        return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_SYSTEM_ERROR_V2;
    }
    RealtimeInstrumentTickDeltaOpenSessionResponseV2 response{};
    int received_fd = -1;
    std::size_t fd_count = 0U;
    const int receive_error = ReceiveHistoryResponse(
        socket_fd,
        &response,
        &received_fd,
        &fd_count);
    if (receive_error !=
        L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_OK_V2) {
        static_cast<void>(::close(socket_fd));
        return receive_error;
    }
    if (received_fd >= 0) {
        static_cast<void>(::close(received_fd));
    }
    if (!HistoryResponsePrefixValid(response, request.request_id, 0U) ||
        fd_count != 0U) {
        static_cast<void>(::close(socket_fd));
        return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_PROTOCOL_ERROR_V2;
    }
    const int status = MapHistoryStatus(response.status);
    if (status != L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_OK_V2) {
        const bool canonical = ErrorResponseCanonical(response);
        static_cast<void>(::close(socket_fd));
        return canonical
                   ? status
                   : L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_PROTOCOL_ERROR_V2;
    }
    if (response.flags != 0U || response.reserved0 != 0U ||
        response.delta_session_token == 0U ||
        !l2flow::ipc::RealtimeGenerationEndpointCanonicalV2(
            response.target_generation) ||
        !EndpointMatchesSession(
            response.target_generation, *expected_session) ||
        (expected_generation != 0U &&
         response.target_generation.generation !=
             expected_generation)) {
        static_cast<void>(::close(socket_fd));
        return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_PROTOCOL_ERROR_V2;
    }
    try {
        auto* const session =
            new l2flow_instrument_raw_event_history_session_v2{};
        session->socket_fd = socket_fd;
        session->target = response.target_generation;
        session->session_token = response.delta_session_token;
        *output = session;
        return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_OK_V2;
    } catch (...) {
        static_cast<void>(::close(socket_fd));
        return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_RESOURCE_EXHAUSTED_V2;
    }
}

extern "C" void
l2flow_instrument_raw_event_history_session_close_v2(
    l2flow_instrument_raw_event_history_session_v2* session) {
    if (session == nullptr) {
        return;
    }
    if (session->active_cursor != nullptr) {
        ReleaseHistoryPage(session->active_cursor);
        session->active_cursor->failed = true;
        session->active_cursor->read_token = 0U;
        session->active_cursor->session = nullptr;
        session->active_cursor = nullptr;
    }
    CloseHistoryTransport(session);
    delete session;
}

extern "C" int
l2flow_instrument_raw_event_history_session_target_v2(
    const l2flow_instrument_raw_event_history_session_v2* session,
    l2flow_instrument_raw_event_history_endpoint_v2* output) {
    if (output == nullptr) {
        return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_INVALID_ARGUMENT_V2;
    }
    if (session == nullptr || session->socket_fd < 0) {
        return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_CLOSED_V2;
    }
    CopyObjectBytes(output, session->target);
    return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_OK_V2;
}

extern "C" int
l2flow_instrument_raw_event_history_open_full_v2(
    l2flow_instrument_raw_event_history_session_v2* session,
    std::uint32_t instrument_id,
    std::uint32_t requested_page_records,
    l2flow_instrument_raw_event_history_cursor_v2** output) {
    return OpenHistoryCursor(
        session,
        instrument_id,
        requested_page_records,
        nullptr,
        output);
}

extern "C" int
l2flow_instrument_raw_event_history_open_update_v2(
    l2flow_instrument_raw_event_history_session_v2* session,
    std::uint32_t instrument_id,
    std::uint32_t requested_page_records,
    const l2flow_instrument_raw_event_history_checkpoint_v2*
        base_checkpoint,
    l2flow_instrument_raw_event_history_cursor_v2** output) {
    if (base_checkpoint == nullptr) {
        if (output != nullptr) {
            *output = nullptr;
        }
        return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_INVALID_ARGUMENT_V2;
    }
    RealtimeInstrumentTickDeltaCheckpointV2 checkpoint{};
    CopyObjectBytes(&checkpoint, *base_checkpoint);
    return OpenHistoryCursor(
        session,
        instrument_id,
        requested_page_records,
        &checkpoint,
        output);
}

extern "C" void
l2flow_instrument_raw_event_history_cursor_close_v2(
    l2flow_instrument_raw_event_history_cursor_v2* cursor) {
    if (cursor == nullptr) {
        return;
    }
    ReleaseHistoryPage(cursor);
    if (cursor->session != nullptr) {
        if (cursor->session->active_cursor == cursor) {
            cursor->session->active_cursor = nullptr;
        }
        if (!cursor->eof) {
            CloseHistoryTransport(cursor->session);
        }
        cursor->session = nullptr;
    }
    delete cursor;
}

extern "C" int
l2flow_instrument_raw_event_history_cursor_metadata_v2(
    const l2flow_instrument_raw_event_history_cursor_v2* cursor,
    l2flow_instrument_raw_event_history_metadata_v2* output) {
    if (cursor == nullptr || output == nullptr) {
        return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_INVALID_ARGUMENT_V2;
    }
    if (cursor->failed) {
        return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_CLOSED_V2;
    }
    CopyObjectBytes(output, cursor->metadata);
    return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_OK_V2;
}

extern "C" int
l2flow_instrument_raw_event_history_cursor_read_v2(
    l2flow_instrument_raw_event_history_cursor_v2* cursor,
    l2flow_instrument_raw_event_history_page_v2* output) {
    if (cursor == nullptr || output == nullptr) {
        return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_INVALID_ARGUMENT_V2;
    }
    *output = {};
    ReleaseHistoryPage(cursor);
    if (cursor->failed) {
        return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_CLOSED_V2;
    }
    if (cursor->eof) {
        FillHistoryPageView(
            *cursor,
            nullptr,
            0U,
            0U,
            cursor->next_page_index,
            0U,
            0U,
            0U,
            0U,
            true,
            output);
        return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_OK_V2;
    }
    if (cursor->session == nullptr ||
        cursor->session->socket_fd < 0 ||
        cursor->session->active_cursor != cursor ||
        cursor->read_token == 0U) {
        FailHistoryCursor(cursor);
        return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_CLOSED_V2;
    }

    RealtimeInstrumentTickDeltaReadRequestV2 request{};
    request.magic = l2flow::ipc::kRealtimeControlMagicV2;
    request.protocol_major = l2flow::ipc::kRealtimeWireMajorV2;
    request.protocol_minor = l2flow::ipc::kRealtimeWireMinorV2;
    request.opcode = static_cast<std::uint16_t>(
        RealtimeInstrumentTickDeltaControlOpcodeV2::
            kReadInstrumentDelta);
    request.message_bytes =
        static_cast<std::uint32_t>(sizeof(request));
    request.request_id = NextHistoryRequestId();
    request.expected_page_index = cursor->next_page_index;
    request.read_token = cursor->read_token;
    if (!SendHistoryRequest(
            cursor->session->socket_fd,
            &request,
            sizeof(request))) {
        FailHistoryCursor(cursor);
        return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_SYSTEM_ERROR_V2;
    }
    RealtimeInstrumentTickDeltaReadResponseV2 response{};
    int page_fd = -1;
    std::size_t fd_count = 0U;
    const int receive_error = ReceiveHistoryResponse(
        cursor->session->socket_fd,
        &response,
        &page_fd,
        &fd_count);
    if (receive_error !=
        L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_OK_V2) {
        FailHistoryCursor(cursor);
        return receive_error;
    }
    if (!HistoryResponsePrefixValid(
            response,
            request.request_id,
            l2flow::ipc::
                kRealtimeInstrumentTickDeltaResponseTerminalV2)) {
        if (page_fd >= 0) {
            static_cast<void>(::close(page_fd));
        }
        FailHistoryCursor(cursor);
        return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_PROTOCOL_ERROR_V2;
    }
    const int status = MapHistoryStatus(response.status);
    if (status != L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_OK_V2) {
        const bool canonical =
            fd_count == 0U && ErrorResponseCanonical(response);
        if (page_fd >= 0) {
            static_cast<void>(::close(page_fd));
        }
        FailHistoryCursor(cursor);
        return canonical
                   ? status
                   : L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_PROTOCOL_ERROR_V2;
    }
    if (response.target_generation !=
            cursor->metadata.target_checkpoint.generation.generation ||
        response.page_index != cursor->next_page_index) {
        if (page_fd >= 0) {
            static_cast<void>(::close(page_fd));
        }
        FailHistoryCursor(cursor);
        return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_PROTOCOL_ERROR_V2;
    }

    const bool terminal =
        (response.flags &
         l2flow::ipc::
             kRealtimeInstrumentTickDeltaResponseTerminalV2) !=
        0U;
    if (terminal) {
        const bool reconciled =
            response.flags ==
                l2flow::ipc::
                    kRealtimeInstrumentTickDeltaResponseTerminalV2 &&
            response.record_count == 0U &&
            response.page_mapping_bytes == 0U &&
            response.next_read_token == 0U && fd_count == 0U &&
            page_fd < 0 &&
            cursor->cumulative_record_count ==
                cursor->metadata.delta_tick_record_count &&
            cursor->cumulative_source_record_counts ==
                cursor->metadata
                    .delta_tick_source_record_counts;
        if (!reconciled) {
            if (page_fd >= 0) {
                static_cast<void>(::close(page_fd));
            }
            FailHistoryCursor(cursor);
            return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_PROTOCOL_ERROR_V2;
        }
        cursor->eof = true;
        cursor->read_token = 0U;
        if (cursor->session != nullptr &&
            cursor->session->active_cursor == cursor) {
            cursor->session->active_cursor = nullptr;
        }
        cursor->session = nullptr;
        FillHistoryPageView(
            *cursor,
            nullptr,
            0U,
            0U,
            response.page_index,
            0U,
            0U,
            0U,
            0U,
            true,
            output);
        return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_OK_V2;
    }

    const std::uint64_t expected_mapping_bytes =
        l2flow::ipc::
            kRealtimeInstrumentTickDeltaPageHeaderBytesV2 +
        static_cast<std::uint64_t>(response.record_count) *
            sizeof(RealtimeWireTickPayloadV2);
    if (response.flags != 0U || response.record_count == 0U ||
        response.record_count > cursor->requested_page_records ||
        response.page_mapping_bytes != expected_mapping_bytes ||
        response.next_read_token == 0U ||
        response.next_read_token == cursor->read_token ||
        fd_count != 1U || page_fd < 0 ||
        !HistoryPageDescriptorValid(
            page_fd, response.page_mapping_bytes)) {
        if (page_fd >= 0) {
            static_cast<void>(::close(page_fd));
        }
        FailHistoryCursor(cursor);
        return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_PROTOCOL_ERROR_V2;
    }
    void* const mapping = ::mmap(
        nullptr,
        static_cast<std::size_t>(response.page_mapping_bytes),
        PROT_READ,
        MAP_SHARED,
        page_fd,
        0);
    static_cast<void>(::close(page_fd));
    if (mapping == MAP_FAILED) {
        FailHistoryCursor(cursor);
        return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_SYSTEM_ERROR_V2;
    }
    const RealtimeWireTickPayloadV2* records = nullptr;
    if (!ValidateMappedHistoryPage(
            cursor,
            mapping,
            static_cast<std::size_t>(
                response.page_mapping_bytes),
            response,
            &records)) {
        static_cast<void>(::munmap(
            mapping,
            static_cast<std::size_t>(
                response.page_mapping_bytes)));
        FailHistoryCursor(cursor);
        return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_PROTOCOL_ERROR_V2;
    }
    cursor->page_mapping = mapping;
    cursor->page_mapping_bytes =
        static_cast<std::size_t>(response.page_mapping_bytes);
    cursor->read_token = response.next_read_token;
    if (cursor->next_page_index ==
        std::numeric_limits<std::uint64_t>::max()) {
        FailHistoryCursor(cursor);
        return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_PROTOCOL_ERROR_V2;
    }
    ++cursor->next_page_index;
    const auto* const header =
        static_cast<const RealtimeInstrumentTickDeltaPageHeaderV2*>(
            mapping);
    FillHistoryPageView(
        *cursor,
        records,
        response.record_count,
        response.page_mapping_bytes,
        response.page_index,
        header->first_ingress_sequence,
        header->last_ingress_sequence,
        header->first_tick_stream_sequence,
        header->last_tick_stream_sequence,
        false,
        output);
    return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_OK_V2;
}

extern "C" int
l2flow_instrument_raw_event_history_cursor_verified_checkpoint_v2(
    const l2flow_instrument_raw_event_history_cursor_v2* cursor,
    l2flow_instrument_raw_event_history_checkpoint_v2* output) {
    if (cursor == nullptr || output == nullptr) {
        return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_INVALID_ARGUMENT_V2;
    }
    if (cursor->failed) {
        return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_CLOSED_V2;
    }
    if (!cursor->eof) {
        return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_NOT_READY_V2;
    }
    CopyObjectBytes(
        output, cursor->metadata.target_checkpoint);
    return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_OK_V2;
}

extern "C" const char*
l2flow_instrument_raw_event_history_error_name_v2(int error) {
    switch (error) {
        case L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_OK_V2:
            return "none";
        case L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_INVALID_ARGUMENT_V2:
            return "invalid_argument";
        case L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_SYSTEM_ERROR_V2:
            return "system_error";
        case L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_PROTOCOL_ERROR_V2:
            return "protocol_error";
        case L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_ABI_MISMATCH_V2:
            return "abi_mismatch";
        case L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_UNAVAILABLE_V2:
            return "unavailable";
        case L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_NOT_FOUND_V2:
            return "not_found";
        case L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_RESOURCE_EXHAUSTED_V2:
            return "resource_exhausted";
        case L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_CHECKPOINT_MISMATCH_V2:
            return "checkpoint_mismatch";
        case L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_NOT_READY_V2:
            return "not_ready";
        case L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_CLOSED_V2:
            return "closed";
        default:
            return "unknown";
    }
}
