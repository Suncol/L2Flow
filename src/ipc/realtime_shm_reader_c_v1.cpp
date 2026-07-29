#include "l2flow/ipc/realtime_shm_reader_c_v1.h"

#include "l2flow/ipc/realtime_instrument_tick_delta_wire_v2.h"
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
static_assert(
    sizeof(l2flow_instrument_tick_delta_page_result_v2) == 80U);

namespace {

using l2flow::ipc::RealtimeHeaderFlagV1;
using l2flow::ipc::RealtimeInstrumentTickDeltaBaseKindV2;
using l2flow::ipc::RealtimeInstrumentTickDeltaCheckpointV2;
using l2flow::ipc::RealtimeInstrumentTickDeltaGenerationEndpointV2;
using l2flow::ipc::RealtimeInstrumentTickDeltaMetadataV2;
using l2flow::ipc::RealtimeInstrumentTickDeltaPageHeaderV2;
using l2flow::ipc::RealtimeInstrumentTickDeltaPayloadProjectionV2;
using l2flow::ipc::RealtimeRegionKindV1;
using l2flow::ipc::RealtimeServerStateV1;
using l2flow::ipc::RealtimeWireHeaderV1;
using l2flow::ipc::RealtimeWireCommonRecordV1;
using l2flow::ipc::RealtimeWireDecimalV1;
using l2flow::ipc::RealtimeWireInstrumentV1;
using l2flow::ipc::RealtimeWireKLinePayloadV1;
using l2flow::ipc::RealtimeWireKLineSlotV1;
using l2flow::ipc::RealtimeWireKLineWindowV1;
using l2flow::ipc::RealtimeWireQuantityV1;
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

bool TickPayloadProjectionValid(
    const RealtimeWireTickPayloadV1& payload) noexcept {
    constexpr std::uint32_t known_flags =
        l2flow::ipc::kRealtimeWireTickRawTypeOmittedV1 |
        l2flow::ipc::kRealtimeWireTickRawTickFlagOmittedV1;
    if ((payload.projection_flags & ~known_flags) != 0U ||
        payload.raw_type_length > payload.raw_type.size() ||
        payload.raw_tick_flag_length >
            payload.raw_tick_flag.size() ||
        payload.reserved0 != 0U) {
        return false;
    }
    const bool raw_type_zero =
        !AnyNonzero(payload.raw_type);
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
         l2flow::ipc::kRealtimeWireTickRawTypeOmittedV1) != 0U;
    const bool raw_tick_flag_omitted =
        (payload.projection_flags &
         l2flow::ipc::
             kRealtimeWireTickRawTickFlagOmittedV1) != 0U;
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

bool WireDecimalValid(
    const RealtimeWireDecimalV1& value) noexcept {
    return value.valid <= 1U && value.is_null <= 1U &&
           !AnyNonzero(value.reserved);
}

bool WireQuantityValid(
    const RealtimeWireQuantityV1& value) noexcept {
    return value.valid <= 1U && value.is_null <= 1U &&
           !AnyNonzero(value.reserved);
}

bool TickPayloadCanonical(
    const RealtimeWireTickPayloadV1& payload) noexcept {
    return payload.common.record_schema_version == 1U &&
           payload.common.record_bytes ==
               sizeof(RealtimeWireTickPayloadV1) &&
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

bool AllZero(const void* data, std::size_t bytes) noexcept {
    if (data == nullptr) {
        return bytes == 0U;
    }
    const auto* const first =
        static_cast<const std::uint8_t*>(data);
    return std::all_of(
        first,
        first + bytes,
        [](std::uint8_t byte) noexcept { return byte == 0U; });
}

bool CheckedAdd(
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

bool GenerationEndpointCanonical(
    const RealtimeInstrumentTickDeltaGenerationEndpointV2&
        endpoint) noexcept {
    constexpr std::uint32_t known_flags =
        l2flow::ipc::kRealtimeInstrumentTickDeltaCoverageFromOpenV2 |
        l2flow::ipc::
            kRealtimeInstrumentTickDeltaTickRecordCoverageCompleteV2 |
        l2flow::ipc::kRealtimeInstrumentTickDeltaFieldCompleteV2;
    if (!AnyNonzero(endpoint.run_id) ||
        endpoint.session_epoch == 0U || endpoint.generation == 0U ||
        endpoint.trade_date == 0U ||
        endpoint.instrument_count == 0U ||
        endpoint.ingress_sequence_exclusive == 0U ||
        endpoint.tick_stream_sequence_exclusive == 0U ||
        endpoint.registry_version == 0U ||
        !AnyNonzero(endpoint.registry_sha256) ||
        !AnyNonzero(endpoint.input_identity_sha256) ||
        (endpoint.flags & ~known_flags) != 0U ||
        (endpoint.flags &
         l2flow::ipc::
             kRealtimeInstrumentTickDeltaTickRecordCoverageCompleteV2) ==
            0U ||
        (endpoint.flags &
         l2flow::ipc::kRealtimeInstrumentTickDeltaFieldCompleteV2) !=
            0U ||
        endpoint.payload_projection !=
            static_cast<std::uint32_t>(
                RealtimeInstrumentTickDeltaPayloadProjectionV2::
                    kCoreV1) ||
        AnyNonzero(endpoint.reserved)) {
        return false;
    }

    std::uint64_t ingress_prefix = 0U;
    for (std::size_t source = 0U;
         source < endpoint.source_stream_ids.size();
         ++source) {
        if (endpoint.source_stream_ids[source] == 0U ||
            endpoint.source_sequence_exclusive[source] == 0U) {
            return false;
        }
        for (std::size_t prior = 0U; prior < source; ++prior) {
            if (endpoint.source_stream_ids[source] ==
                endpoint.source_stream_ids[prior]) {
                return false;
            }
        }
        if (!CheckedAdd(
                ingress_prefix,
                endpoint.source_sequence_exclusive[source] - 1U,
                &ingress_prefix)) {
            return false;
        }
    }
    if (endpoint.ingress_sequence_exclusive - 1U !=
        ingress_prefix) {
        return false;
    }
    std::uint64_t tick_prefix =
        endpoint.source_sequence_exclusive[1U] - 1U;
    return CheckedAdd(
               tick_prefix,
               endpoint.source_sequence_exclusive[3U] - 1U,
               &tick_prefix) &&
           endpoint.tick_stream_sequence_exclusive - 1U ==
               tick_prefix;
}

bool CheckpointCanonical(
    const RealtimeInstrumentTickDeltaCheckpointV2&
        checkpoint) noexcept {
    if (!GenerationEndpointCanonical(checkpoint.generation) ||
        checkpoint.instrument_id == 0U ||
        checkpoint.registry_ordinal >=
            checkpoint.generation.instrument_count ||
        checkpoint.instrument_tick_source_record_counts[0U] != 0U ||
        checkpoint.instrument_tick_source_record_counts[2U] != 0U ||
        AnyNonzero(checkpoint.reserved)) {
        return false;
    }
    std::uint64_t total = 0U;
    for (std::size_t source = 0U;
         source <
         checkpoint.instrument_tick_source_record_counts.size();
         ++source) {
        const std::uint64_t count =
            checkpoint
                .instrument_tick_source_record_counts[source];
        if (!CheckedAdd(total, count, &total) ||
            count >
                checkpoint.generation
                        .source_sequence_exclusive[source] -
                    1U) {
            return false;
        }
    }
    return total == checkpoint.instrument_tick_record_count &&
           total <=
               checkpoint.generation
                       .tick_stream_sequence_exclusive -
                   1U;
}

bool SameStaticGeneration(
    const RealtimeInstrumentTickDeltaGenerationEndpointV2& left,
    const RealtimeInstrumentTickDeltaGenerationEndpointV2&
        right) noexcept {
    return left.run_id == right.run_id &&
           left.session_epoch == right.session_epoch &&
           left.trade_date == right.trade_date &&
           left.instrument_count == right.instrument_count &&
           left.registry_version == right.registry_version &&
           left.registry_sha256 == right.registry_sha256 &&
           left.source_stream_ids == right.source_stream_ids &&
           left.flags == right.flags &&
           left.payload_projection == right.payload_projection;
}

bool MetadataCanonical(
    const RealtimeInstrumentTickDeltaMetadataV2& metadata) noexcept {
    const bool origin =
        metadata.base_kind ==
        static_cast<std::uint32_t>(
            RealtimeInstrumentTickDeltaBaseKindV2::kOrigin);
    const bool checkpoint_base =
        metadata.base_kind ==
        static_cast<std::uint32_t>(
            RealtimeInstrumentTickDeltaBaseKindV2::kCheckpoint);
    if ((!origin && !checkpoint_base) ||
        metadata.selected_source_mask !=
            l2flow::ipc::
                kRealtimeInstrumentTickDeltaSourceMaskV2 ||
        !CheckpointCanonical(metadata.target_checkpoint) ||
        metadata.delta_tick_source_record_counts[0U] != 0U ||
        metadata.delta_tick_source_record_counts[2U] != 0U ||
        metadata.flags !=
            metadata.target_checkpoint.generation.flags ||
        metadata.payload_projection !=
            metadata.target_checkpoint.generation
                .payload_projection ||
        metadata.ingress_sequence_end_exclusive !=
            metadata.target_checkpoint.generation
                .ingress_sequence_exclusive ||
        metadata.tick_stream_sequence_end_exclusive !=
            metadata.target_checkpoint.generation
                .tick_stream_sequence_exclusive ||
        AnyNonzero(metadata.reserved)) {
        return false;
    }

    std::array<std::uint64_t, 4U> base_counts{};
    std::array<std::uint64_t, 4U> base_source_endpoints{
        1U, 1U, 1U, 1U};
    if (origin) {
        if (!AllZero(
                &metadata.base_checkpoint,
                sizeof(metadata.base_checkpoint)) ||
            metadata.ingress_sequence_begin_inclusive != 1U ||
            metadata.tick_stream_sequence_begin_inclusive != 1U) {
            return false;
        }
    } else {
        const auto& base = metadata.base_checkpoint;
        const auto& target = metadata.target_checkpoint;
        if (!CheckpointCanonical(base) ||
            !SameStaticGeneration(
                base.generation, target.generation) ||
            base.instrument_id != target.instrument_id ||
            base.registry_ordinal != target.registry_ordinal ||
            target.generation.generation <
                base.generation.generation ||
            target.generation.ingress_sequence_exclusive <
                base.generation.ingress_sequence_exclusive ||
            target.generation.tick_stream_sequence_exclusive <
                base.generation.tick_stream_sequence_exclusive ||
            target.generation.recv_monotonic_cut_ns <
                base.generation.recv_monotonic_cut_ns ||
            metadata.ingress_sequence_begin_inclusive !=
                base.generation.ingress_sequence_exclusive ||
            metadata.tick_stream_sequence_begin_inclusive !=
                base.generation.tick_stream_sequence_exclusive) {
            return false;
        }

        std::uint64_t source_delta_total = 0U;
        std::uint64_t tick_source_delta_total = 0U;
        for (std::size_t source = 0U;
             source < base_source_endpoints.size();
             ++source) {
            const std::uint64_t base_source =
                base.generation
                    .source_sequence_exclusive[source];
            const std::uint64_t target_source =
                target.generation
                    .source_sequence_exclusive[source];
            const std::uint64_t base_count =
                base.instrument_tick_source_record_counts[source];
            const std::uint64_t target_count =
                target
                    .instrument_tick_source_record_counts[source];
            if (target_source < base_source ||
                target_count < base_count ||
                !CheckedAdd(
                    source_delta_total,
                    target_source - base_source,
                    &source_delta_total)) {
                return false;
            }
            if ((source == 1U || source == 3U) &&
                !CheckedAdd(
                    tick_source_delta_total,
                    target_source - base_source,
                    &tick_source_delta_total)) {
                return false;
            }
            base_source_endpoints[source] = base_source;
            base_counts[source] = base_count;
        }
        const std::uint64_t ingress_delta =
            target.generation.ingress_sequence_exclusive -
            base.generation.ingress_sequence_exclusive;
        const std::uint64_t tick_delta =
            target.generation.tick_stream_sequence_exclusive -
            base.generation.tick_stream_sequence_exclusive;
        if (ingress_delta != source_delta_total ||
            tick_delta != tick_source_delta_total ||
            (target.generation.generation ==
                 base.generation.generation &&
             std::memcmp(&target, &base, sizeof(target)) != 0)) {
            return false;
        }
    }

    std::uint64_t delta_total = 0U;
    for (std::size_t source = 0U;
         source <
         metadata.delta_tick_source_record_counts.size();
         ++source) {
        const std::uint64_t target_count =
            metadata.target_checkpoint
                .instrument_tick_source_record_counts[source];
        const std::uint64_t target_source =
            metadata.target_checkpoint.generation
                .source_sequence_exclusive[source];
        if (target_count < base_counts[source] ||
            target_source < base_source_endpoints[source]) {
            return false;
        }
        const std::uint64_t local_delta =
            target_count - base_counts[source];
        if (metadata.delta_tick_source_record_counts[source] !=
                local_delta ||
            local_delta >
                target_source - base_source_endpoints[source] ||
            !CheckedAdd(delta_total, local_delta, &delta_total)) {
            return false;
        }
    }
    if (delta_total != metadata.delta_tick_record_count ||
        metadata.ingress_sequence_begin_inclusive >
            metadata.ingress_sequence_end_exclusive ||
        metadata.tick_stream_sequence_begin_inclusive >
            metadata.tick_stream_sequence_end_exclusive) {
        return false;
    }
    const std::uint64_t ingress_delta =
        metadata.ingress_sequence_end_exclusive -
        metadata.ingress_sequence_begin_inclusive;
    const std::uint64_t tick_delta =
        metadata.tick_stream_sequence_end_exclusive -
        metadata.tick_stream_sequence_begin_inclusive;
    return delta_total <= tick_delta && tick_delta <= ingress_delta;
}

bool RangesOverlap(
    const void* left,
    std::size_t left_bytes,
    const void* right,
    std::size_t right_bytes) noexcept {
    if (left == nullptr || right == nullptr ||
        left_bytes == 0U || right_bytes == 0U) {
        return false;
    }
    const std::uintptr_t left_begin =
        reinterpret_cast<std::uintptr_t>(left);
    const std::uintptr_t right_begin =
        reinterpret_cast<std::uintptr_t>(right);
    if (left_bytes >
            std::numeric_limits<std::uintptr_t>::max() -
                left_begin ||
        right_bytes >
            std::numeric_limits<std::uintptr_t>::max() -
                right_begin) {
        return true;
    }
    const std::uintptr_t left_end = left_begin + left_bytes;
    const std::uintptr_t right_end = right_begin + right_bytes;
    return left_begin < right_end && right_begin < left_end;
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
    // Registry rows must remain in instrument-ID/slot ordinal order. This
    // reader-owned permutation supplies the independent exact-key order.
    std::uint32_t* instrument_key_ordinals = nullptr;
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

int CompareOpaqueBytes(
    const std::byte* left,
    std::size_t left_size,
    const std::byte* right,
    std::size_t right_size) noexcept {
    const std::size_t common_size = std::min(left_size, right_size);
    for (std::size_t index = 0U; index < common_size; ++index) {
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
    const l2flow_shm_reader_v1& reader,
    std::size_t ordinal,
    std::uint8_t market,
    const std::uint8_t* security_id_source,
    std::size_t security_id_source_length,
    const std::uint8_t* security_id,
    std::size_t security_id_length) noexcept {
    const RealtimeWireInstrumentV1& row =
        reader.instruments[ordinal];
    if (row.market < market) {
        return -1;
    }
    if (row.market > market) {
        return 1;
    }
    const int source_order = CompareOpaqueBytes(
        reader.key_blob + row.security_id_source_offset,
        row.security_id_source_length,
        reinterpret_cast<const std::byte*>(security_id_source),
        security_id_source_length);
    if (source_order != 0) {
        return source_order;
    }
    return CompareOpaqueBytes(
        reader.key_blob + row.security_id_offset,
        row.security_id_length,
        reinterpret_cast<const std::byte*>(security_id),
        security_id_length);
}

int CompareInstrumentRows(
    const l2flow_shm_reader_v1& reader,
    std::size_t left_ordinal,
    std::size_t right_ordinal) noexcept {
    const RealtimeWireInstrumentV1& right =
        reader.instruments[right_ordinal];
    return CompareInstrumentRowToKey(
        reader,
        left_ordinal,
        right.market,
        reinterpret_cast<const std::uint8_t*>(
            reader.key_blob + right.security_id_source_offset),
        right.security_id_source_length,
        reinterpret_cast<const std::uint8_t*>(
            reader.key_blob + right.security_id_offset),
        right.security_id_length);
}

enum class InstrumentKeyIndexResult : std::uint8_t {
    kOk = 0U,
    kResourceExhausted,
    kDuplicateKey,
};

InstrumentKeyIndexResult BuildInstrumentKeyIndex(
    l2flow_shm_reader_v1* reader) noexcept {
    if (reader == nullptr || reader->header == nullptr ||
        reader->instruments == nullptr || reader->key_blob == nullptr ||
        reader->instrument_key_ordinals != nullptr) {
        return InstrumentKeyIndexResult::kResourceExhausted;
    }
    const std::size_t count = reader->header->instrument_count;
    if (count == 0U ||
        count >
            std::numeric_limits<std::size_t>::max() /
                sizeof(std::uint32_t)) {
        return InstrumentKeyIndexResult::kResourceExhausted;
    }
    try {
        reader->instrument_key_ordinals =
            new (std::nothrow) std::uint32_t[count];
        if (reader->instrument_key_ordinals == nullptr) {
            return InstrumentKeyIndexResult::kResourceExhausted;
        }
        for (std::size_t index = 0U; index < count; ++index) {
            reader->instrument_key_ordinals[index] =
                static_cast<std::uint32_t>(index);
        }
        std::sort(
            reader->instrument_key_ordinals,
            reader->instrument_key_ordinals + count,
            [reader](std::uint32_t left, std::uint32_t right) noexcept {
                return CompareInstrumentRows(*reader, left, right) < 0;
            });
    } catch (...) {
        return InstrumentKeyIndexResult::kResourceExhausted;
    }
    for (std::size_t index = 1U; index < count; ++index) {
        if (CompareInstrumentRows(
                *reader,
                reader->instrument_key_ordinals[index - 1U],
                reader->instrument_key_ordinals[index]) >= 0) {
            return InstrumentKeyIndexResult::kDuplicateKey;
        }
    }
    return InstrumentKeyIndexResult::kOk;
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

std::size_t FindInstrumentOrdinalByKey(
    const l2flow_shm_reader_v1& reader,
    std::uint8_t market,
    const std::uint8_t* security_id_source,
    std::size_t security_id_source_length,
    const std::uint8_t* security_id,
    std::size_t security_id_length) noexcept {
    std::size_t begin = 0U;
    std::size_t end = reader.header->instrument_count;
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
    if (begin >= reader.header->instrument_count) {
        return std::numeric_limits<std::size_t>::max();
    }
    const std::size_t ordinal = reader.instrument_key_ordinals[begin];
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
            (row.market != 1U && row.market != 2U) ||
            row.quantity_unit > 5U || row.security_type > 7U ||
            row.asset_scope > 2U ||
            row.security_id_length == 0U ||
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
    const InstrumentKeyIndexResult key_index_result =
        BuildInstrumentKeyIndex(reader);
    if (key_index_result != InstrumentKeyIndexResult::kOk) {
        l2flow_shm_reader_close_v1(reader);
        return key_index_result ==
                       InstrumentKeyIndexResult::kDuplicateKey
                   ? L2FLOW_SHM_READER_LAYOUT_INVALID_V1
                   : L2FLOW_SHM_READER_SYSTEM_ERROR_V1;
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
    delete[] reader->instrument_key_ordinals;
    reader->instrument_key_ordinals = nullptr;
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

extern "C" int l2flow_shm_reader_resolve_instruments_v1(
    const l2flow_shm_reader_v1* reader,
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
          instrument_ids == nullptr || item_statuses == nullptr)) ||
        (count != 0U && reader->instrument_key_ordinals == nullptr)) {
        return L2FLOW_SHM_READER_INVALID_ARGUMENT_V1;
    }
    // Validate every pointer/length pair before mutating any output.
    for (std::size_t index = 0U; index < count; ++index) {
        if ((security_id_source_lengths[index] != 0U &&
             security_id_sources[index] == nullptr) ||
            (security_id_lengths[index] != 0U &&
             security_ids[index] == nullptr)) {
            return L2FLOW_SHM_READER_INVALID_ARGUMENT_V1;
        }
    }
    for (std::size_t index = 0U; index < count; ++index) {
        instrument_ids[index] = 0U;
        if (markets[index] != 1U && markets[index] != 2U) {
            item_statuses[index] =
                L2FLOW_INSTRUMENT_LOOKUP_INVALID_MARKET_V1;
            continue;
        }
        if (security_id_lengths[index] == 0U) {
            item_statuses[index] =
                L2FLOW_INSTRUMENT_LOOKUP_EMPTY_SECURITY_ID_V1;
            continue;
        }
        const std::size_t ordinal = FindInstrumentOrdinalByKey(
            *reader,
            markets[index],
            security_id_sources[index],
            security_id_source_lengths[index],
            security_ids[index],
            security_id_lengths[index]);
        if (ordinal == std::numeric_limits<std::size_t>::max()) {
            item_statuses[index] =
                L2FLOW_INSTRUMENT_LOOKUP_UNKNOWN_V1;
            continue;
        }
        const std::uint32_t instrument_id =
            reader->instruments[ordinal].instrument_id;
        if (instrument_id == 0U) {
            return L2FLOW_SHM_READER_LAYOUT_INVALID_V1;
        }
        instrument_ids[index] = instrument_id;
        item_statuses[index] =
            L2FLOW_INSTRUMENT_LOOKUP_FOUND_V1;
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
                   TickPayloadProjectionValid(payload) &&
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
            !TickPayloadProjectionValid(payload) ||
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

extern "C" int
l2flow_shm_reader_instrument_tick_delta_page_v2(
    int page_fd,
    std::uint64_t expected_mapping_bytes,
    std::uint32_t expected_record_count,
    std::uint64_t expected_page_index,
    const void* expected_metadata,
    std::size_t expected_metadata_bytes,
    std::uint64_t prior_ingress_sequence,
    std::uint64_t prior_tick_stream_sequence,
    const std::uint64_t* prior_source_sequences,
    void* tick_payloads_output,
    std::size_t tick_payloads_output_bytes,
    l2flow_instrument_tick_delta_page_result_v2* result) noexcept {
    constexpr std::uint64_t header_bytes =
        sizeof(RealtimeInstrumentTickDeltaPageHeaderV2);
    constexpr std::uint64_t tick_bytes =
        sizeof(RealtimeWireTickPayloadV1);
    const std::uint64_t payload_bytes =
        static_cast<std::uint64_t>(expected_record_count) *
        tick_bytes;
    std::uint64_t canonical_mapping_bytes = 0U;
    if (page_fd < 0 || expected_record_count == 0U ||
        expected_metadata == nullptr ||
        expected_metadata_bytes !=
            sizeof(RealtimeInstrumentTickDeltaMetadataV2) ||
        prior_source_sequences == nullptr ||
        tick_payloads_output == nullptr || result == nullptr ||
        !CheckedAdd(
            header_bytes,
            payload_bytes,
            &canonical_mapping_bytes) ||
        expected_mapping_bytes >
            static_cast<std::uint64_t>(
                std::numeric_limits<std::size_t>::max()) ||
        expected_mapping_bytes >
            static_cast<std::uint64_t>(
                std::numeric_limits<off_t>::max()) ||
        payload_bytes >
            static_cast<std::uint64_t>(
                std::numeric_limits<std::size_t>::max())) {
        return L2FLOW_SHM_READER_INVALID_ARGUMENT_V1;
    }
    if (expected_mapping_bytes != canonical_mapping_bytes) {
        return L2FLOW_SHM_READER_LAYOUT_INVALID_V1;
    }
    const std::size_t required_output_bytes =
        static_cast<std::size_t>(payload_bytes);
    if (tick_payloads_output_bytes < required_output_bytes) {
        return L2FLOW_SHM_READER_BUFFER_TOO_SMALL_V1;
    }
    if (RangesOverlap(
            tick_payloads_output,
            required_output_bytes,
            expected_metadata,
            expected_metadata_bytes) ||
        RangesOverlap(
            tick_payloads_output,
            required_output_bytes,
            prior_source_sequences,
            4U * sizeof(std::uint64_t)) ||
        RangesOverlap(
            tick_payloads_output,
            required_output_bytes,
            result,
            sizeof(*result)) ||
        RangesOverlap(
            result,
            sizeof(*result),
            expected_metadata,
            expected_metadata_bytes) ||
        RangesOverlap(
            result,
            sizeof(*result),
            prior_source_sequences,
            4U * sizeof(std::uint64_t))) {
        return L2FLOW_SHM_READER_INVALID_ARGUMENT_V1;
    }
    if constexpr (std::endian::native != std::endian::little) {
        return L2FLOW_SHM_READER_ABI_MISMATCH_V1;
    }

    RealtimeInstrumentTickDeltaMetadataV2 expected_metadata_value{};
    std::memcpy(
        &expected_metadata_value,
        expected_metadata,
        sizeof(expected_metadata_value));
    if (!MetadataCanonical(expected_metadata_value) ||
        expected_record_count >
            expected_metadata_value.delta_tick_record_count) {
        return L2FLOW_SHM_READER_LAYOUT_INVALID_V1;
    }
    std::array<std::uint64_t, 4U> prior_sources{};
    std::memcpy(
        prior_sources.data(),
        prior_source_sequences,
        sizeof(prior_sources));

    const int descriptor_flags = ::fcntl(page_fd, F_GETFL);
    const int seals = ::fcntl(page_fd, F_GET_SEALS);
    if (descriptor_flags < 0 || seals < 0) {
        return L2FLOW_SHM_READER_SYSTEM_ERROR_V1;
    }
    constexpr int required_seals =
        F_SEAL_WRITE | F_SEAL_GROW | F_SEAL_SHRINK | F_SEAL_SEAL;
    if ((descriptor_flags & O_ACCMODE) != O_RDONLY ||
        (seals & required_seals) != required_seals) {
        return L2FLOW_SHM_READER_LAYOUT_INVALID_V1;
    }
    struct stat descriptor_stat {};
    if (::fstat(page_fd, &descriptor_stat) != 0) {
        return L2FLOW_SHM_READER_SYSTEM_ERROR_V1;
    }
    if (!S_ISREG(descriptor_stat.st_mode) ||
        descriptor_stat.st_size < 0 ||
        static_cast<std::uint64_t>(descriptor_stat.st_size) !=
            expected_mapping_bytes) {
        return L2FLOW_SHM_READER_LAYOUT_INVALID_V1;
    }

    void* const mapping = ::mmap(
        nullptr,
        static_cast<std::size_t>(expected_mapping_bytes),
        PROT_READ,
        MAP_SHARED,
        page_fd,
        0);
    if (mapping == MAP_FAILED) {
        return L2FLOW_SHM_READER_SYSTEM_ERROR_V1;
    }
    const auto* const page =
        static_cast<const RealtimeInstrumentTickDeltaPageHeaderV2*>(
            mapping);
    int validation_error = L2FLOW_SHM_READER_OK_V1;
    if (page->magic !=
        l2flow::ipc::kRealtimeInstrumentTickDeltaPageMagicV2) {
        validation_error = L2FLOW_SHM_READER_LAYOUT_INVALID_V1;
    } else if (
        page->abi_major != l2flow::ipc::kRealtimeWireMajorV1 ||
        page->abi_minor != l2flow::ipc::kRealtimeWireMinorV1 ||
        page->endian_marker !=
            l2flow::ipc::kRealtimeLittleEndianMarkerV1) {
        validation_error = L2FLOW_SHM_READER_ABI_MISMATCH_V1;
    } else if (
        page->header_bytes != header_bytes || page->flags != 0U ||
        page->total_mapping_bytes != expected_mapping_bytes ||
        page->page_index != expected_page_index ||
        page->record_count != expected_record_count ||
        page->tick_payload_bytes != tick_bytes ||
        page->tick_payloads_offset != header_bytes ||
        std::memcmp(
            &page->metadata,
            &expected_metadata_value,
            sizeof(expected_metadata_value)) != 0 ||
        AnyNonzero(page->reserved)) {
        validation_error = L2FLOW_SHM_READER_LAYOUT_INVALID_V1;
    }

    l2flow_instrument_tick_delta_page_result_v2 local_result{};
    local_result.last_ingress_sequence =
        prior_ingress_sequence;
    local_result.last_tick_stream_sequence =
        prior_tick_stream_sequence;
    std::copy(
        prior_sources.begin(),
        prior_sources.end(),
        local_result.last_source_sequences);
    const auto* const ticks =
        reinterpret_cast<const RealtimeWireTickPayloadV1*>(
            static_cast<const std::byte*>(mapping) +
            static_cast<std::size_t>(header_bytes));
    if (validation_error == L2FLOW_SHM_READER_OK_V1) {
        const auto& metadata = expected_metadata_value;
        const auto& target = metadata.target_checkpoint;
        const bool origin =
            metadata.base_kind ==
            static_cast<std::uint32_t>(
                RealtimeInstrumentTickDeltaBaseKindV2::kOrigin);
        for (std::size_t index = 0U;
             index < expected_record_count;
             ++index) {
            const RealtimeWireTickPayloadV1& tick = ticks[index];
            const RealtimeWireCommonRecordV1& common = tick.common;
            const std::size_t source = common.source_slot;
            const bool source_kind_valid =
                (source == 1U && common.event_kind == 2U) ||
                (source == 3U &&
                 (common.event_kind == 4U ||
                  common.event_kind == 5U));
            const std::uint64_t source_begin =
                !source_kind_valid
                    ? 0U
                    : (origin
                           ? 1U
                           : metadata.base_checkpoint.generation
                                 .source_sequence_exclusive[source]);
            if (!TickPayloadCanonical(tick) ||
                !source_kind_valid ||
                common.instrument_id != target.instrument_id ||
                common.registry_ordinal !=
                    target.registry_ordinal ||
                common.trade_date != target.generation.trade_date ||
                common.source_stream_id !=
                    target.generation.source_stream_ids[source] ||
                common.source_sequence < source_begin ||
                common.source_sequence >=
                    target.generation
                        .source_sequence_exclusive[source] ||
                common.source_sequence <=
                    local_result.last_source_sequences[source] ||
                common.ingress_sequence <
                    metadata
                        .ingress_sequence_begin_inclusive ||
                common.ingress_sequence >=
                    metadata.ingress_sequence_end_exclusive ||
                common.ingress_sequence <=
                    local_result.last_ingress_sequence ||
                common.tick_stream_sequence <
                    metadata
                        .tick_stream_sequence_begin_inclusive ||
                common.tick_stream_sequence >=
                    metadata
                        .tick_stream_sequence_end_exclusive ||
                common.tick_stream_sequence <=
                    local_result.last_tick_stream_sequence ||
                common.tick_stream_sequence >
                    common.ingress_sequence) {
                validation_error =
                    L2FLOW_SHM_READER_LAYOUT_INVALID_V1;
                break;
            }
            ++local_result.source_counts[source];
            local_result.last_source_sequences[source] =
                common.source_sequence;
            local_result.last_ingress_sequence =
                common.ingress_sequence;
            local_result.last_tick_stream_sequence =
                common.tick_stream_sequence;
        }
    }
    if (validation_error == L2FLOW_SHM_READER_OK_V1) {
        const auto& first = ticks[0U].common;
        const auto& last =
            ticks[expected_record_count - 1U].common;
        if (page->first_ingress_sequence !=
                first.ingress_sequence ||
            page->last_ingress_sequence !=
                last.ingress_sequence ||
            page->first_tick_stream_sequence !=
                first.tick_stream_sequence ||
            page->last_tick_stream_sequence !=
                last.tick_stream_sequence ||
            local_result.source_counts[0U] != 0U ||
            local_result.source_counts[2U] != 0U ||
            local_result.source_counts[1U] >
                expected_metadata_value
                    .delta_tick_source_record_counts[1U] ||
            local_result.source_counts[3U] >
                expected_metadata_value
                    .delta_tick_source_record_counts[3U]) {
            validation_error =
                L2FLOW_SHM_READER_LAYOUT_INVALID_V1;
        }
    }
    if (validation_error != L2FLOW_SHM_READER_OK_V1) {
        static_cast<void>(::munmap(
            mapping,
            static_cast<std::size_t>(expected_mapping_bytes)));
        return validation_error;
    }

    std::memmove(
        tick_payloads_output,
        ticks,
        required_output_bytes);
    *result = local_result;
    static_cast<void>(::munmap(
        mapping,
        static_cast<std::size_t>(expected_mapping_bytes)));
    return L2FLOW_SHM_READER_OK_V1;
}
