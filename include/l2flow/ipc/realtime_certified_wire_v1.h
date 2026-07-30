#pragma once

#include "l2flow/ipc/realtime_wire_v2.h"

#include <array>
#include <atomic>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <type_traits>

namespace l2flow::ipc {

// This is an independent sidecar ABI.  It neither extends nor consumes any
// reserved byte, flag, or region in Realtime Wire V2.
inline constexpr std::array<std::uint8_t, 8U>
    kRealtimeCertifiedShmMagicV1{
        'L', '2', 'F', 'C', 'E', 'R', 'T', '1'};
inline constexpr std::uint16_t kRealtimeCertifiedWireMajorV1 = 1U;
inline constexpr std::uint16_t kRealtimeCertifiedWireMinorV1 = 0U;
inline constexpr std::uint32_t
    kRealtimeCertifiedLittleEndianMarkerV1 = 0x01020304U;
inline constexpr std::size_t kRealtimeCertifiedHeaderBytesV1 = 4096U;
inline constexpr std::size_t
    kRealtimeCertifiedChannelStateBytesV1 = 128U;
inline constexpr std::size_t
    kRealtimeCertifiedTickSlotBytesV1 = 512U;
inline constexpr std::uint32_t
    kRealtimeCertifiedRegionAlignmentV1 = 64U;

// Aggregate state is a nonblocking read result.  A native-sequence anomaly
// never changes the health or readability of the independent FAST mapping.
// Frozen states retain the last correct certified prefix.
enum class RealtimeCertifiedStateV1 : std::uint32_t {
    kDisabled = 1U,
    kNoData = 2U,
    kContiguous = 3U,
    kGapOpen = 4U,
    kCatchingUp = 5U,
    kFrozenConflict = 6U,
    kFrozenResource = 7U,
    kStopped = 8U,
};

namespace realtime_certified_wire_v1_detail {

template <typename T, std::size_t Size>
[[nodiscard]] constexpr bool AllZero(
    const std::array<T, Size>& values) noexcept {
    for (const T value : values) {
        if (value != T{}) {
            return false;
        }
    }
    return true;
}

template <typename T, std::size_t Size>
[[nodiscard]] constexpr bool AnyNonzero(
    const std::array<T, Size>& values) noexcept {
    return !AllZero(values);
}

[[nodiscard]] constexpr bool CheckedAdd(
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

[[nodiscard]] constexpr bool CheckedMultiply(
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

[[nodiscard]] constexpr bool ValidTradeDate(
    std::uint32_t value) noexcept {
    const std::uint32_t year = value / 10'000U;
    const std::uint32_t month = (value / 100U) % 100U;
    const std::uint32_t day = value % 100U;
    if (year < 1992U || year > 2200U || month == 0U ||
        month > 12U || day == 0U) {
        return false;
    }
    constexpr std::array<std::uint32_t, 12U> days_by_month{
        31U, 28U, 31U, 30U, 31U, 30U,
        31U, 31U, 30U, 31U, 30U, 31U};
    std::uint32_t maximum_day = days_by_month[month - 1U];
    const bool leap =
        (year % 4U == 0U && year % 100U != 0U) ||
        year % 400U == 0U;
    if (month == 2U && leap) {
        maximum_day = 29U;
    }
    return day <= maximum_day;
}

[[nodiscard]] constexpr bool WireDecimalValid(
    const RealtimeWireDecimalV2& value) noexcept {
    return value.valid <= 1U && value.is_null <= 1U &&
           AllZero(value.reserved);
}

[[nodiscard]] constexpr bool WireQuantityValid(
    const RealtimeWireQuantityV2& value) noexcept {
    return value.valid <= 1U && value.is_null <= 1U &&
           AllZero(value.reserved);
}

[[nodiscard]] constexpr bool CommonTickIdentityValid(
    const RealtimeWireCommonRecordV2& common) noexcept {
    if (common.record_schema_version != 2U ||
        common.record_bytes != sizeof(RealtimeWireTickPayloadV2) ||
        common.instrument_id == 0U ||
        common.ordinal ==
            std::numeric_limits<std::uint32_t>::max() ||
        common.instrument_id != common.ordinal + 1U ||
        common.source_sequence == 0U ||
        common.source_sequence ==
            std::numeric_limits<std::uint64_t>::max() ||
        common.ingress_sequence == 0U ||
        common.ingress_sequence ==
            std::numeric_limits<std::uint64_t>::max() ||
        common.tick_stream_sequence == 0U ||
        common.tick_stream_sequence ==
            std::numeric_limits<std::uint64_t>::max() ||
        common.tick_stream_sequence > common.ingress_sequence ||
        common.source_stream_id == 0U ||
        !ValidTradeDate(common.trade_date) ||
        common.reserved0 != 0U || !AllZero(common.reserved) ||
        common.quantity_unit > 5U || common.security_type > 7U ||
        common.asset_scope > 2U) {
        return false;
    }

    // Frozen MarketEventKindV1/MarketV1/source-slot encodings from the
    // embedded Wire V2 payload.  Snapshots are never certified here.
    if (common.event_kind == 2U) {
        return common.source_slot == 1U && common.market == 1U;
    }
    return (common.event_kind == 4U ||
            common.event_kind == 5U) &&
           common.source_slot == 3U && common.market == 2U;
}

[[nodiscard]] constexpr bool RawProjectionValid(
    const RealtimeWireTickPayloadV2& payload) noexcept {
    constexpr std::uint32_t known_flags =
        kRealtimeWireTickRawTypeOmittedV2 |
        kRealtimeWireTickRawTickFlagOmittedV2;
    if ((payload.projection_flags & ~known_flags) != 0U ||
        payload.raw_type_length > payload.raw_type.size() ||
        payload.raw_tick_flag_length >
            payload.raw_tick_flag.size() ||
        payload.reserved0 != 0U) {
        return false;
    }

    const bool raw_type_zero = AllZero(payload.raw_type);
    const bool raw_tick_flag_zero =
        AllZero(payload.raw_tick_flag);
    if (payload.common.event_kind != 2U) {
        return payload.projection_flags == 0U &&
               payload.raw_type_length == 0U &&
               payload.raw_tick_flag_length == 0U &&
               raw_type_zero && raw_tick_flag_zero;
    }

    const bool raw_type_omitted =
        (payload.projection_flags &
         kRealtimeWireTickRawTypeOmittedV2) != 0U;
    const bool raw_tick_flag_omitted =
        (payload.projection_flags &
         kRealtimeWireTickRawTickFlagOmittedV2) != 0U;
    if ((raw_type_omitted &&
         (payload.raw_type_length != 0U || !raw_type_zero)) ||
        (raw_tick_flag_omitted &&
         (payload.raw_tick_flag_length != 0U ||
          !raw_tick_flag_zero))) {
        return false;
    }
    for (std::size_t index = payload.raw_type_length;
         index < payload.raw_type.size();
         ++index) {
        if (payload.raw_type[index] != 0U) {
            return false;
        }
    }
    for (std::size_t index = payload.raw_tick_flag_length;
         index < payload.raw_tick_flag.size();
         ++index) {
        if (payload.raw_tick_flag[index] != 0U) {
            return false;
        }
    }
    return true;
}

}  // namespace realtime_certified_wire_v1_detail

// Every structure below is fixed-width and little-endian.  Shared memory
// contains no C++ enum, bool, pointer, size_t, string, or atomic object.
//
// The layout is deliberately canonical and contiguous:
//
//   header -> latest[latest_capacity]
//          -> certified_ring[certified_ring_capacity]
//          -> channel_state[channel_state_capacity]
//
// latest is indexed by the embedded V2 payload's catalog ordinal.  The ring
// is dense in canonical_apply_sequence, which is a system publication order,
// not an exchange-provided cross-channel total order.
struct alignas(4096) RealtimeCertifiedHeaderV1 final {
    std::array<std::uint8_t, 8U> magic{};
    std::uint16_t abi_major = 0U;
    std::uint16_t abi_minor = 0U;
    std::uint32_t header_bytes = 0U;
    std::uint32_t endian_marker = 0U;
    std::uint32_t flags = 0U;
    std::uint64_t total_mapping_bytes = 0U;

    // Identity of the FAST V2 session to which this sidecar is bound.
    std::array<std::uint8_t, 16U> run_id{};
    std::uint64_t session_epoch = 0U;
    std::uint32_t trade_date = 0U;
    std::uint32_t reserved_identity = 0U;

    // Immutable layout.
    std::uint64_t latest_offset = 0U;
    std::uint64_t certified_ring_offset = 0U;
    std::uint64_t channel_state_offset = 0U;
    std::uint32_t latest_capacity = 0U;
    std::uint32_t certified_ring_capacity = 0U;
    std::uint32_t channel_state_capacity = 0U;
    std::uint32_t latest_stride =
        kRealtimeCertifiedTickSlotBytesV1;
    std::uint32_t certified_ring_stride =
        kRealtimeCertifiedTickSlotBytesV1;
    std::uint32_t channel_state_stride =
        kRealtimeCertifiedChannelStateBytesV1;
    std::uint32_t region_alignment =
        kRealtimeCertifiedRegionAlignmentV1;
    std::uint32_t reserved_layout0 = 0U;
    std::array<std::uint8_t, 8U> reserved_layout{};

    // Writers update this coherent aggregate block under status_publish_tag:
    // stable even -> odd -> atomic_ref field stores -> next stable even.
    // Every covered scalar is accessed through atomic_ref.
    std::uint64_t status_publish_tag = 0U;
    std::uint64_t heartbeat_monotonic_ns = 0U;
    std::uint64_t canonical_apply_frontier = 0U;
    std::uint64_t correction_epoch = 0U;
    // Includes every validated native message observed by the tracker,
    // including exact duplicates and messages represented by skip markers.
    std::uint64_t observed_native_message_count = 0U;
    // Number of payloads published into the dense certified ring.  It is
    // exactly canonical_apply_frontier because sequence zero is reserved.
    std::uint64_t certified_tick_count = 0U;
    std::uint64_t exact_duplicate_message_count = 0U;
    std::uint64_t gap_opened_count = 0U;
    std::uint64_t gap_recovered_count = 0U;
    std::uint64_t conflicting_duplicate_count = 0U;
    std::uint64_t resource_exhaustion_count = 0U;
    std::uint64_t pending_token_count = 0U;
    std::uint32_t aggregate_state = 0U;
    std::uint32_t channel_state_count = 0U;
    std::uint32_t gap_open_channel_count = 0U;
    std::uint32_t catching_up_channel_count = 0U;
    // Counts only representable rows.  A source-wide freeze caused before a
    // domain row can be installed is expressed by aggregate_state/counters
    // while this value may remain zero.
    std::uint32_t frozen_channel_count = 0U;
    std::uint32_t reserved_status = 0U;

    std::array<std::uint8_t, 3848U> reserved{};
};
static_assert(sizeof(RealtimeCertifiedHeaderV1) == 4096U);
static_assert(alignof(RealtimeCertifiedHeaderV1) == 4096U);
static_assert(std::is_standard_layout_v<RealtimeCertifiedHeaderV1>);
static_assert(
    std::is_trivially_copyable_v<RealtimeCertifiedHeaderV1>);
static_assert(offsetof(RealtimeCertifiedHeaderV1, magic) == 0U);
static_assert(offsetof(RealtimeCertifiedHeaderV1, abi_major) == 8U);
static_assert(offsetof(RealtimeCertifiedHeaderV1, abi_minor) == 10U);
static_assert(
    offsetof(RealtimeCertifiedHeaderV1, header_bytes) == 12U);
static_assert(
    offsetof(RealtimeCertifiedHeaderV1, endian_marker) == 16U);
static_assert(offsetof(RealtimeCertifiedHeaderV1, flags) == 20U);
static_assert(
    offsetof(RealtimeCertifiedHeaderV1, total_mapping_bytes) ==
    24U);
static_assert(offsetof(RealtimeCertifiedHeaderV1, run_id) == 32U);
static_assert(
    offsetof(RealtimeCertifiedHeaderV1, session_epoch) == 48U);
static_assert(
    offsetof(RealtimeCertifiedHeaderV1, trade_date) == 56U);
static_assert(
    offsetof(RealtimeCertifiedHeaderV1, reserved_identity) == 60U);
static_assert(
    offsetof(RealtimeCertifiedHeaderV1, latest_offset) == 64U);
static_assert(
    offsetof(
        RealtimeCertifiedHeaderV1,
        certified_ring_offset) == 72U);
static_assert(
    offsetof(
        RealtimeCertifiedHeaderV1,
        channel_state_offset) == 80U);
static_assert(
    offsetof(RealtimeCertifiedHeaderV1, latest_capacity) == 88U);
static_assert(
    offsetof(
        RealtimeCertifiedHeaderV1,
        certified_ring_capacity) == 92U);
static_assert(
    offsetof(
        RealtimeCertifiedHeaderV1,
        channel_state_capacity) == 96U);
static_assert(
    offsetof(RealtimeCertifiedHeaderV1, latest_stride) == 100U);
static_assert(
    offsetof(
        RealtimeCertifiedHeaderV1,
        certified_ring_stride) == 104U);
static_assert(
    offsetof(
        RealtimeCertifiedHeaderV1,
        channel_state_stride) == 108U);
static_assert(
    offsetof(RealtimeCertifiedHeaderV1, region_alignment) ==
    112U);
static_assert(
    offsetof(RealtimeCertifiedHeaderV1, reserved_layout0) ==
    116U);
static_assert(
    offsetof(RealtimeCertifiedHeaderV1, reserved_layout) == 120U);
static_assert(
    offsetof(
        RealtimeCertifiedHeaderV1,
        status_publish_tag) == 128U);
static_assert(
    offsetof(
        RealtimeCertifiedHeaderV1,
        heartbeat_monotonic_ns) == 136U);
static_assert(
    offsetof(
        RealtimeCertifiedHeaderV1,
        canonical_apply_frontier) == 144U);
static_assert(
    offsetof(RealtimeCertifiedHeaderV1, correction_epoch) == 152U);
static_assert(
    offsetof(
        RealtimeCertifiedHeaderV1,
        observed_native_message_count) == 160U);
static_assert(
    offsetof(
        RealtimeCertifiedHeaderV1,
        certified_tick_count) == 168U);
static_assert(
    offsetof(
        RealtimeCertifiedHeaderV1,
        exact_duplicate_message_count) == 176U);
static_assert(
    offsetof(
        RealtimeCertifiedHeaderV1,
        gap_opened_count) == 184U);
static_assert(
    offsetof(
        RealtimeCertifiedHeaderV1,
        gap_recovered_count) == 192U);
static_assert(
    offsetof(
        RealtimeCertifiedHeaderV1,
        conflicting_duplicate_count) == 200U);
static_assert(
    offsetof(
        RealtimeCertifiedHeaderV1,
        resource_exhaustion_count) == 208U);
static_assert(
    offsetof(
        RealtimeCertifiedHeaderV1,
        pending_token_count) == 216U);
static_assert(
    offsetof(RealtimeCertifiedHeaderV1, aggregate_state) == 224U);
static_assert(
    offsetof(
        RealtimeCertifiedHeaderV1,
        channel_state_count) == 228U);
static_assert(
    offsetof(
        RealtimeCertifiedHeaderV1,
        gap_open_channel_count) == 232U);
static_assert(
    offsetof(
        RealtimeCertifiedHeaderV1,
        catching_up_channel_count) == 236U);
static_assert(
    offsetof(
        RealtimeCertifiedHeaderV1,
        frozen_channel_count) == 240U);
static_assert(
    offsetof(RealtimeCertifiedHeaderV1, reserved_status) == 244U);
static_assert(
    offsetof(RealtimeCertifiedHeaderV1, reserved) == 248U);

// One native continuity domain.  Native order is only per
// (trade_date, market, channel, feed_epoch).  No field below claims an
// exchange-defined order across channels.
struct alignas(64) RealtimeCertifiedChannelStateV1 final {
    // This is both the row seqcount and its aggregate commit epoch. A writer
    // prepares every changed row at the header's next stable
    // status_publish_tag, then commits that same tag in the header. Readers
    // must not expose a row whose publish_tag is newer than the stable header.
    std::uint64_t publish_tag = 0U;
    std::uint64_t feed_epoch = 0U;
    std::int64_t origin_sequence = 0;
    std::int64_t observed_contiguous_frontier = 0;
    std::int64_t certified_published_frontier = 0;
    std::int64_t highest_observed_sequence = 0;
    std::uint64_t canonical_apply_frontier = 0U;
    std::uint64_t observed_native_message_count = 0U;
    std::uint64_t certified_tick_count = 0U;
    std::uint64_t exact_duplicate_message_count = 0U;
    std::uint64_t pending_token_count = 0U;
    std::uint64_t gap_opened_count = 0U;
    std::uint64_t gap_recovered_count = 0U;
    std::uint32_t state = 0U;
    std::uint32_t trade_date = 0U;
    std::int64_t channel = 0;
    std::uint8_t market = 0U;
    std::array<std::uint8_t, 7U> reserved{};
};
static_assert(
    sizeof(RealtimeCertifiedChannelStateV1) ==
    kRealtimeCertifiedChannelStateBytesV1);
static_assert(alignof(RealtimeCertifiedChannelStateV1) == 64U);
static_assert(
    std::is_standard_layout_v<RealtimeCertifiedChannelStateV1>);
static_assert(
    std::is_trivially_copyable_v<
        RealtimeCertifiedChannelStateV1>);
static_assert(
    offsetof(
        RealtimeCertifiedChannelStateV1,
        publish_tag) == 0U);
static_assert(
    offsetof(
        RealtimeCertifiedChannelStateV1,
        feed_epoch) == 8U);
static_assert(
    offsetof(
        RealtimeCertifiedChannelStateV1,
        origin_sequence) == 16U);
static_assert(
    offsetof(
        RealtimeCertifiedChannelStateV1,
        observed_contiguous_frontier) == 24U);
static_assert(
    offsetof(
        RealtimeCertifiedChannelStateV1,
        certified_published_frontier) == 32U);
static_assert(
    offsetof(
        RealtimeCertifiedChannelStateV1,
        highest_observed_sequence) == 40U);
static_assert(
    offsetof(
        RealtimeCertifiedChannelStateV1,
        canonical_apply_frontier) == 48U);
static_assert(
    offsetof(
        RealtimeCertifiedChannelStateV1,
        observed_native_message_count) == 56U);
static_assert(
    offsetof(
        RealtimeCertifiedChannelStateV1,
        certified_tick_count) == 64U);
static_assert(
    offsetof(
        RealtimeCertifiedChannelStateV1,
        exact_duplicate_message_count) == 72U);
static_assert(
    offsetof(
        RealtimeCertifiedChannelStateV1,
        pending_token_count) == 80U);
static_assert(
    offsetof(
        RealtimeCertifiedChannelStateV1,
        gap_opened_count) == 88U);
static_assert(
    offsetof(
        RealtimeCertifiedChannelStateV1,
        gap_recovered_count) == 96U);
static_assert(
    offsetof(RealtimeCertifiedChannelStateV1, state) == 104U);
static_assert(
    offsetof(RealtimeCertifiedChannelStateV1, trade_date) == 108U);
static_assert(
    offsetof(RealtimeCertifiedChannelStateV1, channel) == 112U);
static_assert(
    offsetof(RealtimeCertifiedChannelStateV1, market) == 120U);

// The embedded V2 payload is the sole native domain truth for a published
// tick: market is payload.common.market, channel is payload.channel, and
// native sequence is payload.native_event_sequence.  Duplicating those
// values in this envelope would create two authorities.
struct RealtimeCertifiedTickEnvelopeV1 final {
    std::uint64_t canonical_apply_sequence = 0U;
    std::uint64_t correction_epoch = 0U;
    std::uint64_t feed_epoch = 0U;
    std::uint64_t certified_monotonic_ns = 0U;
    RealtimeWireTickPayloadV2 payload{};
};
static_assert(sizeof(RealtimeCertifiedTickEnvelopeV1) == 368U);
static_assert(
    alignof(RealtimeCertifiedTickEnvelopeV1) ==
    alignof(std::uint64_t));
static_assert(
    std::is_standard_layout_v<RealtimeCertifiedTickEnvelopeV1>);
static_assert(
    std::is_trivially_copyable_v<
        RealtimeCertifiedTickEnvelopeV1>);
static_assert(
    std::has_unique_object_representations_v<
        RealtimeCertifiedTickEnvelopeV1>,
    "the certified envelope must not contain implicit padding");
static_assert(
    offsetof(
        RealtimeCertifiedTickEnvelopeV1,
        canonical_apply_sequence) == 0U);
static_assert(
    offsetof(
        RealtimeCertifiedTickEnvelopeV1,
        correction_epoch) == 8U);
static_assert(
    offsetof(RealtimeCertifiedTickEnvelopeV1, feed_epoch) == 16U);
static_assert(
    offsetof(
        RealtimeCertifiedTickEnvelopeV1,
        certified_monotonic_ns) == 24U);
static_assert(
    offsetof(RealtimeCertifiedTickEnvelopeV1, payload) == 32U);

inline constexpr std::size_t
    kRealtimeCertifiedTickEnvelopeWordsV1 =
        sizeof(RealtimeCertifiedTickEnvelopeV1) /
        sizeof(std::uint64_t);
inline constexpr std::size_t
    kRealtimeCertifiedTickSlotPayloadWordsV1 =
        (kRealtimeCertifiedTickSlotBytesV1 - 64U) /
        sizeof(std::uint64_t);
static_assert(
    sizeof(RealtimeCertifiedTickEnvelopeV1) %
        sizeof(std::uint64_t) ==
    0U);
static_assert(
    kRealtimeCertifiedTickEnvelopeWordsV1 == 46U);
static_assert(
    kRealtimeCertifiedTickSlotPayloadWordsV1 == 56U);

// Both latest and dense-ring regions use this slot.  The first 46 payload
// words are the bit representation of RealtimeCertifiedTickEnvelopeV1; the
// remaining words must be zero.  Word storage lets a writer and reader use
// atomic_ref<uint64_t> for every concurrently accessed payload word, as a
// seqcount alone would not make non-atomic C++ accesses data-race-free.
// publish_tag is a seqcount, not canonical_apply_sequence.  Zero means never
// published.
struct alignas(64) RealtimeCertifiedTickSlotV1 final {
    std::uint64_t publish_tag = 0U;
    std::array<std::uint8_t, 56U> reserved0{};
    std::array<
        std::uint64_t,
        kRealtimeCertifiedTickSlotPayloadWordsV1>
        payload_words{};
};
static_assert(
    sizeof(RealtimeCertifiedTickSlotV1) ==
    kRealtimeCertifiedTickSlotBytesV1);
static_assert(alignof(RealtimeCertifiedTickSlotV1) == 64U);
static_assert(
    std::is_standard_layout_v<RealtimeCertifiedTickSlotV1>);
static_assert(
    std::is_trivially_copyable_v<RealtimeCertifiedTickSlotV1>);
static_assert(
    offsetof(RealtimeCertifiedTickSlotV1, publish_tag) == 0U);
static_assert(
    offsetof(RealtimeCertifiedTickSlotV1, reserved0) == 8U);
static_assert(
    offsetof(RealtimeCertifiedTickSlotV1, payload_words) == 64U);

[[nodiscard]] constexpr bool RealtimeCertifiedStateValidV1(
    std::uint32_t value) noexcept {
    return value >=
               static_cast<std::uint32_t>(
                   RealtimeCertifiedStateV1::kDisabled) &&
           value <=
               static_cast<std::uint32_t>(
                   RealtimeCertifiedStateV1::kStopped);
}

[[nodiscard]] constexpr bool RealtimeCertifiedPublishTagStableV1(
    std::uint64_t publish_tag) noexcept {
    return (publish_tag & 1U) == 0U;
}

[[nodiscard]] constexpr bool RealtimeCertifiedLayoutValidV1(
    const RealtimeCertifiedHeaderV1& header) noexcept {
    using realtime_certified_wire_v1_detail::CheckedAdd;
    using realtime_certified_wire_v1_detail::CheckedMultiply;

    if (header.latest_capacity == 0U ||
        header.certified_ring_capacity == 0U ||
        header.channel_state_capacity == 0U ||
        header.latest_stride !=
            kRealtimeCertifiedTickSlotBytesV1 ||
        header.certified_ring_stride !=
            kRealtimeCertifiedTickSlotBytesV1 ||
        header.channel_state_stride !=
            kRealtimeCertifiedChannelStateBytesV1 ||
        header.region_alignment !=
            kRealtimeCertifiedRegionAlignmentV1 ||
        header.reserved_layout0 != 0U ||
        !realtime_certified_wire_v1_detail::AllZero(
            header.reserved_layout) ||
        header.latest_offset != kRealtimeCertifiedHeaderBytesV1) {
        return false;
    }

    std::uint64_t region_bytes = 0U;
    std::uint64_t region_end = 0U;
    if (!CheckedMultiply(
            header.latest_capacity,
            header.latest_stride,
            &region_bytes) ||
        !CheckedAdd(
            header.latest_offset, region_bytes, &region_end) ||
        header.certified_ring_offset != region_end ||
        !CheckedMultiply(
            header.certified_ring_capacity,
            header.certified_ring_stride,
            &region_bytes) ||
        !CheckedAdd(
            header.certified_ring_offset,
            region_bytes,
            &region_end) ||
        header.channel_state_offset != region_end ||
        !CheckedMultiply(
            header.channel_state_capacity,
            header.channel_state_stride,
            &region_bytes) ||
        !CheckedAdd(
            header.channel_state_offset,
            region_bytes,
            &region_end) ||
        header.total_mapping_bytes != region_end) {
        return false;
    }
    return header.latest_offset %
                   header.region_alignment ==
               0U &&
           header.certified_ring_offset %
                   header.region_alignment ==
               0U &&
           header.channel_state_offset %
                   header.region_alignment ==
               0U &&
           header.total_mapping_bytes %
                   header.region_alignment ==
               0U;
}

[[nodiscard]] constexpr bool
RealtimeCertifiedHeaderCountersValidV1(
    const RealtimeCertifiedHeaderV1& header) noexcept {
    std::uint64_t accounted_messages = 0U;
    if (!realtime_certified_wire_v1_detail::CheckedAdd(
            header.certified_tick_count,
            header.exact_duplicate_message_count,
            &accounted_messages) ||
        accounted_messages >
            header.observed_native_message_count ||
        header.certified_tick_count !=
            header.canonical_apply_frontier ||
        header.gap_recovered_count >
            header.gap_opened_count ||
        header.channel_state_count >
            header.channel_state_capacity) {
        return false;
    }

    std::uint64_t exceptional_channels =
        header.gap_open_channel_count;
    if (!realtime_certified_wire_v1_detail::CheckedAdd(
            exceptional_channels,
            header.catching_up_channel_count,
            &exceptional_channels) ||
        !realtime_certified_wire_v1_detail::CheckedAdd(
            exceptional_channels,
            header.frozen_channel_count,
            &exceptional_channels)) {
        return false;
    }
    return exceptional_channels <= header.channel_state_count;
}

[[nodiscard]] constexpr bool
RealtimeCertifiedHeaderStateValidV1(
    const RealtimeCertifiedHeaderV1& header) noexcept {
    const auto state =
        static_cast<RealtimeCertifiedStateV1>(
            header.aggregate_state);
    const bool empty_activity =
        header.canonical_apply_frontier == 0U &&
        header.observed_native_message_count == 0U &&
        header.certified_tick_count == 0U &&
        header.exact_duplicate_message_count == 0U &&
        header.gap_opened_count == 0U &&
        header.gap_recovered_count == 0U &&
        header.conflicting_duplicate_count == 0U &&
        header.resource_exhaustion_count == 0U &&
        header.pending_token_count == 0U &&
        header.gap_open_channel_count == 0U &&
        header.catching_up_channel_count == 0U &&
        header.frozen_channel_count == 0U;

    switch (state) {
        case RealtimeCertifiedStateV1::kDisabled:
            return header.correction_epoch == 0U &&
                   empty_activity &&
                   header.channel_state_count == 0U;
        case RealtimeCertifiedStateV1::kNoData:
            return header.correction_epoch != 0U &&
                   empty_activity;
        case RealtimeCertifiedStateV1::kContiguous:
            return header.correction_epoch != 0U &&
                   header.channel_state_count != 0U &&
                   header.gap_open_channel_count == 0U &&
                   header.catching_up_channel_count == 0U &&
                   header.frozen_channel_count == 0U &&
                   header.pending_token_count == 0U;
        case RealtimeCertifiedStateV1::kGapOpen:
            return header.correction_epoch != 0U &&
                   header.gap_open_channel_count != 0U &&
                   header.frozen_channel_count == 0U &&
                   header.pending_token_count != 0U;
        case RealtimeCertifiedStateV1::kCatchingUp:
            return header.correction_epoch != 0U &&
                   header.gap_open_channel_count == 0U &&
                   header.catching_up_channel_count != 0U &&
                   header.frozen_channel_count == 0U &&
                   header.pending_token_count != 0U;
        case RealtimeCertifiedStateV1::kFrozenConflict:
            return header.correction_epoch != 0U &&
                   header.conflicting_duplicate_count != 0U;
        case RealtimeCertifiedStateV1::kFrozenResource:
            return header.correction_epoch != 0U &&
                   header.resource_exhaustion_count != 0U;
        case RealtimeCertifiedStateV1::kStopped:
            return true;
    }
    return false;
}

[[nodiscard]] constexpr bool RealtimeCertifiedHeaderCanonicalV1(
    const RealtimeCertifiedHeaderV1& header) noexcept {
    return header.magic == kRealtimeCertifiedShmMagicV1 &&
           header.abi_major == kRealtimeCertifiedWireMajorV1 &&
           header.abi_minor == kRealtimeCertifiedWireMinorV1 &&
           header.header_bytes ==
               kRealtimeCertifiedHeaderBytesV1 &&
           header.endian_marker ==
               kRealtimeCertifiedLittleEndianMarkerV1 &&
           header.flags == 0U &&
           realtime_certified_wire_v1_detail::AnyNonzero(
               header.run_id) &&
           header.session_epoch != 0U &&
           realtime_certified_wire_v1_detail::ValidTradeDate(
               header.trade_date) &&
           header.reserved_identity == 0U &&
           header.status_publish_tag != 0U &&
           RealtimeCertifiedPublishTagStableV1(
               header.status_publish_tag) &&
           header.heartbeat_monotonic_ns != 0U &&
           RealtimeCertifiedStateValidV1(
               header.aggregate_state) &&
           header.reserved_status == 0U &&
           realtime_certified_wire_v1_detail::AllZero(
               header.reserved) &&
           RealtimeCertifiedLayoutValidV1(header) &&
           RealtimeCertifiedHeaderCountersValidV1(header) &&
           RealtimeCertifiedHeaderStateValidV1(header);
}

[[nodiscard]] constexpr bool
RealtimeCertifiedChannelStateCountersValidV1(
    const RealtimeCertifiedChannelStateV1& row) noexcept {
    std::uint64_t accounted_messages = 0U;
    return realtime_certified_wire_v1_detail::CheckedAdd(
               row.certified_tick_count,
               row.exact_duplicate_message_count,
               &accounted_messages) &&
           accounted_messages <=
               row.observed_native_message_count &&
           row.gap_recovered_count <= row.gap_opened_count &&
           row.canonical_apply_frontier >=
               row.certified_tick_count &&
           ((row.certified_tick_count == 0U) ==
            (row.canonical_apply_frontier == 0U));
}

[[nodiscard]] constexpr bool
RealtimeCertifiedChannelStateCanonicalV1(
    const RealtimeCertifiedChannelStateV1& row) noexcept {
    if (row.publish_tag == 0U ||
        !RealtimeCertifiedPublishTagStableV1(row.publish_tag) ||
        !RealtimeCertifiedStateValidV1(row.state) ||
        row.feed_epoch == 0U ||
        !realtime_certified_wire_v1_detail::ValidTradeDate(
            row.trade_date) ||
        (row.market != 1U && row.market != 2U) ||
        (row.market == 1U ? row.channel <= 0
                          : row.channel < 0) ||
        !realtime_certified_wire_v1_detail::AllZero(
            row.reserved) ||
        !RealtimeCertifiedChannelStateCountersValidV1(row)) {
        return false;
    }

    const auto state =
        static_cast<RealtimeCertifiedStateV1>(row.state);
    if (state == RealtimeCertifiedStateV1::kDisabled) {
        // Disabled is aggregate-only; a disabled table has no published rows.
        return false;
    }
    if (state == RealtimeCertifiedStateV1::kNoData) {
        return row.origin_sequence == 0 &&
               row.observed_contiguous_frontier == 0 &&
               row.certified_published_frontier == 0 &&
               row.highest_observed_sequence == 0 &&
               row.canonical_apply_frontier == 0U &&
               row.observed_native_message_count == 0U &&
               row.certified_tick_count == 0U &&
               row.exact_duplicate_message_count == 0U &&
               row.pending_token_count == 0U &&
               row.gap_opened_count == 0U &&
               row.gap_recovered_count == 0U;
    }

    if (row.origin_sequence <= 0 ||
        row.observed_native_message_count == 0U) {
        return false;
    }
    const std::int64_t before_origin = row.origin_sequence - 1;
    if (row.observed_contiguous_frontier < before_origin ||
        row.certified_published_frontier < before_origin ||
        row.certified_published_frontier >
            row.observed_contiguous_frontier ||
        row.highest_observed_sequence <
            row.observed_contiguous_frontier) {
        return false;
    }

    switch (state) {
        case RealtimeCertifiedStateV1::kContiguous:
            return row.observed_contiguous_frontier ==
                       row.highest_observed_sequence &&
                   row.certified_published_frontier ==
                       row.observed_contiguous_frontier &&
                   row.pending_token_count == 0U;
        case RealtimeCertifiedStateV1::kGapOpen:
            return row.observed_contiguous_frontier <
                       row.highest_observed_sequence &&
                   row.pending_token_count != 0U;
        case RealtimeCertifiedStateV1::kCatchingUp:
            return row.observed_contiguous_frontier ==
                       row.highest_observed_sequence &&
                   row.certified_published_frontier <
                       row.observed_contiguous_frontier &&
                   row.pending_token_count != 0U;
        case RealtimeCertifiedStateV1::kFrozenConflict:
        case RealtimeCertifiedStateV1::kFrozenResource:
        case RealtimeCertifiedStateV1::kStopped:
            return true;
        case RealtimeCertifiedStateV1::kDisabled:
        case RealtimeCertifiedStateV1::kNoData:
            return false;
    }
    return false;
}

[[nodiscard]] constexpr bool RealtimeCertifiedTickPayloadCanonicalV1(
    const RealtimeWireTickPayloadV2& payload) noexcept {
    constexpr std::uint32_t known_validity_bits =
        (std::uint32_t{1U} << 12U) - 1U;
    return realtime_certified_wire_v1_detail::
               CommonTickIdentityValid(payload.common) &&
           (payload.common.market == 1U ? payload.channel > 0
                                        : payload.channel >= 0) &&
           payload.native_event_sequence > 0 &&
           (payload.validity_bitmap & ~known_validity_bits) == 0U &&
           payload.action <= 4U && payload.side <= 4U &&
           payload.order_type <= 3U && payload.aggressor <= 3U &&
           payload.phase <= 7U &&
           realtime_certified_wire_v1_detail::WireDecimalValid(
               payload.price) &&
           realtime_certified_wire_v1_detail::WireQuantityValid(
               payload.quantity) &&
           realtime_certified_wire_v1_detail::WireDecimalValid(
               payload.trade_amount) &&
           realtime_certified_wire_v1_detail::WireQuantityValid(
               payload.matched_quantity) &&
           realtime_certified_wire_v1_detail::RawProjectionValid(
               payload);
}

[[nodiscard]] constexpr bool
RealtimeCertifiedTickEnvelopeCanonicalV1(
    const RealtimeCertifiedTickEnvelopeV1& envelope) noexcept {
    return envelope.canonical_apply_sequence != 0U &&
           envelope.correction_epoch != 0U &&
           envelope.feed_epoch != 0U &&
           envelope.certified_monotonic_ns != 0U &&
           RealtimeCertifiedTickPayloadCanonicalV1(
               envelope.payload);
}

// These slot helpers validate a process-local stable copy.  A live mapped
// slot must first be copied by loading publish_tag/payload_words through
// atomic_ref and accepting equal, nonzero, even begin/end tags.
[[nodiscard]] constexpr bool
RealtimeCertifiedTickSlotDecodeV1(
    const RealtimeCertifiedTickSlotV1& stable_slot,
    RealtimeCertifiedTickEnvelopeV1* output) noexcept {
    if (output == nullptr || stable_slot.publish_tag == 0U ||
        !RealtimeCertifiedPublishTagStableV1(
            stable_slot.publish_tag) ||
        !realtime_certified_wire_v1_detail::AllZero(
            stable_slot.reserved0)) {
        return false;
    }

    std::array<
        std::uint64_t,
        kRealtimeCertifiedTickEnvelopeWordsV1>
        envelope_words{};
    for (std::size_t index = 0U;
         index < envelope_words.size();
         ++index) {
        envelope_words[index] = stable_slot.payload_words[index];
    }
    for (std::size_t index = envelope_words.size();
         index < stable_slot.payload_words.size();
         ++index) {
        if (stable_slot.payload_words[index] != 0U) {
            return false;
        }
    }

    const auto envelope =
        std::bit_cast<RealtimeCertifiedTickEnvelopeV1>(
            envelope_words);
    if (!RealtimeCertifiedTickEnvelopeCanonicalV1(envelope)) {
        return false;
    }
    *output = envelope;
    return true;
}

[[nodiscard]] constexpr bool
RealtimeCertifiedTickSlotPublishedV1(
    const RealtimeCertifiedTickSlotV1& stable_slot) noexcept {
    RealtimeCertifiedTickEnvelopeV1 envelope{};
    return RealtimeCertifiedTickSlotDecodeV1(
        stable_slot, &envelope);
}

[[nodiscard]] constexpr bool
RealtimeCertifiedTickMatchesHeaderV1(
    const RealtimeCertifiedTickEnvelopeV1& envelope,
    const RealtimeCertifiedHeaderV1& header) noexcept {
    return RealtimeCertifiedTickEnvelopeCanonicalV1(envelope) &&
           RealtimeCertifiedHeaderCanonicalV1(header) &&
           envelope.payload.common.trade_date ==
               header.trade_date &&
           envelope.payload.common.ordinal <
               header.latest_capacity &&
           envelope.canonical_apply_sequence <=
               header.canonical_apply_frontier &&
           envelope.correction_epoch <=
               header.correction_epoch;
}

[[nodiscard]] constexpr bool
RealtimeCertifiedTickMatchesChannelStateV1(
    const RealtimeCertifiedTickEnvelopeV1& envelope,
    const RealtimeCertifiedChannelStateV1& row) noexcept {
    return RealtimeCertifiedTickEnvelopeCanonicalV1(envelope) &&
           RealtimeCertifiedChannelStateCanonicalV1(row) &&
           envelope.payload.common.trade_date == row.trade_date &&
           envelope.payload.common.market == row.market &&
           envelope.payload.channel == row.channel &&
           envelope.feed_epoch == row.feed_epoch &&
           envelope.payload.native_event_sequence >=
               row.origin_sequence &&
           envelope.payload.native_event_sequence <=
               row.certified_published_frontier &&
           envelope.canonical_apply_sequence <=
               row.canonical_apply_frontier;
}

// The dense ring is 1-based: canonical sequence N occupies
// (N - 1) % capacity.  On failure output is left untouched.
[[nodiscard]] constexpr bool RealtimeCertifiedRingSlotIndexV1(
    std::uint64_t canonical_apply_sequence,
    std::uint32_t capacity,
    std::uint32_t* output) noexcept {
    if (output == nullptr || canonical_apply_sequence == 0U ||
        capacity == 0U) {
        return false;
    }
    *output = static_cast<std::uint32_t>(
        (canonical_apply_sequence - 1U) % capacity);
    return true;
}

static_assert(
    std::atomic_ref<std::uint32_t>::is_always_lock_free,
    "Certified Wire V1 requires lock-free 32-bit atomic_ref");
static_assert(
    std::atomic_ref<std::uint64_t>::is_always_lock_free,
    "Certified Wire V1 requires lock-free 64-bit atomic_ref");

}  // namespace l2flow::ipc
