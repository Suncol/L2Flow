#pragma once

#include "l2flow/ipc/instrument_derived_event_history_c_v1.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <type_traits>

namespace l2flow::ipc {

// Process-start partial Event data has a distinct ABI from the from-open
// CERTIFIED Event V1 mapping. An exact-major reader must never interpret one
// as the other.
inline constexpr std::array<std::uint8_t, 8U>
    kPartialOrderEventMagicV2{
        'L', '2', 'F', 'P', 'E', 'V', 'T', '2'};
inline constexpr std::uint16_t kPartialOrderEventWireMajorV2 = 2U;
inline constexpr std::uint16_t kPartialOrderEventWireMinorV2 = 0U;
inline constexpr std::uint32_t kPartialOrderEventEndianMarkerV2 =
    0x01020304U;
inline constexpr std::uint32_t kPartialOrderEventHeaderBytesV2 =
    4096U;
inline constexpr std::uint32_t kPartialOrderEventSlotBytesV2 =
    384U;
inline constexpr std::uint32_t
    kPartialOrderEventChannelHealthBytesV2 = 128U;
inline constexpr std::uint32_t
    kPartialOrderEventOrderStateVersionBytesV2 = 384U;
inline constexpr std::uint32_t kPartialOrderEventOrderStateSlotBytesV2 =
    832U;
inline constexpr std::uint32_t kPartialOrderEventAlignmentV2 = 64U;

enum class PartialOrderEventTemporalCoverageV2 : std::uint32_t {
    // The mapping starts at coverage_start_unix_ns. It makes no from-open
    // completeness claim.
    kProcessStart = 1U,
};

enum class PartialOrderEventOrderingQualityV2 : std::uint32_t {
    // Bounded buffering repaired the disorder observed inside the declared
    // process-start epoch. This is not a native completeness certificate.
    kBoundedReorderedPartial = 1U,
};

enum class PartialOrderEventServiceStateV2 : std::uint32_t {
    kInitializing = 1U,
    kContiguous,
    kReordering,
    kCatchingUp,
    kFrozenConflict,
    kFrozenResource,
    kRestarting,
    kCorrectionPending,
    kStoppedClean,
};

enum class PartialOrderEventLastErrorV2 : std::uint32_t {
    kNone = 0U,
    kOutOfOrderInput,
    kConflictingDuplicate,
    kResourceExhausted,
    kWorkerExited,
    kPermanentGap,
    kProjectionFailure,
    kPublicationFailure,
    kPublicationInvariant,
};

enum PartialOrderEventChannelFlagV2 : std::uint32_t {
    kPartialOrderEventChannelOriginEstablishedV2 = 1U << 0U,
    kPartialOrderEventChannelAffectedV2 = 1U << 1U,
    kPartialOrderEventChannelStaleV2 = 1U << 2U,
};
inline constexpr std::uint32_t kPartialOrderEventKnownChannelFlagsV2 =
    kPartialOrderEventChannelOriginEstablishedV2 |
    kPartialOrderEventChannelAffectedV2 |
    kPartialOrderEventChannelStaleV2;

// A channel row is a complete snapshot at commit_sequence. Rows in a bank
// are strictly sorted by (market, channel) and are unique.
struct alignas(64) PartialOrderEventChannelHealthV2 final {
    std::uint64_t commit_sequence = 0U;
    std::int64_t channel = 0;
    std::int64_t expected_native_sequence = 0;
    std::int64_t contiguous_native_sequence = 0;
    std::int64_t highest_observed_native_sequence = 0;
    std::int64_t oldest_missing_native_sequence = 0;
    std::uint64_t pending_count = 0U;
    std::uint64_t oldest_gap_age_ns = 0U;
    std::uint32_t market = 0U;
    std::uint32_t flags = 0U;
    PartialOrderEventServiceStateV2 state =
        PartialOrderEventServiceStateV2::kInitializing;
    PartialOrderEventLastErrorV2 last_error =
        PartialOrderEventLastErrorV2::kNone;
    std::array<std::uint8_t, 48U> reserved{};
};
static_assert(sizeof(PartialOrderEventChannelHealthV2) == 128U);
static_assert(alignof(PartialOrderEventChannelHealthV2) == 64U);
static_assert(
    std::is_trivially_copyable_v<
        PartialOrderEventChannelHealthV2>);
static_assert(
    offsetof(PartialOrderEventChannelHealthV2, reserved) == 80U);

// Each cut owns the same-index channel-health bank. The writer first makes
// the target tag odd, then writes that bank and append-only Event rows, and
// finally release-publishes a new even tag. A reader validates both cuts and
// selects the greatest complete commit_sequence. At least one old cut remains
// stable throughout every ordinary commit phase.
struct alignas(64) PartialOrderEventCommitCutV2 final {
    std::uint64_t publish_tag = 0U;
    std::uint64_t commit_sequence = 0U;
    std::uint64_t heartbeat_monotonic_ns = 0U;
    // Local source/capture sequence only; this is not a feeder watermark.
    std::uint64_t captured_source_frontier = 0U;
    std::uint64_t canonical_apply_frontier = 0U;
    std::uint64_t event_published_frontier = 0U;
    std::uint64_t history_generation = 0U;
    // The order-state hash table is versioned by the same canonical cut.
    // generation is the immutable mapping publication_generation when the
    // order-state region is present, otherwise both fields are zero.
    std::uint64_t order_state_generation = 0U;
    std::uint64_t order_state_canonical_frontier = 0U;
    std::uint64_t committed_event_region_bytes = 0U;
    std::uint64_t shanghai_order_state_count = 0U;
    std::uint64_t shenzhen_order_state_count = 0U;
    std::uint64_t pending_count = 0U;
    std::uint64_t reorder_high_water = 0U;
    std::uint64_t oldest_gap_age_ns = 0U;
    std::uint32_t affected_channel_count = 0U;
    std::uint32_t channel_health_count = 0U;
    PartialOrderEventServiceStateV2 state =
        PartialOrderEventServiceStateV2::kInitializing;
    std::uint32_t stale = 1U;
    PartialOrderEventLastErrorV2 last_error =
        PartialOrderEventLastErrorV2::kNone;
    std::uint32_t active_channel_bank = 0U;
    std::uint32_t channel_bank_crc32c = 0U;
    std::uint32_t cut_crc32c = 0U;
    std::array<std::uint8_t, 104U> reserved{};
};
static_assert(sizeof(PartialOrderEventCommitCutV2) == 256U);
static_assert(alignof(PartialOrderEventCommitCutV2) == 64U);
static_assert(
    std::is_trivially_copyable_v<PartialOrderEventCommitCutV2>);
static_assert(
    offsetof(PartialOrderEventCommitCutV2, reserved) == 152U);

struct alignas(64) PartialOrderEventOrderStateVersionV2 final {
    std::uint64_t publish_tag = 0U;
    std::uint64_t canonical_apply_sequence = 0U;
    std::array<std::uint8_t, 48U> reserved{};
    std::array<std::uint64_t, 40U> payload_words{};
};
static_assert(
    sizeof(PartialOrderEventOrderStateVersionV2) == 384U);
static_assert(
    alignof(PartialOrderEventOrderStateVersionV2) == 64U);
static_assert(
    std::is_trivially_copyable_v<
        PartialOrderEventOrderStateVersionV2>);

// Fixed-capacity open-addressed table. The full key is
// (header.trade_date, market, instrument_id, channel, order_id); trade_date is
// immutable at mapping scope and is therefore not repeated in every slot.
// key_publish_tag changes only while an empty slot is first claimed. Each
// immutable key owns two row versions; the writer overwrites only the older
// version, leaving the last-good version readable until the replacement cut
// is published.
struct alignas(64) PartialOrderEventOrderStateSlotV2 final {
    std::uint64_t key_publish_tag = 0U;
    std::uint32_t market = 0U;
    std::uint32_t instrument_id = 0U;
    std::int64_t channel = 0;
    std::int64_t order_id = 0;
    std::array<std::uint8_t, 32U> reserved_identity{};
    std::array<PartialOrderEventOrderStateVersionV2, 2U>
        versions{};
};
static_assert(sizeof(PartialOrderEventOrderStateSlotV2) == 832U);
static_assert(alignof(PartialOrderEventOrderStateSlotV2) == 64U);
static_assert(
    std::is_trivially_copyable_v<
        PartialOrderEventOrderStateSlotV2>);
static_assert(
    offsetof(PartialOrderEventOrderStateSlotV2, versions) == 64U);

struct alignas(4096) PartialOrderEventHeaderV2 final {
    std::array<std::uint8_t, 8U> magic{};
    std::uint16_t abi_major = 0U;
    std::uint16_t abi_minor = 0U;
    std::uint32_t header_bytes = 0U;
    std::uint32_t endian_marker = 0U;
    PartialOrderEventTemporalCoverageV2 temporal_coverage =
        PartialOrderEventTemporalCoverageV2::kProcessStart;
    std::uint64_t total_mapping_bytes = 0U;

    std::array<std::uint8_t, 16U> run_id{};
    std::uint64_t session_epoch = 0U;
    std::uint32_t trade_date = 0U;
    std::uint32_t reserved_identity = 0U;
    std::uint64_t publication_generation = 0U;
    std::uint64_t correction_epoch = 0U;
    std::uint64_t coverage_start_unix_ns = 0U;
    PartialOrderEventOrderingQualityV2 ordering_quality =
        PartialOrderEventOrderingQualityV2::
            kBoundedReorderedPartial;
    std::uint32_t reserved_quality = 0U;

    std::uint64_t slots_offset =
        kPartialOrderEventHeaderBytesV2;
    std::uint64_t event_capacity = 0U;
    std::uint32_t slot_stride = kPartialOrderEventSlotBytesV2;
    std::uint32_t region_alignment =
        kPartialOrderEventAlignmentV2;
    std::uint64_t channel_bank_offsets[2U]{};
    std::uint64_t channel_bank_bytes = 0U;
    std::uint32_t channel_capacity = 0U;
    std::uint32_t channel_stride =
        kPartialOrderEventChannelHealthBytesV2;
    std::uint64_t order_states_offset = 0U;
    std::uint64_t order_state_capacity = 0U;
    std::uint32_t order_state_stride =
        kPartialOrderEventOrderStateSlotBytesV2;
    std::uint32_t reserved_order_state = 0U;
    // Explicit future extension region. V2.0 requires both values to be zero.
    std::uint64_t extension_offset = 0U;
    std::uint64_t extension_bytes = 0U;
    std::array<std::uint8_t, 56U> reserved_layout{};

    std::array<PartialOrderEventCommitCutV2, 2U> cuts{};
    std::array<std::uint8_t, 3328U> reserved{};
};
static_assert(sizeof(PartialOrderEventHeaderV2) == 4096U);
static_assert(alignof(PartialOrderEventHeaderV2) == 4096U);
static_assert(std::is_standard_layout_v<PartialOrderEventHeaderV2>);
static_assert(std::is_trivially_copyable_v<PartialOrderEventHeaderV2>);
static_assert(offsetof(PartialOrderEventHeaderV2, cuts) == 256U);

struct alignas(64) PartialOrderEventSlotV2 final {
    std::uint64_t publish_tag = 0U;
    std::uint64_t canonical_apply_sequence = 0U;
    std::array<std::uint8_t, 48U> reserved{};
    std::array<std::uint64_t, 40U> payload_words{};
};
static_assert(sizeof(PartialOrderEventSlotV2) == 384U);
static_assert(alignof(PartialOrderEventSlotV2) == 64U);
static_assert(std::is_trivially_copyable_v<PartialOrderEventSlotV2>);
static_assert(offsetof(PartialOrderEventSlotV2, payload_words) == 64U);
static_assert(
    sizeof(l2flow_instrument_derived_event_row_v1) ==
    sizeof(PartialOrderEventSlotV2::payload_words));
static_assert(std::atomic_ref<std::uint32_t>::is_always_lock_free);
static_assert(std::atomic_ref<std::uint64_t>::is_always_lock_free);

struct PartialOrderEventEnvelopeV2 final {
    std::uint64_t canonical_apply_sequence = 0U;
    l2flow_instrument_derived_event_row_v1 event{};
};
static_assert(std::is_trivially_copyable_v<PartialOrderEventEnvelopeV2>);

struct PartialOrderEventOrderStateV2 final {
    std::uint64_t canonical_apply_sequence = 0U;
    l2flow_instrument_derived_event_row_v1 order_revision{};
};
static_assert(std::is_trivially_copyable_v<PartialOrderEventOrderStateV2>);

struct PartialOrderEventStatusSnapshotV2 final {
    std::array<std::uint8_t, 16U> run_id{};
    std::uint64_t session_epoch = 0U;
    std::uint32_t trade_date = 0U;
    PartialOrderEventTemporalCoverageV2 temporal_coverage =
        PartialOrderEventTemporalCoverageV2::kProcessStart;
    std::uint64_t publication_generation = 0U;
    std::uint64_t correction_epoch = 0U;
    std::uint64_t coverage_start_unix_ns = 0U;
    PartialOrderEventOrderingQualityV2 ordering_quality =
        PartialOrderEventOrderingQualityV2::
            kBoundedReorderedPartial;
    std::uint64_t event_capacity = 0U;
    std::uint32_t channel_capacity = 0U;
    std::uint64_t order_state_capacity = 0U;
    PartialOrderEventCommitCutV2 cut{};

    [[nodiscard]] bool order_state_available() const noexcept {
        return cut.order_state_generation != 0U &&
               cut.order_state_canonical_frontier != 0U;
    }
};

namespace partial_order_event_wire_v2_detail {

template <typename Value, std::size_t Size>
[[nodiscard]] constexpr bool AllZero(
    const std::array<Value, Size>& values) noexcept {
    return std::all_of(
        values.begin(), values.end(), [](Value value) noexcept {
            return value == Value{};
        });
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

[[nodiscard]] constexpr bool AlignUp(
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

}  // namespace partial_order_event_wire_v2_detail

[[nodiscard]] bool PartialOrderEventHeaderLayoutCanonicalV2(
    const PartialOrderEventHeaderV2& header) noexcept;

[[nodiscard]] bool PartialOrderEventCommitCutCanonicalV2(
    const PartialOrderEventHeaderV2& header,
    const PartialOrderEventCommitCutV2& cut,
    std::uint32_t expected_bank) noexcept;

[[nodiscard]] bool PartialOrderEventChannelHealthCanonicalV2(
    const PartialOrderEventChannelHealthV2& health,
    std::uint64_t expected_commit_sequence) noexcept;

[[nodiscard]] bool PartialOrderEventEnvelopeCanonicalV2(
    const PartialOrderEventEnvelopeV2& envelope,
    std::uint32_t expected_trade_date,
    std::uint64_t expected_derived_event_sequence) noexcept;

[[nodiscard]] bool PartialOrderEventOrderStateCanonicalV2(
    const PartialOrderEventOrderStateV2& state,
    std::uint32_t expected_trade_date,
    std::uint32_t expected_market,
    std::uint32_t expected_instrument_id,
    std::int64_t expected_channel,
    std::int64_t expected_order_id) noexcept;

[[nodiscard]] std::uint32_t PartialOrderEventCommitCutCrc32cV2(
    const PartialOrderEventCommitCutV2& cut) noexcept;

[[nodiscard]] constexpr bool PartialOrderEventSlotIndexV2(
    std::uint64_t derived_event_sequence,
    std::uint64_t event_capacity,
    std::uint64_t* output) noexcept {
    if (output == nullptr || derived_event_sequence == 0U ||
        derived_event_sequence > event_capacity) {
        return false;
    }
    *output = derived_event_sequence - 1U;
    return true;
}

}  // namespace l2flow::ipc
