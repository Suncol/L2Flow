#pragma once

#include "l2flow/ipc/instrument_derived_event_history_c_v1.h"

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <type_traits>

namespace l2flow::ipc {

inline constexpr std::array<std::uint8_t, 8U>
    kOrderEventDeltaMagicV1{
        'L', '2', 'F', 'E', 'V', 'T', '1', '\0'};
inline constexpr std::uint16_t kOrderEventDeltaWireMajorV1 = 1U;
// V1.1 consumed the former 64-bit header reserved scalar as two semantic
// fields. V1.2 carries record-schema-2 source-tick event identity in the
// unchanged 320-byte payload. Exact-minor validation makes older readers fail
// closed instead of interpreting the new semantic bytes as reserved.
inline constexpr std::uint16_t kOrderEventDeltaWireMinorV1 = 2U;
inline constexpr std::uint32_t kOrderEventDeltaEndianMarkerV1 =
    0x01020304U;
inline constexpr std::uint32_t kOrderEventDeltaPayloadSchemaV1 =
    L2FLOW_INSTRUMENT_DERIVED_EVENT_ROW_SCHEMA_V2;
inline constexpr std::uint64_t kOrderEventDeltaHeaderBytesV1 =
    4096U;
inline constexpr std::uint64_t kOrderEventDeltaSlotBytesV1 = 384U;

enum class OrderEventDeltaProducerStateV1 : std::uint32_t {
    kInitializing = 1U,
    kActive = 2U,
    kDraining = 3U,
    kStoppedClean = 4U,
    kFailed = 5U,
};

// Defines the beginning of the locally contiguous source-tick sequence. This
// is a temporal coverage statement, not a claim that every vendor-native
// sequence number was observed.
enum class OrderEventDeltaTemporalCoverageV1 : std::uint32_t {
    kFromMarketOpen = 1U,
    kFromProcessStart = 2U,
};

// The event ring currently supports one stream-quality contract: source
// tick_stream_sequence is dense from one and every accepted source tick is
// committed exactly once. Native BizIndex/ApplSeqNum monotonicity remains a
// separate aggregation-engine guard and is not weakened by this value.
enum class OrderEventDeltaStreamQualityV1 : std::uint32_t {
    kLocalTickStreamContiguous = 1U,
};

enum OrderEventDeltaHeaderFlagV1 : std::uint32_t {
    // Once set, the producer has rejected a source-sequence gap, an
    // unpublishable batch, or another condition which means the live stream
    // can no longer be claimed complete. V1 has no recovery transition.
    kOrderEventDeltaCoverageLostV1 = 1U << 0U,
};

enum class OrderEventDeltaMarketV1 : std::uint8_t {
    kShanghai = 1U,
    kShenzhen = 2U,
};

enum class OrderEventDeltaKindV1 : std::uint8_t {
    kOrderRevision = 1U,
    kTrade = 2U,
    kCancel = 3U,
    kStatus = 4U,
};

// The live ring deliberately reuses the formal derived-history row ABI
// instead of defining a second semantically overlapping event record.
// derived_event_sequence is assigned by the live ring producer and is dense
// across every instrument in that ring session. It is not BizIndex,
// ApplSeqNum, source_sequence, ingress_sequence, or tick_stream_sequence.
using OrderEventDeltaPayloadV1 =
    ::l2flow_instrument_derived_event_row_v1;
static_assert(sizeof(OrderEventDeltaPayloadV1) == 320U);
static_assert(alignof(OrderEventDeltaPayloadV1) == 8U);
static_assert(std::is_standard_layout_v<OrderEventDeltaPayloadV1>);
static_assert(std::is_trivially_copyable_v<OrderEventDeltaPayloadV1>);
static_assert(
    offsetof(OrderEventDeltaPayloadV1, derived_event_sequence) == 8U);
static_assert(
    offsetof(OrderEventDeltaPayloadV1, native_event_sequence) ==
    200U);
static_assert(offsetof(OrderEventDeltaPayloadV1, market) == 288U);

// All mutable scalars are accessed through always-lock-free atomic_ref. The
// producer writes every slot caused by one source tick, then release-publishes
// the complete event prefix, and only then release-publishes the source-tick
// cursor. Thus neither public prefix declares a partially published
// source-tick batch complete.
struct alignas(4096) OrderEventDeltaHeaderV1 final {
    std::array<std::uint8_t, 8U> magic{};
    std::uint16_t abi_major = 0U;
    std::uint16_t abi_minor = 0U;
    std::uint32_t header_bytes = 0U;
    std::uint32_t endian_marker = 0U;
    std::uint32_t producer_state = 0U;
    std::uint64_t total_mapping_bytes = 0U;

    std::array<std::uint8_t, 16U> run_id{};
    std::uint64_t session_epoch = 0U;
    std::uint32_t trade_date = 0U;
    std::uint32_t flags = 0U;

    std::uint64_t ring_capacity = 0U;
    std::uint64_t slot_stride = 0U;
    std::uint64_t slots_offset = 0U;
    std::uint64_t event_published_sequence = 0U;
    std::uint64_t source_tick_consumed_sequence = 0U;
    std::uint64_t heartbeat_monotonic_ns = 0U;
    std::uint64_t producer_started_monotonic_ns = 0U;
    std::uint32_t temporal_coverage = 0U;
    std::uint32_t stream_quality = 0U;
    std::array<std::uint8_t, 3968U> reserved{};
};
static_assert(sizeof(OrderEventDeltaHeaderV1) == 4096U);
static_assert(alignof(OrderEventDeltaHeaderV1) == 4096U);
static_assert(std::is_standard_layout_v<OrderEventDeltaHeaderV1>);
static_assert(offsetof(OrderEventDeltaHeaderV1, producer_state) == 20U);
static_assert(
    offsetof(OrderEventDeltaHeaderV1, total_mapping_bytes) == 24U);
static_assert(offsetof(OrderEventDeltaHeaderV1, session_epoch) == 48U);
static_assert(offsetof(OrderEventDeltaHeaderV1, flags) == 60U);
static_assert(
    offsetof(OrderEventDeltaHeaderV1, event_published_sequence) ==
    88U);
static_assert(
    offsetof(
        OrderEventDeltaHeaderV1,
        source_tick_consumed_sequence) == 96U);
static_assert(
    offsetof(OrderEventDeltaHeaderV1, temporal_coverage) == 120U);
static_assert(
    offsetof(OrderEventDeltaHeaderV1, stream_quality) == 124U);
static_assert(offsetof(OrderEventDeltaHeaderV1, reserved) == 128U);

// Slot payload is protected by an even/odd publish tag. Reserved bytes remain
// zero for the whole session and are validated by readers.
struct alignas(64) OrderEventDeltaSlotV1 final {
    std::uint64_t publish_tag = 0U;
    std::array<std::uint8_t, 56U> reserved{};
    std::array<std::uint64_t, 40U> payload_words{};
};
static_assert(sizeof(OrderEventDeltaSlotV1) == 384U);
static_assert(alignof(OrderEventDeltaSlotV1) == 64U);
static_assert(std::is_standard_layout_v<OrderEventDeltaSlotV1>);
static_assert(
    sizeof(OrderEventDeltaPayloadV1) ==
    sizeof(OrderEventDeltaSlotV1::payload_words));
static_assert(std::atomic_ref<std::uint32_t>::is_always_lock_free);
static_assert(std::atomic_ref<std::uint64_t>::is_always_lock_free);

}  // namespace l2flow::ipc
