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

// Independent, append-only full-day Event/history ABI.  It is deliberately
// separate from both FAST Wire V2 and the bounded CERTIFIED Tick ring.
inline constexpr std::array<std::uint8_t, 8U>
    kCertifiedOrderEventMagicV1{
        'L', '2', 'F', 'C', 'E', 'V', 'T', '1'};
inline constexpr std::uint16_t kCertifiedOrderEventWireMajorV1 = 1U;
inline constexpr std::uint16_t kCertifiedOrderEventWireMinorV1 = 1U;
inline constexpr std::uint32_t kCertifiedOrderEventEndianMarkerV1 =
    0x01020304U;
inline constexpr std::uint32_t kCertifiedOrderEventHeaderBytesV1 =
    4096U;
inline constexpr std::uint32_t kCertifiedOrderEventSlotBytesV1 =
    384U;
inline constexpr std::uint32_t kCertifiedOrderEventAlignmentV1 = 64U;

inline constexpr std::uint64_t
    kCertifiedOrderEventControlResponseMagicV1 =
        0x315456454346324cULL;  // "L2FCEVT1" little-endian

enum CertifiedOrderEventCoverageFlagV1 : std::uint32_t {
    // CERTIFIED always proves native continuity from the documented sequence
    // origin. It is never exposed for a process-start partial session.
    kCertifiedOrderEventCoverageFromOpenV1 = 1U << 0U,
    // The from-open prefix was rebuilt behind an exact worker barrier before
    // the control socket became queryable. Ordinary from-open startup leaves
    // this bit clear.
    kCertifiedOrderEventStartupPrefixRecoveredV1 = 1U << 1U,
};
inline constexpr std::uint32_t
    kCertifiedOrderEventKnownCoverageFlagsV1 =
        kCertifiedOrderEventCoverageFromOpenV1 |
        kCertifiedOrderEventStartupPrefixRecoveredV1;

// Every mutable scalar is accessed through an always-lock-free atomic_ref.
// The journal is append-only: event_published_sequence is the immutable
// visible prefix, while canonical_apply_frontier also advances for an input
// which emits zero Event rows.
struct alignas(4096) CertifiedOrderEventHeaderV1 final {
    std::array<std::uint8_t, 8U> magic{};
    std::uint16_t abi_major = 0U;
    std::uint16_t abi_minor = 0U;
    std::uint32_t header_bytes = 0U;
    std::uint32_t endian_marker = 0U;
    std::uint32_t flags =
        kCertifiedOrderEventCoverageFromOpenV1;
    std::uint64_t total_mapping_bytes = 0U;

    std::array<std::uint8_t, 16U> run_id{};
    std::uint64_t session_epoch = 0U;
    std::uint32_t trade_date = 0U;
    std::uint32_t reserved_identity = 0U;

    std::uint64_t slots_offset =
        kCertifiedOrderEventHeaderBytesV1;
    std::uint64_t event_capacity = 0U;
    std::uint32_t slot_stride = kCertifiedOrderEventSlotBytesV1;
    std::uint32_t region_alignment =
        kCertifiedOrderEventAlignmentV1;
    std::array<std::uint8_t, 40U> reserved_layout{};

    // Stable even -> odd -> field stores -> next stable even.
    std::uint64_t status_publish_tag = 0U;
    std::uint64_t heartbeat_monotonic_ns = 0U;
    std::uint64_t canonical_apply_frontier = 0U;
    std::uint64_t event_published_sequence = 0U;
    std::uint64_t generation = 0U;
    std::uint64_t shanghai_order_state_count = 0U;
    std::uint64_t shenzhen_order_state_count = 0U;
    // Diagnostic only. Physical backing is committed lazily and never
    // authorizes reading past event_published_sequence.
    std::uint64_t committed_mapping_bytes = 0U;
    std::uint64_t reserved_status = 0U;
    std::array<std::uint8_t, 3896U> reserved{};
};
static_assert(sizeof(CertifiedOrderEventHeaderV1) == 4096U);
static_assert(alignof(CertifiedOrderEventHeaderV1) == 4096U);
static_assert(std::is_standard_layout_v<CertifiedOrderEventHeaderV1>);
static_assert(
    std::is_trivially_copyable_v<CertifiedOrderEventHeaderV1>);
static_assert(
    offsetof(CertifiedOrderEventHeaderV1, status_publish_tag) ==
    128U);
static_assert(
    offsetof(CertifiedOrderEventHeaderV1, reserved) == 200U);

// The payload begins on a cache-line boundary. canonical_apply_sequence is
// the process-owned CERTIFIED Tick publication order which caused this row;
// it is not BizIndex, ApplSeqNum, or an exchange cross-channel order.
struct alignas(64) CertifiedOrderEventSlotV1 final {
    std::uint64_t publish_tag = 0U;
    std::uint64_t canonical_apply_sequence = 0U;
    std::array<std::uint8_t, 48U> reserved{};
    std::array<std::uint64_t, 40U> payload_words{};
};
static_assert(sizeof(CertifiedOrderEventSlotV1) == 384U);
static_assert(alignof(CertifiedOrderEventSlotV1) == 64U);
static_assert(std::is_standard_layout_v<CertifiedOrderEventSlotV1>);
static_assert(
    std::is_trivially_copyable_v<CertifiedOrderEventSlotV1>);
static_assert(
    offsetof(CertifiedOrderEventSlotV1, payload_words) == 64U);
static_assert(
    sizeof(l2flow_instrument_derived_event_row_v1) ==
    sizeof(CertifiedOrderEventSlotV1::payload_words));
static_assert(std::atomic_ref<std::uint32_t>::is_always_lock_free);
static_assert(std::atomic_ref<std::uint64_t>::is_always_lock_free);

struct CertifiedOrderEventEnvelopeV1 final {
    std::uint64_t canonical_apply_sequence = 0U;
    l2flow_instrument_derived_event_row_v1 event{};
};
static_assert(
    std::is_trivially_copyable_v<CertifiedOrderEventEnvelopeV1>);

struct CertifiedOrderEventControlResponseV1 final {
    std::uint64_t magic =
        kCertifiedOrderEventControlResponseMagicV1;
    std::uint16_t abi_major = kCertifiedOrderEventWireMajorV1;
    std::uint16_t abi_minor = kCertifiedOrderEventWireMinorV1;
    std::uint16_t status = 0U;
    std::uint16_t response_bytes =
        sizeof(CertifiedOrderEventControlResponseV1);
    std::uint64_t nonce = 0U;
    std::uint64_t mapping_bytes = 0U;
    std::array<std::uint8_t, 16U> run_id{};
    std::uint64_t session_epoch = 0U;
    std::uint32_t trade_date = 0U;
    std::uint32_t reserved0 = 0U;
    std::uint64_t event_capacity = 0U;
    std::uint32_t slot_stride =
        kCertifiedOrderEventSlotBytesV1;
    std::uint32_t coverage_flags = 0U;
    std::array<std::uint8_t, 48U> reserved{};
};
static_assert(sizeof(CertifiedOrderEventControlResponseV1) == 128U);
static_assert(
    std::is_trivially_copyable_v<
        CertifiedOrderEventControlResponseV1>);

namespace certified_order_event_wire_v1_detail {

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

}  // namespace certified_order_event_wire_v1_detail

[[nodiscard]] bool CertifiedOrderEventHeaderCanonicalV1(
    const CertifiedOrderEventHeaderV1& header) noexcept;

[[nodiscard]] bool CertifiedOrderEventRowCanonicalV1(
    const l2flow_instrument_derived_event_row_v1& row,
    std::uint32_t expected_trade_date,
    std::uint64_t expected_derived_event_sequence) noexcept;

[[nodiscard]] bool CertifiedOrderEventEnvelopeCanonicalV1(
    const CertifiedOrderEventEnvelopeV1& envelope,
    std::uint32_t expected_trade_date,
    std::uint64_t expected_derived_event_sequence) noexcept;

[[nodiscard]] constexpr bool CertifiedOrderEventSlotIndexV1(
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
