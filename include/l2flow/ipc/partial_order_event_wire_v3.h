#pragma once

#include "l2flow/ipc/partial_order_event_wire_v2.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <type_traits>

namespace l2flow::ipc {

// V3 retains the V2 Event/cut/channel layouts but replaces the duplicated
// 320-byte order-state payload with a reference to an immutable Event row.
// The distinct magic and major prevent an exact-V2 reader from interpreting
// the compact state slot.
inline constexpr std::array<std::uint8_t, 8U>
    kPartialOrderEventMagicV3{
        'L', '2', 'F', 'P', 'E', 'V', 'T', '3'};
inline constexpr std::uint16_t kPartialOrderEventWireMajorV3 = 3U;
inline constexpr std::uint16_t kPartialOrderEventWireMinorV3 = 0U;
inline constexpr std::uint32_t
    kPartialOrderEventOrderStateVersionBytesV3 = 32U;
inline constexpr std::uint32_t kPartialOrderEventOrderStateSlotBytesV3 =
    128U;

using PartialOrderEventHeaderV3 = PartialOrderEventHeaderV2;
using PartialOrderEventCommitCutV3 = PartialOrderEventCommitCutV2;
using PartialOrderEventChannelHealthV3 = PartialOrderEventChannelHealthV2;
using PartialOrderEventSlotV3 = PartialOrderEventSlotV2;
using PartialOrderEventEnvelopeV3 = PartialOrderEventEnvelopeV2;
using PartialOrderEventOrderStateV3 = PartialOrderEventOrderStateV2;
using PartialOrderEventStatusSnapshotV3 =
    PartialOrderEventStatusSnapshotV2;

struct alignas(32) PartialOrderEventOrderStateVersionV3 final {
    std::uint64_t publish_tag = 0U;
    std::uint64_t canonical_apply_sequence = 0U;
    std::uint64_t derived_event_sequence = 0U;
    std::uint64_t reserved = 0U;
};
static_assert(sizeof(PartialOrderEventOrderStateVersionV3) == 32U);
static_assert(alignof(PartialOrderEventOrderStateVersionV3) == 32U);
static_assert(std::is_trivially_copyable_v<
              PartialOrderEventOrderStateVersionV3>);

struct alignas(64) PartialOrderEventOrderStateSlotV3 final {
    std::uint64_t key_publish_tag = 0U;
    std::uint32_t market = 0U;
    std::uint32_t instrument_id = 0U;
    std::int64_t channel = 0;
    std::int64_t order_id = 0;
    std::array<std::uint8_t, 32U> reserved_identity{};
    std::array<PartialOrderEventOrderStateVersionV3, 2U> versions{};
};
static_assert(sizeof(PartialOrderEventOrderStateSlotV3) == 128U);
static_assert(alignof(PartialOrderEventOrderStateSlotV3) == 64U);
static_assert(std::is_trivially_copyable_v<
              PartialOrderEventOrderStateSlotV3>);
static_assert(offsetof(PartialOrderEventOrderStateSlotV3, versions) ==
              64U);

[[nodiscard]] bool PartialOrderEventHeaderLayoutCanonicalV3(
    const PartialOrderEventHeaderV3& header) noexcept;

[[nodiscard]] bool PartialOrderEventCommitCutCanonicalV3(
    const PartialOrderEventHeaderV3& header,
    const PartialOrderEventCommitCutV3& cut,
    std::uint32_t expected_bank) noexcept;

}  // namespace l2flow::ipc
