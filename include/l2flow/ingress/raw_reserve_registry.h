#pragma once

#include "l2flow/ingress/reserve_state_v1.h"

#include <cstdint>

namespace l2flow::ingress {

// The route is the V1 registry uniqueness key. RegistryEntryKeyV1 adds the
// immutable planned stream-day and the current recovery attempt so mutations
// cannot accidentally target a stale incarnation of the route.
struct RawReserveRegistryRouteV1 final {
    std::uint32_t source_stream_id = 0U;
    std::uint32_t capture_date = 0U;

    friend bool operator==(
        const RawReserveRegistryRouteV1&,
        const RawReserveRegistryRouteV1&) = default;
};

struct RawReserveRegistryEntryKeyV1 final {
    RawReserveRegistryRouteV1 route{};
    ReserveStateV1Identity stream_day_id{};
    ReserveStateV1Identity recovery_attempt_id{};

    friend bool operator==(
        const RawReserveRegistryEntryKeyV1&,
        const RawReserveRegistryEntryKeyV1&) = default;
};

struct RawReserveFreshScaffoldingV1 final {
    RawReserveRegistryEntryKeyV1 key{};
    ReserveStateV1Identity writer_instance{};
    ReserveRecoveryIntentV1 recovery_intent =
        ReserveRecoveryIntentV1::kNone;
    std::uint64_t scaffolding_allocation_cap = 0U;
    std::uint64_t safe_stop_template_id = 0U;
};

struct RawReserveExistingAnchorRecoveryV1 final {
    RawReserveRegistryEntryKeyV1 key{};
    ReserveStateV1Identity writer_instance{};
    ReserveRecoveryIntentV1 recovery_intent =
        ReserveRecoveryIntentV1::kNone;
    std::uint64_t safe_stop_template_id = 0U;
};

struct RawReserveScaffoldingTakeoverV1 final {
    RawReserveRegistryEntryKeyV1 key{};
    ReserveStateV1Identity new_writer_instance{};
    std::uint64_t new_scaffolding_allocation_cap = 0U;
};

struct RawReserveWriterTakeoverV1 final {
    RawReserveRegistryEntryKeyV1 key{};
    ReserveStateV1Identity new_writer_instance{};
};

struct RawReserveActiveTakeoverV1 final {
    RawReserveRegistryEntryKeyV1 old_key{};
    ReserveStateV1Identity new_writer_instance{};
    ReserveStateV1Identity new_recovery_attempt_id{};
    ReserveRecoveryIntentV1 recovery_intent =
        ReserveRecoveryIntentV1::kNone;
};

// Every successful builder returns an independently codec-valid,
// generation+1 PROVISIONED candidate and verifies the exact before/after pair
// with ValidateReserveStateSlotPairV1. Output is unchanged on every failure,
// including generation overflow and stale identity/status input.

[[nodiscard]] ReserveStateV1Error
BuildRawReserveRegisterFreshScaffoldingV1(
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& before,
    const RawReserveFreshScaffoldingV1& registration,
    ReserveStateSlotV1* output) noexcept;

[[nodiscard]] ReserveStateV1Error
BuildRawReservePublishInitV1(
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& before,
    const RawReserveRegistryEntryKeyV1& key,
    ReserveStateSlotV1* output) noexcept;

[[nodiscard]] ReserveStateV1Error
BuildRawReserveRegisterExistingAnchorRecoveringV1(
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& before,
    const RawReserveExistingAnchorRecoveryV1& registration,
    ReserveStateSlotV1* output) noexcept;

[[nodiscard]] ReserveStateV1Error
BuildRawReserveTakeoverScaffoldingV1(
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& before,
    const RawReserveScaffoldingTakeoverV1& takeover,
    ReserveStateSlotV1* output) noexcept;

[[nodiscard]] ReserveStateV1Error
BuildRawReserveTakeoverInitV1(
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& before,
    const RawReserveWriterTakeoverV1& takeover,
    ReserveStateSlotV1* output) noexcept;

[[nodiscard]] ReserveStateV1Error
BuildRawReserveTakeoverRecoveringV1(
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& before,
    const RawReserveWriterTakeoverV1& takeover,
    ReserveStateSlotV1* output) noexcept;

[[nodiscard]] ReserveStateV1Error
BuildRawReserveTakeoverActiveV1(
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& before,
    const RawReserveActiveTakeoverV1& takeover,
    ReserveStateSlotV1* output) noexcept;

[[nodiscard]] ReserveStateV1Error
BuildRawReservePublishActiveV1(
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& before,
    const RawReserveRegistryEntryKeyV1& key,
    ReserveStateSlotV1* output) noexcept;

[[nodiscard]] ReserveStateV1Error
BuildRawReserveUnregisterActiveV1(
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& before,
    const RawReserveRegistryEntryKeyV1& key,
    ReserveStateSlotV1* output) noexcept;

// Removes only an exact RECOVERING + RECOVER_SEAL_ONLY route.  The pure
// state builder cannot validate filesystem evidence; the POSIX coordinator
// exposes this transition only after consuming the corresponding durable
// terminal maintenance-report/sidecar receipt.
[[nodiscard]] ReserveStateV1Error
BuildRawReserveUnregisterTerminalRecoveringV1(
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& before,
    const RawReserveRegistryEntryKeyV1& key,
    ReserveStateSlotV1* output) noexcept;

}  // namespace l2flow::ingress
