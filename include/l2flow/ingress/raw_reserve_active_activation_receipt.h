#pragma once

#include "l2flow/common/identity128.h"
#include "l2flow/ingress/raw_reserve_coordinator.h"

#include <utility>

namespace l2flow::ingress {

// Non-forgeable, process-local proof that the coordinator completed an exact
// INIT -> ACTIVE or RECOVERING -> ACTIVE publication. For a recovered stream
// it is issued only after consuming and revalidating the matching durable
// RecoveryMaintenanceReportV1 receipt. For a fresh stream it is issued only
// while the exact INIT generation and canonical target remain locked. The
// coordinator is the only producer.
class RawReserveActiveActivationReceiptV1 final {
public:
    ~RawReserveActiveActivationReceiptV1() = default;

    RawReserveActiveActivationReceiptV1(
        const RawReserveActiveActivationReceiptV1&) =
        delete;
    RawReserveActiveActivationReceiptV1& operator=(
        const RawReserveActiveActivationReceiptV1&) =
        delete;
    RawReserveActiveActivationReceiptV1(
        RawReserveActiveActivationReceiptV1&&) = delete;
    RawReserveActiveActivationReceiptV1& operator=(
        RawReserveActiveActivationReceiptV1&&) = delete;

    [[nodiscard]] const RawReserveRegistryEntryKeyV1&
    key() const noexcept {
        return key_;
    }
    [[nodiscard]] const l2flow::common::Identity128&
    writer_instance() const noexcept {
        return writer_instance_;
    }
    [[nodiscard]] const
        RawReserveGenerationActionTokenV1&
    active_token() const noexcept {
        return active_token_;
    }
    [[nodiscard]] const
        RawReserveMutationTargetAnchorV1&
    target() const noexcept {
        return target_;
    }

private:
    friend class RawReserveRegistryCoordinatorV1;

    RawReserveActiveActivationReceiptV1(
        RawReserveRegistryEntryKeyV1 key,
        l2flow::common::Identity128 writer_instance,
        RawReserveGenerationActionTokenV1 active_token,
        RawReserveMutationTargetAnchorV1 target) noexcept
        : key_(std::move(key)),
          writer_instance_(writer_instance),
          active_token_(std::move(active_token)),
          target_(std::move(target)) {}

    RawReserveRegistryEntryKeyV1 key_{};
    l2flow::common::Identity128 writer_instance_{};
    RawReserveGenerationActionTokenV1 active_token_{};
    RawReserveMutationTargetAnchorV1 target_;
};

}  // namespace l2flow::ingress
