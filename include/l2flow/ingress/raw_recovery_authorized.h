#pragma once

#include "l2flow/ingress/raw_recovery_executor.h"
#include "l2flow/ingress/raw_reserve_coordinator.h"

#include <cstdint>
#include <memory>
#include <span>
#include <string>

namespace l2flow::ingress {

// Decorates a retained POSIX recovery session (or equivalent RawRecoveryIo)
// with an exact coordinator shared-OFD action gate. Every mutating syscall is
// preceded by a fresh two-slot state reload and token/status validation.
// Bare recovery sessions continue to reject R11 orphan adoption; only this
// composition can authorize a normal R11 window, and continuation R11 remains
// reserved for the separate CONSUMED/ACTIVE grant executor.
class RawCoordinatorAuthorizedRecoveryIoV1 final
    : public RawRecoveryIo,
      public RawReserveMutationTargetProviderV1 {
public:
    RawCoordinatorAuthorizedRecoveryIoV1(
        const RawCoordinatorAuthorizedRecoveryIoV1&) =
        delete;
    RawCoordinatorAuthorizedRecoveryIoV1& operator=(
        const RawCoordinatorAuthorizedRecoveryIoV1&) =
        delete;

    [[nodiscard]] bool
    DependentArtifactsAbsentProven()
        const noexcept override;
    [[nodiscard]] bool R11OrphanAdoptionAuthorized(
        const RawRecoveryPlanV1& plan)
        const noexcept override;
    [[nodiscard]] int SyncParentDirectories()
        noexcept override;
    [[nodiscard]] int TruncateJournal(
        std::uint64_t size) noexcept override;
    [[nodiscard]] int SyncJournal() noexcept override;
    [[nodiscard]] int TruncateSegment(
        std::uint32_t segment_sequence,
        std::uint64_t size) noexcept override;
    [[nodiscard]] int SyncSegment(
        std::uint32_t segment_sequence,
        bool include_allocation_metadata)
        noexcept override;
    [[nodiscard]] RawRecoveryWriteResult
    WriteJournalSome(
        std::uint64_t offset,
        std::span<const std::byte> bytes)
        noexcept override;
    [[nodiscard]] int
    RawReserveMutationTargetDirectoryDescriptorV1()
        const noexcept override;

private:
    friend std::unique_ptr<
        RawCoordinatorAuthorizedRecoveryIoV1>
    CreateRawCoordinatorAuthorizedRecoveryIoV1(
        RawRecoveryIo&,
        RawReserveAuthorizedActionV1&,
        std::string*) noexcept;

    RawCoordinatorAuthorizedRecoveryIoV1(
        RawRecoveryIo& delegate,
        RawReserveAuthorizedActionV1& action,
        RawReserveMutationTargetProviderV1&
            target_provider) noexcept;

    [[nodiscard]] int AuthorizationError()
        const noexcept;

    RawRecoveryIo& delegate_;
    RawReserveAuthorizedActionV1& action_;
    RawReserveMutationTargetProviderV1&
        target_provider_;
};

// The generic RawRecoveryIo interface carries no filesystem target proof.
// This controlled construction therefore rejects delegates that do not
// implement RawReserveMutationTargetProviderV1 or whose retained descriptors
// do not match the action's immutable target anchor.
[[nodiscard]] std::unique_ptr<
    RawCoordinatorAuthorizedRecoveryIoV1>
CreateRawCoordinatorAuthorizedRecoveryIoV1(
    RawRecoveryIo& delegate,
    RawReserveAuthorizedActionV1& action,
    std::string* error = nullptr) noexcept;

}  // namespace l2flow::ingress
