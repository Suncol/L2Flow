#include "l2flow/ingress/raw_recovery_authorized.h"

#include <cerrno>
#include <new>

namespace l2flow::ingress {
namespace {

void SetError(
    std::string* error,
    const char* message) noexcept {
    if (error == nullptr) {
        return;
    }
    try {
        *error = message;
    } catch (...) {
    }
}

}  // namespace

RawCoordinatorAuthorizedRecoveryIoV1::
RawCoordinatorAuthorizedRecoveryIoV1(
    RawRecoveryIo& delegate,
    RawReserveAuthorizedActionV1& action,
    RawReserveMutationTargetProviderV1&
        target_provider) noexcept
    : delegate_(delegate),
      action_(action),
      target_provider_(target_provider) {}

bool RawCoordinatorAuthorizedRecoveryIoV1::
DependentArtifactsAbsentProven() const noexcept {
    return AuthorizationError() == 0 &&
           delegate_.DependentArtifactsAbsentProven();
}

bool RawCoordinatorAuthorizedRecoveryIoV1::
R11OrphanAdoptionAuthorized(
    const RawRecoveryPlanV1& plan) const noexcept {
    if (plan.r11_orphan !=
            RawRecoveryR11OrphanV1::
                kNormalHeaderOnly ||
        action_.required_status() !=
            ReserveRegistryStatusV1::kRecovering ||
        action_.recovery_intent() !=
            ReserveRecoveryIntentV1::kResumeConnect ||
        plan.journal_header.source_stream_id !=
            action_.key().route.source_stream_id ||
        plan.journal_header.capture_date !=
            action_.key().route.capture_date ||
        plan.journal_header.stream_day_id !=
            action_.key().stream_day_id) {
        return false;
    }
    return AuthorizationError() == 0;
}

int RawCoordinatorAuthorizedRecoveryIoV1::
SyncParentDirectories() noexcept {
    const int failure = AuthorizationError();
    return failure == 0
               ? delegate_.SyncParentDirectories()
               : failure;
}

int RawCoordinatorAuthorizedRecoveryIoV1::
TruncateJournal(std::uint64_t size) noexcept {
    const int failure = AuthorizationError();
    return failure == 0
               ? delegate_.TruncateJournal(size)
               : failure;
}

int RawCoordinatorAuthorizedRecoveryIoV1::
SyncJournal() noexcept {
    const int failure = AuthorizationError();
    return failure == 0
               ? delegate_.SyncJournal()
               : failure;
}

int RawCoordinatorAuthorizedRecoveryIoV1::
TruncateSegment(
    std::uint32_t segment_sequence,
    std::uint64_t size) noexcept {
    const int failure = AuthorizationError();
    return failure == 0
               ? delegate_.TruncateSegment(
                     segment_sequence, size)
               : failure;
}

int RawCoordinatorAuthorizedRecoveryIoV1::
SyncSegment(
    std::uint32_t segment_sequence,
    bool include_allocation_metadata) noexcept {
    const int failure = AuthorizationError();
    return failure == 0
               ? delegate_.SyncSegment(
                     segment_sequence,
                     include_allocation_metadata)
               : failure;
}

RawRecoveryWriteResult
RawCoordinatorAuthorizedRecoveryIoV1::
WriteJournalSome(
    std::uint64_t offset,
    std::span<const std::byte> bytes) noexcept {
    const int failure = AuthorizationError();
    return failure == 0
               ? delegate_.WriteJournalSome(
                     offset, bytes)
               : RawRecoveryWriteResult{0U, failure};
}

int RawCoordinatorAuthorizedRecoveryIoV1::
RawReserveMutationTargetDirectoryDescriptorV1()
    const noexcept {
    return target_provider_
        .RawReserveMutationTargetDirectoryDescriptorV1();
}

int RawCoordinatorAuthorizedRecoveryIoV1::
AuthorizationError() const noexcept {
    if (action_.required_status() !=
            ReserveRegistryStatusV1::kRecovering ||
        action_.target() == nullptr ||
        !ValidateRawReserveMutationTargetProviderV1(
            target_provider_,
            *action_.target())) {
        return EPERM;
    }
    return action_.ValidateLatest(nullptr)
               ? 0
               : ESTALE;
}

std::unique_ptr<
    RawCoordinatorAuthorizedRecoveryIoV1>
CreateRawCoordinatorAuthorizedRecoveryIoV1(
    RawRecoveryIo& delegate,
    RawReserveAuthorizedActionV1& action,
    std::string* error) noexcept {
    SetError(error, "");
    if (action.required_status() !=
            ReserveRegistryStatusV1::kRecovering ||
        action.target() == nullptr) {
        SetError(
            error,
            "recovery authorization requires a target-bound RECOVERING action");
        return nullptr;
    }
    auto* const target_provider =
        dynamic_cast<
            RawReserveMutationTargetProviderV1*>(
            &delegate);
    if (target_provider == nullptr ||
        !ValidateRawReserveMutationTargetProviderV1(
            *target_provider,
            *action.target())) {
        SetError(
            error,
            "Raw recovery delegate is not bound to the authorized Raw target");
        return nullptr;
    }
    if (!action.ValidateLatest(error)) {
        if (error != nullptr && error->empty()) {
            SetError(
                error,
                "Raw recovery action is stale");
        }
        return nullptr;
    }
    auto* const authorized =
        new (std::nothrow)
            RawCoordinatorAuthorizedRecoveryIoV1(
                delegate,
                action,
                *target_provider);
    if (authorized == nullptr) {
        SetError(
            error,
            "cannot allocate coordinator-authorized Raw recovery I/O");
        return nullptr;
    }
    return std::unique_ptr<
        RawCoordinatorAuthorizedRecoveryIoV1>(
        authorized);
}

}  // namespace l2flow::ingress
