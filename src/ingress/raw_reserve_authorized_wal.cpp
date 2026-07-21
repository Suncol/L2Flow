#include "l2flow/ingress/raw_reserve_authorized_wal.h"

#include "l2flow/common/identity128.h"
#include "l2flow/ingress/raw_reserve_active_activation_receipt.h"

#include <cerrno>
#include <memory>
#include <span>
#include <utility>

namespace l2flow::ingress {

class RawReserveWalAuthorizationBindingV1 final {
public:
    RawReserveWalAuthorizationBindingV1(
        ReserveRegistryStatusV1 required_status,
        bool promotable) noexcept
        : required_status_(
              static_cast<std::uint8_t>(
                  required_status)),
          promotable_(promotable) {}

    [[nodiscard]] ReserveRegistryStatusV1
    required_status() const noexcept {
        return static_cast<ReserveRegistryStatusV1>(
            required_status_.load(
                std::memory_order_acquire));
    }

    [[nodiscard]] bool active() const noexcept {
        return promotable_ &&
               required_status() ==
               ReserveRegistryStatusV1::kActive;
    }

    [[nodiscard]] bool PromoteToActive() noexcept {
        if (!promotable_) {
            return false;
        }
        std::uint8_t expected =
            required_status_.load(
                std::memory_order_acquire);
        if (expected !=
                static_cast<std::uint8_t>(
                    ReserveRegistryStatusV1::
                        kRecovering) &&
            expected !=
                static_cast<std::uint8_t>(
                    ReserveRegistryStatusV1::kInit)) {
            return false;
        }
        return required_status_.compare_exchange_strong(
            expected,
            static_cast<std::uint8_t>(
                ReserveRegistryStatusV1::kActive),
            std::memory_order_acq_rel,
            std::memory_order_acquire);
    }

private:
    std::atomic<std::uint8_t> required_status_;
    bool promotable_ = false;
};

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

[[nodiscard]] int GateErrno(
    RawReserveCoordinatorErrorV1 failure) noexcept {
    switch (failure) {
        case RawReserveCoordinatorErrorV1::kRouteNotFound:
            return ENOENT;
        case RawReserveCoordinatorErrorV1::
            kRouteIdentityMismatch:
        case RawReserveCoordinatorErrorV1::
            kActionGenerationChanged:
        case RawReserveCoordinatorErrorV1::
            kTargetUnavailable:
        case RawReserveCoordinatorErrorV1::
            kTargetIdentityChanged:
            return ESTALE;
        case RawReserveCoordinatorErrorV1::
            kRouteStatusMismatch:
            return EPERM;
        case RawReserveCoordinatorErrorV1::
            kAllocationFailure:
            return ENOMEM;
        default:
            return EIO;
    }
}

[[nodiscard]] bool CurrentWriterMatches(
    const RawReserveAuthorizedActionV1& action,
    RawReserveRegistryCoordinatorV1& coordinator,
    const RawReserveRegistryEntryKeyV1& key,
    ReserveRegistryStatusV1 required_status,
    const l2flow::common::Identity128&
        expected_writer_instance) noexcept {
    if (l2flow::common::IsZeroIdentity(
            expected_writer_instance) ||
        action.key() != key ||
        action.required_status() != required_status ||
        !action.ValidateLatest(nullptr)) {
        return false;
    }
    try {
        const ReserveCoordinatorStateV1 state =
            coordinator.state();
        if (state.selected_slot >= state.slots.size()) {
            return false;
        }
        const ReserveStateSlotV1& slot =
            state.slots[state.selected_slot];
        if (slot.coordinator_state !=
                ReserveCoordinatorPhaseV1::kProvisioned ||
            slot.entry_count > slot.entries.size()) {
            return false;
        }
        for (std::size_t index = 0U;
             index <
             static_cast<std::size_t>(slot.entry_count);
             ++index) {
            const ReserveStateEntryV1& entry =
                slot.entries[index];
            if (entry.source_stream_id ==
                    key.route.source_stream_id &&
                entry.capture_date ==
                    key.route.capture_date &&
                entry.stream_day_id ==
                    key.stream_day_id &&
                entry.executor_or_recovery_attempt ==
                    key.recovery_attempt_id &&
                entry.registry_status ==
                    required_status) {
                return entry.writer_instance ==
                       expected_writer_instance;
            }
        }
    } catch (...) {
    }
    return false;
}

class RawReserveAuthorizedWalIoV1 final
    : public RawWalIo,
      public RawReserveMutationTargetProviderV1 {
public:
    RawReserveAuthorizedWalIoV1(
        std::unique_ptr<RawWalIo> delegate,
        std::shared_ptr<
            RawReserveRegistryCoordinatorV1> coordinator,
        RawReserveRegistryEntryKeyV1 key,
        ReserveRegistryStatusV1 required_status,
        std::string stream_slug,
        RawReserveMutationTargetProviderV1*
            target_provider,
        std::shared_ptr<
            RawReserveWalAuthorizationBindingV1>
            authorization_binding,
        bool require_writer_binding,
        l2flow::common::Identity128
            expected_writer_instance) noexcept
        : delegate_(std::move(delegate)),
          coordinator_(coordinator),
          key_(key),
          required_status_(required_status),
          stream_slug_(std::move(stream_slug)),
          target_provider_(target_provider),
          authorization_binding_(
              std::move(authorization_binding)),
          require_writer_binding_(
              require_writer_binding),
          expected_writer_instance_(
              expected_writer_instance) {}

    RawWalWriteResult WritevSome(
        RawWalFile file,
        std::uint64_t offset,
        std::span<const RawWalIoVector>
            vectors) noexcept override {
        int gate_error = 0;
        auto action = Acquire(&gate_error);
        if (action == nullptr) {
            return {
                0U, gate_error == 0 ? EIO : gate_error};
        }
        return delegate_->WritevSome(
            file, offset, vectors);
    }

    int Fdatasync(
        RawWalFile file) noexcept override {
        int gate_error = 0;
        auto action = Acquire(&gate_error);
        if (action == nullptr) {
            return gate_error == 0 ? EIO : gate_error;
        }
        return delegate_->Fdatasync(file);
    }

    int Truncate(
        RawWalFile file,
        std::uint64_t logical_size) noexcept override {
        int gate_error = 0;
        auto action = Acquire(&gate_error);
        if (action == nullptr) {
            return gate_error == 0 ? EIO : gate_error;
        }
        return delegate_->Truncate(
            file, logical_size);
    }

    int Close(RawWalFile file) noexcept override {
        return delegate_->Close(file);
    }

    [[nodiscard]] int
    RawReserveMutationTargetDirectoryDescriptorV1()
        const noexcept override {
        return target_provider_ == nullptr
                   ? -1
                   : target_provider_
                         ->RawReserveMutationTargetDirectoryDescriptorV1();
    }

private:
    [[nodiscard]] std::unique_ptr<
        RawReserveAuthorizedActionV1>
    Acquire(int* error_number) noexcept {
        if (error_number != nullptr) {
            *error_number = 0;
        }
        RawReserveCoordinatorErrorV1 failure =
            RawReserveCoordinatorErrorV1::kNone;
        const ReserveRegistryStatusV1 required_status =
            authorization_binding_ == nullptr
                ? ReserveRegistryStatusV1::kUnused
                : authorization_binding_
                      ->required_status();
        auto action =
            coordinator_->AcquireActionForExistingRoute(
                key_,
                required_status,
                stream_slug_,
                &failure,
                nullptr);
        if (action == nullptr) {
            if (error_number != nullptr) {
                *error_number = GateErrno(failure);
            }
            return nullptr;
        }
        if (action->target() == nullptr ||
            target_provider_ == nullptr ||
            !ValidateRawReserveMutationTargetProviderV1(
                *target_provider_,
                *action->target())) {
            if (error_number != nullptr) {
                *error_number = EPERM;
            }
            return nullptr;
        }
        if (require_writer_binding_ &&
            !CurrentWriterMatches(
                *action,
                *coordinator_,
                key_,
                required_status,
                expected_writer_instance_)) {
            if (error_number != nullptr) {
                *error_number = ESTALE;
            }
            return nullptr;
        }
        return action;
    }

    std::unique_ptr<RawWalIo> delegate_;
    std::shared_ptr<
        RawReserveRegistryCoordinatorV1> coordinator_;
    RawReserveRegistryEntryKeyV1 key_{};
    ReserveRegistryStatusV1 required_status_ =
        ReserveRegistryStatusV1::kUnused;
    std::string stream_slug_;
    RawReserveMutationTargetProviderV1*
        target_provider_ = nullptr;
    std::shared_ptr<
        RawReserveWalAuthorizationBindingV1>
        authorization_binding_;
    bool require_writer_binding_ = false;
    l2flow::common::Identity128
        expected_writer_instance_{};
};

[[nodiscard]] bool ValidStatus(
    ReserveRegistryStatusV1 status) noexcept {
    return status ==
               ReserveRegistryStatusV1::kScaffolding ||
           status == ReserveRegistryStatusV1::kInit ||
           status ==
               ReserveRegistryStatusV1::kRecovering ||
           status == ReserveRegistryStatusV1::kActive;
}

[[nodiscard]] std::unique_ptr<RawWalIo>
GateRawWalIoImpl(
    std::unique_ptr<RawWalIo> delegate,
    RawReserveRegistryCoordinatorV1& coordinator,
    RawReserveRegistryEntryKeyV1 key,
    ReserveRegistryStatusV1 required_status,
    std::string_view stream_slug,
    bool require_writer_binding,
    l2flow::common::Identity128
        expected_writer_instance,
    std::string* error) noexcept {
    SetError(error, "");
    if (delegate == nullptr ||
        !ValidStatus(required_status) ||
        (require_writer_binding &&
         l2flow::common::IsZeroIdentity(
             expected_writer_instance))) {
        SetError(
            error,
            "invalid coordinator-gated Raw WAL I/O input");
        return nullptr;
    }
    RawReserveCoordinatorErrorV1 failure =
        RawReserveCoordinatorErrorV1::kNone;
    auto action =
        coordinator.AcquireActionForExistingRoute(
            key,
            required_status,
            stream_slug,
            &failure,
            error);
    if (action == nullptr) {
        if (error != nullptr && error->empty()) {
            SetError(
                error,
                "Raw WAL I/O coordinator action is unavailable");
        }
        return nullptr;
    }
    auto* const target_provider =
        dynamic_cast<
            RawReserveMutationTargetProviderV1*>(
            delegate.get());
    if (action->target() == nullptr ||
        target_provider == nullptr ||
        !ValidateRawReserveMutationTargetProviderV1(
            *target_provider,
            *action->target())) {
        SetError(
            error,
            "Raw WAL I/O delegate is not bound to the authorized Raw target");
        return nullptr;
    }
    if (require_writer_binding &&
        !CurrentWriterMatches(
            *action,
            coordinator,
            key,
            required_status,
            expected_writer_instance)) {
        SetError(
            error,
            "Raw WAL I/O writer instance is stale");
        return nullptr;
    }
    auto retained = coordinator.Retain();
    if (retained == nullptr) {
        SetError(
            error,
            "Raw WAL I/O coordinator lifetime is unavailable");
        return nullptr;
    }
    try {
        auto authorization_binding =
            std::make_shared<
                RawReserveWalAuthorizationBindingV1>(
                required_status,
                require_writer_binding &&
                    (required_status ==
                         ReserveRegistryStatusV1::
                             kRecovering ||
                     required_status ==
                         ReserveRegistryStatusV1::
                             kInit));
        return std::make_unique<
            RawReserveAuthorizedWalIoV1>(
            std::move(delegate),
            std::move(retained),
            key,
            required_status,
            std::string(stream_slug),
            target_provider,
            std::move(authorization_binding),
            require_writer_binding,
            expected_writer_instance);
    } catch (...) {
        SetError(
            error,
            "cannot allocate coordinator-gated Raw WAL I/O");
        return nullptr;
    }
}

}  // namespace

std::unique_ptr<RawWalIo>
GateRawWalIoWithCoordinatorV1(
    std::unique_ptr<RawWalIo> delegate,
    RawReserveRegistryCoordinatorV1& coordinator,
    RawReserveRegistryEntryKeyV1 key,
    ReserveRegistryStatusV1 required_status,
    std::string_view stream_slug,
    std::string* error) noexcept {
    return GateRawWalIoImpl(
        std::move(delegate),
        coordinator,
        key,
        required_status,
        stream_slug,
        false,
        {},
        error);
}

std::unique_ptr<RawWalIo>
GateRawWalIoWithCoordinatorForWriterV1(
    std::unique_ptr<RawWalIo> delegate,
    RawReserveRegistryCoordinatorV1& coordinator,
    RawReserveRegistryEntryKeyV1 key,
    ReserveRegistryStatusV1 required_status,
    std::string_view stream_slug,
    l2flow::common::Identity128
        expected_writer_instance,
    std::string* error) noexcept {
    return GateRawWalIoImpl(
        std::move(delegate),
        coordinator,
        key,
        required_status,
        stream_slug,
        true,
        expected_writer_instance,
        error);
}

std::string_view
RawReserveAuthorizedWalFailureV1Name(
    RawReserveAuthorizedWalFailureV1 failure) noexcept {
    switch (failure) {
        case RawReserveAuthorizedWalFailureV1::kNone:
            return "none";
        case RawReserveAuthorizedWalFailureV1::
            kInvalidArgument:
            return "invalid_argument";
        case RawReserveAuthorizedWalFailureV1::
            kActionGate:
            return "action_gate";
        case RawReserveAuthorizedWalFailureV1::
            kTargetMismatch:
            return "target_mismatch";
        case RawReserveAuthorizedWalFailureV1::
            kWriterMismatch:
            return "writer_mismatch";
        case RawReserveAuthorizedWalFailureV1::
            kDelegate:
            return "delegate";
        case RawReserveAuthorizedWalFailureV1::
            kAllocationFailure:
            return "allocation_failure";
    }
    return "unknown";
}

RawReserveAuthorizedWalStreamBackendV1::
    RawReserveAuthorizedWalStreamBackendV1(
        std::unique_ptr<RawWalStreamBackendV1> delegate,
        std::shared_ptr<
            RawReserveRegistryCoordinatorV1> coordinator,
        RawReserveRegistryEntryKeyV1 key,
        ReserveRegistryStatusV1 required_status,
        std::string stream_slug,
        RawReserveMutationTargetProviderV1*
            target_provider,
        std::shared_ptr<
            RawReserveWalAuthorizationBindingV1>
            authorization_binding,
        bool require_writer_binding,
        l2flow::common::Identity128
            expected_writer_instance) noexcept
    : delegate_(std::move(delegate)),
      coordinator_(coordinator),
      key_(key),
      required_status_(required_status),
      stream_slug_(std::move(stream_slug)),
      target_provider_(target_provider),
      authorization_binding_(
          std::move(authorization_binding)),
      require_writer_binding_(require_writer_binding),
      expected_writer_instance_(
          expected_writer_instance) {}

RawReserveAuthorizedWalStreamBackendV1::
    ~RawReserveAuthorizedWalStreamBackendV1() = default;

bool RawReserveAuthorizedWalStreamBackendV1::
active_binding_validated() const noexcept {
    return require_writer_binding_ &&
           authorization_binding_ != nullptr &&
           authorization_binding_->active();
}

std::unique_ptr<RawWalIo>
RawReserveAuthorizedWalStreamBackendV1::BindInitialIo(
    std::unique_ptr<RawWalIo> delegate,
    std::string* error) noexcept {
    SetError(error, "");
    if (delegate == nullptr ||
        initial_io_bound_ ||
        failure() !=
            RawReserveAuthorizedWalFailureV1::kNone ||
        !require_writer_binding_ ||
        authorization_binding_ == nullptr ||
        authorization_binding_->required_status() !=
            ReserveRegistryStatusV1::kInit) {
        SetError(
            error,
            "invalid or repeated Raw initial-I/O authorization binding");
        return nullptr;
    }
    auto action = Acquire();
    auto* const target_provider =
        dynamic_cast<
            RawReserveMutationTargetProviderV1*>(
            delegate.get());
    if (action == nullptr ||
        action->target() == nullptr ||
        target_provider == nullptr ||
        !ValidateRawReserveMutationTargetProviderV1(
            *target_provider,
            *action->target())) {
        Trip(
            RawReserveAuthorizedWalFailureV1::
                kTargetMismatch);
        SetError(
            error,
            "Raw initial I/O is not bound to the authorized target");
        return nullptr;
    }
    std::unique_ptr<RawWalIo> gated;
    try {
        gated = std::make_unique<
            RawReserveAuthorizedWalIoV1>(
            std::move(delegate),
            coordinator_,
            key_,
            ReserveRegistryStatusV1::kInit,
            stream_slug_,
            target_provider,
            authorization_binding_,
            true,
            expected_writer_instance_);
    } catch (...) {
    }
    if (gated == nullptr) {
        Trip(
            RawReserveAuthorizedWalFailureV1::
                kAllocationFailure);
        SetError(
            error,
            "cannot allocate Raw initial-I/O authorization binding");
        return nullptr;
    }
    initial_io_bound_ = true;
    return gated;
}

bool RawReserveAuthorizedWalStreamBackendV1::
PromoteToActive(
    std::unique_ptr<
        RawReserveActiveActivationReceiptV1>&& receipt,
    std::string* error) noexcept {
    auto owned_receipt = std::move(receipt);
    SetError(error, "");
    if (owned_receipt == nullptr ||
        failure() !=
            RawReserveAuthorizedWalFailureV1::kNone ||
        !require_writer_binding_ ||
        authorization_binding_ == nullptr ||
        (authorization_binding_->required_status() !=
             ReserveRegistryStatusV1::kRecovering &&
         authorization_binding_->required_status() !=
             ReserveRegistryStatusV1::kInit)) {
        SetError(
            error,
            "invalid Raw ACTIVE authorization promotion input");
        return false;
    }
    if (owned_receipt->key() != key_ ||
        owned_receipt->writer_instance() !=
            expected_writer_instance_) {
        Trip(
            RawReserveAuthorizedWalFailureV1::
                kWriterMismatch);
        SetError(
            error,
            "Raw ACTIVE activation receipt identity mismatch");
        return false;
    }

    RawReserveCoordinatorErrorV1 action_failure =
        RawReserveCoordinatorErrorV1::kNone;
    auto active_action =
        coordinator_->AcquireActionForExistingRoute(
            key_,
            ReserveRegistryStatusV1::kActive,
            stream_slug_,
            &action_failure,
            error);
    if (active_action == nullptr ||
        active_action->recovery_intent() !=
            ReserveRecoveryIntentV1::kResumeConnect ||
        active_action->token() !=
            owned_receipt->active_token()) {
        static_cast<void>(action_failure);
        Trip(
            RawReserveAuthorizedWalFailureV1::
                kActionGate);
        if (error != nullptr && error->empty()) {
            SetError(
                error,
                "Raw ACTIVE activation generation is unavailable");
        }
        return false;
    }
    if (active_action->target() == nullptr ||
        target_provider_ == nullptr ||
        *active_action->target() !=
            owned_receipt->target() ||
        !ValidateRawReserveMutationTargetProviderV1(
            *target_provider_,
            *active_action->target())) {
        Trip(
            RawReserveAuthorizedWalFailureV1::
                kTargetMismatch);
        SetError(
            error,
            "Raw ACTIVE activation target mismatch");
        return false;
    }
    if (!CurrentWriterMatches(
            *active_action,
            *coordinator_,
            key_,
            ReserveRegistryStatusV1::kActive,
            expected_writer_instance_)) {
        Trip(
            RawReserveAuthorizedWalFailureV1::
                kWriterMismatch);
        SetError(
            error,
            "Raw ACTIVE activation writer mismatch");
        return false;
    }
    if (!authorization_binding_->PromoteToActive()) {
        Trip(
            RawReserveAuthorizedWalFailureV1::
                kInvalidArgument);
        SetError(
            error,
            "Raw ACTIVE authorization promotion is not one-shot");
        return false;
    }
    return true;
}

std::unique_ptr<RawReserveAuthorizedActionV1>
RawReserveAuthorizedWalStreamBackendV1::Acquire() noexcept {
    if (failure() !=
        RawReserveAuthorizedWalFailureV1::kNone) {
        return nullptr;
    }
    RawReserveCoordinatorErrorV1 failure =
        RawReserveCoordinatorErrorV1::kNone;
    const ReserveRegistryStatusV1 required_status =
        authorization_binding_ == nullptr
            ? ReserveRegistryStatusV1::kUnused
            : authorization_binding_
                  ->required_status();
    auto action =
        coordinator_->AcquireActionForExistingRoute(
            key_,
            required_status,
            stream_slug_,
            &failure,
            nullptr);
    if (action == nullptr) {
        Trip(
            RawReserveAuthorizedWalFailureV1::
                kActionGate);
    } else if (action->target() == nullptr ||
               target_provider_ == nullptr ||
               !ValidateRawReserveMutationTargetProviderV1(
                   *target_provider_,
                   *action->target())) {
        Trip(
            RawReserveAuthorizedWalFailureV1::
                kTargetMismatch);
        action.reset();
    } else if (require_writer_binding_ &&
               !CurrentWriterMatches(
                   *action,
                   *coordinator_,
                   key_,
                   required_status,
                   expected_writer_instance_)) {
        Trip(
            RawReserveAuthorizedWalFailureV1::
                kWriterMismatch);
        action.reset();
    }
    return action;
}

void RawReserveAuthorizedWalStreamBackendV1::Trip(
    RawReserveAuthorizedWalFailureV1 failure) noexcept {
    std::uint8_t expected = static_cast<std::uint8_t>(
        RawReserveAuthorizedWalFailureV1::kNone);
    static_cast<void>(
        failure_.compare_exchange_strong(
            expected,
            static_cast<std::uint8_t>(failure),
            std::memory_order_acq_rel,
            std::memory_order_acquire));
}

bool RawReserveAuthorizedWalStreamBackendV1::
    PublishClosedSegment(
        RawSegmentArtifactPlanV1 plan,
        const RawWalWriterSnapshot&
            sealed_snapshot) noexcept {
    auto action = Acquire();
    if (action == nullptr) {
        return false;
    }
    if (!delegate_->PublishClosedSegment(
            std::move(plan), sealed_snapshot)) {
        Trip(
            RawReserveAuthorizedWalFailureV1::
                kDelegate);
        return false;
    }
    return true;
}

bool RawReserveAuthorizedWalStreamBackendV1::
    CreateNextSegment(
        const RawWalRotationPlan& rotation,
        std::uint64_t opened_monotonic_ns,
        RawWalNextSegmentBootstrapV1*
            bootstrap) noexcept {
    if (bootstrap == nullptr ||
        bootstrap->io != nullptr) {
        Trip(
            RawReserveAuthorizedWalFailureV1::
                kInvalidArgument);
        return false;
    }
    auto action = Acquire();
    if (action == nullptr) {
        return false;
    }
    RawWalNextSegmentBootstrapV1 candidate{};
    if (!delegate_->CreateNextSegment(
            rotation,
            opened_monotonic_ns,
            &candidate) ||
        candidate.io == nullptr) {
        Trip(
            RawReserveAuthorizedWalFailureV1::
                kDelegate);
        return false;
    }
    auto* const candidate_target =
        dynamic_cast<
            RawReserveMutationTargetProviderV1*>(
            candidate.io.get());
    if (action->target() == nullptr ||
        candidate_target == nullptr ||
        !ValidateRawReserveMutationTargetProviderV1(
            *candidate_target,
            *action->target())) {
        Trip(
            RawReserveAuthorizedWalFailureV1::
                kTargetMismatch);
        return false;
    }
    std::unique_ptr<RawWalIo> gated;
    try {
        gated = std::make_unique<
            RawReserveAuthorizedWalIoV1>(
            std::move(candidate.io),
            coordinator_,
            key_,
            authorization_binding_
                ->required_status(),
            stream_slug_,
            candidate_target,
            authorization_binding_,
            require_writer_binding_,
            expected_writer_instance_);
    } catch (...) {
    }
    if (gated == nullptr) {
        Trip(
            RawReserveAuthorizedWalFailureV1::
                kAllocationFailure);
        return false;
    }
    candidate.io = std::move(gated);
    *bootstrap = std::move(candidate);
    return true;
}

bool RawReserveAuthorizedWalStreamBackendV1::
    PublishOpenManifest(
        const SegmentHeaderV1& segment,
        const RawWalWriterSnapshot&
            initialized_snapshot) noexcept {
    auto action = Acquire();
    if (action == nullptr) {
        return false;
    }
    if (!delegate_->PublishOpenManifest(
            segment, initialized_snapshot)) {
        Trip(
            RawReserveAuthorizedWalFailureV1::
                kDelegate);
        return false;
    }
    return true;
}

bool RawReserveAuthorizedWalStreamBackendV1::
    PublishControl(
        const SegmentHeaderV1& segment,
        const RawWalWriterSnapshot&
            snapshot) noexcept {
    auto action = Acquire();
    if (action == nullptr) {
        return false;
    }
    if (!delegate_->PublishControl(
            segment, snapshot)) {
        Trip(
            RawReserveAuthorizedWalFailureV1::
                kDelegate);
        return false;
    }
    return true;
}

int RawReserveAuthorizedWalStreamBackendV1::
RawReserveMutationTargetDirectoryDescriptorV1()
    const noexcept {
    return target_provider_ == nullptr
               ? -1
               : target_provider_
                     ->RawReserveMutationTargetDirectoryDescriptorV1();
}

std::unique_ptr<
    RawReserveAuthorizedWalStreamBackendV1>
GateRawWalStreamBackendWithCoordinatorV1(
    std::unique_ptr<RawWalStreamBackendV1> delegate,
    RawReserveRegistryCoordinatorV1& coordinator,
    RawReserveRegistryEntryKeyV1 key,
    ReserveRegistryStatusV1 required_status,
    std::string_view stream_slug,
    std::string* error) noexcept {
    SetError(error, "");
    if (delegate == nullptr ||
        !ValidStatus(required_status)) {
        SetError(
            error,
            "invalid coordinator-gated Raw stream backend input");
        return nullptr;
    }
    RawReserveCoordinatorErrorV1 failure =
        RawReserveCoordinatorErrorV1::kNone;
    auto action =
        coordinator.AcquireActionForExistingRoute(
            key,
            required_status,
            stream_slug,
            &failure,
            error);
    if (action == nullptr) {
        if (error != nullptr && error->empty()) {
            SetError(
                error,
                "Raw stream backend coordinator action is unavailable");
        }
        return nullptr;
    }
    auto* const target_provider =
        dynamic_cast<
            RawReserveMutationTargetProviderV1*>(
            delegate.get());
    if (action->target() == nullptr ||
        target_provider == nullptr ||
        !ValidateRawReserveMutationTargetProviderV1(
            *target_provider,
            *action->target())) {
        SetError(
            error,
            "Raw stream backend delegate is not bound to the authorized Raw target");
        return nullptr;
    }
    auto retained = coordinator.Retain();
    if (retained == nullptr) {
        SetError(
            error,
            "Raw stream backend coordinator lifetime is unavailable");
        return nullptr;
    }
    try {
        auto authorization_binding =
            std::make_shared<
                RawReserveWalAuthorizationBindingV1>(
                required_status,
                false);
        return std::unique_ptr<
            RawReserveAuthorizedWalStreamBackendV1>(
            new RawReserveAuthorizedWalStreamBackendV1(
                std::move(delegate),
                std::move(retained),
                key,
                required_status,
                std::string(stream_slug),
                target_provider,
                std::move(authorization_binding),
                false,
                {}));
    } catch (...) {
        SetError(
            error,
            "cannot allocate coordinator-gated Raw stream backend");
        return nullptr;
    }
}

std::unique_ptr<
    RawReserveAuthorizedWalStreamBackendV1>
GateRawWalStreamBackendWithCoordinatorForWriterV1(
    std::unique_ptr<RawWalStreamBackendV1> delegate,
    RawReserveRegistryCoordinatorV1& coordinator,
    RawReserveRegistryEntryKeyV1 key,
    ReserveRegistryStatusV1 required_status,
    std::string_view stream_slug,
    l2flow::common::Identity128
        expected_writer_instance,
    std::string* error) noexcept {
    SetError(error, "");
    if (delegate == nullptr ||
        !ValidStatus(required_status) ||
        l2flow::common::IsZeroIdentity(
            expected_writer_instance)) {
        SetError(
            error,
            "invalid writer-bound Raw stream backend input");
        return nullptr;
    }
    RawReserveCoordinatorErrorV1 failure =
        RawReserveCoordinatorErrorV1::kNone;
    auto action =
        coordinator.AcquireActionForExistingRoute(
            key,
            required_status,
            stream_slug,
            &failure,
            error);
    if (action == nullptr) {
        if (error != nullptr && error->empty()) {
            SetError(
                error,
                "Raw stream backend coordinator action is unavailable");
        }
        return nullptr;
    }
    auto* const target_provider =
        dynamic_cast<
            RawReserveMutationTargetProviderV1*>(
            delegate.get());
    if (action->target() == nullptr ||
        target_provider == nullptr ||
        !ValidateRawReserveMutationTargetProviderV1(
            *target_provider,
            *action->target())) {
        SetError(
            error,
            "Raw stream backend delegate is not bound to the authorized Raw target");
        return nullptr;
    }
    if (!CurrentWriterMatches(
            *action,
            coordinator,
            key,
            required_status,
            expected_writer_instance)) {
        SetError(
            error,
            "Raw stream backend writer instance is stale");
        return nullptr;
    }
    auto retained = coordinator.Retain();
    if (retained == nullptr) {
        SetError(
            error,
            "Raw stream backend coordinator lifetime is unavailable");
        return nullptr;
    }
    try {
        auto authorization_binding =
            std::make_shared<
                RawReserveWalAuthorizationBindingV1>(
                required_status,
                required_status ==
                        ReserveRegistryStatusV1::
                            kRecovering ||
                    required_status ==
                        ReserveRegistryStatusV1::
                            kInit);
        return std::unique_ptr<
            RawReserveAuthorizedWalStreamBackendV1>(
            new RawReserveAuthorizedWalStreamBackendV1(
                std::move(delegate),
                std::move(retained),
                key,
                required_status,
                std::string(stream_slug),
                target_provider,
                std::move(authorization_binding),
                true,
                expected_writer_instance));
    } catch (...) {
        SetError(
            error,
            "cannot allocate writer-bound Raw stream backend");
        return nullptr;
    }
}

}  // namespace l2flow::ingress
