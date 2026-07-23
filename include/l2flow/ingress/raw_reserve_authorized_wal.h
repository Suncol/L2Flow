#pragma once

#include "l2flow/common/identity128.h"
#include "l2flow/ingress/raw_reserve_active_activation_receipt.h"
#include "l2flow/ingress/raw_reserve_coordinator.h"
#include "l2flow/ingress/raw_wal_stream.h"

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

namespace l2flow::ingress {

class RawReserveWalAuthorizationBindingV1;
class RawWriterLease;

// Coordinator fence for segment/journal writes. Outside an explicit bounded
// mutation batch, a new independent shared OFD action is acquired for every
// mutating syscall and released immediately afterward. Inside a batch, the
// wrapper reuses one still-live shared action while revalidating the target
// descriptor before every mutation. Close is deliberately unconditional:
// fencing must never prevent descriptor cleanup.
//
// The delegate must expose a retained target-directory descriptor through
// RawReserveMutationTargetProviderV1. The returned wrapper retains the
// coordinator lifetime itself.
[[nodiscard]] std::unique_ptr<RawWalIo>
GateRawWalIoWithCoordinatorV1(
    std::unique_ptr<RawWalIo> delegate,
    RawReserveRegistryCoordinatorV1& coordinator,
    RawReserveRegistryEntryKeyV1 key,
    ReserveRegistryStatusV1 required_status,
    std::string_view stream_slug,
    std::string* error = nullptr) noexcept;

// Production writer-bound variant. In addition to key/status/target, every
// action must observe this exact current registry writer_instance while its
// shared generation gate is held. A same-key/same-status writer takeover
// therefore waits for any bounded batch and fences the old writer before its
// next action.
[[nodiscard]] std::unique_ptr<RawWalIo>
GateRawWalIoWithCoordinatorForWriterV1(
    std::unique_ptr<RawWalIo> delegate,
    RawReserveRegistryCoordinatorV1& coordinator,
    RawReserveRegistryEntryKeyV1 key,
    ReserveRegistryStatusV1 required_status,
    std::string_view stream_slug,
    l2flow::common::Identity128 expected_writer_instance,
    std::string* error = nullptr) noexcept;

enum class RawReserveAuthorizedWalFailureV1
    : std::uint8_t {
    kNone = 0U,
    kInvalidArgument,
    kActionGate,
    kTargetMismatch,
    kWriterMismatch,
    kDelegate,
    kAllocationFailure,
};

[[nodiscard]] std::string_view
RawReserveAuthorizedWalFailureV1Name(
    RawReserveAuthorizedWalFailureV1 failure) noexcept;

// A sink accepted by production runtime composition. Implementations must
// share one writer-bound authorization state across their backend and every
// existing/future RawWalIo, and that state must have consumed a valid ACTIVE
// activation receipt before active_binding_validated() can become true.
class RawActiveBoundWalSinkV1
    : public RawWalSink,
      public RawReserveMutationTargetProviderV1 {
public:
    ~RawActiveBoundWalSinkV1() override = default;

    [[nodiscard]] virtual const
        RawReserveRegistryEntryKeyV1&
    authorization_key() const noexcept = 0;
    [[nodiscard]] virtual const
        l2flow::common::Identity128&
    authorized_writer_instance() const noexcept = 0;
    [[nodiscard]] virtual bool
    active_binding_validated() const noexcept = 0;
    // The production clean-stop barrier borrows the same locked lease owned
    // by the sink. RawIngressApp destroys its clean-stop gate before the
    // sink, so this reference remains valid for the complete barrier.
    [[nodiscard]] virtual RawWriterLease&
    retained_writer_lease() noexcept = 0;
};

// Transaction fence for R6-R14 backend operations. Outside a bounded mutation
// batch, one shared OFD action is held for each complete delegate method.
// Inside a batch, the same still-live shared action covers every method until
// EndMutationBatch(). I/O returned by CreateNextSegment is automatically
// wrapped by the same policy, so rotation cannot reintroduce an ungated
// writer.
//
// The delegate and every rotated RawWalIo it returns must expose the exact
// retained target-directory descriptor. The returned wrapper retains the
// coordinator lifetime itself.
class RawReserveAuthorizedWalStreamBackendV1 final
    : public RawWalStreamBackendV1,
      public RawReserveMutationTargetProviderV1 {
public:
    ~RawReserveAuthorizedWalStreamBackendV1() override;

    RawReserveAuthorizedWalStreamBackendV1(
        const RawReserveAuthorizedWalStreamBackendV1&) =
        delete;
    RawReserveAuthorizedWalStreamBackendV1& operator=(
        const RawReserveAuthorizedWalStreamBackendV1&) =
        delete;
    RawReserveAuthorizedWalStreamBackendV1(
        RawReserveAuthorizedWalStreamBackendV1&&) =
        delete;
    RawReserveAuthorizedWalStreamBackendV1& operator=(
        RawReserveAuthorizedWalStreamBackendV1&&) =
        delete;

    [[nodiscard]] bool BeginMutationBatch() noexcept override;
    void EndMutationBatch() noexcept override;

    [[nodiscard]] bool PublishClosedSegment(
        RawSegmentArtifactPlanV1 plan,
        const RawWalWriterSnapshot&
            sealed_snapshot) noexcept override;
    [[nodiscard]] bool CreateNextSegment(
        const RawWalRotationPlan& rotation,
        std::uint64_t opened_monotonic_ns,
        RawWalNextSegmentBootstrapV1*
            bootstrap) noexcept override;
    [[nodiscard]] bool PublishOpenManifest(
        const SegmentHeaderV1& segment,
        const RawWalWriterSnapshot&
            initialized_snapshot) noexcept override;
    [[nodiscard]] bool PublishControl(
        const SegmentHeaderV1& segment,
        const RawWalWriterSnapshot&
            snapshot) noexcept override;
    [[nodiscard]] int
    RawReserveMutationTargetDirectoryDescriptorV1()
        const noexcept override;

    [[nodiscard]] RawReserveAuthorizedWalFailureV1
    failure() const noexcept {
        return static_cast<
            RawReserveAuthorizedWalFailureV1>(
            failure_.load(std::memory_order_acquire));
    }
    [[nodiscard]] const RawReserveRegistryEntryKeyV1&
    authorization_key() const noexcept {
        return key_;
    }
    [[nodiscard]] const
        l2flow::common::Identity128&
    authorized_writer_instance() const noexcept {
        return expected_writer_instance_;
    }
    [[nodiscard]] bool
    active_binding_validated() const noexcept;

    // One-shot initial-I/O adoption for a freshly initialized segment. The
    // returned per-syscall wrapper shares this backend's exact INIT/ACTIVE
    // authorization binding; promoting the backend therefore cannot leave
    // the already-open segment or journal fenced on the old INIT generation.
    [[nodiscard]] std::unique_ptr<RawWalIo>
    BindInitialIo(
        std::unique_ptr<RawWalIo> delegate,
        std::string* error = nullptr) noexcept;

    // One-way local authorization promotion after the coordinator has
    // durably entered ACTIVE. The opaque receipt is the only accepted input;
    // an action, key, status flag or caller boolean cannot substitute for it.
    [[nodiscard]] bool PromoteToActive(
        std::unique_ptr<
            RawReserveActiveActivationReceiptV1>&& receipt,
        std::string* error = nullptr) noexcept;

private:
    friend std::unique_ptr<
        RawReserveAuthorizedWalStreamBackendV1>
    GateRawWalStreamBackendWithCoordinatorV1(
        std::unique_ptr<RawWalStreamBackendV1>,
        RawReserveRegistryCoordinatorV1&,
        RawReserveRegistryEntryKeyV1,
        ReserveRegistryStatusV1,
        std::string_view,
        std::string*) noexcept;
    friend std::unique_ptr<
        RawReserveAuthorizedWalStreamBackendV1>
    GateRawWalStreamBackendWithCoordinatorForWriterV1(
        std::unique_ptr<RawWalStreamBackendV1>,
        RawReserveRegistryCoordinatorV1&,
        RawReserveRegistryEntryKeyV1,
        ReserveRegistryStatusV1,
        std::string_view,
        l2flow::common::Identity128,
        std::string*) noexcept;

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
            expected_writer_instance) noexcept;

    [[nodiscard]] std::unique_ptr<
        RawReserveAuthorizedActionV1>
    Acquire() noexcept;
    [[nodiscard]] bool ValidateAction(
        const RawReserveAuthorizedActionV1& action) noexcept;
    [[nodiscard]] RawReserveAuthorizedActionV1*
    ActionForMutation(
        std::unique_ptr<RawReserveAuthorizedActionV1>* owned) noexcept;
    void Trip(
        RawReserveAuthorizedWalFailureV1 failure) noexcept;

    std::unique_ptr<RawWalStreamBackendV1> delegate_;
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
    bool initial_io_bound_ = false;
    l2flow::common::Identity128
        expected_writer_instance_{};
    std::unique_ptr<RawReserveAuthorizedActionV1>
        mutation_batch_action_;
    std::atomic<std::uint8_t> failure_{
        static_cast<std::uint8_t>(
            RawReserveAuthorizedWalFailureV1::kNone)};
};

[[nodiscard]] std::unique_ptr<
    RawReserveAuthorizedWalStreamBackendV1>
GateRawWalStreamBackendWithCoordinatorV1(
    std::unique_ptr<RawWalStreamBackendV1> delegate,
    RawReserveRegistryCoordinatorV1& coordinator,
    RawReserveRegistryEntryKeyV1 key,
    ReserveRegistryStatusV1 required_status,
    std::string_view stream_slug,
    std::string* error = nullptr) noexcept;

// Production backend variant with the same writer-generation binding as the
// per-syscall wrapper above. Every rotated RawWalIo inherits the binding.
[[nodiscard]] std::unique_ptr<
    RawReserveAuthorizedWalStreamBackendV1>
GateRawWalStreamBackendWithCoordinatorForWriterV1(
    std::unique_ptr<RawWalStreamBackendV1> delegate,
    RawReserveRegistryCoordinatorV1& coordinator,
    RawReserveRegistryEntryKeyV1 key,
    ReserveRegistryStatusV1 required_status,
    std::string_view stream_slug,
    l2flow::common::Identity128 expected_writer_instance,
    std::string* error = nullptr) noexcept;

}  // namespace l2flow::ingress
