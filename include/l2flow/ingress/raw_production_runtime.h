#pragma once

#include "l2flow/ingress/raw_fresh_route_posix.h"
#include "l2flow/ingress/raw_ingress_app.h"
#include "l2flow/ingress/raw_live_tail_posix.h"
#include "l2flow/ingress/raw_recovery_maintenance_report_store.h"
#include "l2flow/ingress/raw_reserve_authorized_wal.h"
#include "l2flow/ingress/raw_reserve_coordinator.h"
#include "l2flow/ingress/raw_wal_stream_posix.h"

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

namespace l2flow::ingress {

// Immutable service-composition identity captured before RawIngressApp owns
// its moved configuration.  The SourceFrontier pointer is an identity only;
// callers cannot use this accessor to mutate app state.  A null page/zero
// generation is valid for a standalone Raw runtime but is rejected by the
// four-source ProductionServiceV1 composition gate.
struct RawProductionCaptureBindingV1 final {
    std::uint8_t source_slot = UINT8_MAX;
    l2flow::sdk::IngressKind ingress_kind =
        l2flow::sdk::IngressKind::ShSnapshot;
    std::uint32_t source_stream_id = 0U;
    std::uint32_t capture_date = 0U;
    l2flow::common::Identity128 stream_day_id{};
    l2flow::common::Identity128 writer_instance{};
    std::uint64_t source_generation = 0U;
    l2flow::canonical::SourceFrontierPageV1* source_frontier = nullptr;
    // Runtime-only composition identity. It is never persisted or hashed.
    void* fast_capture_context = nullptr;
};

enum class RawProductionRuntimeBlockerV1
    : std::uint8_t {
    kNone = 0U,
    // Assess() has no route/recovery inputs and therefore cannot manufacture
    // a runtime. Call ActivateRecoveredClosed() or AdoptAlreadyActive() with
    // explicit typed resources.
    kExplicitActivationInputsRequired,
};

enum class RawProductionRuntimeFailureV1
    : std::uint8_t {
    kNone = 0U,
    kInvalidInput,
    kActiveActionMismatch,
    kWriterMismatch,
    kTargetMismatch,
    kRecoveryReportPublication,
    kCoordinatorActivation,
    kWalActivationPromotion,
    kFreshRouteActivation,
    kFreshActiveIdentityMismatch,
    kLiveTailSourceOpen,
    kCleanStopGateConstruction,
    kLiveTailAttach,
    kAllocationFailure,
};

class RawProductionRuntimeV1;

struct RawProductionRuntimeBuildResultV1 final {
    RawProductionRuntimeFailureV1 failure =
        RawProductionRuntimeFailureV1::kNone;
    RawProductionRuntimeBlockerV1 blocker =
        RawProductionRuntimeBlockerV1::kNone;
    RawLiveTailError live_tail_error =
        RawLiveTailError::kNone;
    RecoveryMaintenanceReportStoreErrorV1
        recovery_report_error =
            RecoveryMaintenanceReportStoreErrorV1::
                kNone;
    RawReserveCoordinatorErrorV1 coordinator_error =
        RawReserveCoordinatorErrorV1::kNone;
    RawFreshRoutePosixFailureV1 fresh_route_failure =
        RawFreshRoutePosixFailureV1::kNone;
    RawFreshActivePosixStreamFailureV1
        fresh_active_failure =
            RawFreshActivePosixStreamFailureV1::kNone;
    RawFreshActivePublicationStateV1
        fresh_active_publication_state =
            RawFreshActivePublicationStateV1::
                kNotPublished;
    bool fresh_init_publication_attempted = false;
    // A fresh composition which crossed durable INIT, or any composition
    // which consumed already-ACTIVE resources without returning the owning
    // runtime, must be abandoned in-process. Recovery/takeover is the only
    // retry path.
    bool fail_stop_required = false;
    std::unique_ptr<RawProductionRuntimeV1> runtime;

    [[nodiscard]] bool ok() const noexcept {
        return failure ==
                   RawProductionRuntimeFailureV1::kNone &&
               blocker ==
                   RawProductionRuntimeBlockerV1::kNone &&
               runtime != nullptr;
    }
    [[nodiscard]] bool requires_fail_stop()
        const noexcept {
        return fail_stop_required;
    }
};

// One service-monitor sample. The runtime, rather than its caller, owns the
// only RawLiveTailPosixSource and therefore performs the fresh coherent
// control-page read before evaluating the Phase-2 observational gate.
// A failed control read never reuses a previous READY decision.
struct RawProductionReadinessSampleV1 final {
    RawControlSnapshot sampled_control{};
    RawObservationalGateResult gate{};
    std::uint64_t control_generation = 0U;
    int control_error = 0;
    bool sampled = false;

    [[nodiscard]] bool ok() const noexcept {
        return sampled && control_error == 0;
    }
};

// Owns the POSIX source which RawLiveTail borrows and the RawIngressApp which
// owns that tail and its prepared sink. Declaration order makes app teardown
// (including worker joins and sink close) precede source destruction.
class RawProductionRuntimeV1 final {
public:
    ~RawProductionRuntimeV1();

    RawProductionRuntimeV1(
        const RawProductionRuntimeV1&) = delete;
    RawProductionRuntimeV1& operator=(
        const RawProductionRuntimeV1&) = delete;
    RawProductionRuntimeV1(
        RawProductionRuntimeV1&&) = delete;
    RawProductionRuntimeV1& operator=(
        RawProductionRuntimeV1&&) = delete;

    [[nodiscard]] bool Initialize(
        std::string* error = nullptr) noexcept;
    [[nodiscard]] bool Stop(
        std::string* error = nullptr) noexcept;
    // Reads a new coherent control snapshot from the retained POSIX source
    // on every call, then evaluates readiness against that exact snapshot.
    // No cached READY value is returned when the control source is missing,
    // stale, replaced, or otherwise unreadable.
    [[nodiscard]] RawProductionReadinessSampleV1
    SampleReadiness(
        std::uint64_t now_monotonic_ns) const noexcept;
    [[nodiscard]] RawIngressApp& app() noexcept {
        return *app_;
    }
    [[nodiscard]] const RawIngressApp&
    app() const noexcept {
        return *app_;
    }

    // Fresh production composition needs a second, independent Raw
    // single-consumer cursor for the source-order decode pipeline.  The
    // readiness worker owned by RawIngressApp already consumes its own tail;
    // sharing that cursor would violate RawLiveTail's single-consumer
    // contract.  ActivateFreshRegistered() prepares this additional tail at
    // the exact same authenticated header-only boundary.
    //
    // This is a one-shot ownership transfer.  The returned tail continues to
    // borrow pipeline_live_tail_source_, so this runtime must outlive the
    // ProductionSourcePipelineV1 which receives it.  Recovered/adopted
    // runtimes do not manufacture this capability and return nullptr.
    [[nodiscard]] std::unique_ptr<RawLiveTail>
    TakeFreshPipelineLiveTail() noexcept;
    [[nodiscard]] bool HasFreshPipelineLiveTail() const noexcept {
        return pipeline_live_tail_ != nullptr;
    }
    [[nodiscard]] const RawProductionCaptureBindingV1& capture_binding()
        const noexcept {
        return capture_binding_;
    }

private:
    friend class
        RawExistingRouteProductionRuntimeFactoryV1;

    RawProductionRuntimeV1(
        std::unique_ptr<RawLiveTailPosixSource>
            live_tail_source,
        std::unique_ptr<RawIngressApp> app,
        RawProductionCaptureBindingV1 capture_binding) noexcept;

    [[nodiscard]] bool InstallFreshPipelineLiveTail(
        std::unique_ptr<RawLiveTailPosixSource> source,
        std::unique_ptr<RawLiveTail> tail) noexcept;

    std::unique_ptr<RawLiveTailPosixSource>
        live_tail_source_;
    // source_ is declared before tail_ so the local pending tail is destroyed
    // first.  After TakeFreshPipelineLiveTail(), the service-level owner must
    // preserve this runtime until its pipeline has stopped and been destroyed.
    std::unique_ptr<RawLiveTailPosixSource>
        pipeline_live_tail_source_;
    std::unique_ptr<RawLiveTail> pipeline_live_tail_;
    std::unique_ptr<RawIngressApp> app_;
    const RawProductionCaptureBindingV1 capture_binding_{};
};

// This is deliberately split into:
//   (1) ActivateRecoveredClosed(), which completes report publication,
//       receipt-gated RECOVERING->ACTIVE, WAL binding promotion and runtime
//       construction; and
//   (2) ActivateFreshRegistered(), which owns the complete already-registered
//       SCAFFOLDING -> ACTIVE -> live-tail/clean-stop/runtime composition; and
//   (3) AdoptAlreadyActive(), a lifetime-safe seam for a caller that already
//       holds the exact ACTIVE resources.
class RawExistingRouteProductionRuntimeFactoryV1 final {
public:
    [[nodiscard]] static
    RawProductionRuntimeBuildResultV1
    Assess() noexcept;

    // Completes one already durable fresh SCAFFOLDING registration without
    // exposing the intermediate ACTIVE sink. All deployment/runtime inputs
    // remain explicit. The factory derives both live-tail attach structures
    // from the initialized sink snapshot, securely opens its POSIX source,
    // builds a clean-stop gate which borrows the sink-owned locked lease, and
    // finally transfers the entire lifetime chain to RawProductionRuntimeV1.
    //
    // Registration and zero-mutation route discovery remain the caller's
    // responsibility. A non-ok result with requires_fail_stop()==true must
    // never be retried in the same process.
    [[nodiscard]] static
    RawProductionRuntimeBuildResultV1
    ActivateFreshRegistered(
        const std::string& raw_root,
        std::string_view stream_slug,
        const RawReserveFreshScaffoldingV1& registration,
        RawReserveRegistryCoordinatorV1& coordinator,
        RawWalWriterConfig logical_writer_config,
        RawPosixWalStreamBackendOptionsV1 backend_options,
        RawSegmentArtifactOptionsV1 artifact_options,
        RawWalStreamLimitsV1 stream_limits,
        RawIngressAppConfigV1 config,
        std::shared_ptr<l2flow::sdk::SdkFactory>
            sdk_factory,
        std::unique_ptr<CaptureClock> clock,
        RawLiveTailPosixLimitsV1 live_tail_limits = {},
        RawIngressAppOptionsV1 options = {},
        RawIngressLifecycleObserver*
            lifecycle_observer = nullptr,
        std::string* error = nullptr) noexcept;

    // Formal deployment entry. stable_raw_root is retained only as the
    // already-hashed configuration identity; no filesystem operation resolves
    // it. Every Raw mutation is rooted at retained_raw_root_fd, which must be
    // the same inode retained by coordinator. The descriptor remains owned by
    // the caller.
    [[nodiscard]] static
    RawProductionRuntimeBuildResultV1
    ActivateFreshRegisteredAt(
        int retained_raw_root_fd,
        const std::string& stable_raw_root,
        std::string_view stream_slug,
        const RawReserveFreshScaffoldingV1& registration,
        RawReserveRegistryCoordinatorV1& coordinator,
        RawWalWriterConfig logical_writer_config,
        RawPosixWalStreamBackendOptionsV1 backend_options,
        RawSegmentArtifactOptionsV1 artifact_options,
        RawWalStreamLimitsV1 stream_limits,
        RawIngressAppConfigV1 config,
        std::shared_ptr<l2flow::sdk::SdkFactory> sdk_factory,
        std::unique_ptr<CaptureClock> clock,
        RawLiveTailPosixLimitsV1 live_tail_limits = {},
        RawIngressAppOptionsV1 options = {},
        RawIngressLifecycleObserver* lifecycle_observer = nullptr,
        std::string* error = nullptr) noexcept;

    [[nodiscard]] static
    RawProductionRuntimeBuildResultV1
    ActivateRecoveredClosed(
        std::unique_ptr<
            RawRecoveredClosedPosixStreamV1>&&
            recovering_stream,
        const BuiltRecoveryMaintenanceReportV1& report,
        RawReserveRegistryCoordinatorV1& coordinator,
        RawReserveRegistryEntryKeyV1 key,
        std::string_view stream_slug,
        RawIngressAppConfigV1 config,
        std::shared_ptr<l2flow::sdk::SdkFactory>
            sdk_factory,
        std::unique_ptr<CaptureClock> clock,
        std::unique_ptr<RawLiveTailPosixSource>
            live_tail_source,
        RawLiveTailAttachV1 live_tail_attach,
        std::unique_ptr<RawIngressCleanStopGateV1>
            clean_stop_gate,
        RawIngressAppOptionsV1 options = {},
        RawIngressLifecycleObserver*
            lifecycle_observer = nullptr) noexcept;

    [[nodiscard]] static
    RawProductionRuntimeBuildResultV1
    AdoptAlreadyActive(
        std::unique_ptr<
            RawReserveAuthorizedActionV1> active_action,
        RawReserveRegistryCoordinatorV1& coordinator,
        RawIngressAppConfigV1 config,
        std::shared_ptr<l2flow::sdk::SdkFactory>
            sdk_factory,
        std::unique_ptr<CaptureClock> clock,
        std::unique_ptr<RawActiveBoundWalSinkV1>
            prepared_sink,
        std::unique_ptr<RawLiveTailPosixSource>
            live_tail_source,
        RawLiveTailAttachV1 live_tail_attach,
        std::unique_ptr<RawIngressCleanStopGateV1>
            clean_stop_gate,
        RawIngressAppOptionsV1 options = {},
        RawIngressLifecycleObserver*
            lifecycle_observer = nullptr) noexcept;
};

}  // namespace l2flow::ingress
