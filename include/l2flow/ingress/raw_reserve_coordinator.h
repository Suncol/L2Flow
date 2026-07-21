#pragma once

#include "l2flow/ingress/raw_namespace.h"
#include "l2flow/ingress/raw_emergency_reserve_posix.h"
#include "l2flow/ingress/raw_reserve_coordinator_gate.h"
#include "l2flow/ingress/raw_reserve_registry.h"
#include "l2flow/ingress/raw_reserve_state_posix.h"
#include "l2flow/ingress/reserve_emergency_transition_v1.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>

namespace l2flow::ingress {

enum class RawReserveCoordinatorErrorV1 : std::uint8_t {
    kNone = 0U,
    kInvalidArgument,
    kLeaseFailure,
    kStateFailure,
    kIdentityMismatch,
    kTransitionGateFailure,
    kCodecTransitionRejected,
    kStatePublishFailure,
    kRouteNotFound,
    kRouteIdentityMismatch,
    kRouteStatusMismatch,
    kActionGateFailure,
    kActionGenerationChanged,
    kTargetUnavailable,
    kTargetIdentityChanged,
    kReserveInventoryFailure,
    kReserveReleaseFailure,
    kRootSyncFailure,
    kCapacityProbeFailure,
    kAllocationFailure,
};

[[nodiscard]] std::string_view
RawReserveCoordinatorErrorNameV1(
    RawReserveCoordinatorErrorV1 error) noexcept;

class RawReserveRegistryCoordinatorV1;
class RawFinalizationContinuationCoordinatorTestPeerV1;
class SealedRawCertificateReceiptV1;
class RecoveryMaintenanceReportReceiptV1;
class RecoveryTerminalReportReceiptV1;
class RawReserveActiveActivationReceiptV1;
class RawReserveFinalizationActionV1;
class RawFinalizationContinuationReceiptV1;
class FinalizationReportReceiptV1;
class ScaffoldingFinalizationReportReceiptV1;

// Immutable identity of one existing canonical Raw stream-day target.  The
// coordinator derives this only by walking canonical components beneath its
// retained Raw-root descriptor with O_NOFOLLOW while a shared generation
// gate is held.  The action additionally retains the opened route directory;
// the scalar identities below are safe to copy into target-bound delegates.
class RawReserveMutationTargetAnchorV1 final {
public:
    [[nodiscard]] std::uint64_t
    raw_root_device() const noexcept {
        return raw_root_device_;
    }
    [[nodiscard]] std::uint64_t
    raw_root_inode() const noexcept {
        return raw_root_inode_;
    }
    [[nodiscard]] std::uint64_t
    route_device() const noexcept {
        return route_device_;
    }
    [[nodiscard]] std::uint64_t
    route_inode() const noexcept {
        return route_inode_;
    }
    [[nodiscard]] const
        RawReserveCoordinatorLeaseDigestV1&
    mount_identity_sha256() const noexcept {
        return mount_identity_sha256_;
    }
    [[nodiscard]] std::uint32_t
    source_stream_id() const noexcept {
        return source_stream_id_;
    }
    [[nodiscard]] std::uint32_t
    capture_date() const noexcept {
        return capture_date_;
    }
    [[nodiscard]] std::string_view
    stream_slug() const noexcept {
        return stream_slug_;
    }

    friend bool operator==(
        const RawReserveMutationTargetAnchorV1&,
        const RawReserveMutationTargetAnchorV1&) = default;

private:
    friend class RawReserveRegistryCoordinatorV1;

    RawReserveMutationTargetAnchorV1(
        std::uint64_t raw_root_device,
        std::uint64_t raw_root_inode,
        std::uint64_t route_device,
        std::uint64_t route_inode,
        RawReserveCoordinatorLeaseDigestV1
            mount_identity_sha256,
        std::uint32_t source_stream_id,
        std::uint32_t capture_date,
        std::string stream_slug) noexcept;

    std::uint64_t raw_root_device_ = 0U;
    std::uint64_t raw_root_inode_ = 0U;
    std::uint64_t route_device_ = 0U;
    std::uint64_t route_inode_ = 0U;
    RawReserveCoordinatorLeaseDigestV1
        mount_identity_sha256_{};
    std::uint32_t source_stream_id_ = 0U;
    std::uint32_t capture_date_ = 0U;
    std::string stream_slug_;
};

// A delegate may enter a coordinator-authorized mutation path only if it can
// prove that its retained mutation descriptors are bound to this exact
// target.  Generic RawWalIo/RawWalStreamBackendV1/RawRecoveryIo objects do
// not imply such a proof and are rejected by the authorized factories.
class RawReserveMutationTargetProviderV1 {
public:
    virtual ~RawReserveMutationTargetProviderV1() = default;

    // The descriptor must be a retained descriptor for the directory
    // relative to which this delegate performs its Raw mutations. Returning
    // a caller-supplied or unrelated descriptor violates the interface
    // contract. The authorized layer performs fstat itself; delegates do not
    // self-assert a boolean identity comparison.
    [[nodiscard]] virtual int
    RawReserveMutationTargetDirectoryDescriptorV1()
        const noexcept = 0;
};

[[nodiscard]] bool
ValidateRawReserveMutationTargetProviderV1(
    const RawReserveMutationTargetProviderV1& provider,
    const RawReserveMutationTargetAnchorV1&
        target) noexcept;

// One shared-OFD-protected filesystem action. It also implements the fresh
// namespace authorization seam, so PublishFreshRawJournalAnchor() and
// CreateInitialRawSegment() can revalidate the exact durable generation and
// registry stage immediately before every admitted mutation.
class RawReserveAuthorizedActionV1 final
    : public RawFreshMutationAuthorizationGateV1 {
public:
    ~RawReserveAuthorizedActionV1() override;

    RawReserveAuthorizedActionV1(
        const RawReserveAuthorizedActionV1&) = delete;
    RawReserveAuthorizedActionV1& operator=(
        const RawReserveAuthorizedActionV1&) = delete;
    RawReserveAuthorizedActionV1(
        RawReserveAuthorizedActionV1&&) = delete;
    RawReserveAuthorizedActionV1& operator=(
        RawReserveAuthorizedActionV1&&) = delete;

    [[nodiscard]] bool ValidateLatest(
        std::string* error = nullptr) const noexcept;

    [[nodiscard]] bool Authorizes(
        const RawFreshStateAuthorizationV1&
            facts) const noexcept override;

    [[nodiscard]] const
        RawReserveGenerationActionTokenV1&
    token() const noexcept {
        return gate_->token();
    }
    [[nodiscard]] const
        RawReserveRegistryEntryKeyV1&
    key() const noexcept {
        return key_;
    }
    [[nodiscard]] ReserveRegistryStatusV1
    required_status() const noexcept {
        return required_status_;
    }
    [[nodiscard]] ReserveRecoveryIntentV1
    recovery_intent() const noexcept {
        return recovery_intent_;
    }
    [[nodiscard]] const
        RawReserveMutationTargetAnchorV1*
    target() const noexcept {
        return target_.has_value()
                   ? &target_.value()
                   : nullptr;
    }

private:
    friend class RawReserveRegistryCoordinatorV1;

    RawReserveAuthorizedActionV1(
        std::shared_ptr<
            RawReserveRegistryCoordinatorV1> coordinator,
        std::unique_ptr<
            RawReserveGenerationActionGateV1> gate,
        RawReserveRegistryEntryKeyV1 key,
        ReserveRegistryStatusV1 required_status,
        ReserveRecoveryIntentV1 recovery_intent,
        std::size_t entry_index,
        std::optional<
            RawReserveMutationTargetAnchorV1> target,
        int route_directory_fd) noexcept;

    std::shared_ptr<
        RawReserveRegistryCoordinatorV1> coordinator_;
    std::unique_ptr<
        RawReserveGenerationActionGateV1> gate_;
    RawReserveRegistryEntryKeyV1 key_{};
    ReserveRegistryStatusV1 required_status_ =
        ReserveRegistryStatusV1::kUnused;
    ReserveRecoveryIntentV1 recovery_intent_ =
        ReserveRecoveryIntentV1::kNone;
    std::size_t entry_index_ = 0U;
    std::optional<
        RawReserveMutationTargetAnchorV1> target_;
    int route_directory_fd_ = -1;
};

// One exact CONSUMED/ACTIVE + DEBITED finalization action.  Construction is
// coordinator-only and retains an independent shared OFD generation gate for
// the complete bounded filesystem action.  The immutable grant hash, plan,
// caps and original debit generation are snapshots of the durable slot, not
// caller assertions.
class RawReserveFinalizationActionV1 final {
public:
    ~RawReserveFinalizationActionV1();

    RawReserveFinalizationActionV1(
        const RawReserveFinalizationActionV1&) = delete;
    RawReserveFinalizationActionV1& operator=(
        const RawReserveFinalizationActionV1&) = delete;
    RawReserveFinalizationActionV1(
        RawReserveFinalizationActionV1&&) = delete;
    RawReserveFinalizationActionV1& operator=(
        RawReserveFinalizationActionV1&&) = delete;

    [[nodiscard]] bool ValidateLatest(
        std::string* error = nullptr) const noexcept;
    [[nodiscard]] const
        ReserveFinalizationActionKeyV1&
    key() const noexcept {
        return key_;
    }
    [[nodiscard]] const
        RawReserveGenerationActionTokenV1&
    generation_token() const noexcept {
        return gate_->token();
    }
    [[nodiscard]] const ReserveStateV1Digest&
    immutable_grant_sha256() const noexcept {
        return immutable_grant_sha256_;
    }
    [[nodiscard]] const
        FinalizationActionPlanV1&
    plan() const noexcept {
        return plan_;
    }
    [[nodiscard]] std::uint8_t grant_flags()
        const noexcept {
        return grant_flags_;
    }
    [[nodiscard]] std::uint64_t byte_cap()
        const noexcept {
        return byte_cap_;
    }
    [[nodiscard]] std::uint32_t inode_cap()
        const noexcept {
        return inode_cap_;
    }
    [[nodiscard]] std::uint64_t debit_generation()
        const noexcept {
        return debit_generation_;
    }
    [[nodiscard]] const ReserveStateV1Identity&
    executor_instance() const noexcept {
        return executor_instance_;
    }
    [[nodiscard]] int raw_root_descriptor()
        const noexcept {
        return raw_root_directory_fd_;
    }
    // -1 for SCAFFOLDING_ONLY.  RAW_FINALIZATION and RAW_ANCHOR_ONLY always
    // retain the securely resolved canonical stream-day directory.
    [[nodiscard]] int route_directory_descriptor()
        const noexcept {
        return route_directory_fd_;
    }
    [[nodiscard]] const
        RawReserveMutationTargetAnchorV1*
    target() const noexcept {
        return target_.has_value()
                   ? &target_.value()
                   : nullptr;
    }

private:
    friend class RawReserveRegistryCoordinatorV1;

    RawReserveFinalizationActionV1(
        std::shared_ptr<
            RawReserveRegistryCoordinatorV1> coordinator,
        std::unique_ptr<
            RawReserveGenerationActionGateV1> gate,
        ReserveFinalizationActionKeyV1 key,
        ReserveStateV1Digest immutable_grant_sha256,
        FinalizationActionPlanV1 plan,
        std::uint8_t grant_flags,
        std::uint64_t byte_cap,
        std::uint32_t inode_cap,
        std::uint64_t debit_generation,
        ReserveStateV1Identity executor_instance,
        std::size_t entry_index,
        std::optional<
            RawReserveMutationTargetAnchorV1> target,
        int raw_root_directory_fd,
        int route_directory_fd) noexcept;

    std::shared_ptr<
        RawReserveRegistryCoordinatorV1> coordinator_;
    std::unique_ptr<
        RawReserveGenerationActionGateV1> gate_;
    ReserveFinalizationActionKeyV1 key_{};
    ReserveStateV1Digest immutable_grant_sha256_{};
    FinalizationActionPlanV1 plan_{};
    std::uint8_t grant_flags_ = 0U;
    std::uint64_t byte_cap_ = 0U;
    std::uint32_t inode_cap_ = 0U;
    std::uint64_t debit_generation_ = 0U;
    ReserveStateV1Identity executor_instance_{};
    std::size_t entry_index_ = 0U;
    std::optional<
        RawReserveMutationTargetAnchorV1> target_;
    int raw_root_directory_fd_ = -1;
    int route_directory_fd_ = -1;
};

// Concrete composition of the fixed flock coordinator lease, two-slot POSIX
// state store, pure registry FSM, and independent OFD generation gates.
// This class intentionally covers the PROVISIONED route lifecycle. Emergency
// RELEASING/CONSUMED grant execution has a separate state machine and cannot
// be smuggled through these normal registry methods.
class RawReserveRegistryCoordinatorV1 final
    : public std::enable_shared_from_this<
          RawReserveRegistryCoordinatorV1> {
public:
    ~RawReserveRegistryCoordinatorV1();

    RawReserveRegistryCoordinatorV1(
        const RawReserveRegistryCoordinatorV1&) =
        delete;
    RawReserveRegistryCoordinatorV1& operator=(
        const RawReserveRegistryCoordinatorV1&) =
        delete;
    RawReserveRegistryCoordinatorV1(
        RawReserveRegistryCoordinatorV1&&) = delete;
    RawReserveRegistryCoordinatorV1& operator=(
        RawReserveRegistryCoordinatorV1&&) = delete;

    [[nodiscard]] ReserveCoordinatorStateV1
    state() const;
    [[nodiscard]] std::shared_ptr<
        RawReserveRegistryCoordinatorV1>
    Retain() noexcept;

    [[nodiscard]] RawReserveCoordinatorErrorV1
    RegisterFreshScaffolding(
        const RawReserveFreshScaffoldingV1& request,
        std::string* error = nullptr) noexcept;
    [[nodiscard]] RawReserveCoordinatorErrorV1
    PublishInit(
        const RawReserveRegistryEntryKeyV1& key,
        std::string* error = nullptr) noexcept;
    [[nodiscard]] RawReserveCoordinatorErrorV1
    RegisterExistingAnchorRecovering(
        const RawReserveExistingAnchorRecoveryV1& request,
        std::string* error = nullptr) noexcept;
    [[nodiscard]] RawReserveCoordinatorErrorV1
    TakeoverScaffolding(
        const RawReserveScaffoldingTakeoverV1& request,
        std::string* error = nullptr) noexcept;
    [[nodiscard]] RawReserveCoordinatorErrorV1
    TakeoverInit(
        const RawReserveWriterTakeoverV1& request,
        std::string* error = nullptr) noexcept;
    [[nodiscard]] RawReserveCoordinatorErrorV1
    TakeoverRecovering(
        const RawReserveWriterTakeoverV1& request,
        std::string* error = nullptr) noexcept;
    [[nodiscard]] RawReserveCoordinatorErrorV1
    TakeoverActive(
        const RawReserveActiveTakeoverV1& request,
        std::string* error = nullptr) noexcept;
    [[nodiscard]] RawReserveCoordinatorErrorV1
    PublishActive(
        const RawReserveRegistryEntryKeyV1& key,
        std::string* error = nullptr) noexcept;
    // Production fresh-route activation. Unlike the compatibility status
    // transition above, this also binds the canonical target and returns the
    // one-shot capability required to promote the already-initialized INIT
    // WAL authorization binding to ACTIVE.
    [[nodiscard]] std::unique_ptr<
        RawReserveActiveActivationReceiptV1>
    PublishFreshActive(
        const RawReserveRegistryEntryKeyV1& key,
        std::string_view stream_slug,
        RawReserveCoordinatorErrorV1* failure = nullptr,
        std::string* error = nullptr) noexcept;
    // Fresh INIT is the only route allowed through PublishActive(). A
    // recovered route must instead consume the durable RESUMED_OPEN report
    // receipt below. Success returns a process-local proof of the exact
    // newly-published ACTIVE generation; the recovered WAL authorization
    // binding consumes that proof before any ACTIVE syscall can begin.
    [[nodiscard]] std::unique_ptr<
        RawReserveActiveActivationReceiptV1>
    PublishRecoveredActive(
        std::unique_ptr<
            RecoveryMaintenanceReportReceiptV1>&&
            receipt,
        RawReserveCoordinatorErrorV1* failure = nullptr,
        std::string* error = nullptr) noexcept;
    // The only normal ACTIVE unregister entry point. The opaque receipt is
    // issued after the exact terminal SealedRawCertificateV1 has completed
    // its file and directory durability/readback barriers. The transition
    // consumes it and revalidates its generation and immutable route target
    // while holding the exclusive coordinator generation gate.
    [[nodiscard]] RawReserveCoordinatorErrorV1
    UnregisterActive(
        std::unique_ptr<
            SealedRawCertificateReceiptV1>&& receipt,
        std::string* error = nullptr) noexcept;
    // Keeps a literal-null negative probe possible for callers that only
    // include this header; no default_delete for the intentionally
    // incomplete receipt type is instantiated at that call site.
    [[nodiscard]] RawReserveCoordinatorErrorV1
    UnregisterActive(
        std::nullptr_t,
        std::string* error = nullptr) noexcept;

    // Completes the ordinary RECOVERING+RECOVER_SEAL_ONLY lifecycle. The
    // opaque receipt proves an ordered durable terminal sidecar/report
    // sequence and retains the report, sidecar, journal, and maintenance
    // inodes. The coordinator consumes it under the exclusive generation
    // gate, reloads the exact RECOVERING generation, revalidates every
    // retained descriptor and canonical artifact, publishes the terminal
    // unregister state, and fsyncs the retained Raw root.
    [[nodiscard]] RawReserveCoordinatorErrorV1
    UnregisterRecoveredTerminal(
        std::unique_ptr<
            RecoveryTerminalReportReceiptV1>&&
            receipt,
        std::string* error = nullptr) noexcept;
    [[nodiscard]] RawReserveCoordinatorErrorV1
    UnregisterRecoveredTerminal(
        std::nullptr_t,
        std::string* error = nullptr) noexcept;

    // Emergency transitions use the same fixed coordinator lease, retained
    // state inode and exclusive OFD generation gate as normal registry
    // transitions.  These methods deliberately do not collect writer ACKs,
    // construct PREPARED plans or validate terminal report artifacts: those
    // are explicit caller-supplied barriers represented by the typed request
    // objects.  A successful method means the corresponding next slot was
    // validated, fsynced and read back.
    [[nodiscard]] RawReserveCoordinatorErrorV1
    PublishReleasingIntent(
        const ReserveReleaseIntentV1& request,
        std::string* error = nullptr) noexcept;
    [[nodiscard]] RawReserveCoordinatorErrorV1
    PublishReleasingPrepared(
        const ReserveReleasePreparedV1& request,
        std::string* error = nullptr) noexcept;

    struct PreparedReleaseResultV1 final {
        RawReserveCoordinatorErrorV1 coordinator_error =
            RawReserveCoordinatorErrorV1::kNone;
        RawEmergencyReservePosixErrorV1 reserve_error =
            RawEmergencyReservePosixErrorV1::kNone;
        ReserveStateV1Error codec_error =
            ReserveStateV1Error::kNone;
        bool physical_release_complete = false;
        bool consumed_state_published = false;
        bool raw_root_synced = false;

        [[nodiscard]] bool ok() const noexcept {
            return coordinator_error ==
                       RawReserveCoordinatorErrorV1::kNone &&
                   reserve_error ==
                       RawEmergencyReservePosixErrorV1::kNone &&
                   codec_error ==
                       ReserveStateV1Error::kNone &&
                   physical_release_complete &&
                   consumed_state_published &&
                   raw_root_synced;
        }
    };

    // Executes the frozen PREPARED physical order (data first, inventory in
    // descending index order), requires the injected four-dimensional
    // filesystem/quota release proof, and only then publishes the exact
    // PREPARED table as CONSUMED.  The exclusive generation gate remains
    // held for the complete release/probe/state/root-sync transaction.
    [[nodiscard]] PreparedReleaseResultV1
    ReleasePreparedAndPublishConsumed(
        RawEmergencyReserveCapacityProbeV1*
            capacity_probe,
        const RawEmergencyReserveMutationHooksV1*
            hooks = nullptr,
        std::string* error = nullptr) noexcept;

    // Durable CONSUMED grant/receipt transitions.  Filesystem actions still
    // require a separately issued action capability; these methods only
    // publish the externally proven state-machine step and therefore cannot
    // be used as a generic Raw mutation token.
    [[nodiscard]] RawReserveCoordinatorErrorV1
    ActivateNextFinalizationGrant(
        const ReserveGrantActivationV1& request,
        std::string* error = nullptr) noexcept;
    [[nodiscard]] RawReserveCoordinatorErrorV1
    DebitFinalizationAction(
        const ReserveFinalizationActionKeyV1& request,
        std::string* error = nullptr) noexcept;
    // Lower-level seam for action kinds whose production artifact consumer
    // is not specialized here. CONTINUATION is explicitly rejected and can
    // complete only through CompleteFinalizationContinuation().
    [[nodiscard]] RawReserveCoordinatorErrorV1
    CompleteFinalizationAction(
        const ReserveFinalizationActionKeyV1& request,
        std::string* error = nullptr) noexcept;
    // Production CONTINUATION completion is receipt-only. The opaque
    // receipt keeps the exact DEBITED action and retained root/route/WAL/
    // segment/manifest evidence alive until this method consumes it. The
    // shared action gate is validated and released before acquiring the
    // exclusive transition gate; state and artifacts are then revalidated
    // before COMPLETE is published. Every return path consumes the receipt.
    [[nodiscard]] RawReserveCoordinatorErrorV1
    CompleteFinalizationContinuation(
        std::unique_ptr<
            RawFinalizationContinuationReceiptV1>&&
            receipt,
        RawEmergencyReserveCapacityProbeV1*
            capacity_probe,
        std::string* error = nullptr) noexcept;
    // Durable ACTIVE->ACTIVE executor handoff for a crashed DEBITED
    // continuation.  It never creates or adopts bytes.  Replacement is
    // published only when the canonical route contains exactly one safe
    // deterministic continuation final-or-temporary candidate and no
    // competing/higher segment candidate.  The original action plan, caps,
    // debit_generation and activation baselines are preserved byte-for-byte.
    [[nodiscard]] RawReserveCoordinatorErrorV1
    ReplaceDebitedContinuationExecutor(
        const ReserveFinalizationActionKeyV1& key,
        std::string_view canonical_stream_slug,
        const ReserveStateV1Identity&
            replacement_executor,
        std::string* error = nullptr) noexcept;
    [[nodiscard]] RawReserveCoordinatorErrorV1
    FailDebitedFinalizationAction(
        const ReserveFinalizationActionKeyV1& request,
        std::string* error = nullptr) noexcept;
    [[nodiscard]] RawReserveCoordinatorErrorV1
    FailActiveFinalizationGrant(
        const ReserveGrantFailureV1& request,
        std::string* error = nullptr) noexcept;
    [[nodiscard]] RawReserveCoordinatorErrorV1
    MarkFinalizationGrantDone(
        const ReserveGrantTerminalReportV1& request,
        std::string* error = nullptr) noexcept;
    [[nodiscard]] RawReserveCoordinatorErrorV1
    CompleteReportAndMarkFinalizationGrantDone(
        const ReserveAtomicReportCompletionV1& request,
        std::string* error = nullptr) noexcept;

    // Production terminal-report completion.  Unlike the lower-level
    // codec-transition seam above, these overloads consume the publisher's
    // non-forgeable retained-fd receipt.  While holding the exclusive
    // generation gate they revalidate the exact DEBITED action, report
    // bytes/path/inode, immutable grant/model bindings and a four-dimensional
    // post-publication capacity observation before atomically publishing
    // receipt=COMPLETE + report hash + grant=DONE.
    [[nodiscard]] RawReserveCoordinatorErrorV1
    CompleteFinalizationReport(
        std::unique_ptr<
            FinalizationReportReceiptV1>&& receipt,
        RawEmergencyReserveCapacityProbeV1*
            capacity_probe,
        std::string* error = nullptr) noexcept;
    [[nodiscard]] RawReserveCoordinatorErrorV1
    CompleteScaffoldingFinalizationReport(
        std::unique_ptr<
            ScaffoldingFinalizationReportReceiptV1>&&
            receipt,
        RawEmergencyReserveCapacityProbeV1*
            capacity_probe,
        std::string* error = nullptr) noexcept;

    // Acquires the exact sole DEBITED action after locking the independent
    // shared generation-gate OFD and reloading both state slots.  The routed
    // overload is required for RAW_FINALIZATION/RAW_ANCHOR_ONLY; the
    // domain-level overload accepts only SCAFFOLDING_ONLY.
    [[nodiscard]] std::unique_ptr<
        RawReserveFinalizationActionV1>
    AcquireDebitedFinalizationAction(
        const ReserveFinalizationActionKeyV1& key,
        std::string_view stream_slug,
        RawReserveCoordinatorErrorV1* failure = nullptr,
        std::string* error = nullptr) noexcept;
    [[nodiscard]] std::unique_ptr<
        RawReserveFinalizationActionV1>
    AcquireDebitedScaffoldingAction(
        const ReserveFinalizationActionKeyV1& key,
        RawReserveCoordinatorErrorV1* failure = nullptr,
        std::string* error = nullptr) noexcept;

    // Acquires a shared OFD gate from a fresh open description, then reloads
    // the complete state and validates the exact route/status/token while the
    // gate is held. A transition racing between the pre-read and lock
    // acquisition is rejected rather than accepted on stale facts.
    [[nodiscard]] std::unique_ptr<
        RawReserveAuthorizedActionV1>
    AcquireAction(
        const RawReserveRegistryEntryKeyV1& key,
        ReserveRegistryStatusV1 required_status,
        RawReserveCoordinatorErrorV1* failure = nullptr,
        std::string* error = nullptr) noexcept;

    // Existing-route mutation capability. The slug is canonical route
    // identity, not display metadata. The action is issued only after the
    // route has been securely opened beneath this coordinator's retained
    // Raw root, both inode identities have been captured, and the durable
    // coordinator mount-identity digest has been carried into the anchor.
    [[nodiscard]] std::unique_ptr<
        RawReserveAuthorizedActionV1>
    AcquireActionForExistingRoute(
        const RawReserveRegistryEntryKeyV1& key,
        ReserveRegistryStatusV1 required_status,
        std::string_view stream_slug,
        RawReserveCoordinatorErrorV1* failure = nullptr,
        std::string* error = nullptr) noexcept;

private:
    friend class
        RawFinalizationContinuationCoordinatorTestPeerV1;
    friend std::shared_ptr<
        RawReserveRegistryCoordinatorV1>
    AttachRawReserveRegistryCoordinatorAtV1(
        int,
        const RawReserveCoordinatorLeaseMarkerV1&,
        RawReserveCoordinatorErrorV1*,
        std::string*) noexcept;
    friend std::shared_ptr<
        RawReserveRegistryCoordinatorV1>
    PublishFreshRawReserveRegistryCoordinatorAtV1(
        int,
        const RawReserveCoordinatorLeaseMarkerV1&,
        const ReserveCoordinatorStateV1&,
        RawReserveCoordinatorErrorV1*,
        std::string*) noexcept;
    friend class RawReserveAuthorizedActionV1;
    friend class RawReserveFinalizationActionV1;

    RawReserveRegistryCoordinatorV1(
        std::unique_ptr<
            RawReserveCoordinatorLeaseV1> lease,
        std::unique_ptr<RawReserveStateFileV1>
            state_file) noexcept;

    template <
        typename Request,
        ReserveStateV1Error (*Builder)(
            const ReserveCoordinatorHeaderV1&,
            const ReserveStateSlotV1&,
            const Request&,
            ReserveStateSlotV1*) noexcept>
    [[nodiscard]] RawReserveCoordinatorErrorV1
    Transition(
        const Request& request,
        std::string* error) noexcept;

    template <
        typename Request,
        ReserveStateV1Error (*Builder)(
            const ReserveCoordinatorHeaderV1&,
            const ReserveStateSlotV1&,
            const Request&,
            ReserveStateSlotV1*) noexcept>
    [[nodiscard]] RawReserveCoordinatorErrorV1
    EmergencyTransition(
        const Request& request,
        std::string* error) noexcept;

    [[nodiscard]] bool ReloadAndFind(
        const RawReserveRegistryEntryKeyV1& key,
        ReserveRegistryStatusV1 required_status,
        std::size_t* entry_index,
        std::string* error) noexcept;
    [[nodiscard]] bool ValidateActionLatest(
        const RawReserveAuthorizedActionV1& action,
        std::string* error) noexcept;
    [[nodiscard]] std::unique_ptr<
        RawReserveAuthorizedActionV1>
    AcquireActionImpl(
        const RawReserveRegistryEntryKeyV1& key,
        ReserveRegistryStatusV1 required_status,
        const std::string* stream_slug,
        RawReserveCoordinatorErrorV1* failure,
        std::string* error) noexcept;
    [[nodiscard]] std::unique_ptr<
        RawReserveFinalizationActionV1>
    AcquireDebitedFinalizationActionImpl(
        const ReserveFinalizationActionKeyV1& key,
        const std::string* stream_slug,
        RawReserveCoordinatorErrorV1* failure,
        std::string* error) noexcept;
    [[nodiscard]] bool
    ValidateFinalizationActionLatest(
        const RawReserveFinalizationActionV1& action,
        std::string* error) noexcept;
    [[nodiscard]] bool ValidateTargetLatest(
        const RawReserveAuthorizedActionV1& action,
        std::string* error) const noexcept;
    [[nodiscard]] bool SyncRawRoot(
        std::string* error) const noexcept;
    [[nodiscard]] RawReserveCoordinatorErrorV1
    CompleteFinalizationReportImpl(
        FinalizationReportReceiptV1* routed_receipt,
        ScaffoldingFinalizationReportReceiptV1*
            scaffolding_receipt,
        RawEmergencyReserveCapacityProbeV1*
            capacity_probe,
        std::string* error) noexcept;

    std::unique_ptr<RawReserveCoordinatorLeaseV1>
        lease_;
    std::unique_ptr<RawReserveStateFileV1>
        state_file_;
    mutable std::mutex state_mutex_;
};

[[nodiscard]] std::shared_ptr<
    RawReserveRegistryCoordinatorV1>
AttachRawReserveRegistryCoordinatorAtV1(
    int retained_raw_root_fd,
    const RawReserveCoordinatorLeaseMarkerV1&
        expected_marker,
    RawReserveCoordinatorErrorV1* failure = nullptr,
    std::string* error = nullptr) noexcept;

[[nodiscard]] std::shared_ptr<
    RawReserveRegistryCoordinatorV1>
PublishFreshRawReserveRegistryCoordinatorAtV1(
    int retained_raw_root_fd,
    const RawReserveCoordinatorLeaseMarkerV1&
        expected_marker,
    const ReserveCoordinatorStateV1& bootstrap,
    RawReserveCoordinatorErrorV1* failure = nullptr,
    std::string* error = nullptr) noexcept;

template <
    typename Request,
    ReserveStateV1Error (*Builder)(
        const ReserveCoordinatorHeaderV1&,
        const ReserveStateSlotV1&,
        const Request&,
        ReserveStateSlotV1*) noexcept>
RawReserveCoordinatorErrorV1
RawReserveRegistryCoordinatorV1::Transition(
    const Request& request,
    std::string* error) noexcept {
    RawReserveCoordinatorGateError gate_failure =
        RawReserveCoordinatorGateError::kNone;
    auto transition_guard =
        AcquireRawReserveCoordinatorTransitionGuardAtV1(
            lease_->root_directory_descriptor(),
            lease_->anchor(),
            &gate_failure,
            error);
    if (transition_guard == nullptr) {
        return RawReserveCoordinatorErrorV1::
            kTransitionGateFailure;
    }

    std::lock_guard<std::mutex> state_lock(
        state_mutex_);
    ReserveStateV1Error codec_error =
        ReserveStateV1Error::kNone;
    if (state_file_->Reload(
            &codec_error, error) !=
        RawReserveStatePosixError::kNone) {
        return RawReserveCoordinatorErrorV1::
            kStateFailure;
    }
    const ReserveCoordinatorStateV1& state =
        state_file_->state();
    if (state.selected_slot >= state.slots.size()) {
        return RawReserveCoordinatorErrorV1::
            kStateFailure;
    }
    const ReserveStateSlotV1& before =
        state.slots[state.selected_slot];
    ReserveStateSlotV1 candidate{};
    codec_error = Builder(
        state.header, before, request, &candidate);
    if (codec_error != ReserveStateV1Error::kNone) {
        if (error != nullptr) {
            try {
                *error =
                    "reserve registry transition rejected: " +
                    std::string(
                        ReserveStateV1ErrorName(
                            codec_error));
            } catch (...) {
            }
        }
        return RawReserveCoordinatorErrorV1::
            kCodecTransitionRejected;
    }
    if (state_file_->PublishNext(
            candidate, &codec_error, error) !=
        RawReserveStatePosixError::kNone) {
        return RawReserveCoordinatorErrorV1::
            kStatePublishFailure;
    }
    return RawReserveCoordinatorErrorV1::kNone;
}

template <
    typename Request,
    ReserveStateV1Error (*Builder)(
        const ReserveCoordinatorHeaderV1&,
        const ReserveStateSlotV1&,
        const Request&,
        ReserveStateSlotV1*) noexcept>
RawReserveCoordinatorErrorV1
RawReserveRegistryCoordinatorV1::EmergencyTransition(
    const Request& request,
    std::string* error) noexcept {
    RawReserveCoordinatorGateError gate_failure =
        RawReserveCoordinatorGateError::kNone;
    auto transition_guard =
        AcquireRawReserveCoordinatorTransitionGuardAtV1(
            lease_->root_directory_descriptor(),
            lease_->anchor(),
            &gate_failure,
            error);
    if (transition_guard == nullptr) {
        return RawReserveCoordinatorErrorV1::
            kTransitionGateFailure;
    }

    std::lock_guard<std::mutex> state_lock(
        state_mutex_);
    ReserveStateV1Error codec_error =
        ReserveStateV1Error::kNone;
    if (state_file_->Reload(
            &codec_error, error) !=
        RawReserveStatePosixError::kNone) {
        return RawReserveCoordinatorErrorV1::
            kStateFailure;
    }
    const ReserveCoordinatorStateV1& state =
        state_file_->state();
    if (state.selected_slot >= state.slots.size()) {
        return RawReserveCoordinatorErrorV1::
            kStateFailure;
    }

    ReserveStateSlotV1 candidate{};
    codec_error = Builder(
        state.header,
        state.slots[state.selected_slot],
        request,
        &candidate);
    if (codec_error != ReserveStateV1Error::kNone) {
        if (error != nullptr) {
            try {
                *error =
                    "emergency reserve transition rejected: " +
                    std::string(
                        ReserveStateV1ErrorName(
                            codec_error));
            } catch (...) {
            }
        }
        return RawReserveCoordinatorErrorV1::
            kCodecTransitionRejected;
    }
    if (state_file_->PublishNext(
            candidate, &codec_error, error) !=
        RawReserveStatePosixError::kNone) {
        return RawReserveCoordinatorErrorV1::
            kStatePublishFailure;
    }
    if (!SyncRawRoot(error)) {
        return RawReserveCoordinatorErrorV1::
            kRootSyncFailure;
    }
    return RawReserveCoordinatorErrorV1::kNone;
}

}  // namespace l2flow::ingress
