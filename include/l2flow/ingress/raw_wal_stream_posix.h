#pragma once

#include "l2flow/common/identity128.h"
#include "l2flow/ingress/raw_control_file.h"
#include "l2flow/ingress/raw_manifest_v1.h"
#include "l2flow/ingress/raw_reserve_authorized_wal.h"
#include "l2flow/ingress/raw_reserve_coordinator.h"
#include "l2flow/ingress/raw_wal_stream.h"
#include "l2flow/ingress/raw_writer_lease.h"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

namespace l2flow::ingress {

inline constexpr std::size_t
    kRawWalStreamDefaultMaximumManifestBytesV1 =
        16U * 1024U * 1024U;

using RawWalStreamBackendClockNowV1 =
    std::uint64_t (*)(void* context) noexcept;

struct RawPosixWalStreamBackendOptionsV1 final {
    l2flow::common::Identity128 writer_instance{};
    std::uint64_t segment_preallocation_bytes = 0U;
    std::size_t maximum_manifest_bytes =
        kRawWalStreamDefaultMaximumManifestBytesV1;

    // The realtime callback supplies the immutable creation timestamp for
    // each rotated SegmentHeaderV1. The heartbeat callback supplies the
    // host-monotonic timestamp published in control.page. Null callbacks use
    // CLOCK_REALTIME and CLOCK_MONOTONIC respectively.
    RawWalStreamBackendClockNowV1 realtime_now = nullptr;
    void* realtime_clock_context = nullptr;
    RawWalStreamBackendClockNowV1 monotonic_now = nullptr;
    void* monotonic_clock_context = nullptr;
};

// Exact, already-durable recovery frontier from which normal ingestion may
// resume.  The factory below independently reloads the published closed-only
// manifest and securely reads the retained terminal segment and journal; this
// model is therefore a comparison input, not permission to mutate a path.
struct RawRecoveredClosedWalStateV1 final {
    RawManifestV1 closed_manifest{};
    RawV1JournalHeaderWire journal_header_wire{};
    SegmentHeaderV1 terminal_segment{};
    RawWalCursor accepted_sealed_cursor{};
    std::uint64_t journal_logical_size = 0U;
};

enum class RawPosixWalStreamBackendFailureV1
    : std::uint8_t {
    kNone = 0U,
    kInvalidConfiguration,
    kManifestLoad,
    kInvalidState,
    kClosedSnapshotMismatch,
    kJournalVerification,
    kArtifactPublish,
    kManifestTransition,
    kManifestPublish,
    kRotationMismatch,
    kClockFailure,
    kBootstrapCreate,
    kIoAdoption,
    kOpenSnapshotMismatch,
    kControlPublish,
};

[[nodiscard]] const char*
RawPosixWalStreamBackendFailureV1Name(
    RawPosixWalStreamBackendFailureV1 failure) noexcept;

// Production POSIX implementation of the RawWalStreamBackendV1 R6-R14
// persistence boundary. The caller must already hold the stream-day writer
// lease and must already have created/adopted the initial bootstrap
// descriptors. This type never creates a fresh namespace and never grants
// coordinator permission.
//
// All mutating methods are single-writer calls. A first failure permanently
// trips the backend; no later operation is allowed to publish additional
// state.
class RawPosixWalStreamBackendV1 final
    : public RawWalStreamBackendV1,
      public RawReserveMutationTargetProviderV1 {
public:
    ~RawPosixWalStreamBackendV1() override;

    RawPosixWalStreamBackendV1(
        const RawPosixWalStreamBackendV1&) = delete;
    RawPosixWalStreamBackendV1& operator=(
        const RawPosixWalStreamBackendV1&) = delete;
    RawPosixWalStreamBackendV1(
        RawPosixWalStreamBackendV1&&) = delete;
    RawPosixWalStreamBackendV1& operator=(
        RawPosixWalStreamBackendV1&&) = delete;

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
        const noexcept override {
        return lease_.directory_descriptor();
    }

    [[nodiscard]] RawPosixWalStreamBackendFailureV1
    failure() const noexcept {
        return static_cast<
            RawPosixWalStreamBackendFailureV1>(
            failure_.load(std::memory_order_acquire));
    }
    [[nodiscard]] int error_number() const noexcept {
        return error_number_.load(
            std::memory_order_acquire);
    }
    [[nodiscard]] const RawManifestV1*
    current_manifest() const noexcept {
        return have_manifest_ ? &manifest_ : nullptr;
    }
    [[nodiscard]] const RawControlSnapshot*
    last_control_snapshot() const noexcept {
        return have_control_snapshot_
                   ? &last_control_snapshot_
                   : nullptr;
    }

private:
    friend std::unique_ptr<
        RawPosixWalStreamBackendV1>
    CreateRawPosixWalStreamBackendV1(
        RawWriterLease&,
        const RawWalWriterConfig&,
        RawPosixWalStreamBackendOptionsV1,
        std::string*) noexcept;
    friend std::unique_ptr<
        class RawRecoveredClosedPosixStreamV1>
    CreateRecoveredClosedRawPosixStreamV1(
        std::unique_ptr<RawWriterLease>,
        RawRecoveredClosedWalStateV1,
        RawPosixWalStreamBackendOptionsV1,
        RawSegmentArtifactOptionsV1,
        RawWalStreamLimitsV1,
        std::uint64_t,
        RawReserveRegistryCoordinatorV1&,
        RawReserveRegistryEntryKeyV1,
        std::string_view,
        std::string*) noexcept;

    enum class Phase : std::uint8_t {
        kAwaitOpen = 0U,
        kOpen,
        kClosed,
    };

    RawPosixWalStreamBackendV1(
        RawWriterLease& lease,
        RawPosixWalStreamBackendOptionsV1 options,
        RawV1JournalHeaderWire journal_header,
        RawManifestNamespaceV1 namespace_identity,
        SegmentHeaderV1 initial_segment,
        std::uint64_t expected_initialized_journal_size,
        RawManifestV1 initial_manifest,
        bool have_initial_manifest) noexcept;

    [[nodiscard]] bool Failed() const noexcept;
    void Fail(
        RawPosixWalStreamBackendFailureV1 failure,
        int error_number) noexcept;
    [[nodiscard]] bool ReadClock(
        bool realtime,
        std::uint64_t* value) noexcept;
    [[nodiscard]] bool VerifyJournalSeal(
        const RawSegmentArtifactPlanV1& plan,
        const RawWalWriterSnapshot&
            sealed_snapshot) noexcept;
    [[nodiscard]] bool ValidateOpenSnapshot(
        const SegmentHeaderV1& segment,
        const RawWalWriterSnapshot&
            snapshot) const noexcept;
    [[nodiscard]] bool ValidateSealedSnapshot(
        const RawSegmentArtifactPlanV1& plan,
        const RawWalWriterSnapshot&
            snapshot) const noexcept;
    [[nodiscard]] bool ControlPathStillNamesWriter()
        const noexcept;

    RawWriterLease& lease_;
    RawPosixWalStreamBackendOptionsV1 options_{};
    RawV1JournalHeaderWire journal_header_{};
    RawManifestNamespaceV1 namespace_identity_{};
    SegmentHeaderV1 current_segment_{};
    RawManifestV1 manifest_{};
    bool have_manifest_ = false;
    Phase phase_ = Phase::kAwaitOpen;
    std::uint64_t expected_initialized_journal_size_ = 0U;
    std::uint64_t closed_journal_logical_size_ = 0U;
    std::unique_ptr<RawControlFileWriter>
        control_writer_;
    RawControlSnapshot last_control_snapshot_{};
    bool have_control_snapshot_ = false;
    std::uint64_t last_heartbeat_monotonic_ns_ = 0U;

    std::atomic<std::uint8_t> failure_{
        static_cast<std::uint8_t>(
            RawPosixWalStreamBackendFailureV1::kNone)};
    std::atomic<int> error_number_{0};
};

// Validates the exact already-published initial header/config against the
// retained lease and journal header, and loads an existing closed manifest
// when the initial config attaches a rotated segment. Fresh segment 1
// requires manifest.json to be absent. The returned backend does not own the
// lease or the initial RawWalIo.
[[nodiscard]] std::unique_ptr<
    RawPosixWalStreamBackendV1>
CreateRawPosixWalStreamBackendV1(
    RawWriterLease& lease,
    const RawWalWriterConfig& initial_writer_config,
    RawPosixWalStreamBackendOptionsV1 options,
    std::string* error = nullptr) noexcept;

// Exact outcome of the production fresh INIT -> ACTIVE composition.  ACTIVE
// publication is not a reversible operation: a failure or process death
// around the state-file publish can leave the durable registry ACTIVE even
// when no process-local sink capability was returned.  Keeping this state
// explicit prevents callers from treating every null sink as a safe retry.
enum class RawFreshActivePublicationStateV1
    : std::uint8_t {
    kNotPublished = 0U,
    kPublicationIndeterminate,
    kPublishedAwaitingLocalPromotion,
    kActiveBound,
};

enum class RawFreshActivePosixStreamFailureV1
    : std::uint8_t {
    kNone = 0U,
    kInvalidInput,
    kInitAuthorization,
    kExistingManifest,
    kInitialSegmentCreate,
    kIoAdoption,
    kBackendCreate,
    kBackendAuthorization,
    kInitialIoBinding,
    kStreamConstruction,
    kStreamInitialize,
    kOwningStreamAllocation,
    kActivePublication,
    kActivePromotion,
};

[[nodiscard]] std::string_view
RawFreshActivePosixStreamFailureV1Name(
    RawFreshActivePosixStreamFailureV1 failure) noexcept;

struct RawFreshActivePosixStreamResultV1 final {
    std::unique_ptr<RawActiveBoundWalSinkV1> sink;
    RawFreshActivePosixStreamFailureV1 failure =
        RawFreshActivePosixStreamFailureV1::kNone;
    RawFreshActivePublicationStateV1
        publication_state =
            RawFreshActivePublicationStateV1::
                kNotPublished;
    RawReserveCoordinatorErrorV1
        coordinator_failure =
            RawReserveCoordinatorErrorV1::kNone;
    RawReserveAuthorizedWalFailureV1
        authorization_failure =
            RawReserveAuthorizedWalFailureV1::kNone;
    RawWalFailure wal_failure{};
    // True only after CreateInitialRawSegment() completed its final-name and
    // parent-directory barriers. It remains true on every later failure.
    bool initial_segment_published = false;

    [[nodiscard]] bool ok() const noexcept {
        return sink != nullptr &&
               failure ==
                   RawFreshActivePosixStreamFailureV1::
                       kNone &&
               publication_state ==
                   RawFreshActivePublicationStateV1::
                       kActiveBound &&
               sink->active_binding_validated();
    }
    [[nodiscard]] bool requires_fail_stop()
        const noexcept {
        return !ok() &&
               (publication_state ==
                    RawFreshActivePublicationStateV1::
                        kPublicationIndeterminate ||
                publication_state ==
                    RawFreshActivePublicationStateV1::
                        kPublishedAwaitingLocalPromotion);
    }
};

// Production fresh-route factory. The caller supplies the retained writer
// lease and SCAFFOLDING journal anchor after the coordinator has durably
// entered INIT. The logical writer config must still have
// headers_already_persisted=false; this factory alone raises that assertion
// after CreateInitialRawSegment() succeeds.
//
// The complete ordering is:
//   INIT action -> initial segment -> target-bound POSIX I/O ->
//   writer-bound INIT backend/I/O -> RawWalStreamWriter::Initialize() ->
//   durable fresh ACTIVE receipt -> shared binding promotion.
//
// All owning runtime resources are allocated before ACTIVE publication. A
// post-publication promotion failure returns no sink and requires fail-stop;
// this function never creates a Subscriber and never calls Connect().
[[nodiscard]] RawFreshActivePosixStreamResultV1
CreateFreshActiveRawPosixStreamV1(
    std::unique_ptr<RawWriterLease> lease,
    std::unique_ptr<RawFreshJournalAnchor> journal_anchor,
    RawWalWriterConfig logical_writer_config,
    RawPosixWalStreamBackendOptionsV1 backend_options,
    RawSegmentArtifactOptionsV1 artifact_options,
    RawWalStreamLimitsV1 stream_limits,
    RawReserveRegistryCoordinatorV1& coordinator,
    RawReserveRegistryEntryKeyV1 key,
    std::string_view stream_slug,
    std::string* error = nullptr) noexcept;

// Owns the complete reference chain for one initialized post-recovery stream:
//
//   RawWalStreamWriter -> authorized backend -> POSIX backend -> writer lease
//
// It implements the eventual ACTIVE sink capability so the same owning
// object can be handed to RawIngressApp after receipt-gated promotion.
// active_binding_validated() remains false while this is a RECOVERING
// resource, and production composition rejects it in that state.
class RawRecoveredClosedPosixStreamV1 final
    : public RawActiveBoundWalSinkV1 {
public:
    ~RawRecoveredClosedPosixStreamV1() override;

    RawRecoveredClosedPosixStreamV1(
        const RawRecoveredClosedPosixStreamV1&) = delete;
    RawRecoveredClosedPosixStreamV1& operator=(
        const RawRecoveredClosedPosixStreamV1&) = delete;
    RawRecoveredClosedPosixStreamV1(
        RawRecoveredClosedPosixStreamV1&&) = delete;
    RawRecoveredClosedPosixStreamV1& operator=(
        RawRecoveredClosedPosixStreamV1&&) = delete;

    [[nodiscard]] bool AppendRecord(
        const RawWalRecordInputV1& input) noexcept override;
    [[nodiscard]] bool BeginMutationBatch() noexcept override;
    void EndMutationBatch() noexcept override;
    [[nodiscard]] bool FlushDurable() noexcept override;
    [[nodiscard]] bool SealAndClose() noexcept override;
    [[nodiscard]] RawWalWriterSnapshot
    Snapshot() const noexcept override;
    [[nodiscard]] RawWalFailure
    failure() const noexcept override;
    [[nodiscard]] RawWalSinkIdentityV1
    identity() const noexcept override;
    [[nodiscard]] int
    RawReserveMutationTargetDirectoryDescriptorV1()
        const noexcept override {
        return stream_directory_descriptor();
    }
    [[nodiscard]] const
        RawReserveRegistryEntryKeyV1&
    authorization_key() const noexcept override {
        return backend_->authorization_key();
    }
    [[nodiscard]] const
        l2flow::common::Identity128&
    authorized_writer_instance()
        const noexcept override {
        return backend_
            ->authorized_writer_instance();
    }
    [[nodiscard]] bool
    active_binding_validated()
        const noexcept override {
        return backend_
            ->active_binding_validated();
    }
    [[nodiscard]] RawWriterLease&
    retained_writer_lease() noexcept override {
        return *lease_;
    }

    [[nodiscard]] RawWriterLease& lease() noexcept {
        return *lease_;
    }
    [[nodiscard]] const RawWriterLease&
    lease() const noexcept {
        return *lease_;
    }
    [[nodiscard]] int stream_directory_descriptor()
        const noexcept {
        return lease_->directory_descriptor();
    }
    [[nodiscard]] RawReserveAuthorizedWalFailureV1
    authorization_failure() const noexcept {
        return backend_->failure();
    }

private:
    friend std::unique_ptr<
        RawRecoveredClosedPosixStreamV1>
    CreateRecoveredClosedRawPosixStreamV1(
        std::unique_ptr<RawWriterLease>,
        RawRecoveredClosedWalStateV1,
        RawPosixWalStreamBackendOptionsV1,
        RawSegmentArtifactOptionsV1,
        RawWalStreamLimitsV1,
        std::uint64_t,
        RawReserveRegistryCoordinatorV1&,
        RawReserveRegistryEntryKeyV1,
        std::string_view,
        std::string*) noexcept;
    friend std::unique_ptr<
        RawActiveBoundWalSinkV1>
    PromoteRecoveredClosedRawPosixStreamToActiveV1(
        std::unique_ptr<
            RawRecoveredClosedPosixStreamV1>&&,
        std::unique_ptr<
            RawReserveActiveActivationReceiptV1>&&,
        std::string*) noexcept;

    RawRecoveredClosedPosixStreamV1(
        std::unique_ptr<RawWriterLease> lease,
        std::unique_ptr<
            RawReserveAuthorizedWalStreamBackendV1>
            backend,
        std::unique_ptr<RawWalStreamWriter> stream) noexcept;

    // Declaration order is intentional: destruction runs stream, backend,
    // then lease, so every stored reference remains valid.
    std::unique_ptr<RawWriterLease> lease_;
    std::unique_ptr<
        RawReserveAuthorizedWalStreamBackendV1>
        backend_;
    std::unique_ptr<RawWalStreamWriter> stream_;
};

// RECOVERING+RESUME_CONNECT composition for an existing route.  It accepts
// only a closed-only manifest whose terminal header, exact SEGMENT_SEALED
// cursor, journal header/size and retained POSIX files all agree.  It then
// derives the normal rotation plan, creates the next segment through a
// coordinator-authorized backend action, and publishes the initialized open
// manifest/control boundary through separately authorized actions.
//
// The returned stream deliberately remains a RECOVERING resource.  This
// function does not publish a RecoveryMaintenanceReport, transition the route
// to ACTIVE, or connect the SDK.
[[nodiscard]] std::unique_ptr<
    RawRecoveredClosedPosixStreamV1>
CreateRecoveredClosedRawPosixStreamV1(
    std::unique_ptr<RawWriterLease> lease,
    RawRecoveredClosedWalStateV1 recovered,
    RawPosixWalStreamBackendOptionsV1 backend_options,
    RawSegmentArtifactOptionsV1 artifact_options,
    RawWalStreamLimitsV1 stream_limits,
    std::uint64_t opened_monotonic_ns,
    RawReserveRegistryCoordinatorV1& coordinator,
    RawReserveRegistryEntryKeyV1 key,
    std::string_view stream_slug,
    std::string* error = nullptr) noexcept;

// Consumes both inputs. Success returns the same owning stream through the
// stronger ACTIVE-bound sink capability; failure returns null and destroys
// the still-unpublished runtime resources. No action/key/boolean overload is
// provided.
[[nodiscard]] std::unique_ptr<
    RawActiveBoundWalSinkV1>
PromoteRecoveredClosedRawPosixStreamToActiveV1(
    std::unique_ptr<
        RawRecoveredClosedPosixStreamV1>&&
        recovering_stream,
    std::unique_ptr<
        RawReserveActiveActivationReceiptV1>&& receipt,
    std::string* error = nullptr) noexcept;

}  // namespace l2flow::ingress
