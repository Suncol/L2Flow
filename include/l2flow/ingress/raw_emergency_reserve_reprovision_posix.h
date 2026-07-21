#pragma once

#include "l2flow/ingress/finalization_archive_source_cleanup_posix.h"
#include "l2flow/ingress/raw_emergency_reserve_posix.h"
#include "l2flow/ingress/raw_reserve_coordinator_gate.h"

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

namespace l2flow::ingress {

enum class RawEmergencyReserveReprovisionErrorV1
    : std::uint8_t {
    kNone = 0U,
    kInvalidArgument,
    kCleanupBarrierInvalid,
    kBootstrapInvalid,
    kOldStateChanged,
    kUnsafeRoot,
    kNoDurableCandidate,
    kCandidateConflict,
    kInventoryConflict,
    kAllocationProofFailure,
    kCapacityProbeFailure,
    kIoFailure,
    kSyncFailure,
    kPublishConflict,
    kInjectedInterruption,
    // The atomic fixed-state replacement syscall was admitted.  The caller
    // must stop and rediscover the fixed state from disk; the old cleanup
    // capability is intentionally destroyed and is never returned.
    kPostReplaceFailStop,
    kAllocationFailure,
};

[[nodiscard]] std::string_view
RawEmergencyReserveReprovisionErrorV1Name(
    RawEmergencyReserveReprovisionErrorV1 error) noexcept;

enum class RawEmergencyReserveReprovisionMutationPointV1
    : std::uint8_t {
    kCandidateSetReset = 1U,
    kDataCandidateSynced = 2U,
    kInodePublished = 3U,
    kInventorySynced = 4U,
    kStateCandidateSynced = 5U,
    kDataPublished = 6U,
    // This is the final retryable hook.  The old fixed state and cleanup
    // receipt are still authoritative when it runs.
    kBeforeStateReplace = 7U,
    // This hook runs only after the replacement syscall and root fsync.  A
    // rejection is fail-stop and cannot return the old cleanup receipt.
    kStateReplaced = 8U,
    kReadbackComplete = 9U,
};

using RawEmergencyReserveReprovisionMutationHookV1 =
    bool (*)(
        RawEmergencyReserveReprovisionMutationPointV1 point,
        std::uint32_t inode_index,
        void* context) noexcept;

struct RawEmergencyReserveReprovisionHooksV1 final {
    RawEmergencyReserveReprovisionMutationHookV1 after =
        nullptr;
    void* context = nullptr;
};

struct RawEmergencyReserveReprovisionRequestV1 final {
    // Exact byte-identical generation=1 PROVISIONED duplicate bootstrap.
    ReserveCoordinatorStateV1 bootstrap{};

    // Caller-owned conservative metadata/commit headroom.  Both values must
    // be nonzero and are added to the complete reserve plus state-candidate
    // charge for the mandatory four-dimensional pre-provision probe.
    std::uint64_t metadata_margin_bytes = 0U;
    std::uint64_t metadata_margin_inodes = 0U;
};

struct RawEmergencyReserveReprovisionDiscoveryResultV1 final {
    RawEmergencyReserveReprovisionErrorV1 error =
        RawEmergencyReserveReprovisionErrorV1::kNone;
    ReserveStateV1Identity candidate_reserve_state_uuid{};
    RawEmergencyReserveReprovisionRequestV1 request{};

    [[nodiscard]] bool ok() const noexcept {
        return error ==
                   RawEmergencyReserveReprovisionErrorV1::
                       kNone &&
               !l2flow::common::IsZeroIdentity(
                   candidate_reserve_state_uuid);
    }
};

// Builds the only bootstrap shape accepted by reprovision.  It copies the
// old immutable allocation domain and sizing facts, changes only the
// reserve UUID and its deterministic inventory commitment, and emits two
// byte-identical generation=1 PROVISIONED slots.
[[nodiscard]] RawEmergencyReservePosixErrorV1
BuildRawEmergencyReserveReprovisionBootstrapV1(
    const ReserveCoordinatorHeaderV1& old_header,
    const ReserveStateV1Identity& new_reserve_state_uuid,
    ReserveCoordinatorStateV1* bootstrap,
    ReserveStateV1Error* codec_error = nullptr,
    std::string* diagnostic = nullptr) noexcept;

// Restart-only, read-only discovery for a durable pre-replacement candidate
// set.  The cleanup receipt must have been reconstructed from the final
// archive and absent-source barriers while the fixed state is still the
// exact old all-DONE state.  The function accepts exactly one canonical
// non-old UUID across typed data/state/inventory candidate/final artifacts,
// validates every visible artifact against a bootstrap derived from that
// UUID and the old immutable allocation domain, and returns the exact request
// required to resume.  It never generates a replacement UUID and never
// deletes, publishes, fsyncs, or otherwise mutates an artifact.
[[nodiscard]]
RawEmergencyReserveReprovisionDiscoveryResultV1
DiscoverRawEmergencyReserveReprovisionCandidateV1(
    const FinalizationArchiveSourceCleanupReceiptV1&
        cleanup_receipt,
    std::uint64_t metadata_margin_bytes,
    std::uint64_t metadata_margin_inodes,
    std::string* diagnostic = nullptr) noexcept;

class RawEmergencyReserveReprovisionReceiptV1 final {
public:
    ~RawEmergencyReserveReprovisionReceiptV1();

    RawEmergencyReserveReprovisionReceiptV1(
        const RawEmergencyReserveReprovisionReceiptV1&) =
        delete;
    RawEmergencyReserveReprovisionReceiptV1& operator=(
        const RawEmergencyReserveReprovisionReceiptV1&) =
        delete;
    RawEmergencyReserveReprovisionReceiptV1(
        RawEmergencyReserveReprovisionReceiptV1&&) =
        delete;
    RawEmergencyReserveReprovisionReceiptV1& operator=(
        RawEmergencyReserveReprovisionReceiptV1&&) =
        delete;

    [[nodiscard]] const ReserveStateV1Identity&
    old_reserve_state_uuid() const noexcept {
        return old_reserve_state_uuid_;
    }
    [[nodiscard]] const ReserveStateV1Identity&
    new_reserve_state_uuid() const noexcept {
        return new_reserve_state_uuid_;
    }
    [[nodiscard]] int retained_old_state_descriptor()
        const noexcept;
    [[nodiscard]] const RawReserveStateFileV1&
    new_state_file() const noexcept {
        return *new_state_file_;
    }
    [[nodiscard]] const RawEmergencyReserveInventoryV1&
    new_inventory() const noexcept {
        return *new_inventory_;
    }

    // Read-only revalidation.  It proves that the archive and maintenance
    // lease capabilities are still retained, the old open descriptor still
    // contains the archived all-DONE evidence but is no longer the fixed
    // name, and the fixed name plus complete data/inventory are the exact
    // new bootstrap.
    [[nodiscard]] bool Validate(
        std::string* diagnostic = nullptr) const noexcept;

private:
    friend struct RawEmergencyReserveReprovisionAccessV1;
    friend struct RawEmergencyReserveReprovisionEngineV1;

    RawEmergencyReserveReprovisionReceiptV1(
        std::unique_ptr<
            FinalizationArchiveSourceCleanupReceiptV1>
            cleanup_receipt,
        ReserveStateV1Identity old_reserve_state_uuid,
        ReserveStateV1Identity new_reserve_state_uuid,
        ReserveStateV1HeaderWire old_state_header,
        ReserveStateV1SlotWire old_all_done_slot,
        RawReserveCoordinatorLeaseMarkerWireV1
            maintenance_lease_marker,
        ReserveStateV1FileWire new_state_wire,
        ReserveCoordinatorStateV1 bootstrap,
        std::unique_ptr<RawReserveStateFileV1>
            new_state_file,
        std::unique_ptr<RawEmergencyReserveInventoryV1>
            new_inventory) noexcept;

    std::unique_ptr<
        FinalizationArchiveSourceCleanupReceiptV1>
        cleanup_receipt_;
    ReserveStateV1Identity old_reserve_state_uuid_{};
    ReserveStateV1Identity new_reserve_state_uuid_{};
    ReserveStateV1HeaderWire old_state_header_{};
    ReserveStateV1SlotWire old_all_done_slot_{};
    RawReserveCoordinatorLeaseMarkerWireV1
        maintenance_lease_marker_{};
    ReserveStateV1FileWire new_state_wire_{};
    ReserveCoordinatorStateV1 bootstrap_{};
    std::unique_ptr<RawReserveStateFileV1>
        new_state_file_;
    std::unique_ptr<RawEmergencyReserveInventoryV1>
        new_inventory_;
};

struct RawEmergencyReserveReprovisionResultV1 final {
    RawEmergencyReserveReprovisionErrorV1 error =
        RawEmergencyReserveReprovisionErrorV1::kNone;

    // True as soon as the atomic replacement syscall is admitted.  It is
    // therefore true on success too.  When the overall result is a failure,
    // true also requires fail_stop_required and forbids returning the stale
    // old-state retry_cleanup_receipt.
    bool state_replace_may_have_occurred = false;
    bool fail_stop_required = false;

    std::unique_ptr<
        FinalizationArchiveSourceCleanupReceiptV1>
        retry_cleanup_receipt;
    std::unique_ptr<
        RawEmergencyReserveReprovisionReceiptV1>
        receipt;

    [[nodiscard]] bool ok() const noexcept {
        return error ==
                   RawEmergencyReserveReprovisionErrorV1::
                       kNone &&
               !fail_stop_required && receipt != nullptr;
    }
};

// Offline all-DONE reprovision.  The cleanup receipt is the only admission
// capability and is consumed on entry.  Before the state-replacement
// syscall, a fail-closed result returns it only when it still validates.
// Once replacement is admitted, the capability is destroyed and callers
// must perform state-first disk rediscovery.
[[nodiscard]] RawEmergencyReserveReprovisionResultV1
ReprovisionRawEmergencyReserveV1(
    std::unique_ptr<
        FinalizationArchiveSourceCleanupReceiptV1>
        cleanup_receipt,
    const RawEmergencyReserveReprovisionRequestV1& request,
    RawEmergencyReserveCapacityProbeV1* capacity_probe,
    const RawEmergencyReserveReprovisionHooksV1*
        hooks = nullptr,
    std::string* diagnostic = nullptr) noexcept;

}  // namespace l2flow::ingress
