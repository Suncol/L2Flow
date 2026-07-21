#pragma once

#include "l2flow/ingress/finalization_archive_posix_store.h"
#include "l2flow/ingress/raw_reserve_state_posix.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace l2flow::ingress {

enum class FinalizationArchiveSourceCleanupErrorV1
    : std::uint8_t {
    kNone = 0U,
    kInvalidArgument,
    kArchiveBarrierInvalid,
    kUnsafeRawRoot,
    kInvalidSourceLocator,
    kDuplicateSourceMapping,
    kMaintenanceLeaseMissing,
    kMaintenanceLeaseUnsafe,
    kMaintenanceLeaseBusy,
    kMaintenanceMarkerMismatch,
    kStateAttachFailure,
    kStateMismatch,
    kStateNotAllDone,
    kSourceParentMissing,
    kUnsafeSourceParent,
    kSourceConflict,
    kCandidateLimitExceeded,
    kUnlinkFailure,
    kSyncFailure,
    kInjectedInterruption,
    kReadbackFailure,
    kAllocationFailure,
};

[[nodiscard]] std::string_view
FinalizationArchiveSourceCleanupErrorV1Name(
    FinalizationArchiveSourceCleanupErrorV1 error) noexcept;

enum class FinalizationArchiveSourceCleanupMutationPointV1
    : std::uint8_t {
    kBeforeParentSyncForAbsentSource = 1U,
    kAfterParentSyncBeforeAbsentRecheck = 2U,
    kBeforeSourceUnlink = 3U,
    kAfterSourceUnlinkBeforeParentSync = 4U,
    kBeforeFinalReadback = 5U,
};

struct FinalizationArchiveSourceCleanupHooksV1 final {
    bool (*allow)(
        FinalizationArchiveSourceCleanupMutationPointV1,
        std::size_t source_index,
        void*) noexcept = nullptr;
    void* context = nullptr;
};

class FinalizationArchiveSourceCleanupReceiptV1 final {
public:
    ~FinalizationArchiveSourceCleanupReceiptV1();

    FinalizationArchiveSourceCleanupReceiptV1(
        const FinalizationArchiveSourceCleanupReceiptV1&) =
        delete;
    FinalizationArchiveSourceCleanupReceiptV1& operator=(
        const FinalizationArchiveSourceCleanupReceiptV1&) =
        delete;
    FinalizationArchiveSourceCleanupReceiptV1(
        FinalizationArchiveSourceCleanupReceiptV1&&) =
        delete;
    FinalizationArchiveSourceCleanupReceiptV1& operator=(
        FinalizationArchiveSourceCleanupReceiptV1&&) =
        delete;

    [[nodiscard]] const RawV1Identity&
    reserve_state_uuid() const noexcept {
        return reserve_state_uuid_;
    }
    [[nodiscard]] const RawV1Identity&
    finalization_cycle_id() const noexcept {
        return finalization_cycle_id_;
    }
    [[nodiscard]] const RawV1Digest&
    archive_manifest_sha256() const noexcept {
        return archive_manifest_sha256_;
    }
    [[nodiscard]] std::size_t source_count()
        const noexcept {
        return sources_.size();
    }

    // Pure validation of the retained authoritative archive plus every
    // source-parent identity and current source-name absence.  Historical
    // directory-fsync completion is represented only by this unforgeable
    // receipt; Validate itself performs no fsync or mutation.
    [[nodiscard]] bool Validate(
        std::string* diagnostic = nullptr) const noexcept;

    // Public only as concrete private-constructor storage.
    struct RetainedSourceV1 final {
        RetainedSourceV1() noexcept = default;
        ~RetainedSourceV1();
        RetainedSourceV1(const RetainedSourceV1&) =
            delete;
        RetainedSourceV1& operator=(
            const RetainedSourceV1&) = delete;
        RetainedSourceV1(
            RetainedSourceV1&& other) noexcept;
        RetainedSourceV1& operator=(
            RetainedSourceV1&& other) noexcept;

        int parent_directory_fd = -1;
        bool audit_rooted = false;
        std::string parent_locator;
        std::string filename;
        std::uint64_t parent_device = 0U;
        std::uint64_t parent_inode = 0U;
    };

private:
    friend struct
        FinalizationArchiveSourceCleanupAccessV1;
    friend struct
        RawEmergencyReserveReprovisionAccessV1;

    FinalizationArchiveSourceCleanupReceiptV1(
        RawV1Identity reserve_state_uuid,
        RawV1Identity finalization_cycle_id,
        RawV1Digest archive_manifest_sha256,
        int raw_root_fd,
        std::uint64_t raw_root_device,
        std::uint64_t raw_root_inode,
        int maintenance_lease_fd,
        std::uint64_t maintenance_lease_device,
        std::uint64_t maintenance_lease_inode,
        ReserveStateV1HeaderWire frozen_state_header,
        ReserveStateV1SlotWire frozen_all_done_slot,
        RawV1Digest frozen_state_sha256,
        std::unique_ptr<RawReserveStateFileV1>
            state_file,
        std::unique_ptr<
            PublishedFinalizationArchiveReceiptV1>
            archive_receipt,
        std::vector<RetainedSourceV1> sources) noexcept;

    RawV1Identity reserve_state_uuid_{};
    RawV1Identity finalization_cycle_id_{};
    RawV1Digest archive_manifest_sha256_{};
    int raw_root_fd_ = -1;
    std::uint64_t raw_root_device_ = 0U;
    std::uint64_t raw_root_inode_ = 0U;
    int maintenance_lease_fd_ = -1;
    std::uint64_t maintenance_lease_device_ = 0U;
    std::uint64_t maintenance_lease_inode_ = 0U;
    ReserveStateV1HeaderWire frozen_state_header_{};
    ReserveStateV1SlotWire frozen_all_done_slot_{};
    RawV1Digest frozen_state_sha256_{};
    mutable std::unique_ptr<RawReserveStateFileV1>
        state_file_;
    std::unique_ptr<
        PublishedFinalizationArchiveReceiptV1>
        archive_receipt_;
    std::vector<RetainedSourceV1> sources_;
};

struct FinalizationArchiveSourceCleanupResultV1 final {
    FinalizationArchiveSourceCleanupErrorV1 error =
        FinalizationArchiveSourceCleanupErrorV1::kNone;
    std::size_t cleanable_source_count = 0U;
    std::size_t removed_source_count = 0U;
    std::size_t adopted_absent_source_count = 0U;
    std::unique_ptr<
        FinalizationArchiveSourceCleanupReceiptV1>
        receipt;

    [[nodiscard]] bool ok() const noexcept {
        return error ==
                   FinalizationArchiveSourceCleanupErrorV1::
                       kNone &&
               receipt != nullptr;
    }
};

// Deletes only FINALIZATION_REPORT, SCAFFOLDING_FINALIZATION_REPORT, and
// PREEXISTING_RECOVERY_REPORT sources whose exact locator, canonical bytes,
// type, and hash are frozen in the supplied archive receipt.  RawManifest,
// SealedRawCertificateV1, and EmptyAnchorTombstoneV1 are never deleted.
//
// The archive receipt is consumed on every return path.  Its complete
// Validate() is the authoritative pre-mutation barrier.  Routed report
// locators are resolved beneath retained_raw_root_fd; scaffolding report
// locators are exactly emergency-reports/<filename> beneath the receipt's
// retained reserve-audit descriptor.  Every component is secure-opened with
// O_NOFOLLOW/O_NOATIME.
//
// Before any source mutation this function secure-opens the pre-existing
// fixed coordinator lease, validates its marker against the archived state
// header, obtains a nonblocking exclusive flock, attaches fixed
// reserve.state, and proves that its selected highest generation is the
// archive's exact CONSUMED/all-DONE header+slot hash chain.  The returned
// unforgeable receipt retains the flock and state handle.  A live
// coordinator, replaced state, or any mismatch fails before the first
// unlink.  This function never creates a lease, replaces reserve.state, or
// provisions a reserve.
[[nodiscard]] FinalizationArchiveSourceCleanupResultV1
CleanupFinalizationArchiveSourceReportsV1At(
    int retained_raw_root_fd,
    std::unique_ptr<
        PublishedFinalizationArchiveReceiptV1>
        archive_receipt,
    const FinalizationArchiveSourceCleanupHooksV1*
        hooks = nullptr,
    std::string* diagnostic = nullptr) noexcept;

}  // namespace l2flow::ingress
