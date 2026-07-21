#pragma once

#include "l2flow/ingress/finalization_archive_v1.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace l2flow::ingress {

enum class FinalizationArchivePosixStoreErrorV1
    : std::uint8_t {
    kNone = 0U,
    kInvalidArgument,
    kUnsafeAuditDirectory,
    kMalformedCandidateName,
    kUnsafeCandidate,
    kCandidateConflict,
    kTemporaryCreate,
    kDirectoryCreate,
    kFileCreate,
    kWriteFailure,
    kSyncFailure,
    kPublishConflict,
    kReadbackFailure,
    kCleanupFailure,
    kAllocationFailure,
};

[[nodiscard]] std::string_view
FinalizationArchivePosixStoreErrorV1Name(
    FinalizationArchivePosixStoreErrorV1 error) noexcept;

enum class FinalizationArchivePosixDispositionV1
    : std::uint8_t {
    kNone = 0U,
    kPublishedNew,
    kAdoptedCompleteTemporary,
    kAcceptedExistingFinal,
    kRebuiltRecognizedPartialTemporary,
};

class PublishedFinalizationArchiveReceiptV1 final {
public:
    ~PublishedFinalizationArchiveReceiptV1();

    PublishedFinalizationArchiveReceiptV1(
        const PublishedFinalizationArchiveReceiptV1&) =
        delete;
    PublishedFinalizationArchiveReceiptV1& operator=(
        const PublishedFinalizationArchiveReceiptV1&) =
        delete;
    PublishedFinalizationArchiveReceiptV1(
        PublishedFinalizationArchiveReceiptV1&&) =
        delete;
    PublishedFinalizationArchiveReceiptV1& operator=(
        PublishedFinalizationArchiveReceiptV1&&) =
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
    manifest_sha256() const noexcept {
        return manifest_sha256_;
    }
    [[nodiscard]] const RawV1Digest&
    state_sha256() const noexcept {
        return state_sha256_;
    }
    [[nodiscard]] std::string_view directory_name()
        const noexcept {
        return directory_name_;
    }
    [[nodiscard]] std::size_t artifact_count()
        const noexcept {
        return artifact_count_;
    }

    // Pure readback revalidation.  It checks the retained audit/archive/
    // evidence directory identities, exact final name->inode binding,
    // complete bounded directory inventories, every retained regular-file
    // identity/size/hash, manifest JCS, and the state hash chain.  It does
    // not fsync or authorize source-report cleanup/reprovision.
    [[nodiscard]] bool Validate(
        std::string* diagnostic = nullptr) const noexcept;

    // Public only as a concrete storage type required by the out-of-line
    // validator; callers cannot place one into a receipt because receipt
    // construction remains private.
    struct RetainedFileV1 final {
        RetainedFileV1() noexcept = default;
        ~RetainedFileV1();
        RetainedFileV1(const RetainedFileV1&) = delete;
        RetainedFileV1& operator=(
            const RetainedFileV1&) = delete;
        RetainedFileV1(
            RetainedFileV1&& other) noexcept;
        RetainedFileV1& operator=(
            RetainedFileV1&& other) noexcept;

        int descriptor = -1;
        bool evidence_parent = false;
        std::string name;
        RawV1Digest sha256{};
        std::uint64_t byte_count = 0U;
        std::uint64_t device = 0U;
        std::uint64_t inode = 0U;
    };

private:
    friend struct
        FinalizationArchivePosixPublishAccessV1;
    friend struct
        FinalizationArchiveSourceCleanupAccessV1;

    PublishedFinalizationArchiveReceiptV1(
        RawV1Identity reserve_state_uuid,
        RawV1Identity finalization_cycle_id,
        RawV1Digest manifest_sha256,
        RawV1Digest state_sha256,
        std::string directory_name,
        std::size_t artifact_count,
        int audit_directory_fd,
        int archive_directory_fd,
        int evidence_directory_fd,
        std::uint64_t audit_device,
        std::uint64_t audit_inode,
        std::uint64_t archive_device,
        std::uint64_t archive_inode,
        std::uint64_t evidence_device,
        std::uint64_t evidence_inode,
        std::vector<RetainedFileV1> files) noexcept;

    RawV1Identity reserve_state_uuid_{};
    RawV1Identity finalization_cycle_id_{};
    RawV1Digest manifest_sha256_{};
    RawV1Digest state_sha256_{};
    std::string directory_name_;
    std::size_t artifact_count_ = 0U;
    int audit_directory_fd_ = -1;
    int archive_directory_fd_ = -1;
    int evidence_directory_fd_ = -1;
    std::uint64_t audit_device_ = 0U;
    std::uint64_t audit_inode_ = 0U;
    std::uint64_t archive_device_ = 0U;
    std::uint64_t archive_inode_ = 0U;
    std::uint64_t evidence_device_ = 0U;
    std::uint64_t evidence_inode_ = 0U;
    std::vector<RetainedFileV1> files_;
};

struct FinalizationArchivePosixPublishResultV1 final {
    FinalizationArchivePosixStoreErrorV1 error =
        FinalizationArchivePosixStoreErrorV1::kNone;
    FinalizationArchivePosixDispositionV1 disposition =
        FinalizationArchivePosixDispositionV1::kNone;
    std::string directory_name;
    RawV1Digest manifest_sha256{};
    RawV1Digest state_sha256{};
    std::unique_ptr<
        PublishedFinalizationArchiveReceiptV1>
        receipt;
    bool files_synced = false;
    bool subdirectories_synced = false;
    bool audit_parent_synced = false;

    [[nodiscard]] bool ok() const noexcept {
        return error ==
                   FinalizationArchivePosixStoreErrorV1::
                       kNone &&
               receipt != nullptr;
    }
};

struct FinalizationArchivePosixLoadResultV1 final {
    FinalizationArchivePosixStoreErrorV1 error =
        FinalizationArchivePosixStoreErrorV1::kNone;
    std::string directory_name;
    RawV1Digest manifest_sha256{};
    RawV1Digest state_sha256{};
    std::unique_ptr<BuiltFinalizationArchiveV1>
        archive;

    [[nodiscard]] bool ok() const noexcept {
        return error ==
                   FinalizationArchivePosixStoreErrorV1::
                       kNone &&
               archive != nullptr;
    }
};

// Reconstructs the private publication capability from an already-published
// immutable final archive after process restart.  The caller supplies the
// exact fixed-state header and selected all-DONE slot discovered state-first;
// the loader derives the only legal directory name, opens every component
// with O_NOFOLLOW/O_NOATIME, parses bounded canonical evidence, rebuilds the
// capability through the normal causal validator, and requires a byte-exact
// model/tree match.  It performs no fsync, rename, unlink, or other mutation.
//
// A successful result is deliberately not a cleanup receipt.  Call
// PublishFinalizationArchiveV1At() with the returned capability to re-run the
// file/subdirectory/audit-parent durability barriers and obtain the retained
// receipt consumed by source cleanup.
[[nodiscard]] FinalizationArchivePosixLoadResultV1
LoadPublishedFinalizationArchiveCapabilityV1At(
    int retained_audit_directory_fd,
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& all_done_slot,
    std::string* diagnostic = nullptr) noexcept;

// Publishes one immutable archive beneath a retained owner-only
// reserve-audit/ descriptor:
//
//   deterministic tmp tree -> each file fsync
//   -> evidence dir fsync -> tmp root fsync
//   -> renameat2(RENAME_NOREPLACE) -> audit-parent fsync
//   -> exact retained-fd tree readback receipt.
//
// A sole complete matching tmp is adopted.  A sole recognized partial tmp
// whose tree is a safe subset of the capability-derived names is removed
// bottom-up with the corresponding directory syncs and rebuilt.  Unknown,
// unsafe, conflicting, or multiple names are retained and fail closed.  An
// existing exact final is re-fsynced file-by-file and bottom-up before it is
// accepted.  This function neither acquires the required external
// maintenance-mode coordinator flock nor cleans source reports, replaces
// reserve.state, or provisions a new reserve.
[[nodiscard]] FinalizationArchivePosixPublishResultV1
PublishFinalizationArchiveV1At(
    int retained_audit_directory_fd,
    const BuiltFinalizationArchiveV1& archive,
    std::string* diagnostic = nullptr) noexcept;

}  // namespace l2flow::ingress
