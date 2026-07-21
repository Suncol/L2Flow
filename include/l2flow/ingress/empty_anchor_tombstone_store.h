#pragma once

#include "l2flow/ingress/empty_anchor_tombstone_receipt.h"
#include "l2flow/ingress/empty_anchor_tombstone_v1.h"
#include "l2flow/ingress/raw_reserve_coordinator.h"
#include "l2flow/ingress/raw_writer_lease.h"

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

namespace l2flow::ingress {

inline constexpr std::uint32_t
    kEmptyAnchorTombstoneV1MaximumCandidates = 4096U;
inline constexpr std::string_view
    kEmptyAnchorTombstoneV1TemporarySuffix =
        ".empty-anchor-tombstone-v1.tmp";

enum class EmptyAnchorTombstoneStoreErrorV1
    : std::uint8_t {
    kNone = 0U,
    kInvalidArgument,
    kAuthorizationRejected,
    kTargetMismatch,
    kNamespaceInventory,
    kJournalMissing,
    kUnsafeJournal,
    kJournalMismatch,
    kMaintenanceDirectoryMissing,
    kUnsafeMaintenanceDirectory,
    kCandidateLimitExceeded,
    kMalformedCandidateName,
    kUnsafeCandidate,
    kCandidateConflict,
    kTemporaryCreate,
    kWriteFailure,
    kSyncFailure,
    kPublishConflict,
    kReadbackFailure,
    kAllocationFailure,
};

[[nodiscard]] std::string_view
EmptyAnchorTombstoneStoreErrorV1Name(
    EmptyAnchorTombstoneStoreErrorV1 error) noexcept;

enum class EmptyAnchorTombstoneDispositionV1
    : std::uint8_t {
    kNone = 0U,
    kPublishedNew,
    kAdoptedCompleteTemporary,
    kAcceptedExistingFinal,
};

struct EmptyAnchorTombstonePublishResultV1 final {
    EmptyAnchorTombstoneStoreErrorV1 error =
        EmptyAnchorTombstoneStoreErrorV1::kNone;
    EmptyAnchorTombstoneDispositionV1 disposition =
        EmptyAnchorTombstoneDispositionV1::kNone;
    RawV1Digest tombstone_sha256{};
    std::string filename;
    std::unique_ptr<
        EmptyAnchorTombstoneReceiptV1> receipt;
    std::uint32_t observed_candidate_count = 0U;
    bool file_synced = false;
    bool directory_synced = false;

    [[nodiscard]] bool ok() const noexcept {
        return error ==
                   EmptyAnchorTombstoneStoreErrorV1::
                       kNone &&
               receipt != nullptr;
    }
};

// Publishes the ordinary PROVISIONED RECOVER_SEAL_ONLY empty-anchor
// certificate. The action is consumed and its shared generation gate remains
// held across every namespace check and filesystem mutation. The stream-day
// maintenance directory must already exist and is never created here.
//
// A partial, conflicting, unsafe, multiply named, or path-replaced candidate
// is retained as evidence and fails closed. A complete byte-identical typed
// temporary may be adopted; an existing final is accepted only after exact
// canonical-byte, fsync, parent-fsync, name/inode, and retained-fd readback
// validation. This function does not unregister or activate the route.
[[nodiscard]] EmptyAnchorTombstonePublishResultV1
PublishEmptyAnchorTombstoneV1(
    const RawWriterLease& lease,
    std::unique_ptr<RawReserveAuthorizedActionV1>
        recovering_action,
    const BuiltEmptyAnchorTombstoneV1& tombstone,
    std::string* diagnostic = nullptr) noexcept;

}  // namespace l2flow::ingress
