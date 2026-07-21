#pragma once

#include "l2flow/ingress/raw_reserve_state_posix.h"
#include "l2flow/ingress/reserve_headers_v1.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace l2flow::ingress {

// These names are private protocol names relative to one retained Raw-root
// directory.  UUID-bound candidates are deliberately distinct from the
// legacy fixed reserve-state temporary used by raw_reserve_state_posix.
inline constexpr char kRawEmergencyReserveDataFilename[] =
    "reserve.data";
inline constexpr char kRawEmergencyReserveInodesDirectory[] =
    "reserve-inodes";
inline constexpr std::string_view
    kRawEmergencyReserveInventoryCommitmentDomainV1 =
        "L2FLOW_RESERVE_INODE_INVENTORY_V1";

enum class RawEmergencyReservePosixErrorV1 : std::uint8_t {
    kNone = 0U,
    kInvalidArgument,
    kUnsafeRoot,
    kUnsafeInodeDirectory,
    kUnsafeDataFile,
    kUnsafeInodeFile,
    kInventoryMismatch,
    kHeaderMismatch,
    kAllocationProofFailure,
    kCapacityProbeFailure,
    kCandidateConflict,
    kStateConflict,
    kCodecFailure,
    kIoFailure,
    kSyncFailure,
    kPublishConflict,
    kReleaseOrderViolation,
    kInjectedInterruption,
    kAllocationFailure,
};

[[nodiscard]] std::string_view RawEmergencyReservePosixErrorV1Name(
    RawEmergencyReservePosixErrorV1 error) noexcept;

enum class RawEmergencyReserveProbeStageV1 : std::uint8_t {
    kBeforeProvision = 1U,
    kProvisionedCandidate = 2U,
    kAttached = 3U,
    kReleased = 4U,
    // A report receipt has completed its file/parent durability barriers
    // and the coordinator is about to atomically publish
    // FINALIZATION_REPORT=COMPLETE + grant=DONE.  This observation is
    // measured against the immutable first-activation baselines; it never
    // replenishes the durable action ledger when free space rises.
    kFinalizationPostPublish = 5U,
};

struct RawEmergencyReserveCapacityProbeRequestV1 final {
    RawEmergencyReserveProbeStageV1 stage =
        RawEmergencyReserveProbeStageV1::kAttached;
    ReserveAllocationPoolIdentityV1 pool{};
    std::uint16_t byte_probe_version = 0U;
    std::uint16_t inode_probe_version = 0U;
    std::uint64_t required_bytes = 0U;
    std::uint64_t required_inodes = 0U;
};

// A successful observation is explicit evidence for four independent
// dimensions.  filesystem_* must come from the target filesystem; quota_*
// must come from the configured user/group/project quota domain.  A statvfs
// result therefore cannot set either quota proof bit by itself.
struct RawEmergencyReserveCapacityObservationV1 final {
    ReserveAllocationPoolIdentityV1 pool{};
    std::uint16_t byte_probe_version = 0U;
    std::uint16_t inode_probe_version = 0U;
    bool filesystem_bytes_proven = false;
    bool quota_bytes_proven = false;
    bool filesystem_inodes_proven = false;
    bool quota_inodes_proven = false;
    bool reserve_byte_charge_proven = false;
    bool reserve_inode_charge_proven = false;
    std::uint64_t filesystem_free_bytes = 0U;
    std::uint64_t quota_free_bytes = 0U;
    std::uint64_t filesystem_free_inodes = 0U;
    std::uint64_t quota_free_inodes = 0U;
    std::uint64_t proven_reserve_byte_charge = 0U;
    std::uint64_t proven_reserve_inode_charge = 0U;
};

class RawEmergencyReserveCapacityProbeV1 {
public:
    virtual ~RawEmergencyReserveCapacityProbeV1() = default;

    // Production has no statvfs-only fallback.  The caller must inject an
    // implementation capable of proving both filesystem and quota byte/inode
    // facts for the exact pool identity in request.
    [[nodiscard]] virtual bool Observe(
        int retained_raw_root_fd,
        const RawEmergencyReserveCapacityProbeRequestV1& request,
        RawEmergencyReserveCapacityObservationV1* observation,
        std::string* error) noexcept = 0;
};

enum class RawEmergencyReserveMutationPointV1 : std::uint8_t {
    kDataCandidateSynced = 1U,
    kInodePublished = 2U,
    kInventorySynced = 3U,
    kStateCandidateSynced = 4U,
    kDataPublished = 5U,
    kStatePublished = 6U,
    kDataReleased = 7U,
    kInodeReleased = 8U,
};

using RawEmergencyReserveMutationHookV1 = bool (*)(
    RawEmergencyReserveMutationPointV1 point,
    std::uint32_t inode_index,
    void* context) noexcept;

struct RawEmergencyReserveMutationHooksV1 final {
    RawEmergencyReserveMutationHookV1 after = nullptr;
    void* context = nullptr;
};

// Computes the immutable inventory commitment from the state header's pool,
// count and allocation quantum.  Each index contributes its exact canonical
// filename, SHA-256 of the complete ReserveInodeHeaderV1 wire, exact expected
// st_size, and minimum expected 512-byte st_blocks count.  Because these facts
// are deterministic, a release restart can verify an exact remaining prefix
// without reconstructing facts from already-unlinked suffix files.
[[nodiscard]] RawEmergencyReservePosixErrorV1
ComputeRawEmergencyReserveInventorySha256V1(
    const ReserveCoordinatorHeaderV1& header,
    ReserveStateV1Digest* digest,
    std::string* error = nullptr) noexcept;

class RawEmergencyReserveInventoryV1 final {
public:
    ~RawEmergencyReserveInventoryV1();

    RawEmergencyReserveInventoryV1(
        const RawEmergencyReserveInventoryV1&) = delete;
    RawEmergencyReserveInventoryV1& operator=(
        const RawEmergencyReserveInventoryV1&) = delete;
    RawEmergencyReserveInventoryV1(
        RawEmergencyReserveInventoryV1&&) = delete;
    RawEmergencyReserveInventoryV1& operator=(
        RawEmergencyReserveInventoryV1&&) = delete;

    [[nodiscard]] int root_descriptor() const noexcept {
        return root_fd_;
    }
    [[nodiscard]] int data_descriptor() const noexcept {
        return data_fd_;
    }
    [[nodiscard]] int inode_directory_descriptor() const noexcept {
        return inode_directory_fd_;
    }
    [[nodiscard]] std::span<const int> inode_descriptors()
        const noexcept {
        return inode_fds_;
    }
    [[nodiscard]] const ReserveCoordinatorHeaderV1& header()
        const noexcept {
        return header_;
    }
    [[nodiscard]] ReserveCoordinatorPhaseV1 phase() const noexcept {
        return phase_;
    }
    [[nodiscard]] std::uint32_t remaining_inode_prefix_count()
        const noexcept {
        return remaining_inode_prefix_count_;
    }
    [[nodiscard]] bool data_present() const noexcept {
        return data_fd_ >= 0;
    }

private:
    friend std::unique_ptr<RawEmergencyReserveInventoryV1>
    AttachRawEmergencyReserveInventoryV1(
        const RawReserveStateFileV1&,
        RawEmergencyReserveCapacityProbeV1*,
        RawEmergencyReservePosixErrorV1*,
        std::string*) noexcept;
    friend bool ProvisionFreshRawEmergencyReserveV1(
        int,
        const ReserveCoordinatorStateV1&,
        RawEmergencyReserveCapacityProbeV1*,
        const RawEmergencyReserveMutationHooksV1*,
        struct RawEmergencyReserveProvisionResultV1*,
        RawEmergencyReservePosixErrorV1*,
        ReserveStateV1Error*,
        std::string*) noexcept;
    friend RawEmergencyReservePosixErrorV1
    ReleaseRawEmergencyReserveV1(
        RawEmergencyReserveInventoryV1&,
        RawEmergencyReserveCapacityProbeV1*,
        const RawEmergencyReserveMutationHooksV1*,
        std::string*) noexcept;

    RawEmergencyReserveInventoryV1(
        int root_fd,
        int data_fd,
        int inode_directory_fd,
        std::vector<int> inode_fds,
        ReserveCoordinatorHeaderV1 header,
        ReserveStateSlotV1 selected_slot,
        std::uint32_t remaining_inode_prefix_count) noexcept;

    int root_fd_ = -1;
    int data_fd_ = -1;
    int inode_directory_fd_ = -1;
    std::vector<int> inode_fds_{};
    ReserveCoordinatorHeaderV1 header_{};
    ReserveStateSlotV1 selected_slot_{};
    ReserveCoordinatorPhaseV1 phase_ =
        ReserveCoordinatorPhaseV1::kProvisioned;
    std::uint32_t remaining_inode_prefix_count_ = 0U;
};

struct RawEmergencyReserveProvisionResultV1 final {
    std::unique_ptr<RawReserveStateFileV1> state_file{};
    std::unique_ptr<RawEmergencyReserveInventoryV1> inventory{};
};

// Attaches the fixed reserve objects to an already-securely-attached state.
// PROVISIONED/INTENT require data+complete inventory; PREPARED accepts exactly
// the three release-order states from design.md; CONSUMED requires both absent.
// Every success retains the root, data (when present), inode directory and all
// remaining inventory descriptors.
[[nodiscard]] std::unique_ptr<RawEmergencyReserveInventoryV1>
AttachRawEmergencyReserveInventoryV1(
    const RawReserveStateFileV1& state_file,
    RawEmergencyReserveCapacityProbeV1* capacity_probe,
    RawEmergencyReservePosixErrorV1* failure = nullptr,
    std::string* error = nullptr) noexcept;

// Offline initial provision transaction.  bootstrap must be the exact
// duplicate generation=1 PROVISIONED state, including the commitment returned
// by ComputeRawEmergencyReserveInventorySha256V1.  Publication uses UUID-bound
// data/state candidates and never weakens the legacy fixed-state API.
[[nodiscard]] bool ProvisionFreshRawEmergencyReserveV1(
    int retained_raw_root_fd,
    const ReserveCoordinatorStateV1& bootstrap,
    RawEmergencyReserveCapacityProbeV1* capacity_probe,
    const RawEmergencyReserveMutationHooksV1* hooks,
    RawEmergencyReserveProvisionResultV1* result,
    RawEmergencyReservePosixErrorV1* failure = nullptr,
    ReserveStateV1Error* codec_error = nullptr,
    std::string* error = nullptr) noexcept;

// Physical PREPARED release only; publishing CONSUMED remains a coordinator
// responsibility.  Data is unlinked+closed+root-fsynced first. Inventory is
// then released index-descending, with identity revalidation, close and an
// inode-directory fsync after every unlink. A failure leaves the capability at
// the exact remaining prefix for an in-process retry.
[[nodiscard]] RawEmergencyReservePosixErrorV1
ReleaseRawEmergencyReserveV1(
    RawEmergencyReserveInventoryV1& inventory,
    RawEmergencyReserveCapacityProbeV1* capacity_probe,
    const RawEmergencyReserveMutationHooksV1* hooks = nullptr,
    std::string* error = nullptr) noexcept;

}  // namespace l2flow::ingress
