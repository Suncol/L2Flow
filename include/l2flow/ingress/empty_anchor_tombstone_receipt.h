#pragma once

#include "l2flow/ingress/empty_anchor_tombstone_v1.h"
#include "l2flow/ingress/raw_reserve_coordinator.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include <unistd.h>

namespace l2flow::ingress {

class RawWriterLease;
struct EmptyAnchorTombstonePublishResultV1;
class RecoveryMaintenanceReportStoreAccessV1;

[[nodiscard]] EmptyAnchorTombstonePublishResultV1
PublishEmptyAnchorTombstoneV1(
    const RawWriterLease& lease,
    std::unique_ptr<RawReserveAuthorizedActionV1>
        recovering_action,
    const BuiltEmptyAnchorTombstoneV1& tombstone,
    std::string* diagnostic) noexcept;

// Non-forgeable process-local proof that one exact header-only Raw journal
// was revalidated together with the empty stream-day namespace and that the
// matching tombstone crossed file-fsync, NOREPLACE publication,
// maintenance-directory fsync, and retained-fd readback while the exact
// RECOVERING+RECOVER_SEAL_ONLY generation action was held.
//
// The receipt is intentionally not a durable artifact. It retains all three
// evidence inodes and their identities for a later terminal-report or
// coordinator consumer; this publisher never unregisters the route itself.
class EmptyAnchorTombstoneReceiptV1 final {
public:
    ~EmptyAnchorTombstoneReceiptV1() {
        if (tombstone_fd_ >= 0) {
            static_cast<void>(::close(tombstone_fd_));
        }
        if (journal_fd_ >= 0) {
            static_cast<void>(::close(journal_fd_));
        }
        if (maintenance_directory_fd_ >= 0) {
            static_cast<void>(
                ::close(maintenance_directory_fd_));
        }
    }

    EmptyAnchorTombstoneReceiptV1(
        const EmptyAnchorTombstoneReceiptV1&) = delete;
    EmptyAnchorTombstoneReceiptV1& operator=(
        const EmptyAnchorTombstoneReceiptV1&) = delete;
    EmptyAnchorTombstoneReceiptV1(
        EmptyAnchorTombstoneReceiptV1&&) = delete;
    EmptyAnchorTombstoneReceiptV1& operator=(
        EmptyAnchorTombstoneReceiptV1&&) = delete;

private:
    friend EmptyAnchorTombstonePublishResultV1
    PublishEmptyAnchorTombstoneV1(
        const RawWriterLease&,
        std::unique_ptr<
            RawReserveAuthorizedActionV1>,
        const BuiltEmptyAnchorTombstoneV1&,
        std::string*) noexcept;
    friend class RawReserveRegistryCoordinatorV1;
    friend class RecoveryMaintenanceReportStoreAccessV1;

    EmptyAnchorTombstoneReceiptV1(
        RawReserveRegistryEntryKeyV1 key,
        RawReserveGenerationActionTokenV1 token,
        RawReserveMutationTargetAnchorV1 target,
        RawV1Identity writer_instance,
        RawV1Digest tombstone_sha256,
        RawV1Digest journal_header_sha256,
        std::string filename,
        std::uint64_t maintenance_device,
        std::uint64_t maintenance_inode,
        std::uint64_t journal_device,
        std::uint64_t journal_inode,
        std::uint64_t tombstone_device,
        std::uint64_t tombstone_inode,
        int maintenance_directory_fd,
        int journal_fd,
        int tombstone_fd) noexcept
        : key_(std::move(key)),
          token_(std::move(token)),
          target_(std::move(target)),
          writer_instance_(writer_instance),
          tombstone_sha256_(tombstone_sha256),
          journal_header_sha256_(
              journal_header_sha256),
          filename_(std::move(filename)),
          maintenance_device_(maintenance_device),
          maintenance_inode_(maintenance_inode),
          journal_device_(journal_device),
          journal_inode_(journal_inode),
          tombstone_device_(tombstone_device),
          tombstone_inode_(tombstone_inode),
          maintenance_directory_fd_(
              maintenance_directory_fd),
          journal_fd_(journal_fd),
          tombstone_fd_(tombstone_fd) {}

    RawReserveRegistryEntryKeyV1 key_{};
    RawReserveGenerationActionTokenV1 token_{};
    RawReserveMutationTargetAnchorV1 target_;
    RawV1Identity writer_instance_{};
    RawV1Digest tombstone_sha256_{};
    RawV1Digest journal_header_sha256_{};
    std::string filename_;
    std::uint64_t maintenance_device_ = 0U;
    std::uint64_t maintenance_inode_ = 0U;
    std::uint64_t journal_device_ = 0U;
    std::uint64_t journal_inode_ = 0U;
    std::uint64_t tombstone_device_ = 0U;
    std::uint64_t tombstone_inode_ = 0U;
    int maintenance_directory_fd_ = -1;
    int journal_fd_ = -1;
    int tombstone_fd_ = -1;
    bool consumed_ = false;
};

}  // namespace l2flow::ingress
