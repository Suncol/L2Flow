#pragma once

#include "l2flow/ingress/raw_recovery_maintenance_report_v1.h"
#include "l2flow/ingress/raw_reserve_coordinator.h"

#include <cstdint>
#include <string>
#include <utility>

#include <unistd.h>

namespace l2flow::ingress {

class RecoveryMaintenanceReportStoreAccessV1;

// Non-forgeable process-local proof for the ordinary terminal recovery
// sequence:
//
//   durable sidecar -> sidecar directory barrier
//                   -> durable terminal report -> report directory barrier.
//
// The receipt owns retained descriptors for the maintenance directory,
// journal, sidecar, and report together with their inode identities and the
// complete validated terminal report model. It is single-use and is the only
// authority accepted by UnregisterRecoveredTerminal().
class RecoveryTerminalReportReceiptV1 final {
public:
    ~RecoveryTerminalReportReceiptV1() {
        if (report_fd_ >= 0) {
            static_cast<void>(::close(report_fd_));
        }
        if (sidecar_fd_ >= 0) {
            static_cast<void>(::close(sidecar_fd_));
        }
        if (journal_fd_ >= 0) {
            static_cast<void>(::close(journal_fd_));
        }
        if (maintenance_directory_fd_ >= 0) {
            static_cast<void>(
                ::close(maintenance_directory_fd_));
        }
    }

    RecoveryTerminalReportReceiptV1(
        const RecoveryTerminalReportReceiptV1&) = delete;
    RecoveryTerminalReportReceiptV1& operator=(
        const RecoveryTerminalReportReceiptV1&) = delete;
    RecoveryTerminalReportReceiptV1(
        RecoveryTerminalReportReceiptV1&&) = delete;
    RecoveryTerminalReportReceiptV1& operator=(
        RecoveryTerminalReportReceiptV1&&) = delete;

private:
    friend class RecoveryMaintenanceReportStoreAccessV1;
    friend class RawReserveRegistryCoordinatorV1;

    RecoveryTerminalReportReceiptV1(
        RawReserveRegistryEntryKeyV1 key,
        RawReserveGenerationActionTokenV1 token,
        RawReserveMutationTargetAnchorV1 target,
        RawV1Identity writer_instance,
        RecoveryMaintenanceResultV1 result,
        RawV1Digest report_sha256,
        std::string report_filename,
        RawV1Digest sidecar_sha256,
        std::string sidecar_filename,
        RawV1Digest journal_header_sha256,
        RecoveryMaintenanceReportV1 report_model,
        RawV1JournalHeaderWire journal_header_bytes,
        std::uint64_t maintenance_device,
        std::uint64_t maintenance_inode,
        std::uint64_t journal_device,
        std::uint64_t journal_inode,
        std::uint64_t sidecar_device,
        std::uint64_t sidecar_inode,
        std::uint64_t report_device,
        std::uint64_t report_inode,
        int maintenance_directory_fd,
        int journal_fd,
        int sidecar_fd,
        int report_fd) noexcept
        : key_(std::move(key)),
          token_(std::move(token)),
          target_(std::move(target)),
          writer_instance_(writer_instance),
          result_(result),
          report_sha256_(report_sha256),
          report_filename_(std::move(report_filename)),
          sidecar_sha256_(sidecar_sha256),
          sidecar_filename_(std::move(sidecar_filename)),
          journal_header_sha256_(journal_header_sha256),
          report_model_(std::move(report_model)),
          journal_header_bytes_(journal_header_bytes),
          maintenance_device_(maintenance_device),
          maintenance_inode_(maintenance_inode),
          journal_device_(journal_device),
          journal_inode_(journal_inode),
          sidecar_device_(sidecar_device),
          sidecar_inode_(sidecar_inode),
          report_device_(report_device),
          report_inode_(report_inode),
          maintenance_directory_fd_(
              maintenance_directory_fd),
          journal_fd_(journal_fd),
          sidecar_fd_(sidecar_fd),
          report_fd_(report_fd) {}

    RawReserveRegistryEntryKeyV1 key_{};
    RawReserveGenerationActionTokenV1 token_{};
    RawReserveMutationTargetAnchorV1 target_;
    RawV1Identity writer_instance_{};
    RecoveryMaintenanceResultV1 result_ =
        RecoveryMaintenanceResultV1::kResumedOpen;
    RawV1Digest report_sha256_{};
    std::string report_filename_;
    RawV1Digest sidecar_sha256_{};
    std::string sidecar_filename_;
    RawV1Digest journal_header_sha256_{};
    RecoveryMaintenanceReportV1 report_model_{};
    RawV1JournalHeaderWire journal_header_bytes_{};
    std::uint64_t maintenance_device_ = 0U;
    std::uint64_t maintenance_inode_ = 0U;
    std::uint64_t journal_device_ = 0U;
    std::uint64_t journal_inode_ = 0U;
    std::uint64_t sidecar_device_ = 0U;
    std::uint64_t sidecar_inode_ = 0U;
    std::uint64_t report_device_ = 0U;
    std::uint64_t report_inode_ = 0U;
    int maintenance_directory_fd_ = -1;
    int journal_fd_ = -1;
    int sidecar_fd_ = -1;
    int report_fd_ = -1;
    bool consumed_ = false;
};

}  // namespace l2flow::ingress
