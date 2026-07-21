#pragma once

#include "l2flow/ingress/raw_recovery_maintenance_report_v1.h"
#include "l2flow/ingress/raw_reserve_coordinator.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include <unistd.h>

namespace l2flow::ingress {

class RawWriterLease;
struct RecoveryMaintenanceReportPublishResultV1;
class RecoveryMaintenanceReportStoreAccessV1;

[[nodiscard]] RecoveryMaintenanceReportPublishResultV1
PublishRecoveryMaintenanceReportV1(
    const RawWriterLease& lease,
    std::unique_ptr<RawReserveAuthorizedActionV1>
        recovering_action,
    const BuiltRecoveryMaintenanceReportV1& report,
    std::string* diagnostic) noexcept;

// Non-forgeable process-local proof that the exact RESUMED_OPEN report
// completed file-fsync, NOREPLACE publication, maintenance-directory fsync
// and retained-fd readback while the matching RECOVERING/RESUME_CONNECT
// generation action was held. Durable report bytes deliberately omit runtime
// writer identity; this receipt binds it for the later coordinator
// RECOVERING->ACTIVE transition.
class RecoveryMaintenanceReportReceiptV1 final {
public:
    ~RecoveryMaintenanceReportReceiptV1() {
        if (report_fd_ >= 0) {
            static_cast<void>(::close(report_fd_));
        }
        if (maintenance_directory_fd_ >= 0) {
            static_cast<void>(
                ::close(maintenance_directory_fd_));
        }
    }

    RecoveryMaintenanceReportReceiptV1(
        const RecoveryMaintenanceReportReceiptV1&) =
        delete;
    RecoveryMaintenanceReportReceiptV1& operator=(
        const RecoveryMaintenanceReportReceiptV1&) =
        delete;
    RecoveryMaintenanceReportReceiptV1(
        RecoveryMaintenanceReportReceiptV1&&) = delete;
    RecoveryMaintenanceReportReceiptV1& operator=(
        RecoveryMaintenanceReportReceiptV1&&) = delete;

private:
    friend RecoveryMaintenanceReportPublishResultV1
    PublishRecoveryMaintenanceReportV1(
        const RawWriterLease&,
        std::unique_ptr<
            RawReserveAuthorizedActionV1>,
        const BuiltRecoveryMaintenanceReportV1&,
        std::string*) noexcept;
    friend class RawReserveRegistryCoordinatorV1;
    friend class RecoveryMaintenanceReportStoreAccessV1;

    RecoveryMaintenanceReportReceiptV1(
        RawReserveRegistryEntryKeyV1 key,
        RawReserveGenerationActionTokenV1 token,
        RawReserveMutationTargetAnchorV1 target,
        RawV1Identity writer_instance,
        RawV1Digest report_sha256,
        std::string filename,
        std::uint64_t closed_entry_count,
        RawV1Digest closed_prefix_sha256,
        std::uint64_t manifest_generation,
        RawV1Digest manifest_entry_commitment_sha256,
        std::uint32_t segment_sequence,
        RawV1Digest segment_header_sha256,
        RawV1DurableMarkerWire endpoint_marker_bytes,
        RawV1Digest endpoint_marker_sha256,
        RecoveryMaintenanceCursorV1 control_cursor,
        int maintenance_directory_fd,
        int report_fd) noexcept
        : key_(std::move(key)),
          token_(std::move(token)),
          target_(std::move(target)),
          writer_instance_(writer_instance),
          report_sha256_(report_sha256),
          filename_(std::move(filename)),
          closed_entry_count_(closed_entry_count),
          closed_prefix_sha256_(closed_prefix_sha256),
          manifest_generation_(manifest_generation),
          manifest_entry_commitment_sha256_(
              manifest_entry_commitment_sha256),
          segment_sequence_(segment_sequence),
          segment_header_sha256_(segment_header_sha256),
          endpoint_marker_bytes_(endpoint_marker_bytes),
          endpoint_marker_sha256_(endpoint_marker_sha256),
          control_cursor_(control_cursor),
          maintenance_directory_fd_(
              maintenance_directory_fd),
          report_fd_(report_fd) {}

    RawReserveRegistryEntryKeyV1 key_{};
    RawReserveGenerationActionTokenV1 token_{};
    RawReserveMutationTargetAnchorV1 target_;
    RawV1Identity writer_instance_{};
    RawV1Digest report_sha256_{};
    std::string filename_;
    std::uint64_t closed_entry_count_ = 0U;
    RawV1Digest closed_prefix_sha256_{};
    std::uint64_t manifest_generation_ = 0U;
    RawV1Digest manifest_entry_commitment_sha256_{};
    std::uint32_t segment_sequence_ = 0U;
    RawV1Digest segment_header_sha256_{};
    RawV1DurableMarkerWire endpoint_marker_bytes_{};
    RawV1Digest endpoint_marker_sha256_{};
    RecoveryMaintenanceCursorV1 control_cursor_{};
    int maintenance_directory_fd_ = -1;
    int report_fd_ = -1;
    bool consumed_ = false;
};

}  // namespace l2flow::ingress
