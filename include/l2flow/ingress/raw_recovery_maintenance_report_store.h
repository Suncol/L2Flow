#pragma once

#include "l2flow/ingress/empty_anchor_tombstone_receipt.h"
#include "l2flow/ingress/raw_recovery_maintenance_report_receipt.h"
#include "l2flow/ingress/raw_recovery_terminal_report_receipt.h"
#include "l2flow/ingress/raw_recovery_maintenance_report_v1.h"
#include "l2flow/ingress/raw_reserve_coordinator.h"
#include "l2flow/ingress/raw_sealed_certificate_receipt.h"
#include "l2flow/ingress/raw_writer_lease.h"

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

namespace l2flow::ingress {

inline constexpr std::uint32_t
    kRecoveryMaintenanceReportV1MaximumCandidates = 4096U;
inline constexpr std::string_view
    kRecoveryMaintenanceReportV1TemporarySuffix =
        ".recovery-maintenance-report-v1.tmp";

enum class RecoveryMaintenanceReportStoreErrorV1
    : std::uint8_t {
    kNone = 0U,
    kInvalidArgument,
    kAuthorizationRejected,
    kTargetMismatch,
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
RecoveryMaintenanceReportStoreErrorV1Name(
    RecoveryMaintenanceReportStoreErrorV1 error) noexcept;

enum class RecoveryMaintenanceReportDispositionV1
    : std::uint8_t {
    kNone = 0U,
    kPublishedNew,
    kAdoptedCompleteTemporary,
    kAcceptedExistingFinal,
    kAcceptedExistingAndCleanedIdenticalTemporary,
    kRebuiltRecognizedPartialTemporary,
    kAcceptedExistingAndCleanedRecognizedPartialTemporary,
};

struct RecoveryMaintenanceReportPublishResultV1 final {
    RecoveryMaintenanceReportStoreErrorV1 error =
        RecoveryMaintenanceReportStoreErrorV1::kNone;
    RecoveryMaintenanceReportDispositionV1 disposition =
        RecoveryMaintenanceReportDispositionV1::kNone;
    RawV1Digest report_sha256{};
    std::string filename;
    std::unique_ptr<
        RecoveryMaintenanceReportReceiptV1>
        activation_receipt;
    std::unique_ptr<
        RecoveryTerminalReportReceiptV1>
        terminal_receipt;
    std::uint32_t observed_candidate_count = 0U;
    bool file_synced = false;
    bool directory_synced = false;

    [[nodiscard]] bool ok() const noexcept {
        return error ==
               RecoveryMaintenanceReportStoreErrorV1::
                   kNone;
    }
};

// Publishes or re-establishes the exact ordinary PROVISIONED
// RESUMED_OPEN barrier. The action is consumed and must authorize the same
// existing route in RECOVERING with RESUME_CONNECT intent. Its shared OFD
// generation gate remains held across every filesystem mutation and is
// necessarily released before this function returns.
[[nodiscard]] RecoveryMaintenanceReportPublishResultV1
PublishRecoveryMaintenanceReportV1(
    const RawWriterLease& lease,
    std::unique_ptr<RawReserveAuthorizedActionV1>
        recovering_action,
    const BuiltRecoveryMaintenanceReportV1& report,
    std::string* diagnostic = nullptr) noexcept;

// Terminal overloads consume the exact sidecar receipt from the preceding
// RECOVERING+RECOVER_SEAL_ONLY publication. The sidecar receipt is consumed
// even when validation or report publication fails. Success returns only a
// RecoveryTerminalReportReceiptV1 and preserves the strict sidecar barrier
// before report barrier ordering.
[[nodiscard]] RecoveryMaintenanceReportPublishResultV1
PublishRecoveryMaintenanceReportV1(
    const RawWriterLease& lease,
    std::unique_ptr<RawReserveAuthorizedActionV1>
        recovering_action,
    const BuiltRecoveryMaintenanceReportV1& report,
    std::unique_ptr<
        SealedRawRecoveryTerminalReceiptV1>
        sidecar_receipt,
    std::string* diagnostic = nullptr) noexcept;

[[nodiscard]] RecoveryMaintenanceReportPublishResultV1
PublishRecoveryMaintenanceReportV1(
    const RawWriterLease& lease,
    std::unique_ptr<RawReserveAuthorizedActionV1>
        recovering_action,
    const BuiltRecoveryMaintenanceReportV1& report,
    std::unique_ptr<EmptyAnchorTombstoneReceiptV1>
        sidecar_receipt,
    std::string* diagnostic = nullptr) noexcept;

}  // namespace l2flow::ingress
