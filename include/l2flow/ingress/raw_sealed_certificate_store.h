#pragma once

#include "l2flow/ingress/raw_reserve_coordinator.h"
#include "l2flow/ingress/raw_sealed_certificate_receipt.h"
#include "l2flow/ingress/raw_sealed_certificate_v1.h"
#include "l2flow/ingress/raw_writer_lease.h"

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

namespace l2flow::ingress {

inline constexpr std::uint32_t
    kSealedRawCertificateV1MaximumCandidates = 4096U;
inline constexpr std::string_view
    kSealedRawCertificateV1TemporarySuffix =
        ".sealed-raw-certificate-v1.tmp";

enum class SealedRawCertificateStoreErrorV1
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
SealedRawCertificateStoreErrorV1Name(
    SealedRawCertificateStoreErrorV1 error) noexcept;

enum class SealedRawCertificateDispositionV1
    : std::uint8_t {
    kNone = 0U,
    kPublishedNew,
    kAdoptedCompleteTemporary,
    kAcceptedExistingFinal,
    kAcceptedExistingAndCleanedIdenticalTemporary,
    kRebuiltRecognizedPartialTemporary,
    kAcceptedExistingAndCleanedRecognizedPartialTemporary,
};

struct SealedRawCertificatePublishResultV1 final {
    SealedRawCertificateStoreErrorV1 error =
        SealedRawCertificateStoreErrorV1::kNone;
    SealedRawCertificateDispositionV1 disposition =
        SealedRawCertificateDispositionV1::kNone;
    RawV1Digest certificate_sha256{};
    std::string filename;
    std::unique_ptr<SealedRawCertificateReceiptV1>
        unregister_receipt;
    std::unique_ptr<
        SealedRawRecoveryTerminalReceiptV1>
        recovery_terminal_receipt;
    std::uint32_t observed_candidate_count = 0U;
    bool file_synced = false;
    bool directory_synced = false;

    [[nodiscard]] bool ok() const noexcept {
        return error ==
               SealedRawCertificateStoreErrorV1::kNone;
    }
};

// Publishes or re-establishes the exact normal certificate barrier. The
// consumed target-bound action is either ACTIVE (clean stop) or the exact
// RECOVERING+RECOVER_SEAL_ONLY attempt. Its shared OFD generation gate is
// held for the entire call and is necessarily released before this function
// returns; ValidateLatest and target identity are checked before every
// synchronizing mutation. No final name is overwritten. ACTIVE success
// returns only unregister_receipt; recovery success returns only
// recovery_terminal_receipt, which can authorize a terminal maintenance
// report but can never authorize ACTIVE unregister.
//
// The stream-day `maintenance` directory is an earlier scaffolding
// prerequisite and is never created by this function.
[[nodiscard]] SealedRawCertificatePublishResultV1
PublishSealedRawCertificateV1(
    const RawWriterLease& lease,
    std::unique_ptr<RawReserveAuthorizedActionV1>
        authorized_action,
    const BuiltSealedRawCertificateV1& certificate,
    std::string* diagnostic = nullptr) noexcept;

}  // namespace l2flow::ingress
