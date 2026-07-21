#pragma once

#include "l2flow/ingress/finalization_report_receipt.h"
#include "l2flow/ingress/finalization_report_v1.h"
#include "l2flow/ingress/raw_reserve_coordinator.h"

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

namespace l2flow::ingress {

inline constexpr std::uint32_t
    kFinalizationReportStoreMaximumCandidatesV1 = 4096U;
inline constexpr std::string_view
    kFinalizationReportV1TemporarySuffix =
        ".finalization-report-v1.tmp";
inline constexpr std::string_view
    kScaffoldingFinalizationReportV1TemporarySuffix =
        ".scaffolding-finalization-report-v1.tmp";

enum class FinalizationReportStoreErrorV1 : std::uint8_t {
    kNone = 0U,
    kInvalidArgument,
    kAuthorizationRejected,
    kPlanMismatch,
    kTargetMismatch,
    kReportParentMissing,
    kUnsafeReportParent,
    kCandidateLimitExceeded,
    kMalformedCandidateName,
    kUnsafeCandidate,
    kCandidateConflict,
    kTemporaryCreate,
    kWriteFailure,
    kSyncFailure,
    kPublishConflict,
    kReadbackFailure,
    kInjectedInterruption,
    kAllocationFailure,
};

[[nodiscard]] std::string_view
FinalizationReportStoreErrorV1Name(
    FinalizationReportStoreErrorV1 error) noexcept;

enum class FinalizationReportStoreDispositionV1
    : std::uint8_t {
    kNone = 0U,
    kPublishedNew,
    kAdoptedCompleteTemporary,
    kAcceptedExistingFinal,
    kAcceptedExistingFinalAndRemovedIdenticalTemporary,
};

enum class FinalizationReportStoreMutationPointV1
    : std::uint8_t {
    kBeforeTemporaryCreate = 1U,
    kBeforeWrite = 2U,
    kBeforeFileSync = 3U,
    kBeforePublishRename = 4U,
    kBeforeParentSync = 5U,
    kBeforeIdenticalTemporaryCleanup = 6U,
    kBeforeReadback = 7U,
};

struct FinalizationReportStoreHooksV1 final {
    bool (*allow)(
        FinalizationReportStoreMutationPointV1,
        void*) noexcept = nullptr;
    void* context = nullptr;
};

struct FinalizationReportPublishResultV1 final {
    FinalizationReportStoreErrorV1 error =
        FinalizationReportStoreErrorV1::kNone;
    FinalizationReportStoreDispositionV1 disposition =
        FinalizationReportStoreDispositionV1::kNone;
    RawV1Digest report_sha256{};
    std::string filename;
    std::unique_ptr<FinalizationReportReceiptV1> receipt;
    std::uint32_t observed_candidate_count = 0U;
    bool file_synced = false;
    bool directory_synced = false;

    [[nodiscard]] bool ok() const noexcept {
        return error ==
                   FinalizationReportStoreErrorV1::kNone &&
               receipt != nullptr;
    }
};

// Publishes a routed RAW_FINALIZATION or RAW_ANCHOR_ONLY report beneath the
// pre-existing stream-day maintenance directory.  The exact sole DEBITED
// action remains shared-gate protected through every create/write/sync/
// rename/unlink/readback barrier.  This function returns evidence only; it
// never completes the receipt or mutates coordinator state.
[[nodiscard]] FinalizationReportPublishResultV1
PublishFinalizationReportV1(
    std::unique_ptr<RawReserveFinalizationActionV1> action,
    const BuiltFinalizationReportV1& report,
    const FinalizationReportStoreHooksV1* hooks = nullptr,
    std::string* diagnostic = nullptr) noexcept;

}  // namespace l2flow::ingress
