#pragma once

#include "l2flow/ingress/finalization_report_store.h"
#include "l2flow/ingress/scaffolding_finalization_report_receipt.h"
#include "l2flow/ingress/scaffolding_finalization_report_v1.h"

#include <memory>
#include <string>

namespace l2flow::ingress {

struct ScaffoldingFinalizationReportPublishResultV1 final {
    FinalizationReportStoreErrorV1 error =
        FinalizationReportStoreErrorV1::kNone;
    FinalizationReportStoreDispositionV1 disposition =
        FinalizationReportStoreDispositionV1::kNone;
    RawV1Digest report_sha256{};
    std::string filename;
    std::unique_ptr<
        ScaffoldingFinalizationReportReceiptV1> receipt;
    std::uint32_t observed_candidate_count = 0U;
    bool file_synced = false;
    bool directory_synced = false;

    [[nodiscard]] bool ok() const noexcept {
        return error ==
                   FinalizationReportStoreErrorV1::kNone &&
               receipt != nullptr;
    }
};

// Publishes SCAFFOLDING_ONLY audit evidence beneath the pre-created
// Raw-root/reserve-audit/emergency-reports chain.  No stream route or
// maintenance directory is required or created.
[[nodiscard]] ScaffoldingFinalizationReportPublishResultV1
PublishScaffoldingFinalizationReportV1(
    std::unique_ptr<RawReserveFinalizationActionV1> action,
    const BuiltScaffoldingFinalizationReportV1& report,
    const FinalizationReportStoreHooksV1* hooks = nullptr,
    std::string* diagnostic = nullptr) noexcept;

}  // namespace l2flow::ingress
