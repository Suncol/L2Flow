#pragma once

#include "l2flow/ingress/raw_reserve_coordinator.h"
#include "l2flow/ingress/scaffolding_finalization_report_v1.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

namespace l2flow::ingress {

struct ScaffoldingFinalizationReportPublishResultV1;

// Process-local proof for the domain-level
// reserve-audit/emergency-reports publication.  The pre-created directory
// chain is retained descriptor-by-descriptor; this receipt never implies
// that stream scaffolding or a stream maintenance directory existed.
class ScaffoldingFinalizationReportReceiptV1 final {
public:
    ~ScaffoldingFinalizationReportReceiptV1();

    ScaffoldingFinalizationReportReceiptV1(
        const ScaffoldingFinalizationReportReceiptV1&) =
        delete;
    ScaffoldingFinalizationReportReceiptV1& operator=(
        const ScaffoldingFinalizationReportReceiptV1&) =
        delete;
    ScaffoldingFinalizationReportReceiptV1(
        ScaffoldingFinalizationReportReceiptV1&&) = delete;
    ScaffoldingFinalizationReportReceiptV1& operator=(
        ScaffoldingFinalizationReportReceiptV1&&) = delete;

    [[nodiscard]] const RawV1Digest&
    report_sha256() const noexcept {
        return report_sha256_;
    }
    [[nodiscard]] std::string_view filename()
        const noexcept {
        return filename_;
    }
    [[nodiscard]] std::size_t byte_count()
        const noexcept {
        return byte_count_;
    }
    [[nodiscard]] bool Validate(
        std::string* diagnostic = nullptr) const noexcept;

private:
    friend ScaffoldingFinalizationReportPublishResultV1
    PublishScaffoldingFinalizationReportV1(
        std::unique_ptr<RawReserveFinalizationActionV1>,
        const BuiltScaffoldingFinalizationReportV1&,
        const struct FinalizationReportStoreHooksV1*,
        std::string*) noexcept;
    friend class RawReserveRegistryCoordinatorV1;

    ScaffoldingFinalizationReportReceiptV1(
        ReserveFinalizationActionKeyV1 key,
        RawReserveGenerationActionTokenV1 token,
        ReserveStateV1Digest immutable_grant_sha256,
        FinalizationActionPlanV1 plan,
        std::uint8_t grant_flags,
        std::uint64_t byte_cap,
        std::uint32_t inode_cap,
        std::uint64_t debit_generation,
        ReserveStateV1Identity executor_instance,
        RawV1Digest report_sha256,
        std::size_t byte_count,
        std::string filename,
        int raw_root_fd,
        int reserve_audit_fd,
        int report_parent_fd,
        int report_fd,
        std::uint64_t raw_root_device,
        std::uint64_t raw_root_inode,
        std::uint64_t reserve_audit_device,
        std::uint64_t reserve_audit_inode,
        std::uint64_t report_parent_device,
        std::uint64_t report_parent_inode,
        std::uint64_t report_device,
        std::uint64_t report_inode,
        std::uint64_t report_blocks) noexcept;

    ReserveFinalizationActionKeyV1 key_{};
    RawReserveGenerationActionTokenV1 token_{};
    ReserveStateV1Digest immutable_grant_sha256_{};
    FinalizationActionPlanV1 plan_{};
    std::uint8_t grant_flags_ = 0U;
    std::uint64_t byte_cap_ = 0U;
    std::uint32_t inode_cap_ = 0U;
    std::uint64_t debit_generation_ = 0U;
    ReserveStateV1Identity executor_instance_{};
    RawV1Digest report_sha256_{};
    std::size_t byte_count_ = 0U;
    std::string filename_;

    int raw_root_fd_ = -1;
    int reserve_audit_fd_ = -1;
    int report_parent_fd_ = -1;
    int report_fd_ = -1;
    std::uint64_t raw_root_device_ = 0U;
    std::uint64_t raw_root_inode_ = 0U;
    std::uint64_t reserve_audit_device_ = 0U;
    std::uint64_t reserve_audit_inode_ = 0U;
    std::uint64_t report_parent_device_ = 0U;
    std::uint64_t report_parent_inode_ = 0U;
    std::uint64_t report_device_ = 0U;
    std::uint64_t report_inode_ = 0U;
    std::uint64_t report_blocks_ = 0U;
    bool consumed_ = false;
};

}  // namespace l2flow::ingress
