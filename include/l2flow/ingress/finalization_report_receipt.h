#pragma once

#include "l2flow/ingress/finalization_report_v1.h"
#include "l2flow/ingress/raw_reserve_coordinator.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

namespace l2flow::ingress {

struct FinalizationReportPublishResultV1;

// Process-local, non-forgeable proof for one exact routed
// FinalizationReportV1.  Construction is publisher-only.  The retained
// Raw-root, stream-day, maintenance and report descriptors let the
// coordinator's later COMPLETE+DONE transition revalidate the complete
// path chain without trusting caller-provided pathnames.
class FinalizationReportReceiptV1 final {
public:
    ~FinalizationReportReceiptV1();

    FinalizationReportReceiptV1(
        const FinalizationReportReceiptV1&) = delete;
    FinalizationReportReceiptV1& operator=(
        const FinalizationReportReceiptV1&) = delete;
    FinalizationReportReceiptV1(
        FinalizationReportReceiptV1&&) = delete;
    FinalizationReportReceiptV1& operator=(
        FinalizationReportReceiptV1&&) = delete;

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

    // Filesystem-only validation of the retained evidence.  It deliberately
    // does not mutate coordinator state and does not replace the later
    // durable action/grant validation performed by the coordinator.
    [[nodiscard]] bool Validate(
        std::string* diagnostic = nullptr) const noexcept;

private:
    friend FinalizationReportPublishResultV1
    PublishFinalizationReportV1(
        std::unique_ptr<RawReserveFinalizationActionV1>,
        const BuiltFinalizationReportV1&,
        const struct FinalizationReportStoreHooksV1*,
        std::string*) noexcept;
    friend class RawReserveRegistryCoordinatorV1;

    FinalizationReportReceiptV1(
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
        std::string stream_slug,
        int raw_root_fd,
        int capture_date_fd,
        int route_fd,
        int report_parent_fd,
        int report_fd,
        std::uint64_t raw_root_device,
        std::uint64_t raw_root_inode,
        std::uint64_t capture_date_device,
        std::uint64_t capture_date_inode,
        std::uint64_t route_device,
        std::uint64_t route_inode,
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
    std::string stream_slug_;

    int raw_root_fd_ = -1;
    int capture_date_fd_ = -1;
    int route_fd_ = -1;
    int report_parent_fd_ = -1;
    int report_fd_ = -1;
    std::uint64_t raw_root_device_ = 0U;
    std::uint64_t raw_root_inode_ = 0U;
    std::uint64_t capture_date_device_ = 0U;
    std::uint64_t capture_date_inode_ = 0U;
    std::uint64_t route_device_ = 0U;
    std::uint64_t route_inode_ = 0U;
    std::uint64_t report_parent_device_ = 0U;
    std::uint64_t report_parent_inode_ = 0U;
    std::uint64_t report_device_ = 0U;
    std::uint64_t report_inode_ = 0U;
    std::uint64_t report_blocks_ = 0U;
    bool consumed_ = false;
};

}  // namespace l2flow::ingress
