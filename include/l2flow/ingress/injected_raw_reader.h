#pragma once

#include "l2flow/ingress/injected_raw_v1.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <vector>

namespace l2flow::ingress {

// Every span retains an immutable shared owner. A copied or moved view
// therefore remains valid after the scan result that created it is destroyed.
class InjectedRawRecordView final {
public:
    InjectedRawRecordView(const InjectedRawRecordView&) = default;
    InjectedRawRecordView& operator=(
        const InjectedRawRecordView&) = default;
    InjectedRawRecordView(InjectedRawRecordView&&) noexcept = default;
    InjectedRawRecordView& operator=(
        InjectedRawRecordView&&) noexcept = default;
    ~InjectedRawRecordView() = default;

    [[nodiscard]] const InjectedRawRecordHeaderV1& header()
        const noexcept {
        return header_;
    }
    [[nodiscard]] const InjectedRawParentLocator& parent()
        const noexcept {
        return parent_;
    }
    [[nodiscard]] RawReplayProvenance parent_provenance()
        const noexcept {
        return header_.parent_provenance;
    }
    [[nodiscard]] const std::optional<
        InjectedRawMutationEvidence>& mutation() const noexcept {
        return mutation_;
    }
    [[nodiscard]] std::span<const std::byte> vendor_head()
        const noexcept {
        return vendor_head_;
    }
    [[nodiscard]] std::span<const std::byte> vendor_body()
        const noexcept {
        return vendor_body_;
    }
    [[nodiscard]] std::uint64_t record_start_offset()
        const noexcept {
        return record_start_offset_;
    }
    [[nodiscard]] std::uint64_t record_end_offset()
        const noexcept {
        return record_end_offset_;
    }

private:
    friend struct InjectedRawRecordViewFactory;

    InjectedRawRecordView(
        std::shared_ptr<const std::vector<std::byte>> owner,
        InjectedRawRecordHeaderV1 header,
        InjectedRawParentLocator parent,
        std::optional<InjectedRawMutationEvidence> mutation,
        std::span<const std::byte> vendor_head,
        std::span<const std::byte> vendor_body,
        std::uint64_t record_start_offset,
        std::uint64_t record_end_offset) noexcept;

    std::shared_ptr<const std::vector<std::byte>> owner_;
    InjectedRawRecordHeaderV1 header_{};
    InjectedRawParentLocator parent_{};
    std::optional<InjectedRawMutationEvidence> mutation_;
    std::span<const std::byte> vendor_head_{};
    std::span<const std::byte> vendor_body_{};
    std::uint64_t record_start_offset_ = 0U;
    std::uint64_t record_end_offset_ = 0U;
};

struct InjectedRawSegmentScanResult final {
    InjectedRawV1Error error = InjectedRawV1Error::kNone;
    std::uint64_t error_offset = 0U;
    InjectedRawSegmentHeaderV1 segment{};
    std::vector<InjectedRawRecordView> records;
    std::vector<InjectedRawParentLocator> dropped_locators;

    [[nodiscard]] bool ok() const noexcept {
        return error == InjectedRawV1Error::kNone;
    }
};

// Validates the complete immutable byte vector. There is no durable-prefix
// mode: InjectedRawV1 is a synthetic fixture and trailing bytes are rejected.
[[nodiscard]] InjectedRawSegmentScanResult ScanInjectedRawSegmentV1(
    std::shared_ptr<const std::vector<std::byte>> buffer)
    noexcept;

}  // namespace l2flow::ingress
