#pragma once

#include "l2flow/common/sha256.h"
#include "l2flow/control/control_record_v1.h"

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

namespace l2flow::control {

struct SubscriptionStatusV1 final {
    std::uint32_t service_id = 0U;
    std::uint32_t service_version = 0U;
    std::uint32_t message_id = 0U;
    std::uint32_t status = 0U;

    [[nodiscard]] friend constexpr bool operator==(
        const SubscriptionStatusV1&,
        const SubscriptionStatusV1&) noexcept = default;
};

enum class SysControlDecodeErrorV1 : std::uint16_t {
    kNone = 0U,
    kNullOutput = 1U,
    kUnsupportedMessage = 2U,
    kUnsupportedServiceVersion = 3U,
    kTruncated = 4U,
    kOffsetInvalid = 5U,
    kRangeOverlap = 6U,
    kCountExceeded = 7U,
    kDuplicateSubscriptionKey = 8U,
    kResourceExhausted = 9U,
};

struct DecodedSysControlV1 final {
    ControlTypeV1 control_type = ControlTypeV1::kLogonFailure;
    std::uint32_t return_or_error_code = 0U;
    std::uint32_t response_entry_count = 0U;
    std::vector<SubscriptionStatusV1> subscription_statuses;
    l2flow::common::Sha256Digest response_manifest_sha256{};
    bool response_manifest_present = false;
    bool noncanonical_empty_offset = false;
};

[[nodiscard]] SysControlDecodeErrorV1 DecodeSysControlV1(
    std::uint16_t service_version,
    std::uint16_t message_id,
    std::span<const std::byte> body,
    DecodedSysControlV1* output) noexcept;

// Canonical response-manifest identity: numeric tuple order, explicit
// little-endian u32 fields and a fixed domain. Duplicate keys are invalid.
[[nodiscard]] l2flow::common::Sha256Digest
ComputeSubscriptionResponseManifestSha256V1(
    std::span<const SubscriptionStatusV1> sorted_statuses) noexcept;

[[nodiscard]] std::uint64_t SysDecodeQualityFlagsV1(
    SysControlDecodeErrorV1 error) noexcept;

}  // namespace l2flow::control
