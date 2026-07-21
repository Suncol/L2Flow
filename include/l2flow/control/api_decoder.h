#pragma once

#include "l2flow/common/sha256.h"
#include "l2flow/control/checked_body_view.h"
#include "l2flow/control/control_record_v1.h"

#include <cstddef>
#include <cstdint>
#include <span>

namespace l2flow::control {

enum class ApiControlDecodeErrorV1 : std::uint16_t {
    kNone = 0U,
    kNullOutput = 1U,
    kUnsupportedMessage = 2U,
    kUnsupportedServiceVersion = 3U,
    kTruncated = 4U,
    kOffsetInvalid = 5U,
    kRangeOverlap = 6U,
    kResourceExhausted = 7U,
};

struct DecodedApiControlV1 final {
    ControlTypeV1 control_type = ControlTypeV1::kConnecting;
    l2flow::common::Sha256Digest address_sha256{};
    l2flow::common::Sha256Digest error_text_sha256{};
    bool address_present = false;
    bool error_text_present = false;
    bool noncanonical_empty_offset = false;
};

[[nodiscard]] ApiControlDecodeErrorV1 DecodeApiControlV1(
    std::uint16_t service_version,
    std::uint16_t message_id,
    std::span<const std::byte> body,
    DecodedApiControlV1* output) noexcept;

[[nodiscard]] std::uint64_t ApiDecodeQualityFlagsV1(
    ApiControlDecodeErrorV1 error) noexcept;

}  // namespace l2flow::control
