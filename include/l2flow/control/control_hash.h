#pragma once

#include "l2flow/common/sha256.h"

#include <cstddef>
#include <span>
#include <string_view>

namespace l2flow::control {

// Domain-separated hash of one exact byte string. The input length is encoded
// as little-endian u64 before the bytes so concatenations cannot collide.
[[nodiscard]] l2flow::common::Sha256Digest HashControlTextV1(
    std::string_view domain,
    std::span<const std::byte> bytes) noexcept;

}  // namespace l2flow::control
