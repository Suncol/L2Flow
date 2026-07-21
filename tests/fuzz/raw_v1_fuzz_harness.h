#pragma once

#include <cstddef>
#include <cstdint>
#include <span>

namespace l2flow::test {

inline constexpr std::size_t kRawV1FuzzMaxInputBytes =
    1024U * 1024U;

// Exercises Raw V1 byte decoders, the validating reader, and read-only
// recovery analysis. Inputs above the explicit bound are ignored so the same
// entry point is suitable for deterministic corpus tests and libFuzzer.
void RunRawV1FuzzInput(
    std::span<const std::uint8_t> input) noexcept;

}  // namespace l2flow::test
