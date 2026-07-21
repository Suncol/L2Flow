#include "raw_v1_fuzz_harness.h"

#include <cstddef>
#include <cstdint>
#include <span>

extern "C" int LLVMFuzzerTestOneInput(
    const std::uint8_t* data,
    std::size_t size) {
    l2flow::test::RunRawV1FuzzInput(
        std::span<const std::uint8_t>(data, size));
    return 0;
}
