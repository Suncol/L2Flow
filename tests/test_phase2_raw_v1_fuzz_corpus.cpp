#include "fuzz/raw_v1_fuzz_harness.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <span>
#include <string>
#include <vector>

namespace {

[[nodiscard]] bool RunCorpus(
    const std::filesystem::path& directory,
    std::size_t* file_count) {
    if (file_count == nullptr ||
        !std::filesystem::is_directory(directory)) {
        return false;
    }
    *file_count = 0U;
    for (const std::filesystem::directory_entry& entry :
         std::filesystem::directory_iterator(directory)) {
        if (!entry.is_regular_file()) {
            continue;
        }
        std::ifstream input(entry.path(), std::ios::binary);
        const std::vector<std::uint8_t> bytes{
            std::istreambuf_iterator<char>(input),
            std::istreambuf_iterator<char>()};
        if (input.bad() ||
            bytes.size() >
                l2flow::test::kRawV1FuzzMaxInputBytes) {
            return false;
        }
        l2flow::test::RunRawV1FuzzInput(bytes);
        ++*file_count;
    }
    return *file_count != 0U;
}

void RunDeterministicProperties(std::size_t* case_count) {
    constexpr std::array<std::size_t, 13U> kBoundaries{
        0U, 1U, 15U, 16U, 47U, 48U, 95U,
        96U, 127U, 4095U, 4096U, 4097U, 8192U};
    std::uint64_t state = 0x243f6a8885a308d3ULL;
    for (std::uint8_t mode = 0U; mode < 8U; ++mode) {
        for (const std::size_t length : kBoundaries) {
            std::vector<std::uint8_t> input(length + 1U);
            input[0] = mode;
            for (std::size_t index = 1U;
                 index < input.size();
                 ++index) {
                state ^= state << 13U;
                state ^= state >> 7U;
                state ^= state << 17U;
                input[index] =
                    static_cast<std::uint8_t>(state & 0xffU);
            }
            l2flow::test::RunRawV1FuzzInput(input);
            ++*case_count;
        }
    }
    for (std::size_t iteration = 0U;
         iteration < 512U;
         ++iteration) {
        state ^= state << 13U;
        state ^= state >> 7U;
        state ^= state << 17U;
        const std::size_t length =
            static_cast<std::size_t>(state % 16384U);
        std::vector<std::uint8_t> input(length + 1U);
        input[0] = static_cast<std::uint8_t>(iteration & 0x07U);
        for (std::size_t index = 1U;
             index < input.size();
             ++index) {
            state ^= state << 13U;
            state ^= state >> 7U;
            state ^= state << 17U;
            input[index] =
                static_cast<std::uint8_t>(state & 0xffU);
        }
        l2flow::test::RunRawV1FuzzInput(input);
        ++*case_count;
    }
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr
            << "usage: test_phase2_raw_v1_fuzz_corpus "
               "<corpus-directory>\n";
        return 2;
    }
    std::size_t file_count = 0U;
    if (!RunCorpus(argv[1], &file_count)) {
        std::cerr << "failed to read non-empty bounded corpus\n";
        return 1;
    }
    std::size_t case_count = 0U;
    RunDeterministicProperties(&case_count);
    std::cout
        << "Raw V1 deterministic input-safety corpus passed: "
        << file_count << " files, "
        << case_count << " generated cases; "
        << "this is not crash/power-loss evidence\n";
    return 0;
}
