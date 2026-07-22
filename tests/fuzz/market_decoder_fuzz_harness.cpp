#include <array>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <vector>

extern "C" int LLVMFuzzerTestOneInput(
    const std::uint8_t* data,
    std::size_t size);

namespace {

std::uint64_t Next(std::uint64_t* state) noexcept {
    std::uint64_t value = *state;
    value ^= value << 13U;
    value ^= value >> 7U;
    value ^= value << 17U;
    *state = value;
    return value;
}

void Fill(std::vector<std::uint8_t>* input, std::uint64_t* state) {
    for (std::uint8_t& byte : *input) {
        byte = static_cast<std::uint8_t>(Next(state) & 0xffU);
    }
}

}  // namespace

int main() {
    constexpr std::array<std::size_t, 15U> kBodyEdges{
        0U, 1U, 57U, 58U, 69U, 70U, 223U, 224U,
        247U, 248U, 249U, 511U, 1'023U, 4'095U, 4'096U};
    std::uint64_t state = 0x4c32464c4f575034ULL;
    std::vector<std::uint8_t> input;

    // Exercise every core selector and both supported/unknown-version paths
    // at all fixed-size boundaries before the pseudorandom corpus.
    for (std::size_t selector = 0U; selector < 5U; ++selector) {
        for (std::size_t version_mode = 0U;
             version_mode < 2U;
             ++version_mode) {
            for (const std::size_t body_size : kBodyEdges) {
                input.resize(4U + body_size);
                Fill(&input, &state);
                input[0] = static_cast<std::uint8_t>(selector);
                input[1] = static_cast<std::uint8_t>(version_mode);
                input[2] = 0xffU;
                input[3] = 0x7fU;
                if (LLVMFuzzerTestOneInput(input.data(), input.size()) != 0) {
                    return 1;
                }
            }
        }
    }

    constexpr std::size_t kDeterministicCases = 50'000U;
    for (std::size_t iteration = 0U;
         iteration < kDeterministicCases;
         ++iteration) {
        const std::size_t body_size = static_cast<std::size_t>(
            Next(&state) % 4'097U);
        input.resize(4U + body_size);
        Fill(&input, &state);
        input[0] = static_cast<std::uint8_t>(iteration % 5U);
        // Alternate schema-valid parsing with fail-closed unknown versions.
        input[1] = static_cast<std::uint8_t>(iteration & 1U);
        if (LLVMFuzzerTestOneInput(input.data(), input.size()) != 0) {
            return 1;
        }
    }

    std::cout << "phase4 deterministic decoder fuzz smoke passed: "
              << kDeterministicCases + 5U * 2U * kBodyEdges.size()
              << " cases\n";
    return 0;
}
