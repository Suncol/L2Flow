#include "l2flow/common/sha256.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <span>
#include <string_view>
#include <vector>

namespace common = l2flow::common;

namespace {

struct TestContext final {
    int failures = 0;

    void Expect(bool condition, std::string_view message) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << message << '\n';
        }
    }
};

void TestStandardVectors(TestContext* test) {
    test->Expect(
        common::Sha256Hex(common::ComputeSha256("")) ==
            "e3b0c44298fc1c149afbf4c8996fb924"
            "27ae41e4649b934ca495991b7852b855",
        "SHA-256 empty-message vector");
    test->Expect(
        common::Sha256Hex(common::ComputeSha256("abc")) ==
            "ba7816bf8f01cfea414140de5dae2223"
            "b00361a396177a9cb410ff61f20015ad",
        "SHA-256 abc vector");
    test->Expect(
        common::Sha256Hex(
            common::ComputeSha256("123456789")) ==
            "15e2b0d3c33891ebb0f1ef609ec41942"
            "0c20e320ce94c65fbc8c3312448eb225",
        "SHA-256 123456789 vector");
}

void TestEverySmallChunkSize(TestContext* test) {
    std::vector<std::byte> input(1027U);
    for (std::size_t index = 0U; index < input.size(); ++index) {
        input[index] = static_cast<std::byte>(
            static_cast<std::uint8_t>(
                (index * 37U + index / 7U) & 0xffU));
    }
    const common::Sha256Digest expected =
        common::ComputeSha256(input);

    for (std::size_t chunk_size = 1U;
         chunk_size <= 129U;
         ++chunk_size) {
        common::Sha256Hasher hasher;
        std::size_t offset = 0U;
        bool updates_ok = true;
        while (offset < input.size()) {
            const std::size_t count =
                std::min(chunk_size, input.size() - offset);
            updates_ok =
                updates_ok &&
                hasher.Update(std::span<const std::byte>(
                    input.data() + offset, count));
            offset += count;
        }
        common::Sha256Digest actual{};
        test->Expect(
            updates_ok && hasher.Finalize(&actual) &&
                actual == expected &&
                hasher.total_bytes() == input.size(),
            "incremental SHA-256 equals one-shot SHA-256 for every small chunk size");
    }
}

void TestPartialBufferAndLifecycle(TestContext* test) {
    constexpr std::array<std::byte, 3U> first{
        std::byte{'a'}, std::byte{'b'}, std::byte{'c'}};
    constexpr std::array<std::byte, 3U> second{
        std::byte{'d'}, std::byte{'e'}, std::byte{'f'}};
    common::Sha256Hasher hasher;
    common::Sha256Digest digest{};
    const bool accepted =
        hasher.Update(first) &&
        hasher.Update(std::span<const std::byte>{}) &&
        hasher.Update(second) &&
        hasher.Finalize(&digest);
    test->Expect(
        accepted &&
            digest == common::ComputeSha256("abcdef") &&
            hasher.total_bytes() == 6U &&
            hasher.finalized(),
        "successive sub-block updates preserve buffered bytes");

    common::Sha256Digest unchanged{};
    unchanged.fill(std::byte{0xa5});
    const common::Sha256Digest sentinel = unchanged;
    test->Expect(
        !hasher.Update(first) &&
            !hasher.Finalize(&unchanged) &&
            unchanged == sentinel,
        "finalized SHA-256 state rejects further use without changing output");
}

}  // namespace

int main() {
    TestContext test;
    TestStandardVectors(&test);
    TestEverySmallChunkSize(&test);
    TestPartialBufferAndLifecycle(&test);
    if (test.failures != 0) {
        std::cerr << test.failures
                  << " SHA-256 test(s) failed\n";
        return 1;
    }
    std::cout << "SHA-256 tests passed\n";
    return 0;
}
