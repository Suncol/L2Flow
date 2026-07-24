#include "l2flow/common/identity128.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace common = l2flow::common;

namespace {

struct Step final {
    std::vector<std::byte> bytes;
    int error_number = 0;
};

class ScriptedEntropy final : public common::IdentityEntropySource {
public:
    explicit ScriptedEntropy(std::vector<Step> steps)
        : steps_(std::move(steps)) {}

    common::EntropyReadResult ReadSome(
        std::span<std::byte> output) noexcept override {
        if (next_ >= steps_.size()) {
            return {};
        }
        const Step& step = steps_[next_++];
        if (step.error_number != 0) {
            return {0U, step.error_number};
        }
        if (step.bytes.size() > output.size()) {
            return {step.bytes.size(), 0};
        }
        std::copy(
            step.bytes.begin(),
            step.bytes.end(),
            output.begin());
        return {step.bytes.size(), 0};
    }

private:
    std::vector<Step> steps_;
    std::size_t next_ = 0U;
};

struct TestContext final {
    void Expect(bool condition, const std::string& description) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << description << '\n';
        }
    }

    int failures = 0;
};

std::vector<std::byte> Bytes(
    std::uint8_t first,
    std::size_t count) {
    std::vector<std::byte> result(count);
    for (std::size_t index = 0U; index < count; ++index) {
        result[index] = static_cast<std::byte>(
            static_cast<std::uint8_t>(
                first + static_cast<std::uint8_t>(index)));
    }
    return result;
}

}  // namespace

int main() {
    TestContext test;

    ScriptedEntropy short_reads({
        {{}, EINTR},
        {Bytes(1U, 3U), 0},
        {Bytes(4U, 5U), 0},
        {Bytes(9U, 8U), 0},
    });
    common::Identity128 identity{};
    int error_number = -1;
    test.Expect(
        common::GenerateIdentity128(
            short_reads, &identity, &error_number),
        "EINTR and short reads are retried");
    test.Expect(error_number == 0, "successful generation clears errno");
    test.Expect(
        common::Identity128Hex(identity) ==
            "0102030405060708090a0b0c0d0e0f10",
        "identity uses the exact entropy bytes");

    common::Identity128 parsed{};
    test.Expect(
        common::ParseIdentity128Hex(
            "0102030405060708090a0b0c0d0e0f10",
            &parsed) &&
            parsed == identity,
        "lowercase identity hex round-trips");
    test.Expect(
        !common::ParseIdentity128Hex(
            "0102030405060708090A0B0C0D0E0F10",
            &parsed),
        "uppercase identity hex is rejected");
    test.Expect(
        !common::ParseIdentity128Hex(
            "00000000000000000000000000000000",
            &parsed),
        "all-zero identity text is rejected");
    test.Expect(
        common::ParseCanonicalUuid128(
            "01020304-0506-0708-090a-0b0c0d0e0f10",
            &parsed) &&
            parsed == identity,
        "canonical lowercase UUID text maps to the same 16 bytes");
    test.Expect(
        !common::ParseCanonicalUuid128(
            "01020304-0506-0708-090A-0B0C0D0E0F10",
            &parsed),
        "uppercase UUID text is rejected");

    common::Identity128 unchanged{};
    unchanged.fill(std::byte{0x5a});
    ScriptedEntropy zero_progress({});
    test.Expect(
        !common::GenerateIdentity128(
            zero_progress, &unchanged, &error_number) &&
            error_number == EIO,
        "zero progress fails closed");
    test.Expect(
        std::all_of(
            unchanged.begin(),
            unchanged.end(),
            [](std::byte value) {
                return value == std::byte{0x5a};
            }),
        "failure does not change caller output");

    ScriptedEntropy all_zero({
        {std::vector<std::byte>(16U), 0},
    });
    test.Expect(
        !common::GenerateIdentity128(
            all_zero, &unchanged, &error_number) &&
            error_number == EIO,
        "all-zero kernel output fails closed");

    ScriptedEntropy hard_failure({
        {Bytes(1U, 4U), 0},
        {{}, EACCES},
    });
    test.Expect(
        !common::GenerateIdentity128(
            hard_failure, &unchanged, &error_number) &&
            error_number == EACCES,
        "non-EINTR errors are preserved");

    if (test.failures != 0) {
        std::cerr << test.failures
                  << " identity tests failed\n";
        return 1;
    }
    std::cout << "Identity tests passed\n";
    return 0;
}
