#include "l2flow/common/crc32c.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

namespace {

class TestContext final {
public:
    void Expect(bool condition, std::string_view description) {
        if (condition) {
            return;
        }
        ++failures_;
        std::cerr << "FAIL: " << description << '\n';
    }

    [[nodiscard]] int failures() const noexcept {
        return failures_;
    }

private:
    int failures_ = 0;
};

}  // namespace

int main() {
    using l2flow::common::ComputeCrc32c;
    using l2flow::common::Crc32cState;

    static_assert(
        noexcept(std::declval<Crc32cState&>().Reset()));
    static_assert(
        noexcept(std::declval<Crc32cState&>().Update(
            std::declval<std::span<const std::byte>>())));
    static_assert(
        noexcept(std::declval<const Crc32cState&>().Finalize()));
    static_assert(
        noexcept(ComputeCrc32c(
            std::declval<std::span<const std::byte>>())));

    TestContext test;

    test.Expect(
        ComputeCrc32c(std::string_view("123456789")) == 0xe3069283U,
        "standard CRC-32C check vector matches 0xE3069283");
    test.Expect(
        ComputeCrc32c(std::span<const std::byte>()) == 0U,
        "empty input has the standard CRC-32C value");

    Crc32cState incremental;
    incremental.Update(std::string_view("123"));
    incremental.Update(std::string_view("456"));
    incremental.Update(std::string_view("789"));
    test.Expect(
        incremental.Finalize() == 0xe3069283U,
        "incremental updates equal the standard check vector");
    test.Expect(
        incremental.Finalize() == 0xe3069283U,
        "Finalize does not consume incremental state");

    incremental.Reset();
    incremental.Update(std::string_view("123456789"));
    test.Expect(
        incremental.Finalize() == 0xe3069283U,
        "Reset restores the standard initial value");

    constexpr std::array<std::byte, 9U> byte_array = {
        std::byte{'1'}, std::byte{'2'}, std::byte{'3'},
        std::byte{'4'}, std::byte{'5'}, std::byte{'6'},
        std::byte{'7'}, std::byte{'8'}, std::byte{'9'}};
    const std::vector<std::byte> byte_vector(
        byte_array.begin(), byte_array.end());
    test.Expect(
        ComputeCrc32c(byte_vector) == 0xe3069283U,
        "vector of std::byte converts directly to the span API");

    const std::vector<std::uint8_t> uint8_vector = {
        static_cast<std::uint8_t>('1'),
        static_cast<std::uint8_t>('2'),
        static_cast<std::uint8_t>('3'),
        static_cast<std::uint8_t>('4'),
        static_cast<std::uint8_t>('5'),
        static_cast<std::uint8_t>('6'),
        static_cast<std::uint8_t>('7'),
        static_cast<std::uint8_t>('8'),
        static_cast<std::uint8_t>('9')};
    test.Expect(
        ComputeCrc32c(uint8_vector) == 0xe3069283U,
        "vector of uint8_t converts directly to the byte-span API");

    Crc32cState mixed_chunks;
    mixed_chunks.Update(std::span<const std::uint8_t>(
        uint8_vector.data(), 4U));
    mixed_chunks.Update(std::span<const std::byte>(
        byte_array.data() + 4U, byte_array.size() - 4U));
    test.Expect(
        mixed_chunks.Finalize() == 0xe3069283U,
        "incremental state accepts both supported byte-span forms");

    return test.failures() == 0 ? 0 : 1;
}
