#include "l2flow/market/mainland_a_share_filter_v1.h"

#include <array>
#include <cstddef>
#include <iostream>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace {

namespace market = l2flow::market;

class TestContext final {
public:
    void Expect(bool condition, std::string_view description) {
        if (!condition) {
            ++failures_;
            std::cerr << "FAIL: " << description << '\n';
        }
    }

    [[nodiscard]] int failures() const noexcept {
        return failures_;
    }

private:
    int failures_ = 0;
};

std::vector<std::byte> Bytes(std::string_view value) {
    const std::span<const char> characters(value.data(), value.size());
    const std::span<const std::byte> bytes = std::as_bytes(characters);
    return {bytes.begin(), bytes.end()};
}

bool IsAShare(
    market::MainlandExchangeV1 exchange,
    std::string_view security_id) {
    const std::vector<std::byte> bytes = Bytes(security_id);
    return market::IsMainlandAShareSecurityIdV1(exchange, bytes);
}

void TestShanghaiRanges(TestContext* test) {
    constexpr std::array<std::string_view, 10U> kAccepted{
        "600000", "600999",
        "601000", "601999",
        "603000", "603999",
        "605000", "605999",
        "688000", "688999",
    };
    for (const std::string_view security_id : kAccepted) {
        test->Expect(
            IsAShare(
                market::MainlandExchangeV1::kShanghai, security_id),
            std::string("Shanghai accepts ") +
                std::string(security_id));
    }

    constexpr std::array<std::string_view, 12U> kRejected{
        "599999", "602000", "602999", "604000",
        "604999", "606000", "687999", "689000",
        "689999", "900000", "510000", "000001",
    };
    for (const std::string_view security_id : kRejected) {
        test->Expect(
            !IsAShare(
                market::MainlandExchangeV1::kShanghai, security_id),
            std::string("Shanghai rejects ") +
                std::string(security_id));
    }
}

void TestShenzhenRanges(TestContext* test) {
    constexpr std::array<std::string_view, 16U> kAccepted{
        "000001", "000999",
        "001200", "001999",
        "002000", "002999",
        "003000", "003999",
        "004000", "004999",
        "300000", "300999",
        "301000", "301999",
        "309000", "309799",
    };
    for (const std::string_view security_id : kAccepted) {
        test->Expect(
            IsAShare(
                market::MainlandExchangeV1::kShenzhen, security_id),
            std::string("Shenzhen accepts ") +
                std::string(security_id));
    }

    constexpr std::array<std::string_view, 14U> kRejected{
        "000000", "001000", "001001", "001199",
        "005000", "299999", "309800", "309999",
        "310000", "200000", "399001", "600000",
        "920000", "999999",
    };
    for (const std::string_view security_id : kRejected) {
        test->Expect(
            !IsAShare(
                market::MainlandExchangeV1::kShenzhen, security_id),
            std::string("Shenzhen rejects ") +
                std::string(security_id));
    }
}

void TestBeijingRange(TestContext* test) {
    constexpr std::array<std::string_view, 3U> kAccepted{
        "920000", "920500", "920999",
    };
    for (const std::string_view security_id : kAccepted) {
        test->Expect(
            IsAShare(
                market::MainlandExchangeV1::kBeijing, security_id),
            std::string("Beijing accepts ") +
                std::string(security_id));
    }

    constexpr std::array<std::string_view, 7U> kRejected{
        "919999", "921000", "430001", "830001",
        "870001", "880001", "600000",
    };
    for (const std::string_view security_id : kRejected) {
        test->Expect(
            !IsAShare(
                market::MainlandExchangeV1::kBeijing, security_id),
            std::string("Beijing rejects ") +
                std::string(security_id));
    }
}

void TestExchangeIdentityIsAuthoritative(TestContext* test) {
    test->Expect(
        IsAShare(market::MainlandExchangeV1::kShenzhen, "000001"),
        "000001 is accepted for Shenzhen");
    test->Expect(
        !IsAShare(market::MainlandExchangeV1::kShanghai, "000001"),
        "000001 is not accepted for Shanghai");
    test->Expect(
        !IsAShare(market::MainlandExchangeV1::kBeijing, "000001"),
        "000001 is not accepted for Beijing");

    test->Expect(
        IsAShare(market::MainlandExchangeV1::kShanghai, "600000"),
        "600000 is accepted for Shanghai");
    test->Expect(
        !IsAShare(market::MainlandExchangeV1::kShenzhen, "600000"),
        "600000 is not accepted for Shenzhen");

    test->Expect(
        IsAShare(market::MainlandExchangeV1::kBeijing, "920000"),
        "920000 is accepted for Beijing");
    test->Expect(
        !IsAShare(market::MainlandExchangeV1::kShanghai, "920000"),
        "920000 is not accepted for Shanghai");
    test->Expect(
        !IsAShare(market::MainlandExchangeV1::kShenzhen, "920000"),
        "920000 is not accepted for Shenzhen");

    test->Expect(
        !IsAShare(market::MainlandExchangeV1::kUnknown, "600000"),
        "unknown exchange rejects an otherwise valid code");
}

void TestExactAsciiShape(TestContext* test) {
    constexpr std::array<std::string_view, 8U> kRejected{
        "", "92000", "9200000", "92000001",
        "92A000", "920 00", "+92000",
        "\xEF\xBC\x99"
        "\xEF\xBC\x92",
    };
    for (const std::string_view security_id : kRejected) {
        test->Expect(
            !IsAShare(
                market::MainlandExchangeV1::kBeijing, security_id),
            std::string("shape validation rejects byte sequence of size ") +
                std::to_string(security_id.size()));
    }

    const std::array<std::byte, 6U> embedded_null{
        std::byte{'9'}, std::byte{'2'}, std::byte{'0'},
        std::byte{0U}, std::byte{'0'}, std::byte{'0'},
    };
    test->Expect(
        !market::IsMainlandAShareSecurityIdV1(
            market::MainlandExchangeV1::kBeijing,
            embedded_null),
        "embedded NUL is not an ASCII digit");

    const std::array<std::byte, 6U> high_byte{
        std::byte{'9'}, std::byte{'2'}, std::byte{'0'},
        std::byte{0xffU}, std::byte{'0'}, std::byte{'0'},
    };
    test->Expect(
        !market::IsMainlandAShareSecurityIdV1(
            market::MainlandExchangeV1::kBeijing,
            high_byte),
        "non-ASCII byte is rejected");
}

}  // namespace

int main() {
    TestContext test;
    TestShanghaiRanges(&test);
    TestShenzhenRanges(&test);
    TestBeijingRange(&test);
    TestExchangeIdentityIsAuthoritative(&test);
    TestExactAsciiShape(&test);

    if (test.failures() != 0) {
        std::cerr << test.failures()
                  << " mainland A-share filter test(s) failed\n";
        return 1;
    }
    std::cout << "mainland A-share filter tests passed\n";
    return 0;
}
