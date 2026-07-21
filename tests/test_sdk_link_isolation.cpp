#include "l2mock/l2_mock.h"

#include <array>
#include <cstdint>
#include <cstring>
#include <iostream>

namespace {

bool CheckInterlockedHelpers() {
    volatile int value = 0;
    return datayes::DllInterlockedIncrement(&value) == 1 &&
           datayes::DllInterlockedDecrement(&value) == 0 &&
           value == 0;
}

bool CheckEncodingHelper() {
    // UTF-8 for "中文", written as bytes so the test does not depend on the
    // compiler's source-code execution character set.
    constexpr std::array<char, 6> input = {
        static_cast<char>(0xe4),
        static_cast<char>(0xb8),
        static_cast<char>(0xad),
        static_cast<char>(0xe6),
        static_cast<char>(0x96),
        static_cast<char>(0x87)};
    std::array<char, 16> output{};
    std::array<char, 16> round_trip{};
    constexpr std::array<unsigned char, 4> gbk = {
        0xd6, 0xd0, 0xce, 0xc4};

    const uint32_t converted = datayes::mdl::DllConvertUTF8ToAnsi(
        input.data(),
        static_cast<uint32_t>(input.size()),
        output.data(),
        static_cast<uint32_t>(output.size()));

#if defined(L2MOCK_EXPECT_VENDOR_CONVERSION)
    // libmdl_api.so 2.13.234 converts these two code points to GBK.
    const uint32_t restored = datayes::mdl::DllConvertAnsiToUTF8(
        reinterpret_cast<const char*>(gbk.data()),
        static_cast<uint32_t>(gbk.size()),
        round_trip.data(),
        static_cast<uint32_t>(round_trip.size()));
    return converted == gbk.size() &&
           std::memcmp(output.data(),
                       gbk.data(),
                       gbk.size()) == 0 &&
           restored == input.size() &&
           std::memcmp(round_trip.data(),
                       input.data(),
                       input.size()) == 0;
#else
    // The standalone mock fallback deliberately preserves input bytes.
    const uint32_t restored = datayes::mdl::DllConvertAnsiToUTF8(
        reinterpret_cast<const char*>(gbk.data()),
        static_cast<uint32_t>(gbk.size()),
        round_trip.data(),
        static_cast<uint32_t>(round_trip.size()));
    return converted == input.size() &&
           std::memcmp(output.data(), input.data(), input.size()) == 0 &&
           restored == gbk.size() &&
           std::memcmp(round_trip.data(), gbk.data(), gbk.size()) == 0;
#endif
}

} // namespace

int main() {
    if (datayes::mdl::mock::SupportedMessages().empty()) {
        std::cerr << "FAIL: mock core was not linked\n";
        return 1;
    }
    if (!CheckInterlockedHelpers()) {
        std::cerr << "FAIL: SDK refcount helper behavior changed\n";
        return 1;
    }
    if (!CheckEncodingHelper()) {
#if defined(L2MOCK_EXPECT_VENDOR_CONVERSION)
        std::cerr << "FAIL: mock fallback preempted the vendor converter\n";
#else
        std::cerr << "FAIL: standalone SDK fallback was not used\n";
#endif
        return 1;
    }
    return 0;
}
