#include "l2flow/common/identity128.h"

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <limits>

#include <sys/random.h>

namespace l2flow::common {
namespace {

class LinuxIdentityEntropySource final : public IdentityEntropySource {
public:
    EntropyReadResult ReadSome(
        std::span<std::byte> output) noexcept override {
        if (output.empty()) {
            return {};
        }
        const std::size_t requested = std::min(
            output.size(),
            static_cast<std::size_t>(
                std::numeric_limits<ssize_t>::max()));
        const ssize_t result =
            ::getrandom(output.data(), requested, 0U);
        if (result < 0) {
            return {0U, errno};
        }
        return {
            static_cast<std::size_t>(result),
            0};
    }
};

int HexNibble(char value) noexcept {
    if (value >= '0' && value <= '9') {
        return value - '0';
    }
    if (value >= 'a' && value <= 'f') {
        return value - 'a' + 10;
    }
    return -1;
}

void SetError(int* error_number, int value) noexcept {
    if (error_number != nullptr) {
        *error_number = value;
    }
}

}  // namespace

bool GenerateIdentity128(
    IdentityEntropySource& source,
    Identity128* output,
    int* error_number) noexcept {
    if (output == nullptr) {
        SetError(error_number, EINVAL);
        return false;
    }

    Identity128 candidate{};
    std::size_t offset = 0U;
    while (offset < candidate.size()) {
        const EntropyReadResult result =
            source.ReadSome(
                std::span<std::byte>(candidate).subspan(offset));
        if (result.error_number != 0) {
            if (result.error_number == EINTR &&
                result.bytes_read == 0U) {
                continue;
            }
            SetError(error_number, result.error_number);
            return false;
        }
        if (result.bytes_read == 0U ||
            result.bytes_read > candidate.size() - offset) {
            SetError(error_number, EIO);
            return false;
        }
        offset += result.bytes_read;
    }
    if (IsZeroIdentity(candidate)) {
        SetError(error_number, EIO);
        return false;
    }

    *output = candidate;
    SetError(error_number, 0);
    return true;
}

bool GenerateIdentity128(
    Identity128* output,
    int* error_number) noexcept {
    LinuxIdentityEntropySource source;
    return GenerateIdentity128(source, output, error_number);
}

bool IsZeroIdentity(
    const Identity128& identity) noexcept {
    return std::all_of(
        identity.begin(),
        identity.end(),
        [](std::byte value) {
            return value == std::byte{0};
        });
}

std::string Identity128Hex(
    const Identity128& identity) {
    static constexpr char digits[] = "0123456789abcdef";
    std::string result;
    result.resize(identity.size() * 2U);
    for (std::size_t index = 0U;
         index < identity.size();
         ++index) {
        const std::uint8_t value =
            std::to_integer<std::uint8_t>(identity[index]);
        result[index * 2U] =
            digits[(value >> 4U) & 0x0fU];
        result[index * 2U + 1U] =
            digits[value & 0x0fU];
    }
    return result;
}

bool ParseIdentity128Hex(
    std::string_view text,
    Identity128* identity) noexcept {
    if (identity == nullptr ||
        text.size() != Identity128{}.size() * 2U) {
        return false;
    }
    Identity128 parsed{};
    for (std::size_t index = 0U;
         index < parsed.size();
         ++index) {
        const int high = HexNibble(text[index * 2U]);
        const int low = HexNibble(text[index * 2U + 1U]);
        if (high < 0 || low < 0) {
            return false;
        }
        parsed[index] = static_cast<std::byte>(
            static_cast<unsigned int>(
                high * 16 + low));
    }
    if (IsZeroIdentity(parsed)) {
        return false;
    }
    *identity = parsed;
    return true;
}

bool ParseCanonicalUuid128(
    std::string_view text,
    Identity128* identity) noexcept {
    if (identity == nullptr ||
        text.size() != 36U ||
        text[8U] != '-' ||
        text[13U] != '-' ||
        text[18U] != '-' ||
        text[23U] != '-') {
        return false;
    }
    std::array<char, 32U> compact{};
    std::size_t output = 0U;
    for (std::size_t index = 0U;
         index < text.size();
         ++index) {
        if (index == 8U ||
            index == 13U ||
            index == 18U ||
            index == 23U) {
            continue;
        }
        if (output >= compact.size()) {
            return false;
        }
        compact[output++] = text[index];
    }
    return output == compact.size() &&
           ParseIdentity128Hex(
               std::string_view(
                   compact.data(), compact.size()),
               identity);
}

}  // namespace l2flow::common
