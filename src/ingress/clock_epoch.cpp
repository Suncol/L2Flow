#include "l2flow/ingress/clock_epoch.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <limits>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace l2flow::ingress {
namespace {

void SetError(std::string* error, std::string message) noexcept {
    if (error == nullptr) {
        return;
    }
    try {
        *error = std::move(message);
    } catch (...) {
    }
}

void SetErrorLiteral(std::string* error, const char* message) noexcept {
    if (error == nullptr) {
        return;
    }
    try {
        *error = message;
    } catch (...) {
    }
}

void AppendU32(std::vector<std::byte>& output, std::uint32_t value) {
    output.push_back(static_cast<std::byte>((value >> 24U) & 0xffU));
    output.push_back(static_cast<std::byte>((value >> 16U) & 0xffU));
    output.push_back(static_cast<std::byte>((value >> 8U) & 0xffU));
    output.push_back(static_cast<std::byte>(value & 0xffU));
}

void AppendField(std::vector<std::byte>& output, std::string_view value) {
    if (value.size() > std::numeric_limits<std::uint32_t>::max()) {
        throw std::length_error("clock epoch field exceeds uint32");
    }
    AppendU32(output, static_cast<std::uint32_t>(value.size()));
    const auto bytes =
        std::as_bytes(std::span<const char>(value.data(), value.size()));
    output.insert(output.end(), bytes.begin(), bytes.end());
}

bool ReadSmallTextFile(const std::string& path,
                       std::string* output,
                       std::string* error) {
    std::ifstream input(path, std::ios::binary);
    if (!input.is_open()) {
        *error = "cannot open clock identity file: " + path;
        return false;
    }
    constexpr std::size_t maximum_bytes = 4096U;
    std::array<char, maximum_bytes + 1U> buffer{};
    input.read(
        buffer.data(),
        static_cast<std::streamsize>(buffer.size()));
    const std::streamsize count = input.gcount();
    if (input.bad() || count < 0) {
        *error = "cannot read clock identity file: " + path;
        return false;
    }
    if (static_cast<std::size_t>(count) > maximum_bytes) {
        *error = "clock identity file exceeds 4096 bytes: " + path;
        return false;
    }
    std::string value(
        buffer.data(), static_cast<std::size_t>(count));
    while (!value.empty() &&
           (value.back() == '\n' || value.back() == '\r')) {
        value.pop_back();
    }
    if (value.empty() || value.find('\0') != std::string::npos) {
        *error = "clock identity file is empty or contains NUL: " + path;
        return false;
    }
    *output = std::move(value);
    return true;
}

}  // namespace

ClockEpoch ComputeClockEpoch(const ClockEpochInputs& inputs) {
    if (inputs.host_uuid.empty() || inputs.linux_boot_id.empty() ||
        inputs.clock_source_config.empty()) {
        throw std::invalid_argument("clock epoch inputs must be non-empty");
    }
    static constexpr std::string_view domain = "L2FLOW_CLOCK_EPOCH_V1";
    std::vector<std::byte> canonical;
    canonical.reserve(
        domain.size() + inputs.host_uuid.size() +
        inputs.linux_boot_id.size() + inputs.clock_source_config.size() + 16U);
    const auto domain_bytes =
        std::as_bytes(std::span<const char>(domain.data(), domain.size()));
    canonical.insert(
        canonical.end(), domain_bytes.begin(), domain_bytes.end());
    canonical.push_back(std::byte{0});
    AppendField(canonical, inputs.host_uuid);
    AppendField(canonical, inputs.linux_boot_id);
    AppendField(canonical, inputs.clock_source_config);

    ClockEpoch epoch;
    epoch.digest = l2flow::common::ComputeSha256(canonical);
    for (std::size_t index = 0U; index < 8U; ++index) {
        epoch.value =
            (epoch.value << 8U) |
            std::to_integer<std::uint64_t>(epoch.digest[index]);
    }
    return epoch;
}

bool ReadClockEpochInputs(const std::string& host_uuid_path,
                          const std::string& linux_boot_id_path,
                          std::string clock_source_config,
                          ClockEpochInputs* inputs,
                          std::string* error) noexcept {
    if (inputs == nullptr) {
        SetErrorLiteral(error, "clock epoch output pointer is null");
        return false;
    }
    try {
        std::string local_error;
        ClockEpochInputs value;
        if (!ReadSmallTextFile(
                host_uuid_path, &value.host_uuid, &local_error) ||
            !ReadSmallTextFile(
                linux_boot_id_path, &value.linux_boot_id, &local_error)) {
            SetError(error, std::move(local_error));
            return false;
        }
        if (clock_source_config.empty() ||
            clock_source_config.find('\0') != std::string::npos) {
            SetErrorLiteral(
                error, "clock source config is empty or contains NUL");
            return false;
        }
        value.clock_source_config = std::move(clock_source_config);
        *inputs = std::move(value);
        if (error != nullptr) {
            error->clear();
        }
        return true;
    } catch (const std::exception& exception) {
        try {
            SetError(error,
                     std::string("clock epoch input failure: ") +
                         exception.what());
        } catch (...) {
            SetErrorLiteral(error, "clock epoch input failure");
        }
        return false;
    } catch (...) {
        SetErrorLiteral(error, "unknown clock epoch input failure");
        return false;
    }
}

}  // namespace l2flow::ingress
