#include "l2flow/common/linux_thread_affinity_v1.h"

#include <algorithm>
#include <bit>
#include <cerrno>
#include <cctype>
#include <limits>

#include <sched.h>

namespace l2flow::common {
namespace {

static_assert(CPU_SETSIZE >= kLinuxCpuSetMaximumCpuCountV1);

void SetSystemError(int* output, int value) noexcept {
    if (output != nullptr) {
        *output = value;
    }
}

[[nodiscard]] bool DecimalDigit(char value) noexcept {
    return value >= '0' && value <= '9';
}

[[nodiscard]] LinuxCpuSetParseErrorV1 ParseCpu(
    std::string_view text,
    std::size_t* position,
    std::size_t* output) noexcept {
    if (position == nullptr || output == nullptr ||
        *position >= text.size() || !DecimalDigit(text[*position])) {
        return LinuxCpuSetParseErrorV1::kInvalidSyntax;
    }
    std::size_t value = 0U;
    while (*position < text.size() && DecimalDigit(text[*position])) {
        const std::size_t digit = static_cast<std::size_t>(
            text[*position] - '0');
        if (value >
            (kLinuxCpuSetMaximumCpuCountV1 - 1U - digit) / 10U) {
            return LinuxCpuSetParseErrorV1::kCpuOutOfRange;
        }
        value = value * 10U + digit;
        ++(*position);
    }
    if (value >= kLinuxCpuSetMaximumCpuCountV1) {
        return LinuxCpuSetParseErrorV1::kCpuOutOfRange;
    }
    *output = value;
    return LinuxCpuSetParseErrorV1::kNone;
}

[[nodiscard]] cpu_set_t NativeMask(
    const LinuxCpuSetV1& source) noexcept {
    cpu_set_t result{};
    CPU_ZERO(&result);
    for (std::size_t cpu = 0U;
         cpu < kLinuxCpuSetMaximumCpuCountV1;
         ++cpu) {
        if (source.contains(cpu)) {
            CPU_SET(static_cast<int>(cpu), &result);
        }
    }
    return result;
}

[[nodiscard]] LinuxCpuSetV1 PortableMask(
    const cpu_set_t& source) noexcept {
    LinuxCpuSetV1 result{};
    for (std::size_t cpu = 0U;
         cpu < kLinuxCpuSetMaximumCpuCountV1;
         ++cpu) {
        if (CPU_ISSET(static_cast<int>(cpu), &source) != 0) {
            static_cast<void>(result.Add(cpu));
        }
    }
    return result;
}

}  // namespace

bool LinuxCpuSetV1::empty() const noexcept {
    return std::all_of(
        words_.begin(), words_.end(), [](std::uint64_t word) {
            return word == 0U;
        });
}

std::size_t LinuxCpuSetV1::count() const noexcept {
    std::size_t result = 0U;
    for (const std::uint64_t word : words_) {
        result += static_cast<std::size_t>(std::popcount(word));
    }
    return result;
}

bool LinuxCpuSetV1::contains(std::size_t cpu) const noexcept {
    if (cpu >= kLinuxCpuSetMaximumCpuCountV1) {
        return false;
    }
    const std::size_t word = cpu / 64U;
    const unsigned int bit = static_cast<unsigned int>(cpu % 64U);
    return (words_[word] & (std::uint64_t{1U} << bit)) != 0U;
}

bool LinuxCpuSetV1::Add(std::size_t cpu) noexcept {
    if (cpu >= kLinuxCpuSetMaximumCpuCountV1) {
        return false;
    }
    const std::size_t word = cpu / 64U;
    const unsigned int bit = static_cast<unsigned int>(cpu % 64U);
    words_[word] |= std::uint64_t{1U} << bit;
    return true;
}

void LinuxCpuSetV1::Clear() noexcept {
    words_.fill(0U);
}

std::string_view LinuxCpuSetParseErrorNameV1(
    LinuxCpuSetParseErrorV1 error) noexcept {
    switch (error) {
        case LinuxCpuSetParseErrorV1::kNone:
            return "none";
        case LinuxCpuSetParseErrorV1::kNullOutput:
            return "null_output";
        case LinuxCpuSetParseErrorV1::kEmpty:
            return "empty";
        case LinuxCpuSetParseErrorV1::kInvalidSyntax:
            return "invalid_syntax";
        case LinuxCpuSetParseErrorV1::kCpuOutOfRange:
            return "cpu_out_of_range";
        case LinuxCpuSetParseErrorV1::kDescendingRange:
            return "descending_range";
        case LinuxCpuSetParseErrorV1::kDuplicateCpu:
            return "duplicate_cpu";
    }
    return "unknown";
}

LinuxCpuSetParseErrorV1 ParseLinuxCpuSetV1(
    std::string_view text,
    LinuxCpuSetV1* output) noexcept {
    if (output == nullptr) {
        return LinuxCpuSetParseErrorV1::kNullOutput;
    }
    output->Clear();
    if (text.empty()) {
        return LinuxCpuSetParseErrorV1::kEmpty;
    }

    LinuxCpuSetV1 parsed{};
    std::size_t position = 0U;
    while (position < text.size()) {
        std::size_t first = 0U;
        const auto first_error = ParseCpu(text, &position, &first);
        if (first_error != LinuxCpuSetParseErrorV1::kNone) {
            return first_error;
        }
        std::size_t last = first;
        if (position < text.size() && text[position] == '-') {
            ++position;
            const auto last_error = ParseCpu(text, &position, &last);
            if (last_error != LinuxCpuSetParseErrorV1::kNone) {
                return last_error;
            }
            if (last <= first) {
                return LinuxCpuSetParseErrorV1::kDescendingRange;
            }
        }

        for (std::size_t cpu = first;; ++cpu) {
            if (parsed.contains(cpu)) {
                return LinuxCpuSetParseErrorV1::kDuplicateCpu;
            }
            static_cast<void>(parsed.Add(cpu));
            if (cpu == last) {
                break;
            }
        }
        if (position == text.size()) {
            break;
        }
        if (text[position] != ',') {
            return LinuxCpuSetParseErrorV1::kInvalidSyntax;
        }
        ++position;
        if (position == text.size()) {
            return LinuxCpuSetParseErrorV1::kInvalidSyntax;
        }
    }
    *output = parsed;
    return LinuxCpuSetParseErrorV1::kNone;
}

bool LinuxCpuSetIsSubsetV1(
    const LinuxCpuSetV1& subset,
    const LinuxCpuSetV1& superset) noexcept {
    for (std::size_t cpu = 0U;
         cpu < kLinuxCpuSetMaximumCpuCountV1;
         ++cpu) {
        if (subset.contains(cpu) && !superset.contains(cpu)) {
            return false;
        }
    }
    return true;
}

std::string_view LinuxThreadAffinityErrorNameV1(
    LinuxThreadAffinityErrorV1 error) noexcept {
    switch (error) {
        case LinuxThreadAffinityErrorV1::kNone:
            return "none";
        case LinuxThreadAffinityErrorV1::kNullOutput:
            return "null_output";
        case LinuxThreadAffinityErrorV1::kEmptyCpuSet:
            return "empty_cpu_set";
        case LinuxThreadAffinityErrorV1::kSetFailed:
            return "set_failed";
        case LinuxThreadAffinityErrorV1::kGetFailed:
            return "get_failed";
        case LinuxThreadAffinityErrorV1::kReadBackMismatch:
            return "read_back_mismatch";
    }
    return "unknown";
}

LinuxThreadAffinityErrorV1 ReadLinuxThreadAffinityV1(
    pthread_t thread,
    LinuxCpuSetV1* output,
    int* system_error_number) noexcept {
    SetSystemError(system_error_number, 0);
    if (output == nullptr) {
        return LinuxThreadAffinityErrorV1::kNullOutput;
    }
    output->Clear();
    cpu_set_t native{};
    CPU_ZERO(&native);
    const int result = ::pthread_getaffinity_np(
        thread, sizeof(native), &native);
    if (result != 0) {
        SetSystemError(system_error_number, result);
        return LinuxThreadAffinityErrorV1::kGetFailed;
    }
    *output = PortableMask(native);
    return LinuxThreadAffinityErrorV1::kNone;
}

LinuxThreadAffinityErrorV1 ReadCurrentLinuxThreadAffinityV1(
    LinuxCpuSetV1* output,
    int* system_error_number) noexcept {
    return ReadLinuxThreadAffinityV1(
        ::pthread_self(), output, system_error_number);
}

LinuxThreadAffinityErrorV1 ApplyCurrentLinuxThreadAffinityExactV1(
    const LinuxCpuSetV1& requested,
    LinuxCpuSetV1* observed,
    int* system_error_number) noexcept {
    SetSystemError(system_error_number, 0);
    if (observed != nullptr) {
        observed->Clear();
    }
    if (requested.empty()) {
        SetSystemError(system_error_number, EINVAL);
        return LinuxThreadAffinityErrorV1::kEmptyCpuSet;
    }
    const cpu_set_t native = NativeMask(requested);
    const int set_result = ::pthread_setaffinity_np(
        ::pthread_self(), sizeof(native), &native);
    if (set_result != 0) {
        SetSystemError(system_error_number, set_result);
        return LinuxThreadAffinityErrorV1::kSetFailed;
    }
    LinuxCpuSetV1 actual{};
    int get_error = 0;
    const auto get_result = ReadCurrentLinuxThreadAffinityV1(
        &actual, &get_error);
    if (get_result != LinuxThreadAffinityErrorV1::kNone) {
        SetSystemError(system_error_number, get_error);
        return get_result;
    }
    if (observed != nullptr) {
        *observed = actual;
    }
    if (!(actual == requested)) {
        SetSystemError(system_error_number, EINVAL);
        return LinuxThreadAffinityErrorV1::kReadBackMismatch;
    }
    return LinuxThreadAffinityErrorV1::kNone;
}

}  // namespace l2flow::common
