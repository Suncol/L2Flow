#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <string_view>

#include <pthread.h>

namespace l2flow::common {

// Linux cpu_set_t is currently fixed at 1024 CPUs on the supported glibc
// baseline.  Keep the public value type independent of the libc layout so it
// can be parsed, compared, and carried in application configuration without
// exposing CPU_* macros.
inline constexpr std::size_t kLinuxCpuSetMaximumCpuCountV1 = 1024U;
inline constexpr std::size_t kLinuxCpuSetWordCountV1 =
    kLinuxCpuSetMaximumCpuCountV1 / 64U;

class LinuxCpuSetV1 final {
public:
    [[nodiscard]] bool empty() const noexcept;
    [[nodiscard]] std::size_t count() const noexcept;
    [[nodiscard]] bool contains(std::size_t cpu) const noexcept;

    // Returns false only when cpu is outside the supported fixed mask.
    [[nodiscard]] bool Add(std::size_t cpu) noexcept;
    void Clear() noexcept;

    [[nodiscard]] friend bool operator==(
        const LinuxCpuSetV1&,
        const LinuxCpuSetV1&) noexcept = default;

private:
    std::array<std::uint64_t, kLinuxCpuSetWordCountV1> words_{};
};

enum class LinuxCpuSetParseErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kEmpty,
    kInvalidSyntax,
    kCpuOutOfRange,
    kDescendingRange,
    kDuplicateCpu,
};

[[nodiscard]] std::string_view LinuxCpuSetParseErrorNameV1(
    LinuxCpuSetParseErrorV1 error) noexcept;

// Strict grammar: LIST := ITEM (',' ITEM)*; ITEM := CPU | CPU '-' CPU.
// CPU is an unsigned base-10 integer in [0,1023]. Whitespace, empty items,
// descending/singleton ranges, duplicate CPUs, and overlapping ranges are
// rejected. output is cleared before parsing and remains empty on failure.
[[nodiscard]] LinuxCpuSetParseErrorV1 ParseLinuxCpuSetV1(
    std::string_view text,
    LinuxCpuSetV1* output) noexcept;

[[nodiscard]] bool LinuxCpuSetIsSubsetV1(
    const LinuxCpuSetV1& subset,
    const LinuxCpuSetV1& superset) noexcept;

enum class LinuxThreadAffinityErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kEmptyCpuSet,
    kSetFailed,
    kGetFailed,
    kReadBackMismatch,
};

[[nodiscard]] std::string_view LinuxThreadAffinityErrorNameV1(
    LinuxThreadAffinityErrorV1 error) noexcept;

// pthread affinity functions return an errno value directly. These helpers
// copy it to system_error_number (zero on entry/success). Apply performs a
// mandatory get-affinity readback and accepts success only for an exact mask;
// this catches Linux's permitted-cpuset intersection behavior.
[[nodiscard]] LinuxThreadAffinityErrorV1 ReadLinuxThreadAffinityV1(
    pthread_t thread,
    LinuxCpuSetV1* output,
    int* system_error_number = nullptr) noexcept;

[[nodiscard]] LinuxThreadAffinityErrorV1
ReadCurrentLinuxThreadAffinityV1(
    LinuxCpuSetV1* output,
    int* system_error_number = nullptr) noexcept;

[[nodiscard]] LinuxThreadAffinityErrorV1
ApplyCurrentLinuxThreadAffinityExactV1(
    const LinuxCpuSetV1& requested,
    LinuxCpuSetV1* observed = nullptr,
    int* system_error_number = nullptr) noexcept;

}  // namespace l2flow::common
