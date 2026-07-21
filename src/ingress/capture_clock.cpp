#include "l2flow/ingress/capture_clock.h"

#include <cerrno>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <system_error>

#include <time.h>

namespace l2flow::ingress {
namespace {

std::uint64_t ReadClock(clockid_t id) {
    struct timespec value {};
    if (::clock_gettime(id, &value) != 0) {
        throw std::system_error(errno, std::generic_category(),
                                "clock_gettime");
    }
    if (value.tv_sec < 0 || value.tv_nsec < 0 ||
        value.tv_nsec >= 1'000'000'000L) {
        throw std::runtime_error("clock_gettime returned an invalid timespec");
    }
    constexpr std::uint64_t billion = 1'000'000'000ULL;
    const auto seconds = static_cast<std::uint64_t>(value.tv_sec);
    const auto nanoseconds =
        static_cast<std::uint64_t>(value.tv_nsec);
    if (seconds >
        (std::numeric_limits<std::uint64_t>::max() - nanoseconds) /
            billion) {
        throw std::overflow_error("clock timestamp exceeds uint64 nanoseconds");
    }
    return seconds * billion + nanoseconds;
}

}  // namespace

std::uint64_t LinuxCaptureClock::MonotonicRawNanoseconds() {
    return ReadClock(CLOCK_MONOTONIC_RAW);
}

std::uint64_t LinuxCaptureClock::RealtimeNanoseconds() {
    return ReadClock(CLOCK_REALTIME);
}

}  // namespace l2flow::ingress
