#pragma once

#include <cstdint>

namespace l2flow::ingress {

class CaptureClock {
public:
    virtual ~CaptureClock() = default;
    virtual std::uint64_t MonotonicRawNanoseconds() = 0;
    virtual std::uint64_t RealtimeNanoseconds() = 0;
};

class LinuxCaptureClock final : public CaptureClock {
public:
    std::uint64_t MonotonicRawNanoseconds() override;
    std::uint64_t RealtimeNanoseconds() override;
};

}  // namespace l2flow::ingress
