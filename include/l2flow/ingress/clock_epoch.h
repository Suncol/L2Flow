#pragma once

#include "l2flow/common/sha256.h"

#include <cstdint>
#include <string>

namespace l2flow::ingress {

struct ClockEpochInputs {
    std::string host_uuid;
    std::string linux_boot_id;
    std::string clock_source_config;
};

struct ClockEpoch {
    std::uint64_t value = 0U;
    l2flow::common::Sha256Digest digest{};
};

// SHA-256 over a versioned, length-prefixed byte representation. `value` is
// the first eight digest bytes interpreted big-endian; the full digest remains
// available for collision-resistant manifests.
ClockEpoch ComputeClockEpoch(const ClockEpochInputs& inputs);

bool ReadClockEpochInputs(const std::string& host_uuid_path,
                          const std::string& linux_boot_id_path,
                          std::string clock_source_config,
                          ClockEpochInputs* inputs,
                          std::string* error) noexcept;

}  // namespace l2flow::ingress
