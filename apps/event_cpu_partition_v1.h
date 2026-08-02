#pragma once

#include "l2flow/common/linux_thread_affinity_v1.h"

#include <string_view>

namespace l2flow::apps {

enum class EventCpuPartitionErrorV1 : unsigned char {
    kNone = 0U,
    kNullOutput,
    kEmptyOriginalSet,
    kEmptyEventSet,
    kEventOutsideOriginalSet,
    kEmptyFastSet,
};

[[nodiscard]] std::string_view EventCpuPartitionErrorNameV1(
    EventCpuPartitionErrorV1 error) noexcept;

// Splits the startup affinity into two disjoint logical-CPU masks. Event must
// be a strict subset of original; FAST receives the exact complement. This is
// a topology-neutral set operation: operators remain responsible for choosing
// appropriate physical cores, SMT siblings, NUMA nodes, and IRQ placement.
[[nodiscard]] EventCpuPartitionErrorV1 BuildEventCpuPartitionV1(
    const l2flow::common::LinuxCpuSetV1& original,
    const l2flow::common::LinuxCpuSetV1& event,
    l2flow::common::LinuxCpuSetV1* fast) noexcept;

}  // namespace l2flow::apps
