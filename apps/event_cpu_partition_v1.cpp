#include "event_cpu_partition_v1.h"

namespace l2flow::apps {

std::string_view EventCpuPartitionErrorNameV1(
    EventCpuPartitionErrorV1 error) noexcept {
    switch (error) {
        case EventCpuPartitionErrorV1::kNone:
            return "none";
        case EventCpuPartitionErrorV1::kNullOutput:
            return "null_output";
        case EventCpuPartitionErrorV1::kEmptyOriginalSet:
            return "empty_original_set";
        case EventCpuPartitionErrorV1::kEmptyEventSet:
            return "empty_event_set";
        case EventCpuPartitionErrorV1::kEventOutsideOriginalSet:
            return "event_outside_original_set";
        case EventCpuPartitionErrorV1::kEmptyFastSet:
            return "empty_fast_set";
    }
    return "unknown";
}

EventCpuPartitionErrorV1 BuildEventCpuPartitionV1(
    const l2flow::common::LinuxCpuSetV1& original,
    const l2flow::common::LinuxCpuSetV1& event,
    l2flow::common::LinuxCpuSetV1* fast) noexcept {
    if (fast == nullptr) {
        return EventCpuPartitionErrorV1::kNullOutput;
    }
    fast->Clear();
    if (original.empty()) {
        return EventCpuPartitionErrorV1::kEmptyOriginalSet;
    }
    if (event.empty()) {
        return EventCpuPartitionErrorV1::kEmptyEventSet;
    }
    if (!l2flow::common::LinuxCpuSetIsSubsetV1(event, original)) {
        return EventCpuPartitionErrorV1::kEventOutsideOriginalSet;
    }
    for (std::size_t cpu = 0U;
         cpu < l2flow::common::kLinuxCpuSetMaximumCpuCountV1;
         ++cpu) {
        if (original.contains(cpu) && !event.contains(cpu)) {
            static_cast<void>(fast->Add(cpu));
        }
    }
    if (fast->empty()) {
        return EventCpuPartitionErrorV1::kEmptyFastSet;
    }
    return EventCpuPartitionErrorV1::kNone;
}

}  // namespace l2flow::apps
