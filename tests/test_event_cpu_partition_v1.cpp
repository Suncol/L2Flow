#include "../apps/event_cpu_partition_v1.h"

#include <cstddef>
#include <iostream>

namespace {

class Test final {
public:
    void Expect(bool condition, const char* message) {
        if (!condition) {
            ++failures_;
            std::cerr << "FAIL: " << message << '\n';
        }
    }

    [[nodiscard]] int failures() const noexcept {
        return failures_;
    }

private:
    int failures_ = 0;
};

}  // namespace

int main() {
    namespace app = l2flow::apps;
    namespace common = l2flow::common;
    Test test;

    common::LinuxCpuSetV1 original{};
    static_cast<void>(original.Add(1U));
    static_cast<void>(original.Add(2U));
    static_cast<void>(original.Add(4U));
    common::LinuxCpuSetV1 event{};
    static_cast<void>(event.Add(2U));
    common::LinuxCpuSetV1 fast{};
    const auto ok = app::BuildEventCpuPartitionV1(
        original, event, &fast);
    test.Expect(
        ok == app::EventCpuPartitionErrorV1::kNone &&
            fast.count() == 2U && fast.contains(1U) &&
            fast.contains(4U) && !fast.contains(2U),
        "FAST is the exact nonempty complement of Event");
    for (std::size_t cpu = 0U;
         cpu < common::kLinuxCpuSetMaximumCpuCountV1;
         ++cpu) {
        test.Expect(
            !(fast.contains(cpu) && event.contains(cpu)),
            "FAST and Event are disjoint");
        test.Expect(
            original.contains(cpu) ==
                (fast.contains(cpu) || event.contains(cpu)),
            "FAST union Event conserves the startup CPU set");
    }

    common::LinuxCpuSetV1 outside{};
    static_cast<void>(outside.Add(3U));
    test.Expect(
        app::BuildEventCpuPartitionV1(
            original, outside, &fast) ==
                app::EventCpuPartitionErrorV1::
                    kEventOutsideOriginalSet &&
            fast.empty(),
        "Event CPUs outside startup affinity are rejected");
    test.Expect(
        app::BuildEventCpuPartitionV1(
            original, original, &fast) ==
                app::EventCpuPartitionErrorV1::kEmptyFastSet &&
            fast.empty(),
        "Event cannot consume the complete startup affinity");
    common::LinuxCpuSetV1 empty{};
    test.Expect(
        app::BuildEventCpuPartitionV1(
            empty, event, &fast) ==
                app::EventCpuPartitionErrorV1::kEmptyOriginalSet,
        "empty original affinity is rejected");
    test.Expect(
        app::BuildEventCpuPartitionV1(
            original, empty, &fast) ==
                app::EventCpuPartitionErrorV1::kEmptyEventSet,
        "empty Event affinity is rejected");
    test.Expect(
        app::BuildEventCpuPartitionV1(
            original, event, nullptr) ==
                app::EventCpuPartitionErrorV1::kNullOutput,
        "null FAST output is rejected");

    if (test.failures() != 0) {
        return 1;
    }
    std::cout << "PASS: event CPU partition v1\n";
    return 0;
}
