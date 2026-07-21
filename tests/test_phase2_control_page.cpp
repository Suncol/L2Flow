#include "l2flow/ingress/raw_control_page.h"

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <new>
#include <string>
#include <thread>

namespace ingress = l2flow::ingress;

namespace {

struct TestContext final {
    void Expect(bool condition, const std::string& description) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << description << '\n';
        }
    }

    int failures = 0;
};

ingress::RawControlSnapshot MakeSnapshot(
    std::uint64_t value) {
    ingress::RawControlSnapshot result;
    result.writer_instance[0] =
        static_cast<std::byte>(value & 0xffU);
    result.stream_day_id[0] =
        static_cast<std::byte>((value + 1U) & 0xffU);
    result.source_stream_id = 1001U;
    result.capture_date = 20260718U;
    result.segment_sequence =
        static_cast<std::uint32_t>(value);
    result.fatal_state =
        static_cast<std::uint32_t>(value & 1U);
    result.append_global_wal_pos = value * 10U;
    result.append_ingress_sequence = value;
    result.append_segment_offset = value * 2U;
    result.durable_global_wal_pos = value * 10U;
    result.durable_ingress_sequence = value;
    result.durable_segment_offset = value * 2U;
    result.clock_epoch_label = 77U;
    result.heartbeat_monotonic_ns = value * 100U;
    return result;
}

}  // namespace

int main() {
    TestContext test;

    alignas(ingress::kRawControlPageBytes)
        std::array<std::byte, ingress::kRawControlPageBytes>
            storage{};
    ingress::RawControlPageV1* page = nullptr;
    test.Expect(
        ingress::InitializeRawControlPage(
            storage.data(), storage.size(), &page),
        "page initializes in aligned fixed storage");
    test.Expect(page != nullptr, "initializer returns the page");

    ingress::RawControlPageWriter writer(*page);
    const ingress::RawControlSnapshot first =
        MakeSnapshot(1U);
    test.Expect(
        writer.Publish(first),
        "writer publishes the first transaction");
    ingress::RawControlSnapshot observed;
    std::uint64_t generation = 0U;
    test.Expect(
        ingress::ReadRawControlPage(
            *page, &observed, &generation),
        "reader obtains a stable snapshot");
    test.Expect(
        generation == 2U &&
            observed.append_global_wal_pos ==
                first.append_global_wal_pos &&
            observed.writer_instance ==
                first.writer_instance,
        "snapshot fields share one even generation");

    std::atomic<bool> start{false};
    std::atomic<bool> done{false};
    std::atomic<bool> mismatch{false};
    std::thread reader([&]() {
        while (!start.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        while (!done.load(std::memory_order_acquire)) {
            ingress::RawControlSnapshot sample;
            if (ingress::ReadRawControlPage(
                    *page, &sample, nullptr, 10U) &&
                (sample.append_global_wal_pos !=
                     sample.append_ingress_sequence * 10U ||
                 sample.durable_global_wal_pos !=
                     sample.durable_ingress_sequence * 10U)) {
                mismatch.store(true, std::memory_order_release);
                return;
            }
        }
    });
    start.store(true, std::memory_order_release);
    for (std::uint64_t value = 2U;
         value < 20'000U;
         ++value) {
        if (!writer.Publish(MakeSnapshot(value))) {
            mismatch.store(true, std::memory_order_release);
            break;
        }
    }
    done.store(true, std::memory_order_release);
    reader.join();
    test.Expect(
        !mismatch.load(std::memory_order_acquire),
        "concurrent reader never observes a torn transaction");

    page->generation.store(
        std::numeric_limits<std::uint64_t>::max() - 1U,
        std::memory_order_release);
    test.Expect(
        !writer.Publish(MakeSnapshot(20'001U)),
        "writer fails before generation wrap");

    if (test.failures != 0) {
        std::cerr << test.failures
                  << " Phase 2 control-page tests failed\n";
        return 1;
    }
    std::cout << "Phase 2 control-page tests passed\n";
    return 0;
}
