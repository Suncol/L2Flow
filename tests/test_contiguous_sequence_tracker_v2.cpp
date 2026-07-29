#include "l2flow/realtime/contiguous_sequence_tracker_v2.h"

#include <atomic>
#include <cstdint>
#include <iostream>
#include <memory>
#include <thread>
#include <vector>

namespace {

using l2flow::realtime::ContiguousSequenceMarkErrorV2;
using l2flow::realtime::ContiguousSequenceTrackerCreateErrorV2;
using l2flow::realtime::ContiguousSequenceTrackerV2;

bool Expect(bool condition, const char* message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << '\n';
    }
    return condition;
}

bool TestOutOfOrderPrefix() {
    std::unique_ptr<ContiguousSequenceTrackerV2> tracker;
    bool ok = Expect(
        ContiguousSequenceTrackerV2::Create(8U, &tracker) ==
                ContiguousSequenceTrackerCreateErrorV2::kNone &&
            tracker != nullptr,
        "create tracker");
    if (!ok) {
        return false;
    }
    ok &= Expect(
        tracker->MarkCompleted(3U) ==
                ContiguousSequenceMarkErrorV2::kNone &&
            tracker->contiguous_sequence() == 0U,
        "future completion does not skip a gap");
    ok &= Expect(
        tracker->MarkCompleted(1U) ==
                ContiguousSequenceMarkErrorV2::kNone &&
            tracker->contiguous_sequence() == 1U,
        "first completion advances one");
    ok &= Expect(
        tracker->MarkCompleted(2U) ==
                ContiguousSequenceMarkErrorV2::kNone &&
            tracker->contiguous_sequence() == 3U,
        "closing a gap consumes the ready suffix");
    const auto snapshot = tracker->Snapshot();
    ok &= Expect(
        snapshot.highest_observed_sequence == 3U &&
            snapshot.pending_sequences == 0U &&
            snapshot.failed_marks == 0U,
        "snapshot reconciles completed prefix");
    return ok;
}

bool TestFailures() {
    std::unique_ptr<ContiguousSequenceTrackerV2> tracker;
    bool ok = Expect(
        ContiguousSequenceTrackerV2::Create(2U, &tracker) ==
                ContiguousSequenceTrackerCreateErrorV2::kNone,
        "create small tracker");
    if (!ok) {
        return false;
    }
    ok &= Expect(
        tracker->MarkCompleted(3U) ==
            ContiguousSequenceMarkErrorV2::kReorderWindowExceeded,
        "completion outside bounded reorder window fails");
    ok &= Expect(
        tracker->MarkCompleted(1U) ==
                ContiguousSequenceMarkErrorV2::kNone &&
            tracker->MarkCompleted(1U) ==
                ContiguousSequenceMarkErrorV2::kDuplicateSequence,
        "duplicate completion fails");
    ok &= Expect(
        tracker->MarkCompleted(0U) ==
            ContiguousSequenceMarkErrorV2::kInvalidSequence,
        "zero completion fails");
    ok &= Expect(
        tracker->Snapshot().failed_marks == 3U,
        "failed marks are counted");
    return ok;
}

bool TestConcurrentWorkers() {
    constexpr std::uint64_t count = 20'000U;
    std::unique_ptr<ContiguousSequenceTrackerV2> tracker;
    bool ok = Expect(
        ContiguousSequenceTrackerV2::Create(
            static_cast<std::size_t>(count), &tracker) ==
                ContiguousSequenceTrackerCreateErrorV2::kNone,
        "create concurrent tracker");
    if (!ok) {
        return false;
    }

    constexpr std::size_t workers = 4U;
    std::atomic<bool> failed{false};
    std::vector<std::thread> threads;
    threads.reserve(workers);
    for (std::size_t worker = 0U; worker < workers; ++worker) {
        threads.emplace_back([&, worker] {
            for (std::uint64_t sequence =
                     static_cast<std::uint64_t>(worker) + 1U;
                 sequence <= count;
                 sequence += workers) {
                if (tracker->MarkCompleted(sequence) !=
                    ContiguousSequenceMarkErrorV2::kNone) {
                    failed.store(true, std::memory_order_relaxed);
                    return;
                }
            }
        });
    }
    for (std::thread& thread : threads) {
        thread.join();
    }
    const auto snapshot = tracker->Snapshot();
    ok &= Expect(
        !failed.load(std::memory_order_relaxed) &&
            snapshot.contiguous_sequence == count &&
            snapshot.highest_observed_sequence == count &&
            snapshot.pending_sequences == 0U &&
            snapshot.failed_marks == 0U,
        "concurrent out-of-order completions form one exact prefix");
    return ok;
}

}  // namespace

int main() {
    bool ok = true;
    ok &= TestOutOfOrderPrefix();
    ok &= TestFailures();
    ok &= TestConcurrentWorkers();
    if (!ok) {
        return 1;
    }
    std::cout << "contiguous sequence tracker V2 tests passed\n";
    return 0;
}
