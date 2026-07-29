#include "l2flow/runtime/detail/closeable_publication_gate.h"

#include <atomic>
#include <chrono>
#include <iostream>
#include <string_view>
#include <thread>

namespace {

using l2flow::runtime::detail::CloseablePublicationGate;
using namespace std::chrono_literals;

class TestContext final {
public:
    void Expect(bool condition, std::string_view description) {
        if (!condition) {
            ++failures_;
            std::cerr << "FAIL: " << description << '\n';
        }
    }

    [[nodiscard]] int failures() const noexcept { return failures_; }

private:
    int failures_ = 0;
};

void WaitUntilTrue(const std::atomic<bool>& value) {
    bool observed = value.load(std::memory_order_acquire);
    while (!observed) {
        value.wait(observed, std::memory_order_acquire);
        observed = value.load(std::memory_order_acquire);
    }
}

void TestCloseWaitsForPublication(TestContext* test) {
    CloseablePublicationGate gate;
    std::atomic<bool> producer_entered{false};
    std::atomic<bool> allow_publication{false};
    std::atomic<bool> published{false};
    std::atomic<bool> close_returned{false};

    std::thread producer([&] {
        CloseablePublicationGate::Lease lease = gate.TryAcquire();
        if (!lease) {
            return;
        }
        producer_entered.store(true, std::memory_order_release);
        producer_entered.notify_all();
        WaitUntilTrue(allow_publication);

        // This store stands in for writing the slot and release-publishing
        // the queue tail. Lease destruction follows that publication.
        published.store(true, std::memory_order_release);
    });
    WaitUntilTrue(producer_entered);

    std::thread closer([&] {
        gate.CloseAndWait();
        close_returned.store(true, std::memory_order_release);
        close_returned.notify_all();
    });
    while (!gate.close_requested()) {
        std::this_thread::yield();
    }

    test->Expect(
        !close_returned.load(std::memory_order_acquire),
        "close cannot return while a pre-close producer has not published");
    test->Expect(
        !gate.TryAcquire(),
        "a producer arriving after close is rejected without waiting");

    allow_publication.store(true, std::memory_order_release);
    allow_publication.notify_all();
    producer.join();
    closer.join();

    test->Expect(
        close_returned.load(std::memory_order_acquire),
        "close returns after the admitted producer releases its lease");
    test->Expect(
        published.load(std::memory_order_acquire),
        "the admitted publication is visible when close returns");
    test->Expect(
        gate.closed_and_quiesced(),
        "closed-and-empty consumers may exit only after all publishers quiesce");
}

void TestEmptyCloseIsImmediate(TestContext* test) {
    CloseablePublicationGate gate;
    const auto begin = std::chrono::steady_clock::now();
    gate.CloseAndWait();
    const auto elapsed = std::chrono::steady_clock::now() - begin;

    test->Expect(
        gate.closed_and_quiesced(),
        "closing a gate with no active producer reaches quiescence");
    test->Expect(
        elapsed < 100ms,
        "closing a gate with no active producer does not block");
    test->Expect(
        !gate.TryAcquire(),
        "a closed gate never reopens");
}

}  // namespace

int main() {
    TestContext test;
    TestCloseWaitsForPublication(&test);
    TestEmptyCloseIsImmediate(&test);

    if (test.failures() != 0) {
        std::cerr << test.failures()
                  << " closeable publication gate assertion(s) failed\n";
        return 1;
    }
    std::cout << "closeable publication gate tests passed\n";
    return 0;
}
