#include "l2flow/realtime/owned_ingress_message_v1.h"

#include "l2flow/common/identity128.h"
#include "l2flow/sdk/vendor_head_view.h"

#include "mdl_api.h"

#include <array>
#include <atomic>
#include <barrier>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <iostream>
#include <memory>
#include <mutex>
#include <span>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace {

namespace common = l2flow::common;
namespace mdl = datayes::mdl;
namespace realtime = l2flow::realtime;
namespace sdk = l2flow::sdk;

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

class FakeMessage final : public mdl::MDLMessage {
public:
    explicit FakeMessage(std::size_t body_bytes)
        : body_(body_bytes, std::byte{0x5aU}) {
        std::memset(&head_, 0, sizeof(head_));
        head_.HeadSize =
            static_cast<std::uint8_t>(sdk::kVendorHeadBytes);
        head_.MessageSize = static_cast<std::uint32_t>(
            sdk::kVendorHeadBytes + body_.size());
        head_.MessageEncoding =
            static_cast<std::uint8_t>(mdl::MDLEID_BINARY);
        head_.ServiceID = 4U;
        head_.ServiceVersion = 101U;
        head_.MessageID = 4U;
        head_.LocalTime.m_Value = 93'000'000U;
        head_.SequenceID = 1U;
    }

    void AddRef() override {}
    int ReleaseRef() override { return 1; }
    mdl::MDLMessageHead* GetHead() const override {
        return const_cast<mdl::MDLMessageHead*>(&head_);
    }
    char* GetBody() const override {
        return body_.empty()
                   ? nullptr
                   : reinterpret_cast<char*>(
                         const_cast<std::byte*>(body_.data()));
    }
    mdl::MDLMessage* _Copy() const override { return nullptr; }

private:
    mdl::MDLMessageHead head_{};
    std::vector<std::byte> body_;
};

[[nodiscard]] realtime::OwnedIngressMetadataV1 Metadata(
    std::uint64_t sequence) {
    realtime::OwnedIngressMetadataV1 metadata{};
    metadata.run_id[0U] = std::byte{0x31U};
    metadata.run_id[15U] = std::byte{0xa5U};
    metadata.global_ingress_sequence = sequence;
    metadata.source_sequence = sequence;
    metadata.recv_realtime_ns = 1'000U + sequence;
    metadata.recv_monotonic_ns = 2'000U + sequence;
    metadata.tick_stream_sequence = 0U;
    return metadata;
}

[[nodiscard]] bool Inspect(
    const FakeMessage& message,
    std::uint32_t maximum_message_bytes,
    realtime::OwnedIngressMessageInspectionV1* output) {
    return realtime::InspectOwnedIngressMessageV1(
               &message, maximum_message_bytes, output) ==
               realtime::OwnedIngressMessageErrorV1::kNone &&
           static_cast<bool>(*output);
}

[[nodiscard]] std::unique_ptr<realtime::OwnedIngressMessagePoolV1>
CreatePool(
    bool serialized_acquire,
    std::uint32_t maximum_message_bytes,
    std::size_t maximum_messages,
    std::uint32_t prewarm_message_bytes,
    std::size_t prewarm_messages) {
    realtime::OwnedIngressMessagePoolConfigV1 config{};
    config.maximum_message_bytes = maximum_message_bytes;
    config.maximum_inflight_messages = maximum_messages;
    config.prewarm_message_bytes = prewarm_message_bytes;
    config.prewarm_message_count = prewarm_messages;
    config.serialized_acquire = serialized_acquire;
    std::unique_ptr<realtime::OwnedIngressMessagePoolV1> pool;
    if (realtime::OwnedIngressMessagePoolV1::Create(
            config, &pool) !=
        realtime::OwnedIngressMessageErrorV1::kNone) {
        return nullptr;
    }
    return pool;
}

void TestDefaultConcurrentAcquirers(TestContext* test) {
    constexpr std::size_t kThreadCount = 4U;
    constexpr std::size_t kMessagesPerThread = 8U;
    constexpr std::size_t kMessageCount =
        kThreadCount * kMessagesPerThread;
    constexpr std::uint32_t kMaximumMessageBytes = 4096U;
    FakeMessage message(256U);
    realtime::OwnedIngressMessageInspectionV1 inspection{};
    test->Expect(
        Inspect(message, kMaximumMessageBytes, &inspection),
        "default pool test message inspects");
    auto pool = CreatePool(
        false,
        kMaximumMessageBytes,
        kMessageCount,
        512U,
        kMessageCount);
    test->Expect(pool != nullptr, "default multi-acquirer pool creates");
    if (pool == nullptr) {
        return;
    }

    std::array<std::vector<realtime::OwnedIngressMessageHandleV1>,
               kThreadCount>
        held;
    std::array<std::uint32_t, kThreadCount> failures{};
    std::barrier ready(static_cast<std::ptrdiff_t>(kThreadCount + 1U));
    std::atomic<bool> release{false};
    std::vector<std::thread> workers;
    workers.reserve(kThreadCount);
    for (std::size_t worker = 0U; worker < kThreadCount; ++worker) {
        workers.emplace_back([&, worker]() {
            held[worker].reserve(kMessagesPerThread);
            for (std::size_t index = 0U;
                 index < kMessagesPerThread;
                 ++index) {
                realtime::OwnedIngressMessageHandleV1 handle;
                const std::uint64_t sequence =
                    static_cast<std::uint64_t>(
                        worker * kMessagesPerThread + index + 1U);
                if (pool->Acquire(
                        inspection, Metadata(sequence), &handle) !=
                        realtime::OwnedIngressMessageErrorV1::kNone ||
                    !handle) {
                    ++failures[worker];
                } else {
                    held[worker].push_back(std::move(handle));
                }
            }
            ready.arrive_and_wait();
            while (!release.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            held[worker].clear();
        });
    }
    ready.arrive_and_wait();
    const realtime::OwnedIngressMessagePoolSnapshotV1 active =
        pool->Snapshot();
    test->Expect(
        !active.serialized_acquire &&
            active.active_messages == kMessageCount &&
            active.cached_blocks == 0U &&
            active.allocated_blocks == kMessageCount,
        "default mode retains exact concurrent-acquirer accounting");
    release.store(true, std::memory_order_release);
    for (std::thread& worker : workers) {
        worker.join();
    }
    bool all_succeeded = true;
    for (const std::uint32_t failure_count : failures) {
        all_succeeded = all_succeeded && failure_count == 0U;
    }
    const realtime::OwnedIngressMessagePoolSnapshotV1 idle =
        pool->Snapshot();
    test->Expect(
        all_succeeded && idle.active_messages == 0U &&
            idle.cached_blocks == kMessageCount &&
            idle.allocated_blocks == kMessageCount,
        "default mode still supports concurrent Acquire and Recycle");
}

void TestSerializedReuseAndExhaustion(TestContext* test) {
    constexpr std::size_t kMessageCount = 32U;
    constexpr std::size_t kRecyclerCount = 8U;
    constexpr std::uint32_t kMaximumMessageBytes = 4096U;
    FakeMessage message(256U);
    realtime::OwnedIngressMessageInspectionV1 inspection{};
    test->Expect(
        Inspect(message, kMaximumMessageBytes, &inspection),
        "serialized pool test message inspects");
    auto pool = CreatePool(
        true,
        kMaximumMessageBytes,
        kMessageCount,
        512U,
        kMessageCount);
    test->Expect(pool != nullptr, "serialized-acquire pool creates");
    if (pool == nullptr) {
        return;
    }
    const realtime::OwnedIngressMessagePoolSnapshotV1 initial =
        pool->Snapshot();
    test->Expect(
        initial.serialized_acquire && initial.active_messages == 0U &&
            initial.cached_blocks == kMessageCount &&
            initial.allocated_blocks == kMessageCount &&
            initial.allocated_bytes != 0U,
        "serialized prewarm is fully accounted before acquisition");

    std::vector<realtime::OwnedIngressMessageHandleV1> handles(
        kMessageCount);
    bool acquired_all = true;
    for (std::size_t index = 0U; index < kMessageCount; ++index) {
        acquired_all = acquired_all &&
            pool->Acquire(
                inspection,
                Metadata(static_cast<std::uint64_t>(index + 1U)),
                &handles[index]) ==
                realtime::OwnedIngressMessageErrorV1::kNone &&
            static_cast<bool>(handles[index]);
    }
    const realtime::OwnedIngressMessagePoolSnapshotV1 full =
        pool->Snapshot();
    test->Expect(
        acquired_all && full.active_messages == kMessageCount &&
            full.cached_blocks == 0U &&
            full.allocated_blocks == kMessageCount,
        "serialized private cache transfers every prewarmed block");

    realtime::OwnedIngressMessageHandleV1 exhausted;
    test->Expect(
        pool->Acquire(
            inspection, Metadata(10'000U), &exhausted) ==
                realtime::OwnedIngressMessageErrorV1::kPoolExhausted &&
            !exhausted,
        "serialized pool enforces exact inflight exhaustion");

    std::vector<std::thread> recyclers;
    recyclers.reserve(kRecyclerCount);
    for (std::size_t worker = 0U; worker < kRecyclerCount; ++worker) {
        recyclers.emplace_back([&, worker]() {
            for (std::size_t index = worker;
                 index < handles.size();
                 index += kRecyclerCount) {
                handles[index].reset();
            }
        });
    }
    for (std::thread& recycler : recyclers) {
        recycler.join();
    }
    const realtime::OwnedIngressMessagePoolSnapshotV1 returned =
        pool->Snapshot();
    test->Expect(
        returned.active_messages == 0U &&
            returned.cached_blocks == kMessageCount &&
            returned.allocated_blocks == kMessageCount &&
            returned.allocated_bytes == initial.allocated_bytes,
        "multi-thread recycle returns exact serialized cache accounting");

    for (std::size_t index = 0U; index < kMessageCount; ++index) {
        test->Expect(
            pool->Acquire(
                inspection,
                Metadata(static_cast<std::uint64_t>(20'000U + index)),
                &handles[index]) ==
                    realtime::OwnedIngressMessageErrorV1::kNone,
            "serialized callback reacquires a returned batch");
    }
    const realtime::OwnedIngressMessagePoolSnapshotV1 reused =
        pool->Snapshot();
    test->Expect(
        reused.allocated_blocks == initial.allocated_blocks &&
            reused.allocated_bytes == initial.allocated_bytes,
        "serialized batch refill performs no replacement allocation");
    handles.clear();
}

void TestSerializedSizeClassReplacement(TestContext* test) {
    constexpr std::uint32_t kMaximumMessageBytes = 4096U;
    FakeMessage large_message(2048U);
    realtime::OwnedIngressMessageInspectionV1 large_inspection{};
    test->Expect(
        Inspect(
            large_message,
            kMaximumMessageBytes,
            &large_inspection),
        "large replacement message inspects");
    auto pool = CreatePool(
        true,
        kMaximumMessageBytes,
        1U,
        static_cast<std::uint32_t>(sdk::kVendorHeadBytes),
        1U);
    test->Expect(pool != nullptr, "one-block serialized pool creates");
    if (pool == nullptr) {
        return;
    }
    const auto small = pool->Snapshot();
    realtime::OwnedIngressMessageHandleV1 handle;
    test->Expect(
        pool->Acquire(
            large_inspection, Metadata(1U), &handle) ==
                realtime::OwnedIngressMessageErrorV1::kNone &&
            static_cast<bool>(handle),
        "serialized pool evicts an idle undersized class");
    const auto large = pool->Snapshot();
    test->Expect(
        large.active_messages == 1U && large.allocated_blocks == 1U &&
            large.cached_blocks == 0U &&
            large.allocated_bytes > small.allocated_bytes,
        "size-class replacement preserves block bound and exact bytes");
    handle.reset();
}

void TestConcurrentSnapshotAccounting(TestContext* test) {
    constexpr std::uint32_t kMaximumMessageBytes = 4096U;
    constexpr std::size_t kMaximumMessages = 64U;
    constexpr std::uint64_t kTotalMessages = 5'000U;
    constexpr std::size_t kRecyclerCount = 4U;
    FakeMessage message(256U);
    realtime::OwnedIngressMessageInspectionV1 inspection{};
    test->Expect(
        Inspect(message, kMaximumMessageBytes, &inspection),
        "snapshot concurrency message inspects");
    auto pool = CreatePool(
        true,
        kMaximumMessageBytes,
        kMaximumMessages,
        512U,
        kMaximumMessages);
    test->Expect(pool != nullptr, "snapshot concurrency pool creates");
    if (pool == nullptr) {
        return;
    }

    std::mutex queue_mutex;
    std::condition_variable queue_ready;
    std::deque<realtime::OwnedIngressMessageHandleV1> queue;
    bool queue_done = false;
    std::atomic<bool> producer_done{false};
    std::atomic<std::uint32_t> failures{0U};
    std::vector<std::thread> recyclers;
    recyclers.reserve(kRecyclerCount);
    for (std::size_t worker = 0U; worker < kRecyclerCount; ++worker) {
        recyclers.emplace_back([&]() {
            for (;;) {
                realtime::OwnedIngressMessageHandleV1 handle;
                {
                    std::unique_lock<std::mutex> lock(queue_mutex);
                    queue_ready.wait(lock, [&]() {
                        return queue_done || !queue.empty();
                    });
                    if (queue.empty()) {
                        if (queue_done) {
                            return;
                        }
                        continue;
                    }
                    handle = std::move(queue.front());
                    queue.pop_front();
                }
                handle.reset();
            }
        });
    }

    std::thread producer([&]() {
        for (std::uint64_t sequence = 1U;
             sequence <= kTotalMessages;
             ++sequence) {
            realtime::OwnedIngressMessageHandleV1 handle;
            for (;;) {
                const realtime::OwnedIngressMessageErrorV1 error =
                    pool->Acquire(
                        inspection, Metadata(sequence), &handle);
                if (error ==
                    realtime::OwnedIngressMessageErrorV1::kNone) {
                    break;
                }
                if (error !=
                    realtime::OwnedIngressMessageErrorV1::kPoolExhausted) {
                    failures.fetch_add(1U, std::memory_order_relaxed);
                    break;
                }
                std::this_thread::yield();
            }
            if (handle) {
                {
                    std::lock_guard<std::mutex> lock(queue_mutex);
                    queue.push_back(std::move(handle));
                }
                queue_ready.notify_one();
            }
        }
        producer_done.store(true, std::memory_order_release);
    });

    std::size_t snapshots = 0U;
    while (!producer_done.load(std::memory_order_acquire)) {
        const auto snapshot = pool->Snapshot();
        if (!snapshot.serialized_acquire || snapshot.retired ||
            snapshot.active_messages > kMaximumMessages ||
            snapshot.allocated_blocks > kMaximumMessages ||
            snapshot.cached_blocks > snapshot.allocated_blocks ||
            snapshot.active_messages + snapshot.cached_blocks !=
                snapshot.allocated_blocks) {
            failures.fetch_add(1U, std::memory_order_relaxed);
        }
        ++snapshots;
        std::this_thread::yield();
    }
    producer.join();
    {
        std::lock_guard<std::mutex> lock(queue_mutex);
        queue_done = true;
    }
    queue_ready.notify_all();
    for (std::thread& recycler : recyclers) {
        recycler.join();
    }
    const auto idle = pool->Snapshot();
    test->Expect(
        snapshots != 0U &&
            failures.load(std::memory_order_relaxed) == 0U &&
            idle.active_messages == 0U &&
            idle.cached_blocks == idle.allocated_blocks &&
            idle.allocated_blocks <= kMaximumMessages,
        "concurrent snapshots preserve exact active/cache/block identity");
}

void TestSerializedImmediateReuseSnapshotRace(TestContext* test) {
    constexpr std::uint32_t kMaximumMessageBytes = 4096U;
    constexpr std::uint64_t kTotalMessages = 100'000U;
    constexpr std::uint8_t kEmpty = 0U;
    constexpr std::uint8_t kFull = 1U;
    constexpr std::uint8_t kDone = 2U;
    constexpr std::uint8_t kFailed = 3U;

    FakeMessage message(256U);
    realtime::OwnedIngressMessageInspectionV1 inspection{};
    test->Expect(
        Inspect(message, kMaximumMessageBytes, &inspection),
        "immediate-reuse snapshot-race message inspects");
    auto pool = CreatePool(
        true,
        kMaximumMessageBytes,
        1U,
        512U,
        1U);
    test->Expect(
        pool != nullptr,
        "one-block immediate-reuse snapshot-race pool creates");
    if (pool == nullptr) {
        return;
    }

    // The recycler advertises an empty handoff before dropping its final
    // handle. The sole acquirer therefore waits directly on the one-block pool
    // and takes a returned block as soon as Recycle makes it reusable. A
    // concurrent Snapshot repeatedly samples the narrow recycle/reacquire
    // transition that must never count two active messages for one block.
    realtime::OwnedIngressMessageHandleV1 handoff;
    std::atomic<std::uint8_t> handoff_state{kEmpty};
    std::atomic<std::uint32_t> failures{0U};
    std::uint64_t successful_acquires = 0U;

    std::thread recycler([&]() {
        for (std::uint64_t index = 0U;
             index < kTotalMessages;
             ++index) {
            std::uint8_t state = handoff_state.load(
                std::memory_order_acquire);
            while (state != kFull) {
                if (state == kFailed) {
                    return;
                }
                std::this_thread::yield();
                state = handoff_state.load(std::memory_order_acquire);
            }
            realtime::OwnedIngressMessageHandleV1 releasing =
                std::move(handoff);
            handoff_state.store(kEmpty, std::memory_order_release);
            releasing.reset();
        }
        handoff_state.store(kDone, std::memory_order_release);
    });

    std::thread acquirer([&]() {
        for (std::uint64_t sequence = 1U;
             sequence <= kTotalMessages;
             ++sequence) {
            while (handoff_state.load(std::memory_order_acquire) !=
                   kEmpty) {
                std::this_thread::yield();
            }
            realtime::OwnedIngressMessageHandleV1 acquired;
            for (;;) {
                const realtime::OwnedIngressMessageErrorV1 error =
                    pool->Acquire(
                        inspection, Metadata(sequence), &acquired);
                if (error ==
                        realtime::OwnedIngressMessageErrorV1::kNone &&
                    acquired) {
                    break;
                }
                if (error !=
                        realtime::OwnedIngressMessageErrorV1::
                            kPoolExhausted ||
                    acquired) {
                    failures.fetch_add(1U, std::memory_order_relaxed);
                    acquired.reset();
                    handoff_state.store(
                        kFailed, std::memory_order_release);
                    return;
                }
                std::this_thread::yield();
            }
            handoff = std::move(acquired);
            ++successful_acquires;
            handoff_state.store(kFull, std::memory_order_release);
        }
    });

    std::size_t snapshots = 0U;
    for (;;) {
        const std::uint8_t state = handoff_state.load(
            std::memory_order_acquire);
        if (state == kDone || state == kFailed) {
            break;
        }
        const auto snapshot = pool->Snapshot();
        if (!snapshot.serialized_acquire || snapshot.retired ||
            snapshot.active_messages > 1U ||
            snapshot.allocated_blocks != 1U ||
            snapshot.cached_blocks > 1U ||
            snapshot.active_messages + snapshot.cached_blocks != 1U) {
            failures.fetch_add(1U, std::memory_order_relaxed);
        }
        ++snapshots;
    }

    acquirer.join();
    recycler.join();
    const auto idle = pool->Snapshot();
    test->Expect(
        snapshots != 0U &&
            failures.load(std::memory_order_relaxed) == 0U &&
            successful_acquires == kTotalMessages &&
            idle.active_messages == 0U &&
            idle.allocated_blocks == 1U &&
            idle.cached_blocks == 1U,
        "immediate block reuse preserves concurrent snapshot accounting");

    pool->Retire();
    const auto retired = pool->Snapshot();
    test->Expect(
        retired.retired && retired.active_messages == 0U &&
            retired.allocated_blocks == 0U &&
            retired.cached_blocks == 0U &&
            retired.allocated_bytes == 0U,
        "immediate-reuse pool retires after every recycled handle");
}

void TestRetireAndOutlivingHandles(TestContext* test) {
    constexpr std::uint32_t kMaximumMessageBytes = 4096U;
    FakeMessage message(256U);
    realtime::OwnedIngressMessageInspectionV1 inspection{};
    test->Expect(
        Inspect(message, kMaximumMessageBytes, &inspection),
        "retire test message inspects");

    {
        auto pool = CreatePool(
            true, kMaximumMessageBytes, 2U, 512U, 2U);
        realtime::OwnedIngressMessageHandleV1 first;
        test->Expect(
            pool != nullptr &&
                pool->Acquire(inspection, Metadata(1U), &first) ==
                    realtime::OwnedIngressMessageErrorV1::kNone,
            "outliving handle acquires before pool destruction");
        realtime::OwnedIngressMessageHandleV1 second = first;
        pool.reset();
        test->Expect(
            first && second && first->body().size() == 256U &&
                first->body()[0U] == std::byte{0x5aU},
            "intrusive handles remain valid after serialized pool destruction");
        std::thread release_first([&]() { first.reset(); });
        std::thread release_second([&]() { second.reset(); });
        release_first.join();
        release_second.join();
    }

    {
        auto pool = CreatePool(
            true, kMaximumMessageBytes, 1U, 512U, 1U);
        realtime::OwnedIngressMessageHandleV1 handle;
        const bool acquired = pool != nullptr &&
            pool->Acquire(inspection, Metadata(1U), &handle) ==
                realtime::OwnedIngressMessageErrorV1::kNone &&
            static_cast<bool>(handle);
        if (pool != nullptr) {
            pool->Retire();
        }
        const realtime::OwnedIngressMessageErrorV1 retired_error =
            pool == nullptr
                ? realtime::OwnedIngressMessageErrorV1::kResourceExhausted
                : pool->Acquire(inspection, Metadata(2U), &handle);
        const realtime::OwnedIngressMessageErrorV1 null_error =
            pool == nullptr
                ? realtime::OwnedIngressMessageErrorV1::kResourceExhausted
                : pool->Acquire(inspection, Metadata(3U), nullptr);
        test->Expect(
            acquired && retired_error ==
                realtime::OwnedIngressMessageErrorV1::kPoolExhausted &&
                !handle && null_error ==
                realtime::OwnedIngressMessageErrorV1::kNullOutput,
            "retired serialized Acquire clears a prior output and preserves "
            "null-output precedence");
    }

    constexpr std::size_t kRounds = 64U;
    std::uint32_t unexpected = 0U;
    for (std::size_t round = 0U; round < kRounds; ++round) {
        auto pool = CreatePool(
            true, kMaximumMessageBytes, 1U, 512U, 1U);
        if (pool == nullptr) {
            ++unexpected;
            continue;
        }
        std::barrier start(2);
        realtime::OwnedIngressMessageHandleV1 handle;
        realtime::OwnedIngressMessageErrorV1 acquire_error =
            realtime::OwnedIngressMessageErrorV1::kUnexpectedMessageEncoding;
        std::thread acquirer([&]() {
            start.arrive_and_wait();
            acquire_error = pool->Acquire(
                inspection,
                Metadata(static_cast<std::uint64_t>(round + 1U)),
                &handle);
        });
        start.arrive_and_wait();
        pool->Retire();
        acquirer.join();
        if (acquire_error !=
                realtime::OwnedIngressMessageErrorV1::kNone &&
            acquire_error !=
                realtime::OwnedIngressMessageErrorV1::kPoolExhausted) {
            ++unexpected;
        }
        const auto retired = pool->Snapshot();
        if (!retired.retired ||
            retired.active_messages != (handle ? 1U : 0U) ||
            retired.cached_blocks != 0U ||
            retired.allocated_blocks != retired.active_messages) {
            ++unexpected;
        }
        handle.reset();
        const auto drained = pool->Snapshot();
        if (drained.active_messages != 0U ||
            drained.allocated_blocks != 0U ||
            drained.cached_blocks != 0U ||
            drained.allocated_bytes != 0U) {
            ++unexpected;
        }
    }
    test->Expect(
        unexpected == 0U,
        "Retire races entered Acquire and drains outliving handles exactly");
}

}  // namespace

int main() {
    TestContext test;
    TestDefaultConcurrentAcquirers(&test);
    TestSerializedReuseAndExhaustion(&test);
    TestSerializedSizeClassReplacement(&test);
    TestConcurrentSnapshotAccounting(&test);
    TestSerializedImmediateReuseSnapshotRace(&test);
    TestRetireAndOutlivingHandles(&test);

    if (test.failures() != 0) {
        std::cerr << test.failures()
                  << " owned ingress pool test(s) failed\n";
        return 1;
    }
    std::cout << "Owned ingress message pool tests passed\n";
    return 0;
}
