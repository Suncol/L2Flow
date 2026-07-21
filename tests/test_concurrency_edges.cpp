#include "l2mock/l2_mock.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace mdl = datayes::mdl;
namespace mock = datayes::mdl::mock;

namespace {

const std::chrono::seconds kTimeout(5);

struct TestContext {
    void Expect(bool condition, const std::string& description) {
        if (condition) {
            return;
        }
        ++failures;
        std::cerr << "FAIL: " << description << '\n';
    }

    int failures = 0;
};

template <typename Predicate>
bool WaitFor(Predicate predicate,
             std::chrono::steady_clock::duration timeout = kTimeout) {
    const std::chrono::steady_clock::time_point deadline =
        std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return predicate();
}

mock::Config BaseConfig(std::uint64_t seed) {
    mock::Config config;
    config.seed = seed;
    config.messages_per_second = 1000;
    config.book_depth = 2;
    config.orders_per_level = 2;
    config.callback_threads = 2;
    config.callback_queue_capacity = 16;
    config.backpressure = mock::BackpressurePolicy::Block;
    config.clock_mode = mock::ClockMode::SimulatedTradingDay;
    config.simulated_min_step_ms = 1;
    config.simulated_max_step_ms = 1;

    mock::Instrument instrument;
    instrument.service_id = mdl::MDLSID_MDL_SHL2;
    instrument.security_id = "600001";
    instrument.reference_price_milli = 100000;
    instrument.tick_size_milli = 10;
    instrument.lot_size = 100;
    config.instruments.push_back(instrument);
    return config;
}

bool ConnectAll(mdl::IOManagerPtr& manager,
                mdl::MessageHandlerBase* handler,
                bool multithread_callback,
                mdl::SubscriberPtr* subscriber,
                std::string* error) {
    *subscriber =
        manager->CreateSubscriber(handler, multithread_callback);
    if (subscriber->IsNull()) {
        *error = "CreateSubscriber returned null";
        return false;
    }
    mock::SubscribeAll(subscriber->Get());
    const char* const result = (*subscriber)->Connect();
    *error = result == nullptr ? "Connect returned null" : result;
    return result != nullptr && result[0] == '\0';
}

void SubscribeToSample(mdl::Subscriber* subscriber,
                       const mdl::MDLMessage* sample) {
    const mdl::MDLMessageHead* const head = sample->GetHead();
    subscriber->AddSubscription(
        head->ServiceID, head->ServiceVersion, head->MessageID);
}

class SerialGateHandler final : public mdl::MessageHandlerBase {
public:
    void OnMessage(mdl::Subscriber*,
                   const mdl::MDLMessage* message) override {
        const int in_flight =
            in_flight_.fetch_add(1, std::memory_order_acq_rel) + 1;
        int maximum = max_in_flight_.load(std::memory_order_acquire);
        while (in_flight > maximum &&
               !max_in_flight_.compare_exchange_weak(
                   maximum, in_flight, std::memory_order_acq_rel)) {
        }
        if (in_flight != 1) {
            overlap_.store(true, std::memory_order_release);
        }

        if (message != nullptr && message->GetHead() != nullptr) {
            std::lock_guard<std::mutex> lock(sample_mutex_);
            if (sample_.IsNull()) {
                sample_ = message->Copy();
                sample_condition_.notify_all();
            }
        }
        callbacks_.fetch_add(1, std::memory_order_acq_rel);
        std::this_thread::sleep_for(std::chrono::microseconds(100));
        in_flight_.fetch_sub(1, std::memory_order_acq_rel);
    }

    bool WaitForSample() {
        std::unique_lock<std::mutex> lock(sample_mutex_);
        return sample_condition_.wait_for(
            lock, kTimeout, [this] { return !sample_.IsNull(); });
    }

    mdl::MDLMessagePtr sample() const {
        std::lock_guard<std::mutex> lock(sample_mutex_);
        return sample_;
    }

    int max_in_flight() const {
        return max_in_flight_.load(std::memory_order_acquire);
    }

    bool overlap() const {
        return overlap_.load(std::memory_order_acquire);
    }

    std::uint64_t callbacks() const {
        return callbacks_.load(std::memory_order_acquire);
    }

private:
    std::atomic<int> in_flight_{0};
    std::atomic<int> max_in_flight_{0};
    std::atomic<bool> overlap_{false};
    std::atomic<std::uint64_t> callbacks_{0};
    mutable std::mutex sample_mutex_;
    std::condition_variable sample_condition_;
    mdl::MDLMessagePtr sample_;
};

mdl::MDLMessagePtr CheckSerializedDirectCallbacks(TestContext* test) {
    const mock::Config config = BaseConfig(0x5e21a11ULL);
    SerialGateHandler handler;
    mdl::IOManagerPtr manager = mock::CreateIOManager(config);
    mdl::SubscriberPtr subscriber;
    std::string connect_error;
    const bool connected = ConnectAll(
        manager, &handler, false, &subscriber, &connect_error);
    test->Expect(
        connected,
        "serialized callback subscriber connects" +
            (connect_error.empty() ? std::string()
                                   : std::string(": ") + connect_error));
    if (!connected) {
        manager->Shutdown();
        return mdl::MDLMessagePtr();
    }

    const bool captured = handler.WaitForSample();
    test->Expect(captured, "a native generated message is copied in callback");
    const mdl::MDLMessagePtr sample = handler.sample();
    if (!captured || sample.IsNull()) {
        manager->Shutdown();
        return mdl::MDLMessagePtr();
    }

    const int owner_threads = 6;
    const int publishes_per_thread = 40;
    std::atomic<int> ready{0};
    std::atomic<bool> start{false};
    std::atomic<int> finished{0};
    std::vector<std::thread> publishers;
    publishers.reserve(owner_threads);
    for (int thread_index = 0;
         thread_index < owner_threads;
         ++thread_index) {
        publishers.emplace_back([&, thread_index] {
            ready.fetch_add(1, std::memory_order_acq_rel);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            for (int index = 0; index < publishes_per_thread; ++index) {
                if (((index + thread_index) & 1) == 0) {
                    manager->SyncPublish(sample.Get());
                } else {
                    manager->AsyncPublish(sample.Get());
                }
            }
            finished.fetch_add(1, std::memory_order_acq_rel);
        });
    }

    const bool all_ready =
        WaitFor([&ready] { return ready.load() == owner_threads; });
    test->Expect(all_ready, "all external publisher threads reach the gate");
    start.store(true, std::memory_order_release);
    const bool all_finished =
        WaitFor([&finished] { return finished.load() == owner_threads; });
    test->Expect(
        all_finished,
        "concurrent SyncPublish/AsyncPublish calls finish within five seconds");
    if (!all_finished) {
        manager->Shutdown();
    }
    for (std::thread& publisher : publishers) {
        publisher.join();
    }
    manager->Shutdown();

    const std::uint64_t expected_external =
        static_cast<std::uint64_t>(owner_threads) *
        static_cast<std::uint64_t>(publishes_per_thread);
    test->Expect(
        handler.callbacks() >= expected_external + 1U,
        "all external publishes plus the captured generator callback arrive");
    test->Expect(
        !handler.overlap() && handler.max_in_flight() == 1,
        "multithread_callback=false serializes generator and owner callbacks");
    test->Expect(
        mock::GetStatistics(manager.Get()).active_subscribers == 0,
        "serialized callback manager fully shuts down");
    return sample;
}

class ExternalShutdownHandler final : public mdl::MessageHandlerBase {
public:
    explicit ExternalShutdownHandler(mdl::IOManager* manager)
        : manager_(manager) {}

    void ArmForCurrentThread() {
        owner_thread_ = std::this_thread::get_id();
        armed_.store(true, std::memory_order_release);
    }

    void OnMessage(mdl::Subscriber*,
                   const mdl::MDLMessage*) override {
        callbacks_.fetch_add(1, std::memory_order_acq_rel);
        if (!armed_.load(std::memory_order_acquire) ||
            std::this_thread::get_id() != owner_thread_) {
            return;
        }
        bool expected = false;
        if (!called_.compare_exchange_strong(
                expected, true, std::memory_order_acq_rel)) {
            return;
        }
        manager_->Shutdown();
        shutdown_returned_.store(true, std::memory_order_release);
    }

    bool shutdown_returned() const {
        return shutdown_returned_.load(std::memory_order_acquire);
    }

    std::uint64_t callbacks() const {
        return callbacks_.load(std::memory_order_acquire);
    }

private:
    mdl::IOManager* manager_;
    std::thread::id owner_thread_;
    std::atomic<bool> armed_{false};
    std::atomic<bool> called_{false};
    std::atomic<bool> shutdown_returned_{false};
    std::atomic<std::uint64_t> callbacks_{0};
};

void CheckExternalPublishCallbackShutdown(
    const mdl::MDLMessagePtr& sample,
    TestContext* test) {
    mock::Config config = BaseConfig(0x5e21a12ULL);
    config.messages_per_second = 10;
    mdl::IOManagerPtr manager = mock::CreateIOManager(config);
    ExternalShutdownHandler handler(manager.Get());
    mdl::SubscriberPtr subscriber =
        manager->CreateSubscriber(&handler, false);
    SubscribeToSample(subscriber.Get(), sample.Get());
    const char* const result = subscriber->Connect();
    test->Expect(
        result != nullptr && result[0] == '\0',
        "external-shutdown subscriber connects");
    if (result == nullptr || result[0] != '\0') {
        manager->Shutdown();
        return;
    }

    handler.ArmForCurrentThread();
    const std::chrono::steady_clock::time_point publish_start =
        std::chrono::steady_clock::now();
    manager->SyncPublish(sample.Get());
    const std::chrono::steady_clock::duration publish_elapsed =
        std::chrono::steady_clock::now() - publish_start;
    test->Expect(
        publish_elapsed < kTimeout && handler.shutdown_returned(),
        "callback-side Shutdown returns from external SyncPublish");

    const std::chrono::steady_clock::time_point shutdown_start =
        std::chrono::steady_clock::now();
    manager->Shutdown();
    const std::chrono::steady_clock::duration shutdown_elapsed =
        std::chrono::steady_clock::now() - shutdown_start;
    const std::uint64_t stopped_count = handler.callbacks();
    std::this_thread::sleep_for(std::chrono::milliseconds(25));
    test->Expect(
        shutdown_elapsed < kTimeout,
        "owner Shutdown synchronizes external callback teardown");
    test->Expect(
        handler.callbacks() == stopped_count,
        "no callback follows owner Shutdown after callback-side Shutdown");
    test->Expect(
        mock::GetStatistics(manager.Get()).active_subscribers == 0,
        "external callback shutdown clears active subscribers");
}

class NestedBHandler final : public mdl::MessageHandlerBase {
public:
    explicit NestedBHandler(mdl::IOManager* manager_a)
        : manager_a_(manager_a) {}

    void ArmForCurrentThread() {
        owner_thread_ = std::this_thread::get_id();
        armed_.store(true, std::memory_order_release);
    }

    void OnMessage(mdl::Subscriber*,
                   const mdl::MDLMessage*) override {
        if (!armed_.load(std::memory_order_acquire) ||
            std::this_thread::get_id() != owner_thread_) {
            return;
        }
        bool expected = false;
        if (!called_.compare_exchange_strong(
                expected, true, std::memory_order_acq_rel)) {
            return;
        }
        manager_a_->Shutdown();
        returned_.store(true, std::memory_order_release);
    }

    bool returned() const {
        return returned_.load(std::memory_order_acquire);
    }

private:
    mdl::IOManager* manager_a_;
    std::thread::id owner_thread_;
    std::atomic<bool> armed_{false};
    std::atomic<bool> called_{false};
    std::atomic<bool> returned_{false};
};

class NestedAHandler final : public mdl::MessageHandlerBase {
public:
    NestedAHandler(mdl::IOManager* manager_b,
                   const mdl::MDLMessage* sample)
        : manager_b_(manager_b), sample_(sample) {}

    void ArmForCurrentThread() {
        owner_thread_ = std::this_thread::get_id();
        armed_.store(true, std::memory_order_release);
    }

    void OnMessage(mdl::Subscriber*,
                   const mdl::MDLMessage*) override {
        if (!armed_.load(std::memory_order_acquire) ||
            std::this_thread::get_id() != owner_thread_) {
            return;
        }
        bool expected = false;
        if (!called_.compare_exchange_strong(
                expected, true, std::memory_order_acq_rel)) {
            return;
        }
        manager_b_->SyncPublish(sample_);
        returned_.store(true, std::memory_order_release);
    }

    bool returned() const {
        return returned_.load(std::memory_order_acquire);
    }

private:
    mdl::IOManager* manager_b_;
    const mdl::MDLMessage* sample_;
    std::thread::id owner_thread_;
    std::atomic<bool> armed_{false};
    std::atomic<bool> called_{false};
    std::atomic<bool> returned_{false};
};

void CheckNestedRuntimeShutdown(const mdl::MDLMessagePtr& sample,
                                TestContext* test) {
    mock::Config config_a = BaseConfig(0x5e21a13ULL);
    mock::Config config_b = BaseConfig(0x5e21a14ULL);
    config_a.messages_per_second = 10;
    config_b.messages_per_second = 10;
    mdl::IOManagerPtr manager_a = mock::CreateIOManager(config_a);
    mdl::IOManagerPtr manager_b = mock::CreateIOManager(config_b);
    NestedBHandler handler_b(manager_a.Get());
    NestedAHandler handler_a(manager_b.Get(), sample.Get());

    mdl::SubscriberPtr subscriber_a =
        manager_a->CreateSubscriber(&handler_a, false);
    mdl::SubscriberPtr subscriber_b =
        manager_b->CreateSubscriber(&handler_b, false);
    SubscribeToSample(subscriber_a.Get(), sample.Get());
    SubscribeToSample(subscriber_b.Get(), sample.Get());
    const char* const result_a = subscriber_a->Connect();
    const char* const result_b = subscriber_b->Connect();
    const bool connected =
        result_a != nullptr && result_a[0] == '\0' &&
        result_b != nullptr && result_b[0] == '\0';
    test->Expect(connected, "both nested-runtime subscribers connect");
    if (!connected) {
        manager_a->Shutdown();
        manager_b->Shutdown();
        return;
    }

    handler_b.ArmForCurrentThread();
    handler_a.ArmForCurrentThread();
    const std::chrono::steady_clock::time_point nested_start =
        std::chrono::steady_clock::now();
    manager_a->SyncPublish(sample.Get());
    const std::chrono::steady_clock::duration nested_elapsed =
        std::chrono::steady_clock::now() - nested_start;
    test->Expect(
        nested_elapsed < kTimeout &&
            handler_a.returned() && handler_b.returned(),
        "A callback -> B.SyncPublish -> B callback -> A.Shutdown returns");

    const std::chrono::steady_clock::time_point owner_start =
        std::chrono::steady_clock::now();
    manager_a->Shutdown();
    manager_b->Shutdown();
    const std::chrono::steady_clock::duration owner_elapsed =
        std::chrono::steady_clock::now() - owner_start;
    test->Expect(
        owner_elapsed < kTimeout,
        "owner synchronizes both linked runtimes without deadlock");
    test->Expect(
        mock::GetStatistics(manager_a.Get()).active_subscribers == 0 &&
            mock::GetStatistics(manager_b.Get()).active_subscribers == 0,
        "nested runtime teardown clears both subscriber sets");
}

class AtomicCountingHandler final : public mdl::MessageHandlerBase {
public:
    void OnMessage(mdl::Subscriber*,
                   const mdl::MDLMessage*) override {
        callbacks_.fetch_add(1, std::memory_order_acq_rel);
    }

    std::uint64_t callbacks() const {
        return callbacks_.load(std::memory_order_acquire);
    }

private:
    std::atomic<std::uint64_t> callbacks_{0};
};

void CheckConcurrentCreateConnectAndShutdown(TestContext* test) {
    const int rounds = 8;
    const int creators = 8;
    for (int round = 0; round < rounds; ++round) {
        mock::Config config =
            BaseConfig(0x5e22000ULL + static_cast<std::uint64_t>(round));
        config.messages_per_second = 0;
        config.callback_threads = 2;
        config.callback_queue_capacity = 4;
        mdl::IOManagerPtr manager = mock::CreateIOManager(config);

        // Handlers are declared before subscribers so subscribers are released
        // first at scope exit.  Shutdown also synchronizes every callback.
        std::vector<std::shared_ptr<AtomicCountingHandler> > handlers(
            creators);
        std::vector<mdl::SubscriberPtr> subscribers(creators);
        std::vector<std::uint8_t> connect_outcomes(creators, 0);
        std::atomic<int> ready{0};
        std::atomic<bool> start{false};
        std::atomic<int> finished{0};
        std::vector<std::thread> threads;
        threads.reserve(static_cast<std::size_t>(creators + 1));

        for (int index = 0; index < creators; ++index) {
            threads.emplace_back([&, index] {
                std::shared_ptr<AtomicCountingHandler> handler(
                    new AtomicCountingHandler());
                handlers[static_cast<std::size_t>(index)] = handler;
                ready.fetch_add(1, std::memory_order_acq_rel);
                while (!start.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
                mdl::SubscriberPtr subscriber =
                    manager->CreateSubscriber(handler.get(), false);
                mock::SubscribeAll(subscriber.Get());
                const char* const result = subscriber->Connect();
                connect_outcomes[static_cast<std::size_t>(index)] =
                    result == nullptr ? 3U
                                      : (result[0] == '\0' ? 1U : 2U);
                subscribers[static_cast<std::size_t>(index)] = subscriber;
                finished.fetch_add(1, std::memory_order_acq_rel);
            });
        }

        threads.emplace_back([&] {
            ready.fetch_add(1, std::memory_order_acq_rel);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            if ((round & 1) != 0) {
                std::this_thread::yield();
            }
            manager->Shutdown();
            finished.fetch_add(1, std::memory_order_acq_rel);
        });

        const bool all_ready = WaitFor([&ready, creators] {
            return ready.load(std::memory_order_acquire) == creators + 1;
        });
        test->Expect(
            all_ready,
            "lifecycle race round " + std::to_string(round) +
                " reaches the common start gate");
        start.store(true, std::memory_order_release);
        const bool all_finished = WaitFor([&finished, creators] {
            return finished.load(std::memory_order_acquire) == creators + 1;
        });
        test->Expect(
            all_finished,
            "lifecycle race round " + std::to_string(round) +
                " completes within five seconds");
        if (!all_finished) {
            manager->Shutdown();
        }
        for (std::thread& thread : threads) {
            thread.join();
        }

        test->Expect(
            std::all_of(
                connect_outcomes.begin(),
                connect_outcomes.end(),
                [](std::uint8_t value) { return value == 1U || value == 2U; }),
            "lifecycle race Connect calls return a non-null result");
        std::shared_ptr<AtomicCountingHandler> after_handler(
            new AtomicCountingHandler());
        mdl::SubscriberPtr after_subscriber =
            manager->CreateSubscriber(after_handler.get(), false);
        mock::SubscribeAll(after_subscriber.Get());
        const char* const after_result = after_subscriber->Connect();
        test->Expect(
            after_result != nullptr && after_result[0] != '\0',
            "lifecycle race round " + std::to_string(round) +
                " rejects Connect after Shutdown");
        test->Expect(
            mock::GetStatistics(manager.Get()).active_subscribers == 0,
            "lifecycle race round " + std::to_string(round) +
                " leaves active_subscribers at zero");
        manager->Shutdown();
    }
}

class SlowAutoHandler final : public mdl::MessageHandlerBase {
public:
    void OnMessage(mdl::Subscriber*,
                   const mdl::MDLMessage*) override {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            ++callbacks_;
            ++in_flight_;
        }
        condition_.notify_all();
        std::unique_lock<std::mutex> lock(mutex_);
        condition_.wait_for(
            lock, kTimeout, [this] { return released_; });
        --in_flight_;
        condition_.notify_all();
    }

    bool WaitForInFlight() {
        std::unique_lock<std::mutex> lock(mutex_);
        return condition_.wait_for(
            lock, kTimeout, [this] { return in_flight_ != 0; });
    }

    void Release() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            released_ = true;
        }
        condition_.notify_all();
    }

    std::uint64_t callbacks() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return callbacks_;
    }

private:
    mutable std::mutex mutex_;
    std::condition_variable condition_;
    bool released_ = false;
    std::uint64_t callbacks_ = 0;
    std::uint64_t in_flight_ = 0;
};

void CheckOwnerAutoSubscriberStop(TestContext* test) {
    mock::Config config = BaseConfig(0x5e21a15ULL);
    config.messages_per_second = 0;
    config.callback_threads = 4;
    config.callback_queue_capacity = 16;
    SlowAutoHandler handler;
    mdl::IOManagerPtr manager = mock::CreateIOManager(config);
    mdl::AutoSubscriberPtr automatic =
        manager->CreateAutoSubscriber(&handler, true);
    const char* const result = automatic->Start();
    test->Expect(
        result != nullptr && result[0] == '\0',
        "slow multithread AutoSubscriber starts");
    if (result == nullptr || result[0] != '\0') {
        handler.Release();
        manager->Shutdown();
        return;
    }
    const bool entered = handler.WaitForInFlight();
    test->Expect(entered, "slow AutoSubscriber has an in-flight callback");

    std::atomic<bool> stop_started{false};
    std::atomic<bool> stop_returned{false};
    std::thread stopper([&automatic, &stop_started, &stop_returned] {
        stop_started.store(true, std::memory_order_release);
        automatic->Stop();
        stop_returned.store(true, std::memory_order_release);
    });
    const bool started = WaitFor([&stop_started] {
        return stop_started.load(std::memory_order_acquire);
    });
    test->Expect(started, "owner AutoSubscriber::Stop thread starts");
    std::this_thread::sleep_for(std::chrono::milliseconds(25));
    if (entered && started) {
        test->Expect(
            !stop_returned.load(std::memory_order_acquire),
            "owner AutoSubscriber::Stop waits for the in-flight callback");
    }
    handler.Release();
    const bool stopped = WaitFor([&stop_returned] {
        return stop_returned.load(std::memory_order_acquire);
    });
    test->Expect(stopped, "owner AutoSubscriber::Stop returns within timeout");
    stopper.join();

    const std::uint64_t stopped_count = handler.callbacks();
    std::this_thread::sleep_for(std::chrono::milliseconds(25));
    test->Expect(
        handler.callbacks() == stopped_count,
        "callback count is stable after owner AutoSubscriber::Stop");
    test->Expect(
        mock::GetStatistics(manager.Get()).active_subscribers == 0,
        "owner AutoSubscriber::Stop synchronously clears active count");
    manager->Shutdown();
}

class CallbackAutoStopHandler final : public mdl::MessageHandlerBase {
public:
    void SetAutoSubscriber(mdl::AutoSubscriber* automatic) {
        automatic_ = automatic;
    }

    void Arm() {
        armed_.store(true, std::memory_order_release);
    }

    void OnMessage(mdl::Subscriber*,
                   const mdl::MDLMessage*) override {
        callbacks_.fetch_add(1, std::memory_order_acq_rel);
        if (!armed_.load(std::memory_order_acquire)) {
            return;
        }
        bool expected = false;
        if (!called_.compare_exchange_strong(
                expected, true, std::memory_order_acq_rel)) {
            return;
        }
        automatic_->Stop();
        returned_.store(true, std::memory_order_release);
    }

    bool returned() const {
        return returned_.load(std::memory_order_acquire);
    }

    std::uint64_t callbacks() const {
        return callbacks_.load(std::memory_order_acquire);
    }

private:
    mdl::AutoSubscriber* automatic_ = nullptr;
    std::atomic<bool> armed_{false};
    std::atomic<bool> called_{false};
    std::atomic<bool> returned_{false};
    std::atomic<std::uint64_t> callbacks_{0};
};

void CheckCallbackAutoSubscriberStop(TestContext* test) {
    mock::Config config = BaseConfig(0x5e21a16ULL);
    config.messages_per_second = 1000;
    config.callback_threads = 2;
    CallbackAutoStopHandler handler;
    mdl::IOManagerPtr manager = mock::CreateIOManager(config);
    mdl::AutoSubscriberPtr automatic =
        manager->CreateAutoSubscriber(&handler, true);
    handler.SetAutoSubscriber(automatic.Get());
    const char* const result = automatic->Start();
    test->Expect(
        result != nullptr && result[0] == '\0',
        "callback-stop AutoSubscriber starts");
    if (result == nullptr || result[0] != '\0') {
        manager->Shutdown();
        return;
    }
    handler.Arm();
    const bool returned =
        WaitFor([&handler] { return handler.returned(); });
    test->Expect(
        returned,
        "AutoSubscriber::Stop called inside its callback does not deadlock");
    manager->Shutdown();
    const std::uint64_t stopped_count = handler.callbacks();
    std::this_thread::sleep_for(std::chrono::milliseconds(25));
    test->Expect(
        handler.callbacks() == stopped_count,
        "owner manager Shutdown synchronizes callback-side Stop");
    test->Expect(
        mock::GetStatistics(manager.Get()).active_subscribers == 0,
        "callback-side AutoSubscriber::Stop leaves no active subscriber");
}

void CheckConcurrentAutoSubscriberLifecycle(TestContext* test) {
    mock::Config config = BaseConfig(0x5e21a18ULL);
    config.messages_per_second = 0;
    config.callback_threads = 2;
    config.callback_queue_capacity = 8;
    AtomicCountingHandler handler;
    mdl::IOManagerPtr manager = mock::CreateIOManager(config);
    mdl::AutoSubscriberPtr automatic =
        manager->CreateAutoSubscriber(&handler, true);

    const int thread_count = 8;
    const int operations_per_thread = 80;
    std::atomic<int> ready{0};
    std::atomic<bool> start{false};
    std::atomic<int> finished{0};
    std::vector<std::thread> threads;
    threads.reserve(thread_count);
    for (int thread_index = 0;
         thread_index < thread_count;
         ++thread_index) {
        threads.emplace_back([&, thread_index] {
            ready.fetch_add(1, std::memory_order_acq_rel);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            for (int operation = 0;
                 operation < operations_per_thread;
                 ++operation) {
                if (((thread_index + operation) & 1) == 0) {
                    (void)automatic->Start();
                } else {
                    automatic->Stop();
                }
            }
            finished.fetch_add(1, std::memory_order_acq_rel);
        });
    }
    const bool all_ready = WaitFor([&ready, thread_count] {
        return ready.load(std::memory_order_acquire) == thread_count;
    });
    test->Expect(
        all_ready,
        "concurrent AutoSubscriber lifecycle threads reach their start gate");
    start.store(true, std::memory_order_release);
    const bool all_finished = WaitFor([&finished, thread_count] {
        return finished.load(std::memory_order_acquire) == thread_count;
    });
    test->Expect(
        all_finished,
        "concurrent AutoSubscriber Start/Stop finishes within five seconds");
    if (!all_finished) {
        manager->Shutdown();
    }
    for (std::thread& thread : threads) {
        thread.join();
    }

    automatic->Stop();
    test->Expect(
        mock::GetStatistics(manager.Get()).active_subscribers == 0,
        "concurrent AutoSubscriber Start/Stop leaves no active subscriber");
    if (!all_finished) {
        return;
    }

    const std::uint64_t before_restart = handler.callbacks();
    const char* const restart_result = automatic->Start();
    const bool restarted =
        restart_result != nullptr && restart_result[0] == '\0';
    test->Expect(
        restarted,
        "AutoSubscriber remains restartable after concurrent lifecycle calls");
    if (restarted) {
        test->Expect(
            WaitFor([&handler, before_restart] {
                return handler.callbacks() > before_restart;
            }),
            "restarted AutoSubscriber receives callbacks");
    }
    automatic->Stop();
    test->Expect(
        mock::GetStatistics(manager.Get()).active_subscribers == 0,
        "restarted AutoSubscriber Stop preserves active-count accounting");
    manager->Shutdown();
}

class ReentrantAsyncHandler final : public mdl::MessageHandlerBase {
public:
    explicit ReentrantAsyncHandler(mdl::IOManager* manager)
        : manager_(manager) {}

    void OnMessage(mdl::Subscriber*,
                   const mdl::MDLMessage* message) override {
        callbacks_.fetch_add(1, std::memory_order_acq_rel);
        bool expected = false;
        if (first_.compare_exchange_strong(
                expected, true, std::memory_order_acq_rel)) {
            entered_.store(true, std::memory_order_release);
            const std::chrono::steady_clock::time_point deadline =
                std::chrono::steady_clock::now() + kTimeout;
            while (!trigger_.load(std::memory_order_acquire) &&
                   std::chrono::steady_clock::now() < deadline) {
                std::this_thread::yield();
            }
            if (!trigger_.load(std::memory_order_acquire)) {
                return;
            }
            manager_->AsyncPublish(message);
            returned_.store(true, std::memory_order_release);
        }
    }

    void Trigger() {
        trigger_.store(true, std::memory_order_release);
    }

    bool entered() const {
        return entered_.load(std::memory_order_acquire);
    }

    bool returned() const {
        return returned_.load(std::memory_order_acquire);
    }

private:
    mdl::IOManager* manager_;
    std::atomic<bool> first_{false};
    std::atomic<bool> entered_{false};
    std::atomic<bool> trigger_{false};
    std::atomic<bool> returned_{false};
    std::atomic<std::uint64_t> callbacks_{0};
};

void CheckBlockQueueReentrantPublish(TestContext* test) {
    mock::Config config = BaseConfig(0x5e21a17ULL);
    config.messages_per_second = 0;
    config.callback_threads = 1;
    config.callback_queue_capacity = 1;
    config.backpressure = mock::BackpressurePolicy::Block;
    mdl::IOManagerPtr manager = mock::CreateIOManager(config);
    ReentrantAsyncHandler handler(manager.Get());
    mdl::SubscriberPtr subscriber;
    std::string connect_error;
    const bool connected = ConnectAll(
        manager, &handler, true, &subscriber, &connect_error);
    test->Expect(
        connected,
        "reentrant Block-queue subscriber connects" +
            (connect_error.empty() ? std::string()
                                   : std::string(": ") + connect_error));
    if (!connected) {
        handler.Trigger();
        manager->Shutdown();
        return;
    }

    const bool entered =
        WaitFor([&handler] { return handler.entered(); });
    const bool queue_full = WaitFor([&manager] {
        const mock::Statistics statistics =
            mock::GetStatistics(manager.Get());
        // One task is executing, one is queued, and a third generation has
        // reached Enqueue and is blocked on that full queue.
        return statistics.generated >= 3 &&
               statistics.queue_high_watermark == 1 &&
               statistics.delivered == 0;
    });
    test->Expect(entered, "a callback occupies the sole callback worker");
    test->Expect(queue_full, "Block queue reaches its capacity of one");
    handler.Trigger();
    const bool returned =
        WaitFor([&handler] { return handler.returned(); });
    test->Expect(
        returned,
        "callback reentrant AsyncPublish returns while Block queue is full");

    const std::chrono::steady_clock::time_point shutdown_start =
        std::chrono::steady_clock::now();
    manager->Shutdown();
    const std::chrono::steady_clock::duration shutdown_elapsed =
        std::chrono::steady_clock::now() - shutdown_start;
    const mock::Statistics statistics =
        mock::GetStatistics(manager.Get());
    test->Expect(
        shutdown_elapsed < kTimeout,
        "Block-queue reentrant test shuts down within timeout");
    test->Expect(
        statistics.dropped > 0,
        "full Block queue drops reentrant callback publish instead of waiting");
    test->Expect(
        statistics.queue_high_watermark == 1,
        "reentrant Block queue never exceeds configured capacity");
    test->Expect(
        statistics.active_subscribers == 0,
        "reentrant Block queue teardown clears active subscriber");
}

} // namespace

int main() {
    TestContext test;
    const mdl::MDLMessagePtr sample =
        CheckSerializedDirectCallbacks(&test);
    if (sample.IsNull()) {
        std::cerr << "concurrency edge test cannot continue without sample\n";
        return 1;
    }

    CheckExternalPublishCallbackShutdown(sample, &test);
    CheckNestedRuntimeShutdown(sample, &test);
    CheckConcurrentCreateConnectAndShutdown(&test);
    CheckOwnerAutoSubscriberStop(&test);
    CheckCallbackAutoSubscriberStop(&test);
    CheckConcurrentAutoSubscriberLifecycle(&test);
    CheckBlockQueueReentrantPublish(&test);

    if (test.failures != 0) {
        std::cerr << "concurrency edge test failed with "
                  << test.failures << " error(s)\n";
        return 1;
    }
    std::cout
        << "concurrency edge test passed: serialized callbacks, linked "
           "runtime shutdown, lifecycle races, AutoSubscriber Stop, and "
           "reentrant Block queue\n";
    return 0;
}
