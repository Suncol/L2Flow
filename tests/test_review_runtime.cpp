#include "l2mock/l2_mock.h"

#include "mdl_shl2_msg.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace mdl = datayes::mdl;
namespace mock = datayes::mdl::mock;
namespace sh = datayes::mdl::mdl_shl2_msg;

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
bool WaitFor(
    Predicate predicate,
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

mock::Config SingleInstrumentConfig(
    std::uint64_t seed,
    std::uint8_t service_id,
    const std::string& identifier) {
    mock::Config config;
    config.seed = seed;
    config.messages_per_second = 1000;
    config.book_depth = 2;
    config.orders_per_level = 2;
    config.callback_threads = 1;
    config.callback_queue_capacity = 16;
    config.backpressure = mock::BackpressurePolicy::Block;
    config.clock_mode = mock::ClockMode::SimulatedTradingDay;
    config.simulated_min_step_ms = 1;
    config.simulated_max_step_ms = 1;

    mock::Instrument instrument;
    instrument.service_id = service_id;
    instrument.security_id = identifier;
    instrument.reference_price_milli = 100000;
    instrument.tick_size_milli = 10;
    instrument.lot_size = 100;
    config.instruments.push_back(instrument);
    return config;
}

class CaptureHandler final : public mdl::MessageHandlerBase {
public:
    void OnMessage(mdl::Subscriber*,
                   const mdl::MDLMessage* message) override {
        std::lock_guard<std::mutex> lock(mutex_);
        if (sample_.IsNull() && message != nullptr) {
            sample_ = message->Copy();
            condition_.notify_all();
        }
    }

    mdl::MDLMessagePtr WaitForSample() {
        std::unique_lock<std::mutex> lock(mutex_);
        (void)condition_.wait_for(
            lock, kTimeout, [this] { return !sample_.IsNull(); });
        return sample_;
    }

private:
    std::mutex mutex_;
    std::condition_variable condition_;
    mdl::MDLMessagePtr sample_;
};

mdl::MDLMessagePtr CaptureShanghaiMessage(TestContext* test) {
    mock::Config config = SingleInstrumentConfig(
        0x72657669657701ULL, mdl::MDLSID_MDL_SHL2, "600001");
    CaptureHandler handler;
    mdl::IOManagerPtr manager = mock::CreateIOManager(config);
    mdl::SubscriberPtr subscriber =
        manager->CreateSubscriber(&handler, false);
    subscriber->AddSubscription(
        sh::SHL2MarketData::ServiceID,
        sh::SHL2MarketData::ServiceVer,
        sh::SHL2MarketData::MessageID);
    const char* const connect_result = subscriber->Connect();
    const bool connected =
        connect_result != nullptr && connect_result[0] == '\0';
    test->Expect(connected, "sample-capture subscriber connects");
    mdl::MDLMessagePtr sample;
    if (connected) {
        sample = handler.WaitForSample();
        test->Expect(
            !sample.IsNull(),
            "sample-capture subscriber receives a Shanghai message");
    }
    manager->Shutdown();
    return sample;
}

mdl::MDLMessagePtr CaptureShanghaiMessageFor(
    const std::string& identifier,
    std::uint64_t seed,
    TestContext* test) {
    mock::Config config = SingleInstrumentConfig(
        seed, mdl::MDLSID_MDL_SHL2, identifier);
    CaptureHandler handler;
    mdl::IOManagerPtr manager = mock::CreateIOManager(config);
    mdl::SubscriberPtr subscriber =
        manager->CreateSubscriber(&handler, false);
    subscriber->AddSubscription(
        sh::SHL2MarketData::ServiceID,
        sh::SHL2MarketData::ServiceVer,
        sh::SHL2MarketData::MessageID);
    const char* const connect_result = subscriber->Connect();
    const bool connected =
        connect_result != nullptr && connect_result[0] == '\0';
    test->Expect(
        connected,
        "cross-publish sample-capture subscriber connects");
    mdl::MDLMessagePtr sample;
    if (connected) {
        sample = handler.WaitForSample();
        test->Expect(
            !sample.IsNull(),
            "cross-publish sample capture receives its Shanghai message");
    }
    manager->Shutdown();
    return sample;
}

class ReentrantSerializedHandler final : public mdl::MessageHandlerBase {
public:
    explicit ReentrantSerializedHandler(mdl::IOManager* manager)
        : manager_(manager) {}

    void OnMessage(mdl::Subscriber*,
                   const mdl::MDLMessage* message) override {
        const int depth =
            depth_.fetch_add(1, std::memory_order_acq_rel) + 1;
        int maximum = max_depth_.load(std::memory_order_acquire);
        while (depth > maximum &&
               !max_depth_.compare_exchange_weak(
                   maximum, depth, std::memory_order_acq_rel)) {
        }
        callbacks_.fetch_add(1, std::memory_order_acq_rel);

        bool expected = false;
        if (first_.compare_exchange_strong(
                expected, true, std::memory_order_acq_rel)) {
            entered_.store(true, std::memory_order_release);
            manager_->AsyncPublish(message);
            republish_returned_.store(true, std::memory_order_release);
            std::unique_lock<std::mutex> lock(gate_mutex_);
            (void)gate_condition_.wait_for(
                lock, kTimeout, [this] { return released_; });
        }

        depth_.fetch_sub(1, std::memory_order_acq_rel);
    }

    void Release() {
        {
            std::lock_guard<std::mutex> lock(gate_mutex_);
            released_ = true;
        }
        gate_condition_.notify_all();
    }

    bool entered() const {
        return entered_.load(std::memory_order_acquire);
    }

    bool republish_returned() const {
        return republish_returned_.load(std::memory_order_acquire);
    }

    std::uint64_t callbacks() const {
        return callbacks_.load(std::memory_order_acquire);
    }

    int max_depth() const {
        return max_depth_.load(std::memory_order_acquire);
    }

private:
    mdl::IOManager* manager_;
    std::atomic<int> depth_{0};
    std::atomic<int> max_depth_{0};
    std::atomic<std::uint64_t> callbacks_{0};
    std::atomic<bool> first_{false};
    std::atomic<bool> entered_{false};
    std::atomic<bool> republish_returned_{false};
    std::mutex gate_mutex_;
    std::condition_variable gate_condition_;
    bool released_ = false;
};

void CheckSerializedAsyncPublish(
    const mdl::MDLMessagePtr& sample,
    TestContext* test) {
    // This runtime generates only CFFEX messages, so the SH subscription below
    // can be reached only by the two explicit publications in this test.
    mock::Config config = SingleInstrumentConfig(
        0x72657669657702ULL, mdl::MDLSID_MDL_CFFEXL2, "IFMOCK01");
    mdl::IOManagerPtr manager = mock::CreateIOManager(config);
    ReentrantSerializedHandler handler(manager.Get());
    mdl::SubscriberPtr subscriber =
        manager->CreateSubscriber(&handler, false);
    subscriber->AddSubscription(
        sh::SHL2MarketData::ServiceID,
        sh::SHL2MarketData::ServiceVer,
        sh::SHL2MarketData::MessageID);
    const char* const connect_result = subscriber->Connect();
    const bool connected =
        connect_result != nullptr && connect_result[0] == '\0';
    test->Expect(
        connected,
        "serialized explicit-publication subscriber connects");
    if (!connected || sample.IsNull()) {
        handler.Release();
        manager->Shutdown();
        return;
    }

    std::atomic<bool> publisher_returned{false};
    std::thread publisher([&manager, &sample, &publisher_returned] {
        manager->AsyncPublish(sample.Get());
        publisher_returned.store(true, std::memory_order_release);
    });

    const bool entered =
        WaitFor([&handler] { return handler.entered(); });
    const bool republish_returned =
        WaitFor([&handler] { return handler.republish_returned(); });
    const bool nonblocking = WaitFor(
        [&publisher_returned] {
            return publisher_returned.load(std::memory_order_acquire);
        },
        std::chrono::milliseconds(250));
    test->Expect(entered, "serialized asynchronous callback starts");
    test->Expect(
        republish_returned,
        "AsyncPublish called inside a serialized callback returns");
    test->Expect(
        nonblocking,
        "owner AsyncPublish returns while its queued callback is blocked");
    test->Expect(
        handler.max_depth() == 1,
        "reentrant AsyncPublish never recursively invokes the handler");

    handler.Release();
    publisher.join();
    test->Expect(
        WaitFor([&handler] { return handler.callbacks() >= 2U; }),
        "the asynchronously republished message is delivered later");
    manager->Shutdown();
}

class CrossSyncController;

class CrossSyncHandler final : public mdl::MessageHandlerBase {
public:
    CrossSyncHandler(CrossSyncController* controller,
                     std::string outer_identifier)
        : controller_(controller),
          outer_identifier_(std::move(outer_identifier)) {}

    void OnMessage(mdl::Subscriber*,
                   const mdl::MDLMessage* message) override;

private:
    CrossSyncController* controller_;
    std::string outer_identifier_;
};

class CrossSyncController {
public:
    CrossSyncController(
        mdl::IOManager* manager,
        const mdl::MDLMessagePtr& nested_message)
        : manager_(manager),
          nested_message_(nested_message) {}

    void OnMessage(
        const std::string& outer_identifier,
        const mdl::MDLMessage* message) {
        const std::string identifier = Identifier(message);
        if (identifier == "600001") {
            nested_callbacks_.fetch_add(1, std::memory_order_acq_rel);
            return;
        }
        if (identifier != outer_identifier) {
            malformed_.store(true, std::memory_order_release);
            return;
        }

        if (!cross_phase_.load(std::memory_order_acquire)) {
            ProbeSerialization(outer_identifier);
            return;
        }

        const std::uint64_t before =
            nested_callbacks_.load(std::memory_order_acquire);
        manager_->SyncPublish(nested_message_.Get());
        const std::uint64_t after =
            nested_callbacks_.load(std::memory_order_acquire);
        if (after == before + 2U) {
            completed_sync_publishes_.fetch_add(
                1, std::memory_order_acq_rel);
        } else {
            completion_errors_.fetch_add(1, std::memory_order_acq_rel);
        }
    }

    bool WaitForProbeA() {
        std::unique_lock<std::mutex> lock(probe_mutex_);
        return probe_condition_.wait_for(
            lock, kTimeout, [this] { return probe_a_active_; });
    }

    void BeginCrossPhase() {
        cross_phase_.store(true, std::memory_order_release);
    }

    bool overlap_detected() const {
        std::lock_guard<std::mutex> lock(probe_mutex_);
        return probe_overlap_;
    }

    std::uint64_t nested_callbacks() const {
        return nested_callbacks_.load(std::memory_order_acquire);
    }

    std::uint64_t completed_sync_publishes() const {
        return completed_sync_publishes_.load(std::memory_order_acquire);
    }

    std::uint64_t completion_errors() const {
        return completion_errors_.load(std::memory_order_acquire);
    }

    bool malformed() const {
        return malformed_.load(std::memory_order_acquire);
    }

private:
    static std::string Identifier(const mdl::MDLMessage* message) {
        if (message == nullptr || message->GetHead() == nullptr ||
            message->GetBody() == nullptr) {
            return std::string();
        }
        const mdl::MDLMessageHead* const head = message->GetHead();
        if (head->ServiceID != sh::SHL2MarketData::ServiceID ||
            head->ServiceVersion != sh::SHL2MarketData::ServiceVer ||
            head->MessageID != sh::SHL2MarketData::MessageID ||
            head->MessageSize < head->HeadSize ||
            static_cast<std::size_t>(
                head->MessageSize - head->HeadSize) <
                sizeof(sh::SHL2MarketData)) {
            return std::string();
        }
        const sh::SHL2MarketData* const body =
            reinterpret_cast<const sh::SHL2MarketData*>(
                message->GetBody());
        return body->SecurityID.std_str();
    }

    void ProbeSerialization(const std::string& outer_identifier) {
        std::unique_lock<std::mutex> lock(probe_mutex_);
        if (outer_identifier == "600011") {
            probe_a_active_ = true;
            probe_condition_.notify_all();
            (void)probe_condition_.wait_for(
                lock,
                std::chrono::milliseconds(250),
                [this] { return probe_b_entered_; });
            probe_a_active_ = false;
            probe_condition_.notify_all();
            return;
        }
        probe_b_entered_ = true;
        if (probe_a_active_) {
            probe_overlap_ = true;
        }
        probe_condition_.notify_all();
    }

    mdl::IOManager* manager_;
    mdl::MDLMessagePtr nested_message_;
    std::atomic<bool> cross_phase_{false};
    mutable std::mutex probe_mutex_;
    std::condition_variable probe_condition_;
    bool probe_a_active_ = false;
    bool probe_b_entered_ = false;
    bool probe_overlap_ = false;
    std::atomic<std::uint64_t> nested_callbacks_{0};
    std::atomic<std::uint64_t> completed_sync_publishes_{0};
    std::atomic<std::uint64_t> completion_errors_{0};
    std::atomic<bool> malformed_{false};
};

void CrossSyncHandler::OnMessage(
    mdl::Subscriber*,
    const mdl::MDLMessage* message) {
    controller_->OnMessage(outer_identifier_, message);
}

void AddCrossSyncSubscriptions(
    mdl::Subscriber* subscriber,
    const char* outer_identifier) {
    const char* identifiers[] = {
        outer_identifier, "600001"};
    subscriber->AddSubscriptionByFieldValues(
        sh::SHL2MarketData::ServiceID,
        sh::SHL2MarketData::ServiceVer,
        sh::SHL2MarketData::MessageID,
        "SecurityID",
        identifiers,
        2);
}

void CheckCrossSubscriberSyncPublish(
    const mdl::MDLMessagePtr& nested_message,
    TestContext* test) {
    const mdl::MDLMessagePtr outer_a = CaptureShanghaiMessageFor(
        "600011", 0x72657669657711ULL, test);
    const mdl::MDLMessagePtr outer_b = CaptureShanghaiMessageFor(
        "600012", 0x72657669657712ULL, test);
    if (nested_message.IsNull() ||
        outer_a.IsNull() ||
        outer_b.IsNull()) {
        test->Expect(false, "cross-publish messages are available");
        return;
    }

    // The runtime itself generates only CFFEX messages.  Every SH callback in
    // this scenario therefore comes from one of the explicit publications.
    mock::Config config = SingleInstrumentConfig(
        0x72657669657713ULL, mdl::MDLSID_MDL_CFFEXL2, "IFMOCK13");
    config.callback_threads = 2;
    mdl::IOManagerPtr manager = mock::CreateIOManager(config);
    CrossSyncController controller(manager.Get(), nested_message);
    CrossSyncHandler handler_a(&controller, "600011");
    CrossSyncHandler handler_b(&controller, "600012");
    mdl::SubscriberPtr subscriber_a =
        manager->CreateSubscriber(&handler_a, false);
    mdl::SubscriberPtr subscriber_b =
        manager->CreateSubscriber(&handler_b, false);
    AddCrossSyncSubscriptions(subscriber_a.Get(), "600011");
    AddCrossSyncSubscriptions(subscriber_b.Get(), "600012");
    const char* const connect_a = subscriber_a->Connect();
    const char* const connect_b = subscriber_b->Connect();
    const bool connected =
        connect_a != nullptr && connect_a[0] == '\0' &&
        connect_b != nullptr && connect_b[0] == '\0';
    test->Expect(connected, "two serialized cross-publish subscribers connect");
    if (!connected) {
        manager->Shutdown();
        return;
    }

    // First prove that an outer callback for B cannot enter while A is active.
    // Unlike a two-party callback barrier, A has a bounded timeout and can
    // always complete under Runtime-wide serialization.  The pre-fix
    // per-subscriber locks let B enter, which is detected without attempting
    // the known-deadlocking cross publication.
    std::thread probe_a(
        [&manager, &outer_a] { manager->SyncPublish(outer_a.Get()); });
    const bool probe_a_entered = controller.WaitForProbeA();
    test->Expect(
        probe_a_entered,
        "first serialized subscriber enters the bounded overlap probe");
    std::thread probe_b(
        [&manager, &outer_b] { manager->SyncPublish(outer_b.Get()); });
    probe_a.join();
    probe_b.join();
    const bool overlap = controller.overlap_detected();
    test->Expect(
        !overlap,
        "serialized subscribers share a Runtime-wide callback gate");
    if (overlap) {
        manager->Shutdown();
        return;
    }

    controller.BeginCrossPhase();
    std::atomic<unsigned int> publishers_returned{0};
    std::thread publisher_a(
        [&manager, &outer_a, &publishers_returned] {
            manager->SyncPublish(outer_a.Get());
            publishers_returned.fetch_add(1, std::memory_order_acq_rel);
        });
    std::thread publisher_b(
        [&manager, &outer_b, &publishers_returned] {
            manager->SyncPublish(outer_b.Get());
            publishers_returned.fetch_add(1, std::memory_order_acq_rel);
        });
    const bool both_returned = WaitFor(
        [&publishers_returned] {
            return publishers_returned.load(std::memory_order_acquire) == 2U;
        },
        std::chrono::seconds(1));
    test->Expect(
        both_returned,
        "cross-subscriber SyncPublish calls return without ABBA deadlock");
    // The overlap probe above prevents the known pre-fix deadlock path, so
    // these joins remain bounded for both the fixed and reviewed old designs.
    publisher_a.join();
    publisher_b.join();

    test->Expect(
        controller.completed_sync_publishes() == 2U &&
            controller.completion_errors() == 0U,
        "each SyncPublish returns only after both nested callbacks complete");
    test->Expect(
        controller.nested_callbacks() == 4U,
        "two cross publications synchronously deliver four nested callbacks");
    test->Expect(
        !controller.malformed(),
        "cross-publish callbacks retain their expected identifiers");
    manager->Shutdown();
}

class RestartGateHandler final : public mdl::MessageHandlerBase {
public:
    void OnMessage(mdl::Subscriber*,
                   const mdl::MDLMessage*) override {
        const std::uint64_t callback_number =
            callbacks_.fetch_add(1, std::memory_order_acq_rel) + 1U;
        if (restarted_.load(std::memory_order_acquire)) {
            post_restart_callbacks_.fetch_add(
                1, std::memory_order_acq_rel);
        }
        if (callback_number != 1U) {
            return;
        }
        {
            std::lock_guard<std::mutex> lock(gate_mutex_);
            first_entered_ = true;
        }
        gate_condition_.notify_all();
        std::unique_lock<std::mutex> lock(gate_mutex_);
        (void)gate_condition_.wait_for(
            lock, kTimeout, [this] { return released_; });
    }

    bool WaitForFirst() {
        std::unique_lock<std::mutex> lock(gate_mutex_);
        return gate_condition_.wait_for(
            lock, kTimeout, [this] { return first_entered_; });
    }

    void MarkRestarted() {
        restarted_.store(true, std::memory_order_release);
    }

    void Release() {
        {
            std::lock_guard<std::mutex> lock(gate_mutex_);
            released_ = true;
        }
        gate_condition_.notify_all();
    }

    std::uint64_t post_restart_callbacks() const {
        return post_restart_callbacks_.load(std::memory_order_acquire);
    }

private:
    std::atomic<std::uint64_t> callbacks_{0};
    std::atomic<std::uint64_t> post_restart_callbacks_{0};
    std::atomic<bool> restarted_{false};
    std::mutex gate_mutex_;
    std::condition_variable gate_condition_;
    bool first_entered_ = false;
    bool released_ = false;
};

void CheckAutoSubscriberRestartGeneration(TestContext* test) {
    mock::Config config = SingleInstrumentConfig(
        0x72657669657703ULL, mdl::MDLSID_MDL_SHL2, "600003");
    config.messages_per_second = 0;
    config.callback_threads = 1;
    config.callback_queue_capacity = 8;
    RestartGateHandler handler;
    mdl::IOManagerPtr manager = mock::CreateIOManager(config);
    mdl::AutoSubscriberPtr automatic =
        manager->CreateAutoSubscriber(&handler, true);
    const char* const first_start = automatic->Start();
    const bool started =
        first_start != nullptr && first_start[0] == '\0';
    test->Expect(started, "restart-generation AutoSubscriber starts");
    if (!started) {
        handler.Release();
        manager->Shutdown();
        return;
    }

    const bool first_entered = handler.WaitForFirst();
    const bool backlog_full = WaitFor([&manager, &config] {
        const mock::Statistics statistics =
            mock::GetStatistics(manager.Get());
        return statistics.queue_high_watermark ==
                   config.callback_queue_capacity &&
               statistics.generated >=
                   static_cast<std::uint64_t>(
                       config.callback_queue_capacity) +
                       2U;
    });
    test->Expect(
        first_entered,
        "restart-generation callback occupies the sole worker");
    test->Expect(
        backlog_full,
        "restart-generation queue contains a complete old-session backlog");
    if (!first_entered || !backlog_full) {
        handler.Release();
        automatic->Stop();
        manager->Shutdown();
        return;
    }

    std::thread stopper([&automatic] { automatic->Stop(); });
    const bool disconnected = WaitFor([&manager] {
        return mock::GetStatistics(manager.Get()).active_subscribers == 0U;
    });
    test->Expect(
        disconnected,
        "AutoSubscriber Stop invalidates the old connection before waiting");

    const char* const restart_result = automatic->Start();
    const bool restarted =
        restart_result != nullptr && restart_result[0] == '\0';
    test->Expect(
        restarted,
        "AutoSubscriber reconnects while its old callback is winding down");
    handler.MarkRestarted();

    // The full queue keeps the generator from creating any new-session work.
    // Calling Shutdown before releasing the worker then leaves only tasks
    // captured under the old connection generation in the queue.
    std::thread releaser([&handler] {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        handler.Release();
    });
    manager->Shutdown();
    releaser.join();
    stopper.join();

    test->Expect(
        handler.post_restart_callbacks() == 0U,
        "queued tasks from before Stop are not replayed after restart");
}

class GenerationWaitHandler final : public mdl::MessageHandlerBase {
public:
    void OnMessage(mdl::Subscriber*,
                   const mdl::MDLMessage*) override {
        if (!new_generation_.load(std::memory_order_acquire)) {
            bool expected = false;
            if (!old_claimed_.compare_exchange_strong(
                    expected, true, std::memory_order_acq_rel)) {
                return;
            }
            {
                std::lock_guard<std::mutex> lock(mutex_);
                old_active_ = true;
            }
            condition_.notify_all();
            std::unique_lock<std::mutex> lock(mutex_);
            (void)condition_.wait_for(
                lock, kTimeout, [this] { return release_old_; });
            old_active_ = false;
            condition_.notify_all();
            return;
        }

        bool expected = false;
        if (!new_claimed_.compare_exchange_strong(
                expected, true, std::memory_order_acq_rel)) {
            return;
        }
        {
            std::lock_guard<std::mutex> lock(mutex_);
            new_active_ = true;
        }
        condition_.notify_all();
        std::unique_lock<std::mutex> lock(mutex_);
        (void)condition_.wait_for(
            lock, kTimeout, [this] { return release_new_; });
        new_active_ = false;
        condition_.notify_all();
    }

    void BeginNewGeneration() {
        new_generation_.store(true, std::memory_order_release);
    }

    bool WaitForOldActive() {
        std::unique_lock<std::mutex> lock(mutex_);
        return condition_.wait_for(
            lock, kTimeout, [this] { return old_active_; });
    }

    bool WaitForNewActive() {
        std::unique_lock<std::mutex> lock(mutex_);
        return condition_.wait_for(
            lock, kTimeout, [this] { return new_active_; });
    }

    void ReleaseOld() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            release_old_ = true;
        }
        condition_.notify_all();
    }

    void ReleaseNew() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            release_new_ = true;
        }
        condition_.notify_all();
    }

    bool new_active() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return new_active_;
    }

private:
    std::atomic<bool> new_generation_{false};
    std::atomic<bool> old_claimed_{false};
    std::atomic<bool> new_claimed_{false};
    mutable std::mutex mutex_;
    std::condition_variable condition_;
    bool old_active_ = false;
    bool new_active_ = false;
    bool release_old_ = false;
    bool release_new_ = false;
};

void CheckStopWaitsOnlyForDisconnectedGeneration(TestContext* test) {
    mock::Config config = SingleInstrumentConfig(
        0x72657669657705ULL, mdl::MDLSID_MDL_SHL2, "600005");
    config.messages_per_second = 0;
    config.callback_threads = 2;
    config.callback_queue_capacity = 16;
    GenerationWaitHandler handler;
    mdl::IOManagerPtr manager = mock::CreateIOManager(config);
    mdl::AutoSubscriberPtr automatic =
        manager->CreateAutoSubscriber(&handler, true);
    const char* const first_start = automatic->Start();
    const bool started =
        first_start != nullptr && first_start[0] == '\0';
    test->Expect(
        started,
        "generation-specific Stop AutoSubscriber starts");
    if (!started) {
        handler.ReleaseOld();
        handler.ReleaseNew();
        manager->Shutdown();
        return;
    }

    const bool old_active = handler.WaitForOldActive();
    test->Expect(
        old_active,
        "an old-generation callback remains in flight");
    if (!old_active) {
        handler.ReleaseOld();
        handler.ReleaseNew();
        automatic->Stop();
        manager->Shutdown();
        return;
    }

    std::atomic<bool> stop_returned{false};
    std::thread stopper([&automatic, &stop_returned] {
        automatic->Stop();
        stop_returned.store(true, std::memory_order_release);
    });
    const bool disconnected = WaitFor([&manager] {
        return mock::GetStatistics(manager.Get()).active_subscribers == 0U;
    });
    test->Expect(
        disconnected,
        "Stop disconnects the old generation before waiting for it");

    handler.BeginNewGeneration();
    const char* const restart_result = automatic->Start();
    const bool restarted =
        restart_result != nullptr && restart_result[0] == '\0';
    test->Expect(
        restarted,
        "Start reconnects while Stop is waiting for the old generation");
    const bool new_active =
        restarted && handler.WaitForNewActive();
    test->Expect(
        new_active,
        "a new-generation callback enters before the old callback exits");

    handler.ReleaseOld();
    const bool returned_while_new_active = WaitFor(
        [&stop_returned] {
            return stop_returned.load(std::memory_order_acquire);
        },
        std::chrono::milliseconds(250));
    test->Expect(
        returned_while_new_active,
        "Stop returns after its old generation drains");
    test->Expect(
        !new_active || handler.new_active(),
        "Stop does not wait for or cancel an in-flight new-generation callback");

    // Always release the second gate and stop the runtime before joining.  This
    // bounds the failure mode where Stop incorrectly waits for all generations.
    handler.ReleaseNew();
    manager->Shutdown();
    stopper.join();
}

class TimestampHandler final : public mdl::MessageHandlerBase {
public:
    void OnMessage(mdl::Subscriber*,
                   const mdl::MDLMessage*) override {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            timestamps_.push_back(std::chrono::steady_clock::now());
        }
        condition_.notify_all();
    }

    bool WaitForCount(std::size_t count) {
        std::unique_lock<std::mutex> lock(mutex_);
        return condition_.wait_for(lock, kTimeout, [this, count] {
            return timestamps_.size() >= count;
        });
    }

    std::size_t count() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return timestamps_.size();
    }

    std::chrono::steady_clock::duration Span(
        std::size_t first,
        std::size_t last) const {
        std::lock_guard<std::mutex> lock(mutex_);
        if (last >= timestamps_.size() || first > last) {
            return std::chrono::steady_clock::duration::zero();
        }
        return timestamps_[last] - timestamps_[first];
    }

private:
    mutable std::mutex mutex_;
    std::condition_variable condition_;
    std::vector<std::chrono::steady_clock::time_point> timestamps_;
};

void CheckPacingAfterIdleFilter(TestContext* test) {
    mock::Config config = SingleInstrumentConfig(
        0x72657669657704ULL, mdl::MDLSID_MDL_SHL2, "600004");
    config.messages_per_second = 50;
    TimestampHandler handler;
    mdl::IOManagerPtr manager = mock::CreateIOManager(config);
    mdl::SubscriberPtr subscriber =
        manager->CreateSubscriber(&handler, false);

    const char* no_match[] = {"NO_MATCH"};
    subscriber->AddSubscriptionByFieldValues(
        sh::SHL2MarketData::ServiceID,
        sh::SHL2MarketData::ServiceVer,
        sh::SHL2MarketData::MessageID,
        "SecurityID",
        no_match,
        1);
    const char* const connect_result = subscriber->Connect();
    const bool connected =
        connect_result != nullptr && connect_result[0] == '\0';
    test->Expect(connected, "idle-pacing subscriber connects");
    if (!connected) {
        manager->Shutdown();
        return;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    test->Expect(
        handler.count() == 0U,
        "field filter creates an interval with no matched messages");

    const char* match[] = {"600004"};
    subscriber->AddSubscriptionByFieldValues(
        sh::SHL2MarketData::ServiceID,
        sh::SHL2MarketData::ServiceVer,
        sh::SHL2MarketData::MessageID,
        "SecurityID",
        match,
        1);
    const bool received_ten = handler.WaitForCount(10);
    test->Expect(
        received_ten,
        "matching subscription receives ten paced messages");
    if (received_ten) {
        const std::chrono::milliseconds span =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                handler.Span(0, 9));
        test->Expect(
            span >= std::chrono::milliseconds(120),
            "pacing does not repay idle time as an unpaced catch-up burst");
    }
    manager->Shutdown();
}

} // namespace

int main() {
    TestContext test;
    const mdl::MDLMessagePtr sample = CaptureShanghaiMessage(&test);
    CheckSerializedAsyncPublish(sample, &test);
    CheckCrossSubscriberSyncPublish(sample, &test);
    CheckAutoSubscriberRestartGeneration(&test);
    CheckStopWaitsOnlyForDisconnectedGeneration(&test);
    CheckPacingAfterIdleFilter(&test);

    if (test.failures != 0) {
        std::cerr << "review runtime regression test failed with "
                  << test.failures << " error(s)\n";
        return 1;
    }
    std::cout
        << "review runtime regression test passed: asynchronous serialized "
           "dispatch, cross-subscriber synchronous publication, restart "
           "generations, and idle pacing\n";
    return 0;
}
