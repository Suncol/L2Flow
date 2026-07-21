#include "l2mock/l2_mock.h"

#include "mdl_shl2_msg.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <iostream>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace mdl = datayes::mdl;
namespace mock = datayes::mdl::mock;
namespace sh = datayes::mdl::mdl_shl2_msg;

namespace {

const std::size_t kSummaryCount = 128;

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
bool WaitFor(Predicate predicate, std::chrono::milliseconds timeout) {
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

template <typename T>
bool ReadScalar(
    const char* bytes,
    std::size_t size,
    std::size_t offset,
    T* value) {
    if (bytes == nullptr || value == nullptr || offset > size ||
        sizeof(T) > size - offset) {
        return false;
    }
    std::memcpy(value, bytes + offset, sizeof(T));
    return true;
}

bool ReadAnsiString(
    const char* body,
    std::size_t body_size,
    std::size_t field_offset,
    std::string* value) {
    std::uint16_t length = 0;
    std::uint32_t relative_offset = 0;
    if (!ReadScalar(
            body,
            body_size,
            field_offset + offsetof(mdl::MDLString, Length),
            &length) ||
        !ReadScalar(
            body,
            body_size,
            field_offset + offsetof(mdl::MDLString, Offset),
            &relative_offset)) {
        return false;
    }
    if (length == 0) {
        value->clear();
        return relative_offset == 0;
    }
    if (relative_offset == 0 ||
        relative_offset >
            static_cast<std::uint64_t>(body_size - field_offset)) {
        return false;
    }
    const std::size_t data_offset =
        field_offset + static_cast<std::size_t>(relative_offset);
    if (data_offset > body_size || length > body_size - data_offset) {
        return false;
    }
    value->assign(body + data_offset, length);
    return true;
}

struct StableSummary {
    std::uint8_t service_id = 0;
    std::uint16_t service_version = 0;
    std::uint16_t message_id = 0;
    std::uint64_t sequence_id = 0;
    std::uint32_t local_time = 0;
    std::string security_id;
    std::uint32_t event_time = 0;
    std::int32_t last_price_raw = 0;
    std::int64_t traded_volume_raw = 0;
    std::uint32_t trade_count = 0;

    bool operator==(const StableSummary& other) const {
        return service_id == other.service_id &&
               service_version == other.service_version &&
               message_id == other.message_id &&
               sequence_id == other.sequence_id &&
               local_time == other.local_time &&
               security_id == other.security_id &&
               event_time == other.event_time &&
               last_price_raw == other.last_price_raw &&
               traded_volume_raw == other.traded_volume_raw &&
               trade_count == other.trade_count;
    }
};

bool ParseStableSummary(
    const mdl::MDLMessage* message,
    StableSummary* summary,
    std::string* error) {
    if (message == nullptr || message->GetHead() == nullptr ||
        message->GetBody() == nullptr) {
        *error = "null message, head, or body";
        return false;
    }

    const char* const head =
        reinterpret_cast<const char*>(message->GetHead());
    std::uint8_t head_size = 0;
    std::uint32_t message_size = 0;
    std::uint8_t encoding = 0;
    if (!ReadScalar(
            head,
            sizeof(mdl::MDLMessageHead),
            offsetof(mdl::MDLMessageHead, HeadSize),
            &head_size) ||
        !ReadScalar(
            head,
            sizeof(mdl::MDLMessageHead),
            offsetof(mdl::MDLMessageHead, MessageSize),
            &message_size) ||
        !ReadScalar(
            head,
            sizeof(mdl::MDLMessageHead),
            offsetof(mdl::MDLMessageHead, MessageEncoding),
            &encoding) ||
        !ReadScalar(
            head,
            sizeof(mdl::MDLMessageHead),
            offsetof(mdl::MDLMessageHead, ServiceID),
            &summary->service_id) ||
        !ReadScalar(
            head,
            sizeof(mdl::MDLMessageHead),
            offsetof(mdl::MDLMessageHead, ServiceVersion),
            &summary->service_version) ||
        !ReadScalar(
            head,
            sizeof(mdl::MDLMessageHead),
            offsetof(mdl::MDLMessageHead, MessageID),
            &summary->message_id) ||
        !ReadScalar(
            head,
            sizeof(mdl::MDLMessageHead),
            offsetof(mdl::MDLMessageHead, LocalTime),
            &summary->local_time) ||
        !ReadScalar(
            head,
            sizeof(mdl::MDLMessageHead),
            offsetof(mdl::MDLMessageHead, SequenceID),
            &summary->sequence_id)) {
        *error = "header scalar read is out of range";
        return false;
    }
    if (head_size != sizeof(mdl::MDLMessageHead) ||
        message_size < head_size ||
        encoding != mdl::MDLEID_BINARY) {
        *error = "invalid binary message header";
        return false;
    }
    if (summary->service_id != sh::SHL2MarketData::ServiceID ||
        summary->service_version != sh::SHL2MarketData::ServiceVer ||
        summary->message_id != sh::SHL2MarketData::MessageID) {
        *error = "unexpected message key in deterministic capture";
        return false;
    }

    const std::size_t body_size =
        static_cast<std::size_t>(message_size - head_size);
    if (body_size != message->GetBodySize() ||
        body_size < sizeof(sh::SHL2MarketData)) {
        *error = "body size does not match SHL2MarketData";
        return false;
    }
    const char* const body = message->GetBody();
    if (!ReadScalar(
            body,
            body_size,
            offsetof(sh::SHL2MarketData, UpdateTime),
            &summary->event_time) ||
        !ReadAnsiString(
            body,
            body_size,
            offsetof(sh::SHL2MarketData, SecurityID),
            &summary->security_id) ||
        !ReadScalar(
            body,
            body_size,
            offsetof(sh::SHL2MarketData, LastPrice),
            &summary->last_price_raw) ||
        !ReadScalar(
            body,
            body_size,
            offsetof(sh::SHL2MarketData, TradVolume),
            &summary->traded_volume_raw) ||
        !ReadScalar(
            body,
            body_size,
            offsetof(sh::SHL2MarketData, TradNumber),
            &summary->trade_count)) {
        *error = "SHL2MarketData scalar/string read is out of range";
        return false;
    }
    if (summary->security_id.empty()) {
        *error = "SHL2MarketData SecurityID is empty";
        return false;
    }
    mdl::MDLTime event_time;
    event_time.m_Value = summary->event_time;
    mdl::MDLTime local_time;
    local_time.m_Value = summary->local_time;
    if (!event_time.IsValid() || !local_time.IsValid()) {
        *error = "captured event or local time is invalid";
        return false;
    }
    return true;
}

class SummaryHandler final : public mdl::MessageHandler {
public:
    explicit SummaryHandler(std::size_t target) : target_(target) {}

    void OnMDLSHL2Message(const mdl::MDLMessage* message) override {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (summaries_.size() >= target_ || !error_.empty()) {
                return;
            }
        }

        StableSummary summary;
        std::string parse_error;
        const bool parsed =
            ParseStableSummary(message, &summary, &parse_error);

        std::lock_guard<std::mutex> lock(mutex_);
        if (!parsed) {
            error_ = parse_error;
        } else if (summaries_.size() < target_) {
            summaries_.push_back(summary);
        }
        condition_.notify_all();
    }

    bool Wait(std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> lock(mutex_);
        return condition_.wait_for(lock, timeout, [this] {
            return summaries_.size() >= target_ || !error_.empty();
        });
    }

    std::vector<StableSummary> summaries() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return summaries_;
    }

    std::string error() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return error_;
    }

private:
    const std::size_t target_;
    mutable std::mutex mutex_;
    std::condition_variable condition_;
    std::vector<StableSummary> summaries_;
    std::string error_;
};

mock::Instrument MakeShanghaiInstrument(
    const std::string& security_id,
    std::int64_t reference_price_milli) {
    mock::Instrument instrument;
    instrument.service_id = mdl::MDLSID_MDL_SHL2;
    instrument.security_id = security_id;
    instrument.reference_price_milli = reference_price_milli;
    instrument.tick_size_milli = 10;
    instrument.lot_size = 100;
    instrument.option = false;
    return instrument;
}

bool CaptureDeterministicRun(
    std::uint64_t seed,
    std::vector<StableSummary>* summaries,
    std::string* error) {
    try {
        mock::Config config;
        config.seed = seed;
        config.messages_per_second = 0;
        config.book_depth = 3;
        config.orders_per_level = 2;
        // The callback worker pool must remain valid even though this
        // subscriber deliberately selects the direct callback path.
        config.callback_threads = 1;
        config.callback_queue_capacity = 8;
        config.backpressure = mock::BackpressurePolicy::Block;
        config.clock_mode = mock::ClockMode::SimulatedTradingDay;
        config.simulated_start_time = 93000000;
        config.simulated_min_step_ms = 0;
        config.simulated_max_step_ms = 3;
        config.instruments.push_back(
            MakeShanghaiInstrument("MOCK01", 100000));
        config.instruments.push_back(
            MakeShanghaiInstrument("MOCK02", 125000));

        const std::string validation_error = mock::ValidateConfig(config);
        if (!validation_error.empty()) {
            *error = "deterministic config rejected: " + validation_error;
            return false;
        }

        SummaryHandler handler(kSummaryCount);
        mdl::IOManagerPtr manager = mock::CreateIOManager(config);
        if (manager.IsNull()) {
            *error = "CreateIOManager returned null";
            return false;
        }
        mdl::SubscriberPtr subscriber =
            manager->CreateSubscriber(&handler, false);
        if (subscriber.IsNull()) {
            manager->Shutdown();
            *error = "CreateSubscriber returned null";
            return false;
        }
        subscriber->SubcribeMessage<sh::SHL2MarketData>();
        const char* const connect_error = subscriber->Connect();
        if (connect_error != nullptr && connect_error[0] != '\0') {
            manager->Shutdown();
            *error = std::string("Connect failed: ") + connect_error;
            return false;
        }

        const bool completed =
            handler.Wait(std::chrono::seconds(10));
        manager->Shutdown();
        if (!handler.error().empty()) {
            *error = handler.error();
            return false;
        }
        if (!completed) {
            *error = "timed out waiting for deterministic summaries";
            return false;
        }
        *summaries = handler.summaries();
        if (summaries->size() != kSummaryCount) {
            std::ostringstream text;
            text << "expected " << kSummaryCount << " summaries, captured "
                 << summaries->size();
            *error = text.str();
            return false;
        }
        return true;
    } catch (const std::exception& exception) {
        *error = std::string("deterministic run threw: ") + exception.what();
        return false;
    } catch (...) {
        *error = "deterministic run threw a non-standard exception";
        return false;
    }
}

class GatedCountingHandler final : public mdl::MessageHandler {
public:
    void OnMDLSHL2Message(const mdl::MDLMessage* message) override {
        Handle(message);
    }

    void OnMDLSZL2Message(const mdl::MDLMessage* message) override {
        Handle(message);
    }

    void OnMDLCFFEXL2Message(const mdl::MDLMessage* message) override {
        Handle(message);
    }

    void OnMDLSHFEL2Message(const mdl::MDLMessage* message) override {
        Handle(message);
    }

    void OnMDLCZCEL2Message(const mdl::MDLMessage* message) override {
        Handle(message);
    }

    void OnMDLDCEL2Message(const mdl::MDLMessage* message) override {
        Handle(message);
    }

    void OnMDLGFEXL2Message(const mdl::MDLMessage* message) override {
        Handle(message);
    }

    bool WaitForEntered(
        std::uint64_t target,
        std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> lock(mutex_);
        return condition_.wait_for(lock, timeout, [this, target] {
            return callbacks_ >= target;
        });
    }

    void Release() {
        std::lock_guard<std::mutex> lock(mutex_);
        released_ = true;
        condition_.notify_all();
    }

    std::uint64_t callbacks() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return callbacks_;
    }

    std::uint64_t malformed() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return malformed_;
    }

    std::size_t callback_thread_count() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return callback_threads_.size();
    }

private:
    void Handle(const mdl::MDLMessage* message) {
        std::unique_lock<std::mutex> lock(mutex_);
        ++callbacks_;
        callback_threads_.insert(std::this_thread::get_id());
        if (message == nullptr || message->GetHead() == nullptr ||
            message->GetBody() == nullptr) {
            ++malformed_;
        }
        condition_.notify_all();
        condition_.wait(lock, [this] { return released_; });
    }

    mutable std::mutex mutex_;
    std::condition_variable condition_;
    bool released_ = false;
    std::uint64_t callbacks_ = 0;
    std::uint64_t malformed_ = 0;
    std::set<std::thread::id> callback_threads_;
};

class CallbackShutdownHandler final : public mdl::MessageHandler {
public:
    void SetManager(mdl::IOManager* manager) {
        manager_.store(manager, std::memory_order_release);
    }

    void OnMDLSHL2Message(const mdl::MDLMessage* message) override {
        Handle(message);
    }

    void OnMDLSZL2Message(const mdl::MDLMessage* message) override {
        Handle(message);
    }

    void OnMDLCFFEXL2Message(const mdl::MDLMessage* message) override {
        Handle(message);
    }

    void OnMDLSHFEL2Message(const mdl::MDLMessage* message) override {
        Handle(message);
    }

    void OnMDLCZCEL2Message(const mdl::MDLMessage* message) override {
        Handle(message);
    }

    void OnMDLDCEL2Message(const mdl::MDLMessage* message) override {
        Handle(message);
    }

    void OnMDLGFEXL2Message(const mdl::MDLMessage* message) override {
        Handle(message);
    }

    bool WaitForShutdownReturn(std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> lock(mutex_);
        return condition_.wait_for(lock, timeout, [this] {
            return shutdown_returned_;
        });
    }

    std::uint64_t callbacks() const {
        return callbacks_.load(std::memory_order_acquire);
    }

    std::uint64_t malformed() const {
        return malformed_.load(std::memory_order_acquire);
    }

private:
    void Handle(const mdl::MDLMessage* message) {
        callbacks_.fetch_add(1, std::memory_order_acq_rel);
        if (message == nullptr || message->GetHead() == nullptr ||
            message->GetBody() == nullptr) {
            malformed_.fetch_add(1, std::memory_order_acq_rel);
        }

        bool expected = false;
        if (!shutdown_called_.compare_exchange_strong(
                expected, true, std::memory_order_acq_rel)) {
            return;
        }
        mdl::IOManager* const manager =
            manager_.load(std::memory_order_acquire);
        if (manager != nullptr) {
            manager->Shutdown();
        }
        {
            std::lock_guard<std::mutex> lock(mutex_);
            shutdown_returned_ = true;
        }
        condition_.notify_all();
    }

    // The owning test retains an IOManagerPtr through the callback, the
    // coordinator teardown, and the final owner-thread Shutdown call.
    std::atomic<mdl::IOManager*> manager_{nullptr};
    std::atomic<bool> shutdown_called_{false};
    std::atomic<std::uint64_t> callbacks_{0};
    std::atomic<std::uint64_t> malformed_{0};
    mutable std::mutex mutex_;
    std::condition_variable condition_;
    bool shutdown_returned_ = false;
};

const char* PolicyName(mock::BackpressurePolicy policy) {
    return policy == mock::BackpressurePolicy::DropNewest
               ? "DropNewest"
               : "Block";
}

void RunBackpressureStress(
    mock::BackpressurePolicy policy,
    TestContext* test) {
    const std::uint32_t callback_threads = 4;
    const std::uint32_t queue_capacity = 2;
    const std::string label = PolicyName(policy);

    mock::Config config;
    config.seed =
        policy == mock::BackpressurePolicy::DropNewest
            ? 0xd09a5eedULL
            : 0xb10c5eedULL;
    config.messages_per_second = 0;
    config.shanghai_instruments = 32;
    config.shenzhen_instruments = 32;
    config.derivatives_per_market = 4;
    config.include_derivatives = true;
    config.book_depth = 3;
    config.orders_per_level = 2;
    config.callback_threads = callback_threads;
    config.callback_queue_capacity = queue_capacity;
    config.backpressure = policy;
    config.clock_mode = mock::ClockMode::SimulatedTradingDay;
    config.simulated_min_step_ms = 0;
    config.simulated_max_step_ms = 3;

    const std::string validation_error = mock::ValidateConfig(config);
    test->Expect(
        validation_error.empty(),
        label + " stress config passes ValidateConfig");
    if (!validation_error.empty()) {
        return;
    }

    GatedCountingHandler handler;
    mdl::IOManagerPtr manager;
    mdl::SubscriberPtr subscriber;
    try {
        manager = mock::CreateIOManager(config);
        subscriber = manager->CreateSubscriber(&handler, true);
        mock::SubscribeAll(subscriber.Get());
        const char* const connect_error = subscriber->Connect();
        test->Expect(
            connect_error == nullptr || connect_error[0] == '\0',
            label + " stress subscriber connects");
        if (connect_error != nullptr && connect_error[0] != '\0') {
            handler.Release();
            manager->Shutdown();
            return;
        }
    } catch (const std::exception& exception) {
        test->Expect(
            false,
            label + " setup threw: " + std::string(exception.what()));
        handler.Release();
        if (!manager.IsNull()) {
            manager->Shutdown();
        }
        return;
    }

    const bool occupied_all_workers =
        handler.WaitForEntered(callback_threads, std::chrono::seconds(10));
    test->Expect(
        occupied_all_workers,
        label + " dispatches callbacks on all configured workers");

    const bool pressure_observed = WaitFor(
        [&manager, policy, queue_capacity] {
            const mock::Statistics statistics =
                mock::GetStatistics(manager.Get());
            if (statistics.queue_high_watermark < queue_capacity) {
                return false;
            }
            return policy != mock::BackpressurePolicy::DropNewest ||
                   statistics.dropped != 0;
        },
        std::chrono::seconds(10));
    test->Expect(
        pressure_observed,
        label + " reaches the bounded queue pressure condition");

    handler.Release();
    const std::chrono::steady_clock::time_point shutdown_start =
        std::chrono::steady_clock::now();
    manager->Shutdown();
    const std::chrono::steady_clock::duration shutdown_elapsed =
        std::chrono::steady_clock::now() - shutdown_start;

    const mock::Statistics statistics =
        mock::GetStatistics(manager.Get());
    const std::uint64_t accounted =
        statistics.delivered + statistics.dropped;

    test->Expect(
        shutdown_elapsed < std::chrono::seconds(5),
        label + " Shutdown completes within the liveness bound");
    test->Expect(
        statistics.generated != 0,
        label + " generated statistic is nonzero");
    test->Expect(
        statistics.generated >= accounted &&
            statistics.generated - accounted <= 1,
        label +
            " generated messages are delivered/dropped, except at most one "
            "enqueue interrupted by Shutdown");
    test->Expect(
        statistics.delivered == handler.callbacks(),
        label + " delivered statistic equals observed callbacks");
    test->Expect(
        statistics.filtered == 0,
        label + " SubscribeAll leaves no generated message filtered");
    test->Expect(
        statistics.callback_errors == 0,
        label + " has no callback or generator errors");
    test->Expect(
        statistics.queue_high_watermark >= 1 &&
            statistics.queue_high_watermark <= queue_capacity,
        label + " queue high watermark stays within capacity");
    test->Expect(
        statistics.active_subscribers == 0,
        label + " Shutdown clears active subscribers");
    test->Expect(
        handler.malformed() == 0,
        label + " callbacks receive non-null messages");
    test->Expect(
        handler.callback_thread_count() >= 2 &&
            handler.callback_thread_count() <= callback_threads,
        label + " uses multiple callback worker threads");
    if (policy == mock::BackpressurePolicy::DropNewest) {
        test->Expect(
            statistics.dropped != 0,
            "DropNewest counts messages rejected by the full queue");
    } else {
        test->Expect(
            statistics.dropped == 0,
            "Block applies backpressure without dropping messages");
    }
}

void RunCallbackInitiatedShutdown(
    bool multithread_callback,
    TestContext* test) {
    const std::string label =
        multithread_callback ? "multithread callback" : "direct callback";

    mock::Config config;
    config.seed =
        multithread_callback ? 0xca11bac2ULL : 0xca11bac1ULL;
    config.messages_per_second = 0;
    config.book_depth = 3;
    config.orders_per_level = 2;
    config.callback_threads = multithread_callback ? 4 : 1;
    config.callback_queue_capacity = 8;
    config.backpressure = mock::BackpressurePolicy::Block;
    config.clock_mode = mock::ClockMode::SimulatedTradingDay;
    config.simulated_min_step_ms = 1;
    config.simulated_max_step_ms = 1;
    config.instruments.push_back(
        MakeShanghaiInstrument("STOP01", 100000));

    const std::string validation_error = mock::ValidateConfig(config);
    test->Expect(
        validation_error.empty(),
        label + " self-shutdown config passes ValidateConfig");
    if (!validation_error.empty()) {
        return;
    }

    CallbackShutdownHandler handler;
    mdl::IOManagerPtr manager;
    mdl::SubscriberPtr subscriber;
    try {
        manager = mock::CreateIOManager(config);
        // This strong owner makes the raw pointer installed in the handler
        // valid until the owner-thread synchronization below completes.
        handler.SetManager(manager.Get());
        subscriber =
            manager->CreateSubscriber(&handler, multithread_callback);
        mock::SubscribeAll(subscriber.Get());
        const char* const connect_error = subscriber->Connect();
        test->Expect(
            connect_error == nullptr || connect_error[0] == '\0',
            label + " self-shutdown subscriber connects");
        if (connect_error != nullptr && connect_error[0] != '\0') {
            manager->Shutdown();
            return;
        }
    } catch (const std::exception& exception) {
        test->Expect(
            false,
            label + " self-shutdown setup threw: " +
                std::string(exception.what()));
        if (!manager.IsNull()) {
            manager->Shutdown();
        }
        return;
    }

    const bool callback_shutdown_returned =
        handler.WaitForShutdownReturn(std::chrono::seconds(10));
    test->Expect(
        callback_shutdown_returned,
        label + " IOManager::Shutdown returns inside the first callback");

    const std::chrono::steady_clock::time_point owner_shutdown_start =
        std::chrono::steady_clock::now();
    // The callback-side call may hand work to a coordinator.  The owner-side
    // repeat call is the synchronization point for complete teardown.
    manager->Shutdown();
    const std::chrono::steady_clock::duration owner_shutdown_elapsed =
        std::chrono::steady_clock::now() - owner_shutdown_start;

    const mock::Statistics statistics =
        mock::GetStatistics(manager.Get());
    const std::uint64_t callbacks_after_shutdown = handler.callbacks();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    test->Expect(
        owner_shutdown_elapsed < std::chrono::seconds(5),
        label + " owner-thread Shutdown synchronizes within the bound");
    test->Expect(
        callbacks_after_shutdown >= 1,
        label + " observes at least the callback that requested Shutdown");
    test->Expect(
        handler.callbacks() == callbacks_after_shutdown,
        label + " callback count stops after owner-thread Shutdown");
    test->Expect(
        handler.malformed() == 0,
        label + " self-shutdown callback receives a valid message");
    test->Expect(
        statistics.delivered == callbacks_after_shutdown,
        label + " delivered statistic equals callbacks after teardown");
    test->Expect(
        statistics.callback_errors == 0,
        label + " self-shutdown records no callback/runtime errors");
    test->Expect(
        statistics.active_subscribers == 0,
        label + " self-shutdown clears active subscribers");
}

} // namespace

int main() {
    TestContext test;

    std::vector<StableSummary> first;
    std::vector<StableSummary> repeated;
    std::vector<StableSummary> changed_seed;
    std::string first_error;
    std::string repeated_error;
    std::string changed_error;

    const bool first_ok =
        CaptureDeterministicRun(0x5eed1234ULL, &first, &first_error);
    const bool repeated_ok =
        CaptureDeterministicRun(0x5eed1234ULL, &repeated, &repeated_error);
    const bool changed_ok =
        CaptureDeterministicRun(0x5eed1235ULL, &changed_seed, &changed_error);

    test.Expect(
        first_ok,
        "first deterministic run succeeds" +
            (first_error.empty() ? std::string()
                                 : std::string(": ") + first_error));
    test.Expect(
        repeated_ok,
        "repeated deterministic run succeeds" +
            (repeated_error.empty() ? std::string()
                                    : std::string(": ") + repeated_error));
    test.Expect(
        changed_ok,
        "different-seed deterministic run succeeds" +
            (changed_error.empty() ? std::string()
                                   : std::string(": ") + changed_error));

    if (first_ok && repeated_ok) {
        test.Expect(
            first == repeated,
            "same simulated config and seed yield identical stable summaries");
    }
    if (first_ok && changed_ok) {
        test.Expect(
            first != changed_seed,
            "changing the seed changes at least one stable summary field");
    }

    RunBackpressureStress(mock::BackpressurePolicy::DropNewest, &test);
    RunBackpressureStress(mock::BackpressurePolicy::Block, &test);
    RunCallbackInitiatedShutdown(false, &test);
    RunCallbackInitiatedShutdown(true, &test);

    if (test.failures != 0) {
        std::cerr << "determinism/stress test failed with " << test.failures
                  << " error(s)\n";
        return 1;
    }

    std::cout
        << "determinism/stress test passed: " << kSummaryCount
        << " stable records, seed variation, both backpressure policies, "
           "and callback-initiated shutdown\n";
    return 0;
}
