#include "l2flow/runtime/realtime_pipeline_v1.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace {

namespace market = l2flow::market;
namespace mdl = datayes::mdl;
namespace runtime = l2flow::runtime;
namespace sdk = l2flow::sdk;

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

class OneShotGate final {
public:
    void EnterAndWait() const {
        std::unique_lock<std::mutex> lock(mutex_);
        entered_ = true;
        condition_.notify_all();
        condition_.wait(lock, [this] { return open_; });
    }

    [[nodiscard]] bool WaitUntilEntered(
        std::chrono::milliseconds timeout) const {
        std::unique_lock<std::mutex> lock(mutex_);
        return condition_.wait_for(
            lock, timeout, [this] { return entered_; });
    }

    void Open() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            open_ = true;
        }
        condition_.notify_all();
    }

private:
    mutable std::mutex mutex_;
    mutable std::condition_variable condition_;
    mutable bool entered_ = false;
    bool open_ = false;
};

enum class LifecycleEvent : std::uint8_t {
    kCallbackEnter = 0U,
    kCallbackExit,
    kShutdownBegin,
    kShutdownEnd,
    kSubscriberRelease,
    kManagerRelease,
    kTerminalReturn,
};

struct LifecycleSnapshot final {
    std::vector<LifecycleEvent> events;
    std::uint32_t shutdown_calls = 0U;
    std::uint32_t subscriber_release_calls = 0U;
    std::uint32_t manager_release_calls = 0U;
};

class LifecycleState final {
public:
    void InstallHandler(mdl::MessageHandlerBase* handler) {
        std::lock_guard<std::mutex> lock(mutex_);
        handler_ = handler;
    }

    [[nodiscard]] mdl::MessageHandlerBase* handler() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return handler_;
    }

    void CallbackEnter() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            callback_active_ = true;
            events_.push_back(LifecycleEvent::kCallbackEnter);
        }
        condition_.notify_all();
    }

    void CallbackExit() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            callback_active_ = false;
            events_.push_back(LifecycleEvent::kCallbackExit);
        }
        condition_.notify_all();
    }

    void TerminalCallStarted() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            terminal_call_started_ = true;
        }
        condition_.notify_all();
    }

    [[nodiscard]] bool WaitForTerminalCallStarted(
        std::chrono::milliseconds timeout) const {
        std::unique_lock<std::mutex> lock(mutex_);
        return condition_.wait_for(
            lock, timeout, [this] { return terminal_call_started_; });
    }

    void ShutdownAndQuiesce() {
        std::unique_lock<std::mutex> lock(mutex_);
        ++shutdown_calls_;
        events_.push_back(LifecycleEvent::kShutdownBegin);
        condition_.notify_all();
        condition_.wait(lock, [this] { return !callback_active_; });
        shutdown_quiesced_ = true;
        condition_.notify_all();
        condition_.wait(lock, [this] { return allow_shutdown_return_; });
        events_.push_back(LifecycleEvent::kShutdownEnd);
        shutdown_complete_ = true;
        condition_.notify_all();
    }

    [[nodiscard]] bool WaitForShutdownQuiesced(
        std::chrono::milliseconds timeout) const {
        std::unique_lock<std::mutex> lock(mutex_);
        return condition_.wait_for(
            lock, timeout, [this] { return shutdown_quiesced_; });
    }

    void AllowShutdownReturn() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            allow_shutdown_return_ = true;
        }
        condition_.notify_all();
    }

    [[nodiscard]] bool SubscriberRelease() {
        std::lock_guard<std::mutex> lock(mutex_);
        ++subscriber_release_calls_;
        events_.push_back(LifecycleEvent::kSubscriberRelease);
        return shutdown_complete_;
    }

    [[nodiscard]] bool ManagerRelease() {
        std::lock_guard<std::mutex> lock(mutex_);
        ++manager_release_calls_;
        events_.push_back(LifecycleEvent::kManagerRelease);
        return shutdown_complete_;
    }

    void TerminalReturn() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            events_.push_back(LifecycleEvent::kTerminalReturn);
        }
        condition_.notify_all();
    }

    [[nodiscard]] LifecycleSnapshot Snapshot() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return LifecycleSnapshot{
            events_,
            shutdown_calls_,
            subscriber_release_calls_,
            manager_release_calls_};
    }

private:
    mutable std::mutex mutex_;
    mutable std::condition_variable condition_;
    mdl::MessageHandlerBase* handler_ = nullptr;
    bool callback_active_ = false;
    bool terminal_call_started_ = false;
    bool shutdown_quiesced_ = false;
    bool allow_shutdown_return_ = false;
    bool shutdown_complete_ = false;
    std::uint32_t shutdown_calls_ = 0U;
    std::uint32_t subscriber_release_calls_ = 0U;
    std::uint32_t manager_release_calls_ = 0U;
    std::vector<LifecycleEvent> events_;
};

class RecordingSubscriber final : public sdk::SdkSubscriber {
public:
    explicit RecordingSubscriber(
        std::shared_ptr<LifecycleState> state)
        : state_(std::move(state)) {}

    void SetServerAddress(std::string_view) override {}
    void SetUserName(std::string_view) override {}
    void SetHeartbeatInterval(std::uint32_t) override {}
    void SetHeartbeatTimeout(std::uint32_t) override {}
    void SetMessageEncoding(mdl::MDLMessageEncoding encoding) override {
        binary_ = encoding == mdl::MDLEID_BINARY;
    }
    void EnableMergeMessage(bool enable) override { merge_ = enable; }
    void SetSendMacAuth(bool) override {}
    void EnableServerSelect(bool) override {}
    void AddSubscription(const sdk::MessageKey&) override {}

    [[nodiscard]] std::string Connect() override {
        return binary_ && !merge_ ? std::string{} : "invalid test setup";
    }

    [[nodiscard]] bool Release(std::string* error) noexcept override {
        bool success = true;
        if (!released_) {
            success = state_->SubscriberRelease();
            released_ = true;
        }
        if (error != nullptr) {
            if (success) {
                error->clear();
            } else {
                *error = "Subscriber released before Shutdown completed";
            }
        }
        return success;
    }

private:
    std::shared_ptr<LifecycleState> state_;
    bool binary_ = false;
    bool merge_ = true;
    bool released_ = false;
};

class RecordingManager final : public sdk::SdkManager {
public:
    explicit RecordingManager(std::shared_ptr<LifecycleState> state)
        : state_(std::move(state)) {}

    void EnableLog(std::string_view, bool) override {}

    [[nodiscard]] std::unique_ptr<sdk::SdkSubscriber> CreateSubscriber(
        mdl::MessageHandlerBase* handler,
        bool multithread_callback) override {
        if (multithread_callback) {
            return nullptr;
        }
        state_->InstallHandler(handler);
        return std::make_unique<RecordingSubscriber>(state_);
    }

    void Shutdown() override {
        state_->ShutdownAndQuiesce();
        shutdown_complete_ = true;
    }

    [[nodiscard]] bool Release(std::string* error) noexcept override {
        bool success = shutdown_complete_;
        if (!released_) {
            success = state_->ManagerRelease() && success;
            released_ = true;
        }
        if (error != nullptr) {
            if (success) {
                error->clear();
            } else {
                *error = "manager released before Shutdown completed";
            }
        }
        return success;
    }

private:
    std::shared_ptr<LifecycleState> state_;
    bool shutdown_complete_ = false;
    bool released_ = false;
};

class RecordingFactory final : public sdk::SdkFactory {
public:
    explicit RecordingFactory(std::shared_ptr<LifecycleState> state)
        : state_(std::move(state)) {}

    [[nodiscard]] std::unique_ptr<sdk::SdkManager> Create(
        int,
        int) override {
        return std::make_unique<RecordingManager>(state_);
    }

private:
    std::shared_ptr<LifecycleState> state_;
};

class WireWriter final {
public:
    explicit WireWriter(std::size_t fixed_bytes)
        : bytes_(fixed_bytes, std::byte{0U}) {}

    void StoreU16(std::size_t offset, std::uint16_t value) {
        StoreUnsigned(offset, value);
    }
    void StoreU32(std::size_t offset, std::uint32_t value) {
        StoreUnsigned(offset, value);
    }
    void StoreU64(std::size_t offset, std::uint64_t value) {
        StoreUnsigned(offset, value);
    }

    void StoreString(std::size_t descriptor, std::string_view value) {
        const std::size_t start = bytes_.size();
        StoreU16(descriptor, static_cast<std::uint16_t>(value.size()));
        StoreU32(
            descriptor + sizeof(std::uint16_t),
            value.empty()
                ? 0U
                : static_cast<std::uint32_t>(start - descriptor));
        const auto characters =
            std::span<const char>(value.data(), value.size());
        const auto encoded = std::as_bytes(characters);
        bytes_.insert(bytes_.end(), encoded.begin(), encoded.end());
    }

    [[nodiscard]] std::vector<std::byte> Take() && {
        return std::move(bytes_);
    }

private:
    template <typename Unsigned>
    void StoreUnsigned(std::size_t offset, Unsigned value) {
        static_assert(std::is_unsigned_v<Unsigned>);
        for (std::size_t index = 0U; index < sizeof(Unsigned); ++index) {
            bytes_[offset + index] = static_cast<std::byte>(
                (value >> (index * 8U)) &
                static_cast<Unsigned>(0xffU));
        }
    }

    std::vector<std::byte> bytes_;
};

std::vector<std::byte> ShenzhenOrderBody() {
    WireWriter writer(58U);
    writer.StoreU32(0U, 12U);
    writer.StoreU64(4U, 91U);
    writer.StoreU64(30U, 123'456U);
    writer.StoreU64(38U, 201U);
    writer.StoreU32(46U, 49U);
    writer.StoreU32(50U, 93'000'123U);
    writer.StoreU32(54U, 50U);
    writer.StoreString(12U, "010");
    writer.StoreString(18U, "000001");
    writer.StoreString(24U, "102 ");
    return std::move(writer).Take();
}

class BlockingMessage final : public mdl::MDLMessage {
public:
    explicit BlockingMessage(std::shared_ptr<OneShotGate> access_gate)
        : access_gate_(std::move(access_gate)),
          body_(ShenzhenOrderBody()) {
        std::memset(&head_, 0, sizeof(head_));
        head_.HeadSize = static_cast<std::uint8_t>(sdk::kVendorHeadBytes);
        head_.MessageSize = static_cast<std::uint32_t>(
            sdk::kVendorHeadBytes + body_.size());
        head_.MessageEncoding =
            static_cast<std::uint8_t>(mdl::MDLEID_BINARY);
        head_.ServiceID = 6U;
        head_.ServiceVersion = 101U;
        head_.MessageID = 33U;
        head_.LocalTime.m_Value = 93'000'000U;
        head_.SequenceID = 777U;
    }

    void AddRef() override {}
    int ReleaseRef() override { return 1; }

    mdl::MDLMessageHead* GetHead() const override {
        access_gate_->EnterAndWait();
        return const_cast<mdl::MDLMessageHead*>(&head_);
    }

    char* GetBody() const override {
        return reinterpret_cast<char*>(
            const_cast<std::byte*>(body_.data()));
    }

    mdl::MDLMessage* _Copy() const override { return nullptr; }

private:
    std::shared_ptr<OneShotGate> access_gate_;
    mdl::MDLMessageHead head_{};
    std::vector<std::byte> body_;
};

std::vector<std::byte> Bytes(std::string_view text) {
    const auto bytes = std::as_bytes(std::span(text));
    return {bytes.begin(), bytes.end()};
}

std::unique_ptr<market::InstrumentRegistryV1> MakeRegistry() {
    market::InstrumentRegistryEntryV1 entry{};
    entry.instrument_id = 18U;
    entry.key.market = market::MarketV1::kShenzhen;
    entry.key.security_id_source = Bytes("102 ");
    entry.key.security_id = Bytes("000001");
    entry.quantity_unit = market::QuantityUnitV1::kShare;
    entry.security_type = market::SecurityTypeV1::kEquity;
    entry.asset_scope = market::AssetScopeV1::kDocumentedCore;

    std::unique_ptr<market::InstrumentRegistryV1> registry;
    if (market::InstrumentRegistryV1::Create(
            171U,
            std::span<const market::InstrumentRegistryEntryV1>(&entry, 1U),
            &registry) !=
        market::InstrumentRegistryCreateErrorV1::kNone) {
        return nullptr;
    }
    return registry;
}

runtime::RealtimePipelineConfigV1 MakeConfig(
    const market::InstrumentRegistryV1* registry) {
    runtime::RealtimePipelineConfigV1 config{};
    config.run_id[0U] = std::byte{0x51U};
    config.run_id[15U] = std::byte{0xa7U};
    config.trade_date = 20260724U;
    config.registry = registry;
    config.source_stream_ids = {1001U, 1002U, 2001U, 2002U};
    config.maximum_sdk_message_bytes = 4096U;
    config.decoder_queue_capacity_per_source = 16U;
    config.history_worker_count = 1U;
    config.history_queue_capacity_per_source_worker = 16U;
    config.maximum_history_records_per_instrument = 4U;
    config.enforce_receive_trade_date = false;
    config.sdk.enabled = true;
    config.sdk.server_address = "127.0.0.1:9112";
    config.sdk.user_name = "lifecycle-test";
    config.sdk.log_prefix = "lifecycle-test";
    config.sdk.message_encoding = mdl::MDLEID_BINARY;
    config.sdk.merge_message = false;
    return config;
}

std::size_t EventIndex(
    std::span<const LifecycleEvent> events,
    LifecycleEvent wanted) {
    const auto found = std::find(events.begin(), events.end(), wanted);
    return found == events.end()
               ? std::numeric_limits<std::size_t>::max()
               : static_cast<std::size_t>(found - events.begin());
}

}  // namespace

int main() {
    TestContext test;
    std::unique_ptr<market::InstrumentRegistryV1> registry = MakeRegistry();
    test.Expect(registry != nullptr, "registry creation");
    if (registry == nullptr) {
        return 1;
    }

    auto state = std::make_shared<LifecycleState>();
    std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
    std::string detail;
    test.Expect(
        runtime::RealtimePipelineV1::CreateForTest(
            MakeConfig(registry.get()),
            std::make_shared<RecordingFactory>(state),
            &pipeline,
            &detail) == runtime::RealtimePipelineCreateErrorV1::kNone &&
            pipeline != nullptr,
        "pipeline creation: " + detail);
    if (pipeline == nullptr) {
        return 1;
    }

    mdl::MessageHandlerBase* const handler = state->handler();
    test.Expect(handler != nullptr, "SDK callback handler installed");
    if (handler == nullptr) {
        pipeline->StopAndDrain();
        return 1;
    }

    auto message_gate = std::make_shared<OneShotGate>();
    BlockingMessage message(message_gate);
    std::thread callback_thread([&] {
        state->CallbackEnter();
        handler->OnMessage(nullptr, &message);
        state->CallbackExit();
    });

    const bool callback_blocked =
        message_gate->WaitUntilEntered(2s);
    test.Expect(
        callback_blocked,
        "callback reaches the owned-ingress accessor while holding admission");

    runtime::RealtimePipelineCutResultV1 terminal{};
    std::thread terminal_thread([&] {
        state->TerminalCallStarted();
        terminal = pipeline->StopAndPublishFinalGeneration(2s);
        state->TerminalReturn();
    });

    test.Expect(
        state->WaitForTerminalCallStarted(2s),
        "terminal call starts while the SDK callback is blocked");
    const LifecycleSnapshot while_callback_blocked = state->Snapshot();
    test.Expect(
        while_callback_blocked.subscriber_release_calls == 0U &&
            while_callback_blocked.manager_release_calls == 0U,
        "blocked admitted callback has not been followed by SDK release");
    message_gate->Open();
    callback_thread.join();

    const bool shutdown_quiesced = state->WaitForShutdownQuiesced(2s);
    test.Expect(
        shutdown_quiesced,
        "Shutdown observes callback quiescence before returning");
    const LifecycleSnapshot during_shutdown = state->Snapshot();
    test.Expect(
        during_shutdown.subscriber_release_calls == 0U &&
            during_shutdown.manager_release_calls == 0U &&
            EventIndex(
                during_shutdown.events,
                LifecycleEvent::kTerminalReturn) ==
                std::numeric_limits<std::size_t>::max(),
        "terminal cannot release either SDK object while Shutdown is blocked");
    state->AllowShutdownReturn();
    terminal_thread.join();

    test.Expect(
        terminal.published(),
        "terminal publishes after the admitted callback exits");
    if (terminal.published()) {
        const market::RealtimeHistoryWatermarkV1& watermark =
            terminal.history_generation->watermark();
        const market::RealtimeInstrumentGenerationV1* const row =
            terminal.history_generation->Find(18U);
        test.Expect(
            watermark.ingress_sequence_exclusive == 2U &&
                watermark.sources[3U].sequence_exclusive == 2U &&
                watermark.sources[0U].sequence_exclusive == 1U &&
                watermark.sources[1U].sequence_exclusive == 1U &&
                watermark.sources[2U].sequence_exclusive == 1U,
            "final watermark includes the callback that entered admission first");
        test.Expect(
            row != nullptr && row->latest_tick != nullptr &&
                row->history.size() == 1U &&
                row->history.front()->ingress_sequence() == 1U,
            "final history contains exactly the admitted callback record");
        test.Expect(
            terminal.factor_generation->input_history().get() ==
                terminal.history_generation.get(),
            "factor generation retains the exact terminal history handle");
    }

    const runtime::RealtimePipelineSnapshotV1 pipeline_snapshot =
        pipeline->Snapshot();
    test.Expect(
        pipeline_snapshot.accepted_messages == 1U &&
            pipeline_snapshot.decoded_messages == 1U &&
            pipeline_snapshot.global_ingress_sequence == 1U &&
            pipeline_snapshot.source_sequences[3U] == 1U &&
            pipeline_snapshot.last_published_generation == 1U &&
            pipeline_snapshot.stopped && !pipeline_snapshot.accepting &&
            !pipeline_snapshot.fatal,
        "terminal state is the complete non-fatal one-message prefix");

    const LifecycleSnapshot lifecycle = state->Snapshot();
    const std::size_t callback_exit = EventIndex(
        lifecycle.events, LifecycleEvent::kCallbackExit);
    const std::size_t shutdown_begin = EventIndex(
        lifecycle.events, LifecycleEvent::kShutdownBegin);
    const std::size_t shutdown_end = EventIndex(
        lifecycle.events, LifecycleEvent::kShutdownEnd);
    const std::size_t subscriber_release = EventIndex(
        lifecycle.events, LifecycleEvent::kSubscriberRelease);
    const std::size_t manager_release = EventIndex(
        lifecycle.events, LifecycleEvent::kManagerRelease);
    const std::size_t terminal_return = EventIndex(
        lifecycle.events, LifecycleEvent::kTerminalReturn);
    test.Expect(
        callback_exit < subscriber_release,
        "Subscriber is not released before the SDK callback exits");
    test.Expect(
        shutdown_begin < shutdown_end &&
            shutdown_end < subscriber_release &&
            subscriber_release < manager_release &&
            manager_release < terminal_return,
        "lifecycle order is Shutdown, Subscriber Release, manager Release, terminal return");
    test.Expect(
        lifecycle.shutdown_calls == 1U &&
            lifecycle.subscriber_release_calls == 1U &&
            lifecycle.manager_release_calls == 1U,
        "each physical SDK lifecycle operation occurs exactly once");

    pipeline->StopAndDrain();
    pipeline.reset();
    const LifecycleSnapshot after_destruction = state->Snapshot();
    test.Expect(
        after_destruction.shutdown_calls == 1U &&
            after_destruction.subscriber_release_calls == 1U &&
            after_destruction.manager_release_calls == 1U,
        "idempotent stop/destruction does not repeat SDK lifecycle operations");

    return test.failures() == 0 ? 0 : 1;
}
