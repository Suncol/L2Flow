#include "l2flow/ingress/ingress_app.h"

#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <sys/wait.h>
#include <unistd.h>

namespace ingress = l2flow::ingress;
namespace sdk = l2flow::sdk;
namespace mdl = datayes::mdl;

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

class Recorder final {
public:
    void Add(std::string event) {
        std::lock_guard<std::mutex> lock(mutex_);
        events_.push_back(std::move(event));
    }

    [[nodiscard]] std::vector<std::string> Snapshot() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return events_;
    }

private:
    mutable std::mutex mutex_;
    std::vector<std::string> events_;
};

std::string Boolean(bool value) {
    return value ? "true" : "false";
}

std::string KeyText(const sdk::MessageKey& key) {
    return std::to_string(
               static_cast<unsigned int>(key.service_id)) +
           "." + std::to_string(key.service_version) +
           "." + std::to_string(key.message_id);
}

struct FakeState final {
    explicit FakeState(std::shared_ptr<Recorder> recorder_value)
        : recorder(std::move(recorder_value)) {}

    ~FakeState() {
        ReleaseClock();
        JoinCallback();
    }

    void WaitForClockEntry() {
        std::unique_lock<std::mutex> lock(clock_mutex);
        clock_condition.wait(
            lock, [this] { return clock_entered; });
    }

    void ReleaseClock() {
        std::lock_guard<std::mutex> lock(clock_mutex);
        clock_released = true;
        clock_condition.notify_all();
    }

    void JoinCallback() {
        if (callback_thread.joinable()) {
            callback_thread.join();
        }
    }

    std::shared_ptr<Recorder> recorder;
    mdl::MessageHandlerBase* handler = nullptr;
    sdk::MessageKey callback_key{};

    bool factory_returns_null = false;
    bool subscriber_returns_null = false;
    bool connect_returns_error = false;
    bool synchronous_connect_callback = false;
    bool start_blocking_callback = false;
    bool callback_during_shutdown = false;
    bool shutdown_throws = false;
    bool shutdown_leaves_callback_inflight = false;
    bool subscriber_release_fails = false;
    bool manager_release_fails = false;

    std::mutex clock_mutex;
    std::condition_variable clock_condition;
    bool clock_entered = false;
    bool clock_released = false;
    bool clock_block_once = true;
    std::thread callback_thread;

    std::atomic<std::uint64_t> message_head_calls{0U};
    std::atomic<std::uint64_t> message_body_calls{0U};
};

class FakeMessage final : public mdl::MDLMessage {
public:
    FakeMessage(std::shared_ptr<FakeState> state,
                const sdk::MessageKey& key)
        : state_(std::move(state)) {
        std::memset(&head_, 0, sizeof(head_));
        head_.HeadSize =
            static_cast<std::uint8_t>(
                ingress::kVendorMessageHeadBytes);
        head_.MessageSize =
            static_cast<std::uint32_t>(
                ingress::kVendorMessageHeadBytes);
        head_.MessageEncoding =
            static_cast<std::uint8_t>(mdl::MDLEID_BINARY);
        head_.ServiceID = key.service_id;
        head_.ServiceVersion = key.service_version;
        head_.MessageID = key.message_id;
        head_.SequenceID = 91U;
    }

    void AddRef() override {}
    int ReleaseRef() override { return 1; }

    mdl::MDLMessageHead* GetHead() const override {
        state_->message_head_calls.fetch_add(
            1U, std::memory_order_relaxed);
        return const_cast<mdl::MDLMessageHead*>(&head_);
    }

    char* GetBody() const override {
        state_->message_body_calls.fetch_add(
            1U, std::memory_order_relaxed);
        return nullptr;
    }

    mdl::MDLMessage* _Copy() const override {
        return nullptr;
    }

private:
    std::shared_ptr<FakeState> state_;
    mdl::MDLMessageHead head_{};
};

void InvokeCallback(const std::shared_ptr<FakeState>& state) {
    FakeMessage message(state, state->callback_key);
    state->handler->OnMessage(nullptr, &message);
}

class FakeClock final : public ingress::CaptureClock {
public:
    explicit FakeClock(std::shared_ptr<FakeState> state)
        : state_(std::move(state)) {}

    std::uint64_t MonotonicRawNanoseconds() override {
        std::unique_lock<std::mutex> lock(state_->clock_mutex);
        if (state_->start_blocking_callback &&
            state_->clock_block_once) {
            state_->clock_block_once = false;
            state_->clock_entered = true;
            state_->clock_condition.notify_all();
            state_->clock_condition.wait(
                lock,
                [this] { return state_->clock_released; });
        }
        return monotonic_.fetch_add(
            1U, std::memory_order_relaxed);
    }

    std::uint64_t RealtimeNanoseconds() override {
        return realtime_.fetch_add(
            1U, std::memory_order_relaxed);
    }

private:
    std::shared_ptr<FakeState> state_;
    std::atomic<std::uint64_t> monotonic_{1000U};
    std::atomic<std::uint64_t> realtime_{2000U};
};

class FakeSubscriber final : public sdk::SdkSubscriber {
public:
    explicit FakeSubscriber(std::shared_ptr<FakeState> state)
        : state_(std::move(state)) {}

    ~FakeSubscriber() override {
        static_cast<void>(Release(nullptr));
    }

    void SetServerAddress(std::string_view address) override {
        state_->recorder->Add(
            "subscriber.set_server:" + std::string(address));
    }

    void SetUserName(std::string_view) override {
        state_->recorder->Add("subscriber.set_user_name");
    }

    void SetHeartbeatInterval(std::uint32_t seconds) override {
        state_->recorder->Add(
            "subscriber.set_heartbeat_interval:" +
            std::to_string(seconds));
    }

    void SetHeartbeatTimeout(std::uint32_t seconds) override {
        state_->recorder->Add(
            "subscriber.set_heartbeat_timeout:" +
            std::to_string(seconds));
    }

    void SetMessageEncoding(
        mdl::MDLMessageEncoding encoding) override {
        state_->recorder->Add(
            "subscriber.set_encoding:" +
            std::to_string(static_cast<int>(encoding)));
    }

    void EnableMergeMessage(bool enable) override {
        state_->recorder->Add(
            "subscriber.enable_merge:" + Boolean(enable));
    }

    void SetSendMacAuth(bool enable) override {
        state_->recorder->Add(
            "subscriber.set_mac_auth:" + Boolean(enable));
    }

    void EnableServerSelect(bool enable) override {
        state_->recorder->Add(
            "subscriber.enable_server_select:" +
            Boolean(enable));
    }

    void AddSubscription(const sdk::MessageKey& key) override {
        state_->recorder->Add(
            "subscriber.add:" + KeyText(key));
    }

    std::string Connect() override {
        state_->recorder->Add("subscriber.connect");
        if (state_->synchronous_connect_callback) {
            InvokeCallback(state_);
        }
        if (state_->start_blocking_callback) {
            state_->callback_thread =
                std::thread([state = state_] {
                    InvokeCallback(state);
                });
        }
        return state_->connect_returns_error
                   ? "injected connect error"
                   : std::string{};
    }

    bool Release(std::string* error) noexcept override {
        if (released_) {
            if (error != nullptr) {
                error->clear();
            }
            return true;
        }
        released_ = true;
        state_->recorder->Add("subscriber.release");
        if (state_->subscriber_release_fails) {
            if (error != nullptr) {
                *error = "injected subscriber release failure";
            }
            return false;
        }
        if (error != nullptr) {
            error->clear();
        }
        return true;
    }

private:
    std::shared_ptr<FakeState> state_;
    bool released_ = false;
};

class FakeManager final : public sdk::SdkManager {
public:
    explicit FakeManager(std::shared_ptr<FakeState> state)
        : state_(std::move(state)) {}

    ~FakeManager() override {
        static_cast<void>(Release(nullptr));
    }

    void EnableLog(
        std::string_view prefix,
        bool console) override {
        state_->recorder->Add(
            "manager.enable_log:" + std::string(prefix) +
            ":" + Boolean(console));
    }

    std::unique_ptr<sdk::SdkSubscriber> CreateSubscriber(
        mdl::MessageHandlerBase* handler,
        bool multithread_callback) override {
        state_->handler = handler;
        state_->recorder->Add(
            "manager.create_subscriber:" +
            Boolean(multithread_callback));
        if (state_->subscriber_returns_null) {
            return nullptr;
        }
        return std::make_unique<FakeSubscriber>(state_);
    }

    void Shutdown() override {
        state_->recorder->Add("manager.shutdown");
        if (!state_->shutdown_leaves_callback_inflight) {
            state_->ReleaseClock();
            state_->JoinCallback();
        }
        if (state_->callback_during_shutdown) {
            InvokeCallback(state_);
        }
        if (state_->shutdown_throws) {
            throw std::runtime_error(
                "injected shutdown exception");
        }
    }

    bool Release(std::string* error) noexcept override {
        if (released_) {
            if (error != nullptr) {
                error->clear();
            }
            return true;
        }
        released_ = true;
        state_->recorder->Add("manager.release");
        if (state_->manager_release_fails) {
            if (error != nullptr) {
                *error = "injected manager release failure";
            }
            return false;
        }
        if (error != nullptr) {
            error->clear();
        }
        return true;
    }

private:
    std::shared_ptr<FakeState> state_;
    bool released_ = false;
};

class FakeFactory final : public sdk::SdkFactory {
public:
    explicit FakeFactory(std::shared_ptr<FakeState> state)
        : state_(std::move(state)) {}

    std::unique_ptr<sdk::SdkManager> Create(
        int work_threads,
        int io_threads) override {
        state_->recorder->Add(
            "factory.create:" +
            std::to_string(work_threads) + ":" +
            std::to_string(io_threads));
        if (state_->factory_returns_null) {
            return nullptr;
        }
        return std::make_unique<FakeManager>(state_);
    }

private:
    std::shared_ptr<FakeState> state_;
};

struct OutputState final {
    explicit OutputState(std::shared_ptr<Recorder> recorder_value)
        : recorder(std::move(recorder_value)) {}

    std::shared_ptr<Recorder> recorder;
    std::mutex mutex;
    std::vector<std::byte> bytes;
    bool closed = false;
};

class MemoryOutput final : public ingress::ShadowCaptureOutput {
public:
    explicit MemoryOutput(std::shared_ptr<OutputState> state)
        : state_(std::move(state)) {}

    ingress::ShadowOutputWriteResult WriteSome(
        std::span<const std::byte> bytes) noexcept override {
        try {
            std::lock_guard<std::mutex> lock(state_->mutex);
            if (state_->closed) {
                return {0U, EBADF};
            }
            state_->bytes.insert(
                state_->bytes.end(), bytes.begin(), bytes.end());
            return {bytes.size(), 0};
        } catch (...) {
            return {0U, ENOMEM};
        }
    }

    int Fdatasync() noexcept override {
        state_->recorder->Add("output.fdatasync");
        return 0;
    }

    int Close() noexcept override {
        {
            std::lock_guard<std::mutex> lock(state_->mutex);
            state_->closed = true;
        }
        state_->recorder->Add("output.close");
        return 0;
    }

private:
    std::shared_ptr<OutputState> state_;
};

class RecordingObserver final
    : public ingress::IngressLifecycleObserver {
public:
    explicit RecordingObserver(
        std::shared_ptr<Recorder> recorder)
        : recorder_(std::move(recorder)) {}

    void Observe(
        ingress::IngressLifecycleEvent event) noexcept override {
        switch (event) {
        case ingress::IngressLifecycleEvent::ShadowWriterStarted:
            recorder_->Add("app.writer_started");
            break;
        case ingress::IngressLifecycleEvent::HandlerBeginStopping:
            recorder_->Add("app.handler_begin_stopping");
            break;
        case ingress::IngressLifecycleEvent::HandlerQuiesced:
            recorder_->Add("app.handler_quiesced");
            break;
        case ingress::IngressLifecycleEvent::ShadowWriterStopRequested:
            recorder_->Add("app.writer_stop_requested");
            break;
        case ingress::IngressLifecycleEvent::ShadowWriterJoined:
            recorder_->Add("app.writer_joined");
            break;
        }
    }

private:
    std::shared_ptr<Recorder> recorder_;
};

sdk::IngressConfig TestConfig(sdk::IngressKind kind) {
    sdk::IngressConfig config =
        sdk::DefaultIngressConfig(kind);
    config.endpoint.name = "test-endpoint";
    config.endpoint.resolved_server_address =
        "tcp://127.0.0.1:18001";
    config.endpoint.contract_sha256 =
        std::string(64U, 'a');
    config.endpoint.message_encoding = mdl::MDLEID_BINARY;
    config.endpoint.merge_message = true;
    config.endpoint.send_mac_auth = true;
    config.endpoint.server_select = false;
    config.credential_name = "mdl-token";
    config.token = "secret-test-token";
    config.sdk_log_prefix = "phase1-test";
    config.shadow_capture_path = "/tmp/not-used-with-memory-output";
    config.metrics_textfile_path =
        "/tmp/not-used-phase1-test.prom";
    config.heartbeat_interval_seconds = 10U;
    config.heartbeat_timeout_seconds = 30U;
    config.include_optional_index = true;
    config.max_message_bytes = 128U;
    config.ring_capacity_bytes = 1024U;
    config.capture_date = 20260717U;
    config.first_ingress_sequence = 1U;
    return config;
}

std::vector<std::string> InitializationPrefix(
    const sdk::IngressConfig& config,
    const sdk::IngressSpec& spec) {
    std::vector<std::string> expected = {
        "factory.create:" +
            std::to_string(config.work_threads) + ":" +
            std::to_string(config.io_threads),
        "manager.enable_log:phase1-test:false",
        "manager.create_subscriber:false",
        "subscriber.set_server:tcp://127.0.0.1:18001",
        "subscriber.set_user_name",
        "subscriber.set_heartbeat_interval:10",
        "subscriber.set_heartbeat_timeout:30",
        "subscriber.set_encoding:" +
            std::to_string(
                static_cast<int>(mdl::MDLEID_BINARY)),
        "subscriber.enable_merge:true",
        "subscriber.set_mac_auth:true",
        "subscriber.enable_server_select:false",
    };
    for (const sdk::MessageKey& key : spec.required) {
        expected.push_back(
            "subscriber.add:" + KeyText(key));
    }
    for (const sdk::MessageKey& key : spec.optional) {
        expected.push_back(
            "subscriber.add:" + KeyText(key));
    }
    expected.push_back("app.writer_started");
    expected.push_back("subscriber.connect");
    return expected;
}

void AppendSuccessfulStop(std::vector<std::string>* expected) {
    const std::string suffix[] = {
        "app.handler_begin_stopping",
        "manager.shutdown",
        "app.handler_quiesced",
        "app.writer_stop_requested",
        "output.fdatasync",
        "output.close",
        "app.writer_joined",
        "subscriber.release",
        "manager.release",
    };
    expected->insert(
        expected->end(), std::begin(suffix), std::end(suffix));
}

void CheckFourKindsAndExactOrder(TestContext* test) {
    for (const sdk::IngressSpec& spec :
         sdk::AllIngressSpecs()) {
        const auto recorder = std::make_shared<Recorder>();
        const auto state =
            std::make_shared<FakeState>(recorder);
        state->callback_key = spec.required.front();
        const auto output_state =
            std::make_shared<OutputState>(recorder);
        const sdk::IngressConfig config =
            TestConfig(spec.kind);
        const auto factory =
            std::make_shared<FakeFactory>(state);
        RecordingObserver observer(recorder);

        {
            ingress::IngressApp app(
                config,
                factory,
                std::make_unique<FakeClock>(state),
                std::make_unique<MemoryOutput>(output_state),
                ingress::IngressAppOptions{
                    .callback_quiesce_timeout =
                        std::chrono::milliseconds(500),
                    .sdk_log_runtime_prefix = {}},
                &observer);
            std::string error;
            test->Expect(
                app.Initialize(&error),
                std::string(spec.service_name) +
                    " initializes successfully: " + error);
            test->Expect(
                app.state() ==
                    ingress::IngressAppState::Running &&
                    !app.fatal(),
                std::string(spec.service_name) +
                    " is running without fatal");
            const std::string metrics =
                app.prometheus_metrics();
            test->Expect(
                metrics.find("mdl_build_info{") !=
                        std::string::npos &&
                    metrics.find(
                        sdk::IngressConfigSha256(config)) !=
                        std::string::npos &&
                    metrics.find(
                        "mdl_shadow_sink_records_total") !=
                        std::string::npos &&
                    metrics.find(
                        "mdl_shadow_readiness_generation") !=
                        std::string::npos &&
                    metrics.find(
                        "mdl_shadow_required_first_seen") !=
                        std::string::npos,
                std::string(spec.service_name) +
                    " metrics include build, config, sink, readiness, "
                    "and required-message evidence");
            test->Expect(
                metrics.find(config.token) ==
                        std::string::npos &&
                    metrics.find(
                        config.endpoint
                            .resolved_server_address) ==
                        std::string::npos,
                std::string(spec.service_name) +
                    " metrics disclose neither token nor endpoint address");
            test->Expect(
                app.Stop(&error),
                std::string(spec.service_name) +
                    " stops successfully: " + error);
            const std::size_t event_count =
                recorder->Snapshot().size();
            test->Expect(
                app.Stop(&error) &&
                    recorder->Snapshot().size() == event_count,
                std::string(spec.service_name) +
                    " repeated Stop is idempotent");
        }

        std::vector<std::string> expected =
            InitializationPrefix(config, spec);
        AppendSuccessfulStop(&expected);
        test->Expect(
            recorder->Snapshot() == expected,
            std::string(spec.service_name) +
                " complete initialize/stop order is exact");

        const std::vector<std::string> observed =
            recorder->Snapshot();
        const auto contains_forbidden =
            [&observed](std::string_view needle) {
                for (const std::string& event : observed) {
                    if (event.find(needle) !=
                        std::string::npos) {
                        return true;
                    }
                }
                return false;
            };
        test->Expect(
            !contains_forbidden("set_password") &&
                !contains_forbidden("read_buffer"),
            std::string(spec.service_name) +
                " exposes neither forbidden setter");
    }
}

void CheckConnectFailureDrainsSynchronousCallback(
    TestContext* test) {
    const auto recorder = std::make_shared<Recorder>();
    const auto state =
        std::make_shared<FakeState>(recorder);
    const sdk::IngressConfig config =
        TestConfig(sdk::IngressKind::ShTick);
    const sdk::IngressSpec& spec =
        sdk::GetIngressSpec(config.kind);
    state->callback_key = spec.required.front();
    state->connect_returns_error = true;
    state->synchronous_connect_callback = true;
    const auto output_state =
        std::make_shared<OutputState>(recorder);
    RecordingObserver observer(recorder);

    ingress::IngressApp app(
        config,
        std::make_shared<FakeFactory>(state),
        std::make_unique<FakeClock>(state),
        std::make_unique<MemoryOutput>(output_state),
        ingress::IngressAppOptions{
            .callback_quiesce_timeout =
                std::chrono::milliseconds(500),
            .sdk_log_runtime_prefix = {}},
        &observer);
    std::string error;
    test->Expect(
        !app.Initialize(&error) &&
            error.find("injected connect error") !=
                std::string::npos,
        "Connect error is returned as a lifecycle failure");
    test->Expect(
        app.state() == ingress::IngressAppState::Stopped &&
            app.fatal(),
        "Connect failure performs a complete fatal stop");
    test->Expect(
        app.capture_metrics().captured_records == 1U &&
            app.shadow_stats().sink_records == 1U,
        "writer accepts and drains a synchronous callback before Connect returns");

    std::vector<std::string> expected =
        InitializationPrefix(config, spec);
    AppendSuccessfulStop(&expected);
    test->Expect(
        recorder->Snapshot() == expected,
        "Connect failure uses the same exact ordered teardown");
}

void CheckSuccessfulConnectWithFatalCallbackStops(
    TestContext* test) {
    const auto recorder = std::make_shared<Recorder>();
    const auto state =
        std::make_shared<FakeState>(recorder);
    const sdk::IngressConfig config =
        TestConfig(sdk::IngressKind::ShTick);
    const sdk::IngressSpec& spec =
        sdk::GetIngressSpec(config.kind);
    state->callback_key =
        sdk::MessageKey{0xffU, 1U, 1U};
    state->synchronous_connect_callback = true;
    const auto output_state =
        std::make_shared<OutputState>(recorder);
    RecordingObserver observer(recorder);

    ingress::IngressApp app(
        config,
        std::make_shared<FakeFactory>(state),
        std::make_unique<FakeClock>(state),
        std::make_unique<MemoryOutput>(output_state),
        ingress::IngressAppOptions{
            .callback_quiesce_timeout =
                std::chrono::milliseconds(500),
            .sdk_log_runtime_prefix = {}},
        &observer);
    std::string error;
    test->Expect(
        !app.Initialize(&error) &&
            error.find("capture failed during SDK Connect") !=
                std::string::npos,
        "a fatal synchronous callback makes successful Connect fail "
        "initialization");
    test->Expect(
        app.state() == ingress::IngressAppState::Stopped &&
            app.fatal(),
        "fatal capture observed during Connect performs a complete stop");

    std::vector<std::string> expected =
        InitializationPrefix(config, spec);
    AppendSuccessfulStop(&expected);
    test->Expect(
        recorder->Snapshot() == expected,
        "fatal capture during Connect uses exact ordered teardown");
}

void CheckRuntimeSdkLogPrefixDoesNotChangeConfigIdentity(
    TestContext* test) {
    const auto recorder = std::make_shared<Recorder>();
    const auto state =
        std::make_shared<FakeState>(recorder);
    const sdk::IngressConfig config =
        TestConfig(sdk::IngressKind::ShTick);
    const sdk::IngressSpec& spec =
        sdk::GetIngressSpec(config.kind);
    state->callback_key = spec.required.front();
    const auto output_state =
        std::make_shared<OutputState>(recorder);
    RecordingObserver observer(recorder);
    constexpr std::string_view stable_prefix =
        "/proc/self/fd/123/service";

    ingress::IngressAppOptions options{
        .callback_quiesce_timeout =
            std::chrono::milliseconds(500),
        .sdk_log_runtime_prefix = {}};
    options.sdk_log_runtime_prefix =
        stable_prefix;
    ingress::IngressApp app(
        config,
        std::make_shared<FakeFactory>(state),
        std::make_unique<FakeClock>(state),
        std::make_unique<MemoryOutput>(output_state),
        std::move(options),
        &observer);
    std::string error;
    test->Expect(
        app.Initialize(&error),
        "stable runtime SDK log prefix initializes: " +
            error);
    const std::string metrics =
        app.prometheus_metrics();
    test->Expect(
        app.Stop(&error),
        "stable runtime SDK log prefix stops cleanly: " +
            error);

    std::vector<std::string> expected =
        InitializationPrefix(config, spec);
    expected[1U] =
        "manager.enable_log:" +
        std::string(stable_prefix) +
        ":false";
    AppendSuccessfulStop(&expected);
    test->Expect(
        recorder->Snapshot() == expected,
        "only the vendor logging call receives the stable fd path");
    test->Expect(
        metrics.find(
            sdk::IngressConfigSha256(config)) !=
                std::string::npos &&
            metrics.find(stable_prefix) ==
                std::string::npos,
        "runtime log anchoring preserves canonical config identity "
        "and is not exported");
}

void CheckCreateFailures(TestContext* test) {
    {
        const auto recorder = std::make_shared<Recorder>();
        const auto state =
            std::make_shared<FakeState>(recorder);
        state->factory_returns_null = true;
        const sdk::IngressConfig config =
            TestConfig(sdk::IngressKind::ShSnapshot);
        const sdk::IngressSpec& spec =
            sdk::GetIngressSpec(config.kind);
        state->callback_key = spec.required.front();
        const auto output_state =
            std::make_shared<OutputState>(recorder);
        RecordingObserver observer(recorder);
        ingress::IngressApp app(
            config,
            std::make_shared<FakeFactory>(state),
            std::make_unique<FakeClock>(state),
            std::make_unique<MemoryOutput>(output_state),
            {},
            &observer);
        std::string error;
        test->Expect(
            !app.Initialize(&error) &&
                error.find("Create returned null") !=
                    std::string::npos,
            "null manager Create is translated to a fatal error");
        const std::vector<std::string> expected = {
            "factory.create:2:1",
            "app.handler_begin_stopping",
            "app.handler_quiesced",
        };
        test->Expect(
            recorder->Snapshot() == expected,
            "null manager never starts or releases nonexistent SDK objects");
    }

    {
        const auto recorder = std::make_shared<Recorder>();
        const auto state =
            std::make_shared<FakeState>(recorder);
        state->subscriber_returns_null = true;
        const sdk::IngressConfig config =
            TestConfig(sdk::IngressKind::SzTick);
        const sdk::IngressSpec& spec =
            sdk::GetIngressSpec(config.kind);
        state->callback_key = spec.required.front();
        const auto output_state =
            std::make_shared<OutputState>(recorder);
        RecordingObserver observer(recorder);
        ingress::IngressApp app(
            config,
            std::make_shared<FakeFactory>(state),
            std::make_unique<FakeClock>(state),
            std::make_unique<MemoryOutput>(output_state),
            {},
            &observer);
        std::string error;
        test->Expect(
            !app.Initialize(&error) &&
                error.find("CreateSubscriber returned null") !=
                    std::string::npos,
            "null subscriber Create is translated to a fatal error");
        const std::vector<std::string> expected = {
            "factory.create:4:1",
            "manager.enable_log:phase1-test:false",
            "manager.create_subscriber:false",
            "app.handler_begin_stopping",
            "manager.shutdown",
            "app.handler_quiesced",
            "manager.release",
        };
        test->Expect(
            recorder->Snapshot() == expected,
            "null subscriber shuts down and releases its sole manager");
    }
}

void CheckShutdownCallbackAndHandlerLifetime(
    TestContext* test) {
    const auto recorder = std::make_shared<Recorder>();
    const auto state =
        std::make_shared<FakeState>(recorder);
    state->start_blocking_callback = true;
    state->callback_during_shutdown = true;
    const sdk::IngressConfig config =
        TestConfig(sdk::IngressKind::SzTick);
    const sdk::IngressSpec& spec =
        sdk::GetIngressSpec(config.kind);
    state->callback_key = spec.required.front();
    const auto output_state =
        std::make_shared<OutputState>(recorder);
    RecordingObserver observer(recorder);

    ingress::IngressApp app(
        config,
        std::make_shared<FakeFactory>(state),
        std::make_unique<FakeClock>(state),
        std::make_unique<MemoryOutput>(output_state),
        ingress::IngressAppOptions{
            .callback_quiesce_timeout =
                std::chrono::milliseconds(500),
            .sdk_log_runtime_prefix = {}},
        &observer);
    std::string error;
    test->Expect(
        app.Initialize(&error),
        "blocking callback fixture initializes");
    state->WaitForClockEntry();
    test->Expect(
        app.Stop(&error),
        "Shutdown synchronizes the already-inflight callback");

    const ingress::CaptureMetricsSnapshot metrics =
        app.capture_metrics();
    test->Expect(
        metrics.captured_records == 1U &&
            metrics.callbacks_after_stop == 1U &&
            app.shadow_stats().sink_records == 1U,
        "inflight callback completes before drain while a Shutdown callback is gated");
    test->Expect(
        state->message_head_calls.load(
            std::memory_order_relaxed) == 1U &&
            state->message_body_calls.load(
                std::memory_order_relaxed) == 0U,
        "callback begun during Shutdown sees a live stopped handler without touching vendor data");

    std::vector<std::string> expected =
        InitializationPrefix(config, spec);
    AppendSuccessfulStop(&expected);
    test->Expect(
        recorder->Snapshot() == expected,
        "Shutdown callback preserves handler/quiesce/writer/release order");
}

void CheckShutdownExceptionFailsProcessSafe(TestContext* test) {
    enum class FailureMode : std::uint8_t {
        ShutdownThrows = 0U,
        QuiesceTimeout,
        SubscriberRelease,
        ManagerRelease,
    };
    const struct FailureCase {
        FailureMode mode;
        const char* description;
    } cases[] = {
        {FailureMode::ShutdownThrows, "Shutdown exception"},
        {FailureMode::QuiesceTimeout, "callback quiesce timeout"},
        {FailureMode::SubscriberRelease, "Subscriber release failure"},
        {FailureMode::ManagerRelease, "IOManager release failure"},
    };

    for (const FailureCase& failure : cases) {
        const pid_t child = ::fork();
        if (child < 0) {
            test->Expect(
                false,
                std::string("forks ") +
                    failure.description + " safety fixture");
            continue;
        }
        if (child == 0) {
            // Do not let a test runner's custom terminate hook turn the
            // policy into continuation. Exit without unwinding any callback
            // target when opaque SDK object lifetime cannot be proved.
            std::set_terminate([] {
                ::_exit(86);
            });

            const auto recorder = std::make_shared<Recorder>();
            const auto state =
                std::make_shared<FakeState>(recorder);
            switch (failure.mode) {
            case FailureMode::ShutdownThrows:
                state->shutdown_throws = true;
                break;
            case FailureMode::QuiesceTimeout:
                state->start_blocking_callback = true;
                state->shutdown_leaves_callback_inflight = true;
                break;
            case FailureMode::SubscriberRelease:
                state->subscriber_release_fails = true;
                break;
            case FailureMode::ManagerRelease:
                state->manager_release_fails = true;
                break;
            }
            const sdk::IngressConfig config =
                TestConfig(sdk::IngressKind::ShSnapshot);
            const sdk::IngressSpec& spec =
                sdk::GetIngressSpec(config.kind);
            state->callback_key = spec.required.front();
            const auto output_state =
                std::make_shared<OutputState>(recorder);
            RecordingObserver observer(recorder);

            ingress::IngressApp app(
                config,
                std::make_shared<FakeFactory>(state),
                std::make_unique<FakeClock>(state),
                std::make_unique<MemoryOutput>(output_state),
                ingress::IngressAppOptions{
                    .callback_quiesce_timeout =
                        failure.mode ==
                                FailureMode::QuiesceTimeout
                            ? std::chrono::milliseconds(10)
                            : std::chrono::milliseconds(500),
                    .sdk_log_runtime_prefix = {}},
                &observer);
            std::string error;
            if (!app.Initialize(&error)) {
                ::_exit(88);
            }
            if (failure.mode == FailureMode::QuiesceTimeout) {
                state->WaitForClockEntry();
            }
            static_cast<void>(app.Stop(&error));
            ::_exit(87);
        }

        int status = 0;
        const pid_t waited = ::waitpid(child, &status, 0);
        test->Expect(
            waited == child && WIFEXITED(status) &&
                WEXITSTATUS(status) == 86,
            std::string(failure.description) +
                " fail-stops before callback targets can be released "
                "or destroyed");
    }
}

}  // namespace

int main() {
    TestContext test;
    try {
        CheckFourKindsAndExactOrder(&test);
        CheckConnectFailureDrainsSynchronousCallback(&test);
        CheckSuccessfulConnectWithFatalCallbackStops(&test);
        CheckRuntimeSdkLogPrefixDoesNotChangeConfigIdentity(
            &test);
        CheckCreateFailures(&test);
        CheckShutdownCallbackAndHandlerLifetime(&test);
        CheckShutdownExceptionFailsProcessSafe(&test);
    } catch (const std::exception& exception) {
        std::cerr << "UNCAUGHT: " << exception.what() << '\n';
        return 2;
    }

    if (test.failures != 0) {
        std::cerr << test.failures
                  << " phase-1 ingress lifecycle test(s) failed\n";
        return 1;
    }
    std::cout << "phase-1 ingress lifecycle checks passed\n";
    return 0;
}
