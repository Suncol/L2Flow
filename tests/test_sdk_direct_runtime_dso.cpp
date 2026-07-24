#include "mdl_api.h"

#include <atomic>
#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

namespace {

namespace mdl = datayes::mdl;

enum class LifecycleEvent : int {
    kManagerCreated = 1,
    kSubscriberCreated = 2,
    kManagerShutdown = 3,
    kSubscriberReleased = 4,
    kManagerReleased = 5,
};

struct Subscription final {
    std::uint8_t service_id = 0U;
    std::uint16_t service_version = 0U;
    std::uint16_t message_id = 0U;
};

struct TestState final {
    std::mutex mutex;
    std::uint32_t sdk_version = 0U;
    int work_threads = 0;
    int io_threads = 0;
    int multithread_callback = -1;
    bool handler_was_nonnull = false;
    std::vector<Subscription> subscriptions;
    std::vector<LifecycleEvent> lifecycle;
};

TestState g_state;

void RecordLifecycle(LifecycleEvent event) {
    const std::lock_guard<std::mutex> lock(g_state.mutex);
    g_state.lifecycle.push_back(event);
}

class TestSubscriber final : public mdl::Subscriber {
public:
    TestSubscriber() = default;

    void AddRef() override {
        static_cast<void>(
            reference_count_.fetch_add(1, std::memory_order_relaxed));
    }

    int ReleaseRef() override {
        const int remaining =
            reference_count_.fetch_sub(1, std::memory_order_acq_rel) - 1;
        if (remaining == 0) {
            RecordLifecycle(LifecycleEvent::kSubscriberReleased);
            delete this;
        }
        return remaining;
    }

    void AddSubscription(
        std::uint8_t service_id,
        std::uint16_t service_version,
        std::uint16_t message_id) override {
        const std::lock_guard<std::mutex> lock(g_state.mutex);
        g_state.subscriptions.push_back(
            {service_id, service_version, message_id});
    }

    void AddSubscriptionByFieldValues(
        std::uint8_t,
        std::uint16_t,
        std::uint16_t,
        const char*,
        const char**,
        std::uint32_t) override {}

    void DelSubscription(
        std::uint8_t,
        std::uint16_t,
        std::uint16_t) override {}

    void DelSubscriptionByFieldValues(
        std::uint8_t,
        std::uint16_t,
        std::uint16_t,
        const char*,
        const char**,
        std::uint32_t) override {}

    void ClearSubscriptions() override {
        const std::lock_guard<std::mutex> lock(g_state.mutex);
        g_state.subscriptions.clear();
    }

    void SetHeartbeatInterval(std::uint32_t interval) override {
        heartbeat_interval_ = interval;
    }

    std::uint32_t GetHeartbeatInterval() override {
        return heartbeat_interval_;
    }

    void SetHeartbeatTimeout(std::uint32_t timeout) override {
        heartbeat_timeout_ = timeout;
    }

    std::uint32_t GetHeartbeatTimeout() override {
        return heartbeat_timeout_;
    }

    void SetUserName(const char* user_name) override {
        user_name_ = user_name == nullptr ? "" : user_name;
    }

    const char* GetUserName() override { return user_name_.c_str(); }

    void SetPassword(const char* password) override {
        password_ = password == nullptr ? "" : password;
    }

    const char* GetPassword() override { return password_.c_str(); }

    void SetMessageEncoding(mdl::MDLMessageEncoding encoding) override {
        encoding_ = encoding;
    }

    mdl::MDLMessageEncoding GetMessageEncoding() override {
        return encoding_;
    }

    void SetServerAddress(const char* address) override {
        server_address_ = address == nullptr ? "" : address;
    }

    const char* GetServerAddress() override {
        return server_address_.c_str();
    }

    bool PostRequest(mdl::MDLMessage*) override { return false; }
    bool SendRequest(mdl::MDLMessage*) override { return false; }
    const char* Connect() override { return nullptr; }
    void SetReadBufferSize(int size) override { read_buffer_size_ = size; }
    void SetSendMacAuth(bool send) override { send_mac_auth_ = send; }
    void EnableServerSelect(bool enable) override {
        server_select_enabled_ = enable;
    }
    void ReSubscribe() override {}
    const char* GetSubscription() override { return ""; }
    void EnableMergeMessage(bool enable) override {
        merge_message_enabled_ = enable;
    }

private:
    std::atomic<int> reference_count_{1};
    std::uint32_t heartbeat_interval_ = 0U;
    std::uint32_t heartbeat_timeout_ = 0U;
    std::string user_name_;
    std::string password_;
    mdl::MDLMessageEncoding encoding_ =
        static_cast<mdl::MDLMessageEncoding>(0);
    std::string server_address_;
    int read_buffer_size_ = 0;
    bool send_mac_auth_ = false;
    bool server_select_enabled_ = false;
    bool merge_message_enabled_ = false;
};

class TestManager final : public mdl::IOManager {
public:
    TestManager() = default;

    void AddRef() override {
        static_cast<void>(
            reference_count_.fetch_add(1, std::memory_order_relaxed));
    }

    int ReleaseRef() override {
        const int remaining =
            reference_count_.fetch_sub(1, std::memory_order_acq_rel) - 1;
        if (remaining == 0) {
            RecordLifecycle(LifecycleEvent::kManagerReleased);
            delete this;
        }
        return remaining;
    }

    mdl::AutoSubscriberPtr CreateAutoSubscriber(
        mdl::MessageHandlerBase*, bool) override {
        return mdl::AutoSubscriberPtr{};
    }

    bool RegisterMessage(
        std::uint8_t, std::uint16_t, std::uint16_t) override {
        return false;
    }

    void AsyncResponse(
        mdl::Publisher*, datayes::RefCounted*, const mdl::MDLMessage*)
        override {}
    void AsyncPublish(const mdl::MDLMessage*) override {}
    void SyncPublish(const mdl::MDLMessage*) override {}
    void EnableLog(const char*, bool) override {}

    void Shutdown() override {
        if (!shutdown_) {
            shutdown_ = true;
            RecordLifecycle(LifecycleEvent::kManagerShutdown);
        }
    }

    void SetWorkersProxy(mdl::IOManager*) override {}

protected:
    mdl::Publisher* _CreatePublisher(mdl::PublisherType) override {
        return nullptr;
    }

    mdl::Publisher* _GetPublisherByType(mdl::PublisherType) override {
        return nullptr;
    }

    mdl::Subscriber* _CreateSubscriber(
        mdl::MessageHandlerBase* handler,
        bool multithread_callback) override {
        {
            const std::lock_guard<std::mutex> lock(g_state.mutex);
            g_state.multithread_callback =
                multithread_callback ? 1 : 0;
            g_state.handler_was_nonnull = handler != nullptr;
            g_state.lifecycle.push_back(
                LifecycleEvent::kSubscriberCreated);
        }
        return new TestSubscriber();
    }

private:
    std::atomic<int> reference_count_{1};
    bool shutdown_ = false;
};

template <typename Value>
Value ReadState(Value TestState::*member) {
    const std::lock_guard<std::mutex> lock(g_state.mutex);
    return g_state.*member;
}

}  // namespace

extern "C" DTAPIEXPORT mdl::IOManager* DTAPIDLLCALL
DllCreateIOManager(
    std::uint32_t version, int work_threads, int io_threads) {
    {
        const std::lock_guard<std::mutex> lock(g_state.mutex);
        g_state.sdk_version = version;
        g_state.work_threads = work_threads;
        g_state.io_threads = io_threads;
        g_state.lifecycle.push_back(LifecycleEvent::kManagerCreated);
    }
    return new TestManager();
}

extern "C" DTAPIEXPORT void L2FlowTestDirectSdkReset() {
    const std::lock_guard<std::mutex> lock(g_state.mutex);
    g_state.sdk_version = 0U;
    g_state.work_threads = 0;
    g_state.io_threads = 0;
    g_state.multithread_callback = -1;
    g_state.handler_was_nonnull = false;
    g_state.subscriptions.clear();
    g_state.lifecycle.clear();
}

extern "C" DTAPIEXPORT std::uint32_t
L2FlowTestDirectSdkVersion() {
    return ReadState(&TestState::sdk_version);
}

extern "C" DTAPIEXPORT int L2FlowTestDirectSdkWorkThreads() {
    return ReadState(&TestState::work_threads);
}

extern "C" DTAPIEXPORT int L2FlowTestDirectSdkIoThreads() {
    return ReadState(&TestState::io_threads);
}

extern "C" DTAPIEXPORT int
L2FlowTestDirectSdkMultithreadCallback() {
    return ReadState(&TestState::multithread_callback);
}

extern "C" DTAPIEXPORT int L2FlowTestDirectSdkHandlerWasNonnull() {
    return ReadState(&TestState::handler_was_nonnull) ? 1 : 0;
}

extern "C" DTAPIEXPORT std::uint32_t
L2FlowTestDirectSdkSubscriptionCount() {
    const std::lock_guard<std::mutex> lock(g_state.mutex);
    return static_cast<std::uint32_t>(g_state.subscriptions.size());
}

extern "C" DTAPIEXPORT std::uint32_t
L2FlowTestDirectSdkSubscriptionServiceId(std::uint32_t index) {
    const std::lock_guard<std::mutex> lock(g_state.mutex);
    if (index >= g_state.subscriptions.size()) {
        return 0U;
    }
    return g_state.subscriptions[index].service_id;
}

extern "C" DTAPIEXPORT std::uint32_t
L2FlowTestDirectSdkSubscriptionServiceVersion(std::uint32_t index) {
    const std::lock_guard<std::mutex> lock(g_state.mutex);
    if (index >= g_state.subscriptions.size()) {
        return 0U;
    }
    return g_state.subscriptions[index].service_version;
}

extern "C" DTAPIEXPORT std::uint32_t
L2FlowTestDirectSdkSubscriptionMessageId(std::uint32_t index) {
    const std::lock_guard<std::mutex> lock(g_state.mutex);
    if (index >= g_state.subscriptions.size()) {
        return 0U;
    }
    return g_state.subscriptions[index].message_id;
}

extern "C" DTAPIEXPORT std::uint32_t
L2FlowTestDirectSdkLifecycleCount() {
    const std::lock_guard<std::mutex> lock(g_state.mutex);
    return static_cast<std::uint32_t>(g_state.lifecycle.size());
}

extern "C" DTAPIEXPORT int
L2FlowTestDirectSdkLifecycleAt(std::uint32_t index) {
    const std::lock_guard<std::mutex> lock(g_state.mutex);
    if (index >= g_state.lifecycle.size()) {
        return 0;
    }
    return static_cast<int>(g_state.lifecycle[index]);
}
