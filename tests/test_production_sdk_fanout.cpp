#include "l2flow/sdk/sdk_runtime.h"

#include <array>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace {

namespace mdl = datayes::mdl;
namespace sdk = l2flow::sdk;

void Require(bool condition, const char* message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << '\n';
        std::exit(1);
    }
}

struct PhysicalState final {
    std::size_t factory_create_calls = 0U;
    std::size_t enable_log_calls = 0U;
    std::size_t create_subscriber_calls = 0U;
    std::size_t connect_calls = 0U;
    std::size_t shutdown_calls = 0U;
    std::size_t subscriber_release_calls = 0U;
    std::size_t manager_release_calls = 0U;
    int work_threads = 0;
    int io_threads = 0;
    mdl::MessageHandlerBase* handler = nullptr;
    std::string server_address;
    std::string user_name;
    std::uint32_t heartbeat_interval = 0U;
    std::uint32_t heartbeat_timeout = 0U;
    mdl::MDLMessageEncoding message_encoding = mdl::MDLEID_UNDEFINED;
    bool merge_message = false;
    bool send_mac_auth = false;
    bool server_select = false;
    std::vector<sdk::MessageKey> subscriptions;
};

class PhysicalSubscriber final : public sdk::SdkSubscriber {
public:
    explicit PhysicalSubscriber(std::shared_ptr<PhysicalState> state)
        : state_(std::move(state)) {}

    void SetServerAddress(std::string_view address) override {
        state_->server_address = address;
    }
    void SetUserName(std::string_view user_name) override {
        state_->user_name = user_name;
    }
    void SetHeartbeatInterval(std::uint32_t seconds) override {
        state_->heartbeat_interval = seconds;
    }
    void SetHeartbeatTimeout(std::uint32_t seconds) override {
        state_->heartbeat_timeout = seconds;
    }
    void SetMessageEncoding(mdl::MDLMessageEncoding encoding) override {
        state_->message_encoding = encoding;
    }
    void EnableMergeMessage(bool enable) override {
        state_->merge_message = enable;
    }
    void SetSendMacAuth(bool enable) override {
        state_->send_mac_auth = enable;
    }
    void EnableServerSelect(bool enable) override {
        state_->server_select = enable;
    }
    void AddSubscription(const sdk::MessageKey& key) override {
        state_->subscriptions.push_back(key);
    }
    std::string Connect() override {
        ++state_->connect_calls;
        return {};
    }
    bool Release(std::string* error) noexcept override {
        if (!released_) {
            ++state_->subscriber_release_calls;
            released_ = true;
        }
        if (error != nullptr) {
            error->clear();
        }
        return true;
    }

private:
    std::shared_ptr<PhysicalState> state_;
    bool released_ = false;
};

class PhysicalManager final : public sdk::SdkManager {
public:
    explicit PhysicalManager(std::shared_ptr<PhysicalState> state)
        : state_(std::move(state)) {}

    void EnableLog(std::string_view, bool) override {
        ++state_->enable_log_calls;
    }
    std::unique_ptr<sdk::SdkSubscriber> CreateSubscriber(
        mdl::MessageHandlerBase* handler,
        bool multithread_callback) override {
        Require(handler != nullptr, "physical handler must be present");
        Require(!multithread_callback,
                "physical callback must be serialized");
        ++state_->create_subscriber_calls;
        state_->handler = handler;
        return std::make_unique<PhysicalSubscriber>(state_);
    }
    void Shutdown() override { ++state_->shutdown_calls; }
    bool Release(std::string* error) noexcept override {
        if (!released_) {
            ++state_->manager_release_calls;
            released_ = true;
        }
        if (error != nullptr) {
            error->clear();
        }
        return true;
    }

private:
    std::shared_ptr<PhysicalState> state_;
    bool released_ = false;
};

class PhysicalFactory final : public sdk::SdkFactory {
public:
    explicit PhysicalFactory(std::shared_ptr<PhysicalState> state)
        : state_(std::move(state)) {}

    std::unique_ptr<sdk::SdkManager> Create(
        int work_threads,
        int io_threads) override {
        ++state_->factory_create_calls;
        state_->work_threads = work_threads;
        state_->io_threads = io_threads;
        return std::make_unique<PhysicalManager>(state_);
    }

private:
    std::shared_ptr<PhysicalState> state_;
};

class TestMessage final : public mdl::MDLMessage {
public:
    explicit TestMessage(const sdk::MessageKey& key) {
        std::memset(&head_, 0, sizeof(head_));
        head_.HeadSize = static_cast<std::uint8_t>(sizeof(head_));
        head_.MessageSize = static_cast<std::uint32_t>(sizeof(head_));
        head_.MessageEncoding =
            static_cast<std::uint8_t>(mdl::MDLEID_BINARY);
        head_.ServiceID = key.service_id;
        head_.ServiceVersion = key.service_version;
        head_.MessageID = key.message_id;
    }

    void AddRef() override {}
    int ReleaseRef() override { return 1; }
    mdl::MDLMessageHead* GetHead() const override {
        return const_cast<mdl::MDLMessageHead*>(&head_);
    }
    char* GetBody() const override { return nullptr; }
    mdl::MDLMessage* _Copy() const override { return nullptr; }

private:
    mdl::MDLMessageHead head_{};
};

class CountingHandler final : public mdl::MessageHandlerBase {
public:
    void OnMessage(mdl::Subscriber*,
                   const mdl::MDLMessage* message) override {
        const mdl::MDLMessageHead* const head = message->GetHead();
        observed.push_back(sdk::MessageKey{
            head->ServiceID, head->ServiceVersion, head->MessageID});
    }

    std::vector<sdk::MessageKey> observed;
};

struct LogicalSet final {
    std::array<std::unique_ptr<sdk::SdkManager>, 4U> managers;
    std::array<std::unique_ptr<sdk::SdkSubscriber>, 4U> subscribers;
    std::array<CountingHandler, 4U> handlers;
};

void ConfigureSubscriber(sdk::SdkSubscriber* subscriber,
                         const sdk::IngressSpec& spec,
                         std::string_view address) {
    Require(subscriber != nullptr, "logical subscriber must be present");
    subscriber->SetServerAddress(address);
    subscriber->SetUserName("production-user");
    subscriber->SetHeartbeatInterval(10U);
    subscriber->SetHeartbeatTimeout(30U);
    subscriber->SetMessageEncoding(mdl::MDLEID_BINARY);
    subscriber->EnableMergeMessage(false);
    subscriber->SetSendMacAuth(false);
    subscriber->EnableServerSelect(false);
    for (const sdk::MessageKey& key : spec.required) {
        subscriber->AddSubscription(key);
    }
}

void MakeLogicalSet(const std::shared_ptr<sdk::SdkFactory>& factory,
                    LogicalSet* result,
                    std::string_view changed_address = {}) {
    Require(result != nullptr, "logical set output must be present");
    const auto& specs = sdk::AllIngressSpecs();
    Require(specs.size() == result->managers.size(),
            "built-in production lane count must be four");
    for (std::size_t index = 0U; index < result->managers.size(); ++index) {
        result->managers[index] = factory->Create(4, 1);
        Require(result->managers[index] != nullptr,
                "logical manager creation must succeed");
        result->managers[index]->EnableLog(
            "lane-" + std::to_string(index), false);
        result->subscribers[index] =
            result->managers[index]->CreateSubscriber(
                &result->handlers[index], false);
        ConfigureSubscriber(
            result->subscribers[index].get(),
            specs[index],
            index == 3U && !changed_address.empty()
                ? changed_address
                : std::string_view("127.0.0.1:9112"));
    }
}

void StopLogicalSet(LogicalSet* logical) {
    Require(logical != nullptr, "logical set must be present");
    for (auto& manager : logical->managers) {
        manager->Shutdown();
    }
    for (auto& subscriber : logical->subscribers) {
        std::string error;
        Require(subscriber->Release(&error),
                "logical Subscriber release must succeed");
        Require(error.empty(),
                "logical Subscriber release diagnostic must be empty");
        subscriber.reset();
    }
    for (auto& manager : logical->managers) {
        std::string error;
        Require(manager->Release(&error),
                "logical manager release must succeed");
        Require(error.empty(),
                "logical manager release diagnostic must be empty");
        manager.reset();
    }
}

void Emit(const std::shared_ptr<PhysicalState>& state,
          const sdk::MessageKey& key) {
    Require(state->handler != nullptr,
            "physical handler must exist before callback");
    TestMessage message(key);
    state->handler->OnMessage(nullptr, &message);
}

void TestExactFanoutAndLifecycle() {
    const auto state = std::make_shared<PhysicalState>();
    const auto physical = std::make_shared<PhysicalFactory>(state);
    const auto fanout = sdk::MakeProductionFanoutSdkFactory(physical);
    Require(fanout != nullptr, "fanout factory must be created");
    LogicalSet logical;
    MakeLogicalSet(fanout, &logical);

    Require(state->factory_create_calls == 1U,
            "exactly one physical IOManager must be created");
    Require(state->enable_log_calls == 1U,
            "exactly one physical log configuration must be applied");
    for (std::size_t index = 0U; index < 3U; ++index) {
        Require(logical.subscribers[index]->Connect().empty(),
                "first three logical Connect calls must register cleanly");
        Require(state->create_subscriber_calls == 0U,
                "physical Subscriber must not exist before fourth Connect");
        Require(state->connect_calls == 0U,
                "physical Connect must not run before fourth Connect");
    }
    Require(logical.subscribers[3]->Connect().empty(),
            "fourth logical Connect must perform physical Connect");
    Require(state->create_subscriber_calls == 1U,
            "exactly one physical Subscriber must be created");
    Require(state->connect_calls == 1U,
            "exactly one physical Connect must run");
    Require(state->subscriptions.size() == 5U,
            "physical Subscriber must own the five required messages");
    Require(state->server_address == "127.0.0.1:9112" &&
                state->user_name == "production-user" &&
                state->heartbeat_interval == 10U &&
                state->heartbeat_timeout == 30U &&
                state->message_encoding == mdl::MDLEID_BINARY &&
                !state->merge_message && !state->send_mac_auth &&
                !state->server_select,
            "physical connection configuration must match logical inputs");

    Emit(state, sdk::MessageKey{
                    static_cast<std::uint8_t>(mdl::MDLSID_MDL_API),
                    1U,
                    1U});
    Emit(state, sdk::MessageKey{
                    static_cast<std::uint8_t>(mdl::MDLSID_MDL_SYS),
                    1U,
                    1U});
    for (const CountingHandler& handler : logical.handlers) {
        Require(handler.observed.size() == 2U,
                "API/SYS controls must fan out to all four lanes");
    }

    const auto& specs = sdk::AllIngressSpecs();
    for (std::size_t lane = 0U; lane < specs.size(); ++lane) {
        for (const sdk::MessageKey& key : specs[lane].required) {
            std::array<std::size_t, 4U> before{};
            for (std::size_t index = 0U; index < before.size(); ++index) {
                before[index] = logical.handlers[index].observed.size();
            }
            Emit(state, key);
            for (std::size_t index = 0U; index < before.size(); ++index) {
                Require(
                    logical.handlers[index].observed.size() ==
                        before[index] + (index == lane ? 1U : 0U),
                    "market message must enter exactly its owning Raw lane");
            }
        }
    }

    std::array<std::size_t, 4U> before_unknown{};
    for (std::size_t index = 0U; index < before_unknown.size(); ++index) {
        before_unknown[index] = logical.handlers[index].observed.size();
    }
    Emit(state, sdk::MessageKey{6U, 101U, 53U});
    for (std::size_t index = 0U; index < before_unknown.size(); ++index) {
        Require(logical.handlers[index].observed.size() ==
                    before_unknown[index],
                "forbidden/unknown market messages must fail closed");
    }

    StopLogicalSet(&logical);
    Require(state->shutdown_calls == 1U,
            "physical IOManager Shutdown must run exactly once");
    Require(state->subscriber_release_calls == 1U,
            "physical Subscriber release must run exactly once");
    Require(state->manager_release_calls == 1U,
            "physical IOManager release must run exactly once");
}

void TestConfigurationMismatchFailsBeforePhysicalSubscriber() {
    const auto state = std::make_shared<PhysicalState>();
    const auto fanout = sdk::MakeProductionFanoutSdkFactory(
        std::make_shared<PhysicalFactory>(state));
    LogicalSet logical;
    MakeLogicalSet(fanout, &logical, "127.0.0.1:9113");
    for (std::size_t index = 0U; index < 3U; ++index) {
        Require(logical.subscribers[index]->Connect().empty(),
                "registration Connect must succeed before mismatch audit");
    }
    bool rejected = false;
    try {
        static_cast<void>(logical.subscribers[3]->Connect());
    } catch (const std::invalid_argument&) {
        rejected = true;
    }
    Require(rejected,
            "different physical endpoint configuration must fail closed");
    Require(state->create_subscriber_calls == 0U &&
                state->connect_calls == 0U,
            "mismatch must be rejected before physical Subscriber creation");
    StopLogicalSet(&logical);
    Require(state->shutdown_calls == 1U &&
                state->subscriber_release_calls == 0U &&
                state->manager_release_calls == 1U,
            "mismatch cleanup must still release the one physical manager");
}

void TestNullFactoryRejected() {
    Require(sdk::MakeProductionFanoutSdkFactory(nullptr) == nullptr,
            "null physical factory must be rejected");
}

}  // namespace

int main() {
    TestExactFanoutAndLifecycle();
    TestConfigurationMismatchFailsBeforePhysicalSubscriber();
    TestNullFactoryRejected();
    std::cout << "production SDK fanout tests passed\n";
    return 0;
}
