#include "l2flow/sdk/direct_sdk_runtime_v1.h"
#include "l2flow/sdk/production_subscription_v1.h"
#include "l2flow/sdk/sdk_runtime.h"

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <dlfcn.h>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace {

namespace mdl = datayes::mdl;
namespace sdk = l2flow::sdk;

enum class TestSdkLifecycleEvent : int {
    kManagerCreated = 1,
    kSubscriberCreated = 2,
    kManagerShutdown = 3,
    kSubscriberReleased = 4,
    kManagerReleased = 5,
};

class NoopHandler final : public mdl::MessageHandlerBase {
public:
    void OnMessage(mdl::Subscriber*, const mdl::MDLMessage*) override {}
};

class TestDsoHandle final {
public:
    explicit TestDsoHandle(const char* path) {
        ::dlerror();
        handle_ = ::dlopen(path, RTLD_NOW | RTLD_LOCAL);
        if (handle_ == nullptr) {
            const char* const detail = ::dlerror();
            throw std::runtime_error(
                std::string("opening test SDK inspection handle failed: ") +
                (detail == nullptr ? "<no dlerror>" : detail));
        }
    }

    ~TestDsoHandle() {
        if (handle_ != nullptr) {
            static_cast<void>(::dlclose(handle_));
        }
    }

    TestDsoHandle(const TestDsoHandle&) = delete;
    TestDsoHandle& operator=(const TestDsoHandle&) = delete;

    template <typename Function>
    [[nodiscard]] Function Load(const char* name) const {
        ::dlerror();
        void* const raw = ::dlsym(handle_, name);
        const char* const detail = ::dlerror();
        if (raw == nullptr || detail != nullptr) {
            throw std::runtime_error(
                std::string("loading test SDK inspection symbol failed: ") +
                name + ": " +
                (detail == nullptr ? "<null symbol>" : detail));
        }

        static_assert(sizeof(Function) == sizeof(raw));
        Function function = nullptr;
        std::memcpy(&function, &raw, sizeof(function));
        return function;
    }

private:
    void* handle_ = nullptr;
};

struct TestDsoApi final {
    using ResetFunction = void (*)();
    using UnsignedFunction = std::uint32_t (*)();
    using IntFunction = int (*)();
    using IndexedUnsignedFunction = std::uint32_t (*)(std::uint32_t);
    using IndexedIntFunction = int (*)(std::uint32_t);

    explicit TestDsoApi(const TestDsoHandle& dso)
        : reset(dso.Load<ResetFunction>("L2FlowTestDirectSdkReset")),
          sdk_version(dso.Load<UnsignedFunction>(
              "L2FlowTestDirectSdkVersion")),
          work_threads(dso.Load<IntFunction>(
              "L2FlowTestDirectSdkWorkThreads")),
          io_threads(dso.Load<IntFunction>(
              "L2FlowTestDirectSdkIoThreads")),
          multithread_callback(dso.Load<IntFunction>(
              "L2FlowTestDirectSdkMultithreadCallback")),
          handler_was_nonnull(dso.Load<IntFunction>(
              "L2FlowTestDirectSdkHandlerWasNonnull")),
          subscription_count(dso.Load<UnsignedFunction>(
              "L2FlowTestDirectSdkSubscriptionCount")),
          subscription_service_id(dso.Load<IndexedUnsignedFunction>(
              "L2FlowTestDirectSdkSubscriptionServiceId")),
          subscription_service_version(
              dso.Load<IndexedUnsignedFunction>(
                  "L2FlowTestDirectSdkSubscriptionServiceVersion")),
          subscription_message_id(dso.Load<IndexedUnsignedFunction>(
              "L2FlowTestDirectSdkSubscriptionMessageId")),
          lifecycle_count(dso.Load<UnsignedFunction>(
              "L2FlowTestDirectSdkLifecycleCount")),
          lifecycle_at(dso.Load<IndexedIntFunction>(
              "L2FlowTestDirectSdkLifecycleAt")) {}

    ResetFunction reset;
    UnsignedFunction sdk_version;
    IntFunction work_threads;
    IntFunction io_threads;
    IntFunction multithread_callback;
    IntFunction handler_was_nonnull;
    UnsignedFunction subscription_count;
    IndexedUnsignedFunction subscription_service_id;
    IndexedUnsignedFunction subscription_service_version;
    IndexedUnsignedFunction subscription_message_id;
    UnsignedFunction lifecycle_count;
    IndexedIntFunction lifecycle_at;
};

class RecordingSubscriber final : public sdk::SdkSubscriber {
public:
    void SetServerAddress(std::string_view) override {}
    void SetUserName(std::string_view) override {}
    void SetHeartbeatInterval(std::uint32_t) override {}
    void SetHeartbeatTimeout(std::uint32_t) override {}
    void SetMessageEncoding(mdl::MDLMessageEncoding) override {}
    void EnableMergeMessage(bool) override {}
    void SetSendMacAuth(bool) override {}
    void EnableServerSelect(bool) override {}

    void AddSubscription(const sdk::MessageKey& key) override {
        keys.push_back(key);
    }

    [[nodiscard]] std::string Connect() override { return {}; }

    [[nodiscard]] bool Release(std::string* error) noexcept override {
        if (error != nullptr) {
            error->clear();
        }
        return true;
    }

    std::vector<sdk::MessageKey> keys;
};

struct TestContext final {
    void Expect(bool condition, std::string_view description) {
        if (condition) {
            return;
        }
        ++failures;
        std::cerr << "FAIL: " << description << '\n';
    }

    int failures = 0;
};

void CheckProductionSubscriptions(TestContext* test) {
    const std::vector<sdk::MessageKey> expected = {
        {4U, 101U, 24U},
        {6U, 101U, 33U},
        {6U, 101U, 36U},
    };
    const sdk::MessageKey forbidden{6U, 101U, 53U};

    const auto& configured = sdk::ProductionSubscriptionKeysV1();
    test->Expect(
        std::vector<sdk::MessageKey>(
            configured.begin(), configured.end()) == expected,
        "the production catalog is exactly the three required tuples");
    test->Expect(
        std::all_of(
            expected.begin(),
            expected.end(),
            sdk::IsRequiredProductionSubscriptionV1),
        "all three catalog tuples are classified as required");
    test->Expect(
        !sdk::IsRequiredProductionSubscriptionV1(forbidden),
        "6.101.53 is not a required production tuple");
    test->Expect(
        sdk::IsForbiddenProductionSubscriptionV1(forbidden),
        "6.101.53 is explicitly forbidden");

    RecordingSubscriber subscriber;
    sdk::AddProductionSubscriptionsV1(subscriber);
    test->Expect(
        subscriber.keys == expected,
        "one Subscriber receives the three tuples once in stable order");
    test->Expect(
        std::find(
            subscriber.keys.begin(),
            subscriber.keys.end(),
            forbidden) == subscriber.keys.end(),
        "the subscription helper never emits 6.101.53");
}

void CheckDirectLoaderRejectsInvalidPaths(TestContext* test) {
    std::string error;
    const std::shared_ptr<sdk::SdkFactory> empty =
        sdk::LoadSdkFactoryFromPath({}, &error);
    test->Expect(empty == nullptr, "an empty SDK path is rejected");
    test->Expect(!error.empty(), "an empty SDK path has a diagnostic");

    error.clear();
    const std::shared_ptr<sdk::SdkFactory> missing =
        sdk::LoadSdkFactoryFromPath(
            "/definitely/not/a/real/l2flow/vendor-sdk.so", &error);
    test->Expect(missing == nullptr, "dlopen failure is reported");
    test->Expect(
        error.find("dlopen SDK path failed") != std::string::npos,
        "dlopen failure identifies the direct load stage");
}

void CheckDirectLoaderSuccessPath(
    TestContext* test, const char* shared_library_path) {
    std::string error = "stale error";
    const std::shared_ptr<sdk::SdkFactory> factory =
        sdk::LoadSdkFactoryFromPath(shared_library_path, &error);
    test->Expect(
        factory != nullptr,
        "the operator-selected test SDK is loaded by path");
    test->Expect(
        error.empty(), "a successful direct SDK load clears diagnostics");
    if (factory == nullptr) {
        return;
    }

    try {
        // Open a second reference only after the production loader succeeds.
        // This exposes test-only observations without bypassing the loader's
        // real dlopen/dlsym path.
        const TestDsoHandle inspection_handle(shared_library_path);
        const TestDsoApi inspection(inspection_handle);
        inspection.reset();

        constexpr int kWorkThreads = 3;
        constexpr int kIoThreads = 2;
        std::unique_ptr<sdk::SdkManager> manager =
            factory->Create(kWorkThreads, kIoThreads);
        test->Expect(manager != nullptr, "DllCreateIOManager creates a manager");
        if (manager == nullptr) {
            return;
        }

        NoopHandler handler;
        std::unique_ptr<sdk::SdkSubscriber> subscriber =
            manager->CreateSubscriber(&handler, false);
        test->Expect(
            subscriber != nullptr,
            "the direct manager creates one physical Subscriber");
        if (subscriber == nullptr) {
            manager->Shutdown();
            static_cast<void>(manager->Release(nullptr));
            return;
        }

        sdk::AddProductionSubscriptionsV1(*subscriber);

        test->Expect(
            inspection.sdk_version() == mdl::MDL_VERSION,
            "DllCreateIOManager receives the SDK header version");
        test->Expect(
            inspection.work_threads() == kWorkThreads &&
                inspection.io_threads() == kIoThreads,
            "the requested manager thread counts cross the DSO boundary");
        test->Expect(
            inspection.multithread_callback() == 0,
            "the physical Subscriber is created with multithread_callback=false");
        test->Expect(
            inspection.handler_was_nonnull() == 1,
            "the physical Subscriber receives the application handler");

        const auto& expected = sdk::ProductionSubscriptionKeysV1();
        test->Expect(
            inspection.subscription_count() == expected.size(),
            "the DSO observes exactly three subscription calls");
        for (std::uint32_t index = 0U; index < expected.size(); ++index) {
            const sdk::MessageKey& key = expected[index];
            test->Expect(
                inspection.subscription_service_id(index) ==
                        key.service_id &&
                    inspection.subscription_service_version(index) ==
                        key.service_version &&
                    inspection.subscription_message_id(index) ==
                        key.message_id,
                "the direct adapter forwards each exact subscription tuple");
        }

        std::string release_error;
        test->Expect(
            !subscriber->Release(&release_error) &&
                !release_error.empty(),
            "Subscriber release is rejected before manager Shutdown");

        manager->Shutdown();
        release_error = "stale error";
        test->Expect(
            subscriber->Release(&release_error) && release_error.empty(),
            "Subscriber releases successfully after manager Shutdown");
        subscriber.reset();

        release_error = "stale error";
        test->Expect(
            manager->Release(&release_error) && release_error.empty(),
            "IOManager releases successfully after its Subscriber");
        manager.reset();

        constexpr std::array<TestSdkLifecycleEvent, 5U>
            kExpectedLifecycle{
                TestSdkLifecycleEvent::kManagerCreated,
                TestSdkLifecycleEvent::kSubscriberCreated,
                TestSdkLifecycleEvent::kManagerShutdown,
                TestSdkLifecycleEvent::kSubscriberReleased,
                TestSdkLifecycleEvent::kManagerReleased,
            };
        test->Expect(
            inspection.lifecycle_count() == kExpectedLifecycle.size(),
            "the test SDK observes exactly one complete lifecycle");
        for (std::uint32_t index = 0U;
             index < kExpectedLifecycle.size();
             ++index) {
            test->Expect(
                inspection.lifecycle_at(index) ==
                    static_cast<int>(kExpectedLifecycle[index]),
                "lifecycle order is create manager, create Subscriber, "
                "Shutdown, release Subscriber, release manager");
        }
    } catch (const std::exception& exception) {
        test->Expect(false, exception.what());
    }
}

}  // namespace

int main(int argc, char** argv) {
    TestContext test;
    CheckProductionSubscriptions(&test);
    CheckDirectLoaderRejectsInvalidPaths(&test);
    test.Expect(
        argc == 2,
        "the test runner supplies the test-only SDK shared-library path");
    if (argc == 2) {
        CheckDirectLoaderSuccessPath(&test, argv[1]);
    }

    if (test.failures != 0) {
        return 1;
    }
    std::cout
        << "direct SDK dlopen/dlsym, single-subscriber three-key "
           "subscription, and ordered release passed\n";
    return 0;
}
