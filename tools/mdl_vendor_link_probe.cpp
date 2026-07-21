#include "mdl_api.h"

#include <cstdint>
#include <iostream>
#include <string_view>

namespace mdl = datayes::mdl;

namespace l2flow_vendor_probe {

class NoopHandler final : public mdl::MessageHandlerBase {
public:
    void OnMessage(
        mdl::Subscriber*,
        const mdl::MDLMessage*) override {}
};

// This surface is deliberately compiled and linked into the probe but is not
// exposed as a command: real endpoints and credentials belong to the four
// production runners. It makes SDK upgrades compile every reviewed
// connection operation instead of proving only the factory symbol.
const char* CompileReviewedConnectionSurface(
    mdl::IOManager& manager,
    NoopHandler& handler,
    const char* endpoint,
    const char* credential,
    std::uint8_t service_id,
    std::uint16_t service_version,
    std::uint16_t message_id) {
    mdl::SubscriberPtr subscriber =
        manager.CreateSubscriber(&handler, false);
    if (subscriber.IsNull()) {
        return "CreateSubscriber returned null";
    }
    subscriber->SetServerAddress(endpoint);
    subscriber->SetUserName(credential);
    subscriber->SetHeartbeatInterval(10U);
    subscriber->SetHeartbeatTimeout(30U);
    subscriber->SetMessageEncoding(mdl::MDLEID_BINARY);
    subscriber->EnableMergeMessage(false);
    subscriber->SetSendMacAuth(false);
    subscriber->EnableServerSelect(false);
    subscriber->AddSubscription(
        service_id, service_version, message_id);
    return subscriber->Connect();
}

}  // namespace l2flow_vendor_probe

namespace {

int RunFactoryProbe() noexcept {
    mdl::IOManager* manager = nullptr;
    try {
        manager = mdl::DllCreateIOManager(
            static_cast<std::uint32_t>(mdl::MDL_VERSION), 1, 1);
    } catch (...) {
        std::cerr
            << "DllCreateIOManager threw across the vendor ABI\n";
        return 1;
    }
    if (manager == nullptr) {
        std::cerr
            << "DllCreateIOManager returned null for MDL_VERSION\n";
        return 1;
    }

    try {
        manager->Shutdown();
    } catch (...) {
        // Do not call ReleaseRef after an opaque throwing Shutdown: thread
        // convergence is no longer proven. Process exit is the safe boundary.
        std::cerr << "IOManager::Shutdown threw across the vendor ABI\n";
        return 1;
    }
    try {
        const int remaining = manager->ReleaseRef();
        if (remaining != 0) {
            std::cerr
                << "IOManager::ReleaseRef returned "
                << remaining << " instead of zero\n";
            return 1;
        }
    } catch (...) {
        std::cerr
            << "IOManager::ReleaseRef threw across the vendor ABI\n";
        return 1;
    }

    std::cout
        << "vendor factory link and current-version lifecycle passed\n";
    return 0;
}

}  // namespace

int main(int argc, char* argv[]) {
    if (argc == 2 &&
        std::string_view(argv[1]) == "--help") {
        std::cout
            << "Usage: mdl-vendor-link-probe "
               "--factory-probe-after-full-preflight\n"
            << "The target also compiles the reviewed CreateSubscriber, "
               "configuration, subscription, and Connect surface without "
               "embedding or executing an endpoint.\n"
            << "The factory probe must only be run after mdl_abi_preflight "
               "passes for the same approved artifact.\n";
        return 0;
    }
    if (argc == 2 &&
        std::string_view(argv[1]) ==
            "--factory-probe-after-full-preflight") {
        return RunFactoryProbe();
    }
    std::cerr
        << "refusing vendor calls without the explicit post-preflight "
           "factory-probe option\n";
    return 2;
}
