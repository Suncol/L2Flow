#pragma once

#include "l2flow/sdk/market_message_catalog_v1.h"

#include "mdl_api.h"

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

namespace l2flow::sdk {

// Narrow physical SDK lifecycle interfaces. They contain only object
// configuration, callback quiescence, and explicit release operations.
class SdkSubscriber {
public:
    virtual ~SdkSubscriber() = default;

    virtual void SetServerAddress(std::string_view address) = 0;
    virtual void SetUserName(std::string_view user_name) = 0;
    virtual void SetHeartbeatInterval(std::uint32_t seconds) = 0;
    virtual void SetHeartbeatTimeout(std::uint32_t seconds) = 0;
    virtual void SetMessageEncoding(
        datayes::mdl::MDLMessageEncoding encoding) = 0;
    virtual void EnableMergeMessage(bool enable) = 0;
    virtual void SetSendMacAuth(bool enable) = 0;
    virtual void EnableServerSelect(bool enable) = 0;
    virtual void AddSubscription(const MessageKey& key) = 0;

    // An empty result is success, matching the vendor's null-or-empty
    // Connect() convention.
    [[nodiscard]] virtual std::string Connect() = 0;

    // Explicit release makes shutdown order observable and lets the
    // production adapter contain ReleaseRef() failures.  Implementations must
    // make this operation idempotent.
    [[nodiscard]] virtual bool Release(std::string* error) noexcept = 0;
};

class SdkManager {
public:
    virtual ~SdkManager() = default;

    virtual void EnableLog(std::string_view prefix, bool console) = 0;
    [[nodiscard]] virtual std::unique_ptr<SdkSubscriber> CreateSubscriber(
        datayes::mdl::MessageHandlerBase* handler,
        bool multithread_callback) = 0;
    // Full callback-quiescence boundary. On return every OnMessage invocation
    // started by the SDK must have returned, and no queued or new invocation
    // may begin. The realtime owner also tracks callbacks at its handler entry
    // as a defensive lifecycle check before releasing Subscriber/IOManager.
    virtual void Shutdown() = 0;

    // See SdkSubscriber::Release().
    [[nodiscard]] virtual bool Release(std::string* error) noexcept = 0;
};

class SdkFactory {
public:
    virtual ~SdkFactory() = default;

    [[nodiscard]] virtual std::unique_ptr<SdkManager> Create(
        int work_threads,
        int io_threads) = 0;
};

}  // namespace l2flow::sdk
