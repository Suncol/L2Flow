#pragma once

#include "l2flow/sdk/subscription_manifest.h"

#include "mdl_api.h"

#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>

namespace l2flow::sdk {

// Narrow lifecycle-facing SDK interfaces.  SetPassword() and
// SetReadBufferSize() are deliberately absent: Phase 1 cannot call APIs that
// are outside the reviewed endpoint contract.
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

// Opens the supplied path with O_NOFOLLOW, copies the regular file into a
// write/grow/shrink/seal-sealed memfd, and uses only that immutable snapshot
// for hash/ELF/runtime preflight and the retained final dlopen()/dlsym().
// No link-time reference to libmdl_api.so is required.
//
// Production callers must first pass RunVendorPreflight for the complete
// baseline+archive+library set. This loader deliberately repeats the
// library-only component gate on a fresh snapshot so the final mapping cannot
// be redirected after that complete gate.
//
// The returned factory, every manager, and every subscriber share ownership
// of the retained dynamic-library handle.  A failed vendor ReleaseRef() pins
// the mapping for process lifetime rather than unloading live vendor code.
[[nodiscard]] std::shared_ptr<SdkFactory> LoadApprovedSdkFactory(
    const std::filesystem::path& shared_library,
    std::string* error) noexcept;

}  // namespace l2flow::sdk
