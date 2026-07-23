#pragma once

#include "l2flow/common/sha256.h"
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

// Adapts one physical SDK factory to the four logical production ingress
// lanes.  Exactly one physical IOManager and one physical Subscriber are
// created. API/SYS control messages are delivered to all four logical
// handlers, while configured market messages are delivered only to the
// logical handler which owns that subscription key. Unknown market messages
// are not delivered. The fourth logical Connect() performs the one physical
// Connect() after all four configurations have been checked for equality.
//
// This is public so the lifecycle and routing contract can be tested with an
// in-process fake physical SDK. A null physical factory is rejected by
// returning null.
[[nodiscard]] std::shared_ptr<SdkFactory>
MakeProductionFanoutSdkFactory(
    std::shared_ptr<SdkFactory> physical_factory) noexcept;

// Opens the supplied path with O_NOFOLLOW, enforces the candidate-library size
// bound, copies the regular file into a write/grow/shrink/seal-sealed memfd,
// and uses only that immutable snapshot for ELF/dependency/symbol-version,
// compiled-ABI and runtime-lifecycle preflight plus the retained final
// dlopen()/dlsym(). No link-time reference to libmdl_api.so is required.
//
// Callers of the unpinned overload must first pass RunVendorPreflight for the
// complete baseline+archive+compatible-library set.  A production composition
// using the pinned overload instead verifies the baseline and archive without
// loading library code, then lets that overload bind digest verification,
// ELF/ABI/runtime preflight and final mapping to one sealed snapshot.
//
// The returned factory, every manager, and every subscriber share ownership
// of the retained dynamic-library handle.  A failed vendor ReleaseRef() pins
// the mapping for process lifetime rather than unloading live vendor code.
[[nodiscard]] std::shared_ptr<SdkFactory> LoadApprovedSdkFactory(
    const std::filesystem::path& shared_library,
    std::string* error) noexcept;

// Production provenance variant.  The digest is checked against the exact
// sealed memfd snapshot which subsequently passes runtime preflight and is
// handed to dlopen(); it is never computed from a separate pathname open.
// A zero digest is not a wildcard and is rejected like every other mismatch.
[[nodiscard]] std::shared_ptr<SdkFactory> LoadApprovedSdkFactoryPinned(
    const std::filesystem::path& shared_library,
    const l2flow::common::Sha256Digest& expected_sha256,
    std::string* error) noexcept;

// Loads the path selected by the operator without applying the approved
// archive/baseline, snapshot, size, digest, ELF/ABI or runtime-lifecycle gates
// used by LoadApprovedSdkFactory*.  The production manifest's only SDK check
// is that this path names an existing regular file.  dlopen() of that exact
// path and DllCreateIOManager resolution are necessary to use the selected
// library; they are not an SDK approval or identity-validation policy.  A
// successfully composed production factory retains the DSO mapping until
// process exit instead of invoking the vendor's unload finalizers via
// dlclose().
[[nodiscard]] std::shared_ptr<SdkFactory> LoadOperatorSelectedSdkFactory(
    const std::filesystem::path& shared_library,
    std::string* error) noexcept;

}  // namespace l2flow::sdk
