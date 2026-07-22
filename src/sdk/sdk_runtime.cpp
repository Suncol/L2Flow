#include "l2flow/sdk/sdk_runtime.h"

#include "l2flow/baseline/vendor_baseline.h"
#include "l2flow/common/sealed_file_snapshot.h"
#include "l2flow/common/sha256.h"

#include <algorithm>
#include <atomic>
#include <cstring>
#include <dlfcn.h>
#include <exception>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>

namespace l2flow::sdk {
namespace {

namespace mdl = datayes::mdl;

void SetError(std::string* error, std::string message) noexcept {
    if (error == nullptr) {
        return;
    }
    try {
        *error = std::move(message);
    } catch (...) {
    }
}

void SetErrorLiteral(std::string* error, const char* message) noexcept {
    if (error == nullptr) {
        return;
    }
    try {
        *error = message;
    } catch (...) {
    }
}

std::string CopyCStringArgument(std::string_view value,
                                const char* argument_name) {
    if (value.find('\0') != std::string_view::npos) {
        throw std::invalid_argument(
            std::string(argument_name) + " contains NUL");
    }
    return std::string(value);
}

class LoadedSdk final {
public:
    using CreateFunction =
        mdl::IOManager* (*)(std::uint32_t, int, int);

    LoadedSdk(common::SealedFileSnapshot snapshot,
              void* handle,
              CreateFunction create_function) noexcept
        : snapshot_(std::move(snapshot)),
          handle_(handle),
          create_(create_function) {}

    ~LoadedSdk() {
        if (handle_ != nullptr &&
            !pinned_.load(std::memory_order_acquire)) {
            static_cast<void>(::dlclose(handle_));
        }
    }

    LoadedSdk(const LoadedSdk&) = delete;
    LoadedSdk& operator=(const LoadedSdk&) = delete;

    [[nodiscard]] CreateFunction create() const noexcept {
        return create_;
    }

    // If the SDK reports that an object remains alive, dlclose would turn a
    // later virtual call/destructor into unmapped-code execution.
    void PinForProcessLifetime() noexcept {
        pinned_.store(true, std::memory_order_release);
    }

private:
    common::SealedFileSnapshot snapshot_;
    void* handle_;
    CreateFunction create_;
    std::atomic<bool> pinned_{false};
};

struct ShutdownState final {
    std::atomic<bool> complete{false};
};

enum class ReleaseCountExpectation {
    Zero,
    Positive,
    NonNegative,
};

bool ReleaseVendorObject(datayes::RefCounted* object,
                         const std::shared_ptr<LoadedSdk>& library,
                         const char* object_name,
                         ReleaseCountExpectation expectation,
                         std::string* error) noexcept {
    if (object == nullptr) {
        if (error != nullptr) {
            error->clear();
        }
        return true;
    }

    int remaining = 0;
    try {
        remaining = object->ReleaseRef();
    } catch (const std::exception& exception) {
        library->PinForProcessLifetime();
        try {
            SetError(
                error,
                std::string(object_name) +
                    " ReleaseRef threw across the SDK ABI: " +
                    exception.what());
        } catch (...) {
            SetErrorLiteral(
                error, "vendor ReleaseRef threw across the SDK ABI");
        }
        return false;
    } catch (...) {
        library->PinForProcessLifetime();
        SetErrorLiteral(
            error,
            "vendor ReleaseRef threw an unknown exception across the "
            "SDK ABI");
        return false;
    }
    bool expected_count = false;
    switch (expectation) {
    case ReleaseCountExpectation::Zero:
        expected_count = remaining == 0;
        break;
    case ReleaseCountExpectation::Positive:
        expected_count = remaining > 0;
        break;
    case ReleaseCountExpectation::NonNegative:
        expected_count = remaining >= 0;
        break;
    }
    if (expected_count) {
        if (error != nullptr) {
            error->clear();
        }
        return true;
    }

    library->PinForProcessLifetime();
    try {
        SetError(
            error,
            std::string(object_name) +
                " ReleaseRef returned an unexpected reference count: " +
                std::to_string(remaining));
    } catch (...) {
        SetErrorLiteral(
            error,
            "vendor ReleaseRef returned an unexpected reference count");
    }
    return false;
}

class DynamicSdkSubscriber final : public SdkSubscriber {
public:
    DynamicSdkSubscriber(std::shared_ptr<LoadedSdk> library,
                         std::shared_ptr<ShutdownState> shutdown,
                         mdl::Subscriber* subscriber) noexcept
        : library_(std::move(library)),
          shutdown_(std::move(shutdown)),
          subscriber_(subscriber) {}

    ~DynamicSdkSubscriber() override {
        if (!Release(nullptr)) {
            std::terminate();
        }
    }

    void SetServerAddress(std::string_view address) override {
        const std::string copied =
            CopyCStringArgument(address, "server address");
        subscriber_->SetServerAddress(copied.c_str());
    }

    void SetUserName(std::string_view user_name) override {
        const std::string copied =
            CopyCStringArgument(user_name, "user name");
        subscriber_->SetUserName(copied.c_str());
    }

    void SetHeartbeatInterval(std::uint32_t seconds) override {
        subscriber_->SetHeartbeatInterval(seconds);
    }

    void SetHeartbeatTimeout(std::uint32_t seconds) override {
        subscriber_->SetHeartbeatTimeout(seconds);
    }

    void SetMessageEncoding(
        mdl::MDLMessageEncoding encoding) override {
        subscriber_->SetMessageEncoding(encoding);
    }

    void EnableMergeMessage(bool enable) override {
        subscriber_->EnableMergeMessage(enable);
    }

    void SetSendMacAuth(bool enable) override {
        subscriber_->SetSendMacAuth(enable);
    }

    void EnableServerSelect(bool enable) override {
        subscriber_->EnableServerSelect(enable);
    }

    void AddSubscription(const MessageKey& key) override {
        subscriber_->AddSubscription(
            key.service_id, key.service_version, key.message_id);
    }

    std::string Connect() override {
        const char* const result = subscriber_->Connect();
        if (result == nullptr) {
            return {};
        }
        constexpr std::size_t kMaximumConnectErrorBytes = 4096U;
        const std::size_t length =
            ::strnlen(result, kMaximumConnectErrorBytes + 1U);
        if (length > kMaximumConnectErrorBytes) {
            throw std::runtime_error(
                "SDK Connect error exceeds 4096 bytes or lacks NUL");
        }
        return std::string(result, length);
    }

    bool Release(std::string* error) noexcept override {
        if (subscriber_ == nullptr) {
            if (error != nullptr) {
                error->clear();
            }
            return true;
        }
        if (!shutdown_->complete.load(std::memory_order_acquire)) {
            SetErrorLiteral(
                error,
                "Subscriber release requires a completed IOManager "
                "Shutdown");
            return false;
        }
        mdl::Subscriber* const object =
            std::exchange(subscriber_, nullptr);
        return ReleaseVendorObject(
            object,
            library_,
            "Subscriber",
            ReleaseCountExpectation::NonNegative,
            error);
    }

private:
    // Declared before subscriber_ so the mapping outlives every call through
    // subscriber_, including explicit/destructor release.
    std::shared_ptr<LoadedSdk> library_;
    std::shared_ptr<ShutdownState> shutdown_;
    mdl::Subscriber* subscriber_;
};

class DynamicSdkManager final : public SdkManager {
public:
    DynamicSdkManager(std::shared_ptr<LoadedSdk> library,
                      mdl::IOManager* manager)
        : library_(std::move(library)),
          shutdown_(std::make_shared<ShutdownState>()),
          manager_(manager) {}

    ~DynamicSdkManager() override {
        if (!Release(nullptr)) {
            std::terminate();
        }
    }

    void EnableLog(std::string_view prefix, bool console) override {
        const std::string copied =
            CopyCStringArgument(prefix, "SDK log prefix");
        manager_->EnableLog(copied.c_str(), console);
    }

    std::unique_ptr<SdkSubscriber> CreateSubscriber(
        mdl::MessageHandlerBase* handler,
        bool multithread_callback) override {
        mdl::SubscriberPtr temporary =
            manager_->CreateSubscriber(handler, multithread_callback);
        if (temporary.IsNull()) {
            return nullptr;
        }

        // Duplicate one reference for the adapter, leaving the temporary's
        // normal destructor to release its own reference.
        mdl::Subscriber* const raw = temporary.Duplicate();
        try {
            return std::make_unique<DynamicSdkSubscriber>(
                library_, shutdown_, raw);
        } catch (...) {
            std::string ignored;
            if (!ReleaseVendorObject(
                    raw,
                    library_,
                    "Subscriber",
                    ReleaseCountExpectation::Positive,
                    &ignored)) {
                // The vendor object already contains the application's raw
                // handler pointer. If relinquishing the duplicated reference
                // cannot be proved, unwinding that handler would be unsafe.
                std::terminate();
            }
            throw;
        }
    }

    void Shutdown() override {
        if (shutdown_->complete.load(std::memory_order_acquire)) {
            return;
        }
        manager_->Shutdown();
        shutdown_->complete.store(true, std::memory_order_release);
    }

    bool Release(std::string* error) noexcept override {
        if (manager_ == nullptr) {
            if (error != nullptr) {
                error->clear();
            }
            return true;
        }
        if (!shutdown_->complete.load(std::memory_order_acquire)) {
            SetErrorLiteral(
                error,
                "IOManager release requires a completed Shutdown");
            return false;
        }
        mdl::IOManager* const object =
            std::exchange(manager_, nullptr);
        return ReleaseVendorObject(
            object,
            library_,
            "IOManager",
            ReleaseCountExpectation::Zero,
            error);
    }

private:
    std::shared_ptr<LoadedSdk> library_;
    std::shared_ptr<ShutdownState> shutdown_;
    mdl::IOManager* manager_;
};

class DynamicSdkFactory final : public SdkFactory {
public:
    explicit DynamicSdkFactory(
        std::shared_ptr<LoadedSdk> library) noexcept
        : library_(std::move(library)) {}

    std::unique_ptr<SdkManager> Create(
        int work_threads,
        int io_threads) override {
        if (work_threads <= 0 || io_threads <= 0) {
            throw std::invalid_argument(
                "SDK thread counts must be positive");
        }

        mdl::IOManager* raw = nullptr;
        try {
            raw = library_->create()(
                mdl::MDL_VERSION,
                work_threads,
                io_threads);
        } catch (...) {
            // The opaque factory may have started threads or retained code
            // before throwing without returning an object we can shut down.
            library_->PinForProcessLifetime();
            throw;
        }
        if (raw == nullptr) {
            // There is no returned object through which to prove that an
            // approved-version factory cleaned up any partial global state
            // or threads. Conservatively retain the loaded code.
            library_->PinForProcessLifetime();
            return nullptr;
        }
        try {
            return std::make_unique<DynamicSdkManager>(
                library_, raw);
        } catch (...) {
            try {
                raw->Shutdown();
            } catch (...) {
                std::terminate();
            }
            std::string ignored;
            if (!ReleaseVendorObject(
                    raw,
                    library_,
                    "IOManager",
                    ReleaseCountExpectation::Zero,
                    &ignored)) {
                std::terminate();
            }
            throw;
        }
    }

private:
    std::shared_ptr<LoadedSdk> library_;
};

class DynamicHandle final {
public:
    explicit DynamicHandle(void* value) noexcept : value_(value) {}
    ~DynamicHandle() {
        if (value_ != nullptr) {
            static_cast<void>(::dlclose(value_));
        }
    }

    DynamicHandle(const DynamicHandle&) = delete;
    DynamicHandle& operator=(const DynamicHandle&) = delete;

    [[nodiscard]] void* get() const noexcept { return value_; }

    [[nodiscard]] void* release() noexcept {
        return std::exchange(value_, nullptr);
    }

private:
    void* value_;
};

}  // namespace

namespace {

std::shared_ptr<SdkFactory> LoadApprovedSdkFactoryImpl(
    const std::filesystem::path& shared_library,
    const l2flow::common::Sha256Digest* expected_sha256,
    std::string* error) noexcept {
    try {
        if (error != nullptr) {
            error->clear();
        }
        if (shared_library.empty() ||
            shared_library.native().find('\0') != std::string::npos) {
            SetError(error, "SDK shared-library path is empty or contains NUL");
            return nullptr;
        }
        if (expected_sha256 != nullptr &&
            std::none_of(
                expected_sha256->begin(),
                expected_sha256->end(),
                [](std::byte value) { return value != std::byte{0U}; })) {
            SetErrorLiteral(
                error, "sealed SDK snapshot SHA-256 pin is zero");
            return nullptr;
        }

        common::SealedFileSnapshot snapshot;
        if (!common::CreateSealedFileSnapshot(
                shared_library,
                &snapshot,
                error,
                std::nullopt,
                baseline::kMaximumSdkSharedLibraryBytes)) {
            return nullptr;
        }

        if (expected_sha256 != nullptr) {
            common::Sha256Digest observed_sha256{};
            if (!common::ComputeFileSha256ForOpenFd(
                    snapshot.fd(),
                    &observed_sha256,
                    error,
                    baseline::kMaximumSdkSharedLibraryBytes)) {
                return nullptr;
            }
            if (observed_sha256 != *expected_sha256) {
                SetErrorLiteral(
                    error,
                    "sealed SDK snapshot SHA-256 does not match the deployment pin");
                return nullptr;
            }
        }

        const std::string fixed_path = snapshot.proc_fd_path();
        const baseline::PreflightReport preflight =
            baseline::
                RunApprovedLibraryRuntimePreflightForSealedSnapshotFd(
                    snapshot.fd());
        if (!preflight.passed()) {
            SetError(
                error,
                "approved SDK runtime preflight failed: " +
                    baseline::PreflightReportJson(
                        preflight, true));
            return nullptr;
        }

        // Load the exact sealed memfd that passed every preflight operation,
        // never the caller's mutable pathname or source inode.
        ::dlerror();
        void* const raw_handle =
            ::dlopen(fixed_path.c_str(), RTLD_NOW | RTLD_LOCAL);
        if (raw_handle == nullptr) {
            const char* const detail = ::dlerror();
            SetError(
                error,
                std::string("dlopen approved SDK fd failed: ") +
                    (detail == nullptr ? "<no dlerror>" : detail));
            return nullptr;
        }
        DynamicHandle handle(raw_handle);

        ::dlerror();
        void* const raw_symbol =
            ::dlsym(handle.get(), "DllCreateIOManager");
        const char* const symbol_error = ::dlerror();
        if (symbol_error != nullptr || raw_symbol == nullptr) {
            SetError(
                error,
                std::string("dlsym DllCreateIOManager failed: ") +
                    (symbol_error == nullptr
                         ? "<null symbol without dlerror>"
                         : symbol_error));
            return nullptr;
        }

        using CreateFunction = LoadedSdk::CreateFunction;
        static_assert(sizeof(CreateFunction) == sizeof(raw_symbol));
        CreateFunction create = nullptr;
        std::memcpy(&create, &raw_symbol, sizeof(create));
        if (create == nullptr) {
            SetError(error, "DllCreateIOManager resolved to a null function");
            return nullptr;
        }

        // A new-expression allocates before evaluating its constructor
        // arguments. Therefore an allocation failure cannot occur after the
        // RAII guards have released the snapshot or dlopen handle. The
        // shared_ptr constructor deletes the object if allocating its control
        // block subsequently fails.
        auto loaded = std::shared_ptr<LoadedSdk>(
            new LoadedSdk(
                std::move(snapshot), handle.release(), create));
        return std::make_shared<DynamicSdkFactory>(
            std::move(loaded));
    } catch (const std::exception& exception) {
        try {
            SetError(
                error,
                std::string("loading approved SDK failed: ") +
                    exception.what());
        } catch (...) {
            SetErrorLiteral(error, "loading approved SDK failed");
        }
        return nullptr;
    } catch (...) {
        SetErrorLiteral(
            error,
            "loading approved SDK failed with an unknown exception");
        return nullptr;
    }
}

}  // namespace

std::shared_ptr<SdkFactory> LoadApprovedSdkFactory(
    const std::filesystem::path& shared_library,
    std::string* error) noexcept {
    return LoadApprovedSdkFactoryImpl(shared_library, nullptr, error);
}

std::shared_ptr<SdkFactory> LoadApprovedSdkFactoryPinned(
    const std::filesystem::path& shared_library,
    const l2flow::common::Sha256Digest& expected_sha256,
    std::string* error) noexcept {
    return LoadApprovedSdkFactoryImpl(
        shared_library, &expected_sha256, error);
}

}  // namespace l2flow::sdk
