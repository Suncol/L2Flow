#include "l2flow/sdk/direct_sdk_runtime_v1.h"

#include "l2flow/sdk/sdk_runtime.h"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <dlfcn.h>
#include <exception>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
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

void ClearError(std::string* error) noexcept {
    if (error == nullptr) {
        return;
    }
    try {
        error->clear();
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
    void* value_ = nullptr;
};

class DirectSdkLibraryV1 final {
public:
    using CreateFunction =
        mdl::IOManager* (*)(std::uint32_t, int, int);

    DirectSdkLibraryV1(void* handle,
                       CreateFunction create_function) noexcept
        : handle_(handle), create_(create_function) {}

    ~DirectSdkLibraryV1() {
        if (handle_ != nullptr &&
            !retain_for_process_lifetime_.load(
                std::memory_order_acquire)) {
            static_cast<void>(::dlclose(handle_));
        }
    }

    DirectSdkLibraryV1(const DirectSdkLibraryV1&) = delete;
    DirectSdkLibraryV1& operator=(const DirectSdkLibraryV1&) = delete;

    [[nodiscard]] CreateFunction create() const noexcept {
        return create_;
    }

    void RetainForProcessLifetime() noexcept {
        retain_for_process_lifetime_.store(
            true, std::memory_order_release);
    }

private:
    void* handle_ = nullptr;
    CreateFunction create_ = nullptr;
    std::atomic<bool> retain_for_process_lifetime_{false};
};

struct ShutdownStateV1 final {
    std::atomic<bool> complete{false};
};

enum class ReleaseCountExpectationV1 {
    Zero,
    Positive,
    NonNegative,
};

bool ReleaseVendorObjectV1(
    datayes::RefCounted* object,
    const std::shared_ptr<DirectSdkLibraryV1>& library,
    const char* object_name,
    ReleaseCountExpectationV1 expectation,
    std::string* error) noexcept {
    if (object == nullptr) {
        ClearError(error);
        return true;
    }

    int remaining = 0;
    try {
        remaining = object->ReleaseRef();
    } catch (const std::exception& exception) {
        library->RetainForProcessLifetime();
        try {
            SetError(
                error,
                std::string(object_name) +
                    " ReleaseRef threw across the SDK ABI: " +
                    exception.what());
        } catch (...) {
            SetErrorLiteral(
                error,
                "vendor ReleaseRef threw across the SDK ABI");
        }
        return false;
    } catch (...) {
        library->RetainForProcessLifetime();
        SetErrorLiteral(
            error,
            "vendor ReleaseRef threw an unknown exception across the SDK "
            "ABI");
        return false;
    }

    bool expected_count = false;
    switch (expectation) {
    case ReleaseCountExpectationV1::Zero:
        expected_count = remaining == 0;
        break;
    case ReleaseCountExpectationV1::Positive:
        expected_count = remaining > 0;
        break;
    case ReleaseCountExpectationV1::NonNegative:
        expected_count = remaining >= 0;
        break;
    }
    if (expected_count) {
        ClearError(error);
        return true;
    }

    library->RetainForProcessLifetime();
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

class DirectSdkSubscriberV1 final : public SdkSubscriber {
public:
    DirectSdkSubscriberV1(
        std::shared_ptr<DirectSdkLibraryV1> library,
        std::shared_ptr<ShutdownStateV1> shutdown,
        mdl::Subscriber* subscriber) noexcept
        : library_(std::move(library)),
          shutdown_(std::move(shutdown)),
          subscriber_(subscriber) {}

    ~DirectSdkSubscriberV1() override {
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

    void SetMessageEncoding(mdl::MDLMessageEncoding encoding) override {
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

    [[nodiscard]] std::string Connect() override {
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

    [[nodiscard]] bool Release(std::string* error) noexcept override {
        if (subscriber_ == nullptr) {
            ClearError(error);
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
        return ReleaseVendorObjectV1(
            object,
            library_,
            "Subscriber",
            ReleaseCountExpectationV1::NonNegative,
            error);
    }

private:
    // These owners precede subscriber_ so the DSO mapping and shared shutdown
    // state outlive every virtual call through the vendor object.
    std::shared_ptr<DirectSdkLibraryV1> library_;
    std::shared_ptr<ShutdownStateV1> shutdown_;
    mdl::Subscriber* subscriber_ = nullptr;
};

class DirectSdkManagerV1 final : public SdkManager {
public:
    DirectSdkManagerV1(
        std::shared_ptr<DirectSdkLibraryV1> library,
        mdl::IOManager* manager)
        : library_(std::move(library)),
          shutdown_(std::make_shared<ShutdownStateV1>()),
          manager_(manager) {}

    ~DirectSdkManagerV1() override {
        if (!Release(nullptr)) {
            std::terminate();
        }
    }

    void EnableLog(std::string_view prefix, bool console) override {
        const std::string copied =
            CopyCStringArgument(prefix, "SDK log prefix");
        manager_->EnableLog(copied.c_str(), console);
    }

    [[nodiscard]] std::unique_ptr<SdkSubscriber> CreateSubscriber(
        mdl::MessageHandlerBase* handler,
        bool multithread_callback) override {
        mdl::SubscriberPtr temporary =
            manager_->CreateSubscriber(handler, multithread_callback);
        if (temporary.IsNull()) {
            return nullptr;
        }

        // Own an independent reference in the narrow adapter.  The vendor
        // smart pointer releases its temporary reference on scope exit.
        mdl::Subscriber* const raw = temporary.Duplicate();
        try {
            return std::make_unique<DirectSdkSubscriberV1>(
                library_, shutdown_, raw);
        } catch (...) {
            std::string ignored;
            if (!ReleaseVendorObjectV1(
                    raw,
                    library_,
                    "Subscriber",
                    ReleaseCountExpectationV1::Positive,
                    &ignored)) {
                // The raw object already retains the application's handler.
                // Unwinding that handler while ownership is uncertain is not
                // safe.
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

    [[nodiscard]] bool Release(std::string* error) noexcept override {
        if (manager_ == nullptr) {
            ClearError(error);
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
        return ReleaseVendorObjectV1(
            object,
            library_,
            "IOManager",
            ReleaseCountExpectationV1::Zero,
            error);
    }

private:
    std::shared_ptr<DirectSdkLibraryV1> library_;
    std::shared_ptr<ShutdownStateV1> shutdown_;
    mdl::IOManager* manager_ = nullptr;
};

class DirectSdkFactoryV1 final : public SdkFactory {
public:
    explicit DirectSdkFactoryV1(
        std::shared_ptr<DirectSdkLibraryV1> library) noexcept
        : library_(std::move(library)) {}

    [[nodiscard]] std::unique_ptr<SdkManager> Create(
        int work_threads,
        int io_threads) override {
        if (work_threads <= 0 || io_threads <= 0) {
            throw std::invalid_argument(
                "SDK thread counts must be positive");
        }

        mdl::IOManager* raw =
            library_->create()(mdl::MDL_VERSION, work_threads, io_threads);
        if (raw == nullptr) {
            return nullptr;
        }

        try {
            return std::make_unique<DirectSdkManagerV1>(library_, raw);
        } catch (...) {
            try {
                raw->Shutdown();
            } catch (...) {
                std::terminate();
            }
            std::string ignored;
            if (!ReleaseVendorObjectV1(
                    raw,
                    library_,
                    "IOManager",
                    ReleaseCountExpectationV1::Zero,
                    &ignored)) {
                std::terminate();
            }
            throw;
        }
    }

private:
    std::shared_ptr<DirectSdkLibraryV1> library_;
};

}  // namespace

std::shared_ptr<SdkFactory> LoadSdkFactoryFromPath(
    const std::filesystem::path& shared_library,
    std::string* error) noexcept {
    try {
        ClearError(error);
        if (shared_library.empty()) {
            SetErrorLiteral(error, "SDK shared-library path is empty");
            return nullptr;
        }

        const std::string path = shared_library.string();
        if (path.empty() || path.find('\0') != std::string::npos) {
            SetErrorLiteral(
                error,
                "SDK shared-library path is empty or contains NUL");
            return nullptr;
        }

        ::dlerror();
        void* const raw_handle =
            ::dlopen(path.c_str(), RTLD_NOW | RTLD_LOCAL);
        if (raw_handle == nullptr) {
            const char* const detail = ::dlerror();
            SetError(
                error,
                std::string("dlopen SDK path failed: ") +
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

        using CreateFunction = DirectSdkLibraryV1::CreateFunction;
        static_assert(sizeof(CreateFunction) == sizeof(raw_symbol));
        CreateFunction create = nullptr;
        std::memcpy(&create, &raw_symbol, sizeof(create));
        if (create == nullptr) {
            SetErrorLiteral(
                error,
                "DllCreateIOManager resolved to a null function");
            return nullptr;
        }

        auto library = std::shared_ptr<DirectSdkLibraryV1>(
            new DirectSdkLibraryV1(handle.release(), create));
        auto factory =
            std::make_shared<DirectSdkFactoryV1>(library);

        // This is intentionally a lifetime policy, not an identity check.
        // Failed load/symbol/allocation paths above still close the handle.
        library->RetainForProcessLifetime();
        ClearError(error);
        return factory;
    } catch (const std::exception& exception) {
        try {
            SetError(
                error,
                std::string("loading SDK path failed: ") +
                    exception.what());
        } catch (...) {
            SetErrorLiteral(error, "loading SDK path failed");
        }
        return nullptr;
    } catch (...) {
        SetErrorLiteral(
            error,
            "loading SDK path failed with an unknown exception");
        return nullptr;
    }
}

}  // namespace l2flow::sdk
