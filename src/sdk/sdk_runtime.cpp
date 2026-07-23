#include "l2flow/sdk/sdk_runtime.h"

#include "l2flow/baseline/vendor_baseline.h"
#include "l2flow/common/sealed_file_snapshot.h"
#include "l2flow/common/sha256.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstring>
#include <dlfcn.h>
#include <exception>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

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

    LoadedSdk(void* handle,
              CreateFunction create_function) noexcept
        : handle_(handle), create_(create_function) {}

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

// The selected vendor SDK owns dispatcher, worker and logging state
// process-wide. Empirical production evidence also shows that one IOManager
// with four vendor Subscribers does not complete Shutdown. The formal path
// therefore creates exactly one physical IOManager and one physical
// Subscriber, then fans its callbacks into four independent Raw handlers.
inline constexpr std::size_t kProductionFanoutLanes = 4U;

struct FanoutSubscriberSlot final {
    mdl::MessageHandlerBase* handler = nullptr;
    std::optional<std::string> server_address;
    std::optional<std::string> user_name;
    std::optional<std::uint32_t> heartbeat_interval;
    std::optional<std::uint32_t> heartbeat_timeout;
    std::optional<mdl::MDLMessageEncoding> message_encoding;
    std::optional<bool> merge_message;
    std::optional<bool> send_mac_auth;
    std::optional<bool> server_select;
    std::vector<MessageKey> subscriptions;
    std::optional<IngressKind> ingress_kind;
    bool connect_called = false;
    bool released = false;
};

template <typename T>
void AssignFanoutConfiguration(std::optional<T>* target,
                               T value,
                               const char* name) {
    if (target == nullptr) {
        throw std::logic_error("fanout configuration target is null");
    }
    if (target->has_value()) {
        if (**target != value) {
            throw std::invalid_argument(
                std::string("logical Subscriber changed ") + name);
        }
        return;
    }
    *target = std::move(value);
}

[[nodiscard]] bool ContainsMessageKey(
    const std::vector<MessageKey>& keys,
    const MessageKey& wanted) noexcept {
    return std::find(keys.begin(), keys.end(), wanted) != keys.end();
}

[[nodiscard]] std::optional<IngressKind> FanoutLaneForKey(
    const MessageKey& key) {
    std::optional<IngressKind> result;
    for (const IngressSpec& spec : AllIngressSpecs()) {
        if (!ContainsMessageKey(spec.required, key) &&
            !ContainsMessageKey(spec.optional, key)) {
            continue;
        }
        if (result.has_value()) {
            throw std::logic_error(
                "production subscription key belongs to multiple lanes");
        }
        result = spec.kind;
    }
    return result;
}

class ProductionFanoutSdkSession final
    : public mdl::MessageHandlerBase,
      public std::enable_shared_from_this<ProductionFanoutSdkSession> {
public:
    explicit ProductionFanoutSdkSession(
        std::shared_ptr<SdkFactory> physical_factory) noexcept
        : physical_factory_(std::move(physical_factory)) {}

    ~ProductionFanoutSdkSession() {
        if (physical_manager_ == nullptr) {
            return;
        }
        try {
            if (!shutdown_complete_.load(std::memory_order_acquire)) {
                physical_manager_->Shutdown();
                shutdown_complete_.store(true, std::memory_order_release);
            }
        } catch (...) {
            std::terminate();
        }
        std::string ignored;
        if (physical_subscriber_ != nullptr &&
            !physical_subscriber_->Release(&ignored)) {
            std::terminate();
        }
        physical_subscriber_.reset();
        if (!physical_manager_->Release(&ignored)) {
            std::terminate();
        }
        physical_manager_.reset();
    }

    [[nodiscard]] bool AcquireManagerLease(
        int work_threads,
        int io_threads) {
        if (work_threads <= 0 || io_threads <= 0) {
            throw std::invalid_argument(
                "SDK thread counts must be positive");
        }
        std::lock_guard<std::mutex> lock(mutex_);
        if (shutdown_complete_.load(std::memory_order_acquire)) {
            throw std::logic_error(
                "production fanout manager cannot be acquired after Shutdown");
        }
        if (manager_leases_ >= kProductionFanoutLanes) {
            throw std::logic_error(
                "production fanout requires exactly four logical managers");
        }
        if (physical_manager_ == nullptr) {
            physical_manager_ =
                physical_factory_->Create(work_threads, io_threads);
            if (physical_manager_ == nullptr) {
                return false;
            }
            work_threads_ = work_threads;
            io_threads_ = io_threads;
        } else if (work_threads != work_threads_ ||
                   io_threads != io_threads_) {
            throw std::invalid_argument(
                "all production fanout managers require identical thread counts");
        }
        ++manager_leases_;
        return true;
    }

    void AbandonManagerLease() noexcept {
        std::lock_guard<std::mutex> lock(mutex_);
        if (manager_leases_ != 0U) {
            --manager_leases_;
        }
    }

    void EnableLog(std::string_view prefix, bool console) {
        const std::string copied =
            CopyCStringArgument(prefix, "SDK log prefix");
        std::lock_guard<std::mutex> lock(mutex_);
        RequireRunningManagerLocked();
        if (!log_configured_) {
            physical_manager_->EnableLog(copied, console);
            log_console_ = console;
            log_configured_ = true;
            return;
        }
        if (console != log_console_) {
            throw std::invalid_argument(
                "logical managers disagree on SDK console logging");
        }
        // One physical IOManager has one physical log configuration. The
        // first lane owns it; later per-lane prefixes remain Raw manifest
        // metadata and are deliberately not re-applied to the vendor object.
    }

    [[nodiscard]] std::size_t RegisterSubscriber(
        mdl::MessageHandlerBase* handler,
        bool multithread_callback) {
        if (handler == nullptr) {
            throw std::invalid_argument(
                "production fanout handler is null");
        }
        if (multithread_callback) {
            throw std::invalid_argument(
                "production fanout requires serialized SDK callbacks");
        }
        std::lock_guard<std::mutex> lock(mutex_);
        RequireRunningManagerLocked();
        if (slots_.size() >= kProductionFanoutLanes) {
            throw std::logic_error(
                "production fanout requires exactly four logical Subscribers");
        }
        FanoutSubscriberSlot slot;
        slot.handler = handler;
        slots_.push_back(std::move(slot));
        ++subscriber_leases_;
        return slots_.size() - 1U;
    }

    void AbandonSubscriber(std::size_t slot) noexcept {
        std::lock_guard<std::mutex> lock(mutex_);
        if (slot + 1U == slots_.size() &&
            !slots_[slot].connect_called &&
            !slots_[slot].released) {
            slots_.pop_back();
            if (subscriber_leases_ != 0U) {
                --subscriber_leases_;
            }
        }
    }

    void SetServerAddress(std::size_t slot, std::string_view address) {
        std::lock_guard<std::mutex> lock(mutex_);
        AssignFanoutConfiguration(
            &MutableSlotLocked(slot).server_address,
            CopyCStringArgument(address, "server address"),
            "server address");
    }

    void SetUserName(std::size_t slot, std::string_view user_name) {
        std::lock_guard<std::mutex> lock(mutex_);
        AssignFanoutConfiguration(
            &MutableSlotLocked(slot).user_name,
            CopyCStringArgument(user_name, "user name"),
            "user name");
    }

    void SetHeartbeatInterval(std::size_t slot, std::uint32_t seconds) {
        std::lock_guard<std::mutex> lock(mutex_);
        AssignFanoutConfiguration(
            &MutableSlotLocked(slot).heartbeat_interval,
            seconds,
            "heartbeat interval");
    }

    void SetHeartbeatTimeout(std::size_t slot, std::uint32_t seconds) {
        std::lock_guard<std::mutex> lock(mutex_);
        AssignFanoutConfiguration(
            &MutableSlotLocked(slot).heartbeat_timeout,
            seconds,
            "heartbeat timeout");
    }

    void SetMessageEncoding(std::size_t slot,
                            mdl::MDLMessageEncoding encoding) {
        std::lock_guard<std::mutex> lock(mutex_);
        AssignFanoutConfiguration(
            &MutableSlotLocked(slot).message_encoding,
            encoding,
            "message encoding");
    }

    void EnableMergeMessage(std::size_t slot, bool enable) {
        std::lock_guard<std::mutex> lock(mutex_);
        AssignFanoutConfiguration(
            &MutableSlotLocked(slot).merge_message,
            enable,
            "merge-message setting");
    }

    void SetSendMacAuth(std::size_t slot, bool enable) {
        std::lock_guard<std::mutex> lock(mutex_);
        AssignFanoutConfiguration(
            &MutableSlotLocked(slot).send_mac_auth,
            enable,
            "MAC-auth setting");
    }

    void EnableServerSelect(std::size_t slot, bool enable) {
        std::lock_guard<std::mutex> lock(mutex_);
        AssignFanoutConfiguration(
            &MutableSlotLocked(slot).server_select,
            enable,
            "server-select setting");
    }

    void AddSubscription(std::size_t slot, const MessageKey& key) {
        if (!FanoutLaneForKey(key).has_value()) {
            throw std::invalid_argument(
                "production fanout rejected an unknown or forbidden subscription");
        }
        std::lock_guard<std::mutex> lock(mutex_);
        auto& subscriptions = MutableSlotLocked(slot).subscriptions;
        if (ContainsMessageKey(subscriptions, key)) {
            throw std::invalid_argument(
                "production fanout rejected a duplicate subscription");
        }
        subscriptions.push_back(key);
    }

    [[nodiscard]] std::string Connect(std::size_t slot_index) {
        std::lock_guard<std::mutex> lock(mutex_);
        FanoutSubscriberSlot& slot = MutableSlotLocked(slot_index);
        if (slot.connect_called) {
            throw std::logic_error(
                "logical Subscriber Connect was called more than once");
        }
        ValidateSlotLocked(&slot);
        slot.connect_called = true;
        ++connect_calls_;
        if (connect_calls_ < kProductionFanoutLanes) {
            return {};
        }
        if (connect_calls_ != kProductionFanoutLanes ||
            slots_.size() != kProductionFanoutLanes ||
            manager_leases_ != kProductionFanoutLanes ||
            subscriber_leases_ != kProductionFanoutLanes) {
            throw std::logic_error(
                "physical Connect requires four complete logical lanes");
        }

        ValidateAggregateConfigurationLocked();
        BuildRoutingLocked();
        physical_subscriber_ =
            physical_manager_->CreateSubscriber(this, false);
        if (physical_subscriber_ == nullptr) {
            throw std::runtime_error(
                "physical SdkManager::CreateSubscriber returned null");
        }
        ApplyPhysicalConfigurationLocked();
        routing_ready_.store(true, std::memory_order_release);
        return physical_subscriber_->Connect();
    }

    void Shutdown() {
        std::call_once(shutdown_once_, [this]() {
            SdkManager* manager = nullptr;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                if (physical_manager_ == nullptr) {
                    throw std::logic_error(
                        "production fanout manager is unavailable during Shutdown");
                }
                manager = physical_manager_.get();
            }
            manager->Shutdown();
            shutdown_complete_.store(true, std::memory_order_release);
        });
    }

    [[nodiscard]] bool ReleaseSubscriber(
        std::size_t slot_index,
        std::string* error) noexcept {
        try {
            if (!shutdown_complete_.load(std::memory_order_acquire)) {
                SetErrorLiteral(
                    error,
                    "logical Subscriber release requires completed physical Shutdown");
                return false;
            }
            std::lock_guard<std::mutex> lock(mutex_);
            if (slot_index >= slots_.size()) {
                SetErrorLiteral(error, "logical Subscriber slot is invalid");
                return false;
            }
            FanoutSubscriberSlot& slot = slots_[slot_index];
            if (!slot.released) {
                if (subscriber_leases_ == 0U) {
                    SetErrorLiteral(
                        error, "logical Subscriber lease underflow");
                    return false;
                }
                slot.released = true;
                --subscriber_leases_;
            }
            if (subscriber_leases_ == 0U &&
                physical_subscriber_ != nullptr) {
                if (!physical_subscriber_->Release(error)) {
                    return false;
                }
                physical_subscriber_.reset();
            }
            if (error != nullptr) {
                error->clear();
            }
            return true;
        } catch (...) {
            SetErrorLiteral(
                error, "logical Subscriber release failed unexpectedly");
            return false;
        }
    }

    [[nodiscard]] bool ReleaseManagerLease(std::string* error) noexcept {
        try {
            if (!shutdown_complete_.load(std::memory_order_acquire)) {
                SetErrorLiteral(
                    error,
                    "logical IOManager release requires completed physical Shutdown");
                return false;
            }
            std::lock_guard<std::mutex> lock(mutex_);
            if (manager_leases_ == 0U) {
                SetErrorLiteral(error, "logical IOManager lease is not active");
                return false;
            }
            --manager_leases_;
            if (manager_leases_ == 0U) {
                if (subscriber_leases_ != 0U ||
                    physical_subscriber_ != nullptr) {
                    SetErrorLiteral(
                        error,
                        "physical IOManager release preceded Subscriber release");
                    ++manager_leases_;
                    return false;
                }
                if (!physical_manager_->Release(error)) {
                    ++manager_leases_;
                    return false;
                }
                physical_manager_.reset();
            }
            if (error != nullptr) {
                error->clear();
            }
            return true;
        } catch (...) {
            SetErrorLiteral(
                error, "logical IOManager release failed unexpectedly");
            return false;
        }
    }

    void OnMessage(mdl::Subscriber* sender,
                   const mdl::MDLMessage* message) noexcept override {
        if (!routing_ready_.load(std::memory_order_acquire) ||
            message == nullptr) {
            return;
        }
        try {
            const mdl::MDLMessageHead* const head = message->GetHead();
            if (head == nullptr) {
                return;
            }
            if (head->ServiceID == mdl::MDLSID_MDL_API ||
                head->ServiceID == mdl::MDLSID_MDL_SYS) {
                for (mdl::MessageHandlerBase* handler : control_handlers_) {
                    try {
                        handler->OnMessage(sender, message);
                    } catch (...) {
                        // RawUnifiedCallbackRouter is noexcept. Contain any
                        // foreign handler violation at this ABI boundary and
                        // continue the control fanout to the remaining lanes.
                    }
                }
                return;
            }
            const MessageKey key{
                head->ServiceID,
                head->ServiceVersion,
                head->MessageID};
            for (const auto& route : market_routes_) {
                if (route.first == key) {
                    try {
                        route.second->OnMessage(sender, message);
                    } catch (...) {
                    }
                    return;
                }
            }
            // Unknown and forbidden market messages fail closed: the SDK
            // callback is consumed without entering any Raw stream.
        } catch (...) {
            // Never unwind through the vendor callback ABI.
        }
    }

private:
    void RequireRunningManagerLocked() const {
        if (physical_manager_ == nullptr ||
            shutdown_complete_.load(std::memory_order_acquire)) {
            throw std::logic_error(
                "production fanout manager is not running");
        }
    }

    [[nodiscard]] FanoutSubscriberSlot& MutableSlotLocked(
        std::size_t slot) {
        RequireRunningManagerLocked();
        if (slot >= slots_.size() || slots_[slot].released ||
            slots_[slot].connect_called) {
            throw std::logic_error(
                "logical Subscriber is not configurable");
        }
        return slots_[slot];
    }

    static void ValidateSlotLocked(FanoutSubscriberSlot* slot) {
        if (slot == nullptr || slot->handler == nullptr ||
            !slot->server_address.has_value() ||
            !slot->user_name.has_value() ||
            !slot->heartbeat_interval.has_value() ||
            !slot->heartbeat_timeout.has_value() ||
            !slot->message_encoding.has_value() ||
            !slot->merge_message.has_value() ||
            !slot->send_mac_auth.has_value() ||
            !slot->server_select.has_value() ||
            slot->subscriptions.empty()) {
            throw std::invalid_argument(
                "logical Subscriber configuration is incomplete");
        }
        const std::optional<IngressKind> lane =
            FanoutLaneForKey(slot->subscriptions.front());
        if (!lane.has_value()) {
            throw std::invalid_argument(
                "logical Subscriber has no production lane");
        }
        for (const MessageKey& key : slot->subscriptions) {
            if (FanoutLaneForKey(key) != lane) {
                throw std::invalid_argument(
                    "logical Subscriber mixes production lanes");
            }
        }
        const IngressSpec& spec = GetIngressSpec(*lane);
        for (const MessageKey& required : spec.required) {
            if (!ContainsMessageKey(slot->subscriptions, required)) {
                throw std::invalid_argument(
                    "logical Subscriber omits a required market message");
            }
        }
        slot->ingress_kind = lane;
    }

    void ValidateAggregateConfigurationLocked() const {
        std::set<IngressKind> lanes;
        const FanoutSubscriberSlot& first = slots_.front();
        for (const FanoutSubscriberSlot& slot : slots_) {
            if (!slot.connect_called || !slot.ingress_kind.has_value() ||
                !lanes.insert(*slot.ingress_kind).second) {
                throw std::invalid_argument(
                    "production fanout requires four unique ingress lanes");
            }
            if (slot.server_address != first.server_address ||
                slot.user_name != first.user_name ||
                slot.heartbeat_interval != first.heartbeat_interval ||
                slot.heartbeat_timeout != first.heartbeat_timeout ||
                slot.message_encoding != first.message_encoding ||
                slot.merge_message != first.merge_message ||
                slot.send_mac_auth != first.send_mac_auth ||
                slot.server_select != first.server_select) {
                throw std::invalid_argument(
                    "logical Subscribers disagree on physical connection configuration");
            }
        }
    }

    void BuildRoutingLocked() {
        std::set<std::tuple<std::uint8_t, std::uint16_t, std::uint16_t>>
            unique_keys;
        std::array<mdl::MessageHandlerBase*, kProductionFanoutLanes>
            handlers{};
        std::vector<std::pair<MessageKey, mdl::MessageHandlerBase*>> routes;
        for (std::size_t index = 0U; index < slots_.size(); ++index) {
            const FanoutSubscriberSlot& slot = slots_[index];
            handlers[index] = slot.handler;
            for (const MessageKey& key : slot.subscriptions) {
                const auto tuple = std::make_tuple(
                    key.service_id, key.service_version, key.message_id);
                if (!unique_keys.insert(tuple).second) {
                    throw std::invalid_argument(
                        "market subscription is owned by multiple logical lanes");
                }
                routes.emplace_back(key, slot.handler);
            }
        }
        control_handlers_ = handlers;
        market_routes_ = std::move(routes);
    }

    void ApplyPhysicalConfigurationLocked() {
        const FanoutSubscriberSlot& config = slots_.front();
        physical_subscriber_->SetServerAddress(*config.server_address);
        physical_subscriber_->SetUserName(*config.user_name);
        physical_subscriber_->SetHeartbeatInterval(
            *config.heartbeat_interval);
        physical_subscriber_->SetHeartbeatTimeout(
            *config.heartbeat_timeout);
        physical_subscriber_->SetMessageEncoding(
            *config.message_encoding);
        physical_subscriber_->EnableMergeMessage(*config.merge_message);
        physical_subscriber_->SetSendMacAuth(*config.send_mac_auth);
        physical_subscriber_->EnableServerSelect(*config.server_select);
        for (const auto& route : market_routes_) {
            physical_subscriber_->AddSubscription(route.first);
        }
    }

    std::shared_ptr<SdkFactory> physical_factory_;
    std::mutex mutex_;
    std::once_flag shutdown_once_;
    std::unique_ptr<SdkManager> physical_manager_;
    std::unique_ptr<SdkSubscriber> physical_subscriber_;
    std::vector<FanoutSubscriberSlot> slots_;
    std::array<mdl::MessageHandlerBase*, kProductionFanoutLanes>
        control_handlers_{};
    std::vector<std::pair<MessageKey, mdl::MessageHandlerBase*>>
        market_routes_;
    std::atomic<bool> routing_ready_{false};
    std::atomic<bool> shutdown_complete_{false};
    int work_threads_ = 0;
    int io_threads_ = 0;
    std::size_t manager_leases_ = 0U;
    std::size_t subscriber_leases_ = 0U;
    std::size_t connect_calls_ = 0U;
    bool log_configured_ = false;
    bool log_console_ = false;
};

class ProductionFanoutSdkSubscriber final : public SdkSubscriber {
public:
    ProductionFanoutSdkSubscriber(
        std::shared_ptr<ProductionFanoutSdkSession> session,
        std::size_t slot) noexcept
        : session_(std::move(session)), slot_(slot) {}

    ~ProductionFanoutSdkSubscriber() override {
        if (!Release(nullptr)) {
            std::terminate();
        }
    }

    void SetServerAddress(std::string_view address) override {
        session_->SetServerAddress(slot_, address);
    }
    void SetUserName(std::string_view user_name) override {
        session_->SetUserName(slot_, user_name);
    }
    void SetHeartbeatInterval(std::uint32_t seconds) override {
        session_->SetHeartbeatInterval(slot_, seconds);
    }
    void SetHeartbeatTimeout(std::uint32_t seconds) override {
        session_->SetHeartbeatTimeout(slot_, seconds);
    }
    void SetMessageEncoding(mdl::MDLMessageEncoding encoding) override {
        session_->SetMessageEncoding(slot_, encoding);
    }
    void EnableMergeMessage(bool enable) override {
        session_->EnableMergeMessage(slot_, enable);
    }
    void SetSendMacAuth(bool enable) override {
        session_->SetSendMacAuth(slot_, enable);
    }
    void EnableServerSelect(bool enable) override {
        session_->EnableServerSelect(slot_, enable);
    }
    void AddSubscription(const MessageKey& key) override {
        session_->AddSubscription(slot_, key);
    }
    [[nodiscard]] std::string Connect() override {
        return session_->Connect(slot_);
    }

    [[nodiscard]] bool Release(std::string* error) noexcept override {
        if (released_) {
            if (error != nullptr) {
                error->clear();
            }
            return true;
        }
        if (!session_->ReleaseSubscriber(slot_, error)) {
            return false;
        }
        released_ = true;
        session_.reset();
        return true;
    }

private:
    std::shared_ptr<ProductionFanoutSdkSession> session_;
    std::size_t slot_ = 0U;
    bool released_ = false;
};

class ProductionFanoutSdkManager final : public SdkManager {
public:
    explicit ProductionFanoutSdkManager(
        std::shared_ptr<ProductionFanoutSdkSession> session) noexcept
        : session_(std::move(session)) {}

    ~ProductionFanoutSdkManager() override {
        if (!Release(nullptr)) {
            std::terminate();
        }
    }

    void EnableLog(std::string_view prefix, bool console) override {
        session_->EnableLog(prefix, console);
    }

    [[nodiscard]] std::unique_ptr<SdkSubscriber> CreateSubscriber(
        mdl::MessageHandlerBase* handler,
        bool multithread_callback) override {
        if (subscriber_created_) {
            throw std::logic_error(
                "logical IOManager may create only one Subscriber");
        }
        const std::size_t slot =
            session_->RegisterSubscriber(handler, multithread_callback);
        try {
            auto result = std::make_unique<ProductionFanoutSdkSubscriber>(
                session_, slot);
            subscriber_created_ = true;
            return result;
        } catch (...) {
            session_->AbandonSubscriber(slot);
            throw;
        }
    }

    void Shutdown() override { session_->Shutdown(); }

    [[nodiscard]] bool Release(std::string* error) noexcept override {
        if (released_) {
            if (error != nullptr) {
                error->clear();
            }
            return true;
        }
        if (!session_->ReleaseManagerLease(error)) {
            return false;
        }
        released_ = true;
        session_.reset();
        return true;
    }

private:
    std::shared_ptr<ProductionFanoutSdkSession> session_;
    bool subscriber_created_ = false;
    bool released_ = false;
};

class ProductionFanoutSdkFactory final : public SdkFactory {
public:
    explicit ProductionFanoutSdkFactory(
        std::shared_ptr<SdkFactory> physical_factory)
        : session_(std::make_shared<ProductionFanoutSdkSession>(
              std::move(physical_factory))) {}

    [[nodiscard]] std::unique_ptr<SdkManager> Create(
        int work_threads,
        int io_threads) override {
        if (!session_->AcquireManagerLease(work_threads, io_threads)) {
            return nullptr;
        }
        try {
            return std::make_unique<ProductionFanoutSdkManager>(session_);
        } catch (...) {
            session_->AbandonManagerLease();
            throw;
        }
    }

private:
    std::shared_ptr<ProductionFanoutSdkSession> session_;
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

std::shared_ptr<SdkFactory> MakeProductionFanoutSdkFactory(
    std::shared_ptr<SdkFactory> physical_factory) noexcept {
    if (physical_factory == nullptr) {
        return nullptr;
    }
    try {
        return std::make_shared<ProductionFanoutSdkFactory>(
            std::move(physical_factory));
    } catch (...) {
        return nullptr;
    }
}

namespace {

std::shared_ptr<SdkFactory> LoadApprovedSdkFactoryImpl(
    const std::filesystem::path& shared_library,
    const l2flow::common::Sha256Digest* expected_sha256,
    bool run_approved_preflight,
    l2flow::common::Sha256Digest* observed_sha256,
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

        if (expected_sha256 != nullptr || observed_sha256 != nullptr) {
            common::Sha256Digest observed_digest{};
            if (!common::ComputeFileSha256ForOpenFd(
                    snapshot.fd(),
                    &observed_digest,
                    error,
                    baseline::kMaximumSdkSharedLibraryBytes)) {
                return nullptr;
            }
            if (expected_sha256 != nullptr &&
                observed_digest != *expected_sha256) {
                SetErrorLiteral(
                    error,
                    "sealed SDK snapshot SHA-256 does not match the deployment pin");
                return nullptr;
            }
            if (observed_sha256 != nullptr) {
                *observed_sha256 = observed_digest;
            }
        }

        const std::string fixed_path = snapshot.proc_fd_path();
        if (run_approved_preflight) {
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
        }

        // Load the exact sealed memfd selected above, never the caller's
        // mutable pathname or source inode. Approved callers additionally ran
        // preflight; the operator-selected production caller deliberately did
        // not.
        ::dlerror();
        void* const raw_handle =
            ::dlopen(fixed_path.c_str(), RTLD_NOW | RTLD_LOCAL);
        if (raw_handle == nullptr) {
            const char* const detail = ::dlerror();
            SetError(
                error,
                std::string("dlopen SDK snapshot failed: ") +
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
                std::string("loading SDK snapshot failed: ") +
                    exception.what());
        } catch (...) {
            SetErrorLiteral(error, "loading SDK snapshot failed");
        }
        return nullptr;
    } catch (...) {
        SetErrorLiteral(
            error,
            "loading SDK snapshot failed with an unknown exception");
        return nullptr;
    }
}

}  // namespace

std::shared_ptr<SdkFactory> LoadApprovedSdkFactory(
    const std::filesystem::path& shared_library,
    std::string* error) noexcept {
    return LoadApprovedSdkFactoryImpl(
        shared_library, nullptr, true, nullptr, error);
}

std::shared_ptr<SdkFactory> LoadApprovedSdkFactoryPinned(
    const std::filesystem::path& shared_library,
    const l2flow::common::Sha256Digest& expected_sha256,
    std::string* error) noexcept {
    return LoadApprovedSdkFactoryImpl(
        shared_library, &expected_sha256, true, nullptr, error);
}

std::shared_ptr<SdkFactory> LoadOperatorSelectedSdkFactory(
    const std::filesystem::path& shared_library,
    std::string* error) noexcept {
    try {
        if (error != nullptr) {
            error->clear();
        }
        if (shared_library.empty() ||
            shared_library.native().find('\0') != std::string::npos) {
            SetError(
                error,
                "SDK shared-library path is empty or contains NUL");
            return nullptr;
        }

        // Path existence/type is the sole production SDK preflight and is
        // performed by LoadProductionStaticInputs().  Open the operator's
        // exact path directly here: no copy, digest, archive/baseline, ABI or
        // lifecycle approval is performed in this code path.
        const std::string path = shared_library.string();
        ::dlerror();
        void* const raw_handle =
            ::dlopen(path.c_str(), RTLD_NOW | RTLD_LOCAL);
        if (raw_handle == nullptr) {
            const char* const detail = ::dlerror();
            SetError(
                error,
                std::string("dlopen operator-selected SDK path failed: ") +
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

        auto loaded = std::shared_ptr<LoadedSdk>(
            new LoadedSdk(handle.release(), create));
        auto physical_factory =
            std::make_shared<DynamicSdkFactory>(loaded);
        auto fanout_factory = MakeProductionFanoutSdkFactory(
            std::move(physical_factory));
        if (fanout_factory == nullptr) {
            SetErrorLiteral(
                error, "cannot create production SDK fanout adapter");
            return nullptr;
        }

        // The vendor DSO owns process-global runtime state.  Even after the
        // one physical Subscriber and IOManager have shut down and released
        // successfully, its unload finalizers are not a supported production
        // lifecycle boundary and may wait indefinitely.  Keep the selected
        // mapping until process exit; the kernel then reclaims it without an
        // in-process dlclose() transition.  This is a lifetime policy only:
        // it does not copy, inspect, hash or approve the operator's path.
        loaded->PinForProcessLifetime();
        return fanout_factory;
    } catch (const std::exception& exception) {
        try {
            SetError(
                error,
                std::string("loading operator-selected SDK path failed: ") +
                    exception.what());
        } catch (...) {
            SetErrorLiteral(
                error, "loading operator-selected SDK path failed");
        }
        return nullptr;
    } catch (...) {
        SetErrorLiteral(
            error,
            "loading operator-selected SDK path failed unexpectedly");
        return nullptr;
    }
}

}  // namespace l2flow::sdk
