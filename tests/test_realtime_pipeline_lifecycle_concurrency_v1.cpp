#include "l2flow/runtime/realtime_pipeline_v1.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace {

namespace market = l2flow::market;
namespace mdl = datayes::mdl;
namespace realtime = l2flow::realtime;
namespace runtime = l2flow::runtime;
namespace sdk = l2flow::sdk;

using namespace std::chrono_literals;

class TestContext final {
public:
    void Expect(bool condition, std::string_view description) {
        if (!condition) {
            ++failures_;
            std::cerr << "FAIL: " << description << '\n';
        }
    }

    [[nodiscard]] int failures() const noexcept { return failures_; }

private:
    int failures_ = 0;
};

class OneShotGate final {
public:
    void EnterAndWait() const {
        std::unique_lock<std::mutex> lock(mutex_);
        entered_ = true;
        condition_.notify_all();
        condition_.wait(lock, [this] { return open_; });
    }

    [[nodiscard]] bool WaitUntilEntered(
        std::chrono::milliseconds timeout) const {
        std::unique_lock<std::mutex> lock(mutex_);
        return condition_.wait_for(
            lock, timeout, [this] { return entered_; });
    }

    void Open() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            open_ = true;
        }
        condition_.notify_all();
    }

private:
    mutable std::mutex mutex_;
    mutable std::condition_variable condition_;
    mutable bool entered_ = false;
    bool open_ = false;
};

enum class LifecycleEvent : std::uint8_t {
    kCallbackEnter = 0U,
    kCallbackExit,
    kShutdownBegin,
    kShutdownEnd,
    kSubscriberRelease,
    kManagerRelease,
    kTerminalReturn,
};

struct LifecycleSnapshot final {
    std::vector<LifecycleEvent> events;
    std::uint32_t shutdown_calls = 0U;
    std::uint32_t subscriber_release_calls = 0U;
    std::uint32_t manager_release_calls = 0U;
};

class LifecycleState final {
public:
    void InstallHandler(mdl::MessageHandlerBase* handler) {
        std::lock_guard<std::mutex> lock(mutex_);
        handler_ = handler;
    }

    [[nodiscard]] mdl::MessageHandlerBase* handler() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return handler_;
    }

    void CallbackEnter() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            callback_active_ = true;
            events_.push_back(LifecycleEvent::kCallbackEnter);
        }
        condition_.notify_all();
    }

    void CallbackExit() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            callback_active_ = false;
            events_.push_back(LifecycleEvent::kCallbackExit);
        }
        condition_.notify_all();
    }

    void TerminalCallStarted() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            terminal_call_started_ = true;
        }
        condition_.notify_all();
    }

    [[nodiscard]] bool WaitForTerminalCallStarted(
        std::chrono::milliseconds timeout) const {
        std::unique_lock<std::mutex> lock(mutex_);
        return condition_.wait_for(
            lock, timeout, [this] { return terminal_call_started_; });
    }

    void ShutdownAndQuiesce() {
        std::unique_lock<std::mutex> lock(mutex_);
        ++shutdown_calls_;
        events_.push_back(LifecycleEvent::kShutdownBegin);
        condition_.notify_all();
        condition_.wait(lock, [this] { return !callback_active_; });
        shutdown_quiesced_ = true;
        condition_.notify_all();
        condition_.wait(lock, [this] { return allow_shutdown_return_; });
        events_.push_back(LifecycleEvent::kShutdownEnd);
        shutdown_complete_ = true;
        condition_.notify_all();
    }

    [[nodiscard]] bool WaitForShutdownQuiesced(
        std::chrono::milliseconds timeout) const {
        std::unique_lock<std::mutex> lock(mutex_);
        return condition_.wait_for(
            lock, timeout, [this] { return shutdown_quiesced_; });
    }

    void AllowShutdownReturn() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            allow_shutdown_return_ = true;
        }
        condition_.notify_all();
    }

    [[nodiscard]] bool SubscriberRelease() {
        std::lock_guard<std::mutex> lock(mutex_);
        ++subscriber_release_calls_;
        events_.push_back(LifecycleEvent::kSubscriberRelease);
        return shutdown_complete_;
    }

    [[nodiscard]] bool ManagerRelease() {
        std::lock_guard<std::mutex> lock(mutex_);
        ++manager_release_calls_;
        events_.push_back(LifecycleEvent::kManagerRelease);
        return shutdown_complete_;
    }

    void TerminalReturn() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            events_.push_back(LifecycleEvent::kTerminalReturn);
        }
        condition_.notify_all();
    }

    [[nodiscard]] LifecycleSnapshot Snapshot() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return LifecycleSnapshot{
            events_,
            shutdown_calls_,
            subscriber_release_calls_,
            manager_release_calls_};
    }

private:
    mutable std::mutex mutex_;
    mutable std::condition_variable condition_;
    mdl::MessageHandlerBase* handler_ = nullptr;
    bool callback_active_ = false;
    bool terminal_call_started_ = false;
    bool shutdown_quiesced_ = false;
    bool allow_shutdown_return_ = false;
    bool shutdown_complete_ = false;
    std::uint32_t shutdown_calls_ = 0U;
    std::uint32_t subscriber_release_calls_ = 0U;
    std::uint32_t manager_release_calls_ = 0U;
    std::vector<LifecycleEvent> events_;
};

class RecordingSubscriber final : public sdk::SdkSubscriber {
public:
    explicit RecordingSubscriber(
        std::shared_ptr<LifecycleState> state)
        : state_(std::move(state)) {}

    void SetServerAddress(std::string_view) override {}
    void SetUserName(std::string_view) override {}
    void SetHeartbeatInterval(std::uint32_t) override {}
    void SetHeartbeatTimeout(std::uint32_t) override {}
    void SetMessageEncoding(mdl::MDLMessageEncoding encoding) override {
        binary_ = encoding == mdl::MDLEID_BINARY;
    }
    void EnableMergeMessage(bool enable) override { merge_ = enable; }
    void SetSendMacAuth(bool) override {}
    void EnableServerSelect(bool) override {}
    void AddSubscription(const sdk::MessageKey&) override {}

    [[nodiscard]] std::string Connect() override {
        return binary_ && !merge_ ? std::string{} : "invalid test setup";
    }

    [[nodiscard]] bool Release(std::string* error) noexcept override {
        bool success = true;
        if (!released_) {
            success = state_->SubscriberRelease();
            released_ = true;
        }
        if (error != nullptr) {
            if (success) {
                error->clear();
            } else {
                *error = "Subscriber released before Shutdown completed";
            }
        }
        return success;
    }

private:
    std::shared_ptr<LifecycleState> state_;
    bool binary_ = false;
    bool merge_ = true;
    bool released_ = false;
};

class RecordingManager final : public sdk::SdkManager {
public:
    explicit RecordingManager(std::shared_ptr<LifecycleState> state)
        : state_(std::move(state)) {}

    void EnableLog(std::string_view, bool) override {}

    [[nodiscard]] std::unique_ptr<sdk::SdkSubscriber> CreateSubscriber(
        mdl::MessageHandlerBase* handler,
        bool multithread_callback) override {
        if (multithread_callback) {
            return nullptr;
        }
        state_->InstallHandler(handler);
        return std::make_unique<RecordingSubscriber>(state_);
    }

    void Shutdown() override {
        state_->ShutdownAndQuiesce();
        shutdown_complete_ = true;
    }

    [[nodiscard]] bool Release(std::string* error) noexcept override {
        bool success = shutdown_complete_;
        if (!released_) {
            success = state_->ManagerRelease() && success;
            released_ = true;
        }
        if (error != nullptr) {
            if (success) {
                error->clear();
            } else {
                *error = "manager released before Shutdown completed";
            }
        }
        return success;
    }

private:
    std::shared_ptr<LifecycleState> state_;
    bool shutdown_complete_ = false;
    bool released_ = false;
};

class RecordingFactory final : public sdk::SdkFactory {
public:
    explicit RecordingFactory(std::shared_ptr<LifecycleState> state)
        : state_(std::move(state)) {}

    [[nodiscard]] std::unique_ptr<sdk::SdkManager> Create(
        int,
        int) override {
        return std::make_unique<RecordingManager>(state_);
    }

private:
    std::shared_ptr<LifecycleState> state_;
};

class WireWriter final {
public:
    explicit WireWriter(std::size_t fixed_bytes)
        : bytes_(fixed_bytes, std::byte{0U}) {}

    void StoreU16(std::size_t offset, std::uint16_t value) {
        StoreUnsigned(offset, value);
    }
    void StoreU32(std::size_t offset, std::uint32_t value) {
        StoreUnsigned(offset, value);
    }
    void StoreU64(std::size_t offset, std::uint64_t value) {
        StoreUnsigned(offset, value);
    }

    void StoreString(std::size_t descriptor, std::string_view value) {
        const std::size_t start = bytes_.size();
        StoreU16(descriptor, static_cast<std::uint16_t>(value.size()));
        StoreU32(
            descriptor + sizeof(std::uint16_t),
            value.empty()
                ? 0U
                : static_cast<std::uint32_t>(start - descriptor));
        const auto characters =
            std::span<const char>(value.data(), value.size());
        const auto encoded = std::as_bytes(characters);
        bytes_.resize(start + encoded.size());
        if (!encoded.empty()) {
            std::memcpy(
                bytes_.data() + start,
                encoded.data(),
                encoded.size());
        }
    }

    [[nodiscard]] std::vector<std::byte> Take() && {
        return std::move(bytes_);
    }

private:
    template <typename Unsigned>
    void StoreUnsigned(std::size_t offset, Unsigned value) {
        static_assert(std::is_unsigned_v<Unsigned>);
        for (std::size_t index = 0U; index < sizeof(Unsigned); ++index) {
            bytes_[offset + index] = static_cast<std::byte>(
                (value >> (index * 8U)) &
                static_cast<Unsigned>(0xffU));
        }
    }

    std::vector<std::byte> bytes_;
};

std::vector<std::byte> ShenzhenTransactionBody(
    std::uint64_t application_sequence = 91U,
    std::uint64_t price_raw = 123'456U,
    std::uint64_t quantity_raw = 33U,
    std::uint32_t channel = 12U) {
    WireWriter writer(70U);
    writer.StoreU32(0U, channel);
    writer.StoreU64(4U, application_sequence);
    writer.StoreU64(
        18U,
        application_sequence == 0U
            ? 0U
            : application_sequence - 1U);
    writer.StoreU64(26U, 0U);
    writer.StoreU64(46U, price_raw);
    writer.StoreU64(54U, quantity_raw);
    writer.StoreU32(62U, 70U);
    writer.StoreU32(66U, 93'000'124U);
    writer.StoreString(12U, "010");
    writer.StoreString(34U, "000001");
    writer.StoreString(40U, "102 ");
    return std::move(writer).Take();
}

std::vector<std::byte> ShanghaiTickBody(
    std::uint64_t business_index,
    std::string_view type,
    std::string_view flag) {
    WireWriter writer(70U);
    writer.StoreU64(0U, business_index);
    writer.StoreU32(8U, 7U);
    writer.StoreU32(18U, 93'000'125U);
    writer.StoreU64(28U, 11'001U);
    writer.StoreU64(36U, 22'002U);
    writer.StoreU32(44U, 12'345U);
    writer.StoreU64(48U, 41U);
    writer.StoreU64(56U, 1'000U);
    writer.StoreString(12U, "600001");
    writer.StoreString(22U, type);
    writer.StoreString(64U, flag);
    return std::move(writer).Take();
}

class BlockingMessage final : public mdl::MDLMessage {
public:
    explicit BlockingMessage(std::shared_ptr<OneShotGate> access_gate)
        : access_gate_(std::move(access_gate)),
          body_(ShenzhenTransactionBody()) {
        std::memset(&head_, 0, sizeof(head_));
        head_.HeadSize = static_cast<std::uint8_t>(sdk::kVendorHeadBytes);
        head_.MessageSize = static_cast<std::uint32_t>(
            sdk::kVendorHeadBytes + body_.size());
        head_.MessageEncoding =
            static_cast<std::uint8_t>(mdl::MDLEID_BINARY);
        head_.ServiceID = 6U;
        head_.ServiceVersion = 101U;
        head_.MessageID = 36U;
        head_.LocalTime.m_Value = 93'000'000U;
        head_.SequenceID = 777U;
    }

    void AddRef() override {}
    int ReleaseRef() override { return 1; }

    mdl::MDLMessageHead* GetHead() const override {
        access_gate_->EnterAndWait();
        return const_cast<mdl::MDLMessageHead*>(&head_);
    }

    char* GetBody() const override {
        return reinterpret_cast<char*>(
            const_cast<std::byte*>(body_.data()));
    }

    mdl::MDLMessage* _Copy() const override { return nullptr; }

private:
    std::shared_ptr<OneShotGate> access_gate_;
    mdl::MDLMessageHead head_{};
    std::vector<std::byte> body_;
};

class OwnedTestMessage final : public mdl::MDLMessage {
public:
    OwnedTestMessage(
        std::uint64_t vendor_sequence,
        std::uint64_t application_sequence,
        std::uint64_t price_raw,
        std::uint64_t quantity_raw = 1U,
        std::uint32_t vendor_local_time = 93'000'000U)
        : OwnedTestMessage(
              sdk::MessageKey{6U, 101U, 36U},
              vendor_sequence,
              ShenzhenTransactionBody(
                  application_sequence,
                  price_raw,
                  quantity_raw),
              vendor_local_time) {}

    OwnedTestMessage(
        sdk::MessageKey key,
        std::uint64_t vendor_sequence,
        std::vector<std::byte> body,
        std::uint32_t vendor_local_time = 93'000'000U)
        : key_(key), body_(std::move(body)) {
        std::memset(&head_, 0, sizeof(head_));
        head_.HeadSize =
            static_cast<std::uint8_t>(sdk::kVendorHeadBytes);
        head_.MessageSize = static_cast<std::uint32_t>(
            sdk::kVendorHeadBytes + body_.size());
        head_.MessageEncoding =
            static_cast<std::uint8_t>(mdl::MDLEID_BINARY);
        head_.ServiceID = key.service_id;
        head_.ServiceVersion = key.service_version;
        head_.MessageID = key.message_id;
        head_.LocalTime.m_Value = vendor_local_time;
        head_.SequenceID = vendor_sequence;
    }

    void AddRef() override {}
    int ReleaseRef() override { return 1; }

    mdl::MDLMessageHead* GetHead() const override {
        return const_cast<mdl::MDLMessageHead*>(&head_);
    }

    char* GetBody() const override {
        return reinterpret_cast<char*>(
            const_cast<std::byte*>(body_.data()));
    }

    mdl::MDLMessage* _Copy() const override { return nullptr; }

    const sdk::MessageKey& key() const noexcept { return key_; }

private:
    sdk::MessageKey key_{};
    mdl::MDLMessageHead head_{};
    std::vector<std::byte> body_;
};

class TestReplaySource final
    : public l2flow::recovery::StartupReplaySourceV1 {
public:
    explicit TestReplaySource(
        std::vector<std::shared_ptr<OwnedTestMessage>> messages,
        std::vector<sdk::MessageKey> fences = {},
        std::function<void()> after_fences = {})
        : messages_(std::move(messages)),
          fences_(std::move(fences)),
          after_fences_(std::move(after_fences)) {}

    l2flow::recovery::StartupReplayResultV1 Replay(
        l2flow::recovery::StartupReplaySinkV1& sink)
        noexcept override {
        l2flow::recovery::StartupReplayResultV1 result{};
        for (const sdk::MessageKey& key : fences_) {
            std::string detail;
            if (!sink.CaptureTupleFence(key, &detail)) {
                result.error =
                    l2flow::recovery::StartupReplayErrorV1::
                        kSinkRejected;
                result.detail = std::move(detail);
                return result;
            }
        }
        if (after_fences_) {
            after_fences_();
        }
        for (std::size_t index = 0U;
             index < messages_.size();
             ++index) {
            l2flow::recovery::StartupReplayPublicationV1
                publication{};
            publication.message = messages_[index].get();
            publication.key = messages_[index]->key();
            publication.source_line =
                static_cast<std::uint64_t>(index) + 2U;
            publication.csv_sequence =
                messages_[index]->GetHead()->SequenceID;
            std::string detail;
            if (!sink.Publish(publication, &detail)) {
                result.error =
                    l2flow::recovery::StartupReplayErrorV1::
                        kSinkRejected;
                result.error_line = publication.source_line;
                result.detail = std::move(detail);
                return result;
            }
            ++result.counts.shenzhen_transactions;
        }
        return result;
    }

private:
    std::vector<std::shared_ptr<OwnedTestMessage>> messages_;
    std::vector<sdk::MessageKey> fences_;
    std::function<void()> after_fences_;
};

class CaptureProbe final
    : public realtime::RealtimeIngressCaptureSinkV1 {
public:
    explicit CaptureProbe(bool accept) : accept_(accept) {}

    [[nodiscard]] bool Capture(
        const realtime::RealtimeIngressCaptureInputV1& input)
        noexcept override {
        try {
            ++calls_;
            if (input.inspection == nullptr || !*input.inspection) {
                valid_ = false;
                return false;
            }
            key_ = input.inspection->key();
            source_slot_ = input.inspection->source_slot();
            vendor_sequence_ =
                input.inspection->vendor_head().sequence_id();
            realtime_ns_ = input.recv_realtime_ns;
            monotonic_ns_ = input.recv_monotonic_ns;
            body_.assign(
                input.inspection->body().begin(),
                input.inspection->body().end());
            valid_ = realtime_ns_ != 0U && monotonic_ns_ != 0U;
            return accept_ && valid_;
        } catch (...) {
            valid_ = false;
            return false;
        }
    }

    [[nodiscard]] std::uint64_t calls() const noexcept { return calls_; }
    [[nodiscard]] bool valid() const noexcept { return valid_; }
    [[nodiscard]] const sdk::MessageKey& key() const noexcept {
        return key_;
    }
    [[nodiscard]] std::uint8_t source_slot() const noexcept {
        return source_slot_;
    }
    [[nodiscard]] std::uint64_t vendor_sequence() const noexcept {
        return vendor_sequence_;
    }
    [[nodiscard]] std::uint64_t realtime_ns() const noexcept {
        return realtime_ns_;
    }
    [[nodiscard]] std::uint64_t monotonic_ns() const noexcept {
        return monotonic_ns_;
    }
    [[nodiscard]] std::span<const std::byte> body() const noexcept {
        return body_;
    }

private:
    bool accept_ = false;
    bool valid_ = false;
    std::uint64_t calls_ = 0U;
    sdk::MessageKey key_{};
    std::uint8_t source_slot_ = 0U;
    std::uint64_t vendor_sequence_ = 0U;
    std::uint64_t realtime_ns_ = 0U;
    std::uint64_t monotonic_ns_ = 0U;
    std::vector<std::byte> body_;
};

class CapturingProgressSink final
    : public realtime::ProcessingProgressSinkV2 {
public:
    bool PublishProcessingProgress(
        realtime::ProcessingProgressV2 progress) noexcept override {
        std::lock_guard<std::mutex> lock(mutex_);
        latest_ = progress;
        ++publication_count_;
        return true;
    }

    realtime::ProcessingProgressV2 latest() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return latest_;
    }

    std::uint64_t publication_count() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return publication_count_;
    }

private:
    mutable std::mutex mutex_;
    realtime::ProcessingProgressV2 latest_{};
    std::uint64_t publication_count_ = 0U;
};

struct ReplaySdkState final {
    mdl::MessageHandlerBase* handler = nullptr;
    std::vector<std::shared_ptr<OwnedTestMessage>> connect_messages;
    std::uint32_t shutdown_calls = 0U;
};

class ReplaySubscriber final : public sdk::SdkSubscriber {
public:
    explicit ReplaySubscriber(std::shared_ptr<ReplaySdkState> state)
        : state_(std::move(state)) {}

    void SetServerAddress(std::string_view) override {}
    void SetUserName(std::string_view) override {}
    void SetHeartbeatInterval(std::uint32_t) override {}
    void SetHeartbeatTimeout(std::uint32_t) override {}
    void SetMessageEncoding(mdl::MDLMessageEncoding) override {}
    void EnableMergeMessage(bool) override {}
    void SetSendMacAuth(bool) override {}
    void EnableServerSelect(bool) override {}
    void AddSubscription(const sdk::MessageKey&) override {}

    std::string Connect() override {
        for (const auto& message : state_->connect_messages) {
            state_->handler->OnMessage(nullptr, message.get());
        }
        return {};
    }

    bool Release(std::string* error) noexcept override {
        if (error != nullptr) {
            error->clear();
        }
        return true;
    }

private:
    std::shared_ptr<ReplaySdkState> state_;
};

class ReplayManager final : public sdk::SdkManager {
public:
    explicit ReplayManager(std::shared_ptr<ReplaySdkState> state)
        : state_(std::move(state)) {}

    void EnableLog(std::string_view, bool) override {}

    std::unique_ptr<sdk::SdkSubscriber> CreateSubscriber(
        mdl::MessageHandlerBase* handler,
        bool multithread_callback) override {
        if (multithread_callback) {
            return nullptr;
        }
        state_->handler = handler;
        return std::make_unique<ReplaySubscriber>(state_);
    }

    void Shutdown() override { ++state_->shutdown_calls; }

    bool Release(std::string* error) noexcept override {
        if (error != nullptr) {
            error->clear();
        }
        return true;
    }

private:
    std::shared_ptr<ReplaySdkState> state_;
};

class ReplayFactory final : public sdk::SdkFactory {
public:
    explicit ReplayFactory(std::shared_ptr<ReplaySdkState> state)
        : state_(std::move(state)) {}

    std::unique_ptr<sdk::SdkManager> Create(int, int) override {
        return std::make_unique<ReplayManager>(state_);
    }

private:
    std::shared_ptr<ReplaySdkState> state_;
};

struct PipelineCatalogFixture final {
    std::shared_ptr<const market::DailyInstrumentCatalogV2> catalog;
    std::unique_ptr<market::InstrumentRuntimeStateV2> runtime_state;
};

[[nodiscard]] bool MakeCatalogFixture(
    PipelineCatalogFixture* output) {
    if (output == nullptr) {
        return false;
    }
    *output = {};
    market::DailyInstrumentSourceEntryV2 source{};
    source.key.market = market::MarketV1::kShenzhen;
    source.key.security_id_source = {
        std::byte{'1'}, std::byte{'0'}, std::byte{'2'}, std::byte{' '}};
    source.key.security_id = {
        std::byte{'0'}, std::byte{'0'}, std::byte{'0'},
        std::byte{'0'}, std::byte{'0'}, std::byte{'1'}};
    source.metadata = market::InstrumentMetadataV2{
        market::QuantityUnitV1::kShare,
        market::SecurityTypeV1::kEquity,
        market::AssetScopeV1::kDocumentedCore};
    market::DailyInstrumentCatalogConfigV2 config{};
    config.trade_date = 20260724U;
    config.catalog_version = 1U;
    config.session_epoch = 171U;
    config.market_scope = market::kDailyCatalogMainlandScopeV2;
    config.coverage_complete = true;
    std::unique_ptr<market::DailyInstrumentCatalogV2> catalog;
    if (market::DailyInstrumentCatalogV2::Create(
            config, std::span(&source, 1U), &catalog) !=
            market::DailyInstrumentCatalogCreateErrorV2::kNone ||
        catalog == nullptr) {
        return false;
    }
    output->catalog =
        std::shared_ptr<const market::DailyInstrumentCatalogV2>(
            std::move(catalog));
    return market::InstrumentRuntimeStateV2::Create(
               *output->catalog, &output->runtime_state) ==
               market::InstrumentRuntimeStateErrorV2::kNone &&
           output->runtime_state != nullptr;
}

[[nodiscard]] bool MakeShanghaiCatalogFixture(
    PipelineCatalogFixture* output) {
    if (output == nullptr) {
        return false;
    }
    *output = {};
    market::DailyInstrumentSourceEntryV2 source{};
    source.key.market = market::MarketV1::kShanghai;
    source.key.security_id = {
        std::byte{'6'}, std::byte{'0'}, std::byte{'0'},
        std::byte{'0'}, std::byte{'0'}, std::byte{'1'}};
    source.metadata = market::InstrumentMetadataV2{
        market::QuantityUnitV1::kShare,
        market::SecurityTypeV1::kEquity,
        market::AssetScopeV1::kDocumentedCore};
    market::DailyInstrumentCatalogConfigV2 config{};
    config.trade_date = 20260724U;
    config.catalog_version = 1U;
    config.session_epoch = 172U;
    config.market_scope = market::kDailyCatalogMainlandScopeV2;
    config.coverage_complete = true;
    std::unique_ptr<market::DailyInstrumentCatalogV2> catalog;
    if (market::DailyInstrumentCatalogV2::Create(
            config, std::span(&source, 1U), &catalog) !=
            market::DailyInstrumentCatalogCreateErrorV2::kNone ||
        catalog == nullptr) {
        return false;
    }
    output->catalog =
        std::shared_ptr<const market::DailyInstrumentCatalogV2>(
            std::move(catalog));
    return market::InstrumentRuntimeStateV2::Create(
               *output->catalog, &output->runtime_state) ==
               market::InstrumentRuntimeStateErrorV2::kNone &&
           output->runtime_state != nullptr;
}

runtime::RealtimePipelineConfigV1 MakeConfig(
    const PipelineCatalogFixture& fixture) {
    runtime::RealtimePipelineConfigV1 config{};
    config.run_id[0U] = std::byte{0x51U};
    config.run_id[15U] = std::byte{0xa7U};
    config.trade_date = 20260724U;
    config.daily_catalog = fixture.catalog;
    config.runtime_state = fixture.runtime_state.get();
    config.source_stream_ids = {1001U, 1002U, 2001U, 2002U};
    config.maximum_sdk_message_bytes = 4096U;
    config.decoder_queue_capacity_per_source = 16U;
    config.store_worker_count = 1U;
    config.store_queue_capacity_per_source_worker = 16U;
    config.intraday_store.segment_target_bytes = 4U * 1024U;
    config.intraday_store.maximum_session_records = 64U;
    config.intraday_store.maximum_session_accounted_bytes =
        16U * 1024U * 1024U;
    config.intraday_store.maximum_records_per_batch = 4U;
    config.intraday_store.coverage_from_open = true;
    config.kline.windows.push_back(
        market::KLineWindowSpecV1{
            1U, market::kKLineNanosecondsPerSecondV1});
    config.enforce_receive_trade_date = false;
    config.sdk.enabled = true;
    config.sdk.server_address = "127.0.0.1:9112";
    config.sdk.user_name = "lifecycle-test";
    config.sdk.log_prefix = "lifecycle-test";
    config.sdk.message_encoding = mdl::MDLEID_BINARY;
    config.sdk.merge_message = false;
    return config;
}

std::size_t EventIndex(
    std::span<const LifecycleEvent> events,
    LifecycleEvent wanted) {
    const auto found = std::find(events.begin(), events.end(), wanted);
    return found == events.end()
               ? std::numeric_limits<std::size_t>::max()
               : static_cast<std::size_t>(found - events.begin());
}

void RunStartupRecoveryTests(TestContext* test) {
    if (test == nullptr) {
        return;
    }
    const auto message = [](
                             std::uint64_t sequence,
                             std::uint64_t price) {
        return std::make_shared<OwnedTestMessage>(
            sequence, sequence, price, 1U);
    };

    {
        PipelineCatalogFixture fixture;
        test->Expect(
            MakeCatalogFixture(&fixture),
            "startup coverage assertion fixture");
        auto source = std::make_shared<TestReplaySource>(
            std::vector<std::shared_ptr<OwnedTestMessage>>{});
        auto sdk_state = std::make_shared<ReplaySdkState>();
        runtime::RealtimePipelineConfigV1 config =
            MakeConfig(fixture);
        config.intraday_store.coverage_from_open = false;
        config.startup_replay_source = source;
        std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
        std::string detail;
        const auto error =
            runtime::RealtimePipelineV1::CreateForTest(
                std::move(config),
                std::make_shared<ReplayFactory>(sdk_state),
                &pipeline,
                &detail);
        test->Expect(
            error ==
                    runtime::RealtimePipelineCreateErrorV1::
                        kInvalidConfiguration &&
                pipeline == nullptr,
            "a replay source cannot manufacture coverage_from_open");
    }

    {
        PipelineCatalogFixture fixture;
        test->Expect(
            MakeCatalogFixture(&fixture),
            "post-handoff delayed callback fixture");
        auto sdk_state = std::make_shared<ReplaySdkState>();
        auto csv = message(450U, 100'000U);
        auto delayed_duplicate = message(450U, 100'000U);
        auto suffix_later = message(452U, 200'000U);
        auto suffix_earlier = message(451U, 150'000U);
        auto source = std::make_shared<TestReplaySource>(
            std::vector<std::shared_ptr<OwnedTestMessage>>{csv});
        runtime::RealtimePipelineConfigV1 config =
            MakeConfig(fixture);
        config.startup_replay_source = source;
        config.startup_live_buffer_maximum_messages = 4U;
        config.startup_live_buffer_maximum_bytes = 64U * 1024U;
        config.startup_overlap_retention_per_message = 4U;
        config.startup_warmup_timeout = 5s;
        config.startup_replay_backpressure_timeout = 2s;
        std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
        std::string detail;
        const auto error =
            runtime::RealtimePipelineV1::CreateForTest(
                std::move(config),
                std::make_shared<ReplayFactory>(sdk_state),
                &pipeline,
                &detail);
        if (pipeline != nullptr) {
            sdk_state->handler->OnMessage(
                nullptr, delayed_duplicate.get());
            sdk_state->handler->OnMessage(
                nullptr, suffix_later.get());
            sdk_state->handler->OnMessage(
                nullptr, suffix_earlier.get());
            pipeline->StopAndDrain();
        }
        test->Expect(
            error == runtime::RealtimePipelineCreateErrorV1::kNone &&
                pipeline != nullptr &&
                !pipeline->fatal() &&
                pipeline->Snapshot().store.appended_records == 3U,
            "a CSV duplicate delayed until after Create is suppressed, while opaque live suffix identities remain in callback order: " +
                detail);
    }

    {
        PipelineCatalogFixture fixture;
        test->Expect(
            MakeCatalogFixture(&fixture),
            "out-of-order replay identity fixture");
        const sdk::MessageKey key{6U, 101U, 36U};
        auto first = std::make_shared<OwnedTestMessage>(
            key,
            10U,
            ShenzhenTransactionBody(
                1U, 100'010U, 1U, 12U));
        auto high = std::make_shared<OwnedTestMessage>(
            key,
            30U,
            ShenzhenTransactionBody(
                1U, 100'030U, 1U, 13U));
        auto later_released = std::make_shared<OwnedTestMessage>(
            key,
            20U,
            ShenzhenTransactionBody(
                2U, 100'020U, 1U, 12U));
        auto delayed_prefix = std::make_shared<OwnedTestMessage>(
            key,
            25U,
            ShenzhenTransactionBody(
                2U, 100'025U, 1U, 13U));
        auto source = std::make_shared<TestReplaySource>(
            std::vector<std::shared_ptr<OwnedTestMessage>>{
                first, high, later_released});
        auto sdk_state = std::make_shared<ReplaySdkState>();
        runtime::RealtimePipelineConfigV1 config =
            MakeConfig(fixture);
        config.startup_replay_source = source;
        config.startup_live_buffer_maximum_messages = 4U;
        config.startup_live_buffer_maximum_bytes = 64U * 1024U;
        config.startup_overlap_retention_per_message = 4U;
        config.startup_warmup_timeout = 5s;
        config.startup_replay_backpressure_timeout = 2s;
        std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
        std::string detail;
        const auto error =
            runtime::RealtimePipelineV1::CreateForTest(
                std::move(config),
                std::make_shared<ReplayFactory>(sdk_state),
                &pipeline,
                &detail);
        if (pipeline != nullptr) {
            sdk_state->handler->OnMessage(
                nullptr, delayed_prefix.get());
        }
        test->Expect(
            error == runtime::RealtimePipelineCreateErrorV1::kNone &&
                pipeline != nullptr && pipeline->fatal() &&
                pipeline->Snapshot().store.appended_records == 3U,
            "replay may release SeqNo 30 before 20 across native channels, while the tuple cutoff remains the maximum 30: " +
                detail);
        if (pipeline != nullptr) {
            pipeline->StopAndDrain();
        }
    }

    {
        PipelineCatalogFixture fixture;
        test->Expect(
            MakeCatalogFixture(&fixture),
            "out-of-order retained-tail fixture");
        const sdk::MessageKey key{6U, 101U, 36U};
        auto delayed_low = std::make_shared<OwnedTestMessage>(
            key,
            1U,
            ShenzhenTransactionBody(
                2U, 100'001U, 1U, 7U));
        auto physical_tail_first =
            std::make_shared<OwnedTestMessage>(
                key,
                2U,
                ShenzhenTransactionBody(
                    1U, 100'002U, 1U, 8U));
        auto physical_tail_second =
            std::make_shared<OwnedTestMessage>(
                key,
                3U,
                ShenzhenTransactionBody(
                    1U, 100'003U, 1U, 7U));
        auto source = std::make_shared<TestReplaySource>(
            std::vector<std::shared_ptr<OwnedTestMessage>>{
                physical_tail_first,
                physical_tail_second,
                delayed_low});
        auto sdk_state = std::make_shared<ReplaySdkState>();
        sdk_state->connect_messages = {
            physical_tail_first, physical_tail_second};
        runtime::RealtimePipelineConfigV1 config =
            MakeConfig(fixture);
        config.startup_replay_source = source;
        config.startup_live_buffer_maximum_messages = 2U;
        config.startup_live_buffer_maximum_bytes = 64U * 1024U;
        config.startup_overlap_retention_per_message = 2U;
        config.startup_warmup_timeout = 5s;
        config.startup_replay_backpressure_timeout = 2s;
        std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
        std::string detail;
        const auto error =
            runtime::RealtimePipelineV1::CreateForTest(
                std::move(config),
                std::make_shared<ReplayFactory>(sdk_state),
                &pipeline,
                &detail);
        test->Expect(
            error == runtime::RealtimePipelineCreateErrorV1::kNone &&
                pipeline != nullptr &&
                pipeline->Snapshot().store.appended_records == 3U,
            "fingerprint retention keeps the greatest SequenceIDs rather than the last Publish calls when native repair releases 2,3,1: " +
                detail);
        if (pipeline != nullptr) {
            pipeline->StopAndDrain();
        }
    }

    {
        PipelineCatalogFixture fixture;
        test->Expect(
            MakeCatalogFixture(&fixture),
            "post-handoff conflict fixture");
        auto sdk_state = std::make_shared<ReplaySdkState>();
        auto csv = message(460U, 100'000U);
        auto conflict = message(460U, 100'001U);
        auto source = std::make_shared<TestReplaySource>(
            std::vector<std::shared_ptr<OwnedTestMessage>>{csv});
        runtime::RealtimePipelineConfigV1 config =
            MakeConfig(fixture);
        config.startup_replay_source = source;
        config.startup_live_buffer_maximum_messages = 2U;
        config.startup_live_buffer_maximum_bytes = 64U * 1024U;
        config.startup_overlap_retention_per_message = 2U;
        config.startup_warmup_timeout = 5s;
        config.startup_replay_backpressure_timeout = 2s;
        std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
        std::string detail;
        const auto error =
            runtime::RealtimePipelineV1::CreateForTest(
                std::move(config),
                std::make_shared<ReplayFactory>(sdk_state),
                &pipeline,
                &detail);
        if (pipeline != nullptr) {
            sdk_state->handler->OnMessage(nullptr, conflict.get());
        }
        test->Expect(
            error == runtime::RealtimePipelineCreateErrorV1::kNone &&
                pipeline != nullptr && pipeline->fatal() &&
                pipeline->Snapshot().store.appended_records == 1U,
            "a delayed post-Create callback with the CSV identity but a different semantic payload fails the session");
        if (pipeline != nullptr) {
            pipeline->StopAndDrain();
        }
    }

    {
        PipelineCatalogFixture fixture;
        test->Expect(
            MakeCatalogFixture(&fixture),
            "post-handoff evicted-prefix fixture");
        auto sdk_state = std::make_shared<ReplaySdkState>();
        std::vector<std::shared_ptr<OwnedTestMessage>> replay{
            std::make_shared<OwnedTestMessage>(
                470U, 1U, 100'470U),
            std::make_shared<OwnedTestMessage>(
                471U, 2U, 100'471U),
            std::make_shared<OwnedTestMessage>(
                472U, 3U, 100'472U)};
        auto delayed_evicted = std::make_shared<OwnedTestMessage>(
            470U, 1U, 100'470U);
        auto source =
            std::make_shared<TestReplaySource>(replay);
        runtime::RealtimePipelineConfigV1 config =
            MakeConfig(fixture);
        config.startup_replay_source = source;
        config.startup_live_buffer_maximum_messages = 2U;
        config.startup_live_buffer_maximum_bytes = 64U * 1024U;
        config.startup_overlap_retention_per_message = 2U;
        config.startup_warmup_timeout = 5s;
        config.startup_replay_backpressure_timeout = 2s;
        std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
        std::string detail;
        const auto error =
            runtime::RealtimePipelineV1::CreateForTest(
                std::move(config),
                std::make_shared<ReplayFactory>(sdk_state),
                &pipeline,
                &detail);
        if (pipeline != nullptr) {
            sdk_state->handler->OnMessage(
                nullptr, delayed_evicted.get());
        }
        test->Expect(
            error == runtime::RealtimePipelineCreateErrorV1::kNone &&
                pipeline != nullptr && pipeline->fatal() &&
                pipeline->Snapshot().store.appended_records == 3U,
            "a post-Create identity at or below the CSV cutoff fails closed when its bounded fingerprint was evicted");
        if (pipeline != nullptr) {
            pipeline->StopAndDrain();
        }
    }

    {
        PipelineCatalogFixture fixture;
        test->Expect(
            MakeCatalogFixture(&fixture),
            "startup cooperative-cancel fixture");
        auto csv = message(90U, 90'000U);
        auto cancel_requested = std::make_shared<bool>(false);
        auto source = std::make_shared<TestReplaySource>(
            std::vector<std::shared_ptr<OwnedTestMessage>>{csv},
            std::vector<sdk::MessageKey>{csv->key()},
            [cancel_requested]() {
                *cancel_requested = true;
            });
        auto sdk_state = std::make_shared<ReplaySdkState>();
        runtime::RealtimePipelineConfigV1 config =
            MakeConfig(fixture);
        config.startup_replay_source = source;
        config.startup_cancel_requested =
            [cancel_requested]() noexcept {
                return *cancel_requested;
            };
        config.startup_live_buffer_maximum_messages = 2U;
        config.startup_live_buffer_maximum_bytes = 64U * 1024U;
        config.startup_overlap_retention_per_message = 2U;
        config.startup_warmup_timeout = 5s;
        config.startup_replay_backpressure_timeout = 2s;
        std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
        std::string detail;
        const auto error =
            runtime::RealtimePipelineV1::CreateForTest(
                std::move(config),
                std::make_shared<ReplayFactory>(sdk_state),
                &pipeline,
                &detail);
        test->Expect(
            error ==
                    runtime::RealtimePipelineCreateErrorV1::
                        kStartupCancelled &&
                pipeline == nullptr &&
                sdk_state->shutdown_calls == 1U,
            "cooperative cancellation aborts recovery before ACTIVE and shuts down the SDK");
    }

    {
        PipelineCatalogFixture fixture;
        test->Expect(
            MakeCatalogFixture(&fixture),
            "startup opaque live SequenceID fixture");
        auto sdk_state = std::make_shared<ReplaySdkState>();
        auto csv = message(450U, 100'000U);
        auto suffix_later =
            std::make_shared<OwnedTestMessage>(
                452U, 451U, 200'000U);
        auto suffix_earlier =
            std::make_shared<OwnedTestMessage>(
                451U, 452U, 150'000U);
        auto source = std::make_shared<TestReplaySource>(
            std::vector<std::shared_ptr<OwnedTestMessage>>{csv},
            std::vector<sdk::MessageKey>{csv->key()},
            [sdk_state, suffix_later, suffix_earlier]() {
                sdk_state->handler->OnMessage(
                    nullptr, suffix_later.get());
                sdk_state->handler->OnMessage(
                    nullptr, suffix_earlier.get());
            });
        runtime::RealtimePipelineConfigV1 config =
            MakeConfig(fixture);
        config.startup_replay_source = source;
        config.startup_live_buffer_maximum_messages = 4U;
        config.startup_live_buffer_maximum_bytes = 64U * 1024U;
        config.startup_overlap_retention_per_message = 4U;
        config.startup_warmup_timeout = 5s;
        config.startup_replay_backpressure_timeout = 2s;
        std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
        std::string detail;
        const auto error =
            runtime::RealtimePipelineV1::CreateForTest(
                std::move(config),
                std::make_shared<ReplayFactory>(sdk_state),
                &pipeline,
                &detail);
        test->Expect(
            error == runtime::RealtimePipelineCreateErrorV1::kNone &&
                pipeline != nullptr &&
                pipeline->Snapshot().store.appended_records == 3U,
            "live callback order is preserved without inventing an undocumented SequenceID monotonicity rule: " +
                detail);
        if (pipeline != nullptr) {
            pipeline->StopAndDrain();
        }
    }

    {
        PipelineCatalogFixture fixture;
        test->Expect(
            MakeCatalogFixture(&fixture),
            "startup recovery success fixture");
        auto csv = message(100U, 100'000U);
        auto overlap = message(100U, 100'000U);
        auto live = message(101U, 200'000U);
        auto source = std::make_shared<TestReplaySource>(
            std::vector<std::shared_ptr<OwnedTestMessage>>{csv});
        auto sdk_state = std::make_shared<ReplaySdkState>();
        sdk_state->connect_messages = {overlap, live};
        auto progress =
            std::make_shared<CapturingProgressSink>();
        runtime::RealtimePipelineConfigV1 config =
            MakeConfig(fixture);
        config.startup_replay_source = source;
        config.processing_progress_sink = progress;
        config.startup_live_buffer_maximum_messages = 4U;
        config.startup_live_buffer_maximum_bytes = 64U * 1024U;
        config.startup_overlap_retention_per_message = 4U;
        config.startup_warmup_timeout = 5s;
        config.startup_replay_backpressure_timeout = 2s;
        std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
        std::string detail;
        const auto error =
            runtime::RealtimePipelineV1::CreateForTest(
                std::move(config),
                std::make_shared<ReplayFactory>(sdk_state),
                &pipeline,
                &detail);
        test->Expect(
            error == runtime::RealtimePipelineCreateErrorV1::kNone &&
                pipeline != nullptr,
            "CSV replay and overlapping synchronous callbacks recover: " +
                detail);
        const realtime::ProcessingProgressV2 ready_progress =
            progress->latest();
        test->Expect(
            progress->publication_count() != 0U &&
                ready_progress.accepted_sequence == 2U &&
                ready_progress.applied_sequence == 2U,
            "recovery Create synchronously publishes its fully applied query-ready prefix");
        if (pipeline != nullptr) {
            const runtime::RealtimePipelineSnapshotV1 snapshot =
                pipeline->Snapshot();
            test->Expect(
                snapshot.accepted_messages == 2U &&
                    snapshot.decoded_messages == 2U &&
                    snapshot.store.appended_records == 2U,
                "closed handoff drops the exact overlap before assigning a process sequence");
            market::RealtimeLatestRecordViewV1 latest{};
            const bool latest_query_ok =
                pipeline->GetLatestTick(1U, &latest) ==
                    market::RealtimeLatestQueryErrorV1::kNone &&
                latest.available() && latest.record != nullptr &&
                latest.record->ingress_sequence() == 2U;
            const auto* latest_event =
                latest_query_ok
                    ? market::StoredMarketEventGetV1<
                          market::ShenzhenTransactionV1>(
                          latest.record->event())
                    : nullptr;
            test->Expect(
                latest_event != nullptr &&
                    latest_event->fields.price.raw == 200'000,
                "Create returns only after latest query exposes the recovered CSV+live prefix");
            const runtime::RealtimePipelineCutResultV1 terminal =
                pipeline->StopAndPublishFinalGeneration(2s);
            market::IntradayInstrumentSummaryV1 row{};
            std::unique_ptr<market::IntradayInstrumentCursorV1>
                cursor;
            std::array<const market::RealtimeHistoryRecordV1*, 2U>
                records{};
            std::size_t written = 0U;
            const std::uint64_t recovered_notice =
                market::MarketNoticeBitV1(
                    market::MarketNoticeV1::kRecoveredFromCsv);
            bool records_ok = false;
            bool recovered_first = false;
            bool live_second = false;
            bool latest_price_ok = false;
            bool ingress_order_ok = false;
            if (terminal.published() &&
                terminal.store_generation->Find(1U, &row) ==
                    market::IntradayInstrumentStoreQueryErrorV1::
                        kNone &&
                terminal.store_generation->OpenTailCursor(
                    1U, row.record_count, &cursor) ==
                    market::IntradayInstrumentStoreQueryErrorV1::
                        kNone &&
                cursor != nullptr &&
                cursor->ReadBatch(records, &written) ==
                    market::IntradayInstrumentStoreQueryErrorV1::
                        kNone &&
                written == 2U) {
                const auto* first =
                    market::StoredMarketEventGetV1<
                        market::ShenzhenTransactionV1>(
                        records[1U]->event());
                const auto* second =
                    market::StoredMarketEventGetV1<
                        market::ShenzhenTransactionV1>(
                        records[0U]->event());
                records_ok =
                    first != nullptr && second != nullptr &&
                    (ingress_order_ok =
                         records[1U]->ingress_sequence() == 1U &&
                         records[0U]->ingress_sequence() == 2U) &&
                    (recovered_first =
                         (first->common.market_notices &
                          recovered_notice) != 0U) &&
                    (live_second =
                         (second->common.market_notices &
                          recovered_notice) == 0U) &&
                    (latest_price_ok =
                         second->fields.price.raw == 200'000);
            }
            test->Expect(
                terminal.published(),
                "recovered pipeline publishes its final generation");
            test->Expect(
                terminal.published() && row.record_count == 2U,
                "recovered final generation contains two records");
            test->Expect(
                terminal.published() &&
                    terminal.store_generation->coverage_from_open(),
                "explicit recovery coverage assertion reaches the Store only after successful startup");
            test->Expect(
                ingress_order_ok,
                "CSV/live records retain process ingress order");
            test->Expect(
                recovered_first,
                "CSV record carries recovered provenance");
            test->Expect(
                live_second,
                "non-overlap live record does not carry CSV provenance");
            test->Expect(
                latest_price_ok,
                "non-overlap live payload becomes latest");
            test->Expect(
                records_ok,
                "CSV is applied before non-overlap live data with recovery provenance and correct latest payload");
        }
    }

    {
        PipelineCatalogFixture fixture;
        test->Expect(
            MakeCatalogFixture(&fixture),
            "startup fenced live-suffix fixture");
        auto sdk_state = std::make_shared<ReplaySdkState>();
        auto csv = message(150U, 100'000U);
        auto suffix = message(151U, 200'000U);
        auto source = std::make_shared<TestReplaySource>(
            std::vector<std::shared_ptr<OwnedTestMessage>>{csv},
            std::vector<sdk::MessageKey>{suffix->key()},
            [sdk_state, suffix]() {
                sdk_state->handler->OnMessage(
                    nullptr, suffix.get());
            });
        runtime::RealtimePipelineConfigV1 config =
            MakeConfig(fixture);
        config.startup_replay_source = source;
        config.startup_live_buffer_maximum_messages = 4U;
        config.startup_live_buffer_maximum_bytes = 64U * 1024U;
        config.startup_overlap_retention_per_message = 4U;
        config.startup_warmup_timeout = 5s;
        config.startup_replay_backpressure_timeout = 2s;
        std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
        std::string detail;
        const auto error =
            runtime::RealtimePipelineV1::CreateForTest(
                std::move(config),
                std::make_shared<ReplayFactory>(sdk_state),
                &pipeline,
                &detail);
        test->Expect(
            error == runtime::RealtimePipelineCreateErrorV1::kNone &&
                pipeline != nullptr &&
                pipeline->Snapshot().store.appended_records == 2U,
            "callback copied after its CSV cutoff is a valid pure live suffix without a manufactured overlap: " +
                detail);
        if (pipeline != nullptr) {
            pipeline->StopAndDrain();
        }
    }

    {
        PipelineCatalogFixture fixture;
        test->Expect(
            MakeCatalogFixture(&fixture),
            "startup fenced pre-cutoff-missing fixture");
        auto sdk_state = std::make_shared<ReplaySdkState>();
        sdk_state->connect_messages = {
            message(251U, 200'000U)};
        auto csv = message(250U, 100'000U);
        auto source = std::make_shared<TestReplaySource>(
            std::vector<std::shared_ptr<OwnedTestMessage>>{csv},
            std::vector<sdk::MessageKey>{csv->key()});
        runtime::RealtimePipelineConfigV1 config =
            MakeConfig(fixture);
        config.startup_replay_source = source;
        config.startup_live_buffer_maximum_messages = 4U;
        config.startup_live_buffer_maximum_bytes = 64U * 1024U;
        config.startup_overlap_retention_per_message = 4U;
        config.startup_warmup_timeout = 5s;
        config.startup_replay_backpressure_timeout = 2s;
        std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
        std::string detail;
        const auto error =
            runtime::RealtimePipelineV1::CreateForTest(
                std::move(config),
                std::make_shared<ReplayFactory>(sdk_state),
                &pipeline,
                &detail);
        test->Expect(
            error ==
                    runtime::RealtimePipelineCreateErrorV1::
                        kStartupOverlapMissing &&
                pipeline == nullptr,
            "callback copied no later than the CSV cutoff still requires an exact retained prefix match");
    }

    {
        PipelineCatalogFixture fixture;
        test->Expect(
            MakeCatalogFixture(&fixture),
            "startup fenced non-monotone handoff fixture");
        auto sdk_state = std::make_shared<ReplaySdkState>();
        auto csv = message(350U, 100'000U);
        auto suffix = message(351U, 200'000U);
        auto late_prefix = message(350U, 100'000U);
        auto source = std::make_shared<TestReplaySource>(
            std::vector<std::shared_ptr<OwnedTestMessage>>{csv},
            std::vector<sdk::MessageKey>{csv->key()},
            [sdk_state, suffix, late_prefix]() {
                sdk_state->handler->OnMessage(
                    nullptr, suffix.get());
                sdk_state->handler->OnMessage(
                    nullptr, late_prefix.get());
            });
        runtime::RealtimePipelineConfigV1 config =
            MakeConfig(fixture);
        config.startup_replay_source = source;
        config.startup_live_buffer_maximum_messages = 4U;
        config.startup_live_buffer_maximum_bytes = 64U * 1024U;
        config.startup_overlap_retention_per_message = 4U;
        config.startup_warmup_timeout = 5s;
        config.startup_replay_backpressure_timeout = 2s;
        std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
        std::string detail;
        const auto error =
            runtime::RealtimePipelineV1::CreateForTest(
                std::move(config),
                std::make_shared<ReplayFactory>(sdk_state),
                &pipeline,
                &detail);
        test->Expect(
            error ==
                    runtime::RealtimePipelineCreateErrorV1::
                        kStartupOverlapConflict &&
                pipeline == nullptr,
            "handoff cannot return to the CSV prefix after entering the fenced live suffix");
    }

    {
        PipelineCatalogFixture fixture;
        test->Expect(
            MakeCatalogFixture(&fixture),
            "startup evicted-prefix identity fixture");
        auto sdk_state = std::make_shared<ReplaySdkState>();
        std::vector<std::shared_ptr<OwnedTestMessage>> replay{
            message(400U, 100'400U),
            message(401U, 100'401U),
            message(402U, 100'402U),
            message(403U, 100'403U)};
        auto evicted_prefix = message(400U, 100'400U);
        auto source = std::make_shared<TestReplaySource>(
            replay,
            std::vector<sdk::MessageKey>{
                evicted_prefix->key()},
            [sdk_state, evicted_prefix]() {
                sdk_state->handler->OnMessage(
                    nullptr, evicted_prefix.get());
            });
        runtime::RealtimePipelineConfigV1 config =
            MakeConfig(fixture);
        config.startup_replay_source = source;
        config.startup_live_buffer_maximum_messages = 2U;
        config.startup_live_buffer_maximum_bytes = 64U * 1024U;
        config.startup_overlap_retention_per_message = 2U;
        config.startup_warmup_timeout = 5s;
        config.startup_replay_backpressure_timeout = 2s;
        std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
        std::string detail;
        const auto error =
            runtime::RealtimePipelineV1::CreateForTest(
                std::move(config),
                std::make_shared<ReplayFactory>(sdk_state),
                &pipeline,
                &detail);
        test->Expect(
            error ==
                    runtime::RealtimePipelineCreateErrorV1::
                        kStartupOverlapMissing &&
                pipeline == nullptr,
            "a callback at or below the replay tuple cutoff cannot become a duplicate merely because its fingerprint was evicted");
    }

    {
        PipelineCatalogFixture fixture;
        test->Expect(
            MakeCatalogFixture(&fixture),
            "startup overlap-conflict fixture");
        auto source = std::make_shared<TestReplaySource>(
            std::vector<std::shared_ptr<OwnedTestMessage>>{
                message(200U, 100'000U)});
        auto sdk_state = std::make_shared<ReplaySdkState>();
        sdk_state->connect_messages = {
            message(200U, 100'001U)};
        runtime::RealtimePipelineConfigV1 config =
            MakeConfig(fixture);
        config.startup_replay_source = source;
        config.startup_live_buffer_maximum_messages = 2U;
        config.startup_live_buffer_maximum_bytes = 64U * 1024U;
        config.startup_overlap_retention_per_message = 2U;
        config.startup_warmup_timeout = 5s;
        config.startup_replay_backpressure_timeout = 2s;
        std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
        std::string detail;
        const auto error =
            runtime::RealtimePipelineV1::CreateForTest(
                std::move(config),
                std::make_shared<ReplayFactory>(sdk_state),
                &pipeline,
                &detail);
        test->Expect(
            error ==
                    runtime::RealtimePipelineCreateErrorV1::
                        kStartupOverlapConflict &&
                pipeline == nullptr && sdk_state->shutdown_calls == 1U &&
                detail.find("different payloads") !=
                    std::string::npos,
            "same tuple/SequenceID with another semantic payload fails startup and shuts down SDK");
    }

    {
        PipelineCatalogFixture fixture;
        test->Expect(
            MakeCatalogFixture(&fixture),
            "startup local-time conflict fixture");
        auto csv = std::make_shared<OwnedTestMessage>(
            210U, 210U, 100'000U, 1U, 93'000'000U);
        auto live = std::make_shared<OwnedTestMessage>(
            210U, 210U, 100'000U, 1U, 93'000'001U);
        auto source = std::make_shared<TestReplaySource>(
            std::vector<std::shared_ptr<OwnedTestMessage>>{csv});
        auto sdk_state = std::make_shared<ReplaySdkState>();
        sdk_state->connect_messages = {live};
        runtime::RealtimePipelineConfigV1 config =
            MakeConfig(fixture);
        config.startup_replay_source = source;
        config.startup_live_buffer_maximum_messages = 2U;
        config.startup_live_buffer_maximum_bytes = 64U * 1024U;
        config.startup_overlap_retention_per_message = 2U;
        config.startup_warmup_timeout = 5s;
        config.startup_replay_backpressure_timeout = 2s;
        std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
        std::string detail;
        const auto error =
            runtime::RealtimePipelineV1::CreateForTest(
                std::move(config),
                std::make_shared<ReplayFactory>(sdk_state),
                &pipeline,
                &detail);
        test->Expect(
            error ==
                    runtime::RealtimePipelineCreateErrorV1::
                        kStartupOverlapConflict &&
                pipeline == nullptr,
            "same tuple/SequenceID/body with a different vendor LocalTime is not an exact duplicate");
    }

    {
        PipelineCatalogFixture fixture;
        test->Expect(
            MakeCatalogFixture(&fixture),
            "startup missing-overlap fixture");
        auto source = std::make_shared<TestReplaySource>(
            std::vector<std::shared_ptr<OwnedTestMessage>>{
                message(300U, 100'000U)});
        auto sdk_state = std::make_shared<ReplaySdkState>();
        sdk_state->connect_messages = {
            message(301U, 100'001U)};
        runtime::RealtimePipelineConfigV1 config =
            MakeConfig(fixture);
        config.startup_replay_source = source;
        config.startup_live_buffer_maximum_messages = 2U;
        config.startup_live_buffer_maximum_bytes = 64U * 1024U;
        config.startup_overlap_retention_per_message = 2U;
        config.startup_warmup_timeout = 5s;
        config.startup_replay_backpressure_timeout = 2s;
        std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
        std::string detail;
        const auto error =
            runtime::RealtimePipelineV1::CreateForTest(
                std::move(config),
                std::make_shared<ReplayFactory>(sdk_state),
                &pipeline,
                &detail);
        test->Expect(
            error ==
                    runtime::RealtimePipelineCreateErrorV1::
                        kStartupOverlapMissing &&
                pipeline == nullptr && sdk_state->shutdown_calls == 1U,
            "a buffered tuple without any proven CSV overlap fails startup");
    }

    {
        PipelineCatalogFixture fixture;
        test->Expect(
            MakeCatalogFixture(&fixture),
            "startup buffer-overflow fixture");
        auto source = std::make_shared<TestReplaySource>(
            std::vector<std::shared_ptr<OwnedTestMessage>>{
                message(400U, 100'000U)});
        auto sdk_state = std::make_shared<ReplaySdkState>();
        sdk_state->connect_messages = {
            message(400U, 100'000U),
            message(401U, 100'001U)};
        runtime::RealtimePipelineConfigV1 config =
            MakeConfig(fixture);
        config.startup_replay_source = source;
        config.startup_live_buffer_maximum_messages = 1U;
        config.startup_live_buffer_maximum_bytes = 64U * 1024U;
        config.startup_overlap_retention_per_message = 1U;
        config.startup_warmup_timeout = 5s;
        config.startup_replay_backpressure_timeout = 2s;
        std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
        std::string detail;
        const auto error =
            runtime::RealtimePipelineV1::CreateForTest(
                std::move(config),
                std::make_shared<ReplayFactory>(sdk_state),
                &pipeline,
                &detail);
        test->Expect(
            error ==
                    runtime::RealtimePipelineCreateErrorV1::
                        kStartupBufferOverflow &&
                pipeline == nullptr && sdk_state->shutdown_calls == 1U,
            "startup raw buffer message bound fails explicitly and safely");
    }

    {
        PipelineCatalogFixture fixture;
        test->Expect(
            MakeCatalogFixture(&fixture),
            "startup tiny-queue fixture");
        std::vector<std::shared_ptr<OwnedTestMessage>> replay;
        replay.reserve(32U);
        for (std::uint64_t sequence = 1U;
             sequence <= 32U;
             ++sequence) {
            replay.push_back(message(
                500U + sequence,
                100'000U + sequence));
        }
        auto source =
            std::make_shared<TestReplaySource>(replay);
        auto sdk_state = std::make_shared<ReplaySdkState>();
        sdk_state->connect_messages = {replay.back()};
        runtime::RealtimePipelineConfigV1 config =
            MakeConfig(fixture);
        config.startup_replay_source = source;
        config.startup_live_buffer_maximum_messages = 2U;
        config.startup_live_buffer_maximum_bytes = 64U * 1024U;
        config.startup_overlap_retention_per_message = 32U;
        config.startup_warmup_timeout = 5s;
        config.startup_replay_backpressure_timeout = 2s;
        config.decoder_queue_capacity_per_source = 1U;
        config.store_queue_capacity_per_source_worker = 1U;
        std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
        std::string detail;
        const auto error =
            runtime::RealtimePipelineV1::CreateForTest(
                std::move(config),
                std::make_shared<ReplayFactory>(sdk_state),
                &pipeline,
                &detail);
        test->Expect(
            error == runtime::RealtimePipelineCreateErrorV1::kNone &&
                pipeline != nullptr &&
                pipeline->Snapshot().store.appended_records == 32U &&
                !pipeline->fatal(),
            "bulk replay waits through decoder and History backpressure instead of treating a tiny queue as fatal: " +
                detail);
        if (pipeline != nullptr) {
            pipeline->StopAndDrain();
        }
    }

    {
        PipelineCatalogFixture fixture;
        test->Expect(
            MakeCatalogFixture(&fixture),
            "startup retained-tail fixture");
        constexpr std::size_t kReplayCount = 8193U;
        constexpr std::size_t kBufferedCount = 4097U;
        std::vector<std::shared_ptr<OwnedTestMessage>> replay;
        replay.reserve(kReplayCount);
        for (std::size_t index = 0U;
             index < kReplayCount;
             ++index) {
            const std::uint64_t sequence =
                10'000U + static_cast<std::uint64_t>(index);
            replay.push_back(message(
                sequence, 100'000U + sequence));
        }
        auto source =
            std::make_shared<TestReplaySource>(replay);
        auto sdk_state = std::make_shared<ReplaySdkState>();
        sdk_state->connect_messages.insert(
            sdk_state->connect_messages.end(),
            replay.end() -
                static_cast<std::ptrdiff_t>(kBufferedCount),
            replay.end());
        runtime::RealtimePipelineConfigV1 config =
            MakeConfig(fixture);
        config.startup_replay_source = source;
        config.startup_live_buffer_maximum_messages =
            kBufferedCount;
        config.startup_live_buffer_maximum_bytes =
            2U * 1024U * 1024U;
        config.startup_overlap_retention_per_message =
            kBufferedCount;
        config.startup_warmup_timeout = 20s;
        config.startup_replay_backpressure_timeout = 2s;
        config.intraday_store.maximum_session_records =
            kReplayCount;
        config.intraday_store.maximum_session_accounted_bytes =
            256U * 1024U * 1024U;
        std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
        std::string detail;
        const auto error =
            runtime::RealtimePipelineV1::CreateForTest(
                std::move(config),
                std::make_shared<ReplayFactory>(sdk_state),
                &pipeline,
                &detail);
        test->Expect(
            error == runtime::RealtimePipelineCreateErrorV1::kNone &&
                pipeline != nullptr &&
                pipeline->Snapshot().store.appended_records ==
                    kReplayCount &&
                pipeline->Snapshot().accepted_messages ==
                    kReplayCount,
            "a >4096-message buffered overlap remains in the bounded tail and creates no duplicate Store records: " +
                detail);
        if (pipeline != nullptr) {
            pipeline->StopAndDrain();
        }
    }

    {
        PipelineCatalogFixture fixture;
        test->Expect(
            MakeShanghaiCatalogFixture(&fixture),
            "startup Shanghai stateful-fingerprint fixture");
        const sdk::MessageKey sh_tick_key{4U, 101U, 24U};
        auto early = std::make_shared<OwnedTestMessage>(
            sh_tick_key,
            30'000U,
            ShanghaiTickBody(1U, "A", "B"));
        auto status = std::make_shared<OwnedTestMessage>(
            sh_tick_key,
            30'001U,
            ShanghaiTickBody(2U, "S", "TRADE"));
        auto source = std::make_shared<TestReplaySource>(
            std::vector<std::shared_ptr<OwnedTestMessage>>{
                early, status});
        auto sdk_state = std::make_shared<ReplaySdkState>();
        sdk_state->connect_messages = {
            std::make_shared<OwnedTestMessage>(
                sh_tick_key,
                30'000U,
                ShanghaiTickBody(1U, "A", "B"))};
        runtime::RealtimePipelineConfigV1 config =
            MakeConfig(fixture);
        config.startup_replay_source = source;
        config.startup_live_buffer_maximum_messages = 2U;
        config.startup_live_buffer_maximum_bytes = 64U * 1024U;
        config.startup_overlap_retention_per_message = 2U;
        config.startup_warmup_timeout = 5s;
        config.startup_replay_backpressure_timeout = 2s;
        std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
        std::string detail;
        const auto error =
            runtime::RealtimePipelineV1::CreateForTest(
                std::move(config),
                std::make_shared<ReplayFactory>(sdk_state),
                &pipeline,
                &detail);
        test->Expect(
            error == runtime::RealtimePipelineCreateErrorV1::kNone &&
                pipeline != nullptr &&
                pipeline->Snapshot().accepted_messages == 2U &&
                pipeline->Snapshot().store.appended_records == 2U,
            "Shanghai overlap fingerprint excludes decoder-state-derived phase after a later S transition: " +
                detail);
        if (pipeline != nullptr) {
            pipeline->StopAndDrain();
        }
    }
}

void RunOnlineIngressSeamTests(TestContext* test) {
    if (test == nullptr) {
        return;
    }

    {
        PipelineCatalogFixture fixture;
        test->Expect(
            MakeCatalogFixture(&fixture),
            "process-start partial SDK fixture");
        auto sdk_state = std::make_shared<ReplaySdkState>();
        sdk_state->connect_messages.push_back(
            std::make_shared<OwnedTestMessage>(
                650U, 650U, 165'000U, 2U));
        runtime::RealtimePipelineConfigV1 config = MakeConfig(fixture);
        config.intraday_store.coverage_from_open = false;
        config.kline.windows.clear();
        std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
        std::string detail;
        const auto create_error =
            runtime::RealtimePipelineV1::CreateForTest(
                std::move(config),
                std::make_shared<ReplayFactory>(sdk_state),
                &pipeline,
                &detail);
        runtime::RealtimePipelineCutResultV1 generation{};
        if (pipeline != nullptr) {
            generation = pipeline->CutAndPublishGeneration(3s);
        }
        market::IntradayInstrumentSummaryV1 row{};
        test->Expect(
            create_error ==
                    runtime::RealtimePipelineCreateErrorV1::kNone &&
                pipeline != nullptr && generation.published() &&
                generation.store_generation != nullptr &&
                !generation.store_generation->coverage_from_open() &&
                generation.store_generation->Find(1U, &row) ==
                    market::IntradayInstrumentStoreQueryErrorV1::kNone &&
                row.record_count == 1U &&
                pipeline->Snapshot().accepted_messages == 1U,
            "SDK live data advances a process-start partial Store without "
            "a replay source, WAL capture, or from-open claim: " + detail);
        if (pipeline != nullptr) {
            pipeline->StopAndDrain();
        }
    }

    {
        PipelineCatalogFixture fixture;
        test->Expect(
            MakeCatalogFixture(&fixture),
            "external shadow ingress fixture");
        if (fixture.runtime_state != nullptr) {
            runtime::RealtimePipelineConfigV1 config = MakeConfig(fixture);
            config.sdk.enabled = false;
            config.external_ingress_enabled = true;
            std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
            std::string detail;
            const auto create_error = runtime::RealtimePipelineV1::Create(
                std::move(config), &pipeline, &detail);
            auto message = std::make_shared<OwnedTestMessage>(
                700U, 700U, 170'000U, 3U);
            runtime::RealtimePipelineExternalIngressV1 input{};
            input.message = message.get();
            input.recv_realtime_ns = 1'000U;
            input.recv_monotonic_ns = 2'000U;
            input.admission_timeout = 2s;
            runtime::RealtimePipelineIngressResultV1 ingress{};
            if (pipeline != nullptr) {
                ingress = pipeline->IngestExternalMessage(input);
            }
            runtime::RealtimePipelineCutResultV1 generation{};
            if (pipeline != nullptr && ingress.accepted()) {
                generation = pipeline->CutAndPublishGeneration(3s);
            }
            market::IntradayInstrumentSummaryV1 row{};
            test->Expect(
                create_error ==
                        runtime::RealtimePipelineCreateErrorV1::kNone &&
                    pipeline != nullptr && ingress.accepted() &&
                    ingress.global_ingress_sequence == 1U &&
                    ingress.source_sequence == 1U &&
                    generation.published() &&
                    generation.store_generation->Find(1U, &row) ==
                        market::IntradayInstrumentStoreQueryErrorV1::kNone &&
                    row.record_count == 1U,
                "SDK-less external ingress advances the complete shadow pipeline: " +
                    detail);
            if (pipeline != nullptr) {
                pipeline->StopAndDrain();
            }
        }
    }

    {
        PipelineCatalogFixture fixture;
        test->Expect(
            MakeCatalogFixture(&fixture),
            "SDK/external mutual exclusion fixture");
        auto sdk_state = std::make_shared<ReplaySdkState>();
        runtime::RealtimePipelineConfigV1 config = MakeConfig(fixture);
        config.external_ingress_enabled = true;
        std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
        std::string detail;
        test->Expect(
            runtime::RealtimePipelineV1::CreateForTest(
                std::move(config),
                std::make_shared<ReplayFactory>(sdk_state),
                &pipeline,
                &detail) ==
                    runtime::RealtimePipelineCreateErrorV1::
                        kInvalidConfiguration &&
                pipeline == nullptr,
            "one pipeline cannot own both physical SDK and external shadow ingress");
    }

    {
        PipelineCatalogFixture fixture;
        test->Expect(
            MakeCatalogFixture(&fixture),
            "blocking/capture mutual exclusion fixture");
        auto sdk_state = std::make_shared<ReplaySdkState>();
        auto source = std::make_shared<TestReplaySource>(
            std::vector<std::shared_ptr<OwnedTestMessage>>{});
        auto capture = std::make_shared<CaptureProbe>(true);
        runtime::RealtimePipelineConfigV1 config = MakeConfig(fixture);
        config.startup_replay_source = source;
        config.live_ingress_capture_sink = capture;
        std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
        std::string detail;
        test->Expect(
            runtime::RealtimePipelineV1::CreateForTest(
                std::move(config),
                std::make_shared<ReplayFactory>(sdk_state),
                &pipeline,
                &detail) ==
                    runtime::RealtimePipelineCreateErrorV1::
                        kInvalidConfiguration &&
                pipeline == nullptr && capture->calls() == 0U,
            "online capture and synchronous startup replay cannot share one owner");
    }

    {
        PipelineCatalogFixture fixture;
        test->Expect(
            MakeCatalogFixture(&fixture),
            "successful SDK capture fixture");
        auto sdk_state = std::make_shared<ReplaySdkState>();
        const std::vector<std::byte> expected_body =
            ShenzhenTransactionBody(801U, 180'000U, 4U);
        auto message = std::make_shared<OwnedTestMessage>(
            sdk::MessageKey{6U, 101U, 36U},
            800U,
            expected_body);
        sdk_state->connect_messages.push_back(message);
        auto capture = std::make_shared<CaptureProbe>(true);
        runtime::RealtimePipelineConfigV1 config = MakeConfig(fixture);
        config.live_ingress_capture_sink = capture;
        std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
        std::string detail;
        const auto create_error =
            runtime::RealtimePipelineV1::CreateForTest(
                std::move(config),
                std::make_shared<ReplayFactory>(sdk_state),
                &pipeline,
                &detail);
        runtime::RealtimePipelineCutResultV1 generation{};
        if (pipeline != nullptr) {
            generation = pipeline->CutAndPublishGeneration(3s);
        }
        test->Expect(
            create_error == runtime::RealtimePipelineCreateErrorV1::kNone &&
                pipeline != nullptr && capture->calls() == 1U &&
                capture->valid() &&
                capture->key() == sdk::MessageKey{6U, 101U, 36U} &&
                capture->source_slot() == 3U &&
                capture->vendor_sequence() == 800U &&
                capture->realtime_ns() != 0U &&
                capture->monotonic_ns() != 0U &&
                std::equal(
                    expected_body.begin(),
                    expected_body.end(),
                    capture->body().begin(),
                    capture->body().end()) &&
                generation.published() &&
                pipeline->Snapshot().accepted_messages == 1U,
            "Connect callback is independently deep-copied before the same message advances preview: " +
                detail);
        if (pipeline != nullptr) {
            pipeline->StopAndDrain();
        }
    }

    {
        PipelineCatalogFixture fixture;
        test->Expect(
            MakeCatalogFixture(&fixture),
            "failed SDK capture fixture");
        auto sdk_state = std::make_shared<ReplaySdkState>();
        sdk_state->connect_messages.push_back(
            std::make_shared<OwnedTestMessage>(
                900U, 900U, 190'000U, 5U));
        auto capture = std::make_shared<CaptureProbe>(false);
        runtime::RealtimePipelineConfigV1 config = MakeConfig(fixture);
        config.live_ingress_capture_sink = capture;
        std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
        std::string detail;
        const auto create_error =
            runtime::RealtimePipelineV1::CreateForTest(
                std::move(config),
                std::make_shared<ReplayFactory>(sdk_state),
                &pipeline,
                &detail);
        test->Expect(
            create_error ==
                    runtime::RealtimePipelineCreateErrorV1::
                        kSdkCallbackFailed &&
                pipeline == nullptr && capture->calls() == 1U &&
                capture->valid() && sdk_state->shutdown_calls == 1U,
            "capture refusal prevents preview admission and fails the sole SDK owner closed");
    }
}

}  // namespace

int main() {
    TestContext test;

    {
        std::unique_ptr<realtime::OwnedIngressMessagePoolV1> oversized;
        constexpr std::size_t kPrewarmBlockBytes = 8192U;
        const std::size_t excessive_count =
            realtime::kOwnedIngressMaximumPrewarmBytesV1 /
                kPrewarmBlockBytes +
            1U;
        const realtime::OwnedIngressMessageErrorV1 error =
            realtime::OwnedIngressMessagePoolV1::Create(
                realtime::OwnedIngressMessagePoolConfigV1{
                    4096U,
                    excessive_count,
                    4096U,
                    excessive_count},
                &oversized);
        test.Expect(
            error ==
                    realtime::OwnedIngressMessageErrorV1::
                        kInvalidPoolConfiguration &&
                oversized == nullptr,
            "prewarm rejects an eager reservation above its byte cap");
    }
    PipelineCatalogFixture fixture;
    test.Expect(
        MakeCatalogFixture(&fixture),
        "daily catalog/runtime creation");
    if (fixture.runtime_state == nullptr) {
        return 1;
    }

    {
        runtime::RealtimePipelineConfigV1 unordered_sdk =
            MakeConfig(fixture);
        unordered_sdk.sdk.io_threads = 2;
        std::unique_ptr<runtime::RealtimePipelineV1>
            rejected_pipeline;
        std::string rejected_detail;
        test.Expect(
            runtime::RealtimePipelineV1::CreateForTest(
                std::move(unordered_sdk),
                std::make_shared<RecordingFactory>(
                    std::make_shared<LifecycleState>()),
                &rejected_pipeline,
                &rejected_detail) ==
                    runtime::RealtimePipelineCreateErrorV1::
                        kInvalidConfiguration &&
                rejected_pipeline == nullptr,
            "production capture rejects multiple SDK I/O threads");
    }

    auto state = std::make_shared<LifecycleState>();
    std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
    std::string detail;
    test.Expect(
        runtime::RealtimePipelineV1::CreateForTest(
            MakeConfig(fixture),
            std::make_shared<RecordingFactory>(state),
            &pipeline,
            &detail) == runtime::RealtimePipelineCreateErrorV1::kNone &&
            pipeline != nullptr,
        "pipeline creation: " + detail);
    if (pipeline == nullptr) {
        return 1;
    }

    mdl::MessageHandlerBase* const handler = state->handler();
    test.Expect(handler != nullptr, "SDK callback handler installed");
    if (handler == nullptr) {
        pipeline->StopAndDrain();
        return 1;
    }

    auto message_gate = std::make_shared<OneShotGate>();
    BlockingMessage message(message_gate);
    std::thread callback_thread([&] {
        state->CallbackEnter();
        handler->OnMessage(nullptr, &message);
        state->CallbackExit();
    });

    const bool callback_blocked =
        message_gate->WaitUntilEntered(2s);
    test.Expect(
        callback_blocked,
        "callback reaches the owned-ingress accessor while holding admission");

    runtime::RealtimePipelineCutResultV1 terminal{};
    std::thread terminal_thread([&] {
        state->TerminalCallStarted();
        terminal = pipeline->StopAndPublishFinalGeneration(2s);
        state->TerminalReturn();
    });

    test.Expect(
        state->WaitForTerminalCallStarted(2s),
        "terminal call starts while the SDK callback is blocked");
    const LifecycleSnapshot while_callback_blocked = state->Snapshot();
    test.Expect(
        while_callback_blocked.subscriber_release_calls == 0U &&
            while_callback_blocked.manager_release_calls == 0U,
        "blocked admitted callback has not been followed by SDK release");
    message_gate->Open();
    callback_thread.join();

    const bool shutdown_quiesced = state->WaitForShutdownQuiesced(2s);
    test.Expect(
        shutdown_quiesced,
        "Shutdown observes callback quiescence before returning");
    const LifecycleSnapshot during_shutdown = state->Snapshot();
    test.Expect(
        during_shutdown.subscriber_release_calls == 0U &&
            during_shutdown.manager_release_calls == 0U &&
            EventIndex(
                during_shutdown.events,
                LifecycleEvent::kTerminalReturn) ==
                std::numeric_limits<std::size_t>::max(),
        "terminal cannot release either SDK object while Shutdown is blocked");
    state->AllowShutdownReturn();
    terminal_thread.join();

    test.Expect(
        terminal.published(),
        "terminal publishes after the admitted callback exits");
    if (terminal.published()) {
        const market::RealtimeHistoryWatermarkV1& watermark =
            terminal.store_generation->watermark();
        market::IntradayInstrumentSummaryV1 row{};
        const bool row_found =
            terminal.store_generation->Find(1U, &row) ==
            market::IntradayInstrumentStoreQueryErrorV1::kNone;
        test.Expect(
            watermark.ingress_sequence_exclusive == 2U &&
                watermark.sources[3U].sequence_exclusive == 2U &&
                watermark.sources[0U].sequence_exclusive == 1U &&
                watermark.sources[1U].sequence_exclusive == 1U &&
                watermark.sources[2U].sequence_exclusive == 1U,
            "final watermark includes the callback that entered admission first");

        std::unique_ptr<market::IntradayInstrumentCursorV1> tail;
        std::array<const market::RealtimeHistoryRecordV1*, 2U> records{};
        std::size_t written = 0U;
        test.Expect(
            row_found && row.latest_tick != nullptr &&
                row.record_count == 1U &&
                terminal.store_generation->OpenTailCursor(
                    1U, row.record_count, &tail) ==
                    market::IntradayInstrumentStoreQueryErrorV1::kNone &&
                tail != nullptr &&
                tail->ReadBatch(records, &written) ==
                    market::IntradayInstrumentStoreQueryErrorV1::kNone &&
                written == 1U && records[0U] == row.latest_tick &&
                records[0U]->ingress_sequence() == 1U,
            "final store contains exactly the admitted callback record");
        test.Expect(
            terminal.factor_generation->input_store().get() ==
                terminal.store_generation.get(),
            "factor generation retains the exact terminal store handle");
        std::unique_ptr<market::KLineCursorV1> kline_cursor;
        std::array<market::KLineBarV1, 1U> bars{};
        std::size_t bars_written = 0U;
        constexpr std::uint64_t k0930StartNs =
            (9ULL * 60ULL * 60ULL + 30ULL * 60ULL) *
            market::kKLineNanosecondsPerSecondV1;
        test.Expect(
            terminal.kline_generation != nullptr &&
                terminal.kline_generation->input_store() ==
                    terminal.store_generation &&
                terminal.kline_generation->OpenInstrumentCursor(
                    1U, 1U, &kline_cursor) ==
                    market::KLineQueryErrorV1::kNone &&
                kline_cursor != nullptr &&
                kline_cursor->ReadBatch(bars, &bars_written) ==
                    market::KLineQueryErrorV1::kNone &&
                bars_written == 1U &&
                bars[0U].window_start_ns_since_midnight ==
                    k0930StartNs &&
                bars[0U].open_price_p6 == 12'345'600 &&
                bars[0U].volume_raw == 33U &&
                bars[0U].trade_count == 1U,
            "terminal KLine contains the callback admitted before shutdown");
    }

    const runtime::RealtimePipelineSnapshotV1 pipeline_snapshot =
        pipeline->Snapshot();
    test.Expect(
        pipeline_snapshot.accepted_messages == 1U &&
            pipeline_snapshot.decoded_messages == 1U &&
            pipeline_snapshot.store.appended_records == 1U &&
            pipeline_snapshot.store.allocated_segments == 1U &&
            pipeline_snapshot.global_ingress_sequence == 1U &&
            pipeline_snapshot.source_sequences[3U] == 1U &&
            pipeline_snapshot.last_published_generation == 1U &&
            pipeline_snapshot.stopped && !pipeline_snapshot.accepting &&
            !pipeline_snapshot.fatal,
        "terminal state is the complete non-fatal one-message prefix");
    test.Expect(
        pipeline_snapshot.ingress_pool.maximum_inflight_messages != 0U &&
            pipeline_snapshot.ingress_pool.prewarm_message_bytes == 4096U &&
            pipeline_snapshot.ingress_pool.prewarm_message_count ==
                pipeline_snapshot.ingress_pool
                    .maximum_inflight_messages &&
            pipeline_snapshot.ingress_pool.active_messages == 0U &&
            pipeline_snapshot.ingress_pool.allocated_blocks ==
                pipeline_snapshot.ingress_pool.prewarm_message_count &&
            pipeline_snapshot.ingress_pool.allocated_blocks <=
                pipeline_snapshot.ingress_pool
                    .maximum_inflight_messages,
        "pool is prewarmed before callbacks and callback/stop race stays "
        "within its bounded storage");

    const LifecycleSnapshot lifecycle = state->Snapshot();
    const std::size_t callback_exit = EventIndex(
        lifecycle.events, LifecycleEvent::kCallbackExit);
    const std::size_t shutdown_begin = EventIndex(
        lifecycle.events, LifecycleEvent::kShutdownBegin);
    const std::size_t shutdown_end = EventIndex(
        lifecycle.events, LifecycleEvent::kShutdownEnd);
    const std::size_t subscriber_release = EventIndex(
        lifecycle.events, LifecycleEvent::kSubscriberRelease);
    const std::size_t manager_release = EventIndex(
        lifecycle.events, LifecycleEvent::kManagerRelease);
    const std::size_t terminal_return = EventIndex(
        lifecycle.events, LifecycleEvent::kTerminalReturn);
    test.Expect(
        callback_exit < subscriber_release,
        "Subscriber is not released before the SDK callback exits");
    test.Expect(
        shutdown_begin < shutdown_end &&
            shutdown_end < subscriber_release &&
            subscriber_release < manager_release &&
            manager_release < terminal_return,
        "lifecycle order is Shutdown, Subscriber Release, manager Release, terminal return");
    test.Expect(
        lifecycle.shutdown_calls == 1U &&
            lifecycle.subscriber_release_calls == 1U &&
            lifecycle.manager_release_calls == 1U,
        "each physical SDK lifecycle operation occurs exactly once");

    pipeline->StopAndDrain();
    pipeline.reset();
    const LifecycleSnapshot after_destruction = state->Snapshot();
    test.Expect(
        after_destruction.shutdown_calls == 1U &&
            after_destruction.subscriber_release_calls == 1U &&
            after_destruction.manager_release_calls == 1U,
        "idempotent stop/destruction does not repeat SDK lifecycle operations");

    RunStartupRecoveryTests(&test);
    RunOnlineIngressSeamTests(&test);

    return test.failures() == 0 ? 0 : 1;
}
