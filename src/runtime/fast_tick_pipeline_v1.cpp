#include "l2flow/runtime/fast_tick_pipeline_v1.h"

#include "l2flow/market/mainland_a_share_filter_v1.h"
#include "l2flow/sdk/direct_sdk_runtime_v1.h"
#include "l2flow/sdk/production_subscription_v1.h"
#include "l2flow/sdk/vendor_head_view.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <limits>
#include <new>
#include <string>
#include <system_error>
#include <thread>
#include <utility>

#include <time.h>

namespace l2flow::runtime {
namespace {

namespace market = l2flow::market;
namespace realtime = l2flow::realtime;
namespace sdk = l2flow::sdk;
namespace mdl = datayes::mdl;

template <typename T>
class SpscQueueV1 final {
public:
    explicit SpscQueueV1(std::size_t capacity)
        : slot_count_(capacity + 1U),
          slots_(std::make_unique<T[]>(slot_count_)) {}

    SpscQueueV1(const SpscQueueV1&) = delete;
    SpscQueueV1& operator=(const SpscQueueV1&) = delete;

    [[nodiscard]] bool TryPush(T&& value) noexcept {
        const std::size_t tail = tail_.load(std::memory_order_relaxed);
        const std::size_t next = Advance(tail);
        if (next == head_.load(std::memory_order_acquire)) {
            return false;
        }
        slots_[tail] = std::move(value);
        tail_.store(next, std::memory_order_release);
        return true;
    }

    [[nodiscard]] bool TryPop(T* output) noexcept {
        const std::size_t head = head_.load(std::memory_order_relaxed);
        const std::size_t tail = tail_.load(std::memory_order_acquire);
        if (head == tail) {
            return false;
        }
        *output = std::move(slots_[head]);
        head_.store(Advance(head), std::memory_order_release);
        return true;
    }

    [[nodiscard]] bool empty() const noexcept {
        return head_.load(std::memory_order_acquire) ==
               tail_.load(std::memory_order_acquire);
    }

private:
    [[nodiscard]] std::size_t Advance(std::size_t index) const noexcept {
        ++index;
        return index == slot_count_ ? 0U : index;
    }

    std::size_t slot_count_ = 0U;
    std::unique_ptr<T[]> slots_;
    alignas(64) std::atomic<std::size_t> head_{0U};
    alignas(64) std::atomic<std::size_t> tail_{0U};
};

struct DecoderCommandV1 final {
    realtime::OwnedIngressMessageHandleV1 message;
    market::DailyInstrumentIdentityViewV2 identity{};
};

struct SourceSignalV1 final {
    std::atomic<std::uint64_t> epoch{0U};

    void Notify() noexcept {
        epoch.fetch_add(1U, std::memory_order_release);
        epoch.notify_one();
    }
};

[[nodiscard]] bool ReadClock(
    clockid_t clock,
    std::uint64_t* output) noexcept {
    if (output == nullptr) {
        return false;
    }
    *output = 0U;
    struct timespec value {};
    if (::clock_gettime(clock, &value) != 0 || value.tv_sec < 0 ||
        value.tv_nsec < 0 || value.tv_nsec >= 1'000'000'000L) {
        return false;
    }
    constexpr std::uint64_t billion = 1'000'000'000U;
    const auto seconds = static_cast<std::uint64_t>(value.tv_sec);
    const auto nanoseconds = static_cast<std::uint64_t>(value.tv_nsec);
    if (seconds >
        (std::numeric_limits<std::uint64_t>::max() - nanoseconds) /
            billion) {
        return false;
    }
    *output = seconds * billion + nanoseconds;
    return true;
}

[[nodiscard]] bool FixedUtc8TradeDate(
    std::uint64_t realtime_ns,
    std::uint32_t* output) noexcept {
    if (output == nullptr ||
        realtime_ns > static_cast<std::uint64_t>(
                          std::numeric_limits<std::int64_t>::max())) {
        return false;
    }
    const auto point = std::chrono::sys_time<std::chrono::nanoseconds>(
        std::chrono::nanoseconds(
            static_cast<std::int64_t>(realtime_ns))) +
        std::chrono::hours(8);
    const std::chrono::year_month_day date(
        std::chrono::floor<std::chrono::days>(point));
    if (!date.ok()) {
        return false;
    }
    const int year = static_cast<int>(date.year());
    const unsigned int month =
        static_cast<unsigned int>(date.month());
    const unsigned int day = static_cast<unsigned int>(date.day());
    if (year < 0 || year > 9999 || month == 0U || day == 0U) {
        return false;
    }
    *output = static_cast<std::uint32_t>(year) * 10'000U +
              static_cast<std::uint32_t>(month) * 100U +
              static_cast<std::uint32_t>(day);
    return true;
}

[[nodiscard]] constexpr market::MainlandExchangeV1 ExchangeForMarket(
    market::MarketV1 value) noexcept {
    switch (value) {
        case market::MarketV1::kShanghai:
            return market::MainlandExchangeV1::kShanghai;
        case market::MarketV1::kShenzhen:
            return market::MainlandExchangeV1::kShenzhen;
        case market::MarketV1::kUnknown:
            break;
    }
    return market::MainlandExchangeV1::kUnknown;
}

[[nodiscard]] bool NoEmbeddedNull(
    std::string_view value,
    bool require_nonempty) noexcept {
    return (!require_nonempty || !value.empty()) &&
           value.find('\0') == std::string_view::npos;
}

[[nodiscard]] bool SameIdentity(
    const l2flow::common::Identity128& lhs,
    const l2flow::common::Identity128& rhs) noexcept {
    return lhs == rhs;
}

}  // namespace

std::string_view FastTickPipelineCreateErrorNameV1(
    FastTickPipelineCreateErrorV1 error) noexcept {
    switch (error) {
        case FastTickPipelineCreateErrorV1::kNone:
            return "none";
        case FastTickPipelineCreateErrorV1::kNullOutput:
            return "null_output";
        case FastTickPipelineCreateErrorV1::kInvalidConfiguration:
            return "invalid_configuration";
        case FastTickPipelineCreateErrorV1::kPlaneCreateFailed:
            return "plane_create_failed";
        case FastTickPipelineCreateErrorV1::kIngressPoolCreateFailed:
            return "ingress_pool_create_failed";
        case FastTickPipelineCreateErrorV1::kDecoderThreadStartFailed:
            return "decoder_thread_start_failed";
        case FastTickPipelineCreateErrorV1::kSdkLoadFailed:
            return "sdk_load_failed";
        case FastTickPipelineCreateErrorV1::kSdkManagerCreateFailed:
            return "sdk_manager_create_failed";
        case FastTickPipelineCreateErrorV1::kSdkSubscriberCreateFailed:
            return "sdk_subscriber_create_failed";
        case FastTickPipelineCreateErrorV1::kSdkConfigurationFailed:
            return "sdk_configuration_failed";
        case FastTickPipelineCreateErrorV1::kSdkConnectFailed:
            return "sdk_connect_failed";
        case FastTickPipelineCreateErrorV1::kSdkCallbackFailed:
            return "sdk_callback_failed";
        case FastTickPipelineCreateErrorV1::kResourceExhausted:
            return "resource_exhausted";
    }
    return "unknown";
}

std::string_view FastTickPipelineIngressErrorNameV1(
    FastTickPipelineIngressErrorV1 error) noexcept {
    switch (error) {
        case FastTickPipelineIngressErrorV1::kNone:
            return "none";
        case FastTickPipelineIngressErrorV1::kIgnoredUnsupported:
            return "ignored_unsupported";
        case FastTickPipelineIngressErrorV1::kFilteredNonAShare:
            return "filtered_non_a_share";
        case FastTickPipelineIngressErrorV1::kForbiddenCombinedTick:
            return "forbidden_combined_tick";
        case FastTickPipelineIngressErrorV1::kNullMessage:
            return "null_message";
        case FastTickPipelineIngressErrorV1::kClockFailure:
            return "clock_failure";
        case FastTickPipelineIngressErrorV1::kTradeDateBoundary:
            return "trade_date_boundary";
        case FastTickPipelineIngressErrorV1::kConcurrentCallback:
            return "concurrent_callback";
        case FastTickPipelineIngressErrorV1::kSequenceExhausted:
            return "sequence_exhausted";
        case FastTickPipelineIngressErrorV1::kOwnedMessageRejected:
            return "owned_message_rejected";
        case FastTickPipelineIngressErrorV1::kInstrumentKeyRejected:
            return "instrument_key_rejected";
        case FastTickPipelineIngressErrorV1::kCatalogMiss:
            return "catalog_miss";
        case FastTickPipelineIngressErrorV1::kDecoderQueueFull:
            return "decoder_queue_full";
        case FastTickPipelineIngressErrorV1::kStopped:
            return "stopped";
        case FastTickPipelineIngressErrorV1::kFatal:
            return "fatal";
    }
    return "unknown";
}

class FastTickPipelineV1::Impl final : public mdl::MessageHandlerBase {
public:
    class DecoderLane final {
    public:
        DecoderLane(
            std::size_t capacity,
            market::MarketDecoderConfigV1 decoder_config)
            : queue(capacity), decoder(decoder_config) {}

        SpscQueueV1<DecoderCommandV1> queue;
        market::MarketDecoderV1 decoder;
        SourceSignalV1 signal{};
        std::thread thread;
    };

    Impl(
        FastTickPipelineConfigV1 config,
        std::shared_ptr<sdk::SdkFactory> sdk_factory)
        : config_(std::move(config)),
          sdk_factory_(std::move(sdk_factory)) {}

    ~Impl() { StopAndDrain(); }

    [[nodiscard]] bool ValidConfiguration(
        bool has_test_factory) const noexcept {
        const std::size_t instruments =
            config_.daily_catalog == nullptr
                ? 0U
                : config_.daily_catalog->instrument_count();
        if (l2flow::common::IsZeroIdentity(config_.run_id) ||
            config_.daily_catalog == nullptr || instruments == 0U ||
            config_.daily_catalog->trade_date() != config_.trade_date ||
            !config_.daily_catalog->coverage_complete() ||
            config_.maximum_sdk_message_bytes == 0U ||
            config_.maximum_sdk_message_bytes >
                realtime::kOwnedIngressMaximumMessageBytesV1 ||
            config_.decoder_queue_capacity_per_source == 0U ||
            config_.decoder_queue_capacity_per_source ==
                std::numeric_limits<std::size_t>::max() ||
            config_.decoder_batch_budget == 0U ||
            config_.maximum_inflight_messages <
                realtime::kOwnedIngressSourceCountV1 ||
            config_.prewarm_message_count >
                config_.maximum_inflight_messages ||
            config_.prewarm_message_bytes >
                config_.maximum_sdk_message_bytes ||
            !SameIdentity(config_.run_id, config_.planes.fast.session_id) ||
            config_.trade_date != config_.planes.fast.trade_date ||
            config_.planes.fast.instrument_count != instruments ||
            config_.planes.event.instrument_count != instruments ||
            config_.planes.kline.instrument_count != instruments) {
            return false;
        }
        for (std::uint32_t source_stream_id :
             config_.source_stream_ids) {
            if (source_stream_id == 0U) {
                return false;
            }
        }
        if (!config_.sdk.enabled) {
            return !has_test_factory;
        }
        return config_.sdk.work_threads == 1 &&
               config_.sdk.io_threads == 1 &&
               config_.sdk.heartbeat_interval_seconds > 0U &&
               config_.sdk.heartbeat_timeout_seconds > 0U &&
               NoEmbeddedNull(config_.sdk.log_prefix, false) &&
               NoEmbeddedNull(config_.sdk.server_address, true) &&
               NoEmbeddedNull(config_.sdk.user_name, true) &&
               (has_test_factory || !config_.sdk.library_path.empty()) &&
               config_.enforce_receive_trade_date;
    }

    [[nodiscard]] FastTickPipelineCreateErrorV1 Initialize(
        bool has_test_factory,
        std::string* detail) noexcept {
        if (!ValidConfiguration(has_test_factory)) {
            if (detail != nullptr) {
                *detail = "invalid FAST Tick pipeline configuration";
            }
            return FastTickPipelineCreateErrorV1::kInvalidConfiguration;
        }
        try {
            RealtimePlanesCreateErrorV1 plane_error =
                RealtimePlanesV1::Create(
                    config_.planes, &planes_, detail);
            if (plane_error != RealtimePlanesCreateErrorV1::kNone) {
                if (detail != nullptr && detail->empty()) {
                    *detail = "independent plane creation failed: " +
                        std::string(
                            RealtimePlanesCreateErrorNameV1(plane_error));
                }
                return FastTickPipelineCreateErrorV1::kPlaneCreateFailed;
            }

            realtime::OwnedIngressMessagePoolConfigV1 pool_config{};
            pool_config.maximum_message_bytes =
                config_.maximum_sdk_message_bytes;
            pool_config.maximum_inflight_messages =
                config_.maximum_inflight_messages;
            pool_config.prewarm_message_bytes =
                config_.prewarm_message_bytes;
            pool_config.prewarm_message_count =
                config_.prewarm_message_count;
            pool_config.serialized_acquire = true;
            const auto pool_error =
                realtime::OwnedIngressMessagePoolV1::Create(
                    pool_config, &ingress_pool_);
            if (pool_error !=
                    realtime::OwnedIngressMessageErrorV1::kNone ||
                ingress_pool_ == nullptr) {
                if (detail != nullptr) {
                    *detail = "owned ingress pool creation failed: " +
                        std::string(
                            realtime::OwnedIngressMessageErrorNameV1(
                                pool_error));
                }
                return FastTickPipelineCreateErrorV1::
                    kIngressPoolCreateFailed;
            }

            for (std::size_t source = 0U;
                 source < realtime::kOwnedIngressSourceCountV1;
                 ++source) {
                market::MarketDecoderConfigV1 decoder_config{};
                decoder_config.trade_date = config_.trade_date;
                decoder_config.source_stream_id =
                    config_.source_stream_ids[source];
                decoder_config.limits = config_.decoder_limits;
                lanes_[source] = std::make_unique<DecoderLane>(
                    config_.decoder_queue_capacity_per_source,
                    decoder_config);
                if (!lanes_[source]->decoder.configuration_valid()) {
                    if (detail != nullptr) {
                        *detail = "invalid source decoder configuration";
                    }
                    return FastTickPipelineCreateErrorV1::
                        kInvalidConfiguration;
                }
            }
            for (std::size_t source = 0U;
                 source < realtime::kOwnedIngressSourceCountV1;
                 ++source) {
                lanes_[source]->thread = std::thread(
                    [this, source] { DecoderLoop(source); });
            }
        } catch (const std::system_error& error) {
            if (detail != nullptr) {
                try {
                    *detail = error.what();
                } catch (...) {
                    detail->clear();
                }
            }
            return FastTickPipelineCreateErrorV1::
                kDecoderThreadStartFailed;
        } catch (...) {
            return FastTickPipelineCreateErrorV1::kResourceExhausted;
        }
        return StartSdk(has_test_factory, detail);
    }

    void OnMessage(
        mdl::Subscriber*,
        const mdl::MDLMessage* message) override {
        active_callbacks_.fetch_add(1U, std::memory_order_acq_rel);
        if (!callback_gate_closed_.load(std::memory_order_acquire)) {
            std::uint64_t realtime_ns = 0U;
            std::uint64_t monotonic_ns = 0U;
            if (!ReadClock(CLOCK_REALTIME, &realtime_ns) ||
                !ReadClock(CLOCK_MONOTONIC, &monotonic_ns)) {
                TripFatal(0U);
            } else {
                static_cast<void>(Ingest(
                    message, realtime_ns, monotonic_ns));
            }
        }
        if (active_callbacks_.fetch_sub(
                1U, std::memory_order_acq_rel) == 1U) {
            active_callbacks_.notify_all();
        }
    }

    [[nodiscard]] FastTickPipelineIngressResultV1 Ingest(
        const mdl::MDLMessage* message,
        std::uint64_t recv_realtime_ns,
        std::uint64_t recv_monotonic_ns) noexcept {
        FastTickPipelineIngressResultV1 result{};
        if (callback_guard_.test_and_set(std::memory_order_acquire)) {
            result.error =
                FastTickPipelineIngressErrorV1::kConcurrentCallback;
            rejected_messages_.fetch_add(1U, std::memory_order_relaxed);
            TripFatal(0U);
            return result;
        }
        struct Guard final {
            std::atomic_flag* flag = nullptr;
            ~Guard() { flag->clear(std::memory_order_release); }
        } guard{&callback_guard_};

        if (fatal_.load(std::memory_order_acquire)) {
            result.error = FastTickPipelineIngressErrorV1::kFatal;
            return result;
        }
        if (!accepting_.load(std::memory_order_acquire)) {
            result.error = FastTickPipelineIngressErrorV1::kStopped;
            return result;
        }
        if (message == nullptr) {
            result.error = FastTickPipelineIngressErrorV1::kNullMessage;
            result.owned_error =
                realtime::OwnedIngressMessageErrorV1::kNullMessage;
            rejected_messages_.fetch_add(1U, std::memory_order_relaxed);
            TripFatal(0U);
            return result;
        }
        if (recv_realtime_ns == 0U || recv_monotonic_ns == 0U ||
            recv_realtime_ns > static_cast<std::uint64_t>(
                                   std::numeric_limits<std::int64_t>::max()) ||
            recv_monotonic_ns > static_cast<std::uint64_t>(
                                    std::numeric_limits<std::int64_t>::max())) {
            result.error = FastTickPipelineIngressErrorV1::kClockFailure;
            rejected_messages_.fetch_add(1U, std::memory_order_relaxed);
            TripFatal(0U);
            return result;
        }
        if (config_.enforce_receive_trade_date) {
            std::uint32_t receive_date = 0U;
            if (!FixedUtc8TradeDate(recv_realtime_ns, &receive_date)) {
                result.error =
                    FastTickPipelineIngressErrorV1::kClockFailure;
                rejected_messages_.fetch_add(
                    1U, std::memory_order_relaxed);
                TripFatal(0U);
                return result;
            }
            if (receive_date != config_.trade_date) {
                accepting_.store(false, std::memory_order_release);
                result.error =
                    FastTickPipelineIngressErrorV1::kTradeDateBoundary;
                return result;
            }
        }

        realtime::OwnedIngressMessageInspectionV1 inspection{};
        result.owned_error = realtime::InspectOwnedIngressMessageV1(
            message, config_.maximum_sdk_message_bytes, &inspection);
        if (result.owned_error ==
            realtime::OwnedIngressMessageErrorV1::kUnsupportedMessage) {
            result.error =
                FastTickPipelineIngressErrorV1::kIgnoredUnsupported;
            ignored_messages_.fetch_add(1U, std::memory_order_relaxed);
            return result;
        }
        if (result.owned_error ==
            realtime::OwnedIngressMessageErrorV1::
                kForbiddenCombinedTick) {
            result.error = FastTickPipelineIngressErrorV1::
                kForbiddenCombinedTick;
            rejected_messages_.fetch_add(1U, std::memory_order_relaxed);
            TripFatal(0U);
            return result;
        }
        if (result.owned_error !=
                realtime::OwnedIngressMessageErrorV1::kNone ||
            !inspection) {
            result.error =
                FastTickPipelineIngressErrorV1::kOwnedMessageRejected;
            rejected_messages_.fetch_add(1U, std::memory_order_relaxed);
            TripFatal(0U);
            return result;
        }
        const std::size_t source = inspection.source_slot();
        result.source_slot = static_cast<std::uint8_t>(source);

        market::MarketMessageViewV1 admission{};
        admission.service_id = inspection.key().service_id;
        admission.service_version = inspection.key().service_version;
        admission.message_id = inspection.key().message_id;
        admission.body = inspection.body();
        market::ExactInstrumentKeyViewV2 exact{};
        const market::MarketDecodeErrorV1 extraction =
            market::ExtractExactInstrumentKeyV2(
                admission,
                config_.decoder_limits.maximum_text_bytes,
                &exact);
        if (extraction != market::MarketDecodeErrorV1::kNone) {
            result.error = FastTickPipelineIngressErrorV1::
                kInstrumentKeyRejected;
            decoder_failures_.fetch_add(1U, std::memory_order_relaxed);
            rejected_messages_.fetch_add(1U, std::memory_order_relaxed);
            TripFatal(0U);
            return result;
        }
        if (!market::IsMainlandAShareSecurityIdV1(
                ExchangeForMarket(exact.market), exact.security_id)) {
            result.error =
                FastTickPipelineIngressErrorV1::kFilteredNonAShare;
            filtered_messages_.fetch_add(1U, std::memory_order_relaxed);
            return result;
        }
        const market::DailyInstrumentCatalogLookupResultV2 lookup =
            config_.daily_catalog->Lookup(market::InstrumentKeyViewV1{
                exact.market,
                exact.security_id_source,
                exact.security_id});
        if (!lookup.known()) {
            result.error = FastTickPipelineIngressErrorV1::kCatalogMiss;
            rejected_messages_.fetch_add(1U, std::memory_order_relaxed);
            TripFatal(0U);
            return result;
        }
        const market::DailyInstrumentCatalogEntryV2& entry =
            *lookup.entry;
        market::DailyInstrumentIdentityViewV2 identity{};
        identity.key.market = entry.key.market;
        identity.key.security_id_source =
            entry.key.security_id_source;
        identity.key.security_id = entry.key.security_id;
        identity.instrument_id = entry.instrument_id;
        identity.ordinal = entry.ordinal;
        identity.quantity_unit = entry.metadata.quantity_unit;
        identity.security_type = entry.metadata.security_type;
        identity.asset_scope = entry.metadata.asset_scope;

        constexpr std::uint64_t sentinel =
            std::numeric_limits<std::uint64_t>::max();
        if (arrival_id_ >= sentinel - 1U ||
            source_sequences_[source] >= sentinel - 1U) {
            result.error =
                FastTickPipelineIngressErrorV1::kSequenceExhausted;
            rejected_messages_.fetch_add(1U, std::memory_order_relaxed);
            TripFatal(entry.instrument_id);
            return result;
        }
        realtime::OwnedIngressMetadataV1 metadata{};
        metadata.run_id = config_.run_id;
        metadata.global_ingress_sequence = arrival_id_ + 1U;
        metadata.source_sequence = source_sequences_[source] + 1U;
        metadata.recv_realtime_ns = recv_realtime_ns;
        metadata.recv_monotonic_ns = recv_monotonic_ns;
        DecoderCommandV1 command{};
        command.identity = identity;
        result.owned_error = ingress_pool_->Acquire(
            inspection, metadata, &command.message);
        if (result.owned_error !=
                realtime::OwnedIngressMessageErrorV1::kNone ||
            !command.message) {
            result.error =
                FastTickPipelineIngressErrorV1::kOwnedMessageRejected;
            rejected_messages_.fetch_add(1U, std::memory_order_relaxed);
            MarkInstrumentCoverageLost(entry.instrument_id);
            return result;
        }
        if (!lanes_[source]->queue.TryPush(std::move(command))) {
            result.error = FastTickPipelineIngressErrorV1::kDecoderQueueFull;
            rejected_messages_.fetch_add(1U, std::memory_order_relaxed);
            MarkInstrumentCoverageLost(entry.instrument_id);
            return result;
        }

        arrival_id_ = metadata.global_ingress_sequence;
        source_sequences_[source] = metadata.source_sequence;
        result.arrival_id = metadata.global_ingress_sequence;
        result.source_sequence = metadata.source_sequence;
        accepted_messages_.fetch_add(1U, std::memory_order_relaxed);
        accepted_by_source_[source].fetch_add(
            1U, std::memory_order_relaxed);
        lanes_[source]->signal.Notify();
        return result;
    }

    void DecoderLoop(std::size_t source) noexcept {
        DecoderCommandV1 command{};
        for (;;) {
            const std::uint64_t observed_epoch =
                lanes_[source]->signal.epoch.load(
                    std::memory_order_acquire);
            bool worked = false;
            for (std::size_t count = 0U;
                 count < config_.decoder_batch_budget; ++count) {
                if (!lanes_[source]->queue.TryPop(&command)) {
                    break;
                }
                worked = true;
                DecodeOne(source, &command);
                command = DecoderCommandV1{};
            }
            if (decoder_stop_requested_.load(std::memory_order_acquire) &&
                lanes_[source]->queue.empty()) {
                break;
            }
            if (!worked) {
                lanes_[source]->signal.epoch.wait(
                    observed_epoch, std::memory_order_acquire);
            }
        }
    }

    void DecodeOne(
        std::size_t source,
        DecoderCommandV1* command) noexcept {
        if (command == nullptr || !command->message ||
            command->message->source_slot() != source ||
            command->message->recv_realtime_ns() >
                static_cast<std::uint64_t>(
                    std::numeric_limits<std::int64_t>::max()) ||
            command->message->recv_monotonic_ns() >
                static_cast<std::uint64_t>(
                    std::numeric_limits<std::int64_t>::max())) {
            decoder_failures_.fetch_add(1U, std::memory_order_relaxed);
            TripFatal(command == nullptr
                          ? 0U
                          : command->identity.instrument_id);
            return;
        }
        const sdk::VendorHeadView head = command->message->vendor_head();
        market::MarketMessageViewV1 view{};
        view.source_stream_id = config_.source_stream_ids[source];
        view.trade_date = config_.trade_date;
        view.source_sequence = command->message->source_sequence();
        view.service_id = command->message->key().service_id;
        view.service_version = command->message->key().service_version;
        view.message_id = command->message->key().message_id;
        view.message_encoding = head.message_encoding();
        view.vendor_local_time_raw = head.local_time_raw();
        view.vendor_sequence_id = head.sequence_id();
        view.recv_realtime_ns = static_cast<std::int64_t>(
            command->message->recv_realtime_ns());
        view.recv_monotonic_ns = static_cast<std::int64_t>(
            command->message->recv_monotonic_ns());
        view.body = command->message->body();

        market::DecodedMarketEventV1 decoded;
        if (lanes_[source]->decoder.Decode(view, &decoded) !=
                market::MarketDecodeErrorV1::kNone ||
            !market::ApplyDailyInstrumentIdentityV2(
                command->identity, &decoded)) {
            decoder_failures_.fetch_add(1U, std::memory_order_relaxed);
            MarkInstrumentCoverageLost(command->identity.instrument_id);
            return;
        }
        market::DecodedFastTickV1 owned;
        market::CompactFastTickV1 compact{};
        const auto projection = market::ProjectFastTickV1(
            std::move(decoded),
            source == static_cast<std::size_t>(
                          realtime::OwnedIngressSourceV1::kShanghaiTick)
                ? market::FastTickSourceV1::kShanghaiTick
                : market::FastTickSourceV1::kShenzhenTick,
            command->message->global_ingress_sequence(),
            &owned,
            &compact);
        if (projection != market::FastTickProjectionErrorV1::kNone) {
            decoder_failures_.fetch_add(1U, std::memory_order_relaxed);
            MarkInstrumentCoverageLost(command->identity.instrument_id);
            return;
        }
        const RealtimeRouteResultV1 routed = planes_->RouteDecoded(
            compact, std::move(owned));
        if (routed.error != RealtimeRouteErrorV1::kNone) {
            decoder_failures_.fetch_add(1U, std::memory_order_relaxed);
            if (routed.error == RealtimeRouteErrorV1::kFastQueueFull ||
                routed.error ==
                    RealtimeRouteErrorV1::kFastCoverageLost) {
                MarkInstrumentCoverageLost(
                    command->identity.instrument_id);
            } else {
                TripFatal(command->identity.instrument_id);
            }
            return;
        }
        decoded_messages_.fetch_add(1U, std::memory_order_relaxed);
    }

    void TripFatal(std::uint32_t instrument_id) noexcept {
        if (planes_ != nullptr) {
            if (instrument_id != 0U) {
                planes_->MarkFastCoverageLost(instrument_id);
            } else {
                // The rejected callback could not be attributed safely. Once
                // admission stops, no catalog instrument can still claim a
                // complete remainder-of-session raw prefix.
                for (std::size_t ordinal = 0U;
                     ordinal < config_.planes.fast.instrument_count;
                     ++ordinal) {
                    planes_->MarkFastCoverageLost(
                        static_cast<std::uint32_t>(ordinal + 1U));
                }
            }
        }
        fatal_.store(true, std::memory_order_release);
        accepting_.store(false, std::memory_order_release);
    }

    void MarkInstrumentCoverageLost(
        std::uint32_t instrument_id) noexcept {
        if (instrument_id != 0U && planes_ != nullptr) {
            planes_->MarkFastCoverageLost(instrument_id);
        }
    }

    [[nodiscard]] FastTickPipelineCreateErrorV1 StartSdk(
        bool has_test_factory,
        std::string* detail) noexcept {
        if (!config_.sdk.enabled) {
            accepting_.store(true, std::memory_order_release);
            return FastTickPipelineCreateErrorV1::kNone;
        }
        try {
            if (!has_test_factory) {
                std::string load_error;
                sdk_factory_ = sdk::LoadSdkFactoryFromPath(
                    config_.sdk.library_path, &load_error);
                if (sdk_factory_ == nullptr) {
                    if (detail != nullptr) {
                        *detail = std::move(load_error);
                    }
                    return FastTickPipelineCreateErrorV1::kSdkLoadFailed;
                }
            }
            sdk_manager_ = sdk_factory_->Create(
                config_.sdk.work_threads, config_.sdk.io_threads);
            if (sdk_manager_ == nullptr) {
                return FastTickPipelineCreateErrorV1::
                    kSdkManagerCreateFailed;
            }
            sdk_manager_->EnableLog(
                config_.sdk.log_prefix, config_.sdk.log_to_console);
            sdk_subscriber_ = sdk_manager_->CreateSubscriber(this, false);
            if (sdk_subscriber_ == nullptr) {
                return FastTickPipelineCreateErrorV1::
                    kSdkSubscriberCreateFailed;
            }
            sdk_subscriber_->SetServerAddress(config_.sdk.server_address);
            sdk_subscriber_->SetUserName(config_.sdk.user_name);
            sdk_subscriber_->SetHeartbeatInterval(
                config_.sdk.heartbeat_interval_seconds);
            sdk_subscriber_->SetHeartbeatTimeout(
                config_.sdk.heartbeat_timeout_seconds);
            sdk_subscriber_->SetMessageEncoding(mdl::MDLEID_BINARY);
            sdk_subscriber_->EnableMergeMessage(false);
            sdk_subscriber_->SetSendMacAuth(false);
            sdk_subscriber_->EnableServerSelect(false);
            sdk::AddProductionSubscriptionsV1(*sdk_subscriber_);
            accepting_.store(true, std::memory_order_release);
            const std::string connect_error = sdk_subscriber_->Connect();
            if (!connect_error.empty()) {
                accepting_.store(false, std::memory_order_release);
                if (detail != nullptr) {
                    *detail = connect_error;
                }
                return FastTickPipelineCreateErrorV1::kSdkConnectFailed;
            }
            if (fatal_.load(std::memory_order_acquire)) {
                return FastTickPipelineCreateErrorV1::kSdkCallbackFailed;
            }
            return FastTickPipelineCreateErrorV1::kNone;
        } catch (...) {
            accepting_.store(false, std::memory_order_release);
            return FastTickPipelineCreateErrorV1::
                kSdkConfigurationFailed;
        }
    }

    void StopAndDrain() noexcept {
        bool expected = false;
        if (!stopping_.compare_exchange_strong(
                expected, true, std::memory_order_acq_rel)) {
            while (!stopped_.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            return;
        }
        accepting_.store(false, std::memory_order_release);
        callback_gate_closed_.store(true, std::memory_order_release);
        if (sdk_manager_ != nullptr) {
            try {
                sdk_manager_->Shutdown();
            } catch (...) {
                fatal_.store(true, std::memory_order_release);
            }
        }
        std::uint64_t callbacks = active_callbacks_.load(
            std::memory_order_acquire);
        while (callbacks != 0U) {
            active_callbacks_.wait(callbacks, std::memory_order_acquire);
            callbacks = active_callbacks_.load(
                std::memory_order_acquire);
        }
        if (sdk_subscriber_ != nullptr) {
            std::string ignored;
            if (!sdk_subscriber_->Release(&ignored)) {
                fatal_.store(true, std::memory_order_release);
            }
            sdk_subscriber_.reset();
        }
        if (sdk_manager_ != nullptr) {
            std::string ignored;
            if (!sdk_manager_->Release(&ignored)) {
                fatal_.store(true, std::memory_order_release);
            }
            sdk_manager_.reset();
        }

        decoder_stop_requested_.store(true, std::memory_order_release);
        for (auto& lane : lanes_) {
            if (lane != nullptr) {
                lane->signal.Notify();
            }
        }
        for (auto& lane : lanes_) {
            if (lane != nullptr && lane->thread.joinable()) {
                lane->thread.join();
            }
        }
        if (planes_ != nullptr) {
            planes_->StopAndDrain();
        }
        if (ingress_pool_ != nullptr) {
            ingress_pool_->Retire();
        }
        stopped_.store(true, std::memory_order_release);
    }

    [[nodiscard]] FastTickPipelineSnapshotV1 Snapshot() const noexcept {
        FastTickPipelineSnapshotV1 result{};
        result.accepted_messages = accepted_messages_.load(
            std::memory_order_acquire);
        result.ignored_messages = ignored_messages_.load(
            std::memory_order_acquire);
        result.filtered_messages = filtered_messages_.load(
            std::memory_order_acquire);
        result.rejected_messages = rejected_messages_.load(
            std::memory_order_acquire);
        result.decoded_messages = decoded_messages_.load(
            std::memory_order_acquire);
        result.decoder_failures = decoder_failures_.load(
            std::memory_order_acquire);
        for (std::size_t source = 0U;
             source < realtime::kOwnedIngressSourceCountV1;
             ++source) {
            result.accepted_by_source[source] =
                accepted_by_source_[source].load(
                    std::memory_order_acquire);
        }
        result.accepting = accepting_.load(std::memory_order_acquire);
        result.fatal = fatal_.load(std::memory_order_acquire);
        result.stopped = stopped_.load(std::memory_order_acquire);
        if (planes_ != nullptr) {
            result.planes = planes_->Snapshot();
        }
        return result;
    }

    FastTickPipelineConfigV1 config_{};
    std::shared_ptr<sdk::SdkFactory> sdk_factory_;
    std::unique_ptr<sdk::SdkManager> sdk_manager_;
    std::unique_ptr<sdk::SdkSubscriber> sdk_subscriber_;
    std::unique_ptr<RealtimePlanesV1> planes_;
    std::unique_ptr<realtime::OwnedIngressMessagePoolV1> ingress_pool_;
    std::array<std::unique_ptr<DecoderLane>,
               realtime::kOwnedIngressSourceCountV1>
        lanes_{};
    std::array<std::uint64_t,
               realtime::kOwnedIngressSourceCountV1>
        source_sequences_{};
    std::uint64_t arrival_id_ = 0U;
    std::atomic_flag callback_guard_ = ATOMIC_FLAG_INIT;
    std::atomic<bool> accepting_{false};
    std::atomic<bool> fatal_{false};
    std::atomic<bool> callback_gate_closed_{false};
    std::atomic<bool> decoder_stop_requested_{false};
    std::atomic<bool> stopping_{false};
    std::atomic<bool> stopped_{false};
    std::atomic<std::uint64_t> active_callbacks_{0U};
    std::atomic<std::uint64_t> accepted_messages_{0U};
    std::atomic<std::uint64_t> ignored_messages_{0U};
    std::atomic<std::uint64_t> filtered_messages_{0U};
    std::atomic<std::uint64_t> rejected_messages_{0U};
    std::atomic<std::uint64_t> decoded_messages_{0U};
    std::atomic<std::uint64_t> decoder_failures_{0U};
    std::array<std::atomic<std::uint64_t>,
               realtime::kOwnedIngressSourceCountV1>
        accepted_by_source_{};
};

FastTickPipelineV1::FastTickPipelineV1(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

FastTickPipelineV1::~FastTickPipelineV1() {
    if (impl_ != nullptr) {
        impl_->StopAndDrain();
    }
}

FastTickPipelineCreateErrorV1 FastTickPipelineV1::Create(
    FastTickPipelineConfigV1 config,
    std::unique_ptr<FastTickPipelineV1>* output,
    std::string* detail,
    std::shared_ptr<sdk::SdkFactory> sdk_factory_for_test) noexcept {
    if (output == nullptr) {
        return FastTickPipelineCreateErrorV1::kNullOutput;
    }
    output->reset();
    if (detail != nullptr) {
        detail->clear();
    }
    try {
        auto impl = std::make_unique<Impl>(
            std::move(config), std::move(sdk_factory_for_test));
        const bool has_test_factory = impl->sdk_factory_ != nullptr;
        const FastTickPipelineCreateErrorV1 error =
            impl->Initialize(has_test_factory, detail);
        if (error != FastTickPipelineCreateErrorV1::kNone) {
            return error;
        }
        output->reset(new FastTickPipelineV1(std::move(impl)));
        return FastTickPipelineCreateErrorV1::kNone;
    } catch (...) {
        return FastTickPipelineCreateErrorV1::kResourceExhausted;
    }
}

FastTickPipelineIngressResultV1 FastTickPipelineV1::IngestForTest(
    const mdl::MDLMessage* message,
    std::uint64_t recv_realtime_ns,
    std::uint64_t recv_monotonic_ns) noexcept {
    if (impl_ == nullptr) {
        FastTickPipelineIngressResultV1 result{};
        result.error = FastTickPipelineIngressErrorV1::kStopped;
        return result;
    }
    return impl_->Ingest(message, recv_realtime_ns, recv_monotonic_ns);
}

const RealtimePlanesV1& FastTickPipelineV1::planes() const noexcept {
    return *impl_->planes_;
}

RealtimePlanesV1& FastTickPipelineV1::planes() noexcept {
    return *impl_->planes_;
}

FastTickPipelineSnapshotV1 FastTickPipelineV1::Snapshot() const noexcept {
    return impl_ == nullptr ? FastTickPipelineSnapshotV1{}
                            : impl_->Snapshot();
}

const FastTickPipelineConfigV1& FastTickPipelineV1::config()
    const noexcept {
    return impl_->config_;
}

void FastTickPipelineV1::StopAndDrain() noexcept {
    if (impl_ != nullptr) {
        impl_->StopAndDrain();
    }
}

}  // namespace l2flow::runtime
