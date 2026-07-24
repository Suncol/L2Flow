#include "l2flow/runtime/realtime_pipeline_v1.h"

#include "l2flow/sdk/direct_sdk_runtime_v1.h"
#include "l2flow/sdk/production_subscription_v1.h"

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <ctime>
#include <deque>
#include <limits>
#include <mutex>
#include <new>
#include <optional>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

namespace l2flow::runtime {
namespace {

namespace factor = l2flow::factor;
namespace market = l2flow::market;
namespace realtime = l2flow::realtime;
namespace sdk = l2flow::sdk;
namespace mdl = datayes::mdl;

constexpr auto kMaximumCutTimeout = std::chrono::hours(24);

void ClearDetail(std::string* detail) noexcept {
    if (detail == nullptr) {
        return;
    }
    try {
        detail->clear();
    } catch (...) {
    }
}

void SetDetail(std::string* detail, std::string message) noexcept {
    if (detail == nullptr) {
        return;
    }
    try {
        *detail = std::move(message);
    } catch (...) {
    }
}

void SetDetailLiteral(std::string* detail, const char* message) noexcept {
    if (detail == nullptr) {
        return;
    }
    try {
        *detail = message;
    } catch (...) {
    }
}

[[nodiscard]] bool TextValid(std::string_view value,
                             bool require_nonempty) noexcept {
    return (!require_nonempty || !value.empty()) &&
           value.find('\0') == std::string_view::npos;
}

[[nodiscard]] bool MessageEncodingValid(
    mdl::MDLMessageEncoding encoding) noexcept {
    // MarketDecoderV1 consumes the vendor's unpacked binary body layout. It
    // does not contain FAST/JSON/Protobuf decompression or translation.
    return encoding == mdl::MDLEID_BINARY;
}

[[nodiscard]] bool ProductionRegistryValid(
    const market::InstrumentRegistryV1& registry) noexcept {
    constexpr std::array<std::byte, 4U> shenzhen_source{
        std::byte{0x31U},
        std::byte{0x30U},
        std::byte{0x32U},
        std::byte{0x20U}};
    for (const market::InstrumentRegistryEntryV1& entry :
         registry.entries()) {
        const bool reachable_security_id = std::all_of(
            entry.key.security_id.begin(),
            entry.key.security_id.end(),
            [](std::byte value) noexcept {
                const std::uint8_t character =
                    std::to_integer<std::uint8_t>(value);
                return character >= 0x20U && character <= 0x7eU;
            });
        if (entry.key.security_id.empty() || !reachable_security_id) {
            return false;
        }
        if (entry.key.market == market::MarketV1::kShanghai) {
            if (!entry.key.security_id_source.empty()) {
                return false;
            }
        } else if (entry.key.market == market::MarketV1::kShenzhen) {
            if (!std::equal(
                    entry.key.security_id_source.begin(),
                    entry.key.security_id_source.end(),
                    shenzhen_source.begin(),
                    shenzhen_source.end())) {
                return false;
            }
        } else {
            return false;
        }
    }
    return true;
}

[[nodiscard]] bool ReadClockNs(clockid_t clock,
                               std::uint64_t* output) noexcept {
    if (output == nullptr) {
        return false;
    }
    struct timespec value {};
    if (::clock_gettime(clock, &value) != 0 || value.tv_sec < 0 ||
        value.tv_nsec < 0 || value.tv_nsec >= 1'000'000'000L) {
        return false;
    }
    const std::uint64_t seconds =
        static_cast<std::uint64_t>(value.tv_sec);
    constexpr std::uint64_t kNanosPerSecond = 1'000'000'000ULL;
    if (seconds >
        (std::numeric_limits<std::uint64_t>::max() -
         static_cast<std::uint64_t>(value.tv_nsec)) /
            kNanosPerSecond) {
        return false;
    }
    *output = seconds * kNanosPerSecond +
              static_cast<std::uint64_t>(value.tv_nsec);
    return true;
}

[[nodiscard]] bool FixedUtc8TradeDateFromRealtimeNs(
    std::uint64_t realtime_ns,
    std::uint32_t* output) noexcept {
    if (output == nullptr) {
        return false;
    }
    constexpr std::int64_t utc8_offset_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::hours(8))
            .count();
    if (realtime_ns > static_cast<std::uint64_t>(
                          std::numeric_limits<std::int64_t>::max() -
                          utc8_offset_ns)) {
        return false;
    }
    const auto shifted = std::chrono::sys_time<std::chrono::nanoseconds>(
        std::chrono::nanoseconds(
            static_cast<std::int64_t>(realtime_ns) + utc8_offset_ns));
    const std::chrono::year_month_day calendar(
        std::chrono::floor<std::chrono::days>(shifted));
    if (!calendar.ok()) {
        return false;
    }
    const int year = static_cast<int>(calendar.year());
    const unsigned int month =
        static_cast<unsigned int>(calendar.month());
    const unsigned int day = static_cast<unsigned int>(calendar.day());
    if (year <= 0 || year > 9999 || month == 0U || day == 0U) {
        return false;
    }
    *output = static_cast<std::uint32_t>(year) * 10'000U +
              static_cast<std::uint32_t>(month) * 100U +
              static_cast<std::uint32_t>(day);
    return true;
}

[[nodiscard]] std::chrono::nanoseconds Remaining(
    std::chrono::steady_clock::time_point deadline) noexcept {
    const auto now = std::chrono::steady_clock::now();
    if (now >= deadline) {
        return std::chrono::nanoseconds::zero();
    }
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        deadline - now);
}

}  // namespace

std::string_view RealtimePipelineCreateErrorNameV1(
    RealtimePipelineCreateErrorV1 error) noexcept {
    switch (error) {
        case RealtimePipelineCreateErrorV1::kNone:
            return "none";
        case RealtimePipelineCreateErrorV1::kNullOutput:
            return "null_output";
        case RealtimePipelineCreateErrorV1::kInvalidConfiguration:
            return "invalid_configuration";
        case RealtimePipelineCreateErrorV1::kStoreRuntimeCreateFailed:
            return "store_runtime_create_failed";
        case RealtimePipelineCreateErrorV1::kWalCreateFailed:
            return "wal_create_failed";
        case RealtimePipelineCreateErrorV1::kFactorCreateFailed:
            return "factor_create_failed";
        case RealtimePipelineCreateErrorV1::kDecoderThreadStartFailed:
            return "decoder_thread_start_failed";
        case RealtimePipelineCreateErrorV1::kSdkLoadFailed:
            return "sdk_load_failed";
        case RealtimePipelineCreateErrorV1::kSdkManagerCreateFailed:
            return "sdk_manager_create_failed";
        case RealtimePipelineCreateErrorV1::kSdkSubscriberCreateFailed:
            return "sdk_subscriber_create_failed";
        case RealtimePipelineCreateErrorV1::kSdkConfigurationFailed:
            return "sdk_configuration_failed";
        case RealtimePipelineCreateErrorV1::kSdkConnectFailed:
            return "sdk_connect_failed";
        case RealtimePipelineCreateErrorV1::kSdkCallbackFailed:
            return "sdk_callback_failed";
        case RealtimePipelineCreateErrorV1::kResourceExhausted:
            return "resource_exhausted";
        case RealtimePipelineCreateErrorV1::kUnexpectedFailure:
            return "unexpected_failure";
    }
    return "unknown";
}

std::string_view RealtimePipelineIngressErrorNameV1(
    RealtimePipelineIngressErrorV1 error) noexcept {
    switch (error) {
        case RealtimePipelineIngressErrorV1::kNone:
            return "none";
        case RealtimePipelineIngressErrorV1::kIgnoredUnsupported:
            return "ignored_unsupported";
        case RealtimePipelineIngressErrorV1::kNullMessage:
            return "null_message";
        case RealtimePipelineIngressErrorV1::kClockFailure:
            return "clock_failure";
        case RealtimePipelineIngressErrorV1::kTradeDateBoundary:
            return "trade_date_boundary";
        case RealtimePipelineIngressErrorV1::kSequenceExhausted:
            return "sequence_exhausted";
        case RealtimePipelineIngressErrorV1::kOwnedMessageRejected:
            return "owned_message_rejected";
        case RealtimePipelineIngressErrorV1::kForbiddenCombinedTick:
            return "forbidden_combined_tick";
        case RealtimePipelineIngressErrorV1::kDecoderQueueFull:
            return "decoder_queue_full";
        case RealtimePipelineIngressErrorV1::kStopped:
            return "stopped";
        case RealtimePipelineIngressErrorV1::kFatal:
            return "fatal";
    }
    return "unknown";
}

std::string_view RealtimePipelineCutErrorNameV1(
    RealtimePipelineCutErrorV1 error) noexcept {
    switch (error) {
        case RealtimePipelineCutErrorV1::kNone:
            return "none";
        case RealtimePipelineCutErrorV1::kInvalidTimeout:
            return "invalid_timeout";
        case RealtimePipelineCutErrorV1::kStopped:
            return "stopped";
        case RealtimePipelineCutErrorV1::kFatal:
            return "fatal";
        case RealtimePipelineCutErrorV1::kSequenceExhausted:
            return "sequence_exhausted";
        case RealtimePipelineCutErrorV1::kClockFailure:
            return "clock_failure";
        case RealtimePipelineCutErrorV1::kWatermarkFailed:
            return "watermark_failed";
        case RealtimePipelineCutErrorV1::kGenerationBeginFailed:
            return "generation_begin_failed";
        case RealtimePipelineCutErrorV1::kMarkerAdmissionFailed:
            return "marker_admission_failed";
        case RealtimePipelineCutErrorV1::kGenerationWaitFailed:
            return "generation_wait_failed";
        case RealtimePipelineCutErrorV1::kFactorPublishFailed:
            return "factor_publish_failed";
        case RealtimePipelineCutErrorV1::kUnexpectedFailure:
            return "unexpected_failure";
    }
    return "unknown";
}

class RealtimePipelineV1::Impl final : public mdl::MessageHandlerBase {
public:
    enum class CommandKind : std::uint8_t {
        kMessage = 0U,
        kGenerationMarker,
    };

    struct DecoderCommand final {
        CommandKind kind = CommandKind::kMessage;
        realtime::OwnedIngressMessageHandleV1 message;
        std::uint64_t generation = 0U;
    };

    class DecoderQueue final {
    public:
        explicit DecoderQueue(std::size_t capacity)
            : slots_(capacity) {}

        DecoderQueue(const DecoderQueue&) = delete;
        DecoderQueue& operator=(const DecoderQueue&) = delete;

        [[nodiscard]] bool TryPush(DecoderCommand command) noexcept {
            try {
                std::lock_guard<std::mutex> lock(mutex_);
                if (stop_requested_ || size_ == slots_.size()) {
                    return false;
                }
                slots_[tail_].emplace(std::move(command));
                tail_ = Increment(tail_);
                ++size_;
                not_empty_.notify_one();
                return true;
            } catch (...) {
                return false;
            }
        }

        [[nodiscard]] bool PushUntil(
            DecoderCommand command,
            std::chrono::steady_clock::time_point deadline) noexcept {
            try {
                std::unique_lock<std::mutex> lock(mutex_);
                const auto ready = [this] {
                    return stop_requested_ || size_ < slots_.size();
                };
                if (!ready() && !not_full_.wait_until(lock, deadline, ready)) {
                    return false;
                }
                if (stop_requested_ || size_ == slots_.size()) {
                    return false;
                }
                slots_[tail_].emplace(std::move(command));
                tail_ = Increment(tail_);
                ++size_;
                not_empty_.notify_one();
                return true;
            } catch (...) {
                return false;
            }
        }

        [[nodiscard]] bool Pop(DecoderCommand* output) noexcept {
            if (output == nullptr) {
                return false;
            }
            try {
                std::unique_lock<std::mutex> lock(mutex_);
                not_empty_.wait(lock, [this] {
                    return stop_requested_ || size_ != 0U;
                });
                if (size_ == 0U) {
                    return false;
                }
                *output = std::move(*slots_[head_]);
                slots_[head_].reset();
                head_ = Increment(head_);
                --size_;
                not_full_.notify_one();
                return true;
            } catch (...) {
                return false;
            }
        }

        void RequestStop() noexcept {
            std::lock_guard<std::mutex> lock(mutex_);
            stop_requested_ = true;
            not_empty_.notify_all();
            not_full_.notify_all();
        }

    private:
        [[nodiscard]] std::size_t Increment(std::size_t value) const noexcept {
            ++value;
            return value == slots_.size() ? 0U : value;
        }

        std::vector<std::optional<DecoderCommand>> slots_;
        std::size_t head_ = 0U;
        std::size_t tail_ = 0U;
        std::size_t size_ = 0U;
        bool stop_requested_ = false;
        std::mutex mutex_;
        std::condition_variable not_empty_;
        std::condition_variable not_full_;
    };

    struct DecoderLane final {
        DecoderLane(std::size_t capacity,
                    market::MarketDecoderConfigV1 decoder_config)
            : queue(capacity), decoder(decoder_config) {}

        DecoderQueue queue;
        market::MarketDecoderV1 decoder;
        std::thread thread;
    };

    Impl(RealtimePipelineConfigV1 config,
         std::shared_ptr<sdk::SdkFactory> physical_factory)
        : config_(std::move(config)),
          sdk_factory_(std::move(physical_factory)) {}

    ~Impl() { StopAndDrain(); }

    [[nodiscard]] RealtimePipelineCreateErrorV1 Initialize(
        bool factory_is_test_override,
        std::string* detail) noexcept {
        if (!ValidateConfiguration(factory_is_test_override)) {
            SetDetailLiteral(detail, "invalid realtime pipeline configuration");
            return RealtimePipelineCreateErrorV1::kInvalidConfiguration;
        }

        try {
            market::RealtimeHistoryRuntimeConfigV1 history_config{};
            history_config.source_stream_ids = config_.source_stream_ids;
            history_config.worker_count = config_.store_worker_count;
            history_config.queue_capacity_per_source_worker =
                config_.store_queue_capacity_per_source_worker;
            history_config.intraday_store = config_.intraday_store;
            history_config.registry = config_.registry;
            const market::RealtimeHistoryCreateErrorV1
                store_runtime_error =
                market::RealtimeHistoryRuntimeV1::Create(
                    history_config, &history_);
            if (store_runtime_error !=
                market::RealtimeHistoryCreateErrorV1::kNone) {
                SetDetail(
                    detail,
                    "store runtime create failed: " +
                        std::string(
                            market::RealtimeHistoryCreateErrorNameV1(
                                store_runtime_error)));
                return RealtimePipelineCreateErrorV1::
                    kStoreRuntimeCreateFailed;
            }

            const realtime::OptionalWalCreateErrorV1 wal_error =
                realtime::OptionalWalSinkV1::Create(config_.wal, &wal_);
            if (wal_error != realtime::OptionalWalCreateErrorV1::kNone) {
                SetDetail(
                    detail,
                    "optional WAL create failed: " +
                        std::string(
                            realtime::OptionalWalCreateErrorNameV1(
                                wal_error)));
                return RealtimePipelineCreateErrorV1::kWalCreateFailed;
            }

            if (config_.factor_calculator == nullptr) {
                config_.factor_calculator =
                    std::make_shared<factor::SnapshotLastPriceProjectionV1>();
            }
            factor::RealtimeFactorEngineConfigV1 factor_config{};
            factor_config.registry = config_.registry;
            factor_config.generation_runtime = history_.get();
            factor_config.calculator = config_.factor_calculator;
            const factor::RealtimeFactorEngineCreateErrorV1 factor_error =
                factor::RealtimeFactorEngineV1::Create(
                    std::move(factor_config), &factor_);
            if (factor_error !=
                factor::RealtimeFactorEngineCreateErrorV1::kNone) {
                SetDetail(
                    detail,
                    "factor engine create failed: " +
                        std::string(
                            factor::RealtimeFactorEngineCreateErrorNameV1(
                                factor_error)));
                return RealtimePipelineCreateErrorV1::kFactorCreateFailed;
            }

            for (std::uint8_t source = 0U;
                 source < market::kRealtimeHistorySourceCountV1;
                 ++source) {
                market::MarketDecoderConfigV1 decoder_config{};
                decoder_config.trade_date = config_.trade_date;
                decoder_config.source_stream_id =
                    config_.source_stream_ids[source];
                decoder_config.instrument_registry = config_.registry;
                decoder_config.limits = config_.decoder_limits;
                lanes_[source] = std::make_unique<DecoderLane>(
                    config_.decoder_queue_capacity_per_source,
                    decoder_config);
                if (!lanes_[source]->decoder.configuration_valid()) {
                    SetDetailLiteral(detail, "market decoder configuration is invalid");
                    return RealtimePipelineCreateErrorV1::
                        kInvalidConfiguration;
                }
            }

            if (!StartDecoderThreads()) {
                SetDetailLiteral(detail, "cannot start all four decoder threads");
                return RealtimePipelineCreateErrorV1::
                    kDecoderThreadStartFailed;
            }

            const RealtimePipelineCreateErrorV1 sdk_error =
                StartSdk(factory_is_test_override, detail);
            if (sdk_error != RealtimePipelineCreateErrorV1::kNone) {
                return sdk_error;
            }
            ClearDetail(detail);
            return RealtimePipelineCreateErrorV1::kNone;
        } catch (const std::bad_alloc&) {
            SetDetailLiteral(detail, "realtime pipeline allocation failed");
            return RealtimePipelineCreateErrorV1::kResourceExhausted;
        } catch (const std::length_error&) {
            SetDetailLiteral(detail, "realtime pipeline allocation length failed");
            return RealtimePipelineCreateErrorV1::kResourceExhausted;
        } catch (const std::exception& exception) {
            try {
                SetDetail(
                    detail,
                    "realtime pipeline initialization failed: " +
                        std::string(exception.what()));
            } catch (...) {
                SetDetailLiteral(detail, "realtime pipeline initialization failed");
            }
            return RealtimePipelineCreateErrorV1::kUnexpectedFailure;
        } catch (...) {
            SetDetailLiteral(detail, "realtime pipeline initialization failed unexpectedly");
            return RealtimePipelineCreateErrorV1::kUnexpectedFailure;
        }
    }

    void OnMessage(mdl::Subscriber*,
                   const mdl::MDLMessage* message) override {
        active_callbacks_.fetch_add(1U, std::memory_order_acq_rel);
        RealtimePipelineIngressErrorV1 callback_error =
            RealtimePipelineIngressErrorV1::kStopped;
        if (!callback_gate_closed_.load(std::memory_order_acquire)) {
            callback_error = Ingest(message).error;
        }
        last_callback_error_.store(
            static_cast<std::uint8_t>(callback_error),
            std::memory_order_release);
        if (active_callbacks_.fetch_sub(
                1U, std::memory_order_acq_rel) == 1U) {
            active_callbacks_.notify_all();
        }
    }

    [[nodiscard]] RealtimePipelineIngressResultV1 Ingest(
        const mdl::MDLMessage* message) noexcept {
        RealtimePipelineIngressResultV1 result{};
        std::unique_lock<std::mutex> admission(admission_mutex_);
        if (fatal_.load(std::memory_order_acquire) ||
            (history_ != nullptr && history_->fatal())) {
            result.error = RealtimePipelineIngressErrorV1::kFatal;
            ++rejected_messages_;
            TripFatalWithAdmissionLockHeld();
            return result;
        }
        if (!accepting_.load(std::memory_order_acquire)) {
            result.error = RealtimePipelineIngressErrorV1::kStopped;
            ++rejected_messages_;
            return result;
        }
        if (message == nullptr) {
            result.error = RealtimePipelineIngressErrorV1::kNullMessage;
            ++rejected_messages_;
            TripFatalWithAdmissionLockHeld();
            return result;
        }

        sdk::MessageKey key{};
        try {
            const mdl::MDLMessageHead* const head = message->GetHead();
            if (head == nullptr) {
                result.error =
                    RealtimePipelineIngressErrorV1::kOwnedMessageRejected;
                result.owned_error =
                    realtime::OwnedIngressCreateErrorV1::kNullHead;
                ++rejected_messages_;
                TripFatalWithAdmissionLockHeld();
                return result;
            }
            key = sdk::MessageKey{
                head->ServiceID, head->ServiceVersion, head->MessageID};
        } catch (...) {
            result.error =
                RealtimePipelineIngressErrorV1::kOwnedMessageRejected;
            result.owned_error =
                realtime::OwnedIngressCreateErrorV1::kSdkAccess;
            ++rejected_messages_;
            TripFatalWithAdmissionLockHeld();
            return result;
        }

        realtime::OwnedIngressSourceV1 source{};
        const realtime::OwnedIngressKeyErrorV1 key_error =
            realtime::ClassifyOwnedIngressMessageKeyV1(key, &source);
        if (key_error ==
            realtime::OwnedIngressKeyErrorV1::kForbiddenCombinedTick) {
            result.error =
                RealtimePipelineIngressErrorV1::kForbiddenCombinedTick;
            result.owned_error = realtime::OwnedIngressCreateErrorV1::
                kForbiddenCombinedTick;
            ++rejected_messages_;
            TripFatalWithAdmissionLockHeld();
            return result;
        }
        if (key_error != realtime::OwnedIngressKeyErrorV1::kNone) {
            result.error =
                RealtimePipelineIngressErrorV1::kIgnoredUnsupported;
            ++ignored_messages_;
            return result;
        }

        const std::uint8_t source_slot =
            static_cast<std::uint8_t>(source);
        result.source_slot = source_slot;
        constexpr std::uint64_t exhaustion_sentinel =
            std::numeric_limits<std::uint64_t>::max();
        // UINT64_MAX is a valid exclusive cut but is never assigned to a
        // message.  Detect that boundary here instead of misclassifying the
        // candidate as an OwnedIngress metadata failure.
        if (global_ingress_sequence_ >= exhaustion_sentinel - 1U ||
            source_sequences_[source_slot] >= exhaustion_sentinel - 1U) {
            result.error =
                RealtimePipelineIngressErrorV1::kSequenceExhausted;
            ++rejected_messages_;
            TripFatalWithAdmissionLockHeld();
            return result;
        }

        std::uint64_t realtime_ns = 0U;
        std::uint64_t monotonic_ns = 0U;
        if (!ReadClockNs(CLOCK_REALTIME, &realtime_ns) ||
            !ReadClockNs(CLOCK_MONOTONIC, &monotonic_ns)) {
            result.error = RealtimePipelineIngressErrorV1::kClockFailure;
            ++rejected_messages_;
            TripFatalWithAdmissionLockHeld();
            return result;
        }
        if (config_.enforce_receive_trade_date) {
            std::uint32_t receive_trade_date = 0U;
            if (!FixedUtc8TradeDateFromRealtimeNs(
                    realtime_ns, &receive_trade_date)) {
                result.error =
                    RealtimePipelineIngressErrorV1::kClockFailure;
                ++rejected_messages_;
                TripFatalWithAdmissionLockHeld();
                return result;
            }
            if (receive_trade_date != config_.trade_date) {
                // This is a clean terminal boundary, not corrupt market data.
                // Close admission without advancing either sequence; the
                // owner will quiesce the SDK and publish the final prior-day
                // prefix through StopAndPublishFinalGeneration().
                clean_admission_cut_ns_ = monotonic_ns;
                accepting_.store(false, std::memory_order_release);
                trade_date_boundary_reached_.store(
                    true, std::memory_order_release);
                result.error = RealtimePipelineIngressErrorV1::
                    kTradeDateBoundary;
                ++rejected_messages_;
                return result;
            }
        }

        realtime::OwnedIngressMetadataV1 metadata{};
        metadata.run_id = config_.run_id;
        metadata.global_ingress_sequence = global_ingress_sequence_ + 1U;
        metadata.source_sequence = source_sequences_[source_slot] + 1U;
        metadata.recv_realtime_ns = realtime_ns;
        metadata.recv_monotonic_ns = monotonic_ns;

        realtime::OwnedIngressMessageHandleV1 owned;
        result.owned_error = realtime::OwnedIngressMessageV1::Create(
            message,
            metadata,
            config_.maximum_sdk_message_bytes,
            &owned);
        if (result.owned_error !=
                realtime::OwnedIngressCreateErrorV1::kNone ||
            owned == nullptr || owned->source_slot() != source_slot) {
            result.error = result.owned_error ==
                                   realtime::OwnedIngressCreateErrorV1::
                                       kForbiddenCombinedTick
                               ? RealtimePipelineIngressErrorV1::
                                     kForbiddenCombinedTick
                               : RealtimePipelineIngressErrorV1::
                                     kOwnedMessageRejected;
            ++rejected_messages_;
            TripFatalWithAdmissionLockHeld();
            return result;
        }

        DecoderCommand command{};
        command.kind = CommandKind::kMessage;
        command.message = owned;
        if (!lanes_[source_slot]->queue.TryPush(std::move(command))) {
            result.error =
                RealtimePipelineIngressErrorV1::kDecoderQueueFull;
            ++rejected_messages_;
            TripFatalWithAdmissionLockHeld();
            return result;
        }

        global_ingress_sequence_ = metadata.global_ingress_sequence;
        source_sequences_[source_slot] = metadata.source_sequence;
        ++accepted_messages_;
        result.global_ingress_sequence = metadata.global_ingress_sequence;
        result.source_sequence = metadata.source_sequence;

        // The decoder queue and WAL queue receive the same immutable owner.
        // WAL status is intentionally observed only after realtime admission.
        result.wal_result = wal_->TryEnqueue(std::move(owned));
        return result;
    }

    [[nodiscard]] RealtimePipelineCutResultV1 Cut(
        std::chrono::nanoseconds timeout) noexcept {
        RealtimePipelineCutResultV1 result{};
        if (timeout <= std::chrono::nanoseconds::zero() ||
            timeout > kMaximumCutTimeout) {
            result.error = RealtimePipelineCutErrorV1::kInvalidTimeout;
            return result;
        }

        try {
            const std::lock_guard<std::mutex> cut_guard(cut_mutex_);
            return CutWithLock(timeout, false, std::nullopt);
        } catch (...) {
            result.error = RealtimePipelineCutErrorV1::kUnexpectedFailure;
            TripFatal();
            return result;
        }
    }

    [[nodiscard]] RealtimePipelineCutResultV1 StopAndPublishFinal(
        std::chrono::nanoseconds timeout) noexcept {
        RealtimePipelineCutResultV1 result{};
        if (timeout <= std::chrono::nanoseconds::zero() ||
            timeout > kMaximumCutTimeout) {
            result.error = RealtimePipelineCutErrorV1::kInvalidTimeout;
            StopAndDrain();
            return result;
        }

        try {
            const std::lock_guard<std::mutex> stop_guard(stop_mutex_);
            if (stopped_.load(std::memory_order_acquire)) {
                result.error = RealtimePipelineCutErrorV1::kStopped;
                return result;
            }
            const std::lock_guard<std::mutex> cut_guard(cut_mutex_);
            std::optional<std::uint64_t> terminal_cut_ns;
            {
                std::lock_guard<std::mutex> admission(admission_mutex_);
                if (clean_admission_cut_ns_.has_value()) {
                    terminal_cut_ns = clean_admission_cut_ns_;
                } else {
                    std::uint64_t monotonic_cut_ns = 0U;
                    if (!ReadClockNs(CLOCK_MONOTONIC, &monotonic_cut_ns)) {
                        result.error =
                            RealtimePipelineCutErrorV1::kClockFailure;
                        TripFatalWithAdmissionLockHeld();
                    } else {
                        clean_admission_cut_ns_ = monotonic_cut_ns;
                        terminal_cut_ns = monotonic_cut_ns;
                        accepting_.store(false, std::memory_order_release);
                    }
                }
            }

            // Shutdown is the callback-quiescence boundary. Every callback
            // admitted before it is already represented by an owned decoder
            // command; callbacks arriving after admission closure are not
            // part of the accepted prefix.
            StopSdk();
            if (result.error == RealtimePipelineCutErrorV1::kNone) {
                result = CutWithLock(timeout, true, terminal_cut_ns);
            }
            FinishStop();
            return result;
        } catch (...) {
            result.error = RealtimePipelineCutErrorV1::kUnexpectedFailure;
            TripFatal();
            StopAndDrain();
            return result;
        }
    }

    [[nodiscard]] RealtimePipelineCutResultV1 CutWithLock(
        std::chrono::nanoseconds timeout,
        bool admission_is_quiesced,
        std::optional<std::uint64_t> preclosed_monotonic_cut_ns) noexcept {
        RealtimePipelineCutResultV1 result{};
        if (timeout <= std::chrono::nanoseconds::zero() ||
            timeout > kMaximumCutTimeout) {
            result.error = RealtimePipelineCutErrorV1::kInvalidTimeout;
            return result;
        }

        try {
            const auto deadline = std::chrono::steady_clock::now() + timeout;
            std::uint64_t generation = 0U;
            {
                std::unique_lock<std::mutex> admission(admission_mutex_);
                const bool history_fatal = history_->fatal();
                if (fatal_.load(std::memory_order_acquire) ||
                    history_fatal) {
                    if (history_fatal &&
                        history_->StoreSnapshot().coverage_lost) {
                        result.generation_error =
                            market::RealtimeHistoryGenerationErrorV1::
                                kStoreFailed;
                    }
                    result.error = RealtimePipelineCutErrorV1::kFatal;
                    TripFatalWithAdmissionLockHeld();
                    return result;
                }
                if (!admission_is_quiesced &&
                    !accepting_.load(std::memory_order_acquire)) {
                    result.error = RealtimePipelineCutErrorV1::kStopped;
                    return result;
                }
                if (global_ingress_sequence_ ==
                        std::numeric_limits<std::uint64_t>::max() ||
                    last_started_generation_ ==
                        std::numeric_limits<std::uint64_t>::max() ||
                    std::any_of(
                        source_sequences_.begin(),
                        source_sequences_.end(),
                        [](std::uint64_t sequence) {
                            return sequence ==
                                   std::numeric_limits<std::uint64_t>::max();
                        })) {
                    result.error =
                        RealtimePipelineCutErrorV1::kSequenceExhausted;
                    TripFatalWithAdmissionLockHeld();
                    return result;
                }

                std::uint64_t monotonic_cut_ns =
                    preclosed_monotonic_cut_ns.value_or(0U);
                if (!preclosed_monotonic_cut_ns.has_value() &&
                    !ReadClockNs(CLOCK_MONOTONIC, &monotonic_cut_ns)) {
                    result.error =
                        RealtimePipelineCutErrorV1::kClockFailure;
                    TripFatalWithAdmissionLockHeld();
                    return result;
                }
                generation = last_started_generation_ + 1U;
                std::array<market::RealtimeSourceWatermarkV1,
                           market::kRealtimeHistorySourceCountV1>
                    sources{};
                for (std::size_t source = 0U;
                     source < sources.size();
                     ++source) {
                    sources[source].source_stream_id =
                        config_.source_stream_ids[source];
                    sources[source].sequence_exclusive =
                        source_sequences_[source] + 1U;
                }

                market::RealtimeHistoryWatermarkV1 watermark{};
                result.watermark_error =
                    market::BuildRealtimeHistoryWatermarkV1(
                        config_.run_id,
                        generation,
                        config_.trade_date,
                        global_ingress_sequence_ + 1U,
                        monotonic_cut_ns,
                        *config_.registry,
                        sources,
                        &watermark);
                if (result.watermark_error !=
                    market::RealtimeHistoryWatermarkErrorV1::kNone) {
                    result.error =
                        RealtimePipelineCutErrorV1::kWatermarkFailed;
                    TripFatalWithAdmissionLockHeld();
                    return result;
                }

                result.generation_error =
                    history_->BeginGeneration(watermark);
                if (result.generation_error !=
                    market::RealtimeHistoryGenerationErrorV1::kNone) {
                    result.error =
                        RealtimePipelineCutErrorV1::
                            kGenerationBeginFailed;
                    TripFatalWithAdmissionLockHeld();
                    return result;
                }
                last_started_generation_ = generation;

                // Admission stays locked until every marker is in its source
                // queue. Therefore no post-cut callback can overtake a marker.
                for (std::uint8_t source = 0U;
                     source < market::kRealtimeHistorySourceCountV1;
                     ++source) {
                    DecoderCommand marker{};
                    marker.kind = CommandKind::kGenerationMarker;
                    marker.generation = generation;
                    if (!lanes_[source]->queue.PushUntil(
                            std::move(marker), deadline)) {
                        result.error = RealtimePipelineCutErrorV1::
                            kMarkerAdmissionFailed;
                        TripFatalWithAdmissionLockHeld();
                        return result;
                    }
                }
            }

            result.generation_error = history_->WaitForGeneration(
                generation, Remaining(deadline), &result.store_generation);
            if (result.generation_error !=
                market::RealtimeHistoryGenerationErrorV1::kNone) {
                result.error =
                    RealtimePipelineCutErrorV1::kGenerationWaitFailed;
                TripFatal();
                return result;
            }
            if (result.store_generation == nullptr) {
                result.generation_error =
                    market::RealtimeHistoryGenerationErrorV1::
                        kStoreFailed;
                result.error =
                    RealtimePipelineCutErrorV1::kGenerationWaitFailed;
                TripFatal();
                return result;
            }

            result.factor_result =
                factor_->CalculateAndPublish(result.store_generation);
            if (!result.factor_result.published()) {
                result.error =
                    RealtimePipelineCutErrorV1::kFactorPublishFailed;
                TripFatal();
                return result;
            }
            result.factor_generation = result.factor_result.generation;
            last_published_generation_.store(
                generation, std::memory_order_release);
            return result;
        } catch (...) {
            result.error = RealtimePipelineCutErrorV1::kUnexpectedFailure;
            TripFatal();
            return result;
        }
    }

    [[nodiscard]] std::shared_ptr<
        const market::IntradayInstrumentStoreGenerationV1>
    AcquireStore() const noexcept {
        return history_ == nullptr
                   ? nullptr
                   : history_->AcquireLatestGeneration();
    }

    [[nodiscard]] std::shared_ptr<
        const factor::RealtimeFactorGenerationV1>
    AcquireFactor() const noexcept {
        return factor_ == nullptr
                   ? nullptr
                   : factor_->AcquireLatestGeneration();
    }

    [[nodiscard]] RealtimePipelineSnapshotV1 Snapshot() const noexcept {
        RealtimePipelineSnapshotV1 result{};
        {
            std::lock_guard<std::mutex> admission(admission_mutex_);
            result.accepted_messages = accepted_messages_;
            result.ignored_messages = ignored_messages_;
            result.rejected_messages = rejected_messages_;
            result.global_ingress_sequence = global_ingress_sequence_;
            result.source_sequences = source_sequences_;
            result.last_started_generation = last_started_generation_;
        }
        result.decoded_messages =
            decoded_messages_.load(std::memory_order_acquire);
        result.last_published_generation =
            last_published_generation_.load(std::memory_order_acquire);
        result.last_decode_error =
            static_cast<market::MarketDecodeErrorV1>(
                last_decode_error_.load(std::memory_order_acquire));
        if (wal_ != nullptr) {
            result.wal = wal_->Snapshot();
        }
        if (history_ != nullptr) {
            result.store = history_->StoreSnapshot();
        }
        result.accepting = accepting_.load(std::memory_order_acquire);
        result.fatal = fatal_.load(std::memory_order_acquire) ||
                       (history_ != nullptr && history_->fatal());
        result.stopped = stopped_.load(std::memory_order_acquire);
        result.trade_date_boundary_reached =
            trade_date_boundary_reached_.load(std::memory_order_acquire);
        return result;
    }

    [[nodiscard]] bool fatal() const noexcept {
        return fatal_.load(std::memory_order_acquire) ||
               (history_ != nullptr && history_->fatal());
    }

    void StopAndDrain() noexcept {
        const std::lock_guard<std::mutex> stop_guard(stop_mutex_);
        if (stopped_.load(std::memory_order_acquire)) {
            return;
        }
        const std::lock_guard<std::mutex> cut_guard(cut_mutex_);
        {
            std::lock_guard<std::mutex> admission(admission_mutex_);
            accepting_.store(false, std::memory_order_release);
        }

        StopSdk();
        FinishStop();
    }

private:
    void FinishStop() noexcept {
        RequestDecoderStop();
        JoinDecoderThreads();
        if (history_ != nullptr) {
            history_->StopAndDrain();
        }
        if (wal_ != nullptr) {
            wal_->StopAndDrain();
        }
        stopped_.store(true, std::memory_order_release);
    }

    [[nodiscard]] bool ValidateConfiguration(
        bool factory_is_test_override) const noexcept {
        if (config_.registry == nullptr || config_.registry->empty() ||
            !ProductionRegistryValid(*config_.registry) ||
            l2flow::common::IsZeroIdentity(config_.run_id) ||
            config_.trade_date == 0U ||
            config_.maximum_sdk_message_bytes < sdk::kVendorHeadBytes ||
            config_.decoder_queue_capacity_per_source == 0U ||
            config_.store_worker_count == 0U ||
            config_.store_queue_capacity_per_source_worker == 0U ||
            config_.intraday_store.chunk_record_capacity == 0U ||
            config_.intraday_store.chunk_record_capacity >
                market::kIntradayInstrumentStoreMaximumChunkRecordsV1 ||
            config_.intraday_store.maximum_session_records == 0U ||
            config_.intraday_store.maximum_session_accounted_bytes == 0U ||
            config_.intraday_store.maximum_records_per_batch == 0U ||
            config_.intraday_store.maximum_records_per_batch >
                market::kIntradayInstrumentStoreMaximumBatchRecordsV1) {
            return false;
        }
        const std::size_t maximum_body =
            static_cast<std::size_t>(config_.maximum_sdk_message_bytes) -
            sdk::kVendorHeadBytes;
        if (config_.decoder_limits.maximum_body_bytes < maximum_body) {
            return false;
        }
        for (std::size_t source = 0U;
             source < config_.source_stream_ids.size();
             ++source) {
            if (config_.source_stream_ids[source] == 0U) {
                return false;
            }
            for (std::size_t prior = 0U; prior < source; ++prior) {
                if (config_.source_stream_ids[prior] ==
                    config_.source_stream_ids[source]) {
                    return false;
                }
            }
        }
        if (!config_.sdk.enabled) {
            return !factory_is_test_override && sdk_factory_ == nullptr;
        }
        if (factory_is_test_override) {
            if (sdk_factory_ == nullptr) {
                return false;
            }
        } else if (sdk_factory_ != nullptr ||
                   config_.sdk.library_path.empty()) {
            return false;
        }
        return (factory_is_test_override ||
                config_.enforce_receive_trade_date) &&
               config_.sdk.work_threads > 0 &&
               config_.sdk.io_threads > 0 &&
               TextValid(config_.sdk.log_prefix, true) &&
               TextValid(config_.sdk.server_address, true) &&
               TextValid(config_.sdk.user_name, true) &&
               config_.sdk.heartbeat_interval_seconds != 0U &&
               config_.sdk.heartbeat_timeout_seconds >
                   config_.sdk.heartbeat_interval_seconds &&
               MessageEncodingValid(config_.sdk.message_encoding);
    }

    [[nodiscard]] bool StartDecoderThreads() noexcept {
        try {
            for (std::uint8_t source = 0U;
                 source < market::kRealtimeHistorySourceCountV1;
                 ++source) {
                lanes_[source]->thread = std::thread([this, source] {
                    DecoderLoop(source);
                });
            }
            decoder_threads_started_ = true;
            return true;
        } catch (...) {
            RequestDecoderStop();
            JoinDecoderThreads();
            return false;
        }
    }

    [[nodiscard]] RealtimePipelineCreateErrorV1 StartSdk(
        bool factory_is_test_override,
        std::string* detail) noexcept {
        if (!config_.sdk.enabled) {
            accepting_.store(true, std::memory_order_release);
            return RealtimePipelineCreateErrorV1::kNone;
        }

        try {
            if (!factory_is_test_override) {
                std::string load_error;
                sdk_factory_ = sdk::LoadSdkFactoryFromPath(
                    config_.sdk.library_path, &load_error);
                if (sdk_factory_ == nullptr) {
                    SetDetail(
                        detail,
                        load_error.empty()
                            ? "direct SDK load failed"
                            : std::move(load_error));
                    return RealtimePipelineCreateErrorV1::kSdkLoadFailed;
                }
            }

            sdk_manager_ = sdk_factory_->Create(
                config_.sdk.work_threads, config_.sdk.io_threads);
            if (sdk_manager_ == nullptr) {
                SetDetailLiteral(detail, "physical SDK factory returned a null manager");
                return RealtimePipelineCreateErrorV1::
                    kSdkManagerCreateFailed;
            }
            sdk_manager_->EnableLog(
                config_.sdk.log_prefix, config_.sdk.log_to_console);
            sdk_subscriber_ = sdk_manager_->CreateSubscriber(this, false);
            if (sdk_subscriber_ == nullptr) {
                SetDetailLiteral(detail, "physical SDK manager returned a null Subscriber");
                return RealtimePipelineCreateErrorV1::
                    kSdkSubscriberCreateFailed;
            }

            sdk_subscriber_->SetServerAddress(config_.sdk.server_address);
            sdk_subscriber_->SetUserName(config_.sdk.user_name);
            sdk_subscriber_->SetHeartbeatInterval(
                config_.sdk.heartbeat_interval_seconds);
            sdk_subscriber_->SetHeartbeatTimeout(
                config_.sdk.heartbeat_timeout_seconds);
            sdk_subscriber_->SetMessageEncoding(
                config_.sdk.message_encoding);
            sdk_subscriber_->EnableMergeMessage(config_.sdk.merge_message);
            sdk_subscriber_->SetSendMacAuth(config_.sdk.send_mac_auth);
            sdk_subscriber_->EnableServerSelect(config_.sdk.server_select);
            sdk::AddProductionSubscriptionsV1(*sdk_subscriber_);

            // Connect may synchronously deliver API/SYS or market callbacks.
            // Every downstream component is already running at this point.
            accepting_.store(true, std::memory_order_release);
            const std::string connect_error = sdk_subscriber_->Connect();
            if (!connect_error.empty()) {
                accepting_.store(false, std::memory_order_release);
                SetDetail(detail, "SDK Connect failed: " + connect_error);
                return RealtimePipelineCreateErrorV1::kSdkConnectFailed;
            }
            if (fatal_.load(std::memory_order_acquire)) {
                accepting_.store(false, std::memory_order_release);
                SetDetailLiteral(detail, "a callback failed during SDK Connect");
                return RealtimePipelineCreateErrorV1::kSdkCallbackFailed;
            }
            return RealtimePipelineCreateErrorV1::kNone;
        } catch (const std::bad_alloc&) {
            SetDetailLiteral(detail, "SDK composition allocation failed");
            return RealtimePipelineCreateErrorV1::kResourceExhausted;
        } catch (const std::exception& exception) {
            accepting_.store(false, std::memory_order_release);
            try {
                SetDetail(
                    detail,
                    "SDK configuration failed: " +
                        std::string(exception.what()));
            } catch (...) {
                SetDetailLiteral(detail, "SDK configuration failed");
            }
            return RealtimePipelineCreateErrorV1::kSdkConfigurationFailed;
        } catch (...) {
            accepting_.store(false, std::memory_order_release);
            SetDetailLiteral(detail, "SDK configuration failed unexpectedly");
            return RealtimePipelineCreateErrorV1::kSdkConfigurationFailed;
        }
    }

    void DecoderLoop(std::uint8_t source) noexcept {
        DecoderCommand command{};
        while (lanes_[source]->queue.Pop(&command)) {
            if (fatal_.load(std::memory_order_acquire)) {
                command = DecoderCommand{};
                continue;
            }
            if (command.kind == CommandKind::kGenerationMarker) {
                const market::RealtimeHistoryGenerationErrorV1 error =
                    history_->SealSource(source, command.generation);
                if (error !=
                    market::RealtimeHistoryGenerationErrorV1::kNone) {
                    TripFatal();
                }
            } else if (!DecodeOne(source, command.message)) {
                TripFatal();
            }
            command = DecoderCommand{};
        }
    }

    [[nodiscard]] bool DecodeOne(
        std::uint8_t source,
        const realtime::OwnedIngressMessageHandleV1& message) noexcept {
        if (message == nullptr || message->source_slot() != source ||
            message->recv_realtime_ns() >
                static_cast<std::uint64_t>(
                    std::numeric_limits<std::int64_t>::max()) ||
            message->recv_monotonic_ns() >
                static_cast<std::uint64_t>(
                    std::numeric_limits<std::int64_t>::max())) {
            return false;
        }

        const sdk::VendorHeadView head = message->vendor_head();
        if (head.message_encoding() !=
            static_cast<std::uint8_t>(mdl::MDLEID_BINARY)) {
            return false;
        }
        market::MarketMessageViewV1 view{};
        view.source_stream_id = config_.source_stream_ids[source];
        view.trade_date = config_.trade_date;
        view.source_sequence = message->source_sequence();
        view.service_id = message->key().service_id;
        view.service_version = message->key().service_version;
        view.message_id = message->key().message_id;
        view.message_encoding = head.message_encoding();
        view.vendor_local_time_raw = head.local_time_raw();
        view.vendor_sequence_id = head.sequence_id();
        view.recv_realtime_ns =
            static_cast<std::int64_t>(message->recv_realtime_ns());
        view.recv_monotonic_ns =
            static_cast<std::int64_t>(message->recv_monotonic_ns());
        view.body = message->body();

        market::DecodedMarketEventV1 decoded;
        const market::MarketDecodeErrorV1 decode_error =
            lanes_[source]->decoder.Decode(view, &decoded);
        last_decode_error_.store(
            static_cast<std::uint8_t>(decode_error),
            std::memory_order_release);
        if (decode_error != market::MarketDecodeErrorV1::kNone) {
            return false;
        }

        market::RetainedMarketEventV1 retained;
        if (market::RetainMarketEventV1(
                std::move(decoded), &retained) !=
            market::RetainedMarketEventCreateErrorV1::kNone) {
            return false;
        }
        market::RealtimeHistoryRecordHandleV1 record;
        if (!market::RealtimeHistoryRecordV1::Create(
                source,
                message->global_ingress_sequence(),
                std::move(retained),
                &record)) {
            return false;
        }
        if (history_->TrySubmit(std::move(record)) !=
            market::RealtimeHistorySubmitErrorV1::kNone) {
            return false;
        }
        decoded_messages_.fetch_add(1U, std::memory_order_relaxed);
        return true;
    }

    void TripFatalWithAdmissionLockHeld() noexcept {
        // Close ingress first. History owns the generation/publication commit
        // lock, so its fatal transition is the linearization point that
        // prevents a factor store from racing past this failure.
        accepting_.store(false, std::memory_order_release);
        if (history_ != nullptr) {
            history_->MarkFatal();
        }
        fatal_.store(true, std::memory_order_release);
        RequestDecoderStop();
    }

    void TripFatal() noexcept {
        std::lock_guard<std::mutex> admission(admission_mutex_);
        TripFatalWithAdmissionLockHeld();
    }

    void RequestDecoderStop() noexcept {
        for (const std::unique_ptr<DecoderLane>& lane : lanes_) {
            if (lane != nullptr) {
                lane->queue.RequestStop();
            }
        }
    }

    void JoinDecoderThreads() noexcept {
        for (const std::unique_ptr<DecoderLane>& lane : lanes_) {
            if (lane != nullptr && lane->thread.joinable()) {
                lane->thread.join();
            }
        }
        decoder_threads_started_ = false;
    }

    void StopSdk() noexcept {
        callback_gate_closed_.store(true, std::memory_order_release);
        if (sdk_manager_ != nullptr) {
            try {
                sdk_manager_->Shutdown();
            } catch (...) {
                // The narrow adapters cannot be safely destroyed while the
                // vendor may still call the handler. Continuing would create
                // a dangling callback target.
                std::terminate();
            }
        }
        std::uint64_t active =
            active_callbacks_.load(std::memory_order_acquire);
        while (active != 0U) {
            active_callbacks_.wait(active, std::memory_order_acquire);
            active = active_callbacks_.load(std::memory_order_acquire);
        }
        bool release_failed = false;
        std::string ignored;
        if (sdk_subscriber_ != nullptr) {
            if (!sdk_subscriber_->Release(&ignored)) {
                release_failed = true;
            }
            sdk_subscriber_.reset();
        }
        if (sdk_manager_ != nullptr) {
            if (!sdk_manager_->Release(&ignored)) {
                release_failed = true;
            }
            sdk_manager_.reset();
        }
        sdk_factory_.reset();
        if (release_failed) {
            TripFatal();
        }
    }

    RealtimePipelineConfigV1 config_{};
    std::array<std::unique_ptr<DecoderLane>,
               market::kRealtimeHistorySourceCountV1>
        lanes_{};
    bool decoder_threads_started_ = false;

    std::unique_ptr<market::RealtimeHistoryRuntimeV1> history_;
    std::unique_ptr<factor::RealtimeFactorEngineV1> factor_;
    std::unique_ptr<realtime::OptionalWalSinkV1> wal_;

    std::shared_ptr<sdk::SdkFactory> sdk_factory_;
    std::unique_ptr<sdk::SdkManager> sdk_manager_;
    std::unique_ptr<sdk::SdkSubscriber> sdk_subscriber_;

    mutable std::mutex admission_mutex_;
    std::uint64_t global_ingress_sequence_ = 0U;
    std::array<std::uint64_t,
               market::kRealtimeHistorySourceCountV1>
        source_sequences_{};
    std::uint64_t accepted_messages_ = 0U;
    std::uint64_t ignored_messages_ = 0U;
    std::uint64_t rejected_messages_ = 0U;
    std::uint64_t last_started_generation_ = 0U;
    std::optional<std::uint64_t> clean_admission_cut_ns_;

    std::atomic<std::uint64_t> decoded_messages_{0U};
    std::atomic<std::uint64_t> last_published_generation_{0U};
    std::atomic<std::uint8_t> last_decode_error_{
        static_cast<std::uint8_t>(market::MarketDecodeErrorV1::kNone)};
    std::atomic<std::uint8_t> last_callback_error_{
        static_cast<std::uint8_t>(
            RealtimePipelineIngressErrorV1::kNone)};
    std::atomic<bool> accepting_{false};
    std::atomic<bool> fatal_{false};
    std::atomic<bool> stopped_{false};
    std::atomic<bool> trade_date_boundary_reached_{false};

    std::atomic<bool> callback_gate_closed_{false};
    std::atomic<std::uint64_t> active_callbacks_{0U};

    std::mutex cut_mutex_;
    std::mutex stop_mutex_;
};

RealtimePipelineCreateErrorV1 RealtimePipelineV1::CreateImpl(
    RealtimePipelineConfigV1 config,
    std::shared_ptr<sdk::SdkFactory> physical_factory,
    bool factory_is_test_override,
    std::unique_ptr<RealtimePipelineV1>* output,
    std::string* detail) noexcept {
    if (output == nullptr) {
        SetDetailLiteral(detail, "realtime pipeline output is null");
        return RealtimePipelineCreateErrorV1::kNullOutput;
    }
    output->reset();
    ClearDetail(detail);
    try {
        auto impl = std::make_unique<Impl>(
            std::move(config), std::move(physical_factory));
        const RealtimePipelineCreateErrorV1 error =
            impl->Initialize(factory_is_test_override, detail);
        if (error != RealtimePipelineCreateErrorV1::kNone) {
            return error;
        }
        output->reset(new RealtimePipelineV1(std::move(impl)));
        return RealtimePipelineCreateErrorV1::kNone;
    } catch (const std::bad_alloc&) {
        SetDetailLiteral(detail, "realtime pipeline allocation failed");
        return RealtimePipelineCreateErrorV1::kResourceExhausted;
    } catch (...) {
        SetDetailLiteral(detail, "realtime pipeline creation failed unexpectedly");
        return RealtimePipelineCreateErrorV1::kUnexpectedFailure;
    }
}

RealtimePipelineV1::RealtimePipelineV1(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

RealtimePipelineV1::~RealtimePipelineV1() = default;

RealtimePipelineCreateErrorV1 RealtimePipelineV1::Create(
    RealtimePipelineConfigV1 config,
    std::unique_ptr<RealtimePipelineV1>* output,
    std::string* detail) noexcept {
    return CreateImpl(
        std::move(config), nullptr, false, output, detail);
}

RealtimePipelineCreateErrorV1 RealtimePipelineV1::CreateForTest(
    RealtimePipelineConfigV1 config,
    std::shared_ptr<sdk::SdkFactory> physical_factory,
    std::unique_ptr<RealtimePipelineV1>* output,
    std::string* detail) noexcept {
    return CreateImpl(
        std::move(config),
        std::move(physical_factory),
        true,
        output,
        detail);
}

RealtimePipelineIngressResultV1
RealtimePipelineV1::InjectSdkMessageForTest(
    const mdl::MDLMessage* message) noexcept {
    return impl_->Ingest(message);
}

RealtimePipelineCutResultV1
RealtimePipelineV1::CutAndPublishGeneration(
    std::chrono::nanoseconds timeout) noexcept {
    return impl_->Cut(timeout);
}

RealtimePipelineCutResultV1
RealtimePipelineV1::StopAndPublishFinalGeneration(
    std::chrono::nanoseconds timeout) noexcept {
    return impl_->StopAndPublishFinal(timeout);
}

std::shared_ptr<const market::IntradayInstrumentStoreGenerationV1>
RealtimePipelineV1::AcquireLatestStoreGeneration() const noexcept {
    return impl_->AcquireStore();
}

std::shared_ptr<const factor::RealtimeFactorGenerationV1>
RealtimePipelineV1::AcquireLatestFactorGeneration() const noexcept {
    return impl_->AcquireFactor();
}

RealtimePipelineSnapshotV1 RealtimePipelineV1::Snapshot() const noexcept {
    return impl_->Snapshot();
}

bool RealtimePipelineV1::fatal() const noexcept {
    return impl_->fatal();
}

void RealtimePipelineV1::StopAndDrain() noexcept {
    impl_->StopAndDrain();
}

}  // namespace l2flow::runtime
