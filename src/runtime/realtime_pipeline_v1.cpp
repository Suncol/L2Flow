#include "l2flow/runtime/realtime_pipeline_v1.h"

#include "l2flow/market/mainland_a_share_filter_v1.h"
#include "l2flow/runtime/detail/closeable_publication_gate.h"
#include "l2flow/sdk/direct_sdk_runtime_v1.h"
#include "l2flow/sdk/production_subscription_v1.h"

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstdio>
#include <ctime>
#include <limits>
#include <mutex>
#include <new>
#include <optional>
#include <semaphore>
#include <stdexcept>
#include <thread>
#include <type_traits>
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
constexpr auto kProgressPublishMaximumDelay =
    std::chrono::milliseconds(1);

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

[[nodiscard]] constexpr bool IsMixedTickSourceSlot(
    std::uint8_t source_slot) noexcept {
    return source_slot ==
               static_cast<std::uint8_t>(
                   realtime::OwnedIngressSourceV1::kShanghaiTick) ||
           source_slot ==
               static_cast<std::uint8_t>(
                   realtime::OwnedIngressSourceV1::kShenzhenTick);
}

[[nodiscard]] constexpr market::MainlandExchangeV1
MainlandExchangeForMarket(market::MarketV1 value) noexcept {
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

class ConcurrentLinearLatencyHistogram final {
public:
    ConcurrentLinearLatencyHistogram(
        std::int64_t minimum_ns,
        std::int64_t maximum_exclusive_ns,
        std::uint64_t bucket_width_ns)
        : minimum_ns_(minimum_ns),
          maximum_exclusive_ns_(maximum_exclusive_ns),
          bucket_width_ns_(bucket_width_ns),
          buckets_(BucketCount(
              minimum_ns, maximum_exclusive_ns, bucket_width_ns)) {
        for (std::atomic<std::uint64_t>& bucket : buckets_) {
            bucket.store(0U, std::memory_order_relaxed);
        }
    }

    ConcurrentLinearLatencyHistogram(
        const ConcurrentLinearLatencyHistogram&) = delete;
    ConcurrentLinearLatencyHistogram& operator=(
        const ConcurrentLinearLatencyHistogram&) = delete;

    void AddInvalid() noexcept {
        invalid_samples_.fetch_add(1U, std::memory_order_relaxed);
    }

    void Add(std::int64_t value) noexcept {
        UpdateMinimum(value);
        UpdateMaximum(value);
        AddToSum(value);
        if (value < minimum_ns_) {
            below_range_.fetch_add(1U, std::memory_order_relaxed);
        } else if (value >= maximum_exclusive_ns_) {
            above_range_.fetch_add(1U, std::memory_order_relaxed);
        } else {
            const std::uint64_t offset = static_cast<std::uint64_t>(
                value - minimum_ns_);
            const std::size_t index = static_cast<std::size_t>(
                offset / bucket_width_ns_);
            buckets_[index].fetch_add(1U, std::memory_order_relaxed);
        }
        samples_.fetch_add(1U, std::memory_order_release);
    }

    [[nodiscard]] RealtimeLatencyDistributionV1 Snapshot() const noexcept {
        RealtimeLatencyDistributionV1 result{};
        result.samples = samples_.load(std::memory_order_acquire);
        result.invalid_samples =
            invalid_samples_.load(std::memory_order_acquire);
        result.below_histogram_range =
            below_range_.load(std::memory_order_acquire);
        result.above_histogram_range =
            above_range_.load(std::memory_order_acquire);
        result.histogram_minimum_ns = minimum_ns_;
        result.histogram_maximum_ns = maximum_exclusive_ns_ - 1;
        result.histogram_bucket_width_ns = bucket_width_ns_;
        result.sum_saturated =
            sum_saturated_.load(std::memory_order_acquire);
        if (result.samples == 0U) {
            return result;
        }
        result.minimum_ns = minimum_observed_.load(std::memory_order_acquire);
        result.maximum_ns = maximum_observed_.load(std::memory_order_acquire);
        result.mean_ns = sum_.load(std::memory_order_acquire) /
                         static_cast<std::int64_t>(result.samples);
        result.p50 = Quantile(result.samples, 50U, 100U);
        result.p90 = Quantile(result.samples, 90U, 100U);
        result.p95 = Quantile(result.samples, 95U, 100U);
        result.p99 = Quantile(result.samples, 99U, 100U);
        result.p999 = Quantile(result.samples, 999U, 1000U);
        return result;
    }

private:
    [[nodiscard]] static std::size_t BucketCount(
        std::int64_t minimum_ns,
        std::int64_t maximum_exclusive_ns,
        std::uint64_t bucket_width_ns) {
        if (maximum_exclusive_ns <= minimum_ns || bucket_width_ns == 0U) {
            throw std::invalid_argument("invalid latency histogram range");
        }
        std::uint64_t range = 0U;
        if (minimum_ns < 0 && maximum_exclusive_ns >= 0) {
            const std::uint64_t negative_magnitude =
                static_cast<std::uint64_t>(-(minimum_ns + 1)) + 1U;
            const std::uint64_t positive =
                static_cast<std::uint64_t>(maximum_exclusive_ns);
            if (positive >
                std::numeric_limits<std::uint64_t>::max() -
                    negative_magnitude) {
                throw std::length_error("latency histogram is too large");
            }
            range = negative_magnitude + positive;
        } else {
            range = static_cast<std::uint64_t>(
                maximum_exclusive_ns - minimum_ns);
        }
        const std::uint64_t count =
            range / bucket_width_ns +
            (range % bucket_width_ns == 0U ? 0U : 1U);
        if (count == 0U ||
            count > static_cast<std::uint64_t>(
                        std::numeric_limits<std::size_t>::max())) {
            throw std::length_error("latency histogram is too large");
        }
        return static_cast<std::size_t>(count);
    }

    void UpdateMinimum(std::int64_t value) noexcept {
        std::int64_t current =
            minimum_observed_.load(std::memory_order_relaxed);
        while (value < current &&
               !minimum_observed_.compare_exchange_weak(
                   current,
                   value,
                   std::memory_order_relaxed,
                   std::memory_order_relaxed)) {
        }
    }

    void UpdateMaximum(std::int64_t value) noexcept {
        std::int64_t current =
            maximum_observed_.load(std::memory_order_relaxed);
        while (value > current &&
               !maximum_observed_.compare_exchange_weak(
                   current,
                   value,
                   std::memory_order_relaxed,
                   std::memory_order_relaxed)) {
        }
    }

    void AddToSum(std::int64_t value) noexcept {
        std::int64_t current = sum_.load(std::memory_order_relaxed);
        for (;;) {
            std::int64_t next = 0;
            bool saturated = false;
            if (value > 0 &&
                current > std::numeric_limits<std::int64_t>::max() - value) {
                next = std::numeric_limits<std::int64_t>::max();
                saturated = true;
            } else if (
                value < 0 &&
                current < std::numeric_limits<std::int64_t>::min() - value) {
                next = std::numeric_limits<std::int64_t>::min();
                saturated = true;
            } else {
                next = current + value;
            }
            if (sum_.compare_exchange_weak(
                    current,
                    next,
                    std::memory_order_relaxed,
                    std::memory_order_relaxed)) {
                if (saturated) {
                    sum_saturated_.store(true, std::memory_order_relaxed);
                }
                return;
            }
        }
    }

    [[nodiscard]] RealtimeLatencyQuantileV1 Quantile(
        std::uint64_t samples,
        std::uint64_t numerator,
        std::uint64_t denominator) const noexcept {
        RealtimeLatencyQuantileV1 result{};
        if (samples == 0U || denominator == 0U) {
            return result;
        }
        const std::uint64_t sample_index = samples - 1U;
        const std::uint64_t quotient = sample_index / denominator;
        const std::uint64_t remainder = sample_index % denominator;
        const std::uint64_t rank = quotient * numerator +
            (remainder * numerator + denominator - 1U) / denominator;
        std::uint64_t cumulative =
            below_range_.load(std::memory_order_acquire);
        if (rank < cumulative) {
            result.lower_bound_ns =
                minimum_observed_.load(std::memory_order_acquire);
            result.upper_bound_ns = minimum_ns_ - 1;
            result.estimate_ns = result.upper_bound_ns;
            result.clipped_below = true;
            return result;
        }
        for (std::size_t index = 0U; index < buckets_.size(); ++index) {
            const std::uint64_t count =
                buckets_[index].load(std::memory_order_acquire);
            if (count > std::numeric_limits<std::uint64_t>::max() -
                            cumulative) {
                cumulative = std::numeric_limits<std::uint64_t>::max();
            } else {
                cumulative += count;
            }
            if (rank < cumulative) {
                const std::uint64_t offset =
                    static_cast<std::uint64_t>(index) * bucket_width_ns_;
                result.lower_bound_ns = minimum_ns_ +
                    static_cast<std::int64_t>(offset);
                result.upper_bound_ns = std::min(
                    result.lower_bound_ns +
                        static_cast<std::int64_t>(bucket_width_ns_ - 1U),
                    maximum_exclusive_ns_ - 1);
                result.estimate_ns = result.lower_bound_ns +
                    (result.upper_bound_ns - result.lower_bound_ns) / 2;
                return result;
            }
        }
        result.lower_bound_ns = maximum_exclusive_ns_;
        result.upper_bound_ns =
            maximum_observed_.load(std::memory_order_acquire);
        result.estimate_ns = result.lower_bound_ns;
        result.clipped_above = true;
        return result;
    }

    const std::int64_t minimum_ns_;
    const std::int64_t maximum_exclusive_ns_;
    const std::uint64_t bucket_width_ns_;
    std::vector<std::atomic<std::uint64_t>> buckets_;
    std::atomic<std::uint64_t> samples_{0U};
    std::atomic<std::uint64_t> invalid_samples_{0U};
    std::atomic<std::uint64_t> below_range_{0U};
    std::atomic<std::uint64_t> above_range_{0U};
    std::atomic<std::int64_t> minimum_observed_{
        std::numeric_limits<std::int64_t>::max()};
    std::atomic<std::int64_t> maximum_observed_{
        std::numeric_limits<std::int64_t>::min()};
    std::atomic<std::int64_t> sum_{0};
    std::atomic<bool> sum_saturated_{false};
};

[[nodiscard]] bool FixedUtc8MidnightNs(
    std::uint32_t trade_date,
    std::int64_t* output) noexcept {
    if (output == nullptr) {
        return false;
    }
    const int year = static_cast<int>(trade_date / 10'000U);
    const unsigned int month = (trade_date / 100U) % 100U;
    const unsigned int day = trade_date % 100U;
    const std::chrono::year_month_day calendar{
        std::chrono::year(year),
        std::chrono::month(month),
        std::chrono::day(day)};
    if (!calendar.ok()) {
        return false;
    }
    const auto utc_midnight = std::chrono::sys_days(calendar) -
                              std::chrono::hours(8);
    const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        utc_midnight.time_since_epoch());
    *output = ns.count();
    return true;
}

[[nodiscard]] bool SignedDifference(
    std::uint64_t later,
    std::int64_t earlier,
    std::int64_t* output) noexcept {
    if (output == nullptr) {
        return false;
    }
    if (later > static_cast<std::uint64_t>(
                    std::numeric_limits<std::int64_t>::max())) {
        return false;
    }
    const std::int64_t signed_later = static_cast<std::int64_t>(later);
    if (earlier < 0 &&
        signed_later >
            std::numeric_limits<std::int64_t>::max() + earlier) {
        return false;
    }
    *output = signed_later - earlier;
    return true;
}

[[nodiscard]] bool UnsignedElapsed(
    std::uint64_t begin,
    std::uint64_t end,
    std::int64_t* output) noexcept {
    if (output == nullptr || end < begin ||
        end - begin >
            static_cast<std::uint64_t>(
                std::numeric_limits<std::int64_t>::max())) {
        return false;
    }
    *output = static_cast<std::int64_t>(end - begin);
    return true;
}

class StageLatencyCollector final {
public:
    explicit StageLatencyCollector(std::uint32_t trade_date)
        : trade_date_(trade_date),
          sdk_local_to_callback_success_(
              -60'000'000'000LL, 60'000'000'000LL, 100'000ULL),
          sdk_local_to_append_complete_(
              -60'000'000'000LL, 60'000'000'000LL, 100'000ULL),
          callback_entry_to_success_(0, 20'000'000LL, 50ULL),
          callback_entry_to_append_complete_(
              0, 5'000'000'000LL, 5'000ULL),
          callback_entry_to_inprocess_latest_read_(
              0, 5'000'000'000LL, 5'000ULL),
          append_call_(0, 20'000'000LL, 50ULL) {
        trade_date_valid_ =
            FixedUtc8MidnightNs(trade_date_, &fixed_utc8_midnight_ns_);
    }

    void RecordCallback(
        const RealtimePipelineIngressResultV1& ingress,
        std::uint64_t entry_monotonic_ns,
        std::uint64_t success_realtime_ns,
        std::uint64_t success_monotonic_ns,
        bool clock_observation_valid) noexcept {
        if (!ingress.accepted() ||
            ingress.source_slot >=
                market::kRealtimeHistorySourceCountV1) {
            return;
        }
        callback_samples_by_source_[ingress.source_slot].fetch_add(
            1U, std::memory_order_relaxed);
        std::int64_t elapsed = 0;
        if (clock_observation_valid &&
            UnsignedElapsed(
                entry_monotonic_ns, success_monotonic_ns, &elapsed)) {
            callback_entry_to_success_.Add(elapsed);
        } else {
            callback_entry_to_success_.AddInvalid();
        }
        std::int64_t vendor_realtime_ns = 0;
        std::int64_t sdk_age = 0;
        if (clock_observation_valid &&
            VendorRealtimeNs(
                ingress.vendor_local_time_raw, &vendor_realtime_ns) &&
            SignedDifference(
                success_realtime_ns, vendor_realtime_ns, &sdk_age)) {
            sdk_local_to_callback_success_.Add(sdk_age);
        } else {
            sdk_local_to_callback_success_.AddInvalid();
        }
    }

    void RecordAppend(
        const market::RealtimeHistoryAppendObservationV1& observation)
        noexcept {
        if (observation.source_slot >=
            market::kRealtimeHistorySourceCountV1) {
            return;
        }
        append_samples_by_source_[observation.source_slot].fetch_add(
            1U, std::memory_order_relaxed);
        std::int64_t elapsed = 0;
        if (observation.clock_observation_valid &&
            observation.recv_monotonic_ns >= 0 &&
            UnsignedElapsed(
                static_cast<std::uint64_t>(
                    observation.recv_monotonic_ns),
                observation.append_complete_monotonic_ns,
                &elapsed)) {
            callback_entry_to_append_complete_.Add(elapsed);
        } else {
            callback_entry_to_append_complete_.AddInvalid();
        }
        if (observation.clock_observation_valid &&
            UnsignedElapsed(
                observation.append_start_monotonic_ns,
                observation.append_complete_monotonic_ns,
                &elapsed)) {
            append_call_.Add(elapsed);
        } else {
            append_call_.AddInvalid();
        }
        if (observation.inprocess_latest_read_observation_valid &&
            observation.recv_monotonic_ns >= 0 &&
            UnsignedElapsed(
                static_cast<std::uint64_t>(
                    observation.recv_monotonic_ns),
                observation
                    .inprocess_latest_read_complete_monotonic_ns,
                &elapsed)) {
            callback_entry_to_inprocess_latest_read_.Add(elapsed);
        } else {
            callback_entry_to_inprocess_latest_read_.AddInvalid();
        }
        std::int64_t vendor_realtime_ns = 0;
        std::int64_t sdk_age = 0;
        if (observation.clock_observation_valid &&
            VendorRealtimeNs(
                observation.vendor_local_time_raw,
                &vendor_realtime_ns) &&
            SignedDifference(
                observation.append_complete_realtime_ns,
                vendor_realtime_ns,
                &sdk_age)) {
            sdk_local_to_append_complete_.Add(sdk_age);
        } else {
            sdk_local_to_append_complete_.AddInvalid();
        }
    }

    [[nodiscard]] RealtimePipelineStageLatencySnapshotV1 Snapshot()
        const noexcept {
        RealtimePipelineStageLatencySnapshotV1 result{};
        result.enabled = true;
        result.sdk_local_time_trade_date = trade_date_;
        for (std::size_t source = 0U;
             source < market::kRealtimeHistorySourceCountV1;
             ++source) {
            result.callback_samples_by_source[source] =
                callback_samples_by_source_[source].load(
                    std::memory_order_acquire);
            result.append_samples_by_source[source] =
                append_samples_by_source_[source].load(
                    std::memory_order_acquire);
        }
        result.sdk_local_to_callback_success =
            sdk_local_to_callback_success_.Snapshot();
        result.sdk_local_to_append_complete =
            sdk_local_to_append_complete_.Snapshot();
        result.callback_entry_to_success =
            callback_entry_to_success_.Snapshot();
        result.callback_entry_to_append_complete =
            callback_entry_to_append_complete_.Snapshot();
        result.callback_entry_to_inprocess_latest_read =
            callback_entry_to_inprocess_latest_read_.Snapshot();
        result.append_call = append_call_.Snapshot();
        return result;
    }

private:
    [[nodiscard]] bool VendorRealtimeNs(
        std::uint32_t raw,
        std::int64_t* output) const noexcept {
        if (!trade_date_valid_ || output == nullptr ||
            raw >= 1'000'000'000U) {
            return false;
        }
        const std::uint32_t hour = raw / 10'000'000U;
        const std::uint32_t minute = (raw / 100'000U) % 100U;
        const std::uint32_t second = (raw / 1'000U) % 100U;
        const std::uint32_t millisecond = raw % 1'000U;
        if (hour >= 24U || minute >= 60U || second >= 60U) {
            return false;
        }
        const std::uint64_t seconds_since_midnight =
            static_cast<std::uint64_t>(hour) * 3'600U +
            static_cast<std::uint64_t>(minute) * 60U + second;
        const std::uint64_t since_midnight_ns =
            seconds_since_midnight * 1'000'000'000ULL +
            static_cast<std::uint64_t>(millisecond) * 1'000'000ULL;
        if (fixed_utc8_midnight_ns_ < 0 ||
            since_midnight_ns > static_cast<std::uint64_t>(
                std::numeric_limits<std::int64_t>::max() -
                fixed_utc8_midnight_ns_)) {
            return false;
        }
        *output = fixed_utc8_midnight_ns_ +
                  static_cast<std::int64_t>(since_midnight_ns);
        return true;
    }

    const std::uint32_t trade_date_;
    std::int64_t fixed_utc8_midnight_ns_ = 0;
    bool trade_date_valid_ = false;
    std::array<std::atomic<std::uint64_t>,
               market::kRealtimeHistorySourceCountV1>
        callback_samples_by_source_{};
    std::array<std::atomic<std::uint64_t>,
               market::kRealtimeHistorySourceCountV1>
        append_samples_by_source_{};
    ConcurrentLinearLatencyHistogram sdk_local_to_callback_success_;
    ConcurrentLinearLatencyHistogram sdk_local_to_append_complete_;
    ConcurrentLinearLatencyHistogram callback_entry_to_success_;
    ConcurrentLinearLatencyHistogram callback_entry_to_append_complete_;
    ConcurrentLinearLatencyHistogram
        callback_entry_to_inprocess_latest_read_;
    ConcurrentLinearLatencyHistogram append_call_;
};

[[nodiscard]] bool BoundedAppliedWindowCapacity(
    const RealtimePipelineConfigV1& config,
    std::size_t* output) noexcept {
    if (output == nullptr ||
        config.decoder_queue_capacity_per_source >
            (std::numeric_limits<std::size_t>::max() - 4U) / 4U) {
        return false;
    }
    // This explicit dispatcher-side gate, rather than any expectation about
    // queue drain speed, bounds how far completion may run ahead of a missing
    // applied sequence.
    const std::size_t maximum =
        config.decoder_queue_capacity_per_source * 4U + 4U + 1U;
    if (maximum == 0U ||
        maximum > realtime::kOwnedIngressMaximumInflightMessagesV1) {
        return false;
    }
    *output = maximum;
    return true;
}

[[nodiscard]] bool BoundedIngressPoolCapacity(
    const RealtimePipelineConfigV1& config,
    std::size_t applied_window,
    std::size_t* output) noexcept {
    if (output == nullptr || applied_window == 0U) {
        return false;
    }
    std::size_t maximum = applied_window;
    constexpr std::size_t callback_owner = 1U;
    const std::array<std::size_t, 2U> additions{
        config.processing_queue_capacity,
        callback_owner};
    for (const std::size_t addition : additions) {
        if (addition >
            std::numeric_limits<std::size_t>::max() - maximum) {
            return false;
        }
        maximum += addition;
    }
    if (maximum == 0U ||
        maximum > realtime::kOwnedIngressMaximumInflightMessagesV1) {
        return false;
    }
    *output = maximum;
    return true;
}

inline constexpr std::uint32_t kIngressPrewarmMessageBytes = 4096U;
static_assert(
    sizeof(realtime::OwnedIngressMessageV1) +
            kIngressPrewarmMessageBytes -
            l2flow::sdk::kVendorHeadBytes <=
        8192U);
inline constexpr std::size_t kIngressPrewarmMaximumBlocks =
    l2flow::realtime::kOwnedIngressMaximumPrewarmBytesV1 / 8192U;

}  // namespace

bool RealtimePipelineAppliedWindowCapacityV1(
    const RealtimePipelineConfigV1& config,
    std::size_t* output) noexcept {
    return BoundedAppliedWindowCapacity(config, output);
}

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
        case RealtimePipelineCreateErrorV1::kKLineRuntimeCreateFailed:
            return "kline_runtime_create_failed";
        case RealtimePipelineCreateErrorV1::
            kLatestReadModelCreateFailed:
            return "latest_read_model_create_failed";
        case RealtimePipelineCreateErrorV1::kFactorCreateFailed:
            return "factor_create_failed";
        case RealtimePipelineCreateErrorV1::
            kProgressThreadStartFailed:
            return "progress_thread_start_failed";
        case RealtimePipelineCreateErrorV1::
            kProcessingThreadStartFailed:
            return "processing_thread_start_failed";
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
        case RealtimePipelineIngressErrorV1::
            kProcessingAdmissionFailed:
            return "processing_admission_failed";
        case RealtimePipelineIngressErrorV1::kStopped:
            return "stopped";
        case RealtimePipelineIngressErrorV1::kFatal:
            return "fatal";
        case RealtimePipelineIngressErrorV1::kFilteredNonAShare:
            return "filtered_non_a_share";
        case RealtimePipelineIngressErrorV1::kInstrumentKeyRejected:
            return "instrument_key_rejected";
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
        case RealtimePipelineCutErrorV1::kProcessingBarrierFailed:
            return "processing_barrier_failed";
        case RealtimePipelineCutErrorV1::kGenerationBeginFailed:
            return "generation_begin_failed";
        case RealtimePipelineCutErrorV1::kMarkerAdmissionFailed:
            return "marker_admission_failed";
        case RealtimePipelineCutErrorV1::kGenerationWaitFailed:
            return "generation_wait_failed";
        case RealtimePipelineCutErrorV1::
            kStoreGenerationPublishFailed:
            return "store_generation_publish_failed";
        case RealtimePipelineCutErrorV1::kFactorPublishFailed:
            return "factor_publish_failed";
        case RealtimePipelineCutErrorV1::kUnexpectedFailure:
            return "unexpected_failure";
    }
    return "unknown";
}

class RealtimePipelineV1::Impl final : public mdl::MessageHandlerBase {
public:
    struct CallbackClockObservation final {
        std::uint64_t realtime_ns = 0U;
        std::uint64_t monotonic_ns = 0U;
        bool valid = false;
    };

    enum class CommandKind : std::uint8_t {
        kMessage = 0U,
        kGenerationMarker,
    };

    struct DecoderCommand final {
        CommandKind kind = CommandKind::kMessage;
        realtime::OwnedIngressMessageHandleV1 message;
        market::ObservedInstrumentIdentityViewV2 identity{};
        std::uint64_t generation = 0U;
    };

    class ProcessingQueue final {
    public:
        explicit ProcessingQueue(std::size_t capacity)
            : slots_(capacity + 1U) {}

        ProcessingQueue(const ProcessingQueue&) = delete;
        ProcessingQueue& operator=(const ProcessingQueue&) = delete;

        template <typename Commit>
        [[nodiscard]] bool TryPush(
            realtime::OwnedIngressMessageHandleV1 message,
            Commit&& commit) noexcept {
            static_assert(std::is_nothrow_invocable_v<Commit&>);
            if (!message) {
                return false;
            }
            detail::CloseablePublicationGate::Lease publication =
                publication_gate_.TryAcquire();
            if (!publication) {
                return false;
            }
            const std::size_t tail =
                tail_.load(std::memory_order_relaxed);
            const std::size_t next = Increment(tail);
            if (next == head_.load(std::memory_order_acquire)) {
                return false;
            }
            slots_[tail] = std::move(message);
            // This is the admission linearization point. The slot is owned
            // and no later operation can fail, but the consumer cannot see it
            // until the release publication of tail below. The caller
            // therefore commits accepted/capture state before any matching
            // applied publication is possible.
            std::forward<Commit>(commit)();
            tail_.store(next, std::memory_order_release);
            WakeConsumer();
            return true;
        }

        [[nodiscard]] bool Pop(
            realtime::OwnedIngressMessageHandleV1* output) noexcept {
            if (output == nullptr) {
                return false;
            }
            output->reset();
            for (;;) {
                if (TryPop(output)) {
                    return true;
                }
                if (publication_gate_.closed_and_quiesced()) {
                    return false;
                }
                // This must be an RMW paired with WakeConsumer's exchange.
                // With a plain store, the producer may publish the sole
                // record and observe sleeping=false immediately before this
                // store, while this thread subsequently observes the old
                // tail and sleeps with no later producer to wake it. If the
                // producer's exchange wins first, this acquire imports its
                // preceding tail publication; if this exchange wins first,
                // the producer observes true and notifies.
                consumer_sleeping_.exchange(
                    true, std::memory_order_acq_rel);
                if (!Empty() ||
                    publication_gate_.closed_and_quiesced()) {
                    consumer_sleeping_.store(
                        false, std::memory_order_release);
                    continue;
                }
                consumer_sleeping_.wait(
                    true, std::memory_order_acquire);
            }
        }

        void RequestStop() noexcept {
            // A callback that already crossed TryAcquire() must publish its
            // tail before close returns. The callback itself never waits:
            // only the closing thread waits for that bounded memory section.
            publication_gate_.CloseAndWait();
            consumer_sleeping_.store(
                false, std::memory_order_release);
            consumer_sleeping_.notify_all();
        }

    private:
        [[nodiscard]] bool TryPop(
            realtime::OwnedIngressMessageHandleV1* output) noexcept {
            const std::size_t head =
                head_.load(std::memory_order_relaxed);
            const std::size_t tail =
                tail_.load(std::memory_order_acquire);
            if (head == tail) {
                return false;
            }
            *output = std::move(slots_[head]);
            slots_[head].reset();
            head_.store(Increment(head), std::memory_order_release);
            return static_cast<bool>(*output);
        }

        [[nodiscard]] bool Empty() const noexcept {
            return head_.load(std::memory_order_acquire) ==
                   tail_.load(std::memory_order_acquire);
        }

        void WakeConsumer() noexcept {
            if (!consumer_sleeping_.exchange(
                    false, std::memory_order_acq_rel)) {
                return;
            }
            consumer_sleeping_.notify_one();
        }

        [[nodiscard]] std::size_t Increment(
            std::size_t value) const noexcept {
            ++value;
            return value == slots_.size() ? 0U : value;
        }

        std::vector<realtime::OwnedIngressMessageHandleV1> slots_;
        alignas(64) std::atomic<std::size_t> head_{0U};
        alignas(64) std::atomic<std::size_t> tail_{0U};
        detail::CloseablePublicationGate publication_gate_;
        std::atomic<bool> consumer_sleeping_{false};
    };

    class DecoderQueue final {
    public:
        explicit DecoderQueue(std::size_t capacity)
            : slots_(capacity + 1U) {}

        DecoderQueue(const DecoderQueue&) = delete;
        DecoderQueue& operator=(const DecoderQueue&) = delete;

        [[nodiscard]] bool TryPush(DecoderCommand&& command) noexcept {
            detail::CloseablePublicationGate::Lease publication =
                publication_gate_.TryAcquire();
            if (!publication) {
                return false;
            }
            const std::size_t tail =
                tail_.load(std::memory_order_relaxed);
            const std::size_t next = Increment(tail);
            const std::size_t head =
                head_.load(std::memory_order_acquire);
            if (next == head) {
                return false;
            }
            slots_[tail].emplace(std::move(command));
            tail_.store(next, std::memory_order_release);
            WakeConsumer();
            return true;
        }

        [[nodiscard]] bool PushUntil(
            DecoderCommand command,
            std::chrono::steady_clock::time_point deadline) noexcept {
            try {
                while (true) {
                    if (TryPush(std::move(command))) {
                        return true;
                    }
                    if (publication_gate_.close_requested()) {
                        return false;
                    }
                    std::unique_lock<std::mutex> lock(wait_mutex_);
                    producer_waiting_.exchange(
                        true, std::memory_order_acq_rel);
                    if (!Full()) {
                        producer_waiting_.store(
                            false, std::memory_order_release);
                        continue;
                    }
                    const auto ready = [this] {
                        return publication_gate_.close_requested() ||
                               !producer_waiting_.load(
                                   std::memory_order_acquire);
                    };
                    if (!not_full_.wait_until(lock, deadline, ready)) {
                        producer_waiting_.store(
                            false, std::memory_order_release);
                        return false;
                    }
                    if (publication_gate_.close_requested()) {
                        return false;
                    }
                }
            } catch (...) {
                return false;
            }
        }

        [[nodiscard]] bool Pop(DecoderCommand* output) noexcept {
            if (output == nullptr) {
                return false;
            }
            try {
                while (true) {
                    if (TryPop(output)) {
                        return true;
                    }
                    if (publication_gate_.closed_and_quiesced()) {
                        return false;
                    }
                    std::unique_lock<std::mutex> lock(wait_mutex_);
                    consumer_sleeping_.exchange(
                        true, std::memory_order_acq_rel);
                    if (!Empty()) {
                        consumer_sleeping_.store(
                            false, std::memory_order_release);
                        continue;
                    }
                    not_empty_.wait(lock, [this] {
                        return publication_gate_
                                   .closed_and_quiesced() ||
                               !consumer_sleeping_.load(
                                   std::memory_order_acquire);
                    });
                }
            } catch (...) {
                return false;
            }
        }

        void RequestStop() noexcept {
            // Marker/message producers that already passed admission publish
            // before consumers are allowed to interpret closed+empty.
            publication_gate_.CloseAndWait();
            consumer_sleeping_.store(false, std::memory_order_release);
            producer_waiting_.store(false, std::memory_order_release);
            std::lock_guard<std::mutex> lock(wait_mutex_);
            not_empty_.notify_all();
            not_full_.notify_all();
        }

    private:
        [[nodiscard]] bool TryPop(DecoderCommand* output) noexcept {
            const std::size_t head =
                head_.load(std::memory_order_relaxed);
            const std::size_t tail =
                tail_.load(std::memory_order_acquire);
            if (head == tail) {
                return false;
            }
            *output = std::move(*slots_[head]);
            slots_[head].reset();
            head_.store(Increment(head), std::memory_order_release);
            WakeProducer();
            return true;
        }

        [[nodiscard]] bool Empty() const noexcept {
            return head_.load(std::memory_order_acquire) ==
                   tail_.load(std::memory_order_acquire);
        }

        [[nodiscard]] bool Full() const noexcept {
            const std::size_t tail =
                tail_.load(std::memory_order_acquire);
            return Increment(tail) ==
                   head_.load(std::memory_order_acquire);
        }

        void WakeConsumer() noexcept {
            // This RMW pairs with the waiter's exchange(true). A load-only
            // shortcut is not correct: the producer could read the old
            // sleeping=false while the waiter reads the old tail and then
            // sleeps forever. If this exchange wins first, the waiter's
            // acquire observes our prior tail publication; if the waiter
            // wins first, we observe true and notify under the wait mutex.
            if (!consumer_sleeping_.exchange(
                    false, std::memory_order_acq_rel)) {
                return;
            }
            std::lock_guard<std::mutex> lock(wait_mutex_);
            not_empty_.notify_one();
        }

        void WakeProducer() noexcept {
            // Symmetric full->non-full handshake for marker backpressure.
            if (!producer_waiting_.exchange(
                    false, std::memory_order_acq_rel)) {
                return;
            }
            std::lock_guard<std::mutex> lock(wait_mutex_);
            not_full_.notify_one();
        }

        [[nodiscard]] std::size_t Increment(std::size_t value) const noexcept {
            ++value;
            return value == slots_.size() ? 0U : value;
        }

        std::vector<std::optional<DecoderCommand>> slots_;
        alignas(64) std::atomic<std::size_t> head_{0U};
        alignas(64) std::atomic<std::size_t> tail_{0U};
        detail::CloseablePublicationGate publication_gate_;
        std::atomic<bool> consumer_sleeping_{false};
        std::atomic<bool> producer_waiting_{false};
        std::mutex wait_mutex_;
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

    class DispatchBarrierLease final {
    public:
        explicit DispatchBarrierLease(Impl* owner) noexcept
            : owner_(owner) {}

        DispatchBarrierLease(const DispatchBarrierLease&) = delete;
        DispatchBarrierLease& operator=(
            const DispatchBarrierLease&) = delete;

        ~DispatchBarrierLease() {
            if (armed_) {
                owner_->ReleaseDispatchBarrier();
            }
        }

        [[nodiscard]] bool Arm(std::uint64_t sequence) noexcept {
            if (armed_ || owner_ == nullptr ||
                !owner_->ArmDispatchBarrier(sequence)) {
                return false;
            }
            armed_ = true;
            return true;
        }

        void Release() noexcept {
            if (armed_) {
                owner_->ReleaseDispatchBarrier();
                armed_ = false;
            }
        }

    private:
        Impl* owner_ = nullptr;
        bool armed_ = false;
    };

    Impl(RealtimePipelineConfigV1 config,
         std::shared_ptr<sdk::SdkFactory> physical_factory)
        : config_(std::move(config)),
          sdk_factory_(std::move(physical_factory)) {}

    ~Impl() { StopAndDrain(); }

    static void ObserveAppend(
        void* context,
        const market::RealtimeHistoryAppendObservationV1& observation)
        noexcept {
        auto* const owner = static_cast<Impl*>(context);
        if (owner != nullptr && owner->latency_collector_ != nullptr) {
            owner->latency_collector_->RecordAppend(observation);
        }
    }

    static bool ObserveAppliedSequence(
        void* context,
        std::uint64_t ingress_sequence) noexcept {
        auto* const owner = static_cast<Impl*>(context);
        return owner != nullptr &&
               owner->CompleteAppliedSequence(ingress_sequence);
    }

    [[nodiscard]] RealtimePipelineCreateErrorV1 Initialize(
        bool factory_is_test_override,
        std::string* detail) noexcept {
        if (!ValidateConfiguration(factory_is_test_override)) {
            SetDetailLiteral(detail, "invalid realtime pipeline configuration");
            return RealtimePipelineCreateErrorV1::kInvalidConfiguration;
        }

        try {
            std::size_t applied_window = 0U;
            std::size_t maximum_inflight_messages = 0U;
            if (!BoundedAppliedWindowCapacity(
                    config_, &applied_window) ||
                !BoundedIngressPoolCapacity(
                    config_,
                    applied_window,
                    &maximum_inflight_messages)) {
                SetDetailLiteral(
                    detail, "invalid ingress pool capacity");
                return RealtimePipelineCreateErrorV1::
                    kInvalidConfiguration;
            }
            realtime::OwnedIngressMessagePoolConfigV1 pool_config{};
            pool_config.maximum_message_bytes =
                config_.maximum_sdk_message_bytes;
            pool_config.maximum_inflight_messages =
                maximum_inflight_messages;
            pool_config.prewarm_message_bytes = std::min(
                config_.maximum_sdk_message_bytes,
                kIngressPrewarmMessageBytes);
            // Reserve the complete bounded in-flight window before SDK
            // Connect. It covers the processing ring plus the independently
            // bounded decoder/applied completion window, preventing allocator
            // entry and first-touch faults in the callback as backlog grows. A
            // <=4096-byte wire message occupies at most the 8192-byte size
            // class; the byte cap keeps unusually large configured windows
            // from turning startup into an unbounded eager allocation.
            pool_config.prewarm_message_count = std::min(
                maximum_inflight_messages,
                kIngressPrewarmMaximumBlocks);
            const realtime::OwnedIngressMessageErrorV1 pool_error =
                realtime::OwnedIngressMessagePoolV1::Create(
                    pool_config, &ingress_pool_);
            if (pool_error !=
                    realtime::OwnedIngressMessageErrorV1::kNone ||
                ingress_pool_ == nullptr) {
                SetDetail(
                    detail,
                    "owned ingress pool create failed: " +
                        std::string(
                            realtime::OwnedIngressMessageErrorNameV1(
                                pool_error)));
                return pool_error ==
                               realtime::OwnedIngressMessageErrorV1::
                                   kInvalidPoolConfiguration
                           ? RealtimePipelineCreateErrorV1::
                                 kInvalidConfiguration
                           : RealtimePipelineCreateErrorV1::
                                 kResourceExhausted;
            }

            if (config_.measure_stage_latency) {
                latency_collector_ =
                    std::make_unique<StageLatencyCollector>(
                        config_.trade_date);
            }

            if (realtime::ContiguousSequenceTrackerV2::Create(
                    applied_window,
                    &applied_tracker_) !=
                    realtime::ContiguousSequenceTrackerCreateErrorV2::
                        kNone ||
                applied_tracker_ == nullptr) {
                SetDetailLiteral(
                    detail, "applied sequence tracker create failed");
                return RealtimePipelineCreateErrorV1::
                    kResourceExhausted;
            }
            applied_window_capacity_ = applied_window;
            processing_queue_ = std::make_unique<ProcessingQueue>(
                config_.processing_queue_capacity);

            market::RealtimeHistoryRuntimeConfigV1 history_config{};
            history_config.source_stream_ids = config_.source_stream_ids;
            history_config.worker_count = config_.store_worker_count;
            history_config.queue_capacity_per_source_worker =
                config_.store_queue_capacity_per_source_worker;
            history_config.intraday_store = config_.intraday_store;
            history_config.kline = config_.kline;
            history_config.kline.trade_date = config_.trade_date;
            history_config.directory = config_.directory;
            history_config.applied_record_sink =
                config_.applied_record_sink;
            history_config.applied_observer =
                &ObserveAppliedSequence;
            history_config.applied_observer_context = this;
            if (latency_collector_ != nullptr) {
                history_config.append_observer = &ObserveAppend;
                history_config.append_observer_context = this;
            }
            const market::RealtimeHistoryCreateErrorV1
                store_runtime_error =
                market::RealtimeHistoryRuntimeV1::Create(
                    history_config, &history_);
            if (store_runtime_error !=
                market::RealtimeHistoryCreateErrorV1::kNone) {
                SetDetail(
                    detail,
                    "history runtime create failed: " +
                        std::string(
                            market::RealtimeHistoryCreateErrorNameV1(
                                store_runtime_error)));
                if (store_runtime_error ==
                    market::RealtimeHistoryCreateErrorV1::
                        kKLineCreateFailed) {
                    return RealtimePipelineCreateErrorV1::
                        kKLineRuntimeCreateFailed;
                }
                if (store_runtime_error ==
                    market::RealtimeHistoryCreateErrorV1::
                        kLatestReadModelCreateFailed) {
                    return RealtimePipelineCreateErrorV1::
                        kLatestReadModelCreateFailed;
                }
                return RealtimePipelineCreateErrorV1::
                    kStoreRuntimeCreateFailed;
            }

            if (config_.factor_calculator == nullptr) {
                config_.factor_calculator =
                    std::make_shared<factor::SnapshotLastPriceProjectionV1>();
            }
            factor::RealtimeFactorEngineConfigV1 factor_config{};
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
            if (!StartProcessingThread()) {
                SetDetailLiteral(
                    detail,
                    "cannot start ordered processing dispatcher");
                return RealtimePipelineCreateErrorV1::
                    kProcessingThreadStartFailed;
            }
            if (!StartProgressThread()) {
                SetDetailLiteral(
                    detail,
                    "cannot start asynchronous progress publisher");
                return RealtimePipelineCreateErrorV1::
                    kProgressThreadStartFailed;
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
        CallbackClockObservation callback_entry{};
        const CallbackClockObservation* callback_entry_pointer = nullptr;
        if (latency_collector_ != nullptr) {
            callback_entry.valid =
                ReadClockNs(
                    CLOCK_REALTIME, &callback_entry.realtime_ns) &&
                ReadClockNs(
                    CLOCK_MONOTONIC, &callback_entry.monotonic_ns);
            callback_entry_pointer = &callback_entry;
        }
        RealtimePipelineIngressErrorV1 callback_error =
            RealtimePipelineIngressErrorV1::kStopped;
        if (!callback_gate_closed_.load(std::memory_order_acquire)) {
            callback_error =
                Ingest(message, callback_entry_pointer).error;
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
        const mdl::MDLMessage* message,
        const CallbackClockObservation* callback_entry = nullptr) noexcept {
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
            ++post_cut_messages_;
            return result;
        }
        if (message == nullptr) {
            result.error = RealtimePipelineIngressErrorV1::kNullMessage;
            result.owned_error =
                realtime::OwnedIngressMessageErrorV1::kNullMessage;
            ++rejected_messages_;
            TripFatalWithAdmissionLockHeld();
            return result;
        }

        realtime::OwnedIngressMessageInspectionV1 inspection{};
        result.owned_error = realtime::InspectOwnedIngressMessageV1(
            message, config_.maximum_sdk_message_bytes, &inspection);
        if (result.owned_error ==
            realtime::OwnedIngressMessageErrorV1::
                kForbiddenCombinedTick) {
            result.error =
                RealtimePipelineIngressErrorV1::kForbiddenCombinedTick;
            ++rejected_messages_;
            TripFatalWithAdmissionLockHeld();
            return result;
        }
        if (result.owned_error ==
            realtime::OwnedIngressMessageErrorV1::kUnsupportedMessage) {
            result.error =
                RealtimePipelineIngressErrorV1::kIgnoredUnsupported;
            ++ignored_messages_;
            return result;
        }
        if (result.owned_error !=
                realtime::OwnedIngressMessageErrorV1::kNone ||
            !inspection) {
            result.error =
                RealtimePipelineIngressErrorV1::kOwnedMessageRejected;
            ++rejected_messages_;
            TripFatalWithAdmissionLockHeld();
            return result;
        }

        const std::uint8_t source_slot = inspection.source_slot();
        result.source_slot = source_slot;
        result.vendor_local_time_raw =
            inspection.vendor_head().local_time_raw();
        const bool mixed_tick_source =
            IsMixedTickSourceSlot(source_slot);

        std::uint64_t realtime_ns = 0U;
        std::uint64_t monotonic_ns = 0U;
        bool callback_entry_clock_valid = false;
        if (callback_entry != nullptr) {
            realtime_ns = callback_entry->realtime_ns;
            monotonic_ns = callback_entry->monotonic_ns;
            callback_entry_clock_valid = callback_entry->valid;
        } else {
            callback_entry_clock_valid =
                ReadClockNs(CLOCK_REALTIME, &realtime_ns) &&
                ReadClockNs(CLOCK_MONOTONIC, &monotonic_ns);
        }
        if (!callback_entry_clock_valid) {
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

        if (config_.enable_mainland_a_share_filter) {
            market::MarketMessageViewV1 filter_view{};
            filter_view.service_id = inspection.key().service_id;
            filter_view.service_version =
                inspection.key().service_version;
            filter_view.message_id = inspection.key().message_id;
            filter_view.body = inspection.body();

            market::ObservedInstrumentKeyViewV2 extracted{};
            const market::MarketDecodeErrorV1 extraction_error =
                market::ExtractObservedInstrumentKeyV2(
                    filter_view,
                    config_.decoder_limits.maximum_text_bytes,
                    &extracted);
            if (extraction_error !=
                market::MarketDecodeErrorV1::kNone) {
                last_decode_error_.store(
                    static_cast<std::uint8_t>(extraction_error),
                    std::memory_order_release);
                result.error =
                    RealtimePipelineIngressErrorV1::
                        kInstrumentKeyRejected;
                ReportPipelineFailure(
                    "admission_instrument_key_extract",
                    source_slot,
                    0U,
                    static_cast<std::uint64_t>(extraction_error));
                ++rejected_messages_;
                TripFatalWithAdmissionLockHeld();
                return result;
            }
            if (!market::IsMainlandAShareSecurityIdV1(
                    MainlandExchangeForMarket(extracted.market),
                    extracted.security_id)) {
                result.error =
                    RealtimePipelineIngressErrorV1::
                        kFilteredNonAShare;
                ++filtered_messages_;
                ++filtered_messages_by_source_[source_slot];
                return result;
            }
        }

        constexpr std::uint64_t exhaustion_sentinel =
            std::numeric_limits<std::uint64_t>::max();
        // UINT64_MAX is a valid exclusive cut but is never assigned to a
        // message. Filtered callbacks never consume a sequence, so this check
        // intentionally follows the filter.
        if (global_ingress_sequence_ >= exhaustion_sentinel - 1U ||
            source_sequences_[source_slot] >= exhaustion_sentinel - 1U ||
            (mixed_tick_source &&
             tick_stream_sequence_ >= exhaustion_sentinel - 1U)) {
            result.error =
                RealtimePipelineIngressErrorV1::kSequenceExhausted;
            ++rejected_messages_;
            TripFatalWithAdmissionLockHeld();
            return result;
        }

        realtime::OwnedIngressMetadataV1 metadata{};
        metadata.run_id = config_.run_id;
        metadata.global_ingress_sequence = global_ingress_sequence_ + 1U;
        metadata.source_sequence = source_sequences_[source_slot] + 1U;
        metadata.recv_realtime_ns = realtime_ns;
        metadata.recv_monotonic_ns = monotonic_ns;
        metadata.tick_stream_sequence =
            mixed_tick_source ? tick_stream_sequence_ + 1U : 0U;

        realtime::OwnedIngressMessageHandleV1 owned;
        result.owned_error =
            ingress_pool_->Acquire(inspection, metadata, &owned);
        if (result.owned_error !=
                realtime::OwnedIngressMessageErrorV1::kNone ||
            !owned || owned->source_slot() != source_slot) {
            result.error =
                RealtimePipelineIngressErrorV1::kOwnedMessageRejected;
            ++rejected_messages_;
            TripFatalWithAdmissionLockHeld();
            return result;
        }

        // The payload is copied exactly once into the pool and its sole
        // callback-owned handle is transferred to the ordered processing
        // ring. The commit runs after an empty slot is secured but before the
        // ring tail is release-published. It is therefore the irreversible
        // admission point, and no consumer can apply this sequence before the
        // accepted frontier below becomes visible.
        if (processing_queue_ == nullptr ||
            !processing_queue_->TryPush(
                std::move(owned),
                [this,
                 &result,
                 &metadata,
                 source_slot,
                 mixed_tick_source]() noexcept {
                    global_ingress_sequence_ =
                        metadata.global_ingress_sequence;
                    source_sequences_[source_slot] =
                        metadata.source_sequence;
                    if (mixed_tick_source) {
                        tick_stream_sequence_ =
                            metadata.tick_stream_sequence;
                    }
                    result.global_ingress_sequence =
                        metadata.global_ingress_sequence;
                    result.source_sequence = metadata.source_sequence;
                    result.tick_stream_sequence =
                        metadata.tick_stream_sequence;
                    ++accepted_messages_;
                    accepted_sequence_.store(
                        metadata.global_ingress_sequence,
                        std::memory_order_release);
                })) {
            result.error =
                RealtimePipelineIngressErrorV1::
                    kProcessingAdmissionFailed;
            ReportPipelineFailure(
                "processing_queue_admission",
                source_slot,
                metadata.global_ingress_sequence,
                config_.processing_queue_capacity);
            ++rejected_messages_;
            TripFatalWithAdmissionLockHeld();
            return result;
        }
        RequestProgressPublication();

        // Successful admission is complete only after the serialized callback
        // authority has been released.  The completion clocks below therefore
        // include the owned copy, the ordered queue commit, and the admission
        // critical section.
        admission.unlock();
        if (latency_collector_ != nullptr) {
            std::uint64_t success_monotonic_ns = 0U;
            std::uint64_t success_realtime_ns = 0U;
            const bool success_clock_valid =
                ReadClockNs(
                    CLOCK_MONOTONIC, &success_monotonic_ns) &&
                ReadClockNs(
                    CLOCK_REALTIME, &success_realtime_ns);
            latency_collector_->RecordCallback(
                result,
                monotonic_ns,
                success_realtime_ns,
                success_monotonic_ns,
                callback_entry_clock_valid && success_clock_valid);
        }
        return result;
    }

    [[nodiscard]] RealtimePipelineCutResultV1 Cut(
        std::chrono::nanoseconds timeout) noexcept {
        RealtimePipelineCutResultV1 result{};
        result.kline_enabled = config_.kline.enabled();
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
        result.kline_enabled = config_.kline.enabled();
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

            // Shutdown is the callback-quiescence boundary. Every accepted
            // callback already owns an ordered in-memory processing entry;
            // later callbacks are outside the accepted prefix.
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
        result.kline_enabled = config_.kline.enabled();
        if (timeout <= std::chrono::nanoseconds::zero() ||
            timeout > kMaximumCutTimeout) {
            result.error = RealtimePipelineCutErrorV1::kInvalidTimeout;
            return result;
        }

        try {
            const auto deadline = std::chrono::steady_clock::now() + timeout;
            std::uint64_t generation = 0U;
            std::uint64_t cut_sequence = 0U;
            std::uint64_t monotonic_cut_ns = 0U;
            std::array<market::RealtimeSourceWatermarkV1,
                       market::kRealtimeHistorySourceCountV1>
                sources{};
            DispatchBarrierLease dispatch_barrier(this);
            {
                std::unique_lock<std::mutex> admission(admission_mutex_);
                const bool history_fatal = history_->fatal();
                if (fatal_.load(std::memory_order_acquire) ||
                    history_fatal) {
                    if (history_fatal) {
                        result.generation_error =
                            history_->FailureError();
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

                cut_sequence = global_ingress_sequence_;
                monotonic_cut_ns =
                    preclosed_monotonic_cut_ns.value_or(0U);
                if (!preclosed_monotonic_cut_ns.has_value() &&
                    !ReadClockNs(CLOCK_MONOTONIC, &monotonic_cut_ns)) {
                    result.error =
                        RealtimePipelineCutErrorV1::kClockFailure;
                    TripFatalWithAdmissionLockHeld();
                    return result;
                }
                generation = last_started_generation_ + 1U;
                for (std::size_t source = 0U;
                     source < sources.size();
                     ++source) {
                    sources[source].source_stream_id =
                        config_.source_stream_ids[source];
                    sources[source].sequence_exclusive =
                        source_sequences_[source] + 1U;
                }
                // The barrier is armed while callback admission is
                // serialized, so no sequence above this exact cut can reach
                // Directory/Decoder/Store. Callbacks resume immediately and
                // continue entering both asynchronous queues while cut work
                // runs.
                if (!dispatch_barrier.Arm(cut_sequence)) {
                    result.error = RealtimePipelineCutErrorV1::
                        kProcessingBarrierFailed;
                    TripFatalWithAdmissionLockHeld();
                    return result;
                }
            }

            if (cut_sequence != 0U &&
                !WaitAppliedThrough(cut_sequence, deadline)) {
                result.error = RealtimePipelineCutErrorV1::
                    kProcessingBarrierFailed;
                TripFatal();
                return result;
            }

            std::shared_ptr<const
                market::ObservedInstrumentCatalogSnapshotV2>
                catalog_snapshot;
            if (config_.directory->AcquireSnapshot(
                    &catalog_snapshot) !=
                    market::ObservedInstrumentDirectoryErrorV2::kNone ||
                catalog_snapshot == nullptr) {
                result.error =
                    RealtimePipelineCutErrorV1::kWatermarkFailed;
                TripFatal();
                return result;
            }
            // This immutable generation describes the exact accepted/applied
            // prefix isolated by the processing barrier.
            const realtime::ProcessingProgressV2 progress{
                cut_sequence,
                cut_sequence};
            market::RealtimeHistoryWatermarkV1 watermark{};
            result.watermark_error =
                market::BuildRealtimeHistoryWatermarkV1(
                    config_.run_id,
                    generation,
                    config_.trade_date,
                    cut_sequence + 1U,
                    monotonic_cut_ns,
                    std::move(catalog_snapshot),
                    progress,
                    sources,
                    &watermark);
            if (result.watermark_error !=
                market::RealtimeHistoryWatermarkErrorV1::kNone) {
                result.error =
                    RealtimePipelineCutErrorV1::kWatermarkFailed;
                TripFatal();
                return result;
            }

            result.generation_error =
                history_->BeginGeneration(watermark);
            if (result.generation_error !=
                market::RealtimeHistoryGenerationErrorV1::kNone) {
                result.error = RealtimePipelineCutErrorV1::
                    kGenerationBeginFailed;
                TripFatal();
                return result;
            }
            {
                std::lock_guard<std::mutex> admission(
                    admission_mutex_);
                if (last_started_generation_ ==
                        std::numeric_limits<std::uint64_t>::max() ||
                    last_started_generation_ + 1U != generation) {
                    result.error = RealtimePipelineCutErrorV1::
                        kGenerationBeginFailed;
                    TripFatalWithAdmissionLockHeld();
                    return result;
                }
                last_started_generation_ = generation;
            }

            // Ordered in-memory processing is still paused immediately after
            // the cut. Each source marker therefore follows all cut records
            // and precedes every post-cut record without blocking SDK
            // callbacks.
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
                    TripFatal();
                    return result;
                }
            }
            dispatch_barrier.Release();

            result.generation_error = history_->WaitForGeneration(
                generation,
                Remaining(deadline),
                &result.store_generation,
                &result.kline_generation);
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
            if (result.kline_enabled &&
                result.kline_generation == nullptr) {
                result.generation_error =
                    market::RealtimeHistoryGenerationErrorV1::
                        kKLineFailed;
                result.error =
                    RealtimePipelineCutErrorV1::
                        kGenerationWaitFailed;
                TripFatal();
                return result;
            }

            if (config_.store_generation_sink != nullptr &&
                !config_.store_generation_sink->PublishStoreGeneration(
                    result.store_generation)) {
                result.error = RealtimePipelineCutErrorV1::
                    kStoreGenerationPublishFailed;
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
        const market::RealtimeKLineGenerationV1>
    AcquireKLine() const noexcept {
        return history_ == nullptr
                   ? nullptr
                   : history_->AcquireLatestKLineGeneration();
    }

    [[nodiscard]] market::RealtimeLatestQueryErrorV1
    GetLatestSnapshot(
        std::uint32_t instrument_id,
        market::RealtimeLatestRecordViewV1* output) const noexcept {
        if (history_ == nullptr) {
            return market::RealtimeLatestQueryErrorV1::kUnavailable;
        }
        return history_->GetLatestSnapshot(instrument_id, output);
    }

    [[nodiscard]] market::RealtimeLatestQueryErrorV1
    GetLatestSnapshots(
        std::span<const std::uint32_t> instrument_ids,
        std::span<market::RealtimeLatestRecordViewV1> output)
        const noexcept {
        if (history_ == nullptr) {
            return market::RealtimeLatestQueryErrorV1::kUnavailable;
        }
        return history_->GetLatestSnapshots(instrument_ids, output);
    }

    [[nodiscard]] market::RealtimeLatestQueryErrorV1 GetLatestTick(
        std::uint32_t instrument_id,
        market::RealtimeLatestRecordViewV1* output) const noexcept {
        if (history_ == nullptr) {
            return market::RealtimeLatestQueryErrorV1::kUnavailable;
        }
        return history_->GetLatestTick(instrument_id, output);
    }

    [[nodiscard]] market::RealtimeLatestQueryErrorV1 GetLatestTicks(
        std::span<const std::uint32_t> instrument_ids,
        std::span<market::RealtimeLatestRecordViewV1> output)
        const noexcept {
        if (history_ == nullptr) {
            return market::RealtimeLatestQueryErrorV1::kUnavailable;
        }
        return history_->GetLatestTicks(instrument_ids, output);
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
            result.filtered_messages = filtered_messages_;
            result.filtered_messages_by_source =
                filtered_messages_by_source_;
            result.ignored_messages = ignored_messages_;
            result.post_cut_messages = post_cut_messages_;
            result.rejected_messages = rejected_messages_;
            result.global_ingress_sequence = global_ingress_sequence_;
            result.tick_stream_sequence = tick_stream_sequence_;
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
        if (ingress_pool_ != nullptr) {
            result.ingress_pool = ingress_pool_->Snapshot();
        }
        result.processing_progress.applied_sequence =
            applied_sequence_.load(std::memory_order_acquire);
        // Read downstream publications first. Their release chains originate
        // after accepted publication, so the final accepted acquire cannot
        // produce a torn downstream>accepted tuple.
        result.processing_progress.accepted_sequence =
            accepted_sequence_.load(std::memory_order_acquire);
        if (history_ != nullptr) {
            result.store = history_->StoreSnapshot();
        }
        result.accepting = accepting_.load(std::memory_order_acquire);
        result.fatal = fatal_.load(std::memory_order_acquire) ||
                       (history_ != nullptr && history_->fatal());
        result.stopped = stopped_.load(std::memory_order_acquire);
        result.trade_date_boundary_reached =
            trade_date_boundary_reached_.load(std::memory_order_acquire);
        result.mainland_a_share_filter_enabled =
            config_.enable_mainland_a_share_filter;
        return result;
    }

    [[nodiscard]] RealtimePipelineStageLatencySnapshotV1 LatencySnapshot()
        const noexcept {
        return latency_collector_ == nullptr
                   ? RealtimePipelineStageLatencySnapshotV1{}
                   : latency_collector_->Snapshot();
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
        RequestProcessingStop();
        JoinProcessingThread();
        RequestDecoderStop();
        JoinDecoderThreads();
        if (history_ != nullptr) {
            history_->StopAndDrain();
        }
        RequestProgressStop();
        JoinProgressThread();
        stopped_.store(true, std::memory_order_release);
    }

    [[nodiscard]] bool RouteProcessingMessage(
        realtime::OwnedIngressMessageHandleV1 message) noexcept {
        if (!message ||
            fatal_.load(std::memory_order_acquire) ||
            config_.directory == nullptr) {
            MarkAsyncFatal();
            return false;
        }
        const std::uint8_t source = message->source_slot();
        if (source >= market::kRealtimeHistorySourceCountV1 ||
            lanes_[source] == nullptr ||
            message->recv_realtime_ns() >
                static_cast<std::uint64_t>(
                    std::numeric_limits<std::int64_t>::max()) ||
            message->recv_monotonic_ns() >
                static_cast<std::uint64_t>(
                    std::numeric_limits<std::int64_t>::max())) {
            MarkAsyncFatal();
            return false;
        }
        if (!WaitForDispatchBarrier(
                message->global_ingress_sequence())) {
            ReportPipelineFailure(
                "generation_dispatch_barrier",
                source,
                message->global_ingress_sequence(),
                dispatch_barrier_sequence_.load(
                    std::memory_order_acquire));
            MarkAsyncFatal();
            return false;
        }
        if (!WaitForAppliedDispatchWindow(
                message->global_ingress_sequence())) {
            ReportPipelineFailure(
                "applied_dispatch_window",
                source,
                message->global_ingress_sequence(),
                applied_window_capacity_);
            MarkAsyncFatal();
            return false;
        }

        const sdk::VendorHeadView head = message->vendor_head();
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

        market::ObservedInstrumentKeyViewV2 extracted{};
        const market::MarketDecodeErrorV1 extraction_error =
            market::ExtractObservedInstrumentKeyV2(
                view,
                config_.decoder_limits.maximum_text_bytes,
                &extracted);
        if (extraction_error != market::MarketDecodeErrorV1::kNone) {
            last_decode_error_.store(
                static_cast<std::uint8_t>(extraction_error),
                std::memory_order_release);
            ReportPipelineFailure(
                "instrument_key_extract",
                source,
                message->global_ingress_sequence(),
                static_cast<std::uint64_t>(extraction_error));
            MarkAsyncFatal();
            return false;
        }

        market::InstrumentKeyViewV1 key{
            extracted.market,
            extracted.security_id_source,
            extracted.security_id};
        market::ObservedInstrumentBindResultV2 binding{};
        const market::ObservedInstrumentDirectoryErrorV2 bind_error =
            config_.directory->BindOrGet(
                key,
                market::ObservedInstrumentMetadataV2{},
                message->global_ingress_sequence(),
                &binding);
        if (bind_error !=
                market::ObservedInstrumentDirectoryErrorV2::kNone ||
            !binding.entry.bound()) {
            ReportPipelineFailure(
                "instrument_bind",
                source,
                message->global_ingress_sequence(),
                static_cast<std::uint64_t>(bind_error));
            MarkAsyncFatal();
            return false;
        }
        if (binding.newly_bound &&
            config_.instrument_binding_sink != nullptr &&
            !config_.instrument_binding_sink
                 ->PublishObservedInstrumentBinding(binding)) {
            ReportPipelineFailure(
                "instrument_binding_projection",
                source,
                message->global_ingress_sequence(),
                binding.entry.instrument_id);
            MarkAsyncFatal();
            return false;
        }

        DecoderCommand command{};
        command.kind = CommandKind::kMessage;
        command.message = std::move(message);
        command.identity.key.market = binding.entry.key.market;
        command.identity.key.security_id_source =
            binding.entry.key.security_id_source;
        command.identity.key.security_id =
            binding.entry.key.security_id;
        command.identity.instrument_id =
            binding.entry.instrument_id;
        command.identity.ordinal = binding.entry.ordinal;
        command.identity.quantity_unit =
            binding.entry.metadata.quantity_unit;
        command.identity.security_type =
            binding.entry.metadata.security_type;
        command.identity.asset_scope =
            binding.entry.metadata.asset_scope;
        if (!lanes_[source]->queue.PushUntil(
                std::move(command),
                std::chrono::steady_clock::time_point::max())) {
            MarkAsyncFatal();
            return false;
        }
        return true;
    }

    [[nodiscard]] bool CompleteAppliedSequence(
        std::uint64_t ingress_sequence) noexcept {
        const std::uint64_t accepted =
            accepted_sequence_.load(std::memory_order_acquire);
        if (ingress_sequence == 0U || ingress_sequence > accepted ||
            applied_tracker_ == nullptr ||
            applied_tracker_->MarkCompleted(ingress_sequence) !=
                realtime::ContiguousSequenceMarkErrorV2::kNone) {
            MarkAsyncFatal();
            return false;
        }
        const std::uint64_t completed =
            applied_tracker_->contiguous_sequence();
        if (completed > accepted) {
            MarkAsyncFatal();
            return false;
        }
        std::uint64_t published =
            applied_sequence_.load(std::memory_order_acquire);
        while (published < completed &&
               !applied_sequence_.compare_exchange_weak(
                   published,
                   completed,
                   std::memory_order_release,
                   std::memory_order_acquire)) {
        }
        applied_progress_cv_.notify_all();
        RequestProgressPublication();
        return !fatal_.load(std::memory_order_acquire);
    }

    [[nodiscard]] bool ArmDispatchBarrier(
        std::uint64_t sequence) noexcept {
        try {
            std::lock_guard<std::mutex> lock(
                dispatch_barrier_mutex_);
            if (dispatch_barrier_active_.load(
                    std::memory_order_acquire)) {
                return false;
            }
            dispatch_barrier_sequence_.store(
                sequence, std::memory_order_relaxed);
            dispatch_barrier_active_.store(
                true, std::memory_order_release);
            return true;
        } catch (...) {
            return false;
        }
    }

    void ReleaseDispatchBarrier() noexcept {
        dispatch_barrier_active_.store(
            false, std::memory_order_release);
        dispatch_barrier_cv_.notify_all();
    }

    [[nodiscard]] bool WaitForDispatchBarrier(
        std::uint64_t sequence) noexcept {
        if (!dispatch_barrier_active_.load(
                std::memory_order_acquire) ||
            sequence <= dispatch_barrier_sequence_.load(
                            std::memory_order_acquire)) {
            return true;
        }
        try {
            std::unique_lock<std::mutex> lock(
                dispatch_barrier_mutex_);
            dispatch_barrier_cv_.wait(lock, [this, sequence] {
                return fatal_.load(std::memory_order_acquire) ||
                       !dispatch_barrier_active_.load(
                           std::memory_order_acquire) ||
                       sequence <=
                           dispatch_barrier_sequence_.load(
                               std::memory_order_acquire);
            });
            return !fatal_.load(std::memory_order_acquire) &&
                   (!dispatch_barrier_active_.load(
                        std::memory_order_acquire) ||
                    sequence <= dispatch_barrier_sequence_.load(
                                    std::memory_order_acquire));
        } catch (...) {
            return false;
        }
    }

    [[nodiscard]] bool WaitForAppliedDispatchWindow(
        std::uint64_t sequence) noexcept {
        if (sequence == 0U || applied_window_capacity_ == 0U) {
            return false;
        }
        // Normal traffic stays lock-free. Only a real completion-window
        // boundary enters the condition-variable path.
        const std::uint64_t fast_applied =
            applied_sequence_.load(std::memory_order_acquire);
        if (!fatal_.load(std::memory_order_acquire) &&
            sequence > fast_applied &&
            sequence - fast_applied <=
                static_cast<std::uint64_t>(
                    applied_window_capacity_)) {
            return true;
        }
        try {
            std::unique_lock<std::mutex> lock(applied_progress_mutex_);
            for (;;) {
                const bool history_fatal =
                    history_ != nullptr && history_->fatal();
                if (fatal_.load(std::memory_order_acquire) ||
                    history_fatal) {
                    lock.unlock();
                    if (history_fatal) {
                        MarkAsyncFatal();
                    }
                    return false;
                }
                const std::uint64_t applied =
                    applied_sequence_.load(std::memory_order_acquire);
                if (sequence <= applied) {
                    return false;
                }
                if (sequence - applied <=
                    static_cast<std::uint64_t>(
                        applied_window_capacity_)) {
                    return true;
                }
                // Completion normally notifies immediately. The bounded wait
                // also observes a History fatal that cannot itself call the
                // applied observer, preventing dispatcher shutdown deadlock.
                static_cast<void>(applied_progress_cv_.wait_for(
                    lock, std::chrono::milliseconds(1)));
            }
        } catch (...) {
            return false;
        }
    }

    void RequestProgressPublication() noexcept {
        if (config_.processing_progress_sink != nullptr &&
            !progress_wake_pending_.exchange(
                true, std::memory_order_acq_rel)) {
            progress_wake_.release();
        }
    }

    [[nodiscard]] bool PublishProcessingProgressNow() noexcept {
        if (config_.processing_progress_sink == nullptr) {
            return true;
        }
        realtime::ProcessingProgressV2 progress{};
        progress.applied_sequence =
            applied_sequence_.load(std::memory_order_acquire);
        // The downstream release chain begins after accepted publication.
        // Reading accepted last prevents a concurrent completion from
        // creating a transient downstream>accepted sample.
        progress.accepted_sequence =
            accepted_sequence_.load(std::memory_order_acquire);
        if (!progress.valid() ||
            !config_.processing_progress_sink
                 ->PublishProcessingProgress(progress)) {
            MarkAsyncFatal();
            return false;
        }
        return true;
    }

    void MarkAsyncFatal() noexcept {
        accepting_.store(false, std::memory_order_release);
        fatal_.store(true, std::memory_order_release);
        if (history_ != nullptr) {
            history_->MarkFatal();
        }
        RequestProcessingStop();
        RequestDecoderStop();
        RequestProgressStop();
        applied_progress_cv_.notify_all();
        dispatch_barrier_cv_.notify_all();
    }

    [[nodiscard]] bool WaitAppliedThrough(
        std::uint64_t target_sequence,
        std::chrono::steady_clock::time_point deadline) noexcept {
        if (applied_sequence_.load(std::memory_order_acquire) >=
            target_sequence) {
            return true;
        }
        try {
            std::unique_lock<std::mutex> lock(applied_progress_mutex_);
            const auto ready = [this, target_sequence] {
                return applied_sequence_.load(
                           std::memory_order_acquire) >= target_sequence ||
                       fatal_.load(std::memory_order_acquire);
            };
            return (ready() ||
                    applied_progress_cv_.wait_until(
                        lock, deadline, ready)) &&
                   applied_sequence_.load(std::memory_order_acquire) >=
                       target_sequence;
        } catch (...) {
            return false;
        }
    }

    [[nodiscard]] bool ValidateConfiguration(
        bool factory_is_test_override) const noexcept {
        std::size_t applied_window = 0U;
        std::size_t maximum_inflight_messages = 0U;
        if (config_.directory == nullptr ||
            config_.directory->capacity() == 0U ||
            config_.directory->session_epoch() == 0U ||
            l2flow::common::IsZeroIdentity(config_.run_id) ||
            config_.trade_date == 0U ||
            config_.maximum_sdk_message_bytes < sdk::kVendorHeadBytes ||
            config_.maximum_sdk_message_bytes >
                realtime::kOwnedIngressMaximumMessageBytesV1 ||
            config_.processing_queue_capacity == 0U ||
            config_.processing_queue_capacity ==
                std::numeric_limits<std::size_t>::max() ||
            config_.decoder_queue_capacity_per_source == 0U ||
            config_.decoder_queue_capacity_per_source ==
                std::numeric_limits<std::size_t>::max() ||
            config_.store_worker_count == 0U ||
            config_.store_queue_capacity_per_source_worker == 0U ||
            config_.intraday_store.segment_target_bytes <
                market::kIntradayInstrumentStoreMinimumSegmentBytesV1 ||
            config_.intraday_store.segment_target_bytes >
                market::kIntradayInstrumentStoreMaximumSegmentBytesV1 ||
            config_.intraday_store.maximum_session_records == 0U ||
            config_.intraday_store.maximum_session_accounted_bytes == 0U ||
            config_.intraday_store.maximum_records_per_batch == 0U ||
            config_.intraday_store.maximum_records_per_batch >
                market::kIntradayInstrumentStoreMaximumBatchRecordsV1 ||
            !BoundedAppliedWindowCapacity(
                config_, &applied_window) ||
            !BoundedIngressPoolCapacity(
                config_,
                applied_window,
                &maximum_inflight_messages)) {
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
               config_.sdk.io_threads == 1 &&
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

    [[nodiscard]] bool StartProcessingThread() noexcept {
        if (processing_queue_ == nullptr ||
            processing_thread_.joinable()) {
            return false;
        }
        try {
            processing_thread_ =
                std::thread([this] { ProcessingLoop(); });
            processing_thread_started_ = true;
            return true;
        } catch (...) {
            return false;
        }
    }

    [[nodiscard]] bool StartProgressThread() noexcept {
        if (config_.processing_progress_sink == nullptr) {
            return true;
        }
        if (progress_thread_.joinable()) {
            return false;
        }
        try {
            progress_thread_ =
                std::thread([this] { ProgressLoop(); });
            return true;
        } catch (...) {
            return false;
        }
    }

    void ProcessingLoop() noexcept {
        realtime::OwnedIngressMessageHandleV1 message;
        while (processing_queue_ != nullptr &&
               processing_queue_->Pop(&message)) {
            if (!fatal_.load(std::memory_order_acquire) &&
                !RouteProcessingMessage(std::move(message))) {
                MarkAsyncFatal();
            }
            message.reset();
        }
    }

    void ProgressLoop() noexcept {
        realtime::ProcessingProgressV2 last_published{};
        for (;;) {
            const auto publication_deadline =
                std::chrono::steady_clock::now() +
                kProgressPublishMaximumDelay;
            const bool woke =
                progress_wake_.try_acquire_until(
                    publication_deadline);
            if (woke) {
                // Keep wake_pending set through the rest of this interval.
                // accepted/applied can advance on every record, but
                // the IPC status tuple needs only a bounded-delay aggregate.
                // This prevents the progress writer from repeatedly
                // contending with early-session binding and first-availability
                // publication while still bounding status lag to one
                // interval.
                std::this_thread::sleep_until(
                    publication_deadline);
                // Clear only after consuming the matching token. On a
                // timeout a producer may already have changed pending from
                // false to true but not yet released the semaphore; clearing
                // in that race could let a second producer release into an
                // already-full binary semaphore.
                progress_wake_pending_.store(
                    false, std::memory_order_release);
            }
            // A stop request is published only after every normal progress
            // producer has drained. Acquire it before sampling so this
            // iteration publishes the final tuple before exiting.
            const bool stopping =
                progress_stop_requested_.load(
                    std::memory_order_acquire);

            realtime::ProcessingProgressV2 current{};
            current.applied_sequence =
                applied_sequence_.load(std::memory_order_acquire);
            current.accepted_sequence =
                accepted_sequence_.load(std::memory_order_acquire);
            if (current.accepted_sequence !=
                    last_published.accepted_sequence ||
                current.applied_sequence !=
                    last_published.applied_sequence) {
                if (!PublishProcessingProgressNow()) {
                    return;
                }
                last_published = current;
            }
            if (stopping) {
                return;
            }
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
            } else if (!DecodeOne(
                           source,
                           command.message,
                           command.identity)) {
                TripFatal();
            }
            command = DecoderCommand{};
        }
    }

    [[nodiscard]] bool DecodeOne(
        std::uint8_t source,
        const realtime::OwnedIngressMessageHandleV1& message,
        const market::ObservedInstrumentIdentityViewV2& identity)
        noexcept {
        if (!message || message->source_slot() != source ||
            IsMixedTickSourceSlot(source) !=
                (message->tick_stream_sequence() != 0U) ||
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
            ReportPipelineFailure(
                "market_decode",
                source,
                message->global_ingress_sequence(),
                static_cast<std::uint64_t>(decode_error));
            return false;
        }
        if (!market::ApplyObservedInstrumentIdentityV2(
                identity, &decoded)) {
            ReportPipelineFailure(
                "instrument_identity_apply",
                source,
                message->global_ingress_sequence(),
                identity.instrument_id);
            return false;
        }

        std::optional<market::RealtimeHistoryEventInputV1> input =
            market::RealtimeHistoryEventInputV1::Create(
                source,
                message->global_ingress_sequence(),
                std::move(decoded),
                message->tick_stream_sequence());
        if (!input.has_value()) {
            ReportHistoryInputFailure(
                source,
                message->global_ingress_sequence(),
                message->tick_stream_sequence(),
                decoded);
            return false;
        }
        const market::RealtimeHistorySubmitErrorV1 submit_error =
            history_->TrySubmit(std::move(*input));
        if (submit_error !=
            market::RealtimeHistorySubmitErrorV1::kNone) {
            ReportPipelineFailure(
                "history_submit",
                source,
                message->global_ingress_sequence(),
                static_cast<std::uint64_t>(submit_error));
            return false;
        }
        decoded_messages_.fetch_add(1U, std::memory_order_relaxed);
        return true;
    }

    void ReportHistoryInputFailure(
        std::uint8_t source_slot,
        std::uint64_t ingress_sequence,
        std::uint64_t tick_stream_sequence,
        const market::DecodedMarketEventV1& event) noexcept {
        if (pipeline_failure_reported_.test_and_set(
                std::memory_order_relaxed)) {
            return;
        }
        std::visit(
            [&](const auto& value) noexcept {
                std::fprintf(
                    stderr,
                    "l2flow-pipeline: first fatal "
                    "reason=history_input_create source_slot=%u "
                    "ingress_sequence=%llu tick_stream_sequence=%llu "
                    "common_kind=%u source_stream_id=%u "
                    "source_sequence=%llu instrument_id=%u "
                    "ordinal=%zu retained_body_bytes=%zu\n",
                    static_cast<unsigned>(source_slot),
                    static_cast<unsigned long long>(ingress_sequence),
                    static_cast<unsigned long long>(
                        tick_stream_sequence),
                    static_cast<unsigned>(value.common.kind),
                    value.common.origin.source_stream_id,
                    static_cast<unsigned long long>(
                        value.common.origin.source_sequence),
                    value.common.instrument_id,
                    value.common.ordinal,
                    value.common.origin.body.size());
            },
            event);
    }

    void ReportPipelineFailure(
        const char* reason,
        std::uint8_t source_slot,
        std::uint64_t ingress_sequence,
        std::uint64_t detail) noexcept {
        if (pipeline_failure_reported_.test_and_set(
                std::memory_order_relaxed)) {
            return;
        }
        std::fprintf(
            stderr,
            "l2flow-pipeline: first fatal reason=%s source_slot=%u "
            "ingress_sequence=%llu detail=%llu\n",
            reason == nullptr ? "unknown" : reason,
            static_cast<unsigned>(source_slot),
            static_cast<unsigned long long>(ingress_sequence),
            static_cast<unsigned long long>(detail));
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
        RequestProcessingStop();
        RequestDecoderStop();
        RequestProgressStop();
        applied_progress_cv_.notify_all();
        dispatch_barrier_cv_.notify_all();
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

    void RequestProcessingStop() noexcept {
        if (processing_queue_ != nullptr) {
            processing_queue_->RequestStop();
        }
    }

    void RequestProgressStop() noexcept {
        if (config_.processing_progress_sink == nullptr) {
            return;
        }
        progress_stop_requested_.store(
            true, std::memory_order_release);
        RequestProgressPublication();
    }

    void JoinProcessingThread() noexcept {
        if (processing_thread_.joinable()) {
            processing_thread_.join();
        }
        processing_thread_started_ = false;
    }

    void JoinProgressThread() noexcept {
        if (progress_thread_.joinable()) {
            progress_thread_.join();
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
    // Declared before every possible handle owner so pool state is retired
    // only after the processing and decoder rings have released their
    // handles.
    std::unique_ptr<realtime::OwnedIngressMessagePoolV1> ingress_pool_;
    std::unique_ptr<StageLatencyCollector> latency_collector_;
    std::unique_ptr<ProcessingQueue> processing_queue_;
    std::thread processing_thread_;
    bool processing_thread_started_ = false;
    std::thread progress_thread_;
    std::binary_semaphore progress_wake_{0};
    std::atomic<bool> progress_wake_pending_{false};
    std::atomic<bool> progress_stop_requested_{false};
    std::array<std::unique_ptr<DecoderLane>,
               market::kRealtimeHistorySourceCountV1>
        lanes_{};
    bool decoder_threads_started_ = false;

    std::unique_ptr<market::RealtimeHistoryRuntimeV1> history_;
    std::unique_ptr<factor::RealtimeFactorEngineV1> factor_;
    std::unique_ptr<realtime::ContiguousSequenceTrackerV2>
        applied_tracker_;
    std::size_t applied_window_capacity_ = 0U;
    std::atomic<std::uint64_t> accepted_sequence_{0U};
    std::atomic<std::uint64_t> applied_sequence_{0U};
    std::mutex applied_progress_mutex_;
    std::condition_variable applied_progress_cv_;
    std::mutex dispatch_barrier_mutex_;
    std::condition_variable dispatch_barrier_cv_;
    std::atomic<std::uint64_t> dispatch_barrier_sequence_{0U};
    std::atomic<bool> dispatch_barrier_active_{false};

    std::shared_ptr<sdk::SdkFactory> sdk_factory_;
    std::unique_ptr<sdk::SdkManager> sdk_manager_;
    std::unique_ptr<sdk::SdkSubscriber> sdk_subscriber_;

    mutable std::mutex admission_mutex_;
    std::uint64_t global_ingress_sequence_ = 0U;
    std::uint64_t tick_stream_sequence_ = 0U;
    std::array<std::uint64_t,
               market::kRealtimeHistorySourceCountV1>
        source_sequences_{};
    std::uint64_t accepted_messages_ = 0U;
    std::uint64_t filtered_messages_ = 0U;
    std::array<std::uint64_t,
               market::kRealtimeHistorySourceCountV1>
        filtered_messages_by_source_{};
    std::uint64_t ignored_messages_ = 0U;
    std::uint64_t post_cut_messages_ = 0U;
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
    std::atomic_flag pipeline_failure_reported_ = ATOMIC_FLAG_INIT;
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

std::shared_ptr<const market::RealtimeKLineGenerationV1>
RealtimePipelineV1::AcquireLatestKLineGeneration() const noexcept {
    return impl_->AcquireKLine();
}

market::RealtimeLatestQueryErrorV1
RealtimePipelineV1::GetLatestSnapshot(
    std::uint32_t instrument_id,
    market::RealtimeLatestRecordViewV1* output) const noexcept {
    return impl_->GetLatestSnapshot(instrument_id, output);
}

market::RealtimeLatestQueryErrorV1
RealtimePipelineV1::GetLatestSnapshots(
    std::span<const std::uint32_t> instrument_ids,
    std::span<market::RealtimeLatestRecordViewV1> output)
    const noexcept {
    return impl_->GetLatestSnapshots(instrument_ids, output);
}

market::RealtimeLatestQueryErrorV1 RealtimePipelineV1::GetLatestTick(
    std::uint32_t instrument_id,
    market::RealtimeLatestRecordViewV1* output) const noexcept {
    return impl_->GetLatestTick(instrument_id, output);
}

market::RealtimeLatestQueryErrorV1 RealtimePipelineV1::GetLatestTicks(
    std::span<const std::uint32_t> instrument_ids,
    std::span<market::RealtimeLatestRecordViewV1> output)
    const noexcept {
    return impl_->GetLatestTicks(instrument_ids, output);
}

std::shared_ptr<const factor::RealtimeFactorGenerationV1>
RealtimePipelineV1::AcquireLatestFactorGeneration() const noexcept {
    return impl_->AcquireFactor();
}

RealtimePipelineSnapshotV1 RealtimePipelineV1::Snapshot() const noexcept {
    return impl_->Snapshot();
}

RealtimePipelineStageLatencySnapshotV1
RealtimePipelineV1::LatencySnapshot() const noexcept {
    return impl_->LatencySnapshot();
}

bool RealtimePipelineV1::fatal() const noexcept {
    return impl_->fatal();
}

void RealtimePipelineV1::StopAndDrain() noexcept {
    impl_->StopAndDrain();
}

}  // namespace l2flow::runtime
