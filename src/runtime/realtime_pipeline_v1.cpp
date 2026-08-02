#include "l2flow/runtime/realtime_pipeline_v1.h"

#include "l2flow/common/sha256.h"
#include "l2flow/control/quality_flags_v1.h"
#include "l2flow/market/mainland_a_share_filter_v1.h"
#include "l2flow/sdk/direct_sdk_runtime_v1.h"
#include "l2flow/sdk/production_subscription_v1.h"

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstdio>
#include <cstring>
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

[[noreturn]] void TerminateInvariant(const char* reason) noexcept {
    std::fprintf(
        stderr,
        "l2flow-pipeline: invariant failure=%s\n",
        reason != nullptr ? reason : "unknown");
    std::fflush(stderr);
    std::terminate();
}

void IncrementSingleWriterCounter(
    std::atomic<std::uint64_t>* counter) noexcept {
    counter->store(
        counter->load(std::memory_order_relaxed) + 1U,
        std::memory_order_relaxed);
}

void AddSingleWriterCounter(
    std::atomic<std::uint64_t>* counter,
    std::uint64_t amount) noexcept {
    const std::uint64_t current =
        counter->load(std::memory_order_relaxed);
    if (amount >
        std::numeric_limits<std::uint64_t>::max() - current) {
        TerminateInvariant("single_writer_counter_overflow");
    }
    counter->store(current + amount, std::memory_order_relaxed);
}

void IncrementSingleWriterCounterRelease(
    std::atomic<std::uint64_t>* counter) noexcept {
    counter->store(
        counter->load(std::memory_order_relaxed) + 1U,
        std::memory_order_release);
}

void DecrementSingleWriterCounter(
    std::atomic<std::uint64_t>* counter) noexcept {
    const std::uint64_t current =
        counter->load(std::memory_order_relaxed);
    if (current == 0U) {
        TerminateInvariant("single_writer_counter_underflow");
    }
    counter->store(current - 1U, std::memory_order_relaxed);
}

namespace factor = l2flow::factor;
namespace common = l2flow::common;
namespace control = l2flow::control;
namespace market = l2flow::market;
namespace realtime = l2flow::realtime;
namespace sdk = l2flow::sdk;
namespace mdl = datayes::mdl;

constexpr auto kMaximumCutTimeout = std::chrono::hours(24);
constexpr auto kProgressPublishMaximumDelay =
    std::chrono::milliseconds(1);
constexpr auto kAdmissionQueueRetryDelay =
    std::chrono::microseconds(100);
inline constexpr std::size_t kMaximumParallelDecoderSlotsPerShard = 1024U;

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

// Hashes the decoder-observable payload rather than vendor body storage.
// MDL string/list offsets are allocation-layout details, and the V4 Shanghai
// order-queue CSV omits OrderQueOper/OrderQueID (which the decoder deliberately
// does not publish). They must not manufacture a closed-handoff conflict.
class StartupSemanticHasher final {
public:
    explicit StartupSemanticHasher(
        bool normalize_shenzhen_snapshot_channel) noexcept
        : normalize_shenzhen_snapshot_channel_(
              normalize_shenzhen_snapshot_channel) {}

    template <typename Integer>
    void IntegerValue(Integer value) noexcept {
        static_assert(
            std::is_integral_v<Integer> &&
            !std::is_same_v<Integer, bool>);
        if (!ok_) {
            return;
        }
        ok_ = hasher_.Update(
            std::as_bytes(std::span(&value, 1U)));
    }

    template <typename Enum>
    void EnumValue(Enum value) noexcept {
        static_assert(std::is_enum_v<Enum>);
        IntegerValue(
            static_cast<std::underlying_type_t<Enum>>(value));
    }

    void BoolValue(bool value) noexcept {
        IntegerValue<std::uint8_t>(value ? 1U : 0U);
    }

    void StringValue(std::string_view value) noexcept {
        IntegerValue<std::uint64_t>(
            static_cast<std::uint64_t>(value.size()));
        if (ok_) {
            ok_ = hasher_.Update(std::as_bytes(std::span(value)));
        }
    }

    void Decimal(const market::DecimalValueV1& value) noexcept {
        IntegerValue(value.raw);
        IntegerValue(value.normalized_p6);
        IntegerValue(value.scale);
        BoolValue(value.valid);
        BoolValue(value.is_null);
    }

    void Quantity(const market::QuantityValueV1& value) noexcept {
        IntegerValue(value.raw);
        IntegerValue(value.scale);
        BoolValue(value.valid);
        BoolValue(value.is_null);
    }

    void Unsigned(const market::UnsignedValueV1& value) noexcept {
        IntegerValue(value.raw);
        BoolValue(value.valid);
    }

    void Time(const market::TimeValueV1& value) noexcept {
        IntegerValue(value.raw_hhmmssmmm);
        IntegerValue(value.nanoseconds_since_midnight);
        IntegerValue(value.unix_nanoseconds);
        BoolValue(value.valid);
        BoolValue(value.is_null);
        BoolValue(value.unix_nanoseconds_valid);
    }

    void Common(const market::DecodedMarketCommonV1& value) noexcept {
        EnumValue(value.kind);
        EnumValue(value.market);
        Time(value.exchange_time);
        Time(value.vendor_local_time);
        StringValue(value.security_id);
        StringValue(value.security_id_source);
        StringValue(value.md_stream_id);
        BoolValue(value.security_id_valid);
        BoolValue(value.security_id_source_valid);
        BoolValue(value.md_stream_id_valid);
        EnumValue(value.quantity_unit);
        EnumValue(value.security_type);
        EnumValue(value.asset_scope);
        // A nonzero offset for an empty dynamic field is a wire-layout
        // diagnostic, not a semantic difference. CSV reconstruction uses the
        // canonical zero offset, while an equivalent live SDK body need not.
        IntegerValue(
            value.quality_flags &
            ~control::QualityBit(
                control::QualityFlagV1::
                    kNoncanonicalEmptyOffset));
        IntegerValue(value.market_notices);
    }

    void BookLevel(const market::BookLevelV1& value) noexcept {
        Decimal(value.price);
        Quantity(value.quantity);
        IntegerValue(value.order_count);
        BoolValue(value.order_count_valid);
    }

    void BestQueue(const market::BestQueueV1& value) noexcept {
        IntegerValue(value.total_order_count);
        IntegerValue(value.actual_revealed_count);
        IntegerValue(value.retained_count);
        for (const auto& quantity : value.quantities) {
            Quantity(quantity);
        }
    }

    void Book(const market::SnapshotBookV1& value) noexcept {
        IntegerValue(value.actual_bid_depth);
        IntegerValue(value.actual_ask_depth);
        IntegerValue(value.retained_bid_depth);
        IntegerValue(value.retained_ask_depth);
        for (const auto& level : value.bids) {
            BookLevel(level);
        }
        for (const auto& level : value.asks) {
            BookLevel(level);
        }
        BestQueue(value.bid1_queue);
        BestQueue(value.ask1_queue);
    }

    void Tick(
        const market::TickFieldsV1& value,
        bool include_phase = true) noexcept {
        EnumValue(value.action);
        EnumValue(value.side);
        EnumValue(value.order_type);
        EnumValue(value.aggressor);
        if (include_phase) {
            EnumValue(value.phase);
        }
        Decimal(value.price);
        Quantity(value.quantity);
        Decimal(value.trade_amount);
        Quantity(value.matched_quantity);
        IntegerValue(value.primary_order_id);
        IntegerValue(value.buy_order_id);
        IntegerValue(value.sell_order_id);
        IntegerValue(
            include_phase
                ? value.validity_bitmap
                : value.validity_bitmap &
                      ~market::kTickPhaseValidV1);
    }

    void Event(const market::ShanghaiSnapshotV1& value) noexcept {
        Common(value.common);
        IntegerValue(value.image_status);
        StringValue(value.instrument_status);
        BoolValue(value.instrument_status_valid);
        Decimal(value.pre_close_price);
        Decimal(value.open_price);
        Decimal(value.high_price);
        Decimal(value.low_price);
        Decimal(value.last_price);
        Decimal(value.close_price);
        IntegerValue(value.trade_count);
        Quantity(value.trade_volume);
        Decimal(value.turnover);
        Quantity(value.total_bid_volume);
        Decimal(value.weighted_average_bid_price);
        Decimal(value.alternate_weighted_average_bid_price);
        Quantity(value.total_ask_volume);
        Decimal(value.weighted_average_ask_price);
        Decimal(value.alternate_weighted_average_ask_price);
        Unsigned(value.vendor_etf_buy_count);
        Quantity(value.vendor_etf_buy_quantity);
        Decimal(value.vendor_etf_buy_amount);
        Unsigned(value.vendor_etf_sell_count);
        Quantity(value.vendor_etf_sell_quantity);
        Decimal(value.vendor_etf_sell_amount);
        Decimal(value.yield_to_maturity);
        Quantity(value.total_warrant_exercise_quantity);
        Decimal(value.vendor_war_lower_value);
        Decimal(value.vendor_war_upper_value);
        IntegerValue(value.withdrawal_buy_count);
        Quantity(value.withdrawal_buy_volume);
        Decimal(value.withdrawal_buy_amount);
        IntegerValue(value.withdrawal_sell_count);
        Quantity(value.withdrawal_sell_volume);
        Decimal(value.withdrawal_sell_amount);
        IntegerValue(value.total_bid_order_count);
        IntegerValue(value.total_ask_order_count);
        Unsigned(value.maximum_bid_duration);
        Unsigned(value.maximum_ask_duration);
        Decimal(value.iopv);
        Book(value.book);
    }

    void Event(const market::ShanghaiTickV1& value) noexcept {
        Common(value.common);
        IntegerValue(value.business_index);
        IntegerValue(value.channel);
        StringValue(value.raw_type);
        StringValue(value.raw_tick_flag);
        BoolValue(value.raw_type_valid);
        BoolValue(value.raw_tick_flag_valid);
        // phase and its validity bit are inherited from prior S records for
        // A/D/T messages. raw_type/raw_tick_flag already bind the current
        // body's status semantics, so history-derived phase is excluded.
        Tick(value.fields, false);
    }

    void Event(const market::ShenzhenSnapshotV1& value) noexcept {
        Common(value.common);
        // V4 mdl_6_28_0 has no ChannelNo. Zero both sides only for this
        // explicitly documented unavailable source field.
        IntegerValue(
            normalize_shenzhen_snapshot_channel_
                ? std::uint32_t{0U}
                : value.channel);
        StringValue(value.trading_phase_code);
        BoolValue(value.trading_phase_code_valid);
        Decimal(value.pre_close_price);
        IntegerValue(value.trade_count);
        Quantity(value.volume);
        Decimal(value.turnover);
        Decimal(value.last_price);
        Decimal(value.open_price);
        Decimal(value.high_price);
        Decimal(value.low_price);
        Decimal(value.price_change_1);
        Decimal(value.price_change_2);
        Decimal(value.pe_ratio_1);
        Decimal(value.pe_ratio_2);
        Decimal(value.pre_close_iopv);
        Decimal(value.iopv);
        Quantity(value.total_ask_quantity);
        Decimal(value.weighted_average_ask_price);
        Quantity(value.total_bid_quantity);
        Decimal(value.weighted_average_bid_price);
        Decimal(value.high_limit_price);
        Decimal(value.low_limit_price);
        EnumValue(value.high_limit_semantics);
        EnumValue(value.low_limit_semantics);
        Quantity(value.open_interest);
        Decimal(value.vendor_opt_premium_ratio);
        Book(value.book);
    }

    void Event(const market::ShenzhenOrderV1& value) noexcept {
        Common(value.common);
        IntegerValue(value.channel);
        IntegerValue(value.application_sequence);
        IntegerValue(value.raw_side);
        IntegerValue(value.raw_order_type);
        Tick(value.fields);
    }

    void Event(const market::ShenzhenTransactionV1& value) noexcept {
        Common(value.common);
        IntegerValue(value.channel);
        IntegerValue(value.application_sequence);
        IntegerValue(value.raw_execution_type);
        Tick(value.fields);
    }

    [[nodiscard]] bool Finalize(
        common::Sha256Digest* output) noexcept {
        return ok_ && hasher_.Finalize(output);
    }

private:
    common::Sha256Hasher hasher_;
    bool normalize_shenzhen_snapshot_channel_ = false;
    bool ok_ = true;
};

[[nodiscard]] bool CanonicalDecodedDigest(
    const market::DecodedMarketEventV1& event,
    bool normalize_shenzhen_snapshot_channel,
    common::Sha256Digest* output) noexcept {
    if (output == nullptr) {
        return false;
    }
    StartupSemanticHasher hasher(
        normalize_shenzhen_snapshot_channel);
    std::visit(
        [&hasher](const auto& value) noexcept {
            hasher.Event(value);
        },
        event);
    return hasher.Finalize(output);
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
    explicit StageLatencyCollector(
        std::uint32_t trade_date,
        std::size_t completion_window_slots)
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
          append_call_(0, 20'000'000LL, 50ULL),
          callback_to_store_applied_(
              0, 5'000'000'000LL, 5'000ULL),
          callback_to_ipc_visible_(
              0, 5'000'000'000LL, 5'000ULL),
          decode_applied_slot_count_(completion_window_slots),
          decode_applied_slots_(
              std::make_unique<DecodeAppliedTimingSlot[]>(
                  completion_window_slots)) {
        trade_date_valid_ =
            FixedUtc8MidnightNs(trade_date_, &fixed_utc8_midnight_ns_);
        for (std::size_t source = 0U;
             source < market::kRealtimeHistorySourceCountV1;
             ++source) {
            callback_to_decoder_publish_[source] =
                std::make_unique<ConcurrentLinearLatencyHistogram>(
                    0, 20'000'000LL, 50ULL);
            decoder_queue_dwell_[source] =
                std::make_unique<ConcurrentLinearLatencyHistogram>(
                    0, 5'000'000'000LL, 5'000ULL);
            decode_duration_[source] =
                std::make_unique<ConcurrentLinearLatencyHistogram>(
                    0, 20'000'000LL, 50ULL);
            decode_to_history_submit_[source] =
                std::make_unique<ConcurrentLinearLatencyHistogram>(
                    0, 20'000'000LL, 50ULL);
            decode_to_applied_[source] =
                std::make_unique<ConcurrentLinearLatencyHistogram>(
                    0, 5'000'000'000LL, 5'000ULL);
        }
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

    void RecordDecoderPublish(
        std::uint8_t source,
        std::uint64_t callback_entry_monotonic_ns,
        std::uint64_t queue_publish_monotonic_ns) noexcept {
        if (source >= market::kRealtimeHistorySourceCountV1) {
            return;
        }
        std::int64_t elapsed = 0;
        if (callback_entry_monotonic_ns != 0U &&
            queue_publish_monotonic_ns != 0U &&
            UnsignedElapsed(
                callback_entry_monotonic_ns,
                queue_publish_monotonic_ns,
                &elapsed)) {
            callback_to_decoder_publish_[source]->Add(elapsed);
        } else {
            callback_to_decoder_publish_[source]->AddInvalid();
        }
    }

    void RecordDecoderWork(
        std::uint8_t source,
        std::uint64_t queue_publish_monotonic_ns,
        std::uint64_t dequeue_monotonic_ns,
        std::uint64_t decode_start_monotonic_ns,
        std::uint64_t decode_complete_monotonic_ns,
        std::uint64_t history_submit_complete_monotonic_ns,
        bool queue_clocks_valid,
        bool decode_clock_valid,
        bool history_submit_clock_valid) noexcept {
        if (source >= market::kRealtimeHistorySourceCountV1) {
            return;
        }
        std::int64_t elapsed = 0;
        if (queue_clocks_valid &&
            UnsignedElapsed(
                queue_publish_monotonic_ns,
                dequeue_monotonic_ns,
                &elapsed)) {
            decoder_queue_dwell_[source]->Add(elapsed);
        } else {
            decoder_queue_dwell_[source]->AddInvalid();
        }
        if (decode_clock_valid &&
            UnsignedElapsed(
                decode_start_monotonic_ns,
                decode_complete_monotonic_ns,
                &elapsed)) {
            decode_duration_[source]->Add(elapsed);
        } else {
            decode_duration_[source]->AddInvalid();
        }
        if (history_submit_clock_valid &&
            UnsignedElapsed(
                decode_complete_monotonic_ns,
                history_submit_complete_monotonic_ns,
                &elapsed)) {
            decode_to_history_submit_[source]->Add(elapsed);
        } else {
            decode_to_history_submit_[source]->AddInvalid();
        }
    }

    void RecordDecodeAppliedOrigin(
        std::uint8_t source,
        std::uint64_t ingress_sequence,
        std::uint64_t decode_complete_monotonic_ns,
        bool clock_valid) noexcept {
        if (source >= market::kRealtimeHistorySourceCountV1 ||
            ingress_sequence == 0U ||
            decode_applied_slot_count_ == 0U ||
            decode_applied_slots_ == nullptr) {
            return;
        }
        DecodeAppliedTimingSlot& slot =
            decode_applied_slots_[
                static_cast<std::size_t>(
                    ingress_sequence %
                    decode_applied_slot_count_)];
        // D is strictly smaller than this ring. A slot therefore cannot be
        // reused until its prior sequence has called RecordApplied and
        // advanced the contiguous applied frontier.
        slot.decode_complete_monotonic_ns.store(
            decode_complete_monotonic_ns,
            std::memory_order_relaxed);
        slot.source.store(source, std::memory_order_relaxed);
        slot.clock_valid.store(
            clock_valid ? 1U : 0U,
            std::memory_order_relaxed);
        slot.ingress_sequence.store(
            ingress_sequence,
            std::memory_order_release);
    }

    void RecordApplied(
        const market::RealtimeHistoryAppliedObservationV2& observation)
        noexcept {
        std::int64_t elapsed = 0;
        if (observation.source_slot <
                market::kRealtimeHistorySourceCountV1 &&
            observation.ingress_sequence != 0U &&
            decode_applied_slot_count_ != 0U &&
            decode_applied_slots_ != nullptr) {
            DecodeAppliedTimingSlot& slot =
                decode_applied_slots_[
                    static_cast<std::size_t>(
                        observation.ingress_sequence %
                        decode_applied_slot_count_)];
            const std::uint64_t slot_sequence =
                slot.ingress_sequence.load(std::memory_order_acquire);
            const std::uint8_t slot_source =
                slot.source.load(std::memory_order_relaxed);
            const bool slot_clock_valid =
                slot.clock_valid.load(std::memory_order_relaxed) != 0U;
            const std::uint64_t decode_complete =
                slot.decode_complete_monotonic_ns.load(
                    std::memory_order_relaxed);
            if (slot_sequence == observation.ingress_sequence) {
                slot.ingress_sequence.store(
                    0U, std::memory_order_release);
            }
            if (slot_sequence == observation.ingress_sequence &&
                slot_source == observation.source_slot &&
                slot_clock_valid &&
                observation.applied_complete_clock_valid &&
                UnsignedElapsed(
                    decode_complete,
                    observation.applied_complete_monotonic_ns,
                    &elapsed)) {
                decode_to_applied_[observation.source_slot]->Add(
                    elapsed);
            } else {
                decode_to_applied_[observation.source_slot]
                    ->AddInvalid();
            }
        }
        if (observation.external_publication_present &&
            observation.external_publication_clock_valid &&
            observation.recv_monotonic_ns >= 0 &&
            UnsignedElapsed(
                static_cast<std::uint64_t>(
                    observation.recv_monotonic_ns),
                observation
                    .external_publication_complete_monotonic_ns,
                &elapsed)) {
            callback_to_ipc_visible_.Add(elapsed);
        } else {
            callback_to_ipc_visible_.AddInvalid();
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
            callback_to_store_applied_.Add(elapsed);
        } else {
            callback_entry_to_append_complete_.AddInvalid();
            callback_to_store_applied_.AddInvalid();
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
        for (std::size_t source = 0U;
             source < market::kRealtimeHistorySourceCountV1;
             ++source) {
            result.callback_to_decoder_publish[source] =
                callback_to_decoder_publish_[source]->Snapshot();
            result.decoder_queue_dwell[source] =
                decoder_queue_dwell_[source]->Snapshot();
            result.decode_duration[source] =
                decode_duration_[source]->Snapshot();
            result.decode_to_history_submit[source] =
                decode_to_history_submit_[source]->Snapshot();
            result.decode_to_applied[source] =
                decode_to_applied_[source]->Snapshot();
        }
        result.callback_to_store_applied =
            callback_to_store_applied_.Snapshot();
        result.callback_to_ipc_visible =
            callback_to_ipc_visible_.Snapshot();
        return result;
    }

private:
    struct DecodeAppliedTimingSlot final {
        std::atomic<std::uint64_t> ingress_sequence{0U};
        std::atomic<std::uint64_t>
            decode_complete_monotonic_ns{0U};
        std::atomic<std::uint8_t> source{0U};
        std::atomic<std::uint8_t> clock_valid{0U};
    };

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
    ConcurrentLinearLatencyHistogram callback_to_store_applied_;
    ConcurrentLinearLatencyHistogram callback_to_ipc_visible_;
    std::array<std::unique_ptr<ConcurrentLinearLatencyHistogram>,
               market::kRealtimeHistorySourceCountV1>
        callback_to_decoder_publish_{};
    std::array<std::unique_ptr<ConcurrentLinearLatencyHistogram>,
               market::kRealtimeHistorySourceCountV1>
        decoder_queue_dwell_{};
    std::array<std::unique_ptr<ConcurrentLinearLatencyHistogram>,
               market::kRealtimeHistorySourceCountV1>
        decode_duration_{};
    std::array<std::unique_ptr<ConcurrentLinearLatencyHistogram>,
               market::kRealtimeHistorySourceCountV1>
        decode_to_history_submit_{};
    std::array<std::unique_ptr<ConcurrentLinearLatencyHistogram>,
               market::kRealtimeHistorySourceCountV1>
        decode_to_applied_{};
    const std::size_t decode_applied_slot_count_;
    std::unique_ptr<DecodeAppliedTimingSlot[]>
        decode_applied_slots_;
};

[[nodiscard]] bool BoundedAppliedWindowCapacity(
    const RealtimePipelineConfigV1& config,
    std::size_t* output) noexcept {
    constexpr std::size_t source_count =
        market::kRealtimeHistorySourceCountV1;
    if (output == nullptr ||
        config.decoder_queue_capacity_per_source == 0U ||
        config.decoder_queue_capacity_per_source >
            (std::numeric_limits<std::size_t>::max() -
             source_count) /
                source_count ||
        config.completion_tracker_capacity < 2U ||
        config.tick_ring_capacity < 2U) {
        return false;
    }
    std::size_t parallel_inflight = 0U;
    if (config.parallel_decoder_worker_count != 0U) {
        if (config.parallel_decoder_worker_count >
                kRealtimeParallelDecoderMaximumWorkersV1 ||
            config.parallel_decoder_slots_per_source_worker == 0U ||
            config.parallel_decoder_slots_per_source_worker >
                std::numeric_limits<std::size_t>::max() /
                    config.parallel_decoder_worker_count) {
            return false;
        }
        parallel_inflight =
            static_cast<std::size_t>(
                config.parallel_decoder_worker_count) *
            config.parallel_decoder_slots_per_source_worker;
    }
    const std::size_t base =
        config.decoder_queue_capacity_per_source * source_count;
    if (parallel_inflight >
        (std::numeric_limits<std::size_t>::max() - base -
         source_count) /
            source_count) {
        return false;
    }
    // In parallel mode each source FIFO can remain full while its dispatcher
    // owns one command and W*S additional commands occupy decode leases.
    const std::size_t target =
        base + source_count + parallel_inflight * source_count;
    const std::size_t maximum = std::min(
        {target,
         config.completion_tracker_capacity - 1U,
         config.tick_ring_capacity - 1U});
    if (maximum == 0U ||
        maximum >= config.completion_tracker_capacity ||
        maximum >= config.tick_ring_capacity ||
        maximum >
            realtime::kOwnedIngressMaximumInflightMessagesV1) {
        return false;
    }
    *output = maximum;
    return true;
}

[[nodiscard]] bool BoundedIngressPoolCapacity(
    const RealtimePipelineConfigV1& config,
    std::size_t* output) noexcept {
    constexpr std::size_t source_count =
        market::kRealtimeHistorySourceCountV1;
    if (output == nullptr ||
        config.decoder_queue_capacity_per_source == 0U ||
        config.decoder_queue_capacity_per_source >
            std::numeric_limits<std::size_t>::max() /
                source_count) {
        return false;
    }
    std::size_t maximum =
        config.decoder_queue_capacity_per_source * source_count;
    // One message can be held by each source owner after it has left its
    // queue. In parallel mode W*S additional messages can reside in the
    // preallocated decode leases for each source.
    std::size_t decoder_owners = source_count;
    if (config.parallel_decoder_worker_count != 0U) {
        if (config.parallel_decoder_worker_count >
                kRealtimeParallelDecoderMaximumWorkersV1 ||
            config.parallel_decoder_slots_per_source_worker == 0U ||
            config.parallel_decoder_slots_per_source_worker >
                std::numeric_limits<std::size_t>::max() /
                    config.parallel_decoder_worker_count) {
            return false;
        }
        const std::size_t per_source =
            static_cast<std::size_t>(
                config.parallel_decoder_worker_count) *
            config.parallel_decoder_slots_per_source_worker;
        if (per_source >
            (std::numeric_limits<std::size_t>::max() - decoder_owners) /
                source_count) {
            return false;
        }
        decoder_owners += per_source * source_count;
    }
    constexpr std::size_t callback_owner = 1U;
    constexpr std::size_t safety_margin = source_count;
    const std::array<std::size_t, 3U> additions{
        decoder_owners, callback_owner, safety_margin};
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

bool RealtimePipelineStartupSemanticDigestV1(
    const market::DecodedMarketEventV1& event,
    bool normalize_shenzhen_snapshot_channel,
    common::Sha256Digest* output) noexcept {
    return CanonicalDecodedDigest(
        event, normalize_shenzhen_snapshot_channel, output);
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
        case RealtimePipelineIngressErrorV1::kDecoderAdmissionFailed:
            return "decoder_admission_failed";
        case RealtimePipelineIngressErrorV1::kStopped:
            return "stopped";
        case RealtimePipelineIngressErrorV1::kFatal:
            return "fatal";
        case RealtimePipelineIngressErrorV1::kFilteredNonAShare:
            return "filtered_non_a_share";
        case RealtimePipelineIngressErrorV1::kInstrumentKeyRejected:
            return "instrument_key_rejected";
        case RealtimePipelineIngressErrorV1::kCatalogMiss:
            return "catalog_miss";
        case RealtimePipelineIngressErrorV1::kCaptureFailed:
            return "capture_failed";
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
        case RealtimePipelineCutErrorV1::kFenceArrivalFailed:
            return "fence_arrival_failed";
        case RealtimePipelineCutErrorV1::kGenerationBeginFailed:
            return "generation_begin_failed";
        case RealtimePipelineCutErrorV1::kFenceAdmissionFailed:
            return "fence_admission_failed";
        case RealtimePipelineCutErrorV1::kFenceSealFailed:
            return "fence_seal_failed";
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

class RealtimePipelineV1::Impl final
    : public mdl::MessageHandlerBase {
public:
    struct CallbackClockObservation final {
        std::uint64_t realtime_ns = 0U;
        std::uint64_t monotonic_ns = 0U;
        bool valid = false;
    };

    struct DecoderTimingObservation final {
        std::uint64_t decode_complete_monotonic_ns = 0U;
        std::uint64_t history_submit_complete_monotonic_ns = 0U;
        bool decode_complete_clock_valid = false;
        bool history_submit_complete_clock_valid = false;
    };

    enum class CommandKind : std::uint8_t {
        kMessage = 0U,
        kGenerationFence,
    };

    struct DecoderCommand final {
        CommandKind kind = CommandKind::kMessage;
        realtime::OwnedIngressMessageHandleV1 message;
        market::DailyInstrumentIdentityViewV2 identity{};
        std::uint64_t generation = 0U;
        std::uint64_t additional_market_notices = 0U;
        std::uint64_t callback_entry_monotonic_ns = 0U;
        std::uint64_t queue_publish_monotonic_ns = 0U;
    };

    class DecoderQueue final {
    public:
        explicit DecoderQueue(std::size_t message_capacity)
            : message_capacity_(message_capacity),
              // Q message slots, one reserved control slot, and one empty
              // ring cell that distinguishes full from empty.
              slots_(message_capacity + 2U) {}

        DecoderQueue(const DecoderQueue&) = delete;
        DecoderQueue& operator=(const DecoderQueue&) = delete;

        template <typename Commit>
        [[nodiscard]] bool TryPushWithCommit(
            DecoderCommand&& command,
            Commit&& commit) noexcept {
            static_assert(
                std::is_nothrow_invocable_v<
                    Commit&, std::uint64_t>);
            static_assert(
                std::is_nothrow_move_constructible_v<DecoderCommand>);
            if (command.kind != CommandKind::kMessage ||
                !command.message) {
                return false;
            }
            // The enclosing Pipeline serializes every producer with
            // admission_mutex_ and closes admission before RequestStop. The
            // queue therefore needs only this read-mostly terminal flag; a
            // per-message publication lease would add two needless RMWs.
            if (stop_requested_.load(std::memory_order_acquire)) {
                return false;
            }
            if (!WakeEpochAvailable()) {
                return false;
            }
            // All message/fence producers are serialized by admission_mutex_.
            // Keep producer and consumer progress on separate cache lines and
            // refresh the consumer count only near capacity. This avoids four
            // cross-core depth RMWs on every successful SPSC handoff.
            if (producer_message_count_ - cached_consumed_message_count_ >=
                message_capacity_) {
                cached_consumed_message_count_ =
                    consumed_message_count_.load(
                        std::memory_order_acquire);
                if (producer_message_count_ -
                        cached_consumed_message_count_ >=
                    message_capacity_) {
                    full_count_.fetch_add(
                        1U, std::memory_order_relaxed);
                    return false;
                }
            }
            const std::size_t tail =
                tail_.load(std::memory_order_relaxed);
            const std::size_t next = Increment(tail);
            if (next == cached_producer_head_) {
                cached_producer_head_ =
                    head_.load(std::memory_order_acquire);
                if (next == cached_producer_head_) {
                    full_count_.fetch_add(
                        1U, std::memory_order_relaxed);
                    return false;
                }
            }
            slots_[tail].emplace(std::move(command));
            std::uint64_t queue_publish_monotonic_ns = 0U;
            if (slots_[tail]->callback_entry_monotonic_ns != 0U) {
                static_cast<void>(ReadClockNs(
                    CLOCK_MONOTONIC,
                    &queue_publish_monotonic_ns));
            }
            slots_[tail]->queue_publish_monotonic_ns =
                queue_publish_monotonic_ns;
            ++producer_message_count_;
            const std::uint64_t observed_consumed =
                consumed_message_count_.load(
                    std::memory_order_acquire);
            if (observed_consumed > cached_consumed_message_count_) {
                cached_consumed_message_count_ = observed_consumed;
            }
            const std::uint64_t message_depth =
                producer_message_count_ -
                observed_consumed;
            UpdateHighWater(message_depth);

            // Irreversible admission linearization point. Slot construction
            // is complete and every later operation is a noexcept atomic
            // publication. The consumer's acquire tail load therefore also
            // observes all accepted-frontier writes made by commit.
            std::forward<Commit>(commit)(
                queue_publish_monotonic_ns);
            tail_.store(next, std::memory_order_release);
            published_message_count_.store(
                producer_message_count_, std::memory_order_release);
            WakeConsumer();
            return true;
        }

        [[nodiscard]] bool TryPushFence(
            DecoderCommand&& command) noexcept {
            static_assert(
                std::is_nothrow_move_constructible_v<DecoderCommand>);
            if (command.kind != CommandKind::kGenerationFence ||
                command.message || command.generation == 0U) {
                return false;
            }
            if (stop_requested_.load(std::memory_order_acquire) ||
                !WakeEpochAvailable() ||
                control_inflight_.load(std::memory_order_acquire)) {
                return false;
            }
            const std::size_t tail =
                tail_.load(std::memory_order_relaxed);
            const std::size_t next = Increment(tail);
            if (next == cached_producer_head_) {
                cached_producer_head_ =
                    head_.load(std::memory_order_acquire);
                if (next == cached_producer_head_) {
                    return false;
                }
            }
            slots_[tail].emplace(std::move(command));
            control_inflight_.store(true, std::memory_order_relaxed);
            tail_.store(next, std::memory_order_release);
            ++producer_control_count_;
            published_control_count_.store(
                producer_control_count_, std::memory_order_release);
            WakeConsumer();
            return true;
        }

        void CompleteFence() noexcept {
            control_inflight_.store(false, std::memory_order_release);
        }

        [[nodiscard]] bool Pop(
            DecoderCommand* output,
            std::size_t* remaining_ring_depth = nullptr) noexcept {
            if (output == nullptr) {
                return false;
            }
            while (true) {
                if (TryPop(output, remaining_ring_depth)) {
                    return true;
                }
                if (stop_requested_.load(std::memory_order_acquire)) {
                    // RequestStop runs only after the serialized producer is
                    // quiescent. Re-sample the ring after observing that
                    // terminal publication: the preceding empty observation
                    // may have raced the last accepted tail store.
                    return TryPop(output, remaining_ring_depth);
                }
                const std::uint64_t observed_epoch =
                    wake_epoch_.load(std::memory_order_acquire);
                // Close the window between observing an empty tail and
                // arming the atomic wait. If publication wins either side,
                // this pop sees the tail or wait observes a changed epoch.
                if (TryPop(output, remaining_ring_depth)) {
                    return true;
                }
                if (stop_requested_.load(std::memory_order_acquire)) {
                    return TryPop(output, remaining_ring_depth);
                }
                wake_epoch_.wait(
                    observed_epoch, std::memory_order_acquire);
            }
        }

        void RequestStop() noexcept {
            // Pipeline admission serialization guarantees that no producer is
            // inside publication when this terminal transition is requested.
            if (!stop_requested_.exchange(
                    true, std::memory_order_acq_rel)) {
                // Pushes reserve UINT64_MAX for this terminal wake.
                wake_epoch_.fetch_add(1U, std::memory_order_release);
                wake_epoch_.notify_all();
            }
        }

        [[nodiscard]] RealtimeDecoderQueueSnapshotV1 Snapshot()
            const noexcept {
            RealtimeDecoderQueueSnapshotV1 result{};
            result.message_capacity = message_capacity_;
            for (;;) {
                const std::uint64_t published_messages =
                    published_message_count_.load(
                        std::memory_order_acquire);
                const std::uint64_t published_controls =
                    published_control_count_.load(
                        std::memory_order_acquire);
                const std::uint64_t consumed_messages =
                    consumed_message_count_.load(
                        std::memory_order_acquire);
                const std::uint64_t consumed_controls =
                    consumed_control_count_.load(
                        std::memory_order_acquire);
                if (consumed_messages <= published_messages &&
                    consumed_controls <= published_controls) {
                    result.message_depth = static_cast<std::size_t>(
                        published_messages - consumed_messages);
                    result.total_depth = result.message_depth +
                        static_cast<std::size_t>(
                            published_controls - consumed_controls);
                    break;
                }
            }
            result.message_high_water =
                message_high_water_.load(std::memory_order_acquire);
            result.full_count =
                full_count_.load(std::memory_order_acquire);
            return result;
        }

        [[nodiscard]] bool Empty() const noexcept {
            return head_.load(std::memory_order_acquire) ==
                   tail_.load(std::memory_order_acquire);
        }

        [[nodiscard]] std::size_t MessageDepthSnapshot() const noexcept {
            for (;;) {
                const std::uint64_t published =
                    published_message_count_.load(
                        std::memory_order_acquire);
                const std::uint64_t consumed =
                    consumed_message_count_.load(
                        std::memory_order_acquire);
                if (consumed <= published) {
                    return static_cast<std::size_t>(
                        published - consumed);
                }
            }
        }

    private:
        [[nodiscard]] bool TryPop(
            DecoderCommand* output,
            std::size_t* remaining_ring_depth) noexcept {
            const std::size_t head =
                head_.load(std::memory_order_relaxed);
            const std::size_t tail =
                tail_.load(std::memory_order_acquire);
            if (head == tail) {
                return false;
            }
            *output = std::move(*slots_[head]);
            slots_[head].reset();
            // Release the physical ring cell before publishing logical
            // consumption. A producer that observes capacity through the
            // consumed frontier must also be able to observe the new head;
            // otherwise a transient in-progress pop could be misreported as
            // terminal queue-full and fail closed.
            const std::size_t next_head = Increment(head);
            head_.store(next_head, std::memory_order_release);
            if (remaining_ring_depth != nullptr) {
                // Reuse the tail acquire already required by Pop. This is the
                // number of ring cells remaining in the observed SPSC prefix,
                // so adaptive decode can inspect every pop without adding a
                // second cross-core atomic depth sample. At most one cell is a
                // generation fence; counting it can only activate the farm
                // one record early.
                *remaining_ring_depth =
                    tail >= next_head
                        ? tail - next_head
                        : slots_.size() - next_head + tail;
            }
            if (output->kind == CommandKind::kMessage) {
                ++consumer_message_count_;
                consumed_message_count_.store(
                    consumer_message_count_, std::memory_order_release);
            } else {
                ++consumer_control_count_;
                consumed_control_count_.store(
                    consumer_control_count_, std::memory_order_release);
            }
            return true;
        }

        void WakeConsumer() noexcept {
            // The caller checked that UINT64_MAX remains reserved for stop.
            // These are the only post-commit operations: an atomic epoch
            // publication and notification, both sequenced after the release
            // tail store.
            wake_epoch_.fetch_add(1U, std::memory_order_release);
            wake_epoch_.notify_one();
        }

        [[nodiscard]] bool WakeEpochAvailable() const noexcept {
            return wake_epoch_.load(std::memory_order_acquire) <
                   std::numeric_limits<std::uint64_t>::max() - 1U;
        }

        void UpdateHighWater(std::uint64_t candidate) noexcept {
            if (candidate > producer_message_high_water_) {
                producer_message_high_water_ = candidate;
                message_high_water_.store(
                    static_cast<std::size_t>(candidate),
                    std::memory_order_relaxed);
            }
        }

        [[nodiscard]] std::size_t Increment(std::size_t value) const noexcept {
            ++value;
            return value == slots_.size() ? 0U : value;
        }

        const std::size_t message_capacity_;
        std::vector<std::optional<DecoderCommand>> slots_;
        alignas(64) std::atomic<std::size_t> head_{0U};
        std::uint64_t consumer_message_count_ = 0U;
        std::atomic<std::uint64_t> consumed_message_count_{0U};
        std::uint64_t consumer_control_count_ = 0U;
        std::atomic<std::uint64_t> consumed_control_count_{0U};
        alignas(64) std::atomic<std::size_t> tail_{0U};
        std::size_t cached_producer_head_ = 0U;
        std::uint64_t producer_message_count_ = 0U;
        std::uint64_t producer_control_count_ = 0U;
        std::uint64_t cached_consumed_message_count_ = 0U;
        std::uint64_t producer_message_high_water_ = 0U;
        std::atomic<std::uint64_t> published_message_count_{0U};
        std::atomic<std::uint64_t> published_control_count_{0U};
        std::atomic<std::size_t> message_high_water_{0U};
        std::atomic<std::uint64_t> full_count_{0U};
        std::atomic<bool> control_inflight_{false};
        std::atomic<std::uint64_t> wake_epoch_{0U};
        std::atomic<bool> stop_requested_{false};
    };

    struct ParallelDecodeTask final {
        std::atomic<bool> available{true};
        DecoderCommand command{};
        market::DecodedMarketEventV1 decoded{};
        market::MarketDecodeErrorV1 decode_error =
            market::MarketDecodeErrorV1::kNone;
        std::uint8_t source_slot =
            static_cast<std::uint8_t>(
                market::kRealtimeHistorySourceCountV1);
        std::uint64_t global_ingress_sequence = 0U;
        std::uint64_t source_sequence = 0U;
        std::uint64_t tick_stream_sequence = 0U;
        bool identity_applied = false;
        std::uint64_t worker_dequeue_monotonic_ns = 0U;
        std::uint64_t decode_start_monotonic_ns = 0U;
        std::uint64_t decode_complete_monotonic_ns = 0U;
        bool worker_dequeue_clock_valid = false;
        bool decode_start_clock_valid = false;
        bool decode_complete_clock_valid = false;

        ParallelDecodeTask() = default;
        ParallelDecodeTask(const ParallelDecodeTask&) = delete;
        ParallelDecodeTask& operator=(const ParallelDecodeTask&) = delete;

        void ResetAndRelease(
            std::atomic<std::uint64_t>* epoch,
            std::atomic<bool>* waiter_armed) noexcept {
            command = DecoderCommand{};
            // DecodeStateless fully replaces this value on success, and a
            // failed decode is never allowed to inspect it. Do not assign a
            // default DecodedMarketEventV1 here: its first snapshot
            // alternative is deliberately large and zeroing it on every
            // recycle measurably extends callback-to-reader latency.
            decode_error = market::MarketDecodeErrorV1::kNone;
            source_slot = static_cast<std::uint8_t>(
                market::kRealtimeHistorySourceCountV1);
            global_ingress_sequence = 0U;
            source_sequence = 0U;
            tick_stream_sequence = 0U;
            identity_applied = false;
            worker_dequeue_monotonic_ns = 0U;
            decode_start_monotonic_ns = 0U;
            decode_complete_monotonic_ns = 0U;
            worker_dequeue_clock_valid = false;
            decode_start_clock_valid = false;
            decode_complete_clock_valid = false;
            available.store(true, std::memory_order_release);
            // Acquire rechecks available after arming. A release that sees no
            // waiter therefore needs neither an epoch RMW nor a futex wake on
            // the common streaming path.
            if (epoch != nullptr && waiter_armed != nullptr &&
                waiter_armed->exchange(
                    false, std::memory_order_acq_rel)) {
                epoch->fetch_add(1U, std::memory_order_release);
                epoch->notify_one();
            }
        }
    };

    class ParallelIssueQueue final {
    public:
        explicit ParallelIssueQueue(std::size_t capacity)
            : slots_(capacity + 1U, nullptr) {}

        ParallelIssueQueue(const ParallelIssueQueue&) = delete;
        ParallelIssueQueue& operator=(const ParallelIssueQueue&) = delete;

        [[nodiscard]] bool TryPush(ParallelDecodeTask* task) noexcept {
            if (task == nullptr) {
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
            slots_[tail] = task;
            tail_.store(next, std::memory_order_release);
            const std::size_t depth = Distance(next, head);
            if (depth > producer_high_water_) {
                producer_high_water_ = depth;
                high_water_.store(depth, std::memory_order_relaxed);
            }
            return true;
        }

        [[nodiscard]] bool TryPop(ParallelDecodeTask** output) noexcept {
            if (output == nullptr) {
                return false;
            }
            const std::size_t head =
                head_.load(std::memory_order_relaxed);
            if (head == tail_.load(std::memory_order_acquire)) {
                return false;
            }
            *output = slots_[head];
            slots_[head] = nullptr;
            head_.store(Increment(head), std::memory_order_release);
            return *output != nullptr;
        }

        [[nodiscard]] std::size_t DepthSnapshot() const noexcept {
            const std::size_t head =
                head_.load(std::memory_order_acquire);
            const std::size_t tail =
                tail_.load(std::memory_order_acquire);
            return Distance(tail, head);
        }

        [[nodiscard]] std::size_t HighWater() const noexcept {
            return high_water_.load(std::memory_order_acquire);
        }

    private:
        [[nodiscard]] std::size_t Increment(std::size_t value) const
            noexcept {
            ++value;
            return value == slots_.size() ? 0U : value;
        }

        [[nodiscard]] std::size_t Distance(
            std::size_t tail,
            std::size_t head) const noexcept {
            return tail >= head
                       ? tail - head
                       : slots_.size() - head + tail;
        }

        std::vector<ParallelDecodeTask*> slots_;
        alignas(64) std::atomic<std::size_t> head_{0U};
        alignas(64) std::atomic<std::size_t> tail_{0U};
        std::size_t producer_high_water_ = 0U;
        std::atomic<std::size_t> high_water_{0U};
    };

    struct ParallelDecodeShard final {
        explicit ParallelDecodeShard(std::size_t requested_slot_count)
            : slots(std::make_unique<ParallelDecodeTask[]>(
                  requested_slot_count)),
              issue(requested_slot_count),
              slot_count(requested_slot_count) {}

        [[nodiscard]] ParallelDecodeTask* Acquire(
            std::size_t slot,
            const std::atomic<bool>& fatal,
            std::atomic<std::uint64_t>* wait_count) noexcept {
            if (slot >= slot_count) {
                return nullptr;
            }
            if (fatal.load(std::memory_order_acquire)) {
                return nullptr;
            }
            ParallelDecodeTask* const task = &slots[slot];
            bool counted = false;
            while (!task->available.load(std::memory_order_acquire)) {
                if (fatal.load(std::memory_order_acquire)) {
                    return nullptr;
                }
                if (!counted && wait_count != nullptr) {
                    wait_count->fetch_add(1U, std::memory_order_relaxed);
                    counted = true;
                }
                const std::uint64_t observed =
                    release_epoch.load(std::memory_order_acquire);
                const bool already_armed =
                    release_waiter_armed.exchange(
                        true, std::memory_order_acq_rel);
                if (already_armed) {
                    TerminateInvariant("parallel_lease_waiter_rearmed");
                }
                if (!task->available.load(std::memory_order_acquire) &&
                    !fatal.load(std::memory_order_acquire)) {
                    release_epoch.wait(observed, std::memory_order_acquire);
                    // A terminal Wake can advance the epoch just before this
                    // dispatcher arms. In that case wait returns immediately
                    // and no releaser owns the armed bit.
                    static_cast<void>(
                        release_waiter_armed.exchange(
                            false, std::memory_order_acq_rel));
                } else {
                    static_cast<void>(
                        release_waiter_armed.exchange(
                            false, std::memory_order_acq_rel));
                }
            }
            if (fatal.load(std::memory_order_acquire)) {
                return nullptr;
            }
            task->available.store(false, std::memory_order_relaxed);
            return task;
        }

        void Wake() noexcept {
            static_cast<void>(release_waiter_armed.exchange(
                false, std::memory_order_acq_rel));
            release_epoch.fetch_add(1U, std::memory_order_release);
            release_epoch.notify_all();
        }

        std::unique_ptr<ParallelDecodeTask[]> slots;
        ParallelIssueQueue issue;
        const std::size_t slot_count;
        std::atomic<std::uint64_t> release_epoch{0U};
        alignas(64) std::atomic<bool> release_waiter_armed{false};
    };

    class ParallelCompletionRing final {
    public:
        explicit ParallelCompletionRing(std::size_t capacity)
            : capacity_(capacity),
              slots_(std::make_unique<
                     std::atomic<ParallelDecodeTask*>[]>(capacity)),
              failed_slots_(std::make_unique<
                     std::atomic<ParallelDecodeTask*>[]>(capacity)) {
            for (std::size_t index = 0U; index < capacity_; ++index) {
                slots_[index].store(nullptr, std::memory_order_relaxed);
                failed_slots_[index].store(
                    nullptr, std::memory_order_relaxed);
            }
        }

        ParallelCompletionRing(const ParallelCompletionRing&) = delete;
        ParallelCompletionRing& operator=(const ParallelCompletionRing&) =
            delete;

        [[nodiscard]] bool Publish(ParallelDecodeTask* task) noexcept {
            if (task == nullptr ||
                task->source_slot >=
                    market::kRealtimeHistorySourceCountV1 ||
                task->source_sequence == 0U ||
                task->global_ingress_sequence == 0U) {
                publish_failures_.fetch_add(1U, std::memory_order_relaxed);
                return false;
            }
            std::atomic<ParallelDecodeTask*>& cell =
                slots_[Index(task->source_sequence)];
            ParallelDecodeTask* expected = nullptr;
            if (!cell.compare_exchange_strong(
                    expected,
                    task,
                    std::memory_order_release,
                    std::memory_order_relaxed)) {
                publish_failures_.fetch_add(1U, std::memory_order_relaxed);
                return false;
            }
            SignalWaiter();
            return true;
        }

        // A normal completion publication failure is fatal, but the ordered
        // committer still needs an exact-sequence tombstone so it can release
        // the task lease and drain every later completion without hanging.
        [[nodiscard]] bool PublishFailure(
            ParallelDecodeTask* task) noexcept {
            if (task == nullptr ||
                task->source_slot >=
                    market::kRealtimeHistorySourceCountV1 ||
                task->source_sequence == 0U ||
                task->global_ingress_sequence == 0U) {
                return false;
            }
            std::atomic<ParallelDecodeTask*>& cell =
                failed_slots_[Index(task->source_sequence)];
            ParallelDecodeTask* expected = nullptr;
            if (!cell.compare_exchange_strong(
                    expected,
                    task,
                    std::memory_order_release,
                    std::memory_order_relaxed)) {
                return false;
            }
            SignalWaiter();
            return true;
        }

        [[nodiscard]] ParallelDecodeTask* TryTake(
            std::uint64_t source_sequence) noexcept {
            if (source_sequence == 0U) {
                return nullptr;
            }
            return slots_[Index(source_sequence)].exchange(
                nullptr, std::memory_order_acq_rel);
        }

        [[nodiscard]] ParallelDecodeTask* TryTakeFailure(
            std::uint64_t source_sequence) noexcept {
            if (source_sequence == 0U) {
                return nullptr;
            }
            return failed_slots_[Index(source_sequence)].exchange(
                nullptr, std::memory_order_acq_rel);
        }

        [[nodiscard]] std::uint64_t epoch() const noexcept {
            return completion_epoch_.load(std::memory_order_acquire);
        }

        void Wait(std::uint64_t observed,
                  std::uint64_t source_sequence) const noexcept {
            const bool already_armed = waiter_armed_.exchange(
                true, std::memory_order_acq_rel);
            if (already_armed) {
                TerminateInvariant("parallel_completion_waiter_rearmed");
            }
            const std::size_t index = Index(source_sequence);
            if (slots_[index].load(std::memory_order_acquire) == nullptr &&
                failed_slots_[index].load(
                    std::memory_order_acquire) == nullptr) {
                completion_epoch_.wait(
                    observed, std::memory_order_acquire);
                // WakeAll may advance the epoch after the caller sampled it
                // but before this waiter arms. In that case atomic::wait
                // returns immediately and no publisher owns the armed bit.
                // Always disarm on return; a publisher that observed the bit
                // already cleared it before changing the epoch, and a later
                // publisher may rely on the next target-cell recheck instead.
                static_cast<void>(waiter_armed_.exchange(
                    false, std::memory_order_acq_rel));
            } else {
                static_cast<void>(waiter_armed_.exchange(
                    false, std::memory_order_acq_rel));
            }
        }

        void WakeAll() noexcept {
            static_cast<void>(waiter_armed_.exchange(
                false, std::memory_order_acq_rel));
            completion_epoch_.fetch_add(1U, std::memory_order_release);
            completion_epoch_.notify_all();
        }

        [[nodiscard]] std::size_t capacity() const noexcept {
            return capacity_;
        }
        [[nodiscard]] std::uint64_t publish_failures() const noexcept {
            return publish_failures_.load(std::memory_order_acquire);
        }

    private:
        void SignalWaiter() noexcept {
            // The ordered committer rechecks its exact sequence cells after
            // arming. Avoid one epoch RMW and notify syscall per completion
            // while it is already running.
            if (waiter_armed_.exchange(
                    false, std::memory_order_acq_rel)) {
                completion_epoch_.fetch_add(
                    1U, std::memory_order_release);
                completion_epoch_.notify_one();
            }
        }

        [[nodiscard]] std::size_t Index(
            std::uint64_t source_sequence) const noexcept {
            return static_cast<std::size_t>(
                (source_sequence - 1U) % capacity_);
        }

        const std::size_t capacity_;
        std::unique_ptr<std::atomic<ParallelDecodeTask*>[]> slots_;
        std::unique_ptr<std::atomic<ParallelDecodeTask*>[]> failed_slots_;
        std::atomic<std::uint64_t> publish_failures_{0U};
        mutable std::atomic<std::uint64_t> completion_epoch_{0U};
        mutable std::atomic<bool> waiter_armed_{false};
    };

    struct DecoderLane final {
        DecoderLane(std::size_t capacity,
                    market::MarketDecoderConfigV1 decoder_config,
                    std::uint32_t parallel_worker_count,
                    std::size_t parallel_slots_per_worker)
            : queue(capacity),
              decoder(decoder_config),
              completion(
                  parallel_worker_count == 0U
                      ? nullptr
                      : std::make_unique<ParallelCompletionRing>(
                            static_cast<std::size_t>(
                                parallel_worker_count) *
                            parallel_slots_per_worker)) {
            parallel_shards.reserve(parallel_worker_count);
            for (std::uint32_t worker = 0U;
                 worker < parallel_worker_count;
                 ++worker) {
                parallel_shards.push_back(
                    std::make_unique<ParallelDecodeShard>(
                        parallel_slots_per_worker));
            }
        }

        DecoderQueue queue;
        market::MarketDecoderV1 decoder;
        std::vector<std::unique_ptr<ParallelDecodeShard>> parallel_shards;
        std::unique_ptr<ParallelCompletionRing> completion;
        std::thread thread;
        std::thread commit_thread;
        std::atomic<bool> dispatch_done{false};
        std::atomic<std::uint64_t> last_dispatched_source_sequence{0U};
        std::atomic<std::uint64_t> committed_source_sequence{0U};
        // completion->epoch() has exactly one waiter: commit_thread. The
        // source dispatcher uses this independent event only while a rare
        // generation fence waits for ordered commit progress. Keeping the
        // wait domains separate prevents notify_one from waking the wrong
        // waiter and removes a completion-ring WakeAll from every record.
        alignas(64) std::atomic<std::uint64_t> commit_progress_epoch{0U};
        alignas(64) std::atomic<bool> commit_progress_waiter_armed{false};
        // The dispatcher is the sole issuer and the ordered committer is the
        // sole normal-path retiree. Keeping monotonic counters on separate
        // cache lines avoids one cross-core fetch_add/fetch_sub round trip for
        // every farm record; outstanding is their bounded difference.
        alignas(64) std::atomic<std::uint64_t> farm_messages{0U};
        alignas(64) std::atomic<std::uint64_t> retired_messages{0U};
        // Completion depth is sampled from the workers' single-writer publish
        // counters and this committer-owned take counter. It is diagnostic,
        // not a correctness gate, so telemetry cannot serialize all parsers
        // on one shared depth RMW.
        alignas(64) std::atomic<std::uint64_t>
            completion_taken_messages{0U};
        std::atomic<std::size_t> completion_high_water{0U};
        // Farm-only committed count. Snapshot derives inline and total
        // committed counts from the source's authoritative decoded counter.
        std::atomic<std::uint64_t> committed_messages{0U};
        std::atomic<std::uint64_t> history_batch_calls{0U};
        std::atomic<std::uint64_t> history_batched_messages{0U};
        std::atomic<std::size_t> history_batch_max{0U};
        std::atomic<std::uint64_t> discarded_messages{0U};
        std::atomic<std::uint64_t> reorder_wait_samples{0U};
        std::atomic<std::uint64_t> reorder_wait_total_ns{0U};
        std::atomic<std::uint64_t> reorder_wait_max_ns{0U};
        std::atomic<std::uint64_t> lease_wait_count{0U};
    };

    struct ParallelDecoderWorker final {
        std::thread thread;
        std::atomic<std::uint64_t> work_epoch{0U};
        alignas(64) std::atomic<bool> work_waiter_armed{false};
        std::atomic<bool> stop_requested{false};
        alignas(64) std::atomic<std::uint64_t> parsed_messages{0U};
        std::atomic<std::uint64_t> parse_failures{0U};
        std::array<std::atomic<std::uint64_t>,
                   market::kRealtimeHistorySourceCountV1>
            completed_messages_by_source{};
    };

    struct alignas(64) SourceMessageCounter final {
        std::atomic<std::uint64_t> value{0U};
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
        const market::RealtimeHistoryAppliedObservationV2& observation)
        noexcept {
        auto* const owner = static_cast<Impl*>(context);
        if (owner == nullptr) {
            return false;
        }
        if (owner->latency_collector_ != nullptr) {
            owner->latency_collector_->RecordApplied(observation);
        }
        return owner->CompleteAppliedSequence(
            observation.ingress_sequence);
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
            // Reserve every source queue, one current owner per decoder, the
            // callback owner, and a bounded safety margin before SDK Connect.
            // This prevents allocator entry and first-touch faults as a
            // source-local backlog grows. A
            // <=4096-byte wire message occupies at most the 8192-byte size
            // class; the byte cap keeps unusually large configured windows
            // from turning startup into an unbounded eager allocation.
            pool_config.prewarm_message_count = std::min(
                maximum_inflight_messages,
                kIngressPrewarmMaximumBlocks);
            // Every ingress path holds admission_mutex_ across Acquire. This
            // permits the pool's single-acquirer private cache while decoder
            // and History owners continue to recycle from many threads.
            pool_config.serialized_acquire = true;
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
                        config_.trade_date,
                        config_.completion_tracker_capacity);
            }

            if (realtime::ContiguousSequenceTrackerV2::Create(
                    config_.completion_tracker_capacity,
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

            market::RealtimeHistoryRuntimeConfigV1 history_config{};
            history_config.source_stream_ids = config_.source_stream_ids;
            history_config.worker_count = config_.store_worker_count;
            history_config.queue_capacity_per_source_worker =
                config_.store_queue_capacity_per_source_worker;
            history_config.intraday_store = config_.intraday_store;
            history_config.kline = config_.kline;
            history_config.kline.trade_date = config_.trade_date;
            history_config.runtime_state = config_.runtime_state;
            history_config.applied_record_sink =
                config_.applied_record_sink;
            history_config.applied_observer =
                &ObserveAppliedSequence;
            history_config.applied_observer_context = this;
            history_config.measure_applied_latency =
                latency_collector_ != nullptr;
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

            if (config_.factor_generation_enabled) {
                if (config_.factor_calculator == nullptr) {
                    config_.factor_calculator = std::make_shared<
                        factor::SnapshotLastPriceProjectionV1>();
                }
                factor::RealtimeFactorEngineConfigV1 factor_config{};
                factor_config.generation_runtime = history_.get();
                factor_config.calculator = config_.factor_calculator;
                const factor::RealtimeFactorEngineCreateErrorV1
                    factor_error = factor::RealtimeFactorEngineV1::Create(
                        std::move(factor_config), &factor_);
                if (factor_error !=
                    factor::RealtimeFactorEngineCreateErrorV1::kNone) {
                    SetDetail(
                        detail,
                        "factor engine create failed: " +
                            std::string(
                                factor::RealtimeFactorEngineCreateErrorNameV1(
                                    factor_error)));
                    return RealtimePipelineCreateErrorV1::
                        kFactorCreateFailed;
                }
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
                    decoder_config,
                    config_.parallel_decoder_worker_count,
                    config_.parallel_decoder_slots_per_source_worker);
                if (!lanes_[source]->decoder.configuration_valid()) {
                    SetDetailLiteral(detail, "market decoder configuration is invalid");
                    return RealtimePipelineCreateErrorV1::
                        kInvalidConfiguration;
                }
            }

            parallel_decoder_workers_.reserve(
                config_.parallel_decoder_worker_count);
            for (std::uint32_t worker = 0U;
                 worker < config_.parallel_decoder_worker_count;
                 ++worker) {
                parallel_decoder_workers_.push_back(
                    std::make_unique<ParallelDecoderWorker>());
            }

            if (!StartDecoderThreads()) {
                SetDetailLiteral(
                    detail, "cannot start decoder runtime threads");
                return RealtimePipelineCreateErrorV1::
                    kDecoderThreadStartFailed;
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
        if (!callback_gate_closed_.load(std::memory_order_acquire)) {
            if (config_.live_ingress_capture_sink == nullptr) {
                // Preserve the literal no-recovery callback hot path.
                static_cast<void>(
                    Ingest(message, callback_entry_pointer));
            } else {
                static_cast<void>(CaptureAndIngestLive(
                    message, callback_entry_pointer));
            }
        }
        if (active_callbacks_.fetch_sub(
                1U, std::memory_order_acq_rel) == 1U &&
            callback_waiter_armed_.load(std::memory_order_acquire)) {
            active_callbacks_.notify_all();
        }
    }

    [[nodiscard]] RealtimePipelineIngressErrorV1 CaptureAndIngestLive(
        const mdl::MDLMessage* message,
        const CallbackClockObservation* callback_entry) noexcept {
        realtime::OwnedIngressMessageInspectionV1 inspection{};
        const realtime::OwnedIngressMessageErrorV1 inspection_error =
            realtime::InspectOwnedIngressMessageV1(
                message,
                config_.maximum_sdk_message_bytes,
                &inspection);
        if (inspection_error ==
                realtime::OwnedIngressMessageErrorV1::
                    kUnsupportedMessage ||
            inspection_error !=
                realtime::OwnedIngressMessageErrorV1::kNone ||
            !inspection) {
            // Unsupported API/SYS callbacks are intentionally outside the
            // five-tuple recovery identity.  Every malformed/forbidden
            // callback still follows the ordinary fail-closed admission path.
            return Ingest(message, callback_entry).error;
        }

        CallbackClockObservation capture_clock{};
        if (callback_entry != nullptr) {
            capture_clock = *callback_entry;
        } else {
            capture_clock.valid =
                ReadClockNs(
                    CLOCK_REALTIME, &capture_clock.realtime_ns) &&
                ReadClockNs(
                    CLOCK_MONOTONIC, &capture_clock.monotonic_ns);
        }
        if (!capture_clock.valid) {
            return Ingest(message, &capture_clock).error;
        }

        realtime::RealtimeIngressCaptureInputV1 capture{};
        capture.inspection = &inspection;
        capture.recv_realtime_ns = capture_clock.realtime_ns;
        capture.recv_monotonic_ns = capture_clock.monotonic_ns;
        if (!config_.live_ingress_capture_sink->Capture(capture)) {
            {
                std::lock_guard<std::mutex> admission(admission_mutex_);
                ++rejected_messages_;
                ReportPipelineFailure(
                    "live_ingress_capture",
                    inspection.source_slot(),
                    0U,
                    inspection.vendor_head().sequence_id());
                TripFatalWithAdmissionLockHeld();
            }
            return RealtimePipelineIngressErrorV1::kCaptureFailed;
        }
        return Ingest(message, &capture_clock).error;
    }

    [[nodiscard]] RealtimePipelineIngressResultV1 Ingest(
        const mdl::MDLMessage* message,
        const CallbackClockObservation* callback_entry = nullptr,
        std::uint64_t additional_market_notices = 0U,
        bool wait_for_decoder_capacity = false,
        std::chrono::steady_clock::time_point admission_deadline =
            std::chrono::steady_clock::time_point::max()) noexcept {
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

        market::MarketMessageViewV1 admission_view{};
        admission_view.service_id = inspection.key().service_id;
        admission_view.service_version =
            inspection.key().service_version;
        admission_view.message_id = inspection.key().message_id;
        admission_view.body = inspection.body();

        market::ExactInstrumentKeyViewV2 extracted{};
        const market::MarketDecodeErrorV1 extraction_error =
            market::ExtractExactInstrumentKeyV2(
                admission_view,
                config_.decoder_limits.maximum_text_bytes,
                &extracted);
        if (extraction_error != market::MarketDecodeErrorV1::kNone) {
            last_decode_error_.store(
                static_cast<std::uint8_t>(extraction_error),
                std::memory_order_release);
            result.error =
                RealtimePipelineIngressErrorV1::kInstrumentKeyRejected;
            ReportPipelineFailure(
                "admission_instrument_key_extract",
                source_slot,
                0U,
                static_cast<std::uint64_t>(extraction_error));
            ++rejected_messages_;
            TripFatalWithAdmissionLockHeld();
            return result;
        }
        realtime::NativeSequenceDescriptorV1 native_descriptor{};
        bool native_sequence_tracked = false;
        if (config_.native_sequence_observation_sink != nullptr) {
            const realtime::NativeSequenceExtractErrorV1
                native_extract_error =
                    realtime::ExtractNativeSequenceV1(
                        inspection.key(),
                        inspection.body(),
                        &native_descriptor);
            native_sequence_tracked =
                native_extract_error ==
                realtime::NativeSequenceExtractErrorV1::kNone;
            if (!native_sequence_tracked &&
                native_extract_error !=
                    realtime::NativeSequenceExtractErrorV1::
                        kNotTracked) {
                config_.native_sequence_observation_sink
                    ->MarkNativeSequenceObservationFailure(
                        realtime::
                            NativeSequenceObservationFailureV1::
                                kExtraction,
                        inspection.key());
            }
        }
        if (!market::IsMainlandAShareSecurityIdV1(
                MainlandExchangeForMarket(extracted.market),
                extracted.security_id)) {
            if (native_sequence_tracked) {
                realtime::NativeSequenceObservationV1 observation{};
                observation.descriptor = native_descriptor;
                observation.message_key = inspection.key();
                observation.record_class =
                    realtime::
                        NativeSequenceRecoveryRecordClassV1::
                            kFiltered;
                config_.native_sequence_observation_sink
                    ->ObserveNativeSequence(observation);
            }
            result.error =
                RealtimePipelineIngressErrorV1::kFilteredNonAShare;
            ++filtered_messages_;
            ++filtered_messages_by_source_[source_slot];
            return result;
        }

        const market::InstrumentKeyViewV1 catalog_key{
            extracted.market,
            extracted.security_id_source,
            extracted.security_id};
        const market::DailyInstrumentCatalogLookupResultV2 lookup =
            config_.daily_catalog->Lookup(catalog_key);
        if (!lookup.known()) {
            result.error = RealtimePipelineIngressErrorV1::kCatalogMiss;
            ReportPipelineFailure(
                "daily_catalog_miss",
                source_slot,
                0U,
                static_cast<std::uint64_t>(lookup.error));
            ++rejected_messages_;
            TripFatalWithAdmissionLockHeld();
            return result;
        }
        const market::DailyInstrumentCatalogEntryV2& catalog_entry =
            *lookup.entry;
        market::DailyInstrumentIdentityViewV2 identity{};
        identity.key.market = catalog_entry.key.market;
        identity.key.security_id_source =
            catalog_entry.key.security_id_source;
        identity.key.security_id = catalog_entry.key.security_id;
        identity.instrument_id = catalog_entry.instrument_id;
        identity.ordinal = catalog_entry.ordinal;
        identity.quantity_unit =
            catalog_entry.metadata.quantity_unit;
        identity.security_type =
            catalog_entry.metadata.security_type;
        identity.asset_scope = catalog_entry.metadata.asset_scope;

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

        DecoderCommand command{};
        command.kind = CommandKind::kMessage;
        command.message = std::move(owned);
        command.identity = identity;
        command.additional_market_notices =
            additional_market_notices;
        if (latency_collector_ != nullptr) {
            command.callback_entry_monotonic_ns = monotonic_ns;
        }

        // The pooled copy transfers directly to its source FIFO. Commit runs
        // after the slot is fully constructed and before release-publishing
        // the queue tail. A fast decoder can therefore never complete a
        // sequence whose accepted frontier is not yet visible.
        const auto commit =
            [this,
             &result,
             &metadata,
             source_slot,
             mixed_tick_source,
             native_sequence_tracked,
             native_descriptor,
             message_key = inspection.key()](
                std::uint64_t queue_publish_monotonic_ns)
                noexcept {
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
                    if (native_sequence_tracked) {
                        realtime::NativeSequenceObservationV1
                            observation{};
                        observation.descriptor =
                            native_descriptor;
                        observation.message_key = message_key;
                        observation.record_class =
                            realtime::
                                NativeSequenceRecoveryRecordClassV1::
                                    kTarget;
                        observation.ingress_sequence =
                            metadata.global_ingress_sequence;
                        config_.native_sequence_observation_sink
                            ->ObserveNativeSequence(observation);
                    }
                    if (latency_collector_ != nullptr) {
                        latency_collector_->RecordDecoderPublish(
                            source_slot,
                            metadata.recv_monotonic_ns,
                            queue_publish_monotonic_ns);
                    }
                };
        bool published = false;
        while (lanes_[source_slot] != nullptr) {
            published = lanes_[source_slot]->queue.TryPushWithCommit(
                std::move(command), commit);
            if (published || !wait_for_decoder_capacity) {
                break;
            }
            if (fatal_.load(std::memory_order_acquire)) {
                break;
            }
            if (std::chrono::steady_clock::now() >=
                admission_deadline) {
                break;
            }
            // Only the serialized SDK-less external producer requests
            // bounded waiting. Ordinary SDK callbacks remain nonblocking.
            admission.unlock();
            std::this_thread::sleep_for(kAdmissionQueueRetryDelay);
            admission.lock();

            // Stop, a terminal cut, or an asynchronous History failure can
            // linearize while this bounded external/replay producer sleeps.
            // Revalidate under the same mutex that protects every producer
            // before retrying queue publication. This keeps the ordinary SDK
            // callback free of a per-message publication-gate RMW.
            const bool history_fatal =
                history_ != nullptr && history_->fatal();
            if (fatal_.load(std::memory_order_acquire) || history_fatal) {
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
        }
        if (!published) {
            result.error =
                RealtimePipelineIngressErrorV1::
                    kDecoderAdmissionFailed;
            ReportPipelineFailure(
                "decoder_queue_admission",
                source_slot,
                metadata.global_ingress_sequence,
                config_.decoder_queue_capacity_per_source);
            ++rejected_messages_;
            TripFatalWithAdmissionLockHeld();
            return result;
        }
        RequestProgressPublication();

        // Successful admission is complete only after the serialized callback
        // authority has been released.  The completion clocks below therefore
        // include the owned copy, source-queue commit, and admission section.
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

    [[nodiscard]] RealtimePipelineIngressResultV1 IngestExternal(
        const RealtimePipelineExternalIngressV1& input) noexcept {
        RealtimePipelineIngressResultV1 rejected{};
        if (!config_.external_ingress_enabled) {
            rejected.error = RealtimePipelineIngressErrorV1::kStopped;
            return rejected;
        }
        if (input.message == nullptr ||
            input.recv_realtime_ns == 0U ||
            input.recv_monotonic_ns == 0U ||
            input.admission_timeout <=
                std::chrono::nanoseconds::zero() ||
            input.admission_timeout > kMaximumCutTimeout) {
            rejected.error =
                input.message == nullptr
                    ? RealtimePipelineIngressErrorV1::kNullMessage
                    : RealtimePipelineIngressErrorV1::
                          kOwnedMessageRejected;
            rejected.owned_error =
                input.message == nullptr
                    ? realtime::OwnedIngressMessageErrorV1::kNullMessage
                    : realtime::OwnedIngressMessageErrorV1::
                          kInvalidMetadata;
            {
                std::lock_guard<std::mutex> admission(admission_mutex_);
                ++rejected_messages_;
                ReportPipelineFailure(
                    "external_ingress_input", 0U, 0U, 0U);
                TripFatalWithAdmissionLockHeld();
            }
            return rejected;
        }
        // A bounded retry releases admission_mutex_ so decoders and terminal
        // control can make progress. Retain one logical external producer
        // across that gap: otherwise two callers could precompute the same
        // next global/source sequence and later publish both stale commands.
        const std::lock_guard<std::mutex> external_owner(
            external_ingress_mutex_);
        CallbackClockObservation clocks{};
        clocks.realtime_ns = input.recv_realtime_ns;
        clocks.monotonic_ns = input.recv_monotonic_ns;
        clocks.valid = true;
        const auto now = std::chrono::steady_clock::now();
        const auto timeout = std::chrono::duration_cast<
            std::chrono::steady_clock::duration>(
            input.admission_timeout);
        return Ingest(
            input.message,
            &clocks,
            input.additional_market_notices,
            true,
            now + timeout);
    }

    [[nodiscard]] RealtimePipelineCutResultV1 Cut(
        std::chrono::nanoseconds timeout) noexcept {
        RealtimePipelineCutResultV1 result{};
        result.kline_enabled = config_.kline.enabled();
        result.factor_generation_enabled =
            config_.factor_generation_enabled;
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
        result.factor_generation_enabled =
            config_.factor_generation_enabled;
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
        result.factor_generation_enabled =
            config_.factor_generation_enabled;
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
                if (!PrepareGenerationFence(generation)) {
                    result.error =
                        RealtimePipelineCutErrorV1::
                            kFenceAdmissionFailed;
                    TripFatalWithAdmissionLockHeld();
                    return result;
                }
                // Each ring has Q logical message slots plus this one
                // reserved control slot. No wait is permitted while callback
                // admission is held, even when all Q message slots are full.
                for (std::uint8_t source = 0U;
                     source <
                         market::kRealtimeHistorySourceCountV1;
                     ++source) {
                    DecoderCommand fence{};
                    fence.kind = CommandKind::kGenerationFence;
                    fence.generation = generation;
                    if (lanes_[source] == nullptr ||
                        !lanes_[source]->queue.TryPushFence(
                            std::move(fence))) {
                        result.error =
                            RealtimePipelineCutErrorV1::
                                kFenceAdmissionFailed;
                        TripFatalWithAdmissionLockHeld();
                        return result;
                    }
                }
            }

            if (!WaitForFenceArrivals(generation, deadline)) {
                result.error =
                    RealtimePipelineCutErrorV1::kFenceArrivalFailed;
                TripFatal();
                return result;
            }
            if (cut_sequence != 0U &&
                !WaitAppliedThrough(cut_sequence, deadline)) {
                result.error =
                    RealtimePipelineCutErrorV1::kFenceArrivalFailed;
                TripFatal();
                return result;
            }

            std::shared_ptr<const
                market::DailyInstrumentCatalogSnapshotV2>
                catalog_snapshot;
            if (config_.runtime_state->AcquireSnapshot(
                    &catalog_snapshot) !=
                    market::InstrumentRuntimeStateErrorV2::kNone ||
                catalog_snapshot == nullptr) {
                result.error =
                    RealtimePipelineCutErrorV1::kWatermarkFailed;
                TripFatal();
                return result;
            }
            // All four lanes remain parked here. Every pre-cut message is
            // applied and no post-cut message has entered History, so this
            // availability snapshot is the exact cut state.
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

            RequestFenceSeal(generation);
            if (!WaitForFenceSeals(
                    generation, deadline, &result.generation_error)) {
                result.error =
                    RealtimePipelineCutErrorV1::kFenceSealFailed;
                TripFatal();
                return result;
            }
            // Release is one coordinator transition observed by every lane.
            // Waiting for departure also guarantees the reserved control slot
            // is reusable before a repeated generation can begin.
            if (!ReleaseFenceAndWaitForDepartures(
                    generation, deadline)) {
                result.error =
                    RealtimePipelineCutErrorV1::kFenceArrivalFailed;
                TripFatal();
                return result;
            }

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

            if (config_.factor_generation_enabled) {
                if (factor_ == nullptr) {
                    result.error =
                        RealtimePipelineCutErrorV1::kFactorPublishFailed;
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
            }
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
        for (const SourceMessageCounter& counter :
             decoded_messages_by_source_) {
            result.decoded_messages +=
                counter.value.load(std::memory_order_acquire);
        }
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
        for (std::size_t source = 0U;
             source < lanes_.size();
             ++source) {
            if (lanes_[source] != nullptr) {
                result.decoder_queues[source] =
                    lanes_[source]->queue.Snapshot();
                if (lanes_[source]->completion != nullptr) {
                    RealtimeParallelDecoderSourceSnapshotV1& target =
                        result.parallel_decoder.sources[source];
                    const std::uint64_t farm_committed =
                        lanes_[source]->committed_messages.load(
                            std::memory_order_acquire);
                    const std::uint64_t committed_total =
                        decoded_messages_by_source_[source].value.load(
                            std::memory_order_acquire);
                    target.inline_messages =
                        committed_total >= farm_committed
                            ? committed_total - farm_committed
                            : 0U;
                    target.farm_messages =
                        lanes_[source]->farm_messages.load(
                            std::memory_order_acquire);
                    target.dispatched_messages =
                        target.inline_messages + target.farm_messages;
                    target.completed_messages =
                        target.inline_messages;
                    for (const std::unique_ptr<ParallelDecoderWorker>&
                             worker : parallel_decoder_workers_) {
                        target.completed_messages +=
                            worker->completed_messages_by_source[source]
                                .load(std::memory_order_acquire);
                    }
                    target.committed_messages = committed_total;
                    target.history_batch_calls =
                        lanes_[source]->history_batch_calls.load(
                            std::memory_order_acquire);
                    target.history_batched_messages =
                        lanes_[source]->history_batched_messages.load(
                            std::memory_order_acquire);
                    target.history_batch_max =
                        lanes_[source]->history_batch_max.load(
                            std::memory_order_acquire);
                    target.discarded_messages =
                        lanes_[source]->discarded_messages.load(
                            std::memory_order_acquire);
                    target.farm_outstanding =
                        ParallelFarmOutstanding(*lanes_[source]);
                    target.committed_source_sequence = committed_total;
                    target.completion_capacity =
                        lanes_[source]->completion->capacity();
                    target.completion_depth =
                        ParallelCompletionDepth(
                            static_cast<std::uint8_t>(source),
                            *lanes_[source]);
                    target.completion_high_water =
                        lanes_[source]->completion_high_water.load(
                            std::memory_order_acquire);
                    target.completion_publish_failures =
                        lanes_[source]
                            ->completion->publish_failures();
                    target.reorder_wait_samples =
                        lanes_[source]->reorder_wait_samples.load(
                            std::memory_order_acquire);
                    target.reorder_wait_total_ns =
                        lanes_[source]->reorder_wait_total_ns.load(
                            std::memory_order_acquire);
                    target.reorder_wait_max_ns =
                        lanes_[source]->reorder_wait_max_ns.load(
                            std::memory_order_acquire);
                    target.lease_wait_count =
                        lanes_[source]->lease_wait_count.load(
                            std::memory_order_acquire);
                }
            }
        }
        result.parallel_decoder.enabled =
            config_.parallel_decoder_worker_count != 0U;
        result.parallel_decoder.idle_inline_enabled =
            config_.parallel_decoder_worker_count != 0U &&
            config_.parallel_decoder_idle_inline_enabled;
        result.parallel_decoder.worker_count =
            config_.parallel_decoder_worker_count;
        result.parallel_decoder.slots_per_source_worker =
            config_.parallel_decoder_slots_per_source_worker;
        for (std::size_t worker = 0U;
             worker < parallel_decoder_workers_.size();
             ++worker) {
            RealtimeParallelDecoderWorkerSnapshotV1& target =
                result.parallel_decoder.workers[worker];
            target.parsed_messages =
                parallel_decoder_workers_[worker]
                    ->parsed_messages.load(std::memory_order_acquire);
            target.parse_failures =
                parallel_decoder_workers_[worker]
                    ->parse_failures.load(std::memory_order_acquire);
            for (const std::unique_ptr<DecoderLane>& lane : lanes_) {
                if (lane != nullptr &&
                    worker < lane->parallel_shards.size()) {
                    const ParallelIssueQueue& issue =
                        lane->parallel_shards[worker]->issue;
                    target.issue_depth += issue.DepthSnapshot();
                    // TryPush samples the consumer head before publishing its
                    // tail, so a concurrent pop can make each shard HWM a
                    // conservative upper bound rather than an exact peak.
                    // Summing four independently timed shard upper bounds is
                    // also only a conservative simultaneous-depth bound.
                    target.issue_high_water += issue.HighWater();
                }
            }
        }
        result.accepting = accepting_.load(std::memory_order_acquire);
        result.fatal = fatal_.load(std::memory_order_acquire) ||
                       (history_ != nullptr && history_->fatal());
        result.stopped = stopped_.load(std::memory_order_acquire);
        result.trade_date_boundary_reached =
            trade_date_boundary_reached_.load(std::memory_order_acquire);
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

    [[nodiscard]] RealtimePipelineLiveStatusV1 LiveStatus()
        const noexcept {
        RealtimePipelineLiveStatusV1 result{};
        // Read downstream publication first. Its release chain originates
        // after accepted publication, so the final accepted acquire cannot
        // produce a torn applied > accepted pair.
        result.processing_progress.applied_sequence =
            applied_sequence_.load(std::memory_order_acquire);
        result.processing_progress.accepted_sequence =
            accepted_sequence_.load(std::memory_order_acquire);
        result.accepting = accepting_.load(std::memory_order_acquire);
        result.fatal = fatal();
        result.stopped = stopped_.load(std::memory_order_acquire);
        result.trade_date_boundary_reached =
            trade_date_boundary_reached_.load(std::memory_order_acquire);
        return result;
    }

    [[nodiscard]] bool LiveIngressHealthy() const noexcept {
        return LiveStatus().healthy();
    }

    [[nodiscard]] bool WaitAppliedThroughPrefix(
        std::uint64_t target_sequence,
        std::chrono::steady_clock::time_point deadline) noexcept {
        if (target_sequence == 0U) {
            return true;
        }
        // This control-plane API drains only a prefix that admission has
        // already committed. In particular, it must not turn a journal
        // frontier mistake into an unbounded wait for future live traffic.
        const std::uint64_t accepted =
            accepted_sequence_.load(std::memory_order_acquire);
        if (target_sequence > accepted || fatal()) {
            return false;
        }
        if (!WaitAppliedThrough(target_sequence, deadline)) {
            return false;
        }
        // WaitAppliedThrough preserves the original generation-cut behavior
        // and fast-success ordering. The public promotion wait additionally
        // fail-closes if a terminal History/Pipeline transition raced with
        // reaching the requested applied prefix.
        return !fatal();
    }

    [[nodiscard]] market::RealtimeHistoryGenerationErrorV1
    history_failure_error() const noexcept {
        return history_ == nullptr
                   ? market::RealtimeHistoryGenerationErrorV1::kNone
                   : history_->FailureError();
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
        RequestProgressStop();
        JoinProgressThread();
        stopped_.store(true, std::memory_order_release);
    }

    [[nodiscard]] bool CompleteAppliedSequence(
        std::uint64_t ingress_sequence) noexcept {
        const std::uint64_t accepted =
            accepted_sequence_.load(std::memory_order_acquire);
        if (ingress_sequence == 0U || ingress_sequence > accepted ||
            applied_tracker_ == nullptr) {
            ReportAppliedSequenceFailure(
                "precondition", ingress_sequence, accepted, 0U);
            MarkAsyncFatal();
            return false;
        }
        const realtime::ContiguousSequenceMarkErrorV2 mark_error =
            applied_tracker_->MarkCompleted(ingress_sequence);
        if (mark_error !=
            realtime::ContiguousSequenceMarkErrorV2::kNone) {
            ReportAppliedSequenceFailure(
                "tracker_mark",
                ingress_sequence,
                accepted,
                static_cast<std::uint64_t>(mark_error));
            MarkAsyncFatal();
            return false;
        }
        const std::uint64_t completed =
            applied_tracker_->contiguous_sequence();
        // MarkCompleted serializes concurrent completions. While this caller
        // waits for that tracker lock, a later admitted sequence can complete
        // and become part of the same newly contiguous prefix. Re-read the
        // monotonic accepted frontier after the tracker operation; comparing
        // against the pre-lock sample would falsely fail a valid prefix.
        const std::uint64_t accepted_after_mark =
            accepted_sequence_.load(std::memory_order_acquire);
        if (completed > accepted_after_mark) {
            ReportAppliedSequenceFailure(
                "completed_past_accepted",
                ingress_sequence,
                accepted_after_mark,
                completed);
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
        if (applied_progress_waiters_.load(
                std::memory_order_relaxed) != 0U) {
            applied_progress_cv_.notify_all();
        }
        RequestProgressPublication();
        if (fatal_.load(std::memory_order_acquire)) {
            ReportAppliedSequenceFailure(
                "post_completion_fatal",
                ingress_sequence,
                accepted,
                completed);
            return false;
        }
        return true;
    }

    void ReportAppliedSequenceFailure(
        const char* reason,
        std::uint64_t ingress_sequence,
        std::uint64_t accepted_sequence,
        std::uint64_t detail) noexcept {
        if (applied_sequence_failure_reported_.test_and_set(
                std::memory_order_relaxed)) {
            return;
        }
        realtime::ContiguousSequenceTrackerSnapshotV2 tracker{};
        if (applied_tracker_ != nullptr) {
            tracker = applied_tracker_->Snapshot();
        }
        std::fprintf(
            stderr,
            "l2flow-pipeline: first applied-sequence failure reason=%s "
            "ingress_sequence=%llu accepted_sequence=%llu "
            "published_applied=%llu tracker_capacity=%zu "
            "tracker_contiguous=%llu tracker_highest=%llu "
            "tracker_pending=%llu tracker_failed_marks=%llu "
            "detail=%llu\n",
            reason == nullptr ? "unknown" : reason,
            static_cast<unsigned long long>(ingress_sequence),
            static_cast<unsigned long long>(accepted_sequence),
            static_cast<unsigned long long>(
                applied_sequence_.load(std::memory_order_acquire)),
            tracker.capacity,
            static_cast<unsigned long long>(
                tracker.contiguous_sequence),
            static_cast<unsigned long long>(
                tracker.highest_observed_sequence),
            static_cast<unsigned long long>(
                tracker.pending_sequences),
            static_cast<unsigned long long>(tracker.failed_marks),
            static_cast<unsigned long long>(detail));
    }

    [[nodiscard]] bool PrepareGenerationFence(
        std::uint64_t generation) noexcept {
        if (generation == 0U) {
            return false;
        }
        try {
            std::lock_guard<std::mutex> lock(generation_fence_mutex_);
            if (active_fence_generation_ != 0U) {
                return false;
            }
            active_fence_generation_ = generation;
            seal_requested_generation_ = 0U;
            release_fence_generation_ = 0U;
            for (std::size_t source = 0U;
                 source < fence_arrived_generation_.size();
                 ++source) {
                fence_seal_errors_[source] =
                    market::RealtimeHistoryGenerationErrorV1::kNone;
            }
            return true;
        } catch (...) {
            return false;
        }
    }

    [[nodiscard]] bool ParkAtGenerationFence(
        std::uint8_t source,
        std::uint64_t generation) noexcept {
        if (source >= market::kRealtimeHistorySourceCountV1 ||
            generation == 0U || lanes_[source] == nullptr) {
            return false;
        }
        try {
            std::unique_lock<std::mutex> lock(
                generation_fence_mutex_);
            if (active_fence_generation_ != generation ||
                fence_arrived_generation_[source] == generation) {
                lock.unlock();
                lanes_[source]->queue.CompleteFence();
                return false;
            }
            fence_arrived_generation_[source] = generation;
            generation_fence_cv_.notify_all();
            while (!fatal_.load(std::memory_order_acquire) &&
                   seal_requested_generation_ != generation) {
                // A parked lane is a control-plane state, not a hot data
                // path. Periodically rechecking the protected epoch prevents
                // one missed or coalesced condition-variable wake from
                // stranding every source indefinitely after the coordinator
                // has already published the seal transition.
                static_cast<void>(generation_fence_cv_.wait_for(
                    lock, std::chrono::milliseconds(1)));
            }
            if (fatal_.load(std::memory_order_acquire)) {
                lock.unlock();
                lanes_[source]->queue.CompleteFence();
                lock.lock();
                fence_departed_generation_[source] = generation;
                generation_fence_cv_.notify_all();
                return false;
            }
            lock.unlock();

            const market::RealtimeHistoryGenerationErrorV1 seal_error =
                history_->SealSource(source, generation);

            lock.lock();
            fence_seal_errors_[source] = seal_error;
            fence_sealed_generation_[source] = generation;
            generation_fence_cv_.notify_all();
            while (!fatal_.load(std::memory_order_acquire) &&
                   release_fence_generation_ != generation) {
                static_cast<void>(generation_fence_cv_.wait_for(
                    lock, std::chrono::milliseconds(1)));
            }
            const bool released =
                !fatal_.load(std::memory_order_acquire) &&
                release_fence_generation_ == generation &&
                seal_error ==
                    market::RealtimeHistoryGenerationErrorV1::kNone;
            lock.unlock();
            lanes_[source]->queue.CompleteFence();
            lock.lock();
            fence_departed_generation_[source] = generation;
            generation_fence_cv_.notify_all();
            return released;
        } catch (...) {
            lanes_[source]->queue.CompleteFence();
            return false;
        }
    }

    template <typename Array>
    [[nodiscard]] static bool AllFenceEpoch(
        const Array& values,
        std::uint64_t generation) noexcept {
        return std::all_of(
            values.begin(),
            values.end(),
            [generation](std::uint64_t value) noexcept {
                return value == generation;
            });
    }

    [[nodiscard]] bool WaitForFenceArrivals(
        std::uint64_t generation,
        std::chrono::steady_clock::time_point deadline) noexcept {
        try {
            std::unique_lock<std::mutex> lock(
                generation_fence_mutex_);
            const auto ready = [this, generation] {
                return fatal_.load(std::memory_order_acquire) ||
                       AllFenceEpoch(
                           fence_arrived_generation_, generation);
            };
            return (ready() ||
                    generation_fence_cv_.wait_until(
                        lock, deadline, ready)) &&
                   !fatal_.load(std::memory_order_acquire) &&
                   AllFenceEpoch(
                       fence_arrived_generation_, generation);
        } catch (...) {
            return false;
        }
    }

    void RequestFenceSeal(std::uint64_t generation) noexcept {
        try {
            std::lock_guard<std::mutex> lock(generation_fence_mutex_);
            if (active_fence_generation_ == generation) {
                seal_requested_generation_ = generation;
            }
            generation_fence_cv_.notify_all();
        } catch (...) {
            MarkAsyncFatal();
        }
    }

    [[nodiscard]] bool WaitForFenceSeals(
        std::uint64_t generation,
        std::chrono::steady_clock::time_point deadline,
        market::RealtimeHistoryGenerationErrorV1* error) noexcept {
        if (error == nullptr) {
            return false;
        }
        *error = market::RealtimeHistoryGenerationErrorV1::kNone;
        try {
            std::unique_lock<std::mutex> lock(
                generation_fence_mutex_);
            const auto ready = [this, generation] {
                return fatal_.load(std::memory_order_acquire) ||
                       AllFenceEpoch(
                           fence_sealed_generation_, generation);
            };
            if ((!ready() &&
                 !generation_fence_cv_.wait_until(
                     lock, deadline, ready)) ||
                fatal_.load(std::memory_order_acquire) ||
                !AllFenceEpoch(
                    fence_sealed_generation_, generation)) {
                return false;
            }
            for (const auto seal_error : fence_seal_errors_) {
                if (seal_error !=
                    market::RealtimeHistoryGenerationErrorV1::kNone) {
                    *error = seal_error;
                    return false;
                }
            }
            return true;
        } catch (...) {
            return false;
        }
    }

    [[nodiscard]] bool ReleaseFenceAndWaitForDepartures(
        std::uint64_t generation,
        std::chrono::steady_clock::time_point deadline) noexcept {
        try {
            std::unique_lock<std::mutex> lock(
                generation_fence_mutex_);
            if (active_fence_generation_ != generation ||
                !AllFenceEpoch(
                    fence_sealed_generation_, generation)) {
                return false;
            }
            release_fence_generation_ = generation;
            generation_fence_cv_.notify_all();
            const auto ready = [this, generation] {
                return fatal_.load(std::memory_order_acquire) ||
                       AllFenceEpoch(
                           fence_departed_generation_, generation);
            };
            if ((!ready() &&
                 !generation_fence_cv_.wait_until(
                     lock, deadline, ready)) ||
                fatal_.load(std::memory_order_acquire) ||
                !AllFenceEpoch(
                    fence_departed_generation_, generation)) {
                return false;
            }
            active_fence_generation_ = 0U;
            return true;
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
        applied_progress_waiters_.fetch_add(
            1U, std::memory_order_acq_rel);
        struct AppliedWaiterGuard final {
            std::atomic<std::size_t>* count = nullptr;
            ~AppliedWaiterGuard() {
                count->fetch_sub(1U, std::memory_order_acq_rel);
            }
        } waiter_guard{&applied_progress_waiters_};
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
                // applied observer, preventing decoder shutdown deadlock.
                static_cast<void>(applied_progress_cv_.wait_for(
                    lock, std::chrono::milliseconds(1)));
            }
        } catch (...) {
            return false;
        }
    }

    void RequestProgressPublication() noexcept {
        if (config_.processing_progress_sink != nullptr &&
            !progress_wake_pending_.load(std::memory_order_relaxed) &&
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
            ReportPipelineFailure(
                "processing_progress_sink",
                0U,
                progress.accepted_sequence,
                progress.applied_sequence);
            MarkAsyncFatal();
            return false;
        }
        return true;
    }

    void MarkAsyncFatal() noexcept {
        // Fatal admission and callback commit share one linearization mutex.
        // Without it an asynchronous failure could publish fatal=true while
        // an already-active callback subsequently commits a sequence whose
        // decoder is then stopped before applying it.
        TripFatal();
    }

    [[nodiscard]] bool WaitAppliedThrough(
        std::uint64_t target_sequence,
        std::chrono::steady_clock::time_point deadline) noexcept {
        if (applied_sequence_.load(std::memory_order_acquire) >=
            target_sequence) {
            return true;
        }
        applied_progress_waiters_.fetch_add(
            1U, std::memory_order_acq_rel);
        struct AppliedWaiterGuard final {
            std::atomic<std::size_t>* count = nullptr;
            ~AppliedWaiterGuard() {
                count->fetch_sub(1U, std::memory_order_acq_rel);
            }
        } waiter_guard{&applied_progress_waiters_};
        try {
            std::unique_lock<std::mutex> lock(applied_progress_mutex_);
            for (;;) {
                if (applied_sequence_.load(std::memory_order_acquire) >=
                    target_sequence) {
                    return true;
                }
                if (fatal_.load(std::memory_order_acquire)) {
                    return false;
                }
                const auto now = std::chrono::steady_clock::now();
                if (now >= deadline) {
                    return false;
                }
                // Applied completion deliberately does not take this global
                // mutex on the History hot path. A notify can therefore land
                // between this wait's predicate check and its atomic sleep
                // transition. Bound that lost-notify case without adding a
                // cross-lane completion lock.
                static_cast<void>(applied_progress_cv_.wait_until(
                    lock,
                    std::min(
                        deadline,
                        now + std::chrono::milliseconds(1))));
            }
        } catch (...) {
            return false;
        }
    }

    [[nodiscard]] bool ValidateConfiguration(
        bool factory_is_test_override) const noexcept {
        std::size_t applied_window = 0U;
        std::size_t maximum_inflight_messages = 0U;
        if (config_.daily_catalog == nullptr ||
            config_.runtime_state == nullptr ||
            !config_.daily_catalog->coverage_complete() ||
            config_.daily_catalog->market_scope() !=
                market::kDailyCatalogMainlandScopeV2 ||
            config_.daily_catalog->instrument_count() == 0U ||
            config_.runtime_state->capacity() !=
                config_.daily_catalog->instrument_count() ||
            config_.runtime_state->session_epoch() !=
                config_.daily_catalog->session_epoch() ||
            config_.runtime_state->trade_date() !=
                config_.daily_catalog->trade_date() ||
            config_.runtime_state->catalog_version() !=
                config_.daily_catalog->catalog_version() ||
            l2flow::common::IsZeroIdentity(config_.run_id) ||
            config_.trade_date == 0U ||
            config_.trade_date != config_.daily_catalog->trade_date() ||
            config_.maximum_sdk_message_bytes < sdk::kVendorHeadBytes ||
            config_.maximum_sdk_message_bytes >
                realtime::kOwnedIngressMaximumMessageBytesV1 ||
            config_.decoder_queue_capacity_per_source == 0U ||
            config_.decoder_queue_capacity_per_source >
                std::numeric_limits<std::size_t>::max() - 2U ||
            config_.parallel_decoder_worker_count >
                kRealtimeParallelDecoderMaximumWorkersV1 ||
            (config_.parallel_decoder_worker_count != 0U &&
             (config_.parallel_decoder_slots_per_source_worker == 0U ||
              config_.parallel_decoder_slots_per_source_worker >
                  kMaximumParallelDecoderSlotsPerShard)) ||
            config_.completion_tracker_capacity < 2U ||
            config_.tick_ring_capacity < 2U ||
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
                &maximum_inflight_messages)) {
            return false;
        }
        std::shared_ptr<const market::DailyInstrumentCatalogSnapshotV2>
            initial_state;
        if (config_.runtime_state->AcquireSnapshot(&initial_state) !=
                market::InstrumentRuntimeStateErrorV2::kNone ||
            initial_state == nullptr ||
            initial_state->catalog_scope() !=
                market::InstrumentCatalogScopeV2::
                    kDeclaredDailyAShare ||
            !initial_state->coverage_complete() ||
            initial_state->trade_date() != config_.trade_date ||
            initial_state->catalog_version() !=
                config_.daily_catalog->catalog_version() ||
            initial_state->catalog_digest() !=
                config_.daily_catalog->catalog_digest() ||
            initial_state->bound_count() !=
                config_.daily_catalog->instrument_count() ||
            initial_state->available_count() != 0U ||
            initial_state->snapshot_available_count() != 0U ||
            initial_state->tick_available_count() != 0U ||
            initial_state->factor_eligible_count() != 0U) {
            return false;
        }
        for (const market::DailyInstrumentCatalogEntryV2& entry :
             config_.daily_catalog->entries()) {
            if (entry.key.security_id.size() >
                    config_.decoder_limits.maximum_text_bytes ||
                entry.key.security_id_source.size() >
                    config_.decoder_limits.maximum_text_bytes) {
                return false;
            }
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
        if (config_.live_ingress_capture_sink != nullptr &&
            (!config_.sdk.enabled ||
             config_.external_ingress_enabled)) {
            return false;
        }
        if (config_.external_ingress_enabled &&
            (config_.sdk.enabled ||
             config_.live_ingress_capture_sink != nullptr)) {
            return false;
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
            if (config_.parallel_decoder_worker_count != 0U) {
                // Start only the four source owners. Parse workers and
                // ordered committers are created when a source Pop first
                // observes remaining ring occupancy at its local threshold,
                // so the idle path has the same runnable decoder-thread count
                // as the legacy topology.
                for (std::uint8_t source = 0U;
                     source < market::kRealtimeHistorySourceCountV1;
                     ++source) {
                    lanes_[source]->thread = std::thread([this, source] {
                        ParallelDispatchLoop(source);
                    });
                }
                decoder_threads_started_ = true;
                return true;
            }
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
            for (const std::unique_ptr<DecoderLane>& lane : lanes_) {
                if (lane != nullptr && !lane->thread.joinable()) {
                    lane->dispatch_done.store(
                        true, std::memory_order_release);
                    if (lane->completion != nullptr) {
                        lane->completion->WakeAll();
                    }
                }
            }
            JoinDecoderThreads();
            return false;
        }
    }

    [[nodiscard]] bool EnsureParallelFarmStarted() noexcept {
        if (parallel_farm_started_.load(std::memory_order_acquire)) {
            return true;
        }
        try {
            std::call_once(parallel_farm_start_once_, [this]() noexcept {
                try {
                    for (std::uint32_t worker = 0U;
                         worker < config_.parallel_decoder_worker_count;
                         ++worker) {
                        parallel_decoder_workers_[worker]->thread =
                            std::thread([this, worker] {
                                ParallelDecoderWorkerLoop(worker);
                            });
                    }
                    for (std::uint8_t source = 0U;
                         source < market::kRealtimeHistorySourceCountV1;
                         ++source) {
                        lanes_[source]->commit_thread =
                            std::thread([this, source] {
                                ParallelCommitLoop(source);
                            });
                    }
                    parallel_farm_started_.store(
                        true, std::memory_order_release);
                } catch (...) {
                    parallel_farm_start_failed_.store(
                        true, std::memory_order_release);
                }
            });
        } catch (...) {
            parallel_farm_start_failed_.store(
                true, std::memory_order_release);
        }
        return parallel_farm_started_.load(
                   std::memory_order_acquire) &&
               !parallel_farm_start_failed_.load(
                   std::memory_order_acquire);
    }

    [[nodiscard]] std::size_t ParallelFarmActivationDepth() const noexcept {
        if (config_.parallel_decoder_farm_activation_queue_depth == 0U) {
            return 0U;
        }
        const std::size_t queue_capacity =
            config_.decoder_queue_capacity_per_source;
        const std::size_t capacity_limited_threshold =
            queue_capacity -
            std::max<std::size_t>(1U, queue_capacity / 4U);
        return std::min(
            config_.parallel_decoder_farm_activation_queue_depth,
            capacity_limited_threshold);
    }

    [[nodiscard]] bool ParallelFarmPressure(
        std::size_t remaining_ring_depth) const noexcept {
        const std::size_t single_source_threshold =
            ParallelFarmActivationDepth();
        if (single_source_threshold == 0U) {
            return true;
        }
        // The four source owners already decode independently on four cores.
        // Arming the farm from aggregate multi-source pressure replaces that
        // useful parallelism with two extra handoffs per record and can make a
        // balanced stream slower. Reserve the farm for a source that cannot
        // keep up with its own FIFO.
        return remaining_ring_depth >= single_source_threshold;
    }

    [[nodiscard]] static std::size_t ParallelFarmOutstanding(
        const DecoderLane& lane) noexcept {
        const std::uint64_t retired = lane.retired_messages.load(
            std::memory_order_acquire);
        // The committer can only retire a task after its issue-queue and
        // completion publication. Sample the single-writer issued frontier
        // last so an observed retirement cannot be compared with an older
        // issued value.
        const std::uint64_t issued = lane.farm_messages.load(
            std::memory_order_acquire);
        if (retired > issued) {
            return std::numeric_limits<std::size_t>::max();
        }
        if (retired == issued) {
            return 0U;
        }
        const std::uint64_t difference = issued - retired;
        return difference >
                       static_cast<std::uint64_t>(
                           std::numeric_limits<std::size_t>::max())
                   ? std::numeric_limits<std::size_t>::max()
                   : static_cast<std::size_t>(difference);
    }

    [[nodiscard]] static std::size_t ParallelWorkerForSourceSequence(
        std::uint8_t source,
        std::uint64_t source_sequence,
        std::uint32_t worker_count) noexcept {
        if (source_sequence == 0U || worker_count == 0U) {
            return 0U;
        }
        return static_cast<std::size_t>(
            (source_sequence - 1U + source) % worker_count);
    }

    [[nodiscard]] std::size_t ParallelCompletionDepth(
        std::uint8_t source,
        const DecoderLane& lane) const noexcept {
        std::uint64_t published = 0U;
        for (const std::unique_ptr<ParallelDecoderWorker>& worker :
             parallel_decoder_workers_) {
            published += worker->completed_messages_by_source[source].load(
                std::memory_order_acquire);
        }
        const std::uint64_t taken =
            lane.completion_taken_messages.load(
                std::memory_order_acquire);
        if (taken >= published) {
            return 0U;
        }
        const std::uint64_t difference = published - taken;
        const std::size_t capacity = lane.completion->capacity();
        return difference >= static_cast<std::uint64_t>(capacity)
                   ? capacity
                   : static_cast<std::size_t>(difference);
    }

    void SampleParallelCompletionDepth(
        std::uint8_t source,
        DecoderLane* lane) noexcept {
        if (lane == nullptr || lane->completion == nullptr) {
            return;
        }
        const std::size_t depth =
            ParallelCompletionDepth(source, *lane);
        const std::size_t high_water =
            lane->completion_high_water.load(
                std::memory_order_relaxed);
        if (depth > high_water) {
            lane->completion_high_water.store(
                depth, std::memory_order_relaxed);
        }
    }

    void ParallelDispatchLoop(std::uint8_t source) noexcept {
        DecoderLane& lane = *lanes_[source];
        DecoderCommand command{};
        std::size_t remaining_ring_depth = 0U;
        bool farm_interval_active = false;
        while (lane.queue.Pop(&command, &remaining_ring_depth)) {
            if (command.kind == CommandKind::kGenerationFence) {
                const std::uint64_t through =
                    lane.last_dispatched_source_sequence.load(
                        std::memory_order_acquire);
                const bool farm_committed =
                    lane.farm_messages.load(
                        std::memory_order_acquire) == 0U ||
                    WaitForParallelCommit(source, through);
                if ((!farm_committed ||
                     !ParkAtGenerationFence(
                         source, command.generation)) &&
                    !fatal_.load(std::memory_order_acquire)) {
                    TripFatal();
                }
                farm_interval_active = false;
                command = DecoderCommand{};
                continue;
            }
            if (fatal_.load(std::memory_order_acquire)) {
                command = DecoderCommand{};
                continue;
            }
            if (!command.message ||
                command.message->source_slot() != source ||
                !WaitForAppliedDispatchWindow(
                    command.message->global_ingress_sequence())) {
                ReportPipelineFailure(
                    "parallel_dispatch_window",
                    source,
                    command.message
                        ? command.message->global_ingress_sequence()
                        : 0U,
                    applied_window_capacity_);
                TripFatal();
                command = DecoderCommand{};
                continue;
            }

            const std::uint64_t source_sequence =
                command.message->source_sequence();
            // Preserve the legacy low-latency path for bounded bursts. The
            // authoritative source owner performs the existing DecodeOne
            // path until observed pressure crosses the configured threshold.
            // It returns inline after the farm drains and pressure subsides.
            bool decode_inline = false;
            if (config_.parallel_decoder_idle_inline_enabled &&
                config_.parallel_decoder_farm_activation_queue_depth !=
                    0U) {
                if (!farm_interval_active) {
                    decode_inline =
                        !ParallelFarmPressure(remaining_ring_depth);
                    farm_interval_active = !decode_inline;
                } else if (ParallelFarmOutstanding(lane) == 0U &&
                           !ParallelFarmPressure(
                               remaining_ring_depth)) {
                    farm_interval_active = false;
                    decode_inline = true;
                }
            }
            if (decode_inline &&
                !fatal_.load(std::memory_order_acquire)) {
                std::uint64_t dequeue_monotonic_ns = 0U;
                if (latency_collector_ != nullptr) {
                    static_cast<void>(ReadClockNs(
                        CLOCK_MONOTONIC, &dequeue_monotonic_ns));
                }
                std::uint64_t decode_start_monotonic_ns = 0U;
                if (latency_collector_ != nullptr) {
                    static_cast<void>(ReadClockNs(
                        CLOCK_MONOTONIC,
                        &decode_start_monotonic_ns));
                }
                DecoderTimingObservation timing{};
                const bool decoded = DecodeOne(
                    source,
                    command.message,
                    command.identity,
                    command.additional_market_notices,
                    latency_collector_ != nullptr ? &timing : nullptr);
                if (latency_collector_ != nullptr) {
                    latency_collector_->RecordDecoderWork(
                        source,
                        command.queue_publish_monotonic_ns,
                        dequeue_monotonic_ns,
                        decode_start_monotonic_ns,
                        timing.decode_complete_monotonic_ns,
                        timing.history_submit_complete_monotonic_ns,
                        command.queue_publish_monotonic_ns != 0U &&
                            dequeue_monotonic_ns != 0U &&
                            dequeue_monotonic_ns >=
                                command.queue_publish_monotonic_ns,
                        decoded &&
                            decode_start_monotonic_ns != 0U &&
                            timing.decode_complete_clock_valid,
                        decoded &&
                            timing.decode_complete_clock_valid &&
                            timing.history_submit_complete_clock_valid);
                }
                if (!decoded) {
                    TripFatal();
                    command = DecoderCommand{};
                    continue;
                }
                command = DecoderCommand{};
                continue;
            }
            if (!EnsureParallelFarmStarted()) {
                ReportPipelineFailure(
                    "parallel_farm_start",
                    source,
                    command.message
                        ? command.message->global_ingress_sequence()
                        : 0U,
                    config_.parallel_decoder_worker_count);
                TripFatal();
                command = DecoderCommand{};
                continue;
            }
            if (ParallelFarmOutstanding(lane) == 0U) {
                // Every prior item on this source completed through the
                // inline owner. Initialize/advance the farm-only ordering
                // frontiers once when crossing into a new farm interval.
                lane.last_dispatched_source_sequence.store(
                    source_sequence - 1U, std::memory_order_release);
                lane.committed_source_sequence.store(
                    source_sequence - 1U, std::memory_order_release);
            }
            farm_interval_active = true;
            const std::size_t worker =
                ParallelWorkerForSourceSequence(
                    source,
                    source_sequence,
                    config_.parallel_decoder_worker_count);
            const std::size_t slot = static_cast<std::size_t>(
                ((source_sequence - 1U) /
                 config_.parallel_decoder_worker_count) %
                config_.parallel_decoder_slots_per_source_worker);
            ParallelDecodeShard& shard =
                *lane.parallel_shards[worker];
            ParallelDecodeTask* const task = shard.Acquire(
                slot, fatal_, &lane.lease_wait_count);
            if (task == nullptr) {
                if (!fatal_.load(std::memory_order_acquire)) {
                    ReportPipelineFailure(
                        "parallel_decode_lease",
                        source,
                        command.message->global_ingress_sequence(),
                        worker);
                    TripFatal();
                }
                command = DecoderCommand{};
                continue;
            }
            if (fatal_.load(std::memory_order_acquire)) {
                task->ResetAndRelease(
                    &shard.release_epoch,
                    &shard.release_waiter_armed);
                command = DecoderCommand{};
                continue;
            }
            task->source_slot = source;
            task->global_ingress_sequence =
                command.message->global_ingress_sequence();
            task->source_sequence = source_sequence;
            task->tick_stream_sequence =
                command.message->tick_stream_sequence();
            task->command = std::move(command);
            ParallelDecoderWorker& owner =
                *parallel_decoder_workers_[worker];
            IncrementSingleWriterCounter(&lane.farm_messages);
            if (!shard.issue.TryPush(task)) {
                DecrementSingleWriterCounter(&lane.farm_messages);
                const std::uint64_t ingress_sequence =
                    task->command.message
                        ? task->command.message->global_ingress_sequence()
                        : 0U;
                task->ResetAndRelease(
                    &shard.release_epoch,
                    &shard.release_waiter_armed);
                ReportPipelineFailure(
                    "parallel_issue_queue",
                    source,
                    ingress_sequence,
                    worker);
                TripFatal();
                command = DecoderCommand{};
                continue;
            }
            lane.last_dispatched_source_sequence.store(
                source_sequence, std::memory_order_release);
            if (owner.work_waiter_armed.exchange(
                    false, std::memory_order_acq_rel)) {
                owner.work_epoch.fetch_add(
                    1U, std::memory_order_release);
                owner.work_epoch.notify_one();
            }
            command = DecoderCommand{};
        }
        lane.dispatch_done.store(true, std::memory_order_release);
        if (lane.completion != nullptr) {
            lane.completion->WakeAll();
        }
    }

    [[nodiscard]] bool WaitForParallelCommit(
        std::uint8_t source,
        std::uint64_t sequence) noexcept {
        if (source >= market::kRealtimeHistorySourceCountV1 ||
            lanes_[source] == nullptr ||
            lanes_[source]->completion == nullptr) {
            return false;
        }
        DecoderLane& lane = *lanes_[source];
        const auto commit_and_leases_ready = [&]() noexcept {
            return lane.committed_source_sequence.load(
                       std::memory_order_acquire) >= sequence &&
                   ParallelFarmOutstanding(lane) == 0U;
        };
        while (!fatal_.load(std::memory_order_acquire) &&
               !commit_and_leases_ready()) {
            const std::uint64_t observed =
                lane.commit_progress_epoch.load(
                    std::memory_order_acquire);
            const bool already_armed =
                lane.commit_progress_waiter_armed.exchange(
                    true, std::memory_order_acq_rel);
            if (already_armed) {
                TerminateInvariant("parallel_commit_waiter_rearmed");
            }
            const bool wait_required =
                !commit_and_leases_ready() &&
                !fatal_.load(std::memory_order_acquire);
            if (wait_required) {
                lane.commit_progress_epoch.wait(
                    observed, std::memory_order_acquire);
                // Terminal progress may precede arming and make wait return
                // immediately. Always leave the single-waiter flag clear.
                static_cast<void>(
                    lane.commit_progress_waiter_armed.exchange(
                        false, std::memory_order_acq_rel));
            } else {
                // A signal that precedes arming is observed through the
                // exchange above. In that case no signal owns this armed bit,
                // so the waiter must explicitly disarm before returning.
                static_cast<void>(
                    lane.commit_progress_waiter_armed.exchange(
                        false, std::memory_order_acq_rel));
            }
        }
        return !fatal_.load(std::memory_order_acquire) &&
               commit_and_leases_ready();
    }

    static void SignalParallelCommitProgress(
        DecoderLane* lane,
        bool terminal) noexcept {
        if (lane == nullptr) {
            return;
        }
        const bool waiter =
            lane->commit_progress_waiter_armed.exchange(
                false, std::memory_order_acq_rel);
        if (!waiter && !terminal) {
            return;
        }
        lane->commit_progress_epoch.fetch_add(
            1U, std::memory_order_release);
        if (terminal) {
            lane->commit_progress_epoch.notify_all();
        } else {
            lane->commit_progress_epoch.notify_one();
        }
    }

    [[nodiscard]] bool TryPopParallelWorkerTask(
        std::uint32_t worker,
        std::uint8_t* next_source,
        std::uint8_t* output_source,
        ParallelDecodeTask** output) noexcept {
        if (next_source == nullptr || output_source == nullptr ||
            output == nullptr ||
            worker >= parallel_decoder_workers_.size()) {
            return false;
        }
        for (std::size_t offset = 0U;
             offset < market::kRealtimeHistorySourceCountV1;
             ++offset) {
            const std::uint8_t source = static_cast<std::uint8_t>(
                (static_cast<std::size_t>(*next_source) + offset) %
                market::kRealtimeHistorySourceCountV1);
            if (lanes_[source]->parallel_shards[worker]
                    ->issue.TryPop(output)) {
                *output_source = source;
                *next_source = static_cast<std::uint8_t>(
                    (static_cast<std::size_t>(source) + 1U) %
                    market::kRealtimeHistorySourceCountV1);
                return true;
            }
        }
        return false;
    }

    void ParallelDecoderWorkerLoop(std::uint32_t worker) noexcept {
        ParallelDecoderWorker& owner =
            *parallel_decoder_workers_[worker];
        std::uint8_t next_source = 0U;
        for (;;) {
            ParallelDecodeTask* task = nullptr;
            std::uint8_t popped_source =
                static_cast<std::uint8_t>(
                    market::kRealtimeHistorySourceCountV1);
            if (!TryPopParallelWorkerTask(
                    worker, &next_source, &popped_source, &task)) {
                if (owner.stop_requested.load(
                        std::memory_order_acquire)) {
                    // Dispatchers are joined before stop is published. Rescan
                    // after the acquire so their final issue publication
                    // cannot be stranded by an earlier empty observation.
                    if (!TryPopParallelWorkerTask(
                            worker,
                            &next_source,
                            &popped_source,
                            &task)) {
                        return;
                    }
                }
                if (task == nullptr) {
                    const std::uint64_t observed =
                        owner.work_epoch.load(std::memory_order_acquire);
                    const bool already_armed =
                        owner.work_waiter_armed.exchange(
                            true, std::memory_order_acq_rel);
                    if (already_armed) {
                        TerminateInvariant(
                            "parallel_worker_waiter_rearmed");
                    }
                    const bool found_after_arm =
                        TryPopParallelWorkerTask(
                            worker,
                            &next_source,
                            &popped_source,
                            &task);
                    if (!found_after_arm &&
                        !owner.stop_requested.load(
                            std::memory_order_acquire)) {
                        owner.work_epoch.wait(
                            observed, std::memory_order_acquire);
                        // A fatal drain can advance the epoch before this
                        // worker arms, so wait may return immediately without
                        // a producer having observed and cleared the bit.
                        static_cast<void>(
                            owner.work_waiter_armed.exchange(
                                false, std::memory_order_acq_rel));
                        continue;
                    }
                    static_cast<void>(
                        owner.work_waiter_armed.exchange(
                            false, std::memory_order_acq_rel));
                    if (!found_after_arm) {
                        if (owner.stop_requested.load(
                                std::memory_order_acquire)) {
                            // As above, the acquire must precede the final
                            // empty scan that authorizes worker exit.
                            if (!TryPopParallelWorkerTask(
                                    worker,
                                    &next_source,
                                    &popped_source,
                                    &task)) {
                                return;
                            }
                        } else {
                            continue;
                        }
                    }
                }
            }

            if (task == nullptr ||
                popped_source >=
                    market::kRealtimeHistorySourceCountV1 ||
                task->source_sequence == 0U ||
                task->global_ingress_sequence == 0U) {
                TerminateInvariant("parallel_issue_task_identity_corrupt");
            }
            const std::uint8_t source = popped_source;
            if (task->source_slot != source ||
                !task->command.message ||
                task->command.message->source_slot() != source ||
                task->command.message->source_sequence() !=
                    task->source_sequence ||
                task->command.message->global_ingress_sequence() !=
                    task->global_ingress_sequence) {
                ReportPipelineFailure(
                    "parallel_issue_corrupt",
                    source,
                    task->global_ingress_sequence,
                    worker);
                task->source_slot = source;
                task->command.message.reset();
                task->decode_error =
                    market::MarketDecodeErrorV1::kInvalidInput;
                task->identity_applied = false;
                TripFatal();
                if (!lanes_[source]->completion->PublishFailure(task)) {
                    TerminateInvariant(
                        "parallel_issue_failure_tombstone_publish");
                }
                continue;
            }
            if (latency_collector_ != nullptr) {
                task->worker_dequeue_clock_valid = ReadClockNs(
                    CLOCK_MONOTONIC,
                    &task->worker_dequeue_monotonic_ns);
                task->decode_start_clock_valid = ReadClockNs(
                    CLOCK_MONOTONIC,
                    &task->decode_start_monotonic_ns);
            }
            const bool decode_attempted =
                !fatal_.load(std::memory_order_acquire);
            if (decode_attempted) {
                task->decode_error = DecodeStatelessOne(
                    source, task->command.message, &task->decoded);
                if (task->decode_error ==
                    market::MarketDecodeErrorV1::kNone) {
                    task->identity_applied =
                        market::ApplyDailyInstrumentIdentityV2(
                            task->command.identity,
                            &task->decoded);
                    if (task->identity_applied) {
                        std::visit(
                            [additional_market_notices =
                                 task->command
                                     .additional_market_notices](
                                auto& value) noexcept {
                                value.common.market_notices |=
                                    additional_market_notices;
                            },
                            task->decoded);
                    }
                }
            }
            if (latency_collector_ != nullptr) {
                task->decode_complete_clock_valid = ReadClockNs(
                    CLOCK_MONOTONIC,
                    &task->decode_complete_monotonic_ns);
            }
            // DecodeStateless owns every published field and clears
            // origin.body. The large callback copy is no longer needed by
            // ordered finalization, so return it to the ingress pool on the
            // parse core instead of carrying it through the reorder window.
            task->command.message.reset();
            if (decode_attempted) {
                IncrementSingleWriterCounter(
                    &owner.parsed_messages);
                if (task->decode_error !=
                    market::MarketDecodeErrorV1::kNone) {
                    IncrementSingleWriterCounter(
                        &owner.parse_failures);
                }
            }
            if (source >= market::kRealtimeHistorySourceCountV1 ||
                lanes_[source]->completion == nullptr ||
                !lanes_[source]->completion->Publish(task)) {
                const std::uint64_t ingress_sequence =
                    task->global_ingress_sequence;
                ReportPipelineFailure(
                    "parallel_completion_publish",
                    source,
                    ingress_sequence,
                    worker);
                TripFatal();
                if (!lanes_[source]->completion->PublishFailure(task)) {
                    TerminateInvariant(
                        "parallel_completion_failure_tombstone_publish");
                }
                continue;
            }
            if (decode_attempted) {
                // Count only successfully published decode completions. This
                // keeps normal-path completed telemetry exact and makes the
                // sampled published-minus-taken depth a lower-bound estimate
                // instead of counting a cell before it exists.
                std::atomic<std::uint64_t>& completed =
                    owner.completed_messages_by_source[source];
                IncrementSingleWriterCounter(&completed);
            }
        }
    }

    void ParallelCommitLoop(std::uint8_t source) noexcept {
        DecoderLane& lane = *lanes_[source];
        std::uint64_t next_sequence = 1U;
        constexpr std::size_t kCompletionDepthSampleInterval = 64U;
        std::size_t completion_depth_sample_countdown = 1U;
        for (;;) {
            // Observe the wake epoch before checking the target cell. A
            // publication between the empty check and wait then changes this
            // epoch and cannot become a lost final wake.
            const std::uint64_t observed = lane.completion->epoch();
            const std::uint64_t externally_committed =
                lane.committed_source_sequence.load(
                    std::memory_order_acquire);
            if (next_sequence <= externally_committed) {
                next_sequence = externally_committed + 1U;
            }
            --completion_depth_sample_countdown;
            if (completion_depth_sample_countdown == 0U) {
                SampleParallelCompletionDepth(source, &lane);
                completion_depth_sample_countdown =
                    kCompletionDepthSampleInterval;
            }
            ParallelDecodeTask* task =
                lane.completion->TryTake(next_sequence);
            bool completion_publish_failed = false;
            if (task == nullptr) {
                task = lane.completion->TryTakeFailure(next_sequence);
                completion_publish_failed = task != nullptr;
            }
            if (task != nullptr) {
                constexpr std::size_t kMaximumCommitBatch =
                    market::kRealtimeHistoryMaximumSubmitBatchV1;
                std::array<ParallelDecodeTask*, kMaximumCommitBatch>
                    ready_tasks{};
                std::array<bool, kMaximumCommitBatch>
                    completion_failures{};
                std::array<bool, kMaximumCommitBatch>
                    commit_results{};
                ready_tasks[0U] = task;
                completion_failures[0U] =
                    completion_publish_failed;
                std::size_t ready_count = 1U;
                // Greedily take only results that are already contiguous.
                // The first missing cell ends the batch immediately; no
                // latency is added by waiting for a preferred batch size.
                while (ready_count < kMaximumCommitBatch &&
                       next_sequence <=
                           std::numeric_limits<std::uint64_t>::max() -
                               ready_count) {
                    const std::uint64_t candidate_sequence =
                        next_sequence + ready_count;
                    ParallelDecodeTask* candidate =
                        lane.completion->TryTake(candidate_sequence);
                    bool candidate_failed = false;
                    if (candidate == nullptr) {
                        candidate = lane.completion->TryTakeFailure(
                            candidate_sequence);
                        candidate_failed = candidate != nullptr;
                    }
                    if (candidate == nullptr) {
                        break;
                    }
                    ready_tasks[ready_count] = candidate;
                    completion_failures[ready_count] =
                        candidate_failed;
                    ++ready_count;
                }

                market::RealtimeHistorySubmissionBatchV1
                    submission_batch;
                market::RealtimeHistorySubmissionBatchV1*
                    submission_batch_pointer = nullptr;
                if (ready_count > 1U &&
                    !completion_failures[0U] &&
                    !fatal_.load(std::memory_order_acquire)) {
                    const market::RealtimeHistorySubmitErrorV1 begin_error =
                        history_->BeginSubmissionBatch(
                            source, &submission_batch);
                    if (begin_error ==
                        market::RealtimeHistorySubmitErrorV1::kNone) {
                        submission_batch_pointer = &submission_batch;
                    } else {
                        ReportPipelineFailure(
                            "history_submit_batch_begin",
                            source,
                            ready_tasks[0U]
                                ->global_ingress_sequence,
                            static_cast<std::uint64_t>(begin_error));
                        TripFatal();
                    }
                }

                std::size_t batch_committed = 0U;
                for (std::size_t index = 0U;
                     index < ready_count;
                     ++index) {
                    task = ready_tasks[index];
                    completion_publish_failed =
                        completion_failures[index];
                    const std::uint64_t expected_sequence =
                        next_sequence + index;
                    if (!completion_publish_failed) {
                        IncrementSingleWriterCounter(
                            &lane.completion_taken_messages);
                    }
                    const bool task_valid =
                        task != nullptr &&
                        !task->command.message &&
                        task->source_sequence == expected_sequence &&
                        task->source_slot == source &&
                        task->global_ingress_sequence != 0U;
                    if (!task_valid) {
                        ReportPipelineFailure(
                            "parallel_completion_sequence",
                            source,
                            task != nullptr
                                ? task->global_ingress_sequence
                                : 0U,
                            expected_sequence);
                        TripFatal();
                    }
                    std::uint64_t commit_start_ns = 0U;
                    const bool commit_clock_valid =
                        task_valid && latency_collector_ != nullptr &&
                        ReadClockNs(
                            CLOCK_MONOTONIC, &commit_start_ns);
                    if (commit_clock_valid &&
                        task->decode_complete_clock_valid &&
                        commit_start_ns >=
                            task->decode_complete_monotonic_ns) {
                        const std::uint64_t wait_ns =
                            commit_start_ns -
                            task->decode_complete_monotonic_ns;
                        IncrementSingleWriterCounter(
                            &lane.reorder_wait_samples);
                        lane.reorder_wait_total_ns.store(
                            lane.reorder_wait_total_ns.load(
                                std::memory_order_relaxed) + wait_ns,
                            std::memory_order_relaxed);
                        std::uint64_t maximum =
                            lane.reorder_wait_max_ns.load(
                                std::memory_order_relaxed);
                        while (maximum < wait_ns &&
                               !lane.reorder_wait_max_ns
                                    .compare_exchange_weak(
                                        maximum,
                                        wait_ns,
                                        std::memory_order_relaxed,
                                        std::memory_order_relaxed)) {
                        }
                    }

                    const bool committed =
                        task_valid &&
                        !completion_publish_failed &&
                        !fatal_.load(std::memory_order_acquire) &&
                        CommitParallelTask(
                            source,
                            task,
                            submission_batch_pointer);
                    if (!committed &&
                        !fatal_.load(std::memory_order_acquire)) {
                        TripFatal();
                    }
                    if (committed) {
                        ++batch_committed;
                    }
                    commit_results[index] = committed;
                }
                if (submission_batch_pointer != nullptr) {
                    const market::RealtimeHistoryBatchSubmitResultV1
                        batch_result = submission_batch.Finish();
                    if (batch_result.submitted_count !=
                        batch_committed) {
                        TerminateInvariant(
                            "parallel_history_batch_prefix_mismatch");
                    }
                    if (batch_result.submitted_count > 1U) {
                        IncrementSingleWriterCounter(
                            &lane.history_batch_calls);
                        AddSingleWriterCounter(
                            &lane.history_batched_messages,
                            static_cast<std::uint64_t>(
                                batch_result.submitted_count));
                        const std::size_t maximum =
                            lane.history_batch_max.load(
                                std::memory_order_relaxed);
                        if (batch_result.submitted_count > maximum) {
                            lane.history_batch_max.store(
                                batch_result.submitted_count,
                                std::memory_order_relaxed);
                        }
                    }
                }
                // End the History submission gate before publishing any
                // retirement from this batch.  The final retirement permits
                // the source owner to switch back to inline submission and
                // permits a generation fence to call SealSource; neither may
                // overlap this batch's non-atomic source-owner state updates.
                for (std::size_t index = 0U;
                     index < ready_count;
                     ++index) {
                    task = ready_tasks[index];
                    const bool committed = commit_results[index];
                    const std::uint64_t expected_sequence =
                        next_sequence;
                    ParallelDecodeShard& shard =
                        *lane.parallel_shards[
                            ParallelWorkerForSourceSequence(
                                source,
                                expected_sequence,
                                config_
                                    .parallel_decoder_worker_count)];
                    if (committed) {
                        const std::uint64_t committed_messages =
                            lane.committed_messages.load(
                                std::memory_order_relaxed);
                        if (committed_messages ==
                            std::numeric_limits<
                                std::uint64_t>::max()) {
                            TerminateInvariant(
                                "parallel_committed_messages_overflow");
                        }
                        lane.committed_messages.store(
                            committed_messages + 1U,
                            std::memory_order_relaxed);
                        lane.committed_source_sequence.store(
                            expected_sequence,
                            std::memory_order_release);
                    } else {
                        lane.discarded_messages.fetch_add(
                            1U, std::memory_order_relaxed);
                    }
                    ++next_sequence;
                    task->ResetAndRelease(
                        &shard.release_epoch,
                        &shard.release_waiter_armed);
                    // Publish every ordered decoder/History effect, including
                    // batch Finish, before a source owner can observe the farm
                    // as fully drained.
                    IncrementSingleWriterCounterRelease(
                        &lane.retired_messages);
                    const std::uint64_t issued =
                        lane.farm_messages.load(
                            std::memory_order_acquire);
                    if (lane.retired_messages.load(
                            std::memory_order_relaxed) > issued) {
                        TerminateInvariant(
                            committed
                                ? "parallel_commit_retired_exceeds_issued"
                                : "parallel_discard_retired_exceeds_issued");
                    }
                }
                const std::uint64_t issued =
                    lane.farm_messages.load(
                        std::memory_order_acquire);
                if (lane.retired_messages.load(
                        std::memory_order_relaxed) == issued) {
                    SignalParallelCommitProgress(&lane, false);
                }
                continue;
            }

            const bool dispatch_done =
                lane.dispatch_done.load(std::memory_order_acquire);
            const bool fatal =
                fatal_.load(std::memory_order_acquire);
            if (dispatch_done) {
                if (fatal &&
                    ParallelFarmOutstanding(lane) == 0U) {
                    return;
                }
                if (!fatal &&
                    next_sequence >
                        lane.last_dispatched_source_sequence.load(
                            std::memory_order_acquire)) {
                    return;
                }
            }
            // On fatal, workers still publish every lease that the source
            // dispatcher had already issued. Keep consuming those completions
            // without applying them until dispatch is closed and the bounded
            // outstanding count reaches zero.
            SampleParallelCompletionDepth(source, &lane);
            lane.completion->Wait(observed, next_sequence);
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

    [[nodiscard]] market::MarketDecodeErrorV1 DecodeStatelessOne(
        std::uint8_t source,
        const realtime::OwnedIngressMessageHandleV1& message,
        market::DecodedMarketEventV1* output) const noexcept {
        if (output == nullptr || !message ||
            source >= market::kRealtimeHistorySourceCountV1 ||
            message->source_slot() != source ||
            IsMixedTickSourceSlot(source) !=
                (message->tick_stream_sequence() != 0U) ||
            message->recv_realtime_ns() >
                static_cast<std::uint64_t>(
                    std::numeric_limits<std::int64_t>::max()) ||
            message->recv_monotonic_ns() >
                static_cast<std::uint64_t>(
                    std::numeric_limits<std::int64_t>::max())) {
            return market::MarketDecodeErrorV1::kInvalidInput;
        }
        const sdk::VendorHeadView head = message->vendor_head();
        if (head.message_encoding() !=
            static_cast<std::uint8_t>(mdl::MDLEID_BINARY)) {
            return market::MarketDecodeErrorV1::kInvalidInput;
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
        return lanes_[source]->decoder.DecodeStateless(view, output);
    }

    [[nodiscard]] bool CommitParallelTask(
        std::uint8_t source,
        ParallelDecodeTask* task,
        market::RealtimeHistorySubmissionBatchV1* submission_batch)
        noexcept {
        if (task == nullptr || task->command.message ||
            task->source_slot != source ||
            task->source_sequence == 0U ||
            task->global_ingress_sequence == 0U) {
            return false;
        }
        market::MarketDecodeErrorV1 decode_error = task->decode_error;
        if (decode_error == market::MarketDecodeErrorV1::kNone) {
            decode_error = lanes_[source]->decoder.FinalizeInSourceOrder(
                &task->decoded);
        }
        if (decode_error != market::MarketDecodeErrorV1::kNone) {
            last_decode_error_.store(
                static_cast<std::uint8_t>(decode_error),
                std::memory_order_release);
            ReportPipelineFailure(
                "market_decode",
                source,
                task->global_ingress_sequence,
                static_cast<std::uint64_t>(decode_error));
            return false;
        }
        if (!task->identity_applied) {
            ReportPipelineFailure(
                "instrument_identity_apply",
                source,
                task->global_ingress_sequence,
                task->command.identity.instrument_id);
            return false;
        }

        std::uint64_t ordered_decode_complete_ns = 0U;
        const bool ordered_decode_complete_clock_valid =
            latency_collector_ != nullptr &&
            ReadClockNs(
                CLOCK_MONOTONIC, &ordered_decode_complete_ns);
        if (latency_collector_ != nullptr) {
            latency_collector_->RecordDecodeAppliedOrigin(
                source,
                task->global_ingress_sequence,
                ordered_decode_complete_ns,
                ordered_decode_complete_clock_valid);
        }

        std::optional<market::RealtimeHistoryEventInputV1> input =
            market::RealtimeHistoryEventInputV1::Create(
                source,
                task->global_ingress_sequence,
                std::move(task->decoded),
                task->tick_stream_sequence);
        if (!input.has_value()) {
            ReportHistoryInputFailure(
                source,
                task->global_ingress_sequence,
                task->tick_stream_sequence,
                task->decoded);
            return false;
        }
        const market::RealtimeHistorySubmitErrorV1 submit_error =
            submission_batch == nullptr
                ? history_->TrySubmit(std::move(*input))
                : submission_batch->TrySubmit(std::move(*input));
        std::uint64_t history_submit_complete_ns = 0U;
        const bool history_submit_clock_valid =
            latency_collector_ != nullptr &&
            ReadClockNs(
                CLOCK_MONOTONIC, &history_submit_complete_ns);
        const bool submitted =
            submit_error == market::RealtimeHistorySubmitErrorV1::kNone;
        if (latency_collector_ != nullptr) {
            latency_collector_->RecordDecoderWork(
                source,
                task->command.queue_publish_monotonic_ns,
                task->worker_dequeue_monotonic_ns,
                task->decode_start_monotonic_ns,
                ordered_decode_complete_ns,
                history_submit_complete_ns,
                task->command.queue_publish_monotonic_ns != 0U &&
                    task->worker_dequeue_clock_valid &&
                    task->worker_dequeue_monotonic_ns >=
                        task->command.queue_publish_monotonic_ns,
                task->decode_start_clock_valid &&
                    ordered_decode_complete_clock_valid &&
                    ordered_decode_complete_ns >=
                        task->decode_start_monotonic_ns,
                submitted && ordered_decode_complete_clock_valid &&
                    history_submit_clock_valid &&
                    history_submit_complete_ns >=
                        ordered_decode_complete_ns);
        }
        if (!submitted) {
            ReportPipelineFailure(
                "history_submit",
                source,
                task->global_ingress_sequence,
                static_cast<std::uint64_t>(submit_error));
            return false;
        }
        std::atomic<std::uint64_t>& decoded_counter =
            decoded_messages_by_source_[source].value;
        IncrementSingleWriterCounter(&decoded_counter);
        return true;
    }

    void DecoderLoop(std::uint8_t source) noexcept {
        DecoderCommand command{};
        while (lanes_[source]->queue.Pop(&command)) {
            if (command.kind == CommandKind::kGenerationFence) {
                if (!ParkAtGenerationFence(
                        source, command.generation) &&
                    !fatal_.load(std::memory_order_acquire)) {
                    TripFatal();
                }
                command = DecoderCommand{};
                continue;
            }
            if (fatal_.load(std::memory_order_acquire)) {
                command = DecoderCommand{};
                continue;
            }

            std::uint64_t dequeue_monotonic_ns = 0U;
            if (latency_collector_ != nullptr) {
                static_cast<void>(ReadClockNs(
                    CLOCK_MONOTONIC, &dequeue_monotonic_ns));
            }
            if (!WaitForAppliedDispatchWindow(
                    command.message->global_ingress_sequence())) {
                ReportPipelineFailure(
                    "applied_dispatch_window",
                    source,
                    command.message->global_ingress_sequence(),
                    applied_window_capacity_);
                TripFatal();
                command = DecoderCommand{};
                continue;
            }
            std::uint64_t decode_start_monotonic_ns = 0U;
            if (latency_collector_ != nullptr) {
                static_cast<void>(ReadClockNs(
                    CLOCK_MONOTONIC,
                    &decode_start_monotonic_ns));
            }
            DecoderTimingObservation timing{};
            const bool decoded = DecodeOne(
                source,
                command.message,
                command.identity,
                command.additional_market_notices,
                latency_collector_ != nullptr ? &timing : nullptr);
            if (latency_collector_ != nullptr) {
                latency_collector_->RecordDecoderWork(
                    source,
                    command.queue_publish_monotonic_ns,
                    dequeue_monotonic_ns,
                    decode_start_monotonic_ns,
                    timing.decode_complete_monotonic_ns,
                    timing.history_submit_complete_monotonic_ns,
                    command.queue_publish_monotonic_ns != 0U &&
                        dequeue_monotonic_ns != 0U &&
                        dequeue_monotonic_ns >=
                            command.queue_publish_monotonic_ns,
                    decoded && decode_start_monotonic_ns != 0U &&
                        timing.decode_complete_clock_valid,
                    decoded &&
                        timing.decode_complete_clock_valid &&
                        timing.history_submit_complete_clock_valid);
            }
            if (!decoded) {
                TripFatal();
            }
            command = DecoderCommand{};
        }
    }

    [[nodiscard]] bool DecodeOne(
        std::uint8_t source,
        const realtime::OwnedIngressMessageHandleV1& message,
        const market::DailyInstrumentIdentityViewV2& identity,
        std::uint64_t additional_market_notices,
        DecoderTimingObservation* timing)
        noexcept {
        if (timing != nullptr) {
            *timing = DecoderTimingObservation{};
        }
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
        if (decode_error != market::MarketDecodeErrorV1::kNone) {
            last_decode_error_.store(
                static_cast<std::uint8_t>(decode_error),
                std::memory_order_release);
            ReportPipelineFailure(
                "market_decode",
                source,
                message->global_ingress_sequence(),
                static_cast<std::uint64_t>(decode_error));
            return false;
        }
        if (!market::ApplyDailyInstrumentIdentityV2(
                identity, &decoded)) {
            ReportPipelineFailure(
                "instrument_identity_apply",
                source,
                message->global_ingress_sequence(),
                identity.instrument_id);
            return false;
        }
        std::visit(
            [additional_market_notices](auto& value) noexcept {
                value.common.market_notices |=
                    additional_market_notices;
            },
            decoded);
        if (timing != nullptr) {
            timing->decode_complete_clock_valid = ReadClockNs(
                CLOCK_MONOTONIC,
                &timing->decode_complete_monotonic_ns);
            if (latency_collector_ != nullptr) {
                latency_collector_->RecordDecodeAppliedOrigin(
                    source,
                    message->global_ingress_sequence(),
                    timing->decode_complete_monotonic_ns,
                    timing->decode_complete_clock_valid);
            }
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
        if (timing != nullptr) {
            timing->history_submit_complete_clock_valid =
                ReadClockNs(
                    CLOCK_MONOTONIC,
                    &timing
                         ->history_submit_complete_monotonic_ns);
        }
        if (submit_error !=
            market::RealtimeHistorySubmitErrorV1::kNone) {
            ReportPipelineFailure(
                "history_submit",
                source,
                message->global_ingress_sequence(),
                static_cast<std::uint64_t>(submit_error));
            return false;
        }
        std::atomic<std::uint64_t>& decoded_counter =
            decoded_messages_by_source_[source].value;
        IncrementSingleWriterCounter(&decoded_counter);
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
        RequestDecoderStop();
        RequestProgressStop();
        applied_progress_cv_.notify_all();
        generation_fence_cv_.notify_all();
    }

    void TripFatal() noexcept {
        std::lock_guard<std::mutex> admission(admission_mutex_);
        TripFatalWithAdmissionLockHeld();
    }

    void RequestDecoderStop() noexcept {
        for (const std::unique_ptr<DecoderLane>& lane : lanes_) {
            if (lane != nullptr) {
                lane->queue.RequestStop();
                for (const auto& shard : lane->parallel_shards) {
                    shard->Wake();
                }
                if (lane->completion != nullptr) {
                    lane->completion->WakeAll();
                }
                SignalParallelCommitProgress(lane.get(), true);
            }
        }
        for (const auto& worker : parallel_decoder_workers_) {
            static_cast<void>(worker->work_waiter_armed.exchange(
                false, std::memory_order_acq_rel));
            worker->work_epoch.fetch_add(
                1U, std::memory_order_release);
            worker->work_epoch.notify_all();
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

    void JoinProgressThread() noexcept {
        if (progress_thread_.joinable()) {
            progress_thread_.join();
        }
    }

    void JoinDecoderThreads() noexcept {
        if (config_.parallel_decoder_worker_count != 0U) {
            // Source dispatchers are the sole producers for their worker
            // issue queues. Drain/join them before publishing worker stop so
            // every accepted message has either completed inline or owns a
            // consumable decode lease.
            for (const std::unique_ptr<DecoderLane>& lane : lanes_) {
                if (lane != nullptr && lane->thread.joinable()) {
                    lane->thread.join();
                }
            }
            for (const auto& worker : parallel_decoder_workers_) {
                worker->stop_requested.store(
                    true, std::memory_order_release);
                static_cast<void>(worker->work_waiter_armed.exchange(
                    false, std::memory_order_acq_rel));
                worker->work_epoch.fetch_add(
                    1U, std::memory_order_release);
                worker->work_epoch.notify_all();
            }
            for (const auto& worker : parallel_decoder_workers_) {
                if (worker->thread.joinable()) {
                    worker->thread.join();
                }
            }
            for (const std::unique_ptr<DecoderLane>& lane : lanes_) {
                if (lane != nullptr && lane->completion != nullptr) {
                    lane->completion->WakeAll();
                }
            }
            for (const std::unique_ptr<DecoderLane>& lane : lanes_) {
                if (lane != nullptr && lane->commit_thread.joinable()) {
                    lane->commit_thread.join();
                }
            }
            decoder_threads_started_ = false;
            return;
        }
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
                TerminateInvariant("sdk_shutdown_exception");
            }
        }
        // Arm before sampling the count. A callback that completed earlier
        // leaves zero for this load; one that completes later observes the
        // armed flag after its decrement and performs the only needed notify.
        callback_waiter_armed_.store(true, std::memory_order_release);
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
    // only after all decoder rings have released their handles.
    std::unique_ptr<realtime::OwnedIngressMessagePoolV1> ingress_pool_;
    std::unique_ptr<StageLatencyCollector> latency_collector_;
    std::thread progress_thread_;
    std::binary_semaphore progress_wake_{0};
    std::atomic<bool> progress_wake_pending_{false};
    std::atomic<bool> progress_stop_requested_{false};
    std::array<std::unique_ptr<DecoderLane>,
               market::kRealtimeHistorySourceCountV1>
        lanes_{};
    std::vector<std::unique_ptr<ParallelDecoderWorker>>
        parallel_decoder_workers_;
    std::once_flag parallel_farm_start_once_;
    std::atomic<bool> parallel_farm_started_{false};
    std::atomic<bool> parallel_farm_start_failed_{false};
    bool decoder_threads_started_ = false;

    std::unique_ptr<market::RealtimeHistoryRuntimeV1> history_;
    std::unique_ptr<factor::RealtimeFactorEngineV1> factor_;
    std::unique_ptr<realtime::ContiguousSequenceTrackerV2>
        applied_tracker_;
    std::size_t applied_window_capacity_ = 0U;
    alignas(64) std::atomic<std::uint64_t> accepted_sequence_{0U};
    alignas(64) std::atomic<std::uint64_t> applied_sequence_{0U};
    std::mutex applied_progress_mutex_;
    std::condition_variable applied_progress_cv_;
    std::atomic<std::size_t> applied_progress_waiters_{0U};
    std::mutex generation_fence_mutex_;
    std::condition_variable generation_fence_cv_;
    std::uint64_t active_fence_generation_ = 0U;
    std::uint64_t seal_requested_generation_ = 0U;
    std::uint64_t release_fence_generation_ = 0U;
    std::array<std::uint64_t,
               market::kRealtimeHistorySourceCountV1>
        fence_arrived_generation_{};
    std::array<std::uint64_t,
               market::kRealtimeHistorySourceCountV1>
        fence_sealed_generation_{};
    std::array<std::uint64_t,
               market::kRealtimeHistorySourceCountV1>
        fence_departed_generation_{};
    std::array<market::RealtimeHistoryGenerationErrorV1,
               market::kRealtimeHistorySourceCountV1>
        fence_seal_errors_{};

    std::shared_ptr<sdk::SdkFactory> sdk_factory_;
    std::unique_ptr<sdk::SdkManager> sdk_manager_;
    std::unique_ptr<sdk::SdkSubscriber> sdk_subscriber_;

    // Serializes the SDK-less shadow/replay producer across bounded waits
    // that deliberately release admission_mutex_. It is never touched by the
    // ordinary SDK callback path.
    std::mutex external_ingress_mutex_;
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

    std::array<SourceMessageCounter,
               market::kRealtimeHistorySourceCountV1>
        decoded_messages_by_source_{};
    std::atomic<std::uint64_t> last_published_generation_{0U};
    std::atomic<std::uint8_t> last_decode_error_{
        static_cast<std::uint8_t>(market::MarketDecodeErrorV1::kNone)};
    std::atomic_flag applied_sequence_failure_reported_ =
        ATOMIC_FLAG_INIT;
    std::atomic_flag pipeline_failure_reported_ = ATOMIC_FLAG_INIT;
    std::atomic<bool> accepting_{false};
    std::atomic<bool> fatal_{false};
    std::atomic<bool> stopped_{false};
    std::atomic<bool> trade_date_boundary_reached_{false};

    std::atomic<bool> callback_gate_closed_{false};
    std::atomic<std::uint64_t> active_callbacks_{0U};
    std::atomic<bool> callback_waiter_armed_{false};

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
            const RealtimePipelineSnapshotV1 snapshot =
                impl->Snapshot();
            if (detail != nullptr && snapshot.fatal) {
                *detail +=
                    "; startup_diagnostics accepted=" +
                    std::to_string(
                        snapshot.processing_progress
                            .accepted_sequence) +
                    " applied=" +
                    std::to_string(
                        snapshot.processing_progress
                            .applied_sequence) +
                    " decoded=" +
                    std::to_string(snapshot.decoded_messages) +
                    " rejected=" +
                    std::to_string(snapshot.rejected_messages) +
                    " store_records=" +
                    std::to_string(
                        snapshot.store.appended_records) +
                    " coverage_lost=" +
                    std::string(
                        snapshot.store.coverage_lost
                            ? "true"
                            : "false") +
                    " last_decode_error=" +
                    std::to_string(
                        static_cast<unsigned>(
                            snapshot.last_decode_error)) +
                    " history_failure=" +
                    std::string(
                        market::RealtimeHistoryGenerationErrorNameV1(
                            impl->history_failure_error()));
            }
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

RealtimePipelineIngressResultV1
RealtimePipelineV1::IngestExternalMessage(
    const RealtimePipelineExternalIngressV1& input) noexcept {
    return impl_->IngestExternal(input);
}

RealtimePipelineCutResultV1
RealtimePipelineV1::CutAndPublishGeneration(
    std::chrono::nanoseconds timeout) noexcept {
    return impl_->Cut(timeout);
}

bool RealtimePipelineV1::WaitAppliedThroughPrefix(
    std::uint64_t target_sequence,
    std::chrono::steady_clock::time_point deadline) noexcept {
    return impl_->WaitAppliedThroughPrefix(target_sequence, deadline);
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

RealtimePipelineLiveStatusV1 RealtimePipelineV1::LiveStatus()
    const noexcept {
    return impl_->LiveStatus();
}

bool RealtimePipelineV1::LiveIngressHealthy() const noexcept {
    return impl_->LiveIngressHealthy();
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
