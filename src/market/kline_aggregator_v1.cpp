#include "l2flow/market/kline_aggregator_v1.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <limits>
#include <map>
#include <mutex>
#include <new>
#include <tuple>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace l2flow::market {
namespace {

[[nodiscard]] bool ValidTradeDate(std::uint32_t value) noexcept {
    const std::uint32_t year = value / 10'000U;
    const std::uint32_t month = (value / 100U) % 100U;
    const std::uint32_t day = value % 100U;
    if (year < 1992U || year > 2200U || month == 0U ||
        month > 12U || day == 0U) {
        return false;
    }
    constexpr std::uint32_t kDaysByMonth[12U] = {
        31U, 28U, 31U, 30U, 31U, 30U,
        31U, 31U, 30U, 31U, 30U, 31U};
    std::uint32_t maximum_day = kDaysByMonth[month - 1U];
    const bool leap =
        (year % 4U == 0U && year % 100U != 0U) ||
        year % 400U == 0U;
    if (month == 2U && leap) {
        maximum_day = 29U;
    }
    return day <= maximum_day;
}

[[nodiscard]] bool FixedUtc8MidnightUnixNs(
    std::uint32_t trade_date,
    std::int64_t* output) noexcept {
    if (output == nullptr || !ValidTradeDate(trade_date)) {
        return false;
    }
    const std::uint32_t year = trade_date / 10'000U;
    const std::uint32_t month = (trade_date / 100U) % 100U;
    const std::uint32_t day = trade_date % 100U;
    std::int64_t adjusted_year = static_cast<std::int64_t>(year);
    adjusted_year -= month <= 2U ? 1 : 0;
    const std::int64_t era = adjusted_year / 400;
    const std::int64_t year_of_era = adjusted_year - era * 400;
    const std::int64_t adjusted_month =
        static_cast<std::int64_t>(month) + (month > 2U ? -3 : 9);
    const std::int64_t day_of_year =
        (153 * adjusted_month + 2) / 5 +
        static_cast<std::int64_t>(day) - 1;
    const std::int64_t day_of_era =
        year_of_era * 365 + year_of_era / 4 -
        year_of_era / 100 + day_of_year;
    const std::int64_t days_since_epoch =
        era * 146'097 + day_of_era - 719'468;
    constexpr std::int64_t kSecondsPerDay = 86'400LL;
    constexpr std::int64_t kFixedUtc8OffsetSeconds = 8LL * 3'600LL;
    constexpr std::int64_t kNanosecondsPerSecond =
        1'000'000'000LL;
    const std::int64_t seconds =
        days_since_epoch * kSecondsPerDay -
        kFixedUtc8OffsetSeconds;
    *output = seconds * kNanosecondsPerSecond;
    return true;
}

[[nodiscard]] bool EventOrderLess(
    const KLineEventOrderV1& lhs,
    const KLineEventOrderV1& rhs) noexcept {
    return std::tie(
               lhs.event_time_ns_since_midnight,
               lhs.event_sequence,
               lhs.source_sequence,
               lhs.ingress_sequence) <
           std::tie(
               rhs.event_time_ns_since_midnight,
               rhs.event_sequence,
               rhs.source_sequence,
               rhs.ingress_sequence);
}

struct SeriesKey final {
    std::uint32_t instrument_id = 0U;
    std::uint32_t window_id = 0U;

    [[nodiscard]] friend bool operator<(
        const SeriesKey& lhs,
        const SeriesKey& rhs) noexcept {
        return std::tie(lhs.instrument_id, lhs.window_id) <
               std::tie(rhs.instrument_id, rhs.window_id);
    }
};

struct BarChunk final {
    std::vector<KLineBarV1> bars;
};

struct BarSeries final {
    std::vector<std::shared_ptr<BarChunk>> chunks;
    std::uint64_t bar_count = 0U;
};

struct SnapshotSeries final {
    SeriesKey key{};
    std::shared_ptr<const BarSeries> series;
};

struct KLineCaptureIdentity final {};

struct OwnerRowCaptureState final {
    std::mutex mutex;
    std::uint64_t frozen_generation = 0U;
    std::uint32_t frozen_instrument_id = 0U;
};

[[nodiscard]] const SnapshotSeries* FindSnapshotSeries(
    const std::vector<SnapshotSeries>& series,
    const SeriesKey& key) noexcept {
    const auto found = std::lower_bound(
        series.begin(),
        series.end(),
        key,
        [](const SnapshotSeries& lhs, const SeriesKey& rhs) noexcept {
            return lhs.key < rhs;
        });
    if (found == series.end() ||
        found->key.instrument_id != key.instrument_id ||
        found->key.window_id != key.window_id ||
        found->series == nullptr) {
        return nullptr;
    }
    return &*found;
}

[[nodiscard]] const KLineBarV1* FindLatestBar(
    const BarSeries& series) noexcept {
    for (auto chunk = series.chunks.rbegin();
         chunk != series.chunks.rend();
         ++chunk) {
        if (*chunk != nullptr && !(*chunk)->bars.empty()) {
            return &(*chunk)->bars.back();
        }
    }
    return nullptr;
}

[[nodiscard]] KLineTradeProjectionV1 ProjectTickFields(
    const DecodedMarketCommonV1& common,
    const TickFieldsV1& fields,
    std::uint64_t native_event_sequence,
    std::uint64_t ingress_sequence,
    KLineTradeV1* output) noexcept {
    if (fields.action != TickActionV1::kTrade) {
        return KLineTradeProjectionV1::kNotTrade;
    }
    constexpr std::uint32_t kRequiredValidity =
        kTickPriceValidV1 | kTickQuantityValidV1 |
        kTickExchangeTimeValidV1;
    if (output == nullptr ||
        (fields.validity_bitmap & kRequiredValidity) !=
            kRequiredValidity ||
        !fields.price.valid || !fields.quantity.valid ||
        fields.quantity.raw <= 0 || !common.exchange_time.valid ||
        !common.exchange_time.unix_nanoseconds_valid ||
        common.exchange_time.nanoseconds_since_midnight >=
            kKLineNanosecondsPerDayV1 ||
        common.origin.trade_date == 0U ||
        common.origin.source_sequence == 0U ||
        ingress_sequence == 0U || common.instrument_id == 0U ||
        common.ordinal ==
            std::numeric_limits<std::size_t>::max()) {
        return KLineTradeProjectionV1::kInvalidTrade;
    }
    KLineTradeV1 projected{};
    projected.trade_date = common.origin.trade_date;
    projected.instrument_id = common.instrument_id;
    projected.ordinal = common.ordinal;
    projected.event_time_ns_since_midnight =
        common.exchange_time.nanoseconds_since_midnight;
    projected.event_time_unix_ns =
        common.exchange_time.unix_nanoseconds;
    projected.price_p6 = fields.price.normalized_p6;
    projected.quantity_raw =
        static_cast<std::uint64_t>(fields.quantity.raw);
    projected.quantity_scale = fields.quantity.scale;
    // The decoded raw quantity and its scale remain exact even when the daily
    // catalog has no reference metadata from which to infer an economic unit.
    // Preserve kUnknown instead of inventing shares/lots or dropping a valid
    // trade.
    projected.quantity_unit = common.quantity_unit;
    projected.event_sequence =
        native_event_sequence == 0U
            ? common.origin.source_sequence
            : native_event_sequence;
    projected.source_sequence = common.origin.source_sequence;
    projected.ingress_sequence = ingress_sequence;
    *output = projected;
    return KLineTradeProjectionV1::kTrade;
}

[[nodiscard]] bool ValidConfig(
    const KLineAggregatorConfigV1& config) noexcept {
    std::int64_t ignored_midnight = 0;
    if (!FixedUtc8MidnightUnixNs(
            config.trade_date, &ignored_midnight) ||
        config.windows.empty() ||
        config.windows.size() > kKLineMaximumWindowsV1 ||
        config.maximum_bars == 0U || config.bars_per_chunk == 0U ||
        config.bars_per_chunk > kKLineMaximumBarsPerReadV1 ||
        config.maximum_bars_per_read == 0U ||
        config.maximum_bars_per_read >
            kKLineMaximumBarsPerReadV1 ||
        (config.instrument_capacity != 0U &&
         config.windows.size() >
             std::numeric_limits<std::size_t>::max() /
                 config.instrument_capacity)) {
        return false;
    }
    for (std::size_t index = 0U; index < config.windows.size(); ++index) {
        const KLineWindowSpecV1& window = config.windows[index];
        if (window.window_id == 0U ||
            window.duration_ns <
                kKLineNanosecondsPerMillisecondV1 ||
            window.duration_ns > kKLineNanosecondsPerDayV1 ||
            window.duration_ns %
                    kKLineNanosecondsPerMillisecondV1 !=
                0U) {
            return false;
        }
        for (std::size_t prior = 0U; prior < index; ++prior) {
            if (config.windows[prior].window_id == window.window_id ||
                config.windows[prior].duration_ns ==
                    window.duration_ns) {
                return false;
            }
        }
    }
    return true;
}

[[nodiscard]] bool CheckedUnixBoundary(
    std::int64_t midnight_unix_ns,
    std::uint64_t offset_ns,
    std::int64_t* output) noexcept {
    if (output == nullptr ||
        offset_ns >
            static_cast<std::uint64_t>(
                std::numeric_limits<std::int64_t>::max())) {
        return false;
    }
    const std::int64_t signed_offset =
        static_cast<std::int64_t>(offset_ns);
    if (midnight_unix_ns >
        std::numeric_limits<std::int64_t>::max() - signed_offset) {
        return false;
    }
    *output = midnight_unix_ns + signed_offset;
    return true;
}

}  // namespace

KLineTradeProjectionV1 ProjectKLineTradeV1(
    const DecodedMarketEventV1& event,
    std::uint64_t ingress_sequence,
    KLineTradeV1* output) noexcept {
    if (output != nullptr) {
        *output = KLineTradeV1{};
    }
    return std::visit(
        [ingress_sequence, output](const auto& value) noexcept {
            using Event = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<Event, ShanghaiTickV1>) {
                const std::uint64_t native_sequence =
                    value.business_index > 0
                        ? static_cast<std::uint64_t>(
                              value.business_index)
                        : 0U;
                return ProjectTickFields(
                    value.common,
                    value.fields,
                    native_sequence,
                    ingress_sequence,
                    output);
            } else if constexpr (
                std::is_same_v<Event, ShenzhenTransactionV1>) {
                const std::uint64_t native_sequence =
                    value.application_sequence > 0
                        ? static_cast<std::uint64_t>(
                              value.application_sequence)
                        : 0U;
                return ProjectTickFields(
                    value.common,
                    value.fields,
                    native_sequence,
                    ingress_sequence,
                    output);
            } else {
                return KLineTradeProjectionV1::kNotTrade;
            }
        },
        event);
}

std::string_view KLineCreateErrorNameV1(
    KLineCreateErrorV1 error) noexcept {
    switch (error) {
        case KLineCreateErrorV1::kNone:
            return "none";
        case KLineCreateErrorV1::kNullOutput:
            return "null_output";
        case KLineCreateErrorV1::kInvalidConfiguration:
            return "invalid_configuration";
        case KLineCreateErrorV1::kResourceExhausted:
            return "resource_exhausted";
    }
    return "unknown";
}

std::string_view KLineAppendErrorNameV1(
    KLineAppendErrorV1 error) noexcept {
    switch (error) {
        case KLineAppendErrorV1::kNone:
            return "none";
        case KLineAppendErrorV1::kInvalidTrade:
            return "invalid_trade";
        case KLineAppendErrorV1::kBarCapacity:
            return "bar_capacity";
        case KLineAppendErrorV1::kNumericOverflow:
            return "numeric_overflow";
        case KLineAppendErrorV1::kResourceExhausted:
            return "resource_exhausted";
        case KLineAppendErrorV1::kFailed:
            return "failed";
    }
    return "unknown";
}

std::string_view KLineCaptureErrorNameV1(
    KLineCaptureErrorV1 error) noexcept {
    switch (error) {
        case KLineCaptureErrorV1::kNone:
            return "none";
        case KLineCaptureErrorV1::kNullOutput:
            return "null_output";
        case KLineCaptureErrorV1::kResourceExhausted:
            return "resource_exhausted";
        case KLineCaptureErrorV1::kFailed:
            return "failed";
    }
    return "unknown";
}

std::string_view KLineQueryErrorNameV1(
    KLineQueryErrorV1 error) noexcept {
    switch (error) {
        case KLineQueryErrorV1::kNone:
            return "none";
        case KLineQueryErrorV1::kNullOutput:
            return "null_output";
        case KLineQueryErrorV1::kInvalidArgument:
            return "invalid_argument";
        case KLineQueryErrorV1::kNotFound:
            return "not_found";
        case KLineQueryErrorV1::kBatchLimitExceeded:
            return "batch_limit_exceeded";
        case KLineQueryErrorV1::kResourceExhausted:
            return "resource_exhausted";
    }
    return "unknown";
}

class KLineCursorV1::Impl final {
public:
    Impl(
        std::shared_ptr<const BarSeries> series,
        std::size_t maximum_bars_per_read) noexcept
        : series_(std::move(series)),
          maximum_bars_per_read_(maximum_bars_per_read) {}

    std::shared_ptr<const BarSeries> series_;
    std::size_t maximum_bars_per_read_ = 0U;
    std::size_t chunk_index_ = 0U;
    std::size_t bar_index_ = 0U;
};

KLineCursorV1::KLineCursorV1(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

KLineCursorV1::KLineCursorV1(KLineCursorV1&&) noexcept = default;
KLineCursorV1& KLineCursorV1::operator=(KLineCursorV1&&) noexcept =
    default;
KLineCursorV1::~KLineCursorV1() = default;

KLineQueryErrorV1 KLineCursorV1::ReadBatch(
    std::span<KLineBarV1> output,
    std::size_t* written) noexcept {
    if (written == nullptr) {
        return KLineQueryErrorV1::kNullOutput;
    }
    *written = 0U;
    if (impl_ == nullptr || impl_->series_ == nullptr || output.empty()) {
        return KLineQueryErrorV1::kInvalidArgument;
    }
    if (output.size() > impl_->maximum_bars_per_read_) {
        return KLineQueryErrorV1::kBatchLimitExceeded;
    }
    while (*written < output.size() &&
           impl_->chunk_index_ < impl_->series_->chunks.size()) {
        const std::shared_ptr<BarChunk>& chunk =
            impl_->series_->chunks[impl_->chunk_index_];
        if (chunk == nullptr) {
            return KLineQueryErrorV1::kResourceExhausted;
        }
        while (*written < output.size() &&
               impl_->bar_index_ < chunk->bars.size()) {
            output[*written] = chunk->bars[impl_->bar_index_];
            ++(*written);
            ++impl_->bar_index_;
        }
        if (impl_->bar_index_ == chunk->bars.size()) {
            ++impl_->chunk_index_;
            impl_->bar_index_ = 0U;
        }
    }
    return KLineQueryErrorV1::kNone;
}

bool KLineCursorV1::done() const noexcept {
    return impl_ == nullptr || impl_->series_ == nullptr ||
           impl_->chunk_index_ >= impl_->series_->chunks.size();
}

class KLineAggregatorSnapshotV1::Impl final {
public:
    KLineAggregatorConfigV1 config;
    std::vector<SnapshotSeries> series;
    std::uint64_t bar_count = 0U;
};

class KLineAggregatorCutV1::Impl final {
public:
    std::shared_ptr<const KLineCaptureIdentity> identity;
    std::uint64_t generation = 0U;
    std::uint64_t bar_count = 0U;
    std::size_t represented_owner_rows = 0U;
};

KLineAggregatorSnapshotV1::KLineAggregatorSnapshotV1(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

KLineAggregatorSnapshotV1::~KLineAggregatorSnapshotV1() = default;

KLineAggregatorCutV1::KLineAggregatorCutV1(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

KLineAggregatorCutV1::~KLineAggregatorCutV1() = default;

std::uint32_t KLineAggregatorSnapshotV1::trade_date() const noexcept {
    return impl_ == nullptr ? 0U : impl_->config.trade_date;
}

std::size_t KLineAggregatorSnapshotV1::window_count() const noexcept {
    return impl_ == nullptr ? 0U : impl_->config.windows.size();
}

std::span<const KLineWindowSpecV1>
KLineAggregatorSnapshotV1::windows() const noexcept {
    return impl_ == nullptr
               ? std::span<const KLineWindowSpecV1>{}
               : std::span<const KLineWindowSpecV1>(
                     impl_->config.windows);
}

std::uint64_t KLineAggregatorSnapshotV1::bar_count() const noexcept {
    return impl_ == nullptr ? 0U : impl_->bar_count;
}

KLineQueryErrorV1 KLineAggregatorSnapshotV1::GetLatestBar(
    std::uint32_t instrument_id,
    std::uint32_t window_id,
    KLineBarV1* output) const noexcept {
    if (output == nullptr) {
        return KLineQueryErrorV1::kNullOutput;
    }
    *output = KLineBarV1{};
    if (impl_ == nullptr || instrument_id == 0U || window_id == 0U) {
        return KLineQueryErrorV1::kInvalidArgument;
    }
    const SnapshotSeries* const found = FindSnapshotSeries(
        impl_->series, SeriesKey{instrument_id, window_id});
    if (found == nullptr) {
        return KLineQueryErrorV1::kNotFound;
    }
    const KLineBarV1* const latest = FindLatestBar(*found->series);
    if (latest == nullptr) {
        return KLineQueryErrorV1::kNotFound;
    }
    *output = *latest;
    return KLineQueryErrorV1::kNone;
}

KLineQueryErrorV1 KLineAggregatorSnapshotV1::OpenInstrumentCursor(
    std::uint32_t instrument_id,
    std::uint32_t window_id,
    std::unique_ptr<KLineCursorV1>* output) const noexcept {
    if (output == nullptr) {
        return KLineQueryErrorV1::kNullOutput;
    }
    output->reset();
    if (impl_ == nullptr || instrument_id == 0U || window_id == 0U) {
        return KLineQueryErrorV1::kInvalidArgument;
    }
    const SnapshotSeries* const found = FindSnapshotSeries(
        impl_->series, SeriesKey{instrument_id, window_id});
    if (found == nullptr) {
        return KLineQueryErrorV1::kNotFound;
    }
    try {
        auto cursor_impl = std::make_unique<KLineCursorV1::Impl>(
            found->series, impl_->config.maximum_bars_per_read);
        output->reset(new KLineCursorV1(std::move(cursor_impl)));
        return KLineQueryErrorV1::kNone;
    } catch (...) {
        return KLineQueryErrorV1::kResourceExhausted;
    }
}

class KLineAggregatorV1::Impl final {
public:
    Impl(
        KLineAggregatorConfigV1 config,
        std::int64_t midnight_unix_ns)
        : config_(std::move(config)),
          midnight_unix_ns_(midnight_unix_ns),
          capture_identity_(std::make_shared<KLineCaptureIdentity>()) {
        if (config_.instrument_capacity != 0U) {
            hot_instrument_ids_.resize(
                config_.instrument_capacity, 0U);
            hot_series_.resize(
                config_.instrument_capacity *
                    config_.windows.size(),
                nullptr);
            owner_row_capture_states_ =
                std::make_unique<OwnerRowCaptureState[]>(
                    config_.instrument_capacity);
            frozen_series_.resize(
                config_.instrument_capacity *
                config_.windows.size());
        }
    }

    [[nodiscard]] KLineAppendErrorV1 Append(
        const KLineTradeV1& trade,
        std::size_t owner_local_row) {
        if (failed_) {
            return KLineAppendErrorV1::kFailed;
        }
        if (trade.trade_date != config_.trade_date ||
            trade.instrument_id == 0U ||
            trade.event_time_ns_since_midnight >=
                kKLineNanosecondsPerDayV1 ||
            trade.quantity_raw == 0U || trade.source_sequence == 0U ||
            trade.ingress_sequence == 0U) {
            failed_ = true;
            return KLineAppendErrorV1::kInvalidTrade;
        }
        std::int64_t expected_event_time_unix_ns = 0;
        if (!CheckedUnixBoundary(
                midnight_unix_ns_,
                trade.event_time_ns_since_midnight,
                &expected_event_time_unix_ns) ||
            trade.event_time_unix_ns !=
                expected_event_time_unix_ns) {
            failed_ = true;
            return KLineAppendErrorV1::kInvalidTrade;
        }
        const bool use_owner_index =
            owner_local_row !=
            std::numeric_limits<std::size_t>::max();
        const bool owner_index_configured =
            config_.instrument_capacity != 0U;
        if (use_owner_index != owner_index_configured) {
            // Production owner-index state and standalone generic state are
            // intentionally disjoint. Mixing the two would create series
            // that an exact owner cut cannot enumerate.
            failed_ = true;
            return KLineAppendErrorV1::kInvalidTrade;
        }
        if (use_owner_index) {
            if (owner_local_row >=
                    config_.instrument_capacity ||
                hot_instrument_ids_.size() !=
                    config_.instrument_capacity ||
                owner_row_capture_states_ == nullptr ||
                frozen_series_.size() != hot_series_.size()) {
                failed_ = true;
                return KLineAppendErrorV1::kInvalidTrade;
            }
            std::uint32_t& indexed_instrument =
                hot_instrument_ids_[owner_local_row];
            if (indexed_instrument != 0U &&
                indexed_instrument != trade.instrument_id) {
                failed_ = true;
                return KLineAppendErrorV1::kInvalidTrade;
            }
            if (!FreezeOwnerRowIfNeeded(owner_local_row)) {
                failed_ = true;
                return KLineAppendErrorV1::kResourceExhausted;
            }
            if (indexed_instrument == 0U) {
                indexed_instrument = trade.instrument_id;
            }
        }

        try {
            for (std::size_t window_index = 0U;
                 window_index < config_.windows.size();
                 ++window_index) {
                const KLineWindowSpecV1& window =
                    config_.windows[window_index];
                std::shared_ptr<BarSeries>* const series_holder =
                    ResolveSeries(
                        trade.instrument_id,
                        window.window_id,
                        use_owner_index,
                        owner_local_row,
                        window_index);
                const KLineAppendErrorV1 error =
                    AppendWindow(
                        trade,
                        window,
                        midnight_unix_ns_,
                        series_holder);
                if (error != KLineAppendErrorV1::kNone) {
                    failed_ = true;
                    return error;
                }
            }
            return KLineAppendErrorV1::kNone;
        } catch (const std::bad_alloc&) {
            failed_ = true;
            return KLineAppendErrorV1::kResourceExhausted;
        } catch (...) {
            failed_ = true;
            return KLineAppendErrorV1::kResourceExhausted;
        }
    }

    [[nodiscard]] bool FreezeOwnerRowIfNeeded(
        std::size_t owner_local_row) noexcept {
        const std::uint64_t generation =
            active_capture_generation_.load(std::memory_order_acquire);
        if (generation == 0U) {
            return true;
        }
        OwnerRowCaptureState& capture =
            owner_row_capture_states_[owner_local_row];
        if (capture.frozen_generation == generation) {
            return true;
        }
        try {
            std::array<std::shared_ptr<BarSeries>,
                       kKLineMaximumWindowsV1>
                retired;
            {
                std::lock_guard<std::mutex> lock(capture.mutex);
                if (capture.frozen_generation == generation) {
                    return true;
                }
                capture.frozen_instrument_id =
                    hot_instrument_ids_[owner_local_row];
                const std::size_t row_offset =
                    owner_local_row * config_.windows.size();
                for (std::size_t window = 0U;
                     window < config_.windows.size();
                     ++window) {
                    const std::size_t offset = row_offset + window;
                    retired[window] =
                        std::move(frozen_series_[offset]);
                    const std::shared_ptr<BarSeries>* const holder =
                        hot_series_[offset];
                    frozen_series_[offset] =
                        holder == nullptr
                            ? std::shared_ptr<BarSeries>{}
                            : *holder;
                }
                capture.frozen_generation = generation;
            }
            // Release superseded cut references outside the row critical
            // section. The still-published prior generation owns its own
            // references, so this is normally only a bounded refcount drop.
            return true;
        } catch (...) {
            return false;
        }
    }

    [[nodiscard]] std::shared_ptr<BarSeries>* ResolveSeries(
        std::uint32_t instrument_id,
        std::uint32_t window_id,
        bool use_owner_index,
        std::size_t owner_local_row,
        std::size_t window_index) {
        const SeriesKey key{instrument_id, window_id};
        if (!use_owner_index) {
            const auto inserted =
                series_.try_emplace(key, nullptr);
            return &inserted.first->second;
        }
        const std::size_t offset =
            owner_local_row * config_.windows.size() +
            window_index;
        std::shared_ptr<BarSeries>*& cached =
            hot_series_[offset];
        if (cached == nullptr) {
            const auto inserted =
                series_.try_emplace(key, nullptr);
            cached = &inserted.first->second;
        }
        return cached;
    }

    [[nodiscard]] KLineAppendErrorV1 AppendWindow(
        const KLineTradeV1& trade,
        const KLineWindowSpecV1& window,
        std::int64_t midnight_unix_ns,
        std::shared_ptr<BarSeries>* series_holder) {
        const std::uint64_t window_start =
            (trade.event_time_ns_since_midnight / window.duration_ns) *
            window.duration_ns;
        if (window_start >
            std::numeric_limits<std::uint64_t>::max() -
                window.duration_ns) {
            return KLineAppendErrorV1::kNumericOverflow;
        }
        const std::uint64_t window_end =
            window_start + window.duration_ns;

        if (series_holder == nullptr) {
            return KLineAppendErrorV1::kResourceExhausted;
        }
        if (*series_holder == nullptr) {
            if (bar_count_ >= config_.maximum_bars) {
                return KLineAppendErrorV1::kBarCapacity;
            }
            *series_holder = std::make_shared<BarSeries>();
        } else if (!series_holder->unique()) {
            *series_holder =
                std::make_shared<BarSeries>(**series_holder);
        }
        std::shared_ptr<BarSeries>& series = *series_holder;
        if (series == nullptr) {
            return KLineAppendErrorV1::kResourceExhausted;
        }

        auto chunk_iterator = std::lower_bound(
            series->chunks.begin(),
            series->chunks.end(),
            window_start,
            [](const std::shared_ptr<BarChunk>& chunk,
               std::uint64_t value) noexcept {
                return chunk != nullptr && !chunk->bars.empty() &&
                       chunk->bars.back()
                               .window_start_ns_since_midnight <
                           value;
            });
        if (chunk_iterator != series->chunks.end()) {
            std::shared_ptr<BarChunk>& chunk = *chunk_iterator;
            if (chunk == nullptr) {
                return KLineAppendErrorV1::kResourceExhausted;
            }
            const auto bar_iterator = std::lower_bound(
                chunk->bars.begin(),
                chunk->bars.end(),
                window_start,
                [](const KLineBarV1& bar,
                   std::uint64_t value) noexcept {
                    return bar.window_start_ns_since_midnight < value;
                });
            if (bar_iterator != chunk->bars.end() &&
                bar_iterator->window_start_ns_since_midnight ==
                    window_start) {
                if (!chunk.unique()) {
                    chunk = std::make_shared<BarChunk>(*chunk);
                }
                const auto mutable_bar = std::lower_bound(
                    chunk->bars.begin(),
                    chunk->bars.end(),
                    window_start,
                    [](const KLineBarV1& bar,
                       std::uint64_t value) noexcept {
                        return bar.window_start_ns_since_midnight <
                               value;
                    });
                return UpdateBar(&*mutable_bar, trade);
            }
        }

        if (bar_count_ >= config_.maximum_bars) {
            return KLineAppendErrorV1::kBarCapacity;
        }
        KLineBarV1 bar{};
        const KLineAppendErrorV1 build_error = BuildBar(
            trade,
            window,
            window_start,
            window_end,
            midnight_unix_ns,
            &bar);
        if (build_error != KLineAppendErrorV1::kNone) {
            return build_error;
        }
        const KLineAppendErrorV1 insert_error =
            InsertBar(series.get(), std::move(bar));
        if (insert_error != KLineAppendErrorV1::kNone) {
            return insert_error;
        }
        ++bar_count_;
        ++series->bar_count;
        return KLineAppendErrorV1::kNone;
    }

    [[nodiscard]] KLineAppendErrorV1 BuildBar(
        const KLineTradeV1& trade,
        const KLineWindowSpecV1& window,
        std::uint64_t window_start,
        std::uint64_t window_end,
        std::int64_t midnight_unix_ns,
        KLineBarV1* output) const noexcept {
        if (output == nullptr ||
            !CheckedUnixBoundary(
                midnight_unix_ns,
                window_start,
                &output->window_start_unix_ns) ||
            !CheckedUnixBoundary(
                midnight_unix_ns,
                window_end,
                &output->window_end_unix_ns)) {
            return KLineAppendErrorV1::kNumericOverflow;
        }
        output->trade_date = trade.trade_date;
        output->instrument_id = trade.instrument_id;
        output->window_id = window.window_id;
        output->window_duration_ns = window.duration_ns;
        output->window_start_ns_since_midnight = window_start;
        output->window_end_ns_since_midnight = window_end;
        output->open_price_p6 = trade.price_p6;
        output->high_price_p6 = trade.price_p6;
        output->low_price_p6 = trade.price_p6;
        output->close_price_p6 = trade.price_p6;
        output->volume_raw = trade.quantity_raw;
        output->volume_scale = trade.quantity_scale;
        output->quantity_unit = trade.quantity_unit;
        output->trade_count = 1U;
        output->revision = 1U;
        const KLineEventOrderV1 order{
            trade.event_time_ns_since_midnight,
            trade.event_sequence == 0U
                ? trade.source_sequence
                : trade.event_sequence,
            trade.source_sequence,
            trade.ingress_sequence};
        output->first_trade = order;
        output->last_trade = order;
        return KLineAppendErrorV1::kNone;
    }

    [[nodiscard]] KLineAppendErrorV1 UpdateBar(
        KLineBarV1* bar,
        const KLineTradeV1& trade) const noexcept {
        if (bar == nullptr || bar->volume_scale != trade.quantity_scale ||
            bar->quantity_unit != trade.quantity_unit) {
            return KLineAppendErrorV1::kInvalidTrade;
        }
        if (bar->volume_raw >
                std::numeric_limits<std::uint64_t>::max() -
                    trade.quantity_raw ||
            bar->trade_count ==
                std::numeric_limits<std::uint64_t>::max() ||
            bar->revision ==
                std::numeric_limits<std::uint64_t>::max()) {
            return KLineAppendErrorV1::kNumericOverflow;
        }
        bar->high_price_p6 =
            std::max(bar->high_price_p6, trade.price_p6);
        bar->low_price_p6 =
            std::min(bar->low_price_p6, trade.price_p6);
        const KLineEventOrderV1 order{
            trade.event_time_ns_since_midnight,
            trade.event_sequence == 0U
                ? trade.source_sequence
                : trade.event_sequence,
            trade.source_sequence,
            trade.ingress_sequence};
        if (EventOrderLess(order, bar->first_trade)) {
            bar->first_trade = order;
            bar->open_price_p6 = trade.price_p6;
        }
        if (EventOrderLess(bar->last_trade, order)) {
            bar->last_trade = order;
            bar->close_price_p6 = trade.price_p6;
        }
        bar->volume_raw += trade.quantity_raw;
        ++bar->trade_count;
        ++bar->revision;
        return KLineAppendErrorV1::kNone;
    }

    [[nodiscard]] KLineAppendErrorV1 InsertBar(
        BarSeries* series,
        KLineBarV1 bar) {
        if (series == nullptr) {
            return KLineAppendErrorV1::kResourceExhausted;
        }
        if (series->chunks.empty() ||
            series->chunks.back()->bars.back()
                    .window_start_ns_since_midnight <
                bar.window_start_ns_since_midnight) {
            if (series->chunks.empty() ||
                series->chunks.back()->bars.size() >=
                    config_.bars_per_chunk) {
                auto chunk = std::make_shared<BarChunk>();
                chunk->bars.reserve(config_.bars_per_chunk);
                chunk->bars.push_back(std::move(bar));
                series->chunks.push_back(std::move(chunk));
                return KLineAppendErrorV1::kNone;
            }
            std::shared_ptr<BarChunk>& tail = series->chunks.back();
            if (!tail.unique()) {
                tail = std::make_shared<BarChunk>(*tail);
                tail->bars.reserve(config_.bars_per_chunk);
            }
            tail->bars.push_back(std::move(bar));
            return KLineAppendErrorV1::kNone;
        }

        auto chunk_iterator = std::lower_bound(
            series->chunks.begin(),
            series->chunks.end(),
            bar.window_start_ns_since_midnight,
            [](const std::shared_ptr<BarChunk>& chunk,
               std::uint64_t value) noexcept {
                return chunk->bars.back()
                           .window_start_ns_since_midnight <
                       value;
            });
        if (chunk_iterator == series->chunks.end()) {
            return KLineAppendErrorV1::kResourceExhausted;
        }
        if (!chunk_iterator->unique()) {
            *chunk_iterator =
                std::make_shared<BarChunk>(**chunk_iterator);
        }
        std::shared_ptr<BarChunk>& chunk = *chunk_iterator;
        const auto position = std::lower_bound(
            chunk->bars.begin(),
            chunk->bars.end(),
            bar.window_start_ns_since_midnight,
            [](const KLineBarV1& existing,
               std::uint64_t value) noexcept {
                return existing.window_start_ns_since_midnight < value;
            });
        chunk->bars.insert(position, std::move(bar));
        if (chunk->bars.size() <= config_.bars_per_chunk) {
            return KLineAppendErrorV1::kNone;
        }
        auto split = std::make_shared<BarChunk>();
        split->bars.reserve(config_.bars_per_chunk);
        const std::size_t split_at = chunk->bars.size() / 2U;
        split->bars.insert(
            split->bars.end(),
            std::make_move_iterator(
                chunk->bars.begin() +
                static_cast<std::ptrdiff_t>(split_at)),
            std::make_move_iterator(chunk->bars.end()));
        chunk->bars.erase(
            chunk->bars.begin() +
                static_cast<std::ptrdiff_t>(split_at),
            chunk->bars.end());
        series->chunks.insert(
            chunk_iterator + 1, std::move(split));
        return KLineAppendErrorV1::kNone;
    }

    KLineAggregatorConfigV1 config_;
    std::int64_t midnight_unix_ns_ = 0;
    std::shared_ptr<const KLineCaptureIdentity> capture_identity_;
    std::map<SeriesKey, std::shared_ptr<BarSeries>> series_;
    std::vector<std::uint32_t> hot_instrument_ids_;
    std::vector<std::shared_ptr<BarSeries>*> hot_series_;
    std::unique_ptr<OwnerRowCaptureState[]> owner_row_capture_states_;
    std::vector<std::shared_ptr<BarSeries>> frozen_series_;
    std::atomic<std::uint64_t> active_capture_generation_{0U};
    std::atomic<std::uint64_t> materialized_capture_generation_{0U};
    std::uint64_t last_captured_generation_ = 0U;
    std::uint64_t bar_count_ = 0U;
    bool failed_ = false;
};

KLineAggregatorV1::KLineAggregatorV1(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

KLineAggregatorV1::~KLineAggregatorV1() = default;

KLineCreateErrorV1 KLineAggregatorV1::Create(
    KLineAggregatorConfigV1 config,
    std::unique_ptr<KLineAggregatorV1>* output) noexcept {
    if (output == nullptr) {
        return KLineCreateErrorV1::kNullOutput;
    }
    output->reset();
    if (!ValidConfig(config)) {
        return KLineCreateErrorV1::kInvalidConfiguration;
    }
    try {
        std::int64_t midnight_unix_ns = 0;
        if (!FixedUtc8MidnightUnixNs(
                config.trade_date, &midnight_unix_ns)) {
            return KLineCreateErrorV1::kInvalidConfiguration;
        }
        std::sort(
            config.windows.begin(),
            config.windows.end(),
            [](const KLineWindowSpecV1& lhs,
               const KLineWindowSpecV1& rhs) noexcept {
                return lhs.window_id < rhs.window_id;
            });
        auto impl = std::make_unique<Impl>(
            std::move(config), midnight_unix_ns);
        output->reset(new KLineAggregatorV1(std::move(impl)));
        return KLineCreateErrorV1::kNone;
    } catch (...) {
        return KLineCreateErrorV1::kResourceExhausted;
    }
}

KLineAppendErrorV1 KLineAggregatorV1::Append(
    const KLineTradeV1& trade) noexcept {
    return impl_ == nullptr
               ? KLineAppendErrorV1::kFailed
               : impl_->Append(
                     trade,
                     std::numeric_limits<std::size_t>::max());
}

KLineAppendErrorV1 KLineAggregatorV1::Append(
    const KLineTradeV1& trade,
    std::size_t owner_local_row) noexcept {
    return impl_ == nullptr
               ? KLineAppendErrorV1::kFailed
               : impl_->Append(trade, owner_local_row);
}

KLineCaptureErrorV1 KLineAggregatorV1::Capture(
    std::shared_ptr<const KLineAggregatorSnapshotV1>* output) noexcept {
    if (output == nullptr) {
        return KLineCaptureErrorV1::kNullOutput;
    }
    output->reset();
    if (impl_ == nullptr || impl_->failed_) {
        return KLineCaptureErrorV1::kFailed;
    }
    if (impl_->config_.instrument_capacity != 0U) {
        // Owner-index production capture must use the constant-work marker
        // API. Falling back to map enumeration would put O(active-series)
        // work back on the realtime owner.
        impl_->failed_ = true;
        return KLineCaptureErrorV1::kFailed;
    }
    try {
        auto snapshot_impl =
            std::make_unique<KLineAggregatorSnapshotV1::Impl>();
        snapshot_impl->config = impl_->config_;
        snapshot_impl->bar_count = impl_->bar_count_;
        snapshot_impl->series.reserve(impl_->series_.size());
        for (const auto& entry : impl_->series_) {
            SnapshotSeries item{};
            item.key = entry.first;
            item.series = entry.second;
            snapshot_impl->series.push_back(std::move(item));
        }
        output->reset(new KLineAggregatorSnapshotV1(
            std::move(snapshot_impl)));
        return KLineCaptureErrorV1::kNone;
    } catch (...) {
        impl_->failed_ = true;
        return KLineCaptureErrorV1::kResourceExhausted;
    }
}

KLineCaptureErrorV1 KLineAggregatorV1::CaptureOwnerCut(
    std::uint64_t generation,
    std::size_t represented_owner_rows,
    std::unique_ptr<KLineAggregatorCutV1>* output) noexcept {
    if (output == nullptr) {
        return KLineCaptureErrorV1::kNullOutput;
    }
    output->reset();
    if (impl_ == nullptr || impl_->failed_ ||
        impl_->config_.instrument_capacity == 0U ||
        impl_->owner_row_capture_states_ == nullptr ||
        generation == 0U ||
        generation <= impl_->last_captured_generation_ ||
        represented_owner_rows >
            impl_->config_.instrument_capacity) {
        return KLineCaptureErrorV1::kFailed;
    }
    const std::uint64_t active =
        impl_->active_capture_generation_.load(
            std::memory_order_acquire);
    if (active !=
        impl_->materialized_capture_generation_.load(
            std::memory_order_acquire)) {
        // One frozen slot exists per row. A later cut cannot overwrite it
        // until the prior token has been materialized into independently
        // owning shared_ptrs.
        return KLineCaptureErrorV1::kFailed;
    }
    try {
        auto cut_impl = std::make_unique<KLineAggregatorCutV1::Impl>();
        cut_impl->identity = impl_->capture_identity_;
        cut_impl->generation = generation;
        cut_impl->bar_count = impl_->bar_count_;
        cut_impl->represented_owner_rows = represented_owner_rows;
        auto cut = std::unique_ptr<KLineAggregatorCutV1>(
            new KLineAggregatorCutV1(std::move(cut_impl)));

        impl_->active_capture_generation_.store(
            generation, std::memory_order_release);
        impl_->last_captured_generation_ = generation;
        *output = std::move(cut);
        return KLineCaptureErrorV1::kNone;
    } catch (...) {
        impl_->failed_ = true;
        return KLineCaptureErrorV1::kResourceExhausted;
    }
}

KLineCaptureErrorV1 KLineAggregatorV1::MaterializeOwnerCut(
    const KLineAggregatorCutV1& cut,
    std::shared_ptr<const KLineAggregatorSnapshotV1>* output)
    noexcept {
    if (output == nullptr) {
        return KLineCaptureErrorV1::kNullOutput;
    }
    output->reset();
    if (impl_ == nullptr || cut.impl_ == nullptr ||
        impl_->config_.instrument_capacity == 0U ||
        impl_->owner_row_capture_states_ == nullptr ||
        cut.impl_->identity == nullptr ||
        cut.impl_->identity.get() != impl_->capture_identity_.get() ||
        cut.impl_->identity.owner_before(impl_->capture_identity_) ||
        impl_->capture_identity_.owner_before(cut.impl_->identity) ||
        cut.impl_->generation == 0U ||
        cut.impl_->represented_owner_rows >
            impl_->config_.instrument_capacity ||
        impl_->active_capture_generation_.load(
            std::memory_order_acquire) != cut.impl_->generation) {
        return KLineCaptureErrorV1::kFailed;
    }

    try {
        auto snapshot_impl =
            std::make_unique<KLineAggregatorSnapshotV1::Impl>();
        snapshot_impl->config = impl_->config_;
        snapshot_impl->bar_count = cut.impl_->bar_count;
        snapshot_impl->series.reserve(
            cut.impl_->represented_owner_rows *
            impl_->config_.windows.size());

        std::uint64_t materialized_bar_count = 0U;
        for (std::size_t row = 0U;
             row < cut.impl_->represented_owner_rows;
             ++row) {
            std::uint32_t instrument_id = 0U;
            std::array<std::shared_ptr<const BarSeries>,
                       kKLineMaximumWindowsV1>
                captured_series;
            {
                OwnerRowCaptureState& capture =
                    impl_->owner_row_capture_states_[row];
                std::lock_guard<std::mutex> lock(capture.mutex);
                if (capture.frozen_generation ==
                    cut.impl_->generation) {
                    instrument_id = capture.frozen_instrument_id;
                    const std::size_t row_offset =
                        row * impl_->config_.windows.size();
                    for (std::size_t window = 0U;
                         window < impl_->config_.windows.size();
                         ++window) {
                        captured_series[window] =
                            impl_->frozen_series_[
                                row_offset + window];
                    }
                } else {
                    if (capture.frozen_generation >
                        cut.impl_->generation) {
                        return KLineCaptureErrorV1::kFailed;
                    }
                    instrument_id =
                        impl_->hot_instrument_ids_[row];
                    const std::size_t row_offset =
                        row * impl_->config_.windows.size();
                    for (std::size_t window = 0U;
                         window < impl_->config_.windows.size();
                         ++window) {
                        const std::shared_ptr<BarSeries>* const holder =
                            impl_->hot_series_[row_offset + window];
                        if (holder != nullptr) {
                            captured_series[window] = *holder;
                        }
                    }
                }
            }

            bool any_series = false;
            for (std::size_t window = 0U;
                 window < impl_->config_.windows.size();
                 ++window) {
                const auto& series = captured_series[window];
                if (series == nullptr) {
                    continue;
                }
                any_series = true;
                if (instrument_id == 0U || series->bar_count == 0U ||
                    materialized_bar_count >
                        std::numeric_limits<std::uint64_t>::max() -
                            series->bar_count) {
                    return KLineCaptureErrorV1::kFailed;
                }
                materialized_bar_count += series->bar_count;
                SnapshotSeries item{};
                item.key = SeriesKey{
                    instrument_id,
                    impl_->config_.windows[window].window_id};
                item.series = series;
                snapshot_impl->series.push_back(std::move(item));
            }
            if (instrument_id == 0U && any_series) {
                return KLineCaptureErrorV1::kFailed;
            }
        }
        if (materialized_bar_count != cut.impl_->bar_count) {
            return KLineCaptureErrorV1::kFailed;
        }
        std::sort(
            snapshot_impl->series.begin(),
            snapshot_impl->series.end(),
            [](const SnapshotSeries& left,
               const SnapshotSeries& right) noexcept {
                return left.key < right.key;
            });
        for (std::size_t index = 1U;
             index < snapshot_impl->series.size();
             ++index) {
            const SeriesKey& previous =
                snapshot_impl->series[index - 1U].key;
            const SeriesKey& current =
                snapshot_impl->series[index].key;
            if (previous.instrument_id == current.instrument_id &&
                previous.window_id == current.window_id) {
                return KLineCaptureErrorV1::kFailed;
            }
        }

        std::shared_ptr<const KLineAggregatorSnapshotV1> snapshot(
            new KLineAggregatorSnapshotV1(
                std::move(snapshot_impl)));
        *output = std::move(snapshot);
        impl_->materialized_capture_generation_.store(
            cut.impl_->generation, std::memory_order_release);
        return KLineCaptureErrorV1::kNone;
    } catch (...) {
        output->reset();
        return KLineCaptureErrorV1::kResourceExhausted;
    }
}

const KLineAggregatorConfigV1& KLineAggregatorV1::config()
    const noexcept {
    return impl_->config_;
}

std::uint64_t KLineAggregatorV1::bar_count() const noexcept {
    return impl_ == nullptr ? 0U : impl_->bar_count_;
}

}  // namespace l2flow::market
