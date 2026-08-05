#include "l2flow/market/mutable_kline_history_v1.h"

#include <algorithm>
#include <atomic>
#include <map>
#include <mutex>
#include <new>
#include <thread>
#include <tuple>
#include <type_traits>
#include <utility>

namespace l2flow::market {
namespace {

static_assert(std::is_nothrow_move_constructible_v<KLineMutationV1>);

[[nodiscard]] bool ZeroIdentity(
    const l2flow::common::Identity128& identity) noexcept {
    return std::all_of(
        identity.begin(), identity.end(),
        [](std::byte value) noexcept { return value == std::byte{0U}; });
}

[[nodiscard]] bool ValidTradeDate(std::uint32_t value) noexcept {
    const std::uint32_t year = value / 10'000U;
    const std::uint32_t month = (value / 100U) % 100U;
    const std::uint32_t day = value % 100U;
    if (year < 1992U || year > 2200U || month == 0U ||
        month > 12U || day == 0U) {
        return false;
    }
    constexpr std::uint32_t days_by_month[12U] = {
        31U, 28U, 31U, 30U, 31U, 30U,
        31U, 31U, 30U, 31U, 30U, 31U};
    std::uint32_t maximum_day = days_by_month[month - 1U];
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
    constexpr std::int64_t seconds_per_day = 86'400LL;
    constexpr std::int64_t utc8_offset_seconds = 8LL * 3'600LL;
    constexpr std::int64_t nanoseconds_per_second = 1'000'000'000LL;
    *output =
        (days_since_epoch * seconds_per_day - utc8_offset_seconds) *
        nanoseconds_per_second;
    return true;
}

[[nodiscard]] bool CheckedBoundary(
    std::int64_t midnight,
    std::uint64_t offset,
    std::int64_t* output) noexcept {
    if (output == nullptr ||
        offset > static_cast<std::uint64_t>(
                     std::numeric_limits<std::int64_t>::max())) {
        return false;
    }
    const auto signed_offset = static_cast<std::int64_t>(offset);
    if (midnight >
        std::numeric_limits<std::int64_t>::max() - signed_offset) {
        return false;
    }
    *output = midnight + signed_offset;
    return true;
}

[[nodiscard]] bool TradeUidLess(
    const KLineTradeUidV1& lhs,
    const KLineTradeUidV1& rhs) noexcept {
    return std::tie(
               lhs.instrument_id,
               lhs.channel,
               lhs.business_sequence) <
           std::tie(
               rhs.instrument_id,
               rhs.channel,
               rhs.business_sequence);
}

struct TradeUidComparator final {
    [[nodiscard]] bool operator()(
        const KLineTradeUidV1& lhs,
        const KLineTradeUidV1& rhs) const noexcept {
        return TradeUidLess(lhs, rhs);
    }
};

struct BarKeyComparator final {
    [[nodiscard]] bool operator()(
        const KLineBarKeyV1& lhs,
        const KLineBarKeyV1& rhs) const noexcept {
        return KLineBarKeyLessV1(lhs, rhs);
    }
};

[[nodiscard]] KLineBarKeyV1 BarKey(
    const KLineBarV1& bar) noexcept {
    return KLineBarKeyV1{
        bar.instrument_id,
        bar.window_id,
        bar.window_start_ns_since_midnight};
}

struct KLineBlockDataV1 final {
    std::vector<KLineBarV1> bars;
};

[[nodiscard]] KLineTradeUidV1 TradeUid(
    const CompactFastTickV1& tick) noexcept {
    return KLineTradeUidV1{
        tick.instrument_id,
        tick.business_sequence.channel,
        tick.business_sequence.value};
}

[[nodiscard]] KLineEventOrderV1 TradeOrder(
    const CompactFastTickV1& tick) noexcept {
    return KLineEventOrderV1{
        tick.event_time_ns_since_midnight,
        static_cast<std::uint64_t>(tick.business_sequence.value),
        tick.source_sequence,
        tick.arrival_id,
        tick.business_sequence.channel};
}

[[nodiscard]] bool TradeOrderLess(
    const KLineEventOrderV1& lhs,
    const KLineEventOrderV1& rhs) noexcept {
    return std::tie(
               lhs.event_time_ns_since_midnight,
               lhs.channel,
               lhs.event_sequence,
               lhs.source_sequence,
               lhs.ingress_sequence) <
           std::tie(
               rhs.event_time_ns_since_midnight,
               rhs.channel,
               rhs.event_sequence,
               rhs.source_sequence,
               rhs.ingress_sequence);
}

[[nodiscard]] bool SameOrder(
    const KLineEventOrderV1& lhs,
    const KLineEventOrderV1& rhs) noexcept {
    return lhs.event_time_ns_since_midnight ==
               rhs.event_time_ns_since_midnight &&
           lhs.channel == rhs.channel &&
           lhs.event_sequence == rhs.event_sequence &&
           lhs.source_sequence == rhs.source_sequence &&
           lhs.ingress_sequence == rhs.ingress_sequence;
}

[[nodiscard]] bool SameBarIgnoringRevision(
    const KLineBarV1& lhs,
    const KLineBarV1& rhs) noexcept {
    return lhs.trade_date == rhs.trade_date &&
           lhs.instrument_id == rhs.instrument_id &&
           lhs.window_id == rhs.window_id &&
           lhs.window_duration_ns == rhs.window_duration_ns &&
           lhs.window_start_ns_since_midnight ==
               rhs.window_start_ns_since_midnight &&
           lhs.window_end_ns_since_midnight ==
               rhs.window_end_ns_since_midnight &&
           lhs.window_start_unix_ns == rhs.window_start_unix_ns &&
           lhs.window_end_unix_ns == rhs.window_end_unix_ns &&
           lhs.open_price_p6 == rhs.open_price_p6 &&
           lhs.high_price_p6 == rhs.high_price_p6 &&
           lhs.low_price_p6 == rhs.low_price_p6 &&
           lhs.close_price_p6 == rhs.close_price_p6 &&
           lhs.volume_raw == rhs.volume_raw &&
           lhs.volume_scale == rhs.volume_scale &&
           lhs.quantity_unit == rhs.quantity_unit &&
           lhs.trade_count == rhs.trade_count &&
           SameOrder(lhs.first_trade, rhs.first_trade) &&
           SameOrder(lhs.last_trade, rhs.last_trade);
}

[[nodiscard]] bool ValidKLineEnvelope(
    const CompactFastTickV1& tick) noexcept {
    const bool source_matches =
        (tick.source == FastTickSourceV1::kShanghaiTick &&
         tick.market == MarketV1::kShanghai &&
         tick.kind == MarketEventKindV1::kShanghaiTick) ||
        (tick.source == FastTickSourceV1::kShenzhenTick &&
         tick.market == MarketV1::kShenzhen &&
         (tick.kind == MarketEventKindV1::kShenzhenOrder ||
          tick.kind == MarketEventKindV1::kShenzhenTransaction));
    return tick.instrument_id != 0U &&
           tick.ordinal < static_cast<std::size_t>(
                              std::numeric_limits<std::uint32_t>::max()) &&
           tick.instrument_id ==
               static_cast<std::uint32_t>(tick.ordinal + 1U) &&
           tick.business_sequence.channel > 0 &&
           tick.business_sequence.value > 0 &&
           tick.arrival_id > 0U &&
           tick.arrival_id !=
               std::numeric_limits<std::uint64_t>::max() &&
           tick.source_stream_id > 0U && tick.source_sequence > 0U &&
           source_matches;
}

void AtomicMaximum(
    std::atomic<std::uint64_t>* value,
    std::uint64_t candidate) noexcept {
    std::uint64_t current = value->load(std::memory_order_relaxed);
    while (current < candidate &&
           !value->compare_exchange_weak(
               current,
               candidate,
               std::memory_order_release,
               std::memory_order_relaxed)) {
    }
}

void RestoreRepairRequiredUnlessTerminal(
    std::atomic<EventRepairStateV1>* state) noexcept {
    EventRepairStateV1 current = state->load(std::memory_order_acquire);
    for (;;) {
        if (current == EventRepairStateV1::kSourceConflict ||
            current == EventRepairStateV1::kUnrecoverable ||
            current == EventRepairStateV1::kRepairRequired) {
            return;
        }
        if (state->compare_exchange_weak(
                current,
                EventRepairStateV1::kRepairRequired,
                std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            return;
        }
    }
}

void MarkSourceConflictUnlessUnrecoverable(
    std::atomic<EventRepairStateV1>* state) noexcept {
    EventRepairStateV1 current = state->load(std::memory_order_acquire);
    for (;;) {
        if (current == EventRepairStateV1::kUnrecoverable ||
            current == EventRepairStateV1::kSourceConflict) {
            return;
        }
        if (state->compare_exchange_weak(
                current,
                EventRepairStateV1::kSourceConflict,
                std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            return;
        }
    }
}

void CooperativeRepairYield(
    std::size_t* records_since_yield,
    std::size_t record_budget) noexcept {
    ++(*records_since_yield);
    if (*records_since_yield >= record_budget) {
        *records_since_yield = 0U;
        std::this_thread::yield();
    }
}

template <typename T>
[[nodiscard]] bool ReserveForAppend(
    std::vector<T>* values,
    std::size_t additional,
    std::size_t maximum_records) {
    if (values->size() > maximum_records ||
        additional > maximum_records - values->size()) {
        return false;
    }
    const std::size_t required = values->size() + additional;
    if (required <= values->capacity()) {
        return true;
    }
    constexpr std::size_t initial_capacity = 64U;
    std::size_t grown = values->capacity();
    if (grown < initial_capacity) {
        grown = initial_capacity;
    } else if (grown <=
               (std::numeric_limits<std::size_t>::max() - 1U) / 2U) {
        grown += grown / 2U + 1U;
    } else {
        grown = std::numeric_limits<std::size_t>::max();
    }
    grown = std::min(grown, maximum_records);
    values->reserve(std::max(required, grown));
    return true;
}

[[nodiscard]] bool ChangeSequenceCanAppend(
    std::size_t current,
    std::size_t additional) noexcept {
    static_assert(sizeof(std::size_t) <= sizeof(std::uint64_t));
    constexpr std::uint64_t maximum_tail =
        std::numeric_limits<std::uint64_t>::max() - 1U;
    const std::uint64_t current_u64 =
        static_cast<std::uint64_t>(current);
    const std::uint64_t additional_u64 =
        static_cast<std::uint64_t>(additional);
    return current_u64 <= maximum_tail &&
           additional_u64 <= maximum_tail - current_u64;
}

}  // namespace

bool KLineBarKeyLessV1(
    const KLineBarKeyV1& lhs,
    const KLineBarKeyV1& rhs) noexcept {
    return std::tie(
               lhs.instrument_id,
               lhs.window_id,
               lhs.window_start_ns_since_midnight) <
           std::tie(
               rhs.instrument_id,
               rhs.window_id,
               rhs.window_start_ns_since_midnight);
}

class KLineStableRootV1::Impl final {
public:
    std::uint32_t instrument_id = 0U;
    std::uint64_t included_change_sequence = 0U;
    std::uint64_t bar_count = 0U;
    std::vector<std::shared_ptr<const KLineBlockDataV1>> blocks;
};

KLineStableRootV1::KLineStableRootV1(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

KLineStableRootV1::~KLineStableRootV1() = default;

std::uint32_t KLineStableRootV1::instrument_id() const noexcept {
    return impl_ == nullptr ? 0U : impl_->instrument_id;
}

std::uint64_t KLineStableRootV1::included_change_sequence()
    const noexcept {
    return impl_ == nullptr ? 0U : impl_->included_change_sequence;
}

std::uint64_t KLineStableRootV1::bar_count() const noexcept {
    return impl_ == nullptr ? 0U : impl_->bar_count;
}

bool KLineStableRootV1::CopyBars(
    std::vector<KLineBarV1>* output) const noexcept {
    if (output == nullptr) {
        return false;
    }
    output->clear();
    if (impl_ == nullptr) {
        return false;
    }
    try {
        if (impl_->bar_count > static_cast<std::uint64_t>(
                                   std::numeric_limits<std::size_t>::max())) {
            return false;
        }
        output->reserve(static_cast<std::size_t>(impl_->bar_count));
        for (const auto& block : impl_->blocks) {
            output->insert(
                output->end(), block->bars.begin(), block->bars.end());
        }
        return true;
    } catch (...) {
        output->clear();
        return false;
    }
}

bool KLineStableRootV1::Find(
    const KLineBarKeyV1& key,
    KLineBarV1* output) const noexcept {
    if (impl_ == nullptr || output == nullptr ||
        key.instrument_id != impl_->instrument_id) {
        return false;
    }
    const auto block = std::lower_bound(
        impl_->blocks.begin(),
        impl_->blocks.end(),
        key,
        [](const std::shared_ptr<const KLineBlockDataV1>& block_value,
           const KLineBarKeyV1& key_value) noexcept {
            return KLineBarKeyLessV1(
                BarKey(block_value->bars.back()), key_value);
        });
    if (block == impl_->blocks.end()) {
        return false;
    }
    const auto found = std::lower_bound(
        (*block)->bars.begin(),
        (*block)->bars.end(),
        key,
        [](const KLineBarV1& bar,
           const KLineBarKeyV1& candidate) noexcept {
            return KLineBarKeyLessV1(BarKey(bar), candidate);
        });
    if (found == (*block)->bars.end() || BarKey(*found) != key) {
        return false;
    }
    *output = *found;
    return true;
}

std::string_view MutableKLineHistoryCreateErrorNameV1(
    MutableKLineHistoryCreateErrorV1 error) noexcept {
    switch (error) {
        case MutableKLineHistoryCreateErrorV1::kNone:
            return "none";
        case MutableKLineHistoryCreateErrorV1::kNullOutput:
            return "null_output";
        case MutableKLineHistoryCreateErrorV1::kInvalidConfiguration:
            return "invalid_configuration";
        case MutableKLineHistoryCreateErrorV1::kResourceExhausted:
            return "resource_exhausted";
    }
    return "unknown";
}

std::string_view MutableKLineHistoryErrorNameV1(
    MutableKLineHistoryErrorV1 error) noexcept {
    switch (error) {
        case MutableKLineHistoryErrorV1::kNone:
            return "none";
        case MutableKLineHistoryErrorV1::kNullOutput:
            return "null_output";
        case MutableKLineHistoryErrorV1::kInvalidInput:
            return "invalid_input";
        case MutableKLineHistoryErrorV1::kWrongWorker:
            return "wrong_worker";
        case MutableKLineHistoryErrorV1::kNotLive:
            return "not_live";
        case MutableKLineHistoryErrorV1::kNotTrade:
            return "not_trade";
        case MutableKLineHistoryErrorV1::kTradeCapacity:
            return "trade_capacity";
        case MutableKLineHistoryErrorV1::kBarCapacity:
            return "bar_capacity";
        case MutableKLineHistoryErrorV1::kNumericOverflow:
            return "numeric_overflow";
        case MutableKLineHistoryErrorV1::kSourceConflict:
            return "source_conflict";
        case MutableKLineHistoryErrorV1::kFastCoverageLost:
            return "fast_coverage_lost";
        case MutableKLineHistoryErrorV1::kChangeCapacity:
            return "change_capacity";
        case MutableKLineHistoryErrorV1::kCursorMismatch:
            return "cursor_mismatch";
        case MutableKLineHistoryErrorV1::kBatchLimitExceeded:
            return "batch_limit_exceeded";
        case MutableKLineHistoryErrorV1::kResourceExhausted:
            return "resource_exhausted";
    }
    return "unknown";
}

class MutableKLineHistoryV1::Impl final {
public:
    using TradeMap = std::map<
        KLineTradeUidV1, CompactFastTickV1, TradeUidComparator>;
    using BarMap = std::map<KLineBarKeyV1, KLineBarV1, BarKeyComparator>;

    struct WorkingState final {
        TradeMap trades;
        BarMap bars;
        std::map<std::uint32_t, std::uint64_t> latest_window_start;
    };

    struct InstrumentState final {
        std::unique_ptr<WorkingState> working;
        std::atomic<std::shared_ptr<const KLineStableRootV1>> root;
        std::atomic_flag writer = ATOMIC_FLAG_INIT;
        mutable std::mutex changes_mutex;
        std::vector<KLineMutationV1> changes;
        std::uint64_t next_transaction_id = 1U;
        std::atomic<EventRepairStateV1> repair_state{
            EventRepairStateV1::kLive};
        std::atomic<std::uint64_t> repair_through{0U};
    };

    Impl(MutableKLineHistoryConfigV1 config, std::int64_t midnight)
        : config_(std::move(config)),
          midnight_unix_ns_(midnight),
          instruments_(std::make_unique<InstrumentState[]>(
              config_.instrument_count)) {}

    [[nodiscard]] std::shared_ptr<const KLineStableRootV1> BuildRoot(
        std::uint32_t instrument_id,
        std::uint64_t included_change_sequence,
        const BarMap& bars) {
        auto impl = std::make_unique<KLineStableRootV1::Impl>();
        impl->instrument_id = instrument_id;
        impl->included_change_sequence = included_change_sequence;
        impl->bar_count = bars.size();
        const std::size_t block_count =
            bars.size() / config_.stable_block_bars +
            (bars.size() % config_.stable_block_bars == 0U ? 0U : 1U);
        impl->blocks.reserve(block_count);
        std::shared_ptr<KLineBlockDataV1> block;
        for (const auto& [key, bar] : bars) {
            static_cast<void>(key);
            if (block == nullptr ||
                block->bars.size() >= config_.stable_block_bars) {
                block = std::make_shared<KLineBlockDataV1>();
                block->bars.reserve(config_.stable_block_bars);
                impl->blocks.push_back(block);
            }
            block->bars.push_back(bar);
        }
        return std::shared_ptr<const KLineStableRootV1>(
            new KLineStableRootV1(std::move(impl)));
    }

    [[nodiscard]] std::shared_ptr<const KLineStableRootV1>
    ApplyBarsToRoot(
        const KLineStableRootV1& current,
        std::span<const KLineBarV1> changed,
        std::uint64_t included_change_sequence) {
        auto next = std::make_unique<KLineStableRootV1::Impl>();
        next->instrument_id = current.impl_->instrument_id;
        next->included_change_sequence = included_change_sequence;
        next->bar_count = current.impl_->bar_count;
        next->blocks = current.impl_->blocks;

        for (const KLineBarV1& bar : changed) {
            const KLineBarKeyV1 key = BarKey(bar);
            auto block = std::lower_bound(
                next->blocks.begin(),
                next->blocks.end(),
                key,
                [](const std::shared_ptr<const KLineBlockDataV1>& value,
                   const KLineBarKeyV1& candidate) noexcept {
                    return KLineBarKeyLessV1(
                        BarKey(value->bars.back()), candidate);
                });
            if (block == next->blocks.end()) {
                if (next->blocks.empty() ||
                    next->blocks.back()->bars.size() >=
                        config_.stable_block_bars) {
                    auto created =
                        std::make_shared<KLineBlockDataV1>();
                    created->bars.reserve(config_.stable_block_bars);
                    created->bars.push_back(bar);
                    next->blocks.push_back(std::move(created));
                } else {
                    auto replacement =
                        std::make_shared<KLineBlockDataV1>(
                            *next->blocks.back());
                    replacement->bars.reserve(
                        config_.stable_block_bars);
                    replacement->bars.push_back(bar);
                    next->blocks.back() = std::move(replacement);
                }
                ++next->bar_count;
                continue;
            }

            auto replacement = std::make_shared<KLineBlockDataV1>(
                **block);
            auto position = std::lower_bound(
                replacement->bars.begin(),
                replacement->bars.end(),
                key,
                [](const KLineBarV1& value,
                   const KLineBarKeyV1& candidate) noexcept {
                    return KLineBarKeyLessV1(
                        BarKey(value), candidate);
                });
            if (position != replacement->bars.end() &&
                BarKey(*position) == key) {
                *position = bar;
                *block = std::move(replacement);
                continue;
            }
            replacement->bars.insert(position, bar);
            ++next->bar_count;
            if (replacement->bars.size() <=
                config_.stable_block_bars) {
                *block = std::move(replacement);
                continue;
            }
            auto right = std::make_shared<KLineBlockDataV1>();
            right->bars.reserve(config_.stable_block_bars);
            const std::size_t split_at = replacement->bars.size() / 2U;
            right->bars.insert(
                right->bars.end(),
                std::make_move_iterator(
                    replacement->bars.begin() +
                    static_cast<std::ptrdiff_t>(split_at)),
                std::make_move_iterator(replacement->bars.end()));
            replacement->bars.erase(
                replacement->bars.begin() +
                    static_cast<std::ptrdiff_t>(split_at),
                replacement->bars.end());
            *block = std::move(replacement);
            next->blocks.insert(block + 1, std::move(right));
        }
        return std::shared_ptr<const KLineStableRootV1>(
            new KLineStableRootV1(std::move(next)));
    }

    void CommitStagedAndPublish(
        InstrumentState* state,
        std::vector<KLineMutationV1>* staged,
        std::shared_ptr<const KLineStableRootV1> root) {
        std::lock_guard<std::mutex> lock(state->changes_mutex);
        for (KLineMutationV1& mutation : *staged) {
            state->changes.push_back(std::move(mutation));
        }
        // See the Event equivalent: readers remain lock-free when acquiring
        // a root, while ReadChanges cannot pass this lock until the matching
        // log prefix and root are both published.
        state->root.store(std::move(root), std::memory_order_release);
        staged->clear();
    }

    [[nodiscard]] MutableKLineHistoryErrorV1 ApplyWindow(
        const CompactFastTickV1& tick,
        const KLineWindowSpecV1& window,
        BarMap* bars,
        KLineBarV1** output,
        bool* historical_window) const noexcept {
        const std::uint64_t start =
            (tick.event_time_ns_since_midnight / window.duration_ns) *
            window.duration_ns;
        if (start > std::numeric_limits<std::uint64_t>::max() -
                        window.duration_ns) {
            return MutableKLineHistoryErrorV1::kNumericOverflow;
        }
        const std::uint64_t end = start + window.duration_ns;
        const KLineBarKeyV1 key{
            tick.instrument_id, window.window_id, start};
        auto found = bars->find(key);
        if (found == bars->end()) {
            if (bars->size() >=
                config_.maximum_bars_per_instrument) {
                return MutableKLineHistoryErrorV1::kBarCapacity;
            }
            KLineBarV1 bar{};
            bar.trade_date = tick.trade_date;
            bar.instrument_id = tick.instrument_id;
            bar.window_id = window.window_id;
            bar.window_duration_ns = window.duration_ns;
            bar.window_start_ns_since_midnight = start;
            bar.window_end_ns_since_midnight = end;
            if (!CheckedBoundary(
                    midnight_unix_ns_,
                    start,
                    &bar.window_start_unix_ns) ||
                !CheckedBoundary(
                    midnight_unix_ns_,
                    end,
                    &bar.window_end_unix_ns)) {
                return MutableKLineHistoryErrorV1::kNumericOverflow;
            }
            bar.open_price_p6 = tick.price_p6;
            bar.high_price_p6 = tick.price_p6;
            bar.low_price_p6 = tick.price_p6;
            bar.close_price_p6 = tick.price_p6;
            bar.volume_raw = static_cast<std::uint64_t>(tick.quantity_raw);
            bar.volume_scale = tick.quantity_scale;
            bar.quantity_unit = tick.quantity_unit;
            bar.trade_count = 1U;
            bar.revision = 1U;
            bar.first_trade = TradeOrder(tick);
            bar.last_trade = bar.first_trade;
            found = bars->emplace(key, std::move(bar)).first;
            *output = &found->second;
            *historical_window = false;
            return MutableKLineHistoryErrorV1::kNone;
        }

        KLineBarV1& bar = found->second;
        if (bar.volume_scale != tick.quantity_scale ||
            bar.quantity_unit != tick.quantity_unit) {
            return MutableKLineHistoryErrorV1::kInvalidInput;
        }
        if (bar.volume_raw >
                std::numeric_limits<std::uint64_t>::max() -
                    static_cast<std::uint64_t>(tick.quantity_raw) ||
            bar.trade_count == std::numeric_limits<std::uint64_t>::max() ||
            bar.revision == std::numeric_limits<std::uint64_t>::max()) {
            return MutableKLineHistoryErrorV1::kNumericOverflow;
        }
        const KLineEventOrderV1 order = TradeOrder(tick);
        bar.high_price_p6 = std::max(bar.high_price_p6, tick.price_p6);
        bar.low_price_p6 = std::min(bar.low_price_p6, tick.price_p6);
        if (TradeOrderLess(order, bar.first_trade)) {
            bar.first_trade = order;
            bar.open_price_p6 = tick.price_p6;
        }
        if (TradeOrderLess(bar.last_trade, order)) {
            bar.last_trade = order;
            bar.close_price_p6 = tick.price_p6;
        }
        bar.volume_raw += static_cast<std::uint64_t>(tick.quantity_raw);
        ++bar.trade_count;
        ++bar.revision;
        *output = &bar;
        *historical_window = true;
        return MutableKLineHistoryErrorV1::kNone;
    }

    MutableKLineHistoryConfigV1 config_{};
    std::int64_t midnight_unix_ns_ = 0;
    std::unique_ptr<InstrumentState[]> instruments_;
    std::atomic<std::uint64_t> applied_trades_{0U};
    std::atomic<std::uint64_t> late_window_updates_{0U};
    std::atomic<std::uint64_t> duplicate_trades_{0U};
    std::atomic<std::uint64_t> source_conflicts_{0U};
    std::atomic<std::uint64_t> rebuilds_{0U};
};

MutableKLineHistoryV1::MutableKLineHistoryV1(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

MutableKLineHistoryV1::~MutableKLineHistoryV1() = default;

MutableKLineHistoryCreateErrorV1 MutableKLineHistoryV1::Create(
    MutableKLineHistoryConfigV1 config,
    std::unique_ptr<MutableKLineHistoryV1>* output) noexcept {
    if (output == nullptr) {
        return MutableKLineHistoryCreateErrorV1::kNullOutput;
    }
    output->reset();
    std::int64_t midnight = 0;
    if (ZeroIdentity(config.session_id) ||
        !FixedUtc8MidnightUnixNs(config.trade_date, &midnight) ||
        config.instrument_count == 0U || config.worker_count == 0U ||
        config.instrument_count > static_cast<std::size_t>(
            std::numeric_limits<std::uint32_t>::max()) ||
        config.worker_count > config.instrument_count ||
        config.windows.empty() ||
        config.windows.size() > kKLineMaximumWindowsV1 ||
        config.maximum_trades_per_instrument == 0U ||
        config.maximum_bars_per_instrument == 0U ||
        config.stable_block_bars < 2U ||
        config.stable_block_bars > 4096U ||
        config.cdc_range_chunk_bars == 0U ||
        config.maximum_change_records_per_instrument == 0U ||
        config.maximum_changes_per_read == 0U ||
        config.repair_replay_record_budget == 0U ||
        config.kline_routes.size() != config.instrument_count) {
        return MutableKLineHistoryCreateErrorV1::kInvalidConfiguration;
    }
    for (std::size_t index = 0U; index < config.windows.size(); ++index) {
        const KLineWindowSpecV1& window = config.windows[index];
        if (window.window_id == 0U || window.duration_ns == 0U ||
            window.duration_ns > kKLineNanosecondsPerDayV1 ||
            window.duration_ns %
                    kKLineNanosecondsPerMillisecondV1 !=
                0U) {
            return MutableKLineHistoryCreateErrorV1::
                kInvalidConfiguration;
        }
        for (std::size_t prior = 0U; prior < index; ++prior) {
            if (config.windows[prior].window_id == window.window_id ||
                config.windows[prior].duration_ns ==
                    window.duration_ns) {
                return MutableKLineHistoryCreateErrorV1::
                    kInvalidConfiguration;
            }
        }
    }
    for (std::uint32_t worker : config.kline_routes) {
        if (worker >= config.worker_count) {
            return MutableKLineHistoryCreateErrorV1::
                kInvalidConfiguration;
        }
    }
    try {
        std::sort(
            config.windows.begin(),
            config.windows.end(),
            [](const KLineWindowSpecV1& lhs,
               const KLineWindowSpecV1& rhs) noexcept {
                return lhs.window_id < rhs.window_id;
            });
        auto impl = std::make_unique<Impl>(std::move(config), midnight);
        for (std::size_t ordinal = 0U;
             ordinal < impl->config_.instrument_count; ++ordinal) {
            Impl::InstrumentState& state = impl->instruments_[ordinal];
            state.working = std::make_unique<Impl::WorkingState>();
            state.root.store(
                impl->BuildRoot(
                    static_cast<std::uint32_t>(ordinal + 1U),
                    0U,
                    state.working->bars),
                std::memory_order_release);
        }
        output->reset(new MutableKLineHistoryV1(std::move(impl)));
        return MutableKLineHistoryCreateErrorV1::kNone;
    } catch (...) {
        return MutableKLineHistoryCreateErrorV1::kResourceExhausted;
    }
}

MutableKLineHistoryErrorV1 MutableKLineHistoryV1::ResolveRoute(
    std::size_t ordinal,
    std::uint32_t instrument_id,
    KLineRouteTokenV1* output) const noexcept {
    if (output == nullptr) {
        return MutableKLineHistoryErrorV1::kNullOutput;
    }
    *output = {};
    if (impl_ == nullptr || ordinal >= impl_->config_.instrument_count ||
        instrument_id == 0U ||
        instrument_id != static_cast<std::uint32_t>(ordinal + 1U)) {
        return MutableKLineHistoryErrorV1::kInvalidInput;
    }
    output->instrument_id = instrument_id;
    output->ordinal = ordinal;
    output->worker = impl_->config_.kline_routes[ordinal];
    return MutableKLineHistoryErrorV1::kNone;
}

KLineApplyResultV1 MutableKLineHistoryV1::ApplyLive(
    std::uint32_t worker,
    const KLineRouteTokenV1& route,
    const CompactFastTickV1& tick) noexcept {
    KLineApplyResultV1 result{};
    if (impl_ == nullptr || !ValidKLineEnvelope(tick) ||
        route.ordinal >= impl_->config_.instrument_count ||
        tick.ordinal != route.ordinal ||
        tick.instrument_id != route.instrument_id ||
        route.instrument_id !=
            static_cast<std::uint32_t>(route.ordinal + 1U) ||
        tick.trade_date != impl_->config_.trade_date) {
        result.error = MutableKLineHistoryErrorV1::kInvalidInput;
        return result;
    }
    if (worker >= impl_->config_.worker_count ||
        route.worker != worker ||
        impl_->config_.kline_routes[route.ordinal] != worker) {
        result.error = MutableKLineHistoryErrorV1::kWrongWorker;
        return result;
    }
    if (!IsKLineTradeV1(tick)) {
        result.error = MutableKLineHistoryErrorV1::kNotTrade;
        result.disposition = KLineInputDispositionV1::kNotTrade;
        return result;
    }
    if (tick.price_p6 <= 0) {
        result.error = MutableKLineHistoryErrorV1::kInvalidInput;
        return result;
    }
    Impl::InstrumentState& state = impl_->instruments_[route.ordinal];
    if (state.writer.test_and_set(std::memory_order_acquire)) {
        MarkRepairRequired(tick.instrument_id, tick.arrival_id);
        result.error = MutableKLineHistoryErrorV1::kNotLive;
        return result;
    }
    struct WriterGuard final {
        std::atomic_flag* writer = nullptr;
        ~WriterGuard() { writer->clear(std::memory_order_release); }
    } writer_guard{&state.writer};
    if (state.repair_state.load(std::memory_order_acquire) !=
        EventRepairStateV1::kLive) {
        result.error = MutableKLineHistoryErrorV1::kNotLive;
        return result;
    }
    try {
        const KLineTradeUidV1 uid = TradeUid(tick);
        const auto duplicate = state.working->trades.find(uid);
        if (duplicate != state.working->trades.end()) {
            if (SameFastTickPayloadV1(duplicate->second, tick)) {
                impl_->duplicate_trades_.fetch_add(
                    1U, std::memory_order_relaxed);
                result.disposition =
                    KLineInputDispositionV1::kDuplicateIgnored;
                return result;
            }
            impl_->source_conflicts_.fetch_add(
                1U, std::memory_order_relaxed);
            // Do not touch the bar a second time. Force a FAST-based rebuild
            // first; that rebuild will either prove an idempotent duplicate
            // or isolate the irreconcilable source conflict.
            MarkRepairRequired(tick.instrument_id, tick.arrival_id);
            result.error = MutableKLineHistoryErrorV1::kSourceConflict;
            result.disposition =
                KLineInputDispositionV1::kSourceConflict;
            return result;
        }
        if (state.working->trades.size() >=
            impl_->config_.maximum_trades_per_instrument) {
            result.error = MutableKLineHistoryErrorV1::kTradeCapacity;
            MarkUnrecoverable(tick.instrument_id);
            return result;
        }
        std::vector<KLineBarV1> changed;
        struct BarBackup final {
            KLineBarKeyV1 key{};
            bool existed = false;
            KLineBarV1 value{};
        };
        struct LatestBackup final {
            std::uint32_t window_id = 0U;
            bool existed = false;
            std::uint64_t value = 0U;
        };
        std::vector<BarBackup> bar_backups;
        std::vector<LatestBackup> latest_backups;
        changed.reserve(impl_->config_.windows.size());
        bar_backups.reserve(impl_->config_.windows.size());
        latest_backups.reserve(impl_->config_.windows.size());

        const auto inserted_trade =
            state.working->trades.emplace(uid, tick);
        if (!inserted_trade.second) {
            result.error = MutableKLineHistoryErrorV1::kSourceConflict;
            MarkSourceConflictUnlessUnrecoverable(&state.repair_state);
            return result;
        }
        bool committed = false;
        struct RollbackGuard final {
            Impl::WorkingState* working = nullptr;
            const KLineTradeUidV1* uid = nullptr;
            const std::vector<BarBackup>* bars = nullptr;
            const std::vector<LatestBackup>* latest = nullptr;
            bool* committed = nullptr;

            ~RollbackGuard() noexcept {
                if (*committed) {
                    return;
                }
                working->trades.erase(*uid);
                for (auto iterator = bars->rbegin();
                     iterator != bars->rend(); ++iterator) {
                    if (iterator->existed) {
                        const auto found = working->bars.find(iterator->key);
                        if (found != working->bars.end()) {
                            found->second = iterator->value;
                        }
                    } else {
                        working->bars.erase(iterator->key);
                    }
                }
                for (auto iterator = latest->rbegin();
                     iterator != latest->rend(); ++iterator) {
                    if (iterator->existed) {
                        const auto found =
                            working->latest_window_start.find(
                                iterator->window_id);
                        if (found != working->latest_window_start.end()) {
                            found->second = iterator->value;
                        }
                    } else {
                        working->latest_window_start.erase(
                            iterator->window_id);
                    }
                }
            }
        } rollback{
            state.working.get(),
            &uid,
            &bar_backups,
            &latest_backups,
            &committed};

        std::uint64_t late_updates = 0U;
        for (const KLineWindowSpecV1& window : impl_->config_.windows) {
            const std::uint64_t start =
                (tick.event_time_ns_since_midnight /
                 window.duration_ns) *
                window.duration_ns;
            const KLineBarKeyV1 key{
                tick.instrument_id, window.window_id, start};
            const auto old_bar = state.working->bars.find(key);
            BarBackup bar_backup{};
            bar_backup.key = key;
            bar_backup.existed = old_bar != state.working->bars.end();
            if (bar_backup.existed) {
                bar_backup.value = old_bar->second;
            }
            bar_backups.push_back(bar_backup);

            const auto old_latest =
                state.working->latest_window_start.find(
                    window.window_id);
            LatestBackup latest_backup{};
            latest_backup.window_id = window.window_id;
            latest_backup.existed =
                old_latest != state.working->latest_window_start.end();
            if (latest_backup.existed) {
                latest_backup.value = old_latest->second;
            }
            latest_backups.push_back(latest_backup);

            KLineBarV1* bar = nullptr;
            bool historical = false;
            result.error = impl_->ApplyWindow(
                tick,
                window,
                &state.working->bars,
                &bar,
                &historical);
            if (result.error != MutableKLineHistoryErrorV1::kNone) {
                // Capacity, scale/unit mismatch and numeric overflow are
                // deterministic for the already durable FAST prefix. A
                // rebuild cannot cure them without changing configuration or
                // source facts, so fail this instrument closed.
                MarkUnrecoverable(tick.instrument_id);
                return result;
            }
            const auto latest = state.working->latest_window_start.find(
                window.window_id);
            if (latest != state.working->latest_window_start.end() &&
                bar->window_start_ns_since_midnight < latest->second) {
                ++late_updates;
            }
            if (latest == state.working->latest_window_start.end() ||
                latest->second < bar->window_start_ns_since_midnight) {
                state.working->latest_window_start[window.window_id] =
                    bar->window_start_ns_since_midnight;
            }
            static_cast<void>(historical);
            changed.push_back(*bar);
        }

        std::vector<KLineMutationV1> staged;
        std::uint64_t final_sequence = 0U;
        {
            std::lock_guard<std::mutex> lock(state.changes_mutex);
            if (state.changes.size() >
                    impl_->config_.maximum_change_records_per_instrument ||
                changed.size() >
                    impl_->config_.maximum_change_records_per_instrument -
                        state.changes.size() ||
                !ChangeSequenceCanAppend(
                    state.changes.size(), changed.size())) {
                result.error =
                    MutableKLineHistoryErrorV1::kChangeCapacity;
                MarkUnrecoverable(tick.instrument_id);
                return result;
            }
            if (!ReserveForAppend(
                    &state.changes,
                    changed.size(),
                    impl_->config_.maximum_change_records_per_instrument)) {
                throw std::bad_alloc();
            }
            staged.reserve(changed.size());
            for (const KLineBarV1& bar : changed) {
                KLineMutationV1 mutation{};
                mutation.change_sequence =
                    state.changes.size() + staged.size() + 1U;
                mutation.kind = KLineMutationKindV1::kUpsert;
                mutation.key = KLineBarKeyV1{
                    bar.instrument_id,
                    bar.window_id,
                    bar.window_start_ns_since_midnight};
                mutation.bar = bar;
                staged.push_back(std::move(mutation));
            }
            final_sequence = staged.back().change_sequence;
        }
        const auto current = state.root.load(std::memory_order_acquire);
        if (current == nullptr) {
            throw std::bad_alloc();
        }
        const auto root = impl_->ApplyBarsToRoot(
            *current, changed, final_sequence);
        impl_->CommitStagedAndPublish(
            &state, &staged, std::move(root));
        committed = true;
        impl_->applied_trades_.fetch_add(1U, std::memory_order_relaxed);
        impl_->late_window_updates_.fetch_add(
            late_updates, std::memory_order_relaxed);
        result.disposition = KLineInputDispositionV1::kUpserted;
        result.upserted_bars = changed.size();
        return result;
    } catch (...) {
        result.error = MutableKLineHistoryErrorV1::kResourceExhausted;
        MarkRepairRequired(tick.instrument_id, tick.arrival_id);
        return result;
    }
}

KLineRebuildResultV1 MutableKLineHistoryV1::RebuildFromFast(
    std::uint32_t worker,
    const KLineRouteTokenV1& route,
    const FastTickStoreV1& fast_store) noexcept {
    KLineRebuildResultV1 result{};
    if (impl_ == nullptr || route.ordinal >= impl_->config_.instrument_count ||
        route.instrument_id == 0U ||
        route.instrument_id !=
            static_cast<std::uint32_t>(route.ordinal + 1U)) {
        result.error = MutableKLineHistoryErrorV1::kInvalidInput;
        return result;
    }
    if (worker >= impl_->config_.worker_count ||
        route.worker != worker ||
        impl_->config_.kline_routes[route.ordinal] != worker) {
        result.error = MutableKLineHistoryErrorV1::kWrongWorker;
        return result;
    }
    Impl::InstrumentState& state = impl_->instruments_[route.ordinal];
    if (state.writer.test_and_set(std::memory_order_acquire)) {
        result.error = MutableKLineHistoryErrorV1::kNotLive;
        return result;
    }
    struct WriterGuard final {
        std::atomic_flag* writer = nullptr;
        ~WriterGuard() { writer->clear(std::memory_order_release); }
    } writer_guard{&state.writer};
    EventRepairStateV1 prior = state.repair_state.load(
        std::memory_order_acquire);
    for (;;) {
        if (prior == EventRepairStateV1::kSourceConflict ||
            prior == EventRepairStateV1::kUnrecoverable) {
            result.error = prior == EventRepairStateV1::kSourceConflict
                               ? MutableKLineHistoryErrorV1::kSourceConflict
                               : MutableKLineHistoryErrorV1::kFastCoverageLost;
            return result;
        }
        if (state.repair_state.compare_exchange_weak(
                prior,
                EventRepairStateV1::kRebuilding,
                std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            break;
        }
    }
    FastTickInstrumentStatusV1 status{};
    if (fast_store.Status(route.instrument_id, &status) !=
            FastTickStoreQueryErrorV1::kNone ||
        !status.coverage_complete) {
        state.repair_state.store(
            EventRepairStateV1::kUnrecoverable,
            std::memory_order_release);
        result.error = MutableKLineHistoryErrorV1::kFastCoverageLost;
        return result;
    }
    std::vector<CompactFastTickV1> ticks;
    if (fast_store.CopyCompactHistory(
            route.instrument_id,
            &ticks,
            &result.captured_fast_tail) !=
        FastTickStoreQueryErrorV1::kNone) {
        RestoreRepairRequiredUnlessTerminal(&state.repair_state);
        result.error = MutableKLineHistoryErrorV1::kResourceExhausted;
        return result;
    }
    const std::uint64_t captured_arrival_id =
        ticks.empty() ? 0U : ticks.back().arrival_id;

    try {
        auto rebuilt = std::make_unique<Impl::WorkingState>();
        std::size_t records_since_yield = 0U;
        for (const CompactFastTickV1& tick : ticks) {
            CooperativeRepairYield(
                &records_since_yield,
                impl_->config_.repair_replay_record_budget);
            if (!IsKLineTradeV1(tick)) {
                continue;
            }
            const KLineTradeUidV1 uid = TradeUid(tick);
            const auto existing = rebuilt->trades.find(uid);
            if (existing != rebuilt->trades.end()) {
                if (!SameFastTickPayloadV1(existing->second, tick)) {
                    MarkSourceConflictUnlessUnrecoverable(
                        &state.repair_state);
                    impl_->source_conflicts_.fetch_add(
                        1U, std::memory_order_relaxed);
                    result.error =
                        MutableKLineHistoryErrorV1::kSourceConflict;
                    return result;
                }
                continue;
            }
            if (rebuilt->trades.size() >=
                impl_->config_.maximum_trades_per_instrument) {
                state.repair_state.store(
                    EventRepairStateV1::kUnrecoverable,
                    std::memory_order_release);
                result.error =
                    MutableKLineHistoryErrorV1::kTradeCapacity;
                return result;
            }
            rebuilt->trades.emplace(uid, tick);
            for (const KLineWindowSpecV1& window :
                 impl_->config_.windows) {
                KLineBarV1* bar = nullptr;
                bool historical = false;
                result.error = impl_->ApplyWindow(
                    tick,
                    window,
                    &rebuilt->bars,
                    &bar,
                    &historical);
                static_cast<void>(historical);
                if (result.error != MutableKLineHistoryErrorV1::kNone) {
                    state.repair_state.store(
                        EventRepairStateV1::kUnrecoverable,
                        std::memory_order_release);
                    return result;
                }
                auto& latest =
                    rebuilt->latest_window_start[window.window_id];
                latest = std::max(
                    latest, bar->window_start_ns_since_midnight);
            }
        }
        result.rebuilt_trades = rebuilt->trades.size();

        std::vector<KLineMutationV1> staged;
        for (auto& [key, bar] : rebuilt->bars) {
            const auto prior_bar = state.working->bars.find(key);
            if (prior_bar == state.working->bars.end()) {
                bar.revision = 1U;
            } else if (SameBarIgnoringRevision(prior_bar->second, bar)) {
                bar.revision = prior_bar->second.revision;
            } else {
                if (prior_bar->second.revision ==
                    std::numeric_limits<std::uint64_t>::max()) {
                    state.repair_state.store(
                        EventRepairStateV1::kUnrecoverable,
                        std::memory_order_release);
                    result.error =
                        MutableKLineHistoryErrorV1::kNumericOverflow;
                    return result;
                }
                bar.revision = prior_bar->second.revision + 1U;
            }
            if (prior_bar == state.working->bars.end() ||
                !SameBarIgnoringRevision(prior_bar->second, bar)) {
                KLineMutationV1 mutation{};
                mutation.kind = KLineMutationKindV1::kUpsert;
                mutation.key = key;
                mutation.bar = bar;
                staged.push_back(std::move(mutation));
                ++result.upserted_bars;
            }
        }
        for (const auto& [key, bar] : state.working->bars) {
            static_cast<void>(bar);
            if (rebuilt->bars.find(key) == rebuilt->bars.end()) {
                KLineMutationV1 mutation{};
                mutation.kind = KLineMutationKindV1::kDelete;
                mutation.key = key;
                staged.push_back(std::move(mutation));
                ++result.deleted_bars;
            }
        }

        // A repair may add, update, and delete many bars. Publish one
        // complete-instrument range transaction so a paged CDC reader cannot
        // expose only a prefix of the rebuilt result.
        const std::size_t chunk_bars =
            impl_->config_.cdc_range_chunk_bars;
        const std::size_t chunks =
            rebuilt->bars.size() / chunk_bars +
            (rebuilt->bars.size() % chunk_bars == 0U ? 0U : 1U);
        if (chunks >
            std::numeric_limits<std::size_t>::max() - 2U) {
            throw std::bad_alloc();
        }
        const std::size_t mutation_count = chunks + 2U;
        staged.clear();
        std::uint64_t final_sequence = 0U;
        {
            std::lock_guard<std::mutex> lock(state.changes_mutex);
            if (state.changes.size() >
                    impl_->config_.maximum_change_records_per_instrument ||
                mutation_count >
                    impl_->config_.maximum_change_records_per_instrument -
                        state.changes.size() ||
                !ChangeSequenceCanAppend(
                    state.changes.size(), mutation_count)) {
                state.repair_state.store(
                    EventRepairStateV1::kUnrecoverable,
                    std::memory_order_release);
                result.error =
                    MutableKLineHistoryErrorV1::kChangeCapacity;
                return result;
            }
            if (state.next_transaction_id ==
                std::numeric_limits<std::uint64_t>::max()) {
                state.repair_state.store(
                    EventRepairStateV1::kUnrecoverable,
                    std::memory_order_release);
                result.error =
                    MutableKLineHistoryErrorV1::kChangeCapacity;
                return result;
            }
            const std::uint64_t transaction =
                state.next_transaction_id++;
            staged.reserve(mutation_count);
            KLineMutationV1 begin{};
            begin.change_sequence = state.changes.size() + 1U;
            begin.transaction_id = transaction;
            begin.kind = KLineMutationKindV1::kRangeReplaceBegin;
            begin.replace_entire_instrument = true;
            staged.push_back(std::move(begin));

            auto iterator = rebuilt->bars.begin();
            std::size_t remaining_bars = rebuilt->bars.size();
            while (iterator != rebuilt->bars.end()) {
                KLineMutationV1 chunk{};
                chunk.change_sequence =
                    state.changes.size() + staged.size() + 1U;
                chunk.transaction_id = transaction;
                chunk.kind = KLineMutationKindV1::kRangeReplaceChunk;
                const std::size_t bars_in_chunk = std::min(
                    remaining_bars,
                    impl_->config_.cdc_range_chunk_bars);
                chunk.replacement_bars.reserve(bars_in_chunk);
                for (std::size_t count = 0U;
                     count < bars_in_chunk &&
                     iterator != rebuilt->bars.end();
                     ++count, ++iterator) {
                    chunk.replacement_bars.push_back(iterator->second);
                }
                remaining_bars -= bars_in_chunk;
                staged.push_back(std::move(chunk));
            }
            KLineMutationV1 commit{};
            commit.change_sequence =
                state.changes.size() + staged.size() + 1U;
            commit.transaction_id = transaction;
            commit.kind = KLineMutationKindV1::kRangeReplaceCommit;
            staged.push_back(std::move(commit));
            final_sequence = staged.back().change_sequence;
            if (!ReserveForAppend(
                    &state.changes,
                    staged.size(),
                    impl_->config_.maximum_change_records_per_instrument)) {
                state.repair_state.store(
                    EventRepairStateV1::kUnrecoverable,
                    std::memory_order_release);
                result.error =
                    MutableKLineHistoryErrorV1::kChangeCapacity;
                return result;
            }
        }
        const auto root = impl_->BuildRoot(
            route.instrument_id, final_sequence, rebuilt->bars);
        EventRepairStateV1 rebuilding = EventRepairStateV1::kRebuilding;
        if (!state.repair_state.compare_exchange_strong(
                rebuilding,
                EventRepairStateV1::kCatchingUp,
                std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            result.error = rebuilding == EventRepairStateV1::kSourceConflict
                               ? MutableKLineHistoryErrorV1::kSourceConflict
                               : rebuilding ==
                                         EventRepairStateV1::kUnrecoverable
                                     ? MutableKLineHistoryErrorV1::
                                           kFastCoverageLost
                                     : MutableKLineHistoryErrorV1::kNotLive;
            return result;
        }
        impl_->CommitStagedAndPublish(
            &state, &staged, std::move(root));
        state.working = std::move(rebuilt);
        impl_->rebuilds_.fetch_add(1U, std::memory_order_relaxed);
        FastTickInstrumentStatusV1 after{};
        const FastTickStoreQueryErrorV1 after_error =
            fast_store.Status(route.instrument_id, &after);
        if (after_error == FastTickStoreQueryErrorV1::kNone &&
            !after.coverage_complete) {
            state.repair_state.store(
                EventRepairStateV1::kUnrecoverable,
                std::memory_order_release);
        } else if (
            after_error == FastTickStoreQueryErrorV1::kNone &&
            state.repair_through.load(std::memory_order_acquire) <=
                captured_arrival_id) {
            EventRepairStateV1 catching =
                EventRepairStateV1::kCatchingUp;
            static_cast<void>(state.repair_state.compare_exchange_strong(
                catching,
                EventRepairStateV1::kLive,
                std::memory_order_acq_rel,
                std::memory_order_acquire));
        } else {
            EventRepairStateV1 catching =
                EventRepairStateV1::kCatchingUp;
            static_cast<void>(state.repair_state.compare_exchange_strong(
                catching,
                EventRepairStateV1::kRepairRequired,
                std::memory_order_acq_rel,
                std::memory_order_acquire));
        }
        result.complete = true;
        return result;
    } catch (...) {
        RestoreRepairRequiredUnlessTerminal(&state.repair_state);
        result.error = MutableKLineHistoryErrorV1::kResourceExhausted;
        return result;
    }
}

void MutableKLineHistoryV1::MarkRepairRequired(
    std::uint32_t instrument_id,
    std::uint64_t through_arrival_id) noexcept {
    if (impl_ == nullptr || instrument_id == 0U ||
        instrument_id > impl_->config_.instrument_count) {
        return;
    }
    Impl::InstrumentState& state =
        impl_->instruments_[instrument_id - 1U];
    AtomicMaximum(&state.repair_through, through_arrival_id);
    EventRepairStateV1 current = state.repair_state.load(
        std::memory_order_acquire);
    for (;;) {
        if (current == EventRepairStateV1::kSourceConflict ||
            current == EventRepairStateV1::kUnrecoverable ||
            current == EventRepairStateV1::kRepairRequired ||
            current == EventRepairStateV1::kRebuilding) {
            return;
        }
        if (state.repair_state.compare_exchange_weak(
                current,
                EventRepairStateV1::kRepairRequired,
                std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            return;
        }
    }
}

void MutableKLineHistoryV1::MarkUnrecoverable(
    std::uint32_t instrument_id) noexcept {
    if (impl_ == nullptr || instrument_id == 0U ||
        instrument_id > impl_->config_.instrument_count) {
        return;
    }
    impl_->instruments_[instrument_id - 1U].repair_state.store(
        EventRepairStateV1::kUnrecoverable,
        std::memory_order_release);
}

MutableKLineHistoryErrorV1 MutableKLineHistoryV1::AcquireStable(
    std::uint32_t instrument_id,
    KLineStableSnapshotV1* output) const noexcept {
    if (output == nullptr) {
        return MutableKLineHistoryErrorV1::kNullOutput;
    }
    *output = {};
    if (impl_ == nullptr || instrument_id == 0U ||
        instrument_id > impl_->config_.instrument_count) {
        return MutableKLineHistoryErrorV1::kInvalidInput;
    }
    const Impl::InstrumentState& state =
        impl_->instruments_[instrument_id - 1U];
    output->root = state.root.load(std::memory_order_acquire);
    if (output->root == nullptr) {
        return MutableKLineHistoryErrorV1::kResourceExhausted;
    }
    output->next_changes.session_id = impl_->config_.session_id;
    output->next_changes.instrument_id = instrument_id;
    output->next_changes.next_change_sequence =
        output->root->included_change_sequence() + 1U;
    output->repair_state = state.repair_state.load(
        std::memory_order_acquire);
    output->repair_through_arrival_id = state.repair_through.load(
        std::memory_order_acquire);
    return MutableKLineHistoryErrorV1::kNone;
}

MutableKLineHistoryErrorV1 MutableKLineHistoryV1::ReadChanges(
    KLineChangeCursorV1* cursor,
    std::span<KLineMutationV1> output,
    std::size_t* written) const noexcept {
    if (cursor == nullptr || written == nullptr) {
        return MutableKLineHistoryErrorV1::kNullOutput;
    }
    *written = 0U;
    if (impl_ == nullptr || output.empty() ||
        cursor->session_id != impl_->config_.session_id ||
        cursor->instrument_id == 0U ||
        cursor->instrument_id > impl_->config_.instrument_count ||
        cursor->next_change_sequence == 0U) {
        return MutableKLineHistoryErrorV1::kCursorMismatch;
    }
    if (output.size() > impl_->config_.maximum_changes_per_read) {
        return MutableKLineHistoryErrorV1::kBatchLimitExceeded;
    }
    const Impl::InstrumentState& state =
        impl_->instruments_[cursor->instrument_id - 1U];
    try {
        std::lock_guard<std::mutex> lock(state.changes_mutex);
        if (cursor->next_change_sequence > state.changes.size() + 1U) {
            return MutableKLineHistoryErrorV1::kCursorMismatch;
        }
        std::size_t index = static_cast<std::size_t>(
            cursor->next_change_sequence - 1U);
        std::uint64_t next_sequence =
            cursor->next_change_sequence;
        while (index < state.changes.size() &&
               *written < output.size()) {
            output[*written] = state.changes[index];
            ++(*written);
            ++index;
            ++next_sequence;
        }
        cursor->next_change_sequence = next_sequence;
        return MutableKLineHistoryErrorV1::kNone;
    } catch (...) {
        *written = 0U;
        return MutableKLineHistoryErrorV1::kResourceExhausted;
    }
}

EventRepairStateV1 MutableKLineHistoryV1::RepairState(
    std::uint32_t instrument_id) const noexcept {
    if (impl_ == nullptr || instrument_id == 0U ||
        instrument_id > impl_->config_.instrument_count) {
        return EventRepairStateV1::kUnrecoverable;
    }
    return impl_->instruments_[instrument_id - 1U].repair_state.load(
        std::memory_order_acquire);
}

std::uint64_t MutableKLineHistoryV1::RepairThrough(
    std::uint32_t instrument_id) const noexcept {
    if (impl_ == nullptr || instrument_id == 0U ||
        instrument_id > impl_->config_.instrument_count) {
        return 0U;
    }
    return impl_->instruments_[instrument_id - 1U].repair_through.load(
        std::memory_order_acquire);
}

MutableKLineHistoryStatsV1 MutableKLineHistoryV1::Stats()
    const noexcept {
    MutableKLineHistoryStatsV1 result{};
    if (impl_ == nullptr) {
        return result;
    }
    result.applied_trades = impl_->applied_trades_.load(
        std::memory_order_acquire);
    result.late_window_updates = impl_->late_window_updates_.load(
        std::memory_order_acquire);
    result.duplicate_trades = impl_->duplicate_trades_.load(
        std::memory_order_acquire);
    result.source_conflicts = impl_->source_conflicts_.load(
        std::memory_order_acquire);
    result.rebuilds = impl_->rebuilds_.load(std::memory_order_acquire);
    return result;
}

const MutableKLineHistoryConfigV1& MutableKLineHistoryV1::config()
    const noexcept {
    return impl_->config_;
}

}  // namespace l2flow::market
