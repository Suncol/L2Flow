#include "l2flow/market/ordered_event_history_v1.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <map>
#include <mutex>
#include <new>
#include <queue>
#include <thread>
#include <tuple>
#include <type_traits>
#include <utility>

namespace l2flow::market {
namespace {

static_assert(std::is_nothrow_move_constructible_v<EventMutationV1>);

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

[[nodiscard]] bool ValidTickForEvent(
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

struct EventBlockDataV1 final {
    EventBlockDataV1() = default;
    EventBlockDataV1(const EventBlockDataV1&) = delete;
    EventBlockDataV1& operator=(const EventBlockDataV1&) = delete;

    ~EventBlockDataV1() noexcept {
        // A million one-input live publications can form a long persistent
        // chain. Release an exclusively owned prefix iteratively so final
        // session teardown cannot recurse once per historical node.
        auto node = std::move(previous);
        while (node != nullptr && node.use_count() == 1) {
            auto* const mutable_node =
                const_cast<EventBlockDataV1*>(node.get());
            auto next = std::move(mutable_node->previous);
            node.reset();
            node = std::move(next);
        }
    }

    std::vector<OrderedDerivedEventV1> rows;
    std::shared_ptr<const EventBlockDataV1> previous;
};

struct EventChannelDataV1 final {
    std::int32_t channel = 0;
    std::uint64_t row_count = 0U;
    std::shared_ptr<const EventBlockDataV1> tail;
};

[[nodiscard]] DerivedEventKindV1 PayloadKind(
    const DerivedEventPayloadV1& payload) noexcept {
    return std::visit(
        [](const auto& value) noexcept {
            using Event = std::decay_t<decltype(value)>;
            if constexpr (
                std::is_same_v<Event, ShanghaiOrderRevisionEventV1>) {
                return DerivedEventKindV1::kShanghaiOrderRevision;
            } else if constexpr (
                std::is_same_v<Event, ShanghaiTradeEventV1>) {
                return DerivedEventKindV1::kShanghaiTrade;
            } else if constexpr (
                std::is_same_v<Event, ShanghaiCancelEventV1>) {
                return DerivedEventKindV1::kShanghaiCancel;
            } else if constexpr (
                std::is_same_v<Event, ShanghaiStatusEventV1>) {
                return DerivedEventKindV1::kShanghaiStatus;
            } else if constexpr (
                std::is_same_v<Event, ShenzhenOrderRevisionEventV1>) {
                return DerivedEventKindV1::kShenzhenOrderRevision;
            } else if constexpr (
                std::is_same_v<Event, ShenzhenTradeEventV1>) {
                return DerivedEventKindV1::kShenzhenTrade;
            } else {
                return DerivedEventKindV1::kShenzhenCancel;
            }
        },
        payload);
}

[[nodiscard]] std::int64_t AffectedOrderId(
    const DerivedEventPayloadV1& payload) noexcept {
    return std::visit(
        [](const auto& value) noexcept -> std::int64_t {
            using Event = std::decay_t<decltype(value)>;
            if constexpr (
                std::is_same_v<Event, ShanghaiOrderRevisionEventV1>) {
                return value.order.key.order_id;
            } else if constexpr (
                std::is_same_v<Event, ShanghaiCancelEventV1>) {
                return value.key.order_id;
            } else if constexpr (
                std::is_same_v<Event, ShenzhenOrderRevisionEventV1>) {
                return value.order.key.order_id;
            } else if constexpr (
                std::is_same_v<Event, ShenzhenCancelEventV1>) {
                return value.key.order_id;
            } else {
                return 0;
            }
        },
        payload);
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

class OrderedInputBlocksV1 final {
public:
    enum class InsertResult : std::uint8_t {
        kAppended = 0U,
        kLate,
        kDuplicate,
        kConflict,
        kCapacity,
    };

    OrderedInputBlocksV1(
        std::size_t block_records,
        std::size_t maximum_records)
        : block_records_(block_records),
          maximum_records_(maximum_records) {}

    [[nodiscard]] InsertResult Insert(
        const CompactFastTickV1& tick) {
        const std::int64_t sequence = tick.business_sequence.value;
        if (blocks_.empty() ||
            sequence > blocks_.back().back().business_sequence.value) {
            if (count_ >= maximum_records_) {
                return InsertResult::kCapacity;
            }
            if (blocks_.empty() ||
                blocks_.back().size() >= block_records_) {
                blocks_.emplace_back();
                blocks_.back().reserve(block_records_);
            }
            blocks_.back().push_back(tick);
            ++count_;
            maximum_sequence_ = sequence;
            return InsertResult::kAppended;
        }

        auto block = std::lower_bound(
            blocks_.begin(),
            blocks_.end(),
            sequence,
            [](const std::vector<CompactFastTickV1>& candidate,
               std::int64_t value) noexcept {
                return candidate.back().business_sequence.value < value;
            });
        if (block == blocks_.end()) {
            return InsertResult::kCapacity;
        }
        auto position = std::lower_bound(
            block->begin(),
            block->end(),
            sequence,
            [](const CompactFastTickV1& candidate,
               std::int64_t value) noexcept {
                return candidate.business_sequence.value < value;
            });
        if (position != block->end() &&
            position->business_sequence.value == sequence) {
            return SameFastTickPayloadV1(*position, tick)
                       ? InsertResult::kDuplicate
                       : InsertResult::kConflict;
        }
        if (count_ >= maximum_records_) {
            return InsertResult::kCapacity;
        }
        block->insert(position, tick);
        ++count_;
        if (block->size() > block_records_) {
            std::vector<CompactFastTickV1> split;
            split.reserve(block_records_);
            const std::size_t split_at = block->size() / 2U;
            split.insert(
                split.end(),
                std::make_move_iterator(
                    block->begin() +
                    static_cast<std::ptrdiff_t>(split_at)),
                std::make_move_iterator(block->end()));
            block->erase(
                block->begin() +
                    static_cast<std::ptrdiff_t>(split_at),
                block->end());
            blocks_.insert(block + 1, std::move(split));
        }
        return InsertResult::kLate;
    }

    [[nodiscard]] std::int64_t maximum_sequence() const noexcept {
        return maximum_sequence_;
    }

private:
    std::vector<std::vector<CompactFastTickV1>> blocks_;
    std::size_t block_records_ = 0U;
    std::size_t maximum_records_ = 0U;
    std::size_t count_ = 0U;
    std::int64_t maximum_sequence_ = 0;
};

enum class AdaptiveSortChoiceV1 : std::uint8_t {
    kNoSort = 0U,
    kNaturalRuns,
    kRadix,
};

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

[[nodiscard]] AdaptiveSortChoiceV1 AdaptiveBusinessSort(
    std::vector<CompactFastTickV1>* rows,
    std::vector<CompactFastTickV1>* scratch,
    std::size_t record_budget) {
    if (rows->size() < 2U) {
        return AdaptiveSortChoiceV1::kNoSort;
    }
    std::vector<std::pair<std::size_t, std::size_t>> runs;
    runs.reserve(16U);
    std::size_t run_begin = 0U;
    std::size_t inversions = 0U;
    std::size_t records_since_yield = 0U;
    for (std::size_t index = 1U; index < rows->size(); ++index) {
        if ((*rows)[index].business_sequence.value <
            (*rows)[index - 1U].business_sequence.value) {
            ++inversions;
            runs.emplace_back(run_begin, index);
            run_begin = index;
        }
        CooperativeRepairYield(&records_since_yield, record_budget);
    }
    if (inversions == 0U) {
        return AdaptiveSortChoiceV1::kNoSort;
    }
    runs.emplace_back(run_begin, rows->size());

    scratch->clear();
    scratch->resize(rows->size());
    if (runs.size() <= 64U &&
        inversions <= std::max<std::size_t>(1U, rows->size() / 8U)) {
        struct Node final {
            std::size_t run = 0U;
            std::size_t index = 0U;
        };
        const auto later = [rows](const Node& lhs, const Node& rhs) {
            const CompactFastTickV1& left = (*rows)[lhs.index];
            const CompactFastTickV1& right = (*rows)[rhs.index];
            if (left.business_sequence.value !=
                right.business_sequence.value) {
                return left.business_sequence.value >
                       right.business_sequence.value;
            }
            return left.arrival_id > right.arrival_id;
        };
        std::priority_queue<
            Node, std::vector<Node>, decltype(later)> queue(later);
        for (std::size_t run = 0U; run < runs.size(); ++run) {
            queue.push(Node{run, runs[run].first});
        }
        std::size_t output = 0U;
        while (!queue.empty()) {
            const Node node = queue.top();
            queue.pop();
            (*scratch)[output] = std::move((*rows)[node.index]);
            ++output;
            CooperativeRepairYield(
                &records_since_yield, record_budget);
            const std::size_t next = node.index + 1U;
            if (next < runs[node.run].second) {
                queue.push(Node{node.run, next});
            }
        }
        rows->swap(*scratch);
        return AdaptiveSortChoiceV1::kNaturalRuns;
    }

    // Positive signed business sequences have the same order as their u64
    // representation. Eight stable byte passes avoid comparison-sort cost in
    // the highly disordered fallback.
    for (unsigned int pass = 0U; pass < 8U; ++pass) {
        std::array<std::size_t, 256U> counts{};
        const unsigned int shift = pass * 8U;
        for (const CompactFastTickV1& row : *rows) {
            const auto key = static_cast<std::uint64_t>(
                row.business_sequence.value);
            ++counts[(key >> shift) & 0xffU];
            CooperativeRepairYield(
                &records_since_yield, record_budget);
        }
        std::array<std::size_t, 256U> offsets{};
        for (std::size_t index = 1U; index < offsets.size(); ++index) {
            offsets[index] = offsets[index - 1U] + counts[index - 1U];
        }
        for (CompactFastTickV1& row : *rows) {
            const auto key = static_cast<std::uint64_t>(
                row.business_sequence.value);
            const std::size_t bucket = (key >> shift) & 0xffU;
            (*scratch)[offsets[bucket]] = std::move(row);
            ++offsets[bucket];
            CooperativeRepairYield(
                &records_since_yield, record_budget);
        }
        rows->swap(*scratch);
    }
    return AdaptiveSortChoiceV1::kRadix;
}

}  // namespace

bool EventOrderKeyLessV1(
    const EventOrderKeyV1& lhs,
    const EventOrderKeyV1& rhs) noexcept {
    return std::tie(
               lhs.channel,
               lhs.business_sequence,
               lhs.source_event_ordinal,
               lhs.derived_event_ordinal,
               lhs.affected_order_id) <
           std::tie(
               rhs.channel,
               rhs.business_sequence,
               rhs.source_event_ordinal,
               rhs.derived_event_ordinal,
               rhs.affected_order_id);
}

class EventStableRootV1::Impl final {
public:
    std::uint32_t instrument_id = 0U;
    std::uint64_t included_change_sequence = 0U;
    std::uint64_t row_count = 0U;
    std::vector<EventChannelDataV1> channels;
};

EventStableRootV1::EventStableRootV1(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

EventStableRootV1::~EventStableRootV1() = default;

std::uint32_t EventStableRootV1::instrument_id() const noexcept {
    return impl_ == nullptr ? 0U : impl_->instrument_id;
}

std::uint64_t EventStableRootV1::included_change_sequence()
    const noexcept {
    return impl_ == nullptr ? 0U : impl_->included_change_sequence;
}

std::uint64_t EventStableRootV1::row_count() const noexcept {
    return impl_ == nullptr ? 0U : impl_->row_count;
}

bool EventStableRootV1::strictly_ordered() const noexcept {
    if (impl_ == nullptr) {
        return false;
    }
    // Walk the persistent append chains backwards. The observed order is
    // descending, so every next row must compare strictly before the prior
    // (later) row. This validates without allocating reader scratch.
    const OrderedDerivedEventV1* later = nullptr;
    std::uint64_t observed = 0U;
    for (auto channel = impl_->channels.rbegin();
         channel != impl_->channels.rend(); ++channel) {
        if (channel->tail == nullptr || channel->row_count == 0U) {
            return false;
        }
        std::uint64_t channel_rows = 0U;
        for (const EventBlockDataV1* block = channel->tail.get();
             block != nullptr; block = block->previous.get()) {
            if (block->rows.empty()) {
                return false;
            }
            for (auto row = block->rows.rbegin();
                 row != block->rows.rend(); ++row) {
                if (later != nullptr &&
                    !EventOrderKeyLessV1(
                        row->order_key, later->order_key)) {
                    return false;
                }
                later = &*row;
                ++channel_rows;
                ++observed;
            }
        }
        if (channel_rows != channel->row_count) {
            return false;
        }
    }
    return observed == impl_->row_count;
}

bool EventStableRootV1::CopyRows(
    std::vector<OrderedDerivedEventV1>* output) const noexcept {
    if (output == nullptr) {
        return false;
    }
    output->clear();
    if (impl_ == nullptr) {
        return false;
    }
    try {
        if (impl_->row_count >
            static_cast<std::uint64_t>(
                std::numeric_limits<std::size_t>::max())) {
            return false;
        }
        output->reserve(static_cast<std::size_t>(impl_->row_count));
        for (const EventChannelDataV1& channel : impl_->channels) {
            std::vector<const EventBlockDataV1*> blocks;
            for (const EventBlockDataV1* block = channel.tail.get();
                 block != nullptr; block = block->previous.get()) {
                blocks.push_back(block);
            }
            for (auto block = blocks.rbegin(); block != blocks.rend();
                 ++block) {
                output->insert(
                    output->end(),
                    (*block)->rows.begin(),
                    (*block)->rows.end());
            }
        }
        return true;
    } catch (...) {
        output->clear();
        return false;
    }
}

std::string_view OrderedEventHistoryCreateErrorNameV1(
    OrderedEventHistoryCreateErrorV1 error) noexcept {
    switch (error) {
        case OrderedEventHistoryCreateErrorV1::kNone:
            return "none";
        case OrderedEventHistoryCreateErrorV1::kNullOutput:
            return "null_output";
        case OrderedEventHistoryCreateErrorV1::kInvalidConfiguration:
            return "invalid_configuration";
        case OrderedEventHistoryCreateErrorV1::kResourceExhausted:
            return "resource_exhausted";
    }
    return "unknown";
}

std::string_view OrderedEventHistoryErrorNameV1(
    OrderedEventHistoryErrorV1 error) noexcept {
    switch (error) {
        case OrderedEventHistoryErrorV1::kNone:
            return "none";
        case OrderedEventHistoryErrorV1::kNullOutput:
            return "null_output";
        case OrderedEventHistoryErrorV1::kInvalidInput:
            return "invalid_input";
        case OrderedEventHistoryErrorV1::kWrongWorker:
            return "wrong_worker";
        case OrderedEventHistoryErrorV1::kNotLive:
            return "not_live";
        case OrderedEventHistoryErrorV1::kInputCapacity:
            return "input_capacity";
        case OrderedEventHistoryErrorV1::kEventCapacity:
            return "event_capacity";
        case OrderedEventHistoryErrorV1::kProjectionFailed:
            return "projection_failed";
        case OrderedEventHistoryErrorV1::kCoreFailed:
            return "core_failed";
        case OrderedEventHistoryErrorV1::kSourceConflict:
            return "source_conflict";
        case OrderedEventHistoryErrorV1::kFastCoverageLost:
            return "fast_coverage_lost";
        case OrderedEventHistoryErrorV1::kChangeCapacity:
            return "change_capacity";
        case OrderedEventHistoryErrorV1::kCursorMismatch:
            return "cursor_mismatch";
        case OrderedEventHistoryErrorV1::kBatchLimitExceeded:
            return "batch_limit_exceeded";
        case OrderedEventHistoryErrorV1::kResourceExhausted:
            return "resource_exhausted";
    }
    return "unknown";
}

class OrderedEventHistoryV1::Impl final {
public:
    struct ChannelState final {
        ChannelState(
            MarketV1 market_value,
            std::size_t block_records,
            std::size_t maximum_inputs)
            : market(market_value),
              inputs(block_records, maximum_inputs) {}

        MarketV1 market = MarketV1::kUnknown;
        OrderedInputBlocksV1 inputs;
        std::unique_ptr<ShanghaiOrderEventAggregatorV1> shanghai;
        std::unique_ptr<ShenzhenOrderEventProjectorV1> shenzhen;
    };

    struct WorkingState final {
        std::map<std::int32_t, std::unique_ptr<ChannelState>> channels;
        std::size_t input_count = 0U;
    };

    struct InstrumentState final {
        std::unique_ptr<WorkingState> working;
        std::atomic<std::shared_ptr<const EventStableRootV1>> root;
        // Live and recovery executors are distinct threads.  This gate keeps
        // the instrument single-writer without putting a mutex on the normal
        // path; a contended live input is recovered from FAST instead.
        std::atomic_flag writer = ATOMIC_FLAG_INIT;
        mutable std::mutex changes_mutex;
        std::vector<EventMutationV1> changes;
        std::uint64_t next_transaction_id = 1U;
        std::atomic<EventRepairStateV1> repair_state{
            EventRepairStateV1::kLive};
        std::atomic<std::uint64_t> repair_through{0U};
    };

    explicit Impl(OrderedEventHistoryConfigV1 config)
        : config_(std::move(config)),
          instruments_(std::make_unique<InstrumentState[]>(
              config_.instrument_count)) {}

    [[nodiscard]] std::shared_ptr<const EventStableRootV1> EmptyRoot(
        std::uint32_t instrument_id) {
        auto impl = std::make_unique<EventStableRootV1::Impl>();
        impl->instrument_id = instrument_id;
        return std::shared_ptr<const EventStableRootV1>(
            new EventStableRootV1(std::move(impl)));
    }

    [[nodiscard]] std::shared_ptr<const EventStableRootV1> BuildRoot(
        std::uint32_t instrument_id,
        std::uint64_t included_change_sequence,
        std::span<const OrderedDerivedEventV1> rows) {
        auto impl = std::make_unique<EventStableRootV1::Impl>();
        impl->instrument_id = instrument_id;
        impl->included_change_sequence = included_change_sequence;
        impl->row_count = rows.size();
        std::size_t channel_begin = 0U;
        while (channel_begin < rows.size()) {
            const std::int32_t channel_id =
                rows[channel_begin].order_key.channel;
            std::size_t channel_end = channel_begin + 1U;
            while (channel_end < rows.size() &&
                   rows[channel_end].order_key.channel == channel_id) {
                ++channel_end;
            }
            EventChannelDataV1 channel{};
            channel.channel = channel_id;
            channel.row_count = channel_end - channel_begin;
            for (std::size_t begin = channel_begin; begin < channel_end;
                 begin += config_.event_block_records) {
                const std::size_t end = std::min(
                    channel_end, begin + config_.event_block_records);
                auto block = std::make_shared<EventBlockDataV1>();
                block->rows.reserve(end - begin);
                block->rows.insert(
                    block->rows.end(),
                    rows.begin() + static_cast<std::ptrdiff_t>(begin),
                    rows.begin() + static_cast<std::ptrdiff_t>(end));
                block->previous = std::move(channel.tail);
                channel.tail = std::move(block);
            }
            impl->channels.push_back(std::move(channel));
            channel_begin = channel_end;
        }
        return std::shared_ptr<const EventStableRootV1>(
            new EventStableRootV1(std::move(impl)));
    }

    [[nodiscard]] std::shared_ptr<const EventStableRootV1>
    InsertIntoRoot(
        const EventStableRootV1& current,
        std::span<const OrderedDerivedEventV1> inserted,
        std::uint64_t included_change_sequence) {
        auto next = std::make_unique<EventStableRootV1::Impl>();
        next->instrument_id = current.impl_->instrument_id;
        next->included_change_sequence = included_change_sequence;
        next->row_count = current.impl_->row_count;
        next->channels = current.impl_->channels;
        if (inserted.empty()) {
            return std::shared_ptr<const EventStableRootV1>(
                new EventStableRootV1(std::move(next)));
        }
        const std::int32_t channel_id = inserted.front().order_key.channel;
        for (std::size_t index = 0U; index < inserted.size(); ++index) {
            if (inserted[index].order_key.channel != channel_id ||
                (index != 0U &&
                 !EventOrderKeyLessV1(
                     inserted[index - 1U].order_key,
                     inserted[index].order_key))) {
                throw std::bad_alloc();
            }
        }
        auto position = std::lower_bound(
            next->channels.begin(),
            next->channels.end(),
            channel_id,
            [](const EventChannelDataV1& channel,
               std::int32_t value) noexcept {
                return channel.channel < value;
            });
        if (position == next->channels.end() ||
            position->channel != channel_id) {
            position = next->channels.insert(
                position, EventChannelDataV1{channel_id, 0U, nullptr});
        } else if (
            position->tail == nullptr || position->tail->rows.empty() ||
            !EventOrderKeyLessV1(
                position->tail->rows.back().order_key,
                inserted.front().order_key)) {
            throw std::bad_alloc();
        }
        for (std::size_t begin = 0U; begin < inserted.size();
             begin += config_.event_block_records) {
            const std::size_t end = std::min(
                inserted.size(), begin + config_.event_block_records);
            auto block = std::make_shared<EventBlockDataV1>();
            block->rows.reserve(end - begin);
            block->rows.insert(
                block->rows.end(),
                inserted.begin() + static_cast<std::ptrdiff_t>(begin),
                inserted.begin() + static_cast<std::ptrdiff_t>(end));
            block->previous = std::move(position->tail);
            position->tail = std::move(block);
        }
        position->row_count += inserted.size();
        next->row_count += inserted.size();
        return std::shared_ptr<const EventStableRootV1>(
            new EventStableRootV1(std::move(next)));
    }

    [[nodiscard]] OrderedEventHistoryErrorV1 EnsureChannel(
        WorkingState* working,
        const CompactFastTickV1& tick,
        ChannelState** output) {
        auto found = working->channels.find(
            tick.business_sequence.channel);
        if (found != working->channels.end()) {
            if (found->second->market != tick.market) {
                return OrderedEventHistoryErrorV1::kInvalidInput;
            }
            *output = found->second.get();
            return OrderedEventHistoryErrorV1::kNone;
        }
        auto channel = std::make_unique<ChannelState>(
            tick.market,
            config_.input_block_records,
            config_.maximum_inputs_per_instrument);
        if (tick.market == MarketV1::kShanghai) {
            const ShanghaiOrderAggregatorCreateErrorV1 error =
                ShanghaiOrderEventAggregatorV1::Create(
                    ShanghaiOrderEventAggregatorConfigV1{
                        config_.trade_date,
                        config_.maximum_order_states_per_instrument},
                    &channel->shanghai);
            if (error != ShanghaiOrderAggregatorCreateErrorV1::kNone) {
                return OrderedEventHistoryErrorV1::kResourceExhausted;
            }
        } else if (tick.market == MarketV1::kShenzhen) {
            const ShenzhenOrderProjectorCreateErrorV1 error =
                ShenzhenOrderEventProjectorV1::Create(
                    ShenzhenOrderEventProjectorConfigV1{
                        config_.trade_date,
                        config_.maximum_order_states_per_instrument},
                    &channel->shenzhen);
            if (error != ShenzhenOrderProjectorCreateErrorV1::kNone) {
                return OrderedEventHistoryErrorV1::kResourceExhausted;
            }
        } else {
            return OrderedEventHistoryErrorV1::kInvalidInput;
        }
        ChannelState* const result = channel.get();
        working->channels.emplace(
            tick.business_sequence.channel, std::move(channel));
        *output = result;
        return OrderedEventHistoryErrorV1::kNone;
    }

    [[nodiscard]] ShanghaiOrderEventInputV1 ShanghaiInput(
        const CompactFastTickV1& tick) const noexcept {
        ShanghaiOrderEventInputV1 input{};
        input.trade_date = tick.trade_date;
        input.instrument_id = tick.instrument_id;
        input.channel = tick.business_sequence.channel;
        input.anchor.native_event_sequence =
            tick.business_sequence.value;
        input.anchor.source_sequence = tick.source_sequence;
        input.anchor.ingress_sequence = tick.arrival_id;
        input.anchor.vendor_sequence_id = tick.vendor_sequence_id;
        input.anchor.event_time_ns_since_midnight =
            tick.event_time_ns_since_midnight;
        input.anchor.event_time_unix_ns = tick.event_time_unix_ns;
        input.anchor.recv_realtime_ns = tick.recv_realtime_ns;
        input.anchor.recv_monotonic_ns = tick.recv_monotonic_ns;
        input.anchor.vendor_local_time_raw = tick.vendor_local_time_raw;
        input.anchor.vendor_local_time_ns_since_midnight =
            tick.vendor_local_time_ns_since_midnight;
        input.anchor.event_time_valid = tick.event_time_valid;
        input.anchor.event_time_unix_ns_valid =
            tick.event_time_unix_ns_valid;
        input.anchor.vendor_local_time_valid =
            tick.vendor_local_time_valid;
        input.action = tick.action;
        input.side = tick.side;
        input.aggressor = tick.aggressor;
        input.phase = tick.phase;
        input.price_p6 = tick.price_p6;
        input.trade_amount_p6 = tick.trade_amount_p6;
        input.quantity = tick.quantity_raw;
        input.matched_quantity = tick.matched_quantity_raw;
        input.primary_order_id = tick.primary_order_id;
        input.buy_order_id = tick.buy_order_id;
        input.sell_order_id = tick.sell_order_id;
        input.price_valid =
            (tick.validity_bitmap & kTickPriceValidV1) != 0U;
        input.trade_amount_valid =
            (tick.validity_bitmap & kTickTradeAmountValidV1) != 0U;
        input.quantity_valid =
            (tick.validity_bitmap & kTickQuantityValidV1) != 0U;
        input.matched_quantity_valid =
            (tick.validity_bitmap & kTickMatchedQuantityValidV1) != 0U;
        input.phase_valid =
            (tick.validity_bitmap & kTickPhaseValidV1) != 0U;
        input.source_quality_flags = tick.quality_flags;
        input.source_market_notices = tick.market_notices;
        return input;
    }

    [[nodiscard]] ShenzhenOrderEventInputV1 ShenzhenInput(
        const CompactFastTickV1& tick) const noexcept {
        ShenzhenOrderEventInputV1 input{};
        input.trade_date = tick.trade_date;
        input.instrument_id = tick.instrument_id;
        input.channel = static_cast<std::uint32_t>(
            tick.business_sequence.channel);
        input.anchor.native_event_sequence =
            tick.business_sequence.value;
        input.anchor.source_sequence = tick.source_sequence;
        input.anchor.ingress_sequence = tick.arrival_id;
        input.anchor.vendor_sequence_id = tick.vendor_sequence_id;
        input.anchor.event_time_ns_since_midnight =
            tick.event_time_ns_since_midnight;
        input.anchor.event_time_unix_ns = tick.event_time_unix_ns;
        input.anchor.recv_realtime_ns = tick.recv_realtime_ns;
        input.anchor.recv_monotonic_ns = tick.recv_monotonic_ns;
        input.anchor.vendor_local_time_raw = tick.vendor_local_time_raw;
        input.anchor.vendor_local_time_ns_since_midnight =
            tick.vendor_local_time_ns_since_midnight;
        input.anchor.event_time_valid = tick.event_time_valid;
        input.anchor.event_time_unix_ns_valid =
            tick.event_time_unix_ns_valid;
        input.anchor.vendor_local_time_valid =
            tick.vendor_local_time_valid;
        input.action = tick.action;
        input.side = tick.side;
        input.order_type = tick.order_type;
        input.price_p6 = tick.price_p6;
        input.quantity = tick.quantity_raw;
        input.primary_order_id = tick.primary_order_id;
        input.buy_order_id = tick.buy_order_id;
        input.sell_order_id = tick.sell_order_id;
        input.price_valid =
            (tick.validity_bitmap & kTickPriceValidV1) != 0U;
        input.quantity_valid =
            (tick.validity_bitmap & kTickQuantityValidV1) != 0U;
        input.side_valid =
            (tick.validity_bitmap & kTickSideValidV1) != 0U;
        input.order_type_valid =
            (tick.validity_bitmap & kTickOrderTypeValidV1) != 0U;
        input.source_quality_flags = tick.quality_flags;
        input.source_market_notices = tick.market_notices;
        return input;
    }

    [[nodiscard]] OrderedEventHistoryErrorV1 Consume(
        ChannelState* channel,
        const CompactFastTickV1& tick,
        std::vector<OrderedDerivedEventV1>* rows) {
        rows->clear();
        std::vector<DerivedEventPayloadV1> payloads;
        if (channel->market == MarketV1::kShanghai) {
            std::vector<ShanghaiOrderEventV1> output;
            const ShanghaiOrderAggregatorConsumeErrorV1 error =
                channel->shanghai->ConsumeBusinessOrdered(
                    ShanghaiInput(tick), &output);
            if (error != ShanghaiOrderAggregatorConsumeErrorV1::kNone) {
                return error ==
                               ShanghaiOrderAggregatorConsumeErrorV1::
                                   kResourceExhausted
                           ? OrderedEventHistoryErrorV1::
                                 kResourceExhausted
                           : OrderedEventHistoryErrorV1::kCoreFailed;
            }
            payloads.reserve(output.size());
            for (ShanghaiOrderEventV1& event : output) {
                payloads.emplace_back(std::visit(
                    [](auto&& value) -> DerivedEventPayloadV1 {
                        return DerivedEventPayloadV1(
                            std::forward<decltype(value)>(value));
                    },
                    std::move(event)));
            }
        } else {
            std::vector<ShenzhenOrderEventV1> output;
            const ShenzhenOrderProjectorConsumeErrorV1 error =
                channel->shenzhen->ConsumeBusinessOrdered(
                    ShenzhenInput(tick), &output);
            if (error != ShenzhenOrderProjectorConsumeErrorV1::kNone) {
                return error ==
                               ShenzhenOrderProjectorConsumeErrorV1::
                                   kResourceExhausted
                           ? OrderedEventHistoryErrorV1::
                                 kResourceExhausted
                           : OrderedEventHistoryErrorV1::kCoreFailed;
            }
            payloads.reserve(output.size());
            for (ShenzhenOrderEventV1& event : output) {
                payloads.emplace_back(std::visit(
                    [](auto&& value) -> DerivedEventPayloadV1 {
                        return DerivedEventPayloadV1(
                            std::forward<decltype(value)>(value));
                    },
                    std::move(event)));
            }
        }

        rows->reserve(payloads.size());
        std::map<std::pair<DerivedEventKindV1, std::int64_t>,
                 std::uint32_t>
            occurrences;
        for (std::size_t index = 0U; index < payloads.size(); ++index) {
            DerivedEventPayloadV1& payload = payloads[index];
            const DerivedEventKindV1 kind = PayloadKind(payload);
            const std::int64_t affected = AffectedOrderId(payload);
            std::uint32_t& occurrence = occurrences[{kind, affected}];
            OrderedDerivedEventV1 row{};
            row.uid.instrument_id = tick.instrument_id;
            row.uid.channel = tick.business_sequence.channel;
            row.uid.business_sequence = tick.business_sequence.value;
            row.uid.kind = kind;
            row.uid.affected_order_id = affected;
            row.uid.occurrence = occurrence;
            ++occurrence;
            row.order_key.channel = tick.business_sequence.channel;
            row.order_key.business_sequence =
                tick.business_sequence.value;
            row.order_key.source_event_ordinal = 0U;
            row.order_key.derived_event_ordinal =
                static_cast<std::uint32_t>(index);
            row.order_key.affected_order_id = affected;
            row.payload = std::move(payload);
            row.source_arrival_id = tick.arrival_id;
            rows->push_back(std::move(row));
        }
        return OrderedEventHistoryErrorV1::kNone;
    }

    [[nodiscard]] OrderedEventHistoryErrorV1 StageInsertChanges(
        InstrumentState* state,
        std::span<const OrderedDerivedEventV1> rows,
        std::vector<EventMutationV1>* staged,
        std::uint64_t* final_sequence) {
        std::lock_guard<std::mutex> lock(state->changes_mutex);
        const std::uint64_t base = state->changes.size();
        if (state->changes.size() >
                config_.maximum_change_records_per_instrument ||
            rows.size() >
                config_.maximum_change_records_per_instrument -
                    state->changes.size()) {
            return OrderedEventHistoryErrorV1::kChangeCapacity;
        }
        if (!ChangeSequenceCanAppend(
                state->changes.size(), rows.size())) {
            return OrderedEventHistoryErrorV1::kChangeCapacity;
        }
        staged->clear();
        staged->reserve(rows.size());
        for (std::size_t index = 0U; index < rows.size(); ++index) {
            EventMutationV1 mutation{};
            mutation.change_sequence =
                base + static_cast<std::uint64_t>(index) + 1U;
            mutation.kind = EventMutationKindV1::kInsert;
            mutation.uid = rows[index].uid;
            mutation.row = rows[index];
            staged->push_back(std::move(mutation));
        }
        if (!ReserveForAppend(
                &state->changes,
                staged->size(),
                config_.maximum_change_records_per_instrument)) {
            return OrderedEventHistoryErrorV1::kChangeCapacity;
        }
        *final_sequence = base + rows.size();
        return OrderedEventHistoryErrorV1::kNone;
    }

    void CommitStagedAndPublish(
        InstrumentState* state,
        std::vector<EventMutationV1>* staged,
        std::shared_ptr<const EventStableRootV1> root) {
        std::lock_guard<std::mutex> lock(state->changes_mutex);
        for (EventMutationV1& mutation : *staged) {
            state->changes.push_back(std::move(mutation));
        }
        // AcquireStable itself stays lock-free. If it observes this new root
        // while this lock is still held, its subsequent ReadChanges blocks
        // here until every change named by included_change_sequence exists.
        // Conversely, a ReadChanges call cannot observe the new transaction
        // before the root switch because both operations share this lock.
        state->root.store(std::move(root), std::memory_order_release);
        staged->clear();
    }

    OrderedEventHistoryConfigV1 config_{};
    std::unique_ptr<InstrumentState[]> instruments_;
    std::atomic<std::uint64_t> monotonic_fast_path_inputs_{0U};
    std::atomic<std::uint64_t> duplicate_inputs_{0U};
    std::atomic<std::uint64_t> late_inputs_{0U};
    std::atomic<std::uint64_t> source_conflicts_{0U};
    std::atomic<std::uint64_t> rebuild_no_sort_{0U};
    std::atomic<std::uint64_t> rebuild_natural_run_merge_{0U};
    std::atomic<std::uint64_t> rebuild_radix_sort_{0U};
    std::atomic<std::uint64_t> published_range_transactions_{0U};
};

OrderedEventHistoryV1::OrderedEventHistoryV1(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

OrderedEventHistoryV1::~OrderedEventHistoryV1() = default;

OrderedEventHistoryCreateErrorV1 OrderedEventHistoryV1::Create(
    OrderedEventHistoryConfigV1 config,
    std::unique_ptr<OrderedEventHistoryV1>* output) noexcept {
    if (output == nullptr) {
        return OrderedEventHistoryCreateErrorV1::kNullOutput;
    }
    output->reset();
    if (ZeroIdentity(config.session_id) ||
        !ValidTradeDate(config.trade_date) ||
        config.instrument_count == 0U || config.worker_count == 0U ||
        config.instrument_count > static_cast<std::size_t>(
            std::numeric_limits<std::uint32_t>::max()) ||
        config.worker_count > config.instrument_count ||
        config.maximum_order_states_per_instrument == 0U ||
        config.maximum_inputs_per_instrument == 0U ||
        config.maximum_events_per_instrument == 0U ||
        config.input_block_records < 2U ||
        config.input_block_records > 4096U ||
        config.event_block_records < 2U ||
        config.event_block_records > 4096U ||
        config.cdc_range_chunk_records == 0U ||
        config.maximum_change_records_per_instrument == 0U ||
        config.maximum_changes_per_read == 0U ||
        config.repair_replay_record_budget == 0U ||
        config.event_routes.size() != config.instrument_count) {
        return OrderedEventHistoryCreateErrorV1::kInvalidConfiguration;
    }
    for (std::uint32_t worker : config.event_routes) {
        if (worker >= config.worker_count) {
            return OrderedEventHistoryCreateErrorV1::
                kInvalidConfiguration;
        }
    }
    try {
        auto impl = std::make_unique<Impl>(std::move(config));
        for (std::size_t ordinal = 0U;
             ordinal < impl->config_.instrument_count; ++ordinal) {
            Impl::InstrumentState& state = impl->instruments_[ordinal];
            state.working = std::make_unique<Impl::WorkingState>();
            state.root.store(
                impl->EmptyRoot(
                    static_cast<std::uint32_t>(ordinal + 1U)),
                std::memory_order_release);
        }
        output->reset(new OrderedEventHistoryV1(std::move(impl)));
        return OrderedEventHistoryCreateErrorV1::kNone;
    } catch (...) {
        return OrderedEventHistoryCreateErrorV1::kResourceExhausted;
    }
}

OrderedEventHistoryErrorV1 OrderedEventHistoryV1::ResolveRoute(
    std::size_t ordinal,
    std::uint32_t instrument_id,
    EventRouteTokenV1* output) const noexcept {
    if (output == nullptr) {
        return OrderedEventHistoryErrorV1::kNullOutput;
    }
    *output = {};
    if (impl_ == nullptr || ordinal >= impl_->config_.instrument_count ||
        instrument_id == 0U ||
        instrument_id != static_cast<std::uint32_t>(ordinal + 1U)) {
        return OrderedEventHistoryErrorV1::kInvalidInput;
    }
    output->instrument_id = instrument_id;
    output->ordinal = ordinal;
    output->worker = impl_->config_.event_routes[ordinal];
    return OrderedEventHistoryErrorV1::kNone;
}

EventApplyResultV1 OrderedEventHistoryV1::ApplyLive(
    std::uint32_t worker,
    const EventRouteTokenV1& route,
    const CompactFastTickV1& tick) noexcept {
    EventApplyResultV1 result{};
    if (impl_ == nullptr || !ValidTickForEvent(tick) ||
        tick.trade_date != impl_->config_.trade_date ||
        route.ordinal >= impl_->config_.instrument_count ||
        tick.ordinal != route.ordinal ||
        route.instrument_id != tick.instrument_id ||
        route.instrument_id !=
            static_cast<std::uint32_t>(route.ordinal + 1U)) {
        result.error = OrderedEventHistoryErrorV1::kInvalidInput;
        return result;
    }
    if (worker >= impl_->config_.worker_count ||
        route.worker != worker ||
        impl_->config_.event_routes[route.ordinal] != worker) {
        result.error = OrderedEventHistoryErrorV1::kWrongWorker;
        return result;
    }
    Impl::InstrumentState& state = impl_->instruments_[route.ordinal];
    if (state.writer.test_and_set(std::memory_order_acquire)) {
        MarkRepairRequired(tick.instrument_id, tick.arrival_id);
        result.error = OrderedEventHistoryErrorV1::kNotLive;
        result.disposition = EventInputDispositionV1::kRepairRegistered;
        return result;
    }
    struct WriterGuard final {
        std::atomic_flag* writer = nullptr;
        ~WriterGuard() { writer->clear(std::memory_order_release); }
    } writer_guard{&state.writer};
    if (state.repair_state.load(std::memory_order_acquire) !=
        EventRepairStateV1::kLive) {
        result.error = OrderedEventHistoryErrorV1::kNotLive;
        return result;
    }
    try {
        Impl::ChannelState* channel = nullptr;
        result.error = impl_->EnsureChannel(
            state.working.get(), tick, &channel);
        if (result.error != OrderedEventHistoryErrorV1::kNone) {
            result.disposition =
                EventInputDispositionV1::kRepairRegistered;
            if (result.error ==
                OrderedEventHistoryErrorV1::kResourceExhausted) {
                MarkRepairRequired(tick.instrument_id, tick.arrival_id);
            } else {
                // A FAST fact that cannot belong to the instrument/channel
                // core is deterministic. Retrying the same complete history
                // cannot make it valid, so never leave the view marked LIVE.
                MarkUnrecoverable(tick.instrument_id);
            }
            return result;
        }
        const OrderedInputBlocksV1::InsertResult inserted =
            channel->inputs.Insert(tick);
        if (inserted == OrderedInputBlocksV1::InsertResult::kCapacity) {
            result.error = OrderedEventHistoryErrorV1::kInputCapacity;
            MarkUnrecoverable(tick.instrument_id);
            return result;
        }
        if (inserted == OrderedInputBlocksV1::InsertResult::kDuplicate) {
            impl_->duplicate_inputs_.fetch_add(
                1U, std::memory_order_relaxed);
            result.disposition =
                EventInputDispositionV1::kDuplicateIgnored;
            return result;
        }
        if (inserted == OrderedInputBlocksV1::InsertResult::kConflict) {
            impl_->source_conflicts_.fetch_add(
                1U, std::memory_order_relaxed);
            MarkSourceConflictUnlessUnrecoverable(&state.repair_state);
            result.error = OrderedEventHistoryErrorV1::kSourceConflict;
            result.disposition =
                EventInputDispositionV1::kSourceConflict;
            return result;
        }
        if (state.working->input_count >=
            impl_->config_.maximum_inputs_per_instrument) {
            result.error = OrderedEventHistoryErrorV1::kInputCapacity;
            MarkUnrecoverable(tick.instrument_id);
            return result;
        }
        ++state.working->input_count;
        if (inserted == OrderedInputBlocksV1::InsertResult::kLate) {
            impl_->late_inputs_.fetch_add(1U, std::memory_order_relaxed);
            result.disposition =
                EventInputDispositionV1::kRepairRegistered;
            result.dirty_from = tick.business_sequence;
            MarkRepairRequired(tick.instrument_id, tick.arrival_id);
            return result;
        }
        impl_->monotonic_fast_path_inputs_.fetch_add(
            1U, std::memory_order_relaxed);
        std::vector<OrderedDerivedEventV1> rows;
        result.error = impl_->Consume(channel, tick, &rows);
        if (result.error != OrderedEventHistoryErrorV1::kNone) {
            result.disposition =
                EventInputDispositionV1::kRepairRegistered;
            if (result.error ==
                OrderedEventHistoryErrorV1::kResourceExhausted) {
                MarkRepairRequired(tick.instrument_id, tick.arrival_id);
            } else {
                MarkUnrecoverable(tick.instrument_id);
            }
            return result;
        }
        if (rows.empty()) {
            result.disposition =
                EventInputDispositionV1::kNoDerivedRows;
            return result;
        }
        const auto current = state.root.load(std::memory_order_acquire);
        if (current == nullptr ||
            rows.size() >
                impl_->config_.maximum_events_per_instrument ||
            current->row_count() >
                impl_->config_.maximum_events_per_instrument -
                    rows.size()) {
            result.error = OrderedEventHistoryErrorV1::kEventCapacity;
            MarkUnrecoverable(tick.instrument_id);
            return result;
        }
        std::vector<EventMutationV1> staged;
        std::uint64_t final_sequence = 0U;
        result.error = impl_->StageInsertChanges(
            &state, rows, &staged, &final_sequence);
        if (result.error != OrderedEventHistoryErrorV1::kNone) {
            if (result.error ==
                OrderedEventHistoryErrorV1::kChangeCapacity) {
                MarkUnrecoverable(tick.instrument_id);
            } else {
                MarkRepairRequired(tick.instrument_id, tick.arrival_id);
            }
            return result;
        }
        const auto next = impl_->InsertIntoRoot(
            *current, rows, final_sequence);
        impl_->CommitStagedAndPublish(
            &state, &staged, std::move(next));
        result.disposition = EventInputDispositionV1::kPublished;
        result.published_rows = rows.size();
        return result;
    } catch (...) {
        result.error = OrderedEventHistoryErrorV1::kResourceExhausted;
        result.disposition = EventInputDispositionV1::kRepairRegistered;
        MarkRepairRequired(tick.instrument_id, tick.arrival_id);
        return result;
    }
}

EventRebuildResultV1 OrderedEventHistoryV1::RebuildFromFast(
    std::uint32_t worker,
    const EventRouteTokenV1& route,
    const FastTickStoreV1& fast_store) noexcept {
    EventRebuildResultV1 result{};
    if (impl_ == nullptr || route.ordinal >= impl_->config_.instrument_count ||
        route.instrument_id == 0U ||
        route.instrument_id !=
            static_cast<std::uint32_t>(route.ordinal + 1U)) {
        result.error = OrderedEventHistoryErrorV1::kInvalidInput;
        return result;
    }
    if (worker >= impl_->config_.worker_count ||
        route.worker != worker ||
        impl_->config_.event_routes[route.ordinal] != worker) {
        result.error = OrderedEventHistoryErrorV1::kWrongWorker;
        return result;
    }
    Impl::InstrumentState& state = impl_->instruments_[route.ordinal];
    if (state.writer.test_and_set(std::memory_order_acquire)) {
        result.error = OrderedEventHistoryErrorV1::kNotLive;
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
                               ? OrderedEventHistoryErrorV1::kSourceConflict
                               : OrderedEventHistoryErrorV1::kFastCoverageLost;
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

    FastTickInstrumentStatusV1 fast_status{};
    if (fast_store.Status(route.instrument_id, &fast_status) !=
            FastTickStoreQueryErrorV1::kNone ||
        !fast_status.coverage_complete) {
        state.repair_state.store(
            EventRepairStateV1::kUnrecoverable,
            std::memory_order_release);
        result.error = OrderedEventHistoryErrorV1::kFastCoverageLost;
        return result;
    }
    std::vector<CompactFastTickV1> ticks;
    if (fast_store.CopyCompactHistory(
            route.instrument_id,
            &ticks,
            &result.captured_fast_tail) !=
        FastTickStoreQueryErrorV1::kNone) {
        RestoreRepairRequiredUnlessTerminal(&state.repair_state);
        result.error = OrderedEventHistoryErrorV1::kResourceExhausted;
        return result;
    }
    if (ticks.size() > impl_->config_.maximum_inputs_per_instrument) {
        state.repair_state.store(
            EventRepairStateV1::kUnrecoverable,
            std::memory_order_release);
        result.error = OrderedEventHistoryErrorV1::kInputCapacity;
        return result;
    }

    try {
        std::map<std::int32_t, std::vector<CompactFastTickV1>> channels;
        std::size_t records_since_yield = 0U;
        for (const CompactFastTickV1& tick : ticks) {
            if (!ValidTickForEvent(tick) ||
                tick.instrument_id != route.instrument_id ||
                tick.trade_date != impl_->config_.trade_date) {
                state.repair_state.store(
                    EventRepairStateV1::kUnrecoverable,
                    std::memory_order_release);
                result.error = OrderedEventHistoryErrorV1::kInvalidInput;
                return result;
            }
            channels[tick.business_sequence.channel].push_back(tick);
            CooperativeRepairYield(
                &records_since_yield,
                impl_->config_.repair_replay_record_budget);
        }

        auto rebuilt = std::make_unique<Impl::WorkingState>();
        std::vector<OrderedDerivedEventV1> all_rows;
        const std::size_t reserve_rows =
            ticks.size() >
                    impl_->config_.maximum_events_per_instrument / 2U
                ? impl_->config_.maximum_events_per_instrument
                : std::min<std::size_t>(
                      ticks.size() * 2U,
                      impl_->config_.maximum_events_per_instrument);
        all_rows.reserve(reserve_rows);
        std::vector<CompactFastTickV1> scratch;
        for (auto& [channel_id, inputs] : channels) {
            static_cast<void>(channel_id);
            const AdaptiveSortChoiceV1 choice =
                AdaptiveBusinessSort(
                    &inputs,
                    &scratch,
                    impl_->config_.repair_replay_record_budget);
            if (choice == AdaptiveSortChoiceV1::kNoSort) {
                impl_->rebuild_no_sort_.fetch_add(
                    1U, std::memory_order_relaxed);
            } else if (choice == AdaptiveSortChoiceV1::kNaturalRuns) {
                impl_->rebuild_natural_run_merge_.fetch_add(
                    1U, std::memory_order_relaxed);
            } else {
                impl_->rebuild_radix_sort_.fetch_add(
                    1U, std::memory_order_relaxed);
            }

            std::vector<CompactFastTickV1> unique;
            unique.reserve(inputs.size());
            for (CompactFastTickV1& input : inputs) {
                CooperativeRepairYield(
                    &records_since_yield,
                    impl_->config_.repair_replay_record_budget);
                if (!unique.empty() &&
                    unique.back().business_sequence.value ==
                        input.business_sequence.value) {
                    if (!SameFastTickPayloadV1(unique.back(), input)) {
                        MarkSourceConflictUnlessUnrecoverable(
                            &state.repair_state);
                        impl_->source_conflicts_.fetch_add(
                            1U, std::memory_order_relaxed);
                        result.error =
                            OrderedEventHistoryErrorV1::kSourceConflict;
                        return result;
                    }
                    continue;
                }
                unique.push_back(std::move(input));
            }

            for (const CompactFastTickV1& input : unique) {
                Impl::ChannelState* channel = nullptr;
                result.error = impl_->EnsureChannel(
                    rebuilt.get(), input, &channel);
                if (result.error != OrderedEventHistoryErrorV1::kNone) {
                    if (result.error ==
                        OrderedEventHistoryErrorV1::kResourceExhausted) {
                        RestoreRepairRequiredUnlessTerminal(
                            &state.repair_state);
                    } else {
                        state.repair_state.store(
                            EventRepairStateV1::kUnrecoverable,
                            std::memory_order_release);
                    }
                    return result;
                }
                const auto inserted = channel->inputs.Insert(input);
                if (inserted !=
                    OrderedInputBlocksV1::InsertResult::kAppended) {
                    result.error = OrderedEventHistoryErrorV1::kCoreFailed;
                    state.repair_state.store(
                        EventRepairStateV1::kUnrecoverable,
                        std::memory_order_release);
                    return result;
                }
                std::vector<OrderedDerivedEventV1> emitted;
                result.error = impl_->Consume(channel, input, &emitted);
                if (result.error != OrderedEventHistoryErrorV1::kNone) {
                    if (result.error ==
                        OrderedEventHistoryErrorV1::kResourceExhausted) {
                        RestoreRepairRequiredUnlessTerminal(
                            &state.repair_state);
                    } else {
                        state.repair_state.store(
                            EventRepairStateV1::kUnrecoverable,
                            std::memory_order_release);
                    }
                    return result;
                }
                if (emitted.size() >
                        impl_->config_.maximum_events_per_instrument ||
                    all_rows.size() >
                        impl_->config_.maximum_events_per_instrument -
                            emitted.size()) {
                    state.repair_state.store(
                        EventRepairStateV1::kUnrecoverable,
                        std::memory_order_release);
                    result.error =
                        OrderedEventHistoryErrorV1::kEventCapacity;
                    return result;
                }
                all_rows.insert(
                    all_rows.end(),
                    std::make_move_iterator(emitted.begin()),
                    std::make_move_iterator(emitted.end()));
                ++rebuilt->input_count;
                CooperativeRepairYield(
                    &records_since_yield,
                    impl_->config_.repair_replay_record_budget);
            }
        }
        if (!std::is_sorted(
                all_rows.begin(),
                all_rows.end(),
                [](const OrderedDerivedEventV1& lhs,
                   const OrderedDerivedEventV1& rhs) noexcept {
                    return EventOrderKeyLessV1(
                        lhs.order_key, rhs.order_key);
                })) {
            result.error = OrderedEventHistoryErrorV1::kCoreFailed;
            state.repair_state.store(
                EventRepairStateV1::kUnrecoverable,
                std::memory_order_release);
            return result;
        }

        std::vector<EventMutationV1> staged;
        std::uint64_t final_sequence = 0U;
        {
            std::lock_guard<std::mutex> lock(state.changes_mutex);
            const std::size_t chunk_records =
                impl_->config_.cdc_range_chunk_records;
            const std::size_t chunks =
                all_rows.size() / chunk_records +
                (all_rows.size() % chunk_records == 0U ? 0U : 1U);
            if (chunks >
                std::numeric_limits<std::size_t>::max() - 2U) {
                throw std::bad_alloc();
            }
            const std::size_t mutation_count = chunks + 2U;
            if (state.changes.size() >
                    impl_->config_.maximum_change_records_per_instrument ||
                mutation_count >
                    impl_->config_.maximum_change_records_per_instrument -
                        state.changes.size()) {
                state.repair_state.store(
                    EventRepairStateV1::kUnrecoverable,
                    std::memory_order_release);
                result.error =
                    OrderedEventHistoryErrorV1::kChangeCapacity;
                return result;
            }
            if (!ChangeSequenceCanAppend(
                    state.changes.size(), mutation_count)) {
                state.repair_state.store(
                    EventRepairStateV1::kUnrecoverable,
                    std::memory_order_release);
                result.error =
                    OrderedEventHistoryErrorV1::kChangeCapacity;
                return result;
            }
            if (state.next_transaction_id ==
                std::numeric_limits<std::uint64_t>::max()) {
                state.repair_state.store(
                    EventRepairStateV1::kUnrecoverable,
                    std::memory_order_release);
                result.error =
                    OrderedEventHistoryErrorV1::kChangeCapacity;
                return result;
            }
            const std::uint64_t transaction =
                state.next_transaction_id++;
            result.range_transaction_id = transaction;
            staged.reserve(mutation_count);
            EventMutationV1 begin{};
            begin.change_sequence = state.changes.size() + 1U;
            begin.transaction_id = transaction;
            begin.kind = EventMutationKindV1::kRangeReplaceBegin;
            begin.replace_entire_instrument = true;
            staged.push_back(std::move(begin));
            for (std::size_t offset = 0U; offset < all_rows.size();
                 offset += impl_->config_.cdc_range_chunk_records) {
                const std::size_t end = std::min(
                    all_rows.size(),
                    offset + impl_->config_.cdc_range_chunk_records);
                EventMutationV1 chunk{};
                chunk.change_sequence =
                    state.changes.size() + staged.size() + 1U;
                chunk.transaction_id = transaction;
                chunk.kind = EventMutationKindV1::kRangeReplaceChunk;
                chunk.replacement_rows.insert(
                    chunk.replacement_rows.end(),
                    all_rows.begin() +
                        static_cast<std::ptrdiff_t>(offset),
                    all_rows.begin() +
                        static_cast<std::ptrdiff_t>(end));
                staged.push_back(std::move(chunk));
            }
            EventMutationV1 commit{};
            commit.change_sequence =
                state.changes.size() + staged.size() + 1U;
            commit.transaction_id = transaction;
            commit.kind = EventMutationKindV1::kRangeReplaceCommit;
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
                    OrderedEventHistoryErrorV1::kChangeCapacity;
                return result;
            }
        }

        const auto root = impl_->BuildRoot(
            route.instrument_id, final_sequence, all_rows);
        EventRepairStateV1 rebuilding = EventRepairStateV1::kRebuilding;
        if (!state.repair_state.compare_exchange_strong(
                rebuilding,
                EventRepairStateV1::kCatchingUp,
                std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            result.error = rebuilding == EventRepairStateV1::kSourceConflict
                               ? OrderedEventHistoryErrorV1::kSourceConflict
                               : rebuilding ==
                                         EventRepairStateV1::kUnrecoverable
                                     ? OrderedEventHistoryErrorV1::
                                           kFastCoverageLost
                                     : OrderedEventHistoryErrorV1::kNotLive;
            return result;
        }
        impl_->CommitStagedAndPublish(
            &state, &staged, std::move(root));
        state.working = std::move(rebuilt);
        result.rebuilt_inputs = ticks.size();
        result.rebuilt_rows = all_rows.size();
        impl_->published_range_transactions_.fetch_add(
            1U, std::memory_order_relaxed);

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
            after.published_tail == result.captured_fast_tail &&
            state.repair_through.load(std::memory_order_acquire) <=
                after.latest_arrival_id) {
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
        result.error = OrderedEventHistoryErrorV1::kResourceExhausted;
        return result;
    }
}

void OrderedEventHistoryV1::MarkRepairRequired(
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

void OrderedEventHistoryV1::MarkUnrecoverable(
    std::uint32_t instrument_id) noexcept {
    if (impl_ == nullptr || instrument_id == 0U ||
        instrument_id > impl_->config_.instrument_count) {
        return;
    }
    impl_->instruments_[instrument_id - 1U].repair_state.store(
        EventRepairStateV1::kUnrecoverable,
        std::memory_order_release);
}

OrderedEventHistoryErrorV1 OrderedEventHistoryV1::AcquireStable(
    std::uint32_t instrument_id,
    EventStableSnapshotV1* output) const noexcept {
    if (output == nullptr) {
        return OrderedEventHistoryErrorV1::kNullOutput;
    }
    *output = {};
    if (impl_ == nullptr || instrument_id == 0U ||
        instrument_id > impl_->config_.instrument_count) {
        return OrderedEventHistoryErrorV1::kInvalidInput;
    }
    const Impl::InstrumentState& state =
        impl_->instruments_[instrument_id - 1U];
    output->root = state.root.load(std::memory_order_acquire);
    if (output->root == nullptr) {
        return OrderedEventHistoryErrorV1::kResourceExhausted;
    }
    output->next_changes.session_id = impl_->config_.session_id;
    output->next_changes.instrument_id = instrument_id;
    output->next_changes.next_change_sequence =
        output->root->included_change_sequence() + 1U;
    output->repair_state = state.repair_state.load(
        std::memory_order_acquire);
    output->repair_through_arrival_id = state.repair_through.load(
        std::memory_order_acquire);
    return OrderedEventHistoryErrorV1::kNone;
}

OrderedEventHistoryErrorV1 OrderedEventHistoryV1::ReadChanges(
    EventChangeCursorV1* cursor,
    std::span<EventMutationV1> output,
    std::size_t* written) const noexcept {
    if (cursor == nullptr || written == nullptr) {
        return OrderedEventHistoryErrorV1::kNullOutput;
    }
    *written = 0U;
    if (impl_ == nullptr || output.empty() ||
        cursor->session_id != impl_->config_.session_id ||
        cursor->instrument_id == 0U ||
        cursor->instrument_id > impl_->config_.instrument_count ||
        cursor->next_change_sequence == 0U) {
        return OrderedEventHistoryErrorV1::kCursorMismatch;
    }
    if (output.size() > impl_->config_.maximum_changes_per_read) {
        return OrderedEventHistoryErrorV1::kBatchLimitExceeded;
    }
    const Impl::InstrumentState& state =
        impl_->instruments_[cursor->instrument_id - 1U];
    try {
        std::lock_guard<std::mutex> lock(state.changes_mutex);
        if (cursor->next_change_sequence > state.changes.size() + 1U) {
            return OrderedEventHistoryErrorV1::kCursorMismatch;
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
        return OrderedEventHistoryErrorV1::kNone;
    } catch (...) {
        *written = 0U;
        return OrderedEventHistoryErrorV1::kResourceExhausted;
    }
}

EventRepairStateV1 OrderedEventHistoryV1::RepairState(
    std::uint32_t instrument_id) const noexcept {
    if (impl_ == nullptr || instrument_id == 0U ||
        instrument_id > impl_->config_.instrument_count) {
        return EventRepairStateV1::kUnrecoverable;
    }
    return impl_->instruments_[instrument_id - 1U].repair_state.load(
        std::memory_order_acquire);
}

std::uint64_t OrderedEventHistoryV1::RepairThrough(
    std::uint32_t instrument_id) const noexcept {
    if (impl_ == nullptr || instrument_id == 0U ||
        instrument_id > impl_->config_.instrument_count) {
        return 0U;
    }
    return impl_->instruments_[instrument_id - 1U].repair_through.load(
        std::memory_order_acquire);
}

OrderedEventHistoryStatsV1 OrderedEventHistoryV1::Stats()
    const noexcept {
    OrderedEventHistoryStatsV1 result{};
    if (impl_ == nullptr) {
        return result;
    }
    result.monotonic_fast_path_inputs =
        impl_->monotonic_fast_path_inputs_.load(std::memory_order_acquire);
    result.duplicate_inputs = impl_->duplicate_inputs_.load(
        std::memory_order_acquire);
    result.late_inputs = impl_->late_inputs_.load(
        std::memory_order_acquire);
    result.source_conflicts = impl_->source_conflicts_.load(
        std::memory_order_acquire);
    result.rebuild_no_sort = impl_->rebuild_no_sort_.load(
        std::memory_order_acquire);
    result.rebuild_natural_run_merge =
        impl_->rebuild_natural_run_merge_.load(
            std::memory_order_acquire);
    result.rebuild_radix_sort = impl_->rebuild_radix_sort_.load(
        std::memory_order_acquire);
    result.full_comparison_sort_calls = 0U;
    result.published_range_transactions =
        impl_->published_range_transactions_.load(
            std::memory_order_acquire);
    return result;
}

const OrderedEventHistoryConfigV1& OrderedEventHistoryV1::config()
    const noexcept {
    return impl_->config_;
}

}  // namespace l2flow::market
