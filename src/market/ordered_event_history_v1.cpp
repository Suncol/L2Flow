#include "l2flow/market/ordered_event_history_v1.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <deque>
#include <map>
#include <mutex>
#include <new>
#include <optional>
#include <queue>
#include <thread>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
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
    EventOrderKeyV1 first_key{};
    EventOrderKeyV1 last_key{};
    std::uint64_t cumulative_row_count = 0U;
};

struct EventChannelDataV1 final {
    std::int32_t channel = 0;
    std::uint64_t row_count = 0U;
    std::shared_ptr<const EventBlockDataV1> tail;
};

void SealEventBlockMetadata(EventBlockDataV1* block) noexcept {
    if (block == nullptr || block->rows.empty()) {
        return;
    }
    block->first_key = block->rows.front().order_key;
    block->last_key = block->rows.back().order_key;
    const std::uint64_t prior = block->previous == nullptr
        ? 0U
        : block->previous->cumulative_row_count;
    block->cumulative_row_count =
        prior + static_cast<std::uint64_t>(block->rows.size());
}

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

    struct Cursor final {
        std::size_t block = 0U;
        std::size_t row = 0U;
        std::int64_t last_sequence = 0;
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
            ++generation_;
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
        ++generation_;
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

    [[nodiscard]] std::size_t size() const noexcept { return count_; }

    [[nodiscard]] std::uint64_t generation() const noexcept {
        return generation_;
    }

    [[nodiscard]] Cursor LowerBound(std::int64_t sequence) const noexcept {
        Cursor cursor{};
        const auto block = std::lower_bound(
            blocks_.begin(), blocks_.end(), sequence,
            [](const std::vector<CompactFastTickV1>& candidate,
               std::int64_t value) noexcept {
                return candidate.back().business_sequence.value < value;
            });
        if (block == blocks_.end()) {
            cursor.block = blocks_.size();
            return cursor;
        }
        cursor.block = static_cast<std::size_t>(block - blocks_.begin());
        const auto row = std::lower_bound(
            block->begin(), block->end(), sequence,
            [](const CompactFastTickV1& candidate,
               std::int64_t value) noexcept {
                return candidate.business_sequence.value < value;
            });
        cursor.row = static_cast<std::size_t>(row - block->begin());
        return cursor;
    }

    [[nodiscard]] Cursor UpperBound(std::int64_t sequence) const noexcept {
        Cursor cursor{};
        const auto block = std::lower_bound(
            blocks_.begin(), blocks_.end(), sequence,
            [](const std::vector<CompactFastTickV1>& candidate,
               std::int64_t value) noexcept {
                return candidate.back().business_sequence.value <= value;
            });
        if (block == blocks_.end()) {
            cursor.block = blocks_.size();
            cursor.last_sequence = sequence;
            return cursor;
        }
        cursor.block = static_cast<std::size_t>(block - blocks_.begin());
        const auto row = std::upper_bound(
            block->begin(), block->end(), sequence,
            [](std::int64_t value,
               const CompactFastTickV1& candidate) noexcept {
                return value < candidate.business_sequence.value;
            });
        cursor.row = static_cast<std::size_t>(row - block->begin());
        cursor.last_sequence = sequence;
        Normalize(&cursor);
        return cursor;
    }

    [[nodiscard]] std::optional<std::int64_t> Predecessor(
        std::int64_t sequence) const noexcept {
        Cursor cursor = LowerBound(sequence);
        if (cursor.block == blocks_.size()) {
            if (blocks_.empty()) {
                return std::nullopt;
            }
            return blocks_.back().back().business_sequence.value;
        }
        if (cursor.row != 0U) {
            return blocks_[cursor.block][cursor.row - 1U]
                .business_sequence.value;
        }
        if (cursor.block == 0U) {
            return std::nullopt;
        }
        return blocks_[cursor.block - 1U].back()
            .business_sequence.value;
    }

    [[nodiscard]] bool AtEnd(const Cursor& cursor) const noexcept {
        return cursor.block >= blocks_.size();
    }

    [[nodiscard]] bool ReadNext(
        Cursor* cursor,
        CompactFastTickV1* output) const noexcept {
        if (cursor == nullptr || output == nullptr) {
            return false;
        }
        Normalize(cursor);
        if (AtEnd(*cursor)) {
            return false;
        }
        *output = blocks_[cursor->block][cursor->row];
        cursor->last_sequence = output->business_sequence.value;
        ++cursor->row;
        Normalize(cursor);
        return true;
    }

private:
    void Normalize(Cursor* cursor) const noexcept {
        while (cursor->block < blocks_.size() &&
               cursor->row >= blocks_[cursor->block].size()) {
            ++cursor->block;
            cursor->row = 0U;
        }
    }

    std::vector<std::vector<CompactFastTickV1>> blocks_;
    std::size_t block_records_ = 0U;
    std::size_t maximum_records_ = 0U;
    std::size_t count_ = 0U;
    std::uint64_t generation_ = 0U;
    std::int64_t maximum_sequence_ = 0;
};

struct OrderIdentityV1 final {
    MarketV1 market = MarketV1::kUnknown;
    std::int32_t channel = 0;
    std::int64_t order_id = 0;

    [[nodiscard]] friend bool operator==(
        const OrderIdentityV1&,
        const OrderIdentityV1&) noexcept = default;
};

struct OrderIdentityHashV1 final {
    [[nodiscard]] std::size_t operator()(
        const OrderIdentityV1& key) const noexcept {
        std::uint64_t value = static_cast<std::uint64_t>(key.order_id);
        value ^= static_cast<std::uint64_t>(
                     static_cast<std::uint32_t>(key.channel))
                 << 32U;
        value ^= static_cast<std::uint64_t>(key.market) << 56U;
        value ^= value >> 30U;
        value *= UINT64_C(0xbf58476d1ce4e5b9);
        value ^= value >> 27U;
        value *= UINT64_C(0x94d049bb133111eb);
        value ^= value >> 31U;
        return static_cast<std::size_t>(value);
    }
};

struct ShenzhenOrderHiddenStateV1 final {
    bool terminal = false;
    bool finalization_emitted = false;
};

struct ShanghaiOrderHiddenStateV1 final {
    std::int64_t pre_add_active_trade_quantity = 0;
    std::int64_t minimum_execution_price_p6 = 0;
    std::int64_t maximum_execution_price_p6 = 0;
    bool execution_prices_seen = false;
    bool terminal = false;
    bool finalization_emitted = false;
};

using OrderHiddenStateV1 = std::variant<
    ShenzhenOrderHiddenStateV1,
    ShanghaiOrderHiddenStateV1>;

struct PendingOrderVersionV1 final {
    OrderIdentityV1 identity{};
    std::int64_t business_sequence = 0;
    OrderHiddenStateV1 hidden{};
};

[[nodiscard]] std::optional<OrderIdentityV1> RevisionIdentity(
    const OrderedDerivedEventV1& row) noexcept {
    if (const auto* revision =
            std::get_if<ShenzhenOrderRevisionEventV1>(&row.payload);
        revision != nullptr) {
        return OrderIdentityV1{
            MarketV1::kShenzhen,
            static_cast<std::int32_t>(revision->order.key.channel),
            revision->order.key.order_id};
    }
    if (const auto* revision =
            std::get_if<ShanghaiOrderRevisionEventV1>(&row.payload);
        revision != nullptr) {
        return OrderIdentityV1{
            MarketV1::kShanghai,
            revision->order.key.channel,
            revision->order.key.order_id};
    }
    return std::nullopt;
}

class OrderVersionIndexV1 final {
public:
    static constexpr std::uint32_t kInvalidBlockHandle =
        std::numeric_limits<std::uint32_t>::max();

    struct Ref final {
        std::int64_t business_sequence = 0;
        std::uint32_t block_handle = 0U;
        std::uint16_t row_offset = 0U;
        OrderHiddenStateV1 hidden{};
    };

    struct BlockOwner final {
        std::shared_ptr<const EventBlockDataV1> block;
        std::uint32_t reference_count = 0U;
        std::uint32_t next_free = kInvalidBlockHandle;
    };

    using Chain = std::vector<Ref>;

    explicit OrderVersionIndexV1(std::size_t maximum_orders) {
        chains_.reserve(maximum_orders);
        block_owners_.reserve(maximum_orders);
    }

    [[nodiscard]] bool LookupShenzhenBefore(
        const OrderIdentityV1& identity,
        std::int64_t dirty_sequence,
        ShenzhenOrderStateImageV1* output) const noexcept {
        if (output == nullptr || identity.market != MarketV1::kShenzhen) {
            return false;
        }
        const Ref* const ref = LookupRefBefore(identity, dirty_sequence);
        if (ref == nullptr || ref->block_handle >= block_owners_.size()) {
            return false;
        }
        const BlockOwner& owner = block_owners_[ref->block_handle];
        if (owner.block == nullptr ||
            ref->row_offset >= owner.block->rows.size()) {
            return false;
        }
        const auto* revision = std::get_if<ShenzhenOrderRevisionEventV1>(
            &owner.block->rows[ref->row_offset].payload);
        const auto* hidden =
            std::get_if<ShenzhenOrderHiddenStateV1>(&ref->hidden);
        if (revision == nullptr || hidden == nullptr) {
            return false;
        }
        output->snapshot = revision->order;
        output->terminal = hidden->terminal;
        output->finalization_emitted = hidden->finalization_emitted;
        return true;
    }

    [[nodiscard]] bool LookupShanghaiBefore(
        const OrderIdentityV1& identity,
        std::int64_t dirty_sequence,
        ShanghaiOrderStateImageV1* output) const noexcept {
        if (output == nullptr || identity.market != MarketV1::kShanghai) {
            return false;
        }
        const Ref* const ref = LookupRefBefore(identity, dirty_sequence);
        if (ref == nullptr || ref->block_handle >= block_owners_.size()) {
            return false;
        }
        const BlockOwner& owner = block_owners_[ref->block_handle];
        if (owner.block == nullptr ||
            ref->row_offset >= owner.block->rows.size()) {
            return false;
        }
        const auto* revision = std::get_if<ShanghaiOrderRevisionEventV1>(
            &owner.block->rows[ref->row_offset].payload);
        const auto* hidden =
            std::get_if<ShanghaiOrderHiddenStateV1>(&ref->hidden);
        if (revision == nullptr || hidden == nullptr) {
            return false;
        }
        output->snapshot = revision->order;
        output->pre_add_active_trade_quantity =
            hidden->pre_add_active_trade_quantity;
        output->minimum_execution_price_p6 =
            hidden->minimum_execution_price_p6;
        output->maximum_execution_price_p6 =
            hidden->maximum_execution_price_p6;
        output->execution_prices_seen = hidden->execution_prices_seen;
        output->terminal = hidden->terminal;
        output->finalization_emitted = hidden->finalization_emitted;
        return true;
    }

    template <typename Visitor>
    [[nodiscard]] bool VisitLatestShanghaiBefore(
        std::int32_t channel,
        std::int64_t dirty_sequence,
        Visitor&& visitor) const {
        for (const auto& [identity, chain] : chains_) {
            if (identity.market != MarketV1::kShanghai ||
                identity.channel != channel || chain.empty()) {
                continue;
            }
            ShanghaiOrderStateImageV1 image{};
            if (LookupShanghaiBefore(
                    identity, dirty_sequence, &image) &&
                !visitor(identity, image)) {
                return false;
            }
        }
        return true;
    }

    [[nodiscard]] bool HasVersionBefore(
        const OrderIdentityV1& identity,
        std::int64_t dirty_sequence) const noexcept {
        return LookupRefBefore(identity, dirty_sequence) != nullptr;
    }

    void ReservePatch(
        std::span<const PendingOrderVersionV1> versions,
        std::size_t block_count) {
        const std::size_t additional_block_owners =
            block_count > free_block_count_
                ? block_count - free_block_count_
                : 0U;
        if (additional_block_owners >
            std::numeric_limits<std::size_t>::max() -
                block_owners_.size()) {
            throw std::bad_alloc();
        }
        block_owners_.reserve(
            block_owners_.size() + additional_block_owners);
        std::unordered_map<
            OrderIdentityV1, std::size_t, OrderIdentityHashV1> additions;
        additions.reserve(versions.size());
        for (const PendingOrderVersionV1& version : versions) {
            ++additions[version.identity];
        }
        for (const auto& [identity, count] : additions) {
            Chain& chain = chains_[identity];
            if (count >
                std::numeric_limits<std::size_t>::max() - chain.size()) {
                throw std::bad_alloc();
            }
            chain.reserve(chain.size() + count);
        }
    }

    void Append(
        const std::vector<std::shared_ptr<EventBlockDataV1>>& blocks,
        std::span<const PendingOrderVersionV1> versions) {
        std::size_t version_index = 0U;
        for (const auto& mutable_block : blocks) {
            std::uint32_t handle = 0U;
            bool handle_created = false;
            for (std::size_t row_offset = 0U;
                 row_offset < mutable_block->rows.size(); ++row_offset) {
                const auto identity = RevisionIdentity(
                    mutable_block->rows[row_offset]);
                if (!identity.has_value()) {
                    continue;
                }
                if (version_index >= versions.size() ||
                    versions[version_index].identity != *identity ||
                    versions[version_index].business_sequence !=
                        mutable_block->rows[row_offset]
                            .order_key.business_sequence ||
                    row_offset >
                        std::numeric_limits<std::uint16_t>::max()) {
                    throw std::bad_alloc();
                }
                if (!handle_created) {
                    if (free_block_head_ != kInvalidBlockHandle) {
                        handle = free_block_head_;
                        BlockOwner& recycled = block_owners_[handle];
                        free_block_head_ = recycled.next_free;
                        --free_block_count_;
                        recycled.block = mutable_block;
                        recycled.reference_count = 0U;
                        recycled.next_free = kInvalidBlockHandle;
                    } else {
                        if (block_owners_.size() >=
                            static_cast<std::size_t>(
                                kInvalidBlockHandle)) {
                            throw std::bad_alloc();
                        }
                        handle = static_cast<std::uint32_t>(
                            block_owners_.size());
                        block_owners_.push_back(BlockOwner{
                            mutable_block,
                            0U,
                            kInvalidBlockHandle});
                    }
                    handle_created = true;
                }
                BlockOwner& owner = block_owners_[handle];
                if (owner.reference_count ==
                    std::numeric_limits<std::uint32_t>::max()) {
                    throw std::bad_alloc();
                }
                ++owner.reference_count;
                chains_[*identity].push_back(Ref{
                    versions[version_index].business_sequence,
                    handle,
                    static_cast<std::uint16_t>(row_offset),
                    versions[version_index].hidden});
                ++version_index;
            }
        }
        if (version_index != versions.size()) {
            throw std::bad_alloc();
        }
    }

    void TruncateSuffix(
        const OrderIdentityV1& identity,
        std::int64_t dirty_sequence) noexcept {
        const auto found = chains_.find(identity);
        if (found == chains_.end()) {
            return;
        }
        Chain& chain = found->second;
        const auto first_removed = std::lower_bound(
            chain.begin(), chain.end(), dirty_sequence,
            [](const Ref& ref, std::int64_t sequence) noexcept {
                return ref.business_sequence < sequence;
            });
        for (auto position = first_removed; position != chain.end();
             ++position) {
            if (position->block_handle >= block_owners_.size()) {
                continue;
            }
            BlockOwner& owner = block_owners_[position->block_handle];
            if (owner.reference_count != 0U) {
                --owner.reference_count;
                if (owner.reference_count == 0U) {
                    owner.block.reset();
                    owner.next_free = free_block_head_;
                    free_block_head_ = position->block_handle;
                    ++free_block_count_;
                }
            }
        }
        chain.erase(first_removed, chain.end());
    }

private:
    [[nodiscard]] const Ref* LookupRefBefore(
        const OrderIdentityV1& identity,
        std::int64_t dirty_sequence) const noexcept {
        const auto found = chains_.find(identity);
        if (found == chains_.end() || found->second.empty()) {
            return nullptr;
        }
        const Chain& chain = found->second;
        const auto after = std::lower_bound(
            chain.begin(), chain.end(), dirty_sequence,
            [](const Ref& ref, std::int64_t sequence) noexcept {
                return ref.business_sequence < sequence;
            });
        return after == chain.begin() ? nullptr : &*(after - 1);
    }

    std::unordered_map<
        OrderIdentityV1, Chain, OrderIdentityHashV1> chains_;
    std::vector<BlockOwner> block_owners_;
    std::uint32_t free_block_head_ = kInvalidBlockHandle;
    std::size_t free_block_count_ = 0U;
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
    struct MutableTailEntry final {
        CompactFastTickV1 input{};
        std::vector<OrderedDerivedEventV1> published_bundle;
    };

    struct EventSuffixBuilder final {
        explicit EventSuffixBuilder(std::size_t block_records_value)
            : block_records(block_records_value) {}

        void Clear() noexcept {
            blocks.clear();
            row_count = 0U;
        }

        void Append(std::span<const OrderedDerivedEventV1> rows) {
            for (const OrderedDerivedEventV1& row : rows) {
                if (blocks.empty() ||
                    blocks.back()->rows.size() >= block_records) {
                    auto block = std::make_shared<EventBlockDataV1>();
                    block->rows.reserve(block_records);
                    blocks.push_back(std::move(block));
                }
                blocks.back()->rows.push_back(row);
                ++row_count;
            }
        }

        std::size_t block_records = 0U;
        std::vector<std::shared_ptr<EventBlockDataV1>> blocks;
        std::uint64_t row_count = 0U;
    };

    using ProjectorStatePatchV1 = std::variant<
        ShenzhenOrderStateImageV1,
        ShanghaiOrderStateImageV1>;

    struct DirtyReplayContext final {
        DirtyReplayContext(
            BusinessSequenceV1 dirty,
            std::size_t event_block_records)
            : dirty_from(dirty), builder(event_block_records) {}

        BusinessSequenceV1 dirty_from{};
        std::shared_ptr<const EventStableRootV1> base_root;
        OrderedInputBlocksV1::Cursor cursor{};
        std::uint64_t observed_input_generation = 0U;
        std::int64_t previous_sequence = 0;
        std::unique_ptr<ShanghaiOrderEventAggregatorV1> shanghai;
        std::unique_ptr<ShenzhenOrderEventProjectorV1> shenzhen;
        EventSuffixBuilder builder;
        std::vector<PendingOrderVersionV1> versions;
        std::unordered_set<OrderIdentityV1, OrderIdentityHashV1>
            imported_orders;
        std::unordered_set<OrderIdentityV1, OrderIdentityHashV1>
            changed_orders;
        std::unordered_set<OrderIdentityV1, OrderIdentityHashV1>
            old_suffix_orders;
        std::vector<ProjectorStatePatchV1> final_states;
        std::deque<MutableTailEntry> replacement_tail;
        bool initialized = false;
        bool restart_required = false;
        bool dirty_was_in_tail = false;
    };

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
        std::vector<CompactFastTickV1> pending_live;
        std::deque<MutableTailEntry> mutable_tail;
        std::unique_ptr<DirtyReplayContext> repair;
        std::vector<OrderedDerivedEventV1> row_scratch;
        std::vector<OrderedDerivedEventV1> batch_row_scratch;
        std::vector<ShanghaiOrderEventV1> shanghai_scratch;
        std::vector<ShenzhenOrderEventV1> shenzhen_scratch;
        std::vector<PendingOrderVersionV1> version_scratch;
        std::vector<std::shared_ptr<EventBlockDataV1>> block_scratch;
    };

    struct WorkingState final {
        std::map<std::int32_t, std::unique_ptr<ChannelState>> channels;
        std::size_t input_count = 0U;
    };

    struct InstrumentState final {
        std::unique_ptr<WorkingState> working;
        std::unique_ptr<OrderVersionIndexV1> version_index;
        std::atomic<std::shared_ptr<const EventStableRootV1>> root;
        // The routed Event worker is the production writer. This gate also
        // protects direct API/test callers and the exceptional cold rebuild
        // path without putting a mutex on the normal path.
        std::atomic_flag writer = ATOMIC_FLAG_INIT;
        mutable std::mutex changes_mutex;
        std::vector<EventMutationV1> changes;
        std::uint64_t next_transaction_id = 1U;
        std::atomic<EventRepairStateV1> repair_state{
            EventRepairStateV1::kLive};
        std::atomic<std::uint64_t> repair_through{0U};
        std::atomic<bool> cold_rebuild_required{false};
        std::size_t next_repair_channel = 0U;
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
        std::span<const OrderedDerivedEventV1> rows,
        std::vector<std::shared_ptr<EventBlockDataV1>>*
            created_blocks = nullptr) {
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
                SealEventBlockMetadata(block.get());
                if (created_blocks != nullptr) {
                    created_blocks->push_back(block);
                }
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
        std::uint64_t included_change_sequence,
        std::vector<std::shared_ptr<EventBlockDataV1>>*
            created_blocks = nullptr) {
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
            SealEventBlockMetadata(block.get());
            if (created_blocks != nullptr) {
                created_blocks->push_back(block);
            }
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
            if (channel->shanghai->SetPreviousBusinessSequence(
                    tick.business_sequence.channel, 0) !=
                ShanghaiOrderAggregatorConsumeErrorV1::kNone) {
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
            if (channel->shenzhen->SetPreviousBusinessSequence(
                    static_cast<std::uint32_t>(
                        tick.business_sequence.channel),
                    0) != ShenzhenOrderProjectorConsumeErrorV1::kNone) {
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
        std::vector<OrderedDerivedEventV1>* rows,
        ShanghaiOrderEventAggregatorV1* shanghai_override = nullptr,
        ShenzhenOrderEventProjectorV1* shenzhen_override = nullptr) {
        rows->clear();
        auto append_payload = [&tick, rows](
                                  DerivedEventPayloadV1 payload) {
            const DerivedEventKindV1 kind = PayloadKind(payload);
            const std::int64_t affected = AffectedOrderId(payload);
            OrderedDerivedEventV1 row{};
            row.uid.instrument_id = tick.instrument_id;
            row.uid.channel = tick.business_sequence.channel;
            row.uid.business_sequence = tick.business_sequence.value;
            row.uid.kind = kind;
            row.uid.affected_order_id = affected;
            // Both exchange projectors emit at most one row of a given kind
            // for an affected order from one source message. Shanghai END
            // emits many revisions, but every order key is distinct.
            row.uid.occurrence = 0U;
            row.order_key.channel = tick.business_sequence.channel;
            row.order_key.business_sequence =
                tick.business_sequence.value;
            row.order_key.source_event_ordinal = 0U;
            row.order_key.derived_event_ordinal =
                static_cast<std::uint32_t>(rows->size());
            row.order_key.affected_order_id = affected;
            row.payload = std::move(payload);
            row.source_arrival_id = tick.arrival_id;
            rows->push_back(std::move(row));
        };
        if (channel->market == MarketV1::kShanghai) {
            ShanghaiOrderEventAggregatorV1* const projector =
                shanghai_override == nullptr
                    ? channel->shanghai.get()
                    : shanghai_override;
            channel->shanghai_scratch.clear();
            const ShanghaiOrderAggregatorConsumeErrorV1 error =
                projector->ConsumeBusinessOrdered(
                    ShanghaiInput(tick), &channel->shanghai_scratch);
            if (error != ShanghaiOrderAggregatorConsumeErrorV1::kNone) {
                return error ==
                               ShanghaiOrderAggregatorConsumeErrorV1::
                                   kResourceExhausted
                           ? OrderedEventHistoryErrorV1::
                                 kResourceExhausted
                           : OrderedEventHistoryErrorV1::kCoreFailed;
            }
            rows->reserve(channel->shanghai_scratch.size());
            for (ShanghaiOrderEventV1& event :
                 channel->shanghai_scratch) {
                append_payload(std::visit(
                    [](auto&& value) -> DerivedEventPayloadV1 {
                        return DerivedEventPayloadV1(
                            std::forward<decltype(value)>(value));
                    },
                    std::move(event)));
            }
        } else {
            ShenzhenOrderEventProjectorV1* const projector =
                shenzhen_override == nullptr
                    ? channel->shenzhen.get()
                    : shenzhen_override;
            channel->shenzhen_scratch.clear();
            const ShenzhenOrderProjectorConsumeErrorV1 error =
                projector->ConsumeBusinessOrdered(
                    ShenzhenInput(tick), &channel->shenzhen_scratch);
            if (error != ShenzhenOrderProjectorConsumeErrorV1::kNone) {
                return error ==
                               ShenzhenOrderProjectorConsumeErrorV1::
                                   kResourceExhausted
                           ? OrderedEventHistoryErrorV1::
                                 kResourceExhausted
                           : OrderedEventHistoryErrorV1::kCoreFailed;
            }
            rows->reserve(channel->shenzhen_scratch.size());
            for (ShenzhenOrderEventV1& event :
                 channel->shenzhen_scratch) {
                append_payload(std::visit(
                    [](auto&& value) -> DerivedEventPayloadV1 {
                        return DerivedEventPayloadV1(
                            std::forward<decltype(value)>(value));
                    },
                    std::move(event)));
            }
        }
        return OrderedEventHistoryErrorV1::kNone;
    }

    [[nodiscard]] OrderedEventHistoryErrorV1 CaptureVersions(
        ShanghaiOrderEventAggregatorV1* shanghai,
        ShenzhenOrderEventProjectorV1* shenzhen,
        std::span<const OrderedDerivedEventV1> rows,
        std::vector<PendingOrderVersionV1>* output) const {
        for (const OrderedDerivedEventV1& row : rows) {
            const auto identity = RevisionIdentity(row);
            if (!identity.has_value()) {
                continue;
            }
            PendingOrderVersionV1 pending{};
            pending.identity = *identity;
            pending.business_sequence = row.order_key.business_sequence;
            if (identity->market == MarketV1::kShenzhen) {
                if (shenzhen == nullptr) {
                    return OrderedEventHistoryErrorV1::kCoreFailed;
                }
                const auto* revision =
                    std::get_if<ShenzhenOrderRevisionEventV1>(
                        &row.payload);
                ShenzhenOrderStateImageV1 image{};
                if (revision == nullptr ||
                    shenzhen->GetOrderState(
                        revision->order.key, &image) !=
                        ShenzhenOrderProjectorQueryErrorV1::kNone) {
                    return OrderedEventHistoryErrorV1::kCoreFailed;
                }
                pending.hidden = ShenzhenOrderHiddenStateV1{
                    image.terminal, image.finalization_emitted};
            } else {
                if (shanghai == nullptr) {
                    return OrderedEventHistoryErrorV1::kCoreFailed;
                }
                const auto* revision =
                    std::get_if<ShanghaiOrderRevisionEventV1>(
                        &row.payload);
                ShanghaiOrderStateImageV1 image{};
                if (revision == nullptr ||
                    shanghai->GetOrderState(
                        revision->order.key, &image) !=
                        ShanghaiOrderAggregatorQueryErrorV1::kNone) {
                    return OrderedEventHistoryErrorV1::kCoreFailed;
                }
                pending.hidden = ShanghaiOrderHiddenStateV1{
                    image.pre_add_active_trade_quantity,
                    image.minimum_execution_price_p6,
                    image.maximum_execution_price_p6,
                    image.execution_prices_seen,
                    image.terminal,
                    image.finalization_emitted};
            }
            output->push_back(std::move(pending));
        }
        return OrderedEventHistoryErrorV1::kNone;
    }

    void AppendMutableTail(
        ChannelState* channel,
        const CompactFastTickV1& tick,
        std::span<const OrderedDerivedEventV1> rows) {
        MutableTailEntry entry{};
        entry.input = tick;
        entry.published_bundle.assign(rows.begin(), rows.end());
        channel->mutable_tail.push_back(std::move(entry));
        while (channel->mutable_tail.size() >
               config_.mutable_tail_records) {
            channel->mutable_tail.pop_front();
        }
    }

    void CollectOldSuffixOrders(
        const EventStableRootV1& root,
        std::int32_t channel_id,
        std::int64_t dirty_sequence,
        std::unordered_set<OrderIdentityV1, OrderIdentityHashV1>*
            output) const {
        const auto position = std::lower_bound(
            root.impl_->channels.begin(), root.impl_->channels.end(),
            channel_id,
            [](const EventChannelDataV1& channel,
               std::int32_t value) noexcept {
                return channel.channel < value;
            });
        if (position == root.impl_->channels.end() ||
            position->channel != channel_id) {
            return;
        }
        for (const EventBlockDataV1* block = position->tail.get();
             block != nullptr; block = block->previous.get()) {
            if (!block->rows.empty() &&
                block->last_key.business_sequence < dirty_sequence) {
                break;
            }
            for (const OrderedDerivedEventV1& row : block->rows) {
                if (row.order_key.business_sequence < dirty_sequence) {
                    continue;
                }
                const auto identity = RevisionIdentity(row);
                if (identity.has_value()) {
                    output->insert(*identity);
                }
            }
        }
    }

    [[nodiscard]] std::shared_ptr<const EventStableRootV1>
    BuildRootReplacingChannelSuffix(
        const EventStableRootV1& current,
        std::int32_t channel_id,
        std::int64_t dirty_sequence,
        EventSuffixBuilder* suffix,
        std::uint64_t included_change_sequence) {
        auto next = std::make_unique<EventStableRootV1::Impl>();
        next->instrument_id = current.impl_->instrument_id;
        next->included_change_sequence = included_change_sequence;
        next->row_count = current.impl_->row_count;
        next->channels = current.impl_->channels;

        auto position = std::lower_bound(
            next->channels.begin(), next->channels.end(), channel_id,
            [](const EventChannelDataV1& channel,
               std::int32_t value) noexcept {
                return channel.channel < value;
            });
        const std::uint64_t old_channel_rows =
            position != next->channels.end() &&
                    position->channel == channel_id
                ? position->row_count
                : 0U;
        std::shared_ptr<const EventBlockDataV1> kept_tail;
        if (old_channel_rows != 0U) {
            std::shared_ptr<const EventBlockDataV1> cursor =
                position->tail;
            while (cursor != nullptr) {
                if (cursor->last_key.business_sequence < dirty_sequence) {
                    kept_tail = std::move(cursor);
                    break;
                }
                if (cursor->first_key.business_sequence < dirty_sequence) {
                    auto prefix = std::make_shared<EventBlockDataV1>();
                    const auto end = std::lower_bound(
                        cursor->rows.begin(), cursor->rows.end(),
                        dirty_sequence,
                        [](const OrderedDerivedEventV1& row,
                           std::int64_t sequence) noexcept {
                            return row.order_key.business_sequence <
                                   sequence;
                        });
                    prefix->rows.assign(cursor->rows.begin(), end);
                    prefix->previous = cursor->previous;
                    SealEventBlockMetadata(prefix.get());
                    kept_tail = std::move(prefix);
                    break;
                }
                cursor = cursor->previous;
            }
        } else {
            position = next->channels.insert(
                position,
                EventChannelDataV1{channel_id, 0U, nullptr});
        }

        std::shared_ptr<const EventBlockDataV1> tail = kept_tail;
        for (const auto& block : suffix->blocks) {
            block->previous = std::move(tail);
            SealEventBlockMetadata(block.get());
            tail = block;
        }
        const std::uint64_t prefix_rows = kept_tail == nullptr
            ? 0U
            : kept_tail->cumulative_row_count;
        const std::uint64_t replacement_rows = suffix->row_count;
        if (prefix_rows >
                std::numeric_limits<std::uint64_t>::max() -
                    replacement_rows ||
            next->row_count < old_channel_rows) {
            throw std::bad_alloc();
        }
        const std::uint64_t new_channel_rows =
            prefix_rows + replacement_rows;
        next->row_count =
            next->row_count - old_channel_rows + new_channel_rows;
        position->row_count = new_channel_rows;
        position->tail = std::move(tail);
        if (position->row_count == 0U) {
            next->channels.erase(position);
        }
        return std::shared_ptr<const EventStableRootV1>(
            new EventStableRootV1(std::move(next)));
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

    [[nodiscard]] OrderedEventHistoryErrorV1 StageSuffixChanges(
        InstrumentState* state,
        std::int32_t channel,
        std::int64_t dirty_sequence,
        const EventSuffixBuilder& suffix,
        std::vector<EventMutationV1>* staged,
        std::uint64_t* final_sequence,
        std::uint64_t* transaction_id) {
        std::lock_guard<std::mutex> lock(state->changes_mutex);
        if (suffix.row_count > static_cast<std::uint64_t>(
                                   std::numeric_limits<std::size_t>::max())) {
            return OrderedEventHistoryErrorV1::kChangeCapacity;
        }
        const std::size_t row_count =
            static_cast<std::size_t>(suffix.row_count);
        const std::size_t chunk_records =
            config_.cdc_range_chunk_records;
        const std::size_t chunks = row_count / chunk_records +
            (row_count % chunk_records == 0U ? 0U : 1U);
        if (chunks > std::numeric_limits<std::size_t>::max() - 2U) {
            return OrderedEventHistoryErrorV1::kChangeCapacity;
        }
        const std::size_t mutation_count = chunks + 2U;
        if (state->changes.size() >
                config_.maximum_change_records_per_instrument ||
            mutation_count >
                config_.maximum_change_records_per_instrument -
                    state->changes.size() ||
            !ChangeSequenceCanAppend(
                state->changes.size(), mutation_count) ||
            state->next_transaction_id ==
                std::numeric_limits<std::uint64_t>::max()) {
            return OrderedEventHistoryErrorV1::kChangeCapacity;
        }

        staged->clear();
        staged->reserve(mutation_count);
        const std::uint64_t transaction = state->next_transaction_id;
        EventMutationV1 begin{};
        begin.change_sequence = state->changes.size() + 1U;
        begin.transaction_id = transaction;
        begin.kind = EventMutationKindV1::kRangeReplaceBegin;
        begin.range_scope = EventRangeReplaceScopeV1::kChannelSuffix;
        begin.range_channel = channel;
        begin.range_begin_business_sequence = dirty_sequence;
        begin.replace_entire_instrument = false;
        staged->push_back(std::move(begin));

        EventMutationV1 chunk{};
        auto start_chunk = [&]() {
            chunk = {};
            chunk.change_sequence =
                state->changes.size() + staged->size() + 1U;
            chunk.transaction_id = transaction;
            chunk.kind = EventMutationKindV1::kRangeReplaceChunk;
            chunk.range_scope =
                EventRangeReplaceScopeV1::kChannelSuffix;
            chunk.range_channel = channel;
            chunk.range_begin_business_sequence = dirty_sequence;
            chunk.replacement_rows.reserve(chunk_records);
        };
        if (row_count != 0U) {
            start_chunk();
        }
        for (const auto& block : suffix.blocks) {
            for (const OrderedDerivedEventV1& row : block->rows) {
                if (chunk.replacement_rows.size() >= chunk_records) {
                    staged->push_back(std::move(chunk));
                    start_chunk();
                }
                chunk.replacement_rows.push_back(row);
            }
        }
        if (row_count != 0U) {
            staged->push_back(std::move(chunk));
        }
        EventMutationV1 commit{};
        commit.change_sequence =
            state->changes.size() + staged->size() + 1U;
        commit.transaction_id = transaction;
        commit.kind = EventMutationKindV1::kRangeReplaceCommit;
        commit.range_scope = EventRangeReplaceScopeV1::kChannelSuffix;
        commit.range_channel = channel;
        commit.range_begin_business_sequence = dirty_sequence;
        staged->push_back(std::move(commit));
        if (staged->size() != mutation_count ||
            !ReserveForAppend(
                &state->changes,
                staged->size(),
                config_.maximum_change_records_per_instrument)) {
            return OrderedEventHistoryErrorV1::kChangeCapacity;
        }
        *final_sequence = staged->back().change_sequence;
        *transaction_id = transaction;
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

    void RegisterDirty(
        InstrumentState* state,
        ChannelState* channel,
        const CompactFastTickV1& tick) {
        BusinessSequenceV1 effective_dirty = tick.business_sequence;
        if (!channel->pending_live.empty() &&
            channel->pending_live.front().business_sequence.value <
                effective_dirty.value) {
            // These rows were journaled earlier in the same drain batch but
            // deliberately have not been projected yet. If a later arrival
            // exposes an inversion, replay must start at the first unpublished
            // pending row, not merely at the late row itself.
            effective_dirty =
                channel->pending_live.front().business_sequence;
        }
        channel->pending_live.clear();
        if (channel->repair == nullptr) {
            channel->repair = std::make_unique<DirtyReplayContext>(
                effective_dirty, config_.event_block_records);
            if (!channel->mutable_tail.empty()) {
                channel->repair->dirty_was_in_tail =
                    effective_dirty.value >=
                    channel->mutable_tail.front()
                        .input.business_sequence.value;
            }
        } else {
            DirtyReplayContext& repair = *channel->repair;
            if (effective_dirty.value <
                repair.dirty_from.value) {
                repair.dirty_from = effective_dirty;
            }
            // Any insertion (as opposed to a tail append) can be behind the
            // current replay cursor and can split journal blocks. Restart at
            // the earliest dirty key instead of trying to roll back overlay
            // state in place.
            repair.restart_required = true;
        }
        AtomicMaximum(&state->repair_through, tick.arrival_id);
        const EventRepairStateV1 current = state->repair_state.load(
            std::memory_order_acquire);
        if (current != EventRepairStateV1::kSourceConflict &&
            current != EventRepairStateV1::kUnrecoverable) {
            state->repair_state.store(
                EventRepairStateV1::kRepairRequired,
                std::memory_order_release);
        }
    }

    [[nodiscard]] bool HasDirtyChannel(
        const InstrumentState& state) const noexcept {
        if (state.working == nullptr) {
            return false;
        }
        for (const auto& [channel_id, channel] :
             state.working->channels) {
            static_cast<void>(channel_id);
            if (channel->repair != nullptr) {
                return true;
            }
        }
        return false;
    }

    [[nodiscard]] OrderedEventHistoryErrorV1 InitializeDirtyReplay(
        InstrumentState* state,
        ChannelState* channel) {
        DirtyReplayContext& repair = *channel->repair;
        repair.base_root = state->root.load(std::memory_order_acquire);
        if (repair.base_root == nullptr || state->version_index == nullptr) {
            return OrderedEventHistoryErrorV1::kResourceExhausted;
        }
        repair.builder.Clear();
        repair.versions.clear();
        repair.imported_orders.clear();
        repair.changed_orders.clear();
        repair.old_suffix_orders.clear();
        repair.final_states.clear();
        repair.replacement_tail.clear();
        repair.shanghai.reset();
        repair.shenzhen.reset();
        repair.dirty_was_in_tail =
            !channel->mutable_tail.empty() &&
            repair.dirty_from.value >=
                channel->mutable_tail.front()
                    .input.business_sequence.value;

        CollectOldSuffixOrders(
            *repair.base_root,
            repair.dirty_from.channel,
            repair.dirty_from.value,
            &repair.old_suffix_orders);
        std::size_t suffix_created_orders = 0U;
        for (const OrderIdentityV1& identity :
             repair.old_suffix_orders) {
            if (!state->version_index->HasVersionBefore(
                    identity, repair.dirty_from.value)) {
                ++suffix_created_orders;
            }
        }

        const std::size_t live_order_count =
            channel->market == MarketV1::kShanghai
                ? channel->shanghai->order_count()
                : channel->shenzhen->order_count();
        if (suffix_created_orders > live_order_count) {
            return OrderedEventHistoryErrorV1::kCoreFailed;
        }
        const std::size_t base_order_count =
            live_order_count - suffix_created_orders;
        if (channel->market == MarketV1::kShanghai) {
            if (ShanghaiOrderEventAggregatorV1::CreateSparseShadow(
                    ShanghaiOrderEventAggregatorConfigV1{
                        config_.trade_date,
                        config_.maximum_order_states_per_instrument},
                    base_order_count,
                    &repair.shanghai) !=
                ShanghaiOrderAggregatorCreateErrorV1::kNone) {
                return OrderedEventHistoryErrorV1::kResourceExhausted;
            }
        } else {
            if (ShenzhenOrderEventProjectorV1::CreateSparseShadow(
                    ShenzhenOrderEventProjectorConfigV1{
                        config_.trade_date,
                        config_.maximum_order_states_per_instrument},
                    base_order_count,
                    &repair.shenzhen) !=
                ShenzhenOrderProjectorCreateErrorV1::kNone) {
                return OrderedEventHistoryErrorV1::kResourceExhausted;
            }
        }

        repair.previous_sequence = channel->inputs.Predecessor(
            repair.dirty_from.value).value_or(0);
        if (channel->market == MarketV1::kShanghai) {
            if (repair.shanghai->SetPreviousBusinessSequence(
                    repair.dirty_from.channel,
                    repair.previous_sequence) !=
                ShanghaiOrderAggregatorConsumeErrorV1::kNone) {
                return OrderedEventHistoryErrorV1::kResourceExhausted;
            }
        } else if (repair.shenzhen->SetPreviousBusinessSequence(
                       static_cast<std::uint32_t>(
                           repair.dirty_from.channel),
                       repair.previous_sequence) !=
                   ShenzhenOrderProjectorConsumeErrorV1::kNone) {
            return OrderedEventHistoryErrorV1::kResourceExhausted;
        }
        repair.cursor = channel->inputs.LowerBound(
            repair.dirty_from.value);
        repair.observed_input_generation = channel->inputs.generation();
        for (const MutableTailEntry& entry : channel->mutable_tail) {
            if (entry.input.business_sequence.value >=
                repair.dirty_from.value) {
                break;
            }
            repair.replacement_tail.push_back(entry);
        }
        repair.imported_orders.reserve(
            repair.old_suffix_orders.size() + 8U);
        repair.changed_orders.reserve(
            repair.old_suffix_orders.size() + 8U);
        repair.versions.reserve(
            std::min<std::size_t>(
                config_.maximum_events_per_instrument,
                repair.old_suffix_orders.size() * 2U + 8U));
        repair.builder.blocks.reserve(
            repair.old_suffix_orders.size() /
                    config_.event_block_records +
                2U);
        repair.initialized = true;
        repair.restart_required = false;
        state->repair_state.store(
            EventRepairStateV1::kRebuilding,
            std::memory_order_release);
        return OrderedEventHistoryErrorV1::kNone;
    }

    [[nodiscard]] OrderedEventHistoryErrorV1 EnsureShadowOrderLoaded(
        InstrumentState* state,
        ChannelState* channel,
        OrderIdentityV1 identity) {
        DirtyReplayContext& repair = *channel->repair;
        if (identity.order_id <= 0 ||
            repair.imported_orders.contains(identity)) {
            return OrderedEventHistoryErrorV1::kNone;
        }
        const std::uint32_t instrument_id =
            repair.base_root->instrument_id();
        if (identity.market == MarketV1::kShenzhen) {
            ShenzhenOrderKeyV1 key{
                config_.trade_date,
                instrument_id,
                static_cast<std::uint32_t>(identity.channel),
                identity.order_id};
            ShenzhenOrderStateImageV1 image{};
            const auto shadow_query = repair.shenzhen->GetOrderState(
                key, &image);
            if (shadow_query ==
                ShenzhenOrderProjectorQueryErrorV1::kNotFound) {
                if (state->version_index->LookupShenzhenBefore(
                        identity, repair.dirty_from.value, &image) &&
                    repair.shenzhen->ImportExistingOrderState(image) !=
                        ShenzhenOrderProjectorConsumeErrorV1::kNone) {
                    return OrderedEventHistoryErrorV1::kResourceExhausted;
                }
            } else if (shadow_query !=
                       ShenzhenOrderProjectorQueryErrorV1::kNone) {
                return OrderedEventHistoryErrorV1::kCoreFailed;
            }
        } else {
            ShanghaiOrderKeyV1 key{
                config_.trade_date,
                instrument_id,
                identity.channel,
                identity.order_id};
            ShanghaiOrderStateImageV1 image{};
            const auto shadow_query = repair.shanghai->GetOrderState(
                key, &image);
            if (shadow_query ==
                ShanghaiOrderAggregatorQueryErrorV1::kNotFound) {
                if (state->version_index->LookupShanghaiBefore(
                        identity, repair.dirty_from.value, &image) &&
                    repair.shanghai->ImportExistingOrderState(image) !=
                        ShanghaiOrderAggregatorConsumeErrorV1::kNone) {
                    return OrderedEventHistoryErrorV1::kResourceExhausted;
                }
            } else if (shadow_query !=
                       ShanghaiOrderAggregatorQueryErrorV1::kNone) {
                return OrderedEventHistoryErrorV1::kCoreFailed;
            }
        }
        repair.imported_orders.insert(identity);
        return OrderedEventHistoryErrorV1::kNone;
    }

    [[nodiscard]] OrderedEventHistoryErrorV1
    ImportShanghaiEndCheckpoint(
        InstrumentState* state,
        ChannelState* channel) {
        DirtyReplayContext& repair = *channel->repair;
        bool ok = true;
        const bool visited =
            state->version_index->VisitLatestShanghaiBefore(
                repair.dirty_from.channel,
                repair.dirty_from.value,
                [&](const OrderIdentityV1& identity,
                    const ShanghaiOrderStateImageV1& image) {
                    if (repair.imported_orders.contains(identity)) {
                        return true;
                    }
                    const auto imported =
                        repair.shanghai->ImportExistingOrderState(image);
                    if (imported !=
                        ShanghaiOrderAggregatorConsumeErrorV1::kNone) {
                        ok = false;
                        return false;
                    }
                    repair.imported_orders.insert(identity);
                    return true;
                });
        return visited && ok
            ? OrderedEventHistoryErrorV1::kNone
            : OrderedEventHistoryErrorV1::kResourceExhausted;
    }

    [[nodiscard]] OrderedEventHistoryErrorV1 LoadTickDependencies(
        InstrumentState* state,
        ChannelState* channel,
        const CompactFastTickV1& tick) {
        const MarketV1 market = channel->market;
        const std::int32_t channel_id = tick.business_sequence.channel;
        const auto load = [&](std::int64_t order_id) {
            return EnsureShadowOrderLoaded(
                state,
                channel,
                OrderIdentityV1{market, channel_id, order_id});
        };
        if (tick.action == TickActionV1::kAdd ||
            tick.action == TickActionV1::kCancel) {
            return load(tick.primary_order_id);
        }
        if (tick.action == TickActionV1::kTrade) {
            if (tick.buy_order_id > 0) {
                const auto error = load(tick.buy_order_id);
                if (error != OrderedEventHistoryErrorV1::kNone) {
                    return error;
                }
            }
            if (tick.sell_order_id > 0) {
                return load(tick.sell_order_id);
            }
        }
        if (market == MarketV1::kShanghai &&
            tick.action == TickActionV1::kStatus &&
            (tick.validity_bitmap & kTickPhaseValidV1) != 0U &&
            tick.phase == TradingPhaseV1::kEnd) {
            return ImportShanghaiEndCheckpoint(state, channel);
        }
        return OrderedEventHistoryErrorV1::kNone;
    }

    [[nodiscard]] OrderedEventHistoryErrorV1 BuildFinalStatePatch(
        ChannelState* channel) {
        DirtyReplayContext& repair = *channel->repair;
        repair.final_states.clear();
        repair.final_states.reserve(repair.changed_orders.size());
        const std::uint32_t instrument_id =
            repair.base_root->instrument_id();
        for (const OrderIdentityV1& identity : repair.changed_orders) {
            if (identity.market == MarketV1::kShenzhen) {
                ShenzhenOrderStateImageV1 image{};
                const ShenzhenOrderKeyV1 key{
                    config_.trade_date,
                    instrument_id,
                    static_cast<std::uint32_t>(identity.channel),
                    identity.order_id};
                if (repair.shenzhen->GetOrderState(key, &image) !=
                    ShenzhenOrderProjectorQueryErrorV1::kNone) {
                    return OrderedEventHistoryErrorV1::kCoreFailed;
                }
                repair.final_states.emplace_back(std::move(image));
            } else {
                ShanghaiOrderStateImageV1 image{};
                const ShanghaiOrderKeyV1 key{
                    config_.trade_date,
                    instrument_id,
                    identity.channel,
                    identity.order_id};
                if (repair.shanghai->GetOrderState(key, &image) !=
                    ShanghaiOrderAggregatorQueryErrorV1::kNone) {
                    return OrderedEventHistoryErrorV1::kCoreFailed;
                }
                repair.final_states.emplace_back(std::move(image));
            }
        }
        return OrderedEventHistoryErrorV1::kNone;
    }

    [[nodiscard]] OrderedEventHistoryErrorV1 PublishLivePending(
        InstrumentState* state,
        ChannelState* channel,
        std::span<const CompactFastTickV1> ticks,
        std::uint64_t* published_rows) {
        if (ticks.empty()) {
            return OrderedEventHistoryErrorV1::kNone;
        }
        const auto current = state->root.load(std::memory_order_acquire);
        if (current == nullptr) {
            RegisterDirty(state, channel, ticks.front());
            return OrderedEventHistoryErrorV1::kResourceExhausted;
        }
        channel->batch_row_scratch.clear();
        channel->version_scratch.clear();
        for (const CompactFastTickV1& tick : ticks) {
            channel->row_scratch.clear();
            OrderedEventHistoryErrorV1 error = Consume(
                channel, tick, &channel->row_scratch);
            if (error != OrderedEventHistoryErrorV1::kNone) {
                RegisterDirty(state, channel, tick);
                return error;
            }
            error = CaptureVersions(
                channel->shanghai.get(),
                channel->shenzhen.get(),
                channel->row_scratch,
                &channel->version_scratch);
            if (error != OrderedEventHistoryErrorV1::kNone) {
                RegisterDirty(state, channel, tick);
                return error;
            }
            if (channel->row_scratch.size() >
                    config_.maximum_events_per_instrument ||
                channel->batch_row_scratch.size() >
                    config_.maximum_events_per_instrument -
                        channel->row_scratch.size()) {
                RegisterDirty(state, channel, tick);
                return OrderedEventHistoryErrorV1::kEventCapacity;
            }
            AppendMutableTail(channel, tick, channel->row_scratch);
            channel->batch_row_scratch.insert(
                channel->batch_row_scratch.end(),
                std::make_move_iterator(channel->row_scratch.begin()),
                std::make_move_iterator(channel->row_scratch.end()));
        }
        if (channel->batch_row_scratch.empty()) {
            return OrderedEventHistoryErrorV1::kNone;
        }
        if (channel->batch_row_scratch.size() >
                config_.maximum_events_per_instrument ||
            current->row_count() >
                config_.maximum_events_per_instrument -
                    channel->batch_row_scratch.size()) {
            RegisterDirty(state, channel, ticks.front());
            return OrderedEventHistoryErrorV1::kEventCapacity;
        }

        std::vector<EventMutationV1> staged;
        std::uint64_t final_sequence = 0U;
        OrderedEventHistoryErrorV1 error = StageInsertChanges(
            state,
            channel->batch_row_scratch,
            &staged,
            &final_sequence);
        if (error != OrderedEventHistoryErrorV1::kNone) {
            RegisterDirty(state, channel, ticks.front());
            return error;
        }
        channel->block_scratch.clear();
        const auto next = InsertIntoRoot(
            *current,
            channel->batch_row_scratch,
            final_sequence,
            &channel->block_scratch);
        state->version_index->ReservePatch(
            channel->version_scratch,
            channel->block_scratch.size());
        state->version_index->Append(
            channel->block_scratch,
            channel->version_scratch);
        CommitStagedAndPublish(state, &staged, next);
        *published_rows += channel->batch_row_scratch.size();
        return OrderedEventHistoryErrorV1::kNone;
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
    std::atomic<std::uint64_t> mutable_tail_repairs_{0U};
    std::atomic<std::uint64_t> deep_suffix_repairs_{0U};
    std::atomic<std::uint64_t> dirty_replay_inputs_{0U};
    std::atomic<std::uint64_t> cold_fast_rebuilds_{0U};
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
        config.mutable_tail_records == 0U ||
        config.maximum_change_records_per_instrument == 0U ||
        config.maximum_changes_per_read == 0U ||
        config.repair_replay_record_budget == 0U ||
        config.repair_cpu_budget_per_round <=
            std::chrono::nanoseconds::zero() ||
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
            state.version_index = std::make_unique<OrderVersionIndexV1>(
                impl->config_.maximum_order_states_per_instrument);
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
    if (impl_ == nullptr || route.ordinal != tick.ordinal ||
        route.instrument_id != tick.instrument_id ||
        route.worker != worker) {
        result.error = OrderedEventHistoryErrorV1::kInvalidInput;
        return result;
    }
    const EventBatchApplyResultV1 batch = ApplyBatch(
        worker, std::span<const CompactFastTickV1>(&tick, 1U));
    result.error = batch.error;
    result.published_rows = batch.published_rows;
    if (batch.error == OrderedEventHistoryErrorV1::kSourceConflict) {
        result.disposition = EventInputDispositionV1::kSourceConflict;
    } else if (batch.duplicate_inputs != 0U) {
        result.disposition = EventInputDispositionV1::kDuplicateIgnored;
    } else if (batch.dirty_channels != 0U ||
               batch.error != OrderedEventHistoryErrorV1::kNone) {
        result.disposition = EventInputDispositionV1::kRepairRegistered;
        result.dirty_from = tick.business_sequence;
    } else if (batch.published_rows != 0U) {
        result.disposition = EventInputDispositionV1::kPublished;
    } else {
        result.disposition = EventInputDispositionV1::kNoDerivedRows;
    }
    return result;
}

EventBatchApplyResultV1 OrderedEventHistoryV1::ApplyBatch(
    std::uint32_t worker,
    std::span<const CompactFastTickV1> ticks) noexcept {
    EventBatchApplyResultV1 result{};
    if (impl_ == nullptr || worker >= impl_->config_.worker_count) {
        result.error = OrderedEventHistoryErrorV1::kWrongWorker;
        return result;
    }
    if (ticks.empty()) {
        return result;
    }

    std::vector<std::size_t> ordinals;
    std::vector<std::pair<std::size_t, std::int32_t>> touched;
    try {
        ordinals.reserve(ticks.size());
        touched.reserve(ticks.size());
        for (const CompactFastTickV1& tick : ticks) {
            if (!ValidTickForEvent(tick) ||
                tick.trade_date != impl_->config_.trade_date ||
                tick.ordinal >= impl_->config_.instrument_count ||
                impl_->config_.event_routes[tick.ordinal] != worker) {
                result.error =
                    tick.ordinal < impl_->config_.instrument_count
                        ? OrderedEventHistoryErrorV1::kWrongWorker
                        : OrderedEventHistoryErrorV1::kInvalidInput;
                return result;
            }
            ordinals.push_back(tick.ordinal);
        }
        std::sort(ordinals.begin(), ordinals.end());
        ordinals.erase(
            std::unique(ordinals.begin(), ordinals.end()),
            ordinals.end());

        std::size_t locked = 0U;
        for (; locked < ordinals.size(); ++locked) {
            if (impl_->instruments_[ordinals[locked]].writer.test_and_set(
                    std::memory_order_acquire)) {
                break;
            }
        }
        if (locked != ordinals.size()) {
            for (std::size_t index = 0U; index < locked; ++index) {
                impl_->instruments_[ordinals[index]].writer.clear(
                    std::memory_order_release);
            }
            result.error = OrderedEventHistoryErrorV1::kNotLive;
            return result;
        }
        struct BatchWriterGuard final {
            Impl* impl = nullptr;
            std::vector<std::size_t>* ordinals = nullptr;
            std::span<const CompactFastTickV1> ticks;
            bool completed = false;
            ~BatchWriterGuard() {
                // ApplyBatch is journal-first but publication can still fail
                // exceptionally (allocation, capacity, or a source
                // conflict).  The Event worker has already dequeued the
                // whole span, so an early return must make every affected
                // instrument recoverable from FAST rather than strand an
                // unflushed pending_live prefix or silently lose the
                // unvisited suffix of the batch.
                if (!completed) {
                    for (const CompactFastTickV1& tick : ticks) {
                        if (tick.ordinal >=
                            impl->config_.instrument_count) {
                            continue;
                        }
                        Impl::InstrumentState& state =
                            impl->instruments_[tick.ordinal];
                        AtomicMaximum(
                            &state.repair_through, tick.arrival_id);
                        state.cold_rebuild_required.store(
                            true, std::memory_order_release);
                        RestoreRepairRequiredUnlessTerminal(
                            &state.repair_state);
                    }
                }
                for (std::size_t ordinal : *ordinals) {
                    impl->instruments_[ordinal].writer.clear(
                        std::memory_order_release);
                }
            }
        } writer_guard{impl_.get(), &ordinals, ticks, false};

        for (const CompactFastTickV1& tick : ticks) {
            Impl::InstrumentState& state =
                impl_->instruments_[tick.ordinal];
            Impl::ChannelState* channel = nullptr;
            result.error = impl_->EnsureChannel(
                state.working.get(), tick, &channel);
            if (result.error != OrderedEventHistoryErrorV1::kNone) {
                if (result.error ==
                    OrderedEventHistoryErrorV1::kResourceExhausted) {
                    state.cold_rebuild_required.store(
                        true, std::memory_order_release);
                    state.repair_state.store(
                        EventRepairStateV1::kRepairRequired,
                        std::memory_order_release);
                } else {
                    state.repair_state.store(
                        EventRepairStateV1::kUnrecoverable,
                        std::memory_order_release);
                }
                return result;
            }
            touched.emplace_back(
                tick.ordinal, tick.business_sequence.channel);
            const OrderedInputBlocksV1::InsertResult inserted =
                channel->inputs.Insert(tick);
            if (inserted == OrderedInputBlocksV1::InsertResult::kCapacity) {
                result.error = OrderedEventHistoryErrorV1::kInputCapacity;
                state.repair_state.store(
                    EventRepairStateV1::kUnrecoverable,
                    std::memory_order_release);
                return result;
            }
            if (inserted == OrderedInputBlocksV1::InsertResult::kDuplicate) {
                ++result.duplicate_inputs;
                impl_->duplicate_inputs_.fetch_add(
                    1U, std::memory_order_relaxed);
                continue;
            }
            if (inserted == OrderedInputBlocksV1::InsertResult::kConflict) {
                impl_->source_conflicts_.fetch_add(
                    1U, std::memory_order_relaxed);
                MarkSourceConflictUnlessUnrecoverable(&state.repair_state);
                result.error = OrderedEventHistoryErrorV1::kSourceConflict;
                return result;
            }
            if (state.working->input_count >=
                impl_->config_.maximum_inputs_per_instrument) {
                result.error = OrderedEventHistoryErrorV1::kInputCapacity;
                state.repair_state.store(
                    EventRepairStateV1::kUnrecoverable,
                    std::memory_order_release);
                return result;
            }
            ++state.working->input_count;
            ++result.accepted_inputs;
            AtomicMaximum(&state.repair_through, tick.arrival_id);

            if (inserted == OrderedInputBlocksV1::InsertResult::kLate) {
                impl_->late_inputs_.fetch_add(
                    1U, std::memory_order_relaxed);
                impl_->RegisterDirty(&state, channel, tick);
            } else if (
                channel->repair == nullptr &&
                !state.cold_rebuild_required.load(
                    std::memory_order_acquire)) {
                channel->pending_live.push_back(tick);
            }
        }

        std::sort(touched.begin(), touched.end());
        touched.erase(
            std::unique(touched.begin(), touched.end()),
            touched.end());
        for (const auto& [ordinal, channel_id] : touched) {
            Impl::InstrumentState& state = impl_->instruments_[ordinal];
            Impl::ChannelState* channel =
                state.working->channels.at(channel_id).get();
            if (channel->repair != nullptr ||
                state.cold_rebuild_required.load(
                    std::memory_order_acquire)) {
                channel->pending_live.clear();
                continue;
            }
            const std::uint64_t pending_count =
                static_cast<std::uint64_t>(
                    channel->pending_live.size());
            result.error = impl_->PublishLivePending(
                &state,
                channel,
                channel->pending_live,
                &result.published_rows);
            if (result.error != OrderedEventHistoryErrorV1::kNone) {
                channel->pending_live.clear();
                if (result.error !=
                    OrderedEventHistoryErrorV1::kResourceExhausted) {
                    state.repair_state.store(
                        EventRepairStateV1::kUnrecoverable,
                        std::memory_order_release);
                }
                return result;
            }
            result.published_inputs += pending_count;
            impl_->monotonic_fast_path_inputs_.fetch_add(
                pending_count, std::memory_order_relaxed);
            channel->pending_live.clear();
        }

        for (std::size_t ordinal : ordinals) {
            Impl::InstrumentState& state = impl_->instruments_[ordinal];
            if (state.cold_rebuild_required.load(
                    std::memory_order_acquire) ||
                impl_->HasDirtyChannel(state)) {
                state.repair_state.store(
                    EventRepairStateV1::kRepairRequired,
                    std::memory_order_release);
            } else if (state.repair_state.load(
                           std::memory_order_acquire) !=
                       EventRepairStateV1::kSourceConflict &&
                       state.repair_state.load(
                           std::memory_order_acquire) !=
                       EventRepairStateV1::kUnrecoverable) {
                state.repair_state.store(
                    EventRepairStateV1::kLive,
                    std::memory_order_release);
            }
        }
        for (const auto& [ordinal, channel_id] : touched) {
            if (impl_->instruments_[ordinal]
                    .working->channels.at(channel_id)->repair != nullptr) {
                ++result.dirty_channels;
            }
        }
        writer_guard.completed = true;
        return result;
    } catch (...) {
        result.error = OrderedEventHistoryErrorV1::kResourceExhausted;
        for (std::size_t ordinal : ordinals) {
            Impl::InstrumentState& state = impl_->instruments_[ordinal];
            state.cold_rebuild_required.store(
                true, std::memory_order_release);
            state.repair_state.store(
                EventRepairStateV1::kRepairRequired,
                std::memory_order_release);
        }
        return result;
    }
}

EventDirtyReplayResultV1 OrderedEventHistoryV1::AdvanceDirtyReplay(
    std::uint32_t worker,
    const EventRouteTokenV1& route,
    std::size_t record_budget,
    std::chrono::nanoseconds cpu_budget) noexcept {
    EventDirtyReplayResultV1 result{};
    if (impl_ == nullptr || route.ordinal >= impl_->config_.instrument_count ||
        route.instrument_id == 0U ||
        route.instrument_id !=
            static_cast<std::uint32_t>(route.ordinal + 1U) ||
        record_budget == 0U ||
        cpu_budget <= std::chrono::nanoseconds::zero()) {
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
    if (state.cold_rebuild_required.load(std::memory_order_acquire)) {
        result.cold_fallback_required = true;
        return result;
    }
    if (state.writer.test_and_set(std::memory_order_acquire)) {
        result.error = OrderedEventHistoryErrorV1::kNotLive;
        return result;
    }
    struct WriterGuard final {
        std::atomic_flag* writer = nullptr;
        ~WriterGuard() { writer->clear(std::memory_order_release); }
    } writer_guard{&state.writer};

    try {
        Impl::ChannelState* channel = nullptr;
        std::size_t dirty_count = 0U;
        for (const auto& [channel_id, candidate] :
             state.working->channels) {
            static_cast<void>(channel_id);
            if (candidate->repair != nullptr) {
                ++dirty_count;
            }
        }
        if (dirty_count == 0U) {
            if (state.repair_state.load(std::memory_order_acquire) !=
                    EventRepairStateV1::kSourceConflict &&
                state.repair_state.load(std::memory_order_acquire) !=
                    EventRepairStateV1::kUnrecoverable) {
                state.repair_state.store(
                    EventRepairStateV1::kLive,
                    std::memory_order_release);
            }
            return result;
        }
        const std::size_t selected =
            state.next_repair_channel % dirty_count;
        std::size_t observed = 0U;
        for (auto& [channel_id, candidate] : state.working->channels) {
            static_cast<void>(channel_id);
            if (candidate->repair == nullptr) {
                continue;
            }
            if (observed == selected) {
                channel = candidate.get();
                break;
            }
            ++observed;
        }
        if (channel == nullptr) {
            result.error = OrderedEventHistoryErrorV1::kCoreFailed;
            return result;
        }
        state.next_repair_channel = (selected + 1U) % dirty_count;
        Impl::DirtyReplayContext& repair = *channel->repair;
        if (!repair.initialized || repair.restart_required) {
            result.error = impl_->InitializeDirtyReplay(&state, channel);
            if (result.error != OrderedEventHistoryErrorV1::kNone) {
                return result;
            }
            result.worked = true;
        }

        const auto deadline = std::chrono::steady_clock::now() + cpu_budget;
        bool at_stable_end = false;
        while (result.replayed_inputs < record_budget &&
               std::chrono::steady_clock::now() < deadline) {
            CompactFastTickV1 tick{};
            if (!channel->inputs.ReadNext(&repair.cursor, &tick)) {
                const std::uint64_t generation =
                    channel->inputs.generation();
                if (generation != repair.observed_input_generation) {
                    repair.cursor = channel->inputs.UpperBound(
                        repair.previous_sequence);
                    repair.observed_input_generation = generation;
                    continue;
                }
                at_stable_end = true;
                break;
            }
            if (tick.market != channel->market ||
                tick.business_sequence.channel !=
                    repair.dirty_from.channel ||
                tick.business_sequence.value < repair.dirty_from.value ||
                tick.business_sequence.value <= repair.previous_sequence) {
                result.error = OrderedEventHistoryErrorV1::kCoreFailed;
                state.repair_state.store(
                    EventRepairStateV1::kUnrecoverable,
                    std::memory_order_release);
                return result;
            }
            result.error = impl_->LoadTickDependencies(
                &state, channel, tick);
            if (result.error != OrderedEventHistoryErrorV1::kNone) {
                return result;
            }
            channel->row_scratch.clear();
            result.error = impl_->Consume(
                channel,
                tick,
                &channel->row_scratch,
                repair.shanghai.get(),
                repair.shenzhen.get());
            if (result.error != OrderedEventHistoryErrorV1::kNone) {
                state.repair_state.store(
                    EventRepairStateV1::kUnrecoverable,
                    std::memory_order_release);
                return result;
            }
            result.error = impl_->CaptureVersions(
                repair.shanghai.get(),
                repair.shenzhen.get(),
                channel->row_scratch,
                &repair.versions);
            if (result.error != OrderedEventHistoryErrorV1::kNone) {
                state.repair_state.store(
                    EventRepairStateV1::kUnrecoverable,
                    std::memory_order_release);
                return result;
            }
            if (channel->row_scratch.size() >
                    impl_->config_.maximum_events_per_instrument ||
                repair.builder.row_count >
                    impl_->config_.maximum_events_per_instrument -
                        channel->row_scratch.size()) {
                result.error = OrderedEventHistoryErrorV1::kEventCapacity;
                state.repair_state.store(
                    EventRepairStateV1::kUnrecoverable,
                    std::memory_order_release);
                return result;
            }
            repair.builder.Append(channel->row_scratch);
            for (const OrderedDerivedEventV1& row :
                 channel->row_scratch) {
                const auto identity = RevisionIdentity(row);
                if (identity.has_value()) {
                    repair.changed_orders.insert(*identity);
                }
            }
            Impl::MutableTailEntry tail_entry{};
            tail_entry.input = tick;
            tail_entry.published_bundle = channel->row_scratch;
            repair.replacement_tail.push_back(std::move(tail_entry));
            while (repair.replacement_tail.size() >
                   impl_->config_.mutable_tail_records) {
                repair.replacement_tail.pop_front();
            }
            repair.previous_sequence = tick.business_sequence.value;
            ++result.replayed_inputs;
            result.worked = true;
        }
        impl_->dirty_replay_inputs_.fetch_add(
            result.replayed_inputs, std::memory_order_relaxed);
        if (!at_stable_end &&
            channel->inputs.AtEnd(repair.cursor) &&
            channel->inputs.generation() ==
                repair.observed_input_generation) {
            at_stable_end = true;
        }
        if (!at_stable_end) {
            state.repair_state.store(
                EventRepairStateV1::kCatchingUp,
                std::memory_order_release);
            return result;
        }

        // The new suffix normally revises every order revised by the old
        // suffix, but correctness must not depend on that projector detail.
        // If a newly inserted fact suppresses a later revision, restore that
        // order to its exact pre-dirty checkpoint instead of leaving the old
        // live suffix state behind.
        for (const OrderIdentityV1& identity :
             repair.old_suffix_orders) {
            result.error = impl_->EnsureShadowOrderLoaded(
                &state, channel, identity);
            if (result.error != OrderedEventHistoryErrorV1::kNone) {
                return result;
            }
            repair.changed_orders.insert(identity);
        }
        result.error = impl_->BuildFinalStatePatch(channel);
        if (result.error != OrderedEventHistoryErrorV1::kNone) {
            return result;
        }
        std::vector<EventMutationV1> staged;
        std::uint64_t final_change_sequence = 0U;
        std::uint64_t transaction_id = 0U;
        result.error = impl_->StageSuffixChanges(
            &state,
            repair.dirty_from.channel,
            repair.dirty_from.value,
            repair.builder,
            &staged,
            &final_change_sequence,
            &transaction_id);
        if (result.error != OrderedEventHistoryErrorV1::kNone) {
            if (result.error ==
                OrderedEventHistoryErrorV1::kChangeCapacity) {
                state.repair_state.store(
                    EventRepairStateV1::kUnrecoverable,
                    std::memory_order_release);
            }
            return result;
        }
        const auto current = state.root.load(std::memory_order_acquire);
        if (current == nullptr) {
            result.error = OrderedEventHistoryErrorV1::kResourceExhausted;
            return result;
        }
        const auto next = impl_->BuildRootReplacingChannelSuffix(
            *current,
            repair.dirty_from.channel,
            repair.dirty_from.value,
            &repair.builder,
            final_change_sequence);
        if (next->row_count() >
            impl_->config_.maximum_events_per_instrument) {
            result.error = OrderedEventHistoryErrorV1::kEventCapacity;
            state.repair_state.store(
                EventRepairStateV1::kUnrecoverable,
                std::memory_order_release);
            return result;
        }
        state.version_index->ReservePatch(
            repair.versions, repair.builder.blocks.size());

        std::size_t missing_live_orders = 0U;
        for (const Impl::ProjectorStatePatchV1& patch :
             repair.final_states) {
            const bool missing = std::visit(
                [&](const auto& image) {
                    using Image = std::decay_t<decltype(image)>;
                    if constexpr (std::is_same_v<
                                      Image,
                                      ShenzhenOrderStateImageV1>) {
                        ShenzhenOrderStateImageV1 ignored{};
                        return channel->shenzhen->GetOrderState(
                                   image.snapshot.key, &ignored) ==
                               ShenzhenOrderProjectorQueryErrorV1::
                                   kNotFound;
                    } else {
                        ShanghaiOrderStateImageV1 ignored{};
                        return channel->shanghai->GetOrderState(
                                   image.snapshot.key, &ignored) ==
                               ShanghaiOrderAggregatorQueryErrorV1::
                                   kNotFound;
                    }
                },
                patch);
            if (missing) {
                ++missing_live_orders;
            }
        }
        const std::size_t live_count =
            channel->market == MarketV1::kShanghai
                ? channel->shanghai->order_count()
                : channel->shenzhen->order_count();
        if (live_count >
                impl_->config_.maximum_order_states_per_instrument ||
            missing_live_orders >
                impl_->config_.maximum_order_states_per_instrument -
                    live_count) {
            result.error = OrderedEventHistoryErrorV1::kEventCapacity;
            state.repair_state.store(
                EventRepairStateV1::kUnrecoverable,
                std::memory_order_release);
            return result;
        }
        if (channel->inputs.generation() !=
                repair.observed_input_generation ||
            repair.restart_required) {
            repair.restart_required = true;
            state.repair_state.store(
                EventRepairStateV1::kRepairRequired,
                std::memory_order_release);
            return result;
        }

        for (const Impl::ProjectorStatePatchV1& patch :
             repair.final_states) {
            const bool replaced = std::visit(
                [&](const auto& image) {
                    using Image = std::decay_t<decltype(image)>;
                    if constexpr (std::is_same_v<
                                      Image,
                                      ShenzhenOrderStateImageV1>) {
                        return channel->shenzhen
                                   ->ReplaceOrInsertOrderState(image) ==
                               ShenzhenOrderProjectorConsumeErrorV1::
                                   kNone;
                    } else {
                        return channel->shanghai
                                   ->ReplaceOrInsertOrderState(image) ==
                               ShanghaiOrderAggregatorConsumeErrorV1::
                                   kNone;
                    }
                },
                patch);
            if (!replaced) {
                result.error = OrderedEventHistoryErrorV1::kCoreFailed;
                state.repair_state.store(
                    EventRepairStateV1::kUnrecoverable,
                    std::memory_order_release);
                return result;
            }
        }
        const std::int64_t final_business_sequence =
            channel->inputs.maximum_sequence();
        const bool sequence_updated =
            channel->market == MarketV1::kShanghai
                ? channel->shanghai->SetPreviousBusinessSequence(
                      repair.dirty_from.channel,
                      final_business_sequence) ==
                      ShanghaiOrderAggregatorConsumeErrorV1::kNone
                : channel->shenzhen->SetPreviousBusinessSequence(
                      static_cast<std::uint32_t>(
                          repair.dirty_from.channel),
                      final_business_sequence) ==
                      ShenzhenOrderProjectorConsumeErrorV1::kNone;
        if (!sequence_updated) {
            result.error = OrderedEventHistoryErrorV1::kCoreFailed;
            state.repair_state.store(
                EventRepairStateV1::kUnrecoverable,
                std::memory_order_release);
            return result;
        }
        for (const OrderIdentityV1& identity :
             repair.old_suffix_orders) {
            state.version_index->TruncateSuffix(
                identity, repair.dirty_from.value);
        }
        state.version_index->Append(
            repair.builder.blocks, repair.versions);
        ++state.next_transaction_id;
        impl_->CommitStagedAndPublish(&state, &staged, next);
        channel->mutable_tail = std::move(repair.replacement_tail);
        result.rebuilt_rows = repair.builder.row_count;
        result.range_transaction_id = transaction_id;
        result.committed = true;
        impl_->published_range_transactions_.fetch_add(
            1U, std::memory_order_relaxed);
        if (repair.dirty_was_in_tail) {
            impl_->mutable_tail_repairs_.fetch_add(
                1U, std::memory_order_relaxed);
        } else {
            impl_->deep_suffix_repairs_.fetch_add(
                1U, std::memory_order_relaxed);
        }
        channel->repair.reset();
        if (state.cold_rebuild_required.load(std::memory_order_acquire) ||
            impl_->HasDirtyChannel(state)) {
            state.repair_state.store(
                EventRepairStateV1::kRepairRequired,
                std::memory_order_release);
        } else {
            state.repair_state.store(
                EventRepairStateV1::kLive,
                std::memory_order_release);
        }
        return result;
    } catch (...) {
        result.error = OrderedEventHistoryErrorV1::kResourceExhausted;
        RestoreRepairRequiredUnlessTerminal(&state.repair_state);
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
    // Acknowledge the cold-gap request captured by this rebuild. Any queue
    // failure racing with the FAST copy sets the flag again and is therefore
    // not lost when this private root commits.
    state.cold_rebuild_required.store(false, std::memory_order_release);

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
        std::vector<PendingOrderVersionV1> all_versions;
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
                result.error = impl_->CaptureVersions(
                    channel->shanghai.get(),
                    channel->shenzhen.get(),
                    emitted,
                    &all_versions);
                if (result.error !=
                    OrderedEventHistoryErrorV1::kNone) {
                    state.repair_state.store(
                        EventRepairStateV1::kUnrecoverable,
                        std::memory_order_release);
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
                impl_->AppendMutableTail(channel, input, emitted);
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

        std::vector<std::shared_ptr<EventBlockDataV1>> rebuilt_blocks;
        const auto root = impl_->BuildRoot(
            route.instrument_id,
            final_sequence,
            all_rows,
            &rebuilt_blocks);
        auto rebuilt_versions = std::make_unique<OrderVersionIndexV1>(
            impl_->config_.maximum_order_states_per_instrument);
        rebuilt_versions->ReservePatch(
            all_versions, rebuilt_blocks.size());
        rebuilt_versions->Append(rebuilt_blocks, all_versions);
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
        state.version_index = std::move(rebuilt_versions);
        impl_->cold_fast_rebuilds_.fetch_add(
            1U, std::memory_order_relaxed);
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
            !state.cold_rebuild_required.load(
                std::memory_order_acquire) &&
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
            state.cold_rebuild_required.store(
                true, std::memory_order_release);
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
    state.cold_rebuild_required.store(true, std::memory_order_release);
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
    result.mutable_tail_repairs = impl_->mutable_tail_repairs_.load(
        std::memory_order_acquire);
    result.deep_suffix_repairs = impl_->deep_suffix_repairs_.load(
        std::memory_order_acquire);
    result.dirty_replay_inputs = impl_->dirty_replay_inputs_.load(
        std::memory_order_acquire);
    result.cold_fast_rebuilds = impl_->cold_fast_rebuilds_.load(
        std::memory_order_acquire);
    return result;
}

const OrderedEventHistoryConfigV1& OrderedEventHistoryV1::config()
    const noexcept {
    return impl_->config_;
}

}  // namespace l2flow::market
