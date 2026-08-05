#include "l2flow/market/ordered_event_history_v1.h"

#include <array>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <string_view>
#include <utility>
#include <vector>

namespace {

namespace market = l2flow::market;

constexpr std::uint32_t kTradeDate = 20260805U;
constexpr std::int32_t kChannel = 7;

bool Expect(bool condition, std::string_view message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << '\n';
    }
    return condition;
}

l2flow::common::Identity128 SessionId(std::byte tag) {
    l2flow::common::Identity128 result{};
    result[0U] = tag;
    result[15U] = std::byte{0x5aU};
    return result;
}

market::OrderedEventHistoryConfigV1 Config(std::byte tag) {
    market::OrderedEventHistoryConfigV1 config{};
    config.session_id = SessionId(tag);
    config.trade_date = kTradeDate;
    config.instrument_count = 1U;
    config.worker_count = 1U;
    config.maximum_order_states_per_instrument = 64U;
    config.maximum_inputs_per_instrument = 128U;
    config.maximum_events_per_instrument = 512U;
    config.input_block_records = 4U;
    config.event_block_records = 4U;
    config.cdc_range_chunk_records = 2U;
    config.mutable_tail_records = 8U;
    config.maximum_change_records_per_instrument = 1024U;
    config.maximum_changes_per_read = 128U;
    config.repair_replay_record_budget = 1U;
    config.repair_cpu_budget_per_round = std::chrono::milliseconds(10);
    config.event_routes = {0U};
    return config;
}

market::CompactFastTickV1 BaseTick(
    market::MarketV1 exchange,
    std::int64_t sequence,
    std::uint64_t arrival) {
    market::CompactFastTickV1 tick{};
    tick.instrument_id = 1U;
    tick.ordinal = 0U;
    tick.trade_date = kTradeDate;
    tick.market = exchange;
    tick.business_sequence = {kChannel, sequence};
    tick.arrival_id = arrival;
    tick.source_stream_id = 1U;
    tick.source_sequence = arrival;
    tick.vendor_sequence_id = static_cast<std::uint64_t>(sequence);
    tick.event_time_ns_since_midnight =
        34'200'000'000'000ULL + arrival;
    tick.event_time_unix_ns =
        1'785'859'200'000'000'000LL +
        static_cast<std::int64_t>(arrival);
    tick.recv_realtime_ns = static_cast<std::int64_t>(arrival);
    tick.recv_monotonic_ns = static_cast<std::int64_t>(arrival);
    tick.event_time_valid = true;
    tick.event_time_unix_ns_valid = true;
    return tick;
}

market::CompactFastTickV1 ShenzhenAdd(
    std::int64_t sequence,
    std::uint64_t arrival,
    std::int64_t quantity) {
    auto tick = BaseTick(market::MarketV1::kShenzhen, sequence, arrival);
    tick.source = market::FastTickSourceV1::kShenzhenTick;
    tick.kind = market::MarketEventKindV1::kShenzhenOrder;
    tick.action = market::TickActionV1::kAdd;
    tick.side = market::SideV1::kBuy;
    tick.order_type = market::OrderTypeV1::kLimit;
    tick.price_p6 = 10'000'000;
    tick.quantity_raw = quantity;
    tick.primary_order_id = sequence;
    tick.validity_bitmap =
        market::kTickPriceValidV1 |
        market::kTickQuantityValidV1 |
        market::kTickPrimaryOrderIdValidV1 |
        market::kTickSideValidV1 |
        market::kTickOrderTypeValidV1 |
        market::kTickExchangeTimeValidV1;
    return tick;
}

market::CompactFastTickV1 ShenzhenTrade(
    std::int64_t sequence,
    std::uint64_t arrival,
    std::int64_t buy_order,
    std::int64_t quantity) {
    auto tick = BaseTick(market::MarketV1::kShenzhen, sequence, arrival);
    tick.source = market::FastTickSourceV1::kShenzhenTick;
    tick.kind = market::MarketEventKindV1::kShenzhenTransaction;
    tick.action = market::TickActionV1::kTrade;
    tick.price_p6 = 10'100'000;
    tick.quantity_raw = quantity;
    tick.buy_order_id = buy_order;
    tick.validity_bitmap =
        market::kTickPriceValidV1 |
        market::kTickQuantityValidV1 |
        market::kTickBuyOrderIdValidV1 |
        market::kTickExchangeTimeValidV1;
    return tick;
}

market::CompactFastTickV1 ShenzhenCancel(
    std::int64_t sequence,
    std::uint64_t arrival,
    std::int64_t order,
    std::int64_t quantity) {
    auto tick = BaseTick(market::MarketV1::kShenzhen, sequence, arrival);
    tick.source = market::FastTickSourceV1::kShenzhenTick;
    tick.kind = market::MarketEventKindV1::kShenzhenTransaction;
    tick.action = market::TickActionV1::kCancel;
    tick.side = market::SideV1::kBuy;
    tick.quantity_raw = quantity;
    tick.primary_order_id = order;
    tick.buy_order_id = order;
    tick.validity_bitmap =
        market::kTickQuantityValidV1 |
        market::kTickPrimaryOrderIdValidV1 |
        market::kTickBuyOrderIdValidV1 |
        market::kTickSideValidV1 |
        market::kTickExchangeTimeValidV1;
    return tick;
}

market::CompactFastTickV1 ShanghaiTrade(
    std::int64_t sequence,
    std::uint64_t arrival,
    std::int64_t buy_order,
    std::int64_t sell_order,
    std::int64_t price,
    std::int64_t quantity) {
    auto tick = BaseTick(market::MarketV1::kShanghai, sequence, arrival);
    tick.source = market::FastTickSourceV1::kShanghaiTick;
    tick.kind = market::MarketEventKindV1::kShanghaiTick;
    tick.action = market::TickActionV1::kTrade;
    tick.aggressor = market::AggressorV1::kBuy;
    tick.phase = market::TradingPhaseV1::kContinuous;
    tick.price_p6 = price;
    tick.quantity_raw = quantity;
    tick.buy_order_id = buy_order;
    tick.sell_order_id = sell_order;
    tick.validity_bitmap =
        market::kTickPriceValidV1 |
        market::kTickQuantityValidV1 |
        market::kTickBuyOrderIdValidV1 |
        market::kTickSellOrderIdValidV1 |
        market::kTickAggressorValidV1 |
        market::kTickPhaseValidV1 |
        market::kTickExchangeTimeValidV1;
    return tick;
}

market::CompactFastTickV1 ShanghaiAdd(
    std::int64_t sequence,
    std::uint64_t arrival,
    std::int64_t order,
    std::int64_t quantity,
    std::int64_t matched) {
    auto tick = BaseTick(market::MarketV1::kShanghai, sequence, arrival);
    tick.source = market::FastTickSourceV1::kShanghaiTick;
    tick.kind = market::MarketEventKindV1::kShanghaiTick;
    tick.action = market::TickActionV1::kAdd;
    tick.side = market::SideV1::kBuy;
    tick.phase = market::TradingPhaseV1::kContinuous;
    tick.price_p6 = 10'500'000;
    tick.quantity_raw = quantity;
    tick.matched_quantity_raw = matched;
    tick.primary_order_id = order;
    tick.validity_bitmap =
        market::kTickPriceValidV1 |
        market::kTickQuantityValidV1 |
        market::kTickMatchedQuantityValidV1 |
        market::kTickPrimaryOrderIdValidV1 |
        market::kTickSideValidV1 |
        market::kTickPhaseValidV1 |
        market::kTickExchangeTimeValidV1;
    return tick;
}

market::CompactFastTickV1 ShanghaiEnd(
    std::int64_t sequence,
    std::uint64_t arrival) {
    auto tick = BaseTick(market::MarketV1::kShanghai, sequence, arrival);
    tick.source = market::FastTickSourceV1::kShanghaiTick;
    tick.kind = market::MarketEventKindV1::kShanghaiTick;
    tick.action = market::TickActionV1::kStatus;
    tick.phase = market::TradingPhaseV1::kEnd;
    tick.validity_bitmap =
        market::kTickPhaseValidV1 |
        market::kTickExchangeTimeValidV1;
    return tick;
}

bool DrainRepair(
    market::OrderedEventHistoryV1* history,
    const market::EventRouteTokenV1& route) {
    for (std::size_t round = 0U; round < 256U; ++round) {
        if (history->RepairState(1U) ==
            market::EventRepairStateV1::kLive) {
            return true;
        }
        const auto advanced = history->AdvanceDirtyReplay(
            0U, route, 1U, std::chrono::milliseconds(10));
        if (advanced.error !=
                market::OrderedEventHistoryErrorV1::kNone ||
            advanced.cold_fallback_required) {
            return false;
        }
    }
    return false;
}

void TestShenzhenCheckpointedSuffix(bool* ok) {
    std::unique_ptr<market::OrderedEventHistoryV1> history;
    *ok &= Expect(
        market::OrderedEventHistoryV1::Create(
            Config(std::byte{0x11U}), &history) ==
            market::OrderedEventHistoryCreateErrorV1::kNone,
        "create Shenzhen suffix history");
    if (history == nullptr) {
        return;
    }
    market::EventRouteTokenV1 route{};
    *ok &= Expect(
        history->ResolveRoute(0U, 1U, &route) ==
            market::OrderedEventHistoryErrorV1::kNone,
        "resolve Shenzhen suffix route");

    const auto add_100 = ShenzhenAdd(100, 1U, 10);
    const auto trade_102 = ShenzhenTrade(102, 2U, 100, 3);
    *ok &= Expect(
        history->ApplyLive(0U, route, add_100).error ==
                market::OrderedEventHistoryErrorV1::kNone &&
            history->ApplyLive(0U, route, trade_102).error ==
                market::OrderedEventHistoryErrorV1::kNone,
        "publish Shenzhen prefix and gapped trade");
    market::EventStableSnapshotV1 pinned{};
    *ok &= Expect(
        history->AcquireStable(1U, &pinned) ==
                market::OrderedEventHistoryErrorV1::kNone &&
            pinned.root != nullptr && pinned.root->row_count() == 3U,
        "pin Shenzhen root before late add");

    const auto late_101 = ShenzhenAdd(101, 3U, 5);
    const auto late_result = history->ApplyLive(0U, route, late_101);
    *ok &= Expect(
        late_result.disposition ==
                market::EventInputDispositionV1::kRepairRegistered &&
            history->RepairState(1U) !=
                market::EventRepairStateV1::kLive &&
            DrainRepair(history.get(), route),
        "repair Shenzhen suffix from the exact dirty key");

    market::EventStableSnapshotV1 repaired{};
    std::vector<market::OrderedDerivedEventV1> rows;
    std::vector<market::OrderedDerivedEventV1> old_rows;
    *ok &= Expect(
        pinned.root->CopyRows(&old_rows) && old_rows.size() == 3U &&
            history->AcquireStable(1U, &repaired) ==
                market::OrderedEventHistoryErrorV1::kNone &&
            repaired.root != nullptr && repaired.root != pinned.root &&
            repaired.root->CopyRows(&rows) && rows.size() == 4U &&
            rows[0U].order_key.business_sequence == 100 &&
            rows[1U].order_key.business_sequence == 101 &&
            rows[2U].order_key.business_sequence == 102 &&
            rows[3U].order_key.business_sequence == 102,
        "Shenzhen root shares the prefix and replaces only 101+");

    std::array<market::EventMutationV1, 8U> changes{};
    std::size_t written = 0U;
    auto cursor = pinned.next_changes;
    *ok &= Expect(
        history->ReadChanges(&cursor, changes, &written) ==
                market::OrderedEventHistoryErrorV1::kNone &&
            written >= 2U &&
            changes[0U].kind ==
                market::EventMutationKindV1::kRangeReplaceBegin &&
            changes[0U].range_scope ==
                market::EventRangeReplaceScopeV1::kChannelSuffix &&
            changes[0U].range_channel == kChannel &&
            changes[0U].range_begin_business_sequence == 101 &&
            changes[written - 1U].kind ==
                market::EventMutationKindV1::kRangeReplaceCommit,
        "Shenzhen CDC publishes an explicit channel suffix transaction");

    const auto cancel_103 = ShenzhenCancel(103, 4U, 100, 7);
    *ok &= Expect(
        history->ApplyLive(0U, route, cancel_103).error ==
            market::OrderedEventHistoryErrorV1::kNone,
        "live Shenzhen projector continues from repaired final state");
    market::EventStableSnapshotV1 final{};
    std::vector<market::OrderedDerivedEventV1> final_rows;
    bool finalized_order = false;
    if (history->AcquireStable(1U, &final) ==
            market::OrderedEventHistoryErrorV1::kNone &&
        final.root != nullptr && final.root->CopyRows(&final_rows)) {
        for (const auto& row : final_rows) {
            const auto* revision =
                std::get_if<market::ShenzhenOrderRevisionEventV1>(
                    &row.payload);
            if (revision != nullptr &&
                revision->order.key.order_id == 100 &&
                revision->source_anchor.native_event_sequence == 103) {
                finalized_order =
                    revision->operation ==
                        market::ShenzhenOrderDeltaOperationV1::kFinalize &&
                    revision->order.remaining_quantity == 0;
            }
        }
    }
    const auto stats = history->Stats();
    *ok &= Expect(
        finalized_order && stats.mutable_tail_repairs == 1U &&
            stats.dirty_replay_inputs == 2U &&
            stats.cold_fast_rebuilds == 0U,
        "Shenzhen repaired checkpoint remains exact on the next live cancel");
}

void TestShenzhenDeepSuffixCheckpoint(bool* ok) {
    auto config = Config(std::byte{0x16U});
    config.mutable_tail_records = 2U;
    std::unique_ptr<market::OrderedEventHistoryV1> history;
    *ok &= Expect(
        market::OrderedEventHistoryV1::Create(
            std::move(config), &history) ==
            market::OrderedEventHistoryCreateErrorV1::kNone,
        "create Shenzhen deep-suffix history");
    if (history == nullptr) {
        return;
    }
    market::EventRouteTokenV1 route{};
    static_cast<void>(history->ResolveRoute(0U, 1U, &route));
    *ok &= Expect(
        history->ApplyLive(
            0U, route, ShenzhenAdd(90, 1U, 10)).error ==
                market::OrderedEventHistoryErrorV1::kNone &&
            history->ApplyLive(
                0U, route, ShenzhenTrade(102, 2U, 90, 3)).error ==
                market::OrderedEventHistoryErrorV1::kNone,
        "publish checkpoint and distant suffix");
    for (std::int64_t sequence = 103; sequence <= 110; ++sequence) {
        *ok &= Expect(
            history->ApplyLive(
                0U,
                route,
                ShenzhenTrade(
                    sequence,
                    static_cast<std::uint64_t>(sequence - 99),
                    0,
                    1)).error ==
                market::OrderedEventHistoryErrorV1::kNone,
            "extend Shenzhen suffix beyond the mutable tail");
    }
    *ok &= Expect(
        history->ApplyLive(
            0U, route, ShenzhenAdd(101, 12U, 5)).disposition ==
                market::EventInputDispositionV1::kRepairRegistered &&
            DrainRepair(history.get(), route),
        "repair Shenzhen from an exact predecessor before the tail");
    *ok &= Expect(
        history->ApplyLive(
            0U, route, ShenzhenCancel(111, 13U, 90, 7)).error ==
            market::OrderedEventHistoryErrorV1::kNone,
        "continue from the deep-suffix final order state");

    market::EventStableSnapshotV1 stable{};
    std::vector<market::OrderedDerivedEventV1> rows;
    bool finalized = false;
    if (history->AcquireStable(1U, &stable) ==
            market::OrderedEventHistoryErrorV1::kNone &&
        stable.root != nullptr && stable.root->CopyRows(&rows)) {
        for (const auto& row : rows) {
            const auto* revision =
                std::get_if<market::ShenzhenOrderRevisionEventV1>(
                    &row.payload);
            if (revision != nullptr &&
                revision->order.key.order_id == 90 &&
                revision->source_anchor.native_event_sequence == 111) {
                finalized = revision->order.remaining_quantity == 0 &&
                    revision->operation ==
                        market::ShenzhenOrderDeltaOperationV1::kFinalize;
            }
        }
    }
    const auto stats = history->Stats();
    *ok &= Expect(
        finalized && stats.deep_suffix_repairs == 1U &&
            stats.mutable_tail_repairs == 0U &&
            stats.dirty_replay_inputs == 10U,
        "deep replay uses the pre-dirty checkpoint without a cold rebuild");
}

void TestShanghaiHiddenCheckpoint(bool* ok) {
    std::unique_ptr<market::OrderedEventHistoryV1> history;
    *ok &= Expect(
        market::OrderedEventHistoryV1::Create(
            Config(std::byte{0x22U}), &history) ==
            market::OrderedEventHistoryCreateErrorV1::kNone,
        "create Shanghai suffix history");
    if (history == nullptr) {
        return;
    }
    market::EventRouteTokenV1 route{};
    *ok &= Expect(
        history->ResolveRoute(0U, 1U, &route) ==
            market::OrderedEventHistoryErrorV1::kNone,
        "resolve Shanghai suffix route");

    const auto trade_100 = ShanghaiTrade(
        100, 1U, 10, 20, 10'000'000, 2);
    const auto add_102 = ShanghaiAdd(102, 2U, 10, 8, 2);
    *ok &= Expect(
        history->ApplyLive(0U, route, trade_100).error ==
                market::OrderedEventHistoryErrorV1::kNone &&
            history->ApplyLive(0U, route, add_102).error ==
                market::OrderedEventHistoryErrorV1::kNone,
        "publish Shanghai synthetic-order prefix and add");
    const auto late_101 = ShanghaiTrade(
        101, 3U, 10, 30, 12'000'000, 1);
    *ok &= Expect(
        history->ApplyLive(0U, route, late_101).disposition ==
                market::EventInputDispositionV1::kRepairRegistered &&
            DrainRepair(history.get(), route),
        "repair Shanghai suffix with sparse checkpoint state");

    const auto trade_103 = ShanghaiTrade(
        103, 4U, 10, 40, 11'000'000, 1);
    *ok &= Expect(
        history->ApplyLive(0U, route, trade_103).error ==
            market::OrderedEventHistoryErrorV1::kNone,
        "continue Shanghai live projection after suffix commit");
    market::EventStableSnapshotV1 stable{};
    std::vector<market::OrderedDerivedEventV1> rows;
    bool exact = false;
    if (history->AcquireStable(1U, &stable) ==
            market::OrderedEventHistoryErrorV1::kNone &&
        stable.root != nullptr && stable.root->CopyRows(&rows)) {
        for (const auto& row : rows) {
            const auto* revision =
                std::get_if<market::ShanghaiOrderRevisionEventV1>(
                    &row.payload);
            if (revision != nullptr &&
                revision->order.key.order_id == 10 &&
                revision->source_anchor.native_event_sequence == 103) {
                exact =
                    revision->order.observed_pre_add_trade_quantity == 3 &&
                    revision->order.execution_boundary_price_valid &&
                    revision->order.execution_boundary_price_p6 ==
                        12'000'000 &&
                    revision->order.remaining_quantity == 7 &&
                    (revision->order.quality_flags &
                     market::ShanghaiOrderQualityBitV1(
                         market::ShanghaiOrderQualityFlagV1::
                             kPrematchQuantityMismatch)) != 0U;
            }
        }
    }
    const auto stats = history->Stats();
    *ok &= Expect(
        exact && stats.mutable_tail_repairs == 1U &&
            stats.dirty_replay_inputs == 2U,
        "Shanghai replay restores hidden extrema and pre-add accumulator");
}

void TestShanghaiDeepSuffixCheckpoint(bool* ok) {
    auto config = Config(std::byte{0x26U});
    config.mutable_tail_records = 2U;
    std::unique_ptr<market::OrderedEventHistoryV1> history;
    *ok &= Expect(
        market::OrderedEventHistoryV1::Create(
            std::move(config), &history) ==
            market::OrderedEventHistoryCreateErrorV1::kNone,
        "create Shanghai deep-suffix history");
    if (history == nullptr) {
        return;
    }
    market::EventRouteTokenV1 route{};
    static_cast<void>(history->ResolveRoute(0U, 1U, &route));
    *ok &= Expect(
        history->ApplyLive(
            0U,
            route,
            ShanghaiTrade(90, 1U, 10, 20, 10'000'000, 2)).error ==
                market::OrderedEventHistoryErrorV1::kNone &&
            history->ApplyLive(
                0U, route, ShanghaiAdd(102, 2U, 10, 8, 2)).error ==
                market::OrderedEventHistoryErrorV1::kNone,
        "publish Shanghai hidden checkpoint and distant add");
    for (std::int64_t sequence = 103; sequence <= 110; ++sequence) {
        auto status = ShanghaiEnd(
            sequence, static_cast<std::uint64_t>(sequence - 99));
        status.phase = market::TradingPhaseV1::kContinuous;
        *ok &= Expect(
            history->ApplyLive(0U, route, status).error ==
                market::OrderedEventHistoryErrorV1::kNone,
            "extend Shanghai suffix beyond the mutable tail");
    }
    *ok &= Expect(
        history->ApplyLive(
            0U,
            route,
            ShanghaiTrade(101, 12U, 10, 30, 12'000'000, 1))
                .disposition ==
                market::EventInputDispositionV1::kRepairRegistered &&
            DrainRepair(history.get(), route),
        "repair Shanghai hidden state from before the mutable tail");
    *ok &= Expect(
        history->ApplyLive(
            0U,
            route,
            ShanghaiTrade(111, 13U, 10, 40, 11'000'000, 1)).error ==
            market::OrderedEventHistoryErrorV1::kNone,
        "continue Shanghai projection after a deep repair");

    market::EventStableSnapshotV1 stable{};
    std::vector<market::OrderedDerivedEventV1> rows;
    bool exact = false;
    if (history->AcquireStable(1U, &stable) ==
            market::OrderedEventHistoryErrorV1::kNone &&
        stable.root != nullptr && stable.root->CopyRows(&rows)) {
        for (const auto& row : rows) {
            const auto* revision =
                std::get_if<market::ShanghaiOrderRevisionEventV1>(
                    &row.payload);
            if (revision != nullptr &&
                revision->order.key.order_id == 10 &&
                revision->source_anchor.native_event_sequence == 111) {
                exact =
                    revision->order.observed_pre_add_trade_quantity == 3 &&
                    revision->order.execution_boundary_price_valid &&
                    revision->order.execution_boundary_price_p6 ==
                        12'000'000 &&
                    revision->order.remaining_quantity == 7;
            }
        }
    }
    const auto stats = history->Stats();
    *ok &= Expect(
        exact && stats.deep_suffix_repairs == 1U &&
            stats.mutable_tail_repairs == 0U &&
            stats.dirty_replay_inputs == 10U,
        "Shanghai deep replay restores every hidden checkpoint field");
}

void TestMicroBatchJournalsBeforeProjection(bool* ok) {
    std::unique_ptr<market::OrderedEventHistoryV1> history;
    *ok &= Expect(
        market::OrderedEventHistoryV1::Create(
            Config(std::byte{0x33U}), &history) ==
            market::OrderedEventHistoryCreateErrorV1::kNone,
        "create micro-batch history");
    if (history == nullptr) {
        return;
    }
    market::EventRouteTokenV1 route{};
    static_cast<void>(history->ResolveRoute(0U, 1U, &route));
    const std::array<market::CompactFastTickV1, 2U> batch{
        ShenzhenTrade(102, 1U, 101, 2),
        ShenzhenAdd(101, 2U, 5)};
    const auto staged = history->ApplyBatch(0U, batch);
    market::EventStableSnapshotV1 before{};
    *ok &= Expect(
        staged.error == market::OrderedEventHistoryErrorV1::kNone &&
            staged.accepted_inputs == 2U &&
            staged.published_inputs == 0U &&
            staged.dirty_channels == 1U &&
            history->AcquireStable(1U, &before) ==
                market::OrderedEventHistoryErrorV1::kNone &&
            before.root != nullptr && before.root->row_count() == 0U &&
            DrainRepair(history.get(), route),
        "micro-batch journals trade and order before exposing projection");
    market::EventStableSnapshotV1 after{};
    std::vector<market::OrderedDerivedEventV1> rows;
    *ok &= Expect(
        history->AcquireStable(1U, &after) ==
                market::OrderedEventHistoryErrorV1::kNone &&
            after.root != nullptr && after.root->CopyRows(&rows) &&
            rows.size() == 3U &&
            rows.front().order_key.business_sequence == 101 &&
            rows.back().order_key.business_sequence == 102,
        "micro-batch first publication is business ordered and complete");
}

void TestShanghaiEndImportsCheckpointRange(bool* ok) {
    std::unique_ptr<market::OrderedEventHistoryV1> history;
    *ok &= Expect(
        market::OrderedEventHistoryV1::Create(
            Config(std::byte{0x44U}), &history) ==
            market::OrderedEventHistoryCreateErrorV1::kNone,
        "create Shanghai END suffix history");
    if (history == nullptr) {
        return;
    }
    market::EventRouteTokenV1 route{};
    static_cast<void>(history->ResolveRoute(0U, 1U, &route));
    *ok &= Expect(
        history->ApplyLive(
            0U, route, ShanghaiAdd(90, 1U, 10, 8, 0)).error ==
                market::OrderedEventHistoryErrorV1::kNone &&
            history->ApplyLive(
                0U, route, ShanghaiAdd(91, 2U, 20, 6, 0)).error ==
                market::OrderedEventHistoryErrorV1::kNone &&
            history->ApplyLive(
                0U, route, ShanghaiEnd(102, 3U)).error ==
                market::OrderedEventHistoryErrorV1::kNone,
        "publish Shanghai orders and END");

    *ok &= Expect(
        history->ApplyLive(
            0U, route, ShanghaiAdd(101, 4U, 30, 5, 0))
                .disposition ==
                market::EventInputDispositionV1::kRepairRegistered &&
            DrainRepair(history.get(), route),
        "replay a Shanghai suffix that contains END");

    market::EventStableSnapshotV1 stable{};
    std::vector<market::OrderedDerivedEventV1> rows;
    bool order_10_finalized = false;
    bool order_20_finalized = false;
    if (history->AcquireStable(1U, &stable) ==
            market::OrderedEventHistoryErrorV1::kNone &&
        stable.root != nullptr && stable.root->CopyRows(&rows)) {
        for (const auto& row : rows) {
            const auto* revision =
                std::get_if<market::ShanghaiOrderRevisionEventV1>(
                    &row.payload);
            if (revision == nullptr ||
                revision->source_anchor.native_event_sequence != 102) {
                continue;
            }
            if (revision->order.key.order_id == 10) {
                order_10_finalized = true;
            } else if (revision->order.key.order_id == 20) {
                order_20_finalized = true;
            }
        }
    }
    *ok &= Expect(
        order_10_finalized && order_20_finalized,
        "Shanghai END replay imports untouched checkpoint orders");
}

void TestBatchFailureRegistersColdFallback(bool* ok) {
    auto config = Config(std::byte{0x55U});
    config.instrument_count = 2U;
    config.event_routes = {0U, 0U};
    std::unique_ptr<market::OrderedEventHistoryV1> history;
    *ok &= Expect(
        market::OrderedEventHistoryV1::Create(
            std::move(config), &history) ==
            market::OrderedEventHistoryCreateErrorV1::kNone,
        "create two-instrument batch-failure history");
    if (history == nullptr) {
        return;
    }
    market::EventRouteTokenV1 first_route{};
    market::EventRouteTokenV1 second_route{};
    static_cast<void>(history->ResolveRoute(0U, 1U, &first_route));
    static_cast<void>(history->ResolveRoute(1U, 2U, &second_route));
    *ok &= Expect(
        history->ApplyLive(
            0U, first_route, ShenzhenAdd(100, 1U, 10)).error ==
            market::OrderedEventHistoryErrorV1::kNone,
        "publish source used by a later conflict");

    auto second_instrument = ShenzhenAdd(100, 2U, 7);
    second_instrument.instrument_id = 2U;
    second_instrument.ordinal = 1U;
    const std::array<market::CompactFastTickV1, 2U> batch{
        second_instrument,
        ShenzhenAdd(100, 3U, 11)};
    const auto failed = history->ApplyBatch(0U, batch);
    const auto second_repair = history->AdvanceDirtyReplay(
        0U,
        second_route,
        1U,
        std::chrono::milliseconds(10));
    *ok &= Expect(
        failed.error ==
                market::OrderedEventHistoryErrorV1::kSourceConflict &&
            history->RepairState(1U) ==
                market::EventRepairStateV1::kSourceConflict &&
            history->RepairState(2U) ==
                market::EventRepairStateV1::kRepairRequired &&
            second_repair.cold_fallback_required,
        "batch failure preserves every dequeued instrument via cold fallback");
}

}  // namespace

int main(int argc, char** argv) {
    bool ok = true;
    const std::string_view selected =
        argc > 1 ? std::string_view(argv[1]) : std::string_view{};
    if (selected.empty() || selected == "shenzhen") {
        TestShenzhenCheckpointedSuffix(&ok);
    }
    if (selected.empty() || selected == "shenzhen_deep") {
        TestShenzhenDeepSuffixCheckpoint(&ok);
    }
    if (selected.empty() || selected == "shanghai") {
        TestShanghaiHiddenCheckpoint(&ok);
    }
    if (selected.empty() || selected == "shanghai_deep") {
        TestShanghaiDeepSuffixCheckpoint(&ok);
    }
    if (selected.empty() || selected == "microbatch") {
        TestMicroBatchJournalsBeforeProjection(&ok);
    }
    if (selected.empty() || selected == "shanghai_end") {
        TestShanghaiEndImportsCheckpointRange(&ok);
    }
    if (selected.empty() || selected == "batch_failure") {
        TestBatchFailureRegistersColdFallback(&ok);
    }
    return ok ? 0 : 1;
}
