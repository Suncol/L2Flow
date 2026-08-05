#include "l2flow/market/shanghai_order_event_aggregator_v1.h"
#include "l2flow/market/shenzhen_order_event_projector_v1.h"

#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <string_view>
#include <variant>
#include <vector>

namespace {

namespace market = l2flow::market;

constexpr std::uint32_t kTradeDate = 20260805U;

bool Expect(bool condition, std::string_view message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << '\n';
    }
    return condition;
}

market::ShanghaiOrderSourceAnchorV1 ShanghaiAnchor(
    std::int64_t business_sequence,
    std::uint64_t diagnostic_sequence) {
    market::ShanghaiOrderSourceAnchorV1 result{};
    result.native_event_sequence = business_sequence;
    result.source_sequence = diagnostic_sequence;
    result.ingress_sequence = diagnostic_sequence + 1U;
    result.vendor_sequence_id = diagnostic_sequence + 2U;
    result.event_time_ns_since_midnight =
        34'200'000'000'000U +
        static_cast<std::uint64_t>(business_sequence);
    result.event_time_unix_ns =
        1'785'859'200'000'000'000LL +
        static_cast<std::int64_t>(
            result.event_time_ns_since_midnight);
    result.recv_realtime_ns = result.event_time_unix_ns + 10;
    result.recv_monotonic_ns =
        static_cast<std::int64_t>(diagnostic_sequence);
    result.event_time_valid = true;
    result.event_time_unix_ns_valid = true;
    return result;
}

market::ShanghaiOrderEventInputV1 ShanghaiBase(
    std::int64_t business_sequence,
    std::int32_t channel,
    market::TickActionV1 action,
    std::uint64_t diagnostic_sequence) {
    market::ShanghaiOrderEventInputV1 result{};
    result.trade_date = kTradeDate;
    result.instrument_id = 1U;
    result.channel = channel;
    result.anchor = ShanghaiAnchor(
        business_sequence, diagnostic_sequence);
    result.action = action;
    result.phase = market::TradingPhaseV1::kContinuous;
    result.phase_valid = true;
    return result;
}

market::ShanghaiOrderEventInputV1 ShanghaiAdd(
    std::int64_t sequence,
    std::int32_t channel,
    std::int64_t order_id,
    market::SideV1 side,
    std::uint64_t diagnostic_sequence) {
    auto result = ShanghaiBase(
        sequence,
        channel,
        market::TickActionV1::kAdd,
        diagnostic_sequence);
    result.primary_order_id = order_id;
    result.side = side;
    result.price_p6 = 10'000'000 + order_id;
    result.price_valid = true;
    result.quantity = 100;
    result.quantity_valid = true;
    result.matched_quantity = 0;
    result.matched_quantity_valid = true;
    return result;
}

market::ShanghaiOrderEventInputV1 ShanghaiTrade(
    std::int64_t sequence,
    std::int64_t buy,
    std::int64_t sell,
    std::int64_t quantity) {
    auto result = ShanghaiBase(
        sequence, 3, market::TickActionV1::kTrade, 1U);
    result.buy_order_id = buy;
    result.sell_order_id = sell;
    result.aggressor = market::AggressorV1::kNeutral;
    result.price_p6 = 10'050'000;
    result.price_valid = true;
    result.trade_amount_p6 = result.price_p6 * quantity;
    result.trade_amount_valid = true;
    result.quantity = quantity;
    result.quantity_valid = true;
    return result;
}

market::ShanghaiOrderEventInputV1 ShanghaiCancel(
    std::int64_t sequence,
    std::int64_t order_id,
    std::int64_t quantity) {
    auto result = ShanghaiBase(
        sequence, 3, market::TickActionV1::kCancel, 2U);
    result.primary_order_id = order_id;
    result.side = market::SideV1::kBuy;
    result.quantity = quantity;
    result.quantity_valid = true;
    return result;
}

void TestShanghaiRules(bool* ok) {
    std::unique_ptr<market::ShanghaiOrderEventAggregatorV1> core;
    *ok &= Expect(
        market::ShanghaiOrderEventAggregatorV1::Create(
            {kTradeDate, 32U}, &core) ==
                market::ShanghaiOrderAggregatorCreateErrorV1::kNone &&
            core != nullptr,
        "create Shanghai business-ordered core");
    if (core == nullptr) {
        return;
    }

    std::vector<market::ShanghaiOrderEventV1> events;
    *ok &= Expect(
        core->ConsumeBusinessOrdered(
            ShanghaiAdd(100, 3, 10, market::SideV1::kBuy, 500U),
            &events) ==
                market::ShanghaiOrderAggregatorConsumeErrorV1::kNone &&
            events.size() == 1U &&
            std::holds_alternative<
                market::ShanghaiOrderRevisionEventV1>(events[0U]),
        "Shanghai add emits one order insertion");
    // A numeric gap is valid and diagnostic source/arrival values may move
    // backwards because neither is an Event ordering key.
    *ok &= Expect(
        core->ConsumeBusinessOrdered(
            ShanghaiAdd(105, 3, 20, market::SideV1::kSell, 1U),
            &events) ==
                market::ShanghaiOrderAggregatorConsumeErrorV1::kNone,
        "Shanghai BizIndex gap ignores diagnostic sequence regression");

    *ok &= Expect(
        core->ConsumeBusinessOrdered(
            ShanghaiTrade(109, 10, 20, 30), &events) ==
                market::ShanghaiOrderAggregatorConsumeErrorV1::kNone &&
            events.size() == 3U &&
            std::holds_alternative<market::ShanghaiTradeEventV1>(
                events[0U]),
        "Shanghai trade emits source trade plus two order revisions");
    market::ShanghaiOrderSnapshotV1 buy{};
    market::ShanghaiOrderSnapshotV1 sell{};
    *ok &= Expect(
        core->GetOrder({kTradeDate, 1U, 3, 10}, &buy) ==
                market::ShanghaiOrderAggregatorQueryErrorV1::kNone &&
            core->GetOrder({kTradeDate, 1U, 3, 20}, &sell) ==
                market::ShanghaiOrderAggregatorQueryErrorV1::kNone &&
            buy.remaining_quantity == 70 &&
            sell.remaining_quantity == 70 &&
            buy.total_trade_quantity == 30 &&
            sell.total_trade_quantity == 30,
        "Shanghai trade accounting updates both known orders exactly");

    *ok &= Expect(
        core->ConsumeBusinessOrdered(
            ShanghaiCancel(120, 10, 20), &events) ==
                market::ShanghaiOrderAggregatorConsumeErrorV1::kNone &&
            events.size() == 2U &&
            std::holds_alternative<market::ShanghaiCancelEventV1>(
                events[0U]),
        "Shanghai cancel emits source cancel plus order revision");
    *ok &= Expect(
        core->GetOrder({kTradeDate, 1U, 3, 10}, &buy) ==
                market::ShanghaiOrderAggregatorQueryErrorV1::kNone &&
            buy.remaining_quantity == 50 &&
            buy.total_cancel_quantity == 20,
        "Shanghai cancel accounting is exact");

    // Native sequences are scoped to a channel. A lower value in channel 4
    // is valid, while a regression in channel 3 is not.
    *ok &= Expect(
        core->ConsumeBusinessOrdered(
            ShanghaiAdd(1, 4, 30, market::SideV1::kBuy, 9U),
            &events) ==
                market::ShanghaiOrderAggregatorConsumeErrorV1::kNone,
        "Shanghai channels have independent BusinessSequence frontiers");
    *ok &= Expect(
        core->ConsumeBusinessOrdered(
            ShanghaiAdd(119, 3, 40, market::SideV1::kBuy, 10U),
            &events) ==
            market::ShanghaiOrderAggregatorConsumeErrorV1::kOutOfOrderInput,
        "Shanghai rejects only a non-increasing sequence in its channel");

    auto end = ShanghaiBase(
        130, 3, market::TickActionV1::kStatus, 3U);
    end.phase = market::TradingPhaseV1::kEnd;
    *ok &= Expect(
        core->ConsumeBusinessOrdered(end, &events) ==
                market::ShanghaiOrderAggregatorConsumeErrorV1::kNone &&
            !events.empty() &&
            std::holds_alternative<market::ShanghaiStatusEventV1>(
                events.front()),
        "Shanghai END publishes status and finalizes that instrument/channel");
}

market::ShenzhenEventSourceAnchorV1 ShenzhenAnchor(
    std::int64_t business_sequence,
    std::uint64_t diagnostic_sequence) {
    market::ShenzhenEventSourceAnchorV1 result{};
    result.native_event_sequence = business_sequence;
    result.source_sequence = diagnostic_sequence;
    result.ingress_sequence = diagnostic_sequence + 1U;
    result.vendor_sequence_id = diagnostic_sequence + 2U;
    result.event_time_ns_since_midnight =
        34'200'000'000'000U +
        static_cast<std::uint64_t>(business_sequence);
    result.event_time_unix_ns =
        1'785'859'200'000'000'000LL +
        static_cast<std::int64_t>(
            result.event_time_ns_since_midnight);
    result.recv_realtime_ns = result.event_time_unix_ns + 10;
    result.recv_monotonic_ns =
        static_cast<std::int64_t>(diagnostic_sequence);
    result.event_time_valid = true;
    result.event_time_unix_ns_valid = true;
    return result;
}

market::ShenzhenOrderEventInputV1 ShenzhenOrder(
    std::int64_t sequence,
    std::uint32_t channel,
    market::SideV1 side,
    std::uint64_t diagnostic_sequence) {
    market::ShenzhenOrderEventInputV1 result{};
    result.trade_date = kTradeDate;
    result.instrument_id = 2U;
    result.channel = channel;
    result.anchor = ShenzhenAnchor(sequence, diagnostic_sequence);
    result.action = market::TickActionV1::kAdd;
    result.side = side;
    result.order_type = market::OrderTypeV1::kLimit;
    result.price_p6 = 10'000'000 + sequence;
    result.quantity = 100;
    result.primary_order_id = sequence;
    result.price_valid = true;
    result.quantity_valid = true;
    result.side_valid = true;
    result.order_type_valid = true;
    return result;
}

market::ShenzhenOrderEventInputV1 ShenzhenTrade(
    std::int64_t sequence,
    std::int64_t buy,
    std::int64_t sell,
    std::int64_t quantity) {
    market::ShenzhenOrderEventInputV1 result{};
    result.trade_date = kTradeDate;
    result.instrument_id = 2U;
    result.channel = 7U;
    result.anchor = ShenzhenAnchor(sequence, 2U);
    result.action = market::TickActionV1::kTrade;
    result.price_p6 = 10'050'000;
    result.quantity = quantity;
    result.buy_order_id = buy;
    result.sell_order_id = sell;
    result.price_valid = true;
    result.quantity_valid = true;
    return result;
}

market::ShenzhenOrderEventInputV1 ShenzhenCancel(
    std::int64_t sequence,
    std::int64_t order_id,
    std::int64_t quantity) {
    market::ShenzhenOrderEventInputV1 result{};
    result.trade_date = kTradeDate;
    result.instrument_id = 2U;
    result.channel = 7U;
    result.anchor = ShenzhenAnchor(sequence, 3U);
    result.action = market::TickActionV1::kCancel;
    result.side = market::SideV1::kBuy;
    result.quantity = quantity;
    result.primary_order_id = order_id;
    result.buy_order_id = order_id;
    result.quantity_valid = true;
    result.side_valid = true;
    return result;
}

void TestShenzhenRules(bool* ok) {
    std::unique_ptr<market::ShenzhenOrderEventProjectorV1> core;
    *ok &= Expect(
        market::ShenzhenOrderEventProjectorV1::Create(
            {kTradeDate, 32U}, &core) ==
                market::ShenzhenOrderProjectorCreateErrorV1::kNone &&
            core != nullptr,
        "create Shenzhen business-ordered core");
    if (core == nullptr) {
        return;
    }

    std::vector<market::ShenzhenOrderEventV1> events;
    *ok &= Expect(
        core->ConsumeBusinessOrdered(
            ShenzhenOrder(100, 7U, market::SideV1::kBuy, 500U),
            &events) ==
                market::ShenzhenOrderProjectorConsumeErrorV1::kNone &&
            events.size() == 1U,
        "Shenzhen 6.33 order creates one exact order state");
    *ok &= Expect(
        core->ConsumeBusinessOrdered(
            ShenzhenOrder(105, 7U, market::SideV1::kSell, 1U),
            &events) ==
                market::ShenzhenOrderProjectorConsumeErrorV1::kNone,
        "Shenzhen ApplSeqNum gap ignores diagnostic sequence regression");
    *ok &= Expect(
        core->ConsumeBusinessOrdered(
            ShenzhenTrade(109, 100, 105, 30), &events) ==
                market::ShenzhenOrderProjectorConsumeErrorV1::kNone &&
            events.size() == 3U &&
            std::holds_alternative<market::ShenzhenTradeEventV1>(
                events[0U]),
        "Shenzhen trade emits trade plus two known-order revisions");
    market::ShenzhenOrderSnapshotV1 buy{};
    market::ShenzhenOrderSnapshotV1 sell{};
    *ok &= Expect(
        core->GetOrder({kTradeDate, 2U, 7U, 100}, &buy) ==
                market::ShenzhenOrderProjectorQueryErrorV1::kNone &&
            core->GetOrder({kTradeDate, 2U, 7U, 105}, &sell) ==
                market::ShenzhenOrderProjectorQueryErrorV1::kNone &&
            buy.remaining_quantity == 70 &&
            sell.remaining_quantity == 70,
        "Shenzhen trade accounting updates both referenced orders");

    *ok &= Expect(
        core->ConsumeBusinessOrdered(
            ShenzhenCancel(120, 100, 20), &events) ==
                market::ShenzhenOrderProjectorConsumeErrorV1::kNone &&
            events.size() == 2U &&
            std::get<market::ShenzhenCancelEventV1>(events[0U])
                .referenced_order_found,
        "Shenzhen cancel emits source cancel plus known-order revision");
    *ok &= Expect(
        core->GetOrder({kTradeDate, 2U, 7U, 100}, &buy) ==
                market::ShenzhenOrderProjectorQueryErrorV1::kNone &&
            buy.remaining_quantity == 50 &&
            buy.total_cancel_quantity == 20,
        "Shenzhen cancel accounting is exact");

    *ok &= Expect(
        core->ConsumeBusinessOrdered(
            ShenzhenTrade(130, 999, 998, 1), &events) ==
                market::ShenzhenOrderProjectorConsumeErrorV1::kNone &&
            events.size() == 1U &&
            (std::get<market::ShenzhenTradeEventV1>(events[0U])
                 .quality_flags &
             market::ShenzhenEventQualityBitV1(
                 market::ShenzhenEventQualityFlagV1::
                     kUnknownBuyOrderReference)) != 0U,
        "Shenzhen does not invent missing 6.33 order states");

    *ok &= Expect(
        core->ConsumeBusinessOrdered(
            ShenzhenOrder(1, 8U, market::SideV1::kBuy, 4U),
            &events) ==
                market::ShenzhenOrderProjectorConsumeErrorV1::kNone,
        "Shenzhen channels have independent ApplSeqNum frontiers");
    *ok &= Expect(
        core->ConsumeBusinessOrdered(
            ShenzhenTrade(129, 100, 105, 1), &events) ==
            market::ShenzhenOrderProjectorConsumeErrorV1::kOutOfOrderInput,
        "Shenzhen rejects only a non-increasing sequence in its channel");

    market::ShenzhenEventSourceAnchorV1 boundary{};
    *ok &= Expect(
        core->Finalize(boundary, &events) ==
                market::ShenzhenOrderProjectorConsumeErrorV1::kNone &&
            core->finalized() && !events.empty(),
        "Shenzhen explicit source-free boundary finalizes open orders");
}

}  // namespace

int main() {
    bool ok = true;
    TestShanghaiRules(&ok);
    TestShenzhenRules(&ok);
    return ok ? 0 : 1;
}
