#include "l2flow/market/shenzhen_order_event_projector_v1.h"

#include <cstdint>
#include <iostream>
#include <limits>
#include <memory>
#include <string_view>
#include <utility>
#include <vector>

namespace {

namespace market = l2flow::market;

bool Expect(bool condition, std::string_view message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << '\n';
    }
    return condition;
}

market::ShenzhenEventSourceAnchorV1 Anchor(
    std::int64_t application_sequence) {
    market::ShenzhenEventSourceAnchorV1 anchor{};
    anchor.native_event_sequence = application_sequence;
    anchor.source_sequence =
        static_cast<std::uint64_t>(application_sequence) + 10U;
    anchor.ingress_sequence =
        static_cast<std::uint64_t>(application_sequence) + 20U;
    anchor.tick_stream_sequence =
        static_cast<std::uint64_t>(application_sequence) + 30U;
    anchor.vendor_sequence_id =
        static_cast<std::uint64_t>(application_sequence) + 40U;
    anchor.event_time_ns_since_midnight =
        34'200'000'000'000U +
        static_cast<std::uint64_t>(application_sequence);
    anchor.event_time_unix_ns =
        1'785'369'600'000'000'000LL +
        static_cast<std::int64_t>(
            anchor.event_time_ns_since_midnight);
    anchor.recv_realtime_ns = anchor.event_time_unix_ns + 50;
    anchor.recv_monotonic_ns = application_sequence + 60;
    anchor.vendor_local_time_raw = 93000000U;
    anchor.vendor_local_time_ns_since_midnight =
        34'200'000'000'000U;
    anchor.event_time_valid = true;
    anchor.event_time_unix_ns_valid = true;
    anchor.vendor_local_time_valid = true;
    return anchor;
}

market::ShenzhenOrderEventInputV1 Order(
    std::int64_t order_id,
    market::SideV1 side,
    market::OrderTypeV1 order_type,
    std::int64_t quantity,
    std::uint32_t instrument_id = 1U,
    std::uint32_t channel = 7U,
    std::uint32_t trade_date = 20260730U) {
    market::ShenzhenOrderEventInputV1 input{};
    input.trade_date = trade_date;
    input.instrument_id = instrument_id;
    input.channel = channel;
    input.anchor = Anchor(order_id);
    input.action = market::TickActionV1::kAdd;
    input.side = side;
    input.order_type = order_type;
    input.quantity = quantity;
    input.primary_order_id = order_id;
    input.quantity_valid = true;
    input.side_valid = true;
    input.order_type_valid = true;
    if (order_type == market::OrderTypeV1::kLimit) {
        input.price_p6 = 10'500'000;
        input.price_valid = true;
    }
    input.source_quality_flags = 0x10U;
    input.source_market_notices = 0x20U;
    return input;
}

market::ShenzhenOrderEventInputV1 Trade(
    std::int64_t transaction_sequence,
    std::int64_t buy_order_id,
    std::int64_t sell_order_id,
    std::int64_t quantity,
    std::uint32_t instrument_id = 1U,
    std::uint32_t channel = 7U,
    std::uint32_t trade_date = 20260730U) {
    market::ShenzhenOrderEventInputV1 input{};
    input.trade_date = trade_date;
    input.instrument_id = instrument_id;
    input.channel = channel;
    input.anchor = Anchor(transaction_sequence);
    input.action = market::TickActionV1::kTrade;
    input.price_p6 = 10'400'000;
    input.quantity = quantity;
    input.buy_order_id = buy_order_id;
    input.sell_order_id = sell_order_id;
    input.price_valid = true;
    input.quantity_valid = true;
    input.source_quality_flags = 0x40U;
    input.source_market_notices = 0x80U;
    return input;
}

market::ShenzhenOrderEventInputV1 Cancel(
    std::int64_t transaction_sequence,
    std::int64_t order_id,
    market::SideV1 side,
    std::int64_t quantity,
    std::uint32_t instrument_id = 1U,
    std::uint32_t channel = 7U,
    std::uint32_t trade_date = 20260730U) {
    market::ShenzhenOrderEventInputV1 input{};
    input.trade_date = trade_date;
    input.instrument_id = instrument_id;
    input.channel = channel;
    input.anchor = Anchor(transaction_sequence);
    input.action = market::TickActionV1::kCancel;
    input.side = side;
    input.quantity = quantity;
    input.primary_order_id = order_id;
    input.quantity_valid = true;
    input.side_valid = true;
    if (side == market::SideV1::kBuy) {
        input.buy_order_id = order_id;
    } else {
        input.sell_order_id = order_id;
    }
    return input;
}

std::unique_ptr<market::ShenzhenOrderEventProjectorV1> Projector(
    bool* ok,
    std::size_t capacity = 64U,
    std::uint32_t trade_date = 20260730U) {
    market::ShenzhenOrderEventProjectorConfigV1 config{};
    config.trade_date = trade_date;
    config.maximum_order_states = capacity;
    std::unique_ptr<market::ShenzhenOrderEventProjectorV1>
        projector;
    *ok &= Expect(
        market::ShenzhenOrderEventProjectorV1::Create(
            config, &projector) ==
                market::ShenzhenOrderProjectorCreateErrorV1::
                    kNone &&
            projector != nullptr,
        "create Shenzhen order projector");
    return projector;
}

const market::ShenzhenOrderRevisionEventV1* Revision(
    const market::ShenzhenOrderEventV1& event) {
    return std::get_if<market::ShenzhenOrderRevisionEventV1>(
        &event);
}

void TestDirectOrders(bool* ok) {
    auto projector = Projector(ok);
    if (projector == nullptr) {
        return;
    }
    std::vector<market::ShenzhenOrderEventV1> events;

    *ok &= Expect(
        projector->Consume(
            Order(
                101,
                market::SideV1::kBuy,
                market::OrderTypeV1::kLimit,
                100),
            &events) ==
                market::ShenzhenOrderProjectorConsumeErrorV1::
                    kNone &&
            events.size() == 1U,
        "consume direct limit order");
    const auto* limit =
        events.empty() ? nullptr : Revision(events.front());
    *ok &= Expect(
        limit != nullptr &&
            limit->operation ==
                market::ShenzhenOrderDeltaOperationV1::kInsert &&
            limit->order.original_quantity == 100 &&
            limit->order.remaining_quantity == 100 &&
            limit->order.remaining_quantity_valid &&
            limit->order.price_valid &&
            limit->order.price_p6 == 10'500'000 &&
            limit->order.side == market::SideV1::kBuy &&
            limit->order.order_type ==
                market::OrderTypeV1::kLimit &&
            limit->order.revision == 1U,
        "limit order preserves exact source facts");

    *ok &= Expect(
        projector->Consume(
            Order(
                102,
                market::SideV1::kSell,
                market::OrderTypeV1::kMarket,
                200),
            &events) ==
                market::ShenzhenOrderProjectorConsumeErrorV1::
                    kNone,
        "consume market order");
    const auto* market_order =
        events.empty() ? nullptr : Revision(events.front());
    *ok &= Expect(
        market_order != nullptr &&
            market_order->order.order_type ==
                market::OrderTypeV1::kMarket &&
            !market_order->order.price_valid &&
            market_order->order.price_p6 == 0 &&
            market_order->order.original_quantity == 200,
        "market order does not invent a price");

    *ok &= Expect(
        projector->Consume(
            Order(
                103,
                market::SideV1::kSell,
                market::OrderTypeV1::kSameSideBest,
                300),
            &events) ==
                market::ShenzhenOrderProjectorConsumeErrorV1::
                    kNone,
        "consume same-side-best order");
    const auto* own_best =
        events.empty() ? nullptr : Revision(events.front());
    *ok &= Expect(
        own_best != nullptr &&
            own_best->order.order_type ==
                market::OrderTypeV1::kSameSideBest &&
            !own_best->order.price_valid &&
            own_best->order.original_quantity == 300,
        "same-side-best order does not invent a price");

    auto invalid_market = Order(
        104,
        market::SideV1::kBuy,
        market::OrderTypeV1::kMarket,
        10);
    invalid_market.price_valid = true;
    invalid_market.price_p6 = 9'000'000;
    *ok &= Expect(
        projector->Consume(invalid_market, &events) ==
                market::ShenzhenOrderProjectorConsumeErrorV1::
                    kInvalidInput &&
            events.empty(),
        "market input rejects a fabricated valid price");
}

void TestTradeUpdatesAndMissingReferences(bool* ok) {
    auto projector = Projector(ok);
    if (projector == nullptr) {
        return;
    }
    std::vector<market::ShenzhenOrderEventV1> events;
    *ok &= Expect(
        projector->Consume(
            Order(
                100,
                market::SideV1::kBuy,
                market::OrderTypeV1::kLimit,
                100),
            &events) ==
            market::ShenzhenOrderProjectorConsumeErrorV1::kNone,
        "insert known buy");
    *ok &= Expect(
        projector->Consume(
            Order(
                200,
                market::SideV1::kSell,
                market::OrderTypeV1::kLimit,
                80),
            &events) ==
            market::ShenzhenOrderProjectorConsumeErrorV1::kNone,
        "insert known sell");

    const auto trade_input = Trade(900, 100, 200, 30);
    *ok &= Expect(
        projector->Consume(trade_input, &events) ==
                market::ShenzhenOrderProjectorConsumeErrorV1::
                    kNone &&
            events.size() == 3U,
        "trade emits source event and two revisions");
    const auto* trade =
        events.empty()
            ? nullptr
            : std::get_if<market::ShenzhenTradeEventV1>(
                  &events.front());
    const auto* first_revision =
        events.size() < 2U ? nullptr : Revision(events[1U]);
    const auto* second_revision =
        events.size() < 3U ? nullptr : Revision(events[2U]);
    *ok &= Expect(
        trade != nullptr &&
            trade->buy_order_id == 100 &&
            trade->sell_order_id == 200 &&
            trade->aggressor == market::AggressorV1::kUnknown &&
            trade->price_p6 == 10'400'000 &&
            trade->quantity == 30 &&
            !trade->amount_valid &&
            trade->amount_p6 == 0 &&
            trade->source_anchor.native_event_sequence == 900,
        "trade preserves both IDs and source values without amount or aggressor inference");
    *ok &= Expect(
        first_revision != nullptr &&
            second_revision != nullptr &&
            first_revision->order.key.order_id == 100 &&
            first_revision->order.remaining_quantity == 70 &&
            second_revision->order.key.order_id == 200 &&
            second_revision->order.remaining_quantity == 50 &&
            first_revision->order.total_trade_quantity == 30 &&
            second_revision->order.total_trade_quantity == 30,
        "trade revisions are deterministic and update both known sides");

    const std::size_t before_unknown = projector->order_count();
    *ok &= Expect(
        projector->Consume(Trade(901, 777, 888, 5), &events) ==
                market::ShenzhenOrderProjectorConsumeErrorV1::
                    kNone &&
            events.size() == 1U &&
            projector->order_count() == before_unknown,
        "unknown trade references never create synthetic orders");
    trade = events.empty()
                ? nullptr
                : std::get_if<market::ShenzhenTradeEventV1>(
                      &events.front());
    const std::uint64_t both_unknown =
        market::ShenzhenEventQualityBitV1(
            market::ShenzhenEventQualityFlagV1::
                kUnknownBuyOrderReference) |
        market::ShenzhenEventQualityBitV1(
            market::ShenzhenEventQualityFlagV1::
                kUnknownSellOrderReference);
    *ok &= Expect(
        trade != nullptr &&
            (trade->quality_flags & both_unknown) ==
                both_unknown,
        "unknown buy and sell references are audited");

    *ok &= Expect(
        projector->Consume(Trade(902, 100, 999, 5), &events) ==
                market::ShenzhenOrderProjectorConsumeErrorV1::
                    kNone &&
            events.size() == 2U,
        "one known trade reference updates only known order");
    trade = events.empty()
                ? nullptr
                : std::get_if<market::ShenzhenTradeEventV1>(
                      &events.front());
    *ok &= Expect(
        trade != nullptr &&
            (trade->quality_flags &
             market::ShenzhenEventQualityBitV1(
                 market::ShenzhenEventQualityFlagV1::
                     kUnknownSellOrderReference)) != 0U &&
            Revision(events[1U]) != nullptr &&
            Revision(events[1U])->order.key.order_id == 100,
        "single missing reference is explicit and not synthesized");

    *ok &= Expect(
        projector->Consume(Trade(903, 0, 0, 5), &events) ==
                market::ShenzhenOrderProjectorConsumeErrorV1::
                    kNone &&
            events.size() == 1U,
        "zero trade references remain a preserved source trade");
    trade = events.empty()
                ? nullptr
                : std::get_if<market::ShenzhenTradeEventV1>(
                      &events.front());
    *ok &= Expect(
        trade != nullptr &&
            (trade->quality_flags & both_unknown) == both_unknown,
        "zero means no corresponding order and is audited on both sides");

    *ok &= Expect(
        projector->Consume(Trade(904, 100, 100, 5), &events) ==
                market::ShenzhenOrderProjectorConsumeErrorV1::
                    kNone &&
            events.size() == 1U,
        "ambiguous equal positive references never double-apply quantity");
    trade = events.empty()
                ? nullptr
                : std::get_if<market::ShenzhenTradeEventV1>(
                      &events.front());
    *ok &= Expect(
        trade != nullptr &&
            (trade->quality_flags &
             market::ShenzhenEventQualityBitV1(
                 market::ShenzhenEventQualityFlagV1::
                     kAmbiguousTradeOrderReferences)) != 0U,
        "ambiguous equal references are preserved and explicitly flagged");
}

void TestCancelCardinalityAndState(bool* ok) {
    auto projector = Projector(ok);
    if (projector == nullptr) {
        return;
    }
    std::vector<market::ShenzhenOrderEventV1> events;
    *ok &= Expect(
        projector->Consume(
            Order(
                300,
                market::SideV1::kBuy,
                market::OrderTypeV1::kLimit,
                20),
            &events) ==
            market::ShenzhenOrderProjectorConsumeErrorV1::kNone,
        "insert cancellable order");
    *ok &= Expect(
        projector->Consume(
            Cancel(910, 300, market::SideV1::kBuy, 20),
            &events) ==
                market::ShenzhenOrderProjectorConsumeErrorV1::
                    kNone &&
            events.size() == 2U,
        "valid cancel emits cancel and revision");
    const auto* cancel =
        events.empty()
            ? nullptr
            : std::get_if<market::ShenzhenCancelEventV1>(
                  &events.front());
    const auto* revision =
        events.size() < 2U ? nullptr : Revision(events[1U]);
    *ok &= Expect(
        cancel != nullptr && cancel->referenced_order_found &&
            cancel->side == market::SideV1::kBuy &&
            cancel->quantity == 20 &&
            revision != nullptr &&
            revision->operation ==
                market::ShenzhenOrderDeltaOperationV1::kFinalize &&
            revision->order.remaining_quantity == 0 &&
            revision->order.total_cancel_quantity == 20 &&
            revision->order.finality ==
                market::ShenzhenOrderFinalityV1::kFinal,
        "cancel uses source side/quantity and finalizes zero balance");

    const std::size_t before_unknown = projector->order_count();
    *ok &= Expect(
        projector->Consume(
            Cancel(911, 444, market::SideV1::kSell, 2),
            &events) ==
                market::ShenzhenOrderProjectorConsumeErrorV1::
                    kNone &&
            events.size() == 1U &&
            projector->order_count() == before_unknown,
        "unknown cancel remains an audit event");
    cancel = events.empty()
                 ? nullptr
                 : std::get_if<market::ShenzhenCancelEventV1>(
                       &events.front());
    *ok &= Expect(
        cancel != nullptr &&
            !cancel->referenced_order_found &&
            (cancel->quality_flags &
             market::ShenzhenEventQualityBitV1(
                 market::ShenzhenEventQualityFlagV1::
                     kUnknownCancelOrderReference)) != 0U,
        "unknown cancel reference is flagged");

    auto neither = Cancel(
        912, 445, market::SideV1::kBuy, 1);
    neither.buy_order_id = 0;
    *ok &= Expect(
        projector->Consume(neither, &events) ==
                market::ShenzhenOrderProjectorConsumeErrorV1::
                    kInvalidInput &&
            events.empty(),
        "cancel rejects zero referenced orders");

    auto both = Cancel(913, 446, market::SideV1::kBuy, 1);
    both.sell_order_id = 447;
    *ok &= Expect(
        projector->Consume(both, &events) ==
                market::ShenzhenOrderProjectorConsumeErrorV1::
                    kInvalidInput &&
            events.empty(),
        "cancel rejects two positive referenced orders");

    auto invalid_price = Cancel(
        914, 448, market::SideV1::kBuy, 1);
    invalid_price.price_p6 = 1;
    invalid_price.price_valid = true;
    *ok &= Expect(
        projector->Consume(invalid_price, &events) ==
                market::ShenzhenOrderProjectorConsumeErrorV1::
                    kInvalidInput,
        "cancel rejects semantically invalid price publication");
}

void TestBorrowLendReferenceCompatibility(bool* ok) {
    auto projector = Projector(ok);
    if (projector == nullptr) {
        return;
    }
    std::vector<market::ShenzhenOrderEventV1> events;
    *ok &= Expect(
        projector->Consume(
            Order(
                400,
                market::SideV1::kBorrow,
                market::OrderTypeV1::kLimit,
                20),
            &events) ==
                market::ShenzhenOrderProjectorConsumeErrorV1::kNone &&
            projector->Consume(
                Order(
                    401,
                    market::SideV1::kLend,
                    market::OrderTypeV1::kLimit,
                    20),
                &events) ==
                market::ShenzhenOrderProjectorConsumeErrorV1::kNone,
        "insert documented borrow and lend orders");
    *ok &= Expect(
        projector->Consume(Trade(950, 400, 401, 5), &events) ==
                market::ShenzhenOrderProjectorConsumeErrorV1::kNone &&
            events.size() == 3U,
        "Bid/Offer references update compatible borrow/lend states");
    bool compatible = true;
    for (std::size_t index = 1U; index < events.size(); ++index) {
        const auto* revision = Revision(events[index]);
        compatible &=
            revision != nullptr &&
            revision->order.remaining_quantity == 15 &&
            (revision->order.quality_flags &
             market::ShenzhenEventQualityBitV1(
                 market::ShenzhenEventQualityFlagV1::
                     kSideConflict)) == 0U;
    }
    *ok &= Expect(
        compatible,
        "reference position is not misreported as original order side");

    *ok &= Expect(
        projector->Consume(
            Cancel(951, 400, market::SideV1::kBuy, 15),
            &events) ==
                market::ShenzhenOrderProjectorConsumeErrorV1::kNone &&
            events.size() == 2U,
        "bid-position cancel resolves the known borrow order");
    const auto* cancel =
        events.empty()
            ? nullptr
            : std::get_if<market::ShenzhenCancelEventV1>(
                  &events.front());
    *ok &= Expect(
        cancel != nullptr && cancel->side_from_order &&
            cancel->side == market::SideV1::kBorrow,
        "known cancel side comes from 6.33 instead of Bid/Offer naming");
}

void TestQuantityConflicts(bool* ok) {
    std::vector<market::ShenzhenOrderEventV1> events;
    auto underflow = Projector(ok);
    if (underflow == nullptr) {
        return;
    }
    *ok &= Expect(
        underflow->Consume(
            Order(
                500,
                market::SideV1::kBuy,
                market::OrderTypeV1::kLimit,
                5),
            &events) ==
            market::ShenzhenOrderProjectorConsumeErrorV1::kNone,
        "insert underflow test order");
    *ok &= Expect(
        underflow->Consume(Trade(920, 500, 999, 6), &events) ==
            market::ShenzhenOrderProjectorConsumeErrorV1::kNone,
        "consume quantity underflow trade");
    market::ShenzhenOrderSnapshotV1 snapshot{};
    *ok &= Expect(
        underflow->GetOrder(
            {20260730U, 1U, 7U, 500}, &snapshot) ==
                market::ShenzhenOrderProjectorQueryErrorV1::
                    kNone &&
            snapshot.remaining_quantity == -1 &&
            !snapshot.remaining_quantity_valid &&
            snapshot.finality ==
                market::ShenzhenOrderFinalityV1::kConflict &&
            (snapshot.quality_flags &
             market::ShenzhenEventQualityBitV1(
                 market::ShenzhenEventQualityFlagV1::
                     kQuantityConflict)) != 0U,
        "negative balance is preserved and flagged, never clamped");

    auto overflow = Projector(ok);
    if (overflow == nullptr) {
        return;
    }
    constexpr std::int64_t kMaximum =
        std::numeric_limits<std::int64_t>::max();
    *ok &= Expect(
        overflow->Consume(
            Order(
                600,
                market::SideV1::kBuy,
                market::OrderTypeV1::kLimit,
                kMaximum),
            &events) ==
            market::ShenzhenOrderProjectorConsumeErrorV1::kNone,
        "insert numeric overflow test order");
    *ok &= Expect(
        overflow->Consume(
            Trade(930, 600, 999, kMaximum), &events) ==
                market::ShenzhenOrderProjectorConsumeErrorV1::
                    kNone &&
            overflow->Consume(
                Trade(931, 600, 999, 1), &events) ==
                market::ShenzhenOrderProjectorConsumeErrorV1::
                    kNone,
        "consume total-quantity overflow");
    snapshot = {};
    *ok &= Expect(
        overflow->GetOrder(
            {20260730U, 1U, 7U, 600}, &snapshot) ==
                market::ShenzhenOrderProjectorQueryErrorV1::
                    kNone &&
            snapshot.total_trade_quantity == kMaximum &&
            snapshot.remaining_quantity == -1 &&
            (snapshot.quality_flags &
             market::ShenzhenEventQualityBitV1(
                 market::ShenzhenEventQualityFlagV1::
                     kNumericOverflow)) != 0U &&
            snapshot.finality ==
                market::ShenzhenOrderFinalityV1::kConflict,
        "numeric overflow retains last representable total and flags conflict");
}

void TestKeyIsolationAndFinalization(bool* ok) {
    auto projector = Projector(ok);
    if (projector == nullptr) {
        return;
    }
    std::vector<market::ShenzhenOrderEventV1> events;
    const auto first = Order(
        700,
        market::SideV1::kBuy,
        market::OrderTypeV1::kLimit,
        10,
        1U,
        7U);
    const auto instrument_isolated = Order(
        701,
        market::SideV1::kSell,
        market::OrderTypeV1::kMarket,
        20,
        2U,
        7U);
    auto channel_isolated = Order(
        700,
        market::SideV1::kSell,
        market::OrderTypeV1::kSameSideBest,
        30,
        1U,
        8U);
    channel_isolated.anchor.tick_stream_sequence = 732U;
    *ok &= Expect(
        projector->Consume(first, &events) ==
                market::ShenzhenOrderProjectorConsumeErrorV1::
                    kNone &&
            projector->Consume(instrument_isolated, &events) ==
                market::ShenzhenOrderProjectorConsumeErrorV1::
                    kNone &&
            projector->Consume(channel_isolated, &events) ==
                market::ShenzhenOrderProjectorConsumeErrorV1::
                    kNone &&
            projector->order_count() == 3U,
        "instrument and channel participate in exact order key");

    auto wrong_date = first;
    wrong_date.trade_date = 20260731U;
    *ok &= Expect(
        projector->Consume(wrong_date, &events) ==
                market::ShenzhenOrderProjectorConsumeErrorV1::
                    kWrongTradeDate &&
            projector->order_count() == 3U,
        "trade date cannot alias the configured daily state");

    *ok &= Expect(
        projector->Finalize(Anchor(999), &events) ==
                market::ShenzhenOrderProjectorConsumeErrorV1::
                    kNone &&
            events.size() == 3U &&
            projector->finalized(),
        "clean boundary finalizes residual orders");
    bool finalization_valid = true;
    for (const auto& event : events) {
        const auto* revision = Revision(event);
        finalization_valid &=
            revision != nullptr &&
            revision->operation ==
                market::ShenzhenOrderDeltaOperationV1::kFinalize &&
            revision->order.finality ==
                market::ShenzhenOrderFinalityV1::kFinal &&
            (revision->order.quality_flags &
             market::ShenzhenEventQualityBitV1(
                 market::ShenzhenEventQualityFlagV1::
                     kEndedWithObservedBalance)) != 0U;
    }
    *ok &= Expect(
        finalization_valid,
        "finalization marks observed residual without treating it as numeric conflict");

    auto source_free = Projector(ok);
    if (source_free != nullptr) {
        *ok &= Expect(
            source_free->Consume(first, &events) ==
                    market::ShenzhenOrderProjectorConsumeErrorV1::
                        kNone &&
                source_free->Finalize({}, &events) ==
                    market::ShenzhenOrderProjectorConsumeErrorV1::
                        kNone &&
                events.size() == 1U &&
                Revision(events.front()) != nullptr &&
                Revision(events.front())
                        ->source_anchor.native_event_sequence == 0,
            "source-free clean boundary does not fabricate ApplSeqNum");
    }

    auto next_day = Projector(ok, 8U, 20260731U);
    if (next_day != nullptr) {
        auto next_order = first;
        next_order.trade_date = 20260731U;
        *ok &= Expect(
            next_day->Consume(next_order, &events) ==
                    market::ShenzhenOrderProjectorConsumeErrorV1::
                        kNone &&
                next_day->order_count() == 1U,
            "same order identifier is isolated by trade date");
    }
}

void TestDecodedProjection(bool* ok) {
    market::ShenzhenOrderV1 decoded_order{};
    decoded_order.common.kind =
        market::MarketEventKindV1::kShenzhenOrder;
    decoded_order.common.market = market::MarketV1::kShenzhen;
    decoded_order.common.origin.trade_date = 20260730U;
    decoded_order.common.origin.source_sequence = 123U;
    decoded_order.common.origin.vendor_sequence_id = 456U;
    decoded_order.common.origin.vendor_local_time_raw =
        93000001U;
    decoded_order.common.origin.recv_realtime_ns = 789;
    decoded_order.common.origin.recv_monotonic_ns = 790;
    decoded_order.common.instrument_id = 9U;
    decoded_order.common.exchange_time.valid = true;
    decoded_order.common.exchange_time.unix_nanoseconds_valid =
        true;
    decoded_order.common.exchange_time
        .nanoseconds_since_midnight = 34'200'001'000'000U;
    decoded_order.common.exchange_time.unix_nanoseconds = 791;
    decoded_order.common.vendor_local_time.valid = true;
    decoded_order.common.vendor_local_time
        .nanoseconds_since_midnight = 34'200'002'000'000U;
    decoded_order.channel = 12U;
    decoded_order.application_sequence = 800;
    decoded_order.fields.action = market::TickActionV1::kAdd;
    decoded_order.fields.side = market::SideV1::kBuy;
    decoded_order.fields.order_type =
        market::OrderTypeV1::kMarket;
    decoded_order.fields.primary_order_id = 800;
    decoded_order.fields.quantity.raw = 55;
    decoded_order.fields.quantity.valid = true;
    decoded_order.fields.price.normalized_p6 = 99'000'000;
    decoded_order.fields.price.valid = false;
    decoded_order.fields.validity_bitmap =
        market::kTickQuantityValidV1 |
        market::kTickPrimaryOrderIdValidV1 |
        market::kTickSideValidV1 |
        market::kTickOrderTypeValidV1 |
        market::kTickExchangeTimeValidV1;
    market::DecodedMarketEventV1 event(
        std::move(decoded_order));

    market::ShenzhenOrderEventInputV1 projected{};
    *ok &= Expect(
        market::ProjectShenzhenOrderEventInputV1(
            event, 321U, 654U, &projected) ==
                market::ShenzhenOrderEventProjectionV1::
                    kProjected &&
            projected.primary_order_id == 800 &&
            projected.anchor.native_event_sequence == 800 &&
            projected.anchor.source_sequence == 123U &&
            projected.anchor.ingress_sequence == 321U &&
            projected.anchor.tick_stream_sequence == 654U &&
            projected.anchor.vendor_sequence_id == 456U &&
            projected.anchor.event_time_valid &&
            projected.anchor.event_time_unix_ns_valid &&
            projected.anchor.vendor_local_time_valid &&
            !projected.price_valid &&
            projected.price_p6 == 0,
        "decoded adapter preserves sequence/time anchors and suppresses invalid market price");

    market::ShenzhenTransactionV1 ambiguous{};
    ambiguous.common.kind =
        market::MarketEventKindV1::kShenzhenTransaction;
    ambiguous.common.market = market::MarketV1::kShenzhen;
    ambiguous.common.origin.trade_date = 20260730U;
    ambiguous.common.origin.source_sequence = 1U;
    ambiguous.common.instrument_id = 1U;
    ambiguous.channel = 7U;
    ambiguous.application_sequence = 801;
    ambiguous.fields.action = market::TickActionV1::kCancel;
    ambiguous.fields.quantity.raw = 1;
    ambiguous.fields.quantity.valid = true;
    ambiguous.fields.buy_order_id = 1;
    ambiguous.fields.sell_order_id = 2;
    ambiguous.fields.validity_bitmap =
        market::kTickQuantityValidV1;
    event = std::move(ambiguous);
    *ok &= Expect(
        market::ProjectShenzhenOrderEventInputV1(
            event, 1U, 1U, &projected) ==
            market::ShenzhenOrderEventProjectionV1::
                kInvalidEvent,
        "decoded adapter rejects ambiguous cancel reference cardinality");

    market::ShenzhenTransactionV1 zero_reference_trade{};
    zero_reference_trade.common.kind =
        market::MarketEventKindV1::kShenzhenTransaction;
    zero_reference_trade.common.market =
        market::MarketV1::kShenzhen;
    zero_reference_trade.common.origin.trade_date = 20260730U;
    zero_reference_trade.common.origin.source_sequence = 2U;
    zero_reference_trade.common.instrument_id = 1U;
    zero_reference_trade.channel = 7U;
    zero_reference_trade.application_sequence = 802;
    zero_reference_trade.fields.action =
        market::TickActionV1::kTrade;
    zero_reference_trade.fields.quantity.raw = 3;
    zero_reference_trade.fields.quantity.valid = true;
    zero_reference_trade.fields.price.normalized_p6 = 10'000'000;
    zero_reference_trade.fields.price.valid = true;
    zero_reference_trade.fields.buy_order_id = 0;
    zero_reference_trade.fields.sell_order_id = 22;
    zero_reference_trade.fields.validity_bitmap =
        market::kTickQuantityValidV1 |
        market::kTickPriceValidV1 |
        market::kTickSellOrderIdValidV1;
    event = zero_reference_trade;
    *ok &= Expect(
        market::ProjectShenzhenOrderEventInputV1(
            event, 2U, 2U, &projected) ==
                market::ShenzhenOrderEventProjectionV1::
                    kProjected &&
            projected.buy_order_id == 0 &&
            projected.sell_order_id == 22,
        "decoded adapter accepts documented zero 6.36 order reference");

    zero_reference_trade.fields.validity_bitmap |=
        market::kTickBuyOrderIdValidV1;
    event = std::move(zero_reference_trade);
    *ok &= Expect(
        market::ProjectShenzhenOrderEventInputV1(
            event, 3U, 3U, &projected) ==
            market::ShenzhenOrderEventProjectionV1::
                kInvalidEvent,
        "decoded adapter rejects zero reference marked valid");

    market::ShanghaiTickV1 shanghai{};
    event = std::move(shanghai);
    *ok &= Expect(
        market::ProjectShenzhenOrderEventInputV1(
            event, 1U, 1U, &projected) ==
            market::ShenzhenOrderEventProjectionV1::
                kNotShenzhenEvent,
        "decoded adapter rejects non-Shenzhen event");
}

void TestInputOrdering(bool* ok) {
    auto projector = Projector(ok);
    if (projector == nullptr) {
        return;
    }
    std::vector<market::ShenzhenOrderEventV1> events;
    const auto first = Order(
        100,
        market::SideV1::kBuy,
        market::OrderTypeV1::kLimit,
        10);
    *ok &= Expect(
        projector->Consume(first, &events) ==
            market::ShenzhenOrderProjectorConsumeErrorV1::kNone,
        "consume Shenzhen ordering baseline");
    *ok &= Expect(
        projector->Consume(first, &events) ==
                market::ShenzhenOrderProjectorConsumeErrorV1::
                    kOutOfOrderInput &&
            events.empty() && projector->order_count() == 1U,
        "duplicate ApplSeqNum/tick sequence is rejected before mutation");

    auto native_regression = Order(
        99,
        market::SideV1::kSell,
        market::OrderTypeV1::kLimit,
        10);
    native_regression.anchor.tick_stream_sequence =
        first.anchor.tick_stream_sequence + 1U;
    *ok &= Expect(
        projector->Consume(native_regression, &events) ==
                market::ShenzhenOrderProjectorConsumeErrorV1::
                    kOutOfOrderInput &&
            events.empty() && projector->order_count() == 1U,
        "ApplSeqNum regression is rejected before state mutation");
}

}  // namespace

int main() {
    bool ok = true;
    TestDirectOrders(&ok);
    TestTradeUpdatesAndMissingReferences(&ok);
    TestCancelCardinalityAndState(&ok);
    TestBorrowLendReferenceCompatibility(&ok);
    TestQuantityConflicts(&ok);
    TestKeyIsolationAndFinalization(&ok);
    TestDecodedProjection(&ok);
    TestInputOrdering(&ok);
    return ok ? 0 : 1;
}
