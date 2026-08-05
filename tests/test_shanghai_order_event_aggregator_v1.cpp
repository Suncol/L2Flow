#include "l2flow/market/shanghai_order_event_aggregator_v1.h"

#include <cstdint>
#include <iostream>
#include <limits>
#include <memory>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

namespace {

namespace market = l2flow::market;

bool Expect(bool condition, std::string_view message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << '\n';
    }
    return condition;
}

bool HasFlag(
    const market::ShanghaiOrderSnapshotV1& order,
    market::ShanghaiOrderQualityFlagV1 flag) {
    return (order.quality_flags &
            market::ShanghaiOrderQualityBitV1(flag)) != 0U;
}

market::ShanghaiOrderEventInputV1 BaseInput(
    std::int64_t native_sequence,
    market::TickActionV1 action,
    market::TradingPhaseV1 phase) {
    market::ShanghaiOrderEventInputV1 input{};
    input.trade_date = 20260730U;
    input.instrument_id = 17U;
    input.channel = 3;
    input.anchor.native_event_sequence = native_sequence;
    input.anchor.source_sequence =
        static_cast<std::uint64_t>(native_sequence);
    input.anchor.ingress_sequence =
        static_cast<std::uint64_t>(native_sequence + 100);
    input.anchor.tick_stream_sequence =
        static_cast<std::uint64_t>(native_sequence + 200);
    input.anchor.vendor_sequence_id =
        static_cast<std::uint64_t>(native_sequence + 300);
    input.anchor.event_time_ns_since_midnight =
        static_cast<std::uint64_t>(native_sequence) * 1'000'000U;
    input.anchor.event_time_unix_ns =
        1'785'340'800'000'000'000LL +
        static_cast<std::int64_t>(
            input.anchor.event_time_ns_since_midnight);
    input.anchor.recv_realtime_ns =
        input.anchor.event_time_unix_ns + 10;
    input.anchor.recv_monotonic_ns =
        static_cast<std::int64_t>(native_sequence) * 100;
    input.anchor.vendor_local_time_raw = 93'000'000U;
    input.anchor.vendor_local_time_ns_since_midnight =
        34'200'000'000'000ULL;
    input.anchor.event_time_valid = true;
    input.anchor.event_time_unix_ns_valid = true;
    input.anchor.vendor_local_time_valid = true;
    input.action = action;
    input.phase = phase;
    input.phase_valid = phase != market::TradingPhaseV1::kUnknown;
    return input;
}

market::ShanghaiOrderEventInputV1 Trade(
    std::int64_t native_sequence,
    std::int64_t buy_order_id,
    std::int64_t sell_order_id,
    market::AggressorV1 aggressor,
    market::TradingPhaseV1 phase,
    std::int64_t price_p6,
    std::int64_t quantity) {
    market::ShanghaiOrderEventInputV1 input = BaseInput(
        native_sequence, market::TickActionV1::kTrade, phase);
    input.buy_order_id = buy_order_id;
    input.sell_order_id = sell_order_id;
    input.aggressor = aggressor;
    input.price_p6 = price_p6;
    input.price_valid = true;
    input.quantity = quantity;
    input.quantity_valid = true;
    input.trade_amount_p6 = price_p6 * quantity;
    input.trade_amount_valid = true;
    return input;
}

market::ShanghaiOrderEventInputV1 Add(
    std::int64_t native_sequence,
    std::int64_t order_id,
    market::SideV1 side,
    market::TradingPhaseV1 phase,
    std::int64_t price_p6,
    std::int64_t quantity,
    std::int64_t matched_quantity,
    bool matched_quantity_valid = true) {
    market::ShanghaiOrderEventInputV1 input = BaseInput(
        native_sequence, market::TickActionV1::kAdd, phase);
    input.primary_order_id = order_id;
    input.side = side;
    input.price_p6 = price_p6;
    input.price_valid = true;
    input.quantity = quantity;
    input.quantity_valid = true;
    input.matched_quantity = matched_quantity;
    input.matched_quantity_valid = matched_quantity_valid;
    return input;
}

market::ShanghaiOrderEventInputV1 Cancel(
    std::int64_t native_sequence,
    std::int64_t order_id,
    market::SideV1 side,
    market::TradingPhaseV1 phase,
    std::int64_t quantity) {
    market::ShanghaiOrderEventInputV1 input = BaseInput(
        native_sequence, market::TickActionV1::kCancel, phase);
    input.primary_order_id = order_id;
    input.side = side;
    input.quantity = quantity;
    input.quantity_valid = true;
    return input;
}

market::ShanghaiOrderEventInputV1 Status(
    std::int64_t native_sequence,
    market::TradingPhaseV1 phase) {
    return BaseInput(
        native_sequence, market::TickActionV1::kStatus, phase);
}

std::unique_ptr<market::ShanghaiOrderEventAggregatorV1> Aggregator(
    std::size_t capacity,
    bool* ok) {
    market::ShanghaiOrderEventAggregatorConfigV1 config{};
    config.trade_date = 20260730U;
    config.maximum_order_states = capacity;
    std::unique_ptr<market::ShanghaiOrderEventAggregatorV1> result;
    *ok &= Expect(
        market::ShanghaiOrderEventAggregatorV1::Create(
            config, &result) ==
                market::ShanghaiOrderAggregatorCreateErrorV1::kNone &&
            result != nullptr,
        "create Shanghai order aggregator");
    return result;
}

const market::ShanghaiOrderRevisionEventV1* LastRevision(
    const std::vector<market::ShanghaiOrderEventV1>& events) {
    for (auto event = events.rbegin();
         event != events.rend();
         ++event) {
        const auto* revision =
            std::get_if<market::ShanghaiOrderRevisionEventV1>(
                &*event);
        if (revision != nullptr) {
            return revision;
        }
    }
    return nullptr;
}

market::ShanghaiOrderSnapshotV1 ReadOrder(
    const market::ShanghaiOrderEventAggregatorV1& aggregator,
    std::int64_t order_id,
    bool* ok) {
    market::ShanghaiOrderSnapshotV1 order{};
    const market::ShanghaiOrderKeyV1 key{
        20260730U, 17U, 3, order_id};
    *ok &= Expect(
        aggregator.GetOrder(key, &order) ==
            market::ShanghaiOrderAggregatorQueryErrorV1::kNone,
        "read reconstructed order");
    return order;
}

bool Consume(
    market::ShanghaiOrderEventAggregatorV1* aggregator,
    const market::ShanghaiOrderEventInputV1& input,
    std::vector<market::ShanghaiOrderEventV1>* events,
    std::string_view message) {
    return Expect(
        aggregator->Consume(input, events) ==
            market::ShanghaiOrderAggregatorConsumeErrorV1::kNone,
        message);
}

}  // namespace

int main() {
    bool ok = true;
    std::vector<market::ShanghaiOrderEventV1> events;

    {
        std::unique_ptr<market::ShanghaiOrderEventAggregatorV1>
            aggregator = Aggregator(32U, &ok);
        if (aggregator == nullptr) {
            return 1;
        }
        ok &= Consume(
            aggregator.get(),
            Trade(
                1,
                100,
                900,
                market::AggressorV1::kBuy,
                market::TradingPhaseV1::kContinuous,
                10'000'000,
                3),
            &events,
            "consume first pre-A buy trade");
        const auto* first_trade =
            events.empty()
                ? nullptr
                : std::get_if<market::ShanghaiTradeEventV1>(
                      &events.front());
        const auto* first_revision = LastRevision(events);
        ok &= Expect(
            events.size() == 2U && first_trade != nullptr &&
                first_trade->trade_amount_valid &&
                first_trade->trade_amount_p6 == 30'000'000 &&
                first_trade->source_anchor.event_time_unix_ns_valid &&
                first_trade->source_anchor.vendor_local_time_valid,
            "preserve first source trade amount and time anchors");
        ok &= Expect(
            first_revision != nullptr &&
                first_revision->operation ==
                    market::ShanghaiOrderDeltaOperationV1::kInsert &&
                first_revision->order.original_quantity == 3 &&
                first_revision->order.original_quantity_status ==
                    market::ShanghaiOriginalQuantityStatusV1::
                        kLowerBound &&
                first_revision->order.price_p6 == 10'000'000 &&
                first_revision->order.price_source ==
                    market::ShanghaiOrderPriceSourceV1::
                        kBuyMaximumExecution &&
                !first_revision->order.apply_to_book,
            "first T creates a provisional buy lower-bound order");

        ok &= Consume(
            aggregator.get(),
            Trade(
                2,
                100,
                901,
                market::AggressorV1::kBuy,
                market::TradingPhaseV1::kContinuous,
                11'000'000,
                2),
            &events,
            "consume second pre-A buy trade");
        market::ShanghaiOrderSnapshotV1 order =
            ReadOrder(*aggregator, 100, &ok);
        ok &= Expect(
            order.revision == 2U &&
                order.total_trade_quantity == 5 &&
                order.original_quantity == 5 &&
                order.price_p6 == 11'000'000,
            "T-only buy aggregates all fills and maximum price");

        ok &= Consume(
            aggregator.get(),
            Add(
                3,
                100,
                market::SideV1::kBuy,
                market::TradingPhaseV1::kContinuous,
                12'000'000,
                5,
                5),
            &events,
            "consume matching A");
        order = ReadOrder(*aggregator, 100, &ok);
        const auto* add_revision = LastRevision(events);
        ok &= Expect(
            add_revision != nullptr &&
                add_revision->operation ==
                    market::ShanghaiOrderDeltaOperationV1::kUpdate &&
                order.revision == 3U && order.add_seen &&
                order.apply_to_book &&
                order.original_quantity == 10 &&
                order.original_quantity_status ==
                    market::ShanghaiOriginalQuantityStatusV1::kExact &&
                order.observed_pre_add_trade_quantity == 5 &&
                order.source_matched_quantity == 5 &&
                order.remaining_quantity == 5 &&
                order.price_p6 == 12'000'000 &&
                order.price_source ==
                    market::ShanghaiOrderPriceSourceV1::kSourceAdd &&
                !HasFlag(
                    order,
                    market::ShanghaiOrderQualityFlagV1::
                        kPrematchQuantityMismatch),
            "A uses Qty+source matched and does not subtract pre-A fills");

        ok &= Consume(
            aggregator.get(),
            Trade(
                4,
                100,
                902,
                market::AggressorV1::kNeutral,
                market::TradingPhaseV1::kContinuous,
                9'000'000,
                2),
            &events,
            "consume post-A neutral trade");
        order = ReadOrder(*aggregator, 100, &ok);
        ok &= Expect(
            order.original_quantity == 10 &&
                order.remaining_quantity == 3 &&
                order.post_add_trade_quantity == 2 &&
                order.total_trade_quantity == 7,
            "post-A trade decrements only published remaining quantity");

        ok &= Consume(
            aggregator.get(),
            Cancel(
                5,
                100,
                market::SideV1::kBuy,
                market::TradingPhaseV1::kContinuous,
                3),
            &events,
            "consume final cancel");
        order = ReadOrder(*aggregator, 100, &ok);
        const auto* cancel_revision = LastRevision(events);
        ok &= Expect(
            events.size() == 2U && cancel_revision != nullptr &&
                cancel_revision->operation ==
                    market::ShanghaiOrderDeltaOperationV1::kFinalize &&
                order.revision == 5U &&
                order.remaining_quantity == 0 &&
                order.total_cancel_quantity == 3 &&
                order.finality ==
                    market::ShanghaiOrderFinalityV1::kFinal,
            "cancel finalizes exact zero balance");
    }

    {
        std::unique_ptr<market::ShanghaiOrderEventAggregatorV1>
            aggregator = Aggregator(16U, &ok);
        ok &= Consume(
            aggregator.get(),
            Trade(
                10,
                700,
                500,
                market::AggressorV1::kSell,
                market::TradingPhaseV1::kContinuous,
                10'000'000,
                2),
            &events,
            "consume first T-only sell trade");
        ok &= Consume(
            aggregator.get(),
            Trade(
                11,
                701,
                500,
                market::AggressorV1::kSell,
                market::TradingPhaseV1::kContinuous,
                9'000'000,
                3),
            &events,
            "consume second T-only sell trade");
        market::ShanghaiOrderSnapshotV1 order =
            ReadOrder(*aggregator, 500, &ok);
        ok &= Expect(
            order.original_quantity == 5 &&
                order.original_quantity_status ==
                    market::ShanghaiOriginalQuantityStatusV1::
                        kLowerBound &&
                order.price_p6 == 9'000'000 &&
                order.price_source ==
                    market::ShanghaiOrderPriceSourceV1::
                        kSellMinimumExecution &&
                order.finality ==
                    market::ShanghaiOrderFinalityV1::kProvisional,
            "T-only sell uses fill sum and minimum execution boundary");
        ok &= Consume(
            aggregator.get(),
            Status(12, market::TradingPhaseV1::kEnd),
            &events,
            "consume ENDTR status");
        order = ReadOrder(*aggregator, 500, &ok);
        ok &= Expect(
            !events.empty() &&
                std::holds_alternative<
                    market::ShanghaiStatusEventV1>(
                    events.front()) &&
                order.revision == 3U &&
                order.finality ==
                    market::ShanghaiOrderFinalityV1::kFinal &&
                HasFlag(
                    order,
                    market::ShanghaiOrderQualityFlagV1::
                        kSyntheticOrder) &&
                HasFlag(
                    order,
                    market::ShanghaiOrderQualityFlagV1::
                        kOriginalQuantityLowerBound),
            "ENDTR finalizes T-only order without upgrading exactness");
    }

    {
        std::unique_ptr<market::ShanghaiOrderEventAggregatorV1>
            aggregator = Aggregator(16U, &ok);
        ok &= Consume(
            aggregator.get(),
            Trade(
                20,
                800,
                900,
                market::AggressorV1::kNeutral,
                market::TradingPhaseV1::kContinuous,
                10'000'000,
                1),
            &events,
            "consume N trade");
        ok &= Expect(
            events.size() == 1U &&
                std::holds_alternative<
                    market::ShanghaiTradeEventV1>(events.front()) &&
                aggregator->order_count() == 0U,
            "N trade is preserved but does not guess an aggressor order");

        ok &= Consume(
            aggregator.get(),
            Trade(
                21,
                801,
                901,
                market::AggressorV1::kBuy,
                market::TradingPhaseV1::kOpeningCall,
                10'000'000,
                1),
            &events,
            "consume opening-call trade");
        ok &= Expect(
            events.size() == 1U && aggregator->order_count() == 0U,
            "non-continuous trade does not create T-only order");
    }

    {
        std::unique_ptr<market::ShanghaiOrderEventAggregatorV1>
            aggregator = Aggregator(16U, &ok);
        const market::TradingPhaseV1 direct_phases[] = {
            market::TradingPhaseV1::kOpeningCall,
            market::TradingPhaseV1::kSuspended,
            market::TradingPhaseV1::kClosingCall};
        std::int64_t sequence = 30;
        std::int64_t order_id = 1'000;
        for (market::TradingPhaseV1 phase : direct_phases) {
            ok &= Consume(
                aggregator.get(),
                Add(
                    sequence,
                    order_id,
                    market::SideV1::kBuy,
                    phase,
                    10'000'000,
                    100,
                    40),
                &events,
                "consume delayed call/suspension A");
            const market::ShanghaiOrderSnapshotV1 order =
                ReadOrder(*aggregator, order_id, &ok);
            ok &= Expect(
                order.original_quantity == 100 &&
                    order.original_quantity_status ==
                        market::ShanghaiOriginalQuantityStatusV1::
                            kExact &&
                    order.remaining_quantity == 100,
                "OCALL/SUSP/CCALL A.Qty is already original quantity");
            ++sequence;
            ++order_id;
        }

        market::ShanghaiOrderEventInputV1 unknown = Add(
            sequence,
            order_id,
            market::SideV1::kBuy,
            market::TradingPhaseV1::kUnknown,
            10'000'000,
            100,
            40);
        unknown.phase_valid = false;
        ok &= Consume(
            aggregator.get(),
            unknown,
            &events,
            "consume unknown-phase A");
        market::ShanghaiOrderSnapshotV1 order =
            ReadOrder(*aggregator, order_id, &ok);
        ok &= Expect(
            order.original_quantity == 100 &&
                order.original_quantity_status ==
                    market::ShanghaiOriginalQuantityStatusV1::
                        kLowerBound &&
                order.finality ==
                    market::ShanghaiOrderFinalityV1::kConflict &&
                HasFlag(
                    order,
                    market::ShanghaiOrderQualityFlagV1::
                        kPhaseUnknown),
            "unknown phase never applies continuous Qty+matched formula");

        market::ShanghaiOrderEventInputV1 undocumented = Add(
            sequence + 1,
            order_id + 1,
            market::SideV1::kBuy,
            market::TradingPhaseV1::kContinuous,
            10'000'000,
            100,
            0);
        undocumented.phase =
            static_cast<market::TradingPhaseV1>(255U);
        ok &= Expect(
            aggregator->Consume(undocumented, &events) ==
                    market::ShanghaiOrderAggregatorConsumeErrorV1::
                        kInvalidInput &&
                events.empty(),
            "undocumented phase enum is rejected");
    }

    {
        std::unique_ptr<market::ShanghaiOrderEventAggregatorV1>
            aggregator = Aggregator(16U, &ok);
        ok &= Consume(
            aggregator.get(),
            Trade(
                40,
                2'000,
                2'100,
                market::AggressorV1::kBuy,
                market::TradingPhaseV1::kContinuous,
                10'000'000,
                4),
            &events,
            "consume mismatch pre-A trade");
        ok &= Consume(
            aggregator.get(),
            Add(
                41,
                2'000,
                market::SideV1::kBuy,
                market::TradingPhaseV1::kContinuous,
                11'000'000,
                6,
                3),
            &events,
            "consume mismatch A");
        market::ShanghaiOrderSnapshotV1 order =
            ReadOrder(*aggregator, 2'000, &ok);
        ok &= Expect(
            order.original_quantity == 9 &&
                order.original_quantity_status ==
                    market::ShanghaiOriginalQuantityStatusV1::kExact &&
                order.observed_pre_add_trade_quantity == 4 &&
                order.source_matched_quantity == 3 &&
                HasFlag(
                    order,
                    market::ShanghaiOrderQualityFlagV1::
                        kPrematchQuantityMismatch) &&
                order.finality ==
                    market::ShanghaiOrderFinalityV1::kConflict,
            "observed pre-A quantity validates but never replaces source");

        std::unique_ptr<market::ShanghaiOrderEventAggregatorV1>
            missing_matched = Aggregator(32U, &ok);
        if (missing_matched != nullptr) {
            ok &= Consume(
                missing_matched.get(),
                Trade(
                    3,
                    501,
                    901,
                    market::AggressorV1::kBuy,
                    market::TradingPhaseV1::kContinuous,
                    10'000'000,
                    5),
                &events,
                "consume pre-A trade with later unavailable matched quantity");
            ok &= Consume(
                missing_matched.get(),
                Add(
                    4,
                    501,
                    market::SideV1::kBuy,
                    market::TradingPhaseV1::kContinuous,
                    10'100'000,
                    5,
                    0,
                    false),
                &events,
                "consume A with unavailable matched quantity");
            const market::ShanghaiOrderSnapshotV1 lower_bound =
                ReadOrder(*missing_matched, 501, &ok);
            ok &= Expect(
                lower_bound.original_quantity == 10 &&
                    lower_bound.original_quantity_status ==
                        market::ShanghaiOriginalQuantityStatusV1::
                            kLowerBound &&
                    (lower_bound.quality_flags &
                     market::ShanghaiOrderQualityBitV1(
                         market::ShanghaiOrderQualityFlagV1::
                             kPrematchQuantityUnavailable)) != 0U,
                "missing matched quantity uses A residual plus observed pre-A fills as lower bound");
        }

        ok &= Consume(
            aggregator.get(),
            Trade(
                42,
                2'000,
                2'101,
                market::AggressorV1::kNeutral,
                market::TradingPhaseV1::kContinuous,
                10'000'000,
                7),
            &events,
            "consume negative-balance trade");
        order = ReadOrder(*aggregator, 2'000, &ok);
        ok &= Expect(
            order.remaining_quantity == -1 &&
                !order.remaining_quantity_valid &&
                HasFlag(
                    order,
                    market::ShanghaiOrderQualityFlagV1::
                        kQuantityConflict),
            "negative balance is retained as conflict and never clamped");
    }

    {
        std::unique_ptr<market::ShanghaiOrderEventAggregatorV1>
            aggregator = Aggregator(4U, &ok);
        market::ShanghaiOrderEventInputV1 overflow = Add(
            50,
            3'000,
            market::SideV1::kBuy,
            market::TradingPhaseV1::kContinuous,
            10'000'000,
            std::numeric_limits<std::int64_t>::max() - 1,
            2);
        ok &= Expect(
            aggregator->Consume(overflow, &events) ==
                    market::ShanghaiOrderAggregatorConsumeErrorV1::
                        kNumericOverflow &&
                events.empty() && aggregator->order_count() == 0U,
            "Qty+matched overflow is rejected before state mutation");
    }

    {
        market::ShanghaiTickV1 tick{};
        tick.common.kind =
            market::MarketEventKindV1::kShanghaiTick;
        tick.common.market = market::MarketV1::kShanghai;
        tick.common.origin.trade_date = 20260730U;
        tick.common.origin.source_sequence = 60U;
        tick.common.origin.vendor_sequence_id = 600U;
        tick.common.origin.vendor_local_time_raw = 93'000'001U;
        tick.common.origin.recv_realtime_ns = 1'000;
        tick.common.origin.recv_monotonic_ns = 2'000;
        tick.common.instrument_id = 17U;
        tick.common.exchange_time.valid = true;
        tick.common.exchange_time.unix_nanoseconds_valid = true;
        tick.common.exchange_time.nanoseconds_since_midnight = 123U;
        tick.common.exchange_time.unix_nanoseconds = 456;
        tick.common.vendor_local_time.valid = true;
        tick.common.vendor_local_time.nanoseconds_since_midnight = 789U;
        tick.business_index = 60;
        tick.channel = 3;
        tick.fields.action = market::TickActionV1::kTrade;
        tick.fields.aggressor = market::AggressorV1::kBuy;
        tick.fields.phase = market::TradingPhaseV1::kContinuous;
        tick.fields.buy_order_id = 4'000;
        tick.fields.sell_order_id = 4'001;
        tick.fields.price.valid = true;
        tick.fields.price.normalized_p6 = 10'000'000;
        tick.fields.quantity.valid = true;
        tick.fields.quantity.raw = 5;
        tick.fields.quantity.scale = 0U;
        tick.fields.trade_amount.valid = true;
        tick.fields.trade_amount.normalized_p6 = 50'000'123;
        tick.fields.validity_bitmap =
            market::kTickPriceValidV1 |
            market::kTickQuantityValidV1 |
            market::kTickTradeAmountValidV1 |
            market::kTickExchangeTimeValidV1 |
            market::kTickAggressorValidV1 |
            market::kTickPhaseValidV1;
        market::DecodedMarketEventV1 decoded(std::move(tick));
        market::ShanghaiOrderEventInputV1 projected{};
        ok &= Expect(
            market::ProjectShanghaiOrderEventInputV1(
                decoded, 160U, 260U, &projected) ==
                market::ShanghaiOrderEventProjectionV1::kProjected,
            "project decoded Shanghai tick");
        ok &= Expect(
            projected.trade_amount_valid &&
                projected.trade_amount_p6 == 50'000'123 &&
                projected.anchor.ingress_sequence == 160U &&
                projected.anchor.tick_stream_sequence == 260U &&
                projected.anchor.event_time_unix_ns == 456 &&
                projected.anchor.vendor_local_time_raw == 93'000'001U &&
                projected.anchor.vendor_local_time_ns_since_midnight ==
                    789U,
            "decoded projection preserves amount and all time anchors");
    }

    {
        std::unique_ptr<market::ShanghaiOrderEventAggregatorV1>
            aggregator = Aggregator(4U, &ok);
        ok &= Consume(
            aggregator.get(),
            Trade(
                70,
                5'000,
                5'001,
                market::AggressorV1::kBuy,
                market::TradingPhaseV1::kContinuous,
                10'000'000,
                1),
            &events,
            "consume order before explicit finalization");
        market::ShanghaiOrderSourceAnchorV1 boundary{};
        market::ShanghaiOrderSourceAnchorV1 malformed_boundary{};
        malformed_boundary.native_event_sequence = 1;
        ok &= Expect(
            aggregator->Finalize(malformed_boundary, &events) ==
                    market::ShanghaiOrderAggregatorConsumeErrorV1::
                        kInvalidInput &&
                events.empty() && !aggregator->finalized(),
            "partial source boundary anchor is rejected");
        ok &= Expect(
            aggregator->Finalize(boundary, &events) ==
                    market::ShanghaiOrderAggregatorConsumeErrorV1::
                        kNone &&
                aggregator->finalized(),
            "explicit clean boundary finalizes and seals aggregator");
        ok &= Expect(
            aggregator->Consume(
                Status(71, market::TradingPhaseV1::kEnd),
                &events) ==
                market::ShanghaiOrderAggregatorConsumeErrorV1::
                    kAlreadyFinalized,
            "sealed aggregator rejects later input");
    }

    {
        std::unique_ptr<market::ShanghaiOrderEventAggregatorV1>
            aggregator = Aggregator(4U, &ok);
        market::ShanghaiOrderEventInputV1 first = Trade(
            70,
            7'000,
            7'100,
            market::AggressorV1::kBuy,
            market::TradingPhaseV1::kContinuous,
            10'000'000,
            2);
        ok &= Consume(
            aggregator.get(),
            first,
            &events,
            "consume ordering baseline");
        const market::ShanghaiOrderSnapshotV1 before =
            ReadOrder(*aggregator, 7'000, &ok);
        ok &= Expect(
            aggregator->Consume(first, &events) ==
                    market::ShanghaiOrderAggregatorConsumeErrorV1::
                        kOutOfOrderInput &&
                events.empty(),
            "duplicate tick/native sequence is rejected before mutation");

        market::ShanghaiOrderEventInputV1 native_regression =
            Trade(
                69,
                7'000,
                7'101,
                market::AggressorV1::kBuy,
                market::TradingPhaseV1::kContinuous,
                11'000'000,
                3);
        native_regression.anchor.tick_stream_sequence =
            first.anchor.tick_stream_sequence + 1U;
        ok &= Expect(
            aggregator->Consume(native_regression, &events) ==
                    market::ShanghaiOrderAggregatorConsumeErrorV1::
                        kOutOfOrderInput &&
                events.empty(),
            "BizIndex regression is rejected before mutation");
        const market::ShanghaiOrderSnapshotV1 after =
            ReadOrder(*aggregator, 7'000, &ok);
        ok &= Expect(
            after.revision == before.revision &&
                after.total_trade_quantity ==
                    before.total_trade_quantity,
            "rejected ordering violations cannot double-count quantity");
    }

    {
        std::unique_ptr<market::ShanghaiOrderEventAggregatorV1>
            aggregator = Aggregator(4U, &ok);
        market::ShanghaiOrderEventInputV1 first = Add(
            90,
            9'000,
            market::SideV1::kBuy,
            market::TradingPhaseV1::kContinuous,
            10'000'000,
            10,
            0);
        first.anchor.source_sequence = 11'000U;
        first.anchor.ingress_sequence = 12'000U;
        first.anchor.tick_stream_sequence = 13'000U;
        ok &= Expect(
            aggregator->ConsumeCanonical(first, 1U, &events) ==
                    market::ShanghaiOrderAggregatorConsumeErrorV1::
                        kNone &&
                events.size() == 1U,
            "consume first Shanghai canonical input");

        market::ShanghaiOrderEventInputV1 second = Add(
            91,
            9'001,
            market::SideV1::kSell,
            market::TradingPhaseV1::kContinuous,
            10'100'000,
            20,
            0);
        second.anchor.source_sequence = 11'001U;
        second.anchor.ingress_sequence = 12'001U;
        second.anchor.tick_stream_sequence = 12'999U;
        ok &= Expect(
            second.anchor.tick_stream_sequence <
                    first.anchor.tick_stream_sequence &&
                aggregator->ConsumeCanonical(
                    second, 2U, &events) ==
                    market::ShanghaiOrderAggregatorConsumeErrorV1::
                        kNone &&
                events.size() == 1U,
            "monotonic Shanghai canonical order accepts regressed arrival tick sequence");
        const auto* canonical_revision =
            events.empty()
                ? nullptr
                : std::get_if<
                      market::ShanghaiOrderRevisionEventV1>(
                      &events.front());
        ok &= Expect(
            canonical_revision != nullptr &&
                canonical_revision->source_anchor.source_sequence ==
                    second.anchor.source_sequence &&
                canonical_revision->source_anchor.ingress_sequence ==
                    second.anchor.ingress_sequence &&
                canonical_revision->source_anchor
                        .tick_stream_sequence ==
                    second.anchor.tick_stream_sequence &&
                canonical_revision->order.first_anchor
                        .source_sequence ==
                    second.anchor.source_sequence &&
                canonical_revision->order.first_anchor
                        .ingress_sequence ==
                    second.anchor.ingress_sequence &&
                canonical_revision->order.first_anchor
                        .tick_stream_sequence ==
                    second.anchor.tick_stream_sequence,
            "Shanghai canonical output preserves original source, ingress, and tick anchors");

        market::ShanghaiOrderEventInputV1 blocked = Add(
            92,
            9'002,
            market::SideV1::kBuy,
            market::TradingPhaseV1::kContinuous,
            10'200'000,
            30,
            0);
        blocked.anchor.tick_stream_sequence = 12'998U;
        ok &= Expect(
            aggregator->ConsumeCanonical(
                blocked, 2U, &events) ==
                    market::ShanghaiOrderAggregatorConsumeErrorV1::
                        kOutOfOrderInput &&
                events.empty() && aggregator->order_count() == 2U,
            "duplicate Shanghai canonical sequence is rejected before mutation");
        ok &= Expect(
            aggregator->ConsumeCanonical(
                blocked, 1U, &events) ==
                    market::ShanghaiOrderAggregatorConsumeErrorV1::
                        kOutOfOrderInput &&
                events.empty() && aggregator->order_count() == 2U,
            "regressed Shanghai canonical sequence is rejected before mutation");
    }

    {
        std::unique_ptr<market::ShanghaiOrderEventAggregatorV1>
            aggregator = Aggregator(4U, &ok);
        ok &= Consume(
            aggregator.get(),
            Trade(
                80,
                8'000,
                8'100,
                market::AggressorV1::kBuy,
                market::TradingPhaseV1::kContinuous,
                10'000'000,
                2),
            &events,
            "consume T-only state before unexpected cancel");
        ok &= Consume(
            aggregator.get(),
            Cancel(
                81,
                8'000,
                market::SideV1::kBuy,
                market::TradingPhaseV1::kContinuous,
                1),
            &events,
            "consume cancel without a published A");
        const market::ShanghaiOrderSnapshotV1 order =
            ReadOrder(*aggregator, 8'000, &ok);
        ok &= Expect(
            HasFlag(
                order,
                market::ShanghaiOrderQualityFlagV1::
                    kCancelWithoutAdd) &&
                order.finality ==
                    market::ShanghaiOrderFinalityV1::kConflict,
            "cancel without A is conflict-audited and never treated as a clean T-only finalization");
    }

    {
        std::unique_ptr<market::ShanghaiOrderEventAggregatorV1>
            aggregator = Aggregator(8U, &ok);
        market::ShanghaiOrderEventInputV1 other_instrument = Add(
            100,
            900,
            market::SideV1::kBuy,
            market::TradingPhaseV1::kContinuous,
            10'000'000,
            10,
            0);
        other_instrument.instrument_id = 18U;
        market::ShanghaiOrderEventInputV1 target_high = Add(
            101,
            300,
            market::SideV1::kSell,
            market::TradingPhaseV1::kContinuous,
            10'100'000,
            20,
            0);
        market::ShanghaiOrderEventInputV1 target_low = Add(
            102,
            100,
            market::SideV1::kBuy,
            market::TradingPhaseV1::kContinuous,
            9'900'000,
            30,
            0);
        market::ShanghaiOrderEventInputV1 other_channel = Add(
            103,
            50,
            market::SideV1::kBuy,
            market::TradingPhaseV1::kContinuous,
            9'800'000,
            40,
            0);
        other_channel.channel = 4;
        ok &= Consume(
            aggregator.get(),
            other_instrument,
            &events,
            "consume END-range other instrument");
        ok &= Consume(
            aggregator.get(),
            target_high,
            &events,
            "consume END-range high target order");
        ok &= Consume(
            aggregator.get(),
            target_low,
            &events,
            "consume END-range low target order");
        ok &= Consume(
            aggregator.get(),
            other_channel,
            &events,
            "consume END-range other channel");

        const market::ShanghaiOrderEventInputV1 end =
            Status(104, market::TradingPhaseV1::kEnd);
        std::size_t maximum_output = 0U;
        ok &= Expect(
            aggregator->MaximumOutputForInput(
                end, &maximum_output) ==
                    market::ShanghaiOrderAggregatorConsumeErrorV1::
                        kNone &&
                maximum_output == 3U,
            "END maximum output counts only unfinished orders in the exact instrument/channel range");
        ok &= Consume(
            aggregator.get(),
            end,
            &events,
            "consume exact instrument-range END");
        const auto* first_revision =
            events.size() > 1U
                ? std::get_if<
                      market::ShanghaiOrderRevisionEventV1>(
                      &events[1U])
                : nullptr;
        const auto* second_revision =
            events.size() > 2U
                ? std::get_if<
                      market::ShanghaiOrderRevisionEventV1>(
                      &events[2U])
                : nullptr;
        ok &= Expect(
            events.size() == 3U &&
                std::holds_alternative<
                    market::ShanghaiStatusEventV1>(events[0U]) &&
                first_revision != nullptr &&
                second_revision != nullptr &&
                first_revision->order.key.order_id == 100 &&
                second_revision->order.key.order_id == 300,
            "END finalizes only its range in ascending OrderKey order");

        market::ShanghaiOrderSnapshotV1 untouched{};
        ok &= Expect(
            aggregator->GetOrder(
                {20260730U, 18U, 3, 900}, &untouched) ==
                    market::ShanghaiOrderAggregatorQueryErrorV1::
                        kNone &&
                untouched.finality ==
                    market::ShanghaiOrderFinalityV1::kProvisional &&
                aggregator->GetOrder(
                    {20260730U, 17U, 4, 50}, &untouched) ==
                    market::ShanghaiOrderAggregatorQueryErrorV1::
                        kNone &&
                untouched.finality ==
                    market::ShanghaiOrderFinalityV1::kProvisional,
            "END leaves other instruments and channels provisional");

        const market::ShanghaiOrderEventInputV1 repeated_end =
            Status(105, market::TradingPhaseV1::kEnd);
        maximum_output = 0U;
        ok &= Expect(
            aggregator->MaximumOutputForInput(
                repeated_end, &maximum_output) ==
                    market::ShanghaiOrderAggregatorConsumeErrorV1::
                        kNone &&
                maximum_output == 1U,
            "repeated END upper bound excludes already-finalized revisions");
    }

    return ok ? 0 : 1;
}
