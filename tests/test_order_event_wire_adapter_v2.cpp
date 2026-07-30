#include "l2flow/ipc/order_event_wire_adapter_v2.h"

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <limits>
#include <memory>
#include <string_view>
#include <variant>
#include <vector>

namespace {

namespace ipc = l2flow::ipc;
namespace market = l2flow::market;

bool Expect(bool condition, std::string_view message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << '\n';
    }
    return condition;
}

void FillCommon(
    ipc::RealtimeWireTickPayloadV2* payload,
    market::MarketEventKindV1 kind,
    market::MarketV1 market_value,
    std::uint8_t source_slot) {
    payload->common.record_schema_version = 2U;
    payload->common.record_bytes =
        sizeof(ipc::RealtimeWireTickPayloadV2);
    payload->common.instrument_id = 17U;
    payload->common.ordinal = 16U;
    payload->common.source_sequence = 100U;
    payload->common.ingress_sequence = 200U;
    payload->common.tick_stream_sequence = 150U;
    payload->common.vendor_sequence_id = 300U;
    payload->common.event_time_unix_ns =
        1'785'375'000'000'000'000LL;
    payload->common.recv_realtime_ns =
        1'785'375'000'000'000'100LL;
    payload->common.recv_monotonic_ns = 500U;
    payload->common.exchange_time_ns_since_midnight =
        34'200'000'000'000ULL;
    payload->common.quality_flags = 0x12U;
    payload->common.market_notices = 0x34U;
    payload->common.source_stream_id = 9U;
    payload->common.trade_date = 20260730U;
    payload->common.vendor_local_time_raw = 93'000'001U;
    payload->common.source_slot = source_slot;
    payload->common.event_kind =
        static_cast<std::uint8_t>(kind);
    payload->common.market =
        static_cast<std::uint8_t>(market_value);
    payload->common.quantity_unit =
        static_cast<std::uint8_t>(
            market::QuantityUnitV1::kShare);
    payload->common.security_type =
        static_cast<std::uint8_t>(
            market::SecurityTypeV1::kEquity);
    payload->common.asset_scope =
        static_cast<std::uint8_t>(
            market::AssetScopeV1::kDocumentedCore);
}

void SetDecimal(
    ipc::RealtimeWireDecimalV2* value,
    std::int64_t raw,
    std::uint8_t scale,
    bool valid) {
    value->raw = raw;
    value->scale = scale;
    value->valid = valid ? 1U : 0U;
    if (!valid) {
        value->normalized_p6 = 0;
        return;
    }
    std::int64_t multiplier = 1;
    for (std::uint8_t index = scale; index < 6U; ++index) {
        multiplier *= 10;
    }
    value->normalized_p6 = raw * multiplier;
}

void SetQuantity(
    ipc::RealtimeWireQuantityV2* value,
    std::int64_t raw,
    bool valid) {
    value->raw = raw;
    value->scale = 0U;
    value->valid = valid ? 1U : 0U;
}

void SetRaw(
    std::array<std::uint8_t, 32U>* output,
    std::uint8_t* length,
    std::string_view value) {
    output->fill(0U);
    std::transform(
        value.begin(),
        value.end(),
        output->begin(),
        [](char character) {
            return static_cast<std::uint8_t>(
                static_cast<unsigned char>(character));
        });
    *length = static_cast<std::uint8_t>(value.size());
}

ipc::RealtimeWireTickPayloadV2 ShanghaiTrade() {
    ipc::RealtimeWireTickPayloadV2 payload{};
    FillCommon(
        &payload,
        market::MarketEventKindV1::kShanghaiTick,
        market::MarketV1::kShanghai,
        1U);
    payload.channel = 3;
    payload.native_event_sequence = 77;
    payload.action =
        static_cast<std::uint8_t>(
            market::TickActionV1::kTrade);
    payload.aggressor =
        static_cast<std::uint8_t>(
            market::AggressorV1::kBuy);
    payload.phase =
        static_cast<std::uint8_t>(
            market::TradingPhaseV1::kContinuous);
    SetDecimal(&payload.price, 10'000, 3U, true);
    SetQuantity(&payload.quantity, 5, true);
    SetDecimal(&payload.trade_amount, 50'000, 3U, true);
    SetQuantity(&payload.matched_quantity, 0, false);
    payload.buy_order_id = 1'000;
    payload.sell_order_id = 2'000;
    payload.validity_bitmap =
        market::kTickPriceValidV1 |
        market::kTickQuantityValidV1 |
        market::kTickTradeAmountValidV1 |
        market::kTickBuyOrderIdValidV1 |
        market::kTickSellOrderIdValidV1 |
        market::kTickExchangeTimeValidV1 |
        market::kTickAggressorValidV1 |
        market::kTickPhaseValidV1;
    SetRaw(
        &payload.raw_type,
        &payload.raw_type_length,
        "T");
    SetRaw(
        &payload.raw_tick_flag,
        &payload.raw_tick_flag_length,
        "B");
    return payload;
}

ipc::RealtimeWireTickPayloadV2 ShanghaiAdd() {
    ipc::RealtimeWireTickPayloadV2 payload{};
    FillCommon(
        &payload,
        market::MarketEventKindV1::kShanghaiTick,
        market::MarketV1::kShanghai,
        1U);
    payload.channel = 3;
    payload.native_event_sequence = 78;
    payload.action =
        static_cast<std::uint8_t>(
            market::TickActionV1::kAdd);
    payload.side =
        static_cast<std::uint8_t>(market::SideV1::kBuy);
    payload.phase =
        static_cast<std::uint8_t>(
            market::TradingPhaseV1::kContinuous);
    SetDecimal(&payload.price, 10'100, 3U, true);
    SetQuantity(&payload.quantity, 10, true);
    // For Shanghai A, source TradeMoney is retained as an invalid amount
    // while its exact p3 integer value yields scale-zero matched quantity.
    SetDecimal(&payload.trade_amount, 7'000, 3U, false);
    SetQuantity(&payload.matched_quantity, 7, true);
    payload.primary_order_id = 1'000;
    payload.buy_order_id = 1'000;
    payload.validity_bitmap =
        market::kTickPriceValidV1 |
        market::kTickQuantityValidV1 |
        market::kTickMatchedQuantityValidV1 |
        market::kTickPrimaryOrderIdValidV1 |
        market::kTickExchangeTimeValidV1 |
        market::kTickSideValidV1 |
        market::kTickPhaseValidV1;
    SetRaw(
        &payload.raw_type,
        &payload.raw_type_length,
        "A");
    SetRaw(
        &payload.raw_tick_flag,
        &payload.raw_tick_flag_length,
        "B");
    return payload;
}

ipc::RealtimeWireTickPayloadV2 ShenzhenLimitOrder() {
    ipc::RealtimeWireTickPayloadV2 payload{};
    FillCommon(
        &payload,
        market::MarketEventKindV1::kShenzhenOrder,
        market::MarketV1::kShenzhen,
        3U);
    payload.channel = 7;
    payload.native_event_sequence = 1'001;
    payload.source_raw_code_1 = 49;
    payload.source_raw_code_2 = 50;
    payload.action =
        static_cast<std::uint8_t>(
            market::TickActionV1::kAdd);
    payload.side =
        static_cast<std::uint8_t>(market::SideV1::kBuy);
    payload.order_type =
        static_cast<std::uint8_t>(
            market::OrderTypeV1::kLimit);
    SetDecimal(&payload.price, 100'000, 4U, true);
    SetQuantity(&payload.quantity, 20, true);
    SetDecimal(&payload.trade_amount, 0, 0U, false);
    SetQuantity(&payload.matched_quantity, 0, false);
    payload.primary_order_id = 1'001;
    payload.validity_bitmap =
        market::kTickPriceValidV1 |
        market::kTickQuantityValidV1 |
        market::kTickPrimaryOrderIdValidV1 |
        market::kTickExchangeTimeValidV1 |
        market::kTickSideValidV1 |
        market::kTickOrderTypeValidV1;
    return payload;
}

ipc::RealtimeWireTickPayloadV2 ShenzhenTrade() {
    ipc::RealtimeWireTickPayloadV2 payload{};
    FillCommon(
        &payload,
        market::MarketEventKindV1::kShenzhenTransaction,
        market::MarketV1::kShenzhen,
        3U);
    payload.channel = 7;
    payload.native_event_sequence = 1'002;
    payload.source_raw_code_1 = 70;
    payload.action =
        static_cast<std::uint8_t>(
            market::TickActionV1::kTrade);
    SetDecimal(&payload.price, 99'900, 4U, true);
    SetQuantity(&payload.quantity, 3, true);
    SetDecimal(&payload.trade_amount, 0, 0U, false);
    SetQuantity(&payload.matched_quantity, 0, false);
    payload.buy_order_id = 1'001;
    payload.sell_order_id = 2'001;
    payload.validity_bitmap =
        market::kTickPriceValidV1 |
        market::kTickQuantityValidV1 |
        market::kTickBuyOrderIdValidV1 |
        market::kTickSellOrderIdValidV1 |
        market::kTickExchangeTimeValidV1;
    return payload;
}

ipc::RealtimeWireTickPayloadV2 ShenzhenCancel() {
    ipc::RealtimeWireTickPayloadV2 payload{};
    FillCommon(
        &payload,
        market::MarketEventKindV1::kShenzhenTransaction,
        market::MarketV1::kShenzhen,
        3U);
    payload.channel = 7;
    payload.native_event_sequence = 1'003;
    payload.source_raw_code_1 = 52;
    payload.action =
        static_cast<std::uint8_t>(
            market::TickActionV1::kCancel);
    payload.side =
        static_cast<std::uint8_t>(market::SideV1::kBuy);
    SetDecimal(&payload.price, 123'456, 4U, false);
    SetQuantity(&payload.quantity, 2, true);
    SetDecimal(&payload.trade_amount, 0, 0U, false);
    SetQuantity(&payload.matched_quantity, 0, false);
    payload.primary_order_id = 1'001;
    payload.buy_order_id = 1'001;
    payload.validity_bitmap =
        market::kTickQuantityValidV1 |
        market::kTickPrimaryOrderIdValidV1 |
        market::kTickBuyOrderIdValidV1 |
        market::kTickExchangeTimeValidV1 |
        market::kTickSideValidV1;
    return payload;
}

bool TestShanghaiProjection() {
    bool ok = true;
    const ipc::RealtimeWireTickPayloadV2 source =
        ShanghaiTrade();
    market::ShanghaiOrderEventInputV1 projected{};
    ok &= Expect(
        ipc::ProjectShanghaiOrderEventInputFromWireV2(
            source, &projected) ==
            ipc::WireOrderEventProjectionResultV2::kProjected,
        "valid Shanghai Wire trade projects");
    ok &= Expect(
        projected.trade_date == 20260730U &&
            projected.instrument_id == 17U &&
            projected.channel == 3 &&
            projected.anchor.native_event_sequence == 77 &&
            projected.anchor.source_sequence == 100U &&
            projected.anchor.ingress_sequence == 200U &&
            projected.anchor.tick_stream_sequence == 150U &&
            projected.anchor.event_time_valid &&
            projected.anchor.event_time_unix_ns_valid &&
            projected.anchor.vendor_local_time_valid &&
            projected.anchor.vendor_local_time_ns_since_midnight ==
                34'200'001'000'000ULL &&
            projected.trade_amount_valid &&
            projected.trade_amount_p6 == 50'000'000 &&
            projected.source_quality_flags == 0x12U &&
            projected.source_market_notices == 0x34U,
        "Shanghai projection preserves exact sequences, times, and amount");

    std::unique_ptr<market::ShanghaiOrderEventAggregatorV1>
        aggregator;
    ok &= Expect(
        market::ShanghaiOrderEventAggregatorV1::Create(
            {.trade_date = 20260730U,
             .maximum_order_states = 4U},
            &aggregator) ==
            market::ShanghaiOrderAggregatorCreateErrorV1::kNone,
        "create Shanghai core for direct Wire feed");
    if (aggregator != nullptr) {
        std::vector<market::ShanghaiOrderEventV1> events;
        ok &= Expect(
            aggregator->Consume(projected, &events) ==
                    market::ShanghaiOrderAggregatorConsumeErrorV1::
                        kNone &&
                !events.empty(),
            "projected Shanghai Wire input feeds reconstruction core");
    }

    {
        const auto add = ShanghaiAdd();
        ok &= Expect(
            ipc::ProjectShanghaiOrderEventInputFromWireV2(
                add, &projected) ==
                    ipc::WireOrderEventProjectionResultV2::
                        kProjected &&
                projected.action == market::TickActionV1::kAdd &&
                projected.primary_order_id == 1'000 &&
                projected.matched_quantity_valid &&
                projected.matched_quantity == 7 &&
                !projected.trade_amount_valid,
            "Shanghai A maps source TradeMoney only to matched quantity");
        auto invalid = add;
        invalid.matched_quantity.raw = 6;
        ok &= Expect(
            ipc::ProjectShanghaiOrderEventInputFromWireV2(
                invalid, &projected) ==
                ipc::WireOrderEventProjectionResultV2::
                    kInvalidEventContract,
            "Shanghai A matched quantity must agree with exact source p3");
    }
    {
        auto invalid = source;
        invalid.common.source_slot = 3U;
        projected.trade_date = 1U;
        ok &= Expect(
            ipc::ProjectShanghaiOrderEventInputFromWireV2(
                invalid, &projected) ==
                    ipc::WireOrderEventProjectionResultV2::
                        kInvalidEnvelope &&
                projected.trade_date == 0U,
            "wrong Shanghai source slot fails closed and clears output");
    }
    {
        auto invalid = source;
        invalid.quantity.scale = 1U;
        ok &= Expect(
            ipc::ProjectShanghaiOrderEventInputFromWireV2(
                invalid, &projected) ==
                ipc::WireOrderEventProjectionResultV2::
                    kInvalidFieldEncoding,
            "nonzero Shanghai quantity scale is rejected");
    }
    {
        auto invalid = source;
        ++invalid.price.normalized_p6;
        ok &= Expect(
            ipc::ProjectShanghaiOrderEventInputFromWireV2(
                invalid, &projected) ==
                ipc::WireOrderEventProjectionResultV2::
                    kInvalidFieldEncoding,
            "inconsistent p6 normalization is rejected");
    }
    {
        auto invalid = source;
        invalid.validity_bitmap &=
            ~market::kTickPriceValidV1;
        ok &= Expect(
            ipc::ProjectShanghaiOrderEventInputFromWireV2(
                invalid, &projected) ==
                ipc::WireOrderEventProjectionResultV2::
                    kInvalidFieldEncoding,
            "numeric valid flag cannot disagree with validity bitmap");
    }
    {
        auto invalid = source;
        ++invalid.common.event_time_unix_ns;
        ok &= Expect(
            ipc::ProjectShanghaiOrderEventInputFromWireV2(
                invalid, &projected) ==
                ipc::WireOrderEventProjectionResultV2::
                    kInvalidFieldEncoding,
            "exchange event Unix time must match date and time-of-day");
    }
    {
        auto invalid = source;
        SetRaw(
            &invalid.raw_tick_flag,
            &invalid.raw_tick_flag_length,
            "S");
        ok &= Expect(
            ipc::ProjectShanghaiOrderEventInputFromWireV2(
                invalid, &projected) ==
                ipc::WireOrderEventProjectionResultV2::
                    kInvalidEventContract,
            "Shanghai raw aggressor cannot contradict normalized enum");
    }
    {
        auto no_event_time = source;
        no_event_time.validity_bitmap &=
            ~market::kTickExchangeTimeValidV1;
        no_event_time.common.exchange_time_ns_since_midnight = 0U;
        no_event_time.common.event_time_unix_ns = 0;
        no_event_time.common.vendor_local_time_raw = 246'000'000U;
        ok &= Expect(
            ipc::ProjectShanghaiOrderEventInputFromWireV2(
                no_event_time, &projected) ==
                    ipc::WireOrderEventProjectionResultV2::
                        kProjected &&
                !projected.anchor.event_time_valid &&
                !projected.anchor.event_time_unix_ns_valid &&
                !projected.anchor.vendor_local_time_valid &&
                projected.anchor
                        .vendor_local_time_ns_since_midnight == 0U,
            "invalid event time and LocalTime remain explicitly invalid");
    }
    ok &= Expect(
        ipc::ProjectShanghaiOrderEventInputFromWireV2(
            source, nullptr) ==
            ipc::WireOrderEventProjectionResultV2::kNullOutput,
        "null Shanghai output is rejected");
    return ok;
}

bool TestShenzhenProjection() {
    bool ok = true;
    const ipc::RealtimeWireTickPayloadV2 order =
        ShenzhenLimitOrder();
    market::ShenzhenOrderEventInputV1 projected{};
    ok &= Expect(
        ipc::ProjectShenzhenOrderEventInputFromWireV2(
            order, &projected) ==
                ipc::WireOrderEventProjectionResultV2::kProjected &&
            projected.action == market::TickActionV1::kAdd &&
            projected.side == market::SideV1::kBuy &&
            projected.order_type == market::OrderTypeV1::kLimit &&
            projected.price_p6 == 10'000'000 &&
            projected.quantity == 20 &&
            projected.primary_order_id == 1'001,
        "valid Shenzhen 6.33 limit order projects");
    ok &= Expect(
        ipc::ProjectShanghaiOrderEventInputFromWireV2(
            order, nullptr) ==
            ipc::WireOrderEventProjectionResultV2::kNullOutput,
        "null target is diagnosed before event family");
    market::ShanghaiOrderEventInputV1 shanghai_output{};
    ok &= Expect(
        ipc::ProjectShanghaiOrderEventInputFromWireV2(
            order, &shanghai_output) ==
            ipc::WireOrderEventProjectionResultV2::kNotTargetEvent,
        "valid Shenzhen Wire event is not a Shanghai event");

    std::unique_ptr<market::ShenzhenOrderEventProjectorV1>
        projector;
    ok &= Expect(
        market::ShenzhenOrderEventProjectorV1::Create(
            {.trade_date = 20260730U,
             .maximum_order_states = 4U},
            &projector) ==
            market::ShenzhenOrderProjectorCreateErrorV1::kNone,
        "create Shenzhen core for direct Wire feed");
    if (projector != nullptr) {
        std::vector<market::ShenzhenOrderEventV1> events;
        ok &= Expect(
            projector->Consume(projected, &events) ==
                    market::ShenzhenOrderProjectorConsumeErrorV1::
                        kNone &&
                events.size() == 1U,
            "projected Shenzhen Wire input feeds reconstruction core");
    }

    {
        const auto trade = ShenzhenTrade();
        ok &= Expect(
            ipc::ProjectShenzhenOrderEventInputFromWireV2(
                trade, &projected) ==
                    ipc::WireOrderEventProjectionResultV2::
                        kProjected &&
                projected.action ==
                    market::TickActionV1::kTrade &&
                projected.buy_order_id == 1'001 &&
                projected.sell_order_id == 2'001 &&
                !projected.side_valid &&
                !projected.order_type_valid,
            "valid Shenzhen 6.36 trade projects without aggressor");
    }
    {
        auto trade = ShenzhenTrade();
        trade.buy_order_id = 0;
        trade.validity_bitmap &=
            ~market::kTickBuyOrderIdValidV1;
        ok &= Expect(
            ipc::ProjectShenzhenOrderEventInputFromWireV2(
                trade, &projected) ==
                    ipc::WireOrderEventProjectionResultV2::
                        kProjected &&
                projected.buy_order_id == 0 &&
                projected.sell_order_id == 2'001,
            "Shenzhen trade preserves missing buy reference");
    }
    {
        auto trade = ShenzhenTrade();
        trade.sell_order_id = 0;
        trade.validity_bitmap &=
            ~market::kTickSellOrderIdValidV1;
        ok &= Expect(
            ipc::ProjectShenzhenOrderEventInputFromWireV2(
                trade, &projected) ==
                    ipc::WireOrderEventProjectionResultV2::
                        kProjected &&
                projected.buy_order_id == 1'001 &&
                projected.sell_order_id == 0,
            "Shenzhen trade preserves missing sell reference");
    }
    {
        auto trade = ShenzhenTrade();
        trade.buy_order_id = 0;
        trade.sell_order_id = 0;
        trade.validity_bitmap &=
            ~(market::kTickBuyOrderIdValidV1 |
              market::kTickSellOrderIdValidV1);
        ok &= Expect(
            ipc::ProjectShenzhenOrderEventInputFromWireV2(
                trade, &projected) ==
                    ipc::WireOrderEventProjectionResultV2::
                        kProjected &&
                projected.buy_order_id == 0 &&
                projected.sell_order_id == 0,
            "Shenzhen trade preserves two unavailable references");
    }
    {
        const auto cancel = ShenzhenCancel();
        ok &= Expect(
            ipc::ProjectShenzhenOrderEventInputFromWireV2(
                cancel, &projected) ==
                    ipc::WireOrderEventProjectionResultV2::
                        kProjected &&
                projected.action ==
                    market::TickActionV1::kCancel &&
                projected.side == market::SideV1::kBuy &&
                projected.primary_order_id == 1'001 &&
                !projected.price_valid &&
                projected.price_p6 == 0,
            "valid Shenzhen 6.36 cancel uses its sole reference");
    }
    {
        auto invalid = ShenzhenTrade();
        invalid.source_raw_code_2 = 50;
        ok &= Expect(
            ipc::ProjectShenzhenOrderEventInputFromWireV2(
                invalid, &projected) ==
                ipc::WireOrderEventProjectionResultV2::
                    kInvalidEventContract,
            "transaction cannot carry an order-type raw code");
    }
    {
        auto invalid = ShenzhenCancel();
        invalid.sell_order_id = 2'001;
        invalid.validity_bitmap |=
            market::kTickSellOrderIdValidV1;
        ok &= Expect(
            ipc::ProjectShenzhenOrderEventInputFromWireV2(
                invalid, &projected) ==
                ipc::WireOrderEventProjectionResultV2::
                    kInvalidEventContract,
            "ambiguous Shenzhen cancel references are rejected");
    }
    {
        auto invalid = ShenzhenTrade();
        SetDecimal(&invalid.trade_amount, 1, 0U, true);
        invalid.validity_bitmap |=
            market::kTickTradeAmountValidV1;
        ok &= Expect(
            ipc::ProjectShenzhenOrderEventInputFromWireV2(
                invalid, &projected) ==
                ipc::WireOrderEventProjectionResultV2::
                    kInvalidEventContract,
            "adapter never invents Shenzhen 6.36 trade amount");
    }
    {
        auto invalid = order;
        invalid.source_raw_code_2 = 49;
        invalid.order_type =
            static_cast<std::uint8_t>(
                market::OrderTypeV1::kMarket);
        invalid.validity_bitmap &=
            ~market::kTickPriceValidV1;
        invalid.price.valid = 0U;
        invalid.price.normalized_p6 = 0;
        ok &= Expect(
            ipc::ProjectShenzhenOrderEventInputFromWireV2(
                invalid, &projected) ==
                    ipc::WireOrderEventProjectionResultV2::
                        kProjected &&
                projected.order_type ==
                    market::OrderTypeV1::kMarket &&
                !projected.price_valid &&
                projected.price_p6 == 0,
            "Shenzhen market order retains no fabricated limit price");
    }
    {
        auto invalid = order;
        invalid.quantity.scale = 4U;
        ok &= Expect(
            ipc::ProjectShenzhenOrderEventInputFromWireV2(
                invalid, &projected) ==
                ipc::WireOrderEventProjectionResultV2::
                    kInvalidFieldEncoding,
            "nonzero Shenzhen quantity scale is rejected");
    }
    ok &= Expect(
        ipc::WireOrderEventProjectionResultNameV2(
            ipc::WireOrderEventProjectionResultV2::
                kInvalidEventContract) ==
            "invalid_event_contract",
        "projection result name is stable");
    return ok;
}

}  // namespace

int main() {
    const bool ok =
        TestShanghaiProjection() &&
        TestShenzhenProjection();
    if (!ok) {
        return 1;
    }
    std::cout
        << "order event Wire V2 adapter tests passed\n";
    return 0;
}
