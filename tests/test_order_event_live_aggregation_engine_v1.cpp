#include "l2flow/ipc/order_event_live_aggregation_engine_v1.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <span>
#include <string_view>

#include <unistd.h>

namespace {

namespace ipc = l2flow::ipc;
namespace market = l2flow::market;

constexpr std::uint32_t kTradeDate = 20260730U;

bool Expect(bool condition, std::string_view message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << '\n';
    }
    return condition;
}

l2flow::common::Identity128 RunId(std::uint8_t seed) {
    l2flow::common::Identity128 result{};
    for (std::size_t index = 0U; index < result.size(); ++index) {
        result[index] = static_cast<std::byte>(
            static_cast<std::uint8_t>(
                seed + static_cast<std::uint8_t>(index)));
    }
    return result;
}

std::unique_ptr<ipc::OrderEventDeltaRingProducerV1> Producer(
    std::uint64_t capacity,
    std::uint8_t seed,
    bool* ok) {
    ipc::OrderEventDeltaRingConfigV1 config{};
    config.run_id = RunId(seed);
    config.session_epoch = 91U + seed;
    config.trade_date = kTradeDate;
    config.ring_capacity = capacity;
    config.maximum_mapping_bytes = 64U * 1024U * 1024U;
    config.producer_started_monotonic_ns = 1'000U;
    std::unique_ptr<ipc::OrderEventDeltaRingProducerV1> producer;
    *ok &= Expect(
        ipc::OrderEventDeltaRingProducerV1::Create(
            config, &producer) ==
                ipc::OrderEventDeltaRingCreateErrorV1::kNone &&
            producer != nullptr,
        "create delta producer");
    return producer;
}

std::unique_ptr<ipc::OrderEventLiveAggregationEngineV1> Engine(
    ipc::OrderEventDeltaRingProducerV1* producer,
    bool* ok) {
    std::unique_ptr<ipc::OrderEventLiveAggregationEngineV1> engine;
    *ok &= Expect(
        ipc::OrderEventLiveAggregationEngineV1::Create(
            {.maximum_shanghai_order_states = 100U,
             .maximum_shenzhen_order_states = 100U},
            producer,
            &engine) == ipc::OrderEventLiveCreateErrorV1::kNone &&
            engine != nullptr,
        "create live aggregation engine");
    return engine;
}

std::unique_ptr<ipc::OrderEventDeltaRingReaderV1> Reader(
    const ipc::OrderEventDeltaRingProducerV1& producer,
    bool* ok) {
    int descriptor = -1;
    *ok &= Expect(
        producer.DuplicateReadOnlyDescriptor(&descriptor) &&
            descriptor >= 0,
        "duplicate delta descriptor");
    std::unique_ptr<ipc::OrderEventDeltaRingReaderV1> reader;
    if (descriptor >= 0) {
        *ok &= Expect(
            ipc::OrderEventDeltaRingReaderV1::Open(
                descriptor, producer.session(), &reader) ==
                    ipc::OrderEventDeltaReaderOpenErrorV1::kNone &&
                reader != nullptr,
            "open delta reader");
        static_cast<void>(::close(descriptor));
    }
    return reader;
}

void FillCommon(
    ipc::RealtimeWireTickPayloadV2* payload,
    std::uint64_t tick_sequence,
    std::uint8_t event_kind,
    market::MarketV1 market_value,
    std::uint8_t source_slot,
    std::uint32_t instrument_id = 17U) {
    *payload = {};
    payload->common.record_schema_version = 2U;
    payload->common.record_bytes =
        sizeof(ipc::RealtimeWireTickPayloadV2);
    payload->common.instrument_id = instrument_id;
    payload->common.ordinal = instrument_id - 1U;
    payload->common.source_sequence = tick_sequence;
    payload->common.ingress_sequence = 100U + tick_sequence;
    payload->common.tick_stream_sequence = tick_sequence;
    payload->common.vendor_sequence_id = 300U + tick_sequence;
    payload->common.event_time_unix_ns =
        1'785'375'000'000'000'000LL +
        static_cast<std::int64_t>(tick_sequence);
    payload->common.recv_realtime_ns =
        1'785'375'000'000'000'100LL +
        static_cast<std::int64_t>(tick_sequence);
    payload->common.recv_monotonic_ns = 500U + tick_sequence;
    payload->common.exchange_time_ns_since_midnight =
        34'200'000'000'000ULL + tick_sequence;
    payload->common.source_stream_id =
        source_slot == 3U ? 44U : 22U;
    payload->common.trade_date = kTradeDate;
    payload->common.vendor_local_time_raw = 93'000'001U;
    payload->common.source_slot = source_slot;
    payload->common.event_kind = event_kind;
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

ipc::RealtimeWireTickPayloadV2 NonTarget(
    std::uint64_t sequence) {
    ipc::RealtimeWireTickPayloadV2 payload{};
    // A nonzero unknown kind is layout-valid but outside the target kinds
    // known to this V1 engine.
    FillCommon(
        &payload,
        sequence,
        99U,
        market::MarketV1::kShanghai,
        0U);
    return payload;
}

ipc::RealtimeWireTickPayloadV2 ShanghaiTrade(
    std::uint64_t sequence) {
    ipc::RealtimeWireTickPayloadV2 payload{};
    FillCommon(
        &payload,
        sequence,
        static_cast<std::uint8_t>(
            market::MarketEventKindV1::kShanghaiTick),
        market::MarketV1::kShanghai,
        1U);
    payload.channel = 3;
    payload.native_event_sequence = 77;
    payload.action = static_cast<std::uint8_t>(
        market::TickActionV1::kTrade);
    payload.aggressor = static_cast<std::uint8_t>(
        market::AggressorV1::kBuy);
    payload.phase = static_cast<std::uint8_t>(
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
    SetRaw(&payload.raw_type, &payload.raw_type_length, "T");
    SetRaw(
        &payload.raw_tick_flag,
        &payload.raw_tick_flag_length,
        "B");
    return payload;
}

ipc::RealtimeWireTickPayloadV2 ShanghaiAdd(
    std::uint64_t sequence) {
    ipc::RealtimeWireTickPayloadV2 payload{};
    FillCommon(
        &payload,
        sequence,
        static_cast<std::uint8_t>(
            market::MarketEventKindV1::kShanghaiTick),
        market::MarketV1::kShanghai,
        1U);
    payload.channel = 3;
    payload.native_event_sequence = 78;
    payload.action = static_cast<std::uint8_t>(
        market::TickActionV1::kAdd);
    payload.side = static_cast<std::uint8_t>(
        market::SideV1::kBuy);
    payload.phase = static_cast<std::uint8_t>(
        market::TradingPhaseV1::kContinuous);
    SetDecimal(&payload.price, 10'100, 3U, true);
    SetQuantity(&payload.quantity, 10, true);
    SetDecimal(&payload.trade_amount, 5'000, 3U, false);
    SetQuantity(&payload.matched_quantity, 5, true);
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
    SetRaw(&payload.raw_type, &payload.raw_type_length, "A");
    SetRaw(
        &payload.raw_tick_flag,
        &payload.raw_tick_flag_length,
        "B");
    return payload;
}

ipc::RealtimeWireTickPayloadV2 SecondShanghaiTrade(
    std::uint64_t sequence) {
    ipc::RealtimeWireTickPayloadV2 payload =
        ShanghaiTrade(sequence);
    payload.native_event_sequence = 78;
    payload.buy_order_id = 1'001;
    payload.sell_order_id = 2'001;
    return payload;
}

ipc::RealtimeWireTickPayloadV2 ShanghaiEndStatus(
    std::uint64_t sequence) {
    ipc::RealtimeWireTickPayloadV2 payload{};
    FillCommon(
        &payload,
        sequence,
        static_cast<std::uint8_t>(
            market::MarketEventKindV1::kShanghaiTick),
        market::MarketV1::kShanghai,
        1U);
    payload.channel = 3;
    payload.native_event_sequence = 79;
    payload.action = static_cast<std::uint8_t>(
        market::TickActionV1::kStatus);
    payload.phase = static_cast<std::uint8_t>(
        market::TradingPhaseV1::kEnd);
    SetDecimal(&payload.price, 0, 3U, false);
    SetQuantity(&payload.quantity, 0, false);
    SetDecimal(&payload.trade_amount, 0, 3U, false);
    SetQuantity(&payload.matched_quantity, 0, false);
    payload.validity_bitmap =
        market::kTickExchangeTimeValidV1 |
        market::kTickPhaseValidV1;
    SetRaw(&payload.raw_type, &payload.raw_type_length, "S");
    SetRaw(
        &payload.raw_tick_flag,
        &payload.raw_tick_flag_length,
        "ENDTR");
    return payload;
}

ipc::RealtimeWireTickPayloadV2 ShenzhenOrder(
    std::uint64_t sequence) {
    ipc::RealtimeWireTickPayloadV2 payload{};
    FillCommon(
        &payload,
        sequence,
        static_cast<std::uint8_t>(
            market::MarketEventKindV1::kShenzhenOrder),
        market::MarketV1::kShenzhen,
        3U,
        18U);
    payload.channel = 7;
    payload.native_event_sequence = 1'001;
    payload.source_raw_code_1 = 49;
    payload.source_raw_code_2 = 50;
    payload.action = static_cast<std::uint8_t>(
        market::TickActionV1::kAdd);
    payload.side = static_cast<std::uint8_t>(
        market::SideV1::kBuy);
    payload.order_type = static_cast<std::uint8_t>(
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

ipc::RealtimeWireTickPayloadV2 ShenzhenMarketOrder(
    std::uint64_t sequence) {
    ipc::RealtimeWireTickPayloadV2 payload =
        ShenzhenOrder(sequence);
    payload.source_raw_code_2 = 49;
    payload.order_type = static_cast<std::uint8_t>(
        market::OrderTypeV1::kMarket);
    SetDecimal(&payload.price, 0, 4U, false);
    payload.validity_bitmap &= ~market::kTickPriceValidV1;
    return payload;
}

ipc::RealtimeWireTickPayloadV2 ShenzhenTrade(
    std::uint64_t sequence,
    std::int64_t application_sequence = 1'002) {
    ipc::RealtimeWireTickPayloadV2 payload{};
    FillCommon(
        &payload,
        sequence,
        static_cast<std::uint8_t>(
            market::MarketEventKindV1::kShenzhenTransaction),
        market::MarketV1::kShenzhen,
        3U,
        18U);
    payload.channel = 7;
    payload.native_event_sequence = application_sequence;
    payload.source_raw_code_1 = 70;
    payload.action = static_cast<std::uint8_t>(
        market::TickActionV1::kTrade);
    SetDecimal(&payload.price, 100'000, 4U, true);
    SetQuantity(&payload.quantity, 5, true);
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

bool Consume(
    ipc::OrderEventLiveAggregationEngineV1* engine,
    const ipc::RealtimeWireTickPayloadV2& payload,
    std::size_t expected_events,
    ipc::OrderEventLiveSourceClassV1 expected_class,
    std::string_view label) {
    ipc::OrderEventLiveConsumeResultV1 result{};
    return Expect(
        engine != nullptr &&
            engine->Consume(payload, &result) ==
                ipc::OrderEventLiveConsumeErrorV1::kNone &&
            result.source_tick_sequence ==
                payload.common.tick_stream_sequence &&
            result.derived_event_count == expected_events &&
            result.source_class == expected_class,
        label);
}

void TestShenzhenMergedOrderThenTransaction(bool* ok) {
    auto producer = Producer(16U, 11U, ok);
    if (producer == nullptr) {
        return;
    }
    auto engine = Engine(producer.get(), ok);
    auto reader = Reader(*producer, ok);
    if (engine == nullptr || reader == nullptr) {
        return;
    }
    *ok &= Consume(
        engine.get(),
        ShenzhenOrder(1U),
        1U,
        ipc::OrderEventLiveSourceClassV1::kShenzhen,
        "merged Shenzhen 6.33 order is consumed first");
    *ok &= Consume(
        engine.get(),
        ShenzhenTrade(2U),
        2U,
        ipc::OrderEventLiveSourceClassV1::kShenzhen,
        "later 6.36 transaction emits trade then order revision");

    std::array<ipc::OrderEventDeltaPayloadV1, 4U> rows{};
    ipc::OrderEventDeltaReadResultV1 read{};
    *ok &= Expect(
        reader->Read(1U, rows, &read) ==
                ipc::OrderEventDeltaReadErrorV1::kNone &&
            read.written == 3U &&
            read.consumed_source_tick_sequence == 2U &&
            rows[0U].event_kind ==
                L2FLOW_INSTRUMENT_DERIVED_EVENT_ORDER_REVISION_V1 &&
            rows[0U].reserved0 == 0U &&
            rows[0U].native_event_sequence == 1'001 &&
            rows[1U].event_kind ==
                L2FLOW_INSTRUMENT_DERIVED_EVENT_TRADE_V1 &&
            rows[1U].reserved0 == 0U &&
            rows[1U].native_event_sequence == 1'002 &&
            rows[1U].trade_amount_valid == 0U &&
            rows[2U].event_kind ==
                L2FLOW_INSTRUMENT_DERIVED_EVENT_ORDER_REVISION_V1 &&
            rows[2U].reserved0 == 1U &&
            rows[0U].reserved1[0U] ==
                L2FLOW_INSTRUMENT_DERIVED_EVENT_SOURCE_TICK_ORDINAL_VALID_V2 &&
            rows[1U].reserved1[0U] ==
                L2FLOW_INSTRUMENT_DERIVED_EVENT_SOURCE_TICK_ORDINAL_VALID_V2 &&
            rows[2U].reserved1[0U] ==
                L2FLOW_INSTRUMENT_DERIVED_EVENT_SOURCE_TICK_ORDINAL_VALID_V2 &&
            rows[2U].order_id == 1'001 &&
            rows[2U].revision == 2U &&
            rows[2U].remaining_quantity == 15,
        "6.33/6.36 merged order is preserved through live Wire projection");

    auto failed_producer = Producer(8U, 12U, ok);
    auto failed_engine =
        failed_producer == nullptr
            ? nullptr
            : Engine(failed_producer.get(), ok);
    if (failed_engine != nullptr) {
        *ok &= Consume(
            failed_engine.get(),
            ShenzhenOrder(1U),
            1U,
            ipc::OrderEventLiveSourceClassV1::kShenzhen,
            "ordering check accepts Shenzhen baseline");
        ipc::OrderEventLiveConsumeResultV1 result{};
        *ok &= Expect(
            failed_engine->Consume(
                ShenzhenTrade(2U, 1'000), &result) ==
                    ipc::OrderEventLiveConsumeErrorV1::
                        kCoreAggregationFailed &&
                failed_engine->last_shenzhen_error() ==
                    market::ShenzhenOrderProjectorConsumeErrorV1::
                        kOutOfOrderInput &&
                failed_engine->failed() &&
                failed_producer->state() ==
                    ipc::OrderEventDeltaProducerStateV1::kFailed,
            "non-increasing cross-family ApplSeqNum fails closed");
    }
}

void TestMixedDenseStreamAndState(bool* ok) {
    auto producer = Producer(32U, 1U, ok);
    if (producer == nullptr) {
        return;
    }
    auto engine = Engine(producer.get(), ok);
    auto reader = Reader(*producer, ok);
    if (engine == nullptr || reader == nullptr) {
        return;
    }
    *ok &= Consume(
        engine.get(),
        NonTarget(1U),
        0U,
        ipc::OrderEventLiveSourceClassV1::kNonTarget,
        "non-target tick advances with zero derived events");
    *ok &= Consume(
        engine.get(),
        ShanghaiTrade(2U),
        2U,
        ipc::OrderEventLiveSourceClassV1::kShanghai,
        "Shanghai T emits trade and synthetic order");
    *ok &= Consume(
        engine.get(),
        ShenzhenOrder(3U),
        1U,
        ipc::OrderEventLiveSourceClassV1::kShenzhen,
        "Shenzhen 6.33 order emits exact source order");
    *ok &= Consume(
        engine.get(),
        NonTarget(4U),
        0U,
        ipc::OrderEventLiveSourceClassV1::kNonTarget,
        "interleaved non-target preserves both market states");
    *ok &= Consume(
        engine.get(),
        ShanghaiAdd(5U),
        1U,
        ipc::OrderEventLiveSourceClassV1::kShanghai,
        "later Shanghai A revises prior T-only state");

    std::array<ipc::OrderEventDeltaPayloadV1, 16U> rows{};
    ipc::OrderEventDeltaReadResultV1 read{};
    *ok &= Expect(
        reader->Read(1U, rows, &read) ==
                ipc::OrderEventDeltaReadErrorV1::kNone &&
            read.written == 4U && read.next_sequence == 5U &&
            read.consumed_source_tick_sequence == 5U &&
            producer->consumed_source_tick_sequence() == 5U &&
            engine->consumed_source_tick_sequence() == 5U &&
            engine->shanghai_order_count() == 1U &&
            engine->shenzhen_order_count() == 1U,
        "mixed stream exposes four events and dense source cursor five");
    if (read.written == 4U) {
        *ok &= Expect(
            rows[0U].derived_event_sequence == 1U &&
                rows[1U].derived_event_sequence == 2U &&
                rows[2U].derived_event_sequence == 3U &&
                rows[3U].derived_event_sequence == 4U &&
                rows[0U].tick_stream_sequence == 2U &&
                rows[1U].tick_stream_sequence == 2U &&
                rows[2U].tick_stream_sequence == 3U &&
                rows[3U].tick_stream_sequence == 5U &&
                rows[0U].reserved0 == 0U &&
                rows[1U].reserved0 == 1U &&
                rows[2U].reserved0 == 0U &&
                rows[3U].reserved0 == 0U,
            "delta event sequence is dense while source anchors are exact");
        *ok &= Expect(
            rows[1U].event_kind ==
                    L2FLOW_INSTRUMENT_DERIVED_EVENT_ORDER_REVISION_V1 &&
                rows[1U].order_source == 2U &&
                rows[1U].original_quantity == 5 &&
                rows[1U].original_quantity_status == 2U &&
                rows[1U].price_source == 2U &&
                rows[1U].apply_to_book == 0U,
            "T-only order is lower-bound with buy maximum execution price");
        *ok &= Expect(
            rows[2U].market ==
                    L2FLOW_INSTRUMENT_DERIVED_EVENT_MARKET_SHENZHEN_V1 &&
                rows[2U].order_id == 1'001 &&
                rows[2U].side_source ==
                    L2FLOW_INSTRUMENT_DERIVED_EVENT_SIDE_SOURCE_DIRECT_V1 &&
                rows[2U].order_source ==
                    L2FLOW_INSTRUMENT_DERIVED_EVENT_ORDER_SOURCE_EVENT_V1 &&
                rows[2U].price_valid == 1U &&
                rows[2U].price_source ==
                    L2FLOW_INSTRUMENT_DERIVED_EVENT_PRICE_SOURCE_EVENT_V1 &&
                rows[2U].published_quantity == 20 &&
                rows[2U].published_quantity_valid == 1U &&
                rows[2U].original_quantity == 20 &&
                rows[2U].original_quantity_valid == 1U &&
                rows[2U].original_quantity_status ==
                    L2FLOW_INSTRUMENT_DERIVED_EVENT_ORIGINAL_QUANTITY_EXACT_V1 &&
                rows[2U].order_type ==
                    static_cast<std::uint8_t>(
                        market::OrderTypeV1::kLimit) &&
                rows[2U].add_seen == 1U &&
                rows[2U].apply_to_book == 1U,
            "Shenzhen source order retains direct/exact source semantics");
        *ok &= Expect(
            rows[3U].operation == 1U &&
                rows[3U].revision == 2U &&
                rows[3U].order_source == 1U &&
                rows[3U].original_quantity == 15 &&
                rows[3U].original_quantity_status == 1U &&
                rows[3U].observed_pre_add_trade_quantity == 5 &&
                rows[3U].source_matched_quantity == 5 &&
                rows[3U].published_quantity == 10 &&
                rows[3U].remaining_quantity == 10 &&
                rows[3U].price_source == 1U &&
                rows[3U].apply_to_book == 1U,
            "A revision preserves cross-tick state and exact quantity math");
    }
}

void TestShenzhenPricelessSourceOrderSemantics(bool* ok) {
    auto producer = Producer(8U, 10U, ok);
    if (producer == nullptr) {
        return;
    }
    auto engine = Engine(producer.get(), ok);
    auto reader = Reader(*producer, ok);
    if (engine == nullptr || reader == nullptr) {
        return;
    }
    *ok &= Consume(
        engine.get(),
        ShenzhenMarketOrder(1U),
        1U,
        ipc::OrderEventLiveSourceClassV1::kShenzhen,
        "Shenzhen market order emits one source order revision");

    std::array<ipc::OrderEventDeltaPayloadV1, 2U> rows{};
    ipc::OrderEventDeltaReadResultV1 read{};
    *ok &= Expect(
        reader->Read(1U, rows, &read) ==
                ipc::OrderEventDeltaReadErrorV1::kNone &&
            read.written == 1U &&
            rows[0U].order_type ==
                static_cast<std::uint8_t>(
                    market::OrderTypeV1::kMarket) &&
            rows[0U].side_source ==
                L2FLOW_INSTRUMENT_DERIVED_EVENT_SIDE_SOURCE_DIRECT_V1 &&
            rows[0U].order_source ==
                L2FLOW_INSTRUMENT_DERIVED_EVENT_ORDER_SOURCE_EVENT_V1 &&
            rows[0U].price_valid == 0U &&
            rows[0U].price_source ==
                L2FLOW_INSTRUMENT_DERIVED_EVENT_PRICE_SOURCE_UNKNOWN_V1 &&
            rows[0U].published_quantity == 20 &&
            rows[0U].published_quantity_valid == 1U &&
            rows[0U].original_quantity == 20 &&
            rows[0U].original_quantity_status ==
                L2FLOW_INSTRUMENT_DERIVED_EVENT_ORIGINAL_QUANTITY_EXACT_V1 &&
            rows[0U].add_seen == 1U &&
            rows[0U].apply_to_book == 1U,
        "Shenzhen market order is exact/direct without inventing a price");
}

void TestGapAndMalformedFailClosed(bool* ok) {
    {
        auto producer = Producer(8U, 20U, ok);
        auto engine = producer == nullptr
                          ? nullptr
                          : Engine(producer.get(), ok);
        if (engine == nullptr) {
            return;
        }
        *ok &= Consume(
            engine.get(),
            NonTarget(1U),
            0U,
            ipc::OrderEventLiveSourceClassV1::kNonTarget,
            "gap fixture consumes sequence one");
        ipc::OrderEventLiveConsumeResultV1 result{};
        *ok &= Expect(
            engine->Consume(NonTarget(3U), &result) ==
                    ipc::OrderEventLiveConsumeErrorV1::
                        kSourceTickGap &&
                engine->failed() &&
                producer->state() ==
                    ipc::OrderEventDeltaProducerStateV1::kFailed &&
                producer->consumed_source_tick_sequence() == 1U &&
                engine->Consume(NonTarget(2U), &result) ==
                    ipc::OrderEventLiveConsumeErrorV1::kFailed,
            "global source gap permanently fail-closes engine and ring");
    }
    {
        auto producer = Producer(8U, 30U, ok);
        auto engine = producer == nullptr
                          ? nullptr
                          : Engine(producer.get(), ok);
        if (engine == nullptr) {
            return;
        }
        auto malformed = ShanghaiTrade(1U);
        malformed.common.source_slot = 3U;
        ipc::OrderEventLiveConsumeResultV1 result{};
        *ok &= Expect(
            engine->Consume(malformed, &result) ==
                    ipc::OrderEventLiveConsumeErrorV1::
                        kInvalidSourceEnvelope &&
                engine->failed() &&
                producer->state() ==
                    ipc::OrderEventDeltaProducerStateV1::kFailed &&
                producer->consumed_source_tick_sequence() == 0U,
            "target kind with wrong source slot is fatal");
    }
    {
        auto producer = Producer(8U, 40U, ok);
        auto engine = producer == nullptr
                          ? nullptr
                          : Engine(producer.get(), ok);
        if (engine == nullptr) {
            return;
        }
        auto malformed = ShanghaiTrade(1U);
        malformed.price.scale = 6U;
        ipc::OrderEventLiveConsumeResultV1 result{};
        *ok &= Expect(
            engine->Consume(malformed, &result) ==
                    ipc::OrderEventLiveConsumeErrorV1::
                        kWireProjectionFailed &&
                engine->last_wire_projection_result() ==
                    ipc::WireOrderEventProjectionResultV2::
                        kInvalidFieldEncoding &&
                producer->state() ==
                    ipc::OrderEventDeltaProducerStateV1::kFailed,
            "strict target Wire projection failure is fatal");
    }
}

void TestWholeTickBatchFailurePublishesNothing(bool* ok) {
    auto producer = Producer(1U, 50U, ok);
    auto engine =
        producer == nullptr ? nullptr : Engine(producer.get(), ok);
    if (engine == nullptr) {
        return;
    }
    ipc::OrderEventLiveConsumeResultV1 result{};
    *ok &= Expect(
        engine->Consume(ShanghaiTrade(1U), &result) ==
                ipc::OrderEventLiveConsumeErrorV1::
                    kDeltaPublicationFailed &&
            engine->last_publish_error() ==
                ipc::OrderEventDeltaPublishErrorV1::kBatchTooLarge &&
            engine->failed() &&
            producer->published_event_sequence() == 0U &&
            producer->consumed_source_tick_sequence() == 0U &&
            producer->state() ==
                ipc::OrderEventDeltaProducerStateV1::kFailed,
        "unretainable two-event tick publishes no partial event or cursor");
}

void TestEndStatusIsOneAtomicSourceBatch(bool* ok) {
    {
        auto producer = Producer(8U, 55U, ok);
        auto engine = producer == nullptr
                          ? nullptr
                          : Engine(producer.get(), ok);
        auto reader = producer == nullptr
                          ? nullptr
                          : Reader(*producer, ok);
        if (engine == nullptr || reader == nullptr) {
            return;
        }
        *ok &= Consume(
            engine.get(),
            ShanghaiTrade(1U),
            2U,
            ipc::OrderEventLiveSourceClassV1::kShanghai,
            "first Shanghai trade creates first active order");
        *ok &= Consume(
            engine.get(),
            SecondShanghaiTrade(2U),
            2U,
            ipc::OrderEventLiveSourceClassV1::kShanghai,
            "second Shanghai trade creates second active order");
        *ok &= Consume(
            engine.get(),
            ShanghaiEndStatus(3U),
            3U,
            ipc::OrderEventLiveSourceClassV1::kShanghai,
            "ENDTR emits status plus both final revisions");

        std::array<ipc::OrderEventDeltaPayloadV1, 8U> rows{};
        ipc::OrderEventDeltaReadResultV1 read{};
        *ok &= Expect(
            reader->Read(1U, rows, &read) ==
                    ipc::OrderEventDeltaReadErrorV1::kNone &&
                read.written == 7U &&
                read.next_sequence == 8U &&
                read.consumed_source_tick_sequence == 3U &&
                rows[4U].event_kind ==
                    L2FLOW_INSTRUMENT_DERIVED_EVENT_STATUS_V1 &&
                rows[4U].tick_stream_sequence == 3U &&
                rows[4U].reserved0 == 0U &&
                rows[5U].operation ==
                    static_cast<std::uint8_t>(
                        market::ShanghaiOrderDeltaOperationV1::
                            kFinalize) &&
                rows[6U].operation ==
                    static_cast<std::uint8_t>(
                        market::ShanghaiOrderDeltaOperationV1::
                            kFinalize) &&
                rows[5U].tick_stream_sequence == 3U &&
                rows[6U].tick_stream_sequence == 3U &&
                rows[5U].reserved0 == 1U &&
                rows[6U].reserved0 == 2U,
            "ENDTR complete event batch becomes visible as one prefix");
    }
    {
        auto producer = Producer(2U, 56U, ok);
        auto engine = producer == nullptr
                          ? nullptr
                          : Engine(producer.get(), ok);
        if (engine == nullptr) {
            return;
        }
        *ok &= Consume(
            engine.get(),
            ShanghaiTrade(1U),
            2U,
            ipc::OrderEventLiveSourceClassV1::kShanghai,
            "small ring accepts first complete trade batch");
        *ok &= Consume(
            engine.get(),
            SecondShanghaiTrade(2U),
            2U,
            ipc::OrderEventLiveSourceClassV1::kShanghai,
            "small ring accepts second complete trade batch");
        ipc::OrderEventLiveConsumeResultV1 result{};
        *ok &= Expect(
            engine->Consume(ShanghaiEndStatus(3U), &result) ==
                    ipc::OrderEventLiveConsumeErrorV1::
                        kDeltaPublicationFailed &&
                engine->last_publish_error() ==
                    ipc::OrderEventDeltaPublishErrorV1::
                        kBatchTooLarge &&
                producer->published_event_sequence() == 4U &&
                producer->consumed_source_tick_sequence() == 2U &&
                producer->state() ==
                    ipc::OrderEventDeltaProducerStateV1::kFailed,
            "oversized ENDTR batch never splits or advances source cursor");
    }
}

void TestCreateRequiresPristineProducer(bool* ok) {
    auto producer = Producer(8U, 60U, ok);
    if (producer == nullptr) {
        return;
    }
    *ok &= Expect(
        producer->PublishSourceTick(1U, {}) ==
            ipc::OrderEventDeltaPublishErrorV1::kNone,
        "advance producer before live-engine create");
    std::unique_ptr<ipc::OrderEventLiveAggregationEngineV1> engine;
    *ok &= Expect(
        ipc::OrderEventLiveAggregationEngineV1::Create(
            {.maximum_shanghai_order_states = 10U,
             .maximum_shenzhen_order_states = 10U},
            producer.get(),
            &engine) ==
                ipc::OrderEventLiveCreateErrorV1::
                    kProducerNotPristine &&
            engine == nullptr,
        "live engine refuses a producer whose source prefix already moved");
}

}  // namespace

int main() {
    bool ok = true;
    TestShenzhenMergedOrderThenTransaction(&ok);
    TestMixedDenseStreamAndState(&ok);
    TestShenzhenPricelessSourceOrderSemantics(&ok);
    TestGapAndMalformedFailClosed(&ok);
    TestWholeTickBatchFailurePublishesNothing(&ok);
    TestEndStatusIsOneAtomicSourceBatch(&ok);
    TestCreateRequiresPristineProducer(&ok);
    if (!ok) {
        return 1;
    }
    std::cout << "order event live aggregation engine tests passed\n";
    return 0;
}
