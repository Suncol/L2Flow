#include "l2flow/ipc/certified_order_event_history_v1.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <limits>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <variant>
#include <vector>

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

void SetDecimal(
    ipc::RealtimeWireDecimalV2* value,
    std::int64_t raw,
    std::uint8_t scale,
    bool valid) {
    *value = {};
    value->raw = raw;
    value->scale = scale;
    value->valid = valid ? 1U : 0U;
    if (!valid) {
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
    *value = {};
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

void FillCommon(
    ipc::RealtimeWireTickPayloadV2* payload,
    std::uint64_t arrival_tick_sequence,
    market::MarketEventKindV1 event_kind,
    market::MarketV1 market_value,
    std::uint8_t source_slot,
    std::uint32_t instrument_id = 17U) {
    *payload = {};
    payload->common.record_schema_version = 2U;
    payload->common.record_bytes =
        sizeof(ipc::RealtimeWireTickPayloadV2);
    payload->common.instrument_id = instrument_id;
    payload->common.ordinal = instrument_id - 1U;
    payload->common.source_sequence =
        1'000U + arrival_tick_sequence;
    payload->common.ingress_sequence =
        10'000U + arrival_tick_sequence;
    payload->common.tick_stream_sequence =
        arrival_tick_sequence;
    payload->common.vendor_sequence_id =
        20'000U + arrival_tick_sequence;
    payload->common.event_time_unix_ns =
        1'785'375'000'000'000'000LL +
        static_cast<std::int64_t>(
            arrival_tick_sequence);
    payload->common.recv_realtime_ns =
        payload->common.event_time_unix_ns + 100;
    payload->common.recv_monotonic_ns =
        static_cast<std::int64_t>(
            30'000U + arrival_tick_sequence);
    payload->common.exchange_time_ns_since_midnight =
        34'200'000'000'000ULL +
        arrival_tick_sequence;
    payload->common.quality_flags = 0x12U;
    payload->common.market_notices = 0x34U;
    payload->common.source_stream_id =
        source_slot == 1U ? 22U : 44U;
    payload->common.trade_date = kTradeDate;
    payload->common.vendor_local_time_raw = 93'000'001U;
    payload->common.source_slot = source_slot;
    payload->common.event_kind =
        static_cast<std::uint8_t>(event_kind);
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

ipc::RealtimeWireTickPayloadV2 ShanghaiTrade(
    std::int64_t native_sequence,
    std::uint64_t arrival_tick_sequence,
    std::int64_t quantity) {
    ipc::RealtimeWireTickPayloadV2 payload{};
    FillCommon(
        &payload,
        arrival_tick_sequence,
        market::MarketEventKindV1::kShanghaiTick,
        market::MarketV1::kShanghai,
        1U);
    payload.channel = 3;
    payload.native_event_sequence = native_sequence;
    payload.action = static_cast<std::uint8_t>(
        market::TickActionV1::kTrade);
    payload.aggressor = static_cast<std::uint8_t>(
        market::AggressorV1::kBuy);
    payload.phase = static_cast<std::uint8_t>(
        market::TradingPhaseV1::kContinuous);
    SetDecimal(&payload.price, 10'000, 3U, true);
    SetQuantity(&payload.quantity, quantity, true);
    SetDecimal(
        &payload.trade_amount,
        10'000 * quantity,
        3U,
        true);
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

ipc::RealtimeWireTickPayloadV2 ShanghaiAdd(
    std::int64_t native_sequence,
    std::uint64_t arrival_tick_sequence,
    std::int64_t order_id = 1'000) {
    ipc::RealtimeWireTickPayloadV2 payload{};
    FillCommon(
        &payload,
        arrival_tick_sequence,
        market::MarketEventKindV1::kShanghaiTick,
        market::MarketV1::kShanghai,
        1U);
    payload.channel = 3;
    payload.native_event_sequence = native_sequence;
    payload.action = static_cast<std::uint8_t>(
        market::TickActionV1::kAdd);
    payload.side = static_cast<std::uint8_t>(
        market::SideV1::kBuy);
    payload.phase = static_cast<std::uint8_t>(
        market::TradingPhaseV1::kContinuous);
    SetDecimal(&payload.price, 10'100, 3U, true);
    SetQuantity(&payload.quantity, 10, true);
    // Shanghai A carries matched quantity in the source TradeMoney integer;
    // the Wire adapter checks their exact p3-to-scale-zero relation.
    SetDecimal(&payload.trade_amount, 5'000, 3U, false);
    SetQuantity(&payload.matched_quantity, 5, true);
    payload.primary_order_id = order_id;
    payload.buy_order_id = order_id;
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

ipc::RealtimeWireTickPayloadV2 ShenzhenOrder(
    std::int64_t native_sequence,
    std::uint64_t arrival_tick_sequence,
    std::int64_t channel = 7) {
    ipc::RealtimeWireTickPayloadV2 payload{};
    FillCommon(
        &payload,
        arrival_tick_sequence,
        market::MarketEventKindV1::kShenzhenOrder,
        market::MarketV1::kShenzhen,
        3U);
    payload.channel = channel;
    payload.native_event_sequence = native_sequence;
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
    payload.primary_order_id = native_sequence;
    payload.validity_bitmap =
        market::kTickPriceValidV1 |
        market::kTickQuantityValidV1 |
        market::kTickPrimaryOrderIdValidV1 |
        market::kTickExchangeTimeValidV1 |
        market::kTickSideValidV1 |
        market::kTickOrderTypeValidV1;
    return payload;
}

ipc::RealtimeWireTickPayloadV2 ShenzhenTrade(
    std::int64_t native_sequence,
    std::uint64_t arrival_tick_sequence,
    std::int64_t order_id = 1'000) {
    ipc::RealtimeWireTickPayloadV2 payload{};
    FillCommon(
        &payload,
        arrival_tick_sequence,
        market::MarketEventKindV1::kShenzhenTransaction,
        market::MarketV1::kShenzhen,
        3U);
    payload.channel = 7;
    payload.native_event_sequence = native_sequence;
    payload.source_raw_code_1 = 70;
    payload.action = static_cast<std::uint8_t>(
        market::TickActionV1::kTrade);
    SetDecimal(&payload.price, 99'900, 4U, true);
    SetQuantity(&payload.quantity, 3, true);
    SetDecimal(&payload.trade_amount, 0, 0U, false);
    SetQuantity(&payload.matched_quantity, 0, false);
    payload.buy_order_id = order_id;
    payload.sell_order_id = 2'000;
    payload.validity_bitmap =
        market::kTickPriceValidV1 |
        market::kTickQuantityValidV1 |
        market::kTickBuyOrderIdValidV1 |
        market::kTickSellOrderIdValidV1 |
        market::kTickExchangeTimeValidV1;
    return payload;
}

ipc::RealtimeWireTickPayloadV2 ShenzhenCancel(
    std::int64_t native_sequence,
    std::uint64_t arrival_tick_sequence,
    std::int64_t order_id = 1'000) {
    ipc::RealtimeWireTickPayloadV2 payload{};
    FillCommon(
        &payload,
        arrival_tick_sequence,
        market::MarketEventKindV1::kShenzhenTransaction,
        market::MarketV1::kShenzhen,
        3U);
    payload.channel = 7;
    payload.native_event_sequence = native_sequence;
    payload.source_raw_code_1 = 52;
    payload.action = static_cast<std::uint8_t>(
        market::TickActionV1::kCancel);
    payload.side = static_cast<std::uint8_t>(
        market::SideV1::kBuy);
    SetDecimal(&payload.price, 0, 4U, false);
    SetQuantity(&payload.quantity, 2, true);
    SetDecimal(&payload.trade_amount, 0, 0U, false);
    SetQuantity(&payload.matched_quantity, 0, false);
    payload.primary_order_id = order_id;
    payload.buy_order_id = order_id;
    payload.validity_bitmap =
        market::kTickQuantityValidV1 |
        market::kTickPrimaryOrderIdValidV1 |
        market::kTickBuyOrderIdValidV1 |
        market::kTickExchangeTimeValidV1 |
        market::kTickSideValidV1;
    return payload;
}

std::unique_ptr<ipc::CertifiedOrderEventHistoryV1> History(
    bool* ok,
    std::size_t maximum_events = 128U,
    std::size_t maximum_states = 64U) {
    std::unique_ptr<ipc::CertifiedOrderEventHistoryV1> history;
    *ok &= Expect(
        ipc::CertifiedOrderEventHistoryV1::Create(
            {.trade_date = kTradeDate,
             .maximum_shanghai_order_states =
                 maximum_states,
             .maximum_shenzhen_order_states =
                 maximum_states,
             .maximum_events = maximum_events,
             .external_journal = {}},
            &history) ==
                ipc::CertifiedOrderEventHistoryErrorV1::kNone &&
            history != nullptr,
        "create certified order-event history");
    return history;
}

ipc::CertifiedOrderEventHistorySnapshotV1 Acquire(
    const ipc::CertifiedOrderEventHistoryV1& history,
    bool* ok,
    std::string_view label) {
    ipc::CertifiedOrderEventHistorySnapshotV1 snapshot;
    *ok &= Expect(
        history.AcquireGeneration(&snapshot) ==
                ipc::CertifiedOrderEventHistoryErrorV1::kNone &&
            snapshot.valid(),
        label);
    return snapshot;
}

struct SourceAnchorView final {
    std::int64_t native_sequence = 0;
    std::uint64_t source_sequence = 0U;
    std::uint64_t ingress_sequence = 0U;
    std::uint64_t tick_stream_sequence = 0U;
};

SourceAnchorView SourceAnchor(
    const ipc::InstrumentDerivedEventPayloadV1& payload) {
    return std::visit(
        [](const auto& event) {
            return SourceAnchorView{
                event.source_anchor.native_event_sequence,
                event.source_anchor.source_sequence,
                event.source_anchor.ingress_sequence,
                event.source_anchor.tick_stream_sequence};
        },
        payload);
}

bool AppendedAnchorsEqual(
    std::span<const ipc::InstrumentDerivedEventV1> events,
    std::size_t begin,
    const ipc::RealtimeWireTickPayloadV2& source) {
    if (begin >= events.size()) {
        return false;
    }
    for (std::size_t index = begin;
         index < events.size();
         ++index) {
        const SourceAnchorView anchor =
            SourceAnchor(events[index].payload);
        if (anchor.native_sequence !=
                source.native_event_sequence ||
            anchor.source_sequence !=
                source.common.source_sequence ||
            anchor.ingress_sequence !=
                source.common.ingress_sequence ||
            anchor.tick_stream_sequence !=
                source.common.tick_stream_sequence) {
            return false;
        }
    }
    return true;
}

bool DenseDerivedSequences(
    std::span<const ipc::InstrumentDerivedEventV1> events) {
    for (std::size_t index = 0U;
         index < events.size();
         ++index) {
        if (events[index].derived_event_sequence !=
            static_cast<std::uint64_t>(index) + 1U) {
            return false;
        }
    }
    return true;
}

bool SourceTickOrdinalsCanonical(
    std::span<const ipc::InstrumentDerivedEventV1> events) {
    std::uint64_t current_tick = 0U;
    std::uint32_t expected_ordinal = 0U;
    for (const auto& event : events) {
        const std::uint64_t tick =
            SourceAnchor(event.payload).tick_stream_sequence;
        if (tick == 0U ||
            !event.source_tick_event_ordinal_valid) {
            return false;
        }
        if (tick != current_tick) {
            current_tick = tick;
            expected_ordinal = 0U;
        }
        if (event.source_tick_event_ordinal != expected_ordinal) {
            return false;
        }
        ++expected_ordinal;
    }
    return true;
}

std::optional<std::uint64_t> ResidentSetBytes() {
    std::ifstream statm("/proc/self/statm");
    std::uint64_t virtual_pages = 0U;
    std::uint64_t resident_pages = 0U;
    if (!(statm >> virtual_pages >> resident_pages)) {
        return std::nullopt;
    }
    const long page_size = ::sysconf(_SC_PAGESIZE);
    if (page_size <= 0) {
        return std::nullopt;
    }
    const std::uint64_t page_bytes =
        static_cast<std::uint64_t>(page_size);
    if (resident_pages >
        std::numeric_limits<std::uint64_t>::max() /
            page_bytes) {
        return std::nullopt;
    }
    return resident_pages * page_bytes;
}

std::string StableShanghaiPayload(
    const ipc::InstrumentDerivedEventPayloadV1& payload) {
    std::ostringstream output;
    std::visit(
        [&output](const auto& event) {
            using Event = std::decay_t<decltype(event)>;
            if constexpr (
                std::is_same_v<
                    Event,
                    market::ShanghaiOrderRevisionEventV1>) {
                const auto& order = event.order;
                output
                    << "SHR|"
                    << static_cast<unsigned>(event.operation)
                    << '|' << order.key.trade_date
                    << '|' << order.key.instrument_id
                    << '|' << order.key.channel
                    << '|' << order.key.order_id
                    << '|' << static_cast<unsigned>(order.side)
                    << '|'
                    << static_cast<unsigned>(
                           order.side_source)
                    << '|'
                    << static_cast<unsigned>(
                           order.order_source)
                    << '|' << order.price_p6
                    << '|' << order.price_valid
                    << '|'
                    << static_cast<unsigned>(
                           order.price_source)
                    << '|'
                    << order.execution_boundary_price_p6
                    << '|'
                    << order.execution_boundary_price_valid
                    << '|' << order.published_quantity
                    << '|'
                    << order.published_quantity_valid
                    << '|' << order.original_quantity
                    << '|' << order.original_quantity_valid
                    << '|'
                    << static_cast<unsigned>(
                           order.original_quantity_status)
                    << '|' << order.remaining_quantity
                    << '|' << order.remaining_quantity_valid
                    << '|' << order.source_matched_quantity
                    << '|'
                    << order.source_matched_quantity_valid
                    << '|'
                    << order.observed_pre_add_trade_quantity
                    << '|' << order.post_add_trade_quantity
                    << '|' << order.total_trade_quantity
                    << '|' << order.total_cancel_quantity
                    << '|' << order.trade_count
                    << '|'
                    << static_cast<unsigned>(
                           order.phase_at_first)
                    << '|'
                    << static_cast<unsigned>(
                           order.phase_at_add)
                    << '|'
                    << static_cast<unsigned>(
                           order.phase_at_last)
                    << '|' << order.add_seen
                    << '|' << order.apply_to_book
                    << '|' << order.revision
                    << '|'
                    << static_cast<unsigned>(order.finality)
                    << '|' << order.quality_flags
                    << '|' << order.source_quality_flags
                    << '|' << order.source_market_notices;
            } else if constexpr (
                std::is_same_v<
                    Event,
                    market::ShanghaiTradeEventV1>) {
                output
                    << "SHT|" << event.trade_date
                    << '|' << event.instrument_id
                    << '|' << event.channel
                    << '|' << event.buy_order_id
                    << '|' << event.sell_order_id
                    << '|'
                    << static_cast<unsigned>(
                           event.aggressor)
                    << '|'
                    << static_cast<unsigned>(event.phase)
                    << '|' << event.price_p6
                    << '|' << event.trade_amount_p6
                    << '|' << event.trade_amount_valid
                    << '|' << event.quantity
                    << '|' << event.source_quality_flags
                    << '|' << event.source_market_notices;
            } else if constexpr (
                std::is_same_v<
                    Event,
                    market::ShanghaiCancelEventV1>) {
                output
                    << "SHC|" << event.key.trade_date
                    << '|' << event.key.instrument_id
                    << '|' << event.key.channel
                    << '|' << event.key.order_id
                    << '|' << static_cast<unsigned>(event.side)
                    << '|'
                    << static_cast<unsigned>(event.phase)
                    << '|' << event.quantity
                    << '|' << event.referenced_order_found
                    << '|' << event.source_quality_flags
                    << '|' << event.source_market_notices;
            } else if constexpr (
                std::is_same_v<
                    Event,
                    market::ShanghaiStatusEventV1>) {
                output
                    << "SHS|" << event.trade_date
                    << '|' << event.instrument_id
                    << '|' << event.channel
                    << '|'
                    << static_cast<unsigned>(event.phase)
                    << '|' << event.quality_flags
                    << '|' << event.source_quality_flags
                    << '|' << event.source_market_notices;
            } else {
                output << "NON_SHANGHAI";
            }
        },
        payload);
    return output.str();
}

bool StableShanghaiEquivalent(
    std::span<const ipc::InstrumentDerivedEventV1> left,
    std::span<const ipc::InstrumentDerivedEventV1> right) {
    if (left.size() != right.size()) {
        return false;
    }
    for (std::size_t index = 0U;
         index < left.size();
         ++index) {
        if (left[index].derived_event_sequence !=
                right[index].derived_event_sequence ||
            left[index].source_tick_event_ordinal !=
                right[index].source_tick_event_ordinal ||
            left[index].source_tick_event_ordinal_valid !=
                right[index].source_tick_event_ordinal_valid ||
            StableShanghaiPayload(left[index].payload) !=
                StableShanghaiPayload(
                    right[index].payload) ||
            StableShanghaiPayload(left[index].payload) ==
                "NON_SHANGHAI") {
            return false;
        }
    }
    return true;
}

void TestShanghaiExternalRepair(bool* ok) {
    auto clean = History(ok);
    auto repaired = History(ok);
    if (clean == nullptr || repaired == nullptr) {
        return;
    }

    const std::array<ipc::RealtimeWireTickPayloadV2, 3U>
        clean_inputs{
            ShanghaiTrade(100, 10U, 5),
            ShanghaiAdd(101, 11U),
            ShanghaiTrade(102, 12U, 2)};
    const std::array<ipc::RealtimeWireTickPayloadV2, 3U>
        repaired_inputs{
            // Original arrival was native 100, 102, 101. The coordinator
            // supplies native order 100, 101, 102 while preserving arrival
            // tick anchors 10, 12, 11.
            ShanghaiTrade(100, 10U, 5),
            ShanghaiAdd(101, 12U),
            ShanghaiTrade(102, 11U, 2)};

    for (std::size_t index = 0U;
         index < clean_inputs.size();
         ++index) {
        const auto clean_error =
            clean->AppendCertifiedTick(
                clean_inputs[index],
                static_cast<std::uint64_t>(index) + 1U);
        const auto repaired_error =
            repaired->AppendCertifiedTick(
                repaired_inputs[index],
                static_cast<std::uint64_t>(index) + 1U);
        if (clean_error !=
            ipc::CertifiedOrderEventHistoryErrorV1::kNone) {
            std::cerr
                << "clean append index=" << index
                << " error="
                << ipc::CertifiedOrderEventHistoryErrorNameV1(
                       clean_error)
                << " wire="
                << ipc::WireOrderEventProjectionResultNameV2(
                       clean->last_wire_projection_result())
                << " sh="
                << market::
                       ShanghaiOrderAggregatorConsumeErrorNameV1(
                           clean->last_shanghai_error())
                << '\n';
        }
        if (repaired_error !=
            ipc::CertifiedOrderEventHistoryErrorV1::kNone) {
            std::cerr
                << "repaired append index=" << index
                << " error="
                << ipc::CertifiedOrderEventHistoryErrorNameV1(
                       repaired_error)
                << " wire="
                << ipc::WireOrderEventProjectionResultNameV2(
                       repaired
                           ->last_wire_projection_result())
                << " sh="
                << market::
                       ShanghaiOrderAggregatorConsumeErrorNameV1(
                           repaired->last_shanghai_error())
                << '\n';
        }
        *ok &= Expect(
            clean_error ==
                ipc::CertifiedOrderEventHistoryErrorV1::kNone,
            "append clean Shanghai canonical input");
        *ok &= Expect(
            repaired_error ==
                ipc::CertifiedOrderEventHistoryErrorV1::kNone,
            "append externally repaired Shanghai canonical input");
    }

    const auto clean_snapshot =
        Acquire(*clean, ok, "acquire clean Shanghai generation");
    const auto repaired_snapshot = Acquire(
        *repaired,
        ok,
        "acquire repaired Shanghai generation");
    const auto repaired_generation =
        repaired_snapshot.generation();
    *ok &= Expect(
        StableShanghaiEquivalent(
            clean_snapshot.events(),
            repaired_snapshot.events()),
        "clean and externally repaired Shanghai histories are structurally equivalent");
    *ok &= Expect(
        DenseDerivedSequences(repaired_snapshot.events()) &&
            SourceTickOrdinalsCanonical(
                repaired_snapshot.events()) &&
            repaired_generation.generation == 3U &&
            repaired_generation
                    .derived_event_sequence_exclusive ==
                repaired_snapshot.events().size() + 1U &&
            repaired_generation.input_frontier
                    .canonical_apply_sequence == 3U &&
            repaired_generation.input_frontier.market ==
                market::MarketV1::kShanghai &&
            repaired_generation.input_frontier.channel == 3 &&
            repaired_generation.input_frontier
                    .native_event_sequence == 102 &&
            repaired_generation.input_frontier
                    .source_sequence ==
                repaired_inputs[2U].common.source_sequence &&
            repaired_generation.input_frontier
                    .ingress_sequence ==
                repaired_inputs[2U].common.ingress_sequence &&
            repaired_generation.input_frontier
                    .tick_stream_sequence == 11U,
        "Shanghai generation publishes certified input and dense derived frontiers");

    for (const auto& event : repaired_snapshot.events()) {
        const SourceAnchorView anchor =
            SourceAnchor(event.payload);
        const ipc::RealtimeWireTickPayloadV2* expected = nullptr;
        for (const auto& input : repaired_inputs) {
            if (input.native_event_sequence ==
                anchor.native_sequence) {
                expected = &input;
                break;
            }
        }
        *ok &= Expect(
            expected != nullptr &&
                anchor.source_sequence ==
                    expected->common.source_sequence &&
                anchor.ingress_sequence ==
                    expected->common.ingress_sequence &&
                anchor.tick_stream_sequence ==
                    expected->common.tick_stream_sequence,
            "repaired Shanghai journal preserves original arrival anchors");
    }
    bool add_anchor_preserved = false;
    for (const auto& event : repaired_snapshot.events()) {
        const auto* revision =
            std::get_if<
                market::ShanghaiOrderRevisionEventV1>(
                &event.payload);
        if (revision != nullptr &&
            revision->source_anchor.native_event_sequence ==
                101) {
            add_anchor_preserved =
                revision->order.add_anchor.source_sequence ==
                    repaired_inputs[1U]
                        .common.source_sequence &&
                revision->order.add_anchor.ingress_sequence ==
                    repaired_inputs[1U]
                        .common.ingress_sequence &&
                revision->order.add_anchor
                        .tick_stream_sequence ==
                    repaired_inputs[1U]
                        .common.tick_stream_sequence;
        }
    }
    *ok &= Expect(
        add_anchor_preserved,
        "Shanghai T-to-A snapshot retains the repaired A arrival anchor");
}

void TestShenzhenLifecycleAndImmutableGeneration(bool* ok) {
    auto history = History(ok);
    if (history == nullptr) {
        return;
    }
    const auto order = ShenzhenOrder(1'000, 20U);
    const auto trade = ShenzhenTrade(1'001, 21U);
    const auto cancel = ShenzhenCancel(1'002, 22U);

    *ok &= Expect(
        history->AppendCertifiedTick(order, 1U) ==
            ipc::CertifiedOrderEventHistoryErrorV1::kNone,
        "append certified Shenzhen order");
    const auto order_generation = Acquire(
        *history,
        ok,
        "acquire immutable Shenzhen order generation");
    const auto order_events = order_generation.events();
    const auto* initial_revision =
        order_events.size() != 1U
            ? nullptr
            : std::get_if<
                  market::ShenzhenOrderRevisionEventV1>(
                  &order_events.front().payload);
    *ok &= Expect(
        order_generation.generation().generation == 1U &&
            initial_revision != nullptr &&
            initial_revision->order.first_anchor
                    .source_sequence ==
                order.common.source_sequence &&
            initial_revision->order.first_anchor
                    .ingress_sequence ==
                order.common.ingress_sequence &&
            initial_revision->order.first_anchor
                    .tick_stream_sequence ==
                order.common.tick_stream_sequence &&
            AppendedAnchorsEqual(
                order_events, 0U, order),
        "first Shenzhen generation contains lossless order revision");

    *ok &= Expect(
        history->AppendCertifiedTick(trade, 2U) ==
            ipc::CertifiedOrderEventHistoryErrorV1::kNone,
        "append certified Shenzhen trade");
    const auto trade_generation = Acquire(
        *history,
        ok,
        "acquire Shenzhen trade generation");
    *ok &= Expect(
        trade_generation.events().size() == 3U &&
            SourceTickOrdinalsCanonical(
                trade_generation.events()) &&
            trade_generation.events()[1U]
                    .source_tick_event_ordinal == 0U &&
            trade_generation.events()[2U]
                    .source_tick_event_ordinal == 1U &&
            std::holds_alternative<
                market::ShenzhenTradeEventV1>(
                trade_generation.events()[1U].payload) &&
            std::holds_alternative<
                market::ShenzhenOrderRevisionEventV1>(
                trade_generation.events()[2U].payload) &&
            AppendedAnchorsEqual(
                trade_generation.events(), 1U, trade),
        "Shenzhen trade appends source event and order revision");

    *ok &= Expect(
        history->AppendCertifiedTick(cancel, 3U) ==
            ipc::CertifiedOrderEventHistoryErrorV1::kNone,
        "append certified Shenzhen cancel");
    const auto cancel_generation = Acquire(
        *history,
        ok,
        "acquire Shenzhen cancel generation");
    const auto cancel_events = cancel_generation.events();
    *ok &= Expect(
        cancel_events.size() == 5U &&
            std::holds_alternative<
                market::ShenzhenCancelEventV1>(
                cancel_events[3U].payload) &&
            std::holds_alternative<
                market::ShenzhenOrderRevisionEventV1>(
                cancel_events[4U].payload) &&
            AppendedAnchorsEqual(cancel_events, 3U, cancel) &&
            DenseDerivedSequences(cancel_events) &&
            SourceTickOrdinalsCanonical(cancel_events) &&
            cancel_events[3U].source_tick_event_ordinal == 0U &&
            cancel_events[4U].source_tick_event_ordinal == 1U,
        "Shenzhen cancel appends lossless source event and revision");
    const auto* final_revision =
        cancel_events.size() != 5U
            ? nullptr
            : std::get_if<
                  market::ShenzhenOrderRevisionEventV1>(
                  &cancel_events[4U].payload);
    *ok &= Expect(
        final_revision != nullptr &&
            final_revision->order.remaining_quantity == 15 &&
            final_revision->order.total_trade_quantity == 3 &&
            final_revision->order.total_cancel_quantity == 2 &&
            final_revision->order.revision == 3U &&
            final_revision->order.last_anchor.source_sequence ==
                cancel.common.source_sequence &&
            final_revision->order.last_anchor.ingress_sequence ==
                cancel.common.ingress_sequence &&
            final_revision->order.last_anchor
                    .tick_stream_sequence ==
                cancel.common.tick_stream_sequence,
        "Shenzhen order/trade/cancel lifecycle state is exact");

    // The old snapshot shares fixed storage but retains its published prefix
    // and generation boundary after two later writes.
    *ok &= Expect(
        order_generation.generation().generation == 1U &&
            order_generation.events().size() == 1U &&
            order_generation.events().front()
                    .derived_event_sequence == 1U &&
            SourceAnchor(
                order_generation.events().front().payload)
                    .tick_stream_sequence == 20U &&
            cancel_generation.generation().generation == 3U &&
            cancel_generation.generation()
                    .derived_event_sequence_exclusive == 6U &&
            cancel_generation.generation()
                    .shenzhen_order_state_count == 1U,
        "acquired history generation remains immutable across later publication");
}

void TestFilteredSkipAndFailureBoundaries(bool* ok) {
    {
        auto history = History(ok);
        if (history != nullptr) {
            const auto first = ShanghaiAdd(100, 30U, 3'000);
            const auto after_filtered =
                ShanghaiAdd(102, 31U, 3'001);
            auto independent_channel =
                ShanghaiAdd(100, 32U, 3'002);
            independent_channel.channel = 4;
            *ok &= Expect(
                history->AppendCertifiedTick(first, 1U) ==
                        ipc::CertifiedOrderEventHistoryErrorV1::
                            kNone &&
                    history->AppendCertifiedTick(
                        after_filtered, 2U) ==
                        ipc::CertifiedOrderEventHistoryErrorV1::
                            kNone &&
                    history->AppendCertifiedTick(
                        independent_channel, 3U) ==
                        ipc::CertifiedOrderEventHistoryErrorV1::
                            kNone,
                "certified filtered native position does not create a false target gap");
            const auto snapshot = Acquire(
                *history,
                ok,
                "acquire filtered-skip generation");
            *ok &= Expect(
                snapshot.generation().generation == 3U &&
                    snapshot.generation().input_frontier
                            .native_event_sequence == 100 &&
                    snapshot.generation().input_frontier.channel ==
                        4 &&
                    snapshot.events().size() == 3U,
                "filtered position and independent channel do not imply a cross-channel native total order");
        }
    }

    {
        auto history = History(ok);
        if (history != nullptr) {
            const auto first = ShanghaiAdd(500, 40U, 4'000);
            const auto next = ShanghaiAdd(501, 41U, 4'001);
            *ok &= Expect(
                history->AppendCertifiedTick(first, 5U) ==
                    ipc::CertifiedOrderEventHistoryErrorV1::
                        kNone,
                "append canonical regression baseline");
            const auto before = Acquire(
                *history,
                ok,
                "acquire generation before canonical regression");
            *ok &= Expect(
                history->AppendCertifiedTick(next, 4U) ==
                        ipc::CertifiedOrderEventHistoryErrorV1::
                            kCanonicalSequence &&
                    history->failed() &&
                    history->last_error() ==
                        ipc::CertifiedOrderEventHistoryErrorV1::
                            kCanonicalSequence &&
                    history->AppendCertifiedTick(next, 6U) ==
                        ipc::CertifiedOrderEventHistoryErrorV1::
                            kFailed,
                "canonical regression permanently fail-closes certified writer");
            const auto after = Acquire(
                *history,
                ok,
                "last correct generation remains readable after fail-close");
            *ok &= Expect(
                before.generation().generation == 1U &&
                    after.generation().generation == 1U &&
                    before.events().size() == 1U &&
                    after.events().size() == 1U,
                "canonical failure cannot publish a partial generation");
        }
    }

    {
        auto history = History(ok, 1U);
        if (history != nullptr) {
            const auto trade = ShanghaiTrade(700, 50U, 1);
            *ok &= Expect(
                history->AppendCertifiedTick(trade, 1U) ==
                        ipc::CertifiedOrderEventHistoryErrorV1::
                            kEventCapacity &&
                    history->failed(),
                "bounded journal rejects an input whose lossless batch cannot fit");
            const auto snapshot = Acquire(
                *history,
                ok,
                "acquire empty generation after capacity fail-close");
            *ok &= Expect(
                snapshot.generation().generation == 0U &&
                    snapshot.events().empty(),
                "capacity failure leaves the last immutable generation unchanged");
        }
    }

    {
        auto history = History(ok, 16U, 1U);
        if (history != nullptr) {
            const auto first = ShenzhenOrder(750, 55U);
            const auto overflow = ShenzhenOrder(751, 56U);
            *ok &= Expect(
                history->AppendCertifiedTick(first, 1U) ==
                        ipc::CertifiedOrderEventHistoryErrorV1::
                            kNone &&
                    history->AppendCertifiedTick(overflow, 2U) ==
                        ipc::CertifiedOrderEventHistoryErrorV1::
                            kAggregationError &&
                    history->last_shenzhen_error() ==
                        market::
                            ShenzhenOrderProjectorConsumeErrorV1::
                                kOrderCapacity &&
                    history->failed(),
                "bounded Shenzhen order-state capacity fail-closes certified projector");
            const auto snapshot = Acquire(
                *history,
                ok,
                "acquire generation after order-state capacity failure");
            *ok &= Expect(
                snapshot.generation().generation == 1U &&
                    snapshot.events().size() == 1U,
                "order-state capacity failure cannot publish a partial journal");
        }
    }

    {
        auto history = History(ok);
        if (history != nullptr) {
            const auto first = ShenzhenOrder(800, 60U);
            const auto regression = ShenzhenTrade(799, 61U, 800);
            *ok &= Expect(
                history->AppendCertifiedTick(first, 1U) ==
                        ipc::CertifiedOrderEventHistoryErrorV1::
                            kNone &&
                    history->AppendCertifiedTick(
                        regression, 2U) ==
                        ipc::CertifiedOrderEventHistoryErrorV1::
                            kNativeSequenceRegression &&
                    history->failed(),
                "native duplicate or regression is rejected per market channel");
        }
    }
}

void TestSparseLargeJournalReservation(bool* ok) {
    constexpr std::size_t kProductionEventCapacity =
        16'000'000U;
    constexpr std::uint64_t kMaximumResidentGrowth =
        64U * 1024U * 1024U;

    static_cast<void>(ResidentSetBytes());
    const std::optional<std::uint64_t> before =
        ResidentSetBytes();
    std::unique_ptr<ipc::CertifiedOrderEventHistoryV1> history;
    const auto create_error =
        ipc::CertifiedOrderEventHistoryV1::Create(
            {.trade_date = kTradeDate,
             .maximum_shanghai_order_states = 8U,
             .maximum_shenzhen_order_states = 8U,
             .maximum_events = kProductionEventCapacity,
             .external_journal = {}},
            &history);
    const std::optional<std::uint64_t> after =
        ResidentSetBytes();

    *ok &= Expect(
        before.has_value() && after.has_value(),
        "read Linux resident-set accounting for sparse journal");
    *ok &= Expect(
        create_error ==
                ipc::CertifiedOrderEventHistoryErrorV1::kNone &&
            history != nullptr,
        "production-sized journal reserves virtual capacity without constructing every event");
    if (history == nullptr) {
        return;
    }
    {
        const auto snapshot = Acquire(
            *history,
            ok,
            "acquire empty production-sized sparse journal");
        *ok &= Expect(
            snapshot.events().empty() &&
                snapshot.generation().generation == 0U &&
                history->config().maximum_events ==
                    kProductionEventCapacity,
            "production-sized sparse journal starts with an empty immutable prefix");
    }

    if (before.has_value() && after.has_value()) {
        const std::uint64_t growth =
            *after > *before ? *after - *before : 0U;
        *ok &= Expect(
            growth <= kMaximumResidentGrowth,
            "production-sized sparse journal does not precommit gigabytes of resident memory");
    }

    const auto one = ShenzhenOrder(1'500, 70U);
    *ok &= Expect(
        history->AppendCertifiedTick(one, 1U) ==
            ipc::CertifiedOrderEventHistoryErrorV1::kNone,
        "append one event into production-sized sparse journal");
    const auto retained = Acquire(
        *history,
        ok,
        "acquire production-sized sparse journal prefix");
    history.reset();
    *ok &= Expect(
        retained.valid() &&
            retained.events().size() == 1U &&
            retained.events().front().derived_event_sequence ==
                1U,
        "snapshot keeps mapped journal alive after projector destruction");
}

void TestConfigurationOverflowAndConcurrentAcquire(bool* ok) {
    {
        std::unique_ptr<ipc::CertifiedOrderEventHistoryV1> history;
        const std::size_t overflow_capacity =
            static_cast<std::size_t>(
                std::numeric_limits<std::ptrdiff_t>::max()) /
                sizeof(ipc::InstrumentDerivedEventV1) +
            1U;
        *ok &= Expect(
            ipc::CertifiedOrderEventHistoryV1::Create(
                {.trade_date = kTradeDate,
                 .maximum_shanghai_order_states = 1U,
                 .maximum_shenzhen_order_states = 1U,
                 .maximum_events = overflow_capacity,
                 .external_journal = {}},
                &history) ==
                    ipc::CertifiedOrderEventHistoryErrorV1::
                        kInvalidConfiguration &&
                history == nullptr,
            "journal rejects a capacity whose contiguous span exceeds PTRDIFF_MAX");
    }

    constexpr std::uint64_t kInputs = 512U;
    auto history = History(ok, 1'024U, 1'024U);
    if (history == nullptr) {
        return;
    }
    std::atomic<bool> writer_done{false};
    std::atomic<bool> reader_ok{true};
    std::thread reader([&history, &writer_done, &reader_ok]() {
        do {
            ipc::CertifiedOrderEventHistorySnapshotV1 snapshot;
            if (history->AcquireGeneration(&snapshot) !=
                    ipc::CertifiedOrderEventHistoryErrorV1::
                        kNone ||
                !snapshot.valid()) {
                reader_ok.store(false, std::memory_order_relaxed);
                return;
            }
            const auto generation = snapshot.generation();
            const auto events = snapshot.events();
            if (generation.generation !=
                    generation.event_count ||
                generation.derived_event_sequence_exclusive !=
                    generation.event_count + 1U ||
                events.size() != generation.event_count ||
                (generation.generation != 0U &&
                 (generation.input_frontier
                          .canonical_apply_sequence !=
                      generation.generation ||
                  events.back().derived_event_sequence !=
                      generation.event_count))) {
                reader_ok.store(false, std::memory_order_relaxed);
                return;
            }
        } while (!writer_done.load(std::memory_order_acquire));
    });

    for (std::uint64_t index = 0U;
         index < kInputs;
         ++index) {
        const std::int64_t native_sequence =
            2'000 + static_cast<std::int64_t>(index);
        const auto input = ShanghaiAdd(
            native_sequence,
            100U + index,
            10'000 + static_cast<std::int64_t>(index));
        if (history->AppendCertifiedTick(input, index + 1U) !=
            ipc::CertifiedOrderEventHistoryErrorV1::kNone) {
            *ok &= Expect(
                false,
                "append while concurrently acquiring certified generations");
            break;
        }
    }
    writer_done.store(true, std::memory_order_release);
    reader.join();
    *ok &= Expect(
        reader_ok.load(std::memory_order_relaxed),
        "concurrent Acquire observes only coherent published generation prefixes");
}

}  // namespace

int main() {
    bool ok = true;
    TestShanghaiExternalRepair(&ok);
    TestShenzhenLifecycleAndImmutableGeneration(&ok);
    TestFilteredSkipAndFailureBoundaries(&ok);
    TestSparseLargeJournalReservation(&ok);
    TestConfigurationOverflowAndConcurrentAcquire(&ok);
    return ok ? 0 : 1;
}
