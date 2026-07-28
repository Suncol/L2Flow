#include "l2flow/ipc/realtime_wire_projection_v1.h"

#include "l2flow/market/market_types_v1.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string>
#include <type_traits>
#include <variant>

namespace l2flow::ipc {
namespace {

namespace market = l2flow::market;

RealtimeWireDecimalV1 Decimal(
    const market::DecimalValueV1& value) noexcept {
    RealtimeWireDecimalV1 result{};
    result.raw = value.raw;
    result.normalized_p6 = value.normalized_p6;
    result.scale = value.scale;
    result.valid = value.valid ? 1U : 0U;
    result.is_null = value.is_null ? 1U : 0U;
    return result;
}

RealtimeWireQuantityV1 Quantity(
    const market::QuantityValueV1& value) noexcept {
    RealtimeWireQuantityV1 result{};
    result.raw = value.raw;
    result.scale = value.scale;
    result.valid = value.valid ? 1U : 0U;
    result.is_null = value.is_null ? 1U : 0U;
    return result;
}

bool Common(
    const market::RealtimeHistoryRecordV1& record,
    std::size_t registry_ordinal,
    const market::DecodedMarketCommonV1& common,
    std::uint32_t record_bytes,
    RealtimeWireCommonRecordV1* output) noexcept {
    if (output == nullptr ||
        registry_ordinal >
            static_cast<std::size_t>(
                std::numeric_limits<std::uint32_t>::max()) ||
        common.instrument_id != record.instrument_id() ||
        common.registry_ordinal != registry_ordinal ||
        common.kind != record.kind() ||
        common.origin.source_stream_id !=
            record.source_stream_id() ||
        common.origin.source_sequence != record.source_sequence() ||
        static_cast<std::uint8_t>(common.quantity_unit) >
            static_cast<std::uint8_t>(
                market::QuantityUnitV1::kIndexUnit) ||
        static_cast<std::uint8_t>(common.security_type) >
            static_cast<std::uint8_t>(
                market::SecurityTypeV1::kOption) ||
        static_cast<std::uint8_t>(common.asset_scope) >
            static_cast<std::uint8_t>(
                market::AssetScopeV1::kOutsideDocumentedCore)) {
        return false;
    }
    const market::MarketV1 expected_market =
        record.kind() ==
                    market::MarketEventKindV1::kShanghaiSnapshot ||
                record.kind() ==
                    market::MarketEventKindV1::kShanghaiTick
            ? market::MarketV1::kShanghai
            : market::MarketV1::kShenzhen;
    if (common.market != expected_market) {
        return false;
    }
    RealtimeWireCommonRecordV1 result{};
    result.record_bytes = record_bytes;
    result.instrument_id = record.instrument_id();
    result.registry_ordinal =
        static_cast<std::uint32_t>(registry_ordinal);
    result.source_sequence = record.source_sequence();
    result.ingress_sequence = record.ingress_sequence();
    result.tick_stream_sequence = record.tick_stream_sequence();
    result.vendor_sequence_id = common.origin.vendor_sequence_id;
    result.event_time_unix_ns = record.event_time_ns();
    result.recv_realtime_ns = record.recv_realtime_ns();
    result.recv_monotonic_ns = record.recv_monotonic_ns();
    result.exchange_time_ns_since_midnight =
        common.exchange_time.nanoseconds_since_midnight;
    result.quality_flags = common.quality_flags;
    result.market_notices = common.market_notices;
    result.source_stream_id = record.source_stream_id();
    result.trade_date = common.origin.trade_date;
    result.vendor_local_time_raw =
        common.origin.vendor_local_time_raw;
    result.source_slot = record.source_slot();
    result.event_kind =
        static_cast<std::uint8_t>(record.kind());
    result.market = static_cast<std::uint8_t>(common.market);
    result.quantity_unit =
        static_cast<std::uint8_t>(common.quantity_unit);
    result.security_type =
        static_cast<std::uint8_t>(common.security_type);
    result.asset_scope =
        static_cast<std::uint8_t>(common.asset_scope);
    *output = result;
    return true;
}

void Book(
    const market::SnapshotBookV1& source,
    RealtimeWireSnapshotPayloadV1* output) noexcept {
    output->actual_bid_depth = source.actual_bid_depth;
    output->actual_ask_depth = source.actual_ask_depth;
    output->retained_bid_depth = source.retained_bid_depth;
    output->retained_ask_depth = source.retained_ask_depth;
    for (std::size_t index = 0U; index < source.bids.size(); ++index) {
        output->bids[index].price = Decimal(source.bids[index].price);
        output->bids[index].quantity =
            Quantity(source.bids[index].quantity);
        output->bids[index].order_count =
            source.bids[index].order_count;
        output->bids[index].order_count_valid =
            source.bids[index].order_count_valid ? 1U : 0U;
        output->asks[index].price = Decimal(source.asks[index].price);
        output->asks[index].quantity =
            Quantity(source.asks[index].quantity);
        output->asks[index].order_count =
            source.asks[index].order_count;
        output->asks[index].order_count_valid =
            source.asks[index].order_count_valid ? 1U : 0U;
    }
    output->bid1_queue.total_order_count =
        source.bid1_queue.total_order_count;
    output->bid1_queue.actual_revealed_count =
        source.bid1_queue.actual_revealed_count;
    output->bid1_queue.retained_count =
        source.bid1_queue.retained_count;
    output->ask1_queue.total_order_count =
        source.ask1_queue.total_order_count;
    output->ask1_queue.actual_revealed_count =
        source.ask1_queue.actual_revealed_count;
    output->ask1_queue.retained_count =
        source.ask1_queue.retained_count;
    for (std::size_t index = 0U;
         index < source.bid1_queue.quantities.size();
         ++index) {
        output->bid1_queue_quantities[index] =
            Quantity(source.bid1_queue.quantities[index]);
        output->ask1_queue_quantities[index] =
            Quantity(source.ask1_queue.quantities[index]);
    }
}

bool BookRepresentable(
    const market::SnapshotBookV1& book) noexcept {
    return book.retained_bid_depth <= book.bids.size() &&
           book.retained_ask_depth <= book.asks.size() &&
           book.bid1_queue.retained_count <=
               book.bid1_queue.quantities.size() &&
           book.ask1_queue.retained_count <=
               book.ask1_queue.quantities.size();
}

bool TickFieldsRepresentable(
    const market::TickFieldsV1& fields) noexcept {
    return static_cast<std::uint8_t>(fields.action) <=
               static_cast<std::uint8_t>(
                   market::TickActionV1::kStatus) &&
           static_cast<std::uint8_t>(fields.side) <=
               static_cast<std::uint8_t>(
                   market::SideV1::kLend) &&
           static_cast<std::uint8_t>(fields.order_type) <=
               static_cast<std::uint8_t>(
                   market::OrderTypeV1::kSameSideBest) &&
           static_cast<std::uint8_t>(fields.aggressor) <=
               static_cast<std::uint8_t>(
                   market::AggressorV1::kNeutral) &&
           static_cast<std::uint8_t>(fields.phase) <=
               static_cast<std::uint8_t>(
                   market::TradingPhaseV1::kEnd);
}

void TickFields(
    const market::TickFieldsV1& source,
    RealtimeWireTickPayloadV1* output) noexcept {
    output->validity_bitmap = source.validity_bitmap;
    output->action = static_cast<std::uint8_t>(source.action);
    output->side = static_cast<std::uint8_t>(source.side);
    output->order_type =
        static_cast<std::uint8_t>(source.order_type);
    output->aggressor =
        static_cast<std::uint8_t>(source.aggressor);
    output->phase = static_cast<std::uint8_t>(source.phase);
    output->price = Decimal(source.price);
    output->quantity = Quantity(source.quantity);
    output->trade_amount = Decimal(source.trade_amount);
    output->matched_quantity = Quantity(source.matched_quantity);
    output->primary_order_id = source.primary_order_id;
    output->buy_order_id = source.buy_order_id;
    output->sell_order_id = source.sell_order_id;
}

bool CopyRaw(
    const std::string& source,
    std::array<std::uint8_t, 32U>* destination,
    std::uint8_t* length,
    std::uint32_t omission_flag,
    std::uint32_t* projection_flags) noexcept {
    if (destination == nullptr || length == nullptr ||
        projection_flags == nullptr) {
        return false;
    }
    if (source.size() > destination->size()) {
        destination->fill(0U);
        *length = 0U;
        *projection_flags |= omission_flag;
        return true;
    }
    destination->fill(0U);
    std::transform(
        source.begin(),
        source.end(),
        destination->begin(),
        [](char value) noexcept {
            return static_cast<std::uint8_t>(
                static_cast<unsigned char>(value));
        });
    *length = static_cast<std::uint8_t>(source.size());
    return true;
}

bool ProjectTickCore(
    const market::RealtimeHistoryRecordV1& record,
    std::size_t registry_ordinal,
    RealtimeWireTickPayloadV1* output,
    std::uint32_t* projection_flags) noexcept {
    if (output == nullptr || projection_flags == nullptr ||
        !market::IsTickEventKindV1(record.kind())) {
        return false;
    }
    RealtimeWireTickPayloadV1 projected{};
    std::uint32_t projected_flags = 0U;
    const bool ok = std::visit(
        [&](const auto* event) noexcept -> bool {
            using Pointer = std::decay_t<decltype(event)>;
            using Event = std::remove_cv_t<
                std::remove_pointer_t<Pointer>>;
            if (event == nullptr) {
                return false;
            }
            if constexpr (
                std::is_same_v<Event, market::ShanghaiTickV1>) {
                if (record.kind() !=
                        market::MarketEventKindV1::kShanghaiTick ||
                    !TickFieldsRepresentable(event->fields) ||
                    !Common(
                        record,
                        registry_ordinal,
                        event->common,
                        static_cast<std::uint32_t>(
                            sizeof(projected)),
                        &projected.common) ||
                    !CopyRaw(
                        event->raw_type,
                        &projected.raw_type,
                        &projected.raw_type_length,
                        kRealtimeWireTickRawTypeOmittedV1,
                        &projected_flags) ||
                    !CopyRaw(
                        event->raw_tick_flag,
                        &projected.raw_tick_flag,
                        &projected.raw_tick_flag_length,
                        kRealtimeWireTickRawTickFlagOmittedV1,
                        &projected_flags)) {
                    return false;
                }
                projected.channel =
                    static_cast<std::int64_t>(event->channel);
                projected.native_event_sequence =
                    event->business_index;
                TickFields(event->fields, &projected);
                return true;
            } else if constexpr (
                std::is_same_v<Event, market::ShenzhenOrderV1>) {
                if (record.kind() !=
                        market::MarketEventKindV1::kShenzhenOrder ||
                    !TickFieldsRepresentable(event->fields) ||
                    !Common(
                        record,
                        registry_ordinal,
                        event->common,
                        static_cast<std::uint32_t>(
                            sizeof(projected)),
                        &projected.common)) {
                    return false;
                }
                projected.channel =
                    static_cast<std::int64_t>(event->channel);
                projected.native_event_sequence =
                    event->application_sequence;
                projected.source_raw_code_1 = event->raw_side;
                projected.source_raw_code_2 = event->raw_order_type;
                TickFields(event->fields, &projected);
                return true;
            } else if constexpr (
                std::is_same_v<
                    Event,
                    market::ShenzhenTransactionV1>) {
                if (record.kind() !=
                        market::MarketEventKindV1::
                            kShenzhenTransaction ||
                    !TickFieldsRepresentable(event->fields) ||
                    !Common(
                        record,
                        registry_ordinal,
                        event->common,
                        static_cast<std::uint32_t>(
                            sizeof(projected)),
                        &projected.common)) {
                    return false;
                }
                projected.channel =
                    static_cast<std::int64_t>(event->channel);
                projected.native_event_sequence =
                    event->application_sequence;
                projected.source_raw_code_1 =
                    event->raw_execution_type;
                TickFields(event->fields, &projected);
                return true;
            } else {
                return false;
            }
        },
        record.event());
    if (!ok) {
        return false;
    }
    projected.projection_flags = projected_flags;
    *output = projected;
    *projection_flags = projected_flags;
    return true;
}

}  // namespace

bool ProjectSnapshotWireV1(
    const market::RealtimeHistoryRecordV1& record,
    std::size_t registry_ordinal,
    RealtimeWireSnapshotPayloadV1* output) noexcept {
    if (output == nullptr ||
        !market::IsSnapshotEventKindV1(record.kind())) {
        return false;
    }
    RealtimeWireSnapshotPayloadV1 projected{};
    const bool ok = std::visit(
        [&](const auto* event) noexcept -> bool {
            using Pointer = std::decay_t<decltype(event)>;
            using Event = std::remove_cv_t<
                std::remove_pointer_t<Pointer>>;
            if (event == nullptr) {
                return false;
            }
            if constexpr (
                std::is_same_v<Event, market::ShanghaiSnapshotV1>) {
                if (record.kind() !=
                        market::MarketEventKindV1::kShanghaiSnapshot ||
                    !BookRepresentable(event->book) ||
                    !Common(
                        record,
                        registry_ordinal,
                        event->common,
                        static_cast<std::uint32_t>(
                            sizeof(projected)),
                        &projected.common)) {
                    return false;
                }
                projected.trade_count =
                    static_cast<std::int64_t>(event->trade_count);
                projected.image_status = event->image_status;
                projected.pre_close_price =
                    Decimal(event->pre_close_price);
                projected.open_price = Decimal(event->open_price);
                projected.high_price = Decimal(event->high_price);
                projected.low_price = Decimal(event->low_price);
                projected.last_price = Decimal(event->last_price);
                projected.close_price = Decimal(event->close_price);
                projected.trade_volume =
                    Quantity(event->trade_volume);
                projected.turnover = Decimal(event->turnover);
                projected.total_bid_quantity =
                    Quantity(event->total_bid_volume);
                projected.weighted_average_bid_price =
                    Decimal(event->weighted_average_bid_price);
                projected.total_ask_quantity =
                    Quantity(event->total_ask_volume);
                projected.weighted_average_ask_price =
                    Decimal(event->weighted_average_ask_price);
                projected.iopv = Decimal(event->iopv);
                Book(event->book, &projected);
                return true;
            } else if constexpr (
                std::is_same_v<Event, market::ShenzhenSnapshotV1>) {
                if (record.kind() !=
                        market::MarketEventKindV1::kShenzhenSnapshot ||
                    !BookRepresentable(event->book) ||
                    !Common(
                        record,
                        registry_ordinal,
                        event->common,
                        static_cast<std::uint32_t>(
                            sizeof(projected)),
                        &projected.common)) {
                    return false;
                }
                projected.trade_count = event->trade_count;
                projected.channel = event->channel;
                projected.pre_close_price =
                    Decimal(event->pre_close_price);
                projected.open_price = Decimal(event->open_price);
                projected.high_price = Decimal(event->high_price);
                projected.low_price = Decimal(event->low_price);
                projected.last_price = Decimal(event->last_price);
                projected.trade_volume = Quantity(event->volume);
                projected.turnover = Decimal(event->turnover);
                projected.total_bid_quantity =
                    Quantity(event->total_bid_quantity);
                projected.weighted_average_bid_price =
                    Decimal(event->weighted_average_bid_price);
                projected.total_ask_quantity =
                    Quantity(event->total_ask_quantity);
                projected.weighted_average_ask_price =
                    Decimal(event->weighted_average_ask_price);
                projected.high_limit_price =
                    Decimal(event->high_limit_price);
                projected.low_limit_price =
                    Decimal(event->low_limit_price);
                projected.iopv = Decimal(event->iopv);
                projected.open_interest =
                    Quantity(event->open_interest);
                Book(event->book, &projected);
                return true;
            } else {
                return false;
            }
        },
        record.event());
    if (!ok) {
        return false;
    }
    *output = projected;
    return true;
}

bool ProjectTickWireV1(
    const market::RealtimeHistoryRecordV1& record,
    std::size_t registry_ordinal,
    RealtimeWireTickPayloadV1* output) noexcept {
    if (output == nullptr || !market::IsTickEventKindV1(record.kind()) ||
        record.tick_stream_sequence() == 0U) {
        return false;
    }
    std::uint32_t projection_flags = 0U;
    return ProjectTickCore(
        record,
        registry_ordinal,
        output,
        &projection_flags);
}

bool ProjectHistoryTickWireV1(
    const market::RealtimeHistoryRecordV1& record,
    std::size_t registry_ordinal,
    RealtimeWireTickPayloadV1* output,
    std::uint32_t* projection_flags) noexcept {
    return ProjectTickCore(
        record,
        registry_ordinal,
        output,
        projection_flags);
}

bool ProjectKLineWireV1(
    std::uint64_t generation,
    const market::KLineBarV1& bar,
    RealtimeWireKLinePayloadV1* output) noexcept {
    if (output == nullptr || generation == 0U ||
        bar.instrument_id == 0U || bar.window_id == 0U) {
        return false;
    }
    RealtimeWireKLinePayloadV1 projected{};
    projected.generation = generation;
    projected.trade_date = bar.trade_date;
    projected.instrument_id = bar.instrument_id;
    projected.window_id = bar.window_id;
    projected.window_duration_ns = bar.window_duration_ns;
    projected.window_start_ns_since_midnight =
        bar.window_start_ns_since_midnight;
    projected.window_end_ns_since_midnight =
        bar.window_end_ns_since_midnight;
    projected.window_start_unix_ns = bar.window_start_unix_ns;
    projected.window_end_unix_ns = bar.window_end_unix_ns;
    projected.open_price_p6 = bar.open_price_p6;
    projected.high_price_p6 = bar.high_price_p6;
    projected.low_price_p6 = bar.low_price_p6;
    projected.close_price_p6 = bar.close_price_p6;
    projected.volume_raw = bar.volume_raw;
    projected.trade_count = bar.trade_count;
    projected.revision = bar.revision;
    projected.first_event_time_ns_since_midnight =
        bar.first_trade.event_time_ns_since_midnight;
    projected.first_event_sequence =
        bar.first_trade.event_sequence;
    projected.first_source_sequence =
        bar.first_trade.source_sequence;
    projected.first_ingress_sequence =
        bar.first_trade.ingress_sequence;
    projected.last_event_time_ns_since_midnight =
        bar.last_trade.event_time_ns_since_midnight;
    projected.last_event_sequence = bar.last_trade.event_sequence;
    projected.last_source_sequence = bar.last_trade.source_sequence;
    projected.last_ingress_sequence = bar.last_trade.ingress_sequence;
    projected.volume_scale = bar.volume_scale;
    projected.quantity_unit =
        static_cast<std::uint8_t>(bar.quantity_unit);
    projected.present = 1U;
    *output = projected;
    return true;
}

}  // namespace l2flow::ipc
