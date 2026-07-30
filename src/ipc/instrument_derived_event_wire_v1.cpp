#include "l2flow/ipc/instrument_derived_event_wire_v1.h"

#include <cstdint>
#include <variant>

namespace l2flow::ipc {
namespace {

template <typename Anchor>
void CopyAnchor(
    const Anchor& source,
    l2flow_instrument_derived_event_row_v1* output) noexcept {
    output->native_event_sequence = source.native_event_sequence;
    output->source_sequence = source.source_sequence;
    output->ingress_sequence = source.ingress_sequence;
    output->tick_stream_sequence = source.tick_stream_sequence;
    output->vendor_sequence_id = source.vendor_sequence_id;
    output->event_time_ns_since_midnight =
        source.event_time_ns_since_midnight;
    output->event_time_unix_ns = source.event_time_unix_ns;
    output->recv_realtime_ns = source.recv_realtime_ns;
    output->recv_monotonic_ns = source.recv_monotonic_ns;
    output->vendor_local_time_raw = source.vendor_local_time_raw;
    output->vendor_local_time_ns_since_midnight =
        source.vendor_local_time_ns_since_midnight;
    output->event_time_valid = source.event_time_valid ? 1U : 0U;
    output->event_time_unix_ns_valid =
        source.event_time_unix_ns_valid ? 1U : 0U;
    output->vendor_local_time_valid =
        source.vendor_local_time_valid ? 1U : 0U;
}

void Flatten(
    const market::ShanghaiOrderRevisionEventV1& source,
    l2flow_instrument_derived_event_row_v1* output) noexcept {
    const auto& order = source.order;
    output->market =
        L2FLOW_INSTRUMENT_DERIVED_EVENT_MARKET_SHANGHAI_V1;
    output->event_kind =
        L2FLOW_INSTRUMENT_DERIVED_EVENT_ORDER_REVISION_V1;
    output->trade_date = order.key.trade_date;
    output->instrument_id = order.key.instrument_id;
    output->channel = order.key.channel;
    output->order_id = order.key.order_id;
    output->operation =
        static_cast<std::uint8_t>(source.operation);
    output->finality =
        static_cast<std::uint8_t>(order.finality);
    output->side = static_cast<std::uint8_t>(order.side);
    output->side_source =
        static_cast<std::uint8_t>(order.side_source);
    output->phase_at_first =
        static_cast<std::uint8_t>(order.phase_at_first);
    output->phase_at_add =
        static_cast<std::uint8_t>(order.phase_at_add);
    output->phase_at_last =
        static_cast<std::uint8_t>(order.phase_at_last);
    output->order_source =
        static_cast<std::uint8_t>(order.order_source);
    output->price_source =
        static_cast<std::uint8_t>(order.price_source);
    output->original_quantity_status =
        static_cast<std::uint8_t>(
            order.original_quantity_status);
    output->price_p6 = order.price_p6;
    output->price_valid = order.price_valid ? 1U : 0U;
    output->execution_boundary_price_p6 =
        order.execution_boundary_price_p6;
    output->execution_boundary_price_valid =
        order.execution_boundary_price_valid ? 1U : 0U;
    output->published_quantity = order.published_quantity;
    output->published_quantity_valid =
        order.published_quantity_valid ? 1U : 0U;
    output->original_quantity = order.original_quantity;
    output->original_quantity_valid =
        order.original_quantity_valid ? 1U : 0U;
    output->remaining_quantity = order.remaining_quantity;
    output->remaining_quantity_valid =
        order.remaining_quantity_valid ? 1U : 0U;
    output->source_matched_quantity =
        order.source_matched_quantity;
    output->source_matched_quantity_valid =
        order.source_matched_quantity_valid ? 1U : 0U;
    output->observed_pre_add_trade_quantity =
        order.observed_pre_add_trade_quantity;
    output->post_add_trade_quantity =
        order.post_add_trade_quantity;
    output->total_trade_quantity =
        order.total_trade_quantity;
    output->total_cancel_quantity =
        order.total_cancel_quantity;
    output->revision = order.revision;
    output->trade_count = order.trade_count;
    output->quality_flags = order.quality_flags;
    output->source_quality_flags =
        order.source_quality_flags;
    output->source_market_notices =
        order.source_market_notices;
    output->add_seen = order.add_seen ? 1U : 0U;
    output->apply_to_book = order.apply_to_book ? 1U : 0U;
    CopyAnchor(source.source_anchor, output);
}

void Flatten(
    const market::ShanghaiTradeEventV1& source,
    l2flow_instrument_derived_event_row_v1* output) noexcept {
    output->market =
        L2FLOW_INSTRUMENT_DERIVED_EVENT_MARKET_SHANGHAI_V1;
    output->event_kind =
        L2FLOW_INSTRUMENT_DERIVED_EVENT_TRADE_V1;
    output->trade_date = source.trade_date;
    output->instrument_id = source.instrument_id;
    output->channel = source.channel;
    output->buy_order_id = source.buy_order_id;
    output->sell_order_id = source.sell_order_id;
    output->aggressor =
        static_cast<std::uint8_t>(source.aggressor);
    output->phase = static_cast<std::uint8_t>(source.phase);
    output->price_p6 = source.price_p6;
    output->price_valid = 1U;
    output->quantity = source.quantity;
    output->trade_amount_p6 = source.trade_amount_p6;
    output->trade_amount_valid =
        source.trade_amount_valid ? 1U : 0U;
    output->source_quality_flags =
        source.source_quality_flags;
    output->source_market_notices =
        source.source_market_notices;
    CopyAnchor(source.source_anchor, output);
}

void Flatten(
    const market::ShanghaiCancelEventV1& source,
    l2flow_instrument_derived_event_row_v1* output) noexcept {
    output->market =
        L2FLOW_INSTRUMENT_DERIVED_EVENT_MARKET_SHANGHAI_V1;
    output->event_kind =
        L2FLOW_INSTRUMENT_DERIVED_EVENT_CANCEL_V1;
    output->trade_date = source.key.trade_date;
    output->instrument_id = source.key.instrument_id;
    output->channel = source.key.channel;
    output->order_id = source.key.order_id;
    output->side = static_cast<std::uint8_t>(source.side);
    output->phase = static_cast<std::uint8_t>(source.phase);
    output->quantity = source.quantity;
    output->referenced_order_found =
        source.referenced_order_found ? 1U : 0U;
    output->source_quality_flags =
        source.source_quality_flags;
    output->source_market_notices =
        source.source_market_notices;
    CopyAnchor(source.source_anchor, output);
}

void Flatten(
    const market::ShanghaiStatusEventV1& source,
    l2flow_instrument_derived_event_row_v1* output) noexcept {
    output->market =
        L2FLOW_INSTRUMENT_DERIVED_EVENT_MARKET_SHANGHAI_V1;
    output->event_kind =
        L2FLOW_INSTRUMENT_DERIVED_EVENT_STATUS_V1;
    output->trade_date = source.trade_date;
    output->instrument_id = source.instrument_id;
    output->channel = source.channel;
    output->phase = static_cast<std::uint8_t>(source.phase);
    output->quality_flags = source.quality_flags;
    output->source_quality_flags =
        source.source_quality_flags;
    output->source_market_notices =
        source.source_market_notices;
    CopyAnchor(source.source_anchor, output);
}

void Flatten(
    const market::ShenzhenOrderRevisionEventV1& source,
    l2flow_instrument_derived_event_row_v1* output) noexcept {
    const auto& order = source.order;
    output->market =
        L2FLOW_INSTRUMENT_DERIVED_EVENT_MARKET_SHENZHEN_V1;
    output->event_kind =
        L2FLOW_INSTRUMENT_DERIVED_EVENT_ORDER_REVISION_V1;
    output->trade_date = order.key.trade_date;
    output->instrument_id = order.key.instrument_id;
    output->channel = order.key.channel;
    output->order_id = order.key.order_id;
    output->operation =
        static_cast<std::uint8_t>(source.operation);
    output->finality =
        static_cast<std::uint8_t>(order.finality);
    output->side = static_cast<std::uint8_t>(order.side);
    output->side_source =
        L2FLOW_INSTRUMENT_DERIVED_EVENT_SIDE_SOURCE_DIRECT_V1;
    output->order_type =
        static_cast<std::uint8_t>(order.order_type);
    output->order_source =
        L2FLOW_INSTRUMENT_DERIVED_EVENT_ORDER_SOURCE_EVENT_V1;
    output->price_p6 = order.price_p6;
    output->price_valid = order.price_valid ? 1U : 0U;
    output->price_source =
        order.price_valid
            ? L2FLOW_INSTRUMENT_DERIVED_EVENT_PRICE_SOURCE_EVENT_V1
            : L2FLOW_INSTRUMENT_DERIVED_EVENT_PRICE_SOURCE_UNKNOWN_V1;
    output->published_quantity = order.original_quantity;
    output->published_quantity_valid = 1U;
    output->original_quantity = order.original_quantity;
    output->original_quantity_valid = 1U;
    output->original_quantity_status =
        L2FLOW_INSTRUMENT_DERIVED_EVENT_ORIGINAL_QUANTITY_EXACT_V1;
    output->remaining_quantity = order.remaining_quantity;
    output->remaining_quantity_valid =
        order.remaining_quantity_valid ? 1U : 0U;
    output->total_trade_quantity =
        order.total_trade_quantity;
    output->total_cancel_quantity =
        order.total_cancel_quantity;
    output->revision = order.revision;
    output->trade_count = order.trade_count;
    output->cancel_count = order.cancel_count;
    output->quality_flags = order.quality_flags;
    output->source_quality_flags =
        order.source_quality_flags;
    output->source_market_notices =
        order.source_market_notices;
    output->add_seen = 1U;
    output->apply_to_book = 1U;
    CopyAnchor(source.source_anchor, output);
}

void Flatten(
    const market::ShenzhenTradeEventV1& source,
    l2flow_instrument_derived_event_row_v1* output) noexcept {
    output->market =
        L2FLOW_INSTRUMENT_DERIVED_EVENT_MARKET_SHENZHEN_V1;
    output->event_kind =
        L2FLOW_INSTRUMENT_DERIVED_EVENT_TRADE_V1;
    output->trade_date = source.trade_date;
    output->instrument_id = source.instrument_id;
    output->channel = source.channel;
    output->buy_order_id = source.buy_order_id;
    output->sell_order_id = source.sell_order_id;
    output->aggressor =
        static_cast<std::uint8_t>(source.aggressor);
    output->price_p6 = source.price_p6;
    output->price_valid = 1U;
    output->quantity = source.quantity;
    output->trade_amount_p6 = source.amount_p6;
    output->trade_amount_valid =
        source.amount_valid ? 1U : 0U;
    output->quality_flags = source.quality_flags;
    output->source_quality_flags =
        source.source_quality_flags;
    output->source_market_notices =
        source.source_market_notices;
    CopyAnchor(source.source_anchor, output);
}

void Flatten(
    const market::ShenzhenCancelEventV1& source,
    l2flow_instrument_derived_event_row_v1* output) noexcept {
    output->market =
        L2FLOW_INSTRUMENT_DERIVED_EVENT_MARKET_SHENZHEN_V1;
    output->event_kind =
        L2FLOW_INSTRUMENT_DERIVED_EVENT_CANCEL_V1;
    output->trade_date = source.key.trade_date;
    output->instrument_id = source.key.instrument_id;
    output->channel = source.key.channel;
    output->order_id = source.key.order_id;
    output->side = static_cast<std::uint8_t>(source.side);
    output->quantity = source.quantity;
    output->referenced_order_found =
        source.referenced_order_found ? 1U : 0U;
    output->side_from_order =
        source.side_from_order ? 1U : 0U;
    output->quality_flags = source.quality_flags;
    output->source_quality_flags =
        source.source_quality_flags;
    output->source_market_notices =
        source.source_market_notices;
    CopyAnchor(source.source_anchor, output);
}

}  // namespace

bool ProjectInstrumentDerivedEventWireV1(
    const InstrumentDerivedEventV1& source,
    l2flow_instrument_derived_event_row_v1* output) noexcept {
    if (output == nullptr) {
        return false;
    }
    *output = {};
    output->record_schema_version = 1U;
    output->record_bytes = sizeof(*output);
    output->derived_event_sequence =
        source.derived_event_sequence;
    std::visit(
        [output](const auto& event) {
            Flatten(event, output);
        },
        source.payload);
    return true;
}

}  // namespace l2flow::ipc
