#include "l2flow/ipc/instrument_derived_event_history_c_v1.h"

#include "l2flow/ipc/instrument_derived_event_history_v1.h"

#include <algorithm>
#include <cstring>
#include <iterator>
#include <memory>
#include <new>
#include <utility>
#include <vector>

namespace {

using l2flow::ipc::InstrumentDerivedEventCheckpointV1;
using l2flow::ipc::InstrumentDerivedEventHistoryErrorV1;
using l2flow::ipc::InstrumentDerivedEventHistoryPageV1;
using l2flow::ipc::InstrumentDerivedEventHistorySessionV1;
using l2flow::ipc::InstrumentDerivedEventV1;

static_assert(
    sizeof(l2flow_instrument_derived_event_row_v1) == 320U);
static_assert(
    sizeof(l2flow_instrument_derived_event_checkpoint_v1) == 344U);

[[nodiscard]] int ToCError(
    InstrumentDerivedEventHistoryErrorV1 error) noexcept {
    return static_cast<int>(error);
}

[[nodiscard]] l2flow::market::MarketV1 ToMarket(
    std::uint8_t market) noexcept {
    switch (market) {
        case L2FLOW_INSTRUMENT_DERIVED_EVENT_MARKET_SHANGHAI_V1:
            return l2flow::market::MarketV1::kShanghai;
        case L2FLOW_INSTRUMENT_DERIVED_EVENT_MARKET_SHENZHEN_V1:
            return l2flow::market::MarketV1::kShenzhen;
        default:
            return l2flow::market::MarketV1::kUnknown;
    }
}

void ToCCheckpoint(
    const InstrumentDerivedEventCheckpointV1& source,
    l2flow_instrument_derived_event_checkpoint_v1* output) noexcept {
    *output = {};
    output->raw_checkpoint = source.raw_checkpoint;
    output->derived_event_sequence_exclusive =
        source.derived_event_sequence_exclusive;
    output->order_state_count = source.order_state_count;
    output->instrument_id = source.instrument_id;
    output->trade_date = source.trade_date;
    output->market = static_cast<std::uint8_t>(source.market);
    output->finalized = source.finalized ? 1U : 0U;
}

[[nodiscard]] InstrumentDerivedEventCheckpointV1 FromCCheckpoint(
    const l2flow_instrument_derived_event_checkpoint_v1&
        source) noexcept {
    InstrumentDerivedEventCheckpointV1 output{};
    output.raw_checkpoint = source.raw_checkpoint;
    output.derived_event_sequence_exclusive =
        source.derived_event_sequence_exclusive;
    output.order_state_count = source.order_state_count;
    output.instrument_id = source.instrument_id;
    output.trade_date = source.trade_date;
    output.market = ToMarket(source.market);
    output.finalized = source.finalized != 0U;
    return output;
}

[[nodiscard]] bool CanonicalCheckpoint(
    const l2flow_instrument_derived_event_checkpoint_v1&
        checkpoint) noexcept {
    return std::all_of(
               std::begin(checkpoint.reserved),
               std::end(checkpoint.reserved),
               [](std::uint8_t value) {
                   return value == 0U;
               }) &&
           checkpoint.finalized <= 1U &&
           (checkpoint.market ==
                L2FLOW_INSTRUMENT_DERIVED_EVENT_MARKET_SHANGHAI_V1 ||
            checkpoint.market ==
                L2FLOW_INSTRUMENT_DERIVED_EVENT_MARKET_SHENZHEN_V1);
}

template <typename Anchor>
void CopyAnchor(
    const Anchor& source,
    l2flow_instrument_derived_event_row_v1* output) noexcept {
    output->native_event_sequence =
        source.native_event_sequence;
    output->source_sequence = source.source_sequence;
    output->ingress_sequence = source.ingress_sequence;
    output->tick_stream_sequence =
        source.tick_stream_sequence;
    output->vendor_sequence_id = source.vendor_sequence_id;
    output->event_time_ns_since_midnight =
        source.event_time_ns_since_midnight;
    output->event_time_unix_ns = source.event_time_unix_ns;
    output->recv_realtime_ns = source.recv_realtime_ns;
    output->recv_monotonic_ns = source.recv_monotonic_ns;
    output->vendor_local_time_raw =
        source.vendor_local_time_raw;
    output->vendor_local_time_ns_since_midnight =
        source.vendor_local_time_ns_since_midnight;
    output->event_time_valid =
        source.event_time_valid ? 1U : 0U;
    output->event_time_unix_ns_valid =
        source.event_time_unix_ns_valid ? 1U : 0U;
    output->vendor_local_time_valid =
        source.vendor_local_time_valid ? 1U : 0U;
}

void Flatten(
    const l2flow::market::ShanghaiOrderRevisionEventV1& source,
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
    output->operation = static_cast<std::uint8_t>(
        source.operation);
    output->finality = static_cast<std::uint8_t>(
        order.finality);
    output->side = static_cast<std::uint8_t>(order.side);
    output->side_source = static_cast<std::uint8_t>(
        order.side_source);
    output->phase_at_first = static_cast<std::uint8_t>(
        order.phase_at_first);
    output->phase_at_add = static_cast<std::uint8_t>(
        order.phase_at_add);
    output->phase_at_last = static_cast<std::uint8_t>(
        order.phase_at_last);
    output->order_source = static_cast<std::uint8_t>(
        order.order_source);
    output->price_source = static_cast<std::uint8_t>(
        order.price_source);
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
    output->apply_to_book =
        order.apply_to_book ? 1U : 0U;
    CopyAnchor(source.source_anchor, output);
}

void Flatten(
    const l2flow::market::ShanghaiTradeEventV1& source,
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
    output->aggressor = static_cast<std::uint8_t>(
        source.aggressor);
    output->phase = static_cast<std::uint8_t>(
        source.phase);
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
    const l2flow::market::ShanghaiCancelEventV1& source,
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
    output->phase = static_cast<std::uint8_t>(
        source.phase);
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
    const l2flow::market::ShanghaiStatusEventV1& source,
    l2flow_instrument_derived_event_row_v1* output) noexcept {
    output->market =
        L2FLOW_INSTRUMENT_DERIVED_EVENT_MARKET_SHANGHAI_V1;
    output->event_kind =
        L2FLOW_INSTRUMENT_DERIVED_EVENT_STATUS_V1;
    output->trade_date = source.trade_date;
    output->instrument_id = source.instrument_id;
    output->channel = source.channel;
    output->phase = static_cast<std::uint8_t>(
        source.phase);
    output->quality_flags = source.quality_flags;
    output->source_quality_flags =
        source.source_quality_flags;
    output->source_market_notices =
        source.source_market_notices;
    CopyAnchor(source.source_anchor, output);
}

void Flatten(
    const l2flow::market::ShenzhenOrderRevisionEventV1& source,
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
    output->operation = static_cast<std::uint8_t>(
        source.operation);
    output->finality = static_cast<std::uint8_t>(
        order.finality);
    output->side = static_cast<std::uint8_t>(order.side);
    output->side_source =
        L2FLOW_INSTRUMENT_DERIVED_EVENT_SIDE_SOURCE_DIRECT_V1;
    output->order_type = static_cast<std::uint8_t>(
        order.order_type);
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
    const l2flow::market::ShenzhenTradeEventV1& source,
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
    output->aggressor = static_cast<std::uint8_t>(
        source.aggressor);
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
    const l2flow::market::ShenzhenCancelEventV1& source,
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

[[nodiscard]] bool Flatten(
    const InstrumentDerivedEventV1& source,
    l2flow_instrument_derived_event_row_v1* output) noexcept {
    *output = {};
    output->record_schema_version =
        L2FLOW_INSTRUMENT_DERIVED_EVENT_ROW_SCHEMA_V2;
    output->record_bytes = sizeof(*output);
    output->derived_event_sequence =
        source.derived_event_sequence;
    std::visit(
        [output](const auto& event) {
            Flatten(event, output);
        },
        source.payload);
    const bool has_source_tick =
        output->tick_stream_sequence != 0U;
    const bool source_free_finalize =
        !has_source_tick &&
        output->event_kind ==
            L2FLOW_INSTRUMENT_DERIVED_EVENT_ORDER_REVISION_V1 &&
        output->operation == static_cast<std::uint8_t>(
            l2flow::market::ShanghaiOrderDeltaOperationV1::kFinalize);
    if (source.source_tick_event_ordinal_valid != has_source_tick ||
        (!source.source_tick_event_ordinal_valid &&
         (source.source_tick_event_ordinal != 0U ||
          !source_free_finalize))) {
        *output = {};
        return false;
    }
    output->reserved0 = source.source_tick_event_ordinal;
    output->reserved1[0U] =
        source.source_tick_event_ordinal_valid
            ? L2FLOW_INSTRUMENT_DERIVED_EVENT_SOURCE_TICK_ORDINAL_VALID_V2
            : L2FLOW_INSTRUMENT_DERIVED_EVENT_SOURCE_TICK_ORDINAL_INVALID_V2;
    return true;
}

}  // namespace

struct l2flow_instrument_derived_event_history_session_v1 {
    std::unique_ptr<InstrumentDerivedEventHistorySessionV1>
        session;
    InstrumentDerivedEventHistoryPageV1 pending_page;
    std::vector<InstrumentDerivedEventV1> pending_finalization;
    bool pending_page_valid = false;
    bool pending_finalization_valid = false;
    bool finalization_delivered = false;
};

extern "C" {

int l2flow_instrument_derived_event_history_session_open_v1(
    const char* absolute_control_socket_path,
    const l2flow_shm_session_info_v2* expected_session,
    std::uint32_t instrument_id,
    std::uint8_t market,
    std::size_t maximum_order_states,
    std::uint32_t timeout_ms,
    l2flow_instrument_derived_event_history_session_v1** output) {
    if (output == nullptr) {
        return L2FLOW_INSTRUMENT_DERIVED_EVENT_HISTORY_NULL_OUTPUT_V1;
    }
    *output = nullptr;
    if (absolute_control_socket_path == nullptr ||
        expected_session == nullptr) {
        return L2FLOW_INSTRUMENT_DERIVED_EVENT_HISTORY_INVALID_CONFIGURATION_V1;
    }
    try {
        auto holder = std::make_unique<
            l2flow_instrument_derived_event_history_session_v1>();
        l2flow::ipc::InstrumentDerivedEventHistoryConfigV1 config{};
        config.control_socket_path = absolute_control_socket_path;
        config.expected_session = *expected_session;
        config.instrument_id = instrument_id;
        config.market = ToMarket(market);
        config.maximum_order_states = maximum_order_states;
        config.timeout_ms = timeout_ms;
        const auto error =
            InstrumentDerivedEventHistorySessionV1::Create(
                std::move(config), &holder->session);
        if (error !=
            InstrumentDerivedEventHistoryErrorV1::kNone) {
            return ToCError(error);
        }
        *output = holder.release();
        return L2FLOW_INSTRUMENT_DERIVED_EVENT_HISTORY_OK_V1;
    } catch (const std::bad_alloc&) {
        return L2FLOW_INSTRUMENT_DERIVED_EVENT_HISTORY_RESOURCE_EXHAUSTED_V1;
    } catch (...) {
        return L2FLOW_INSTRUMENT_DERIVED_EVENT_HISTORY_FAILED_V1;
    }
}

void l2flow_instrument_derived_event_history_session_close_v1(
    l2flow_instrument_derived_event_history_session_v1* session) {
    delete session;
}

int l2flow_instrument_derived_event_history_begin_full_v1(
    l2flow_instrument_derived_event_history_session_v1* session,
    std::uint64_t expected_generation,
    std::uint32_t requested_raw_page_records) {
    if (session == nullptr || session->session == nullptr) {
        return L2FLOW_INSTRUMENT_DERIVED_EVENT_HISTORY_INVALID_STATE_V1;
    }
    if (session->pending_page_valid ||
        session->pending_finalization_valid) {
        return L2FLOW_INSTRUMENT_DERIVED_EVENT_HISTORY_INVALID_STATE_V1;
    }
    return ToCError(session->session->BeginFull(
        expected_generation, requested_raw_page_records));
}

int l2flow_instrument_derived_event_history_begin_update_v1(
    l2flow_instrument_derived_event_history_session_v1* session,
    const l2flow_instrument_derived_event_checkpoint_v1*
        base_checkpoint,
    std::uint64_t expected_generation,
    std::uint32_t requested_raw_page_records) {
    if (session == nullptr || session->session == nullptr ||
        base_checkpoint == nullptr ||
        !CanonicalCheckpoint(*base_checkpoint)) {
        return L2FLOW_INSTRUMENT_DERIVED_EVENT_HISTORY_INVALID_STATE_V1;
    }
    if (session->pending_page_valid ||
        session->pending_finalization_valid) {
        return L2FLOW_INSTRUMENT_DERIVED_EVENT_HISTORY_INVALID_STATE_V1;
    }
    return ToCError(session->session->BeginUpdate(
        FromCCheckpoint(*base_checkpoint),
        expected_generation,
        requested_raw_page_records));
}

int l2flow_instrument_derived_event_history_read_v1(
    l2flow_instrument_derived_event_history_session_v1* session,
    l2flow_instrument_derived_event_row_v1* rows,
    std::size_t capacity,
    std::size_t* record_count,
    std::uint32_t* eof) {
    if (record_count == nullptr || eof == nullptr) {
        return L2FLOW_INSTRUMENT_DERIVED_EVENT_HISTORY_NULL_OUTPUT_V1;
    }
    *record_count = 0U;
    *eof = 0U;
    if (session == nullptr || session->session == nullptr ||
        session->pending_finalization_valid ||
        (capacity != 0U && rows == nullptr)) {
        return L2FLOW_INSTRUMENT_DERIVED_EVENT_HISTORY_INVALID_STATE_V1;
    }
    if (!session->pending_page_valid) {
        const auto error = session->session->ReadPage(
            &session->pending_page);
        if (error !=
            InstrumentDerivedEventHistoryErrorV1::kNone) {
            return ToCError(error);
        }
        session->pending_page_valid = true;
    }
    const std::size_t required =
        session->pending_page.events.size();
    *record_count = required;
    if (capacity < required) {
        return L2FLOW_INSTRUMENT_DERIVED_EVENT_HISTORY_BUFFER_TOO_SMALL_V1;
    }
    for (std::size_t index = 0U; index < required; ++index) {
        if (!Flatten(
                session->pending_page.events[index],
                &rows[index])) {
            return L2FLOW_INSTRUMENT_DERIVED_EVENT_HISTORY_WIRE_PROJECTION_ERROR_V1;
        }
    }
    *eof = session->pending_page.eof ? 1U : 0U;
    session->pending_page.events.clear();
    session->pending_page_valid = false;
    return L2FLOW_INSTRUMENT_DERIVED_EVENT_HISTORY_OK_V1;
}

int l2flow_instrument_derived_event_history_verified_checkpoint_v1(
    const l2flow_instrument_derived_event_history_session_v1* session,
    l2flow_instrument_derived_event_checkpoint_v1* output) {
    if (output == nullptr) {
        return L2FLOW_INSTRUMENT_DERIVED_EVENT_HISTORY_NULL_OUTPUT_V1;
    }
    *output = {};
    if (session == nullptr || session->session == nullptr ||
        session->pending_page_valid ||
        session->pending_finalization_valid) {
        return L2FLOW_INSTRUMENT_DERIVED_EVENT_HISTORY_INVALID_STATE_V1;
    }
    InstrumentDerivedEventCheckpointV1 checkpoint{};
    const auto error =
        session->session->VerifiedCheckpoint(&checkpoint);
    if (error !=
        InstrumentDerivedEventHistoryErrorV1::kNone) {
        return ToCError(error);
    }
    ToCCheckpoint(checkpoint, output);
    return L2FLOW_INSTRUMENT_DERIVED_EVENT_HISTORY_OK_V1;
}

int l2flow_instrument_derived_event_history_finalize_v1(
    l2flow_instrument_derived_event_history_session_v1* session,
    l2flow_instrument_derived_event_row_v1* rows,
    std::size_t capacity,
    std::size_t* record_count) {
    if (record_count == nullptr) {
        return L2FLOW_INSTRUMENT_DERIVED_EVENT_HISTORY_NULL_OUTPUT_V1;
    }
    *record_count = 0U;
    if (session == nullptr || session->session == nullptr ||
        session->pending_page_valid ||
        session->finalization_delivered ||
        (capacity != 0U && rows == nullptr)) {
        return L2FLOW_INSTRUMENT_DERIVED_EVENT_HISTORY_INVALID_STATE_V1;
    }
    if (!session->pending_finalization_valid) {
        const auto error = session->session->FinalizeTradingDay(
            &session->pending_finalization);
        if (error !=
            InstrumentDerivedEventHistoryErrorV1::kNone) {
            return ToCError(error);
        }
        session->pending_finalization_valid = true;
    }
    const std::size_t required =
        session->pending_finalization.size();
    *record_count = required;
    if (capacity < required) {
        return L2FLOW_INSTRUMENT_DERIVED_EVENT_HISTORY_BUFFER_TOO_SMALL_V1;
    }
    for (std::size_t index = 0U; index < required; ++index) {
        if (!Flatten(
                session->pending_finalization[index],
                &rows[index])) {
            return L2FLOW_INSTRUMENT_DERIVED_EVENT_HISTORY_WIRE_PROJECTION_ERROR_V1;
        }
    }
    session->pending_finalization.clear();
    session->pending_finalization_valid = false;
    session->finalization_delivered = true;
    return L2FLOW_INSTRUMENT_DERIVED_EVENT_HISTORY_OK_V1;
}

int l2flow_instrument_derived_event_history_last_raw_error_v1(
    const l2flow_instrument_derived_event_history_session_v1* session) {
    if (session == nullptr || session->session == nullptr) {
        return L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_CLOSED_V2;
    }
    return static_cast<int>(session->session->last_raw_error());
}

const char* l2flow_instrument_derived_event_history_error_name_v1(
    int error) {
    if (error ==
        L2FLOW_INSTRUMENT_DERIVED_EVENT_HISTORY_BUFFER_TOO_SMALL_V1) {
        return "buffer_too_small";
    }
    if (error < 0 ||
        error >
            L2FLOW_INSTRUMENT_DERIVED_EVENT_HISTORY_FAILED_V1) {
        return "unknown";
    }
    return l2flow::ipc::InstrumentDerivedEventHistoryErrorNameV1(
               static_cast<InstrumentDerivedEventHistoryErrorV1>(
                   error))
        .data();
}

}  // extern "C"
