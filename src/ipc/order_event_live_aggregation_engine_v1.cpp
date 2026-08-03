#include "l2flow/ipc/order_event_live_aggregation_engine_v1.h"

#include <algorithm>
#include <array>
#include <limits>
#include <new>
#include <span>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace l2flow::ipc {
namespace {

enum class SourceClassificationV1 : std::uint8_t {
    kNonTarget = 0U,
    kShanghai,
    kShenzhen,
    kInvalid,
};

[[nodiscard]] bool AllZero(
    std::span<const std::uint8_t> values) noexcept {
    return std::all_of(
        values.begin(), values.end(), [](std::uint8_t value) {
            return value == 0U;
        });
}

[[nodiscard]] bool CommonEnvelopeCanonical(
    const RealtimeWireCommonRecordV2& common,
    std::uint32_t expected_trade_date) noexcept {
    return expected_trade_date != 0U &&
           common.record_schema_version == 2U &&
           common.record_bytes ==
               sizeof(RealtimeWireTickPayloadV2) &&
           common.instrument_id != 0U &&
           common.ordinal !=
               std::numeric_limits<std::uint32_t>::max() &&
           common.instrument_id == common.ordinal + 1U &&
           common.source_sequence != 0U &&
           common.source_sequence !=
               std::numeric_limits<std::uint64_t>::max() &&
           common.ingress_sequence != 0U &&
           common.ingress_sequence !=
               std::numeric_limits<std::uint64_t>::max() &&
           common.tick_stream_sequence != 0U &&
           common.tick_stream_sequence !=
               std::numeric_limits<std::uint64_t>::max() &&
           common.tick_stream_sequence <= common.ingress_sequence &&
           common.source_stream_id != 0U &&
           common.trade_date == expected_trade_date &&
           common.reserved0 == 0U && AllZero(common.reserved) &&
           common.source_slot <= 3U &&
           common.event_kind != 0U &&
           common.market >=
               static_cast<std::uint8_t>(
                   market::MarketV1::kShanghai) &&
           common.market <=
               static_cast<std::uint8_t>(
                   market::MarketV1::kShenzhen) &&
           common.quantity_unit <=
               static_cast<std::uint8_t>(
                   market::QuantityUnitV1::kIndexUnit) &&
           common.security_type <=
               static_cast<std::uint8_t>(
                   market::SecurityTypeV1::kOption) &&
           common.asset_scope <=
               static_cast<std::uint8_t>(
                   market::AssetScopeV1::
                       kOutsideDocumentedCore);
}

[[nodiscard]] SourceClassificationV1 ClassifySource(
    const RealtimeWireCommonRecordV2& common) noexcept {
    const auto market_value =
        static_cast<market::MarketV1>(common.market);
    switch (static_cast<market::MarketEventKindV1>(
        common.event_kind)) {
        case market::MarketEventKindV1::kShanghaiSnapshot:
            return market_value == market::MarketV1::kShanghai &&
                           common.source_slot == 0U
                       ? SourceClassificationV1::kNonTarget
                       : SourceClassificationV1::kInvalid;
        case market::MarketEventKindV1::kShanghaiTick:
            return market_value == market::MarketV1::kShanghai &&
                           common.source_slot == 1U
                       ? SourceClassificationV1::kShanghai
                       : SourceClassificationV1::kInvalid;
        case market::MarketEventKindV1::kShenzhenSnapshot:
            return market_value == market::MarketV1::kShenzhen &&
                           common.source_slot == 2U
                       ? SourceClassificationV1::kNonTarget
                       : SourceClassificationV1::kInvalid;
        case market::MarketEventKindV1::kShenzhenOrder:
        case market::MarketEventKindV1::kShenzhenTransaction:
            return market_value == market::MarketV1::kShenzhen &&
                           common.source_slot == 3U
                       ? SourceClassificationV1::kShenzhen
                       : SourceClassificationV1::kInvalid;
    }
    // Unknown nonzero event kinds are forward/non-target records for this
    // V1 engine. Their common envelope still advances the dense source cursor.
    return SourceClassificationV1::kNonTarget;
}

template <typename Anchor>
void CopyAnchor(
    const Anchor& source,
    OrderEventDeltaPayloadV1* output) noexcept {
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

void InitializeRow(OrderEventDeltaPayloadV1* output) noexcept {
    *output = {};
    output->record_schema_version =
        kOrderEventDeltaPayloadSchemaV1;
    output->record_bytes = sizeof(*output);
}

void Project(
    const market::ShanghaiOrderRevisionEventV1& source,
    OrderEventDeltaPayloadV1* output) noexcept {
    InitializeRow(output);
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
    output->apply_to_book =
        order.apply_to_book ? 1U : 0U;
    CopyAnchor(source.source_anchor, output);
}

void Project(
    const market::ShanghaiTradeEventV1& source,
    OrderEventDeltaPayloadV1* output) noexcept {
    InitializeRow(output);
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

void Project(
    const market::ShanghaiCancelEventV1& source,
    OrderEventDeltaPayloadV1* output) noexcept {
    InitializeRow(output);
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

void Project(
    const market::ShanghaiStatusEventV1& source,
    OrderEventDeltaPayloadV1* output) noexcept {
    InitializeRow(output);
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

void Project(
    const market::ShenzhenOrderRevisionEventV1& source,
    OrderEventDeltaPayloadV1* output) noexcept {
    InitializeRow(output);
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

void Project(
    const market::ShenzhenTradeEventV1& source,
    OrderEventDeltaPayloadV1* output) noexcept {
    InitializeRow(output);
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

void Project(
    const market::ShenzhenCancelEventV1& source,
    OrderEventDeltaPayloadV1* output) noexcept {
    InitializeRow(output);
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

template <typename EventVariant>
[[nodiscard]] bool AppendProjected(
    const EventVariant& events,
    std::uint32_t trade_date,
    std::uint64_t source_tick_sequence,
    std::vector<OrderEventDeltaPayloadV1>* output) {
    if (output == nullptr ||
        (!events.empty() &&
         events.size() - 1U >
             static_cast<std::size_t>(
                 std::numeric_limits<std::uint32_t>::max()))) {
        return false;
    }
    output->reserve(output->size() + events.size());
    for (std::size_t index = 0U; index < events.size(); ++index) {
        const auto& event = events[index];
        OrderEventDeltaPayloadV1 row{};
        std::visit(
            [&row](const auto& value) {
                Project(value, &row);
            },
            event);
        row.reserved0 = static_cast<std::uint32_t>(index);
        row.reserved1[0U] =
            L2FLOW_INSTRUMENT_DERIVED_EVENT_SOURCE_TICK_ORDINAL_VALID_V2;
        if (!OrderEventDeltaPayloadCanonicalV1(
                row, trade_date, source_tick_sequence)) {
            return false;
        }
        output->push_back(row);
    }
    return true;
}

}  // namespace

class OrderEventLiveAggregationEngineV1::Impl final {
public:
    Impl(
        OrderEventLiveAggregationConfigV1 config,
        OrderEventDeltaRingProducerV1* producer) noexcept
        : config_(config), producer_(producer) {}

    [[nodiscard]] OrderEventLiveCreateErrorV1 Initialize() noexcept {
        if (producer_ == nullptr ||
            config_.maximum_shanghai_order_states == 0U ||
            config_.maximum_shenzhen_order_states == 0U) {
            return OrderEventLiveCreateErrorV1::
                kInvalidConfiguration;
        }
        const OrderEventDeltaSessionV1 session =
            producer_->session();
        if (session.trade_date == 0U ||
            producer_->state() !=
                OrderEventDeltaProducerStateV1::kActive ||
            producer_->consumed_source_tick_sequence() != 0U ||
            producer_->published_event_sequence() != 0U) {
            return OrderEventLiveCreateErrorV1::
                kProducerNotPristine;
        }
        const auto shanghai_error =
            market::ShanghaiOrderEventAggregatorV1::Create(
                {session.trade_date,
                 config_.maximum_shanghai_order_states},
                &shanghai_);
        if (shanghai_error !=
            market::ShanghaiOrderAggregatorCreateErrorV1::kNone) {
            return shanghai_error ==
                           market::
                               ShanghaiOrderAggregatorCreateErrorV1::
                                   kResourceExhausted
                       ? OrderEventLiveCreateErrorV1::
                             kResourceExhausted
                       : OrderEventLiveCreateErrorV1::
                             kCoreCreateFailed;
        }
        const auto shenzhen_error =
            market::ShenzhenOrderEventProjectorV1::Create(
                {session.trade_date,
                 config_.maximum_shenzhen_order_states},
                &shenzhen_);
        if (shenzhen_error !=
            market::ShenzhenOrderProjectorCreateErrorV1::kNone) {
            shanghai_.reset();
            return shenzhen_error ==
                           market::
                               ShenzhenOrderProjectorCreateErrorV1::
                                   kResourceExhausted
                       ? OrderEventLiveCreateErrorV1::
                             kResourceExhausted
                       : OrderEventLiveCreateErrorV1::
                             kCoreCreateFailed;
        }
        return OrderEventLiveCreateErrorV1::kNone;
    }

    [[nodiscard]] OrderEventLiveConsumeErrorV1 Consume(
        const RealtimeWireTickPayloadV2& source_tick,
        OrderEventLiveConsumeResultV1* output) noexcept {
        if (output == nullptr) {
            return OrderEventLiveConsumeErrorV1::kNullOutput;
        }
        *output = {};
        if (failed_ || producer_ == nullptr ||
            shanghai_ == nullptr || shenzhen_ == nullptr) {
            return OrderEventLiveConsumeErrorV1::kFailed;
        }
        const std::uint64_t source_sequence =
            source_tick.common.tick_stream_sequence;
        if (last_source_tick_sequence_ ==
                std::numeric_limits<std::uint64_t>::max() ||
            source_sequence != last_source_tick_sequence_ + 1U ||
            producer_->consumed_source_tick_sequence() !=
                last_source_tick_sequence_) {
            return Fail(
                OrderEventLiveConsumeErrorV1::kSourceTickGap);
        }
        const std::uint32_t trade_date =
            producer_->session().trade_date;
        if (!CommonEnvelopeCanonical(
                source_tick.common, trade_date)) {
            return Fail(
                OrderEventLiveConsumeErrorV1::
                    kInvalidSourceEnvelope);
        }
        const SourceClassificationV1 classification =
            ClassifySource(source_tick.common);
        if (classification ==
            SourceClassificationV1::kInvalid) {
            return Fail(
                OrderEventLiveConsumeErrorV1::
                    kInvalidSourceEnvelope);
        }

        rows_.clear();
        try {
            if (classification ==
                SourceClassificationV1::kShanghai) {
                market::ShanghaiOrderEventInputV1 input{};
                last_wire_projection_ =
                    ProjectShanghaiOrderEventInputFromWireV2(
                        source_tick, &input);
                if (last_wire_projection_ !=
                    WireOrderEventProjectionResultV2::kProjected) {
                    return Fail(
                        OrderEventLiveConsumeErrorV1::
                            kWireProjectionFailed);
                }
                shanghai_events_.clear();
                last_shanghai_error_ =
                    shanghai_->Consume(input, &shanghai_events_);
                if (last_shanghai_error_ !=
                    market::
                        ShanghaiOrderAggregatorConsumeErrorV1::
                            kNone) {
                    return Fail(
                        OrderEventLiveConsumeErrorV1::
                            kCoreAggregationFailed);
                }
                if (!AppendProjected(
                        shanghai_events_,
                        trade_date,
                        source_sequence,
                        &rows_)) {
                    return Fail(
                        OrderEventLiveConsumeErrorV1::
                            kDerivedProjectionFailed);
                }
            } else if (
                classification ==
                SourceClassificationV1::kShenzhen) {
                market::ShenzhenOrderEventInputV1 input{};
                last_wire_projection_ =
                    ProjectShenzhenOrderEventInputFromWireV2(
                        source_tick, &input);
                if (last_wire_projection_ !=
                    WireOrderEventProjectionResultV2::kProjected) {
                    return Fail(
                        OrderEventLiveConsumeErrorV1::
                            kWireProjectionFailed);
                }
                shenzhen_events_.clear();
                last_shenzhen_error_ =
                    shenzhen_->Consume(input, &shenzhen_events_);
                if (last_shenzhen_error_ !=
                    market::
                        ShenzhenOrderProjectorConsumeErrorV1::
                            kNone) {
                    return Fail(
                        OrderEventLiveConsumeErrorV1::
                            kCoreAggregationFailed);
                }
                if (!AppendProjected(
                        shenzhen_events_,
                        trade_date,
                        source_sequence,
                        &rows_)) {
                    return Fail(
                        OrderEventLiveConsumeErrorV1::
                            kDerivedProjectionFailed);
                }
            }
        } catch (const std::bad_alloc&) {
            return Fail(
                OrderEventLiveConsumeErrorV1::
                    kResourceExhausted);
        } catch (...) {
            return Fail(OrderEventLiveConsumeErrorV1::kFailed);
        }

        last_publish_error_ = producer_->PublishSourceTick(
            source_sequence, rows_);
        if (last_publish_error_ !=
            OrderEventDeltaPublishErrorV1::kNone) {
            return Fail(
                OrderEventLiveConsumeErrorV1::
                    kDeltaPublicationFailed);
        }
        last_source_tick_sequence_ = source_sequence;
        output->source_tick_sequence = source_sequence;
        output->published_event_sequence =
            producer_->published_event_sequence();
        output->derived_event_count = rows_.size();
        output->source_class =
            classification == SourceClassificationV1::kShanghai
                ? OrderEventLiveSourceClassV1::kShanghai
                : (classification ==
                           SourceClassificationV1::kShenzhen
                       ? OrderEventLiveSourceClassV1::kShenzhen
                       : OrderEventLiveSourceClassV1::kNonTarget);
        return OrderEventLiveConsumeErrorV1::kNone;
    }

    [[nodiscard]] bool failed() const noexcept {
        return failed_;
    }
    [[nodiscard]] std::uint64_t consumed() const noexcept {
        return last_source_tick_sequence_;
    }
    [[nodiscard]] std::size_t shanghai_order_count()
        const noexcept {
        return shanghai_ == nullptr ? 0U : shanghai_->order_count();
    }
    [[nodiscard]] std::size_t shenzhen_order_count()
        const noexcept {
        return shenzhen_ == nullptr ? 0U : shenzhen_->order_count();
    }
    [[nodiscard]] WireOrderEventProjectionResultV2
    last_wire_projection() const noexcept {
        return last_wire_projection_;
    }
    [[nodiscard]] market::ShanghaiOrderAggregatorConsumeErrorV1
    last_shanghai_error() const noexcept {
        return last_shanghai_error_;
    }
    [[nodiscard]] market::ShenzhenOrderProjectorConsumeErrorV1
    last_shenzhen_error() const noexcept {
        return last_shenzhen_error_;
    }
    [[nodiscard]] OrderEventDeltaPublishErrorV1
    last_publish_error() const noexcept {
        return last_publish_error_;
    }

private:
    [[nodiscard]] OrderEventLiveConsumeErrorV1 Fail(
        OrderEventLiveConsumeErrorV1 error) noexcept {
        failed_ = true;
        if (producer_ != nullptr) {
            producer_->MarkFailed();
        }
        return error;
    }

    OrderEventLiveAggregationConfigV1 config_{};
    OrderEventDeltaRingProducerV1* producer_ = nullptr;
    std::unique_ptr<market::ShanghaiOrderEventAggregatorV1>
        shanghai_;
    std::unique_ptr<market::ShenzhenOrderEventProjectorV1>
        shenzhen_;
    std::vector<market::ShanghaiOrderEventV1> shanghai_events_;
    std::vector<market::ShenzhenOrderEventV1> shenzhen_events_;
    std::vector<OrderEventDeltaPayloadV1> rows_;
    std::uint64_t last_source_tick_sequence_ = 0U;
    WireOrderEventProjectionResultV2 last_wire_projection_ =
        WireOrderEventProjectionResultV2::kProjected;
    market::ShanghaiOrderAggregatorConsumeErrorV1
        last_shanghai_error_ =
            market::ShanghaiOrderAggregatorConsumeErrorV1::kNone;
    market::ShenzhenOrderProjectorConsumeErrorV1
        last_shenzhen_error_ =
            market::ShenzhenOrderProjectorConsumeErrorV1::kNone;
    OrderEventDeltaPublishErrorV1 last_publish_error_ =
        OrderEventDeltaPublishErrorV1::kNone;
    bool failed_ = false;
};

std::string_view OrderEventLiveCreateErrorNameV1(
    OrderEventLiveCreateErrorV1 error) noexcept {
    switch (error) {
        case OrderEventLiveCreateErrorV1::kNone:
            return "none";
        case OrderEventLiveCreateErrorV1::kNullOutput:
            return "null_output";
        case OrderEventLiveCreateErrorV1::kInvalidConfiguration:
            return "invalid_configuration";
        case OrderEventLiveCreateErrorV1::kProducerNotPristine:
            return "producer_not_pristine";
        case OrderEventLiveCreateErrorV1::kCoreCreateFailed:
            return "core_create_failed";
        case OrderEventLiveCreateErrorV1::kResourceExhausted:
            return "resource_exhausted";
    }
    return "unknown";
}

std::string_view OrderEventLiveConsumeErrorNameV1(
    OrderEventLiveConsumeErrorV1 error) noexcept {
    switch (error) {
        case OrderEventLiveConsumeErrorV1::kNone:
            return "none";
        case OrderEventLiveConsumeErrorV1::kNullOutput:
            return "null_output";
        case OrderEventLiveConsumeErrorV1::kSourceTickGap:
            return "source_tick_gap";
        case OrderEventLiveConsumeErrorV1::kInvalidSourceEnvelope:
            return "invalid_source_envelope";
        case OrderEventLiveConsumeErrorV1::kWireProjectionFailed:
            return "wire_projection_failed";
        case OrderEventLiveConsumeErrorV1::kCoreAggregationFailed:
            return "core_aggregation_failed";
        case OrderEventLiveConsumeErrorV1::kDerivedProjectionFailed:
            return "derived_projection_failed";
        case OrderEventLiveConsumeErrorV1::kDeltaPublicationFailed:
            return "delta_publication_failed";
        case OrderEventLiveConsumeErrorV1::kResourceExhausted:
            return "resource_exhausted";
        case OrderEventLiveConsumeErrorV1::kFailed:
            return "failed";
    }
    return "unknown";
}

OrderEventLiveAggregationEngineV1::
    OrderEventLiveAggregationEngineV1(
        std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

OrderEventLiveAggregationEngineV1::
    ~OrderEventLiveAggregationEngineV1() = default;

OrderEventLiveCreateErrorV1
OrderEventLiveAggregationEngineV1::Create(
    OrderEventLiveAggregationConfigV1 config,
    OrderEventDeltaRingProducerV1* producer,
    std::unique_ptr<OrderEventLiveAggregationEngineV1>* output)
    noexcept {
    if (output == nullptr) {
        return OrderEventLiveCreateErrorV1::kNullOutput;
    }
    output->reset();
    if (producer == nullptr ||
        config.maximum_shanghai_order_states == 0U ||
        config.maximum_shenzhen_order_states == 0U) {
        return OrderEventLiveCreateErrorV1::
            kInvalidConfiguration;
    }
    try {
        auto impl = std::make_unique<Impl>(config, producer);
        const OrderEventLiveCreateErrorV1 error =
            impl->Initialize();
        if (error != OrderEventLiveCreateErrorV1::kNone) {
            return error;
        }
        output->reset(new OrderEventLiveAggregationEngineV1(
            std::move(impl)));
        return OrderEventLiveCreateErrorV1::kNone;
    } catch (const std::bad_alloc&) {
        return OrderEventLiveCreateErrorV1::kResourceExhausted;
    } catch (...) {
        return OrderEventLiveCreateErrorV1::kResourceExhausted;
    }
}

OrderEventLiveConsumeErrorV1
OrderEventLiveAggregationEngineV1::Consume(
    const RealtimeWireTickPayloadV2& source_tick,
    OrderEventLiveConsumeResultV1* output) noexcept {
    return impl_ == nullptr
               ? OrderEventLiveConsumeErrorV1::kFailed
               : impl_->Consume(source_tick, output);
}

bool OrderEventLiveAggregationEngineV1::failed() const noexcept {
    return impl_ == nullptr || impl_->failed();
}

std::uint64_t
OrderEventLiveAggregationEngineV1::
    consumed_source_tick_sequence() const noexcept {
    return impl_ == nullptr ? 0U : impl_->consumed();
}

std::size_t
OrderEventLiveAggregationEngineV1::shanghai_order_count()
    const noexcept {
    return impl_ == nullptr ? 0U
                            : impl_->shanghai_order_count();
}

std::size_t
OrderEventLiveAggregationEngineV1::shenzhen_order_count()
    const noexcept {
    return impl_ == nullptr ? 0U
                            : impl_->shenzhen_order_count();
}

WireOrderEventProjectionResultV2
OrderEventLiveAggregationEngineV1::
    last_wire_projection_result() const noexcept {
    return impl_ == nullptr
               ? WireOrderEventProjectionResultV2::kInvalidEnvelope
               : impl_->last_wire_projection();
}

market::ShanghaiOrderAggregatorConsumeErrorV1
OrderEventLiveAggregationEngineV1::last_shanghai_error()
    const noexcept {
    return impl_ == nullptr
               ? market::
                     ShanghaiOrderAggregatorConsumeErrorV1::kFailed
               : impl_->last_shanghai_error();
}

market::ShenzhenOrderProjectorConsumeErrorV1
OrderEventLiveAggregationEngineV1::last_shenzhen_error()
    const noexcept {
    return impl_ == nullptr
               ? market::ShenzhenOrderProjectorConsumeErrorV1::
                     kFailed
               : impl_->last_shenzhen_error();
}

OrderEventDeltaPublishErrorV1
OrderEventLiveAggregationEngineV1::last_publish_error()
    const noexcept {
    return impl_ == nullptr
               ? OrderEventDeltaPublishErrorV1::kFailed
               : impl_->last_publish_error();
}

}  // namespace l2flow::ipc
