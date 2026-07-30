#include "l2flow/market/shenzhen_order_event_projector_v1.h"

#include <algorithm>
#include <array>
#include <limits>
#include <map>
#include <new>
#include <tuple>
#include <utility>

namespace l2flow::market {
namespace {

constexpr std::uint64_t kOrderConflictMask =
    ShenzhenEventQualityBitV1(
        ShenzhenEventQualityFlagV1::kQuantityConflict) |
    ShenzhenEventQualityBitV1(
        ShenzhenEventQualityFlagV1::kNumericOverflow) |
    ShenzhenEventQualityBitV1(
        ShenzhenEventQualityFlagV1::kSideConflict) |
    ShenzhenEventQualityBitV1(
        ShenzhenEventQualityFlagV1::kDuplicateOrder);

[[nodiscard]] bool ValidTradeDate(std::uint32_t value) noexcept {
    const std::uint32_t year = value / 10'000U;
    const std::uint32_t month = (value / 100U) % 100U;
    const std::uint32_t day = value % 100U;
    if (year < 1992U || year > 2200U || month == 0U ||
        month > 12U || day == 0U) {
        return false;
    }
    constexpr std::array<std::uint32_t, 12U> kDaysByMonth{
        31U, 28U, 31U, 30U, 31U, 30U,
        31U, 31U, 30U, 31U, 30U, 31U};
    std::uint32_t maximum_day = kDaysByMonth[month - 1U];
    const bool leap =
        (year % 4U == 0U && year % 100U != 0U) ||
        year % 400U == 0U;
    if (month == 2U && leap) {
        maximum_day = 29U;
    }
    return day <= maximum_day;
}

[[nodiscard]] bool CheckedAdd(
    std::int64_t lhs,
    std::int64_t rhs,
    std::int64_t* output) noexcept {
    if (output == nullptr ||
        (rhs > 0 &&
         lhs > std::numeric_limits<std::int64_t>::max() - rhs) ||
        (rhs < 0 &&
         lhs < std::numeric_limits<std::int64_t>::min() - rhs)) {
        return false;
    }
    *output = lhs + rhs;
    return true;
}

[[nodiscard]] bool CheckedSubtract(
    std::int64_t lhs,
    std::int64_t rhs,
    std::int64_t* output) noexcept {
    if (output == nullptr ||
        (rhs > 0 &&
         lhs < std::numeric_limits<std::int64_t>::min() + rhs) ||
        (rhs < 0 &&
         lhs > std::numeric_limits<std::int64_t>::max() + rhs)) {
        return false;
    }
    *output = lhs - rhs;
    return true;
}

[[nodiscard]] bool ValidAnchor(
    const ShenzhenEventSourceAnchorV1& anchor) noexcept {
    return anchor.native_event_sequence > 0 &&
           anchor.source_sequence > 0U &&
           anchor.ingress_sequence > 0U &&
           anchor.tick_stream_sequence > 0U &&
           (!anchor.event_time_unix_ns_valid ||
            anchor.event_time_valid);
}

[[nodiscard]] bool ZeroAnchor(
    const ShenzhenEventSourceAnchorV1& anchor) noexcept {
    return anchor.native_event_sequence == 0 &&
           anchor.source_sequence == 0U &&
           anchor.ingress_sequence == 0U &&
           anchor.tick_stream_sequence == 0U &&
           anchor.vendor_sequence_id == 0U &&
           anchor.event_time_ns_since_midnight == 0U &&
           anchor.event_time_unix_ns == 0 &&
           anchor.recv_realtime_ns == 0 &&
           anchor.recv_monotonic_ns == 0 &&
           anchor.vendor_local_time_raw == 0U &&
           anchor.vendor_local_time_ns_since_midnight == 0U &&
           !anchor.event_time_valid &&
           !anchor.event_time_unix_ns_valid &&
           !anchor.vendor_local_time_valid;
}

[[nodiscard]] bool DirectOrderSide(SideV1 side) noexcept {
    return side == SideV1::kBuy || side == SideV1::kSell ||
           side == SideV1::kBorrow || side == SideV1::kLend;
}

[[nodiscard]] bool DirectOrderType(OrderTypeV1 type) noexcept {
    return type == OrderTypeV1::kMarket ||
           type == OrderTypeV1::kLimit ||
           type == OrderTypeV1::kSameSideBest;
}

[[nodiscard]] bool ValidInput(
    const ShenzhenOrderEventInputV1& input) noexcept {
    if (!ValidTradeDate(input.trade_date) ||
        input.instrument_id == 0U || input.channel == 0U ||
        !ValidAnchor(input.anchor) ||
        !input.quantity_valid || input.quantity <= 0) {
        return false;
    }

    switch (input.action) {
        case TickActionV1::kAdd:
            if (!input.side_valid ||
                !DirectOrderSide(input.side) ||
                !input.order_type_valid ||
                !DirectOrderType(input.order_type) ||
                input.primary_order_id <= 0 ||
                input.primary_order_id !=
                    input.anchor.native_event_sequence ||
                input.buy_order_id != 0 ||
                input.sell_order_id != 0) {
                return false;
            }
            if (input.order_type == OrderTypeV1::kLimit) {
                return input.price_valid && input.price_p6 > 0;
            }
            return !input.price_valid && input.price_p6 == 0;

        case TickActionV1::kTrade:
            return !input.side_valid &&
                   input.side == SideV1::kUnknown &&
                   !input.order_type_valid &&
                   input.order_type == OrderTypeV1::kUnknown &&
                   input.price_valid && input.price_p6 > 0 &&
                   input.primary_order_id == 0 &&
                   input.buy_order_id >= 0 &&
                   input.sell_order_id >= 0;

        case TickActionV1::kCancel: {
            const bool has_buy = input.buy_order_id > 0;
            const bool has_sell = input.sell_order_id > 0;
            return input.side_valid &&
                   (input.side == SideV1::kBuy ||
                    input.side == SideV1::kSell) &&
                   !input.order_type_valid &&
                   input.order_type == OrderTypeV1::kUnknown &&
                   !input.price_valid && input.price_p6 == 0 &&
                   input.primary_order_id > 0 &&
                   input.buy_order_id >= 0 &&
                   input.sell_order_id >= 0 &&
                   has_buy != has_sell &&
                   input.primary_order_id ==
                       (has_buy ? input.buy_order_id
                                : input.sell_order_id) &&
                   input.side ==
                       (has_buy ? SideV1::kBuy
                                : SideV1::kSell);
        }

        case TickActionV1::kUnknown:
        case TickActionV1::kStatus:
            return false;
    }
    return false;
}

struct OrderState final {
    ShenzhenOrderSnapshotV1 snapshot{};
    bool terminal = false;
    bool finalization_emitted = false;
};

void RefreshFinality(OrderState* state) noexcept {
    if (state == nullptr) {
        return;
    }
    if ((state->snapshot.quality_flags &
         kOrderConflictMask) != 0U) {
        state->snapshot.finality =
            ShenzhenOrderFinalityV1::kConflict;
    } else if (state->terminal) {
        state->snapshot.finality =
            ShenzhenOrderFinalityV1::kFinal;
    } else {
        state->snapshot.finality =
            ShenzhenOrderFinalityV1::kProvisional;
    }
}

void MarkMutation(
    const ShenzhenOrderEventInputV1& input,
    OrderState* state) noexcept {
    state->snapshot.last_anchor = input.anchor;
    state->snapshot.source_quality_flags |=
        input.source_quality_flags;
    state->snapshot.source_market_notices |=
        input.source_market_notices;
}

void IncrementRevision(OrderState* state) noexcept {
    if (state->snapshot.revision ==
        std::numeric_limits<std::uint64_t>::max()) {
        state->snapshot.quality_flags |=
            ShenzhenEventQualityBitV1(
                ShenzhenEventQualityFlagV1::kNumericOverflow) |
            ShenzhenEventQualityBitV1(
                ShenzhenEventQualityFlagV1::kQuantityConflict);
        return;
    }
    ++state->snapshot.revision;
}

[[nodiscard]] bool ReferenceSideCompatible(
    SideV1 reference_side,
    SideV1 order_side) noexcept {
    if (reference_side == SideV1::kBuy) {
        return order_side == SideV1::kBuy ||
               order_side == SideV1::kBorrow;
    }
    if (reference_side == SideV1::kSell) {
        return order_side == SideV1::kSell ||
               order_side == SideV1::kLend;
    }
    return false;
}

[[nodiscard]] std::uint64_t ApplyQuantityReduction(
    const ShenzhenOrderEventInputV1& input,
    SideV1 expected_side,
    bool trade,
    OrderState* state) noexcept {
    const std::uint64_t before = state->snapshot.quality_flags;
    if (!ReferenceSideCompatible(
            expected_side, state->snapshot.side)) {
        state->snapshot.quality_flags |=
            ShenzhenEventQualityBitV1(
                ShenzhenEventQualityFlagV1::kSideConflict);
    }
    if (state->terminal) {
        state->snapshot.quality_flags |=
            ShenzhenEventQualityBitV1(
                ShenzhenEventQualityFlagV1::kQuantityConflict);
    }

    std::int64_t next = 0;
    std::int64_t* const total =
        trade ? &state->snapshot.total_trade_quantity
              : &state->snapshot.total_cancel_quantity;
    if (CheckedAdd(*total, input.quantity, &next)) {
        *total = next;
    } else {
        state->snapshot.quality_flags |=
            ShenzhenEventQualityBitV1(
                ShenzhenEventQualityFlagV1::kNumericOverflow) |
            ShenzhenEventQualityBitV1(
                ShenzhenEventQualityFlagV1::kQuantityConflict);
    }

    std::uint64_t* const count =
        trade ? &state->snapshot.trade_count
              : &state->snapshot.cancel_count;
    if (*count == std::numeric_limits<std::uint64_t>::max()) {
        state->snapshot.quality_flags |=
            ShenzhenEventQualityBitV1(
                ShenzhenEventQualityFlagV1::kNumericOverflow) |
            ShenzhenEventQualityBitV1(
                ShenzhenEventQualityFlagV1::kQuantityConflict);
    } else {
        ++(*count);
    }

    if (CheckedSubtract(
            state->snapshot.remaining_quantity,
            input.quantity,
            &next)) {
        state->snapshot.remaining_quantity = next;
        state->snapshot.remaining_quantity_valid = next >= 0;
        if (next < 0) {
            state->snapshot.quality_flags |=
                ShenzhenEventQualityBitV1(
                    ShenzhenEventQualityFlagV1::
                        kQuantityConflict);
        } else if (next == 0) {
            state->terminal = true;
        }
    } else {
        state->snapshot.remaining_quantity_valid = false;
        state->snapshot.quality_flags |=
            ShenzhenEventQualityBitV1(
                ShenzhenEventQualityFlagV1::kNumericOverflow) |
            ShenzhenEventQualityBitV1(
                ShenzhenEventQualityFlagV1::kQuantityConflict);
    }

    MarkMutation(input, state);
    IncrementRevision(state);
    RefreshFinality(state);
    return state->snapshot.quality_flags & ~before;
}

[[nodiscard]] ShenzhenOrderDeltaOperationV1 MutationOperation(
    OrderState* state) noexcept {
    if (state->terminal && !state->finalization_emitted) {
        state->finalization_emitted = true;
        return ShenzhenOrderDeltaOperationV1::kFinalize;
    }
    return ShenzhenOrderDeltaOperationV1::kUpdate;
}

[[nodiscard]] ShenzhenOrderRevisionEventV1 RevisionEvent(
    ShenzhenOrderDeltaOperationV1 operation,
    const ShenzhenEventSourceAnchorV1& anchor,
    const OrderState& state) noexcept {
    ShenzhenOrderRevisionEventV1 event{};
    event.operation = operation;
    event.source_anchor = anchor;
    event.order = state.snapshot;
    return event;
}

void FillAnchor(
    const DecodedMarketCommonV1& common,
    std::int64_t application_sequence,
    std::uint64_t ingress_sequence,
    std::uint64_t tick_stream_sequence,
    ShenzhenEventSourceAnchorV1* output) noexcept {
    output->native_event_sequence = application_sequence;
    output->source_sequence = common.origin.source_sequence;
    output->ingress_sequence = ingress_sequence;
    output->tick_stream_sequence = tick_stream_sequence;
    output->vendor_sequence_id =
        common.origin.vendor_sequence_id;
    output->event_time_ns_since_midnight =
        common.exchange_time.nanoseconds_since_midnight;
    output->event_time_unix_ns =
        common.exchange_time.unix_nanoseconds;
    output->recv_realtime_ns =
        common.origin.recv_realtime_ns;
    output->recv_monotonic_ns =
        common.origin.recv_monotonic_ns;
    output->vendor_local_time_raw =
        common.origin.vendor_local_time_raw;
    output->vendor_local_time_ns_since_midnight =
        common.vendor_local_time.nanoseconds_since_midnight;
    output->event_time_valid = common.exchange_time.valid;
    output->event_time_unix_ns_valid =
        common.exchange_time.valid &&
        common.exchange_time.unix_nanoseconds_valid;
    output->vendor_local_time_valid =
        common.vendor_local_time.valid;
}

}  // namespace

bool operator<(
    const ShenzhenOrderKeyV1& lhs,
    const ShenzhenOrderKeyV1& rhs) noexcept {
    return std::tie(
               lhs.trade_date,
               lhs.instrument_id,
               lhs.channel,
               lhs.order_id) <
           std::tie(
               rhs.trade_date,
               rhs.instrument_id,
               rhs.channel,
               rhs.order_id);
}

ShenzhenOrderEventProjectionV1
ProjectShenzhenOrderEventInputV1(
    const DecodedMarketEventV1& event,
    std::uint64_t ingress_sequence,
    std::uint64_t tick_stream_sequence,
    ShenzhenOrderEventInputV1* output) noexcept {
    if (output == nullptr) {
        return ShenzhenOrderEventProjectionV1::kInvalidEvent;
    }
    *output = {};
    if (ingress_sequence == 0U ||
        tick_stream_sequence == 0U) {
        return ShenzhenOrderEventProjectionV1::kInvalidEvent;
    }

    if (const auto* order =
            std::get_if<ShenzhenOrderV1>(&event);
        order != nullptr) {
        const DecodedMarketCommonV1& common = order->common;
        if (common.kind !=
                MarketEventKindV1::kShenzhenOrder ||
            common.market != MarketV1::kShenzhen ||
            order->fields.action != TickActionV1::kAdd ||
            order->fields.quantity.scale != 0U ||
            (order->fields.validity_bitmap &
             kTickPrimaryOrderIdValidV1) == 0U) {
            return ShenzhenOrderEventProjectionV1::kInvalidEvent;
        }
        ShenzhenOrderEventInputV1 projected{};
        projected.trade_date = common.origin.trade_date;
        projected.instrument_id = common.instrument_id;
        projected.channel = order->channel;
        FillAnchor(
            common,
            order->application_sequence,
            ingress_sequence,
            tick_stream_sequence,
            &projected.anchor);
        projected.action = TickActionV1::kAdd;
        projected.side = order->fields.side;
        projected.order_type = order->fields.order_type;
        projected.quantity = order->fields.quantity.raw;
        projected.primary_order_id =
            order->fields.primary_order_id;
        projected.quantity_valid =
            (order->fields.validity_bitmap &
             kTickQuantityValidV1) != 0U &&
            order->fields.quantity.valid;
        projected.side_valid =
            (order->fields.validity_bitmap &
             kTickSideValidV1) != 0U;
        projected.order_type_valid =
            (order->fields.validity_bitmap &
             kTickOrderTypeValidV1) != 0U;
        projected.price_valid =
            (order->fields.validity_bitmap &
             kTickPriceValidV1) != 0U &&
            order->fields.price.valid;
        projected.price_p6 =
            projected.price_valid
                ? order->fields.price.normalized_p6
                : 0;
        projected.source_quality_flags =
            common.quality_flags;
        projected.source_market_notices =
            common.market_notices;
        if (!ValidInput(projected)) {
            return ShenzhenOrderEventProjectionV1::kInvalidEvent;
        }
        *output = projected;
        return ShenzhenOrderEventProjectionV1::kProjected;
    }

    if (const auto* transaction =
            std::get_if<ShenzhenTransactionV1>(&event);
        transaction != nullptr) {
        const DecodedMarketCommonV1& common =
            transaction->common;
        if (common.kind !=
                MarketEventKindV1::kShenzhenTransaction ||
            common.market != MarketV1::kShenzhen ||
            transaction->fields.quantity.scale != 0U) {
            return ShenzhenOrderEventProjectionV1::kInvalidEvent;
        }
        ShenzhenOrderEventInputV1 projected{};
        projected.trade_date = common.origin.trade_date;
        projected.instrument_id = common.instrument_id;
        projected.channel = transaction->channel;
        FillAnchor(
            common,
            transaction->application_sequence,
            ingress_sequence,
            tick_stream_sequence,
            &projected.anchor);
        projected.action = transaction->fields.action;
        projected.side = transaction->fields.side;
        projected.order_type = OrderTypeV1::kUnknown;
        projected.quantity =
            transaction->fields.quantity.raw;
        projected.primary_order_id =
            transaction->fields.primary_order_id;
        projected.buy_order_id =
            transaction->fields.buy_order_id;
        projected.sell_order_id =
            transaction->fields.sell_order_id;
        projected.quantity_valid =
            (transaction->fields.validity_bitmap &
             kTickQuantityValidV1) != 0U &&
            transaction->fields.quantity.valid;
        projected.side_valid =
            (transaction->fields.validity_bitmap &
             kTickSideValidV1) != 0U;
        projected.price_valid =
            (transaction->fields.validity_bitmap &
             kTickPriceValidV1) != 0U &&
            transaction->fields.price.valid;
        projected.price_p6 =
            projected.price_valid
                ? transaction->fields.price.normalized_p6
                : 0;
        projected.source_quality_flags =
            common.quality_flags;
        projected.source_market_notices =
            common.market_notices;
        const std::uint32_t validity =
            transaction->fields.validity_bitmap;
        if (projected.action == TickActionV1::kTrade) {
            // 6.36 defines zero as "no corresponding order" independently
            // for BidApplSeqNum and OfferApplSeqNum. The decoder therefore
            // publishes an ID-valid bit exactly when the retained reference
            // is positive; requiring both bits would reject a documented
            // source value. Keep this decoded-event adapter identical to the
            // strict Wire V2 adapter.
            const bool has_buy = projected.buy_order_id > 0;
            const bool has_sell = projected.sell_order_id > 0;
            if (((validity & kTickBuyOrderIdValidV1) != 0U) !=
                    has_buy ||
                ((validity & kTickSellOrderIdValidV1) != 0U) !=
                    has_sell) {
                return ShenzhenOrderEventProjectionV1::kInvalidEvent;
            }
        }
        if (projected.action == TickActionV1::kCancel &&
            (validity &
             kTickPrimaryOrderIdValidV1) == 0U) {
            return ShenzhenOrderEventProjectionV1::kInvalidEvent;
        }
        if (!ValidInput(projected)) {
            return ShenzhenOrderEventProjectionV1::kInvalidEvent;
        }
        *output = projected;
        return ShenzhenOrderEventProjectionV1::kProjected;
    }

    return ShenzhenOrderEventProjectionV1::kNotShenzhenEvent;
}

class ShenzhenOrderEventProjectorV1::Impl final {
public:
    explicit Impl(
        ShenzhenOrderEventProjectorConfigV1 config_value) noexcept
        : config(std::move(config_value)) {}

    ShenzhenOrderEventProjectorConfigV1 config{};
    std::map<ShenzhenOrderKeyV1, OrderState> orders;
    std::map<std::uint32_t, std::int64_t>
        last_native_sequence_by_channel;
    std::uint64_t last_tick_stream_sequence = 0U;
    bool finalized = false;
    bool failed = false;
};

std::string_view ShenzhenOrderProjectorCreateErrorNameV1(
    ShenzhenOrderProjectorCreateErrorV1 error) noexcept {
    switch (error) {
        case ShenzhenOrderProjectorCreateErrorV1::kNone:
            return "none";
        case ShenzhenOrderProjectorCreateErrorV1::kNullOutput:
            return "null_output";
        case ShenzhenOrderProjectorCreateErrorV1::
            kInvalidConfiguration:
            return "invalid_configuration";
        case ShenzhenOrderProjectorCreateErrorV1::
            kResourceExhausted:
            return "resource_exhausted";
    }
    return "unknown";
}

std::string_view ShenzhenOrderProjectorConsumeErrorNameV1(
    ShenzhenOrderProjectorConsumeErrorV1 error) noexcept {
    switch (error) {
        case ShenzhenOrderProjectorConsumeErrorV1::kNone:
            return "none";
        case ShenzhenOrderProjectorConsumeErrorV1::kNullOutput:
            return "null_output";
        case ShenzhenOrderProjectorConsumeErrorV1::kInvalidInput:
            return "invalid_input";
        case ShenzhenOrderProjectorConsumeErrorV1::kWrongTradeDate:
            return "wrong_trade_date";
        case ShenzhenOrderProjectorConsumeErrorV1::
            kAlreadyFinalized:
            return "already_finalized";
        case ShenzhenOrderProjectorConsumeErrorV1::kOrderCapacity:
            return "order_capacity";
        case ShenzhenOrderProjectorConsumeErrorV1::kOutOfOrderInput:
            return "out_of_order_input";
        case ShenzhenOrderProjectorConsumeErrorV1::
            kResourceExhausted:
            return "resource_exhausted";
        case ShenzhenOrderProjectorConsumeErrorV1::kFailed:
            return "failed";
    }
    return "unknown";
}

std::string_view ShenzhenOrderProjectorQueryErrorNameV1(
    ShenzhenOrderProjectorQueryErrorV1 error) noexcept {
    switch (error) {
        case ShenzhenOrderProjectorQueryErrorV1::kNone:
            return "none";
        case ShenzhenOrderProjectorQueryErrorV1::kNullOutput:
            return "null_output";
        case ShenzhenOrderProjectorQueryErrorV1::kInvalidKey:
            return "invalid_key";
        case ShenzhenOrderProjectorQueryErrorV1::kNotFound:
            return "not_found";
    }
    return "unknown";
}

ShenzhenOrderEventProjectorV1::ShenzhenOrderEventProjectorV1(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

ShenzhenOrderEventProjectorV1::~ShenzhenOrderEventProjectorV1() =
    default;

ShenzhenOrderProjectorCreateErrorV1
ShenzhenOrderEventProjectorV1::Create(
    ShenzhenOrderEventProjectorConfigV1 config,
    std::unique_ptr<ShenzhenOrderEventProjectorV1>* output)
    noexcept {
    if (output == nullptr) {
        return ShenzhenOrderProjectorCreateErrorV1::kNullOutput;
    }
    output->reset();
    if (!ValidTradeDate(config.trade_date) ||
        config.maximum_order_states == 0U) {
        return ShenzhenOrderProjectorCreateErrorV1::
            kInvalidConfiguration;
    }
    try {
        auto impl = std::make_unique<Impl>(std::move(config));
        *output = std::unique_ptr<ShenzhenOrderEventProjectorV1>(
            new ShenzhenOrderEventProjectorV1(std::move(impl)));
        return ShenzhenOrderProjectorCreateErrorV1::kNone;
    } catch (const std::bad_alloc&) {
        return ShenzhenOrderProjectorCreateErrorV1::
            kResourceExhausted;
    } catch (...) {
        return ShenzhenOrderProjectorCreateErrorV1::
            kResourceExhausted;
    }
}

ShenzhenOrderProjectorConsumeErrorV1
ShenzhenOrderEventProjectorV1::Consume(
    const ShenzhenOrderEventInputV1& input,
    std::vector<ShenzhenOrderEventV1>* output) noexcept {
    if (output == nullptr) {
        return ShenzhenOrderProjectorConsumeErrorV1::kNullOutput;
    }
    output->clear();
    if (impl_ == nullptr) {
        return ShenzhenOrderProjectorConsumeErrorV1::kFailed;
    }
    if (impl_->failed) {
        return ShenzhenOrderProjectorConsumeErrorV1::kFailed;
    }
    if (!ValidInput(input)) {
        return ShenzhenOrderProjectorConsumeErrorV1::kInvalidInput;
    }
    if (input.trade_date != impl_->config.trade_date) {
        return ShenzhenOrderProjectorConsumeErrorV1::
            kWrongTradeDate;
    }
    if (impl_->finalized) {
        return ShenzhenOrderProjectorConsumeErrorV1::
            kAlreadyFinalized;
    }

    try {
        auto [channel_position, channel_inserted] =
            impl_->last_native_sequence_by_channel.try_emplace(
                input.channel, 0);
        static_cast<void>(channel_inserted);
        if ((impl_->last_tick_stream_sequence != 0U &&
             input.anchor.tick_stream_sequence <=
                 impl_->last_tick_stream_sequence) ||
            (channel_position->second != 0 &&
             input.anchor.native_event_sequence <=
                 channel_position->second)) {
            return ShenzhenOrderProjectorConsumeErrorV1::
                kOutOfOrderInput;
        }
        output->reserve(3U);
        ShenzhenOrderProjectorConsumeErrorV1 result =
            ShenzhenOrderProjectorConsumeErrorV1::kNone;
        if (input.action == TickActionV1::kAdd) {
            const ShenzhenOrderKeyV1 key{
                input.trade_date,
                input.instrument_id,
                input.channel,
                input.primary_order_id};
            const auto existing = impl_->orders.find(key);
            if (existing == impl_->orders.end()) {
                if (impl_->orders.size() >=
                    impl_->config.maximum_order_states) {
                    return ShenzhenOrderProjectorConsumeErrorV1::
                        kOrderCapacity;
                }
                OrderState state{};
                state.snapshot.key = key;
                state.snapshot.side = input.side;
                state.snapshot.order_type = input.order_type;
                state.snapshot.price_p6 =
                    input.price_valid ? input.price_p6 : 0;
                state.snapshot.price_valid = input.price_valid;
                state.snapshot.original_quantity =
                    input.quantity;
                state.snapshot.remaining_quantity =
                    input.quantity;
                state.snapshot.remaining_quantity_valid = true;
                state.snapshot.first_anchor = input.anchor;
                state.snapshot.last_anchor = input.anchor;
                state.snapshot.revision = 1U;
                state.snapshot.source_quality_flags =
                    input.source_quality_flags;
                state.snapshot.source_market_notices =
                    input.source_market_notices;
                const auto [iterator, inserted] =
                    impl_->orders.emplace(key, state);
                if (!inserted) {
                    return ShenzhenOrderProjectorConsumeErrorV1::
                        kFailed;
                }
                output->emplace_back(RevisionEvent(
                    ShenzhenOrderDeltaOperationV1::kInsert,
                    input.anchor,
                    iterator->second));
                result =
                    ShenzhenOrderProjectorConsumeErrorV1::kNone;
            } else {
                OrderState& state = existing->second;
                state.snapshot.quality_flags |=
                    ShenzhenEventQualityBitV1(
                        ShenzhenEventQualityFlagV1::
                            kDuplicateOrder);
                if (state.snapshot.side != input.side) {
                    state.snapshot.quality_flags |=
                        ShenzhenEventQualityBitV1(
                            ShenzhenEventQualityFlagV1::
                                kSideConflict);
                }
                if (state.snapshot.original_quantity !=
                    input.quantity) {
                    state.snapshot.quality_flags |=
                        ShenzhenEventQualityBitV1(
                            ShenzhenEventQualityFlagV1::
                                kQuantityConflict);
                }
                MarkMutation(input, &state);
                IncrementRevision(&state);
                RefreshFinality(&state);
                output->emplace_back(RevisionEvent(
                    ShenzhenOrderDeltaOperationV1::kUpdate,
                    input.anchor,
                    state));
            }
        } else if (input.action == TickActionV1::kTrade) {
            const ShenzhenOrderKeyV1 buy_key{
                input.trade_date,
                input.instrument_id,
                input.channel,
                input.buy_order_id};
            const ShenzhenOrderKeyV1 sell_key{
                input.trade_date,
                input.instrument_id,
                input.channel,
                input.sell_order_id};
            const bool ambiguous_references =
                input.buy_order_id > 0 &&
                input.buy_order_id == input.sell_order_id;
            auto buy = ambiguous_references ||
                               input.buy_order_id == 0
                           ? impl_->orders.end()
                           : impl_->orders.find(buy_key);
            auto sell = ambiguous_references ||
                                input.sell_order_id == 0
                            ? impl_->orders.end()
                            : impl_->orders.find(sell_key);

            ShenzhenTradeEventV1 trade{};
            trade.trade_date = input.trade_date;
            trade.instrument_id = input.instrument_id;
            trade.channel = input.channel;
            trade.source_anchor = input.anchor;
            trade.buy_order_id = input.buy_order_id;
            trade.sell_order_id = input.sell_order_id;
            trade.aggressor = AggressorV1::kUnknown;
            trade.price_p6 = input.price_p6;
            trade.quantity = input.quantity;
            trade.amount_p6 = 0;
            trade.amount_valid = false;
            trade.source_quality_flags =
                input.source_quality_flags;
            trade.source_market_notices =
                input.source_market_notices;
            if (ambiguous_references) {
                trade.quality_flags |=
                    ShenzhenEventQualityBitV1(
                        ShenzhenEventQualityFlagV1::
                            kAmbiguousTradeOrderReferences);
            } else if (buy == impl_->orders.end()) {
                trade.quality_flags |=
                    ShenzhenEventQualityBitV1(
                        ShenzhenEventQualityFlagV1::
                            kUnknownBuyOrderReference);
            }
            if (!ambiguous_references &&
                sell == impl_->orders.end()) {
                trade.quality_flags |=
                    ShenzhenEventQualityBitV1(
                        ShenzhenEventQualityFlagV1::
                            kUnknownSellOrderReference);
            }

            struct Updated final {
                ShenzhenOrderKeyV1 key{};
                OrderState* state = nullptr;
                ShenzhenOrderDeltaOperationV1 operation =
                    ShenzhenOrderDeltaOperationV1::kUpdate;
            };
            std::array<Updated, 2U> updates{};
            std::size_t update_count = 0U;
            if (buy != impl_->orders.end()) {
                trade.quality_flags |= ApplyQuantityReduction(
                    input, SideV1::kBuy, true, &buy->second);
                updates[update_count++] = {
                    buy_key,
                    &buy->second,
                    MutationOperation(&buy->second)};
            }
            if (sell != impl_->orders.end()) {
                trade.quality_flags |= ApplyQuantityReduction(
                    input, SideV1::kSell, true, &sell->second);
                updates[update_count++] = {
                    sell_key,
                    &sell->second,
                    MutationOperation(&sell->second)};
            }
            std::sort(
                updates.begin(),
                updates.begin() +
                    static_cast<std::ptrdiff_t>(update_count),
                [](const Updated& lhs, const Updated& rhs) {
                    return lhs.key < rhs.key;
                });

            output->emplace_back(trade);
            for (std::size_t index = 0U;
                 index < update_count;
                 ++index) {
                output->emplace_back(RevisionEvent(
                    updates[index].operation,
                    input.anchor,
                    *updates[index].state));
            }
        } else {
            const ShenzhenOrderKeyV1 key{
                input.trade_date,
                input.instrument_id,
                input.channel,
                input.primary_order_id};
            const auto existing = impl_->orders.find(key);
            ShenzhenCancelEventV1 cancel{};
            cancel.key = key;
            cancel.source_anchor = input.anchor;
            cancel.side =
                existing == impl_->orders.end()
                    ? input.side
                    : existing->second.snapshot.side;
            cancel.side_from_order =
                existing != impl_->orders.end();
            cancel.quantity = input.quantity;
            cancel.referenced_order_found =
                existing != impl_->orders.end();
            cancel.source_quality_flags =
                input.source_quality_flags;
            cancel.source_market_notices =
                input.source_market_notices;
            if (existing == impl_->orders.end()) {
                cancel.quality_flags |=
                    ShenzhenEventQualityBitV1(
                        ShenzhenEventQualityFlagV1::
                            kUnknownCancelOrderReference);
                output->emplace_back(cancel);
            } else {
                OrderState& state = existing->second;
                cancel.quality_flags |= ApplyQuantityReduction(
                    input, input.side, false, &state);
                output->emplace_back(cancel);
                output->emplace_back(RevisionEvent(
                    MutationOperation(&state),
                    input.anchor,
                    state));
            }
        }
        if (result == ShenzhenOrderProjectorConsumeErrorV1::kNone) {
            impl_->last_tick_stream_sequence =
                input.anchor.tick_stream_sequence;
            channel_position->second =
                input.anchor.native_event_sequence;
        }
        return result;
    } catch (const std::bad_alloc&) {
        output->clear();
        impl_->failed = true;
        return ShenzhenOrderProjectorConsumeErrorV1::
            kResourceExhausted;
    } catch (...) {
        output->clear();
        impl_->failed = true;
        return ShenzhenOrderProjectorConsumeErrorV1::kFailed;
    }
}

ShenzhenOrderProjectorConsumeErrorV1
ShenzhenOrderEventProjectorV1::ConsumeDecoded(
    const DecodedMarketEventV1& event,
    std::uint64_t ingress_sequence,
    std::uint64_t tick_stream_sequence,
    std::vector<ShenzhenOrderEventV1>* output) noexcept {
    if (output == nullptr) {
        return ShenzhenOrderProjectorConsumeErrorV1::kNullOutput;
    }
    output->clear();
    ShenzhenOrderEventInputV1 input{};
    const ShenzhenOrderEventProjectionV1 projected =
        ProjectShenzhenOrderEventInputV1(
            event,
            ingress_sequence,
            tick_stream_sequence,
            &input);
    if (projected !=
        ShenzhenOrderEventProjectionV1::kProjected) {
        return ShenzhenOrderProjectorConsumeErrorV1::
            kInvalidInput;
    }
    return Consume(input, output);
}

ShenzhenOrderProjectorConsumeErrorV1
ShenzhenOrderEventProjectorV1::Finalize(
    const ShenzhenEventSourceAnchorV1& source_anchor,
    std::vector<ShenzhenOrderEventV1>* output) noexcept {
    if (output == nullptr) {
        return ShenzhenOrderProjectorConsumeErrorV1::kNullOutput;
    }
    output->clear();
    if (impl_ == nullptr) {
        return ShenzhenOrderProjectorConsumeErrorV1::kFailed;
    }
    if (impl_->failed) {
        return ShenzhenOrderProjectorConsumeErrorV1::kFailed;
    }
    if (!ValidAnchor(source_anchor) &&
        !ZeroAnchor(source_anchor)) {
        return ShenzhenOrderProjectorConsumeErrorV1::kInvalidInput;
    }
    if (impl_->finalized) {
        return ShenzhenOrderProjectorConsumeErrorV1::
            kAlreadyFinalized;
    }

    try {
        output->reserve(impl_->orders.size());
        for (auto& [key, state] : impl_->orders) {
            static_cast<void>(key);
            if (state.finalization_emitted) {
                continue;
            }
            if (state.snapshot.remaining_quantity != 0) {
                state.snapshot.quality_flags |=
                    ShenzhenEventQualityBitV1(
                        ShenzhenEventQualityFlagV1::
                            kEndedWithObservedBalance);
            }
            state.terminal = true;
            if (!ZeroAnchor(source_anchor)) {
                state.snapshot.last_anchor = source_anchor;
            }
            IncrementRevision(&state);
            RefreshFinality(&state);
            state.finalization_emitted = true;
            output->emplace_back(RevisionEvent(
                ShenzhenOrderDeltaOperationV1::kFinalize,
                source_anchor,
                state));
        }
        impl_->finalized = true;
        return ShenzhenOrderProjectorConsumeErrorV1::kNone;
    } catch (const std::bad_alloc&) {
        output->clear();
        impl_->failed = true;
        return ShenzhenOrderProjectorConsumeErrorV1::
            kResourceExhausted;
    } catch (...) {
        output->clear();
        impl_->failed = true;
        return ShenzhenOrderProjectorConsumeErrorV1::kFailed;
    }
}

ShenzhenOrderProjectorQueryErrorV1
ShenzhenOrderEventProjectorV1::GetOrder(
    const ShenzhenOrderKeyV1& key,
    ShenzhenOrderSnapshotV1* output) const noexcept {
    if (output == nullptr) {
        return ShenzhenOrderProjectorQueryErrorV1::kNullOutput;
    }
    *output = {};
    if (impl_ == nullptr || !ValidTradeDate(key.trade_date) ||
        key.instrument_id == 0U || key.channel == 0U ||
        key.order_id <= 0) {
        return ShenzhenOrderProjectorQueryErrorV1::kInvalidKey;
    }
    const auto found = impl_->orders.find(key);
    if (found == impl_->orders.end()) {
        return ShenzhenOrderProjectorQueryErrorV1::kNotFound;
    }
    *output = found->second.snapshot;
    return ShenzhenOrderProjectorQueryErrorV1::kNone;
}

std::size_t ShenzhenOrderEventProjectorV1::order_count()
    const noexcept {
    return impl_ == nullptr ? 0U : impl_->orders.size();
}

bool ShenzhenOrderEventProjectorV1::finalized() const noexcept {
    return impl_ != nullptr && impl_->finalized;
}

const ShenzhenOrderEventProjectorConfigV1&
ShenzhenOrderEventProjectorV1::config() const noexcept {
    return impl_->config;
}

}  // namespace l2flow::market
