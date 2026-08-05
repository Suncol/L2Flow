#include "l2flow/market/shanghai_order_event_aggregator_v1.h"

#include <algorithm>
#include <array>
#include <limits>
#include <map>
#include <new>
#include <tuple>
#include <utility>

namespace l2flow::market {
namespace {

constexpr std::uint64_t kConflictQualityMask =
    ShanghaiOrderQualityBitV1(
        ShanghaiOrderQualityFlagV1::kPrematchQuantityMismatch) |
    ShanghaiOrderQualityBitV1(
        ShanghaiOrderQualityFlagV1::kPrematchQuantityUnavailable) |
    ShanghaiOrderQualityBitV1(
        ShanghaiOrderQualityFlagV1::kQuantityConflict) |
    ShanghaiOrderQualityBitV1(
        ShanghaiOrderQualityFlagV1::kPhaseUnknown) |
    ShanghaiOrderQualityBitV1(
        ShanghaiOrderQualityFlagV1::kUnexpectedPhase) |
    ShanghaiOrderQualityBitV1(
        ShanghaiOrderQualityFlagV1::kSideConflict) |
    ShanghaiOrderQualityBitV1(
        ShanghaiOrderQualityFlagV1::kDuplicateAdd) |
    ShanghaiOrderQualityBitV1(
        ShanghaiOrderQualityFlagV1::kCancelWithoutAdd);

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

[[nodiscard]] bool SupportedPhaseValue(
    TradingPhaseV1 phase) noexcept {
    return static_cast<std::uint8_t>(phase) <=
           static_cast<std::uint8_t>(TradingPhaseV1::kEnd);
}

[[nodiscard]] bool IsDirectOriginalQuantityPhase(
    TradingPhaseV1 phase) noexcept {
    return phase == TradingPhaseV1::kOpeningCall ||
           phase == TradingPhaseV1::kSuspended ||
           phase == TradingPhaseV1::kClosingCall;
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
    const ShanghaiOrderSourceAnchorV1& anchor) noexcept {
    return anchor.native_event_sequence > 0 &&
           anchor.source_sequence > 0U &&
           anchor.ingress_sequence > 0U &&
           anchor.tick_stream_sequence > 0U &&
           (!anchor.event_time_unix_ns_valid ||
            anchor.event_time_valid);
}

[[nodiscard]] bool ZeroAnchor(
    const ShanghaiOrderSourceAnchorV1& anchor) noexcept {
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

[[nodiscard]] bool ValidInput(
    const ShanghaiOrderEventInputV1& input) noexcept {
    if (!ValidTradeDate(input.trade_date) ||
        input.instrument_id == 0U || input.channel <= 0 ||
        !ValidAnchor(input.anchor) ||
        !SupportedPhaseValue(input.phase) ||
        (input.phase_valid &&
         input.phase == TradingPhaseV1::kUnknown)) {
        return false;
    }
    switch (input.action) {
        case TickActionV1::kAdd:
            return (input.side == SideV1::kBuy ||
                    input.side == SideV1::kSell) &&
                   input.primary_order_id > 0 &&
                   input.price_valid && input.price_p6 > 0 &&
                   input.quantity_valid && input.quantity > 0 &&
                   (!input.matched_quantity_valid ||
                    input.matched_quantity >= 0);
        case TickActionV1::kCancel:
            return (input.side == SideV1::kBuy ||
                    input.side == SideV1::kSell) &&
                   input.primary_order_id > 0 &&
                   input.quantity_valid && input.quantity > 0;
        case TickActionV1::kTrade:
            return input.buy_order_id > 0 &&
                   input.sell_order_id > 0 &&
                   input.buy_order_id != input.sell_order_id &&
                   input.price_valid && input.price_p6 > 0 &&
                   input.quantity_valid && input.quantity > 0 &&
                   (input.aggressor == AggressorV1::kUnknown ||
                    input.aggressor == AggressorV1::kBuy ||
                    input.aggressor == AggressorV1::kSell ||
                    input.aggressor == AggressorV1::kNeutral);
        case TickActionV1::kStatus:
            return true;
        case TickActionV1::kUnknown:
            return false;
    }
    return false;
}

struct OrderState final {
    ShanghaiOrderSnapshotV1 snapshot{};
    std::int64_t pre_add_active_trade_quantity = 0;
    std::int64_t minimum_execution_price_p6 = 0;
    std::int64_t maximum_execution_price_p6 = 0;
    bool execution_prices_seen = false;
    bool terminal = false;
    bool finalization_emitted = false;
};

[[nodiscard]] bool HasConflict(const OrderState& state) noexcept {
    return (state.snapshot.quality_flags &
            kConflictQualityMask) != 0U;
}

void RefreshExecutionBoundary(OrderState* state) noexcept {
    if (state == nullptr || !state->execution_prices_seen) {
        return;
    }
    if (state->snapshot.side == SideV1::kBuy) {
        state->snapshot.execution_boundary_price_p6 =
            state->maximum_execution_price_p6;
    } else if (state->snapshot.side == SideV1::kSell) {
        state->snapshot.execution_boundary_price_p6 =
            state->minimum_execution_price_p6;
    } else {
        return;
    }
    state->snapshot.execution_boundary_price_valid = true;
}

void RefreshDerivedState(OrderState* state) noexcept {
    if (state == nullptr) {
        return;
    }
    RefreshExecutionBoundary(state);
    if (!state->snapshot.add_seen) {
        state->snapshot.order_source =
            ShanghaiOrderSourceV1::kReconstructedFromTrades;
        state->snapshot.original_quantity =
            state->snapshot.total_trade_quantity;
        state->snapshot.original_quantity_valid =
            state->snapshot.total_trade_quantity > 0;
        state->snapshot.original_quantity_status =
            state->snapshot.original_quantity_valid
                ? ShanghaiOriginalQuantityStatusV1::kLowerBound
                : ShanghaiOriginalQuantityStatusV1::kUnknown;
        state->snapshot.observed_pre_add_trade_quantity =
            state->pre_add_active_trade_quantity;
        state->snapshot.quality_flags |=
            ShanghaiOrderQualityBitV1(
                ShanghaiOrderQualityFlagV1::kSyntheticOrder) |
            ShanghaiOrderQualityBitV1(
                ShanghaiOrderQualityFlagV1::
                    kOriginalQuantityLowerBound);
        if (state->snapshot.execution_boundary_price_valid) {
            state->snapshot.price_p6 =
                state->snapshot.execution_boundary_price_p6;
            state->snapshot.price_valid = true;
            state->snapshot.price_source =
                state->snapshot.side == SideV1::kBuy
                    ? ShanghaiOrderPriceSourceV1::
                          kBuyMaximumExecution
                    : ShanghaiOrderPriceSourceV1::
                          kSellMinimumExecution;
            state->snapshot.quality_flags |=
                ShanghaiOrderQualityBitV1(
                    ShanghaiOrderQualityFlagV1::
                        kExecutionBoundaryPrice);
        }
    }
    if (HasConflict(*state)) {
        state->snapshot.finality =
            ShanghaiOrderFinalityV1::kConflict;
    } else if (state->terminal) {
        state->snapshot.finality =
            ShanghaiOrderFinalityV1::kFinal;
    } else {
        state->snapshot.finality =
            ShanghaiOrderFinalityV1::kProvisional;
    }
}

void InitializeState(
    const ShanghaiOrderKeyV1& key,
    SideV1 side,
    ShanghaiOrderSideSourceV1 side_source,
    const ShanghaiOrderEventInputV1& input,
    OrderState* state) noexcept {
    state->snapshot.key = key;
    state->snapshot.side = side;
    state->snapshot.side_source = side_source;
    state->snapshot.phase_at_first =
        input.phase_valid ? input.phase : TradingPhaseV1::kUnknown;
    state->snapshot.phase_at_last =
        state->snapshot.phase_at_first;
    state->snapshot.first_anchor = input.anchor;
    state->snapshot.last_anchor = input.anchor;
    state->snapshot.source_quality_flags =
        input.source_quality_flags;
    state->snapshot.source_market_notices =
        input.source_market_notices;
    if (!input.phase_valid ||
        input.phase == TradingPhaseV1::kUnknown) {
        state->snapshot.quality_flags |=
            ShanghaiOrderQualityBitV1(
                ShanghaiOrderQualityFlagV1::kPhaseUnknown);
    }
}

void MarkMutation(
    const ShanghaiOrderEventInputV1& input,
    OrderState* state) noexcept {
    state->snapshot.last_anchor = input.anchor;
    state->snapshot.phase_at_last =
        input.phase_valid ? input.phase : TradingPhaseV1::kUnknown;
    state->snapshot.source_quality_flags |=
        input.source_quality_flags;
    state->snapshot.source_market_notices |=
        input.source_market_notices;
    if (!input.phase_valid ||
        input.phase == TradingPhaseV1::kUnknown) {
        state->snapshot.quality_flags |=
            ShanghaiOrderQualityBitV1(
                ShanghaiOrderQualityFlagV1::kPhaseUnknown);
    }
}

void MarkSideConflict(
    SideV1 expected,
    OrderState* state) noexcept {
    if (state->snapshot.side != expected) {
        state->snapshot.quality_flags |=
            ShanghaiOrderQualityBitV1(
                ShanghaiOrderQualityFlagV1::kSideConflict);
    }
}

[[nodiscard]] bool CanApplyTrade(
    const OrderState& state,
    std::int64_t quantity,
    bool active_continuous) noexcept {
    std::int64_t ignored = 0;
    if (!CheckedAdd(
            state.snapshot.total_trade_quantity,
            quantity,
            &ignored) ||
        state.snapshot.trade_count ==
            std::numeric_limits<std::uint64_t>::max()) {
        return false;
    }
    if (state.snapshot.add_seen) {
        return CheckedAdd(
                   state.snapshot.post_add_trade_quantity,
                   quantity,
                   &ignored) &&
               CheckedSubtract(
                   state.snapshot.remaining_quantity,
                   quantity,
                   &ignored);
    }
    return !active_continuous ||
           CheckedAdd(
               state.pre_add_active_trade_quantity,
               quantity,
               &ignored);
}

void ApplyTrade(
    const ShanghaiOrderEventInputV1& input,
    SideV1 expected_side,
    bool active_continuous,
    OrderState* state) noexcept {
    MarkSideConflict(expected_side, state);
    if (state->terminal) {
        state->snapshot.quality_flags |=
            ShanghaiOrderQualityBitV1(
                ShanghaiOrderQualityFlagV1::kQuantityConflict);
    }
    std::int64_t value = 0;
    const bool total_ok = CheckedAdd(
        state->snapshot.total_trade_quantity,
        input.quantity,
        &value);
    if (total_ok) {
        state->snapshot.total_trade_quantity = value;
    }
    ++state->snapshot.trade_count;

    if (!state->execution_prices_seen) {
        state->minimum_execution_price_p6 = input.price_p6;
        state->maximum_execution_price_p6 = input.price_p6;
        state->execution_prices_seen = true;
    } else {
        state->minimum_execution_price_p6 = std::min(
            state->minimum_execution_price_p6,
            input.price_p6);
        state->maximum_execution_price_p6 = std::max(
            state->maximum_execution_price_p6,
            input.price_p6);
    }

    if (state->snapshot.add_seen) {
        const bool post_ok = CheckedAdd(
            state->snapshot.post_add_trade_quantity,
            input.quantity,
            &value);
        if (post_ok) {
            state->snapshot.post_add_trade_quantity = value;
        }
        const bool remaining_ok = CheckedSubtract(
            state->snapshot.remaining_quantity,
            input.quantity,
            &value);
        if (remaining_ok) {
            state->snapshot.remaining_quantity = value;
            state->snapshot.remaining_quantity_valid = value >= 0;
            if (value < 0) {
                state->snapshot.quality_flags |=
                    ShanghaiOrderQualityBitV1(
                        ShanghaiOrderQualityFlagV1::
                            kQuantityConflict);
            } else if (value == 0) {
                state->terminal = true;
            }
        }
    } else if (active_continuous) {
        const bool pre_ok = CheckedAdd(
            state->pre_add_active_trade_quantity,
            input.quantity,
            &value);
        if (pre_ok) {
            state->pre_add_active_trade_quantity = value;
        }
    }
    MarkMutation(input, state);
    RefreshDerivedState(state);
}

[[nodiscard]] bool CanApplyCancel(
    const OrderState& state,
    std::int64_t quantity) noexcept {
    std::int64_t ignored = 0;
    if (!CheckedAdd(
            state.snapshot.total_cancel_quantity,
            quantity,
            &ignored)) {
        return false;
    }
    return !state.snapshot.add_seen ||
           CheckedSubtract(
               state.snapshot.remaining_quantity,
               quantity,
               &ignored);
}

void ApplyCancel(
    const ShanghaiOrderEventInputV1& input,
    OrderState* state) noexcept {
    MarkSideConflict(input.side, state);
    if (state->terminal) {
        state->snapshot.quality_flags |=
            ShanghaiOrderQualityBitV1(
                ShanghaiOrderQualityFlagV1::kQuantityConflict);
    }
    std::int64_t value = 0;
    const bool cancel_ok = CheckedAdd(
        state->snapshot.total_cancel_quantity,
        input.quantity,
        &value);
    if (cancel_ok) {
        state->snapshot.total_cancel_quantity = value;
    }
    if (state->snapshot.add_seen) {
        const bool remaining_ok = CheckedSubtract(
            state->snapshot.remaining_quantity,
            input.quantity,
            &value);
        if (remaining_ok) {
            state->snapshot.remaining_quantity = value;
            state->snapshot.remaining_quantity_valid = value >= 0;
            if (value < 0) {
                state->snapshot.quality_flags |=
                    ShanghaiOrderQualityBitV1(
                        ShanghaiOrderQualityFlagV1::
                            kQuantityConflict);
            } else if (value == 0) {
                state->terminal = true;
            }
        }
    } else {
        state->snapshot.quality_flags |=
            ShanghaiOrderQualityBitV1(
                ShanghaiOrderQualityFlagV1::kCancelWithoutAdd);
        state->terminal = true;
    }
    MarkMutation(input, state);
    RefreshDerivedState(state);
}

}  // namespace

bool operator<(
    const ShanghaiOrderKeyV1& lhs,
    const ShanghaiOrderKeyV1& rhs) noexcept {
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

ShanghaiOrderEventProjectionV1 ProjectShanghaiOrderEventInputV1(
    const DecodedMarketEventV1& event,
    std::uint64_t ingress_sequence,
    std::uint64_t tick_stream_sequence,
    ShanghaiOrderEventInputV1* output) noexcept {
    if (output == nullptr) {
        return ShanghaiOrderEventProjectionV1::kInvalidTick;
    }
    *output = {};
    const ShanghaiTickV1* tick =
        std::get_if<ShanghaiTickV1>(&event);
    if (tick == nullptr) {
        return ShanghaiOrderEventProjectionV1::kNotShanghaiTick;
    }
    if (tick->common.kind !=
            MarketEventKindV1::kShanghaiTick ||
        tick->common.market != MarketV1::kShanghai ||
        tick->common.origin.trade_date == 0U ||
        tick->common.instrument_id == 0U ||
        tick->channel <= 0 || tick->business_index <= 0 ||
        ingress_sequence == 0U || tick_stream_sequence == 0U ||
        (tick->fields.quantity.valid &&
         tick->fields.quantity.scale != 0U) ||
        (tick->fields.matched_quantity.valid &&
         tick->fields.matched_quantity.scale != 0U)) {
        return ShanghaiOrderEventProjectionV1::kInvalidTick;
    }

    ShanghaiOrderEventInputV1 projected{};
    projected.trade_date = tick->common.origin.trade_date;
    projected.instrument_id = tick->common.instrument_id;
    projected.channel = tick->channel;
    projected.anchor.native_event_sequence =
        tick->business_index;
    projected.anchor.source_sequence =
        tick->common.origin.source_sequence;
    projected.anchor.ingress_sequence = ingress_sequence;
    projected.anchor.tick_stream_sequence =
        tick_stream_sequence;
    projected.anchor.vendor_sequence_id =
        tick->common.origin.vendor_sequence_id;
    projected.anchor.event_time_ns_since_midnight =
        tick->common.exchange_time.nanoseconds_since_midnight;
    projected.anchor.event_time_unix_ns =
        tick->common.exchange_time.unix_nanoseconds;
    projected.anchor.recv_realtime_ns =
        tick->common.origin.recv_realtime_ns;
    projected.anchor.recv_monotonic_ns =
        tick->common.origin.recv_monotonic_ns;
    projected.anchor.vendor_local_time_raw =
        tick->common.origin.vendor_local_time_raw;
    projected.anchor.vendor_local_time_ns_since_midnight =
        tick->common.vendor_local_time.nanoseconds_since_midnight;
    projected.anchor.event_time_valid =
        tick->common.exchange_time.valid &&
        (tick->fields.validity_bitmap &
         kTickExchangeTimeValidV1) != 0U;
    projected.anchor.event_time_unix_ns_valid =
        projected.anchor.event_time_valid &&
        tick->common.exchange_time.unix_nanoseconds_valid;
    projected.anchor.vendor_local_time_valid =
        tick->common.vendor_local_time.valid;
    projected.action = tick->fields.action;
    projected.side = tick->fields.side;
    projected.aggressor = tick->fields.aggressor;
    projected.phase = tick->fields.phase;
    projected.price_p6 =
        tick->fields.price.normalized_p6;
    projected.trade_amount_p6 =
        tick->fields.trade_amount.normalized_p6;
    projected.quantity = tick->fields.quantity.raw;
    projected.matched_quantity =
        tick->fields.matched_quantity.raw;
    projected.primary_order_id =
        tick->fields.primary_order_id;
    projected.buy_order_id = tick->fields.buy_order_id;
    projected.sell_order_id =
        tick->fields.sell_order_id;
    projected.price_valid =
        tick->fields.price.valid &&
        (tick->fields.validity_bitmap &
         kTickPriceValidV1) != 0U;
    projected.trade_amount_valid =
        tick->fields.trade_amount.valid &&
        (tick->fields.validity_bitmap &
         kTickTradeAmountValidV1) != 0U;
    projected.quantity_valid =
        tick->fields.quantity.valid &&
        (tick->fields.validity_bitmap &
         kTickQuantityValidV1) != 0U;
    projected.matched_quantity_valid =
        tick->fields.matched_quantity.valid &&
        (tick->fields.validity_bitmap &
         kTickMatchedQuantityValidV1) != 0U;
    projected.phase_valid =
        (tick->fields.validity_bitmap &
         kTickPhaseValidV1) != 0U;
    projected.source_quality_flags =
        tick->common.quality_flags;
    projected.source_market_notices =
        tick->common.market_notices;
    *output = projected;
    return ShanghaiOrderEventProjectionV1::kProjected;
}

class ShanghaiOrderEventAggregatorV1::Impl final {
public:
    explicit Impl(
        ShanghaiOrderEventAggregatorConfigV1 config) noexcept
        : config_(config) {}

    [[nodiscard]] ShanghaiOrderAggregatorConsumeErrorV1 Consume(
        const ShanghaiOrderEventInputV1& input,
        std::uint64_t ordering_sequence,
        std::vector<ShanghaiOrderEventV1>* output) {
        if (output == nullptr) {
            return ShanghaiOrderAggregatorConsumeErrorV1::kNullOutput;
        }
        output->clear();
        if (failed_) {
            return ShanghaiOrderAggregatorConsumeErrorV1::kFailed;
        }
        if (finalized_) {
            return ShanghaiOrderAggregatorConsumeErrorV1::
                kAlreadyFinalized;
        }
        if (!ValidInput(input) || ordering_sequence == 0U ||
            ordering_sequence ==
                std::numeric_limits<std::uint64_t>::max()) {
            return ShanghaiOrderAggregatorConsumeErrorV1::kInvalidInput;
        }
        if (input.trade_date != config_.trade_date) {
            return ShanghaiOrderAggregatorConsumeErrorV1::kWrongTradeDate;
        }
        auto [channel_position, channel_inserted] =
            last_native_sequence_by_channel_.try_emplace(
                input.channel, 0);
        static_cast<void>(channel_inserted);
        if ((last_ordering_sequence_ != 0U &&
             ordering_sequence <= last_ordering_sequence_) ||
            (channel_position->second != 0 &&
             input.anchor.native_event_sequence <=
                 channel_position->second)) {
            return ShanghaiOrderAggregatorConsumeErrorV1::
                kOutOfOrderInput;
        }
        ShanghaiOrderAggregatorConsumeErrorV1 error =
            ShanghaiOrderAggregatorConsumeErrorV1::kInvalidInput;
        switch (input.action) {
            case TickActionV1::kAdd:
                error = ConsumeAdd(input, output);
                break;
            case TickActionV1::kCancel:
                error = ConsumeCancel(input, output);
                break;
            case TickActionV1::kTrade:
                error = ConsumeTrade(input, output);
                break;
            case TickActionV1::kStatus:
                error = ConsumeStatus(input, output);
                break;
            case TickActionV1::kUnknown:
                return ShanghaiOrderAggregatorConsumeErrorV1::
                    kInvalidInput;
        }
        if (error == ShanghaiOrderAggregatorConsumeErrorV1::kNone) {
            last_ordering_sequence_ = ordering_sequence;
            channel_position->second =
                input.anchor.native_event_sequence;
        }
        return error;
    }

    [[nodiscard]] ShanghaiOrderAggregatorConsumeErrorV1 Finalize(
        const ShanghaiOrderSourceAnchorV1& source_anchor,
        std::vector<ShanghaiOrderEventV1>* output) {
        if (output == nullptr) {
            return ShanghaiOrderAggregatorConsumeErrorV1::kNullOutput;
        }
        output->clear();
        if (failed_) {
            return ShanghaiOrderAggregatorConsumeErrorV1::kFailed;
        }
        if (finalized_) {
            return ShanghaiOrderAggregatorConsumeErrorV1::
                kAlreadyFinalized;
        }
        if (!ValidAnchor(source_anchor) &&
            !ZeroAnchor(source_anchor)) {
            return ShanghaiOrderAggregatorConsumeErrorV1::
                kInvalidInput;
        }
        output->reserve(orders_.size());
        for (auto& [key, state] : orders_) {
            static_cast<void>(key);
            const ShanghaiOrderAggregatorConsumeErrorV1 error =
                FinalizeState(source_anchor, &state, output);
            if (error !=
                ShanghaiOrderAggregatorConsumeErrorV1::kNone) {
                return error;
            }
        }
        finalized_ = true;
        return ShanghaiOrderAggregatorConsumeErrorV1::kNone;
    }

    [[nodiscard]] ShanghaiOrderAggregatorQueryErrorV1 GetOrder(
        const ShanghaiOrderKeyV1& key,
        ShanghaiOrderSnapshotV1* output) const noexcept {
        if (output == nullptr) {
            return ShanghaiOrderAggregatorQueryErrorV1::kNullOutput;
        }
        *output = {};
        if (!ValidTradeDate(key.trade_date) ||
            key.instrument_id == 0U || key.channel <= 0 ||
            key.order_id <= 0) {
            return ShanghaiOrderAggregatorQueryErrorV1::kInvalidKey;
        }
        const auto found = orders_.find(key);
        if (found == orders_.end()) {
            return ShanghaiOrderAggregatorQueryErrorV1::kNotFound;
        }
        *output = found->second.snapshot;
        return ShanghaiOrderAggregatorQueryErrorV1::kNone;
    }

    [[nodiscard]] std::size_t order_count() const noexcept {
        return orders_.size();
    }

    [[nodiscard]] ShanghaiOrderAggregatorConsumeErrorV1
    MaximumOutputForInput(
        const ShanghaiOrderEventInputV1& input,
        std::size_t* output) const noexcept {
        if (output == nullptr) {
            return ShanghaiOrderAggregatorConsumeErrorV1::kNullOutput;
        }
        *output = 0U;
        if (failed_) {
            return ShanghaiOrderAggregatorConsumeErrorV1::kFailed;
        }
        if (finalized_) {
            return ShanghaiOrderAggregatorConsumeErrorV1::
                kAlreadyFinalized;
        }
        if (!ValidInput(input)) {
            return ShanghaiOrderAggregatorConsumeErrorV1::kInvalidInput;
        }
        if (input.trade_date != config_.trade_date) {
            return ShanghaiOrderAggregatorConsumeErrorV1::kWrongTradeDate;
        }
        switch (input.action) {
            case TickActionV1::kAdd:
                *output = 1U;
                return ShanghaiOrderAggregatorConsumeErrorV1::kNone;
            case TickActionV1::kCancel:
                *output = 2U;
                return ShanghaiOrderAggregatorConsumeErrorV1::kNone;
            case TickActionV1::kTrade:
                *output = 3U;
                return ShanghaiOrderAggregatorConsumeErrorV1::kNone;
            case TickActionV1::kStatus:
                return StatusOutputCount(input, output);
            case TickActionV1::kUnknown:
                return ShanghaiOrderAggregatorConsumeErrorV1::
                    kInvalidInput;
        }
        return ShanghaiOrderAggregatorConsumeErrorV1::kInvalidInput;
    }

    [[nodiscard]] bool finalized() const noexcept {
        return finalized_;
    }

    [[nodiscard]] const ShanghaiOrderEventAggregatorConfigV1&
    config() const noexcept {
        return config_;
    }

    void MarkFailed() noexcept {
        failed_ = true;
    }

private:
    using OrderMap = std::map<ShanghaiOrderKeyV1, OrderState>;

    [[nodiscard]] std::pair<
        OrderMap::iterator,
        OrderMap::iterator>
    InstrumentRange(
        const ShanghaiOrderEventInputV1& input) noexcept {
        const ShanghaiOrderKeyV1 lower{
            input.trade_date,
            input.instrument_id,
            input.channel,
            std::numeric_limits<std::int64_t>::min()};
        const ShanghaiOrderKeyV1 upper{
            input.trade_date,
            input.instrument_id,
            input.channel,
            std::numeric_limits<std::int64_t>::max()};
        return {orders_.lower_bound(lower), orders_.upper_bound(upper)};
    }

    [[nodiscard]] std::pair<
        OrderMap::const_iterator,
        OrderMap::const_iterator>
    InstrumentRange(
        const ShanghaiOrderEventInputV1& input) const noexcept {
        const ShanghaiOrderKeyV1 lower{
            input.trade_date,
            input.instrument_id,
            input.channel,
            std::numeric_limits<std::int64_t>::min()};
        const ShanghaiOrderKeyV1 upper{
            input.trade_date,
            input.instrument_id,
            input.channel,
            std::numeric_limits<std::int64_t>::max()};
        return {orders_.lower_bound(lower), orders_.upper_bound(upper)};
    }

    [[nodiscard]] ShanghaiOrderAggregatorConsumeErrorV1
    StatusOutputCount(
        const ShanghaiOrderEventInputV1& input,
        std::size_t* output) const noexcept {
        *output = 1U;
        if (!input.phase_valid ||
            input.phase != TradingPhaseV1::kEnd) {
            return ShanghaiOrderAggregatorConsumeErrorV1::kNone;
        }
        const auto [first, last] = InstrumentRange(input);
        for (auto position = first; position != last; ++position) {
            if (position->second.finalization_emitted) {
                continue;
            }
            if (*output == std::numeric_limits<std::size_t>::max()) {
                return ShanghaiOrderAggregatorConsumeErrorV1::
                    kNumericOverflow;
            }
            ++(*output);
        }
        return ShanghaiOrderAggregatorConsumeErrorV1::kNone;
    }

    [[nodiscard]] ShanghaiOrderAggregatorConsumeErrorV1 EnsureCapacity()
        const noexcept {
        return orders_.size() >= config_.maximum_order_states
                   ? ShanghaiOrderAggregatorConsumeErrorV1::kOrderCapacity
                   : ShanghaiOrderAggregatorConsumeErrorV1::kNone;
    }

    [[nodiscard]] OrderState* InsertState(
        const ShanghaiOrderKeyV1& key,
        SideV1 side,
        ShanghaiOrderSideSourceV1 side_source,
        const ShanghaiOrderEventInputV1& input) {
        OrderState initial{};
        InitializeState(key, side, side_source, input, &initial);
        const auto [position, inserted] =
            orders_.emplace(key, std::move(initial));
        return inserted ? &position->second : nullptr;
    }

    [[nodiscard]] ShanghaiOrderAggregatorConsumeErrorV1 EmitRevision(
        const ShanghaiOrderSourceAnchorV1& source_anchor,
        OrderState* state,
        std::vector<ShanghaiOrderEventV1>* output) {
        if (state->snapshot.revision ==
            std::numeric_limits<std::uint64_t>::max()) {
            return ShanghaiOrderAggregatorConsumeErrorV1::
                kNumericOverflow;
        }
        const bool first_revision =
            state->snapshot.revision == 0U;
        const bool first_finalization =
            state->terminal && !state->finalization_emitted;
        ++state->snapshot.revision;
        RefreshDerivedState(state);
        ShanghaiOrderRevisionEventV1 event{};
        event.operation =
            first_revision
                ? ShanghaiOrderDeltaOperationV1::kInsert
                : (first_finalization
                       ? ShanghaiOrderDeltaOperationV1::kFinalize
                       : ShanghaiOrderDeltaOperationV1::kUpdate);
        event.source_anchor = source_anchor;
        event.order = state->snapshot;
        output->emplace_back(std::move(event));
        if (first_finalization) {
            state->finalization_emitted = true;
        }
        return ShanghaiOrderAggregatorConsumeErrorV1::kNone;
    }

    [[nodiscard]] ShanghaiOrderAggregatorConsumeErrorV1 ConsumeAdd(
        const ShanghaiOrderEventInputV1& input,
        std::vector<ShanghaiOrderEventV1>* output) {
        const ShanghaiOrderKeyV1 key{
            input.trade_date,
            input.instrument_id,
            input.channel,
            input.primary_order_id};
        auto found = orders_.find(key);
        if (found == orders_.end()) {
            const ShanghaiOrderAggregatorConsumeErrorV1 capacity =
                EnsureCapacity();
            if (capacity !=
                ShanghaiOrderAggregatorConsumeErrorV1::kNone) {
                return capacity;
            }
        }

        std::int64_t continuous_original = 0;
        if (input.phase_valid &&
            input.phase == TradingPhaseV1::kContinuous &&
            input.matched_quantity_valid &&
            !CheckedAdd(
                input.quantity,
                input.matched_quantity,
                &continuous_original)) {
            return ShanghaiOrderAggregatorConsumeErrorV1::
                kNumericOverflow;
        }
        output->reserve(1U);
        if (found == orders_.end()) {
            OrderState* inserted = InsertState(
                key,
                input.side,
                ShanghaiOrderSideSourceV1::kSourceAddFlag,
                input);
            if (inserted == nullptr) {
                return ShanghaiOrderAggregatorConsumeErrorV1::kFailed;
            }
            found = orders_.find(key);
        }
        OrderState& state = found->second;
        if (state.snapshot.add_seen) {
            state.snapshot.quality_flags |=
                ShanghaiOrderQualityBitV1(
                    ShanghaiOrderQualityFlagV1::kDuplicateAdd);
            MarkMutation(input, &state);
            RefreshDerivedState(&state);
            return EmitRevision(input.anchor, &state, output);
        }

        std::int64_t continuous_observed_lower_bound = 0;
        if (input.phase_valid &&
            input.phase == TradingPhaseV1::kContinuous &&
            !input.matched_quantity_valid &&
            !CheckedAdd(
                input.quantity,
                state.pre_add_active_trade_quantity,
                &continuous_observed_lower_bound)) {
            return ShanghaiOrderAggregatorConsumeErrorV1::
                kNumericOverflow;
        }

        MarkSideConflict(input.side, &state);
        const bool was_terminal = state.terminal;
        if (was_terminal) {
            state.snapshot.quality_flags |=
                ShanghaiOrderQualityBitV1(
                    ShanghaiOrderQualityFlagV1::kQuantityConflict);
        }
        state.snapshot.quality_flags &=
            ~(ShanghaiOrderQualityBitV1(
                  ShanghaiOrderQualityFlagV1::kSyntheticOrder) |
              ShanghaiOrderQualityBitV1(
                  ShanghaiOrderQualityFlagV1::
                      kOriginalQuantityLowerBound) |
              ShanghaiOrderQualityBitV1(
                  ShanghaiOrderQualityFlagV1::
                      kExecutionBoundaryPrice));
        state.snapshot.side = input.side;
        state.snapshot.side_source =
            ShanghaiOrderSideSourceV1::kSourceAddFlag;
        state.snapshot.order_source =
            ShanghaiOrderSourceV1::kSourceAdd;
        state.snapshot.add_seen = true;
        state.snapshot.apply_to_book = true;
        state.snapshot.phase_at_add =
            input.phase_valid ? input.phase
                              : TradingPhaseV1::kUnknown;
        state.snapshot.add_anchor = input.anchor;
        state.snapshot.price_p6 = input.price_p6;
        state.snapshot.price_valid = true;
        state.snapshot.price_source =
            ShanghaiOrderPriceSourceV1::kSourceAdd;
        state.snapshot.published_quantity = input.quantity;
        state.snapshot.published_quantity_valid = true;
        state.snapshot.remaining_quantity = input.quantity;
        state.snapshot.remaining_quantity_valid = true;
        state.snapshot.source_matched_quantity =
            input.matched_quantity;
        state.snapshot.source_matched_quantity_valid =
            input.matched_quantity_valid;
        state.snapshot.observed_pre_add_trade_quantity =
            state.pre_add_active_trade_quantity;
        state.snapshot.post_add_trade_quantity = 0;

        if (input.phase_valid &&
            input.phase == TradingPhaseV1::kContinuous) {
            if (input.matched_quantity_valid) {
                state.snapshot.original_quantity =
                    continuous_original;
                state.snapshot.original_quantity_valid = true;
                state.snapshot.original_quantity_status =
                    ShanghaiOriginalQuantityStatusV1::kExact;
                if (input.matched_quantity !=
                    state.pre_add_active_trade_quantity) {
                    state.snapshot.quality_flags |=
                        ShanghaiOrderQualityBitV1(
                            ShanghaiOrderQualityFlagV1::
                                kPrematchQuantityMismatch);
                }
            } else {
                // The observed pre-A active fills and A.Qty are disjoint:
                // A.Qty is the residual after the first match. Even without
                // the vendor matched-quantity field, their checked sum is the
                // strongest quantity lower bound justified by the feed.
                state.snapshot.original_quantity =
                    continuous_observed_lower_bound;
                state.snapshot.original_quantity_valid = true;
                state.snapshot.original_quantity_status =
                    ShanghaiOriginalQuantityStatusV1::kLowerBound;
                state.snapshot.quality_flags |=
                    ShanghaiOrderQualityBitV1(
                        ShanghaiOrderQualityFlagV1::
                            kPrematchQuantityUnavailable) |
                    ShanghaiOrderQualityBitV1(
                        ShanghaiOrderQualityFlagV1::
                            kOriginalQuantityLowerBound);
            }
        } else if (
            input.phase_valid &&
            IsDirectOriginalQuantityPhase(input.phase)) {
            // 4.24 note 2: during opening/closing call and suspension,
            // published A.Qty is already the original order quantity.
            state.snapshot.original_quantity = input.quantity;
            state.snapshot.original_quantity_valid = true;
            state.snapshot.original_quantity_status =
                ShanghaiOriginalQuantityStatusV1::kExact;
        } else {
            state.snapshot.original_quantity = std::max(
                input.quantity,
                state.snapshot.total_trade_quantity);
            state.snapshot.original_quantity_valid = true;
            state.snapshot.original_quantity_status =
                ShanghaiOriginalQuantityStatusV1::kLowerBound;
            state.snapshot.quality_flags |=
                ShanghaiOrderQualityBitV1(
                    ShanghaiOrderQualityFlagV1::
                        kOriginalQuantityLowerBound);
            if (!input.phase_valid ||
                input.phase == TradingPhaseV1::kUnknown) {
                state.snapshot.quality_flags |=
                    ShanghaiOrderQualityBitV1(
                        ShanghaiOrderQualityFlagV1::kPhaseUnknown);
            } else {
                state.snapshot.quality_flags |=
                    ShanghaiOrderQualityBitV1(
                        ShanghaiOrderQualityFlagV1::
                            kUnexpectedPhase);
            }
        }
        if (!was_terminal) {
            state.terminal = false;
        }
        MarkMutation(input, &state);
        RefreshDerivedState(&state);
        return EmitRevision(input.anchor, &state, output);
    }

    [[nodiscard]] ShanghaiOrderAggregatorConsumeErrorV1 ConsumeTrade(
        const ShanghaiOrderEventInputV1& input,
        std::vector<ShanghaiOrderEventV1>* output) {
        struct Reference final {
            ShanghaiOrderKeyV1 key{};
            SideV1 side = SideV1::kUnknown;
            bool active_continuous = false;
            OrderState* state = nullptr;
        };
        const bool continuous =
            input.phase_valid &&
            input.phase == TradingPhaseV1::kContinuous;
        std::array<Reference, 2U> references{{
            {{input.trade_date,
              input.instrument_id,
              input.channel,
              input.buy_order_id},
             SideV1::kBuy,
             continuous &&
                 input.aggressor == AggressorV1::kBuy,
             nullptr},
            {{input.trade_date,
              input.instrument_id,
              input.channel,
              input.sell_order_id},
             SideV1::kSell,
             continuous &&
                 input.aggressor == AggressorV1::kSell,
             nullptr},
        }};

        std::size_t required_new_states = 0U;
        for (Reference& reference : references) {
            auto found = orders_.find(reference.key);
            if (found != orders_.end()) {
                reference.state = &found->second;
                if (!CanApplyTrade(
                        *reference.state,
                        input.quantity,
                        reference.active_continuous)) {
                    return ShanghaiOrderAggregatorConsumeErrorV1::
                        kNumericOverflow;
                }
            } else if (reference.active_continuous) {
                ++required_new_states;
            }
        }
        if (required_new_states >
            config_.maximum_order_states - orders_.size()) {
            return ShanghaiOrderAggregatorConsumeErrorV1::kOrderCapacity;
        }

        output->reserve(1U + references.size());
        ShanghaiTradeEventV1 trade{};
        trade.trade_date = input.trade_date;
        trade.instrument_id = input.instrument_id;
        trade.channel = input.channel;
        trade.source_anchor = input.anchor;
        trade.buy_order_id = input.buy_order_id;
        trade.sell_order_id = input.sell_order_id;
        trade.aggressor = input.aggressor;
        trade.phase =
            input.phase_valid ? input.phase
                              : TradingPhaseV1::kUnknown;
        trade.price_p6 = input.price_p6;
        trade.trade_amount_p6 = input.trade_amount_p6;
        trade.trade_amount_valid = input.trade_amount_valid;
        trade.quantity = input.quantity;
        trade.source_quality_flags =
            input.source_quality_flags;
        trade.source_market_notices =
            input.source_market_notices;
        output->emplace_back(std::move(trade));

        std::array<OrderState*, 2U> changed{};
        std::size_t changed_count = 0U;
        for (Reference& reference : references) {
            if (reference.state == nullptr &&
                reference.active_continuous) {
                reference.state = InsertState(
                    reference.key,
                    reference.side,
                    ShanghaiOrderSideSourceV1::
                        kSourceAggressorFlag,
                    input);
                if (reference.state == nullptr) {
                    return ShanghaiOrderAggregatorConsumeErrorV1::
                        kFailed;
                }
            }
            if (reference.state != nullptr) {
                ApplyTrade(
                    input,
                    reference.side,
                    reference.active_continuous,
                    reference.state);
                changed[changed_count] = reference.state;
                ++changed_count;
            }
        }
        if (changed_count == 2U &&
            changed[1U]->snapshot.key <
                changed[0U]->snapshot.key) {
            std::swap(changed[0U], changed[1U]);
        }
        for (std::size_t index = 0U;
             index < changed_count;
             ++index) {
            const ShanghaiOrderAggregatorConsumeErrorV1 error =
                EmitRevision(input.anchor, changed[index], output);
            if (error !=
                ShanghaiOrderAggregatorConsumeErrorV1::kNone) {
                return error;
            }
        }
        return ShanghaiOrderAggregatorConsumeErrorV1::kNone;
    }

    [[nodiscard]] ShanghaiOrderAggregatorConsumeErrorV1 ConsumeCancel(
        const ShanghaiOrderEventInputV1& input,
        std::vector<ShanghaiOrderEventV1>* output) {
        const ShanghaiOrderKeyV1 key{
            input.trade_date,
            input.instrument_id,
            input.channel,
            input.primary_order_id};
        auto found = orders_.find(key);
        if (found != orders_.end() &&
            !CanApplyCancel(found->second, input.quantity)) {
            return ShanghaiOrderAggregatorConsumeErrorV1::
                kNumericOverflow;
        }
        output->reserve(found == orders_.end() ? 1U : 2U);
        ShanghaiCancelEventV1 cancel{};
        cancel.key = key;
        cancel.source_anchor = input.anchor;
        cancel.side = input.side;
        cancel.phase =
            input.phase_valid ? input.phase
                              : TradingPhaseV1::kUnknown;
        cancel.quantity = input.quantity;
        cancel.referenced_order_found =
            found != orders_.end();
        cancel.source_quality_flags =
            input.source_quality_flags;
        cancel.source_market_notices =
            input.source_market_notices;
        output->emplace_back(std::move(cancel));
        if (found == orders_.end()) {
            return ShanghaiOrderAggregatorConsumeErrorV1::kNone;
        }
        ApplyCancel(input, &found->second);
        return EmitRevision(input.anchor, &found->second, output);
    }

    [[nodiscard]] ShanghaiOrderAggregatorConsumeErrorV1 ConsumeStatus(
        const ShanghaiOrderEventInputV1& input,
        std::vector<ShanghaiOrderEventV1>* output) {
        std::size_t output_count = 0U;
        const ShanghaiOrderAggregatorConsumeErrorV1 count_error =
            StatusOutputCount(input, &output_count);
        if (count_error !=
            ShanghaiOrderAggregatorConsumeErrorV1::kNone) {
            return count_error;
        }
        output->reserve(output_count);
        ShanghaiStatusEventV1 status{};
        status.trade_date = input.trade_date;
        status.instrument_id = input.instrument_id;
        status.channel = input.channel;
        status.source_anchor = input.anchor;
        status.phase =
            input.phase_valid ? input.phase
                              : TradingPhaseV1::kUnknown;
        if (!input.phase_valid ||
            input.phase == TradingPhaseV1::kUnknown) {
            status.quality_flags |=
                ShanghaiOrderQualityBitV1(
                    ShanghaiOrderQualityFlagV1::kPhaseUnknown);
        }
        status.source_quality_flags =
            input.source_quality_flags;
        status.source_market_notices =
            input.source_market_notices;
        output->emplace_back(std::move(status));
        if (!input.phase_valid ||
            input.phase != TradingPhaseV1::kEnd) {
            return ShanghaiOrderAggregatorConsumeErrorV1::kNone;
        }
        const auto [first, last] = InstrumentRange(input);
        for (auto position = first; position != last; ++position) {
            const ShanghaiOrderAggregatorConsumeErrorV1 error =
                FinalizeState(
                    input.anchor, &position->second, output);
            if (error !=
                ShanghaiOrderAggregatorConsumeErrorV1::kNone) {
                return error;
            }
        }
        return ShanghaiOrderAggregatorConsumeErrorV1::kNone;
    }

    [[nodiscard]] ShanghaiOrderAggregatorConsumeErrorV1 FinalizeState(
        const ShanghaiOrderSourceAnchorV1& source_anchor,
        OrderState* state,
        std::vector<ShanghaiOrderEventV1>* output) {
        if (state->finalization_emitted) {
            return ShanghaiOrderAggregatorConsumeErrorV1::kNone;
        }
        state->terminal = true;
        if (state->snapshot.add_seen &&
            state->snapshot.remaining_quantity_valid &&
            state->snapshot.remaining_quantity > 0) {
            state->snapshot.quality_flags |=
                ShanghaiOrderQualityBitV1(
                    ShanghaiOrderQualityFlagV1::
                        kEndedWithObservedBalance);
        }
        RefreshDerivedState(state);
        return EmitRevision(source_anchor, state, output);
    }

    ShanghaiOrderEventAggregatorConfigV1 config_{};
    OrderMap orders_;
    std::map<std::int32_t, std::int64_t>
        last_native_sequence_by_channel_;
    std::uint64_t last_ordering_sequence_ = 0U;
    bool finalized_ = false;
    bool failed_ = false;
};

std::string_view ShanghaiOrderAggregatorCreateErrorNameV1(
    ShanghaiOrderAggregatorCreateErrorV1 error) noexcept {
    switch (error) {
        case ShanghaiOrderAggregatorCreateErrorV1::kNone:
            return "none";
        case ShanghaiOrderAggregatorCreateErrorV1::kNullOutput:
            return "null_output";
        case ShanghaiOrderAggregatorCreateErrorV1::
            kInvalidConfiguration:
            return "invalid_configuration";
        case ShanghaiOrderAggregatorCreateErrorV1::
            kResourceExhausted:
            return "resource_exhausted";
    }
    return "unknown";
}

std::string_view ShanghaiOrderAggregatorConsumeErrorNameV1(
    ShanghaiOrderAggregatorConsumeErrorV1 error) noexcept {
    switch (error) {
        case ShanghaiOrderAggregatorConsumeErrorV1::kNone:
            return "none";
        case ShanghaiOrderAggregatorConsumeErrorV1::kNullOutput:
            return "null_output";
        case ShanghaiOrderAggregatorConsumeErrorV1::kInvalidInput:
            return "invalid_input";
        case ShanghaiOrderAggregatorConsumeErrorV1::kWrongTradeDate:
            return "wrong_trade_date";
        case ShanghaiOrderAggregatorConsumeErrorV1::kAlreadyFinalized:
            return "already_finalized";
        case ShanghaiOrderAggregatorConsumeErrorV1::kOrderCapacity:
            return "order_capacity";
        case ShanghaiOrderAggregatorConsumeErrorV1::kNumericOverflow:
            return "numeric_overflow";
        case ShanghaiOrderAggregatorConsumeErrorV1::kOutOfOrderInput:
            return "out_of_order_input";
        case ShanghaiOrderAggregatorConsumeErrorV1::kResourceExhausted:
            return "resource_exhausted";
        case ShanghaiOrderAggregatorConsumeErrorV1::kFailed:
            return "failed";
    }
    return "unknown";
}

std::string_view ShanghaiOrderAggregatorQueryErrorNameV1(
    ShanghaiOrderAggregatorQueryErrorV1 error) noexcept {
    switch (error) {
        case ShanghaiOrderAggregatorQueryErrorV1::kNone:
            return "none";
        case ShanghaiOrderAggregatorQueryErrorV1::kNullOutput:
            return "null_output";
        case ShanghaiOrderAggregatorQueryErrorV1::kInvalidKey:
            return "invalid_key";
        case ShanghaiOrderAggregatorQueryErrorV1::kNotFound:
            return "not_found";
    }
    return "unknown";
}

ShanghaiOrderEventAggregatorV1::ShanghaiOrderEventAggregatorV1(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

ShanghaiOrderEventAggregatorV1::~ShanghaiOrderEventAggregatorV1() =
    default;

ShanghaiOrderAggregatorCreateErrorV1
ShanghaiOrderEventAggregatorV1::Create(
    ShanghaiOrderEventAggregatorConfigV1 config,
    std::unique_ptr<ShanghaiOrderEventAggregatorV1>* output)
    noexcept {
    if (output == nullptr) {
        return ShanghaiOrderAggregatorCreateErrorV1::kNullOutput;
    }
    output->reset();
    if (!ValidTradeDate(config.trade_date) ||
        config.maximum_order_states == 0U) {
        return ShanghaiOrderAggregatorCreateErrorV1::
            kInvalidConfiguration;
    }
    try {
        auto impl = std::make_unique<Impl>(config);
        output->reset(new ShanghaiOrderEventAggregatorV1(
            std::move(impl)));
        return ShanghaiOrderAggregatorCreateErrorV1::kNone;
    } catch (const std::bad_alloc&) {
        return ShanghaiOrderAggregatorCreateErrorV1::
            kResourceExhausted;
    } catch (...) {
        return ShanghaiOrderAggregatorCreateErrorV1::
            kResourceExhausted;
    }
}

ShanghaiOrderAggregatorConsumeErrorV1
ShanghaiOrderEventAggregatorV1::Consume(
    const ShanghaiOrderEventInputV1& input,
    std::vector<ShanghaiOrderEventV1>* output) noexcept {
    return ConsumeCanonical(
        input, input.anchor.tick_stream_sequence, output);
}

ShanghaiOrderAggregatorConsumeErrorV1
ShanghaiOrderEventAggregatorV1::ConsumeCanonical(
    const ShanghaiOrderEventInputV1& input,
    std::uint64_t canonical_apply_sequence,
    std::vector<ShanghaiOrderEventV1>* output) noexcept {
    if (impl_ == nullptr) {
        if (output != nullptr) {
            output->clear();
        }
        return ShanghaiOrderAggregatorConsumeErrorV1::kFailed;
    }
    try {
        return impl_->Consume(
            input, canonical_apply_sequence, output);
    } catch (const std::bad_alloc&) {
        if (output != nullptr) {
            output->clear();
        }
        impl_->MarkFailed();
        return ShanghaiOrderAggregatorConsumeErrorV1::
            kResourceExhausted;
    } catch (...) {
        if (output != nullptr) {
            output->clear();
        }
        impl_->MarkFailed();
        return ShanghaiOrderAggregatorConsumeErrorV1::kFailed;
    }
}

ShanghaiOrderAggregatorConsumeErrorV1
ShanghaiOrderEventAggregatorV1::MaximumOutputForInput(
    const ShanghaiOrderEventInputV1& input,
    std::size_t* output) const noexcept {
    if (impl_ == nullptr) {
        if (output != nullptr) {
            *output = 0U;
        }
        return ShanghaiOrderAggregatorConsumeErrorV1::kFailed;
    }
    return impl_->MaximumOutputForInput(input, output);
}

ShanghaiOrderAggregatorConsumeErrorV1
ShanghaiOrderEventAggregatorV1::ConsumeDecoded(
    const DecodedMarketEventV1& event,
    std::uint64_t ingress_sequence,
    std::uint64_t tick_stream_sequence,
    std::vector<ShanghaiOrderEventV1>* output) noexcept {
    if (output == nullptr) {
        return ShanghaiOrderAggregatorConsumeErrorV1::kNullOutput;
    }
    output->clear();
    ShanghaiOrderEventInputV1 projected{};
    if (ProjectShanghaiOrderEventInputV1(
            event,
            ingress_sequence,
            tick_stream_sequence,
            &projected) !=
        ShanghaiOrderEventProjectionV1::kProjected) {
        return ShanghaiOrderAggregatorConsumeErrorV1::kInvalidInput;
    }
    return Consume(projected, output);
}

ShanghaiOrderAggregatorConsumeErrorV1
ShanghaiOrderEventAggregatorV1::Finalize(
    const ShanghaiOrderSourceAnchorV1& source_anchor,
    std::vector<ShanghaiOrderEventV1>* output) noexcept {
    if (impl_ == nullptr) {
        if (output != nullptr) {
            output->clear();
        }
        return ShanghaiOrderAggregatorConsumeErrorV1::kFailed;
    }
    try {
        return impl_->Finalize(source_anchor, output);
    } catch (const std::bad_alloc&) {
        if (output != nullptr) {
            output->clear();
        }
        impl_->MarkFailed();
        return ShanghaiOrderAggregatorConsumeErrorV1::
            kResourceExhausted;
    } catch (...) {
        if (output != nullptr) {
            output->clear();
        }
        impl_->MarkFailed();
        return ShanghaiOrderAggregatorConsumeErrorV1::kFailed;
    }
}

ShanghaiOrderAggregatorQueryErrorV1
ShanghaiOrderEventAggregatorV1::GetOrder(
    const ShanghaiOrderKeyV1& key,
    ShanghaiOrderSnapshotV1* output) const noexcept {
    if (impl_ == nullptr) {
        if (output != nullptr) {
            *output = {};
        }
        return ShanghaiOrderAggregatorQueryErrorV1::kNotFound;
    }
    return impl_->GetOrder(key, output);
}

std::size_t ShanghaiOrderEventAggregatorV1::order_count()
    const noexcept {
    return impl_ == nullptr ? 0U : impl_->order_count();
}

bool ShanghaiOrderEventAggregatorV1::finalized() const noexcept {
    return impl_ != nullptr && impl_->finalized();
}

const ShanghaiOrderEventAggregatorConfigV1&
ShanghaiOrderEventAggregatorV1::config() const noexcept {
    return impl_->config();
}

}  // namespace l2flow::market
