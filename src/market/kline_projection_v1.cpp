#include "l2flow/market/kline_projection_v1.h"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <type_traits>
#include <variant>

namespace l2flow::market {
namespace {

[[nodiscard]] KLineTradeProjectionV1 ProjectTickFields(
    const DecodedMarketCommonV1& common,
    const TickFieldsV1& fields,
    std::int32_t channel,
    std::uint64_t native_event_sequence,
    std::uint64_t arrival_id,
    KLineTradeV1* output) noexcept {
    if (fields.action != TickActionV1::kTrade) {
        return KLineTradeProjectionV1::kNotTrade;
    }
    constexpr std::uint32_t required_validity =
        kTickPriceValidV1 | kTickQuantityValidV1 |
        kTickExchangeTimeValidV1;
    if (output == nullptr ||
        (fields.validity_bitmap & required_validity) !=
            required_validity ||
        !fields.price.valid || !fields.quantity.valid ||
        fields.price.normalized_p6 <= 0 || fields.quantity.raw <= 0 ||
        !common.exchange_time.valid ||
        !common.exchange_time.unix_nanoseconds_valid ||
        common.exchange_time.nanoseconds_since_midnight >=
            kKLineNanosecondsPerDayV1 ||
        common.origin.trade_date == 0U || channel <= 0 ||
        native_event_sequence == 0U ||
        common.origin.source_sequence == 0U || arrival_id == 0U ||
        common.instrument_id == 0U ||
        common.ordinal == std::numeric_limits<std::size_t>::max()) {
        return KLineTradeProjectionV1::kInvalidTrade;
    }
    KLineTradeV1 projected{};
    projected.trade_date = common.origin.trade_date;
    projected.instrument_id = common.instrument_id;
    projected.ordinal = common.ordinal;
    projected.event_time_ns_since_midnight =
        common.exchange_time.nanoseconds_since_midnight;
    projected.event_time_unix_ns =
        common.exchange_time.unix_nanoseconds;
    projected.price_p6 = fields.price.normalized_p6;
    projected.quantity_raw =
        static_cast<std::uint64_t>(fields.quantity.raw);
    projected.quantity_scale = fields.quantity.scale;
    projected.quantity_unit = common.quantity_unit;
    projected.event_sequence = native_event_sequence;
    projected.source_sequence = common.origin.source_sequence;
    projected.ingress_sequence = arrival_id;
    projected.channel = channel;
    *output = projected;
    return KLineTradeProjectionV1::kTrade;
}

}  // namespace

KLineTradeProjectionV1 ProjectKLineTradeV1(
    const DecodedMarketEventV1& event,
    std::uint64_t arrival_id,
    KLineTradeV1* output) noexcept {
    if (output != nullptr) {
        *output = KLineTradeV1{};
    }
    return std::visit(
        [arrival_id, output](const auto& value) noexcept {
            using Event = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<Event, ShanghaiTickV1>) {
                if (value.common.market != MarketV1::kShanghai ||
                    value.common.kind !=
                        MarketEventKindV1::kShanghaiTick) {
                    return KLineTradeProjectionV1::kInvalidTrade;
                }
                const std::uint64_t native_sequence =
                    value.business_index > 0
                        ? static_cast<std::uint64_t>(
                              value.business_index)
                        : 0U;
                return ProjectTickFields(
                    value.common,
                    value.fields,
                    value.channel,
                    native_sequence,
                    arrival_id,
                    output);
            } else if constexpr (
                std::is_same_v<Event, ShenzhenTransactionV1>) {
                if (value.common.market != MarketV1::kShenzhen ||
                    value.common.kind !=
                        MarketEventKindV1::kShenzhenTransaction) {
                    return KLineTradeProjectionV1::kInvalidTrade;
                }
                const std::uint64_t native_sequence =
                    value.application_sequence > 0
                        ? static_cast<std::uint64_t>(
                              value.application_sequence)
                        : 0U;
                const std::int32_t channel =
                    value.channel <= static_cast<std::uint32_t>(
                                         std::numeric_limits<
                                             std::int32_t>::max())
                        ? static_cast<std::int32_t>(value.channel)
                        : 0;
                return ProjectTickFields(
                    value.common,
                    value.fields,
                    channel,
                    native_sequence,
                    arrival_id,
                    output);
            } else {
                return KLineTradeProjectionV1::kNotTrade;
            }
        },
        event);
}

}  // namespace l2flow::market
