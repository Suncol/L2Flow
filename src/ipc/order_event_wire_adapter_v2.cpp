#include "l2flow/ipc/order_event_wire_adapter_v2.h"

#include <algorithm>
#include <array>
#include <cstdint>
#include <limits>

namespace l2flow::ipc {
namespace {

constexpr std::uint64_t kNanosecondsPerSecond = 1'000'000'000ULL;
constexpr std::uint64_t kNanosecondsPerMillisecond = 1'000'000ULL;
constexpr std::uint64_t kNanosecondsPerDay =
    86'400ULL * kNanosecondsPerSecond;
constexpr std::uint32_t kKnownTickValidityBits =
    market::kTickPriceValidV1 |
    market::kTickQuantityValidV1 |
    market::kTickTradeAmountValidV1 |
    market::kTickMatchedQuantityValidV1 |
    market::kTickPrimaryOrderIdValidV1 |
    market::kTickBuyOrderIdValidV1 |
    market::kTickSellOrderIdValidV1 |
    market::kTickExchangeTimeValidV1 |
    market::kTickSideValidV1 |
    market::kTickOrderTypeValidV1 |
    market::kTickAggressorValidV1 |
    market::kTickPhaseValidV1;

template <typename Value, std::size_t Size>
[[nodiscard]] bool AllZero(
    const std::array<Value, Size>& values) noexcept {
    return std::all_of(
        values.begin(), values.end(), [](Value value) noexcept {
            return value == Value{};
        });
}

[[nodiscard]] bool ValidTradeDate(
    std::uint32_t value) noexcept {
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

[[nodiscard]] bool DaysSinceUnixEpoch(
    std::uint32_t trade_date,
    std::int64_t* output) noexcept {
    if (output == nullptr || !ValidTradeDate(trade_date)) {
        return false;
    }
    const std::int64_t year =
        static_cast<std::int64_t>(trade_date / 10'000U);
    const std::uint32_t month =
        (trade_date / 100U) % 100U;
    const std::uint32_t day = trade_date % 100U;
    const std::int64_t adjusted_year =
        year - (month <= 2U ? 1 : 0);
    const std::int64_t era =
        (adjusted_year >= 0 ? adjusted_year
                            : adjusted_year - 399) /
        400;
    const std::int64_t year_of_era =
        adjusted_year - era * 400;
    const std::int64_t adjusted_month =
        static_cast<std::int64_t>(month) +
        (month > 2U ? -3 : 9);
    const std::int64_t day_of_year =
        (153 * adjusted_month + 2) / 5 +
        static_cast<std::int64_t>(day) - 1;
    const std::int64_t day_of_era =
        year_of_era * 365 + year_of_era / 4 -
        year_of_era / 100 + day_of_year;
    *output =
        era * 146'097 + day_of_era - 719'468;
    return true;
}

[[nodiscard]] bool EventTimeEncodingValid(
    const RealtimeWireTickPayloadV2& payload) noexcept {
    const bool valid =
        (payload.validity_bitmap &
         market::kTickExchangeTimeValidV1) != 0U;
    const RealtimeWireCommonRecordV2& common = payload.common;
    if (!valid) {
        return common.exchange_time_ns_since_midnight == 0U &&
               common.event_time_unix_ns == 0;
    }
    if (common.exchange_time_ns_since_midnight >=
        kNanosecondsPerDay) {
        return false;
    }
    std::int64_t days = 0;
    if (!DaysSinceUnixEpoch(common.trade_date, &days)) {
        return false;
    }
    constexpr std::int64_t kSecondsPerDay = 86'400;
    constexpr std::int64_t kUtcPlusEightSeconds = 8 * 3'600;
    const std::int64_t seconds =
        days * kSecondsPerDay - kUtcPlusEightSeconds;
    const std::int64_t expected =
        seconds * static_cast<std::int64_t>(
                      kNanosecondsPerSecond) +
        static_cast<std::int64_t>(
            common.exchange_time_ns_since_midnight);
    return common.event_time_unix_ns == expected;
}

struct LocalTimeProjection final {
    std::uint64_t nanoseconds_since_midnight = 0U;
    bool valid = false;
};

[[nodiscard]] LocalTimeProjection ProjectLocalTime(
    std::uint32_t raw) noexcept {
    LocalTimeProjection result{};
    if (raw == std::numeric_limits<std::uint32_t>::max()) {
        return result;
    }
    const std::uint32_t hour = raw / 10'000'000U;
    const std::uint32_t minute = (raw / 100'000U) % 100U;
    const std::uint32_t second = (raw / 1'000U) % 100U;
    const std::uint32_t millisecond = raw % 1'000U;
    if (hour >= 24U || minute >= 60U || second >= 60U ||
        millisecond >= 1'000U) {
        return result;
    }
    const std::uint64_t seconds_since_midnight =
        static_cast<std::uint64_t>(hour) * 3'600U +
        static_cast<std::uint64_t>(minute) * 60U + second;
    result.nanoseconds_since_midnight =
        seconds_since_midnight * kNanosecondsPerSecond +
        static_cast<std::uint64_t>(millisecond) *
            kNanosecondsPerMillisecond;
    result.valid = true;
    return result;
}

[[nodiscard]] bool DecimalEncodingValid(
    const RealtimeWireDecimalV2& value,
    std::uint8_t required_scale) noexcept {
    if (value.scale != required_scale || value.valid > 1U ||
        value.is_null > 1U || !AllZero(value.reserved) ||
        (value.valid != 0U && value.is_null != 0U)) {
        return false;
    }
    if (value.valid == 0U) {
        return value.normalized_p6 == 0;
    }
    if (required_scale > 6U) {
        return false;
    }
    constexpr std::array<std::int64_t, 7U> kPowersOfTen{
        1LL, 10LL, 100LL, 1'000LL,
        10'000LL, 100'000LL, 1'000'000LL};
    const std::int64_t multiplier =
        kPowersOfTen[6U - required_scale];
    if ((value.raw > 0 &&
         value.raw >
             std::numeric_limits<std::int64_t>::max() /
                 multiplier) ||
        (value.raw < 0 &&
         value.raw <
             std::numeric_limits<std::int64_t>::min() /
                 multiplier)) {
        return false;
    }
    return value.normalized_p6 == value.raw * multiplier;
}

[[nodiscard]] bool QuantityEncodingValid(
    const RealtimeWireQuantityV2& value,
    std::uint8_t required_scale) noexcept {
    return value.scale == required_scale &&
           value.valid <= 1U && value.is_null <= 1U &&
           !(value.valid != 0U && value.is_null != 0U) &&
           AllZero(value.reserved);
}

[[nodiscard]] bool RawProjectionEncodingValid(
    const RealtimeWireTickPayloadV2& payload) noexcept {
    constexpr std::uint32_t kKnownProjectionFlags =
        kRealtimeWireTickRawTypeOmittedV2 |
        kRealtimeWireTickRawTickFlagOmittedV2;
    if ((payload.projection_flags & ~kKnownProjectionFlags) != 0U ||
        payload.raw_type_length > payload.raw_type.size() ||
        payload.raw_tick_flag_length >
            payload.raw_tick_flag.size() ||
        payload.reserved0 != 0U) {
        return false;
    }
    const bool raw_type_omitted =
        (payload.projection_flags &
         kRealtimeWireTickRawTypeOmittedV2) != 0U;
    const bool raw_tick_flag_omitted =
        (payload.projection_flags &
         kRealtimeWireTickRawTickFlagOmittedV2) != 0U;
    if ((raw_type_omitted &&
         (payload.raw_type_length != 0U ||
          !AllZero(payload.raw_type))) ||
        (raw_tick_flag_omitted &&
         (payload.raw_tick_flag_length != 0U ||
          !AllZero(payload.raw_tick_flag)))) {
        return false;
    }
    return std::all_of(
               payload.raw_type.begin() +
                   payload.raw_type_length,
               payload.raw_type.end(),
               [](std::uint8_t value) noexcept {
                   return value == 0U;
               }) &&
           std::all_of(
               payload.raw_tick_flag.begin() +
                   payload.raw_tick_flag_length,
               payload.raw_tick_flag.end(),
               [](std::uint8_t value) noexcept {
                   return value == 0U;
               });
}

[[nodiscard]] bool CommonEnvelopeValid(
    const RealtimeWireTickPayloadV2& payload) noexcept {
    const RealtimeWireCommonRecordV2& common = payload.common;
    if (common.record_schema_version != 2U ||
        common.record_bytes != sizeof(RealtimeWireTickPayloadV2) ||
        common.instrument_id == 0U ||
        common.ordinal ==
            std::numeric_limits<std::uint32_t>::max() ||
        common.instrument_id != common.ordinal + 1U ||
        common.source_sequence == 0U ||
        common.source_sequence ==
            std::numeric_limits<std::uint64_t>::max() ||
        common.ingress_sequence == 0U ||
        common.ingress_sequence ==
            std::numeric_limits<std::uint64_t>::max() ||
        common.tick_stream_sequence == 0U ||
        common.tick_stream_sequence ==
            std::numeric_limits<std::uint64_t>::max() ||
        common.tick_stream_sequence > common.ingress_sequence ||
        common.source_stream_id == 0U ||
        !ValidTradeDate(common.trade_date) ||
        common.reserved0 != 0U || !AllZero(common.reserved) ||
        common.quantity_unit >
            static_cast<std::uint8_t>(
                market::QuantityUnitV1::kIndexUnit) ||
        common.security_type >
            static_cast<std::uint8_t>(
                market::SecurityTypeV1::kOption) ||
        common.asset_scope >
            static_cast<std::uint8_t>(
                market::AssetScopeV1::kOutsideDocumentedCore)) {
        return false;
    }
    switch (static_cast<market::MarketEventKindV1>(
        common.event_kind)) {
        case market::MarketEventKindV1::kShanghaiTick:
            return common.source_slot == 1U &&
                   common.market ==
                       static_cast<std::uint8_t>(
                           market::MarketV1::kShanghai);
        case market::MarketEventKindV1::kShenzhenOrder:
        case market::MarketEventKindV1::kShenzhenTransaction:
            return common.source_slot == 3U &&
                   common.market ==
                       static_cast<std::uint8_t>(
                           market::MarketV1::kShenzhen);
        case market::MarketEventKindV1::kShanghaiSnapshot:
        case market::MarketEventKindV1::kShenzhenSnapshot:
            return false;
    }
    return false;
}

[[nodiscard]] bool EnumAndNumericEncodingValid(
    const RealtimeWireTickPayloadV2& payload,
    std::uint8_t price_scale,
    std::uint8_t trade_amount_scale) noexcept {
    if ((payload.validity_bitmap & ~kKnownTickValidityBits) != 0U ||
        payload.action >
            static_cast<std::uint8_t>(
                market::TickActionV1::kStatus) ||
        payload.side >
            static_cast<std::uint8_t>(market::SideV1::kLend) ||
        payload.order_type >
            static_cast<std::uint8_t>(
                market::OrderTypeV1::kSameSideBest) ||
        payload.aggressor >
            static_cast<std::uint8_t>(
                market::AggressorV1::kNeutral) ||
        payload.phase >
            static_cast<std::uint8_t>(
                market::TradingPhaseV1::kEnd) ||
        !DecimalEncodingValid(payload.price, price_scale) ||
        !QuantityEncodingValid(payload.quantity, 0U) ||
        !DecimalEncodingValid(
            payload.trade_amount, trade_amount_scale) ||
        !QuantityEncodingValid(payload.matched_quantity, 0U) ||
        payload.quantity.is_null != 0U ||
        payload.matched_quantity.is_null != 0U ||
        !RawProjectionEncodingValid(payload) ||
        !EventTimeEncodingValid(payload)) {
        return false;
    }

    const auto bit_matches = [&](std::uint32_t bit,
                                 bool valid) noexcept {
        return ((payload.validity_bitmap & bit) != 0U) == valid;
    };
    return bit_matches(
               market::kTickPriceValidV1,
               payload.price.valid != 0U) &&
           bit_matches(
               market::kTickQuantityValidV1,
               payload.quantity.valid != 0U) &&
           bit_matches(
               market::kTickTradeAmountValidV1,
               payload.trade_amount.valid != 0U) &&
           bit_matches(
               market::kTickMatchedQuantityValidV1,
               payload.matched_quantity.valid != 0U) &&
           bit_matches(
               market::kTickSideValidV1,
               payload.side !=
                   static_cast<std::uint8_t>(
                       market::SideV1::kUnknown)) &&
           bit_matches(
               market::kTickOrderTypeValidV1,
               payload.order_type !=
                   static_cast<std::uint8_t>(
                       market::OrderTypeV1::kUnknown)) &&
           bit_matches(
               market::kTickAggressorValidV1,
               payload.aggressor !=
                   static_cast<std::uint8_t>(
                       market::AggressorV1::kUnknown)) &&
           bit_matches(
               market::kTickPhaseValidV1,
               payload.phase !=
                   static_cast<std::uint8_t>(
                       market::TradingPhaseV1::kUnknown));
}

[[nodiscard]] bool RawBytesEqual(
    const std::array<std::uint8_t, 32U>& bytes,
    std::uint8_t length,
    std::string_view expected) noexcept {
    if (length != expected.size()) {
        return false;
    }
    return std::equal(
        expected.begin(),
        expected.end(),
        bytes.begin(),
        [](char expected_value,
           std::uint8_t actual_value) noexcept {
            return static_cast<std::uint8_t>(
                       static_cast<unsigned char>(
                           expected_value)) == actual_value;
        });
}

[[nodiscard]] std::string_view ShanghaiRawType(
    market::TickActionV1 action) noexcept {
    switch (action) {
        case market::TickActionV1::kAdd:
            return "A";
        case market::TickActionV1::kCancel:
            return "D";
        case market::TickActionV1::kTrade:
            return "T";
        case market::TickActionV1::kStatus:
            return "S";
        case market::TickActionV1::kUnknown:
            return {};
    }
    return {};
}

[[nodiscard]] bool ShanghaiStatusFlagConsistent(
    const RealtimeWireTickPayloadV2& payload,
    market::TradingPhaseV1 phase) noexcept {
    std::string_view expected;
    switch (phase) {
        case market::TradingPhaseV1::kStart:
            expected = "START";
            break;
        case market::TradingPhaseV1::kOpeningCall:
            expected = "OCALL";
            break;
        case market::TradingPhaseV1::kContinuous:
            expected = "TRADE";
            break;
        case market::TradingPhaseV1::kSuspended:
            expected = "SUSP";
            break;
        case market::TradingPhaseV1::kClosingCall:
            expected = "CCALL";
            break;
        case market::TradingPhaseV1::kClosed:
            expected = "CLOSE";
            break;
        case market::TradingPhaseV1::kEnd:
            expected = "ENDTR";
            break;
        case market::TradingPhaseV1::kUnknown:
            return !RawBytesEqual(
                       payload.raw_tick_flag,
                       payload.raw_tick_flag_length,
                       "START") &&
                   !RawBytesEqual(
                       payload.raw_tick_flag,
                       payload.raw_tick_flag_length,
                       "OCALL") &&
                   !RawBytesEqual(
                       payload.raw_tick_flag,
                       payload.raw_tick_flag_length,
                       "TRADE") &&
                   !RawBytesEqual(
                       payload.raw_tick_flag,
                       payload.raw_tick_flag_length,
                       "SUSP") &&
                   !RawBytesEqual(
                       payload.raw_tick_flag,
                       payload.raw_tick_flag_length,
                       "CCALL") &&
                   !RawBytesEqual(
                       payload.raw_tick_flag,
                       payload.raw_tick_flag_length,
                       "CLOSE") &&
                   !RawBytesEqual(
                       payload.raw_tick_flag,
                       payload.raw_tick_flag_length,
                       "ENDTR");
    }
    return RawBytesEqual(
        payload.raw_tick_flag,
        payload.raw_tick_flag_length,
        expected);
}

[[nodiscard]] bool ShanghaiEventContractValid(
    const RealtimeWireTickPayloadV2& payload) noexcept {
    if (payload.channel <= 0 ||
        payload.channel >
            std::numeric_limits<std::int32_t>::max() ||
        payload.native_event_sequence <= 0 ||
        payload.source_raw_code_1 != 0 ||
        payload.source_raw_code_2 != 0 ||
        payload.projection_flags != 0U) {
        return false;
    }
    const auto action =
        static_cast<market::TickActionV1>(payload.action);
    const auto side = static_cast<market::SideV1>(payload.side);
    const auto order_type =
        static_cast<market::OrderTypeV1>(payload.order_type);
    const auto aggressor =
        static_cast<market::AggressorV1>(payload.aggressor);
    const auto phase =
        static_cast<market::TradingPhaseV1>(payload.phase);
    if (action == market::TickActionV1::kUnknown ||
        order_type != market::OrderTypeV1::kUnknown ||
        !RawBytesEqual(
            payload.raw_type,
            payload.raw_type_length,
            ShanghaiRawType(action))) {
        return false;
    }
    const auto has = [&](std::uint32_t bit) noexcept {
        return (payload.validity_bitmap & bit) != 0U;
    };
    const bool matched_quantity_zero =
        payload.matched_quantity.raw == 0 &&
        payload.matched_quantity.valid == 0U &&
        payload.matched_quantity.is_null == 0U;

    switch (action) {
        case market::TickActionV1::kAdd:
        case market::TickActionV1::kCancel: {
            const bool add =
                action == market::TickActionV1::kAdd;
            if ((side != market::SideV1::kBuy &&
                 side != market::SideV1::kSell) ||
                payload.primary_order_id <= 0 ||
                payload.primary_order_id !=
                    (side == market::SideV1::kBuy
                         ? payload.buy_order_id
                         : payload.sell_order_id) ||
                !has(market::kTickPrimaryOrderIdValidV1) ||
                has(market::kTickBuyOrderIdValidV1) ||
                has(market::kTickSellOrderIdValidV1) ||
                aggressor != market::AggressorV1::kUnknown ||
                !RawBytesEqual(
                    payload.raw_tick_flag,
                    payload.raw_tick_flag_length,
                    side == market::SideV1::kBuy ? "B" : "S") ||
                !has(market::kTickQuantityValidV1) ||
                payload.quantity.raw <= 0 ||
                has(market::kTickTradeAmountValidV1)) {
                return false;
            }
            if (add) {
                const bool source_matched_valid =
                    payload.trade_amount.is_null == 0U &&
                    payload.trade_amount.raw >= 0 &&
                    payload.trade_amount.raw % 1'000 == 0;
                return has(market::kTickPriceValidV1) &&
                       payload.price.normalized_p6 > 0 &&
                       has(
                           market::
                               kTickMatchedQuantityValidV1) ==
                           source_matched_valid &&
                       (source_matched_valid
                            ? payload.matched_quantity.raw ==
                                  payload.trade_amount.raw /
                                      1'000
                            : matched_quantity_zero);
            }
            return !has(market::kTickPriceValidV1) &&
                   !has(
                       market::kTickMatchedQuantityValidV1) &&
                   matched_quantity_zero;
        }
        case market::TickActionV1::kTrade: {
            bool raw_flag_valid = false;
            if (aggressor == market::AggressorV1::kBuy) {
                raw_flag_valid = RawBytesEqual(
                    payload.raw_tick_flag,
                    payload.raw_tick_flag_length,
                    "B");
            } else if (
                aggressor == market::AggressorV1::kSell) {
                raw_flag_valid = RawBytesEqual(
                    payload.raw_tick_flag,
                    payload.raw_tick_flag_length,
                    "S");
            } else if (
                aggressor == market::AggressorV1::kNeutral) {
                raw_flag_valid = RawBytesEqual(
                    payload.raw_tick_flag,
                    payload.raw_tick_flag_length,
                    "N");
            } else {
                raw_flag_valid =
                    !RawBytesEqual(
                        payload.raw_tick_flag,
                        payload.raw_tick_flag_length,
                        "B") &&
                    !RawBytesEqual(
                        payload.raw_tick_flag,
                        payload.raw_tick_flag_length,
                        "S") &&
                    !RawBytesEqual(
                        payload.raw_tick_flag,
                        payload.raw_tick_flag_length,
                        "N");
            }
            return raw_flag_valid &&
                   side == market::SideV1::kUnknown &&
                   payload.primary_order_id == 0 &&
                   !has(market::kTickPrimaryOrderIdValidV1) &&
                   payload.buy_order_id > 0 &&
                   payload.sell_order_id > 0 &&
                   payload.buy_order_id !=
                       payload.sell_order_id &&
                   has(market::kTickBuyOrderIdValidV1) &&
                   has(market::kTickSellOrderIdValidV1) &&
                   has(market::kTickPriceValidV1) &&
                   payload.price.normalized_p6 > 0 &&
                   has(market::kTickQuantityValidV1) &&
                   payload.quantity.raw > 0 &&
                   !has(
                       market::kTickMatchedQuantityValidV1) &&
                   matched_quantity_zero;
        }
        case market::TickActionV1::kStatus:
            return side == market::SideV1::kUnknown &&
                   aggressor == market::AggressorV1::kUnknown &&
                   payload.primary_order_id == 0 &&
                   !has(market::kTickPrimaryOrderIdValidV1) &&
                   !has(market::kTickBuyOrderIdValidV1) &&
                   !has(market::kTickSellOrderIdValidV1) &&
                   !has(market::kTickPriceValidV1) &&
                   !has(market::kTickQuantityValidV1) &&
                   !has(market::kTickTradeAmountValidV1) &&
                   !has(
                       market::kTickMatchedQuantityValidV1) &&
                   matched_quantity_zero &&
                   ShanghaiStatusFlagConsistent(payload, phase);
        case market::TickActionV1::kUnknown:
            return false;
    }
    return false;
}

[[nodiscard]] market::SideV1 ShenzhenSideFromRaw(
    std::int32_t raw) noexcept {
    switch (raw) {
        case 49:
            return market::SideV1::kBuy;
        case 50:
            return market::SideV1::kSell;
        case 70:
            return market::SideV1::kLend;
        case 71:
            return market::SideV1::kBorrow;
        default:
            return market::SideV1::kUnknown;
    }
}

[[nodiscard]] market::OrderTypeV1 ShenzhenOrderTypeFromRaw(
    std::int32_t raw) noexcept {
    switch (raw) {
        case 49:
            return market::OrderTypeV1::kMarket;
        case 50:
            return market::OrderTypeV1::kLimit;
        case 85:
            return market::OrderTypeV1::kSameSideBest;
        default:
            return market::OrderTypeV1::kUnknown;
    }
}

[[nodiscard]] bool ShenzhenEventContractValid(
    const RealtimeWireTickPayloadV2& payload) noexcept {
    if (payload.channel < 0 ||
        payload.channel >
            std::numeric_limits<std::uint32_t>::max() ||
        payload.native_event_sequence <= 0 ||
        payload.projection_flags != 0U ||
        payload.raw_type_length != 0U ||
        payload.raw_tick_flag_length != 0U ||
        !AllZero(payload.raw_type) ||
        !AllZero(payload.raw_tick_flag) ||
        payload.phase !=
            static_cast<std::uint8_t>(
                market::TradingPhaseV1::kUnknown) ||
        payload.aggressor !=
            static_cast<std::uint8_t>(
                market::AggressorV1::kUnknown)) {
        return false;
    }
    const auto action =
        static_cast<market::TickActionV1>(payload.action);
    const auto side = static_cast<market::SideV1>(payload.side);
    const auto order_type =
        static_cast<market::OrderTypeV1>(payload.order_type);
    const auto has = [&](std::uint32_t bit) noexcept {
        return (payload.validity_bitmap & bit) != 0U;
    };
    if (!has(market::kTickQuantityValidV1) ||
        payload.quantity.raw <= 0 ||
        has(market::kTickTradeAmountValidV1) ||
        has(market::kTickMatchedQuantityValidV1) ||
        payload.trade_amount.raw != 0 ||
        payload.trade_amount.normalized_p6 != 0 ||
        payload.trade_amount.valid != 0U ||
        payload.trade_amount.is_null != 0U ||
        payload.matched_quantity.raw != 0 ||
        payload.matched_quantity.valid != 0U ||
        payload.matched_quantity.is_null != 0U ||
        has(market::kTickAggressorValidV1) ||
        has(market::kTickPhaseValidV1)) {
        return false;
    }

    const auto kind =
        static_cast<market::MarketEventKindV1>(
            payload.common.event_kind);
    if (kind == market::MarketEventKindV1::kShenzhenOrder) {
        if (action != market::TickActionV1::kAdd ||
            side != ShenzhenSideFromRaw(
                        payload.source_raw_code_1) ||
            side == market::SideV1::kUnknown ||
            order_type != ShenzhenOrderTypeFromRaw(
                              payload.source_raw_code_2) ||
            order_type == market::OrderTypeV1::kUnknown ||
            payload.primary_order_id <= 0 ||
            payload.primary_order_id !=
                payload.native_event_sequence ||
            payload.buy_order_id != 0 ||
            payload.sell_order_id != 0 ||
            !has(market::kTickPrimaryOrderIdValidV1) ||
            has(market::kTickBuyOrderIdValidV1) ||
            has(market::kTickSellOrderIdValidV1)) {
            return false;
        }
        if (order_type == market::OrderTypeV1::kLimit) {
            return has(market::kTickPriceValidV1) &&
                   payload.price.normalized_p6 > 0;
        }
        return !has(market::kTickPriceValidV1) &&
               payload.price.normalized_p6 == 0;
    }

    if (kind !=
        market::MarketEventKindV1::kShenzhenTransaction) {
        return false;
    }
    if (payload.source_raw_code_2 != 0) {
        return false;
    }
    if (payload.source_raw_code_1 == 70) {
        const bool has_buy = payload.buy_order_id > 0;
        const bool has_sell = payload.sell_order_id > 0;
        return action == market::TickActionV1::kTrade &&
               side == market::SideV1::kUnknown &&
               order_type == market::OrderTypeV1::kUnknown &&
               payload.primary_order_id == 0 &&
               !has(market::kTickPrimaryOrderIdValidV1) &&
               payload.buy_order_id >= 0 &&
               payload.sell_order_id >= 0 &&
               has(market::kTickBuyOrderIdValidV1) == has_buy &&
               has(market::kTickSellOrderIdValidV1) == has_sell &&
               has(market::kTickPriceValidV1) &&
               payload.price.normalized_p6 > 0;
    }
    if (payload.source_raw_code_1 != 52) {
        return false;
    }
    const bool has_buy = payload.buy_order_id > 0;
    const bool has_sell = payload.sell_order_id > 0;
    return action == market::TickActionV1::kCancel &&
           (side == market::SideV1::kBuy ||
            side == market::SideV1::kSell) &&
           order_type == market::OrderTypeV1::kUnknown &&
           payload.primary_order_id > 0 &&
           has_buy != has_sell &&
           has(market::kTickPrimaryOrderIdValidV1) &&
           has(market::kTickBuyOrderIdValidV1) == has_buy &&
           has(market::kTickSellOrderIdValidV1) == has_sell &&
           payload.primary_order_id ==
               (has_buy ? payload.buy_order_id
                        : payload.sell_order_id) &&
           side == (has_buy ? market::SideV1::kBuy
                            : market::SideV1::kSell) &&
           !has(market::kTickPriceValidV1);
}

template <typename Anchor>
void FillAnchor(
    const RealtimeWireTickPayloadV2& payload,
    Anchor* output) noexcept {
    const LocalTimeProjection local_time =
        ProjectLocalTime(
            payload.common.vendor_local_time_raw);
    output->native_event_sequence =
        payload.native_event_sequence;
    output->source_sequence =
        payload.common.source_sequence;
    output->ingress_sequence =
        payload.common.ingress_sequence;
    output->tick_stream_sequence =
        payload.common.tick_stream_sequence;
    output->vendor_sequence_id =
        payload.common.vendor_sequence_id;
    output->event_time_ns_since_midnight =
        payload.common.exchange_time_ns_since_midnight;
    output->event_time_unix_ns =
        payload.common.event_time_unix_ns;
    output->recv_realtime_ns =
        payload.common.recv_realtime_ns;
    output->recv_monotonic_ns =
        payload.common.recv_monotonic_ns;
    output->vendor_local_time_raw =
        payload.common.vendor_local_time_raw;
    output->vendor_local_time_ns_since_midnight =
        local_time.nanoseconds_since_midnight;
    output->event_time_valid =
        (payload.validity_bitmap &
         market::kTickExchangeTimeValidV1) != 0U;
    output->event_time_unix_ns_valid =
        output->event_time_valid;
    output->vendor_local_time_valid = local_time.valid;
}

}  // namespace

std::string_view WireOrderEventProjectionResultNameV2(
    WireOrderEventProjectionResultV2 result) noexcept {
    switch (result) {
        case WireOrderEventProjectionResultV2::kProjected:
            return "projected";
        case WireOrderEventProjectionResultV2::kNotTargetEvent:
            return "not_target_event";
        case WireOrderEventProjectionResultV2::kNullOutput:
            return "null_output";
        case WireOrderEventProjectionResultV2::kInvalidEnvelope:
            return "invalid_envelope";
        case WireOrderEventProjectionResultV2::
            kInvalidFieldEncoding:
            return "invalid_field_encoding";
        case WireOrderEventProjectionResultV2::
            kInvalidEventContract:
            return "invalid_event_contract";
    }
    return "unknown";
}

WireOrderEventProjectionResultV2
ProjectShanghaiOrderEventInputFromWireV2(
    const RealtimeWireTickPayloadV2& payload,
    market::ShanghaiOrderEventInputV1* output) noexcept {
    if (output == nullptr) {
        return WireOrderEventProjectionResultV2::kNullOutput;
    }
    *output = {};
    if (!CommonEnvelopeValid(payload)) {
        return WireOrderEventProjectionResultV2::kInvalidEnvelope;
    }
    if (payload.common.event_kind !=
        static_cast<std::uint8_t>(
            market::MarketEventKindV1::kShanghaiTick)) {
        return WireOrderEventProjectionResultV2::kNotTargetEvent;
    }
    if (!EnumAndNumericEncodingValid(payload, 3U, 3U)) {
        return WireOrderEventProjectionResultV2::
            kInvalidFieldEncoding;
    }
    if (!ShanghaiEventContractValid(payload)) {
        return WireOrderEventProjectionResultV2::
            kInvalidEventContract;
    }

    market::ShanghaiOrderEventInputV1 projected{};
    projected.trade_date = payload.common.trade_date;
    projected.instrument_id = payload.common.instrument_id;
    projected.channel =
        static_cast<std::int32_t>(payload.channel);
    FillAnchor(payload, &projected.anchor);
    projected.action =
        static_cast<market::TickActionV1>(payload.action);
    projected.side =
        static_cast<market::SideV1>(payload.side);
    projected.aggressor =
        static_cast<market::AggressorV1>(
            payload.aggressor);
    projected.phase =
        static_cast<market::TradingPhaseV1>(payload.phase);
    projected.price_p6 = payload.price.normalized_p6;
    projected.trade_amount_p6 =
        payload.trade_amount.normalized_p6;
    projected.quantity = payload.quantity.raw;
    projected.matched_quantity =
        payload.matched_quantity.raw;
    projected.primary_order_id =
        payload.primary_order_id;
    projected.buy_order_id = payload.buy_order_id;
    projected.sell_order_id = payload.sell_order_id;
    projected.price_valid = payload.price.valid != 0U;
    projected.trade_amount_valid =
        payload.trade_amount.valid != 0U;
    projected.quantity_valid =
        payload.quantity.valid != 0U;
    projected.matched_quantity_valid =
        payload.matched_quantity.valid != 0U;
    projected.phase_valid =
        (payload.validity_bitmap &
         market::kTickPhaseValidV1) != 0U;
    projected.source_quality_flags =
        payload.common.quality_flags;
    projected.source_market_notices =
        payload.common.market_notices;
    *output = projected;
    return WireOrderEventProjectionResultV2::kProjected;
}

WireOrderEventProjectionResultV2
ProjectShenzhenOrderEventInputFromWireV2(
    const RealtimeWireTickPayloadV2& payload,
    market::ShenzhenOrderEventInputV1* output) noexcept {
    if (output == nullptr) {
        return WireOrderEventProjectionResultV2::kNullOutput;
    }
    *output = {};
    if (!CommonEnvelopeValid(payload)) {
        return WireOrderEventProjectionResultV2::kInvalidEnvelope;
    }
    if (payload.common.event_kind !=
            static_cast<std::uint8_t>(
                market::MarketEventKindV1::kShenzhenOrder) &&
        payload.common.event_kind !=
            static_cast<std::uint8_t>(
                market::MarketEventKindV1::
                    kShenzhenTransaction)) {
        return WireOrderEventProjectionResultV2::kNotTargetEvent;
    }
    if (!EnumAndNumericEncodingValid(payload, 4U, 0U)) {
        return WireOrderEventProjectionResultV2::
            kInvalidFieldEncoding;
    }
    if (!ShenzhenEventContractValid(payload)) {
        return WireOrderEventProjectionResultV2::
            kInvalidEventContract;
    }

    market::ShenzhenOrderEventInputV1 projected{};
    projected.trade_date = payload.common.trade_date;
    projected.instrument_id = payload.common.instrument_id;
    projected.channel =
        static_cast<std::uint32_t>(payload.channel);
    FillAnchor(payload, &projected.anchor);
    projected.action =
        static_cast<market::TickActionV1>(payload.action);
    projected.side =
        static_cast<market::SideV1>(payload.side);
    projected.order_type =
        static_cast<market::OrderTypeV1>(
            payload.order_type);
    projected.price_p6 =
        payload.price.valid != 0U
            ? payload.price.normalized_p6
            : 0;
    projected.quantity = payload.quantity.raw;
    projected.primary_order_id =
        payload.primary_order_id;
    projected.buy_order_id = payload.buy_order_id;
    projected.sell_order_id = payload.sell_order_id;
    projected.price_valid = payload.price.valid != 0U;
    projected.quantity_valid =
        payload.quantity.valid != 0U;
    projected.side_valid =
        (payload.validity_bitmap &
         market::kTickSideValidV1) != 0U;
    projected.order_type_valid =
        (payload.validity_bitmap &
         market::kTickOrderTypeValidV1) != 0U;
    projected.source_quality_flags =
        payload.common.quality_flags;
    projected.source_market_notices =
        payload.common.market_notices;
    *output = projected;
    return WireOrderEventProjectionResultV2::kProjected;
}

}  // namespace l2flow::ipc
