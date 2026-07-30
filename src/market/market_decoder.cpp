#include "l2flow/market/market_decoder.h"

#include "l2flow/control/checked_body_view.h"
#include "l2flow/control/quality_flags_v1.h"

#include "mdl_shl2_msg.h"
#include "mdl_szl2_msg.h"
#include "mdl_api_types.h"

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <new>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

namespace l2flow::market {
namespace {

namespace sh = datayes::mdl::mdl_shl2_msg;
namespace sz = datayes::mdl::mdl_szl2_msg;
using l2flow::control::CheckedBodyErrorV1;
using l2flow::control::CheckedBodyRangeV1;
using l2flow::control::CheckedBodyViewV1;
using l2flow::control::QualityBit;
using l2flow::control::QualityFlagV1;

constexpr std::uint16_t kCoreServiceVersion = 101U;
constexpr std::uint32_t kNullTime = 1'000'000'000U;
// This V1 implementation intentionally uses a fixed UTC+08:00 conversion
// rather than embedding a timezone database.  Reject older Asia/Shanghai
// dates which can contain historical offset/DST transitions.
constexpr std::uint32_t kMinimumFixedUtc8TradeDate = 19'920'101U;
constexpr std::int32_t kNullI32 =
    std::numeric_limits<std::int32_t>::min();
constexpr std::int64_t kNullI64 =
    std::numeric_limits<std::int64_t>::min();

#define L2FLOW_ASSERT_OFFSET(type, member, expected)                         \
    static_assert(offsetof(type, member) == (expected))
#define L2FLOW_ASSERT_MESSAGE_KEY(type, service, version, message)           \
    static_assert(type::ServiceID == (service));                             \
    static_assert(type::ServiceVer == (version));                            \
    static_assert(type::MessageID == (message))

L2FLOW_ASSERT_MESSAGE_KEY(sh::SHL2MarketData, 4, 101, 4);
L2FLOW_ASSERT_MESSAGE_KEY(sh::NGTSTick, 4, 101, 24);
L2FLOW_ASSERT_MESSAGE_KEY(sz::Snapshot300111_v2, 6, 101, 28);
L2FLOW_ASSERT_MESSAGE_KEY(sz::Order300192_v2, 6, 101, 33);
L2FLOW_ASSERT_MESSAGE_KEY(sz::Transaction300191_v2, 6, 101, 36);

static_assert(sizeof(sh::SHL2MarketData) == 248U);
L2FLOW_ASSERT_OFFSET(sh::SHL2MarketData, UpdateTime, 0U);
L2FLOW_ASSERT_OFFSET(sh::SHL2MarketData, SecurityID, 4U);
L2FLOW_ASSERT_OFFSET(sh::SHL2MarketData, ImageStatus, 10U);
L2FLOW_ASSERT_OFFSET(sh::SHL2MarketData, PreCloPrice, 14U);
L2FLOW_ASSERT_OFFSET(sh::SHL2MarketData, OpenPrice, 18U);
L2FLOW_ASSERT_OFFSET(sh::SHL2MarketData, HighPrice, 22U);
L2FLOW_ASSERT_OFFSET(sh::SHL2MarketData, LowPrice, 26U);
L2FLOW_ASSERT_OFFSET(sh::SHL2MarketData, LastPrice, 30U);
L2FLOW_ASSERT_OFFSET(sh::SHL2MarketData, ClosePrice, 34U);
L2FLOW_ASSERT_OFFSET(sh::SHL2MarketData, InstruStatus, 38U);
L2FLOW_ASSERT_OFFSET(sh::SHL2MarketData, TradNumber, 44U);
L2FLOW_ASSERT_OFFSET(sh::SHL2MarketData, TradVolume, 48U);
L2FLOW_ASSERT_OFFSET(sh::SHL2MarketData, Turnover, 56U);
L2FLOW_ASSERT_OFFSET(sh::SHL2MarketData, TotalBidVol, 64U);
L2FLOW_ASSERT_OFFSET(sh::SHL2MarketData, WAvgBidPri, 72U);
L2FLOW_ASSERT_OFFSET(sh::SHL2MarketData, AltWAvgBidPri, 76U);
L2FLOW_ASSERT_OFFSET(sh::SHL2MarketData, TotalAskVol, 80U);
L2FLOW_ASSERT_OFFSET(sh::SHL2MarketData, WAvgAskPri, 88U);
L2FLOW_ASSERT_OFFSET(sh::SHL2MarketData, AltWAvgAskPri, 92U);
L2FLOW_ASSERT_OFFSET(sh::SHL2MarketData, EtfBuyNumber, 96U);
L2FLOW_ASSERT_OFFSET(sh::SHL2MarketData, EtfBuyVolume, 100U);
L2FLOW_ASSERT_OFFSET(sh::SHL2MarketData, EtfBuyMoney, 108U);
L2FLOW_ASSERT_OFFSET(sh::SHL2MarketData, EtfSellNumber, 116U);
L2FLOW_ASSERT_OFFSET(sh::SHL2MarketData, EtfSellVolume, 120U);
L2FLOW_ASSERT_OFFSET(sh::SHL2MarketData, ETFSellMoney, 128U);
L2FLOW_ASSERT_OFFSET(sh::SHL2MarketData, YieldToMatu, 136U);
L2FLOW_ASSERT_OFFSET(sh::SHL2MarketData, TotWarExNum, 140U);
L2FLOW_ASSERT_OFFSET(sh::SHL2MarketData, WarLowerPri, 148U);
L2FLOW_ASSERT_OFFSET(sh::SHL2MarketData, WarUpperPri, 156U);
L2FLOW_ASSERT_OFFSET(sh::SHL2MarketData, WiDBuyNum, 164U);
L2FLOW_ASSERT_OFFSET(sh::SHL2MarketData, WiDBuyVol, 168U);
L2FLOW_ASSERT_OFFSET(sh::SHL2MarketData, WiDBuyMon, 176U);
L2FLOW_ASSERT_OFFSET(sh::SHL2MarketData, WiDSellNum, 184U);
L2FLOW_ASSERT_OFFSET(sh::SHL2MarketData, WiDSellVol, 188U);
L2FLOW_ASSERT_OFFSET(sh::SHL2MarketData, WiDSellMon, 196U);
L2FLOW_ASSERT_OFFSET(sh::SHL2MarketData, TotBidNum, 204U);
L2FLOW_ASSERT_OFFSET(sh::SHL2MarketData, TotSellNum, 208U);
L2FLOW_ASSERT_OFFSET(sh::SHL2MarketData, MaxBidDur, 212U);
L2FLOW_ASSERT_OFFSET(sh::SHL2MarketData, MaxSellDur, 216U);
L2FLOW_ASSERT_OFFSET(sh::SHL2MarketData, BidNum, 220U);
L2FLOW_ASSERT_OFFSET(sh::SHL2MarketData, SellNum, 224U);
L2FLOW_ASSERT_OFFSET(sh::SHL2MarketData, BidLevels, 228U);
L2FLOW_ASSERT_OFFSET(sh::SHL2MarketData, SellLevels, 236U);
L2FLOW_ASSERT_OFFSET(sh::SHL2MarketData, IOPV, 244U);
static_assert(sizeof(sh::SHL2MarketData::BidLevelsItem) == 28U);
L2FLOW_ASSERT_OFFSET(sh::SHL2MarketData::BidLevelsItem, OrderPrice, 4U);
L2FLOW_ASSERT_OFFSET(sh::SHL2MarketData::BidLevelsItem, OrderVol, 8U);
L2FLOW_ASSERT_OFFSET(sh::SHL2MarketData::BidLevelsItem, OrderNum, 16U);
L2FLOW_ASSERT_OFFSET(sh::SHL2MarketData::BidLevelsItem, NOrders, 20U);
static_assert(
    sizeof(sh::SHL2MarketData::BidLevelsItem::NOrdersItem) == 16U);
L2FLOW_ASSERT_OFFSET(
    sh::SHL2MarketData::BidLevelsItem::NOrdersItem, OrderQty, 8U);
static_assert(sizeof(sh::SHL2MarketData::SellLevelsItem) == 28U);
L2FLOW_ASSERT_OFFSET(sh::SHL2MarketData::SellLevelsItem, OrderPrice, 4U);
L2FLOW_ASSERT_OFFSET(sh::SHL2MarketData::SellLevelsItem, OrderVol, 8U);
L2FLOW_ASSERT_OFFSET(sh::SHL2MarketData::SellLevelsItem, OrderNum, 16U);
L2FLOW_ASSERT_OFFSET(sh::SHL2MarketData::SellLevelsItem, NoOrders, 20U);
static_assert(
    sizeof(sh::SHL2MarketData::SellLevelsItem::NoOrdersItem) == 16U);
L2FLOW_ASSERT_OFFSET(
    sh::SHL2MarketData::SellLevelsItem::NoOrdersItem, OrderQty, 8U);

static_assert(sizeof(sh::NGTSTick) == 70U);
L2FLOW_ASSERT_OFFSET(sh::NGTSTick, BizIndex, 0U);
L2FLOW_ASSERT_OFFSET(sh::NGTSTick, Channel, 8U);
L2FLOW_ASSERT_OFFSET(sh::NGTSTick, SecurityID, 12U);
L2FLOW_ASSERT_OFFSET(sh::NGTSTick, TickTime, 18U);
L2FLOW_ASSERT_OFFSET(sh::NGTSTick, Type, 22U);
L2FLOW_ASSERT_OFFSET(sh::NGTSTick, BuyOrderNO, 28U);
L2FLOW_ASSERT_OFFSET(sh::NGTSTick, SellOrderNO, 36U);
L2FLOW_ASSERT_OFFSET(sh::NGTSTick, Price, 44U);
L2FLOW_ASSERT_OFFSET(sh::NGTSTick, Qty, 48U);
L2FLOW_ASSERT_OFFSET(sh::NGTSTick, TradeMoney, 56U);
L2FLOW_ASSERT_OFFSET(sh::NGTSTick, TickBSFlag, 64U);

static_assert(sizeof(sz::Snapshot300111_v2) == 224U);
L2FLOW_ASSERT_OFFSET(sz::Snapshot300111_v2, UpdateTime, 0U);
L2FLOW_ASSERT_OFFSET(sz::Snapshot300111_v2, ChannelNo, 4U);
L2FLOW_ASSERT_OFFSET(sz::Snapshot300111_v2, MDStreamID, 8U);
L2FLOW_ASSERT_OFFSET(sz::Snapshot300111_v2, SecurityID, 14U);
L2FLOW_ASSERT_OFFSET(sz::Snapshot300111_v2, SecurityIDSource, 20U);
L2FLOW_ASSERT_OFFSET(sz::Snapshot300111_v2, TradingPhaseCode, 26U);
L2FLOW_ASSERT_OFFSET(sz::Snapshot300111_v2, PreCloPrice, 32U);
L2FLOW_ASSERT_OFFSET(sz::Snapshot300111_v2, TurnNum, 40U);
L2FLOW_ASSERT_OFFSET(sz::Snapshot300111_v2, Volume, 48U);
L2FLOW_ASSERT_OFFSET(sz::Snapshot300111_v2, Turnover, 56U);
L2FLOW_ASSERT_OFFSET(sz::Snapshot300111_v2, LastPrice, 64U);
L2FLOW_ASSERT_OFFSET(sz::Snapshot300111_v2, OpenPrice, 72U);
L2FLOW_ASSERT_OFFSET(sz::Snapshot300111_v2, HighPrice, 80U);
L2FLOW_ASSERT_OFFSET(sz::Snapshot300111_v2, LowPrice, 88U);
L2FLOW_ASSERT_OFFSET(sz::Snapshot300111_v2, DifPrice1, 96U);
L2FLOW_ASSERT_OFFSET(sz::Snapshot300111_v2, DifPrice2, 104U);
L2FLOW_ASSERT_OFFSET(sz::Snapshot300111_v2, PE1, 112U);
L2FLOW_ASSERT_OFFSET(sz::Snapshot300111_v2, PE2, 120U);
L2FLOW_ASSERT_OFFSET(sz::Snapshot300111_v2, PreCloseIOPV, 128U);
L2FLOW_ASSERT_OFFSET(sz::Snapshot300111_v2, IOPV, 136U);
L2FLOW_ASSERT_OFFSET(sz::Snapshot300111_v2, TotalOfferQty, 144U);
L2FLOW_ASSERT_OFFSET(sz::Snapshot300111_v2, WeightedAvgOfferPx, 152U);
L2FLOW_ASSERT_OFFSET(sz::Snapshot300111_v2, TotalBidQty, 160U);
L2FLOW_ASSERT_OFFSET(sz::Snapshot300111_v2, WeightedAvgBidPx, 168U);
L2FLOW_ASSERT_OFFSET(sz::Snapshot300111_v2, HighLimitPrice, 176U);
L2FLOW_ASSERT_OFFSET(sz::Snapshot300111_v2, LowLimitPrice, 184U);
L2FLOW_ASSERT_OFFSET(sz::Snapshot300111_v2, OpenInt, 192U);
L2FLOW_ASSERT_OFFSET(sz::Snapshot300111_v2, OptPremiumRatio, 200U);
L2FLOW_ASSERT_OFFSET(sz::Snapshot300111_v2, BidPriceLevel, 208U);
L2FLOW_ASSERT_OFFSET(sz::Snapshot300111_v2, AskPriceLevel, 216U);
static_assert(sizeof(sz::Snapshot300111_v2::BidPriceLevelItem) == 28U);
L2FLOW_ASSERT_OFFSET(sz::Snapshot300111_v2::BidPriceLevelItem, Volume, 0U);
L2FLOW_ASSERT_OFFSET(sz::Snapshot300111_v2::BidPriceLevelItem, Price, 8U);
L2FLOW_ASSERT_OFFSET(sz::Snapshot300111_v2::BidPriceLevelItem, NumOrders, 16U);
L2FLOW_ASSERT_OFFSET(sz::Snapshot300111_v2::BidPriceLevelItem, Orders, 20U);
static_assert(
    sizeof(sz::Snapshot300111_v2::BidPriceLevelItem::OrdersItem) == 8U);
L2FLOW_ASSERT_OFFSET(
    sz::Snapshot300111_v2::BidPriceLevelItem::OrdersItem, OrderQty, 0U);
static_assert(sizeof(sz::Snapshot300111_v2::AskPriceLevelItem) == 28U);
L2FLOW_ASSERT_OFFSET(sz::Snapshot300111_v2::AskPriceLevelItem, Volume, 0U);
L2FLOW_ASSERT_OFFSET(sz::Snapshot300111_v2::AskPriceLevelItem, Price, 8U);
L2FLOW_ASSERT_OFFSET(sz::Snapshot300111_v2::AskPriceLevelItem, NumOrders, 16U);
L2FLOW_ASSERT_OFFSET(sz::Snapshot300111_v2::AskPriceLevelItem, Orders, 20U);
static_assert(
    sizeof(sz::Snapshot300111_v2::AskPriceLevelItem::OrdersItem) == 8U);
L2FLOW_ASSERT_OFFSET(
    sz::Snapshot300111_v2::AskPriceLevelItem::OrdersItem, OrderQty, 0U);

static_assert(sizeof(sz::Order300192_v2) == 58U);
L2FLOW_ASSERT_OFFSET(sz::Order300192_v2, ChannelNo, 0U);
L2FLOW_ASSERT_OFFSET(sz::Order300192_v2, ApplSeqNum, 4U);
L2FLOW_ASSERT_OFFSET(sz::Order300192_v2, MDStreamID, 12U);
L2FLOW_ASSERT_OFFSET(sz::Order300192_v2, SecurityID, 18U);
L2FLOW_ASSERT_OFFSET(sz::Order300192_v2, SecurityIDSource, 24U);
L2FLOW_ASSERT_OFFSET(sz::Order300192_v2, Price, 30U);
L2FLOW_ASSERT_OFFSET(sz::Order300192_v2, OrderQty, 38U);
L2FLOW_ASSERT_OFFSET(sz::Order300192_v2, Side, 46U);
L2FLOW_ASSERT_OFFSET(sz::Order300192_v2, TransactTime, 50U);
L2FLOW_ASSERT_OFFSET(sz::Order300192_v2, OrdType, 54U);

static_assert(sizeof(sz::Transaction300191_v2) == 70U);
L2FLOW_ASSERT_OFFSET(sz::Transaction300191_v2, ChannelNo, 0U);
L2FLOW_ASSERT_OFFSET(sz::Transaction300191_v2, ApplSeqNum, 4U);
L2FLOW_ASSERT_OFFSET(sz::Transaction300191_v2, MDStreamID, 12U);
L2FLOW_ASSERT_OFFSET(sz::Transaction300191_v2, BidApplSeqNum, 18U);
L2FLOW_ASSERT_OFFSET(sz::Transaction300191_v2, OfferApplSeqNum, 26U);
L2FLOW_ASSERT_OFFSET(sz::Transaction300191_v2, SecurityID, 34U);
L2FLOW_ASSERT_OFFSET(sz::Transaction300191_v2, SecurityIDSource, 40U);
L2FLOW_ASSERT_OFFSET(sz::Transaction300191_v2, LastPx, 46U);
L2FLOW_ASSERT_OFFSET(sz::Transaction300191_v2, LastQty, 54U);
L2FLOW_ASSERT_OFFSET(sz::Transaction300191_v2, ExecType, 62U);
L2FLOW_ASSERT_OFFSET(sz::Transaction300191_v2, TransactTime, 66U);

static_assert(std::is_same_v<
              decltype(sh::SHL2MarketData::PreCloPrice),
              datayes::mdl::MDLFloatT<3>>);
static_assert(std::is_same_v<
              decltype(sh::SHL2MarketData::Turnover),
              datayes::mdl::MDLDoubleT<5>>);
static_assert(std::is_same_v<
              decltype(sh::SHL2MarketData::WarLowerPri),
              datayes::mdl::MDLDoubleT<3>>);
static_assert(std::is_same_v<
              decltype(sh::SHL2MarketData::WarUpperPri),
              datayes::mdl::MDLDoubleT<5>>);
static_assert(std::is_same_v<
              decltype(sh::NGTSTick::Price),
              datayes::mdl::MDLFloatT<3>>);
static_assert(std::is_same_v<
              decltype(sh::NGTSTick::TradeMoney),
              datayes::mdl::MDLDoubleT<3>>);
static_assert(std::is_same_v<
              decltype(sz::Snapshot300111_v2::PreCloPrice),
              datayes::mdl::MDLDoubleT<4>>);
static_assert(std::is_same_v<
              decltype(sz::Snapshot300111_v2::LastPrice),
              datayes::mdl::MDLDoubleT<6>>);
static_assert(std::is_same_v<
              decltype(sz::Order300192_v2::Price),
              datayes::mdl::MDLDoubleT<4>>);
static_assert(std::is_same_v<
              decltype(sz::Transaction300191_v2::LastPx),
              datayes::mdl::MDLDoubleT<4>>);

#undef L2FLOW_ASSERT_MESSAGE_KEY
#undef L2FLOW_ASSERT_OFFSET

MarketDecodeErrorV1 TranslateBodyError(
    CheckedBodyErrorV1 error) noexcept {
    switch (error) {
        case CheckedBodyErrorV1::kNone:
            return MarketDecodeErrorV1::kNone;
        case CheckedBodyErrorV1::kTruncated:
            return MarketDecodeErrorV1::kTruncated;
        case CheckedBodyErrorV1::kRangeOverlap:
            return MarketDecodeErrorV1::kRangeOverlap;
        case CheckedBodyErrorV1::kCountExceeded:
            return MarketDecodeErrorV1::kCountExceeded;
        case CheckedBodyErrorV1::kResourceExhausted:
            return MarketDecodeErrorV1::kResourceExhausted;
        case CheckedBodyErrorV1::kArithmeticOverflow:
        case CheckedBodyErrorV1::kOffsetInvalid:
            return MarketDecodeErrorV1::kOffsetInvalid;
    }
    return MarketDecodeErrorV1::kOffsetInvalid;
}

bool IsLeapYear(std::uint32_t year) noexcept {
    return (year % 4U == 0U && year % 100U != 0U) ||
           year % 400U == 0U;
}

bool ParseDate(
    std::uint32_t date,
    std::int64_t* days_since_epoch) noexcept {
    if (days_since_epoch == nullptr) {
        return false;
    }
    const std::uint32_t year = date / 10'000U;
    const std::uint32_t month = (date / 100U) % 100U;
    const std::uint32_t day = date % 100U;
    if (year < 1970U || year > 2200U || month == 0U ||
        month > 12U || day == 0U) {
        return false;
    }
    constexpr std::array<std::uint32_t, 12U> kMonthDays{
        31U, 28U, 31U, 30U, 31U, 30U,
        31U, 31U, 30U, 31U, 30U, 31U};
    std::uint32_t maximum_day = kMonthDays[month - 1U];
    if (month == 2U && IsLeapYear(year)) {
        maximum_day = 29U;
    }
    if (day > maximum_day) {
        return false;
    }

    // Howard Hinnant's proleptic-Gregorian civil-date transform.  The input
    // range above keeps every intermediate far inside int64_t.
    std::int64_t adjusted_year = static_cast<std::int64_t>(year);
    adjusted_year -= month <= 2U ? 1 : 0;
    const std::int64_t era = adjusted_year / 400;
    const std::int64_t year_of_era = adjusted_year - era * 400;
    const std::int64_t adjusted_month =
        static_cast<std::int64_t>(month) + (month > 2U ? -3 : 9);
    const std::int64_t day_of_year =
        (153 * adjusted_month + 2) / 5 +
        static_cast<std::int64_t>(day) - 1;
    const std::int64_t day_of_era =
        year_of_era * 365 + year_of_era / 4 -
        year_of_era / 100 + day_of_year;
    *days_since_epoch = era * 146'097 + day_of_era - 719'468;
    return true;
}

TimeValueV1 DecodeTime(
    std::uint32_t raw,
    std::uint32_t trade_date,
    bool project_trade_date,
    MarketNoticeV1 invalid_notice,
    std::uint64_t* quality_flags,
    std::uint64_t* market_notices) noexcept {
    TimeValueV1 result{};
    result.raw_hhmmssmmm = raw;
    if (raw == kNullTime) {
        result.is_null = true;
        *quality_flags |= QualityBit(QualityFlagV1::kNullValuePresent);
        return result;
    }
    if (raw >= kNullTime) {
        *market_notices |= MarketNoticeBitV1(invalid_notice);
        return result;
    }
    const std::uint32_t hour = raw / 10'000'000U;
    const std::uint32_t minute = (raw / 100'000U) % 100U;
    const std::uint32_t second = (raw / 1'000U) % 100U;
    const std::uint32_t millisecond = raw % 1'000U;
    if (hour >= 24U || minute >= 60U || second >= 60U ||
        millisecond >= 1'000U) {
        *market_notices |= MarketNoticeBitV1(invalid_notice);
        return result;
    }
    const std::uint64_t seconds_since_midnight =
        static_cast<std::uint64_t>(hour) * 3'600U +
        static_cast<std::uint64_t>(minute) * 60U + second;
    result.nanoseconds_since_midnight =
        seconds_since_midnight * 1'000'000'000ULL +
        static_cast<std::uint64_t>(millisecond) * 1'000'000ULL;
    result.valid = true;

    // The SDK header LocalTime is a time-of-day only.  It may be observed
    // during replay, after-midnight capture, or delayed delivery, so it must
    // not be assigned the exchange trade_date without a separate trusted
    // capture calendar date.
    if (!project_trade_date) {
        return result;
    }

    std::int64_t days = 0;
    if (!ParseDate(trade_date, &days)) {
        return result;
    }
    constexpr std::int64_t kSecondsPerDay = 86'400;
    constexpr std::int64_t kShanghaiUtcOffsetSeconds = 8 * 3'600;
    const std::int64_t seconds =
        days * kSecondsPerDay +
        static_cast<std::int64_t>(seconds_since_midnight) -
        kShanghaiUtcOffsetSeconds;
    if (seconds <=
            std::numeric_limits<std::int64_t>::max() /
                1'000'000'000LL &&
        seconds >=
            std::numeric_limits<std::int64_t>::min() /
                1'000'000'000LL) {
        result.unix_nanoseconds =
            seconds * 1'000'000'000LL +
            static_cast<std::int64_t>(millisecond) * 1'000'000LL;
        result.unix_nanoseconds_valid = true;
    }
    return result;
}

template <typename Unsigned>
bool LoadUnsigned(
    std::span<const std::byte> body,
    std::size_t offset,
    Unsigned* output) noexcept {
    static_assert(std::is_unsigned_v<Unsigned>);
    if (output == nullptr || offset > body.size() ||
        sizeof(Unsigned) > body.size() - offset) {
        return false;
    }
    Unsigned value = 0;
    for (std::size_t index = 0U; index < sizeof(Unsigned); ++index) {
        value |= static_cast<Unsigned>(
                     std::to_integer<Unsigned>(body[offset + index]))
                 << (index * 8U);
    }
    *output = value;
    return true;
}

bool LoadI32(
    std::span<const std::byte> body,
    std::size_t offset,
    std::int32_t* output) noexcept {
    std::uint32_t wire = 0U;
    if (!LoadUnsigned(body, offset, &wire) || output == nullptr) {
        return false;
    }
    *output = std::bit_cast<std::int32_t>(wire);
    return true;
}

bool LoadI64(
    std::span<const std::byte> body,
    std::size_t offset,
    std::int64_t* output) noexcept {
    std::uint64_t wire = 0U;
    if (!LoadUnsigned(body, offset, &wire) || output == nullptr) {
        return false;
    }
    *output = std::bit_cast<std::int64_t>(wire);
    return true;
}

MarketDecodeErrorV1 LoadDecimalRaw(
    std::int64_t raw,
    std::uint8_t scale,
    std::int64_t null_value,
    std::uint64_t* quality_flags,
    DecimalValueV1* output) noexcept {
    if (output == nullptr || scale > 6U) {
        return MarketDecodeErrorV1::kFixedPointOverflow;
    }
    output->raw = raw;
    output->scale = scale;
    if (raw == null_value) {
        output->is_null = true;
        *quality_flags |= QualityBit(QualityFlagV1::kNullValuePresent);
        return MarketDecodeErrorV1::kNone;
    }
    return MarketDecodeErrorV1::kNone;
}

MarketDecodeErrorV1 NormalizeDecimalP6(
    DecimalValueV1* output) noexcept {
    if (output == nullptr || output->scale > 6U) {
        return MarketDecodeErrorV1::kFixedPointOverflow;
    }
    if (output->is_null) {
        return MarketDecodeErrorV1::kNone;
    }
    constexpr std::array<std::int64_t, 7U> kPowersOfTen{
        1LL, 10LL, 100LL, 1'000LL,
        10'000LL, 100'000LL, 1'000'000LL};
    const std::int64_t multiplier =
        kPowersOfTen[6U - output->scale];
    if ((output->raw > 0 &&
         output->raw >
             std::numeric_limits<std::int64_t>::max() / multiplier) ||
        (output->raw < 0 &&
         output->raw <
             std::numeric_limits<std::int64_t>::min() / multiplier)) {
        return MarketDecodeErrorV1::kFixedPointOverflow;
    }
    output->normalized_p6 = output->raw * multiplier;
    output->valid = true;
    return MarketDecodeErrorV1::kNone;
}

MarketDecodeErrorV1 DecodeDecimal(
    std::int64_t raw,
    std::uint8_t scale,
    std::int64_t null_value,
    std::uint64_t* quality_flags,
    DecimalValueV1* output) noexcept {
    const MarketDecodeErrorV1 load_error = LoadDecimalRaw(
        raw, scale, null_value, quality_flags, output);
    if (load_error != MarketDecodeErrorV1::kNone) {
        return load_error;
    }
    return NormalizeDecimalP6(output);
}

MarketDecodeErrorV1 LoadDecimalI32Raw(
    std::span<const std::byte> body,
    std::size_t offset,
    std::uint8_t scale,
    std::uint64_t* quality_flags,
    DecimalValueV1* output) noexcept {
    std::int32_t raw = 0;
    if (!LoadI32(body, offset, &raw)) {
        return MarketDecodeErrorV1::kTruncated;
    }
    return LoadDecimalRaw(
        raw, scale, kNullI32, quality_flags, output);
}

MarketDecodeErrorV1 LoadDecimalI64Raw(
    std::span<const std::byte> body,
    std::size_t offset,
    std::uint8_t scale,
    std::uint64_t* quality_flags,
    DecimalValueV1* output) noexcept {
    std::int64_t raw = 0;
    if (!LoadI64(body, offset, &raw)) {
        return MarketDecodeErrorV1::kTruncated;
    }
    return LoadDecimalRaw(
        raw, scale, kNullI64, quality_flags, output);
}

MarketDecodeErrorV1 DecodeDecimalI64(
    std::span<const std::byte> body,
    std::size_t offset,
    std::uint8_t scale,
    std::uint64_t* quality_flags,
    DecimalValueV1* output) noexcept {
    std::int64_t raw = 0;
    if (!LoadI64(body, offset, &raw)) {
        return MarketDecodeErrorV1::kTruncated;
    }
    return DecodeDecimal(
        raw, scale, kNullI64, quality_flags, output);
}

MarketDecodeErrorV1 NormalizeStrictlyPositivePriceP6(
    DecimalValueV1* output,
    std::uint64_t* market_notices) noexcept {
    if (output == nullptr || market_notices == nullptr) {
        return MarketDecodeErrorV1::kInvalidInput;
    }
    if (output->is_null) {
        return MarketDecodeErrorV1::kNone;
    }
    if (output->raw <= 0) {
        // Domain rejection deliberately precedes p6 multiplication.  An
        // invalid negative extreme remains an auditable decoded value instead
        // of turning the whole message into a fixed-point-overflow failure.
        *market_notices |= MarketNoticeBitV1(
            MarketNoticeV1::kAbsolutePriceDomainInvalid);
        return MarketDecodeErrorV1::kNone;
    }
    return NormalizeDecimalP6(output);
}

MarketDecodeErrorV1 DecodeStrictlyPositivePriceI32(
    std::span<const std::byte> body,
    std::size_t offset,
    std::uint8_t scale,
    std::uint64_t* quality_flags,
    std::uint64_t* market_notices,
    DecimalValueV1* output) noexcept {
    const MarketDecodeErrorV1 error = LoadDecimalI32Raw(
        body, offset, scale, quality_flags, output);
    if (error != MarketDecodeErrorV1::kNone) {
        return error;
    }
    return NormalizeStrictlyPositivePriceP6(output, market_notices);
}

MarketDecodeErrorV1 DecodeStrictlyPositivePriceI64(
    std::span<const std::byte> body,
    std::size_t offset,
    std::uint8_t scale,
    std::uint64_t* quality_flags,
    std::uint64_t* market_notices,
    DecimalValueV1* output) noexcept {
    const MarketDecodeErrorV1 error = LoadDecimalI64Raw(
        body, offset, scale, quality_flags, output);
    if (error != MarketDecodeErrorV1::kNone) {
        return error;
    }
    return NormalizeStrictlyPositivePriceP6(output, market_notices);
}

MarketDecodeErrorV1 LoadQuantityI64Raw(
    std::span<const std::byte> body,
    std::size_t offset,
    std::uint8_t scale,
    bool nullable,
    std::uint64_t* quality_flags,
    QuantityValueV1* output) noexcept {
    if (quality_flags == nullptr || output == nullptr) {
        return MarketDecodeErrorV1::kInvalidInput;
    }
    std::int64_t raw = 0;
    if (!LoadI64(body, offset, &raw)) {
        return MarketDecodeErrorV1::kTruncated;
    }
    output->raw = raw;
    output->scale = scale;
    if (nullable && raw == kNullI64) {
        output->is_null = true;
        *quality_flags |= QualityBit(QualityFlagV1::kNullValuePresent);
    }
    // valid remains false: this helper is for exact raw retention when a
    // versioned product-applicability contract is unavailable.
    return MarketDecodeErrorV1::kNone;
}

MarketDecodeErrorV1 DecodeQuantityI64(
    std::span<const std::byte> body,
    std::size_t offset,
    std::uint8_t scale,
    bool nullable,
    std::uint64_t* quality_flags,
    std::uint64_t* market_notices,
    QuantityValueV1* output) noexcept {
    if (quality_flags == nullptr || market_notices == nullptr ||
        output == nullptr) {
        return MarketDecodeErrorV1::kInvalidInput;
    }
    std::int64_t raw = 0;
    if (!LoadI64(body, offset, &raw)) {
        return MarketDecodeErrorV1::kTruncated;
    }
    output->raw = raw;
    output->scale = scale;
    if (nullable && raw == kNullI64) {
        output->is_null = true;
        *quality_flags |= QualityBit(QualityFlagV1::kNullValuePresent);
        return MarketDecodeErrorV1::kNone;
    }
    output->valid = raw >= 0;
    if (!output->valid) {
        *market_notices |= MarketNoticeBitV1(
            MarketNoticeV1::kQuantityDomainInvalid);
    }
    return MarketDecodeErrorV1::kNone;
}

bool IsPrintableAscii(std::span<const std::byte> value) noexcept {
    return std::all_of(
        value.begin(), value.end(), [](std::byte byte) {
            const std::uint8_t character =
                std::to_integer<std::uint8_t>(byte);
            return character >= 0x20U && character <= 0x7eU;
        });
}

MarketDecodeErrorV1 ReadText(
    CheckedBodyViewV1* view,
    std::size_t descriptor_offset,
    std::size_t fixed_bytes,
    std::size_t maximum_text_bytes,
    bool require_nonempty,
    std::uint64_t* quality_flags,
    std::string* output,
    bool* field_valid) {
    if (quality_flags == nullptr || output == nullptr ||
        field_valid == nullptr) {
        return MarketDecodeErrorV1::kInvalidInput;
    }
    *field_valid = false;
    std::span<const std::byte> bytes;
    const CheckedBodyErrorV1 body_error = view->ReadString(
        descriptor_offset, fixed_bytes, &bytes);
    if (body_error != CheckedBodyErrorV1::kNone) {
        return TranslateBodyError(body_error);
    }
    if (bytes.size() > maximum_text_bytes) {
        return MarketDecodeErrorV1::kCountExceeded;
    }
    if (bytes.empty()) {
        output->clear();
    } else {
        output->assign(
            reinterpret_cast<const char*>(bytes.data()), bytes.size());
    }
    const bool valid =
        (!require_nonempty || !bytes.empty()) &&
        IsPrintableAscii(bytes);
    if (!valid) {
        *quality_flags |= QualityBit(QualityFlagV1::kDecodeTextInvalid);
    }
    *field_valid = valid;
    return MarketDecodeErrorV1::kNone;
}

void ApplyBodyNotices(
    const CheckedBodyViewV1& view,
    DecodedMarketCommonV1* common) noexcept {
    if (view.noncanonical_empty_offset()) {
        common->quality_flags |=
            QualityBit(QualityFlagV1::kNoncanonicalEmptyOffset);
    }
}

DecodedMarketCommonV1 MakeCommon(
    const MarketMessageViewV1& input,
    MarketEventKindV1 kind,
    MarketV1 market,
    std::uint32_t exchange_time_raw) noexcept {
    DecodedMarketCommonV1 common{};
    common.kind = kind;
    common.market = market;
    common.origin = input;
    common.origin.body = {};
    common.exchange_time = DecodeTime(
        exchange_time_raw,
        input.trade_date,
        true,
        MarketNoticeV1::kExchangeTimeInvalid,
        &common.quality_flags,
        &common.market_notices);
    common.vendor_local_time = DecodeTime(
        input.vendor_local_time_raw,
        input.trade_date,
        false,
        MarketNoticeV1::kVendorLocalTimeInvalid,
        &common.quality_flags,
        &common.market_notices);
    return common;
}

void MarkInstrumentPendingObservedBinding(
    DecodedMarketCommonV1* common) noexcept {
    common->quality_flags |=
        QualityBit(QualityFlagV1::kInstrumentUnknown);
    common->quality_flags |=
        QualityBit(QualityFlagV1::kQtyUnitUnknown);
}

MarketDecodeErrorV1 DecodeShLevel(
    CheckedBodyViewV1* view,
    const CheckedBodyRangeV1& levels,
    std::size_t level_index,
    bool bid,
    const MarketDecoderLimitsV1& limits,
    std::uint64_t* quality_flags,
    std::uint64_t* market_notices,
    BookLevelV1* retained_level,
    BestQueueV1* best_queue) noexcept {
    const std::size_t level_offset =
        levels.start + level_index * levels.item_bytes;
    BookLevelV1 local_level{};
    MarketDecodeErrorV1 error = DecodeStrictlyPositivePriceI32(
        view->body(), level_offset + 4U, 3U,
        quality_flags, market_notices, &local_level.price);
    if (error != MarketDecodeErrorV1::kNone) {
        return error;
    }
    error = DecodeQuantityI64(
        view->body(), level_offset + 8U, 3U, true,
        quality_flags, market_notices, &local_level.quantity);
    if (error != MarketDecodeErrorV1::kNone) {
        return error;
    }
    if (!LoadUnsigned(
            view->body(), level_offset + 16U,
            &local_level.order_count)) {
        return MarketDecodeErrorV1::kTruncated;
    }
    local_level.order_count_valid = true;

    CheckedBodyRangeV1 queue;
    const CheckedBodyErrorV1 body_error = view->ReadList(
        level_offset + 20U,
        16U,
        sizeof(sh::SHL2MarketData),
        limits.maximum_queue_items,
        &queue);
    if (body_error != CheckedBodyErrorV1::kNone) {
        return TranslateBodyError(body_error);
    }
    if (queue.count > local_level.order_count) {
        return MarketDecodeErrorV1::kCountMismatch;
    }
    if (queue.count > kMaximumPublicQueueV1) {
        *quality_flags |=
            QualityBit(QualityFlagV1::kQueueTruncatedTo50);
    }
    for (std::size_t index = 0U; index < queue.count; ++index) {
        QuantityValueV1 quantity{};
        error = DecodeQuantityI64(
            view->body(),
            queue.start + index * queue.item_bytes + 8U,
            3U,
            true,
            quality_flags,
            market_notices,
            &quantity);
        if (error != MarketDecodeErrorV1::kNone) {
            return error;
        }
        if (level_index == 0U && index < kMaximumPublicQueueV1) {
            best_queue->quantities[index] = quantity;
        }
    }
    if (level_index == 0U) {
        best_queue->total_order_count = local_level.order_count;
        best_queue->actual_revealed_count =
            static_cast<std::uint32_t>(queue.count);
        best_queue->retained_count = static_cast<std::uint32_t>(
            std::min(queue.count, kMaximumPublicQueueV1));
    }
    if (retained_level != nullptr) {
        *retained_level = local_level;
    }
    static_cast<void>(bid);
    return MarketDecodeErrorV1::kNone;
}

MarketDecodeErrorV1 DecodeSzLevel(
    CheckedBodyViewV1* view,
    const CheckedBodyRangeV1& levels,
    std::size_t level_index,
    const MarketDecoderLimitsV1& limits,
    std::uint64_t* quality_flags,
    std::uint64_t* market_notices,
    BookLevelV1* retained_level,
    BestQueueV1* best_queue) noexcept {
    const std::size_t level_offset =
        levels.start + level_index * levels.item_bytes;
    BookLevelV1 local_level{};
    MarketDecodeErrorV1 error = DecodeQuantityI64(
        view->body(), level_offset, 0U, false,
        quality_flags, market_notices, &local_level.quantity);
    if (error != MarketDecodeErrorV1::kNone) {
        return error;
    }
    error = DecodeStrictlyPositivePriceI64(
        view->body(), level_offset + 8U, 6U,
        quality_flags, market_notices, &local_level.price);
    if (error != MarketDecodeErrorV1::kNone) {
        return error;
    }
    if (!LoadUnsigned(
            view->body(), level_offset + 16U,
            &local_level.order_count)) {
        return MarketDecodeErrorV1::kTruncated;
    }
    local_level.order_count_valid = true;

    CheckedBodyRangeV1 queue;
    const CheckedBodyErrorV1 body_error = view->ReadList(
        level_offset + 20U,
        8U,
        sizeof(sz::Snapshot300111_v2),
        limits.maximum_queue_items,
        &queue);
    if (body_error != CheckedBodyErrorV1::kNone) {
        return TranslateBodyError(body_error);
    }
    if (queue.count > local_level.order_count) {
        return MarketDecodeErrorV1::kCountMismatch;
    }
    if (queue.count > kMaximumPublicQueueV1) {
        *quality_flags |=
            QualityBit(QualityFlagV1::kQueueTruncatedTo50);
    }
    for (std::size_t index = 0U; index < queue.count; ++index) {
        QuantityValueV1 quantity{};
        error = DecodeQuantityI64(
            view->body(),
            queue.start + index * queue.item_bytes,
            0U,
            false,
            quality_flags,
            market_notices,
            &quantity);
        if (error != MarketDecodeErrorV1::kNone) {
            return error;
        }
        if (level_index == 0U && index < kMaximumPublicQueueV1) {
            best_queue->quantities[index] = quantity;
        }
    }
    if (level_index == 0U) {
        best_queue->total_order_count = local_level.order_count;
        best_queue->actual_revealed_count =
            static_cast<std::uint32_t>(queue.count);
        best_queue->retained_count = static_cast<std::uint32_t>(
            std::min(queue.count, kMaximumPublicQueueV1));
    }
    if (retained_level != nullptr) {
        *retained_level = local_level;
    }
    return MarketDecodeErrorV1::kNone;
}

void ApplyDepthQuality(
    std::size_t bid_depth,
    std::size_t ask_depth,
    DecodedMarketCommonV1* common) noexcept {
    if (bid_depth < kMaximumPublicDepthV1 ||
        ask_depth < kMaximumPublicDepthV1) {
        common->quality_flags |=
            QualityBit(QualityFlagV1::kSnapshotDepthShort);
    }
    if (bid_depth > kMaximumPublicDepthV1 ||
        ask_depth > kMaximumPublicDepthV1) {
        common->market_notices |= MarketNoticeBitV1(
            MarketNoticeV1::kSnapshotDepthTruncatedTo10);
    }
}

MarketDecodeErrorV1 DecodeShanghaiSnapshot(
    const MarketMessageViewV1& input,
    const MarketDecoderConfigV1& config,
    ShanghaiSnapshotV1* output) {
    CheckedBodyViewV1 view(input.body);
    CheckedBodyErrorV1 body_error =
        view.RequireFixed(sizeof(sh::SHL2MarketData));
    if (body_error != CheckedBodyErrorV1::kNone) {
        return TranslateBodyError(body_error);
    }
    std::uint32_t exchange_time_raw = 0U;
    if (!LoadUnsigned(input.body, 0U, &exchange_time_raw)) {
        return MarketDecodeErrorV1::kTruncated;
    }
    ShanghaiSnapshotV1 decoded{};
    decoded.common = MakeCommon(
        input,
        MarketEventKindV1::kShanghaiSnapshot,
        MarketV1::kShanghai,
        exchange_time_raw);
    MarketDecodeErrorV1 error = ReadText(
        &view, 4U, sizeof(sh::SHL2MarketData),
        config.limits.maximum_text_bytes, true,
        &decoded.common.quality_flags,
        &decoded.common.security_id,
        &decoded.common.security_id_valid);
    if (error != MarketDecodeErrorV1::kNone) {
        return error;
    }
    error = ReadText(
        &view, 38U, sizeof(sh::SHL2MarketData),
        config.limits.maximum_text_bytes, false,
        &decoded.common.quality_flags,
        &decoded.instrument_status,
        &decoded.instrument_status_valid);
    if (error != MarketDecodeErrorV1::kNone) {
        return error;
    }
    if (!LoadI32(input.body, 10U, &decoded.image_status)) {
        return MarketDecodeErrorV1::kTruncated;
    }

    const auto price32 = [&](std::size_t offset,
                             std::uint8_t scale,
                             DecimalValueV1* value) {
        return DecodeStrictlyPositivePriceI32(
            input.body, offset, scale,
            &decoded.common.quality_flags,
            &decoded.common.market_notices, value);
    };
    const auto decimal64 = [&](std::size_t offset,
                               std::uint8_t scale,
                               DecimalValueV1* value) {
        return DecodeDecimalI64(
            input.body, offset, scale,
            &decoded.common.quality_flags, value);
    };
    const auto quantity64 = [&](std::size_t offset,
                                std::uint8_t scale,
                                QuantityValueV1* value) {
        return DecodeQuantityI64(
            input.body, offset, scale, true,
            &decoded.common.quality_flags,
            &decoded.common.market_notices, value);
    };
#define L2FLOW_DECODE_OR_RETURN(expression)                                  \
    do {                                                                     \
        error = (expression);                                                \
        if (error != MarketDecodeErrorV1::kNone) {                          \
            return error;                                                    \
        }                                                                    \
    } while (false)
    L2FLOW_DECODE_OR_RETURN(price32(14U, 3U, &decoded.pre_close_price));
    L2FLOW_DECODE_OR_RETURN(price32(18U, 3U, &decoded.open_price));
    L2FLOW_DECODE_OR_RETURN(price32(22U, 3U, &decoded.high_price));
    L2FLOW_DECODE_OR_RETURN(price32(26U, 3U, &decoded.low_price));
    L2FLOW_DECODE_OR_RETURN(price32(30U, 3U, &decoded.last_price));
    L2FLOW_DECODE_OR_RETURN(price32(34U, 3U, &decoded.close_price));
    if (!LoadUnsigned(input.body, 44U, &decoded.trade_count)) {
        return MarketDecodeErrorV1::kTruncated;
    }
    L2FLOW_DECODE_OR_RETURN(quantity64(48U, 3U, &decoded.trade_volume));
    L2FLOW_DECODE_OR_RETURN(decimal64(56U, 5U, &decoded.turnover));
    L2FLOW_DECODE_OR_RETURN(quantity64(64U, 3U, &decoded.total_bid_volume));
    L2FLOW_DECODE_OR_RETURN(price32(
        72U, 3U, &decoded.weighted_average_bid_price));
    L2FLOW_DECODE_OR_RETURN(price32(
        76U, 3U, &decoded.alternate_weighted_average_bid_price));
    L2FLOW_DECODE_OR_RETURN(quantity64(80U, 3U, &decoded.total_ask_volume));
    L2FLOW_DECODE_OR_RETURN(price32(
        88U, 3U, &decoded.weighted_average_ask_price));
    L2FLOW_DECODE_OR_RETURN(price32(
        92U, 3U, &decoded.alternate_weighted_average_ask_price));
    if (!LoadUnsigned(
            input.body, 96U, &decoded.vendor_etf_buy_count.raw)) {
        return MarketDecodeErrorV1::kTruncated;
    }
    L2FLOW_DECODE_OR_RETURN(LoadQuantityI64Raw(
        input.body, 100U, 3U, true,
        &decoded.common.quality_flags,
        &decoded.vendor_etf_buy_quantity));
    L2FLOW_DECODE_OR_RETURN(LoadDecimalI64Raw(
        input.body, 108U, 5U,
        &decoded.common.quality_flags,
        &decoded.vendor_etf_buy_amount));
    if (!LoadUnsigned(
            input.body, 116U, &decoded.vendor_etf_sell_count.raw)) {
        return MarketDecodeErrorV1::kTruncated;
    }
    L2FLOW_DECODE_OR_RETURN(LoadQuantityI64Raw(
        input.body, 120U, 3U, true,
        &decoded.common.quality_flags,
        &decoded.vendor_etf_sell_quantity));
    L2FLOW_DECODE_OR_RETURN(LoadDecimalI64Raw(
        input.body, 128U, 5U,
        &decoded.common.quality_flags,
        &decoded.vendor_etf_sell_amount));
    L2FLOW_DECODE_OR_RETURN(LoadDecimalI32Raw(
        input.body, 136U, 4U,
        &decoded.common.quality_flags,
        &decoded.yield_to_maturity));
    L2FLOW_DECODE_OR_RETURN(LoadQuantityI64Raw(
        input.body, 140U, 3U, true,
        &decoded.common.quality_flags,
        &decoded.total_warrant_exercise_quantity));
    L2FLOW_DECODE_OR_RETURN(LoadDecimalI64Raw(
        input.body, 148U, 3U, &decoded.common.quality_flags,
        &decoded.vendor_war_lower_value));
    decoded.common.market_notices |= MarketNoticeBitV1(
        MarketNoticeV1::kVendorWarLowerSemanticsUnknown);
    L2FLOW_DECODE_OR_RETURN(LoadDecimalI64Raw(
        input.body, 156U, 5U, &decoded.common.quality_flags,
        &decoded.vendor_war_upper_value));
    decoded.common.market_notices |= MarketNoticeBitV1(
        MarketNoticeV1::kVendorWarUpperSemanticsUnknown);
    decoded.common.market_notices |= MarketNoticeBitV1(
        MarketNoticeV1::kProductApplicabilityUnknown);
    if (!LoadUnsigned(input.body, 164U, &decoded.withdrawal_buy_count)) {
        return MarketDecodeErrorV1::kTruncated;
    }
    L2FLOW_DECODE_OR_RETURN(quantity64(
        168U, 3U, &decoded.withdrawal_buy_volume));
    L2FLOW_DECODE_OR_RETURN(decimal64(
        176U, 5U, &decoded.withdrawal_buy_amount));
    if (!LoadUnsigned(input.body, 184U, &decoded.withdrawal_sell_count)) {
        return MarketDecodeErrorV1::kTruncated;
    }
    L2FLOW_DECODE_OR_RETURN(quantity64(
        188U, 3U, &decoded.withdrawal_sell_volume));
    L2FLOW_DECODE_OR_RETURN(decimal64(
        196U, 5U, &decoded.withdrawal_sell_amount));
    if (!LoadUnsigned(
            input.body, 204U, &decoded.total_bid_order_count) ||
        !LoadUnsigned(
            input.body, 208U, &decoded.total_ask_order_count) ||
        !LoadUnsigned(
            input.body, 212U, &decoded.maximum_bid_duration.raw) ||
        !LoadUnsigned(
            input.body, 216U, &decoded.maximum_ask_duration.raw)) {
        return MarketDecodeErrorV1::kTruncated;
    }
    decoded.maximum_bid_duration.valid =
        decoded.maximum_bid_duration.raw !=
        std::numeric_limits<std::uint32_t>::max();
    decoded.maximum_ask_duration.valid =
        decoded.maximum_ask_duration.raw !=
        std::numeric_limits<std::uint32_t>::max();
    if (!decoded.maximum_bid_duration.valid ||
        !decoded.maximum_ask_duration.valid) {
        decoded.common.market_notices |= MarketNoticeBitV1(
            MarketNoticeV1::kMaximumDurationUnavailable);
    }
    L2FLOW_DECODE_OR_RETURN(LoadDecimalI32Raw(
        input.body, 244U, 3U,
        &decoded.common.quality_flags, &decoded.iopv));

    CheckedBodyRangeV1 bids;
    body_error = view.ReadList(
        228U, 28U, sizeof(sh::SHL2MarketData),
        config.limits.maximum_depth_items, &bids);
    if (body_error != CheckedBodyErrorV1::kNone) {
        return TranslateBodyError(body_error);
    }
    CheckedBodyRangeV1 asks;
    body_error = view.ReadList(
        236U, 28U, sizeof(sh::SHL2MarketData),
        config.limits.maximum_depth_items, &asks);
    if (body_error != CheckedBodyErrorV1::kNone) {
        return TranslateBodyError(body_error);
    }
    // SHL2MarketData::BidNum/SellNum are independent feed counters, not the
    // MDLListT lengths.  Live continuous-trading messages routinely carry,
    // for example, BidNum=43 and SellNum=74 while both level lists contain
    // the published ten-depth book.  The list descriptors are the only
    // authoritative bounds for the following dynamic level payloads.
    decoded.book.actual_bid_depth = static_cast<std::uint32_t>(bids.count);
    decoded.book.actual_ask_depth = static_cast<std::uint32_t>(asks.count);
    decoded.book.retained_bid_depth = static_cast<std::uint32_t>(
        std::min(bids.count, kMaximumPublicDepthV1));
    decoded.book.retained_ask_depth = static_cast<std::uint32_t>(
        std::min(asks.count, kMaximumPublicDepthV1));
    for (std::size_t index = 0U; index < bids.count; ++index) {
        BookLevelV1* retained =
            index < kMaximumPublicDepthV1
                ? &decoded.book.bids[index]
                : nullptr;
        L2FLOW_DECODE_OR_RETURN(DecodeShLevel(
            &view, bids, index, true, config.limits,
            &decoded.common.quality_flags,
            &decoded.common.market_notices, retained,
            &decoded.book.bid1_queue));
    }
    for (std::size_t index = 0U; index < asks.count; ++index) {
        BookLevelV1* retained =
            index < kMaximumPublicDepthV1
                ? &decoded.book.asks[index]
                : nullptr;
        L2FLOW_DECODE_OR_RETURN(DecodeShLevel(
            &view, asks, index, false, config.limits,
            &decoded.common.quality_flags,
            &decoded.common.market_notices, retained,
            &decoded.book.ask1_queue));
    }
    ApplyDepthQuality(bids.count, asks.count, &decoded.common);
    ApplyBodyNotices(view, &decoded.common);
    MarkInstrumentPendingObservedBinding(&decoded.common);
    *output = std::move(decoded);
    return MarketDecodeErrorV1::kNone;
#undef L2FLOW_DECODE_OR_RETURN
}

TradingPhaseV1 ParseShPhase(std::string_view value) noexcept {
    if (value == "START") {
        return TradingPhaseV1::kStart;
    }
    if (value == "OCALL") {
        return TradingPhaseV1::kOpeningCall;
    }
    if (value == "TRADE") {
        return TradingPhaseV1::kContinuous;
    }
    if (value == "SUSP") {
        return TradingPhaseV1::kSuspended;
    }
    if (value == "CCALL") {
        return TradingPhaseV1::kClosingCall;
    }
    if (value == "CLOSE") {
        return TradingPhaseV1::kClosed;
    }
    if (value == "ENDTR") {
        return TradingPhaseV1::kEnd;
    }
    return TradingPhaseV1::kUnknown;
}

MarketDecodeErrorV1 DecodeShanghaiTick(
    const MarketMessageViewV1& input,
    const MarketDecoderConfigV1& config,
    std::map<std::string, TradingPhaseV1>* phases,
    ShanghaiTickV1* output) {
    CheckedBodyViewV1 view(input.body);
    CheckedBodyErrorV1 body_error =
        view.RequireFixed(sizeof(sh::NGTSTick));
    if (body_error != CheckedBodyErrorV1::kNone) {
        return TranslateBodyError(body_error);
    }
    std::uint32_t exchange_time_raw = 0U;
    if (!LoadUnsigned(input.body, 18U, &exchange_time_raw)) {
        return MarketDecodeErrorV1::kTruncated;
    }
    ShanghaiTickV1 decoded{};
    decoded.common = MakeCommon(
        input,
        MarketEventKindV1::kShanghaiTick,
        MarketV1::kShanghai,
        exchange_time_raw);
    MarketDecodeErrorV1 error = ReadText(
        &view, 12U, sizeof(sh::NGTSTick),
        config.limits.maximum_text_bytes, true,
        &decoded.common.quality_flags,
        &decoded.common.security_id,
        &decoded.common.security_id_valid);
    if (error != MarketDecodeErrorV1::kNone) {
        return error;
    }
    error = ReadText(
        &view, 22U, sizeof(sh::NGTSTick),
        config.limits.maximum_text_bytes, true,
        &decoded.common.quality_flags,
        &decoded.raw_type,
        &decoded.raw_type_valid);
    if (error != MarketDecodeErrorV1::kNone) {
        return error;
    }
    error = ReadText(
        &view, 64U, sizeof(sh::NGTSTick),
        config.limits.maximum_text_bytes, true,
        &decoded.common.quality_flags,
        &decoded.raw_tick_flag,
        &decoded.raw_tick_flag_valid);
    if (error != MarketDecodeErrorV1::kNone) {
        return error;
    }
    if (!LoadI64(input.body, 0U, &decoded.business_index) ||
        !LoadI32(input.body, 8U, &decoded.channel) ||
        !LoadI64(
            input.body, 28U, &decoded.fields.buy_order_id) ||
        !LoadI64(
            input.body, 36U, &decoded.fields.sell_order_id)) {
        return MarketDecodeErrorV1::kTruncated;
    }
    if (decoded.business_index <= 0) {
        decoded.common.market_notices |= MarketNoticeBitV1(
            MarketNoticeV1::kEventSequenceDomainInvalid);
    }
    if (decoded.fields.buy_order_id < 0 ||
        decoded.fields.sell_order_id < 0) {
        decoded.common.market_notices |= MarketNoticeBitV1(
            MarketNoticeV1::kOrderReferenceDomainInvalid);
    }
    // Load the exact wire values first.  Normalization is action-dependent:
    // fields which the vendor validity matrix marks meaningless must not be
    // able to reject an otherwise valid D/S event merely because their raw
    // placeholder would overflow p6.
    error = LoadDecimalI32Raw(
        input.body, 44U, 3U,
        &decoded.common.quality_flags,
        &decoded.fields.price);
    if (error != MarketDecodeErrorV1::kNone) {
        return error;
    }
    error = DecodeQuantityI64(
        input.body, 48U, 0U, false,
        &decoded.common.quality_flags,
        &decoded.common.market_notices,
        &decoded.fields.quantity);
    if (error != MarketDecodeErrorV1::kNone) {
        return error;
    }
    error = LoadDecimalI64Raw(
        input.body, 56U, 3U,
        &decoded.common.quality_flags,
        &decoded.fields.trade_amount);
    if (error != MarketDecodeErrorV1::kNone) {
        return error;
    }
    if (decoded.common.exchange_time.valid) {
        decoded.fields.validity_bitmap |= kTickExchangeTimeValidV1;
    }

    const auto set_phase_from_history = [&]() {
        if (!decoded.common.security_id_valid) {
            return;
        }
        const auto found = phases->find(decoded.common.security_id);
        if (found != phases->end() &&
            found->second != TradingPhaseV1::kUnknown) {
            decoded.fields.phase = found->second;
            decoded.fields.validity_bitmap |= kTickPhaseValidV1;
        }
    };
    const auto set_order_side = [&]() {
        if (decoded.raw_tick_flag == "B") {
            decoded.fields.side = SideV1::kBuy;
            decoded.fields.primary_order_id =
                decoded.fields.buy_order_id;
        } else if (decoded.raw_tick_flag == "S") {
            decoded.fields.side = SideV1::kSell;
            decoded.fields.primary_order_id =
                decoded.fields.sell_order_id;
        } else {
            decoded.common.quality_flags |=
                QualityBit(QualityFlagV1::kUnknownEnum);
            return;
        }
        decoded.fields.validity_bitmap |= kTickSideValidV1;
        if (decoded.fields.primary_order_id > 0) {
            decoded.fields.validity_bitmap |=
                kTickPrimaryOrderIdValidV1;
        }
    };

    if (decoded.raw_type == "A") {
        decoded.fields.action = TickActionV1::kAdd;
        error = NormalizeStrictlyPositivePriceP6(
            &decoded.fields.price,
            &decoded.common.market_notices);
        if (error != MarketDecodeErrorV1::kNone) {
            return error;
        }
        if (decoded.fields.price.valid) {
            decoded.fields.validity_bitmap |= kTickPriceValidV1;
        }
        if (decoded.fields.quantity.valid) {
            decoded.fields.validity_bitmap |= kTickQuantityValidV1;
        }
        set_order_side();
        if (decoded.fields.trade_amount.is_null) {
            // NULL_VALUE_PRESENT already describes this case; null is not a
            // mathematical non-integrality error.
        } else if (decoded.fields.trade_amount.raw < 0) {
            decoded.common.market_notices |= MarketNoticeBitV1(
                MarketNoticeV1::kMatchedQuantityDomainInvalid);
        } else if (decoded.fields.trade_amount.raw % 1'000 == 0) {
            decoded.fields.matched_quantity.raw =
                decoded.fields.trade_amount.raw / 1'000;
            decoded.fields.matched_quantity.scale = 0U;
            decoded.fields.matched_quantity.valid = true;
            decoded.fields.validity_bitmap |=
                kTickMatchedQuantityValidV1;
        } else {
            decoded.common.quality_flags |= QualityBit(
                QualityFlagV1::kNonIntegralMatchedQty);
        }
        // For A, the vendor field named TradeMoney carries matched quantity;
        // it is not a valid trade-amount field.
        decoded.fields.trade_amount.valid = false;
        set_phase_from_history();
    } else if (decoded.raw_type == "D") {
        decoded.fields.action = TickActionV1::kCancel;
        decoded.fields.price.valid = false;
        decoded.fields.trade_amount.valid = false;
        if (decoded.fields.quantity.valid) {
            decoded.fields.validity_bitmap |= kTickQuantityValidV1;
        }
        set_order_side();
        set_phase_from_history();
    } else if (decoded.raw_type == "T") {
        decoded.fields.action = TickActionV1::kTrade;
        error = NormalizeStrictlyPositivePriceP6(
            &decoded.fields.price,
            &decoded.common.market_notices);
        if (error != MarketDecodeErrorV1::kNone) {
            return error;
        }
        if (decoded.fields.trade_amount.is_null) {
            // Preserve the source null sentinel and its existing quality bit.
        } else if (decoded.fields.trade_amount.raw < 0) {
            // A transaction amount cannot be negative. Retain the exact raw
            // diagnostic, but reject it before p6 multiplication so a
            // negative extreme is not promoted to a valid amount or turned
            // into a decoder-wide overflow failure.
            decoded.common.market_notices |= MarketNoticeBitV1(
                MarketNoticeV1::kTradeAmountDomainInvalid);
        } else {
            error = NormalizeDecimalP6(&decoded.fields.trade_amount);
            if (error != MarketDecodeErrorV1::kNone) {
                return error;
            }
        }
        if (decoded.fields.price.valid) {
            decoded.fields.validity_bitmap |= kTickPriceValidV1;
        }
        if (decoded.fields.quantity.valid) {
            decoded.fields.validity_bitmap |= kTickQuantityValidV1;
        }
        if (decoded.fields.trade_amount.valid) {
            decoded.fields.validity_bitmap |=
                kTickTradeAmountValidV1;
        }
        if (decoded.fields.buy_order_id > 0) {
            decoded.fields.validity_bitmap |= kTickBuyOrderIdValidV1;
        }
        if (decoded.fields.sell_order_id > 0) {
            decoded.fields.validity_bitmap |= kTickSellOrderIdValidV1;
        }
        if (decoded.raw_tick_flag == "B") {
            decoded.fields.aggressor = AggressorV1::kBuy;
        } else if (decoded.raw_tick_flag == "S") {
            decoded.fields.aggressor = AggressorV1::kSell;
        } else if (decoded.raw_tick_flag == "N") {
            decoded.fields.aggressor = AggressorV1::kNeutral;
        } else {
            decoded.common.quality_flags |=
                QualityBit(QualityFlagV1::kUnknownEnum);
        }
        if (decoded.fields.aggressor != AggressorV1::kUnknown) {
            decoded.fields.validity_bitmap |= kTickAggressorValidV1;
        }
        set_phase_from_history();
    } else if (decoded.raw_type == "S") {
        decoded.fields.action = TickActionV1::kStatus;
        decoded.fields.price.valid = false;
        decoded.fields.quantity.valid = false;
        decoded.fields.trade_amount.valid = false;
        decoded.fields.phase = ParseShPhase(decoded.raw_tick_flag);
        if (decoded.fields.phase == TradingPhaseV1::kUnknown) {
            decoded.common.quality_flags |=
                QualityBit(QualityFlagV1::kUnknownEnum);
        } else {
            decoded.fields.validity_bitmap |= kTickPhaseValidV1;
        }
        if (decoded.fields.phase != TradingPhaseV1::kUnknown &&
            decoded.common.security_id_valid &&
            decoded.raw_tick_flag_valid) {
            const auto existing = phases->find(decoded.common.security_id);
            if (existing != phases->end()) {
                existing->second = decoded.fields.phase;
            } else {
                if (phases->size() >=
                    config.limits.maximum_phase_products) {
                    return MarketDecodeErrorV1::
                        kPhaseProductLimitExceeded;
                }
                phases->emplace(
                    decoded.common.security_id, decoded.fields.phase);
            }
        }
    } else {
        decoded.fields.price.valid = false;
        decoded.fields.quantity.valid = false;
        decoded.fields.trade_amount.valid = false;
        decoded.common.quality_flags |=
            QualityBit(QualityFlagV1::kUnknownEnum);
        set_phase_from_history();
    }

    ApplyBodyNotices(view, &decoded.common);
    MarkInstrumentPendingObservedBinding(&decoded.common);
    *output = std::move(decoded);
    return MarketDecodeErrorV1::kNone;
}

MarketDecodeErrorV1 DecodeShenzhenSnapshot(
    const MarketMessageViewV1& input,
    const MarketDecoderConfigV1& config,
    ShenzhenSnapshotV1* output) {
    CheckedBodyViewV1 view(input.body);
    CheckedBodyErrorV1 body_error =
        view.RequireFixed(sizeof(sz::Snapshot300111_v2));
    if (body_error != CheckedBodyErrorV1::kNone) {
        return TranslateBodyError(body_error);
    }
    std::uint32_t exchange_time_raw = 0U;
    if (!LoadUnsigned(input.body, 0U, &exchange_time_raw)) {
        return MarketDecodeErrorV1::kTruncated;
    }
    ShenzhenSnapshotV1 decoded{};
    decoded.common = MakeCommon(
        input,
        MarketEventKindV1::kShenzhenSnapshot,
        MarketV1::kShenzhen,
        exchange_time_raw);
    MarketDecodeErrorV1 error = ReadText(
        &view, 8U, sizeof(sz::Snapshot300111_v2),
        config.limits.maximum_text_bytes, true,
        &decoded.common.quality_flags,
        &decoded.common.md_stream_id,
        &decoded.common.md_stream_id_valid);
    if (error != MarketDecodeErrorV1::kNone) {
        return error;
    }
    error = ReadText(
        &view, 14U, sizeof(sz::Snapshot300111_v2),
        config.limits.maximum_text_bytes, true,
        &decoded.common.quality_flags,
        &decoded.common.security_id,
        &decoded.common.security_id_valid);
    if (error != MarketDecodeErrorV1::kNone) {
        return error;
    }
    error = ReadText(
        &view, 20U, sizeof(sz::Snapshot300111_v2),
        config.limits.maximum_text_bytes, true,
        &decoded.common.quality_flags,
        &decoded.common.security_id_source,
        &decoded.common.security_id_source_valid);
    if (error != MarketDecodeErrorV1::kNone) {
        return error;
    }
    error = ReadText(
        &view, 26U, sizeof(sz::Snapshot300111_v2),
        config.limits.maximum_text_bytes, true,
        &decoded.common.quality_flags,
        &decoded.trading_phase_code,
        &decoded.trading_phase_code_valid);
    if (error != MarketDecodeErrorV1::kNone) {
        return error;
    }
    if (!LoadUnsigned(input.body, 4U, &decoded.channel) ||
        !LoadI64(input.body, 40U, &decoded.trade_count)) {
        return MarketDecodeErrorV1::kTruncated;
    }
    const auto decimal64 = [&](std::size_t offset,
                               std::uint8_t scale,
                               DecimalValueV1* value) {
        return DecodeDecimalI64(
            input.body, offset, scale,
            &decoded.common.quality_flags, value);
    };
    const auto price64 = [&](std::size_t offset,
                             std::uint8_t scale,
                             DecimalValueV1* value) {
        return DecodeStrictlyPositivePriceI64(
            input.body, offset, scale,
            &decoded.common.quality_flags,
            &decoded.common.market_notices, value);
    };
    const auto quantity64 = [&](std::size_t offset,
                                QuantityValueV1* value) {
        return DecodeQuantityI64(
            input.body, offset, 0U, false,
            &decoded.common.quality_flags,
            &decoded.common.market_notices, value);
    };
#define L2FLOW_DECODE_OR_RETURN(expression)                                  \
    do {                                                                     \
        error = (expression);                                                \
        if (error != MarketDecodeErrorV1::kNone) {                          \
            return error;                                                    \
        }                                                                    \
    } while (false)
    L2FLOW_DECODE_OR_RETURN(price64(32U, 4U, &decoded.pre_close_price));
    L2FLOW_DECODE_OR_RETURN(quantity64(48U, &decoded.volume));
    L2FLOW_DECODE_OR_RETURN(decimal64(56U, 4U, &decoded.turnover));
    L2FLOW_DECODE_OR_RETURN(price64(64U, 6U, &decoded.last_price));
    L2FLOW_DECODE_OR_RETURN(price64(72U, 6U, &decoded.open_price));
    L2FLOW_DECODE_OR_RETURN(price64(80U, 6U, &decoded.high_price));
    L2FLOW_DECODE_OR_RETURN(price64(88U, 6U, &decoded.low_price));
    L2FLOW_DECODE_OR_RETURN(decimal64(96U, 6U, &decoded.price_change_1));
    L2FLOW_DECODE_OR_RETURN(decimal64(104U, 6U, &decoded.price_change_2));
    L2FLOW_DECODE_OR_RETURN(LoadDecimalI64Raw(
        input.body, 112U, 6U,
        &decoded.common.quality_flags, &decoded.pe_ratio_1));
    L2FLOW_DECODE_OR_RETURN(LoadDecimalI64Raw(
        input.body, 120U, 6U,
        &decoded.common.quality_flags, &decoded.pe_ratio_2));
    L2FLOW_DECODE_OR_RETURN(LoadDecimalI64Raw(
        input.body, 128U, 6U,
        &decoded.common.quality_flags, &decoded.pre_close_iopv));
    L2FLOW_DECODE_OR_RETURN(LoadDecimalI64Raw(
        input.body, 136U, 6U,
        &decoded.common.quality_flags, &decoded.iopv));
    L2FLOW_DECODE_OR_RETURN(quantity64(144U, &decoded.total_ask_quantity));
    L2FLOW_DECODE_OR_RETURN(price64(
        152U, 6U, &decoded.weighted_average_ask_price));
    L2FLOW_DECODE_OR_RETURN(quantity64(160U, &decoded.total_bid_quantity));
    L2FLOW_DECODE_OR_RETURN(price64(
        168U, 6U, &decoded.weighted_average_bid_price));
    // No authoritative sentinel table is present in this decoder.
    // Preserve exact raw values but do not publish them as factor-safe finite
    // prices merely because they fit int64/p6.  Versioned reference policy
    // must resolve kUnknown before a downstream calculator uses them.
    L2FLOW_DECODE_OR_RETURN(LoadDecimalI64Raw(
        input.body, 176U, 6U, &decoded.common.quality_flags,
        &decoded.high_limit_price));
    L2FLOW_DECODE_OR_RETURN(LoadDecimalI64Raw(
        input.body, 184U, 6U, &decoded.common.quality_flags,
        &decoded.low_limit_price));
    decoded.common.market_notices |= MarketNoticeBitV1(
        MarketNoticeV1::kLimitPriceSemanticsUnknown);
    L2FLOW_DECODE_OR_RETURN(LoadQuantityI64Raw(
        input.body, 192U, 0U, false,
        &decoded.common.quality_flags, &decoded.open_interest));
    L2FLOW_DECODE_OR_RETURN(LoadDecimalI64Raw(
        input.body, 200U, 6U, &decoded.common.quality_flags,
        &decoded.vendor_opt_premium_ratio));
    decoded.common.market_notices |= MarketNoticeBitV1(
        MarketNoticeV1::kVendorOptPremiumRatioSemanticsUnknown);
    decoded.common.market_notices |= MarketNoticeBitV1(
        MarketNoticeV1::kProductApplicabilityUnknown);

    CheckedBodyRangeV1 bids;
    body_error = view.ReadList(
        208U, 28U, sizeof(sz::Snapshot300111_v2),
        config.limits.maximum_depth_items, &bids);
    if (body_error != CheckedBodyErrorV1::kNone) {
        return TranslateBodyError(body_error);
    }
    CheckedBodyRangeV1 asks;
    body_error = view.ReadList(
        216U, 28U, sizeof(sz::Snapshot300111_v2),
        config.limits.maximum_depth_items, &asks);
    if (body_error != CheckedBodyErrorV1::kNone) {
        return TranslateBodyError(body_error);
    }
    decoded.book.actual_bid_depth = static_cast<std::uint32_t>(bids.count);
    decoded.book.actual_ask_depth = static_cast<std::uint32_t>(asks.count);
    decoded.book.retained_bid_depth = static_cast<std::uint32_t>(
        std::min(bids.count, kMaximumPublicDepthV1));
    decoded.book.retained_ask_depth = static_cast<std::uint32_t>(
        std::min(asks.count, kMaximumPublicDepthV1));
    for (std::size_t index = 0U; index < bids.count; ++index) {
        BookLevelV1* retained =
            index < kMaximumPublicDepthV1
                ? &decoded.book.bids[index]
                : nullptr;
        L2FLOW_DECODE_OR_RETURN(DecodeSzLevel(
            &view, bids, index, config.limits,
            &decoded.common.quality_flags,
            &decoded.common.market_notices, retained,
            &decoded.book.bid1_queue));
    }
    for (std::size_t index = 0U; index < asks.count; ++index) {
        BookLevelV1* retained =
            index < kMaximumPublicDepthV1
                ? &decoded.book.asks[index]
                : nullptr;
        L2FLOW_DECODE_OR_RETURN(DecodeSzLevel(
            &view, asks, index, config.limits,
            &decoded.common.quality_flags,
            &decoded.common.market_notices, retained,
            &decoded.book.ask1_queue));
    }
    ApplyDepthQuality(bids.count, asks.count, &decoded.common);
    ApplyBodyNotices(view, &decoded.common);
    MarkInstrumentPendingObservedBinding(&decoded.common);
    *output = std::move(decoded);
    return MarketDecodeErrorV1::kNone;
#undef L2FLOW_DECODE_OR_RETURN
}

MarketDecodeErrorV1 ReadSzIdentity(
    CheckedBodyViewV1* view,
    std::size_t fixed_bytes,
    std::size_t stream_offset,
    std::size_t security_offset,
    std::size_t source_offset,
    const MarketDecoderConfigV1& config,
    DecodedMarketCommonV1* common) {
    MarketDecodeErrorV1 error = ReadText(
        view, stream_offset, fixed_bytes,
        config.limits.maximum_text_bytes, true,
        &common->quality_flags, &common->md_stream_id,
        &common->md_stream_id_valid);
    if (error != MarketDecodeErrorV1::kNone) {
        return error;
    }
    error = ReadText(
        view, security_offset, fixed_bytes,
        config.limits.maximum_text_bytes, true,
        &common->quality_flags, &common->security_id,
        &common->security_id_valid);
    if (error != MarketDecodeErrorV1::kNone) {
        return error;
    }
    return ReadText(
        view, source_offset, fixed_bytes,
        config.limits.maximum_text_bytes, true,
        &common->quality_flags, &common->security_id_source,
        &common->security_id_source_valid);
}

SideV1 DecodeSzSide(std::int32_t raw) noexcept {
    switch (raw) {
        case 49:
            return SideV1::kBuy;
        case 50:
            return SideV1::kSell;
        case 71:
            return SideV1::kBorrow;
        case 70:
            return SideV1::kLend;
        default:
            return SideV1::kUnknown;
    }
}

OrderTypeV1 DecodeSzOrderType(std::int32_t raw) noexcept {
    switch (raw) {
        case 49:
            return OrderTypeV1::kMarket;
        case 50:
            return OrderTypeV1::kLimit;
        case 85:
            return OrderTypeV1::kSameSideBest;
        default:
            return OrderTypeV1::kUnknown;
    }
}

MarketDecodeErrorV1 DecodeShenzhenOrder(
    const MarketMessageViewV1& input,
    const MarketDecoderConfigV1& config,
    ShenzhenOrderV1* output) {
    CheckedBodyViewV1 view(input.body);
    const CheckedBodyErrorV1 body_error =
        view.RequireFixed(sizeof(sz::Order300192_v2));
    if (body_error != CheckedBodyErrorV1::kNone) {
        return TranslateBodyError(body_error);
    }
    std::uint32_t exchange_time_raw = 0U;
    if (!LoadUnsigned(input.body, 50U, &exchange_time_raw)) {
        return MarketDecodeErrorV1::kTruncated;
    }
    ShenzhenOrderV1 decoded{};
    decoded.common = MakeCommon(
        input,
        MarketEventKindV1::kShenzhenOrder,
        MarketV1::kShenzhen,
        exchange_time_raw);
    MarketDecodeErrorV1 error = ReadSzIdentity(
        &view, sizeof(sz::Order300192_v2),
        12U, 18U, 24U, config, &decoded.common);
    if (error != MarketDecodeErrorV1::kNone) {
        return error;
    }
    if (!LoadUnsigned(input.body, 0U, &decoded.channel) ||
        !LoadI64(input.body, 4U, &decoded.application_sequence) ||
        !LoadI64(input.body, 38U, &decoded.fields.quantity.raw) ||
        !LoadI32(input.body, 46U, &decoded.raw_side) ||
        !LoadI32(input.body, 54U, &decoded.raw_order_type)) {
        return MarketDecodeErrorV1::kTruncated;
    }
    decoded.fields.quantity.scale = 0U;
    decoded.fields.quantity.valid = decoded.fields.quantity.raw >= 0;
    if (!decoded.fields.quantity.valid) {
        decoded.common.market_notices |= MarketNoticeBitV1(
            MarketNoticeV1::kQuantityDomainInvalid);
    }
    decoded.fields.action = TickActionV1::kAdd;
    decoded.fields.primary_order_id = decoded.application_sequence;
    if (decoded.fields.quantity.valid) {
        decoded.fields.validity_bitmap |= kTickQuantityValidV1;
    }
    if (decoded.application_sequence > 0) {
        decoded.fields.validity_bitmap |= kTickPrimaryOrderIdValidV1;
    } else {
        // ApplSeqNum is both the event sequence and the primary order ID for
        // this message family, so a nonpositive value invalidates both roles.
        decoded.common.market_notices |=
            MarketNoticeBitV1(
                MarketNoticeV1::kEventSequenceDomainInvalid) |
            MarketNoticeBitV1(
                MarketNoticeV1::kOrderReferenceDomainInvalid);
    }
    if (decoded.common.exchange_time.valid) {
        decoded.fields.validity_bitmap |= kTickExchangeTimeValidV1;
    }
    error = LoadDecimalI64Raw(
        input.body, 30U, 4U,
        &decoded.common.quality_flags,
        &decoded.fields.price);
    if (error != MarketDecodeErrorV1::kNone) {
        return error;
    }
    decoded.fields.side = DecodeSzSide(decoded.raw_side);
    if (decoded.fields.side == SideV1::kUnknown) {
        decoded.common.quality_flags |=
            QualityBit(QualityFlagV1::kUnknownEnum);
    } else {
        decoded.fields.validity_bitmap |= kTickSideValidV1;
    }
    decoded.fields.order_type = DecodeSzOrderType(
        decoded.raw_order_type);
    if (decoded.fields.order_type == OrderTypeV1::kUnknown) {
        decoded.common.quality_flags |=
            QualityBit(QualityFlagV1::kUnknownEnum);
    } else {
        decoded.fields.validity_bitmap |= kTickOrderTypeValidV1;
    }
    if (decoded.fields.order_type == OrderTypeV1::kLimit) {
        error = NormalizeStrictlyPositivePriceP6(
            &decoded.fields.price,
            &decoded.common.market_notices);
        if (error != MarketDecodeErrorV1::kNone) {
            return error;
        }
        if (decoded.fields.price.valid) {
            decoded.fields.validity_bitmap |= kTickPriceValidV1;
        }
    }
    ApplyBodyNotices(view, &decoded.common);
    MarkInstrumentPendingObservedBinding(&decoded.common);
    *output = std::move(decoded);
    return MarketDecodeErrorV1::kNone;
}

MarketDecodeErrorV1 DecodeShenzhenTransaction(
    const MarketMessageViewV1& input,
    const MarketDecoderConfigV1& config,
    ShenzhenTransactionV1* output) {
    CheckedBodyViewV1 view(input.body);
    const CheckedBodyErrorV1 body_error =
        view.RequireFixed(sizeof(sz::Transaction300191_v2));
    if (body_error != CheckedBodyErrorV1::kNone) {
        return TranslateBodyError(body_error);
    }
    std::uint32_t exchange_time_raw = 0U;
    if (!LoadUnsigned(input.body, 66U, &exchange_time_raw)) {
        return MarketDecodeErrorV1::kTruncated;
    }
    ShenzhenTransactionV1 decoded{};
    decoded.common = MakeCommon(
        input,
        MarketEventKindV1::kShenzhenTransaction,
        MarketV1::kShenzhen,
        exchange_time_raw);
    MarketDecodeErrorV1 error = ReadSzIdentity(
        &view, sizeof(sz::Transaction300191_v2),
        12U, 34U, 40U, config, &decoded.common);
    if (error != MarketDecodeErrorV1::kNone) {
        return error;
    }
    if (!LoadUnsigned(input.body, 0U, &decoded.channel) ||
        !LoadI64(input.body, 4U, &decoded.application_sequence) ||
        !LoadI64(input.body, 18U, &decoded.fields.buy_order_id) ||
        !LoadI64(input.body, 26U, &decoded.fields.sell_order_id) ||
        !LoadI64(input.body, 54U, &decoded.fields.quantity.raw) ||
        !LoadI32(input.body, 62U, &decoded.raw_execution_type)) {
        return MarketDecodeErrorV1::kTruncated;
    }
    if (decoded.application_sequence <= 0) {
        decoded.common.market_notices |= MarketNoticeBitV1(
            MarketNoticeV1::kEventSequenceDomainInvalid);
    }
    if (decoded.fields.buy_order_id < 0 ||
        decoded.fields.sell_order_id < 0) {
        decoded.common.market_notices |= MarketNoticeBitV1(
            MarketNoticeV1::kOrderReferenceDomainInvalid);
    }
    decoded.fields.quantity.scale = 0U;
    decoded.fields.quantity.valid = decoded.fields.quantity.raw >= 0;
    if (!decoded.fields.quantity.valid) {
        decoded.common.market_notices |= MarketNoticeBitV1(
            MarketNoticeV1::kQuantityDomainInvalid);
    }
    error = LoadDecimalI64Raw(
        input.body, 46U, 4U,
        &decoded.common.quality_flags,
        &decoded.fields.price);
    if (error != MarketDecodeErrorV1::kNone) {
        return error;
    }
    if (decoded.common.exchange_time.valid) {
        decoded.fields.validity_bitmap |= kTickExchangeTimeValidV1;
    }
    const auto publish_known_order_ids = [&]() {
        if (decoded.fields.buy_order_id > 0) {
            decoded.fields.validity_bitmap |= kTickBuyOrderIdValidV1;
        }
        if (decoded.fields.sell_order_id > 0) {
            decoded.fields.validity_bitmap |= kTickSellOrderIdValidV1;
        }
    };

    if (decoded.raw_execution_type == 70) {
        decoded.fields.action = TickActionV1::kTrade;
        publish_known_order_ids();
        error = NormalizeStrictlyPositivePriceP6(
            &decoded.fields.price,
            &decoded.common.market_notices);
        if (error != MarketDecodeErrorV1::kNone) {
            return error;
        }
        if (decoded.fields.quantity.valid) {
            decoded.fields.validity_bitmap |= kTickQuantityValidV1;
        }
        if (decoded.fields.price.valid) {
            decoded.fields.validity_bitmap |= kTickPriceValidV1;
        }
    } else if (decoded.raw_execution_type == 52) {
        decoded.fields.action = TickActionV1::kCancel;
        publish_known_order_ids();
        if (decoded.fields.quantity.valid) {
            decoded.fields.validity_bitmap |= kTickQuantityValidV1;
        }
        // Zero means no referenced order for SZ transactions; negative IDs
        // are retained as raw diagnostics but never count as present.
        const bool has_bid = decoded.fields.buy_order_id > 0;
        const bool has_ask = decoded.fields.sell_order_id > 0;
        if (has_bid != has_ask) {
            decoded.fields.primary_order_id =
                has_bid ? decoded.fields.buy_order_id
                        : decoded.fields.sell_order_id;
            decoded.fields.side =
                has_bid ? SideV1::kBuy : SideV1::kSell;
            decoded.fields.validity_bitmap |=
                kTickPrimaryOrderIdValidV1 | kTickSideValidV1;
        } else {
            decoded.common.quality_flags |= QualityBit(
                QualityFlagV1::kAmbiguousOrderReference);
        }
        // Cancellation price is semantically invalid regardless of raw 0 or
        // any other supplied value; kTickPriceValidV1 remains clear.
    } else {
        decoded.fields.quantity.valid = false;
        decoded.common.quality_flags |=
            QualityBit(QualityFlagV1::kUnknownEnum);
    }
    ApplyBodyNotices(view, &decoded.common);
    MarkInstrumentPendingObservedBinding(&decoded.common);
    *output = std::move(decoded);
    return MarketDecodeErrorV1::kNone;
}

bool RecognizedCoreMessage(
    std::uint8_t service_id,
    std::uint16_t message_id) noexcept {
    return (service_id == sh::SHL2MarketData::ServiceID &&
            (message_id == sh::SHL2MarketData::MessageID ||
             message_id == sh::NGTSTick::MessageID)) ||
           (service_id == sz::Snapshot300111_v2::ServiceID &&
            (message_id == sz::Snapshot300111_v2::MessageID ||
             message_id == sz::Order300192_v2::MessageID ||
             message_id == sz::Transaction300191_v2::MessageID));
}

[[nodiscard]] bool ReadU16Little(
    std::span<const std::byte> body,
    std::size_t offset,
    std::uint16_t* output) noexcept {
    if (output == nullptr || offset > body.size() ||
        sizeof(std::uint16_t) > body.size() - offset) {
        return false;
    }
    *output =
        std::to_integer<std::uint16_t>(body[offset]) |
        static_cast<std::uint16_t>(
            std::to_integer<std::uint16_t>(body[offset + 1U]) << 8U);
    return true;
}

[[nodiscard]] bool ReadU32Little(
    std::span<const std::byte> body,
    std::size_t offset,
    std::uint32_t* output) noexcept {
    if (output == nullptr || offset > body.size() ||
        sizeof(std::uint32_t) > body.size() - offset) {
        return false;
    }
    std::uint32_t value = 0U;
    for (std::size_t index = 0U; index < sizeof(value); ++index) {
        value |= std::to_integer<std::uint32_t>(body[offset + index])
                 << static_cast<unsigned int>(index * 8U);
    }
    *output = value;
    return true;
}

[[nodiscard]] MarketDecodeErrorV1 ReadObservedKeyString(
    std::span<const std::byte> body,
    std::size_t descriptor_offset,
    std::size_t fixed_bytes,
    std::size_t maximum_text_bytes,
    std::span<const std::byte>* output) noexcept {
    if (output == nullptr) {
        return MarketDecodeErrorV1::kNullOutput;
    }
    *output = {};
    if (body.size() < fixed_bytes) {
        return MarketDecodeErrorV1::kTruncated;
    }
    std::uint16_t length = 0U;
    std::uint32_t relative_offset = 0U;
    if (!ReadU16Little(body, descriptor_offset, &length) ||
        !ReadU32Little(
            body, descriptor_offset + sizeof(length), &relative_offset)) {
        return MarketDecodeErrorV1::kTruncated;
    }
    const std::size_t size = static_cast<std::size_t>(length);
    if (size == 0U || size > maximum_text_bytes) {
        return size == 0U
                   ? MarketDecodeErrorV1::kTextInvalid
                   : MarketDecodeErrorV1::kCountExceeded;
    }
    if (relative_offset < 6U ||
        static_cast<std::size_t>(relative_offset) >
            std::numeric_limits<std::size_t>::max() - descriptor_offset) {
        return MarketDecodeErrorV1::kOffsetInvalid;
    }
    const std::size_t start =
        descriptor_offset + static_cast<std::size_t>(relative_offset);
    if (start < fixed_bytes || start > body.size() ||
        size > body.size() - start) {
        return MarketDecodeErrorV1::kOffsetInvalid;
    }
    const std::span<const std::byte> value = body.subspan(start, size);
    if (!IsPrintableAscii(value)) {
        return MarketDecodeErrorV1::kTextInvalid;
    }
    *output = value;
    return MarketDecodeErrorV1::kNone;
}

[[nodiscard]] bool SpansOverlap(
    std::span<const std::byte> left,
    std::span<const std::byte> right) noexcept {
    if (left.empty() || right.empty()) {
        return false;
    }
    const std::byte* const left_end = left.data() + left.size();
    const std::byte* const right_end = right.data() + right.size();
    return left.data() < right_end && right.data() < left_end;
}

[[nodiscard]] bool StringEqualsBytes(
    const std::string& text,
    std::span<const std::byte> bytes) noexcept {
    return text.size() == bytes.size() &&
           std::equal(
               text.begin(),
               text.end(),
               reinterpret_cast<const char*>(bytes.data()));
}

}  // namespace

MarketDecodeErrorV1 ExtractObservedInstrumentKeyV2(
    const MarketMessageViewV1& input,
    std::size_t maximum_text_bytes,
    ObservedInstrumentKeyViewV2* output) noexcept {
    if (output == nullptr) {
        return MarketDecodeErrorV1::kNullOutput;
    }
    *output = {};
    if (maximum_text_bytes == 0U ||
        !RecognizedCoreMessage(input.service_id, input.message_id)) {
        return maximum_text_bytes == 0U
                   ? MarketDecodeErrorV1::kInvalidInput
                   : MarketDecodeErrorV1::kUnsupportedMessage;
    }
    if (input.service_version != kCoreServiceVersion) {
        return MarketDecodeErrorV1::kUnsupportedServiceVersion;
    }

    std::size_t fixed_bytes = 0U;
    std::size_t security_id_offset = 0U;
    std::size_t source_offset = 0U;
    MarketV1 market = MarketV1::kUnknown;
    if (input.service_id == sh::SHL2MarketData::ServiceID &&
        input.message_id == sh::SHL2MarketData::MessageID) {
        fixed_bytes = sizeof(sh::SHL2MarketData);
        security_id_offset = 4U;
        market = MarketV1::kShanghai;
    } else if (
        input.service_id == sh::NGTSTick::ServiceID &&
        input.message_id == sh::NGTSTick::MessageID) {
        fixed_bytes = sizeof(sh::NGTSTick);
        security_id_offset = 12U;
        market = MarketV1::kShanghai;
    } else if (
        input.message_id == sz::Snapshot300111_v2::MessageID) {
        fixed_bytes = sizeof(sz::Snapshot300111_v2);
        security_id_offset = 14U;
        source_offset = 20U;
        market = MarketV1::kShenzhen;
    } else if (input.message_id == sz::Order300192_v2::MessageID) {
        fixed_bytes = sizeof(sz::Order300192_v2);
        security_id_offset = 18U;
        source_offset = 24U;
        market = MarketV1::kShenzhen;
    } else {
        fixed_bytes = sizeof(sz::Transaction300191_v2);
        security_id_offset = 34U;
        source_offset = 40U;
        market = MarketV1::kShenzhen;
    }

    ObservedInstrumentKeyViewV2 extracted{};
    extracted.market = market;
    MarketDecodeErrorV1 error = ReadObservedKeyString(
        input.body,
        security_id_offset,
        fixed_bytes,
        maximum_text_bytes,
        &extracted.security_id);
    if (error != MarketDecodeErrorV1::kNone) {
        return error;
    }
    if (market == MarketV1::kShenzhen) {
        error = ReadObservedKeyString(
            input.body,
            source_offset,
            fixed_bytes,
            maximum_text_bytes,
            &extracted.security_id_source);
        if (error != MarketDecodeErrorV1::kNone) {
            return error;
        }
        if (SpansOverlap(
                extracted.security_id_source,
                extracted.security_id)) {
            return MarketDecodeErrorV1::kRangeOverlap;
        }
    }
    *output = extracted;
    return MarketDecodeErrorV1::kNone;
}

bool ApplyObservedInstrumentIdentityV2(
    const ObservedInstrumentIdentityViewV2& identity,
    DecodedMarketEventV1* event) noexcept {
    if (event == nullptr || identity.instrument_id == 0U ||
        identity.ordinal == std::numeric_limits<std::size_t>::max() ||
        identity.ordinal >=
            static_cast<std::size_t>(
                std::numeric_limits<std::uint32_t>::max()) ||
        identity.instrument_id !=
            static_cast<std::uint32_t>(identity.ordinal + 1U)) {
        return false;
    }

    bool applied = false;
    std::visit(
        [&](auto& value) noexcept {
            DecodedMarketCommonV1& common = value.common;
            if (common.market != identity.key.market ||
                !common.security_id_valid ||
                !StringEqualsBytes(
                    common.security_id, identity.key.security_id) ||
                (common.market == MarketV1::kShenzhen &&
                 (!common.security_id_source_valid ||
                  !StringEqualsBytes(
                      common.security_id_source,
                      identity.key.security_id_source)))) {
                return;
            }
            common.instrument_id = identity.instrument_id;
            common.ordinal = identity.ordinal;
            common.quantity_unit = identity.quantity_unit;
            common.security_type = identity.security_type;
            common.asset_scope = identity.asset_scope;
            common.quality_flags &=
                ~QualityBit(QualityFlagV1::kInstrumentUnknown);
            if (identity.quantity_unit != QuantityUnitV1::kUnknown) {
                common.quality_flags &=
                    ~QualityBit(QualityFlagV1::kQtyUnitUnknown);
            }
            applied = true;
        },
        *event);
    return applied;
}

MarketDecoderV1::MarketDecoderV1(
    MarketDecoderConfigV1 config) noexcept
    : config_(config) {
    std::int64_t ignored_days = 0;
    configuration_valid_ =
        config_.source_stream_id != 0U &&
        config_.trade_date >= kMinimumFixedUtc8TradeDate &&
        ParseDate(config_.trade_date, &ignored_days) &&
        config_.limits.maximum_body_bytes >=
            sizeof(sh::SHL2MarketData) &&
        config_.limits.maximum_text_bytes != 0U &&
        config_.limits.maximum_depth_items != 0U &&
        config_.limits.maximum_queue_items != 0U &&
        config_.limits.maximum_phase_products != 0U;
}

MarketDecodeErrorV1 MarketDecoderV1::Decode(
    const MarketMessageViewV1& input,
    DecodedMarketEventV1* output) noexcept {
    if (output == nullptr) {
        return MarketDecodeErrorV1::kNullOutput;
    }
    if (!configuration_valid_) {
        return MarketDecodeErrorV1::kInvalidConfiguration;
    }
    if (input.trade_date != config_.trade_date ||
        input.source_stream_id != config_.source_stream_id ||
        input.source_sequence == 0U ||
        input.message_encoding != static_cast<std::uint8_t>(
            datayes::mdl::MDLEID_BINARY) ||
        input.body.size() > config_.limits.maximum_body_bytes) {
        return MarketDecodeErrorV1::kInvalidInput;
    }
    if (!RecognizedCoreMessage(input.service_id, input.message_id)) {
        return MarketDecodeErrorV1::kUnsupportedMessage;
    }
    if (input.service_version != kCoreServiceVersion) {
        return MarketDecodeErrorV1::kUnsupportedServiceVersion;
    }

    try {
        if (input.service_id == sh::SHL2MarketData::ServiceID &&
            input.message_id == sh::SHL2MarketData::MessageID) {
            ShanghaiSnapshotV1 decoded{};
            const MarketDecodeErrorV1 error =
                DecodeShanghaiSnapshot(input, config_, &decoded);
            if (error == MarketDecodeErrorV1::kNone) {
                *output = DecodedMarketEventV1(std::move(decoded));
            }
            return error;
        }
        if (input.service_id == sh::NGTSTick::ServiceID &&
            input.message_id == sh::NGTSTick::MessageID) {
            ShanghaiTickV1 decoded{};
            const MarketDecodeErrorV1 error = DecodeShanghaiTick(
                input, config_, &sh_phases_, &decoded);
            if (error == MarketDecodeErrorV1::kNone) {
                *output = DecodedMarketEventV1(std::move(decoded));
            }
            return error;
        }
        if (input.service_id == sz::Snapshot300111_v2::ServiceID &&
            input.message_id == sz::Snapshot300111_v2::MessageID) {
            ShenzhenSnapshotV1 decoded{};
            const MarketDecodeErrorV1 error =
                DecodeShenzhenSnapshot(input, config_, &decoded);
            if (error == MarketDecodeErrorV1::kNone) {
                *output = DecodedMarketEventV1(std::move(decoded));
            }
            return error;
        }
        if (input.service_id == sz::Order300192_v2::ServiceID &&
            input.message_id == sz::Order300192_v2::MessageID) {
            ShenzhenOrderV1 decoded{};
            const MarketDecodeErrorV1 error =
                DecodeShenzhenOrder(input, config_, &decoded);
            if (error == MarketDecodeErrorV1::kNone) {
                *output = DecodedMarketEventV1(std::move(decoded));
            }
            return error;
        }
        ShenzhenTransactionV1 decoded{};
        const MarketDecodeErrorV1 error =
            DecodeShenzhenTransaction(input, config_, &decoded);
        if (error == MarketDecodeErrorV1::kNone) {
            *output = DecodedMarketEventV1(std::move(decoded));
        }
        return error;
    } catch (const std::bad_alloc&) {
        return MarketDecodeErrorV1::kResourceExhausted;
    } catch (...) {
        // Do not misclassify an invariant/programming exception as memory
        // pressure.  The API is noexcept, but the typed reason remains
        // explicit so the owning source can fail-stop and diagnose it.
        return MarketDecodeErrorV1::kUnexpectedFailure;
    }
}

std::uint64_t MarketDecodeQualityFlagsV1(
    MarketDecodeErrorV1 error) noexcept {
    switch (error) {
        case MarketDecodeErrorV1::kNone:
        case MarketDecodeErrorV1::kUnsupportedMessage:
            return 0U;
        case MarketDecodeErrorV1::kUnsupportedServiceVersion:
            return QualityBit(QualityFlagV1::kSchemaUnknown);
        case MarketDecodeErrorV1::kTruncated:
            return QualityBit(QualityFlagV1::kDecodeTruncated);
        case MarketDecodeErrorV1::kTextInvalid:
            return QualityBit(QualityFlagV1::kDecodeTextInvalid);
        case MarketDecodeErrorV1::kNullOutput:
        case MarketDecodeErrorV1::kInvalidConfiguration:
        case MarketDecodeErrorV1::kInvalidInput:
        case MarketDecodeErrorV1::kFixedPointOverflow:
        case MarketDecodeErrorV1::kPhaseProductLimitExceeded:
        case MarketDecodeErrorV1::kCountExceeded:
        case MarketDecodeErrorV1::kCountMismatch:
        case MarketDecodeErrorV1::kResourceExhausted:
        case MarketDecodeErrorV1::kUnexpectedFailure:
            // These typed failures are not evidence of a malformed relative
            // offset.  A later QualityRecord may carry their explicit reason;
            // the frozen bitmap has no truthful one-bit equivalent.
            return 0U;
        case MarketDecodeErrorV1::kOffsetInvalid:
        case MarketDecodeErrorV1::kRangeOverlap:
            return QualityBit(QualityFlagV1::kDecodeOffsetInvalid);
    }
    return QualityBit(QualityFlagV1::kDecodeOffsetInvalid);
}

}  // namespace l2flow::market
