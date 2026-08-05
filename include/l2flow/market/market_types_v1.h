#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>
#include <span>
#include <string>
#include <variant>

namespace l2flow::market {

// These values describe the decoded, owned market view. They deliberately do
// not reuse vendor packed enums or structs.
enum class MarketV1 : std::uint8_t {
    kUnknown = 0U,
    kShanghai = 1U,
    kShenzhen = 2U,
};

enum class QuantityUnitV1 : std::uint8_t {
    kUnknown = 0U,
    kShare,
    kFundUnit,
    kLot,
    kBondPiece,
    kIndexUnit,
};

enum class SecurityTypeV1 : std::uint8_t {
    kUnknown = 0U,
    kEquity,
    kFund,
    kBond,
    kConvertibleBond,
    kIndex,
    kWarrant,
    kOption,
};

// Scope is explicit daily-catalog metadata. The decoder never guesses it from
// a security-code prefix.
enum class AssetScopeV1 : std::uint8_t {
    kUnknown = 0U,
    kDocumentedCore = 1U,
    kOutsideDocumentedCore = 2U,
};

enum class MarketEventKindV1 : std::uint8_t {
    kShanghaiTick = 2U,
    kShenzhenOrder = 4U,
    kShenzhenTransaction = 5U,
};

[[nodiscard]] constexpr bool IsTickEventKindV1(
    MarketEventKindV1 kind) noexcept {
    return kind == MarketEventKindV1::kShanghaiTick ||
           kind == MarketEventKindV1::kShenzhenOrder ||
           kind == MarketEventKindV1::kShenzhenTransaction;
}

enum class TickActionV1 : std::uint8_t {
    kUnknown = 0U,
    kAdd,
    kCancel,
    kTrade,
    kStatus,
};

enum class SideV1 : std::uint8_t {
    kUnknown = 0U,
    kBuy,
    kSell,
    kBorrow,
    kLend,
};

enum class OrderTypeV1 : std::uint8_t {
    kUnknown = 0U,
    kMarket,
    kLimit,
    kSameSideBest,
};

enum class AggressorV1 : std::uint8_t {
    kUnknown = 0U,
    kBuy,
    kSell,
    kNeutral,
};

enum class TradingPhaseV1 : std::uint8_t {
    kUnknown = 0U,
    kStart,
    kOpeningCall,
    kContinuous,
    kSuspended,
    kClosingCall,
    kClosed,
    kEnd,
};

struct DecimalValueV1 final {
    // Exact vendor fixed-point integer and scale are retained even when the
    // field is null, action-inapplicable, product applicability is unknown,
    // or a documented domain check fails.  valid means the decoder has both
    // normalized the field to p6 without overflow and accepted it under that
    // field's explicit action/product/domain contract.  It does not by itself
    // make a value suitable for an arbitrary financial calculation: each
    // calculator must still enforce its own documented economic constraints.
    // No floating-point round trip is involved in normalized_p6.
    std::int64_t raw = 0;
    std::int64_t normalized_p6 = 0;
    std::uint8_t scale = 0U;
    bool valid = false;
    bool is_null = false;
};

struct QuantityValueV1 final {
    // Quantities retain their native vendor scale.  A scale of zero is an
    // integer native quantity.  The decoder does not assume that the unit is
    // shares; quantity_unit comes from daily-catalog metadata.
    std::int64_t raw = 0;
    std::uint8_t scale = 0U;
    bool valid = false;
    bool is_null = false;
};

struct TimeValueV1 final {
    std::uint32_t raw_hhmmssmmm = 0U;
    std::uint64_t nanoseconds_since_midnight = 0U;
    // UTC Unix nanoseconds for trade_date at fixed UTC+08:00.  This is valid
    // only when valid and unix_nanoseconds_valid are both true.
    std::int64_t unix_nanoseconds = 0;
    bool valid = false;
    bool is_null = false;
    bool unix_nanoseconds_valid = false;
};

struct MarketMessageViewV1 final {
    std::uint32_t source_stream_id = 0U;
    std::uint32_t trade_date = 0U;
    // A caller-owned, dense per-source ingress order. Zero is not accepted;
    // the value has no persistence-position or exchange-time meaning.
    std::uint64_t source_sequence = 0U;
    std::uint8_t service_id = 0U;
    std::uint16_t service_version = 0U;
    std::uint16_t message_id = 0U;
    std::uint8_t message_encoding = 0U;
    std::uint32_t vendor_local_time_raw = 0U;
    std::uint64_t vendor_sequence_id = 0U;
    std::int64_t recv_realtime_ns = 0;
    std::int64_t recv_monotonic_ns = 0;
    std::span<const std::byte> body;
};

// Decoder-specific notices supplement the shared quality bitmap. They
// intentionally do not consume undocumented QualityFlagV1 bit numbers.
enum class MarketNoticeV1 : std::uint8_t {
    kExchangeTimeInvalid = 0U,
    kVendorLocalTimeInvalid,
    kMatchedQuantityDomainInvalid,
    kEventSequenceDomainInvalid,
    kOrderReferenceDomainInvalid,
    kVendorWarLowerSemanticsUnknown,
    kVendorWarUpperSemanticsUnknown,
    kVendorOptPremiumRatioSemanticsUnknown,
    kQuantityDomainInvalid,
    kAbsolutePriceDomainInvalid,
    kProductApplicabilityUnknown,
    kMaximumDurationUnavailable,
    kTradeAmountDomainInvalid,
};

[[nodiscard]] constexpr std::uint64_t MarketNoticeBitV1(
    MarketNoticeV1 notice) noexcept {
    return std::uint64_t{1U}
           << static_cast<std::uint8_t>(notice);
}

struct DecodedMarketCommonV1 final {
    MarketEventKindV1 kind = MarketEventKindV1::kShanghaiTick;
    MarketV1 market = MarketV1::kUnknown;
    MarketMessageViewV1 origin{};
    // origin.body is cleared before publication so an owned event never
    // retains callback-message-scoped memory.
    TimeValueV1 exchange_time{};
    // SDK header LocalTime is retained only as a validated time-of-day;
    // without a trusted capture calendar date its Unix projection is always
    // invalid, because it need not share exchange_time's trade_date.
    TimeValueV1 vendor_local_time{};
    std::string security_id;
    std::string security_id_source;
    std::string md_stream_id;
    bool security_id_valid = false;
    bool security_id_source_valid = false;
    bool md_stream_id_valid = false;
    std::uint32_t instrument_id = 0U;
    // Stable ordinal in the frozen daily catalog. Callback admission obtains
    // it with instrument_id; downstream routing performs no second key lookup.
    std::size_t ordinal =
        std::numeric_limits<std::size_t>::max();
    QuantityUnitV1 quantity_unit = QuantityUnitV1::kUnknown;
    SecurityTypeV1 security_type = SecurityTypeV1::kUnknown;
    AssetScopeV1 asset_scope = AssetScopeV1::kUnknown;
    std::uint64_t quality_flags = 0U;
    std::uint64_t market_notices = 0U;
};

enum TickValidityBitV1 : std::uint32_t {
    kTickPriceValidV1 = 1U << 0U,
    kTickQuantityValidV1 = 1U << 1U,
    kTickTradeAmountValidV1 = 1U << 2U,
    kTickMatchedQuantityValidV1 = 1U << 3U,
    kTickPrimaryOrderIdValidV1 = 1U << 4U,
    kTickBuyOrderIdValidV1 = 1U << 5U,
    kTickSellOrderIdValidV1 = 1U << 6U,
    kTickExchangeTimeValidV1 = 1U << 7U,
    kTickSideValidV1 = 1U << 8U,
    kTickOrderTypeValidV1 = 1U << 9U,
    kTickAggressorValidV1 = 1U << 10U,
    kTickPhaseValidV1 = 1U << 11U,
};

struct TickFieldsV1 final {
    TickActionV1 action = TickActionV1::kUnknown;
    SideV1 side = SideV1::kUnknown;
    OrderTypeV1 order_type = OrderTypeV1::kUnknown;
    AggressorV1 aggressor = AggressorV1::kUnknown;
    TradingPhaseV1 phase = TradingPhaseV1::kUnknown;
    DecimalValueV1 price{};
    QuantityValueV1 quantity{};
    DecimalValueV1 trade_amount{};
    QuantityValueV1 matched_quantity{};
    std::int64_t primary_order_id = 0;
    std::int64_t buy_order_id = 0;
    std::int64_t sell_order_id = 0;
    std::uint32_t validity_bitmap = 0U;
};

struct ShanghaiTickV1 final {
    DecodedMarketCommonV1 common{};
    std::int64_t business_index = 0;
    std::int32_t channel = 0;
    std::string raw_type;
    std::string raw_tick_flag;
    bool raw_type_valid = false;
    bool raw_tick_flag_valid = false;
    TickFieldsV1 fields{};
};

struct ShenzhenOrderV1 final {
    DecodedMarketCommonV1 common{};
    std::uint32_t channel = 0U;
    std::int64_t application_sequence = 0;
    std::int32_t raw_side = 0;
    std::int32_t raw_order_type = 0;
    TickFieldsV1 fields{};
};

struct ShenzhenTransactionV1 final {
    DecodedMarketCommonV1 common{};
    std::uint32_t channel = 0U;
    std::int64_t application_sequence = 0;
    std::int32_t raw_execution_type = 0;
    TickFieldsV1 fields{};
};

// Production decoding has exactly the three subscribed Tick alternatives.
using DecodedMarketEventV1 = std::variant<
    ShanghaiTickV1,
    ShenzhenOrderV1,
    ShenzhenTransactionV1>;

[[nodiscard]] const DecodedMarketCommonV1& MarketCommonV1(
    const DecodedMarketEventV1& event) noexcept;
[[nodiscard]] DecodedMarketCommonV1& MarketCommonV1(
    DecodedMarketEventV1& event) noexcept;

}  // namespace l2flow::market
