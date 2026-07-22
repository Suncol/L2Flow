#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <variant>

namespace l2flow::market {

// These values describe the decoded, owned Phase-4 view.  They are not a
// persistent Canonical schema and deliberately do not reuse vendor packed
// enums or structs.
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

// Scope is explicit registry data.  The decoder never guesses it from a
// security-code prefix.
enum class AssetScopeV1 : std::uint8_t {
    kUnknown = 0U,
    kDocumentedCore = 1U,
    kOutsideDocumentedCore = 2U,
};

enum class MarketEventKindV1 : std::uint8_t {
    kShanghaiSnapshot = 1U,
    kShanghaiTick = 2U,
    kShenzhenSnapshot = 3U,
    kShenzhenOrder = 4U,
    kShenzhenTransaction = 5U,
};

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

// A numeric high/low limit is not factor-safe until versioned reference data
// has distinguished an ordinary finite value from the vendor's business
// sentinels.  Phase 4 retains the raw wire integer but defaults to unknown;
// it never guesses from magnitude.
enum class LimitPriceSemanticsV1 : std::uint8_t {
    kUnknown = 0U,
    kFinite,
    kNoLimit,
};

struct DecimalValueV1 final {
    // Exact vendor fixed-point integer and scale are retained even when the
    // action-specific validity matrix makes this field meaningless.  Only a
    // semantically valid field is normalized to p6; no floating-point round
    // trip is involved.
    std::int64_t raw = 0;
    std::int64_t normalized_p6 = 0;
    std::uint8_t scale = 0U;
    bool valid = false;
    bool is_null = false;
};

struct QuantityValueV1 final {
    // Quantities retain their native vendor scale.  A scale of zero is an
    // integer native quantity.  The decoder does not assume that the unit is
    // shares; quantity_unit comes from the instrument registry.
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
    // A caller-owned, per-source callback/Raw order.  Zero is not accepted.
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

// Phase-4-specific notices supplement the frozen Phase-3 quality bitmap.
// They intentionally do not consume undocumented QualityFlagV1 bit numbers.
enum class MarketNoticeV1 : std::uint8_t {
    kExchangeTimeInvalid = 0U,
    kVendorLocalTimeInvalid,
    kSnapshotDepthTruncatedTo10,
    kLimitPriceSemanticsUnknown,
    kMatchedQuantityDomainInvalid,
    kEventSequenceDomainInvalid,
    kOrderReferenceDomainInvalid,
    kLegacyWarLowerSemanticsUnknown,
};

[[nodiscard]] constexpr std::uint64_t MarketNoticeBitV1(
    MarketNoticeV1 notice) noexcept {
    return std::uint64_t{1U}
           << static_cast<std::uint8_t>(notice);
}

struct DecodedMarketCommonV1 final {
    MarketEventKindV1 kind = MarketEventKindV1::kShanghaiSnapshot;
    MarketV1 market = MarketV1::kUnknown;
    MarketMessageViewV1 origin{};
    // origin.body is cleared before publication so an owned event never
    // retains callback- or Raw-view-scoped memory.
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
    QuantityUnitV1 quantity_unit = QuantityUnitV1::kUnknown;
    SecurityTypeV1 security_type = SecurityTypeV1::kUnknown;
    AssetScopeV1 asset_scope = AssetScopeV1::kUnknown;
    std::uint64_t quality_flags = 0U;
    std::uint64_t market_notices = 0U;
    // A Phase-5 sequence guard must decide whether an SH status event is
    // accepted before it mutates product-phase history.  When true, the
    // decoder parsed the status event but deliberately did not read or update
    // its internal phase map; the accepting downstream normalizer owns phase
    // attribution and commit.
    bool sh_phase_attribution_deferred = false;
};

inline constexpr std::size_t kMaximumPublicDepthV1 = 10U;
inline constexpr std::size_t kMaximumPublicQueueV1 = 50U;

struct BookLevelV1 final {
    DecimalValueV1 price{};
    QuantityValueV1 quantity{};
    std::uint32_t order_count = 0U;
    bool order_count_valid = false;
};

struct BestQueueV1 final {
    std::uint32_t total_order_count = 0U;
    std::uint32_t actual_revealed_count = 0U;
    std::uint32_t retained_count = 0U;
    std::array<QuantityValueV1, kMaximumPublicQueueV1> quantities{};
};

struct SnapshotBookV1 final {
    std::uint32_t actual_bid_depth = 0U;
    std::uint32_t actual_ask_depth = 0U;
    std::uint32_t retained_bid_depth = 0U;
    std::uint32_t retained_ask_depth = 0U;
    std::array<BookLevelV1, kMaximumPublicDepthV1> bids{};
    std::array<BookLevelV1, kMaximumPublicDepthV1> asks{};
    BestQueueV1 bid1_queue{};
    BestQueueV1 ask1_queue{};
};

struct ShanghaiSnapshotV1 final {
    DecodedMarketCommonV1 common{};
    std::int32_t image_status = 0;
    std::string instrument_status;
    bool instrument_status_valid = false;
    DecimalValueV1 pre_close_price{};
    DecimalValueV1 open_price{};
    DecimalValueV1 high_price{};
    DecimalValueV1 low_price{};
    DecimalValueV1 last_price{};
    DecimalValueV1 close_price{};
    std::uint32_t trade_count = 0U;
    QuantityValueV1 trade_volume{};
    DecimalValueV1 turnover{};
    QuantityValueV1 total_bid_volume{};
    DecimalValueV1 weighted_average_bid_price{};
    DecimalValueV1 alternate_weighted_average_bid_price{};
    QuantityValueV1 total_ask_volume{};
    DecimalValueV1 weighted_average_ask_price{};
    DecimalValueV1 alternate_weighted_average_ask_price{};
    // Vendor EtfBuy* means ETF subscription and EtfSell* means redemption.
    std::uint32_t etf_subscription_count = 0U;
    QuantityValueV1 etf_subscription_quantity{};
    DecimalValueV1 etf_subscription_amount{};
    std::uint32_t etf_redemption_count = 0U;
    QuantityValueV1 etf_redemption_quantity{};
    DecimalValueV1 etf_redemption_amount{};
    DecimalValueV1 yield_to_maturity{};
    QuantityValueV1 total_warrant_exercise_quantity{};
    // Legacy WarLowerPri has been assigned product/version-dependent
    // meanings.  V1 retains raw/scale with valid=false until applicability
    // policy is pinned; consumers must not assume it is a warrant price.
    DecimalValueV1 legacy_war_lower_value{};
    // The legacy vendor member name is WarUpperPri; in this V4 schema the
    // p5 value is the high-precision ETF IOPV, not a warrant upper limit.
    DecimalValueV1 high_precision_iopv{};
    std::uint32_t withdrawal_buy_count = 0U;
    QuantityValueV1 withdrawal_buy_volume{};
    DecimalValueV1 withdrawal_buy_amount{};
    std::uint32_t withdrawal_sell_count = 0U;
    QuantityValueV1 withdrawal_sell_volume{};
    DecimalValueV1 withdrawal_sell_amount{};
    std::uint32_t total_bid_order_count = 0U;
    std::uint32_t total_ask_order_count = 0U;
    std::uint32_t maximum_bid_duration = 0U;
    std::uint32_t maximum_ask_duration = 0U;
    DecimalValueV1 iopv{};
    SnapshotBookV1 book{};
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

struct ShenzhenSnapshotV1 final {
    DecodedMarketCommonV1 common{};
    std::uint32_t channel = 0U;
    std::string trading_phase_code;
    bool trading_phase_code_valid = false;
    DecimalValueV1 pre_close_price{};
    std::int64_t trade_count = 0;
    QuantityValueV1 volume{};
    DecimalValueV1 turnover{};
    DecimalValueV1 last_price{};
    DecimalValueV1 open_price{};
    DecimalValueV1 high_price{};
    DecimalValueV1 low_price{};
    DecimalValueV1 price_change_1{};
    DecimalValueV1 price_change_2{};
    DecimalValueV1 pe_ratio_1{};
    DecimalValueV1 pe_ratio_2{};
    DecimalValueV1 pre_close_iopv{};
    DecimalValueV1 iopv{};
    QuantityValueV1 total_ask_quantity{};
    DecimalValueV1 weighted_average_ask_price{};
    QuantityValueV1 total_bid_quantity{};
    DecimalValueV1 weighted_average_bid_price{};
    // Sentinel interpretation is intentionally delegated to versioned
    // registry/reference data.  Until such policy is supplied, raw/scale are
    // preserved, DecimalValueV1::valid is false, and semantics is unknown.
    DecimalValueV1 high_limit_price{};
    DecimalValueV1 low_limit_price{};
    LimitPriceSemanticsV1 high_limit_semantics =
        LimitPriceSemanticsV1::kUnknown;
    LimitPriceSemanticsV1 low_limit_semantics =
        LimitPriceSemanticsV1::kUnknown;
    QuantityValueV1 open_interest{};
    // Vendor OptPremiumRatio is the warrant premium ratio in this feed.
    DecimalValueV1 warrant_premium_ratio{};
    SnapshotBookV1 book{};
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

using DecodedMarketEventV1 = std::variant<
    ShanghaiSnapshotV1,
    ShanghaiTickV1,
    ShenzhenSnapshotV1,
    ShenzhenOrderV1,
    ShenzhenTransactionV1>;

// Decoder output is a by-value variant and therefore has the footprint of
// its largest snapshot alternative.  Keeping that representation for every
// tick would waste several KiB per record.  The session layer materializes an
// exact-type immutable allocation and stores only this small tagged owner.
using RetainedMarketEventV1 = std::variant<
    std::unique_ptr<const ShanghaiSnapshotV1>,
    std::unique_ptr<const ShanghaiTickV1>,
    std::unique_ptr<const ShenzhenSnapshotV1>,
    std::unique_ptr<const ShenzhenOrderV1>,
    std::unique_ptr<const ShenzhenTransactionV1>>;

enum class RetainedMarketEventCreateErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kResourceExhausted,
    kUnexpectedFailure,
};

[[nodiscard]] RetainedMarketEventCreateErrorV1
RetainMarketEventV1(
    DecodedMarketEventV1 event,
    RetainedMarketEventV1* output) noexcept;

template <typename Event>
[[nodiscard]] const Event* RetainedMarketEventGetV1(
    const RetainedMarketEventV1& event) noexcept {
    const auto* owner =
        std::get_if<std::unique_ptr<const Event>>(&event);
    return owner == nullptr ? nullptr : owner->get();
}

[[nodiscard]] const DecodedMarketCommonV1& MarketCommonV1(
    const DecodedMarketEventV1& event) noexcept;
[[nodiscard]] DecodedMarketCommonV1& MarketCommonV1(
    DecodedMarketEventV1& event) noexcept;
[[nodiscard]] std::size_t EstimateOwnedMarketEventBytesV1(
    const DecodedMarketEventV1& event) noexcept;
[[nodiscard]] std::size_t EstimateRetainedMarketEventBytesV1(
    const DecodedMarketEventV1& event) noexcept;
// Measures the materialized owner after string moves/allocations, so session
// admission charges the actual retained representation rather than assuming
// a source string's capacity is preserved by its move constructor.
[[nodiscard]] std::size_t EstimateRetainedMarketEventBytesV1(
    const RetainedMarketEventV1& event) noexcept;

}  // namespace l2flow::market
