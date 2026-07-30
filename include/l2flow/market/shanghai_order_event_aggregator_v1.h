#pragma once

#include "l2flow/market/market_types_v1.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>
#include <variant>
#include <vector>

namespace l2flow::market {

// This component reconstructs an order-analysis view from the Shanghai 4.24
// combined order/trade feed. It does not replace the native BizIndex-ordered
// event stream used to rebuild a book: a reconstructed order has no native
// add event.
enum class ShanghaiOrderQualityFlagV1 : std::uint8_t {
    kSyntheticOrder = 0U,
    kOriginalQuantityLowerBound,
    kExecutionBoundaryPrice,
    kPrematchQuantityMismatch,
    kPrematchQuantityUnavailable,
    kQuantityConflict,
    kPhaseUnknown,
    kUnexpectedPhase,
    kSideConflict,
    kDuplicateAdd,
    kCancelWithoutAdd,
    kEndedWithObservedBalance,
};

[[nodiscard]] constexpr std::uint64_t ShanghaiOrderQualityBitV1(
    ShanghaiOrderQualityFlagV1 flag) noexcept {
    return std::uint64_t{1U}
           << static_cast<std::uint8_t>(flag);
}

enum class ShanghaiOriginalQuantityStatusV1 : std::uint8_t {
    kUnknown = 0U,
    kExact,
    kLowerBound,
};

enum class ShanghaiOrderPriceSourceV1 : std::uint8_t {
    kUnknown = 0U,
    kSourceAdd,
    kBuyMaximumExecution,
    kSellMinimumExecution,
};

enum class ShanghaiOrderSourceV1 : std::uint8_t {
    kUnknown = 0U,
    kSourceAdd,
    kReconstructedFromTrades,
};

enum class ShanghaiOrderSideSourceV1 : std::uint8_t {
    kUnknown = 0U,
    kSourceAddFlag,
    kSourceAggressorFlag,
};

enum class ShanghaiOrderFinalityV1 : std::uint8_t {
    kProvisional = 0U,
    kFinal,
    kConflict,
};

enum class ShanghaiOrderDeltaOperationV1 : std::uint8_t {
    kInsert = 0U,
    kUpdate,
    kFinalize,
};

struct ShanghaiOrderKeyV1 final {
    std::uint32_t trade_date = 0U;
    std::uint32_t instrument_id = 0U;
    std::int32_t channel = 0;
    std::int64_t order_id = 0;

    [[nodiscard]] friend bool operator==(
        const ShanghaiOrderKeyV1&,
        const ShanghaiOrderKeyV1&) noexcept = default;

    [[nodiscard]] friend bool operator<(
        const ShanghaiOrderKeyV1& lhs,
        const ShanghaiOrderKeyV1& rhs) noexcept;
};

// A nonzero SourceAnchor identifies a real 4.24 message. A reconstructed
// order uses the anchor of the source trade which caused its current
// revision; it must never be represented as a fabricated BizIndex. The
// explicit trade-date Finalize API may use an all-zero anchor to state that
// its lifecycle boundary has no native source event.
struct ShanghaiOrderSourceAnchorV1 final {
    std::int64_t native_event_sequence = 0;
    std::uint64_t source_sequence = 0U;
    std::uint64_t ingress_sequence = 0U;
    std::uint64_t tick_stream_sequence = 0U;
    std::uint64_t vendor_sequence_id = 0U;
    std::uint64_t event_time_ns_since_midnight = 0U;
    std::int64_t event_time_unix_ns = 0;
    std::int64_t recv_realtime_ns = 0;
    std::int64_t recv_monotonic_ns = 0;
    std::uint32_t vendor_local_time_raw = 0U;
    std::uint64_t vendor_local_time_ns_since_midnight = 0U;
    bool event_time_valid = false;
    bool event_time_unix_ns_valid = false;
    bool vendor_local_time_valid = false;
};

// Common fixed-point input shared by decoded-event and Wire adapters. Price is
// normalized p6. Shanghai 4.24 quantities are integer native units (scale 0).
// No floating-point value crosses this boundary.
struct ShanghaiOrderEventInputV1 final {
    std::uint32_t trade_date = 0U;
    std::uint32_t instrument_id = 0U;
    std::int32_t channel = 0;
    ShanghaiOrderSourceAnchorV1 anchor{};

    TickActionV1 action = TickActionV1::kUnknown;
    SideV1 side = SideV1::kUnknown;
    AggressorV1 aggressor = AggressorV1::kUnknown;
    TradingPhaseV1 phase = TradingPhaseV1::kUnknown;

    std::int64_t price_p6 = 0;
    std::int64_t trade_amount_p6 = 0;
    std::int64_t quantity = 0;
    std::int64_t matched_quantity = 0;
    std::int64_t primary_order_id = 0;
    std::int64_t buy_order_id = 0;
    std::int64_t sell_order_id = 0;

    bool price_valid = false;
    bool trade_amount_valid = false;
    bool quantity_valid = false;
    bool matched_quantity_valid = false;
    bool phase_valid = false;

    std::uint64_t source_quality_flags = 0U;
    std::uint64_t source_market_notices = 0U;
};

enum class ShanghaiOrderEventProjectionV1 : std::uint8_t {
    kProjected = 0U,
    kNotShanghaiTick,
    kInvalidTick,
};

// Projects the owned decoder representation into the dependency-light core
// input. An IPC process can map RealtimeWireTickPayloadV2 to the same input
// without linking the core back to IPC.
[[nodiscard]] ShanghaiOrderEventProjectionV1
ProjectShanghaiOrderEventInputV1(
    const DecodedMarketEventV1& event,
    std::uint64_t ingress_sequence,
    std::uint64_t tick_stream_sequence,
    ShanghaiOrderEventInputV1* output) noexcept;

struct ShanghaiOrderSnapshotV1 final {
    ShanghaiOrderKeyV1 key{};
    SideV1 side = SideV1::kUnknown;
    ShanghaiOrderSideSourceV1 side_source =
        ShanghaiOrderSideSourceV1::kUnknown;
    ShanghaiOrderSourceV1 order_source =
        ShanghaiOrderSourceV1::kUnknown;

    std::int64_t price_p6 = 0;
    bool price_valid = false;
    ShanghaiOrderPriceSourceV1 price_source =
        ShanghaiOrderPriceSourceV1::kUnknown;
    std::int64_t execution_boundary_price_p6 = 0;
    bool execution_boundary_price_valid = false;

    std::int64_t published_quantity = 0;
    bool published_quantity_valid = false;
    std::int64_t original_quantity = 0;
    bool original_quantity_valid = false;
    ShanghaiOriginalQuantityStatusV1 original_quantity_status =
        ShanghaiOriginalQuantityStatusV1::kUnknown;
    std::int64_t remaining_quantity = 0;
    bool remaining_quantity_valid = false;

    std::int64_t source_matched_quantity = 0;
    bool source_matched_quantity_valid = false;
    std::int64_t observed_pre_add_trade_quantity = 0;
    std::int64_t post_add_trade_quantity = 0;
    std::int64_t total_trade_quantity = 0;
    std::int64_t total_cancel_quantity = 0;
    std::uint64_t trade_count = 0U;

    TradingPhaseV1 phase_at_first =
        TradingPhaseV1::kUnknown;
    TradingPhaseV1 phase_at_add = TradingPhaseV1::kUnknown;
    TradingPhaseV1 phase_at_last = TradingPhaseV1::kUnknown;
    bool add_seen = false;
    bool apply_to_book = false;

    ShanghaiOrderSourceAnchorV1 first_anchor{};
    ShanghaiOrderSourceAnchorV1 last_anchor{};
    ShanghaiOrderSourceAnchorV1 add_anchor{};

    std::uint64_t revision = 0U;
    ShanghaiOrderFinalityV1 finality =
        ShanghaiOrderFinalityV1::kProvisional;
    std::uint64_t quality_flags = 0U;
    std::uint64_t source_quality_flags = 0U;
    std::uint64_t source_market_notices = 0U;
};

struct ShanghaiOrderRevisionEventV1 final {
    ShanghaiOrderDeltaOperationV1 operation =
        ShanghaiOrderDeltaOperationV1::kInsert;
    ShanghaiOrderSourceAnchorV1 source_anchor{};
    ShanghaiOrderSnapshotV1 order{};
};

struct ShanghaiTradeEventV1 final {
    std::uint32_t trade_date = 0U;
    std::uint32_t instrument_id = 0U;
    std::int32_t channel = 0;
    ShanghaiOrderSourceAnchorV1 source_anchor{};
    std::int64_t buy_order_id = 0;
    std::int64_t sell_order_id = 0;
    AggressorV1 aggressor = AggressorV1::kUnknown;
    TradingPhaseV1 phase = TradingPhaseV1::kUnknown;
    std::int64_t price_p6 = 0;
    // Exact normalized source TradeMoney for T. It is never recomputed from
    // price and quantity.
    std::int64_t trade_amount_p6 = 0;
    bool trade_amount_valid = false;
    std::int64_t quantity = 0;
    std::uint64_t source_quality_flags = 0U;
    std::uint64_t source_market_notices = 0U;
};

struct ShanghaiCancelEventV1 final {
    ShanghaiOrderKeyV1 key{};
    ShanghaiOrderSourceAnchorV1 source_anchor{};
    SideV1 side = SideV1::kUnknown;
    TradingPhaseV1 phase = TradingPhaseV1::kUnknown;
    std::int64_t quantity = 0;
    bool referenced_order_found = false;
    std::uint64_t source_quality_flags = 0U;
    std::uint64_t source_market_notices = 0U;
};

struct ShanghaiStatusEventV1 final {
    std::uint32_t trade_date = 0U;
    std::uint32_t instrument_id = 0U;
    std::int32_t channel = 0;
    ShanghaiOrderSourceAnchorV1 source_anchor{};
    TradingPhaseV1 phase = TradingPhaseV1::kUnknown;
    std::uint64_t quality_flags = 0U;
    std::uint64_t source_quality_flags = 0U;
    std::uint64_t source_market_notices = 0U;
};

using ShanghaiOrderEventV1 = std::variant<
    ShanghaiOrderRevisionEventV1,
    ShanghaiTradeEventV1,
    ShanghaiCancelEventV1,
    ShanghaiStatusEventV1>;

struct ShanghaiOrderEventAggregatorConfigV1 final {
    std::uint32_t trade_date = 0U;
    std::size_t maximum_order_states = 0U;
};

enum class ShanghaiOrderAggregatorCreateErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidConfiguration,
    kResourceExhausted,
};

enum class ShanghaiOrderAggregatorConsumeErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidInput,
    kWrongTradeDate,
    kAlreadyFinalized,
    kOrderCapacity,
    kNumericOverflow,
    kOutOfOrderInput,
    kResourceExhausted,
    kFailed,
};

enum class ShanghaiOrderAggregatorQueryErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidKey,
    kNotFound,
};

[[nodiscard]] std::string_view
ShanghaiOrderAggregatorCreateErrorNameV1(
    ShanghaiOrderAggregatorCreateErrorV1 error) noexcept;
[[nodiscard]] std::string_view
ShanghaiOrderAggregatorConsumeErrorNameV1(
    ShanghaiOrderAggregatorConsumeErrorV1 error) noexcept;
[[nodiscard]] std::string_view
ShanghaiOrderAggregatorQueryErrorNameV1(
    ShanghaiOrderAggregatorQueryErrorV1 error) noexcept;

class ShanghaiOrderEventAggregatorV1 final {
public:
    // One decoder/aggregation lane owns an instance. Public methods, including
    // queries and destruction, are not safe for concurrent calls.
    ShanghaiOrderEventAggregatorV1(
        const ShanghaiOrderEventAggregatorV1&) = delete;
    ShanghaiOrderEventAggregatorV1& operator=(
        const ShanghaiOrderEventAggregatorV1&) = delete;
    ShanghaiOrderEventAggregatorV1(
        ShanghaiOrderEventAggregatorV1&&) = delete;
    ShanghaiOrderEventAggregatorV1& operator=(
        ShanghaiOrderEventAggregatorV1&&) = delete;
    ~ShanghaiOrderEventAggregatorV1();

    [[nodiscard]] static ShanghaiOrderAggregatorCreateErrorV1 Create(
        ShanghaiOrderEventAggregatorConfigV1 config,
        std::unique_ptr<ShanghaiOrderEventAggregatorV1>* output)
        noexcept;

    // output is replaced with the events caused by exactly this input.
    // Trades and cancels are emitted as source events; order revisions follow
    // them in ascending OrderKey order. Inputs must retain upstream order:
    // tick_stream_sequence is strictly increasing globally and BizIndex is
    // strictly increasing among observed messages in each channel. Gaps are
    // allowed here because an instrument-filtered stream naturally omits other
    // products; exchange-level gap completeness must be established before
    // filtering.
    [[nodiscard]] ShanghaiOrderAggregatorConsumeErrorV1 Consume(
        const ShanghaiOrderEventInputV1& input,
        std::vector<ShanghaiOrderEventV1>* output) noexcept;

    [[nodiscard]] ShanghaiOrderAggregatorConsumeErrorV1 ConsumeDecoded(
        const DecodedMarketEventV1& event,
        std::uint64_t ingress_sequence,
        std::uint64_t tick_stream_sequence,
        std::vector<ShanghaiOrderEventV1>* output) noexcept;

    // Finalizes all still-open states at the explicit clean trade-date
    // boundary. It performs no clock lookup and permanently seals this
    // instance against later input.
    [[nodiscard]] ShanghaiOrderAggregatorConsumeErrorV1 Finalize(
        const ShanghaiOrderSourceAnchorV1& source_anchor,
        std::vector<ShanghaiOrderEventV1>* output) noexcept;

    [[nodiscard]] ShanghaiOrderAggregatorQueryErrorV1 GetOrder(
        const ShanghaiOrderKeyV1& key,
        ShanghaiOrderSnapshotV1* output) const noexcept;
    [[nodiscard]] std::size_t order_count() const noexcept;
    [[nodiscard]] bool finalized() const noexcept;
    [[nodiscard]] const ShanghaiOrderEventAggregatorConfigV1& config()
        const noexcept;

private:
    class Impl;
    explicit ShanghaiOrderEventAggregatorV1(
        std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::market
