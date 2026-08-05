#pragma once

#include "l2flow/market/market_types_v1.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>
#include <variant>
#include <vector>

namespace l2flow::market {

// The projector is an order-analysis view over Shenzhen 6.33 orders and 6.36
// transactions. It never manufactures an order for a transaction reference
// which has not been observed in 6.33.
enum class ShenzhenEventQualityFlagV1 : std::uint8_t {
    kUnknownBuyOrderReference = 0U,
    kUnknownSellOrderReference,
    kUnknownCancelOrderReference,
    kQuantityConflict,
    kNumericOverflow,
    kSideConflict,
    kDuplicateOrder,
    kEndedWithObservedBalance,
    kAmbiguousTradeOrderReferences,
};

[[nodiscard]] constexpr std::uint64_t ShenzhenEventQualityBitV1(
    ShenzhenEventQualityFlagV1 flag) noexcept {
    return std::uint64_t{1U}
           << static_cast<std::uint8_t>(flag);
}

enum class ShenzhenOrderFinalityV1 : std::uint8_t {
    kProvisional = 0U,
    kFinal,
    kConflict,
};

enum class ShenzhenOrderDeltaOperationV1 : std::uint8_t {
    kInsert = 0U,
    kUpdate,
    kFinalize,
};

struct ShenzhenOrderKeyV1 final {
    std::uint32_t trade_date = 0U;
    std::uint32_t instrument_id = 0U;
    std::uint32_t channel = 0U;
    // For an order this is the 6.33 ApplSeqNum.
    std::int64_t order_id = 0;

    [[nodiscard]] friend bool operator==(
        const ShenzhenOrderKeyV1&,
        const ShenzhenOrderKeyV1&) noexcept = default;

    friend bool operator<(
        const ShenzhenOrderKeyV1& lhs,
        const ShenzhenOrderKeyV1& rhs) noexcept;
};

// The native event sequence is the source message's ApplSeqNum. The other
// sequences retain their existing pipeline meanings and are not substituted
// for one another.
struct ShenzhenEventSourceAnchorV1 final {
    std::int64_t native_event_sequence = 0;
    std::uint64_t source_sequence = 0U;
    std::uint64_t ingress_sequence = 0U;
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

// Dependency-light, integer-only input shared by the decoded-event adapter
// and a future Wire adapter. Prices are normalized p6 and quantities retain
// the decoder's native scale-zero units.
struct ShenzhenOrderEventInputV1 final {
    std::uint32_t trade_date = 0U;
    std::uint32_t instrument_id = 0U;
    std::uint32_t channel = 0U;
    ShenzhenEventSourceAnchorV1 anchor{};

    TickActionV1 action = TickActionV1::kUnknown;
    SideV1 side = SideV1::kUnknown;
    OrderTypeV1 order_type = OrderTypeV1::kUnknown;

    std::int64_t price_p6 = 0;
    std::int64_t quantity = 0;
    std::int64_t primary_order_id = 0;
    std::int64_t buy_order_id = 0;
    std::int64_t sell_order_id = 0;

    bool price_valid = false;
    bool quantity_valid = false;
    bool side_valid = false;
    bool order_type_valid = false;

    std::uint64_t source_quality_flags = 0U;
    std::uint64_t source_market_notices = 0U;
};

enum class ShenzhenOrderEventProjectionV1 : std::uint8_t {
    kProjected = 0U,
    kNotShenzhenEvent,
    kInvalidEvent,
};

[[nodiscard]] ShenzhenOrderEventProjectionV1
ProjectShenzhenOrderEventInputV1(
    const DecodedMarketEventV1& event,
    std::uint64_t arrival_id,
    ShenzhenOrderEventInputV1* output) noexcept;

struct ShenzhenOrderSnapshotV1 final {
    ShenzhenOrderKeyV1 key{};
    SideV1 side = SideV1::kUnknown;
    OrderTypeV1 order_type = OrderTypeV1::kUnknown;

    // Only a valid 6.33 limit price is published. Market and same-side-best
    // orders deliberately retain price_valid=false.
    std::int64_t price_p6 = 0;
    bool price_valid = false;

    // A 6.33 OrderQty is the exact original quantity.
    std::int64_t original_quantity = 0;
    std::int64_t remaining_quantity = 0;
    bool remaining_quantity_valid = false;
    std::int64_t total_trade_quantity = 0;
    std::int64_t total_cancel_quantity = 0;
    std::uint64_t trade_count = 0U;
    std::uint64_t cancel_count = 0U;

    ShenzhenEventSourceAnchorV1 first_anchor{};
    ShenzhenEventSourceAnchorV1 last_anchor{};
    std::uint64_t revision = 0U;
    ShenzhenOrderFinalityV1 finality =
        ShenzhenOrderFinalityV1::kProvisional;
    std::uint64_t quality_flags = 0U;
    std::uint64_t source_quality_flags = 0U;
    std::uint64_t source_market_notices = 0U;
};

struct ShenzhenOrderRevisionEventV1 final {
    ShenzhenOrderDeltaOperationV1 operation =
        ShenzhenOrderDeltaOperationV1::kInsert;
    ShenzhenEventSourceAnchorV1 source_anchor{};
    ShenzhenOrderSnapshotV1 order{};
};

// Exact projector checkpoint for one order.  The snapshot itself is already
// carried by every order-revision Event; the two booleans are the only live
// projector state which is not represented by that payload.  Ordered Event
// history uses this image to seed a sparse shadow projector at dirty-1.
struct ShenzhenOrderStateImageV1 final {
    ShenzhenOrderSnapshotV1 snapshot{};
    bool terminal = false;
    bool finalization_emitted = false;
};

struct ShenzhenTradeEventV1 final {
    std::uint32_t trade_date = 0U;
    std::uint32_t instrument_id = 0U;
    std::uint32_t channel = 0U;
    ShenzhenEventSourceAnchorV1 source_anchor{};
    std::int64_t buy_order_id = 0;
    std::int64_t sell_order_id = 0;
    AggressorV1 aggressor = AggressorV1::kUnknown;
    std::int64_t price_p6 = 0;
    std::int64_t quantity = 0;
    // 6.36 supplies no source amount. It is intentionally not synthesized
    // from price and quantity.
    std::int64_t amount_p6 = 0;
    bool amount_valid = false;
    std::uint64_t quality_flags = 0U;
    std::uint64_t source_quality_flags = 0U;
    std::uint64_t source_market_notices = 0U;
};

struct ShenzhenCancelEventV1 final {
    ShenzhenOrderKeyV1 key{};
    ShenzhenEventSourceAnchorV1 source_anchor{};
    SideV1 side = SideV1::kUnknown;
    // When the referenced 6.33 order is known, side is copied from that order.
    // Otherwise it is only the Buy/Sell position implied by which 6.36
    // reference is nonzero.
    bool side_from_order = false;
    std::int64_t quantity = 0;
    bool referenced_order_found = false;
    std::uint64_t quality_flags = 0U;
    std::uint64_t source_quality_flags = 0U;
    std::uint64_t source_market_notices = 0U;
};

using ShenzhenOrderEventV1 = std::variant<
    ShenzhenOrderRevisionEventV1,
    ShenzhenTradeEventV1,
    ShenzhenCancelEventV1>;

struct ShenzhenOrderEventProjectorConfigV1 final {
    std::uint32_t trade_date = 0U;
    std::size_t maximum_order_states = 0U;
};

enum class ShenzhenOrderProjectorCreateErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidConfiguration,
    kResourceExhausted,
};

enum class ShenzhenOrderProjectorConsumeErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidInput,
    kWrongTradeDate,
    kAlreadyFinalized,
    kOrderCapacity,
    kOutOfOrderInput,
    kResourceExhausted,
    kFailed,
};

enum class ShenzhenOrderProjectorQueryErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidKey,
    kNotFound,
};

[[nodiscard]] std::string_view
ShenzhenOrderProjectorCreateErrorNameV1(
    ShenzhenOrderProjectorCreateErrorV1 error) noexcept;
[[nodiscard]] std::string_view
ShenzhenOrderProjectorConsumeErrorNameV1(
    ShenzhenOrderProjectorConsumeErrorV1 error) noexcept;
[[nodiscard]] std::string_view
ShenzhenOrderProjectorQueryErrorNameV1(
    ShenzhenOrderProjectorQueryErrorV1 error) noexcept;

class ShenzhenOrderEventProjectorV1 final {
public:
    // One merged 6.33/6.36 projection lane owns an instance. Public methods,
    // including queries and destruction, are not safe for concurrent calls.
    ShenzhenOrderEventProjectorV1(
        const ShenzhenOrderEventProjectorV1&) = delete;
    ShenzhenOrderEventProjectorV1& operator=(
        const ShenzhenOrderEventProjectorV1&) = delete;
    ShenzhenOrderEventProjectorV1(
        ShenzhenOrderEventProjectorV1&&) = delete;
    ShenzhenOrderEventProjectorV1& operator=(
        ShenzhenOrderEventProjectorV1&&) = delete;
    ~ShenzhenOrderEventProjectorV1();

    [[nodiscard]] static ShenzhenOrderProjectorCreateErrorV1 Create(
        ShenzhenOrderEventProjectorConfigV1 config,
        std::unique_ptr<ShenzhenOrderEventProjectorV1>* output)
        noexcept;

    // A shadow projector stores only imported/touched orders while retaining
    // the logical base order count for capacity checks.  It is intended for
    // dirty-suffix replay and follows the exact same ConsumeBusinessOrdered
    // transition implementation as a live projector.
    [[nodiscard]] static ShenzhenOrderProjectorCreateErrorV1
    CreateSparseShadow(
        ShenzhenOrderEventProjectorConfigV1 config,
        std::size_t logical_base_order_count,
        std::unique_ptr<ShenzhenOrderEventProjectorV1>* output)
        noexcept;

    // Ordering is established exclusively by
    // (channel, ApplSeqNum) before this call. Source/arrival IDs remain
    // diagnostic and are not ordering inputs. Numeric gaps are valid.
    [[nodiscard]] ShenzhenOrderProjectorConsumeErrorV1
    ConsumeBusinessOrdered(
        const ShenzhenOrderEventInputV1& input,
        std::vector<ShenzhenOrderEventV1>* output) noexcept;

    [[nodiscard]] ShenzhenOrderProjectorConsumeErrorV1 ConsumeDecoded(
        const DecodedMarketEventV1& event,
        std::uint64_t arrival_id,
        std::vector<ShenzhenOrderEventV1>* output) noexcept;

    // The anchor may identify a real source message selected by the caller as
    // the clean trade-date boundary, or it may be all zero for a source-free
    // explicit boundary. Finalize never fabricates an ApplSeqNum.
    [[nodiscard]] ShenzhenOrderProjectorConsumeErrorV1 Finalize(
        const ShenzhenEventSourceAnchorV1& source_anchor,
        std::vector<ShenzhenOrderEventV1>* output) noexcept;

    [[nodiscard]] ShenzhenOrderProjectorQueryErrorV1 GetOrder(
        const ShenzhenOrderKeyV1& key,
        ShenzhenOrderSnapshotV1* output) const noexcept;
    [[nodiscard]] ShenzhenOrderProjectorQueryErrorV1 GetOrderState(
        const ShenzhenOrderKeyV1& key,
        ShenzhenOrderStateImageV1* output) const noexcept;

    // ImportExisting is allocation-bearing and is used only while building a
    // private sparse replay overlay. ReplaceOrInsert is allocation-free for a
    // live projector because BoundedOrderTableV1 owns its complete slab.
    [[nodiscard]] ShenzhenOrderProjectorConsumeErrorV1
    ImportExistingOrderState(
        const ShenzhenOrderStateImageV1& image) noexcept;
    [[nodiscard]] ShenzhenOrderProjectorConsumeErrorV1
    ReplaceOrInsertOrderState(
        const ShenzhenOrderStateImageV1& image) noexcept;
    [[nodiscard]] ShenzhenOrderProjectorConsumeErrorV1
    SetPreviousBusinessSequence(
        std::uint32_t channel,
        std::int64_t sequence) noexcept;
    [[nodiscard]] std::size_t order_count() const noexcept;
    [[nodiscard]] bool finalized() const noexcept;
    [[nodiscard]] const ShenzhenOrderEventProjectorConfigV1& config()
        const noexcept;

private:
    class Impl;
    explicit ShenzhenOrderEventProjectorV1(
        std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::market
