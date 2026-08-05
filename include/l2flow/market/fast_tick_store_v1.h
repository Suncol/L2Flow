#pragma once

#include "l2flow/common/identity128.h"
#include "l2flow/market/market_types_v1.h"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <span>
#include <string_view>
#include <variant>
#include <vector>

namespace l2flow::market {

inline constexpr std::size_t kFastTickSourceCountV1 = 2U;
inline constexpr std::size_t kFastTickDefaultRecordsPerChunkV1 = 256U;
inline constexpr std::size_t kFastTickMaximumRecordsPerChunkV1 = 4096U;

enum class FastTickSourceV1 : std::uint8_t {
    kShanghaiTick = 0U,
    kShenzhenTick = 1U,
};

// Business sequence is meaningful only inside one exchange channel. It is
// never compared across channels and is unrelated to arrival/source IDs.
struct BusinessSequenceV1 final {
    std::int32_t channel = 0;
    std::int64_t value = 0;

    [[nodiscard]] friend constexpr bool operator==(
        const BusinessSequenceV1&,
        const BusinessSequenceV1&) noexcept = default;
};

[[nodiscard]] constexpr bool BusinessSequenceLessV1(
    const BusinessSequenceV1& lhs,
    const BusinessSequenceV1& rhs) noexcept {
    return lhs.channel < rhs.channel ||
           (lhs.channel == rhs.channel && lhs.value < rhs.value);
}

// This is the only decoded payload variant accepted by the new production
// data path. Snapshot alternatives deliberately do not exist here.
using DecodedFastTickV1 = std::variant<
    ShanghaiTickV1,
    ShenzhenOrderV1,
    ShenzhenTransactionV1>;

// Fixed-size fan-out projection. FAST stores this beside the full owned Tick;
// Event and KLine queues copy only this object. arrival_id is diagnostic and
// provides a FAST-publication handshake; it is not an ordering key for Event
// or KLine history.
struct CompactFastTickV1 final {
    FastTickSourceV1 source = FastTickSourceV1::kShanghaiTick;
    MarketV1 market = MarketV1::kUnknown;
    MarketEventKindV1 kind = MarketEventKindV1::kShanghaiTick;
    std::uint32_t source_stream_id = 0U;
    std::uint64_t source_sequence = 0U;
    std::uint64_t arrival_id = 0U;
    std::uint64_t vendor_sequence_id = 0U;
    std::uint32_t trade_date = 0U;
    std::uint32_t instrument_id = 0U;
    std::size_t ordinal = std::numeric_limits<std::size_t>::max();
    BusinessSequenceV1 business_sequence{};
    std::uint64_t event_time_ns_since_midnight = 0U;
    std::int64_t event_time_unix_ns = 0;
    std::int64_t recv_realtime_ns = 0;
    std::int64_t recv_monotonic_ns = 0;
    std::uint32_t vendor_local_time_raw = 0U;
    std::uint64_t vendor_local_time_ns_since_midnight = 0U;
    TickActionV1 action = TickActionV1::kUnknown;
    SideV1 side = SideV1::kUnknown;
    OrderTypeV1 order_type = OrderTypeV1::kUnknown;
    AggressorV1 aggressor = AggressorV1::kUnknown;
    TradingPhaseV1 phase = TradingPhaseV1::kUnknown;
    std::int64_t price_p6 = 0;
    std::int64_t trade_amount_p6 = 0;
    std::int64_t quantity_raw = 0;
    std::int64_t matched_quantity_raw = 0;
    std::int64_t primary_order_id = 0;
    std::int64_t buy_order_id = 0;
    std::int64_t sell_order_id = 0;
    std::uint8_t quantity_scale = 0U;
    QuantityUnitV1 quantity_unit = QuantityUnitV1::kUnknown;
    std::uint32_t validity_bitmap = 0U;
    std::uint64_t quality_flags = 0U;
    std::uint64_t market_notices = 0U;
    std::int32_t raw_code_1 = 0;
    std::int32_t raw_code_2 = 0;
    bool event_time_valid = false;
    bool event_time_unix_ns_valid = false;
    bool vendor_local_time_valid = false;

    [[nodiscard]] friend constexpr bool operator==(
        const CompactFastTickV1&,
        const CompactFastTickV1&) noexcept = default;
};

enum class FastTickProjectionErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kNotTick,
    kSourceMismatch,
    kInvalidIdentity,
    kInvalidBusinessSequence,
};

[[nodiscard]] std::string_view FastTickProjectionErrorNameV1(
    FastTickProjectionErrorV1 error) noexcept;

// Moves one of the three supported Tick alternatives out of the decoder
// result and builds the compact projection before ownership is transferred to
// FAST. Any unsupported alternative returns kNotTick and leaves output empty.
[[nodiscard]] FastTickProjectionErrorV1 ProjectFastTickV1(
    DecodedMarketEventV1&& decoded,
    FastTickSourceV1 source,
    std::uint64_t arrival_id,
    DecodedFastTickV1* owned_tick,
    CompactFastTickV1* compact) noexcept;

[[nodiscard]] bool IsKLineTradeV1(
    const CompactFastTickV1& tick) noexcept;

// Compares exchange/business payload while deliberately ignoring process and
// transport anchors (arrival/source/vendor IDs and receive timestamps). It is
// used for idempotent source duplicate detection; a false result for the same
// BusinessSequence is a source conflict, not a second event.
[[nodiscard]] bool SameFastTickPayloadV1(
    const CompactFastTickV1& lhs,
    const CompactFastTickV1& rhs) noexcept;

class FastTickRecordV1 final {
public:
    FastTickRecordV1(const FastTickRecordV1&) = delete;
    FastTickRecordV1& operator=(const FastTickRecordV1&) = delete;
    FastTickRecordV1(FastTickRecordV1&&) = delete;
    FastTickRecordV1& operator=(FastTickRecordV1&&) = delete;
    ~FastTickRecordV1() = default;

    [[nodiscard]] std::uint64_t instrument_tick_sequence()
        const noexcept {
        return instrument_tick_sequence_;
    }
    [[nodiscard]] const CompactFastTickV1& compact() const noexcept {
        return compact_;
    }
    [[nodiscard]] const DecodedFastTickV1& decoded() const noexcept {
        return decoded_;
    }

private:
    FastTickRecordV1(
        std::uint64_t instrument_tick_sequence,
        CompactFastTickV1 compact,
        DecodedFastTickV1&& decoded) noexcept;

    std::uint64_t instrument_tick_sequence_ = 0U;
    CompactFastTickV1 compact_{};
    DecodedFastTickV1 decoded_;

    friend class FastTickStoreV1;
};

struct FastTickRouteTokenV1 final {
    std::uint32_t instrument_id = 0U;
    std::size_t ordinal = std::numeric_limits<std::size_t>::max();
    std::uint32_t worker = std::numeric_limits<std::uint32_t>::max();
};

struct FastTickStoreConfigV1 final {
    l2flow::common::Identity128 session_id{};
    std::uint32_t trade_date = 0U;
    std::size_t instrument_count = 0U;
    std::uint32_t worker_count = 0U;
    std::uint64_t maximum_session_records = 0U;
    // Optional permanent per-instrument capacity partition. When omitted,
    // maximum_session_records is divided as evenly as possible by ordinal.
    // When supplied, every entry must be positive and the sum must equal
    // maximum_session_records. This makes exhaustion instrument-local and
    // prevents partially used pages for one instrument from silently
    // consuming another instrument's advertised record capacity.
    std::vector<std::uint64_t> instrument_record_capacities;
    std::size_t records_per_chunk =
        kFastTickDefaultRecordsPerChunkV1;
    std::size_t maximum_records_per_read = 64U * 1024U;
    bool coverage_from_open = false;
    // One permanent worker ID for every dense catalog ordinal.
    std::vector<std::uint32_t> tick_routes;
};

enum class FastTickStoreCreateErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidConfiguration,
    kResourceExhausted,
};

enum class FastTickStoreAppendErrorV1 : std::uint8_t {
    kNone = 0U,
    kInvalidInput,
    kWrongWorker,
    kCoverageLost,
    kRecordCapacity,
    kResourceExhausted,
};

enum class FastTickStoreQueryErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidArgument,
    kNotFound,
    kBatchLimitExceeded,
    kResourceExhausted,
};

[[nodiscard]] std::string_view FastTickStoreCreateErrorNameV1(
    FastTickStoreCreateErrorV1 error) noexcept;
[[nodiscard]] std::string_view FastTickStoreAppendErrorNameV1(
    FastTickStoreAppendErrorV1 error) noexcept;
[[nodiscard]] std::string_view FastTickStoreQueryErrorNameV1(
    FastTickStoreQueryErrorV1 error) noexcept;

struct FastTickInstrumentStatusV1 final {
    std::uint32_t instrument_id = 0U;
    std::uint64_t published_tail = 0U;
    std::uint64_t record_capacity = 0U;
    std::uint64_t coverage_start = 1U;
    std::uint64_t latest_arrival_id = 0U;
    BusinessSequenceV1 latest_source_anchor{};
    bool coverage_from_open = false;
    bool coverage_complete = true;
};

struct FastTickStoreSnapshotV1 final {
    std::uint64_t maximum_session_records = 0U;
    std::uint64_t appended_records = 0U;
    std::uint64_t failed_appends = 0U;
    std::uint64_t allocated_chunks = 0U;
    std::uint64_t chunk_capacity = 0U;
    bool any_coverage_lost = false;
};

class FastTickCursorV1 final {
public:
    FastTickCursorV1(const FastTickCursorV1&) = delete;
    FastTickCursorV1& operator=(const FastTickCursorV1&) = delete;
    FastTickCursorV1(FastTickCursorV1&&) noexcept;
    FastTickCursorV1& operator=(FastTickCursorV1&&) noexcept;
    ~FastTickCursorV1();

    // Pointers remain owned by the append-only store, which must outlive the
    // cursor and every returned pointer. A cursor captures one
    // instrument-local tail; later appends are intentionally not included.
    [[nodiscard]] FastTickStoreQueryErrorV1 ReadBatch(
        std::span<const FastTickRecordV1*> output,
        std::size_t* written) noexcept;
    [[nodiscard]] bool done() const noexcept;
    [[nodiscard]] std::uint64_t next_arrival_row() const noexcept;
    [[nodiscard]] std::uint64_t target_tail() const noexcept;

private:
    class Impl;
    explicit FastTickCursorV1(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;

    friend class FastTickStoreV1;
};

// Append-only, per-instrument FAST fact history. Append must be called by the
// permanent worker named by the route token. Readers acquire only published
// instrument-local tails; no global live ring or global contiguous frontier
// exists in this type.
class FastTickStoreV1 final {
public:
    FastTickStoreV1(const FastTickStoreV1&) = delete;
    FastTickStoreV1& operator=(const FastTickStoreV1&) = delete;
    FastTickStoreV1(FastTickStoreV1&&) = delete;
    FastTickStoreV1& operator=(FastTickStoreV1&&) = delete;
    ~FastTickStoreV1();

    [[nodiscard]] static FastTickStoreCreateErrorV1 Create(
        FastTickStoreConfigV1 config,
        std::unique_ptr<FastTickStoreV1>* output) noexcept;

    [[nodiscard]] FastTickStoreQueryErrorV1 ResolveRoute(
        std::size_t ordinal,
        std::uint32_t instrument_id,
        FastTickRouteTokenV1* output) const noexcept;

    [[nodiscard]] FastTickStoreAppendErrorV1 Append(
        std::uint32_t worker,
        const FastTickRouteTokenV1& route,
        CompactFastTickV1 compact,
        DecodedFastTickV1&& decoded,
        const FastTickRecordV1** appended_record = nullptr) noexcept;

    [[nodiscard]] FastTickStoreQueryErrorV1 Latest(
        std::uint32_t instrument_id,
        const FastTickRecordV1** output) const noexcept;
    [[nodiscard]] FastTickStoreQueryErrorV1 Status(
        std::uint32_t instrument_id,
        FastTickInstrumentStatusV1* output) const noexcept;
    [[nodiscard]] bool PublishedThrough(
        std::uint32_t instrument_id,
        std::uint64_t arrival_id) const noexcept;

    // next_arrival_row is one-based. tail+1 is a valid empty append cursor.
    [[nodiscard]] FastTickStoreQueryErrorV1 OpenCursor(
        std::uint32_t instrument_id,
        std::uint64_t next_arrival_row,
        std::unique_ptr<FastTickCursorV1>* output) const noexcept;

    // Cold recovery helper. It snapshots the instrument tail and copies only
    // compact fixed-size rows, never the full owned decoded payload.
    [[nodiscard]] FastTickStoreQueryErrorV1 CopyCompactHistory(
        std::uint32_t instrument_id,
        std::vector<CompactFastTickV1>* output,
        std::uint64_t* captured_tail = nullptr) const noexcept;

    void MarkCoverageLost(std::uint32_t instrument_id) noexcept;
    [[nodiscard]] FastTickStoreSnapshotV1 Snapshot() const noexcept;
    [[nodiscard]] const FastTickStoreConfigV1& config() const noexcept;

private:
    class Impl;
    explicit FastTickStoreV1(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::market
