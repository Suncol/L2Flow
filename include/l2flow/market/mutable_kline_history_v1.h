#pragma once

#include "l2flow/common/identity128.h"
#include "l2flow/market/fast_tick_store_v1.h"
#include "l2flow/market/kline_types_v1.h"
#include "l2flow/market/ordered_event_history_v1.h"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <span>
#include <string_view>
#include <vector>

namespace l2flow::market {

struct KLineBarKeyV1 final {
    std::uint32_t instrument_id = 0U;
    std::uint32_t window_id = 0U;
    std::uint64_t window_start_ns_since_midnight = 0U;

    [[nodiscard]] friend constexpr bool operator==(
        const KLineBarKeyV1&, const KLineBarKeyV1&) noexcept = default;
};

[[nodiscard]] bool KLineBarKeyLessV1(
    const KLineBarKeyV1& lhs,
    const KLineBarKeyV1& rhs) noexcept;

struct KLineTradeUidV1 final {
    std::uint32_t instrument_id = 0U;
    std::int32_t channel = 0;
    std::int64_t business_sequence = 0;

    [[nodiscard]] friend constexpr bool operator==(
        const KLineTradeUidV1&, const KLineTradeUidV1&) noexcept =
        default;
};

enum class KLineMutationKindV1 : std::uint8_t {
    kUpsert = 0U,
    kDelete,
    kRangeReplaceBegin,
    kRangeReplaceChunk,
    kRangeReplaceCommit,
};

struct KLineMutationV1 final {
    std::uint64_t change_sequence = 0U;
    std::uint64_t transaction_id = 0U;
    KLineMutationKindV1 kind = KLineMutationKindV1::kUpsert;
    KLineBarKeyV1 key{};
    KLineBarV1 bar{};
    // Rebuild transactions currently replace the complete instrument. The
    // chunks remain invisible to CDC consumers until the matching COMMIT.
    bool replace_entire_instrument = false;
    std::vector<KLineBarV1> replacement_bars;
};

struct KLineChangeCursorV1 final {
    l2flow::common::Identity128 session_id{};
    std::uint32_t instrument_id = 0U;
    std::uint64_t next_change_sequence = 1U;
};

class KLineStableRootV1 final {
public:
    KLineStableRootV1(const KLineStableRootV1&) = delete;
    KLineStableRootV1& operator=(const KLineStableRootV1&) = delete;
    KLineStableRootV1(KLineStableRootV1&&) = delete;
    KLineStableRootV1& operator=(KLineStableRootV1&&) = delete;
    ~KLineStableRootV1();

    [[nodiscard]] std::uint32_t instrument_id() const noexcept;
    [[nodiscard]] std::uint64_t included_change_sequence()
        const noexcept;
    [[nodiscard]] std::uint64_t bar_count() const noexcept;
    [[nodiscard]] bool CopyBars(std::vector<KLineBarV1>* output)
        const noexcept;
    [[nodiscard]] bool Find(
        const KLineBarKeyV1& key,
        KLineBarV1* output) const noexcept;

private:
    class Impl;
    explicit KLineStableRootV1(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;

    friend class MutableKLineHistoryV1;
};

struct KLineStableSnapshotV1 final {
    std::shared_ptr<const KLineStableRootV1> root;
    KLineChangeCursorV1 next_changes{};
    EventRepairStateV1 repair_state = EventRepairStateV1::kLive;
    std::uint64_t repair_through_arrival_id = 0U;
};

struct KLineRouteTokenV1 final {
    std::uint32_t instrument_id = 0U;
    std::size_t ordinal = std::numeric_limits<std::size_t>::max();
    std::uint32_t worker = std::numeric_limits<std::uint32_t>::max();
};

struct MutableKLineHistoryConfigV1 final {
    l2flow::common::Identity128 session_id{};
    std::uint32_t trade_date = 0U;
    std::size_t instrument_count = 0U;
    std::uint32_t worker_count = 0U;
    std::vector<KLineWindowSpecV1> windows;
    std::size_t maximum_trades_per_instrument = 0U;
    std::size_t maximum_bars_per_instrument = 0U;
    std::size_t stable_block_bars = 64U;
    std::size_t cdc_range_chunk_bars = 1024U;
    // Complete per-instrument CDC retention for the session. Publication
    // fails closed before this bound is exceeded; the vector allocator is
    // never permitted to grow past it.
    std::size_t maximum_change_records_per_instrument = 0U;
    std::size_t maximum_changes_per_read = 64U * 1024U;
    // RebuildFromFast cooperatively yields its plane CPU after at most this
    // many raw FAST records.
    std::size_t repair_replay_record_budget = 4096U;
    std::vector<std::uint32_t> kline_routes;
};

enum class MutableKLineHistoryCreateErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidConfiguration,
    kResourceExhausted,
};

enum class MutableKLineHistoryErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidInput,
    kWrongWorker,
    kNotLive,
    kNotTrade,
    kTradeCapacity,
    kBarCapacity,
    kNumericOverflow,
    kSourceConflict,
    kFastCoverageLost,
    kChangeCapacity,
    kCursorMismatch,
    kBatchLimitExceeded,
    kResourceExhausted,
};

[[nodiscard]] std::string_view MutableKLineHistoryCreateErrorNameV1(
    MutableKLineHistoryCreateErrorV1 error) noexcept;
[[nodiscard]] std::string_view MutableKLineHistoryErrorNameV1(
    MutableKLineHistoryErrorV1 error) noexcept;

enum class KLineInputDispositionV1 : std::uint8_t {
    kUpserted = 0U,
    kNotTrade,
    kDuplicateIgnored,
    kSourceConflict,
};

struct KLineApplyResultV1 final {
    MutableKLineHistoryErrorV1 error =
        MutableKLineHistoryErrorV1::kNone;
    KLineInputDispositionV1 disposition =
        KLineInputDispositionV1::kNotTrade;
    std::uint64_t upserted_bars = 0U;
};

struct KLineRebuildResultV1 final {
    MutableKLineHistoryErrorV1 error =
        MutableKLineHistoryErrorV1::kNone;
    std::uint64_t captured_fast_tail = 0U;
    std::uint64_t rebuilt_trades = 0U;
    std::uint64_t upserted_bars = 0U;
    std::uint64_t deleted_bars = 0U;
    bool complete = false;
};

struct MutableKLineHistoryStatsV1 final {
    std::uint64_t applied_trades = 0U;
    std::uint64_t late_window_updates = 0U;
    std::uint64_t duplicate_trades = 0U;
    std::uint64_t source_conflicts = 0U;
    std::uint64_t rebuilds = 0U;
};

class MutableKLineHistoryV1 final {
public:
    MutableKLineHistoryV1(const MutableKLineHistoryV1&) = delete;
    MutableKLineHistoryV1& operator=(const MutableKLineHistoryV1&) =
        delete;
    MutableKLineHistoryV1(MutableKLineHistoryV1&&) = delete;
    MutableKLineHistoryV1& operator=(MutableKLineHistoryV1&&) = delete;
    ~MutableKLineHistoryV1();

    [[nodiscard]] static MutableKLineHistoryCreateErrorV1 Create(
        MutableKLineHistoryConfigV1 config,
        std::unique_ptr<MutableKLineHistoryV1>* output) noexcept;

    [[nodiscard]] MutableKLineHistoryErrorV1 ResolveRoute(
        std::size_t ordinal,
        std::uint32_t instrument_id,
        KLineRouteTokenV1* output) const noexcept;

    [[nodiscard]] KLineApplyResultV1 ApplyLive(
        std::uint32_t worker,
        const KLineRouteTokenV1& route,
        const CompactFastTickV1& tick) noexcept;

    [[nodiscard]] KLineRebuildResultV1 RebuildFromFast(
        std::uint32_t worker,
        const KLineRouteTokenV1& route,
        const FastTickStoreV1& fast_store) noexcept;

    void MarkRepairRequired(
        std::uint32_t instrument_id,
        std::uint64_t through_arrival_id) noexcept;
    void MarkUnrecoverable(std::uint32_t instrument_id) noexcept;

    [[nodiscard]] MutableKLineHistoryErrorV1 AcquireStable(
        std::uint32_t instrument_id,
        KLineStableSnapshotV1* output) const noexcept;
    [[nodiscard]] MutableKLineHistoryErrorV1 ReadChanges(
        KLineChangeCursorV1* cursor,
        std::span<KLineMutationV1> output,
        std::size_t* written) const noexcept;

    [[nodiscard]] EventRepairStateV1 RepairState(
        std::uint32_t instrument_id) const noexcept;
    [[nodiscard]] std::uint64_t RepairThrough(
        std::uint32_t instrument_id) const noexcept;
    [[nodiscard]] MutableKLineHistoryStatsV1 Stats() const noexcept;
    [[nodiscard]] const MutableKLineHistoryConfigV1& config()
        const noexcept;

private:
    class Impl;
    explicit MutableKLineHistoryV1(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::market
