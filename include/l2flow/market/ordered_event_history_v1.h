#pragma once

#include "l2flow/common/identity128.h"
#include "l2flow/market/fast_tick_store_v1.h"
#include "l2flow/market/shanghai_order_event_aggregator_v1.h"
#include "l2flow/market/shenzhen_order_event_projector_v1.h"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <span>
#include <string_view>
#include <variant>
#include <vector>

namespace l2flow::market {

using DerivedEventPayloadV1 = std::variant<
    ShanghaiOrderRevisionEventV1,
    ShanghaiTradeEventV1,
    ShanghaiCancelEventV1,
    ShanghaiStatusEventV1,
    ShenzhenOrderRevisionEventV1,
    ShenzhenTradeEventV1,
    ShenzhenCancelEventV1>;

enum class DerivedEventKindV1 : std::uint8_t {
    kShanghaiOrderRevision = 1U,
    kShanghaiTrade,
    kShanghaiCancel,
    kShanghaiStatus,
    kShenzhenOrderRevision,
    kShenzhenTrade,
    kShenzhenCancel,
};

// Stable identity is independent of dense row position. affected_order_id is
// zero for source-level Trade/Status events. occurrence disambiguates the
// rare case where one source input emits multiple rows of the same kind for
// the same affected key.
struct EventUidV1 final {
    std::uint32_t instrument_id = 0U;
    std::int32_t channel = 0;
    std::int64_t business_sequence = 0;
    DerivedEventKindV1 kind =
        DerivedEventKindV1::kShanghaiOrderRevision;
    std::int64_t affected_order_id = 0;
    std::uint32_t occurrence = 0U;

    [[nodiscard]] friend constexpr bool operator==(
        const EventUidV1&, const EventUidV1&) noexcept = default;
};

struct EventOrderKeyV1 final {
    std::int32_t channel = 0;
    std::int64_t business_sequence = 0;
    std::uint32_t source_event_ordinal = 0U;
    std::uint32_t derived_event_ordinal = 0U;
    std::int64_t affected_order_id = 0;

    [[nodiscard]] friend constexpr bool operator==(
        const EventOrderKeyV1&, const EventOrderKeyV1&) noexcept =
        default;
};

[[nodiscard]] bool EventOrderKeyLessV1(
    const EventOrderKeyV1& lhs,
    const EventOrderKeyV1& rhs) noexcept;

struct OrderedDerivedEventV1 final {
    EventUidV1 uid{};
    EventOrderKeyV1 order_key{};
    DerivedEventPayloadV1 payload{};
    std::uint64_t source_arrival_id = 0U;
};

enum class EventRepairStateV1 : std::uint8_t {
    kLive = 0U,
    kRepairRequired,
    kRebuilding,
    kCatchingUp,
    kSourceConflict,
    kUnrecoverable,
};

enum class EventMutationKindV1 : std::uint8_t {
    kInsert = 0U,
    kUpdate,
    kDelete,
    kRangeReplaceBegin,
    kRangeReplaceChunk,
    kRangeReplaceCommit,
};

enum class EventRangeReplaceScopeV1 : std::uint8_t {
    kInstrumentAll = 0U,
    kChannelSuffix,
};

struct EventMutationV1 final {
    std::uint64_t change_sequence = 0U;
    std::uint64_t transaction_id = 0U;
    EventMutationKindV1 kind = EventMutationKindV1::kInsert;
    EventUidV1 uid{};
    // Retained for Wire/API compatibility with cold full rebuilds and the
    // previous bounded-key range protocol. New late-data repair uses the
    // explicit kChannelSuffix fields below and never fabricates an end key.
    bool replace_entire_instrument = false;
    EventOrderKeyV1 range_begin{};
    EventOrderKeyV1 range_end_exclusive{};
    OrderedDerivedEventV1 row{};
    std::vector<OrderedDerivedEventV1> replacement_rows;
    // Appended after the legacy fields so source-level positional aggregate
    // initialization retains its prior meaning.
    EventRangeReplaceScopeV1 range_scope =
        EventRangeReplaceScopeV1::kInstrumentAll;
    std::int32_t range_channel = 0;
    std::int64_t range_begin_business_sequence = 0;
};

struct EventChangeCursorV1 final {
    l2flow::common::Identity128 session_id{};
    std::uint32_t instrument_id = 0U;
    std::uint64_t next_change_sequence = 1U;
};

class EventStableRootV1 final {
public:
    EventStableRootV1(const EventStableRootV1&) = delete;
    EventStableRootV1& operator=(const EventStableRootV1&) = delete;
    EventStableRootV1(EventStableRootV1&&) = delete;
    EventStableRootV1& operator=(EventStableRootV1&&) = delete;
    ~EventStableRootV1();

    [[nodiscard]] std::uint32_t instrument_id() const noexcept;
    [[nodiscard]] std::uint64_t included_change_sequence()
        const noexcept;
    [[nodiscard]] std::uint64_t row_count() const noexcept;
    [[nodiscard]] bool strictly_ordered() const noexcept;
    [[nodiscard]] bool CopyRows(
        std::vector<OrderedDerivedEventV1>* output) const noexcept;

private:
    class Impl;
    explicit EventStableRootV1(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;

    friend class OrderedEventHistoryV1;
};

struct EventStableSnapshotV1 final {
    std::shared_ptr<const EventStableRootV1> root;
    EventChangeCursorV1 next_changes{};
    EventRepairStateV1 repair_state = EventRepairStateV1::kLive;
    std::uint64_t repair_through_arrival_id = 0U;
};

struct EventRouteTokenV1 final {
    std::uint32_t instrument_id = 0U;
    std::size_t ordinal = std::numeric_limits<std::size_t>::max();
    std::uint32_t worker = std::numeric_limits<std::uint32_t>::max();
};

struct OrderedEventHistoryConfigV1 final {
    l2flow::common::Identity128 session_id{};
    std::uint32_t trade_date = 0U;
    std::size_t instrument_count = 0U;
    std::uint32_t worker_count = 0U;
    std::size_t maximum_order_states_per_instrument = 0U;
    std::size_t maximum_inputs_per_instrument = 0U;
    std::size_t maximum_events_per_instrument = 0U;
    std::size_t input_block_records = 256U;
    std::size_t event_block_records = 256U;
    std::size_t cdc_range_chunk_records = 1024U;
    // Complete per-instrument CDC retention for the session. Publication
    // fails closed before this bound is exceeded; the vector allocator is
    // never permitted to grow past it.
    std::size_t maximum_change_records_per_instrument = 0U;
    std::size_t maximum_changes_per_read = 64U * 1024U;
    // A repair turn processes at most this many source records; the Event
    // worker also applies the time bound below before returning to live work.
    std::size_t repair_replay_record_budget = 4096U;
    std::vector<std::uint32_t> event_routes;
    // New suffix-replay controls are appended after the legacy aggregate
    // fields so positional initialization keeps its prior meaning.
    // Mutable tail is a bounded cache only; source inputs remain permanently
    // present in the per-channel input journal.
    std::size_t mutable_tail_records = 128U;
    std::chrono::nanoseconds repair_cpu_budget_per_round =
        std::chrono::microseconds(250);
};

enum class OrderedEventHistoryCreateErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidConfiguration,
    kResourceExhausted,
};

enum class OrderedEventHistoryErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidInput,
    kWrongWorker,
    kNotLive,
    kInputCapacity,
    kEventCapacity,
    kProjectionFailed,
    kCoreFailed,
    kSourceConflict,
    kFastCoverageLost,
    kChangeCapacity,
    kCursorMismatch,
    kBatchLimitExceeded,
    kResourceExhausted,
};

[[nodiscard]] std::string_view OrderedEventHistoryCreateErrorNameV1(
    OrderedEventHistoryCreateErrorV1 error) noexcept;
[[nodiscard]] std::string_view OrderedEventHistoryErrorNameV1(
    OrderedEventHistoryErrorV1 error) noexcept;

enum class EventInputDispositionV1 : std::uint8_t {
    kPublished = 0U,
    kNoDerivedRows,
    kDuplicateIgnored,
    kRepairRegistered,
    kSourceConflict,
};

struct EventApplyResultV1 final {
    OrderedEventHistoryErrorV1 error =
        OrderedEventHistoryErrorV1::kNone;
    EventInputDispositionV1 disposition =
        EventInputDispositionV1::kNoDerivedRows;
    std::uint64_t published_rows = 0U;
    BusinessSequenceV1 dirty_from{};
};

struct EventRebuildResultV1 final {
    OrderedEventHistoryErrorV1 error =
        OrderedEventHistoryErrorV1::kNone;
    std::uint64_t captured_fast_tail = 0U;
    std::uint64_t rebuilt_inputs = 0U;
    std::uint64_t rebuilt_rows = 0U;
    std::uint64_t range_transaction_id = 0U;
    bool complete = false;
};

struct EventBatchApplyResultV1 final {
    OrderedEventHistoryErrorV1 error =
        OrderedEventHistoryErrorV1::kNone;
    std::uint64_t accepted_inputs = 0U;
    std::uint64_t published_inputs = 0U;
    std::uint64_t published_rows = 0U;
    std::uint64_t duplicate_inputs = 0U;
    std::uint64_t dirty_channels = 0U;
};

struct EventDirtyReplayResultV1 final {
    OrderedEventHistoryErrorV1 error =
        OrderedEventHistoryErrorV1::kNone;
    std::uint64_t replayed_inputs = 0U;
    std::uint64_t rebuilt_rows = 0U;
    std::uint64_t range_transaction_id = 0U;
    bool worked = false;
    bool committed = false;
    bool cold_fallback_required = false;
};

struct OrderedEventHistoryStatsV1 final {
    std::uint64_t monotonic_fast_path_inputs = 0U;
    std::uint64_t duplicate_inputs = 0U;
    std::uint64_t late_inputs = 0U;
    std::uint64_t source_conflicts = 0U;
    std::uint64_t rebuild_no_sort = 0U;
    std::uint64_t rebuild_natural_run_merge = 0U;
    std::uint64_t rebuild_radix_sort = 0U;
    std::uint64_t full_comparison_sort_calls = 0U;
    std::uint64_t published_range_transactions = 0U;
    std::uint64_t mutable_tail_repairs = 0U;
    std::uint64_t deep_suffix_repairs = 0U;
    std::uint64_t dirty_replay_inputs = 0U;
    std::uint64_t cold_fast_rebuilds = 0U;
};

// One Event worker is the sole writer for every instrument on its immutable
// route. Normal monotonic input never invokes a sorter. Late input leaves the
// prior immutable root visible while the worker builds and atomically commits
// a checkpointed channel suffix. RebuildFromFast is the cold fallback for a
// journal gap, not the ordinary out-of-order path.
class OrderedEventHistoryV1 final {
public:
    OrderedEventHistoryV1(const OrderedEventHistoryV1&) = delete;
    OrderedEventHistoryV1& operator=(const OrderedEventHistoryV1&) =
        delete;
    OrderedEventHistoryV1(OrderedEventHistoryV1&&) = delete;
    OrderedEventHistoryV1& operator=(OrderedEventHistoryV1&&) = delete;
    ~OrderedEventHistoryV1();

    [[nodiscard]] static OrderedEventHistoryCreateErrorV1 Create(
        OrderedEventHistoryConfigV1 config,
        std::unique_ptr<OrderedEventHistoryV1>* output) noexcept;

    [[nodiscard]] OrderedEventHistoryErrorV1 ResolveRoute(
        std::size_t ordinal,
        std::uint32_t instrument_id,
        EventRouteTokenV1* output) const noexcept;

    [[nodiscard]] EventApplyResultV1 ApplyLive(
        std::uint32_t worker,
        const EventRouteTokenV1& route,
        const CompactFastTickV1& tick) noexcept;

    // The Event worker drains a micro-batch, journals every source fact first,
    // and only then publishes monotonic channels or marks a dirty suffix. This
    // makes an order/trade inversion contained in one drain batch invisible
    // to readers until its correct business-ordered bundle is available.
    [[nodiscard]] EventBatchApplyResultV1 ApplyBatch(
        std::uint32_t worker,
        std::span<const CompactFastTickV1> ticks) noexcept;

    // Advances at most one dirty channel for this route. New appends are
    // followed in-place; an insertion behind the replay cursor restarts from
    // the earliest dirty key. Allocation and projection happen before the
    // suffix root/CDC transaction is atomically committed.
    [[nodiscard]] EventDirtyReplayResultV1 AdvanceDirtyReplay(
        std::uint32_t worker,
        const EventRouteTokenV1& route,
        std::size_t record_budget,
        std::chrono::nanoseconds cpu_budget) noexcept;

    [[nodiscard]] EventRebuildResultV1 RebuildFromFast(
        std::uint32_t worker,
        const EventRouteTokenV1& route,
        const FastTickStoreV1& fast_store) noexcept;

    void MarkRepairRequired(
        std::uint32_t instrument_id,
        std::uint64_t through_arrival_id) noexcept;
    void MarkUnrecoverable(std::uint32_t instrument_id) noexcept;

    [[nodiscard]] OrderedEventHistoryErrorV1 AcquireStable(
        std::uint32_t instrument_id,
        EventStableSnapshotV1* output) const noexcept;

    [[nodiscard]] OrderedEventHistoryErrorV1 ReadChanges(
        EventChangeCursorV1* cursor,
        std::span<EventMutationV1> output,
        std::size_t* written) const noexcept;

    [[nodiscard]] EventRepairStateV1 RepairState(
        std::uint32_t instrument_id) const noexcept;
    [[nodiscard]] std::uint64_t RepairThrough(
        std::uint32_t instrument_id) const noexcept;
    [[nodiscard]] OrderedEventHistoryStatsV1 Stats() const noexcept;
    [[nodiscard]] const OrderedEventHistoryConfigV1& config()
        const noexcept;

private:
    class Impl;
    explicit OrderedEventHistoryV1(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::market
