#pragma once

#include "l2flow/market/market_types_v1.h"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string_view>
#include <type_traits>
#include <vector>

namespace l2flow::market {

inline constexpr std::size_t kInstrumentHistorySourceCountV1 = 4U;
inline constexpr std::size_t kInstrumentHistoryLogicalShardCountV1 = 16U;

// This runtime indexes owned Phase-4 decoded market events after the matching
// Canonical bundle has committed.  The payload remains the decoder-domain
// event: it does not copy Canonical-only enrichment such as SH tick phase
// attribution, sticky sequence-quality flags, or Canonical event IDs.  Factor
// code which requires those fields must consume/join the committed Canonical
// record rather than treating this history as a Canonical projection.

// Source lanes are deliberately independent.  In particular, a worker's
// scheduling order never creates a cross-source market order.  Shenzhen order
// and transaction messages both belong to kTick.
enum class InstrumentHistoryLaneV1 : std::uint8_t {
    kSnapshot = 0U,
    kTick = 1U,
};

[[nodiscard]] InstrumentHistoryLaneV1 InstrumentHistoryLaneForKindV1(
    MarketEventKindV1 kind) noexcept;

enum class OwnedInstrumentEventCreateErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidSourceSlot,
    kNullEvent,
    kBorrowedBody,
    kInvalidSource,
    kInvalidSourceSequence,
    kInvalidReceiveTime,
    kUnknownInstrument,
    kInvalidKind,
};

[[nodiscard]] std::string_view OwnedInstrumentEventCreateErrorNameV1(
    OwnedInstrumentEventCreateErrorV1 error) noexcept;

// The envelope owns the exact retained event and is intentionally move-only.
// All routing metadata is derived from the retained event; callers cannot
// claim an instrument, lane, or source sequence that disagrees with it.
class OwnedInstrumentEventEnvelopeV1 final {
public:
    OwnedInstrumentEventEnvelopeV1(
        const OwnedInstrumentEventEnvelopeV1&) = delete;
    OwnedInstrumentEventEnvelopeV1& operator=(
        const OwnedInstrumentEventEnvelopeV1&) = delete;
    OwnedInstrumentEventEnvelopeV1(
        OwnedInstrumentEventEnvelopeV1&&) noexcept = default;
    OwnedInstrumentEventEnvelopeV1& operator=(
        OwnedInstrumentEventEnvelopeV1&&) noexcept = default;
    ~OwnedInstrumentEventEnvelopeV1() = default;

    [[nodiscard]] static OwnedInstrumentEventCreateErrorV1 Create(
        std::uint8_t source_slot,
        RetainedMarketEventV1 event,
        std::optional<OwnedInstrumentEventEnvelopeV1>* output) noexcept;

    [[nodiscard]] std::uint8_t source_slot() const noexcept {
        return source_slot_;
    }
    [[nodiscard]] std::uint32_t source_stream_id() const noexcept {
        return source_stream_id_;
    }
    [[nodiscard]] std::uint64_t source_sequence() const noexcept {
        return source_sequence_;
    }
    [[nodiscard]] std::uint32_t instrument_id() const noexcept {
        return instrument_id_;
    }
    [[nodiscard]] std::uint8_t logical_shard() const noexcept {
        return logical_shard_;
    }
    [[nodiscard]] InstrumentHistoryLaneV1 lane() const noexcept {
        return lane_;
    }
    [[nodiscard]] MarketEventKindV1 kind() const noexcept {
        return kind_;
    }
    [[nodiscard]] std::int64_t recv_monotonic_ns() const noexcept {
        return recv_monotonic_ns_;
    }
    [[nodiscard]] std::size_t owned_payload_bytes() const noexcept {
        return owned_payload_bytes_;
    }
    [[nodiscard]] std::uint64_t dispatch_ticket() const noexcept {
        return dispatch_ticket_;
    }
    [[nodiscard]] const RetainedMarketEventV1& event() const noexcept {
        return event_;
    }

private:
    friend class InstrumentHistoryRuntimeV1;

    OwnedInstrumentEventEnvelopeV1(
        std::uint8_t source_slot,
        std::uint32_t source_stream_id,
        std::uint64_t source_sequence,
        std::uint32_t instrument_id,
        InstrumentHistoryLaneV1 lane,
        MarketEventKindV1 kind,
        std::int64_t recv_monotonic_ns,
        std::size_t owned_payload_bytes,
        RetainedMarketEventV1 event) noexcept;

    void SetDispatchTicket(std::uint64_t ticket) noexcept {
        dispatch_ticket_ = ticket;
    }

    std::uint8_t source_slot_ = 0U;
    std::uint8_t logical_shard_ = 0U;
    InstrumentHistoryLaneV1 lane_ = InstrumentHistoryLaneV1::kSnapshot;
    MarketEventKindV1 kind_ = MarketEventKindV1::kShanghaiSnapshot;
    std::uint32_t source_stream_id_ = 0U;
    std::uint64_t source_sequence_ = 0U;
    std::uint32_t instrument_id_ = 0U;
    std::int64_t recv_monotonic_ns_ = 0;
    std::size_t owned_payload_bytes_ = 0U;
    std::uint64_t dispatch_ticket_ = 0U;
    RetainedMarketEventV1 event_;
};

static_assert(std::is_nothrow_move_constructible_v<
              OwnedInstrumentEventEnvelopeV1>);
static_assert(std::is_nothrow_move_assignable_v<
              OwnedInstrumentEventEnvelopeV1>);

enum class InstrumentHistoryCreateErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidSourceIds,
    kInvalidWorkerCount,
    kInvalidWorkerMapping,
    kInvalidQueueCapacity,
    kInvalidInflightLimit,
    kInvalidChunkCapacity,
    kInvalidStoreLimits,
    kResourceExhausted,
    kThreadStartFailed,
    kInvalidQueryLimit,
};

[[nodiscard]] std::string_view InstrumentHistoryCreateErrorNameV1(
    InstrumentHistoryCreateErrorV1 error) noexcept;

struct InstrumentHistoryRuntimeConfigV1 final {
    std::array<std::uint32_t, kInstrumentHistorySourceCountV1>
        source_stream_ids{};
    // Must be selected explicitly in [1, 16] from the deployment's CPU/NUMA
    // budget.  Zero deliberately fails creation instead of silently running
    // the production route on one worker.
    std::uint32_t physical_worker_count = 0U;
    // When false, logical shard s is owned by s % physical_worker_count.
    bool use_explicit_worker_mapping = false;
    std::array<std::uint8_t, kInstrumentHistoryLogicalShardCountV1>
        physical_worker_by_logical_shard{};
    // This is the usable capacity of each of the fixed 4 x 16 SPSC queues.
    std::size_t queue_capacity = 1024U;
    // Bounds submitted-but-not-contiguously-acknowledged tickets for each
    // source.  It also bounds exact out-of-order completion bookkeeping.
    std::size_t maximum_inflight_per_source = 16U * 1024U;
    std::size_t chunk_record_capacity = 1024U;
    // Hard output/work bound for Tail and RangeBySourceSequence.  Queries
    // above this limit fail before taking a shard lock or allocating output.
    std::size_t maximum_records_per_query = 64U * 1024U;
    std::uint64_t maximum_records_per_logical_shard = 10'000'000U;
    std::uint32_t maximum_instruments_per_logical_shard = 100'000U;
    // Required hard bound for retained decoded payload owned by one logical
    // shard.  Zero is invalid: record count alone is not a memory bound when
    // snapshots contain variable-size depth and queue vectors.
    std::uint64_t maximum_owned_payload_bytes_per_logical_shard = 0U;
    // Optional deterministic test/telemetry seam, called by the owning worker
    // immediately before append.  It must not throw or re-enter this runtime.
    // A production deployment normally leaves it null.
    using BeforeAppendHook = void (*)(
        void* context,
        std::uint32_t physical_worker,
        const OwnedInstrumentEventEnvelopeV1& envelope) noexcept;
    BeforeAppendHook before_append_hook = nullptr;
    void* before_append_hook_context = nullptr;
    // Deterministic test/telemetry seam invoked after a Tail/Range query has
    // released the shard lock and before it traverses immutable full chunks.
    // It must not throw or re-enter this runtime.  Production leaves it null.
    using AfterQuerySnapshotHook = void (*)(void* context) noexcept;
    AfterQuerySnapshotHook after_query_snapshot_hook = nullptr;
    void* after_query_snapshot_hook_context = nullptr;
};

enum class InstrumentHistorySubmitErrorV1 : std::uint8_t {
    kNone = 0U,
    kStopped,
    kSourceFatal,
    kSourceMismatch,
    kEnvelopeAlreadySubmitted,
    kSourceSequenceNotIncreasing,
    kInflightLimit,
    kQueueFull,
};

[[nodiscard]] std::string_view InstrumentHistorySubmitErrorNameV1(
    InstrumentHistorySubmitErrorV1 error) noexcept;

struct InstrumentHistorySourceFrontierV1 final {
    std::uint64_t submitted_ticket = 0U;
    std::uint64_t acknowledged_ticket = 0U;
    std::uint64_t submitted_source_sequence = 0U;
    std::uint64_t acknowledged_source_sequence = 0U;
    // Completed tickets above acknowledged_ticket that are waiting for an
    // earlier gap.  This is diagnostic only; none are query-visible.
    std::uint64_t completed_out_of_order = 0U;
    bool fatal = false;
};

struct InstrumentHistoryBarrierV1 final {
    std::uint8_t source_slot = 0U;
    std::uint64_t ticket = 0U;
    std::uint64_t source_sequence = 0U;
    bool valid = false;
};

enum class InstrumentHistoryBarrierWaitErrorV1 : std::uint8_t {
    kNone = 0U,
    kInvalidBarrier,
    kTimeout,
    kSourceFatal,
};

[[nodiscard]] std::string_view InstrumentHistoryBarrierWaitErrorNameV1(
    InstrumentHistoryBarrierWaitErrorV1 error) noexcept;

// A copied handle pins the owning append-only chunk and points at one immutable
// record.  No handle spans two source slots, so it cannot imply a cross-source
// total order.
class InstrumentHistoryRecordHandleV1 final {
public:
    InstrumentHistoryRecordHandleV1() noexcept = default;

    [[nodiscard]] const OwnedInstrumentEventEnvelopeV1* get()
        const noexcept {
        return record_;
    }
    [[nodiscard]] const OwnedInstrumentEventEnvelopeV1* operator->()
        const noexcept {
        return get();
    }
    [[nodiscard]] const OwnedInstrumentEventEnvelopeV1& operator*()
        const noexcept {
        return *get();
    }
    [[nodiscard]] explicit operator bool() const noexcept {
        return get() != nullptr;
    }

private:
    friend class InstrumentHistoryRuntimeV1;

    InstrumentHistoryRecordHandleV1(
        std::shared_ptr<const void> owner,
        const OwnedInstrumentEventEnvelopeV1* record) noexcept
        : owner_(std::move(owner)), record_(record) {}

    std::shared_ptr<const void> owner_;
    const OwnedInstrumentEventEnvelopeV1* record_ = nullptr;
};

enum class InstrumentHistoryQueryErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidArgument,
    kSourceFatal,
    kNotFound,
    kResourceExhausted,
    kQueryLimitExceeded,
};

[[nodiscard]] std::string_view InstrumentHistoryQueryErrorNameV1(
    InstrumentHistoryQueryErrorV1 error) noexcept;

struct InstrumentHistoryRangeResultV1 final {
    InstrumentHistoryQueryErrorV1 error =
        InstrumentHistoryQueryErrorV1::kNone;
    std::uint64_t matched_records = 0U;
    bool truncated = false;
};

// Owns 64 bounded SPSC queues (four source producers by sixteen logical
// shards), sixteen append-only shard stores, and M physical consumer workers.
// A source slot must have exactly one producer thread.  Every logical queue has
// exactly one physical consumer according to the immutable startup mapping.
class InstrumentHistoryRuntimeV1 final {
public:
    InstrumentHistoryRuntimeV1(const InstrumentHistoryRuntimeV1&) = delete;
    InstrumentHistoryRuntimeV1& operator=(
        const InstrumentHistoryRuntimeV1&) = delete;
    InstrumentHistoryRuntimeV1(InstrumentHistoryRuntimeV1&&) = delete;
    InstrumentHistoryRuntimeV1& operator=(
        InstrumentHistoryRuntimeV1&&) = delete;
    ~InstrumentHistoryRuntimeV1();

    [[nodiscard]] static InstrumentHistoryCreateErrorV1 Create(
        InstrumentHistoryRuntimeConfigV1 config,
        std::unique_ptr<InstrumentHistoryRuntimeV1>* output) noexcept;

    // The four distinct source slots may submit concurrently, but each slot
    // must have exactly one producer thread.  Every non-kNone result preserves
    // envelope byte-for-byte as a valid retry object; only kNone assigns its
    // dense dispatch ticket and transfers ownership to the runtime.
    [[nodiscard]] InstrumentHistorySubmitErrorV1 TrySubmit(
        OwnedInstrumentEventEnvelopeV1&& envelope) noexcept;

    [[nodiscard]] InstrumentHistorySourceFrontierV1 Frontier(
        std::uint8_t source_slot) const noexcept;

    // Captures all successful submissions preceding this call for one source.
    // WaitForBarrier succeeds only after that dense ticket and every earlier
    // ticket have been acknowledged; gaps in vendor/source sequence are legal.
    [[nodiscard]] InstrumentHistoryBarrierV1 CaptureBarrier(
        std::uint8_t source_slot) const noexcept;

    [[nodiscard]] InstrumentHistoryBarrierWaitErrorV1 WaitForBarrier(
        const InstrumentHistoryBarrierV1& barrier,
        std::chrono::nanoseconds timeout) const noexcept;

    // Idempotently closes admission for exactly one source and makes every
    // subsequent query fail through its sticky fatal frontier.  Already
    // returned shared-chunk handles remain memory-safe and readable; they are
    // not evidence that the source/route generation is still authoritative.
    // Already submitted queue entries are drained and destroyed safely, but
    // are not made query-visible.  Other source slots continue independently.
    void MarkSourceFatal(std::uint8_t source_slot) noexcept;

    // Query visibility is bounded by the source's contiguously acknowledged
    // history frontier, not by Canonical SourceFrontier.processed.  For a
    // just-committed market event, first prove that
    // acknowledged_source_sequence >= that event's origin source sequence.
    // The returned status is a point-in-time proof: factor state and retained
    // handles must remain bound to their route/source generation and be
    // discarded if that generation later becomes Fatal.
    [[nodiscard]] InstrumentHistoryQueryErrorV1 Latest(
        std::uint32_t instrument_id,
        std::uint8_t source_slot,
        InstrumentHistoryLaneV1 lane,
        InstrumentHistoryRecordHandleV1* output) const noexcept;

    // Returns the newest count records in ascending source-sequence order.
    [[nodiscard]] InstrumentHistoryQueryErrorV1 Tail(
        std::uint32_t instrument_id,
        std::uint8_t source_slot,
        InstrumentHistoryLaneV1 lane,
        std::size_t count,
        std::vector<InstrumentHistoryRecordHandleV1>* output) const noexcept;

    // Returns [begin_inclusive, end_exclusive), always within one exact source
    // slot and lane.  maximum_records is a hard output bound.
    [[nodiscard]] InstrumentHistoryRangeResultV1 RangeBySourceSequence(
        std::uint32_t instrument_id,
        std::uint8_t source_slot,
        InstrumentHistoryLaneV1 lane,
        std::uint64_t begin_inclusive,
        std::uint64_t end_exclusive,
        std::size_t maximum_records,
        std::vector<InstrumentHistoryRecordHandleV1>* output) const noexcept;

    [[nodiscard]] std::uint8_t PhysicalWorkerForLogicalShard(
        std::uint8_t logical_shard) const noexcept;

    // Idempotent.  New admission stops before every queue is drained and all
    // worker threads join.  A source-fatal frontier deliberately remains short
    // of barriers whose records could not be stored.
    void StopAndDrain() noexcept;

    [[nodiscard]] const InstrumentHistoryRuntimeConfigV1& config()
        const noexcept;

private:
    class Impl;
    explicit InstrumentHistoryRuntimeV1(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::market
