#pragma once

#include "l2flow/common/identity128.h"
#include "l2flow/ipc/instrument_derived_event_history_v1.h"
#include "l2flow/ipc/partial_order_event_wire_v2.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string_view>

namespace l2flow::ipc {

class PartialOrderEventJournalProducerV3;

struct PartialOrderEventJournalConfigV2 final {
    common::Identity128 run_id{};
    std::uint64_t session_epoch = 0U;
    std::uint32_t trade_date = 0U;
    std::uint64_t publication_generation = 0U;
    std::uint64_t correction_epoch = 0U;
    std::uint64_t coverage_start_unix_ns = 0U;
    PartialOrderEventOrderingQualityV2 ordering_quality =
        PartialOrderEventOrderingQualityV2::
            kBoundedReorderedPartial;
    std::uint64_t event_capacity = 0U;
    // The channel bank carries affected-channel diagnostics only. It is not
    // the native recovery coordinator's authoritative state store.
    std::uint32_t affected_channel_capacity = 0U;
    // Power-of-two fixed-capacity hash table keyed by
    // (market, instrument_id, channel, order_id).
    std::uint64_t order_state_capacity = 0U;
    // Bounds producer-owned scratch reserved once during Create. Zero derives
    // the bound from order_state_capacity, which is required for an
    // untruncated Shanghai END batch (one final revision per retained order).
    // A smaller nonzero value is legal only when the caller has a stronger
    // upstream batch bound; exceeding it fails before publication mutation.
    std::uint64_t maximum_order_state_updates_per_commit = 0U;
    // Bounds the projection scratch reserved once during Create. Zero preserves
    // the legacy full-event-capacity bound. Microbatch producers should set a
    // tighter proven maximum; a larger commit is rejected before publication
    // mutation or sparse-backing growth.
    std::uint64_t maximum_events_per_commit = 0U;
    std::uint64_t maximum_mapping_bytes = 0U;
    std::uint64_t lazy_commit_chunk_bytes =
        64ULL * 1024ULL * 1024ULL;
    // Optionally faults every order-state page into the writer mapping during
    // PreallocateBacking. This is a strict opt-in: when
    // MADV_POPULATE_WRITE is unavailable or rejected, PreallocateBacking
    // returns kBackingCommitFailed with ENOTSUP or the madvise errno. It does
    // not silently weaken the request to asynchronous MADV_WILLNEED.
    bool prefault_order_state_pages = false;
    // Apply the same strict policy to the complete append-only Event region.
    // This prevents first-write page faults from running on the Event worker.
    bool prefault_event_pages = false;
};

struct PartialOrderEventJournalSessionV2 final {
    common::Identity128 run_id{};
    std::uint64_t session_epoch = 0U;
    std::uint32_t trade_date = 0U;
    std::uint64_t publication_generation = 0U;
    std::uint64_t correction_epoch = 0U;
    std::uint64_t coverage_start_unix_ns = 0U;
    PartialOrderEventOrderingQualityV2 ordering_quality =
        PartialOrderEventOrderingQualityV2::
            kBoundedReorderedPartial;
    std::uint64_t event_capacity = 0U;
    std::uint32_t affected_channel_capacity = 0U;
    std::uint64_t order_state_capacity = 0U;
    std::uint64_t total_mapping_bytes = 0U;

    [[nodiscard]] friend bool operator==(
        const PartialOrderEventJournalSessionV2&,
        const PartialOrderEventJournalSessionV2&) noexcept = default;
};

struct PartialOrderEventStatusUpdateV2 final {
    // Local source/capture sequence only. It must be monotonic, but is not a
    // claim that all feeder lanes crossed a native watermark.
    std::uint64_t captured_source_frontier = 0U;
    PartialOrderEventServiceStateV2 state =
        PartialOrderEventServiceStateV2::kInitializing;
    bool stale = true;
    std::uint64_t reorder_high_water = 0U;
    PartialOrderEventLastErrorV2 last_error =
        PartialOrderEventLastErrorV2::kNone;
};

// One canonical input inside a microbatch. event_count partitions the flattened
// Event span supplied to PublishCanonicalBatch in the same order as these
// slices. Zero-Event inputs are retained so the canonical frontier can advance
// without inventing an Event row.
struct PartialOrderEventCanonicalSliceV2 final {
    std::uint64_t canonical_apply_sequence = 0U;
    std::size_t event_count = 0U;
};

// Writer-thread or quiescent resource diagnostics. Compare a snapshot taken
// after PreallocateBacking with one taken after the writer stops to prove that
// no Event or order-state backing allocation occurred in between. Allocation
// calls count successful post-Create fallocate operations; the fixed header and
// channel-bank allocations performed by Create are excluded.
struct PartialOrderEventJournalResourceSnapshotV2 final {
    std::uint64_t committed_event_region_bytes = 0U;
    std::uint64_t order_state_backed_bytes = 0U;
    std::uint64_t order_state_backed_chunk_count = 0U;
    std::uint64_t order_state_backing_allocation_calls = 0U;
    std::uint64_t event_backing_allocation_calls = 0U;
    std::uint64_t event_prefault_attempts = 0U;
    std::uint64_t event_prefaulted_bytes = 0U;
    // Attempts includes a failed unsupported-kernel request. Prefaulted bytes
    // advances only after MADV_POPULATE_WRITE succeeds for the complete region.
    std::uint64_t order_state_prefault_attempts = 0U;
    std::uint64_t order_state_prefaulted_bytes = 0U;
    // True only after the complete backing and any requested strict prefault
    // have succeeded.
    bool fully_preallocated = false;

    [[nodiscard]] friend bool operator==(
        const PartialOrderEventJournalResourceSnapshotV2&,
        const PartialOrderEventJournalResourceSnapshotV2&) noexcept = default;
};

enum class PartialOrderEventJournalCreateErrorV2 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidConfiguration,
    kLayoutOverflow,
    kMappingCreateFailed,
    kReadOnlyHandleFailed,
    kSealFailed,
    kResourceExhausted,
    kUnexpectedFailure,
};

enum class PartialOrderEventJournalPublishErrorV2 : std::uint8_t {
    kNone = 0U,
    kInvalidArgument,
    kCanonicalSequence,
    kEventCapacity,
    kChannelCapacity,
    kOrderStateCapacity,
    kBackingCommitFailed,
    kProjectionError,
    kPublicationInvariant,
    kInjectedFailure,
    kFailed,
};

// Test-only interruption points used to prove that every pre-cut state leaves
// the previous stable cut readable. They do not model persistence across host
// power loss; the mapping is an in-memory broker-held generation.
enum class PartialOrderEventCommitFailpointV2 : std::uint8_t {
    kNone = 0U,
    kAfterTargetCutInvalidated,
    kAfterChannelBankWritten,
    kAfterOrderStateKeyInvalidated,
    kAfterOrderStateVersionInvalidated,
    kAfterEventRowsWritten,
    kBeforeCutPublished,
};

[[nodiscard]] std::string_view
PartialOrderEventJournalCreateErrorNameV2(
    PartialOrderEventJournalCreateErrorV2 error) noexcept;
[[nodiscard]] std::string_view
PartialOrderEventJournalPublishErrorNameV2(
    PartialOrderEventJournalPublishErrorV2 error) noexcept;

// Serial writer. It projects complete per-canonical-input Event batches into
// one append-only journal and materializes the latest order-revision row in a
// fixed hash table. Status-only commits make gap/restart progress visible
// without advancing the canonical or Event frontiers.
class PartialOrderEventJournalProducerV2 final {
public:
    PartialOrderEventJournalProducerV2(
        const PartialOrderEventJournalProducerV2&) = delete;
    PartialOrderEventJournalProducerV2& operator=(
        const PartialOrderEventJournalProducerV2&) = delete;
    PartialOrderEventJournalProducerV2(
        PartialOrderEventJournalProducerV2&&) = delete;
    PartialOrderEventJournalProducerV2& operator=(
        PartialOrderEventJournalProducerV2&&) = delete;
    ~PartialOrderEventJournalProducerV2();

    [[nodiscard]] static PartialOrderEventJournalCreateErrorV2 Create(
        PartialOrderEventJournalConfigV2 config,
        std::shared_ptr<PartialOrderEventJournalProducerV2>* output,
        int* system_error_number = nullptr) noexcept;

    // Commits the complete fixed mapping before a latency-sensitive writer is
    // started. The default journal remains sparse until this is called.
    [[nodiscard]] PartialOrderEventJournalPublishErrorV2 PreallocateBacking(
        int* system_error_number = nullptr) noexcept;

    [[nodiscard]] PartialOrderEventJournalPublishErrorV2
    EnsureEventWritable(std::uint64_t required_event_count) noexcept;

    [[nodiscard]] PartialOrderEventJournalPublishErrorV2
    PublishCanonicalTick(
        std::uint64_t canonical_apply_sequence,
        const PartialOrderEventStatusUpdateV2& status,
        std::span<const InstrumentDerivedEventV1> events,
        std::span<const PartialOrderEventChannelHealthV2>
            affected_channels = {}) noexcept;

    // Atomically publishes one contiguous canonical prefix. Readers observe
    // either the preceding complete cut or the final cut for the whole batch;
    // every Event and materialized order-state version retains the canonical
    // sequence of its owning slice.
    [[nodiscard]] PartialOrderEventJournalPublishErrorV2
    PublishCanonicalBatch(
        std::span<const PartialOrderEventCanonicalSliceV2> slices,
        const PartialOrderEventStatusUpdateV2& status,
        std::span<const InstrumentDerivedEventV1> events,
        std::span<const PartialOrderEventChannelHealthV2>
            affected_channels = {}) noexcept;

    // Equivalent serial path for rows already flattened by an owner-private
    // history. Rows receive the same complete canonical validation before any
    // cut is changed; this overload only removes redundant variant projection.
    [[nodiscard]] PartialOrderEventJournalPublishErrorV2
    PublishCanonicalBatchProjected(
        std::span<const PartialOrderEventCanonicalSliceV2> slices,
        const PartialOrderEventStatusUpdateV2& status,
        std::span<const l2flow_instrument_derived_event_row_v1> events,
        std::span<const PartialOrderEventChannelHealthV2>
            affected_channels = {}) noexcept;

    [[nodiscard]] PartialOrderEventJournalPublishErrorV2 PublishStatus(
        const PartialOrderEventStatusUpdateV2& status,
        std::span<const PartialOrderEventChannelHealthV2>
            affected_channels = {}) noexcept;

    [[nodiscard]] bool DuplicateReadOnlyDescriptor(
        int* output,
        int* system_error_number = nullptr) const noexcept;

    [[nodiscard]] PartialOrderEventJournalSessionV2 session()
        const noexcept;
    [[nodiscard]] std::uint64_t commit_sequence() const noexcept;
    [[nodiscard]] std::uint64_t canonical_apply_frontier()
        const noexcept;
    [[nodiscard]] std::uint64_t published_event_frontier()
        const noexcept;
    [[nodiscard]] PartialOrderEventJournalResourceSnapshotV2
    ResourceSnapshot() const noexcept;
    [[nodiscard]] bool failed() const noexcept;

    void SetCommitFailpointForTest(
        PartialOrderEventCommitFailpointV2 failpoint) noexcept;

private:
    friend class PartialOrderEventJournalProducerV3;
    [[nodiscard]] static PartialOrderEventJournalCreateErrorV2
    CreateInternal(
        PartialOrderEventJournalConfigV2 config,
        bool compact_state_references,
        std::shared_ptr<PartialOrderEventJournalProducerV2>* output,
        int* system_error_number) noexcept;
    class Impl;
    explicit PartialOrderEventJournalProducerV2(
        std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::ipc
