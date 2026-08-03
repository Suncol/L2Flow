#pragma once

#include "l2flow/common/identity128.h"
#include "l2flow/ipc/instrument_derived_event_history_v1.h"
#include "l2flow/ipc/partial_order_event_wire_v2.h"

#include <cstdint>
#include <memory>
#include <span>
#include <string_view>

namespace l2flow::ipc {

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
    std::uint64_t maximum_mapping_bytes = 0U;
    std::uint64_t lazy_commit_chunk_bytes =
        64ULL * 1024ULL * 1024ULL;
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

// Writer-thread resource diagnostics. Order-state backing is committed in
// fixed lazy chunks before any cut, key, version, or Event row is mutated.
struct PartialOrderEventJournalResourceSnapshotV2 final {
    std::uint64_t committed_event_region_bytes = 0U;
    std::uint64_t order_state_backed_bytes = 0U;
    std::uint64_t order_state_backed_chunk_count = 0U;
    std::uint64_t order_state_backing_allocation_calls = 0U;
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

    [[nodiscard]] PartialOrderEventJournalPublishErrorV2
    EnsureEventWritable(std::uint64_t required_event_count) noexcept;

    [[nodiscard]] PartialOrderEventJournalPublishErrorV2
    PublishCanonicalTick(
        std::uint64_t canonical_apply_sequence,
        const PartialOrderEventStatusUpdateV2& status,
        std::span<const InstrumentDerivedEventV1> events,
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
    class Impl;
    explicit PartialOrderEventJournalProducerV2(
        std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::ipc
