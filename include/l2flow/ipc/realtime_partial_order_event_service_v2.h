#pragma once

#include "l2flow/common/identity128.h"
#include "l2flow/ipc/partial_order_event_journal_v2.h"
#include "l2flow/market/realtime_history_v1.h"
#include "l2flow/realtime/native_sequence_observer_v1.h"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

namespace l2flow::ipc {

using RealtimePartialOrderEventFailureNotifierV2 =
    void (*)(void* context) noexcept;

struct RealtimePartialOrderEventServiceConfigV2 final {
    common::Identity128 run_id{};
    std::uint64_t session_epoch = 0U;
    std::uint32_t trade_date = 0U;
    // This is the conservative process-owned capture boundary. It is never
    // presented as a market-open boundary or feeder watermark.
    std::uint64_t coverage_start_unix_ns = 0U;
    std::uint64_t publication_generation = 1U;
    std::uint64_t correction_epoch = 1U;

    // Required FAST sink. PublishApplied invokes it before touching any Event
    // state. Once FAST succeeds, every Event failure is absorbed.
    std::shared_ptr<market::RealtimeAppliedRecordSinkV1> fast_sink;

    // Optional process-local failure edge. It runs at most once after Event
    // admission has failed closed. A frozen journal cut is only a best-effort
    // follow-up when the journal remains writable; the broker lifecycle edge is
    // therefore the immediate terminal-failure signal. The callback must remain
    // nonblocking and allocation-free because worker and ingress paths invoke
    // it.
    RealtimePartialOrderEventFailureNotifierV2 failure_notifier = nullptr;
    void* failure_notifier_context = nullptr;

    std::uint32_t channel_capacity = 256U;
    // Capacity of each preallocated observation/applied handoff lane. Keeping
    // the serialized admission lane separate prevents Store publishers from
    // contending on the callback's enqueue position.
    std::uint64_t handoff_queue_capacity = 262'144U;
    std::uint64_t maximum_pending_entries = 262'144U;
    std::uint64_t maximum_pending_entries_per_channel = 16'384U;
    std::uint64_t duplicate_retention_entries = 262'144U;
    std::uint64_t maximum_reorder_span = 1'048'576U;

    // A newly seen native channel remains unpublished until this process-local
    // discovery horizon expires. The selected minimum is explicitly sealed as
    // bounded partial coverage; it is not a native completeness proof.
    std::chrono::nanoseconds discovery_horizon =
        std::chrono::milliseconds(25);

    std::size_t maximum_shanghai_order_states = 1'000'000U;
    std::size_t maximum_shenzhen_order_states = 1'000'000U;
    std::size_t maximum_derived_events = 16'000'000U;

    std::uint64_t event_journal_capacity = 16'000'000U;
    std::uint64_t order_state_capacity = 2'097'152U;
    // Zero lets the journal derive the bound from order_state_capacity. A
    // production composition may set a tighter measured bound explicitly.
    std::uint32_t maximum_order_state_updates_per_commit = 0U;
    std::uint64_t maximum_mapping_bytes = 0U;
    std::uint64_t lazy_commit_chunk_bytes =
        64ULL * 1024ULL * 1024ULL;

    // Empty preserves inherited affinity. A nonempty mask uses the strict
    // Linux cpuset grammar and is applied/read back by the worker before start
    // succeeds.
    std::string worker_cpu_set;
};

enum class RealtimePartialOrderEventServiceCreateErrorV2
    : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidConfiguration,
    kRecoveryCreateFailed,
    kHistoryCreateFailed,
    kJournalCreateFailed,
    kResourceExhausted,
    kUnexpectedFailure,
};

[[nodiscard]] std::string_view
RealtimePartialOrderEventServiceCreateErrorNameV2(
    RealtimePartialOrderEventServiceCreateErrorV2 error) noexcept;

struct RealtimePartialOrderEventServiceSnapshotV2 final {
    PartialOrderEventServiceStateV2 state =
        PartialOrderEventServiceStateV2::kInitializing;
    PartialOrderEventLastErrorV2 last_error =
        PartialOrderEventLastErrorV2::kNone;
    std::uint64_t canonical_apply_frontier = 0U;
    std::uint64_t published_event_frontier = 0U;
    std::uint64_t captured_source_frontier = 0U;
    std::uint64_t observed_native_messages = 0U;
    std::uint64_t applied_records = 0U;
    std::uint64_t enqueued_handoffs = 0U;
    std::uint64_t processed_handoffs = 0U;
    std::uint64_t dropped_handoffs = 0U;
    std::uint64_t handoff_queue_depth = 0U;
    // Sampled by the Event worker at bounded batch boundaries. Exact current
    // backlog is handoff_queue_depth; this value is diagnostic only.
    std::uint64_t handoff_queue_high_water = 0U;
    std::uint64_t journal_canonical_commits = 0U;
    std::uint64_t journal_status_commits = 0U;
    std::uint64_t journal_published_slices = 0U;
    std::uint64_t journal_maximum_batch_slices = 0U;
    std::uint64_t reorder_high_water = 0U;
    std::uint64_t pending_entries = 0U;
    std::uint32_t channel_count = 0U;
    std::uint32_t unsealed_channel_count = 0U;
    std::uint32_t gap_channel_count = 0U;
    std::uint32_t correction_pending_channel_count = 0U;
    std::uint32_t frozen_channel_count = 0U;
    bool worker_running = false;
    bool accepting = false;
    bool globally_frozen = false;
    bool journal_failed = false;
    bool history_failed = false;
};

// Process-start partial ordered Event service. Cross-channel polling produces
// one process-local serial publication order that is scheduling-dependent; it
// is not an exchange total order and does not wait for a cross-channel
// watermark. Native ordering is asserted solely within each Shanghai
// (Channel, BizIndex) or Shenzhen shared (ChannelNo, ApplSeqNum) domain.
class RealtimePartialOrderEventServiceV2 final
    : public market::RealtimeAppliedRecordSinkV1,
      public realtime::NativeSequenceObservationSinkV1 {
public:
    RealtimePartialOrderEventServiceV2(
        const RealtimePartialOrderEventServiceV2&) = delete;
    RealtimePartialOrderEventServiceV2& operator=(
        const RealtimePartialOrderEventServiceV2&) = delete;
    RealtimePartialOrderEventServiceV2(
        RealtimePartialOrderEventServiceV2&&) = delete;
    RealtimePartialOrderEventServiceV2& operator=(
        RealtimePartialOrderEventServiceV2&&) = delete;
    ~RealtimePartialOrderEventServiceV2() override;

    [[nodiscard]] static
    RealtimePartialOrderEventServiceCreateErrorV2 Create(
        RealtimePartialOrderEventServiceConfigV2 config,
        std::shared_ptr<RealtimePartialOrderEventServiceV2>* output,
        int* system_error_number = nullptr) noexcept;

    [[nodiscard]] bool StartWorker(
        int* system_error_number = nullptr) noexcept;

    [[nodiscard]] bool PublishApplied(
        std::size_t ordinal,
        const market::RealtimeHistoryRecordV1& record) noexcept override;
    void MarkCoverageLost() noexcept override;
    void QuiesceRecordReferences() noexcept override;

    void ObserveNativeSequence(
        const realtime::NativeSequenceObservationV1& observation)
        noexcept override;
    void MarkNativeSequenceObservationFailure(
        realtime::NativeSequenceObservationFailureV1 failure,
        const sdk::MessageKey& message_key) noexcept override;

    void MarkDraining() noexcept;
    void MarkStoppedClean() noexcept;
    void Stop() noexcept;

    [[nodiscard]] RealtimePartialOrderEventServiceSnapshotV2 Snapshot()
        const noexcept;
    [[nodiscard]] bool DuplicateReadOnlyDescriptor(
        int* output_fd,
        int* system_error_number = nullptr) const noexcept;
    [[nodiscard]] PartialOrderEventJournalSessionV2 session()
        const noexcept;
    // Read before admission or after Stop/MarkStoppedClean when proving that
    // the Event worker performed no backing allocation on its hot path.
    [[nodiscard]] PartialOrderEventJournalResourceSnapshotV2
    JournalResourceSnapshot() const noexcept;
    // Test-only drain fence for work already admitted when this call observes
    // the queue. It includes an in-progress journal commit, but it does not wait
    // for a future process-local discovery-horizon deadline.
    [[nodiscard]] bool WaitUntilIdleForTest(
        std::chrono::milliseconds timeout) const noexcept;
    void SetWorkerPausedForTest(bool paused) noexcept;
    [[nodiscard]] bool WorkerPauseReachedForTest() const noexcept;
    // Deterministic test hook at the journal publication boundary.
    void SetJournalPublicationPausedForTest(bool paused) noexcept;
    [[nodiscard]] bool JournalPublicationPauseReachedForTest()
        const noexcept;

private:
    class Impl;
    explicit RealtimePartialOrderEventServiceV2(
        std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::ipc
