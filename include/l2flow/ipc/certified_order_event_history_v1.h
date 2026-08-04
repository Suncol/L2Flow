#pragma once

#include "l2flow/ipc/instrument_derived_event_history_c_v1.h"
#include "l2flow/ipc/instrument_derived_event_history_v1.h"
#include "l2flow/ipc/order_event_wire_adapter_v2.h"
#include "l2flow/ipc/realtime_wire_v2.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string_view>

namespace l2flow::ipc {

class CertifiedOrderEventJournalProducerV1;
enum class CertifiedOrderEventJournalPublishErrorV1
    : std::uint8_t;

// Bounds are established before the first input. The two order-state limits
// are independent because Shanghai and Shenzhen have independent native
// channel domains and reconstruction cores. maximum_events bounds the complete
// lossless journal, including every source event and every order revision.
struct CertifiedOrderEventHistoryConfigV1 final {
    std::uint32_t trade_date = 0U;
    std::size_t maximum_shanghai_order_states = 0U;
    std::size_t maximum_shenzhen_order_states = 0U;
    std::size_t maximum_events = 0U;
    // Owner-private mode retains only the rows awaiting publication by its
    // serial caller. Zero preserves the legacy maximum_events-sized buffer;
    // a microbatch publisher should provide its proven per-commit bound.
    std::size_t maximum_private_batch_events = 0U;
    // Commits and strictly write-prefaults the fixed event mapping during
    // Create so the serial writer neither expands nor first-touches backing
    // pages on its hot path. Create fails if strict population is unsupported.
    bool preallocate_event_storage = false;
    // Disable only for an owner-private history whose serial writer consumes
    // append results directly and never exposes process-local snapshots.
    bool publish_process_snapshots = true;
    // Optional external append-only wire journal. Production supplies it by
    // default; focused process-local tests may omit it. When present, backing
    // space is reserved before either stateful projection core is mutated.
    std::shared_ptr<CertifiedOrderEventJournalProducerV1>
        external_journal;
};

// This is the last input accepted in the externally supplied, process-owned
// canonical apply order. It is not an assertion of a total exchange order.
// Completeness, including native positions removed by an upstream filter, is
// established by the coordinator before this module. This retained target
// stream checks only strict native increase per (market, channel); a numeric
// gap is not evidence of loss here.
struct CertifiedOrderEventInputFrontierV1 final {
    std::uint64_t canonical_apply_sequence = 0U;
    market::MarketV1 market = market::MarketV1::kUnknown;
    std::int64_t channel = 0;
    std::int64_t native_event_sequence = 0;
    std::uint64_t source_sequence = 0U;
    std::uint64_t ingress_sequence = 0U;
    std::uint64_t tick_stream_sequence = 0U;
};

// generation advances once for every successfully accepted input, including
// inputs which would produce no rows in a future compatible extension.
// derived_event_sequence is dense from one within this projector. Therefore
// event_count == derived_event_sequence_exclusive - 1 for every generation.
struct CertifiedOrderEventHistoryGenerationV1 final {
    std::uint64_t generation = 0U;
    CertifiedOrderEventInputFrontierV1 input_frontier{};
    std::uint64_t derived_event_sequence_exclusive = 1U;
    std::size_t event_count = 0U;
    std::size_t shanghai_order_state_count = 0U;
    std::size_t shenzhen_order_state_count = 0U;
};

// Serial-writer result for callers which immediately publish the newly
// appended prefix. In snapshot mode the span points into fixed append-only
// storage. In owner-private mode appended_wire_events remains valid until
// ReleasePrivateWireBatch is called after the external publication succeeds.
struct CertifiedOrderEventHistoryAppendResultV1 final {
    CertifiedOrderEventHistoryGenerationV1 generation{};
    std::span<const InstrumentDerivedEventV1> appended_events{};
    // Owner-private mode (publish_process_snapshots=false) emits the same
    // lossless rows directly in the frozen 320-byte wire schema, avoiding a
    // second large cross-market variant materialization. Exactly one of the
    // two appended spans is nonempty for a source tick with output.
    std::span<const l2flow_instrument_derived_event_row_v1>
        appended_wire_events{};
};

enum class CertifiedOrderEventHistoryErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidConfiguration,
    kCanonicalSequence,
    kNativeSequenceRegression,
    kWireProjectionError,
    kAggregationError,
    kEventCapacity,
    kExternalJournalError,
    kResourceExhausted,
    kFailed,
};

[[nodiscard]] std::string_view
CertifiedOrderEventHistoryErrorNameV1(
    CertifiedOrderEventHistoryErrorV1 error) noexcept;

struct CertifiedOrderEventHistoryJournalStorageV1;

// Copying a snapshot is cheap. Its generation metadata and visible prefix
// remain immutable even while the owning projector publishes later
// generations. The returned span is valid while this snapshot (or a copy)
// remains alive.
class CertifiedOrderEventHistorySnapshotV1 final {
public:
    CertifiedOrderEventHistorySnapshotV1() noexcept = default;
    CertifiedOrderEventHistorySnapshotV1(
        const CertifiedOrderEventHistorySnapshotV1&) noexcept = default;
    CertifiedOrderEventHistorySnapshotV1& operator=(
        const CertifiedOrderEventHistorySnapshotV1&) noexcept = default;
    CertifiedOrderEventHistorySnapshotV1(
        CertifiedOrderEventHistorySnapshotV1&&) noexcept = default;
    CertifiedOrderEventHistorySnapshotV1& operator=(
        CertifiedOrderEventHistorySnapshotV1&&) noexcept = default;
    ~CertifiedOrderEventHistorySnapshotV1() = default;

    [[nodiscard]] bool valid() const noexcept;
    [[nodiscard]] CertifiedOrderEventHistoryGenerationV1
    generation() const noexcept;
    [[nodiscard]] std::span<const InstrumentDerivedEventV1>
    events() const noexcept;

private:
    friend class CertifiedOrderEventHistoryV1;
    std::shared_ptr<
        const CertifiedOrderEventHistoryJournalStorageV1>
        storage_;
    CertifiedOrderEventHistoryGenerationV1 generation_{};
};

// AppendCertifiedTick is serial-only. AcquireGeneration is safe concurrently
// with AppendCertifiedTick and with other AcquireGeneration calls. Create
// reserves one fixed, contiguous Linux virtual-memory region for the bounded
// journal without constructing or faulting in every slot. Before mutating a
// projection core, a writer commits a bounded writable chunk sufficient for
// that input's worst-case output, then constructs only previously unpublished
// elements and publishes a new immutable visible-prefix boundary. Snapshot
// publication has no per-generation heap allocation. Appends inside an
// already committed chunk neither map nor allocate journal storage; only a
// chunk-boundary append performs the bounded mmap replacement.
//
// Chunk commitment makes an mmap resource rejection observable before core
// mutation on kernels which enforce commit accounting. It cannot turn a
// process- or cgroup-level OOM kill into a C++ error; absolute FAST isolation
// from such host failure still requires a separate process boundary.
//
// The caller must submit records in strictly increasing
// canonical_apply_sequence order after external gap coordination. Original
// source/ingress/tick anchors are never replaced by that canonical sequence.
// Native order is checked only as strict increase within each
// (market, channel). Numeric gaps are valid after the upstream coordinator has
// consumed certified filtered positions. This module neither recreates that
// completeness proof nor infers a cross-channel exchange total order.
//
// Every non-kNone AppendCertifiedTick result permanently fail-closes further
// writes. An already published immutable generation remains available through
// AcquireGeneration.
class CertifiedOrderEventHistoryV1 final {
public:
    CertifiedOrderEventHistoryV1(
        const CertifiedOrderEventHistoryV1&) = delete;
    CertifiedOrderEventHistoryV1& operator=(
        const CertifiedOrderEventHistoryV1&) = delete;
    CertifiedOrderEventHistoryV1(
        CertifiedOrderEventHistoryV1&&) = delete;
    CertifiedOrderEventHistoryV1& operator=(
        CertifiedOrderEventHistoryV1&&) = delete;
    ~CertifiedOrderEventHistoryV1();

    [[nodiscard]] static CertifiedOrderEventHistoryErrorV1 Create(
        CertifiedOrderEventHistoryConfigV1 config,
        std::unique_ptr<CertifiedOrderEventHistoryV1>* output)
        noexcept;

    [[nodiscard]] CertifiedOrderEventHistoryErrorV1
    AppendCertifiedTick(
        const RealtimeWireTickPayloadV2& input,
        std::uint64_t canonical_apply_sequence) noexcept;

    [[nodiscard]] CertifiedOrderEventHistoryErrorV1
    AppendCertifiedTick(
        const RealtimeWireTickPayloadV2& input,
        std::uint64_t canonical_apply_sequence,
        CertifiedOrderEventHistoryAppendResultV1* output) noexcept;

    // Serial fast path for a caller which already validated and projected the
    // Shenzhen wire payload before advancing its recovery coordinator.
    [[nodiscard]] CertifiedOrderEventHistoryErrorV1
    AppendCertifiedTick(
        const market::ShenzhenOrderEventInputV1& input,
        std::uint64_t canonical_apply_sequence,
        CertifiedOrderEventHistoryAppendResultV1* output) noexcept;

    // Serial-only publication acknowledgement for owner-private mode. It
    // recycles the fixed wire batch without changing global generation or
    // derived-event sequence state. Returns false in snapshot mode.
    [[nodiscard]] bool ReleasePrivateWireBatch() noexcept;

    [[nodiscard]] CertifiedOrderEventHistoryErrorV1
    AcquireGeneration(
        CertifiedOrderEventHistorySnapshotV1* output) const noexcept;

    [[nodiscard]] bool failed() const noexcept;
    [[nodiscard]] CertifiedOrderEventHistoryErrorV1
    last_error() const noexcept;
    [[nodiscard]] WireOrderEventProjectionResultV2
    last_wire_projection_result() const noexcept;
    [[nodiscard]] market::ShanghaiOrderAggregatorConsumeErrorV1
    last_shanghai_error() const noexcept;
    [[nodiscard]] market::ShenzhenOrderProjectorConsumeErrorV1
    last_shenzhen_error() const noexcept;
    [[nodiscard]] CertifiedOrderEventJournalPublishErrorV1
    last_external_journal_error() const noexcept;
    [[nodiscard]] const CertifiedOrderEventHistoryConfigV1&
    config() const noexcept;

private:
    class Impl;
    explicit CertifiedOrderEventHistoryV1(
        std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::ipc
