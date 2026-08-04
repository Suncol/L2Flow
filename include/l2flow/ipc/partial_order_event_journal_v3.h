#pragma once

#include "l2flow/ipc/partial_order_event_journal_v2.h"
#include "l2flow/ipc/partial_order_event_wire_v3.h"

#include <cstdint>
#include <memory>
#include <span>
#include <utility>

namespace l2flow::ipc {

using PartialOrderEventJournalConfigV3 =
    PartialOrderEventJournalConfigV2;
using PartialOrderEventJournalSessionV3 =
    PartialOrderEventJournalSessionV2;
using PartialOrderEventStatusUpdateV3 =
    PartialOrderEventStatusUpdateV2;
using PartialOrderEventCanonicalSliceV3 =
    PartialOrderEventCanonicalSliceV2;
using PartialOrderEventJournalResourceSnapshotV3 =
    PartialOrderEventJournalResourceSnapshotV2;
using PartialOrderEventJournalCreateErrorV3 =
    PartialOrderEventJournalCreateErrorV2;
using PartialOrderEventJournalPublishErrorV3 =
    PartialOrderEventJournalPublishErrorV2;
using PartialOrderEventCommitFailpointV3 =
    PartialOrderEventCommitFailpointV2;

// V3 shares the proven V2 cut/Event publication engine but emits the distinct
// V3 major and compact order-state reference slots. V2 Create never selects
// this mode, so its descriptor ABI remains unchanged.
class PartialOrderEventJournalProducerV3 final {
public:
    PartialOrderEventJournalProducerV3(
        const PartialOrderEventJournalProducerV3&) = delete;
    PartialOrderEventJournalProducerV3& operator=(
        const PartialOrderEventJournalProducerV3&) = delete;
    ~PartialOrderEventJournalProducerV3() = default;

    [[nodiscard]] static PartialOrderEventJournalCreateErrorV3 Create(
        PartialOrderEventJournalConfigV3 config,
        std::shared_ptr<PartialOrderEventJournalProducerV3>* output,
        int* system_error_number = nullptr) noexcept;

    [[nodiscard]] PartialOrderEventJournalPublishErrorV3
    PreallocateBacking(int* system_error_number = nullptr) noexcept;
    [[nodiscard]] PartialOrderEventJournalPublishErrorV3
    EnsureEventWritable(std::uint64_t required_event_count) noexcept;
    [[nodiscard]] PartialOrderEventJournalPublishErrorV3
    PublishCanonicalTick(
        std::uint64_t canonical_apply_sequence,
        const PartialOrderEventStatusUpdateV3& status,
        std::span<const InstrumentDerivedEventV1> events,
        std::span<const PartialOrderEventChannelHealthV3>
            affected_channels = {}) noexcept;
    [[nodiscard]] PartialOrderEventJournalPublishErrorV3
    PublishCanonicalBatch(
        std::span<const PartialOrderEventCanonicalSliceV3> slices,
        const PartialOrderEventStatusUpdateV3& status,
        std::span<const InstrumentDerivedEventV1> events,
        std::span<const PartialOrderEventChannelHealthV3>
            affected_channels = {}) noexcept;
    [[nodiscard]] PartialOrderEventJournalPublishErrorV3
    PublishCanonicalBatchProjected(
        std::span<const PartialOrderEventCanonicalSliceV3> slices,
        const PartialOrderEventStatusUpdateV3& status,
        std::span<const l2flow_instrument_derived_event_row_v1> events,
        std::span<const PartialOrderEventChannelHealthV3>
            affected_channels = {}) noexcept;
    [[nodiscard]] PartialOrderEventJournalPublishErrorV3 PublishStatus(
        const PartialOrderEventStatusUpdateV3& status,
        std::span<const PartialOrderEventChannelHealthV3>
            affected_channels = {}) noexcept;

    [[nodiscard]] bool DuplicateReadOnlyDescriptor(
        int* output,
        int* system_error_number = nullptr) const noexcept;
    [[nodiscard]] PartialOrderEventJournalSessionV3 session()
        const noexcept;
    [[nodiscard]] std::uint64_t commit_sequence() const noexcept;
    [[nodiscard]] std::uint64_t canonical_apply_frontier()
        const noexcept;
    [[nodiscard]] std::uint64_t published_event_frontier()
        const noexcept;
    [[nodiscard]] PartialOrderEventJournalResourceSnapshotV3
    ResourceSnapshot() const noexcept;
    [[nodiscard]] bool failed() const noexcept;
    void SetCommitFailpointForTest(
        PartialOrderEventCommitFailpointV3 failpoint) noexcept;

private:
    explicit PartialOrderEventJournalProducerV3(
        std::shared_ptr<PartialOrderEventJournalProducerV2> core) noexcept
        : core_(std::move(core)) {}
    std::shared_ptr<PartialOrderEventJournalProducerV2> core_;
};

}  // namespace l2flow::ipc
