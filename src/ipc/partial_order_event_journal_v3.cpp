#include "l2flow/ipc/partial_order_event_journal_v3.h"

#include <new>
#include <utility>

namespace l2flow::ipc {

PartialOrderEventJournalCreateErrorV3
PartialOrderEventJournalProducerV3::Create(
    PartialOrderEventJournalConfigV3 config,
    std::shared_ptr<PartialOrderEventJournalProducerV3>* output,
    int* system_error_number) noexcept {
    if (output == nullptr) {
        return PartialOrderEventJournalCreateErrorV3::kNullOutput;
    }
    output->reset();
    std::shared_ptr<PartialOrderEventJournalProducerV2> core;
    const auto error =
        PartialOrderEventJournalProducerV2::CreateInternal(
            std::move(config), true, &core, system_error_number);
    if (error != PartialOrderEventJournalCreateErrorV3::kNone ||
        core == nullptr) {
        return error;
    }
    try {
        *output = std::shared_ptr<PartialOrderEventJournalProducerV3>(
            new PartialOrderEventJournalProducerV3(std::move(core)));
        return PartialOrderEventJournalCreateErrorV3::kNone;
    } catch (const std::bad_alloc&) {
        return PartialOrderEventJournalCreateErrorV3::kResourceExhausted;
    } catch (...) {
        return PartialOrderEventJournalCreateErrorV3::kUnexpectedFailure;
    }
}

PartialOrderEventJournalPublishErrorV3
PartialOrderEventJournalProducerV3::PreallocateBacking(
    int* system_error_number) noexcept {
    return core_->PreallocateBacking(system_error_number);
}

PartialOrderEventJournalPublishErrorV3
PartialOrderEventJournalProducerV3::EnsureEventWritable(
    std::uint64_t required_event_count) noexcept {
    return core_->EnsureEventWritable(required_event_count);
}

PartialOrderEventJournalPublishErrorV3
PartialOrderEventJournalProducerV3::PublishCanonicalTick(
    std::uint64_t canonical_apply_sequence,
    const PartialOrderEventStatusUpdateV3& status,
    std::span<const InstrumentDerivedEventV1> events,
    std::span<const PartialOrderEventChannelHealthV3>
        affected_channels) noexcept {
    return core_->PublishCanonicalTick(
        canonical_apply_sequence, status, events, affected_channels);
}

PartialOrderEventJournalPublishErrorV3
PartialOrderEventJournalProducerV3::PublishCanonicalBatch(
    std::span<const PartialOrderEventCanonicalSliceV3> slices,
    const PartialOrderEventStatusUpdateV3& status,
    std::span<const InstrumentDerivedEventV1> events,
    std::span<const PartialOrderEventChannelHealthV3>
        affected_channels) noexcept {
    return core_->PublishCanonicalBatch(
        slices, status, events, affected_channels);
}

PartialOrderEventJournalPublishErrorV3
PartialOrderEventJournalProducerV3::PublishCanonicalBatchProjected(
    std::span<const PartialOrderEventCanonicalSliceV3> slices,
    const PartialOrderEventStatusUpdateV3& status,
    std::span<const l2flow_instrument_derived_event_row_v1> events,
    std::span<const PartialOrderEventChannelHealthV3>
        affected_channels) noexcept {
    return core_->PublishCanonicalBatchProjected(
        slices, status, events, affected_channels);
}

PartialOrderEventJournalPublishErrorV3
PartialOrderEventJournalProducerV3::PublishStatus(
    const PartialOrderEventStatusUpdateV3& status,
    std::span<const PartialOrderEventChannelHealthV3>
        affected_channels) noexcept {
    return core_->PublishStatus(status, affected_channels);
}

bool PartialOrderEventJournalProducerV3::DuplicateReadOnlyDescriptor(
    int* output,
    int* system_error_number) const noexcept {
    return core_->DuplicateReadOnlyDescriptor(
        output, system_error_number);
}

PartialOrderEventJournalSessionV3
PartialOrderEventJournalProducerV3::session() const noexcept {
    return core_->session();
}

std::uint64_t PartialOrderEventJournalProducerV3::commit_sequence()
    const noexcept {
    return core_->commit_sequence();
}

std::uint64_t
PartialOrderEventJournalProducerV3::canonical_apply_frontier()
    const noexcept {
    return core_->canonical_apply_frontier();
}

std::uint64_t
PartialOrderEventJournalProducerV3::published_event_frontier()
    const noexcept {
    return core_->published_event_frontier();
}

PartialOrderEventJournalResourceSnapshotV3
PartialOrderEventJournalProducerV3::ResourceSnapshot() const noexcept {
    return core_->ResourceSnapshot();
}

bool PartialOrderEventJournalProducerV3::failed() const noexcept {
    return core_->failed();
}

void PartialOrderEventJournalProducerV3::SetCommitFailpointForTest(
    PartialOrderEventCommitFailpointV3 failpoint) noexcept {
    core_->SetCommitFailpointForTest(failpoint);
}

}  // namespace l2flow::ipc
