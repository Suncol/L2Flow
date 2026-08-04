#include "l2flow/ipc/partial_order_event_reader_v3.h"

#include <new>
#include <utility>

namespace l2flow::ipc {

PartialOrderEventReaderOpenErrorV3
PartialOrderEventReaderV3::OpenDescriptor(
    int descriptor,
    const PartialOrderEventExpectedSessionV3& expected_session,
    std::unique_ptr<PartialOrderEventReaderV3>* output,
    int* system_error_number) noexcept {
    if (output == nullptr) {
        return PartialOrderEventReaderOpenErrorV3::kNullOutput;
    }
    output->reset();
    std::unique_ptr<PartialOrderEventReaderV2> core;
    const auto error = PartialOrderEventReaderV2::OpenDescriptorInternal(
        descriptor,
        expected_session,
        true,
        &core,
        system_error_number);
    if (error != PartialOrderEventReaderOpenErrorV3::kNone ||
        core == nullptr) {
        return error;
    }
    try {
        output->reset(new PartialOrderEventReaderV3(std::move(core)));
        return PartialOrderEventReaderOpenErrorV3::kNone;
    } catch (const std::bad_alloc&) {
        return PartialOrderEventReaderOpenErrorV3::kUnexpectedFailure;
    } catch (...) {
        return PartialOrderEventReaderOpenErrorV3::kUnexpectedFailure;
    }
}

PartialOrderEventReadResultV3 PartialOrderEventReaderV3::ReadStatus(
    PartialOrderEventStatusSnapshotV3* output) const noexcept {
    return core_->ReadStatus(output);
}

PartialOrderEventReadResultV3 PartialOrderEventReaderV3::ReadEvent(
    std::uint64_t derived_event_sequence,
    PartialOrderEventEnvelopeV3* output) const noexcept {
    return core_->ReadEvent(derived_event_sequence, output);
}

PartialOrderEventReadResultV3 PartialOrderEventReaderV3::ReadEvents(
    std::uint64_t first_derived_event_sequence,
    std::span<PartialOrderEventEnvelopeV3> output,
    PartialOrderEventReadBatchResultV3* result) const noexcept {
    return core_->ReadEvents(
        first_derived_event_sequence, output, result);
}

PartialOrderEventReadResultV3
PartialOrderEventReaderV3::ReadAffectedChannels(
    std::span<PartialOrderEventChannelHealthV3> output,
    std::size_t* rows_read,
    PartialOrderEventStatusSnapshotV3* status) const noexcept {
    return core_->ReadAffectedChannels(output, rows_read, status);
}

PartialOrderEventReadResultV3
PartialOrderEventReaderV3::FindOrderState(
    const PartialOrderEventOrderKeyV3& key,
    PartialOrderEventOrderStateV3* output,
    PartialOrderEventStatusSnapshotV3* status) const noexcept {
    return core_->FindOrderState(key, output, status);
}

PartialOrderEventReadResultV3
PartialOrderEventReaderV3::ReadOrderStates(
    std::uint64_t first_physical_slot,
    std::span<PartialOrderEventOrderStateV3> output,
    PartialOrderEventOrderStateBatchResultV3* result) const noexcept {
    return core_->ReadOrderStates(first_physical_slot, output, result);
}

}  // namespace l2flow::ipc
