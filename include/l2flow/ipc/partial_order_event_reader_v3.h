#pragma once

#include "l2flow/ipc/partial_order_event_reader_v2.h"
#include "l2flow/ipc/partial_order_event_wire_v3.h"

#include <cstdint>
#include <memory>
#include <span>
#include <utility>

namespace l2flow::ipc {

using PartialOrderEventExpectedSessionV3 =
    PartialOrderEventExpectedSessionV2;
using PartialOrderEventOrderKeyV3 = PartialOrderEventOrderKeyV2;
using PartialOrderEventReaderOpenErrorV3 =
    PartialOrderEventReaderOpenErrorV2;
using PartialOrderEventReadResultV3 = PartialOrderEventReadResultV2;
using PartialOrderEventReadBatchResultV3 =
    PartialOrderEventReadBatchResultV2;
using PartialOrderEventOrderStateBatchResultV3 =
    PartialOrderEventOrderStateBatchResultV2;

class PartialOrderEventReaderV3 final {
public:
    PartialOrderEventReaderV3(const PartialOrderEventReaderV3&) = delete;
    PartialOrderEventReaderV3& operator=(
        const PartialOrderEventReaderV3&) = delete;
    ~PartialOrderEventReaderV3() = default;

    [[nodiscard]] static PartialOrderEventReaderOpenErrorV3
    OpenDescriptor(
        int descriptor,
        const PartialOrderEventExpectedSessionV3& expected_session,
        std::unique_ptr<PartialOrderEventReaderV3>* output,
        int* system_error_number = nullptr) noexcept;

    [[nodiscard]] PartialOrderEventReadResultV3 ReadStatus(
        PartialOrderEventStatusSnapshotV3* output) const noexcept;
    [[nodiscard]] PartialOrderEventReadResultV3 ReadEvent(
        std::uint64_t derived_event_sequence,
        PartialOrderEventEnvelopeV3* output) const noexcept;
    [[nodiscard]] PartialOrderEventReadResultV3 ReadEvents(
        std::uint64_t first_derived_event_sequence,
        std::span<PartialOrderEventEnvelopeV3> output,
        PartialOrderEventReadBatchResultV3* result) const noexcept;
    [[nodiscard]] PartialOrderEventReadResultV3 ReadAffectedChannels(
        std::span<PartialOrderEventChannelHealthV3> output,
        std::size_t* rows_read,
        PartialOrderEventStatusSnapshotV3* status = nullptr)
        const noexcept;
    [[nodiscard]] PartialOrderEventReadResultV3 FindOrderState(
        const PartialOrderEventOrderKeyV3& key,
        PartialOrderEventOrderStateV3* output,
        PartialOrderEventStatusSnapshotV3* status = nullptr)
        const noexcept;
    [[nodiscard]] PartialOrderEventReadResultV3 ReadOrderStates(
        std::uint64_t first_physical_slot,
        std::span<PartialOrderEventOrderStateV3> output,
        PartialOrderEventOrderStateBatchResultV3* result) const noexcept;

private:
    explicit PartialOrderEventReaderV3(
        std::unique_ptr<PartialOrderEventReaderV2> core) noexcept
        : core_(std::move(core)) {}
    std::unique_ptr<PartialOrderEventReaderV2> core_;
};

}  // namespace l2flow::ipc
