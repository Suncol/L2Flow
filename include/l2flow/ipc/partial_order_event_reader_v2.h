#pragma once

#include "l2flow/common/identity128.h"
#include "l2flow/ipc/partial_order_event_wire_v2.h"

#include <cstdint>
#include <memory>
#include <span>
#include <string_view>

namespace l2flow::ipc {

struct PartialOrderEventExpectedSessionV2 final {
    common::Identity128 run_id{};
    std::uint64_t session_epoch = 0U;
    std::uint32_t trade_date = 0U;
    std::uint64_t publication_generation = 0U;
    std::uint64_t correction_epoch = 0U;
};

struct PartialOrderEventOrderKeyV2 final {
    std::uint32_t market = 0U;
    std::uint32_t instrument_id = 0U;
    std::int64_t channel = 0;
    std::int64_t order_id = 0;
};

enum class PartialOrderEventReaderOpenErrorV2 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidDescriptor,
    kInvalidExpectedSession,
    kDescriptorStatFailed,
    kDescriptorSealMismatch,
    kMappingFailed,
    kIncompatibleLayout,
    kSessionMismatch,
    kNoStablePublicationCut,
    kUnexpectedFailure,
};

enum class PartialOrderEventReadResultV2 : std::uint8_t {
    kOk = 0U,
    kInvalidArgument,
    kInconsistent,
    kNotYetPublished,
    kOutputTooSmall,
    kNotFound,
    kCorrupt,
};

struct PartialOrderEventReadBatchResultV2 final {
    std::size_t rows_read = 0U;
    std::uint64_t next_event_sequence = 0U;
    PartialOrderEventStatusSnapshotV2 status{};
};

struct PartialOrderEventOrderStateBatchResultV2 final {
    std::size_t rows_read = 0U;
    std::uint64_t next_physical_slot = 0U;
    PartialOrderEventStatusSnapshotV2 status{};
};

[[nodiscard]] std::string_view
PartialOrderEventReaderOpenErrorNameV2(
    PartialOrderEventReaderOpenErrorV2 error) noexcept;
[[nodiscard]] std::string_view PartialOrderEventReadResultNameV2(
    PartialOrderEventReadResultV2 result) noexcept;

// The reader duplicates and owns descriptor. The expected identity is
// mandatory so broker generation switches cannot be crossed silently.
class PartialOrderEventReaderV2 final {
public:
    PartialOrderEventReaderV2(const PartialOrderEventReaderV2&) = delete;
    PartialOrderEventReaderV2& operator=(
        const PartialOrderEventReaderV2&) = delete;
    PartialOrderEventReaderV2(PartialOrderEventReaderV2&&) = delete;
    PartialOrderEventReaderV2& operator=(
        PartialOrderEventReaderV2&&) = delete;
    ~PartialOrderEventReaderV2();

    [[nodiscard]] static PartialOrderEventReaderOpenErrorV2
    OpenDescriptor(
        int descriptor,
        const PartialOrderEventExpectedSessionV2& expected_session,
        std::unique_ptr<PartialOrderEventReaderV2>* output,
        int* system_error_number = nullptr) noexcept;

    [[nodiscard]] PartialOrderEventReadResultV2 ReadStatus(
        PartialOrderEventStatusSnapshotV2* output) const noexcept;

    [[nodiscard]] PartialOrderEventReadResultV2 ReadEvent(
        std::uint64_t derived_event_sequence,
        PartialOrderEventEnvelopeV2* output) const noexcept;

    [[nodiscard]] PartialOrderEventReadResultV2 ReadEvents(
        std::uint64_t first_derived_event_sequence,
        std::span<PartialOrderEventEnvelopeV2> output,
        PartialOrderEventReadBatchResultV2* result) const noexcept;

    // Reads the complete affected-channel bank for one stable cut and verifies
    // its CRC. output.size() must cover status.cut.channel_health_count.
    [[nodiscard]] PartialOrderEventReadResultV2 ReadAffectedChannels(
        std::span<PartialOrderEventChannelHealthV2> output,
        std::size_t* rows_read,
        PartialOrderEventStatusSnapshotV2* status = nullptr) const noexcept;

    [[nodiscard]] PartialOrderEventReadResultV2 FindOrderState(
        const PartialOrderEventOrderKeyV2& key,
        PartialOrderEventOrderStateV2* output,
        PartialOrderEventStatusSnapshotV2* status = nullptr) const noexcept;

    // Physical-slot pagination avoids a second mutable index. Invisible hash
    // entries and versions newer than the selected cut are skipped. Each call
    // is coherent with result.status, but the cursor does not pin that cut:
    // pages read during active publication are not one cross-page snapshot.
    // Once writes to this mapping are quiescent, a full-table scan is stable.
    [[nodiscard]] PartialOrderEventReadResultV2 ReadOrderStates(
        std::uint64_t first_physical_slot,
        std::span<PartialOrderEventOrderStateV2> output,
        PartialOrderEventOrderStateBatchResultV2* result) const noexcept;

private:
    class Impl;
    explicit PartialOrderEventReaderV2(
        std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::ipc
