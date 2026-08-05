#include "l2flow/ipc/instrument_data_service_v3.h"

#include <algorithm>
#include <array>
#include <limits>
#include <utility>

namespace l2flow::ipc {
namespace {

[[nodiscard]] RealtimeRepairStateWireV3 RepairState(
    l2flow::market::EventRepairStateV1 state) noexcept {
    using Source = l2flow::market::EventRepairStateV1;
    switch (state) {
        case Source::kLive:
            return RealtimeRepairStateWireV3::kLive;
        case Source::kRepairRequired:
            return RealtimeRepairStateWireV3::kRepairRequired;
        case Source::kRebuilding:
            return RealtimeRepairStateWireV3::kRebuilding;
        case Source::kCatchingUp:
            return RealtimeRepairStateWireV3::kCatchingUp;
        case Source::kSourceConflict:
            return RealtimeRepairStateWireV3::kSourceConflict;
        case Source::kUnrecoverable:
            return RealtimeRepairStateWireV3::kUnrecoverable;
    }
    return RealtimeRepairStateWireV3::kUnrecoverable;
}

}  // namespace

std::string_view InstrumentDataServiceErrorNameV3(
    InstrumentDataServiceErrorV3 error) noexcept {
    switch (error) {
        case InstrumentDataServiceErrorV3::kNone:
            return "none";
        case InstrumentDataServiceErrorV3::kNullOutput:
            return "null_output";
        case InstrumentDataServiceErrorV3::kInvalidConfiguration:
            return "invalid_configuration";
        case InstrumentDataServiceErrorV3::kCursorMismatch:
            return "cursor_mismatch";
        case InstrumentDataServiceErrorV3::kBatchLimitExceeded:
            return "batch_limit_exceeded";
        case InstrumentDataServiceErrorV3::kNotFound:
            return "not_found";
        case InstrumentDataServiceErrorV3::kFastReadFailed:
            return "fast_read_failed";
        case InstrumentDataServiceErrorV3::kEventReadFailed:
            return "event_read_failed";
        case InstrumentDataServiceErrorV3::kKLineReadFailed:
            return "kline_read_failed";
        case InstrumentDataServiceErrorV3::kResourceExhausted:
            return "resource_exhausted";
    }
    return "unknown";
}

class InstrumentDataServiceV3::Impl final {
public:
    Impl(
        InstrumentDataServiceConfigV3 config,
        const l2flow::runtime::RealtimePlanesV1* planes) noexcept
        : config_(config), planes_(planes) {}

    InstrumentDataServiceConfigV3 config_{};
    const l2flow::runtime::RealtimePlanesV1* planes_ = nullptr;
};

InstrumentDataServiceV3::InstrumentDataServiceV3(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

InstrumentDataServiceV3::~InstrumentDataServiceV3() = default;

InstrumentDataServiceErrorV3 InstrumentDataServiceV3::Create(
    InstrumentDataServiceConfigV3 config,
    const l2flow::runtime::RealtimePlanesV1* planes,
    std::unique_ptr<InstrumentDataServiceV3>* output) noexcept {
    if (output == nullptr) {
        return InstrumentDataServiceErrorV3::kNullOutput;
    }
    output->reset();
    if (planes == nullptr || config.maximum_fast_rows_per_read == 0U ||
        config.maximum_event_changes_per_read == 0U ||
        config.maximum_kline_changes_per_read == 0U ||
        config.maximum_instruments_per_batch == 0U) {
        return InstrumentDataServiceErrorV3::kInvalidConfiguration;
    }
    try {
        output->reset(new InstrumentDataServiceV3(
            std::make_unique<Impl>(config, planes)));
        return InstrumentDataServiceErrorV3::kNone;
    } catch (...) {
        return InstrumentDataServiceErrorV3::kResourceExhausted;
    }
}

InstrumentDataServiceErrorV3 InstrumentDataServiceV3::ReadFastDelta(
    const FastTickCursorWireV3& cursor,
    std::size_t maximum_rows,
    FastTickDeltaV3* output) const noexcept {
    if (output == nullptr) {
        return InstrumentDataServiceErrorV3::kNullOutput;
    }
    *output = {};
    const auto& store = impl_->planes_->fast_store();
    if (cursor.reserved != 0U || cursor.session_id !=
            store.config().session_id || cursor.instrument_id == 0U ||
        cursor.next_arrival_row == 0U) {
        return InstrumentDataServiceErrorV3::kCursorMismatch;
    }
    if (maximum_rows == 0U ||
        maximum_rows > impl_->config_.maximum_fast_rows_per_read ||
        maximum_rows > store.config().maximum_records_per_read) {
        return InstrumentDataServiceErrorV3::kBatchLimitExceeded;
    }
    l2flow::market::FastTickInstrumentStatusV1 status{};
    if (store.Status(cursor.instrument_id, &status) !=
        l2flow::market::FastTickStoreQueryErrorV1::kNone) {
        return InstrumentDataServiceErrorV3::kNotFound;
    }
    std::unique_ptr<l2flow::market::FastTickCursorV1> reader;
    if (store.OpenCursor(
            cursor.instrument_id,
            cursor.next_arrival_row,
            &reader) != l2flow::market::FastTickStoreQueryErrorV1::kNone ||
        reader == nullptr) {
        return InstrumentDataServiceErrorV3::kCursorMismatch;
    }
    try {
        std::vector<const l2flow::market::FastTickRecordV1*> records(
            maximum_rows);
        std::size_t written = 0U;
        if (reader->ReadBatch(records, &written) !=
            l2flow::market::FastTickStoreQueryErrorV1::kNone) {
            return InstrumentDataServiceErrorV3::kFastReadFailed;
        }
        output->rows.reserve(written);
        for (std::size_t index = 0U; index < written; ++index) {
            output->rows.push_back(FastTickDeltaRowV3{
                records[index]->instrument_tick_sequence(),
                records[index]->compact()});
        }
        output->next = cursor;
        output->next.next_arrival_row = reader->next_arrival_row();
        output->captured_tail = reader->target_tail();
        // Coverage loss is monotonic. Sample it after copying so a failure
        // that occurred during this read cannot be reported as complete.
        if (store.Status(cursor.instrument_id, &status) !=
            l2flow::market::FastTickStoreQueryErrorV1::kNone) {
            *output = {};
            return InstrumentDataServiceErrorV3::kFastReadFailed;
        }
        output->coverage_from_open = status.coverage_from_open;
        output->coverage_complete = status.coverage_complete;
        return InstrumentDataServiceErrorV3::kNone;
    } catch (...) {
        *output = {};
        return InstrumentDataServiceErrorV3::kResourceExhausted;
    }
}

InstrumentDataServiceErrorV3 InstrumentDataServiceV3::ReadFastBatch(
    std::span<const FastTickCursorWireV3> cursors,
    std::size_t maximum_rows_per_instrument,
    std::vector<FastTickDeltaV3>* output) const noexcept {
    if (output == nullptr) {
        return InstrumentDataServiceErrorV3::kNullOutput;
    }
    output->clear();
    if (cursors.empty() ||
        cursors.size() > impl_->config_.maximum_instruments_per_batch) {
        return InstrumentDataServiceErrorV3::kBatchLimitExceeded;
    }
    try {
        output->reserve(cursors.size());
        for (const FastTickCursorWireV3& cursor : cursors) {
            FastTickDeltaV3 delta{};
            const InstrumentDataServiceErrorV3 error = ReadFastDelta(
                cursor, maximum_rows_per_instrument, &delta);
            if (error != InstrumentDataServiceErrorV3::kNone) {
                output->clear();
                return error;
            }
            output->push_back(std::move(delta));
        }
        return InstrumentDataServiceErrorV3::kNone;
    } catch (...) {
        output->clear();
        return InstrumentDataServiceErrorV3::kResourceExhausted;
    }
}

InstrumentDataServiceErrorV3
InstrumentDataServiceV3::AcquireEventStable(
    std::uint32_t instrument_id,
    EventStableViewV3* output) const noexcept {
    if (output == nullptr) {
        return InstrumentDataServiceErrorV3::kNullOutput;
    }
    *output = {};
    l2flow::market::EventStableSnapshotV1 snapshot{};
    if (impl_->planes_->event_history().AcquireStable(
            instrument_id, &snapshot) !=
        l2flow::market::OrderedEventHistoryErrorV1::kNone) {
        return InstrumentDataServiceErrorV3::kEventReadFailed;
    }
    output->root = std::move(snapshot.root);
    output->next_changes.session_id =
        snapshot.next_changes.session_id;
    output->next_changes.instrument_id = instrument_id;
    output->next_changes.next_change_sequence =
        snapshot.next_changes.next_change_sequence;
    output->status.dataset = static_cast<std::uint8_t>(
        RealtimeDatasetV3::kDerivedEvent);
    output->status.repair_state = static_cast<std::uint8_t>(
        RepairState(snapshot.repair_state));
    output->status.instrument_id = instrument_id;
    output->status.stable_tail =
        output->root == nullptr ? 0U : output->root->row_count();
    output->status.repair_through_arrival_id =
        snapshot.repair_through_arrival_id;
    return InstrumentDataServiceErrorV3::kNone;
}

InstrumentDataServiceErrorV3 InstrumentDataServiceV3::ReadEventChanges(
    EventChangeCursorWireV3* cursor,
    std::span<l2flow::market::EventMutationV1> output,
    std::size_t* written) const noexcept {
    if (cursor == nullptr || written == nullptr) {
        return InstrumentDataServiceErrorV3::kNullOutput;
    }
    *written = 0U;
    if (cursor->reserved != 0U) {
        return InstrumentDataServiceErrorV3::kCursorMismatch;
    }
    if (output.empty() ||
        output.size() > impl_->config_.maximum_event_changes_per_read) {
        return InstrumentDataServiceErrorV3::kBatchLimitExceeded;
    }
    l2flow::market::EventChangeCursorV1 native{};
    native.session_id = cursor->session_id;
    native.instrument_id = cursor->instrument_id;
    native.next_change_sequence = cursor->next_change_sequence;
    const auto error = impl_->planes_->event_history().ReadChanges(
        &native, output, written);
    if (error != l2flow::market::OrderedEventHistoryErrorV1::kNone) {
        if (error == l2flow::market::OrderedEventHistoryErrorV1::
                         kCursorMismatch) {
            return InstrumentDataServiceErrorV3::kCursorMismatch;
        }
        if (error == l2flow::market::OrderedEventHistoryErrorV1::
                         kBatchLimitExceeded) {
            return InstrumentDataServiceErrorV3::kBatchLimitExceeded;
        }
        return InstrumentDataServiceErrorV3::kEventReadFailed;
    }
    cursor->next_change_sequence = native.next_change_sequence;
    return InstrumentDataServiceErrorV3::kNone;
}

InstrumentDataServiceErrorV3
InstrumentDataServiceV3::AcquireKLineStable(
    std::uint32_t instrument_id,
    KLineStableViewV3* output) const noexcept {
    if (output == nullptr) {
        return InstrumentDataServiceErrorV3::kNullOutput;
    }
    *output = {};
    l2flow::market::KLineStableSnapshotV1 snapshot{};
    if (impl_->planes_->kline_history().AcquireStable(
            instrument_id, &snapshot) !=
        l2flow::market::MutableKLineHistoryErrorV1::kNone) {
        return InstrumentDataServiceErrorV3::kKLineReadFailed;
    }
    output->root = std::move(snapshot.root);
    output->next_changes.session_id =
        snapshot.next_changes.session_id;
    output->next_changes.instrument_id = instrument_id;
    output->next_changes.next_change_sequence =
        snapshot.next_changes.next_change_sequence;
    output->status.dataset = static_cast<std::uint8_t>(
        RealtimeDatasetV3::kKLine);
    output->status.repair_state = static_cast<std::uint8_t>(
        RepairState(snapshot.repair_state));
    output->status.instrument_id = instrument_id;
    output->status.stable_tail =
        output->root == nullptr ? 0U : output->root->bar_count();
    output->status.repair_through_arrival_id =
        snapshot.repair_through_arrival_id;
    return InstrumentDataServiceErrorV3::kNone;
}

InstrumentDataServiceErrorV3 InstrumentDataServiceV3::ReadKLineChanges(
    KLineChangeCursorWireV3* cursor,
    std::span<l2flow::market::KLineMutationV1> output,
    std::size_t* written) const noexcept {
    if (cursor == nullptr || written == nullptr) {
        return InstrumentDataServiceErrorV3::kNullOutput;
    }
    *written = 0U;
    if (cursor->reserved != 0U) {
        return InstrumentDataServiceErrorV3::kCursorMismatch;
    }
    if (output.empty() ||
        output.size() > impl_->config_.maximum_kline_changes_per_read) {
        return InstrumentDataServiceErrorV3::kBatchLimitExceeded;
    }
    l2flow::market::KLineChangeCursorV1 native{};
    native.session_id = cursor->session_id;
    native.instrument_id = cursor->instrument_id;
    native.next_change_sequence = cursor->next_change_sequence;
    const auto error = impl_->planes_->kline_history().ReadChanges(
        &native, output, written);
    if (error != l2flow::market::MutableKLineHistoryErrorV1::kNone) {
        if (error == l2flow::market::MutableKLineHistoryErrorV1::
                         kCursorMismatch) {
            return InstrumentDataServiceErrorV3::kCursorMismatch;
        }
        if (error == l2flow::market::MutableKLineHistoryErrorV1::
                         kBatchLimitExceeded) {
            return InstrumentDataServiceErrorV3::kBatchLimitExceeded;
        }
        return InstrumentDataServiceErrorV3::kKLineReadFailed;
    }
    cursor->next_change_sequence = native.next_change_sequence;
    return InstrumentDataServiceErrorV3::kNone;
}

const InstrumentDataServiceConfigV3& InstrumentDataServiceV3::config()
    const noexcept {
    return impl_->config_;
}

}  // namespace l2flow::ipc
