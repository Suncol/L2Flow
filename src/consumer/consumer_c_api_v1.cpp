#include "l2flow/consumer/consumer_c_api_v1.h"

#include "l2flow/canonical/canonical_schema_v1.h"
#include "l2flow/canonical/safe_mux_v1.h"
#include "l2flow/consumer/canonical_batch_reader_v1.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <new>
#include <span>
#include <string>
#include <utility>
#include <vector>

struct l2flow_consumer_batch_reader_handle_v1 final {
    l2flow_consumer_raw_durability_observer_v1 c_observer = nullptr;
    void* c_observer_context = nullptr;
    std::shared_ptr<const l2flow::canonical::CanonicalSegmentReaderV1>
        mapping;
    std::unique_ptr<l2flow::consumer::CanonicalCommittedBatchReaderV1>
        reader;
};

struct l2flow_consumer_batch_view_handle_v1 final {
    l2flow::consumer::MdlBatchViewV1 view;
};

namespace {

constexpr std::size_t kMaximumCInputsV1 = 65'536U;

[[nodiscard]] bool PointerAligned(
    const void* pointer,
    std::size_t alignment) noexcept {
    return pointer != nullptr &&
           reinterpret_cast<std::uintptr_t>(pointer) % alignment == 0U;
}

[[nodiscard]] bool AllZero(
    const std::uint8_t* bytes,
    std::size_t size) noexcept {
    return std::all_of(
        bytes, bytes + size,
        [](std::uint8_t value) { return value == 0U; });
}

[[nodiscard]] std::uint32_t ExpectedRecordSize(
    std::uint16_t event_type) noexcept {
    using l2flow::canonical::CanonicalEventTypeV1;
    switch (static_cast<CanonicalEventTypeV1>(event_type)) {
        case CanonicalEventTypeV1::kSnapshot:
            return static_cast<std::uint32_t>(
                l2flow::canonical::kCanonicalSnapshotRecordBytesV1);
        case CanonicalEventTypeV1::kTick:
            return static_cast<std::uint32_t>(
                l2flow::canonical::kCanonicalTickRecordBytesV1);
        case CanonicalEventTypeV1::kQuality:
            return static_cast<std::uint32_t>(
                l2flow::canonical::kCanonicalQualityRecordBytesV1);
        case CanonicalEventTypeV1::kControl:
            return static_cast<std::uint32_t>(
                l2flow::canonical::kCanonicalControlRecordBytesV1);
        case CanonicalEventTypeV1::kUnknown:
            return 0U;
    }
    return 0U;
}

[[nodiscard]] l2flow::canonical::ClockEpochIdentityV1 Clock(
    const l2flow_consumer_clock_epoch_v1& input) noexcept {
    l2flow::canonical::ClockEpochIdentityV1 output{};
    output.algorithm = input.algorithm;
    std::memcpy(output.digest.data(), input.digest, output.digest.size());
    output.label = input.label;
    return output;
}

[[nodiscard]] l2flow::canonical::CanonicalEventKeyV1 Key(
    const l2flow_consumer_event_key_v1& input) noexcept {
    l2flow::canonical::CanonicalEventKeyV1 output{};
    output.recv_monotonic_ns = input.recv_monotonic_ns;
    output.source_stream_id = input.source_stream_id;
    output.origin_ingress_sequence = input.origin_ingress_sequence;
    output.sub_index = input.sub_index;
    return output;
}

void StoreClock(
    const l2flow::canonical::ClockEpochIdentityV1& input,
    l2flow_consumer_clock_epoch_v1* output) noexcept {
    output->algorithm = input.algorithm;
    std::memcpy(output->digest, input.digest.data(), input.digest.size());
    output->label = input.label;
}

void StoreKey(
    const l2flow::canonical::CanonicalEventKeyV1& input,
    l2flow_consumer_event_key_v1* output) noexcept {
    output->recv_monotonic_ns = input.recv_monotonic_ns;
    output->source_stream_id = input.source_stream_id;
    output->origin_ingress_sequence = input.origin_ingress_sequence;
    output->sub_index = input.sub_index;
}

[[nodiscard]] bool ConvertInput(
    const l2flow_consumer_mux_input_v1& input,
    l2flow::canonical::SafeMuxInputV1* output) noexcept {
    if (output == nullptr || input.required > 1U || input.has_next > 1U ||
        input.reserved0 != 0U) {
        return false;
    }
    l2flow::canonical::SafeMuxInputV1 candidate{};
    candidate.required = input.required != 0U;
    candidate.has_next = input.has_next != 0U;
    candidate.next_key = Key(input.next_key);
    candidate.next_origin_wal_end_pos = input.next_origin_wal_end_pos;
    candidate.next_clock_epoch = Clock(input.next_clock_epoch);
    candidate.next_capture_date = input.next_capture_date;
    std::memcpy(
        candidate.next_stream_day_id.data(),
        input.next_stream_day_id,
        candidate.next_stream_day_id.size());
    std::memcpy(
        candidate.next_writer_instance.data(),
        input.next_writer_instance,
        candidate.next_writer_instance.size());
    candidate.next_generation = input.next_generation;
    candidate.frontier_page =
        static_cast<const l2flow::canonical::SourceFrontierPageV1*>(
            input.frontier_page);
    if (candidate.frontier_page != nullptr &&
        !PointerAligned(
            candidate.frontier_page,
            alignof(l2flow::canonical::SourceFrontierPageV1))) {
        return false;
    }
    *output = candidate;
    return true;
}

[[nodiscard]] l2flow::canonical::CanonicalSegmentDescriptorV1 Descriptor(
    const l2flow_consumer_segment_descriptor_v1& input) noexcept {
    l2flow::canonical::CanonicalSegmentDescriptorV1 output{};
    output.event_type = static_cast<
        l2flow::canonical::CanonicalEventTypeV1>(input.event_type);
    output.record_size = input.record_size;
    output.source_stream_id = input.source_stream_id;
    output.shard = input.shard_id;
    output.trade_date = input.trade_date;
    output.origin_capture_date = input.origin_capture_date;
    std::memcpy(
        output.origin_stream_day_id.data(),
        input.origin_stream_day_id,
        output.origin_stream_day_id.size());
    std::memcpy(
        output.origin_source_writer_instance.data(),
        input.origin_source_writer_instance,
        output.origin_source_writer_instance.size());
    output.origin_source_generation = input.origin_source_generation;
    output.clock_epoch = Clock(input.clock_epoch);
    std::memcpy(
        output.schema_sha256.data(), input.schema_sha256,
        output.schema_sha256.size());
    std::memcpy(
        output.dtype_sha256.data(), input.dtype_sha256,
        output.dtype_sha256.size());
    output.registry_version = input.registry_version;
    std::memcpy(
        output.registry_sha256.data(), input.registry_sha256,
        output.registry_sha256.size());
    std::memcpy(
        output.normalizer_build_sha256.data(),
        input.normalizer_build_sha256,
        output.normalizer_build_sha256.size());
    std::memcpy(
        output.normalizer_config_sha256.data(),
        input.normalizer_config_sha256,
        output.normalizer_config_sha256.size());
    output.generation = input.canonical_generation;
    output.segment_sequence = input.segment_sequence;
    output.capacity_records = input.capacity_records;
    return output;
}

[[nodiscard]] bool ObserveRawForC(
    void* opaque,
    l2flow::ingress::RawControlSnapshot* output) noexcept {
    auto* handle =
        static_cast<l2flow_consumer_batch_reader_handle_v1*>(opaque);
    if (handle == nullptr || output == nullptr || handle->c_observer == nullptr) {
        return false;
    }
    l2flow_consumer_raw_control_snapshot_v1 observed{};
    int success = 0;
    try {
        success = handle->c_observer(
            handle->c_observer_context, &observed);
    } catch (...) {
        return false;
    }
    if (success == 0) {
        return false;
    }
    l2flow::ingress::RawControlSnapshot candidate{};
    std::memcpy(
        candidate.writer_instance.data(), observed.writer_instance,
        candidate.writer_instance.size());
    std::memcpy(
        candidate.stream_day_id.data(), observed.stream_day_id,
        candidate.stream_day_id.size());
    candidate.source_stream_id = observed.source_stream_id;
    candidate.capture_date = observed.capture_date;
    candidate.segment_sequence = observed.segment_sequence;
    candidate.fatal_state = observed.fatal_state;
    candidate.append_global_wal_pos = observed.append_global_wal_pos;
    candidate.append_ingress_sequence = observed.append_ingress_sequence;
    candidate.append_segment_offset = observed.append_segment_offset;
    candidate.durable_global_wal_pos = observed.durable_global_wal_pos;
    candidate.durable_ingress_sequence = observed.durable_ingress_sequence;
    candidate.durable_segment_offset = observed.durable_segment_offset;
    candidate.clock_epoch_label = observed.clock_epoch_label;
    candidate.heartbeat_monotonic_ns = observed.heartbeat_monotonic_ns;
    *output = candidate;
    return true;
}

[[nodiscard]] int BatchError(
    l2flow::consumer::CanonicalBatchErrorV1 error) noexcept {
    if (error == l2flow::consumer::CanonicalBatchErrorV1::kNone) {
        return L2FLOW_CONSUMER_C_OK_V1;
    }
    if (error == l2flow::consumer::CanonicalBatchErrorV1::kWouldBlock) {
        return L2FLOW_CONSUMER_C_BATCH_WOULD_BLOCK_V1;
    }
    return L2FLOW_CONSUMER_C_BATCH_FAILED_V1;
}

}  // namespace

extern "C" int l2flow_consumer_validate_attach_v1(
    const l2flow_consumer_attach_identity_v1* expected,
    const l2flow_consumer_attach_identity_v1* actual) {
    if (expected == nullptr || actual == nullptr) {
        return L2FLOW_CONSUMER_C_NULL_ARGUMENT_V1;
    }
    if (!l2flow::canonical::CanonicalHostIsLittleEndianV1()) {
        return L2FLOW_CONSUMER_C_UNSUPPORTED_ABI_V1;
    }
    if (expected->reserved0 != 0U || actual->reserved0 != 0U ||
        expected->registry_version == 0U ||
        AllZero(expected->schema_sha256, 32U) ||
        AllZero(expected->dtype_sha256, 32U) ||
        AllZero(expected->registry_sha256, 32U)) {
        return L2FLOW_CONSUMER_C_INVALID_ARGUMENT_V1;
    }
    const std::uint32_t fixed_size =
        ExpectedRecordSize(expected->event_type);
    if (fixed_size == 0U || expected->record_size != fixed_size) {
        return L2FLOW_CONSUMER_C_INVALID_ARGUMENT_V1;
    }
    if (actual->event_type != expected->event_type) {
        return L2FLOW_CONSUMER_C_EVENT_TYPE_MISMATCH_V1;
    }
    if (actual->record_size != expected->record_size) {
        return L2FLOW_CONSUMER_C_RECORD_SIZE_MISMATCH_V1;
    }
    if (std::memcmp(
            actual->schema_sha256, expected->schema_sha256, 32U) != 0) {
        return L2FLOW_CONSUMER_C_SCHEMA_MISMATCH_V1;
    }
    if (std::memcmp(
            actual->dtype_sha256, expected->dtype_sha256, 32U) != 0) {
        return L2FLOW_CONSUMER_C_DTYPE_MISMATCH_V1;
    }
    if (actual->registry_version != expected->registry_version ||
        std::memcmp(
            actual->registry_sha256,
            expected->registry_sha256,
            32U) != 0) {
        return L2FLOW_CONSUMER_C_REGISTRY_MISMATCH_V1;
    }
    return L2FLOW_CONSUMER_C_OK_V1;
}

extern "C" int l2flow_consumer_safe_mux_select_v1(
    const l2flow_consumer_mux_input_v1* inputs,
    std::size_t input_count,
    l2flow_consumer_mux_selection_v1* output) {
    if (output == nullptr) {
        return L2FLOW_CONSUMER_C_NULL_ARGUMENT_V1;
    }
    *output = l2flow_consumer_mux_selection_v1{};
    output->decision = static_cast<std::uint8_t>(
        l2flow::canonical::SafeMuxDecisionV1::kInvalidInput);
    if (inputs == nullptr || input_count == 0U ||
        input_count > kMaximumCInputsV1) {
        return L2FLOW_CONSUMER_C_INVALID_ARGUMENT_V1;
    }
    try {
        std::vector<l2flow::canonical::SafeMuxInputV1> converted(input_count);
        for (std::size_t index = 0U; index < input_count; ++index) {
            if (!ConvertInput(inputs[index], &converted[index])) {
                return L2FLOW_CONSUMER_C_INVALID_ARGUMENT_V1;
            }
        }
        const auto selected =
            l2flow::canonical::SelectSafeMuxCandidateV1(converted);
        output->decision = static_cast<std::uint8_t>(selected.decision);
        output->input_index = selected.input_index;
        StoreKey(selected.key, &output->key);
        StoreClock(selected.clock_epoch, &output->clock_epoch);
    } catch (const std::bad_alloc&) {
        return L2FLOW_CONSUMER_C_RESOURCE_EXHAUSTED_V1;
    } catch (...) {
        return L2FLOW_CONSUMER_C_RESOURCE_EXHAUSTED_V1;
    }
    return L2FLOW_CONSUMER_C_OK_V1;
}

extern "C" int l2flow_consumer_snapshot_asof_prove_v1(
    const l2flow_consumer_event_key_v1* tick,
    const l2flow_consumer_clock_epoch_v1* tick_clock_epoch,
    const l2flow_consumer_mux_input_v1* snapshot_input,
    l2flow_consumer_snapshot_asof_proof_v1* output) {
    if (output == nullptr) {
        return L2FLOW_CONSUMER_C_NULL_ARGUMENT_V1;
    }
    *output = l2flow_consumer_snapshot_asof_proof_v1{};
    output->proof = static_cast<std::uint8_t>(
        l2flow::canonical::SnapshotAsofProofV1::kInvalidInput);
    if (tick == nullptr || tick_clock_epoch == nullptr ||
        snapshot_input == nullptr) {
        return L2FLOW_CONSUMER_C_NULL_ARGUMENT_V1;
    }
    l2flow::canonical::SafeMuxInputV1 converted{};
    if (!ConvertInput(*snapshot_input, &converted)) {
        return L2FLOW_CONSUMER_C_INVALID_ARGUMENT_V1;
    }
    const auto proof = l2flow::canonical::ProveSnapshotAsofTickV1(
        Key(*tick), Clock(*tick_clock_epoch), converted);
    output->proof = static_cast<std::uint8_t>(proof);
    return L2FLOW_CONSUMER_C_OK_V1;
}

extern "C" int l2flow_consumer_snapshot_asof_select_v1(
    const l2flow_consumer_event_key_v1* consumed_snapshots,
    std::size_t snapshot_count,
    const l2flow_consumer_event_key_v1* tick,
    l2flow_consumer_snapshot_asof_selection_v1* output) {
    if (output == nullptr) {
        return L2FLOW_CONSUMER_C_NULL_ARGUMENT_V1;
    }
    *output = l2flow_consumer_snapshot_asof_selection_v1{};
    if (tick == nullptr ||
        (snapshot_count != 0U && consumed_snapshots == nullptr) ||
        snapshot_count > kMaximumCInputsV1) {
        return L2FLOW_CONSUMER_C_INVALID_ARGUMENT_V1;
    }
    try {
        std::vector<l2flow::canonical::CanonicalEventKeyV1> converted;
        converted.reserve(snapshot_count);
        for (std::size_t index = 0U; index < snapshot_count; ++index) {
            converted.push_back(Key(consumed_snapshots[index]));
        }
        const auto selected =
            l2flow::canonical::SelectLatestSnapshotAsofV1(
                converted, Key(*tick));
        output->found = selected.found ? 1U : 0U;
        output->index = selected.index;
    } catch (const std::bad_alloc&) {
        return L2FLOW_CONSUMER_C_RESOURCE_EXHAUSTED_V1;
    } catch (...) {
        return L2FLOW_CONSUMER_C_RESOURCE_EXHAUSTED_V1;
    }
    return L2FLOW_CONSUMER_C_OK_V1;
}

extern "C" int l2flow_consumer_batch_open_v1(
    const l2flow_consumer_batch_open_config_v1* config,
    l2flow_consumer_batch_reader_handle_v1** output) {
    if (output == nullptr) {
        return L2FLOW_CONSUMER_C_NULL_ARGUMENT_V1;
    }
    *output = nullptr;
    if (config == nullptr || config->segment_path == nullptr ||
        config->segment_path[0] == '\0' ||
        config->raw_durability_observer == nullptr ||
        !PointerAligned(
            config->source_frontier_page,
            alignof(l2flow::canonical::SourceFrontierPageV1)) ||
        config->expected_segment.reserved0 != 0U) {
        return L2FLOW_CONSUMER_C_INVALID_ARGUMENT_V1;
    }
    l2flow_consumer_attach_identity_v1 descriptor_attach{};
    descriptor_attach.event_type = config->expected_segment.event_type;
    descriptor_attach.record_size = config->expected_segment.record_size;
    std::memcpy(
        descriptor_attach.schema_sha256,
        config->expected_segment.schema_sha256,
        32U);
    std::memcpy(
        descriptor_attach.dtype_sha256,
        config->expected_segment.dtype_sha256,
        32U);
    descriptor_attach.registry_version =
        config->expected_segment.registry_version;
    std::memcpy(
        descriptor_attach.registry_sha256,
        config->expected_segment.registry_sha256,
        32U);
    const int attach_error = l2flow_consumer_validate_attach_v1(
        &config->expected_attach, &descriptor_attach);
    if (attach_error != L2FLOW_CONSUMER_C_OK_V1) {
        return attach_error;
    }

    auto handle = std::unique_ptr<l2flow_consumer_batch_reader_handle_v1>(
        new (std::nothrow) l2flow_consumer_batch_reader_handle_v1{});
    if (handle == nullptr) {
        return L2FLOW_CONSUMER_C_RESOURCE_EXHAUSTED_V1;
    }
    const auto descriptor = Descriptor(config->expected_segment);
    std::unique_ptr<l2flow::canonical::CanonicalSegmentReaderV1> opened;
    try {
        if (l2flow::canonical::CanonicalSegmentReaderV1::Open(
                std::string(config->segment_path), descriptor, &opened) !=
                l2flow::canonical::CanonicalSegmentErrorV1::kNone ||
            opened == nullptr) {
            return L2FLOW_CONSUMER_C_OPEN_FAILED_V1;
        }
        handle->mapping =
            std::shared_ptr<const l2flow::canonical::CanonicalSegmentReaderV1>(
                std::move(opened));
    } catch (const std::bad_alloc&) {
        return L2FLOW_CONSUMER_C_RESOURCE_EXHAUSTED_V1;
    } catch (...) {
        return L2FLOW_CONSUMER_C_OPEN_FAILED_V1;
    }
    handle->c_observer = config->raw_durability_observer;
    handle->c_observer_context = config->raw_durability_observer_context;
    l2flow::consumer::CanonicalBatchReaderConfigV1 reader_config{};
    reader_config.segment_reader = handle->mapping;
    reader_config.source_frontier = static_cast<
        const l2flow::canonical::SourceFrontierPageV1*>(
            config->source_frontier_page);
    reader_config.expected.event_type =
        static_cast<l2flow::canonical::CanonicalEventTypeV1>(
            config->expected_attach.event_type);
    reader_config.expected.record_size =
        config->expected_attach.record_size;
    std::memcpy(
        reader_config.expected.schema_sha256.data(),
        config->expected_attach.schema_sha256,
        reader_config.expected.schema_sha256.size());
    std::memcpy(
        reader_config.expected.dtype_sha256.data(),
        config->expected_attach.dtype_sha256,
        reader_config.expected.dtype_sha256.size());
    reader_config.expected.registry_version =
        config->expected_attach.registry_version;
    std::memcpy(
        reader_config.expected.registry_sha256.data(),
        config->expected_attach.registry_sha256,
        reader_config.expected.registry_sha256.size());
    reader_config.initial_canonical_cursor =
        config->initial_canonical_cursor;
    reader_config.raw_durability_observer = &ObserveRawForC;
    reader_config.raw_durability_observer_context = handle.get();
    const auto create_error =
        l2flow::consumer::CanonicalCommittedBatchReaderV1::Create(
            std::move(reader_config), &handle->reader);
    if (create_error != l2flow::consumer::CanonicalBatchErrorV1::kNone) {
        return BatchError(create_error);
    }
    *output = handle.release();
    return L2FLOW_CONSUMER_C_OK_V1;
}

extern "C" int l2flow_consumer_batch_peek_v1(
    l2flow_consumer_batch_reader_handle_v1* reader,
    std::size_t maximum_records,
    std::uint64_t watermark_set_id,
    l2flow_consumer_batch_view_handle_v1** output) {
    if (output == nullptr) {
        return L2FLOW_CONSUMER_C_NULL_ARGUMENT_V1;
    }
    *output = nullptr;
    if (reader == nullptr || reader->reader == nullptr) {
        return L2FLOW_CONSUMER_C_INVALID_ARGUMENT_V1;
    }
    auto view = std::unique_ptr<l2flow_consumer_batch_view_handle_v1>(
        new (std::nothrow) l2flow_consumer_batch_view_handle_v1{});
    if (view == nullptr) {
        return L2FLOW_CONSUMER_C_RESOURCE_EXHAUSTED_V1;
    }
    const auto error = reader->reader->Peek(
        maximum_records, watermark_set_id, &view->view);
    if (error != l2flow::consumer::CanonicalBatchErrorV1::kNone) {
        return BatchError(error);
    }
    *output = view.release();
    return L2FLOW_CONSUMER_C_OK_V1;
}

extern "C" int l2flow_consumer_batch_view_records_v1(
    const l2flow_consumer_batch_view_handle_v1* view,
    const std::uint8_t** records,
    std::size_t* record_count,
    std::uint32_t* record_size) {
    if (view == nullptr || records == nullptr || record_count == nullptr ||
        record_size == nullptr || !view->view.valid()) {
        return L2FLOW_CONSUMER_C_NULL_ARGUMENT_V1;
    }
    *records = reinterpret_cast<const std::uint8_t*>(
        view->view.records_bytes().data());
    *record_count = view->view.record_count();
    *record_size = view->view.record_size();
    return L2FLOW_CONSUMER_C_OK_V1;
}

extern "C" int l2flow_consumer_batch_view_metadata_v1(
    const l2flow_consumer_batch_view_handle_v1* view,
    l2flow_consumer_batch_metadata_v1* output) {
    if (view == nullptr || output == nullptr || !view->view.valid()) {
        return L2FLOW_CONSUMER_C_NULL_ARGUMENT_V1;
    }
    *output = l2flow_consumer_batch_metadata_v1{};
    const auto& input = view->view.metadata();
    output->source_stream_id = input.source_stream_id;
    output->origin_capture_date = input.origin_capture_date;
    output->trade_date = input.trade_date;
    std::memcpy(
        output->origin_stream_day_id,
        input.origin_stream_day_id.data(),
        input.origin_stream_day_id.size());
    output->family = static_cast<std::uint16_t>(input.family);
    output->shard_id = input.shard_id;
    output->begin_canonical_cursor = input.begin_canonical_cursor;
    output->end_canonical_cursor = input.end_canonical_cursor;
    output->max_consumed_origin_wal_end_pos =
        input.max_consumed_origin_wal_end_pos;
    output->observed_raw_durable_wal_pos =
        input.observed_raw_durable_wal_pos;
    StoreClock(input.clock_epoch, &output->clock_epoch);
    std::memcpy(
        output->schema_sha256,
        input.schema_sha256.data(), input.schema_sha256.size());
    std::memcpy(
        output->dtype_sha256,
        input.dtype_sha256.data(), input.dtype_sha256.size());
    output->registry_version = input.registry_version;
    std::memcpy(
        output->registry_sha256,
        input.registry_sha256.data(), input.registry_sha256.size());
    output->batch_quality_flags = input.batch_quality_flags;
    output->watermark_set_id = input.watermark_set_id;
    std::memcpy(
        output->origin_source_writer_instance,
        input.origin_source_writer_instance.data(),
        input.origin_source_writer_instance.size());
    output->origin_source_generation = input.origin_source_generation;
    output->canonical_generation = input.canonical_generation;
    return L2FLOW_CONSUMER_C_OK_V1;
}

extern "C" int l2flow_consumer_batch_commit_v1(
    l2flow_consumer_batch_reader_handle_v1* reader,
    const l2flow_consumer_batch_view_handle_v1* view) {
    if (reader == nullptr || reader->reader == nullptr || view == nullptr) {
        return L2FLOW_CONSUMER_C_NULL_ARGUMENT_V1;
    }
    return BatchError(reader->reader->Commit(view->view));
}

extern "C" std::uint64_t l2flow_consumer_batch_cursor_v1(
    const l2flow_consumer_batch_reader_handle_v1* reader) {
    return reader == nullptr || reader->reader == nullptr
               ? 0U
               : reader->reader->cursor();
}

extern "C" void l2flow_consumer_batch_view_release_v1(
    l2flow_consumer_batch_view_handle_v1* view) {
    delete view;
}

extern "C" void l2flow_consumer_batch_reader_release_v1(
    l2flow_consumer_batch_reader_handle_v1* reader) {
    delete reader;
}
