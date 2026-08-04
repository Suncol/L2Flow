#include "l2flow/ipc/certified_order_event_reader_c_v1.h"

#include "l2flow/ipc/certified_order_event_reader_v1.h"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <new>
#include <span>
#include <utility>

namespace ipc = l2flow::ipc;

struct l2flow_certified_order_event_reader_v1 final {
    std::unique_ptr<ipc::CertifiedOrderEventReaderV1> reader;
};

namespace {

constexpr std::uint32_t kStatusSchemaVersion = 1U;
constexpr std::uint32_t kResultSchemaVersion = 1U;

static_assert(
    sizeof(l2flow_certified_order_event_expected_session_v1) == 32U);
static_assert(sizeof(l2flow_certified_order_event_session_v1) == 64U);
static_assert(sizeof(l2flow_certified_order_event_status_v1) == 232U);
static_assert(
    sizeof(l2flow_certified_order_event_envelope_v1) ==
    sizeof(ipc::CertifiedOrderEventEnvelopeV1));
static_assert(sizeof(l2flow_certified_order_event_envelope_v1) == 328U);
static_assert(
    sizeof(l2flow_certified_order_event_read_batch_result_v1) == 288U);
static_assert(
    alignof(l2flow_certified_order_event_envelope_v1) ==
    alignof(ipc::CertifiedOrderEventEnvelopeV1));
static_assert(
    offsetof(
        l2flow_certified_order_event_envelope_v1,
        canonical_apply_sequence) ==
    offsetof(
        ipc::CertifiedOrderEventEnvelopeV1,
        canonical_apply_sequence));
static_assert(
    offsetof(l2flow_certified_order_event_envelope_v1, event) ==
    offsetof(ipc::CertifiedOrderEventEnvelopeV1, event));

static_assert(
    static_cast<int>(
        ipc::CertifiedOrderEventReaderOpenErrorV1::kUnexpectedFailure) ==
    L2FLOW_CERTIFIED_ORDER_EVENT_OPEN_UNEXPECTED_FAILURE_V1);
static_assert(
    static_cast<int>(
        ipc::CertifiedOrderEventReadResultV1::kProducerFailed) ==
    L2FLOW_CERTIFIED_ORDER_EVENT_READ_PRODUCER_FAILED_V1);
static_assert(
    static_cast<std::uint32_t>(
        ipc::RealtimeCertifiedStateV1::kStopped) ==
    L2FLOW_CERTIFIED_ORDER_EVENT_STATE_STOPPED_V1);
static_assert(
    static_cast<std::uint32_t>(
        ipc::RealtimeCertifiedStateV1::kDegraded) ==
    L2FLOW_CERTIFIED_ORDER_EVENT_STATE_DEGRADED_V1);

[[nodiscard]] ipc::RealtimeCertifiedExpectedSessionV1 ExpectedSession(
    const l2flow_certified_order_event_expected_session_v1& source)
    noexcept {
    ipc::RealtimeCertifiedExpectedSessionV1 result{};
    std::memcpy(
        result.run_id.data(), source.run_id, result.run_id.size());
    result.session_epoch = source.session_epoch;
    result.trade_date = source.trade_date;
    return result;
}

void CopyStatus(
    const ipc::CertifiedOrderEventStatusSnapshotV1& source,
    l2flow_certified_order_event_status_v1* output) noexcept {
    *output = {};
    output->status_schema_version = kStatusSchemaVersion;
    output->status_bytes = sizeof(*output);
    output->coverage_flags = source.coverage_flags;
    output->certified_state =
        static_cast<std::uint32_t>(source.tick.state);
    output->tick_publish_tag = source.tick.publish_tag;
    output->tick_heartbeat_monotonic_ns =
        source.tick.heartbeat_monotonic_ns;
    output->tick_canonical_apply_frontier =
        source.tick.canonical_apply_frontier;
    output->correction_epoch = source.tick.correction_epoch;
    output->observed_native_message_count =
        source.tick.observed_native_message_count;
    output->certified_tick_count =
        source.tick.certified_tick_count;
    output->exact_duplicate_message_count =
        source.tick.exact_duplicate_message_count;
    output->gap_opened_count = source.tick.gap_opened_count;
    output->gap_recovered_count = source.tick.gap_recovered_count;
    output->conflicting_duplicate_count =
        source.tick.conflicting_duplicate_count;
    output->resource_exhaustion_count =
        source.tick.resource_exhaustion_count;
    output->pending_token_count = source.tick.pending_token_count;
    output->event_publish_tag = source.event_publish_tag;
    output->event_heartbeat_monotonic_ns =
        source.event_heartbeat_monotonic_ns;
    output->event_canonical_apply_frontier =
        source.event_canonical_apply_frontier;
    output->event_published_sequence =
        source.event_published_sequence;
    output->event_generation = source.event_generation;
    output->shanghai_order_state_count =
        source.shanghai_order_state_count;
    output->shenzhen_order_state_count =
        source.shenzhen_order_state_count;
    output->committed_mapping_bytes =
        source.committed_mapping_bytes;
    output->coherent_canonical_apply_frontier =
        source.coherent_canonical_apply_frontier;
    output->channel_state_count = source.tick.channel_state_count;
    output->gap_open_channel_count =
        source.tick.gap_open_channel_count;
    output->catching_up_channel_count =
        source.tick.catching_up_channel_count;
    output->frozen_channel_count =
        source.tick.frozen_channel_count;
}

[[nodiscard]] int ReadStatus(
    const l2flow_certified_order_event_reader_v1* reader,
    l2flow_certified_order_event_status_v1* output) noexcept {
    if (reader == nullptr || reader->reader == nullptr ||
        output == nullptr) {
        return L2FLOW_CERTIFIED_ORDER_EVENT_READ_OUT_OF_RANGE_V1;
    }
    ipc::CertifiedOrderEventStatusSnapshotV1 status{};
    const auto result = reader->reader->ReadStatus(&status);
    if (result == ipc::CertifiedOrderEventReadResultV1::kOk) {
        CopyStatus(status, output);
    } else {
        *output = {};
    }
    return static_cast<int>(result);
}

}  // namespace

extern "C" int l2flow_certified_order_event_reader_open_v1(
    const char* absolute_control_socket_path,
    const l2flow_certified_order_event_expected_session_v1*
        expected_session,
    std::uint32_t coverage_requirement,
    std::uint32_t timeout_ms,
    l2flow_certified_order_event_reader_v1** output,
    int* system_error_number) {
    if (system_error_number != nullptr) {
        *system_error_number = 0;
    }
    if (output == nullptr) {
        return L2FLOW_CERTIFIED_ORDER_EVENT_OPEN_NULL_OUTPUT_V1;
    }
    *output = nullptr;
    if (absolute_control_socket_path == nullptr ||
        expected_session == nullptr || timeout_ms == 0U ||
        expected_session->reserved != 0U ||
        coverage_requirement >
            L2FLOW_CERTIFIED_ORDER_EVENT_REQUIRE_ANY_EXPLICIT_V1) {
        return L2FLOW_CERTIFIED_ORDER_EVENT_OPEN_INVALID_ARGUMENT_V1;
    }
    try {
        ipc::RealtimeCertifiedReaderOpenOptionsV1 options{};
        options.control_socket_path = absolute_control_socket_path;
        options.expected_session = ExpectedSession(*expected_session);
        options.timeout = std::chrono::milliseconds(timeout_ms);
        std::unique_ptr<ipc::CertifiedOrderEventReaderV1> native;
        const auto error = ipc::CertifiedOrderEventReaderV1::Open(
            std::move(options),
            static_cast<
                ipc::CertifiedOrderEventCoverageRequirementV1>(
                coverage_requirement),
            &native,
            system_error_number);
        if (error !=
                ipc::CertifiedOrderEventReaderOpenErrorV1::kNone ||
            native == nullptr) {
            return static_cast<int>(error);
        }
        std::unique_ptr<l2flow_certified_order_event_reader_v1> wrapper(
            new (std::nothrow)
                l2flow_certified_order_event_reader_v1{});
        if (wrapper == nullptr) {
            return L2FLOW_CERTIFIED_ORDER_EVENT_OPEN_RESOURCE_EXHAUSTED_V1;
        }
        wrapper->reader = std::move(native);
        *output = wrapper.release();
        return L2FLOW_CERTIFIED_ORDER_EVENT_OPEN_OK_V1;
    } catch (...) {
        return L2FLOW_CERTIFIED_ORDER_EVENT_OPEN_UNEXPECTED_FAILURE_V1;
    }
}

extern "C" void l2flow_certified_order_event_reader_close_v1(
    l2flow_certified_order_event_reader_v1* reader) {
    delete reader;
}

extern "C" int l2flow_certified_order_event_reader_session_v1(
    const l2flow_certified_order_event_reader_v1* reader,
    l2flow_certified_order_event_session_v1* output) {
    if (reader == nullptr || reader->reader == nullptr ||
        output == nullptr) {
        return L2FLOW_CERTIFIED_ORDER_EVENT_READ_OUT_OF_RANGE_V1;
    }
    *output = {};
    const auto& session = reader->reader->session();
    std::memcpy(output->run_id, session.run_id.data(), sizeof(output->run_id));
    output->session_epoch = session.session_epoch;
    output->trade_date = session.trade_date;
    output->event_capacity = reader->reader->event_capacity();
    l2flow_certified_order_event_status_v1 status{};
    const int result = ReadStatus(reader, &status);
    if (result != L2FLOW_CERTIFIED_ORDER_EVENT_READ_OK_V1) {
        *output = {};
        return result;
    }
    output->coverage_flags = status.coverage_flags;
    output->coverage_start_unix_ns =
        reader->reader->coverage_start_unix_ns();
    return L2FLOW_CERTIFIED_ORDER_EVENT_READ_OK_V1;
}

extern "C" int l2flow_certified_order_event_reader_status_v1(
    const l2flow_certified_order_event_reader_v1* reader,
    l2flow_certified_order_event_status_v1* output) {
    return ReadStatus(reader, output);
}

extern "C" int l2flow_certified_order_event_reader_event_v1(
    const l2flow_certified_order_event_reader_v1* reader,
    std::uint64_t derived_event_sequence,
    l2flow_certified_order_event_envelope_v1* output) {
    if (reader == nullptr || reader->reader == nullptr ||
        output == nullptr) {
        return L2FLOW_CERTIFIED_ORDER_EVENT_READ_OUT_OF_RANGE_V1;
    }
    *output = {};
    auto* native = reinterpret_cast<
        ipc::CertifiedOrderEventEnvelopeV1*>(output);
    return static_cast<int>(reader->reader->ReadEvent(
        derived_event_sequence, native));
}

extern "C" int l2flow_certified_order_event_reader_read_v1(
    const l2flow_certified_order_event_reader_v1* reader,
    std::uint64_t expected_event_sequence,
    l2flow_certified_order_event_envelope_v1* output,
    std::size_t capacity,
    l2flow_certified_order_event_read_batch_result_v1* result) {
    if (result == nullptr) {
        return L2FLOW_CERTIFIED_ORDER_EVENT_READ_OUT_OF_RANGE_V1;
    }
    *result = {};
    result->result_schema_version = kResultSchemaVersion;
    result->result_bytes = sizeof(*result);
    result->next_event_sequence = expected_event_sequence;
    if (reader == nullptr || reader->reader == nullptr ||
        expected_event_sequence == 0U ||
        (capacity != 0U && output == nullptr)) {
        return L2FLOW_CERTIFIED_ORDER_EVENT_READ_OUT_OF_RANGE_V1;
    }
    auto* native = reinterpret_cast<
        ipc::CertifiedOrderEventEnvelopeV1*>(output);
    ipc::CertifiedOrderEventReadBatchResultV1 native_result{};
    std::span<ipc::CertifiedOrderEventEnvelopeV1>
        native_output{};
    if (capacity != 0U) {
        native_output = std::span<
            ipc::CertifiedOrderEventEnvelopeV1>(native, capacity);
    }
    const auto read = reader->reader->Read(
        expected_event_sequence,
        native_output,
        &native_result);
    result->records_written = native_result.written;
    result->next_event_sequence =
        native_result.next_event_sequence;
    if (native_result.status.tick.publish_tag != 0U &&
        native_result.status.event_publish_tag != 0U) {
        CopyStatus(native_result.status, &result->status);
    }
    return static_cast<int>(read);
}

extern "C" const char*
l2flow_certified_order_event_open_error_name_v1(int error) {
    if (error < 0 ||
        error >
            L2FLOW_CERTIFIED_ORDER_EVENT_OPEN_UNEXPECTED_FAILURE_V1) {
        return "unknown";
    }
    return ipc::CertifiedOrderEventReaderOpenErrorNameV1(
               static_cast<
                   ipc::CertifiedOrderEventReaderOpenErrorV1>(error))
        .data();
}

extern "C" const char*
l2flow_certified_order_event_read_result_name_v1(int result) {
    if (result < 0 ||
        result >
            L2FLOW_CERTIFIED_ORDER_EVENT_READ_PRODUCER_FAILED_V1) {
        return "unknown";
    }
    return ipc::CertifiedOrderEventReadResultNameV1(
               static_cast<ipc::CertifiedOrderEventReadResultV1>(result))
        .data();
}
