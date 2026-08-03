#include "l2flow/ipc/realtime_certified_tick_history_reader_c_v1.h"

#include "l2flow/ipc/realtime_certified_tick_history_reader_v1.h"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <new>
#include <span>
#include <utility>

namespace ipc = l2flow::ipc;

struct l2flow_certified_tick_history_reader_v1 final {
    std::unique_ptr<ipc::RealtimeCertifiedTickHistoryReaderV1> reader;
};

namespace {

constexpr std::uint32_t kStatusSchemaVersion = 1U;
constexpr std::uint32_t kResultSchemaVersion = 1U;

static_assert(
    sizeof(l2flow_certified_tick_history_expected_session_v1) == 32U);
static_assert(sizeof(l2flow_certified_tick_history_session_v1) == 64U);
static_assert(sizeof(l2flow_certified_tick_history_status_v1) == 96U);
static_assert(sizeof(l2flow_certified_tick_history_slot_v1) == 512U);
static_assert(
    sizeof(l2flow_certified_tick_history_read_batch_result_v1) == 160U);
static_assert(
    sizeof(l2flow_certified_tick_history_slot_v1) ==
    sizeof(ipc::RealtimeCertifiedTickSlotV1));
static_assert(
    offsetof(l2flow_certified_tick_history_slot_v1, publish_tag) ==
    offsetof(ipc::RealtimeCertifiedTickSlotV1, publish_tag));
static_assert(
    offsetof(l2flow_certified_tick_history_slot_v1, reserved0) ==
    offsetof(ipc::RealtimeCertifiedTickSlotV1, reserved0));
static_assert(
    offsetof(l2flow_certified_tick_history_slot_v1, payload_words) ==
    offsetof(ipc::RealtimeCertifiedTickSlotV1, payload_words));
static_assert(
    offsetof(l2flow_certified_tick_history_slot_v1, payload_words) ==
    L2FLOW_CERTIFIED_TICK_HISTORY_PAYLOAD_OFFSET_V1);
static_assert(
    sizeof(ipc::RealtimeCertifiedTickEnvelopeV1) ==
    L2FLOW_CERTIFIED_TICK_HISTORY_ENVELOPE_BYTES_V1);
static_assert(
    static_cast<int>(
        ipc::RealtimeCertifiedReaderOpenErrorV1::kUnexpectedFailure) ==
    L2FLOW_CERTIFIED_TICK_HISTORY_OPEN_UNEXPECTED_FAILURE_V1);
static_assert(
    static_cast<int>(ipc::CertifiedTickJournalReadResultV1::kCorrupt) ==
    L2FLOW_CERTIFIED_TICK_HISTORY_READ_CORRUPT_V1);
static_assert(
    static_cast<std::uint32_t>(
        ipc::CertifiedTickJournalStateV1::kFailed) ==
    L2FLOW_CERTIFIED_TICK_HISTORY_STATE_FAILED_V1);
static_assert(
    static_cast<std::uint32_t>(
        ipc::CertifiedTickJournalCoverageKindV1::kFromOpen) ==
    L2FLOW_CERTIFIED_TICK_HISTORY_COVERAGE_FROM_OPEN_V1);
static_assert(
    static_cast<std::uint32_t>(
        ipc::CertifiedTickJournalAppendErrorV1::kSourceReadFailed) ==
    L2FLOW_CERTIFIED_TICK_HISTORY_FAILURE_SOURCE_READ_V1);

[[nodiscard]] ipc::RealtimeCertifiedExpectedSessionV1 ExpectedSession(
    const l2flow_certified_tick_history_expected_session_v1& source)
    noexcept {
    ipc::RealtimeCertifiedExpectedSessionV1 result{};
    std::memcpy(
        result.run_id.data(), source.run_id, result.run_id.size());
    result.session_epoch = source.session_epoch;
    result.trade_date = source.trade_date;
    return result;
}

void CopyStatus(
    const ipc::CertifiedTickJournalStatusV1& source,
    l2flow_certified_tick_history_status_v1* output) noexcept {
    *output = {};
    output->status_schema_version = kStatusSchemaVersion;
    output->status_bytes = sizeof(*output);
    output->publish_tag = source.publish_tag;
    output->heartbeat_monotonic_ns =
        source.heartbeat_monotonic_ns;
    output->canonical_apply_frontier =
        source.canonical_apply_frontier;
    output->generation = source.generation;
    output->published_tick_count = source.published_tick_count;
    output->committed_mapping_bytes =
        source.committed_mapping_bytes;
    output->state = static_cast<std::uint32_t>(source.state);
    output->failure = static_cast<std::uint32_t>(source.failure);
}

}  // namespace

extern "C" int l2flow_certified_tick_history_reader_open_v1(
    const char* absolute_control_socket_path,
    const l2flow_certified_tick_history_expected_session_v1*
        expected_session,
    std::uint32_t timeout_ms,
    l2flow_certified_tick_history_reader_v1** output,
    int* system_error_number) {
    if (system_error_number != nullptr) {
        *system_error_number = 0;
    }
    if (output == nullptr) {
        return L2FLOW_CERTIFIED_TICK_HISTORY_OPEN_NULL_OUTPUT_V1;
    }
    *output = nullptr;
    if (absolute_control_socket_path == nullptr ||
        expected_session == nullptr ||
        expected_session->reserved != 0U) {
        return L2FLOW_CERTIFIED_TICK_HISTORY_OPEN_INVALID_ARGUMENT_V1;
    }
    try {
        ipc::RealtimeCertifiedReaderOpenOptionsV1 options{};
        options.control_socket_path = absolute_control_socket_path;
        options.expected_session = ExpectedSession(*expected_session);
        options.timeout = std::chrono::milliseconds(timeout_ms);
        std::unique_ptr<ipc::RealtimeCertifiedTickHistoryReaderV1> native;
        const auto error =
            ipc::RealtimeCertifiedTickHistoryReaderV1::Open(
                std::move(options), &native, system_error_number);
        if (error != ipc::RealtimeCertifiedReaderOpenErrorV1::kNone ||
            native == nullptr) {
            return static_cast<int>(error);
        }
        std::unique_ptr<l2flow_certified_tick_history_reader_v1> wrapper(
            new (std::nothrow)
                l2flow_certified_tick_history_reader_v1{});
        if (wrapper == nullptr) {
            return L2FLOW_CERTIFIED_TICK_HISTORY_OPEN_RESOURCE_EXHAUSTED_V1;
        }
        wrapper->reader = std::move(native);
        *output = wrapper.release();
        return L2FLOW_CERTIFIED_TICK_HISTORY_OPEN_OK_V1;
    } catch (...) {
        return L2FLOW_CERTIFIED_TICK_HISTORY_OPEN_UNEXPECTED_FAILURE_V1;
    }
}

extern "C" void l2flow_certified_tick_history_reader_close_v1(
    l2flow_certified_tick_history_reader_v1* reader) {
    delete reader;
}

extern "C" int l2flow_certified_tick_history_reader_session_v1(
    const l2flow_certified_tick_history_reader_v1* reader,
    l2flow_certified_tick_history_session_v1* output) {
    if (reader == nullptr || reader->reader == nullptr ||
        output == nullptr) {
        return L2FLOW_CERTIFIED_TICK_HISTORY_READ_INVALID_ARGUMENT_V1;
    }
    *output = {};
    const auto& session = reader->reader->session();
    std::memcpy(
        output->run_id, session.run_id.data(), sizeof(output->run_id));
    output->session_epoch = session.session_epoch;
    output->total_mapping_bytes = session.total_mapping_bytes;
    output->tick_capacity = session.tick_capacity;
    output->trade_date = session.trade_date;
    output->slot_bytes = L2FLOW_CERTIFIED_TICK_HISTORY_SLOT_BYTES_V1;
    output->coverage_kind =
        static_cast<std::uint32_t>(session.coverage_kind);
    output->coverage_start_unix_ns =
        session.coverage_start_unix_ns;
    return L2FLOW_CERTIFIED_TICK_HISTORY_READ_OK_V1;
}

extern "C" int l2flow_certified_tick_history_reader_status_v1(
    const l2flow_certified_tick_history_reader_v1* reader,
    l2flow_certified_tick_history_status_v1* output) {
    if (reader == nullptr || reader->reader == nullptr ||
        output == nullptr) {
        return L2FLOW_CERTIFIED_TICK_HISTORY_READ_INVALID_ARGUMENT_V1;
    }
    ipc::CertifiedTickJournalStatusV1 native{};
    const auto result = reader->reader->ReadStatus(&native);
    if (result == ipc::CertifiedTickJournalReadResultV1::kOk) {
        CopyStatus(native, output);
    } else {
        *output = {};
    }
    return static_cast<int>(result);
}

extern "C" int l2flow_certified_tick_history_reader_read_v1(
    const l2flow_certified_tick_history_reader_v1* reader,
    std::uint64_t first_canonical_apply_sequence,
    l2flow_certified_tick_history_slot_v1* output,
    std::size_t capacity,
    l2flow_certified_tick_history_read_batch_result_v1* result) {
    if (result == nullptr) {
        return L2FLOW_CERTIFIED_TICK_HISTORY_READ_INVALID_ARGUMENT_V1;
    }
    *result = {};
    result->result_schema_version = kResultSchemaVersion;
    result->result_bytes = sizeof(*result);
    result->next_canonical_apply_sequence =
        first_canonical_apply_sequence;
    if (reader == nullptr || reader->reader == nullptr ||
        first_canonical_apply_sequence == 0U || output == nullptr ||
        capacity == 0U ||
        capacity > std::numeric_limits<std::size_t>::max() /
                       L2FLOW_CERTIFIED_TICK_HISTORY_SLOT_BYTES_V1) {
        return L2FLOW_CERTIFIED_TICK_HISTORY_READ_INVALID_ARGUMENT_V1;
    }
    auto* bytes = reinterpret_cast<std::byte*>(output);
    const std::size_t output_bytes =
        capacity * L2FLOW_CERTIFIED_TICK_HISTORY_SLOT_BYTES_V1;
    ipc::CertifiedTickJournalReadBatchV1 native_result{};
    const auto read = reader->reader->ReadSlotBytes(
        first_canonical_apply_sequence,
        std::span<std::byte>(bytes, output_bytes),
        &native_result);
    result->records_written = native_result.written;
    result->next_canonical_apply_sequence =
        native_result.next_canonical_apply_sequence;
    if (native_result.status.publish_tag != 0U) {
        CopyStatus(native_result.status, &result->status);
    }
    return static_cast<int>(read);
}

extern "C" const char*
l2flow_certified_tick_history_open_error_name_v1(int error) {
    if (error < 0 ||
        error >
            L2FLOW_CERTIFIED_TICK_HISTORY_OPEN_UNEXPECTED_FAILURE_V1) {
        return "UNKNOWN";
    }
    return ipc::RealtimeCertifiedReaderOpenErrorNameV1(
               static_cast<ipc::RealtimeCertifiedReaderOpenErrorV1>(
                   error))
        .data();
}

extern "C" const char*
l2flow_certified_tick_history_read_result_name_v1(int result) {
    if (result < 0 ||
        result > L2FLOW_CERTIFIED_TICK_HISTORY_READ_CORRUPT_V1) {
        return "unknown";
    }
    return ipc::CertifiedTickJournalReadResultNameV1(
               static_cast<ipc::CertifiedTickJournalReadResultV1>(
                   result))
        .data();
}
