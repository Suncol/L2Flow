#include "l2flow/ipc/order_event_delta_ring_c_v1.h"

#include "l2flow/ipc/order_event_delta_ring_v1.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <limits>
#include <memory>
#include <new>
#include <span>
#include <string_view>
#include <type_traits>
#include <utility>

namespace {

namespace ipc = l2flow::ipc;

static_assert(sizeof(std::size_t) <= sizeof(std::uint64_t));
static_assert(sizeof(ipc::OrderEventDeltaPayloadV1) == 320U);
static_assert(
    std::is_same_v<
        ipc::OrderEventDeltaPayloadV1,
        l2flow_instrument_derived_event_row_v1>);

[[nodiscard]] bool AllZero(
    const std::uint8_t* bytes,
    std::size_t count) noexcept {
    return bytes != nullptr &&
           std::all_of(
               bytes,
               bytes + count,
               [](std::uint8_t value) { return value == 0U; });
}

[[nodiscard]] bool CanonicalSession(
    const l2flow_order_event_delta_session_v1& session) noexcept {
    return session.reserved0 == 0U &&
           AllZero(session.reserved, sizeof(session.reserved)) &&
           session.session_epoch != 0U && session.trade_date != 0U &&
           session.ring_capacity != 0U &&
           session.total_mapping_bytes != 0U &&
           std::any_of(
               std::begin(session.run_id),
               std::end(session.run_id),
               [](std::uint8_t value) { return value != 0U; });
}

[[nodiscard]] ipc::OrderEventDeltaSessionV1 FromCSession(
    const l2flow_order_event_delta_session_v1& source) noexcept {
    ipc::OrderEventDeltaSessionV1 result{};
    std::memcpy(
        result.run_id.data(),
        source.run_id,
        result.run_id.size());
    result.session_epoch = source.session_epoch;
    result.trade_date = source.trade_date;
    result.ring_capacity = source.ring_capacity;
    result.total_mapping_bytes = source.total_mapping_bytes;
    return result;
}

void ToCSession(
    const ipc::OrderEventDeltaSessionV1& source,
    l2flow_order_event_delta_session_v1* output) noexcept {
    *output = {};
    std::memcpy(
        output->run_id,
        source.run_id.data(),
        source.run_id.size());
    output->session_epoch = source.session_epoch;
    output->trade_date = source.trade_date;
    output->ring_capacity = source.ring_capacity;
    output->total_mapping_bytes = source.total_mapping_bytes;
}

void InitializeResult(
    std::uint64_t next_sequence,
    l2flow_order_event_delta_read_result_v1* output) noexcept {
    *output = {};
    output->result_schema_version = 1U;
    output->result_bytes = sizeof(*output);
    output->next_sequence = next_sequence;
}

void ToCResult(
    const ipc::OrderEventDeltaReadResultV1& source,
    l2flow_order_event_delta_read_result_v1* output) noexcept {
    const std::uint32_t schema = output->result_schema_version;
    const std::uint32_t bytes = output->result_bytes;
    *output = {};
    output->result_schema_version = schema;
    output->result_bytes = bytes;
    output->records_written =
        static_cast<std::uint64_t>(source.written);
    output->next_sequence = source.next_sequence;
    output->observed_sequence = source.observed_sequence;
    output->published_event_sequence =
        source.published_event_sequence;
    output->consumed_source_tick_sequence =
        source.consumed_source_tick_sequence;
    output->heartbeat_monotonic_ns =
        source.heartbeat_monotonic_ns;
    output->producer_state = source.producer_state;
    output->header_flags = source.header_flags;
}

[[nodiscard]] int FromOpenError(
    ipc::OrderEventDeltaReaderOpenErrorV1 error) noexcept {
    switch (error) {
        case ipc::OrderEventDeltaReaderOpenErrorV1::kNone:
            return L2FLOW_ORDER_EVENT_DELTA_OK_V1;
        case ipc::OrderEventDeltaReaderOpenErrorV1::kNullOutput:
            return L2FLOW_ORDER_EVENT_DELTA_NULL_OUTPUT_V1;
        case ipc::OrderEventDeltaReaderOpenErrorV1::
            kInvalidArgument:
            return L2FLOW_ORDER_EVENT_DELTA_INVALID_ARGUMENT_V1;
        case ipc::OrderEventDeltaReaderOpenErrorV1::
            kDescriptorInvalid:
            return L2FLOW_ORDER_EVENT_DELTA_DESCRIPTOR_INVALID_V1;
        case ipc::OrderEventDeltaReaderOpenErrorV1::
            kDescriptorNotReadOnly:
            return
                L2FLOW_ORDER_EVENT_DELTA_DESCRIPTOR_NOT_READ_ONLY_V1;
        case ipc::OrderEventDeltaReaderOpenErrorV1::kMappingFailed:
            return L2FLOW_ORDER_EVENT_DELTA_MAPPING_FAILED_V1;
        case ipc::OrderEventDeltaReaderOpenErrorV1::kLayoutInvalid:
            return L2FLOW_ORDER_EVENT_DELTA_LAYOUT_INVALID_V1;
        case ipc::OrderEventDeltaReaderOpenErrorV1::
            kSessionMismatch:
            return L2FLOW_ORDER_EVENT_DELTA_SESSION_MISMATCH_V1;
        case ipc::OrderEventDeltaReaderOpenErrorV1::kUnavailable:
            return L2FLOW_ORDER_EVENT_DELTA_UNAVAILABLE_V1;
        case ipc::OrderEventDeltaReaderOpenErrorV1::
            kResourceExhausted:
            return L2FLOW_ORDER_EVENT_DELTA_RESOURCE_EXHAUSTED_V1;
        case ipc::OrderEventDeltaReaderOpenErrorV1::
            kUnexpectedFailure:
            return
                L2FLOW_ORDER_EVENT_DELTA_UNEXPECTED_FAILURE_V1;
    }
    return L2FLOW_ORDER_EVENT_DELTA_UNEXPECTED_FAILURE_V1;
}

[[nodiscard]] int FromReadError(
    ipc::OrderEventDeltaReadErrorV1 error) noexcept {
    switch (error) {
        case ipc::OrderEventDeltaReadErrorV1::kNone:
            return L2FLOW_ORDER_EVENT_DELTA_OK_V1;
        case ipc::OrderEventDeltaReadErrorV1::kInvalidArgument:
            return L2FLOW_ORDER_EVENT_DELTA_INVALID_ARGUMENT_V1;
        case ipc::OrderEventDeltaReadErrorV1::kUnavailable:
            return L2FLOW_ORDER_EVENT_DELTA_UNAVAILABLE_V1;
        case ipc::OrderEventDeltaReadErrorV1::kLayoutInvalid:
            return L2FLOW_ORDER_EVENT_DELTA_LAYOUT_INVALID_V1;
        case ipc::OrderEventDeltaReadErrorV1::kInconsistentRead:
            return L2FLOW_ORDER_EVENT_DELTA_INCONSISTENT_READ_V1;
        case ipc::OrderEventDeltaReadErrorV1::kOverrun:
            return L2FLOW_ORDER_EVENT_DELTA_OVERRUN_V1;
    }
    return L2FLOW_ORDER_EVENT_DELTA_UNEXPECTED_FAILURE_V1;
}

}  // namespace

struct l2flow_order_event_delta_reader_v1 {
    std::unique_ptr<ipc::OrderEventDeltaRingReaderV1> reader;
    std::uint64_t next_sequence = 1U;
};

extern "C" {

int l2flow_order_event_delta_reader_open_v1(
    int read_only_descriptor,
    const l2flow_order_event_delta_session_v1* expected_session,
    l2flow_order_event_delta_reader_v1** output,
    int* system_error_number) {
    if (system_error_number != nullptr) {
        *system_error_number = 0;
    }
    if (output == nullptr) {
        return L2FLOW_ORDER_EVENT_DELTA_NULL_OUTPUT_V1;
    }
    *output = nullptr;
    if (expected_session == nullptr ||
        !CanonicalSession(*expected_session)) {
        return L2FLOW_ORDER_EVENT_DELTA_INVALID_ARGUMENT_V1;
    }
    try {
        auto holder =
            std::make_unique<l2flow_order_event_delta_reader_v1>();
        const auto error = ipc::OrderEventDeltaRingReaderV1::Open(
            read_only_descriptor,
            FromCSession(*expected_session),
            &holder->reader,
            system_error_number);
        if (error !=
            ipc::OrderEventDeltaReaderOpenErrorV1::kNone) {
            return FromOpenError(error);
        }
        *output = holder.release();
        return L2FLOW_ORDER_EVENT_DELTA_OK_V1;
    } catch (const std::bad_alloc&) {
        return L2FLOW_ORDER_EVENT_DELTA_RESOURCE_EXHAUSTED_V1;
    } catch (...) {
        return L2FLOW_ORDER_EVENT_DELTA_UNEXPECTED_FAILURE_V1;
    }
}

void l2flow_order_event_delta_reader_close_v1(
    l2flow_order_event_delta_reader_v1* reader) {
    delete reader;
}

int l2flow_order_event_delta_reader_read_v1(
    l2flow_order_event_delta_reader_v1* reader,
    l2flow_instrument_derived_event_row_v1* rows,
    std::uint64_t capacity,
    l2flow_order_event_delta_read_result_v1* result) {
    if (result == nullptr) {
        return L2FLOW_ORDER_EVENT_DELTA_NULL_OUTPUT_V1;
    }
    InitializeResult(
        reader == nullptr ? 0U : reader->next_sequence, result);
    if (reader == nullptr || reader->reader == nullptr ||
        (capacity != 0U && rows == nullptr) ||
        capacity >
            static_cast<std::uint64_t>(
                std::numeric_limits<std::size_t>::max())) {
        return L2FLOW_ORDER_EVENT_DELTA_INVALID_ARGUMENT_V1;
    }
    try {
        ipc::OrderEventDeltaReadResultV1 native_result{};
        const auto error = reader->reader->Read(
            reader->next_sequence,
            std::span<ipc::OrderEventDeltaPayloadV1>(
                rows, static_cast<std::size_t>(capacity)),
            &native_result);
        ToCResult(native_result, result);
        if (error == ipc::OrderEventDeltaReadErrorV1::kNone) {
            reader->next_sequence = native_result.next_sequence;
        }
        return FromReadError(error);
    } catch (const std::bad_alloc&) {
        return L2FLOW_ORDER_EVENT_DELTA_RESOURCE_EXHAUSTED_V1;
    } catch (...) {
        return L2FLOW_ORDER_EVENT_DELTA_UNEXPECTED_FAILURE_V1;
    }
}

int l2flow_order_event_delta_reader_session_v1(
    const l2flow_order_event_delta_reader_v1* reader,
    l2flow_order_event_delta_session_v1* output) {
    if (output == nullptr) {
        return L2FLOW_ORDER_EVENT_DELTA_NULL_OUTPUT_V1;
    }
    *output = {};
    if (reader == nullptr || reader->reader == nullptr) {
        return L2FLOW_ORDER_EVENT_DELTA_INVALID_ARGUMENT_V1;
    }
    ToCSession(reader->reader->session(), output);
    return L2FLOW_ORDER_EVENT_DELTA_OK_V1;
}

int l2flow_order_event_delta_reader_state_v1(
    const l2flow_order_event_delta_reader_v1* reader,
    std::uint32_t* output) {
    if (output == nullptr) {
        return L2FLOW_ORDER_EVENT_DELTA_NULL_OUTPUT_V1;
    }
    *output = 0U;
    if (reader == nullptr || reader->reader == nullptr) {
        return L2FLOW_ORDER_EVENT_DELTA_INVALID_ARGUMENT_V1;
    }
    *output = static_cast<std::uint32_t>(
        reader->reader->state());
    return L2FLOW_ORDER_EVENT_DELTA_OK_V1;
}

const char* l2flow_order_event_delta_error_name_v1(int error) {
    switch (error) {
        case L2FLOW_ORDER_EVENT_DELTA_OK_V1:
            return "none";
        case L2FLOW_ORDER_EVENT_DELTA_NULL_OUTPUT_V1:
            return "null_output";
        case L2FLOW_ORDER_EVENT_DELTA_INVALID_ARGUMENT_V1:
            return "invalid_argument";
        case L2FLOW_ORDER_EVENT_DELTA_DESCRIPTOR_INVALID_V1:
            return "descriptor_invalid";
        case L2FLOW_ORDER_EVENT_DELTA_DESCRIPTOR_NOT_READ_ONLY_V1:
            return "descriptor_not_read_only";
        case L2FLOW_ORDER_EVENT_DELTA_MAPPING_FAILED_V1:
            return "mapping_failed";
        case L2FLOW_ORDER_EVENT_DELTA_LAYOUT_INVALID_V1:
            return "layout_invalid";
        case L2FLOW_ORDER_EVENT_DELTA_SESSION_MISMATCH_V1:
            return "session_mismatch";
        case L2FLOW_ORDER_EVENT_DELTA_UNAVAILABLE_V1:
            return "unavailable";
        case L2FLOW_ORDER_EVENT_DELTA_INCONSISTENT_READ_V1:
            return "inconsistent_read";
        case L2FLOW_ORDER_EVENT_DELTA_OVERRUN_V1:
            return "overrun";
        case L2FLOW_ORDER_EVENT_DELTA_RESOURCE_EXHAUSTED_V1:
            return "resource_exhausted";
        case L2FLOW_ORDER_EVENT_DELTA_UNEXPECTED_FAILURE_V1:
            return "unexpected_failure";
        default:
            return "unknown";
    }
}

}  // extern "C"
