#include "l2flow/ipc/order_event_delta_ring_c_v1.h"

#include "l2flow/ipc/order_event_delta_ring_v1.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>
#include <span>
#include <string_view>

#include <fcntl.h>
#include <unistd.h>

namespace {

namespace ipc = l2flow::ipc;

bool Expect(bool condition, std::string_view message) {
    if (!condition) {
        std::cerr << "FAILED: " << message << '\n';
    }
    return condition;
}

ipc::OrderEventDeltaRingConfigV1 Config(
    std::uint64_t capacity,
    std::uint8_t seed) {
    ipc::OrderEventDeltaRingConfigV1 result{};
    for (std::size_t index = 0U; index < result.run_id.size();
         ++index) {
        result.run_id[index] = static_cast<std::byte>(
            static_cast<std::uint8_t>(
                seed + static_cast<std::uint8_t>(index)));
    }
    result.session_epoch = 91U;
    result.trade_date = 20260730U;
    result.ring_capacity = capacity;
    result.maximum_mapping_bytes = 64ULL * 1024ULL * 1024ULL;
    return result;
}

std::unique_ptr<ipc::OrderEventDeltaRingProducerV1> Producer(
    std::uint64_t capacity,
    std::uint8_t seed,
    bool* ok) {
    std::unique_ptr<ipc::OrderEventDeltaRingProducerV1> producer;
    *ok &= Expect(
        ipc::OrderEventDeltaRingProducerV1::Create(
            Config(capacity, seed), &producer) ==
                ipc::OrderEventDeltaRingCreateErrorV1::kNone &&
            producer != nullptr,
        "create producer for C ABI test");
    return producer;
}

l2flow_order_event_delta_session_v1 CSession(
    const ipc::OrderEventDeltaSessionV1& source) {
    l2flow_order_event_delta_session_v1 result{};
    std::memcpy(
        result.run_id,
        source.run_id.data(),
        source.run_id.size());
    result.session_epoch = source.session_epoch;
    result.trade_date = source.trade_date;
    result.ring_capacity = source.ring_capacity;
    result.total_mapping_bytes = source.total_mapping_bytes;
    return result;
}

ipc::OrderEventDeltaPayloadV1 Order(
    std::uint64_t tick_sequence,
    std::int64_t order_id) {
    ipc::OrderEventDeltaPayloadV1 result{};
    result.record_schema_version = 1U;
    result.record_bytes = sizeof(result);
    result.trade_date = 20260730U;
    result.instrument_id = 1U;
    result.channel = 7;
    result.market =
        L2FLOW_INSTRUMENT_DERIVED_EVENT_MARKET_SHANGHAI_V1;
    result.event_kind =
        L2FLOW_INSTRUMENT_DERIVED_EVENT_ORDER_REVISION_V1;
    result.order_id = order_id;
    result.revision = 1U;
    result.tick_stream_sequence = tick_sequence;
    result.native_event_sequence =
        static_cast<std::int64_t>(tick_sequence);
    result.source_sequence = tick_sequence;
    result.ingress_sequence = tick_sequence;
    result.vendor_sequence_id = tick_sequence;
    return result;
}

l2flow_order_event_delta_reader_v1* Open(
    const ipc::OrderEventDeltaRingProducerV1& producer,
    int* descriptor,
    bool* ok) {
    *descriptor = -1;
    *ok &= Expect(
        producer.DuplicateReadOnlyDescriptor(descriptor) &&
            *descriptor >= 0,
        "duplicate descriptor for C ABI");
    l2flow_order_event_delta_reader_v1* reader = nullptr;
    int system_error = -1;
    const auto session = CSession(producer.session());
    *ok &= Expect(
        l2flow_order_event_delta_reader_open_v1(
            *descriptor,
            &session,
            &reader,
            &system_error) == L2FLOW_ORDER_EVENT_DELTA_OK_V1 &&
            reader != nullptr && system_error == 0,
        "open C ABI reader");
    return reader;
}

void TestAbiAndArguments(bool* ok) {
    *ok &= Expect(
        sizeof(l2flow_order_event_delta_session_v1) == 64U &&
            sizeof(l2flow_order_event_delta_read_result_v1) == 80U &&
            sizeof(l2flow_instrument_derived_event_row_v1) == 320U,
        "C ABI fixed sizes");
    *ok &= Expect(
        l2flow_order_event_delta_reader_open_v1(
            -1, nullptr, nullptr, nullptr) ==
            L2FLOW_ORDER_EVENT_DELTA_NULL_OUTPUT_V1,
        "C open null output classification");

    auto producer = Producer(4U, 1U, ok);
    if (producer == nullptr) {
        return;
    }
    int descriptor = -1;
    *ok &= Expect(
        producer->DuplicateReadOnlyDescriptor(&descriptor),
        "descriptor for invalid session");
    auto session = CSession(producer->session());
    session.reserved[3U] = 1U;
    l2flow_order_event_delta_reader_v1* reader = nullptr;
    *ok &= Expect(
        l2flow_order_event_delta_reader_open_v1(
            descriptor, &session, &reader, nullptr) ==
                L2FLOW_ORDER_EVENT_DELTA_INVALID_ARGUMENT_V1 &&
            reader == nullptr,
        "C open rejects nonzero session reserve");
    *ok &= Expect(
        ::fcntl(descriptor, F_GETFD) >= 0,
        "C open never consumes caller descriptor");
    if (descriptor >= 0) {
        static_cast<void>(::close(descriptor));
    }
}

void TestReadAndOwnership(bool* ok) {
    auto producer = Producer(8U, 20U, ok);
    if (producer == nullptr) {
        return;
    }
    int descriptor = -1;
    l2flow_order_event_delta_reader_v1* reader =
        Open(*producer, &descriptor, ok);
    if (reader == nullptr) {
        return;
    }

    l2flow_order_event_delta_session_v1 returned_session{};
    *ok &= Expect(
        l2flow_order_event_delta_reader_session_v1(
            reader, &returned_session) ==
                L2FLOW_ORDER_EVENT_DELTA_OK_V1 &&
            returned_session.session_epoch ==
                producer->session().session_epoch &&
            returned_session.ring_capacity == 8U,
        "C reader returns exact session");

    // Open made a private duplicate; close the caller's fd before reading.
    static_cast<void>(::close(descriptor));
    descriptor = -1;

    l2flow_order_event_delta_read_result_v1 result{};
    *ok &= Expect(
        l2flow_order_event_delta_reader_read_v1(
            reader, nullptr, 0U, &result) ==
                L2FLOW_ORDER_EVENT_DELTA_OK_V1 &&
            result.result_schema_version == 1U &&
            result.result_bytes == sizeof(result) &&
            result.records_written == 0U &&
            result.next_sequence == 1U &&
            result.producer_state ==
                L2FLOW_ORDER_EVENT_DELTA_ACTIVE_V1,
        "zero-capacity C poll on empty stream");

    *ok &= Expect(
        producer->PublishSourceTick(1U, {}) ==
            ipc::OrderEventDeltaPublishErrorV1::kNone,
        "publish zero-event source tick for C poll");
    const std::array<ipc::OrderEventDeltaPayloadV1, 2U> batch{
        Order(2U, 101),
        Order(2U, 102)};
    *ok &= Expect(
        producer->PublishSourceTick(2U, batch) ==
                ipc::OrderEventDeltaPublishErrorV1::kNone &&
            producer->UpdateHeartbeat(123'456U),
        "publish two rows and heartbeat");

    std::array<l2flow_instrument_derived_event_row_v1, 1U> row{};
    *ok &= Expect(
        l2flow_order_event_delta_reader_read_v1(
            reader, row.data(), row.size(), &result) ==
                L2FLOW_ORDER_EVENT_DELTA_OK_V1 &&
            result.records_written == 1U &&
            result.next_sequence == 2U &&
            result.published_event_sequence == 2U &&
            result.consumed_source_tick_sequence == 2U &&
            result.heartbeat_monotonic_ns == 123'456U &&
            row[0U].derived_event_sequence == 1U &&
            row[0U].order_id == 101,
        "C reader returns first bounded row and metadata");
    *ok &= Expect(
        l2flow_order_event_delta_reader_read_v1(
            reader, row.data(), row.size(), &result) ==
                L2FLOW_ORDER_EVENT_DELTA_OK_V1 &&
            result.records_written == 1U &&
            result.next_sequence == 3U &&
            row[0U].derived_event_sequence == 2U &&
            row[0U].order_id == 102,
        "C reader owns and advances stateful cursor");

    std::uint32_t state = 0U;
    *ok &= Expect(
        l2flow_order_event_delta_reader_state_v1(
            reader, &state) ==
                L2FLOW_ORDER_EVENT_DELTA_OK_V1 &&
            state == L2FLOW_ORDER_EVENT_DELTA_ACTIVE_V1,
        "C reader exposes producer state");
    l2flow_order_event_delta_reader_close_v1(reader);
}

void TestOverrunAndFailure(bool* ok) {
    {
        auto producer = Producer(2U, 40U, ok);
        if (producer == nullptr) {
            return;
        }
        int descriptor = -1;
        l2flow_order_event_delta_reader_v1* reader =
            Open(*producer, &descriptor, ok);
        if (descriptor >= 0) {
            static_cast<void>(::close(descriptor));
        }
        if (reader == nullptr) {
            return;
        }
        for (std::uint64_t sequence = 1U; sequence <= 3U;
             ++sequence) {
            const auto event = Order(
                sequence,
                static_cast<std::int64_t>(sequence));
            *ok &= Expect(
                producer->PublishSourceTick(
                    sequence,
                    std::span<
                        const ipc::OrderEventDeltaPayloadV1>(
                        &event, 1U)) ==
                    ipc::OrderEventDeltaPublishErrorV1::kNone,
                "publish C overrun fixture");
        }
        std::array<l2flow_instrument_derived_event_row_v1, 2U>
            rows{};
        l2flow_order_event_delta_read_result_v1 result{};
        *ok &= Expect(
            l2flow_order_event_delta_reader_read_v1(
                reader, rows.data(), rows.size(), &result) ==
                    L2FLOW_ORDER_EVENT_DELTA_OVERRUN_V1 &&
                result.records_written == 0U &&
                result.next_sequence == 1U &&
                result.observed_sequence == 2U,
            "C reader reports overrun without cursor advance");
        *ok &= Expect(
            l2flow_order_event_delta_reader_read_v1(
                reader, rows.data(), rows.size(), &result) ==
                L2FLOW_ORDER_EVENT_DELTA_UNAVAILABLE_V1,
            "C reader remains fail-closed after overrun");
        l2flow_order_event_delta_reader_close_v1(reader);
    }
    {
        auto producer = Producer(4U, 60U, ok);
        if (producer == nullptr) {
            return;
        }
        int descriptor = -1;
        l2flow_order_event_delta_reader_v1* reader =
            Open(*producer, &descriptor, ok);
        if (descriptor >= 0) {
            static_cast<void>(::close(descriptor));
        }
        if (reader == nullptr) {
            return;
        }
        *ok &= Expect(
            producer->PublishSourceTick(2U, {}) ==
                ipc::OrderEventDeltaPublishErrorV1::
                    kSourceSequenceGap,
            "source gap fails producer for C reader");
        l2flow_order_event_delta_read_result_v1 result{};
        *ok &= Expect(
            l2flow_order_event_delta_reader_read_v1(
                reader, nullptr, 0U, &result) ==
                    L2FLOW_ORDER_EVENT_DELTA_UNAVAILABLE_V1 &&
                result.producer_state ==
                    L2FLOW_ORDER_EVENT_DELTA_FAILED_V1 &&
                (result.header_flags &
                 L2FLOW_ORDER_EVENT_DELTA_COVERAGE_LOST_V1) != 0U,
            "C read exposes failed state and coverage flag");
        l2flow_order_event_delta_reader_close_v1(reader);
    }
}

}  // namespace

int main() {
    bool ok = true;
    TestAbiAndArguments(&ok);
    TestReadAndOwnership(&ok);
    TestOverrunAndFailure(&ok);
    if (!ok) {
        return 1;
    }
    std::cout << "order event delta ring C ABI tests passed\n";
    return 0;
}
