#include "l2flow/ingress/raw_emergency_writer_ack.h"
#include "l2flow/ingress/raw_v1.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <string_view>
#include <type_traits>

namespace ingress = l2flow::ingress;

namespace {

struct TestContext final {
    void Expect(bool condition, std::string_view description) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << description << '\n';
        }
    }

    int failures = 0;
};

template <std::size_t Size>
std::array<std::byte, Size> Pattern(std::uint8_t seed) {
    std::array<std::byte, Size> result{};
    for (std::size_t index = 0U; index < Size; ++index) {
        result[index] = static_cast<std::byte>(
            static_cast<std::uint8_t>(seed + index));
    }
    return result;
}

ingress::RawEmergencyWriterAckFactsV1
MakeExactQueuedFacts() {
    ingress::RawRecordLayoutV1 layout{};
    if (ingress::ComputeRawRecordLayoutV1(
            0U, &layout) !=
        ingress::RawV1Error::kNone) {
        throw std::runtime_error(
            "cannot derive Raw record fixture");
    }

    ingress::RawEmergencyWriterAckFactsV1 facts;
    facts.started_runtime.source_stream_id = 1U;
    facts.started_runtime.capture_date = 20260719U;
    facts.started_runtime.stream_day_id =
        Pattern<16U>(0x10U);
    facts.started_runtime.writer_instance =
        Pattern<16U>(0x30U);
    facts.started_runtime.append = {
        .segment_sequence = 1U,
        .global_wal_pos =
            ingress::kRawV1SegmentHeaderBytes,
        .ingress_sequence = 10U,
        .segment_offset =
            ingress::kRawV1SegmentHeaderBytes,
    };
    facts.started_runtime.durable =
        facts.started_runtime.append;
    facts.started_runtime.current_segment_sequence = 1U;

    facts.writer.writer_instance =
        facts.started_runtime.writer_instance;
    facts.writer.stream_day_id =
        facts.started_runtime.stream_day_id;
    facts.writer.source_stream_id =
        facts.started_runtime.source_stream_id;
    facts.writer.capture_date =
        facts.started_runtime.capture_date;
    facts.writer.segment_sequence = 1U;
    facts.writer.segment_base_wal_pos = 0U;
    facts.writer.first_ingress_sequence = 1U;

    facts.callback.captured_records = 3U;
    facts.callback.captured_vendor_bytes =
        3U * ingress::kVendorMessageHeadBytes;
    facts.callback.captured_framed_wal_bytes =
        3U * layout.record_size;
    facts.callback.captured_ingress_sequence = 13U;
    facts.capture.append.records = 1U;
    facts.capture.append.vendor_bytes =
        ingress::kVendorMessageHeadBytes;
    facts.capture.append.framed_wal_bytes =
        layout.record_size;
    facts.capture.append.last_ingress_sequence = 11U;
    facts.capture.durable.last_ingress_sequence = 10U;
    facts.capture.startup_complete = true;
    facts.capture.startup_succeeded = true;
    facts.capture.emergency_pause_requested = true;
    facts.capture.emergency_paused = true;

    facts.wal.initialized = true;
    facts.wal.append = {
        .global_wal_pos =
            ingress::kRawV1SegmentHeaderBytes +
            layout.record_size,
        .ingress_sequence = 11U,
        .segment_offset =
            ingress::kRawV1SegmentHeaderBytes +
            layout.record_size,
    };
    facts.wal.durable = {
        .global_wal_pos =
            ingress::kRawV1SegmentHeaderBytes,
        .ingress_sequence = 10U,
        .segment_offset =
            ingress::kRawV1SegmentHeaderBytes,
    };
    facts.queued_record_count = 2U;
    facts.queued_framed_wal_bytes =
        2U * layout.record_size;
    facts.ring_consumed_position = 100U;
    facts.ring_published_position = 200U;
    facts.ring_used_bytes = 100U;
    facts.sdk_shutdown_returned = true;
    facts.callback_quiesced = true;
    facts.regular_writer_mutation_stopped = true;
    facts.same_process_ring_suffix_retained = true;
    return facts;
}

}  // namespace

int main() {
    static_assert(
        !std::is_default_constructible_v<
            ingress::RawEmergencyWriterAckV1>);
    static_assert(
        !std::is_copy_constructible_v<
            ingress::RawEmergencyWriterAckV1>);
    static_assert(
        !std::is_move_constructible_v<
            ingress::RawEmergencyWriterAckV1>);

    TestContext test;
    const ingress::RawEmergencyWriterAckFactsV1 exact =
        MakeExactQueuedFacts();
    test.Expect(
        exact.exact(),
        "exact queued suffix facts validate");

    auto changed = exact;
    changed.callback.callback_inflight = true;
    test.Expect(
        !changed.exact(),
        "inflight callback cannot be acknowledged");
    changed = exact;
    changed.capture.emergency_paused = false;
    test.Expect(
        !changed.exact(),
        "writer without pause barrier cannot be acknowledged");
    changed = exact;
    changed.queued_framed_wal_bytes += 8U;
    test.Expect(
        !changed.exact(),
        "queued framed-WAL mismatch is rejected");
    changed = exact;
    changed.writer.writer_instance[0U] ^=
        std::byte{0x7fU};
    test.Expect(
        !changed.exact(),
        "foreign writer instance is rejected");
    changed = exact;
    changed.same_process_ring_suffix_retained = false;
    test.Expect(
        !changed.exact(),
        "ACK cannot claim a volatile suffix without same-process ownership");

    if (test.failures != 0) {
        std::cerr << test.failures
                  << " Raw emergency writer ACK test(s) failed\n";
        return 1;
    }
    std::cout
        << "Phase 2 Raw emergency writer ACK tests passed\n";
    return 0;
}
