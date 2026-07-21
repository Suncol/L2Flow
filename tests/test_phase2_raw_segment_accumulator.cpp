#include "l2flow/ingress/raw_segment_accumulator.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <span>
#include <string>
#include <vector>

namespace ingress = l2flow::ingress;

namespace {

struct TestContext final {
    void Expect(bool condition, const std::string& description) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << description << '\n';
        }
    }
    int failures = 0;
};

template <std::size_t Size>
void FillNonzero(
    std::array<std::byte, Size>* bytes,
    std::uint8_t seed) {
    for (std::size_t index = 0U;
         index < bytes->size();
         ++index) {
        (*bytes)[index] = static_cast<std::byte>(
            static_cast<std::uint8_t>(
                seed +
                static_cast<std::uint8_t>(index)));
    }
}

void StoreU16(
    std::span<std::byte> bytes,
    std::size_t offset,
    std::uint16_t value) {
    bytes[offset] =
        static_cast<std::byte>(value & 0xffU);
    bytes[offset + 1U] =
        static_cast<std::byte>((value >> 8U) & 0xffU);
}

void StoreU32(
    std::span<std::byte> bytes,
    std::size_t offset,
    std::uint32_t value) {
    for (std::size_t index = 0U;
         index < sizeof(value);
         ++index) {
        bytes[offset + index] =
            static_cast<std::byte>(
                (value >>
                 static_cast<unsigned int>(
                     index * 8U)) &
                0xffU);
    }
}

void StoreU64(
    std::span<std::byte> bytes,
    std::size_t offset,
    std::uint64_t value) {
    for (std::size_t index = 0U;
         index < sizeof(value);
         ++index) {
        bytes[offset + index] =
            static_cast<std::byte>(
                (value >>
                 static_cast<unsigned int>(
                     index * 8U)) &
                0xffU);
    }
}

ingress::RawV1VendorHead MakeVendorHead(
    std::uint32_t body_size,
    std::uint64_t sequence) {
    ingress::RawV1VendorHead head{};
    std::span<std::byte> bytes(head);
    head[0U] = static_cast<std::byte>(
        ingress::kVendorMessageHeadBytes);
    StoreU32(
        bytes,
        1U,
        static_cast<std::uint32_t>(
            ingress::kVendorMessageHeadBytes) +
            body_size);
    head[5U] = std::byte{2U};
    head[6U] = std::byte{3U};
    StoreU16(bytes, 7U, 101U);
    StoreU16(bytes, 9U, 3001U);
    StoreU32(bytes, 11U, 0x10203040U);
    StoreU64(bytes, 15U, sequence);
    return head;
}

ingress::SegmentHeaderV1 MakeSegmentHeader() {
    ingress::SegmentHeaderV1 header{};
    header.source_stream_id = 1001U;
    header.capture_date = 20260718U;
    FillNonzero(&header.stream_day_id, 1U);
    header.segment_sequence = 1U;
    header.segment_base_wal_pos = 0U;
    header.first_ingress_sequence = 1U;
    header.created_realtime_ns = 1000U;
    header.created_monotonic_ns = 900U;
    FillNonzero(&header.host_uuid, 21U);
    FillNonzero(&header.linux_boot_id, 41U);
    header.clock_epoch_algorithm = 1U;
    FillNonzero(&header.clock_epoch_digest, 61U);
    header.clock_epoch_label = 77U;
    FillNonzero(&header.sdk_archive_sha256, 81U);
    FillNonzero(&header.libmdl_api_sha256, 101U);
    FillNonzero(
        &header.endpoint_contract_sha256, 121U);
    FillNonzero(&header.config_sha256, 141U);
    FillNonzero(&header.raw_schema_sha256, 161U);
    FillNonzero(&header.build_manifest_sha256, 181U);
    return header;
}

ingress::RawSegmentArtifactOptionsV1 MakeOptions(
    const ingress::SegmentHeaderV1& header) {
    ingress::RawSegmentArtifactOptionsV1 options{};
    options.expected_raw_schema_sha256 =
        header.raw_schema_sha256;
    options.sample_record_interval = 2U;
    options.sample_raw_bytes_interval =
        ingress::kRawIndexV1DefaultRawBytesInterval;
    options.maximum_segment_bytes = 1U << 20U;
    return options;
}

void TestIncrementalMatchesRecoveryPlanner(
    TestContext* test) {
    const ingress::SegmentHeaderV1 header =
        MakeSegmentHeader();
    const ingress::RawSegmentArtifactOptionsV1 options =
        MakeOptions(header);
    ingress::RawSegmentAccumulatorV1 accumulator(options);

    ingress::RawV1SegmentHeaderWire header_wire{};
    test->Expect(
        ingress::EncodeSegmentHeaderV1(
            header, &header_wire) ==
            ingress::RawV1Error::kNone,
        "fixture segment header encodes");
    test->Expect(
        accumulator.OnSegmentOpened(header_wire),
        "incremental accumulator accepts segment header");

    auto exact_segment =
        std::make_shared<std::vector<std::byte>>(
            header_wire.begin(), header_wire.end());
    for (std::uint64_t sequence = 1U;
         sequence <= 5U;
         ++sequence) {
        const std::vector<std::byte> body(
            static_cast<std::size_t>(sequence + 3U),
            static_cast<std::byte>(
                static_cast<std::uint8_t>(
                    20U + sequence)));
        ingress::RawRecordInputV1 input{};
        input.meta.source_stream_id =
            header.source_stream_id;
        input.meta.connection_epoch_hint = 9U;
        input.meta.ingress_sequence = sequence;
        input.meta.recv_realtime_ns =
            2000U + sequence;
        input.meta.recv_monotonic_ns =
            1500U + sequence;
        input.meta.capture_date =
            header.capture_date;
        input.vendor_head =
            MakeVendorHead(
                static_cast<std::uint32_t>(
                    body.size()),
                900U + sequence);
        input.vendor_body = body;

        std::vector<std::byte> record;
        test->Expect(
            ingress::EncodeRawRecordV1(
                input, &record) ==
                ingress::RawV1Error::kNone,
            "fixture record encodes");
        const std::uint64_t segment_offset =
            exact_segment->size();
        test->Expect(
            accumulator.OnRecordCommitted(
                record,
                segment_offset,
                header.segment_base_wal_pos +
                    segment_offset),
            "incremental accumulator accepts complete record");
        exact_segment->insert(
            exact_segment->end(),
            record.begin(),
            record.end());
    }

    ingress::DurableMarkerV1 marker{};
    marker.source_stream_id =
        header.source_stream_id;
    marker.segment_sequence =
        header.segment_sequence;
    marker.durable_global_wal_pos =
        exact_segment->size();
    marker.durable_ingress_sequence = 5U;
    marker.durable_segment_offset =
        exact_segment->size();
    marker.marker_flags =
        ingress::kRawV1SegmentSealed;
    ingress::RawV1DurableMarkerWire marker_wire{};
    test->Expect(
        ingress::EncodeDurableMarkerV1(
            marker, &marker_wire) ==
            ingress::RawV1Error::kNone,
        "fixture seal marker encodes");
    const ingress::RawWalCursor sealed_cursor{
        marker.durable_global_wal_pos,
        marker.durable_ingress_sequence,
        marker.durable_segment_offset};
    test->Expect(
        accumulator.OnSegmentSealed(
            marker_wire, sealed_cursor),
        "incremental accumulator finalizes at accepted seal");

    ingress::RawSegmentArtifactPlanV1
        incremental{};
    test->Expect(
        accumulator.TakeArtifactPlan(&incremental),
        "sealed incremental artifact plan transfers once");
    test->Expect(
        !accumulator.TakeArtifactPlan(&incremental),
        "incremental artifact plan rejects a second transfer");
    test->Expect(
        incremental.ok(),
        "writer observer returns an intrinsically valid no-rescan plan");

    const ingress::RawSegmentArtifactPlanV1 recovery =
        ingress::BuildRawSegmentArtifactPlanV1(
            exact_segment, marker_wire, options);
    test->Expect(
        recovery.ok(),
        "full recovery scanner builds comparison artifacts");
    test->Expect(
        incremental.index_bytes == recovery.index_bytes,
        "runtime incremental index is byte-identical to recovery rebuild");
    test->Expect(
        incremental.metadata.segment_sha256 ==
            recovery.metadata.segment_sha256,
        "runtime incremental segment hash matches exact recovery hash");
    test->Expect(
        incremental.metadata.index_sha256 ==
            recovery.metadata.index_sha256,
        "runtime incremental index hash matches recovery rebuild");
    test->Expect(
        incremental.metadata.record_count == 5U &&
            incremental.metadata
                    .actual_first_ingress_sequence ==
                1U &&
            incremental.metadata
                    .actual_last_ingress_sequence ==
                5U,
        "incremental metadata preserves exact record range");
}

void TestCoordinateFailureIsSticky(
    TestContext* test) {
    const ingress::SegmentHeaderV1 header =
        MakeSegmentHeader();
    ingress::RawSegmentAccumulatorV1 accumulator(
        MakeOptions(header));
    ingress::RawV1SegmentHeaderWire header_wire{};
    static_cast<void>(
        ingress::EncodeSegmentHeaderV1(
            header, &header_wire));
    test->Expect(
        accumulator.OnSegmentOpened(header_wire),
        "failure fixture opens");

    const std::array<std::byte, 8U> invalid_record{};
    test->Expect(
        !accumulator.OnRecordCommitted(
            invalid_record,
            ingress::kRawV1SegmentHeaderBytes + 8U,
            ingress::kRawV1SegmentHeaderBytes + 8U),
        "offset gap is rejected before record decoding");
    test->Expect(
        accumulator.error() ==
            ingress::RawSegmentAccumulatorErrorV1::
                kRecordCoordinateMismatch,
        "first accumulator failure remains classified");
    test->Expect(
        !accumulator.OnSegmentOpened(header_wire),
        "failed accumulator cannot be reused");
    test->Expect(
        accumulator.error() ==
            ingress::RawSegmentAccumulatorErrorV1::
                kRecordCoordinateMismatch,
        "later invalid-state call does not overwrite root cause");
}

}  // namespace

int main() {
    TestContext test;
    TestIncrementalMatchesRecoveryPlanner(&test);
    TestCoordinateFailureIsSticky(&test);
    if (test.failures != 0) {
        std::cerr << test.failures
                  << " raw segment accumulator test(s) failed\n";
        return 1;
    }
    std::cout
        << "Phase 2 Raw segment accumulator tests passed\n";
    return 0;
}
