#include "l2flow/common/crc32c.h"
#include "l2flow/ingress/raw_reader.h"
#include "l2flow/ingress/raw_v1.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace common = l2flow::common;
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
    for (std::size_t index = 0U; index < 4U; ++index) {
        bytes[offset + index] = static_cast<std::byte>(
            (value >> (index * 8U)) & 0xffU);
    }
}

void StoreU64(
    std::span<std::byte> bytes,
    std::size_t offset,
    std::uint64_t value) {
    for (std::size_t index = 0U; index < 8U; ++index) {
        bytes[offset + index] = static_cast<std::byte>(
            (value >> (index * 8U)) & 0xffU);
    }
}

template <std::size_t Size>
void FillNonzero(
    std::array<std::byte, Size>* bytes,
    std::uint8_t seed) {
    if (bytes == nullptr) {
        return;
    }
    for (std::size_t index = 0U; index < Size; ++index) {
        (*bytes)[index] = static_cast<std::byte>(
            static_cast<std::uint8_t>(
                seed + static_cast<std::uint8_t>(index)));
    }
}

ingress::RawV1VendorHead MakeVendorHead(
    std::uint32_t body_size,
    std::uint64_t vendor_sequence) {
    ingress::RawV1VendorHead result{};
    std::span<std::byte> bytes(result);
    result[0] = static_cast<std::byte>(
        ingress::kVendorMessageHeadBytes);
    StoreU32(
        bytes,
        1U,
        static_cast<std::uint32_t>(
            ingress::kVendorMessageHeadBytes) +
            body_size);
    result[5] = std::byte{2U};
    result[6] = std::byte{3U};
    StoreU16(bytes, 7U, 101U);
    StoreU16(bytes, 9U, 3001U);
    StoreU32(bytes, 11U, 0x12345678U);
    StoreU64(bytes, 15U, vendor_sequence);
    return result;
}

struct BuiltSegment final {
    std::shared_ptr<std::vector<std::byte>> bytes;
    std::vector<std::size_t> starts;
    std::vector<std::size_t> ends;
    std::vector<std::size_t> body_sizes;
    std::uint64_t base_wal_pos = 0U;
};

BuiltSegment BuildSegment(
    const std::vector<std::uint64_t>& sequences,
    const std::vector<std::size_t>& body_sizes) {
    BuiltSegment result;
    result.bytes =
        std::make_shared<std::vector<std::byte>>();
    result.base_wal_pos = 8192U;

    ingress::SegmentHeaderV1 segment;
    segment.source_stream_id = 1001U;
    segment.capture_date = 20260718U;
    FillNonzero(&segment.stream_day_id, 1U);
    segment.segment_sequence = 7U;
    segment.segment_base_wal_pos = result.base_wal_pos;
    segment.first_ingress_sequence =
        sequences.empty() ? 1U : sequences.front();
    segment.created_realtime_ns = 1'000'000U;
    segment.created_monotonic_ns = 500'000U;
    FillNonzero(&segment.host_uuid, 21U);
    FillNonzero(&segment.linux_boot_id, 41U);
    segment.clock_epoch_algorithm = 1U;
    FillNonzero(&segment.clock_epoch_digest, 61U);
    segment.clock_epoch_label = 123U;
    FillNonzero(&segment.sdk_archive_sha256, 81U);
    FillNonzero(&segment.libmdl_api_sha256, 101U);
    FillNonzero(&segment.endpoint_contract_sha256, 121U);
    FillNonzero(&segment.config_sha256, 141U);
    FillNonzero(&segment.raw_schema_sha256, 161U);
    FillNonzero(&segment.build_manifest_sha256, 181U);

    ingress::RawV1SegmentHeaderWire header_wire{};
    if (ingress::EncodeSegmentHeaderV1(
            segment, &header_wire) !=
        ingress::RawV1Error::kNone) {
        return result;
    }
    result.bytes->assign(
        header_wire.begin(), header_wire.end());

    const std::size_t count =
        std::min(sequences.size(), body_sizes.size());
    for (std::size_t index = 0U; index < count; ++index) {
        std::vector<std::byte> body(body_sizes[index]);
        for (std::size_t body_index = 0U;
             body_index < body.size();
             ++body_index) {
            body[body_index] = static_cast<std::byte>(
                static_cast<std::uint8_t>(
                    10U + index + body_index));
        }

        ingress::RawRecordInputV1 input;
        input.meta.source_stream_id = segment.source_stream_id;
        input.meta.connection_epoch_hint = 9U;
        input.meta.ingress_sequence = sequences[index];
        input.meta.recv_realtime_ns =
            2'000'000U + sequences[index];
        input.meta.recv_monotonic_ns =
            1'500'000U + sequences[index];
        input.meta.capture_date = segment.capture_date;
        input.vendor_head = MakeVendorHead(
            static_cast<std::uint32_t>(body.size()),
            900U + sequences[index]);
        input.vendor_body = body;

        std::vector<std::byte> record_wire;
        if (ingress::EncodeRawRecordV1(
                input, &record_wire) !=
            ingress::RawV1Error::kNone) {
            continue;
        }
        result.starts.push_back(result.bytes->size());
        result.body_sizes.push_back(body.size());
        result.bytes->insert(
            result.bytes->end(),
            record_wire.begin(),
            record_wire.end());
        result.ends.push_back(result.bytes->size());
    }
    return result;
}

std::shared_ptr<std::vector<std::byte>> Clone(
    const BuiltSegment& built) {
    return std::make_shared<std::vector<std::byte>>(
        *built.bytes);
}

void RewriteRecordHeader(
    std::vector<std::byte>* bytes,
    std::size_t record_start,
    const ingress::RawRecordHeaderV1& header) {
    ingress::RawV1RecordHeaderWire wire{};
    if (bytes == nullptr ||
        ingress::EncodeRawRecordHeaderV1(
            header, &wire) !=
            ingress::RawV1Error::kNone) {
        return;
    }
    std::copy(
        wire.begin(),
        wire.end(),
        bytes->begin() +
            static_cast<std::ptrdiff_t>(record_start));
}

ingress::RawRecordHeaderV1 ReadRecordHeader(
    const std::vector<std::byte>& bytes,
    std::size_t record_start) {
    ingress::RawRecordHeaderV1 result;
    static_cast<void>(ingress::DecodeRawRecordHeaderV1(
        std::span<const std::byte>(
            bytes.data() + record_start,
            ingress::kRawV1RecordHeaderBytes),
        &result));
    return result;
}

void RecomputeHeaderCrc(
    std::vector<std::byte>* bytes,
    std::size_t record_start) {
    if (bytes == nullptr) {
        return;
    }
    std::span<std::byte> header(
        bytes->data() + record_start,
        ingress::kRawV1RecordHeaderBytes);
    StoreU32(
        header,
        ingress::raw_v1_offset::record_header::kHeaderCrc32c,
        0U);
    const std::uint32_t checksum =
        common::ComputeCrc32c(
            std::span<const std::byte>(
                header.data(), header.size()));
    StoreU32(
        header,
        ingress::raw_v1_offset::record_header::kHeaderCrc32c,
        checksum);
}

ingress::RawSegmentScanResult Scan(
    const std::shared_ptr<std::vector<std::byte>>& bytes,
    std::size_t durable_limit) {
    std::shared_ptr<const std::vector<std::byte>> immutable =
        bytes;
    return ingress::ScanRawSegmentV1(
        std::move(immutable),
        static_cast<std::uint64_t>(durable_limit));
}

void TestValidAndLifetime(TestContext* test) {
    BuiltSegment built = BuildSegment({41U, 42U}, {2U, 9U});
    test->Expect(
        built.starts.size() == 2U,
        "fixture encodes two records");
    ingress::RawSegmentScanResult scanned =
        Scan(built.bytes, built.bytes->size());
    test->Expect(
        scanned.ok() && scanned.records.size() == 2U,
        "valid multi-record durable range is accepted");
    if (!scanned.ok() || scanned.records.size() != 2U) {
        return;
    }
    test->Expect(
        scanned.segment.source_stream_id == 1001U &&
            scanned.segment.capture_date == 20260718U &&
            scanned.records[0].metadata().ingress_sequence == 41U &&
            scanned.records[1].metadata().ingress_sequence == 42U,
        "segment namespace and strict sequence are decoded");
    test->Expect(
        scanned.records[0].vendor_head().size() ==
                ingress::kVendorMessageHeadBytes &&
            scanned.records[0].vendor_body().size() == 2U &&
            scanned.records[1].vendor_body().size() == 9U,
        "head and body spans expose only validated payload bytes");
    test->Expect(
        scanned.validated_end_offset == built.bytes->size() &&
            scanned.validated_end_wal_pos ==
                built.base_wal_pos + built.bytes->size() &&
            scanned.records[0].record_start_offset() ==
                built.starts[0] &&
            scanned.records[1].record_end_offset() ==
                built.ends[1],
        "exclusive segment and global WAL cursors are exact");

    std::optional<ingress::RawRecordView> retained(
        scanned.records.front());
    const std::byte expected = retained->vendor_body().front();
    scanned.records.clear();
    built.bytes.reset();
    test->Expect(
        retained->vendor_body().front() == expected,
        "RawRecordView owns the backing-buffer lifetime token");
}

void TestDurableBound(TestContext* test) {
    BuiltSegment built = BuildSegment({1U, 2U}, {2U, 9U});
    std::shared_ptr<std::vector<std::byte>> corrupt =
        Clone(built);
    (*corrupt)[built.starts[1] +
        ingress::raw_v1_offset::record_header::kHeaderCrc32c] ^=
        std::byte{1U};
    const ingress::RawSegmentScanResult prefix =
        Scan(corrupt, built.ends[0]);
    test->Expect(
        prefix.ok() && prefix.records.size() == 1U &&
            prefix.validated_end_offset == built.ends[0],
        "bytes after the exclusive durable cursor are invisible");

    const ingress::RawSegmentScanResult cut =
        Scan(built.bytes, built.ends[0] - 1U);
    test->Expect(
        !cut.ok() &&
            cut.error ==
                ingress::RawReaderError::kRecordPastDurableLimit &&
            cut.records.empty(),
        "a durable cursor inside the first record exposes no record");

    const ingress::RawSegmentScanResult too_far =
        ingress::ScanRawSegmentV1(
            built.bytes,
            static_cast<std::uint64_t>(
                built.bytes->size()) +
                1U);
    test->Expect(
        too_far.error ==
            ingress::RawReaderError::kDurableLimitPastBuffer,
        "durable cursor beyond the owned buffer is rejected");
}

void TestCrcPaddingTrailerAndSize(TestContext* test) {
    BuiltSegment built = BuildSegment({1U}, {2U});
    const std::size_t start = built.starts[0];

    std::shared_ptr<std::vector<std::byte>> header_crc =
        Clone(built);
    (*header_crc)[start +
        ingress::raw_v1_offset::record_header::
            kConnectionEpochHint] ^= std::byte{1U};
    test->Expect(
        Scan(header_crc, header_crc->size()).error ==
            ingress::RawReaderError::kRecordHeaderInvalid,
        "record-header CRC corruption is rejected");

    std::shared_ptr<std::vector<std::byte>> payload_crc =
        Clone(built);
    (*payload_crc)[
        start + ingress::kRawV1RecordHeaderBytes +
        ingress::kVendorMessageHeadBytes] ^= std::byte{1U};
    test->Expect(
        Scan(payload_crc, payload_crc->size()).error ==
            ingress::RawReaderError::kPayloadCrcMismatch,
        "payload CRC corruption is rejected");

    std::shared_ptr<std::vector<std::byte>> padding =
        Clone(built);
    const std::size_t padding_offset =
        start + ingress::kRawV1RecordHeaderBytes +
        ingress::kVendorMessageHeadBytes +
        built.body_sizes[0];
    (*padding)[padding_offset] = std::byte{1U};
    test->Expect(
        Scan(padding, padding->size()).error ==
            ingress::RawReaderError::kNonZeroPadding,
        "non-zero alignment padding is rejected");

    std::shared_ptr<std::vector<std::byte>> trailer =
        Clone(built);
    (*trailer)[
        built.ends[0] - ingress::kRawV1RecordTrailerBytes +
        ingress::raw_v1_offset::record_trailer::kCommitMagic] ^=
        std::byte{1U};
    test->Expect(
        Scan(trailer, trailer->size()).error ==
            ingress::RawReaderError::kTrailerInvalid,
        "trailer commit corruption is rejected");

    std::shared_ptr<std::vector<std::byte>> size =
        Clone(built);
    std::span<std::byte> size_header(
        size->data() + start,
        ingress::kRawV1RecordHeaderBytes);
    StoreU32(
        size_header,
        ingress::raw_v1_offset::record_header::kRecordSize,
        static_cast<std::uint32_t>(
            built.ends[0] - built.starts[0] + 8U));
    RecomputeHeaderCrc(size.get(), start);
    test->Expect(
        !Scan(size, size->size()).ok(),
        "CRC-consistent record-size corruption is rejected");

    std::shared_ptr<std::vector<std::byte>> segment_crc =
        Clone(built);
    (*segment_crc)[
        ingress::raw_v1_offset::segment::kCreatedRealtimeNs] ^=
        std::byte{1U};
    test->Expect(
        Scan(segment_crc, segment_crc->size()).error ==
            ingress::RawReaderError::kSegmentHeaderInvalid,
        "segment-header CRC corruption is rejected");
}

void TestSemanticCrossChecks(TestContext* test) {
    BuiltSegment built = BuildSegment({1U, 2U}, {2U, 9U});
    const std::size_t first = built.starts[0];
    const std::size_t second = built.starts[1];

    std::shared_ptr<std::vector<std::byte>> vendor =
        Clone(built);
    const std::size_t head_offset =
        first + ingress::kRawV1RecordHeaderBytes;
    (*vendor)[head_offset + 9U] ^= std::byte{1U};
    ingress::RawRecordHeaderV1 vendor_header =
        ReadRecordHeader(*built.bytes, first);
    common::Crc32cState payload_crc;
    payload_crc.Update(std::span<const std::byte>(
        vendor->data() + head_offset,
        ingress::kVendorMessageHeadBytes +
            built.body_sizes[0]));
    vendor_header.payload_crc32c = payload_crc.Finalize();
    RewriteRecordHeader(vendor.get(), first, vendor_header);
    test->Expect(
        Scan(vendor, vendor->size()).error ==
            ingress::RawReaderError::kVendorHeadMismatch,
        "CRC-consistent duplicated vendor-field mismatch is rejected");

    std::shared_ptr<std::vector<std::byte>> namespace_bad =
        Clone(built);
    ingress::RawRecordHeaderV1 namespace_header =
        ReadRecordHeader(*namespace_bad, first);
    ++namespace_header.source_stream_id;
    RewriteRecordHeader(
        namespace_bad.get(), first, namespace_header);
    test->Expect(
        Scan(namespace_bad, namespace_bad->size()).error ==
            ingress::RawReaderError::kNamespaceMismatch,
        "record stream/date namespace must match its segment");

    std::shared_ptr<std::vector<std::byte>> sequence_bad =
        Clone(built);
    ingress::RawRecordHeaderV1 sequence_header =
        ReadRecordHeader(*sequence_bad, second);
    ++sequence_header.ingress_sequence;
    RewriteRecordHeader(
        sequence_bad.get(), second, sequence_header);
    const ingress::RawSegmentScanResult sequence_result =
        Scan(sequence_bad, sequence_bad->size());
    test->Expect(
        sequence_result.error ==
                ingress::RawReaderError::kIngressSequenceMismatch &&
            sequence_result.records.size() == 1U &&
            sequence_result.validated_end_offset == built.ends[0],
        "records must advance ingress sequence by exactly one");

    std::shared_ptr<std::vector<std::byte>> reserved =
        Clone(built);
    (*reserved)[
        first +
        ingress::raw_v1_offset::record_header::kReserved1] =
        std::byte{1U};
    RecomputeHeaderCrc(reserved.get(), first);
    test->Expect(
        Scan(reserved, reserved->size()).error ==
            ingress::RawReaderError::kRecordHeaderInvalid,
        "CRC-consistent non-zero reserved bytes are rejected");
}

}  // namespace

int main() {
    TestContext test;
    TestValidAndLifetime(&test);
    TestDurableBound(&test);
    TestCrcPaddingTrailerAndSize(&test);
    TestSemanticCrossChecks(&test);

    if (test.failures != 0) {
        std::cerr << test.failures
                  << " Phase 2 raw-reader tests failed\n";
        return 1;
    }
    std::cout << "Phase 2 raw-reader tests passed\n";
    return 0;
}
