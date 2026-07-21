#include "l2flow/common/crc32c.h"
#include "l2flow/ingress/raw_index_v1.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <iostream>
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

    void ExpectError(
        ingress::RawIndexV1Error actual,
        ingress::RawIndexV1Error expected,
        const std::string& description) {
        if (actual != expected) {
            ++failures;
            std::cerr
                << "FAIL: " << description << " (expected "
                << ingress::RawIndexV1ErrorName(expected)
                << ", got "
                << ingress::RawIndexV1ErrorName(actual)
                << ")\n";
        }
    }

    int failures = 0;
};

void PutU16(
    std::span<std::byte> bytes,
    std::size_t offset,
    std::uint16_t value) {
    bytes[offset] =
        static_cast<std::byte>(value & 0xffU);
    bytes[offset + 1U] =
        static_cast<std::byte>((value >> 8U) & 0xffU);
}

void PutU32(
    std::span<std::byte> bytes,
    std::size_t offset,
    std::uint32_t value) {
    for (std::size_t index = 0U;
         index < sizeof(value);
         ++index) {
        const unsigned int shift =
            static_cast<unsigned int>(index * 8U);
        bytes[offset + index] =
            static_cast<std::byte>((value >> shift) & 0xffU);
    }
}

void PutU64(
    std::span<std::byte> bytes,
    std::size_t offset,
    std::uint64_t value) {
    for (std::size_t index = 0U;
         index < sizeof(value);
         ++index) {
        const unsigned int shift =
            static_cast<unsigned int>(index * 8U);
        bytes[offset + index] =
            static_cast<std::byte>((value >> shift) & 0xffU);
    }
}

std::uint32_t GetU32(
    std::span<const std::byte> bytes,
    std::size_t offset) {
    std::uint32_t value = 0U;
    for (std::size_t index = 0U;
         index < sizeof(value);
         ++index) {
        const unsigned int shift =
            static_cast<unsigned int>(index * 8U);
        value |=
            std::to_integer<std::uint32_t>(bytes[offset + index])
            << shift;
    }
    return value;
}

template <std::size_t Size>
std::array<std::byte, Size> Pattern(std::uint8_t first) {
    std::array<std::byte, Size> bytes{};
    for (std::size_t index = 0U; index < bytes.size(); ++index) {
        bytes[index] = static_cast<std::byte>(
            static_cast<std::uint8_t>(
                static_cast<unsigned int>(first) +
                static_cast<unsigned int>(index)));
    }
    return bytes;
}

template <std::size_t Size>
void PutBytes(
    std::span<std::byte> output,
    std::size_t offset,
    const std::array<std::byte, Size>& value) {
    std::copy(
        value.begin(), value.end(), output.begin() + offset);
}

ingress::RawIndexHeaderV1 MakeHeader() {
    ingress::RawIndexHeaderV1 header{};
    header.capture_date = 20260718U;
    header.source_stream_id = 0x01020304U;
    header.stream_day_id = Pattern<16U>(0x10U);
    header.segment_sequence = 1U;
    header.segment_base_wal_pos = 0U;
    header.raw_schema_sha256 = Pattern<32U>(0x40U);
    return header;
}

ingress::RawIndexEntryV1 MakeEntry(
    std::uint64_t ingress_sequence,
    std::uint64_t file_offset) {
    ingress::RawIndexEntryV1 entry{};
    entry.ingress_sequence = ingress_sequence;
    entry.record_start_wal_pos = file_offset;
    entry.record_end_wal_pos = file_offset + 144U;
    entry.segment_file_offset = file_offset;
    entry.vendor_sequence_id =
        0x0102030405060700ULL + ingress_sequence;
    entry.recv_monotonic_ns =
        0x1112131415161700ULL + ingress_sequence;
    entry.connection_epoch_hint =
        0x21222300U +
        static_cast<std::uint32_t>(ingress_sequence);
    entry.vendor_service_version = 0x3456U;
    entry.vendor_message_id = 0x789aU;
    entry.vendor_service_id = 0xbcU;
    return entry;
}

ingress::RawV1DurableMarkerWire MakeSealedMarker(
    std::uint64_t ingress_sequence,
    std::uint64_t logical_end) {
    ingress::DurableMarkerV1 marker{};
    marker.source_stream_id = 0x01020304U;
    marker.segment_sequence = 1U;
    marker.durable_global_wal_pos = logical_end;
    marker.durable_ingress_sequence = ingress_sequence;
    marker.durable_segment_offset = logical_end;
    marker.marker_flags = ingress::kRawV1SegmentSealed;
    ingress::RawV1DurableMarkerWire wire{};
    const ingress::RawV1Error result =
        ingress::EncodeDurableMarkerV1(marker, &wire);
    if (result != ingress::RawV1Error::kNone) {
        std::cerr << "cannot construct test seal marker\n";
    }
    return wire;
}

ingress::RawIndexFooterV1 MakeFooter(
    std::uint64_t entry_count,
    std::uint64_t record_count,
    std::uint64_t ingress_sequence,
    std::uint64_t logical_end) {
    ingress::RawIndexFooterV1 footer{};
    footer.entry_count = entry_count;
    footer.segment_record_count = record_count;
    footer.segment_logical_end_offset = logical_end;
    footer.segment_sha256 = Pattern<32U>(0x80U);
    footer.accepted_segment_sealed_marker =
        MakeSealedMarker(ingress_sequence, logical_end);
    return footer;
}

void RecomputeBlockCrc(
    std::span<std::byte> file,
    std::size_t block_offset,
    std::size_t block_size,
    std::size_t field_offset) {
    PutU32(file, block_offset + field_offset, 0U);
    const std::uint32_t crc =
        l2flow::common::ComputeCrc32c(
            std::span<const std::byte>(
                file.subspan(block_offset, block_size)));
    PutU32(file, block_offset + field_offset, crc);
}

void RecomputeFileCrc(std::span<std::byte> file) {
    const std::size_t footer_offset =
        file.size() - ingress::kRawIndexV1FooterBytes;
    const std::size_t crc_offset =
        footer_offset +
        ingress::raw_index_v1_offset::footer::
            kIndexFileCrc32c;
    PutU32(file, crc_offset, 0U);
    const std::uint32_t crc =
        l2flow::common::ComputeCrc32c(
            std::span<const std::byte>(file));
    PutU32(file, crc_offset, crc);
}

std::vector<std::byte> ExpectedGoldenFile() {
    constexpr std::uint32_t header_crc = 0x0c424c40U;
    constexpr std::uint32_t first_entry_crc = 0xf68d2ceeU;
    constexpr std::uint32_t second_entry_crc = 0x824c3350U;
    constexpr std::uint32_t marker_crc = 0x4f379736U;
    constexpr std::uint32_t file_crc = 0x49bad2d0U;

    std::vector<std::byte> expected(
        ingress::kRawIndexV1HeaderBytes +
            2U * ingress::kRawIndexV1EntryBytes +
            ingress::kRawIndexV1FooterBytes,
        std::byte{0});
    std::span<std::byte> bytes(expected);
    const ingress::RawIndexHeaderV1 header = MakeHeader();
    PutU32(bytes, 0U, 0x3149524dU);
    PutU16(bytes, 4U, 1U);
    bytes[6U] = std::byte{1};
    PutU32(bytes, 8U, 4096U);
    PutU32(bytes, 12U, 64U);
    PutU32(bytes, 16U, 4096U);
    PutU32(bytes, 24U, header.capture_date);
    PutU32(bytes, 28U, header.source_stream_id);
    PutBytes(bytes, 32U, header.stream_day_id);
    PutU32(bytes, 48U, header.segment_sequence);
    PutU64(bytes, 56U, header.segment_base_wal_pos);
    PutBytes(bytes, 64U, header.raw_schema_sha256);
    PutU64(bytes, 96U, 4096U);
    PutU64(bytes, 104U, 4U * 1024U * 1024U);
    PutU32(bytes, 112U, header_crc);

    const std::array<ingress::RawIndexEntryV1, 2U> entries{{
        MakeEntry(1U, 4096U),
        MakeEntry(2U, 4240U),
    }};
    const std::array<std::uint32_t, 2U> entry_crcs{{
        first_entry_crc,
        second_entry_crc,
    }};
    for (std::size_t index = 0U; index < entries.size(); ++index) {
        const std::size_t offset =
            ingress::kRawIndexV1HeaderBytes +
            index * ingress::kRawIndexV1EntryBytes;
        const ingress::RawIndexEntryV1& entry = entries[index];
        PutU64(bytes, offset + 0U, entry.ingress_sequence);
        PutU64(bytes, offset + 8U, entry.record_start_wal_pos);
        PutU64(bytes, offset + 16U, entry.record_end_wal_pos);
        PutU64(bytes, offset + 24U, entry.segment_file_offset);
        PutU64(bytes, offset + 32U, entry.vendor_sequence_id);
        PutU64(bytes, offset + 40U, entry.recv_monotonic_ns);
        PutU32(
            bytes,
            offset + 48U,
            entry.connection_epoch_hint);
        PutU16(
            bytes,
            offset + 52U,
            entry.vendor_service_version);
        PutU16(
            bytes,
            offset + 54U,
            entry.vendor_message_id);
        bytes[offset + 56U] =
            static_cast<std::byte>(entry.vendor_service_id);
        PutU32(bytes, offset + 60U, entry_crcs[index]);
    }

    const std::size_t footer_offset =
        ingress::kRawIndexV1HeaderBytes +
        2U * ingress::kRawIndexV1EntryBytes;
    PutU32(bytes, footer_offset + 0U, 0x3146524dU);
    PutU16(bytes, footer_offset + 4U, 1U);
    bytes[footer_offset + 6U] = std::byte{1};
    PutU32(bytes, footer_offset + 8U, 4096U);
    PutU64(bytes, footer_offset + 16U, 2U);
    PutU64(bytes, footer_offset + 24U, 2U);
    PutU64(bytes, footer_offset + 32U, 4384U);
    PutBytes(bytes, footer_offset + 40U, Pattern<32U>(0x80U));

    const std::size_t marker_offset = footer_offset + 72U;
    PutU32(bytes, marker_offset + 0U, 0x3152444dU);
    PutU16(bytes, marker_offset + 4U, 1U);
    PutU16(bytes, marker_offset + 6U, 48U);
    PutU32(bytes, marker_offset + 8U, 0x01020304U);
    PutU32(bytes, marker_offset + 12U, 1U);
    PutU64(bytes, marker_offset + 16U, 4384U);
    PutU64(bytes, marker_offset + 24U, 2U);
    PutU64(bytes, marker_offset + 32U, 4384U);
    PutU32(bytes, marker_offset + 40U, marker_crc);
    PutU32(
        bytes,
        marker_offset + 44U,
        ingress::kRawV1SegmentSealed);
    PutU32(bytes, footer_offset + 120U, file_crc);
    return expected;
}

ingress::RawIndexExpectedIdentityV1 ExpectedIdentity() {
    const ingress::RawIndexHeaderV1 header = MakeHeader();
    ingress::RawIndexExpectedIdentityV1 expected{};
    expected.capture_date = header.capture_date;
    expected.source_stream_id = header.source_stream_id;
    expected.stream_day_id = header.stream_day_id;
    expected.segment_sequence = header.segment_sequence;
    expected.segment_base_wal_pos =
        header.segment_base_wal_pos;
    expected.raw_schema_sha256 = header.raw_schema_sha256;
    expected.sample_record_interval =
        header.sample_record_interval;
    expected.sample_raw_bytes_interval =
        header.sample_raw_bytes_interval;
    return expected;
}

}  // namespace

int main() {
    TestContext test;

    const ingress::RawIndexHeaderV1 header = MakeHeader();
    const std::array<ingress::RawIndexEntryV1, 2U> entries{{
        MakeEntry(1U, 4096U),
        MakeEntry(2U, 4240U),
    }};
    const ingress::RawIndexFooterV1 footer =
        MakeFooter(2U, 2U, 2U, 4384U);
    std::vector<std::byte> golden;
    test.ExpectError(
        ingress::BuildRawIndexFileV1(
            header, entries, footer, &golden),
        ingress::RawIndexV1Error::kNone,
        "golden index file builds");
    test.Expect(
        golden.size() ==
            4096U + 2U * 64U + 4096U,
        "file length is exactly 4096 + 64*n + 4096");

    const std::vector<std::byte> expected_golden =
        ExpectedGoldenFile();
    if (golden != expected_golden && !golden.empty()) {
        const std::size_t footer_offset =
            golden.size() - ingress::kRawIndexV1FooterBytes;
        std::cerr
            << std::hex << std::setfill('0')
            << "actual golden CRCs: header=0x"
            << std::setw(8)
            << GetU32(golden, 112U)
            << " entry0=0x"
            << std::setw(8)
            << GetU32(golden, 4096U + 60U)
            << " entry1=0x"
            << std::setw(8)
            << GetU32(golden, 4096U + 64U + 60U)
            << " marker=0x"
            << std::setw(8)
            << GetU32(golden, footer_offset + 72U + 40U)
            << " file=0x"
            << std::setw(8)
            << GetU32(golden, footer_offset + 120U)
            << std::dec << '\n';
    }
    test.Expect(
        golden == expected_golden,
        "complete RawIndexV1 golden bytes are frozen");

    ingress::RawIndexFileV1 decoded{};
    test.ExpectError(
        ingress::DecodeRawIndexFileV1(golden, &decoded),
        ingress::RawIndexV1Error::kNone,
        "golden file decodes");
    test.Expect(
        decoded.entries.size() == 2U &&
            decoded.footer.segment_record_count == 2U &&
            decoded.footer.segment_logical_end_offset == 4384U &&
            decoded.accepted_segment_sealed_marker.marker_flags ==
                ingress::kRawV1SegmentSealed,
        "decoded footer and accepted seal are retained");
    test.ExpectError(
        ingress::ValidateRawIndexFileV1(
            golden, ExpectedIdentity(), nullptr),
        ingress::RawIndexV1Error::kNone,
        "exact namespace/schema validation succeeds");

    ingress::RawIndexExpectedIdentityV1 wrong_schema =
        ExpectedIdentity();
    wrong_schema.raw_schema_sha256[0U] ^= std::byte{0x01};
    test.ExpectError(
        ingress::ValidateRawIndexFileV1(
            golden, wrong_schema, nullptr),
        ingress::RawIndexV1Error::kSchemaMismatch,
        "Raw schema mismatch is rejected");

    std::vector<std::byte> corrupted = golden;
    corrupted[116U] = std::byte{1};
    test.ExpectError(
        ingress::DecodeRawIndexFileV1(corrupted, &decoded),
        ingress::RawIndexV1Error::kNonzeroReserved,
        "nonzero header reserved byte is rejected");

    corrupted = golden;
    PutU32(corrupted, 20U, 1U);
    RecomputeBlockCrc(
        corrupted, 0U, 4096U, 112U);
    RecomputeFileCrc(corrupted);
    test.ExpectError(
        ingress::DecodeRawIndexFileV1(corrupted, &decoded),
        ingress::RawIndexV1Error::kUnknownFlags,
        "unknown header flag is rejected even with valid CRCs");

    corrupted = golden;
    corrupted[4096U + 57U] = std::byte{1};
    RecomputeFileCrc(corrupted);
    test.ExpectError(
        ingress::DecodeRawIndexFileV1(corrupted, &decoded),
        ingress::RawIndexV1Error::kNonzeroReserved,
        "nonzero entry reserved byte is rejected");

    corrupted = golden;
    corrupted[4096U + 32U] ^= std::byte{1};
    RecomputeFileCrc(corrupted);
    test.ExpectError(
        ingress::DecodeRawIndexFileV1(corrupted, &decoded),
        ingress::RawIndexV1Error::kEntryCrcMismatch,
        "entry CRC detects a changed entry");

    corrupted = golden;
    const std::size_t footer_offset =
        corrupted.size() - ingress::kRawIndexV1FooterBytes;
    corrupted[footer_offset + 124U] = std::byte{1};
    test.ExpectError(
        ingress::DecodeRawIndexFileV1(corrupted, &decoded),
        ingress::RawIndexV1Error::kNonzeroReserved,
        "nonzero footer reserved byte is rejected");

    corrupted = golden;
    corrupted[footer_offset + 40U] ^= std::byte{1};
    test.ExpectError(
        ingress::DecodeRawIndexFileV1(corrupted, &decoded),
        ingress::RawIndexV1Error::kFileCrcMismatch,
        "complete-file CRC detects a changed footer binding");

    corrupted = golden;
    PutU64(corrupted, footer_offset + 16U, 1U);
    RecomputeFileCrc(corrupted);
    test.ExpectError(
        ingress::DecodeRawIndexFileV1(corrupted, &decoded),
        ingress::RawIndexV1Error::kEntryCountMismatch,
        "entry count cannot describe a different file length");

    corrupted = golden;
    ingress::DurableMarkerV1 mismatched_marker =
        decoded.accepted_segment_sealed_marker;
    mismatched_marker.durable_global_wal_pos += 8U;
    ingress::RawV1DurableMarkerWire mismatched_marker_wire{};
    test.Expect(
        ingress::EncodeDurableMarkerV1(
            mismatched_marker, &mismatched_marker_wire) ==
            ingress::RawV1Error::kNone,
        "mismatched test marker remains structurally valid");
    std::copy(
        mismatched_marker_wire.begin(),
        mismatched_marker_wire.end(),
        corrupted.begin() + footer_offset + 72U);
    RecomputeFileCrc(corrupted);
    test.ExpectError(
        ingress::DecodeRawIndexFileV1(corrupted, &decoded),
        ingress::RawIndexV1Error::kMarkerMismatch,
        "seal marker cursor must match header base and footer end");

    ingress::RawIndexHeaderV1 sampled_header = MakeHeader();
    sampled_header.sample_record_interval = 2U;
    sampled_header.sample_raw_bytes_interval =
        ingress::kRawIndexV1DefaultRawBytesInterval;
    ingress::RawIndexBuilderV1 builder(sampled_header);
    std::uint64_t next_offset = 4096U;
    for (std::uint64_t sequence = 1U; sequence <= 5U; ++sequence) {
        test.ExpectError(
            builder.AddRecord(
                MakeEntry(sequence, next_offset)),
            ingress::RawIndexV1Error::kNone,
            "builder accepts contiguous Raw record");
        next_offset += 144U;
    }
    std::vector<std::byte> sampled_file;
    test.ExpectError(
        builder.Build(
            Pattern<32U>(0x90U),
            MakeSealedMarker(5U, next_offset),
            &sampled_file),
        ingress::RawIndexV1Error::kNone,
        "builder seals sampled index");
    ingress::RawIndexFileV1 sampled{};
    test.ExpectError(
        ingress::DecodeRawIndexFileV1(sampled_file, &sampled),
        ingress::RawIndexV1Error::kNone,
        "sampled builder output validates");
    test.Expect(
        sampled.entries.size() == 3U &&
            sampled.entries[0U].ingress_sequence == 1U &&
            sampled.entries[1U].ingress_sequence == 3U &&
            sampled.entries[2U].ingress_sequence == 5U,
        "builder samples first, threshold crossing and final record");

    ingress::RawIndexBuilderV1 broken_builder(MakeHeader());
    test.ExpectError(
        broken_builder.AddRecord(MakeEntry(1U, 4096U)),
        ingress::RawIndexV1Error::kNone,
        "builder accepts its first record");
    test.ExpectError(
        broken_builder.AddRecord(MakeEntry(3U, 4240U)),
        ingress::RawIndexV1Error::kEntryOrderViolation,
        "builder rejects a skipped ingress sequence");

    ingress::RawIndexBuilderV1 empty_builder(MakeHeader());
    std::vector<std::byte> empty_file;
    test.ExpectError(
        empty_builder.Build(
            Pattern<32U>(0xa0U),
            MakeSealedMarker(0U, 4096U),
            &empty_file),
        ingress::RawIndexV1Error::kNone,
        "header-only sealed segment index builds");
    ingress::RawIndexFileV1 empty_index{};
    test.ExpectError(
        ingress::DecodeRawIndexFileV1(
            empty_file, &empty_index),
        ingress::RawIndexV1Error::kNone,
        "header-only sealed segment index validates");
    test.Expect(
        empty_index.entries.empty() &&
            empty_index.footer.segment_record_count == 0U &&
            empty_index.footer.segment_logical_end_offset ==
                4096U,
        "empty index binds the legal data-begin boundary");

    if (test.failures != 0) {
        std::cerr << test.failures
                  << " Phase 2 RawIndexV1 tests failed\n";
        return 1;
    }
    std::cout << "Phase 2 RawIndexV1 tests passed\n";
    return 0;
}
