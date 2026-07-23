#include "l2flow/common/crc32c.h"
#include "l2flow/ingress/raw_v1.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <limits>
#include <span>
#include <string>
#include <vector>

namespace ingress = l2flow::ingress;

namespace {

struct TestContext final {
    void Expect(bool condition, const std::string& message) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << message << '\n';
        }
    }

    void ExpectError(
        ingress::RawV1Error actual,
        ingress::RawV1Error expected,
        const std::string& message) {
        if (actual != expected) {
            ++failures;
            std::cerr << "FAIL: " << message << " (expected "
                      << ingress::RawV1ErrorName(expected)
                      << ", got "
                      << ingress::RawV1ErrorName(actual)
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
        value.begin(), value.end(), output.data() + offset);
}

void RecomputeCrc(
    std::span<std::byte> bytes,
    std::size_t crc_offset) {
    PutU32(bytes, crc_offset, 0U);
    const std::uint32_t crc =
        l2flow::common::ComputeCrc32c(
            std::span<const std::byte>(bytes));
    PutU32(bytes, crc_offset, crc);
}

ingress::SegmentHeaderV1 MakeSegmentHeader() {
    ingress::SegmentHeaderV1 header{};
    header.source_stream_id = 0x01020304U;
    header.capture_date = 20260718U;
    header.stream_day_id = Pattern<16U>(0x10U);
    header.segment_sequence = 1U;
    header.segment_flags = 0U;
    header.segment_base_wal_pos = 0U;
    header.first_ingress_sequence = 1U;
    header.created_realtime_ns = 0x0102030405060708ULL;
    header.created_monotonic_ns = 0x1112131415161718ULL;
    header.host_uuid = Pattern<16U>(0x20U);
    header.linux_boot_id = Pattern<16U>(0x30U);
    header.clock_epoch_algorithm = 0x01020304U;
    header.clock_epoch_digest = Pattern<32U>(0x40U);
    header.clock_epoch_label = 0x2122232425262728ULL;
    header.sdk_archive_sha256 = Pattern<32U>(0x60U);
    header.libmdl_api_sha256 = Pattern<32U>(0x80U);
    header.endpoint_contract_sha256 = Pattern<32U>(0xa0U);
    header.config_sha256 = Pattern<32U>(0xc0U);
    header.raw_schema_sha256 = Pattern<32U>(0x01U);
    header.build_manifest_sha256 = Pattern<32U>(0x31U);
    return header;
}

ingress::RawV1SegmentHeaderWire ExpectedSegmentWire(
    std::uint32_t crc) {
    ingress::RawV1SegmentHeaderWire expected{};
    std::span<std::byte> bytes(expected);
    const ingress::SegmentHeaderV1 header = MakeSegmentHeader();
    PutU32(bytes, 0U, 0x3153524dU);
    PutU16(bytes, 4U, 1U);
    bytes[6U] = std::byte{1};
    PutU32(bytes, 8U, 4096U);
    PutU32(bytes, 12U, header.source_stream_id);
    PutU32(bytes, 16U, header.capture_date);
    PutBytes(bytes, 20U, header.stream_day_id);
    PutU32(bytes, 36U, header.segment_sequence);
    PutU32(bytes, 40U, header.segment_flags);
    PutU64(bytes, 112U, header.segment_base_wal_pos);
    PutU64(bytes, 120U, header.first_ingress_sequence);
    PutU64(bytes, 128U, header.created_realtime_ns);
    PutU64(bytes, 136U, header.created_monotonic_ns);
    PutBytes(bytes, 144U, header.host_uuid);
    PutBytes(bytes, 160U, header.linux_boot_id);
    PutU32(bytes, 176U, header.clock_epoch_algorithm);
    PutBytes(bytes, 184U, header.clock_epoch_digest);
    PutU64(bytes, 216U, header.clock_epoch_label);
    PutBytes(bytes, 224U, header.sdk_archive_sha256);
    PutBytes(bytes, 256U, header.libmdl_api_sha256);
    PutBytes(bytes, 288U, header.endpoint_contract_sha256);
    PutBytes(bytes, 320U, header.config_sha256);
    PutBytes(bytes, 352U, header.raw_schema_sha256);
    PutBytes(bytes, 384U, header.build_manifest_sha256);
    PutU32(bytes, 416U, crc);
    return expected;
}

ingress::DurableJournalHeaderV1 MakeJournalHeader() {
    ingress::DurableJournalHeaderV1 header{};
    header.capture_date = 20260718U;
    header.source_stream_id = 0x01020304U;
    header.stream_day_id = Pattern<16U>(0x10U);
    header.raw_schema_sha256 = Pattern<32U>(0x01U);
    header.created_host_uuid = Pattern<16U>(0x20U);
    header.created_linux_boot_id = Pattern<16U>(0x30U);
    header.created_clock_epoch_algorithm = 0x01020304U;
    header.created_clock_epoch_digest = Pattern<32U>(0x40U);
    header.created_clock_epoch_label =
        0x2122232425262728ULL;
    return header;
}

ingress::RawV1JournalHeaderWire ExpectedJournalWire(
    std::uint32_t crc) {
    ingress::RawV1JournalHeaderWire expected{};
    std::span<std::byte> bytes(expected);
    const ingress::DurableJournalHeaderV1 header =
        MakeJournalHeader();
    PutU32(bytes, 0U, 0x314a444dU);
    PutU16(bytes, 4U, 1U);
    bytes[6U] = std::byte{1};
    PutU32(bytes, 8U, 4096U);
    PutU32(bytes, 12U, header.capture_date);
    PutU32(bytes, 16U, header.source_stream_id);
    PutBytes(bytes, 20U, header.stream_day_id);
    PutBytes(bytes, 40U, header.raw_schema_sha256);
    PutBytes(bytes, 72U, header.created_host_uuid);
    PutBytes(bytes, 88U, header.created_linux_boot_id);
    PutU32(bytes, 104U, header.created_clock_epoch_algorithm);
    PutBytes(bytes, 112U, header.created_clock_epoch_digest);
    PutU64(bytes, 144U, header.created_clock_epoch_label);
    PutU32(bytes, 152U, crc);
    return expected;
}

ingress::DurableMarkerV1 MakeMarker() {
    ingress::DurableMarkerV1 marker{};
    marker.source_stream_id = 0x01020304U;
    marker.segment_sequence = 7U;
    marker.durable_global_wal_pos =
        0x0000000100003000ULL;
    marker.durable_ingress_sequence =
        0x0102030405060708ULL;
    marker.durable_segment_offset = 0x3000U;
    marker.marker_flags = ingress::kRawV1SegmentSealed;
    return marker;
}

ingress::RawV1DurableMarkerWire ExpectedMarkerWire(
    std::uint32_t crc) {
    ingress::RawV1DurableMarkerWire expected{};
    std::span<std::byte> bytes(expected);
    const ingress::DurableMarkerV1 marker = MakeMarker();
    PutU32(bytes, 0U, 0x3152444dU);
    PutU16(bytes, 4U, 1U);
    PutU16(bytes, 6U, 48U);
    PutU32(bytes, 8U, marker.source_stream_id);
    PutU32(bytes, 12U, marker.segment_sequence);
    PutU64(bytes, 16U, marker.durable_global_wal_pos);
    PutU64(bytes, 24U, marker.durable_ingress_sequence);
    PutU64(bytes, 32U, marker.durable_segment_offset);
    PutU32(bytes, 40U, crc);
    PutU32(bytes, 44U, marker.marker_flags);
    return expected;
}

ingress::RawV1VendorHead MakeVendorHead(
    std::size_t body_size) {
    ingress::RawV1VendorHead head{};
    std::span<std::byte> bytes(head);
    head[0U] = static_cast<std::byte>(
        ingress::kVendorMessageHeadBytes);
    PutU32(
        bytes,
        1U,
        static_cast<std::uint32_t>(
            ingress::kVendorMessageHeadBytes + body_size));
    head[5U] = std::byte{0x01};
    head[6U] = std::byte{0x02};
    PutU16(bytes, 7U, 0x1234U);
    PutU16(bytes, 9U, 0x5678U);
    PutU32(bytes, 11U, 0x89abcdefU);
    PutU64(bytes, 15U, 0x0123456789abcdefULL);
    return head;
}

ingress::RawRecordInputV1 MakeRecordInput(
    std::span<const std::byte> body) {
    ingress::RawRecordInputV1 input{};
    input.meta.source_stream_id = 0x10203040U;
    input.meta.connection_epoch_hint = 0x50607080U;
    input.meta.ingress_sequence = 0x0102030405060708ULL;
    input.meta.recv_realtime_ns = 0x1112131415161718ULL;
    input.meta.recv_monotonic_ns = 0x2122232425262728ULL;
    input.meta.capture_date = 20260718U;
    input.meta.flags = 0U;
    input.vendor_head = MakeVendorHead(body.size());
    input.vendor_body = body;
    return input;
}

std::vector<std::byte> ExpectedRecordWire(
    std::span<const std::byte> body,
    std::uint32_t payload_crc,
    std::uint32_t header_crc) {
    std::vector<std::byte> expected(144U, std::byte{0});
    std::span<std::byte> bytes(expected);
    const ingress::RawRecordInputV1 input =
        MakeRecordInput(body);
    PutU32(bytes, 0U, 0x3157524dU);
    PutU16(bytes, 4U, 1U);
    PutU16(bytes, 6U, 96U);
    PutU32(bytes, 8U, 144U);
    PutU32(bytes, 12U, input.meta.flags);
    PutU32(bytes, 16U, input.meta.source_stream_id);
    PutU32(bytes, 20U, input.meta.connection_epoch_hint);
    PutU64(bytes, 24U, input.meta.ingress_sequence);
    PutU64(bytes, 32U, input.meta.recv_realtime_ns);
    PutU64(bytes, 40U, input.meta.recv_monotonic_ns);
    PutU32(bytes, 48U, input.meta.capture_date);
    PutU32(bytes, 52U, 0x89abcdefU);
    PutU64(bytes, 56U, 0x0123456789abcdefULL);
    PutU32(bytes, 64U, 30U);
    PutU32(bytes, 68U, 7U);
    PutU16(bytes, 72U, 0x1234U);
    PutU16(bytes, 74U, 0x5678U);
    bytes[76U] = std::byte{0x02};
    bytes[77U] = std::byte{0x01};
    bytes[78U] = std::byte{23};
    PutU32(bytes, 80U, payload_crc);
    PutU32(bytes, 84U, header_crc);
    std::copy(
        input.vendor_head.begin(),
        input.vendor_head.end(),
        expected.begin() + 96U);
    std::copy(body.begin(), body.end(), expected.begin() + 119U);
    PutU32(bytes, 128U, 144U);
    PutU32(bytes, 132U, 0x3143524dU);
    PutU64(bytes, 136U, input.meta.ingress_sequence);
    return expected;
}

void RecomputeRecordChecksums(
    std::vector<std::byte>* record) {
    std::span<std::byte> bytes(*record);
    const std::uint32_t message_size = GetU32(bytes, 64U);
    const std::uint32_t payload_crc =
        l2flow::common::ComputeCrc32c(
            std::span<const std::byte>(bytes).subspan(
                96U, message_size));
    PutU32(bytes, 80U, payload_crc);
    RecomputeCrc(bytes.first(96U), 84U);
}

void TestSchemaAndLayout(TestContext* test) {
    static_assert(ingress::kRawV1SegmentHeaderBytes == 4096U);
    static_assert(ingress::kRawV1RecordHeaderBytes == 96U);
    static_assert(ingress::kRawV1RecordTrailerBytes == 16U);
    static_assert(ingress::kRawV1JournalHeaderBytes == 4096U);
    static_assert(ingress::kRawV1DurableMarkerBytes == 48U);
    static_assert(
        ingress::raw_v1_offset::segment::kHeaderCrc32c ==
        416U);
    static_assert(
        ingress::raw_v1_offset::record_header::kHeaderCrc32c ==
        84U);
    static_assert(
        ingress::raw_v1_offset::journal::kHeaderCrc32c ==
        152U);
    static_assert(
        ingress::raw_v1_offset::marker::kMarkerCrc32c ==
        40U);

    ingress::RawRecordLayoutV1 layout{};
    test->ExpectError(
        ingress::ComputeRawRecordLayoutV1(0U, &layout),
        ingress::RawV1Error::kNone,
        "zero-body record layout is representable");
    test->Expect(
        layout.vendor_message_size == 23U &&
            layout.padding_size == 1U &&
            layout.record_size == 136U,
        "zero-body layout follows the frozen alignment formula");

    test->ExpectError(
        ingress::ComputeRawRecordLayoutV1(7U, &layout),
        ingress::RawV1Error::kNone,
        "seven-byte body layout is representable");
    test->Expect(
        layout.vendor_message_size == 30U &&
            layout.padding_size == 2U &&
            layout.record_size == 144U,
        "seven-byte body layout follows the frozen alignment formula");

    test->ExpectError(
        ingress::ComputeRawRecordLayoutV1(
            std::numeric_limits<std::size_t>::max(), &layout),
        ingress::RawV1Error::kSizeOverflow,
        "record layout rejects size_t overflow");
    if constexpr (
        std::numeric_limits<std::size_t>::max() >=
        std::numeric_limits<std::uint32_t>::max()) {
        constexpr std::size_t kLargestBodyWithU32Record =
            4'294'967'153ULL;
        test->ExpectError(
            ingress::ComputeRawRecordLayoutV1(
                kLargestBodyWithU32Record, &layout),
            ingress::RawV1Error::kNone,
            "largest aligned u32 Raw record is representable");
        test->Expect(
            layout.record_size == 4'294'967'288U,
            "largest aligned Raw record remains below UINT32_MAX");
        test->ExpectError(
            ingress::ComputeRawRecordLayoutV1(
                kLargestBodyWithU32Record + 1U, &layout),
            ingress::RawV1Error::kSizeOverflow,
            "next body byte would overflow record_size");
    }
    test->ExpectError(
        ingress::ComputeRawRecordLayoutV1(0U, nullptr),
        ingress::RawV1Error::kNullOutput,
        "record layout rejects a null output");
}

void TestSegmentGoldenAndNegatives(TestContext* test) {
    constexpr std::uint32_t kGoldenSegmentCrc = 0xd3fd5f13U;
    ingress::RawV1SegmentHeaderWire wire{};
    test->ExpectError(
        ingress::EncodeSegmentHeaderV1(
            MakeSegmentHeader(), &wire),
        ingress::RawV1Error::kNone,
        "segment header encodes");
    if (GetU32(wire, 416U) != kGoldenSegmentCrc) {
        std::cerr << "INFO: segment golden CRC is 0x"
                  << std::hex << GetU32(wire, 416U)
                  << std::dec << '\n';
    }
    test->Expect(
        wire == ExpectedSegmentWire(kGoldenSegmentCrc),
        "segment header matches all 4096 golden bytes");
    test->ExpectError(
        ingress::ValidateSegmentHeaderV1(wire),
        ingress::RawV1Error::kNone,
        "segment golden validates");

    ingress::SegmentHeaderV1 decoded{};
    test->ExpectError(
        ingress::DecodeSegmentHeaderV1(wire, &decoded),
        ingress::RawV1Error::kNone,
        "segment golden decodes");
    test->Expect(
        decoded.stream_day_id ==
                MakeSegmentHeader().stream_day_id &&
            decoded.raw_schema_sha256 ==
                MakeSegmentHeader().raw_schema_sha256 &&
            decoded.header_crc32c == kGoldenSegmentCrc,
        "segment decode preserves identities and CRC");

    ingress::SegmentHeaderV1 path_only_sdk = MakeSegmentHeader();
    path_only_sdk.sdk_archive_sha256 = {};
    path_only_sdk.libmdl_api_sha256 = {};
    ingress::RawV1SegmentHeaderWire path_only_wire{};
    test->ExpectError(
        ingress::EncodeSegmentHeaderV1(path_only_sdk, &path_only_wire),
        ingress::RawV1Error::kNone,
        "path-only SDK records both unavailable provenance digests as zero");
    ingress::SegmentHeaderV1 mixed_sdk_identity = path_only_sdk;
    mixed_sdk_identity.libmdl_api_sha256 = Pattern<32U>(0x80U);
    test->ExpectError(
        ingress::EncodeSegmentHeaderV1(
            mixed_sdk_identity, &path_only_wire),
        ingress::RawV1Error::kInvalidIdentity,
        "SDK archive/library provenance cannot mix unavailable and pinned identities");

    ingress::RawV1SegmentHeaderWire corrupted = wire;
    corrupted[300U] ^= std::byte{0x80};
    test->ExpectError(
        ingress::ValidateSegmentHeaderV1(corrupted),
        ingress::RawV1Error::kHeaderCrcMismatch,
        "segment corruption is rejected by whole-header CRC");

    corrupted = wire;
    corrupted[420U] = std::byte{1};
    RecomputeCrc(corrupted, 416U);
    test->ExpectError(
        ingress::ValidateSegmentHeaderV1(corrupted),
        ingress::RawV1Error::kNonzeroReserved,
        "segment nonzero reserved tail is rejected");

    corrupted = wire;
    PutU32(corrupted, 40U, 0x80000000U);
    RecomputeCrc(corrupted, 416U);
    test->ExpectError(
        ingress::ValidateSegmentHeaderV1(corrupted),
        ingress::RawV1Error::kUnknownFlags,
        "segment unknown flags are rejected");

    corrupted = wire;
    corrupted[6U] = std::byte{2};
    test->ExpectError(
        ingress::ValidateSegmentHeaderV1(corrupted),
        ingress::RawV1Error::kInvalidEndian,
        "segment non-little-endian tag is rejected");

    ingress::SegmentHeaderV1 invalid = MakeSegmentHeader();
    invalid.reserve_state_uuid[0U] = std::byte{1};
    const ingress::RawV1SegmentHeaderWire before_failure = wire;
    test->ExpectError(
        ingress::EncodeSegmentHeaderV1(invalid, &wire),
        ingress::RawV1Error::kInvalidIdentity,
        "normal segment rejects finalization identity");
    test->Expect(
        wire == before_failure,
        "failed segment encode leaves the caller's wire unchanged");

    ingress::SegmentHeaderV1 continuation = MakeSegmentHeader();
    continuation.segment_flags =
        ingress::kRawV1FinalizationContinuation;
    continuation.reserve_state_uuid = Pattern<16U>(0xe0U);
    continuation.finalization_cycle_id = Pattern<16U>(0xf0U);
    continuation.immutable_grant_sha256 =
        Pattern<32U>(0x55U);
    test->ExpectError(
        ingress::EncodeSegmentHeaderV1(continuation, &wire),
        ingress::RawV1Error::kNone,
        "continuation requires and accepts all three grant identities");
}

void TestJournalGoldenAndNegatives(TestContext* test) {
    constexpr std::uint32_t kGoldenJournalCrc = 0xb25e5081U;
    ingress::RawV1JournalHeaderWire wire{};
    test->ExpectError(
        ingress::EncodeDurableJournalHeaderV1(
            MakeJournalHeader(), &wire),
        ingress::RawV1Error::kNone,
        "journal header encodes");
    if (GetU32(wire, 152U) != kGoldenJournalCrc) {
        std::cerr << "INFO: journal golden CRC is 0x"
                  << std::hex << GetU32(wire, 152U)
                  << std::dec << '\n';
    }
    test->Expect(
        wire == ExpectedJournalWire(kGoldenJournalCrc),
        "journal header matches all 4096 golden bytes");
    test->ExpectError(
        ingress::ValidateDurableJournalHeaderV1(wire),
        ingress::RawV1Error::kNone,
        "journal golden validates");

    ingress::DurableJournalHeaderV1 decoded{};
    test->ExpectError(
        ingress::DecodeDurableJournalHeaderV1(
            wire, &decoded),
        ingress::RawV1Error::kNone,
        "journal golden decodes");
    test->Expect(
        decoded.stream_day_id ==
                MakeJournalHeader().stream_day_id &&
            decoded.header_crc32c == kGoldenJournalCrc,
        "journal decode preserves namespace and CRC");

    ingress::RawV1JournalHeaderWire corrupted = wire;
    corrupted[50U] ^= std::byte{1};
    test->ExpectError(
        ingress::ValidateDurableJournalHeaderV1(corrupted),
        ingress::RawV1Error::kHeaderCrcMismatch,
        "journal corruption is rejected by whole-header CRC");

    corrupted = wire;
    corrupted[36U] = std::byte{1};
    RecomputeCrc(corrupted, 152U);
    test->ExpectError(
        ingress::ValidateDurableJournalHeaderV1(corrupted),
        ingress::RawV1Error::kNonzeroReserved,
        "journal internal reserved bytes must remain zero");
}

void TestMarkerGoldenAndNegatives(TestContext* test) {
    constexpr std::uint32_t kGoldenMarkerCrc = 0x6b18e898U;
    ingress::RawV1DurableMarkerWire wire{};
    test->ExpectError(
        ingress::EncodeDurableMarkerV1(MakeMarker(), &wire),
        ingress::RawV1Error::kNone,
        "durable marker encodes");
    if (GetU32(wire, 40U) != kGoldenMarkerCrc) {
        std::cerr << "INFO: marker golden CRC is 0x"
                  << std::hex << GetU32(wire, 40U)
                  << std::dec << '\n';
    }
    test->Expect(
        wire == ExpectedMarkerWire(kGoldenMarkerCrc),
        "durable marker matches all 48 golden bytes");
    test->ExpectError(
        ingress::ValidateDurableMarkerV1(wire),
        ingress::RawV1Error::kNone,
        "durable marker golden validates");

    ingress::DurableMarkerV1 decoded{};
    test->ExpectError(
        ingress::DecodeDurableMarkerV1(wire, &decoded),
        ingress::RawV1Error::kNone,
        "durable marker golden decodes");
    test->Expect(
        decoded.durable_global_wal_pos ==
                MakeMarker().durable_global_wal_pos &&
            decoded.marker_crc32c == kGoldenMarkerCrc,
        "durable marker decode preserves cursors and CRC");

    ingress::RawV1DurableMarkerWire corrupted = wire;
    corrupted[24U] ^= std::byte{1};
    test->ExpectError(
        ingress::ValidateDurableMarkerV1(corrupted),
        ingress::RawV1Error::kHeaderCrcMismatch,
        "marker corruption is rejected by whole-marker CRC");

    corrupted = wire;
    PutU32(corrupted, 44U, 0x00000002U);
    RecomputeCrc(corrupted, 40U);
    test->ExpectError(
        ingress::ValidateDurableMarkerV1(corrupted),
        ingress::RawV1Error::kUnknownFlags,
        "marker unknown flag is rejected");

    ingress::DurableMarkerV1 invalid = MakeMarker();
    invalid.durable_global_wal_pos = 4095U;
    invalid.durable_segment_offset = 4096U;
    test->ExpectError(
        ingress::EncodeDurableMarkerV1(invalid, &wire),
        ingress::RawV1Error::kRecordSizeMismatch,
        "marker rejects a global cursor before its segment cursor");
}

void TestRecordGoldenAndNegatives(TestContext* test) {
    constexpr std::array<std::byte, 7U> body{
        std::byte{0xde}, std::byte{0xad}, std::byte{0x00},
        std::byte{0xff}, std::byte{0x11}, std::byte{0x22},
        std::byte{0x33}};
    constexpr std::uint32_t kGoldenPayloadCrc = 0x369d963eU;
    constexpr std::uint32_t kGoldenRecordHeaderCrc =
        0xaec73017U;

    const ingress::RawRecordInputV1 input = MakeRecordInput(body);
    std::vector<std::byte> wire;
    ingress::RawRecordHeaderV1 encoded_header{};
    test->ExpectError(
        ingress::EncodeRawRecordV1(
            input, &wire, &encoded_header),
        ingress::RawV1Error::kNone,
        "Raw record encodes");
    if (GetU32(wire, 80U) != kGoldenPayloadCrc ||
        GetU32(wire, 84U) != kGoldenRecordHeaderCrc) {
        std::cerr << "INFO: record golden payload/header CRCs are 0x"
                  << std::hex << GetU32(wire, 80U)
                  << "/0x" << GetU32(wire, 84U)
                  << std::dec << '\n';
    }
    test->Expect(
        wire == ExpectedRecordWire(
                    body,
                    kGoldenPayloadCrc,
                    kGoldenRecordHeaderCrc),
        "Raw record matches all 144 golden bytes");
    test->Expect(
        encoded_header.payload_crc32c == kGoldenPayloadCrc &&
            encoded_header.header_crc32c ==
                kGoldenRecordHeaderCrc,
        "record encoder reports both frozen CRC values");

    ingress::RawRecordNamespaceV1 expected_namespace{};
    expected_namespace.source_stream_id =
        input.meta.source_stream_id;
    expected_namespace.capture_date = input.meta.capture_date;
    ingress::RawRecordViewV1 decoded{};
    test->ExpectError(
        ingress::DecodeRawRecordV1(
            wire, &decoded, &expected_namespace),
        ingress::RawV1Error::kNone,
        "Raw record golden decodes with namespace binding");
    test->Expect(
        std::equal(
            decoded.vendor_head.begin(),
            decoded.vendor_head.end(),
            input.vendor_head.begin(),
            input.vendor_head.end()) &&
            std::equal(
                decoded.vendor_body.begin(),
                decoded.vendor_body.end(),
                body.begin(),
                body.end()) &&
            decoded.padding.size() == 2U &&
            decoded.trailer.ingress_sequence ==
                input.meta.ingress_sequence,
        "decoder preserves opaque payload and validates trailer");

    std::vector<std::byte> corrupted = wire;
    corrupted[119U] ^= std::byte{1};
    test->ExpectError(
        ingress::ValidateRawRecordV1(corrupted),
        ingress::RawV1Error::kPayloadCrcMismatch,
        "body corruption is rejected by payload CRC");

    corrupted = wire;
    corrupted[32U] ^= std::byte{1};
    test->ExpectError(
        ingress::ValidateRawRecordV1(corrupted),
        ingress::RawV1Error::kHeaderCrcMismatch,
        "record-header corruption is rejected by header CRC");

    corrupted = wire;
    corrupted[126U] = std::byte{1};
    test->ExpectError(
        ingress::ValidateRawRecordV1(corrupted),
        ingress::RawV1Error::kNonzeroPadding,
        "record padding must remain zero");

    corrupted = wire;
    corrupted[88U] = std::byte{1};
    RecomputeCrc(
        std::span<std::byte>(corrupted).first(96U), 84U);
    test->ExpectError(
        ingress::ValidateRawRecordV1(corrupted),
        ingress::RawV1Error::kNonzeroReserved,
        "record header reserved bytes must remain zero");

    corrupted = wire;
    corrupted[76U] ^= std::byte{1};
    RecomputeCrc(
        std::span<std::byte>(corrupted).first(96U), 84U);
    test->ExpectError(
        ingress::ValidateRawRecordV1(corrupted),
        ingress::RawV1Error::kVendorFieldMismatch,
        "copied header fields must match the opaque vendor head");

    corrupted = wire;
    PutU32(corrupted, 97U, 31U);
    RecomputeRecordChecksums(&corrupted);
    test->ExpectError(
        ingress::ValidateRawRecordV1(corrupted),
        ingress::RawV1Error::kInvalidVendorHead,
        "vendor MessageSize must match the framed payload");

    corrupted = wire;
    corrupted[136U] ^= std::byte{1};
    test->ExpectError(
        ingress::ValidateRawRecordV1(corrupted),
        ingress::RawV1Error::kTrailerMismatch,
        "trailer ingress sequence must match the header");

    corrupted = wire;
    corrupted[132U] ^= std::byte{1};
    test->ExpectError(
        ingress::ValidateRawRecordV1(corrupted),
        ingress::RawV1Error::kTrailerMismatch,
        "unknown trailer commit magic is rejected");

    corrupted = wire;
    corrupted.push_back(std::byte{0});
    test->ExpectError(
        ingress::ValidateRawRecordV1(corrupted),
        ingress::RawV1Error::kRecordSizeMismatch,
        "full-record decoder requires exact record_size bytes");

    ingress::RawRecordNamespaceV1 wrong_namespace =
        expected_namespace;
    ++wrong_namespace.capture_date;
    test->ExpectError(
        ingress::ValidateRawRecordV1(
            wire, &wrong_namespace),
        ingress::RawV1Error::kInvalidIdentity,
        "record namespace mismatch is rejected");

    corrupted = wire;
    PutU32(corrupted, 12U, 1U);
    RecomputeCrc(
        std::span<std::byte>(corrupted).first(96U), 84U);
    test->ExpectError(
        ingress::ValidateRawRecordV1(corrupted),
        ingress::RawV1Error::kUnknownFlags,
        "unassigned Raw record flags are rejected");

    corrupted = wire;
    PutU32(corrupted, 8U, 152U);
    RecomputeCrc(
        std::span<std::byte>(corrupted).first(96U), 84U);
    test->ExpectError(
        ingress::ValidateRawRecordV1(corrupted),
        ingress::RawV1Error::kRecordSizeMismatch,
        "header record_size must equal the checked body formula");

    ingress::RawRecordInputV1 invalid_input = input;
    invalid_input.vendor_head[0U] = std::byte{22};
    const std::vector<std::byte> before_failure = wire;
    test->ExpectError(
        ingress::EncodeRawRecordV1(
            invalid_input, &wire),
        ingress::RawV1Error::kInvalidVendorHead,
        "record encoder rejects an invalid vendor HeadSize");
    test->Expect(
        wire == before_failure,
        "failed record encode leaves the caller's wire unchanged");

    invalid_input = input;
    invalid_input.meta.flags = 1U;
    test->ExpectError(
        ingress::EncodeRawRecordV1(
            invalid_input, &wire),
        ingress::RawV1Error::kUnknownFlags,
        "record encoder rejects unassigned flags");

    ingress::RawV1RecordTrailerWire trailer_wire{};
    ingress::RawRecordTrailerV1 trailer{};
    trailer.record_size = 144U;
    trailer.ingress_sequence = input.meta.ingress_sequence;
    test->ExpectError(
        ingress::EncodeRawRecordTrailerV1(
            trailer, &trailer_wire),
        ingress::RawV1Error::kNone,
        "standalone trailer codec encodes");
    test->ExpectError(
        ingress::ValidateRawRecordTrailerV1(trailer_wire),
        ingress::RawV1Error::kNone,
        "standalone trailer codec validates");
}

}  // namespace

int main() {
    TestContext test;
    TestSchemaAndLayout(&test);
    TestSegmentGoldenAndNegatives(&test);
    TestJournalGoldenAndNegatives(&test);
    TestMarkerGoldenAndNegatives(&test);
    TestRecordGoldenAndNegatives(&test);

    if (test.failures != 0) {
        std::cerr << test.failures
                  << " Phase 2 Raw V1 test(s) failed\n";
        return 1;
    }
    std::cout << "Phase 2 Raw V1 codec tests passed\n";
    return 0;
}
