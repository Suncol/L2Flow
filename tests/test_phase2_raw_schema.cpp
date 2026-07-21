#include "l2flow/common/sha256.h"
#include "l2flow/ingress/raw_control_page.h"
#include "l2flow/ingress/raw_index_v1.h"
#include "l2flow/ingress/raw_schema.h"
#include "l2flow/ingress/raw_v1.h"
#include "l2flow/ingress/raw_writer_lease.h"

#include <cstddef>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <string>
#include <system_error>

namespace common = l2flow::common;
namespace ingress = l2flow::ingress;

namespace {

struct TestContext final {
    void Expect(
        bool condition,
        const std::string& message) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << message << '\n';
        }
    }

    int failures = 0;
};

static_assert(ingress::kRawV1FormatVersion == 1U);
static_assert(ingress::kRawV1LittleEndian == 1U);
static_assert(ingress::kRawV1RecordAlignment == 8U);
static_assert(ingress::kRawV1SegmentHeaderBytes == 4096U);
static_assert(ingress::kRawV1RecordHeaderBytes == 96U);
static_assert(ingress::kRawV1RecordTrailerBytes == 16U);
static_assert(ingress::kRawV1JournalHeaderBytes == 4096U);
static_assert(ingress::kRawV1DurableMarkerBytes == 48U);
static_assert(ingress::kRawV1SegmentMagic == 0x3153524dU);
static_assert(ingress::kRawV1RecordMagic == 0x3157524dU);
static_assert(
    ingress::kRawV1RecordCommitMagic == 0x3143524dU);
static_assert(ingress::kRawV1JournalMagic == 0x314a444dU);
static_assert(
    ingress::kRawV1DurableMarkerMagic == 0x3152444dU);

static_assert(
    ingress::raw_v1_offset::segment::kHeaderCrc32c ==
    416U);
static_assert(
    ingress::raw_v1_offset::segment::kReservedTail ==
    420U);
static_assert(
    ingress::raw_v1_offset::record_header::kPayloadCrc32c ==
    80U);
static_assert(
    ingress::raw_v1_offset::record_header::kHeaderCrc32c ==
    84U);
static_assert(
    ingress::raw_v1_offset::record_header::kReserved1 ==
    88U);
static_assert(
    ingress::raw_v1_offset::record_trailer::kCommitMagic ==
    4U);
static_assert(
    ingress::raw_v1_offset::journal::kHeaderCrc32c ==
    152U);
static_assert(
    ingress::raw_v1_offset::journal::kReservedTail ==
    156U);
static_assert(
    ingress::raw_v1_offset::marker::kMarkerCrc32c ==
    40U);
static_assert(
    ingress::raw_v1_offset::marker::kMarkerFlags == 44U);

static_assert(ingress::kRawIndexV1FormatVersion == 1U);
static_assert(ingress::kRawIndexV1LittleEndian == 1U);
static_assert(ingress::kRawIndexV1HeaderBytes == 4096U);
static_assert(ingress::kRawIndexV1EntryBytes == 64U);
static_assert(ingress::kRawIndexV1FooterBytes == 4096U);
static_assert(
    ingress::kRawIndexV1HeaderMagic == 0x3149524dU);
static_assert(
    ingress::kRawIndexV1FooterMagic == 0x3146524dU);
static_assert(
    ingress::raw_index_v1_offset::header::kHeaderCrc32c ==
    112U);
static_assert(
    ingress::raw_index_v1_offset::header::kReservedTail ==
    116U);
static_assert(
    ingress::raw_index_v1_offset::entry::kReserved == 57U);
static_assert(
    ingress::raw_index_v1_offset::entry::kEntryCrc32c ==
    60U);
static_assert(
    ingress::raw_index_v1_offset::footer::
        kAcceptedSegmentSealedMarker == 72U);
static_assert(
    ingress::raw_index_v1_offset::footer::
        kIndexFileCrc32c == 120U);
static_assert(
    ingress::raw_index_v1_offset::footer::kReservedTail ==
    124U);

static_assert(ingress::kRawControlPageBytes == 4096U);
static_assert(ingress::kRawControlPageVersion == 1U);
static_assert(
    offsetof(ingress::RawControlPageV1, generation) == 24U);
static_assert(
    offsetof(
        ingress::RawControlPageV1,
        append_global_wal_pos) == 96U);
static_assert(
    offsetof(
        ingress::RawControlPageV1,
        durable_global_wal_pos) == 120U);
static_assert(
    offsetof(ingress::RawControlPageV1, reserved) == 160U);

static_assert(ingress::kRawWriterLeaseMarkerBytes == 64U);
static_assert(ingress::kRawWriterLeaseVersion == 1U);
static_assert(
    ingress::raw_writer_lease_offset::kVersion == 8U);
static_assert(
    ingress::raw_writer_lease_offset::kReserved == 20U);
static_assert(
    ingress::raw_writer_lease_offset::kCrc32c == 60U);

void TestFrozenDigest(
    const std::filesystem::path& schema,
    TestContext* test) {
    std::error_code size_error;
    const std::uintmax_t size =
        std::filesystem::file_size(schema, size_error);
    test->Expect(
        !size_error &&
            size == ingress::kFrozenRawSchemaBytes,
        "schema byte length matches the frozen length");

    common::Sha256Digest actual{};
    std::string hash_error;
    test->Expect(
        common::ComputeFileSha256(
            schema, &actual, &hash_error),
        "schema exact regular-file snapshot can be hashed");
    test->Expect(
        actual == ingress::kFrozenRawSchemaSha256,
        "schema exact UTF-8 bytes match the frozen binary digest");
    test->Expect(
        common::Sha256Hex(actual) ==
            ingress::kFrozenRawSchemaSha256Hex,
        "frozen binary and hexadecimal digests have one value");
    test->Expect(
        &ingress::RawSchemaSha256Digest() ==
            &ingress::kFrozenRawSchemaSha256,
        "runtime binary digest API returns the frozen object");
    test->Expect(
        ingress::RawSchemaSha256Hex() ==
            ingress::kFrozenRawSchemaSha256Hex,
        "runtime hexadecimal digest API returns the frozen value");

    common::Sha256Digest parsed{};
    std::string parse_error;
    test->Expect(
        common::ParseSha256Hex(
            ingress::RawSchemaSha256Hex(),
            &parsed,
            &parse_error) &&
            parsed == ingress::RawSchemaSha256Digest(),
        "frozen hexadecimal digest round-trips to binary");

    std::string verify_error;
    test->Expect(
        ingress::VerifyRawSchemaFile(schema, &verify_error) &&
            verify_error.empty(),
        "schema verifier accepts the frozen file");

    const std::filesystem::path missing =
        schema.string() + ".does-not-exist";
    test->Expect(
        !ingress::VerifyRawSchemaFile(
            missing, &verify_error) &&
            !verify_error.empty() &&
            verify_error.find(missing.string()) ==
                std::string::npos,
        "schema verifier rejects a missing file without path disclosure");
}

void TestSchemaContent(
    const std::filesystem::path& schema,
    TestContext* test) {
    std::ifstream input(schema, std::ios::binary);
    test->Expect(
        input.is_open(),
        "schema opens for independent content checks");
    if (!input.is_open()) {
        return;
    }
    const std::string text{
        std::istreambuf_iterator<char>(input),
        std::istreambuf_iterator<char>()};
    test->Expect(
        !text.empty() && text.back() == '\n',
        "schema has its frozen single trailing LF");
    test->Expect(
        text.size() < 3U ||
            !(static_cast<unsigned char>(text[0]) == 0xefU &&
              static_cast<unsigned char>(text[1]) == 0xbbU &&
              static_cast<unsigned char>(text[2]) == 0xbfU),
        "schema has no UTF-8 BOM");
    test->Expect(
        text.find('\r') == std::string::npos,
        "schema uses deterministic LF line endings");
    test->Expect(
        text.find("\"portable\": false") != std::string::npos &&
            text.find("\"durable\": false") !=
                std::string::npos &&
            text.find("\"volatile\": true") !=
                std::string::npos,
        "control page is explicitly host-local and non-durable");
    test->Expect(
        text.find("\"wire_ascii\": \"MRS1\"") !=
                std::string::npos &&
            text.find("\"wire_ascii\": \"MRW1\"") !=
                std::string::npos &&
            text.find("\"wire_ascii\": \"MRC1\"") !=
                std::string::npos &&
            text.find("\"wire_ascii\": \"MDJ1\"") !=
                std::string::npos &&
            text.find("\"wire_ascii\": \"MDR1\"") !=
                std::string::npos &&
            text.find("\"wire_ascii\": \"MRI1\"") !=
                std::string::npos &&
            text.find("\"wire_ascii\": \"MRF1\"") !=
                std::string::npos,
        "all portable Raw V1 magic byte strings are frozen");
    test->Expect(
        text.find(
            "\"field_offset_expression\": "
            "\"4096 + 64 * entry_count + 120\"") !=
                std::string::npos &&
            text.find(
                "\"padding_included\": false") !=
                std::string::npos,
        "whole-index and record-payload CRC domains are frozen");
    test->Expect(
        text.find("\"raw_writer_lease_marker_v1\"") !=
            std::string::npos,
        "typed Raw writer lease marker is in the schema");
    test->Expect(
        text.find("\"generated_at\"") == std::string::npos &&
            text.find("\"timestamp\"") == std::string::npos,
        "schema has no generated timestamp");
}

}  // namespace

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr
            << "usage: test_phase2_raw_schema <raw_v1.json>\n";
        return 2;
    }

    TestContext test;
    const std::filesystem::path schema(argv[1]);
    TestFrozenDigest(schema, &test);
    TestSchemaContent(schema, &test);
    return test.failures == 0 ? 0 : 1;
}
