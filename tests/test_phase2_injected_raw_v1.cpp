#include "l2flow/common/crc32c.h"
#include "l2flow/common/sha256.h"
#include "l2flow/ingress/injected_raw_reader.h"
#include "l2flow/ingress/injected_raw_v1.h"
#include "l2flow/ingress/raw_reader.h"
#include "l2flow/ingress/raw_v1.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <iterator>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace ingress = l2flow::ingress;

namespace {

struct TestContext final {
    void Expect(
        bool condition,
        const std::string& description) {
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
    for (std::size_t index = 0U; index < 2U; ++index) {
        bytes[offset + index] = static_cast<std::byte>(
            static_cast<std::uint8_t>(
                (value >> (index * 8U)) & 0xffU));
    }
}

void StoreU32(
    std::span<std::byte> bytes,
    std::size_t offset,
    std::uint32_t value) {
    for (std::size_t index = 0U; index < 4U; ++index) {
        bytes[offset + index] = static_cast<std::byte>(
            static_cast<std::uint8_t>(
                (value >> (index * 8U)) & 0xffU));
    }
}

void StoreU64(
    std::span<std::byte> bytes,
    std::size_t offset,
    std::uint64_t value) {
    for (std::size_t index = 0U; index < 8U; ++index) {
        bytes[offset + index] = static_cast<std::byte>(
            static_cast<std::uint8_t>(
                (value >> (index * 8U)) & 0xffU));
    }
}

std::uint32_t LoadU32(
    std::span<const std::byte> bytes,
    std::size_t offset) {
    std::uint32_t value = 0U;
    for (std::size_t index = 0U; index < 4U; ++index) {
        value |=
            static_cast<std::uint32_t>(
                std::to_integer<std::uint8_t>(
                    bytes[offset + index]))
            << (index * 8U);
    }
    return value;
}

std::uint64_t LoadU64(
    std::span<const std::byte> bytes,
    std::size_t offset) {
    std::uint64_t value = 0U;
    for (std::size_t index = 0U; index < 8U; ++index) {
        value |=
            static_cast<std::uint64_t>(
                std::to_integer<std::uint8_t>(
                    bytes[offset + index]))
            << (index * 8U);
    }
    return value;
}

template <std::size_t Size>
void Fill(
    std::array<std::byte, Size>* bytes,
    std::uint8_t seed) {
    if (bytes == nullptr) {
        return;
    }
    for (std::size_t index = 0U;
         index < bytes->size();
         ++index) {
        (*bytes)[index] = static_cast<std::byte>(
            static_cast<std::uint8_t>(
                seed +
                static_cast<std::uint8_t>(index)));
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
    head[5U] = std::byte{1U};
    head[6U] = std::byte{2U};
    StoreU16(bytes, 7U, 7U);
    StoreU16(bytes, 9U, 41U);
    StoreU32(bytes, 11U, 1234U);
    StoreU64(bytes, 15U, sequence);
    return head;
}

struct RawFixture final {
    std::shared_ptr<std::vector<std::byte>> bytes;
    ingress::RawSegmentScanResult scan;
    std::vector<ingress::RawReplayRecord> replay;
};

RawFixture BuildRawFixture() {
    RawFixture fixture;
    ingress::SegmentHeaderV1 segment;
    segment.source_stream_id = 1001U;
    segment.capture_date = 20260719U;
    Fill(&segment.stream_day_id, 1U);
    segment.segment_sequence = 1U;
    segment.segment_base_wal_pos = 0U;
    segment.first_ingress_sequence = 1U;
    segment.created_realtime_ns = 100U;
    segment.created_monotonic_ns = 50U;
    Fill(&segment.host_uuid, 21U);
    Fill(&segment.linux_boot_id, 41U);
    segment.clock_epoch_algorithm = 1U;
    Fill(&segment.clock_epoch_digest, 61U);
    segment.clock_epoch_label = 1U;
    Fill(&segment.sdk_archive_sha256, 81U);
    Fill(&segment.libmdl_api_sha256, 101U);
    Fill(&segment.endpoint_contract_sha256, 121U);
    Fill(&segment.config_sha256, 141U);
    Fill(&segment.raw_schema_sha256, 161U);
    Fill(&segment.build_manifest_sha256, 181U);

    ingress::RawV1SegmentHeaderWire header_wire{};
    if (ingress::EncodeSegmentHeaderV1(
            segment, &header_wire) !=
        ingress::RawV1Error::kNone) {
        return fixture;
    }
    fixture.bytes =
        std::make_shared<std::vector<std::byte>>(
            header_wire.begin(), header_wire.end());

    const std::array<std::size_t, 4U> body_sizes{
        3U, 7U, 10U, 13U};
    for (std::size_t index = 0U;
         index < body_sizes.size();
         ++index) {
        std::vector<std::byte> body(body_sizes[index]);
        for (std::size_t body_index = 0U;
             body_index < body.size();
             ++body_index) {
            body[body_index] = static_cast<std::byte>(
                static_cast<std::uint8_t>(
                    10U + index + body_index));
        }
        ingress::RawRecordInputV1 input;
        input.meta.source_stream_id =
            segment.source_stream_id;
        input.meta.connection_epoch_hint = 9U;
        input.meta.ingress_sequence =
            static_cast<std::uint64_t>(index) + 1U;
        input.meta.recv_realtime_ns =
            1000U + static_cast<std::uint64_t>(index);
        input.meta.recv_monotonic_ns =
            900U + static_cast<std::uint64_t>(index);
        input.meta.capture_date = segment.capture_date;
        input.vendor_head = MakeVendorHead(
            static_cast<std::uint32_t>(body.size()),
            100U + static_cast<std::uint64_t>(index));
        input.vendor_body = body;
        std::vector<std::byte> record;
        if (ingress::EncodeRawRecordV1(input, &record) !=
            ingress::RawV1Error::kNone) {
            fixture.bytes.reset();
            return fixture;
        }
        fixture.bytes->insert(
            fixture.bytes->end(),
            record.begin(),
            record.end());
    }

    std::shared_ptr<const std::vector<std::byte>> immutable =
        fixture.bytes;
    fixture.scan = ingress::ScanRawSegmentV1(
        immutable,
        static_cast<std::uint64_t>(
            immutable->size()));
    if (!fixture.scan.ok()) {
        return fixture;
    }

    ingress::RawReplaySegmentContext context;
    context.source_stream_id =
        fixture.scan.segment.source_stream_id;
    context.capture_date =
        fixture.scan.segment.capture_date;
    context.stream_day_id =
        fixture.scan.segment.stream_day_id;
    context.segment_sequence =
        fixture.scan.segment.segment_sequence;
    context.clock_epoch.algorithm =
        fixture.scan.segment.clock_epoch_algorithm;
    context.clock_epoch.digest =
        fixture.scan.segment.clock_epoch_digest;
    context.clock_epoch.label =
        fixture.scan.segment.clock_epoch_label;
    fixture.replay.reserve(fixture.scan.records.size());
    for (const ingress::RawRecordView& view :
         fixture.scan.records) {
        fixture.replay.push_back(
            ingress::RawReplayRecord{
                view,
                context,
                ingress::RawReplayProvenance::kDurable});
    }
    return fixture;
}

ingress::InjectedRawPlanIdentity MakePlanIdentity(
    const ingress::RawV1Digest& raw_schema) {
    ingress::InjectedRawPlanIdentity identity;
    Fill(&identity.run_id, 201U);
    Fill(&identity.synthetic_namespace_id, 221U);
    identity.raw_schema_sha256 = raw_schema;
    identity.injected_schema_identity_sha256 =
        ingress::kInjectedRawV1SchemaSha256;
    Fill(&identity.parent_raw_identity_sha256, 91U);
    Fill(&identity.fault_rule_sha256, 131U);
    return identity;
}

std::vector<std::byte> ReadFile(
    const std::string& path) {
    std::ifstream input(path, std::ios::binary);
    if (!input) {
        return {};
    }
    const std::vector<char> chars{
        std::istreambuf_iterator<char>(input),
        std::istreambuf_iterator<char>()};
    std::vector<std::byte> bytes;
    bytes.reserve(chars.size());
    std::transform(
        chars.begin(),
        chars.end(),
        std::back_inserter(bytes),
        [](char value) {
            return static_cast<std::byte>(
                static_cast<unsigned char>(value));
        });
    return bytes;
}

void RefreshCrc(
    std::vector<std::byte>* bytes,
    std::size_t object_offset,
    std::size_t object_size,
    std::size_t crc_relative_offset) {
    if (bytes == nullptr) {
        return;
    }
    std::span<std::byte> mutable_bytes(*bytes);
    StoreU32(
        mutable_bytes,
        object_offset + crc_relative_offset,
        0U);
    const std::uint32_t crc =
        l2flow::common::ComputeCrc32c(
            std::span<const std::byte>(
                bytes->data() + object_offset,
                object_size));
    StoreU32(
        mutable_bytes,
        object_offset + crc_relative_offset,
        crc);
}

void RefreshSegmentHeader(
    std::vector<std::byte>* bytes) {
    RefreshCrc(
        bytes,
        0U,
        ingress::kInjectedRawV1SegmentHeaderBytes,
        ingress::injected_raw_v1_offset::segment::
            kHeaderCrc32c);
}

void RefreshRecordHeader(
    std::vector<std::byte>* bytes,
    std::size_t record_offset) {
    RefreshCrc(
        bytes,
        record_offset,
        ingress::kInjectedRawV1RecordHeaderBytes,
        ingress::injected_raw_v1_offset::record_header::
            kHeaderCrc32c);
}

void RefreshParentAndPayload(
    std::vector<std::byte>* bytes,
    std::size_t record_offset) {
    if (bytes == nullptr) {
        return;
    }
    const std::size_t parent_offset =
        record_offset +
        ingress::kInjectedRawV1RecordHeaderBytes;
    RefreshCrc(
        bytes,
        parent_offset,
        ingress::kInjectedRawV1ParentLocatorBytes,
        ingress::injected_raw_v1_offset::parent_locator::
            kLocatorCrc32c);
    const std::span<const std::byte> immutable(*bytes);
    const std::uint32_t vendor_message_size = LoadU32(
        immutable,
        record_offset +
            ingress::injected_raw_v1_offset::
                record_header::kVendorMessageSize);
    const std::uint32_t payload_crc =
        l2flow::common::ComputeCrc32c(
            immutable.subspan(
                parent_offset,
                ingress::kInjectedRawV1ParentLocatorBytes +
                    vendor_message_size));
    StoreU32(
        std::span<std::byte>(*bytes),
        record_offset +
            ingress::injected_raw_v1_offset::
                record_header::kPayloadCrc32c,
        payload_crc);
    RefreshRecordHeader(bytes, record_offset);
}

ingress::InjectedRawSegmentScanResult ScanCopy(
    const std::vector<std::byte>& bytes) {
    std::shared_ptr<const std::vector<std::byte>> owned =
        std::make_shared<const std::vector<std::byte>>(bytes);
    return ingress::ScanInjectedRawSegmentV1(
        std::move(owned));
}

bool BytesEqual(
    std::span<const std::byte> left,
    std::span<const std::byte> right) {
    return left.size() == right.size() &&
        std::equal(left.begin(), left.end(), right.begin());
}

bool ParentEqual(
    const ingress::InjectedRawParentLocator& left,
    const ingress::InjectedRawParentLocator& right) {
    return left.capture_date == right.capture_date &&
        left.source_stream_id == right.source_stream_id &&
        left.stream_day_id == right.stream_day_id &&
        left.ingress_sequence == right.ingress_sequence &&
        left.record_start_wal_pos ==
            right.record_start_wal_pos &&
        left.record_end_wal_pos ==
            right.record_end_wal_pos &&
        left.occurrence == right.occurrence;
}

void ExpectScanError(
    TestContext* context,
    const std::vector<std::byte>& bytes,
    ingress::InjectedRawV1Error expected,
    const std::string& description) {
    if (context == nullptr) {
        return;
    }
    const ingress::InjectedRawSegmentScanResult result =
        ScanCopy(bytes);
    context->Expect(
        result.error == expected,
        description + ": got " +
            std::string(
                ingress::InjectedRawV1ErrorName(
                    result.error)));
}

}  // namespace

int main(int argc, char** argv) {
    TestContext context;
    context.Expect(
        argc == 2,
        "schema path is passed by CTest");
    if (argc == 2) {
        const std::vector<std::byte> schema =
            ReadFile(argv[1]);
        context.Expect(
            schema.size() ==
                ingress::kInjectedRawV1SchemaBytes,
            "machine-readable schema byte length is frozen");
        context.Expect(
            l2flow::common::ComputeSha256(schema) ==
                ingress::kInjectedRawV1SchemaSha256,
            "machine-readable schema SHA-256 is frozen");
    }

    const RawFixture raw = BuildRawFixture();
    context.Expect(
        raw.bytes != nullptr && raw.scan.ok() &&
            raw.replay.size() == 4U,
        "validated RawV1 replay fixture is available");
    if (raw.bytes == nullptr || !raw.scan.ok()) {
        return 1;
    }

    ingress::RawLogicalFaultRule rule;
    rule.seed = 77U;
    rule.drop_one_in = 2U;
    rule.duplicate_one_in = 1U;
    rule.mutate_one_in = 1U;
    rule.duplicate_additional_copies = 1U;
    rule.reorder_window = 2U;
    rule.mutation =
        ingress::RawLogicalMutationKind::
            kVendorBodyByteXor;
    rule.mutation_xor_mask = std::byte{0x5aU};
    const ingress::InjectedRawPlanIdentity identity =
        MakePlanIdentity(raw.scan.segment.raw_schema_sha256);
    ingress::InjectedRawTransformPlan plan;
    const ingress::RawLogicalFaultError plan_error =
        ingress::BuildRawLogicalFaultPlan(
            raw.replay,
            identity,
            rule,
            ingress::RawLogicalFaultLimits{},
            &plan);
    context.Expect(
        plan_error == ingress::RawLogicalFaultError::kNone,
        "validated RawV1 records produce a logical plan");
    context.Expect(
        !plan.records.empty() &&
            !plan.dropped_locators.empty(),
        "golden plan contains emitted and dropped parents");
    context.Expect(
        plan.records.size() % 2U == 0U,
        "every emitted parent has two occurrences");

    std::shared_ptr<const std::vector<std::byte>> encoded;
    const ingress::InjectedRawV1Error produce_error =
        ingress::ProduceInjectedRawSegmentV1(
            plan, &encoded);
    context.Expect(
        produce_error == ingress::InjectedRawV1Error::kNone &&
            encoded != nullptr,
        "logical plan produces owned InjectedRawV1 bytes");
    if (encoded == nullptr) {
        return 1;
    }
    context.Expect(
        !std::equal(
            ingress::kInjectedRawUnframedPlanMagic.begin(),
            ingress::kInjectedRawUnframedPlanMagic.end(),
            encoded->begin()),
        "unframed plan tag is absent from wire bytes");
    context.Expect(
        std::equal(
            ingress::kInjectedRawV1SegmentMagic.begin(),
            ingress::kInjectedRawV1SegmentMagic.end(),
            encoded->begin()),
        "segment uses the independent InjectedRawV1 magic");

    const std::string golden_sha256 =
        l2flow::common::Sha256Hex(
            l2flow::common::ComputeSha256(*encoded));
    constexpr std::string_view kExpectedGoldenSha256 =
        "b931bbf36c7cc8cd4d96bf02dfe1b740bd1d5b777bb823d35b20bb9957b15c6d";
    context.Expect(
        golden_sha256 == kExpectedGoldenSha256,
        "golden segment SHA-256 is frozen (actual " +
            golden_sha256 + ")");
    constexpr std::size_t kExpectedGoldenBytes = 5'248U;
    context.Expect(
        encoded->size() == kExpectedGoldenBytes,
        "golden segment byte length is frozen (actual " +
            std::to_string(encoded->size()) + ")");

    ingress::InjectedRawSegmentScanResult scan =
        ingress::ScanInjectedRawSegmentV1(encoded);
    context.Expect(
        scan.ok(),
        "strict InjectedRawV1 reader accepts producer bytes");
    context.Expect(
        scan.segment.record_count == plan.records.size() &&
            scan.segment.dropped_locator_count ==
                plan.dropped_locators.size() &&
            scan.segment.fault_seed == rule.seed &&
            scan.segment.injected_schema_sha256 ==
                ingress::kInjectedRawV1SchemaSha256,
        "segment header preserves frozen identity and counts");
    std::uint64_t expected_parent_wal_begin =
        std::numeric_limits<std::uint64_t>::max();
    std::uint64_t expected_parent_wal_end = 0U;
    for (const ingress::InjectedRawPlanRecord& record :
         plan.records) {
        expected_parent_wal_begin = std::min(
            expected_parent_wal_begin,
            record.parent.record_start_wal_pos);
        expected_parent_wal_end = std::max(
            expected_parent_wal_end,
            record.parent.record_end_wal_pos);
    }
    for (const ingress::InjectedRawParentLocator& locator :
         plan.dropped_locators) {
        expected_parent_wal_begin = std::min(
            expected_parent_wal_begin,
            locator.record_start_wal_pos);
        expected_parent_wal_end = std::max(
            expected_parent_wal_end,
            locator.record_end_wal_pos);
    }
    context.Expect(
        scan.segment.parent_wal_begin ==
                expected_parent_wal_begin &&
            scan.segment.parent_wal_end ==
                expected_parent_wal_end,
        "segment header binds the enclosing parent Raw WAL range");
    context.Expect(
        scan.records.size() == plan.records.size() &&
            scan.dropped_locators.size() ==
                plan.dropped_locators.size() &&
            std::equal(
                scan.dropped_locators.begin(),
                scan.dropped_locators.end(),
                plan.dropped_locators.begin(),
                ParentEqual),
        "reader exposes every record and dropped locator");
    for (std::size_t index = 0U;
         index < scan.records.size() &&
         index < plan.records.size();
         ++index) {
        const ingress::InjectedRawRecordView& view =
            scan.records[index];
        const ingress::InjectedRawPlanRecord& expected =
            plan.records[index];
        context.Expect(
            view.header().synthetic_ingress_sequence ==
                    static_cast<std::uint64_t>(index) + 1U &&
                view.parent().occurrence ==
                    expected.parent.occurrence &&
                BytesEqual(
                    view.vendor_head(),
                    std::span<const std::byte>(
                        expected.vendor_head)) &&
                BytesEqual(
                    view.vendor_body(),
                    std::span<const std::byte>(
                        expected.vendor_body)),
            "reader record matches owned transform-plan data");
    }

    if (!scan.records.empty()) {
        ingress::InjectedRawRecordView retained =
            scan.records.front();
        const std::vector<std::byte> retained_body(
            retained.vendor_body().begin(),
            retained.vendor_body().end());
        scan = ingress::InjectedRawSegmentScanResult{};
        encoded.reset();
        context.Expect(
            std::vector<std::byte>(
                retained.vendor_body().begin(),
                retained.vendor_body().end()) ==
                retained_body,
            "record view owns the lifetime behind its spans");
    }

    std::shared_ptr<const std::vector<std::byte>> fresh;
    context.Expect(
        ingress::ProduceInjectedRawSegmentV1(
            plan, &fresh) ==
                ingress::InjectedRawV1Error::kNone &&
            fresh != nullptr,
        "producer is deterministic for the same plan");
    if (fresh == nullptr) {
        return 1;
    }
    const ingress::RawSegmentScanResult raw_reject =
        ingress::ScanRawSegmentV1(
            fresh,
            static_cast<std::uint64_t>(fresh->size()));
    context.Expect(
        raw_reject.error ==
                ingress::RawReaderError::
                    kSegmentHeaderInvalid &&
            raw_reject.codec_error ==
                ingress::RawV1Error::kInvalidMagic,
        "RawV1 validating reader rejects InjectedRawV1 magic");

    const std::vector<std::byte> golden = *fresh;
    const std::span<const std::byte> golden_span(golden);
    const std::size_t first_record =
        ingress::kInjectedRawV1SegmentHeaderBytes;
    const std::uint32_t first_record_size = LoadU32(
        golden_span,
        first_record +
            ingress::injected_raw_v1_offset::
                record_header::kRecordSize);
    const std::uint32_t first_body_size = LoadU32(
        golden_span,
        first_record +
            ingress::injected_raw_v1_offset::
                record_header::kVendorBodySize);
    ingress::InjectedRawRecordLayoutV1 first_layout{};
    context.Expect(
        ingress::ComputeInjectedRawRecordLayoutV1(
            first_body_size, &first_layout) ==
                ingress::InjectedRawV1Error::kNone &&
            first_layout.record_size == first_record_size,
        "record layout arithmetic matches frozen offsets");
    context.Expect(
        LoadU64(
            golden_span,
            ingress::injected_raw_v1_offset::segment::
                kRecordsOffset) ==
                ingress::kInjectedRawV1SegmentHeaderBytes &&
            LoadU64(
                golden_span,
                first_record +
                    ingress::injected_raw_v1_offset::
                        record_header::
                            kSyntheticIngressSequence) ==
                1U,
        "multi-byte fields are explicitly little-endian");

    std::vector<std::byte> damaged = golden;
    damaged[0U] ^= std::byte{1U};
    ExpectScanError(
        &context,
        damaged,
        ingress::InjectedRawV1Error::kInvalidMagic,
        "segment magic corruption is rejected");

    damaged = golden;
    damaged[
        ingress::injected_raw_v1_offset::segment::
            kFormatVersion] = std::byte{2U};
    ExpectScanError(
        &context,
        damaged,
        ingress::InjectedRawV1Error::kUnsupportedVersion,
        "unknown segment version is rejected");

    damaged = golden;
    damaged[
        ingress::injected_raw_v1_offset::segment::kEndian] =
        std::byte{2U};
    ExpectScanError(
        &context,
        damaged,
        ingress::InjectedRawV1Error::kInvalidEndian,
        "non-little-endian marker is rejected");

    damaged = golden;
    StoreU32(
        damaged,
        ingress::injected_raw_v1_offset::segment::kFlags,
        ingress::kInjectedRawV1Synthetic | 0x80000000U);
    RefreshSegmentHeader(&damaged);
    ExpectScanError(
        &context,
        damaged,
        ingress::InjectedRawV1Error::kUnknownFlags,
        "unknown segment flag is rejected after valid CRC");

    damaged = golden;
    damaged[
        ingress::injected_raw_v1_offset::segment::
            kReservedTail] = std::byte{1U};
    ExpectScanError(
        &context,
        damaged,
        ingress::InjectedRawV1Error::kNonzeroReserved,
        "nonzero segment reserved byte is rejected");

    damaged = golden;
    damaged[
        ingress::injected_raw_v1_offset::segment::
            kInjectedSchemaSha256] ^= std::byte{1U};
    RefreshSegmentHeader(&damaged);
    ExpectScanError(
        &context,
        damaged,
        ingress::InjectedRawV1Error::
            kSchemaIdentityMismatch,
        "schema digest mismatch is rejected after valid CRC");

    damaged = golden;
    StoreU64(
        damaged,
        ingress::injected_raw_v1_offset::segment::
            kRecordCount,
        ingress::kInjectedRawV1MaximumRecordCount + 1U);
    RefreshSegmentHeader(&damaged);
    ExpectScanError(
        &context,
        damaged,
        ingress::InjectedRawV1Error::kInvalidCount,
        "record count above the frozen bound is rejected");

    damaged = golden;
    damaged[
        ingress::injected_raw_v1_offset::segment::
            kHeaderCrc32c] ^= std::byte{1U};
    ExpectScanError(
        &context,
        damaged,
        ingress::InjectedRawV1Error::kHeaderCrcMismatch,
        "segment header CRC corruption is rejected");

    damaged = golden;
    StoreU64(
        damaged,
        ingress::injected_raw_v1_offset::segment::
            kParentWalBegin,
        expected_parent_wal_begin + 1U);
    RefreshSegmentHeader(&damaged);
    ExpectScanError(
        &context,
        damaged,
        ingress::InjectedRawV1Error::
            kInvalidParentLocator,
        "header parent Raw WAL range must match sidecars");

    damaged = golden;
    damaged.push_back(std::byte{0U});
    ExpectScanError(
        &context,
        damaged,
        ingress::InjectedRawV1Error::kInvalidSize,
        "trailing segment byte is rejected");

    damaged = golden;
    StoreU32(
        damaged,
        first_record +
            ingress::injected_raw_v1_offset::
                record_header::kFlags,
        0x80000000U);
    RefreshRecordHeader(&damaged, first_record);
    ExpectScanError(
        &context,
        damaged,
        ingress::InjectedRawV1Error::kUnknownFlags,
        "unknown record flag is rejected after valid CRC");

    damaged = golden;
    damaged[
        first_record +
        ingress::injected_raw_v1_offset::record_header::
            kReservedTail] = std::byte{1U};
    ExpectScanError(
        &context,
        damaged,
        ingress::InjectedRawV1Error::kNonzeroReserved,
        "nonzero record reserved byte is rejected");

    damaged = golden;
    StoreU64(
        damaged,
        first_record +
            ingress::injected_raw_v1_offset::
                record_header::
                    kSyntheticIngressSequence,
        2U);
    RefreshRecordHeader(&damaged, first_record);
    const std::size_t first_trailer =
        first_record +
        static_cast<std::size_t>(first_record_size) -
        ingress::kInjectedRawV1RecordTrailerBytes;
    StoreU64(
        damaged,
        first_trailer +
            ingress::injected_raw_v1_offset::
                record_trailer::
                    kSyntheticIngressSequence,
        2U);
    RefreshCrc(
        &damaged,
        first_trailer,
        ingress::kInjectedRawV1RecordTrailerBytes,
        ingress::injected_raw_v1_offset::record_trailer::
            kTrailerCrc32c);
    ExpectScanError(
        &context,
        damaged,
        ingress::InjectedRawV1Error::kInvalidSequence,
        "non-continuous synthetic sequence is rejected");

    const std::size_t first_parent =
        first_record +
        ingress::kInjectedRawV1RecordHeaderBytes;
    damaged = golden;
    damaged[
        first_parent +
        ingress::injected_raw_v1_offset::parent_locator::
            kReserved] = std::byte{1U};
    ExpectScanError(
        &context,
        damaged,
        ingress::InjectedRawV1Error::kNonzeroReserved,
        "nonzero parent sidecar reserved byte is rejected");

    damaged = golden;
    damaged[
        first_parent +
        ingress::injected_raw_v1_offset::parent_locator::
            kLocatorCrc32c] ^= std::byte{1U};
    ExpectScanError(
        &context,
        damaged,
        ingress::InjectedRawV1Error::
            kParentLocatorCrcMismatch,
        "parent sidecar CRC corruption is rejected");

    damaged = golden;
    const std::size_t first_vendor_body =
        first_parent +
        ingress::kInjectedRawV1ParentLocatorBytes +
        ingress::kVendorMessageHeadBytes;
    damaged[first_vendor_body] ^= std::byte{1U};
    ExpectScanError(
        &context,
        damaged,
        ingress::InjectedRawV1Error::kPayloadCrcMismatch,
        "payload corruption is rejected");

    damaged = golden;
    const std::size_t first_padding =
        first_vendor_body +
        static_cast<std::size_t>(first_body_size);
    context.Expect(
        first_layout.padding_size != 0U,
        "golden first record contains padding");
    damaged[first_padding] = std::byte{1U};
    ExpectScanError(
        &context,
        damaged,
        ingress::InjectedRawV1Error::kNonzeroPadding,
        "nonzero record padding is rejected");

    damaged = golden;
    damaged[first_trailer] ^= std::byte{1U};
    ExpectScanError(
        &context,
        damaged,
        ingress::InjectedRawV1Error::kTrailerMismatch,
        "trailer magic corruption is rejected");

    const ingress::InjectedRawSegmentScanResult golden_scan =
        ScanCopy(golden);
    auto occurrence_two = std::find_if(
        golden_scan.records.begin(),
        golden_scan.records.end(),
        [](const ingress::InjectedRawRecordView& record) {
            return record.parent().occurrence == 2U;
        });
    context.Expect(
        occurrence_two != golden_scan.records.end(),
        "golden contains a duplicate occurrence");
    if (occurrence_two != golden_scan.records.end()) {
        const std::size_t duplicate_record =
            static_cast<std::size_t>(
                occurrence_two->record_start_offset());
        const std::size_t duplicate_parent =
            duplicate_record +
            ingress::kInjectedRawV1RecordHeaderBytes;
        damaged = golden;
        StoreU32(
            damaged,
            duplicate_parent +
                ingress::injected_raw_v1_offset::
                    parent_locator::kOccurrence,
            3U);
        RefreshParentAndPayload(
            &damaged, duplicate_record);
        ExpectScanError(
            &context,
            damaged,
            ingress::InjectedRawV1Error::
                kInvalidParentOccurrence,
            "non-contiguous parent occurrence is rejected");
    }

    ingress::InjectedRawTransformPlan bad_plan = plan;
    bad_plan.identity.injected_schema_identity_sha256[0U] ^=
        std::byte{1U};
    std::shared_ptr<const std::vector<std::byte>> rejected;
    context.Expect(
        ingress::ProduceInjectedRawSegmentV1(
            bad_plan, &rejected) ==
                ingress::InjectedRawV1Error::
                    kSchemaIdentityMismatch &&
            rejected == nullptr,
        "producer requires the exact frozen schema digest");

    bad_plan = plan;
    bad_plan.identity.plan_magic[0U] ^= std::byte{1U};
    context.Expect(
        ingress::ProduceInjectedRawSegmentV1(
            bad_plan, &rejected) ==
                ingress::InjectedRawV1Error::
                    kInvalidIdentity,
        "producer accepts only the typed plan tag seam");

    bad_plan = plan;
    if (!bad_plan.records.empty()) {
        bad_plan.records.front().
            synthetic_ingress_sequence = 2U;
        context.Expect(
            ingress::ProduceInjectedRawSegmentV1(
                bad_plan, &rejected) ==
                    ingress::InjectedRawV1Error::
                        kInvalidSequence,
            "producer rejects a discontinuous plan sequence");
    }

    ingress::InjectedRawTransformPlan empty_plan;
    empty_plan.identity = identity;
    std::shared_ptr<const std::vector<std::byte>> empty_wire;
    context.Expect(
        ingress::ProduceInjectedRawSegmentV1(
            empty_plan, &empty_wire) ==
                ingress::InjectedRawV1Error::kNone &&
            empty_wire != nullptr &&
            empty_wire->size() ==
                ingress::kInjectedRawV1SegmentHeaderBytes,
        "empty plan has one valid header-only representation");
    if (empty_wire != nullptr) {
        const ingress::InjectedRawSegmentScanResult
            empty_scan =
                ingress::ScanInjectedRawSegmentV1(
                    empty_wire);
        context.Expect(
            empty_scan.ok() &&
                empty_scan.segment.record_count == 0U &&
                empty_scan.segment.
                        dropped_locator_count ==
                    0U &&
                empty_scan.segment.parent_wal_begin == 0U &&
                empty_scan.segment.parent_wal_end == 0U,
            "empty segment freezes zero parent range");
    }

    ingress::InjectedRawRecordLayoutV1 maximum_layout{};
    context.Expect(
        ingress::ComputeInjectedRawRecordLayoutV1(
            ingress::
                kInjectedRawV1MaximumVendorBodyBytes,
            &maximum_layout) ==
                ingress::InjectedRawV1Error::kNone &&
            maximum_layout.record_size ==
                ingress::
                    kInjectedRawV1MaximumRecordBytes,
        "maximum record layout exactly matches frozen bound");
    context.Expect(
        ingress::ComputeInjectedRawRecordLayoutV1(
            static_cast<std::size_t>(
                ingress::
                    kInjectedRawV1MaximumVendorBodyBytes) +
                1U,
            &maximum_layout) ==
            ingress::InjectedRawV1Error::
                kResourceLimitExceeded,
        "vendor body above frozen bound is rejected");

    if (context.failures != 0) {
        std::cerr << context.failures
                  << " InjectedRawV1 test(s) failed\n";
        return 1;
    }
    std::cout << "InjectedRawV1 schema/codec/reader tests passed\n";
    return 0;
}
