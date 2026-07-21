#include "l2flow/common/sha256.h"
#include "l2flow/ingress/finalization_report_v1.h"
#include "l2flow/ingress/raw_manifest_transition.h"
#include "l2flow/ingress/raw_schema.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <iterator>
#include <memory>
#include <string>
#include <type_traits>

namespace common = l2flow::common;
namespace ingress = l2flow::ingress;

namespace {

static_assert(
    !std::is_default_constructible_v<
        ingress::BuiltFinalizationReportV1>);
static_assert(
    !std::is_copy_constructible_v<
        ingress::BuiltFinalizationReportV1>);
static_assert(
    !std::is_move_constructible_v<
        ingress::BuiltFinalizationReportV1>);

struct TestContext final {
    void Expect(bool condition, const char* description) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << description << '\n';
        }
    }
    int failures = 0;
};

template <std::size_t Size>
std::array<std::byte, Size> Pattern(std::uint8_t start) {
    std::array<std::byte, Size> value{};
    for (std::size_t index = 0U; index < Size; ++index) {
        value[index] = static_cast<std::byte>(
            static_cast<std::uint8_t>(
                start +
                static_cast<std::uint8_t>(index)));
    }
    return value;
}

ingress::SegmentHeaderV1 MakeSegment() {
    ingress::SegmentHeaderV1 segment{};
    segment.source_stream_id = 1001U;
    segment.capture_date = 20260718U;
    segment.stream_day_id = Pattern<16U>(0x01U);
    segment.segment_sequence = 1U;
    segment.segment_base_wal_pos = 0U;
    segment.first_ingress_sequence = 1U;
    segment.created_realtime_ns = 11U;
    segment.created_monotonic_ns = 12U;
    segment.host_uuid = Pattern<16U>(0x21U);
    segment.linux_boot_id = Pattern<16U>(0x31U);
    segment.clock_epoch_algorithm = 1U;
    segment.clock_epoch_digest = Pattern<32U>(0x41U);
    segment.clock_epoch_label = 77U;
    segment.sdk_archive_sha256 = Pattern<32U>(0x61U);
    segment.libmdl_api_sha256 = Pattern<32U>(0x81U);
    segment.endpoint_contract_sha256 =
        Pattern<32U>(0xa1U);
    segment.config_sha256 = Pattern<32U>(0xc1U);
    segment.raw_schema_sha256 =
        ingress::kFrozenRawSchemaSha256;
    segment.build_manifest_sha256 =
        Pattern<32U>(0x21U);
    return segment;
}

ingress::RawV1JournalHeaderWire MakeJournal(
    const ingress::SegmentHeaderV1& segment) {
    ingress::DurableJournalHeaderV1 journal{};
    journal.capture_date = segment.capture_date;
    journal.source_stream_id = segment.source_stream_id;
    journal.stream_day_id = segment.stream_day_id;
    journal.raw_schema_sha256 =
        ingress::kFrozenRawSchemaSha256;
    journal.created_host_uuid = segment.host_uuid;
    journal.created_linux_boot_id =
        segment.linux_boot_id;
    journal.created_clock_epoch_algorithm =
        segment.clock_epoch_algorithm;
    journal.created_clock_epoch_digest =
        segment.clock_epoch_digest;
    journal.created_clock_epoch_label =
        segment.clock_epoch_label;
    ingress::RawV1JournalHeaderWire wire{};
    static_cast<void>(
        ingress::EncodeDurableJournalHeaderV1(
            journal, &wire));
    return wire;
}

ingress::RawWalWriterSnapshot MakeInitialSnapshot() {
    ingress::RawWalWriterSnapshot snapshot{};
    snapshot.append = {
        ingress::kRawV1SegmentHeaderBytes,
        0U,
        ingress::kRawV1SegmentHeaderBytes};
    snapshot.durable = snapshot.append;
    snapshot.journal_logical_size =
        ingress::kRawV1JournalHeaderBytes +
        ingress::kRawV1DurableMarkerBytes;
    snapshot.initialized = true;
    return snapshot;
}

struct SealedFixture final {
    ingress::FinalizationReportV1 report{};
    std::unique_ptr<
        ingress::BuiltSealedRawCertificateV1>
        certificate;
};

SealedFixture MakeSealedFixture(TestContext* test) {
    const ingress::SegmentHeaderV1 segment = MakeSegment();
    const ingress::RawV1JournalHeaderWire journal =
        MakeJournal(segment);

    ingress::RawManifestV1 open{};
    test->Expect(
        ingress::BuildFreshOpenRawManifestV1(
            segment, MakeInitialSnapshot(), &open) ==
            ingress::RawManifestTransitionErrorV1::kNone,
        "fresh manifest fixture builds");

    ingress::RawV1SegmentHeaderWire segment_wire{};
    static_cast<void>(
        ingress::EncodeSegmentHeaderV1(
            segment, &segment_wire));
    ingress::RawSealedSegmentMetadataV1 metadata{};
    metadata.segment = segment;
    metadata.segment_sha256 =
        common::ComputeSha256(segment_wire);
    metadata.index_sha256 = Pattern<32U>(0xe1U);
    metadata.logical_end_offset =
        ingress::kRawV1SegmentHeaderBytes;
    ingress::DurableMarkerV1 marker{};
    marker.source_stream_id = segment.source_stream_id;
    marker.segment_sequence = segment.segment_sequence;
    marker.durable_global_wal_pos =
        ingress::kRawV1SegmentHeaderBytes;
    marker.durable_ingress_sequence = 0U;
    marker.durable_segment_offset =
        ingress::kRawV1SegmentHeaderBytes;
    marker.marker_flags = ingress::kRawV1SegmentSealed;
    static_cast<void>(
        ingress::EncodeDurableMarkerV1(
            marker,
            &metadata.accepted_sealed_marker_bytes));
    static_cast<void>(
        ingress::DecodeDurableMarkerV1(
            metadata.accepted_sealed_marker_bytes,
            &metadata.accepted_sealed_marker));

    ingress::RawManifestV1 closed{};
    test->Expect(
        ingress::TransitionOpenRawManifestToClosedV1(
            open, metadata, &closed) ==
            ingress::RawManifestTransitionErrorV1::kNone,
        "closed manifest fixture builds");

    ingress::RawWalSinkIdentityV1 sink{};
    sink.writer_instance = Pattern<16U>(0xf0U);
    sink.stream_day_id = segment.stream_day_id;
    sink.source_stream_id = segment.source_stream_id;
    sink.capture_date = segment.capture_date;
    sink.segment_sequence = segment.segment_sequence;
    sink.segment_base_wal_pos =
        segment.segment_base_wal_pos;
    sink.first_ingress_sequence =
        segment.first_ingress_sequence;

    ingress::RawWalWriterSnapshot final_wal =
        MakeInitialSnapshot();
    final_wal.journal_logical_size +=
        ingress::kRawV1DurableMarkerBytes;
    final_wal.sealed = true;
    final_wal.closed = true;

    SealedFixture fixture{};
    test->Expect(
        ingress::BuildSealedRawCertificateCapabilityV1(
            journal,
            closed,
            sink,
            final_wal,
            &fixture.certificate) ==
                ingress::SealedRawCertificateV1Error::kNone &&
            fixture.certificate != nullptr,
        "sealed certificate capability fixture builds");
    if (fixture.certificate == nullptr) {
        return fixture;
    }
    const auto& certificate =
        fixture.certificate->model();
    auto& report = fixture.report;
    report.reserve_state_uuid = Pattern<16U>(0x10U);
    report.finalization_cycle_id =
        Pattern<16U>(0x30U);
    report.namespace_identity =
        certificate.namespace_identity;
    report.ack_status = ingress::ReserveAckStatusV1::kAcked;
    report.ack_writer_instance =
        Pattern<16U>(0x50U);
    report.immutable_grant_sha256 =
        Pattern<32U>(0x70U);
    const ingress::FinalizationReportCursorV1 cursor{
        certificate.terminal_append_cursor
            .segment_sequence,
        certificate.terminal_append_cursor
            .global_wal_pos,
        certificate.terminal_append_cursor
            .ingress_sequence,
        certificate.terminal_append_cursor
            .segment_offset};
    report.initial_append_cursor = cursor;
    report.initial_durable_cursor = cursor;
    report.final_append_cursor = cursor;
    report.final_durable_cursor = cursor;
    report.tail_classification =
        ingress::FinalizationReportTailClassificationV1::
            kNone;
    report.tail_classification_valid = true;
    report.gap_classification =
        ingress::FinalizationReportGapClassificationV1::
            kAckedRingDrained;
    report.gap_classification_valid = true;
    report.raw_allocation_delta_before_report = {
        8192U, 4096U, 3U, 1U};
    report.result =
        ingress::FinalizationReportResultV1::kSealedRaw;
    report.segments.push_back(
        {ingress::FinalizationReportSegmentKindV1::
             kCurrent,
         certificate.last_segment_sequence,
         certificate.last_segment_base_wal_pos,
         certificate.last_segment_logical_length,
         certificate.last_segment_sha256});
    report.final_seal =
        ingress::FinalizationReportSealV1{
            certificate.last_segment_sequence,
            certificate.accepted_sealed_marker_bytes,
            certificate.accepted_sealed_marker_sha256};
    report.manifest_frontier =
        ingress::FinalizationReportManifestFrontierV1{
            certificate.closed_entry_count,
            certificate.last_segment_sequence,
            certificate.last_segment_sha256,
            certificate.accepted_sealed_marker_sha256,
            certificate.closed_prefix_sha256};
    report.sealed_raw_certificate_sha256 =
        fixture.certificate->certificate_sha256();
    report.preexisting_recovery_report =
        ingress::FinalizationPreexistingRecoveryReportV1{
            Pattern<16U>(0x90U),
            Pattern<32U>(0xb0U)};
    return fixture;
}

std::unique_ptr<ingress::BuiltEmptyAnchorTombstoneV1>
MakeTombstone(TestContext* test) {
    const ingress::SegmentHeaderV1 segment = MakeSegment();
    ingress::EmptyAnchorObservationV1 observation{};
    observation.journal_header_bytes =
        MakeJournal(segment);
    observation.journal_logical_size =
        ingress::kRawV1JournalHeaderBytes;
    std::unique_ptr<
        ingress::BuiltEmptyAnchorTombstoneV1>
        tombstone;
    test->Expect(
        ingress::BuildEmptyAnchorTombstoneCapabilityV1(
            observation, &tombstone) ==
                ingress::EmptyAnchorTombstoneV1Error::kNone &&
            tombstone != nullptr,
        "empty tombstone fixture builds");
    return tombstone;
}

void TestSealedReport(TestContext* test) {
    SealedFixture fixture = MakeSealedFixture(test);
    if (fixture.certificate == nullptr) {
        return;
    }
    std::unique_ptr<ingress::BuiltFinalizationReportV1>
        built;
    test->Expect(
        ingress::BuildFinalizationReportCapabilityV1(
            fixture.report,
            ingress::kReserveGrantRawFinalization,
            fixture.certificate.get(),
            nullptr,
            &built) ==
                ingress::FinalizationReportV1Error::kNone &&
            built != nullptr,
        "SEALED_RAW builds a private validated capability");
    if (built == nullptr) {
        return;
    }
    test->Expect(
        built->filename() ==
            "finalization-"
            "303132333435363738393a3b3c3d3e3f"
            ".json",
        "finalization locator derives only from cycle identity");
    test->Expect(
        built->canonical_jcs().size() <=
                ingress::kFinalizationReportV1MaximumBytes &&
            built->canonical_jcs().find('\n') ==
                std::string_view::npos &&
            built->canonical_jcs().find(
                "\"journal_header_sha256\":null") !=
                std::string_view::npos &&
            built->canonical_jcs().find(
                "\"empty_anchor_tombstone_sha256\":null") !=
                std::string_view::npos,
        "SEALED_RAW JCS is bounded and carries exact nullability");

    const std::string golden = common::Sha256Hex(
        common::ComputeSha256(built->canonical_jcs()));
    if (golden !=
        "9827bbf96f0096cbaa159cb4780b46d5"
        "5050f545b973477bc6daa76686a196fb") {
        std::cerr << "observed finalization report golden digest: "
                  << golden << '\n';
    }
    test->Expect(
        golden ==
            "9827bbf96f0096cbaa159cb4780b46d5"
            "5050f545b973477bc6daa76686a196fb",
        "SEALED_RAW complete-byte JCS golden SHA-256");
    test->Expect(
        built->report_sha256() ==
            common::ComputeSha256(built->canonical_jcs()),
        "capability freezes exact report content hash");

    ingress::FinalizationReportV1 parsed{};
    test->Expect(
        ingress::ParseFinalizationReportV1Jcs(
            built->canonical_jcs(), &parsed) ==
                ingress::FinalizationReportV1Error::kNone &&
            parsed.final_seal.has_value() &&
            parsed.final_seal
                    ->accepted_sealed_marker_sha256 ==
                fixture.report.final_seal
                    ->accepted_sealed_marker_sha256 &&
            parsed.preexisting_recovery_report.has_value(),
        "strict parser round-trips sealed causal facts");

    const auto sentinel_cycle = parsed.finalization_cycle_id;
    const std::string unknown =
        std::string(built->canonical_jcs().substr(
            0U, built->canonical_jcs().size() - 1U)) +
        ",\"x\":0}";
    test->Expect(
        ingress::ParseFinalizationReportV1Jcs(
            unknown, &parsed) ==
                ingress::FinalizationReportV1Error::
                    kInvalidCanonicalJson &&
            parsed.finalization_cycle_id == sentinel_cycle,
        "unknown field is rejected without modifying output");
    std::string leading_zero(built->canonical_jcs());
    const std::string canonical_allocation =
        "\"allocated_bytes\":\"8192\"";
    const std::size_t allocation =
        leading_zero.find(canonical_allocation);
    test->Expect(
        allocation != std::string::npos,
        "golden contains a canonical uint64 string");
    if (allocation != std::string::npos) {
        leading_zero.replace(
            allocation,
            canonical_allocation.size(),
            "\"allocated_bytes\":\"08192\"");
        test->Expect(
            ingress::ParseFinalizationReportV1Jcs(
                leading_zero, &parsed) ==
                    ingress::FinalizationReportV1Error::
                        kInvalidCanonicalJson &&
                parsed.finalization_cycle_id ==
                    sentinel_cycle,
            "leading-zero uint64 is rejected without modifying output");
    }
    std::string uppercase(built->canonical_jcs());
    const std::string lowercase_hex = "7a7b";
    const std::size_t lowercase = uppercase.find(lowercase_hex);
    test->Expect(
        lowercase != std::string::npos,
        "golden contains lowercase hexadecimal");
    if (lowercase != std::string::npos) {
        uppercase.replace(lowercase, 4U, "7A7b");
        test->Expect(
            ingress::ParseFinalizationReportV1Jcs(
                uppercase, &parsed) ==
                    ingress::FinalizationReportV1Error::
                        kInvalidCanonicalJson &&
                parsed.finalization_cycle_id ==
                    sentinel_cycle,
            "uppercase hexadecimal is rejected without modifying output");
    }
    const std::string oversized(
        ingress::kFinalizationReportV1MaximumBytes + 1U,
        'x');
    test->Expect(
        ingress::ParseFinalizationReportV1Jcs(
            oversized, &parsed) ==
                ingress::FinalizationReportV1Error::
                    kEncodedSizeExceeded &&
            parsed.finalization_cycle_id == sentinel_cycle,
        "oversized bytes are rejected before parsing and leave output unchanged");

    ingress::FinalizationReportV1 invalid =
        fixture.report;
    invalid.segments.push_back(invalid.segments.front());
    invalid.segments.push_back(invalid.segments.front());
    std::string output = "sentinel";
    test->Expect(
        ingress::EncodeFinalizationReportV1Jcs(
            invalid, &output) ==
                ingress::FinalizationReportV1Error::
                    kInvalidResultShape &&
            output == "sentinel",
        "segment bound failure leaves encode output unchanged");

    const auto* const original_built = built.get();
    test->Expect(
        ingress::BuildFinalizationReportCapabilityV1(
            fixture.report,
            ingress::kReserveGrantRawAnchorOnly,
            fixture.certificate.get(),
            nullptr,
            &built) ==
                ingress::FinalizationReportV1Error::
                    kGrantKindMismatch &&
            built.get() == original_built,
        "grant mismatch cannot replace an existing built capability");
}

void TestEmptyReport(TestContext* test) {
    auto tombstone = MakeTombstone(test);
    if (tombstone == nullptr) {
        return;
    }
    ingress::FinalizationReportV1 report{};
    report.reserve_state_uuid = Pattern<16U>(0x10U);
    report.finalization_cycle_id = Pattern<16U>(0x30U);
    const auto& tombstone_model = tombstone->model();
    report.namespace_identity = {
        tombstone_model.namespace_identity.capture_date,
        tombstone_model.namespace_identity.source_stream_id,
        tombstone_model.namespace_identity.stream_day_id};
    report.ack_status =
        ingress::ReserveAckStatusV1::kFencedNoAck;
    report.immutable_grant_sha256 =
        Pattern<32U>(0x70U);
    report.tail_classification =
        ingress::FinalizationReportTailClassificationV1::
            kNone;
    report.tail_classification_valid = true;
    report.gap_classification =
        ingress::FinalizationReportGapClassificationV1::
            kNone;
    report.gap_classification_valid = true;
    report.result =
        ingress::FinalizationReportResultV1::
            kEmptyAnchorOnly;
    report.journal_header_sha256 =
        tombstone_model.journal_header_sha256;
    report.marker_count = 0U;
    report.empty_anchor_tombstone_sha256 =
        tombstone->tombstone_sha256();

    std::unique_ptr<ingress::BuiltFinalizationReportV1>
        built;
    test->Expect(
        ingress::BuildFinalizationReportCapabilityV1(
            report,
            ingress::kReserveGrantRawAnchorOnly,
            nullptr,
            tombstone.get(),
            &built) ==
                ingress::FinalizationReportV1Error::kNone &&
            built != nullptr,
        "EMPTY_ANCHOR_ONLY builds only with matching tombstone");
    if (built == nullptr) {
        return;
    }
    test->Expect(
        built->canonical_jcs().find(
            "\"final_append_cursor\":null") !=
                std::string_view::npos &&
            built->canonical_jcs().find(
                "\"manifest_frontier\":null") !=
                std::string_view::npos &&
            built->canonical_jcs().find(
                "\"segments\":null") !=
                std::string_view::npos &&
            built->canonical_jcs().find(
                "\"sealed_raw_certificate_sha256\":null") !=
                std::string_view::npos,
        "empty result nulls every cursor/segment/seal/frontier/certificate field");

    ingress::FinalizationReportV1 wrong = report;
    wrong.marker_count = 1U;
    test->Expect(
        ingress::ValidateFinalizationReportV1(wrong) ==
            ingress::FinalizationReportV1Error::
                kInvalidResultShape,
        "empty result cannot claim a marker");
}

void TestSchema(TestContext* test) {
    const std::string path =
        std::string(L2FLOW_SOURCE_DIR) +
        "/schemas/finalization_report_v1.json";
    std::ifstream input(path, std::ios::binary);
    const std::string bytes{
        std::istreambuf_iterator<char>(input),
        std::istreambuf_iterator<char>()};
    test->Expect(
        input.good() || input.eof(),
        "finalization report schema is readable");
    const std::string digest =
        common::Sha256Hex(common::ComputeSha256(bytes));
    test->Expect(
        bytes.size() ==
            ingress::kFinalizationReportV1SchemaBytes,
        "schema byte count is frozen");
    test->Expect(
        digest ==
            ingress::kFinalizationReportV1SchemaSha256Hex,
        "schema SHA-256 is frozen");
}

}  // namespace

int main() {
    TestContext test;
    TestSealedReport(&test);
    TestEmptyReport(&test);
    TestSchema(&test);
    if (test.failures != 0) {
        std::cerr << test.failures
                  << " finalization report assertion(s) failed\n";
        return 1;
    }
    std::cout << "finalization report tests passed\n";
    return 0;
}
