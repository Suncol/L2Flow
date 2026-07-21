#include "l2flow/common/sha256.h"
#include "l2flow/ingress/raw_manifest_transition.h"
#include "l2flow/ingress/raw_recovery_maintenance_report_receipt.h"
#include "l2flow/ingress/raw_recovery_maintenance_report_v1.h"
#include "l2flow/ingress/raw_schema.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

namespace common = l2flow::common;
namespace ingress = l2flow::ingress;

namespace {

static_assert(
    !std::is_default_constructible_v<
        ingress::BuiltRecoveryMaintenanceReportV1>);
static_assert(
    !std::is_copy_constructible_v<
        ingress::BuiltRecoveryMaintenanceReportV1>);
static_assert(
    !std::is_move_constructible_v<
        ingress::BuiltRecoveryMaintenanceReportV1>);
static_assert(
    !std::is_default_constructible_v<
        ingress::RecoveryMaintenanceReportReceiptV1>);

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
std::array<std::byte, Size> Pattern(
    std::uint8_t start) {
    std::array<std::byte, Size> result{};
    for (std::size_t index = 0U;
         index < Size;
         ++index) {
        result[index] = static_cast<std::byte>(
            static_cast<std::uint8_t>(
                start +
                static_cast<std::uint8_t>(index)));
    }
    return result;
}

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
        const unsigned shift =
            static_cast<unsigned>(index * 8U);
        bytes[offset + index] =
            static_cast<std::byte>(
                (value >> shift) & 0xffU);
    }
}

void PutU64(
    std::span<std::byte> bytes,
    std::size_t offset,
    std::uint64_t value) {
    for (std::size_t index = 0U;
         index < sizeof(value);
         ++index) {
        const unsigned shift =
            static_cast<unsigned>(index * 8U);
        bytes[offset + index] =
            static_cast<std::byte>(
                (value >> shift) & 0xffU);
    }
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
            ingress::kVendorMessageHeadBytes +
            body_size));
    head[5U] = std::byte{0x01};
    head[6U] = std::byte{0x02};
    PutU16(bytes, 7U, 0x1234U);
    PutU16(bytes, 9U, 0x5678U);
    PutU32(bytes, 11U, 0x89abcdefU);
    PutU64(
        bytes, 15U, 0x0123456789abcdefULL);
    return head;
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
    segment.clock_epoch_digest =
        Pattern<32U>(0x41U);
    segment.clock_epoch_label = 77U;
    segment.sdk_archive_sha256 =
        Pattern<32U>(0x61U);
    segment.libmdl_api_sha256 =
        Pattern<32U>(0x81U);
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
    journal.source_stream_id =
        segment.source_stream_id;
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

ingress::RawSealedSegmentMetadataV1
MakeSealedMetadata(
    const ingress::SegmentHeaderV1& segment) {
    ingress::RawV1SegmentHeaderWire header_bytes{};
    static_cast<void>(
        ingress::EncodeSegmentHeaderV1(
            segment, &header_bytes));
    ingress::RawSealedSegmentMetadataV1 metadata{};
    metadata.segment = segment;
    metadata.segment_sha256 =
        common::ComputeSha256(header_bytes);
    metadata.index_sha256 =
        Pattern<32U>(0xe1U);
    metadata.logical_end_offset =
        ingress::kRawV1SegmentHeaderBytes;
    ingress::DurableMarkerV1 marker{};
    marker.source_stream_id =
        segment.source_stream_id;
    marker.segment_sequence =
        segment.segment_sequence;
    marker.durable_global_wal_pos =
        segment.segment_base_wal_pos +
        ingress::kRawV1SegmentHeaderBytes;
    marker.durable_ingress_sequence =
        segment.first_ingress_sequence - 1U;
    marker.durable_segment_offset =
        ingress::kRawV1SegmentHeaderBytes;
    marker.marker_flags =
        ingress::kRawV1SegmentSealed;
    static_cast<void>(
        ingress::EncodeDurableMarkerV1(
            marker,
            &metadata.accepted_sealed_marker_bytes));
    static_cast<void>(
        ingress::DecodeDurableMarkerV1(
            metadata.accepted_sealed_marker_bytes,
            &metadata.accepted_sealed_marker));
    return metadata;
}

struct Fixture final {
    ingress::SegmentHeaderV1 segment = MakeSegment();
    ingress::RawV1JournalHeaderWire journal =
        MakeJournal(segment);
    std::shared_ptr<const std::vector<std::byte>>
        segment_bytes;
    ingress::RawManifestV1 manifest{};
    ingress::RawRecoveryPlanV1 analysis{};
    ingress::RawRecoveryExecutionResultV1 execution{};
    ingress::RawWalSinkIdentityV1 sink{};
    ingress::RawWalWriterSnapshot wal{};
    ingress::RawReserveRegistryEntryKeyV1 key{};
};

Fixture MakeFixture(bool has_initial_marker) {
    Fixture fixture{};
    ingress::RawV1SegmentHeaderWire segment_wire{};
    static_cast<void>(
        ingress::EncodeSegmentHeaderV1(
            fixture.segment, &segment_wire));
    auto bytes =
        std::make_shared<std::vector<std::byte>>(
            segment_wire.begin(), segment_wire.end());
    fixture.segment_bytes = bytes;

    fixture.wal.append = {
        ingress::kRawV1SegmentHeaderBytes,
        0U,
        ingress::kRawV1SegmentHeaderBytes};
    fixture.wal.durable = fixture.wal.append;
    fixture.wal.journal_logical_size =
        ingress::kRawV1JournalHeaderBytes +
        ingress::kRawV1DurableMarkerBytes;
    fixture.wal.initialized = true;
    static_cast<void>(
        ingress::BuildFreshOpenRawManifestV1(
            fixture.segment,
            fixture.wal,
            &fixture.manifest));

    static_cast<void>(
        ingress::DecodeDurableJournalHeaderV1(
            fixture.journal,
            &fixture.analysis.journal_header));
    fixture.analysis.accepted_journal_size =
        has_initial_marker
            ? fixture.wal.journal_logical_size
            : ingress::kRawV1JournalHeaderBytes;
    fixture.analysis.has_accepted_cursor =
        has_initial_marker;
    if (has_initial_marker) {
        fixture.analysis.accepted_cursor = {
            1U,
            ingress::kRawV1SegmentHeaderBytes,
            0U,
            ingress::kRawV1SegmentHeaderBytes,
            0U};
    } else {
        fixture.analysis.initial_anchor =
            ingress::RawRecoveryInitialAnchorV1::
                kSegmentHeaderOnly;
    }
    ingress::RawRecoverySegmentPlanV1 plan{};
    plan.segment_sequence = 1U;
    plan.segment_base_wal_pos = 0U;
    plan.has_accepted_marker = has_initial_marker;
    plan.accepted_marker_wire =
        fixture.manifest.open_entry
            ->accepted_marker_bytes;
    plan.durable_end_offset =
        ingress::kRawV1SegmentHeaderBytes;
    plan.validated_logical_end_offset =
        ingress::kRawV1SegmentHeaderBytes;
    plan.validated_last_ingress_sequence = 0U;
    plan.append_only_begin_offset =
        ingress::kRawV1SegmentHeaderBytes;
    plan.append_only_end_offset =
        ingress::kRawV1SegmentHeaderBytes;
    plan.tail_begin_offset =
        ingress::kRawV1SegmentHeaderBytes;
    plan.tail_end_offset =
        ingress::kRawV1SegmentHeaderBytes;
    fixture.analysis.segments.push_back(plan);

    fixture.execution.cursor_publishable = true;
    fixture.execution.retained_journal_size =
        fixture.wal.journal_logical_size;
    fixture.execution.recovered_cursor = {
        1U,
        ingress::kRawV1SegmentHeaderBytes,
        0U,
        ingress::kRawV1SegmentHeaderBytes,
        0U};

    fixture.sink.writer_instance =
        Pattern<16U>(0xe0U);
    fixture.sink.stream_day_id =
        fixture.segment.stream_day_id;
    fixture.sink.source_stream_id =
        fixture.segment.source_stream_id;
    fixture.sink.capture_date =
        fixture.segment.capture_date;
    fixture.sink.segment_sequence = 1U;
    fixture.sink.segment_base_wal_pos = 0U;
    fixture.sink.first_ingress_sequence = 1U;

    fixture.key.route.source_stream_id =
        fixture.segment.source_stream_id;
    fixture.key.route.capture_date =
        fixture.segment.capture_date;
    fixture.key.stream_day_id =
        fixture.segment.stream_day_id;
    fixture.key.recovery_attempt_id =
        Pattern<16U>(0xf0U);
    return fixture;
}

void TestReuseOpen(TestContext* test) {
    Fixture fixture = MakeFixture(true);
    std::unique_ptr<
        ingress::BuiltRecoveryMaintenanceReportV1>
        built;
    const auto result =
        ingress::BuildResumedOpenRecoveryMaintenanceReportV1(
            fixture.key,
            fixture.journal,
            fixture.analysis,
            fixture.execution,
            fixture.wal.journal_logical_size,
            fixture.manifest,
            fixture.segment_bytes,
            fixture.sink,
            fixture.wal,
            nullptr,
            &built);
    test->Expect(
        result ==
                ingress::RecoveryMaintenanceReportV1Error::
                    kNone &&
            built != nullptr,
        "REUSE_OPEN builds a private report capability");
    if (built == nullptr) {
        return;
    }
    const auto& model = built->model();
    test->Expect(
        model.open_boundary.open_variant ==
                ingress::RecoveryMaintenanceOpenVariantV1::
                    kReuseOpen &&
            model.open_boundary.reuse_segment
                    .reported_logical_end_offset ==
                ingress::kRawV1SegmentHeaderBytes &&
            model.closed_frontier.closed_entry_count ==
                0U,
        "REUSE_OPEN freezes header-only endpoint and zero frontier");
    test->Expect(
        built->filename() ==
            "recovery-f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff.json",
        "report filename is derived only from the recovery attempt");
    test->Expect(
        built->canonical_jcs().find(
            "\"open_variant\":\"REUSE_OPEN\"") !=
                std::string_view::npos &&
            built->canonical_jcs().find(
                "\"new_segment\":null") !=
                std::string_view::npos &&
            built->canonical_jcs().find(
                "\"current_sealed_raw_certificate_sha256\":null") !=
                std::string_view::npos &&
            built->canonical_jcs().size() <=
                ingress::
                    kRecoveryMaintenanceReportV1MaximumBytes,
        "REUSE_OPEN exact JCS carries required nullability and bound");

    ingress::RecoveryMaintenanceReportV1 parsed{};
    test->Expect(
        ingress::ParseRecoveryMaintenanceReportV1Jcs(
            built->canonical_jcs(), &parsed) ==
                ingress::RecoveryMaintenanceReportV1Error::
                    kNone &&
            parsed.open_boundary
                    .manifest_entry_commitment_sha256 ==
                model.open_boundary
                    .manifest_entry_commitment_sha256,
        "exact report JCS round-trips");
    std::string noncanonical(built->canonical_jcs());
    noncanonical.insert(1U, " ");
    test->Expect(
        ingress::ParseRecoveryMaintenanceReportV1Jcs(
            noncanonical, &parsed) ==
            ingress::RecoveryMaintenanceReportV1Error::
                kInvalidCanonicalJson,
        "whitespace and noncanonical bytes are rejected");

    const std::string digest = common::Sha256Hex(
        common::ComputeSha256(built->canonical_jcs()));
    if (digest !=
        "8db0d222b74caab3f9ecf1312d49dcad"
        "7d3b626393943e40be423311178dfb6d") {
        std::cerr
            << "observed RESUMED_OPEN report golden digest: "
            << digest << '\n';
    }
    test->Expect(
        digest ==
            "8db0d222b74caab3f9ecf1312d49dcad"
            "7d3b626393943e40be423311178dfb6d",
        "REUSE_OPEN complete-byte JCS golden SHA-256");
}

void TestRecordBearingReuseOpen(TestContext* test) {
    Fixture fixture = MakeFixture(true);
    const std::array<std::byte, 7U> body =
        Pattern<7U>(0x90U);
    ingress::RawRecordInputV1 input{};
    input.meta.source_stream_id =
        fixture.segment.source_stream_id;
    input.meta.connection_epoch_hint = 3U;
    input.meta.ingress_sequence = 1U;
    input.meta.recv_realtime_ns = 100U;
    input.meta.recv_monotonic_ns = 90U;
    input.meta.capture_date =
        fixture.segment.capture_date;
    input.vendor_head = MakeVendorHead(body.size());
    input.vendor_body = body;
    std::vector<std::byte> record;
    test->Expect(
        ingress::EncodeRawRecordV1(
            input, &record, nullptr) ==
            ingress::RawV1Error::kNone,
        "record-bearing REUSE fixture encodes one exact Raw record");
    auto bytes =
        std::make_shared<std::vector<std::byte>>(
            *fixture.segment_bytes);
    bytes->insert(
        bytes->end(), record.begin(), record.end());
    fixture.segment_bytes = bytes;
    const std::uint64_t logical_end =
        static_cast<std::uint64_t>(bytes->size());

    auto& open = *fixture.manifest.open_entry;
    open.segment_logical_length = logical_end;
    open.segment_sha256 =
        common::ComputeSha256(
            std::span<const std::byte>(*bytes));
    open.record_count = 1U;
    open.actual_first_ingress_sequence = 1U;
    open.actual_last_ingress_sequence = 1U;
    ingress::DurableMarkerV1 endpoint{};
    endpoint.source_stream_id =
        fixture.segment.source_stream_id;
    endpoint.segment_sequence = 1U;
    endpoint.durable_global_wal_pos = logical_end;
    endpoint.durable_ingress_sequence = 1U;
    endpoint.durable_segment_offset = logical_end;
    endpoint.marker_flags = 0U;
    static_cast<void>(
        ingress::EncodeDurableMarkerV1(
            endpoint, &open.accepted_marker_bytes));
    open.accepted_marker_sha256 =
        ingress::ComputeAcceptedMarkerSha256(
            open.accepted_marker_bytes);
    fixture.manifest.manifest_generation = 2U;
    test->Expect(
        ingress::ValidateManifestModel(
            fixture.manifest) ==
            ingress::RawManifestV1Error::kNone,
        "record-bearing final open manifest validates");

    auto& plan = fixture.analysis.segments.front();
    plan.validated_logical_end_offset = logical_end;
    plan.validated_last_ingress_sequence = 1U;
    plan.append_only_begin_offset =
        ingress::kRawV1SegmentHeaderBytes;
    plan.append_only_end_offset = logical_end;
    fixture.execution.mutated = true;
    fixture.execution.retained_journal_size =
        ingress::kRawV1JournalHeaderBytes +
        2U * ingress::kRawV1DurableMarkerBytes;
    fixture.execution.recovered_cursor = {
        1U, logical_end, 1U, logical_end, 0U};
    fixture.wal.append = {
        logical_end, 1U, logical_end};
    fixture.wal.durable = fixture.wal.append;
    fixture.wal.journal_logical_size =
        fixture.execution.retained_journal_size;

    std::unique_ptr<
        ingress::BuiltRecoveryMaintenanceReportV1>
        built;
    test->Expect(
        ingress::BuildResumedOpenRecoveryMaintenanceReportV1(
            fixture.key,
            fixture.journal,
            fixture.analysis,
            fixture.execution,
            fixture.analysis.accepted_journal_size,
            fixture.manifest,
            fixture.segment_bytes,
            fixture.sink,
            fixture.wal,
            nullptr,
            &built) ==
                ingress::
                    RecoveryMaintenanceReportV1Error::
                        kNone &&
            built != nullptr,
        "record-bearing REUSE_OPEN binds a record-end marker and exact prefix");
    if (built != nullptr) {
        test->Expect(
            built->model().open_boundary.reuse_segment
                        .reported_logical_end_offset ==
                    logical_end &&
                built->model().final_durable_cursor
                        .ingress_sequence ==
                    1U &&
                built->model().open_boundary
                        .endpoint_marker.marker_bytes ==
                    open.accepted_marker_bytes,
            "record-bearing REUSE_OPEN does not masquerade as header-only");
    }
}

void TestNewOpenAfterCanonicalZero(TestContext* test) {
    Fixture fixture = MakeFixture(false);
    ingress::RecoveryMaintenanceNewOpenPredecessorV1
        predecessor{};
    predecessor.terminal.kind =
        ingress::
            RecoveryMaintenancePreviousTerminalKindV1::
                kCanonicalZero;
    predecessor.reopens_empty_tombstone_sha256 =
        Pattern<32U>(0xb0U);

    std::unique_ptr<
        ingress::BuiltRecoveryMaintenanceReportV1>
        built;
    const auto result =
        ingress::BuildResumedOpenRecoveryMaintenanceReportV1(
            fixture.key,
            fixture.journal,
            fixture.analysis,
            fixture.execution,
            fixture.wal.journal_logical_size,
            fixture.manifest,
            fixture.segment_bytes,
            fixture.sink,
            fixture.wal,
            &predecessor,
            &built);
    test->Expect(
        result ==
                ingress::RecoveryMaintenanceReportV1Error::
                    kNone &&
            built != nullptr,
        "NEW_OPEN_AFTER_SEALED accepts canonical zero terminal");
    if (built == nullptr) {
        return;
    }
    test->Expect(
        built->canonical_jcs().find(
            "\"open_variant\":\"NEW_OPEN_AFTER_SEALED\"") !=
                std::string_view::npos &&
            built->canonical_jcs().find(
                "\"kind\":\"CANONICAL_ZERO\","
                "\"marker_bytes\":null") !=
                std::string_view::npos &&
            built->canonical_jcs().find(
                "\"reuse_segment\":null") !=
                std::string_view::npos,
        "NEW_OPEN_AFTER_SEALED freezes exact canonical-zero nullability");

    ingress::RecoveryMaintenanceReportV1 parsed{};
    test->Expect(
        ingress::ParseRecoveryMaintenanceReportV1Jcs(
            built->canonical_jcs(), &parsed) ==
                ingress::RecoveryMaintenanceReportV1Error::
                    kNone,
        "NEW_OPEN_AFTER_SEALED exact JCS round-trips");

    predecessor.reopens_sealed_raw_certificate_sha256 =
        Pattern<32U>(0xc0U);
    std::unique_ptr<
        ingress::BuiltRecoveryMaintenanceReportV1>
        invalid;
    test->Expect(
        ingress::BuildResumedOpenRecoveryMaintenanceReportV1(
            fixture.key,
            fixture.journal,
            fixture.analysis,
            fixture.execution,
            fixture.wal.journal_logical_size,
            fixture.manifest,
            fixture.segment_bytes,
            fixture.sink,
            fixture.wal,
            &predecessor,
            &invalid) ==
            ingress::RecoveryMaintenanceReportV1Error::
                kVariantMismatch,
        "at most one reopens digest is accepted");
}

void TestNewOpenAfterSealedFrontier(
    TestContext* test) {
    const ingress::SegmentHeaderV1 first =
        MakeSegment();
    ingress::RawWalWriterSnapshot first_open{};
    first_open.append = {
        ingress::kRawV1SegmentHeaderBytes,
        0U,
        ingress::kRawV1SegmentHeaderBytes};
    first_open.durable = first_open.append;
    first_open.journal_logical_size =
        ingress::kRawV1JournalHeaderBytes +
        ingress::kRawV1DurableMarkerBytes;
    first_open.initialized = true;
    ingress::RawManifestV1 open{};
    ingress::RawManifestV1 closed{};
    test->Expect(
        ingress::BuildFreshOpenRawManifestV1(
            first, first_open, &open) ==
                ingress::
                    RawManifestTransitionErrorV1::kNone &&
            ingress::TransitionOpenRawManifestToClosedV1(
                open,
                MakeSealedMetadata(first),
                &closed) ==
                ingress::
                    RawManifestTransitionErrorV1::kNone,
        "sealed predecessor manifest fixture builds");

    ingress::SegmentHeaderV1 second = first;
    second.segment_sequence = 2U;
    second.segment_base_wal_pos =
        ingress::kRawV1SegmentHeaderBytes;
    second.created_realtime_ns += 10U;
    second.created_monotonic_ns += 10U;
    ingress::RawV1SegmentHeaderWire second_wire{};
    static_cast<void>(
        ingress::EncodeSegmentHeaderV1(
            second, &second_wire));
    auto second_bytes =
        std::make_shared<std::vector<std::byte>>(
            second_wire.begin(), second_wire.end());

    ingress::RawWalWriterSnapshot final_wal{};
    final_wal.append = {
        2U * ingress::kRawV1SegmentHeaderBytes,
        0U,
        ingress::kRawV1SegmentHeaderBytes};
    final_wal.durable = final_wal.append;
    final_wal.journal_logical_size =
        ingress::kRawV1JournalHeaderBytes +
        3U * ingress::kRawV1DurableMarkerBytes;
    final_wal.initialized = true;
    ingress::RawManifestV1 final_manifest{};
    test->Expect(
        ingress::TransitionClosedRawManifestToNextOpenV1(
            closed,
            second,
            final_wal,
            &final_manifest) ==
            ingress::RawManifestTransitionErrorV1::kNone,
        "sealed frontier transitions to a header-only next open");

    ingress::RawRecoveryPlanV1 analysis{};
    const ingress::RawV1JournalHeaderWire journal =
        MakeJournal(first);
    static_cast<void>(
        ingress::DecodeDurableJournalHeaderV1(
            journal, &analysis.journal_header));
    analysis.accepted_journal_size =
        ingress::kRawV1JournalHeaderBytes +
        2U * ingress::kRawV1DurableMarkerBytes;
    analysis.has_accepted_cursor = true;
    analysis.accepted_cursor = {
        1U,
        ingress::kRawV1SegmentHeaderBytes,
        0U,
        ingress::kRawV1SegmentHeaderBytes,
        ingress::kRawV1SegmentSealed};
    analysis.r11_orphan =
        ingress::RawRecoveryR11OrphanV1::
            kNormalHeaderOnly;
    ingress::RawRecoverySegmentPlanV1 sealed_plan{};
    sealed_plan.segment_sequence = 1U;
    sealed_plan.segment_base_wal_pos = 0U;
    sealed_plan.has_accepted_marker = true;
    sealed_plan.accepted_marker_wire =
        closed.closed_entries.back()
            .accepted_marker_bytes;
    sealed_plan.durable_end_offset =
        ingress::kRawV1SegmentHeaderBytes;
    sealed_plan.validated_logical_end_offset =
        ingress::kRawV1SegmentHeaderBytes;
    sealed_plan.validated_last_ingress_sequence = 0U;
    sealed_plan.append_only_begin_offset =
        ingress::kRawV1SegmentHeaderBytes;
    sealed_plan.append_only_end_offset =
        ingress::kRawV1SegmentHeaderBytes;
    sealed_plan.tail_begin_offset =
        ingress::kRawV1SegmentHeaderBytes;
    sealed_plan.tail_end_offset =
        ingress::kRawV1SegmentHeaderBytes;
    sealed_plan.sealed = true;
    analysis.segments.push_back(sealed_plan);
    ingress::RawRecoverySegmentPlanV1 new_plan{};
    new_plan.segment_sequence = 2U;
    new_plan.segment_base_wal_pos =
        ingress::kRawV1SegmentHeaderBytes;
    new_plan.validated_logical_end_offset =
        ingress::kRawV1SegmentHeaderBytes;
    new_plan.validated_last_ingress_sequence = 0U;
    new_plan.append_only_begin_offset =
        ingress::kRawV1SegmentHeaderBytes;
    new_plan.append_only_end_offset =
        ingress::kRawV1SegmentHeaderBytes;
    new_plan.tail_begin_offset =
        ingress::kRawV1SegmentHeaderBytes;
    new_plan.tail_end_offset =
        ingress::kRawV1SegmentHeaderBytes;
    analysis.segments.push_back(new_plan);

    ingress::RawRecoveryExecutionResultV1 execution{};
    execution.cursor_publishable = true;
    execution.mutated = true;
    execution.retained_journal_size =
        final_wal.journal_logical_size;
    execution.recovered_cursor = {
        2U,
        2U * ingress::kRawV1SegmentHeaderBytes,
        0U,
        ingress::kRawV1SegmentHeaderBytes,
        0U};

    ingress::RawWalSinkIdentityV1 sink{};
    sink.writer_instance = Pattern<16U>(0xe0U);
    sink.stream_day_id = second.stream_day_id;
    sink.source_stream_id = second.source_stream_id;
    sink.capture_date = second.capture_date;
    sink.segment_sequence = 2U;
    sink.segment_base_wal_pos =
        ingress::kRawV1SegmentHeaderBytes;
    sink.first_ingress_sequence = 1U;
    ingress::RawReserveRegistryEntryKeyV1 key{};
    key.route.source_stream_id =
        second.source_stream_id;
    key.route.capture_date = second.capture_date;
    key.stream_day_id = second.stream_day_id;
    key.recovery_attempt_id =
        Pattern<16U>(0xf0U);

    const auto& frontier =
        final_manifest.closed_entries.back();
    ingress::RecoveryMaintenanceNewOpenPredecessorV1
        predecessor{};
    predecessor.terminal.kind =
        ingress::
            RecoveryMaintenancePreviousTerminalKindV1::
                kSealed;
    predecessor.terminal.marker_bytes =
        frontier.accepted_marker_bytes;
    predecessor.terminal.marker_sha256 =
        frontier.accepted_marker_sha256;
    predecessor.terminal.segment_sequence =
        frontier.segment_sequence;
    predecessor.terminal.segment_sha256 =
        frontier.segment_sha256;
    predecessor.reopens_sealed_raw_certificate_sha256 =
        Pattern<32U>(0xc0U);

    std::unique_ptr<
        ingress::BuiltRecoveryMaintenanceReportV1>
        built;
    test->Expect(
        ingress::BuildResumedOpenRecoveryMaintenanceReportV1(
            key,
            journal,
            analysis,
            execution,
            analysis.accepted_journal_size,
            final_manifest,
            second_bytes,
            sink,
            final_wal,
            &predecessor,
            &built) ==
                ingress::
                    RecoveryMaintenanceReportV1Error::
                        kNone &&
            built != nullptr,
        "NEW_OPEN_AFTER_SEALED binds exact sealed frontier and new header marker");
    if (built != nullptr) {
        ingress::RecoveryMaintenanceReportV1 parsed{};
        test->Expect(
            built->model().closed_frontier
                        .closed_entry_count ==
                    1U &&
                built->model().open_boundary
                        .previous_terminal
                        .marker_sha256 ==
                    frontier.accepted_marker_sha256 &&
                ingress::
                        ParseRecoveryMaintenanceReportV1Jcs(
                            built->canonical_jcs(),
                            &parsed) ==
                    ingress::
                        RecoveryMaintenanceReportV1Error::
                            kNone,
            "sealed-frontier NEW_OPEN exact JCS round-trips");
    }
}

void TestCrossChecks(TestContext* test) {
    Fixture fixture = MakeFixture(true);
    fixture.wal.durable.segment_offset += 8U;
    fixture.wal.append = fixture.wal.durable;
    std::unique_ptr<
        ingress::BuiltRecoveryMaintenanceReportV1>
        built;
    test->Expect(
        ingress::BuildResumedOpenRecoveryMaintenanceReportV1(
            fixture.key,
            fixture.journal,
            fixture.analysis,
            fixture.execution,
            fixture.wal.journal_logical_size,
            fixture.manifest,
            fixture.segment_bytes,
            fixture.sink,
            fixture.wal,
            nullptr,
            &built) ==
            ingress::RecoveryMaintenanceReportV1Error::
                kCursorMismatch,
        "writer/marker cursor mismatch cannot mint a capability");

    fixture = MakeFixture(true);
    auto changed =
        std::make_shared<std::vector<std::byte>>(
            *fixture.segment_bytes);
    changed->back() = std::byte{0xff};
    fixture.segment_bytes = changed;
    test->Expect(
        ingress::BuildResumedOpenRecoveryMaintenanceReportV1(
            fixture.key,
            fixture.journal,
            fixture.analysis,
            fixture.execution,
            fixture.wal.journal_logical_size,
            fixture.manifest,
            fixture.segment_bytes,
            fixture.sink,
            fixture.wal,
            nullptr,
            &built) ==
            ingress::RecoveryMaintenanceReportV1Error::
                kOpenSegmentInvalid,
        "modified exact segment header cannot mint a capability");
}

void TestSealedRaw(TestContext* test) {
    const ingress::SegmentHeaderV1 segment =
        MakeSegment();
    const ingress::RawV1JournalHeaderWire journal =
        MakeJournal(segment);
    ingress::RawWalWriterSnapshot open_wal{};
    open_wal.append = {
        ingress::kRawV1SegmentHeaderBytes,
        0U,
        ingress::kRawV1SegmentHeaderBytes};
    open_wal.durable = open_wal.append;
    open_wal.journal_logical_size =
        ingress::kRawV1JournalHeaderBytes +
        ingress::kRawV1DurableMarkerBytes;
    open_wal.initialized = true;
    ingress::RawManifestV1 open_manifest{};
    ingress::RawManifestV1 closed_manifest{};
    test->Expect(
        ingress::BuildFreshOpenRawManifestV1(
            segment, open_wal, &open_manifest) ==
                ingress::
                    RawManifestTransitionErrorV1::kNone &&
            ingress::TransitionOpenRawManifestToClosedV1(
                open_manifest,
                MakeSealedMetadata(segment),
                &closed_manifest) ==
                ingress::
                    RawManifestTransitionErrorV1::kNone,
        "SEALED_RAW fixture builds a terminal manifest");

    ingress::RawWalSinkIdentityV1 sink{};
    sink.writer_instance = Pattern<16U>(0xe0U);
    sink.stream_day_id = segment.stream_day_id;
    sink.source_stream_id = segment.source_stream_id;
    sink.capture_date = segment.capture_date;
    sink.segment_sequence = segment.segment_sequence;
    sink.segment_base_wal_pos =
        segment.segment_base_wal_pos;
    sink.first_ingress_sequence =
        segment.first_ingress_sequence;
    ingress::RawWalWriterSnapshot final_wal{};
    final_wal.append = {
        ingress::kRawV1SegmentHeaderBytes,
        0U,
        ingress::kRawV1SegmentHeaderBytes};
    final_wal.durable = final_wal.append;
    final_wal.journal_logical_size =
        ingress::kRawV1JournalHeaderBytes +
        2U * ingress::kRawV1DurableMarkerBytes;
    final_wal.initialized = true;
    final_wal.sealed = true;
    final_wal.closed = true;
    std::unique_ptr<
        ingress::BuiltSealedRawCertificateV1>
        certificate;
    test->Expect(
        ingress::BuildSealedRawCertificateCapabilityV1(
            journal,
            closed_manifest,
            sink,
            final_wal,
            &certificate) ==
                ingress::
                    SealedRawCertificateV1Error::kNone &&
            certificate != nullptr,
        "SEALED_RAW fixture mints a private sealed sidecar capability");
    if (certificate == nullptr) {
        return;
    }

    ingress::RawRecoveryPlanV1 analysis{};
    static_cast<void>(
        ingress::DecodeDurableJournalHeaderV1(
            journal, &analysis.journal_header));
    analysis.accepted_journal_size =
        final_wal.journal_logical_size;
    analysis.has_accepted_cursor = true;
    analysis.accepted_cursor = {
        1U,
        ingress::kRawV1SegmentHeaderBytes,
        0U,
        ingress::kRawV1SegmentHeaderBytes,
        ingress::kRawV1SegmentSealed};
    ingress::RawRecoverySegmentPlanV1 terminal{};
    terminal.segment_sequence = 1U;
    terminal.segment_base_wal_pos = 0U;
    terminal.has_accepted_marker = true;
    terminal.accepted_marker_wire =
        closed_manifest.closed_entries.back()
            .accepted_marker_bytes;
    terminal.durable_end_offset =
        ingress::kRawV1SegmentHeaderBytes;
    terminal.validated_logical_end_offset =
        ingress::kRawV1SegmentHeaderBytes;
    terminal.validated_last_ingress_sequence = 0U;
    terminal.append_only_begin_offset =
        ingress::kRawV1SegmentHeaderBytes;
    terminal.append_only_end_offset =
        ingress::kRawV1SegmentHeaderBytes;
    terminal.tail_begin_offset =
        ingress::kRawV1SegmentHeaderBytes;
    terminal.tail_end_offset =
        ingress::kRawV1SegmentHeaderBytes;
    terminal.sealed = true;
    analysis.segments.push_back(terminal);
    ingress::RawRecoveryExecutionResultV1 execution{};
    execution.cursor_publishable = true;
    execution.retained_journal_size =
        analysis.accepted_journal_size;
    execution.recovered_cursor =
        analysis.accepted_cursor;
    ingress::RawReserveRegistryEntryKeyV1 key{};
    key.route.source_stream_id =
        segment.source_stream_id;
    key.route.capture_date = segment.capture_date;
    key.stream_day_id = segment.stream_day_id;
    key.recovery_attempt_id = Pattern<16U>(0xf0U);

    std::unique_ptr<
        ingress::BuiltRecoveryMaintenanceReportV1>
        built;
    const auto sealed_build =
        ingress::BuildSealedRawRecoveryMaintenanceReportV1(
            key,
            journal,
            analysis,
            execution,
            analysis.accepted_journal_size,
            *certificate,
            &built);
    if (sealed_build !=
        ingress::RecoveryMaintenanceReportV1Error::kNone) {
        std::cerr
            << "observed SEALED_RAW build error: "
            << ingress::RecoveryMaintenanceReportV1ErrorName(
                   sealed_build)
            << '\n';
    }
    test->Expect(
        sealed_build ==
                ingress::
                    RecoveryMaintenanceReportV1Error::
                        kNone &&
            built != nullptr,
        "SEALED_RAW builds only from recovery facts and a sealed capability");
    if (built == nullptr) {
        return;
    }
    const auto& model = built->model();
    test->Expect(
        model.result ==
                ingress::
                    RecoveryMaintenanceResultV1::
                        kSealedRaw &&
            model.intent ==
                ingress::
                    RecoveryMaintenanceIntentV1::
                        kRecoverSealOnly &&
            model.current_sealed_raw_certificate_sha256 ==
                certificate->certificate_sha256() &&
            model.closed_frontier
                    .frontier_segment_sha256 ==
                certificate->model()
                    .last_segment_sha256 &&
            model.sealed_boundary
                    .accepted_sealed_marker_bytes ==
                certificate->model()
                    .accepted_sealed_marker_bytes,
        "SEALED_RAW freezes certificate hash, terminal cursor, seal and frontier");
    test->Expect(
        built->canonical_jcs().find(
            "\"open_boundary\":null") !=
                std::string_view::npos &&
            built->canonical_jcs().find(
                "\"current_empty_anchor_tombstone_sha256\":null") !=
                std::string_view::npos &&
            built->canonical_jcs().find(
                "\"marker_count\":null") !=
                std::string_view::npos &&
            built->canonical_jcs().find(
                "\"result\":\"SEALED_RAW\"") !=
                std::string_view::npos,
        "SEALED_RAW exact JCS freezes its tagged nullability");
    ingress::RecoveryMaintenanceReportV1 parsed{};
    test->Expect(
        ingress::ParseRecoveryMaintenanceReportV1Jcs(
            built->canonical_jcs(), &parsed) ==
                ingress::
                    RecoveryMaintenanceReportV1Error::
                        kNone &&
            parsed.result ==
                ingress::
                    RecoveryMaintenanceResultV1::
                        kSealedRaw &&
            parsed.sealed_boundary.last_segment_sha256 ==
                model.sealed_boundary.last_segment_sha256,
        "SEALED_RAW exact JCS round-trips");

    const std::string digest = common::Sha256Hex(
        common::ComputeSha256(built->canonical_jcs()));
    if (digest !=
        "3593cd964f0762832b6cfe5fc5955d60"
        "860e40e6219dc3af8aa134c1f79b7f73") {
        std::cerr
            << "observed SEALED_RAW report golden digest: "
            << digest << '\n';
    }
    test->Expect(
        digest ==
            "3593cd964f0762832b6cfe5fc5955d60"
            "860e40e6219dc3af8aa134c1f79b7f73",
        "SEALED_RAW complete-byte JCS golden SHA-256");

    std::unique_ptr<
        ingress::BuiltRecoveryMaintenanceReportV1>
        sentinel;
    Fixture resumed_fixture = MakeFixture(true);
    static_cast<void>(
        ingress::BuildResumedOpenRecoveryMaintenanceReportV1(
            resumed_fixture.key,
            resumed_fixture.journal,
            resumed_fixture.analysis,
            resumed_fixture.execution,
            resumed_fixture.wal.journal_logical_size,
            resumed_fixture.manifest,
            resumed_fixture.segment_bytes,
            resumed_fixture.sink,
            resumed_fixture.wal,
            nullptr,
            &sentinel));
    const auto* const sentinel_address = sentinel.get();
    ingress::RawRecoveryPlanV1 mismatched = analysis;
    mismatched.segments.back().accepted_marker_wire[0U] =
        std::byte{0xff};
    test->Expect(
        ingress::BuildSealedRawRecoveryMaintenanceReportV1(
            key,
            journal,
            mismatched,
            execution,
            mismatched.accepted_journal_size,
            *certificate,
            &sentinel) ==
                ingress::
                    RecoveryMaintenanceReportV1Error::
                        kFrontierMismatch &&
            sentinel.get() == sentinel_address,
        "SEALED_RAW sidecar/frontier failure is output-atomic");

    ingress::RecoveryMaintenanceReportV1 tampered = model;
    tampered.current_sealed_raw_certificate_sha256.fill(
        std::byte{0});
    test->Expect(
        ingress::ValidateRecoveryMaintenanceReportV1(
            tampered) ==
            ingress::RecoveryMaintenanceReportV1Error::
                kVariantMismatch,
        "SEALED_RAW cannot omit its required certificate hash");
}

void TestEmptyAnchorOnly(TestContext* test) {
    const ingress::SegmentHeaderV1 segment =
        MakeSegment();
    const ingress::RawV1JournalHeaderWire journal =
        MakeJournal(segment);
    ingress::EmptyAnchorObservationV1 observation{};
    observation.journal_header_bytes = journal;
    observation.journal_logical_size =
        ingress::kRawV1JournalHeaderBytes;
    std::unique_ptr<
        ingress::BuiltEmptyAnchorTombstoneV1>
        tombstone;
    test->Expect(
        ingress::BuildEmptyAnchorTombstoneCapabilityV1(
            observation, &tombstone) ==
                ingress::
                    EmptyAnchorTombstoneV1Error::kNone &&
            tombstone != nullptr,
        "EMPTY_ANCHOR_ONLY fixture mints a private tombstone capability");
    if (tombstone == nullptr) {
        return;
    }

    ingress::RawRecoveryPlanV1 analysis{};
    static_cast<void>(
        ingress::DecodeDurableJournalHeaderV1(
            journal, &analysis.journal_header));
    analysis.accepted_journal_size =
        ingress::kRawV1JournalHeaderBytes;
    analysis.initial_anchor =
        ingress::RawRecoveryInitialAnchorV1::
            kJournalOnly;
    ingress::RawRecoveryExecutionResultV1 execution{};
    execution.retained_journal_size =
        ingress::kRawV1JournalHeaderBytes;
    ingress::RawReserveRegistryEntryKeyV1 key{};
    key.route.source_stream_id =
        segment.source_stream_id;
    key.route.capture_date = segment.capture_date;
    key.stream_day_id = segment.stream_day_id;
    key.recovery_attempt_id = Pattern<16U>(0xf0U);

    std::unique_ptr<
        ingress::BuiltRecoveryMaintenanceReportV1>
        built;
    test->Expect(
        ingress::
            BuildEmptyAnchorOnlyRecoveryMaintenanceReportV1(
                key,
                journal,
                analysis,
                execution,
                ingress::kRawV1JournalHeaderBytes,
                *tombstone,
                &built) ==
                ingress::
                    RecoveryMaintenanceReportV1Error::
                        kNone &&
            built != nullptr,
        "EMPTY_ANCHOR_ONLY builds only from journal-only facts and tombstone capability");
    if (built == nullptr) {
        return;
    }
    const auto& model = built->model();
    test->Expect(
        model.result ==
                ingress::
                    RecoveryMaintenanceResultV1::
                        kEmptyAnchorOnly &&
            model.intent ==
                ingress::
                    RecoveryMaintenanceIntentV1::
                        kRecoverSealOnly &&
            model.current_empty_anchor_tombstone_sha256 ==
                tombstone->tombstone_sha256() &&
            model.marker_count == 0U &&
            model.segment_count == 0U &&
            model.record_count == 0U,
        "EMPTY_ANCHOR_ONLY freezes tombstone hash and exact zero counts");
    test->Expect(
        built->canonical_jcs().find(
            "\"closed_frontier\":null") !=
                std::string_view::npos &&
            built->canonical_jcs().find(
                "\"final_durable_cursor\":null") !=
                std::string_view::npos &&
            built->canonical_jcs().find(
                "\"marker_count\":\"0\"") !=
                std::string_view::npos &&
            built->canonical_jcs().find(
                "\"open_boundary\":null") !=
                std::string_view::npos &&
            built->canonical_jcs().find(
                "\"recovery_range\":null") !=
                std::string_view::npos &&
            built->canonical_jcs().find(
                "\"sealed_boundary\":null") !=
                std::string_view::npos &&
            built->canonical_jcs().find(
                "\"segment_tail_classification\":null") !=
                std::string_view::npos,
        "EMPTY_ANCHOR_ONLY freezes segment/seal/frontier/cursor-range nullability");

    ingress::RecoveryMaintenanceReportV1 parsed{};
    test->Expect(
        ingress::ParseRecoveryMaintenanceReportV1Jcs(
            built->canonical_jcs(), &parsed) ==
                ingress::
                    RecoveryMaintenanceReportV1Error::
                        kNone &&
            parsed.result ==
                ingress::
                    RecoveryMaintenanceResultV1::
                        kEmptyAnchorOnly &&
            parsed.current_empty_anchor_tombstone_sha256 ==
                tombstone->tombstone_sha256(),
        "EMPTY_ANCHOR_ONLY exact JCS round-trips");
    const std::string digest = common::Sha256Hex(
        common::ComputeSha256(built->canonical_jcs()));
    if (digest !=
        "3263373dc2ac8029d7eeadf514395fe6d"
        "20b20193e8d8ebfe96f91ab9a6ffa7b") {
        std::cerr
            << "observed EMPTY_ANCHOR_ONLY report golden digest: "
            << digest << '\n';
    }
    test->Expect(
        digest ==
            "3263373dc2ac8029d7eeadf514395fe6d"
            "20b20193e8d8ebfe96f91ab9a6ffa7b",
        "EMPTY_ANCHOR_ONLY complete-byte JCS golden SHA-256");

    const ingress::RecoveryMaintenanceReportV1 sentinel =
        parsed;
    std::string noncanonical(built->canonical_jcs());
    noncanonical.insert(
        noncanonical.size() - 1U,
        ",\"unknown\":null");
    test->Expect(
        ingress::ParseRecoveryMaintenanceReportV1Jcs(
            noncanonical, &parsed) ==
                ingress::
                    RecoveryMaintenanceReportV1Error::
                        kInvalidCanonicalJson &&
            parsed.current_empty_anchor_tombstone_sha256 ==
                sentinel
                    .current_empty_anchor_tombstone_sha256,
        "unknown fields are rejected without changing parser output");
    noncanonical =
        std::string(built->canonical_jcs()) + "\n";
    test->Expect(
        ingress::ParseRecoveryMaintenanceReportV1Jcs(
            noncanonical, &parsed) ==
                ingress::
                    RecoveryMaintenanceReportV1Error::
                        kInvalidCanonicalJson &&
            parsed.current_empty_anchor_tombstone_sha256 ==
                sentinel
                    .current_empty_anchor_tombstone_sha256,
        "trailing bytes are rejected output-atomically");
    noncanonical = built->canonical_jcs();
    const std::string capture_prefix =
        "{\"capture_date\":20260718";
    test->Expect(
        noncanonical.starts_with(capture_prefix),
        "EMPTY golden begins with the canonical first key");
    if (noncanonical.starts_with(capture_prefix)) {
        noncanonical.insert(
            capture_prefix.size(),
            ",\"capture_date\":20260718");
        test->Expect(
            ingress::
                    ParseRecoveryMaintenanceReportV1Jcs(
                        noncanonical, &parsed) ==
                ingress::
                    RecoveryMaintenanceReportV1Error::
                        kInvalidCanonicalJson,
            "duplicate fields are rejected");
    }
    noncanonical = built->canonical_jcs();
    const std::string ordered_prefix =
        "{\"capture_date\":20260718,"
        "\"closed_frontier\":null";
    test->Expect(
        noncanonical.starts_with(ordered_prefix),
        "EMPTY golden contains the two canonical leading keys");
    if (noncanonical.starts_with(ordered_prefix)) {
        noncanonical.replace(
            0U,
            ordered_prefix.size(),
            "{\"closed_frontier\":null,"
            "\"capture_date\":20260718");
        test->Expect(
            ingress::
                    ParseRecoveryMaintenanceReportV1Jcs(
                        noncanonical, &parsed) ==
                ingress::
                    RecoveryMaintenanceReportV1Error::
                        kInvalidCanonicalJson,
            "reordered fields are rejected");
    }
    noncanonical = built->canonical_jcs();
    const std::string quoted_zero =
        "\"marker_count\":\"0\"";
    const std::size_t count =
        noncanonical.find(quoted_zero);
    test->Expect(
        count != std::string::npos,
        "EMPTY golden contains a quoted marker count");
    if (count != std::string::npos) {
        noncanonical.replace(
            count,
            quoted_zero.size(),
            "\"marker_count\":0");
        test->Expect(
            ingress::
                    ParseRecoveryMaintenanceReportV1Jcs(
                        noncanonical, &parsed) ==
                ingress::
                    RecoveryMaintenanceReportV1Error::
                        kInvalidCanonicalJson,
            "uint64 counts reject unquoted JSON numbers");
    }
    noncanonical = built->canonical_jcs();
    const std::size_t canonical_count =
        noncanonical.find(quoted_zero);
    if (canonical_count != std::string::npos) {
        noncanonical.replace(
            canonical_count,
            quoted_zero.size(),
            "\"marker_count\":\"00\"");
        test->Expect(
            ingress::
                    ParseRecoveryMaintenanceReportV1Jcs(
                        noncanonical, &parsed) ==
                ingress::
                    RecoveryMaintenanceReportV1Error::
                        kInvalidCanonicalJson,
            "uint64 counts reject leading-zero decimals");
    }
    noncanonical = built->canonical_jcs();
    const std::size_t stream_day_key =
        noncanonical.find("\"stream_day_id\":\"");
    const std::size_t lowercase_a =
        stream_day_key == std::string::npos
            ? std::string::npos
            : noncanonical.find('a', stream_day_key);
    test->Expect(
        lowercase_a != std::string::npos,
        "EMPTY golden contains lowercase identity hex");
    if (lowercase_a != std::string::npos) {
        noncanonical[lowercase_a] = 'A';
        test->Expect(
            ingress::
                    ParseRecoveryMaintenanceReportV1Jcs(
                        noncanonical, &parsed) ==
                ingress::
                    RecoveryMaintenanceReportV1Error::
                        kInvalidCanonicalJson,
            "uppercase identity hex is rejected");
    }
    noncanonical.assign(
        ingress::
                kRecoveryMaintenanceReportV1MaximumBytes +
            1U,
        'x');
    test->Expect(
        ingress::ParseRecoveryMaintenanceReportV1Jcs(
            noncanonical, &parsed) ==
                ingress::
                    RecoveryMaintenanceReportV1Error::
                        kInvalidCanonicalJson &&
            parsed.current_empty_anchor_tombstone_sha256 ==
                sentinel
                    .current_empty_anchor_tombstone_sha256,
        "oversized input is rejected output-atomically");

    ingress::RawRecoveryPlanV1 nonempty = analysis;
    nonempty.segments.push_back(
        ingress::RawRecoverySegmentPlanV1{});
    std::unique_ptr<
        ingress::BuiltRecoveryMaintenanceReportV1>
        unchanged;
    test->Expect(
        ingress::
            BuildEmptyAnchorOnlyRecoveryMaintenanceReportV1(
                key,
                journal,
                nonempty,
                execution,
                ingress::kRawV1JournalHeaderBytes,
                *tombstone,
                &unchanged) ==
                ingress::
                    RecoveryMaintenanceReportV1Error::
                        kVariantMismatch &&
            unchanged == nullptr,
        "EMPTY_ANCHOR_ONLY rejects any segment fact output-atomically");

    ingress::SegmentHeaderV1 other_segment = segment;
    ++other_segment.capture_date;
    ingress::EmptyAnchorObservationV1 other_observation{};
    other_observation.journal_header_bytes =
        MakeJournal(other_segment);
    other_observation.journal_logical_size =
        ingress::kRawV1JournalHeaderBytes;
    std::unique_ptr<
        ingress::BuiltEmptyAnchorTombstoneV1>
        other_tombstone;
    test->Expect(
        ingress::BuildEmptyAnchorTombstoneCapabilityV1(
            other_observation, &other_tombstone) ==
                ingress::
                    EmptyAnchorTombstoneV1Error::kNone &&
            other_tombstone != nullptr,
        "mismatched tombstone capability fixture builds independently");
    if (other_tombstone != nullptr) {
        test->Expect(
            ingress::
                BuildEmptyAnchorOnlyRecoveryMaintenanceReportV1(
                    key,
                    journal,
                    analysis,
                    execution,
                    ingress::kRawV1JournalHeaderBytes,
                    *other_tombstone,
                    &unchanged) ==
                    ingress::
                        RecoveryMaintenanceReportV1Error::
                            kSidecarMismatch &&
                unchanged == nullptr,
            "EMPTY builder cross-validates sidecar namespace and journal hash");
    }

    ingress::RecoveryMaintenanceReportV1 tampered = model;
    tampered.current_sealed_raw_certificate_sha256 =
        Pattern<32U>(0x90U);
    test->Expect(
        ingress::ValidateRecoveryMaintenanceReportV1(
            tampered) ==
            ingress::RecoveryMaintenanceReportV1Error::
                kVariantMismatch,
        "EMPTY_ANCHOR_ONLY rejects a non-null certificate field");
}

void TestFrozenSchema(TestContext* test) {
#if defined(L2FLOW_SOURCE_DIR)
    const std::string path =
        std::string(L2FLOW_SOURCE_DIR) +
        "/schemas/recovery_maintenance_report_v1.json";
    std::ifstream input(path, std::ios::binary);
    const std::string bytes{
        std::istreambuf_iterator<char>(input),
        std::istreambuf_iterator<char>()};
    test->Expect(
        input.good() || input.eof(),
        "frozen recovery report schema is readable");
    test->Expect(
        bytes.size() ==
                ingress::
                    kRecoveryMaintenanceReportV1SchemaBytes &&
            common::Sha256Hex(
                common::ComputeSha256(bytes)) ==
                ingress::
                    kRecoveryMaintenanceReportV1SchemaSha256Hex,
        "frozen recovery report schema byte count and SHA-256 match constants");
#else
    test->Expect(
        false,
        "test target provides the source schema directory");
#endif
}

}  // namespace

int main() {
    TestContext test{};
    TestFrozenSchema(&test);
    TestReuseOpen(&test);
    TestRecordBearingReuseOpen(&test);
    TestNewOpenAfterCanonicalZero(&test);
    TestNewOpenAfterSealedFrontier(&test);
    TestCrossChecks(&test);
    TestSealedRaw(&test);
    TestEmptyAnchorOnly(&test);
    if (test.failures != 0) {
        std::cerr << test.failures
                  << " recovery maintenance report tests failed\n";
        return 1;
    }
    std::cout
        << "Phase 2 recovery maintenance report tests passed\n";
    return 0;
}
