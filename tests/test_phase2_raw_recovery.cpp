#include "l2flow/ingress/raw_recovery.h"
#include "l2flow/ingress/raw_v1.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <span>
#include <string>
#include <utility>
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

void StoreU16(
    std::span<std::byte> bytes,
    std::size_t offset,
    std::uint16_t value) {
    bytes[offset] = static_cast<std::byte>(value & 0xffU);
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

struct NamespaceFixture final {
    std::uint32_t source_stream_id = 1001U;
    std::uint32_t capture_date = 20260718U;
    ingress::RawV1Identity stream_day_id{};
    ingress::RawV1Digest schema{};
};

NamespaceFixture MakeNamespace() {
    NamespaceFixture result;
    FillNonzero(&result.stream_day_id, 1U);
    FillNonzero(&result.schema, 31U);
    return result;
}

std::shared_ptr<std::vector<std::byte>> MakeJournal(
    const NamespaceFixture& identity) {
    ingress::DurableJournalHeaderV1 header;
    header.capture_date = identity.capture_date;
    header.source_stream_id = identity.source_stream_id;
    header.stream_day_id = identity.stream_day_id;
    header.raw_schema_sha256 = identity.schema;
    FillNonzero(&header.created_host_uuid, 71U);
    FillNonzero(&header.created_linux_boot_id, 91U);
    header.created_clock_epoch_algorithm = 1U;
    FillNonzero(&header.created_clock_epoch_digest, 111U);
    header.created_clock_epoch_label = 4U;

    ingress::RawV1JournalHeaderWire wire{};
    if (ingress::EncodeDurableJournalHeaderV1(
            header, &wire) != ingress::RawV1Error::kNone) {
        return std::make_shared<std::vector<std::byte>>();
    }
    return std::make_shared<std::vector<std::byte>>(
        wire.begin(), wire.end());
}

ingress::RawV1VendorHead MakeVendorHead(
    std::uint32_t body_size,
    std::uint64_t vendor_sequence) {
    ingress::RawV1VendorHead result{};
    std::span<std::byte> bytes(result);
    result[0] =
        static_cast<std::byte>(ingress::kVendorMessageHeadBytes);
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
    std::uint32_t sequence = 0U;
    std::uint64_t base = 0U;
    std::uint64_t first_ingress = 0U;
    std::vector<std::uint64_t> record_ends;
};

BuiltSegment MakeSegment(
    const NamespaceFixture& identity,
    std::uint32_t sequence,
    std::uint64_t base,
    std::uint64_t first_ingress,
    const std::vector<std::size_t>& body_sizes) {
    BuiltSegment result;
    result.bytes =
        std::make_shared<std::vector<std::byte>>();
    result.sequence = sequence;
    result.base = base;
    result.first_ingress = first_ingress;

    ingress::SegmentHeaderV1 header;
    header.source_stream_id = identity.source_stream_id;
    header.capture_date = identity.capture_date;
    header.stream_day_id = identity.stream_day_id;
    header.segment_sequence = sequence;
    header.segment_base_wal_pos = base;
    header.first_ingress_sequence = first_ingress;
    header.created_realtime_ns = 1'000U;
    header.created_monotonic_ns = 500U;
    FillNonzero(&header.host_uuid, 3U);
    FillNonzero(&header.linux_boot_id, 23U);
    header.clock_epoch_algorithm = 1U;
    FillNonzero(&header.clock_epoch_digest, 43U);
    header.clock_epoch_label = 2U;
    FillNonzero(&header.sdk_archive_sha256, 63U);
    FillNonzero(&header.libmdl_api_sha256, 83U);
    FillNonzero(&header.endpoint_contract_sha256, 103U);
    FillNonzero(&header.config_sha256, 123U);
    header.raw_schema_sha256 = identity.schema;
    FillNonzero(&header.build_manifest_sha256, 143U);

    ingress::RawV1SegmentHeaderWire header_wire{};
    if (ingress::EncodeSegmentHeaderV1(
            header, &header_wire) !=
        ingress::RawV1Error::kNone) {
        return result;
    }
    result.bytes->assign(
        header_wire.begin(), header_wire.end());

    for (std::size_t index = 0U;
         index < body_sizes.size();
         ++index) {
        std::vector<std::byte> body(
            body_sizes[index],
            static_cast<std::byte>(
                static_cast<std::uint8_t>(10U + index)));
        ingress::RawRecordInputV1 input;
        input.meta.source_stream_id =
            identity.source_stream_id;
        input.meta.connection_epoch_hint = 7U;
        input.meta.ingress_sequence =
            first_ingress +
            static_cast<std::uint64_t>(index);
        input.meta.recv_realtime_ns =
            10'000U + input.meta.ingress_sequence;
        input.meta.recv_monotonic_ns =
            5'000U + input.meta.ingress_sequence;
        input.meta.capture_date = identity.capture_date;
        input.vendor_head = MakeVendorHead(
            static_cast<std::uint32_t>(body.size()),
            900U + input.meta.ingress_sequence);
        input.vendor_body = body;

        std::vector<std::byte> record;
        if (ingress::EncodeRawRecordV1(
                input, &record, nullptr) !=
            ingress::RawV1Error::kNone) {
            continue;
        }
        result.bytes->insert(
            result.bytes->end(),
            record.begin(),
            record.end());
        result.record_ends.push_back(
            static_cast<std::uint64_t>(
                result.bytes->size()));
    }
    return result;
}

ingress::RawV1DurableMarkerWire MakeMarker(
    const NamespaceFixture& identity,
    const BuiltSegment& segment,
    std::uint64_t offset,
    std::uint64_t ingress_sequence,
    std::uint32_t flags) {
    ingress::DurableMarkerV1 marker;
    marker.source_stream_id = identity.source_stream_id;
    marker.segment_sequence = segment.sequence;
    marker.durable_global_wal_pos = segment.base + offset;
    marker.durable_ingress_sequence = ingress_sequence;
    marker.durable_segment_offset = offset;
    marker.marker_flags = flags;
    ingress::RawV1DurableMarkerWire wire{};
    static_cast<void>(
        ingress::EncodeDurableMarkerV1(marker, &wire));
    return wire;
}

void AppendMarker(
    std::vector<std::byte>* journal,
    const ingress::RawV1DurableMarkerWire& marker) {
    if (journal == nullptr) {
        return;
    }
    journal->insert(
        journal->end(), marker.begin(), marker.end());
}

ingress::RawRecoveryInputV1 MakeInput(
    const std::shared_ptr<std::vector<std::byte>>& journal,
    const std::vector<BuiltSegment>& segments) {
    ingress::RawRecoveryInputV1 input;
    input.journal = journal;
    for (const BuiltSegment& segment : segments) {
        input.segments.push_back({segment.bytes});
    }
    return input;
}

void TestInitializationRecordsAndSeal(TestContext* test) {
    const NamespaceFixture identity = MakeNamespace();
    const BuiltSegment segment =
        MakeSegment(identity, 1U, 0U, 1U, {2U, 9U});
    const auto journal = MakeJournal(identity);
    AppendMarker(
        journal.get(),
        MakeMarker(
            identity,
            segment,
            ingress::kRawV1SegmentHeaderBytes,
            0U,
            0U));
    AppendMarker(
        journal.get(),
        MakeMarker(
            identity,
            segment,
            segment.record_ends.back(),
            2U,
            0U));
    const ingress::RawV1DurableMarkerWire seal_marker =
        MakeMarker(
            identity,
            segment,
            segment.record_ends.back(),
            2U,
            ingress::kRawV1SegmentSealed);
    AppendMarker(journal.get(), seal_marker);

    const ingress::RawRecoveryPlanV1 plan =
        ingress::AnalyzeRawRecoveryV1(
            MakeInput(journal, {segment}));
    test->Expect(
        plan.ok() && plan.has_accepted_cursor &&
            plan.accepted_cursor.segment_offset ==
                segment.record_ends.back() &&
            plan.accepted_cursor.ingress_sequence == 2U &&
            plan.accepted_cursor.marker_flags ==
                ingress::kRawV1SegmentSealed,
        "single-segment init, record and flags0-to-seal chain is accepted");
    test->Expect(
        plan.segments.size() == 1U &&
            plan.segments.front().sealed &&
            plan.segments.front().has_accepted_marker &&
            plan.segments.front().accepted_marker_wire ==
                seal_marker &&
            plan.segments.front().validated_logical_end_offset ==
                segment.record_ends.back(),
        "sealed segment retains its exact accepted seal-marker wire");
}

void TestDuplicateAndRotation(TestContext* test) {
    const NamespaceFixture identity = MakeNamespace();
    const BuiltSegment first =
        MakeSegment(identity, 1U, 0U, 1U, {3U});
    const BuiltSegment second = MakeSegment(
        identity,
        2U,
        first.record_ends.back(),
        2U,
        {4U});
    const auto journal = MakeJournal(identity);
    const ingress::RawV1DurableMarkerWire initial =
        MakeMarker(
            identity,
            first,
            ingress::kRawV1SegmentHeaderBytes,
            0U,
            0U);
    AppendMarker(journal.get(), initial);
    AppendMarker(journal.get(), initial);
    const ingress::RawV1DurableMarkerWire first_seal =
        MakeMarker(
            identity,
            first,
            first.record_ends.back(),
            1U,
            ingress::kRawV1SegmentSealed);
    AppendMarker(journal.get(), first_seal);
    AppendMarker(
        journal.get(),
        MakeMarker(
            identity,
            second,
            ingress::kRawV1SegmentHeaderBytes,
            1U,
            0U));
    const ingress::RawV1DurableMarkerWire second_open =
        MakeMarker(
            identity,
            second,
            second.record_ends.back(),
            2U,
            0U);
    AppendMarker(journal.get(), second_open);

    const ingress::RawRecoveryPlanV1 plan =
        ingress::AnalyzeRawRecoveryV1(
            MakeInput(journal, {first, second}));
    test->Expect(
        plan.ok() && plan.segments.size() == 2U &&
            plan.segments[0].sealed &&
            plan.segments[0].has_accepted_marker &&
            plan.segments[0].accepted_marker_wire ==
                first_seal &&
            !plan.segments[1].sealed &&
            plan.segments[1].has_accepted_marker &&
            plan.segments[1].accepted_marker_wire ==
                second_open &&
            plan.accepted_cursor.segment_sequence == 2U,
        "each segment retains its own last exact accepted marker wire");
}

void TestJournalRollbackSuffixes(TestContext* test) {
    const NamespaceFixture identity = MakeNamespace();
    const BuiltSegment segment =
        MakeSegment(identity, 1U, 0U, 1U, {5U});
    const ingress::RawV1DurableMarkerWire initial =
        MakeMarker(
            identity,
            segment,
            ingress::kRawV1SegmentHeaderBytes,
            0U,
            0U);
    const ingress::RawV1DurableMarkerWire record_marker =
        MakeMarker(
            identity,
            segment,
            segment.record_ends.back(),
            1U,
            0U);

    auto partial = MakeJournal(identity);
    AppendMarker(partial.get(), initial);
    partial->insert(
        partial->end(),
        record_marker.begin(),
        record_marker.begin() + 17);
    const ingress::RawRecoveryPlanV1 partial_plan =
        ingress::AnalyzeRawRecoveryV1(
            MakeInput(partial, {segment}));
    test->Expect(
        partial_plan.ok() &&
            partial_plan.journal_tail ==
                ingress::RawRecoveryJournalTailV1::
                    kPartialMarker &&
            partial_plan.accepted_journal_size ==
                ingress::kRawV1JournalHeaderBytes +
                    ingress::kRawV1DurableMarkerBytes,
        "terminal sub-48-byte journal suffix is a rollback candidate");

    auto crc_invalid = MakeJournal(identity);
    AppendMarker(crc_invalid.get(), initial);
    ingress::RawV1DurableMarkerWire damaged = record_marker;
    damaged[ingress::raw_v1_offset::marker::kMarkerCrc32c] ^=
        std::byte{1U};
    AppendMarker(crc_invalid.get(), damaged);
    const ingress::RawRecoveryPlanV1 crc_plan =
        ingress::AnalyzeRawRecoveryV1(
            MakeInput(crc_invalid, {segment}));
    test->Expect(
        crc_plan.ok() &&
            crc_plan.journal_tail ==
                ingress::RawRecoveryJournalTailV1::
                    kTerminalCrcInvalidMarker &&
            crc_plan.segments.front()
                    .append_only_end_offset ==
                segment.record_ends.back(),
        "one terminal full-size CRC-invalid marker rolls back and exposes Raw suffix");

    auto middle = MakeJournal(identity);
    AppendMarker(middle.get(), initial);
    AppendMarker(middle.get(), damaged);
    AppendMarker(middle.get(), record_marker);
    const ingress::RawRecoveryPlanV1 middle_plan =
        ingress::AnalyzeRawRecoveryV1(
            MakeInput(middle, {segment}));
    test->Expect(
        middle_plan.fatal ==
            ingress::RawRecoveryFatalV1::
                kJournalMarkerCorruption,
        "CRC-invalid marker with later bytes is fatal middle corruption");
}

void TestMarkerSemanticAndDurableRawCorruption(
    TestContext* test) {
    const NamespaceFixture identity = MakeNamespace();
    const BuiltSegment segment =
        MakeSegment(identity, 1U, 0U, 1U, {7U});
    const ingress::RawV1DurableMarkerWire initial =
        MakeMarker(
            identity,
            segment,
            ingress::kRawV1SegmentHeaderBytes,
            0U,
            0U);

    auto non_boundary = MakeJournal(identity);
    AppendMarker(non_boundary.get(), initial);
    AppendMarker(
        non_boundary.get(),
        MakeMarker(
            identity,
            segment,
            segment.record_ends.back() - 1U,
            1U,
            0U));
    const ingress::RawRecoveryPlanV1 boundary_plan =
        ingress::AnalyzeRawRecoveryV1(
            MakeInput(non_boundary, {segment}));
    test->Expect(
        boundary_plan.fatal ==
            ingress::RawRecoveryFatalV1::
                kJournalSemanticViolation,
        "marker pointing inside a record is fatal journal semantic corruption");

    BuiltSegment corrupt = segment;
    corrupt.bytes =
        std::make_shared<std::vector<std::byte>>(
            *segment.bytes);
    (*corrupt.bytes)[
        ingress::kRawV1SegmentHeaderBytes +
        ingress::kRawV1RecordHeaderBytes +
        ingress::kVendorMessageHeadBytes] ^=
        std::byte{1U};
    auto durable = MakeJournal(identity);
    AppendMarker(durable.get(), initial);
    AppendMarker(
        durable.get(),
        MakeMarker(
            identity,
            corrupt,
            corrupt.record_ends.back(),
            1U,
            0U));
    const ingress::RawRecoveryPlanV1 corrupt_plan =
        ingress::AnalyzeRawRecoveryV1(
            MakeInput(durable, {corrupt}));
    test->Expect(
        corrupt_plan.fatal ==
            ingress::RawRecoveryFatalV1::
                kRawDurableCorruption,
        "Raw corruption inside a marker-proven range is fatal");
}

void TestAppendOnlyAndTailClassification(TestContext* test) {
    const NamespaceFixture identity = MakeNamespace();
    const BuiltSegment complete =
        MakeSegment(identity, 1U, 0U, 1U, {2U, 8U});
    const ingress::RawV1DurableMarkerWire initial =
        MakeMarker(
            identity,
            complete,
            ingress::kRawV1SegmentHeaderBytes,
            0U,
            0U);

    auto journal = MakeJournal(identity);
    AppendMarker(journal.get(), initial);
    const ingress::RawRecoveryPlanV1 append_plan =
        ingress::AnalyzeRawRecoveryV1(
            MakeInput(journal, {complete}));
    test->Expect(
        append_plan.ok() &&
            append_plan.segments.front()
                    .append_only_begin_offset ==
                ingress::kRawV1SegmentHeaderBytes &&
            append_plan.segments.front()
                    .append_only_end_offset ==
                complete.record_ends.back() &&
            append_plan.segments.front().tail ==
                ingress::RawRecoverySegmentTailV1::kNone,
        "complete records after durable boundary form exact append-only range");

    const BuiltSegment one =
        MakeSegment(identity, 1U, 0U, 1U, {2U});
    const BuiltSegment two =
        MakeSegment(identity, 1U, 0U, 1U, {2U, 8U});
    BuiltSegment partial = one;
    partial.bytes =
        std::make_shared<std::vector<std::byte>>(*one.bytes);
    partial.bytes->insert(
        partial.bytes->end(),
        two.bytes->begin() +
            static_cast<std::ptrdiff_t>(
                two.record_ends.front()),
        two.bytes->begin() +
            static_cast<std::ptrdiff_t>(
                two.record_ends.front() + 20U));
    const ingress::RawRecoveryPlanV1 partial_plan =
        ingress::AnalyzeRawRecoveryV1(
            MakeInput(journal, {partial}));
    test->Expect(
        partial_plan.ok() &&
            partial_plan.segments.front().tail ==
                ingress::RawRecoverySegmentTailV1::
                    kPartialRecord &&
            partial_plan.segments.front()
                    .append_only_end_offset ==
                one.record_ends.back(),
        "complete append-only record followed by short record prefix is partial tail");

    BuiltSegment invalid = complete;
    invalid.bytes =
        std::make_shared<std::vector<std::byte>>(
            *complete.bytes);
    (*invalid.bytes)[
        static_cast<std::size_t>(complete.record_ends.front()) +
        ingress::raw_v1_offset::record_header::
            kHeaderCrc32c] ^=
        std::byte{1U};
    const ingress::RawRecoveryPlanV1 invalid_plan =
        ingress::AnalyzeRawRecoveryV1(
            MakeInput(journal, {invalid}));
    test->Expect(
        invalid_plan.ok() &&
            invalid_plan.segments.front().tail ==
                ingress::RawRecoverySegmentTailV1::
                    kInvalidRecord &&
            invalid_plan.segments.front()
                    .append_only_end_offset ==
                complete.record_ends.front(),
        "invalid record after complete append-only prefix is classified without mutation");
}

void TestInitialAnchorWindows(TestContext* test) {
    const NamespaceFixture identity = MakeNamespace();
    const auto journal_only = MakeJournal(identity);
    const ingress::RawRecoveryPlanV1 anchor =
        ingress::AnalyzeRawRecoveryV1(
            MakeInput(journal_only, {}));
    test->Expect(
        anchor.ok() &&
            anchor.initial_anchor ==
                ingress::RawRecoveryInitialAnchorV1::
                    kJournalOnly &&
            !anchor.has_accepted_cursor,
        "header-only journal is classified as an initial anchor");

    BuiltSegment segment_anchor =
        MakeSegment(identity, 1U, 0U, 1U, {});
    segment_anchor.bytes->resize(8192U, std::byte{0});
    const ingress::RawRecoveryPlanV1 with_segment =
        ingress::AnalyzeRawRecoveryV1(
            MakeInput(journal_only, {segment_anchor}));
    test->Expect(
        with_segment.ok() &&
            with_segment.initial_anchor ==
                ingress::RawRecoveryInitialAnchorV1::
                    kSegmentHeaderOnly &&
            with_segment.segments.size() == 1U &&
            with_segment.segments.front().tail ==
                ingress::RawRecoverySegmentTailV1::
                    kZeroPreallocation,
        "one valid sequence-1 header plus zero preallocation is adoptable");

    (*segment_anchor.bytes)[
        ingress::kRawV1SegmentHeaderBytes] =
            std::byte{1U};
    const ingress::RawRecoveryPlanV1 dirty_segment =
        ingress::AnalyzeRawRecoveryV1(
            MakeInput(journal_only, {segment_anchor}));
    test->Expect(
        !dirty_segment.ok() &&
            dirty_segment.fatal ==
                ingress::RawRecoveryFatalV1::
                    kUnexpectedSegment,
        "unmarked nonzero segment bytes are fatal");
}

void TestR11OrphanClassification(TestContext* test) {
    const NamespaceFixture identity = MakeNamespace();
    const BuiltSegment first =
        MakeSegment(identity, 1U, 0U, 1U, {3U});
    BuiltSegment next = MakeSegment(
        identity,
        2U,
        first.record_ends.back(),
        2U,
        {});
    next.bytes->resize(8192U, std::byte{0});
    const auto journal = MakeJournal(identity);
    AppendMarker(
        journal.get(),
        MakeMarker(
            identity,
            first,
            ingress::kRawV1SegmentHeaderBytes,
            0U,
            0U));
    AppendMarker(
        journal.get(),
        MakeMarker(
            identity,
            first,
            first.record_ends.back(),
            1U,
            ingress::kRawV1SegmentSealed));

    const ingress::RawRecoveryPlanV1 orphan =
        ingress::AnalyzeRawRecoveryV1(
            MakeInput(journal, {first, next}));
    test->Expect(
        orphan.ok() &&
            orphan.r11_orphan ==
                ingress::RawRecoveryR11OrphanV1::
                    kNormalHeaderOnly &&
            orphan.segments.size() == 2U &&
            orphan.segments.back()
                    .validated_logical_end_offset ==
                ingress::kRawV1SegmentHeaderBytes &&
            orphan.segments.back().tail ==
                ingress::RawRecoverySegmentTailV1::
                    kZeroPreallocation,
        "R11-to-R12 normal orphan is classified from exact sealed-chain, header and zero-preallocation facts");

    (*next.bytes)[ingress::kRawV1SegmentHeaderBytes] =
        std::byte{1U};
    const ingress::RawRecoveryPlanV1 dirty =
        ingress::AnalyzeRawRecoveryV1(
            MakeInput(journal, {first, next}));
    test->Expect(
        !dirty.ok() &&
            dirty.fatal ==
                ingress::RawRecoveryFatalV1::
                    kUnexpectedSegment,
        "an orphan carrying any nonzero post-header bytes is fatal");
}

}  // namespace

int main() {
    TestContext test;
    TestInitializationRecordsAndSeal(&test);
    TestDuplicateAndRotation(&test);
    TestJournalRollbackSuffixes(&test);
    TestMarkerSemanticAndDurableRawCorruption(&test);
    TestAppendOnlyAndTailClassification(&test);
    TestInitialAnchorWindows(&test);
    TestR11OrphanClassification(&test);
    if (test.failures != 0) {
        std::cerr << test.failures
                  << " raw recovery test(s) failed\n";
        return 1;
    }
    std::cout << "Raw recovery tests passed\n";
    return 0;
}
