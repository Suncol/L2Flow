#include "l2flow/common/sha256.h"
#include "l2flow/ingress/raw_manifest_store.h"
#include "l2flow/ingress/raw_manifest_transition.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace {

namespace common = l2flow::common;
namespace ingress = l2flow::ingress;

int failures = 0;

void Expect(bool condition, std::string_view message) {
    if (!condition) {
        ++failures;
        std::cerr << "FAIL: " << message << '\n';
    }
}

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
    std::uint64_t vendor_sequence) {
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
    StoreU64(bytes, 15U, vendor_sequence);
    return head;
}

ingress::SegmentHeaderV1 MakeHeader(
    std::uint32_t segment_sequence,
    std::uint64_t segment_base_wal_pos,
    std::uint64_t first_ingress_sequence) {
    ingress::SegmentHeaderV1 header{};
    header.source_stream_id = 1001U;
    header.capture_date = 20260718U;
    header.stream_day_id = Pattern<16U>(0x01U);
    header.segment_sequence = segment_sequence;
    header.segment_base_wal_pos =
        segment_base_wal_pos;
    header.first_ingress_sequence =
        first_ingress_sequence;
    header.created_realtime_ns =
        1'000U + segment_sequence;
    header.created_monotonic_ns =
        900U + segment_sequence;
    header.host_uuid = Pattern<16U>(0x21U);
    header.linux_boot_id = Pattern<16U>(0x31U);
    header.clock_epoch_algorithm = 1U;
    header.clock_epoch_digest =
        Pattern<32U>(0x41U);
    header.clock_epoch_label = 77U;
    header.sdk_archive_sha256 =
        Pattern<32U>(0x61U);
    header.libmdl_api_sha256 =
        Pattern<32U>(0x81U);
    header.endpoint_contract_sha256 =
        Pattern<32U>(0xa1U);
    header.config_sha256 = Pattern<32U>(0xc1U);
    header.raw_schema_sha256 =
        Pattern<32U>(0x01U);
    header.build_manifest_sha256 =
        Pattern<32U>(0x21U);
    return header;
}

ingress::RawWalWriterSnapshot MakeInitializedSnapshot(
    const ingress::SegmentHeaderV1& header) {
    ingress::RawWalWriterSnapshot snapshot{};
    snapshot.append.segment_offset =
        ingress::kRawV1SegmentHeaderBytes;
    snapshot.append.global_wal_pos =
        header.segment_base_wal_pos +
        ingress::kRawV1SegmentHeaderBytes;
    snapshot.append.ingress_sequence =
        header.first_ingress_sequence - 1U;
    snapshot.durable = snapshot.append;
    const std::uint64_t minimum_marker_count =
        static_cast<std::uint64_t>(
            header.segment_sequence - 1U) *
            2U +
        1U;
    snapshot.journal_logical_size =
        ingress::kRawV1JournalHeaderBytes +
        minimum_marker_count *
            ingress::kRawV1DurableMarkerBytes;
    snapshot.initialized = true;
    return snapshot;
}

ingress::RawSealedSegmentMetadataV1 MakeSealedMetadata(
    const ingress::SegmentHeaderV1& header,
    std::span<const std::byte> record = {}) {
    ingress::RawV1SegmentHeaderWire header_wire{};
    Expect(
        ingress::EncodeSegmentHeaderV1(
            header, &header_wire) ==
            ingress::RawV1Error::kNone,
        "fixture header encodes");
    std::vector<std::byte> logical_bytes(
        header_wire.begin(), header_wire.end());
    logical_bytes.insert(
        logical_bytes.end(),
        record.begin(),
        record.end());

    ingress::RawSealedSegmentMetadataV1 metadata{};
    metadata.segment = header;
    metadata.logical_end_offset =
        logical_bytes.size();
    metadata.segment_sha256 =
        common::ComputeSha256(logical_bytes);
    metadata.index_sha256 = Pattern<32U>(0xe1U);
    if (!record.empty()) {
        metadata.record_count = 1U;
        metadata.actual_first_ingress_sequence =
            header.first_ingress_sequence;
        metadata.actual_last_ingress_sequence =
            header.first_ingress_sequence;
    }

    ingress::DurableMarkerV1 marker{};
    marker.source_stream_id = header.source_stream_id;
    marker.segment_sequence = header.segment_sequence;
    marker.durable_global_wal_pos =
        header.segment_base_wal_pos +
        metadata.logical_end_offset;
    marker.durable_ingress_sequence =
        record.empty()
            ? header.first_ingress_sequence - 1U
            : header.first_ingress_sequence;
    marker.durable_segment_offset =
        metadata.logical_end_offset;
    marker.marker_flags = ingress::kRawV1SegmentSealed;
    Expect(
        ingress::EncodeDurableMarkerV1(
            marker,
            &metadata.accepted_sealed_marker_bytes) ==
            ingress::RawV1Error::kNone,
        "fixture sealed marker encodes");
    Expect(
        ingress::DecodeDurableMarkerV1(
            metadata.accepted_sealed_marker_bytes,
            &metadata.accepted_sealed_marker) ==
            ingress::RawV1Error::kNone,
        "fixture sealed marker decodes");
    return metadata;
}

std::vector<std::byte> MakeOneRecord(
    const ingress::SegmentHeaderV1& header) {
    const std::vector<std::byte> body{
        std::byte{0x11},
        std::byte{0x22},
        std::byte{0x33},
        std::byte{0x44},
        std::byte{0x55}};
    ingress::RawRecordInputV1 input{};
    input.meta.source_stream_id =
        header.source_stream_id;
    input.meta.connection_epoch_hint = 9U;
    input.meta.ingress_sequence =
        header.first_ingress_sequence;
    input.meta.recv_realtime_ns = 2'001U;
    input.meta.recv_monotonic_ns = 1'501U;
    input.meta.capture_date = header.capture_date;
    input.vendor_head = MakeVendorHead(
        static_cast<std::uint32_t>(body.size()),
        901U);
    input.vendor_body = body;
    std::vector<std::byte> wire;
    Expect(
        ingress::EncodeRawRecordV1(
            input, &wire) ==
            ingress::RawV1Error::kNone,
        "fixture one-record wire encodes");
    return wire;
}

bool SameNamespace(
    const ingress::RawManifestNamespaceV1& left,
    const ingress::RawManifestNamespaceV1& right) {
    return left.capture_date == right.capture_date &&
           left.source_stream_id ==
               right.source_stream_id &&
           left.stream_day_id == right.stream_day_id;
}

bool SameEntry(
    const ingress::RawManifestSegmentEntryV1& left,
    const ingress::RawManifestSegmentEntryV1& right) {
    return SameNamespace(
               left.namespace_identity,
               right.namespace_identity) &&
           left.segment_sequence == right.segment_sequence &&
           left.segment_flags == right.segment_flags &&
           left.state == right.state &&
           left.segment_base_wal_pos ==
               right.segment_base_wal_pos &&
           left.segment_logical_length ==
               right.segment_logical_length &&
           left.segment_sha256 == right.segment_sha256 &&
           left.record_count == right.record_count &&
           left.actual_first_ingress_sequence ==
               right.actual_first_ingress_sequence &&
           left.actual_last_ingress_sequence ==
               right.actual_last_ingress_sequence &&
           left.next_expected_first_ingress_sequence ==
               right.next_expected_first_ingress_sequence &&
           left.accepted_marker_bytes ==
               right.accepted_marker_bytes &&
           left.accepted_marker_sha256 ==
               right.accepted_marker_sha256 &&
           left.host_uuid == right.host_uuid &&
           left.linux_boot_id == right.linux_boot_id &&
           left.clock_epoch_algorithm ==
               right.clock_epoch_algorithm &&
           left.clock_epoch_digest ==
               right.clock_epoch_digest &&
           left.clock_epoch_label ==
               right.clock_epoch_label &&
           left.sdk_archive_sha256 ==
               right.sdk_archive_sha256 &&
           left.libmdl_api_sha256 ==
               right.libmdl_api_sha256 &&
           left.endpoint_contract_sha256 ==
               right.endpoint_contract_sha256 &&
           left.config_sha256 == right.config_sha256 &&
           left.raw_schema_sha256 ==
               right.raw_schema_sha256 &&
           left.build_manifest_sha256 ==
               right.build_manifest_sha256 &&
           left.reserve_state_uuid ==
               right.reserve_state_uuid &&
           left.finalization_cycle_id ==
               right.finalization_cycle_id &&
           left.immutable_grant_sha256 ==
               right.immutable_grant_sha256 &&
           left.maintenance_report_locator ==
               right.maintenance_report_locator &&
           left.archive_locator == right.archive_locator;
}

std::string EncodeManifest(
    const ingress::RawManifestV1& manifest) {
    std::string encoded;
    Expect(
        ingress::EncodeRawManifestJcs(
            manifest, &encoded) ==
            ingress::RawManifestV1Error::kNone,
        "manifest fixture encodes");
    return encoded;
}

void ExpectStoreRoundTrip(
    const ingress::RawManifestV1& manifest,
    std::string_view context) {
    const std::string encoded = EncodeManifest(manifest);
    ingress::RawManifestV1 decoded{};
    ingress::RawManifestV1Error model_error =
        ingress::RawManifestV1Error::kNone;
    Expect(
        ingress::ParseRawManifestJcs(
            encoded, &decoded, &model_error) ==
            ingress::RawManifestStoreError::kNone,
        std::string(context) +
            " strict store parser accepts transition");
    Expect(
        EncodeManifest(decoded) == encoded,
        std::string(context) +
            " strict store parser preserves exact JCS");
    Expect(
        decoded.manifest_generation ==
                manifest.manifest_generation &&
            SameNamespace(
                decoded.namespace_identity,
                manifest.namespace_identity) &&
            decoded.closed_entry_count ==
                manifest.closed_entry_count &&
            decoded.closed_prefix_sha256 ==
                manifest.closed_prefix_sha256 &&
            decoded.closed_entries.size() ==
                manifest.closed_entries.size() &&
            decoded.open_entry.has_value() ==
                manifest.open_entry.has_value(),
        std::string(context) +
            " store model top-level fields match");
    for (std::size_t index = 0U;
         index < decoded.closed_entries.size() &&
         index < manifest.closed_entries.size();
         ++index) {
        Expect(
            SameEntry(
                decoded.closed_entries[index],
                manifest.closed_entries[index]),
            std::string(context) +
                " store closed entry fields match");
    }
    if (decoded.open_entry.has_value() &&
        manifest.open_entry.has_value()) {
        Expect(
            SameEntry(
                *decoded.open_entry,
                *manifest.open_entry),
            std::string(context) +
                " store open entry fields match");
    }
}

void TestExactOpenEntryAndEmptyTransitions() {
    const ingress::SegmentHeaderV1 first =
        MakeHeader(1U, 0U, 1U);
    const ingress::RawWalWriterSnapshot initialized =
        MakeInitializedSnapshot(first);

    ingress::RawManifestSegmentEntryV1 open{};
    const auto open_result =
        ingress::BuildOpenRawManifestEntryV1(
            first, initialized, &open);
    Expect(
        open_result ==
            ingress::RawManifestTransitionErrorV1::kNone,
        "header-only open entry builds");
    Expect(
        open.namespace_identity.capture_date ==
                first.capture_date &&
            open.namespace_identity.source_stream_id ==
                first.source_stream_id &&
            open.namespace_identity.stream_day_id ==
                first.stream_day_id &&
            open.segment_sequence ==
                first.segment_sequence &&
            open.segment_flags == 0U &&
            open.state ==
                ingress::RawManifestSegmentStateV1::kOpen &&
            open.segment_base_wal_pos == 0U &&
            open.segment_logical_length ==
                ingress::kRawV1SegmentHeaderBytes &&
            open.record_count == 0U &&
            !open.actual_first_ingress_sequence.has_value() &&
            !open.actual_last_ingress_sequence.has_value() &&
            open.next_expected_first_ingress_sequence == 1U,
        "open entry range and namespace fields are exact");
    Expect(
        open.host_uuid == first.host_uuid &&
            open.linux_boot_id == first.linux_boot_id &&
            open.clock_epoch_algorithm ==
                first.clock_epoch_algorithm &&
            open.clock_epoch_digest ==
                first.clock_epoch_digest &&
            open.clock_epoch_label ==
                first.clock_epoch_label &&
            open.sdk_archive_sha256 ==
                first.sdk_archive_sha256 &&
            open.libmdl_api_sha256 ==
                first.libmdl_api_sha256 &&
            open.endpoint_contract_sha256 ==
                first.endpoint_contract_sha256 &&
            open.config_sha256 ==
                first.config_sha256 &&
            open.raw_schema_sha256 ==
                first.raw_schema_sha256 &&
            open.build_manifest_sha256 ==
                first.build_manifest_sha256,
        "open entry copies every normal segment identity field");
    Expect(
        common::IsZeroIdentity(open.reserve_state_uuid) &&
            common::IsZeroIdentity(
                open.finalization_cycle_id) &&
            open.immutable_grant_sha256 ==
                ingress::RawV1Digest{} &&
            !open.maintenance_report_locator.has_value() &&
            !open.archive_locator.has_value(),
        "normal open entry has no fabricated finalization identity");

    ingress::RawV1SegmentHeaderWire header_wire{};
    Expect(
        ingress::EncodeSegmentHeaderV1(
            first, &header_wire) ==
            ingress::RawV1Error::kNone &&
            open.segment_sha256 ==
                common::ComputeSha256(header_wire),
        "open segment hash covers exact canonical header bytes");
    ingress::DurableMarkerV1 open_marker{};
    Expect(
        ingress::DecodeDurableMarkerV1(
            open.accepted_marker_bytes,
            &open_marker) ==
                ingress::RawV1Error::kNone &&
            open_marker.source_stream_id ==
                first.source_stream_id &&
            open_marker.segment_sequence == 1U &&
            open_marker.durable_global_wal_pos ==
                ingress::kRawV1SegmentHeaderBytes &&
            open_marker.durable_ingress_sequence == 0U &&
            open_marker.durable_segment_offset ==
                ingress::kRawV1SegmentHeaderBytes &&
            open_marker.marker_flags == 0U &&
            open.accepted_marker_sha256 ==
                ingress::ComputeAcceptedMarkerSha256(
                    open.accepted_marker_bytes),
        "open entry binds exact header-only marker bytes and hash");

    ingress::RawManifestV1 fresh{};
    Expect(
        ingress::BuildFreshOpenRawManifestV1(
            first, initialized, &fresh) ==
            ingress::RawManifestTransitionErrorV1::kNone,
        "fresh-open transition succeeds");
    Expect(
        fresh.manifest_generation == 1U &&
            fresh.closed_entries.empty() &&
            fresh.closed_entry_count == 0U &&
            fresh.open_entry.has_value() &&
            SameEntry(*fresh.open_entry, open) &&
            ingress::ValidateManifestModel(fresh) ==
                ingress::RawManifestV1Error::kNone,
        "fresh-open transition is a valid generation-one model");
    ExpectStoreRoundTrip(fresh, "fresh open");

    const ingress::RawV1Digest zero_frontier =
        fresh.closed_prefix_sha256;
    const ingress::RawSealedSegmentMetadataV1 empty_metadata =
        MakeSealedMetadata(first);
    ingress::RawManifestSegmentEntryV1 empty_closed{};
    Expect(
        ingress::BuildClosedRawManifestEntryV1(
            empty_metadata, &empty_closed) ==
            ingress::RawManifestTransitionErrorV1::kNone,
        "empty sealed metadata builds closed entry");
    Expect(
        empty_closed.record_count == 0U &&
            !empty_closed.actual_first_ingress_sequence
                 .has_value() &&
            !empty_closed.actual_last_ingress_sequence
                 .has_value() &&
            empty_closed.next_expected_first_ingress_sequence ==
                1U &&
            empty_closed.segment_logical_length ==
                ingress::kRawV1SegmentHeaderBytes,
        "empty closed entry preserves null actual range");

    ingress::RawManifestV1 closed{};
    Expect(
        ingress::TransitionOpenRawManifestToClosedV1(
            fresh, empty_metadata, &closed) ==
            ingress::RawManifestTransitionErrorV1::kNone,
        "open-to-empty-closed transition succeeds");
    Expect(
        closed.manifest_generation == 2U &&
            closed.closed_entry_count == 1U &&
            closed.closed_entries.size() == 1U &&
            !closed.open_entry.has_value() &&
            SameEntry(
                closed.closed_entries.front(),
                empty_closed) &&
            closed.closed_prefix_sha256 != zero_frontier &&
            ingress::ValidateManifestModel(
                closed, &fresh) ==
                ingress::RawManifestV1Error::kNone,
        "empty close advances generation and frontier append-only");
    ExpectStoreRoundTrip(closed, "empty closed");

    const ingress::SegmentHeaderV1 second = MakeHeader(
        2U,
        ingress::kRawV1SegmentHeaderBytes,
        1U);
    const auto second_initialized =
        MakeInitializedSnapshot(second);
    ingress::RawManifestV1 reopened{};
    Expect(
        ingress::
            TransitionClosedRawManifestToNextOpenV1(
                closed,
                second,
                second_initialized,
                &reopened) ==
            ingress::RawManifestTransitionErrorV1::kNone,
        "closed-to-next-open transition succeeds");
    Expect(
        reopened.manifest_generation == 3U &&
            reopened.closed_entry_count == 1U &&
            reopened.closed_entries.size() == 1U &&
            SameEntry(
                reopened.closed_entries.front(),
                closed.closed_entries.front()) &&
            reopened.open_entry.has_value() &&
            reopened.open_entry->segment_sequence == 2U &&
            reopened.closed_prefix_sha256 ==
                closed.closed_prefix_sha256 &&
            ingress::ValidateManifestModel(
                reopened, &closed) ==
                ingress::RawManifestV1Error::kNone,
        "next open preserves exact closed frontier");
    ExpectStoreRoundTrip(reopened, "next open");
}

void TestOneRecordClose() {
    const ingress::SegmentHeaderV1 header =
        MakeHeader(1U, 0U, 1U);
    ingress::RawManifestV1 fresh{};
    Expect(
        ingress::BuildFreshOpenRawManifestV1(
            header,
            MakeInitializedSnapshot(header),
            &fresh) ==
            ingress::RawManifestTransitionErrorV1::kNone,
        "one-record fixture starts fresh");

    const std::vector<std::byte> record =
        MakeOneRecord(header);
    const auto metadata =
        MakeSealedMetadata(header, record);
    ingress::RawManifestV1 closed{};
    Expect(
        ingress::TransitionOpenRawManifestToClosedV1(
            fresh, metadata, &closed) ==
            ingress::RawManifestTransitionErrorV1::kNone,
        "one-record segment closes");
    Expect(
        closed.closed_entries.size() == 1U &&
            closed.closed_entries[0U].record_count == 1U &&
            closed.closed_entries[0U]
                    .actual_first_ingress_sequence ==
                1U &&
            closed.closed_entries[0U]
                    .actual_last_ingress_sequence ==
                1U &&
            closed.closed_entries[0U]
                    .segment_logical_length ==
                ingress::kRawV1SegmentHeaderBytes +
                    record.size() &&
            closed.closed_entries[0U].segment_sha256 ==
                metadata.segment_sha256,
        "one-record close copies exact logical range and hash");
    ExpectStoreRoundTrip(closed, "one-record closed");
}

void TestFinalizationContinuationTransition() {
    const ingress::SegmentHeaderV1 first =
        MakeHeader(1U, 0U, 1U);
    ingress::RawManifestV1 first_open{};
    Expect(
        ingress::BuildFreshOpenRawManifestV1(
            first,
            MakeInitializedSnapshot(first),
            &first_open) ==
            ingress::RawManifestTransitionErrorV1::kNone,
        "continuation fixture starts with a normal open segment");
    const auto first_sealed =
        MakeSealedMetadata(first);
    ingress::RawManifestV1 first_closed{};
    Expect(
        ingress::TransitionOpenRawManifestToClosedV1(
            first_open,
            first_sealed,
            &first_closed) ==
            ingress::RawManifestTransitionErrorV1::kNone,
        "continuation fixture seals its normal predecessor");

    ingress::SegmentHeaderV1 continuation =
        MakeHeader(
            2U,
            first_sealed.logical_end_offset,
            1U);
    continuation.segment_flags =
        ingress::kRawV1FinalizationContinuation;
    continuation.reserve_state_uuid =
        Pattern<16U>(0x51U);
    continuation.finalization_cycle_id =
        Pattern<16U>(0x71U);
    continuation.immutable_grant_sha256 =
        Pattern<32U>(0x91U);
    const ingress::RawWalWriterSnapshot initialized =
        MakeInitializedSnapshot(continuation);

    ingress::RawManifestSegmentEntryV1 sentinel{};
    Expect(
        ingress::BuildOpenRawManifestEntryV1(
            continuation,
            initialized,
            &sentinel) ==
            ingress::RawManifestTransitionErrorV1::
                kFinalizationContinuationUnsupported,
        "normal open builder cannot manufacture a continuation");

    ingress::RawManifestSegmentEntryV1 open{};
    Expect(
        ingress::
            BuildOpenFinalizationContinuationRawManifestEntryV1(
                continuation,
                initialized,
                &open) ==
            ingress::RawManifestTransitionErrorV1::kNone,
        "typed continuation builder creates the header-only open barrier");
    std::string expected_report;
    std::string expected_archive;
    Expect(
        ingress::FinalizationContinuationLocatorsV1(
            continuation,
            &expected_report,
            &expected_archive) ==
                ingress::RawManifestTransitionErrorV1::kNone &&
            open.maintenance_report_locator ==
                expected_report &&
            open.archive_locator == expected_archive &&
            open.reserve_state_uuid ==
                continuation.reserve_state_uuid &&
            open.finalization_cycle_id ==
                continuation.finalization_cycle_id &&
            open.immutable_grant_sha256 ==
                continuation.immutable_grant_sha256,
        "open continuation binds exact grant-derived report and archive locators");

    ingress::RawManifestV1 with_open_continuation{};
    Expect(
        ingress::
            TransitionClosedRawManifestToFinalizationContinuationOpenV1(
                first_closed,
                continuation,
                initialized,
                &with_open_continuation) ==
                ingress::RawManifestTransitionErrorV1::kNone &&
            with_open_continuation.open_entry.has_value() &&
            SameEntry(
                *with_open_continuation.open_entry,
                open) &&
            with_open_continuation.closed_prefix_sha256 ==
                first_closed.closed_prefix_sha256 &&
            ingress::ValidateManifestModel(
                with_open_continuation,
                &first_closed) ==
                ingress::RawManifestV1Error::kNone,
        "continuation open manifest is append-only and preserves the closed frontier");
    ExpectStoreRoundTrip(
        with_open_continuation,
        "open finalization continuation");

    const std::vector<std::byte> record =
        MakeOneRecord(continuation);
    const auto continuation_sealed =
        MakeSealedMetadata(continuation, record);
    ingress::RawManifestSegmentEntryV1 closed_entry{};
    Expect(
        ingress::BuildClosedRawManifestEntryV1(
            continuation_sealed,
            &closed_entry) ==
            ingress::RawManifestTransitionErrorV1::
                kFinalizationContinuationUnsupported,
        "normal closed builder cannot manufacture continuation locators");
    Expect(
        ingress::
            BuildClosedFinalizationContinuationRawManifestEntryV1(
                continuation_sealed,
                &closed_entry) ==
                ingress::RawManifestTransitionErrorV1::kNone &&
            closed_entry.maintenance_report_locator ==
                expected_report &&
            closed_entry.archive_locator ==
                expected_archive,
        "typed closed builder re-derives the same immutable locators");

    ingress::RawManifestV1 terminal{};
    Expect(
        ingress::TransitionOpenRawManifestToClosedV1(
            with_open_continuation,
            continuation_sealed,
            &terminal) ==
                ingress::RawManifestTransitionErrorV1::kNone &&
            !terminal.open_entry.has_value() &&
            terminal.closed_entries.size() == 2U &&
            SameEntry(
                terminal.closed_entries.back(),
                closed_entry) &&
            ingress::ValidateManifestModel(
                terminal,
                &with_open_continuation) ==
                ingress::RawManifestV1Error::kNone,
        "generic seal transition preserves the typed continuation identity");
    ExpectStoreRoundTrip(
        terminal,
        "closed finalization continuation");

    const std::array<
        ingress::RawSealedSegmentMetadataV1,
        2U>
        recovered_segments{
            first_sealed,
            continuation_sealed};
    ingress::RawManifestV1 recovered{};
    Expect(
        ingress::BuildRecoveredClosedRawManifestV1(
            recovered_segments,
            &with_open_continuation,
            &recovered) ==
                ingress::RawManifestTransitionErrorV1::kNone &&
            EncodeManifest(recovered) ==
                EncodeManifest(terminal),
        "recovery seals an already represented open continuation without inventing locators");
    ingress::RawManifestV1 absent_manifest_sentinel =
        terminal;
    const std::string absent_manifest_before =
        EncodeManifest(absent_manifest_sentinel);
    Expect(
        ingress::BuildRecoveredClosedRawManifestV1(
            recovered_segments,
            nullptr,
            &absent_manifest_sentinel) ==
                ingress::RawManifestTransitionErrorV1::
                    kFinalizationContinuationUnsupported &&
            EncodeManifest(absent_manifest_sentinel) ==
                absent_manifest_before,
        "metadata alone cannot rebuild continuation audit locators without existing durable evidence");
}

void TestRecoveredClosedManifestConstruction() {
    const ingress::SegmentHeaderV1 first =
        MakeHeader(1U, 0U, 1U);
    const ingress::RawSealedSegmentMetadataV1 first_empty =
        MakeSealedMetadata(first);
    const ingress::SegmentHeaderV1 second = MakeHeader(
        2U,
        first_empty.logical_end_offset,
        1U);
    const ingress::RawSealedSegmentMetadataV1 second_empty =
        MakeSealedMetadata(second);
    const std::vector<ingress::RawSealedSegmentMetadataV1>
        complete{first_empty, second_empty};

    ingress::RawManifestV1 rebuilt{};
    Expect(
        ingress::BuildRecoveredClosedRawManifestV1(
            complete, nullptr, &rebuilt) ==
            ingress::RawManifestTransitionErrorV1::kNone,
        "missing manifest rebuilds from the complete sealed metadata set");
    Expect(
        rebuilt.manifest_generation == 1U &&
            rebuilt.closed_entries.size() == 2U &&
            rebuilt.closed_entry_count == 2U &&
            !rebuilt.open_entry.has_value() &&
            rebuilt.closed_entries[0U].segment_sequence ==
                1U &&
            rebuilt.closed_entries[1U].segment_sequence ==
                2U &&
            ingress::ValidateManifestModel(rebuilt) ==
                ingress::RawManifestV1Error::kNone,
        "recovered manifest is closed-only, ordered and intrinsically valid");

    ingress::RawManifestV1 fresh_open{};
    Expect(
        ingress::BuildFreshOpenRawManifestV1(
            first,
            MakeInitializedSnapshot(first),
            &fresh_open) ==
            ingress::RawManifestTransitionErrorV1::kNone,
        "recovered-open predecessor fixture builds");
    ingress::RawManifestV1 converted{};
    const std::array<ingress::RawSealedSegmentMetadataV1, 1U>
        only_first{first_empty};
    Expect(
        ingress::BuildRecoveredClosedRawManifestV1(
            only_first,
            &fresh_open,
            &converted) ==
            ingress::RawManifestTransitionErrorV1::kNone &&
            converted.manifest_generation ==
                fresh_open.manifest_generation + 1U &&
            converted.closed_entries.size() == 1U &&
            !converted.open_entry.has_value() &&
            ingress::ValidateManifestModel(
                converted, &fresh_open) ==
                ingress::RawManifestV1Error::kNone,
        "the existing open is the sole first newly closed entry");

    ingress::RawManifestV1 idempotent{};
    Expect(
        ingress::BuildRecoveredClosedRawManifestV1(
            only_first,
            &converted,
            &idempotent) ==
                ingress::RawManifestTransitionErrorV1::kNone &&
            EncodeManifest(idempotent) ==
                EncodeManifest(converted),
        "an already closed exact prefix is returned without a fabricated generation");

    ingress::RawManifestV1 sentinel = converted;
    const std::string sentinel_bytes =
        EncodeManifest(sentinel);
    const std::vector<std::byte> first_record =
        MakeOneRecord(first);
    const ingress::RawSealedSegmentMetadataV1
        conflicting_first =
            MakeSealedMetadata(first, first_record);
    const std::array<ingress::RawSealedSegmentMetadataV1, 1U>
        conflicting_prefix{conflicting_first};
    Expect(
        ingress::BuildRecoveredClosedRawManifestV1(
            conflicting_prefix,
            &converted,
            &sentinel) ==
                ingress::RawManifestTransitionErrorV1::
                    kRecoveredClosedPrefixConflict &&
            EncodeManifest(sentinel) == sentinel_bytes,
        "closed segment hash and accepted-marker conflicts are fatal and output-atomic");

    ingress::SegmentHeaderV1 foreign_first = first;
    foreign_first.host_uuid[0U] ^= std::byte{0x01};
    const std::array<ingress::RawSealedSegmentMetadataV1, 1U>
        foreign_open{
            MakeSealedMetadata(foreign_first)};
    Expect(
        ingress::BuildRecoveredClosedRawManifestV1(
            foreign_open,
            &fresh_open,
            &sentinel) ==
                ingress::RawManifestTransitionErrorV1::
                    kRecoveredOpenConflict &&
            EncodeManifest(sentinel) == sentinel_bytes,
        "old open immutable identity conflict is fatal and output-atomic");

    Expect(
        ingress::BuildRecoveredClosedRawManifestV1(
            complete,
            &converted,
            &sentinel) ==
                ingress::RawManifestTransitionErrorV1::
                    kRecoveredClosedPrefixConflict &&
            EncodeManifest(sentinel) == sentinel_bytes,
        "a closed predecessor without an open entry cannot gain an unrepresented segment");

    const std::array<ingress::RawSealedSegmentMetadataV1, 2U>
        reversed{second_empty, first_empty};
    Expect(
        ingress::BuildRecoveredClosedRawManifestV1(
            reversed,
            nullptr,
            &sentinel) ==
                ingress::RawManifestTransitionErrorV1::
                    kRecoveredMetadataOrder &&
            EncodeManifest(sentinel) == sentinel_bytes,
        "recovered metadata must begin at one and remain strictly contiguous");
}

void TestFailureAtomicityAndOverflow() {
    const ingress::SegmentHeaderV1 first =
        MakeHeader(1U, 0U, 1U);
    const ingress::RawWalWriterSnapshot initialized =
        MakeInitializedSnapshot(first);
    ingress::RawManifestSegmentEntryV1 entry{};
    Expect(
        ingress::BuildOpenRawManifestEntryV1(
            first, initialized, &entry) ==
            ingress::RawManifestTransitionErrorV1::kNone,
        "entry sentinel builds");
    const ingress::RawManifestSegmentEntryV1
        entry_before = entry;
    ingress::RawWalWriterSnapshot invalid_snapshot =
        initialized;
    invalid_snapshot.durable.segment_offset += 8U;
    Expect(
        ingress::BuildOpenRawManifestEntryV1(
            first, invalid_snapshot, &entry) ==
                ingress::RawManifestTransitionErrorV1::
                    kInvalidInitializedSnapshot &&
            SameEntry(entry, entry_before),
        "invalid open input leaves entry output unchanged");

    ingress::SegmentHeaderV1 continuation = first;
    continuation.segment_flags =
        ingress::kRawV1FinalizationContinuation;
    continuation.reserve_state_uuid =
        Pattern<16U>(0x51U);
    continuation.finalization_cycle_id =
        Pattern<16U>(0x71U);
    continuation.immutable_grant_sha256 =
        Pattern<32U>(0x91U);
    Expect(
        ingress::BuildOpenRawManifestEntryV1(
            continuation, initialized, &entry) ==
                ingress::RawManifestTransitionErrorV1::
                    kFinalizationContinuationUnsupported &&
            SameEntry(entry, entry_before),
        "normal builder rejects continuation without inventing locators");

    auto invalid_closed_metadata =
        MakeSealedMetadata(first);
    invalid_closed_metadata.index_sha256 = {};
    Expect(
        ingress::BuildClosedRawManifestEntryV1(
            invalid_closed_metadata, &entry) ==
                ingress::RawManifestTransitionErrorV1::
                    kInvalidSealedMetadata &&
            SameEntry(entry, entry_before),
        "invalid sealed metadata leaves entry output unchanged");

    ingress::RawManifestV1 fresh{};
    Expect(
        ingress::BuildFreshOpenRawManifestV1(
            first, initialized, &fresh) ==
            ingress::RawManifestTransitionErrorV1::kNone,
        "manifest sentinel builds");
    ingress::RawManifestV1 output = fresh;
    const std::string output_before =
        EncodeManifest(output);

    ingress::SegmentHeaderV1 mismatched_header = first;
    mismatched_header.host_uuid[0U] ^=
        std::byte{0x01};
    const auto mismatched_metadata =
        MakeSealedMetadata(mismatched_header);
    Expect(
        ingress::TransitionOpenRawManifestToClosedV1(
            fresh, mismatched_metadata, &output) ==
                ingress::RawManifestTransitionErrorV1::
                    kSegmentIdentityMismatch &&
            EncodeManifest(output) == output_before,
        "identity mismatch leaves manifest output unchanged");

    ingress::RawManifestV1 maximum_generation = fresh;
    maximum_generation.manifest_generation =
        std::numeric_limits<std::uint64_t>::max();
    Expect(
        ingress::ValidateManifestModel(
            maximum_generation) ==
            ingress::RawManifestV1Error::kNone,
        "maximum-generation predecessor is valid");
    Expect(
        ingress::TransitionOpenRawManifestToClosedV1(
            maximum_generation,
            MakeSealedMetadata(first),
            &output) ==
                ingress::RawManifestTransitionErrorV1::
                    kGenerationOverflow &&
            EncodeManifest(output) == output_before,
        "generation overflow is checked and output-atomic");

    ingress::RawManifestV1 closed{};
    Expect(
        ingress::TransitionOpenRawManifestToClosedV1(
            fresh,
            MakeSealedMetadata(first),
            &closed) ==
            ingress::RawManifestTransitionErrorV1::kNone,
        "identity test predecessor closes");
    ingress::SegmentHeaderV1 wrong_namespace = MakeHeader(
        2U,
        ingress::kRawV1SegmentHeaderBytes,
        1U);
    wrong_namespace.stream_day_id[0U] ^=
        std::byte{0x01};
    Expect(
        ingress::
            TransitionClosedRawManifestToNextOpenV1(
                closed,
                wrong_namespace,
                MakeInitializedSnapshot(wrong_namespace),
                &output) ==
                ingress::RawManifestTransitionErrorV1::
                    kSegmentIdentityMismatch &&
            EncodeManifest(output) == output_before,
        "next-open namespace mismatch is fail-closed and output-atomic");

    ingress::RawManifestV1 maximum_closed_generation =
        closed;
    maximum_closed_generation.manifest_generation =
        std::numeric_limits<std::uint64_t>::max();
    const ingress::SegmentHeaderV1 second = MakeHeader(
        2U,
        ingress::kRawV1SegmentHeaderBytes,
        1U);
    Expect(
        ingress::
            TransitionClosedRawManifestToNextOpenV1(
                maximum_closed_generation,
                second,
                MakeInitializedSnapshot(second),
                &output) ==
                ingress::RawManifestTransitionErrorV1::
                    kGenerationOverflow &&
            EncodeManifest(output) == output_before,
        "next-open generation overflow is checked and output-atomic");
}

}  // namespace

int main() {
    TestExactOpenEntryAndEmptyTransitions();
    TestOneRecordClose();
    TestFinalizationContinuationTransition();
    TestRecoveredClosedManifestConstruction();
    TestFailureAtomicityAndOverflow();
    if (failures != 0) {
        std::cerr << failures
                  << " Phase 2 RawManifest transition tests failed\n";
        return 1;
    }
    std::cout
        << "Phase 2 RawManifest transition tests passed\n";
    return 0;
}
