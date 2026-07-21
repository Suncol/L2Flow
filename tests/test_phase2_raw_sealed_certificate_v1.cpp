#include "l2flow/common/sha256.h"
#include "l2flow/ingress/raw_manifest_transition.h"
#include "l2flow/ingress/raw_schema.h"
#include "l2flow/ingress/raw_sealed_certificate_v1.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <type_traits>

namespace common = l2flow::common;
namespace ingress = l2flow::ingress;

namespace {

static_assert(
    !std::is_default_constructible_v<
        ingress::BuiltSealedRawCertificateV1>);
static_assert(
    !std::is_copy_constructible_v<
        ingress::BuiltSealedRawCertificateV1>);
static_assert(
    !std::is_move_constructible_v<
        ingress::BuiltSealedRawCertificateV1>);

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

ingress::RawWalWriterSnapshot MakeInitialized() {
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
        ingress::kRawV1SegmentHeaderBytes;
    marker.durable_ingress_sequence = 0U;
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

ingress::RawV1JournalHeaderWire MakeJournal(
    const ingress::SegmentHeaderV1& segment) {
    ingress::DurableJournalHeaderV1 journal{};
    journal.capture_date = segment.capture_date;
    journal.source_stream_id =
        segment.source_stream_id;
    journal.stream_day_id =
        segment.stream_day_id;
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

void CheckCertificate(TestContext* test) {
    const ingress::SegmentHeaderV1 segment =
        MakeSegment();
    ingress::RawManifestV1 open{};
    test->Expect(
        ingress::BuildFreshOpenRawManifestV1(
            segment, MakeInitialized(), &open) ==
            ingress::RawManifestTransitionErrorV1::kNone,
        "fresh open manifest fixture builds");
    const ingress::RawSealedSegmentMetadataV1 metadata =
        MakeSealedMetadata(segment);
    ingress::RawManifestV1 closed{};
    test->Expect(
        ingress::TransitionOpenRawManifestToClosedV1(
            open, metadata, &closed) ==
            ingress::RawManifestTransitionErrorV1::kNone,
        "terminal closed manifest fixture builds");

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

    ingress::SealedRawCertificateV1 certificate{};
    const ingress::RawV1JournalHeaderWire journal =
        MakeJournal(segment);
    test->Expect(
        ingress::BuildSealedRawCertificateV1(
            journal,
            closed,
            sink,
            final_wal,
            &certificate) ==
            ingress::SealedRawCertificateV1Error::kNone,
        "terminal Raw facts build one sealed certificate");

    std::unique_ptr<
        ingress::BuiltSealedRawCertificateV1>
        built_certificate;
    test->Expect(
        ingress::
            BuildSealedRawCertificateCapabilityV1(
                journal,
                closed,
                sink,
                final_wal,
                &built_certificate) ==
                ingress::SealedRawCertificateV1Error::
                    kNone &&
            built_certificate != nullptr,
        "terminal Raw facts build the private publication capability");
    test->Expect(
        ingress::ValidateSealedRawCertificateV1(
            certificate) ==
            ingress::SealedRawCertificateV1Error::kNone,
        "built sealed certificate validates intrinsically");

    std::string encoded;
    test->Expect(
        ingress::EncodeSealedRawCertificateV1Jcs(
            certificate, &encoded) ==
            ingress::SealedRawCertificateV1Error::kNone &&
            !encoded.empty() &&
            encoded.front() == '{' &&
            encoded.back() == '}' &&
            encoded.find('\n') == std::string::npos &&
            encoded.size() <=
                ingress::
                    kSealedRawCertificateV1MaximumBytes,
        "sealed certificate emits bounded canonical bytes without newline");
    const std::string encoded_hash =
        common::Sha256Hex(
            common::ComputeSha256(encoded));
    if (encoded_hash !=
        "0382405b2031e4b53fa3f2bca4d9a832"
        "c1f30b25ff63e02c19e9047f1e833cff") {
        std::cerr << "observed certificate golden digest: "
                  << encoded_hash << '\n';
    }
    test->Expect(
        encoded_hash ==
            "0382405b2031e4b53fa3f2bca4d9a832"
            "c1f30b25ff63e02c19e9047f1e833cff",
        "sealed certificate canonical bytes retain their golden digest");
    test->Expect(
        built_certificate != nullptr &&
            built_certificate->canonical_jcs() ==
                encoded &&
            built_certificate->certificate_sha256() ==
                common::ComputeSha256(encoded),
        "publication capability freezes exact canonical bytes and digest");

    ingress::SealedRawCertificateV1 parsed{};
    test->Expect(
        ingress::ParseSealedRawCertificateV1Jcs(
            encoded, &parsed) ==
                ingress::SealedRawCertificateV1Error::kNone &&
            parsed.namespace_identity.stream_day_id ==
                certificate.namespace_identity.stream_day_id &&
            parsed.closed_prefix_sha256 ==
                certificate.closed_prefix_sha256 &&
            parsed.accepted_sealed_marker_bytes ==
                certificate.accepted_sealed_marker_bytes,
        "strict parser round-trips all causal certificate facts");

    const ingress::SealedRawCertificateV1 sentinel =
        parsed;
    std::string noncanonical = encoded + "\n";
    test->Expect(
        ingress::ParseSealedRawCertificateV1Jcs(
            noncanonical, &parsed) ==
                ingress::SealedRawCertificateV1Error::
                    kInvalidCanonicalJson &&
            parsed.closed_prefix_sha256 ==
                sentinel.closed_prefix_sha256,
        "trailing bytes are rejected without changing parser output");
    noncanonical = encoded;
    const std::size_t marker_global =
        noncanonical.find("\"global_wal_pos\":\"4096\"");
    test->Expect(
        marker_global != std::string::npos,
        "golden contains the marker global cursor");
    if (marker_global != std::string::npos) {
        noncanonical.replace(
            marker_global,
            std::string(
                "\"global_wal_pos\":\"4096\"").size(),
            "\"global_wal_pos\":\"4097\"");
        test->Expect(
            ingress::ParseSealedRawCertificateV1Jcs(
                noncanonical, &parsed) ==
                ingress::SealedRawCertificateV1Error::
                    kInvalidCanonicalJson,
            "redundant marker facts cannot disagree with terminal cursor");
    }

    std::string filename;
    test->Expect(
        ingress::SealedRawCertificateV1Filename(
            certificate, &filename) ==
            ingress::SealedRawCertificateV1Error::kNone &&
            filename ==
                "sealed-raw-0102030405060708090a0b0c0d0e0f10-" +
                    common::Sha256Hex(
                        closed.closed_prefix_sha256) +
                    ".json",
        "sealed certificate locator is derived from stream-day and frontier");
    test->Expect(
        built_certificate != nullptr &&
            built_certificate->filename() == filename,
        "publication capability freezes the deterministic locator");

    ingress::SealedRawCertificateV1 tampered =
        certificate;
    tampered.terminal_durable_cursor.global_wal_pos +=
        1U;
    test->Expect(
        ingress::ValidateSealedRawCertificateV1(
            tampered) ==
            ingress::SealedRawCertificateV1Error::
                kTerminalCursorMismatch,
        "same namespace with a different terminal cursor is rejected");
    test->Expect(
        built_certificate != nullptr &&
            built_certificate->model()
                    .terminal_durable_cursor
                    .global_wal_pos !=
                tampered.terminal_durable_cursor
                    .global_wal_pos &&
            built_certificate->canonical_jcs() ==
                encoded,
        "mutable read-only models cannot alter a built publication capability");

    ingress::RawManifestV1 nonterminal = closed;
    nonterminal.open_entry =
        open.open_entry;
    test->Expect(
        ingress::BuildSealedRawCertificateV1(
            journal,
            nonterminal,
            sink,
            final_wal,
            &certificate) !=
            ingress::SealedRawCertificateV1Error::kNone,
        "a manifest with an open entry cannot be certified terminal");
}

}  // namespace

int main() {
    TestContext test;
    CheckCertificate(&test);
    if (test.failures != 0) {
        std::cerr << test.failures
                  << " sealed-certificate assertion(s) failed\n";
        return 1;
    }
    std::cout
        << "Phase 2 sealed Raw certificate checks passed\n";
    return 0;
}
