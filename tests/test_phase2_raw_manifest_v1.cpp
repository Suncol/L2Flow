#include "l2flow/common/identity128.h"
#include "l2flow/common/sha256.h"
#include "l2flow/ingress/raw_manifest_v1.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <string>
#include <string_view>

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
                start + static_cast<std::uint8_t>(index)));
    }
    return value;
}

ingress::RawManifestNamespaceV1 MakeNamespace() {
    ingress::RawManifestNamespaceV1 value{};
    value.capture_date = 20260718U;
    value.source_stream_id = 1001U;
    value.stream_day_id = Pattern<16U>(0x01U);
    return value;
}

ingress::RawManifestSegmentEntryV1 MakeEntry(
    std::uint32_t segment_sequence,
    std::uint64_t base,
    std::uint64_t logical_length,
    std::uint64_t expected_first,
    std::uint64_t record_count,
    std::uint64_t previous_ingress,
    ingress::RawManifestSegmentStateV1 state) {
    ingress::RawManifestSegmentEntryV1 entry{};
    entry.namespace_identity = MakeNamespace();
    entry.segment_sequence = segment_sequence;
    entry.state = state;
    entry.segment_base_wal_pos = base;
    entry.segment_logical_length = logical_length;
    entry.segment_sha256 = Pattern<32U>(0x10U);
    entry.record_count = record_count;
    entry.next_expected_first_ingress_sequence = expected_first;
    std::uint64_t marker_ingress = previous_ingress;
    if (record_count != 0U) {
        entry.actual_first_ingress_sequence = expected_first;
        entry.actual_last_ingress_sequence =
            expected_first + record_count - 1U;
        marker_ingress = *entry.actual_last_ingress_sequence;
    }

    ingress::DurableMarkerV1 marker{};
    marker.source_stream_id =
        entry.namespace_identity.source_stream_id;
    marker.segment_sequence = segment_sequence;
    marker.durable_global_wal_pos = base + logical_length;
    marker.durable_ingress_sequence = marker_ingress;
    marker.durable_segment_offset = logical_length;
    marker.marker_flags =
        state == ingress::RawManifestSegmentStateV1::kClosed
            ? ingress::kRawV1SegmentSealed
            : 0U;
    const ingress::RawV1Error marker_result =
        ingress::EncodeDurableMarkerV1(
            marker, &entry.accepted_marker_bytes);
    Expect(
        marker_result == ingress::RawV1Error::kNone,
        "test marker encodes");
    entry.accepted_marker_sha256 =
        ingress::ComputeAcceptedMarkerSha256(
            entry.accepted_marker_bytes);

    entry.host_uuid = Pattern<16U>(0x21U);
    entry.linux_boot_id = Pattern<16U>(0x31U);
    entry.clock_epoch_algorithm = 1U;
    entry.clock_epoch_digest = Pattern<32U>(0x41U);
    entry.clock_epoch_label =
        std::numeric_limits<std::uint64_t>::max() - 1U;
    entry.sdk_archive_sha256 = Pattern<32U>(0x61U);
    entry.libmdl_api_sha256 = Pattern<32U>(0x81U);
    entry.endpoint_contract_sha256 = Pattern<32U>(0xa1U);
    entry.config_sha256 = Pattern<32U>(0xc1U);
    entry.raw_schema_sha256 = Pattern<32U>(0x01U);
    entry.build_manifest_sha256 = Pattern<32U>(0x21U);
    return entry;
}

bool SetClosedPrefix(ingress::RawManifestV1* manifest) {
    ingress::RawV1Digest digest{};
    const ingress::RawManifestV1Error result =
        ingress::ComputeClosedPrefix(*manifest, &digest);
    if (result != ingress::RawManifestV1Error::kNone) {
        std::cerr << "ComputeClosedPrefix failed: "
                  << ingress::RawManifestV1ErrorName(result)
                  << '\n';
        ++failures;
        return false;
    }
    manifest->closed_prefix_sha256 = digest;
    return true;
}

std::string EncodedSha256(std::string_view encoded) {
    return common::Sha256Hex(common::ComputeSha256(encoded));
}

void TestEmptyGolden() {
    ingress::RawManifestV1 manifest{};
    manifest.manifest_generation = 1U;
    manifest.namespace_identity = MakeNamespace();
    manifest.closed_entry_count = 0U;

    std::string frontier;
    ingress::RawV1Digest digest{};
    const auto prefix_result = ingress::ComputeClosedPrefix(
        manifest, &digest, &frontier);
    Expect(
        prefix_result == ingress::RawManifestV1Error::kNone,
        "empty frontier computes");
    constexpr std::string_view kExpectedFrontier =
        "{\"closed_entries\":[],\"closed_entry_count\":\"0\","
        "\"namespace\":{\"capture_date\":20260718,"
        "\"source_stream_id\":1001,"
        "\"stream_day_id\":\"0102030405060708090a0b0c0d0e0f10\"},"
        "\"schema_version\":1}";
    Expect(frontier == kExpectedFrontier, "empty frontier golden bytes");
    Expect(
        common::Sha256Hex(digest) ==
            "4724e3c136b0e2b9957f5d4cbc43a7ee"
            "22a718a5dcdbd705da31c03be05c4998",
        "empty frontier golden SHA-256");
    manifest.closed_prefix_sha256 = digest;

    std::string encoded = "unchanged";
    const auto encode_result =
        ingress::EncodeRawManifestJcs(manifest, &encoded);
    Expect(
        encode_result == ingress::RawManifestV1Error::kNone,
        "empty manifest encodes");
    constexpr std::string_view kExpectedManifest =
        "{\"closed_entries\":[],\"closed_entry_count\":\"0\","
        "\"closed_prefix_sha256\":"
        "\"4724e3c136b0e2b9957f5d4cbc43a7ee"
        "22a718a5dcdbd705da31c03be05c4998\","
        "\"manifest_generation\":\"1\","
        "\"namespace\":{\"capture_date\":20260718,"
        "\"source_stream_id\":1001,"
        "\"stream_day_id\":\"0102030405060708090a0b0c0d0e0f10\"},"
        "\"open_entry\":null,\"schema_version\":1}";
    Expect(encoded == kExpectedManifest, "empty manifest golden bytes");
    Expect(
        encoded.empty() || encoded.back() != '\n',
        "manifest has no trailing newline");
}

void TestOpenGoldenAndLargeIntegers() {
    ingress::RawManifestV1 manifest{};
    manifest.manifest_generation =
        std::numeric_limits<std::uint64_t>::max();
    manifest.namespace_identity = MakeNamespace();
    manifest.open_entry = MakeEntry(
        1U,
        0U,
        ingress::kRawV1SegmentHeaderBytes,
        1U,
        0U,
        0U,
        ingress::RawManifestSegmentStateV1::kOpen);
    manifest.closed_entry_count = 0U;
    if (!SetClosedPrefix(&manifest)) {
        return;
    }

    std::string encoded;
    const auto result =
        ingress::EncodeRawManifestJcs(manifest, &encoded);
    Expect(
        result == ingress::RawManifestV1Error::kNone,
        "open manifest encodes");
    Expect(
        encoded.find(
            "\"manifest_generation\":"
            "\"18446744073709551615\"") != std::string::npos,
        "uint64 max is a JSON decimal string");
    Expect(
        encoded.find(
            "\"clock_epoch_label\":"
            "\"18446744073709551614\"") != std::string::npos,
        "large clock label is a JSON decimal string");
    Expect(
        encoded.find("\"record_count\":\"0\"") !=
            std::string::npos,
        "record count is a JSON string");
    Expect(
        encoded.find(
            "\"actual_first_ingress_sequence\":null,"
            "\"actual_last_ingress_sequence\":null") !=
            std::string::npos,
        "empty segment actual range is null");

    const std::string actual_sha = EncodedSha256(encoded);
    constexpr std::string_view kOpenGoldenSha256 =
        "979cdfe509b57e05ab045d2a442dd590"
        "ba7ac889de83da5ffa6aba0581f29003";
    if (actual_sha != kOpenGoldenSha256) {
        std::cerr << "open golden actual SHA-256: "
                  << actual_sha << '\n';
    }
    Expect(
        actual_sha == kOpenGoldenSha256,
        "open manifest complete-byte golden SHA-256");
}

ingress::RawManifestV1 MakeOneClosedManifest() {
    ingress::RawManifestV1 manifest{};
    manifest.manifest_generation = 2U;
    manifest.namespace_identity = MakeNamespace();
    manifest.closed_entries.push_back(MakeEntry(
        1U,
        0U,
        4224U,
        1U,
        1U,
        0U,
        ingress::RawManifestSegmentStateV1::kClosed));
    manifest.closed_entry_count = 1U;
    static_cast<void>(SetClosedPrefix(&manifest));
    return manifest;
}

void TestClosedGoldenAndStableFrontier() {
    ingress::RawManifestV1 closed = MakeOneClosedManifest();
    std::string encoded;
    const auto result =
        ingress::EncodeRawManifestJcs(closed, &encoded);
    Expect(
        result == ingress::RawManifestV1Error::kNone,
        "closed manifest encodes");
    const std::string actual_sha = EncodedSha256(encoded);
    constexpr std::string_view kClosedGoldenSha256 =
        "53221d7f94b103c8cc1992de796aca8b"
        "5a0cca72c1631b1030a3c5315d95473c";
    if (actual_sha != kClosedGoldenSha256) {
        std::cerr << "closed golden actual SHA-256: "
                  << actual_sha << '\n';
    }
    Expect(
        actual_sha == kClosedGoldenSha256,
        "closed manifest complete-byte golden SHA-256");
    const std::string closed_prefix_sha =
        common::Sha256Hex(closed.closed_prefix_sha256);
    constexpr std::string_view kClosedPrefixGoldenSha256 =
        "fa7bb723c0f4c2bb95406b5981efd70f"
        "36837ff17303005065350947daae04c2";
    if (closed_prefix_sha != kClosedPrefixGoldenSha256) {
        std::cerr << "closed prefix actual SHA-256: "
                  << closed_prefix_sha << '\n';
    }
    Expect(
        closed_prefix_sha == kClosedPrefixGoldenSha256,
        "closed frontier golden SHA-256");

    ingress::RawManifestV1 with_open = closed;
    with_open.manifest_generation = 3U;
    with_open.open_entry = MakeEntry(
        2U,
        4224U,
        ingress::kRawV1SegmentHeaderBytes,
        2U,
        0U,
        1U,
        ingress::RawManifestSegmentStateV1::kOpen);
    Expect(
        ingress::ValidateManifestModel(with_open, &closed) ==
            ingress::RawManifestV1Error::kNone,
        "adding an open entry is append-only");
    Expect(
        with_open.closed_prefix_sha256 ==
            closed.closed_prefix_sha256,
        "open entry and generation are outside closed frontier");

    ingress::RawManifestV1 progressed = with_open;
    progressed.manifest_generation = 4U;
    progressed.open_entry = MakeEntry(
        2U,
        4224U,
        4224U,
        2U,
        1U,
        1U,
        ingress::RawManifestSegmentStateV1::kOpen);
    Expect(
        ingress::ValidateManifestModel(progressed, &with_open) ==
            ingress::RawManifestV1Error::kNone,
        "an open segment may advance without changing frontier");
    Expect(
        progressed.closed_prefix_sha256 ==
            with_open.closed_prefix_sha256,
        "progressed open entry is outside closed frontier");

    ingress::RawManifestV1 mutated = closed;
    mutated.manifest_generation = 5U;
    mutated.closed_entries[0U].segment_sha256[0U] ^=
        std::byte{0x01};
    Expect(SetClosedPrefix(&mutated), "mutated model rehashes");
    Expect(
        ingress::ValidateManifestModel(mutated, &closed) ==
            ingress::RawManifestV1Error::kNotAppendOnly,
        "a prior closed entry cannot be corrected");
}

void TestStrictEscapingAndValidationFailures() {
    ingress::RawManifestV1 continuation{};
    continuation.manifest_generation = 1U;
    continuation.namespace_identity = MakeNamespace();
    auto entry = MakeEntry(
        1U,
        0U,
        ingress::kRawV1SegmentHeaderBytes,
        1U,
        0U,
        0U,
        ingress::RawManifestSegmentStateV1::kClosed);
    entry.segment_flags =
        ingress::kRawV1FinalizationContinuation;
    entry.reserve_state_uuid = Pattern<16U>(0x51U);
    entry.finalization_cycle_id = Pattern<16U>(0x71U);
    entry.immutable_grant_sha256 = Pattern<32U>(0x91U);
    entry.maintenance_report_locator =
        "maintenance/finalization-" +
        common::Identity128Hex(
            entry.finalization_cycle_id) +
        ".json";
    entry.archive_locator =
        "reserve-audit/finalization-" +
        common::Identity128Hex(entry.reserve_state_uuid) +
        "-" +
        common::Identity128Hex(
            entry.finalization_cycle_id) +
        "/";
    continuation.closed_entries.push_back(entry);
    continuation.closed_entry_count = 1U;
    if (!SetClosedPrefix(&continuation)) {
        return;
    }
    std::string encoded;
    Expect(
        ingress::EncodeRawManifestJcs(
            continuation, &encoded) ==
            ingress::RawManifestV1Error::kNone,
        "deterministic continuation locators encode");
    const bool exact_locators =
        encoded.find(
            *entry.maintenance_report_locator) !=
            std::string::npos &&
        encoded.find(*entry.archive_locator) !=
            std::string::npos;
    Expect(
        exact_locators,
        "JCS preserves the exact deterministic continuation locators");

    ingress::RawManifestV1 bad_utf8 = continuation;
    bad_utf8.manifest_generation = 2U;
    bad_utf8.closed_entries[0U].archive_locator =
        std::string("\xc0\xaf", 2U);
    Expect(
        ingress::ComputeClosedPrefix(
            bad_utf8, &bad_utf8.closed_prefix_sha256) ==
            ingress::RawManifestV1Error::kInvalidUtf8,
        "non-shortest invalid UTF-8 is rejected");

    ingress::RawManifestV1 invented_locator =
        continuation;
    invented_locator.manifest_generation = 2U;
    invented_locator.closed_entries[0U]
        .maintenance_report_locator =
        "maintenance/finalization-invented.json";
    Expect(
        ingress::ComputeClosedPrefix(
            invented_locator,
            &invented_locator.closed_prefix_sha256) ==
            ingress::RawManifestV1Error::
                kInvalidFinalization,
        "continuation locator cannot be invented independently of its header identities");

    ingress::RawManifestV1 wrong_count = continuation;
    wrong_count.closed_entry_count = 0U;
    Expect(
        ingress::ValidateManifestModel(wrong_count) ==
            ingress::RawManifestV1Error::kClosedEntryCountMismatch,
        "closed entry count mismatch is rejected");

    ingress::RawManifestV1 wrong_prefix = continuation;
    wrong_prefix.closed_prefix_sha256[0U] ^= std::byte{0x01};
    Expect(
        ingress::ValidateManifestModel(wrong_prefix) ==
            ingress::RawManifestV1Error::kClosedPrefixMismatch,
        "closed prefix mismatch is rejected");

    ingress::RawManifestV1 wrong_marker = continuation;
    wrong_marker.closed_entries[0U].accepted_marker_sha256[0U] ^=
        std::byte{0x01};
    Expect(
        ingress::ValidateManifestModel(wrong_marker) ==
            ingress::RawManifestV1Error::kMarkerHashMismatch,
        "accepted marker digest mismatch is rejected");
}

}  // namespace

int main() {
    TestEmptyGolden();
    TestOpenGoldenAndLargeIntegers();
    TestClosedGoldenAndStableFrontier();
    TestStrictEscapingAndValidationFailures();
    if (failures != 0) {
        std::cerr << failures
                  << " Phase 2 RawManifestV1 tests failed\n";
        return 1;
    }
    std::cout << "Phase 2 RawManifestV1 tests passed\n";
    return 0;
}
