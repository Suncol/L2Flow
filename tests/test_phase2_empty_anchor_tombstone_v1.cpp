#include "l2flow/common/sha256.h"
#include "l2flow/ingress/empty_anchor_tombstone_v1.h"
#include "l2flow/ingress/raw_schema.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>

namespace common = l2flow::common;
namespace ingress = l2flow::ingress;

namespace {

struct TestContext final {
    void Expect(
        bool condition,
        const char* description) {
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
         index < result.size();
         ++index) {
        result[index] =
            static_cast<std::byte>(
                static_cast<std::uint8_t>(
                    start +
                    static_cast<std::uint8_t>(index)));
    }
    return result;
}

ingress::EmptyAnchorObservationV1
MakeObservation() {
    ingress::DurableJournalHeaderV1 journal{};
    journal.capture_date = 20260718U;
    journal.source_stream_id = 1001U;
    journal.stream_day_id = Pattern<16U>(1U);
    journal.raw_schema_sha256 =
        ingress::kFrozenRawSchemaSha256;
    journal.created_host_uuid =
        Pattern<16U>(31U);
    journal.created_linux_boot_id =
        Pattern<16U>(61U);
    journal.created_clock_epoch_algorithm = 1U;
    journal.created_clock_epoch_digest =
        Pattern<32U>(91U);
    journal.created_clock_epoch_label = 7U;

    ingress::EmptyAnchorObservationV1 observation{};
    static_cast<void>(
        ingress::EncodeDurableJournalHeaderV1(
            journal,
            &observation.journal_header_bytes));
    observation.journal_logical_size =
        ingress::kRawV1JournalHeaderBytes;
    return observation;
}

void TestRoundTrip(TestContext* test) {
    const ingress::EmptyAnchorObservationV1
        observation = MakeObservation();
    ingress::EmptyAnchorTombstoneV1 tombstone{};
    test->Expect(
        ingress::BuildEmptyAnchorTombstoneV1(
            observation, &tombstone) ==
            ingress::EmptyAnchorTombstoneV1Error::
                kNone,
        "exact header-only observation builds tombstone");
    test->Expect(
        tombstone.journal_header_sha256 ==
            common::ComputeSha256(
                observation.journal_header_bytes),
        "tombstone hashes exact journal header bytes");

    std::unique_ptr<
        ingress::BuiltEmptyAnchorTombstoneV1>
        capability;
    test->Expect(
        ingress::
            BuildEmptyAnchorTombstoneCapabilityV1(
                observation, &capability) ==
                ingress::EmptyAnchorTombstoneV1Error::
                    kNone &&
            capability != nullptr,
        "builder freezes publication input");

    std::string encoded;
    test->Expect(
        ingress::EncodeEmptyAnchorTombstoneV1Jcs(
            tombstone, &encoded) ==
            ingress::EmptyAnchorTombstoneV1Error::
                kNone &&
            encoded.size() <=
                ingress::
                    kEmptyAnchorTombstoneV1MaximumBytes &&
            encoded.find('\n') == std::string::npos,
        "tombstone encodes bounded canonical bytes");
    const std::string golden =
        common::Sha256Hex(
            common::ComputeSha256(encoded));
    if (golden !=
        "430b2c8b13bbc5c58db26be743ccd5f1"
        "9e27fec7e74e854b3a4fad557f34031a") {
        std::cerr << "observed tombstone golden digest: "
                  << golden << '\n';
    }
    test->Expect(
        golden ==
            "430b2c8b13bbc5c58db26be743ccd5f1"
            "9e27fec7e74e854b3a4fad557f34031a",
        "canonical tombstone golden digest is frozen");
    test->Expect(
        capability != nullptr &&
            capability->canonical_jcs() == encoded &&
            capability->tombstone_sha256() ==
                common::ComputeSha256(encoded) &&
            capability->filename() ==
                "empty-anchor-"
                "0102030405060708090a0b0c0d0e0f10"
                ".json",
        "capability binds exact bytes/hash/locator");

    ingress::EmptyAnchorTombstoneV1 parsed{};
    test->Expect(
        ingress::ParseEmptyAnchorTombstoneV1Jcs(
            encoded, &parsed) ==
            ingress::EmptyAnchorTombstoneV1Error::
                kNone &&
            parsed.namespace_identity.stream_day_id ==
                tombstone.namespace_identity.stream_day_id,
        "strict parser round-trips canonical bytes");

    const std::string unknown =
        encoded.substr(0U, encoded.size() - 1U) +
        ",\"x\":0}";
    test->Expect(
        ingress::ParseEmptyAnchorTombstoneV1Jcs(
            unknown, &parsed) ==
            ingress::EmptyAnchorTombstoneV1Error::
                kInvalidCanonicalJson,
        "unknown field is rejected");
    std::string reordered = encoded;
    const std::size_t capture =
        reordered.find("\"capture_date\"");
    const std::size_t source =
        reordered.find("\"source_stream_id\"");
    test->Expect(
        capture == 1U && source != std::string::npos,
        "fixture contains canonical ordered fields");
    if (capture == 1U && source != std::string::npos) {
        reordered.replace(
            capture,
            std::string("\"capture_date\"").size(),
            "\"source_stream_id\"");
        test->Expect(
            ingress::ParseEmptyAnchorTombstoneV1Jcs(
                reordered, &parsed) ==
                ingress::EmptyAnchorTombstoneV1Error::
                    kInvalidCanonicalJson,
            "reordered/mislabeled field is rejected");
    }
    test->Expect(
        ingress::ParseEmptyAnchorTombstoneV1Jcs(
            encoded + "\n", &parsed) ==
            ingress::EmptyAnchorTombstoneV1Error::
                kInvalidCanonicalJson,
        "trailing newline is rejected");
}

void TestInvalidFacts(TestContext* test) {
    ingress::EmptyAnchorObservationV1 observation =
        MakeObservation();
    ingress::EmptyAnchorTombstoneV1 output{};
    std::unique_ptr<
        ingress::BuiltEmptyAnchorTombstoneV1>
        retained;
    test->Expect(
        ingress::
            BuildEmptyAnchorTombstoneCapabilityV1(
                observation, &retained) ==
                ingress::EmptyAnchorTombstoneV1Error::
                    kNone &&
            retained != nullptr,
        "valid capability fixture builds");
    auto* const retained_address = retained.get();

    observation.marker_count = 1U;
    test->Expect(
        ingress::BuildEmptyAnchorTombstoneV1(
            observation, &output) ==
            ingress::EmptyAnchorTombstoneV1Error::
                kNotEmptyAnchor,
        "nonzero marker count cannot be certified empty");
    test->Expect(
        ingress::
            BuildEmptyAnchorTombstoneCapabilityV1(
                observation, &retained) ==
                ingress::EmptyAnchorTombstoneV1Error::
                    kNotEmptyAnchor &&
            retained.get() == retained_address,
        "failed capability build preserves caller ownership");
    observation = MakeObservation();
    observation.segment_count = 1U;
    test->Expect(
        ingress::BuildEmptyAnchorTombstoneV1(
            observation, &output) ==
            ingress::EmptyAnchorTombstoneV1Error::
                kNotEmptyAnchor,
        "nonzero segment count cannot be certified empty");
    observation = MakeObservation();
    observation.record_count = 1U;
    test->Expect(
        ingress::BuildEmptyAnchorTombstoneV1(
            observation, &output) ==
            ingress::EmptyAnchorTombstoneV1Error::
                kNotEmptyAnchor,
        "nonzero record count cannot be certified empty");
    observation = MakeObservation();
    observation.journal_logical_size += 1U;
    test->Expect(
        ingress::BuildEmptyAnchorTombstoneV1(
            observation, &output) ==
            ingress::EmptyAnchorTombstoneV1Error::
                kNotEmptyAnchor,
        "journal suffix cannot be certified empty");
    observation = MakeObservation();
    observation.journal_header_bytes[0] ^=
        std::byte{0xffU};
    test->Expect(
        ingress::BuildEmptyAnchorTombstoneV1(
            observation, &output) ==
            ingress::EmptyAnchorTombstoneV1Error::
                kInvalidJournalHeader,
        "invalid Raw journal header is rejected");
}

void TestFrozenSchema(TestContext* test) {
    const std::filesystem::path path =
        std::filesystem::path(__FILE__)
            .parent_path()
            .parent_path() /
        "schemas/empty_anchor_tombstone_v1.json";
    std::error_code size_error;
    const std::uintmax_t size =
        std::filesystem::file_size(path, size_error);
    test->Expect(
        !size_error &&
            size ==
                ingress::
                    kEmptyAnchorTombstoneV1SchemaBytes,
        "tombstone schema exact byte count is frozen");
    ingress::RawV1Digest digest{};
    std::string error;
    const bool hashed = common::ComputeFileSha256(
        path,
        &digest,
        &error,
        std::optional<std::uint64_t>{
            ingress::
                kEmptyAnchorTombstoneV1SchemaBytes});
    test->Expect(
        hashed &&
            common::Sha256Hex(digest) ==
                ingress::
                    kEmptyAnchorTombstoneV1SchemaSha256Hex,
        "tombstone schema exact SHA-256 is frozen");
}

}  // namespace

int main() {
    static_assert(
        !std::is_default_constructible_v<
            ingress::BuiltEmptyAnchorTombstoneV1>);
    static_assert(
        !std::is_copy_constructible_v<
            ingress::BuiltEmptyAnchorTombstoneV1>);
    static_assert(
        !std::is_move_constructible_v<
            ingress::BuiltEmptyAnchorTombstoneV1>);

    TestContext test;
    TestFrozenSchema(&test);
    TestRoundTrip(&test);
    TestInvalidFacts(&test);
    if (test.failures != 0) {
        std::cerr << test.failures
                  << " empty-anchor tombstone test(s) failed\n";
        return 1;
    }
    return 0;
}
