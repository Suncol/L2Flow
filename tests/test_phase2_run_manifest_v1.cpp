#include "l2flow/common/sha256.h"
#include "l2flow/ingress/run_manifest_v1.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <initializer_list>
#include <iostream>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace common = l2flow::common;
namespace ingress = l2flow::ingress;

namespace {

struct TestContext final {
    void Expect(
        bool condition,
        std::string_view description) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << description << '\n';
        }
    }

    void ExpectError(
        ingress::RunManifestV1Error actual,
        ingress::RunManifestV1Error expected,
        std::string_view description) {
        if (actual != expected) {
            ++failures;
            std::cerr
                << "FAIL: " << description
                << " (actual="
                << ingress::RunManifestV1ErrorName(actual)
                << ", expected="
                << ingress::RunManifestV1ErrorName(expected)
                << ")\n";
        }
    }

    int failures = 0;
};

template <std::size_t Size>
std::array<std::byte, Size> Pattern(
    std::uint8_t seed) {
    std::array<std::byte, Size> result{};
    for (std::size_t index = 0U;
         index < Size;
         ++index) {
        result[index] =
            static_cast<std::byte>(
                static_cast<std::uint8_t>(
                    seed +
                    static_cast<std::uint8_t>(
                        index)));
    }
    return result;
}

std::uint64_t ClockLabel(
    const ingress::RawV1Digest& digest) {
    std::uint64_t result = 0U;
    for (std::size_t index = 0U; index < 8U; ++index) {
        result |= static_cast<std::uint64_t>(
                      std::to_integer<std::uint8_t>(
                          digest[index]))
                  << (8U * index);
    }
    return result;
}

template <std::size_t Size>
std::string Hex(
    const std::array<std::byte, Size>& value) {
    constexpr std::string_view alphabet =
        "0123456789abcdef";
    std::string result;
    result.reserve(Size * 2U);
    for (const std::byte byte : value) {
        const std::uint8_t octet =
            std::to_integer<std::uint8_t>(byte);
        result.push_back(
            alphabet[(octet >> 4U) & 0x0fU]);
        result.push_back(alphabet[octet & 0x0fU]);
    }
    return result;
}

void RefreshMarker(
    ingress::RunManifestRawInputV1* input) {
    if (input == nullptr) {
        return;
    }
    static_cast<void>(
        ingress::EncodeDurableMarkerV1(
            input->durable_marker,
            &input->durable_marker_bytes));
    static_cast<void>(
        ingress::DecodeDurableMarkerV1(
            input->durable_marker_bytes,
            &input->durable_marker));
}

ingress::RunManifestFaultRuleV1 MakeFaultRule(
    std::string canonical_rule) {
    ingress::RunManifestFaultRuleV1 rule{};
    rule.rule_sha256 =
        common::ComputeSha256(
            std::string_view{canonical_rule});
    rule.canonical_rule = std::move(canonical_rule);
    return rule;
}

ingress::RunManifestV1 MakeManifest() {
    ingress::RunManifestV1 manifest{};
    manifest.run_id = Pattern<16U>(0x01U);
    manifest.mode = ingress::RunManifestModeV1::kLive;
    manifest.capture_date = 20260718U;
    manifest.host_uuid = Pattern<16U>(0x11U);
    manifest.linux_boot_id = Pattern<16U>(0x21U);
    manifest.clock_epoch_algorithm = 1U;
    manifest.clock_epoch_digest =
        Pattern<32U>(0x31U);
    manifest.clock_epoch_label =
        ClockLabel(manifest.clock_epoch_digest);

    manifest.vendor.sdk_version = 213234U;
    manifest.vendor.sdk_archive_sha256 =
        Pattern<32U>(0x41U);
    manifest.vendor.libmdl_api_sha256 =
        Pattern<32U>(0x51U);
    manifest.vendor.elf_build_id = "abcd1234";

    manifest.build.build_manifest_sha256 =
        Pattern<32U>(0x61U);
    manifest.build.source_revision_status =
        ingress::
            RunManifestSourceRevisionStatusV1::
                kUnavailable;
    manifest.build.compiler = "gcc-13.2";
    manifest.build.cxx_flags =
        "-O2 -g \"quoted\"";
    manifest.build.dependency_lock_sha256 =
        Pattern<32U>(0x71U);

    manifest.configuration.config_sha256 =
        Pattern<32U>(0x81U);
    manifest.configuration
        .endpoint_contract_sha256 =
        Pattern<32U>(0x91U);
    manifest.configuration.registry_version =
        "raw-readiness-v1";
    manifest.configuration.registry_sha256 =
        Pattern<32U>(0xa1U);
    manifest.configuration.raw_schema_sha256 =
        Pattern<32U>(0xb1U);
    manifest.configuration.shard_count = 0U;

    ingress::RunManifestRawInputV1 input{};
    input.source_stream_id = 1001U;
    input.capture_date = manifest.capture_date;
    input.stream_day_id = Pattern<16U>(0xc1U);
    input.durable_journal_header_sha256 =
        Pattern<32U>(0xd1U);
    input.range =
        ingress::RunManifestRawRangeV1{
            4096U, 4224U, 1U, 1U};
    input.durable_marker.source_stream_id =
        input.source_stream_id;
    input.durable_marker.segment_sequence = 1U;
    input.durable_marker.durable_global_wal_pos =
        4224U;
    input.durable_marker
        .durable_ingress_sequence = 1U;
    input.durable_marker.durable_segment_offset =
        4224U;
    input.durable_marker.marker_flags =
        ingress::kRawV1SegmentSealed;
    RefreshMarker(&input);
    input.segment_sha256.push_back(
        Pattern<32U>(0xe1U));
    input.clock_epoch_transitions.push_back(
        ingress::RunManifestClockTransitionV1{
            4096U,
            manifest.clock_epoch_algorithm,
            manifest.clock_epoch_digest});
    manifest.raw_inputs.push_back(std::move(input));
    return manifest;
}

bool AppearsInOrder(
    std::string_view text,
    std::size_t start,
    std::initializer_list<std::string_view> tokens) {
    std::size_t cursor = start;
    for (const std::string_view token : tokens) {
        const std::size_t found =
            text.find(token, cursor);
        if (found == std::string_view::npos) {
            return false;
        }
        cursor = found + token.size();
    }
    return true;
}

void TestFrozenSchema(TestContext* test) {
    const std::filesystem::path path =
        std::filesystem::path(__FILE__)
            .parent_path()
            .parent_path() /
        "schemas/run_manifest_v1.json";
    std::error_code size_error;
    const std::uintmax_t size =
        std::filesystem::file_size(path, size_error);
    test->Expect(
        !size_error &&
            size == ingress::kRunManifestV1SchemaBytes,
        "RunManifest schema exact byte count is frozen");

    ingress::RawV1Digest digest{};
    std::string error;
    const bool hashed = common::ComputeFileSha256(
        path,
        &digest,
        &error,
        std::optional<std::uint64_t>{
            ingress::kRunManifestV1SchemaBytes});
    test->Expect(
        hashed,
        "RunManifest schema exact regular-file bytes hash");
    if (hashed) {
        test->Expect(
            digest ==
                ingress::kRunManifestV1SchemaSha256,
            "RunManifest schema binary SHA-256 constant matches");
        test->Expect(
            common::Sha256Hex(digest) ==
                ingress::
                    kRunManifestV1SchemaSha256Hex,
            "RunManifest schema hexadecimal SHA-256 constant matches");
    }
}

void TestCanonicalGolden(TestContext* test) {
    const ingress::RunManifestV1 manifest =
        MakeManifest();
    test->ExpectError(
        ingress::ValidateRunManifestV1(manifest),
        ingress::RunManifestV1Error::kNone,
        "typed live Raw run manifest validates");

    std::string canonical;
    test->ExpectError(
        ingress::EncodeRunManifestV1Jcs(
            manifest, &canonical),
        ingress::RunManifestV1Error::kNone,
        "minimal manifest encodes");
    test->Expect(
        !canonical.empty() &&
            canonical.front() == '{' &&
            canonical.back() == '}' &&
            canonical.find('\n') ==
                std::string::npos &&
            canonical.find(
                "\"clock_epoch_label\":\""
                "4050765991979987505\"") !=
                std::string::npos &&
            canonical.find(
                "\"first_record_start_wal_pos\":"
                "\"4096\"") !=
                std::string::npos &&
            canonical.find("\"trade_date\":null") !=
                std::string::npos,
        "JCS uses exact identities, uint64 strings, nulls and no newline");

    const std::string golden_sha256 =
        common::Sha256Hex(
            common::ComputeSha256(
                std::string_view{canonical}));
    constexpr std::string_view kGoldenSha256 =
        "38bfa298ba906ee7a5fc78d473fa3616"
        "b13de9f9327687990787889adf3df97a";
    if (golden_sha256 != kGoldenSha256) {
        std::cerr
            << "observed RunManifestV1 golden SHA-256: "
            << golden_sha256 << '\n';
    }
    test->Expect(
        golden_sha256 == kGoldenSha256,
        "minimal RunManifest canonical bytes retain their golden digest");

    // RFC 8785 orders object property names by UTF-16 code units. All V1
    // property names are ASCII, so byte lexical order is the same order.
    test->Expect(
        AppearsInOrder(
            canonical,
            0U,
            {"\"build\":",
             "\"capture_date\":",
             "\"clock_epoch_algorithm\":",
             "\"clock_epoch_digest\":",
             "\"clock_epoch_label\":",
             "\"configuration\":",
             "\"factor\":",
             "\"fault_injection\":",
             "\"host_uuid\":",
             "\"inputs\":",
             "\"linux_boot_id\":",
             "\"manifest_schema_version\":",
             "\"mode\":",
             "\"run_id\":",
             "\"trade_date\":",
             "\"vendor\":"}),
        "top-level properties follow JCS UTF-16 key order");
    const std::size_t raw_start =
        canonical.find("\"raw_streams\":[{");
    test->Expect(
        raw_start != std::string::npos &&
            AppearsInOrder(
                canonical,
                raw_start,
                {"\"append_only_reason\":",
                 "\"capture_date\":",
                 "\"clock_epoch_transitions\":",
                 "\"durability_policy\":",
                 "\"durable_journal_header_sha256\":",
                 "\"durable_marker\":",
                 "\"fault_rule_hash\":",
                 "\"fault_seed\":",
                 "\"first_ingress_sequence\":",
                 "\"first_record_start_wal_pos\":",
                 "\"last_ingress_sequence\":",
                 "\"last_record_end_wal_pos\":",
                 "\"parent_raw_identity_hash\":",
                 "\"parent_run_id\":",
                 "\"reserve_finalization\":",
                 "\"segment_sha256\":",
                 "\"source_stream_id\":",
                 "\"stream_day_id\":",
                 "\"synthetic\":",
                 "\"synthetic_schema\":"}),
        "Raw input properties follow JCS UTF-16 key order");

    std::unique_ptr<ingress::BuiltRunManifestV1> built;
    test->ExpectError(
        ingress::BuildRunManifestV1(
            manifest, &built),
        ingress::RunManifestV1Error::kNone,
        "built manifest capability is created");
    test->Expect(
        built != nullptr &&
            built->canonical_jcs() == canonical &&
            built->sha256() ==
                common::ComputeSha256(
                    std::string_view{canonical}),
        "built capability freezes exact canonical bytes and digest");

    ingress::RunManifestV1 escaped = manifest;
    escaped.build.cxx_flags =
        std::string{
            "\x01\n\"\\ UTF-8:\xe4\xb8\xad"
            "\xf0\x9f\x98\x80"};
    std::string escaped_jcs;
    test->ExpectError(
        ingress::EncodeRunManifestV1Jcs(
            escaped, &escaped_jcs),
        ingress::RunManifestV1Error::kNone,
        "valid Unicode and control characters encode");
    test->Expect(
        escaped_jcs.find(
            "\\u0001\\n\\\"\\\\ UTF-8:"
            "\xe4\xb8\xad\xf0\x9f\x98\x80") !=
            std::string::npos,
        "JCS uses shortest escapes and preserves valid UTF-8 bytes");
}

void TestPreciseErrorClassification(
    TestContext* test) {
    const ingress::RunManifestV1 baseline =
        MakeManifest();
    ingress::RunManifestV1 invalid = baseline;

    invalid.raw_inputs.front().durability_policy =
        static_cast<
            ingress::RunManifestDurabilityPolicyV1>(
            std::numeric_limits<std::uint8_t>::max());
    test->ExpectError(
        ingress::ValidateRunManifestV1(invalid),
        ingress::RunManifestV1Error::
            kInvalidDurabilityPolicy,
        "unknown durability policy has its own classification");

    invalid = baseline;
    invalid.raw_inputs.front()
        .durable_marker_bytes[0U] ^=
        std::byte{1U};
    test->ExpectError(
        ingress::ValidateRunManifestV1(invalid),
        ingress::RunManifestV1Error::kInvalidMarker,
        "invalid marker wire has marker classification");

    invalid = baseline;
    ++invalid.raw_inputs.front()
          .durable_marker.durable_global_wal_pos;
    test->ExpectError(
        ingress::ValidateRunManifestV1(invalid),
        ingress::RunManifestV1Error::kMarkerMismatch,
        "typed/wire marker disagreement is distinguished");

    invalid = baseline;
    invalid.raw_inputs.front().segment_sha256.clear();
    test->ExpectError(
        ingress::ValidateRunManifestV1(invalid),
        ingress::RunManifestV1Error::
            kInvalidSegmentSet,
        "empty segment digest set has segment classification");

    invalid = baseline;
    ++invalid.raw_inputs.front()
          .clock_epoch_transitions.front()
          .record_start_wal_pos;
    test->ExpectError(
        ingress::ValidateRunManifestV1(invalid),
        ingress::RunManifestV1Error::
            kInvalidClockTransitions,
        "uncovered range start has clock-transition classification");

    invalid = baseline;
    invalid.raw_inputs.front()
        .reserve_finalization.reserve_state_uuid =
        Pattern<16U>(0x33U);
    test->ExpectError(
        ingress::ValidateRunManifestV1(invalid),
        ingress::RunManifestV1Error::
            kInvalidReserveFinalization,
        "partial reserve evidence has reserve classification");

    invalid = baseline;
    invalid.clock_epoch_label ^= 1U;
    test->ExpectError(
        ingress::ValidateRunManifestV1(invalid),
        ingress::RunManifestV1Error::kInvalidIdentity,
        "clock label cannot disagree with its full digest");

    invalid = baseline;
    invalid.raw_inputs.front()
        .clock_epoch_transitions.front().digest[0U] ^=
        std::byte{1U};
    test->ExpectError(
        ingress::ValidateRunManifestV1(invalid),
        ingress::RunManifestV1Error::
            kInvalidClockTransitions,
        "live input clock identity must match the run identity");

    std::string unchanged = "sentinel";
    invalid = baseline;
    invalid.build.source_revision_status =
        ingress::
            RunManifestSourceRevisionStatusV1::
                kAvailable;
    test->ExpectError(
        ingress::EncodeRunManifestV1Jcs(
            invalid, &unchanged),
        ingress::RunManifestV1Error::
            kInvalidSourceRevision,
        "source revision status is classified precisely");
    test->Expect(
        unchanged == "sentinel",
        "validation failure leaves encoded output unchanged");
}

void TestRecoveredAppendOnly(TestContext* test) {
    ingress::RunManifestV1 manifest = MakeManifest();
    manifest.mode = ingress::RunManifestModeV1::kReplay;
    auto& input = manifest.raw_inputs.front();
    input.durability_policy =
        ingress::RunManifestDurabilityPolicyV1::
            kIncludesRecoveredAppendOnly;
    input.append_only_reason =
        "recovery-promoted complete suffix";

    test->ExpectError(
        ingress::ValidateRunManifestV1(manifest),
        ingress::RunManifestV1Error::
            kInvalidDurabilityPolicy,
        "reason alone cannot relabel a wholly durable range");

    input.range->last_record_end_wal_pos = 4352U;
    input.range->last_ingress_sequence = 2U;
    test->ExpectError(
        ingress::ValidateRunManifestV1(manifest),
        ingress::RunManifestV1Error::kNone,
        "replay explicitly records a recovered append-only suffix");

    ingress::RunManifestV1 invalid = manifest;
    invalid.mode = ingress::RunManifestModeV1::kLive;
    test->ExpectError(
        ingress::ValidateRunManifestV1(invalid),
        ingress::RunManifestV1Error::kNone,
        "live crash recovery may classify an explicit recovered suffix");

    invalid = manifest;
    invalid.raw_inputs.front().range.reset();
    invalid.raw_inputs.front()
        .clock_epoch_transitions.clear();
    test->ExpectError(
        ingress::ValidateRunManifestV1(invalid),
        ingress::RunManifestV1Error::
            kInvalidDurabilityPolicy,
        "append-only provenance requires an exact selected range");

    invalid = MakeManifest();
    invalid.raw_inputs.front().range.reset();
    test->ExpectError(
        ingress::ValidateRunManifestV1(invalid),
        ingress::RunManifestV1Error::
            kInvalidClockTransitions,
        "an empty selected range cannot retain transition records");
    invalid.raw_inputs.front()
        .clock_epoch_transitions.clear();
    test->ExpectError(
        ingress::ValidateRunManifestV1(invalid),
        ingress::RunManifestV1Error::kNone,
        "empty range and empty transition list are consistent");
}

void TestSyntheticProvenance(TestContext* test) {
    ingress::RunManifestV1 manifest = MakeManifest();
    manifest.mode = ingress::RunManifestModeV1::kReplay;
    auto& input = manifest.raw_inputs.front();
    input.synthetic = true;
    input.synthetic_schema = "InjectedRawV1";
    input.parent_run_id = Pattern<16U>(0x44U);
    input.parent_raw_identity_sha256 =
        Pattern<32U>(0x55U);
    input.fault_seed = 7U;
    manifest.fault_injection.enabled = true;
    manifest.fault_injection.seed = 7U;
    manifest.fault_injection.rules.push_back(
        MakeFaultRule(
            "{\"kind\":\"drop\",\"n\":\"1\"}"));
    input.fault_rule_sha256 =
        manifest.fault_injection.rules.front()
            .rule_sha256;

    test->ExpectError(
        ingress::ValidateRunManifestV1(manifest),
        ingress::RunManifestV1Error::kNone,
        "synthetic replay binds parent, exact rule and exact seed");

    ingress::RunManifestV1 invalid = manifest;
    invalid.mode = ingress::RunManifestModeV1::kLive;
    test->ExpectError(
        ingress::ValidateRunManifestV1(invalid),
        ingress::RunManifestV1Error::
            kInvalidSyntheticProvenance,
        "synthetic input cannot be represented as live Raw");

    invalid = manifest;
    invalid.raw_inputs.front().parent_run_id =
        invalid.run_id;
    test->ExpectError(
        ingress::ValidateRunManifestV1(invalid),
        ingress::RunManifestV1Error::
            kInvalidSyntheticProvenance,
        "synthetic parent run cannot be the run itself");

    invalid = manifest;
    invalid.raw_inputs.front().fault_seed = 8U;
    test->ExpectError(
        ingress::ValidateRunManifestV1(invalid),
        ingress::RunManifestV1Error::
            kInvalidSyntheticProvenance,
        "per-input fault seed must equal the run-level seed");

    invalid = manifest;
    invalid.fault_injection.rules.front()
        .canonical_rule.push_back(' ');
    test->ExpectError(
        ingress::ValidateRunManifestV1(invalid),
        ingress::RunManifestV1Error::
            kInvalidFaultInjection,
        "fault rule hash covers the exact canonical rule bytes");

    invalid = manifest;
    invalid.fault_injection.rules.push_back(
        invalid.fault_injection.rules.front());
    test->ExpectError(
        ingress::ValidateRunManifestV1(invalid),
        ingress::RunManifestV1Error::
            kInvalidFaultInjection,
        "duplicate fault rule hashes are rejected");

    invalid = manifest;
    invalid.raw_inputs.front().synthetic = false;
    test->ExpectError(
        ingress::ValidateRunManifestV1(invalid),
        ingress::RunManifestV1Error::
            kInvalidSyntheticProvenance,
        "non-synthetic input cannot retain synthetic provenance");
}

void TestReserveAndStageInvariants(
    TestContext* test) {
    ingress::RunManifestV1 manifest = MakeManifest();
    auto& reserve = manifest.raw_inputs.front()
                        .reserve_finalization;
    reserve.reserve_state_uuid =
        Pattern<16U>(0x33U);
    reserve.finalization_cycle_id =
        Pattern<16U>(0x43U);
    reserve.immutable_grant_sha256 =
        Pattern<32U>(0x53U);
    reserve.maintenance_report_locator =
        "maintenance/finalization-" +
        Hex(reserve.finalization_cycle_id) +
        ".json";
    reserve.maintenance_report_sha256 =
        Pattern<32U>(0x63U);
    reserve.continuation_segment_sequences = {1U};
    test->ExpectError(
        ingress::ValidateRunManifestV1(manifest),
        ingress::RunManifestV1Error::kNone,
        "complete reserve finalization binds its deterministic report");

    ingress::RunManifestV1 invalid = manifest;
    invalid.raw_inputs.front()
        .reserve_finalization
        .maintenance_report_locator =
        "maintenance/finalization-wrong.json";
    test->ExpectError(
        ingress::ValidateRunManifestV1(invalid),
        ingress::RunManifestV1Error::
            kInvalidReserveFinalization,
        "reserve report locator is cycle-derived");

    invalid = manifest;
    invalid.raw_inputs.front()
        .reserve_finalization
        .continuation_segment_sequences = {1U, 2U};
    test->ExpectError(
        ingress::ValidateRunManifestV1(invalid),
        ingress::RunManifestV1Error::
            kInvalidReserveFinalization,
        "one cycle cannot claim two continuation segments");

    invalid = manifest;
    invalid.raw_inputs.front()
        .reserve_finalization
        .continuation_segment_sequences = {2U};
    test->ExpectError(
        ingress::ValidateRunManifestV1(invalid),
        ingress::RunManifestV1Error::
            kInvalidReserveFinalization,
        "continuation identity must be the terminal marker segment");

    invalid = MakeManifest();
    invalid.configuration.canonical_schema_sha256 =
        Pattern<32U>(0x73U);
    test->ExpectError(
        ingress::ValidateRunManifestV1(invalid),
        ingress::RunManifestV1Error::
            kInvalidConfiguration,
        "decoded configuration hashes are an all-or-none pair");
    invalid.configuration.dtype_sha256 =
        Pattern<32U>(0x83U);
    invalid.configuration.shard_count = 16U;
    test->ExpectError(
        ingress::ValidateRunManifestV1(invalid),
        ingress::RunManifestV1Error::kNone,
        "decoded configuration has both hashes and nonzero shards");

    invalid.factor = ingress::RunManifestFactorV1{
        "microstructure",
        Pattern<32U>(0x93U),
        Pattern<32U>(0xa3U),
        Pattern<32U>(0xb3U)};
    test->ExpectError(
        ingress::ValidateRunManifestV1(invalid),
        ingress::RunManifestV1Error::kInvalidFactor,
        "factor stage cannot omit trade date");
    invalid.trade_date = 20260718U;
    test->ExpectError(
        ingress::ValidateRunManifestV1(invalid),
        ingress::RunManifestV1Error::kNone,
        "factor stage carries decoded schema, dtype, shards and date");
}

void TestBoundsAndBuildFailureAtomicity(
    TestContext* test) {
    ingress::RunManifestV1 manifest = MakeManifest();
    std::unique_ptr<ingress::BuiltRunManifestV1> built;
    test->ExpectError(
        ingress::BuildRunManifestV1(
            manifest, &built),
        ingress::RunManifestV1Error::kNone,
        "baseline capability builds");
    ingress::BuiltRunManifestV1* const original =
        built.get();
    manifest.schema_version = 2U;
    test->ExpectError(
        ingress::BuildRunManifestV1(
            manifest, &built),
        ingress::RunManifestV1Error::
            kUnsupportedSchema,
        "invalid build input is rejected");
    test->Expect(
        built.get() == original,
        "failed build leaves caller capability unchanged");

    manifest = MakeManifest();
    manifest.raw_inputs.front().segment_sha256.assign(
        65'000U,
        Pattern<32U>(0xd3U));
    std::string output = "sentinel";
    test->ExpectError(
        ingress::EncodeRunManifestV1Jcs(
            manifest, &output),
        ingress::RunManifestV1Error::
            kEncodedSizeExceeded,
        "encoder stops at the four-MiB exact-byte limit");
    test->Expect(
        output == "sentinel",
        "size failure leaves caller bytes unchanged");
}

}  // namespace

int main() {
    TestContext test;
    TestFrozenSchema(&test);
    TestCanonicalGolden(&test);
    TestPreciseErrorClassification(&test);
    TestRecoveredAppendOnly(&test);
    TestSyntheticProvenance(&test);
    TestReserveAndStageInvariants(&test);
    TestBoundsAndBuildFailureAtomicity(&test);

    if (test.failures != 0) {
        std::cerr << test.failures
                  << " run-manifest checks failed\n";
        return 1;
    }
    std::cout
        << "Phase 2 RunManifestV1 checks passed\n";
    return 0;
}
