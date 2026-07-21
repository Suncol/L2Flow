#include "l2flow/common/sha256.h"
#include "l2flow/ingress/raw_schema.h"
#include "l2flow/ingress/scaffolding_finalization_report_v1.h"

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
        ingress::BuiltScaffoldingFinalizationReportV1>);
static_assert(
    !std::is_copy_constructible_v<
        ingress::BuiltScaffoldingFinalizationReportV1>);
static_assert(
    !std::is_move_constructible_v<
        ingress::BuiltScaffoldingFinalizationReportV1>);

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

ingress::EmptyAnchorObservationV1 MakeObservation() {
    ingress::DurableJournalHeaderV1 journal{};
    journal.capture_date = 20260718U;
    journal.source_stream_id = 1001U;
    journal.stream_day_id = Pattern<16U>(0x01U);
    journal.raw_schema_sha256 =
        ingress::kFrozenRawSchemaSha256;
    journal.created_host_uuid = Pattern<16U>(0x21U);
    journal.created_linux_boot_id =
        Pattern<16U>(0x31U);
    journal.created_clock_epoch_algorithm = 1U;
    journal.created_clock_epoch_digest =
        Pattern<32U>(0x41U);
    journal.created_clock_epoch_label = 77U;
    ingress::EmptyAnchorObservationV1 observation{};
    static_cast<void>(
        ingress::EncodeDurableJournalHeaderV1(
            journal,
            &observation.journal_header_bytes));
    observation.journal_logical_size =
        ingress::kRawV1JournalHeaderBytes;
    return observation;
}

ingress::ScaffoldingObjectPostStateV1 PostState(
    ingress::ScaffoldingObjectRoleV1 role) {
    switch (role) {
    case ingress::ScaffoldingObjectRoleV1::
        kCaptureDirectory:
    case ingress::ScaffoldingObjectRoleV1::
        kStreamDirectory:
    case ingress::ScaffoldingObjectRoleV1::
        kMaintenanceDirectory:
        return ingress::ScaffoldingObjectPostStateV1::
            kValidDirectory;
    case ingress::ScaffoldingObjectRoleV1::
        kWriterLeaseTemporary:
    case ingress::ScaffoldingObjectRoleV1::
        kJournalTemporary:
        return ingress::ScaffoldingObjectPostStateV1::
            kAbsent;
    case ingress::ScaffoldingObjectRoleV1::
        kWriterLeaseFinal:
        return ingress::ScaffoldingObjectPostStateV1::
            kValidFile;
    case ingress::ScaffoldingObjectRoleV1::
        kJournalFinal:
        return ingress::ScaffoldingObjectPostStateV1::
            kHeaderOnlyJournal;
    }
    return ingress::ScaffoldingObjectPostStateV1::kAbsent;
}

struct Fixture final {
    ingress::ScaffoldingFinalizationReportV1 report{};
    std::unique_ptr<
        ingress::BuiltEmptyAnchorTombstoneV1>
        tombstone;
};

Fixture MakeFixture(TestContext* test) {
    Fixture fixture{};
    const auto observation = MakeObservation();
    test->Expect(
        ingress::BuildEmptyAnchorTombstoneCapabilityV1(
            observation, &fixture.tombstone) ==
                ingress::EmptyAnchorTombstoneV1Error::kNone &&
            fixture.tombstone != nullptr,
        "tombstone capability fixture builds");
    if (fixture.tombstone == nullptr) {
        return fixture;
    }
    const auto& tombstone = fixture.tombstone->model();
    auto& report = fixture.report;
    report.reserve_state_uuid = Pattern<16U>(0x10U);
    report.finalization_cycle_id =
        Pattern<16U>(0x30U);
    report.planned_namespace = {
        tombstone.namespace_identity.capture_date,
        tombstone.namespace_identity.source_stream_id,
        tombstone.namespace_identity.stream_day_id};
    report.planned_recovery_attempt_id =
        Pattern<16U>(0x50U);
    report.immutable_grant_sha256 =
        Pattern<32U>(0x70U);
    report.object_snapshot_sha256 =
        Pattern<32U>(0x90U);
    report.required_action_bitmap = 0x7fU;

    for (std::uint8_t wire = 1U; wire <= 7U; ++wire) {
        const auto role =
            static_cast<ingress::ScaffoldingObjectRoleV1>(
                wire);
        ingress::ScaffoldingObjectStateV1 object{};
        object.role = role;
        object.start_state =
            wire % 3U == 1U
                ? ingress::ScaffoldingObjectStartStateV1::
                      kAbsent
                : (wire % 3U == 2U
                       ? ingress::
                             ScaffoldingObjectStartStateV1::
                                 kValidFinalOrCompleteTemporary
                       : ingress::
                             ScaffoldingObjectStartStateV1::
                                 kRecognizedPartialTemporary);
        object.post_state = PostState(role);
        object.barriers.object_synced =
            object.post_state !=
            ingress::ScaffoldingObjectPostStateV1::kAbsent;
        object.barriers.parent_directory_synced = true;
        object.barriers.retained_fd_revalidated = true;
        report.objects.push_back(object);
        const std::uint64_t start_wire =
            object.start_state ==
                    ingress::
                        ScaffoldingObjectStartStateV1::
                            kAbsent
                ? 0U
                : (object.start_state ==
                           ingress::
                               ScaffoldingObjectStartStateV1::
                                   kValidFinalOrCompleteTemporary
                       ? 1U
                       : 2U);
        report.observed_object_bitmap |=
            start_wire
            << static_cast<unsigned>((wire - 1U) * 4U);

        ingress::ScaffoldingActionResultV1 action{};
        action.action_id =
            static_cast<std::uint8_t>(wire - 1U);
        action.action_kind =
            ingress::ScaffoldingActionKindV1::
                kRevalidatePostState;
        action.object_role = role;
        action.post_state = object.post_state;
        action.completed = true;
        report.actions.push_back(action);
    }
    report.journal_header_sha256 =
        tombstone.journal_header_sha256;
    report.index_absent = true;
    report.manifest_absent = true;
    report.control_absent = true;
    report.empty_anchor_tombstone_sha256 =
        fixture.tombstone->tombstone_sha256();
    return fixture;
}

void TestReport(TestContext* test) {
    Fixture fixture = MakeFixture(test);
    if (fixture.tombstone == nullptr) {
        return;
    }
    std::unique_ptr<
        ingress::BuiltScaffoldingFinalizationReportV1>
        built;
    test->Expect(
        ingress::
            BuildScaffoldingFinalizationReportCapabilityV1(
                fixture.report,
                ingress::kReserveGrantScaffoldingOnly,
                *fixture.tombstone,
                &built) ==
                ingress::
                    ScaffoldingFinalizationReportV1Error::
                        kNone &&
            built != nullptr,
        "SCAFFOLDING_ONLY builds a private validated capability");
    if (built == nullptr) {
        return;
    }
    test->Expect(
        built->filename() ==
            "scaffolding-"
            "303132333435363738393a3b3c3d3e3f-"
            "707172737475767778797a7b7c7d7e7f"
            "808182838485868788898a8b8c8d8e8f"
            ".json",
        "scaffolding locator derives from cycle and immutable grant hash");
    test->Expect(
        built->canonical_jcs().size() <=
                ingress::
                    kScaffoldingFinalizationReportV1MaximumBytes &&
            built->canonical_jcs().find('\n') ==
                std::string_view::npos &&
            built->canonical_jcs().find(
                "\"sealed_raw_certificate_sha256\":null") !=
                std::string_view::npos &&
            built->canonical_jcs().find(
                "\"segment_count\":\"0\"") !=
                std::string_view::npos,
        "scaffolding JCS is bounded and cannot claim Raw seal/segments");
    const std::string golden = common::Sha256Hex(
        common::ComputeSha256(built->canonical_jcs()));
    if (golden !=
        "e1190a3e04511de07556687a982f3354"
        "0f74c442f832dd5636ff52c9ba61fc8f") {
        std::cerr
            << "observed scaffolding report golden digest: "
            << golden << '\n';
    }
    test->Expect(
        golden ==
            "e1190a3e04511de07556687a982f3354"
            "0f74c442f832dd5636ff52c9ba61fc8f",
        "scaffolding complete-byte JCS golden SHA-256");
    test->Expect(
        built->report_sha256() ==
            common::ComputeSha256(built->canonical_jcs()),
        "capability freezes exact report content hash");

    ingress::ScaffoldingFinalizationReportV1 parsed{};
    test->Expect(
        ingress::ParseScaffoldingFinalizationReportV1Jcs(
            built->canonical_jcs(), &parsed) ==
                ingress::
                    ScaffoldingFinalizationReportV1Error::
                        kNone &&
            parsed.objects.size() == 7U &&
            parsed.actions.size() == 7U &&
            parsed.actions.back().action_id == 6U,
        "strict parser round-trips bounded vectors");

    const auto sentinel = parsed.reserve_state_uuid;
    const std::string unknown =
        std::string(built->canonical_jcs().substr(
            0U, built->canonical_jcs().size() - 1U)) +
        ",\"x\":0}";
    test->Expect(
        ingress::ParseScaffoldingFinalizationReportV1Jcs(
            unknown, &parsed) ==
                ingress::
                    ScaffoldingFinalizationReportV1Error::
                        kInvalidCanonicalJson &&
            parsed.reserve_state_uuid == sentinel,
        "unknown field is rejected without modifying output");
    std::string leading_zero(built->canonical_jcs());
    const std::string canonical_bitmap =
        "\"observed_object_bitmap\":\"2163216\"";
    const std::size_t bitmap =
        leading_zero.find(canonical_bitmap);
    test->Expect(
        bitmap != std::string::npos,
        "golden contains canonical observed bitmap");
    if (bitmap != std::string::npos) {
        leading_zero.replace(
            bitmap,
            canonical_bitmap.size(),
            "\"observed_object_bitmap\":\"02163216\"");
        test->Expect(
            ingress::
                ParseScaffoldingFinalizationReportV1Jcs(
                    leading_zero, &parsed) ==
                    ingress::
                        ScaffoldingFinalizationReportV1Error::
                            kInvalidCanonicalJson &&
                parsed.reserve_state_uuid == sentinel,
            "leading-zero uint64 is rejected without modifying output");
    }

    ingress::ScaffoldingFinalizationReportV1 invalid =
        fixture.report;
    invalid.objects[2U].barriers.object_synced = true;
    std::string output = "sentinel";
    test->Expect(
        ingress::EncodeScaffoldingFinalizationReportV1Jcs(
            invalid, &output) ==
                ingress::
                    ScaffoldingFinalizationReportV1Error::
                        kInvalidPostState &&
            output == "sentinel",
        "invalid tmp post-state barrier leaves output unchanged");

    invalid = fixture.report;
    invalid.required_action_bitmap = 0xffU;
    test->Expect(
        ingress::
            ValidateScaffoldingFinalizationReportV1(
                invalid) ==
            ingress::
                ScaffoldingFinalizationReportV1Error::
                    kInvalidActionVector,
        "action vector must equal the frozen required bitmap");
    invalid = fixture.report;
    while (invalid.actions.size() <=
           ingress::
               kScaffoldingFinalizationReportV1MaximumActions) {
        invalid.actions.push_back(invalid.actions.back());
    }
    test->Expect(
        ingress::
            ValidateScaffoldingFinalizationReportV1(
                invalid) ==
            ingress::
                ScaffoldingFinalizationReportV1Error::
                    kInvalidActionVector,
        "action vector cannot exceed the frozen capacity of 16");

    const auto* const original_built = built.get();
    test->Expect(
        ingress::
            BuildScaffoldingFinalizationReportCapabilityV1(
                fixture.report,
                ingress::kReserveGrantRawAnchorOnly,
                *fixture.tombstone,
                &built) ==
                ingress::
                    ScaffoldingFinalizationReportV1Error::
                        kGrantKindMismatch &&
            built.get() == original_built,
        "grant mismatch cannot replace an existing built capability");
}

void TestSchema(TestContext* test) {
    const std::string path =
        std::string(L2FLOW_SOURCE_DIR) +
        "/schemas/scaffolding_finalization_report_v1.json";
    std::ifstream input(path, std::ios::binary);
    const std::string bytes{
        std::istreambuf_iterator<char>(input),
        std::istreambuf_iterator<char>()};
    test->Expect(
        input.good() || input.eof(),
        "scaffolding report schema is readable");
    const std::string digest =
        common::Sha256Hex(common::ComputeSha256(bytes));
    test->Expect(
        bytes.size() ==
            ingress::
                kScaffoldingFinalizationReportV1SchemaBytes,
        "schema byte count is frozen");
    test->Expect(
        digest ==
            ingress::
                kScaffoldingFinalizationReportV1SchemaSha256Hex,
        "schema SHA-256 is frozen");
}

}  // namespace

int main() {
    TestContext test;
    TestReport(&test);
    TestSchema(&test);
    if (test.failures != 0) {
        std::cerr << test.failures
                  << " scaffolding report assertion(s) failed\n";
        return 1;
    }
    std::cout << "scaffolding finalization report tests passed\n";
    return 0;
}
