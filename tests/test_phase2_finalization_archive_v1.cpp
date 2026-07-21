#include "l2flow/common/sha256.h"
#include "l2flow/ingress/empty_anchor_tombstone_v1.h"
#include "l2flow/ingress/finalization_archive_posix_store.h"
#include "l2flow/ingress/finalization_archive_v1.h"
#include "l2flow/ingress/finalization_report_v1.h"
#include "l2flow/ingress/raw_schema.h"
#include "l2flow/ingress/reserve_emergency_transition_v1.h"

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace common = l2flow::common;
namespace ingress = l2flow::ingress;

namespace {

static_assert(
    !std::is_default_constructible_v<
        ingress::BuiltFinalizationArchiveV1>);
static_assert(
    !std::is_copy_constructible_v<
        ingress::BuiltFinalizationArchiveV1>);
static_assert(
    !std::is_move_constructible_v<
        ingress::BuiltFinalizationArchiveV1>);
static_assert(
    !std::is_default_constructible_v<
        ingress::PublishedFinalizationArchiveReceiptV1>);

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

template <std::size_t Size>
std::array<std::byte, Size> Pattern(
    std::uint8_t start) {
    std::array<std::byte, Size> value{};
    for (std::size_t index = 0U;
         index < Size;
         ++index) {
        value[index] = static_cast<std::byte>(
            static_cast<std::uint8_t>(
                start +
                static_cast<std::uint8_t>(index)));
    }
    return value;
}

class ScopedFd final {
public:
    explicit ScopedFd(int value = -1) noexcept
        : value_(value) {}
    ~ScopedFd() {
        if (value_ >= 0) {
            static_cast<void>(::close(value_));
        }
    }
    ScopedFd(const ScopedFd&) = delete;
    ScopedFd& operator=(const ScopedFd&) = delete;
    ScopedFd(ScopedFd&& other) noexcept
        : value_(std::exchange(other.value_, -1)) {}
    ScopedFd& operator=(ScopedFd&& other) noexcept {
        if (this != &other) {
            if (value_ >= 0) {
                static_cast<void>(::close(value_));
            }
            value_ = std::exchange(other.value_, -1);
        }
        return *this;
    }
    [[nodiscard]] int get() const noexcept {
        return value_;
    }

private:
    int value_ = -1;
};

class TempDirectory final {
public:
    TempDirectory() {
        std::array<char, 64U> pattern{};
        const std::string prefix =
            "/tmp/l2flow-finalization-archive-XXXXXX";
        std::copy(
            prefix.begin(),
            prefix.end(),
            pattern.begin());
        char* result = ::mkdtemp(pattern.data());
        if (result != nullptr) {
            path_ = result;
        }
    }
    ~TempDirectory() {
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
    }
    TempDirectory(const TempDirectory&) = delete;
    TempDirectory& operator=(const TempDirectory&) = delete;

    [[nodiscard]] const std::filesystem::path&
    path() const noexcept {
        return path_;
    }

private:
    std::filesystem::path path_;
};

[[nodiscard]] bool Fsync(int fd) noexcept {
    for (;;) {
        if (::fsync(fd) == 0) {
            return true;
        }
        if (errno != EINTR) {
            return false;
        }
    }
}

ingress::ReserveCoordinatorHeaderV1 MakeHeader() {
    ingress::ReserveCoordinatorHeaderV1 header{};
    header.reserve_state_uuid = Pattern<16U>(0x10U);
    header.schema_sha256 =
        ingress::kReserveStateV1SchemaSha256;
    header.quota_identity_sha256 =
        Pattern<32U>(0x20U);
    header.mount_identity_sha256 =
        Pattern<32U>(0x40U);
    header.device_id = 0x0102030405060708ULL;
    header.declared_releasable_bytes =
        1ULL << 30U;
    header.allocation_quantum_bytes = 4096U;
    header.declared_inode_reserve_count = 1000U;
    header.byte_probe_version = 1U;
    header.inode_probe_version = 1U;
    header.inode_inventory_sha256 =
        Pattern<32U>(0x60U);
    header.safe_stop_catalog_sha256 =
        Pattern<32U>(0x80U);
    return header;
}

struct EmptyEvidenceFixture final {
    std::unique_ptr<
        ingress::BuiltEmptyAnchorTombstoneV1>
        tombstone;
    ingress::RawV1Digest journal_header_sha256{};
    ingress::RawManifestNamespaceV1 namespace_identity{};
};

EmptyEvidenceFixture MakeEmptyEvidence(
    TestContext* test) {
    ingress::DurableJournalHeaderV1 journal{};
    journal.capture_date = 20260719U;
    journal.source_stream_id = 1001U;
    journal.stream_day_id = Pattern<16U>(0x91U);
    journal.raw_schema_sha256 =
        ingress::kFrozenRawSchemaSha256;
    journal.created_host_uuid =
        Pattern<16U>(0x21U);
    journal.created_linux_boot_id =
        Pattern<16U>(0x31U);
    journal.created_clock_epoch_algorithm = 1U;
    journal.created_clock_epoch_digest =
        Pattern<32U>(0x41U);
    journal.created_clock_epoch_label = 77U;

    ingress::EmptyAnchorObservationV1 observation{};
    test->Expect(
        ingress::EncodeDurableJournalHeaderV1(
            journal,
            &observation.journal_header_bytes) ==
            ingress::RawV1Error::kNone,
        "encode empty-anchor journal fixture");
    observation.journal_logical_size =
        ingress::kRawV1JournalHeaderBytes;

    EmptyEvidenceFixture fixture{};
    test->Expect(
        ingress::BuildEmptyAnchorTombstoneCapabilityV1(
            observation, &fixture.tombstone) ==
                ingress::EmptyAnchorTombstoneV1Error::
                    kNone &&
            fixture.tombstone != nullptr,
        "build empty-anchor tombstone fixture");
    if (fixture.tombstone != nullptr) {
        const auto& model =
            fixture.tombstone->model();
        fixture.journal_header_sha256 =
            model.journal_header_sha256;
        fixture.namespace_identity = {
            model.namespace_identity.capture_date,
            model.namespace_identity.source_stream_id,
            model.namespace_identity.stream_day_id};
    }
    return fixture;
}

void SetAction(
    ingress::ReserveStateEntryV1* entry,
    std::size_t action_id,
    ingress::FinalizationActionKindV1 kind,
    std::uint32_t byte_cap_quanta,
    std::uint32_t inode_cap,
    std::uint8_t seed) {
    auto& receipt = entry->actions[action_id];
    receipt.action_id =
        static_cast<std::uint16_t>(action_id);
    receipt.action_kind = kind;
    receipt.action_state =
        ingress::FinalizationActionStateV1::kPending;
    receipt.byte_cap_quanta = byte_cap_quanta;
    receipt.inode_cap = inode_cap;
    receipt.object_plan_sha256 =
        Pattern<32U>(seed);

    auto& plan = entry->plans[action_id];
    plan.plan_version = 1U;
    plan.object_type = kind;
    plan.object_sequence =
        static_cast<std::uint32_t>(action_id + 1U);
    plan.range_start =
        static_cast<std::uint64_t>(
            action_id * 4096U);
    plan.range_end_or_size =
        static_cast<std::uint64_t>(
            (action_id + 1U) * 4096U);
    plan.causal_id =
        Pattern<16U>(
            static_cast<std::uint8_t>(seed + 1U));
}

ingress::ReserveFinalizationGrantKeyV1 GrantKey(
    const ingress::ReserveStateSlotV1& slot) {
    const auto& entry = slot.entries[0U];
    return ingress::ReserveFinalizationGrantKeyV1{
        .finalization_cycle_id =
            slot.finalization_cycle_id,
        .source_stream_id = entry.source_stream_id,
        .capture_date = entry.capture_date,
        .stream_day_id = entry.stream_day_id,
        .ack_status = entry.ack_status,
        .ack_writer_instance = entry.writer_instance,
        .safe_stop_template_id =
            entry.safe_stop_template_id};
}

ingress::ReserveFinalizationActionKeyV1 ActionKey(
    const ingress::ReserveStateSlotV1& slot,
    std::size_t action_index) {
    const auto& action =
        slot.entries[0U].actions[action_index];
    return ingress::ReserveFinalizationActionKeyV1{
        .grant = GrantKey(slot),
        .action_id =
            static_cast<std::uint16_t>(action_index),
        .action_kind = action.action_kind,
        .object_plan_sha256 =
            action.object_plan_sha256};
}

struct ArchiveFixture final {
    ingress::ReserveCoordinatorHeaderV1 header{};
    ingress::ReserveStateSlotV1 before_report_debit{};
    ingress::ReserveStateSlotV1 before_all_done{};
    ingress::ReserveStateSlotV1 all_done{};
    std::unique_ptr<
        ingress::BuiltEmptyAnchorTombstoneV1>
        tombstone;
    std::unique_ptr<
        ingress::BuiltFinalizationReportV1>
        report;
    std::unique_ptr<
        ingress::BuiltFinalizationArchiveV1>
        archive;
};

ArchiveFixture MakeArchiveFixtureWithHeader(
    TestContext* test,
    const ingress::ReserveCoordinatorHeaderV1&
        requested_header,
    std::uint8_t cycle_seed = 0x30U) {
    ArchiveFixture fixture{};
    fixture.header = requested_header;
    EmptyEvidenceFixture empty =
        MakeEmptyEvidence(test);
    fixture.tombstone = std::move(empty.tombstone);
    if (fixture.tombstone == nullptr) {
        return fixture;
    }

    ingress::ReserveStateSlotV1 provisioned{};
    provisioned.coordinator_state =
        ingress::ReserveCoordinatorPhaseV1::kProvisioned;
    provisioned.generation = 10U;
    provisioned.reserve_state_uuid =
        fixture.header.reserve_state_uuid;
    provisioned.entry_count = 1U;
    auto& registry = provisioned.entries[0U];
    registry.source_stream_id =
        empty.namespace_identity.source_stream_id;
    registry.capture_date =
        empty.namespace_identity.capture_date;
    registry.stream_day_id =
        empty.namespace_identity.stream_day_id;
    registry.registry_status =
        ingress::ReserveRegistryStatusV1::kInit;
    registry.recovery_origin =
        ingress::ReserveRecoveryOriginV1::kFreshInit;
    registry.recovery_intent =
        ingress::ReserveRecoveryIntentV1::
            kResumeConnect;
    registry.writer_instance =
        Pattern<16U>(0x51U);
    registry.executor_or_recovery_attempt =
        Pattern<16U>(0x71U);
    registry.safe_stop_template_id = 9001U;

    ingress::ReserveReleaseIntentV1 intent_request{};
    intent_request.reason =
        ingress::ReserveReleaseReasonV1::
            kLowWatermark;
    intent_request.trigger =
        ingress::ReserveReleaseTriggerV1::
            kFilesystemBytes;
    intent_request.writer_set_sha256 =
        Pattern<32U>(0xa1U);
    ingress::ReserveStateSlotV1 intent{};
    test->Expect(
        ingress::BuildReserveReleasingIntentV1(
            fixture.header,
            provisioned,
            intent_request,
            &intent) ==
            ingress::ReserveStateV1Error::kNone,
        "build archive release INTENT fixture");

    ingress::ReserveStateEntryV1 grant{};
    grant.source_stream_id = registry.source_stream_id;
    grant.capture_date = registry.capture_date;
    grant.stream_day_id = registry.stream_day_id;
    grant.ack_status =
        ingress::ReserveAckStatusV1::kAcked;
    grant.grant_status =
        ingress::ReserveGrantStatusV1::kPending;
    grant.grant_flags =
        ingress::kReserveGrantRawAnchorOnly;
    grant.writer_instance = registry.writer_instance;
    grant.anchor_only_payload.recovery_attempt_id =
        registry.executor_or_recovery_attempt;
    grant.anchor_only_payload.journal_header_sha256 =
        empty.journal_header_sha256;
    grant.anchor_only_payload.origin_registry_status =
        ingress::ReserveRegistryStatusV1::kInit;
    grant.anchor_only_payload.recovery_origin =
        registry.recovery_origin;
    grant.anchor_only_payload.original_recovery_intent =
        registry.recovery_intent;
    grant.anchor_only_payload.finalization_intent =
        ingress::ReserveRecoveryIntentV1::
            kRecoverSealOnly;
    grant.anchor_only_payload.required_action_bitmap =
        0x7U;
    grant.safe_stop_template_id =
        registry.safe_stop_template_id;
    SetAction(
        &grant,
        0U,
        ingress::FinalizationActionKindV1::
            kJournalAnchor,
        1U,
        1U,
        0xb1U);
    SetAction(
        &grant,
        1U,
        ingress::FinalizationActionKindV1::
            kEmptyAnchorTombstone,
        1U,
        1U,
        0xc1U);
    SetAction(
        &grant,
        2U,
        ingress::FinalizationActionKindV1::
            kFinalizationReport,
        16U,
        1U,
        0xd1U);
    grant.grant_bytes =
        18U *
        fixture.header.allocation_quantum_bytes;

    const std::array grants{grant};
    ingress::ReserveReleasePreparedV1 prepared_request{};
    prepared_request.finalization_cycle_id =
        Pattern<16U>(cycle_seed);
    prepared_request.grant_entries = grants;
    prepared_request.pre_release_fs_free_bytes =
        1'000'000U;
    prepared_request.pre_release_quota_free_bytes =
        900'000U;
    prepared_request.pre_release_fs_free_inodes = 500U;
    prepared_request.pre_release_quota_free_inodes =
        400U;
    prepared_request.expected_release_fs_bytes =
        fixture.header.declared_releasable_bytes;
    prepared_request.expected_release_quota_bytes =
        fixture.header.declared_releasable_bytes;
    prepared_request.expected_release_fs_inodes =
        fixture.header.declared_inode_reserve_count + 1U;
    prepared_request.expected_release_quota_inodes =
        fixture.header.declared_inode_reserve_count + 1U;
    prepared_request.reserved_margin_bytes =
        fixture.header.allocation_quantum_bytes;
    prepared_request.reserved_margin_inodes = 1U;
    prepared_request.effective_min_fs_free_bytes =
        fixture.header.declared_releasable_bytes;
    prepared_request.effective_min_quota_free_bytes =
        fixture.header.declared_releasable_bytes;
    prepared_request.effective_min_fs_free_inodes =
        fixture.header.declared_inode_reserve_count;
    prepared_request.effective_min_quota_free_inodes =
        fixture.header.declared_inode_reserve_count;

    ingress::ReserveStateSlotV1 prepared{};
    test->Expect(
        ingress::BuildReserveReleasingPreparedV1(
            fixture.header,
            intent,
            prepared_request,
            &prepared) ==
            ingress::ReserveStateV1Error::kNone,
        "build archive release PREPARED fixture");
    ingress::ReserveStateSlotV1 consumed{};
    test->Expect(
        ingress::BuildReserveConsumedV1(
            fixture.header,
            prepared,
            &consumed) ==
            ingress::ReserveStateV1Error::kNone,
        "build archive CONSUMED fixture");

    ingress::ReserveStateV1Digest grant_hash{};
    test->Expect(
        ingress::
            ComputeImmutableFinalizationGrantSha256V1(
                fixture.header,
                consumed,
                0U,
                &grant_hash) ==
            ingress::ReserveStateV1Error::kNone,
        "derive archive immutable grant hash");

    ingress::FinalizationReportV1 report_model{};
    report_model.reserve_state_uuid =
        fixture.header.reserve_state_uuid;
    report_model.finalization_cycle_id =
        consumed.finalization_cycle_id;
    report_model.namespace_identity =
        empty.namespace_identity;
    report_model.ack_status = grant.ack_status;
    report_model.ack_writer_instance =
        grant.writer_instance;
    report_model.immutable_grant_sha256 =
        grant_hash;
    report_model.tail_classification =
        ingress::
            FinalizationReportTailClassificationV1::
                kNone;
    report_model.tail_classification_valid = true;
    report_model.gap_classification =
        ingress::
            FinalizationReportGapClassificationV1::
                kNone;
    report_model.gap_classification_valid = true;
    report_model.result =
        ingress::FinalizationReportResultV1::
            kEmptyAnchorOnly;
    report_model.journal_header_sha256 =
        empty.journal_header_sha256;
    report_model.marker_count = 0U;
    report_model.empty_anchor_tombstone_sha256 =
        fixture.tombstone->tombstone_sha256();
    test->Expect(
        ingress::BuildFinalizationReportCapabilityV1(
            report_model,
            ingress::kReserveGrantRawAnchorOnly,
            nullptr,
            fixture.tombstone.get(),
            &fixture.report) ==
                ingress::FinalizationReportV1Error::
                    kNone &&
            fixture.report != nullptr,
        "build archive final report fixture");
    if (fixture.report == nullptr) {
        return fixture;
    }

    ingress::ReserveGrantActivationV1 activation{};
    activation.grant = GrantKey(consumed);
    activation.executor_instance =
        grant.writer_instance;
    activation.filesystem_free_byte_baseline =
        800'000U;
    activation.quota_free_byte_baseline = 700'000U;
    activation.filesystem_free_inode_baseline = 300U;
    activation.quota_free_inode_baseline = 200U;
    ingress::ReserveStateSlotV1 state{};
    test->Expect(
        ingress::BuildReserveActivateNextGrantV1(
            fixture.header,
            consumed,
            activation,
            &state) ==
            ingress::ReserveStateV1Error::kNone,
        "activate archive grant");
    for (std::size_t action_index = 0U;
         action_index < 2U;
         ++action_index) {
        ingress::ReserveStateSlotV1 debited{};
        test->Expect(
            ingress::BuildReserveDebitActionV1(
                fixture.header,
                state,
                ActionKey(state, action_index),
                &debited) ==
                ingress::ReserveStateV1Error::kNone,
            "debit archive evidence action");
        ingress::ReserveStateSlotV1 complete{};
        test->Expect(
            ingress::BuildReserveCompleteActionV1(
                fixture.header,
                debited,
                ActionKey(debited, action_index),
                &complete) ==
                ingress::ReserveStateV1Error::kNone,
            "complete archive evidence action");
        state = complete;
    }
    ingress::ReserveStateSlotV1 report_debited{};
    fixture.before_report_debit = state;
    test->Expect(
        ingress::BuildReserveDebitActionV1(
            fixture.header,
            state,
            ActionKey(state, 2U),
            &report_debited) ==
            ingress::ReserveStateV1Error::kNone,
        "debit archive report action");
    ingress::ReserveAtomicReportCompletionV1 completion{};
    completion.report_action =
        ActionKey(report_debited, 2U);
    completion.maintenance_report_sha256 =
        fixture.report->report_sha256();
    fixture.before_all_done = report_debited;
    test->Expect(
        ingress::
            BuildReserveCompleteReportAndMarkGrantDoneV1(
                fixture.header,
                report_debited,
                completion,
                &fixture.all_done) ==
            ingress::ReserveStateV1Error::kNone,
        "complete archive report and all-DONE grant");

    std::vector<
        ingress::FinalizationArchiveArtifactInputV1>
        evidence;
    evidence.push_back({
        ingress::FinalizationArchiveArtifactTypeV1::
            kEmptyAnchorTombstone,
        std::string(
            "capture_date=20260719/"
            "stream=1001-sh-snapshot/"
            "maintenance/") +
        std::string(fixture.tombstone->filename()),
        std::string(
            fixture.tombstone->canonical_jcs()),
        {}});
    evidence.push_back({
        ingress::FinalizationArchiveArtifactTypeV1::
            kFinalizationReport,
        {},
        std::string(fixture.report->canonical_jcs()),
        "sh-snapshot"});
    test->Expect(
        ingress::BuildFinalizationArchiveCapabilityV1(
            fixture.header,
            fixture.all_done,
            evidence,
            &fixture.archive) ==
                ingress::FinalizationArchiveV1Error::
                    kNone &&
            fixture.archive != nullptr,
        "build private all-DONE finalization archive capability");
    return fixture;
}

ArchiveFixture MakeArchiveFixture(
    TestContext* test) {
    return MakeArchiveFixtureWithHeader(
        test, MakeHeader());
}

void TestModelAndSchema(TestContext* test) {
    ArchiveFixture fixture = MakeArchiveFixture(test);
    if (fixture.archive == nullptr) {
        return;
    }
    test->Expect(
        fixture.archive->directory_name() ==
            "finalization-"
            "101112131415161718191a1b1c1d1e1f-"
            "303132333435363738393a3b3c3d3e3f",
        "archive final directory matches the frozen locator");
    test->Expect(
        fixture.archive->directory_locator() ==
            "finalization-"
            "101112131415161718191a1b1c1d1e1f-"
            "303132333435363738393a3b3c3d3e3f/",
        "archive logical locator has the required trailing slash");
    test->Expect(
        fixture.archive->temporary_directory_name() ==
            ".finalization-"
            "101112131415161718191a1b1c1d1e1f-"
            "303132333435363738393a3b3c3d3e3f"
            ".finalization-archive-v1.tmp",
        "archive temporary is deterministic");
    test->Expect(
        fixture.archive->model().artifact_count == 2U &&
            fixture.archive->model()
                    .artifacts[0U]
                    .artifact_type ==
                ingress::
                    FinalizationArchiveArtifactTypeV1::
                        kFinalizationReport &&
            fixture.archive->model()
                    .artifacts[1U]
                    .artifact_type ==
                ingress::
                    FinalizationArchiveArtifactTypeV1::
                        kEmptyAnchorTombstone,
        "builder canonicalizes evidence ordering");

    ingress::FinalizationArchiveV1 parsed{};
    test->Expect(
        ingress::ParseFinalizationArchiveV1Jcs(
            fixture.archive->canonical_jcs(),
            &parsed) ==
                ingress::FinalizationArchiveV1Error::
                    kNone &&
            parsed == fixture.archive->model(),
        "archive strict JCS parser round-trips exact bytes");
    std::string unknown(
        fixture.archive->canonical_jcs());
    unknown.insert(unknown.size() - 1U, ",\"x\":0");
    const auto sentinel = parsed.reserve_state_uuid;
    test->Expect(
        ingress::ParseFinalizationArchiveV1Jcs(
            unknown, &parsed) ==
                ingress::FinalizationArchiveV1Error::
                    kInvalidCanonicalJson &&
            parsed.reserve_state_uuid == sentinel,
        "archive parser rejects unknown fields atomically");
    std::string wrong_routed_slug(
        fixture.archive->canonical_jcs());
    constexpr std::string_view expected_route_slug =
        "stream=1001-sh-snapshot/"
        "maintenance/";
    const std::size_t route_slug =
        wrong_routed_slug.find(expected_route_slug);
    test->Expect(
        route_slug != std::string::npos,
        "archive golden contains the built-in stream route");
    if (route_slug != std::string::npos) {
        wrong_routed_slug.replace(
            route_slug,
            expected_route_slug.size(),
            "stream=1001-sh-tick/"
            "maintenance/");
        test->Expect(
            ingress::ParseFinalizationArchiveV1Jcs(
                wrong_routed_slug, &parsed) ==
                ingress::FinalizationArchiveV1Error::
                    kInvalidSourceLocator,
            "archive parser rejects a legal slug belonging to another stream id");
    }
    std::string leading_zero(
        fixture.archive->canonical_jcs());
    const std::string canonical =
        "\"all_done_generation\":\"20\"";
    const std::size_t generation =
        leading_zero.find(canonical);
    test->Expect(
        generation != std::string::npos,
        "archive golden contains all-DONE generation");
    if (generation != std::string::npos) {
        leading_zero.replace(
            generation,
            canonical.size(),
            "\"all_done_generation\":\"020\"");
        test->Expect(
            ingress::ParseFinalizationArchiveV1Jcs(
                leading_zero, &parsed) ==
                ingress::FinalizationArchiveV1Error::
                    kInvalidCanonicalJson,
            "archive parser rejects leading-zero uint64");
    }

    const std::string digest = common::Sha256Hex(
        common::ComputeSha256(
            fixture.archive->canonical_jcs()));
    if (digest !=
        "ab901dee24e106d234cb076ae7def850"
        "57503209c75672d9567a56c9680adebf") {
        std::cerr
            << "observed archive golden digest: "
            << digest << '\n';
    }
    test->Expect(
        digest ==
            "ab901dee24e106d234cb076ae7def850"
            "57503209c75672d9567a56c9680adebf",
        "archive complete-byte JCS golden SHA-256");
    test->Expect(
        fixture.archive->manifest_sha256() ==
            common::ComputeSha256(
                fixture.archive->canonical_jcs()),
        "archive capability freezes manifest hash");

    const std::string schema_path =
        std::string(L2FLOW_SOURCE_DIR) +
        "/schemas/finalization_archive_v1.json";
    std::ifstream input(
        schema_path, std::ios::binary);
    const std::string schema{
        std::istreambuf_iterator<char>(input),
        std::istreambuf_iterator<char>()};
    test->Expect(
        input.good() || input.eof(),
        "archive schema is readable");
    test->Expect(
        schema.size() ==
            ingress::kFinalizationArchiveV1SchemaBytes,
        "archive schema byte count is frozen");
    test->Expect(
        common::Sha256Hex(
            common::ComputeSha256(schema)) ==
            ingress::
                kFinalizationArchiveV1SchemaSha256Hex,
        "archive schema SHA-256 is frozen");

    std::vector<
        ingress::FinalizationArchiveArtifactInputV1>
        missing_sidecar;
    missing_sidecar.push_back({
        ingress::FinalizationArchiveArtifactTypeV1::
            kFinalizationReport,
        {},
        std::string(fixture.report->canonical_jcs()),
        "sh-snapshot"});
    std::unique_ptr<
        ingress::BuiltFinalizationArchiveV1>
        rejected;
    test->Expect(
        ingress::BuildFinalizationArchiveCapabilityV1(
            fixture.header,
            fixture.all_done,
            missing_sidecar,
            &rejected) ==
                ingress::FinalizationArchiveV1Error::
                    kMissingSidecar &&
            rejected == nullptr,
        "archive builder cannot omit a report-referenced sidecar");

    const std::array arbitrary_parent{
        ingress::FinalizationArchiveArtifactInputV1{
            ingress::
                FinalizationArchiveArtifactTypeV1::
                    kFinalizationReport,
            std::string(
                "arbitrary/foo/maintenance/") +
                std::string(fixture.report->filename()),
            std::string(
                fixture.report->canonical_jcs()),
            "sh-snapshot"}};
    test->Expect(
        ingress::BuildFinalizationArchiveCapabilityV1(
            fixture.header,
            fixture.all_done,
            arbitrary_parent,
            &rejected) ==
                ingress::FinalizationArchiveV1Error::
                    kInvalidSourceLocator &&
            rejected == nullptr,
        "archive builder never trusts a caller-supplied routed report parent");

    const std::array wrong_canonical_slug{
        ingress::FinalizationArchiveArtifactInputV1{
            ingress::
                FinalizationArchiveArtifactTypeV1::
                    kFinalizationReport,
            {},
            std::string(
                fixture.report->canonical_jcs()),
            "sh-tick"}};
    test->Expect(
        ingress::BuildFinalizationArchiveCapabilityV1(
            fixture.header,
            fixture.all_done,
            wrong_canonical_slug,
            &rejected) ==
                ingress::FinalizationArchiveV1Error::
                    kInvalidSourceLocator &&
            rejected == nullptr,
        "archive builder rejects a canonical slug mapped to another stream id");
}

struct AuditDirectory final {
    TempDirectory root;
    std::filesystem::path path;
    ScopedFd descriptor;

    explicit AuditDirectory(TestContext* test) {
        path = root.path() / "reserve-audit";
        test->Expect(
            ::mkdir(path.c_str(), 0700U) == 0,
            "create private reserve-audit fixture");
        descriptor = ScopedFd(::open(
            path.c_str(),
            O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
                O_CLOEXEC));
        test->Expect(
            descriptor.get() >= 0,
            "open retained reserve-audit fixture");
    }
};

void TestPosixTree(TestContext* test) {
    ArchiveFixture fixture = MakeArchiveFixture(test);
    if (fixture.archive == nullptr) {
        return;
    }

    AuditDirectory first(test);
    auto published =
        ingress::PublishFinalizationArchiveV1At(
            first.descriptor.get(),
            *fixture.archive);
    test->Expect(
        published.ok() &&
            published.disposition ==
                ingress::
                    FinalizationArchivePosixDispositionV1::
                        kPublishedNew &&
            published.files_synced &&
            published.subdirectories_synced &&
            published.audit_parent_synced &&
            published.receipt->Validate(),
        "publish immutable archive tree and retained receipt");
    auto existing =
        ingress::PublishFinalizationArchiveV1At(
            first.descriptor.get(),
            *fixture.archive);
    test->Expect(
        existing.ok() &&
            existing.disposition ==
                ingress::
                    FinalizationArchivePosixDispositionV1::
                        kAcceptedExistingFinal &&
            existing.receipt->Validate(),
        "existing exact archive is re-synced and accepted");

    AuditDirectory adoption(test);
    auto initial =
        ingress::PublishFinalizationArchiveV1At(
            adoption.descriptor.get(),
            *fixture.archive);
    test->Expect(
        initial.ok(), "build complete archive for adoption");
    initial.receipt.reset();
    test->Expect(
        ::renameat(
            adoption.descriptor.get(),
            std::string(
                fixture.archive->directory_name())
                .c_str(),
            adoption.descriptor.get(),
            std::string(
                fixture.archive
                    ->temporary_directory_name())
                .c_str()) == 0 &&
            Fsync(adoption.descriptor.get()),
        "place complete tree in deterministic tmp crash window");
    auto adopted =
        ingress::PublishFinalizationArchiveV1At(
            adoption.descriptor.get(),
            *fixture.archive);
    test->Expect(
        adopted.ok() &&
            adopted.disposition ==
                ingress::
                    FinalizationArchivePosixDispositionV1::
                        kAdoptedCompleteTemporary &&
            adopted.receipt->Validate(),
        "adopt complete archive temporary through no-replace barrier");

    AuditDirectory partial(test);
    const std::string temporary_name(
        fixture.archive->temporary_directory_name());
    test->Expect(
        ::mkdirat(
            partial.descriptor.get(),
            temporary_name.c_str(),
            0700U) == 0,
        "create recognized partial archive root");
    ScopedFd partial_root(::openat(
        partial.descriptor.get(),
        temporary_name.c_str(),
        O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
            O_CLOEXEC));
    ScopedFd partial_file(::openat(
        partial_root.get(),
        std::string(
            ingress::
                kFinalizationArchiveV1StateHeaderFilename)
            .c_str(),
        O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW |
            O_CLOEXEC,
        0600U));
    const std::array<std::byte, 16U> prefix =
        Pattern<16U>(0xe1U);
    test->Expect(
        partial_root.get() >= 0 &&
            partial_file.get() >= 0 &&
            ::write(
                partial_file.get(),
                prefix.data(),
                prefix.size()) ==
                static_cast<ssize_t>(prefix.size()) &&
            Fsync(partial_file.get()) &&
            Fsync(partial_root.get()) &&
            Fsync(partial.descriptor.get()),
        "persist recognized short-write archive crash window");
    partial_file = ScopedFd{};
    partial_root = ScopedFd{};
    auto rebuilt =
        ingress::PublishFinalizationArchiveV1At(
            partial.descriptor.get(),
            *fixture.archive);
    test->Expect(
        rebuilt.ok() &&
            rebuilt.disposition ==
                ingress::
                    FinalizationArchivePosixDispositionV1::
                        kRebuiltRecognizedPartialTemporary &&
            rebuilt.receipt->Validate(),
        "recognized partial tmp is removed bottom-up and rebuilt");

    if (published.receipt != nullptr) {
        const auto& artifact =
            fixture.archive->model().artifacts[0U];
        const std::filesystem::path evidence_path =
            first.path /
            std::string(
                fixture.archive->directory_name()) /
            artifact.archive_path;
        ScopedFd tamper(::open(
            evidence_path.c_str(),
            O_RDWR | O_NOFOLLOW | O_CLOEXEC));
        const std::byte replacement{0x5bU};
        test->Expect(
            tamper.get() >= 0 &&
                ::pwrite(
                    tamper.get(),
                    &replacement,
                    1U,
                    0) == 1 &&
                Fsync(tamper.get()),
            "tamper archive evidence for receipt negative case");
        test->Expect(
            !published.receipt->Validate(),
            "retained archive receipt detects exact-byte change");
    }
}

void TestRestartLoad(TestContext* test) {
    ArchiveFixture fixture = MakeArchiveFixture(test);
    if (fixture.archive == nullptr) {
        return;
    }
    AuditDirectory audit(test);
    const auto expected_manifest =
        fixture.archive->manifest_sha256();
    const std::string expected_directory(
        fixture.archive->directory_name());
    auto published =
        ingress::PublishFinalizationArchiveV1At(
            audit.descriptor.get(),
            *fixture.archive);
    test->Expect(
        published.ok() &&
            published.receipt->Validate(),
        "publish archive before simulated process restart");
    published.receipt.reset();
    fixture.archive.reset();

    std::string diagnostic;
    auto loaded =
        ingress::
            LoadPublishedFinalizationArchiveCapabilityV1At(
                audit.descriptor.get(),
                fixture.header,
                fixture.all_done,
                &diagnostic);
    test->Expect(
        loaded.ok() &&
            loaded.directory_name ==
                expected_directory &&
            loaded.manifest_sha256 ==
                expected_manifest &&
            loaded.archive->manifest_sha256() ==
                expected_manifest,
        "restart reconstructs the private archive capability from exact durable bytes: " +
            diagnostic);
    if (loaded.ok()) {
        auto republished =
            ingress::PublishFinalizationArchiveV1At(
                audit.descriptor.get(),
                *loaded.archive,
                &diagnostic);
        test->Expect(
            republished.ok() &&
                republished.disposition ==
                    ingress::
                        FinalizationArchivePosixDispositionV1::
                            kAcceptedExistingFinal &&
                republished.receipt->Validate(),
            "reconstructed capability re-runs durability barriers and issues a cleanup receipt");
    }

    auto wrong_slot = fixture.all_done;
    wrong_slot.finalization_cycle_id =
        Pattern<16U>(0xf1U);
    auto rejected =
        ingress::
            LoadPublishedFinalizationArchiveCapabilityV1At(
                audit.descriptor.get(),
                fixture.header,
                wrong_slot);
    test->Expect(
        !rejected.ok() &&
            rejected.archive == nullptr,
        "restart loader never selects an archive from mismatched state identity");
}

}  // namespace

int main() {
    TestContext test;
    TestModelAndSchema(&test);
    TestPosixTree(&test);
    TestRestartLoad(&test);
    if (test.failures != 0) {
        std::cerr << test.failures
                  << " finalization archive assertion(s) failed\n";
        return 1;
    }
    std::cout << "finalization archive tests passed\n";
    return 0;
}
