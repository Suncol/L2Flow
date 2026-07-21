#define main l2flow_finalization_archive_core_test_main
#include "test_phase2_finalization_archive_v1.cpp"
#undef main

#include "l2flow/ingress/finalization_archive_source_cleanup_posix.h"
#include "l2flow/ingress/raw_reserve_coordinator_gate.h"
#include "l2flow/ingress/scaffolding_finalization_report_v1.h"

namespace {

struct RawSourceTree final {
    TempDirectory root;
    std::filesystem::path maintenance;
    ScopedFd root_fd;
    ScopedFd maintenance_fd;

    explicit RawSourceTree(TestContext* test) {
        const auto date =
            root.path() / "capture_date=20260719";
        const auto stream =
            date /
            "stream=1001-sh-snapshot";
        maintenance = stream / "maintenance";
        test->Expect(
            ::mkdir(date.c_str(), 0700U) == 0 &&
                ::mkdir(stream.c_str(), 0700U) == 0 &&
                ::mkdir(maintenance.c_str(), 0700U) ==
                    0,
            "create owner-only Raw source route");
        root_fd = ScopedFd(::open(
            root.path().c_str(),
            O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
                O_CLOEXEC | O_NOATIME));
        maintenance_fd = ScopedFd(::open(
            maintenance.c_str(),
            O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
                O_CLOEXEC | O_NOATIME));
        test->Expect(
            root_fd.get() >= 0 &&
                maintenance_fd.get() >= 0,
            "retain Raw root and source maintenance directory");
    }
};

ingress::ScaffoldingObjectPostStateV1
ScaffoldingPostState(
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
    return ingress::ScaffoldingObjectPostStateV1::
        kAbsent;
}

struct ScaffoldingArchiveFixture final {
    ingress::ReserveCoordinatorHeaderV1 header{};
    ingress::ReserveStateSlotV1 before_report_debit{};
    ingress::ReserveStateSlotV1 before_all_done{};
    ingress::ReserveStateSlotV1 all_done{};
    std::unique_ptr<
        ingress::BuiltEmptyAnchorTombstoneV1>
        tombstone;
    std::unique_ptr<
        ingress::BuiltScaffoldingFinalizationReportV1>
        report;
    std::unique_ptr<
        ingress::BuiltFinalizationArchiveV1>
        archive;
};

ScaffoldingArchiveFixture
MakeScaffoldingArchiveFixture(
    TestContext* test,
    const ingress::ReserveCoordinatorHeaderV1&
        requested_header) {
    ScaffoldingArchiveFixture fixture{};
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
        ingress::ReserveRegistryStatusV1::kScaffolding;
    registry.recovery_origin =
        ingress::ReserveRecoveryOriginV1::kFreshInit;
    registry.recovery_intent =
        ingress::ReserveRecoveryIntentV1::
            kResumeConnect;
    registry.writer_instance = Pattern<16U>(0x51U);
    registry.executor_or_recovery_attempt =
        Pattern<16U>(0x71U);
    registry.safe_stop_template_id = 9001U;
    registry.grant_bytes =
        19U *
        fixture.header.allocation_quantum_bytes;

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
        "build scaffolding archive INTENT");

    ingress::ReserveStateEntryV1 grant{};
    grant.source_stream_id = registry.source_stream_id;
    grant.capture_date = registry.capture_date;
    grant.stream_day_id = registry.stream_day_id;
    grant.ack_status =
        ingress::ReserveAckStatusV1::kAcked;
    grant.grant_status =
        ingress::ReserveGrantStatusV1::kPending;
    grant.grant_flags =
        ingress::kReserveGrantScaffoldingOnly;
    grant.writer_instance = registry.writer_instance;
    grant.scaffolding_payload.recovery_attempt_id =
        registry.executor_or_recovery_attempt;
    grant.scaffolding_payload.object_snapshot_sha256 =
        Pattern<32U>(0x91U);
    grant.scaffolding_payload.observed_object_bitmap = 0U;
    grant.scaffolding_payload.required_action_bitmap =
        0x7fU;
    grant.safe_stop_template_id =
        registry.safe_stop_template_id;
    SetAction(
        &grant,
        0U,
        ingress::FinalizationActionKindV1::
            kDirectoryLeaseScaffold,
        1U,
        1U,
        0xb1U);
    SetAction(
        &grant,
        1U,
        ingress::FinalizationActionKindV1::
            kJournalAnchor,
        1U,
        1U,
        0xc1U);
    SetAction(
        &grant,
        2U,
        ingress::FinalizationActionKindV1::
            kEmptyAnchorTombstone,
        1U,
        1U,
        0xd1U);
    SetAction(
        &grant,
        3U,
        ingress::FinalizationActionKindV1::
            kFinalizationReport,
        16U,
        1U,
        0xe1U);
    grant.grant_bytes =
        19U *
        fixture.header.allocation_quantum_bytes;

    const std::array grants{grant};
    ingress::ReserveReleasePreparedV1 prepared_request{};
    prepared_request.finalization_cycle_id =
        Pattern<16U>(0x30U);
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
        "build scaffolding archive PREPARED");
    ingress::ReserveStateSlotV1 consumed{};
    test->Expect(
        ingress::BuildReserveConsumedV1(
            fixture.header,
            prepared,
            &consumed) ==
            ingress::ReserveStateV1Error::kNone,
        "build scaffolding archive CONSUMED");

    ingress::ReserveStateV1Digest grant_hash{};
    test->Expect(
        ingress::
            ComputeImmutableFinalizationGrantSha256V1(
                fixture.header,
                consumed,
                0U,
                &grant_hash) ==
            ingress::ReserveStateV1Error::kNone,
        "compute scaffolding immutable grant hash");

    ingress::ScaffoldingFinalizationReportV1 model{};
    model.reserve_state_uuid =
        fixture.header.reserve_state_uuid;
    model.finalization_cycle_id =
        consumed.finalization_cycle_id;
    model.planned_namespace =
        empty.namespace_identity;
    model.planned_recovery_attempt_id =
        grant.scaffolding_payload.recovery_attempt_id;
    model.immutable_grant_sha256 = grant_hash;
    model.object_snapshot_sha256 =
        grant.scaffolding_payload.object_snapshot_sha256;
    model.observed_object_bitmap = 0U;
    model.required_action_bitmap = 0x7fU;
    for (std::uint8_t wire = 1U; wire <= 7U; ++wire) {
        const auto role =
            static_cast<
                ingress::ScaffoldingObjectRoleV1>(wire);
        ingress::ScaffoldingObjectStateV1 object{};
        object.role = role;
        object.start_state =
            ingress::ScaffoldingObjectStartStateV1::
                kAbsent;
        object.post_state =
            ScaffoldingPostState(role);
        object.barriers.object_synced =
            object.post_state !=
            ingress::ScaffoldingObjectPostStateV1::
                kAbsent;
        object.barriers.parent_directory_synced = true;
        object.barriers.retained_fd_revalidated = true;
        model.objects.push_back(object);

        ingress::ScaffoldingActionResultV1 action{};
        action.action_id =
            static_cast<std::uint8_t>(wire - 1U);
        action.action_kind =
            ingress::ScaffoldingActionKindV1::
                kRevalidatePostState;
        action.object_role = role;
        action.post_state = object.post_state;
        action.completed = true;
        model.actions.push_back(action);
    }
    model.journal_header_sha256 =
        empty.journal_header_sha256;
    model.index_absent = true;
    model.manifest_absent = true;
    model.control_absent = true;
    model.empty_anchor_tombstone_sha256 =
        fixture.tombstone->tombstone_sha256();
    test->Expect(
        ingress::
            BuildScaffoldingFinalizationReportCapabilityV1(
                model,
                ingress::kReserveGrantScaffoldingOnly,
                *fixture.tombstone,
                &fixture.report) ==
                ingress::
                    ScaffoldingFinalizationReportV1Error::
                        kNone &&
            fixture.report != nullptr,
        "build scaffolding finalization report capability");
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
        "activate scaffolding archive grant");
    for (std::size_t action_index = 0U;
         action_index < 3U;
         ++action_index) {
        ingress::ReserveStateSlotV1 debited{};
        ingress::ReserveStateSlotV1 complete{};
        test->Expect(
            ingress::BuildReserveDebitActionV1(
                fixture.header,
                state,
                ActionKey(state, action_index),
                &debited) ==
                ingress::ReserveStateV1Error::kNone &&
                ingress::BuildReserveCompleteActionV1(
                    fixture.header,
                    debited,
                    ActionKey(debited, action_index),
                    &complete) ==
                ingress::ReserveStateV1Error::kNone,
            "complete scaffolding archive non-report action");
        state = complete;
    }
    ingress::ReserveStateSlotV1 report_debited{};
    fixture.before_report_debit = state;
    test->Expect(
        ingress::BuildReserveDebitActionV1(
            fixture.header,
            state,
            ActionKey(state, 3U),
            &report_debited) ==
            ingress::ReserveStateV1Error::kNone,
        "debit scaffolding archive report action");
    ingress::ReserveAtomicReportCompletionV1 completion{};
    completion.report_action =
        ActionKey(report_debited, 3U);
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
        "complete scaffolding archive report action");

    const std::array evidence{
        ingress::FinalizationArchiveArtifactInputV1{
            ingress::
                FinalizationArchiveArtifactTypeV1::
                    kScaffoldingFinalizationReport,
            {},
            std::string(
                fixture.report->canonical_jcs()),
            {}},
        ingress::FinalizationArchiveArtifactInputV1{
            ingress::
                FinalizationArchiveArtifactTypeV1::
                    kEmptyAnchorTombstone,
            std::string(
                "capture_date=20260719/"
                "stream=1001-sh-snapshot/"
                "maintenance/") +
                std::string(
                    fixture.tombstone->filename()),
            std::string(
                fixture.tombstone->canonical_jcs()),
            {}}};
    test->Expect(
        ingress::BuildFinalizationArchiveCapabilityV1(
            fixture.header,
            fixture.all_done,
            evidence,
            &fixture.archive) ==
                ingress::FinalizationArchiveV1Error::
                    kNone &&
            fixture.archive != nullptr,
        "build scaffolding all-DONE archive capability");
    return fixture;
}

[[nodiscard]] bool WriteAll(
    int fd,
    std::string_view bytes) noexcept {
    std::size_t completed = 0U;
    while (completed < bytes.size()) {
        const ssize_t result = ::write(
            fd,
            bytes.data() + completed,
            bytes.size() - completed);
        if (result < 0 && errno == EINTR) {
            continue;
        }
        if (result <= 0) {
            return false;
        }
        completed +=
            static_cast<std::size_t>(result);
    }
    return true;
}

ingress::ReserveCoordinatorHeaderV1 HeaderForRawRoot(
    TestContext* test,
    const RawSourceTree& raw) {
    auto header = MakeHeader();
    struct stat status {};
    test->Expect(
        ::fstat(raw.root_fd.get(), &status) == 0,
        "inspect Raw root device for reserve header");
    header.device_id =
        static_cast<std::uint64_t>(status.st_dev);
    return header;
}

[[nodiscard]] bool WriteStateAt(
    int raw_root_fd,
    const ingress::ReserveCoordinatorHeaderV1& header,
    const ingress::ReserveStateSlotV1& older,
    const ingress::ReserveStateSlotV1& selected,
    bool create) {
    ingress::ReserveCoordinatorStateV1 state{};
    state.header = header;
    const std::size_t newer_index =
        (selected.generation & 1U) == 0U ? 1U : 0U;
    state.slots[newer_index] = selected;
    state.slots[1U - newer_index] = older;
    state.selected_slot = newer_index;
    ingress::ReserveStateV1FileWire wire{};
    const auto encode_error =
        ingress::EncodeReserveCoordinatorStateV1(
            state, &wire);
    if (encode_error !=
        ingress::ReserveStateV1Error::kNone) {
        std::cerr
            << "state fixture encode failed: "
            << ingress::ReserveStateV1ErrorName(
                   encode_error)
            << " older=" << older.generation
            << " selected=" << selected.generation
            << '\n';
        return false;
    }
    const int creation_flags =
        create ? O_CREAT | O_EXCL : O_TRUNC;
    ScopedFd file(::openat(
        raw_root_fd,
        ingress::kRawReserveStateFilename,
        O_RDWR | O_NOFOLLOW | O_CLOEXEC |
            creation_flags,
        0600U));
    return file.get() >= 0 &&
           ::fchmod(file.get(), 0600U) == 0 &&
           WriteAll(
               file.get(),
               std::string_view(
                   reinterpret_cast<const char*>(
                       wire.data()),
                   wire.size())) &&
           Fsync(file.get()) && Fsync(raw_root_fd);
}

template <typename Fixture>
[[nodiscard]] bool InstallMaintenanceDomain(
    TestContext* test,
    const RawSourceTree& raw,
    const Fixture& fixture) {
    ingress::RawReserveCoordinatorLeaseMarkerV1 marker{};
    marker.coordinator_identity =
        Pattern<16U>(0xe1U);
    marker.device_id = fixture.header.device_id;
    marker.quota_identity_sha256 =
        fixture.header.quota_identity_sha256;
    marker.mount_identity_sha256 =
        fixture.header.mount_identity_sha256;
    ingress::RawReserveCoordinatorGateError error{};
    std::string diagnostic;
    auto lease =
        ingress::AcquireRawReserveCoordinatorLeaseAtV1(
            raw.root_fd.get(),
            marker,
            &error,
            &diagnostic);
    test->Expect(
        lease != nullptr,
        "create fixed coordinator lease fixture: " +
            diagnostic);
    if (lease == nullptr) {
        return false;
    }
    const bool state = WriteStateAt(
        raw.root_fd.get(),
        fixture.header,
        fixture.before_all_done,
        fixture.all_done,
        true);
    test->Expect(
        state,
        "publish exact live all-DONE reserve.state fixture");
    return state;
}

template <typename Fixture>
[[nodiscard]] std::unique_ptr<
    ingress::RawReserveCoordinatorLeaseV1>
AcquireBusyCoordinator(
    TestContext* test,
    const RawSourceTree& raw,
    const Fixture& fixture) {
    ingress::RawReserveCoordinatorLeaseMarkerV1 marker{};
    marker.coordinator_identity =
        Pattern<16U>(0xe1U);
    marker.device_id = fixture.header.device_id;
    marker.quota_identity_sha256 =
        fixture.header.quota_identity_sha256;
    marker.mount_identity_sha256 =
        fixture.header.mount_identity_sha256;
    std::string diagnostic;
    auto lease =
        ingress::AcquireRawReserveCoordinatorLeaseAtV1(
            raw.root_fd.get(),
            marker,
            nullptr,
            &diagnostic);
    test->Expect(
        lease != nullptr,
        "acquire live coordinator fixture: " +
            diagnostic);
    return lease;
}

[[nodiscard]] bool CreateFileAt(
    int parent_fd,
    std::string_view filename,
    std::string_view bytes) noexcept {
    ScopedFd file(::openat(
        parent_fd,
        std::string(filename).c_str(),
        O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW |
            O_CLOEXEC,
        0600U));
    return file.get() >= 0 &&
           ::fchmod(file.get(), 0600U) == 0 &&
           WriteAll(file.get(), bytes) &&
           Fsync(file.get()) && Fsync(parent_fd);
}

[[nodiscard]] bool ExistsNoFollow(
    int parent_fd,
    std::string_view filename,
    struct stat* output = nullptr) noexcept {
    struct stat status {};
    const bool exists =
        ::fstatat(
            parent_fd,
            std::string(filename).c_str(),
            &status,
            AT_SYMLINK_NOFOLLOW) == 0;
    if (exists && output != nullptr) {
        *output = status;
    }
    return exists;
}

struct ReappearHookContext final {
    int parent_fd = -1;
    std::string filename;
    std::string bytes;
    bool created = false;
};

bool ReappearAfterAbsentSync(
    ingress::
        FinalizationArchiveSourceCleanupMutationPointV1
            point,
    std::size_t,
    void* opaque) noexcept {
    auto* context =
        static_cast<ReappearHookContext*>(opaque);
    if (context == nullptr ||
        point !=
            ingress::
                FinalizationArchiveSourceCleanupMutationPointV1::
                    kAfterParentSyncBeforeAbsentRecheck) {
        return true;
    }
    context->created = CreateFileAt(
        context->parent_fd,
        context->filename,
        context->bytes);
    return context->created;
}

bool InterruptAfterUnlink(
    ingress::
        FinalizationArchiveSourceCleanupMutationPointV1
            point,
    std::size_t,
    void*) noexcept {
    return point !=
           ingress::
               FinalizationArchiveSourceCleanupMutationPointV1::
                   kAfterSourceUnlinkBeforeParentSync;
}

[[nodiscard]] ingress::
    FinalizationArchivePosixPublishResultV1
PublishArchive(
    AuditDirectory* audit,
    const ArchiveFixture& fixture) {
    return ingress::PublishFinalizationArchiveV1At(
        audit->descriptor.get(), *fixture.archive);
}

void TestCleanupAndAbsentAdoption(TestContext* test) {
    RawSourceTree raw(test);
    ArchiveFixture fixture =
        MakeArchiveFixtureWithHeader(
            test, HeaderForRawRoot(test, raw));
    if (fixture.archive == nullptr ||
        fixture.report == nullptr ||
        fixture.tombstone == nullptr ||
        !InstallMaintenanceDomain(test, raw, fixture)) {
        return;
    }
    AuditDirectory audit(test);
    auto published = PublishArchive(&audit, fixture);
    test->Expect(
        published.ok() && published.receipt->Validate(),
        "publish authoritative archive before source cleanup");
    test->Expect(
        CreateFileAt(
            raw.maintenance_fd.get(),
            fixture.report->filename(),
            fixture.report->canonical_jcs()) &&
            CreateFileAt(
                raw.maintenance_fd.get(),
                fixture.tombstone->filename(),
                fixture.tombstone->canonical_jcs()),
        "persist report source and retained tombstone source");

    std::string diagnostic;
    auto cleaned =
        ingress::CleanupFinalizationArchiveSourceReportsV1At(
            raw.root_fd.get(),
            std::move(published.receipt),
            nullptr,
            &diagnostic);
    if (!cleaned.ok()) {
        std::cerr
            << "cleanup diagnostic: "
            << ingress::
                   FinalizationArchiveSourceCleanupErrorV1Name(
                       cleaned.error)
            << ": " << diagnostic << '\n';
    }
    test->Expect(
        cleaned.ok() &&
            cleaned.cleanable_source_count == 1U &&
            cleaned.removed_source_count == 1U &&
            cleaned.adopted_absent_source_count == 0U &&
            cleaned.receipt->Validate(),
        "exact archived finalization report is unlinked and parent-synced");
    test->Expect(
        !ExistsNoFollow(
            raw.maintenance_fd.get(),
            fixture.report->filename()) &&
            ExistsNoFollow(
                raw.maintenance_fd.get(),
                fixture.tombstone->filename()),
        "cleanup never removes EmptyAnchorTombstoneV1");
    ingress::RawReserveCoordinatorLeaseMarkerV1 marker{};
    marker.coordinator_identity =
        Pattern<16U>(0xe1U);
    marker.device_id = fixture.header.device_id;
    marker.quota_identity_sha256 =
        fixture.header.quota_identity_sha256;
    marker.mount_identity_sha256 =
        fixture.header.mount_identity_sha256;
    ingress::RawReserveCoordinatorGateError busy_error{};
    auto blocked =
        ingress::AcquireRawReserveCoordinatorLeaseAtV1(
            raw.root_fd.get(),
            marker,
            &busy_error);
    test->Expect(
        blocked == nullptr &&
            busy_error ==
                ingress::
                    RawReserveCoordinatorGateError::
                        kCoordinatorBusy,
        "successful cleanup receipt retains the real exclusive maintenance flock");
    cleaned.receipt.reset();

    auto republished = PublishArchive(&audit, fixture);
    auto adopted =
        ingress::CleanupFinalizationArchiveSourceReportsV1At(
            raw.root_fd.get(),
            std::move(republished.receipt));
    test->Expect(
        adopted.ok() &&
            adopted.removed_source_count == 0U &&
            adopted.adopted_absent_source_count == 1U &&
            adopted.receipt->Validate(),
        "restart fsyncs absent source parent and adopts durable absence");
}

void TestFaultWindowAndReappearance(TestContext* test) {
    RawSourceTree raw(test);
    ArchiveFixture fixture =
        MakeArchiveFixtureWithHeader(
            test, HeaderForRawRoot(test, raw));
    if (fixture.archive == nullptr ||
        fixture.report == nullptr ||
        !InstallMaintenanceDomain(test, raw, fixture)) {
        return;
    }
    AuditDirectory audit(test);
    auto first = PublishArchive(&audit, fixture);
    test->Expect(
        first.ok() &&
            CreateFileAt(
                raw.maintenance_fd.get(),
                fixture.report->filename(),
                fixture.report->canonical_jcs()),
        "prepare source unlink crash window");
    const ingress::
        FinalizationArchiveSourceCleanupHooksV1
            interrupt_hooks{
                .allow = &InterruptAfterUnlink,
                .context = nullptr};
    auto interrupted =
        ingress::CleanupFinalizationArchiveSourceReportsV1At(
            raw.root_fd.get(),
            std::move(first.receipt),
            &interrupt_hooks);
    test->Expect(
        interrupted.error ==
                ingress::
                    FinalizationArchiveSourceCleanupErrorV1::
                        kInjectedInterruption &&
            interrupted.receipt == nullptr &&
            !ExistsNoFollow(
                raw.maintenance_fd.get(),
                fixture.report->filename()),
        "fault after unlink leaves restart-adoptable absent source");

    auto retry_archive = PublishArchive(&audit, fixture);
    auto retry =
        ingress::CleanupFinalizationArchiveSourceReportsV1At(
            raw.root_fd.get(),
            std::move(retry_archive.receipt));
    test->Expect(
        retry.ok() &&
            retry.adopted_absent_source_count == 1U &&
            retry.receipt->Validate(),
        "restart closes unlink-before-dirsync window");
    retry.receipt.reset();

    auto reappear_archive = PublishArchive(&audit, fixture);
    ReappearHookContext context{
        .parent_fd = raw.maintenance_fd.get(),
        .filename =
            std::string(fixture.report->filename()),
        .bytes =
            std::string(fixture.report->canonical_jcs()),
        .created = false};
    const ingress::
        FinalizationArchiveSourceCleanupHooksV1
            reappear_hooks{
                .allow = &ReappearAfterAbsentSync,
                .context = &context};
    auto reappeared =
        ingress::CleanupFinalizationArchiveSourceReportsV1At(
            raw.root_fd.get(),
            std::move(reappear_archive.receipt),
            &reappear_hooks);
    test->Expect(
        reappeared.ok() && context.created &&
            reappeared.removed_source_count == 1U &&
            !ExistsNoFollow(
                raw.maintenance_fd.get(),
                fixture.report->filename()) &&
            reappeared.receipt->Validate(),
        "exact name reappearing after absent-parent resync is revalidated and deleted");
}

void TestScaffoldingAuditRootCleanup(TestContext* test) {
    RawSourceTree raw(test);
    ScaffoldingArchiveFixture fixture =
        MakeScaffoldingArchiveFixture(
            test, HeaderForRawRoot(test, raw));
    if (fixture.archive == nullptr ||
        fixture.report == nullptr ||
        fixture.tombstone == nullptr ||
        !InstallMaintenanceDomain(test, raw, fixture)) {
        return;
    }
    AuditDirectory audit(test);
    test->Expect(
        ::mkdirat(
            audit.descriptor.get(),
            "emergency-reports",
            0700U) == 0 &&
            Fsync(audit.descriptor.get()),
        "create retained emergency-reports directory");
    ScopedFd emergency(::openat(
        audit.descriptor.get(),
        "emergency-reports",
        O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
            O_NOATIME | O_CLOEXEC));
    test->Expect(
        emergency.get() >= 0 &&
            CreateFileAt(
                emergency.get(),
                fixture.report->filename(),
                fixture.report->canonical_jcs()),
        "persist scaffolding finalization report source");
    auto published =
        ingress::PublishFinalizationArchiveV1At(
            audit.descriptor.get(),
            *fixture.archive);
    const bool archive_published = published.ok();
    auto cleaned =
        ingress::CleanupFinalizationArchiveSourceReportsV1At(
            raw.root_fd.get(),
            std::move(published.receipt));
    test->Expect(
        archive_published && cleaned.ok() &&
            cleaned.cleanable_source_count == 1U &&
            cleaned.removed_source_count == 1U &&
            !ExistsNoFollow(
                emergency.get(),
                fixture.report->filename()) &&
            cleaned.receipt->Validate(),
        "scaffolding report resolves only beneath retained reserve-audit/emergency-reports");

    const std::array bad_evidence{
        ingress::FinalizationArchiveArtifactInputV1{
            ingress::
                FinalizationArchiveArtifactTypeV1::
                    kScaffoldingFinalizationReport,
            std::string("wrong/emergency-reports/") +
                std::string(fixture.report->filename()),
            std::string(
                fixture.report->canonical_jcs()),
            {}},
        ingress::FinalizationArchiveArtifactInputV1{
            ingress::
                FinalizationArchiveArtifactTypeV1::
                    kEmptyAnchorTombstone,
            std::string(
                "capture_date=20260719/"
                "stream=1001-sh-snapshot/"
                "maintenance/") +
                std::string(
                    fixture.tombstone->filename()),
            std::string(
                fixture.tombstone->canonical_jcs()),
            {}}};
    std::unique_ptr<
        ingress::BuiltFinalizationArchiveV1>
        rejected;
    test->Expect(
        ingress::BuildFinalizationArchiveCapabilityV1(
            fixture.header,
            fixture.all_done,
            bad_evidence,
            &rejected) ==
                ingress::FinalizationArchiveV1Error::
                    kInvalidSourceLocator &&
            rejected == nullptr,
        "archive capability rejects caller-chosen scaffolding cleanup roots");
}

void TestMaintenanceAuthorization(TestContext* test) {
    RawSourceTree raw(test);
    const auto header = HeaderForRawRoot(test, raw);
    ArchiveFixture fixture =
        MakeArchiveFixtureWithHeader(
            test, header, 0x30U);
    ArchiveFixture other_cycle =
        MakeArchiveFixtureWithHeader(
            test, header, 0x31U);
    if (fixture.archive == nullptr ||
        fixture.report == nullptr ||
        other_cycle.archive == nullptr ||
        !InstallMaintenanceDomain(test, raw, fixture)) {
        return;
    }
    AuditDirectory audit(test);
    test->Expect(
        CreateFileAt(
            raw.maintenance_fd.get(),
            fixture.report->filename(),
            fixture.report->canonical_jcs()),
        "persist maintenance-gated source report");

    auto busy_archive = PublishArchive(&audit, fixture);
    auto live_coordinator =
        AcquireBusyCoordinator(test, raw, fixture);
    auto busy =
        ingress::CleanupFinalizationArchiveSourceReportsV1At(
            raw.root_fd.get(),
            std::move(busy_archive.receipt));
    test->Expect(
        busy.error ==
                ingress::
                    FinalizationArchiveSourceCleanupErrorV1::
                        kMaintenanceLeaseBusy &&
            ExistsNoFollow(
                raw.maintenance_fd.get(),
                fixture.report->filename()),
        "live coordinator exclusive flock rejects cleanup before unlink");
    live_coordinator.reset();

    auto non_done_owner =
        AcquireBusyCoordinator(test, raw, fixture);
    test->Expect(
        non_done_owner != nullptr &&
            WriteStateAt(
                raw.root_fd.get(),
                fixture.header,
                fixture.before_report_debit,
                fixture.before_all_done,
                false),
        "replace test state with valid non-all-DONE frontier under lease");
    non_done_owner.reset();
    auto non_done_archive = PublishArchive(&audit, fixture);
    auto non_done =
        ingress::CleanupFinalizationArchiveSourceReportsV1At(
            raw.root_fd.get(),
            std::move(non_done_archive.receipt));
    if (non_done.error !=
        ingress::
            FinalizationArchiveSourceCleanupErrorV1::
                kStateNotAllDone) {
        std::cerr
            << "non-DONE cleanup error: "
            << ingress::
                   FinalizationArchiveSourceCleanupErrorV1Name(
                       non_done.error)
            << '\n';
    }
    test->Expect(
        non_done.error ==
                ingress::
                    FinalizationArchiveSourceCleanupErrorV1::
                        kStateNotAllDone &&
            ExistsNoFollow(
                raw.maintenance_fd.get(),
                fixture.report->filename()),
        "valid CONSUMED but non-all-DONE state rejects cleanup before unlink");

    auto mismatch_owner =
        AcquireBusyCoordinator(test, raw, fixture);
    test->Expect(
        mismatch_owner != nullptr &&
            WriteStateAt(
                raw.root_fd.get(),
                other_cycle.header,
                other_cycle.before_all_done,
                other_cycle.all_done,
                false),
        "replace test state with another valid all-DONE cycle");
    mismatch_owner.reset();
    auto mismatch_archive = PublishArchive(&audit, fixture);
    auto mismatch =
        ingress::CleanupFinalizationArchiveSourceReportsV1At(
            raw.root_fd.get(),
            std::move(mismatch_archive.receipt));
    test->Expect(
        mismatch.error ==
                ingress::
                    FinalizationArchiveSourceCleanupErrorV1::
                        kStateMismatch &&
            ExistsNoFollow(
                raw.maintenance_fd.get(),
                fixture.report->filename()),
        "different valid all-DONE header/slot/hash rejects cleanup before unlink");

    auto restore_owner =
        AcquireBusyCoordinator(test, raw, fixture);
    test->Expect(
        restore_owner != nullptr &&
            WriteStateAt(
                raw.root_fd.get(),
                fixture.header,
                fixture.before_all_done,
                fixture.all_done,
                false),
        "restore exact archived all-DONE live state");
    restore_owner.reset();
    auto exact_archive = PublishArchive(&audit, fixture);
    auto exact =
        ingress::CleanupFinalizationArchiveSourceReportsV1At(
            raw.root_fd.get(),
            std::move(exact_archive.receipt));
    test->Expect(
        exact.ok() &&
            exact.removed_source_count == 1U &&
            exact.receipt->Validate(),
        "exact lease marker plus live header/slot/hash authorizes cleanup");
}

void TestConflictsFailClosed(TestContext* test) {
    RawSourceTree raw(test);
    ArchiveFixture fixture =
        MakeArchiveFixtureWithHeader(
            test, HeaderForRawRoot(test, raw));
    if (fixture.archive == nullptr ||
        fixture.report == nullptr ||
        !InstallMaintenanceDomain(test, raw, fixture)) {
        return;
    }
    AuditDirectory audit(test);

    auto conflict_archive = PublishArchive(&audit, fixture);
    test->Expect(
        conflict_archive.ok() &&
            CreateFileAt(
                raw.maintenance_fd.get(),
                fixture.report->filename(),
                "{}"),
        "prepare conflicting report source");
    auto conflict =
        ingress::CleanupFinalizationArchiveSourceReportsV1At(
            raw.root_fd.get(),
            std::move(conflict_archive.receipt));
    test->Expect(
        conflict.error ==
                ingress::
                    FinalizationArchiveSourceCleanupErrorV1::
                        kSourceConflict &&
            ExistsNoFollow(
                raw.maintenance_fd.get(),
                fixture.report->filename()),
        "byte/hash/JCS conflict is retained and fails closed");
    test->Expect(
        ::unlinkat(
            raw.maintenance_fd.get(),
            std::string(fixture.report->filename())
                .c_str(),
            0) == 0 &&
            Fsync(raw.maintenance_fd.get()),
        "remove test-only conflicting source");

    auto symlink_archive = PublishArchive(&audit, fixture);
    const std::string target_name = "symlink-target";
    test->Expect(
        CreateFileAt(
            raw.maintenance_fd.get(),
            target_name,
            fixture.report->canonical_jcs()) &&
            ::symlinkat(
                target_name.c_str(),
                raw.maintenance_fd.get(),
                std::string(fixture.report->filename())
                    .c_str()) == 0 &&
            Fsync(raw.maintenance_fd.get()),
        "prepare no-follow symlink source");
    auto symlink =
        ingress::CleanupFinalizationArchiveSourceReportsV1At(
            raw.root_fd.get(),
            std::move(symlink_archive.receipt));
    struct stat symlink_status {};
    test->Expect(
        symlink.error ==
                ingress::
                    FinalizationArchiveSourceCleanupErrorV1::
                        kSourceConflict &&
            ExistsNoFollow(
                raw.maintenance_fd.get(),
                fixture.report->filename(),
                &symlink_status) &&
            S_ISLNK(symlink_status.st_mode),
        "symlink source is never followed or unlinked");
    test->Expect(
        ::unlinkat(
            raw.maintenance_fd.get(),
            std::string(fixture.report->filename())
                .c_str(),
            0) == 0 &&
            ::unlinkat(
                raw.maintenance_fd.get(),
                target_name.c_str(),
                0) == 0 &&
            Fsync(raw.maintenance_fd.get()),
        "remove test-only symlink fixture");

    auto hardlink_archive = PublishArchive(&audit, fixture);
    const std::string hardlink_target = "hardlink-target";
    test->Expect(
        CreateFileAt(
            raw.maintenance_fd.get(),
            hardlink_target,
            fixture.report->canonical_jcs()) &&
            ::linkat(
                raw.maintenance_fd.get(),
                hardlink_target.c_str(),
                raw.maintenance_fd.get(),
                std::string(fixture.report->filename())
                    .c_str(),
                0) == 0 &&
            Fsync(raw.maintenance_fd.get()),
        "prepare hardlinked report source");
    auto hardlink =
        ingress::CleanupFinalizationArchiveSourceReportsV1At(
            raw.root_fd.get(),
            std::move(hardlink_archive.receipt));
    struct stat hardlink_status {};
    test->Expect(
        hardlink.error ==
                ingress::
                    FinalizationArchiveSourceCleanupErrorV1::
                        kSourceConflict &&
            ExistsNoFollow(
                raw.maintenance_fd.get(),
                fixture.report->filename(),
                &hardlink_status) &&
            hardlink_status.st_nlink ==
                static_cast<nlink_t>(2),
        "hardlinked report source is retained and fails closed");
}

}  // namespace

#ifndef L2FLOW_EMBED_FINALIZATION_ARCHIVE_SOURCE_CLEANUP_TEST
int main() {
    TestContext test;
    TestCleanupAndAbsentAdoption(&test);
    TestFaultWindowAndReappearance(&test);
    TestScaffoldingAuditRootCleanup(&test);
    TestMaintenanceAuthorization(&test);
    TestConflictsFailClosed(&test);
    if (test.failures != 0) {
        std::cerr << test.failures
                  << " finalization archive source-cleanup assertion(s) failed\n";
        return 1;
    }
    std::cout
        << "finalization archive source-cleanup tests passed\n";
    return 0;
}
#endif
