#define L2FLOW_EMBED_FINALIZATION_ARCHIVE_SOURCE_CLEANUP_TEST
#include "test_phase2_finalization_archive_source_cleanup_posix.cpp"
#undef L2FLOW_EMBED_FINALIZATION_ARCHIVE_SOURCE_CLEANUP_TEST

#include "l2flow/ingress/raw_emergency_reserve_reprovision_posix.h"

#include <atomic>
#include <csignal>
#include <thread>

#include <sys/wait.h>

namespace {

class ReprovisionCapacityProbe final
    : public ingress::RawEmergencyReserveCapacityProbeV1 {
public:
    [[nodiscard]] bool Observe(
        int,
        const ingress::
            RawEmergencyReserveCapacityProbeRequestV1&
                request,
        ingress::
            RawEmergencyReserveCapacityObservationV1*
                observation,
        std::string*) noexcept override {
        requests.push_back(request);
        if (observation == nullptr) {
            return false;
        }
        ingress::RawEmergencyReserveCapacityObservationV1
            value{};
        value.pool = request.pool;
        value.byte_probe_version =
            request.byte_probe_version;
        value.inode_probe_version =
            request.inode_probe_version;
        value.filesystem_bytes_proven = true;
        value.quota_bytes_proven = true;
        value.filesystem_inodes_proven = true;
        value.quota_inodes_proven =
            !reject_quota_inode_proof;
        value.reserve_byte_charge_proven = true;
        value.reserve_inode_charge_proven = true;
        value.filesystem_free_bytes =
            1ULL << 40U;
        value.quota_free_bytes = 1ULL << 40U;
        value.filesystem_free_inodes =
            1ULL << 30U;
        value.quota_free_inodes = 1ULL << 30U;
        value.proven_reserve_byte_charge =
            request.required_bytes;
        value.proven_reserve_inode_charge =
            request.required_inodes;
        *observation = value;
        return true;
    }

    bool reject_quota_inode_proof = false;
    std::vector<
        ingress::RawEmergencyReserveCapacityProbeRequestV1>
        requests;
};

ingress::ReserveCoordinatorHeaderV1 SmallHeaderForRawRoot(
    TestContext* test,
    const RawSourceTree& raw) {
    auto header = HeaderForRawRoot(test, raw);
    header.declared_releasable_bytes = 1024U * 1024U;
    header.allocation_quantum_bytes = 4096U;
    header.declared_inode_reserve_count = 8U;
    ingress::ReserveStateV1Digest inventory{};
    test->Expect(
        ingress::
            ComputeRawEmergencyReserveInventorySha256V1(
                header, &inventory) ==
            ingress::
                RawEmergencyReservePosixErrorV1::kNone,
        "compute small old reserve inventory commitment");
    header.inode_inventory_sha256 = inventory;
    return header;
}

struct ReprovisionHarness final {
    RawSourceTree raw;
    ArchiveFixture fixture;
    AuditDirectory audit;
    std::unique_ptr<
        ingress::FinalizationArchiveSourceCleanupReceiptV1>
        cleanup_receipt;

    explicit ReprovisionHarness(TestContext* test)
        : raw(test),
          fixture(MakeArchiveFixtureWithHeader(
              test,
              SmallHeaderForRawRoot(test, raw))),
          audit(test) {
        if (fixture.archive == nullptr ||
            fixture.report == nullptr) {
            return;
        }
        test->Expect(
            ::mkdirat(
                raw.root_fd.get(),
                ingress::
                    kRawEmergencyReserveInodesDirectory,
                0700U) == 0 &&
                Fsync(raw.root_fd.get()),
            "preserve the empty reserve-inodes directory from the old reserve");
        if (!InstallMaintenanceDomain(
                test, raw, fixture)) {
            return;
        }
        auto archive = PublishArchive(&audit, fixture);
        test->Expect(
            archive.ok() &&
                CreateFileAt(
                    raw.maintenance_fd.get(),
                    fixture.report->filename(),
                    fixture.report->canonical_jcs()),
            "publish final archive and live source report before cleanup");
        if (!archive.ok()) {
            return;
        }
        auto cleanup =
            ingress::
                CleanupFinalizationArchiveSourceReportsV1At(
                    raw.root_fd.get(),
                    std::move(archive.receipt));
        test->Expect(
            cleanup.ok() &&
                cleanup.receipt->Validate(),
            "obtain archive+source-cleanup+maintenance capability");
        cleanup_receipt = std::move(cleanup.receipt);
    }

    [[nodiscard]] ingress::
        RawEmergencyReserveReprovisionRequestV1
    Request(
        TestContext* test,
        std::uint8_t uuid_seed = 0xd0U) const {
        ingress::
            RawEmergencyReserveReprovisionRequestV1
                request{};
        ingress::ReserveStateV1Error codec_error{};
        std::string diagnostic;
        test->Expect(
            ingress::
                BuildRawEmergencyReserveReprovisionBootstrapV1(
                    fixture.header,
                    Pattern<16U>(uuid_seed),
                    &request.bootstrap,
                    &codec_error,
                    &diagnostic) ==
                    ingress::
                        RawEmergencyReservePosixErrorV1::
                            kNone,
            "build exact new-UUID reprovision bootstrap: " +
                diagnostic);
        request.metadata_margin_bytes = 32U * 1024U;
        request.metadata_margin_inodes = 4U;
        return request;
    }
};

struct InterruptContext final {
    ingress::
        RawEmergencyReserveReprovisionMutationPointV1
            target =
                ingress::
                    RawEmergencyReserveReprovisionMutationPointV1::
                        kCandidateSetReset;
    std::uint32_t target_index = 0U;
    bool interrupted = false;
};

bool InterruptAt(
    ingress::
        RawEmergencyReserveReprovisionMutationPointV1 point,
    std::uint32_t index,
    void* opaque) noexcept {
    auto* context =
        static_cast<InterruptContext*>(opaque);
    if (context != nullptr &&
        point == context->target &&
        index == context->target_index) {
        context->interrupted = true;
        return false;
    }
    return true;
}

bool KillProcessAtStateCandidate(
    ingress::
        RawEmergencyReserveReprovisionMutationPointV1 point,
    std::uint32_t,
    void*) noexcept {
    if (point ==
        ingress::
            RawEmergencyReserveReprovisionMutationPointV1::
                kStateCandidateSynced) {
        static_cast<void>(
            ::kill(::getpid(), SIGKILL));
    }
    return true;
}

std::string DataCandidateNameForTest(
    const ingress::ReserveStateV1Identity& uuid) {
    return ".reserve.data." +
           common::Identity128Hex(uuid) +
           ".reserve-file-v1.tmp";
}

[[nodiscard]] bool PreadExactForTest(
    int fd,
    std::span<std::byte> bytes) noexcept {
    std::size_t completed = 0U;
    while (completed < bytes.size()) {
        const ssize_t count = ::pread(
            fd,
            bytes.data() + completed,
            bytes.size() - completed,
            static_cast<off_t>(completed));
        if (count < 0 && errno == EINTR) {
            continue;
        }
        if (count <= 0) {
            return false;
        }
        completed += static_cast<std::size_t>(count);
    }
    return true;
}

[[nodiscard]] bool ReplaceFixedStateWithExactNewInode(
    int root_fd) {
    ScopedFd old_state(::openat(
        root_fd,
        ingress::kRawReserveStateFilename,
        O_RDONLY | O_NOFOLLOW | O_CLOEXEC));
    ingress::ReserveStateV1FileWire wire{};
    if (old_state.get() < 0 ||
        !PreadExactForTest(old_state.get(), wire)) {
        return false;
    }
    constexpr char candidate[] =
        ".reserve.state.test-exact-inode-attack";
    ScopedFd replacement(::openat(
        root_fd,
        candidate,
        O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW |
            O_CLOEXEC,
        0600U));
    return replacement.get() >= 0 &&
           WriteAll(
               replacement.get(),
               std::string_view(
                   reinterpret_cast<const char*>(
                       wire.data()),
                   wire.size())) &&
           Fsync(replacement.get()) &&
           ::renameat(
               root_fd,
               candidate,
               root_fd,
               ingress::kRawReserveStateFilename) == 0 &&
           Fsync(root_fd);
}

void TestNormalAtomicReplacement(TestContext* test) {
    ReprovisionHarness harness(test);
    if (harness.cleanup_receipt == nullptr) {
        return;
    }
    auto request = harness.Request(test);
    ReprovisionCapacityProbe probe;
    const int old_state_fd =
        harness.cleanup_receipt->Validate()
            ? ::openat(
                  harness.raw.root_fd.get(),
                  ingress::kRawReserveStateFilename,
                  O_RDONLY | O_NOFOLLOW | O_CLOEXEC)
            : -1;
    ScopedFd retained_old(old_state_fd);
    ScopedFd retained_lease(::openat(
        harness.raw.root_fd.get(),
        ingress::kRawReserveCoordinatorLeaseFilename,
        O_RDONLY | O_NOFOLLOW | O_CLOEXEC));
    struct stat old_before {};
    struct stat lease_before {};
    ingress::RawReserveCoordinatorLeaseMarkerWireV1
        lease_wire_before{};
    test->Expect(
        retained_old.get() >= 0 &&
            retained_lease.get() >= 0 &&
            ::fstat(retained_old.get(), &old_before) == 0 &&
            ::fstat(
                retained_lease.get(),
                &lease_before) == 0 &&
            PreadExactForTest(
                retained_lease.get(),
                lease_wire_before),
        "retain old-state and immutable coordinator-lease evidence before replacement");

    std::string diagnostic;
    auto result =
        ingress::ReprovisionRawEmergencyReserveV1(
            std::move(harness.cleanup_receipt),
            request,
            &probe,
            nullptr,
            &diagnostic);
    if (!result.ok()) {
        std::cerr
            << "normal reprovision failed: "
            << ingress::
                   RawEmergencyReserveReprovisionErrorV1Name(
                       result.error)
            << ": " << diagnostic << '\n';
    }
    test->Expect(
        result.ok() &&
            result.state_replace_may_have_occurred &&
            !result.fail_stop_required &&
            result.receipt->Validate(),
        "all-DONE cleanup capability publishes and validates a complete new reserve");
    if (!result.ok()) {
        return;
    }

    struct stat old_after {};
    struct stat fixed_after {};
    struct stat lease_after {};
    ingress::RawReserveCoordinatorLeaseMarkerWireV1
        lease_wire_after{};
    test->Expect(
        ::fstat(retained_old.get(), &old_after) == 0 &&
            ::fstatat(
                harness.raw.root_fd.get(),
                ingress::kRawReserveStateFilename,
                &fixed_after,
                AT_SYMLINK_NOFOLLOW) == 0 &&
            old_before.st_dev == old_after.st_dev &&
            old_before.st_ino == old_after.st_ino &&
            old_after.st_ino != fixed_after.st_ino,
        "atomic replacement keeps the old fd on the old inode while fixed name selects the new inode");
    test->Expect(
        ::fstat(retained_lease.get(), &lease_after) == 0 &&
            PreadExactForTest(
                retained_lease.get(),
                lease_wire_after) &&
            lease_before.st_dev == lease_after.st_dev &&
            lease_before.st_ino == lease_after.st_ino &&
            lease_wire_before == lease_wire_after &&
            result.receipt
                    ->retained_old_state_descriptor() >=
                0,
        "reprovision leaves persistent coordinator lease inode/marker unchanged and retains archived old state fd");

    ingress::RawReserveStatePosixError state_error{};
    std::string attach_diagnostic;
    auto attached_state =
        ingress::AttachRawReserveStateAtV1(
            harness.raw.root_fd.get(),
            &state_error,
            nullptr,
            &attach_diagnostic);
    auto attached_inventory =
        attached_state == nullptr
            ? nullptr
            : ingress::
                  AttachRawEmergencyReserveInventoryV1(
                      *attached_state,
                      &probe,
                      nullptr,
                      &attach_diagnostic);
    test->Expect(
        attached_state != nullptr &&
            attached_state->state() ==
                request.bootstrap &&
            attached_inventory != nullptr &&
            attached_inventory->data_present() &&
            attached_inventory
                    ->remaining_inode_prefix_count() ==
                request.bootstrap.header
                    .declared_inode_reserve_count,
        "state-first attach sees only the exact complete new PROVISIONED reserve");
    test->Expect(
        !probe.requests.empty() &&
            probe.requests.front().stage ==
                ingress::
                    RawEmergencyReserveProbeStageV1::
                        kBeforeProvision &&
            probe.requests.front().required_bytes >=
                request.bootstrap.header
                    .declared_releasable_bytes +
                    request.metadata_margin_bytes &&
            probe.requests.front().required_inodes >=
                request.bootstrap.header
                    .declared_inode_reserve_count +
                    request.metadata_margin_inodes,
        "four-dimensional pre-provision probe includes reserve, state, and caller metadata margins");
}

void TestBootstrapAndCapabilityGates(TestContext* test) {
    ReprovisionHarness harness(test);
    if (harness.cleanup_receipt == nullptr) {
        return;
    }
    ingress::ReserveCoordinatorStateV1 rejected{};
    test->Expect(
        ingress::
            BuildRawEmergencyReserveReprovisionBootstrapV1(
                harness.fixture.header,
                {},
                &rejected) ==
                ingress::
                    RawEmergencyReservePosixErrorV1::
                        kInvalidArgument &&
            ingress::
                BuildRawEmergencyReserveReprovisionBootstrapV1(
                    harness.fixture.header,
                    harness.fixture.header
                        .reserve_state_uuid,
                    &rejected) ==
                ingress::
                    RawEmergencyReservePosixErrorV1::
                        kInvalidArgument,
        "zero and reused reserve UUIDs cannot form a reprovision bootstrap");

    auto request = harness.Request(test);
    ReprovisionCapacityProbe probe;
    auto no_candidate =
        ingress::
            DiscoverRawEmergencyReserveReprovisionCandidateV1(
                *harness.cleanup_receipt,
                request.metadata_margin_bytes,
                request.metadata_margin_inodes);
    test->Expect(
        no_candidate.error ==
                ingress::
                    RawEmergencyReserveReprovisionErrorV1::
                        kNoDurableCandidate &&
            !no_candidate.ok(),
        "clean old all-DONE state has no restart UUID to invent or guess");
    auto missing =
        ingress::ReprovisionRawEmergencyReserveV1(
            nullptr, request, &probe);
    test->Expect(
        missing.error ==
                ingress::
                    RawEmergencyReserveReprovisionErrorV1::
                        kInvalidArgument &&
            missing.receipt == nullptr &&
            missing.retry_cleanup_receipt == nullptr,
        "reprovision cannot run without the cleanup capability");

    auto wrong_domain = request;
    wrong_domain.bootstrap.header
        .safe_stop_catalog_sha256[0U] ^=
        std::byte{0x01U};
    auto domain_rejected =
        ingress::ReprovisionRawEmergencyReserveV1(
            std::move(harness.cleanup_receipt),
            wrong_domain,
            &probe);
    test->Expect(
        domain_rejected.error ==
                ingress::
                    RawEmergencyReserveReprovisionErrorV1::
                        kBootstrapInvalid &&
            domain_rejected.retry_cleanup_receipt !=
                nullptr &&
            domain_rejected.retry_cleanup_receipt
                ->Validate(),
        "new bootstrap cannot change sizing, device, quota/mount, probe, or safe-stop domain facts");
    harness.cleanup_receipt =
        std::move(
            domain_rejected.retry_cleanup_receipt);

    test->Expect(
        WriteStateAt(
            harness.raw.root_fd.get(),
            harness.fixture.header,
            harness.fixture.before_report_debit,
            harness.fixture.before_all_done,
            false),
        "simulate fixed old-state mutation after cleanup capability issuance");
    auto changed =
        ingress::ReprovisionRawEmergencyReserveV1(
            std::move(harness.cleanup_receipt),
            request,
            &probe);
    test->Expect(
        changed.error ==
                ingress::
                    RawEmergencyReserveReprovisionErrorV1::
                        kCleanupBarrierInvalid &&
            changed.receipt == nullptr &&
            changed.retry_cleanup_receipt == nullptr,
        "changed old state invalidates cleanup authority before new allocation");
}

void TestByteIdenticalOldStateInodeAttack(
    TestContext* test) {
    ReprovisionHarness harness(test);
    if (harness.cleanup_receipt == nullptr) {
        return;
    }
    auto request = harness.Request(test, 0xdbU);
    ReprovisionCapacityProbe probe;
    test->Expect(
        ReplaceFixedStateWithExactNewInode(
            harness.raw.root_fd.get()),
        "replace fixed old state with a byte-identical different inode");
    auto rejected =
        ingress::ReprovisionRawEmergencyReserveV1(
            std::move(harness.cleanup_receipt),
            request,
            &probe);
    test->Expect(
        rejected.error ==
                ingress::
                    RawEmergencyReserveReprovisionErrorV1::
                        kCleanupBarrierInvalid &&
            rejected.retry_cleanup_receipt == nullptr &&
            !ExistsNoFollow(
                harness.raw.root_fd.get(),
                ingress::
                    kRawEmergencyReserveDataFilename),
        "byte-identical fixed-state inode replacement invalidates authority before allocation");
}

void TestInterruptedBuildAndPublishedDataRetry(
    TestContext* test) {
    {
        ReprovisionHarness harness(test);
        if (harness.cleanup_receipt == nullptr) {
            return;
        }
        auto request = harness.Request(test, 0xd1U);
        ReprovisionCapacityProbe probe;
        InterruptContext context{
            .target =
                ingress::
                    RawEmergencyReserveReprovisionMutationPointV1::
                        kInodePublished,
            .target_index = 0U,
            .interrupted = false};
        const ingress::
            RawEmergencyReserveReprovisionHooksV1
                hooks{.after = &InterruptAt,
                      .context = &context};
        auto interrupted =
            ingress::ReprovisionRawEmergencyReserveV1(
                std::move(harness.cleanup_receipt),
                request,
                &probe,
                &hooks);
        test->Expect(
            interrupted.error ==
                    ingress::
                        RawEmergencyReserveReprovisionErrorV1::
                            kInjectedInterruption &&
                context.interrupted &&
                !interrupted
                     .state_replace_may_have_occurred &&
                interrupted.retry_cleanup_receipt !=
                    nullptr &&
                interrupted.retry_cleanup_receipt
                    ->Validate(),
            "candidate-build interruption returns only a still-valid cleanup capability");
        auto retry =
            ingress::ReprovisionRawEmergencyReserveV1(
                std::move(
                    interrupted
                        .retry_cleanup_receipt),
                request,
                &probe);
        test->Expect(
            retry.ok() && retry.receipt->Validate(),
            "retry deletes the single partial UUID set and rebuilds it completely");
    }

    {
        ReprovisionHarness harness(test);
        if (harness.cleanup_receipt == nullptr) {
            return;
        }
        auto request = harness.Request(test, 0xd8U);
        ReprovisionCapacityProbe probe;
        InterruptContext context{
            .target =
                ingress::
                    RawEmergencyReserveReprovisionMutationPointV1::
                        kStateCandidateSynced,
            .target_index = 0U,
            .interrupted = false};
        const ingress::
            RawEmergencyReserveReprovisionHooksV1
                hooks{.after = &InterruptAt,
                      .context = &context};
        auto interrupted =
            ingress::ReprovisionRawEmergencyReserveV1(
                std::move(harness.cleanup_receipt),
                request,
                &probe,
                &hooks);
        test->Expect(
            interrupted.error ==
                    ingress::
                        RawEmergencyReserveReprovisionErrorV1::
                            kInjectedInterruption &&
                interrupted.retry_cleanup_receipt !=
                    nullptr &&
                !ExistsNoFollow(
                    harness.raw.root_fd.get(),
                    ingress::
                        kRawEmergencyReserveDataFilename),
            "complete inventory and matching state tmp remain unpublished while fixed data is absent");
        auto discovered =
            ingress::
                DiscoverRawEmergencyReserveReprovisionCandidateV1(
                    *interrupted
                         .retry_cleanup_receipt,
                    request.metadata_margin_bytes,
                    request.metadata_margin_inodes);
        test->Expect(
            discovered.ok() &&
                discovered
                        .candidate_reserve_state_uuid ==
                    request.bootstrap.header
                        .reserve_state_uuid &&
                discovered.request.bootstrap ==
                    request.bootstrap,
            "read-only restart discovery reconstructs the exact UUID/bootstrap from one durable unpublished set");
        auto retry =
            ingress::ReprovisionRawEmergencyReserveV1(
                std::move(
                    interrupted
                        .retry_cleanup_receipt),
                discovered.request,
                &probe);
        test->Expect(
            retry.ok() && retry.receipt->Validate(),
            "inventory-complete/data-unpublished retry removes the whole UUID set and restarts");
    }

    {
        ReprovisionHarness harness(test);
        if (harness.cleanup_receipt == nullptr) {
            return;
        }
        auto request = harness.Request(test, 0xd2U);
        ReprovisionCapacityProbe probe;
        InterruptContext context{
            .target =
                ingress::
                    RawEmergencyReserveReprovisionMutationPointV1::
                        kDataPublished,
            .target_index = 0U,
            .interrupted = false};
        const ingress::
            RawEmergencyReserveReprovisionHooksV1
                hooks{.after = &InterruptAt,
                      .context = &context};
        auto interrupted =
            ingress::ReprovisionRawEmergencyReserveV1(
                std::move(harness.cleanup_receipt),
                request,
                &probe,
                &hooks);
        test->Expect(
            interrupted.error ==
                    ingress::
                        RawEmergencyReserveReprovisionErrorV1::
                            kInjectedInterruption &&
                interrupted.retry_cleanup_receipt !=
                    nullptr &&
                ExistsNoFollow(
                    harness.raw.root_fd.get(),
                    ingress::
                        kRawEmergencyReserveDataFilename),
            "data-final crash window retains complete inventory and matching state candidate");
        auto retry =
            ingress::ReprovisionRawEmergencyReserveV1(
                std::move(
                    interrupted
                        .retry_cleanup_receipt),
                request,
                &probe);
        test->Expect(
            retry.ok() && retry.receipt->Validate(),
            "old state plus fixed data, complete inventory, and valid state tmp resumes with state replacement only");
    }
}

void TestPostReplaceFailStopAndNoNameGap(
    TestContext* test) {
    ReprovisionHarness harness(test);
    if (harness.cleanup_receipt == nullptr) {
        return;
    }
    auto request = harness.Request(test, 0xd3U);
    ReprovisionCapacityProbe probe;
    InterruptContext context{
        .target =
            ingress::
                RawEmergencyReserveReprovisionMutationPointV1::
                    kStateReplaced,
        .target_index = 0U,
        .interrupted = false};
    const ingress::
        RawEmergencyReserveReprovisionHooksV1
            hooks{.after = &InterruptAt,
                  .context = &context};

    std::atomic<bool> stop{false};
    std::atomic<std::uint64_t> missing{0U};
    std::thread observer([&]() {
        while (!stop.load(std::memory_order_relaxed)) {
            struct stat status {};
            if (::fstatat(
                    harness.raw.root_fd.get(),
                    ingress::kRawReserveStateFilename,
                    &status,
                    AT_SYMLINK_NOFOLLOW) != 0 &&
                errno == ENOENT) {
                static_cast<void>(
                    missing.fetch_add(
                        1U,
                        std::memory_order_relaxed));
            }
        }
    });
    auto interrupted =
        ingress::ReprovisionRawEmergencyReserveV1(
            std::move(harness.cleanup_receipt),
            request,
            &probe,
            &hooks);
    stop.store(true, std::memory_order_relaxed);
    observer.join();
    test->Expect(
        interrupted.error ==
                ingress::
                    RawEmergencyReserveReprovisionErrorV1::
                        kPostReplaceFailStop &&
            interrupted.state_replace_may_have_occurred &&
            interrupted.fail_stop_required &&
            interrupted.retry_cleanup_receipt == nullptr &&
            interrupted.receipt == nullptr &&
            context.interrupted && missing.load() == 0U,
        "post-replace interruption destroys old authority and atomic rename never exposes a state-name gap");

    ingress::RawReserveStatePosixError state_error{};
    auto attached =
        ingress::AttachRawReserveStateAtV1(
            harness.raw.root_fd.get(), &state_error);
    auto inventory =
        attached == nullptr
            ? nullptr
            : ingress::
                  AttachRawEmergencyReserveInventoryV1(
                      *attached, &probe);
    test->Expect(
        attached != nullptr &&
            attached->state() == request.bootstrap &&
            inventory != nullptr,
        "surviving post-replace disk state is a complete normal-attach PROVISIONED set");
}

void TestSigkillRestartConvergesThroughArchiveLoader(
    TestContext* test) {
    ReprovisionHarness harness(test);
    if (harness.cleanup_receipt == nullptr) {
        return;
    }
    auto request = harness.Request(test, 0xdcU);
    const pid_t builder = ::fork();
    if (builder == 0) {
        ReprovisionCapacityProbe probe;
        const ingress::
            RawEmergencyReserveReprovisionHooksV1
                hooks{
                    .after =
                        &KillProcessAtStateCandidate,
                    .context = nullptr};
        static_cast<void>(
            ingress::ReprovisionRawEmergencyReserveV1(
                std::move(harness.cleanup_receipt),
                request,
                &probe,
                &hooks));
        ::_exit(91);
    }
    int builder_status = 0;
    test->Expect(
        builder > 0 &&
            ::waitpid(
                builder, &builder_status, 0) == builder &&
            WIFSIGNALED(builder_status) &&
            WTERMSIG(builder_status) == SIGKILL,
        "child is SIGKILLed after durable inventory/state-candidate barriers");

    // Drop the parent's fork-time copy.  A real next process has no
    // in-memory cleanup receipt; only the durable archive and old all-DONE
    // state remain.
    harness.cleanup_receipt.reset();
    const pid_t starter = ::fork();
    if (starter == 0) {
        ReprovisionCapacityProbe probe;
        ingress::RawReserveStatePosixError state_error{};
        auto state =
            ingress::AttachRawReserveStateAtV1(
                harness.raw.root_fd.get(),
                &state_error);
        ingress::RawEmergencyReservePosixErrorV1
            inventory_error{};
        auto inventory =
            state == nullptr
                ? nullptr
                : ingress::
                      AttachRawEmergencyReserveInventoryV1(
                          *state,
                          &probe,
                          &inventory_error);
        const bool old_consumed =
            state != nullptr &&
            state->state().selected_slot <
                state->state().slots.size() &&
            state->state()
                    .slots[state->state().selected_slot]
                    .coordinator_state ==
                ingress::
                    ReserveCoordinatorPhaseV1::kConsumed;
        if (!old_consumed || inventory != nullptr ||
            inventory_error !=
                ingress::
                    RawEmergencyReservePosixErrorV1::
                        kCandidateConflict) {
            ::_exit(92);
        }
        const auto& selected =
            state->state()
                .slots[state->state().selected_slot];
        auto loaded =
            ingress::
                LoadPublishedFinalizationArchiveCapabilityV1At(
                    harness.audit.descriptor.get(),
                    state->state().header,
                    selected);
        if (!loaded.ok()) {
            ::_exit(93);
        }
        auto republished =
            ingress::PublishFinalizationArchiveV1At(
                harness.audit.descriptor.get(),
                *loaded.archive);
        if (!republished.ok()) {
            ::_exit(94);
        }
        auto cleanup =
            ingress::
                CleanupFinalizationArchiveSourceReportsV1At(
                    harness.raw.root_fd.get(),
                    std::move(republished.receipt));
        if (!cleanup.ok()) {
            ::_exit(95);
        }
        auto discovered =
            ingress::
                DiscoverRawEmergencyReserveReprovisionCandidateV1(
                    *cleanup.receipt,
                    request.metadata_margin_bytes,
                    request.metadata_margin_inodes);
        if (!discovered.ok() ||
            discovered.candidate_reserve_state_uuid !=
                request.bootstrap.header
                    .reserve_state_uuid ||
            discovered.request.bootstrap !=
                request.bootstrap) {
            ::_exit(96);
        }
        auto completed =
            ingress::ReprovisionRawEmergencyReserveV1(
                std::move(cleanup.receipt),
                discovered.request,
                &probe);
        ::_exit(
            completed.ok() &&
                    completed.receipt->Validate()
                ? 0
                : 97);
    }
    int starter_status = 0;
    test->Expect(
        starter > 0 &&
            ::waitpid(
                starter, &starter_status, 0) == starter &&
            WIFEXITED(starter_status) &&
            WEXITSTATUS(starter_status) == 0,
        "next process state-first loads the final archive, re-establishes absent-source cleanup, discovers the sole durable UUID, and completes replacement");

    ReprovisionCapacityProbe parent_probe;
    ingress::RawReserveStatePosixError state_error{};
    auto attached =
        ingress::AttachRawReserveStateAtV1(
            harness.raw.root_fd.get(), &state_error);
    auto attached_inventory =
        attached == nullptr
            ? nullptr
            : ingress::
                  AttachRawEmergencyReserveInventoryV1(
                      *attached, &parent_probe);
    test->Expect(
        attached != nullptr &&
            attached->state() == request.bootstrap &&
            attached_inventory != nullptr,
        "parent observes the exact child-discovered PROVISIONED UUID and complete reserve after restart");
}

void TestCapacityAndCandidateAttacks(TestContext* test) {
    {
        ReprovisionHarness harness(test);
        if (harness.cleanup_receipt == nullptr) {
            return;
        }
        auto request = harness.Request(test, 0xd4U);
        ReprovisionCapacityProbe probe;
        probe.reject_quota_inode_proof = true;
        auto rejected =
            ingress::ReprovisionRawEmergencyReserveV1(
                std::move(harness.cleanup_receipt),
                request,
                &probe);
        test->Expect(
            rejected.error ==
                    ingress::
                        RawEmergencyReserveReprovisionErrorV1::
                            kCapacityProbeFailure &&
                rejected.retry_cleanup_receipt != nullptr &&
                !ExistsNoFollow(
                    harness.raw.root_fd.get(),
                    ingress::
                        kRawEmergencyReserveDataFilename),
            "missing quota-inode proof rejects before reserve allocation");
    }

    {
        ReprovisionHarness harness(test);
        if (harness.cleanup_receipt == nullptr) {
            return;
        }
        auto request = harness.Request(test, 0xd5U);
        ReprovisionCapacityProbe probe;
        const std::string expected_candidate =
            DataCandidateNameForTest(
                request.bootstrap.header
                    .reserve_state_uuid);
        test->Expect(
            ::symlinkat(
                ingress::kRawReserveStateFilename,
                harness.raw.root_fd.get(),
                expected_candidate.c_str()) == 0 &&
                Fsync(harness.raw.root_fd.get()),
            "install typed-name symlink attack");
        auto symlink =
            ingress::ReprovisionRawEmergencyReserveV1(
                std::move(harness.cleanup_receipt),
                request,
                &probe);
        struct stat status {};
        test->Expect(
            symlink.error ==
                    ingress::
                        RawEmergencyReserveReprovisionErrorV1::
                            kCandidateConflict &&
                symlink.retry_cleanup_receipt != nullptr &&
                ExistsNoFollow(
                    harness.raw.root_fd.get(),
                    expected_candidate,
                    &status) &&
                S_ISLNK(status.st_mode),
            "typed candidate symlink is never followed or deleted");
    }

    {
        ReprovisionHarness harness(test);
        if (harness.cleanup_receipt == nullptr) {
            return;
        }
        auto request = harness.Request(test, 0xd9U);
        ReprovisionCapacityProbe probe;
        const std::string expected_candidate =
            DataCandidateNameForTest(
                request.bootstrap.header
                    .reserve_state_uuid);
        const std::string hardlink_source =
            "reprovision-hardlink-source";
        test->Expect(
            CreateFileAt(
                harness.raw.root_fd.get(),
                hardlink_source,
                "hardlink") &&
                ::linkat(
                    harness.raw.root_fd.get(),
                    hardlink_source.c_str(),
                    harness.raw.root_fd.get(),
                    expected_candidate.c_str(),
                    0) == 0 &&
                Fsync(harness.raw.root_fd.get()),
            "install typed-name hardlink attack");
        auto hardlink =
            ingress::ReprovisionRawEmergencyReserveV1(
                std::move(harness.cleanup_receipt),
                request,
                &probe);
        struct stat status {};
        test->Expect(
            hardlink.error ==
                    ingress::
                        RawEmergencyReserveReprovisionErrorV1::
                            kCandidateConflict &&
                hardlink.retry_cleanup_receipt !=
                    nullptr &&
                ExistsNoFollow(
                    harness.raw.root_fd.get(),
                    expected_candidate,
                    &status) &&
                status.st_nlink ==
                    static_cast<nlink_t>(2),
            "hardlinked typed candidate fails closed and is not deleted");
    }

    {
        ReprovisionHarness harness(test);
        if (harness.cleanup_receipt == nullptr) {
            return;
        }
        auto request = harness.Request(test, 0xdaU);
        ReprovisionCapacityProbe probe;
        const std::string expected_candidate =
            DataCandidateNameForTest(
                request.bootstrap.header
                    .reserve_state_uuid);
        test->Expect(
            ::mkdirat(
                harness.raw.root_fd.get(),
                expected_candidate.c_str(),
                0700U) == 0 &&
                Fsync(harness.raw.root_fd.get()),
            "install typed-name wrong-object-type attack");
        auto wrong_type =
            ingress::ReprovisionRawEmergencyReserveV1(
                std::move(harness.cleanup_receipt),
                request,
                &probe);
        struct stat status {};
        test->Expect(
            wrong_type.error ==
                    ingress::
                        RawEmergencyReserveReprovisionErrorV1::
                            kCandidateConflict &&
                wrong_type.retry_cleanup_receipt !=
                    nullptr &&
                ExistsNoFollow(
                    harness.raw.root_fd.get(),
                    expected_candidate,
                    &status) &&
                S_ISDIR(status.st_mode),
            "wrong-type typed candidate fails closed and is not removed");
    }

    {
        ReprovisionHarness harness(test);
        if (harness.cleanup_receipt == nullptr) {
            return;
        }
        auto request = harness.Request(test, 0xd6U);
        ReprovisionCapacityProbe probe;
        const std::string other_candidate =
            DataCandidateNameForTest(
                Pattern<16U>(0xe6U));
        const std::string requested_candidate =
            DataCandidateNameForTest(
                request.bootstrap.header
                    .reserve_state_uuid);
        test->Expect(
            CreateFileAt(
                harness.raw.root_fd.get(),
                other_candidate,
                "foreign") &&
                CreateFileAt(
                    harness.raw.root_fd.get(),
                    requested_candidate,
                    "requested") &&
                Fsync(harness.raw.root_fd.get()),
            "install a second UUID candidate set");
        auto multiple =
            ingress::
                DiscoverRawEmergencyReserveReprovisionCandidateV1(
                    *harness.cleanup_receipt,
                    request.metadata_margin_bytes,
                    request.metadata_margin_inodes);
        test->Expect(
            multiple.error ==
                    ingress::
                        RawEmergencyReserveReprovisionErrorV1::
                            kCandidateConflict &&
                !multiple.ok(),
            "restart discovery rejects multiple UUID sets before choosing a bootstrap");
        auto reprovision_multiple =
            ingress::ReprovisionRawEmergencyReserveV1(
                std::move(harness.cleanup_receipt),
                request,
                &probe);
        test->Expect(
            reprovision_multiple.error ==
                    ingress::
                        RawEmergencyReserveReprovisionErrorV1::
                            kCandidateConflict &&
                reprovision_multiple
                        .retry_cleanup_receipt !=
                    nullptr &&
                ExistsNoFollow(
                    harness.raw.root_fd.get(),
                    other_candidate),
            "multiple UUID candidate sets fail closed without merging or deletion");
    }
}

void TestPublishedDataIncompleteInventoryIsFatal(
    TestContext* test) {
    ReprovisionHarness harness(test);
    if (harness.cleanup_receipt == nullptr) {
        return;
    }
    auto request = harness.Request(test, 0xd7U);
    ReprovisionCapacityProbe probe;
    InterruptContext context{
        .target =
            ingress::
                RawEmergencyReserveReprovisionMutationPointV1::
                    kDataPublished,
        .target_index = 0U,
        .interrupted = false};
    const ingress::
        RawEmergencyReserveReprovisionHooksV1
            hooks{.after = &InterruptAt,
                  .context = &context};
    auto interrupted =
        ingress::ReprovisionRawEmergencyReserveV1(
            std::move(harness.cleanup_receipt),
            request,
            &probe,
            &hooks);
    std::string final_name;
    test->Expect(
        interrupted.retry_cleanup_receipt != nullptr &&
            ingress::FormatReserveInodeFilenameV1(
                request.bootstrap.header
                    .reserve_state_uuid,
                7U,
                &final_name) ==
                ingress::ReserveHeaderV1Error::kNone &&
            ::unlinkat(
                harness.raw.root_fd.get(),
                (std::string(
                     ingress::
                         kRawEmergencyReserveInodesDirectory) +
                 "/" + final_name)
                    .c_str(),
                0) == 0,
        "simulate impossible fixed-data plus incomplete inventory crash image");
    auto fatal =
        ingress::ReprovisionRawEmergencyReserveV1(
            std::move(
                interrupted.retry_cleanup_receipt),
            request,
            &probe);
    test->Expect(
        fatal.error ==
                ingress::
                    RawEmergencyReserveReprovisionErrorV1::
                        kInventoryConflict &&
            fatal.retry_cleanup_receipt != nullptr &&
            ExistsNoFollow(
                harness.raw.root_fd.get(),
                ingress::
                    kRawEmergencyReserveDataFilename),
        "fixed data plus incomplete inventory is fatal and is never repaired by mixing artifacts");
}

}  // namespace

int main() {
    static_assert(
        !std::is_default_constructible_v<
            ingress::
                RawEmergencyReserveReprovisionReceiptV1>);
    static_assert(
        !std::is_copy_constructible_v<
            ingress::
                RawEmergencyReserveReprovisionReceiptV1>);
    static_assert(
        !std::is_move_constructible_v<
            ingress::
                RawEmergencyReserveReprovisionReceiptV1>);

    TestContext test;
    // Keep the embedded cleanup capability producer covered in this
    // translation unit too; this also prevents its security cases from
    // becoming unused fixture code.
    TestCleanupAndAbsentAdoption(&test);
    TestFaultWindowAndReappearance(&test);
    TestScaffoldingAuditRootCleanup(&test);
    TestMaintenanceAuthorization(&test);
    TestConflictsFailClosed(&test);
    TestNormalAtomicReplacement(&test);
    TestBootstrapAndCapabilityGates(&test);
    TestByteIdenticalOldStateInodeAttack(&test);
    TestInterruptedBuildAndPublishedDataRetry(&test);
    TestPostReplaceFailStopAndNoNameGap(&test);
    TestSigkillRestartConvergesThroughArchiveLoader(&test);
    TestCapacityAndCandidateAttacks(&test);
    TestPublishedDataIncompleteInventoryIsFatal(&test);
    if (test.failures != 0) {
        std::cerr
            << test.failures
            << " raw emergency reserve reprovision assertion(s) failed\n";
        return 1;
    }
    std::cout
        << "raw emergency reserve reprovision tests passed\n";
    return 0;
}
