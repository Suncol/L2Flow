#include "l2flow/ingress/raw_reserve_coordinator.h"
#include "l2flow/ingress/raw_reserve_authorized_wal.h"
#include "l2flow/ingress/raw_recovery_authorized.h"

#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace ingress = l2flow::ingress;

namespace {

struct TestContext final {
    void Expect(bool condition, std::string_view description) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << description << '\n';
        }
    }
    int failures = 0;
};

class TempDirectory final {
public:
    TempDirectory() {
        char pattern[] =
            "/tmp/l2flow-reserve-coordinator-XXXXXX";
        char* const created = ::mkdtemp(pattern);
        if (created == nullptr) {
            return;
        }
        path_ = created;
        fd_ = ::open(
            path_.c_str(),
            O_RDONLY | O_DIRECTORY |
                O_NOFOLLOW | O_CLOEXEC);
    }

    ~TempDirectory() {
        if (fd_ >= 0) {
            static_cast<void>(::close(fd_));
        }
        if (!path_.empty()) {
            std::error_code ignored;
            static_cast<void>(
                std::filesystem::remove_all(
                    path_, ignored));
        }
    }

    [[nodiscard]] int fd() const noexcept {
        return fd_;
    }
    [[nodiscard]] const std::string& path()
        const noexcept {
        return path_;
    }

private:
    std::string path_;
    int fd_ = -1;
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
                    seed + index));
    }
    return result;
}

ingress::ReserveCoordinatorStateV1 MakeBootstrap(
    int directory_fd) {
    struct stat status {};
    if (::fstat(directory_fd, &status) != 0) {
        return {};
    }
    ingress::ReserveCoordinatorStateV1 state;
    state.header.reserve_state_uuid =
        Pattern<16U>(0x10U);
    state.header.quota_identity_sha256 =
        Pattern<32U>(0x30U);
    state.header.mount_identity_sha256 =
        Pattern<32U>(0x50U);
    state.header.device_id =
        static_cast<std::uint64_t>(status.st_dev);
    state.header.declared_releasable_bytes =
        1U << 20U;
    state.header.allocation_quantum_bytes = 4096U;
    state.header.declared_inode_reserve_count = 64U;
    state.header.byte_probe_version = 1U;
    state.header.inode_probe_version = 1U;
    state.header.inode_inventory_sha256 =
        Pattern<32U>(0x70U);
    state.header.safe_stop_catalog_sha256 =
        Pattern<32U>(0x90U);
    ingress::ReserveStateSlotV1 slot;
    slot.coordinator_state =
        ingress::ReserveCoordinatorPhaseV1::
            kProvisioned;
    slot.generation = 1U;
    slot.reserve_state_uuid =
        state.header.reserve_state_uuid;
    state.slots[0U] = slot;
    state.slots[1U] = slot;
    state.selected_slot = 0U;
    return state;
}

ingress::RawReserveCoordinatorLeaseMarkerV1
MakeMarker(
    const ingress::ReserveCoordinatorStateV1&
        state) {
    ingress::RawReserveCoordinatorLeaseMarkerV1 marker;
    marker.coordinator_identity =
        Pattern<16U>(0x05U);
    marker.device_id = state.header.device_id;
    marker.quota_identity_sha256 =
        state.header.quota_identity_sha256;
    marker.mount_identity_sha256 =
        state.header.mount_identity_sha256;
    return marker;
}

ingress::RawReserveRegistryEntryKeyV1 MakeKey() {
    ingress::RawReserveRegistryEntryKeyV1 key;
    key.route.source_stream_id = 2002U;
    key.route.capture_date = 20260718U;
    key.stream_day_id = Pattern<16U>(0xb0U);
    key.recovery_attempt_id =
        Pattern<16U>(0xd0U);
    return key;
}

class TargetBinding final {
public:
    TargetBinding(
        int route_directory_fd,
        std::uint32_t source_stream_id,
        std::uint32_t capture_date,
        std::string stream_slug)
        : route_directory_fd_(
              std::make_shared<std::atomic<int>>(
                  route_directory_fd)),
          source_stream_id_(source_stream_id),
          capture_date_(capture_date),
          stream_slug_(std::move(stream_slug)) {}

    [[nodiscard]] int descriptor()
        const noexcept {
        return route_directory_fd_->load(
            std::memory_order_acquire);
    }
    void SetDescriptor(int descriptor) const noexcept {
        route_directory_fd_->store(
            descriptor,
            std::memory_order_release);
    }

private:
    std::shared_ptr<std::atomic<int>>
        route_directory_fd_;
    std::uint32_t source_stream_id_ = 0U;
    std::uint32_t capture_date_ = 0U;
    std::string stream_slug_;
};

class RecordingRecoveryIo final
    : public ingress::RawRecoveryIo,
      public ingress::
          RawReserveMutationTargetProviderV1 {
public:
    explicit RecordingRecoveryIo(
        TargetBinding target)
        : target_(std::move(target)) {}

    bool DependentArtifactsAbsentProven()
        const noexcept override {
        return true;
    }
    bool R11OrphanAdoptionAuthorized(
        const ingress::RawRecoveryPlanV1&)
        const noexcept override {
        return false;
    }
    int SyncParentDirectories() noexcept override {
        ++calls;
        return 0;
    }
    int TruncateJournal(
        std::uint64_t) noexcept override {
        ++calls;
        return 0;
    }
    int SyncJournal() noexcept override {
        ++calls;
        return 0;
    }
    int TruncateSegment(
        std::uint32_t,
        std::uint64_t) noexcept override {
        ++calls;
        return 0;
    }
    int SyncSegment(
        std::uint32_t,
        bool) noexcept override {
        ++calls;
        return 0;
    }
    ingress::RawRecoveryWriteResult
    WriteJournalSome(
        std::uint64_t,
        std::span<const std::byte> bytes)
        noexcept override {
        ++calls;
        return {bytes.size(), 0};
    }
    int
    RawReserveMutationTargetDirectoryDescriptorV1()
        const noexcept override {
        return target_.descriptor();
    }

    std::uint64_t calls = 0U;

private:
    TargetBinding target_;
};

struct RecordingWalState final {
    std::uint64_t writes = 0U;
    std::uint64_t syncs = 0U;
    std::uint64_t truncates = 0U;
    std::uint64_t closes = 0U;
    std::uint64_t backend_calls = 0U;
};

class RecordingWalIo final
    : public ingress::RawWalIo,
      public ingress::
          RawReserveMutationTargetProviderV1 {
public:
    explicit RecordingWalIo(
        std::shared_ptr<RecordingWalState> state,
        TargetBinding target)
        : state_(std::move(state)),
          target_(std::move(target)) {}

    ingress::RawWalWriteResult WritevSome(
        ingress::RawWalFile,
        std::uint64_t,
        std::span<const ingress::RawWalIoVector>
            vectors) noexcept override {
        std::size_t bytes = 0U;
        for (const auto& vector : vectors) {
            bytes += vector.bytes.size();
        }
        ++state_->writes;
        return {bytes, 0};
    }
    int Fdatasync(
        ingress::RawWalFile) noexcept override {
        ++state_->syncs;
        return 0;
    }
    int Truncate(
        ingress::RawWalFile,
        std::uint64_t) noexcept override {
        ++state_->truncates;
        return 0;
    }
    int Close(
        ingress::RawWalFile) noexcept override {
        ++state_->closes;
        return 0;
    }
    int
    RawReserveMutationTargetDirectoryDescriptorV1()
        const noexcept override {
        return target_.descriptor();
    }

private:
    std::shared_ptr<RecordingWalState> state_;
    TargetBinding target_;
};

class UnboundRecordingWalIo final
    : public ingress::RawWalIo {
public:
    explicit UnboundRecordingWalIo(
        std::shared_ptr<RecordingWalState> state)
        : state_(std::move(state)) {}

    ingress::RawWalWriteResult WritevSome(
        ingress::RawWalFile,
        std::uint64_t,
        std::span<const ingress::RawWalIoVector>
            vectors) noexcept override {
        std::size_t bytes = 0U;
        for (const auto& vector : vectors) {
            bytes += vector.bytes.size();
        }
        ++state_->writes;
        return {bytes, 0};
    }
    int Fdatasync(
        ingress::RawWalFile) noexcept override {
        ++state_->syncs;
        return 0;
    }
    int Truncate(
        ingress::RawWalFile,
        std::uint64_t) noexcept override {
        ++state_->truncates;
        return 0;
    }
    int Close(
        ingress::RawWalFile) noexcept override {
        ++state_->closes;
        return 0;
    }

private:
    std::shared_ptr<RecordingWalState> state_;
};

class RecordingWalBackend final
    : public ingress::RawWalStreamBackendV1,
      public ingress::
          RawReserveMutationTargetProviderV1 {
public:
    explicit RecordingWalBackend(
        std::shared_ptr<RecordingWalState> state,
        TargetBinding target)
        : state_(std::move(state)),
          target_(std::move(target)) {}

    bool PublishClosedSegment(
        ingress::RawSegmentArtifactPlanV1,
        const ingress::RawWalWriterSnapshot&)
        noexcept override {
        ++state_->backend_calls;
        return true;
    }
    bool CreateNextSegment(
        const ingress::RawWalRotationPlan&,
        std::uint64_t,
        ingress::RawWalNextSegmentBootstrapV1*
            bootstrap) noexcept override {
        ++state_->backend_calls;
        if (bootstrap == nullptr ||
            bootstrap->io != nullptr) {
            return false;
        }
        bootstrap->io =
            std::make_unique<RecordingWalIo>(
                state_,
                target_);
        return true;
    }
    bool PublishOpenManifest(
        const ingress::SegmentHeaderV1&,
        const ingress::RawWalWriterSnapshot&)
        noexcept override {
        ++state_->backend_calls;
        return true;
    }
    bool PublishControl(
        const ingress::SegmentHeaderV1&,
        const ingress::RawWalWriterSnapshot&)
        noexcept override {
        ++state_->backend_calls;
        return true;
    }
    int
    RawReserveMutationTargetDirectoryDescriptorV1()
        const noexcept override {
        return target_.descriptor();
    }

private:
    std::shared_ptr<RecordingWalState> state_;
    TargetBinding target_;
};

void TestDurableRegistryAndActionGates(
    TestContext* test) {
    TempDirectory directory;
    test->Expect(
        directory.fd() >= 0,
        "private coordinator root opens");
    if (directory.fd() < 0) {
        return;
    }
    const auto bootstrap =
        MakeBootstrap(directory.fd());
    const auto marker = MakeMarker(bootstrap);
    ingress::RawReserveCoordinatorErrorV1 failure =
        ingress::RawReserveCoordinatorErrorV1::kNone;
    std::string error;
    auto coordinator =
        ingress::
            PublishFreshRawReserveRegistryCoordinatorAtV1(
                directory.fd(),
                marker,
                bootstrap,
                &failure,
                &error);
    test->Expect(
        coordinator != nullptr,
        "fresh lease/state composition publishes");
    if (coordinator == nullptr) {
        std::cerr << error << '\n';
        return;
    }

    const auto key = MakeKey();
    ingress::RawReserveFreshScaffoldingV1 fresh;
    fresh.key = key;
    fresh.writer_instance = Pattern<16U>(0xf0U);
    fresh.recovery_intent =
        ingress::ReserveRecoveryIntentV1::
            kResumeConnect;
    fresh.scaffolding_allocation_cap = 65536U;
    fresh.safe_stop_template_id = 7U;
    test->Expect(
        coordinator->RegisterFreshScaffolding(
            fresh, &error) ==
                ingress::RawReserveCoordinatorErrorV1::
                    kNone &&
            coordinator->state()
                    .slots[
                        coordinator->state()
                            .selected_slot]
                    .generation == 2U,
        "fresh route durably enters SCAFFOLDING generation 2");

    auto scaffolding_action =
        coordinator->AcquireAction(
            key,
            ingress::ReserveRegistryStatusV1::
                kScaffolding,
            &failure,
            &error);
    test->Expect(
        scaffolding_action != nullptr &&
            scaffolding_action->ValidateLatest(),
        "shared OFD action binds exact scaffolding state");
    ingress::RawFreshStateAuthorizationV1 facts;
    facts.source_stream_id =
        key.route.source_stream_id;
    facts.capture_date = key.route.capture_date;
    facts.stream_day_id = key.stream_day_id;
    facts.recovery_attempt =
        key.recovery_attempt_id;
    facts.registry_stage =
        ingress::RawFreshRegistryStageV1::
            kScaffolding;
    facts.durable_state_generation = 2U;
    test->Expect(
        scaffolding_action != nullptr &&
            scaffolding_action->Authorizes(facts),
        "fresh namespace seam accepts exact durable scaffolding facts");
    auto stream_directory =
        ingress::
            OpenOrCreateAuthorizedFreshRawStreamDirectory(
                directory.path(),
                key.route.source_stream_id,
                key.route.capture_date,
                "sz-tick",
                facts,
                *scaffolding_action,
                &error);
    test->Expect(
        stream_directory != nullptr,
        "authorized SCAFFOLDING action durably creates route directories");
    ++facts.durable_state_generation;
    test->Expect(
        scaffolding_action != nullptr &&
            !scaffolding_action->Authorizes(facts),
        "fresh namespace seam rejects invented generation");
    auto rejected_directory =
        ingress::
            OpenOrCreateAuthorizedFreshRawStreamDirectory(
                directory.path(),
                key.route.source_stream_id,
                key.route.capture_date,
                "second-route",
                facts,
                *scaffolding_action,
                &error);
    test->Expect(
        rejected_directory == nullptr,
        "wrong generation is rejected before namespace mutation");
    scaffolding_action.reset();

    test->Expect(
        coordinator->PublishInit(key, &error) ==
                ingress::RawReserveCoordinatorErrorV1::
                    kNone,
        "journal-anchor barrier advances SCAFFOLDING to INIT");
    auto init_action = coordinator->AcquireAction(
        key,
        ingress::ReserveRegistryStatusV1::kInit,
        &failure,
        &error);
    facts.registry_stage =
        ingress::RawFreshRegistryStageV1::kInit;
    facts.durable_state_generation = 3U;
    test->Expect(
        init_action != nullptr &&
            init_action->Authorizes(facts),
        "initial segment action binds higher INIT generation");
    init_action.reset();

    test->Expect(
        coordinator->PublishActive(key, &error) ==
                ingress::RawReserveCoordinatorErrorV1::
                    kNone,
        "initialized route enters ACTIVE only through codec FSM");

    auto active_target_action =
        coordinator->AcquireActionForExistingRoute(
            key,
            ingress::ReserveRegistryStatusV1::kActive,
            "sz-tick",
            &failure,
            &error);
    test->Expect(
        active_target_action != nullptr &&
            active_target_action->target() != nullptr &&
            active_target_action->target()
                    ->source_stream_id() ==
                key.route.source_stream_id &&
            active_target_action->target()
                    ->capture_date() ==
                key.route.capture_date &&
            active_target_action->target()
                    ->stream_slug() == "sz-tick" &&
            active_target_action->target()
                    ->raw_root_device() != 0U &&
            active_target_action->target()
                    ->raw_root_inode() != 0U &&
            active_target_action->target()
                    ->route_device() != 0U &&
            active_target_action->target()
                    ->route_inode() != 0U &&
            active_target_action->target()
                    ->mount_identity_sha256() ==
                marker.mount_identity_sha256,
        "existing action carries canonical root/route target identity");
    active_target_action.reset();

    const auto wal_state =
        std::make_shared<RecordingWalState>();
    const TargetBinding correct_target(
        stream_directory->descriptor(),
        key.route.source_stream_id,
        key.route.capture_date,
        "sz-tick");

    TempDirectory wrong_root;
    auto wrong_stream_directory =
        ingress::OpenOrCreateRawStreamDirectory(
            wrong_root.path(),
            key.route.source_stream_id,
            key.route.capture_date,
            "sz-tick",
            &error);
    test->Expect(
        wrong_stream_directory != nullptr,
        "wrong-root target fixture is available");
    const TargetBinding wrong_target(
        wrong_stream_directory == nullptr
            ? -1
            : wrong_stream_directory->descriptor(),
        key.route.source_stream_id,
        key.route.capture_date,
        "sz-tick");
    auto unbound_gated_io =
        ingress::GateRawWalIoWithCoordinatorV1(
            std::make_unique<
                UnboundRecordingWalIo>(wal_state),
            *coordinator,
            key,
            ingress::ReserveRegistryStatusV1::kActive,
            "sz-tick",
            &error);
    test->Expect(
        unbound_gated_io == nullptr &&
            wal_state->writes == 0U &&
            wal_state->syncs == 0U &&
            wal_state->truncates == 0U,
        "generic WAL delegate without retained target proof is rejected");

    auto wrong_gated_io =
        ingress::GateRawWalIoWithCoordinatorV1(
            std::make_unique<RecordingWalIo>(
                wal_state, wrong_target),
            *coordinator,
            key,
            ingress::ReserveRegistryStatusV1::kActive,
            "sz-tick",
            &error);
    auto wrong_gated_backend =
        ingress::
            GateRawWalStreamBackendWithCoordinatorV1(
                std::make_unique<
                    RecordingWalBackend>(
                    wal_state, wrong_target),
                *coordinator,
                key,
                ingress::ReserveRegistryStatusV1::
                    kActive,
                "sz-tick",
                &error);
    test->Expect(
        wrong_gated_io == nullptr &&
            wrong_gated_backend == nullptr &&
            wal_state->writes == 0U &&
            wal_state->syncs == 0U &&
            wal_state->truncates == 0U &&
            wal_state->backend_calls == 0U,
        "wrong-root WAL delegates are rejected before any delegate mutation");

    TargetBinding switching_target(
        stream_directory->descriptor(),
        key.route.source_stream_id,
        key.route.capture_date,
        "sz-tick");
    auto switching_io =
        ingress::GateRawWalIoWithCoordinatorV1(
            std::make_unique<RecordingWalIo>(
                wal_state, switching_target),
            *coordinator,
            key,
            ingress::ReserveRegistryStatusV1::kActive,
            "sz-tick",
            &error);
    auto switching_backend =
        ingress::
            GateRawWalStreamBackendWithCoordinatorV1(
                std::make_unique<
                    RecordingWalBackend>(
                    wal_state, switching_target),
                *coordinator,
                key,
                ingress::ReserveRegistryStatusV1::
                    kActive,
                "sz-tick",
                &error);
    switching_target.SetDescriptor(
        wrong_stream_directory == nullptr
            ? -1
            : wrong_stream_directory->descriptor());
    const std::uint64_t calls_before_switched_target =
        wal_state->syncs +
        wal_state->backend_calls;
    test->Expect(
            switching_io != nullptr &&
            switching_backend != nullptr &&
            switching_io->Fdatasync(
                ingress::RawWalFile::kSegment) ==
                EPERM &&
            !switching_backend->PublishControl(
                {}, {}) &&
            switching_backend->failure() ==
                ingress::
                    RawReserveAuthorizedWalFailureV1::
                        kTargetMismatch &&
            wal_state->syncs +
                    wal_state->backend_calls ==
                calls_before_switched_target,
        "WAL wrappers recheck the delegate target before every mutation");
    if (switching_io != nullptr) {
        static_cast<void>(
            switching_io->Close(
                ingress::RawWalFile::kSegment));
    }

    auto gated_io =
        ingress::GateRawWalIoWithCoordinatorV1(
            std::make_unique<RecordingWalIo>(
                wal_state, correct_target),
            *coordinator,
            key,
            ingress::ReserveRegistryStatusV1::kActive,
            "sz-tick",
            &error);
    auto gated_backend =
        ingress::
            GateRawWalStreamBackendWithCoordinatorV1(
                std::make_unique<
                    RecordingWalBackend>(
                    wal_state, correct_target),
                *coordinator,
                key,
                ingress::ReserveRegistryStatusV1::
                    kActive,
                "sz-tick",
                &error);
    const std::array<std::byte, 1U> one{
        std::byte{1U}};
    const ingress::RawWalIoVector vector{one};
    ingress::RawWalNextSegmentBootstrapV1
        next_bootstrap{};
    test->Expect(
        gated_io != nullptr &&
            gated_backend != nullptr &&
            gated_io
                    ->WritevSome(
                        ingress::RawWalFile::kSegment,
                        0U,
                        std::span<
                            const ingress::
                                RawWalIoVector>(
                            &vector, 1U))
                    .bytes_written == 1U &&
            gated_io->Fdatasync(
                ingress::RawWalFile::kSegment) == 0 &&
            gated_backend->CreateNextSegment(
                {}, 1U, &next_bootstrap) &&
            next_bootstrap.io != nullptr &&
            next_bootstrap.io->Fdatasync(
                ingress::RawWalFile::kJournal) == 0,
        "ACTIVE WAL syscalls and backend transactions acquire short-lived shared gates");

    ingress::RawReserveActiveTakeoverV1 takeover;
    takeover.old_key = key;
    takeover.new_writer_instance =
        Pattern<16U>(0x22U);
    takeover.new_recovery_attempt_id =
        Pattern<16U>(0x44U);
    takeover.recovery_intent =
        ingress::ReserveRecoveryIntentV1::
            kResumeConnect;
    test->Expect(
        coordinator->TakeoverActive(
            takeover, &error) ==
                ingress::RawReserveCoordinatorErrorV1::
                    kNone,
        "ACTIVE restart durably fences old writer into RECOVERING");
    const std::uint64_t calls_before_fenced_io =
        wal_state->syncs;
    test->Expect(
        gated_io->Fdatasync(
            ingress::RawWalFile::kSegment) ==
                ESTALE &&
            next_bootstrap.io->Fdatasync(
                ingress::RawWalFile::kJournal) ==
                ESTALE &&
            wal_state->syncs == calls_before_fenced_io &&
            gated_io->Close(
                ingress::RawWalFile::kSegment) == 0,
        "ACTIVE takeover fences old and rotated WAL I/O while still permitting close");
    test->Expect(
        !gated_backend->PublishControl(
            {}, {}) &&
            gated_backend->failure() ==
                ingress::
                    RawReserveAuthorizedWalFailureV1::
                        kActionGate,
        "ACTIVE takeover permanently fail-stops the old stream backend");
    auto recovering_key = key;
    recovering_key.recovery_attempt_id =
        takeover.new_recovery_attempt_id;

    auto writer_bound_io =
        ingress::
            GateRawWalIoWithCoordinatorForWriterV1(
                std::make_unique<RecordingWalIo>(
                    wal_state, correct_target),
                *coordinator,
                recovering_key,
                ingress::ReserveRegistryStatusV1::
                    kRecovering,
                "sz-tick",
                takeover.new_writer_instance,
                &error);
    auto writer_bound_backend =
        ingress::
            GateRawWalStreamBackendWithCoordinatorForWriterV1(
                std::make_unique<
                    RecordingWalBackend>(
                    wal_state, correct_target),
                *coordinator,
                recovering_key,
                ingress::ReserveRegistryStatusV1::
                    kRecovering,
                "sz-tick",
                takeover.new_writer_instance,
                &error);
    ingress::RawWalNextSegmentBootstrapV1
        writer_bound_bootstrap{};
    test->Expect(
        writer_bound_io != nullptr &&
            writer_bound_backend != nullptr &&
            writer_bound_io->Fdatasync(
                ingress::RawWalFile::kSegment) == 0 &&
            writer_bound_backend->CreateNextSegment(
                {},
                2U,
                &writer_bound_bootstrap) &&
            writer_bound_bootstrap.io != nullptr,
        "writer-bound RECOVERING wrappers admit the exact current writer");

    ingress::RawReserveWriterTakeoverV1
        same_status_takeover;
    same_status_takeover.key = recovering_key;
    same_status_takeover.new_writer_instance =
        Pattern<16U>(0x66U);
    test->Expect(
        coordinator->TakeoverRecovering(
            same_status_takeover,
            &error) ==
            ingress::RawReserveCoordinatorErrorV1::
                kNone,
        "same-status RECOVERING takeover changes the durable writer identity");
    const std::uint64_t calls_before_writer_fence =
        wal_state->syncs +
        wal_state->backend_calls;
    test->Expect(
        writer_bound_io != nullptr &&
            writer_bound_bootstrap.io != nullptr &&
            writer_bound_io->Fdatasync(
                ingress::RawWalFile::kSegment) ==
                ESTALE &&
            writer_bound_bootstrap.io->Fdatasync(
                ingress::RawWalFile::kJournal) ==
                ESTALE &&
            !writer_bound_backend->PublishControl(
                {},
                {}) &&
            writer_bound_backend->failure() ==
                ingress::
                    RawReserveAuthorizedWalFailureV1::
                        kWriterMismatch &&
            wal_state->syncs +
                    wal_state->backend_calls ==
                calls_before_writer_fence &&
            writer_bound_io->Close(
                ingress::RawWalFile::kSegment) == 0,
        "same-status takeover fences old writer and rotated I/O before delegate mutation while close remains unconditional");

    auto current_writer_io =
        ingress::
            GateRawWalIoWithCoordinatorForWriterV1(
                std::make_unique<RecordingWalIo>(
                    wal_state, correct_target),
                *coordinator,
                recovering_key,
                ingress::ReserveRegistryStatusV1::
                    kRecovering,
                "sz-tick",
                same_status_takeover
                    .new_writer_instance,
                &error);
    auto current_writer_backend =
        ingress::
            GateRawWalStreamBackendWithCoordinatorForWriterV1(
                std::make_unique<
                    RecordingWalBackend>(
                    wal_state, correct_target),
                *coordinator,
                recovering_key,
                ingress::ReserveRegistryStatusV1::
                    kRecovering,
                "sz-tick",
                same_status_takeover
                    .new_writer_instance,
                &error);
    test->Expect(
        current_writer_io != nullptr &&
            current_writer_backend != nullptr &&
            current_writer_io->Fdatasync(
                ingress::RawWalFile::kSegment) == 0 &&
            current_writer_backend->PublishControl(
                {},
                {}),
        "writer-bound I/O and backend admit the post-takeover current writer");
    if (current_writer_io != nullptr) {
        static_cast<void>(
            current_writer_io->Close(
                ingress::RawWalFile::kSegment));
    }
    writer_bound_bootstrap.io.reset();
    writer_bound_backend.reset();
    writer_bound_io.reset();
    current_writer_io.reset();
    current_writer_backend.reset();

    auto recovery_action =
        coordinator->AcquireActionForExistingRoute(
            recovering_key,
            ingress::ReserveRegistryStatusV1::
                kRecovering,
            "sz-tick",
            &failure,
            &error);
    test->Expect(
        recovery_action != nullptr &&
            recovery_action->ValidateLatest(),
        "RECOVERING filesystem action receives exact shared gate");

    RecordingRecoveryIo wrong_recovery(
        wrong_target);
    auto rejected_recovery =
        ingress::
            CreateRawCoordinatorAuthorizedRecoveryIoV1(
                wrong_recovery,
                *recovery_action,
                &error);
    test->Expect(
        rejected_recovery == nullptr &&
            wrong_recovery.calls == 0U,
        "wrong-root recovery delegate is rejected before any delegate mutation");

    TargetBinding switching_recovery_target(
        stream_directory->descriptor(),
        key.route.source_stream_id,
        key.route.capture_date,
        "sz-tick");
    RecordingRecoveryIo switching_recovery(
        switching_recovery_target);
    auto switched_authorized_recovery =
        ingress::
            CreateRawCoordinatorAuthorizedRecoveryIoV1(
                switching_recovery,
                *recovery_action,
                &error);
    switching_recovery_target.SetDescriptor(
        wrong_stream_directory == nullptr
            ? -1
            : wrong_stream_directory->descriptor());
    test->Expect(
        switched_authorized_recovery != nullptr &&
            switched_authorized_recovery
                    ->SyncJournal() == EPERM &&
            switching_recovery.calls == 0U,
        "recovery wrapper rechecks its retained target before every mutation");
    switched_authorized_recovery.reset();

    RecordingRecoveryIo bare_recovery(
        correct_target);
    auto authorized_recovery =
        ingress::
            CreateRawCoordinatorAuthorizedRecoveryIoV1(
                bare_recovery,
                *recovery_action,
                &error);
    ingress::RawRecoveryPlanV1 r11_plan;
    r11_plan.r11_orphan =
        ingress::RawRecoveryR11OrphanV1::
            kNormalHeaderOnly;
    r11_plan.journal_header.source_stream_id =
        recovering_key.route.source_stream_id;
    r11_plan.journal_header.capture_date =
        recovering_key.route.capture_date;
    r11_plan.journal_header.stream_day_id =
        recovering_key.stream_day_id;
    test->Expect(
        authorized_recovery != nullptr &&
            authorized_recovery
                ->R11OrphanAdoptionAuthorized(
                    r11_plan) &&
            authorized_recovery->SyncJournal() == 0 &&
            bare_recovery.calls == 1U,
        "normal R11 mutation is admitted only through exact RECOVERING token");
    r11_plan.r11_orphan =
        ingress::RawRecoveryR11OrphanV1::
            kFinalizationContinuationHeaderOnly;
    test->Expect(
        authorized_recovery != nullptr &&
            !authorized_recovery
             ->R11OrphanAdoptionAuthorized(
                 r11_plan),
        "normal coordinator action cannot authorize continuation R11");

    std::atomic<bool> transition_started{false};
    std::atomic<bool> transition_finished{false};
    ingress::RawReserveCoordinatorErrorV1
        transition_result =
            ingress::RawReserveCoordinatorErrorV1::
                kInvalidArgument;
    std::thread transition(
        [&]() {
            transition_started.store(
                true, std::memory_order_release);
            transition_result =
                coordinator->PublishActive(
                    recovering_key, &error);
            transition_finished.store(
                true, std::memory_order_release);
        });
    while (!transition_started.load(
        std::memory_order_acquire)) {
        std::this_thread::yield();
    }
    std::this_thread::sleep_for(
        std::chrono::milliseconds(20));
    test->Expect(
        !transition_finished.load(
            std::memory_order_acquire),
        "exclusive transition waits for live shared action OFD");
    authorized_recovery.reset();
    recovery_action.reset();
    transition.join();
    test->Expect(
        transition_result ==
                ingress::RawReserveCoordinatorErrorV1::
                    kRouteStatusMismatch,
        "receipt-free RECOVERING activation is rejected after the shared gate drains");

    auto active_action = coordinator->AcquireAction(
        recovering_key,
        ingress::ReserveRegistryStatusV1::kRecovering,
        &failure,
        &error);
    test->Expect(
        active_action != nullptr &&
            active_action->ValidateLatest(),
        "rejected bypass leaves the route durably RECOVERING");
    active_action.reset();

    const auto stale_path_state =
        std::make_shared<RecordingWalState>();
    auto stale_path_io =
        ingress::GateRawWalIoWithCoordinatorV1(
            std::make_unique<RecordingWalIo>(
                stale_path_state,
                correct_target),
            *coordinator,
            recovering_key,
            ingress::ReserveRegistryStatusV1::kRecovering,
            "sz-tick",
            &error);
    const std::string date_name =
        "capture_date=" +
        std::to_string(
            recovering_key.route.capture_date);
    const std::string route_name =
        "stream=" +
        std::to_string(
            recovering_key.route.source_stream_id) +
        "-sz-tick";
    const std::string moved_route_name =
        route_name + "-moved";
    const int date_fd = ::openat(
        directory.fd(),
        date_name.c_str(),
        O_RDONLY | O_DIRECTORY |
            O_NOFOLLOW | O_CLOEXEC);
    const bool route_replaced =
        date_fd >= 0 &&
        ::renameat(
            date_fd,
            route_name.c_str(),
            date_fd,
            moved_route_name.c_str()) == 0;
    if (date_fd >= 0) {
        static_cast<void>(::close(date_fd));
    }
    test->Expect(
        stale_path_io != nullptr &&
            route_replaced &&
            stale_path_io->Fdatasync(
                ingress::RawWalFile::kSegment) ==
                ESTALE &&
            stale_path_state->syncs == 0U,
        "canonical route pathname replacement returns ESTALE before delegate mutation");
    if (stale_path_io != nullptr) {
        static_cast<void>(
            stale_path_io->Close(
                ingress::RawWalFile::kSegment));
    }
    stale_path_io.reset();

    test->Expect(
        coordinator->UnregisterActive(
            nullptr, &error) ==
                ingress::RawReserveCoordinatorErrorV1::
                    kInvalidArgument,
        "bare ACTIVE unregister is impossible without a sealed-certificate receipt");
    const auto& final_state = coordinator->state();
    const auto& final_slot =
        final_state.slots[final_state.selected_slot];
    test->Expect(
        final_slot.generation == 6U &&
            final_slot.entry_count == 1U &&
            final_slot.entries[0U].registry_status ==
                ingress::ReserveRegistryStatusV1::kRecovering,
        "rejected bare unregister preserves the RECOVERING route");

    auto competing =
        ingress::AttachRawReserveRegistryCoordinatorAtV1(
            directory.fd(),
            marker,
            &failure,
            &error);
    test->Expect(
        competing == nullptr &&
            failure ==
                ingress::RawReserveCoordinatorErrorV1::
                    kLeaseFailure,
        "fixed flock prevents a second coordinator");
    next_bootstrap.io.reset();
    switching_io.reset();
    switching_backend.reset();
    gated_io.reset();
    gated_backend.reset();
    coordinator.reset();
    auto reattached =
        ingress::AttachRawReserveRegistryCoordinatorAtV1(
            directory.fd(),
            marker,
            &failure,
            &error);
    test->Expect(
        reattached != nullptr &&
            reattached->state()
                    .slots[
                        reattached->state()
                            .selected_slot]
                    .generation == 6U,
        "new coordinator securely reattaches final two-slot state");
}

}  // namespace

int main() {
    TestContext test;
    TestDurableRegistryAndActionGates(&test);
    if (test.failures != 0) {
        std::cerr << test.failures
                  << " Raw reserve coordinator test(s) failed\n";
        return 1;
    }
    std::cout << "Phase 2 Raw reserve coordinator tests passed\n";
    return 0;
}
