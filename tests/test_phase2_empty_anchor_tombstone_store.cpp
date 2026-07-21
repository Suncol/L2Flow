#include "l2flow/common/sha256.h"
#include "l2flow/ingress/empty_anchor_tombstone_store.h"
#include "l2flow/ingress/raw_namespace.h"
#include "l2flow/ingress/raw_recovery_maintenance_report_store.h"
#include "l2flow/ingress/raw_reserve_coordinator.h"
#include "l2flow/ingress/raw_schema.h"

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <utility>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace common = l2flow::common;
namespace ingress = l2flow::ingress;

namespace {

using PublishFunction = ingress::
    EmptyAnchorTombstonePublishResultV1 (*)(
        const ingress::RawWriterLease&,
        std::unique_ptr<
            ingress::RawReserveAuthorizedActionV1>,
        const ingress::BuiltEmptyAnchorTombstoneV1&,
        std::string*) noexcept;

static_assert(
    std::is_same_v<
        decltype(
            &ingress::PublishEmptyAnchorTombstoneV1),
        PublishFunction>);
static_assert(
    !std::is_default_constructible_v<
        ingress::EmptyAnchorTombstoneReceiptV1>);
static_assert(
    !std::is_copy_constructible_v<
        ingress::EmptyAnchorTombstoneReceiptV1>);
static_assert(
    !std::is_move_constructible_v<
        ingress::EmptyAnchorTombstoneReceiptV1>);

struct TestContext final {
    void Expect(
        bool condition,
        std::string_view description) {
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
            "/tmp/l2flow-empty-anchor-store-XXXXXX";
        char* const created = ::mkdtemp(pattern);
        if (created == nullptr) {
            return;
        }
        path_ = created;
        fd_ = ::open(
            path_.c_str(),
            O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
                O_CLOEXEC);
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

    TempDirectory(const TempDirectory&) = delete;
    TempDirectory& operator=(
        const TempDirectory&) = delete;

    [[nodiscard]] int fd() const noexcept {
        return fd_;
    }
    [[nodiscard]] const std::string&
    path() const noexcept {
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
                    seed +
                    static_cast<std::uint8_t>(
                        index)));
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
    state.header.declared_inode_reserve_count = 8192U;
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
MakeCoordinatorMarker(
    const ingress::ReserveCoordinatorStateV1&
        state) {
    ingress::RawReserveCoordinatorLeaseMarkerV1 marker;
    marker.coordinator_identity =
        state.header.reserve_state_uuid;
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

ingress::RawV1JournalHeaderWire MakeJournal(
    const ingress::RawReserveRegistryEntryKeyV1&
        key) {
    ingress::DurableJournalHeaderV1 journal{};
    journal.capture_date = key.route.capture_date;
    journal.source_stream_id =
        key.route.source_stream_id;
    journal.stream_day_id = key.stream_day_id;
    journal.raw_schema_sha256 =
        ingress::kFrozenRawSchemaSha256;
    journal.created_host_uuid =
        Pattern<16U>(0x21U);
    journal.created_linux_boot_id =
        Pattern<16U>(0x41U);
    journal.created_clock_epoch_algorithm = 1U;
    journal.created_clock_epoch_digest =
        Pattern<32U>(0x61U);
    journal.created_clock_epoch_label = 77U;
    ingress::RawV1JournalHeaderWire wire{};
    static_cast<void>(
        ingress::EncodeDurableJournalHeaderV1(
            journal, &wire));
    return wire;
}

[[nodiscard]] bool WriteBytesAt(
    int directory_fd,
    const std::string& name,
    std::span<const std::byte> bytes,
    bool sync_file = true) {
    const int fd = ::openat(
        directory_fd,
        name.c_str(),
        O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW |
            O_CLOEXEC,
        0600);
    if (fd < 0) {
        return false;
    }
    bool ok = ::fchmod(fd, 0600) == 0;
    std::size_t completed = 0U;
    while (ok && completed < bytes.size()) {
        const ssize_t written = ::pwrite(
            fd,
            bytes.data() + completed,
            bytes.size() - completed,
            static_cast<off_t>(completed));
        if (written < 0 && errno == EINTR) {
            continue;
        }
        if (written <= 0) {
            ok = false;
            break;
        }
        completed += static_cast<std::size_t>(written);
    }
    if (ok && sync_file) {
        ok = ::fsync(fd) == 0;
    }
    return ::close(fd) == 0 && ok;
}

[[nodiscard]] bool WriteFileAt(
    int directory_fd,
    const std::string& name,
    std::string_view bytes,
    bool sync_file = true) {
    return WriteBytesAt(
        directory_fd,
        name,
        std::span<const std::byte>(
            reinterpret_cast<const std::byte*>(
                bytes.data()),
            bytes.size()),
        sync_file);
}

[[nodiscard]] bool RewriteBytesAt(
    int directory_fd,
    const std::string& name,
    std::span<const std::byte> bytes) {
    const int fd = ::openat(
        directory_fd,
        name.c_str(),
        O_RDWR | O_TRUNC | O_NOFOLLOW | O_CLOEXEC);
    if (fd < 0) {
        return false;
    }
    bool ok = ::fchmod(fd, 0600) == 0;
    std::size_t completed = 0U;
    while (ok && completed < bytes.size()) {
        const ssize_t written = ::pwrite(
            fd,
            bytes.data() + completed,
            bytes.size() - completed,
            static_cast<off_t>(completed));
        if (written < 0 && errno == EINTR) {
            continue;
        }
        if (written <= 0) {
            ok = false;
            break;
        }
        completed += static_cast<std::size_t>(written);
    }
    if (ok) {
        ok = ::fsync(fd) == 0;
    }
    return ::close(fd) == 0 && ok;
}

[[nodiscard]] std::string ReadFileAt(
    int directory_fd,
    const std::string& name) {
    const int fd = ::openat(
        directory_fd,
        name.c_str(),
        O_RDONLY | O_NOFOLLOW | O_CLOEXEC |
            O_NOATIME);
    if (fd < 0) {
        return {};
    }
    struct stat status {};
    if (::fstat(fd, &status) != 0 ||
        status.st_size < 0) {
        static_cast<void>(::close(fd));
        return {};
    }
    std::string bytes(
        static_cast<std::size_t>(status.st_size),
        '\0');
    std::size_t completed = 0U;
    while (completed < bytes.size()) {
        const ssize_t read = ::pread(
            fd,
            bytes.data() + completed,
            bytes.size() - completed,
            static_cast<off_t>(completed));
        if (read < 0 && errno == EINTR) {
            continue;
        }
        if (read <= 0) {
            bytes.clear();
            break;
        }
        completed += static_cast<std::size_t>(read);
    }
    static_cast<void>(::close(fd));
    return bytes;
}

[[nodiscard]] bool NameExists(
    int directory_fd,
    const std::string& name) {
    struct stat status {};
    return ::fstatat(
               directory_fd,
               name.c_str(),
               &status,
               AT_SYMLINK_NOFOLLOW) == 0;
}

class RecoverSealFixture final {
public:
    [[nodiscard]] bool Initialize(
        TestContext* test,
        ingress::ReserveRecoveryIntentV1 intent =
            ingress::ReserveRecoveryIntentV1::
                kRecoverSealOnly) {
        if (test == nullptr || root_.fd() < 0) {
            return false;
        }
        key_ = MakeKey();
        writer_instance_ = Pattern<16U>(0xf0U);
        journal_ = MakeJournal(key_);
        std::string error;
        stream_directory_ =
            ingress::OpenOrCreateRawStreamDirectory(
                root_.path(),
                key_.route.source_stream_id,
                key_.route.capture_date,
                slug_,
                &error);
        test->Expect(
            stream_directory_ != nullptr,
            "fixture creates a private canonical Raw route");
        if (stream_directory_ == nullptr) {
            std::cerr << error << '\n';
            return false;
        }
        lease_ = ingress::AcquireRawWriterLeaseAtV1(
            stream_directory_->descriptor(),
            key_.route.source_stream_id,
            key_.route.capture_date,
            key_.recovery_attempt_id,
            &error);
        test->Expect(
            lease_ != nullptr,
            "fixture acquires the exact recovery writer lease");
        if (lease_ == nullptr) {
            std::cerr << error << '\n';
            return false;
        }
        if (::mkdirat(
                stream_directory_->descriptor(),
                "maintenance",
                0700) != 0 ||
            ::fsync(
                stream_directory_->descriptor()) != 0) {
            test->Expect(
                false,
                "fixture durably creates maintenance before publication");
            return false;
        }
        maintenance_fd_ = ::openat(
            stream_directory_->descriptor(),
            "maintenance",
            O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
                O_CLOEXEC | O_NOATIME);
        if (maintenance_fd_ < 0 ||
            !WriteBytesAt(
                stream_directory_->descriptor(),
                ingress::kRawJournalFilename,
                journal_) ||
            ::fsync(
                stream_directory_->descriptor()) != 0) {
            test->Expect(
                false,
                "fixture persists one exact header-only journal");
            return false;
        }

        const auto bootstrap =
            MakeBootstrap(root_.fd());
        const auto marker =
            MakeCoordinatorMarker(bootstrap);
        ingress::RawReserveCoordinatorErrorV1 failure =
            ingress::RawReserveCoordinatorErrorV1::
                kNone;
        coordinator_ = ingress::
            PublishFreshRawReserveRegistryCoordinatorAtV1(
                root_.fd(),
                marker,
                bootstrap,
                &failure,
                &error);
        test->Expect(
            coordinator_ != nullptr,
            "fixture publishes the durable coordinator");
        if (coordinator_ == nullptr) {
            std::cerr << error << '\n';
            return false;
        }
        ingress::RawReserveExistingAnchorRecoveryV1
            registration{};
        registration.key = key_;
        registration.writer_instance =
            writer_instance_;
        registration.recovery_intent = intent;
        registration.safe_stop_template_id = 7U;
        test->Expect(
            coordinator_->
                    RegisterExistingAnchorRecovering(
                        registration,
                        &error) ==
                ingress::
                    RawReserveCoordinatorErrorV1::kNone,
            "fixture persists one existing-anchor RECOVERING entry");
        if (coordinator_->state()
                .slots[
                    coordinator_->state()
                        .selected_slot]
                .entry_count != 1U) {
            std::cerr << error << '\n';
            return false;
        }

        ingress::EmptyAnchorObservationV1
            observation{};
        observation.journal_header_bytes = journal_;
        observation.journal_logical_size =
            ingress::kRawV1JournalHeaderBytes;
        observation.marker_count = 0U;
        observation.segment_count = 0U;
        observation.record_count = 0U;
        const auto build =
            ingress::
                BuildEmptyAnchorTombstoneCapabilityV1(
                    observation,
                    &tombstone_);
        test->Expect(
            build ==
                    ingress::
                        EmptyAnchorTombstoneV1Error::
                            kNone &&
                tombstone_ != nullptr,
            "fixture builds the immutable empty-anchor capability");
        return tombstone_ != nullptr;
    }

    ~RecoverSealFixture() {
        if (maintenance_fd_ >= 0) {
            static_cast<void>(
                ::close(maintenance_fd_));
        }
    }

    RecoverSealFixture() = default;
    RecoverSealFixture(
        const RecoverSealFixture&) = delete;
    RecoverSealFixture& operator=(
        const RecoverSealFixture&) = delete;

    [[nodiscard]] std::unique_ptr<
        ingress::RawReserveAuthorizedActionV1>
    AcquireRecovering() {
        ingress::RawReserveCoordinatorErrorV1 failure =
            ingress::RawReserveCoordinatorErrorV1::
                kNone;
        std::string error;
        return coordinator_->
            AcquireActionForExistingRoute(
                key_,
                ingress::ReserveRegistryStatusV1::
                    kRecovering,
                slug_,
                &failure,
                &error);
    }

    [[nodiscard]] bool IsRecovering() const {
        const auto state = coordinator_->state();
        const auto& slot =
            state.slots[state.selected_slot];
        return slot.entry_count == 1U &&
               slot.entries[0U].registry_status ==
                   ingress::ReserveRegistryStatusV1::
                       kRecovering;
    }

    [[nodiscard]] bool IsUnregistered() const {
        const auto state = coordinator_->state();
        return state
                   .slots[state.selected_slot]
                   .entry_count == 0U;
    }

    [[nodiscard]] ingress::RawWriterLease&
    lease() noexcept {
        return *lease_;
    }
    [[nodiscard]] const ingress::
        BuiltEmptyAnchorTombstoneV1&
    tombstone() const noexcept {
        return *tombstone_;
    }
    [[nodiscard]] int route_fd() const noexcept {
        return stream_directory_->descriptor();
    }
    [[nodiscard]] int maintenance_fd()
        const noexcept {
        return maintenance_fd_;
    }
    [[nodiscard]] int root_fd() const noexcept {
        return root_.fd();
    }
    [[nodiscard]] const std::string&
    root_path() const noexcept {
        return root_.path();
    }
    [[nodiscard]] const std::string&
    slug() const noexcept {
        return slug_;
    }
    [[nodiscard]] const ingress::
        RawReserveRegistryEntryKeyV1&
    key() const noexcept {
        return key_;
    }
    [[nodiscard]] const ingress::
        RawV1JournalHeaderWire&
    journal() const noexcept {
        return journal_;
    }
    [[nodiscard]] const ingress::RawV1Identity&
    writer_instance() const noexcept {
        return writer_instance_;
    }
    [[nodiscard]] std::shared_ptr<
        ingress::RawReserveRegistryCoordinatorV1>
    coordinator() const noexcept {
        return coordinator_;
    }

private:
    TempDirectory root_;
    ingress::RawReserveRegistryEntryKeyV1 key_{};
    ingress::RawV1Identity writer_instance_{};
    std::string slug_ = "sz-tick";
    ingress::RawV1JournalHeaderWire journal_{};
    std::unique_ptr<ingress::RawStreamDirectory>
        stream_directory_;
    std::unique_ptr<ingress::RawWriterLease> lease_;
    std::shared_ptr<
        ingress::RawReserveRegistryCoordinatorV1>
        coordinator_;
    std::unique_ptr<
        ingress::BuiltEmptyAnchorTombstoneV1>
        tombstone_;
    int maintenance_fd_ = -1;
};

ingress::EmptyAnchorTombstonePublishResultV1
Publish(RecoverSealFixture* fixture) {
    std::string diagnostic;
    return ingress::PublishEmptyAnchorTombstoneV1(
        fixture->lease(),
        fixture->AcquireRecovering(),
        fixture->tombstone(),
        &diagnostic);
}

std::unique_ptr<
    ingress::BuiltRecoveryMaintenanceReportV1>
MakeEmptyTerminalReport(
    const RecoverSealFixture& fixture) {
    ingress::RawRecoveryPlanV1 analysis{};
    if (ingress::DecodeDurableJournalHeaderV1(
            fixture.journal(),
            &analysis.journal_header) !=
        ingress::RawV1Error::kNone) {
        return nullptr;
    }
    analysis.accepted_journal_size =
        ingress::kRawV1JournalHeaderBytes;
    analysis.initial_anchor =
        ingress::RawRecoveryInitialAnchorV1::
            kJournalOnly;
    ingress::RawRecoveryExecutionResultV1 execution{};
    execution.retained_journal_size =
        ingress::kRawV1JournalHeaderBytes;
    std::unique_ptr<
        ingress::BuiltRecoveryMaintenanceReportV1>
        report;
    if (ingress::
            BuildEmptyAnchorOnlyRecoveryMaintenanceReportV1(
                fixture.key(),
                fixture.journal(),
                analysis,
                execution,
                ingress::kRawV1JournalHeaderBytes,
                fixture.tombstone(),
                &report) !=
        ingress::RecoveryMaintenanceReportV1Error::
            kNone) {
        return nullptr;
    }
    return report;
}

std::string TemporaryName(
    const RecoverSealFixture& fixture) {
    return "." +
           std::string(fixture.tombstone().filename()) +
           std::string(
               ingress::
                   kEmptyAnchorTombstoneV1TemporarySuffix);
}

void TestNewAndExistingPublication(
    TestContext* test) {
    RecoverSealFixture fixture;
    if (!fixture.Initialize(test)) {
        return;
    }
    auto result = Publish(&fixture);
    const std::string final_name(
        fixture.tombstone().filename());
    const std::string temporary =
        TemporaryName(fixture);
    struct stat status {};
    test->Expect(
        result.ok() &&
            result.disposition ==
                ingress::
                    EmptyAnchorTombstoneDispositionV1::
                        kPublishedNew &&
            result.observed_candidate_count == 0U &&
            result.file_synced &&
            result.directory_synced &&
            result.filename == final_name &&
            result.tombstone_sha256 ==
                fixture.tombstone()
                    .tombstone_sha256() &&
            result.receipt != nullptr &&
            fixture.IsRecovering(),
        "new publication crosses both durability barriers, retains a receipt, and does not unregister");
    test->Expect(
        ::fstatat(
            fixture.maintenance_fd(),
            final_name.c_str(),
            &status,
            AT_SYMLINK_NOFOLLOW) == 0 &&
            S_ISREG(status.st_mode) &&
            status.st_uid == ::geteuid() &&
            (status.st_mode & 07777U) == 0600U &&
            status.st_nlink ==
                static_cast<nlink_t>(1) &&
            ReadFileAt(
                fixture.maintenance_fd(),
                final_name) ==
                fixture.tombstone().canonical_jcs() &&
            !NameExists(
                fixture.maintenance_fd(),
                temporary),
        "published final is exact owner-only JCS and the deterministic tmp is absent");

    auto repeated = Publish(&fixture);
    test->Expect(
        repeated.ok() &&
            repeated.disposition ==
                ingress::
                    EmptyAnchorTombstoneDispositionV1::
                        kAcceptedExistingFinal &&
            repeated.observed_candidate_count == 1U &&
            repeated.file_synced &&
            repeated.directory_synced &&
            repeated.receipt != nullptr &&
            fixture.IsRecovering(),
        "an exact existing final is re-synced, re-read, and accepted idempotently");
}

void TestCompleteTemporaryAdoption(
    TestContext* test) {
    RecoverSealFixture fixture;
    if (!fixture.Initialize(test)) {
        return;
    }
    const std::string temporary =
        TemporaryName(fixture);
    struct stat before {};
    struct stat after {};
    test->Expect(
        WriteFileAt(
            fixture.maintenance_fd(),
            temporary,
            fixture.tombstone().canonical_jcs()) &&
            ::fstatat(
                fixture.maintenance_fd(),
                temporary.c_str(),
                &before,
                AT_SYMLINK_NOFOLLOW) == 0,
        "complete deterministic crash tmp is persisted");
    auto result = Publish(&fixture);
    test->Expect(
        result.ok() &&
            result.disposition ==
                ingress::
                    EmptyAnchorTombstoneDispositionV1::
                        kAdoptedCompleteTemporary &&
            result.observed_candidate_count == 1U &&
            ::fstatat(
                fixture.maintenance_fd(),
                std::string(
                    fixture.tombstone().filename())
                    .c_str(),
                &after,
                AT_SYMLINK_NOFOLLOW) == 0 &&
            before.st_dev == after.st_dev &&
            before.st_ino == after.st_ino &&
            !NameExists(
                fixture.maintenance_fd(),
                temporary),
        "complete tmp is fsynced and adopted without replacing its inode");
}

void TestPartialAndConflictingCandidates(
    TestContext* test) {
    {
        RecoverSealFixture fixture;
        if (!fixture.Initialize(test)) {
            return;
        }
        const std::string temporary =
            TemporaryName(fixture);
        const std::string_view desired =
            fixture.tombstone().canonical_jcs();
        const std::string prefix(
            desired.substr(0U, desired.size() / 2U));
        test->Expect(
            WriteFileAt(
                fixture.maintenance_fd(),
                temporary,
                prefix),
            "partial deterministic tmp is persisted");
        const auto result = Publish(&fixture);
        test->Expect(
            result.error ==
                    ingress::
                        EmptyAnchorTombstoneStoreErrorV1::
                            kCandidateConflict &&
                result.receipt == nullptr &&
                ReadFileAt(
                    fixture.maintenance_fd(),
                    temporary) == prefix &&
                !NameExists(
                    fixture.maintenance_fd(),
                    std::string(
                        fixture.tombstone()
                            .filename())),
            "partial tmp fails closed, remains evidence, and is never rebuilt");
    }

    {
        RecoverSealFixture fixture;
        if (!fixture.Initialize(test)) {
            return;
        }
        const std::string final_name(
            fixture.tombstone().filename());
        test->Expect(
            WriteFileAt(
                fixture.maintenance_fd(),
                final_name,
                "{}"),
            "conflicting fixed final is persisted");
        const auto result = Publish(&fixture);
        test->Expect(
            result.error ==
                    ingress::
                        EmptyAnchorTombstoneStoreErrorV1::
                            kCandidateConflict &&
                result.receipt == nullptr &&
                ReadFileAt(
                    fixture.maintenance_fd(),
                    final_name) == "{}",
            "conflicting final is retained and never overwritten");
    }

    {
        RecoverSealFixture fixture;
        if (!fixture.Initialize(test)) {
            return;
        }
        const std::string final_name(
            fixture.tombstone().filename());
        const std::string temporary =
            TemporaryName(fixture);
        test->Expect(
            WriteFileAt(
                fixture.maintenance_fd(),
                final_name,
                fixture.tombstone()
                    .canonical_jcs()) &&
                WriteFileAt(
                    fixture.maintenance_fd(),
                    temporary,
                    fixture.tombstone()
                        .canonical_jcs()),
            "exact final and exact tmp are both persisted");
        const auto result = Publish(&fixture);
        test->Expect(
            result.error ==
                    ingress::
                        EmptyAnchorTombstoneStoreErrorV1::
                            kCandidateConflict &&
                result.receipt == nullptr &&
                NameExists(
                    fixture.maintenance_fd(),
                    final_name) &&
                NameExists(
                    fixture.maintenance_fd(),
                    temporary),
            "final plus tmp is ambiguous and both evidence names remain");
    }
}

void TestUnsafeCandidates(
    TestContext* test) {
    {
        RecoverSealFixture fixture;
        if (!fixture.Initialize(test)) {
            return;
        }
        const std::string temporary =
            TemporaryName(fixture);
        test->Expect(
            ::symlinkat(
                "missing-target",
                fixture.maintenance_fd(),
                temporary.c_str()) == 0,
            "typed symlink tmp is created");
        const auto result = Publish(&fixture);
        test->Expect(
            result.error ==
                    ingress::
                        EmptyAnchorTombstoneStoreErrorV1::
                            kUnsafeCandidate &&
                result.receipt == nullptr &&
                NameExists(
                    fixture.maintenance_fd(),
                    temporary),
            "typed symlink is rejected without traversal or cleanup");
    }

    {
        RecoverSealFixture fixture;
        if (!fixture.Initialize(test)) {
            return;
        }
        const std::string temporary =
            TemporaryName(fixture);
        test->Expect(
            WriteFileAt(
                fixture.maintenance_fd(),
                temporary,
                fixture.tombstone()
                    .canonical_jcs()) &&
                ::linkat(
                    fixture.maintenance_fd(),
                    temporary.c_str(),
                    fixture.maintenance_fd(),
                    "retained-hardlink-evidence",
                    0) == 0,
            "typed tmp hardlink alias is created");
        const auto result = Publish(&fixture);
        struct stat status {};
        test->Expect(
            result.error ==
                    ingress::
                        EmptyAnchorTombstoneStoreErrorV1::
                            kUnsafeCandidate &&
                result.receipt == nullptr &&
                ::fstatat(
                    fixture.maintenance_fd(),
                    temporary.c_str(),
                    &status,
                    AT_SYMLINK_NOFOLLOW) == 0 &&
                status.st_nlink ==
                    static_cast<nlink_t>(2),
            "multiply linked candidate fails closed and both links remain");
    }

    {
        RecoverSealFixture fixture;
        if (!fixture.Initialize(test)) {
            return;
        }
        const std::string temporary =
            TemporaryName(fixture);
        test->Expect(
            WriteFileAt(
                fixture.maintenance_fd(),
                temporary,
                fixture.tombstone()
                    .canonical_jcs()) &&
                ::fchmodat(
                    fixture.maintenance_fd(),
                    temporary.c_str(),
                    0644,
                    0) == 0,
            "wrong-mode typed tmp is created");
        const auto result = Publish(&fixture);
        test->Expect(
            result.error ==
                    ingress::
                        EmptyAnchorTombstoneStoreErrorV1::
                            kUnsafeCandidate &&
                result.receipt == nullptr,
            "non-0600 candidate is rejected");
    }
}

void TestStrictStreamDayInventory(
    TestContext* test) {
    constexpr std::array<std::string_view, 6U>
        unexpected_names{
            "segment-00000001.raw",
            "segment-00000001.idx",
            "manifest.json",
            "control.page",
            ".segment-00000001.raw.tmp",
            "unknown.raw-evidence"};
    for (const std::string_view unexpected :
         unexpected_names) {
        RecoverSealFixture fixture;
        if (!fixture.Initialize(test)) {
            return;
        }
        test->Expect(
            WriteFileAt(
                fixture.route_fd(),
                std::string(unexpected),
                {}),
            "unexpected stream-day artifact is created");
        const auto result = Publish(&fixture);
        test->Expect(
            result.error ==
                    ingress::
                        EmptyAnchorTombstoneStoreErrorV1::
                            kNamespaceInventory &&
                result.receipt == nullptr &&
                NameExists(
                    fixture.route_fd(),
                    std::string(unexpected)) &&
                !NameExists(
                    fixture.maintenance_fd(),
                    std::string(
                        fixture.tombstone()
                            .filename())),
            "any segment/index/manifest/control/tmp/unknown route artifact rejects the empty-anchor claim");
    }
}

void TestJournalAndMaintenanceSafety(
    TestContext* test) {
    {
        RecoverSealFixture fixture;
        if (!fixture.Initialize(test)) {
            return;
        }
        const int fd = ::openat(
            fixture.route_fd(),
            ingress::kRawJournalFilename,
            O_RDWR | O_NOFOLLOW | O_CLOEXEC);
        const bool enlarged =
            fd >= 0 &&
            ::ftruncate(
                fd,
                static_cast<off_t>(
                    ingress::
                        kRawV1JournalHeaderBytes +
                    1U)) == 0 &&
            ::fsync(fd) == 0;
        if (fd >= 0) {
            static_cast<void>(::close(fd));
        }
        test->Expect(
            enlarged,
            "journal is enlarged beyond the exact header");
        const auto result = Publish(&fixture);
        test->Expect(
            result.error ==
                    ingress::
                        EmptyAnchorTombstoneStoreErrorV1::
                            kUnsafeJournal &&
                result.receipt == nullptr,
            "a journal with any marker/tail byte is not an empty anchor");
    }

    {
        RecoverSealFixture fixture;
        if (!fixture.Initialize(test)) {
            return;
        }
        auto corrupt = fixture.journal();
        corrupt[64U] ^= std::byte{0x01U};
        test->Expect(
            RewriteBytesAt(
                fixture.route_fd(),
                ingress::kRawJournalFilename,
                corrupt),
            "same-size journal corruption is persisted");
        const auto result = Publish(&fixture);
        test->Expect(
            result.error ==
                    ingress::
                        EmptyAnchorTombstoneStoreErrorV1::
                            kJournalMismatch &&
                result.receipt == nullptr,
            "journal CRC/canonical/hash mismatch is rejected");
    }

    {
        RecoverSealFixture fixture;
        if (!fixture.Initialize(test)) {
            return;
        }
        test->Expect(
            ::unlinkat(
                fixture.route_fd(),
                "maintenance",
                AT_REMOVEDIR) == 0 &&
                ::fsync(fixture.route_fd()) == 0,
            "pre-existing maintenance name is removed");
        const auto result = Publish(&fixture);
        test->Expect(
            result.error ==
                    ingress::
                        EmptyAnchorTombstoneStoreErrorV1::
                            kMaintenanceDirectoryMissing &&
                result.receipt == nullptr &&
                !NameExists(
                    fixture.route_fd(),
                    "maintenance"),
            "publisher never creates a missing maintenance directory");
    }
}

void TestAuthorizationAndTargetBinding(
    TestContext* test) {
    {
        RecoverSealFixture fixture;
        if (!fixture.Initialize(
                test,
                ingress::ReserveRecoveryIntentV1::
                    kResumeConnect)) {
            return;
        }
        const auto result = Publish(&fixture);
        test->Expect(
            result.error ==
                    ingress::
                        EmptyAnchorTombstoneStoreErrorV1::
                            kAuthorizationRejected &&
                result.receipt == nullptr,
            "RECOVERING without RECOVER_SEAL_ONLY cannot publish the tombstone");
    }

    {
        RecoverSealFixture fixture;
        if (!fixture.Initialize(test)) {
            return;
        }
        std::string diagnostic;
        auto result =
            ingress::PublishEmptyAnchorTombstoneV1(
                fixture.lease(),
                nullptr,
                fixture.tombstone(),
                &diagnostic);
        test->Expect(
            result.error ==
                    ingress::
                        EmptyAnchorTombstoneStoreErrorV1::
                            kInvalidArgument &&
                result.receipt == nullptr,
            "missing shared-gate action is rejected before filesystem mutation");
    }

    {
        RecoverSealFixture fixture;
        if (!fixture.Initialize(test)) {
            return;
        }
        auto action = fixture.AcquireRecovering();
        const std::string date_name =
            "capture_date=" +
            std::to_string(
                fixture.key().route.capture_date);
        const std::string route_name =
            "stream=" +
            std::to_string(
                fixture.key().route
                    .source_stream_id) +
            "-" + fixture.slug();
        const std::string moved_name =
            route_name + "-moved";
        const int date_fd = ::openat(
            fixture.root_fd(),
            date_name.c_str(),
            O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
                O_CLOEXEC);
        const bool moved =
            date_fd >= 0 &&
            ::renameat(
                date_fd,
                route_name.c_str(),
                date_fd,
                moved_name.c_str()) == 0 &&
            ::fsync(date_fd) == 0;
        std::string diagnostic;
        auto result =
            ingress::PublishEmptyAnchorTombstoneV1(
                fixture.lease(),
                std::move(action),
                fixture.tombstone(),
                &diagnostic);
        test->Expect(
            moved &&
                result.error ==
                    ingress::
                        EmptyAnchorTombstoneStoreErrorV1::
                            kAuthorizationRejected &&
                result.receipt == nullptr &&
                !NameExists(
                    fixture.maintenance_fd(),
                    std::string(
                        fixture.tombstone()
                            .filename())),
            "canonical route replacement invalidates the action before tombstone mutation");
        const bool restored =
            date_fd >= 0 &&
            ::renameat(
                date_fd,
                moved_name.c_str(),
                date_fd,
                route_name.c_str()) == 0 &&
            ::fsync(date_fd) == 0;
        if (date_fd >= 0) {
            static_cast<void>(::close(date_fd));
        }
        test->Expect(
            restored,
            "canonical route replacement fixture is restored");
    }
}

void TestTypedNameUniqueness(
    TestContext* test) {
    {
        RecoverSealFixture fixture;
        if (!fixture.Initialize(test)) {
            return;
        }
        const std::string malformed =
            "empty-anchor-deadbeef.json";
        test->Expect(
            WriteFileAt(
                fixture.maintenance_fd(),
                malformed,
                "{}"),
            "malformed tombstone-like name is created");
        const auto result = Publish(&fixture);
        test->Expect(
            result.error ==
                    ingress::
                        EmptyAnchorTombstoneStoreErrorV1::
                            kMalformedCandidateName &&
                result.receipt == nullptr &&
                NameExists(
                    fixture.maintenance_fd(),
                    malformed),
            "malformed typed evidence fails closed and remains");
    }

    {
        RecoverSealFixture fixture;
        if (!fixture.Initialize(test)) {
            return;
        }
        const std::string foreign =
            "empty-anchor-"
            "00000000000000000000000000000000"
            ".json";
        test->Expect(
            foreign !=
                    fixture.tombstone().filename() &&
                WriteFileAt(
                    fixture.maintenance_fd(),
                    foreign,
                    "{}"),
            "second syntactically valid tombstone locator is created");
        const auto result = Publish(&fixture);
        test->Expect(
            result.error ==
                    ingress::
                        EmptyAnchorTombstoneStoreErrorV1::
                            kCandidateConflict &&
                result.receipt == nullptr &&
                NameExists(
                    fixture.maintenance_fd(),
                    foreign),
            "a stream-day cannot contain a second typed tombstone locator");
    }
}

void TestEmptyTerminalRecoveryLifecycle(
    TestContext* test) {
    RecoverSealFixture fixture;
    if (!fixture.Initialize(test)) {
        return;
    }
    auto sidecar = Publish(&fixture);
    auto report = MakeEmptyTerminalReport(fixture);
    test->Expect(
        sidecar.ok() &&
            sidecar.receipt != nullptr &&
            report != nullptr,
        "empty terminal fixture crosses the sidecar barrier and builds its tagged report");
    if (sidecar.receipt == nullptr ||
        report == nullptr) {
        return;
    }

    const std::string temporary =
        "." + std::string(report->filename()) +
        std::string(
            ingress::
                kRecoveryMaintenanceReportV1TemporarySuffix);
    test->Expect(
        WriteFileAt(
            fixture.maintenance_fd(),
            temporary,
            report->canonical_jcs()),
        "empty terminal report crash-complete temporary is durable");
    std::string diagnostic;
    auto report_publication =
        ingress::PublishRecoveryMaintenanceReportV1(
            fixture.lease(),
            fixture.AcquireRecovering(),
            *report,
            std::move(sidecar.receipt),
            &diagnostic);
    test->Expect(
        report_publication.ok() &&
            report_publication.disposition ==
                ingress::
                    RecoveryMaintenanceReportDispositionV1::
                        kAdoptedCompleteTemporary &&
            report_publication.activation_receipt ==
                nullptr &&
            report_publication.terminal_receipt !=
                nullptr &&
            fixture.IsRecovering(),
        "EMPTY_ANCHOR_ONLY consumes its sidecar receipt before adopting and barriering the terminal report");
    if (report_publication.terminal_receipt ==
        nullptr) {
        std::cerr << diagnostic << '\n';
        return;
    }
    std::string error;
    test->Expect(
        fixture.coordinator()->
                UnregisterRecoveredTerminal(
                    std::move(
                        report_publication
                            .terminal_receipt),
                    &error) ==
                ingress::
                    RawReserveCoordinatorErrorV1::kNone &&
            fixture.IsUnregistered(),
        "terminal receipt revalidates empty sidecar/report/journal evidence and unregisters exact RECOVERING");
    test->Expect(
        fixture.coordinator()->
                UnregisterRecoveredTerminal(
                    nullptr, &error) ==
            ingress::RawReserveCoordinatorErrorV1::
                kInvalidArgument,
        "a terminal receipt cannot be replayed as a null capability");
}

void TestTerminalReceiptStaleAndReplacement(
    TestContext* test) {
    {
        RecoverSealFixture fixture;
        if (!fixture.Initialize(test)) {
            return;
        }
        auto sidecar = Publish(&fixture);
        auto report = MakeEmptyTerminalReport(fixture);
        if (sidecar.receipt == nullptr ||
            report == nullptr) {
            test->Expect(
                false,
                "stale terminal fixture builds both capabilities");
            return;
        }
        std::string diagnostic;
        auto publication =
            ingress::PublishRecoveryMaintenanceReportV1(
                fixture.lease(),
                fixture.AcquireRecovering(),
                *report,
                std::move(sidecar.receipt),
                &diagnostic);
        ingress::RawReserveWriterTakeoverV1 takeover{};
        takeover.key = fixture.key();
        takeover.new_writer_instance =
            Pattern<16U>(0x81U);
        std::string error;
        test->Expect(
            publication.terminal_receipt != nullptr &&
                fixture.coordinator()->
                        TakeoverRecovering(
                            takeover, &error) ==
                    ingress::
                        RawReserveCoordinatorErrorV1::
                            kNone &&
                fixture.coordinator()->
                        UnregisterRecoveredTerminal(
                            std::move(
                                publication
                                    .terminal_receipt),
                            &error) ==
                    ingress::
                        RawReserveCoordinatorErrorV1::
                            kActionGenerationChanged &&
                fixture.IsRecovering(),
            "stale terminal receipt cannot unregister a newer RECOVERING generation");
    }

    {
        RecoverSealFixture fixture;
        if (!fixture.Initialize(test)) {
            return;
        }
        auto sidecar = Publish(&fixture);
        auto report = MakeEmptyTerminalReport(fixture);
        if (sidecar.receipt == nullptr ||
            report == nullptr) {
            test->Expect(
                false,
                "replacement terminal fixture builds both capabilities");
            return;
        }
        std::string diagnostic;
        auto publication =
            ingress::PublishRecoveryMaintenanceReportV1(
                fixture.lease(),
                fixture.AcquireRecovering(),
                *report,
                std::move(sidecar.receipt),
                &diagnostic);
        const std::string report_name(
            report->filename());
        const bool replaced =
            ::unlinkat(
                fixture.maintenance_fd(),
                report_name.c_str(), 0) == 0 &&
            WriteFileAt(
                fixture.maintenance_fd(),
                report_name,
                report->canonical_jcs()) &&
            ::fsync(
                fixture.maintenance_fd()) == 0;
        std::string error;
        test->Expect(
            publication.terminal_receipt != nullptr &&
                replaced &&
                fixture.coordinator()->
                        UnregisterRecoveredTerminal(
                            std::move(
                                publication
                                    .terminal_receipt),
                            &error) ==
                    ingress::
                        RawReserveCoordinatorErrorV1::
                            kTargetIdentityChanged &&
                fixture.IsRecovering(),
            "same-name same-bytes terminal report inode replacement is rejected");
    }
}

void TestTerminalReportRequiresExactSidecar(
    TestContext* test) {
    {
        RecoverSealFixture fixture;
        if (!fixture.Initialize(test)) {
            return;
        }
        auto report = MakeEmptyTerminalReport(fixture);
        if (report == nullptr) {
            test->Expect(
                false,
                "missing-sidecar terminal report fixture builds");
            return;
        }
        std::string diagnostic;
        const auto publication =
            ingress::PublishRecoveryMaintenanceReportV1(
                fixture.lease(),
                fixture.AcquireRecovering(),
                *report,
                &diagnostic);
        test->Expect(
            publication.error ==
                    ingress::
                        RecoveryMaintenanceReportStoreErrorV1::
                            kInvalidArgument &&
                publication.terminal_receipt ==
                    nullptr &&
                !NameExists(
                    fixture.maintenance_fd(),
                    std::string(report->filename())),
            "terminal report tag cannot use the RESUMED_OPEN publisher shape or mutate without a sidecar receipt");
    }

    {
        RecoverSealFixture fixture;
        if (!fixture.Initialize(test)) {
            return;
        }
        auto sidecar = Publish(&fixture);
        auto report = MakeEmptyTerminalReport(fixture);
        if (sidecar.receipt == nullptr ||
            report == nullptr) {
            test->Expect(
                false,
                "changed-sidecar terminal report fixture builds");
            return;
        }
        std::string changed(
            fixture.tombstone().canonical_jcs());
        changed[changed.size() / 2U] =
            changed[changed.size() / 2U] == '0'
                ? '1'
                : '0';
        const auto changed_bytes =
            std::span<const std::byte>(
                reinterpret_cast<const std::byte*>(
                    changed.data()),
                changed.size());
        test->Expect(
            RewriteBytesAt(
                fixture.maintenance_fd(),
                std::string(
                    fixture.tombstone().filename()),
                changed_bytes),
            "retained sidecar inode is changed after its barrier");
        std::string diagnostic;
        const auto publication =
            ingress::PublishRecoveryMaintenanceReportV1(
                fixture.lease(),
                fixture.AcquireRecovering(),
                *report,
                std::move(sidecar.receipt),
                &diagnostic);
        test->Expect(
            publication.error ==
                    ingress::
                        RecoveryMaintenanceReportStoreErrorV1::
                            kAuthorizationRejected &&
                publication.terminal_receipt ==
                    nullptr &&
                !NameExists(
                    fixture.maintenance_fd(),
                    std::string(report->filename())),
            "changed sidecar hash/JCS is rejected before any terminal report mutation");
    }
}

}  // namespace

int main() {
    TestContext test;
    TestNewAndExistingPublication(&test);
    TestCompleteTemporaryAdoption(&test);
    TestPartialAndConflictingCandidates(&test);
    TestUnsafeCandidates(&test);
    TestStrictStreamDayInventory(&test);
    TestJournalAndMaintenanceSafety(&test);
    TestAuthorizationAndTargetBinding(&test);
    TestTypedNameUniqueness(&test);
    TestEmptyTerminalRecoveryLifecycle(&test);
    TestTerminalReceiptStaleAndReplacement(&test);
    TestTerminalReportRequiresExactSidecar(&test);
    if (test.failures != 0) {
        std::cerr << test.failures
                  << " empty-anchor tombstone store assertion(s) failed\n";
        return 1;
    }
    std::cout
        << "Phase 2 empty-anchor tombstone POSIX store checks passed\n";
    return 0;
}
