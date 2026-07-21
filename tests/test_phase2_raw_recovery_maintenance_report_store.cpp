#include "l2flow/common/sha256.h"
#include "l2flow/ingress/raw_manifest_transition.h"
#include "l2flow/ingress/raw_namespace.h"
#include "l2flow/ingress/raw_recovery_maintenance_report_store.h"
#include "l2flow/ingress/raw_reserve_active_activation_receipt.h"
#include "l2flow/ingress/raw_reserve_coordinator.h"
#include "l2flow/ingress/raw_schema.h"

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace common = l2flow::common;
namespace ingress = l2flow::ingress;

namespace {

using PublishFunction = ingress::
    RecoveryMaintenanceReportPublishResultV1 (*)(
        const ingress::RawWriterLease&,
        std::unique_ptr<
            ingress::RawReserveAuthorizedActionV1>,
        const ingress::
            BuiltRecoveryMaintenanceReportV1&,
        std::string*) noexcept;

static_assert(
    std::is_same_v<
        decltype(
            static_cast<PublishFunction>(
                &ingress::
                    PublishRecoveryMaintenanceReportV1)),
        PublishFunction>);
static_assert(
    !std::is_default_constructible_v<
        ingress::RecoveryTerminalReportReceiptV1>);
static_assert(
    !std::is_copy_constructible_v<
        ingress::RecoveryTerminalReportReceiptV1>);
static_assert(
    !std::is_move_constructible_v<
        ingress::RecoveryTerminalReportReceiptV1>);

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
            "/tmp/l2flow-recovery-report-store-XXXXXX";
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

ingress::SegmentHeaderV1 MakeSegment(
    const ingress::RawReserveRegistryEntryKeyV1&
        key) {
    ingress::SegmentHeaderV1 segment{};
    segment.source_stream_id =
        key.route.source_stream_id;
    segment.capture_date =
        key.route.capture_date;
    segment.stream_day_id = key.stream_day_id;
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
    segment.config_sha256 =
        Pattern<32U>(0xc1U);
    segment.raw_schema_sha256 =
        ingress::kFrozenRawSchemaSha256;
    segment.build_manifest_sha256 =
        Pattern<32U>(0x22U);
    return segment;
}

ingress::RawV1JournalHeaderWire MakeJournal(
    const ingress::SegmentHeaderV1& segment) {
    ingress::DurableJournalHeaderV1 journal{};
    journal.capture_date = segment.capture_date;
    journal.source_stream_id =
        segment.source_stream_id;
    journal.stream_day_id = segment.stream_day_id;
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

std::unique_ptr<
    ingress::BuiltRecoveryMaintenanceReportV1>
MakeReport(
    const ingress::RawReserveRegistryEntryKeyV1& key,
    const ingress::RawV1Identity& writer_instance) {
    const ingress::SegmentHeaderV1 segment =
        MakeSegment(key);
    ingress::RawV1SegmentHeaderWire segment_wire{};
    if (ingress::EncodeSegmentHeaderV1(
            segment, &segment_wire) !=
        ingress::RawV1Error::kNone) {
        return nullptr;
    }
    auto bytes =
        std::make_shared<std::vector<std::byte>>(
            segment_wire.begin(), segment_wire.end());

    ingress::RawWalWriterSnapshot wal{};
    wal.append = {
        ingress::kRawV1SegmentHeaderBytes,
        0U,
        ingress::kRawV1SegmentHeaderBytes};
    wal.durable = wal.append;
    wal.journal_logical_size =
        ingress::kRawV1JournalHeaderBytes +
        ingress::kRawV1DurableMarkerBytes;
    wal.initialized = true;

    ingress::RawManifestV1 manifest{};
    if (ingress::BuildFreshOpenRawManifestV1(
            segment, wal, &manifest) !=
        ingress::RawManifestTransitionErrorV1::kNone) {
        return nullptr;
    }

    ingress::RawRecoveryPlanV1 analysis{};
    const ingress::RawV1JournalHeaderWire journal =
        MakeJournal(segment);
    if (ingress::DecodeDurableJournalHeaderV1(
            journal, &analysis.journal_header) !=
        ingress::RawV1Error::kNone) {
        return nullptr;
    }
    analysis.accepted_journal_size =
        wal.journal_logical_size;
    analysis.has_accepted_cursor = true;
    analysis.accepted_cursor = {
        1U,
        ingress::kRawV1SegmentHeaderBytes,
        0U,
        ingress::kRawV1SegmentHeaderBytes,
        0U};
    ingress::RawRecoverySegmentPlanV1 plan{};
    plan.segment_sequence = 1U;
    plan.segment_base_wal_pos = 0U;
    plan.has_accepted_marker = true;
    plan.accepted_marker_wire =
        manifest.open_entry->accepted_marker_bytes;
    plan.durable_end_offset =
        ingress::kRawV1SegmentHeaderBytes;
    plan.validated_logical_end_offset =
        ingress::kRawV1SegmentHeaderBytes;
    plan.validated_last_ingress_sequence = 0U;
    plan.append_only_begin_offset =
        ingress::kRawV1SegmentHeaderBytes;
    plan.append_only_end_offset =
        ingress::kRawV1SegmentHeaderBytes;
    plan.tail_begin_offset =
        ingress::kRawV1SegmentHeaderBytes;
    plan.tail_end_offset =
        ingress::kRawV1SegmentHeaderBytes;
    analysis.segments.push_back(plan);

    ingress::RawRecoveryExecutionResultV1 execution{};
    execution.cursor_publishable = true;
    execution.retained_journal_size =
        wal.journal_logical_size;
    execution.recovered_cursor =
        analysis.accepted_cursor;

    ingress::RawWalSinkIdentityV1 sink{};
    sink.writer_instance = writer_instance;
    sink.stream_day_id = key.stream_day_id;
    sink.source_stream_id =
        key.route.source_stream_id;
    sink.capture_date = key.route.capture_date;
    sink.segment_sequence = 1U;
    sink.segment_base_wal_pos = 0U;
    sink.first_ingress_sequence = 1U;

    std::unique_ptr<
        ingress::BuiltRecoveryMaintenanceReportV1>
        report;
    if (ingress::
            BuildResumedOpenRecoveryMaintenanceReportV1(
                key,
                journal,
                analysis,
                execution,
                wal.journal_logical_size,
                manifest,
                bytes,
                sink,
                wal,
                nullptr,
                &report) !=
        ingress::RecoveryMaintenanceReportV1Error::
            kNone) {
        return nullptr;
    }
    return report;
}

[[nodiscard]] bool WriteFileAt(
    int directory_fd,
    const std::string& name,
    std::string_view bytes,
    bool sync_file = true) {
    const int fd = ::openat(
        directory_fd,
        name.c_str(),
        O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW |
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
    const int close_result = ::close(fd);
    return ok && close_result == 0;
}

[[nodiscard]] bool ReplaceFileAt(
    int directory_fd,
    const std::string& name,
    std::string_view bytes) {
    return ::unlinkat(
               directory_fd, name.c_str(), 0) == 0 &&
           WriteFileAt(directory_fd, name, bytes) &&
           ::fsync(directory_fd) == 0;
}

[[nodiscard]] bool RewriteFileAt(
    int directory_fd,
    const std::string& name,
    std::string_view bytes) {
    const int fd = ::openat(
        directory_fd,
        name.c_str(),
        O_WRONLY | O_TRUNC | O_NOFOLLOW | O_CLOEXEC);
    if (fd < 0) {
        return false;
    }
    bool ok = true;
    std::size_t completed = 0U;
    while (completed < bytes.size()) {
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

class RecoveringFixture final {
public:
    [[nodiscard]] bool Initialize(
        TestContext* test) {
        if (test == nullptr || root_.fd() < 0) {
            return false;
        }
        key_ = MakeKey();
        writer_instance_ = Pattern<16U>(0xf0U);
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
            "fixture acquires the recovery-attempt writer lease");
        if (lease_ == nullptr) {
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
                "fixture durably creates maintenance");
            return false;
        }
        maintenance_fd_ = ::openat(
            stream_directory_->descriptor(),
            "maintenance",
            O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
                O_CLOEXEC | O_NOATIME);
        if (maintenance_fd_ < 0) {
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
            return false;
        }

        ingress::RawReserveExistingAnchorRecoveryV1
            registration{};
        registration.key = key_;
        registration.writer_instance =
            writer_instance_;
        registration.recovery_intent =
            ingress::ReserveRecoveryIntentV1::
                kResumeConnect;
        registration.safe_stop_template_id = 7U;
        test->Expect(
            coordinator_->
                    RegisterExistingAnchorRecovering(
                        registration,
                        &error) ==
                ingress::
                    RawReserveCoordinatorErrorV1::kNone,
            "fixture remains durably RECOVERING");
        report_ = MakeReport(key_, writer_instance_);
        test->Expect(
            report_ != nullptr,
            "fixture builds the private report capability");
        return report_ != nullptr;
    }

    ~RecoveringFixture() {
        if (maintenance_fd_ >= 0) {
            static_cast<void>(
                ::close(maintenance_fd_));
        }
    }

    RecoveringFixture() = default;
    RecoveringFixture(
        const RecoveringFixture&) = delete;
    RecoveringFixture& operator=(
        const RecoveringFixture&) = delete;

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

    [[nodiscard]] bool IsStatus(
        ingress::ReserveRegistryStatusV1 status) const {
        const auto state = coordinator_->state();
        const auto& slot =
            state.slots[state.selected_slot];
        return slot.entry_count == 1U &&
               slot.entries[0U].registry_status ==
                   status;
    }

    [[nodiscard]] ingress::RawWriterLease&
    lease() noexcept {
        return *lease_;
    }
    [[nodiscard]] const ingress::
        BuiltRecoveryMaintenanceReportV1&
    report() const noexcept {
        return *report_;
    }
    [[nodiscard]] int maintenance_fd()
        const noexcept {
        return maintenance_fd_;
    }
    [[nodiscard]] const ingress::
        RawReserveRegistryEntryKeyV1&
    key() const noexcept {
        return key_;
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
    std::unique_ptr<ingress::RawStreamDirectory>
        stream_directory_;
    std::unique_ptr<ingress::RawWriterLease> lease_;
    std::shared_ptr<
        ingress::RawReserveRegistryCoordinatorV1>
        coordinator_;
    std::unique_ptr<
        ingress::BuiltRecoveryMaintenanceReportV1>
        report_;
    int maintenance_fd_ = -1;
};

ingress::RecoveryMaintenanceReportPublishResultV1
Publish(RecoveringFixture* fixture) {
    std::string diagnostic;
    return ingress::PublishRecoveryMaintenanceReportV1(
        fixture->lease(),
        fixture->AcquireRecovering(),
        fixture->report(),
        &diagnostic);
}

std::string TemporaryName(
    const RecoveringFixture& fixture) {
    return "." +
           std::string(fixture.report().filename()) +
           std::string(
               ingress::
                   kRecoveryMaintenanceReportV1TemporarySuffix);
}

void TestCompleteTemporaryAdoption(
    TestContext* test) {
    RecoveringFixture fixture;
    if (!fixture.Initialize(test)) {
        return;
    }
    const std::string temporary =
        TemporaryName(fixture);
    test->Expect(
        WriteFileAt(
            fixture.maintenance_fd(),
            temporary,
            fixture.report().canonical_jcs()),
        "writes one fully synced report typed temporary");
    auto publication = Publish(&fixture);
    struct stat final_status {};
    struct stat temporary_status {};
    test->Expect(
        publication.ok() &&
            publication.disposition ==
                ingress::
                    RecoveryMaintenanceReportDispositionV1::
                        kAdoptedCompleteTemporary &&
            publication.observed_candidate_count == 1U &&
            publication.activation_receipt != nullptr &&
            ::fstatat(
                fixture.maintenance_fd(),
                std::string(fixture.report().filename())
                    .c_str(),
                &final_status,
                AT_SYMLINK_NOFOLLOW) == 0 &&
            ::fstatat(
                fixture.maintenance_fd(),
                temporary.c_str(),
                &temporary_status,
                AT_SYMLINK_NOFOLLOW) != 0 &&
            errno == ENOENT,
        "complete typed tmp is adopted by NOREPLACE publication");
}

void TestRecognizedPartialRebuild(
    TestContext* test) {
    RecoveringFixture fixture;
    if (!fixture.Initialize(test)) {
        return;
    }
    const std::string temporary =
        TemporaryName(fixture);
    const std::string_view desired =
        fixture.report().canonical_jcs();
    test->Expect(
        WriteFileAt(
            fixture.maintenance_fd(),
            temporary,
            desired.substr(0U, desired.size() / 2U)),
        "writes one exact-prefix partial typed tmp");
    auto publication = Publish(&fixture);
    test->Expect(
        publication.ok() &&
            publication.disposition ==
                ingress::
                    RecoveryMaintenanceReportDispositionV1::
                        kRebuiltRecognizedPartialTemporary &&
            publication.observed_candidate_count == 1U &&
            publication.activation_receipt != nullptr,
        "only the current exact-prefix partial tmp is cleaned and rebuilt");
}

void TestInvalidTemporaryRetained(
    TestContext* test) {
    RecoveringFixture fixture;
    if (!fixture.Initialize(test)) {
        return;
    }
    const std::string temporary =
        TemporaryName(fixture);
    test->Expect(
        WriteFileAt(
            fixture.maintenance_fd(),
            temporary,
            "not-canonical-json"),
        "writes an invalid current typed tmp");
    auto publication = Publish(&fixture);
    struct stat retained {};
    test->Expect(
        publication.error ==
                ingress::
                    RecoveryMaintenanceReportStoreErrorV1::
                        kCandidateConflict &&
            publication.activation_receipt == nullptr &&
            ::fstatat(
                fixture.maintenance_fd(),
                temporary.c_str(),
                &retained,
                AT_SYMLINK_NOFOLLOW) == 0 &&
            S_ISREG(retained.st_mode),
        "invalid/non-prefix tmp is fatal evidence and is never deleted");
}

void TestRealActivationChain(TestContext* test) {
    RecoveringFixture fixture;
    if (!fixture.Initialize(test)) {
        return;
    }
    auto publication = Publish(&fixture);
    test->Expect(
        publication.ok() &&
            publication.disposition ==
                ingress::
                    RecoveryMaintenanceReportDispositionV1::
                        kPublishedNew &&
            publication.observed_candidate_count == 0U &&
            publication.file_synced &&
            publication.directory_synced &&
            publication.filename ==
                fixture.report().filename() &&
            publication.report_sha256 ==
                fixture.report().report_sha256() &&
            publication.activation_receipt != nullptr &&
            fixture.IsStatus(
                ingress::ReserveRegistryStatusV1::
                    kRecovering),
        "real POSIX report publication completes all barriers and retains RECOVERING");
    ingress::RawReserveCoordinatorErrorV1 failure =
        ingress::RawReserveCoordinatorErrorV1::kNone;
    std::string error;
    auto active = fixture.coordinator()->
        PublishRecoveredActive(
            std::move(publication.activation_receipt),
            &failure,
            &error);
    test->Expect(
        active != nullptr &&
            failure ==
                ingress::RawReserveCoordinatorErrorV1::
                    kNone &&
            active->key() == fixture.key() &&
            active->writer_instance() ==
                fixture.writer_instance() &&
            fixture.IsStatus(
                ingress::ReserveRegistryStatusV1::
                    kActive),
        "opaque report receipt is the real RECOVERING-to-ACTIVE gate");
}

void TestStaleGenerationRejected(TestContext* test) {
    RecoveringFixture fixture;
    if (!fixture.Initialize(test)) {
        return;
    }
    auto publication = Publish(&fixture);
    if (publication.activation_receipt == nullptr) {
        test->Expect(
            false,
            "stale-generation fixture publishes receipt");
        return;
    }
    ingress::RawReserveWriterTakeoverV1 takeover{};
    takeover.key = fixture.key();
    takeover.new_writer_instance =
        Pattern<16U>(0x80U);
    std::string error;
    test->Expect(
        fixture.coordinator()->TakeoverRecovering(
            takeover, &error) ==
            ingress::RawReserveCoordinatorErrorV1::kNone,
        "takeover advances RECOVERING generation");
    ingress::RawReserveCoordinatorErrorV1 failure =
        ingress::RawReserveCoordinatorErrorV1::kNone;
    auto active = fixture.coordinator()->
        PublishRecoveredActive(
            std::move(publication.activation_receipt),
            &failure,
            &error);
    test->Expect(
        active == nullptr &&
            failure ==
                ingress::RawReserveCoordinatorErrorV1::
                    kActionGenerationChanged &&
            fixture.IsStatus(
                ingress::ReserveRegistryStatusV1::
                    kRecovering),
        "stale report receipt cannot activate a newer generation");
}

void TestFinalPathReplacementRejected(
    TestContext* test) {
    RecoveringFixture fixture;
    if (!fixture.Initialize(test)) {
        return;
    }
    auto publication = Publish(&fixture);
    if (publication.activation_receipt == nullptr) {
        test->Expect(false, "replacement fixture publishes receipt");
        return;
    }
    test->Expect(
        ReplaceFileAt(
            fixture.maintenance_fd(),
            std::string(fixture.report().filename()),
            fixture.report().canonical_jcs()),
        "same bytes replace final with a different inode");
    ingress::RawReserveCoordinatorErrorV1 failure =
        ingress::RawReserveCoordinatorErrorV1::kNone;
    std::string error;
    auto active = fixture.coordinator()->
        PublishRecoveredActive(
            std::move(publication.activation_receipt),
            &failure,
            &error);
    test->Expect(
        active == nullptr &&
            fixture.IsStatus(
                ingress::ReserveRegistryStatusV1::
                    kRecovering),
        "same-name same-bytes replacement inode cannot activate");
}

void TestFinalContentChangeRejected(
    TestContext* test) {
    RecoveringFixture fixture;
    if (!fixture.Initialize(test)) {
        return;
    }
    auto publication = Publish(&fixture);
    if (publication.activation_receipt == nullptr) {
        test->Expect(false, "content fixture publishes receipt");
        return;
    }
    std::string changed(
        fixture.report().canonical_jcs());
    changed[changed.size() / 2U] =
        changed[changed.size() / 2U] == '0'
            ? '1'
            : '0';
    test->Expect(
        RewriteFileAt(
            fixture.maintenance_fd(),
            std::string(fixture.report().filename()),
            changed),
        "mutates the retained final report inode");
    ingress::RawReserveCoordinatorErrorV1 failure =
        ingress::RawReserveCoordinatorErrorV1::kNone;
    std::string error;
    auto active = fixture.coordinator()->
        PublishRecoveredActive(
            std::move(publication.activation_receipt),
            &failure,
            &error);
    test->Expect(
        active == nullptr &&
            fixture.IsStatus(
                ingress::ReserveRegistryStatusV1::
                    kRecovering),
        "content-changed report cannot activate");
}

[[nodiscard]] bool MakeHistoricalReport(
    const ingress::BuiltRecoveryMaintenanceReportV1&
        current,
    std::uint32_t identity,
    std::string* filename,
    std::string* bytes) {
    if (filename == nullptr || bytes == nullptr) {
        return false;
    }
    ingress::RecoveryMaintenanceReportV1 model =
        current.model();
    model.recovery_attempt_id.fill(std::byte{0});
    model.recovery_attempt_id[0U] = std::byte{0x80};
    model.recovery_attempt_id[12U] =
        static_cast<std::byte>(
            (identity >> 24U) & 0xffU);
    model.recovery_attempt_id[13U] =
        static_cast<std::byte>(
            (identity >> 16U) & 0xffU);
    model.recovery_attempt_id[14U] =
        static_cast<std::byte>(
            (identity >> 8U) & 0xffU);
    model.recovery_attempt_id[15U] =
        static_cast<std::byte>(identity & 0xffU);
    return ingress::EncodeRecoveryMaintenanceReportV1Jcs(
               model, bytes) ==
               ingress::
                   RecoveryMaintenanceReportV1Error::
                       kNone &&
           ingress::RecoveryMaintenanceReportV1Filename(
               model, filename) ==
               ingress::
                   RecoveryMaintenanceReportV1Error::
                       kNone &&
           *filename != current.filename();
}

void TestCandidateAdmissionBound(TestContext* test) {
    RecoveringFixture fixture;
    if (!fixture.Initialize(test)) {
        return;
    }
    for (std::uint32_t index = 0U;
         index < 4095U;
         ++index) {
        std::string filename;
        std::string bytes;
        if (!MakeHistoricalReport(
                fixture.report(),
                index,
                &filename,
                &bytes) ||
            !WriteFileAt(
                fixture.maintenance_fd(),
                filename,
                bytes,
                false)) {
            test->Expect(
                false,
                "creates 4095 valid historical report candidates");
            return;
        }
    }
    auto publication = Publish(&fixture);
    test->Expect(
        publication.error ==
                ingress::
                    RecoveryMaintenanceReportStoreErrorV1::
                        kCandidateLimitExceeded &&
            publication.observed_candidate_count ==
                4095U &&
            publication.activation_receipt == nullptr &&
            fixture.IsStatus(
                ingress::ReserveRegistryStatusV1::
                    kRecovering),
        "4095 historical names reject an absent attempt before tmp+final creation");
    struct stat absent {};
    test->Expect(
        ::fstatat(
            fixture.maintenance_fd(),
            std::string(fixture.report().filename())
                .c_str(),
            &absent,
            AT_SYMLINK_NOFOLLOW) != 0 &&
            errno == ENOENT,
        "candidate admission rejection performs no report mutation");
}

}  // namespace

int main() {
    TestContext test{};
    TestCompleteTemporaryAdoption(&test);
    TestRecognizedPartialRebuild(&test);
    TestInvalidTemporaryRetained(&test);
    TestRealActivationChain(&test);
    TestStaleGenerationRejected(&test);
    TestFinalPathReplacementRejected(&test);
    TestFinalContentChangeRejected(&test);
    TestCandidateAdmissionBound(&test);
    if (test.failures != 0) {
        std::cerr << test.failures
                  << " recovery report store tests failed\n";
        return 1;
    }
    std::cout
        << "Phase 2 recovery maintenance report store tests passed\n";
    return 0;
}
