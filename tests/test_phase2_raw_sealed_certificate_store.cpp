#include "l2flow/common/sha256.h"
#include "l2flow/ingress/raw_manifest_transition.h"
#include "l2flow/ingress/raw_namespace.h"
#include "l2flow/ingress/raw_recovery_maintenance_report_store.h"
#include "l2flow/ingress/raw_reserve_coordinator.h"
#include "l2flow/ingress/raw_schema.h"
#include "l2flow/ingress/raw_sealed_certificate_store.h"

#include <algorithm>
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
    SealedRawCertificatePublishResultV1 (*)(
        const ingress::RawWriterLease&,
        std::unique_ptr<
            ingress::RawReserveAuthorizedActionV1>,
        const ingress::BuiltSealedRawCertificateV1&,
        std::string*) noexcept;

static_assert(
    std::is_same_v<
        decltype(
            &ingress::PublishSealedRawCertificateV1),
        PublishFunction>);

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
            "/tmp/l2flow-sealed-store-XXXXXX";
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

ingress::RawWalWriterSnapshot MakeInitialized() {
    ingress::RawWalWriterSnapshot snapshot{};
    snapshot.append = {
        ingress::kRawV1SegmentHeaderBytes,
        0U,
        ingress::kRawV1SegmentHeaderBytes};
    snapshot.durable = snapshot.append;
    snapshot.journal_logical_size =
        ingress::kRawV1JournalHeaderBytes +
        ingress::kRawV1DurableMarkerBytes;
    snapshot.initialized = true;
    return snapshot;
}

ingress::RawSealedSegmentMetadataV1
MakeSealedMetadata(
    const ingress::SegmentHeaderV1& segment) {
    ingress::RawV1SegmentHeaderWire header_bytes{};
    static_cast<void>(
        ingress::EncodeSegmentHeaderV1(
            segment, &header_bytes));
    ingress::RawSealedSegmentMetadataV1 metadata{};
    metadata.segment = segment;
    metadata.segment_sha256 =
        common::ComputeSha256(header_bytes);
    metadata.index_sha256 =
        Pattern<32U>(0xe1U);
    metadata.logical_end_offset =
        ingress::kRawV1SegmentHeaderBytes;

    ingress::DurableMarkerV1 marker{};
    marker.source_stream_id =
        segment.source_stream_id;
    marker.segment_sequence =
        segment.segment_sequence;
    marker.durable_global_wal_pos =
        ingress::kRawV1SegmentHeaderBytes;
    marker.durable_ingress_sequence = 0U;
    marker.durable_segment_offset =
        ingress::kRawV1SegmentHeaderBytes;
    marker.marker_flags =
        ingress::kRawV1SegmentSealed;
    static_cast<void>(
        ingress::EncodeDurableMarkerV1(
            marker,
            &metadata.accepted_sealed_marker_bytes));
    static_cast<void>(
        ingress::DecodeDurableMarkerV1(
            metadata.accepted_sealed_marker_bytes,
            &metadata.accepted_sealed_marker));
    return metadata;
}

ingress::RawV1JournalHeaderWire MakeJournal(
    const ingress::SegmentHeaderV1& segment) {
    ingress::DurableJournalHeaderV1 journal{};
    journal.capture_date = segment.capture_date;
    journal.source_stream_id =
        segment.source_stream_id;
    journal.stream_day_id =
        segment.stream_day_id;
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

std::unique_ptr<ingress::BuiltSealedRawCertificateV1>
MakeCertificate(
    const ingress::RawReserveRegistryEntryKeyV1& key,
    const ingress::RawV1Identity& writer_instance) {
    const ingress::SegmentHeaderV1 segment =
        MakeSegment(key);
    ingress::RawManifestV1 open{};
    if (ingress::BuildFreshOpenRawManifestV1(
            segment, MakeInitialized(), &open) !=
        ingress::RawManifestTransitionErrorV1::kNone) {
        return nullptr;
    }
    ingress::RawManifestV1 closed{};
    if (ingress::TransitionOpenRawManifestToClosedV1(
            open,
            MakeSealedMetadata(segment),
            &closed) !=
        ingress::RawManifestTransitionErrorV1::kNone) {
        return nullptr;
    }

    ingress::RawWalSinkIdentityV1 sink{};
    sink.writer_instance = writer_instance;
    sink.stream_day_id = key.stream_day_id;
    sink.source_stream_id =
        key.route.source_stream_id;
    sink.capture_date = key.route.capture_date;
    sink.segment_sequence = 1U;
    sink.segment_base_wal_pos = 0U;
    sink.first_ingress_sequence = 1U;

    ingress::RawWalWriterSnapshot final_wal{};
    final_wal.append = {
        ingress::kRawV1SegmentHeaderBytes,
        0U,
        ingress::kRawV1SegmentHeaderBytes};
    final_wal.durable = final_wal.append;
    final_wal.journal_logical_size =
        ingress::kRawV1JournalHeaderBytes +
        2U * ingress::kRawV1DurableMarkerBytes;
    final_wal.initialized = true;
    final_wal.sealed = true;
    final_wal.closed = true;

    std::unique_ptr<
        ingress::BuiltSealedRawCertificateV1>
        certificate;
    if (ingress::
            BuildSealedRawCertificateCapabilityV1(
                MakeJournal(segment),
                closed,
                sink,
                final_wal,
                &certificate) !=
        ingress::SealedRawCertificateV1Error::kNone) {
        return nullptr;
    }
    return certificate;
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

[[nodiscard]] bool RemoveFileAt(
    int directory_fd,
    const std::string& name) {
    return ::unlinkat(
               directory_fd, name.c_str(), 0) == 0 &&
           ::fsync(directory_fd) == 0;
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

class ActiveFixture final {
public:
    [[nodiscard]] bool Initialize(
        TestContext* test,
        bool recovering_terminal = false) {
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
            "store fixture creates a private canonical Raw route");
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
            "store fixture acquires the exact Raw writer lease");
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
                "store fixture durably creates maintenance");
            return false;
        }
        maintenance_fd_ = ::openat(
            stream_directory_->descriptor(),
            "maintenance",
            O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
                O_CLOEXEC | O_NOATIME);
        test->Expect(
            maintenance_fd_ >= 0,
            "store fixture retains maintenance directory");
        if (maintenance_fd_ < 0) {
            return false;
        }
        if (recovering_terminal) {
            const ingress::RawV1JournalHeaderWire
                journal =
                    MakeJournal(MakeSegment(key_));
            std::string journal_bytes(
                reinterpret_cast<const char*>(
                    journal.data()),
                journal.size());
            journal_bytes.resize(
                ingress::kRawV1JournalHeaderBytes +
                    2U *
                        ingress::
                            kRawV1DurableMarkerBytes,
                '\0');
            if (!WriteFileAt(
                    stream_directory_->descriptor(),
                    ingress::kRawJournalFilename,
                    journal_bytes) ||
                ::fsync(
                    stream_directory_->descriptor()) !=
                    0) {
                test->Expect(
                    false,
                    "recovering fixture persists its exact retained journal inode");
                return false;
            }
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
            "store fixture publishes the durable coordinator");
        if (coordinator_ == nullptr) {
            std::cerr << error << '\n';
            return false;
        }

        bool registered = false;
        if (recovering_terminal) {
            ingress::RawReserveExistingAnchorRecoveryV1
                registration{};
            registration.key = key_;
            registration.writer_instance =
                writer_instance_;
            registration.recovery_intent =
                ingress::ReserveRecoveryIntentV1::
                    kRecoverSealOnly;
            registration.safe_stop_template_id = 7U;
            registered =
                coordinator_->
                    RegisterExistingAnchorRecovering(
                        registration,
                        &error) ==
                ingress::
                    RawReserveCoordinatorErrorV1::kNone;
        } else {
            ingress::RawReserveFreshScaffoldingV1
                registration{};
            registration.key = key_;
            registration.writer_instance =
                writer_instance_;
            registration.recovery_intent =
                ingress::ReserveRecoveryIntentV1::
                    kResumeConnect;
            registration.scaffolding_allocation_cap =
                4096U;
            registration.safe_stop_template_id = 7U;
            registered =
                coordinator_->
                        RegisterFreshScaffolding(
                            registration,
                            &error) ==
                    ingress::
                        RawReserveCoordinatorErrorV1::
                            kNone &&
                coordinator_->PublishInit(
                    key_, &error) ==
                    ingress::
                        RawReserveCoordinatorErrorV1::
                            kNone &&
                coordinator_->PublishActive(
                    key_, &error) ==
                    ingress::
                        RawReserveCoordinatorErrorV1::
                            kNone;
        }
        test->Expect(
            registered,
            recovering_terminal
                ? "store fixture reaches durable RECOVERING+RECOVER_SEAL_ONLY"
                : "store fixture reaches durable ACTIVE");
        if (coordinator_->state()
                .slots[
                    coordinator_->state()
                        .selected_slot]
                .entry_count != 1U) {
            std::cerr << error << '\n';
            return false;
        }
        certificate_ =
            MakeCertificate(
                key_, writer_instance_);
        test->Expect(
            certificate_ != nullptr,
            "store fixture builds a terminal publication capability");
        return certificate_ != nullptr;
    }

    ~ActiveFixture() {
        if (maintenance_fd_ >= 0) {
            static_cast<void>(
                ::close(maintenance_fd_));
        }
    }

    ActiveFixture() = default;
    ActiveFixture(const ActiveFixture&) = delete;
    ActiveFixture& operator=(
        const ActiveFixture&) = delete;

    [[nodiscard]] std::unique_ptr<
        ingress::RawReserveAuthorizedActionV1>
    AcquireActive() {
        ingress::RawReserveCoordinatorErrorV1 failure =
            ingress::RawReserveCoordinatorErrorV1::
                kNone;
        std::string error;
        return coordinator_->
            AcquireActionForExistingRoute(
                key_,
                ingress::ReserveRegistryStatusV1::
                    kActive,
                slug_,
                &failure,
                &error);
    }

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

    [[nodiscard]] bool IsUnregistered() const {
        const auto state = coordinator_->state();
        return state
                   .slots[state.selected_slot]
                   .entry_count == 0U;
    }

    [[nodiscard]] int maintenance_fd()
        const noexcept {
        return maintenance_fd_;
    }
    [[nodiscard]] ingress::RawWriterLease&
    lease() noexcept {
        return *lease_;
    }
    [[nodiscard]] const ingress::
        BuiltSealedRawCertificateV1&
    certificate() const noexcept {
        return *certificate_;
    }
    [[nodiscard]] std::shared_ptr<
        ingress::RawReserveRegistryCoordinatorV1>
    coordinator() const noexcept {
        return coordinator_;
    }
    [[nodiscard]] const ingress::
        RawReserveRegistryEntryKeyV1&
    key() const noexcept {
        return key_;
    }
    [[nodiscard]] int route_fd() const noexcept {
        return stream_directory_->descriptor();
    }
    [[nodiscard]] int root_fd() const noexcept {
        return root_.fd();
    }
    [[nodiscard]] const std::string&
    slug() const noexcept {
        return slug_;
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
        ingress::BuiltSealedRawCertificateV1>
        certificate_;
    int maintenance_fd_ = -1;
};

[[nodiscard]] bool MakeHistoricalCertificate(
    const ingress::BuiltSealedRawCertificateV1&
        current,
    std::uint32_t identity,
    std::string* filename,
    std::string* bytes) {
    if (filename == nullptr || bytes == nullptr) {
        return false;
    }
    ingress::SealedRawCertificateV1 model =
        current.model();
    model.closed_prefix_sha256.fill(std::byte{0});
    model.closed_prefix_sha256[0U] =
        std::byte{0x80U};
    model.closed_prefix_sha256[28U] =
        static_cast<std::byte>(
            (identity >> 24U) & 0xffU);
    model.closed_prefix_sha256[29U] =
        static_cast<std::byte>(
            (identity >> 16U) & 0xffU);
    model.closed_prefix_sha256[30U] =
        static_cast<std::byte>(
            (identity >> 8U) & 0xffU);
    model.closed_prefix_sha256[31U] =
        static_cast<std::byte>(
            identity & 0xffU);
    return ingress::EncodeSealedRawCertificateV1Jcs(
               model, bytes) ==
               ingress::SealedRawCertificateV1Error::
                   kNone &&
           ingress::SealedRawCertificateV1Filename(
               model, filename) ==
               ingress::SealedRawCertificateV1Error::
                   kNone &&
           *filename != current.filename();
}

std::unique_ptr<
    ingress::BuiltRecoveryMaintenanceReportV1>
MakeSealedTerminalReport(
    const ActiveFixture& fixture) {
    const ingress::SegmentHeaderV1 segment =
        MakeSegment(fixture.key());
    const ingress::RawV1JournalHeaderWire journal =
        MakeJournal(segment);
    const ingress::SealedRawCertificateV1& certificate =
        fixture.certificate().model();
    ingress::RawRecoveryPlanV1 analysis{};
    if (ingress::DecodeDurableJournalHeaderV1(
            journal, &analysis.journal_header) !=
        ingress::RawV1Error::kNone) {
        return nullptr;
    }
    analysis.accepted_journal_size =
        ingress::kRawV1JournalHeaderBytes +
        2U * ingress::kRawV1DurableMarkerBytes;
    analysis.has_accepted_cursor = true;
    analysis.accepted_cursor = {
        certificate.last_segment_sequence,
        certificate.terminal_durable_cursor
            .global_wal_pos,
        certificate.terminal_durable_cursor
            .ingress_sequence,
        certificate.terminal_durable_cursor
            .segment_offset,
        ingress::kRawV1SegmentSealed};
    ingress::RawRecoverySegmentPlanV1 terminal{};
    terminal.segment_sequence =
        certificate.last_segment_sequence;
    terminal.segment_base_wal_pos =
        certificate.last_segment_base_wal_pos;
    terminal.has_accepted_marker = true;
    terminal.accepted_marker_wire =
        certificate.accepted_sealed_marker_bytes;
    terminal.durable_end_offset =
        certificate.last_segment_logical_length;
    terminal.validated_logical_end_offset =
        certificate.last_segment_logical_length;
    terminal.validated_last_ingress_sequence =
        certificate.terminal_durable_cursor
            .ingress_sequence;
    terminal.append_only_begin_offset =
        certificate.last_segment_logical_length;
    terminal.append_only_end_offset =
        certificate.last_segment_logical_length;
    terminal.tail_begin_offset =
        certificate.last_segment_logical_length;
    terminal.tail_end_offset =
        certificate.last_segment_logical_length;
    terminal.sealed = true;
    analysis.segments.push_back(terminal);

    ingress::RawRecoveryExecutionResultV1 execution{};
    execution.cursor_publishable = true;
    execution.retained_journal_size =
        analysis.accepted_journal_size;
    execution.recovered_cursor =
        analysis.accepted_cursor;
    std::unique_ptr<
        ingress::BuiltRecoveryMaintenanceReportV1>
        report;
    if (ingress::
            BuildSealedRawRecoveryMaintenanceReportV1(
                fixture.key(),
                journal,
                analysis,
                execution,
                analysis.accepted_journal_size,
                fixture.certificate(),
                &report) !=
        ingress::RecoveryMaintenanceReportV1Error::
            kNone) {
        return nullptr;
    }
    return report;
}

void TestPublicationRecoveryAndReceipt(
    TestContext* test) {
    ActiveFixture fixture;
    if (!fixture.Initialize(test)) {
        return;
    }
    std::string diagnostic;
    auto result =
        ingress::PublishSealedRawCertificateV1(
            fixture.lease(),
            fixture.AcquireActive(),
            fixture.certificate(),
            &diagnostic);
    test->Expect(
        result.ok() &&
            result.disposition ==
                ingress::
                    SealedRawCertificateDispositionV1::
                        kPublishedNew &&
            result.observed_candidate_count == 0U &&
            result.file_synced &&
            result.directory_synced &&
            result.filename ==
                fixture.certificate().filename() &&
            result.certificate_sha256 ==
                fixture.certificate()
                    .certificate_sha256() &&
            result.unregister_receipt != nullptr,
        "real POSIX publication completes all barriers and returns an opaque receipt");

    const std::string final_name(
        fixture.certificate().filename());
    const std::string temporary_name =
        "." + final_name +
        std::string(
            ingress::
                kSealedRawCertificateV1TemporarySuffix);
    struct stat status {};
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
                fixture.certificate().canonical_jcs() &&
            ::fstatat(
                fixture.maintenance_fd(),
                temporary_name.c_str(),
                &status,
                AT_SYMLINK_NOFOLLOW) != 0 &&
            errno == ENOENT,
        "published final is owner-only exact JCS and consumes its typed tmp");

    std::string historical_name;
    std::string historical_bytes;
    test->Expect(
        MakeHistoricalCertificate(
            fixture.certificate(),
            9001U,
            &historical_name,
            &historical_bytes),
        "historical candidate fixture encodes");
    const std::string historical_temporary =
        "." + historical_name +
        std::string(
            ingress::
                kSealedRawCertificateV1TemporarySuffix);
    test->Expect(
        WriteFileAt(
            fixture.maintenance_fd(),
            historical_temporary,
            "{}"),
        "foreign invalid typed tmp is created");
    auto rejected =
        ingress::PublishSealedRawCertificateV1(
            fixture.lease(),
            fixture.AcquireActive(),
            fixture.certificate(),
            &diagnostic);
    test->Expect(
        rejected.error ==
                ingress::
                    SealedRawCertificateStoreErrorV1::
                        kCandidateConflict &&
            rejected.unregister_receipt == nullptr &&
            ::fstatat(
                fixture.maintenance_fd(),
                historical_temporary.c_str(),
                &status,
                AT_SYMLINK_NOFOLLOW) == 0,
        "foreign invalid tmp fails closed and is never deleted");
    test->Expect(
        RemoveFileAt(
            fixture.maintenance_fd(),
            historical_temporary),
        "foreign invalid tmp fixture is removed explicitly");

    test->Expect(
        ::symlinkat(
            "missing-target",
            fixture.maintenance_fd(),
            historical_name.c_str()) == 0,
        "syntactic symlink candidate is created");
    rejected =
        ingress::PublishSealedRawCertificateV1(
            fixture.lease(),
            fixture.AcquireActive(),
            fixture.certificate(),
            &diagnostic);
    test->Expect(
        rejected.error ==
                ingress::
                    SealedRawCertificateStoreErrorV1::
                        kUnsafeCandidate &&
            rejected.unregister_receipt == nullptr,
        "symlink certificate candidate is rejected without traversal");
    test->Expect(
        RemoveFileAt(
            fixture.maintenance_fd(),
            historical_name),
        "symlink fixture is removed explicitly");

    ingress::SealedRawCertificateV1 pair_other =
        fixture.certificate().model();
    pair_other.closed_prefix_sha256.fill(
        std::byte{0});
    pair_other.closed_prefix_sha256[0U] =
        std::byte{0x81U};
    std::string pair_final_bytes;
    std::string pair_name;
    test->Expect(
        ingress::EncodeSealedRawCertificateV1Jcs(
            pair_other, &pair_final_bytes) ==
                ingress::SealedRawCertificateV1Error::
                    kNone &&
            ingress::SealedRawCertificateV1Filename(
                pair_other, &pair_name) ==
                ingress::SealedRawCertificateV1Error::
                    kNone,
        "historical final/tmp pair fixture encodes");
    ingress::SealedRawCertificateV1 pair_mismatch =
        pair_other;
    pair_mismatch.journal_header_sha256[0U] ^=
        std::byte{0x01U};
    std::string pair_temporary_bytes;
    const std::string pair_temporary_name =
        "." + pair_name +
        std::string(
            ingress::
                kSealedRawCertificateV1TemporarySuffix);
    test->Expect(
        ingress::EncodeSealedRawCertificateV1Jcs(
            pair_mismatch,
            &pair_temporary_bytes) ==
                ingress::SealedRawCertificateV1Error::
                    kNone &&
            WriteFileAt(
                fixture.maintenance_fd(),
                pair_name,
                pair_final_bytes) &&
            WriteFileAt(
                fixture.maintenance_fd(),
                pair_temporary_name,
                pair_temporary_bytes),
        "historical mismatched complete pair is created");
    rejected =
        ingress::PublishSealedRawCertificateV1(
            fixture.lease(),
            fixture.AcquireActive(),
            fixture.certificate(),
            &diagnostic);
    test->Expect(
        rejected.error ==
                ingress::
                    SealedRawCertificateStoreErrorV1::
                        kCandidateConflict &&
            rejected.unregister_receipt == nullptr,
        "same-locator historical final/tmp must have identical content hashes");
    test->Expect(
        RemoveFileAt(
            fixture.maintenance_fd(),
            pair_name) &&
            RemoveFileAt(
                fixture.maintenance_fd(),
                pair_temporary_name),
        "mismatched historical pair is removed explicitly");

    test->Expect(
        WriteFileAt(
            fixture.maintenance_fd(),
            temporary_name,
            fixture.certificate().canonical_jcs()),
        "identical current complete tmp is created");
    result = ingress::PublishSealedRawCertificateV1(
        fixture.lease(),
        fixture.AcquireActive(),
        fixture.certificate(),
        &diagnostic);
    test->Expect(
        result.ok() &&
            result.disposition ==
                ingress::
                    SealedRawCertificateDispositionV1::
                        kAcceptedExistingAndCleanedIdenticalTemporary &&
            ::fstatat(
                fixture.maintenance_fd(),
                temporary_name.c_str(),
                &status,
                AT_SYMLINK_NOFOLLOW) != 0 &&
            errno == ENOENT,
        "existing exact final cleans only its identical complete tmp");

    test->Expect(
        RemoveFileAt(
            fixture.maintenance_fd(),
            final_name) &&
            WriteFileAt(
                fixture.maintenance_fd(),
                temporary_name,
                fixture.certificate()
                    .canonical_jcs()
                    .substr(
                        0U,
                        fixture.certificate()
                                .canonical_jcs()
                                .size() /
                            3U)),
        "short exact-prefix crash tmp is created");
    result = ingress::PublishSealedRawCertificateV1(
        fixture.lease(),
        fixture.AcquireActive(),
        fixture.certificate(),
        &diagnostic);
    test->Expect(
        result.ok() &&
            result.disposition ==
                ingress::
                    SealedRawCertificateDispositionV1::
                        kRebuiltRecognizedPartialTemporary &&
            ReadFileAt(
                fixture.maintenance_fd(),
                final_name) ==
                fixture.certificate().canonical_jcs(),
        "recognized short current tmp is unlinked, dir-synced and rebuilt");

    test->Expect(
        RemoveFileAt(
            fixture.maintenance_fd(),
            final_name) &&
            WriteFileAt(
                fixture.maintenance_fd(),
                temporary_name,
                {}),
        "zero-byte deterministic crash tmp is created");
    result = ingress::PublishSealedRawCertificateV1(
        fixture.lease(),
        fixture.AcquireActive(),
        fixture.certificate(),
        &diagnostic);
    test->Expect(
        result.ok() &&
            result.disposition ==
                ingress::
                    SealedRawCertificateDispositionV1::
                        kRebuiltRecognizedPartialTemporary,
        "recognized zero-byte current tmp is safely rebuilt");

    test->Expect(
        RemoveFileAt(
            fixture.maintenance_fd(),
            final_name) &&
            WriteFileAt(
                fixture.maintenance_fd(),
                temporary_name,
                fixture.certificate().canonical_jcs()),
        "complete deterministic crash tmp is created");
    result = ingress::PublishSealedRawCertificateV1(
        fixture.lease(),
        fixture.AcquireActive(),
        fixture.certificate(),
        &diagnostic);
    test->Expect(
        result.ok() &&
            result.disposition ==
                ingress::
                    SealedRawCertificateDispositionV1::
                        kAdoptedCompleteTemporary &&
            result.unregister_receipt != nullptr,
        "complete current tmp is file-synced and adopted with NOREPLACE");

    auto receipt =
        std::move(result.unregister_receipt);
    std::string error;
    test->Expect(
        fixture.coordinator()->UnregisterActive(
            std::move(receipt),
            &error) ==
                ingress::
                    RawReserveCoordinatorErrorV1::
                        kNone,
        "opaque retained-fd receipt authorizes ACTIVE unregister after store released its action");
    const auto state =
        fixture.coordinator()->state();
    const auto& selected =
        state.slots[state.selected_slot];
    test->Expect(
        selected.generation == 5U &&
            selected.entry_count == 0U,
        "successful receipt consumption removes the ACTIVE registry entry");
}

void TestCandidateAdmissionBound(
    TestContext* test) {
    ActiveFixture fixture;
    if (!fixture.Initialize(test)) {
        return;
    }
    std::string first_historical;
    for (std::uint32_t identity = 1U;
         identity <=
             ingress::
                 kSealedRawCertificateV1MaximumCandidates -
                 1U;
         ++identity) {
        std::string filename;
        std::string bytes;
        if (!MakeHistoricalCertificate(
                fixture.certificate(),
                identity,
                &filename,
                &bytes) ||
            !WriteFileAt(
                fixture.maintenance_fd(),
                filename,
                bytes,
                false)) {
            test->Expect(
                false,
                "4095 exact historical candidates are created");
            return;
        }
        if (first_historical.empty()) {
            first_historical = filename;
        }
    }
    static_cast<void>(
        ::fsync(fixture.maintenance_fd()));

    std::string diagnostic;
    auto result =
        ingress::PublishSealedRawCertificateV1(
            fixture.lease(),
            fixture.AcquireActive(),
            fixture.certificate(),
            &diagnostic);
    test->Expect(
        result.error ==
                ingress::
                    SealedRawCertificateStoreErrorV1::
                        kCandidateLimitExceeded &&
            result.observed_candidate_count == 4095U &&
            result.unregister_receipt == nullptr,
        "4095 existing names reject an absent target that must reserve tmp+final");

    const std::string temporary_name =
        "." +
        std::string(
            fixture.certificate().filename()) +
        std::string(
            ingress::
                kSealedRawCertificateV1TemporarySuffix);
    test->Expect(
        WriteFileAt(
            fixture.maintenance_fd(),
            temporary_name,
            fixture.certificate().canonical_jcs()),
        "complete current tmp raises observed count to 4096");
    result = ingress::PublishSealedRawCertificateV1(
        fixture.lease(),
        fixture.AcquireActive(),
        fixture.certificate(),
        &diagnostic);
    test->Expect(
        result.error ==
                ingress::
                    SealedRawCertificateStoreErrorV1::
                        kCandidateLimitExceeded &&
            result.observed_candidate_count == 4096U &&
            result.unregister_receipt == nullptr,
        "tmp-only adoption at count 4096 reserves one possible final name and rejects");

    test->Expect(
        RemoveFileAt(
            fixture.maintenance_fd(),
            first_historical),
        "one historical candidate is retired explicitly");
    result = ingress::PublishSealedRawCertificateV1(
        fixture.lease(),
        fixture.AcquireActive(),
        fixture.certificate(),
        &diagnostic);
    test->Expect(
        result.ok() &&
            result.observed_candidate_count == 4095U &&
            result.disposition ==
                ingress::
                    SealedRawCertificateDispositionV1::
                        kAdoptedCompleteTemporary,
        "tmp-only adoption at count 4095 is admitted exactly at the bound");
}

void TestRouteReplacementAndStaleReceipt(
    TestContext* test) {
    {
        ActiveFixture fixture;
        if (!fixture.Initialize(test)) {
            return;
        }
        auto action = fixture.AcquireActive();
        const std::string date_name =
            "capture_date=" +
            std::to_string(
                fixture.key().route.capture_date);
        const std::string route_name =
            "stream=" +
            std::to_string(
                fixture.key().route.source_stream_id) +
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
            ingress::PublishSealedRawCertificateV1(
                fixture.lease(),
                std::move(action),
                fixture.certificate(),
                &diagnostic);
        test->Expect(
            moved &&
                result.error ==
                    ingress::
                        SealedRawCertificateStoreErrorV1::
                            kAuthorizationRejected &&
                result.unregister_receipt == nullptr,
            "canonical route replacement is rejected before certificate mutation or receipt generation");
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
            "canonical route fixture is restored");
    }

    {
        ActiveFixture fixture;
        if (!fixture.Initialize(test)) {
            return;
        }
        std::string diagnostic;
        auto result =
            ingress::PublishSealedRawCertificateV1(
                fixture.lease(),
                fixture.AcquireActive(),
                fixture.certificate(),
                &diagnostic);
        auto replaced_receipt =
            std::move(result.unregister_receipt);
        const std::string final_name(
            fixture.certificate().filename());
        test->Expect(
            result.ok() &&
                replaced_receipt != nullptr &&
                RemoveFileAt(
                    fixture.maintenance_fd(),
                    final_name) &&
                WriteFileAt(
                    fixture.maintenance_fd(),
                    final_name,
                    fixture.certificate()
                        .canonical_jcs()),
            "same-content replacement final is created after receipt issuance");
        std::string error;
        test->Expect(
            fixture.coordinator()->UnregisterActive(
                std::move(replaced_receipt),
                &error) ==
                ingress::
                    RawReserveCoordinatorErrorV1::
                        kTargetIdentityChanged,
            "retained receipt fd rejects same-bytes replacement inode before unregister");
        const auto state =
            fixture.coordinator()->state();
        const auto& selected =
            state.slots[state.selected_slot];
        test->Expect(
            selected.entry_count == 1U &&
                selected.entries[0U].registry_status ==
                    ingress::
                        ReserveRegistryStatusV1::kActive,
            "replacement-inode rejection preserves ACTIVE");
    }

    {
        ActiveFixture fixture;
        if (!fixture.Initialize(test)) {
            return;
        }
        std::string diagnostic;
        auto result =
            ingress::PublishSealedRawCertificateV1(
                fixture.lease(),
                fixture.AcquireActive(),
                fixture.certificate(),
                &diagnostic);
        auto stale_receipt =
            std::move(result.unregister_receipt);
        ingress::RawReserveActiveTakeoverV1 takeover{};
        takeover.old_key = fixture.key();
        takeover.new_writer_instance =
            Pattern<16U>(0x15U);
        takeover.new_recovery_attempt_id =
            Pattern<16U>(0x25U);
        takeover.recovery_intent =
            ingress::ReserveRecoveryIntentV1::
                kResumeConnect;
        std::string error;
        test->Expect(
            result.ok() &&
                stale_receipt != nullptr &&
                fixture.coordinator()->TakeoverActive(
                    takeover,
                    &error) ==
                    ingress::
                        RawReserveCoordinatorErrorV1::
                            kNone,
            "ACTIVE generation advances after receipt issuance");
        test->Expect(
            fixture.coordinator()->UnregisterActive(
                std::move(stale_receipt),
                &error) ==
                ingress::
                    RawReserveCoordinatorErrorV1::
                        kActionGenerationChanged,
            "stale publication receipt cannot unregister a newer generation");
        const auto state =
            fixture.coordinator()->state();
        const auto& selected =
            state.slots[state.selected_slot];
        test->Expect(
            selected.entry_count == 1U &&
                selected.entries[0U].registry_status ==
                    ingress::
                        ReserveRegistryStatusV1::
                            kRecovering,
            "rejected stale receipt preserves the newer RECOVERING entry");
    }
}

void TestRecoveredSealedTerminalLifecycle(
    TestContext* test) {
    ActiveFixture fixture;
    if (!fixture.Initialize(test, true)) {
        return;
    }
    std::string diagnostic;
    auto sidecar =
        ingress::PublishSealedRawCertificateV1(
            fixture.lease(),
            fixture.AcquireRecovering(),
            fixture.certificate(),
            &diagnostic);
    test->Expect(
        sidecar.ok() &&
            sidecar.unregister_receipt == nullptr &&
            sidecar.recovery_terminal_receipt !=
                nullptr,
        "RECOVERING+RECOVER_SEAL_ONLY certificate publication returns only its distinct terminal receipt");
    auto report = MakeSealedTerminalReport(fixture);
    test->Expect(
        report != nullptr,
        "sealed recovery fixture builds the exact SEALED_RAW report");
    if (sidecar.recovery_terminal_receipt == nullptr ||
        report == nullptr) {
        std::cerr << diagnostic << '\n';
        return;
    }
    auto report_publication =
        ingress::PublishRecoveryMaintenanceReportV1(
            fixture.lease(),
            fixture.AcquireRecovering(),
            *report,
            std::move(
                sidecar.recovery_terminal_receipt),
            &diagnostic);
    test->Expect(
        report_publication.ok() &&
            report_publication.activation_receipt ==
                nullptr &&
            report_publication.terminal_receipt !=
                nullptr,
        "SEALED_RAW consumes the sealed terminal sidecar receipt and crosses the later report barrier");
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
        "SEALED_RAW terminal receipt closes exact RECOVERING under the exclusive generation gate");
}

}  // namespace

int main() {
    TestContext test;
    TestPublicationRecoveryAndReceipt(&test);
    TestCandidateAdmissionBound(&test);
    TestRouteReplacementAndStaleReceipt(&test);
    TestRecoveredSealedTerminalLifecycle(&test);
    if (test.failures != 0) {
        std::cerr << test.failures
                  << " sealed-certificate store assertion(s) failed\n";
        return 1;
    }
    std::cout
        << "Phase 2 sealed Raw certificate POSIX store checks passed\n";
    return 0;
}
