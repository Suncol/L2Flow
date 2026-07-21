#include "l2flow/common/sha256.h"
#include "l2flow/ingress/raw_clean_stop_gate.h"
#include "l2flow/ingress/raw_manifest_store.h"
#include "l2flow/ingress/raw_manifest_transition.h"
#include "l2flow/ingress/raw_namespace.h"
#include "l2flow/ingress/raw_schema.h"
#include "l2flow/ingress/raw_sealed_certificate_v1.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace common = l2flow::common;
namespace ingress = l2flow::ingress;

namespace {

constexpr std::size_t kMaximumManifestBytes =
    1024U * 1024U;

struct TestContext final {
    void Expect(bool condition, std::string_view message) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << message << '\n';
        }
    }

    int failures = 0;
};

class TempDirectory final {
public:
    TempDirectory() {
        char pattern[] =
            "/tmp/l2flow-clean-stop-gate-XXXXXX";
        char* const created = ::mkdtemp(pattern);
        if (created == nullptr) {
            return;
        }
        path_ = created;
        static_cast<void>(::chmod(path_.c_str(), 0700));
        descriptor_ = ::open(
            path_.c_str(),
            O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
                O_NONBLOCK | O_CLOEXEC | O_NOATIME);
    }

    ~TempDirectory() {
        if (descriptor_ >= 0) {
            static_cast<void>(::close(descriptor_));
        }
        if (!path_.empty()) {
            std::error_code ignored;
            static_cast<void>(
                std::filesystem::remove_all(
                    path_, ignored));
        }
    }

    TempDirectory(const TempDirectory&) = delete;
    TempDirectory& operator=(const TempDirectory&) =
        delete;

    [[nodiscard]] int descriptor() const noexcept {
        return descriptor_;
    }

    [[nodiscard]] const std::string& path() const noexcept {
        return path_;
    }

private:
    std::string path_;
    int descriptor_ = -1;
};

template <std::size_t Size>
std::array<std::byte, Size> Pattern(
    std::uint8_t seed) {
    std::array<std::byte, Size> result{};
    for (std::size_t index = 0U; index < Size; ++index) {
        result[index] = static_cast<std::byte>(
            static_cast<std::uint8_t>(
                seed +
                static_cast<std::uint8_t>(index)));
    }
    return result;
}

[[nodiscard]] bool WriteAll(
    int descriptor,
    std::uint64_t offset,
    std::span<const std::byte> bytes) {
    std::size_t completed = 0U;
    while (completed < bytes.size()) {
        if (offset >
                static_cast<std::uint64_t>(
                    std::numeric_limits<off_t>::max()) ||
            completed >
                static_cast<std::size_t>(
                    std::numeric_limits<off_t>::max()) ||
            offset >
                static_cast<std::uint64_t>(
                    std::numeric_limits<off_t>::max()) -
                    static_cast<std::uint64_t>(completed)) {
            return false;
        }
        const ssize_t result = ::pwrite(
            descriptor,
            bytes.data() +
                static_cast<std::ptrdiff_t>(completed),
            bytes.size() - completed,
            static_cast<off_t>(
                offset +
                static_cast<std::uint64_t>(
                    completed)));
        if (result < 0 && errno == EINTR) {
            continue;
        }
        if (result <= 0) {
            return false;
        }
        completed += static_cast<std::size_t>(result);
    }
    return true;
}

[[nodiscard]] int CreateFileAt(
    int directory_fd,
    const char* name,
    std::span<const std::byte> bytes) {
    int descriptor = -1;
    do {
        descriptor = ::openat(
            directory_fd,
            name,
            O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW |
                O_NONBLOCK | O_CLOEXEC,
            0600);
    } while (descriptor < 0 && errno == EINTR);
    if (descriptor < 0) {
        return -1;
    }
    const bool okay =
        ::fchmod(descriptor, 0600) == 0 &&
        WriteAll(descriptor, 0U, bytes) &&
        ::fsync(descriptor) == 0;
    if (!okay) {
        static_cast<void>(::close(descriptor));
        static_cast<void>(
            ::unlinkat(directory_fd, name, 0));
        return -1;
    }
    return descriptor;
}

[[nodiscard]] std::string ReadFileAt(
    int directory_fd,
    const char* name) {
    int descriptor = -1;
    do {
        descriptor = ::openat(
            directory_fd,
            name,
            O_RDONLY | O_NOFOLLOW | O_NONBLOCK |
                O_CLOEXEC | O_NOATIME);
    } while (descriptor < 0 && errno == EINTR);
    if (descriptor < 0) {
        return {};
    }
    struct stat status {};
    if (::fstat(descriptor, &status) != 0 ||
        status.st_size < 0) {
        static_cast<void>(::close(descriptor));
        return {};
    }
    std::string result(
        static_cast<std::size_t>(status.st_size),
        '\0');
    std::size_t completed = 0U;
    while (completed < result.size()) {
        const ssize_t count = ::pread(
            descriptor,
            result.data() + completed,
            result.size() - completed,
            static_cast<off_t>(completed));
        if (count < 0 && errno == EINTR) {
            continue;
        }
        if (count <= 0) {
            result.clear();
            break;
        }
        completed += static_cast<std::size_t>(count);
    }
    static_cast<void>(::close(descriptor));
    return result;
}

ingress::ReserveCoordinatorStateV1 MakeBootstrap(
    int root_fd) {
    struct stat status {};
    if (::fstat(root_fd, &status) != 0) {
        return {};
    }
    ingress::ReserveCoordinatorStateV1 state{};
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
    ingress::ReserveStateSlotV1 slot{};
    slot.coordinator_state =
        ingress::ReserveCoordinatorPhaseV1::kProvisioned;
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
    const ingress::ReserveCoordinatorStateV1& state) {
    ingress::RawReserveCoordinatorLeaseMarkerV1 marker{};
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
    ingress::RawReserveRegistryEntryKeyV1 key{};
    key.route.source_stream_id = 2002U;
    key.route.capture_date = 20260719U;
    key.stream_day_id = Pattern<16U>(0xb0U);
    key.recovery_attempt_id = Pattern<16U>(0xd0U);
    return key;
}

ingress::SegmentHeaderV1 MakeSegment(
    const ingress::RawReserveRegistryEntryKeyV1& key) {
    ingress::SegmentHeaderV1 segment{};
    segment.source_stream_id =
        key.route.source_stream_id;
    segment.capture_date = key.route.capture_date;
    segment.stream_day_id = key.stream_day_id;
    segment.segment_sequence = 1U;
    segment.segment_base_wal_pos = 0U;
    segment.first_ingress_sequence = 1U;
    segment.created_realtime_ns = 11U;
    segment.created_monotonic_ns = 12U;
    segment.host_uuid = Pattern<16U>(0x21U);
    segment.linux_boot_id = Pattern<16U>(0x31U);
    segment.clock_epoch_algorithm = 1U;
    segment.clock_epoch_digest = Pattern<32U>(0x41U);
    segment.clock_epoch_label = 77U;
    segment.sdk_archive_sha256 = Pattern<32U>(0x61U);
    segment.libmdl_api_sha256 = Pattern<32U>(0x81U);
    segment.endpoint_contract_sha256 =
        Pattern<32U>(0xa1U);
    segment.config_sha256 = Pattern<32U>(0xc1U);
    segment.raw_schema_sha256 =
        ingress::kFrozenRawSchemaSha256;
    segment.build_manifest_sha256 =
        Pattern<32U>(0x22U);
    return segment;
}

ingress::RawV1JournalHeaderWire MakeJournalHeader(
    const ingress::SegmentHeaderV1& segment) {
    ingress::DurableJournalHeaderV1 header{};
    header.capture_date = segment.capture_date;
    header.source_stream_id = segment.source_stream_id;
    header.stream_day_id = segment.stream_day_id;
    header.raw_schema_sha256 =
        ingress::kFrozenRawSchemaSha256;
    header.created_host_uuid = segment.host_uuid;
    header.created_linux_boot_id =
        segment.linux_boot_id;
    header.created_clock_epoch_algorithm =
        segment.clock_epoch_algorithm;
    header.created_clock_epoch_digest =
        segment.clock_epoch_digest;
    header.created_clock_epoch_label =
        segment.clock_epoch_label;
    ingress::RawV1JournalHeaderWire wire{};
    static_cast<void>(
        ingress::EncodeDurableJournalHeaderV1(
            header, &wire));
    return wire;
}

ingress::RawV1DurableMarkerWire MakeMarker(
    const ingress::SegmentHeaderV1& segment,
    std::uint32_t flags,
    std::uint32_t segment_sequence = 1U) {
    ingress::DurableMarkerV1 marker{};
    marker.source_stream_id = segment.source_stream_id;
    marker.segment_sequence = segment_sequence;
    marker.durable_global_wal_pos =
        ingress::kRawV1SegmentHeaderBytes;
    marker.durable_ingress_sequence = 0U;
    marker.durable_segment_offset =
        ingress::kRawV1SegmentHeaderBytes;
    marker.marker_flags = flags;
    ingress::RawV1DurableMarkerWire wire{};
    static_cast<void>(
        ingress::EncodeDurableMarkerV1(marker, &wire));
    return wire;
}

ingress::RawWalWriterSnapshot MakeOpenSnapshot() {
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

ingress::RawSealedSegmentMetadataV1 MakeSealedMetadata(
    const ingress::SegmentHeaderV1& segment,
    const ingress::RawV1DurableMarkerWire& sealed_marker) {
    ingress::RawV1SegmentHeaderWire header_wire{};
    static_cast<void>(
        ingress::EncodeSegmentHeaderV1(
            segment, &header_wire));
    ingress::RawSealedSegmentMetadataV1 metadata{};
    metadata.segment = segment;
    metadata.segment_sha256 =
        common::ComputeSha256(header_wire);
    metadata.index_sha256 = Pattern<32U>(0xe1U);
    metadata.logical_end_offset =
        ingress::kRawV1SegmentHeaderBytes;
    metadata.accepted_sealed_marker_bytes =
        sealed_marker;
    static_cast<void>(
        ingress::DecodeDurableMarkerV1(
            sealed_marker,
            &metadata.accepted_sealed_marker));
    return metadata;
}

ingress::RawIngressCleanStopEvidenceV1 MakeEvidence(
    const ingress::RawReserveRegistryEntryKeyV1& key,
    const ingress::RawV1Identity& writer_instance) {
    ingress::RawIngressCleanStopEvidenceV1 evidence{};
    evidence.started_runtime.source_stream_id =
        key.route.source_stream_id;
    evidence.started_runtime.capture_date =
        key.route.capture_date;
    evidence.started_runtime.stream_day_id =
        key.stream_day_id;
    evidence.started_runtime.recovered_next_ingress_sequence =
        1U;
    evidence.started_runtime.append = {
        1U,
        ingress::kRawV1SegmentHeaderBytes,
        0U,
        ingress::kRawV1SegmentHeaderBytes};
    evidence.started_runtime.durable =
        evidence.started_runtime.append;
    evidence.started_runtime.writer_instance =
        writer_instance;
    evidence.started_runtime.current_segment_sequence = 1U;
    evidence.started_runtime.clock_epoch_algorithm_version =
        1U;
    evidence.started_runtime.clock_epoch_digest =
        Pattern<32U>(0x41U);
    evidence.started_runtime.clock_epoch_label = 77U;

    evidence.capture.stop_requested = true;
    evidence.capture.startup_complete = true;
    evidence.capture.startup_succeeded = true;
    evidence.capture.finished = true;

    evidence.observer.generation.writer_instance =
        writer_instance;
    evidence.observer.generation.stream_day_id =
        key.stream_day_id;
    evidence.observer.generation.source_stream_id =
        key.route.source_stream_id;
    evidence.observer.generation.capture_date =
        key.route.capture_date;
    evidence.observer.observer_processed_wal_pos =
        ingress::kRawV1SegmentHeaderBytes;
    evidence.observer
        .observer_processed_ingress_sequence = 0U;
    evidence.observer
        .observer_processed_segment_sequence = 1U;
    evidence.observer
        .observer_processed_segment_offset =
        ingress::kRawV1SegmentHeaderBytes;
    evidence.observer.generation_active = true;
    evidence.observer.observer_healthy = true;

    evidence.final_sink_identity.writer_instance =
        writer_instance;
    evidence.final_sink_identity.stream_day_id =
        key.stream_day_id;
    evidence.final_sink_identity.source_stream_id =
        key.route.source_stream_id;
    evidence.final_sink_identity.capture_date =
        key.route.capture_date;
    evidence.final_sink_identity.segment_sequence = 1U;
    evidence.final_sink_identity.segment_base_wal_pos = 0U;
    evidence.final_sink_identity.first_ingress_sequence = 1U;

    evidence.final_wal.append = {
        ingress::kRawV1SegmentHeaderBytes,
        0U,
        ingress::kRawV1SegmentHeaderBytes};
    evidence.final_wal.durable =
        evidence.final_wal.append;
    evidence.final_wal.journal_logical_size =
        ingress::kRawV1JournalHeaderBytes +
        2U * ingress::kRawV1DurableMarkerBytes;
    evidence.final_wal.initialized = true;
    evidence.final_wal.sealed = true;
    evidence.final_wal.closed = true;
    return evidence;
}

class CleanStopFixture final {
public:
    CleanStopFixture() = default;
    ~CleanStopFixture() {
        if (journal_fd_ >= 0) {
            static_cast<void>(::close(journal_fd_));
        }
    }

    CleanStopFixture(const CleanStopFixture&) = delete;
    CleanStopFixture& operator=(const CleanStopFixture&) =
        delete;

    [[nodiscard]] bool Initialize(TestContext* test) {
        if (test == nullptr || root_.descriptor() < 0) {
            return false;
        }
        key_ = MakeKey();
        writer_instance_ = Pattern<16U>(0xf0U);
        segment_ = MakeSegment(key_);
        journal_header_ = MakeJournalHeader(segment_);
        initial_marker_ = MakeMarker(segment_, 0U);
        sealed_marker_ = MakeMarker(
            segment_, ingress::kRawV1SegmentSealed);
        evidence_ = MakeEvidence(key_, writer_instance_);

        std::string error;
        route_ = ingress::OpenOrCreateRawStreamDirectory(
            root_.path(),
            key_.route.source_stream_id,
            key_.route.capture_date,
            slug_,
            &error);
        test->Expect(
            route_ != nullptr,
            "clean-stop fixture creates canonical route");
        if (route_ == nullptr) {
            std::cerr << error << '\n';
            return false;
        }
        lease_ = ingress::AcquireRawWriterLeaseAtV1(
            route_->descriptor(),
            key_.route.source_stream_id,
            key_.route.capture_date,
            key_.recovery_attempt_id,
            &error);
        test->Expect(
            lease_ != nullptr,
            "clean-stop fixture acquires writer lease");
        if (lease_ == nullptr) {
            std::cerr << error << '\n';
            return false;
        }
        if (::mkdirat(
                route_->descriptor(),
                "maintenance",
                0700) != 0 ||
            ::fsync(route_->descriptor()) != 0) {
            test->Expect(
                false,
                "clean-stop fixture publishes maintenance directory");
            return false;
        }

        journal_bytes_.reserve(
            journal_header_.size() +
            initial_marker_.size() +
            sealed_marker_.size());
        journal_bytes_.insert(
            journal_bytes_.end(),
            journal_header_.begin(),
            journal_header_.end());
        journal_bytes_.insert(
            journal_bytes_.end(),
            initial_marker_.begin(),
            initial_marker_.end());
        journal_bytes_.insert(
            journal_bytes_.end(),
            sealed_marker_.begin(),
            sealed_marker_.end());
        journal_fd_ = CreateFileAt(
            route_->descriptor(),
            ingress::kRawJournalFilename,
            journal_bytes_);
        test->Expect(
            journal_fd_ >= 0 &&
                ::fsync(route_->descriptor()) == 0,
            "clean-stop fixture retains durable journal inode");
        if (journal_fd_ < 0) {
            return false;
        }

        if (ingress::BuildFreshOpenRawManifestV1(
                segment_,
                MakeOpenSnapshot(),
                &open_manifest_) !=
                ingress::
                    RawManifestTransitionErrorV1::kNone ||
            ingress::PublishCurrentRawManifest(
                *lease_,
                Namespace(),
                open_manifest_,
                kMaximumManifestBytes,
                nullptr,
                &error) !=
                ingress::RawManifestStoreError::kNone ||
            ingress::TransitionOpenRawManifestToClosedV1(
                open_manifest_,
                MakeSealedMetadata(
                    segment_, sealed_marker_),
                &closed_manifest_) !=
                ingress::
                    RawManifestTransitionErrorV1::kNone ||
            ingress::PublishCurrentRawManifest(
                *lease_,
                Namespace(),
                closed_manifest_,
                kMaximumManifestBytes,
                nullptr,
                &error) !=
                ingress::RawManifestStoreError::kNone) {
            test->Expect(
                false,
                "clean-stop fixture publishes closed manifest");
            std::cerr << error << '\n';
            return false;
        }

        const auto bootstrap =
            MakeBootstrap(root_.descriptor());
        const auto coordinator_marker =
            MakeCoordinatorMarker(bootstrap);
        ingress::RawReserveCoordinatorErrorV1 failure =
            ingress::RawReserveCoordinatorErrorV1::kNone;
        coordinator_ = ingress::
            PublishFreshRawReserveRegistryCoordinatorAtV1(
                root_.descriptor(),
                coordinator_marker,
                bootstrap,
                &failure,
                &error);
        test->Expect(
            coordinator_ != nullptr,
            "clean-stop fixture publishes coordinator");
        if (coordinator_ == nullptr) {
            std::cerr << error << '\n';
            return false;
        }
        ingress::RawReserveFreshScaffoldingV1
            registration{};
        registration.key = key_;
        registration.writer_instance = writer_instance_;
        registration.recovery_intent =
            ingress::ReserveRecoveryIntentV1::
                kResumeConnect;
        registration.scaffolding_allocation_cap =
            4096U;
        registration.safe_stop_template_id = 7U;
        if (coordinator_->
                    RegisterFreshScaffolding(
                        registration, &error) !=
                ingress::RawReserveCoordinatorErrorV1::
                    kNone ||
            coordinator_->PublishInit(key_, &error) !=
                ingress::RawReserveCoordinatorErrorV1::
                    kNone ||
            coordinator_->PublishActive(key_, &error) !=
                ingress::RawReserveCoordinatorErrorV1::
                    kNone) {
            test->Expect(
                false,
                "clean-stop fixture reaches durable ACTIVE");
            std::cerr << error << '\n';
            return false;
        }
        test->Expect(
            evidence_.exact(),
            "clean-stop fixture evidence reconciles exactly");
        return evidence_.exact();
    }

    [[nodiscard]] std::unique_ptr<
        ingress::RawPosixCleanStopGateV1>
    CreateGate(TestContext* test) {
        std::string error;
        auto gate = ingress::CreateRawPosixCleanStopGateV1(
            *lease_,
            *coordinator_,
            key_,
            slug_,
            kMaximumManifestBytes,
            &error);
        test->Expect(
            gate != nullptr,
            "POSIX clean-stop gate constructs");
        if (gate == nullptr) {
            std::cerr << error << '\n';
        }
        return gate;
    }

    [[nodiscard]] bool ReplaceJournalWithIdenticalInode(
        TestContext* test) {
        constexpr char kOldName[] =
            ".durable.journal.replaced-test-original";
        if (::renameat(
                route_->descriptor(),
                ingress::kRawJournalFilename,
                route_->descriptor(),
                kOldName) != 0 ||
            ::fsync(route_->descriptor()) != 0) {
            return false;
        }
        const int replacement = CreateFileAt(
            route_->descriptor(),
            ingress::kRawJournalFilename,
            journal_bytes_);
        if (replacement < 0 ||
            ::fsync(route_->descriptor()) != 0) {
            if (replacement >= 0) {
                static_cast<void>(::close(replacement));
            }
            return false;
        }
        struct stat retained {};
        struct stat named {};
        const bool replaced =
            ::fstat(journal_fd_, &retained) == 0 &&
            ::fstatat(
                route_->descriptor(),
                ingress::kRawJournalFilename,
                &named,
                AT_SYMLINK_NOFOLLOW) == 0 &&
            (retained.st_dev != named.st_dev ||
             retained.st_ino != named.st_ino);
        static_cast<void>(::close(replacement));
        struct stat old_named {};
        const bool old_retained_linked =
            retained.st_nlink == static_cast<nlink_t>(1) &&
            ::fstatat(
                route_->descriptor(),
                kOldName,
                &old_named,
                AT_SYMLINK_NOFOLLOW) == 0 &&
            retained.st_dev == old_named.st_dev &&
            retained.st_ino == old_named.st_ino;
        test->Expect(
            replaced && old_retained_linked,
            "test replaces the fixed journal name while the retained old inode remains otherwise safe");
        return replaced && old_retained_linked;
    }

    [[nodiscard]] bool OverwriteLastMarkerMismatch() {
        const auto mismatching =
            MakeMarker(segment_,
                       ingress::kRawV1SegmentSealed,
                       2U);
        const std::uint64_t offset =
            ingress::kRawV1JournalHeaderBytes +
            ingress::kRawV1DurableMarkerBytes;
        return WriteAll(
                   journal_fd_, offset, mismatching) &&
               ::fsync(journal_fd_) == 0 &&
               ::fsync(route_->descriptor()) == 0;
    }

    [[nodiscard]] ingress::RawManifestNamespaceV1
    Namespace() const noexcept {
        return {
            key_.route.capture_date,
            key_.route.source_stream_id,
            key_.stream_day_id};
    }

    [[nodiscard]] const
        ingress::RawIngressCleanStopEvidenceV1&
    evidence() const noexcept {
        return evidence_;
    }

    [[nodiscard]] const ingress::RawV1JournalHeaderWire&
    journal_header() const noexcept {
        return journal_header_;
    }

    [[nodiscard]] const ingress::RawManifestV1&
    closed_manifest() const noexcept {
        return closed_manifest_;
    }

    [[nodiscard]] std::shared_ptr<
        ingress::RawReserveRegistryCoordinatorV1>
    coordinator() const noexcept {
        return coordinator_;
    }

    [[nodiscard]] int route_fd() const noexcept {
        return route_->descriptor();
    }

private:
    TempDirectory root_;
    ingress::RawReserveRegistryEntryKeyV1 key_{};
    ingress::RawV1Identity writer_instance_{};
    std::string slug_ = "sz-tick";
    std::unique_ptr<ingress::RawStreamDirectory> route_;
    std::unique_ptr<ingress::RawWriterLease> lease_;
    std::shared_ptr<
        ingress::RawReserveRegistryCoordinatorV1>
        coordinator_;
    ingress::SegmentHeaderV1 segment_{};
    ingress::RawV1JournalHeaderWire journal_header_{};
    ingress::RawV1DurableMarkerWire initial_marker_{};
    ingress::RawV1DurableMarkerWire sealed_marker_{};
    std::vector<std::byte> journal_bytes_;
    int journal_fd_ = -1;
    ingress::RawManifestV1 open_manifest_{};
    ingress::RawManifestV1 closed_manifest_{};
    ingress::RawIngressCleanStopEvidenceV1 evidence_{};
};

[[nodiscard]] bool RouteIsActive(
    const std::shared_ptr<
        ingress::RawReserveRegistryCoordinatorV1>&
        coordinator) {
    const auto state = coordinator->state();
    if (state.selected_slot >= state.slots.size()) {
        return false;
    }
    const auto& slot = state.slots[state.selected_slot];
    return slot.entry_count == 1U &&
           slot.entries[0U].registry_status ==
               ingress::ReserveRegistryStatusV1::kActive;
}

void TestHappyPath(TestContext* test) {
    CleanStopFixture fixture;
    if (!fixture.Initialize(test)) {
        return;
    }
    auto gate = fixture.CreateGate(test);
    if (gate == nullptr) {
        return;
    }

    std::unique_ptr<
        ingress::BuiltSealedRawCertificateV1>
        expected_certificate;
    test->Expect(
        ingress::BuildSealedRawCertificateCapabilityV1(
            fixture.journal_header(),
            fixture.closed_manifest(),
            fixture.evidence().final_sink_identity,
            fixture.evidence().final_wal,
            &expected_certificate) ==
                ingress::SealedRawCertificateV1Error::kNone &&
            expected_certificate != nullptr,
        "happy path independently derives terminal certificate");
    if (expected_certificate == nullptr) {
        return;
    }

    test->Expect(
        gate->Complete(fixture.evidence()) &&
            gate->failure() ==
                ingress::RawCleanStopGateFailureV1::kNone &&
            gate->diagnostic().empty(),
        "real POSIX clean-stop publishes certificate and unregisters");
    const auto state = fixture.coordinator()->state();
    const auto& slot = state.slots[state.selected_slot];
    test->Expect(
        slot.entry_count == 0U,
        "opaque certificate receipt is consumed by durable unregister");

    const int maintenance_fd = ::openat(
        fixture.route_fd(),
        "maintenance",
        O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
            O_NONBLOCK | O_CLOEXEC | O_NOATIME);
    test->Expect(
        maintenance_fd >= 0,
        "happy path reopens maintenance directory");
    if (maintenance_fd >= 0) {
        const std::string filename(
            expected_certificate->filename());
        struct stat status {};
        test->Expect(
            ::fstatat(
                maintenance_fd,
                filename.c_str(),
                &status,
                AT_SYMLINK_NOFOLLOW) == 0 &&
                S_ISREG(status.st_mode) &&
                status.st_uid == ::geteuid() &&
                (status.st_mode & 07777U) == 0600U &&
                status.st_nlink ==
                    static_cast<nlink_t>(1) &&
                ReadFileAt(
                    maintenance_fd,
                    filename.c_str()) ==
                    expected_certificate->canonical_jcs(),
            "certificate final is exact owner-only canonical JCS");
        static_cast<void>(::close(maintenance_fd));
    }
    test->Expect(
        gate->Complete(fixture.evidence()),
        "completed gate is idempotent after unregister");
}

void TestMarkerMismatchFailsClosed(TestContext* test) {
    CleanStopFixture fixture;
    if (!fixture.Initialize(test)) {
        return;
    }
    auto gate = fixture.CreateGate(test);
    if (gate == nullptr) {
        return;
    }
    test->Expect(
        fixture.OverwriteLastMarkerMismatch(),
        "test publishes a valid but wrong terminal marker");
    test->Expect(
        !gate->Complete(fixture.evidence()) &&
            gate->failure() ==
                ingress::RawCleanStopGateFailureV1::
                    kJournalEvidenceInvalid &&
            RouteIsActive(fixture.coordinator()),
        "terminal marker mismatch fails closed before certificate/unregister");
    test->Expect(
        !gate->Complete(fixture.evidence()) &&
            gate->failure() ==
                ingress::RawCleanStopGateFailureV1::
                    kJournalEvidenceInvalid,
        "failed clean-stop gate is not retryable in-process");
}

void TestJournalReplacementFailsClosed(TestContext* test) {
    CleanStopFixture fixture;
    if (!fixture.Initialize(test)) {
        return;
    }
    auto gate = fixture.CreateGate(test);
    if (gate == nullptr) {
        return;
    }
    if (!fixture.ReplaceJournalWithIdenticalInode(test)) {
        test->Expect(false, "journal replacement fixture succeeds");
        return;
    }
    test->Expect(
        !gate->Complete(fixture.evidence()) &&
            gate->failure() ==
                ingress::RawCleanStopGateFailureV1::
                    kJournalEvidenceInvalid &&
            RouteIsActive(fixture.coordinator()),
        "byte-identical journal pathname replacement fails closed against retained writer inode");
}

}  // namespace

int main() {
    TestContext test;
    TestHappyPath(&test);
    TestMarkerMismatchFailsClosed(&test);
    TestJournalReplacementFailsClosed(&test);
    if (test.failures != 0) {
        std::cerr << test.failures
                  << " clean-stop gate assertion(s) failed\n";
        return 1;
    }
    std::cout
        << "Phase 2 POSIX clean-stop gate checks passed\n";
    return 0;
}
