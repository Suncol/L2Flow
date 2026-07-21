#include "l2flow/ingress/raw_control_file.h"
#include "l2flow/ingress/raw_clean_stop_gate.h"
#include "l2flow/ingress/raw_fresh_route_posix.h"
#include "l2flow/ingress/raw_index_v1.h"
#include "l2flow/ingress/raw_manifest_store.h"
#include "l2flow/ingress/raw_namespace.h"
#include "l2flow/ingress/raw_posix_io.h"
#include "l2flow/ingress/raw_v1.h"
#include "l2flow/ingress/raw_wal_stream.h"
#include "l2flow/ingress/raw_wal_stream_posix.h"
#include "l2flow/ingress/raw_writer_lease.h"

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

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

    int failures = 0;
};

template <std::size_t Size>
void Fill(
    std::array<std::byte, Size>* bytes,
    std::uint8_t seed) {
    for (std::size_t index = 0U;
         index < bytes->size();
         ++index) {
        (*bytes)[index] =
            static_cast<std::byte>(
                static_cast<std::uint8_t>(
                    seed +
                    static_cast<std::uint8_t>(index)));
    }
}

ingress::ReserveCoordinatorStateV1
MakeCoordinatorBootstrap(int root_directory_fd) {
    struct stat status {};
    if (::fstat(root_directory_fd, &status) != 0) {
        return {};
    }
    ingress::ReserveCoordinatorStateV1 state{};
    Fill(&state.header.reserve_state_uuid, 0x11U);
    Fill(&state.header.quota_identity_sha256, 0x31U);
    Fill(&state.header.mount_identity_sha256, 0x51U);
    state.header.device_id =
        static_cast<std::uint64_t>(status.st_dev);
    state.header.declared_releasable_bytes =
        UINT64_C(1) << 20U;
    state.header.allocation_quantum_bytes = 4096U;
    state.header.declared_inode_reserve_count = 64U;
    state.header.byte_probe_version = 1U;
    state.header.inode_probe_version = 1U;
    Fill(
        &state.header.inode_inventory_sha256,
        0x71U);
    Fill(
        &state.header.safe_stop_catalog_sha256,
        0x91U);
    ingress::ReserveStateSlotV1 slot{};
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

void StoreU16(
    std::uint16_t value,
    std::byte* output) {
    output[0U] =
        static_cast<std::byte>(value & 0xffU);
    output[1U] =
        static_cast<std::byte>((value >> 8U) & 0xffU);
}

void StoreU32(
    std::uint32_t value,
    std::byte* output) {
    for (std::size_t index = 0U;
         index < sizeof(value);
         ++index) {
        output[index] =
            static_cast<std::byte>(
                (value >>
                 static_cast<unsigned int>(
                     index * 8U)) &
                0xffU);
    }
}

void StoreU64(
    std::uint64_t value,
    std::byte* output) {
    for (std::size_t index = 0U;
         index < sizeof(value);
         ++index) {
        output[index] =
            static_cast<std::byte>(
                (value >>
                 static_cast<unsigned int>(
                     index * 8U)) &
                0xffU);
    }
}

class TemporaryDirectory final {
public:
    TemporaryDirectory() {
        std::array<char, 64U> pattern{};
        const char prefix[] =
            "/tmp/l2flow-raw-stream-posix-XXXXXX";
        static_assert(sizeof(prefix) <= 64U);
        std::memcpy(
            pattern.data(), prefix, sizeof(prefix));
        char* const created = ::mkdtemp(pattern.data());
        if (created == nullptr) {
            return;
        }
        path_ = created;
        do {
            descriptor_ = ::open(
                path_.c_str(),
                O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
                    O_CLOEXEC | O_NOATIME);
        } while (descriptor_ < 0 && errno == EINTR);
    }

    ~TemporaryDirectory() {
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

    [[nodiscard]] bool ok() const noexcept {
        return descriptor_ >= 0;
    }
    [[nodiscard]] int descriptor() const noexcept {
        return descriptor_;
    }
    [[nodiscard]] const std::string& path()
        const noexcept {
        return path_;
    }

private:
    std::string path_;
    int descriptor_ = -1;
};

struct ClockFixture final {
    static std::uint64_t Realtime(
        void* context) noexcept {
        ClockFixture* const clock =
            static_cast<ClockFixture*>(context);
        clock->realtime += 100U;
        return clock->realtime;
    }

    static std::uint64_t Heartbeat(
        void* context) noexcept {
        ClockFixture* const clock =
            static_cast<ClockFixture*>(context);
        clock->heartbeat += 10U;
        return clock->heartbeat;
    }

    static std::uint64_t StreamMonotonic(
        void* context) noexcept {
        ClockFixture* const clock =
            static_cast<ClockFixture*>(context);
        clock->stream_monotonic += 10U;
        return clock->stream_monotonic;
    }

    std::uint64_t realtime =
        UINT64_C(1'721'234'567'000'000'000);
    std::uint64_t heartbeat = 10'000U;
    std::uint64_t stream_monotonic = 1'000U;
};

struct RecordFixture final {
    ingress::CaptureMetaV1 meta{};
    ingress::RawV1VendorHead head{};
    std::vector<std::byte> body;

    [[nodiscard]] ingress::RawWalRecordInputV1
    input() const noexcept {
        return {meta, head, body};
    }
};

RecordFixture MakeRecord(std::uint64_t sequence) {
    RecordFixture record{};
    record.meta.source_stream_id = 2002U;
    record.meta.connection_epoch_hint = 7U;
    record.meta.ingress_sequence = sequence;
    record.meta.recv_realtime_ns =
        5'000U + sequence;
    record.meta.recv_monotonic_ns =
        4'000U + sequence;
    record.meta.capture_date = 20260718U;
    record.body.assign(13U, std::byte{0x5a});
    record.head[0U] = static_cast<std::byte>(
        ingress::kVendorMessageHeadBytes);
    StoreU32(
        static_cast<std::uint32_t>(
            ingress::kVendorMessageHeadBytes +
            record.body.size()),
        record.head.data() + 1U);
    record.head[5U] = std::byte{1U};
    record.head[6U] = std::byte{6U};
    StoreU16(101U, record.head.data() + 7U);
    StoreU16(36U, record.head.data() + 9U);
    StoreU32(93000123U, record.head.data() + 11U);
    StoreU64(
        90'000U + sequence,
        record.head.data() + 15U);
    return record;
}

struct InitialFixture final {
    ingress::SegmentHeaderV1 segment{};
    ingress::DurableJournalHeaderV1 journal{};
    ingress::RawWalWriterConfig config{};
};

InitialFixture MakeInitialFixture() {
    InitialFixture fixture{};
    fixture.segment.source_stream_id = 2002U;
    fixture.segment.capture_date = 20260718U;
    Fill(&fixture.segment.stream_day_id, 0x10U);
    fixture.segment.segment_sequence = 1U;
    fixture.segment.segment_base_wal_pos = 0U;
    fixture.segment.first_ingress_sequence = 1U;
    fixture.segment.created_realtime_ns =
        UINT64_C(1'721'234'567'000'000'000);
    fixture.segment.created_monotonic_ns = 900U;
    Fill(&fixture.segment.host_uuid, 0x30U);
    Fill(&fixture.segment.linux_boot_id, 0x50U);
    fixture.segment.clock_epoch_algorithm = 1U;
    Fill(
        &fixture.segment.clock_epoch_digest,
        0x70U);
    fixture.segment.clock_epoch_label = 77U;
    Fill(
        &fixture.segment.sdk_archive_sha256,
        0x80U);
    Fill(
        &fixture.segment.libmdl_api_sha256,
        0x90U);
    Fill(
        &fixture.segment.endpoint_contract_sha256,
        0xa0U);
    Fill(&fixture.segment.config_sha256, 0xb0U);
    Fill(
        &fixture.segment.raw_schema_sha256,
        0xc0U);
    Fill(
        &fixture.segment.build_manifest_sha256,
        0xd0U);

    fixture.journal.capture_date =
        fixture.segment.capture_date;
    fixture.journal.source_stream_id =
        fixture.segment.source_stream_id;
    fixture.journal.stream_day_id =
        fixture.segment.stream_day_id;
    fixture.journal.raw_schema_sha256 =
        fixture.segment.raw_schema_sha256;
    fixture.journal.created_host_uuid =
        fixture.segment.host_uuid;
    fixture.journal.created_linux_boot_id =
        fixture.segment.linux_boot_id;
    fixture.journal.created_clock_epoch_algorithm =
        fixture.segment.clock_epoch_algorithm;
    fixture.journal.created_clock_epoch_digest =
        fixture.segment.clock_epoch_digest;
    fixture.journal.created_clock_epoch_label =
        fixture.segment.clock_epoch_label;

    static_cast<void>(ingress::EncodeSegmentHeaderV1(
        fixture.segment,
        &fixture.config.segment_header_wire));
    static_cast<void>(
        ingress::EncodeDurableJournalHeaderV1(
            fixture.journal,
            &fixture.config.journal_header_wire));
    fixture.config.source_stream_id =
        fixture.segment.source_stream_id;
    fixture.config.capture_date =
        fixture.segment.capture_date;
    fixture.config.segment_sequence = 1U;
    fixture.config.segment_base_wal_pos = 0U;
    fixture.config.first_ingress_sequence = 1U;
    fixture.config.initial_durable_ingress_sequence =
        0U;
    fixture.config.initialization_mode =
        ingress::RawWalInitializationMode::
            kFreshJournal;
    fixture.config.headers_already_persisted = true;
    return fixture;
}

class ExactFreshGate final
    : public ingress::RawFreshMutationAuthorizationGateV1 {
public:
    ingress::RawFreshStateAuthorizationV1 expected{};

    [[nodiscard]] bool Authorizes(
        const ingress::RawFreshStateAuthorizationV1&
            facts) const noexcept override {
        return facts.source_stream_id ==
                   expected.source_stream_id &&
               facts.capture_date ==
                   expected.capture_date &&
               facts.stream_day_id ==
                   expected.stream_day_id &&
               facts.recovery_attempt ==
                   expected.recovery_attempt &&
               facts.registry_stage ==
                   expected.registry_stage &&
               facts.durable_state_generation ==
                   expected.durable_state_generation;
    }
};

struct FreshFactoryFixture final {
    [[nodiscard]] bool Initialize(
        std::uint8_t writer_seed,
        std::uint8_t attempt_seed,
        std::string_view requested_slug =
            "sz-tick") {
        slug = std::string(requested_slug);
        if (!root.ok() ||
            ingress::ComputeRawRecordLayoutV1(
                13U, &layout) !=
                ingress::RawV1Error::kNone) {
            error =
                "fresh factory fixture cannot open root or compute layout";
            return false;
        }
        segment_target =
            ingress::kRawV1SegmentHeaderBytes +
            layout.record_size;

        const ingress::ReserveCoordinatorStateV1
            coordinator_bootstrap =
                MakeCoordinatorBootstrap(
                    root.descriptor());
        const auto coordinator_marker =
            MakeCoordinatorMarker(
                coordinator_bootstrap);
        ingress::RawReserveCoordinatorErrorV1
            coordinator_failure =
                ingress::RawReserveCoordinatorErrorV1::
                    kNone;
        coordinator =
            ingress::
                PublishFreshRawReserveRegistryCoordinatorAtV1(
                    root.descriptor(),
                    coordinator_marker,
                    coordinator_bootstrap,
                    &coordinator_failure,
                    &error);
        if (coordinator == nullptr) {
            return false;
        }

        key.route.source_stream_id =
            initial.segment.source_stream_id;
        key.route.capture_date =
            initial.segment.capture_date;
        key.stream_day_id =
            initial.segment.stream_day_id;
        Fill(&key.recovery_attempt_id, attempt_seed);
        ingress::RawReserveFreshScaffoldingV1
            registration{};
        registration.key = key;
        Fill(
            &registration.writer_instance,
            writer_seed);
        registration.recovery_intent =
            ingress::ReserveRecoveryIntentV1::
                kResumeConnect;
        registration.scaffolding_allocation_cap =
            UINT64_C(1) << 20U;
        registration.safe_stop_template_id = 7U;
        if (coordinator->RegisterFreshScaffolding(
                registration, &error) !=
            ingress::RawReserveCoordinatorErrorV1::
                kNone) {
            return false;
        }

        auto scaffolding_action =
            coordinator->AcquireAction(
                key,
                ingress::ReserveRegistryStatusV1::
                    kScaffolding,
                &coordinator_failure,
                &error);
        if (scaffolding_action == nullptr) {
            return false;
        }
        ingress::RawFreshStateAuthorizationV1
            authorization{};
        authorization.source_stream_id =
            key.route.source_stream_id;
        authorization.capture_date =
            key.route.capture_date;
        authorization.stream_day_id =
            key.stream_day_id;
        authorization.recovery_attempt =
            key.recovery_attempt_id;
        authorization.registry_stage =
            ingress::RawFreshRegistryStageV1::
                kScaffolding;
        authorization.durable_state_generation =
            scaffolding_action->token()
                .state_generation;
        stream_directory =
            ingress::
                OpenOrCreateAuthorizedFreshRawStreamDirectory(
                    root.path(),
                    key.route.source_stream_id,
                    key.route.capture_date,
                    slug,
                    authorization,
                    *scaffolding_action,
                    &error);
        if (stream_directory == nullptr) {
            return false;
        }
        if (!ingress::
                CreateOrAdoptAuthorizedFreshRawMaintenanceDirectoryV1(
                    *stream_directory,
                    authorization,
                    *scaffolding_action,
                    &error)) {
            return false;
        }

        lease = ingress::AcquireRawWriterLeaseAtV1(
            stream_directory->descriptor(),
            key.route.source_stream_id,
            key.route.capture_date,
            key.recovery_attempt_id,
            &error);
        if (lease == nullptr) {
            return false;
        }
        anchor =
            ingress::PublishFreshRawJournalAnchor(
                *lease,
                initial.config.journal_header_wire,
                authorization,
                *scaffolding_action,
                &error);
        if (anchor == nullptr) {
            return false;
        }
        scaffolding_action.reset();
        if (coordinator->PublishInit(
                key, &error) !=
            ingress::RawReserveCoordinatorErrorV1::
                kNone) {
            return false;
        }

        initial.config.headers_already_persisted =
            false;
        initial.config.writer_instance =
            registration.writer_instance;
        backend_options.writer_instance =
            registration.writer_instance;
        backend_options.segment_preallocation_bytes =
            segment_target;
        backend_options.maximum_manifest_bytes =
            1024U * 1024U;
        backend_options.realtime_now =
            &ClockFixture::Realtime;
        backend_options.realtime_clock_context =
            &clocks;
        backend_options.monotonic_now =
            &ClockFixture::Heartbeat;
        backend_options.monotonic_clock_context =
            &clocks;
        artifact_options
            .expected_raw_schema_sha256 =
            initial.segment.raw_schema_sha256;
        artifact_options.maximum_segment_bytes =
            segment_target;
        limits.segment_target_bytes = segment_target;
        limits.segment_max_age_ns =
            std::numeric_limits<std::uint64_t>::max();
        limits.maximum_record_bytes =
            layout.record_size;
        limits.monotonic_now =
            &ClockFixture::StreamMonotonic;
        limits.monotonic_clock_context = &clocks;
        return true;
    }

    TemporaryDirectory root;
    InitialFixture initial = MakeInitialFixture();
    ingress::RawRecordLayoutV1 layout{};
    std::uint64_t segment_target = 0U;
    ClockFixture clocks{};
    std::shared_ptr<
        ingress::RawReserveRegistryCoordinatorV1>
        coordinator;
    ingress::RawReserveRegistryEntryKeyV1 key{};
    std::unique_ptr<ingress::RawStreamDirectory>
        stream_directory;
    std::unique_ptr<ingress::RawWriterLease> lease;
    std::unique_ptr<ingress::RawFreshJournalAnchor>
        anchor;
    ingress::RawPosixWalStreamBackendOptionsV1
        backend_options{};
    ingress::RawSegmentArtifactOptionsV1
        artifact_options{};
    ingress::RawWalStreamLimitsV1 limits{};
    std::string slug;
    std::string error;
};

[[nodiscard]] bool RegistryEntryHasStatus(
    const ingress::ReserveCoordinatorStateV1& state,
    const ingress::RawReserveRegistryEntryKeyV1& key,
    ingress::ReserveRegistryStatusV1 status) {
    if (state.selected_slot >= state.slots.size()) {
        return false;
    }
    const ingress::ReserveStateSlotV1& slot =
        state.slots[state.selected_slot];
    if (slot.entry_count > slot.entries.size()) {
        return false;
    }
    for (std::size_t index = 0U;
         index <
         static_cast<std::size_t>(slot.entry_count);
         ++index) {
        const ingress::ReserveStateEntryV1& entry =
            slot.entries[index];
        if (entry.source_stream_id ==
                key.route.source_stream_id &&
            entry.capture_date ==
                key.route.capture_date &&
            entry.stream_day_id ==
                key.stream_day_id &&
            entry.executor_or_recovery_attempt ==
                key.recovery_attempt_id) {
            return entry.registry_status == status;
        }
    }
    return false;
}

[[nodiscard]] bool FileAbsentAt(
    int directory_fd,
    const char* name) {
    struct stat status {};
    errno = 0;
    return ::fstatat(
               directory_fd,
               name,
               &status,
               AT_SYMLINK_NOFOLLOW) != 0 &&
           errno == ENOENT;
}

[[nodiscard]] bool ReadExactFileAt(
    int directory_fd,
    const std::string& name,
    std::vector<std::byte>* bytes) {
    if (bytes == nullptr) {
        return false;
    }
    int descriptor = -1;
    do {
        descriptor = ::openat(
            directory_fd,
            name.c_str(),
            O_RDONLY | O_NOFOLLOW | O_CLOEXEC |
                O_NOATIME);
    } while (descriptor < 0 && errno == EINTR);
    if (descriptor < 0) {
        return false;
    }
    struct stat status {};
    if (::fstat(descriptor, &status) != 0 ||
        status.st_size < 0 ||
        static_cast<std::uint64_t>(status.st_size) >
            static_cast<std::uint64_t>(
                std::numeric_limits<std::size_t>::max())) {
        static_cast<void>(::close(descriptor));
        return false;
    }
    try {
        bytes->assign(
            static_cast<std::size_t>(status.st_size),
            std::byte{0});
    } catch (...) {
        static_cast<void>(::close(descriptor));
        return false;
    }
    std::size_t completed = 0U;
    while (completed < bytes->size()) {
        const ssize_t result = ::pread(
            descriptor,
            bytes->data() +
                static_cast<std::ptrdiff_t>(completed),
            bytes->size() - completed,
            static_cast<off_t>(completed));
        if (result > 0) {
            completed +=
                static_cast<std::size_t>(result);
        } else if (result < 0 && errno == EINTR) {
            continue;
        } else {
            static_cast<void>(::close(descriptor));
            return false;
        }
    }
    static_cast<void>(::close(descriptor));
    return true;
}

std::string SegmentName(
    std::uint32_t sequence,
    std::string_view extension) {
    std::array<char, 64U> buffer{};
    const int count = ::snprintf(
        buffer.data(),
        buffer.size(),
        "segment-%08u.%.*s",
        sequence,
        static_cast<int>(extension.size()),
        extension.data());
    if (count < 0 ||
        static_cast<std::size_t>(count) >=
            buffer.size()) {
        return {};
    }
    return std::string(
        buffer.data(),
        static_cast<std::size_t>(count));
}

void RunFreshActiveFactory(TestContext* test) {
    ingress::RawFreshActivePosixStreamResultV1
        indeterminate_publication{};
    indeterminate_publication.failure =
        ingress::
            RawFreshActivePosixStreamFailureV1::
                kActivePublication;
    indeterminate_publication.publication_state =
        ingress::RawFreshActivePublicationStateV1::
            kPublicationIndeterminate;
    ingress::RawFreshActivePosixStreamResultV1
        unpromoted_publication{};
    unpromoted_publication.failure =
        ingress::
            RawFreshActivePosixStreamFailureV1::
                kActivePromotion;
    unpromoted_publication.publication_state =
        ingress::RawFreshActivePublicationStateV1::
            kPublishedAwaitingLocalPromotion;
    test->Expect(
        indeterminate_publication
                .requires_fail_stop() &&
            unpromoted_publication
                .requires_fail_stop(),
        "uncertain ACTIVE publication and post-ACTIVE promotion failure are explicit fail-stop outcomes");

    FreshFactoryFixture fresh;
    test->Expect(
        fresh.Initialize(0xe2U, 0xd3U),
        "fresh factory fixture reaches durable INIT");
    if (fresh.coordinator == nullptr ||
        fresh.stream_directory == nullptr ||
        fresh.lease == nullptr ||
        fresh.anchor == nullptr) {
        std::cerr << fresh.error << '\n';
        return;
    }
    const int stream_directory_fd =
        fresh.stream_directory->descriptor();
    test->Expect(
        RegistryEntryHasStatus(
            fresh.coordinator->state(),
            fresh.key,
            ingress::ReserveRegistryStatusV1::kInit) &&
            FileAbsentAt(
                stream_directory_fd,
                ingress::kRawFirstSegmentFilename) &&
            FileAbsentAt(
                stream_directory_fd,
                ingress::
                    kRawManifestCurrentFilename) &&
            FileAbsentAt(
                stream_directory_fd,
                ingress::kRawControlFilename),
        "fresh factory begins at INIT with no segment, manifest, or control");

    ingress::RawFreshActivePosixStreamResultV1 result =
        ingress::CreateFreshActiveRawPosixStreamV1(
            std::move(fresh.lease),
            std::move(fresh.anchor),
            fresh.initial.config,
            fresh.backend_options,
            fresh.artifact_options,
            fresh.limits,
            *fresh.coordinator,
            fresh.key,
            fresh.slug,
            &fresh.error);
    test->Expect(
        result.ok() &&
            result.initial_segment_published &&
            result.publication_state ==
                ingress::
                    RawFreshActivePublicationStateV1::
                        kActiveBound &&
            !result.requires_fail_stop(),
        "factory returns only an ACTIVE-bound owning Raw sink");
    if (!result.ok()) {
        std::cerr
            << ingress::
                   RawFreshActivePosixStreamFailureV1Name(
                       result.failure)
            << ": " << fresh.error << '\n';
        return;
    }
    test->Expect(
        result.sink->authorization_key() ==
                fresh.key &&
            result.sink
                    ->authorized_writer_instance() ==
                fresh.backend_options.writer_instance &&
            RegistryEntryHasStatus(
                fresh.coordinator->state(),
                fresh.key,
                ingress::ReserveRegistryStatusV1::
                    kActive),
        "ACTIVE sink identity matches the durable route and writer");
    auto clean_stop_gate =
        ingress::CreateRawPosixCleanStopGateV1(
            *result.sink,
            *fresh.coordinator,
            fresh.key,
            fresh.slug,
            fresh.backend_options.maximum_manifest_bytes,
            &fresh.error);
    test->Expect(
        clean_stop_gate != nullptr &&
            result.sink->retained_writer_lease()
                    .directory_descriptor() ==
                result.sink
                    ->RawReserveMutationTargetDirectoryDescriptorV1(),
        "ACTIVE owning sink safely exposes its retained writer lease to the clean-stop gate");
    clean_stop_gate.reset();

    std::vector<std::byte> journal_bytes;
    test->Expect(
        ReadExactFileAt(
            stream_directory_fd,
            ingress::kRawJournalFilename,
            &journal_bytes) &&
            journal_bytes.size() ==
                ingress::kRawV1JournalHeaderBytes +
                    ingress::
                        kRawV1DurableMarkerBytes,
        "INIT initialization durably writes exactly one header-only marker");
    if (journal_bytes.size() ==
        ingress::kRawV1JournalHeaderBytes +
            ingress::kRawV1DurableMarkerBytes) {
        ingress::DurableMarkerV1 marker{};
        test->Expect(
            ingress::DecodeDurableMarkerV1(
                std::span<const std::byte>(
                    journal_bytes.data() +
                        static_cast<std::ptrdiff_t>(
                            ingress::
                                kRawV1JournalHeaderBytes),
                    ingress::
                        kRawV1DurableMarkerBytes),
                &marker) ==
                    ingress::RawV1Error::kNone &&
                marker.segment_sequence == 1U &&
                marker.marker_flags == 0U &&
                marker.durable_ingress_sequence ==
                    0U,
            "fresh marker is the exact segment-1 header boundary");
    }

    ingress::RawManifestV1 manifest{};
    test->Expect(
        ingress::LoadCurrentRawManifestAt(
            stream_directory_fd,
            {fresh.initial.segment.capture_date,
             fresh.initial.segment.source_stream_id,
             fresh.initial.segment.stream_day_id},
            fresh.backend_options
                .maximum_manifest_bytes,
            &manifest,
            nullptr,
            nullptr,
            &fresh.error) ==
                ingress::RawManifestStoreError::kNone &&
            manifest.closed_entries.empty() &&
            manifest.open_entry.has_value() &&
            manifest.open_entry->segment_sequence ==
                1U,
        "open manifest is durable before the INIT binding can be promoted");
    auto control = ingress::OpenRawControlFile(
        stream_directory_fd,
        fresh.initial.segment.source_stream_id,
        fresh.initial.segment.capture_date,
        &fresh.error);
    ingress::RawControlSnapshot control_snapshot{};
    test->Expect(
        control != nullptr &&
            control->Read(&control_snapshot) &&
            control_snapshot.writer_instance ==
                fresh.backend_options.writer_instance &&
            control_snapshot.segment_sequence == 1U &&
            control_snapshot.append_ingress_sequence ==
                0U &&
            control_snapshot
                    .durable_ingress_sequence == 0U,
        "control boundary exposes the initialized writer before any record");

    ingress::RawReserveCoordinatorErrorV1
        action_failure =
            ingress::RawReserveCoordinatorErrorV1::
                kNone;
    auto stale_init =
        fresh.coordinator
            ->AcquireActionForExistingRoute(
                fresh.key,
                ingress::ReserveRegistryStatusV1::
                    kInit,
                fresh.slug,
                &action_failure,
                &fresh.error);
    auto active_action =
        fresh.coordinator
            ->AcquireActionForExistingRoute(
                fresh.key,
                ingress::ReserveRegistryStatusV1::
                    kActive,
                fresh.slug,
                &action_failure,
                &fresh.error);
    test->Expect(
        stale_init == nullptr &&
            active_action != nullptr &&
            active_action->target() != nullptr,
        "ACTIVE publication invalidates INIT actions and exposes the exact ACTIVE target");
    active_action.reset();

    ingress::RawReserveCoordinatorErrorV1
        duplicate_failure =
            ingress::RawReserveCoordinatorErrorV1::
                kNone;
    auto duplicate_receipt =
        fresh.coordinator->PublishFreshActive(
            fresh.key,
            fresh.slug,
            &duplicate_failure,
            &fresh.error);
    test->Expect(
        duplicate_receipt == nullptr &&
            duplicate_failure ==
                ingress::RawReserveCoordinatorErrorV1::
                    kRouteStatusMismatch &&
            result.sink->active_binding_validated(),
        "fresh ACTIVE receipt/promotion cannot be repeated");

    const RecordFixture first = MakeRecord(1U);
    test->Expect(
        result.sink->AppendRecord(first.input()) &&
            result.sink->FlushDurable(),
        "the exact INIT-opened segment and journal continue through the promoted ACTIVE binding");
    const ingress::RawWalWriterSnapshot snapshot =
        result.sink->Snapshot();
    test->Expect(
        snapshot.initialized &&
            !snapshot.sealed &&
            !snapshot.closed &&
            !snapshot.fatal &&
            snapshot.append.ingress_sequence == 1U &&
            snapshot.durable.ingress_sequence == 1U,
        "ACTIVE writes advance the same initialized stream");
    test->Expect(
        result.sink->SealAndClose(),
        "fresh ACTIVE stream closes cleanly");

    FreshFactoryFixture wrong_writer;
    test->Expect(
        wrong_writer.Initialize(0xe3U, 0xd4U),
        "wrong-writer fixture reaches INIT");
    if (wrong_writer.coordinator != nullptr &&
        wrong_writer.stream_directory != nullptr &&
        wrong_writer.lease != nullptr &&
        wrong_writer.anchor != nullptr) {
        ingress::RawWalWriterConfig wrong_config =
            wrong_writer.initial.config;
        ingress::RawPosixWalStreamBackendOptionsV1
            wrong_options =
                wrong_writer.backend_options;
        Fill(&wrong_config.writer_instance, 0x43U);
        wrong_options.writer_instance =
            wrong_config.writer_instance;
        auto rejected_writer =
            ingress::CreateFreshActiveRawPosixStreamV1(
                std::move(wrong_writer.lease),
                std::move(wrong_writer.anchor),
                wrong_config,
                wrong_options,
                wrong_writer.artifact_options,
                wrong_writer.limits,
                *wrong_writer.coordinator,
                wrong_writer.key,
                wrong_writer.slug,
                &wrong_writer.error);
        test->Expect(
            !rejected_writer.ok() &&
                rejected_writer.failure ==
                    ingress::
                        RawFreshActivePosixStreamFailureV1::
                            kInitAuthorization &&
                rejected_writer.publication_state ==
                    ingress::
                        RawFreshActivePublicationStateV1::
                            kNotPublished &&
                !rejected_writer
                     .initial_segment_published &&
                !rejected_writer.requires_fail_stop() &&
                RegistryEntryHasStatus(
                    wrong_writer.coordinator->state(),
                    wrong_writer.key,
                    ingress::
                        ReserveRegistryStatusV1::
                            kInit) &&
                FileAbsentAt(
                    wrong_writer.stream_directory
                        ->descriptor(),
                    ingress::
                        kRawFirstSegmentFilename) &&
                FileAbsentAt(
                    wrong_writer.stream_directory
                        ->descriptor(),
                    ingress::kRawControlFilename),
            "wrong writer is rejected before mutation, ACTIVE publication, or runtime capability");
    }

    FreshFactoryFixture wrong_target;
    test->Expect(
        wrong_target.Initialize(0xe4U, 0xd5U),
        "wrong-target fixture reaches INIT");
    if (wrong_target.coordinator != nullptr &&
        wrong_target.stream_directory != nullptr &&
        wrong_target.lease != nullptr &&
        wrong_target.anchor != nullptr) {
        auto rejected_target =
            ingress::CreateFreshActiveRawPosixStreamV1(
                std::move(wrong_target.lease),
                std::move(wrong_target.anchor),
                wrong_target.initial.config,
                wrong_target.backend_options,
                wrong_target.artifact_options,
                wrong_target.limits,
                *wrong_target.coordinator,
                wrong_target.key,
                "sh-tick",
                &wrong_target.error);
        test->Expect(
            !rejected_target.ok() &&
                rejected_target.failure ==
                    ingress::
                        RawFreshActivePosixStreamFailureV1::
                            kInitAuthorization &&
                rejected_target.publication_state ==
                    ingress::
                        RawFreshActivePublicationStateV1::
                            kNotPublished &&
                !rejected_target
                     .initial_segment_published &&
                !rejected_target.requires_fail_stop() &&
                RegistryEntryHasStatus(
                    wrong_target.coordinator->state(),
                    wrong_target.key,
                    ingress::
                        ReserveRegistryStatusV1::
                            kInit) &&
                FileAbsentAt(
                    wrong_target.stream_directory
                        ->descriptor(),
                    ingress::
                        kRawFirstSegmentFilename) &&
                FileAbsentAt(
                    wrong_target.stream_directory
                        ->descriptor(),
                    ingress::kRawControlFilename),
            "wrong canonical target is rejected while registry remains INIT and no runtime can connect");
    }
}

void RunRegisteredFreshRouteComposition(
    TestContext* test) {
    TemporaryDirectory root;
    InitialFixture initial = MakeInitialFixture();
    ingress::RawRecordLayoutV1 layout{};
    test->Expect(
        root.ok() &&
            ingress::ComputeRawRecordLayoutV1(
                13U, &layout) ==
                ingress::RawV1Error::kNone,
        "registered fresh-route fixture prepares root and record layout");
    if (!root.ok() ||
        layout.record_size == 0U) {
        return;
    }

    const ingress::ReserveCoordinatorStateV1 bootstrap =
        MakeCoordinatorBootstrap(root.descriptor());
    const auto marker = MakeCoordinatorMarker(bootstrap);
    ingress::RawReserveCoordinatorErrorV1
        coordinator_failure =
            ingress::RawReserveCoordinatorErrorV1::kNone;
    std::string error;
    auto coordinator =
        ingress::
            PublishFreshRawReserveRegistryCoordinatorAtV1(
                root.descriptor(),
                marker,
                bootstrap,
                &coordinator_failure,
                &error);
    test->Expect(
        coordinator != nullptr,
        "registered fresh-route fixture publishes coordinator");
    if (coordinator == nullptr) {
        return;
    }

    ingress::RawReserveFreshScaffoldingV1
        registration{};
    registration.key.route.source_stream_id =
        initial.segment.source_stream_id;
    registration.key.route.capture_date =
        initial.segment.capture_date;
    registration.key.stream_day_id =
        initial.segment.stream_day_id;
    Fill(
        &registration.key.recovery_attempt_id,
        0xe7U);
    Fill(&registration.writer_instance, 0xc7U);
    registration.recovery_intent =
        ingress::ReserveRecoveryIntentV1::
            kResumeConnect;
    registration.scaffolding_allocation_cap =
        UINT64_C(1) << 20U;
    registration.safe_stop_template_id = 7U;
    test->Expect(
        coordinator->RegisterFreshScaffolding(
            registration, &error) ==
            ingress::RawReserveCoordinatorErrorV1::
                kNone,
        "fresh registration is durable before POSIX composition");

    const std::uint64_t segment_target =
        ingress::kRawV1SegmentHeaderBytes +
        layout.record_size;
    ClockFixture clocks{};
    initial.config.headers_already_persisted = false;
    initial.config.writer_instance =
        registration.writer_instance;
    ingress::RawPosixWalStreamBackendOptionsV1
        backend_options{};
    backend_options.writer_instance =
        registration.writer_instance;
    backend_options.segment_preallocation_bytes =
        segment_target;
    backend_options.maximum_manifest_bytes =
        1024U * 1024U;
    backend_options.realtime_now =
        &ClockFixture::Realtime;
    backend_options.realtime_clock_context = &clocks;
    backend_options.monotonic_now =
        &ClockFixture::Heartbeat;
    backend_options.monotonic_clock_context = &clocks;
    ingress::RawSegmentArtifactOptionsV1
        artifact_options{};
    artifact_options.expected_raw_schema_sha256 =
        initial.segment.raw_schema_sha256;
    artifact_options.maximum_segment_bytes =
        segment_target;
    ingress::RawWalStreamLimitsV1 limits{};
    limits.segment_target_bytes = segment_target;
    limits.segment_max_age_ns =
        std::numeric_limits<std::uint64_t>::max();
    limits.maximum_record_bytes = layout.record_size;
    limits.monotonic_now =
        &ClockFixture::StreamMonotonic;
    limits.monotonic_clock_context = &clocks;

    ingress::RawFreshRoutePosixResultV1 result =
        ingress::CompleteRegisteredFreshRawRouteV1(
            root.path(),
            "sz-tick",
            registration,
            *coordinator,
            initial.config,
            backend_options,
            artifact_options,
            limits,
            &error);
    test->Expect(
        result.ok() &&
            result.stream_directory_durable &&
            result.maintenance_directory_durable &&
            result.writer_lease_durable &&
            result.journal_anchor_durable &&
            result.init_publication_attempted &&
            !result.requires_fail_stop(),
        "registered SCAFFOLDING composes through one ACTIVE-bound owning stream");
    if (!result.ok()) {
        std::cerr
            << ingress::RawFreshRoutePosixFailureV1Name(
                   result.failure)
            << ": " << error << '\n';
        return;
    }
    test->Expect(
        result.active.sink != nullptr &&
            result.active.sink
                ->active_binding_validated() &&
            result.active.sink->authorization_key() ==
                registration.key &&
            result.active.sink->SealAndClose(),
        "composed fresh route is writable, ACTIVE-bound, and cleanly sealable");
}

void RunRealRotation(TestContext* test) {
    TemporaryDirectory directory;
    test->Expect(
        directory.ok(),
        "temporary stream directory opens");
    if (!directory.ok()) {
        return;
    }

    InitialFixture fixture =
        MakeInitialFixture();
    ingress::RawRecordLayoutV1 layout{};
    test->Expect(
        ingress::ComputeRawRecordLayoutV1(
            13U, &layout) ==
            ingress::RawV1Error::kNone,
        "record layout computes");
    const std::uint64_t segment_target =
        ingress::kRawV1SegmentHeaderBytes +
        layout.record_size;

    std::string error;
    std::unique_ptr<ingress::RawWriterLease> lease =
        ingress::AcquireRawWriterLeaseAt(
            directory.descriptor(),
            fixture.segment.source_stream_id,
            fixture.segment.capture_date,
            &error);
    test->Expect(
        lease != nullptr,
        "writer lease acquired");
    if (lease == nullptr) {
        return;
    }

    // The POSIX backend rejects a config which has not already crossed the
    // namespace/bootstrap header barriers.
    ingress::RawWalWriterConfig unpersisted =
        fixture.config;
    unpersisted.headers_already_persisted = false;
    ingress::RawPosixWalStreamBackendOptionsV1
        invalid_options{};
    Fill(&invalid_options.writer_instance, 0xe0U);
    invalid_options.segment_preallocation_bytes =
        segment_target;
    test->Expect(
        ingress::CreateRawPosixWalStreamBackendV1(
            *lease,
            unpersisted,
            invalid_options,
            &error) == nullptr,
        "unpersisted initial config is rejected");

    ingress::RawFreshStateAuthorizationV1
        authorization;
    authorization.source_stream_id =
        fixture.segment.source_stream_id;
    authorization.capture_date =
        fixture.segment.capture_date;
    authorization.stream_day_id =
        fixture.segment.stream_day_id;
    Fill(&authorization.recovery_attempt, 0xd0U);
    authorization.registry_stage =
        ingress::RawFreshRegistryStageV1::
            kScaffolding;
    authorization.durable_state_generation = 1U;
    ExactFreshGate fresh_gate;
    fresh_gate.expected = authorization;
    std::unique_ptr<ingress::RawFreshJournalAnchor>
        anchor =
            ingress::PublishFreshRawJournalAnchor(
                *lease,
                fixture.config.journal_header_wire,
                authorization,
                fresh_gate,
                &error);
    test->Expect(
        anchor != nullptr,
        "fresh SCAFFOLDING journal anchor is durably published");
    if (anchor == nullptr) {
        return;
    }
    authorization.registry_stage =
        ingress::RawFreshRegistryStageV1::kInit;
    authorization.durable_state_generation = 2U;
    fresh_gate.expected = authorization;
    std::unique_ptr<ingress::RawBootstrapFiles>
        bootstrap =
            ingress::CreateInitialRawSegment(
                *lease,
                *anchor,
                fixture.config.segment_header_wire,
                authorization,
                fresh_gate,
                segment_target,
                &error);
    test->Expect(
        bootstrap != nullptr,
        "fresh bootstrap is durably published");
    if (bootstrap == nullptr) {
        return;
    }
    const int segment_fd =
        bootstrap->ReleaseSegmentFd();
    const int journal_fd =
        bootstrap->ReleaseJournalFd();
    std::unique_ptr<ingress::RawWalIo> io =
        ingress::AdoptPosixRawWalIo(
            segment_fd, journal_fd, &error);
    test->Expect(
        io != nullptr,
        "fresh bootstrap descriptors are adopted");
    if (io == nullptr) {
        static_cast<void>(::close(segment_fd));
        static_cast<void>(::close(journal_fd));
        return;
    }

    ClockFixture clocks{};
    ingress::RawPosixWalStreamBackendOptionsV1
        backend_options{};
    Fill(&backend_options.writer_instance, 0xe0U);
    fixture.config.writer_instance =
        backend_options.writer_instance;
    backend_options.segment_preallocation_bytes =
        segment_target;
    backend_options.maximum_manifest_bytes =
        1024U * 1024U;
    backend_options.realtime_now =
        &ClockFixture::Realtime;
    backend_options.realtime_clock_context = &clocks;
    backend_options.monotonic_now =
        &ClockFixture::Heartbeat;
    backend_options.monotonic_clock_context = &clocks;
    std::unique_ptr<
        ingress::RawPosixWalStreamBackendV1>
        backend =
            ingress::CreateRawPosixWalStreamBackendV1(
                *lease,
                fixture.config,
                backend_options,
                &error);
    test->Expect(
        backend != nullptr,
        "POSIX stream backend accepts exact bootstrap config");
    if (backend == nullptr) {
        return;
    }

    ingress::RawSegmentArtifactOptionsV1
        artifact_options{};
    artifact_options.expected_raw_schema_sha256 =
        fixture.segment.raw_schema_sha256;
    artifact_options.maximum_segment_bytes =
        segment_target;
    ingress::RawWalStreamLimitsV1 limits{};
    limits.segment_target_bytes = segment_target;
    limits.segment_max_age_ns =
        std::numeric_limits<std::uint64_t>::max();
    limits.maximum_record_bytes = layout.record_size;
    limits.monotonic_now =
        &ClockFixture::StreamMonotonic;
    limits.monotonic_clock_context = &clocks;

    {
        ingress::RawWalStreamWriter stream(
            fixture.config,
            std::move(io),
            artifact_options,
            limits,
            *backend);
        test->Expect(
            stream.Initialize(),
            "stream initializes segment 1 manifest and control");
        for (std::uint64_t sequence = 1U;
             sequence <= 3U;
             ++sequence) {
            const RecordFixture record =
                MakeRecord(sequence);
            test->Expect(
                stream.AppendRecord(record.input()),
                "record append crosses real POSIX rotation");
        }
        test->Expect(
            stream.rotation_count() == 2U,
            "three records rotate across three segments");
        test->Expect(
            stream.SealAndClose(),
            "final segment seals and publishes artifacts");
        const ingress::RawWalWriterSnapshot snapshot =
            stream.Snapshot();
        test->Expect(
            snapshot.sealed && snapshot.closed &&
                !snapshot.fatal,
            "final writer snapshot is cleanly sealed");
    }

    test->Expect(
        backend->failure() ==
            ingress::
                RawPosixWalStreamBackendFailureV1::kNone,
        "backend remains nonfatal");
    const ingress::RawManifestV1* in_memory_manifest =
        backend->current_manifest();
    test->Expect(
        in_memory_manifest != nullptr &&
            in_memory_manifest->closed_entries.size() ==
                3U &&
            !in_memory_manifest->open_entry.has_value() &&
            in_memory_manifest->manifest_generation ==
                6U,
        "in-memory manifest records three open/close transitions");

    ingress::RawManifestNamespaceV1 expected_namespace{
        fixture.segment.capture_date,
        fixture.segment.source_stream_id,
        fixture.segment.stream_day_id};
    ingress::RawManifestV1 loaded_manifest{};
    test->Expect(
        ingress::LoadCurrentRawManifestAt(
            directory.descriptor(),
            expected_namespace,
            backend_options.maximum_manifest_bytes,
            &loaded_manifest,
            nullptr,
            nullptr,
            &error) ==
            ingress::RawManifestStoreError::kNone,
        "published manifest strictly reloads");
    test->Expect(
        loaded_manifest.closed_entries.size() == 3U &&
            !loaded_manifest.open_entry.has_value() &&
            loaded_manifest.closed_entry_count == 3U,
        "reloaded manifest has the exact closed frontier");

    for (std::uint32_t sequence = 1U;
         sequence <= 3U;
         ++sequence) {
        std::vector<std::byte> segment_bytes;
        std::vector<std::byte> index_bytes;
        test->Expect(
            ReadExactFileAt(
                directory.descriptor(),
                SegmentName(sequence, "raw"),
                &segment_bytes),
            "sealed segment is retained");
        test->Expect(
            segment_bytes.size() == segment_target,
            "sealed segment is truncated to logical end");
        test->Expect(
            ReadExactFileAt(
                directory.descriptor(),
                SegmentName(sequence, "idx"),
                &index_bytes),
            "segment index is durably published");
        ingress::RawIndexFileV1 index{};
        test->Expect(
            ingress::DecodeRawIndexFileV1(
                index_bytes, &index) ==
                ingress::RawIndexV1Error::kNone,
            "published index self-validates");
        test->Expect(
            index.header.segment_sequence == sequence &&
                index.footer.segment_record_count == 1U &&
                index.footer
                        .segment_logical_end_offset ==
                    segment_target,
            "index binds the exact one-record segment");
    }

    std::vector<std::byte> journal_bytes;
    test->Expect(
        ReadExactFileAt(
            directory.descriptor(),
            ingress::kRawJournalFilename,
            &journal_bytes),
        "journal is readable through its final name");
    test->Expect(
        journal_bytes.size() ==
            ingress::kRawV1JournalHeaderBytes +
                6U *
                    ingress::kRawV1DurableMarkerBytes,
        "journal has one header-only and one sealed marker per segment");
    if (journal_bytes.size() ==
        ingress::kRawV1JournalHeaderBytes +
            6U * ingress::kRawV1DurableMarkerBytes) {
        for (std::size_t marker_index = 0U;
             marker_index < 6U;
             ++marker_index) {
            const std::size_t offset =
                ingress::kRawV1JournalHeaderBytes +
                marker_index *
                    ingress::kRawV1DurableMarkerBytes;
            ingress::DurableMarkerV1 marker{};
            test->Expect(
                ingress::DecodeDurableMarkerV1(
                    std::span<const std::byte>(
                        journal_bytes.data() +
                            static_cast<std::ptrdiff_t>(
                                offset),
                        ingress::
                            kRawV1DurableMarkerBytes),
                    &marker) ==
                    ingress::RawV1Error::kNone,
                "journal marker decodes");
            test->Expect(
                marker.segment_sequence ==
                        static_cast<std::uint32_t>(
                            marker_index / 2U + 1U) &&
                    marker.marker_flags ==
                        ((marker_index % 2U) == 0U
                             ? 0U
                             : ingress::
                                   kRawV1SegmentSealed),
                "journal marker chain alternates header-only and sealed");
        }
    }

    std::unique_ptr<ingress::RawControlFileReader>
        control = ingress::OpenRawControlFile(
            directory.descriptor(),
            fixture.segment.source_stream_id,
            fixture.segment.capture_date,
            &error);
    ingress::RawControlSnapshot control_snapshot{};
    test->Expect(
        control != nullptr &&
            control->Read(&control_snapshot),
        "control.page attaches and reads coherently");
    test->Expect(
        control_snapshot.segment_sequence == 3U &&
            control_snapshot.append_ingress_sequence ==
                3U &&
            control_snapshot.append_segment_offset ==
                segment_target &&
            control_snapshot.fatal_state == 0U,
        "control.page exposes the latest complete record cursor");
}

void RunRecoveredClosedResume(TestContext* test) {
    TemporaryDirectory root;
    test->Expect(
        root.ok(),
        "resume Raw root opens");
    if (!root.ok()) {
        return;
    }

    InitialFixture fixture = MakeInitialFixture();
    ingress::RawRecordLayoutV1 layout{};
    if (ingress::ComputeRawRecordLayoutV1(
            13U, &layout) !=
        ingress::RawV1Error::kNone) {
        test->Expect(false, "resume record layout computes");
        return;
    }
    const std::uint64_t segment_target =
        ingress::kRawV1SegmentHeaderBytes +
        layout.record_size;
    std::string error;
    auto stream_directory =
        ingress::OpenOrCreateRawStreamDirectory(
            root.path(),
            fixture.segment.source_stream_id,
            fixture.segment.capture_date,
            "sz-tick",
            &error);
    test->Expect(
        stream_directory != nullptr,
        "canonical resume route is created");
    if (stream_directory == nullptr) {
        return;
    }

    auto initial_lease =
        ingress::AcquireRawWriterLeaseAt(
            stream_directory->descriptor(),
            fixture.segment.source_stream_id,
            fixture.segment.capture_date,
            &error);
    test->Expect(
        initial_lease != nullptr,
        "initial route lease is acquired");
    if (initial_lease == nullptr) {
        return;
    }

    ingress::RawFreshStateAuthorizationV1
        authorization{};
    authorization.source_stream_id =
        fixture.segment.source_stream_id;
    authorization.capture_date =
        fixture.segment.capture_date;
    authorization.stream_day_id =
        fixture.segment.stream_day_id;
    Fill(&authorization.recovery_attempt, 0xd1U);
    authorization.registry_stage =
        ingress::RawFreshRegistryStageV1::
            kScaffolding;
    authorization.durable_state_generation = 1U;
    ExactFreshGate fresh_gate;
    fresh_gate.expected = authorization;
    auto anchor =
        ingress::PublishFreshRawJournalAnchor(
            *initial_lease,
            fixture.config.journal_header_wire,
            authorization,
            fresh_gate,
            &error);
    authorization.registry_stage =
        ingress::RawFreshRegistryStageV1::kInit;
    authorization.durable_state_generation = 2U;
    fresh_gate.expected = authorization;
    auto bootstrap =
        anchor == nullptr
            ? nullptr
            : ingress::CreateInitialRawSegment(
                  *initial_lease,
                  *anchor,
                  fixture.config.segment_header_wire,
                  authorization,
                  fresh_gate,
                  segment_target,
                  &error);
    test->Expect(
        bootstrap != nullptr,
        "initial closed-route bootstrap is durable");
    if (bootstrap == nullptr) {
        return;
    }

    const int initial_segment_fd =
        bootstrap->ReleaseSegmentFd();
    const int initial_journal_fd =
        bootstrap->ReleaseJournalFd();
    auto initial_io =
        ingress::AdoptPosixRawWalIo(
            initial_segment_fd,
            initial_journal_fd,
            &error);
    if (initial_io == nullptr) {
        static_cast<void>(::close(initial_segment_fd));
        static_cast<void>(::close(initial_journal_fd));
        test->Expect(false, "initial Raw descriptors adopt");
        return;
    }

    ClockFixture clocks{};
    ingress::RawPosixWalStreamBackendOptionsV1
        backend_options{};
    Fill(&backend_options.writer_instance, 0xe1U);
    fixture.config.writer_instance =
        backend_options.writer_instance;
    backend_options.segment_preallocation_bytes =
        segment_target;
    backend_options.maximum_manifest_bytes =
        1024U * 1024U;
    backend_options.realtime_now =
        &ClockFixture::Realtime;
    backend_options.realtime_clock_context = &clocks;
    backend_options.monotonic_now =
        &ClockFixture::Heartbeat;
    backend_options.monotonic_clock_context = &clocks;
    auto initial_backend =
        ingress::CreateRawPosixWalStreamBackendV1(
            *initial_lease,
            fixture.config,
            backend_options,
            &error);
    ingress::RawSegmentArtifactOptionsV1
        artifact_options{};
    artifact_options.expected_raw_schema_sha256 =
        fixture.segment.raw_schema_sha256;
    artifact_options.maximum_segment_bytes =
        segment_target;
    ingress::RawWalStreamLimitsV1 limits{};
    limits.segment_target_bytes = segment_target;
    limits.segment_max_age_ns =
        std::numeric_limits<std::uint64_t>::max();
    limits.maximum_record_bytes = layout.record_size;
    limits.monotonic_now =
        &ClockFixture::StreamMonotonic;
    limits.monotonic_clock_context = &clocks;

    ingress::RawWalWriterSnapshot closed_snapshot{};
    ingress::RawManifestV1 closed_manifest{};
    if (initial_backend != nullptr) {
        ingress::RawWalStreamWriter initial_stream(
            fixture.config,
            std::move(initial_io),
            artifact_options,
            limits,
            *initial_backend);
        const RecordFixture first = MakeRecord(1U);
        test->Expect(
            initial_stream.Initialize() &&
                initial_stream.AppendRecord(
                    first.input()) &&
                initial_stream.SealAndClose(),
            "initial route reaches a real closed-only frontier");
        closed_snapshot = initial_stream.Snapshot();
        if (initial_backend->current_manifest() !=
            nullptr) {
            closed_manifest =
                *initial_backend->current_manifest();
        }
    }
    test->Expect(
        closed_snapshot.sealed &&
            closed_snapshot.closed &&
            !closed_manifest.closed_entries.empty() &&
            !closed_manifest.open_entry.has_value(),
        "closed recovery input is captured exactly");
    initial_backend.reset();
    initial_lease.reset();
    if (!closed_snapshot.sealed ||
        closed_manifest.closed_entries.empty()) {
        return;
    }

    const ingress::ReserveCoordinatorStateV1
        coordinator_bootstrap =
            MakeCoordinatorBootstrap(
                root.descriptor());
    const auto coordinator_marker =
        MakeCoordinatorMarker(
            coordinator_bootstrap);
    ingress::RawReserveCoordinatorErrorV1
        coordinator_failure =
            ingress::RawReserveCoordinatorErrorV1::
                kNone;
    auto coordinator =
        ingress::
            PublishFreshRawReserveRegistryCoordinatorAtV1(
                root.descriptor(),
                coordinator_marker,
                coordinator_bootstrap,
                &coordinator_failure,
                &error);
    ingress::RawReserveRegistryEntryKeyV1 key{};
    key.route.source_stream_id =
        fixture.segment.source_stream_id;
    key.route.capture_date =
        fixture.segment.capture_date;
    key.stream_day_id =
        fixture.segment.stream_day_id;
    Fill(&key.recovery_attempt_id, 0xd2U);
    ingress::RawReserveExistingAnchorRecoveryV1
        registration{};
    registration.key = key;
    registration.writer_instance =
        backend_options.writer_instance;
    registration.recovery_intent =
        ingress::ReserveRecoveryIntentV1::
            kResumeConnect;
    registration.safe_stop_template_id = 7U;
    test->Expect(
        coordinator != nullptr &&
            coordinator->RegisterExistingAnchorRecovering(
                registration,
                &error) ==
                ingress::RawReserveCoordinatorErrorV1::
                    kNone,
        "existing closed route enters durable RECOVERING");
    if (coordinator == nullptr) {
        return;
    }

    auto invalid_recovery_lease =
        ingress::AcquireRawWriterLeaseAtV1(
            stream_directory->descriptor(),
            fixture.segment.source_stream_id,
            fixture.segment.capture_date,
            key.recovery_attempt_id,
            &error);
    ingress::RawRecoveredClosedWalStateV1
        recovered{};
    recovered.closed_manifest =
        closed_manifest;
    recovered.journal_header_wire =
        fixture.config.journal_header_wire;
    recovered.terminal_segment =
        fixture.segment;
    recovered.accepted_sealed_cursor =
        closed_snapshot.durable;
    recovered.journal_logical_size =
        closed_snapshot.journal_logical_size;

    ingress::RawRecoveredClosedWalStateV1
        wrong_cursor = recovered;
    ++wrong_cursor.accepted_sealed_cursor
          .ingress_sequence;
    auto rejected =
        ingress::CreateRecoveredClosedRawPosixStreamV1(
            std::move(invalid_recovery_lease),
            wrong_cursor,
            backend_options,
            artifact_options,
            limits,
            2'000U,
            *coordinator,
            key,
            "sz-tick",
            &error);
    struct stat absent_next {};
    errno = 0;
    const bool next_is_absent =
        ::fstatat(
            stream_directory->descriptor(),
            "segment-00000002.raw",
            &absent_next,
            AT_SYMLINK_NOFOLLOW) != 0 &&
        errno == ENOENT;
    test->Expect(
        rejected == nullptr && next_is_absent,
        "mismatched accepted seal cursor is rejected before namespace mutation");

    auto recovery_lease =
        ingress::AcquireRawWriterLeaseAtV1(
            stream_directory->descriptor(),
            fixture.segment.source_stream_id,
            fixture.segment.capture_date,
            key.recovery_attempt_id,
            &error);
    auto resumed =
        ingress::CreateRecoveredClosedRawPosixStreamV1(
            std::move(recovery_lease),
            recovered,
            backend_options,
            artifact_options,
            limits,
            2'000U,
            *coordinator,
            key,
            "sz-tick",
            &error);
    test->Expect(
        resumed != nullptr,
        "closed recovery frontier creates an authorized next open segment");
    if (resumed == nullptr) {
        std::cerr << error << '\n';
        return;
    }
    const ingress::RawWalSinkIdentityV1
        resumed_identity = resumed->identity();
    const ingress::RawWalWriterSnapshot
        resumed_snapshot = resumed->Snapshot();
    test->Expect(
        resumed_identity.segment_sequence == 2U &&
            resumed_identity.segment_base_wal_pos ==
                closed_snapshot.durable
                    .global_wal_pos &&
            resumed_identity.first_ingress_sequence ==
                closed_snapshot.durable
                        .ingress_sequence +
                    1U &&
            resumed_snapshot.initialized &&
            !resumed_snapshot.sealed &&
            resumed_snapshot.durable
                    .segment_offset ==
                ingress::kRawV1SegmentHeaderBytes,
        "resumed stream identity and initialized cursor derive from the exact seal");

    ingress::RawManifestV1 open_manifest{};
    test->Expect(
        ingress::LoadCurrentRawManifestAt(
            stream_directory->descriptor(),
            {fixture.segment.capture_date,
             fixture.segment.source_stream_id,
             fixture.segment.stream_day_id},
            backend_options.maximum_manifest_bytes,
            &open_manifest,
            nullptr,
            nullptr,
            &error) ==
                ingress::RawManifestStoreError::kNone &&
            open_manifest.closed_entries.size() == 1U &&
            open_manifest.open_entry.has_value() &&
            open_manifest.open_entry
                    ->segment_sequence == 2U,
        "resumed open manifest is durably published");
    auto control = ingress::OpenRawControlFile(
        stream_directory->descriptor(),
        fixture.segment.source_stream_id,
        fixture.segment.capture_date,
        &error);
    ingress::RawControlSnapshot control_snapshot{};
    test->Expect(
        control != nullptr &&
            control->Read(&control_snapshot) &&
            control_snapshot.writer_instance ==
                backend_options.writer_instance &&
            control_snapshot.segment_sequence == 2U,
        "resumed control boundary exposes the exact current writer");

    test->Expect(
        !resumed->active_binding_validated(),
        "resumed open stream remains RECOVERING until an opaque activation receipt is consumed");
}

}  // namespace

int main() {
    TestContext test;
    RunFreshActiveFactory(&test);
    RunRegisteredFreshRouteComposition(&test);
    RunRealRotation(&test);
    RunRecoveredClosedResume(&test);
    if (test.failures != 0) {
        std::cerr << test.failures
                  << " POSIX Raw WAL stream test(s) failed\n";
        return 1;
    }
    std::cout << "phase2 POSIX Raw WAL stream tests passed\n";
    return 0;
}
