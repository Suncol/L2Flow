#include "l2flow/ingress/raw_namespace.h"

#include "l2flow/common/sha256.h"
#include "l2flow/ingress/raw_posix_io.h"
#include "l2flow/ingress/raw_wal_writer.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace common = l2flow::common;
namespace ingress = l2flow::ingress;

namespace {

struct TestContext final {
    void Expect(bool condition, const std::string& description) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << description << '\n';
        }
    }

    int failures = 0;
};

class TemporaryRoot final {
public:
    TemporaryRoot() {
        std::array<char, 64U> path{};
        const std::string pattern =
            "/tmp/l2flow-raw-root-XXXXXX";
        std::copy(pattern.begin(), pattern.end(), path.begin());
        const char* const created = ::mkdtemp(path.data());
        if (created != nullptr) {
            path_ = created;
            static_cast<void>(::chmod(path_.c_str(), 0700));
        }
    }

    ~TemporaryRoot() {
        if (!path_.empty()) {
            std::error_code ignored;
            std::filesystem::remove_all(path_, ignored);
        }
    }

    [[nodiscard]] const std::string& path() const noexcept {
        return path_;
    }

private:
    std::string path_;
};

template <std::size_t Size>
void FillNonZero(std::array<std::byte, Size>* value,
                 std::uint8_t seed) {
    for (std::size_t index = 0U; index < Size; ++index) {
        (*value)[index] = static_cast<std::byte>(
            static_cast<std::uint8_t>(
                seed + static_cast<std::uint8_t>(index)));
    }
}

void MakeHeaders(
    ingress::RawV1SegmentHeaderWire* segment_wire,
    ingress::RawV1JournalHeaderWire* journal_wire) {
    ingress::SegmentHeaderV1 segment;
    segment.source_stream_id = 1001U;
    segment.capture_date = 20260718U;
    FillNonZero(&segment.stream_day_id, 1U);
    segment.segment_sequence = 1U;
    segment.first_ingress_sequence = 1U;
    segment.created_realtime_ns = 1U;
    segment.created_monotonic_ns = 2U;
    FillNonZero(&segment.host_uuid, 3U);
    FillNonZero(&segment.linux_boot_id, 4U);
    segment.clock_epoch_algorithm = 1U;
    FillNonZero(&segment.clock_epoch_digest, 5U);
    FillNonZero(&segment.sdk_archive_sha256, 6U);
    FillNonZero(&segment.libmdl_api_sha256, 7U);
    FillNonZero(&segment.endpoint_contract_sha256, 8U);
    FillNonZero(&segment.config_sha256, 9U);
    FillNonZero(&segment.raw_schema_sha256, 10U);
    FillNonZero(&segment.build_manifest_sha256, 11U);
    static_cast<void>(
        ingress::EncodeSegmentHeaderV1(
            segment, segment_wire));

    ingress::DurableJournalHeaderV1 journal;
    journal.capture_date = segment.capture_date;
    journal.source_stream_id = segment.source_stream_id;
    journal.stream_day_id = segment.stream_day_id;
    journal.raw_schema_sha256 = segment.raw_schema_sha256;
    journal.created_host_uuid = segment.host_uuid;
    journal.created_linux_boot_id = segment.linux_boot_id;
    journal.created_clock_epoch_algorithm =
        segment.clock_epoch_algorithm;
    journal.created_clock_epoch_digest =
        segment.clock_epoch_digest;
    journal.created_clock_epoch_label =
        segment.clock_epoch_label;
    static_cast<void>(
        ingress::EncodeDurableJournalHeaderV1(
            journal, journal_wire));
}

bool SameAuthorization(
    const ingress::RawFreshStateAuthorizationV1& left,
    const ingress::RawFreshStateAuthorizationV1& right) {
    return left.source_stream_id ==
               right.source_stream_id &&
           left.capture_date == right.capture_date &&
           left.stream_day_id == right.stream_day_id &&
           left.recovery_attempt ==
               right.recovery_attempt &&
           left.registry_stage == right.registry_stage &&
           left.durable_state_generation ==
               right.durable_state_generation;
}

class ExactAuthorizationGate final
    : public ingress::RawFreshMutationAuthorizationGateV1 {
public:
    void Expect(
        const ingress::RawFreshStateAuthorizationV1&
            expected) {
        expected_ = expected;
    }

    [[nodiscard]] bool Authorizes(
        const ingress::RawFreshStateAuthorizationV1&
            facts) const noexcept override {
        return SameAuthorization(facts, expected_);
    }

private:
    ingress::RawFreshStateAuthorizationV1 expected_{};
};

ingress::RawFreshStateAuthorizationV1
MakeScaffoldingAuthorization(
    const ingress::RawV1SegmentHeaderWire&
        segment_header) {
    ingress::SegmentHeaderV1 decoded;
    static_cast<void>(
        ingress::DecodeSegmentHeaderV1(
            segment_header, &decoded));
    ingress::RawFreshStateAuthorizationV1 facts;
    facts.source_stream_id =
        decoded.source_stream_id;
    facts.capture_date = decoded.capture_date;
    facts.stream_day_id = decoded.stream_day_id;
    FillNonZero(&facts.recovery_attempt, 0xd0U);
    facts.registry_stage =
        ingress::RawFreshRegistryStageV1::
            kScaffolding;
    facts.durable_state_generation = 7U;
    return facts;
}

void TestFreshInventoryRejectsUnexpectedEntries(
    TestContext* test,
    const std::string& raw_root,
    const ingress::RawV1SegmentHeaderWire&
        segment_header,
    const ingress::RawV1JournalHeaderWire&
        journal_header) {
    const std::array<const char*, 5U> names{{
        "segment-00000002.raw",
        "segment-00000001.idx",
        "manifest.json",
        "control.page",
        "unexpected-object"}};
    const std::array<const char*, 5U> slugs{{
        "reject-seq-two",
        "reject-index",
        "reject-manifest",
        "reject-control",
        "reject-unknown"}};
    const auto scaffolding =
        MakeScaffoldingAuthorization(segment_header);
    for (std::size_t index = 0U;
         index < names.size();
         ++index) {
        std::string error;
        std::unique_ptr<ingress::RawStreamDirectory>
            directory =
                ingress::OpenOrCreateRawStreamDirectory(
                    raw_root,
                    1001U,
                    20260718U,
                    slugs[index],
                    &error);
        std::unique_ptr<ingress::RawWriterLease> lease;
        if (directory != nullptr) {
            lease = ingress::AcquireRawWriterLeaseAt(
                directory->descriptor(),
                1001U,
                20260718U,
                &error);
        }
        test->Expect(
            lease != nullptr,
            "unexpected-entry fixture acquires its writer lease");
        if (lease == nullptr) {
            continue;
        }
        const int artifact_fd = ::openat(
            directory->descriptor(),
            names[index],
            O_WRONLY | O_CREAT | O_EXCL |
                O_NOFOLLOW | O_CLOEXEC,
            0600);
        test->Expect(
            artifact_fd >= 0,
            "unexpected fresh artifact fixture is created");
        if (artifact_fd >= 0) {
            static_cast<void>(::close(artifact_fd));
        }
        ExactAuthorizationGate gate;
        gate.Expect(scaffolding);
        std::unique_ptr<ingress::RawFreshJournalAnchor>
            anchor =
                ingress::PublishFreshRawJournalAnchor(
                    *lease,
                    journal_header,
                    scaffolding,
                    gate,
                    &error);
        struct stat status {};
        test->Expect(
            anchor == nullptr &&
                ::fstatat(
                    directory->descriptor(),
                    ingress::kRawJournalFilename,
                    &status,
                    AT_SYMLINK_NOFOLLOW) != 0 &&
                errno == ENOENT,
            std::string(
                "fresh inventory rejects before anchor: ") +
                names[index]);
    }
}

}  // namespace

int main() {
    TestContext test;
    test.Expect(
        ingress::CanonicalRawStreamSlugV1(1001U) ==
                std::optional<std::string_view>(
                    "sh-snapshot") &&
            ingress::CanonicalRawStreamSlugV1(1002U) ==
                std::optional<std::string_view>(
                    "sh-tick") &&
            ingress::CanonicalRawStreamSlugV1(2001U) ==
                std::optional<std::string_view>(
                    "sz-snapshot") &&
            ingress::CanonicalRawStreamSlugV1(2002U) ==
                std::optional<std::string_view>(
                    "sz-tick") &&
            !ingress::CanonicalRawStreamSlugV1(7U)
                 .has_value() &&
            ingress::IsCanonicalRawStreamRouteV1(
                1001U, "sh-snapshot") &&
            !ingress::IsCanonicalRawStreamRouteV1(
                1001U, "sh-tick") &&
            !ingress::IsCanonicalRawStreamRouteV1(
                7U, "test"),
        "production Raw route mapping is the exact frozen four-stream namespace");
    TemporaryRoot root;
    test.Expect(!root.path().empty(), "private Raw root is created");

    std::string error;
    std::unique_ptr<ingress::RawStreamDirectory> directory =
        ingress::OpenOrCreateRawStreamDirectory(
            root.path(),
            1001U,
            20260718U,
            "sh-snapshot",
            &error);
    test.Expect(
        directory != nullptr,
        "date and stream directories are durably created");
    if (directory == nullptr) {
        std::cerr << error << '\n';
        return 1;
    }
    ingress::RawV1SegmentHeaderWire segment_header{};
    ingress::RawV1JournalHeaderWire journal_header{};
    MakeHeaders(&segment_header, &journal_header);
    const auto scaffolding =
        MakeScaffoldingAuthorization(segment_header);
    ExactAuthorizationGate gate;
    gate.Expect(scaffolding);
    test.Expect(
        ingress::
            CreateOrAdoptAuthorizedFreshRawMaintenanceDirectoryV1(
                *directory,
                scaffolding,
                gate,
                &error),
        "SCAFFOLDING durably creates the empty maintenance directory before writer lease");
    struct stat maintenance_status {};
    test.Expect(
        ::fstatat(
            directory->descriptor(),
            "maintenance",
            &maintenance_status,
            AT_SYMLINK_NOFOLLOW) == 0 &&
            S_ISDIR(maintenance_status.st_mode) &&
            (maintenance_status.st_mode & 07777) ==
                0700,
        "fresh maintenance scaffold is exact owner-only directory");
    test.Expect(
        ingress::
            CreateOrAdoptAuthorizedFreshRawMaintenanceDirectoryV1(
                *directory,
                scaffolding,
                gate,
                &error),
        "same SCAFFOLDING generation idempotently re-establishes maintenance barriers");

    std::unique_ptr<ingress::RawWriterLease> lease =
        ingress::AcquireRawWriterLeaseAt(
            directory->descriptor(),
            1001U,
            20260718U,
            &error);
    test.Expect(lease != nullptr, "writer lease is acquired");
    if (lease == nullptr) {
        return 1;
    }
    test.Expect(
        !ingress::
             CreateOrAdoptAuthorizedFreshRawMaintenanceDirectoryV1(
                 *directory,
                 scaffolding,
                 gate,
                 &error),
        "maintenance scaffolding cannot be invoked after writer-lease publication");

    TestFreshInventoryRejectsUnexpectedEntries(
        &test,
        root.path(),
        segment_header,
        journal_header);

    std::unique_ptr<ingress::RawFreshJournalAnchor>
        anchor =
            ingress::PublishFreshRawJournalAnchor(
                *lease,
                journal_header,
                scaffolding,
                gate,
                &error);
    test.Expect(
        anchor != nullptr,
        "SCAFFOLDING publishes only the retained journal anchor");
    if (anchor == nullptr) {
        std::cerr << error << '\n';
        return 1;
    }

    auto missing_init = scaffolding;
    gate.Expect(missing_init);
    std::unique_ptr<ingress::RawBootstrapFiles>
        rejected_missing_init =
            ingress::CreateInitialRawSegment(
                *lease,
                *anchor,
                segment_header,
                missing_init,
                gate,
                8192U,
                &error);
    test.Expect(
        rejected_missing_init == nullptr,
        "SCAFFOLDING authorization cannot create the initial segment");

    auto same_generation = scaffolding;
    same_generation.registry_stage =
        ingress::RawFreshRegistryStageV1::kInit;
    gate.Expect(same_generation);
    std::unique_ptr<ingress::RawBootstrapFiles>
        rejected_same_generation =
            ingress::CreateInitialRawSegment(
                *lease,
                *anchor,
                segment_header,
                same_generation,
                gate,
                8192U,
                &error);
    test.Expect(
        rejected_same_generation == nullptr,
        "INIT at the SCAFFOLDING generation cannot create a segment");

    auto init = same_generation;
    init.durable_state_generation =
        scaffolding.durable_state_generation + 1U;
    gate.Expect(init);
    const int intervening_fd = ::openat(
        directory->descriptor(),
        "control.page",
        O_WRONLY | O_CREAT | O_EXCL |
            O_NOFOLLOW | O_CLOEXEC,
        0600);
    test.Expect(
        intervening_fd >= 0,
        "post-anchor unauthorized artifact fixture is created");
    if (intervening_fd >= 0) {
        static_cast<void>(::close(intervening_fd));
    }
    std::unique_ptr<ingress::RawBootstrapFiles>
        rejected_changed_inventory =
            ingress::CreateInitialRawSegment(
                *lease,
                *anchor,
                segment_header,
                init,
                gate,
                8192U,
                &error);
    struct stat absent_segment {};
    test.Expect(
        rejected_changed_inventory == nullptr &&
            ::fstatat(
                directory->descriptor(),
                ingress::kRawFirstSegmentFilename,
                &absent_segment,
                AT_SYMLINK_NOFOLLOW) != 0 &&
            errno == ENOENT,
        "INIT re-inventories and rejects an artifact added after the anchor");
    test.Expect(
        ::unlinkat(
            directory->descriptor(),
            "control.page",
            0) == 0,
        "post-anchor inventory fixture is removed");

    std::unique_ptr<ingress::RawBootstrapFiles> bootstrap =
        ingress::CreateInitialRawSegment(
            *lease,
            *anchor,
            segment_header,
            init,
            gate,
            8192U,
            &error);
    test.Expect(
        bootstrap != nullptr,
        "higher-generation durable INIT publishes the preallocated segment");
    if (bootstrap == nullptr) {
        std::cerr << error << '\n';
        return 1;
    }

    const int segment_fd = bootstrap->ReleaseSegmentFd();
    const int journal_fd = bootstrap->ReleaseJournalFd();
    struct stat segment_status {};
    struct stat journal_status {};
    test.Expect(
        ::fstat(segment_fd, &segment_status) == 0 &&
            segment_status.st_size == 8192 &&
            ::fstat(journal_fd, &journal_status) == 0 &&
            journal_status.st_size == 4096,
        "fresh files have preallocation and exact anchor sizes");

    std::unique_ptr<ingress::RawWalIo> io =
        ingress::AdoptPosixRawWalIo(
            segment_fd, journal_fd, &error);
    test.Expect(io != nullptr, "fresh retained fds pass POSIX gates");
    ingress::RawWalWriterConfig writer_config;
    writer_config.segment_header_wire = segment_header;
    writer_config.journal_header_wire = journal_header;
    writer_config.source_stream_id = 1001U;
    writer_config.capture_date = 20260718U;
    writer_config.segment_sequence = 1U;
    writer_config.segment_base_wal_pos = 0U;
    writer_config.first_ingress_sequence = 1U;
    writer_config.initial_durable_ingress_sequence = 0U;
    writer_config.headers_already_persisted = true;
    ingress::RawWalWriter writer(
        writer_config, std::move(io));
    test.Expect(
        writer.Initialize(),
        "real POSIX writer publishes the initial durable marker");
    const ingress::RawWalWriterSnapshot initialized =
        writer.Snapshot();
    test.Expect(
        initialized.append.global_wal_pos == 4096U &&
            initialized.durable.global_wal_pos == 4096U &&
            initialized.durable.ingress_sequence == 0U,
        "initial exclusive cursor is the header-only boundary");
    test.Expect(
        writer.SealAndClose(),
        "empty segment seals and closes through real descriptors");

    ingress::SegmentHeaderV1 first_header;
    test.Expect(
        ingress::DecodeSegmentHeaderV1(
            segment_header, &first_header) ==
            ingress::RawV1Error::kNone,
        "first segment header decodes for rotation");
    const ingress::RawWalRotationPlan rotation =
        ingress::PlanRawWalRotation(
            first_header, writer.Snapshot());
    test.Expect(
        rotation.ok(),
        "sealed first segment yields a rotation plan");
    ingress::SegmentHeaderV1 second_header =
        first_header;
    second_header.segment_sequence =
        rotation.next_segment_sequence;
    second_header.segment_flags =
        rotation.next_segment_flags;
    second_header.segment_base_wal_pos =
        rotation.next_segment_base_wal_pos;
    second_header.first_ingress_sequence =
        rotation.next_first_ingress_sequence;
    second_header.created_realtime_ns += 1U;
    second_header.created_monotonic_ns += 1U;
    ingress::RawV1SegmentHeaderWire second_header_wire{};
    test.Expect(
        ingress::EncodeSegmentHeaderV1(
            second_header,
            &second_header_wire) ==
            ingress::RawV1Error::kNone,
        "second segment header encodes");
    std::unique_ptr<ingress::RawBootstrapFiles> rotated =
        ingress::CreateRotatedRawBootstrap(
            *lease,
            second_header_wire,
            journal_header,
            rotation.existing_journal,
            8192U,
            &error);
    test.Expect(
        rotated != nullptr,
        "rotation securely reopens the journal and publishes segment 2");
    if (rotated != nullptr) {
        std::unique_ptr<ingress::RawWalIo> rotated_io =
            ingress::AdoptPosixRawWalIo(
                rotated->ReleaseSegmentFd(),
                rotated->ReleaseJournalFd(),
                &error);
        ingress::RawWalWriterConfig second_config;
        second_config.segment_header_wire =
            second_header_wire;
        second_config.journal_header_wire =
            journal_header;
        second_config.source_stream_id = 1001U;
        second_config.capture_date = 20260718U;
        second_config.segment_sequence =
            rotation.next_segment_sequence;
        second_config.segment_base_wal_pos =
            rotation.next_segment_base_wal_pos;
        second_config.first_ingress_sequence =
            rotation.next_first_ingress_sequence;
        second_config.initial_durable_ingress_sequence =
            rotation.initial_durable_ingress_sequence;
        second_config.initialization_mode =
            ingress::RawWalInitializationMode::
                kExistingJournal;
        second_config.existing_journal =
            rotation.existing_journal;
        second_config.headers_already_persisted = true;
        ingress::RawWalWriter second_writer(
            second_config, std::move(rotated_io));
        test.Expect(
            second_writer.Initialize() &&
                second_writer.SealAndClose(),
            "real rotated writer appends header-only and seal markers");
        const ingress::RawWalWriterSnapshot second_snapshot =
            second_writer.Snapshot();
        test.Expect(
            second_snapshot.durable.segment_offset ==
                    ingress::kRawV1SegmentHeaderBytes &&
                second_snapshot.durable.ingress_sequence ==
                    0U &&
                second_snapshot.journal_logical_size ==
                    writer.Snapshot().journal_logical_size +
                        2U *
                            ingress::kRawV1DurableMarkerBytes,
            "empty rotation preserves ingress and extends the journal exactly");
    }

    gate.Expect(scaffolding);
    std::unique_ptr<ingress::RawFreshJournalAnchor>
        duplicate =
            ingress::PublishFreshRawJournalAnchor(
            *lease,
            journal_header,
            scaffolding,
            gate,
            &error);
    test.Expect(
        duplicate == nullptr,
        "fresh bootstrap never overwrites an existing namespace");

    if (test.failures != 0) {
        std::cerr << test.failures
                  << " Phase 2 Raw namespace tests failed\n";
        return 1;
    }
    std::cout << "Phase 2 Raw namespace tests passed\n";
    return 0;
}
