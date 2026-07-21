#include "l2flow/ingress/raw_namespace.h"
#include "l2flow/ingress/raw_manifest_transition.h"
#include "l2flow/ingress/raw_posix_io.h"
#include "l2flow/ingress/raw_recovery.h"
#include "l2flow/ingress/raw_recovery_executor.h"
#include "l2flow/ingress/raw_recovery_posix.h"
#include "l2flow/ingress/raw_v1.h"
#include "l2flow/ingress/raw_wal_writer.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <dirent.h>
#include <filesystem>
#include <fcntl.h>
#include <iostream>
#include <memory>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include <sys/stat.h>
#include <unistd.h>

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
        constexpr std::string_view pattern =
            "/tmp/l2flow-recovery-posix-XXXXXX";
        std::copy(
            pattern.begin(), pattern.end(), path.begin());
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
void FillNonzero(
    std::array<std::byte, Size>* bytes,
    std::uint8_t seed) {
    for (std::size_t index = 0U; index < Size; ++index) {
        (*bytes)[index] = static_cast<std::byte>(
            static_cast<std::uint8_t>(
                seed + static_cast<std::uint8_t>(index)));
    }
}

void StoreU16(
    std::uint16_t value,
    std::span<std::byte> output,
    std::size_t offset) {
    output[offset] = static_cast<std::byte>(value & 0xffU);
    output[offset + 1U] =
        static_cast<std::byte>((value >> 8U) & 0xffU);
}

void StoreU32(
    std::uint32_t value,
    std::span<std::byte> output,
    std::size_t offset) {
    for (std::size_t index = 0U; index < 4U; ++index) {
        output[offset + index] = static_cast<std::byte>(
            (value >>
             static_cast<unsigned int>(index * 8U)) &
            0xffU);
    }
}

void StoreU64(
    std::uint64_t value,
    std::span<std::byte> output,
    std::size_t offset) {
    for (std::size_t index = 0U; index < 8U; ++index) {
        output[offset + index] = static_cast<std::byte>(
            (value >>
             static_cast<unsigned int>(index * 8U)) &
            0xffU);
    }
}

ingress::RawV1VendorHead MakeVendorHead(
    std::uint32_t body_size,
    std::uint64_t vendor_sequence) {
    ingress::RawV1VendorHead head{};
    std::span<std::byte> bytes(head);
    head[0] = static_cast<std::byte>(
        ingress::kVendorMessageHeadBytes);
    StoreU32(
        static_cast<std::uint32_t>(
            ingress::kVendorMessageHeadBytes) +
            body_size,
        bytes,
        1U);
    head[5] = std::byte{2U};
    head[6] = std::byte{3U};
    StoreU16(101U, bytes, 7U);
    StoreU16(3001U, bytes, 9U);
    StoreU32(0x12345678U, bytes, 11U);
    StoreU64(vendor_sequence, bytes, 15U);
    return head;
}

struct RecordFixture final {
    ingress::CaptureMetaV1 meta{};
    ingress::RawV1VendorHead head{};
    std::vector<std::byte> body;

    [[nodiscard]] ingress::RawWalRecordInputV1 wal_input()
        const noexcept {
        return {meta, head, body};
    }

    [[nodiscard]] std::vector<std::byte> Encode() const {
        ingress::RawRecordInputV1 input;
        input.meta = meta;
        input.vendor_head = head;
        input.vendor_body = body;
        std::vector<std::byte> wire;
        if (ingress::EncodeRawRecordV1(
                input, &wire, nullptr) !=
            ingress::RawV1Error::kNone) {
            wire.clear();
        }
        return wire;
    }
};

RecordFixture MakeRecord(std::uint64_t ingress_sequence) {
    RecordFixture record;
    record.meta.source_stream_id = 1001U;
    record.meta.connection_epoch_hint = 7U;
    record.meta.ingress_sequence = ingress_sequence;
    record.meta.recv_realtime_ns =
        10'000U + ingress_sequence;
    record.meta.recv_monotonic_ns =
        5'000U + ingress_sequence;
    record.meta.capture_date = 20260718U;
    record.head = MakeVendorHead(
        13U, 900U + ingress_sequence);
    record.body.resize(13U);
    for (std::size_t index = 0U;
         index < record.body.size();
         ++index) {
        record.body[index] =
            static_cast<std::byte>(
                static_cast<std::uint8_t>(index + 11U));
    }
    return record;
}

void MakeHeaders(
    ingress::RawV1SegmentHeaderWire* segment_wire,
    ingress::RawV1JournalHeaderWire* journal_wire) {
    ingress::SegmentHeaderV1 segment;
    segment.source_stream_id = 1001U;
    segment.capture_date = 20260718U;
    FillNonzero(&segment.stream_day_id, 1U);
    segment.segment_sequence = 1U;
    segment.segment_base_wal_pos = 0U;
    segment.first_ingress_sequence = 1U;
    segment.created_realtime_ns = 1U;
    segment.created_monotonic_ns = 2U;
    FillNonzero(&segment.host_uuid, 3U);
    FillNonzero(&segment.linux_boot_id, 4U);
    segment.clock_epoch_algorithm = 1U;
    FillNonzero(&segment.clock_epoch_digest, 5U);
    FillNonzero(&segment.sdk_archive_sha256, 6U);
    FillNonzero(&segment.libmdl_api_sha256, 7U);
    FillNonzero(&segment.endpoint_contract_sha256, 8U);
    FillNonzero(&segment.config_sha256, 9U);
    FillNonzero(&segment.raw_schema_sha256, 10U);
    FillNonzero(&segment.build_manifest_sha256, 11U);
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

class NamespaceFixture final {
public:
    explicit NamespaceFixture(
        std::uint64_t preallocation_bytes = 8192U) {
        if (root_.path().empty()) {
            return;
        }
        directory_ =
            ingress::OpenOrCreateRawStreamDirectory(
                root_.path(),
                1001U,
                20260718U,
                "sh-snapshot",
                &error_);
        if (directory_ == nullptr) {
            return;
        }
        lease_ = ingress::AcquireRawWriterLeaseAt(
            directory_->descriptor(),
            1001U,
            20260718U,
            &error_);
        if (lease_ == nullptr) {
            return;
        }
        MakeHeaders(&segment_header_, &journal_header_);
        ingress::SegmentHeaderV1 decoded;
        if (ingress::DecodeSegmentHeaderV1(
                segment_header_, &decoded) !=
            ingress::RawV1Error::kNone) {
            return;
        }
        ingress::RawFreshStateAuthorizationV1
            authorization;
        authorization.source_stream_id =
            decoded.source_stream_id;
        authorization.capture_date =
            decoded.capture_date;
        authorization.stream_day_id =
            decoded.stream_day_id;
        FillNonzero(
            &authorization.recovery_attempt,
            0xd0U);
        authorization.registry_stage =
            ingress::RawFreshRegistryStageV1::
                kScaffolding;
        authorization.durable_state_generation = 1U;
        ExactFreshGate gate;
        gate.expected = authorization;
        std::unique_ptr<ingress::RawFreshJournalAnchor>
            anchor =
                ingress::PublishFreshRawJournalAnchor(
                    *lease_,
                    journal_header_,
                    authorization,
                    gate,
                    &error_);
        if (anchor == nullptr) {
            return;
        }
        authorization.registry_stage =
            ingress::RawFreshRegistryStageV1::kInit;
        authorization.durable_state_generation = 2U;
        gate.expected = authorization;
        std::unique_ptr<ingress::RawBootstrapFiles> bootstrap =
            ingress::CreateInitialRawSegment(
                *lease_,
                *anchor,
                segment_header_,
                authorization,
                gate,
                preallocation_bytes,
                &error_);
        if (bootstrap == nullptr) {
            return;
        }
        segment_fd_ = bootstrap->ReleaseSegmentFd();
        journal_fd_ = bootstrap->ReleaseJournalFd();
    }

    ~NamespaceFixture() {
        if (segment_fd_ >= 0) {
            static_cast<void>(::close(segment_fd_));
        }
        if (journal_fd_ >= 0) {
            static_cast<void>(::close(journal_fd_));
        }
    }

    [[nodiscard]] bool ok() const noexcept {
        return directory_ != nullptr &&
               lease_ != nullptr &&
               segment_fd_ >= 0 &&
               journal_fd_ >= 0;
    }
    [[nodiscard]] int directory_fd() const noexcept {
        return directory_ == nullptr
                   ? -1
                   : directory_->descriptor();
    }
    [[nodiscard]] ingress::RawWriterLease& lease() noexcept {
        return *lease_;
    }
    [[nodiscard]] const ingress::RawV1SegmentHeaderWire&
    segment_header() const noexcept {
        return segment_header_;
    }
    [[nodiscard]] const ingress::RawV1JournalHeaderWire&
    journal_header() const noexcept {
        return journal_header_;
    }
    [[nodiscard]] int ReleaseSegmentFd() noexcept {
        return std::exchange(segment_fd_, -1);
    }
    [[nodiscard]] int ReleaseJournalFd() noexcept {
        return std::exchange(journal_fd_, -1);
    }
    void CloseBootstrapDescriptors() noexcept {
        if (segment_fd_ >= 0) {
            static_cast<void>(::close(segment_fd_));
            segment_fd_ = -1;
        }
        if (journal_fd_ >= 0) {
            static_cast<void>(::close(journal_fd_));
            journal_fd_ = -1;
        }
    }
    void ReleaseOriginalLease() noexcept {
        lease_.reset();
    }
    [[nodiscard]] const std::string& error() const noexcept {
        return error_;
    }

private:
    TemporaryRoot root_;
    std::unique_ptr<ingress::RawStreamDirectory> directory_;
    std::unique_ptr<ingress::RawWriterLease> lease_;
    ingress::RawV1SegmentHeaderWire segment_header_{};
    ingress::RawV1JournalHeaderWire journal_header_{};
    int segment_fd_ = -1;
    int journal_fd_ = -1;
    std::string error_;
};

bool PwriteAll(
    int fd,
    std::uint64_t offset,
    std::span<const std::byte> bytes) {
    std::size_t completed = 0U;
    while (completed < bytes.size()) {
        const ssize_t result = ::pwrite(
            fd,
            bytes.data() + completed,
            bytes.size() - completed,
            static_cast<off_t>(
                offset +
                static_cast<std::uint64_t>(completed)));
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

int OpenFinal(int directory_fd, const char* name) {
    return ::openat(
        directory_fd,
        name,
        O_RDWR | O_NOFOLLOW | O_CLOEXEC);
}

int FindOpenDescriptor(
    ingress::RawPosixRecoveryFileIdentityV1 identity) {
    DIR* const directory = ::opendir("/proc/self/fd");
    if (directory == nullptr) {
        return -1;
    }
    const int enumeration_fd = ::dirfd(directory);
    int found = -1;
    for (;;) {
        errno = 0;
        struct dirent* const entry = ::readdir(directory);
        if (entry == nullptr) {
            break;
        }
        const std::string_view name(entry->d_name);
        int candidate = -1;
        const auto parsed = std::from_chars(
            name.data(),
            name.data() + name.size(),
            candidate);
        if (parsed.ec != std::errc{} ||
            parsed.ptr != name.data() + name.size() ||
            candidate < 0 ||
            candidate == enumeration_fd) {
            continue;
        }
        struct stat status {};
        if (::fstat(candidate, &status) == 0 &&
            static_cast<std::uint64_t>(status.st_dev) ==
                identity.device &&
            static_cast<std::uint64_t>(status.st_ino) ==
                identity.inode) {
            found = candidate;
            break;
        }
    }
    static_cast<void>(::closedir(directory));
    return found;
}

bool ResizeReplacement(
    int directory_fd,
    const char* name,
    off_t size) {
    const int fd = ::openat(
        directory_fd,
        name,
        O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW |
            O_CLOEXEC,
        0600);
    if (fd < 0) {
        return false;
    }
    const bool result = ::ftruncate(fd, size) == 0;
    static_cast<void>(::close(fd));
    return result;
}

ingress::RawPosixRecoveryLimitsV1 TestLimits() {
    ingress::RawPosixRecoveryLimitsV1 limits;
    limits.max_segments = 8U;
    limits.max_segment_bytes = 1U << 20U;
    limits.max_total_segment_bytes = 2U << 20U;
    limits.max_journal_markers = 64U;
    return limits;
}

void TestCrashLikeAnalyzeAndExecute(TestContext* test) {
    NamespaceFixture fixture(16U * 1024U);
    test->Expect(
        fixture.ok(),
        "fresh retained Raw namespace is created");
    if (!fixture.ok()) {
        std::cerr << fixture.error() << '\n';
        return;
    }

    const int segment_fd = fixture.ReleaseSegmentFd();
    const int journal_fd = fixture.ReleaseJournalFd();
    std::string error;
    std::unique_ptr<ingress::RawWalIo> io =
        ingress::AdoptPosixRawWalIo(
            segment_fd, journal_fd, &error);
    test->Expect(
        io != nullptr,
        "fresh bootstrap descriptors pass writer gates");
    if (io == nullptr) {
        static_cast<void>(::close(segment_fd));
        static_cast<void>(::close(journal_fd));
        return;
    }

    const RecordFixture first = MakeRecord(1U);
    std::uint64_t complete_record_end = 0U;
    {
        ingress::RawWalWriterConfig config;
        config.segment_header_wire = fixture.segment_header();
        config.journal_header_wire = fixture.journal_header();
        config.source_stream_id = 1001U;
        config.capture_date = 20260718U;
        config.segment_sequence = 1U;
        config.segment_base_wal_pos = 0U;
        config.first_ingress_sequence = 1U;
        ingress::RawWalWriter writer(
            config, std::move(io));
        test->Expect(
            writer.Initialize() &&
                writer.AppendRecord(first.wal_input()),
            "writer leaves one complete append-only record");
        complete_record_end =
            writer.Snapshot().append.segment_offset;
        // Destructor closes without a clean seal, modeling process death
        // after the complete trailer but before a record marker.
    }

    ingress::DurableMarkerV1 record_marker;
    record_marker.source_stream_id = 1001U;
    record_marker.segment_sequence = 1U;
    record_marker.durable_global_wal_pos =
        complete_record_end;
    record_marker.durable_ingress_sequence = 1U;
    record_marker.durable_segment_offset =
        complete_record_end;
    ingress::RawV1DurableMarkerWire marker_wire{};
    test->Expect(
        ingress::EncodeDurableMarkerV1(
            record_marker, &marker_wire) ==
            ingress::RawV1Error::kNone,
        "crash marker fixture encodes");

    const RecordFixture second = MakeRecord(2U);
    const std::vector<std::byte> second_wire =
        second.Encode();
    const int direct_journal = OpenFinal(
        fixture.directory_fd(),
        ingress::kRawJournalFilename);
    const int direct_segment = OpenFinal(
        fixture.directory_fd(),
        ingress::kRawFirstSegmentFilename);
    const std::size_t partial_marker_bytes = 17U;
    const std::size_t partial_record_bytes = 20U;
    const bool crash_bytes_written =
        direct_journal >= 0 &&
        direct_segment >= 0 &&
        second_wire.size() >= partial_record_bytes &&
        PwriteAll(
            direct_journal,
            ingress::kRawV1JournalHeaderBytes +
                ingress::kRawV1DurableMarkerBytes,
            std::span<const std::byte>(marker_wire)
                .first(partial_marker_bytes)) &&
        PwriteAll(
            direct_segment,
            complete_record_end,
            std::span<const std::byte>(second_wire)
                .first(partial_record_bytes)) &&
        ::ftruncate(
            direct_segment,
            static_cast<off_t>(
                complete_record_end +
                partial_record_bytes)) == 0 &&
        ::fdatasync(direct_segment) == 0 &&
        ::fdatasync(direct_journal) == 0;
    test->Expect(
        crash_bytes_written,
        "partial marker and partial next record reach the crash image");
    if (direct_segment >= 0) {
        static_cast<void>(::close(direct_segment));
    }
    if (direct_journal >= 0) {
        static_cast<void>(::close(direct_journal));
    }

    std::unique_ptr<ingress::RawPosixRecoverySessionV1>
        session = ingress::LoadRawPosixRecoverySessionV1(
            fixture.lease(), TestLimits(), &error);
    test->Expect(
        session != nullptr,
        "secure loader retains the crash-image files");
    if (session == nullptr) {
        std::cerr << error << '\n';
        return;
    }
    const ingress::RawRecoveryPlanV1 plan =
        ingress::AnalyzeRawRecoveryV1(session->input());
    test->Expect(
        plan.ok() &&
            plan.journal_tail ==
                ingress::RawRecoveryJournalTailV1::
                    kPartialMarker &&
            plan.segments.size() == 1U &&
            plan.segments.front().tail ==
                ingress::RawRecoverySegmentTailV1::
                    kPartialRecord &&
            plan.segments.front().append_only_end_offset ==
                complete_record_end,
        "owned snapshot classifies journal and segment terminal tails");

    const ingress::RawRecoveryExecutionResultV1 result =
        ingress::ExecuteRawRecoveryPlanV1(
            plan,
            *session);
    test->Expect(
        result.failure ==
                ingress::RawRecoveryExecutionFailureV1::kNone &&
            result.mutated &&
            result.cursor_publishable &&
            result.recovered_cursor.segment_offset ==
                complete_record_end &&
            result.recovered_cursor.ingress_sequence == 1U,
        "real executor truncates tails and promotes the complete record");
    session.reset();

    struct stat journal_status {};
    struct stat segment_status {};
    test->Expect(
        ::fstatat(
            fixture.directory_fd(),
            ingress::kRawJournalFilename,
            &journal_status,
            AT_SYMLINK_NOFOLLOW) == 0 &&
            journal_status.st_size ==
                static_cast<off_t>(
                    ingress::kRawV1JournalHeaderBytes +
                    2U * ingress::kRawV1DurableMarkerBytes) &&
        ::fstatat(
            fixture.directory_fd(),
            ingress::kRawFirstSegmentFilename,
            &segment_status,
            AT_SYMLINK_NOFOLLOW) == 0 &&
            segment_status.st_size ==
                static_cast<off_t>(complete_record_end),
        "real repair leaves exact logical journal and segment sizes");

    session = ingress::LoadRawPosixRecoverySessionV1(
        fixture.lease(), TestLimits(), &error);
    const ingress::RawRecoveryPlanV1 repaired =
        session == nullptr
            ? ingress::RawRecoveryPlanV1{}
            : ingress::AnalyzeRawRecoveryV1(session->input());
    test->Expect(
        session != nullptr &&
            repaired.ok() &&
            repaired.journal_tail ==
                ingress::RawRecoveryJournalTailV1::kNone &&
            repaired.segments.size() == 1U &&
            repaired.segments.front().tail ==
                ingress::RawRecoverySegmentTailV1::kNone &&
            repaired.accepted_cursor.segment_offset ==
                complete_record_end,
        "a fresh post-repair snapshot is clean and self-consistent");
}

void TestRecoveredOpenAdoptionAndSeal(TestContext* test) {
    NamespaceFixture fixture(16U * 1024U);
    test->Expect(
        fixture.ok(),
        "recovered-seal fixture creates a retained Raw namespace");
    if (!fixture.ok()) {
        return;
    }

    std::string error;
    std::unique_ptr<ingress::RawWalIo> initial_io =
        ingress::AdoptPosixRawWalIo(
            fixture.ReleaseSegmentFd(),
            fixture.ReleaseJournalFd(),
            &error);
    if (initial_io == nullptr) {
        test->Expect(false, "recovered-seal fixture adopts bootstrap files");
        return;
    }

    const RecordFixture record = MakeRecord(1U);
    {
        ingress::RawWalWriterConfig config;
        config.segment_header_wire = fixture.segment_header();
        config.journal_header_wire = fixture.journal_header();
        config.source_stream_id = 1001U;
        config.capture_date = 20260718U;
        config.segment_sequence = 1U;
        config.segment_base_wal_pos = 0U;
        config.first_ingress_sequence = 1U;
        config.headers_already_persisted = true;
        ingress::RawWalWriter writer(
            config, std::move(initial_io));
        test->Expect(
            writer.Initialize() &&
                writer.AppendRecord(record.wal_input()) &&
                writer.FlushDurable(),
            "crash image has one durable record but no seal marker");
    }

    std::unique_ptr<ingress::RawPosixRecoverySessionV1>
        session = ingress::LoadRawPosixRecoverySessionV1(
            fixture.lease(), TestLimits(), &error);
    if (session == nullptr) {
        test->Expect(false, "recovered-seal crash image loads");
        return;
    }
    const ingress::RawRecoveryPlanV1 plan =
        ingress::AnalyzeRawRecoveryV1(session->input());
    const ingress::RawRecoveryExecutionResultV1 execution =
        ingress::ExecuteRawRecoveryPlanV1(plan, *session);
    test->Expect(
        plan.ok() &&
            plan.segments.size() == 1U &&
            !plan.segments.front().sealed &&
            plan.segments.front().has_accepted_marker &&
            execution.failure ==
                ingress::RawRecoveryExecutionFailureV1::kNone &&
            execution.cursor_publishable &&
            execution.recovered_cursor.marker_flags == 0U,
        "recovery establishes one exact non-sealed terminal cursor");
    ingress::RawSegmentArtifactOptionsV1 open_options;
    open_options.expected_raw_schema_sha256 =
        plan.journal_header.raw_schema_sha256;
    open_options.maximum_segment_bytes = 1U << 20U;
    const ingress::RawRecoveredSealedArtifactsV1
        rejected_open_artifacts =
            session->PrepareRecoveredSealedRawArtifacts(
                plan, open_options);
    test->Expect(
        rejected_open_artifacts.failure ==
            ingress::RawRecoveredSealedArtifactsFailureV1::
                kNotCompletelySealed,
        "artifact recovery refuses a stream whose terminal segment is still open");

    std::unique_ptr<ingress::RawWalIo> recovered_io =
        session->AdoptRecoveredSealIo(
            plan, execution, &error);
    test->Expect(
        recovered_io != nullptr &&
            session->journal_open_flags() < 0 &&
            session->segment_open_flags(1U) < 0,
        "session transfers recovered files and capabilities exactly once");
    if (recovered_io == nullptr) {
        std::cerr << error << '\n';
        return;
    }
    test->Expect(
        session->AdoptRecoveredSealIo(
            plan, execution, &error) == nullptr,
        "recovered seal I/O cannot be adopted twice");

    fixture.ReleaseOriginalLease();
    session.reset();
    std::unique_ptr<ingress::RawWriterLease> competing =
        ingress::AcquireRawWriterLeaseAt(
            fixture.directory_fd(),
            1001U,
            20260718U,
            &error);
    test->Expect(
        competing == nullptr,
        "adopted seal I/O retains the writer lease after session destruction");

    ingress::RawWalWriterSnapshot sealed_snapshot;
    {
        ingress::RawWalWriterConfig config;
        config.segment_header_wire = fixture.segment_header();
        config.journal_header_wire = fixture.journal_header();
        config.source_stream_id = 1001U;
        config.capture_date = 20260718U;
        config.segment_sequence = 1U;
        config.segment_base_wal_pos = 0U;
        config.first_ingress_sequence = 1U;
        config.initial_durable_ingress_sequence =
            execution.recovered_cursor.ingress_sequence;
        config.initialization_mode =
            ingress::RawWalInitializationMode::
                kRecoveredSealOnly;
        config.recovered_open.journal_append_offset =
            execution.retained_journal_size;
        config.recovered_open.recovered_cursor = {
            execution.recovered_cursor.global_wal_pos,
            execution.recovered_cursor.ingress_sequence,
            execution.recovered_cursor.segment_offset};
        config.recovered_open.accepted_marker_flags =
            execution.recovered_cursor.marker_flags;
        config.headers_already_persisted = true;

        ingress::RawWalWriter writer(
            config, std::move(recovered_io));
        test->Expect(
            writer.Initialize() &&
                writer.SealAndClose(),
            "kRecoveredSealOnly seals and closes the adopted old open segment");
        sealed_snapshot = writer.Snapshot();
    }
    test->Expect(
        sealed_snapshot.sealed &&
            sealed_snapshot.closed &&
            !sealed_snapshot.fatal &&
            sealed_snapshot.durable.segment_offset ==
                execution.recovered_cursor.segment_offset &&
            sealed_snapshot.durable.ingress_sequence ==
                execution.recovered_cursor.ingress_sequence,
        "seal-only writer preserves the exact recovered cursor");

    std::unique_ptr<ingress::RawWriterLease> post_seal_lease =
        ingress::AcquireRawWriterLeaseAt(
            fixture.directory_fd(),
            1001U,
            20260718U,
            &error);
    std::unique_ptr<ingress::RawPosixRecoverySessionV1>
        sealed_session =
            post_seal_lease == nullptr
                ? nullptr
                : ingress::LoadRawPosixRecoverySessionV1(
                      *post_seal_lease,
                      TestLimits(),
                      &error);
    const ingress::RawRecoveryPlanV1 sealed_plan =
        sealed_session == nullptr
            ? ingress::RawRecoveryPlanV1{}
            : ingress::AnalyzeRawRecoveryV1(
                  sealed_session->input());
    ingress::DurableMarkerV1 exact_seal;
    const bool exact_seal_decodes =
        sealed_plan.segments.size() == 1U &&
        sealed_plan.segments.front().has_accepted_marker &&
        ingress::DecodeDurableMarkerV1(
            sealed_plan.segments.front()
                .accepted_marker_wire,
            &exact_seal) ==
            ingress::RawV1Error::kNone;
    struct stat absent_index {};
    errno = 0;
    const bool index_absent_before =
        ::fstatat(
            fixture.directory_fd(),
            "segment-00000001.idx",
            &absent_index,
            AT_SYMLINK_NOFOLLOW) != 0 &&
        errno == ENOENT;
    ingress::RawSegmentArtifactOptionsV1 options;
    options.expected_raw_schema_sha256 =
        sealed_plan.journal_header.raw_schema_sha256;
    options.maximum_segment_bytes = 1U << 20U;
    const ingress::RawRecoveredSealedArtifactsV1 artifacts =
        sealed_session == nullptr
            ? ingress::RawRecoveredSealedArtifactsV1{}
            : sealed_session
                  ->PrepareRecoveredSealedRawArtifacts(
                      sealed_plan, options);
    errno = 0;
    const bool index_absent_after =
        ::fstatat(
            fixture.directory_fd(),
            "segment-00000001.idx",
            &absent_index,
            AT_SYMLINK_NOFOLLOW) != 0 &&
        errno == ENOENT;
    ingress::RawManifestV1 recovered_manifest{};
    const bool manifest_built =
        artifacts.ok() &&
        ingress::BuildRecoveredClosedRawManifestV1(
            artifacts.metadata,
            nullptr,
            &recovered_manifest) ==
            ingress::RawManifestTransitionErrorV1::kNone;
    test->Expect(
        sealed_session != nullptr &&
            sealed_plan.ok() &&
            sealed_plan.segments.size() == 1U &&
            sealed_plan.segments.front().sealed &&
            exact_seal_decodes &&
            exact_seal.marker_flags ==
                ingress::kRawV1SegmentSealed &&
            exact_seal.durable_segment_offset ==
                execution.recovered_cursor.segment_offset,
        "post-seal analysis retains the exact accepted seal-marker wire");
    test->Expect(
        index_absent_before &&
            artifacts.ok() &&
            artifacts.artifact_plans.size() == 1U &&
            artifacts.existing_index_states.size() ==
                1U &&
            artifacts.existing_index_states.front() ==
                ingress::
                    RawSegmentArtifactExistingStateV1::
                        kAbsent &&
            artifacts.metadata.size() == 1U &&
            artifacts.artifact_plans.front()
                .retained_segment_fd_bound &&
            !artifacts.artifact_plans.front()
                 .index_bytes.empty() &&
            artifacts.metadata.front()
                    .segment.segment_sequence == 1U &&
            artifacts.metadata.front()
                    .accepted_sealed_marker_bytes ==
                sealed_plan.segments.front()
                    .accepted_marker_wire &&
            index_absent_after,
        "sealed recovery rebuilds and self-validates deterministic RawIndex bytes from retained fds without publishing");
    test->Expect(
        manifest_built &&
            recovered_manifest.closed_entries.size() ==
                1U &&
            !recovered_manifest.open_entry.has_value() &&
            recovered_manifest.closed_entries.front()
                    .segment_sha256 ==
                artifacts.metadata.front()
                    .segment_sha256 &&
            ingress::ValidateManifestModel(
                recovered_manifest) ==
                ingress::RawManifestV1Error::kNone,
        "sorted recovered metadata constructs a valid closed-only manifest");

    const int rebuilt_index_fd =
        artifacts.ok() &&
                !artifacts.artifact_plans.empty()
            ? ::openat(
                  fixture.directory_fd(),
                  "segment-00000001.idx",
                  O_RDWR | O_CREAT | O_EXCL |
                      O_NOFOLLOW | O_NONBLOCK |
                      O_CLOEXEC,
                  0600)
            : -1;
    const bool rebuilt_index_published =
        rebuilt_index_fd >= 0 &&
        PwriteAll(
            rebuilt_index_fd,
            0U,
            artifacts.artifact_plans.front()
                .index_bytes) &&
        ::fdatasync(rebuilt_index_fd) == 0 &&
        ::fsync(fixture.directory_fd()) == 0;
    if (rebuilt_index_fd >= 0) {
        static_cast<void>(::close(rebuilt_index_fd));
    }
    const ingress::RawRecoveredSealedArtifactsV1
        accepted_existing =
            sealed_session
                ->PrepareRecoveredSealedRawArtifacts(
                    sealed_plan, options);
    test->Expect(
        rebuilt_index_published &&
            accepted_existing.ok() &&
            accepted_existing
                    .existing_index_states.size() == 1U &&
            accepted_existing
                    .existing_index_states.front() ==
                ingress::
                    RawSegmentArtifactExistingStateV1::
                        kMatchingFinal &&
            !accepted_existing.metadata.empty() &&
            accepted_existing.metadata.front()
                    .index_sha256 ==
                artifacts.metadata.front().index_sha256,
        "a stable exact existing RawIndex final is accepted against the retained-fd rebuild");

    const int conflicting_index_fd = ::openat(
        fixture.directory_fd(),
        "segment-00000001.idx",
        O_RDWR | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC);
    const std::array<std::byte, 1U> conflict{
        std::byte{0xff}};
    const bool index_conflicted =
        conflicting_index_fd >= 0 &&
        PwriteAll(
            conflicting_index_fd, 0U, conflict) &&
        ::fdatasync(conflicting_index_fd) == 0;
    if (conflicting_index_fd >= 0) {
        static_cast<void>(::close(conflicting_index_fd));
    }
    const ingress::RawRecoveredSealedArtifactsV1
        conflicting_existing =
            sealed_session
                ->PrepareRecoveredSealedRawArtifacts(
                    sealed_plan, options);
    test->Expect(
        index_conflicted &&
            conflicting_existing.failure ==
                ingress::
                    RawRecoveredSealedArtifactsFailureV1::
                        kArtifactPlanFailure &&
            conflicting_existing.artifact_failure ==
                ingress::RawSegmentArtifactFailureV1::
                    kFinalConflict &&
            conflicting_existing.artifact_plans.empty() &&
            conflicting_existing.metadata.empty(),
        "conflicting existing RawIndex bytes fail closed without returning a partial metadata set");

    ingress::RawRecoveryPlanV1 forged_plan =
        sealed_plan;
    forged_plan.segments.front()
        .accepted_marker_wire[0U] ^=
        std::byte{0x01};
    const ingress::RawRecoveredSealedArtifactsV1
        forged_result =
            sealed_session
                ->PrepareRecoveredSealedRawArtifacts(
                    forged_plan, options);
    test->Expect(
        forged_result.failure ==
            ingress::RawRecoveredSealedArtifactsFailureV1::
                kPlanDoesNotMatchSession,
        "artifact rebuild rejects a caller plan whose exact seal wire differs from the session snapshot");
}

void TestRecoveredOpenAdoptionRevalidatesFinalName(
    TestContext* test) {
    NamespaceFixture fixture;
    if (!fixture.ok()) {
        test->Expect(
            false,
            "adoption-revalidation fixture creates a Raw namespace");
        return;
    }
    const int segment_fd = fixture.ReleaseSegmentFd();
    const int journal_fd = fixture.ReleaseJournalFd();
    std::string error;
    std::unique_ptr<ingress::RawWalIo> io =
        ingress::AdoptPosixRawWalIo(
            segment_fd, journal_fd, &error);
    if (io == nullptr) {
        static_cast<void>(::close(segment_fd));
        static_cast<void>(::close(journal_fd));
        test->Expect(false, "adoption-revalidation fixture adopts files");
        return;
    }
    {
        ingress::RawWalWriterConfig config;
        config.segment_header_wire = fixture.segment_header();
        config.journal_header_wire = fixture.journal_header();
        config.source_stream_id = 1001U;
        config.capture_date = 20260718U;
        config.segment_sequence = 1U;
        config.segment_base_wal_pos = 0U;
        config.first_ingress_sequence = 1U;
        config.headers_already_persisted = true;
        ingress::RawWalWriter writer(config, std::move(io));
        test->Expect(
            writer.Initialize(),
            "adoption-revalidation crash image has an open marker");
    }

    std::unique_ptr<ingress::RawPosixRecoverySessionV1>
        session = ingress::LoadRawPosixRecoverySessionV1(
            fixture.lease(), TestLimits(), &error);
    if (session == nullptr) {
        test->Expect(false, "adoption-revalidation crash image loads");
        return;
    }
    const ingress::RawRecoveryPlanV1 plan =
        ingress::AnalyzeRawRecoveryV1(session->input());
    const ingress::RawRecoveryExecutionResultV1 execution =
        ingress::ExecuteRawRecoveryPlanV1(plan, *session);
    const bool replaced =
        ::renameat(
            fixture.directory_fd(),
            ingress::kRawFirstSegmentFilename,
            fixture.directory_fd(),
            "raw-replaced-open-segment") == 0 &&
        ResizeReplacement(
            fixture.directory_fd(),
            ingress::kRawFirstSegmentFilename,
            static_cast<off_t>(
                execution.recovered_cursor
                    .segment_offset));
    std::unique_ptr<ingress::RawWalIo> rejected =
        replaced
            ? session->AdoptRecoveredSealIo(
                  plan, execution, &error)
            : nullptr;
    test->Expect(
        plan.ok() &&
            execution.failure ==
                ingress::RawRecoveryExecutionFailureV1::kNone &&
            replaced &&
            rejected == nullptr &&
            session->segment_open_flags(1U) >= 0 &&
            session->journal_open_flags() >= 0 &&
            session->TruncateSegment(
                1U,
                execution.recovered_cursor
                    .segment_offset) == 0,
        "final-name inode replacement rejects adoption without losing retained descriptor ownership");
}

void TestSecureOpenRejections(TestContext* test) {
    std::string error;

    NamespaceFixture indexed;
    indexed.CloseBootstrapDescriptors();
    const int index_fd = ::openat(
        indexed.directory_fd(),
        "segment-00000001.idx",
        O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC,
        0600);
    if (index_fd >= 0) {
        static_cast<void>(::close(index_fd));
    }
    auto indexed_session =
        index_fd >= 0
            ? ingress::LoadRawPosixRecoverySessionV1(
                  indexed.lease(), TestLimits(), &error)
            : nullptr;
    test->Expect(
        index_fd >= 0 && indexed_session != nullptr &&
            !indexed_session
                 ->DependentArtifactsAbsentProven(),
        "valid final sparse-index names do not masquerade as malformed Raw segment names");

    NamespaceFixture symlink;
    symlink.CloseBootstrapDescriptors();
    const bool made_symlink =
        ::unlinkat(
            symlink.directory_fd(),
            ingress::kRawJournalFilename,
            0) == 0 &&
        ::symlinkat(
            ingress::kRawFirstSegmentFilename,
            symlink.directory_fd(),
            ingress::kRawJournalFilename) == 0;
    std::unique_ptr<ingress::RawPosixRecoverySessionV1>
        rejected =
            made_symlink
                ? ingress::LoadRawPosixRecoverySessionV1(
                      symlink.lease(), TestLimits(), &error)
                : nullptr;
    test->Expect(
        made_symlink && rejected == nullptr,
        "durable.journal symlink is rejected");

    NamespaceFixture hardlink;
    hardlink.CloseBootstrapDescriptors();
    const bool made_hardlink =
        ::linkat(
            hardlink.directory_fd(),
            ingress::kRawFirstSegmentFilename,
            hardlink.directory_fd(),
            "raw-hardlink-alias",
            0) == 0;
    rejected =
        made_hardlink
            ? ingress::LoadRawPosixRecoverySessionV1(
                  hardlink.lease(), TestLimits(), &error)
            : nullptr;
    test->Expect(
        made_hardlink && rejected == nullptr,
        "multiply-linked segment inode is rejected");

    NamespaceFixture mode;
    mode.CloseBootstrapDescriptors();
    const bool changed_mode =
        ::fchmodat(
            mode.directory_fd(),
            ingress::kRawFirstSegmentFilename,
            0640,
            0) == 0;
    rejected =
        changed_mode
            ? ingress::LoadRawPosixRecoverySessionV1(
                  mode.lease(), TestLimits(), &error)
            : nullptr;
    test->Expect(
        changed_mode && rejected == nullptr,
        "non-0600 final segment is rejected");

    NamespaceFixture gap;
    gap.CloseBootstrapDescriptors();
    const bool made_gap =
        ::renameat(
            gap.directory_fd(),
            ingress::kRawFirstSegmentFilename,
            gap.directory_fd(),
            "segment-00000002.raw") == 0;
    rejected =
        made_gap
            ? ingress::LoadRawPosixRecoverySessionV1(
                  gap.lease(), TestLimits(), &error)
            : nullptr;
    test->Expect(
        made_gap && rejected == nullptr,
        "segment sequence that does not begin at one is rejected");

    NamespaceFixture malformed;
    malformed.CloseBootstrapDescriptors();
    const int malformed_fd = ::openat(
        malformed.directory_fd(),
        "segment-0000001.raw",
        O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC,
        0600);
    if (malformed_fd >= 0) {
        static_cast<void>(::close(malformed_fd));
    }
    rejected =
        malformed_fd >= 0
            ? ingress::LoadRawPosixRecoverySessionV1(
                  malformed.lease(), TestLimits(), &error)
            : nullptr;
    test->Expect(
        malformed_fd >= 0 && rejected == nullptr,
        "segment-like filename outside exact %08u grammar is rejected");

    NamespaceFixture bounded;
    bounded.CloseBootstrapDescriptors();
    ingress::RawPosixRecoveryLimitsV1 impossible =
        TestLimits();
    impossible.max_segments =
        ingress::kRawPosixRecoveryAbsoluteMaxSegments + 1U;
    rejected = ingress::LoadRawPosixRecoverySessionV1(
        bounded.lease(), impossible, &error);
    test->Expect(
        rejected == nullptr,
        "configured segment count cannot exceed the hard 100000 bound");

    ingress::RawPosixRecoveryLimitsV1 too_small =
        TestLimits();
    too_small.max_segment_bytes = 4096U;
    too_small.max_total_segment_bytes = 4096U;
    rejected = ingress::LoadRawPosixRecoverySessionV1(
        bounded.lease(), too_small, &error);
    test->Expect(
        rejected == nullptr,
        "physical segment size is rejected before file-sized allocation");
}

void TestRetainedInodesAndOpenFlags(TestContext* test) {
    NamespaceFixture fixture;
    fixture.CloseBootstrapDescriptors();
    std::string error;
    std::unique_ptr<ingress::RawPosixRecoverySessionV1>
        session = ingress::LoadRawPosixRecoverySessionV1(
            fixture.lease(), TestLimits(), &error);
    test->Expect(
        session != nullptr,
        "valid namespace loads for retained-inode test");
    if (session == nullptr) {
        return;
    }

    const int journal_flags = session->journal_open_flags();
    const int segment_flags = session->segment_open_flags(1U);
    test->Expect(
        journal_flags >= 0 &&
            segment_flags >= 0 &&
            (journal_flags & O_ACCMODE) == O_RDWR &&
            (segment_flags & O_ACCMODE) == O_RDWR &&
            (journal_flags & O_NONBLOCK) != 0 &&
            (segment_flags & O_NONBLOCK) != 0 &&
            (journal_flags & O_NOATIME) != 0 &&
            (segment_flags & O_NOATIME) != 0 &&
            (journal_flags & O_APPEND) == 0 &&
            (segment_flags & O_APPEND) == 0,
        "retained recovery descriptions are RW/nonblocking/noatime without O_APPEND");

    const ingress::RawPosixRecoveryFileIdentityV1
        journal_identity = session->journal_identity();
    ingress::RawPosixRecoveryFileIdentityV1 segment_identity;
    test->Expect(
        session->segment_identity(1U, &segment_identity),
        "retained segment identity is available");

    const int hidden_journal =
        FindOpenDescriptor(journal_identity);
    const int original_flags =
        hidden_journal < 0
            ? -1
            : ::fcntl(hidden_journal, F_GETFL);
    const bool append_injected =
        hidden_journal >= 0 &&
        original_flags >= 0 &&
        ::fcntl(
            hidden_journal,
            F_SETFL,
            original_flags | O_APPEND) == 0;
    const std::array<std::byte, 1U> probe{
        std::byte{0x7fU}};
    const ingress::RawRecoveryWriteResult append_rejected =
        append_injected
            ? session->WriteJournalSome(0U, probe)
            : ingress::RawRecoveryWriteResult{1U, 0};
    test->Expect(
        append_injected &&
            append_rejected.bytes_written == 0U &&
            append_rejected.error_number == EINVAL,
        "backend rejects an open description changed to O_APPEND");
    if (append_injected) {
        static_cast<void>(
            ::fcntl(hidden_journal, F_SETFL, original_flags));
    }

    const bool replaced =
        ::renameat(
            fixture.directory_fd(),
            ingress::kRawJournalFilename,
            fixture.directory_fd(),
            "raw-retained-journal") == 0 &&
        ResizeReplacement(
            fixture.directory_fd(),
            ingress::kRawJournalFilename,
            7000) &&
        ::renameat(
            fixture.directory_fd(),
            ingress::kRawFirstSegmentFilename,
            fixture.directory_fd(),
            "raw-retained-segment") == 0 &&
        ResizeReplacement(
            fixture.directory_fd(),
            ingress::kRawFirstSegmentFilename,
            9000);
    test->Expect(
        replaced,
        "final paths are atomically replaced after snapshot load");

    const bool mutated_retained =
        session->TruncateJournal(4096U) == 0 &&
        session->TruncateSegment(1U, 4096U) == 0 &&
        session->SyncJournal() == 0 &&
        session->SyncSegment(1U, false) == 0 &&
        session->SyncParentDirectories() == 0;
    test->Expect(
        mutated_retained,
        "executor backend continues through retained descriptors");

    struct stat retained_journal {};
    struct stat replacement_journal {};
    struct stat retained_segment {};
    struct stat replacement_segment {};
    const bool inspected =
        ::fstatat(
            fixture.directory_fd(),
            "raw-retained-journal",
            &retained_journal,
            AT_SYMLINK_NOFOLLOW) == 0 &&
        ::fstatat(
            fixture.directory_fd(),
            ingress::kRawJournalFilename,
            &replacement_journal,
            AT_SYMLINK_NOFOLLOW) == 0 &&
        ::fstatat(
            fixture.directory_fd(),
            "raw-retained-segment",
            &retained_segment,
            AT_SYMLINK_NOFOLLOW) == 0 &&
        ::fstatat(
            fixture.directory_fd(),
            ingress::kRawFirstSegmentFilename,
            &replacement_segment,
            AT_SYMLINK_NOFOLLOW) == 0;
    test->Expect(
        inspected &&
            retained_journal.st_size == 4096 &&
            retained_segment.st_size == 4096 &&
            replacement_journal.st_size == 7000 &&
            replacement_segment.st_size == 9000 &&
            journal_identity.device ==
                static_cast<std::uint64_t>(
                    retained_journal.st_dev) &&
            journal_identity.inode ==
                static_cast<std::uint64_t>(
                    retained_journal.st_ino) &&
            segment_identity.device ==
                static_cast<std::uint64_t>(
                    retained_segment.st_dev) &&
            segment_identity.inode ==
                static_cast<std::uint64_t>(
                    retained_segment.st_ino),
        "path replacement cannot redirect retained truncate or sync");

    fixture.ReleaseOriginalLease();
    std::unique_ptr<ingress::RawWriterLease> competing =
        ingress::AcquireRawWriterLeaseAt(
            fixture.directory_fd(),
            1001U,
            20260718U,
            &error);
    test->Expect(
        competing == nullptr,
        "session's duplicated lease descriptor retains the exclusive flock");
}

}  // namespace

int main() {
    TestContext test;
    TestCrashLikeAnalyzeAndExecute(&test);
    TestRecoveredOpenAdoptionAndSeal(&test);
    TestRecoveredOpenAdoptionRevalidatesFinalName(&test);
    TestSecureOpenRejections(&test);
    TestRetainedInodesAndOpenFlags(&test);

    if (test.failures != 0) {
        std::cerr << test.failures
                  << " Phase 2 POSIX recovery tests failed\n";
        return 1;
    }
    std::cout << "Phase 2 POSIX recovery tests passed\n";
    return 0;
}
