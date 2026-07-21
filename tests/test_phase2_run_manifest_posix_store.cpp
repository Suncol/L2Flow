#include "l2flow/ingress/run_manifest_posix_store.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace ingress = l2flow::ingress;

namespace {

using PublishFunction =
    ingress::RunManifestPosixPublishResultV1 (*)(
        int,
        const ingress::BuiltRunManifestV1&,
        std::string*) noexcept;

static_assert(
    std::is_same_v<
        decltype(
            &ingress::PublishRunManifestV1At),
        PublishFunction>);
static_assert(
    !std::is_move_constructible_v<
        ingress::PublishedRunManifestReceiptV1>);

struct TestContext final {
    void Expect(
        bool condition,
        std::string_view description) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << description << '\n';
        }
    }

    void ExpectError(
        ingress::RunManifestPosixStoreErrorV1 actual,
        ingress::RunManifestPosixStoreErrorV1 expected,
        std::string_view description) {
        if (actual != expected) {
            ++failures;
            std::cerr
                << "FAIL: " << description
                << " (actual="
                << ingress::
                       RunManifestPosixStoreErrorV1Name(
                           actual)
                << ", expected="
                << ingress::
                       RunManifestPosixStoreErrorV1Name(
                           expected)
                << ")\n";
        }
    }

    int failures = 0;
};

class TempDirectory final {
public:
    TempDirectory() {
        char pattern[] =
            "/tmp/l2flow-run-manifest-XXXXXX";
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

std::uint64_t ClockLabel(
    const ingress::RawV1Digest& digest) {
    std::uint64_t result = 0U;
    for (std::size_t index = 0U; index < 8U; ++index) {
        result |= static_cast<std::uint64_t>(
                      std::to_integer<std::uint8_t>(
                          digest[index]))
                  << (8U * index);
    }
    return result;
}

void RefreshMarker(
    ingress::RunManifestRawInputV1* input) {
    if (input == nullptr) {
        return;
    }
    static_cast<void>(
        ingress::EncodeDurableMarkerV1(
            input->durable_marker,
            &input->durable_marker_bytes));
    static_cast<void>(
        ingress::DecodeDurableMarkerV1(
            input->durable_marker_bytes,
            &input->durable_marker));
}

ingress::RunManifestV1 MakeManifest(
    std::uint8_t seed = 0x01U) {
    ingress::RunManifestV1 manifest{};
    manifest.run_id = Pattern<16U>(seed);
    manifest.mode =
        ingress::RunManifestModeV1::kLive;
    manifest.capture_date = 20260718U;
    manifest.host_uuid = Pattern<16U>(0x11U);
    manifest.linux_boot_id =
        Pattern<16U>(0x21U);
    manifest.clock_epoch_algorithm = 1U;
    manifest.clock_epoch_digest =
        Pattern<32U>(0x31U);
    manifest.clock_epoch_label =
        ClockLabel(manifest.clock_epoch_digest);
    manifest.vendor.sdk_version = 213234U;
    manifest.vendor.sdk_archive_sha256 =
        Pattern<32U>(0x41U);
    manifest.vendor.libmdl_api_sha256 =
        Pattern<32U>(0x51U);
    manifest.vendor.elf_build_id = "abcd1234";
    manifest.build.build_manifest_sha256 =
        Pattern<32U>(0x61U);
    manifest.build.compiler = "gcc-13.2";
    manifest.build.cxx_flags = "-O2";
    manifest.build.dependency_lock_sha256 =
        Pattern<32U>(0x71U);
    manifest.configuration.config_sha256 =
        Pattern<32U>(0x81U);
    manifest.configuration
        .endpoint_contract_sha256 =
        Pattern<32U>(0x91U);
    manifest.configuration.registry_version =
        "raw-readiness-v1";
    manifest.configuration.registry_sha256 =
        Pattern<32U>(0xa1U);
    manifest.configuration.raw_schema_sha256 =
        Pattern<32U>(0xb1U);

    ingress::RunManifestRawInputV1 input{};
    input.source_stream_id = 1001U;
    input.capture_date = manifest.capture_date;
    input.stream_day_id = Pattern<16U>(0xc1U);
    input.durable_journal_header_sha256 =
        Pattern<32U>(0xd1U);
    input.range =
        ingress::RunManifestRawRangeV1{
            4096U, 4224U, 1U, 1U};
    input.durable_marker.source_stream_id =
        input.source_stream_id;
    input.durable_marker.segment_sequence = 1U;
    input.durable_marker.durable_global_wal_pos =
        4224U;
    input.durable_marker
        .durable_ingress_sequence = 1U;
    input.durable_marker.durable_segment_offset =
        4224U;
    input.durable_marker.marker_flags =
        ingress::kRawV1SegmentSealed;
    RefreshMarker(&input);
    input.segment_sha256.push_back(
        Pattern<32U>(0xe1U));
    input.clock_epoch_transitions.push_back(
        ingress::RunManifestClockTransitionV1{
            4096U,
            manifest.clock_epoch_algorithm,
            manifest.clock_epoch_digest});
    manifest.raw_inputs.push_back(std::move(input));
    return manifest;
}

std::unique_ptr<ingress::BuiltRunManifestV1>
MakeBuilt(
    TestContext* test,
    std::uint8_t seed = 0x01U) {
    std::unique_ptr<ingress::BuiltRunManifestV1>
        built;
    const ingress::RunManifestV1Error error =
        ingress::BuildRunManifestV1(
            MakeManifest(seed), &built);
    test->Expect(
        error == ingress::RunManifestV1Error::kNone &&
            built != nullptr,
        "test RunManifest capability builds");
    return built;
}

bool WriteFileAt(
    int directory_fd,
    std::string_view name,
    std::string_view bytes,
    mode_t mode = 0600) {
    const std::string stable_name(name);
    const int fd = ::openat(
        directory_fd,
        stable_name.c_str(),
        O_WRONLY | O_CREAT | O_EXCL |
            O_NOFOLLOW | O_CLOEXEC,
        mode);
    if (fd < 0) {
        return false;
    }
    bool ok = ::fchmod(fd, mode) == 0;
    std::size_t completed = 0U;
    while (ok && completed < bytes.size()) {
        const ssize_t written = ::write(
            fd,
            bytes.data() + completed,
            bytes.size() - completed);
        if (written < 0 && errno == EINTR) {
            continue;
        }
        if (written <= 0) {
            ok = false;
            break;
        }
        completed +=
            static_cast<std::size_t>(written);
    }
    ok = ok && ::fsync(fd) == 0;
    static_cast<void>(::close(fd));
    return ok && ::fsync(directory_fd) == 0;
}

bool ExistsAt(
    int directory_fd,
    std::string_view name) {
    const std::string stable_name(name);
    struct stat status {};
    return ::fstatat(
               directory_fd,
               stable_name.c_str(),
               &status,
               AT_SYMLINK_NOFOLLOW) == 0;
}

std::string ReadFileAt(
    int directory_fd,
    std::string_view name) {
    const std::string stable_name(name);
    const int fd = ::openat(
        directory_fd,
        stable_name.c_str(),
        O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
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
        const ssize_t count = ::pread(
            fd,
            bytes.data() + completed,
            bytes.size() - completed,
            static_cast<off_t>(completed));
        if (count < 0 && errno == EINTR) {
            continue;
        }
        if (count <= 0) {
            bytes.clear();
            break;
        }
        completed +=
            static_cast<std::size_t>(count);
    }
    static_cast<void>(::close(fd));
    return bytes;
}

void TestFilenameAndNewPublish(TestContext* test) {
    TempDirectory directory;
    std::unique_ptr<ingress::BuiltRunManifestV1>
        built = MakeBuilt(test);
    if (directory.fd() < 0 || built == nullptr) {
        test->Expect(false, "new publish fixture");
        return;
    }

    std::string final_name = "sentinel";
    std::string temporary_name;
    test->ExpectError(
        ingress::RunManifestV1Filename(
            *built, &final_name),
        ingress::RunManifestPosixStoreErrorV1::kNone,
        "run-id-derived final filename");
    test->Expect(
        final_name ==
            "run-manifest-"
            "0102030405060708090a0b0c0d0e0f10"
            ".json",
        "final filename has frozen lowercase grammar");
    test->ExpectError(
        ingress::RunManifestV1TemporaryFilename(
            *built, &temporary_name),
        ingress::RunManifestPosixStoreErrorV1::kNone,
        "typed deterministic temporary filename");
    test->Expect(
        temporary_name ==
            "." + final_name +
                ".run-manifest-v1.tmp",
        "temporary name is uniquely derived from final");

    std::string diagnostic;
    ingress::RunManifestPosixPublishResultV1 result =
        ingress::PublishRunManifestV1At(
            directory.fd(), *built, &diagnostic);
    test->ExpectError(
        result.error,
        ingress::RunManifestPosixStoreErrorV1::kNone,
        "new RunManifest publishes");
    test->Expect(
        result.disposition ==
                ingress::
                    RunManifestPosixDispositionV1::
                        kPublishedNew &&
            result.file_synced &&
            result.directory_synced &&
            result.observed_candidate_count == 0U &&
            result.filename == final_name &&
            result.manifest_sha256 ==
                built->sha256() &&
            result.receipt != nullptr &&
            diagnostic.empty(),
        "new publication reports both durability barriers");
    test->Expect(
        ExistsAt(directory.fd(), final_name) &&
            !ExistsAt(
                directory.fd(), temporary_name) &&
            ReadFileAt(
                directory.fd(), final_name) ==
                built->canonical_jcs(),
        "new publication leaves only exact final bytes");
    struct stat final_status {};
    test->Expect(
        ::fstatat(
            directory.fd(),
            final_name.c_str(),
            &final_status,
            AT_SYMLINK_NOFOLLOW) == 0 &&
            S_ISREG(final_status.st_mode) &&
            (final_status.st_mode & 07777U) == 0600U &&
            final_status.st_nlink ==
                static_cast<nlink_t>(1),
        "published final is owner-only singly-linked regular file");
    test->Expect(
        result.receipt != nullptr &&
            result.receipt->Validate() &&
            result.receipt->filename() ==
                final_name &&
            result.receipt->run_id() ==
                built->model().run_id &&
            result.receipt->sha256() ==
                built->sha256() &&
            result.receipt->byte_count() ==
                built->canonical_jcs().size(),
        "receipt retains actual parent/final identity and readback hash");

    ingress::RunManifestPosixPublishResultV1 retry =
        ingress::PublishRunManifestV1At(
            directory.fd(), *built);
    test->ExpectError(
        retry.error,
        ingress::RunManifestPosixStoreErrorV1::kNone,
        "exact EEXIST final is accepted");
    test->Expect(
        retry.disposition ==
                ingress::
                    RunManifestPosixDispositionV1::
                        kAcceptedExistingFinal &&
            retry.file_synced &&
            retry.directory_synced &&
            retry.receipt != nullptr &&
            retry.receipt->Validate(),
        "exact EEXIST re-establishes file/parent/readback barriers");

    std::unique_ptr<ingress::BuiltRunManifestV1>
        other = MakeBuilt(test, 0x22U);
    std::string other_name;
    static_cast<void>(
        ingress::RunManifestV1Filename(
            *other, &other_name));
    test->Expect(
        WriteFileAt(
            directory.fd(),
            other_name,
            other->canonical_jcs()) &&
            result.receipt != nullptr &&
            !result.receipt->Validate() &&
            ::unlinkat(
                directory.fd(),
                other_name.c_str(),
                0) == 0 &&
            ::fsync(directory.fd()) == 0 &&
            result.receipt->Validate(),
        "receipt detects and then clears a second-candidate violation");

    const std::string moved = "moved-manifest";
    test->Expect(
        ::renameat(
            directory.fd(),
            final_name.c_str(),
            directory.fd(),
            moved.c_str()) == 0 &&
            WriteFileAt(
                directory.fd(),
                final_name,
                built->canonical_jcs()),
        "receipt replacement fixture installs a new inode");
    test->Expect(
        result.receipt != nullptr &&
            !result.receipt->Validate(),
        "receipt detects final name-to-inode replacement");
}

void TestCompleteTemporaryAdoption(TestContext* test) {
    TempDirectory directory;
    std::unique_ptr<ingress::BuiltRunManifestV1>
        built = MakeBuilt(test, 0x21U);
    if (directory.fd() < 0 || built == nullptr) {
        test->Expect(false, "temporary adoption fixture");
        return;
    }
    std::string final_name;
    std::string temporary_name;
    static_cast<void>(
        ingress::RunManifestV1Filename(
            *built, &final_name));
    static_cast<void>(
        ingress::RunManifestV1TemporaryFilename(
            *built, &temporary_name));
    test->Expect(
        WriteFileAt(
            directory.fd(),
            temporary_name,
            built->canonical_jcs()),
        "complete crash temporary fixture");

    ingress::RunManifestPosixPublishResultV1 result =
        ingress::PublishRunManifestV1At(
            directory.fd(), *built);
    test->ExpectError(
        result.error,
        ingress::RunManifestPosixStoreErrorV1::kNone,
        "complete exact temporary is adopted");
    test->Expect(
        result.disposition ==
                ingress::
                    RunManifestPosixDispositionV1::
                        kAdoptedCompleteTemporary &&
            result.observed_candidate_count == 1U &&
            result.file_synced &&
            result.directory_synced &&
            result.receipt != nullptr &&
            result.receipt->Validate() &&
            ExistsAt(directory.fd(), final_name) &&
            !ExistsAt(
                directory.fd(), temporary_name),
        "adoption performs rename, actual-parent sync and readback");
}

void TestPartialAndConflictingCandidates(
    TestContext* test) {
    {
        TempDirectory directory;
        std::unique_ptr<ingress::BuiltRunManifestV1>
            built = MakeBuilt(test, 0x31U);
        std::string final_name;
        std::string temporary_name;
        static_cast<void>(
            ingress::RunManifestV1Filename(
                *built, &final_name));
        static_cast<void>(
            ingress::RunManifestV1TemporaryFilename(
                *built, &temporary_name));
        const std::string partial(
            built->canonical_jcs().substr(
                0U,
                built->canonical_jcs().size() / 2U));
        test->Expect(
            WriteFileAt(
                directory.fd(),
                temporary_name,
                partial),
            "partial temporary fixture");
        const auto result =
            ingress::PublishRunManifestV1At(
                directory.fd(), *built);
        test->ExpectError(
            result.error,
            ingress::RunManifestPosixStoreErrorV1::
                kCandidateConflict,
            "partial deterministic temporary fails closed");
        test->Expect(
            !ExistsAt(directory.fd(), final_name) &&
                ReadFileAt(
                    directory.fd(),
                    temporary_name) == partial,
            "partial temporary is preserved without mutation");
    }
    {
        TempDirectory directory;
        std::unique_ptr<ingress::BuiltRunManifestV1>
            built = MakeBuilt(test, 0x41U);
        std::string final_name;
        static_cast<void>(
            ingress::RunManifestV1Filename(
                *built, &final_name));
        std::string conflict(
            built->canonical_jcs());
        conflict[conflict.size() / 2U] ^= 1;
        test->Expect(
            WriteFileAt(
                directory.fd(),
                final_name,
                conflict),
            "conflicting final fixture");
        const auto result =
            ingress::PublishRunManifestV1At(
                directory.fd(), *built);
        test->ExpectError(
            result.error,
            ingress::RunManifestPosixStoreErrorV1::
                kCandidateConflict,
            "same-size conflicting EEXIST final fails closed");
        test->Expect(
            ReadFileAt(
                directory.fd(), final_name) ==
                conflict,
            "conflicting final is never overwritten");
    }
}

void TestAmbiguityAndUnsafeObjects(
    TestContext* test) {
    {
        TempDirectory directory;
        std::unique_ptr<ingress::BuiltRunManifestV1>
            built = MakeBuilt(test, 0x51U);
        std::string final_name;
        std::string temporary_name;
        static_cast<void>(
            ingress::RunManifestV1Filename(
                *built, &final_name));
        static_cast<void>(
            ingress::RunManifestV1TemporaryFilename(
                *built, &temporary_name));
        test->Expect(
            WriteFileAt(
                directory.fd(),
                final_name,
                built->canonical_jcs()) &&
                WriteFileAt(
                    directory.fd(),
                    temporary_name,
                    built->canonical_jcs()),
            "double candidate fixture");
        const auto result =
            ingress::PublishRunManifestV1At(
                directory.fd(), *built);
        test->ExpectError(
            result.error,
            ingress::RunManifestPosixStoreErrorV1::
                kAmbiguousCandidates,
            "final plus temporary is a second-candidate conflict");
        test->Expect(
            result.observed_candidate_count == 2U,
            "ambiguous inventory reports bounded candidate count");
    }
    {
        TempDirectory directory;
        std::unique_ptr<ingress::BuiltRunManifestV1>
            built = MakeBuilt(test, 0x61U);
        std::unique_ptr<ingress::BuiltRunManifestV1>
            other = MakeBuilt(test, 0x71U);
        std::string other_name;
        static_cast<void>(
            ingress::RunManifestV1Filename(
                *other, &other_name));
        test->Expect(
            WriteFileAt(
                directory.fd(),
                other_name,
                other->canonical_jcs()),
            "second run candidate fixture");
        const auto result =
            ingress::PublishRunManifestV1At(
                directory.fd(), *built);
        test->ExpectError(
            result.error,
            ingress::RunManifestPosixStoreErrorV1::
                kAmbiguousCandidates,
            "dedicated run directory rejects a second run candidate");
    }
    {
        TempDirectory directory;
        std::unique_ptr<ingress::BuiltRunManifestV1>
            built = MakeBuilt(test, 0x81U);
        test->Expect(
            WriteFileAt(
                directory.fd(),
                ".run-manifest-invalid.tmp",
                "x"),
            "malformed typed-name fixture");
        const auto result =
            ingress::PublishRunManifestV1At(
                directory.fd(), *built);
        test->ExpectError(
            result.error,
            ingress::RunManifestPosixStoreErrorV1::
                kMalformedCandidateName,
            "malformed candidate-like name fails closed");
    }
    {
        TempDirectory directory;
        std::unique_ptr<ingress::BuiltRunManifestV1>
            built = MakeBuilt(test, 0x91U);
        std::string final_name;
        static_cast<void>(
            ingress::RunManifestV1Filename(
                *built, &final_name));
        test->Expect(
            ::symlinkat(
                "/dev/null",
                directory.fd(),
                final_name.c_str()) == 0,
            "symlink candidate fixture");
        const auto result =
            ingress::PublishRunManifestV1At(
                directory.fd(), *built);
        test->ExpectError(
            result.error,
            ingress::RunManifestPosixStoreErrorV1::
                kUnsafeCandidate,
            "final symlink is never followed");
    }
    {
        TempDirectory directory;
        std::unique_ptr<ingress::BuiltRunManifestV1>
            built = MakeBuilt(test, 0xa1U);
        std::string final_name;
        static_cast<void>(
            ingress::RunManifestV1Filename(
                *built, &final_name));
        test->Expect(
            WriteFileAt(
                directory.fd(),
                final_name,
                built->canonical_jcs(),
                0644),
            "wrong-mode final fixture");
        const auto result =
            ingress::PublishRunManifestV1At(
                directory.fd(), *built);
        test->ExpectError(
            result.error,
            ingress::RunManifestPosixStoreErrorV1::
                kUnsafeCandidate,
            "non-owner-only final is rejected");
    }
    {
        TempDirectory directory;
        std::unique_ptr<ingress::BuiltRunManifestV1>
            built = MakeBuilt(test, 0xb1U);
        test->Expect(
            ::fchmod(directory.fd(), 0755) == 0,
            "unsafe directory fixture");
        std::string diagnostic;
        const auto result =
            ingress::PublishRunManifestV1At(
                directory.fd(), *built, &diagnostic);
        test->ExpectError(
            result.error,
            ingress::RunManifestPosixStoreErrorV1::
                kUnsafeDirectory,
            "non-owner-only actual parent is rejected");
        test->Expect(
            diagnostic.find(directory.path()) ==
                std::string::npos,
            "diagnostics never disclose the directory path");
    }
}

void TestHardlinkAndFourMiBGate(
    TestContext* test) {
    {
        TempDirectory directory;
        std::unique_ptr<ingress::BuiltRunManifestV1>
            built = MakeBuilt(test, 0xc1U);
        std::string final_name;
        static_cast<void>(
            ingress::RunManifestV1Filename(
                *built, &final_name));
        test->Expect(
            WriteFileAt(
                directory.fd(),
                final_name,
                built->canonical_jcs()) &&
                ::linkat(
                    directory.fd(),
                    final_name.c_str(),
                    directory.fd(),
                    "extra-hardlink",
                    0) == 0,
            "hardlink candidate fixture");
        const auto result =
            ingress::PublishRunManifestV1At(
                directory.fd(), *built);
        test->ExpectError(
            result.error,
            ingress::RunManifestPosixStoreErrorV1::
                kUnsafeCandidate,
            "multiply-linked final is rejected");
    }

    ingress::RunManifestV1 oversized =
        MakeManifest(0xd1U);
    oversized.raw_inputs.front()
        .segment_sha256.assign(
            65'000U,
            Pattern<32U>(0xe1U));
    std::unique_ptr<ingress::BuiltRunManifestV1>
        impossible;
    test->Expect(
        ingress::BuildRunManifestV1(
            std::move(oversized),
            &impossible) ==
                ingress::RunManifestV1Error::
                    kEncodedSizeExceeded &&
            impossible == nullptr,
        "four-MiB model gate prevents an oversized store capability");
}

}  // namespace

int main() {
    TestContext test;
    TestFilenameAndNewPublish(&test);
    TestCompleteTemporaryAdoption(&test);
    TestPartialAndConflictingCandidates(&test);
    TestAmbiguityAndUnsafeObjects(&test);
    TestHardlinkAndFourMiBGate(&test);

    if (test.failures != 0) {
        std::cerr << test.failures
                  << " run-manifest POSIX store checks failed\n";
        return 1;
    }
    std::cout
        << "Phase 2 RunManifest POSIX store checks passed\n";
    return 0;
}
