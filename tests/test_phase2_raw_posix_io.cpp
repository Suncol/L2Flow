#include "l2flow/ingress/raw_posix_io.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <span>
#include <string>

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

int MakeTemporaryFile(std::string* retained_path) {
    char path[] = "/tmp/l2flow-raw-io-XXXXXX";
    const int fd = ::mkstemp(path);
    if (fd >= 0) {
        static_cast<void>(::fchmod(fd, 0600));
        *retained_path = path;
    }
    return fd;
}

}  // namespace

int main() {
    TestContext test;

    std::string segment_path;
    std::string journal_path;
    int segment_fd = MakeTemporaryFile(&segment_path);
    int journal_fd = MakeTemporaryFile(&journal_path);
    test.Expect(
        segment_fd >= 0 && journal_fd >= 0,
        "temporary retained descriptors are created");

    std::string error;
    std::unique_ptr<ingress::RawWalIo> io =
        ingress::AdoptPosixRawWalIo(
            segment_fd, journal_fd, &error);
    test.Expect(io != nullptr, "valid retained descriptors are adopted");
    if (io != nullptr) {
        const std::array<std::byte, 3U> first{
            std::byte{'a'}, std::byte{'b'}, std::byte{'c'}};
        const std::array<std::byte, 2U> second{
            std::byte{'d'}, std::byte{'e'}};
        const std::array<ingress::RawWalIoVector, 2U> vectors{{
            {std::span<const std::byte>(first)},
            {std::span<const std::byte>(second)},
        }};
        const ingress::RawWalWriteResult write =
            io->WritevSome(
                ingress::RawWalFile::kSegment,
                7U,
                vectors);
        test.Expect(
            write.error_number == 0 &&
                write.bytes_written == 5U,
            "pwritev backend writes all supplied vectors");
        std::array<char, 5U> actual{};
        const ssize_t read =
            ::pread(segment_fd, actual.data(), actual.size(), 7);
        test.Expect(
            read == 5 &&
                std::string(actual.data(), actual.size()) == "abcde",
            "write uses the explicit requested offset");
        test.Expect(
            io->Fdatasync(ingress::RawWalFile::kSegment) == 0,
            "segment fdatasync succeeds");
        test.Expect(
            io->Truncate(
                ingress::RawWalFile::kSegment, 9U) == 0,
            "segment truncate succeeds");
        struct stat status {};
        test.Expect(
            ::fstat(segment_fd, &status) == 0 &&
                status.st_size == 9,
            "truncate changes the logical file size");
        test.Expect(
            io->Close(ingress::RawWalFile::kSegment) == 0 &&
                io->Close(ingress::RawWalFile::kSegment) == 0,
            "close is idempotent");
        test.Expect(
            io->Close(ingress::RawWalFile::kJournal) == 0,
            "journal closes cleanly");
    } else {
        if (segment_fd >= 0) {
            static_cast<void>(::close(segment_fd));
        }
        if (journal_fd >= 0) {
            static_cast<void>(::close(journal_fd));
        }
    }
    if (!segment_path.empty()) {
        static_cast<void>(::unlink(segment_path.c_str()));
    }
    if (!journal_path.empty()) {
        static_cast<void>(::unlink(journal_path.c_str()));
    }

    std::string readonly_path;
    const int readonly = MakeTemporaryFile(&readonly_path);
    const int duplicate =
        readonly >= 0 ? ::dup(readonly) : -1;
    if (readonly >= 0) {
        const int flags = ::fcntl(readonly, F_GETFL);
        if (flags >= 0) {
            const std::string path =
                "/proc/self/fd/" + std::to_string(readonly);
            const int replacement =
                ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
            if (replacement >= 0) {
                static_cast<void>(::close(readonly));
                std::unique_ptr<ingress::RawWalIo> rejected =
                    ingress::AdoptPosixRawWalIo(
                        replacement, duplicate, &error);
                test.Expect(
                    rejected == nullptr,
                    "read-only descriptor is rejected");
                static_cast<void>(::close(replacement));
                static_cast<void>(::close(duplicate));
            } else {
                static_cast<void>(::close(readonly));
                static_cast<void>(::close(duplicate));
            }
        }
    }
    if (!readonly_path.empty()) {
        static_cast<void>(::unlink(readonly_path.c_str()));
    }

    char directory_pattern[] =
        "/tmp/l2flow-target-bound-io-XXXXXX";
    char* const directory_path =
        ::mkdtemp(directory_pattern);
    const int directory_fd =
        directory_path == nullptr
            ? -1
            : ::open(
                  directory_path,
                  O_RDONLY | O_DIRECTORY |
                      O_NOFOLLOW | O_CLOEXEC);
    const int bound_segment =
        directory_fd < 0
            ? -1
            : ::openat(
                  directory_fd,
                  "segment-00000001.raw",
                  O_RDWR | O_CREAT | O_EXCL |
                      O_NOFOLLOW | O_CLOEXEC,
                  0600);
    const int bound_journal =
        directory_fd < 0
            ? -1
            : ::openat(
                  directory_fd,
                  "durable.journal",
                  O_RDWR | O_CREAT | O_EXCL |
                      O_NOFOLLOW | O_CLOEXEC,
                  0600);
    test.Expect(
        directory_fd >= 0 &&
            bound_segment >= 0 &&
            bound_journal >= 0,
        "target-bound fixture creates canonical retained names");
    std::unique_ptr<ingress::RawWalIo> wrong_name =
        ingress::AdoptTargetBoundPosixRawWalIo(
            directory_fd,
            "segment-00000002.raw",
            bound_segment,
            bound_journal,
            &error);
    test.Expect(
        wrong_name == nullptr,
        "target-bound adoption rejects a non-naming segment path");
    std::unique_ptr<ingress::RawWalIo> bound =
        ingress::AdoptTargetBoundPosixRawWalIo(
            directory_fd,
            "segment-00000001.raw",
            bound_segment,
            bound_journal,
            &error);
    auto* const target_provider =
        dynamic_cast<
            ingress::
                RawReserveMutationTargetProviderV1*>(
            bound.get());
    struct stat expected_directory {};
    struct stat retained_directory {};
    test.Expect(
        bound != nullptr &&
            target_provider != nullptr &&
            ::fstat(
                directory_fd,
                &expected_directory) == 0 &&
            ::fstat(
                target_provider
                    ->RawReserveMutationTargetDirectoryDescriptorV1(),
                &retained_directory) == 0 &&
            expected_directory.st_dev ==
                retained_directory.st_dev &&
            expected_directory.st_ino ==
                retained_directory.st_ino,
        "target-bound WAL I/O retains the exact stream-day target proof");
    const bool replaced =
        bound != nullptr &&
        ::renameat(
            directory_fd,
            "segment-00000001.raw",
            directory_fd,
            "segment-00000001.moved") == 0;
    test.Expect(
        replaced &&
            bound->Fdatasync(
                ingress::RawWalFile::kSegment) ==
                ESTALE,
        "target-bound WAL I/O rejects a replaced canonical segment name");
    if (bound != nullptr) {
        static_cast<void>(
            bound->Close(
                ingress::RawWalFile::kSegment));
        static_cast<void>(
            bound->Close(
                ingress::RawWalFile::kJournal));
        bound.reset();
    } else {
        if (bound_segment >= 0) {
            static_cast<void>(::close(bound_segment));
        }
        if (bound_journal >= 0) {
            static_cast<void>(::close(bound_journal));
        }
    }
    if (directory_fd >= 0) {
        static_cast<void>(
            ::unlinkat(
                directory_fd,
                "segment-00000001.moved",
                0));
        static_cast<void>(
            ::unlinkat(
                directory_fd,
                "segment-00000001.raw",
                0));
        static_cast<void>(
            ::unlinkat(
                directory_fd,
                "durable.journal",
                0));
        static_cast<void>(::close(directory_fd));
    }
    if (directory_path != nullptr) {
        static_cast<void>(::rmdir(directory_path));
    }

    if (test.failures != 0) {
        std::cerr << test.failures
                  << " Phase 2 POSIX I/O tests failed\n";
        return 1;
    }
    std::cout << "Phase 2 POSIX I/O tests passed\n";
    return 0;
}
