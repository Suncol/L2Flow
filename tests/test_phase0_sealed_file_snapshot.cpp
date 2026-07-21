#include "l2flow/baseline/vendor_baseline.h"
#include "l2flow/common/sealed_file_snapshot.h"
#include "l2flow/sdk/sdk_runtime.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace common = l2flow::common;
namespace baseline = l2flow::baseline;
namespace sdk = l2flow::sdk;

namespace {

class TestContext {
public:
    void Expect(bool condition, std::string_view description) {
        if (condition) {
            return;
        }
        ++failures_;
        std::cerr << "FAIL: " << description << '\n';
    }

    [[nodiscard]] int failures() const noexcept {
        return failures_;
    }

private:
    int failures_ = 0;
};

class TemporaryDirectory {
public:
    TemporaryDirectory() {
        const auto ticks =
            std::chrono::steady_clock::now().time_since_epoch().count();
        path_ = std::filesystem::temp_directory_path() /
                ("l2flow-sealed-snapshot-" +
                 std::to_string(::getpid()) + "-" +
                 std::to_string(ticks));
        std::filesystem::create_directories(path_);
    }

    ~TemporaryDirectory() {
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
    }

    [[nodiscard]] const std::filesystem::path& path() const noexcept {
        return path_;
    }

private:
    std::filesystem::path path_;
};

class OwnedFd {
public:
    explicit OwnedFd(int fd) noexcept : fd_(fd) {}

    ~OwnedFd() {
        if (fd_ >= 0) {
            static_cast<void>(::close(fd_));
        }
    }

    OwnedFd(const OwnedFd&) = delete;
    OwnedFd& operator=(const OwnedFd&) = delete;

    [[nodiscard]] int get() const noexcept {
        return fd_;
    }

private:
    int fd_;
};

bool WriteBytes(const std::filesystem::path& path,
                std::span<const std::byte> bytes) {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    if (!output.is_open()) {
        return false;
    }
    if (!bytes.empty()) {
        output.write(
            reinterpret_cast<const char*>(bytes.data()),
            static_cast<std::streamsize>(bytes.size()));
    }
    output.flush();
    return static_cast<bool>(output);
}

bool WriteText(const std::filesystem::path& path,
               std::string_view text) {
    return WriteBytes(
        path,
        std::as_bytes(std::span{text.data(), text.size()}));
}

bool ReadSnapshot(int fd,
                  std::size_t expected_size,
                  std::vector<std::byte>* output) {
    std::vector<std::byte> value(expected_size);
    std::size_t read_bytes = 0;
    while (read_bytes < value.size()) {
        ssize_t count = 0;
        do {
            count = ::pread(
                fd,
                value.data() + read_bytes,
                value.size() - read_bytes,
                static_cast<off_t>(read_bytes));
        } while (count < 0 && errno == EINTR);
        if (count <= 0) {
            return false;
        }
        read_bytes += static_cast<std::size_t>(count);
    }
    std::byte extra{};
    ssize_t trailing = 0;
    do {
        trailing = ::pread(
            fd,
            &extra,
            1U,
            static_cast<off_t>(expected_size));
    } while (trailing < 0 && errno == EINTR);
    if (trailing != 0) {
        return false;
    }
    *output = std::move(value);
    return true;
}

const baseline::CheckResult* FindCheck(
    const baseline::PreflightReport& report,
    std::string_view id) {
    const auto found = std::find_if(
        report.checks.begin(),
        report.checks.end(),
        [id](const baseline::CheckResult& check) {
            return check.id == id;
        });
    return found == report.checks.end() ? nullptr : &*found;
}

void CheckExactImmutableSnapshot(const TemporaryDirectory& temporary,
                                 TestContext* test) {
    std::vector<std::byte> original(200003U);
    for (std::size_t index = 0; index < original.size(); ++index) {
        original[index] = static_cast<std::byte>(
            (index * 131U + index / 7U) & 0xffU);
    }

    const std::filesystem::path source =
        temporary.path() / "source.bin";
    test->Expect(
        WriteBytes(source, original),
        "writes exact-byte source fixture");

    common::SealedFileSnapshot snapshot;
    std::string error = "stale";
    test->Expect(
        common::CreateSealedFileSnapshot(
            source, &snapshot, &error),
        "creates a sealed immutable snapshot");
    test->Expect(error.empty(), "snapshot success clears error output");
    test->Expect(snapshot.valid(), "snapshot owns a valid descriptor");
    test->Expect(
        snapshot.size() == original.size(),
        "snapshot records exact source size");
    test->Expect(
        snapshot.proc_fd_path() ==
            "/proc/self/fd/" + std::to_string(snapshot.fd()),
        "snapshot exposes only its retained proc-fd path");

    std::vector<std::byte> copied;
    test->Expect(
        ReadSnapshot(snapshot.fd(), original.size(), &copied) &&
            copied == original,
        "snapshot contains every source byte exactly once");

    std::vector<std::byte> replacement(31U, std::byte{0xa5});
    test->Expect(
        WriteBytes(source, replacement),
        "mutates and truncates source after snapshot");
    std::error_code remove_error;
    std::filesystem::remove(source, remove_error);
    test->Expect(
        !remove_error,
        "removes mutable source after snapshot");

    copied.clear();
    test->Expect(
        ReadSnapshot(snapshot.fd(), original.size(), &copied) &&
            copied == original,
        "source mutation and unlink cannot change snapshot bytes");

    const int required_seals =
        F_SEAL_WRITE | F_SEAL_GROW | F_SEAL_SHRINK | F_SEAL_SEAL;
    const int seals = ::fcntl(snapshot.fd(), F_GET_SEALS);
    test->Expect(
        seals >= 0 && (seals & required_seals) == required_seals,
        "snapshot has every required immutable seal");

    const std::byte changed{0x7f};
    errno = 0;
    const ssize_t write_result =
        ::pwrite(snapshot.fd(), &changed, 1U, 0);
    test->Expect(
        write_result < 0 && errno == EPERM,
        "write seal rejects in-place overwrite");

    errno = 0;
    const int grow_result = ::ftruncate(
        snapshot.fd(),
        static_cast<off_t>(original.size() + 1U));
    test->Expect(
        grow_result != 0 && errno == EPERM,
        "grow seal rejects extension");

    errno = 0;
    const int shrink_result = ::ftruncate(
        snapshot.fd(),
        static_cast<off_t>(original.size() - 1U));
    test->Expect(
        shrink_result != 0 && errno == EPERM,
        "shrink seal rejects truncation");

    std::uint64_t validated_size =
        std::numeric_limits<std::uint64_t>::max();
    error = "stale";
    test->Expect(
        common::ValidateSealedFileSnapshotFd(
            snapshot.fd(), &validated_size, &error) &&
            validated_size == original.size() && error.empty(),
        "sealed descriptor validation confirms seals and exact size");
}

void CheckOpenFdVariant(const TemporaryDirectory& temporary,
                        TestContext* test) {
    const std::filesystem::path source =
        temporary.path() / "open-fd-source.bin";
    test->Expect(
        WriteText(source, "0123456789"),
        "writes open-fd snapshot fixture");
    const OwnedFd fd(
        ::open(source.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW));
    test->Expect(fd.get() >= 0, "opens source descriptor");
    if (fd.get() < 0) {
        return;
    }
    test->Expect(
        ::lseek(fd.get(), 7, SEEK_SET) == 7,
        "sets caller-owned source offset");

    common::SealedFileSnapshot snapshot;
    std::string error;
    test->Expect(
        common::CreateSealedFileSnapshotFromOpenFd(
            fd.get(), &snapshot, &error),
        "snapshots an already-open regular descriptor");
    test->Expect(
        ::lseek(fd.get(), 0, SEEK_CUR) == 7,
        "open-fd snapshot leaves caller offset unchanged");

    test->Expect(
        WriteText(source, "changed"),
        "mutates open-fd source pathname");
    std::vector<std::byte> copied;
    const std::string expected = "0123456789";
    test->Expect(
        ReadSnapshot(snapshot.fd(), expected.size(), &copied) &&
            copied ==
                std::vector<std::byte>(
                    std::as_bytes(std::span{
                        expected.data(), expected.size()}).begin(),
                    std::as_bytes(std::span{
                        expected.data(), expected.size()}).end()),
        "open-fd snapshot remains independent of source mutation");
}

void CheckRejectionsAndPrivacy(const TemporaryDirectory& temporary,
                               TestContext* test) {
    const std::string secret_name =
        "TOP-SECRET-vendor-library-name.so";
    const std::filesystem::path regular =
        temporary.path() / secret_name;
    test->Expect(
        WriteText(regular, "ordinary"),
        "writes privacy fixture");

    const std::filesystem::path symlink =
        temporary.path() / "TOP-SECRET-symlink.so";
    std::error_code symlink_error;
    std::filesystem::create_symlink(
        regular, symlink, symlink_error);
    test->Expect(!symlink_error, "creates symlink rejection fixture");

    common::SealedFileSnapshot retained;
    std::string error;
    test->Expect(
        common::CreateSealedFileSnapshot(
            regular, &retained, &error),
        "creates retained snapshot before failure tests");
    const int retained_fd = retained.fd();

    error.clear();
    test->Expect(
        !common::CreateSealedFileSnapshot(
            symlink, &retained, &error),
        "final-component symlink is rejected");
    test->Expect(
        retained.fd() == retained_fd && retained.valid(),
        "failed creation leaves existing output snapshot unchanged");
    test->Expect(
        error.find("TOP-SECRET") == std::string::npos &&
            error.find(symlink.string()) == std::string::npos,
        "symlink error does not disclose source pathname");

    const std::filesystem::path directory =
        temporary.path() / "TOP-SECRET-directory";
    std::filesystem::create_directory(directory);
    error.clear();
    common::SealedFileSnapshot rejected;
    test->Expect(
        !common::CreateSealedFileSnapshot(
            directory, &rejected, &error) &&
            !rejected.valid(),
        "directory is rejected as a non-regular source");
    test->Expect(
        error.find("TOP-SECRET") == std::string::npos &&
            error.find(directory.string()) == std::string::npos,
        "non-regular error does not disclose source pathname");

    const std::filesystem::path fifo =
        temporary.path() / "TOP-SECRET-fifo";
    const int fifo_result = ::mkfifo(fifo.c_str(), 0600);
    test->Expect(fifo_result == 0, "creates FIFO rejection fixture");
    if (fifo_result == 0) {
        error.clear();
        test->Expect(
            !common::CreateSealedFileSnapshot(
                fifo, &rejected, &error),
            "FIFO is rejected without a blocking open");
        test->Expect(
            error.find("TOP-SECRET") == std::string::npos,
            "FIFO error does not disclose source pathname");
    }

    const std::filesystem::path missing =
        temporary.path() / "TOP-SECRET-missing.so";
    error.clear();
    test->Expect(
        !common::CreateSealedFileSnapshot(
            missing, &rejected, &error),
        "missing source is rejected");
    test->Expect(
        error.find("TOP-SECRET") == std::string::npos &&
            error.find(missing.string()) == std::string::npos,
        "open error does not disclose source pathname");

    const OwnedFd ordinary_fd(
        ::open(regular.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW));
    std::uint64_t ignored_size = 0;
    error.clear();
    test->Expect(
        ordinary_fd.get() >= 0 &&
            !common::ValidateSealedFileSnapshotFd(
                ordinary_fd.get(), &ignored_size, &error),
        "ordinary regular fd cannot bypass sealed-snapshot validation");
    if (ordinary_fd.get() >= 0) {
        const baseline::PreflightReport bypass_attempt =
            baseline::
                RunApprovedLibraryRuntimePreflightForSealedSnapshotFd(
                    ordinary_fd.get());
        const baseline::CheckResult* bypass_hash =
            FindCheck(
                bypass_attempt,
                "artifact.shared_library_sha256");
        test->Expect(
            !bypass_attempt.passed() &&
                !bypass_attempt.runtime_probe_attempted &&
                bypass_hash != nullptr &&
                bypass_hash->actual == "<unavailable>",
            "preflight sealed-fd continuation rejects an ordinary fd");

        const baseline::PreflightReport copied_preflight =
            baseline::RunApprovedLibraryRuntimePreflightForOpenFd(
                ordinary_fd.get());
        const baseline::CheckResult* copied_hash =
            FindCheck(
                copied_preflight,
                "artifact.shared_library_sha256");
        test->Expect(
            !copied_preflight.passed() &&
                !copied_preflight.runtime_probe_attempted &&
                copied_hash != nullptr &&
                copied_hash->actual == "<unavailable>",
            "ordinary open-fd preflight rejects a wrong-sized snapshot "
            "before hashing or runtime");
    }

    const baseline::PreflightReport sealed_preflight =
        baseline::
            RunApprovedLibraryRuntimePreflightForSealedSnapshotFd(
                retained.fd());
    const baseline::CheckResult* sealed_hash =
        FindCheck(
            sealed_preflight,
            "artifact.shared_library_sha256");
    test->Expect(
        !sealed_preflight.passed() &&
            !sealed_preflight.runtime_probe_attempted &&
            sealed_hash != nullptr &&
            sealed_hash->actual == "<unavailable>",
        "sealed-fd continuation rejects a wrong-sized immutable snapshot "
        "before hashing or runtime");

    error.clear();
    const std::shared_ptr<sdk::SdkFactory> rejected_factory =
        sdk::LoadApprovedSdkFactory(regular, &error);
    test->Expect(
        rejected_factory == nullptr && !error.empty(),
        "production loader rejects a sealed but unapproved snapshot");
    test->Expect(
        error.find(secret_name) == std::string::npos &&
            error.find(regular.string()) == std::string::npos,
        "production-loader rejection does not disclose source pathname");

    error.clear();
    test->Expect(
        !common::ValidateSealedFileSnapshotFd(
            -1, &ignored_size, &error),
        "invalid fd cannot bypass sealed-snapshot validation");
}

void CheckExactSizeAndDescriptorGuards(
    const TemporaryDirectory& temporary,
    TestContext* test) {
    const std::filesystem::path sparse =
        temporary.path() / "oversized-sparse.bin";
    const int sparse_fd =
        ::open(
            sparse.c_str(),
            O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC,
            0600);
    test->Expect(
        sparse_fd >= 0,
        "creates sparse exact-size rejection fixture");
    if (sparse_fd >= 0) {
        constexpr off_t kSparseSize =
            static_cast<off_t>(1U) << 30U;
        const int truncate_result =
            ::ftruncate(sparse_fd, kSparseSize);
        const int close_result =
            ::close(sparse_fd);
        test->Expect(
            truncate_result == 0 && close_result == 0,
            "creates a one-GiB sparse source without copying it");

        common::SealedFileSnapshot rejected;
        std::string error;
        const auto started =
            std::chrono::steady_clock::now();
        test->Expect(
            !common::CreateSealedFileSnapshot(
                sparse,
                &rejected,
                &error,
                17U) &&
                !rejected.valid() &&
                !error.empty(),
            "exact-size capture rejects a mismatched regular file");
        test->Expect(
            std::chrono::steady_clock::now() - started <
                std::chrono::seconds(1),
            "exact-size rejection occurs before copying the sparse file");
    }

    int partial_fd = ::memfd_create(
        "l2flow-partial-seals",
        MFD_ALLOW_SEALING | MFD_CLOEXEC);
    test->Expect(
        partial_fd >= 0,
        "creates partial-seal memfd fixture");
    if (partial_fd >= 0) {
        const std::byte value{0x42};
        const ssize_t written =
            ::write(partial_fd, &value, 1U);
        const int seal_result =
            ::fcntl(
                partial_fd,
                F_ADD_SEALS,
                F_SEAL_WRITE);
        std::uint64_t ignored_size = 0U;
        std::string error;
        test->Expect(
            written == 1 &&
                seal_result == 0 &&
                !common::ValidateSealedFileSnapshotFd(
                    partial_fd,
                    &ignored_size,
                    &error),
            "a memfd with only a subset of required seals is rejected");
        const baseline::PreflightReport report =
            baseline::
                RunApprovedLibraryRuntimePreflightForSealedSnapshotFd(
                    partial_fd);
        const baseline::CheckResult* hash =
            FindCheck(
                report,
                "artifact.shared_library_sha256");
        test->Expect(
            !report.runtime_probe_attempted &&
                hash != nullptr &&
                hash->actual == "<unavailable>",
            "partial seals cannot reach hash or vendor runtime");
        static_cast<void>(::close(partial_fd));
    }

    const std::filesystem::path cloexec_source =
        temporary.path() / "cloexec-source.bin";
    test->Expect(
        WriteText(cloexec_source, "sealed"),
        "writes close-on-exec validation fixture");
    common::SealedFileSnapshot no_cloexec;
    std::string error;
    test->Expect(
        common::CreateSealedFileSnapshot(
            cloexec_source,
            &no_cloexec,
            &error),
        "creates fully sealed close-on-exec fixture");
    if (no_cloexec.valid()) {
        const int flags =
            ::fcntl(no_cloexec.fd(), F_GETFD);
        test->Expect(
            flags >= 0 &&
                ::fcntl(
                    no_cloexec.fd(),
                    F_SETFD,
                    flags & ~FD_CLOEXEC) == 0,
            "clears close-on-exec on the validation fixture");
        std::uint64_t ignored_size = 0U;
        error.clear();
        test->Expect(
            !common::ValidateSealedFileSnapshotFd(
                no_cloexec.fd(),
                &ignored_size,
                &error),
            "a fully sealed fd without close-on-exec is rejected");
        const baseline::PreflightReport report =
            baseline::
                RunApprovedLibraryRuntimePreflightForSealedSnapshotFd(
                    no_cloexec.fd());
        const baseline::CheckResult* hash =
            FindCheck(
                report,
                "artifact.shared_library_sha256");
        test->Expect(
            !report.runtime_probe_attempted &&
                hash != nullptr &&
                hash->actual == "<unavailable>",
            "missing close-on-exec cannot reach hash or vendor runtime");
    }
}

void CheckEmptySnapshot(const TemporaryDirectory& temporary,
                        TestContext* test) {
    const std::filesystem::path empty =
        temporary.path() / "empty.bin";
    test->Expect(WriteText(empty, {}), "writes empty source fixture");
    common::SealedFileSnapshot snapshot;
    std::string error;
    test->Expect(
        common::CreateSealedFileSnapshot(
            empty, &snapshot, &error) &&
            snapshot.size() == 0U,
        "empty regular file produces a valid sealed snapshot");
}

}  // namespace

int main() {
    TemporaryDirectory temporary;
    TestContext test;

    CheckExactImmutableSnapshot(temporary, &test);
    CheckOpenFdVariant(temporary, &test);
    CheckRejectionsAndPrivacy(temporary, &test);
    CheckExactSizeAndDescriptorGuards(temporary, &test);
    CheckEmptySnapshot(temporary, &test);

    if (test.failures() != 0) {
        std::cerr << test.failures()
                  << " sealed snapshot assertion(s) failed\n";
        return 1;
    }
    std::cout
        << "sealed snapshots are exact, immutable, and path-private\n";
    return 0;
}
