#include "l2flow/ingress/shadow_capture.h"
#include "l2flow/ops/metrics_textfile.h"
#include "l2flow/ops/stable_output_prefix.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace ops = l2flow::ops;

namespace {

constexpr std::size_t kMaximumMetricsBytes =
    ops::kMaximumPrometheusTextfileBodyBytes;

struct TestContext final {
    void Expect(
        bool condition,
        const std::string& description) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << description << '\n';
        }
    }

    int failures = 0;
};

[[noreturn]] void ThrowErrno(const char* operation) {
    throw std::runtime_error(
        std::string(operation) + " failed: " +
        std::strerror(errno));
}

class ScopedFd final {
public:
    explicit ScopedFd(int fd = -1) noexcept : fd_(fd) {}

    ~ScopedFd() {
        if (fd_ >= 0) {
            static_cast<void>(::close(fd_));
        }
    }

    ScopedFd(const ScopedFd&) = delete;
    ScopedFd& operator=(const ScopedFd&) = delete;

    [[nodiscard]] int get() const noexcept {
        return fd_;
    }

private:
    int fd_;
};

class TempDirectory final {
public:
    TempDirectory() {
        std::array<char, 64U> pattern{};
        constexpr std::string_view value =
            "/tmp/l2flow-metrics-textfile-XXXXXX";
        static_assert(value.size() < pattern.size());
        std::copy(
            value.begin(), value.end(), pattern.begin());
        const char* const created = ::mkdtemp(pattern.data());
        if (created == nullptr) {
            ThrowErrno("mkdtemp");
        }
        path_ = created;
    }

    ~TempDirectory() {
        std::error_code ignored;
        static_cast<void>(
            std::filesystem::remove_all(path_, ignored));
    }

    TempDirectory(const TempDirectory&) = delete;
    TempDirectory& operator=(const TempDirectory&) = delete;

    [[nodiscard]] const std::string& path() const noexcept {
        return path_;
    }

    [[nodiscard]] std::string Child(
        std::string_view name) const {
        return path_ + "/" + std::string(name);
    }

private:
    std::string path_;
};

void WriteExclusiveFixture(
    const std::string& path,
    std::string_view contents,
    mode_t mode) {
    int fd = -1;
    do {
        fd = ::open(
            path.c_str(),
            O_WRONLY | O_CREAT | O_EXCL |
                O_CLOEXEC | O_NOFOLLOW,
            0600);
    } while (fd < 0 && errno == EINTR);
    if (fd < 0) {
        ThrowErrno("create fixture file");
    }

    std::size_t offset = 0U;
    while (offset < contents.size()) {
        const ssize_t count =
            ::write(
                fd,
                contents.data() + offset,
                contents.size() - offset);
        if (count < 0 && errno == EINTR) {
            continue;
        }
        if (count <= 0) {
            const int saved_errno =
                count < 0 ? errno : EIO;
            static_cast<void>(::close(fd));
            errno = saved_errno;
            ThrowErrno("write fixture file");
        }
        offset += static_cast<std::size_t>(count);
    }
    if (::fchmod(fd, mode) != 0) {
        const int saved_errno = errno;
        static_cast<void>(::close(fd));
        errno = saved_errno;
        ThrowErrno("set fixture file mode");
    }
    if (::close(fd) != 0) {
        ThrowErrno("close fixture file");
    }
}

class ScopedUnlink final {
public:
    explicit ScopedUnlink(std::string path)
        : path_(std::move(path)) {}

    ~ScopedUnlink() {
        static_cast<void>(::unlink(path_.c_str()));
    }

    ScopedUnlink(const ScopedUnlink&) = delete;
    ScopedUnlink& operator=(const ScopedUnlink&) = delete;

private:
    std::string path_;
};

[[nodiscard]] ScopedFd OpenReadOnly(
    const std::string& path) {
    int fd = -1;
    do {
        fd = ::open(
            path.c_str(),
            O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NOCTTY);
    } while (fd < 0 && errno == EINTR);
    if (fd < 0) {
        ThrowErrno("open metrics file");
    }
    return ScopedFd(fd);
}

[[nodiscard]] std::string ReadAllRaw(int fd) {
    if (::lseek(fd, 0, SEEK_SET) < 0) {
        ThrowErrno("seek metrics file");
    }

    std::string contents;
    std::array<char, 4096U> buffer{};
    for (;;) {
        const ssize_t count =
            ::read(fd, buffer.data(), buffer.size());
        if (count < 0 && errno == EINTR) {
            continue;
        }
        if (count < 0) {
            ThrowErrno("read metrics file");
        }
        if (count == 0) {
            return contents;
        }
        contents.append(
            buffer.data(), static_cast<std::size_t>(count));
    }
}

[[nodiscard]] std::string StripMetricsMagic(
    std::string contents) {
    if (contents.starts_with(
            ops::kPrometheusTextfileMagic)) {
        contents.erase(
            0U,
            ops::kPrometheusTextfileMagic.size());
    }
    return contents;
}

[[nodiscard]] std::string ReadAll(int fd) {
    return StripMetricsMagic(
        ReadAllRaw(fd));
}

[[nodiscard]] std::string ReadFile(
    const std::string& path) {
    ScopedFd fd = OpenReadOnly(path);
    return ReadAll(fd.get());
}

[[nodiscard]] std::string ReadFileRaw(
    const std::string& path) {
    ScopedFd fd = OpenReadOnly(path);
    return ReadAllRaw(fd.get());
}

[[nodiscard]] struct stat StatFile(
    const std::string& path) {
    struct stat metadata {};
    if (::lstat(path.c_str(), &metadata) != 0) {
        ThrowErrno("stat metrics file");
    }
    return metadata;
}

[[nodiscard]] std::size_t CountTemporaryFiles(
    const std::string& directory) {
    constexpr std::string_view prefix =
        ".l2flow-metrics.tmp.";
    std::size_t count = 0U;
    for (const auto& entry :
         std::filesystem::directory_iterator(
             directory)) {
        const std::string filename =
            entry.path().filename().string();
        if (filename.starts_with(prefix)) {
            ++count;
        }
    }
    return count;
}

void ExpectTargetUnchanged(
    TestContext* test,
    const std::string& target,
    std::string_view expected_contents,
    const struct stat& expected_metadata,
    const std::string& description) {
    const struct stat actual_metadata = StatFile(target);
    test->Expect(
        ReadFile(target) == expected_contents,
        description + " preserves target contents");
    test->Expect(
        actual_metadata.st_dev == expected_metadata.st_dev &&
            actual_metadata.st_ino == expected_metadata.st_ino,
        description + " preserves target inode");
    test->Expect(
        (actual_metadata.st_mode & 07777) ==
            (expected_metadata.st_mode & 07777),
        description + " preserves target permissions");
}

void CheckAtomicPublication(
    TestContext* test,
    const TempDirectory& temporary,
    std::string* target,
    std::string* current_contents,
    struct stat* current_metadata) {
    *target = temporary.Child("l2flow.prom");
    constexpr std::string_view first =
        "# HELP l2flow_ready Readiness state.\n"
        "# TYPE l2flow_ready gauge\n"
        "l2flow_ready 0\n";

    std::string error = "stale error";
    test->Expect(
        ops::PublishPrometheusTextfile(
            *target, first, &error),
        "first metrics publication succeeds");
    test->Expect(
        error.empty(),
        "successful first publication clears the error");
    const struct stat first_metadata = StatFile(*target);
    test->Expect(
        S_ISREG(first_metadata.st_mode),
        "first publication creates a regular file");
    test->Expect(
        (first_metadata.st_mode & 07777) == 0600,
        "first publication creates exact owner-only mode 0600");
    test->Expect(
        ReadFile(*target) == first,
        "first publication writes the complete metrics text");
    test->Expect(
        ReadFileRaw(*target).starts_with(
            ops::kPrometheusTextfileMagic),
        "first publication persists the frozen metrics type marker");
    test->Expect(
        CountTemporaryFiles(temporary.path()) == 0U,
        "first publication leaves no temporary file");

    ScopedFd old_reader = OpenReadOnly(*target);
    std::string second =
        "# HELP l2flow_ready Readiness state after replacement.\n"
        "# TYPE l2flow_ready gauge\n"
        "l2flow_ready 1\n";
    second.append(32768U, '#');
    second.push_back('\n');

    error = "stale error";
    test->Expect(
        ops::PublishPrometheusTextfile(
            *target, second, &error),
        "second metrics publication succeeds");
    test->Expect(
        error.empty(),
        "successful second publication clears the error");

    const struct stat second_metadata = StatFile(*target);
    test->Expect(
        second_metadata.st_dev != first_metadata.st_dev ||
            second_metadata.st_ino != first_metadata.st_ino,
        "second publication atomically replaces the target inode");
    test->Expect(
        (second_metadata.st_mode & 07777) == 0600,
        "replacement metrics file retains exact mode 0600");
    test->Expect(
        ReadFile(*target) == second,
        "target path exposes the complete replacement text");
    test->Expect(
        ReadAll(old_reader.get()) == first,
        "reader holding the old inode still sees complete old text");
    test->Expect(
        CountTemporaryFiles(temporary.path()) == 0U,
        "second publication leaves no temporary file");

    *current_contents = std::move(second);
    *current_metadata = second_metadata;
}

void ExpectRejectedContents(
    TestContext* test,
    const TempDirectory& temporary,
    const std::string& target,
    std::string_view contents,
    std::string_view expected_contents,
    const struct stat& expected_metadata,
    const std::string& description) {
    std::string error = "stale error";
    test->Expect(
        !ops::PublishPrometheusTextfile(
            target, contents, &error),
        description + " is rejected");
    test->Expect(
        !error.empty(),
        description + " reports an error");
    test->Expect(
        error.find(target) == std::string::npos &&
            (contents.empty() ||
             error.find(contents) == std::string::npos),
        description + " does not disclose its path or metrics");
    ExpectTargetUnchanged(
        test,
        target,
        expected_contents,
        expected_metadata,
        description);
    test->Expect(
        CountTemporaryFiles(temporary.path()) == 0U,
        description + " leaves no temporary file");
}

void CheckRejectedContents(
    TestContext* test,
    const TempDirectory& temporary,
    const std::string& target,
    std::string_view expected_contents,
    const struct stat& expected_metadata) {
    ExpectRejectedContents(
        test,
        temporary,
        target,
        std::string_view(),
        expected_contents,
        expected_metadata,
        "empty metrics text");

    constexpr char embedded_nul[] =
        "l2flow_before_nul 1\n\0l2flow_after_nul 2\n";
    ExpectRejectedContents(
        test,
        temporary,
        target,
        std::string_view(
            embedded_nul, sizeof(embedded_nul) - 1U),
        expected_contents,
        expected_metadata,
        "metrics text containing NUL");

    const std::string oversized(
        kMaximumMetricsBytes + 1U, 'x');
    ExpectRejectedContents(
        test,
        temporary,
        target,
        oversized,
        expected_contents,
        expected_metadata,
        "metrics body larger than its maximum marked-file budget");
}

void CheckRejectedPaths(
    TestContext* test,
    const TempDirectory& temporary,
    const std::string& target,
    std::string_view expected_contents,
    const struct stat& expected_metadata) {
    constexpr std::string_view valid_metrics =
        "l2flow_path_validation 1\n";
    std::string error = "stale error";
    test->Expect(
        !ops::PublishPrometheusTextfile(
            std::string(), valid_metrics, &error),
        "empty metrics path is rejected");
    test->Expect(
        !error.empty(),
        "empty metrics path reports an error");

    const std::string relative =
        "l2flow-metrics-relative-" +
        std::to_string(
            static_cast<unsigned long long>(::getpid())) +
        ".prom";
    ScopedUnlink relative_cleanup(relative);
    struct stat unused {};
    if (::lstat(relative.c_str(), &unused) == 0 ||
        errno != ENOENT) {
        throw std::runtime_error(
            "relative metrics fixture path already exists");
    }
    error = "stale error";
    test->Expect(
        !ops::PublishPrometheusTextfile(
            relative, valid_metrics, &error),
        "relative metrics path is rejected");
    test->Expect(
        !error.empty(),
        "relative metrics path reports an error");
    errno = 0;
    test->Expect(
        ::lstat(relative.c_str(), &unused) != 0 &&
            errno == ENOENT,
        "relative metrics path creates no target");

    std::string nul_path = target;
    nul_path.push_back('\0');
    nul_path.append("ignored-suffix");
    error = "stale error";
    test->Expect(
        !ops::PublishPrometheusTextfile(
            nul_path, valid_metrics, &error),
        "metrics path containing NUL is rejected");
    test->Expect(
        !error.empty(),
        "metrics path containing NUL reports an error");
    ExpectTargetUnchanged(
        test,
        target,
        expected_contents,
        expected_metadata,
        "metrics path containing NUL");
    test->Expect(
        CountTemporaryFiles(temporary.path()) == 0U,
        "rejected paths leave no target temporary file");

    const std::string reserved =
        temporary.Child(
            ops::kSdkLogDirectoryMarkerFilename);
    error = "stale error";
    test->Expect(
        !ops::PublishPrometheusTextfile(
            reserved,
            valid_metrics,
            &error),
        "reserved SDK log marker basename is rejected for metrics");
    test->Expect(
        !error.empty() &&
            error.find(reserved) == std::string::npos &&
            ::lstat(reserved.c_str(), &unused) != 0 &&
            errno == ENOENT,
        "reserved metrics basename creates no file and discloses no path");
}

void CheckAmbiguousPathRejection(
    TestContext* test,
    const TempDirectory& temporary,
    const std::string& target,
    std::string_view expected_contents,
    const struct stat& expected_metadata) {
    constexpr std::string_view metrics =
        "l2flow_ambiguous_path 1\n";
    const std::array<std::string, 4U> ambiguous_paths{
        temporary.path() + "//l2flow.prom",
        temporary.path() + "/./l2flow.prom",
        temporary.path() + "/unused/../l2flow.prom",
        target + "/"};

    for (const std::string& ambiguous : ambiguous_paths) {
        std::string error = "stale error";
        test->Expect(
            !ops::PublishPrometheusTextfile(
                ambiguous, metrics, &error),
            "ambiguous metrics path is rejected");
        test->Expect(
            !error.empty() &&
                error.find(ambiguous) ==
                    std::string::npos,
            "ambiguous path rejection is path-redacted");
    }
    ExpectTargetUnchanged(
        test,
        target,
        expected_contents,
        expected_metadata,
        "ambiguous path rejection");
    test->Expect(
        CountTemporaryFiles(temporary.path()) == 0U,
        "ambiguous path rejection leaves no temporary file");
}

void CheckUntrustedDirectoryRejection(
    TestContext* test,
    const TempDirectory& temporary) {
    const std::string directory =
        temporary.Child("writable-parent");
    if (::mkdir(directory.c_str(), 0700) != 0) {
        ThrowErrno("mkdir writable parent");
    }
    const std::string target =
        directory + "/private-metrics.prom";
    constexpr std::string_view original =
        "l2flow_untrusted_directory 0\n";
    std::string error;
    if (!ops::PublishPrometheusTextfile(
            target, original, &error)) {
        throw std::runtime_error(
            "create writable-directory fixture failed");
    }
    const struct stat original_metadata =
        StatFile(target);
    if (::chmod(directory.c_str(), 0722) != 0) {
        ThrowErrno("chmod writable parent");
    }

    constexpr std::string_view replacement =
        "l2flow_untrusted_directory 1\n";
    error = "stale error";
    test->Expect(
        !ops::PublishPrometheusTextfile(
            target, replacement, &error),
        "group/world-writable destination directory is rejected");
    test->Expect(
        !error.empty() &&
            error.find(directory) == std::string::npos &&
            error.find(replacement) ==
                std::string::npos,
        "untrusted-directory error discloses no path or metrics");
    ExpectTargetUnchanged(
        test,
        target,
        original,
        original_metadata,
        "untrusted destination directory");
    test->Expect(
        CountTemporaryFiles(directory) == 0U,
        "untrusted destination directory leaves no temporary file");

    if (::chmod(directory.c_str(), 0700) != 0) {
        ThrowErrno("restore writable parent mode");
    }

    const std::string unsafe_ancestor =
        temporary.Child("unsafe-ancestor");
    const std::string nested_destination =
        unsafe_ancestor + "/private-child";
    if (::mkdir(unsafe_ancestor.c_str(), 0700) != 0 ||
        ::mkdir(nested_destination.c_str(), 0700) != 0) {
        ThrowErrno("mkdir unsafe metrics ancestor fixture");
    }
    const std::string nested_target =
        nested_destination + "/metrics.prom";
    if (!ops::PublishPrometheusTextfile(
            nested_target, original, &error)) {
        throw std::runtime_error(
            "create unsafe-ancestor metrics fixture failed");
    }
    const struct stat nested_metadata =
        StatFile(nested_target);
    if (::chmod(unsafe_ancestor.c_str(), 0777) != 0) {
        ThrowErrno("chmod unsafe metrics ancestor");
    }
    test->Expect(
        !ops::PublishPrometheusTextfile(
            nested_target, replacement, &error),
        "a non-sticky writable non-final ancestor is rejected");
    ExpectTargetUnchanged(
        test,
        nested_target,
        original,
        nested_metadata,
        "unsafe non-final metrics ancestor");
    test->Expect(
        CountTemporaryFiles(nested_destination) == 0U,
        "unsafe non-final ancestor leaves no temporary file");
    if (::chmod(unsafe_ancestor.c_str(), 0700) != 0) {
        ThrowErrno("restore unsafe metrics ancestor mode");
    }
}

void CheckParentSymlinkRejection(
    TestContext* test,
    const TempDirectory& temporary) {
    const std::string actual_parent =
        temporary.Child("actual-parent");
    const std::string linked_parent =
        temporary.Child("linked-parent");
    if (::mkdir(actual_parent.c_str(), 0700) != 0) {
        ThrowErrno("mkdir actual metrics parent");
    }
    if (::symlink(
            "actual-parent", linked_parent.c_str()) != 0) {
        ThrowErrno("create metrics parent symlink");
    }

    const std::string target =
        linked_parent + "/symlink-secret.prom";
    constexpr std::string_view metrics =
        "l2flow_parent_symlink 1\n";
    std::string error = "stale error";
    test->Expect(
        !ops::PublishPrometheusTextfile(
            target, metrics, &error),
        "symbolic-link parent directory is rejected");
    test->Expect(
        !error.empty() &&
            error.find(target) == std::string::npos &&
            error.find(metrics) == std::string::npos,
        "parent-symlink error discloses no path or metrics");

    struct stat unused {};
    errno = 0;
    test->Expect(
        ::lstat(
            (actual_parent + "/symlink-secret.prom").c_str(),
            &unused) != 0 &&
            errno == ENOENT,
        "parent-symlink rejection creates no target through the link");
    test->Expect(
        CountTemporaryFiles(actual_parent) == 0U &&
            CountTemporaryFiles(temporary.path()) == 0U,
        "parent-symlink rejection leaves no temporary file");
}

void CheckTargetSymlinkRejection(
    TestContext* test,
    const TempDirectory& temporary) {
    const std::string actual_target =
        temporary.Child("actual-symlink-target.prom");
    const std::string linked_target =
        temporary.Child("linked-target.prom");
    constexpr std::string_view original =
        "l2flow_target_symlink 0\n";
    std::string error;
    if (!ops::PublishPrometheusTextfile(
            actual_target, original, &error)) {
        throw std::runtime_error(
            "create target-symlink fixture failed");
    }
    const struct stat actual_metadata =
        StatFile(actual_target);
    if (::symlink(
            "actual-symlink-target.prom",
            linked_target.c_str()) != 0) {
        ThrowErrno("create metrics target symlink");
    }
    const struct stat link_metadata =
        StatFile(linked_target);

    constexpr std::string_view replacement =
        "l2flow_target_symlink 1\n";
    error = "stale error";
    test->Expect(
        !ops::PublishPrometheusTextfile(
            linked_target, replacement, &error),
        "symbolic-link metrics target is rejected");
    test->Expect(
        !error.empty() &&
            error.find(linked_target) ==
                std::string::npos &&
            error.find(replacement) ==
                std::string::npos,
        "target-symlink error discloses no path or metrics");
    ExpectTargetUnchanged(
        test,
        actual_target,
        original,
        actual_metadata,
        "symbolic-link metrics target");
    const struct stat link_after = StatFile(linked_target);
    test->Expect(
        S_ISLNK(link_after.st_mode) &&
            link_after.st_dev == link_metadata.st_dev &&
            link_after.st_ino == link_metadata.st_ino,
        "target-symlink rejection preserves the link inode");
    test->Expect(
        CountTemporaryFiles(temporary.path()) == 0U,
        "target-symlink rejection leaves no temporary file");
}

void CheckLockedTargetRejection(
    TestContext* test,
    const TempDirectory& temporary,
    const std::string& target,
    std::string_view expected_contents,
    const struct stat& expected_metadata) {
    ScopedFd locked = OpenReadOnly(target);
    int lock_result = -1;
    do {
        lock_result =
            ::flock(locked.get(), LOCK_EX | LOCK_NB);
    } while (lock_result != 0 && errno == EINTR);
    if (lock_result != 0) {
        ThrowErrno("lock metrics target fixture");
    }

    ExpectRejectedContents(
        test,
        temporary,
        target,
        "l2flow_locked_target 1\n",
        expected_contents,
        expected_metadata,
        "locked metrics target");
}

void CheckHardLinkedTargetRejection(
    TestContext* test,
    const TempDirectory& temporary,
    const std::string& target,
    std::string_view expected_contents) {
    const std::string alias =
        temporary.Child("l2flow.prom.hardlink");
    if (::link(target.c_str(), alias.c_str()) != 0) {
        ThrowErrno("create metrics hardlink fixture");
    }
    ScopedUnlink alias_cleanup(alias);
    const struct stat linked_metadata = StatFile(target);
    test->Expect(
        linked_metadata.st_nlink == 2,
        "hardlink fixture has exactly two names");

    ExpectRejectedContents(
        test,
        temporary,
        target,
        "l2flow_hardlinked_target 1\n",
        expected_contents,
        linked_metadata,
        "hard-linked metrics target");
}

void CheckWideTargetModeRejection(
    TestContext* test,
    const TempDirectory& temporary,
    const std::string& target,
    std::string_view expected_contents) {
    if (::chmod(target.c_str(), 0640) != 0) {
        ThrowErrno("widen metrics target mode");
    }
    const struct stat wide_metadata = StatFile(target);
    ExpectRejectedContents(
        test,
        temporary,
        target,
        "l2flow_wide_target_mode 1\n",
        expected_contents,
        wide_metadata,
        "non-0600 metrics target");
    if (::chmod(target.c_str(), 0600) != 0) {
        ThrowErrno("restore metrics target mode");
    }
}

void CheckPostCreationFailureCleanup(
    TestContext* test,
    const TempDirectory& temporary) {
    const std::string target_directory =
        temporary.Child("existing-target");
    if (::mkdir(target_directory.c_str(), 0700) != 0) {
        ThrowErrno("mkdir existing target");
    }
    const std::string sentinel =
        target_directory + "/sentinel";
    const int sentinel_fd =
        ::open(
            sentinel.c_str(),
            O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC,
            0600);
    if (sentinel_fd < 0) {
        ThrowErrno("create sentinel");
    }
    if (::close(sentinel_fd) != 0) {
        ThrowErrno("close sentinel");
    }

    constexpr std::string_view sensitive_metrics =
        "l2flow_sensitive_metric{secret=\"do-not-log\"} 1\n";
    std::string error = "stale error";
    test->Expect(
        !ops::PublishPrometheusTextfile(
            target_directory, sensitive_metrics, &error),
        "rename over an existing directory is rejected");
    test->Expect(
        !error.empty(),
        "post-creation rename failure reports an error");
    test->Expect(
        error.find("l2flow_sensitive_metric") ==
                std::string::npos &&
            error.find("do-not-log") == std::string::npos,
        "post-creation error does not disclose metric contents");
    const struct stat directory_metadata =
        StatFile(target_directory);
    test->Expect(
        S_ISDIR(directory_metadata.st_mode),
        "post-creation failure preserves the existing target directory");
    test->Expect(
        ReadFile(sentinel).empty(),
        "post-creation failure preserves the target directory contents");
    test->Expect(
        CountTemporaryFiles(
            temporary.path()) == 0U,
        "post-creation failure removes its temporary file");
}

void CheckClosedShadowIsNeverRetyped(
    TestContext* test,
    const TempDirectory& temporary) {
    const std::string target =
        temporary.Child("closed-shadow.capture");
    const std::string shadow_bytes(
        l2flow::ingress::kShadowCaptureFileMagic.begin(),
        l2flow::ingress::kShadowCaptureFileMagic.end());
    int fd = -1;
    do {
        fd = ::open(
            target.c_str(),
            O_WRONLY | O_CREAT | O_EXCL |
                O_CLOEXEC | O_NOFOLLOW,
            0600);
    } while (fd < 0 && errno == EINTR);
    if (fd < 0) {
        ThrowErrno("create closed shadow fixture");
    }
    const ssize_t written =
        ::write(
            fd,
            shadow_bytes.data(),
            shadow_bytes.size());
    const int write_error =
        written == static_cast<ssize_t>(
                       shadow_bytes.size())
            ? 0
            : (written < 0 ? errno : EIO);
    const int close_result = ::close(fd);
    const int close_error =
        close_result == 0 ? 0 : errno;
    if (write_error != 0 ||
        close_error != 0) {
        errno =
            write_error != 0
                ? write_error
                : close_error;
        ThrowErrno("write closed shadow fixture");
    }
    const struct stat original_metadata =
        StatFile(target);

    std::string error;
    test->Expect(
        !ops::PublishPrometheusTextfile(
            target,
            "l2flow_must_not_replace_shadow 1\n",
            &error),
        "a closed shadow capture cannot be retyped as metrics");
    test->Expect(
        !error.empty(),
        "closed-shadow type rejection reports an error");
    ExpectTargetUnchanged(
        test,
        target,
        shadow_bytes,
        original_metadata,
        "closed-shadow type rejection");
    test->Expect(
        CountTemporaryFiles(temporary.path()) == 0U,
        "closed-shadow type rejection removes its temporary file");
}

void CheckSdkLogDirectoryIsReserved(
    TestContext* test,
    const TempDirectory& temporary) {
    const std::string directory =
        temporary.Child("reserved-sdk-logs");
    if (::mkdir(directory.c_str(), 0700) != 0) {
        ThrowErrno("create reserved SDK log directory");
    }
    const std::string marker =
        directory + "/" +
        std::string(
            ops::kSdkLogDirectoryMarkerFilename);
    WriteExclusiveFixture(
        marker,
        ops::kSdkLogDirectoryMarkerContent,
        0444);

    const std::string target =
        directory + "/must-not-be-metrics.prom";
    std::string error;
    test->Expect(
        !ops::PublishPrometheusTextfile(
            target,
            "l2flow_must_not_enter_sdk_log_directory 1\n",
            &error),
        "a marked SDK log directory rejects metrics publication");
    test->Expect(
        !error.empty() &&
            error.find(target) == std::string::npos,
        "SDK log directory rejection is reported without path disclosure");
    struct stat target_metadata {};
    test->Expect(
        ::lstat(target.c_str(), &target_metadata) != 0 &&
            errno == ENOENT,
        "SDK log directory rejection creates no metrics target");
    test->Expect(
        CountTemporaryFiles(directory) == 0U,
        "SDK log directory rejection creates no metrics temporary file");
}

}  // namespace

int main() {
    static_assert(
        noexcept(ops::PublishPrometheusTextfile(
            std::declval<const std::string&>(),
            std::declval<std::string_view>(),
            nullptr)),
        "metrics publication must not propagate exceptions");

    TestContext test;
    try {
        TempDirectory temporary;
        std::string target;
        std::string current_contents;
        struct stat current_metadata {};
        CheckAtomicPublication(
            &test,
            temporary,
            &target,
            &current_contents,
            &current_metadata);
        CheckRejectedContents(
            &test,
            temporary,
            target,
            current_contents,
            current_metadata);
        CheckRejectedPaths(
            &test,
            temporary,
            target,
            current_contents,
            current_metadata);
        CheckAmbiguousPathRejection(
            &test,
            temporary,
            target,
            current_contents,
            current_metadata);
        CheckLockedTargetRejection(
            &test,
            temporary,
            target,
            current_contents,
            current_metadata);
        CheckHardLinkedTargetRejection(
            &test,
            temporary,
            target,
            current_contents);
        CheckWideTargetModeRejection(
            &test,
            temporary,
            target,
            current_contents);
        CheckUntrustedDirectoryRejection(
            &test, temporary);
        CheckParentSymlinkRejection(
            &test, temporary);
        CheckTargetSymlinkRejection(
            &test, temporary);
        CheckPostCreationFailureCleanup(
            &test, temporary);
        CheckClosedShadowIsNeverRetyped(
            &test, temporary);
        CheckSdkLogDirectoryIsReserved(
            &test, temporary);
    } catch (const std::exception& error) {
        std::cerr
            << "FAIL: metrics textfile fixture raised: "
            << error.what() << '\n';
        ++test.failures;
    } catch (...) {
        std::cerr
            << "FAIL: metrics textfile fixture raised "
               "a non-standard exception\n";
        ++test.failures;
    }

    if (test.failures != 0) {
        std::cerr
            << "phase1 metrics textfile test failed with "
            << test.failures << " error(s)\n";
        return 1;
    }
    std::cout
        << "phase1 metrics textfile dirfd traversal, locking, "
           "atomicity, permissions, and cleanup checks passed\n";
    return 0;
}
