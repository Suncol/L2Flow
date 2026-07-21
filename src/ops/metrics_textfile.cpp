#include "l2flow/ops/metrics_textfile.h"

#include "l2flow/common/sha256.h"
#include "l2flow/ops/stable_output_prefix.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <new>
#include <string>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <unistd.h>

namespace l2flow::ops {
namespace {

constexpr std::size_t kMaximumTemporaryCreateAttempts = 64U;
constexpr std::size_t kMaximumTargetRaceAttempts = 8U;
constexpr unsigned int kRenameNoReplace = 1U;
constexpr std::string_view kMetricsLeaseMagic =
    "l2flow-metrics-lease-v1\n";
std::atomic<std::uint64_t> g_temporary_sequence{0U};

void SetError(
    std::string* error,
    const char* operation,
    int error_number) noexcept {
    if (error == nullptr) {
        return;
    }
    try {
        *error = std::string(operation) + " failed: " +
                 std::strerror(error_number);
    } catch (...) {
        try {
            *error = "metrics textfile operation failed";
        } catch (...) {
        }
    }
}

void SetErrorLiteral(
    std::string* error,
    const char* message) noexcept {
    if (error == nullptr) {
        return;
    }
    try {
        *error = message;
    } catch (...) {
    }
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

    ScopedFd(ScopedFd&& other) noexcept
        : fd_(other.Release()) {}

    ScopedFd& operator=(ScopedFd&& other) noexcept {
        if (this != &other) {
            Reset(other.Release());
        }
        return *this;
    }

    [[nodiscard]] int get() const noexcept {
        return fd_;
    }

    [[nodiscard]] int Release() noexcept {
        const int released = fd_;
        fd_ = -1;
        return released;
    }

    void Reset(int fd = -1) noexcept {
        if (fd_ >= 0) {
            static_cast<void>(::close(fd_));
        }
        fd_ = fd;
    }

private:
    int fd_;
};

class TemporaryFile final {
public:
    TemporaryFile(
        int directory_fd,
        int fd,
        std::string name)
        : directory_fd_(directory_fd),
          fd_(fd),
          name_(std::move(name)) {}

    ~TemporaryFile() {
        if (fd_ >= 0) {
            static_cast<void>(::close(fd_));
        }
        if (!name_.empty()) {
            static_cast<void>(
                ::unlinkat(
                    directory_fd_, name_.c_str(), 0));
        }
    }

    TemporaryFile(const TemporaryFile&) = delete;
    TemporaryFile& operator=(const TemporaryFile&) = delete;

    [[nodiscard]] int fd() const noexcept {
        return fd_;
    }

    [[nodiscard]] const std::string& name() const noexcept {
        return name_;
    }

    void Commit() noexcept {
        name_.clear();
    }

    [[nodiscard]] int ReleaseAndCommit() noexcept {
        name_.clear();
        return std::exchange(fd_, -1);
    }

private:
    int directory_fd_;
    int fd_;
    std::string name_;
};

[[nodiscard]] int OpenRootDirectory() noexcept {
    int fd = -1;
    do {
        fd = ::open(
            "/",
            O_RDONLY | O_DIRECTORY | O_CLOEXEC |
                O_NOFOLLOW | O_NOCTTY);
    } while (fd < 0 && errno == EINTR);
    return fd;
}

[[nodiscard]] int OpenDirectoryAt(
    int directory_fd,
    const char* component) noexcept {
    int fd = -1;
    do {
        fd = ::openat(
            directory_fd,
            component,
            O_RDONLY | O_DIRECTORY | O_CLOEXEC |
                O_NOFOLLOW | O_NOCTTY);
    } while (fd < 0 && errno == EINTR);
    return fd;
}

[[nodiscard]] int OpenTargetAt(
    int directory_fd,
    const char* basename) noexcept {
    int fd = -1;
    do {
        fd = ::openat(
            directory_fd,
            basename,
            O_RDONLY | O_CLOEXEC | O_NOFOLLOW |
                O_NOCTTY | O_NONBLOCK);
    } while (fd < 0 && errno == EINTR);
    return fd;
}

[[nodiscard]] bool SplitAbsolutePath(
    const std::string& path,
    std::vector<std::string>* parent_components,
    std::string* basename,
    std::string* error) {
    if (path.empty() ||
        path.front() != '/' ||
        path.find('\0') != std::string::npos) {
        SetErrorLiteral(
            error,
            "metrics textfile path must be absolute and contain no NUL");
        return false;
    }

    std::vector<std::string> components;
    std::size_t start = 1U;
    while (start < path.size()) {
        const std::size_t separator = path.find('/', start);
        const std::size_t end =
            separator == std::string::npos
                ? path.size()
                : separator;
        const std::string component =
            path.substr(start, end - start);
        if (component.empty() ||
            component == "." ||
            component == "..") {
            SetErrorLiteral(
                error,
                "metrics textfile path contains an ambiguous component");
            return false;
        }
        components.push_back(component);
        if (separator == std::string::npos) {
            break;
        }
        start = separator + 1U;
        if (start == path.size()) {
            SetErrorLiteral(
                error,
                "metrics textfile path contains an ambiguous component");
            return false;
        }
    }

    if (components.empty()) {
        SetErrorLiteral(
            error,
            "metrics textfile path must name a file");
        return false;
    }
    if (IsSdkLogDirectoryMarkerFilename(
            components.back()) ||
        IsPrometheusTextfileLeaseFilename(
            components.back())) {
        SetErrorLiteral(
            error,
            "metrics textfile basename is reserved");
        return false;
    }
    *basename = std::move(components.back());
    components.pop_back();
    *parent_components = std::move(components);
    return true;
}

[[nodiscard]] bool IsSafeTarget(
    const struct stat& metadata) noexcept {
    return S_ISREG(metadata.st_mode) &&
           metadata.st_uid == ::geteuid() &&
           metadata.st_nlink == 1 &&
           (metadata.st_mode & 07777) == 0600;
}

[[nodiscard]] bool ValidateDestinationDirectory(
    int directory_fd,
    std::string* error) noexcept {
    struct stat metadata {};
    if (::fstat(directory_fd, &metadata) != 0) {
        SetError(
            error,
            "inspect metrics destination directory",
            errno);
        return false;
    }
    if (!S_ISDIR(metadata.st_mode) ||
        metadata.st_uid != ::geteuid() ||
        (metadata.st_mode &
         (S_IWGRP | S_IWOTH)) != 0) {
        SetErrorLiteral(
            error,
            "metrics destination directory is not private and owner-controlled");
        return false;
    }
    return true;
}

[[nodiscard]] bool RejectSdkLogDirectory(
    int directory_fd,
    std::string* error) noexcept {
    struct stat marker {};
    if (::fstatat(
            directory_fd,
            kSdkLogDirectoryMarkerFilename.data(),
            &marker,
            AT_SYMLINK_NOFOLLOW) == 0) {
        SetErrorLiteral(
            error,
            "metrics destination is reserved for SDK logs");
        return false;
    }
    if (errno != ENOENT) {
        SetError(
            error,
            "inspect SDK log directory reservation",
            errno);
        return false;
    }
    return true;
}

[[nodiscard]] bool IsTrustedDirectoryOwner(
    uid_t owner,
    uid_t namespace_root_owner) noexcept {
    return owner == ::geteuid() ||
           owner == 0 ||
           owner == namespace_root_owner;
}

[[nodiscard]] bool ValidateAncestorTransition(
    int parent_fd,
    int child_fd,
    uid_t namespace_root_owner,
    std::string* error) noexcept {
    struct stat parent {};
    struct stat child {};
    if (::fstat(parent_fd, &parent) != 0 ||
        ::fstat(child_fd, &child) != 0) {
        SetError(
            error,
            "inspect metrics ancestor directories",
            errno);
        return false;
    }
    if (!S_ISDIR(parent.st_mode) ||
        !S_ISDIR(child.st_mode) ||
        !IsTrustedDirectoryOwner(
            parent.st_uid,
            namespace_root_owner)) {
        SetErrorLiteral(
            error,
            "metrics path has an untrusted ancestor directory");
        return false;
    }
    const bool writable_by_other =
        (parent.st_mode &
         (S_IWGRP | S_IWOTH)) != 0;
    const bool sticky =
        (parent.st_mode & S_ISVTX) != 0;
    if (writable_by_other &&
        (!sticky ||
         !IsTrustedDirectoryOwner(
             child.st_uid,
             namespace_root_owner))) {
        SetErrorLiteral(
            error,
            "metrics path ancestor permits an unsafe rename");
        return false;
    }
    return true;
}

[[nodiscard]] bool SameInode(
    const struct stat& left,
    const struct stat& right) noexcept {
    return left.st_dev == right.st_dev &&
           left.st_ino == right.st_ino;
}

[[nodiscard]] bool TemporaryNameStillMatches(
    int directory_fd,
    const TemporaryFile& temporary,
    std::string* error) noexcept {
    struct stat opened {};
    struct stat named {};
    if (::fstat(temporary.fd(), &opened) != 0 ||
        ::fstatat(
            directory_fd,
            temporary.name().c_str(),
            &named,
            AT_SYMLINK_NOFOLLOW) != 0) {
        SetError(
            error,
            "reinspect metrics temporary file",
            errno);
        return false;
    }
    if (!IsSafeTarget(opened) ||
        !IsSafeTarget(named) ||
        !SameInode(opened, named)) {
        SetErrorLiteral(
            error,
            "metrics temporary file identity changed");
        return false;
    }
    return true;
}

[[nodiscard]] bool HasMetricsTextfileMagic(
    int fd,
    std::string* error) noexcept {
    std::array<char, kPrometheusTextfileMagic.size()>
        observed{};
    std::size_t offset = 0U;
    while (offset < observed.size()) {
        const ssize_t count =
            ::pread(
                fd,
                observed.data() + offset,
                observed.size() - offset,
                static_cast<off_t>(offset));
        if (count < 0 && errno == EINTR) {
            continue;
        }
        if (count < 0) {
            SetError(
                error,
                "read existing metrics type marker",
                errno);
            return false;
        }
        if (count == 0) {
            SetErrorLiteral(
                error,
                "existing target is not an L2Flow metrics textfile");
            return false;
        }
        offset += static_cast<std::size_t>(count);
    }
    if (!std::equal(
            observed.begin(),
            observed.end(),
            kPrometheusTextfileMagic.begin())) {
        SetErrorLiteral(
            error,
            "existing target is not an L2Flow metrics textfile");
        return false;
    }
    return true;
}

[[nodiscard]] int LockExclusiveNonBlocking(int fd) noexcept {
    int result = -1;
    do {
        result = ::flock(fd, LOCK_EX | LOCK_NB);
    } while (result != 0 && errno == EINTR);
    return result;
}

[[nodiscard]] int RenameNoReplace(
    int old_directory_fd,
    const char* old_name,
    int new_directory_fd,
    const char* new_name) noexcept {
#if defined(SYS_renameat2)
    long result = -1L;
    do {
        result = ::syscall(
            SYS_renameat2,
            old_directory_fd,
            old_name,
            new_directory_fd,
            new_name,
            kRenameNoReplace);
    } while (result != 0L && errno == EINTR);
    return result == 0L ? 0 : -1;
#else
    static_cast<void>(old_directory_fd);
    static_cast<void>(old_name);
    static_cast<void>(new_directory_fd);
    static_cast<void>(new_name);
    errno = ENOSYS;
    return -1;
#endif
}

[[nodiscard]] bool WriteAll(
    int fd,
    std::string_view contents,
    std::string* error) noexcept {
    std::size_t offset = 0U;
    while (offset < contents.size()) {
        const std::size_t remaining =
            contents.size() - offset;
        const std::size_t requested =
            std::min(
                remaining,
                static_cast<std::size_t>(
                    std::numeric_limits<ssize_t>::max()));
        const ssize_t count =
            ::write(fd, contents.data() + offset, requested);
        if (count < 0 && errno == EINTR) {
            continue;
        }
        if (count < 0) {
            SetError(error, "write metrics temporary file", errno);
            return false;
        }
        if (count == 0) {
            SetErrorLiteral(
                error,
                "write metrics temporary file made no progress");
            return false;
        }
        offset += static_cast<std::size_t>(count);
    }
    return true;
}

[[nodiscard]] bool HasExactLeaseMagic(
    int fd,
    std::string* error) noexcept {
    struct stat metadata {};
    if (::fstat(fd, &metadata) != 0 ||
        metadata.st_size < 0 ||
        static_cast<std::uintmax_t>(
            metadata.st_size) !=
            static_cast<std::uintmax_t>(
                kMetricsLeaseMagic.size())) {
        SetErrorLiteral(
            error,
            "metrics ownership sidecar has invalid content");
        return false;
    }
    std::array<char, kMetricsLeaseMagic.size()>
        observed{};
    std::size_t offset = 0U;
    while (offset < observed.size()) {
        const ssize_t count =
            ::pread(
                fd,
                observed.data() + offset,
                observed.size() - offset,
                static_cast<off_t>(offset));
        if (count < 0 && errno == EINTR) {
            continue;
        }
        if (count <= 0) {
            SetErrorLiteral(
                error,
                "metrics ownership sidecar read failed");
            return false;
        }
        offset += static_cast<std::size_t>(count);
    }
    if (!std::equal(
            observed.begin(),
            observed.end(),
            kMetricsLeaseMagic.begin())) {
        SetErrorLiteral(
            error,
            "metrics ownership sidecar has invalid content");
        return false;
    }
    return true;
}

}  // namespace

PrometheusTextfileLease::PrometheusTextfileLease(
    int directory_fd,
    int lock_fd,
    std::string basename) noexcept
    : directory_fd_(directory_fd),
      lock_fd_(lock_fd),
      basename_(std::move(basename)) {}

PrometheusTextfileLease::~PrometheusTextfileLease() {
    if (lock_fd_ >= 0) {
        static_cast<void>(::close(lock_fd_));
    }
    if (directory_fd_ >= 0) {
        static_cast<void>(::close(directory_fd_));
    }
}

std::unique_ptr<PrometheusTextfileLease>
AcquirePrometheusTextfileLease(
    const std::string& path,
    std::string* error) noexcept {
    if (error != nullptr) {
        error->clear();
    }
    try {
        std::vector<std::string> parent_components;
        std::string basename;
        if (!SplitAbsolutePath(
                path,
                &parent_components,
                &basename,
                error)) {
            return nullptr;
        }

        std::string folded_basename = basename;
        for (char& character : folded_basename) {
            const unsigned char value =
                static_cast<unsigned char>(character);
            const bool portable =
                (value >= static_cast<unsigned char>('a') &&
                 value <= static_cast<unsigned char>('z')) ||
                (value >= static_cast<unsigned char>('A') &&
                 value <= static_cast<unsigned char>('Z')) ||
                (value >= static_cast<unsigned char>('0') &&
                 value <= static_cast<unsigned char>('9')) ||
                value == static_cast<unsigned char>('.') ||
                value == static_cast<unsigned char>('_') ||
                value == static_cast<unsigned char>('-');
            if (!portable) {
                SetErrorLiteral(
                    error,
                    "metrics lease basename is not portable ASCII");
                return nullptr;
            }
            if (value >=
                    static_cast<unsigned char>('A') &&
                value <=
                    static_cast<unsigned char>('Z')) {
                character = static_cast<char>(
                    value -
                    static_cast<unsigned char>('A') +
                    static_cast<unsigned char>('a'));
            }
        }

        ScopedFd directory(OpenRootDirectory());
        if (directory.get() < 0) {
            SetError(
                error,
                "open metrics lease root directory",
                errno);
            return nullptr;
        }
        struct stat namespace_root {};
        if (::fstat(
                directory.get(),
                &namespace_root) != 0 ||
            !S_ISDIR(namespace_root.st_mode)) {
            SetErrorLiteral(
                error,
                "metrics lease root inspection failed");
            return nullptr;
        }
        for (const std::string& component :
             parent_components) {
            ScopedFd next(
                OpenDirectoryAt(
                    directory.get(),
                    component.c_str()));
            if (next.get() < 0) {
                SetError(
                    error,
                    "open metrics lease parent directory",
                    errno);
                return nullptr;
            }
            if (!ValidateAncestorTransition(
                    directory.get(),
                    next.get(),
                    namespace_root.st_uid,
                    error)) {
                return nullptr;
            }
            directory = std::move(next);
        }
        if (!ValidateDestinationDirectory(
                directory.get(),
                error) ||
            !RejectSdkLogDirectory(
                directory.get(),
                error)) {
            return nullptr;
        }

        const std::string lock_name =
            std::string(
                kPrometheusTextfileLeaseFilenamePrefix) +
            common::Sha256Hex(
                common::ComputeSha256(
                    folded_basename));
        int lock_fd = -1;
        bool created = false;
        do {
            lock_fd = ::openat(
                directory.get(),
                lock_name.c_str(),
                O_RDWR | O_CREAT | O_EXCL |
                    O_CLOEXEC | O_NOFOLLOW |
                    O_NONBLOCK | O_NOCTTY,
                S_IRUSR | S_IWUSR);
        } while (lock_fd < 0 && errno == EINTR);
        if (lock_fd >= 0) {
            created = true;
        } else if (errno == EEXIST) {
            do {
                lock_fd = ::openat(
                    directory.get(),
                    lock_name.c_str(),
                    O_RDWR | O_CLOEXEC |
                        O_NOFOLLOW | O_NONBLOCK |
                        O_NOCTTY);
            } while (lock_fd < 0 && errno == EINTR);
        }
        if (lock_fd < 0) {
            SetError(
                error,
                "open metrics ownership sidecar",
                errno);
            return nullptr;
        }
        TemporaryFile candidate(
            directory.get(),
            lock_fd,
            created ? lock_name : std::string{});
        if (created &&
            ::fchmod(
                candidate.fd(),
                S_IRUSR | S_IWUSR) != 0) {
            SetError(
                error,
                "set metrics ownership sidecar mode",
                errno);
            return nullptr;
        }
        struct stat opened {};
        if (::fstat(
                candidate.fd(),
                &opened) != 0 ||
            !IsSafeTarget(opened)) {
            SetErrorLiteral(
                error,
                "metrics ownership sidecar has unsafe metadata");
            return nullptr;
        }
        if (LockExclusiveNonBlocking(
                candidate.fd()) != 0) {
            SetErrorLiteral(
                error,
                "metrics textfile is already leased");
            return nullptr;
        }
        if (created) {
            if (!WriteAll(
                    candidate.fd(),
                    kMetricsLeaseMagic,
                    error)) {
                return nullptr;
            }
            int sync_result = -1;
            do {
                sync_result =
                    ::fdatasync(candidate.fd());
            } while (sync_result != 0 &&
                     errno == EINTR);
            if (sync_result != 0) {
                SetError(
                    error,
                    "sync metrics ownership sidecar",
                    errno);
                return nullptr;
            }
        } else if (!HasExactLeaseMagic(
                       candidate.fd(),
                       error)) {
            return nullptr;
        }

        struct stat locked {};
        struct stat named {};
        if (::fstat(
                candidate.fd(),
                &locked) != 0 ||
            ::fstatat(
                directory.get(),
                lock_name.c_str(),
                &named,
                AT_SYMLINK_NOFOLLOW) != 0 ||
            !IsSafeTarget(locked) ||
            !IsSafeTarget(named) ||
            !SameInode(locked, named)) {
            SetErrorLiteral(
                error,
                "metrics ownership sidecar identity changed");
            return nullptr;
        }

        const int retained_directory =
            directory.Release();
        const int retained_lock =
            candidate.ReleaseAndCommit();
        std::unique_ptr<PrometheusTextfileLease> lease(
            new (std::nothrow)
                PrometheusTextfileLease(
                    retained_directory,
                    retained_lock,
                    std::move(basename)));
        if (lease == nullptr) {
            static_cast<void>(
                ::close(retained_lock));
            static_cast<void>(
                ::close(retained_directory));
            SetErrorLiteral(
                error,
                "metrics ownership lease allocation failed");
        }
        return lease;
    } catch (...) {
        SetErrorLiteral(
            error,
            "metrics ownership lease operation failed");
        return nullptr;
    }
}

namespace {

bool PublishPrometheusTextfileImpl(
    const std::string* path,
    int retained_directory_fd,
    const std::string* retained_basename,
    std::string_view contents,
    std::string* error) noexcept {
    if (error != nullptr) {
        error->clear();
    }
    try {
        if (contents.empty() ||
            contents.size() >
                kMaximumPrometheusTextfileBodyBytes ||
            contents.find('\0') != std::string_view::npos) {
            SetErrorLiteral(
                error,
                "metrics body must be non-empty, NUL-free, and fit "
                "within a one MiB marked textfile");
            return false;
        }
        std::string marked_contents;
        marked_contents.reserve(
            kPrometheusTextfileMagic.size() +
            contents.size());
        marked_contents.append(
            kPrometheusTextfileMagic);
        marked_contents.append(contents);

        std::string basename;
        ScopedFd directory;
        if (retained_directory_fd >= 0) {
            if (retained_basename == nullptr ||
                retained_basename->empty()) {
                SetErrorLiteral(
                    error,
                    "retained metrics destination is invalid");
                return false;
            }
            int duplicate = -1;
            do {
                duplicate = ::fcntl(
                    retained_directory_fd,
                    F_DUPFD_CLOEXEC,
                    0);
            } while (duplicate < 0 && errno == EINTR);
            if (duplicate < 0) {
                SetError(
                    error,
                    "duplicate retained metrics directory",
                    errno);
                return false;
            }
            directory.Reset(duplicate);
            basename = *retained_basename;
        } else {
            if (path == nullptr) {
                SetErrorLiteral(
                    error,
                    "metrics textfile path is unavailable");
                return false;
            }
            std::vector<std::string> parent_components;
            if (!SplitAbsolutePath(
                    *path,
                    &parent_components,
                    &basename,
                    error)) {
                return false;
            }

            directory.Reset(OpenRootDirectory());
            if (directory.get() < 0) {
                SetError(error, "open root directory", errno);
                return false;
            }
            struct stat namespace_root {};
            if (::fstat(
                    directory.get(),
                    &namespace_root) != 0) {
                SetError(
                    error,
                    "inspect root directory",
                    errno);
                return false;
            }
            if (!S_ISDIR(namespace_root.st_mode)) {
                SetErrorLiteral(
                    error,
                    "root path is not a directory");
                return false;
            }
            for (const std::string& component :
                 parent_components) {
                ScopedFd next(
                    OpenDirectoryAt(
                        directory.get(),
                        component.c_str()));
                if (next.get() < 0) {
                    SetError(
                        error,
                        "open metrics parent directory",
                        errno);
                    return false;
                }
                if (!ValidateAncestorTransition(
                        directory.get(),
                        next.get(),
                        namespace_root.st_uid,
                        error)) {
                    return false;
                }
                directory = std::move(next);
            }
        }

        if (!ValidateDestinationDirectory(
                directory.get(), error)) {
            return false;
        }
        if (!RejectSdkLogDirectory(
                directory.get(), error)) {
            return false;
        }

        int temporary_fd = -1;
        std::string temporary_name;
        for (std::size_t attempt = 0U;
             attempt < kMaximumTemporaryCreateAttempts;
             ++attempt) {
            const std::uint64_t sequence =
                g_temporary_sequence.fetch_add(
                    1U, std::memory_order_relaxed);
            temporary_name =
                ".l2flow-metrics.tmp." +
                std::to_string(
                    static_cast<unsigned long long>(
                        ::getpid())) +
                "." +
                std::to_string(
                    static_cast<unsigned long long>(
                        sequence));
            do {
                temporary_fd = ::openat(
                    directory.get(),
                    temporary_name.c_str(),
                    O_WRONLY | O_CREAT | O_EXCL |
                        O_CLOEXEC | O_NOFOLLOW | O_NOCTTY,
                    S_IRUSR | S_IWUSR);
            } while (temporary_fd < 0 && errno == EINTR);
            if (temporary_fd >= 0) {
                break;
            }
            if (errno != EEXIST) {
                SetError(
                    error,
                    "create metrics temporary file",
                    errno);
                return false;
            }
        }
        if (temporary_fd < 0) {
            SetErrorLiteral(
                error,
                "could not allocate a unique metrics temporary file");
            return false;
        }

        TemporaryFile temporary(
            directory.get(),
            temporary_fd,
            std::move(temporary_name));
        if (::fchmod(
                temporary.fd(),
                S_IRUSR | S_IWUSR) != 0) {
            SetError(error, "chmod metrics temporary file", errno);
            return false;
        }
        struct stat temporary_metadata {};
        if (::fstat(
                temporary.fd(),
                &temporary_metadata) != 0) {
            SetError(error, "inspect metrics temporary file", errno);
            return false;
        }
        if (!IsSafeTarget(temporary_metadata)) {
            SetErrorLiteral(
                error,
                "metrics temporary file has unsafe metadata");
            return false;
        }
        if (!WriteAll(
                temporary.fd(),
                marked_contents,
                error)) {
            return false;
        }
        int sync_result = -1;
        do {
            sync_result =
                ::fdatasync(temporary.fd());
        } while (sync_result != 0 && errno == EINTR);
        if (sync_result != 0) {
            SetError(
                error,
                "sync metrics temporary file",
                errno);
            return false;
        }

        for (std::size_t attempt = 0U;
             attempt < kMaximumTargetRaceAttempts;
             ++attempt) {
            ScopedFd existing(
                OpenTargetAt(
                    directory.get(), basename.c_str()));
            if (existing.get() < 0) {
                const int open_error = errno;
                if (open_error != ENOENT) {
                    SetError(
                        error,
                        "open existing metrics target",
                        open_error);
                    return false;
                }

                if (!ValidateDestinationDirectory(
                        directory.get(), error)) {
                    return false;
                }
                if (!RejectSdkLogDirectory(
                        directory.get(), error)) {
                    return false;
                }
                if (!TemporaryNameStillMatches(
                        directory.get(),
                        temporary,
                        error)) {
                    return false;
                }
                if (RenameNoReplace(
                        directory.get(),
                        temporary.name().c_str(),
                        directory.get(),
                        basename.c_str()) == 0) {
                    temporary.Commit();
                    return true;
                }
                const int rename_error = errno;
                if (rename_error == EEXIST) {
                    continue;
                }
                SetError(
                    error,
                    "install new metrics target",
                    rename_error);
                return false;
            }

            struct stat existing_metadata {};
            if (::fstat(
                    existing.get(),
                    &existing_metadata) != 0) {
                SetError(
                    error,
                    "inspect existing metrics target",
                    errno);
                return false;
            }
            if (!IsSafeTarget(existing_metadata)) {
                SetErrorLiteral(
                    error,
                    "existing metrics target has unsafe metadata");
                return false;
            }
            if (LockExclusiveNonBlocking(existing.get()) != 0) {
                const int lock_error = errno;
                if (lock_error == EWOULDBLOCK ||
                    lock_error == EAGAIN) {
                    SetErrorLiteral(
                        error,
                        "existing metrics target is locked");
                } else {
                    SetError(
                        error,
                        "lock existing metrics target",
                        lock_error);
                }
                return false;
            }

            struct stat locked_metadata {};
            if (::fstat(
                    existing.get(),
                    &locked_metadata) != 0) {
                SetError(
                    error,
                    "reinspect locked metrics target",
                    errno);
                return false;
            }
            struct stat named_metadata {};
            if (::fstatat(
                    directory.get(),
                    basename.c_str(),
                    &named_metadata,
                    AT_SYMLINK_NOFOLLOW) != 0) {
                if (errno == ENOENT) {
                    continue;
                }
                SetError(
                    error,
                    "reinspect named metrics target",
                    errno);
                return false;
            }
            if (!IsSafeTarget(locked_metadata) ||
                !IsSafeTarget(named_metadata)) {
                SetErrorLiteral(
                    error,
                    "existing metrics target became unsafe");
                return false;
            }
            if (!SameInode(
                    locked_metadata, named_metadata)) {
                continue;
            }
            if (!HasMetricsTextfileMagic(
                    existing.get(), error)) {
                return false;
            }
            if (!ValidateDestinationDirectory(
                    directory.get(), error)) {
                return false;
            }
            if (!RejectSdkLogDirectory(
                    directory.get(), error)) {
                return false;
            }
            if (!TemporaryNameStillMatches(
                    directory.get(),
                    temporary,
                    error)) {
                return false;
            }

            if (::renameat(
                    directory.get(),
                    temporary.name().c_str(),
                    directory.get(),
                    basename.c_str()) != 0) {
                SetError(
                    error,
                    "replace metrics target",
                    errno);
                return false;
            }
            temporary.Commit();
            return true;
        }

        SetErrorLiteral(
            error,
            "metrics target changed repeatedly during publication");
        return false;
    } catch (...) {
        SetErrorLiteral(
            error,
            "metrics textfile publication raised an internal exception");
        return false;
    }
}

}  // namespace

bool PrometheusTextfileLease::Publish(
    std::string_view contents,
    std::string* error) noexcept {
    return PublishPrometheusTextfileImpl(
        nullptr,
        directory_fd_,
        &basename_,
        contents,
        error);
}

bool PublishPrometheusTextfile(
    const std::string& path,
    std::string_view contents,
    std::string* error) noexcept {
    return PublishPrometheusTextfileImpl(
        &path,
        -1,
        nullptr,
        contents,
        error);
}

}  // namespace l2flow::ops
