#include "l2flow/ops/stable_output_prefix.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <new>
#include <string>
#include <utility>

#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace l2flow::ops {
namespace {

void SetError(
    std::string* error,
    const char* classification) noexcept {
    if (error == nullptr) {
        return;
    }
    try {
        *error = classification;
    } catch (...) {
    }
}

void ClearError(std::string* error) noexcept {
    if (error == nullptr) {
        return;
    }
    try {
        error->clear();
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

[[nodiscard]] int OpenRootDirectory() noexcept {
    constexpr int kDirectoryFlags =
        O_RDONLY | O_DIRECTORY | O_CLOEXEC |
        O_NOFOLLOW | O_NOCTTY;
    int fd = -1;
    do {
        fd = ::open("/", kDirectoryFlags);
    } while (fd < 0 && errno == EINTR);
    return fd;
}

[[nodiscard]] int OpenDirectoryAt(
    int parent_fd,
    const char* component) noexcept {
    constexpr int kDirectoryFlags =
        O_RDONLY | O_DIRECTORY | O_CLOEXEC |
        O_NOFOLLOW | O_NOCTTY;
    int fd = -1;
    do {
        fd = ::openat(
            parent_fd,
            component,
            kDirectoryFlags);
    } while (fd < 0 && errno == EINTR);
    return fd;
}

[[nodiscard]] bool IsTrustedOwner(
    uid_t owner,
    uid_t namespace_root_owner) noexcept {
    return owner == ::geteuid() ||
           owner == static_cast<uid_t>(0) ||
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
            "stable output prefix ancestor inspection failed");
        return false;
    }
    if (!S_ISDIR(parent.st_mode) ||
        !S_ISDIR(child.st_mode) ||
        !IsTrustedOwner(
            parent.st_uid,
            namespace_root_owner)) {
        SetError(
            error,
            "stable output prefix has an untrusted ancestor");
        return false;
    }

    const bool writable_by_group_or_world =
        (parent.st_mode &
         (S_IWGRP | S_IWOTH)) != 0;
    const bool sticky =
        (parent.st_mode & S_ISVTX) != 0;
    if (writable_by_group_or_world &&
        (!sticky ||
         !IsTrustedOwner(
             child.st_uid,
             namespace_root_owner))) {
        SetError(
            error,
            "stable output prefix ancestor permits unsafe replacement");
        return false;
    }
    return true;
}

[[nodiscard]] bool ValidateFinalParent(
    int directory_fd,
    std::string* error) noexcept {
    struct stat metadata {};
    if (::fstat(directory_fd, &metadata) != 0) {
        SetError(
            error,
            "stable output prefix parent inspection failed");
        return false;
    }
    if (!S_ISDIR(metadata.st_mode) ||
        metadata.st_uid != ::geteuid() ||
        (metadata.st_mode &
         (S_IWGRP | S_IWOTH)) != 0) {
        SetError(
            error,
            "stable output prefix parent is not private and owner-controlled");
        return false;
    }
    return true;
}

[[nodiscard]] int OpenMarkerAt(
    int directory_fd) noexcept {
    int fd = -1;
    do {
        fd = ::openat(
            directory_fd,
            kSdkLogDirectoryMarkerFilename.data(),
            O_RDONLY | O_CLOEXEC | O_NOFOLLOW |
                O_NONBLOCK | O_NOCTTY);
    } while (fd < 0 && errno == EINTR);
    return fd;
}

[[nodiscard]] bool OpenValidateAndLockDirectoryMarker(
    int directory_fd,
    uid_t namespace_root_owner,
    ScopedFd* retained_marker,
    std::string* error) noexcept {
    const int marker_fd = OpenMarkerAt(directory_fd);
    if (marker_fd < 0) {
        SetError(
            error,
            "stable output prefix directory marker open failed");
        return false;
    }
    ScopedFd marker(marker_fd);

    struct stat metadata {};
    if (::fstat(marker.get(), &metadata) != 0) {
        SetError(
            error,
            "stable output prefix directory marker inspection failed");
        return false;
    }
    if (!S_ISREG(metadata.st_mode) ||
        metadata.st_nlink != 1 ||
        !IsTrustedOwner(
            metadata.st_uid,
            namespace_root_owner) ||
        (metadata.st_mode & 07777) != 0444 ||
        metadata.st_size < 0 ||
        static_cast<std::uintmax_t>(metadata.st_size) !=
            static_cast<std::uintmax_t>(
                kSdkLogDirectoryMarkerContent.size())) {
        SetError(
            error,
            "stable output prefix directory marker violates policy");
        return false;
    }

    int lock_result = -1;
    do {
        lock_result =
            ::flock(
                marker.get(),
                LOCK_EX | LOCK_NB);
    } while (lock_result != 0 && errno == EINTR);
    if (lock_result != 0) {
        SetError(
            error,
            "stable output prefix directory marker is already leased");
        return false;
    }

    std::array<
        char,
        kSdkLogDirectoryMarkerContent.size()> observed{};
    std::size_t offset = 0U;
    while (offset < observed.size()) {
        const ssize_t count =
            ::pread(
                marker.get(),
                observed.data() + offset,
                observed.size() - offset,
                static_cast<off_t>(offset));
        if (count < 0 && errno == EINTR) {
            continue;
        }
        if (count <= 0) {
            SetError(
                error,
                "stable output prefix directory marker read failed");
            return false;
        }
        offset += static_cast<std::size_t>(count);
    }
    char extra = '\0';
    ssize_t extra_count = -1;
    do {
        extra_count =
            ::pread(
                marker.get(),
                &extra,
                1U,
                static_cast<off_t>(observed.size()));
    } while (extra_count < 0 && errno == EINTR);
    if (extra_count != 0 ||
        !std::equal(
            observed.begin(),
            observed.end(),
            kSdkLogDirectoryMarkerContent.begin())) {
        SetError(
            error,
            "stable output prefix directory marker content differs");
        return false;
    }

    struct stat locked {};
    struct stat named {};
    if (::fstat(marker.get(), &locked) != 0 ||
        ::fstatat(
            directory_fd,
            kSdkLogDirectoryMarkerFilename.data(),
            &named,
            AT_SYMLINK_NOFOLLOW) != 0 ||
        locked.st_dev != named.st_dev ||
        locked.st_ino != named.st_ino ||
        !S_ISREG(locked.st_mode) ||
        locked.st_nlink != 1 ||
        !IsTrustedOwner(
            locked.st_uid,
            namespace_root_owner) ||
        (locked.st_mode & 07777) != 0444 ||
        locked.st_size != metadata.st_size) {
        SetError(
            error,
            "stable output prefix directory marker identity changed");
        return false;
    }
    *retained_marker = std::move(marker);
    return true;
}

[[nodiscard]] bool ValidatePathSyntax(
    const std::string& path,
    std::size_t* final_separator,
    std::string* error) noexcept {
    if (path.empty() ||
        path.front() != '/' ||
        path.find('\0') != std::string::npos) {
        SetError(
            error,
            "stable output prefix must be an absolute NUL-free path");
        return false;
    }
    if (path.size() == 1U ||
        path.back() == '/') {
        SetError(
            error,
            "stable output prefix must name a nonempty basename");
        return false;
    }

    std::size_t component_start = 1U;
    for (;;) {
        const std::size_t separator =
            path.find('/', component_start);
        const std::size_t component_end =
            separator == std::string::npos
                ? path.size()
                : separator;
        const std::size_t component_size =
            component_end - component_start;
        if (component_size == 0U ||
            (component_size == 1U &&
             path[component_start] == '.') ||
            (component_size == 2U &&
             path[component_start] == '.' &&
             path[component_start + 1U] == '.')) {
            SetError(
                error,
                "stable output prefix contains an ambiguous component");
            return false;
        }
        if (separator == std::string::npos) {
            *final_separator = path.rfind('/');
            return true;
        }
        component_start = separator + 1U;
    }
}

}  // namespace

StableOutputPrefix::StableOutputPrefix(
    int directory_fd,
    int marker_fd,
    std::string stable_prefix) noexcept
    : directory_fd_(directory_fd),
      marker_fd_(marker_fd),
      stable_prefix_(std::move(stable_prefix)) {}

StableOutputPrefix::~StableOutputPrefix() {
    if (marker_fd_ >= 0) {
        static_cast<void>(::close(marker_fd_));
    }
    if (directory_fd_ >= 0) {
        static_cast<void>(::close(directory_fd_));
    }
}

std::unique_ptr<StableOutputPrefix>
OpenStableOutputPrefix(
    const std::string& absolute_prefix,
    std::string* error) noexcept {
    ClearError(error);
    try {
        std::size_t final_separator = 0U;
        if (!ValidatePathSyntax(
                absolute_prefix,
                &final_separator,
                error)) {
            return nullptr;
        }

        const int root_fd = OpenRootDirectory();
        if (root_fd < 0) {
            SetError(
                error,
                "stable output prefix root open failed");
            return nullptr;
        }
        ScopedFd current(root_fd);

        struct stat namespace_root {};
        if (::fstat(current.get(), &namespace_root) != 0 ||
            !S_ISDIR(namespace_root.st_mode)) {
            SetError(
                error,
                "stable output prefix root inspection failed");
            return nullptr;
        }

        std::size_t component_start = 1U;
        while (component_start < final_separator) {
            const std::size_t separator =
                absolute_prefix.find('/', component_start);
            const std::string component =
                absolute_prefix.substr(
                    component_start,
                    separator - component_start);
            const int child_fd =
                OpenDirectoryAt(
                    current.get(),
                    component.c_str());
            if (child_fd < 0) {
                SetError(
                    error,
                    "stable output prefix parent open failed");
                return nullptr;
            }
            ScopedFd child(child_fd);
            if (!ValidateAncestorTransition(
                    current.get(),
                    child.get(),
                    namespace_root.st_uid,
                    error)) {
                return nullptr;
            }
            current = std::move(child);
            component_start = separator + 1U;
        }

        if (!ValidateFinalParent(current.get(), error)) {
            return nullptr;
        }
        ScopedFd marker;
        if (!OpenValidateAndLockDirectoryMarker(
                current.get(),
                namespace_root.st_uid,
                &marker,
                error)) {
            return nullptr;
        }

        const std::string basename =
            absolute_prefix.substr(final_separator + 1U);
        std::string stable =
            "/proc/self/fd/" +
            std::to_string(current.get()) +
            "/" + basename;
        const int leased_fd = current.Release();
        const int marker_fd = marker.Release();
        std::unique_ptr<StableOutputPrefix> lease(
            new (std::nothrow) StableOutputPrefix(
                leased_fd,
                marker_fd,
                std::move(stable)));
        if (lease == nullptr) {
            static_cast<void>(::close(marker_fd));
            static_cast<void>(::close(leased_fd));
            SetError(
                error,
                "stable output prefix allocation failed");
        }
        return lease;
    } catch (...) {
        SetError(
            error,
            "stable output prefix allocation failed");
        return nullptr;
    }
}

}  // namespace l2flow::ops
