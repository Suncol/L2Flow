#include "l2flow/ingress/raw_namespace.h"

#include "l2flow/common/identity128.h"
#include "l2flow/sdk/subscription_manifest.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <charconv>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <utility>

#include <dirent.h>
#include <fcntl.h>
#include <linux/fs.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>

namespace l2flow::ingress {

std::optional<std::string_view>
CanonicalRawStreamSlugV1(
    std::uint32_t source_stream_id) noexcept {
    try {
        const auto& specs = sdk::AllIngressSpecs();
        if (specs.size() != 4U) {
            return std::nullopt;
        }
        const sdk::IngressSpec* match = nullptr;
        for (const sdk::IngressSpec& spec : specs) {
            if (spec.source_stream_id !=
                source_stream_id) {
                continue;
            }
            if (match != nullptr) {
                return std::nullopt;
            }
            match = &spec;
        }
        if (match == nullptr) {
            return std::nullopt;
        }
        switch (match->kind) {
            case sdk::IngressKind::ShSnapshot:
                return "sh-snapshot";
            case sdk::IngressKind::ShTick:
                return "sh-tick";
            case sdk::IngressKind::SzSnapshot:
                return "sz-snapshot";
            case sdk::IngressKind::SzTick:
                return "sz-tick";
        }
    } catch (...) {
    }
    return std::nullopt;
}

bool IsCanonicalRawStreamRouteV1(
    std::uint32_t source_stream_id,
    std::string_view stream_slug) noexcept {
    const auto canonical =
        CanonicalRawStreamSlugV1(source_stream_id);
    return canonical.has_value() &&
           stream_slug == *canonical;
}

namespace {

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
        : fd_(std::exchange(other.fd_, -1)) {}
    ScopedFd& operator=(ScopedFd&& other) noexcept {
        if (this != &other) {
            Reset(std::exchange(other.fd_, -1));
        }
        return *this;
    }
    [[nodiscard]] int get() const noexcept {
        return fd_;
    }
    [[nodiscard]] int Release() noexcept {
        return std::exchange(fd_, -1);
    }
    void Reset(int fd = -1) noexcept {
        if (fd_ >= 0) {
            static_cast<void>(::close(fd_));
        }
        fd_ = fd;
    }

private:
    int fd_ = -1;
};

class ScopedDir final {
public:
    explicit ScopedDir(DIR* directory = nullptr) noexcept
        : directory_(directory) {}
    ~ScopedDir() {
        if (directory_ != nullptr) {
            static_cast<void>(::closedir(directory_));
        }
    }
    ScopedDir(const ScopedDir&) = delete;
    ScopedDir& operator=(const ScopedDir&) = delete;

    [[nodiscard]] DIR* get() const noexcept {
        return directory_;
    }

private:
    DIR* directory_ = nullptr;
};

void SetError(
    std::string* error,
    std::string message) noexcept {
    if (error == nullptr) {
        return;
    }
    try {
        *error = std::move(message);
    } catch (...) {
    }
}

bool FsyncLoop(int fd, bool data_only) noexcept {
    for (;;) {
        const int result =
            data_only ? ::fdatasync(fd) : ::fsync(fd);
        if (result == 0) {
            return true;
        }
        if (errno != EINTR) {
            return false;
        }
    }
}

bool WriteAll(
    int fd,
    std::span<const std::byte> bytes) noexcept {
    std::size_t offset = 0U;
    while (offset < bytes.size()) {
        const std::size_t request = std::min(
            bytes.size() - offset,
            static_cast<std::size_t>(
                std::numeric_limits<ssize_t>::max()));
        const ssize_t result = ::pwrite(
            fd,
            bytes.data() + offset,
            request,
            static_cast<off_t>(offset));
        if (result < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (result == 0) {
            errno = EIO;
            return false;
        }
        offset += static_cast<std::size_t>(result);
    }
    return true;
}

int OpenDirectoryAt(
    int parent_fd,
    const std::string& component) noexcept {
    for (;;) {
        const int fd = ::openat(
            parent_fd,
            component.c_str(),
            O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
        if (fd >= 0 || errno != EINTR) {
            return fd;
        }
    }
}

bool IsTrustedOwner(
    uid_t owner,
    uid_t namespace_root_owner) noexcept {
    return owner == 0 ||
           owner == ::geteuid() ||
           owner == namespace_root_owner;
}

bool ValidateAncestorTransition(
    int parent_fd,
    int child_fd,
    uid_t namespace_root_owner) noexcept {
    struct stat parent {};
    struct stat child {};
    if (::fstat(parent_fd, &parent) != 0 ||
        ::fstat(child_fd, &child) != 0 ||
        !S_ISDIR(parent.st_mode) ||
        !S_ISDIR(child.st_mode) ||
        !IsTrustedOwner(
            parent.st_uid,
            namespace_root_owner)) {
        return false;
    }
    const bool writable_by_other =
        (parent.st_mode & (S_IWGRP | S_IWOTH)) != 0;
    const bool sticky =
        (parent.st_mode & S_ISVTX) != 0;
    return !writable_by_other ||
           (sticky &&
            IsTrustedOwner(
                child.st_uid,
                namespace_root_owner));
}

bool ValidatePrivateOwnedDirectory(int fd) noexcept {
    struct stat status {};
    return ::fstat(fd, &status) == 0 &&
           S_ISDIR(status.st_mode) &&
           status.st_uid == ::geteuid() &&
           (status.st_mode & (S_IWGRP | S_IWOTH)) == 0;
}

bool ValidateAbsoluteRoot(
    std::string_view path) noexcept {
    if (path.size() < 2U ||
        path.front() != '/' ||
        path.back() == '/') {
        return false;
    }
    std::size_t start = 1U;
    while (start < path.size()) {
        const std::size_t end = path.find('/', start);
        const std::size_t component_end =
            end == std::string_view::npos
                ? path.size()
                : end;
        const std::string_view component =
            path.substr(start, component_end - start);
        if (component.empty() ||
            component == "." ||
            component == "..") {
            return false;
        }
        if (end == std::string_view::npos) {
            break;
        }
        start = end + 1U;
    }
    return true;
}

int OpenStableRawRoot(
    const std::string& path) noexcept {
    if (!ValidateAbsoluteRoot(path)) {
        errno = EINVAL;
        return -1;
    }
    ScopedFd current(
        ::open(
            "/",
            O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC));
    if (current.get() < 0) {
        return -1;
    }
    struct stat namespace_root {};
    if (::fstat(current.get(), &namespace_root) != 0 ||
        !S_ISDIR(namespace_root.st_mode)) {
        return -1;
    }
    std::size_t start = 1U;
    while (start < path.size()) {
        const std::size_t separator =
            path.find('/', start);
        const std::size_t end =
            separator == std::string::npos
                ? path.size()
                : separator;
        const std::string component =
            path.substr(start, end - start);
        ScopedFd child(
            OpenDirectoryAt(current.get(), component));
        if (child.get() < 0 ||
            !ValidateAncestorTransition(
                current.get(),
                child.get(),
                namespace_root.st_uid)) {
            errno = EACCES;
            return -1;
        }
        current = std::move(child);
        if (separator == std::string::npos) {
            break;
        }
        start = separator + 1U;
    }
    if (!ValidatePrivateOwnedDirectory(current.get())) {
        errno = EACCES;
        return -1;
    }
    return current.Release();
}

bool IsGregorianDate(std::uint32_t value) noexcept {
    if (value < 10000101U ||
        value > 99991231U) {
        return false;
    }
    const std::uint32_t year = value / 10000U;
    const std::uint32_t month =
        (value / 100U) % 100U;
    const std::uint32_t day = value % 100U;
    if (month == 0U || month > 12U) {
        return false;
    }
    constexpr std::array<std::uint32_t, 12U> days{{
        31U, 28U, 31U, 30U, 31U, 30U,
        31U, 31U, 30U, 31U, 30U, 31U}};
    std::uint32_t maximum = days[month - 1U];
    const bool leap =
        year % 4U == 0U &&
        (year % 100U != 0U || year % 400U == 0U);
    if (month == 2U && leap) {
        maximum = 29U;
    }
    return day != 0U && day <= maximum;
}

bool IsValidSlug(std::string_view slug) noexcept {
    if (slug.empty() ||
        slug.size() > 48U ||
        slug.front() == '-' ||
        slug.back() == '-') {
        return false;
    }
    bool previous_hyphen = false;
    for (const char character : slug) {
        const bool hyphen = character == '-';
        if ((!hyphen &&
             !(character >= 'a' && character <= 'z') &&
             !(character >= '0' && character <= '9')) ||
            (hyphen && previous_hyphen)) {
            return false;
        }
        previous_hyphen = hyphen;
    }
    return true;
}

bool EnsureChildDirectory(
    int parent_fd,
    const std::string& name,
    ScopedFd* child) noexcept {
    for (;;) {
        if (::mkdirat(parent_fd, name.c_str(), 0700) == 0 ||
            errno == EEXIST) {
            break;
        }
        if (errno != EINTR) {
            return false;
        }
    }
    child->Reset(OpenDirectoryAt(parent_fd, name));
    if (child->get() < 0 ||
        !ValidatePrivateOwnedDirectory(child->get()) ||
        !FsyncLoop(parent_fd, false)) {
        return false;
    }
    return true;
}

int CreateTypedTemporary(
    int directory_fd,
    const char* name) noexcept {
    for (;;) {
        const int fd = ::openat(
            directory_fd,
            name,
            O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW |
                O_NONBLOCK | O_CLOEXEC,
            0600);
        if (fd >= 0 || errno != EINTR) {
            return fd;
        }
    }
}

int RenameNoReplace(
    int directory_fd,
    const char* old_name,
    const char* new_name) noexcept {
    return static_cast<int>(
        ::syscall(
            SYS_renameat2,
            directory_fd,
            old_name,
            directory_fd,
            new_name,
            RENAME_NOREPLACE));
}

bool SameNamedInode(
    int directory_fd,
    const char* name,
    int fd) noexcept {
    struct stat named {};
    struct stat opened {};
    return ::fstatat(
               directory_fd,
               name,
               &named,
               AT_SYMLINK_NOFOLLOW) == 0 &&
           ::fstat(fd, &opened) == 0 &&
           named.st_dev == opened.st_dev &&
           named.st_ino == opened.st_ino;
}

bool SameOpenInode(
    int left_fd,
    int right_fd) noexcept {
    struct stat left {};
    struct stat right {};
    return left_fd >= 0 &&
           right_fd >= 0 &&
           ::fstat(left_fd, &left) == 0 &&
           ::fstat(right_fd, &right) == 0 &&
           left.st_dev == right.st_dev &&
           left.st_ino == right.st_ino;
}

bool ValidateMutableRawFile(
    int directory_fd,
    const char* name,
    int fd,
    off_t expected_size) noexcept {
    struct stat status {};
    const int flags = ::fcntl(fd, F_GETFL);
    return fd >= 0 &&
           ::fstat(fd, &status) == 0 &&
           S_ISREG(status.st_mode) &&
           status.st_uid == ::geteuid() &&
           (status.st_mode & 07777) == 0600 &&
           status.st_nlink == 1 &&
           status.st_size == expected_size &&
           flags >= 0 &&
           (flags & O_ACCMODE) == O_RDWR &&
           (flags & O_APPEND) == 0 &&
           SameNamedInode(directory_fd, name, fd);
}

bool PreadAll(
    int fd,
    std::uint64_t offset,
    std::span<std::byte> output) noexcept {
    std::size_t completed = 0U;
    while (completed < output.size()) {
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
            errno = EOVERFLOW;
            return false;
        }
        const std::size_t request = std::min(
            output.size() - completed,
            static_cast<std::size_t>(
                std::numeric_limits<ssize_t>::max()));
        const ssize_t result = ::pread(
            fd,
            output.data() + completed,
            request,
            static_cast<off_t>(
                offset +
                static_cast<std::uint64_t>(completed)));
        if (result < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (result == 0) {
            errno = EIO;
            return false;
        }
        completed += static_cast<std::size_t>(result);
    }
    return true;
}

std::string SegmentFilename(
    std::uint32_t sequence,
    bool temporary) {
    std::array<char, 9U> digits{};
    digits[8U] = '\0';
    std::uint32_t remaining = sequence;
    for (std::size_t index = 0U; index < 8U; ++index) {
        digits[7U - index] = static_cast<char>(
            '0' + (remaining % 10U));
        remaining /= 10U;
    }
    if (remaining != 0U || sequence == 0U) {
        return {};
    }
    const std::string final_name =
        "segment-" + std::string(digits.data(), 8U) +
        ".raw";
    return temporary
               ? "." + final_name + ".raw-segment.tmp"
               : final_name;
}

int OpenInventoryDirectory(int directory_fd) noexcept {
    for (;;) {
        const int fd = ::openat(
            directory_fd,
            ".",
            O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
                O_NOATIME | O_CLOEXEC);
        if (fd >= 0 || errno != EINTR) {
            return fd;
        }
    }
}

bool ValidateRetainedWriterLease(
    int directory_fd,
    const RawWriterLease& lease) noexcept {
    struct stat status {};
    const int flags =
        ::fcntl(lease.descriptor(), F_GETFL);
    std::array<std::byte, kRawWriterLeaseMarkerBytes>
        marker_wire{};
    RawWriterLeaseMarkerV1 marker;
    return lease.descriptor() >= 0 &&
           SameOpenInode(
               directory_fd,
               lease.directory_descriptor()) &&
           ::fstat(lease.descriptor(), &status) == 0 &&
           S_ISREG(status.st_mode) &&
           status.st_uid == ::geteuid() &&
           (status.st_mode & 07777) == 0600 &&
           status.st_nlink == 1 &&
           status.st_size ==
               static_cast<off_t>(
                   kRawWriterLeaseMarkerBytes) &&
           flags >= 0 &&
           (flags & O_ACCMODE) == O_RDWR &&
           (flags & O_APPEND) == 0 &&
           SameNamedInode(
               directory_fd,
               kRawWriterLeaseFilename,
               lease.descriptor()) &&
           PreadAll(
               lease.descriptor(),
               0U,
               marker_wire) &&
           DecodeRawWriterLeaseMarkerV1(
               marker_wire, &marker) &&
           marker.source_stream_id ==
               lease.source_stream_id() &&
           marker.capture_date ==
               lease.capture_date();
}

bool ValidateEmptyMaintenanceDirectory(
    int directory_fd) noexcept {
    ScopedFd maintenance_fd;
    for (;;) {
        maintenance_fd.Reset(::openat(
            directory_fd,
            "maintenance",
            O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
                O_NOATIME | O_CLOEXEC));
        if (maintenance_fd.get() >= 0 ||
            errno != EINTR) {
            break;
        }
    }
    struct stat status {};
    if (maintenance_fd.get() < 0 ||
        ::fstat(maintenance_fd.get(), &status) != 0 ||
        !S_ISDIR(status.st_mode) ||
        status.st_uid != ::geteuid() ||
        (status.st_mode & 07777) != 0700 ||
        !SameNamedInode(
            directory_fd,
            "maintenance",
            maintenance_fd.get())) {
        return false;
    }
    ScopedDir entries(
        ::fdopendir(maintenance_fd.Release()));
    if (entries.get() == nullptr) {
        return false;
    }
    errno = 0;
    for (;;) {
        dirent* entry = ::readdir(entries.get());
        if (entry == nullptr) {
            return errno == 0 &&
                   SameNamedInode(
                       directory_fd,
                       "maintenance",
                       ::dirfd(entries.get()));
        }
        if (std::strcmp(entry->d_name, ".") != 0 &&
            std::strcmp(entry->d_name, "..") != 0) {
            errno = ENOTEMPTY;
            return false;
        }
    }
}

bool InventoryBeforeMaintenanceScaffolding(
    int directory_fd) noexcept {
    ScopedFd inventory_fd(
        OpenInventoryDirectory(directory_fd));
    if (inventory_fd.get() < 0 ||
        !ValidatePrivateOwnedDirectory(
            inventory_fd.get()) ||
        !SameOpenInode(
            inventory_fd.get(), directory_fd)) {
        return false;
    }
    ScopedDir entries(
        ::fdopendir(inventory_fd.Release()));
    if (entries.get() == nullptr) {
        return false;
    }
    bool saw_maintenance = false;
    errno = 0;
    for (;;) {
        dirent* entry = ::readdir(entries.get());
        if (entry == nullptr) {
            return errno == 0 &&
                   (!saw_maintenance ||
                    ValidateEmptyMaintenanceDirectory(
                        directory_fd));
        }
        if (std::strcmp(entry->d_name, ".") == 0 ||
            std::strcmp(entry->d_name, "..") == 0) {
            continue;
        }
        if (!saw_maintenance &&
            std::strcmp(
                entry->d_name, "maintenance") == 0) {
            saw_maintenance = true;
            continue;
        }
        errno = ENOTEMPTY;
        return false;
    }
}

bool InventoryFreshNamespace(
    int directory_fd,
    const RawWriterLease& lease,
    int expected_journal_fd,
    std::string* error) noexcept {
    ScopedFd inventory_fd(
        OpenInventoryDirectory(directory_fd));
    if (inventory_fd.get() < 0 ||
        !ValidatePrivateOwnedDirectory(
            inventory_fd.get()) ||
        !SameOpenInode(
            inventory_fd.get(),
            directory_fd)) {
        SetError(
            error,
            "cannot securely enumerate the fresh Raw namespace with O_NOATIME");
        return false;
    }
    ScopedDir entries(
        ::fdopendir(inventory_fd.Release()));
    if (entries.get() == nullptr) {
        SetError(
            error,
            "cannot enumerate the fresh Raw namespace");
        return false;
    }

    bool saw_writer_lease = false;
    bool saw_maintenance = false;
    bool saw_journal = false;
    errno = 0;
    for (;;) {
        dirent* entry = ::readdir(entries.get());
        if (entry == nullptr) {
            if (errno != 0) {
                SetError(
                    error,
                    "fresh Raw namespace enumeration failed");
                return false;
            }
            break;
        }
        const char* const name = entry->d_name;
        if (std::strcmp(name, ".") == 0 ||
            std::strcmp(name, "..") == 0) {
            continue;
        }
        if (std::strcmp(
                name,
                kRawWriterLeaseFilename) == 0 &&
            !saw_writer_lease) {
            saw_writer_lease = true;
            continue;
        }
        if (std::strcmp(name, "maintenance") == 0 &&
            !saw_maintenance) {
            saw_maintenance = true;
            continue;
        }
        if (expected_journal_fd >= 0 &&
            std::strcmp(
                name,
                kRawJournalFilename) == 0 &&
            !saw_journal) {
            saw_journal = true;
            continue;
        }
        SetError(
            error,
            "fresh Raw namespace contains an unauthorized entry");
        return false;
    }
    if (!saw_writer_lease ||
        !ValidateRetainedWriterLease(
            directory_fd, lease) ||
        (saw_maintenance &&
         !ValidateEmptyMaintenanceDirectory(
             directory_fd)) ||
        (expected_journal_fd >= 0 &&
         (!saw_journal ||
          !SameNamedInode(
              directory_fd,
              kRawJournalFilename,
              expected_journal_fd))) ||
        (expected_journal_fd < 0 && saw_journal)) {
        SetError(
            error,
            "fresh Raw namespace inventory failed retained-object validation");
        return false;
    }
    return true;
}

void CleanupUnpublished(
    int directory_fd,
    const char* name) noexcept {
    if (::unlinkat(directory_fd, name, 0) == 0) {
        static_cast<void>(FsyncLoop(directory_fd, false));
    }
}

bool PublishHeaderFile(
    int directory_fd,
    const char* temporary_name,
    const char* final_name,
    std::span<const std::byte> header,
    std::uint64_t allocation_bytes,
    bool require_full_fsync,
    ScopedFd* retained,
    std::string* error) noexcept {
    if (header.size() !=
        kRawV1SegmentHeaderBytes) {
        errno = EINVAL;
        SetError(
            error,
            "Raw header publication size is invalid");
        return false;
    }
    retained->Reset(
        CreateTypedTemporary(
            directory_fd, temporary_name));
    if (retained->get() < 0) {
        SetError(
            error,
            std::string("cannot create Raw typed temporary: ") +
                std::strerror(errno));
        return false;
    }
    bool allocated = true;
    if (allocation_bytes != 0U) {
        if (allocation_bytes >
            static_cast<std::uint64_t>(
                std::numeric_limits<off_t>::max())) {
            errno = EOVERFLOW;
            allocated = false;
        } else {
            int allocation_error = 0;
            do {
                allocation_error =
                    ::posix_fallocate(
                        retained->get(),
                        0,
                        static_cast<off_t>(
                            allocation_bytes));
            } while (allocation_error == EINTR);
            if (allocation_error != 0) {
                errno = allocation_error;
                allocated = false;
            }
        }
    }
    if (!allocated ||
        !WriteAll(retained->get(), header) ||
        !FsyncLoop(
            retained->get(),
            !require_full_fsync) ||
        RenameNoReplace(
            directory_fd,
            temporary_name,
            final_name) != 0 ||
        !FsyncLoop(directory_fd, false) ||
        !SameNamedInode(
            directory_fd,
            final_name,
            retained->get())) {
        const int saved_error =
            errno == 0 ? EIO : errno;
        CleanupUnpublished(
            directory_fd, temporary_name);
        errno = saved_error;
        SetError(
            error,
            std::string("cannot publish Raw header file: ") +
                std::strerror(saved_error));
        return false;
    }
    RawV1SegmentHeaderWire readback{};
    const off_t expected_size =
        static_cast<off_t>(
            allocation_bytes == 0U
                ? header.size()
                : allocation_bytes);
    if (!PreadAll(
            retained->get(), 0U, readback) ||
        !std::equal(
            header.begin(),
            header.end(),
            readback.begin()) ||
        !ValidateMutableRawFile(
            directory_fd,
            final_name,
            retained->get(),
            expected_size)) {
        SetError(
            error,
            "published Raw header failed retained-fd readback");
        return false;
    }
    return true;
}

}  // namespace

RawStreamDirectory::RawStreamDirectory(
    int directory_fd,
    std::uint32_t source_stream_id,
    std::uint32_t capture_date) noexcept
    : directory_fd_(directory_fd),
      source_stream_id_(source_stream_id),
      capture_date_(capture_date) {}

RawStreamDirectory::~RawStreamDirectory() {
    if (directory_fd_ >= 0) {
        static_cast<void>(::close(directory_fd_));
    }
}

std::unique_ptr<RawStreamDirectory>
OpenOrCreateRawStreamDirectory(
    const std::string& raw_root,
    std::uint32_t source_stream_id,
    std::uint32_t capture_date,
    const std::string& stream_slug,
    std::string* error) noexcept {
    SetError(error, {});
    if (source_stream_id == 0U ||
        !IsGregorianDate(capture_date) ||
        !IsValidSlug(stream_slug)) {
        SetError(error, "Raw stream namespace is invalid");
        return nullptr;
    }
    ScopedFd root(OpenStableRawRoot(raw_root));
    if (root.get() < 0) {
        SetError(
            error,
            std::string("cannot open stable Raw root: ") +
                std::strerror(errno));
        return nullptr;
    }

    const std::string date_name =
        "capture_date=" + std::to_string(capture_date);
    const std::string stream_name =
        "stream=" + std::to_string(source_stream_id) +
        "-" + stream_slug;
    ScopedFd date;
    ScopedFd stream;
    if (!EnsureChildDirectory(
            root.get(), date_name, &date) ||
        !EnsureChildDirectory(
            date.get(), stream_name, &stream)) {
        SetError(
            error,
            std::string("cannot durably create Raw namespace: ") +
                std::strerror(errno));
        return nullptr;
    }
    try {
        return std::unique_ptr<RawStreamDirectory>(
            new RawStreamDirectory(
                stream.Release(),
                source_stream_id,
                capture_date));
    } catch (...) {
        SetError(error, "cannot allocate Raw stream directory");
        return nullptr;
    }
}

std::unique_ptr<RawStreamDirectory>
OpenOrCreateAuthorizedFreshRawStreamDirectory(
    const std::string& raw_root,
    std::uint32_t source_stream_id,
    std::uint32_t capture_date,
    const std::string& stream_slug,
    const RawFreshStateAuthorizationV1& authorization,
    const RawFreshMutationAuthorizationGateV1&
        authorization_gate,
    std::string* error) noexcept {
    SetError(error, {});
    if (authorization.source_stream_id !=
            source_stream_id ||
        authorization.capture_date != capture_date ||
        authorization.registry_stage !=
            RawFreshRegistryStageV1::kScaffolding ||
        !authorization_gate.Authorizes(
            authorization)) {
        SetError(
            error,
            "fresh Raw namespace mutation lacks exact durable SCAFFOLDING authorization");
        return nullptr;
    }
    // The authorization object retains its independent shared OFD lock for
    // this complete call. Once the exact generation is checked above, an
    // exclusive state transition cannot pass between any of the following
    // mkdir/open/fsync operations.
    return OpenOrCreateRawStreamDirectory(
        raw_root,
        source_stream_id,
        capture_date,
        stream_slug,
        error);
}

bool
CreateOrAdoptAuthorizedFreshRawMaintenanceDirectoryV1(
    const RawStreamDirectory& stream_directory,
    const RawFreshStateAuthorizationV1& authorization,
    const RawFreshMutationAuthorizationGateV1&
        authorization_gate,
    std::string* error) noexcept {
    SetError(error, {});
    const int stream_fd = stream_directory.descriptor();
    if (stream_fd < 0 ||
        authorization.source_stream_id !=
            stream_directory.source_stream_id() ||
        authorization.capture_date !=
            stream_directory.capture_date() ||
        authorization.registry_stage !=
            RawFreshRegistryStageV1::kScaffolding ||
        !authorization_gate.Authorizes(
            authorization)) {
        SetError(
            error,
            "fresh maintenance scaffold lacks exact durable SCAFFOLDING authorization");
        return false;
    }
    if (!InventoryBeforeMaintenanceScaffolding(
            stream_fd)) {
        SetError(
            error,
            "fresh maintenance scaffold found an unauthorized stream entry");
        return false;
    }

    // Revalidate immediately before the first namespace mutation. The
    // production action retains its shared OFD generation gate throughout
    // this call, so an exclusive state transition cannot pass afterward.
    if (!authorization_gate.Authorizes(
            authorization)) {
        SetError(
            error,
            "fresh maintenance SCAFFOLDING generation changed");
        return false;
    }
    for (;;) {
        if (::mkdirat(
                stream_fd, "maintenance", 0700) == 0 ||
            errno == EEXIST) {
            break;
        }
        if (errno != EINTR) {
            SetError(
                error,
                "cannot create fresh Raw maintenance directory");
            return false;
        }
    }

    ScopedFd maintenance(
        OpenDirectoryAt(stream_fd, "maintenance"));
    struct stat stream_status {};
    struct stat maintenance_status {};
    struct stat named_status {};
    if (maintenance.get() < 0 ||
        ::fstat(stream_fd, &stream_status) != 0 ||
        ::fstat(
            maintenance.get(),
            &maintenance_status) != 0 ||
        ::fstatat(
            stream_fd,
            "maintenance",
            &named_status,
            AT_SYMLINK_NOFOLLOW) != 0 ||
        !S_ISDIR(stream_status.st_mode) ||
        !S_ISDIR(maintenance_status.st_mode) ||
        !S_ISDIR(named_status.st_mode) ||
        stream_status.st_uid != ::geteuid() ||
        maintenance_status.st_uid != ::geteuid() ||
        named_status.st_uid != ::geteuid() ||
        (maintenance_status.st_mode & 07777) != 0700 ||
        (named_status.st_mode & 07777) != 0700 ||
        stream_status.st_dev !=
            maintenance_status.st_dev ||
        maintenance_status.st_dev !=
            named_status.st_dev ||
        maintenance_status.st_ino !=
            named_status.st_ino ||
        !ValidateEmptyMaintenanceDirectory(
            stream_fd) ||
        !FsyncLoop(maintenance.get(), false) ||
        !FsyncLoop(stream_fd, false) ||
        !SameNamedInode(
            stream_fd,
            "maintenance",
            maintenance.get())) {
        SetError(
            error,
            "fresh Raw maintenance directory failed secure durability validation");
        return false;
    }
    return true;
}

std::unique_ptr<RawStreamDirectory>
OpenExistingRawStreamDirectory(
    const std::string& raw_root,
    std::uint32_t source_stream_id,
    std::uint32_t capture_date,
    const std::string& stream_slug,
    std::string* error) noexcept {
    SetError(error, {});
    if (source_stream_id == 0U ||
        !IsGregorianDate(capture_date) ||
        !IsValidSlug(stream_slug)) {
        SetError(
            error,
            "existing Raw stream namespace is invalid");
        return nullptr;
    }
    ScopedFd root(OpenStableRawRoot(raw_root));
    if (root.get() < 0) {
        SetError(
            error,
            std::string(
                "cannot open stable Raw root: ") +
                std::strerror(errno));
        return nullptr;
    }
    const std::string date_name =
        "capture_date=" +
        std::to_string(capture_date);
    const std::string stream_name =
        "stream=" +
        std::to_string(source_stream_id) +
        "-" + stream_slug;
    ScopedFd date(
        OpenDirectoryAt(root.get(), date_name));
    ScopedFd stream(
        date.get() < 0
            ? -1
            : OpenDirectoryAt(
                  date.get(), stream_name));
    if (date.get() < 0 ||
        stream.get() < 0 ||
        !ValidatePrivateOwnedDirectory(
            date.get()) ||
        !ValidatePrivateOwnedDirectory(
            stream.get())) {
        SetError(
            error,
            std::string(
                "cannot securely open existing Raw namespace: ") +
                std::strerror(errno));
        return nullptr;
    }
    try {
        return std::unique_ptr<RawStreamDirectory>(
            new RawStreamDirectory(
                stream.Release(),
                source_stream_id,
                capture_date));
    } catch (...) {
        SetError(
            error,
            "cannot allocate existing Raw stream directory");
        return nullptr;
    }
}

RawFreshJournalAnchor::RawFreshJournalAnchor(
    int directory_fd,
    int journal_fd,
    RawV1JournalHeaderWire journal_header,
    RawFreshStateAuthorizationV1
        scaffolding_authorization) noexcept
    : directory_fd_(directory_fd),
      journal_fd_(journal_fd),
      journal_header_(journal_header),
      scaffolding_authorization_(
          scaffolding_authorization) {}

RawFreshJournalAnchor::~RawFreshJournalAnchor() {
    if (journal_fd_ >= 0) {
        static_cast<void>(::close(journal_fd_));
    }
    if (directory_fd_ >= 0) {
        static_cast<void>(::close(directory_fd_));
    }
}

int RawFreshJournalAnchor::ReleaseJournalFd() noexcept {
    return std::exchange(journal_fd_, -1);
}

RawBootstrapFiles::RawBootstrapFiles(
    int segment_fd,
    int journal_fd) noexcept
    : segment_fd_(segment_fd),
      journal_fd_(journal_fd) {}

RawBootstrapFiles::~RawBootstrapFiles() {
    if (segment_fd_ >= 0) {
        static_cast<void>(::close(segment_fd_));
    }
    if (journal_fd_ >= 0) {
        static_cast<void>(::close(journal_fd_));
    }
}

int RawBootstrapFiles::ReleaseSegmentFd() noexcept {
    return std::exchange(segment_fd_, -1);
}

int RawBootstrapFiles::ReleaseJournalFd() noexcept {
    return std::exchange(journal_fd_, -1);
}

std::unique_ptr<RawFreshJournalAnchor>
PublishFreshRawJournalAnchor(
    const RawWriterLease& lease,
    const RawV1JournalHeaderWire& journal_header,
    const RawFreshStateAuthorizationV1&
        scaffolding_authorization,
    const RawFreshMutationAuthorizationGateV1&
        authorization_gate,
    std::string* error) noexcept {
    SetError(error, {});
    DurableJournalHeaderV1 decoded_journal;
    if (lease.directory_descriptor() < 0 ||
        DecodeDurableJournalHeaderV1(
            journal_header, &decoded_journal) !=
            RawV1Error::kNone ||
        decoded_journal.source_stream_id !=
            lease.source_stream_id() ||
        decoded_journal.capture_date !=
            lease.capture_date() ||
        scaffolding_authorization.source_stream_id !=
            decoded_journal.source_stream_id ||
        scaffolding_authorization.capture_date !=
            decoded_journal.capture_date ||
        scaffolding_authorization.stream_day_id !=
            decoded_journal.stream_day_id ||
        l2flow::common::IsZeroIdentity(
            scaffolding_authorization
                .recovery_attempt) ||
        scaffolding_authorization.registry_stage !=
            RawFreshRegistryStageV1::kScaffolding ||
        scaffolding_authorization
                .durable_state_generation ==
            0U ||
        !authorization_gate.Authorizes(
            scaffolding_authorization)) {
        SetError(
            error,
            "fresh Raw journal anchor lacks exact durable SCAFFOLDING authorization");
        return nullptr;
    }

    ScopedFd directory_fd(
        OpenInventoryDirectory(
            lease.directory_descriptor()));
    if (directory_fd.get() < 0 ||
        !SameOpenInode(
            directory_fd.get(),
            lease.directory_descriptor()) ||
        !InventoryFreshNamespace(
            directory_fd.get(),
            lease,
            -1,
            error)) {
        if (error == nullptr || error->empty()) {
            SetError(
                error,
                "fresh Raw anchor pre-publication inventory failed");
        }
        return nullptr;
    }

    ScopedFd journal;
    if (!authorization_gate.Authorizes(
            scaffolding_authorization) ||
        !PublishHeaderFile(
            directory_fd.get(),
            kRawJournalTemporaryFilename,
            kRawJournalFilename,
            journal_header,
            0U,
            false,
            &journal,
            error)) {
        if (error != nullptr && error->empty()) {
            SetError(
                error,
                "fresh Raw SCAFFOLDING authorization was revoked");
        }
        return nullptr;
    }

    try {
        return std::unique_ptr<RawFreshJournalAnchor>(
            new RawFreshJournalAnchor(
                directory_fd.Release(),
                journal.Release(),
                journal_header,
                scaffolding_authorization));
    } catch (...) {
        SetError(
            error,
            "cannot allocate retained fresh Raw journal anchor");
        return nullptr;
    }
}

std::unique_ptr<RawBootstrapFiles>
CreateInitialRawSegment(
    const RawWriterLease& lease,
    RawFreshJournalAnchor& journal_anchor,
    const RawV1SegmentHeaderWire& segment_header,
    const RawFreshStateAuthorizationV1&
        init_authorization,
    const RawFreshMutationAuthorizationGateV1&
        authorization_gate,
    std::uint64_t segment_preallocation_bytes,
    std::string* error) noexcept {
    SetError(error, {});
    SegmentHeaderV1 decoded_segment;
    DurableJournalHeaderV1 decoded_journal;
    const RawFreshStateAuthorizationV1&
        scaffolding =
            journal_anchor.scaffolding_authorization_;
    if (lease.directory_descriptor() < 0 ||
        journal_anchor.directory_fd_ < 0 ||
        journal_anchor.journal_fd_ < 0 ||
        !SameOpenInode(
            lease.directory_descriptor(),
            journal_anchor.directory_fd_) ||
        DecodeSegmentHeaderV1(
            segment_header, &decoded_segment) !=
            RawV1Error::kNone ||
        DecodeDurableJournalHeaderV1(
            journal_anchor.journal_header_,
            &decoded_journal) !=
            RawV1Error::kNone ||
        decoded_segment.source_stream_id !=
            lease.source_stream_id() ||
        decoded_segment.capture_date !=
            lease.capture_date() ||
        decoded_journal.source_stream_id !=
            lease.source_stream_id() ||
        decoded_journal.capture_date !=
            lease.capture_date() ||
        decoded_segment.stream_day_id !=
            decoded_journal.stream_day_id ||
        decoded_segment.raw_schema_sha256 !=
            decoded_journal.raw_schema_sha256 ||
        decoded_segment.segment_sequence != 1U ||
        decoded_segment.segment_base_wal_pos != 0U ||
        decoded_segment.segment_flags != 0U ||
        segment_preallocation_bytes <
            kRawV1SegmentHeaderBytes ||
        init_authorization.source_stream_id !=
            scaffolding.source_stream_id ||
        init_authorization.capture_date !=
            scaffolding.capture_date ||
        init_authorization.stream_day_id !=
            scaffolding.stream_day_id ||
        init_authorization.recovery_attempt !=
            scaffolding.recovery_attempt ||
        init_authorization.registry_stage !=
            RawFreshRegistryStageV1::kInit ||
        init_authorization.durable_state_generation <=
            scaffolding.durable_state_generation ||
        !authorization_gate.Authorizes(
            init_authorization)) {
        SetError(
            error,
            "initial Raw segment lacks exact higher-generation durable INIT authorization");
        return nullptr;
    }

    RawV1JournalHeaderWire journal_readback{};
    if (!InventoryFreshNamespace(
            journal_anchor.directory_fd_,
            lease,
            journal_anchor.journal_fd_,
            error) ||
        !ValidateMutableRawFile(
            journal_anchor.directory_fd_,
            kRawJournalFilename,
            journal_anchor.journal_fd_,
            static_cast<off_t>(
                kRawV1JournalHeaderBytes)) ||
        !PreadAll(
            journal_anchor.journal_fd_,
            0U,
            journal_readback) ||
        journal_readback !=
            journal_anchor.journal_header_) {
        if (error == nullptr || error->empty()) {
            SetError(
                error,
                "retained fresh Raw journal anchor changed before INIT");
        }
        return nullptr;
    }

    ScopedFd segment;
    if (!authorization_gate.Authorizes(
            init_authorization) ||
        !PublishHeaderFile(
            journal_anchor.directory_fd_,
            kRawFirstSegmentTemporaryFilename,
            kRawFirstSegmentFilename,
            segment_header,
            segment_preallocation_bytes,
            true,
            &segment,
            error)) {
        if (error != nullptr && error->empty()) {
            SetError(
                error,
                "fresh Raw INIT authorization was revoked");
        }
        return nullptr;
    }
    try {
        return std::unique_ptr<RawBootstrapFiles>(
            new RawBootstrapFiles(
                segment.Release(),
                journal_anchor.ReleaseJournalFd()));
    } catch (...) {
        SetError(error, "cannot allocate Raw bootstrap descriptors");
        return nullptr;
    }
}

std::unique_ptr<RawBootstrapFiles>
CreateRotatedRawBootstrap(
    const RawWriterLease& lease,
    const RawV1SegmentHeaderWire& next_segment_header,
    const RawV1JournalHeaderWire& expected_journal_header,
    const RawWalExistingJournalInit& existing_journal,
    std::uint64_t segment_preallocation_bytes,
    std::string* error) noexcept {
    SetError(error, {});
    SegmentHeaderV1 segment;
    DurableJournalHeaderV1 journal;
    if (lease.directory_descriptor() < 0 ||
        DecodeSegmentHeaderV1(
            next_segment_header, &segment) !=
            RawV1Error::kNone ||
        DecodeDurableJournalHeaderV1(
            expected_journal_header, &journal) !=
            RawV1Error::kNone ||
        segment.source_stream_id !=
            lease.source_stream_id() ||
        segment.capture_date != lease.capture_date() ||
        journal.source_stream_id !=
            lease.source_stream_id() ||
        journal.capture_date != lease.capture_date() ||
        segment.stream_day_id != journal.stream_day_id ||
        segment.raw_schema_sha256 !=
            journal.raw_schema_sha256 ||
        segment.segment_flags != 0U ||
        segment.segment_sequence <= 1U ||
        existing_journal.previous_segment_sequence == 0U ||
        existing_journal.previous_segment_sequence ==
            std::numeric_limits<std::uint32_t>::max() ||
        segment.segment_sequence !=
            existing_journal.previous_segment_sequence + 1U ||
        segment.segment_base_wal_pos !=
            existing_journal.previous_sealed_cursor
                .global_wal_pos ||
        existing_journal.previous_marker_flags !=
            kRawV1SegmentSealed ||
        existing_journal.previous_sealed_cursor
                .segment_offset <
            kRawV1SegmentHeaderBytes ||
        existing_journal.previous_sealed_cursor
                .ingress_sequence ==
            std::numeric_limits<std::uint64_t>::max() ||
        segment.first_ingress_sequence !=
            existing_journal.previous_sealed_cursor
                    .ingress_sequence +
                1U ||
        existing_journal.journal_append_offset <
            kRawV1JournalHeaderBytes +
                kRawV1DurableMarkerBytes ||
        (existing_journal.journal_append_offset -
         kRawV1JournalHeaderBytes) %
                kRawV1DurableMarkerBytes !=
            0U ||
        existing_journal.journal_append_offset >
            static_cast<std::uint64_t>(
                std::numeric_limits<off_t>::max()) ||
        segment_preallocation_bytes <
            kRawV1SegmentHeaderBytes) {
        SetError(
            error,
            "rotated Raw bootstrap identity or cursor is invalid");
        return nullptr;
    }

    ScopedFd journal_fd;
    for (;;) {
        journal_fd.Reset(::openat(
            lease.directory_descriptor(),
            kRawJournalFilename,
            O_RDWR | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC));
        if (journal_fd.get() >= 0 || errno != EINTR) {
            break;
        }
    }
    if (!ValidateMutableRawFile(
            lease.directory_descriptor(),
            kRawJournalFilename,
            journal_fd.get(),
            static_cast<off_t>(
                existing_journal.journal_append_offset))) {
        SetError(
            error,
            "existing Raw journal failed secure rotation attach");
        return nullptr;
    }
    RawV1JournalHeaderWire actual_header{};
    RawV1DurableMarkerWire marker_wire{};
    if (!PreadAll(
            journal_fd.get(), 0U, actual_header) ||
        actual_header != expected_journal_header ||
        !PreadAll(
            journal_fd.get(),
            existing_journal.journal_append_offset -
                kRawV1DurableMarkerBytes,
            marker_wire)) {
        SetError(
            error,
            "existing Raw journal bytes changed before rotation");
        return nullptr;
    }
    DurableMarkerV1 marker;
    if (DecodeDurableMarkerV1(
            marker_wire, &marker) !=
            RawV1Error::kNone ||
        marker.source_stream_id !=
            lease.source_stream_id() ||
        marker.segment_sequence !=
            existing_journal.previous_segment_sequence ||
        marker.durable_global_wal_pos !=
            existing_journal.previous_sealed_cursor
                .global_wal_pos ||
        marker.durable_ingress_sequence !=
            existing_journal.previous_sealed_cursor
                .ingress_sequence ||
        marker.durable_segment_offset !=
            existing_journal.previous_sealed_cursor
                .segment_offset ||
        marker.marker_flags != kRawV1SegmentSealed) {
        SetError(
            error,
            "existing Raw journal does not end at the expected seal");
        return nullptr;
    }

    const std::string final_name =
        SegmentFilename(segment.segment_sequence, false);
    const std::string temporary_name =
        SegmentFilename(segment.segment_sequence, true);
    if (final_name.empty() || temporary_name.empty()) {
        SetError(error, "rotated Raw segment name is invalid");
        return nullptr;
    }
    struct stat status {};
    if (::fstatat(
            lease.directory_descriptor(),
            final_name.c_str(),
            &status,
            AT_SYMLINK_NOFOLLOW) == 0 ||
        errno != ENOENT ||
        ::fstatat(
            lease.directory_descriptor(),
            temporary_name.c_str(),
            &status,
            AT_SYMLINK_NOFOLLOW) == 0 ||
        errno != ENOENT) {
        SetError(
            error,
            "rotated Raw bootstrap requires absent target and typed tmp");
        return nullptr;
    }

    ScopedFd segment_fd;
    if (!PublishHeaderFile(
            lease.directory_descriptor(),
            temporary_name.c_str(),
            final_name.c_str(),
            next_segment_header,
            segment_preallocation_bytes,
            true,
            &segment_fd,
            error)) {
        return nullptr;
    }
    try {
        return std::unique_ptr<RawBootstrapFiles>(
            new RawBootstrapFiles(
                segment_fd.Release(),
                journal_fd.Release()));
    } catch (...) {
        SetError(
            error,
            "cannot allocate rotated Raw bootstrap result");
        return nullptr;
    }
}

}  // namespace l2flow::ingress
