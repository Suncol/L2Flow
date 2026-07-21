#include "l2flow/common/sealed_file_snapshot.h"

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <limits>
#include <optional>
#include <string>
#include <utility>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace l2flow::common {
namespace {

constexpr int kRequiredSeals =
    F_SEAL_WRITE | F_SEAL_GROW | F_SEAL_SHRINK | F_SEAL_SEAL;

void SetError(std::string* error, std::string message) noexcept {
    if (error == nullptr) {
        return;
    }
    try {
        *error = std::move(message);
    } catch (...) {
    }
}

void SetErrorLiteral(std::string* error, const char* message) noexcept {
    if (error == nullptr) {
        return;
    }
    try {
        *error = message;
    } catch (...) {
    }
}

std::string ErrnoDetail(const char* operation, int error_number) {
    return std::string(operation) + " failed: " +
           std::strerror(error_number);
}

class OwnedFd final {
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

    [[nodiscard]] int release() noexcept {
        return std::exchange(fd_, -1);
    }

private:
    int fd_;
};

bool SameSourceVersion(const struct stat& before,
                       const struct stat& after) noexcept {
    return before.st_dev == after.st_dev &&
           before.st_ino == after.st_ino &&
           before.st_mode == after.st_mode &&
           before.st_size == after.st_size &&
           before.st_mtim.tv_sec == after.st_mtim.tv_sec &&
           before.st_mtim.tv_nsec == after.st_mtim.tv_nsec &&
           before.st_ctim.tv_sec == after.st_ctim.tv_sec &&
           before.st_ctim.tv_nsec == after.st_ctim.tv_nsec;
}

bool WriteAll(int fd,
              const std::byte* bytes,
              std::size_t size,
              std::string* error) {
    std::size_t written = 0;
    while (written < size) {
        const ssize_t result =
            ::write(fd, bytes + written, size - written);
        if (result < 0) {
            if (errno == EINTR) {
                continue;
            }
            SetError(
                error,
                ErrnoDetail("write sealed snapshot", errno));
            return false;
        }
        if (result == 0) {
            SetErrorLiteral(
                error, "write sealed snapshot made no progress");
            return false;
        }
        written += static_cast<std::size_t>(result);
    }
    return true;
}

bool CopyAndSeal(int source_fd,
                 std::optional<std::uint64_t> exact_size,
                 std::optional<std::uint64_t> maximum_size,
                 int* snapshot_fd,
                 std::uint64_t* snapshot_size,
                 std::string* error) {
    struct stat before {};
    if (::fstat(source_fd, &before) != 0) {
        SetError(
            error, ErrnoDetail("stat snapshot source", errno));
        return false;
    }
    if (!S_ISREG(before.st_mode)) {
        SetErrorLiteral(error, "snapshot source is not a regular file");
        return false;
    }
    if (before.st_size < 0) {
        SetErrorLiteral(error, "snapshot source has a negative size");
        return false;
    }
    const std::uint64_t expected_size =
        static_cast<std::uint64_t>(before.st_size);
    if (exact_size.has_value() &&
        expected_size != *exact_size) {
        SetErrorLiteral(
            error,
            "snapshot source size differs from the required size");
        return false;
    }
    if (maximum_size.has_value() &&
        expected_size > *maximum_size) {
        SetErrorLiteral(
            error,
            "snapshot source exceeds size bound");
        return false;
    }
    if (expected_size >
        static_cast<std::uint64_t>(
            std::numeric_limits<off_t>::max())) {
        SetErrorLiteral(
            error, "snapshot source exceeds supported offset range");
        return false;
    }

    const int raw_memfd = ::memfd_create(
        "l2flow-sealed-snapshot",
        MFD_ALLOW_SEALING | MFD_CLOEXEC);
    if (raw_memfd < 0) {
        const int saved_errno = errno;
        SetError(
            error,
            ErrnoDetail("create sealed snapshot memfd", saved_errno));
        return false;
    }
    OwnedFd destination(raw_memfd);

    std::array<std::byte, 64U * 1024U> buffer{};
    std::uint64_t copied = 0;
    while (copied < expected_size) {
        const std::uint64_t remaining = expected_size - copied;
        const std::size_t requested =
            remaining < static_cast<std::uint64_t>(buffer.size())
                ? static_cast<std::size_t>(remaining)
                : buffer.size();
        ssize_t count = 0;
        do {
            count = ::pread(
                source_fd,
                buffer.data(),
                requested,
                static_cast<off_t>(copied));
        } while (count < 0 && errno == EINTR);
        if (count < 0) {
            SetError(
                error,
                ErrnoDetail("read snapshot source", errno));
            return false;
        }
        if (count == 0) {
            SetErrorLiteral(
                error,
                "snapshot source changed or ended during copy");
            return false;
        }
        const std::size_t unsigned_count =
            static_cast<std::size_t>(count);
        if (!WriteAll(
                destination.get(),
                buffer.data(),
                unsigned_count,
                error)) {
            return false;
        }
        copied += static_cast<std::uint64_t>(unsigned_count);
    }

    struct stat after {};
    if (::fstat(source_fd, &after) != 0) {
        SetError(
            error,
            ErrnoDetail("restat snapshot source", errno));
        return false;
    }
    if (!SameSourceVersion(before, after)) {
        SetErrorLiteral(
            error, "snapshot source changed during copy");
        return false;
    }

    struct stat copied_metadata {};
    if (::fstat(destination.get(), &copied_metadata) != 0) {
        SetError(
            error,
            ErrnoDetail("stat sealed snapshot", errno));
        return false;
    }
    if (!S_ISREG(copied_metadata.st_mode) ||
        copied_metadata.st_size != before.st_size) {
        SetErrorLiteral(
            error, "sealed snapshot size verification failed");
        return false;
    }

    if (::fcntl(destination.get(), F_ADD_SEALS, kRequiredSeals) != 0) {
        SetError(
            error,
            ErrnoDetail("seal immutable snapshot", errno));
        return false;
    }
    const int actual_seals =
        ::fcntl(destination.get(), F_GET_SEALS);
    if (actual_seals < 0 ||
        (actual_seals & kRequiredSeals) != kRequiredSeals) {
        SetErrorLiteral(
            error, "sealed snapshot seal verification failed");
        return false;
    }

    *snapshot_fd = destination.release();
    *snapshot_size = expected_size;
    if (error != nullptr) {
        error->clear();
    }
    return true;
}

}  // namespace

SealedFileSnapshot::SealedFileSnapshot(
    int fd, std::uint64_t size) noexcept
    : fd_(fd), size_(size) {}

SealedFileSnapshot::~SealedFileSnapshot() {
    Reset();
}

SealedFileSnapshot::SealedFileSnapshot(
    SealedFileSnapshot&& other) noexcept
    : fd_(std::exchange(other.fd_, -1)),
      size_(std::exchange(other.size_, 0)) {}

SealedFileSnapshot& SealedFileSnapshot::operator=(
    SealedFileSnapshot&& other) noexcept {
    if (this == &other) {
        return *this;
    }
    Reset();
    fd_ = std::exchange(other.fd_, -1);
    size_ = std::exchange(other.size_, 0);
    return *this;
}

bool SealedFileSnapshot::valid() const noexcept {
    return fd_ >= 0;
}

int SealedFileSnapshot::fd() const noexcept {
    return fd_;
}

std::uint64_t SealedFileSnapshot::size() const noexcept {
    return size_;
}

std::string SealedFileSnapshot::proc_fd_path() const {
    return "/proc/self/fd/" + std::to_string(fd_);
}

void SealedFileSnapshot::Reset() noexcept {
    if (fd_ >= 0) {
        static_cast<void>(::close(fd_));
    }
    fd_ = -1;
    size_ = 0;
}

bool CreateSealedFileSnapshot(
    const std::filesystem::path& source,
    SealedFileSnapshot* snapshot,
    std::string* error,
    std::optional<std::uint64_t> exact_size,
    std::optional<std::uint64_t> maximum_size) noexcept {
    if (snapshot == nullptr) {
        SetErrorLiteral(error, "sealed snapshot output pointer is null");
        return false;
    }
    try {
        if (source.empty() ||
            source.native().find('\0') != std::string::npos) {
            SetErrorLiteral(
                error,
                "snapshot source path is empty or contains NUL");
            return false;
        }
        static_assert(O_NOFOLLOW != 0);
        const int raw_fd = ::open(
            source.c_str(),
            O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
        if (raw_fd < 0) {
            SetError(
                error,
                ErrnoDetail("open snapshot source", errno));
            return false;
        }
        const OwnedFd source_fd(raw_fd);
        int created_fd = -1;
        std::uint64_t created_size = 0;
        if (!CopyAndSeal(
                source_fd.get(),
                exact_size,
                maximum_size,
                &created_fd,
                &created_size,
                error)) {
            return false;
        }
        SealedFileSnapshot created(created_fd, created_size);
        *snapshot = std::move(created);
        return true;
    } catch (const std::exception&) {
        SetErrorLiteral(error, "create sealed snapshot failed");
        return false;
    } catch (...) {
        SetErrorLiteral(
            error,
            "create sealed snapshot failed with an unknown exception");
        return false;
    }
}

bool CreateSealedFileSnapshotFromOpenFd(
    int source_fd,
    SealedFileSnapshot* snapshot,
    std::string* error,
    std::optional<std::uint64_t> exact_size,
    std::optional<std::uint64_t> maximum_size) noexcept {
    if (snapshot == nullptr) {
        SetErrorLiteral(error, "sealed snapshot output pointer is null");
        return false;
    }
    try {
        if (source_fd < 0) {
            SetErrorLiteral(
                error, "snapshot source descriptor is invalid");
            return false;
        }
        int duplicated_fd = -1;
        do {
            duplicated_fd =
                ::fcntl(source_fd, F_DUPFD_CLOEXEC, 0);
        } while (duplicated_fd < 0 && errno == EINTR);
        if (duplicated_fd < 0) {
            SetError(
                error,
                ErrnoDetail(
                    "retain snapshot source descriptor", errno));
            return false;
        }
        const OwnedFd stable_source(duplicated_fd);
        int created_fd = -1;
        std::uint64_t created_size = 0;
        if (!CopyAndSeal(
                stable_source.get(),
                exact_size,
                maximum_size,
                &created_fd,
                &created_size,
                error)) {
            return false;
        }
        SealedFileSnapshot created(created_fd, created_size);
        *snapshot = std::move(created);
        return true;
    } catch (const std::exception&) {
        SetErrorLiteral(error, "create sealed snapshot failed");
        return false;
    } catch (...) {
        SetErrorLiteral(
            error,
            "create sealed snapshot failed with an unknown exception");
        return false;
    }
}

bool ValidateSealedFileSnapshotFd(
    int fd,
    std::uint64_t* size,
    std::string* error) noexcept {
    if (size == nullptr) {
        SetErrorLiteral(error, "sealed snapshot size pointer is null");
        return false;
    }
    try {
        const int descriptor_flags =
            fd < 0 ? -1 : ::fcntl(fd, F_GETFD);
        if (descriptor_flags < 0) {
            SetErrorLiteral(
                error, "sealed snapshot descriptor is invalid");
            return false;
        }
        if ((descriptor_flags & FD_CLOEXEC) == 0) {
            SetErrorLiteral(
                error,
                "sealed snapshot descriptor is not close-on-exec");
            return false;
        }
        struct stat metadata {};
        if (::fstat(fd, &metadata) != 0) {
            SetError(
                error,
                ErrnoDetail("stat sealed snapshot", errno));
            return false;
        }
        if (!S_ISREG(metadata.st_mode) || metadata.st_size < 0) {
            SetErrorLiteral(
                error,
                "sealed snapshot descriptor is not a regular file");
            return false;
        }
        const int seals = ::fcntl(fd, F_GET_SEALS);
        if (seals < 0 ||
            (seals & kRequiredSeals) != kRequiredSeals) {
            SetErrorLiteral(
                error,
                "snapshot descriptor lacks required immutable seals");
            return false;
        }
        *size = static_cast<std::uint64_t>(metadata.st_size);
        if (error != nullptr) {
            error->clear();
        }
        return true;
    } catch (const std::exception&) {
        SetErrorLiteral(error, "validate sealed snapshot failed");
        return false;
    } catch (...) {
        SetErrorLiteral(
            error,
            "validate sealed snapshot failed with an unknown exception");
        return false;
    }
}

}  // namespace l2flow::common
