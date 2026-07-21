#include "l2flow/ingress/raw_posix_io.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <limits>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <unistd.h>

namespace l2flow::ingress {
namespace {

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

bool IsValidWritableRawFd(
    int fd,
    std::string_view label,
    std::string* error) noexcept {
    if (fd < 0) {
        SetError(error, std::string(label) + " descriptor is invalid");
        return false;
    }
    struct stat status {};
    if (::fstat(fd, &status) != 0) {
        SetError(
            error,
            std::string("cannot inspect ") + std::string(label) +
                " descriptor: " + std::strerror(errno));
        return false;
    }
    if (!S_ISREG(status.st_mode) ||
        status.st_uid != ::geteuid() ||
        status.st_nlink != 1 ||
        (status.st_mode & 0777U) != 0600U) {
        SetError(
            error,
            std::string(label) +
                " must be an owner-only singly-linked regular file");
        return false;
    }
    const int flags = ::fcntl(fd, F_GETFL);
    if (flags < 0) {
        SetError(
            error,
            std::string("cannot inspect ") + std::string(label) +
                " descriptor flags: " + std::strerror(errno));
        return false;
    }
    if ((flags & O_ACCMODE) != O_RDWR ||
        (flags & O_APPEND) != 0) {
        SetError(
            error,
            std::string(label) +
                " must be O_RDWR without O_APPEND");
        return false;
    }
    return true;
}

[[nodiscard]] bool IsCanonicalSegmentFilename(
    std::string_view name) noexcept {
    constexpr std::string_view prefix = "segment-";
    constexpr std::string_view suffix = ".raw";
    if (name.size() != 20U ||
        name.substr(0U, prefix.size()) != prefix ||
        name.substr(16U, suffix.size()) != suffix) {
        return false;
    }
    bool nonzero = false;
    for (std::size_t index = 8U; index < 16U; ++index) {
        if (name[index] < '0' || name[index] > '9') {
            return false;
        }
        nonzero = nonzero || name[index] != '0';
    }
    return nonzero;
}

[[nodiscard]] bool SameNamedInode(
    int directory_fd,
    std::string_view name,
    int descriptor,
    const struct stat& expected) noexcept {
    if (name.empty() ||
        name.size() > 20U ||
        name.find('/') != std::string_view::npos ||
        name.find('\0') != std::string_view::npos) {
        return false;
    }
    struct stat opened {};
    struct stat named {};
    std::array<char, 21U> owned_name{};
    std::memcpy(
        owned_name.data(),
        name.data(),
        name.size());
    return ::fstat(descriptor, &opened) == 0 &&
           ::fstatat(
               directory_fd,
               owned_name.data(),
               &named,
               AT_SYMLINK_NOFOLLOW) == 0 &&
           opened.st_dev == expected.st_dev &&
           opened.st_ino == expected.st_ino &&
           opened.st_dev == named.st_dev &&
           opened.st_ino == named.st_ino &&
           opened.st_size == expected.st_size &&
           S_ISREG(named.st_mode);
}

[[nodiscard]] int DuplicateDirectory(
    int descriptor) noexcept {
    for (;;) {
        const int duplicate =
            ::fcntl(
                descriptor,
                F_DUPFD_CLOEXEC,
                0);
        if (duplicate >= 0 || errno != EINTR) {
            return duplicate;
        }
    }
}

class PosixRawWalIo final
    : public RawWalIo,
      public RawReserveMutationTargetProviderV1 {
public:
    PosixRawWalIo(
        int segment_fd,
        int journal_fd,
        int stream_directory_fd,
        std::string segment_name,
        dev_t segment_device,
        ino_t segment_inode,
        dev_t journal_device,
        ino_t journal_inode)
        : segment_fd_(segment_fd),
          journal_fd_(journal_fd),
          stream_directory_fd_(stream_directory_fd),
          segment_name_(std::move(segment_name)),
          segment_device_(segment_device),
          segment_inode_(segment_inode),
          journal_device_(journal_device),
          journal_inode_(journal_inode) {}

    ~PosixRawWalIo() override {
        CloseOne(&segment_fd_);
        CloseOne(&journal_fd_);
        CloseOne(&stream_directory_fd_);
    }

    RawWalWriteResult WritevSome(
        RawWalFile file,
        std::uint64_t offset,
        std::span<const RawWalIoVector> vectors) noexcept override {
        const int fd = Get(file);
        if (fd < 0) {
            return {0U, EBADF};
        }
        if (!TargetStillNames(file)) {
            return {0U, ESTALE};
        }
        if (offset >
            static_cast<std::uint64_t>(
                std::numeric_limits<off_t>::max())) {
            return {0U, EOVERFLOW};
        }
        if (vectors.empty()) {
            return {};
        }

        long configured_iov_max = ::sysconf(_SC_IOV_MAX);
        if (configured_iov_max <= 0) {
            configured_iov_max = 16;
        }
        const std::size_t iov_max =
            static_cast<std::size_t>(configured_iov_max);
        const std::size_t count =
            std::min(vectors.size(), iov_max);
        std::vector<struct iovec> native;
        try {
            native.reserve(count);
        } catch (...) {
            return {0U, ENOMEM};
        }

        const std::size_t maximum_total =
            static_cast<std::size_t>(
                std::numeric_limits<ssize_t>::max());
        std::size_t remaining = maximum_total;
        for (std::size_t index = 0U;
             index < count && remaining != 0U;
             ++index) {
            const std::span<const std::byte> bytes =
                vectors[index].bytes;
            if (bytes.empty()) {
                continue;
            }
            const std::size_t included =
                std::min(bytes.size(), remaining);
            struct iovec entry {};
            entry.iov_base = const_cast<std::byte*>(bytes.data());
            entry.iov_len = included;
            try {
                native.push_back(entry);
            } catch (...) {
                return {0U, ENOMEM};
            }
            remaining -= included;
            if (included != bytes.size()) {
                break;
            }
        }
        if (native.empty()) {
            return {};
        }
        if (native.size() >
            static_cast<std::size_t>(
                std::numeric_limits<int>::max())) {
            return {0U, EOVERFLOW};
        }
        const ssize_t result = ::pwritev(
            fd,
            native.data(),
            static_cast<int>(native.size()),
            static_cast<off_t>(offset));
        if (result < 0) {
            return {0U, errno};
        }
        return {
            static_cast<std::size_t>(result),
            0};
    }

    int Fdatasync(RawWalFile file) noexcept override {
        const int fd = Get(file);
        if (fd < 0) {
            return EBADF;
        }
        if (!TargetStillNames(file)) {
            return ESTALE;
        }
        return ::fdatasync(fd) == 0 ? 0 : errno;
    }

    int Truncate(
        RawWalFile file,
        std::uint64_t logical_size) noexcept override {
        const int fd = Get(file);
        if (fd < 0) {
            return EBADF;
        }
        if (!TargetStillNames(file)) {
            return ESTALE;
        }
        if (logical_size >
            static_cast<std::uint64_t>(
                std::numeric_limits<off_t>::max())) {
            return EOVERFLOW;
        }
        return ::ftruncate(
                   fd,
                   static_cast<off_t>(logical_size)) == 0
                   ? 0
                   : errno;
    }

    int Close(RawWalFile file) noexcept override {
        int* const fd =
            file == RawWalFile::kSegment
                ? &segment_fd_
                : &journal_fd_;
        if (*fd < 0) {
            return 0;
        }
        const int closing = std::exchange(*fd, -1);
        return ::close(closing) == 0 ? 0 : errno;
    }

    [[nodiscard]] int
    RawReserveMutationTargetDirectoryDescriptorV1()
        const noexcept override {
        return stream_directory_fd_;
    }

private:
    [[nodiscard]] bool TargetStillNames(
        RawWalFile file) const noexcept {
        if (stream_directory_fd_ < 0) {
            return true;
        }
        const int descriptor = Get(file);
        const char* const name =
            file == RawWalFile::kSegment
                ? segment_name_.c_str()
                : "durable.journal";
        const dev_t expected_device =
            file == RawWalFile::kSegment
                ? segment_device_
                : journal_device_;
        const ino_t expected_inode =
            file == RawWalFile::kSegment
                ? segment_inode_
                : journal_inode_;
        struct stat opened {};
        struct stat named {};
        return descriptor >= 0 &&
               ::fstat(descriptor, &opened) == 0 &&
               ::fstatat(
                   stream_directory_fd_,
                   name,
                   &named,
                   AT_SYMLINK_NOFOLLOW) == 0 &&
               opened.st_dev == expected_device &&
               opened.st_ino == expected_inode &&
               named.st_dev == expected_device &&
               named.st_ino == expected_inode &&
               S_ISREG(opened.st_mode) &&
               S_ISREG(named.st_mode);
    }

    int Get(RawWalFile file) const noexcept {
        return file == RawWalFile::kSegment
                   ? segment_fd_
                   : journal_fd_;
    }

    static void CloseOne(int* fd) noexcept {
        if (*fd >= 0) {
            const int closing = std::exchange(*fd, -1);
            static_cast<void>(::close(closing));
        }
    }

    int segment_fd_ = -1;
    int journal_fd_ = -1;
    int stream_directory_fd_ = -1;
    std::string segment_name_;
    dev_t segment_device_ = 0;
    ino_t segment_inode_ = 0;
    dev_t journal_device_ = 0;
    ino_t journal_inode_ = 0;
};

}  // namespace

std::unique_ptr<RawWalIo> AdoptPosixRawWalIo(
    int segment_fd,
    int journal_fd,
    std::string* error) noexcept {
    if (!IsValidWritableRawFd(
            segment_fd, "segment", error) ||
        !IsValidWritableRawFd(
            journal_fd, "journal", error)) {
        return nullptr;
    }
    struct stat segment_status {};
    struct stat journal_status {};
    if (::fstat(segment_fd, &segment_status) != 0 ||
        ::fstat(journal_fd, &journal_status) != 0) {
        SetError(
            error,
            std::string("cannot compare Raw descriptors: ") +
                std::strerror(errno));
        return nullptr;
    }
    if (segment_status.st_dev == journal_status.st_dev &&
        segment_status.st_ino == journal_status.st_ino) {
        SetError(
            error,
            "segment and journal descriptors alias one inode");
        return nullptr;
    }
    try {
        auto result =
            std::make_unique<PosixRawWalIo>(
                segment_fd,
                journal_fd,
                -1,
                std::string{},
                0,
                0,
                0,
                0);
        SetError(error, {});
        return result;
    } catch (...) {
        SetError(error, "cannot allocate the POSIX Raw I/O backend");
        return nullptr;
    }
}

std::unique_ptr<RawWalIo>
AdoptTargetBoundPosixRawWalIo(
    int stream_directory_fd,
    std::string_view canonical_segment_filename,
    int segment_fd,
    int journal_fd,
    std::string* error) noexcept {
    if (!IsCanonicalSegmentFilename(
            canonical_segment_filename) ||
        !IsValidWritableRawFd(
            segment_fd, "segment", error) ||
        !IsValidWritableRawFd(
            journal_fd, "journal", error)) {
        if (!IsCanonicalSegmentFilename(
                canonical_segment_filename)) {
            SetError(
                error,
                "segment filename is not canonical");
        }
        return nullptr;
    }
    struct stat directory_status {};
    struct stat segment_status {};
    struct stat journal_status {};
    if (stream_directory_fd < 0 ||
        ::fstat(
            stream_directory_fd,
            &directory_status) != 0 ||
        !S_ISDIR(directory_status.st_mode) ||
        directory_status.st_uid != ::geteuid() ||
        (directory_status.st_mode & 07777U) != 0700U ||
        ::fstat(segment_fd, &segment_status) != 0 ||
        ::fstat(journal_fd, &journal_status) != 0 ||
        segment_status.st_dev != directory_status.st_dev ||
        journal_status.st_dev != directory_status.st_dev ||
        (segment_status.st_dev == journal_status.st_dev &&
         segment_status.st_ino == journal_status.st_ino) ||
        !SameNamedInode(
            stream_directory_fd,
            canonical_segment_filename,
            segment_fd,
            segment_status) ||
        !SameNamedInode(
            stream_directory_fd,
            "durable.journal",
            journal_fd,
            journal_status)) {
        SetError(
            error,
            "Raw WAL descriptors are not bound to the retained stream-day directory");
        return nullptr;
    }
    const int retained_directory =
        DuplicateDirectory(stream_directory_fd);
    if (retained_directory < 0 ||
        !SameNamedInode(
            retained_directory,
            canonical_segment_filename,
            segment_fd,
            segment_status) ||
        !SameNamedInode(
            retained_directory,
            "durable.journal",
            journal_fd,
            journal_status)) {
        if (retained_directory >= 0) {
            static_cast<void>(
                ::close(retained_directory));
        }
        SetError(
            error,
            "Raw WAL target changed during descriptor adoption");
        return nullptr;
    }
    try {
        auto result =
            std::make_unique<PosixRawWalIo>(
                segment_fd,
                journal_fd,
                retained_directory,
                std::string(
                    canonical_segment_filename),
                segment_status.st_dev,
                segment_status.st_ino,
                journal_status.st_dev,
                journal_status.st_ino);
        SetError(error, {});
        return result;
    } catch (...) {
        static_cast<void>(
            ::close(retained_directory));
        SetError(
            error,
            "cannot allocate the target-bound POSIX Raw I/O backend");
        return nullptr;
    }
}

}  // namespace l2flow::ingress
