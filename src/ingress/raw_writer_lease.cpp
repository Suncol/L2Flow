#include "l2flow/ingress/raw_writer_lease.h"

#include "l2flow/common/crc32c.h"

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

#include <dirent.h>
#include <fcntl.h>
#include <linux/fs.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/syscall.h>
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

void StoreU16(
    std::uint16_t value,
    std::span<std::byte> output,
    std::size_t offset) noexcept {
    output[offset] = static_cast<std::byte>(value & 0xffU);
    output[offset + 1U] =
        static_cast<std::byte>((value >> 8U) & 0xffU);
}

void StoreU32(
    std::uint32_t value,
    std::span<std::byte> output,
    std::size_t offset) noexcept {
    for (std::size_t index = 0U; index < 4U; ++index) {
        output[offset + index] =
            static_cast<std::byte>(
                (value >>
                 static_cast<unsigned int>(index * 8U)) &
                0xffU);
    }
}

std::uint16_t LoadU16(
    std::span<const std::byte> input,
    std::size_t offset) noexcept {
    return static_cast<std::uint16_t>(
        std::to_integer<std::uint16_t>(input[offset]) |
        static_cast<std::uint16_t>(
            std::to_integer<std::uint16_t>(
                input[offset + 1U])
            << 8U));
}

std::uint32_t LoadU32(
    std::span<const std::byte> input,
    std::size_t offset) noexcept {
    std::uint32_t result = 0U;
    for (std::size_t index = 0U; index < 4U; ++index) {
        result |=
            std::to_integer<std::uint32_t>(
                input[offset + index])
            << static_cast<unsigned int>(index * 8U);
    }
    return result;
}

bool IsZero(
    std::span<const std::byte> bytes) noexcept {
    return std::all_of(
        bytes.begin(),
        bytes.end(),
        [](std::byte value) {
            return value == std::byte{0};
        });
}

int CloseNoIntr(int fd) noexcept {
    return fd < 0 || ::close(fd) == 0 ? 0 : errno;
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

bool PwriteAll(
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

bool PreadAll(
    int fd,
    std::span<std::byte> bytes) noexcept {
    std::size_t offset = 0U;
    while (offset < bytes.size()) {
        const ssize_t result = ::pread(
            fd,
            bytes.data() + offset,
            bytes.size() - offset,
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

bool ValidateDirectoryFd(
    int directory_fd,
    std::string* error) noexcept {
    struct stat status {};
    if (directory_fd < 0 ||
        ::fstat(directory_fd, &status) != 0) {
        SetError(
            error,
            std::string("cannot inspect Raw stream directory: ") +
                std::strerror(errno));
        return false;
    }
    if (!S_ISDIR(status.st_mode) ||
        status.st_uid != ::geteuid() ||
        (status.st_mode & (S_IWGRP | S_IWOTH)) != 0) {
        SetError(
            error,
            "Raw stream directory must be private and owned by the service UID");
        return false;
    }
    return true;
}

bool ValidateLeaseFd(
    int fd,
    std::uint32_t source_stream_id,
    std::uint32_t capture_date,
    RawWriterLeaseMarkerV1* marker,
    std::string* error) noexcept {
    struct stat status {};
    if (::fstat(fd, &status) != 0) {
        SetError(
            error,
            std::string("cannot inspect Raw writer lease: ") +
                std::strerror(errno));
        return false;
    }
    if (!S_ISREG(status.st_mode) ||
        status.st_uid != ::geteuid() ||
        status.st_nlink != 1 ||
        (status.st_mode & 0777U) != 0600U ||
        status.st_size !=
            static_cast<off_t>(
                kRawWriterLeaseMarkerBytes)) {
        SetError(
            error,
            "Raw writer lease has an unsafe type, owner, mode, link count, or size");
        return false;
    }
    const int flags = ::fcntl(fd, F_GETFL);
    if (flags < 0 ||
        (flags & O_ACCMODE) != O_RDWR ||
        (flags & O_APPEND) != 0) {
        SetError(
            error,
            "Raw writer lease is not retained O_RDWR without O_APPEND");
        return false;
    }

    std::array<std::byte, kRawWriterLeaseMarkerBytes> wire{};
    RawWriterLeaseMarkerV1 decoded;
    if (!PreadAll(fd, wire) ||
        !DecodeRawWriterLeaseMarkerV1(wire, &decoded) ||
        decoded.source_stream_id != source_stream_id ||
        decoded.capture_date != capture_date) {
        SetError(
            error,
            "Raw writer lease marker is invalid or belongs to another namespace");
        return false;
    }
    *marker = decoded;
    return true;
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

int OpenLeaseAt(
    int directory_fd,
    const char* name,
    bool create) noexcept {
    const int flags =
        O_RDWR | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC |
        (create ? O_CREAT | O_EXCL : 0);
    for (;;) {
        const int fd =
            ::openat(directory_fd, name, flags, 0600);
        if (fd >= 0 || errno != EINTR) {
            return fd;
        }
    }
}

bool AcquireLock(int fd) noexcept {
    for (;;) {
        if (::flock(fd, LOCK_EX | LOCK_NB) == 0) {
            return true;
        }
        if (errno != EINTR) {
            return false;
        }
    }
}

bool InventoryWriterLeaseTemporaries(
    int directory_fd,
    std::string_view expected_name,
    bool* expected_exists,
    bool* conflicting_exists) noexcept {
    if (expected_exists == nullptr ||
        conflicting_exists == nullptr) {
        errno = EINVAL;
        return false;
    }
    *expected_exists = false;
    *conflicting_exists = false;
    int independent = -1;
    for (;;) {
        independent = ::openat(
            directory_fd,
            ".",
            O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
                O_NOATIME | O_CLOEXEC);
        if (independent >= 0 || errno != EINTR) {
            break;
        }
    }
    struct stat expected_directory {};
    struct stat opened_directory {};
    if (independent < 0 ||
        ::fstat(directory_fd, &expected_directory) != 0 ||
        ::fstat(independent, &opened_directory) != 0 ||
        !S_ISDIR(expected_directory.st_mode) ||
        !S_ISDIR(opened_directory.st_mode) ||
        expected_directory.st_dev !=
            opened_directory.st_dev ||
        expected_directory.st_ino !=
            opened_directory.st_ino) {
        if (independent >= 0) {
            static_cast<void>(::close(independent));
        }
        return false;
    }
    DIR* const directory = ::fdopendir(independent);
    if (directory == nullptr) {
        static_cast<void>(::close(independent));
        return false;
    }
    errno = 0;
    for (;;) {
        const dirent* const entry =
            ::readdir(directory);
        if (entry == nullptr) {
            const int saved_error = errno;
            static_cast<void>(::closedir(directory));
            errno = saved_error;
            return saved_error == 0;
        }
        const std::string_view name(entry->d_name);
        if (!name.starts_with(
                kRawWriterLeaseAttemptTemporaryPrefix) &&
            name != kRawWriterLeaseTemporaryFilename) {
            continue;
        }
        if (name == expected_name) {
            *expected_exists = true;
        } else {
            *conflicting_exists = true;
        }
    }
}

}  // namespace

bool EncodeRawWriterLeaseMarkerV1(
    const RawWriterLeaseMarkerV1& marker,
    std::array<std::byte, kRawWriterLeaseMarkerBytes>* wire) noexcept {
    if (wire == nullptr ||
        marker.source_stream_id == 0U ||
        marker.capture_date == 0U) {
        return false;
    }
    std::array<std::byte, kRawWriterLeaseMarkerBytes> encoded{};
    std::copy(
        kRawWriterLeaseMagic.begin(),
        kRawWriterLeaseMagic.end(),
        encoded.begin());
    std::span<std::byte> bytes(encoded);
    StoreU16(
        kRawWriterLeaseVersion,
        bytes,
        raw_writer_lease_offset::kVersion);
    StoreU16(
        static_cast<std::uint16_t>(
            kRawWriterLeaseMarkerBytes),
        bytes,
        raw_writer_lease_offset::kMarkerSize);
    StoreU32(
        marker.source_stream_id,
        bytes,
        raw_writer_lease_offset::kSourceStreamId);
    StoreU32(
        marker.capture_date,
        bytes,
        raw_writer_lease_offset::kCaptureDate);
    const std::uint32_t crc =
        l2flow::common::ComputeCrc32c(encoded);
    StoreU32(
        crc,
        bytes,
        raw_writer_lease_offset::kCrc32c);
    *wire = encoded;
    return true;
}

bool DecodeRawWriterLeaseMarkerV1(
    std::span<const std::byte> wire,
    RawWriterLeaseMarkerV1* marker) noexcept {
    if (marker == nullptr ||
        wire.size() != kRawWriterLeaseMarkerBytes ||
        !std::equal(
            kRawWriterLeaseMagic.begin(),
            kRawWriterLeaseMagic.end(),
            wire.begin()) ||
        LoadU16(
            wire,
            raw_writer_lease_offset::kVersion) !=
            kRawWriterLeaseVersion ||
        LoadU16(
            wire,
            raw_writer_lease_offset::kMarkerSize) !=
            kRawWriterLeaseMarkerBytes ||
        !IsZero(
            wire.subspan(
                raw_writer_lease_offset::kReserved,
                raw_writer_lease_offset::kCrc32c -
                    raw_writer_lease_offset::kReserved))) {
        return false;
    }
    std::array<std::byte, kRawWriterLeaseMarkerBytes> copy{};
    std::copy(wire.begin(), wire.end(), copy.begin());
    const std::uint32_t stored_crc =
        LoadU32(
            wire,
            raw_writer_lease_offset::kCrc32c);
    StoreU32(
        0U,
        copy,
        raw_writer_lease_offset::kCrc32c);
    if (l2flow::common::ComputeCrc32c(copy) != stored_crc) {
        return false;
    }
    RawWriterLeaseMarkerV1 decoded;
    decoded.source_stream_id =
        LoadU32(
            wire,
            raw_writer_lease_offset::kSourceStreamId);
    decoded.capture_date =
        LoadU32(
            wire,
            raw_writer_lease_offset::kCaptureDate);
    decoded.crc32c = stored_crc;
    if (decoded.source_stream_id == 0U ||
        decoded.capture_date == 0U) {
        return false;
    }
    *marker = decoded;
    return true;
}

RawWriterLease::RawWriterLease(
    int directory_fd,
    int lease_fd,
    RawWriterLeaseMarkerV1 marker) noexcept
    : directory_fd_(directory_fd),
      lease_fd_(lease_fd),
      marker_(marker) {}

RawWriterLease::~RawWriterLease() {
    static_cast<void>(CloseNoIntr(lease_fd_));
    static_cast<void>(CloseNoIntr(directory_fd_));
}

std::unique_ptr<RawWriterLease>
AcquireRawWriterLeaseAtV1(
    int directory_fd,
    std::uint32_t source_stream_id,
    std::uint32_t capture_date,
    const l2flow::common::Identity128&
        recovery_attempt,
    std::string* error) noexcept {
    SetError(error, {});
    if (source_stream_id == 0U ||
        capture_date == 0U ||
        l2flow::common::IsZeroIdentity(
            recovery_attempt) ||
        !ValidateDirectoryFd(directory_fd, error)) {
        if (source_stream_id == 0U ||
            capture_date == 0U ||
            l2flow::common::IsZeroIdentity(
                recovery_attempt)) {
            SetError(error, "Raw writer lease namespace is invalid");
        }
        return nullptr;
    }
    std::string temporary_name;
    try {
        temporary_name =
            RawWriterLeaseAttemptTemporaryFilenameV1(
                recovery_attempt);
    } catch (...) {
        SetError(
            error,
            "cannot derive Raw writer lease temporary name");
        return nullptr;
    }
    const int retained_directory =
        ::fcntl(directory_fd, F_DUPFD_CLOEXEC, 0);
    if (retained_directory < 0) {
        SetError(
            error,
            std::string("cannot retain Raw stream directory: ") +
                std::strerror(errno));
        return nullptr;
    }

    struct stat final_status {};
    const bool final_exists =
        ::fstatat(
            retained_directory,
            kRawWriterLeaseFilename,
            &final_status,
            AT_SYMLINK_NOFOLLOW) == 0;
    const int final_stat_error = final_exists ? 0 : errno;
    bool temporary_exists = false;
    bool conflicting_temporary = false;
    const bool inventory_ok =
        InventoryWriterLeaseTemporaries(
            retained_directory,
            temporary_name,
            &temporary_exists,
            &conflicting_temporary);
    if ((!final_exists && final_stat_error != ENOENT) ||
        !inventory_ok ||
        conflicting_temporary ||
        (final_exists && temporary_exists)) {
        SetError(
            error,
            "Raw writer lease namespace has an ambiguous final/attempt-tmp state");
        static_cast<void>(CloseNoIntr(retained_directory));
        return nullptr;
    }

    const char* const open_name =
        final_exists
            ? kRawWriterLeaseFilename
            : temporary_name.c_str();
    const bool create =
        !final_exists && !temporary_exists;
    const int lease_fd =
        OpenLeaseAt(retained_directory, open_name, create);
    if (lease_fd < 0) {
        SetError(
            error,
            std::string("cannot open Raw writer lease: ") +
                std::strerror(errno));
        static_cast<void>(CloseNoIntr(retained_directory));
        return nullptr;
    }

    if (create) {
        RawWriterLeaseMarkerV1 new_marker;
        new_marker.source_stream_id = source_stream_id;
        new_marker.capture_date = capture_date;
        std::array<std::byte, kRawWriterLeaseMarkerBytes> wire{};
        if (!EncodeRawWriterLeaseMarkerV1(
                new_marker, &wire) ||
            !PwriteAll(lease_fd, wire) ||
            !FsyncLoop(lease_fd, true)) {
            SetError(
                error,
                std::string("cannot initialize Raw writer lease: ") +
                    std::strerror(errno));
            static_cast<void>(CloseNoIntr(lease_fd));
            static_cast<void>(CloseNoIntr(retained_directory));
            return nullptr;
        }
    }

    RawWriterLeaseMarkerV1 decoded;
    if (!ValidateLeaseFd(
            lease_fd,
            source_stream_id,
            capture_date,
            &decoded,
            error) ||
        !AcquireLock(lease_fd)) {
        if (error != nullptr && error->empty()) {
            SetError(
                error,
                std::string("Raw writer lease is already held: ") +
                    std::strerror(errno));
        }
        static_cast<void>(CloseNoIntr(lease_fd));
        static_cast<void>(CloseNoIntr(retained_directory));
        return nullptr;
    }

    // An adopted temporary or pre-existing final may have survived a crash
    // after its bytes reached page cache but before the corresponding file
    // barrier. Re-establish durability before publication/use on every path.
    if (!FsyncLoop(lease_fd, true) ||
        (final_exists &&
         !SameNamedInode(
             retained_directory,
             kRawWriterLeaseFilename,
             lease_fd))) {
        SetError(
            error,
            std::string(
                "cannot synchronize and revalidate Raw writer lease: ") +
                std::strerror(errno));
        static_cast<void>(CloseNoIntr(lease_fd));
        static_cast<void>(CloseNoIntr(retained_directory));
        return nullptr;
    }

    if (!final_exists) {
        if (RenameNoReplace(
                retained_directory,
                temporary_name.c_str(),
                kRawWriterLeaseFilename) != 0 ||
            !FsyncLoop(retained_directory, false)) {
            SetError(
                error,
                std::string("cannot publish Raw writer lease: ") +
                    std::strerror(errno));
            static_cast<void>(CloseNoIntr(lease_fd));
            static_cast<void>(CloseNoIntr(retained_directory));
            return nullptr;
        }
    }
    if (!SameNamedInode(
            retained_directory,
            kRawWriterLeaseFilename,
            lease_fd)) {
        SetError(
            error,
            "Raw writer lease pathname no longer names the locked inode");
        static_cast<void>(CloseNoIntr(lease_fd));
        static_cast<void>(CloseNoIntr(retained_directory));
        return nullptr;
    }

    try {
        auto lease = std::unique_ptr<RawWriterLease>(
            new RawWriterLease(
                retained_directory,
                lease_fd,
                decoded));
        SetError(error, {});
        return lease;
    } catch (...) {
        SetError(error, "cannot allocate Raw writer lease");
        static_cast<void>(CloseNoIntr(lease_fd));
        static_cast<void>(CloseNoIntr(retained_directory));
        return nullptr;
    }
}

std::string
RawWriterLeaseAttemptTemporaryFilenameV1(
    const l2flow::common::Identity128&
        recovery_attempt) {
    if (l2flow::common::IsZeroIdentity(
            recovery_attempt)) {
        throw std::invalid_argument(
            "zero recovery attempt");
    }
    return std::string(
               kRawWriterLeaseAttemptTemporaryPrefix) +
           l2flow::common::Identity128Hex(
               recovery_attempt) +
           ".tmp";
}

std::unique_ptr<RawWriterLease>
AcquireRawWriterLeaseAt(
    int directory_fd,
    std::uint32_t source_stream_id,
    std::uint32_t capture_date,
    std::string* error) noexcept {
    l2flow::common::Identity128
        compatibility_attempt{};
    compatibility_attempt[0U] = std::byte{1U};
    return AcquireRawWriterLeaseAtV1(
        directory_fd,
        source_stream_id,
        capture_date,
        compatibility_attempt,
        error);
}

}  // namespace l2flow::ingress
