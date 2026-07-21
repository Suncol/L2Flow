#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "l2flow/ingress/raw_reserve_coordinator_gate.h"

#include "l2flow/common/crc32c.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <limits>
#include <new>
#include <span>
#include <string>
#include <utility>

#include <fcntl.h>
#include <linux/fs.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>

namespace l2flow::ingress {
namespace {

static_assert(
    raw_reserve_coordinator_lease_offset::kCrc32c +
            sizeof(std::uint32_t) ==
        kRawReserveCoordinatorLeaseMarkerBytes);
static_assert(
    kRawReserveGenerationGateOffset +
            kRawReserveGenerationGateLength <=
        kRawReserveCoordinatorLeaseMarkerBytes);
static_assert(
    kRawReserveCoordinatorLeaseMarkerBytes <=
        std::numeric_limits<std::uint16_t>::max());
static_assert(
    kRawReserveGenerationGateOffset <=
        static_cast<std::uint64_t>(
            std::numeric_limits<off_t>::max()));
static_assert(
    kRawReserveGenerationGateLength <=
        static_cast<std::uint64_t>(
            std::numeric_limits<off_t>::max()));

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

void SetFailure(
    RawReserveCoordinatorGateError* failure,
    RawReserveCoordinatorGateError value) noexcept {
    if (failure != nullptr) {
        *failure = value;
    }
}

void StoreU16(
    std::uint16_t value,
    std::span<std::byte> output,
    std::size_t offset) noexcept {
    output[offset] =
        static_cast<std::byte>(value & 0xffU);
    output[offset + 1U] = static_cast<std::byte>(
        (value >> 8U) & 0xffU);
}

void StoreU32(
    std::uint32_t value,
    std::span<std::byte> output,
    std::size_t offset) noexcept {
    for (std::size_t index = 0U; index < 4U; ++index) {
        output[offset + index] = static_cast<std::byte>(
            (value >>
             static_cast<unsigned int>(index * 8U)) &
            0xffU);
    }
}

void StoreU64(
    std::uint64_t value,
    std::span<std::byte> output,
    std::size_t offset) noexcept {
    for (std::size_t index = 0U; index < 8U; ++index) {
        output[offset + index] = static_cast<std::byte>(
            (value >>
             static_cast<unsigned int>(index * 8U)) &
            0xffU);
    }
}

[[nodiscard]] std::uint16_t LoadU16(
    std::span<const std::byte> input,
    std::size_t offset) noexcept {
    return static_cast<std::uint16_t>(
        std::to_integer<std::uint16_t>(input[offset]) |
        static_cast<std::uint16_t>(
            std::to_integer<std::uint16_t>(
                input[offset + 1U])
            << 8U));
}

[[nodiscard]] std::uint32_t LoadU32(
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

[[nodiscard]] std::uint64_t LoadU64(
    std::span<const std::byte> input,
    std::size_t offset) noexcept {
    std::uint64_t result = 0U;
    for (std::size_t index = 0U; index < 8U; ++index) {
        result |=
            std::to_integer<std::uint64_t>(
                input[offset + index])
            << static_cast<unsigned int>(index * 8U);
    }
    return result;
}

[[nodiscard]] bool IsZero(
    std::span<const std::byte> bytes) noexcept {
    return std::all_of(
        bytes.begin(),
        bytes.end(),
        [](std::byte value) {
            return value == std::byte{0};
        });
}

[[nodiscard]] bool MarkerFieldsAreValid(
    const RawReserveCoordinatorLeaseMarkerV1&
        marker) noexcept {
    return !l2flow::common::IsZeroIdentity(
               marker.coordinator_identity) &&
           marker.device_id != 0U &&
           !IsZero(marker.quota_identity_sha256) &&
           !IsZero(marker.mount_identity_sha256);
}

[[nodiscard]] bool SameMarkerIdentity(
    const RawReserveCoordinatorLeaseMarkerV1& left,
    const RawReserveCoordinatorLeaseMarkerV1&
        right) noexcept {
    return left.coordinator_identity ==
               right.coordinator_identity &&
           left.device_id == right.device_id &&
           left.quota_identity_sha256 ==
               right.quota_identity_sha256 &&
           left.mount_identity_sha256 ==
               right.mount_identity_sha256;
}

[[nodiscard]] bool FsyncLoop(int fd) noexcept {
    for (;;) {
        if (::fsync(fd) == 0) {
            return true;
        }
        if (errno != EINTR) {
            return false;
        }
    }
}

[[nodiscard]] bool PreadAll(
    int fd,
    std::span<std::byte> bytes) noexcept {
    std::size_t completed = 0U;
    while (completed < bytes.size()) {
        const std::size_t request = std::min(
            bytes.size() - completed,
            static_cast<std::size_t>(
                std::numeric_limits<ssize_t>::max()));
        const ssize_t result = ::pread(
            fd,
            bytes.data() + completed,
            request,
            static_cast<off_t>(completed));
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

[[nodiscard]] bool PwriteAll(
    int fd,
    std::span<const std::byte> bytes) noexcept {
    std::size_t completed = 0U;
    while (completed < bytes.size()) {
        const std::size_t request = std::min(
            bytes.size() - completed,
            static_cast<std::size_t>(
                std::numeric_limits<ssize_t>::max()));
        const ssize_t result = ::pwrite(
            fd,
            bytes.data() + completed,
            request,
            static_cast<off_t>(completed));
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

[[nodiscard]] int OpenAtLoop(
    int directory_fd,
    const char* name,
    int flags,
    mode_t mode = 0U) noexcept {
    for (;;) {
        const int fd =
            ::openat(directory_fd, name, flags, mode);
        if (fd >= 0 || errno != EINTR) {
            return fd;
        }
    }
}

[[nodiscard]] bool SameNamedInode(
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

[[nodiscard]] bool ValidatePrivateRoot(
    int directory_fd,
    struct stat* status,
    std::string* error) noexcept {
    struct stat inspected {};
    if (directory_fd < 0 ||
        ::fstat(directory_fd, &inspected) != 0) {
        SetError(
            error,
            std::string(
                "cannot inspect retained Raw root: ") +
                std::strerror(errno));
        return false;
    }
    if (!S_ISDIR(inspected.st_mode) ||
        inspected.st_uid != ::geteuid() ||
        (inspected.st_mode & (S_IWGRP | S_IWOTH)) != 0) {
        SetError(
            error,
            "Raw root must be a private directory owned by the service UID");
        return false;
    }
    if (status != nullptr) {
        *status = inspected;
    }
    return true;
}

[[nodiscard]] int RetainPrivateRoot(
    int supplied_fd,
    std::string* error) noexcept {
    struct stat supplied {};
    if (!ValidatePrivateRoot(
            supplied_fd, &supplied, error)) {
        return -1;
    }
    const int retained = OpenAtLoop(
        supplied_fd,
        ".",
        O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC |
            O_NOATIME);
    if (retained < 0) {
        SetError(
            error,
            std::string("cannot retain private Raw root: ") +
                std::strerror(errno));
        return -1;
    }
    struct stat reopened {};
    if (!ValidatePrivateRoot(
            retained, &reopened, error)) {
        static_cast<void>(::close(retained));
        return -1;
    }
    if (supplied.st_dev != reopened.st_dev ||
        supplied.st_ino != reopened.st_ino) {
        SetError(
            error,
            "retained Raw root changed inode");
        static_cast<void>(::close(retained));
        return -1;
    }
    return retained;
}

[[nodiscard]] bool ValidateExpectedDevice(
    const struct stat& root_status,
    const RawReserveCoordinatorLeaseMarkerV1&
        expected,
    std::string* error) noexcept {
    if (!MarkerFieldsAreValid(expected) ||
        expected.device_id !=
            static_cast<std::uint64_t>(
                root_status.st_dev)) {
        SetError(
            error,
            "coordinator lease identity is invalid or belongs to another Raw device");
        return false;
    }
    return true;
}

[[nodiscard]] bool ValidateLeaseFd(
    int root_fd,
    const char* name,
    int lease_fd,
    const RawReserveCoordinatorLeaseMarkerV1&
        expected_marker,
    const RawReserveCoordinatorLeaseAnchorV1*
        expected_anchor,
    RawReserveCoordinatorLeaseAnchorV1* actual_anchor,
    RawReserveCoordinatorGateError* failure,
    std::string* error) noexcept {
    struct stat root_status {};
    struct stat lease_status {};
    if (!ValidatePrivateRoot(
            root_fd, &root_status, error)) {
        SetFailure(
            failure,
            RawReserveCoordinatorGateError::kUnsafeRoot);
        return false;
    }
    if (::fstat(lease_fd, &lease_status) != 0) {
        SetFailure(
            failure,
            RawReserveCoordinatorGateError::kUnsafeLease);
        SetError(
            error,
            std::string(
                "cannot inspect coordinator lease: ") +
                std::strerror(errno));
        return false;
    }
    const int flags = ::fcntl(lease_fd, F_GETFL);
    if (!S_ISREG(lease_status.st_mode) ||
        lease_status.st_uid != ::geteuid() ||
        (lease_status.st_mode & 0777U) != 0600U ||
        lease_status.st_nlink != 1 ||
        lease_status.st_dev != root_status.st_dev ||
        lease_status.st_size !=
            static_cast<off_t>(
                kRawReserveCoordinatorLeaseMarkerBytes) ||
        flags < 0 ||
        (flags & O_ACCMODE) != O_RDWR ||
        (flags & O_APPEND) != 0 ||
        !SameNamedInode(root_fd, name, lease_fd)) {
        SetFailure(
            failure,
            RawReserveCoordinatorGateError::kUnsafeLease);
        SetError(
            error,
            "coordinator lease has an unsafe type, owner, mode, link count, device, size, flags, or name-to-inode binding");
        return false;
    }

    RawReserveCoordinatorLeaseMarkerWireV1 wire{};
    RawReserveCoordinatorLeaseMarkerV1 decoded{};
    if (!PreadAll(lease_fd, wire) ||
        !DecodeRawReserveCoordinatorLeaseMarkerV1(
            wire, &decoded)) {
        SetFailure(
            failure,
            RawReserveCoordinatorGateError::kUnsafeLease);
        SetError(
            error,
            "coordinator lease marker or CRC is invalid");
        return false;
    }
    if (!SameMarkerIdentity(decoded, expected_marker) ||
        decoded.device_id !=
            static_cast<std::uint64_t>(
                root_status.st_dev)) {
        SetFailure(
            failure,
            RawReserveCoordinatorGateError::kMarkerMismatch);
        SetError(
            error,
            "coordinator lease marker does not match the expected reserve domain");
        return false;
    }

    RawReserveCoordinatorLeaseAnchorV1 actual{};
    actual.marker = decoded;
    actual.filesystem_device =
        static_cast<std::uint64_t>(lease_status.st_dev);
    actual.inode =
        static_cast<std::uint64_t>(lease_status.st_ino);
    if (expected_anchor != nullptr &&
        (expected_anchor->filesystem_device !=
             actual.filesystem_device ||
         expected_anchor->inode != actual.inode ||
         expected_anchor->marker != decoded)) {
        SetFailure(
            failure,
            RawReserveCoordinatorGateError::kIdentityChanged);
        SetError(
            error,
            "coordinator lease pathname no longer names the trusted inode");
        return false;
    }
    if (!SameNamedInode(root_fd, name, lease_fd)) {
        SetFailure(
            failure,
            RawReserveCoordinatorGateError::kIdentityChanged);
        SetError(
            error,
            "coordinator lease pathname changed while its marker was read");
        return false;
    }
    if (actual_anchor != nullptr) {
        *actual_anchor = actual;
    }
    return true;
}

[[nodiscard]] int RenameNoReplace(
    int root_fd,
    const char* old_name,
    const char* new_name) noexcept {
    return static_cast<int>(
        ::syscall(
            SYS_renameat2,
            root_fd,
            old_name,
            root_fd,
            new_name,
            RENAME_NOREPLACE));
}

[[nodiscard]] bool AcquireFlock(
    int fd,
    bool nonblocking) noexcept {
    const int operation =
        LOCK_EX | (nonblocking ? LOCK_NB : 0);
    for (;;) {
        if (::flock(fd, operation) == 0) {
            return true;
        }
        if (errno != EINTR) {
            return false;
        }
    }
}

[[nodiscard]] bool IsOfdUnsupportedError(
    int error_number) noexcept {
    return error_number == EINVAL ||
           error_number == ENOSYS ||
           error_number == EOPNOTSUPP;
}

enum class OfdLockResult : std::uint8_t {
    kAcquired = 0U,
    kBusy,
    kUnsupported,
    kFailure,
};

[[nodiscard]] OfdLockResult AcquireOfdLock(
    int fd,
    short lock_type,
    bool wait) noexcept {
#if defined(F_OFD_SETLK) && defined(F_OFD_SETLKW)
    struct flock lock {};
    lock.l_type = lock_type;
    lock.l_whence = SEEK_SET;
    lock.l_start = static_cast<off_t>(
        kRawReserveGenerationGateOffset);
    lock.l_len = static_cast<off_t>(
        kRawReserveGenerationGateLength);
    lock.l_pid = 0;
    const int command =
        wait ? F_OFD_SETLKW : F_OFD_SETLK;
    for (;;) {
        if (::fcntl(fd, command, &lock) == 0) {
            return OfdLockResult::kAcquired;
        }
        const int saved_errno = errno;
        if (saved_errno == EINTR) {
            continue;
        }
        if (saved_errno == EACCES ||
            saved_errno == EAGAIN) {
            return OfdLockResult::kBusy;
        }
        if (IsOfdUnsupportedError(saved_errno)) {
            return OfdLockResult::kUnsupported;
        }
        return OfdLockResult::kFailure;
    }
#else
    static_cast<void>(fd);
    static_cast<void>(lock_type);
    static_cast<void>(wait);
    errno = EOPNOTSUPP;
    return OfdLockResult::kUnsupported;
#endif
}

[[nodiscard]] int OpenExistingLease(
    int root_fd) noexcept {
    return OpenAtLoop(
        root_fd,
        kRawReserveCoordinatorLeaseFilename,
        O_RDWR | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC |
            O_NOATIME);
}

[[nodiscard]] int OpenFreshLeaseTemporary(
    int root_fd) noexcept {
    return OpenAtLoop(
        root_fd,
        kRawReserveCoordinatorLeaseTemporaryFilename,
        O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW |
            O_NONBLOCK | O_CLOEXEC | O_NOATIME,
        0600U);
}

[[nodiscard]] bool NameExists(
    int root_fd,
    const char* name,
    bool* exists,
    std::string* error) noexcept {
    struct stat status {};
    if (::fstatat(
            root_fd,
            name,
            &status,
            AT_SYMLINK_NOFOLLOW) == 0) {
        *exists = true;
        return true;
    }
    if (errno == ENOENT) {
        *exists = false;
        return true;
    }
    SetError(
        error,
        std::string("cannot inspect coordinator lease name ") +
            name + ": " + std::strerror(errno));
    return false;
}

[[nodiscard]] bool ActionTokenIsValid(
    const RawReserveGenerationActionTokenV1&
        token,
    const RawReserveCoordinatorLeaseAnchorV1&
        lease) noexcept {
    return MarkerFieldsAreValid(lease.marker) &&
           !l2flow::common::IsZeroIdentity(
               token.reserve_state_uuid) &&
           token.state_generation != 0U &&
           !l2flow::common::IsZeroIdentity(
               token.writer_instance_id) &&
           !l2flow::common::IsZeroIdentity(
               token.recovery_attempt_id);
}

[[nodiscard]] int OpenAndValidateIndependentLease(
    int root_fd,
    const RawReserveCoordinatorLeaseAnchorV1&
        expected_lease,
    RawReserveCoordinatorGateError* failure,
    std::string* error) noexcept {
    struct stat root_status {};
    if (!ValidatePrivateRoot(
            root_fd, &root_status, error)) {
        SetFailure(
            failure,
            RawReserveCoordinatorGateError::kUnsafeRoot);
        return -1;
    }
    if (!ValidateExpectedDevice(
            root_status,
            expected_lease.marker,
            error) ||
        expected_lease.filesystem_device !=
            static_cast<std::uint64_t>(
                root_status.st_dev) ||
        expected_lease.inode == 0U) {
        SetFailure(
            failure,
            RawReserveCoordinatorGateError::kInvalidArgument);
        return -1;
    }
    const int fd = OpenExistingLease(root_fd);
    if (fd < 0) {
        SetFailure(
            failure,
            errno == ENOENT
                ? RawReserveCoordinatorGateError::kNotFound
                : RawReserveCoordinatorGateError::kUnsafeLease);
        SetError(
            error,
            std::string(
                "cannot independently open coordinator lease: ") +
                std::strerror(errno));
        return -1;
    }
    if (!ValidateLeaseFd(
            root_fd,
            kRawReserveCoordinatorLeaseFilename,
            fd,
            expected_lease.marker,
            &expected_lease,
            nullptr,
            failure,
            error)) {
        static_cast<void>(::close(fd));
        return -1;
    }
    return fd;
}

void SetOfdFailure(
    OfdLockResult result,
    RawReserveCoordinatorGateError* failure,
    std::string* error,
    bool action_gate) noexcept {
    switch (result) {
        case OfdLockResult::kAcquired:
            SetFailure(
                failure,
                RawReserveCoordinatorGateError::kNone);
            break;
        case OfdLockResult::kBusy:
            SetFailure(
                failure,
                action_gate
                    ? RawReserveCoordinatorGateError::
                          kGenerationGateBusy
                    : RawReserveCoordinatorGateError::
                          kLockFailure);
            SetError(
                error,
                action_gate
                    ? "coordinator transition holds the generation gate"
                    : "exclusive coordinator generation gate is busy");
            break;
        case OfdLockResult::kUnsupported:
            SetFailure(
                failure,
                RawReserveCoordinatorGateError::
                    kOfdLocksUnsupported);
            SetError(
                error,
                "Linux open-file-description locks are unavailable");
            break;
        case OfdLockResult::kFailure:
            SetFailure(
                failure,
                RawReserveCoordinatorGateError::kLockFailure);
            SetError(
                error,
                std::string(
                    "cannot acquire coordinator generation gate: ") +
                    std::strerror(errno));
            break;
    }
}

}  // namespace

bool EncodeRawReserveCoordinatorLeaseMarkerV1(
    const RawReserveCoordinatorLeaseMarkerV1& marker,
    RawReserveCoordinatorLeaseMarkerWireV1* wire) noexcept {
    if (wire == nullptr ||
        !MarkerFieldsAreValid(marker)) {
        return false;
    }
    RawReserveCoordinatorLeaseMarkerWireV1 encoded{};
    std::copy(
        kRawReserveCoordinatorLeaseMagic.begin(),
        kRawReserveCoordinatorLeaseMagic.end(),
        encoded.begin());
    std::span<std::byte> bytes(encoded);
    StoreU16(
        kRawReserveCoordinatorLeaseVersion,
        bytes,
        raw_reserve_coordinator_lease_offset::kVersion);
    StoreU16(
        static_cast<std::uint16_t>(
            kRawReserveCoordinatorLeaseMarkerBytes),
        bytes,
        raw_reserve_coordinator_lease_offset::
            kMarkerSize);
    std::copy(
        marker.coordinator_identity.begin(),
        marker.coordinator_identity.end(),
        encoded.begin() +
            static_cast<std::ptrdiff_t>(
                raw_reserve_coordinator_lease_offset::
                    kCoordinatorIdentity));
    StoreU64(
        marker.device_id,
        bytes,
        raw_reserve_coordinator_lease_offset::kDeviceId);
    std::copy(
        marker.quota_identity_sha256.begin(),
        marker.quota_identity_sha256.end(),
        encoded.begin() +
            static_cast<std::ptrdiff_t>(
                raw_reserve_coordinator_lease_offset::
                    kQuotaIdentitySha256));
    std::copy(
        marker.mount_identity_sha256.begin(),
        marker.mount_identity_sha256.end(),
        encoded.begin() +
            static_cast<std::ptrdiff_t>(
                raw_reserve_coordinator_lease_offset::
                    kMountIdentitySha256));
    const std::uint32_t crc =
        l2flow::common::ComputeCrc32c(encoded);
    StoreU32(
        crc,
        bytes,
        raw_reserve_coordinator_lease_offset::kCrc32c);
    *wire = encoded;
    return true;
}

bool DecodeRawReserveCoordinatorLeaseMarkerV1(
    std::span<const std::byte> wire,
    RawReserveCoordinatorLeaseMarkerV1* marker) noexcept {
    if (marker == nullptr ||
        wire.size() !=
            kRawReserveCoordinatorLeaseMarkerBytes ||
        !std::equal(
            kRawReserveCoordinatorLeaseMagic.begin(),
            kRawReserveCoordinatorLeaseMagic.end(),
            wire.begin()) ||
        LoadU16(
            wire,
            raw_reserve_coordinator_lease_offset::kVersion) !=
            kRawReserveCoordinatorLeaseVersion ||
        LoadU16(
            wire,
            raw_reserve_coordinator_lease_offset::
                kMarkerSize) !=
            kRawReserveCoordinatorLeaseMarkerBytes ||
        !IsZero(
            wire.subspan(
                raw_reserve_coordinator_lease_offset::
                    kReserved0,
                raw_reserve_coordinator_lease_offset::
                        kCoordinatorIdentity -
                    raw_reserve_coordinator_lease_offset::
                        kReserved0)) ||
        !IsZero(
            wire.subspan(
                raw_reserve_coordinator_lease_offset::
                    kReserved1,
                raw_reserve_coordinator_lease_offset::
                        kCrc32c -
                    raw_reserve_coordinator_lease_offset::
                        kReserved1))) {
        return false;
    }

    RawReserveCoordinatorLeaseMarkerWireV1 copy{};
    std::copy(wire.begin(), wire.end(), copy.begin());
    const std::uint32_t stored_crc =
        LoadU32(
            wire,
            raw_reserve_coordinator_lease_offset::kCrc32c);
    StoreU32(
        0U,
        copy,
        raw_reserve_coordinator_lease_offset::kCrc32c);
    if (l2flow::common::ComputeCrc32c(copy) !=
        stored_crc) {
        return false;
    }

    RawReserveCoordinatorLeaseMarkerV1 decoded{};
    std::copy_n(
        wire.begin() +
            static_cast<std::ptrdiff_t>(
                raw_reserve_coordinator_lease_offset::
                    kCoordinatorIdentity),
        decoded.coordinator_identity.size(),
        decoded.coordinator_identity.begin());
    decoded.device_id = LoadU64(
        wire,
        raw_reserve_coordinator_lease_offset::kDeviceId);
    std::copy_n(
        wire.begin() +
            static_cast<std::ptrdiff_t>(
                raw_reserve_coordinator_lease_offset::
                    kQuotaIdentitySha256),
        decoded.quota_identity_sha256.size(),
        decoded.quota_identity_sha256.begin());
    std::copy_n(
        wire.begin() +
            static_cast<std::ptrdiff_t>(
                raw_reserve_coordinator_lease_offset::
                    kMountIdentitySha256),
        decoded.mount_identity_sha256.size(),
        decoded.mount_identity_sha256.begin());
    decoded.crc32c = stored_crc;
    if (!MarkerFieldsAreValid(decoded)) {
        return false;
    }
    *marker = decoded;
    return true;
}

std::string_view RawReserveCoordinatorGateErrorName(
    RawReserveCoordinatorGateError error) noexcept {
    switch (error) {
        case RawReserveCoordinatorGateError::kNone:
            return "none";
        case RawReserveCoordinatorGateError::kInvalidArgument:
            return "invalid_argument";
        case RawReserveCoordinatorGateError::kUnsafeRoot:
            return "unsafe_root";
        case RawReserveCoordinatorGateError::kNotFound:
            return "not_found";
        case RawReserveCoordinatorGateError::kAmbiguousTemporary:
            return "ambiguous_temporary";
        case RawReserveCoordinatorGateError::kUnsafeLease:
            return "unsafe_lease";
        case RawReserveCoordinatorGateError::kMarkerMismatch:
            return "marker_mismatch";
        case RawReserveCoordinatorGateError::kWriteFailure:
            return "write_failure";
        case RawReserveCoordinatorGateError::kSyncFailure:
            return "sync_failure";
        case RawReserveCoordinatorGateError::kPublishConflict:
            return "publish_conflict";
        case RawReserveCoordinatorGateError::kCoordinatorBusy:
            return "coordinator_busy";
        case RawReserveCoordinatorGateError::kGenerationGateBusy:
            return "generation_gate_busy";
        case RawReserveCoordinatorGateError::kOfdLocksUnsupported:
            return "ofd_locks_unsupported";
        case RawReserveCoordinatorGateError::kLockFailure:
            return "lock_failure";
        case RawReserveCoordinatorGateError::kIdentityChanged:
            return "identity_changed";
        case RawReserveCoordinatorGateError::kAllocationFailure:
            return "allocation_failure";
    }
    return "unknown";
}

RawReserveCoordinatorLeaseV1::
RawReserveCoordinatorLeaseV1(
    int root_directory_fd,
    int lease_fd,
    RawReserveCoordinatorLeaseAnchorV1 anchor) noexcept
    : root_directory_fd_(root_directory_fd),
      lease_fd_(lease_fd),
      anchor_(std::move(anchor)) {}

RawReserveCoordinatorLeaseV1::
~RawReserveCoordinatorLeaseV1() {
    if (lease_fd_ >= 0) {
        static_cast<void>(::close(lease_fd_));
    }
    if (root_directory_fd_ >= 0) {
        static_cast<void>(::close(root_directory_fd_));
    }
}

std::unique_ptr<RawReserveCoordinatorLeaseV1>
AcquireRawReserveCoordinatorLeaseAtV1(
    int retained_raw_root_fd,
    const RawReserveCoordinatorLeaseMarkerV1&
        expected_marker,
    RawReserveCoordinatorGateError* failure,
    std::string* error) noexcept {
    SetFailure(
        failure,
        RawReserveCoordinatorGateError::kNone);
    SetError(error, {});

    ScopedFd root(
        RetainPrivateRoot(retained_raw_root_fd, error));
    if (root.get() < 0) {
        SetFailure(
            failure,
            RawReserveCoordinatorGateError::kUnsafeRoot);
        return nullptr;
    }
    struct stat root_status {};
    if (!ValidatePrivateRoot(
            root.get(), &root_status, error) ||
        !ValidateExpectedDevice(
            root_status, expected_marker, error)) {
        SetFailure(
            failure,
            RawReserveCoordinatorGateError::kInvalidArgument);
        return nullptr;
    }

    bool final_exists = false;
    bool temporary_exists = false;
    if (!NameExists(
            root.get(),
            kRawReserveCoordinatorLeaseFilename,
            &final_exists,
            error) ||
        !NameExists(
            root.get(),
            kRawReserveCoordinatorLeaseTemporaryFilename,
            &temporary_exists,
            error)) {
        SetFailure(
            failure,
            RawReserveCoordinatorGateError::kUnsafeLease);
        return nullptr;
    }
    if (final_exists && temporary_exists) {
        SetFailure(
            failure,
            RawReserveCoordinatorGateError::
                kAmbiguousTemporary);
        SetError(
            error,
            "coordinator lease final and typed temporary coexist");
        return nullptr;
    }

    const bool create =
        !final_exists && !temporary_exists;
    const char* const opened_name =
        final_exists
            ? kRawReserveCoordinatorLeaseFilename
            : kRawReserveCoordinatorLeaseTemporaryFilename;
    ScopedFd lease(
        create
            ? OpenFreshLeaseTemporary(root.get())
            : OpenAtLoop(
                  root.get(),
                  opened_name,
                  O_RDWR | O_NOFOLLOW | O_NONBLOCK |
                      O_CLOEXEC | O_NOATIME));
    if (lease.get() < 0) {
        SetFailure(
            failure,
            create
                ? RawReserveCoordinatorGateError::
                      kWriteFailure
                : RawReserveCoordinatorGateError::
                      kUnsafeLease);
        SetError(
            error,
            std::string("cannot open coordinator lease ") +
                opened_name + ": " + std::strerror(errno));
        return nullptr;
    }

    if (create) {
        RawReserveCoordinatorLeaseMarkerWireV1 wire{};
        if (::fchmod(lease.get(), 0600U) != 0 ||
            !EncodeRawReserveCoordinatorLeaseMarkerV1(
                expected_marker, &wire) ||
            !PwriteAll(lease.get(), wire)) {
            SetFailure(
                failure,
                RawReserveCoordinatorGateError::kWriteFailure);
            SetError(
                error,
                std::string(
                    "cannot initialize coordinator lease temporary: ") +
                    std::strerror(errno));
            return nullptr;
        }
        if (!FsyncLoop(lease.get())) {
            SetFailure(
                failure,
                RawReserveCoordinatorGateError::kSyncFailure);
            SetError(
                error,
                std::string(
                    "cannot sync coordinator lease temporary: ") +
                    std::strerror(errno));
            return nullptr;
        }
    }

    RawReserveCoordinatorLeaseAnchorV1 anchor{};
    if (!ValidateLeaseFd(
            root.get(),
            opened_name,
            lease.get(),
            expected_marker,
            nullptr,
            &anchor,
            failure,
            error)) {
        return nullptr;
    }
    if (!AcquireFlock(lease.get(), !create)) {
        SetFailure(
            failure,
            RawReserveCoordinatorGateError::
                kCoordinatorBusy);
        SetError(
            error,
            "another coordinator holds the fixed lease");
        return nullptr;
    }

    // The path may have been inspected before waiting for a crash survivor's
    // flock. Re-establish the exact name/inode/marker proof while holding the
    // lock, before either syncing or publishing that inode.
    if (!ValidateLeaseFd(
            root.get(),
            opened_name,
            lease.get(),
            expected_marker,
            nullptr,
            &anchor,
            failure,
            error)) {
        return nullptr;
    }

    if (!final_exists) {
        // A visible complete temporary may be a retry from an earlier
        // process. A successful read does not prove that process observed a
        // completed sync, so repeat the inode barrier before adoption.
        if (!create && !FsyncLoop(lease.get())) {
            SetFailure(
                failure,
                RawReserveCoordinatorGateError::kSyncFailure);
            SetError(
                error,
                std::string(
                    "cannot sync adopted coordinator lease temporary: ") +
                    std::strerror(errno));
            return nullptr;
        }
        if (RenameNoReplace(
                root.get(),
                kRawReserveCoordinatorLeaseTemporaryFilename,
                kRawReserveCoordinatorLeaseFilename) != 0) {
            SetFailure(
                failure,
                RawReserveCoordinatorGateError::
                    kPublishConflict);
            SetError(
                error,
                std::string(
                    "cannot publish fixed coordinator lease: ") +
                    std::strerror(errno));
            return nullptr;
        }
        if (!FsyncLoop(root.get())) {
            SetFailure(
                failure,
                RawReserveCoordinatorGateError::kSyncFailure);
            SetError(
                error,
                std::string(
                    "cannot sync Raw root after coordinator lease publication: ") +
                    std::strerror(errno));
            return nullptr;
        }
    } else if (!FsyncLoop(lease.get())) {
        SetFailure(
            failure,
            RawReserveCoordinatorGateError::kSyncFailure);
        SetError(
            error,
            std::string(
                "cannot sync existing coordinator lease: ") +
                std::strerror(errno));
        return nullptr;
    }

    if (!ValidateLeaseFd(
            root.get(),
            kRawReserveCoordinatorLeaseFilename,
            lease.get(),
            expected_marker,
            nullptr,
            &anchor,
            failure,
            error)) {
        return nullptr;
    }

    auto* const allocated =
        new (std::nothrow) RawReserveCoordinatorLeaseV1(
            root.Release(),
            lease.Release(),
            anchor);
    if (allocated == nullptr) {
        SetFailure(
            failure,
            RawReserveCoordinatorGateError::
                kAllocationFailure);
        SetError(
            error,
            "cannot allocate coordinator lease handle");
        return nullptr;
    }
    return std::unique_ptr<
        RawReserveCoordinatorLeaseV1>(allocated);
}

RawReserveGenerationActionGateV1::
RawReserveGenerationActionGateV1(
    int lease_fd,
    l2flow::common::Identity128 reserve_state_uuid,
    RawReserveGenerationActionTokenV1 token) noexcept
    : lease_fd_(lease_fd),
      reserve_state_uuid_(reserve_state_uuid),
      token_(std::move(token)) {}

RawReserveGenerationActionGateV1::
~RawReserveGenerationActionGateV1() {
    if (lease_fd_ >= 0) {
        static_cast<void>(::close(lease_fd_));
    }
}

std::unique_ptr<RawReserveGenerationActionGateV1>
AcquireUnboundRawReserveGenerationActionGateAtV1(
    int retained_raw_root_fd,
    const RawReserveCoordinatorLeaseAnchorV1&
        expected_lease,
    RawReserveCoordinatorGateError* failure,
    std::string* error) noexcept {
    SetFailure(
        failure,
        RawReserveCoordinatorGateError::kNone);
    SetError(error, {});
    ScopedFd lease(
        OpenAndValidateIndependentLease(
            retained_raw_root_fd,
            expected_lease,
            failure,
            error));
    if (lease.get() < 0) {
        return nullptr;
    }
    const OfdLockResult locked =
        AcquireOfdLock(lease.get(), F_RDLCK, false);
    if (locked != OfdLockResult::kAcquired) {
        SetOfdFailure(
            locked, failure, error, true);
        return nullptr;
    }
    if (!ValidateLeaseFd(
            retained_raw_root_fd,
            kRawReserveCoordinatorLeaseFilename,
            lease.get(),
            expected_lease.marker,
            &expected_lease,
            nullptr,
            failure,
            error)) {
        return nullptr;
    }

    auto* const allocated =
        new (std::nothrow)
            RawReserveGenerationActionGateV1(
                lease.Release(),
                {},
                {});
    if (allocated == nullptr) {
        SetFailure(
            failure,
            RawReserveCoordinatorGateError::
                kAllocationFailure);
        SetError(
            error,
            "cannot allocate generation action gate");
        return nullptr;
    }
    return std::unique_ptr<
        RawReserveGenerationActionGateV1>(allocated);
}

bool BindRawReserveGenerationActionGateTokenV1(
    RawReserveGenerationActionGateV1& gate,
    const RawReserveGenerationActionTokenV1& token,
    std::string* error) noexcept {
    SetError(error, {});
    if (gate.lease_fd_ < 0 ||
        !l2flow::common::IsZeroIdentity(
            gate.token_.reserve_state_uuid) ||
        !l2flow::common::IsZeroIdentity(
            gate.reserve_state_uuid_) ||
        l2flow::common::IsZeroIdentity(
            token.reserve_state_uuid) ||
        token.state_generation == 0U ||
        l2flow::common::IsZeroIdentity(
            token.writer_instance_id) ||
        l2flow::common::IsZeroIdentity(
            token.recovery_attempt_id)) {
        SetError(
            error,
            "generation action gate cannot bind the supplied token");
        return false;
    }
    gate.reserve_state_uuid_ =
        token.reserve_state_uuid;
    gate.token_ = token;
    return true;
}

std::unique_ptr<RawReserveGenerationActionGateV1>
AcquireRawReserveGenerationActionGateAtV1(
    int retained_raw_root_fd,
    const RawReserveCoordinatorLeaseAnchorV1&
        expected_lease,
    const RawReserveGenerationActionTokenV1& token,
    RawReserveCoordinatorGateError* failure,
    std::string* error) noexcept {
    if (!ActionTokenIsValid(token, expected_lease)) {
        SetFailure(
            failure,
            RawReserveCoordinatorGateError::
                kInvalidArgument);
        SetError(
            error,
            "generation action token is invalid or bound to another reserve state");
        return nullptr;
    }
    auto gate =
        AcquireUnboundRawReserveGenerationActionGateAtV1(
            retained_raw_root_fd,
            expected_lease,
            failure,
            error);
    if (gate == nullptr ||
        !BindRawReserveGenerationActionGateTokenV1(
            *gate, token, error)) {
        if (gate != nullptr) {
            SetFailure(
                failure,
                RawReserveCoordinatorGateError::
                    kInvalidArgument);
        }
        return nullptr;
    }
    return gate;
}

bool ValidateRawReserveGenerationObservationV1(
    const RawReserveGenerationActionGateV1& gate,
    const RawReserveGenerationObservationV1& observation,
    std::string* error) noexcept {
    SetError(error, {});
    const RawReserveGenerationActionTokenV1& token =
        gate.token();
    if (observation.reserve_state_uuid !=
            token.reserve_state_uuid ||
        observation.state_generation !=
            token.state_generation ||
        observation.writer_instance_id !=
            token.writer_instance_id ||
        observation.recovery_attempt_id !=
            token.recovery_attempt_id ||
        observation.finalization_cycle_id !=
            token.finalization_cycle_id) {
        SetError(
            error,
            "latest decoded reserve state no longer matches the generation action token");
        return false;
    }
    return true;
}

bool ValidateRawReserveGenerationStateV1(
    const RawReserveGenerationActionGateV1& gate,
    const ReserveCoordinatorStateV1& latest_decoded_state,
    std::size_t entry_index,
    std::string* error) noexcept {
    SetError(error, {});
    if (latest_decoded_state.selected_slot >=
            latest_decoded_state.slots.size()) {
        SetError(
            error,
            "latest decoded reserve state has no selected slot");
        return false;
    }
    const ReserveStateSlotV1& slot =
        latest_decoded_state
            .slots[latest_decoded_state.selected_slot];
    if (entry_index >= slot.entry_count ||
        entry_index >= slot.entries.size()) {
        SetError(
            error,
            "latest decoded reserve state does not contain the token entry");
        return false;
    }
    const RawReserveGenerationActionTokenV1& token =
        gate.token();
    if (latest_decoded_state.header.reserve_state_uuid !=
            token.reserve_state_uuid ||
        slot.reserve_state_uuid != token.reserve_state_uuid) {
        SetError(
            error,
            "latest decoded reserve header or slot belongs to another reserve state");
        return false;
    }
    const ReserveStateEntryV1& entry =
        slot.entries[entry_index];
    RawReserveGenerationObservationV1 observation{};
    observation.reserve_state_uuid =
        slot.reserve_state_uuid;
    observation.state_generation = slot.generation;
    observation.writer_instance_id =
        entry.writer_instance;
    observation.recovery_attempt_id =
        entry.executor_or_recovery_attempt;
    observation.finalization_cycle_id =
        slot.finalization_cycle_id;
    return ValidateRawReserveGenerationObservationV1(
        gate, observation, error);
}

RawReserveCoordinatorTransitionGuardV1::
RawReserveCoordinatorTransitionGuardV1(
    int lease_fd) noexcept
    : lease_fd_(lease_fd) {}

RawReserveCoordinatorTransitionGuardV1::
~RawReserveCoordinatorTransitionGuardV1() {
    if (lease_fd_ >= 0) {
        static_cast<void>(::close(lease_fd_));
    }
}

std::unique_ptr<
    RawReserveCoordinatorTransitionGuardV1>
AcquireRawReserveCoordinatorTransitionGuardAtV1(
    int retained_raw_root_fd,
    const RawReserveCoordinatorLeaseAnchorV1&
        expected_lease,
    RawReserveCoordinatorGateError* failure,
    std::string* error) noexcept {
    SetFailure(
        failure,
        RawReserveCoordinatorGateError::kNone);
    SetError(error, {});
    ScopedFd lease(
        OpenAndValidateIndependentLease(
            retained_raw_root_fd,
            expected_lease,
            failure,
            error));
    if (lease.get() < 0) {
        return nullptr;
    }
    const OfdLockResult locked =
        AcquireOfdLock(lease.get(), F_WRLCK, true);
    if (locked != OfdLockResult::kAcquired) {
        SetOfdFailure(
            locked, failure, error, false);
        return nullptr;
    }
    if (!ValidateLeaseFd(
            retained_raw_root_fd,
            kRawReserveCoordinatorLeaseFilename,
            lease.get(),
            expected_lease.marker,
            &expected_lease,
            nullptr,
            failure,
            error)) {
        return nullptr;
    }

    auto* const allocated =
        new (std::nothrow)
            RawReserveCoordinatorTransitionGuardV1(
                lease.Release());
    if (allocated == nullptr) {
        SetFailure(
            failure,
            RawReserveCoordinatorGateError::
                kAllocationFailure);
        SetError(
            error,
            "cannot allocate coordinator transition guard");
        return nullptr;
    }
    return std::unique_ptr<
        RawReserveCoordinatorTransitionGuardV1>(allocated);
}

}  // namespace l2flow::ingress
